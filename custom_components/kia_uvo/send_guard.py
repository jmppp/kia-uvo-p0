"""Guarded climate boundary; all HA state is read on the HA event loop."""
import asyncio
from concurrent.futures import TimeoutError as FutureTimeout
from threading import Event
from time import monotonic, time
from homeassistant.core import callback
from homeassistant.const import EVENT_HOMEASSISTANT_STOP, EVENT_STATE_CHANGED
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.template import Template
from custom_components.kia_uvo._vendor.hyundai_kia_connect_api.command_gate import CommandGate

class SendGuard:
    def __init__(self, hass, spec):
        if not isinstance(spec, dict) or set(spec) != {"context", "checks", "expires_at"}:
            raise HomeAssistantError("Missing send guard")
        remaining = float(spec["expires_at"]) - time()
        if not 0 < remaining <= 180:
            raise HomeAssistantError("Invalid command deadline")
        self.hass = hass
        self.deadline = monotonic() + remaining
        self.context = dict(spec["context"])
        self.checks = spec["checks"]
        self.invalid = Event()
        self.closed = False
        self.unsub = []
        # Same controls as the existing PG request snapshot. Every change cancels
        # the lease, including revoke-and-restore while auth/executor is waiting.
        controls = self.context["safety_snapshot"]["controls"]
        @callback
        def changed(event):
            old, new = event.data.get("old_state"), event.data.get("new_state")
            if event.data.get("entity_id") in controls and (old is None or new is None or old.state != new.state or old.attributes.get("generation") != new.attributes.get("generation")):
                self.invalid.set()
        @callback
        def stopping(event):
            self.invalid.set()
        self.unsub = [hass.bus.async_listen(EVENT_STATE_CHANGED, changed),
                      hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, stopping)]
        self.gate = CommandGate(self.authorize_thread, self.deadline)

    def validate(self):
        if self.closed or self.invalid.is_set() or self.hass.is_stopping or monotonic() >= self.deadline:
            raise HomeAssistantError("Command authority revoked or deadline passed")
        variables = dict(self.context)
        for key, template in self.checks.items():
            variables[key] = Template(template, self.hass).async_render(variables) if isinstance(template, str) else template
        if str(variables.get("reason", "")).strip() != "ok":
            raise HomeAssistantError("Command blocked: " + str(variables.get("reason", "missing guard")))

    async def authorize_loop(self):
        self.validate()  # no await: one HA-loop policy decision

    def authorize_thread(self):
        if self.invalid.is_set() or self.closed:
            raise HomeAssistantError("Command cancelled")
        future = asyncio.run_coroutine_threadsafe(self.authorize_loop(), self.hass.loop)
        try:
            future.result(timeout=max(0.001, min(2, self.deadline - monotonic())))
        except FutureTimeout as err:
            future.cancel()
            self.invalid.set()
            raise HomeAssistantError("Send authorization timed out") from err
        if self.invalid.is_set() or self.closed:
            raise HomeAssistantError("Command cancelled")

    def close(self):
        self.closed = True
        self.invalid.set()
        for unsubscribe in self.unsub:
            unsubscribe()
