"""Explicit administrator recovery for one protocol-paused config entry."""
from datetime import timedelta
from time import monotonic
import voluptuous as vol
from homeassistant.config_entries import ConfigEntryState, current_entry
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.service import async_register_admin_service
from homeassistant.util import dt as dt_util
from .const import DOMAIN


def auth_diagnostics(hass, entry_id):
    record = hass.data.get("kia_uvo_auth_budgets", {}).get(entry_id, {})
    remaining = max(0, record.get("retry_at", 0) - monotonic())
    count = record.get("protocol_failures", 0)
    return dict(
        protocol_paused=record.get("protocol_blocked", False),
        pause_reason="protocol_recovery_required" if record.get("protocol_blocked") else None,
        last_error=record.get("last_error"),
        protocol_failures=count, automatic_protocol_limit=3,
        automatic_attempts_remaining=max(0, 3-count),
        next_attempt_not_before=(dt_util.utcnow()+timedelta(seconds=remaining)).isoformat(),
        backoff_seconds=round(remaining, 1),
        auth_busy=bool((op := hass.data.get("kia_uvo_auth_operations", {}).get(entry_id)) and op.busy),
        reconnect_busy=entry_id in hass.data.get("kia_uvo_reconnects", {}),
        recovery_action="kia_uvo.reconnect",
    )


def async_setup_reconnect(hass):
    if hass.services.has_service(DOMAIN, "reconnect"):
        return

    async def reconnect(call):
        if not call.context.user_id or call.context.parent_id:
            raise HomeAssistantError("Kia reconnect requires a direct administrator action")
        entry = hass.config_entries.async_get_entry(call.data["config_entry_id"])
        if entry is None or entry.domain != DOMAIN or entry.disabled_by is not None:
            raise HomeAssistantError("Kia config entry is not available")
        if hass.is_stopping or entry.state not in (ConfigEntryState.LOADED, ConfigEntryState.SETUP_RETRY):
            raise HomeAssistantError("Kia config entry is not ready for reconnection")
        coordinator = hass.data.get(DOMAIN, {}).get(entry.unique_id)
        temporary = entry.state == ConfigEntryState.SETUP_RETRY
        if temporary:
            # Failed setup owns no loaded platforms. Recover credentials/cache
            # once, then let Core's EXISTING scheduled setup retry continue.
            from .coordinator import HyundaiKiaConnectDataUpdateCoordinator
            token = current_entry.set(entry)
            try:
                coordinator = HyundaiKiaConnectDataUpdateCoordinator(hass, entry)
            finally:
                current_entry.reset(token)
        if coordinator is None or coordinator.config_entry.entry_id != entry.entry_id:
            raise HomeAssistantError("Kia coordinator is unavailable")
        try:
            await coordinator.async_reconnect()
        finally:
            if temporary:
                # A cancelled caller must not stop a still-running owned task.
                task = hass.data.get("kia_uvo_reconnects", {}).get(entry.entry_id)
                if task is not None and not task.done():
                    task.add_done_callback(lambda _: hass.async_create_task(coordinator.async_shutdown()))
                else:
                    await coordinator.async_shutdown()

    async_register_admin_service(hass, DOMAIN, "reconnect", reconnect,
        schema=vol.Schema({vol.Required("config_entry_id"): str}))
