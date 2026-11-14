"""Entry-wide durable force admission. No account secrets in this journal."""
import asyncio
from copy import deepcopy
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
import json
import math
import os
from pathlib import Path
import tempfile
from threading import Event
from time import time, monotonic

from homeassistant.const import EVENT_HOMEASSISTANT_STOP
from homeassistant.exceptions import HomeAssistantError
from .send_guard import SendGuard
from ._vendor.hyundai_kia_connect_api.auth_transport import TransientAuthError
from ._vendor.hyundai_kia_connect_api.command_gate import command_scope, CommandGate
from ._vendor.hyundai_kia_connect_api.exceptions import RateLimitingError, AuthenticationError, APIError


class AtomicJournal:
    """One event-loop owner, fsync file+directory before acknowledging a save."""
    def __init__(self, path, *, recover_wakes=False):
        self.path = Path(path)
        self.data = None
        self.recover_wakes = recover_wakes
        self._load_task = None

    def _load(self):
        if not self.path.exists():
            return {'version': 1, 'until': 0, 'server_until': 0, 'records': {}}
        data = json.loads(self.path.read_text())
        if data.get('version') != 1 or not isinstance(data.get('records'), dict) or type(data.get('until')) not in (int, float) or not math.isfinite(data['until']):
            raise ValueError('Invalid wake journal')
        if type(data.get('server_until')) not in (int,float) or not math.isfinite(data['server_until']):raise ValueError('Invalid server budget')
        if self.recover_wakes:
            recovered = False
            for record in data['records'].values():
                if not isinstance(record, dict):raise ValueError('Invalid wake record')
                if record.get('state') in ('reserved', 'sending'):
                    record['state'] = 'unknown_outcome'
                    data['poll_blocked'] = True
                    recovered = True
            if recovered:
                # Runs on the load executor. Save/ack before returning admission.
                self._save(data)
        return data

    async def load(self, hass):
        if self.data is None:
            # Concurrent cache-backoff/admission must not race recovery writes.
            # A cancelled reader does not release an in-flight disk operation.
            if self._load_task is None:
                async def load_once():
                    self.data = await hass.async_add_executor_job(self._load)
                self._load_task = hass.async_create_task(load_once(), 'Kia wake journal recovery')
                self._load_task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
            await asyncio.shield(self._load_task)
        return self.data

    def _save(self, data):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(prefix='.kia-wake-', dir=self.path.parent)
        try:
            with os.fdopen(fd, 'w') as out:
                os.fchmod(out.fileno(), 0o600)
                json.dump(data, out, sort_keys=True, allow_nan=False)
                out.flush(); os.fsync(out.fileno())
            os.replace(name, self.path)
            directory = os.open(self.path.parent, os.O_DIRECTORY)
            try: os.fsync(directory)
            finally: os.close(directory)
        finally:
            if os.path.exists(name): os.unlink(name)

    async def save(self, hass, data):
        # Keep conservative in-memory consumption even if persistence fails.
        self.data = deepcopy(data)
        await hass.async_add_executor_job(self._save, self.data)


class WakeError(HomeAssistantError):
    def __init__(self, category, retry_after=0):
        self.category = category
        self.retry_after = retry_after
        super().__init__('Kia status wake: ' + category)


def retry_delay(response):
    value = response.headers.get('Retry-After', '')
    try: return max(0, float(value))
    except ValueError:
        try: return max(0, parsedate_to_datetime(value).timestamp() - time())
        except (ValueError, TypeError, OverflowError): return 0


def key_for(vehicle_id, departure):
    return hashlib.sha256((vehicle_id + ':' + str(int(departure))).encode()).hexdigest()


class WakeVehicleAuthError(AuthenticationError, HomeAssistantError):
    """Vehicle rejection; account policy belongs to the coordinator cache cycle."""


class WakeAPIError(APIError, HomeAssistantError):
    """Sanitized vehicle API failure, retaining coordinator classification."""


class WakeRateLimitError(RateLimitingError, HomeAssistantError):
    """Sanitized account backoff with the original retry budget."""


class WakeBudget:
    def __init__(self, hass, entry_id):
        self.hass = hass
        digest = hashlib.sha256(entry_id.encode()).hexdigest()
        self.journal = AtomicJournal(hass.config.path('.storage', 'kia_wake_' + digest + '.json'), recover_wakes=True)
        self.task = None
        self.stopping = False
        self.clock = time
        self.persistence_lock = asyncio.Lock()
        self.unsub = hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, self._stop)

    def _stop(self, event):
        self.stopping = True

    async def persist(self, data):
        async with self.persistence_lock:
            current = await self.journal.load(self.hass)
            data['until'] = max(data['until'], current['until'])
            data['server_until'] = max(data['server_until'], current['server_until'])
            await self.journal.save(self.hass, data)

    async def defer(self, delay):
        async with self.persistence_lock:
            data = deepcopy(await self.journal.load(self.hass))
            data['until'] = max(data['until'], self.clock() + delay)
            data['server_until'] = max(data['server_until'], self.clock() + delay)
            await self.journal.save(self.hass, data)

    async def run(self, coordinator, vehicle_id, spec=None, departure=None, source='native'):
        if self.stopping or self.hass.is_stopping or coordinator._auth_stopping or coordinator._shutdown_requested:
            raise WakeError('stopping')
        if self.task is not None and not self.task.done():
            raise WakeError('busy')
        guard = SendGuard(self.hass, spec) if spec is not None else None
        cancelled = Event()
        self.task = self.hass.async_create_task(self._run(coordinator, vehicle_id, guard, departure, cancelled, source), 'Kia single status wake')
        self.task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
        try:
            return await asyncio.shield(self.task)
        except asyncio.CancelledError:
            cancelled.set()
            if guard: guard.close()
            # Task/worker remains the budget owner. Do not cancel executor wrapper.
            raise

    async def _run(self, c, vehicle_id, guard, departure, cancelled, source):
        record = None
        data = None
        owned = False
        force_phase = False
        native_deadline = monotonic() + 90
        owner = asyncio.current_task()
        try:
            data = await self.journal.load(self.hass)
            now = self.clock()
            if source=='poll' and data.get('poll_blocked',False):raise WakeError('previous_uncertain_wake')
            if now < data['until']:
                raise WakeError('cooldown', data['until'] - now)
            vehicle = c.vehicle_manager.vehicles.get(vehicle_id)
            if vehicle is None or not vehicle.enabled or c.config_entry.data.get('region') != 1 or vehicle.ccu_ccs2_protocol_support != 0:
                raise WakeError('unsupported_vehicle')
            if departure is not None and (type(departure) not in (int, float) or not now < departure <= now + 86400):
                raise WakeError('invalid_departure')
            record = key_for(vehicle_id, departure) if departure else 'native:' + str(now)
            if record in data['records']:
                raise WakeError('departure_consumed')
            # Reservation is irreversible; also reserves while auth/executor waits.
            data = deepcopy(data)
            data['until'] = now + 1800
            data['records'][record] = {'reserved': now, 'state': 'reserved'}
            owned = True
            await self.persist(data)
            await c.async_check_and_refresh_token()
            if c._auth_recovery_pending:
                raise WakeError('cache_recovery_required')
            def validate():
                if cancelled.is_set() or self.stopping or self.hass.is_stopping or c._auth_stopping or c._shutdown_requested:
                    raise WakeError('stopping')
                if self.task is not owner:raise WakeError('ownership_lost')
                saved = self.journal.data
                reservation = saved['records'].get(record, {})
                if reservation.get('reserved') != now or reservation.get('state') not in ('reserved','sending') or saved['until'] < now+1800:
                    raise WakeError('reservation_lost')
                if saved['server_until'] > self.clock() or c._auth_retry_at > monotonic():raise WakeError('server_backoff')
                if guard: guard.validate()
            validate()
            # A single loop decision includes BOTH entry/budget veto and SendGuard.
            # The climate SendGuard implementation is not modified.
            deadline = guard.deadline if guard else native_deadline
            async def authorize_loop():
                validate()
            def authorize():
                f = asyncio.run_coroutine_threadsafe(authorize_loop(), self.hass.loop)
                try: f.result(timeout=max(.001, min(2, deadline-monotonic())))
                except BaseException:
                    cancelled.set(); f.cancel(); raise
            gate = guard.gate if guard else CommandGate(authorize, deadline)
            gate.authorize = authorize
            data['records'][record]['state'] = 'sending'
            await self.persist(data)
            before = vehicle.last_updated_at
            transport_started = [None]
            authorize = gate.authorize
            def authorize_send():
                authorize()
                transport_started[0] = self.clock()
            gate.authorize = authorize_send
            def work():
                with command_scope(gate):
                    return c.vehicle_manager.api.force_status_once(c.vehicle_manager.token, vehicle)
            force_phase = True
            worker = asyncio.ensure_future(self.hass.async_add_executor_job(work))
            try:
                raw = await asyncio.shield(worker)
            except asyncio.CancelledError:
                if guard: guard.close()
                await asyncio.gather(worker, return_exceptions=True)
                raise
            validate()  # late completion never restores invalid authority
            stamp = vehicle.last_updated_at.timestamp() if vehicle.last_updated_at else None
            if not isinstance(raw.get('time'), str) or type(raw.get('engine')) is not bool or type(raw.get('airCtrlOn')) is not bool or stamp is None or transport_started[0] is None or not transport_started[0]-5 <= stamp <= self.clock() or (before is not None and stamp <= before.timestamp()):
                raise WakeError('unverified_vehicle_status')
            data['records'][record]['state'] = 'completed'
            data['records'][record]['vehicle_time'] = stamp
            data['poll_blocked'] = False
            await self.persist(data)
            c.async_set_updated_data(c.data)
            return raw
        except BaseException as error:
            # Only touch this operation's record, never another consumed departure.
            if owned:
                data = deepcopy(data)
                data['records'][record]['state'] = 'unknown_outcome'
                data['poll_blocked'] = True
                delay = getattr(error, 'retry_after', 0) or 0
                if isinstance(error, RateLimitingError): delay = max(7200, delay)
                # Auth backoff is shared by auth; also persist its remaining budget.
                delay = max(delay, max(0, c._auth_retry_at - monotonic()))
                data['until'] = max(data['until'], self.clock() + delay)
                await self.persist(data)
            if guard is not None and not guard.gate.entered and str(error)=='Command blocked: Fresh status; wake unnecessary':
                raise WakeError('not_needed') from None
            if isinstance(error, (WakeError, asyncio.CancelledError)): raise
            if isinstance(error, AuthenticationError):
                if not force_phase:
                    # Proven account auth failure; preserve the established reauth path.
                    c.config_entry.async_start_reauth_if_available(self.hass)
                    raise type(error)('Kia authentication rejected') from None
                raise WakeVehicleAuthError('Kia vehicle authentication rejected') from None
            if isinstance(error, TransientAuthError):
                raise error from None  # existing sanitized auth protocol/backoff types
            if isinstance(error, RateLimitingError):
                sanitized = WakeRateLimitError('Kia status rate limited')
                sanitized.retry_after = max(7200, getattr(error,'retry_after',0) or 0)
                raise sanitized from None
            if isinstance(error, APIError):
                sanitized = WakeAPIError('Kia status API failure')
                sanitized.retry_after = max(0, getattr(error,'retry_after',0) or 0)
                raise sanitized from None
            raise WakeError('request_failed', max(0, (data or {}).get('until', 0) - self.clock())) from None
        finally:
            if guard: guard.close()


def budget(c):
    budgets = c.hass.data.setdefault('kia_wake_budgets', {})
    return budgets.setdefault(c.config_entry.entry_id, WakeBudget(c.hass, c.config_entry.entry_id)) if c.config_entry.entry_id not in budgets else budgets[c.config_entry.entry_id]
