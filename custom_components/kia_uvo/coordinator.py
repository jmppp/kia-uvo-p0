"""Coordinator for Hyundai / Kia Connect integration."""

from __future__ import annotations

import asyncio
from time import monotonic
from custom_components.kia_uvo._vendor.hyundai_kia_connect_api.auth_transport import TransientAuthError, AuthProtocolError, AuthConfigurationError
from collections.abc import Callable
import datetime as dt
from datetime import timedelta
from functools import partial
import logging
from typing import Any, Literal

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    EVENT_HOMEASSISTANT_STOP,
    CONF_PASSWORD,
    CONF_PIN,
    CONF_REGION,
    CONF_SCAN_INTERVAL,
    CONF_USERNAME,
)
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryError, HomeAssistantError
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.dispatcher import async_dispatcher_send
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as dt_util
from custom_components.kia_uvo._vendor.hyundai_kia_connect_api import (
    ClimateRequestOptions,
    POIInfo,
    ScheduleChargingClimateRequestOptions,
    SVMDetails,
    Token,
    Vehicle,
    VehicleManager,
    WindowRequestOptions,
)
from custom_components.kia_uvo._vendor.hyundai_kia_connect_api.const import WINDOW_STATE
from custom_components.kia_uvo._vendor.hyundai_kia_connect_api.exceptions import (
    APIError,
    AuthenticationError,
    RateLimitingError,
    UnsupportedControlError,
)
from custom_components.kia_uvo._vendor.hyundai_kia_connect_api.svm_image import render_views

from .auth_operation import AuthOperation

from .const import (
    CONF_BRAND,
    CONF_ENABLE_GEOLOCATION_ENTITY,
    CONF_FORCE_REFRESH_INTERVAL,
    CONF_NO_FORCE_REFRESH_HOUR_FINISH,
    CONF_NO_FORCE_REFRESH_HOUR_START,
    CONF_TOKEN,
    CONF_USE_EMAIL_WITH_GEOCODE_API,
    DEFAULT_ENABLE_GEOLOCATION_ENTITY,
    DEFAULT_FORCE_REFRESH_INTERVAL,
    DEFAULT_NO_FORCE_REFRESH_HOUR_FINISH,
    DEFAULT_NO_FORCE_REFRESH_HOUR_START,
    DEFAULT_SCAN_INTERVAL,
    DEFAULT_USE_EMAIL_WITH_GEOCODE_API,
    DOMAIN,
    OffPeakChargingMode,
)

_LOGGER = logging.getLogger(__name__)

# Render-invalidation signal for the SVM image entities: sent by
# set_svm_dewarp, consumed in image.py.
SIGNAL_SVM_RENDER = DOMAIN + "_{}_svm_render"

# "check" lets the library pick force or cached, "cached" is the server-side
# cached state, "force" is an explicit fresh read.
PollMode = Literal["check", "cached", "force"]


class HyundaiKiaConnectDataUpdateCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Class to manage fetching data from the API."""

    def __init__(self, hass: HomeAssistant, config_entry: ConfigEntry) -> None:
        """Initialize."""
        self.platforms: set[str] = set()
        self._action_lock = asyncio.Lock()
        self._reconnects = hass.data.setdefault("kia_uvo_reconnects", {})
        self._auth_operation = None
        self._auth_result = None
        self._auth_worker = None
        self._auth_stopping = False
        self._auth_entry_id = config_entry.entry_id
        # Process-local admission survives replacement coordinators while an old
        # executor drains. No credentials or persistent schema data live here.
        self._auth_operations = hass.data.setdefault("kia_uvo_auth_operations", {})
        self._auth_unsub_stop = hass.bus.async_listen_once(
            EVENT_HOMEASSISTANT_STOP, self._async_auth_on_stop
        )
        self._auth_last_error = None
        self._auth_protocol_failures = 0
        self._auth_protocol_blocked = None
        self._auth_failures = 0
        self._auth_retry_at = 0.0
        self._auth_recovery_pending = False
        self._auth_budgets = hass.data.setdefault("kia_uvo_auth_budgets", {})
        self._auth_budget_revision = -1
        self._sync_auth_budget()
        self._svm_details: dict[str, SVMDetails] = {}
        # Per-vehicle SVM fisheye dewarp toggle (local UI state, off by default).
        self._svm_dewarp: dict[str, bool] = {}
        # Rendered SVM views cache: vehicle_id -> ((captured_at, dewarp), views).
        # Invalidated by key change on a new capture or a switch toggle — no
        # explicit invalidation hooks needed.
        self._svm_views: dict[
            str, tuple[tuple[dt.datetime | None, bool], dict[str, bytes]]
        ] = {}
        # Vehicles whose last poll failed with a vehicle-scoped API error.
        self._failed_vehicles: set[str] = set()
        # Consecutive failed polls per vehicle (drives the "skip it" hint).
        self._vehicle_failures: dict[str, int] = {}

        self.vehicle_manager = VehicleManager(
            region=config_entry.data[CONF_REGION],
            brand=config_entry.data[CONF_BRAND],
            username=config_entry.data[CONF_USERNAME],
            password=config_entry.data[CONF_PASSWORD],
            pin=config_entry.data[CONF_PIN],
            geocode_api_enable=config_entry.options.get(
                CONF_ENABLE_GEOLOCATION_ENTITY, DEFAULT_ENABLE_GEOLOCATION_ENTITY
            ),
            geocode_api_use_email=config_entry.options.get(
                CONF_USE_EMAIL_WITH_GEOCODE_API, DEFAULT_USE_EMAIL_WITH_GEOCODE_API
            ),
            language=hass.config.language,
            token=Token.from_dict(config_entry.data.get(CONF_TOKEN, None))
            if config_entry.data.get(CONF_TOKEN, None)
            else None,
        )
        self.scan_interval: int = (
            config_entry.options.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL) * 60
        )
        self.force_refresh_interval: int = (
            config_entry.options.get(
                CONF_FORCE_REFRESH_INTERVAL, DEFAULT_FORCE_REFRESH_INTERVAL
            )
            * 60
        )
        self.no_force_refresh_hour_start: int = config_entry.options.get(
            CONF_NO_FORCE_REFRESH_HOUR_START, DEFAULT_NO_FORCE_REFRESH_HOUR_START
        )
        self.no_force_refresh_hour_finish: int = config_entry.options.get(
            CONF_NO_FORCE_REFRESH_HOUR_FINISH, DEFAULT_NO_FORCE_REFRESH_HOUR_FINISH
        )
        self.enable_geolocation_entity = config_entry.options.get(
            CONF_ENABLE_GEOLOCATION_ENTITY, DEFAULT_ENABLE_GEOLOCATION_ENTITY
        )
        self.use_email_with_geocode_api = config_entry.options.get(
            CONF_USE_EMAIL_WITH_GEOCODE_API, DEFAULT_USE_EMAIL_WITH_GEOCODE_API
        )

        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=timedelta(
                seconds=min(self.scan_interval, self.force_refresh_interval)
            ),
        )
        _LOGGER.debug(
            "%s - Polling configured: scan_interval=%ds, "
            "force_refresh_interval=%ds, update_interval=%ds, "
            "no_force_refresh_hours=%d-%d",
            DOMAIN,
            self.scan_interval,
            self.force_refresh_interval,
            min(self.scan_interval, self.force_refresh_interval),
            self.no_force_refresh_hour_start,
            self.no_force_refresh_hour_finish,
        )

    async def _async_update_data(self) -> dict[str, Any]:
        """Update data via library. Called by update_coordinator periodically.

        Allow to update for the first time without further checking
        Allow force update, if time diff between latest update and `now` is greater than force refresh delta
        """
        _LOGGER.debug(
            "%s - _async_update_data called, scan_interval=%ds, force_refresh_interval=%ds",
            DOMAIN,
            self.scan_interval,
            self.force_refresh_interval,
        )
        try:
            await self.async_check_and_refresh_token()
        except AuthConfigurationError:
            raise ConfigEntryError("Invalid Kia auth endpoint configuration; manual review required") from None
        except AuthProtocolError as err:
            raise UpdateFailed("Kia auth protocol invalid; automatic attempts paused" if self._auth_protocol_blocked else "Invalid Kia auth protocol response", retry_after=max(60, err.retry_after)) from err
        except TransientAuthError as err:
            raise UpdateFailed("Temporary Kia authentication failure", retry_after=max(1, err.retry_after)) from err
        except AuthenticationError as AuthError:
            raise ConfigEntryAuthFailed(AuthError) from AuthError
        except Exception as err:
            # Transient API errors (e.g. DeviceIDError, ReadTimeoutError) from
            # Kia's EU backend must be surfaced as UpdateFailed rather than
            # propagating as unexpected exceptions.  HA's update coordinator
            # counts unexpected exceptions and cancels the config entry after
            # enough consecutive failures, which makes all entities permanently
            # unavailable until the integration is manually reloaded.
            # Raising UpdateFailed(retry_after=60) keeps entities temporarily
            # unavailable and schedules an automatic retry after 60 seconds
            # instead of waiting for the next full poll interval.
            # See: https://github.com/Hyundai-Kia-Connect/kia_uvo/issues/1538
            raise UpdateFailed(
                f"Token refresh failed, will retry in 60s: {err}",
                retry_after=60,
            ) from err
        current_hour = dt_util.now().hour

        in_force_window = (
            (self.no_force_refresh_hour_start <= self.no_force_refresh_hour_finish)
            and (
                current_hour < self.no_force_refresh_hour_start
                or current_hour >= self.no_force_refresh_hour_finish
            )
        ) or (
            (self.no_force_refresh_hour_start >= self.no_force_refresh_hour_finish)
            and (
                current_hour < self.no_force_refresh_hour_start
                and current_hour >= self.no_force_refresh_hour_finish
            )
        )

        await self._async_poll_vehicles(mode="check" if in_force_window and not self._auth_recovery_pending else "cached")
        if not self._failed_vehicles:
            self._auth_recovery_pending = False
            self._publish_auth_budget()
        return self.data

    async def async_update_all(self) -> None:
        """Update vehicle data."""
        await self.async_check_and_refresh_token()
        await self._async_poll_vehicles(mode="cached")
        if not self._failed_vehicles:
            self._auth_recovery_pending = False
            self._publish_auth_budget()
        self.async_set_updated_data(self.data)

    async def async_force_update_all(self) -> None:
        """Force refresh vehicle data and update it."""
        await self.async_check_and_refresh_token()
        if self._auth_recovery_pending:
            raise HomeAssistantError("Cache recovery is required before force refresh")
        await self._async_poll_vehicles(mode="force")
        self.async_set_updated_data(self.data)

    async def _async_poll_vehicles(self, *, mode: PollMode) -> None:
        """Poll each vehicle on its own so one failing vehicle can't fail all.

        APIError is vehicle-scoped. Rate limiting and unknown exceptions are
        account-scoped and raise UpdateFailed so Home Assistant backs off.
        """
        failed: set[str] = set()
        auth_failures = 0
        for vehicle_id in list(self.vehicle_manager.vehicles):
            try:
                await self._async_poll_vehicle(vehicle_id, mode=mode)
            except AuthenticationError as err:
                # resCode 7501 arrives per vehicle; only all of them failing
                # means the account itself is rejected.
                auth_failures += 1
                failed.add(vehicle_id)
                self._log_vehicle_failure(vehicle_id, err)
                if auth_failures == len(self.vehicle_manager.vehicles):
                    raise ConfigEntryAuthFailed(err) from err
            except RateLimitingError as err:
                raise UpdateFailed(
                    f"Rate limited by the API: {err}", retry_after=2 * 60 * 60
                ) from err
            except APIError as err:
                failed.add(vehicle_id)
                self._log_vehicle_failure(vehicle_id, err)
            except Exception as err:
                # UpdateFailed is logged without a traceback.
                _LOGGER.exception("%s - Error communicating with API", DOMAIN)
                raise UpdateFailed(f"Error communicating with API: {err}") from err
            else:
                self._vehicle_failures.pop(vehicle_id, None)
        self._failed_vehicles = failed

    async def _async_poll_vehicle(self, vehicle_id: str, *, mode: PollMode) -> None:
        """Update one vehicle, falling back to the cached state if a force fails."""
        try:
            if mode == "check":
                await self.hass.async_add_executor_job(
                    self.vehicle_manager.check_and_force_update_vehicle,
                    self.force_refresh_interval,
                    vehicle_id,
                )
            elif mode == "force":
                await self.hass.async_add_executor_job(
                    self.vehicle_manager.force_refresh_vehicle_state, vehicle_id
                )
            else:
                await self.hass.async_add_executor_job(
                    self.vehicle_manager.update_vehicle_with_cached_state, vehicle_id
                )
        except Exception as err:
            if mode == "cached":
                raise
            _LOGGER.warning(
                "%s - Force refresh failed for %s (%s), using cached state: %s",
                DOMAIN,
                self.vehicle_manager.vehicles[vehicle_id].name,
                vehicle_id,
                err,
            )
            await self.hass.async_add_executor_job(
                self.vehicle_manager.update_vehicle_with_cached_state, vehicle_id
            )

    def _log_vehicle_failure(self, vehicle_id: str, err: Exception) -> None:
        vehicle = self.vehicle_manager.vehicles[vehicle_id]
        failures = self._vehicle_failures.get(vehicle_id, 0) + 1
        self._vehicle_failures[vehicle_id] = failures
        _LOGGER.warning(
            "%s - Failed to update %s (%s): %s: %s",
            DOMAIN,
            vehicle.name,
            vehicle_id,
            type(err).__name__,
            err,
        )
        if failures == 3:
            # A vehicle the backend no longer serves (e.g. a sold car) fails
            # every poll; stop polling it by disabling the device in HA.
            _LOGGER.warning(
                "%s - Vehicle %s (%s) failed %s polls in a row. If the backend "
                "no longer serves it, disable its device in Home Assistant "
                "to stop polling it.",
                DOMAIN,
                vehicle.name,
                vehicle_id,
                failures,
            )

    def vehicle_available(self, vehicle_id: str) -> bool:
        """Return False if the vehicle's last poll failed."""
        return vehicle_id not in self._failed_vehicles

    async def async_force_refresh_vehicle(self, vehicle_id: str) -> None:
        """Force refresh a single vehicle's state."""
        await self.async_check_and_refresh_token()
        if self._auth_recovery_pending:
            raise HomeAssistantError("Cache recovery is required before force refresh")
        await self.hass.async_add_executor_job(
            self.vehicle_manager.force_refresh_vehicle_state, vehicle_id
        )
        self.async_set_updated_data(self.data)

    async def async_supports_svm(self, vehicle_id: str) -> bool:
        """Return whether the given vehicle supports SVM.

        Capability is a per-region class attribute stamped on the Vehicle by
        the API library (like supports_window_control), so this is a plain
        attribute read — no API call, no executor job needed.
        """
        vehicle = self.vehicle_manager.vehicles.get(vehicle_id)
        if vehicle is None:
            return False
        return bool(vehicle.supports_svm)

    def svm_dewarp_enabled(self, vehicle_id: str) -> bool:
        """Return the per-vehicle SVM fisheye dewarp preference."""
        return self._svm_dewarp.get(vehicle_id, False)

    def set_svm_dewarp(self, vehicle_id: str, enabled: bool) -> None:
        """Set the per-vehicle SVM fisheye dewarp preference."""
        self._svm_dewarp[vehicle_id] = enabled
        # The render key changes with the toggle, but captured_at does not —
        # without this signal the image proxy keeps serving the pre-toggle
        # image until the next capture.
        async_dispatcher_send(self.hass, SIGNAL_SVM_RENDER.format(vehicle_id))

    def get_cached_svm_details(self, vehicle_id: str) -> SVMDetails | None:
        """Return cached SVM details for a vehicle, or None if not yet fetched."""
        return self._svm_details.get(vehicle_id)

    async def async_get_svm_details(self, vehicle_id: str) -> SVMDetails:
        """Fetch the latest cached SVM image and metadata from the API."""
        details = await self.hass.async_add_executor_job(
            self.vehicle_manager.get_svm_details, vehicle_id
        )
        self._svm_details[vehicle_id] = details
        return details

    async def async_request_svm_capture(self, vehicle_id: str) -> SVMDetails:
        """Trigger a fresh SVM capture and update the cached details."""
        details = await self.hass.async_add_executor_job(
            self.vehicle_manager.request_svm_capture,
            vehicle_id,
            True,  # acknowledged_warning — capture is always a user-initiated action
        )
        self._svm_details[vehicle_id] = details
        self.async_set_updated_data(self.data)
        return details

    async def async_get_svm_views(self, vehicle_id: str) -> dict[str, bytes] | None:
        """Return the 5 rendered SVM views (front/rear/left/right/top) as JPEG.

        Renders once per (captured_at, dewarp switch) state: every image
        entity serves from the same dict, and a new capture or a switch
        toggle invalidates it by key change. The render runs in an executor
        thread (dewarp is CPU-bound numpy work); render errors (e.g. missing
        Pillow) propagate to the caller.
        """
        details = self.get_cached_svm_details(vehicle_id)
        if details is None or not details.image_bytes or not details.image_sizes:
            return None
        key = (details.captured_at, self.svm_dewarp_enabled(vehicle_id))
        cached = self._svm_views.get(vehicle_id)
        if cached is not None and cached[0] == key:
            return cached[1]
        views: dict[str, bytes] = await self.hass.async_add_executor_job(
            partial(render_views, details, dewarp=key[1])
        )
        self._svm_views[vehicle_id] = (key, views)
        return views

    @callback
    def _sync_auth_budget(self):
        record = self._auth_budgets.get(self._auth_entry_id)
        if record is None or record["revision"] == self._auth_budget_revision:
            return
        self._auth_budget_revision = record["revision"]
        self._auth_last_error = record.get("last_error")
        self._auth_failures = record["failures"]
        self._auth_retry_at = record["retry_at"]
        self._auth_recovery_pending = record["recovery_pending"]
        self._auth_protocol_failures = record["protocol_failures"]
        self._auth_protocol_blocked = AuthProtocolError(1800, record["protocol_status"]) if record["protocol_blocked"] else None

    @callback
    def _publish_auth_budget(self):
        # Numeric/status metadata only. Setup retries must not reset an entry's
        # backoff or bounded protocol recovery by creating another coordinator.
        revision = self._auth_budgets.get(self._auth_entry_id, {}).get("revision", 0) + 1
        self._auth_budgets[self._auth_entry_id] = dict(
            revision=revision, last_error=self._auth_last_error, failures=self._auth_failures, retry_at=self._auth_retry_at,
            recovery_pending=self._auth_recovery_pending,
            protocol_failures=self._auth_protocol_failures,
            protocol_blocked=self._auth_protocol_blocked is not None,
            protocol_status=self._auth_protocol_blocked.status if self._auth_protocol_blocked is not None else None,
        )
        self._auth_budget_revision = revision

    @callback
    def _pause_protocol(self, status=None):
        if self._auth_protocol_blocked is None:
            _LOGGER.warning("Kia authentication paused after protocol failures; wait for backoff, then use kia_uvo.reconnect for this config entry")
        self._auth_protocol_blocked = AuthProtocolError(1800, status)

    async def async_reconnect(self):
        """User action: one auth/cache attempt, never a command or reload."""
        self._sync_auth_budget()
        if self._auth_stopping or self.hass.is_stopping or self._shutdown_requested:
            raise HomeAssistantError("Kia authentication is stopping")
        previous = self._auth_operations.get(self._auth_entry_id)
        if self._auth_entry_id in self._reconnects or (previous and previous.busy):
            raise HomeAssistantError("Kia authentication/reconnection is still in progress")
        if self._auth_protocol_blocked is None:
            raise HomeAssistantError("Kia authentication is not protocol-paused")
        if self._auth_retry_at > monotonic():
            raise HomeAssistantError("Kia authentication backoff is still active; see diagnostics")
        # Reserve before any await; consumers cannot cancel this owned attempt.
        task = self.hass.async_create_task(self._async_reconnect_once(), "Kia manual reconnect")
        self._reconnects[self._auth_entry_id] = task
        def finished(done):
            if self._reconnects.get(self._auth_entry_id) is done:
                self._reconnects.pop(self._auth_entry_id)
            if not done.cancelled():
                done.exception()
        task.add_done_callback(finished)
        await asyncio.shield(task)

    async def _async_reconnect_once(self):
        complete = False
        cache_retry_at = 0.0
        try:
            await self.async_check_and_refresh_token(_reconnect=True)
            try:
                await self._async_poll_vehicles(mode="cached")
            except UpdateFailed as err:
                cache_retry_at = monotonic() + max(0, err.retry_after or 0)
                self._auth_last_error = "rate_limit" if isinstance(err.__cause__, RateLimitingError) else "cache"
                raise
            except Exception:
                self._auth_last_error = "cache"
                raise
            if self._failed_vehicles:
                self._auth_last_error = "cache"
                raise HomeAssistantError("Kia cache recovery was not verified")
            if self._auth_stopping or self.hass.is_stopping or self._shutdown_requested:
                raise HomeAssistantError("Kia authentication is stopping")
            self._auth_recovery_pending = False
            self._publish_auth_budget()
            self.async_set_updated_data(self.data)
            complete = True
        except (AuthenticationError, ConfigEntryAuthFailed):
            self._auth_last_error = "reauth_required"
            self.config_entry.async_start_reauth_if_available(self.hass)
            raise HomeAssistantError("Kia authentication requires user action") from None
        except asyncio.CancelledError:
            raise
        except Exception:
            raise HomeAssistantError("Kia reconnection failed; see sanitized authentication diagnostics") from None
        finally:
            if not complete:
                self._sync_auth_budget()
                self._auth_protocol_failures = max(3, self._auth_protocol_failures)
                self._pause_protocol()
                self._auth_recovery_pending = True
                # Also bound cache/early local failures; never shorten Retry-After.
                self._auth_retry_at = max(self._auth_retry_at, cache_retry_at, monotonic() + 60)
                self._publish_auth_budget()

    # Bounds for consumers/shutdown, never a lease renewal or a worker retry.
    AUTH_WAIT_TIMEOUT = 120
    AUTH_SHUTDOWN_TIMEOUT = 5

    @callback
    def _async_auth_on_stop(self, event):
        self._auth_unsub_stop = None  # once-listener already removed itself
        self._async_stop_auth()

    @callback
    def _async_stop_auth(self):
        self._auth_stopping = True
        if self._auth_operation is not None:
            self._auth_operation.abort_queued(HomeAssistantError("Kia authentication stopped before start"))
        if self._auth_unsub_stop is not None:
            self._auth_unsub_stop()
            self._auth_unsub_stop = None

    async def async_shutdown(self):
        self._async_stop_auth()
        await super().async_shutdown()
        result = self._auth_result
        if result is not None and not result.done():
            # wait() does not cancel the shared future on timeout/cancellation.
            _, pending = await asyncio.wait({result}, timeout=self.AUTH_SHUTDOWN_TIMEOUT)
            if pending:
                _LOGGER.warning("Kia authentication still draining; new authentication remains blocked")

    @callback
    def _auth_completed(self, result):
        # Observe even when all consumers have cancelled; never log credentials.
        if not result.cancelled():
            result.exception()
        operation = self._auth_operations.get(self._auth_entry_id)
        if operation is not None and operation.result is result and not operation.busy:
            self._auth_operations.pop(self._auth_entry_id)

    def _run_auth_worker(self, operation):
        if not operation.begin():
            return  # queued cancellation won the start boundary; no SDK call
        error = None
        try:
            self.vehicle_manager.check_and_refresh_token()
        except BaseException as err:
            error = err
        finally:
            operation.finish_worker(error)

    @callback
    def _auth_wrapper_done(self, operation, worker):
        if worker.cancelled():
            # Only a QUEUED operation may be released here. A RUNNING worker
            # still owns the entry until its explicit SDK completion arrives.
            operation.abort_queued(HomeAssistantError("Kia authentication cancelled before start"))
        else:
            worker.exception()  # observe wrapper failures independently of consumers

    @callback
    def _auth_worker_result(self, operation, error):
        if not operation.claim_completion():
            return
        result = operation.result
        self._auth_last_error = ("protocol" if isinstance(error, AuthProtocolError) else
            "transport" if isinstance(error, TransientAuthError) else
            "reauth_required" if isinstance(error, AuthenticationError) else
            "configuration" if isinstance(error, AuthConfigurationError) else
            "local_failure" if error is not None else None)
        if isinstance(error, TransientAuthError):
            if isinstance(error, AuthProtocolError):
                self._auth_protocol_failures += 1
                if self._auth_protocol_failures >= 3:
                    self._pause_protocol(error.status)
            delay = max((60, 300, 900, 1800)[min(self._auth_failures, 3)], error.retry_after)
            self._auth_failures += 1
            self._auth_retry_at = monotonic() + delay
            self._auth_recovery_pending = True
            self._publish_auth_budget()
            error_type = AuthProtocolError if isinstance(error, AuthProtocolError) else TransientAuthError
            if not result.done():
                result.set_exception(error_type(delay, error.status))
        elif error is not None:
            self._publish_auth_budget()
            if not result.done():
                result.set_exception(error)
        elif self._auth_stopping or self.hass.is_stopping:
            if not result.done():
                result.set_exception(HomeAssistantError("Kia authentication is stopping"))
        elif not result.done():
            self.hass.async_create_task(self._async_finish_auth(result), "Kia auth completion")
        if result.done():
            self._auth_completed(result)

    async def _async_finish_auth(self, result):
        try:
            if not self._auth_stopping and not self.hass.is_stopping:
                await self._async_save_token()
                self._sync_vehicle_enabled_states()
            self._auth_retry_at = 0
            self._auth_failures = 0
            self._auth_protocol_failures = 0
            self._auth_protocol_blocked = None
            self._publish_auth_budget()
            # Recovery remains cache-only until a successful vehicle cache cycle.
        except BaseException as err:
            result.set_exception(err)
        else:
            result.set_result(None)

    async def async_check_and_refresh_token(self, *, _reconnect=False) -> None:
        """Join one coordinator-owned operation; cancellation only leaves it."""
        if self._auth_stopping or self.hass.is_stopping or self._shutdown_requested:
            raise HomeAssistantError("Kia authentication is stopping")
        self._sync_auth_budget()
        if self._auth_entry_id in self._reconnects and not _reconnect:
            raise HomeAssistantError("Kia reconnection is still in progress")
        if self._auth_protocol_blocked is not None and not _reconnect:
            raise AuthProtocolError(1800, self._auth_protocol_blocked.status) from None
        result = self._auth_result
        if result is not None and result.done() and not result.cancelled():
            error = result.exception()
            if isinstance(error, (AuthenticationError, AuthConfigurationError)):
                # A late credentials/OTP/consent failure must reach the next
                # Core consumer, not become a new automatic login attempt.
                raise type(error)(*error.args) from None
        if result is None or result.done():
            previous = self._auth_operations.get(self._auth_entry_id)
            if previous is not None and previous.busy:
                raise HomeAssistantError("Previous Kia authentication is still draining")
            remaining = self._auth_retry_at - monotonic()
            if remaining > 0:
                raise TransientAuthError(remaining)
            operation = AuthOperation(self.hass.loop, self._auth_worker_result)
            self._auth_operation = operation
            result = self._auth_result = operation.result
            self._auth_operations[self._auth_entry_id] = operation
            result.add_done_callback(self._auth_completed)
            try:
                self._auth_worker = self.hass.async_add_executor_job(self._run_auth_worker, operation)
            except Exception:
                operation.abort_queued(HomeAssistantError("Kia authentication executor submission failed"))
            else:
                self._auth_worker.add_done_callback(lambda worker: self._auth_wrapper_done(operation, worker))
        try:
            async with asyncio.timeout(self.AUTH_WAIT_TIMEOUT):
                await asyncio.shield(result)
                if self._auth_stopping or self.hass.is_stopping:
                    raise HomeAssistantError("Kia authentication is stopping")
        except TimeoutError as err:
            # Ownership persists and late results are still booked. Do not start
            # a second worker or infer that the original operation did not run.
            raise HomeAssistantError("Kia authentication is still in progress") from err

    @callback
    def _sync_vehicle_enabled_states(self) -> None:
        """Stop polling vehicles whose device the user disabled in HA."""
        registry = dr.async_get(self.hass)
        for vehicle_id, vehicle in self.vehicle_manager.vehicles.items():
            identifiers = {(DOMAIN, vehicle_id)}
            if vehicle.VIN:
                identifiers.add((DOMAIN, f"vin:{vehicle.VIN}"))
            device = registry.async_get_device(identifiers=identifiers)
            if device is not None and device.disabled:
                self.vehicle_manager.disable_vehicle(vehicle_id)
            else:
                self.vehicle_manager.enable_vehicle(vehicle_id)

    async def async_await_action_and_refresh(
        self, vehicle_id: str, action_id: str
    ) -> None:
        """Wait for a vehicle action to complete, then refresh the cached data."""
        try:
            await asyncio.sleep(5)
            await self.hass.async_add_executor_job(
                self.vehicle_manager.check_action_status,
                vehicle_id,
                action_id,
                True,
                60,
            )
        finally:
            await self.async_refresh()

    async def async_await_action_and_force_refresh(
        self, vehicle_id: str, action_id: str
    ) -> None:
        """Wait for action then force refresh to get fresh vehicle data.

        Used after setting charge limits because the soft refresh (cmm/gvi)
        does not return targetSOC for some vehicles. A force refresh (rems/rvs)
        ensures the fresh charge limits are read back immediately.

        Uses async_set_updated_data instead of async_refresh to avoid a
        redundant cmm/gvi API call — the force refresh already updates the
        vehicle objects in-place (rems/rvs + cmm/gvi), so we just need to
        notify HA entities to re-read their state.
        """
        try:
            await asyncio.sleep(5)
            await self.hass.async_add_executor_job(
                self.vehicle_manager.check_action_status,
                vehicle_id,
                action_id,
                True,
                60,
            )
        finally:
            try:
                await self.hass.async_add_executor_job(
                    self.vehicle_manager.force_refresh_vehicle_state, vehicle_id
                )
            except Exception:
                _LOGGER.exception("Force refresh after call failed")
            self.async_set_updated_data(self.data)

    async def _async_send_action(
        self,
        vehicle_id: str,
        action_fn: Callable[[], Any],
        error_label: str,
        *,
        force_refresh: bool = False,
        send_guard=None,
    ) -> None:
        """Send a vehicle action, wait for completion, and refresh data.

        Serializes actions with a lock to prevent DuplicateRequestError
        from the Hyundai API when commands overlap. If another action is
        already in progress, raises HomeAssistantError immediately so
        the user gets a clear message instead of a mysterious long wait.
        """
        if self._action_lock.locked():
            _LOGGER.warning(
                "Vehicle action '%s' rejected: another action is already in progress",
                error_label,
            )
            raise HomeAssistantError(
                "Another vehicle action is in progress. "
                "Please wait for it to complete and try again."
            )
        async with self._action_lock:
            await self.async_check_and_refresh_token()
            if self._auth_recovery_pending:
                raise HomeAssistantError("Cache recovery is required before vehicle commands")
            try:
                if send_guard is None:
                    action_id = await self.hass.async_add_executor_job(action_fn)
                else:
                    from custom_components.kia_uvo._vendor.hyundai_kia_connect_api.command_gate import command_scope
                    send_guard.validate()  # after authentication, before executor queue
                    def guarded_action():
                        with command_scope(send_guard.gate):
                            return action_fn()
                    task = asyncio.ensure_future(self.hass.async_add_executor_job(guarded_action))
                    try:
                        action_id = await asyncio.shield(task)
                    except asyncio.CancelledError:
                        send_guard.close()  # revoke a worker that has not sent yet
                        await asyncio.gather(task, return_exceptions=True)
                        raise
            except UnsupportedControlError as err:
                raise HomeAssistantError(
                    f"Vehicle does not support this action: {err}"
                ) from err
            except Exception as err:
                raise HomeAssistantError(f"Failed to {error_label}: {err}") from err
            try:
                if force_refresh:
                    await self.async_await_action_and_force_refresh(
                        vehicle_id, action_id
                    )
                else:
                    await self.async_await_action_and_refresh(vehicle_id, action_id)
            except Exception:
                _LOGGER.exception(
                    "Action '%s' was sent but confirmation polling failed",
                    error_label,
                )

    async def async_lock_vehicle(self, vehicle_id: str) -> None:
        """Lock the vehicle."""
        await self._async_send_action(
            vehicle_id,
            lambda: self.vehicle_manager.lock(vehicle_id),
            "lock vehicle",
        )

    async def async_unlock_vehicle(self, vehicle_id: str) -> None:
        """Unlock the vehicle."""
        await self._async_send_action(
            vehicle_id,
            lambda: self.vehicle_manager.unlock(vehicle_id),
            "unlock vehicle",
        )

    async def async_open_charge_port(self, vehicle_id: str) -> None:
        """Open the vehicle's charge port."""
        await self._async_send_action(
            vehicle_id,
            lambda: self.vehicle_manager.open_charge_port(vehicle_id),
            "open charge port",
        )

    async def async_close_charge_port(self, vehicle_id: str) -> None:
        """Close the vehicle's charge port."""
        await self._async_send_action(
            vehicle_id,
            lambda: self.vehicle_manager.close_charge_port(vehicle_id),
            "close charge port",
        )

    async def async_start_climate_default(self, vehicle_id: str) -> None:
        """Start climate with default options (API fills sensible defaults)."""
        await self.async_start_climate(vehicle_id, ClimateRequestOptions())

    async def async_start_climate(
        self, vehicle_id: str, climate_options: ClimateRequestOptions
    ) -> None:
        """Start climate control with the given options."""
        await self._async_send_action(
            vehicle_id,
            lambda: self.vehicle_manager.start_climate(vehicle_id, climate_options),
            "start climate",
        )

    async def async_stop_climate(self, vehicle_id: str) -> None:
        """Stop climate control."""
        await self._async_send_action(
            vehicle_id,
            lambda: self.vehicle_manager.stop_climate(vehicle_id),
            "stop climate",
        )

    async def async_start_charge(self, vehicle_id: str) -> None:
        """Start charging."""
        await self._async_send_action(
            vehicle_id,
            lambda: self.vehicle_manager.start_charge(vehicle_id),
            "start charge",
        )

    async def async_stop_charge(self, vehicle_id: str) -> None:
        """Stop charging."""
        await self._async_send_action(
            vehicle_id,
            lambda: self.vehicle_manager.stop_charge(vehicle_id),
            "stop charge",
        )

    async def async_set_charge_limits(self, vehicle_id: str, ac: int, dc: int) -> None:
        """Set the AC and DC charge limits (percent)."""
        await self._async_send_action(
            vehicle_id,
            lambda: self.vehicle_manager.set_charge_limits(vehicle_id, ac, dc),
            "set charge limits",
        )

    async def async_set_charging_current(self, vehicle_id: str, level: int) -> None:
        """Set the AC charging current level."""
        await self._async_send_action(
            vehicle_id,
            lambda: self.vehicle_manager.set_charging_current(vehicle_id, level),
            "set charging current",
        )

    async def async_schedule_charging_and_climate(
        self, vehicle_id: str, schedule_options: ScheduleChargingClimateRequestOptions
    ) -> None:
        """Set the charging and climate schedule."""
        await self._async_send_action(
            vehicle_id,
            lambda: self.vehicle_manager.schedule_charging_and_climate(
                vehicle_id, schedule_options
            ),
            "schedule charging and climate",
        )

    def _build_schedule_options_from_vehicle(
        self, vehicle: Vehicle
    ) -> ScheduleChargingClimateRequestOptions:
        """Build schedule options from current vehicle state for partial updates."""
        return ScheduleChargingClimateRequestOptions(
            first_departure=ScheduleChargingClimateRequestOptions.DepartureOptions(
                enabled=vehicle.ev_first_departure_enabled or False,
                days=vehicle.ev_first_departure_days or [0],
                time=vehicle.ev_first_departure_time or dt.time(),
            ),
            second_departure=ScheduleChargingClimateRequestOptions.DepartureOptions(
                enabled=vehicle.ev_second_departure_enabled or False,
                days=vehicle.ev_second_departure_days or [0],
                time=vehicle.ev_second_departure_time or dt.time(),
            ),
            charging_enabled=vehicle.ev_schedule_charge_enabled or False,
            off_peak_start_time=vehicle.ev_off_peak_start_time or dt.time(),
            off_peak_end_time=vehicle.ev_off_peak_end_time or dt.time(),
            off_peak_charge_only_enabled=vehicle.ev_off_peak_charge_only_enabled
            or False,
            climate_enabled=vehicle.ev_first_departure_climate_enabled or False,
            temperature=vehicle.ev_first_departure_climate_temperature or 21.0,
            defrost=vehicle.ev_first_departure_climate_defrost or False,
        )

    async def async_set_schedule_charge_enabled(
        self, vehicle_id: str, enabled: bool
    ) -> None:
        """Toggle scheduled charging on/off."""
        vehicle = self.vehicle_manager.vehicles[vehicle_id]
        options = self._build_schedule_options_from_vehicle(vehicle)
        options.charging_enabled = enabled
        await self.async_schedule_charging_and_climate(vehicle_id, options)

    async def async_set_off_peak_charge_only_enabled(
        self, vehicle_id: str, enabled: bool
    ) -> None:
        """Toggle off-peak charge only on/off."""
        vehicle = self.vehicle_manager.vehicles[vehicle_id]
        options = self._build_schedule_options_from_vehicle(vehicle)
        options.off_peak_charge_only_enabled = enabled
        await self.async_schedule_charging_and_climate(vehicle_id, options)

    async def async_set_off_peak_charging(
        self,
        vehicle_id: str,
        *,
        mode: OffPeakChargingMode | None = None,
        start: dt.time | None = None,
        end: dt.time | None = None,
    ) -> None:
        """Set the off-peak charging schedule mode and/or window.

        ``mode`` maps to the (charging_enabled, off_peak_charge_only_enabled)
        pair the API expects: ``OFF`` disables scheduled charging, ``TIME``
        charges only during the off-peak window (time priority), ``TARGET``
        prefers off-peak tariffs but continues past the window to reach the
        target SoC (target priority). ``mode=None`` preserves the current mode
        and only adjusts the window — used by the time entities. Other
        schedule fields (departures) are preserved.
        """
        vehicle = self.vehicle_manager.vehicles[vehicle_id]
        options = self._build_schedule_options_from_vehicle(vehicle)
        if mode is not None:
            if mode is OffPeakChargingMode.OFF:
                options.charging_enabled = False
            elif mode is OffPeakChargingMode.TIME:
                options.charging_enabled = True
                options.off_peak_charge_only_enabled = True
            elif mode is OffPeakChargingMode.TARGET:
                options.charging_enabled = True
                options.off_peak_charge_only_enabled = False
        if start is not None:
            options.off_peak_start_time = start
        if end is not None:
            options.off_peak_end_time = end
        await self.async_schedule_charging_and_climate(vehicle_id, options)

    async def async_set_departure_enabled(
        self, vehicle_id: str, departure_num: int, enabled: bool
    ) -> None:
        """Toggle a departure schedule on/off."""
        vehicle = self.vehicle_manager.vehicles[vehicle_id]
        options = self._build_schedule_options_from_vehicle(vehicle)
        departure = (
            options.first_departure if departure_num == 1 else options.second_departure
        )
        if departure is not None:
            departure.enabled = enabled
        # reservFlag (charging_enabled) must be 1 for departure slots to take
        # effect. If the vehicle doesn't expose ev_schedule_charge_enabled
        # (None), the builder defaults it to False, causing the API to accept
        # the request but ignore per-slot reservChargeSet.
        if enabled and not options.charging_enabled:
            options.charging_enabled = True
        await self.async_schedule_charging_and_climate(vehicle_id, options)

    async def async_set_departure_climate_enabled(
        self, vehicle_id: str, departure_num: int, enabled: bool
    ) -> None:
        """Toggle departure climate on/off."""
        vehicle = self.vehicle_manager.vehicles[vehicle_id]
        options = self._build_schedule_options_from_vehicle(vehicle)
        options.climate_enabled = enabled
        await self.async_schedule_charging_and_climate(vehicle_id, options)

    async def async_set_departure_defrost(
        self, vehicle_id: str, departure_num: int, enabled: bool
    ) -> None:
        """Toggle departure defrost on/off."""
        vehicle = self.vehicle_manager.vehicles[vehicle_id]
        options = self._build_schedule_options_from_vehicle(vehicle)
        options.defrost = enabled
        await self.async_schedule_charging_and_climate(vehicle_id, options)

    async def async_start_hazard_lights(self, vehicle_id: str) -> None:
        """Flash the hazard lights."""
        await self._async_send_action(
            vehicle_id,
            lambda: self.vehicle_manager.start_hazard_lights(vehicle_id),
            "start hazard lights",
        )

    async def async_start_hazard_lights_and_horn(self, vehicle_id: str) -> None:
        """Flash the hazard lights and sound the horn."""
        await self._async_send_action(
            vehicle_id,
            lambda: self.vehicle_manager.start_hazard_lights_and_horn(vehicle_id),
            "start hazard lights and horn",
        )

    async def async_start_valet_mode(self, vehicle_id: str) -> None:
        """Turn on valet mode."""
        await self._async_send_action(
            vehicle_id,
            lambda: self.vehicle_manager.start_valet_mode(vehicle_id),
            "start valet mode",
        )

    async def async_stop_valet_mode(self, vehicle_id: str) -> None:
        """Turn off valet mode."""
        await self._async_send_action(
            vehicle_id,
            lambda: self.vehicle_manager.stop_valet_mode(vehicle_id),
            "stop valet mode",
        )

    async def async_set_v2l_limit(self, vehicle_id: str, limit: int) -> None:
        """Set the vehicle-to-load discharge limit (percent)."""
        await self._async_send_action(
            vehicle_id,
            lambda: self.vehicle_manager.set_vehicle_to_load_discharge_limit(
                vehicle_id, limit
            ),
            "set V2L limit",
        )

    async def async_set_windows(
        self, vehicle_id: str, windowOptions: WindowRequestOptions
    ) -> None:
        """Set the state of one or more windows."""
        await self._async_send_action(
            vehicle_id,
            lambda: self.vehicle_manager.set_windows_state(vehicle_id, windowOptions),
            "set windows",
        )

    async def async_set_navigation(
        self, vehicle_id: str, poi_list: list[POIInfo]
    ) -> None:
        """Send points of interest to the vehicle's navigation."""
        await self._async_send_action(
            vehicle_id,
            lambda: self.vehicle_manager.set_navigation(vehicle_id, poi_list),
            "set navigation",
        )

    async def async_open_all_windows(self, vehicle_id: str) -> None:
        """Open all four windows."""
        options = WindowRequestOptions(
            front_left=WINDOW_STATE.OPEN,
            front_right=WINDOW_STATE.OPEN,
            back_left=WINDOW_STATE.OPEN,
            back_right=WINDOW_STATE.OPEN,
        )
        await self._async_send_action(
            vehicle_id,
            lambda: self.vehicle_manager.set_windows_state(vehicle_id, options),
            "open all windows",
        )

    async def async_close_all_windows(self, vehicle_id: str) -> None:
        """Close all four windows."""
        options = WindowRequestOptions(
            front_left=WINDOW_STATE.CLOSED,
            front_right=WINDOW_STATE.CLOSED,
            back_left=WINDOW_STATE.CLOSED,
            back_right=WINDOW_STATE.CLOSED,
        )
        await self._async_send_action(
            vehicle_id,
            lambda: self.vehicle_manager.set_windows_state(vehicle_id, options),
            "close all windows",
        )

    async def async_vent_all_windows(self, vehicle_id: str) -> None:
        """Put all four windows in the ventilation position."""
        options = WindowRequestOptions(
            front_left=WINDOW_STATE.VENTILATION,
            front_right=WINDOW_STATE.VENTILATION,
            back_left=WINDOW_STATE.VENTILATION,
            back_right=WINDOW_STATE.VENTILATION,
        )
        await self._async_send_action(
            vehicle_id,
            lambda: self.vehicle_manager.set_windows_state(vehicle_id, options),
            "vent all windows",
        )

    async def _async_save_token(self) -> None:
        """Persist the latest token into the config entry."""
        config_entry = self.config_entry
        assert config_entry is not None
        new_token = self.vehicle_manager.token.to_dict()
        # Only update if token actually changed
        if new_token and new_token != config_entry.data.get(CONF_TOKEN):
            updated_data = {**config_entry.data, CONF_TOKEN: new_token}
            self.hass.config_entries.async_update_entry(config_entry, data=updated_data)
