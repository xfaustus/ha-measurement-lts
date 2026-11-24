"""Measurement LTS Backfill integration."""

from functools import partial
import logging

from homeassistant.core import HomeAssistant, ServiceCall, SupportsResponse
from homeassistant.helpers.service import async_register_admin_service

from .control import BackfillCancelled, active_job

DOMAIN = "measurement_lts"
_LOGGER = logging.getLogger(__name__)


async def _cancel(hass: HomeAssistant, call: ServiceCall):
    job = active_job(hass)
    if job is None:
        return {"status": "idle"}
    if not job.cancel():
        return {"status": "too_late"}
    try:
        await job.wait()
    except BackfillCancelled:
        return {"status": "cancelled"}
    except Exception:
        _LOGGER.exception("Measurement LTS operation failed while being cancelled")
        return {"status": "failed"}
    return {"status": "finished"}


async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    """Register the administrator cancellation action."""
    async_register_admin_service(
        hass, DOMAIN, "cancel", partial(_cancel, hass), supports_response=SupportsResponse.ONLY,
    )
    return True


async def async_setup_entry(hass: HomeAssistant, entry) -> bool:
    return True


async def async_unload_entry(hass: HomeAssistant, entry) -> bool:
    return True
