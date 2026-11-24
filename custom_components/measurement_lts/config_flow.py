"""Run measurement statistics maintenance from the Home Assistant UI."""

from datetime import date, datetime, time, timedelta
import logging

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import selector
from homeassistant.util import dt as dt_util

from .aggregate import HOUR
from .backfill import Request, eligible, find_backfill_sensors, first_history_timestamps, run
from .control import BackfillCancelled, Cancellation, active_job, start_job

DOMAIN = "measurement_lts"
_LOGGER = logging.getLogger(__name__)


class ConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Create the integration entry."""

    VERSION = 1

    async def async_step_user(self, user_input=None):
        if self._async_current_entries():
            return self.async_abort(reason="already_configured")
        return self.async_create_entry(title="Measurement LTS Backfill", data={})

    @staticmethod
    def async_get_options_flow(config_entry):
        return BackfillOptions()


class BackfillOptions(config_entries.OptionsFlow):
    """Choose a measurement, preview and optionally apply."""

    def _eligible_entities(self) -> tuple[str, ...]:
        return tuple(sorted(
            state.entity_id
            for state in self.hass.states.async_all("sensor")
            if eligible(self.hass, state)
        ))

    async def async_step_init(self, user_input=None):
        if active_job(self.hass) is not None:
            return self.async_abort(reason="operation_running")
        if not self._eligible_entities():
            return self.async_abort(reason="no_sensors")
        return self.async_show_menu(step_id="init", menu_options=["select", "find"])

    async def async_step_find(self, user_input=None):
        try:
            found = await find_backfill_sensors(self.hass, self._eligible_entities())
        except Exception:
            _LOGGER.exception("Could not find sensors with retained history before their LTS")
            return self._select_form(errors={"base": "failed"})
        if not found:
            return self._select_form(errors={"base": "no_backfill_matches"})
        return self._select_form(suggested={"entity_ids": found})

    async def async_step_select(self, user_input=None):
        choices = self._eligible_entities()
        errors = {}
        missing = []
        if user_input is not None:
            entity_ids = tuple(dict.fromkeys(user_input["entity_ids"]))
            if not entity_ids or any(entity_id not in choices for entity_id in entity_ids):
                errors["base"] = "invalid_entity"
            else:
                try:
                    first = await first_history_timestamps(self.hass, entity_ids)
                except Exception:
                    _LOGGER.exception("Could not find Recorder history for %s", entity_ids)
                    errors["base"] = "failed"
                else:
                    missing = [entity_id for entity_id, timestamp in first.items() if timestamp is None]
                    if missing:
                        errors["base"] = "no_history"
                    else:
                        self.entity_ids = entity_ids
                        self.tz = dt_util.get_time_zone(self.hass.config.time_zone)
                        self.first_date = datetime.fromtimestamp(min(first.values()), self.tz).date()
                        return await self.async_step_range()

        return self._select_form(
            suggested=user_input, errors=errors, missing_entities=", ".join(missing),
        )

    def _select_form(self, *, suggested=None, errors=None, missing_entities=""):
        choices = self._eligible_entities()
        if not choices:
            return self.async_abort(reason="no_sensors")
        schema = vol.Schema({
            vol.Required("entity_ids"): selector.EntitySelector(
                selector.EntitySelectorConfig(include_entities=list(choices), multiple=True)
            )
        })
        return self.async_show_form(
            step_id="select",
            data_schema=self.add_suggested_values_to_schema(schema, suggested),
            errors=errors,
            description_placeholders={"missing_entities": missing_entities},
        )

    async def async_step_range(self, user_input=None):
        errors = {}
        today = dt_util.now(self.tz).date()
        if user_input is not None:
            try:
                first = date.fromisoformat(user_input["start_date"])
                last = date.fromisoformat(user_input["end_date"])
                if not self.first_date <= first <= last <= today:
                    raise HomeAssistantError("invalid_dates")

                start = int(datetime.combine(first, time.min, tzinfo=self.tz).timestamp())
                end = int(datetime.combine(last + timedelta(days=1), time.min, tzinfo=self.tz).timestamp())
                start = -(-start // HOUR) * HOUR
                end = min(end, int(dt_util.utcnow().timestamp())) // HOUR * HOUR
                if start >= end:
                    raise HomeAssistantError("invalid_dates")

                days = float(user_input["keep_days"])
                purge = user_input["purge"]
                if not days.is_integer() or not 0 <= days <= 365000:
                    raise HomeAssistantError("invalid_keep_days")
                requests = []
                for entity_id in self.entity_ids:
                    state = self.hass.states.get(entity_id)
                    if state is None or not eligible(self.hass, state):
                        raise HomeAssistantError("invalid_entity")
                    requests.append(Request(
                        entity_id, state.attributes.get("unit_of_measurement"),
                        start, end,
                        include_short_term=user_input["include_short_term"],
                        purge_keep_days=int(days) if purge else None,
                    ))
                self.requests = tuple(requests)
                self._start_work(self.requests)
                return await self.async_step_preview_work()
            except HomeAssistantError as exc:
                errors["base"] = str(exc)
            except Exception:
                _LOGGER.exception("Could not preview the LTS backfill")
                errors["base"] = "failed"

        schema = vol.Schema({
            vol.Required("start_date", default=self.first_date.isoformat()): selector.DateSelector(),
            vol.Required("end_date", default=today.isoformat()): selector.DateSelector(),
            vol.Required("include_short_term", default=False): selector.BooleanSelector(),
            vol.Required("purge", default=False): selector.BooleanSelector(),
            vol.Required("keep_days", default=30): selector.NumberSelector(
                selector.NumberSelectorConfig(min=0, max=365000, step=1, mode=selector.NumberSelectorMode.BOX)
            ),
        })
        return self.async_show_form(
            step_id="range", data_schema=self.add_suggested_values_to_schema(schema, user_input),
            description_placeholders={
                "count": str(len(self.entity_ids)),
                "entity": ", ".join(self.entity_ids),
                "first": self.first_date.isoformat(),
            },
            errors=errors,
        )

    def _start_work(self, requests, reviewed=None, *, replace_conflicts=False):
        if active_job(self.hass) is not None:
            raise HomeAssistantError("operation_running")
        self._work_entity_id = requests[0].entity_id
        self._work_total = len(requests)
        self._work_completed = 0
        self._work_refresh = None
        cancellation = Cancellation()
        self._job = start_job(self.hass, cancellation, run(
            self.hass, requests, reviewed,
            replace_conflicts=replace_conflicts,
            on_progress=self._work_sensor_changed,
            cancellation=cancellation,
        ), self.async_notify_flow_changed)
        self._work_task = self.hass.async_create_task(self._job.wait())

    def _work_sensor_changed(self, entity_id, completed):
        """Accept a sensor update from Recorder on the Home Assistant loop."""
        if self._work_task.done():
            return
        self._work_completed = completed
        if entity_id is not None:
            self._work_entity_id = entity_id
        if self._work_refresh is None:
            self._work_refresh = self.hass.loop.call_later(0.25, self._refresh_work)

    def _refresh_work(self):
        """Coalesce UI refreshes when several small sensors finish together."""
        self._work_refresh = None
        if not self._work_task.done():
            self.async_notify_flow_changed()

    def _publish_work_percent(self):
        if not self._work_task.done():
            update = getattr(self, "async_update_progress", None)
            if update is not None:
                update(self._work_completed / self._work_total)

    def _work_progress(self, action, next_step):
        if self._work_task.done():
            return self.async_show_progress_done(next_step_id=next_step)

        entity_id = self._work_entity_id
        state = self.hass.states.get(entity_id)
        sensor_name = state.name if state is not None else entity_id
        self.hass.loop.call_soon(self._publish_work_percent)
        return self.async_show_progress(
            step_id=f"{action}_work",
            progress_action="cancelling" if self._job.cancellation.requested else action,
            progress_task=self._work_task,
            description_placeholders={
                "sensor": sensor_name,
                "entity_id": entity_id,
                "completed": str(self._work_completed),
                "total": str(self._work_total),
                "percent": str(self._work_completed * 100 // self._work_total),
            },
        )

    async def async_step_preview_work(self, user_input=None):
        return self._work_progress("preview", "preview_result")

    async def async_step_preview_result(self, user_input=None):
        try:
            self.plans, self.skipped = self._work_task.result()
        except BackfillCancelled:
            return self.async_abort(reason="work_cancelled")
        except Exception:
            _LOGGER.exception("Could not preview the LTS backfill")
            return self.async_abort(reason="failed")
        if not self.plans:
            return self.async_abort(reason="no_ready_sensors", description_placeholders=self._skipped())
        self.requests = tuple(r for r in self.requests if r.entity_id in self.plans)
        return await self.async_step_confirm()

    def _skipped(self):
        return {
            "not_ready": ", ".join(entity_id for entity_id, why in self.skipped.items()
                                   if why == "missing_metadata") or "\u2014",
            "incompatible": ", ".join(entity_id for entity_id, why in self.skipped.items()
                                      if why == "incompatible_metadata") or "\u2014",
        }

    async def async_step_confirm(self, user_input=None):
        request = self.requests[0]
        purge = request.purge_keep_days is not None
        conflicts = any(plan.conflicts for plan in self.plans.values())
        interval_hours = (request.end - request.start) // HOUR
        details = {
            "count": str(len(self.requests)),
            "keep_days": str(request.purge_keep_days or 0),
            "rows": self._preview_rows(interval_hours),
            "entity": ", ".join(self.plans),
        } | self._skipped()
        # Keep older browser translations usable until they refresh.
        plans = self.plans.values()
        details.update({
            "candidates": str(sum(len(p.candidates) for p in plans)),
            "missing": str(sum(len(p.missing) for p in plans)),
            "matched": str(sum(p.identical for p in plans)),
            "different": str(sum(len(p.conflicts) for p in plans)),
            "occupied": str(sum(p.occupied_without_candidate for p in plans)),
            "gaps": str(sum(interval_hours - len(p.candidates) for p in plans)),
            "short_missing": str(sum(len(p.short_missing) for p in plans)),
            "short_existing": str(sum(p.short_existing for p in plans)),
            "short_expired": str(sum(p.short_expired for p in plans)),
            "short_to_purge": str(sum(p.short_to_purge for p in plans)),
        })
        if user_input is not None:
            if not user_input["apply"]:
                return self.async_abort(reason="cancelled")
            replace_conflicts = bool(conflicts and user_input.get("replace_conflicts"))
            if not (replace_conflicts or purge or any(p.missing or p.short_missing for p in self.plans.values())):
                return self.async_abort(reason="nothing_to_add", description_placeholders=details)
            requests = tuple(
                request for request in self.requests
                if purge or self.plans[request.entity_id].missing
                or self.plans[request.entity_id].short_missing
                or (replace_conflicts and self.plans[request.entity_id].conflicts)
            )
            try:
                self._start_work(requests, self.plans, replace_conflicts=replace_conflicts)
            except HomeAssistantError:
                return self.async_abort(reason="operation_running")
            return await self.async_step_apply_work()
        fields = {}
        if conflicts:
            fields[vol.Required("replace_conflicts", default=False)] = selector.BooleanSelector()
        fields[vol.Required("apply", default=False)] = selector.BooleanSelector()
        return self.async_show_form(
            step_id="confirm_purge" if purge else "confirm",
            description_placeholders=details,
            data_schema=vol.Schema(fields),
        )

    async def async_step_apply_work(self, user_input=None):
        return self._work_progress("apply", "apply_result")

    async def async_step_apply_result(self, user_input=None):
        try:
            applied = self._work_task.result()
        except BackfillCancelled:
            return self.async_abort(reason="work_cancelled")
        except HomeAssistantError as err:
            reason = str(err) if str(err) in {"history_changed", "statistics_changed"} else "failed"
            return self.async_abort(reason=reason)
        except Exception:
            _LOGGER.exception("Could not apply the LTS backfill")
            return self.async_abort(reason="failed")
        added, replaced, short_added, short_purged = map(sum, zip(*applied.values(), strict=True))
        days = self.requests[0].purge_keep_days
        return self.async_abort(
            reason="purge_queued" if days is not None else "completed",
            description_placeholders={
                "added": str(added), "replaced": str(replaced), "short_added": str(short_added),
                "short_purged": str(short_purged), "keep_days": str(days or 0),
            } | self._skipped(),
        )

    def _preview_rows(self, interval_hours):
        rows = []
        for entity_id, plan in self.plans.items():
            rows.append(
                f"| {entity_id} | {len(plan.missing)} | {plan.identical} | {len(plan.conflicts)} | "
                f"{plan.occupied_without_candidate} | {interval_hours - len(plan.candidates)} | "
                f"{len(plan.short_missing)} | {plan.short_existing} | {plan.short_expired} | {plan.short_to_purge} |"
            )
        return "\n".join(rows)

    async def async_step_confirm_purge(self, user_input=None):
        """Use the same confirmation handler for a preview that includes purge."""
        return await self.async_step_confirm(user_input)

    def async_remove(self):
        """Request a safe cancellation when the user closes an active flow."""
        job = getattr(self, "_job", None)
        if job is not None and not job.task.done():
            job.cancel(notify=False)
        refresh = getattr(self, "_work_refresh", None)
        if refresh is not None:
            refresh.cancel()
