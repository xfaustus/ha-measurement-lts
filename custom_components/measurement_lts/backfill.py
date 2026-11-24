"""Read historical states and insert missing hourly statistics."""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from itertools import chain
import json
from math import isfinite
from time import time

from sqlalchemy import delete, func, select, update
from sqlalchemy.orm import Session

from homeassistant.components.recorder import get_instance, is_entity_recorded
from homeassistant.components.recorder.core import Recorder
from homeassistant.components.recorder.db_schema import (
    StateAttributes,
    States,
    StatesMeta,
    Statistics,
    StatisticsMeta,
    StatisticsShortTerm,
)
from homeassistant.components.recorder.tasks import PurgeEntitiesTask, RecorderTask
from homeassistant.core import HomeAssistant, State
from homeassistant.exceptions import HomeAssistantError

from .aggregate import HOUR, Hour, Sample, combine_hours, five_minute, matches
from .control import Cancellation


def eligible(hass: HomeAssistant, state: State) -> bool:
    """Limit backfill to currently recorded measurement sensors."""
    return (
        state.domain == "sensor"
        and state.attributes.get("state_class") == "measurement"
        and is_entity_recorded(hass, state.entity_id)
    )


@dataclass(frozen=True)
class Request:
    """Scope of a single backfill."""

    entity_id: str
    unit: str | None
    start: int
    end: int
    include_short_term: bool = False
    purge_keep_days: int | None = None


@dataclass(frozen=True)
class Plan:
    """Hourly data and comparison against Recorder."""

    candidates: dict[int, Hour]
    missing: dict[int, Hour]
    identical: int
    conflicts: dict[int, tuple[float | None, float | None, float | None]]
    occupied_without_candidate: int
    short_missing: dict[int, tuple[float, float, float]]
    short_existing: int
    short_expired: int
    short_to_purge: int



@lru_cache(maxsize=512)
def _recorded_unit(attributes: str | None):
    try:
        return json.loads(attributes or "{}").get("unit_of_measurement")
    except (TypeError, ValueError, AttributeError):
        return None


def _sample(row, unit: str | None) -> Sample:
    """Skip nonnumeric readings and incompatible historical units."""
    timestamp, value, attributes, changed = row
    significant = changed is None or changed == timestamp
    try:
        number = float(value)
    except (TypeError, ValueError):
        return timestamp, None, significant
    valid = isfinite(number) and _recorded_unit(attributes) == unit
    return timestamp, number if valid else None, significant


def _short_before(metadata_id: int, cutoff: float):
    """Limit short-term cleanup to the reviewed statistic and cutoff."""
    return (
        StatisticsShortTerm.metadata_id == metadata_id,
        StatisticsShortTerm.start_ts < cutoff,
    )


def _build(
    session: Session, request: Request, short_cutoff: float, purge_cutoff: float | None,
    cancellation: Cancellation | None = None,
) -> tuple[int, Plan]:
    metadata = session.scalar(
        select(StatisticsMeta).where(StatisticsMeta.statistic_id == request.entity_id)
    )
    if metadata is None:
        raise HomeAssistantError("missing_metadata")
    if (
        metadata.source != "recorder"
        or metadata.unit_of_measurement != request.unit
        or metadata.has_sum
        or int(getattr(metadata, "mean_type", 0) or 0) not in (0, 1)
        or not (getattr(metadata, "mean_type", 0) == 1 or getattr(metadata, "has_mean", False))
    ):
        raise HomeAssistantError("incompatible_metadata")

    states_id = _history_metadata_id(request.entity_id)
    history = (
        select(States.last_updated_ts, States.state, StateAttributes.shared_attrs, States.last_changed_ts)
        .outerjoin(StateAttributes, States.attributes_id == StateAttributes.attributes_id)
        .where(States.metadata_id == states_id)
    )
    predecessor = session.execute(
        history.where(States.last_updated_ts < request.start)
        .order_by(States.last_updated_ts.desc(), States.state_id.desc()).limit(1)
    ).first()
    samples = session.execute(
        history.where(
            States.last_updated_ts >= request.start,
            States.last_updated_ts < request.end,
        ).order_by(States.last_updated_ts, States.state_id)
    ).yield_per(2048)
    checkpoint = cancellation.check if cancellation is not None else None

    def source():
        for index, row in enumerate(chain([predecessor] if predecessor else [], samples)):
            if checkpoint is not None and index % 2048 == 0:
                checkpoint()
            yield _sample(row, request.unit)

    blocks = five_minute(source(), request.start, request.end, checkpoint)
    short_missing = {}
    short_existing = short_expired = 0
    if request.include_short_term:
        block_values = dict(blocks)
        candidates = combine_hours(block_values.items())
        eligible_blocks = {
            start: values for start, values in block_values.items() if start >= short_cutoff
        }
        short_expired = len(block_values) - len(eligible_blocks)
        existing_blocks = set(session.scalars(
            select(StatisticsShortTerm.start_ts).where(
                StatisticsShortTerm.metadata_id == metadata.id,
                StatisticsShortTerm.start_ts >= max(request.start, short_cutoff),
                StatisticsShortTerm.start_ts < request.end,
            )
        ))
        short_existing = len(eligible_blocks.keys() & existing_blocks)
        short_missing = {
            start: values for start, values in eligible_blocks.items() if start not in existing_blocks
        }
    else:
        candidates = combine_hours(blocks)
    existing = {
        int(row.start_ts): (row.mean, row.min, row.max)
        for row in session.execute(
            select(Statistics.start_ts, Statistics.mean, Statistics.min, Statistics.max)
            .where(
                Statistics.metadata_id == metadata.id,
                Statistics.start_ts >= request.start,
                Statistics.start_ts < request.end,
            )
        )
    }
    short_to_purge = 0
    if purge_cutoff is not None:
        short_to_purge = session.scalar(
            select(func.count()).select_from(StatisticsShortTerm)
            .where(*_short_before(metadata.id, purge_cutoff))
        ) or 0
    overlap = candidates.keys() & existing.keys()
    conflicts = {
        hour: existing[hour]
        for hour in overlap if not matches(candidates[hour], existing[hour])
    }
    missing = {hour: candidates[hour] for hour in candidates.keys() - existing.keys()}
    return metadata.id, Plan(
        candidates, missing, len(overlap) - len(conflicts), conflicts,
        len(existing.keys() - candidates.keys()), short_missing, short_existing, short_expired, short_to_purge,
    )


def _resolve(future, value=None, error=None) -> None:
    if future.done():
        return
    if error is not None:
        future.set_exception(error)
    else:
        future.set_result(value)


def _write_plan(session, metadata_id, plan, reviewed, replace_conflicts, purge_cutoff, cancellation):
    """Apply one reviewed sensor inside the batch transaction."""
    if plan.candidates != reviewed.candidates:
        raise HomeAssistantError("history_changed")
    replacements = reviewed.conflicts if replace_conflicts else {}
    if any(plan.conflicts.get(hour) != original for hour, original in replacements.items()):
        raise HomeAssistantError("statistics_changed")

    # Add only hours that were missing in both snapshots.
    starts = sorted(plan.missing.keys() & reviewed.missing.keys())
    for start in starts:
        cancellation.check()
        mean, minimum, maximum = plan.missing[start].values()
        session.add(Statistics.from_stats_ts(metadata_id, {
            "start_ts": start, "mean": mean, "min": minimum, "max": maximum,
        }))
    for start, (old_mean, old_min, old_max) in replacements.items():
        cancellation.check()
        mean, minimum, maximum = plan.candidates[start].values()
        updated = session.execute(
            update(Statistics)
            .where(
                Statistics.metadata_id == metadata_id,
                Statistics.start_ts == start,
                Statistics.mean == old_mean,
                Statistics.min == old_min,
                Statistics.max == old_max,
            )
            .values(mean=mean, min=minimum, max=maximum, mean_weight=None)
            .execution_options(synchronize_session=False)
        )
        if updated.rowcount != 1:
            raise HomeAssistantError("statistics_changed")
    short_starts = sorted(plan.short_missing.keys() & reviewed.short_missing.keys())
    for start in short_starts:
        cancellation.check()
        if plan.short_missing[start] != reviewed.short_missing[start]:
            raise HomeAssistantError("history_changed")
        mean, minimum, maximum = plan.short_missing[start]
        session.add(StatisticsShortTerm.from_stats_ts(metadata_id, {
            "start_ts": start, "mean": mean, "min": minimum, "max": maximum,
        }))
    short_purged = 0
    if purge_cutoff is not None:
        short_purged = session.execute(
            delete(StatisticsShortTerm)
            .where(*_short_before(metadata_id, purge_cutoff))
            .execution_options(synchronize_session=False)
        ).rowcount
    return (len(starts), len(replacements), len(short_starts), short_purged)


@dataclass
class _BackfillTask(RecorderTask):
    requests: tuple[Request, ...]
    future: asyncio.Future
    reviewed: dict[str, Plan] | None
    replace_conflicts: bool
    cancellation: Cancellation
    on_progress: Callable[[str | None, int], None] | None = None

    def run(self, instance: Recorder) -> None:
        """Commit the selected sensors atomically and queue their detailed-history purge."""
        loop = instance.hass.loop
        now = time()
        days = self.requests[0].purge_keep_days
        purge_cutoff = now - days * 86400 if days is not None else None
        global_cutoff = now - instance.keep_days * 86400
        short_cutoff = max(global_cutoff, purge_cutoff) if purge_cutoff is not None else global_cutoff
        try:
            result = {}
            with instance.get_session() as session, session.begin():
                for completed, request in enumerate(self.requests):
                    self.cancellation.check()
                    if self.on_progress is not None:
                        loop.call_soon_threadsafe(self.on_progress, request.entity_id, completed)
                    metadata_id, plan = _build(session, request, short_cutoff, purge_cutoff, self.cancellation)
                    result[request.entity_id] = (
                        plan if self.reviewed is None else _write_plan(
                            session, metadata_id, plan, self.reviewed[request.entity_id],
                            self.replace_conflicts, purge_cutoff, self.cancellation,
                        )
                    )
                self.cancellation.begin_commit()
            if self.reviewed is not None and purge_cutoff is not None:
                entity_ids = frozenset(request.entity_id for request in self.requests)
                instance.queue_task(PurgeEntitiesTask(
                    entity_ids.__contains__,
                    datetime.fromtimestamp(purge_cutoff, timezone.utc),
                ))
        except Exception as exc:
            loop.call_soon_threadsafe(_resolve, self.future, None, exc)
        else:
            if self.on_progress is not None:
                loop.call_soon_threadsafe(self.on_progress, None, len(self.requests))
            loop.call_soon_threadsafe(_resolve, self.future, result)


def _preview_batch(recorder: Recorder, requests: tuple[Request, ...], on_progress=None, cancellation=None):
    now = time()
    cutoff = now - recorder.keep_days * 86400
    days = requests[0].purge_keep_days
    purge_cutoff = now - days * 86400 if days is not None else None
    short_cutoff = max(cutoff, purge_cutoff) if purge_cutoff is not None else cutoff
    plans = {}
    skipped = {}
    with recorder.get_session() as session:
        for completed, request in enumerate(requests):
            if cancellation is not None:
                cancellation.check()
            if on_progress is not None:
                recorder.hass.loop.call_soon_threadsafe(on_progress, request.entity_id, completed)
            try:
                _, plan = _build(session, request, short_cutoff, purge_cutoff, cancellation)
            except HomeAssistantError as err:
                if str(err) not in {"missing_metadata", "incompatible_metadata"}:
                    raise
                skipped[request.entity_id] = str(err)
            else:
                plans[request.entity_id] = plan

    if on_progress is not None:
        recorder.hass.loop.call_soon_threadsafe(on_progress, None, len(requests))
    return plans, skipped


async def run(
    hass: HomeAssistant,
    requests: tuple[Request, ...],
    reviewed: dict[str, Plan] | None = None,
    *,
    replace_conflicts: bool = False,
    on_progress=None,
    cancellation: Cancellation | None = None,
):
    """Queue one transaction for the selected measurement sensors."""
    for request in requests:
        state = hass.states.get(request.entity_id)
        if (
            state is None
            or not eligible(hass, state)
            or state.attributes.get("unit_of_measurement") != request.unit
        ):
            raise HomeAssistantError("invalid_entity")
    recorder = get_instance(hass)
    if not recorder.is_alive():
        raise HomeAssistantError("recorder_unavailable")
    cancellation = cancellation or Cancellation()
    cancellation.check()
    if reviewed is None:
        result = await recorder.async_add_executor_job(
            _preview_batch, recorder, requests, on_progress, cancellation,
        )
        cancellation.check()
        return result
    future = hass.loop.create_future()
    recorder.queue_task(_BackfillTask(requests, future, reviewed, replace_conflicts, cancellation, on_progress))
    return await future


def _history_metadata_id(entity_id: str):
    return select(StatesMeta.metadata_id).where(
        StatesMeta.entity_id == entity_id
    ).scalar_subquery()


def _first_state(session: Session, entity_id: str) -> float | None:
    return session.scalar(
        select(States.last_updated_ts)
        .where(
            States.metadata_id == _history_metadata_id(entity_id),
            States.last_updated_ts.is_not(None),
        )
        .order_by(States.last_updated_ts)
        .limit(1)
    )


def _first_history_timestamps(recorder: Recorder, entity_ids: tuple[str, ...]):
    with recorder.get_session() as session:
        return {entity_id: _first_state(session, entity_id) for entity_id in entity_ids}


async def first_history_timestamps(hass: HomeAssistant, entity_ids: tuple[str, ...]):
    """Find the first retained state for each sensor using Recorder's timestamp index."""
    recorder = get_instance(hass)
    return await recorder.async_add_executor_job(_first_history_timestamps, recorder, entity_ids)


def _find_backfill_sensors(recorder: Recorder, entity_ids: tuple[str, ...]) -> list[str]:
    """Find retained history before the first saved hourly statistic."""
    found = []
    with recorder.get_session() as session:
        for entity_id in entity_ids:
            first_state = _first_state(session, entity_id)
            if first_state is None:
                continue
            metadata_id = select(StatisticsMeta.id).where(
                StatisticsMeta.statistic_id == entity_id
            ).scalar_subquery()
            first_lts = session.scalar(
                select(Statistics.start_ts)
                .where(Statistics.metadata_id == metadata_id, Statistics.start_ts.is_not(None))
                .order_by(Statistics.start_ts)
                .limit(1)
            )
            if first_lts is None or first_state // HOUR < first_lts // HOUR:
                found.append(entity_id)
    return found


async def find_backfill_sensors(hass: HomeAssistant, entity_ids: tuple[str, ...]) -> list[str]:
    """Search the selected candidates without writing to Recorder."""
    recorder = get_instance(hass)
    return await recorder.async_add_executor_job(_find_backfill_sensors, recorder, entity_ids)
