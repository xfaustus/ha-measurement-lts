"""Cooperative cancellation shared by the options flow and Recorder."""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from threading import Event, Lock


ACTIVE_JOB = "measurement_lts_active_job"
_CANCELLED = object()


class BackfillCancelled(Exception):
    """Recorder reached a safe cancellation point."""


class Cancellation:
    """Serialize cancellation with the handoff to database commit."""

    def __init__(self) -> None:
        self._event = Event()
        self._lock = Lock()
        self._committing = False

    @property
    def requested(self) -> bool:
        return self._event.is_set()

    def request(self) -> bool:
        with self._lock:
            if self._committing:
                return False
            self._event.set()
            return True

    def check(self) -> None:
        if self.requested:
            raise BackfillCancelled

    def begin_commit(self) -> None:
        with self._lock:
            self.check()
            self._committing = True


@dataclass
class Job:
    """Keep the operation alive until Recorder acknowledges completion."""

    cancellation: Cancellation
    task: asyncio.Task
    on_cancel: Callable[[], None]

    def cancel(self, *, notify: bool = True) -> bool:
        already_requested = self.cancellation.requested
        if not self.cancellation.request():
            return False
        if notify and not already_requested:
            self.on_cancel()
        return True

    async def wait(self):
        result = await asyncio.shield(self.task)
        if result is _CANCELLED:
            raise BackfillCancelled
        return result


def active_job(hass) -> Job | None:
    job = hass.data.get(ACTIVE_JOB)
    return job if job is not None and not job.task.done() else None


def start_job(hass, cancellation: Cancellation, coro, on_cancel) -> Job:
    async def execute():
        try:
            return await coro
        except BackfillCancelled:
            return _CANCELLED

    task = hass.async_create_task(execute())
    job = Job(cancellation, task, on_cancel)
    hass.data[ACTIVE_JOB] = job

    def finished(done):
        if hass.data.get(ACTIVE_JOB) is job:
            hass.data.pop(ACTIVE_JOB)
        if not done.cancelled():
            done.exception()

    task.add_done_callback(finished)
    return job
