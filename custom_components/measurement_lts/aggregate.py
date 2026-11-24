"""Reproduce Recorder's arithmetic measurement statistics from retained states."""

from collections.abc import Iterable
from dataclasses import dataclass, field
from math import isclose, isfinite

HOUR = 3600
PERIOD = 300
Sample = tuple[float, float | None, bool]


@dataclass
class Hour:
    """Aggregate the five-minute means and extrema for an hour."""

    means: list[float] = field(default_factory=list)
    minimum: float = float("inf")
    maximum: float = float("-inf")

    def add(self, mean: float, minimum: float, maximum: float) -> None:
        self.means.append(mean)
        self.minimum = min(self.minimum, minimum)
        self.maximum = max(self.maximum, maximum)

    def values(self) -> tuple[float, float, float]:
        """Return Recorder's arithmetic mean of the available block means."""
        return sum(self.means) / len(self.means), self.minimum, self.maximum


def five_minute(samples: Iterable[Sample], start: int, end: int, checkpoint=None):
    """Replay Recorder's start state and significant changes per five-minute block."""
    iterator = iter(samples)
    upcoming = next(iterator, None)
    previous_value = None
    while upcoming is not None and upcoming[0] < start:
        _, previous_value, _ = upcoming
        upcoming = next(iterator, None)

    for block_start in range(start, end, PERIOD):
        if checkpoint is not None:
            checkpoint()
        block_end = block_start + PERIOD
        points = []
        if previous_value is not None and isfinite(previous_value):
            points.append((block_start, previous_value))

        while upcoming is not None and upcoming[0] < block_end:
            timestamp, value, significant = upcoming
            previous_value = value
            if significant and value is not None and isfinite(value):
                points.append((timestamp, value))
            upcoming = next(iterator, None)

        if not points:
            continue
        weighted = sum(
            value * (next_time - timestamp)
            for (timestamp, value), (next_time, _) in zip(
                points, points[1:] + [(block_end, 0.0)]
            )
        )
        minimum = min(value for _, value in points)
        maximum = max(value for _, value in points)
        yield block_start, (weighted / (block_end - points[0][0]), minimum, maximum)


def combine_hours(blocks) -> dict[int, Hour]:
    """Aggregate the available five-minute blocks by UTC hour."""
    hours: dict[int, Hour] = {}
    for start, values in blocks:
        hours.setdefault(start // HOUR * HOUR, Hour()).add(*values)
    return hours


def hourly(samples: Iterable[Sample], start: int, end: int) -> dict[int, Hour]:
    """Calculate hourly statistics from historical state changes."""
    return combine_hours(five_minute(samples, start, end))


def matches(hour: Hour, stored: tuple[float | None, ...]) -> bool:
    """Compare an existing statistic with the reconstructed values."""
    return all(
        old is not None and isclose(new, old, rel_tol=1e-9, abs_tol=1e-6)
        for new, old in zip(hour.values(), stored, strict=True)
    )
