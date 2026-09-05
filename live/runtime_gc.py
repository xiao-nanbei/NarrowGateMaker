"""Allocation-light CPython GC pause observability for the live runtime."""

from __future__ import annotations

import gc
import time
from collections.abc import Mapping
from typing import Any


_PAUSE_BUCKET_UPPER_NS = (
    10_000,
    25_000,
    50_000,
    100_000,
    250_000,
    500_000,
    1_000_000,
    2_500_000,
    5_000_000,
    10_000_000,
)


class GcPauseMonitor:
    """Observe stop-the-world GC durations without changing GC policy.

    The callback performs no logging and reuses fixed-size counters.  Runtime
    health serialization reads a snapshot outside the callback hot path.
    """

    __slots__ = (
        "_callback_ref",
        "_bucket_counts",
        "_count",
        "_generation_counts",
        "_installed",
        "_last_ns",
        "_max_ns",
        "_starts_ns",
        "_total_ns",
    )

    def __init__(self) -> None:
        self._starts_ns = [0, 0, 0]
        self._generation_counts = [0, 0, 0]
        # The final bucket is the overflow bucket.  Fixed storage keeps the GC
        # callback allocation-free while still making p99-scale regressions
        # visible in health output.
        self._bucket_counts = [0] * (len(_PAUSE_BUCKET_UPPER_NS) + 1)
        self._count = 0
        self._total_ns = 0
        self._max_ns = 0
        self._last_ns = 0
        self._installed = False
        self._callback_ref = self._on_gc

    def _on_gc(self, phase: str, info: Mapping[str, Any]) -> None:
        generation = int(info.get("generation", -1))
        if generation < 0 or generation >= len(self._starts_ns):
            return
        if phase == "start":
            self._starts_ns[generation] = time.perf_counter_ns()
            return
        if phase != "stop":
            return
        started_ns = self._starts_ns[generation]
        self._starts_ns[generation] = 0
        if started_ns <= 0:
            return
        elapsed_ns = max(0, time.perf_counter_ns() - started_ns)
        self._count += 1
        self._generation_counts[generation] += 1
        self._total_ns += elapsed_ns
        self._last_ns = elapsed_ns
        if elapsed_ns > self._max_ns:
            self._max_ns = elapsed_ns
        bucket_index = len(_PAUSE_BUCKET_UPPER_NS)
        for index, upper_ns in enumerate(_PAUSE_BUCKET_UPPER_NS):
            if elapsed_ns <= upper_ns:
                bucket_index = index
                break
        self._bucket_counts[bucket_index] += 1

    def install(self) -> None:
        if self._installed:
            return
        gc.callbacks.append(self._callback_ref)
        self._installed = True

    def close(self) -> None:
        if not self._installed:
            return
        try:
            gc.callbacks.remove(self._callback_ref)
        except ValueError:
            pass
        self._installed = False

    def snapshot(self) -> dict[str, object]:
        return {
            "count": int(self._count),
            "total_ns": int(self._total_ns),
            "max_ns": int(self._max_ns),
            "last_ns": int(self._last_ns),
            "generation_counts": tuple(
                int(value) for value in self._generation_counts
            ),
            "pause_bucket_upper_ns": _PAUSE_BUCKET_UPPER_NS,
            "pause_bucket_counts": tuple(
                int(value) for value in self._bucket_counts
            ),
        }

    def __enter__(self) -> GcPauseMonitor:
        self.install()
        return self

    def __exit__(self, *_exc_info: object) -> None:
        self.close()
