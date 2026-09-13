from __future__ import annotations

import gc

from live.runtime_gc import GcPauseMonitor


def test_gc_pause_monitor_observes_durations_without_changing_gc_policy(
    monkeypatch,
) -> None:
    enabled_before = gc.isenabled()
    monitor = GcPauseMonitor()

    try:
        monitor.install()
        monitor.install()
        assert gc.isenabled() is enabled_before
        assert gc.callbacks.count(monitor._callback_ref) == 1
    finally:
        monitor.close()

    assert monitor._callback_ref not in gc.callbacks
    assert gc.isenabled() is enabled_before

    clock = iter((1_000, 1_125, 2_000, 2_350))
    monkeypatch.setattr("live.runtime_gc.time.perf_counter_ns", lambda: next(clock))
    measured = GcPauseMonitor()
    measured._on_gc("start", {"generation": 0})
    measured._on_gc("stop", {"generation": 0})
    measured._on_gc("start", {"generation": 2})
    measured._on_gc("stop", {"generation": 2})

    assert measured.snapshot() == {
        "count": 2,
        "total_ns": 475,
        "max_ns": 350,
        "last_ns": 350,
        "generation_counts": (1, 0, 1),
        "pause_bucket_upper_ns": (
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
        ),
        "pause_bucket_counts": (2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    }


def test_gc_pause_monitor_ignores_unpaired_or_unknown_callbacks() -> None:
    monitor = GcPauseMonitor()

    monitor._on_gc("stop", {"generation": 1})
    monitor._on_gc("start", {"generation": 9})
    monitor._on_gc("unknown", {"generation": 0})

    assert monitor.snapshot()["count"] == 0
