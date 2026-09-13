from __future__ import annotations

from pathlib import Path

import pandas as pd

from research.families.f09_campaign_action_uplift.audit.volatility_time_add_rearm_feasibility_v2 import (
    load_causal_bbo_variance_samples,
    measure_clock_coverage,
    summarize_v2,
)
from strategy.fill_cooldown import CausalVarianceSample


def _write_bbo(root: Path, day: str, rows: list[tuple[int, float, float]]) -> None:
    path = root / "bbo" / f"BTCUSDC-bbo-{day}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=["timestamp", "best_bid", "best_ask"]).to_parquet(
        path,
        index=False,
    )


def test_bbo_clock_uses_completed_left_closed_bucket(tmp_path: Path) -> None:
    day = "2026-01-02"
    start = int(pd.Timestamp(day, tz="UTC").timestamp() * 1000)
    previous = "2026-01-01"
    _write_bbo(
        tmp_path,
        previous,
        [(start - offset, 99.9, 100.1) for offset in range(120_000, 0, -100)],
    )
    rows = [
        (start + offset, 99.9 + offset / 1_000_000.0, 100.1 + offset / 1_000_000.0)
        for offset in range(0, 65_000, 100)
    ]
    # The event at exactly +1s must not enter the bucket ending at +1s.
    rows.append((start + 1_000, 199.9, 200.1))
    _write_bbo(tmp_path, day, rows)
    samples = load_causal_bbo_variance_samples(
        tmp_path,
        day,
        rolling_window_s=2,
        max_bbo_source_age_ms=500,
        max_abs_return_bps_1s=20_000.0,
        ready_delay_ms=250,
    )
    first_target_bucket = samples.loc[
        samples["bucket_end_ts_ms"].eq(start + 1_000)
    ].iloc[0]
    assert int(first_target_bucket["feature_ready_ts_ms"]) == start + 1_250
    assert float(first_target_bucket["price"]) < 150.0


def test_bbo_clock_invalidates_stale_bucket_and_rolling_window(tmp_path: Path) -> None:
    day = "2026-01-02"
    start = int(pd.Timestamp(day, tz="UTC").timestamp() * 1000)
    previous = "2026-01-01"
    _write_bbo(
        tmp_path,
        previous,
        [(start - offset, 99.9, 100.1) for offset in range(120_000, 0, -100)],
    )
    rows = []
    for offset in range(0, 70_000, 100):
        if 10_000 <= offset < 12_000:
            continue
        rows.append((start + offset, 99.9 + offset / 1_000_000.0, 100.1 + offset / 1_000_000.0))
    _write_bbo(tmp_path, day, rows)
    samples = load_causal_bbo_variance_samples(
        tmp_path,
        day,
        rolling_window_s=3,
        max_bbo_source_age_ms=500,
        max_abs_return_bps_1s=20_000.0,
        ready_delay_ms=0,
    )
    affected = samples[
        samples["bucket_end_ts_ms"].between(start + 11_000, start + 14_000)
    ]
    assert not affected["valid"].any()


def test_v2_does_not_promote_clock_without_live_blocker_parity() -> None:
    rows = []
    for delay in (0, 250, 1_000):
        for side in ("BUY", "SELL"):
            for index in range(120):
                rows.append(
                    {
                        "ready_delay_ms": delay,
                        "side": side,
                        "day": f"2026-01-{index % 12 + 1:02d}",
                        "timing_delta_s": -10.0 if index % 2 == 0 else 10.0,
                        "start_variance_valid": True,
                        "candidate_reason": "budget_exhausted",
                        "candidate_valid_interval_ms": 10_000.0,
                        "candidate_observed_ms": 10_000.0,
                        "cpp_variance_clock_match": True,
                        "consecutive_same_side_fill_units": 1.0 if index < 60 else 2.0,
                    }
                )
    cells, _, summary = summarize_v2(
        pd.DataFrame(rows),
        material_delta_s=5.0,
        gates={
            "minimum_episodes_per_side": 100,
            "minimum_days_per_side": 10,
            "minimum_n1_episodes_per_side": 50,
            "minimum_n2_episodes_per_side": 10,
            "minimum_start_variance_valid_rate": 0.95,
            "minimum_aggregate_valid_time_rate": 0.95,
            "maximum_wall_cap_rate": 0.50,
            "minimum_earlier_material_rate": 0.05,
            "minimum_later_material_rate": 0.05,
            "minimum_candidate_effective_rate": 0.20,
        },
        blocker_contract={
            "buy_q90_replayed": False,
            "consecutive_loss_cooldown_replayed": False,
            "sync_degrade_event_semantics_frozen": False,
        },
    )
    assert cells["side_feasibility_pass"].all()
    assert summary["variance_clock_mechanics_passed"] is True
    assert summary["current_live_blocker_parity_passed"] is False
    assert summary["feasibility_passed"] is False
    assert summary["action_experiment_created"] is False


def test_clock_coverage_stops_at_intra_interval_release() -> None:
    samples = [
        CausalVarianceSample(0, 100.0, 1.0, True),
        CausalVarianceSample(1_000, 100.0, 1.0, True),
    ]
    valid_ms, stale_ms = measure_clock_coverage(
        samples,
        start_ms=0,
        stop_ms=250,
        max_feature_age_ms=2_000,
    )
    assert valid_ms == 250.0
    assert stale_ms == 0.0
