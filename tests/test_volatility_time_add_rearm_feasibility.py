from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from research.families.f09_campaign_action_uplift.audit.volatility_time_add_rearm_feasibility import (
    build_fill_unit_episodes,
    load_causal_variance_samples,
    load_fill_events,
    summarize_feasibility,
)


def _event(
    ts: int,
    side: str,
    before: float,
    after: float,
    *,
    order_id: int,
) -> dict[str, object]:
    return {
        "day": "2026-06-10",
        "arm": "baseline",
        "side": side,
        "fill_ts": ts,
        "fill_qty": 0.001,
        "inventory_before_fill": before,
        "inventory_after_fill": after,
        "order_id": order_id,
        "microprice_shift_bps": 0.0,
        "l2_book_refresh_ratio": 0.2,
        "l2_book_cancel_ratio": 0.1,
        "queue_before": 1.0,
    }


def test_fill_units_include_reducing_fills_and_only_opposite_side_resets() -> None:
    events = pd.DataFrame(
        [
            _event(1_000, "BUY", 0.0, 0.001, order_id=1),
            _event(2_000, "SELL", 0.001, 0.0, order_id=2),
            _event(3_000, "SELL", 0.0, -0.001, order_id=3),
            _event(4_000, "SELL", -0.001, -0.002, order_id=4),
            _event(5_000, "BUY", -0.002, -0.001, order_id=5),
            _event(6_000, "BUY", -0.001, 0.0, order_id=6),
            _event(7_000, "BUY", 0.0, 0.001, order_id=7),
        ]
    )
    episodes = build_fill_unit_episodes(
        events,
        order_size_btc=0.001,
        lot_size_btc=0.001,
    )
    assert episodes["side"].tolist() == ["BUY", "SELL", "SELL", "BUY"]
    assert episodes["consecutive_same_side_fill_units"].tolist() == [1.0, 2.0, 3.0, 3.0]
    assert episodes["inventory_role_at_fill"].tolist() == [
        "opener",
        "opener",
        "add",
        "opener",
    ]
    assert episodes["inventory_campaign_id"].tolist() == [
        "2026-06-10:campaign:0",
        "2026-06-10:campaign:1",
        "2026-06-10:campaign:1",
        "2026-06-10:campaign:2",
    ]
    assert episodes["censor_reason"].tolist()[:3] == [
        "opposite_fill_reset",
        "same_side_add_restart",
        "opposite_fill_reset",
    ]


def test_fill_loader_does_not_require_absent_role_or_campaign_columns(
    tmp_path: Path,
) -> None:
    path = tmp_path / "fill_trace.csv"
    pd.DataFrame([_event(1_000, "BUY", 0.0, 0.001, order_id=1)]).to_csv(
        path, index=False
    )
    loaded = load_fill_events(path, ["2026-06-10"])
    episodes = build_fill_unit_episodes(
        loaded,
        order_size_btc=0.001,
        lot_size_btc=0.001,
    )
    assert episodes.loc[0, "inventory_role_at_fill"] == "opener"
    assert episodes.loc[0, "inventory_campaign_id"] == "2026-06-10:campaign:0"


def test_completed_bucket_variance_has_explicit_ready_time(tmp_path: Path) -> None:
    day = "2026-06-10"
    start = int(pd.Timestamp(day, tz="UTC").timestamp() * 1000)
    timestamp = np.arange(start, start + 180_000, 1_000, dtype=np.int64)
    close = 100.0 + np.arange(timestamp.size, dtype=float) * 0.01
    pd.DataFrame({"close": close}, index=timestamp).to_parquet(
        tmp_path / f"BTCUSDC-1s-{day}.parquet"
    )
    samples = load_causal_variance_samples(
        tmp_path,
        day,
        rolling_window_s=60,
        max_close_carry_ms=2_000,
        max_abs_return_bps_1s=500.0,
    )
    first = samples[samples["valid"]].iloc[0]
    assert int(first["feature_ready_ts_ms"]) >= start + 61_000
    assert float(first["sigma_sq_price_per_s"]) >= 0.0


def test_side_gate_requires_two_sided_material_variation() -> None:
    rows = []
    for side in ("BUY", "SELL"):
        for index in range(120):
            delta = -10.0 if index < 30 else (10.0 if index < 60 else 0.0)
            rows.append(
                {
                    "day": f"2026-06-{7 + index % 12:02d}",
                    "side": side,
                    "timing_delta_s": delta,
                    "start_variance_valid": True,
                    "candidate_reason": "variance_budget",
                    "candidate_valid_interval_ms": 80_000.0,
                    "candidate_observed_ms": 80_000.0,
                    "consecutive_same_side_fill_units": 1.0 if index < 90 else 2.0,
                    "cpp_variance_clock_match": True,
                }
            )
    gates = {
        "minimum_episodes_per_side": 100,
        "minimum_days_per_side": 10,
        "minimum_n1_episodes_per_side": 50,
        "minimum_n2_episodes_per_side": 10,
        "minimum_start_variance_valid_rate": 0.95,
        "minimum_aggregate_valid_time_rate": 0.95,
        "maximum_wall_cap_rate": 0.5,
        "minimum_earlier_material_rate": 0.05,
        "minimum_later_material_rate": 0.05,
        "minimum_candidate_effective_rate": 0.2,
    }
    cells, _, summary = summarize_feasibility(
        pd.DataFrame(rows), material_delta_s=5.0, gates=gates
    )
    assert cells["side_feasibility_pass"].all()
    assert summary["feasibility_passed"]
    assert not summary["action_experiment_created"]
    assert not summary["validation_read"]
