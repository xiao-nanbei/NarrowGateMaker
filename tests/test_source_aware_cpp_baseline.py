from __future__ import annotations

import pandas as pd

from models.source_aware_cpp_baseline import _paired_summary, _summary


def test_summary_never_pools_source_authorities() -> None:
    frame = pd.DataFrame(
        [
            {
                "day": "2025-08-02",
                "source_authority": "provider_normalized_causal",
                "arm": "ml_off",
                "pnl": 1.0,
                "terminal_mtm_pnl": 1.0,
                "inventory_adjusted_pnl": 0.5,
                "fills_total": 10,
                "fills_bid": 4,
                "fills_ask": 6,
                "abs_inventory_time_s": 2.0,
                "avg_markout": 0.1,
                "runtime_s": 0.2,
            },
            {
                "day": "2026-07-22",
                "source_authority": "native_formal_lifecycle",
                "arm": "ml_off",
                "pnl": -2.0,
                "terminal_mtm_pnl": -2.0,
                "inventory_adjusted_pnl": -1.5,
                "fills_total": 12,
                "fills_bid": 7,
                "fills_ask": 5,
                "abs_inventory_time_s": 3.0,
                "avg_markout": -0.2,
                "runtime_s": 0.3,
            },
        ]
    )
    rows = _summary(frame)
    assert len(rows) == 2
    assert {row["source_authority"] for row in rows} == {
        "provider_normalized_causal",
        "native_formal_lifecycle",
    }
    assert sum(row["sum_pnl"] for row in rows) == -1.0


def test_paired_summary_uses_day_level_differences() -> None:
    frame = pd.DataFrame(
        [
            {
                "day": day,
                "arm": arm,
                "pnl": pnl,
                "terminal_mtm_pnl": pnl,
                "inventory_adjusted_pnl": pnl,
                "fills_total": fills,
                "abs_inventory_time_s": inventory_time,
                "avg_markout": pnl,
            }
            for day, off, on in (
                ("2026-07-01", 1.0, 2.0),
                ("2026-07-02", -1.0, -3.0),
            )
            for arm, pnl, fills, inventory_time in (
                ("ml_off", off, 10, 4.0),
                ("ml_on", on, 8, 3.0),
            )
        ]
    )
    result = _paired_summary(frame, bootstrap_draws=100, seed=7)
    assert result is not None
    assert result["metrics"]["pnl"]["sum_delta"] == -1.0
    assert result["metrics"]["pnl"]["positive_day_rate"] == 0.5
    assert result["fill_retention"] == 0.8
    assert result["inventory_time_ratio"] == 0.75
