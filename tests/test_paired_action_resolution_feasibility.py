from __future__ import annotations

import pandas as pd
import pytest

from research.families.f06_placement_fill_cif.audit.paired_action_resolution_feasibility import (
    ACTION_OFFSETS,
    ACTION_ORDER,
    _day_simultaneous_bands,
    build_contrast_rows,
    build_resolution_cohorts,
    classify_resolution,
)


def _source_row(side: str, baseline_price_tick: int) -> dict[str, object]:
    return {
        "cohort_id": f"cohort-{side}",
        "day": "2026-01-01",
        "side": side,
        "inventory_role": "opener",
        "campaign_id": 0,
        "submit_ts_ns": 1_000_000_000,
        "activate_ts_ns": 1_010_000_000,
        "cancel_request_ts_ns": 6_010_000_000,
        "cancel_ack_ts_ns": 6_020_000_000,
        "observation_end_ts_ns": 6_020_000_000,
        "baseline_price_tick": baseline_price_tick,
        "quantity": 0.001,
        "mid": 100_000.0,
        "baseline_queue_deplete_mult": 1.0,
    }


def test_expanded_action_prices_move_away_from_same_side_bbo() -> None:
    cohorts = build_resolution_cohorts(
        pd.DataFrame([_source_row("BUY", 1_000), _source_row("SELL", 1_001)])
    )
    buy, sell = cohorts
    for action, offset in ACTION_OFFSETS.items():
        assert buy.children[action].price_tick == 1_000 - offset
        assert sell.children[action].price_tick == 1_001 + offset


def _action_rows(*, violate: bool = False) -> pd.DataFrame:
    rows = []
    filled = {
        "closer_4tick",
        "closer_2tick",
        "closer_1tick",
        "current",
    }
    if violate:
        filled.remove("current")
        filled.add("farther_1tick")
    for action in ACTION_ORDER:
        rows.append(
            {
                "cohort_id": "c1",
                "day": "2026-01-01",
                "side": "BUY",
                "inventory_role": "add",
                "campaign_id": 7,
                "mid": 100_000.0,
                "quantity": 0.001,
                "action": action,
                "activation_status": "active",
                "activation_ts_ns": 1_000_000_000,
                "first_fill_ts_ns": 2_000_000_000 if action in filled else 0,
            }
        )
    return pd.DataFrame(rows)


def test_contrast_rows_use_one_common_clock_and_preserve_path_monotonicity() -> None:
    contrasts, diagnostics = build_contrast_rows(
        _action_rows(), clock_ms=5_000, stress_fill_value_bps=100.0
    )
    current_farther = contrasts.loc[
        contrasts["contrast"].eq("current_farther")
        & contrasts["gap_ticks"].eq(1)
    ].iloc[0]
    assert current_farther["fill_difference"] == 1
    assert current_farther["quantity_difference_btc"] == pytest.approx(0.001)
    assert current_farther["shared_fill_price_improvement_usdc"] == 0.0
    assert current_farther["extra_fill_stress_usdc"] == pytest.approx(1.0)
    assert diagnostics["all_grid_activated_cohorts"] == 1
    assert diagnostics["observed_monotonicity_violations"] == 0

    with pytest.raises(RuntimeError, match="outcome monotonicity failed"):
        build_contrast_rows(
            _action_rows(violate=True),
            clock_ms=5_000,
            stress_fill_value_bps=100.0,
        )


def test_day_clustered_bands_and_resolution_classification_are_separate() -> None:
    rows = []
    for day in ("2026-01-01", "2026-01-02", "2026-01-03"):
        for gap in (1, 2):
            rows.append(
                {
                    "day": day,
                    "side": "BUY",
                    "inventory_role": "add",
                    "gap_ticks": gap,
                    "contrast": "current_farther",
                    "campaign_cluster_id": f"{day}:campaign:1",
                    "decisions": 100,
                    "fill_difference": 10 if gap == 1 else 20,
                    "quantity_difference_btc": 0.01 if gap == 1 else 0.02,
                    "shared_fill_price_improvement_usdc": 0.10,
                    "extra_fill_stress_usdc": 0.02,
                    "conservative_value_lower_usdc": 0.08,
                    "conservative_value_upper_usdc": 0.12,
                }
            )
    clusters = pd.DataFrame(rows)
    cells, daily, _ = _day_simultaneous_bands(
        clusters,
        bootstrap_samples=200,
        bootstrap_seed=17,
        confidence=0.95,
    )
    assert len(daily) == 6
    classified, minima, decision = classify_resolution(
        cells, nuisance_uncertainty_usdc=0.0002
    )
    assert classified["raw_fill_resolution_supported"].all()
    assert classified["economic_interval_resolution_supported"].all()
    assert minima.iloc[0]["minimum_raw_fill_gap_ticks"] == 1
    assert minima.iloc[0]["minimum_economic_interval_gap_ticks"] == 1
    assert decision == "one_tick_economic_resolution_exists_model_is_bottleneck"
