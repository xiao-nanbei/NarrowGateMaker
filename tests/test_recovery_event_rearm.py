from __future__ import annotations

import numpy as np
import pandas as pd

from research.families.f09_campaign_action_uplift.audit.recovery_event_support_preflight import (
    build_support_grid,
    recovery_scores,
    select_support_row,
)
from models.replay_policies import RecoveryEventSpec, evaluate_recovery_event


def _state(**overrides: float) -> dict[str, float]:
    values = {
        "path_feature_valid": 1.0,
        "path_l2_snapshot_count": 50.0,
        "path_book_age_ms": 100.0,
        "shock_adverse_flow_imbalance_1s": 0.05,
        "shock_adverse_flow_imbalance_5s": 0.50,
        "shock_adverse_flow_imbalance_since_fill": 0.40,
        "refill_recovery_ratio": 0.90,
        "refill_current_vs_start_ratio": 0.95,
        "recovery_microprice_ratio": 0.80,
    }
    values.update(overrides)
    return values


def test_recovery_event_uses_all_four_causal_paths() -> None:
    spec = RecoveryEventSpec(score_threshold=0.70)
    recovered = evaluate_recovery_event(_state(), spec)
    assert recovered.data_valid
    assert recovered.recovery_event
    assert not recovered.hold_add_active

    component_failures = (
        {"shock_adverse_flow_imbalance_1s": 0.50},
        {"refill_recovery_ratio": 0.01},
        {"recovery_microprice_ratio": 0.01},
        {"refill_current_vs_start_ratio": 0.01},
    )
    for replacement in component_failures:
        decision = evaluate_recovery_event(_state(**replacement), spec)
        assert decision.hold_add_active
        assert not decision.recovery_event


def test_invalid_recovery_state_falls_back_at_entry() -> None:
    decision = evaluate_recovery_event(
        _state(path_book_age_ms=5_000.0),
        RecoveryEventSpec(score_threshold=0.50),
    )
    assert not decision.data_valid
    assert not decision.recovery_event
    assert not decision.hold_add_active


def _support_frame(rows: int = 400) -> pd.DataFrame:
    index = np.arange(rows)
    action = np.where(index % 2 == 0, "baseline_rearm", "continue_block_until_recovery")
    quality = (index + 1) / rows
    return pd.DataFrame(
        {
            "day": [f"2026-01-{1 + (value % 20):02d}" for value in index],
            "decision_id": [f"d{value}" for value in index],
            "campaign_id": index,
            "side": "SELL",
            "action": action,
            "behavior_propensity": 0.5,
            "behavior_prob_baseline_rearm": 0.5,
            "behavior_prob_continue_block_until_recovery": 0.5,
            "path_feature_valid": 1.0,
            "path_l2_snapshot_count": 20.0,
            "path_book_age_ms": 100.0,
            "shock_adverse_flow_imbalance_1s": 1.0 - quality,
            "shock_adverse_flow_imbalance_5s": 1.0,
            "shock_adverse_flow_imbalance_since_fill": 1.0,
            "refill_recovery_ratio": quality,
            "refill_current_vs_start_ratio": quality,
            "recovery_microprice_ratio": quality,
            "intervention_fill_count": np.where(index % 2 == 0, 1.0, 0.0),
        }
    )


def test_support_preflight_selects_bounded_candidate_footprint() -> None:
    frame = _support_frame()
    scores = recovery_scores(frame)
    assert scores["recovery_score"].is_monotonic_increasing

    grid = build_support_grid(
        frame,
        quantiles=(0.05, 0.10, 0.15, 0.20, 0.30),
        target_candidate_rate=0.15,
        minimum_candidate_rate=0.05,
        maximum_candidate_rate=0.30,
        minimum_preflight_fill_retention=0.80,
        minimum_candidate_rows=10,
        minimum_candidate_days=5,
        minimum_baseline_fill_events=50,
    )
    selected = select_support_row(grid)
    assert selected is not None
    assert 0.05 <= float(selected["candidate_rate"]) <= 0.30
    assert float(selected["conservative_fill_retention"]) >= 0.80


def test_support_preflight_rejects_fill_collapse() -> None:
    frame = _support_frame()
    frame.loc[
        frame["action"].eq("baseline_rearm") & (frame.index.to_numpy() < 120),
        "intervention_fill_count",
    ] = 10.0
    grid = build_support_grid(
        frame,
        quantiles=(0.10, 0.15, 0.20),
        target_candidate_rate=0.15,
        minimum_candidate_rate=0.05,
        maximum_candidate_rate=0.30,
        minimum_preflight_fill_retention=0.95,
        minimum_candidate_rows=10,
        minimum_candidate_days=5,
        minimum_baseline_fill_events=50,
    )
    assert select_support_row(grid) is None
