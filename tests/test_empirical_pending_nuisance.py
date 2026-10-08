from __future__ import annotations

import numpy as np
import pandas as pd

from research.families.f06_placement_fill_cif.audit.evaluate_empirical_pending_nuisance import (
    ACTIONS,
    BetaCell,
    _daily_action_metrics,
    _fit_child_cells,
    _fit_parent_cells,
    _pre_monotonicity_metrics,
    _quantity_metrics,
)


def test_pending_hierarchy_uses_side_action_parent_and_separate_children() -> None:
    frame = pd.DataFrame(
        {
            "side": ["BUY"] * 8 + ["SELL"] * 8,
            "action": (["closer_1tick"] * 4 + ["current"] * 4) * 2,
            "inventory_role": (["opener", "opener", "add", "add"] * 4),
            "cancel_request_reason": (["ttl", "ttl", "replace", "replace"] * 4),
        }
    )
    target = np.asarray([1, 0, 0, 0, 0, 0, 0, 0] * 2, dtype=np.int8)
    parents = _fit_parent_cells(frame, target, equivalent_trials=10.0)
    assert set(parents) == {
        ("BUY", "closer_1tick"),
        ("BUY", "current"),
        ("BUY", "farther_1tick"),
        ("SELL", "closer_1tick"),
        ("SELL", "current"),
        ("SELL", "farther_1tick"),
    }
    assert parents[("BUY", "closer_1tick")].mean > parents[("BUY", "current")].mean
    role = _fit_child_cells(
        frame,
        target,
        parents,
        child="inventory_role",
        equivalent_trials=5.0,
    )
    reason = _fit_child_cells(
        frame,
        target,
        parents,
        child="cancel_request_reason",
        equivalent_trials=5.0,
    )
    assert ("BUY", "closer_1tick", "opener") in role
    assert ("BUY", "closer_1tick", "ttl") in reason


def test_action_daily_metrics_do_not_pool_placement_actions() -> None:
    frame = pd.DataFrame(
        {
            "day": ["2026-01-01", "2026-01-01"],
            "side": ["BUY", "BUY"],
            "inventory_role": ["opener", "opener"],
            "action": ["closer_1tick", "current"],
            "phase": ["pre_request", "pre_request"],
            "horizon_ms": [100, 100],
            "fill_target": [1, 0],
            "ack_target": [0, 0],
            "no_event_target": [0, 1],
            "fill_probability": [0.8, 0.2],
            "ack_probability": [0.0, 0.0],
            "no_event_probability": [0.2, 0.8],
            "baseline_fill_probability": [0.5, 0.5],
            "baseline_ack_probability": [0.0, 0.0],
            "baseline_no_event_probability": [0.5, 0.5],
        }
    )
    result = _daily_action_metrics(frame)
    assert len(result) == 2
    assert set(result["action"]) == {"closer_1tick", "current"}


def test_common_support_monotonicity_excludes_gtx_reject() -> None:
    probabilities = dict(zip(ACTIONS, (0.6, 0.4, 0.2), strict=True))
    predictions = pd.DataFrame(
        [
            {
                "cohort_id": "c1",
                "day": "2026-01-01",
                "side": "BUY",
                "inventory_role": "opener",
                "action": action,
                "action_lifecycle_id": f"c1:{action}",
                "phase": "pre_request",
                "horizon_ms": 100,
                "fill_probability": probability,
                "fill_target": int(action == "closer_1tick"),
            }
            for action, probability in probabilities.items()
        ]
    )
    raw = pd.DataFrame(
        [
            {
                "action_lifecycle_id": f"c1:{action}",
                "cohort_id": "c1",
                "side": "BUY",
                "inventory_role": "opener",
                "action": action,
                "activation_status": "active",
                "request_model_risk_set": 1,
            }
            for action in ACTIONS
        ]
    )
    result = _pre_monotonicity_metrics(
        predictions,
        raw,
        fold=0,
        horizons_ms=[100],
    )
    assert result[0]["common_support_cohorts"] == 1
    assert result[0]["prediction_violations"] == 0
    assert result[0]["observed_path_violations"] == 0
    raw.loc[raw["action"].eq("closer_1tick"), "activation_status"] = "gtx_reject"
    excluded = _pre_monotonicity_metrics(
        predictions,
        raw,
        fold=0,
        horizons_ms=[100],
    )
    assert excluded == []


def test_pending_quantity_reports_probability_quantity_and_ev_bound() -> None:
    frame = pd.DataFrame(
        {
            "side": ["BUY", "BUY"],
            "action": ["current", "current"],
            "pending_cancel_fill_qty": [0.001, 0.0],
            "request_remaining_qty": [0.001, 0.001],
            "request_mid": [100000.0, 100000.0],
        }
    )
    cells = {("BUY", "current"): BetaCell(2.0, 1000.0, 1, 2)}
    result = _quantity_metrics(
        frame,
        np.asarray([1, 0], dtype=np.int8),
        cells,
        posterior_probability=0.99,
        stress_bps=[100],
        negligible_usdc=1.0,
    )
    assert result[0]["pending_fill_probability"] == 0.5
    assert result[0]["conditional_pending_fill_qty"] == 0.001
    assert result[0]["conditional_pending_fill_fraction"] == 1.0
    assert result[0]["maximum_ev_uncertainty_usdc"] > 0.0
