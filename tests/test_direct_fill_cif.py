from __future__ import annotations

import numpy as np
import pandas as pd

from research.families.f06_placement_fill_cif.audit.direct_fill_cif import (
    BASE_FEATURES,
    _apply_cell_offsets,
    _feature_frame,
    _fit_cell_offsets,
    _fit_model,
    _new_model,
    _predict_raw,
    apply_past_only_rolling_calibration,
    expand_placement_panel,
    make_expanding_folds,
    placement_input_columns,
)
from research.families.f06_placement_fill_cif.audit.paired_order_lifecycle import ACTION_ORDER, HORIZONS_MS


def _wide_row() -> dict[str, object]:
    row: dict[str, object] = {
        "cohort_id": "2026-01-01:1",
        "decision_id": "d1",
        "day": "2026-01-01",
        "side": "BUY",
        "inventory_role": "opener",
        "campaign_id": 0,
        "submit_ts_ns": 1_000_000_000,
        "feature_ready_ts_ns": 1_000_000_000,
        "best_bid": 100.0,
        "best_ask": 100.1,
        "mid": 100.05,
        "sigma_sq_raw": 1.0,
        "sigma_sq_blended": 1.0,
        "quote_horizon_s": 1.0,
        "campaign_age_s": 0.0,
        "l2_near_depth_total": 2.0,
        "monotonicity_violation_count": 0,
    }
    for index, action in enumerate(ACTION_ORDER):
        row[f"{action}__action"] = action
        row[f"{action}__price_tick"] = 990 - index
        for horizon in HORIZONS_MS:
            row[f"{action}__placement_observed_{horizon}ms"] = 1
            row[f"{action}__placement_filled_{horizon}ms"] = int(index == 0)
    return row


def test_expand_placement_panel_keeps_only_new_placement_actions() -> None:
    panel = expand_placement_panel(pd.DataFrame([_wide_row()]))
    assert len(panel) == len(ACTION_ORDER) * len(HORIZONS_MS)
    assert set(panel["action"]) == set(ACTION_ORDER)
    assert not {"keep", "replace"} & set(panel["action"])
    assert panel.groupby("action")["distance_ticks"].first().is_monotonic_increasing


def test_direct_cif_projects_only_decision_and_placement_columns() -> None:
    columns = set(placement_input_columns())
    assert "current__placement_filled_10000ms" in columns
    assert "current__native_refill_qty" not in columns
    assert "current__first_touch_ts_ns" not in columns
    assert "campaign_pnl_so_far" in columns


def test_expanding_folds_are_past_only_with_embargo() -> None:
    days = [f"2026-01-{day:02d}" for day in range(1, 31)]
    folds = make_expanding_folds(
        days, min_train_days=20, embargo_days=1, test_days=5
    )
    assert folds
    for fold in folds:
        assert max(fold["train_days"]) < min(fold["embargo_days"])
        assert max(fold["embargo_days"]) < min(fold["test_days"])


def test_direct_cif_model_freezes_distance_and_horizon_monotonicity() -> None:
    model = _new_model()
    constraints = dict(zip(BASE_FEATURES, model.monotonic_cst))  # noqa: B905
    assert constraints["distance_ticks"] == -1
    assert constraints["distance_vol_units"] == -1
    assert constraints["log_horizon_ms"] == 1


def test_direct_cif_predictions_are_monotone_on_shared_state() -> None:
    rng = np.random.default_rng(20260726)
    rows = 1200
    frame = pd.DataFrame(0.0, index=range(rows), columns=BASE_FEATURES)
    frame["distance_ticks"] = rng.uniform(50.0, 300.0, rows)
    frame["distance_vol_units"] = frame["distance_ticks"] / 100.0
    frame["log_horizon_ms"] = rng.choice(
        np.log(np.asarray(HORIZONS_MS, dtype=float)), rows
    )
    probability = 1.0 / (
        1.0
        + np.exp(
            0.025 * (frame["distance_ticks"] - 150.0)
            - 0.8 * (frame["log_horizon_ms"] - np.log(1000.0))
        )
    )
    frame["target"] = rng.binomial(1, probability)
    frame["row_weight"] = 1.0
    model = _fit_model(frame)
    probe = pd.DataFrame(0.0, index=range(3), columns=BASE_FEATURES)
    probe["distance_ticks"] = [100.0, 101.0, 102.0]
    probe["distance_vol_units"] = probe["distance_ticks"] / 100.0
    probe["log_horizon_ms"] = np.log(5000.0)
    prediction = _predict_raw(model, probe)
    assert prediction[0] >= prediction[1] >= prediction[2]
    assert list(_feature_frame(probe).columns) == list(BASE_FEATURES)


def test_v2_cell_intercept_calibration_pools_actions_and_preserves_order() -> None:
    rows = []
    for role in ("opener", "add", "reducing"):
        for horizon in HORIZONS_MS:
            for action_index, action in enumerate(ACTION_ORDER):
                for index in range(200):
                    rows.append(
                        {
                            "inventory_role": role,
                            "horizon_ms": horizon,
                            "action": action,
                            "target": int(index < 10 + action_index),
                            "probability": 0.10 - 0.01 * action_index,
                        }
                    )
    frame = pd.DataFrame(rows)
    offsets = _fit_cell_offsets(frame)
    calibrated = _apply_cell_offsets(
        frame, frame["probability"].to_numpy(), offsets
    )
    frame["calibrated"] = calibrated
    for _, group in frame.groupby(["inventory_role", "horizon_ms"]):
        assert abs(group["calibrated"].mean() - group["target"].mean()) < 1e-4
        action_probability = group.groupby("action")["calibrated"].mean()
        assert action_probability["closer_1tick"] > action_probability["current"]
        assert action_probability["current"] > action_probability["farther_1tick"]


def test_rolling_calibration_uses_only_prior_current_action_days() -> None:
    rows = []
    for day_index in range(5):
        for action in ACTION_ORDER:
            for index in range(100):
                rows.append(
                    {
                        "day": f"2026-01-{day_index + 1:02d}",
                        "side": "BUY",
                        "inventory_role": "opener",
                        "horizon_ms": 1000,
                        "action": action,
                        "target": int(index < 20),
                        "probability": 0.10,
                    }
                )
    frame = pd.DataFrame(rows)
    calibrated = apply_past_only_rolling_calibration(
        frame,
        window_days=3,
        minimum_history_days=3,
        source_action="current",
    )
    first = calibrated.loc[calibrated["day"].eq("2026-01-01")]
    fourth = calibrated.loc[calibrated["day"].eq("2026-01-04")]
    assert not bool(first["rolling_calibration_eligible"].any())
    assert bool(fourth["rolling_calibration_eligible"].all())
    assert np.isclose(first["probability"].mean(), 0.10)
    assert np.isclose(fourth["probability"].mean(), 0.20)
