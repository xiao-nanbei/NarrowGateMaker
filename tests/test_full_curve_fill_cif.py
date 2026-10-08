from __future__ import annotations

import numpy as np
import pandas as pd

from research.families.f06_placement_fill_cif.audit.competing_curve_fill_cif import (
    apply_competing_baseline,
    competing_labels_at_horizons,
    fit_competing_baseline_rates,
    predict_competing_cif_at_horizons,
)
from research.families.f06_placement_fill_cif.audit.full_curve_fill_cif import (
    ACTION_ORDER,
    MODEL_FEATURES,
    STATIC_SOURCE_COLUMNS,
    build_sampled_risk_rows,
    derive_duration_contract,
    expand_action_lifecycles,
    lifecycle_labels_at_horizons,
    predict_cif_at_horizons,
)
from research.families.f06_placement_fill_cif.audit.nested_role_calibrated_competing_curve_cif import (
    apply_logit_calibrator,
    fit_logit_calibrator,
    fit_role_calibrators,
    predict_nested_role_competing_cif,
)
from research.families.f06_placement_fill_cif.audit.role_calibrated_competing_curve_cif import (
    predict_role_calibrated_competing_cif,
)


def _wide_row() -> pd.DataFrame:
    row: dict[str, object] = {name: 0.0 for name in STATIC_SOURCE_COLUMNS}
    row.update(
        cohort_id="c1",
        day="2026-01-01",
        side="BUY",
        inventory_role="add",
        submit_ts_ns=1_000_000_000,
        feature_ready_ts_ns=1_000_000_000,
        observation_end_ts_ns=11_000_000_000,
        best_bid=100.0,
        best_ask=100.1,
        sigma_sq_raw=1.0,
        sigma_sq_blended=1.0,
        monotonicity_violation_count=0,
    )
    for index, action in enumerate(ACTION_ORDER):
        prefix = f"{action}__"
        row.update(
            {
                f"{prefix}price_tick": 1000 - index,
                f"{prefix}activation_ts_ns": 1_010_000_000,
                f"{prefix}activation_status": "active",
                f"{prefix}first_fill_ts_ns": (
                    1_260_000_000 if action == "closer_1tick" else 0
                ),
                f"{prefix}cancel_request_ts_ns": 5_990_000_000,
                f"{prefix}cancel_ack_ts_ns": 6_010_000_000,
                f"{prefix}terminal_ts_ns": (
                    1_260_000_000 if action == "closer_1tick" else 6_010_000_000
                ),
                f"{prefix}terminal_reason": (
                    "exact_queue" if action == "closer_1tick" else "cancel_ack"
                ),
                f"{prefix}terminal_observed": 1,
            }
        )
    return pd.DataFrame([row])


def test_action_lifecycle_uses_event_and_cancel_ack_not_fixed_horizon_labels() -> None:
    lifecycle = expand_action_lifecycles(_wide_row())
    assert len(lifecycle) == 3
    closer = lifecycle.loc[lifecycle["action"].eq("closer_1tick")].iloc[0]
    current = lifecycle.loc[lifecycle["action"].eq("current")].iloc[0]
    assert closer["event_observed"] == 1
    assert closer["event_time_ms"] == 250.0
    assert current["event_observed"] == 0
    assert current["risk_end_ms"] == 5000.0

    labels = lifecycle_labels_at_horizons(lifecycle, [100, 300, 6000])
    closer_labels = labels.loc[labels["action"].eq("closer_1tick")]
    assert closer_labels.set_index("horizon_ms")["target"].to_dict() == {
        100: 0,
        300: 1,
        6000: 1,
    }


def test_sampled_risk_rows_preserve_full_interval_likelihood_weight() -> None:
    lifecycle = expand_action_lifecycles(_wide_row())
    sampled = build_sampled_risk_rows(
        lifecycle,
        interval_ms=100,
        maximum_support_ms=10_000,
        maximum_negative_intervals_per_action=2,
    )
    # closer: two negative bins + event; current/farther: 50 negative bins each.
    assert sampled.attrs["full_negative_interval_weight"] == 102
    assert sampled.attrs["event_intervals"] == 1
    assert np.isclose(
        sampled.loc[sampled["target"].eq(0), "sample_weight"].sum(), 102.0
    )
    assert sampled.loc[sampled["target"].eq(1), "sample_weight"].sum() == 1.0


class _DistanceHazardModel:
    def predict_proba(self, frame: pd.DataFrame) -> np.ndarray:
        assert tuple(frame.columns) == MODEL_FEATURES
        hazard = np.clip(0.02 * np.exp(-0.5 * frame["distance_ticks"]), 1e-6, 0.5)
        return np.column_stack([1.0 - hazard, hazard])


class _ConstantHazardModel:
    def __init__(self, probability: float) -> None:
        self.probability = float(probability)

    def predict_proba(self, frame: pd.DataFrame) -> np.ndarray:
        probability = np.full(len(frame), self.probability, dtype=float)
        return np.column_stack([1.0 - probability, probability])


def test_full_curve_is_time_monotone_and_distance_monotone() -> None:
    lifecycle = expand_action_lifecycles(_wide_row())
    cells = {}
    for action in ACTION_ORDER:
        cells[f"BUY|add|{action}"] = {
            "probability": 1.0,
            "latency_p50_ms": 10.0,
            "latency_p95_ms": 20.0,
        }
    prediction = predict_cif_at_horizons(
        _DistanceHazardModel(),
        lifecycle,
        [100, 500],
        activation_contract={"cells": cells},
        hazard_offset=0.0,
        interval_ms=100,
        maximum_support_ms=10_000,
        chunk_size=10,
    )
    pivot = prediction.pivot(index="action", columns="horizon_ms", values="probability")
    assert bool((pivot[500] >= pivot[100]).all())
    assert pivot.loc["closer_1tick", 500] >= pivot.loc["current", 500]
    assert pivot.loc["current", 500] >= pivot.loc["farther_1tick", 500]


def test_duration_contract_uses_development_exposure_quantiles() -> None:
    lifecycle = expand_action_lifecycles(_wide_row())
    contract = derive_duration_contract(
        lifecycle,
        interval_ms=100,
        report_quantiles=[0.25, 0.5, 0.75],
        maximum_support_quantile=0.99,
    )
    assert contract["report_quantiles"] == {"p25": 5000, "p50": 5000, "p75": 5000}
    assert contract["maximum_support_ms"] == 5000


def test_cancel_ack_hazard_caps_fill_cumulative_incidence() -> None:
    lifecycle = expand_action_lifecycles(_wide_row())
    sampled = build_sampled_risk_rows(
        lifecycle,
        interval_ms=100,
        maximum_support_ms=10_000,
        maximum_negative_intervals_per_action=4,
        hazard_causes=("fill", "cancel_ack"),
    )
    assert sampled.attrs["fill_event_intervals"] == 1
    assert sampled.attrs["cancel_event_intervals"] == 2
    assert set(sampled["target"].unique()) == {0, 1, 2}

    cells = {
        f"BUY|add|{action}": {
            "probability": 1.0,
            "latency_p50_ms": 0.0,
            "latency_p95_ms": 0.0,
        }
        for action in ACTION_ORDER
    }
    prediction = predict_cif_at_horizons(
        {
            "fill": _ConstantHazardModel(0.01),
            "cancel_ack": _ConstantHazardModel(0.20),
        },
        lifecycle,
        [100, 10_000],
        activation_contract={"cells": cells},
        hazard_offset={"fill": 0.0, "cancel_ack": 0.0},
        interval_ms=100,
        maximum_support_ms=10_000,
    )
    pivot = prediction.pivot(index="action", columns="horizon_ms", values="probability")
    assert bool((pivot[10_000] >= pivot[100]).all())
    assert bool((pivot[10_000] < 0.05).all())


def test_competing_labels_export_fill_cancel_and_no_event() -> None:
    lifecycle = expand_action_lifecycles(_wide_row())
    labels = competing_labels_at_horizons(lifecycle, [300, 6000])
    assert bool(
        labels[["fill_target", "cancel_ack_target", "no_event_target"]]
        .sum(axis=1)
        .eq(1)
        .all()
    )
    closer = labels.loc[
        labels["action"].eq("closer_1tick")
        & labels["horizon_ms"].eq(300)
    ].iloc[0]
    current = labels.loc[
        labels["action"].eq("current") & labels["horizon_ms"].eq(6000)
    ].iloc[0]
    assert closer["fill_target"] == 1
    assert closer["cancel_ack_target"] == 0
    assert current["fill_target"] == 0
    assert current["cancel_ack_target"] == 1


def test_competing_prediction_preserves_probability_simplex() -> None:
    lifecycle = expand_action_lifecycles(_wide_row())
    cells = {
        f"BUY|add|{action}": {
            "probability": 0.9,
            "latency_p50_ms": 10.0,
            "latency_p95_ms": 20.0,
        }
        for action in ACTION_ORDER
    }
    prediction = predict_competing_cif_at_horizons(
        {
            "fill": _ConstantHazardModel(0.01),
            "cancel_ack": _ConstantHazardModel(0.02),
        },
        lifecycle,
        [100, 1000],
        activation_contract={"cells": cells},
        hazard_offset={"fill": 0.0, "cancel_ack": 0.0},
        interval_ms=100,
        maximum_support_ms=10_000,
    )
    total = prediction[
        [
            "fill_probability",
            "cancel_ack_probability",
            "no_event_probability",
        ]
    ].sum(axis=1)
    assert np.allclose(total, 1.0)
    ordered = prediction.sort_values(["action", "horizon_ms"])
    for cause in ("fill_probability", "cancel_ack_probability"):
        difference = ordered.groupby("action")[cause].diff().dropna()
        assert bool((difference >= 0.0).all())


def test_competing_baseline_is_a_valid_multinomial_probability() -> None:
    lifecycle = expand_action_lifecycles(_wide_row())
    rates = fit_competing_baseline_rates(lifecycle, [300, 6000])
    labels = competing_labels_at_horizons(lifecycle, [300, 6000])
    fill, cancel = apply_competing_baseline(labels, rates)
    assert bool((fill >= 0.0).all())
    assert bool((cancel >= 0.0).all())
    assert bool((fill + cancel <= 1.0).all())


def test_role_calibration_offsets_apply_only_to_matching_role() -> None:
    add = _wide_row()
    reducing = _wide_row()
    reducing["cohort_id"] = "c2"
    reducing["inventory_role"] = "reducing"
    lifecycle = expand_action_lifecycles(pd.concat([add, reducing], ignore_index=True))
    cells = {}
    for role in ("add", "reducing"):
        for action in ACTION_ORDER:
            cells[f"BUY|{role}|{action}"] = {
                "probability": 1.0,
                "latency_p50_ms": 0.0,
                "latency_p95_ms": 0.0,
            }
    prediction = predict_role_calibrated_competing_cif(
        {
            "fill": _ConstantHazardModel(0.01),
            "cancel_ack": _ConstantHazardModel(0.02),
        },
        lifecycle,
        [1000],
        role_offsets={
            "add": {"fill": -1.0, "cancel_ack": 0.0},
            "reducing": {"fill": 1.0, "cancel_ack": 0.0},
        },
        activation_contract={"cells": cells},
        interval_ms=100,
        maximum_support_ms=10_000,
        chunk_size=10,
    )
    mean_fill = prediction.groupby("inventory_role")["fill_probability"].mean()
    assert mean_fill["reducing"] > mean_fill["add"]


def test_inner_oof_logit_calibrator_is_positive_and_monotone() -> None:
    raw = np.linspace(0.001, 0.20, 1000)
    rng = np.random.default_rng(20260727)
    target = (rng.random(1000) < (0.02 + 0.80 * raw)).astype(np.int8)
    calibrator = fit_logit_calibrator(
        raw,
        target,
        np.ones(1000),
        regularization_c=100.0,
        minimum_event_intervals=20,
        minimum_non_event_intervals=100,
    )
    calibrated = apply_logit_calibrator(raw, calibrator)
    assert calibrator["slope"] > 0.0
    assert bool((np.diff(calibrated) > 0.0).all())


def test_role_calibrator_uses_only_outer_train_inner_oof_days() -> None:
    rows = []
    for day in ("2026-01-01", "2026-01-02", "2026-01-03"):
        for role in ("opener", "add", "reducing"):
            for index in range(240):
                rows.append(
                    {
                        "day": day,
                        "inventory_role": role,
                        "target": 1 if index % 12 == 0 else 2 if index % 10 == 0 else 0,
                        "sample_weight": 1.0,
                        "raw_fill": 0.20 if index % 12 == 0 else 0.01,
                        "raw_cancel_ack": 0.25 if index % 10 == 0 else 0.02,
                    }
                )
    spec = {
        "development_fit": {
            "inner_oof_role_calibration": {
                "roles": ["opener", "add", "reducing"],
                "regularization_c": 100.0,
                "minimum_event_intervals": 20,
                "minimum_non_event_intervals": 100,
            }
        }
    }
    calibrators, identity = fit_role_calibrators(
        pd.DataFrame(rows),
        outer_train_days=["2026-01-01", "2026-01-02"],
        spec=spec,
    )
    assert identity["inner_oof_days"] == ["2026-01-01", "2026-01-02"]
    assert set(calibrators) == {"opener", "add", "reducing"}
    assert all(
        calibrators[role][cause]["slope"] > 0.0
        for role in calibrators
        for cause in ("fill", "cancel_ack")
    )


def test_nested_role_calibration_preserves_competing_simplex() -> None:
    lifecycle = expand_action_lifecycles(_wide_row())
    cells = {
        f"BUY|add|{action}": {
            "probability": 1.0,
            "latency_p50_ms": 0.0,
            "latency_p95_ms": 0.0,
        }
        for action in ACTION_ORDER
    }
    calibrator = {"intercept": 0.0, "slope": 1.0}
    prediction = predict_nested_role_competing_cif(
        {
            "fill": _ConstantHazardModel(0.01),
            "cancel_ack": _ConstantHazardModel(0.02),
        },
        lifecycle,
        [100, 1000],
        role_calibrators={
            "add": {"fill": calibrator, "cancel_ack": calibrator}
        },
        activation_contract={"cells": cells},
        interval_ms=100,
        maximum_support_ms=10_000,
        chunk_size=10,
    )
    total = prediction[
        ["fill_probability", "cancel_ack_probability", "no_event_probability"]
    ].sum(axis=1)
    assert np.allclose(total, 1.0)
