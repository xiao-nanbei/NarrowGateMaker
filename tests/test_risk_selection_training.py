from copy import deepcopy

import numpy as np
import pytest

from research.families.f05_fill_quality_quote_ev.risk_selection_training import (
    LABEL_SCOPE,
    train_chronological_ridge,
)
from strategy.risk_selection import RiskSelectionPolicy


def label(index, *, day=1, surface="E:BUY", x=None):
    kind, side = surface.split(":")
    start = day * 86_400_000
    value = 0.01 * index
    return {
        "opportunity_id": f"{day}:{index}:{surface}", "kind": kind, "side": side,
        "order_id": f"order-{day}-{index}" if kind == "C" else "",
        "value_scope": LABEL_SCOPE, "additive_portfolio_return": False,
        "baseline_action": "POST" if kind == "E" else "KEEP",
        "alternative_action": "WAIT" if kind == "E" else "CANCEL",
        "replay_start_ts_ms": start, "terminal_mark_ts_ms": start + 86_399_999,
        "decision_ts_ns": (start + 100 * index) * 1_000_000,
        "feature_ready_ts_ns": start * 1_000_000,
        "baseline_value_usdc": value, "alternative_value_usdc": 0,
        "value_difference_usdc": value, "matched_opportunity_prefix_count": index + 1,
        "features": {"x": index if x is None else x},
    }


def fit(rows, **kwargs):
    return train_chronological_ridge(rows, feature_units={"x": "bps"},
                                    validation_start_ns=3 * 86_400_000_000_000, **kwargs)


def test_training_only_transforms_and_per_surface_models():
    rows = [label(i, surface=surface) for surface in ("E:BUY", "C:SELL") for i in range(10)]
    validation = [label(i, day=3, x=1000) for i in range(3)]
    policy, report = fit(rows + validation)
    parsed = RiskSelectionPolicy.from_dict(policy)
    assert set(parsed.models) == {"E:BUY", "C:SELL"}
    assert parsed.features["x"][1] == pytest.approx(4.5)
    assert report["surfaces"]["E:SELL"]["status"] == "insufficient_training_rows"
    assert report["surfaces"]["E:BUY"]["status"] == "chronological_prediction_diagnostic"
    assert report["surfaces"]["C:SELL"]["status"] == "training_only"
    altered = deepcopy(rows + validation)
    for row in altered[len(rows):]:
        row["features"]["x"] = -1e9
        row["baseline_value_usdc"] = row["value_difference_usdc"] = 100
    assert fit(altered)[0] == policy


def test_single_common_terminal_cannot_be_randomly_split_as_independent_validation():
    rows = [label(i) for i in range(10)]
    cutoff = rows[5]["decision_ts_ns"]
    policy, report = train_chronological_ridge(
        rows, feature_units={"x": "bps"}, validation_start_ns=cutoff,
    )
    assert report["train_rows"] == 0
    assert report["excluded"]["overlapping_validation_outcome"] == 5
    assert not policy["models"]


def test_missing_and_nonfinite_features_are_reported_not_imputed():
    rows = [label(i) for i in range(10)]
    rows[0]["features"] = {}
    rows[1]["features"]["x"] = None
    rows[2]["features"]["x"] = np.nan
    policy, report = fit(rows)
    assert report["excluded"]["train_missing_feature"] == 3
    assert not policy["models"]


@pytest.mark.parametrize("field,value", [
    ("value_difference_usdc", 99), ("feature_ready_ts_ns", 10**20),
    ("additive_portfolio_return", True), ("matched_opportunity_prefix_count", 0),
    ("alternative_action", "CANCEL"), ("value_scope", "live_profit"),
])
def test_invalid_pair_contract_is_rejected(field, value):
    row = label(1)
    row[field] = value
    with pytest.raises(ValueError):
        fit([row])


def test_duplicate_opportunities_are_not_extra_independent_training_rows():
    row = label(1)
    with pytest.raises(ValueError, match="duplicate opportunity"):
        fit([row, deepcopy(row)])


def test_constant_feature_scaling_and_past_intercept_reference():
    policy, report = fit([label(i, x=2) for i in range(10)] + [label(2, day=3, x=2)])
    assert policy["features"]["x"]["scale"] == 1
    model = policy["models"]["E:BUY"]
    assert model["coefficients"]["x"] == 0
    assert model["intercept_usdc"] == pytest.approx(.045)
    surface = report["surfaces"]["E:BUY"]
    assert surface["validation_mse"] == surface["past_only_intercept_mse"]
    assert surface["train_feature_support"]["x"] == {
        "unique_values": 1, "minimum": 2.0, "maximum": 2.0, "constant": True,
    }
    assert surface["train_decision_utc_hours"] == {"0": 10}


def test_support_report_keeps_sparse_and_absent_surfaces_without_validation_leakage():
    rows = [label(1, surface="C:BUY", x=0.001),
            label(2, surface="C:BUY", x=np.nextafter(0.001, np.inf)),
            label(1, surface="C:BUY", day=3, x=1e9)]
    policy, report = fit(rows)
    assert not policy["models"]
    sparse = report["surfaces"]["C:BUY"]
    assert sparse["train_outcome_windows"] == 1
    support = sparse["train_feature_support"]["x"]
    assert support["unique_values"] == 2
    assert support["constant"] is False
    assert support["maximum"] == np.nextafter(0.001, np.inf)
    absent = report["surfaces"]["E:SELL"]
    assert absent["train_feature_support"]["x"] == {
        "unique_values": 0, "minimum": None, "maximum": None, "constant": None,
    }
    assert absent["train_outcome_windows"] == 0
    assert absent["train_decision_utc_hours"] == {}


@pytest.mark.parametrize("feature_order", [
    ("quantity", "x", "near_constant"),
    ("x", "near_constant", "quantity"),
    ("near_constant", "quantity", "x"),
])
@pytest.mark.parametrize("row_order", ["original", "reversed", "shuffled"])
def test_decimal_constant_is_exact_under_row_and_column_reordering(feature_order, row_order):
    rows = [label(i) for i in range(43)]
    for index, row in enumerate(rows):
        row["features"].update({
            "quantity": 0.001,
            "near_constant": np.nextafter(0.001, np.inf) if index % 2 else 0.001,
        })
    if row_order == "reversed":
        rows.reverse()
    elif row_order == "shuffled":
        rows = [rows[index] for index in np.random.default_rng(7).permutation(len(rows))]
    units = {name: "BTC" if name != "x" else "bps" for name in feature_order}
    policy, report = train_chronological_ridge(
        rows, feature_units=units, validation_start_ns=3 * 86_400_000_000_000,
    )
    assert report["train_rows"] == 43
    assert policy["features"]["quantity"] == {"unit": "BTC", "mean": 0.001, "scale": 1.0}
    assert policy["models"]["E:BUY"]["coefficients"]["quantity"] == 0.0
    matrix = np.asarray([[row["features"][name] for name in feature_order] for row in rows])
    means, scales = matrix.mean(axis=0), matrix.std(axis=0)
    for name in ("x", "near_constant"):
        index = feature_order.index(name)
        assert policy["features"][name]["mean"] == means[index]
        assert policy["features"][name]["scale"] == scales[index]
    assert 0 < policy["features"]["near_constant"]["scale"] < 1e-15
