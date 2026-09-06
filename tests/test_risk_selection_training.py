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
