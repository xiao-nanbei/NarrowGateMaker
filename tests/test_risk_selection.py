from dataclasses import FrozenInstanceError, replace
from pathlib import Path

import pytest

from strategy.risk_selection import (
    SCHEMA_VERSION,
    VALUE_UNIT,
    LinearValueModel,
    PendingExposure,
    RiskSelectionCandidate,
    RiskSelectionObservation,
    RiskSelectionPolicy,
    candidate_role,
    evaluate_risk_selection,
    inventory_role_for_target,
)


def observation(inventory=0.0, **kwargs):
    return RiskSelectionObservation(100_000_000, 90_000_000, inventory, **kwargs)


def candidate(kind="E", side="BUY", **kwargs):
    defaults = dict(
        opportunity_id=f"{kind}:{side}", kind=kind, side=side, quantity_btc=0.001,
        baseline_action="POST" if kind == "E" else "KEEP",
        order_id="working-order" if kind == "C" else "",
    )
    defaults.update(kwargs)
    return RiskSelectionCandidate(**defaults)


def constant_policy(value=-0.02):
    return RiskSelectionPolicy(
        "mechanics-placeholder", {},
        {f"{kind}:{side}": LinearValueModel(value, {})
         for kind in ("E", "C") for side in ("BUY", "SELL")},
    )


@pytest.mark.parametrize("side,q,qty,expected", [
    ("BUY", 0, 0.001, "opener"), ("SELL", 0, 0.001, "opener"),
    ("BUY", 0.002, 0.001, "add"), ("SELL", -0.002, 0.001, "add"),
    ("BUY", -0.002, 0.001, "reducing"), ("SELL", 0.001, 0.001, "reducing"),
    ("BUY", -0.0004, 0.001, "mixed_cross_zero"),
    ("SELL", 0.0004, 0.001, "mixed_cross_zero"),
])
def test_quantity_role_and_legacy_ber_delegate(side, q, qty, expected):
    from strategy.quote_core import ber_inventory_role_for_target

    assert inventory_role_for_target(side, q, qty) == expected
    assert ber_inventory_role_for_target(side, q, qty) == expected


@pytest.mark.parametrize("kind,q", [("E", 0), ("C", 0), ("C", 0.002)])
@pytest.mark.parametrize("side", ["BUY", "SELL"])
@pytest.mark.parametrize("value,veto", [(-0.02, True), (0, False), (0.02, False)])
def test_value_difference_is_usdc_per_action_with_zero_baseline_tie(kind, q, side, value, veto):
    obs = observation(q if side == "BUY" else -q)
    choice = candidate(kind, side)
    result, = evaluate_risk_selection(obs, [choice], constant_policy(value))
    assert result.action == (("WAIT" if kind == "E" else "CANCEL") if veto
                             else choice.baseline_action)
    assert result.value_delta_usdc == value
    assert not result.out_of_scope
    assert result.feature_ready_ts_ns == obs.feature_ready_ts_ns


@pytest.mark.parametrize("kind,q,quantity", [
    ("E", 0.001, 0.001), ("E", -0.001, 0.001),
    ("C", -0.001, 0.001), ("C", -0.0004, 0.001),
])
def test_add_entry_reducing_and_mixed_roles_are_not_silently_reclassified(kind, q, quantity):
    choice = candidate(kind, quantity_btc=quantity)
    result, = evaluate_risk_selection(observation(q), [choice], constant_policy())
    assert result.action == choice.baseline_action
    assert result.out_of_scope and result.value_delta_usdc is None


def test_pending_sides_do_not_net_to_zero_or_clear_on_cancel_request():
    orders = (PendingExposure("b-pending-new", "BUY", .001),
              PendingExposure("s-pending-cancel", "SELL", .001))
    obs = observation(pending_orders=orders)
    choice = candidate()
    assert candidate_role(obs, choice) == "ambiguous_pending"
    result, = evaluate_risk_selection(obs, [choice], constant_policy())
    assert result.action == "POST" and result.reason == "ambiguous_pending"


def test_c_target_excluded_but_other_pending_fills_can_change_role():
    choice = candidate("C")
    target = PendingExposure(choice.order_id, "BUY", .001)
    assert candidate_role(observation(pending_orders=(target,)), choice) == "opener"
    obs = observation(.001, pending_orders=(target, PendingExposure("sell", "SELL", .003)))
    assert candidate_role(obs, choice) == "ambiguous_pending"
    with pytest.raises(ValueError, match="differs from the order snapshot"):
        candidate_role(observation(pending_orders=(replace(target, remaining_qty_btc=.0004),)),
                       choice)


def test_role_uses_current_remaining_quantity_not_original_submission_role():
    choice = candidate("C", quantity_btc=.001)
    assert candidate_role(observation(-.0004), choice) == "mixed_cross_zero"
    assert candidate_role(observation(-.0004), replace(choice, quantity_btc=.0004)) == "reducing"
    assert candidate_role(observation(.0004), replace(choice, quantity_btc=.0004)) == "add"


@pytest.mark.parametrize("kind,blocked_action", [("E", "WAIT"), ("C", "CANCEL")])
def test_h0_blocked_action_is_never_reenabled(kind, blocked_action):
    choice = candidate(kind, baseline_action=blocked_action, baseline_allowed=False)
    result, = evaluate_risk_selection(observation(), [choice], constant_policy(10))
    assert result.action == blocked_action and result.reason == "baseline_blocked"
    with pytest.raises(ValueError, match="H0-blocked"):
        candidate(kind, baseline_allowed=False)


def test_no_model_is_passthrough_and_batch_sides_share_one_snapshot():
    choices = (candidate(side="BUY"), candidate(side="SELL"))
    obs = observation()
    assert [d.action for d in evaluate_risk_selection(obs, choices)] == ["POST", "POST"]
    policy = constant_policy()
    forward = evaluate_risk_selection(obs, choices, policy)
    reverse = evaluate_risk_selection(obs, choices[::-1], policy)
    assert forward == reverse[::-1]
    assert {d.role for d in forward} == {"opener"}
    assert {d.action for d in forward} == {"WAIT"}
    assert obs.inventory_btc == 0


def test_observation_candidate_and_policy_detach_mutable_caller_inputs():
    source, side_source, coefficients = {"x": 2}, {"x": 3}, {"x": 1}
    obs = observation(features=source)
    choice = candidate(features=side_source)
    policy = RiskSelectionPolicy("test", {"x": ("bps", 1, 2)},
                                 {"E:BUY": LinearValueModel(-2, coefficients)})
    source["x"], side_source["x"], coefficients["x"] = 900, 900, 900
    result, = evaluate_risk_selection(obs, [choice], policy)
    assert result.value_delta_usdc == -1 and result.action == "WAIT"
    with pytest.raises(TypeError):
        obs.features["x"] = 20
    with pytest.raises(FrozenInstanceError):
        obs.inventory_btc = 20


@pytest.mark.parametrize("features", [{}, {"x": None}, {"x": float("nan")},
                                      {"x": float("inf")}, {"x": {}}, {"x": "unknown"}])
def test_unavailable_feature_abstains_without_defaulting_to_zero(features):
    policy = RiskSelectionPolicy("test", {"x": ("bps", 0, 1)},
                                 {"E:BUY": LinearValueModel(-2, {"x": 1})})
    result, = evaluate_risk_selection(observation(features=features), [candidate()], policy)
    assert result.action == "POST" and result.value_delta_usdc is None
    assert result.reason == "unavailable_feature"


def test_future_features_abstain_and_invalid_account_state_is_not_model_fallback():
    obs = replace(observation(), feature_ready_ts_ns=100_000_001)
    result, = evaluate_risk_selection(obs, [candidate()], constant_policy())
    assert result.action == "POST" and result.reason == "future_feature"
    with pytest.raises(ValueError, match="inventory_btc"):
        observation(float("nan"))


def payload():
    return {"schema_version": SCHEMA_VERSION, "value_unit": VALUE_UNIT,
            "policy_id": "synthetic-only", "features": {"x": {"unit": "bps", "mean": 1,
                                                                      "scale": 2}},
            "models": {"E:BUY": {"intercept_usdc": -2, "coefficients": {"x": 1}}}}


@pytest.mark.parametrize("field,value", [("schema_version", "wrong"),
                                         ("value_unit", "bps"), ("models", []),
                                         ("features", {}), ("policy_id", ""),
                                         ("policy_id", 7)])
def test_malformed_artifacts_fail_during_loading(field, value):
    raw = payload()
    raw[field] = value
    with pytest.raises(ValueError):
        RiskSelectionPolicy.from_dict(raw)


def test_loader_does_io_once_and_evaluation_reuses_parsed_policy(tmp_path, monkeypatch):
    import json

    path = tmp_path / "synthetic-policy.json"
    path.write_text(json.dumps(payload()))
    policy = RiskSelectionPolicy.load(path)
    monkeypatch.setattr(Path, "read_text", lambda *args, **kwargs: pytest.fail("runtime file read"))
    for _ in range(2):
        result, = evaluate_risk_selection(observation(features={"x": 3}), [candidate()], policy)
        assert result.value_delta_usdc == -1 and result.action == "WAIT"


def test_duplicate_order_and_opportunity_identifiers_are_not_silently_merged():
    order = PendingExposure("duplicate", "BUY", .001)
    with pytest.raises(ValueError, match="unique"):
        observation(pending_orders=(order, order))
    with pytest.raises(ValueError, match="unique"):
        evaluate_risk_selection(observation(), [candidate(), candidate()])
