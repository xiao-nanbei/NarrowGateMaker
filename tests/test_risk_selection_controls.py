"""Random participation and Flat are controls, not learned economic policies."""
import copy
import random

import numpy as np
import pytest

from models.replay.risk_selection import ReplayRiskSelection, random_control_draw
from tests.test_python_planned_maintenance_replay import (
    _async_fifo_params,
    _risk_policy_payload,
    _run,
)


def random_params(mode="E", rate=0.5):
    return {
        "risk_selection_control": "random", "risk_selection_mode": mode,
        "risk_selection_policy": _risk_policy_payload(),
        "risk_selection_random_seed": 71, "risk_selection_random_scope": "synthetic-segment",
        "risk_selection_random_rates": {f"{kind}:{side}": rate for kind in mode
                                        for side in ("BUY", "SELL")},
        "planned_quote_stop_ts_ms": 0,
    }


def rows():
    return [dict(opportunity_id=f"one:{side}", kind="E", side=side, quantity_btc=.001,
                 baseline_action="POST", baseline_allowed=True, order_id="",
                 features={}, pending_orders=[], inventory_btc=0.,
                 decision_ts_ns=1_000_000, feature_ready_ts_ns=1_000_000)
            for side in ("BUY", "SELL")]


def collector(rate=.5, **kwargs):
    options = dict(control="random", mode="E", policy=_risk_policy_payload(),
                   random_seed=71, random_scope="synthetic-segment",
                   random_rates={"E:BUY": rate, "E:SELL": rate})
    options.update(kwargs)
    return ReplayRiskSelection(**options)


def test_keyed_random_control_does_not_consume_other_rng_or_depend_on_iteration_order():
    random.seed(88)
    np.random.seed(93)
    py_state, np_state = random.getstate(), np.random.get_state()
    ids = [f"BTCUSDC:{n}:E:BUY" for n in range(100)]
    expected = {key: random_control_draw(71, "segment", key) for key in ids}
    assert {key: random_control_draw(71, "segment", key) for key in reversed(ids)} == expected
    assert all(0 <= value < 1 for value in expected.values())
    assert len(set(expected.values())) == len(ids)
    assert random_control_draw(71, "another-segment", ids[0]) != expected[ids[0]]
    assert random.getstate() == py_state
    after = np.random.get_state()
    assert after[0] == np_state[0] and np.array_equal(after[1], np_state[1])
    assert after[2:] == np_state[2:]
    first, second = collector(), collector()
    a, b = rows(), list(reversed(rows()))
    assert dict(zip([r["opportunity_id"] for r in a], first.observe_batch(a), strict=True)) == dict(
        zip([r["opportunity_id"] for r in b], second.observe_batch(b), strict=True))


@pytest.mark.parametrize("change,reason", [
    ("no_model", "no_model"), ("missing_feature", "unavailable_feature"),
    ("future", "future_feature"), ("blocked", "baseline_blocked"),
    ("missing_rate", "random_veto_rate_missing"),
])
def test_random_support_matches_reference_and_missing_rates_are_explicit(change, reason):
    row = rows()[0]
    payload = _risk_policy_payload()
    options = {}
    if change == "no_model":
        payload["models"].pop("E:BUY")
    elif change == "missing_feature":
        payload["features"] = {"unobserved": {"unit": "bps", "mean": 0., "scale": 1.}}
        payload["models"]["E:BUY"]["coefficients"] = {"unobserved": 1.}
    elif change == "future":
        row["feature_ready_ts_ns"] += 1
    elif change == "blocked":
        row.update(baseline_allowed=False, baseline_action="WAIT")
    else:
        options["random_rates"] = {"E:SELL": 1.}
    rng = collector(rate=1., policy=payload, **options)
    assert rng.observe(row) == row["baseline_action"]
    assert row["value_delta_usdc"] is None and row["random_draw"] is None
    result = rng.finish()
    assert result["risk_selection_control_fallback_counts"] == {reason: 1}
    assert result["risk_selection_policy_decision_count"] == 0
    assert result["risk_selection_reference_evaluation_count"] == 1


def test_random_actions_ignore_reference_value_sign_and_freeze_input_rates():
    rates = {"E:BUY": .5, "E:SELL": .5}
    negative = collector(random_rates=rates)
    positive = collector(policy=_risk_policy_payload(100.))
    rates["E:BUY"] = 1.
    a, b = rows(), rows()
    assert negative.observe_batch(a) == positive.observe_batch(b)
    assert all(row["value_delta_usdc"] is None and row["policy_id"] == "" for row in a)
    assert all(row["random_veto_rate"] == .5 for row in a)
    assert negative.finish()["risk_selection_policy_change_count"] == 0


@pytest.mark.parametrize("async_gateway", [False, True])
def test_zero_random_veto_reproduces_baseline_order_fill_cash_and_cadence(async_gateway):
    overrides = _async_fifo_params() if async_gateway else {}
    overrides["planned_quote_stop_ts_ms"] = 0
    baseline = _run(keep_until_stop=True, param_overrides=overrides)
    control = _run(keep_until_stop=True, param_overrides={**overrides, **random_params("EC", 0.)})
    for key in ("_quote_trace", "_fill_trace", "_decision_trace",
                "pnl", "final_inventory", "n_requotes"):
        assert control[key] == baseline[key]
    assert control["risk_selection_control_change_count"] == 0
    assert control["risk_selection_policy_decision_count"] == 0


@pytest.mark.parametrize("control", ["random", "flat"])
def test_continuous_no_entry_control_never_submits_or_fills_and_does_not_shorten_cadence(control):
    params = {**_async_fifo_params(), **random_params("E", 1.)}
    if control == "flat":
        params = {**_async_fifo_params(), "risk_selection_control": "flat",
                  "risk_selection_mode": "E", "risk_selection_policy": {"not": "parsed"}}
    params.update(post_cooldown_incremental_inventory_budget_enabled=True,
                  post_cooldown_incremental_inventory_budget_units=1, fill_cooldown=1.,
                  trace_post_cooldown_incremental_inventory_budget_max=100,
                  decision_trace_profile="mechanics_only",
                  fill_cooldown_consecutive_reset_policy="opposite_fill_only")
    result = _run(keep_until_stop=True, crossing_fill_ts_ms=1200, param_overrides=params)
    assert result["_quote_trace"] == result["_fill_trace"] == []
    assert result["pnl"] == result["final_inventory"] == 0.
    assert result["risk_selection_control_action_counts"]["WAIT"] > 2
    assert result["risk_selection_control_fallback_counts"] == {}
    assert result["risk_selection_policy_decision_count"] == 0
    if control == "flat":
        assert result["risk_selection_reference_evaluation_count"] == 0


@pytest.mark.parametrize("private_delay", [0., 1200.])
def test_random_cancel_stays_pending_and_can_fill_before_effective(private_delay):
    result = _run(keep_until_stop=True, crossing_fill_ts_ms=1200, param_overrides={
        **_async_fifo_params(new=(2., 5., 30.), cancel=(500., 700., 900.)),
        **random_params("C", 1.), "initial_inventory": .002, "initial_entry_price": 100.,
        "_private_fill_visibility_latency_samples_ms": [private_delay],
    })
    buy = next(row for row in result["_quote_trace"] if row["side"] == "BUY")
    assert buy["cancel_request_ts"] == 1000
    assert buy["outcome"] == "fill" and buy["outcome_ts"] == 1200 + private_delay
    assert result["risk_selection_control_action_counts"]["CANCEL"] > 0
    assert result["risk_selection_intervention_count"] == 0


@pytest.mark.parametrize("overrides", [
    {"initial_inventory": .001}, {"initial_inventory": float("nan")},
    {"initial_live_state": {"health_orders": 1}},
    {"initial_live_state": {"active_orders": [{"order_id": "unknown"}]}},
])
def test_flat_does_not_liquidate_or_erase_existing_or_unknown_account_state(overrides):
    with pytest.raises(ValueError):
        _run(param_overrides={"risk_selection_control": "flat", "risk_selection_mode": "E",
                              **overrides})


@pytest.mark.parametrize("options", [
    {"control": "unknown"}, {"control": []}, {"control": "flat", "mode": "B"},
    {"random_seed": None}, {"random_seed": True}, {"random_scope": " "},
    {"random_rates": {"E:BUY": True}}, {"random_rates": {"bad": .5}},
    {"random_rates": {"E:BUY": float("inf")}}, {"random_rates": {"E:BUY": -1.}},
    {"random_rates": {"E:BUY": 1.1}}, {"random_rates": {}},
    {"policy": None}, {"mode": "B"},
    {"intervention": {"opportunity_id": "one", "action": "WAIT"}},
])
def test_controls_validate_frozen_parameters(options):
    with pytest.raises(ValueError):
        collector(**copy.deepcopy(options))


@pytest.mark.parametrize("control", ["flat", "random"])
def test_cpp_cannot_ignore_controls(control):
    from models.backtest_tick import _simulate_tick_cpp
    from tests.test_python_planned_maintenance_replay import _inputs, _params

    trades, bbo = _inputs()
    with pytest.raises(NotImplementedError, match="Python-authoritative"):
        _simulate_tick_cpp(trades, np.asarray([0]), np.asarray([1.]),
                           {**_params(), "risk_selection_control": control}, bbo_data=bbo)
