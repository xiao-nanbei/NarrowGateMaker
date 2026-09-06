"""F01 consumes one frozen policy and keeps complete independent economic paths."""
import json
from dataclasses import replace

import numpy as np
import pytest

from research.families.f01_fixed_parameter_racing import campaign_outcome_replay_audit as audit
from strategy.risk_selection import SCHEMA_VERSION, VALUE_UNIT


def policy_payload():
    return {"schema_version": SCHEMA_VERSION, "value_unit": VALUE_UNIT,
            "policy_id": "test-negative-values", "features": {},
            "models": {surface: {"intercept_usdc": -1., "coefficients": {}}
                       for surface in ("E:BUY", "E:SELL", "C:BUY", "C:SELL")}}


def arm(name, **overrides):
    return audit.smoke.SmokeArm(name, "synthetic", overrides, "")


def random_overrides(**changes):
    return {"risk_selection_mode": "EC", "risk_selection_control": "random",
            "risk_selection_random_rates": {"E:BUY": 1., "E:SELL": 1.},
            "risk_selection_random_seed": 19, "risk_selection_random_scope": "test-only",
            **changes}


def test_shared_policy_file_read_once_before_any_arm(tmp_path, monkeypatch):
    path = tmp_path / "policy.json"
    path.write_text(json.dumps(policy_payload()))
    reads = []
    read_text = type(path).read_text

    def read_once(self, *args, **kwargs):
        reads.append(self)
        return read_text(self, *args, **kwargs)

    monkeypatch.setattr(type(path), "read_text", read_once)
    result = audit._load_risk_policy_for_arms(path, [
        arm("B"), arm("EC", risk_selection_mode="EC"), arm("R", **random_overrides()),
        arm("Flat", risk_selection_mode="E", risk_selection_control="flat"),
    ], engine="python")
    assert result == policy_payload()
    assert reads == [path]


@pytest.mark.parametrize("mode", ["bad", None, [], {}])
def test_unknown_policy_mode_never_silently_becomes_baseline(mode):
    with pytest.raises(ValueError, match="risk_selection_mode"):
        audit._load_risk_policy_for_arms(None, [arm("bad", risk_selection_mode=mode)],
                                        engine="python")


def test_policy_loader_keeps_baseline_and_labels_separate(tmp_path):
    path = tmp_path / "policy.json"
    path.write_text(json.dumps(policy_payload()))
    assert audit._load_risk_policy_for_arms(None, [arm("B")], engine="python") is None
    assert audit._load_risk_policy_for_arms(None, [arm("one", risk_selection_intervention={
        "opportunity_id": "one", "action": "WAIT",
    })], engine="python") is None
    with pytest.raises(ValueError, match="require --risk-selection-policy"):
        audit._load_risk_policy_for_arms(None, [arm("E", risk_selection_mode="E")], engine="python")
    for options, arms, message in (
        ({"engine": "cpp"}, [arm("B")], "engine python"),
        ({"engine": "python", "paired": True}, [arm("B")], "single-intervention"),
        ({"engine": "python"}, [arm("B", risk_selection_policy={})], "not arm overrides"),
    ):
        with pytest.raises(ValueError, match=message):
            audit._load_risk_policy_for_arms(path, arms, **options)


@pytest.mark.parametrize("payload", [None, [], {}])
def test_present_policy_file_cannot_silently_become_no_model(tmp_path, payload):
    path = tmp_path / "policy.json"
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="policy"):
        audit._load_risk_policy_for_arms(path, [arm("E", risk_selection_mode="E")],
                                        engine="python")


def test_flat_without_policy_and_random_reference_requirement():
    flat = arm("Flat", risk_selection_mode="E", risk_selection_control="flat")
    assert audit._load_risk_policy_for_arms(None, [flat], engine="python") is None
    with pytest.raises(ValueError, match="engine python"):
        audit._load_risk_policy_for_arms(None, [flat], engine="cpp")
    with pytest.raises(ValueError, match="require --risk-selection-policy"):
        audit._load_risk_policy_for_arms(None, [arm("R", **random_overrides())], engine="python")
    with pytest.raises(ValueError, match="single-intervention"):
        audit._load_risk_policy_for_arms(None, [flat], engine="python", paired=True)


@pytest.mark.parametrize("overrides, message", [
    ({"risk_selection_control": None}, "risk_selection_control"),
    ({"risk_selection_control": []}, "risk_selection_control"),
    ({"risk_selection_control": "unknown"}, "risk_selection_control"),
    ({"risk_selection_control": "flat"}, "Flat uses mode E"),
    (random_overrides(risk_selection_random_seed=True), "integer seed"),
    (random_overrides(risk_selection_random_scope="  "), "integer seed and scope"),
    (random_overrides(risk_selection_random_rates={"E:BUY": True}), "not bool"),
    (random_overrides(risk_selection_random_rates={"E:BUY": float("nan")}), "finite"),
    (random_overrides(risk_selection_random_rates={"E:BUY": 1.1}), "within"),
])
def test_control_configuration_fails_before_market_loading(tmp_path, overrides, message):
    path = tmp_path / "policy.json"
    path.write_text(json.dumps(policy_payload()))
    with pytest.raises(ValueError, match=message):
        audit._load_risk_policy_for_arms(path, [arm("control", **overrides)], engine="python")


def test_flat_cli_requires_continuous_funding_without_reading_market(monkeypatch):
    monkeypatch.setattr(audit.bt, "configure_symbol", lambda *_a, **_kw: None)
    monkeypatch.setattr(audit, "_arm_map", lambda: {
        "Flat": arm("Flat", risk_selection_mode="E", risk_selection_control="flat"),
    })
    with pytest.raises(SystemExit, match="--continuous and --funding-history"):
        audit.main(["--days", "2026-01-01", "--arms", "Flat", "--engine", "python",
                    "--replay-purpose", "diagnostic"])


def test_single_intervention_helper_rejects_learned_baseline_before_running():
    with pytest.raises(ValueError, match="baseline mode B"):
        audit._risk_pair_arms([
            arm("B", risk_selection_mode="E"),
            arm("wait", risk_selection_mode="E", risk_selection_intervention={
                "opportunity_id": "one", "action": "WAIT",
            }),
        ], "B")
    with pytest.raises(ValueError, match="baseline mode B"):
        audit._risk_pair_arms([arm("B", risk_selection_control="flat")], "B")


def test_complete_policy_arms_share_input_not_account_and_never_create_pair_labels(monkeypatch):
    from tests.test_python_planned_maintenance_replay import _async_fifo_params, _inputs, _params

    day = "2026-01-01"
    start_ms = int(audit._day_start_ts(day) * 1000)
    trades, bbo = _inputs(crossing_fill_ts_ms=500)
    trades["transact_time"] += start_ms
    bbo = replace(bbo, ts_ms=bbo.ts_ms + start_ms)
    window = {"trades": trades, "bbo_data": bbo, "var_ts_ms": np.array([start_ms]),
              "var_ssq": np.array([1.]), "l2_data": None, "ml_data": None,
              "var_ti": None, "var_retsq": None}
    loads = []
    monkeypatch.setattr(audit.bt, "configure_symbol", lambda *_a, **_kw: None)
    monkeypatch.setattr(audit.smoke, "_load_window", lambda *_a: loads.append(1) or window)
    monkeypatch.setattr(audit, "build_configured_cooldown_policy_adapter", lambda **_kw: None)
    base = {**_params(), **_async_fifo_params(new=(2., 5., 30.)),
            "risk_selection_policy": policy_payload(), "planned_quote_stop_ts_ms": 0,
            "requote_threshold_bps": 1., "maker_fee": .0001,
            "trace_fills_max": 1000}
    result = audit._run_day_campaign_audit(
        day=day, symbol="BTCUSDC", base=base,
        arms=[arm("B"), arm("E", risk_selection_mode="E"), arm("EC", risk_selection_mode="EC"),
              arm("R", **random_overrides()),
              arm("Flat", risk_selection_mode="E", risk_selection_control="flat"),
              arm("R_missing", **random_overrides(
                  risk_selection_mode="E", risk_selection_random_rates={"E:BUY": 1.},
              ))],
        engine="python", day_initial={}, day_live_state=None, use_initial_state=False,
        continuous_days=[day], replay_end_ts_ms=start_ms + 4000,
        funding_events=[{"fundingTime": start_ms + 2500, "markPrice": 100., "fundingRate": .01}],
    )
    assert len(loads) == 1
    assert not result["risk_selection_paired_labels"]
    baseline, entry, combined, random, flat, partial_random = result["daily_rows"]
    assert entry["risk_selection_wait_count"] > 0
    assert combined["risk_selection_policy_decision_count"] > 0
    assert "risk_selection_mode" not in baseline
    assert baseline["replay_net_pnl"] != entry["replay_net_pnl"]
    for row in result["daily_rows"]:
        assert row["replay_net_pnl"] == pytest.approx(
            row["replay_pnl"] + row["funding_cashflow_usdc"])
    assert {row["arm"] for row in result["risk_selection_opportunity_rows"]} == {
        "B", "E", "EC", "R", "Flat", "R_missing",
    }
    for control in (random, flat):
        assert control["risk_selection_policy_id"] == ""
        assert control["risk_selection_policy_decision_count"] == 0
        assert control["risk_selection_policy_change_count"] == 0
        assert control["risk_selection_wait_count"] == 0
        assert control["risk_selection_control_wait_count"] > 0
        assert control["risk_selection_control_decision_count"] > 0
        assert control["risk_selection_control_change_count"] > 0
        assert json.loads(control["risk_selection_control_fallback_counts"]) == {}
    assert random["risk_selection_reference_evaluation_count"] > 0
    assert random["risk_selection_reference_policy_id"] == policy_payload()["policy_id"]
    assert random["risk_selection_random_seed"] == 19
    assert random["risk_selection_random_scope"] == "test-only"
    assert json.loads(random["risk_selection_random_rates"]) == {"E:BUY": 1., "E:SELL": 1.}
    assert flat["risk_selection_reference_evaluation_count"] == 0
    assert flat["risk_selection_reference_policy_id"] == ""
    assert flat["replay_net_pnl"] == 0
    assert json.loads(partial_random["risk_selection_control_fallback_counts"])[
        "random_veto_rate_missing"
    ] > 0
    for row in result["risk_selection_opportunity_rows"]:
        if row["arm"] in {"R", "Flat", "R_missing"}:
            assert row["value_delta_usdc"] is None
            assert row["policy_id"] == ""
