"""Synthetic F04 contract and actual signal-entry tests; no purchased bytes."""

from datetime import datetime
import hashlib
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest

from research.families.f04_external_market_alpha import tardis_direction as direction
from research.families.f04_external_market_alpha import daylight_stage
from research.families.f04_external_market_alpha import tardis_economic as economic
from research.families.f04_external_market_alpha.tardis_comparison import compare_economic_pair
from research.families.f04_external_market_alpha.tardis_economic import (
    daylight_budget_seconds,
    verified_funding,
)
from strategy.signal import Prediction, SignalEngine


SHANGHAI = ZoneInfo("Asia/Shanghai")


@pytest.mark.parametrize("hour,minute,allowed", [
    (7, 59, False), (8, 0, True), (19, 59, True),
    (20, 0, False), (21, 59, False), (22, 0, False),
])
def test_f04_full_account_daylight_admission(hour, minute, allowed):
    instant = datetime(2026, 9, 24, hour, minute, tzinfo=SHANGHAI)
    if allowed:
        assert daylight_budget_seconds(instant) >= 7200
    else:
        with pytest.raises(RuntimeError, match="daylight budget"):
            daylight_budget_seconds(instant)


def test_f04_stage_launcher_rechecks_clock_and_stops_at_cutoff(monkeypatch):
    with pytest.raises(RuntimeError, match="daytime budget"):
        daylight_stage.remaining_seconds(datetime(2026, 9, 24, 7, 59, tzinfo=SHANGHAI),
                                         minimum_seconds=3600)
    assert daylight_stage.remaining_seconds(datetime(2026, 9, 24, 8, 0, tzinfo=SHANGHAI),
                                            minimum_seconds=3600) > 3600
    monkeypatch.setattr(daylight_stage, "remaining_seconds", lambda **_kwargs: 7200.)
    events = []

    class FakeProcess:
        pid = 12345
        calls = 0

        def wait(self, *, timeout=None):
            self.calls += 1
            events.append(("wait", timeout))
            if self.calls == 1:
                raise subprocess.TimeoutExpired("synthetic", timeout)
            return -15

    monkeypatch.setattr(daylight_stage.subprocess, "Popen", lambda *args, **kwargs:
                        (events.append(("spawn", args[0], kwargs["start_new_session"])) or FakeProcess()))
    monkeypatch.setattr(daylight_stage.os, "killpg", lambda pid, how: events.append(("kill", pid, how)))
    with pytest.raises(TimeoutError, match="before 22:00"):
        daylight_stage.run_daylight(["synthetic-command"], minimum_seconds=3600)
    assert events[0] == ("spawn", ["synthetic-command"], True)
    assert ("kill", 12345, daylight_stage.signal.SIGTERM) in events


def test_f04_actual_worker_rechecks_clock_and_installs_cutoff(monkeypatch):
    monkeypatch.setattr(daylight_stage, "remaining_seconds", lambda **_kwargs: 7200.)
    recorded = []
    monkeypatch.setattr(daylight_stage.signal, "signal", lambda *args: recorded.append(args))
    monkeypatch.setattr(daylight_stage.signal, "setitimer", lambda *args: recorded.append(args))
    assert daylight_stage.admit_worker(minimum_seconds=3600) == 7200.
    assert recorded[0][0] == daylight_stage.signal.SIGALRM
    assert recorded[1] == (daylight_stage.signal.ITIMER_REAL, 7140.)


def _fake_environment(monkeypatch, *, arm):
    plan = {
        "prediction": {"frozen_F03_inf_model_manifest_sha256": "a" * 64,
                       "reference_columns": ["spread_bps"]},
        "evaluation": {"shard": "development-synthetic",
                       "execution_consumer_manifest_sha256": "b" * 64},
        "observation": {"reference_asof_max_age_ns": 10_000_000_000},
    }
    manifest = {"plan_sha256": "c" * 64}
    model = SimpleNamespace(arm=arm, plan=plan, manifest=manifest,
                            feature_cols=("local", f"{direction.REFERENCE_FEATURE_PREFIX}spread_bps")
                            if arm == "M1" else ("local",),
                            predict=lambda rows: np.asarray([.75 if row[0] == 1 else .25 for row in rows]))
    execution = SimpleNamespace(root=Path("/synthetic/execution"),
        source_paths=lambda: (), manifest={"plan": {"market_id": direction.EXECUTION_MARKET},
        "input_contract_id": "input", "observation_contract_id": "observation",
        "feature_contract_id": "features"})
    monkeypatch.setattr(direction, "ConsumerBundle", lambda *_: execution)
    monkeypatch.setattr(direction, "FEATURE_COLUMNS", ("local",))
    monkeypatch.setattr(direction, "_sha", lambda *_: "a" * 64)
    monkeypatch.setattr(direction, "model_row", lambda frame, *_args, **_kwargs: [frame.local])

    class Base:
        def compute_feature_frames(self, frames):
            return [Prediction(touch_conditioned_up_probability_10000ms=.5, absolute_price_variance_rate_10000ms=3., touch_conditioned_price_change_fraction_10000ms=.01,
                               touch_side_adverse_probability_bid_10000ms=.2, touch_side_adverse_probability_ask_10000ms=.3) for _ in frames]

    monkeypatch.setattr(SignalEngine, "from_public_models", classmethod(
        lambda _cls, *_args, **_kwargs: Base()))
    if arm == "M1":
        monkeypatch.setattr(direction, "_verified_reference_cursor", lambda *_args, **_kwargs: "reference")
        monkeypatch.setattr(direction, "_reference_row", lambda *_args, **_kwargs:
                            ({"spread_bps": 4.}, 100, False))
    return model


@pytest.mark.parametrize("arm", ["M0", "M1"])
def test_f04_real_signal_entry_replaces_only_direction(monkeypatch, arm):
    model = _fake_environment(monkeypatch, arm=arm)
    engine = direction.TardisDirectionSignalEngine(
        "/synthetic/execution", "/synthetic/reference" if arm == "M1" else None,
        "/synthetic/frozen", model)
    frames = [SimpleNamespace(cutoff_ns=100, local=1),
              SimpleNamespace(cutoff_ns=200, local=0)]
    predictions = engine.compute_feature_frames(frames)
    assert [row.touch_conditioned_up_probability_10000ms for row in predictions] == [.75, .25]
    assert [row.absolute_price_variance_rate_10000ms for row in predictions] == [3., 3.]
    assert [row.touch_conditioned_price_change_fraction_10000ms for row in predictions] == [.01, .01]
    assert [row.touch_side_adverse_probability_bid_10000ms for row in predictions] == [.2, .2]
    assert engine.report()["decisions"] == 2
    assert engine.report()["reference_market"] == (
        direction.REFERENCE_MARKET if arm == "M1" else None)


def test_f04_m0_cannot_consume_reference_and_m1_requires_it(monkeypatch):
    for arm, reference in (("M0", "/synthetic/reference"), ("M1", None)):
        model = _fake_environment(monkeypatch, arm=arm)
        with pytest.raises(ValueError, match="reference|BTCUSDT"):
            direction.TardisDirectionSignalEngine(
                "/synthetic/execution", reference, "/synthetic/frozen", model)


def test_f04_rejects_wrong_frozen_base_identity(monkeypatch):
    model = _fake_environment(monkeypatch, arm="M0")
    monkeypatch.setattr(direction, "_sha", lambda *_: "wrong")
    with pytest.raises(ValueError, match="frozen F03/inf"):
        direction.TardisDirectionSignalEngine(
            "/synthetic/execution", None, "/synthetic/frozen", model)


def test_f04_real_panel_entry_preserves_actual_end_and_denominator(tmp_path):
    from data.runtime import ConsumerBundle
    from research.families.f04_external_market_alpha.reference_input_production import (
        derive_reference_input,
    )
    from test_f04_reference_input_production import _spec

    spec = _spec(tmp_path)
    execution_root = Path(spec["execution_root"])
    day = spec["unit"]
    frames = list(ConsumerBundle(execution_root).frames())
    assert len(frames) >= 3
    # Real mirrors may carry an accepted execution feature bundle without the
    # producer machine's absolute raw-fact directory. Panel fitting is allowed;
    # economic replay still requires those facts independently.
    execution_manifest_path = execution_root / "manifest.json"
    execution_manifest = json.loads(execution_manifest_path.read_text())
    for source in execution_manifest["source_bundles"]:
        source["path"] = str(tmp_path / "missing-original-facts" / Path(source["path"]).name)
    execution_manifest_path.write_text(json.dumps(execution_manifest))
    assert not Path(execution_manifest["source_bundles"][0]["path"]).exists()
    ready = [frame.cutoff_ns for frame in frames[:3]]
    label_root = tmp_path / "labels"
    label_root.mkdir()
    day_end = pd.Timestamp(day, tz="UTC").value + 86_400_000_000_000
    label_rows = []
    for i, frame in enumerate(frames[:3]):
        values = dict(frame.values)
        values[direction.LABEL] = float(i) if i < 2 else float("nan")
        values[direction.END] = (pd.Timestamp(ready[i] + 1, unit="ns", tz="UTC") if i == 0
                                 else pd.Timestamp(day_end, unit="ns", tz="UTC") if i == 1
                                 else pd.NaT)
        label_rows.append(values)
    labels = pd.DataFrame(label_rows, index=pd.to_datetime(ready, utc=True, unit="ns"))
    labels.to_parquet(label_root / "labels.parquet")
    digest = lambda path: hashlib.sha256(Path(path).read_bytes()).hexdigest()
    label_manifest = {"schema": "f03.public_label_day.v1", "day": day,
                      "consumer_manifest_sha256": digest(execution_root / "manifest.json"),
                      "feature_cols": list(direction.FEATURE_COLUMNS),
                      "labels_sha256": digest(label_root / "labels.parquet")}
    (label_root / "manifest.json").write_text(json.dumps(label_manifest))
    contract_path = Path(spec["contract_path"])
    contract = json.loads(contract_path.read_text())
    contract["status"] = "frozen_before_reference_production_and_evaluation_outcome_read"
    contract["prediction"] = {"target": "existing F03 label_touch_conditioned_up_probability_10000ms, binary",
                              "reference_columns": ["spread_bps"]}
    contract["observation"]["reference_asof_max_age_ns"] = 10_000_000_000
    contract["bound_existing_fit_inputs"][day]["execution_consumer_manifest_sha256"] = digest(
        execution_manifest_path)
    contract["bound_existing_fit_inputs"][day]["label_manifest_sha256"] = digest(label_root / "manifest.json")
    contract_path.write_text(json.dumps(contract))
    reference_root = tmp_path / "reference-input"
    assert derive_reference_input(spec, reference_root)["status"] == "completed"
    output = tmp_path / "training-view"
    receipt = direction.build_training_panel(contract_path, {day: execution_root},
        {day: reference_root}, {day: label_root}, output)
    readback = pd.read_parquet(output / "panel.parquet")
    assert receipt["rows"] == len(readback) == 3
    assert receipt["day_counts"][day]["fit_eligible"] == 1
    assert readback["fit_eligible"].tolist() == [True, False, False]
    assert str(readback["actual_outcome_end_ns"].dtype) == "Int64"
    assert readback.loc[0, "actual_outcome_end_ns"] == ready[0] + 1
    assert readback.loc[1, "actual_outcome_end_ns"] == day_end
    assert pd.isna(readback.loc[2, "actual_outcome_end_ns"])
    assert pd.isna(readback.loc[0, f"{direction.REFERENCE_FEATURE_PREFIX}spread_bps"])
    with pytest.raises(FileExistsError, match="exists"):
        direction.build_training_panel(contract_path, {day: execution_root},
            {day: reference_root}, {day: label_root}, output)


def test_f04_fixed_pair_fits_real_lightgbm_and_reloads_without_selection(tmp_path, monkeypatch):
    day = "2025-08-01"
    monkeypatch.setattr(direction, "FEATURE_COLUMNS", ("local",))
    execution_sha, label_sha, reference_sha = "b" * 64, "d" * 64, "e" * 64
    contract = {"schema": "narrowgate.f04.tardis_two_market_first_batch.v1",
                "status": "frozen_before_reference_production_and_evaluation_outcome_read",
                "markets": {"execution": direction.EXECUTION_MARKET,
                            "reference": direction.REFERENCE_MARKET,
                            "reference_currency_conversion": "none; only currency-invariant reference features admitted"},
                "fit_days_utc": [day], "bound_existing_fit_inputs": {day: {
                    "execution_consumer_manifest_sha256": execution_sha,
                    "label_manifest_sha256": label_sha}},
                "prediction": {"target": "existing F03 label_touch_conditioned_up_probability_10000ms, binary",
                               "reference_columns": ["spread_bps"],
                               "frozen_F03_inf_model_manifest_sha256": "a" * 64},
                "budget": {"new_fit_calls": 2, "inner_selection_calls": 0, "refits": 0,
                           "new_full_account_replays_M0": 1, "new_full_account_replays_M1": 1,
                           "F03_reference_replay_if_existing_not_strictly_comparable": 1,
                           "additional_candidates_or_evaluation_accounts": 0,
                           "initial_independent_workers": 1, "maximum_independent_workers": 2}}
    contract_path = tmp_path / "frozen.json"
    contract_path.write_text(json.dumps(contract))
    digest = lambda path: hashlib.sha256(Path(path).read_bytes()).hexdigest()
    panel_root = tmp_path / "panel"
    panel_root.mkdir()
    values = np.linspace(-1, 1, 400)
    first_ns = pd.Timestamp(day, tz="UTC").value
    decision_ns = first_ns + np.arange(400, dtype=np.int64) * 10_000_000_000
    panel = pd.DataFrame({"day": [day] * 400,
                          "decision_ns": decision_ns,
                          "actual_outcome_end_ns": pd.array(decision_ns + 1, dtype="Int64"),
                          "reference_frame_cutoff_ns": pd.array(decision_ns - 1, dtype="Int64"),
                          direction.LABEL: (values > 0).astype(float),
                          "fit_eligible": [True] * 400,
                          "local": values,
                          f"{direction.REFERENCE_FEATURE_PREFIX}spread_bps": values[::-1]})
    panel.to_parquet(panel_root / "panel.parquet", index=False)
    (panel_root / "manifest.json").write_text(json.dumps({
        "schema": direction.PANEL_SCHEMA, "plan_sha256": digest(contract_path),
        "panel_sha256": digest(panel_root / "panel.parquet"),
        "parents": [{"day": day, "execution_consumer_manifest_sha256": execution_sha,
                     "label_manifest_sha256": label_sha,
                     "reference_consumer_manifest_sha256": reference_sha}],
        "day_counts": {day: {"declared_decisions": 400, "fit_eligible": 400}},
        "rows": 400, "feature_cols_M0": ["local"],
        "feature_cols_M1": ["local", f"{direction.REFERENCE_FEATURE_PREFIX}spread_bps"]}))
    fitted = tmp_path / "fitted"
    receipt = direction.fit_direction_pair(contract_path, panel_root, fitted)
    assert receipt["fit_calls"] == 2
    assert receipt["inner_selection_calls"] == receipt["refits"] == 0
    for arm in ("M0", "M1"):
        model = direction.DirectionModel(fitted, arm, plan_path=contract_path)
        original_predict = model.booster.predict
        thread_budgets = []

        def bounded_predict(matrix, *, _original=original_predict,
                            _budgets=thread_budgets, **kwargs):
            _budgets.append(kwargs.get("num_threads"))
            return _original(matrix, **kwargs)

        monkeypatch.setattr(model.booster, "predict", bounded_predict)
        assert model.predict(np.ones((3, len(model.feature_cols)))).shape == (3,)
        assert thread_budgets == [2]
        assert receipt["arms"][arm]["fitted_rows"] == 400
    manifest_path = fitted / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["arms"]["M0"]["target"] = "label_dir_10s"
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="current touch-conditioned direction target"):
        direction.DirectionModel(fitted, "M0", plan_path=contract_path)
    manifest["arms"]["M0"]["target"] = direction.LABEL
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(FileExistsError, match="exists"):
        direction.fit_direction_pair(contract_path, panel_root, fitted)
    panel.loc[0, "actual_outcome_end_ns"] = first_ns + 86_400_000_000_000
    panel.to_parquet(panel_root / "panel.parquet", index=False)
    metadata = json.loads((panel_root / "manifest.json").read_text())
    metadata["panel_sha256"] = digest(panel_root / "panel.parquet")
    (panel_root / "manifest.json").write_text(json.dumps(metadata))
    with pytest.raises(ValueError, match="day counts changed|eligibility bypassed"):
        direction.fit_direction_pair(contract_path, panel_root, tmp_path / "unsafe-fit")


def test_f04_verified_funding_keeps_observed_one_ms_offset(tmp_path):
    start = pd.Timestamp("2025-08-29T00:00:00Z").value
    end = pd.Timestamp("2025-08-31T00:00:00Z").value
    days = ["2025-08-29", "2025-08-30", "2025-08-31"]
    digests = {}
    for day in days:
        midnight = pd.Timestamp(day, tz="UTC").value
        clocks = [midnight + hour * 3600 * 10**9 + 1_000_000 for hour in (0, 8, 16)]
        rows = [{"symbol": "BTCUSDC", "fundingTime": clock // 1_000_000,
                 "fundingRate": .0001, "markPrice": 100_000.} for clock in clocks]
        path = tmp_path / f"{day}.parquet"
        pd.DataFrame(rows).to_parquet(path, index=False)
        digests[day] = hashlib.sha256(path.read_bytes()).hexdigest()
    plan = {"evaluation": {"account_start_utc": "2025-08-29T00:00:00Z",
                           "account_end_exclusive_utc": "2025-08-31T00:00:00Z"},
            "economic_environment": {"funding_day_sha256": digests}}
    funding = verified_funding(plan, tmp_path)
    assert funding["coverage_start_ns"] == start
    assert funding["coverage_end_ns"] == end
    assert len(funding["events"]) == 6
    assert funding["events"][0]["settlement_ns"] == start + 1_000_000
    assert funding["events"][-1]["settlement_ns"] == end - 8 * 3600 * 10**9 + 1_000_000
    assert all(event["settlement_ns"] < end for event in funding["events"])
    pd.DataFrame(columns=["symbol", "fundingTime", "fundingRate", "markPrice"]).to_parquet(
        tmp_path / "2025-08-30.parquet", index=False)
    with pytest.raises(ValueError, match="identity changed"):
        verified_funding(plan, tmp_path)


def test_f04_verified_funding_includes_observed_exact_account_end(tmp_path):
    end = pd.Timestamp("2025-08-31T00:00:00Z").value
    digests = {}
    for day in ("2025-08-29", "2025-08-30", "2025-08-31"):
        midnight = pd.Timestamp(day, tz="UTC").value
        clocks = [midnight + hour * 3600 * 10**9 + 1_000_000 for hour in (0, 8, 16)]
        if day == "2025-08-31":
            clocks[0] = end
        path = tmp_path / f"{day}.parquet"
        pd.DataFrame([{"symbol": "BTCUSDC", "fundingTime": clock // 1_000_000,
                       "fundingRate": .0001, "markPrice": 100_000.}
                      for clock in clocks]).to_parquet(path, index=False)
        digests[day] = hashlib.sha256(path.read_bytes()).hexdigest()
    plan = {"evaluation": {"account_start_utc": "2025-08-29T00:00:00Z",
                           "account_end_exclusive_utc": "2025-08-31T00:00:00Z"},
            "economic_environment": {"funding_day_sha256": digests}}
    funding = verified_funding(plan, tmp_path)
    assert funding["events"][-1]["settlement_ns"] == end
    assert len(funding["events"]) == 7


def test_f04_economic_failure_preserves_incomplete_stage(tmp_path, monkeypatch):
    monkeypatch.setattr(economic, "daylight_budget_seconds", lambda *_args, **_kwargs: 10_000.)
    monkeypatch.setattr(economic, "_plan", lambda *_args: ({"evaluation": {}}, "plan"))
    monkeypatch.setattr(economic, "_evaluation_bundle", lambda *_args: (
        (_ for _ in ()).throw(RuntimeError("synthetic preflight failure"))))
    output = tmp_path / "economic-arm"
    arguments = dict(arm="M0", plan_path=tmp_path / "plan", execution_root=tmp_path / "input",
                     relocation_manifest=None, reference_root=None,
                     original_execution_manifest=None, pair_model_root=tmp_path / "model",
                     frozen_f03_root=tmp_path / "f03", config_path=tmp_path / "config",
                     timing_path=tmp_path / "timing", funding_root=tmp_path / "funding",
                     output=output)
    with pytest.raises(RuntimeError, match="synthetic preflight failure"):
        economic.run_one_arm(**arguments)
    incomplete = list(tmp_path.glob("economic-arm.*.part"))
    assert not output.exists() and len(incomplete) == 1
    assert json.loads((incomplete[0] / "failure.json").read_text()) == {
        "type": "RuntimeError", "message": "synthetic preflight failure"}
    with pytest.raises(FileExistsError, match="incomplete"):
        economic.run_one_arm(**arguments)


def test_f04_pair_comparison_reconciles_fills_and_refuses_changed_trace(tmp_path):
    dig = lambda path: hashlib.sha256(Path(path).read_bytes()).hexdigest()
    start, end = 1_000_000_000, 5_000_000_000

    def arm(name, filled, account, reference):
        root = tmp_path / name
        root.mkdir()
        traces = {
            "fills.parquet": pd.DataFrame(filled, columns=["fill_sequence", "fill_ts", "side",
                                                         "fill_qty", "quote_px", "fill_fee_usdc"]),
            "orders.parquet": pd.DataFrame([{"order_id": 1, "side": "BUY", "outcome": "FILLED",
                                             "price": 100., "quantity": .001}]),
            "decisions.parquet": pd.DataFrame([{"decision_id": "one", "final_price": 100.,
                                                "final_size": .001, "action": "POST",
                                                "pred_dir": .6 if name == "M0" else .7}]),
        }
        for filename, frame in traces.items():
            frame.to_parquet(root / filename, index=False)
        (root / "utc-equity-marks.json").write_text("[]")
        receipt = {"schema": "narrowgate.f04.first_pair_economic_arm.v1", "arm": name,
                   "plan_sha256": "plan", "source_archive_sha256": "source",
                   "model_manifest_sha256": "pair",
                   "execution_manifest_sha256": "input",
                   "frozen_f03_manifest_sha256": "f03", "config_sha256": "config",
                   "timing_sha256": "timing", "funding_source_identity": ["funding"],
                   "reference_manifest_sha256": reference,
                   "input": {"account_start_ns": start, "input_manifest_id": "input"},
                   "accounting": {"economic_complete": True, "initial_capital": 10_000.,
                       "account_start_ns": start, "account_end_ns": end,
                       "valuation_origin": "delivered_BBO_mid_not_official_mark",
                       "funding_policy_feedback": "post_execution_accounting_only",
                       "tie_policy": "funding_before_equal_time_fills_end_settlement_before_MTM",
                       "valuation_price": 101., **account},
                   "trace_files": {filename: {"sha256": dig(root / filename), "rows": len(frame)}
                                   for filename, frame in traces.items()},
                   "utc_equity_marks_sha256": dig(root / "utc-equity-marks.json"),
                   "trace_coverage": {key: {"unrecorded": 0} for key in (
                       "fills", "order_outcomes", "routed_decisions")}}
        (root / "accounting.json").write_text(json.dumps(receipt))
        return root

    m0 = arm("M0", [{"fill_sequence": 0, "fill_ts": 2000, "side": "BUY",
                     "fill_qty": .001, "quote_px": 100., "fill_fee_usdc": .001}],
             {"terminal_inventory": .001, "realized_trading_pnl": 0.,
              "terminal_unrealized_pnl": .001, "fees": .001,
              "funding_cashflow": 0., "all_in_net_pnl": 0.}, None)
    m1 = arm("M1", [{"fill_sequence": 0, "fill_ts": 2000, "side": "BUY",
                     "fill_qty": .001, "quote_px": 100., "fill_fee_usdc": .001},
                    {"fill_sequence": 1, "fill_ts": 3000, "side": "SELL",
                     "fill_qty": .001, "quote_px": 101., "fill_fee_usdc": .001}],
             {"terminal_inventory": 0., "realized_trading_pnl": .001,
              "terminal_unrealized_pnl": 0., "fees": .002,
              "funding_cashflow": 0., "all_in_net_pnl": -.001}, "reference")
    out = tmp_path / "pair.json"
    result = compare_economic_pair(m0, m1, out)
    assert result["M1_minus_M0_net_pnl_usdc"] == pytest.approx(-.001)
    assert result["M0"]["peak_absolute_inventory_btc"] == pytest.approx(.001)
    assert result["M1"]["fills"] == 2
    assert result["decision_changes"]["changed_direction_prediction"] == 1
    assert result["risk_scope"].startswith("fill-reconstructed")
    with pytest.raises(FileExistsError, match="create-only"):
        compare_economic_pair(m0, m1, out)
    pd.DataFrame([{"decision_id": "one", "final_price": 99., "final_size": .001,
                   "action": "CHANGED", "pred_dir": .7}]).to_parquet(
                       m1 / "decisions.parquet", index=False)
    with pytest.raises(ValueError, match="trace identity changed"):
        compare_economic_pair(m0, m1, tmp_path / "second.json")


def test_f04_eval_locator_rebinding_needs_exact_two_step_parent(tmp_path, monkeypatch):
    from copy import deepcopy
    from dataclasses import asdict

    from data.runtime import ConsumerBundle, ObservationProfile, derive_inputs
    from test_f04_reference_input_production import _facts, DAY, START_NS

    monkeypatch.setattr(economic, "daylight_budget_seconds", lambda *_args, **_kwargs: 10_000.)
    fact = _facts(tmp_path / "facts", "BTCUSDC")
    original = tmp_path / "original"
    profile = ObservationProfile("execution_fixture", "source_timestamp_proxy", 200_000_000,
                                 0, 100_000_000, 1_000_000_000, trade_coverage="observed")
    derive_inputs({"facts_root": str(fact), "market_id": direction.EXECUTION_MARKET,
                   "observation_profile": asdict(profile), "start_ns": START_NS,
                   "end_ns": START_NS + 4_000_000_000}, original)
    digest = lambda path: hashlib.sha256(Path(path).read_bytes()).hexdigest()
    original_sha = digest(original / "manifest.json")
    frozen = deepcopy(json.loads((original / "manifest.json").read_text()))
    frozen["plan"]["facts_root"] = "/previous-host/facts"
    frozen["source_bundles"][0]["path"] = "/previous-host/facts/" + DAY
    frozen["relocated_from_manifest_sha256"] = original_sha
    frozen_root = tmp_path / "frozen-relocated"
    frozen_root.mkdir()
    frozen_path = frozen_root / "manifest.json"
    frozen_path.write_text(json.dumps(frozen))
    for name in ("features", "bars", "depth"):
        filename = frozen["files"][name]["file"]
        (frozen_root / filename).hardlink_to(original / filename)
    original_manifest = tmp_path / "original-manifest.json"
    original_manifest.write_bytes((original / "manifest.json").read_bytes())
    # The original execution Parquet need not be present on the research host.
    for name in ("features", "bars", "depth"):
        (original / frozen["files"][name]["file"]).unlink()
    frozen_sha = digest(frozen_path)
    contract_path = tmp_path / "f04-contract.json"
    contract_path.write_text(json.dumps({
        "schema": "narrowgate.f04.tardis_two_market_first_batch.v1",
        "status": "frozen_before_reference_production_and_evaluation_outcome_read",
        "markets": {"execution": direction.EXECUTION_MARKET,
                    "reference": direction.REFERENCE_MARKET,
                    "reference_currency_conversion":
                    "none; only currency-invariant reference features admitted"},
        "fit_days_utc": [DAY], "bound_existing_fit_inputs": {DAY: {}},
        "prediction": {"target": "existing F03 label_touch_conditioned_up_probability_10000ms, binary"},
        "evaluation": {"execution_consumer_manifest_sha256": frozen_sha,
                       "account_start_utc": pd.Timestamp(START_NS + 1_000_000_000,
                                                         unit="ns", tz="UTC").isoformat(),
                       "account_end_exclusive_utc": pd.Timestamp(START_NS + 4_000_000_000,
                                                                  unit="ns", tz="UTC").isoformat()},
    }))
    source_sha = digest(fact / "manifest.json")
    output = tmp_path / "host-relocated"
    receipt = economic.relocate_evaluation_bundle(
        plan_path=contract_path, original_manifest=original_manifest, frozen_root=frozen_root,
        fact_paths_by_sha={source_sha: str(fact)}, measured_latency_path=None, output=output)
    assert receipt["frozen_manifest_sha256"] == frozen_sha
    assert receipt["execution_parquet_hardlinks"] == 3
    assert economic._evaluation_bundle(output, json.loads(contract_path.read_text()),
                                       frozen_path).source_paths() == [fact.resolve()]
    with pytest.raises(FileExistsError, match="already exists"):
        economic.relocate_evaluation_bundle(
            plan_path=contract_path, original_manifest=original_manifest, frozen_root=frozen_root,
            fact_paths_by_sha={source_sha: str(fact)}, measured_latency_path=None, output=output)
    with pytest.raises(ValueError, match="fact manifest identity changed"):
        economic.relocate_evaluation_bundle(
            plan_path=contract_path, original_manifest=original_manifest, frozen_root=frozen_root,
            fact_paths_by_sha={source_sha: str(tmp_path)}, measured_latency_path=None,
            output=tmp_path / "invalid")

    # Reference derivation is bound to d03, while replay uses d03→e34→host.
    # Both links must be supplied, not inferred from matching filenames.
    reference_fact = _facts(tmp_path / "reference-facts", "BTCUSDT")
    accepted = ConsumerBundle(output)
    reference_profile = deepcopy(accepted.manifest["plan"]["observation_profile"])
    reference_profile.update(profile_id="simulated_reference", market_delay_ns=250_000_000,
                             processing_ns=1_000_000, measured_latency_path=None,
                             measured_latency_sha256=None, measured_latency_market_id=None)
    plan_sha = "c" * 64
    unit = "development-synthetic"
    reference_root = tmp_path / "reference"
    derive_inputs({"source_bundles": [{"path": str(reference_fact),
                                       "sha256": digest(reference_fact / "manifest.json")}],
                   "market_id": direction.REFERENCE_MARKET,
                   "observation_profile": reference_profile,
                   "start_ns": accepted.manifest["plan"]["start_ns"],
                   "end_ns": accepted.manifest["plan"]["end_ns"],
                   "include_outcome_bars": False,
                   "frozen_contract_sha256": plan_sha, "frozen_unit": unit,
                   "execution_parent_manifest_sha256": original_sha,
                   "bound_execution_manifest_sha256": frozen_sha}, reference_root)
    accepted_plan = {"evaluation": {"shard": unit},
                     "observation": {"reference": {key: reference_profile[key] for key in (
                         "profile_id", "clock_policy", "market_delay_ns", "processing_ns",
                         "measured_latency_path", "measured_latency_sha256",
                         "measured_latency_market_id")}}}
    cursor = direction._verified_reference_cursor(reference_root, accepted_plan, accepted,
        plan_sha=plan_sha, unit=unit, bound_execution_sha=frozen_sha,
        relocation_manifest=frozen_path, original_execution_manifest=original_manifest)
    assert cursor.bundle.manifest["plan"]["market_id"] == direction.REFERENCE_MARKET
    with pytest.raises(ValueError, match="reference execution parent"):
        direction._verified_reference_cursor(reference_root, accepted_plan, accepted,
            plan_sha=plan_sha, unit=unit, bound_execution_sha=frozen_sha,
            relocation_manifest=frozen_path)
    changed = json.loads((reference_root / "manifest.json").read_text())
    changed["plan"]["observation_profile"]["max_book_age_ns"] += 1
    (reference_root / "manifest.json").write_text(json.dumps(changed))
    with pytest.raises(ValueError, match="different market, interval or clock"):
        direction._verified_reference_cursor(reference_root, accepted_plan, accepted,
            plan_sha=plan_sha, unit=unit, bound_execution_sha=frozen_sha,
            relocation_manifest=frozen_path, original_execution_manifest=original_manifest)
