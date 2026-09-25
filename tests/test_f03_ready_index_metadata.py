"""D3: actual training entry and public prediction clock regressions."""
from dataclasses import replace
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from data.observation import CONTRACT, FEATURE_CONTRACT, FeatureFrame
from data.tardis_input import CONTRACT as INPUT
from models.replay.public_input import public_predictions
from research.families.f03_causal_13_head import ml_model as trainer
from strategy.signal import SignalEngine


def identity(meaning):
    if meaning == "feature_ready_index":
        return {"feature_timestamp_semantics": meaning, "feature_cutoff_semantics": meaning,
                "public_input_contract": {"decision_time_semantics": meaning}}
    return {"feature_timestamp_semantics": meaning,
            "feature_cutoff_semantics": "strict_exclusive_completed_bucket_end"}


def test_actual_train_one_keeps_panel_clock(monkeypatch):
    meaning = "feature_ready_index"
    monkeypatch.setattr(trainer, "_feature_panel_identity", lambda: identity(meaning))
    frame = pd.DataFrame({"close": np.arange(80, dtype=float),
                          "label_touch_conditioned_price_change_fraction_10000ms": np.arange(80, dtype=float) / 1000})
    frame.attrs["decision_time_semantics"] = meaning
    model, metadata = trainer.train_one("touch_conditioned_price_change_fraction_10000ms", frame.iloc[:60], frame.iloc[60:],
        params_override={"n_estimators": 2, "num_threads": 1, "verbosity": -1,
                         "min_child_samples": 2})
    assert model.booster_.num_feature() == 1
    assert metadata["feature_timestamp_semantics"] == meaning
    assert metadata["feature_sampling_interval_ms"] == 10_000
    assert "feature_bucket_ms" not in metadata


@pytest.mark.parametrize("mutation", ["timestamp", "bucket", "frame"])
def test_train_one_rejects_conflict_before_fitting(monkeypatch, mutation):
    panel = identity("feature_ready_index")
    frame = pd.DataFrame()
    if mutation == "timestamp":
        panel["feature_timestamp_semantics"] = "left_label_bucket_end"
    elif mutation == "bucket":
        panel["feature_bucket_ms"] = 10_000
    else:
        frame.attrs["decision_time_semantics"] = "left_label_bucket_end"
    monkeypatch.setattr(trainer, "_feature_panel_identity", lambda: panel)
    with pytest.raises(ValueError, match="clock declarations|feature_ready_index"):
        trainer.train_one("touch_conditioned_price_change_fraction_10000ms", frame, frame)


@pytest.mark.parametrize("unit", ["ms", "us", "ns"])
@pytest.mark.parametrize("zone", ["UTC", "Asia/Shanghai"])
def test_public_signal_and_prediction_use_ready_index_not_cadence(unit, zone):
    instant = pd.Timestamp("2026-01-01T12:00:10Z").tz_convert(zone).as_unit(unit)
    ns = instant.as_unit("ns").value
    frame = FeatureFrame(INPUT, CONTRACT, FEATURE_CONTRACT, ns, ns - 1,
                         (("close", 100.),), (), (("close", True),))
    engine = SignalEngine(enable_ml=False)
    prediction = engine.compute_signal(feature_frame=frame, decision_ns=ns)
    assert prediction.ts == ns / 1e9
    result = public_predictions([frame], engine, ())
    assert result[0].tolist() == [ns // 1_000_000]
    with pytest.raises(ValueError, match="future feature dependency"):
        public_predictions([replace(frame, max_dependency_ready_ns=ns + 1)], engine, ())


def test_weighting_clock_rejects_retired_offset_protocol():
    assert trainer._weight_policy_decision_offset({
        "schema_version": "narrowgate.f03.time_half_life_daily.v2",
        "decision_clock": "feature_ready_index"}) == pd.Timedelta(0)
    with pytest.raises(ValueError, match="unsupported sample weight policy"):
        trainer._weight_policy_decision_offset({
            "schema_version": "narrowgate.f03.time_half_life_daily.v1"})
    with pytest.raises(ValueError, match="unsupported sample weight policy"):
        trainer._weight_policy_decision_offset({
            "schema_version": "narrowgate.f03.time_half_life_daily.v1",
            "decision_clock": "feature_ready_index"})


def test_actual_train_rejects_legacy_weights_on_ready_panel(monkeypatch):
    monkeypatch.setattr(trainer, "_feature_panel_identity", lambda: identity("feature_ready_index"))
    contract = SimpleNamespace(sample_weight_policy={
        "schema_version": "narrowgate.f03.time_half_life_daily.v1"})
    with pytest.raises(ValueError, match="unsupported sample weight policy"):
        trainer.train_one("touch_conditioned_price_change_fraction_10000ms", pd.DataFrame(), pd.DataFrame(), selection_contract=contract)
