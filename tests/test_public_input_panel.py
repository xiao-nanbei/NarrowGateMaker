import numpy as np
import pandas as pd
import pytest
import hashlib
import json
from pathlib import Path

from research.families.f03_causal_13_head import public_input_panel as panel


def selection_identity():
    return {"spec_sha256": "spec", "feature_manifest_sha256": "panel",
        "feature_dag_sha256": "features", "source_manifest_sha256": "source",
        "train_source_identity_sha256": "inputs", "fit_days": ["fit"],
        "selection_days": ["validation"], "refit_days": ["fit", "validation"],
        "sample_weight_policy": {"half_life_days": 120}, "external_panel_read_during_fit": False}


def calibration_fixture(tmp_path):
    from research.families.f03_causal_13_head.time_weighted_evaluation import TRAIN_DAYS
    profile = dict(profile_id="controlled", clock_policy="strict_exchange", market_delay_ns=1,
                   processing_ns=0, allowed_lateness_ns=1, max_book_age_ns=100)
    rows, sources = [], []
    for day in TRAIN_DAYS:
        consumer = {"plan": {"market_id": "BTCUSDC", "observation_profile": profile},
                    "source_bundles": [{"path": "/synthetic/"+day, "sha256": "source"+day}]}
        path = tmp_path/(day+".json")
        path.write_text(json.dumps(consumer))
        rows.append({"day": day, "consumer_manifest_path": str(path),
                     "consumer_manifest_sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
        sources.append({"day": day, "source_bundles": [{"day": day, "manifest_sha256": "source"+day}]})
    p3 = {"schema_version": "narrowgate_p3_touch_calibration.v4", "model_type": "empirical_survival",
        "metadata": {"fit_days": list(TRAIN_DAYS), "event_type": "touch", "horizon_s": 10.,
            "distance_unit": "USDC_per_BTC", "queue_included": False, "quote_tick_size": 0.1, "daily_inputs": sources,
            "plan": {"market_id": "BTCUSDC", "fit_days": list(TRAIN_DAYS), "observation_profile": profile}}}
    return p3, rows


def test_new_identity_cannot_claim_legacy_features_or_old_p3(tmp_path):
    from data.observation import CONTRACT as OBS, FEATURE_CONTRACT
    from data.tardis_input import CONTRACT as INPUT
    from research.families.f03_causal_13_head.time_weighted_evaluation import TRAIN_DAYS
    p3 = tmp_path/'p3.json'
    calibration, rows = calibration_fixture(tmp_path)
    p3.write_text(json.dumps(calibration))
    value = dict(schema="f03.public_feature_panel.v1", symbol="BTCUSDC", source_profile="tardis_only",
        input_contract_id=INPUT, observation_contract_id=OBS, feature_contract_id=FEATURE_CONTRACT,
        decision_time_semantics="feature_ready_index", reference_market=None,
        feature_cols=list(panel.FEATURE_COLUMNS), split={"train": list(TRAIN_DAYS)},
        label_contract_id="controlled_label", split_manifest_id="controlled_split", daily_inputs=rows,
        label_quote_calibration={"path": str(p3), "sha256": hashlib.sha256(p3.read_bytes()).hexdigest()})
    manifest = tmp_path/'public_feature_manifest.json'
    manifest.write_text(json.dumps(value))
    identity = panel.training_identity(manifest)
    assert identity["feature_dag_id"] == FEATURE_CONTRACT
    assert identity["reference_trade_count_unit"] == "unavailable"
    assert identity["feature_timestamp_semantics"] == "feature_ready_index"
    assert identity["feature_sampling_interval_ms"] == 10_000
    assert "feature_bucket_ms" not in identity
    value["feature_timestamp_semantics"] = "left_label_bucket_end"
    manifest.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="clock declarations"):
        panel.training_identity(manifest)
    value.pop("feature_timestamp_semantics")
    value['feature_cols'].append('retired_column')
    manifest.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="feature/split"):
        panel.training_identity(manifest)


def test_public_model_contract_requires_all_heads_and_exact_schema(tmp_path):
    from strategy.model_contract import REQUIRED_MODEL_HEADS
    identity = dict(input_contract_id="synthetic", observation_contract_id="synthetic",
        feature_contract_id="synthetic", label_contract_id="synthetic", split_manifest_id="synthetic")
    for head in REQUIRED_MODEL_HEADS:
        (tmp_path/(head+'.txt')).write_text('synthetic booster, not a trained model')
        (tmp_path/(head+'_meta.json')).write_text(json.dumps({**identity, 'name': head,
            'feature_timestamp_semantics': 'feature_ready_index', 'feature_cutoff_semantics': 'feature_ready_index',
            'train_only_selection': selection_identity(), 'feature_cols': list(panel.FEATURE_COLUMNS)}))
    with pytest.raises(ValueError, match="all 13"):
        panel.publish_model_contract(tmp_path, identity, REQUIRED_MODEL_HEADS[:-1], selection_contract=selection_identity())
    result = panel.publish_model_contract(tmp_path, identity, REQUIRED_MODEL_HEADS, selection_contract=selection_identity())
    assert len(result['heads']) == 13 and result['reference_market'] is None
    (tmp_path/(REQUIRED_MODEL_HEADS[0]+'_meta.json')).write_text(json.dumps({'feature_cols': ['old']}))
    with pytest.raises(ValueError, match="schema"):
        panel.publish_model_contract(tmp_path, identity, REQUIRED_MODEL_HEADS, selection_contract=selection_identity())


@pytest.mark.parametrize("mutation", ["source", "delay", "unit", "fit_days", "tick"])
def test_calibration_rejects_wrong_parent(tmp_path, mutation):
    p3, rows = calibration_fixture(tmp_path)
    consumer = json.loads(Path(rows[0]["consumer_manifest_path"]).read_text())
    panel.validate_calibration_consumer(p3, consumer, rows[0]["day"])
    if mutation == "source":
        consumer["source_bundles"][0]["sha256"] = "different"
    elif mutation == "delay":
        consumer["plan"]["observation_profile"]["market_delay_ns"] += 1
    elif mutation == "unit":
        p3["metadata"]["distance_unit"] = "bps"
    elif mutation == "tick":
        p3["metadata"]["quote_tick_size"] = 1.
    else:
        p3["metadata"]["fit_days"] = []
    with pytest.raises(ValueError, match="P3"):
        panel.validate_calibration_consumer(p3, consumer, rows[0]["day"])


def test_calibration_allows_relocation_and_declared_frame_cadence(tmp_path):
    p3, rows = calibration_fixture(tmp_path)
    consumer = json.loads(Path(rows[0]["consumer_manifest_path"]).read_text())
    consumer["source_bundles"][0]["path"] = "/relocated/"+rows[0]["day"]
    consumer["plan"]["observation_profile"]["feature_period_ns"] = 10_000_000_000
    panel.validate_calibration_consumer(p3, consumer, rows[0]["day"])


def test_label_entry_rejects_wrong_calibration_before_reading_tables(tmp_path, monkeypatch):
    import yaml
    from types import SimpleNamespace
    p3, rows = calibration_fixture(tmp_path)
    consumer = json.loads(Path(rows[0]["consumer_manifest_path"]).read_text())
    p3["metadata"]["distance_unit"] = "bps"
    (tmp_path/"touch_probability.json").write_text(json.dumps(p3))
    config = tmp_path/"config.yaml"
    config.write_text(yaml.safe_dump({"tick_size": .1, "ml": {"model_dir": str(tmp_path)}}))
    monkeypatch.setattr("data.runtime.ConsumerBundle", lambda _: SimpleNamespace(manifest=consumer))
    with pytest.raises(ValueError, match="P3"):
        panel.build_label_day(tmp_path, day=rows[0]["day"], config_path=config, output=tmp_path/"labels")
    assert not (tmp_path/"labels").exists()


@pytest.mark.parametrize("field", ["sample_weight_policy", "spec_sha256", "train_source_identity_sha256", "label_contract_id"])
def test_publication_rejects_one_mixed_head(tmp_path, field):
    from strategy.model_contract import REQUIRED_MODEL_HEADS
    identity = {k: "synthetic" for k in ("input_contract_id", "observation_contract_id",
        "feature_contract_id", "label_contract_id", "split_manifest_id")}
    for head in REQUIRED_MODEL_HEADS:
        meta = {**identity, "name": head, "feature_cols": list(panel.FEATURE_COLUMNS),
                "feature_timestamp_semantics": "feature_ready_index", "feature_cutoff_semantics": "feature_ready_index",
                "train_only_selection": selection_identity()}
        if head == REQUIRED_MODEL_HEADS[-1]:
            if field == "label_contract_id":
                meta[field] = "wrong"
            else:
                meta["train_only_selection"][field] = {"half_life_days": 60} if field == "sample_weight_policy" else "wrong"
        (tmp_path/(head+"_meta.json")).write_text(json.dumps(meta))
        (tmp_path/(head+".txt")).write_text("controlled")
    with pytest.raises(ValueError, match="identity"):
        panel.publish_model_contract(tmp_path, identity, REQUIRED_MODEL_HEADS, selection_contract=selection_identity())
    assert not (tmp_path/"public_input_model.json").exists()


def test_public_source_identity_binds_every_training_receipt(tmp_path):
    consumer = tmp_path/'consumer.json'
    consumer.write_text('{}')
    manifest = tmp_path/'panel.json'
    manifest.write_text(json.dumps({'daily_inputs': [{'day': '2025-08-01',
        'consumer_manifest_path': str(consumer),
        'consumer_manifest_sha256': hashlib.sha256(consumer.read_bytes()).hexdigest()}]}))
    panel.training_source_identity(manifest, ['2025-08-01'])
    with pytest.raises(ValueError, match="every declared"):
        panel.training_source_identity(manifest, ['2025-08-01', '2025-08-04'])
    consumer.write_text('{"changed":true}')
    with pytest.raises(ValueError, match="identity changed"):
        panel.training_source_identity(manifest, ['2025-08-01'])


def test_frame_ready_time_and_visible_variance_not_future(monkeypatch):
    decisions = np.array([70, 80], dtype=np.int64)*1_000_000_000
    frames = pd.DataFrame({name: [1., 1.] for name in panel.FEATURE_COLUMNS})
    frames["cutoff_ns"] = decisions
    frames["max_dependency_ready_ns"] = decisions - 1
    bars = pd.DataFrame({"end_ns": np.arange(1, 102)*1_000_000_000,
                         "ready_ns": np.arange(1, 102)*1_000_000_000 + 1,
                         "close": np.arange(101.)**2, "coverage": "observed"})
    outcomes = pd.DataFrame(index=pd.date_range("1970-01-01", periods=120, freq="s", tz="UTC"))
    seen = []

    def labels(frame, outcome, **kwargs):
        seen.append(kwargs)
        assert frame.index.asi8.tolist() == decisions.tolist()
        assert "native_packet_count_10s" not in frame
        return frame

    monkeypatch.setattr(panel, "add_labels", labels)
    result = panel.label_frames(frames, outcomes, config_path=None, visible_bars=bars)
    bars.loc[bars.ready_ns > decisions[-1], "close"] = 1e15
    panel.label_frames(frames, outcomes, config_path=None, visible_bars=bars)
    np.testing.assert_array_equal(seen[0]["decision_variance"], seen[1]["decision_variance"])
    assert np.isfinite(seen[0]["decision_variance"]).all()
    assert result.attrs["reference_market"] is None
    frames.loc[0, "max_dependency_ready_ns"] = decisions[0]+1
    with pytest.raises(ValueError, match="future feature"):
        panel.label_frames(frames, outcomes, config_path=None, visible_bars=bars)
def test_training_input_plan_keeps_frozen_profile_and_restricts_days():
    from research.families.f03_causal_13_head.public_input_panel import training_input_plan
    import pytest
    import pandas as pd
    template = {"observation_profile": {"feature_period_ns": 10_000_000_000}}
    first = training_input_plan(template, "2025-08-01")
    assert first["start_ns"] == pd.Timestamp("2025-08-01", tz="UTC").value
    later = training_input_plan(template, "2025-08-04")
    assert later["source_start_day"] == "2025-08-03"
    assert later["start_ns"] == pd.Timestamp("2025-08-04", tz="UTC").value-120_000_000_000
    assert later["observation_profile"] == template["observation_profile"]
    assert later["include_outcome_bars"] is True
    with pytest.raises(ValueError):
        training_input_plan(template, "2025-08-02")
