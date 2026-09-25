"""Synthetic fixtures only; no private model bytes or market data."""

from dataclasses import replace
import json

import lightgbm as lgb
import numpy as np
import pytest

from data.observation import CONTRACT, FEATURE_CONTRACT
from data.tardis_input import CONTRACT as INPUT
from research.families.f03_causal_13_head import public_input_panel as panel
from strategy.model_contract import (
    REQUIRED_MODEL_HEADS, ABSOLUTE_PRICE_VARIANCE_SEMANTICS,
    absolute_price_variance_unit_contract, resolve_model_authorization_manifest,
)
from strategy.public_model_contract import digest, validate_public_bundle


@pytest.fixture
def model_bundle(tmp_path):
    names = list(panel.FEATURE_COLUMNS)
    rng = np.random.default_rng(42)
    model = lgb.train({"objective": "regression", "num_threads": 1, "verbosity": -1},
                      lgb.Dataset(rng.normal(size=(80, len(names))), label=np.ones(80),
                                  feature_name=names), num_boost_round=2)
    identity = dict(input_contract_id=INPUT, observation_contract_id=CONTRACT,
                    feature_contract_id=FEATURE_CONTRACT, label_contract_id="synthetic_label",
                    split_manifest_id="synthetic_split")
    selection = {"spec_sha256": "spec", "feature_manifest_sha256": "panel",
        "feature_dag_sha256": "features", "source_manifest_sha256": "source",
        "train_source_identity_sha256": "inputs", "fit_days": ["fit"],
        "selection_days": ["validation"], "refit_days": ["fit", "validation"],
        "sample_weight_policy": {"half_life_days": "inf"}, "external_panel_read_during_fit": False}
    for name in REQUIRED_MODEL_HEADS:
        model.save_model(str(tmp_path / f"{name}.txt"))
        (tmp_path / f"{name}_meta.json").write_text(json.dumps({
            **identity, "name": name, "feature_cols": names,
            "feature_timestamp_semantics": "feature_ready_index", "feature_cutoff_semantics": "feature_ready_index",
            "train_only_selection": selection,
            "volatility_unit_contract": absolute_price_variance_unit_contract("BTCUSDC"),
            "label_semantics": ABSOLUTE_PRICE_VARIANCE_SEMANTICS,
        }))
    panel.publish_model_contract(tmp_path, identity, REQUIRED_MODEL_HEADS, selection_contract=selection)
    (tmp_path / "live_input_authorization.json").write_text(json.dumps({
        "schema": "narrowgate.execution_v1_live_authorization.v1",
        "model_manifest_sha256": digest(tmp_path / "public_input_model.json"),
        "feature_contract_id": FEATURE_CONTRACT,
        "trade_source": "binance_usdm_individual_trade",
        "owner_authorized": True, "economic_promotion_claim": False,
    }))
    return tmp_path


def test_live_authorization_does_not_rewrite_models(model_bundle):
    before = {p.name: digest(p) for p in model_bundle.iterdir()}
    metadata = validate_public_bundle(model_bundle, live=True)
    assert len(metadata) == 13
    assert resolve_model_authorization_manifest(model_bundle, metadata).name == "live_input_authorization.json"
    assert before == {p.name: digest(p) for p in model_bundle.iterdir()}
    (model_bundle / "touch_conditioned_price_change_fraction_10000ms.txt").write_text("changed")
    with pytest.raises(ValueError, match="identity"):
        validate_public_bundle(model_bundle, live=True)


def test_new_publication_rejects_conflicting_clock_without_rewriting_frozen_bytes(model_bundle):
    manifest = json.loads((model_bundle / "public_input_model.json").read_text())
    path = model_bundle / "touch_conditioned_price_change_fraction_10000ms_meta.json"
    metadata = json.loads(path.read_text())
    metadata["feature_timestamp_semantics"] = "left_label_bucket_end"
    path.write_text(json.dumps(metadata))
    before = {p.name: digest(p) for p in model_bundle.iterdir()}
    with pytest.raises(ValueError, match="unambiguous feature_ready_index"):
        panel.publish_model_contract(model_bundle, manifest, REQUIRED_MODEL_HEADS,
                                     selection_contract=metadata["train_only_selection"])
    assert before == {p.name: digest(p) for p in model_bundle.iterdir()}


def test_loaded_boosters_consume_actual_ready_frame_without_bucket_offset(model_bundle):
    from data.observation import FeatureFrame
    from models.replay.public_input import public_predictions
    from strategy.signal import SignalEngine
    ns = 1_767_268_810_000_000_001
    values = tuple((name, 1.) for name in panel.FEATURE_COLUMNS)
    frame = FeatureFrame(INPUT, CONTRACT, FEATURE_CONTRACT, ns, ns - 1,
                         values, (), tuple((name, True) for name in panel.FEATURE_COLUMNS))
    engine = SignalEngine.from_public_models(model_bundle)
    result = public_predictions([frame], engine, ())
    assert result[0].tolist() == [(ns + 999_999) // 1_000_000]
    assert engine._last_prediction.ts == ns / 1e9


def test_missing_live_authorization_fails_closed(model_bundle):
    (model_bundle / "live_input_authorization.json").unlink()
    assert len(validate_public_bundle(model_bundle)) == 13
    with pytest.raises(ValueError, match="regular"):
        validate_public_bundle(model_bundle, live=True)


@pytest.mark.parametrize("field,value", [
    ("volatility_unit_contract", {
        **absolute_price_variance_unit_contract("BTCUSDC"),
        "variance_units": "fraction_squared",
    }),
    ("label_semantics", "standard_deviation"),
])
def test_offline_rejects_identity_consistent_wrong_variance(model_bundle, field, value):
    from strategy.signal import SignalEngine
    path = model_bundle / "absolute_price_variance_rate_10000ms_meta.json"
    meta = json.loads(path.read_text())
    meta[field] = value
    path.write_text(json.dumps(meta))
    manifest_path = model_bundle / "public_input_model.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["heads"]["absolute_price_variance_rate_10000ms"]["metadata_sha256"] = digest(path)
    manifest_path.write_text(json.dumps(manifest))
    for load in (validate_public_bundle, SignalEngine.from_public_models):
        with pytest.raises(ValueError):
            load(model_bundle)


def test_offline_preserves_validated_head_semantics(model_bundle):
    from strategy.signal import SignalEngine
    metadata = validate_public_bundle(model_bundle)
    engine = SignalEngine.from_public_models(model_bundle)
    assert engine._model_metadata == metadata
    assert len(engine._models) == 13
    for name, model in engine._models.items():
        assert model.feature_name() == metadata[name]["feature_cols"]


@pytest.mark.parametrize("damage", ["mixed_head", "feature_order", "model_hash"])
def test_public_entrypoints_reject_artifact_contract_damage(model_bundle, damage):
    from strategy.signal import SignalEngine
    path = model_bundle / "absolute_price_variance_rate_10000ms_meta.json"
    meta = json.loads(path.read_text())
    manifest_path = model_bundle / "public_input_model.json"
    manifest = json.loads(manifest_path.read_text())
    if damage == "mixed_head":
        meta["name"] = "absolute_price_variance_rate_30000ms"
    elif damage == "feature_order":
        meta["feature_cols"] = list(reversed(meta["feature_cols"]))
    else:
        (model_bundle / "absolute_price_variance_rate_10000ms.txt").write_text("changed")
    path.write_text(json.dumps(meta))
    manifest["heads"]["absolute_price_variance_rate_10000ms"]["metadata_sha256"] = digest(path)
    manifest_path.write_text(json.dumps(manifest))
    for load in (validate_public_bundle, SignalEngine.from_public_models):
        with pytest.raises(ValueError):
            load(model_bundle)


def test_offline_checks_booster_internal_order_even_when_metadata_agrees(model_bundle):
    from strategy.signal import SignalEngine
    manifest_path = model_bundle / "public_input_model.json"
    manifest = json.loads(manifest_path.read_text())
    for name in REQUIRED_MODEL_HEADS:
        path = model_bundle / f"{name}_meta.json"
        meta = json.loads(path.read_text())
        meta["feature_cols"] = list(reversed(meta["feature_cols"]))
        path.write_text(json.dumps(meta))
        manifest["heads"][name].update(feature_cols=meta["feature_cols"], metadata_sha256=digest(path))
    manifest_path.write_text(json.dumps(manifest))
    assert len(validate_public_bundle(model_bundle)) == 13
    with pytest.raises(ValueError, match="file/schema"):
        SignalEngine.from_public_models(model_bundle)


def test_ml_off_entry_does_not_load_models(monkeypatch):
    from models import backtest_tick
    from models.replay.benchmark import measure
    from strategy.signal import SignalEngine

    def forbidden(*args, **kwargs):
        pytest.fail("ML-OFF attempted to load a model")

    class ReachedReplay(Exception):
        pass

    def replay(bundle, params, *, signal_engine):
        assert signal_engine is None
        raise ReachedReplay

    monkeypatch.setattr(SignalEngine, "from_public_models", forbidden)
    monkeypatch.setattr(backtest_tick, "simulate_public_inputs", replay)
    with pytest.raises(ReachedReplay):
        measure(None, {"ml_enabled": False}, model=None)


def test_legacy_signal_cannot_admit_shared_feature_model(model_bundle):
    from strategy.signal import SignalEngine
    with pytest.raises(ValueError, match="explicit shared-feature"):
        SignalEngine(model_dir=model_bundle, symbol="BTCUSDC")


def test_websocket_subscription_and_dispatch_are_individual_only():
    from types import SimpleNamespace
    from unittest.mock import Mock
    from live.config import Config
    from live.ws_handler import WSHandler
    from test_live_feature_protocol import individual, trade
    cfg = Config()
    cfg.ml.feature_protocol = "execution_v1"
    engine = SimpleNamespace(signal=SimpleNamespace(on_trade=Mock(), on_agg_trade=Mock()),
                             inventory=SimpleNamespace(update_mark_price=Mock()))
    handler = WSHandler(engine, cfg)
    client = SimpleNamespace(subscribe=Mock(), agg_trade=Mock(), list_subscribe=Mock())
    handler._subscribe_market_streams("btcusdc", [], client=client)
    assert client.subscribe.call_args.kwargs["stream"] == "btcusdc@trade"
    client.agg_trade.assert_not_called()
    handler._record_binance_market_event = Mock()
    handler._on_market_message(None, individual(0))
    engine.signal.on_trade.assert_called_once()
    assert handler._exec_trade_count == 1
    handler._on_market_message(None, trade(0))
    engine.signal.on_agg_trade.assert_not_called()
    assert handler._exec_trade_count == 1
    old = Config()
    with pytest.raises(ValueError, match="restart-only"):
        handler.validate_config_reload(old, cfg)


def test_feature_gap_cancels_existing_quotes_and_blocks_direct_requote():
    from types import SimpleNamespace
    from unittest.mock import Mock
    from strategy.maker_engine import MakerEngine
    engine = SimpleNamespace(
        cfg=SimpleNamespace(ml=SimpleNamespace(feature_protocol="execution_v1")),
        signal=SimpleNamespace(is_warmed_up=False),
        orders=SimpleNamespace(has_active_orders=lambda: True),
        _cancel_all_orders=Mock(), _drop_replace_terminal_continuations=Mock(),
    )
    assert MakerEngine._enforce_stale_quote_stop(engine)
    engine._cancel_all_orders.assert_called_once()
    MakerEngine._requote(engine)
    assert engine._cancel_all_orders.call_count == 2
    engine._drop_replace_terminal_continuations.assert_called_once()


def test_live_shared_frame_prediction_warmup_gap_and_disconnect(model_bundle, monkeypatch):
    from strategy.live_public_signal import LivePublicSignalEngine
    from strategy.signal import SignalEngine
    from test_live_feature_protocol import individual, depth, NS
    now = [0]
    monkeypatch.setattr("strategy.live_public_signal.time.time_ns", lambda: now[0])
    engine = LivePublicSignalEngine(model_dir=model_bundle, symbol="BTCUSDC", ret_demean_halflife=0)
    assert not engine.is_warmed_up
    with pytest.raises(ValueError, match="aggTrade"):
        engine.on_agg_trade({})
    for i in range(60):
        now[0] = i*NS+103_000_000
        engine.on_trade(individual(i), receive_ts_ns=now[0]-1_000_000)
    now[0] = 60*NS+103_000_000
    event = {**depth(), "T": 60100, "E": 60101}
    engine.on_depth(event, receive_ts_ns=now[0]-1_000_000)
    assert engine.is_warmed_up
    live_prediction = engine.compute_signal()
    frame = engine._live_features.frame(now[0])
    offline = SignalEngine.from_public_models(model_bundle, ret_demean_halflife=0)
    expected = offline.consume_feature_frame(replace(frame, input_contract_id=INPUT), decision_ns=now[0])
    for name in REQUIRED_MODEL_HEADS:
        assert getattr(live_prediction, name) == getattr(expected, name)
    assert engine._close_history
    now[0] = 62*NS+103_000_000
    engine.on_trade(individual(62), receive_ts_ns=now[0]-1_000_000)
    assert not engine.is_warmed_up
    assert not engine._close_history
    engine.market_disconnected()
    assert not engine.is_warmed_up
    with pytest.raises(ValueError, match="disconnected"):
        engine.compute_signal()
