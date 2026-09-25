from dataclasses import replace
from decimal import Decimal as D

import pytest
import numpy as np

from data.contracts import TerminalAccount, label_before_boundary, two_day_slices
from data.observation import (
    DeliveryQueue, DepthPublisher, LatencyProfile, LiveAggregateAdapter, SharedFeatures, VisibleTradeWindows,
    historical_trade, live_aggregate,
)
from data.tardis_input import BookView, TradeExecution


def test_new_model_loader_and_inference_honor_mask_without_fitting(tmp_path, monkeypatch):
    import hashlib
    import json
    import lightgbm
    from data.observation import FeatureFrame, CONTRACT, FEATURE_CONTRACT
    from data.tardis_input import CONTRACT as INPUT
    from strategy.signal import SignalEngine, REQUIRED_MODEL_HEADS
    from strategy.model_contract import absolute_price_variance_unit_contract, ABSOLUTE_PRICE_VARIANCE_SEMANTICS
    seen = []

    class ControlledModel:
        def __init__(self, **kwargs):
            pass

        def feature_name(self):
            return ["mid"]

        def num_feature(self):
            return 1

        def predict(self, row):
            seen.append(row.copy())
            return [0.25]

    monkeypatch.setattr(lightgbm, "Booster", ControlledModel)
    meta = dict(input_contract_id=INPUT, observation_contract_id=CONTRACT,
                feature_contract_id=FEATURE_CONTRACT, symbol="BTCUSDC",
                label_contract_id="synthetic-not-trained", split_manifest_id="synthetic-not-research", heads={})
    selection = {"spec_sha256": "spec", "feature_manifest_sha256": "panel", "feature_dag_sha256": "features",
                 "source_manifest_sha256": "source", "train_source_identity_sha256": "inputs",
                 "fit_days": ["fit"], "selection_days": ["validation"], "refit_days": ["fit", "validation"],
                 "sample_weight_policy": {"half_life_days": 120}, "external_panel_read_during_fit": False}
    from research.families.f03_causal_13_head.public_input_panel import head_training_identity
    for head in REQUIRED_MODEL_HEADS:
        payload = ("synthetic:"+head).encode()
        (tmp_path/(head+".txt")).write_bytes(payload)
        meta["heads"][head] = dict(sha256=hashlib.sha256(payload).hexdigest(), feature_cols=["mid"], missing_policy="native_nan")
        head_meta = {k: meta[k] for k in ("input_contract_id", "observation_contract_id", "feature_contract_id",
                                        "label_contract_id", "split_manifest_id")}
        head_meta.update(name=head, feature_cols=["mid"], train_only_selection=selection)
        head_meta.update(volatility_unit_contract=absolute_price_variance_unit_contract("BTCUSDC"),
                         label_semantics=ABSOLUTE_PRICE_VARIANCE_SEMANTICS)
        encoded = json.dumps(head_meta).encode()
        (tmp_path/(head+"_meta.json")).write_bytes(encoded)
        meta["heads"][head]["metadata_sha256"] = hashlib.sha256(encoded).hexdigest()
        meta["training_identity"] = head_training_identity(head_meta, meta, head)
    (tmp_path/"public_input_model.json").write_text(json.dumps(meta))
    engine = SignalEngine.from_public_models(tmp_path, ret_demean_halflife=0)
    frame = FeatureFrame(INPUT, CONTRACT, FEATURE_CONTRACT, 1000, 999,
                         (("mid", D(101)),), (("execution", 1),), (("mid", False),))
    prediction = engine.compute_signal(feature_frame=frame, decision_ns=1000)
    assert len(seen) == 13 and all(np.isnan(row).all() for row in seen)
    assert prediction.touch_conditioned_price_change_fraction_10000ms == .25 and np.isnan(prediction.features).all()
    with pytest.raises(ValueError, match="future"):
        engine.compute_signal(feature_frame=frame, decision_ns=998)
    head = REQUIRED_MODEL_HEADS[0]
    meta_path = tmp_path/(head+"_meta.json")
    original = meta_path.read_bytes()
    altered = json.loads(original)
    altered["train_only_selection"]["sample_weight_policy"]["half_life_days"] = 60
    meta_path.write_text(json.dumps(altered))
    old_digest = meta["heads"][head]["metadata_sha256"]
    meta["heads"][head]["metadata_sha256"] = hashlib.sha256(meta_path.read_bytes()).hexdigest()
    (tmp_path/"public_input_model.json").write_text(json.dumps(meta))
    with pytest.raises(ValueError, match="mixed training identity"):
        SignalEngine.from_public_models(tmp_path)
    meta_path.write_bytes(original)
    meta["heads"][head]["metadata_sha256"] = old_digest
    (tmp_path/"public_input_model.json").write_text(json.dumps(meta))
    (tmp_path/(REQUIRED_MODEL_HEADS[0]+".txt")).write_text("changed")
    with pytest.raises(ValueError, match="identity mismatch"):
        SignalEngine.from_public_models(tmp_path)


def test_execution_features_mask_stale_and_missing_without_reference():
    from data.observation import ExecutionFeatures, Observation, FEATURE_CONTRACT, model_row
    from data.tardis_input import CONTRACT as INPUT
    book = BookView(1, ((D(100), D(2)),), ((D(102), D(3)),), 0, 0, "unknown", True)
    obs = Observation("a", "market", INPUT, "derived", 0, 0, 0, 0, book)
    engine = ExecutionFeatures(INPUT, market_id="market", max_book_age_ns=1_000_000_000)
    engine.advance(0, (obs,))
    frame = engine.frame(0)
    assert dict(frame.values)["mid"] == 101
    assert dict(frame.values)["depth_bid_20"] is None
    assert dict(frame.values)["volume_10s"] is None
    metadata = dict(input_contract_id=INPUT, observation_contract_id=frame.observation_contract_id,
                    feature_contract_id=FEATURE_CONTRACT, feature_cols=["mid"])
    assert model_row(frame, metadata, decision_ns=0) == [101]
    engine.advance(2_000_000_000)
    stale = engine.frame(2_000_000_000)
    assert dict(stale.values)["mid"] is None and dict(stale.values)["book_age_s"] == 2
    with pytest.raises(ValueError, match="missing"):
        model_row(stale, metadata, decision_ns=2_000_000_000)
    with pytest.raises(ValueError, match="mismatch"):
        model_row(frame, {**metadata, "feature_contract_id": "retired"}, decision_ns=0)


def test_source_consumer_stream_and_signal_share_future_boundary(tmp_path):
    from data.facts import materialize
    from data.runtime import ObservationProfile, PublicInputStream, derive_inputs, ConsumerBundle
    from data.tardis_input import CONTRACT as INPUT
    from strategy.signal import SignalEngine
    book = tmp_path / "book.csv"
    book.write_text("exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount\n"
        "binance-futures,BTCUSDC,1000000,8000000,true,bid,100,1\n"
        "binance-futures,BTCUSDC,1000000,8000000,true,ask,102,1\n"
        "binance-futures,BTCUSDC,2100000,9000000,false,bid,101,1\n")
    trade_file = tmp_path / "trade.csv"
    trade_file.write_text("exchange,symbol,timestamp,local_timestamp,id,side,price,amount\n"
        "binance-futures,BTCUSDC,1200000,8000000,1,buy,101,2\n")
    plan = {"source_profile": "tardis_only", "files": [
        {"path": str(book), "symbol": "BTCUSDC", "channel": "incremental_book_L2"},
        {"path": str(trade_file), "symbol": "BTCUSDC", "channel": "trades"}]}
    materialize(plan, tmp_path / "facts")
    profile = ObservationProfile("controlled", "source_timestamp_proxy", 200_000_000, 0,
                                 300_000_000, 1_000_000_000, trade_coverage="observed")
    kwargs = dict(profile=profile, start_ns=1_000_000_000, end_ns=4_000_000_000,
                  market_id="binance_futures:perpetual:BTCUSDC", input_contract_id=INPUT)
    ticks = list(PublicInputStream([tmp_path / "facts"], **kwargs))
    frames = [t.feature_frame for t in ticks if t.feature_frame is not None]
    assert dict(frames[0].values)["mid"] is None  # not delivered at publication
    assert dict(frames[1].values)["mid"] == 101
    assert frames[1].max_dependency_ready_ns <= frames[1].cutoff_ns
    assert [b.volume for t in ticks for b in t.bars][0] == 2
    signal = SignalEngine(enable_ml=False)
    prediction = signal.compute_signal(feature_frame=frames[1], decision_ns=frames[1].cutoff_ns)
    assert prediction.feature_dict["mid"] == 101 and prediction.ts == 2
    with pytest.raises(ValueError, match="future"):
        signal.compute_signal(feature_frame=frames[1], decision_ns=0)
    from dataclasses import asdict
    result = derive_inputs({"facts_root": str(tmp_path/"facts"), "observation_profile": asdict(profile),
        "start_ns": 1_000_000_000, "end_ns": 4_000_000_000, "market_id": kwargs["market_id"]}, tmp_path/"derived")
    assert result["stats"]["future_fill_violations"] == 0
    saved = list(ConsumerBundle(tmp_path/"derived").frames())
    assert dict(saved[1].values)["mid"] == dict(frames[1].values)["mid"]
    assert saved[1].source_ages_ns == frames[1].source_ages_ns
    from research.families.f03_causal_13_head.ml_model import load_public_feature_panel
    metadata = {"input_contract_id": INPUT, "observation_contract_id": saved[1].observation_contract_id,
                "feature_contract_id": saved[1].feature_contract_id,
                "feature_cols": ["mid", "native_packet_count_10s"], "missing_policy": "native_nan"}
    panel = load_public_feature_panel(tmp_path/"derived", model_metadata=metadata)
    assert panel.loc[2_000_000_000, "mid"] == prediction.feature_dict["mid"]
    assert panel["native_packet_count_10s"].isna().all()
    from models.backtest_tick import load_public_inputs
    from models.replay.public_input import public_predictions
    replay = load_public_inputs(tmp_path/"derived", tick_size=0.1)
    assert len(replay["trades"]) == 1 and replay["trades"].iloc[0]["quantity"] == 2
    assert replay["bbo"].ts_ms[0] == 1200  # delivered, not source/publication time
    assert replay["bbo"].observation_ts_us[0] == 1_000_000
    assert replay["bbo"].usable[-1] == False  # noqa: E712
    assert np.isnan(replay["l2"].bid_px[-1]).all()
    assert replay["bars"].index[0]+1000 == 2300  # actual Bar ready boundary
    assert np.isnan(replay["trades"].iloc[0]["normal_quantity"])
    tape = list(replay["exchange_book_event_tape"])
    assert len(tape) == 2 and tape[0].exchange_ts_ns == 1_000_000_000
    assert tape[0].first_update_id is None
    predictions = public_predictions(saved, signal, ["cv_ref_perp_available"])
    assert predictions[0].tolist() == [1000, 2000, 3000]
    assert np.isnan(predictions[6]).all()
    # Exercise the maintained executor on controlled synthetic input only.
    from models.backtest_tick import simulate_public_inputs
    params = dict(gamma=0.01, kappa=1.0, order_size=0.001, max_inventory=0.01,
        requote_interval=1.0, rq_min=1.0, rq_max=1.0, requote_clock="fixed",
        maker_fee=0.0, taker_fee=0.0, tick_size=0.1, lot_size=0.001,
        queue_base=0.0, queue_decay=0.0, maker_fill_prob=1.0, use_bar_pricing=True,
        replay_event_clock="merged", replay_clock_interval_ms=100,
        exchange_book_queue_mode="diagnostic", public_fill_volume_policy="all_public_volume_eligible",
        max_exec_book_age_s=1.0, collect_curves=False, position_timeout=0.0,
        markout_ema_span_fills=0, account_start_ns=1_200_000_000)
    result = simulate_public_inputs(tmp_path/"derived", params)
    assert result["public_input_contract"]["native_observation_parity"] == "not_proven"
    with pytest.raises(ValueError, match="eligibility"):
        simulate_public_inputs(tmp_path/"derived", {**params, "public_fill_volume_policy": None})
    strict = replace(profile, clock_policy="strict_exchange")
    with pytest.raises(ValueError, match="unverified"):
        list(PublicInputStream([tmp_path/"facts"], **{**kwargs, "profile": strict}))


def trade(ts=100, *, price="100", quantity="2", ordinal=0):
    return TradeExecution("market", "source", 1, ordinal, ts // 1000, "exchange_trade_T",
                          ts, 9999999999, ordinal, "buy", D(price), D(quantity))


def publish(queue, contribution, *, publish_ns=None, delay=None):
    queue.publish(event_id=contribution.event_id, market_id="market", input_contract_id="input",
                  origin="trade", source_asof_ns=contribution.exchange_ts_ns,
                  publish_ns=contribution.exchange_ts_ns if publish_ns is None else publish_ns,
                  payload=contribution, coverage="observed", market_delay_ns=delay)


def test_receive_reordering_and_processor_queue_no_future_visibility():
    queue = DeliveryQueue(LatencyProfile(10, 5, "synthetic"))
    publish(queue, historical_trade(trade(100)), delay=100)
    publish(queue, historical_trade(trade(101, ordinal=1)), delay=0)
    assert queue.advance(105) == ()
    assert queue.advance(106)[0].payload.source_ordinal == 1
    assert queue.advance(204) == ()
    assert queue.advance(205)[0].payload.source_ordinal == 0


def test_provider_clock_has_no_strategy_clock_authority():
    a = trade(100)
    b = replace(a, provider_receive_ts_us=1234567)
    assert historical_trade(a) == historical_trade(b)
    with pytest.raises(ValueError, match="evidence"):
        historical_trade(replace(a, exchange_ts_ns=None))


def test_publisher_carries_same_immutable_version_and_age():
    book = BookView(1, ((D(100), D(2)),), ((D(102), D(3)),), 1000, 1000, "unknown", True)
    queue = DeliveryQueue(LatencyProfile(0, 0, "synthetic"))
    publisher = DepthPublisher(period_ns=1_000_000)
    for ts in (1_000_000, 2_000_000):
        publisher.publish(queue, book, now_ns=ts, market_id="market", input_contract_id="input",
                          source_clock_evidence="exchange_event_E")
    observations = queue.advance(2_000_000)
    assert [x.source_asof_ns for x in observations] == [1_000_000, 1_000_000]
    assert [x.publish_ns - x.source_asof_ns for x in observations] == [0, 1_000_000]
    assert observations[0].payload.bbo == observations[1].payload.bbo
    with pytest.raises(ValueError, match="future"):
        DepthPublisher().publish(queue, replace(book, source_asof_us=999999999), now_ns=100_000_000,
                                 market_id="market", input_contract_id="input", source_clock_evidence="exchange_event_E")


def test_late_record_never_rewrites_closed_bar_and_empty_ohlc_null():
    queue = DeliveryQueue(LatencyProfile(0, 0, "synthetic"))
    publish(queue, historical_trade(trade(100)), delay=0)
    publish(queue, historical_trade(trade(200, ordinal=1)), delay=1000)
    windows = VisibleTradeWindows(start_ns=0, period_ns=1000, allowed_lateness_ns=100,
                                  market_id="market", coverage="observed")
    bars = windows.advance(1100, queue.advance(1100))
    assert bars[0].volume == D(2) and bars[0].turnover == D(200)
    assert bars[0].native_packet_count is None
    assert windows.advance(1200, queue.advance(1200)) == ()
    assert windows.late_count == 1 and bars[0].volume == D(2)
    empty = windows.advance(2100)[0]
    assert empty.volume == 0 and empty.close is None and empty.native_packet_count is None


def test_missing_volume_not_zero():
    window = VisibleTradeWindows(start_ns=0, period_ns=1000, allowed_lateness_ns=0, market_id="market")
    empty = window.advance(1000)[0]
    assert empty.volume is None and empty.individual_count is None and empty.close is None


def test_timer_service_not_backdated_and_same_deadline_is_included():
    queue = DeliveryQueue(LatencyProfile(900, 0, "synthetic"))
    publish(queue, historical_trade(trade(100)))
    windows = VisibleTradeWindows(start_ns=0, period_ns=1000, allowed_lateness_ns=0,
                                  market_id="market", coverage="observed")
    bar = windows.advance(1500, queue.advance(1500))[0]
    assert bar.ready_ns == 1500 and bar.volume == D(2)


def test_live_identity_duplicate_conflict_range_and_gap():
    adapter = LiveAggregateAdapter()
    fields = dict(exchange_ts_ns=100, price="100", quantity="1", side="buy")
    a = adapter.adapt(packet_id=1, first_id=10, last_id=18, **fields)
    assert a.individual_count == 9
    assert adapter.adapt(packet_id=1, first_id=10, last_id=18, **fields) is None
    with pytest.raises(ValueError, match="conflicting"):
        adapter.adapt(packet_id=1, first_id=10, last_id=19, **fields)
    with pytest.raises(ValueError, match="overlapping"):
        adapter.adapt(packet_id=2, first_id=18, last_id=20, **fields)
    b = adapter.adapt(packet_id=2, first_id=30, last_id=30, **fields)
    assert b.individual_count == 1 and adapter.id_gaps == 1


def test_live_packet_nine_children_visible_once_and_conservation():
    packet = live_aggregate(event_id="packet", exchange_ts_ns=100, price="100", quantity="9",
                            side="buy", first_id=20, last_id=28)
    assert packet.individual_count == 9 and packet.native_packet_count == 1
    queue = DeliveryQueue(LatencyProfile(20, 5, "synthetic"))
    publish(queue, packet)
    assert queue.advance(124) == ()
    received = queue.advance(125)
    assert len(received) == 1
    window = VisibleTradeWindows(start_ns=0, period_ns=1000, allowed_lateness_ns=0,
                                  market_id="market", coverage="observed")
    bar = window.advance(1000, received)[0]
    assert (bar.individual_count, bar.native_packet_count, bar.volume, bar.turnover) == (9, 1, D(9), D(900))


def test_reference_cannot_enter_before_ready_and_future_change_does_not_change_frame():
    book = BookView(1, ((D(100), D(2)),), ((D(102), D(3)),), 0, 0, "unknown", True)
    queue = DeliveryQueue(LatencyProfile(100, 5, "synthetic"))
    queue.publish(event_id="ref", market_id="reference", input_contract_id="input", origin="depth",
                  source_asof_ns=0, publish_ns=0, payload=book)
    features = SharedFeatures("input")
    features.advance(100, queue.advance(100))
    before = features.frame(100)
    assert before.values == ()
    features.advance(105, queue.advance(105))
    assert features.frame(105).values == (("reference:mid", D(101)),)
    assert before.values == ()
    with pytest.raises(ValueError):
        features.frame(100)


def test_407_day_slices_and_actual_label_end_boundary():
    slices = two_day_slices()
    assert len(slices) == 204
    assert sum(x["hours"] == 48 for x in slices) == 203
    assert slices[-1]["hours"] == 24 and slices[-1]["tail"]
    assert all(a["end_ns"] == b["start_ns"] for a, b in zip(slices, slices[1:], strict=False))
    assert label_before_boundary(decision_ns=1, actual_outcome_end_ns=[2, 9], boundary_ns=10)
    assert not label_before_boundary(decision_ns=1, actual_outcome_end_ns=[2, 10], boundary_ns=10)
    assert not label_before_boundary(decision_ns=1, actual_outcome_end_ns=[None], boundary_ns=10)


def test_terminal_inventory_loss_and_unknown_funding_not_erased():
    account = TerminalAccount(D(10), D(1), D(-20), D(2), None, D(100), 100, 0)
    assert account.totals()["pnl_before_funding"] == D(-12)
    assert account.totals()["all_in_net_pnl"] is None
    assert not account.totals()["economic_complete"]
    funded = replace(account, funding_cashflow=D(-3))
    assert funded.totals()["all_in_net_pnl"] == D(-15)
    assert not replace(funded, valuation_price=None).totals()["economic_complete"]


def test_bar_identity_blocks_old_or_other_market_features():
    window = VisibleTradeWindows(start_ns=0, period_ns=1000, allowed_lateness_ns=0,
                                 market_id="market", coverage="observed", input_contract_id="input")
    bar = window.advance(1000)[0]
    features = SharedFeatures("input")
    with pytest.raises(ValueError, match="contract mismatch"):
        features.advance(1000, closed_bars=[("market", replace(bar, input_contract_id="retired"))])
    with pytest.raises(ValueError, match="contract mismatch"):
        features.advance(1000, closed_bars=[("reference", bar)])
    features.advance(1000, closed_bars=[("market", bar)])
    assert features.frame(1000).values == (("market:volume", D(0)),)


def test_observation_invalid_clock_and_mixed_window_contract_rejected():
    queue = DeliveryQueue(LatencyProfile(0, 0, "synthetic"))
    publish(queue, historical_trade(trade(100)))
    observation = queue.advance(100)[0]
    with pytest.raises(ValueError, match="causal order"):
        replace(observation, ready_ns=99)
    window = VisibleTradeWindows(start_ns=0, period_ns=1000, allowed_lateness_ns=0,
                                 market_id="market", input_contract_id="current")
    with pytest.raises(ValueError, match="mixed input"):
        window.advance(100, [observation])
