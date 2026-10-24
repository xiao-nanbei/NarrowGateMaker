"""Controlled synthetic input; no purchased market excerpts or research results."""

from dataclasses import asdict

import numpy as np
import pandas as pd
import pytest

from data.feature_cursor import FeatureCursor
from data.facts import materialize
from data.runtime import ObservationProfile, derive_inputs

MARKET = "binance_futures:perpetual:BTCUSDC"
SECOND = 1_000_000_000


def test_registered_new_input_modules_import_without_launching_jobs():
    import importlib
    import json
    from pathlib import Path
    registry = json.loads((Path(__file__).resolve().parents[1] / "research/registry.json").read_text())
    migrated = {row["id"]: row["public_input_module"] for row in registry["families"]
                if "public_input_module" in row}
    assert set(migrated) == {"F01", "F04", "F05", "F06", "F07", "F08", "F09", "F10"}
    for module in migrated.values():
        assert importlib.import_module(module)


@pytest.fixture
def bundle(tmp_path):
    book, trade = tmp_path / "book.csv", tmp_path / "trade.csv"
    book.write_text(
        "exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount\n"
        "binance-futures,BTCUSDC,1000000,8000000,true,bid,100,1\n"
        "binance-futures,BTCUSDC,1000000,8000000,true,ask,102,1\n"
        "binance-futures,BTCUSDC,2100000,9000000,false,bid,101,1\n"
    )
    trade.write_text(
        "exchange,symbol,timestamp,local_timestamp,id,side,price,amount\n"
        "binance-futures,BTCUSDC,1200000,8000000,1,buy,101,2\n"
    )
    materialize({"source_profile": "tardis_only", "files": [
        {"path": str(book), "symbol": "BTCUSDC", "channel": "incremental_book_L2"},
        {"path": str(trade), "symbol": "BTCUSDC", "channel": "trades"},
    ]}, tmp_path / "facts")
    profile = ObservationProfile("controlled", "source_timestamp_proxy", 200_000_000, 0,
                                 300_000_000, SECOND, trade_coverage="observed")
    root = tmp_path / "consumer"
    derive_inputs({"facts_root": str(tmp_path / "facts"), "observation_profile": asdict(profile),
                   "start_ns": SECOND, "end_ns": 4 * SECOND, "market_id": MARKET}, root)
    return root


def binding(bundle):
    return {"input_manifest_id": FeatureCursor(bundle).input_manifest_id}


def test_modeled_delivery_preserves_older_source_age_without_relaxing_future_check():
    from models.tick_data_types import HistoricalBBOData, book_observation_times_us
    from dataclasses import replace

    book = HistoricalBBOData(np.array([2000, 2100]), *[np.ones(2) for _ in range(4)],
        observation_ts_us=np.array([1900000, 1800000]))
    with pytest.raises(ValueError, match="must not regress"):
        book_observation_times_us(book)
    np.testing.assert_array_equal(
        book_observation_times_us(book, allow_source_regression=True), [1900000, 1800000])
    with pytest.raises(ValueError, match="future"):
        book_observation_times_us(replace(book, observation_ts_us=np.array([2200000, 1800000])),
                                  allow_source_regression=True)


def test_depth_loader_bounded_expansion_preserves_batch_boundary(bundle):
    import hashlib
    import json
    import pyarrow as pa
    import pyarrow.parquet as pq
    from data.runtime import ConsumerBundle
    from models.replay.public_input import load_public_replay_inputs

    row = ConsumerBundle(bundle).table("depth").slice(0, 1)
    large = pa.concat_tables([row] * 8193)
    pq.write_table(large, bundle / "depth.parquet")
    path = bundle / "manifest.json"
    manifest = json.loads(path.read_text())
    manifest["files"]["depth"].update(rows=8193, sha256=hashlib.sha256(
        (bundle / "depth.parquet").read_bytes()).hexdigest())
    path.write_text(json.dumps(manifest))
    reader = ConsumerBundle(bundle)
    assert pa.Table.from_batches(list(reader.batches("depth"))).equals(reader.table("depth"))
    result = load_public_replay_inputs(bundle, tick_size=.1)
    l2 = result["l2"]
    # Both sides of the expansion boundary retain the same short book and
    # unknown deeper levels. No copying or zero-filling of missing prices.
    assert len(l2.ts_ms) == 8193
    np.testing.assert_equal(l2.bid_px[8191], l2.bid_px[8192])
    np.testing.assert_equal(l2.ask_px[8191], l2.ask_px[8192])


def action_contract(bundle, family="F06", action="default", spread_mult=1.):
    return dict(schema="research.action_policy.v1", policy_id="controlled-rule", family=family,
        **binding(bundle), feature_cols=["mid"], missing_policy="skip_decision", max_age_ns=SECOND,
        trace_limit=100, rules={side: [dict(feature="mid", op="ge", threshold=0.,
            action=action, spread_mult=spread_mult)] for side in ("BUY", "SELL")})


def test_action_policy_binding_causality_and_missing(bundle):
    from models.replay.public_strategy import PublicStrategy

    contract = action_contract(bundle, action="widen", spread_mult=2.)
    policy = PublicStrategy(bundle, contract)
    policy.start(binding(bundle)["input_manifest_id"])
    assert policy.decide(SECOND)["BUY"]["action"] == "default"
    assert policy.decide(2 * SECOND)["BUY"]["spread_mult"] == 2.
    assert policy.counts["missing_decisions"] == 1
    with pytest.raises(ValueError, match="regressed"):
        policy.decide(SECOND)
    with pytest.raises(ValueError, match="fresh"):
        policy.start(binding(bundle)["input_manifest_id"])
    contract["rules"]["BUY"][0]["spread_mult"] = 10.
    assert policy.decide(3 * SECOND)["BUY"]["spread_mult"] == 2.
    bad = action_contract(bundle)
    bad["missing_policy"] = "reject"
    strict = PublicStrategy(bundle, bad)
    strict.start(binding(bundle)["input_manifest_id"])
    with pytest.raises(ValueError, match="missing strategy"):
        strict.decide(SECOND)
    bad["input_manifest_id"] = "wrong"
    with pytest.raises(ValueError, match="manifest"):
        PublicStrategy(bundle, bad)
    with pytest.raises(ValueError, match="context"):
        policy.decide(10 * SECOND)


@pytest.mark.parametrize("family,action,mult", [
    ("F06", "cancel", 1.), ("F07", "widen", 2.),
    ("F06", "widen", .5), ("F07", "keep", 2.),
    ("F06", "widen", float("nan")),
])
def test_action_policy_rejects_incompatible_actions(bundle, family, action, mult):
    from models.replay.public_strategy import PublicStrategy

    with pytest.raises(ValueError):
        PublicStrategy(bundle, action_contract(bundle, family, action, mult))


def test_action_trace_is_bounded_and_not_fill_evidence(bundle):
    from models.replay.public_strategy import PublicStrategy

    contract = action_contract(bundle)
    contract["trace_limit"] = 0
    policy = PublicStrategy(bundle, contract)
    policy.start(binding(bundle)["input_manifest_id"])
    policy.decide(2 * SECOND)
    policy.resolved(2 * SECOND, "BUY", action="place", price=100., quantity=.001, route_due=True)
    report = policy.report()
    assert report["counts"]["trace_dropped"] == 1
    assert report["decisions"] == []
    assert report["receipt_semantics"] == "resolved_intent_not_ack_or_fill"


@pytest.mark.parametrize("enabled,force,has_order,expected", [
    (True, False, True, (True, False)), (True, True, True, (True, True)),
    (False, False, True, (False, True)), (True, False, False, (True, True)),
])
def test_keep_cannot_override_safety_or_create_order(enabled, force, has_order, expected):
    from models.replay.public_strategy import PublicStrategy

    assert PublicStrategy.continuation("keep", enabled=enabled, updated=True,
        has_order=has_order, force_update=force) == expected


def public_replay_params():
    return dict(eta_inventory=.01, a_spread=.01, risk_per_order=.01, inventory_reference_qty=1., execution_intensity_slope=1., risk_horizon_s=1., trade_intensity_acceleration_spread_mult=2., order_size=.001, max_inventory=.01,
        requote_interval=.2, rq_min=.2, rq_max=.2, requote_clock="fixed", maker_fee=0.,
        taker_fee=0., tick_size=.1, lot_size=.001, queue_base=0., queue_decay=0.,
        maker_fill_prob=1., use_bar_pricing=True, replay_event_clock="merged",
        replay_clock_interval_ms=100, exchange_book_queue_mode="diagnostic",
        public_fill_volume_policy="all_public_volume_eligible", max_exec_book_age_s=10.,
        collect_curves=False, position_timeout=0., markout_ema_span_fills=0,
        account_start_ns=1_200_000_000, trace_fills_max=1000, trace_quotes_max=1000,
        trace_decisions_max=1000, ml_enabled=False)


def test_parent_predictions_reused_without_mutable_signal_state(bundle, monkeypatch):
    from types import SimpleNamespace
    from models.backtest_tick import prepare_public_inputs, simulate_prepared_inputs, XMARKET_REPLAY_FEATURE_COLUMNS
    from models.replay.public_input import PreparedPublicPredictions

    class Signal:
        calls = 0

        def compute_feature_frames(self, frames):
            self.calls += 1
            return [SimpleNamespace(touch_conditioned_up_probability_10000ms=.6,
                absolute_price_variance_rate_10000ms=.01,
                touch_conditioned_price_change_fraction_10000ms=.001,
                touch_side_adverse_probability_bid_10000ms=.2,
                touch_side_adverse_probability_ask_10000ms=.3) for _ in frames]

    prepared = prepare_public_inputs(bundle, tick_size=.1)
    signal = Signal()
    predictions = PreparedPublicPredictions.create(prepared, signal, XMARKET_REPLAY_FEATURE_COLUMNS)
    assert signal.calls == 1
    with pytest.raises(ValueError):
        predictions.values[0].flags.writeable = True
    with pytest.raises(TypeError):
        predictions.values[-1]['new'] = np.zeros(1)
    mapped = predictions.for_inputs(prepared)
    mapped[-1].clear()
    assert predictions.values[-1]
    params = dict(public_replay_params(), ml_enabled=True)
    from models.replay.runtime_checkpoint_io import public_checkpoint_binding
    values = predictions.for_inputs(prepared)
    expected_binding = public_checkpoint_binding(prepared.manifest_id, params, values)
    assert public_checkpoint_binding(prepared.manifest_id, params, values,
        prediction_owner=predictions) == expected_binding
    with monkeypatch.context() as patch:
        def no_scan(*args, **kwargs):
            raise AssertionError('immutable predictions scanned twice')
        patch.setattr(np, 'ascontiguousarray', no_scan)
        assert public_checkpoint_binding(prepared.manifest_id, params, values,
            prediction_owner=predictions) == expected_binding
    changed = public_checkpoint_binding(prepared.manifest_id, {**params, 'asym_strength': .123},
        values, prediction_owner=predictions)
    assert changed != expected_binding
    assert len(predictions._bindings) == 2
    direct = simulate_prepared_inputs(prepared, params, signal_engine=Signal())
    for _ in range(2):
        cached = simulate_prepared_inputs(prepared, params, prepared_predictions=predictions)
        for key in ('fills_total', 'cash_before_terminal', 'final_inventory'):
            assert direct[key] == cached[key]
        pd.testing.assert_frame_equal(pd.DataFrame(direct['_quote_trace']),
                                      pd.DataFrame(cached['_quote_trace']), check_exact=True)
    assert signal.calls == 1
    with pytest.raises(ValueError, match='another input owner'):
        predictions.for_inputs(prepare_public_inputs(bundle, tick_size=.1))
    with pytest.raises(ValueError, match='either a signal engine'):
        simulate_prepared_inputs(prepared, params, signal_engine=signal,
                                 prepared_predictions=predictions)
    checkpoint = simulate_prepared_inputs(prepared, params, prepared_predictions=predictions,
                                          checkpoint_at_ts_ms=2100)['_replay_checkpoint']
    restored = simulate_prepared_inputs(prepared, params, prepared_predictions=predictions,
                                        resume_checkpoint=checkpoint)
    for key in ('fills_total', 'cash_before_terminal', 'final_inventory'):
        assert restored[key] == direct[key]


def test_configured_actions_reach_real_executor(bundle):
    from models.backtest_tick import simulate_public_inputs
    from research.families.f06_placement_fill_cif.public_input import replay_placement_strategy
    from research.families.f07_active_order_continuation.public_input import replay_continuation_strategy

    params = public_replay_params()
    kwargs = dict(params=params, initial_capital=100., max_mark_age_ns=10 * SECOND)
    control = replay_placement_strategy(bundle, contract=action_contract(bundle), **kwargs)
    plain = simulate_public_inputs(bundle, params)
    for key in ("fills_total", "cash_before_terminal", "final_inventory"):
        assert control[key] == plain[key]
    pd.testing.assert_frame_equal(pd.DataFrame(control["_quote_trace"]),
                                  pd.DataFrame(plain["_quote_trace"]), check_exact=True)
    wider = replay_placement_strategy(bundle,
        contract=action_contract(bundle, action="widen", spread_mult=2.), **kwargs)
    assert wider["public_strategy"]["counts"]["requested_sides"] > 0
    assert [row["price"] for row in wider["_quote_trace"]] != [row["price"] for row in control["_quote_trace"]]
    cancel = replay_continuation_strategy(bundle,
        contract=action_contract(bundle, "F07", "cancel"), **kwargs)
    intents = cancel["public_strategy"]["decisions"]
    assert any(row["resolved_intent"] == "cancel" for row in intents)
    assert 0 < len(cancel["_quote_trace"]) < len(control["_quote_trace"])
    kept = replay_continuation_strategy(bundle,
        contract=action_contract(bundle, "F07", "keep"), **kwargs)
    assert any(row["resolved_intent"] == "keep" for row in kept["public_strategy"]["decisions"])
    assert 0 < len(kept["_quote_trace"]) < len(control["_quote_trace"])
    assert all(row["feature_cutoff_ns"] <= row["decision_ns"] for row in intents)
    assert cancel["all_in_net_pnl"] is None
    assert cancel["accounting"]["terminal_inventory"] == cancel["final_inventory"]


@pytest.mark.parametrize('family,action,mult', [('F06', 'widen', 2.), ('F07', 'keep', 1.), ('F07', 'cancel', 1.)])
@pytest.mark.parametrize('cut', [1200, 1600, 2100, 3000])
def test_public_strategy_checkpoint_forks_preserve_actions_l2_and_account(bundle, tmp_path, family, action, mult, cut):
    from models.backtest_tick import prepare_public_inputs, simulate_prepared_inputs
    from models.replay.public_strategy import PublicStrategy
    from models.replay.l2_journal import ReplayL2Journal
    from models.replay.runtime_checkpoint_io import save_runtime_checkpoint, load_trusted_runtime_checkpoint
    from execution.chunked_parquet_journal import iter_chunked_parquet_journal
    from tests.test_tick_runtime_checkpoint import assert_same

    params = public_replay_params()
    prepared = prepare_public_inputs(bundle, tick_size=params['tick_size'])
    contract = action_contract(bundle, family, action, spread_mult=mult)
    def run(name, **options):
        journal = ReplayL2Journal(tmp_path / name, identity={'contract': contract}, chunk_rows=2)
        return simulate_prepared_inputs(prepared, {**params, '_l2_journal': journal},
            public_strategy=PublicStrategy(bundle, contract), **options)
    expected = run('whole')
    receipt = expected.pop('_l2_journal')
    rows = list(iter_chunked_parquet_journal(receipt['manifest']))
    checkpoint = run('prefix', checkpoint_at_ts_ms=cut)['_replay_checkpoint']
    path = tmp_path / 'state.pickle'
    save_runtime_checkpoint(path, checkpoint)
    for name in ('left', 'right'):
        actual = run(name, resume_checkpoint=load_trusted_runtime_checkpoint(path))
        branch = actual.pop('_l2_journal')
        assert_same(actual, expected)
        assert list(iter_chunked_parquet_journal(branch['manifest'])) == rows
        assert branch['production'] == receipt['production']
    params['maker_fee'] = .01
    with pytest.raises(ValueError, match='input, parameters, predictions or implementation changed'):
        run('bad-config', resume_checkpoint=load_trusted_runtime_checkpoint(path))


def test_reference_uses_ready_frame_and_preserves_missing(bundle):
    from research.families.f04_external_market_alpha.public_input import build_reference_panel

    panel = build_reference_panel({MARKET: bundle}, [SECOND, 2 * SECOND],
        columns={MARKET: ["mid", "native_packet_count_10s"]},
        max_age_ns=SECOND, missing_policy="native_nan")
    assert np.isnan(panel.iloc[0][f"{MARKET}/mid"])
    assert panel.iloc[1][f"{MARKET}/mid"] == 101
    assert panel[f"{MARKET}/native_packet_count_10s"].isna().all()
    with pytest.raises(ValueError, match="identity"):
        build_reference_panel({"wrong": bundle}, [SECOND], columns={"wrong": ["mid"]},
                              max_age_ns=SECOND, missing_policy="native_nan")
    cursor = FeatureCursor(bundle)
    with pytest.raises(ValueError, match="context"):
        cursor.at(SECOND - 1, max_age_ns=SECOND)
    with pytest.raises(ValueError, match="context"):
        cursor.at(10 * SECOND, max_age_ns=SECOND)


def test_common_stream_reiterable_and_side_flow_conservation(bundle):
    from data.runtime import ConsumerBundle
    from research.families.f08_side_taker_lifecycle.public_input import load_visible_side_flow

    consumer = ConsumerBundle(bundle)
    first = [(t.now_ns, t.observations) for t in consumer.stream()]
    assert first == [(t.now_ns, t.observations) for t in consumer.stream()]
    panel = load_visible_side_flow(bundle)
    assert panel.volume.sum() == 2
    assert panel.turnover.sum() == 202
    assert panel.buy_count.sum() == 1
    assert panel.native_packet_count.isna().all()
    assert (panel.ready_ns >= panel.end_ns).all()


def test_opportunity_labels_purge_actual_end_without_zero_imputation(bundle):
    from research.families.f05_fill_quality_quote_ev.public_input import build_opportunity_panel

    opportunities = pd.DataFrame([
        dict(opportunity_id=k, decision_ns=2 * SECOND, side="bid", **binding(bundle))
        for k in ("filled", "no_fill", "late", "censored")])
    outcomes = pd.DataFrame([
        dict(opportunity_id=k, horizon_ns=SECOND, actual_outcome_end_ns=end,
             right_censored=censor, filled_quantity=q, markout_bps=mark, **binding(bundle))
        for k, end, censor, q, mark in [
            ("filled", 3 * SECOND - 1, False, .1, -2),
            ("no_fill", 3 * SECOND - 1, False, 0, None),
            ("late", 3 * SECOND, False, .1, 2),
            ("censored", 3 * SECOND - 1, True, 0, None)]])
    kwargs = dict(feature_columns=["mid"], missing_policy="reject", max_age_ns=SECOND,
                  training_boundary_ns=3 * SECOND)
    panel = build_opportunity_panel(bundle, opportunities, outcomes, **kwargs).set_index("opportunity_id")
    assert set(panel.index) == {"filled", "no_fill"}
    assert panel.loc["filled", "opportunity_markout_bps"] == -2
    assert panel.loc["no_fill", "opportunity_markout_bps"] == 0
    assert np.isnan(panel.loc["no_fill", "conditional_markout_bps"])
    assert (panel.mid == 101).all()
    with pytest.raises(ValueError, match="missing its outcome"):
        build_opportunity_panel(bundle, opportunities, outcomes.iloc[1:], **kwargs)
    outcomes.loc[0, "markout_bps"] = None
    unknown = build_opportunity_panel(bundle, opportunities, outcomes, **kwargs).set_index("opportunity_id")
    assert unknown.loc["filled", "fill_label"] == 1
    assert np.isnan(unknown.loc["filled", "conditional_markout_bps"])
    assert np.isnan(unknown.loc["filled", "opportunity_markout_bps"])


def order_events(bundle):
    return [dict(order_id="o", effective_ns=clock, kind=kind, quantity=.2, **binding(bundle))
            for clock, kind in [(2_100_000_000, "active"), (2_200_000_000, "cancel_requested"),
                                (2_300_000_000, "snapshot_rebase"), (2_400_000_000, "fill"),
                                (2_500_000_000, "cancel_active")]]


def test_placement_and_continuation_keep_cancel_inflight_and_unknown_queue(bundle):
    from research.families.f06_placement_fill_cif.public_input import build_placement_panel, placement_risk_targets
    from research.families.f07_active_order_continuation.public_input import continuation_report

    events = order_events(bundle)
    placements = pd.DataFrame([dict(order_id="o", decision_ns=2 * SECOND, quantity=1., **binding(bundle))])
    panel = build_placement_panel(bundle, placements, events, feature_columns=["mid"],
                                  missing_policy="reject", max_age_ns=SECOND, observation_end_ns=3 * SECOND)
    assert panel.iloc[0].first_fill_ns == 2_400_000_000
    assert panel.iloc[0].terminal_reason == "cancel"
    assert not panel.iloc[0].queue_position_known
    risk = placement_risk_targets(panel, horizon_ns=SECOND)
    assert risk.iloc[0].event_kind == "fill"
    assert risk.iloc[0].exposure_ns == 300_000_000
    report = continuation_report(bundle, events, order_id="o", initial_quantity=1,
                                  observation_end_ns=2_500_000_000)
    assert report["right_censored"] and report["pending_cancel"]
    assert report["filled_quantity"] == .2
    assert report["native_queue_closed"] is False
    events[0]["input_manifest_id"] = "wrong"
    with pytest.raises(ValueError, match="another input"):
        continuation_report(bundle, events, order_id="o", initial_quantity=1, observation_end_ns=3 * SECOND)


@pytest.mark.parametrize("events", [
    [(1, "fill", .1)], [(2, "active", 0), (1, "fill", .1)],
    [(1, "active", 0), (2, "cancel_active", 0)],
    [(1, "active", 0), (2, "fill", 2)],
    [(1, "active", 0), (2, "fill", 1), (3, "fill", .1)],
])
def test_invalid_order_paths_rejected(events):
    from models.replay.order_exposure import order_exposure
    with pytest.raises(ValueError):
        order_exposure([dict(order_id="o", effective_ns=t, kind=k, quantity=q) for t, k, q in events],
                        order_id="o", observation_end_ns=10, initial_quantity=1.)


def account(bundle, time, cash):
    return dict(boundary_ts_ms=time, cash_usdc=cash, inventory_btc=0, **binding(bundle))


def test_action_reward_excludes_preassignment_profit_and_checks_end(bundle):
    from research.families.f09_inventory_lifecycle_action_uplift.public_input import build_action_panel
    from research.families.f09_inventory_lifecycle_action_uplift.audit.offline_policy_evaluation import (
        OPEConfig, _prepare_panel,
    )
    assignment = dict(decision_id="a", decision_ts_ns=2 * SECOND, actual_outcome_end_ns=3 * SECOND,
                      action="hold", behavior_propensity=.5, start_state=account(bundle, 2000, 150),
                      end_state=account(bundle, 3000, 147), fees_usdc=1., funding_cashflow_usdc=0.,
                      **binding(bundle))
    kwargs = dict(feature_columns=["mid"], missing_policy="reject", max_age_ns=SECOND,
                  max_mark_age_ms=1000, training_boundary_ns=4 * SECOND)
    panel = build_action_panel(bundle, [assignment], **kwargs)
    assert panel.reward.tolist() == [-3]  # not inventory_lifecycle profit 47; fee already in cash
    assert _prepare_panel(panel, OPEConfig())._ope_reward.tolist() == [-3]
    assert build_action_panel(bundle, [assignment], **{**kwargs, "training_boundary_ns": 3 * SECOND}).empty
    with pytest.raises(ValueError, match="incomplete"):
        build_action_panel(bundle, [{**assignment, "funding_cashflow_usdc": None}], **kwargs)


def test_account_attribution_keeps_unknown_funding(bundle):
    from research.families.f10_live_replay_attribution.public_input import attribute_interval
    identity = dict(**binding(bundle), observation_contract_id="data.observation.v1",
                    execution_contract_id="synthetic", epoch_id="synthetic")
    report = attribute_interval(bundle, account(bundle, 2000, 100), account(bundle, 3000, 98),
                                fees_usdc=1, funding_cashflow_usdc=None,
                                max_mark_age_ms=1000, run_identity=identity)
    assert report["observed_equity_change_usdc"] == -2
    assert report["net_equity_change_usdc"] is None and not report["economic_complete"]


def test_fixed_parameter_runner_rejects_different_execution_assumptions(bundle):
    from research.families.f01_fixed_parameter_racing.public_input import replay_parameter_candidates
    with pytest.raises(ValueError, match="quote parameters"):
        replay_parameter_candidates(bundle, {"bad": {"maker_fee": -1}}, common_params={})


@pytest.mark.parametrize("economic", [False, True])
@pytest.mark.parametrize("with_funding", [False, True])
def test_fixed_parameter_runner_repeats_independent_synthetic_accounts(bundle, economic, with_funding):
    from research.families.f01_fixed_parameter_racing.public_input import replay_parameter_candidates
    params = dict(eta_inventory=.01, a_spread=.01, risk_per_order=.01, inventory_reference_qty=1., execution_intensity_slope=1., risk_horizon_s=1., trade_intensity_acceleration_spread_mult=2., order_size=.001, max_inventory=.01,
        requote_interval=1., rq_min=1., rq_max=1., requote_clock="fixed", maker_fee=0.,
        taker_fee=0., tick_size=.1, lot_size=.001, queue_base=0., queue_decay=0.,
        maker_fill_prob=1., use_bar_pricing=True, replay_event_clock="merged",
        replay_clock_interval_ms=100, exchange_book_queue_mode="diagnostic",
        public_fill_volume_policy="all_public_volume_eligible", max_exec_book_age_s=1.,
        collect_curves=False, position_timeout=0., markout_ema_span_fills=0,
        account_start_ns=1_200_000_000)
    candidates = {"first": {"eta_inventory": .01, "risk_per_order": .01},
                  "repeat": {"eta_inventory": .01, "risk_per_order": .01}}
    if economic:
        from research.families.f01_fixed_parameter_racing.public_input import replay_economic_candidates
        settled = replay_economic_candidates(bundle, candidates, common_params=params,
            initial_capital=10000., max_mark_age_ns=1_000_000_000, trace_limit=10000,
            funding=(dict(market_id=MARKET, source_identity="controlled-empty-settlement-window",
                coverage_start_ns=SECOND, coverage_end_ns=4 * SECOND,
                expected_settlements_ns=[], events=[]) if with_funding else None))
        assert settled['first']['accounting'] == settled['repeat']['accounting']
        assert (settled['first']['accounting']['all_in_net_pnl'] is None) == (not with_funding)
        results = {name: value['replay'] for name, value in settled.items()}
        assert 'trace_fills_max' not in params
    else:
        results = replay_parameter_candidates(bundle, candidates, common_params=params)
    for result in results.values():
        assert result["economic_complete"] is False
        assert result["all_in_net_pnl"] is None
        assert result["public_input_contract"]["account_start_ns"] == 1_200_000_000
    for key in ("total_pnl", "fills", "trades"):
        if key in results["first"] and np.isscalar(results["first"][key]):
            assert results["first"][key] == results["repeat"][key]


def test_economic_parameter_runner_rejects_account_restore_before_replay(bundle):
    from research.families.f01_fixed_parameter_racing.public_input import replay_economic_candidates
    with pytest.raises(ValueError, match="fresh independent"):
        replay_economic_candidates(bundle, {"candidate": {"eta_inventory": .01, "risk_per_order": .01}},
            common_params={"initial_live_state": {"inventory": 1}},
            initial_capital=10000., max_mark_age_ns=1000, trace_limit=10)


def test_ope_fold_purges_actual_outcome_end_before_any_fit():
    from research.families.f09_inventory_lifecycle_action_uplift.audit.offline_policy_evaluation import (
        DayFold, OPEConfig, _fold_predictions,
    )
    boundary = pd.Timestamp("2025-08-02", tz="UTC").value
    frame = pd.DataFrame({"day": ["2025-08-01", "2025-08-01", "2025-08-02"],
                          "decision_ts_ns": [boundary - 10, boundary - 9, boundary + 10],
                          "actual_outcome_end_ns": [boundary - 1, boundary, boundary + 20]})
    cfg = OPEConfig(actual_outcome_end_col="actual_outcome_end_ns", min_train_rows=2)
    output, summary = _fold_predictions(frame, DayFold(0, ("2025-08-01",), ("2025-08-02",)), [], [], cfg)
    assert output.empty
    assert summary["train_rows"] == 1  # equality at boundary is excluded


def test_sequence_recomputes_continuous_windows_instead_of_concatenating_cold_frames(bundle, tmp_path):
    from data.consumer_sequence import derive_consumer_sequence
    from data.runtime import ConsumerBundle

    original = ConsumerBundle(bundle)
    plan = original.manifest["plan"]
    left, right, joined = (tmp_path / name for name in ("left", "right", "joined"))
    derive_inputs({**plan, "end_ns": 2 * SECOND}, left)
    derive_inputs({**plan, "start_ns": 2 * SECOND}, right)
    derive_consumer_sequence([left, right], joined)
    merged = ConsumerBundle(joined)
    for name in ("features", "bars", "depth"):
        assert merged.table(name).equals(original.table(name))
    assert len(merged.manifest["source_bundles"]) == 1
    assert len(merged.manifest["plan"]["consumer_parent_ids"]) == 2
    with pytest.raises(ValueError, match="adjacent"):
        derive_consumer_sequence([right, left], tmp_path / "bad-order")
    with pytest.raises(ValueError, match="adjacent"):
        derive_consumer_sequence([left, left], tmp_path / "overlap")
    with pytest.raises(FileExistsError):
        derive_consumer_sequence([left, right], joined)


def test_reference_prediction_adapter_does_not_read_future_reference(bundle, tmp_path):
    import hashlib
    import json
    from types import SimpleNamespace
    from data.runtime import ConsumerBundle
    from research.families.f04_external_market_alpha.public_input import (
        ReferenceSignalAdapter, build_reference_panel,
    )

    reference_market = MARKET.replace("BTCUSDC", "BTCUSDT")
    files = []
    for name, channel in (("book", "incremental_book_L2"), ("trade", "trades")):
        source = tmp_path / (name + ".csv")
        target = tmp_path / (name + "-ref.csv")
        target.write_text(source.read_text().replace("BTCUSDC", "BTCUSDT"))
        files.append({"path": str(target), "symbol": "BTCUSDT", "channel": channel})
    facts, reference = tmp_path / "reference-facts", tmp_path / "reference"
    materialize({"source_profile": "tardis_only", "files": files}, facts)
    plan = ConsumerBundle(bundle).manifest["plan"]
    derive_inputs({**plan, "market_id": reference_market, "facts_root": str(facts)}, reference)
    contract = dict(schema="research.reference_signal.v1", model_id="synthetic-model",
        output_contract_id="quote_prediction.five_head.v1", execution_market=MARKET,
        execution_columns=["mid"], reference_columns={reference_market: ["mid"]},
        missing_policy="native_nan", max_age_ns=SECOND,
        input_manifest_ids={"execution": FeatureCursor(bundle).input_manifest_id,
                            reference_market: FeatureCursor(reference).input_manifest_id})

    class Predictor:
        input_contract = contract

        def predict(self, *, execution, references, decision_ns):
            value = references[reference_market]["mid"]
            return SimpleNamespace(touch_conditioned_up_probability_10000ms=.5, absolute_price_variance_rate_10000ms=1., touch_conditioned_price_change_fraction_10000ms=value,
                                   touch_side_adverse_probability_bid_10000ms=0., touch_side_adverse_probability_ask_10000ms=0.)

    adapter = ReferenceSignalAdapter(bundle, {reference_market: reference}, Predictor(), contract=contract)
    cursor = FeatureCursor(bundle)
    with pytest.raises(ValueError, match="nonfinite"):
        adapter.compute_signal(feature_frame=cursor.at(SECOND, max_age_ns=SECOND), decision_ns=SECOND)
    later = adapter.compute_signal(feature_frame=cursor.at(2 * SECOND, max_age_ns=SECOND), decision_ns=2 * SECOND)
    assert later.touch_conditioned_price_change_fraction_10000ms == 101
    assert adapter.observations[-1]["reference_cutoffs"][reference_market] <= 2 * SECOND
    with pytest.raises(ValueError, match="binding"):
        bad = {**contract, "input_manifest_ids": {}}
        predictor = Predictor()
        predictor.input_contract = bad
        ReferenceSignalAdapter(bundle, {reference_market: reference}, predictor, contract=bad)

    # Explicit synthetic predictor policy for incomplete warmup, not a loader
    # that silently fills missing production features with zero.
    class WarmupPredictor(Predictor):
        def predict(self, **kwargs):
            return SimpleNamespace(touch_conditioned_up_probability_10000ms=.5, absolute_price_variance_rate_10000ms=.001, touch_conditioned_price_change_fraction_10000ms=.0001,
                                   touch_side_adverse_probability_bid_10000ms=.1, touch_side_adverse_probability_ask_10000ms=.1)

    from research.families.f04_external_market_alpha.public_input import replay_reference_strategy
    params = dict(eta_inventory=.01, a_spread=.01, risk_per_order=.01, inventory_reference_qty=1., execution_intensity_slope=1., risk_horizon_s=1., trade_intensity_acceleration_spread_mult=2., order_size=.001, max_inventory=.01,
        requote_interval=1., rq_min=1., rq_max=1., requote_clock="fixed", maker_fee=0.,
        taker_fee=0., tick_size=.1, lot_size=.001, queue_base=0., queue_decay=0.,
        maker_fill_prob=1., use_bar_pricing=True, replay_event_clock="merged",
        replay_clock_interval_ms=100, exchange_book_queue_mode="diagnostic",
        public_fill_volume_policy="all_public_volume_eligible", max_exec_book_age_s=1.,
        collect_curves=False, position_timeout=0., markout_ema_span_fills=0,
        account_start_ns=1_200_000_000, trace_fills_max=1000, ml_enabled=True)
    result = replay_reference_strategy(bundle, {reference_market: reference}, WarmupPredictor(),
        contract=contract, params=params, initial_capital=100., max_mark_age_ns=SECOND,
        funding=dict(market_id=MARKET, source_identity="controlled-empty-settlement-window",
            coverage_start_ns=SECOND, coverage_end_ns=4 * SECOND,
            expected_settlements_ns=[], events=[]))
    assert result["reference_observations"]
    assert result["accounting"]["terminal_inventory"] == result["final_inventory"]
    assert result["accounting"]["terminal_liquidation_applied"] is False

    # A real reference bundle must not borrow execution-market delay samples.
    manifest_path = reference / "manifest.json"
    reference_manifest = json.loads(manifest_path.read_text())
    reference_manifest["plan"]["observation_profile"].update(
        measured_latency_market_id="binance:perp:BTCUSDC",
        measured_latency_path=str(tmp_path / "unread-measured-profile.json"),
        measured_latency_sha256="a" * 64, market_delay_ns=0, processing_ns=0)
    manifest_path.write_text(json.dumps(reference_manifest))
    with pytest.raises(ValueError, match="latency profile market identity"):
        build_reference_panel({reference_market: reference}, [2 * SECOND],
            columns={reference_market: ["mid"]}, max_age_ns=SECOND,
            missing_policy="native_nan")
    misbound = {**contract, "input_manifest_ids": {
        "execution": FeatureCursor(bundle).input_manifest_id,
        reference_market: hashlib.sha256(manifest_path.read_bytes()).hexdigest()}}
    predictor = Predictor()
    predictor.input_contract = misbound
    with pytest.raises(ValueError, match="latency profile market identity"):
        ReferenceSignalAdapter(bundle, {reference_market: reference}, predictor,
            contract=misbound)


def test_wrong_market_facts_cannot_publish_reference_bundle(bundle, tmp_path):
    from data.runtime import ConsumerBundle

    execution = ConsumerBundle(bundle)
    plan = execution.manifest["plan"]
    with pytest.raises(ValueError, match="market/channel identity"):
        derive_inputs({**plan,
                       "facts_root": str(execution.source_paths()[0]),
                       "market_id": MARKET.replace("BTCUSDC", "BTCUSDT")},
                      tmp_path / "invalid-reference")
    assert not (tmp_path / "invalid-reference").exists()


def test_new_quote_training_publishes_and_loads_without_legacy_cleaner(bundle, tmp_path):
    from research.families.f05_fill_quality_quote_ev.public_input import (
        build_opportunity_panel, train_opportunity_models,
    )
    from research.families.f05_fill_quality_quote_ev.quote_ev import QuoteEVModel

    opportunities = pd.DataFrame([
        dict(opportunity_id=str(i), decision_ns=2 * SECOND, side="bid", **binding(bundle))
        for i in range(4)])
    outcomes = pd.DataFrame([
        dict(opportunity_id=str(i), horizon_ns=h * SECOND, actual_outcome_end_ns=33 * SECOND,
             right_censored=False, filled_quantity=0. if i < 2 else .1,
             markout_bps=None if i < 2 else -2. if i == 2 else 2., **binding(bundle))
        for i in range(4) for h in (1, 5, 30)])
    panel = build_opportunity_panel(bundle, opportunities, outcomes, feature_columns=["mid"],
        missing_policy="reject", max_age_ns=SECOND, training_boundary_ns=40 * SECOND)
    identity = {key: panel.attrs[key] for key in (
        "input_contract_id", "observation_contract_id", "feature_contract_id")}
    identity.update(source_manifest_sha256=panel.attrs["input_manifest_id"],
                    training_contract_id="synthetic-training", label_contract_id="synthetic-outcomes")
    output = tmp_path / "synthetic-models"
    kwargs = dict(input_identity=identity, feature_columns=["mid"], side="bid", missing_policy="reject",
        training_boundary_ns=40 * SECOND, bucket_edges=[0.], bucket_values=[-2., 2.],
        adverse_threshold_bps=0., parameters={"num_threads": 1, "verbosity": -1,
            "min_data_in_leaf": 1, "min_data_in_bin": 1, "seed": 1}, num_boost_round=1)
    report = train_opportunity_models(panel, output, **kwargs)
    assert len(report["heads"]) == 5
    model = QuoteEVModel.load(output, input_identity=identity)
    prediction = model.predict_frame(FeatureCursor(bundle).at(2 * SECOND, max_age_ns=SECOND),
                                     decision_ns=2 * SECOND)
    assert 0 <= prediction.lifecycle_fill_probability <= 1
    relocated_identity = {k: v for k, v in identity.items() if k != "source_manifest_sha256"}
    equivalent = QuoteEVModel.load(output, input_identity=relocated_identity)
    assert equivalent.fill_prob_features == model.fill_prob_features
    with pytest.raises(ValueError, match="identity mismatch"):
        QuoteEVModel.load(output, input_identity={**relocated_identity,
                                                "feature_contract_id": "incompatible"})
    with pytest.raises(FileExistsError):
        train_opportunity_models(panel, output, **kwargs)
    with pytest.raises(ValueError, match="two classes"):
        train_opportunity_models(panel[panel.fill_label == 0], tmp_path / "bad-models", **kwargs)
    assert not (tmp_path / "bad-models").exists()


def test_public_accounting_reconciles_fees_funding_and_rejects_truncated_trace(bundle):
    from models.replay.public_accounting import settle_public_replay

    result = dict(public_input_contract={"account_start_ns": SECOND, **binding(bundle)}, fills_total=2,
        final_inventory=0., cash_before_terminal=-3., _fill_trace=[
            dict(fill_sequence=i, fill_ts=t, side=side, fill_qty=1., quote_px=px, fill_fee_usdc=1.)
            for i, t, side, px in [(0, 2000, "BUY", 101.), (1, 3000, "SELL", 100.)]])
    funding = dict(market_id=MARKET, source_identity="controlled-accounting-fixture",
        coverage_start_ns=SECOND, coverage_end_ns=4 * SECOND,
        expected_settlements_ns=[2_500_000_000],
        events=[dict(settlement_ns=2_500_000_000, mark_price=100., rate=.01)])
    report = settle_public_replay(bundle, result, initial_capital=100., max_mark_age_ns=SECOND,
                                  funding=funding)
    assert report["realized_trading_pnl"] == -1
    assert report["fees"] == 2
    assert report["funding_cashflow"] == -1
    assert report["pnl_before_funding"] == -3
    assert report["all_in_net_pnl"] == -4
    assert report["terminal_equity"] == 96
    assert report["economic_complete"] and not report["terminal_liquidation_applied"]
    unknown = settle_public_replay(bundle, result, initial_capital=100., max_mark_age_ns=SECOND)
    assert unknown["all_in_net_pnl"] is None
    with pytest.raises(ValueError, match="incomplete"):
        settle_public_replay(bundle, {**result, "_fill_trace": result["_fill_trace"][:1]},
                             initial_capital=100., max_mark_age_ns=SECOND)
    with pytest.raises(ValueError, match="schedule"):
        settle_public_replay(bundle, result, initial_capital=100., max_mark_age_ns=SECOND,
                             funding={**funding, "events": []})
    with pytest.raises(ValueError, match="cash ledger"):
        settle_public_replay(bundle, {**result, "cash_before_terminal": 0.},
                             initial_capital=100., max_mark_age_ns=SECOND)


def test_public_accounting_keeps_unmarked_inventory_unknown(bundle):
    from models.replay.public_accounting import settle_public_replay

    result = dict(public_input_contract={"account_start_ns": SECOND, **binding(bundle)}, fills_total=1,
        final_inventory=1., cash_before_terminal=-102., _fill_trace=[
            dict(fill_sequence=0, fill_ts=2000, side="BUY", fill_qty=1.,
                 quote_px=101., fill_fee_usdc=1.)])
    report = settle_public_replay(bundle, result, initial_capital=100., max_mark_age_ns=0)
    assert report["terminal_inventory"] == 1
    assert report["pnl_before_funding"] is None
    assert report["terminal_unrealized_pnl"] is None


def test_paired_terminal_valuations_match_direct_scan_and_reuse_without_depth_io(bundle, monkeypatch):
    from dataclasses import replace
    from data.runtime import ConsumerBundle
    from models.backtest_tick import prepare_public_inputs, simulate_prepared_inputs
    from models.replay.public_accounting import terminal_valuations, settle_public_replay

    # Force batch boundaries, including endpoints equal to an observation clock.
    original = ConsumerBundle.batches
    calls = []
    def small_batches(self, name, **kwargs):
        calls.append(name)
        yield from original(self, name, batch_size=1)
    monkeypatch.setattr(ConsumerBundle, 'batches', small_batches)
    ends = [SECOND + 1, 1_300_000_000, 2_300_000_000, 2_500_000_000, 4*SECOND]
    values = terminal_valuations(bundle, ends + ends, max_mark_age_ns=SECOND)
    assert calls == ['depth']
    rows = ConsumerBundle(bundle).table('depth').to_pylist()
    for end in ends:
        earlier = [r for r in rows if r['ready_ns'] < end]
        last = earlier[-1] if earlier else None
        age = None if last is None or last['source_asof_ns'] is None else end-last['source_asof_ns']
        mark = None
        if (last and last['valid'] and not last['stale'] and age is not None
                and 0 <= age <= SECOND and last['bid_px'] and last['ask_px']):
            mark = (float(last['bid_px'][0])+float(last['ask_px'][0]))/2
        assert (values[end].price, values[end].age_ns) == (mark, age)

    params = public_replay_params()
    prepared = prepare_public_inputs(bundle, tick_size=params['tick_size'])
    end = 2_500_000_000
    result = simulate_prepared_inputs(prepared, params, execution_end_ns=end)
    kw = dict(initial_capital=100., max_mark_age_ns=SECOND)
    direct = settle_public_replay(bundle, result, **kw)
    def no_scan(*args, **kwargs):
        raise AssertionError('paired settlement must reuse the prepared valuation')
    monkeypatch.setattr(ConsumerBundle, 'batches', no_scan)
    for _ in range(2):
        assert settle_public_replay(bundle, result, terminal_valuation=values[end], **kw) == direct
    for bad in (replace(values[end], input_manifest_id='wrong'),
                replace(values[end], end_ns=end+1), replace(values[end], max_mark_age_ns=0)):
        with pytest.raises(ValueError, match='valuation input, endpoint or age policy'):
            settle_public_replay(bundle, result, terminal_valuation=bad, **kw)


def test_prefill_feature_adapter_observes_without_changing_baseline_execution(bundle):
    from models.backtest_tick import prepare_public_inputs, simulate_prepared_inputs
    from strategy.risk_selection import PREFILL_FEATURE_CONTRACT
    from tests.test_tick_runtime_checkpoint import assert_same

    params = {**public_replay_params(), 'risk_selection_collect_opportunities': True,
              'risk_selection_scope': 'visible_inventory'}
    prepared = prepare_public_inputs(bundle, tick_size=params['tick_size'])
    baseline = simulate_prepared_inputs(prepared, params)
    current = simulate_prepared_inputs(prepared, {
        **params, 'risk_selection_feature_contract': PREFILL_FEATURE_CONTRACT})
    for key in ('_fill_trace', 'fills_total', 'final_inventory', 'cash_before_terminal',
                'pnl', 'risk_selection_intervention_count', 'risk_selection_opportunity_counts'):
        assert_same(baseline[key], current[key])
    assert current['_risk_selection_opportunities']
    for bad in ('unknown', False):
        with pytest.raises(ValueError, match='unknown risk selection feature contract'):
            simulate_prepared_inputs(prepared, {**params, 'risk_selection_feature_contract': bad})
    with pytest.raises(ValueError, match='visible-inventory'):
        simulate_prepared_inputs(prepared, {**params, 'risk_selection_scope': 'reachable_inventory',
            'risk_selection_feature_contract': PREFILL_FEATURE_CONTRACT})


def test_bounded_public_replay_executes_cutoff_without_changing_input(bundle):
    from hashlib import sha256
    from models.backtest_tick import prepare_public_inputs, simulate_prepared_inputs
    from models.replay.public_accounting import settle_public_replay, funding_window

    before = sha256((bundle / 'manifest.json').read_bytes()).hexdigest()
    params = public_replay_params()
    prepared = prepare_public_inputs(bundle, tick_size=params['tick_size'])
    end = 2_500_000_000
    result = simulate_prepared_inputs(prepared, params, execution_end_ns=end)
    assert result['execution_window'] == dict(schema='public_bounded_execution.v1',
        exclusive_end_ns=end, last_market_event_ns=end - 1_000_000,
        first_unprocessed_event_ns=end, completed=True)
    assert result['public_input_contract']['account_end_ns'] == end
    assert all(r['fill_ts'] * 1_000_000 < end for r in result['_fill_trace'])
    funding = dict(market_id=MARKET, source_identity='controlled-funding',
        coverage_start_ns=SECOND, coverage_end_ns=4*SECOND,
        expected_settlements_ns=[end, end+1_000_000],
        events=[dict(settlement_ns=t, mark_price=100., rate=.01)
                for t in (end, end+1_000_000)])
    selected = funding_window(funding, start_ns=SECOND, end_ns=end)
    assert selected['expected_settlements_ns'] == [end]
    assert len(funding['events']) == 2
    report = settle_public_replay(bundle, result, initial_capital=100.,
        max_mark_age_ns=SECOND, funding=selected)
    assert report['account_end_ns'] == end
    assert report['economic_complete']
    assert sha256((bundle / 'manifest.json').read_bytes()).hexdigest() == before
    # Future trades are retained in the parent input but cannot enter this
    # bounded clock, its queue budget, or its account.
    from dataclasses import replace
    from types import MappingProxyType
    from tests.test_tick_runtime_checkpoint import assert_same
    future = prepared.inputs['trades'].iloc[[0]].copy()
    future['transact_time'] = 3000
    future['price'] = 9999.
    future['quantity'] = 999.
    changed_trades = pd.concat([prepared.inputs['trades'], future], ignore_index=True)
    changed_trades.attrs = dict(prepared.inputs['trades'].attrs)
    changed = replace(prepared, inputs=MappingProxyType({**prepared.inputs, 'trades': changed_trades}))
    assert_same(simulate_prepared_inputs(changed, params, execution_end_ns=end), result)
    without_proof = dict(result)
    without_proof.pop('execution_window')
    with pytest.raises(ValueError, match='actual completed execution window'):
        settle_public_replay(bundle, without_proof, initial_capital=100., max_mark_age_ns=SECOND)
    for bad in (True, 2_500_000_001, SECOND, 5*SECOND):
        with pytest.raises(ValueError, match='execution end'):
            simulate_prepared_inputs(prepared, params, execution_end_ns=bad)


def test_bounded_checkpoint_cannot_change_its_execution_end(bundle):
    from models.backtest_tick import prepare_public_inputs, simulate_prepared_inputs
    from tests.test_tick_runtime_checkpoint import assert_same

    params = public_replay_params()
    prepared = prepare_public_inputs(bundle, tick_size=params['tick_size'])
    expected = simulate_prepared_inputs(prepared, params, execution_end_ns=3*SECOND)
    checkpoint = simulate_prepared_inputs(prepared, params, execution_end_ns=3*SECOND,
        checkpoint_at_ts_ms=2000)['_replay_checkpoint']
    actual = simulate_prepared_inputs(prepared, params, execution_end_ns=3*SECOND,
        resume_checkpoint=checkpoint)
    assert_same(actual, expected)
    with pytest.raises(ValueError, match='input, parameters, predictions or implementation changed'):
        simulate_prepared_inputs(prepared, params, execution_end_ns=4*SECOND,
            resume_checkpoint=checkpoint)


def test_funding_window_does_not_hide_missing_parent_events():
    from models.replay.public_accounting import funding_window

    parent = dict(source_identity='fixture', coverage_start_ns=0, coverage_end_ns=100,
        events=[], expected_settlements_ns=[90])
    with pytest.raises(ValueError, match='incomplete'):
        funding_window(parent, start_ns=10, end_ns=20)


def test_prefill_parent_segments_advance_once_and_keep_full_account(bundle):
    from models.backtest_tick import prepare_public_inputs, simulate_prepared_inputs
    from research.families.f05_fill_quality_quote_ev.public_input import iter_prefill_parent_segments
    from strategy.risk_selection import PREFILL_FEATURE_CONTRACT
    from tests.test_tick_runtime_checkpoint import assert_same

    params = dict(public_replay_params(), risk_selection_collect_opportunities=True,
                  risk_selection_scope='visible_inventory',
                  risk_selection_feature_contract=PREFILL_FEATURE_CONTRACT)
    prepared = prepare_public_inputs(bundle, tick_size=.1)
    direct = simulate_prepared_inputs(prepared, params)
    segments = list(iter_prefill_parent_segments(prepared, params,
                    cut_times_ms=[1200, 1800, 2600, 3200]))
    assert segments[0]['checkpoint'] is None
    assert not segments[0]['opportunities']
    for previous, following in zip(segments, segments[1:], strict=False):
        assert following['checkpoint'] is previous['next_checkpoint']
    rows = [r for segment in segments for r in segment['opportunities']]
    assert rows == direct['_risk_selection_opportunities']
    assert_same(segments[-1]['result'], direct)
    resumed = list(iter_prefill_parent_segments(prepared, params,
        cut_times_ms=[1800, 2600, 3200], resume_checkpoint=segments[1]['next_checkpoint']))
    assert_same(resumed[-1]['result'], direct)
    assert [r for part in resumed for r in part['opportunities']] == rows[len(
        segments[1]['next_checkpoint']['runtime'].risk_selection.rows):]
    with pytest.raises(ValueError, match='begin at account start'):
        list(iter_prefill_parent_segments(prepared, params, cut_times_ms=[1800]))


def test_prefill_parent_retires_previous_generations(bundle):
    import weakref
    from models.backtest_tick import prepare_public_inputs
    from research.families.f05_fill_quality_quote_ev.public_input import iter_prefill_parent_segments
    from strategy.risk_selection import PREFILL_FEATURE_CONTRACT

    params = dict(public_replay_params(), risk_selection_collect_opportunities=True,
                  risk_selection_scope='visible_inventory',
                  risk_selection_feature_contract=PREFILL_FEATURE_CONTRACT)
    prepared = prepare_public_inputs(bundle, tick_size=.1)
    parents = iter_prefill_parent_segments(prepared, params,
        cut_times_ms=[1200, 1800, 2600, 3200])
    refs = []
    for segment in parents:
        current = segment['next_checkpoint']
        if current is not None:
            # Trade columns are prepared-owned immutable storage; sampling
            # arrays represent each generation's independently mutable state.
            refs.append(weakref.ref(current['runtime'].pnl_arr))
        # Only the preceding and current cut are needed by this consumer.
        assert all(ref() is None for ref in refs[:-2])
        del current, segment
    assert all(ref() is None for ref in refs)


def test_prefill_suffix_iterator_settles_real_branches_and_reuses_parent(bundle, tmp_path, monkeypatch):
    import weakref
    from models import backtest_tick
    from data.runtime import ConsumerBundle
    from models.backtest_tick import prepare_public_inputs, simulate_prepared_inputs
    from research.families.f05_fill_quality_quote_ev.public_input import iter_prefill_suffix_labels
    from strategy.risk_selection import PREFILL_FEATURE_CONTRACT

    plan = dict(ConsumerBundle(bundle).manifest['plan'])
    plan['end_ns'] = 40*SECOND
    extended = tmp_path/'suffix-input'
    derive_inputs(plan, extended)
    params = dict(public_replay_params(), risk_selection_collect_opportunities=True,
        risk_selection_scope='visible_inventory',
        risk_selection_feature_contract=PREFILL_FEATURE_CONTRACT)
    prepared = prepare_public_inputs(extended, tick_size=params['tick_size'])
    parent = simulate_prepared_inputs(prepared, params)
    row = parent['_risk_selection_opportunities'][0]
    checkpoint = simulate_prepared_inputs(prepared, params,
        checkpoint_at_ts_ms=row['decision_ts_ns']//1_000_000)['_replay_checkpoint']
    funding = dict(market_id=plan['market_id'], coverage_start_ns=plan['start_ns'],
        coverage_end_ns=plan['end_ns'], source_identity='controlled-fixture',
        events=[], expected_settlements_ns=[])
    kwargs = dict(checkpoint=checkpoint, opportunities=[row], prepared_predictions=None,
        funding=funding, initial_capital=10000., max_mark_age_ns=100*SECOND)
    original = backtest_tick.simulate_prefill_branch
    branch_arrays = []

    def tracked(*args, **kw):
        result = original(*args, **kw)
        result['_lifetime_probe'] = np.zeros(1)
        branch_arrays.append(weakref.ref(result['_lifetime_probe']))
        return result

    monkeypatch.setattr(backtest_tick, 'simulate_prefill_branch', tracked)
    iterator = iter_prefill_suffix_labels(prepared, params, verify_first_control=True, **kwargs)
    receipt = next(iterator)
    assert len(branch_arrays) == 3
    assert all(ref() is None for ref in branch_arrays)
    iterator.close()
    assert receipt['branch_calls'] == 2
    assert all(a['economic_complete'] for a in receipt['accounting'])
    assert receipt['label']['horizon_ns'] == 30*SECOND
    repeated, = iter_prefill_suffix_labels(prepared, params, **kwargs)
    assert repeated['label'] == receipt['label']
    from models.replay.public_input import PreparedReplayInputs
    with monkeypatch.context() as context:
        context.setattr(PreparedReplayInputs, 'immutable_clone_inputs', lambda *args: ())
        unshared, = iter_prefill_suffix_labels(prepared, params, verify_first_control=True, **kwargs)
    assert unshared['label'] == receipt['label']
    assert unshared['accounting'] == receipt['accounting']
    with pytest.raises(ValueError, match='duplicate'):
        list(iter_prefill_suffix_labels(prepared, params,
             **{**kwargs, 'opportunities': [row, row]}))


@pytest.mark.parametrize('kind,baseline_action,alternative_action', [('E', 'POST', 'WAIT'), ('C', 'KEEP', 'CANCEL')])
@pytest.mark.parametrize('prefill_contract', [False, True])
def test_prefill_checkpoint_branch_reaches_one_exact_decision_and_preserves_parent(
        bundle, tmp_path, monkeypatch, kind, baseline_action, alternative_action, prefill_contract):
    from data.runtime import ConsumerBundle
    from models.backtest_tick import prepare_public_inputs, simulate_prepared_inputs, simulate_prefill_branch
    from tests.test_tick_runtime_checkpoint import assert_same
    from strategy.risk_selection import PREFILL_FEATURE_CONTRACT, PREFILL_FEATURE_UNITS

    plan = dict(ConsumerBundle(bundle).manifest['plan'])
    plan['end_ns'] = 40*SECOND
    extended = tmp_path/'extended'
    derive_inputs(plan, extended)
    params = {**public_replay_params(), 'risk_selection_collect_opportunities': True,
              'risk_selection_scope': 'visible_inventory', 'requote_threshold_bps': 10000.}
    if prefill_contract:
        params['risk_selection_feature_contract'] = PREFILL_FEATURE_CONTRACT
    prepared = prepare_public_inputs(extended, tick_size=params['tick_size'])
    parent = simulate_prepared_inputs(prepared, params)
    opportunity = next(r for r in parent['_risk_selection_opportunities'] if r['kind'] == kind)
    if prefill_contract:
        assert opportunity['feature_contract'] == PREFILL_FEATURE_CONTRACT
        assert set(opportunity['features']) == set(PREFILL_FEATURE_UNITS)
        assert opportunity['features']['side_l1_imbalance'] is not None
        assert opportunity['features']['spread_ticks'] > 0
        assert opportunity['features']['side_trade_imbalance_5000ms'] is None
        assert opportunity['features']['adverse_mid_change_5000ms_bps'] is None
        if kind == 'E':
            assert opportunity['features']['local_order_age_ms'] == 0
        else:
            assert opportunity['features']['local_order_age_ms'] > 0
    checkpoint = simulate_prepared_inputs(prepared, params,
        checkpoint_at_ts_ms=opportunity['decision_ts_ns']//1_000_000)['_replay_checkpoint']
    # Reverse branch order and repeat: no mutable state is consumed by a fork.
    wait = simulate_prefill_branch(prepared, params, checkpoint=checkpoint,
        opportunity=opportunity, action=alternative_action)
    post = simulate_prefill_branch(prepared, params, checkpoint=checkpoint,
        opportunity=opportunity, action=baseline_action)
    again = simulate_prefill_branch(prepared, params, checkpoint=checkpoint,
        opportunity=opportunity, action=alternative_action)
    assert_same(wait, again)
    assert_same(post, simulate_prefill_branch(prepared, params, checkpoint=checkpoint,
        opportunity=opportunity, action=baseline_action))
    from models.replay.public_input import PreparedReplayInputs
    with monkeypatch.context() as context:
        context.setattr(PreparedReplayInputs, 'immutable_clone_inputs', lambda *args: ())
        for action, expected in ((baseline_action, post), (alternative_action, wait)):
            assert_same(expected, simulate_prefill_branch(prepared, params, checkpoint=checkpoint,
                opportunity=opportunity, action=action))
    end = opportunity['decision_ts_ns']+30*SECOND
    assert post['prefill_fork'] == wait['prefill_fork']
    assert post['risk_selection_intervention_count'] == 0
    assert wait['risk_selection_intervention_count'] == 1
    for result in (post, wait):
        assert result['execution_window']['exclusive_end_ns'] == end
        assert result['execution_window']['first_unprocessed_event_ns'] >= end
        assert all(r['fill_ts']*1_000_000 < end for r in result['_fill_trace'])
    assert not checkpoint['runtime'].risk_selection.target
    assert_same(simulate_prepared_inputs(prepared, params, resume_checkpoint=checkpoint), parent)
    with pytest.raises(ValueError, match='parameters, predictions or implementation changed'):
        simulate_prefill_branch(prepared, {**params, 'maker_fee': .01}, checkpoint=checkpoint,
            opportunity=opportunity, action=baseline_action)
    with pytest.raises(ValueError, match='exact target'):
        simulate_prefill_branch(prepared, params, checkpoint=checkpoint,
            opportunity={**opportunity, 'opportunity_id': 'missing'}, action=baseline_action)


def _delayed_accounting_fixture(bundle):
    facts = [dict(match_sequence=i, order_id=i+10, fill_ts=t, side=side,
                  fill_qty=1., quote_px=px, fill_fee_usdc=1.)
             for i, t, side, px in [(0, 2000, "BUY", 101.), (1, 3000, "SELL", 100.)]]
    notifications = [dict(facts[i], fill_sequence=j, economic_match_sequence=i,
        fill_clock_context=dict(match_ts_ms=facts[i]["fill_ts"], visible_ts_ms=v,
                                processed_ts_ms=v))
        for j, (i, v) in enumerate([(1, 3100), (0, 3500)])]
    result = dict(public_input_contract={"account_start_ns": SECOND, **binding(bundle)},
        private_fill_visibility_enabled=True,
        economic_fill_contract="match_facts_and_local_notifications.v1",
        _economic_fill_trace=facts, _fill_trace=notifications, fills_total=2,
        private_fill_exchange_match_count=2, private_fill_visible_count=2,
        private_fill_pending_visibility_count=0, final_inventory=0., cash_before_terminal=-3.,
        economic_match_inventory=0., exchange_inventory_at_window_end=0., economic_match_cash=-3.)
    funding = dict(market_id=MARKET, source_identity="controlled-accounting-fixture",
        coverage_start_ns=SECOND, coverage_end_ns=4*SECOND,
        expected_settlements_ns=[2_500_000_000],
        events=[dict(settlement_ns=2_500_000_000, mark_price=100., rate=.01)])
    return result, funding


def test_delayed_accounting_funding_uses_match_not_notification_order(bundle):
    from models.replay.public_accounting import settle_public_replay
    result, funding = _delayed_accounting_fixture(bundle)
    report = settle_public_replay(bundle, result, initial_capital=100.,
                                  max_mark_age_ns=SECOND, funding=funding)
    assert report["funding_cashflow"] == -1.
    assert report["all_in_net_pnl"] == -4.
    assert [r["fill_ts"] for r in result["_fill_trace"]] == [3000, 2000]
    assert report["economic_complete"]


def test_delayed_accounting_keeps_matched_but_unnotified_terminal_inventory(bundle):
    from models.replay.public_accounting import settle_public_replay
    result, funding = _delayed_accounting_fixture(bundle)
    result.update(_fill_trace=[], fills_total=0, private_fill_visible_count=0,
        _economic_fill_trace=result["_economic_fill_trace"][:1],
        private_fill_exchange_match_count=1, private_fill_pending_visibility_count=1,
        final_inventory=0., cash_before_terminal=0., economic_match_inventory=1.,
        exchange_inventory_at_window_end=1., economic_match_cash=-102.)
    report = settle_public_replay(bundle, result, initial_capital=100.,
                                  max_mark_age_ns=4*SECOND, funding=funding)
    assert report["terminal_inventory"] == 1.
    assert report["funding_cashflow"] == -1.
    # This fixture's terminal book is stale. Preserving the pending match must
    # not turn a missing valuation into a fictitious complete PnL.
    assert not report["economic_complete"]
    assert report["all_in_net_pnl"] is None
    assert result["final_inventory"] == 0.  # No invented strategy notification.


@pytest.mark.parametrize("fault", ["missing", "duplicate", "binding", "order", "backwards", "cutoff", "old"])
def test_delayed_accounting_rejects_invalid_matching_evidence(bundle, fault):
    from models.replay.public_accounting import settle_public_replay
    result, funding = _delayed_accounting_fixture(bundle)
    if fault == "missing":
        result["_economic_fill_trace"].pop()
    elif fault == "duplicate":
        result["_fill_trace"][1]["economic_match_sequence"] = 1
    elif fault == "binding":
        result["_fill_trace"][1]["fill_ts"] += 1
    elif fault == "order":
        result["_economic_fill_trace"][1]["match_sequence"] = 0
    elif fault == "backwards":
        result["_economic_fill_trace"][1]["fill_ts"] = 1999
        result["_fill_trace"][0]["fill_ts"] = 1999
        result["_fill_trace"][0]["fill_clock_context"]["match_ts_ms"] = 1999
    elif fault == "cutoff":
        result["_economic_fill_trace"][1]["fill_ts"] = 4000
        result["_fill_trace"][0].update(fill_ts=4000,
            fill_clock_context=dict(match_ts_ms=4000, visible_ts_ms=4000, processed_ts_ms=4000))
        result["_fill_trace"][1]["fill_clock_context"].update(visible_ts_ms=4001, processed_ts_ms=4001)
    else:
        result.pop("economic_fill_contract")
    with pytest.raises(ValueError):
        settle_public_replay(bundle, result, initial_capital=100., max_mark_age_ns=SECOND, funding=funding)
