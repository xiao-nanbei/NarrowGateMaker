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
    from research.families.f09_campaign_action_uplift.public_input import build_action_panel
    from research.families.f09_campaign_action_uplift.audit.offline_policy_evaluation import (
        OPEConfig, _prepare_panel,
    )
    assignment = dict(decision_id="a", decision_ts_ns=2 * SECOND, actual_outcome_end_ns=3 * SECOND,
                      action="hold", behavior_propensity=.5, start_state=account(bundle, 2000, 150),
                      end_state=account(bundle, 3000, 147), fees_usdc=1., funding_cashflow_usdc=0.,
                      **binding(bundle))
    kwargs = dict(feature_columns=["mid"], missing_policy="reject", max_age_ns=SECOND,
                  max_mark_age_ms=1000, training_boundary_ns=4 * SECOND)
    panel = build_action_panel(bundle, [assignment], **kwargs)
    assert panel.reward.tolist() == [-3]  # not campaign profit 47; fee already in cash
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
    from research.families.f09_campaign_action_uplift.audit.offline_policy_evaluation import (
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
