from dataclasses import replace
from decimal import Decimal
import math
import pickle

import pytest

from data.depth_observation import DepthStateTap, MeasuredDepthStateTap
from research.families.f08_side_taker_lifecycle.depth_dynamics import VisibleDepthEndpoints
from research.families.f08_side_taker_lifecycle.depth_price_reference import (
    GeometryEwma, ReferenceMass, component_corrections, radius_grid,
)
from research.families.f08_side_taker_lifecycle.depth_quote_runtime import DepthQuotingPolicy
from research.families.f08_side_taker_lifecycle.depth_price_replay import DepthPriceReplayRuntime
from tests.test_depth_observation import stream
from tests.test_optional_update_branch import synthetic_parent  # noqa: F401
from tests.test_depth_price_replay import clocks
from tests.test_target_observation import exact
from strategy.quote_core import DepthSnapshot, QuoteState


def new_runtime(arm='OBSERVE_ZERO', cost=0):
    keys = ('source_events','source_levels','copied_points','published_levels','publications','indexed_points',
            'radius_integrations','region_queries','endpoint_queries','correction_queries','audit_bytes')
    costs = dict.fromkeys(keys, 0)
    costs['correction_queries'] = cost
    return DepthPriceReplayRuntime(arm=arm, tick_size=.1, source_config={}, cost_profile=dict(
        schema='depth-quoting-work-cost.v1', measurement_id='synthetic-not-formal', ns_per_unit=costs))


@pytest.mark.parametrize('arm', sorted(DepthQuotingPolicy.ARMS))
def test_complete_account_records_the_new_runtime_immutable_contract(arm):
    from copy import deepcopy
    from narrowgate.replay.account import effective_parameter_record
    runtime = new_runtime(arm, cost=10)
    expected = deepcopy(runtime.contract)
    record = effective_parameter_record({'_quote_price_runtime': runtime})
    runtime.counts['captures'] += 1
    assert record == {'_quote_price_runtime': expected}
    assert effective_parameter_record({'_quote_price_runtime': runtime}) == record


@pytest.mark.parametrize('arm', sorted(DepthQuotingPolicy.ARMS))
def test_new_policy_runs_through_complete_account_and_terminal_validation(
        synthetic_parent, tmp_path, monkeypatch, arm):  # noqa: F811
    import json
    from narrowgate.replay import account
    import research.families.f08_side_taker_lifecycle.depth_price_replay as module
    from tests.test_independent_incremental_journal import configuration
    from research.families.f01_fixed_parameter_racing.inventory_soft_control import ARMS
    monkeypatch.setattr(module, 'streams', lambda config, tap: stream(tap))
    prepared, base = synthetic_parent
    target = tmp_path / arm
    cfg = configuration(prepared, base, target, ARMS[1], monkeypatch)
    cfg['params']['_quote_price_runtime'] = new_runtime(arm, cost=2_100_000)
    cfg['params'].update(clocks())
    cfg['incremental_journal'] = True
    assert account.execute_bound_cell(prepared, **cfg)['status'] == 'COMPLETE'
    cfg['validate_output'](target)
    recorded = json.loads((target / 'effective_params.json').read_text())
    assert recorded['_quote_price_runtime']['schema'] == 'quote-price-replay-v2'
    assert account.execute_bound_cell(prepared, **cfg)['status'] == 'ALREADY_COMPLETE'


def test_new_zero_control_through_actual_executor_and_cold_restore(synthetic_parent, monkeypatch, tmp_path):  # noqa: F811
    from models.backtest_tick import simulate_prepared_inputs
    from models.replay.runtime_checkpoint_io import load_trusted_runtime_checkpoint, save_runtime_checkpoint
    import research.families.f08_side_taker_lifecycle.depth_price_replay as module
    monkeypatch.setattr(module, 'streams', lambda config, tap: stream(tap))
    prepared, base = synthetic_parent
    params = dict(base, **clocks())
    baseline = simulate_prepared_inputs(prepared, params)
    zero = simulate_prepared_inputs(prepared, dict(params, _quote_price_runtime=new_runtime()))
    policy = zero.pop('quote_price_policy')
    assert policy['counts']['queries'] > 0
    assert policy['counts']['charged_ms'] == 0
    for row in zero.get('response_update_observation', {}).get('samples', []):
        row.get('compute', {}).pop('quote_price_extra_ms', None)
        row.get('compute', {}).pop('depth_quoting_capture_id', None)
    exact(baseline, zero)
    candidate = dict(params, _quote_price_runtime=new_runtime(cost=2_100_000))
    full = simulate_prepared_inputs(prepared, candidate)
    checkpoint = simulate_prepared_inputs(prepared, candidate, checkpoint_at_ts_ms=10000)['_replay_checkpoint']
    save_runtime_checkpoint(tmp_path/'new-policy', checkpoint)
    restored = load_trusted_runtime_checkpoint(tmp_path/'new-policy')
    exact(full, simulate_prepared_inputs(prepared, candidate, resume_checkpoint=restored))
    for row in full['response_update_observation']['samples']:
        assert row['compute']['quote_price_extra_ms'] >= 3
        for request in row.get('submitted_requests', []):
            assert request['request_ns'] >= row['compute']['request_ready_ts']*1_000_000


def test_complete_capture_panels_and_order_opportunities_share_capture_ids(synthetic_parent, monkeypatch, tmp_path):  # noqa: F811
    from models.backtest_tick import simulate_prepared_inputs
    from models.replay.l2_journal import ReplayL2Journal
    from execution.chunked_parquet_journal import iter_chunked_parquet_journal
    import research.families.f08_side_taker_lifecycle.depth_price_replay as module
    monkeypatch.setattr(module, 'streams', lambda config, tap: stream(tap))
    prepared, base = synthetic_parent
    journal = ReplayL2Journal(tmp_path/'journal', identity={'engineering': 'new-quoting'})
    result = simulate_prepared_inputs(prepared, dict(base, **clocks(),
        _quote_price_runtime=new_runtime(), _l2_journal=journal))
    rows = list(iter_chunked_parquet_journal(result['_l2_journal']['manifest'],verify_checksum=True))
    panels = {r['record']['capture_id']: r['record'] for r in rows if r['event_type']=='depth_quoting_capture'}
    assert len(panels)==result['quote_price_policy']['counts']['captures']
    assert panels
    for row in rows:
        if row['event_type']=='decision' and row['record'].get('depth_quoting_capture_id'):
            assert row['record']['depth_quoting_capture_id'] in panels
    for panel in panels.values():
        assert set(panel['known_depth_bounds']) == {'bid', 'ask'}
        assert panel['source_ready_ns'] is None or panel['source_ready_ns'] <= panel['read_ns']
        assert set(panel['side_correction_index']) == {'BUY', 'SELL'}
        assert isinstance(panel['old_orders'], list)
        for index in panel['side_correction_index'].values():
            assert index is None or 0 <= index < len(panel['corrections'])
        for correction in panel['corrections']:
            assert set(correction['options'])=={'LOCAL_STATIC','LOCAL_DYNAMIC','QUOTE_STATIC','QUOTE_DYNAMIC'}
            assert {'I','T','F','E','masks','reasons'}<=correction['local'].keys()


def test_offline_book_bootstrap_is_not_decision_maintenance_cost():
    from data.tardis_input import BookMessage, ObservableBook
    tap=MeasuredDepthStateTap(measure=True)
    tap.work_start_ns=10_000_000_000
    book=ObservableBook()
    message=BookMessage(market_id='binance_futures:perpetual:BTCUSDC',source_file_id='engineering',
        first_row=1,last_row=2,source_ordinal=0,provider_group_id='snapshot',kind='snapshot',
        source_timestamp_us=1_000_000,source_clock_kind='unknown',exchange_ts_ns=None,
        provider_receive_ts_us=1_000_000,boundary_status='complete',state_time_certainty='observed',
        levels=(('bid',Decimal('999'),Decimal('2')),('ask',Decimal('1001'),Decimal('3'))))
    book.apply(message);tap.source_applied(message,book)
    assert tap.offline_bootstrap_work['source_events']==1
    assert tap.work['source_events']==0
    newer=replace(message,kind='delta',source_ordinal=1,source_timestamp_us=10_000_000,
                  levels=(('bid',Decimal('999'),Decimal('4')),))
    book.apply(newer);tap.source_applied(newer,book)
    tap.publish('current',book,10_000_000_000,0)
    state=tap.pending['current']
    assert state.quantity('bid',Decimal('999'))==4
    assert state.quantity('ask',Decimal('1001'))==3
    assert dict(tap.publication_work['current'][0])['source_events']==1


def observation(t=0, *, factor=1, rebase=0):
    base = next(o for tick in stream(DepthStateTap()) for o in tick.research_depth_observations)
    buckets = tuple({key: q * (factor if key[0] == 'bid' else 1) for key, q in bucket.items()}
                    for bucket in base.snapshot.buckets)
    book = replace(base.snapshot.book, bids=tuple((p, q * factor) for p, q in base.snapshot.book.bids),
                   version=t + 1)
    return replace(base, original=replace(base.original, ready_ns=t, source_asof_ns=t,
                                         publish_ns=t, receive_ns=t, event_id=str(t)),
                   snapshot=replace(base.snapshot, buckets=buckets, book=book, snapshot_version=rebase))


def query(policy, t, bid=990, ask=1010):
    book = policy.history.current.snapshot.book
    return policy.correction(bid=bid, ask=ask, read_ns=t,
                             state=QuoteState(mid=1000, best_bid=999, best_ask=1001, inventory=0, sigma_sq=1),
                             depth=DepthSnapshot(bids=tuple((float(p), float(q)) for p, q in book.bids),
                                                 asks=tuple((float(p), float(q)) for p, q in book.asks)))


def test_radius_grid_and_widths_are_fixed_integer_not_fitted():
    grid = radius_grid(100_000)
    assert grid[:12] == list(range(9)) + [9, 11, 13]
    assert grid[-1] >= 100_000
    for a, b in zip(grid[1:-1], grid[2:], strict=True):
        assert b == max(a + 1, (9 * a + 7) // 8)
    with pytest.raises(ValueError):
        radius_grid(1.2)


def test_ewma_constant_time_weight_missing_mass_and_zero_duration():
    one, many = ReferenceMass(), ReferenceMass()
    one.integrate(60_000_000_000, 3., 60_000_000_000)
    for _ in range(600):
        many.integrate(100_000_000, 3., 60_000_000_000)
    assert one.query()['available']
    assert many.query()['rho_ref'] == pytest.approx(3.)
    assert vars(one) == pytest.approx(vars(many), rel=1e-12, abs=1e-14)
    before = vars(one).copy()
    one.integrate(0, 1000., 60_000_000_000)
    assert vars(one) == before
    one.integrate(60_000_000_000, None, 60_000_000_000)
    assert one.valid_mass == pytest.approx(.25)
    assert one.nominal_mass == pytest.approx(.75)
    assert one.numerator == pytest.approx(.75)
    assert not one.query()['available']


def test_source_expiry_splits_held_interval_and_new_radius_is_cold():
    bank = GeometryEwma()
    bank.publish(now=0, expiry=1_000_000_000, densities={1: 3.})
    bank.integrate_to(60_000_000_000)
    expected = ReferenceMass()
    expected.integrate(1_000_000_000, 3., bank.half_life)
    expected.integrate(59_000_000_000, None, bank.half_life)
    assert vars(bank.mass[1]) == vars(expected)
    bank.establish(4, 99.)
    assert bank.mass[4].valid_mass == 0
    restored = pickle.loads(pickle.dumps(bank))
    restored.integrate_to(61_000_000_000)
    bank.integrate_to(61_000_000_000)
    assert vars(restored.mass[1]) == vars(bank.mass[1])


def test_reference_does_not_learn_new_state_backwards():
    bank = GeometryEwma()
    bank.publish(now=0, expiry=100_000_000_000, densities={1: 3.})
    bank.publish(now=60_000_000_000, expiry=100_000_000_000, densities={1: 99.})
    assert bank.query(1)['rho_ref'] == pytest.approx(3.)
    bank.publish(now=60_000_000_000, expiry=100_000_000_000, densities={1: 50.})
    assert bank.query(1)['rho_ref'] == pytest.approx(3.)


def test_partial_components_mirror_and_missing_preservation():
    missing = dict(available=False, reason='reference_warmup', rho_ref=None)
    row = component_corrections(qb=3, qa=1, grid_points=2, reference=missing,
                               previous_qb=1, previous_qa=1)
    assert row['I'] == .5 and row['T'] is None
    assert row['F'] == pytest.approx(1 / 3)
    assert row['static'] == dict(u=.05, v=0.)
    assert row['dynamic']['u'] > row['static']['u']
    mirror = component_corrections(qb=1, qa=3, grid_points=2, reference=missing,
                                  previous_qb=1, previous_qa=1)
    assert mirror['dynamic']['u'] == -row['dynamic']['u']
    assert mirror['dynamic']['v'] == row['dynamic']['v']
    no_dynamic = component_corrections(qb=3, qa=1, grid_points=2, reference=missing)
    assert no_dynamic['static'] == no_dynamic['dynamic']
    assert no_dynamic['F'] is None and no_dynamic['E'] is None


@pytest.mark.parametrize('arm', sorted(DepthQuotingPolicy.ARMS))
def test_full_history_not_top20_and_current_imbalance_survives_warmup(arm):
    policy = DepthQuotingPolicy(arm=arm, tick_size=1)
    policy.advance(observation(factor=3))
    row = query(policy, 0)
    assert policy.history.current.snapshot.quantity('bid', Decimal(975)) == 3
    assert row['local']['masks'] == dict(I=True, T=False, D=False)
    assert row['quote']['masks'] == dict(I=True, T=False, D=False)
    assert row['local']['I'] == pytest.approx(.5)
    assert row['u'] == (0. if arm == 'OBSERVE_ZERO' else pytest.approx(.05))
    assert row['quote']['radius_ticks'] >= row['local']['radius_ticks']
    assert pickle.loads(pickle.dumps(policy)).last_audit == policy.last_audit


def test_snapshot_does_not_reset_valid_reference_or_disallow_endpoint_net_difference():
    policy = DepthQuotingPolicy(arm='QUOTE_DYNAMIC', tick_size=1)
    for second in range(66):
        policy.advance(observation(second * 10**9, factor=1 if second < 64 else 3,
                                   rebase=1 if second >= 64 else 0))
    row = query(policy, 65 * 10**9)
    assert row['quote']['masks'] == dict(I=True, T=True, D=True)
    assert row['quote']['endpoint']['crossed_gap_or_rebase']
    assert row['quote']['F'] > 0
    assert row['u'] > 0
    assert query(pickle.loads(pickle.dumps(policy)), 65 * 10**9) == query(policy, 65 * 10**9)


def test_endpoints_gap_does_not_fabricate_continuous_flow_or_future_endpoint():
    history = VisibleDepthEndpoints()
    history.advance(observation())
    history.advance(observation(5 * 10**9, factor=3, rebase=1))
    row = history.query(regions=[('bid', Decimal(990), Decimal(999))], read_ns=5 * 10**9)
    assert row['available'] and row['crossed_gap_or_rebase']
    assert row['quantities'] == [10.]
    history.advance(observation(6 * 10**9 + 1))
    row = history.query(regions=[('bid', Decimal(990), Decimal(999))], read_ns=6 * 10**9 + 1)
    assert not row['available'] and row['reason'] == 'endpoint_stale'


def test_current_staleness_and_unknown_region_never_become_zero_density():
    policy = DepthQuotingPolicy(arm='QUOTE_DYNAMIC', tick_size=1)
    policy.advance(observation(factor=3))
    row = query(policy, 0, bid=900, ask=1100)
    assert row['u'] == row['v'] == 0 and row['quote']['I'] is None
    stale = query(policy, 10**9 + 1)
    assert stale['reason'] == 'current_source_stale'
    assert not stale['quote']['masks']['I']


def test_same_capture_queries_do_not_learn_again():
    policy = DepthQuotingPolicy(arm='QUOTE_DYNAMIC', tick_size=1)
    policy.advance(observation())
    for second in range(1, 61):
        policy.advance(observation(second * 10**9))
    first = query(policy, 60 * 10**9)
    before = pickle.dumps(policy.reference)
    assert query(policy, 60 * 10**9) == first
    assert pickle.dumps(policy.reference) == before


def test_measured_tap_keeps_original_stream_and_does_not_charge_lookahead():
    original = list(stream())
    tap = MeasuredDepthStateTap(measure=True)
    expanded = list(stream(tap))
    assert [replace(t, research_depth_observations=()) for t in expanded] == original
    rows = [o for t in expanded for o in t.research_depth_observations]
    assert sum(dict(o.maintenance_work).get('source_events', 0) for o in rows) > 0
    assert all(o.measured_maintenance_ns >= 0 for o in rows)


def test_shared_runtime_calculates_all_options_and_combines_cost_once(monkeypatch):
    import research.families.f08_side_taker_lifecycle.depth_price_replay as replay
    monkeypatch.setattr(replay, 'streams', lambda config, tap: stream(tap))
    units = ('source_events source_levels copied_points published_levels publications indexed_points '
             'radius_integrations region_queries endpoint_queries correction_queries audit_bytes').split()
    profile = dict(schema='depth-quoting-work-cost.v1', measurement_id='synthetic-only',
                   ns_per_unit=dict.fromkeys(units, 1))
    runtime = DepthPriceReplayRuntime(arm='OBSERVE_ZERO', tick_size=1, source_config={}, cost_profile=profile)
    runtime.begin_capture(10**9)
    for _ in range(2):
        book = runtime.policy.history.current.snapshot.book
        row = runtime.correction(bid=990, ask=1010,
            state=QuoteState(mid=1000, best_bid=999, best_ask=1001, inventory=0, sigma_sq=1),
            depth=DepthSnapshot(bids=tuple((float(p), float(q)) for p, q in book.bids),
                                asks=tuple((float(p), float(q)) for p, q in book.asks)))
        assert row['u'] == row['v'] == 0
        assert len(row['options']) == 4
    elapsed = runtime.finish_capture()
    assert elapsed == math.ceil(sum(runtime.work.values()) / 1e6)
    assert runtime.work['correction_queries'] == 2
    with pytest.raises(ValueError, match='already charged'):
        runtime.finish_capture()


@pytest.mark.parametrize('arm', sorted(DepthQuotingPolicy.ARMS))
def test_first_edition_final_identity_requires_its_frozen_nonzero_cost(arm):
    from research.families.f08_side_taker_lifecycle.depth_price_execution import validate_identity

    policy = dict(schema=DepthQuotingPolicy.SCHEMA, arm=arm)
    identity = dict(experiment_id='F08_DEPTH_QUOTING_20261024', baseline_id='B0_20261007_02',
        account_contract='DEPTH_QUOTING_FINAL107', parent_id='final-000-2026-05-28',
        start_ns=1779926400000000000, end_ns=1780099200000000000,
        seed_pair=dict(rng_seed=42, latency_seed=59), input_manifest_id='a' * 64,
        release_identity='b' * 64, arm_id=arm, policy=policy,
        cost_profile=dict(status='FROZEN_REAL_PREFLIGHT', ns_per_unit=dict(publications=10)))
    baseline = dict(spec=dict(identity, arm_id='DEFENSE_ON__URGENCY_OFF'))
    assert validate_identity(identity, baseline, policy) == identity
    identity['cost_profile']['status'] = 'ENGINEERING_ONLY'
    with pytest.raises(ValueError, match='frozen cost'):
        validate_identity(identity, baseline, policy)
    identity['cost_profile']['status'] = 'FROZEN_REAL_PREFLIGHT'
    identity['cost_profile']['ns_per_unit']['publications'] = 0
    with pytest.raises(ValueError, match='frozen cost'):
        validate_identity(identity, baseline, policy)
