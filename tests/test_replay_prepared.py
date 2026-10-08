"""Prepared replay isolation and numeric tape equivalence, synthetic inputs."""

from dataclasses import asdict
import json

import numpy as np
import pytest

from test_research_public_inputs import bundle as input_bundle
from models import backtest_tick as replay

bundle = input_bundle


def test_numeric_frame_shell_preserves_metadata_and_isolates_writes():
    import pandas as pd
    from models.replay.public_input import _ExecutionFrame
    original = pd.DataFrame({'x': np.array([1, 2], dtype=np.int64), 'y': [1., float('nan')]},
                            index=pd.Index([9, 8], name='original'))
    original.attrs = {'nested': {'value': 1}}
    original.columns.name = 'event fields'
    owner = _ExecutionFrame.create(original)
    a, b = owner.frame(), owner.frame()
    pd.testing.assert_frame_equal(a, original)
    a.attrs['nested']['value'] = 7
    a['x'] = [4, 5]
    a.index = [1, 2]
    a.columns.name = 'mutated shell'
    pd.testing.assert_frame_equal(b, original)
    for _, array in owner.columns:
        with pytest.raises(ValueError):
            array.setflags(write=True)
    assert _ExecutionFrame.create(pd.DataFrame({'x': [object()]})) is None


def test_event_numeric_storage_is_immutable_and_clone_shared(bundle):
    from models.replay.runtime_checkpoint_io import clone_runtime_state
    prepared = replay.prepare_public_inputs(bundle, tick_size=.1)
    cp = replay.simulate_prepared_inputs(prepared, parameters(),
                                         checkpoint_at_ts_ms=1800)['_replay_checkpoint']
    rt = cp['runtime']
    clone = clone_runtime_state(rt, immutable_inputs=prepared.immutable_clone_inputs())
    for name in ('trade_ts', 'trade_price', 'trade_qty'):
        value = getattr(rt, name)
        assert getattr(clone, name) is value
        with pytest.raises(ValueError):
            value.setflags(write=True)
    for key, owner in prepared._clock_frames.items():
        assert owner is not None
        frame, _ = prepared._clocks[key]
        for name, array in owner.columns:
            assert np.shares_memory(frame[name].values, array)
            assert not frame[name].values.flags.writeable
    with pytest.raises(ValueError, match='admitted clock'):
        prepared.clock_arrays(b'foreign-clock')


def test_count_only_prepared_binding_restore_and_accounting(bundle, tmp_path):
    from models.replay import runtime_checkpoint_io as cio
    from models.replay.public_accounting import settle_public_replay
    from tests.test_opportunity_retention import business
    from test_tick_runtime_checkpoint import assert_same
    prepared = replay.prepare_public_inputs(bundle, tick_size=.1)
    params = dict(parameters(), risk_selection_collect_opportunities=True)
    full = replay.simulate_prepared_inputs(prepared, params)
    params['risk_selection_opportunity_retention'] = 'counts_only'
    expected = replay.simulate_prepared_inputs(prepared, params)
    assert_same(business(expected), business(full))
    cp = replay.simulate_prepared_inputs(prepared, params,
        checkpoint_at_ts_ms=1800)['_replay_checkpoint']
    path = tmp_path/'counts.checkpoint'
    cio.save_runtime_checkpoint(path, cp)
    restored = cio.load_trusted_runtime_checkpoint(path)
    actual = replay.simulate_prepared_inputs(prepared, params,
        resume_checkpoint=restored, consume_resume_checkpoint=True)
    assert_same(actual, expected)
    plan = prepared.inputs['bundle'].manifest['plan']
    funding = dict(market_id=plan['market_id'], source_identity='synthetic-funding',
                   coverage_start_ns=plan['start_ns'], coverage_end_ns=plan['end_ns'],
                   expected_settlements_ns=[2_500_000_000],
                   events=[dict(settlement_ns=2_500_000_000, mark_price=100., rate=.01)])
    accounting = dict(initial_capital=10000, funding=funding, max_mark_age_ns=1_000_000_000)
    assert_same(settle_public_replay(bundle, full, **accounting),
                settle_public_replay(bundle, actual, **accounting))
    with pytest.raises(ValueError, match='public checkpoint'):
        replay.simulate_prepared_inputs(prepared,
            {**params, 'risk_selection_opportunity_retention': 'full'}, resume_checkpoint=cp)


def test_static_coverage_reused_across_consume_segments(bundle, monkeypatch):
    from test_tick_runtime_checkpoint import assert_same
    from models.replay.public_input import PreparedReplayInputs
    prepared = replay.prepare_public_inputs(bundle, tick_size=.1)
    calls = dict(span=0, active=0, union=0, validate=0)
    for name, key in [('_historical_book_coverage_ratio', 'span'),
                      ('_historical_book_active_coverage_ratio', 'active'),
                      ('_coverage_union_seconds', 'union'),
                      ('_validate_historical_book_coverage', 'validate')]:
        original = getattr(replay, name)
        def counted(*args, _original=original, _key=key, **kwargs):
            calls[_key] += 1
            return _original(*args, **kwargs)
        monkeypatch.setattr(replay, name, counted)
    cp = None
    for cut in (1800, 2200, 2600):
        cp = replay.simulate_prepared_inputs(prepared, parameters(),
            checkpoint_at_ts_ms=cut, resume_checkpoint=cp,
            consume_resume_checkpoint=True)['_replay_checkpoint']
    actual = replay.simulate_prepared_inputs(prepared, parameters(),
        resume_checkpoint=cp, consume_resume_checkpoint=True)
    assert calls == dict(span=1, active=1, union=1, validate=4)
    assert all(len(value) == 2 for value in prepared._book_coverage.values())
    with monkeypatch.context() as context:
        context.setattr(PreparedReplayInputs, 'historical_book_coverage', lambda *a: None)
        expected = replay.simulate_prepared_inputs(prepared, parameters())
    assert_same(actual, expected)


@pytest.mark.parametrize('times,book_times,freshness', [
    ([0, 1000, 2000], [0, 1000, 2000], 1000),
    ([0, 1000, 2000, 3000], [0, 3000], 100),
    ([0, 0, 1000, 2000], [0, 0, 500, 1000], 1000),
    ([0], [0], 1000), ([], [], 1000), ([0, 1000], [], 1000),
    ([0, 1000], [0, 1000], 0), ([0, 1000], [0, 1000], -1),
])
def test_static_coverage_values_equal_original(bundle, times, book_times, freshness):
    from dataclasses import replace
    from models.replay.public_input import _immutable_array
    import pandas as pd
    prepared = replay.prepare_public_inputs(bundle, tick_size=.1)
    bbo = replace(prepared.inputs['bbo'], ts_ms=_immutable_array(book_times))
    l2 = replace(prepared.inputs['l2'], ts_ms=_immutable_array([500, 1500]))
    # Synthetic admitted owner with immutable book timelines.
    from types import MappingProxyType
    prepared = replace(prepared, inputs=MappingProxyType(dict(prepared.inputs, bbo=bbo, l2=l2)))
    key = b'synthetic-clock'
    prepared._clocks[key] = pd.DataFrame({'transact_time': times}), len(times)
    for books in ((bbo, l2), (bbo, None), (None, l2), (None, None)):
        expected = replay._historical_book_coverage_values(np.asarray(times), *books, freshness)
        actual = prepared.historical_book_coverage(key, *books, freshness,
                                                   replay._historical_book_coverage_values)
        assert actual == expected


def test_static_coverage_threshold_and_identity_changes(bundle, monkeypatch):
    from dataclasses import replace
    prepared = replay.prepare_public_inputs(bundle, tick_size=.1)
    replay.simulate_prepared_inputs(prepared, parameters())
    key = next(iter(prepared._clocks))
    ts = prepared._clocks[key][0].transact_time.to_numpy()
    bbo, l2 = prepared.inputs['bbo'], prepared.inputs['l2']
    params = dict(require_historical_bbo=True, min_historical_book_coverage=.01,
                  max_exec_book_age_s=.001)
    # Both paths must make the same decision, including exact error wording.
    for threshold in (.01, .5, 1., 1.000000000002):
        params['min_historical_book_coverage'] = threshold
        errors = []
        for kw in ({}, dict(_prepared_owner=prepared, _clock_key=key)):
            try:
                replay._validate_historical_book_coverage(ts, bbo, l2, params, **kw)
                errors.append(None)
            except ValueError as error:
                errors.append(str(error))
        assert errors[0] == errors[1]
    count = len(prepared._book_coverage)
    prepared.historical_book_coverage(key, bbo, l2, 2, replay._historical_book_coverage_values)
    assert len(prepared._book_coverage) == count + 1
    assert prepared.historical_book_coverage(key, replace(bbo), l2, 2,
                                            replay._historical_book_coverage_values) is None
    other = replay.prepare_public_inputs(bundle, tick_size=.1)
    assert other.historical_book_coverage(key, bbo, l2, 2,
                                         replay._historical_book_coverage_values) is None
    replay.simulate_prepared_inputs(prepared, parameters(), execution_end_ns=2_500_000_000)
    assert len(prepared._clocks) == 2
    assert len({entry[0] for entry in prepared._book_coverage}) == 2


def test_mutable_external_coverage_not_cached(monkeypatch):
    from types import SimpleNamespace
    ts = np.array([0, 1000, 2000, 3000])
    book = SimpleNamespace(ts_ms=ts.copy())
    params = dict(require_historical_bbo=True, min_historical_book_coverage=.9,
                  max_exec_book_age_s=.1)
    replay._validate_historical_book_coverage(ts, book, None, params)
    book.ts_ms[1:3] = 0  # Same length and endpoints, but missing interior coverage.
    with pytest.raises(ValueError, match='coverage too low'):
        replay._validate_historical_book_coverage(ts, book, None, params)
    book.ts_ms = np.array([0, 1000, 3000])
    ts = np.array([0, 1000, 1000, 3000])
    replay._validate_historical_book_coverage(ts, book, None, params)
    ts[2] = 2000  # Mutable event axis, also unchanged length and endpoints.
    with pytest.raises(ValueError, match='coverage too low'):
        replay._validate_historical_book_coverage(ts, book, None, params)


def test_coverage_failure_precedes_consume_transfer(bundle, monkeypatch):
    prepared = replay.prepare_public_inputs(bundle, tick_size=.1)
    cp = replay.simulate_prepared_inputs(prepared, parameters(),
        checkpoint_at_ts_ms=1800)['_replay_checkpoint']
    runtime = cp['runtime']
    # A cached low value must still fail admission before transfer.
    for key in prepared._book_coverage:
        prepared._book_coverage[key] = (0., 0.)
    with pytest.raises(ValueError, match='coverage too low'):
        replay.simulate_prepared_inputs(prepared, parameters(), resume_checkpoint=cp,
                                       consume_resume_checkpoint=True)
    assert cp['runtime'] is runtime and not cp.get('consumed')


@pytest.mark.parametrize('freshness', [float('nan'), float('inf'), -1., 0.])
def test_coverage_special_freshness_same_rejection(bundle, freshness):
    prepared = replay.prepare_public_inputs(bundle, tick_size=.1)
    replay.simulate_prepared_inputs(prepared, parameters())
    key = next(iter(prepared._clocks))
    ts = prepared._clocks[key][0].transact_time.to_numpy()
    params = dict(require_historical_bbo=True, min_historical_book_coverage=.9,
                  max_exec_book_age_s=freshness)
    failures = []
    for kw in ({}, dict(_prepared_owner=prepared, _clock_key=key)):
        try:
            replay._validate_historical_book_coverage(ts, prepared.inputs['bbo'],
                                                     prepared.inputs['l2'], params, **kw)
            failures.append(None)
        except (ValueError, OverflowError) as error:
            failures.append((type(error), str(error)))
    assert failures[0] == failures[1]


def test_coverage_disabled_and_tolerance_preserved(bundle, monkeypatch):
    prepared = replay.prepare_public_inputs(bundle, tick_size=.1)
    replay.simulate_prepared_inputs(prepared, parameters())
    key = next(iter(prepared._clocks))
    ts = prepared._clocks[key][0].transact_time.to_numpy()
    bbo, l2 = prepared.inputs['bbo'], prepared.inputs['l2']
    prepared._book_coverage[(key, True, True, 1000)] = (.5, .5)
    kw = dict(_prepared_owner=prepared, _clock_key=key)
    for params in (dict(require_historical_bbo=False),
                   dict(require_historical_bbo=True, min_historical_book_coverage=0)):
        replay._validate_historical_book_coverage(ts, bbo, l2, params, **kw)
    params = dict(require_historical_bbo=True, max_exec_book_age_s=1.,
                  min_historical_book_coverage=.5 + 0.5e-12)
    replay._validate_historical_book_coverage(ts, bbo, l2, params, **kw)
    with pytest.raises(ValueError, match='coverage too low'):
        replay._validate_historical_book_coverage(ts, bbo, l2,
            dict(params, min_historical_book_coverage=.5 + 2e-12), **kw)


def test_prepared_explicit_consume_matches_default_and_persists(bundle, tmp_path, monkeypatch):
    from models.replay import runtime_checkpoint_io as cio
    from test_tick_runtime_checkpoint import assert_same
    prepared = replay.prepare_public_inputs(bundle, tick_size=.1)
    params = parameters()
    expected = replay.simulate_prepared_inputs(prepared, params)
    cp = replay.simulate_prepared_inputs(prepared, params,
                                        checkpoint_at_ts_ms=1800)['_replay_checkpoint']
    path = tmp_path/'pause.checkpoint'
    cio.save_runtime_checkpoint(path, cp)
    restored = cio.load_trusted_runtime_checkpoint(path)
    def forbidden(*args, **kwargs):
        raise AssertionError('explicit single-successor must not clone')
    with monkeypatch.context() as context:
        context.setattr(cio, 'clone_runtime_state', forbidden)
        successor = replay.simulate_prepared_inputs(prepared, params,
            resume_checkpoint=restored, checkpoint_at_ts_ms=2600,
            consume_resume_checkpoint=True)['_replay_checkpoint']
        assert restored['consumed'] and 'runtime' not in restored
        actual = replay.simulate_prepared_inputs(prepared, params,
            resume_checkpoint=successor, consume_resume_checkpoint=True)
    assert_same(actual, expected)
    assert_same(replay.simulate_prepared_inputs(prepared, params, resume_checkpoint=cp), expected)
    assert_same(replay.simulate_prepared_inputs(prepared, params, resume_checkpoint=cp), expected)
    assert 'runtime' in cp and not cp.get('consumed')


def test_prepared_rejected_binding_keeps_owned_checkpoint(bundle):
    prepared = replay.prepare_public_inputs(bundle, tick_size=.1)
    params = parameters()
    cp = replay.simulate_prepared_inputs(prepared, params,
                                        checkpoint_at_ts_ms=1800)['_replay_checkpoint']
    runtime = cp['runtime']
    with pytest.raises(ValueError, match='public checkpoint'):
        replay.simulate_prepared_inputs(prepared, dict(params, maker_fee=.001),
            resume_checkpoint=cp, consume_resume_checkpoint=True)
    assert cp['runtime'] is runtime and not cp.get('consumed')


def test_exact_numeric_seek_counts_and_boundaries(bundle, tmp_path, monkeypatch):
    from models.replay import public_tape_cache as cache
    from models.replay.public_input import PublicExchangeBookTape
    from data.runtime import ConsumerBundle
    tape = cache.cached_public_tape(PublicExchangeBookTape(ConsumerBundle(bundle), tick_size=.1),
        tmp_path, manifest_id="fixture", chunk_events=2)
    expected = list(tape)
    original = cache.HistoricalExchangeBookEvent
    calls = []
    def counted(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)
    monkeypatch.setattr(cache, "HistoricalExchangeBookEvent", counted)
    for offset in range(len(expected) + 1):
        calls.clear()
        assert list(tape.iter_from(offset)) == expected[offset:]
        assert len(calls) == len(expected) - offset
    for offset in (-1, len(expected)+1, True):
        with pytest.raises(ValueError, match="cursor"):
            list(tape.iter_from(offset))
    with (tape.root / "events-0.npy").open("ab") as stream:
        stream.write(b"changed")
    with pytest.raises(ValueError, match="length mismatch"):
        list(tape.iter_from(0))


def test_empty_valuation_and_reader_identity(bundle, monkeypatch):
    from data.runtime import ConsumerBundle
    from models.replay.public_accounting import terminal_valuations
    reader = ConsumerBundle(bundle)
    original = ConsumerBundle.batches
    def forbidden(*args, **kwargs):
        raise AssertionError("empty endpoints read depth")
    monkeypatch.setattr(ConsumerBundle, "batches", forbidden)
    assert terminal_valuations(reader, [], max_mark_age_ns=10**9) == {}
    monkeypatch.setattr(ConsumerBundle, "batches", original)
    ends = [reader.manifest["plan"]["end_ns"]]
    assert terminal_valuations(reader, ends, max_mark_age_ns=10**9) == terminal_valuations(
        bundle, ends, max_mark_age_ns=10**9)
    depth = reader.root / reader.manifest["files"]["depth"]["file"]
    with depth.open("ab") as stream:
        stream.write(b"changed")
    with pytest.raises(ValueError, match="checksum"):
        terminal_valuations(reader, ends, max_mark_age_ns=10**9)


def numeric_fixture(tmp_path):
    from types import SimpleNamespace
    from models.replay.public_tape_cache import cached_public_tape
    from models.tick_data_types import HistoricalExchangeBookEvent

    class Tape(list):
        tick_size = .1
        day_start_ns = 1
        bundle = SimpleNamespace(manifest={"plan": {"market_id": "synthetic"}})

    def event(kind, ts, ordinal, levels):
        return HistoricalExchangeBookEvent(
            "synthetic", kind, ts, levels=levels, source="synthetic-source",
            source_ordinal=ordinal, sequence_scope="provider_ordered")

    rows = Tape([
        event("snapshot", 1000000001, 40, (("bid", 100, 2.), ("ask", 102, 3.))),
        event("delta", 1000000002, 41, (("ask", 102, 0.), ("bid", 99, .125))),
        event("delta", 1000000002, 42, (("bid", 100, 1.),)),
        event("delta", 1000000002, 43, (("bid", 100, 1.),)),
        event("source_gap", 1000000003, 44, ()),
        event("snapshot", 1000000004, 45, (("ask", 103, 1.25), ("bid", 101, 2.5))),
    ])
    return rows, cached_public_tape(rows, tmp_path, manifest_id="synthetic", chunk_events=2)


def test_numeric_columns_all_fields_and_scheduler_boundaries(tmp_path):
    from models.exchange_book_replay import HistoricalExchangeBookScheduler

    rows, tape = numeric_fixture(tmp_path)
    for offset in range(len(rows) + 1):
        assert [asdict(e) for e in tape.iter_from(offset)] == [asdict(e) for e in rows[offset:]]
    # Independent cursors retain duplicate contents with distinct source ordinals.
    left, right = tape.iter_from(2), tape.iter_from(2)
    assert next(left) == next(right) == rows[2]
    assert next(left) == rows[3]
    assert next(right) == rows[3]
    expected = HistoricalExchangeBookScheduler(rows, strict_sequence=False)
    actual = HistoricalExchangeBookScheduler(tape, strict_sequence=False)
    for boundary in (1000000000, 1000000001, 1000000002, 1000000003, 1000000004, 1000000005):
        expected._ensure_lookahead_past(boundary)
        actual._ensure_lookahead_past(boundary)
        assert actual.advance_to(boundary) == expected.advance_to(boundary)
        assert vars(actual.book) == vars(expected.book)
        assert {k: v for k, v in vars(actual.sequence).items() if k != "book"} == {
            k: v for k, v in vars(expected.sequence).items() if k != "book"}
        for name in ("known_ticks", "snapshot_ranges", "_next_event", "_lookahead",
                     "_source_read_count", "_consumed", "_accepted", "_rejected"):
            assert getattr(actual, name) == getattr(expected, name)
        actual = HistoricalExchangeBookScheduler.from_checkpoint(
            actual.checkpoint(), tape.iter_from(actual.source_read_count))


@pytest.mark.parametrize("field,value", [
    ("ts", 0), ("tick", 0), ("quantity", -1.), ("quantity", float("nan")),
    ("quantity", float("inf")), ("kind", "invalid"),
])
def test_numeric_columns_preserve_constructor_rejections(tmp_path, field, value):
    from models.tick_data_types import HistoricalExchangeBookEvent

    _, tape = numeric_fixture(tmp_path)
    kwargs = dict(market_id="synthetic", event_type="snapshot", exchange_ts_ns=1000000001,
                  levels=(("bid", 100, 2.), ("ask", 102, 3.)), source="synthetic-source",
                  source_ordinal=40, sequence_scope="provider_ordered")
    if field == "kind":
        tape.manifest["kinds"][0] = value
        kwargs["event_type"] = value
    else:
        chunk = tape.manifest["chunks"][0]
        name = chunk["events" if field == "ts" else "levels"]
        path = tape.root / name
        data = np.load(path, allow_pickle=False)
        data[field][0] = value
        np.save(path, data, allow_pickle=False)
        assert path.stat().st_size == chunk["sizes"][name]
        if field == "ts":
            kwargs["exchange_ts_ns"] = value
        else:
            kwargs["levels"] = (("bid", value if field == "tick" else 100,
                                 value if field == "quantity" else 2.), ("ask", 102, 3.))
    with pytest.raises(ValueError) as original:
        HistoricalExchangeBookEvent(**kwargs)
    with pytest.raises(ValueError) as decoded:
        next(iter(tape))
    assert str(decoded.value) == str(original.value)


def test_prepared_clock_constructed_once_and_isolated(bundle, monkeypatch):
    prepared = replay.prepare_public_inputs(bundle, tick_size=.1)
    original = replay.build_replay_event_clock
    calls = []
    def counted(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)
    monkeypatch.setattr(replay, "build_replay_event_clock", counted)
    first = replay.simulate_prepared_inputs(prepared, parameters())
    second = replay.simulate_prepared_inputs(prepared, parameters())
    assert first == second
    assert len(calls) == 1
    replay.simulate_prepared_inputs(prepared, parameters(), execution_end_ns=2_500_000_000)
    assert len(calls) == 2


def test_account_snapshot_hit_never_hashes_frame_and_copies_are_isolated(bundle, monkeypatch):
    import pandas as pd
    from models.replay import public_input
    prepared = replay.prepare_public_inputs(bundle, tick_size=.1)
    def forbidden(*args, **kwargs):
        raise AssertionError('internal clock hit scanned the entire frame')
    monkeypatch.setattr(pd.util, 'hash_pandas_object', forbidden)
    first = replay.simulate_prepared_inputs(prepared, parameters())
    accounts = tuple(prepared._accounts.values())
    books = dict(prepared._book_inputs)
    monkeypatch.setattr(public_input, 'book_observation_times_us', forbidden)
    second = replay.simulate_prepared_inputs(prepared, parameters())
    assert first == second
    assert tuple(prepared._accounts.values()) == accounts
    assert all(prepared._book_inputs[key] is value for key, value in books.items())
    account, = accounts
    copy = account.copy()
    copy.loc[:, 'price'] = 0
    assert (account.copy().price != 0).all()
    with pytest.raises(ValueError):
        prepared.inputs['bbo'].best_bid.flags.writeable = True
    with pytest.raises(ValueError):
        prepared.execution_inputs(next(iter(prepared._clocks)))[1].flags.writeable = True
    assert not any(key[1] for key in prepared._execution_inputs)  # disabled cumulative branch


def test_external_frame_clock_mutation_and_account_owner_rejected(bundle):
    import pandas as pd
    prepared = replay.prepare_public_inputs(bundle, tick_size=.1)
    other = replay.prepare_public_inputs(bundle, tick_size=.1)
    frame = prepared.inputs['trades'].copy()
    calls = []
    def builder(frame, **kwargs):
        calls.append(1)
        return frame.copy(), len(frame)
    first, _ = prepared.event_clock(frame, builder, initial_clock_price=1.)
    frame.loc[:, 'price'] += 1
    second, _ = prepared.event_clock(frame, builder, initial_clock_price=1.)
    assert len(calls) == 2
    assert not first.equals(second)
    prepared.event_clock(frame, builder, initial_clock_price=2.)
    assert len(calls) == 3
    a = prepared.account_trades(1200, None, quantity_rule='all_public_volume_eligible')
    expected = prepared.inputs['trades'].loc[lambda x: x.transact_time >= 1200].copy()
    expected['normal_quantity'] = expected.quantity
    pd.testing.assert_frame_equal(a.copy(), expected)
    b = prepared.account_trades(1201, None, quantity_rule='all_public_volume_eligible')
    assert b is not a
    with pytest.raises(ValueError, match='another prepared owner'):
        other.event_clock(a.copy(), builder, _account=a)
    with pytest.raises(ValueError, match='eligibility'):
        prepared.account_trades(1200, None, quantity_rule='changed')


def test_cached_cumulative_inputs_equal_clock_formula(bundle):
    import numpy as np
    prepared = replay.prepare_public_inputs(bundle, tick_size=.1)
    replay.simulate_prepared_inputs(prepared, parameters())
    key = next(iter(prepared._clocks))
    frame, _ = prepared._clocks[key]
    eligible = frame['_is_execution_trade'].to_numpy(dtype=bool)
    seller = frame.is_buyer_maker.to_numpy(dtype=np.uint8) == 1
    qty = np.where(eligible, frame.quantity.to_numpy(dtype=float), 0.)
    actual = prepared.execution_inputs(key, cumulative=True)
    expected = (qty, np.r_[0., np.cumsum(np.where(seller, 0., qty))],
                np.r_[0., np.cumsum(np.where(seller, qty, 0.))],
                np.maximum.accumulate(np.where(eligible, np.arange(len(frame)), -1)))
    for a, b in zip(actual, expected, strict=True):
        np.testing.assert_array_equal(a, b)
    assert prepared.execution_inputs(key, cumulative=True) is actual


def test_exact_seek_restores_scheduler_prefetch(bundle, tmp_path, monkeypatch):
    from models.replay.public_input import PreparedReplayInputs
    from research.families.f05_fill_quality_quote_ev.public_input import iter_prefill_parent_segments
    from strategy.risk_selection import PREFILL_FEATURE_CONTRACT
    from test_research_public_inputs import public_replay_params
    from test_tick_runtime_checkpoint import assert_same
    params = dict(public_replay_params(), risk_selection_collect_opportunities=True,
                  risk_selection_scope='visible_inventory',
                  risk_selection_feature_contract=PREFILL_FEATURE_CONTRACT)
    uncached = replay.prepare_public_inputs(bundle, tick_size=.1)
    cached = replay.prepare_public_inputs(bundle, tick_size=.1, cache_dir=tmp_path/'numeric')
    with monkeypatch.context() as context:
        context.setattr(PreparedReplayInputs, 'immutable_clone_inputs', lambda *args: ())
        baseline = list(iter_prefill_parent_segments(uncached, params,
                        cut_times_ms=[1200, 1800, 2600, 3200]))
    actual = list(iter_prefill_parent_segments(cached, params,
                    cut_times_ms=[1200, 1800, 2600, 3200]))
    assert_same(actual[-1]['result'], baseline[-1]['result'])
    for left, right in zip(baseline, actual, strict=True):
        assert left['opportunities'] == right['opportunities']
        if left['next_checkpoint'] is not None:
            a = left['next_checkpoint']['runtime'].exchange_book_scheduler
            b = right['next_checkpoint']['runtime'].exchange_book_scheduler
            assert a.source_read_count == b.source_read_count
            assert a._next_event == b._next_event
            assert list(a._lookahead) == list(b._lookahead)


@pytest.mark.parametrize("tick,quantity", [(0, 1.0), (100, -1.0), (100, np.nan), (100, np.inf)])
def test_invalid_book_level_reports_event_and_value(tick, quantity):
    from models.tick_data_types import HistoricalExchangeBookEvent

    with pytest.raises(ValueError, match="finite non-negative quantities") as exc:
        HistoricalExchangeBookEvent(
            "market", "delta", 123456789,
            levels=(("bid", 101, 2.0), ("ask", tick, quantity)),
            source="fixture.parquet", source_ordinal=42,
        )
    message = str(exc.value)
    for expected in ("exchange_ts_ns=123456789", "source='fixture.parquet'",
                     "source_ordinal=42", "level_index=1", "side='ask'",
                     f"tick={tick!r}", f"quantity={quantity!r}"):
        assert expected in message


def parameters():
    return dict(
        inventory_reference_qty=1.0,
        eta_inventory=0.01, a_spread=0.01, risk_per_order=0.01,
        execution_intensity_slope=1.0, risk_horizon_s=1.0,
        trade_intensity_acceleration_spread_mult=2.0,
        order_size=0.001,
        max_inventory=0.01,
        requote_interval=1.0,
        rq_min=1.0,
        rq_max=1.0,
        requote_clock="fixed",
        maker_fee=0.0,
        taker_fee=0.0,
        tick_size=0.1,
        lot_size=0.001,
        queue_base=0.0,
        queue_decay=0.0,
        maker_fill_prob=1.0,
        use_bar_pricing=True,
        replay_event_clock="merged",
        replay_clock_interval_ms=100,
        exchange_book_queue_mode="diagnostic",
        public_fill_volume_policy="all_public_volume_eligible",
        max_exec_book_age_s=1.0,
        collect_curves=False,
        position_timeout=0.0,
        markout_ema_span_fills=0,
        account_start_ns=1_200_000_000,
    )


def test_prepared_a_b_a_and_readonly(bundle):
    p = parameters()
    prepared = replay.prepare_public_inputs(bundle, tick_size=0.1)
    first = replay.simulate_prepared_inputs(prepared, p)
    replay.simulate_prepared_inputs(prepared, {**p, "eta_inventory": 0.02,
                                             "a_spread": 0.02, "risk_per_order": 0.02})
    repeat = replay.simulate_prepared_inputs(prepared, p)
    assert first == repeat == replay.simulate_public_inputs(bundle, p)
    with pytest.raises(ValueError):
        prepared.inputs["l2"].bid_px[0, 0] = 0
    with pytest.raises(ValueError, match="tick contract"):
        replay.simulate_prepared_inputs(prepared, {**p, "tick_size": 1.0})


def test_prepared_cold_boundary_collects_actual_recursive_helpers(bundle):
    import gc
    import sys
    import weakref

    prepared = replay.prepare_public_inputs(bundle, tick_size=0.1)
    edges = (('_resume_maker_close', '_attempt_async_emergency_close'),
             ('_attempt_async_emergency_close', '_requote_circuit_breaker_close'),
             ('_requote_circuit_breaker_close', '_resume_maker_close'))
    functions, arrays, verified = [], [], []

    def profile(frame, event, arg):
        if event != 'return' or frame.f_code is not replay.simulate_tick.__code__:
            return
        local = frame.f_locals
        for source, target in edges:
            fn = local[source]
            cells = dict(zip(fn.__code__.co_freevars, fn.__closure__, strict=True))
            verified.append(cells[target].cell_contents is local[target])
            verified.append(cells['_tick_state'].cell_contents is local['_tick_state'])
            functions.append(weakref.ref(fn))

    previous = sys.getprofile()
    enabled = gc.isenabled()
    gc.collect()
    gc.disable()  # The maintained cold boundary, not automatic GC, must release it.
    try:
        sys.setprofile(profile)
        for _ in range(5):
            result = replay.simulate_prepared_inputs(
                prepared, parameters(), checkpoint_at_ts_ms=1800)
            # Event columns now belong to the prepared owner. Path sampling
            # storage must still die with the result, as must the closures.
            arrays.append(weakref.ref(result['_replay_checkpoint']['runtime'].pnl_arr))
            assert all(ref() is None for ref in functions)
            del result
            assert all(ref() is None for ref in arrays)
        assert len(verified) == 30 and all(verified)
    finally:
        sys.setprofile(previous)
        if enabled:
            gc.enable()


def test_numeric_tape_cursor_cache_and_corruption(bundle, tmp_path):
    uncached = replay.prepare_public_inputs(bundle, tick_size=0.1)
    cached = replay.prepare_public_inputs(bundle, tick_size=0.1, cache_dir=tmp_path / "cache")
    tape = cached.inputs["exchange_book_event_tape"]
    expected = [asdict(e) for e in uncached.inputs["exchange_book_event_tape"]]
    assert [asdict(e) for e in tape] == expected == [asdict(e) for e in tape]
    assert replay.simulate_prepared_inputs(cached, parameters()) == replay.simulate_prepared_inputs(
        uncached, parameters()
    )
    array = np.load(tape.root / "events-0.npy", mmap_mode="r", allow_pickle=False)
    assert not array.flags.writeable
    with (tape.root / "events-0.npy").open("ab") as f:
        f.write(b"corrupt")
    rebuilt = replay.prepare_public_inputs(bundle, tick_size=0.1, cache_dir=tmp_path / "cache")
    assert [asdict(e) for e in rebuilt.inputs["exchange_book_event_tape"]] == expected


def test_required_cache_never_rebuilds_and_ignores_runtime_lock(bundle, tmp_path, monkeypatch):
    cache = tmp_path / "cache"
    replay.prepare_public_inputs(bundle, tick_size=.1, cache_dir=cache)
    destination = next(p for p in cache.iterdir() if p.is_dir())
    manifest = destination / "manifest.json"
    meta = json.loads(manifest.read_text())
    meta["files"]["tape/" + "a" * 64 + ".lock"] = 0
    manifest.write_text(json.dumps(meta))
    def forbidden(*args, **kwargs):
        raise AssertionError("read-only reuse cannot decode")
    monkeypatch.setattr(replay, "load_public_inputs", forbidden)
    replay.prepare_public_inputs(bundle, tick_size=.1, cache_dir=cache, require_existing_cache=True)
    with pytest.raises(FileNotFoundError):
        replay.prepare_public_inputs(bundle, tick_size=.1, cache_dir=tmp_path / "missing",
                                     require_existing_cache=True)
    assert not (tmp_path / "missing").exists()
    damaged = destination / next(iter(meta["arrays"].values()))
    damaged.write_bytes(b"invalid")
    with pytest.raises(ValueError):
        replay.prepare_public_inputs(bundle, tick_size=.1, cache_dir=cache, require_existing_cache=True)
    assert damaged.read_bytes() == b"invalid"


def test_f01_prepares_once(bundle, monkeypatch):
    from research.families.f01_fixed_parameter_racing.public_input import (
        replay_parameter_candidates,
    )

    original = replay.load_public_inputs
    calls = []

    def load(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)

    monkeypatch.setattr(replay, "load_public_inputs", load)
    results = replay_parameter_candidates(
        bundle, {"a": {"eta_inventory": 0.01}, "b": {"eta_inventory": 0.02}}, common_params=parameters()
    )
    assert len(calls) == 1 and len(results) == 2


def test_f01_ml_candidates_load_independent_engines_and_match_direct_b0(bundle, monkeypatch, tmp_path):
    from types import SimpleNamespace
    from research.families.f01_fixed_parameter_racing.public_input import (
        replay_parameter_candidates,
    )
    from strategy.signal import SignalEngine

    created = []

    class Engine:
        def __init__(self):
            self.calls = 0

        def compute_feature_frames(self, frames):
            self.calls += 1
            return [SimpleNamespace(touch_conditioned_up_probability_10000ms=0.5, absolute_price_variance_rate_10000ms=0.0, touch_conditioned_price_change_fraction_10000ms=0.0,
                                    touch_side_adverse_probability_bid_10000ms=0.5, touch_side_adverse_probability_ask_10000ms=0.5)
                    for _ in frames]

    def load(cls, model_dir, *, symbol, ret_demean_halflife):
        assert model_dir == tmp_path / "frozen" and symbol == "BTCUSDC"
        assert ret_demean_halflife == 0
        engine = Engine()
        created.append(engine)
        return engine

    monkeypatch.setattr(SignalEngine, "from_public_models", classmethod(load))
    params = {**parameters(), "ml_enabled": True, "vol_blend": 0.5, "asym_strength": 0.,
              "ret_demean_halflife": 0}
    with pytest.raises(ValueError, match="explicit frozen model_dir"):
        replay_parameter_candidates(bundle, {"b0": {"eta_inventory": params["eta_inventory"]}},
                                    common_params=params)
    with pytest.raises(ValueError, match="loaded frozen P3 identity"):
        replay_parameter_candidates(bundle, {"b0": {"eta_inventory": params["eta_inventory"]}},
                                    common_params=params, model_dir=tmp_path / "frozen")
    params.update(touch_probability_calibrated=True, p3_identity_required=True,
                  p3_distance_touch_product_argmax=1.0, p3_touch_log_probability_distance_slope=0.05,
                  touch_probability_event_type="touch", touch_probability_horizon_s=10.0,
                  touch_probability_distance_origin="same_side_best_bid_or_ask_at_window_start",
                  touch_probability_distance_unit="USDC_per_BTC",
                  touch_probability_side="pooled_buy_sell",
                  touch_probability_queue_included=False,
                  touch_probability_artifact_sha256="a" * 64)
    arms = replay_parameter_candidates(
        bundle, {"b0": {"eta_inventory": params["eta_inventory"], "asym_strength": 0.},
                 "candidate": {"eta_inventory": params["eta_inventory"] * 1.2, "asym_strength": .1}},
        common_params=params, model_dir=tmp_path / "frozen",
    )
    assert len(created) == 2 and created[0] is not created[1]
    assert [engine.calls for engine in created] == [1, 1]
    direct = replay.simulate_public_inputs(bundle, params, signal_engine=Engine())
    assert arms["b0"] == direct


@pytest.mark.parametrize("change, mask, match", [
    ({"gamma": 0.02}, {"a_spread": 0.01}, "only declared quote parameters"),
    ({"gamma": 0.02}, {"quote_math_mode": "quantity_aware_v1"}, "only declared quote parameters"),
    ({"kappa": 2.0}, {"execution_intensity_slope": 1.0}, "only declared quote parameters"),
    ({"kappa": 2.0}, {"p3_touch_log_probability_distance_slope": 3.0}, "only declared quote parameters"),
    ({"max_spread_bps": 25.0}, {"dynamic_cap_enabled": True,
                                  "dynamic_cap_base_bps": 20.0}, "max_spread_bps is masked"),
])
def test_f01_rejects_masked_parameter_aliases(bundle, change, mask, match):
    from research.families.f01_fixed_parameter_racing.public_input import (
        replay_parameter_candidates,
    )
    params = {**parameters(), "max_spread_bps": 20.0, **mask}
    with pytest.raises(ValueError, match=match):
        replay_parameter_candidates(bundle, {"candidate": change}, common_params=params)


def test_f01_explicit_parameters_reach_effective_quote_coefficients():
    from research.families.f01_fixed_parameter_racing.public_input import (
        _validate_effective_quote_change,
    )
    from strategy.quote_core import quote_core_config_from_params

    base = {**parameters(), "max_spread_bps": 20.0,
            "dynamic_cap_enabled": True}

    def effective(params):
        return quote_core_config_from_params(
            params, tick_size=params["tick_size"], lot_size=params["lot_size"],
            use_ml=True, use_depth_weighted_mid_proxy=False, use_depth_liquidity_scaling=False,
        )

    b0 = effective(base)
    for change, fields in (
        ({"eta_inventory": 0.02, "a_spread": 0.02, "risk_per_order": 0.02},
         ("eta_inventory", "a_spread", "risk_per_order")),
        ({"execution_intensity_slope": 2.0}, ("execution_intensity_slope",)),
        ({"max_spread_bps": 25.0}, ("max_spread_bps", "dynamic_cap_base_bps")),
    ):
        _validate_effective_quote_change(base, change)
        candidate = effective({**base, **change})
        assert all(getattr(candidate, field) != getattr(b0, field) for field in fields)


def test_batched_prediction_preserves_all_heads_and_ema(bundle):
    from data.runtime import ConsumerBundle
    from strategy.signal import SignalEngine, REQUIRED_MODEL_HEADS

    frames = tuple(ConsumerBundle(bundle).frames())

    class Model:
        def __init__(self, offset):
            self.offset = offset
            self.calls = 0

        def predict(self, matrix, **kwargs):
            self.calls += 1
            return np.nan_to_num(matrix).sum(axis=1) * 0.001 + self.offset

    def engine():
        value = SignalEngine(enable_ml=False, ret_demean_halflife=3)
        names = [name for name, _ in frames[0].values]
        value._model_metadata = {
            head: dict(
                input_contract_id=frames[0].input_contract_id,
                observation_contract_id=frames[0].observation_contract_id,
                feature_contract_id=frames[0].feature_contract_id,
                feature_cols=names,
                missing_policy="native_nan",
            )
            for head in REQUIRED_MODEL_HEADS
        }
        value._model_feature_cols = {head: names for head in REQUIRED_MODEL_HEADS}
        value._model_feature_schema = tuple(names)
        value._models = {head: Model(i - 7.0) for i, head in enumerate(REQUIRED_MODEL_HEADS)}
        value._enable_ml = True
        return value

    one, batch = engine(), engine()
    expected = [one.compute_signal(feature_frame=f, decision_ns=f.cutoff_ns) for f in frames]
    actual = batch.compute_feature_frames(frames, batch_size=2)
    for left, right in zip(expected, actual, strict=True):
        for head in REQUIRED_MODEL_HEADS:
            assert getattr(left, head) == getattr(right, head)
        assert left.ts == right.ts
        np.testing.assert_array_equal(left.features, right.features)
    np.testing.assert_array_equal(one._pred_ret_ema, batch._pred_ret_ema)
    assert all(m.calls == (len(frames) + 1) // 2 for m in batch._models.values())
