import numpy as np
import pytest

from tests.test_optional_update_branch import synthetic_parent  # noqa: F401
from tests.test_target_observation import exact
from models.backtest_tick import simulate_prepared_inputs, XMARKET_REPLAY_FEATURE_COLUMNS
from models.replay.public_input import PreparedPublicPredictions, load_public_replay_inputs
from models.replay.public_input_window import build_global_timelines, GlobalPublicTimelines
from strategy.signal import SignalEngine


def test_rolling_trade_windows_decode_prefix_once_and_keep_boundaries(long_parent, monkeypatch):
    from models.replay.public_input_window import RollingTradeWindow
    from data.runtime import PublicInputStream
    import pandas as pd

    bundle = long_parent[0].inputs['bundle']
    original = PublicInputStream._channel
    counts = dict(streams=0, events=0)

    def counted(self, channel):
        counts['streams'] += 1
        for item in original(self, channel):
            counts['events'] += 1
            yield item

    intervals = [(1, 40), (35, 70), (65, 110), (105, 151)]
    expected = [load_public_replay_inputs(bundle, tick_size=.1,
                window_ns=(a*10**9, b*10**9))['trades'] for a,b in intervals]
    monkeypatch.setattr(PublicInputStream, '_channel', counted)
    cache = RollingTradeWindow(bundle)
    for (a,b), frame in zip(intervals, expected, strict=True):
        actual = load_public_replay_inputs(bundle, tick_size=.1,
            window_ns=(a*10**9,b*10**9),trade_window=cache)['trades']
        pd.testing.assert_frame_equal(frame, actual, check_exact=True)
        assert all(a*10**9 <= row[0] < b*10**9 for row in cache._rows)
    assert counts == dict(streams=1, events=150)
    # A rewind/fresh owner reconstructs, rather than inventing, scheduler state.
    actual = load_public_replay_inputs(bundle,tick_size=.1,
        window_ns=(10**9,40*10**9),trade_window=cache)['trades']
    pd.testing.assert_frame_equal(actual,expected[0],check_exact=True)
    assert counts['streams']==2


def test_rolling_trade_window_preserves_regressions_ties_and_empty_windows(monkeypatch):
    from types import SimpleNamespace
    from models.replay.public_input_window import RollingTradeWindow
    from data.runtime import PublicInputStream, ObservationProfile
    from dataclasses import dataclass
    @dataclass(frozen=True)
    class Trade:
        market_id: str = 'test:BTCUSDC'
        exchange_ts_ns: int | None = None
        source_timestamp_us: int = 0
        source_ordinal: int = 0
    profile=ObservationProfile('test','source_timestamp_proxy',0,0,0,0)
    events=[Trade(exchange_ts_ns=t) for t in (1,3,2,3,8)]
    # Exercise the real scheduling high-water mark through a fact-free stream.
    import data.runtime as runtime
    monkeypatch.setattr(runtime,'read_facts',lambda *a,**k:iter(events))
    import json
    monkeypatch.setattr(runtime.Path,'read_text',lambda *a,**k:json.dumps({'files':[]}))
    bundle=SimpleNamespace(manifest={'plan':{'start_ns':0,'end_ns':10}},
        source_paths=lambda:[], stream=lambda:PublicInputStream(['unused'],profile=profile,
            start_ns=0,end_ns=10,market_id='test:BTCUSDC',input_contract_id='test'))
    expected=list(bundle.stream()._channel('trades'))
    cache=RollingTradeWindow(bundle)
    for a,b in [(0,3),(3,4),(4,7),(7,10),(0,10)]:
        assert list(cache.events(a,b))==[r for r in expected if a<=r[0]<b]
    assert [t for t,_ in expected]==[1,3,3,3,8]


@pytest.fixture
def long_parent(tmp_path, request):
    from dataclasses import asdict
    from data.facts import materialize
    from data.runtime import ObservationProfile, derive_inputs
    from models.backtest_tick import prepare_public_inputs
    from tests.test_research_public_inputs import public_replay_params, MARKET
    book, trade = tmp_path/'book.csv', tmp_path/'trade.csv'
    books = ['exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount']
    trades = ['exchange,symbol,timestamp,local_timestamp,id,side,price,amount']
    for second in range(1, 151):
        # One past extreme distinguishes a full 120s rank from a truncated
        # 60s rank just after rotation. Other tests retain the original tape.
        bump = 30 if getattr(request, 'param', False) and second==5 else 0
        for side, price in [('bid', 100+second%5), ('ask', 102+second%5)]:
            price += bump
            books.append(f'binance-futures,BTCUSDC,{second*1000000},{second*1000000},true,{side},{price},1')
        trades.append(f'binance-futures,BTCUSDC,{second*1000000+300000},{second*1000000+300000},{second},buy,{102+second%5+bump},0.01')
    book.write_text('\n'.join(books)+'\n')
    trade.write_text('\n'.join(trades)+'\n')
    materialize({'source_profile':'tardis_only', 'files':[
        {'path':str(book),'symbol':'BTCUSDC','channel':'incremental_book_L2'},
        {'path':str(trade),'symbol':'BTCUSDC','channel':'trades'}]},tmp_path/'facts')
    profile = ObservationProfile('synthetic','source_timestamp_proxy',0,0,0,1_000_000_000,
                                 trade_coverage='observed')
    derive_inputs(dict(facts_root=str(tmp_path/'facts'), observation_profile=asdict(profile),
        start_ns=1_000_000_000,end_ns=151_000_000_000,market_id=MARKET,
        include_response_observations=True),tmp_path/'inputs')
    return prepare_public_inputs(tmp_path/'inputs',tick_size=.1), dict(public_replay_params(),
        response_observe_only=True,response_update_branch_enabled=True,
        risk_selection_collect_opportunities=True)


def test_bounded_loader_preserves_global_identity_and_exact_slice(synthetic_parent):  # noqa: F811
    prepared, _ = synthetic_parent
    bundle = prepared.inputs['bundle']
    begin, end = 7_000_000_000, 16_000_000_000
    window = load_public_replay_inputs(bundle, tick_size=.1, window_ns=(begin, end))
    assert window['bundle'] is bundle
    assert window['bundle'].input_manifest_id == prepared.manifest_id
    for key in ('bbo', 'l2'):
        original, actual = prepared.inputs[key], window[key]
        mask = (original.ts_ms >= begin//10**6) & (original.ts_ms < end//10**6)
        for field in original.__dataclass_fields__:
            value = getattr(original, field)
            if isinstance(value, np.ndarray):
                np.testing.assert_array_equal(value[mask], getattr(actual, field))
    assert len(window['frames']) < len(prepared.inputs['frames'])


def test_fact_source_cursor_tail_matches_full_stream(long_parent):
    from dataclasses import replace
    from models.replay.public_input import PublicExchangeBookTape

    prepared, _ = long_parent
    tape = PublicExchangeBookTape(prepared.inputs['bundle'], tick_size=.1)
    events = list(tape)
    for index in (0, 5, len(events)-2, len(events)-1):
        assert list(tape.iter_after_source_cursor(events[index], index+1)) == events[index+1:]
    with pytest.raises(ValueError, match='changed the saved source'):
        list(tape.iter_after_source_cursor(replace(events[5], levels=(('bid', 1000, 123.0),)), 6))
    with pytest.raises(ValueError, match='source cursor'):
        list(tape.iter_after_source_cursor(replace(events[5], source='missing'), 6))


def test_fact_source_cursor_scheduler_resume_matches_full(long_parent):
    from models.replay.public_input import PublicExchangeBookTape
    from models.exchange_book_replay import HistoricalExchangeBookScheduler

    prepared, _ = long_parent
    tape = PublicExchangeBookTape(prepared.inputs['bundle'], tick_size=.1)
    original = HistoricalExchangeBookScheduler(tape, strict_sequence=False)
    original.advance_to(70_000_000_000)
    restored = HistoricalExchangeBookScheduler.from_checkpoint(original.checkpoint(), [])
    restored.resume_input_source(tape)
    assert restored.advance_to(149_000_000_000) == original.advance_to(149_000_000_000)
    assert restored.state_fingerprint() == original.state_fingerprint()
    assert restored.stats() == original.stats()


def test_global_prediction_and_runtime_continuation_matches_full(synthetic_parent, tmp_path):  # noqa: F811
    prepared, params = synthetic_parent
    kwargs = dict(signal_engine=SignalEngine(enable_ml=False),
                  cross_columns=XMARKET_REPLAY_FEATURE_COLUMNS, model_identity='synthetic-test-only', batch_size=3)
    timelines = build_global_timelines(prepared.inputs['bundle'], tmp_path/'timeline', **kwargs)
    full_predictions = PreparedPublicPredictions.create(prepared, SignalEngine(enable_ml=False),
                                                       XMARKET_REPLAY_FEATURE_COLUMNS)
    exact(full_predictions.for_inputs(prepared), timelines.predictions_for(prepared).for_inputs(prepared))
    full = simulate_prepared_inputs(prepared, params, prepared_predictions=full_predictions)
    first, predictions = timelines.prepare(1_000_000_000, 16_000_000_000, tick_size=.1)
    with pytest.raises(ValueError, match='nonterminal'):
        simulate_prepared_inputs(first, params, prepared_predictions=predictions)
    partial = simulate_prepared_inputs(first, params, prepared_predictions=predictions,
                                      checkpoint_at_ts_ms=10000)['_replay_checkpoint']
    last, predictions = timelines.prepare(1_000_000_000, 21_000_000_000, tick_size=.1)
    restored = simulate_prepared_inputs(last, params, prepared_predictions=predictions,
                                       resume_checkpoint=partial)
    # Input-window telemetry is intentionally different; economics and all
    # order/response state must still match the uninterrupted global input.
    full.pop('input_window_count', None)
    restored.pop('input_window_count', None)
    exact(full, restored)
    assert not np.shares_memory(first.inputs['l2'].bid_px, last.inputs['l2'].bid_px)
    with (tmp_path/'timeline'/'prediction-1.npy').open('ab') as target:
        target.write(b'changed')
    with pytest.raises(ValueError, match='payload changed'):
        GlobalPublicTimelines(prepared.inputs['bundle'], tmp_path/'timeline')


@pytest.mark.parametrize('main_loop', [False, True])
def test_rotation_discards_consumed_prefix_without_reset(long_parent, tmp_path, main_loop):
    prepared, params = long_parent
    if main_loop:
        from tests.test_python_planned_maintenance_replay import _async_fifo_params
        params = {**params, **_async_fifo_params(), 'rq_min': .1, 'rq_max': .5}
    timeline = build_global_timelines(prepared.inputs['bundle'],tmp_path/'timeline',
        signal_engine=SignalEngine(enable_ml=False),cross_columns=XMARKET_REPLAY_FEATURE_COLUMNS,
        model_identity='synthetic-only',batch_size=7)
    full_predictions = PreparedPublicPredictions.create(prepared,SignalEngine(enable_ml=False),
                                                       XMARKET_REPLAY_FEATURE_COLUMNS)
    expected = simulate_prepared_inputs(prepared,params,prepared_predictions=full_predictions)
    first, predictions = timeline.prepare(1_000_000_000,120_000_000_000,tick_size=.1)
    checkpoint = simulate_prepared_inputs(first,params,prepared_predictions=predictions,
        checkpoint_at_ts_ms=100000)['_replay_checkpoint']
    from models.replay.runtime_input_window import runtime_input_context_start
    begin_ms = runtime_input_context_start(checkpoint['runtime'], 30000, 60000)
    # A dormant variance consumer can retain the first book row even though
    # the first trade is 300 ms later; sparse trade clocks are not a floor.
    assert begin_ms >= 1000
    if main_loop:
        # The switch is enabled but public inputs never provide var_retsq.
        # Its unused index zero must not pin the first window or change the
        # global diagnostic index when the ordinary variance prefix rotates.
        assert checkpoint['runtime'].dynamic_rq
        assert not checkpoint['runtime'].main_loop_rq_input_active
        assert checkpoint['runtime'].input_price_lookback_ms == 120000
    last, predictions = timeline.prepare(begin_ms*10**6,151_000_000_000,tick_size=.1)
    actual = simulate_prepared_inputs(last,params,prepared_predictions=predictions,
                                     resume_checkpoint=checkpoint)
    expected.pop('input_window_count',None)
    actual.pop('input_window_count',None)
    exact(expected,actual)


def test_separate_feature_history_keeps_exact_rows_and_small_market(long_parent, tmp_path):
    prepared, params = long_parent
    timeline = build_global_timelines(prepared.inputs['bundle'], tmp_path/'timeline',
        signal_engine=SignalEngine(enable_ml=False), cross_columns=XMARKET_REPLAY_FEATURE_COLUMNS,
        model_identity='independent-context-test')
    whole, wp = timeline.prepare(10**9, 151*10**9, tick_size=.1)
    small, sp = timeline.prepare(130*10**9, 151*10**9, tick_size=.1, feature_begin_ns=10**9)
    assert len(small.inputs['l2'].ts_ms) < len(whole.inputs['l2'].ts_ms)/5
    exact((small.variance_ts, small.variance), (whole.variance_ts, whole.variance))
    exact(sp.for_inputs(small), wp.for_inputs(whole))
    with pytest.raises(ValueError, match='feature context'):
        timeline.prepare(130*10**9, 151*10**9, tick_size=.1, feature_begin_ns=140*10**9)


@pytest.mark.parametrize('long_parent', [True], indirect=True)
def test_rank_history_survives_window_rotation_and_serialized_restore(long_parent, tmp_path):
    from models.replay.runtime_input_window import runtime_input_context_start
    from models.replay.runtime_checkpoint_io import save_runtime_checkpoint, load_trusted_runtime_checkpoint
    from tests.test_python_planned_maintenance_replay import _async_fifo_params

    prepared, params = long_parent
    params = {**params, **_async_fifo_params(), 'rq_min': .1, 'rq_max': .5}
    timelines=build_global_timelines(prepared.inputs['bundle'],tmp_path/'timelines',
        signal_engine=SignalEngine(enable_ml=False),cross_columns=XMARKET_REPLAY_FEATURE_COLUMNS,
        model_identity='synthetic-rank-only')
    expected=simulate_prepared_inputs(prepared,params,prepared_predictions=timelines.predictions_for(prepared))
    first,predictions=timelines.prepare(1_000_000_000,120_000_000_000,tick_size=.1)
    checkpoint=simulate_prepared_inputs(first,params,prepared_predictions=predictions,
        checkpoint_at_ts_ms=100000)['_replay_checkpoint']
    path=tmp_path/'latest.checkpoint';save_runtime_checkpoint(path,checkpoint)
    saved=load_trusted_runtime_checkpoint(path)
    begin=runtime_input_context_start(saved['runtime'],40000,60000)
    assert begin==1000  # Includes the extreme 95 seconds before the cut.
    legacy=load_trusted_runtime_checkpoint(path)
    del legacy['runtime'].input_price_lookback_ms
    assert runtime_input_context_start(legacy['runtime'],40000,60000)>5000
    last,predictions=timelines.prepare(begin*10**6,151_000_000_000,tick_size=.1)
    actual=simulate_prepared_inputs(last,params,prepared_predictions=predictions,resume_checkpoint=saved)
    expected.pop('input_window_count',None);actual.pop('input_window_count',None)
    exact(expected,actual)
