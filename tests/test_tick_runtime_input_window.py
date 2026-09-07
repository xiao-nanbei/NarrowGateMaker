from dataclasses import replace

import numpy as np
import pytest

from models.backtest_tick import simulate_tick
from models.replay.runtime_checkpoint_io import load_trusted_runtime_checkpoint, save_runtime_checkpoint
from tests.test_tick_runtime_checkpoint import assert_same, scenario


def test_configured_buy_sell_policy_state_persists_and_rotates(tmp_path, monkeypatch):
    from models.exchange_book_replay import HistoricalMessageDeliverySchedule, ReceiveTimeCooldownReplayAdapter
    from models.tick_data_types import HistoricalL2Data
    from strategy.boolean_cooldown_live import (
        LiveBooleanCooldownPolicy, RuntimeCooldownPolicyEvaluator, OWNER_POLICY_SELECTED_PREDICATES,
    )
    from tests.test_boolean_cooldown_buy_e3 import _artifact

    monkeypatch.setenv("NARROWGATE_CPP_COOLDOWN", "0")
    ts = np.arange(10, 5_010, 100, dtype=np.int64)
    depth = HistoricalL2Data(ts, (100 + np.sin(np.arange(len(ts))))[:, None],
                             np.ones((len(ts), 1)), (102 + np.sin(np.arange(len(ts))))[:, None],
                             np.ones((len(ts), 1)))

    def adapter(start, end, directory):
        directory.mkdir()
        buy, _ = _artifact(directory)
        sell = LiveBooleanCooldownPolicy(evaluator=RuntimeCooldownPolicyEvaluator(
            rules=(("FIXED_166S", (tuple((name, False) for name in OWNER_POLICY_SELECTED_PREDICATES),)),),
            policy_sha256="1" * 64, predicate_bundle_sha256="2" * 64,
        ), warmup_s=.2, max_feature_age_s=5., native_runtime=False)
        part = replace(depth, **{name: getattr(depth, name)[start:end].copy()
                                for name in ("ts_ms", "bid_px", "bid_qty", "ask_px", "ask_qty")})
        clock = part.ts_ms * 1_000_000
        return ReceiveTimeCooldownReplayAdapter(
            part, HistoricalMessageDeliverySchedule(clock, clock + 1, clock + 2),
            policies={"BUY": buy, "SELL": sell},
        )

    expected = adapter(0, len(ts), tmp_path / "full")
    actual = adapter(0, 35, tmp_path / "first")

    def capture(subject, cutoff, side):
        cutoff *= 1_000_000
        snapshot = subject.capture_exposure_fill(
            assignment_id=f"{cutoff}:{side}", fill_exchange_ts_ns=cutoff - 1,
            fill_visible_ts_ns=cutoff, m0_context={"side": side, "fill_visible_ts_ns": cutoff,
                "baseline_duration_ms": 85_000., "campaign_age_s": 300.},
        )
        return subject.evaluate(snapshot, 85_000.)

    for cutoff in (500, 1_250, 2_050):
        for side in ("BUY", "SELL"):
            assert capture(actual, cutoff, side) == capture(expected, cutoff, side)
    path = tmp_path / "policies.pickle"
    save_runtime_checkpoint(path, {"schema": "tick_replay_runtime.v1", "adapter": actual,
                                   "emitter": actual})
    restored = load_trusted_runtime_checkpoint(path)
    actual = restored["adapter"]
    assert restored["emitter"] is actual
    assert actual._policies["BUY"].windows._updates > 0
    actual.resume_input_window(adapter(10, len(ts), tmp_path / "second"))
    for cutoff in (2_450, 3_999, 5_100):
        for side in ("BUY", "SELL"):
            assert capture(actual, cutoff, side) == capture(expected, cutoff, side)
    for side in ("BUY", "SELL"):
        assert_same(vars(actual._policies[side].windows._state),
                    vars(expected._policies[side].windows._state))
        assert actual._policies[side].windows._feature_ready_ts_ns == expected._policies[side].windows._feature_ready_ts_ns
    for key in ("depth_callbacks_consumed", "depth_rows_available", "snapshots_emitted", "evaluations"):
        assert actual.audit()[key] == expected.audit()[key]

    def replay_inputs(directory, depth_start=0):
        args, kwargs = scenario("async")
        policy = adapter(depth_start, len(ts), directory)
        args[3].update(fill_cooldown=85., requote_threshold_bps=1., cooldown_duration_policy_evaluator=policy,
                       cooldown_v2_snapshot_emitter=policy)
        return args, kwargs

    args, kwargs = replay_inputs(tmp_path / "runtime-full")
    uninterrupted = simulate_tick(*args, **kwargs)
    args, kwargs = replay_inputs(tmp_path / "runtime-first")
    partial = simulate_tick(*args, **kwargs, checkpoint_at_ts_ms=1_250)
    saved = partial["_replay_checkpoint"]
    assert saved["runtime"].cooldown_duration_policy_evaluator._captures > 0
    save_runtime_checkpoint(path, saved)
    args, kwargs = replay_inputs(tmp_path / "runtime-next", depth_start=10)
    args = (args[0][args[0].transact_time >= 500].copy(), *args[1:])
    continued = simulate_tick(*args, **kwargs, resume_input_batch=True,
                              resume_checkpoint=load_trusted_runtime_checkpoint(path))
    # The health audit measures real host evaluation microseconds; it is not an
    # economic/replay clock. All modeled outputs must still match exactly.
    for result in (continued, uninterrupted):
        for key in ("_cooldown_v2_snapshot_emitter_audit", "_cooldown_duration_policy_audit"):
            result.pop(key, None)
    assert_same(continued, uninterrupted)


def test_native_file_window_rotation_matches_uninterrupted_runtime(tmp_path):
    from models.exchange_book_replay import HistoricalExchangeBookEvent

    args, kwargs = scenario("async")
    base = 1_700_000_000_000
    args[0]["transact_time"] += base
    args[1][:] += base
    kwargs["bbo_data"].ts_ms[:] += base
    args[3].update(exchange_book_queue_mode="diagnostic",
                   replay_event_clock_end_ts_ms=base + 3_000)
    events = [HistoricalExchangeBookEvent(
        market_id="binance_futures:perpetual:BTCUSDC", event_type="snapshot",
        exchange_ts_ns=(base + timestamp) * 1_000_000,
        local_receive_ts_ns=(base + timestamp + 1) * 1_000_000,
        last_update_id=index + 1, source=f"/old/{'00' if index < 2 else '01'}.jsonl",
        source_ordinal=index,
        levels=(("bid", 960, 1.), ("bid", 999, 1.), ("ask", 1001, 1.), ("ask", 1040, 1.)),
    ) for index, timestamp in enumerate((-100, 300, 800, 1_105, 1_105, 1_200, 2_000, 2_800))]
    expected = simulate_tick(*args, **kwargs, exchange_book_event_tape=events)
    partial = simulate_tick(*args, **kwargs, exchange_book_event_tape=events,
                            checkpoint_at_ts_ms=base + 1_110)
    path = tmp_path / "native-window.pickle"
    save_runtime_checkpoint(path, partial["_replay_checkpoint"])
    cropped = args[0][args[0].transact_time >= base + 500].copy()
    bbo = kwargs["bbo_data"]
    bbo = replace(bbo, **{name: getattr(bbo, name)[bbo.ts_ms >= base + 500].copy()
                          for name in ("ts_ms", "best_bid", "best_ask", "bid_qty", "ask_qty")})
    new_events = [replace(event, source="/new/01.jsonl", source_ordinal=index)
                  for index, event in enumerate(events[2:])]
    actual = simulate_tick(cropped, *args[1:], bbo_data=bbo,
                           exchange_book_event_tape=new_events,
                           resume_checkpoint=load_trusted_runtime_checkpoint(path),
                           resume_input_batch=True)
    assert_same(actual, expected)


@pytest.mark.parametrize("mode", ["ordinary", "async", "compute", "timeout", "emergency", "close_replace"])
@pytest.mark.parametrize("cut", [1_005, 1_110, 1_121, 1_399, 2_001])
def test_rotated_arrays_keep_accounting_and_order_lifecycle(mode, cut, tmp_path):
    args, kwargs = scenario(mode)
    expected = simulate_tick(*args, **kwargs)
    # The first batch has lookahead beyond the cut. The second drops a real
    # consumed prefix; it is not merely an uninterrupted replay with checkpoints.
    first_args = (args[0][args[0].transact_time < 2_500].copy(), args[1], args[2],
                  {**args[3], "replay_event_clock_end_ts_ms": 2_499})
    first_bbo = kwargs["bbo_data"]
    partial = simulate_tick(*first_args, bbo_data=first_bbo, checkpoint_at_ts_ms=cut)
    if partial.get("completed") is not False:
        assert_same(partial, expected)
        return
    path = tmp_path / "batch.pickle"
    save_runtime_checkpoint(path, partial["_replay_checkpoint"])
    old = load_trusted_runtime_checkpoint(path)
    second_args = (args[0][args[0].transact_time >= 500].copy(), args[1], args[2], args[3])
    # The delayed-compute case still references the first quote's snapshot.
    # Keep that actual predecessor instead of fabricating a newer snapshot.
    bbo_start = 0 if mode == "compute" else 500
    bbo = replace(first_bbo, **{
        name: getattr(first_bbo, name)[first_bbo.ts_ms >= bbo_start].copy()
        for name in ("ts_ms", "best_bid", "best_ask", "bid_qty", "ask_qty")
    })
    actual = simulate_tick(*second_args, bbo_data=bbo, resume_checkpoint=old, resume_input_batch=True)
    assert_same(actual, expected)
    assert len(second_args[0]) < len(args[0])


@pytest.mark.parametrize("delayed_fill", [False, True])
def test_multiple_windows_rotate_book_variance_predictions_and_pending_fills(tmp_path, delayed_fill):
    import pandas as pd
    from models.tick_data_types import HistoricalBBOData, HistoricalL2Data
    from tests.test_python_planned_maintenance_replay import _async_fifo_params, _params

    # Real 120-second local-rank context is retained in every batch. Earlier
    # short tests exercise cursor translation; this case exercises lookbacks.
    times = np.arange(0, 142_001, 100)
    trades = pd.DataFrame({
        "transact_time": times, "price": np.full(times.size, 100.0),
        "quantity": np.zeros(times.size), "is_buyer_maker": np.ones(times.size, dtype=np.uint8),
    })
    for timestamp, price, side in ((133_100, 90., 1), (137_100, 110., 0), (139_100, 90., 1)):
        trades.loc[(trades.transact_time >= timestamp) & (trades.transact_time < timestamp + 500),
                   ["price", "quantity", "is_buyer_maker"]] = [price, 10., side]
    variance_ts = np.arange(0, 142_001, 1_000)
    variance = np.linspace(1.0, 1.2, variance_ts.size)
    params = {**_params(), **_async_fifo_params(), "replay_event_clock_end_ts_ms": 142_000,
              "rq_min": 0.5, "rq_max": 1.0, "trace_decisions_max": 1_000,
              "ml_enabled": True, "vol_blend": 0.2,
              "queue_l2_cancel_ahead_enabled": True}
    if delayed_fill:
        params["_private_fill_visibility_latency_samples_ms"] = [1_200.]

    def inputs(start, end):
        clock = times[(times >= start) & (times <= end)]
        mask = (variance_ts >= start) & (variance_ts <= end)
        return {
            "trades_df": trades[(trades.transact_time >= start) & (trades.transact_time <= end)].copy(),
            "var_ts_ms": variance_ts[mask], "var_ssq": variance[mask],
            "var_ti": np.linspace(40., 60., variance_ts.size)[mask],
            "var_retsq": np.linspace(1., 2., variance_ts.size)[mask],
            "ml_data": (variance_ts[mask], np.linspace(.4, .6, variance_ts.size)[mask],
                        variance[mask], np.zeros(mask.sum())),
            "params": {**params, "replay_event_clock_end_ts_ms": end},
            "bbo_data": HistoricalBBOData(clock, np.full(clock.size, 99.9), np.full(clock.size, 100.1),
                                          np.ones(clock.size), np.ones(clock.size)),
            "l2_data": HistoricalL2Data(clock, np.tile([99.9, 96.], (clock.size, 1)),
                                        np.ones((clock.size, 2)), np.tile([100.1, 104.], (clock.size, 1)),
                                        np.ones((clock.size, 2))),
        }

    expected = simulate_tick(**inputs(0, 142_000))
    checkpoint = None
    for start, end, cut in ((0, 140_000, 134_001), (1_000, 142_000, 138_001)):
        partial = simulate_tick(**inputs(start, end), resume_checkpoint=checkpoint,
                                resume_input_batch=checkpoint is not None, checkpoint_at_ts_ms=cut)
        path = tmp_path / "rolling.pickle"
        save_runtime_checkpoint(path, partial["_replay_checkpoint"])
        checkpoint = load_trusted_runtime_checkpoint(path)
    actual = simulate_tick(**inputs(5_000, 142_000), resume_checkpoint=checkpoint, resume_input_batch=True)
    assert expected["fills_bid"] + expected["fills_ask"] >= 2
    assert_same(actual, expected)
