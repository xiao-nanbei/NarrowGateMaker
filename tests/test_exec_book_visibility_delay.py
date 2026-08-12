import numpy as np
import pandas as pd

from models import backtest_tick as bt
from models.backtest_tick import (
    _exec_book_visibility_delay_ms,
    _load_exec_book_visibility_profile,
    _load_live_requote_clock,
    _paired_exec_book_visibility_state,
    _paired_exec_source_visible_ts,
)
from models.tick_data_types import HistoricalL2Data


def test_visibility_profile_loads_receive_time_book_ages(tmp_path) -> None:
    source = tmp_path / "quote_decisions.csv"
    pd.DataFrame({"depth_age_s": [0.100, 0.200, 1.739, None]}).to_csv(
        source,
        index=False,
    )

    profile = _load_exec_book_visibility_profile(source)

    assert profile["exec_book_visibility_age_column"] == "depth_age_s"
    assert profile["exec_book_visibility_delay_samples_ms"].tolist() == [
        100.0,
        200.0,
        1739.0,
    ]
    assert profile["exec_book_visibility_delay_mean_ms"] == 679.6666666666666


def test_visibility_sample_is_deterministic_and_empirical() -> None:
    samples = np.asarray([82.0, 116.0, 820.0, 1739.0], dtype=np.float64)
    first = _exec_book_visibility_delay_ms(
        1_785_528_006_426,
        mean_ms=0.0,
        jitter_ms=0.0,
        seed=20260718,
        samples_ms=samples,
    )
    second = _exec_book_visibility_delay_ms(
        1_785_528_006_426,
        mean_ms=0.0,
        jitter_ms=0.0,
        seed=20260718,
        samples_ms=samples,
    )

    assert first == second
    assert first in {82, 116, 820, 1739}


def test_mean_plus_jitter_fallback_is_bounded() -> None:
    delay = _exec_book_visibility_delay_ms(
        123_456,
        mean_ms=100.0,
        jitter_ms=25.0,
        seed=7,
    )
    assert 75 <= delay <= 125


def test_visibility_profile_preserves_paired_book_depth_and_mid(tmp_path) -> None:
    source = tmp_path / "live_perf.csv"
    pd.DataFrame(
        {
            "timestamp": [1_784_505_604.294, 1_784_505_627.291],
            "event": ["requote", "requote"],
            "status": ["ok", "ok"],
            "exec_book_age_s": [1.475, 0.001],
            "exec_depth_age_s": [4.221, 1.279],
            "exec_trade_age_s": [1.238, 0.422],
            "mid": [64659.65, 64611.05],
        }
    ).to_csv(source, index=False)

    profile = _load_exec_book_visibility_profile(source)
    state = _paired_exec_book_visibility_state(
        1_784_505_604_294,
        timestamps_ms=profile["exec_book_visibility_paired_ts_ms"],
        book_delays_ms=profile["exec_book_visibility_paired_delay_ms"],
        depth_delays_ms=profile["exec_depth_visibility_paired_delay_ms"],
        trade_delays_ms=profile["exec_trade_visibility_paired_delay_ms"],
        observed_mid=profile["exec_book_visibility_paired_mid"],
        max_gap_ms=5,
    )

    assert state == (1475, 4221, 1238, 64659.65)
    assert profile["exec_book_visibility_paired_count"] == 2
    assert profile["exec_book_visibility_trade_age_column"] == "exec_trade_age_s"


def test_live_telemetry_visibility_is_rebased_to_pre_order_update(tmp_path) -> None:
    source = tmp_path / "live_perf.csv"
    pd.DataFrame(
        {
            "timestamp": [10.0],
            "event": ["requote"],
            "status": ["ok"],
            "update_orders_us": [2_000_000.0],
            "exec_book_age_s": [1.0],
            "exec_depth_age_s": [4.0],
            "exec_trade_age_s": [2.5],
            "mid": [100.0],
        }
    ).to_csv(source, index=False)

    profile = _load_exec_book_visibility_profile(source)

    assert profile["exec_book_visibility_paired_ts_ms"].tolist() == [8_000]
    assert profile["exec_book_visibility_paired_delay_ms"].tolist() == [0.0]
    assert profile["exec_depth_visibility_paired_delay_ms"].tolist() == [2_000.0]
    assert profile["exec_trade_visibility_paired_delay_ms"].tolist() == [500.0]
    assert profile["exec_book_visibility_timestamp_semantics"] == "pre_order_update"


def test_live_requote_clock_uses_pre_order_update_and_pre_cancel_times(tmp_path) -> None:
    source = tmp_path / "live_perf.csv"
    pd.DataFrame(
        {
            "timestamp": [10.0, 12.0],
            "event": ["requote", "requote"],
            "status": ["ok", "stale_book"],
            "update_orders_us": [2_000_000.0, 0.0],
            "rest_cancel_all_sum_us": [0.0, 500_000.0],
        }
    ).to_csv(source, index=False)

    clock = _load_live_requote_clock(source)

    assert clock["requote_clock_ts_ms"].tolist() == [8_000, 11_500]
    assert clock["requote_clock_observed_ts_ms"].tolist() == [10_000, 12_000]
    assert clock["requote_clock_stage_lag_ms"].tolist() == [2_000.0, 500.0]
    assert clock["requote_clock_action"].tolist() == [2, 3]


def test_paired_source_boundary_is_held_through_depth_stream_stall() -> None:
    timestamps = np.asarray([4_294, 6_903, 12_081, 17_161], dtype=np.int64)
    depth_ages = np.asarray([4_221, 6_830, 12_008, 16_983], dtype=np.float64)

    assert _paired_exec_source_visible_ts(
        14_628,
        timestamps_ms=timestamps,
        source_delays_ms=depth_ages,
    ) == 73
    assert _paired_exec_source_visible_ts(
        18_000,
        timestamps_ms=timestamps,
        source_delays_ms=depth_ages,
    ) == 178


def test_refill_features_use_depth_visibility_clock() -> None:
    bt.configure_symbol("BTCUSDC")
    ts = np.arange(0, 6_001, 1_000, dtype=np.int64)
    trades = pd.DataFrame(
        {
            "transact_time": ts,
            "price": np.full(ts.size, 100.0),
            "quantity": np.zeros(ts.size),
            "is_buyer_maker": np.zeros(ts.size, dtype=np.uint8),
            "_is_execution_trade": np.zeros(ts.size, dtype=np.bool_),
        }
    )
    levels = 20
    bid_qty = np.ones((ts.size, levels), dtype=np.float64)
    # Top-ten depth increases while levels 11-20 decrease by the same amount.
    # Live policy observes the increase because it summarizes top ten; a
    # replay incorrectly using top twenty would report no refill.
    bid_qty[4:, :10] = 2.0
    bid_qty[4:, 10:] = 0.0
    l2 = HistoricalL2Data(
        ts_ms=ts,
        bid_px=np.full((ts.size, levels), 99.9),
        bid_qty=bid_qty,
        ask_px=np.full((ts.size, levels), 100.1),
        ask_qty=np.ones((ts.size, levels)),
    )
    params = {
        "gamma": 0.01,
        "kappa": 1.0,
        "maker_fee": 0.0,
        "max_inventory": 0.01,
        "order_size": 0.001,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "requote_clock": "fixed",
        "queue_base": 0.0,
        "queue_decay": 0.0,
        "queue_ahead_mode": "exact_level",
        "maker_fill_prob": 1.0,
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 1_000,
        "use_bar_pricing": False,
        "dynamic_cap_enabled": False,
        "max_spread_bps": 20.0,
        "max_exec_book_age_s": 0.0,
        "ml_enabled": False,
        "trace_decisions_max": 100,
        "collect_curves": False,
        "l2_refill_cancel_lookback_s": 10.0,
        "exec_book_visibility_mode": "paired",
        "exec_book_visibility_paired_max_gap_ms": 5,
        "exec_depth_visibility_source_offset_ms": 17,
        "_exec_book_visibility_paired_ts_ms": ts[1:],
        "_exec_book_visibility_paired_delay_ms": np.zeros(ts.size - 1),
        "_exec_depth_visibility_paired_delay_ms": np.full(ts.size - 1, 3_000.0),
        "_exec_trade_visibility_paired_delay_ms": np.zeros(ts.size - 1),
        "_exec_book_visibility_paired_mid": np.full(ts.size - 1, 100.0),
    }

    result = bt.simulate_tick(
        trades,
        np.asarray([0], dtype=np.int64),
        np.asarray([1.0]),
        params,
        l2_data=l2,
    )
    decisions = pd.DataFrame(result["_decision_trace"])
    delayed = decisions[(decisions["ts_ms"] == 4_000) & (decisions["side"] == "BUY")]

    assert len(delayed) == 1
    assert delayed.iloc[0]["l2_book_refresh_ratio"] == 0.0
    assert result["exec_depth_visibility_source_offset_ms"] == 17

    current_params = dict(params)
    current_params["exec_depth_visibility_source_offset_ms"] = 0
    current_params["_exec_depth_visibility_paired_delay_ms"] = np.zeros(
        ts.size - 1
    )
    current = bt.simulate_tick(
        trades,
        np.asarray([0], dtype=np.int64),
        np.asarray([1.0]),
        current_params,
        l2_data=l2,
    )
    current_decisions = pd.DataFrame(current["_decision_trace"])
    visible = current_decisions[
        (current_decisions["ts_ms"] == 4_000)
        & (current_decisions["side"] == "BUY")
    ]

    assert len(visible) == 1
    assert visible.iloc[0]["l2_book_refresh_ratio"] > 0.0


def test_source_offset_does_not_make_paired_depth_age_negative() -> None:
    bt.configure_symbol("BTCUSDC")
    trade_ts = np.arange(0, 6_001, 1_000, dtype=np.int64)
    trades = pd.DataFrame(
        {
            "transact_time": trade_ts,
            "price": np.full(trade_ts.size, 100.0),
            "quantity": np.zeros(trade_ts.size),
            "is_buyer_maker": np.zeros(trade_ts.size, dtype=np.uint8),
            "_is_execution_trade": np.zeros(trade_ts.size, dtype=np.bool_),
        }
    )
    # The source-label offset deliberately selects the 1,010ms frame for the
    # 1,000ms decision. Freshness must still come from the paired receive-time
    # observation (0ms), not from 1,000 - 1,010 = -10ms.
    l2 = HistoricalL2Data(
        ts_ms=np.concatenate(
            [np.asarray([0], dtype=np.int64), trade_ts[1:] + 10]
        ),
        bid_px=np.full((trade_ts.size, 1), 99.9),
        bid_qty=np.ones((trade_ts.size, 1)),
        ask_px=np.full((trade_ts.size, 1), 100.1),
        ask_qty=np.ones((trade_ts.size, 1)),
    )
    params = {
        "gamma": 0.01,
        "kappa": 1.0,
        "maker_fee": 0.0,
        "max_inventory": 0.01,
        "order_size": 0.001,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "requote_clock": "fixed",
        "queue_base": 0.0,
        "queue_decay": 0.0,
        "queue_ahead_mode": "exact_level",
        "maker_fill_prob": 1.0,
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 1_000,
        "use_bar_pricing": False,
        "dynamic_cap_enabled": False,
        "max_spread_bps": 20.0,
        # Disable the scheduler's independent fixed-clock stale precheck. This
        # test targets the common-policy freshness value used by empirical
        # live-parity replay, where the recorded status owns that precheck.
        "max_exec_book_age_s": 0.0,
        "ml_enabled": False,
        "trace_decisions_max": 20,
        "collect_curves": False,
        "exec_book_visibility_mode": "paired",
        "exec_book_visibility_paired_max_gap_ms": 5,
        "exec_depth_visibility_source_offset_ms": 17,
        "_exec_book_visibility_paired_ts_ms": trade_ts[1:],
        "_exec_book_visibility_paired_delay_ms": np.zeros(trade_ts.size - 1),
        "_exec_depth_visibility_paired_delay_ms": np.zeros(trade_ts.size - 1),
        "_exec_trade_visibility_paired_delay_ms": np.zeros(trade_ts.size - 1),
        "_exec_book_visibility_paired_mid": np.full(trade_ts.size - 1, 100.0),
    }

    result = bt.simulate_tick(
        trades,
        np.asarray([0], dtype=np.int64),
        np.asarray([1.0]),
        params,
        l2_data=l2,
    )
    decisions = pd.DataFrame(result["_decision_trace"])
    first = decisions[decisions["ts_ms"] > 0]

    assert len(first) >= 2
    assert (first["depth_age_s"] == 0.0).all()
    assert not first["reason_text"].str.contains("common_policy_hard_pause").any()
