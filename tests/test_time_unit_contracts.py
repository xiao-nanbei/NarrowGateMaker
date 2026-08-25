from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from models.backtest_tick import (
    _aggregate_quality_segment_results,
    build_replay_event_clock,
    causal_prediction_ready_indices,
    causal_complete_1s_bars,
    ml_feature_ready_timestamps_ms,
    replay_elapsed_days,
    simulate_tick,
)
from models.backtest_config import build_backtest_base_params
from models.tick_data_types import HistoricalBBOData
from strategy.maker_engine import MakerEngine
from strategy.quote_core import reservation_price
from strategy.quote_core import (
    QuoteCoreConfig,
    QuotePrediction,
    QuoteState,
    compute_quote_core,
    circuit_breaker_loss_threshold,
    circuit_breaker_triggered,
    price_variance_pnl_sigma,
)


def _cfg(**overrides) -> QuoteCoreConfig:
    values = {
        "gamma": 0.1,
        "kappa": 1.0,
        "tick_size": 0.001,
        "lot_size": 0.001,
        "maker_fee": 0.0,
        "order_size": 0.001,
        "max_inventory": 1.0,
        "ml_enabled": False,
    }
    values.update(overrides)
    return QuoteCoreConfig(**values)


def test_absolute_price_variance_converts_to_quote_currency_without_mid() -> None:
    # sqrt(9 (USDC/BTC)^2/s * 4s) * 0.01 BTC = 0.06 USDC.
    assert price_variance_pnl_sigma(9.0, 4.0, 0.01) == pytest.approx(0.06)


def test_circuit_breaker_uses_the_same_quote_currency_threshold() -> None:
    assert circuit_breaker_loss_threshold(9.0, 4.0, 0.01, 8.0) == pytest.approx(0.48)
    assert not circuit_breaker_triggered(-0.48, 9.0, 4.0, 0.01, 8.0)
    assert circuit_breaker_triggered(-0.481, 9.0, 4.0, 0.01, 8.0)


def test_live_to_replay_mapping_preserves_time_and_risk_contract() -> None:
    params = build_backtest_base_params(
        {
            "gamma": 0.05,
            "kappa": 0.1,
            "order_size": 0.001,
            "max_inventory": 0.01,
            "maker_fee": 0.0,
            "quote_horizon_s": 5.0,
            "pnl_volatility_horizon_s": 300.0,
            "circuit_breaker_sigma": 8.0,
        }
    )
    assert params["quote_horizon_s"] == pytest.approx(5.0)
    assert params["pnl_volatility_horizon_s"] == pytest.approx(300.0)
    assert params["circuit_breaker_sigma"] == pytest.approx(8.0)


def test_python_tick_replay_applies_the_shared_circuit_breaker() -> None:
    trades = pd.DataFrame(
        {
            "transact_time": [0, 1_000, 2_000, 3_000],
            "price": [100.0, 90.0, 90.0, 90.0],
            "quantity": [0.0, 0.0, 0.0, 0.0],
            "is_buyer_maker": [False, False, False, False],
        }
    )
    result = simulate_tick(
        trades,
        np.array([0], dtype=np.int64),
        np.array([1.0], dtype=np.float64),
        {
            "gamma": 0.01,
            "kappa": 1.0,
            "order_size": 0.001,
            "max_inventory": 0.01,
            "requote_interval": 1.0,
            "rq_min": 1.0,
            "rq_max": 1.0,
            "maker_fee": 0.0,
            "taker_fee": 0.0,
            "tick_size": 0.1,
            "lot_size": 0.001,
            "use_bar_pricing": True,
            "initial_inventory": 0.001,
            "initial_entry_price": 100.0,
            "circuit_breaker_sigma": 1.0,
            "circuit_breaker_exit_mode": "immediate_taker",
            "pnl_volatility_horizon_s": 1.0,
            "replay_event_clock": "trade",
            "collect_curves": False,
            "position_timeout": 0.0,
            "markout_ema_span_fills": 0,
        },
    )
    assert result["circuit_breaker_count"] == 1
    assert result["final_inventory"] == pytest.approx(0.0, abs=1e-12)


def test_python_tick_replay_circuit_breaker_uses_maker_close_state() -> None:
    trades = pd.DataFrame(
        {
            "transact_time": [0, 1_000, 2_000, 3_000, 4_000, 5_000],
            "price": [100.0, 90.0, 90.0, 90.0, 90.0, 90.0],
            "quantity": [0.0, 0.0, 0.0, 0.0, 0.01, 0.0],
            "is_buyer_maker": [False, False, False, False, False, False],
        }
    )
    result = simulate_tick(
        trades,
        np.array([0], dtype=np.int64),
        np.array([1.0], dtype=np.float64),
        {
            "gamma": 0.01,
            "kappa": 1.0,
            "order_size": 0.001,
            "max_inventory": 0.01,
            "requote_interval": 1.0,
            "rq_min": 1.0,
            "rq_max": 1.0,
            "maker_fee": 0.0,
            "taker_fee": 0.01,
            "tick_size": 0.1,
            "lot_size": 0.001,
            "queue_base": 0.0,
            "queue_decay": 0.0,
            "maker_fill_prob": 1.0,
            "use_bar_pricing": False,
            "initial_inventory": 0.001,
            "initial_entry_price": 100.0,
            "circuit_breaker_sigma": 1.0,
            "circuit_breaker_exit_mode": "maker_close",
            "pnl_volatility_horizon_s": 1.0,
            "replay_event_clock": "trade",
            "collect_curves": False,
            "position_timeout": 0.0,
            "markout_ema_span_fills": 0,
        },
    )
    assert result["circuit_breaker_count"] == 1
    assert result["circuit_breaker_close_place_count"] >= 1
    assert result["circuit_breaker_close_fill_count"] == 1
    assert result["circuit_breaker_closing"] is False
    assert result["final_inventory"] == pytest.approx(0.0, abs=1e-12)
    assert result["pnl"] == pytest.approx(-0.01, abs=1e-12)


def test_python_tick_replay_maker_close_escalates_to_ioc() -> None:
    ts = np.arange(0, 100_000, 10_000, dtype=np.int64)
    trades = pd.DataFrame(
        {
            "transact_time": ts,
            "price": np.asarray([100.0] + [90.0] * (ts.size - 1)),
            "quantity": np.zeros(ts.size),
            "is_buyer_maker": np.zeros(ts.size, dtype=np.uint8),
        }
    )
    result = simulate_tick(
        trades,
        np.array([0], dtype=np.int64),
        np.array([1.0], dtype=np.float64),
        {
            "gamma": 0.01,
            "kappa": 1.0,
            "order_size": 0.001,
            "max_inventory": 0.01,
            "requote_interval": 10.0,
            "rq_min": 10.0,
            "rq_max": 10.0,
            "maker_fee": 0.0,
            "taker_fee": 0.01,
            "tick_size": 0.1,
            "lot_size": 0.001,
            "queue_base": 0.0,
            "queue_decay": 0.0,
            "maker_fill_prob": 1.0,
            "use_bar_pricing": False,
            "initial_inventory": 0.001,
            "initial_entry_price": 100.0,
            "circuit_breaker_sigma": 1.0,
            "circuit_breaker_exit_mode": "maker_close",
            "pnl_volatility_horizon_s": 1.0,
            "replay_event_clock": "trade",
            "collect_curves": False,
            "position_timeout": 0.0,
            "markout_ema_span_fills": 0,
        },
    )
    assert result["circuit_breaker_count"] == 1
    assert result["circuit_breaker_close_ioc_place_count"] >= 1
    assert result["circuit_breaker_close_ioc_fill_count"] == 1
    assert result["circuit_breaker_close_ioc_expire_count"] == 0
    assert result["circuit_breaker_closing"] is False
    assert result["final_inventory"] == pytest.approx(0.0, abs=1e-12)


def test_quote_horizon_integrates_per_second_variance_explicitly() -> None:
    state = QuoteState(mid=100.0, inventory=0.01, sigma_sq=4.0)
    pred = QuotePrediction()
    one = compute_quote_core(state, _cfg(quote_horizon_s=1.0), pred)
    five = compute_quote_core(state, _cfg(quote_horizon_s=5.0), pred)
    assert one.diagnostics["sigma_sq_horizon"] == pytest.approx(4.0)
    assert five.diagnostics["sigma_sq_horizon"] == pytest.approx(20.0)
    assert five.diagnostics["reservation_price"] == pytest.approx(
        one.diagnostics["reservation_price"] - 0.016
    )


def test_standalone_as_helper_requires_explicit_variance_horizon() -> None:
    assert reservation_price(100.0, 0.01, 0.1, 4.0, 5.0) == pytest.approx(99.98)


def test_pnl_urgency_uses_absolute_price_sigma_contract() -> None:
    state = QuoteState(
        mid=100.0,
        inventory=0.01,
        sigma_sq=4.0,
        position_open=True,
        unrealized_pnl=-0.1,
    )
    result = compute_quote_core(
        state,
        _cfg(
            exit_urgency_strength=1.0,
            urgency_time_weight=0.0,
            urgency_pnl_weight=1.0,
            urgency_signal_weight=0.0,
            pnl_volatility_horizon_s=25.0,
        ),
        QuotePrediction(),
    )
    # PnL sigma is 0.1 USDC, so urgency is one and long inventory skews SELL-ward.
    assert result.diagnostics["asym"] == pytest.approx(-0.9)


def test_left_labelled_ml_bucket_is_visible_only_at_bucket_end() -> None:
    index = pd.DatetimeIndex(
        ["2026-07-01T00:00:00Z", "2026-07-01T00:00:10Z"]
    )
    ready = ml_feature_ready_timestamps_ms(index)
    expected = np.array(
        [
            pd.Timestamp("2026-07-01T00:00:10Z").value // 1_000_000,
            pd.Timestamp("2026-07-01T00:00:20Z").value // 1_000_000,
        ],
        dtype=np.int64,
    )
    np.testing.assert_array_equal(ready, expected)


def test_prediction_window_keeps_only_latest_causal_warm_state() -> None:
    ready = np.asarray(
        [80_000, 90_000, 100_000, 110_000, 120_000],
        dtype=np.int64,
    )
    selected = causal_prediction_ready_indices(
        ready,
        start_ms=100_001,
        end_ms=120_000,
    )
    assert selected.tolist() == [2, 3, 4]


def test_merged_replay_clock_preserves_trades_and_adds_zero_qty_events() -> None:
    trades = pd.DataFrame(
        {
            "transact_time": [1_000, 1_000, 1_250],
            "price": [100.0, 100.1, 100.2],
            "quantity": [0.1, 0.2, 0.3],
            "is_buyer_maker": [True, False, False],
        }
    )
    bbo = HistoricalBBOData(
        ts_ms=np.array([1_050, 1_200], dtype=np.int64),
        best_bid=np.array([99.9, 100.0]),
        best_ask=np.array([100.1, 100.2]),
        bid_qty=np.ones(2),
        ask_qty=np.ones(2),
        source="test",
    )
    events, n_execution = build_replay_event_clock(
        trades,
        mode="merged",
        interval_ms=100,
        bbo_data=bbo,
    )

    assert n_execution == 3
    assert events["transact_time"].tolist() == [
        1_000,
        1_000,
        1_050,
        1_100,
        1_200,
        1_250,
    ]
    execution = events[events["_is_execution_trade"]]
    assert execution["quantity"].tolist() == [0.1, 0.2, 0.3]
    synthetic = events[~events["_is_execution_trade"]]
    assert synthetic["quantity"].eq(0.0).all()
    assert synthetic["price"].tolist() == [100.1, 100.1, 100.1]


def test_causal_1s_completion_matches_live_flat_bar_contract() -> None:
    bars = pd.DataFrame(
        {
            "close": [100.0, 102.0],
            "trade_count": [3, 2],
        },
        index=pd.Index([1_783_987_200_000, 1_783_987_202_000]),
    )

    completed = causal_complete_1s_bars(bars)

    assert completed.index.tolist() == [
        1_783_987_200_000,
        1_783_987_201_000,
        1_783_987_202_000,
    ]
    assert completed["close"].tolist() == [100.0, 100.0, 102.0]
    assert completed["trade_count"].tolist() == [3.0, 0.0, 2.0]


def test_replay_day_denominator_uses_elapsed_time_not_active_trade_seconds() -> None:
    sparse_full_day = np.array([0, 43_200_000, 86_400_000], dtype=np.int64)
    assert replay_elapsed_days(sparse_full_day) == pytest.approx(1.0)


def test_segment_markout_is_fill_quantity_weighted() -> None:
    common = {
        "pnl": 0.0,
        "inventory_adjusted_pnl": 0.0,
        "n_days": 1.0,
        "fills_total": 1,
        "fills_bid": 1,
        "fills_ask": 0,
        "n_requotes": 1,
        "n_final_spread": 1,
        "markout_count": 1,
        "_ts": np.array([], dtype=np.int64),
        "_pnl_ts": np.array([], dtype=np.float64),
        "_inv_ts": np.array([], dtype=np.float64),
    }
    small = dict(
        common,
        avg_markout=1.0,
        avg_markout_bid=1.0,
        markout_qty_btc=0.001,
        markout_qty_bid_btc=0.001,
    )
    large = dict(
        common,
        avg_markout=5.0,
        avg_markout_bid=5.0,
        markout_qty_btc=0.003,
        markout_qty_bid_btc=0.003,
    )
    result = _aggregate_quality_segment_results(
        [small, large],
        full_calendar_days=2.0,
        quality_days=2.0,
        max_gap_s=300.0,
    )
    assert result["avg_markout"] == pytest.approx(4.0)
    assert result["avg_markout_bid"] == pytest.approx(4.0)


def test_segment_campaign_run_totals_are_additive_integers() -> None:
    common = {
        "pnl": 0.0,
        "inventory_adjusted_pnl": 0.0,
        "n_days": 1.0,
        "fills_total": 1,
        "fills_bid": 1,
        "fills_ask": 0,
        "n_requotes": 1,
        "n_final_spread": 1,
        "_ts": np.array([], dtype=np.int64),
        "_pnl_ts": np.array([], dtype=np.float64),
        "_inv_ts": np.array([], dtype=np.float64),
    }
    first = {
        **common,
        "campaign_exposure_increasing_fills": 2,
        "campaign_reducing_fills": 1,
        "campaign_buy_fills": 1,
        "campaign_sell_fills": 2,
    }
    second = {
        **common,
        "campaign_exposure_increasing_fills": 3,
        "campaign_reducing_fills": 2,
        "campaign_buy_fills": 4,
        "campaign_sell_fills": 1,
    }

    result = _aggregate_quality_segment_results(
        [first, second],
        full_calendar_days=2.0,
        quality_days=2.0,
        max_gap_s=300.0,
    )

    assert (
        result["campaign_exposure_increasing_fills"],
        result["campaign_reducing_fills"],
        result["campaign_buy_fills"],
        result["campaign_sell_fills"],
    ) == (5, 3, 5, 3)
    assert all(
        isinstance(result[key], int)
        for key in (
            "campaign_exposure_increasing_fills",
            "campaign_reducing_fills",
            "campaign_buy_fills",
            "campaign_sell_fills",
        )
    )


def test_live_markout_resolves_at_configured_wallclock_horizon() -> None:
    engine = object.__new__(MakerEngine)
    engine.cfg = SimpleNamespace(
        strategy=SimpleNamespace(
            markout_ema_span_fills=3,
            markout_spread_scale=0.2,
            markout_horizon_s=5.0,
            adverse_markout_decay_tau_s=0.0,
            adverse_markout_pause_hybrid=False,
        )
    )
    engine._mo_pending = [(100.0, 100.0, "BUY")]
    engine._mo_ema_bid = 0.0
    engine._mo_ema_ask = 0.0
    engine._mo_ema_all = 0.0
    engine._mo_last_decay_time = 0.0

    engine._resolve_pending_markouts(104.999, 102.0)
    assert len(engine._mo_pending) == 1

    engine._resolve_pending_markouts(105.0, 102.0)
    assert engine._mo_pending == []
    assert engine._mo_ema_bid == pytest.approx(1.0)
    assert engine._mo_ema_all == pytest.approx(1.0)
