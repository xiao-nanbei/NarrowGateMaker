from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from live.config import Config, to_backtest_params
from models.backtest_config import build_backtest_base_params
from models.backtest_tick import (
    _aggregate_quality_segment_results,
    build_replay_event_clock,
    build_rolling_variance,
    build_trade_intensity,
    causal_complete_1s_bars,
    causal_prediction_ready_indices,
    load_1s_bars,
    ml_feature_ready_timestamps_ms,
    replay_elapsed_days,
    require_formal_dense_1s_timeline,
    require_formal_dense_variance_timeline,
    simulate_tick,
)
from models.tick_data_types import HistoricalBBOData
from strategy.maker_engine import MakerEngine
from strategy.order_manager import Side
from strategy.quote_core import (
    QuoteCoreConfig,
    QuotePrediction,
    QuoteState,
    circuit_breaker_loss_threshold,
    circuit_breaker_triggered,
    compute_quote_core,
    price_variance_pnl_sigma,
    quote_core_config_from_params,
    reservation_price,
    validate_p3_touch_identity,
)
from strategy.signal import Bar1s, SignalEngine


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


def test_live_trade_intensity_is_mean_count_per_completed_10s_bar() -> None:
    start = 1_000_000
    counts = np.arange(1, 61, dtype=np.float64)
    bars = pd.DataFrame(
        {"trade_count": counts},
        index=pd.to_datetime(start + np.arange(60) * 1_000, unit="ms", utc=True),
    )
    _, per_second = build_trade_intensity(bars)
    engine = SignalEngine(enable_ml=False)
    engine._cpp_signal_features_enabled = False
    for second in range(61):
        engine._finalize_bar(Bar1s(
            ts=start + second * 1_000,
            open=100.0, high=100.0, low=100.0, close=100.0,
            trade_count=second + 1,
            volume=1.0, buy_volume=0.5, sell_volume=0.5,
            buy_count=1, sell_count=1,
        ))
    engine.compute_signal()
    published = engine._feat_history[-1]["trade_intensity_60s"]
    assert len(engine._feat_history) == 6
    assert published == pytest.approx(counts.reshape(6, 10).sum(axis=1).mean())
    assert published == pytest.approx(per_second[-1] * 10.0)
    # A second call cannot republish the unfinished next 10s bucket.
    engine.compute_signal()
    assert len(engine._feat_history) == 6
    assert engine._feat_history[-1]["trade_intensity_60s"] == published


@pytest.mark.parametrize(
    "offset_ms,expected", [(59_999, 255.0), (60_000, 305.0), (65_000, 305.0)],
)
def test_replay_quote_trade_intensity_publishes_complete_bucket_then_holds(
    offset_ms: int, expected: float,
) -> None:
    import models.backtest_tick as bt

    start = 1_000_000
    source_ts = start + np.arange(70, dtype=np.int64) * 1_000
    _, per_second = build_trade_intensity(pd.DataFrame(
        {"trade_count": np.arange(1, 71)},
        index=pd.to_datetime(source_ts, unit="ms", utc=True),
    ))
    trade_ts = np.asarray([start + offset_ms, start + offset_ms + 1])
    trades = pd.DataFrame({
        "transact_time": trade_ts, "price": 100.0, "quantity": 0.0,
        "is_buyer_maker": False,
    })
    # Both engines must consume the same actual book, not their different
    # legacy trade-only synthetic-BBO fallbacks.
    bbo = HistoricalBBOData(
        trade_ts, np.full(2, 99.9), np.full(2, 100.1), np.ones(2), np.ones(2),
    )
    params = {
        "gamma": 0.1, "kappa": 1.0, "maker_fee": 0.0,
        "order_size": 0.001, "max_inventory": 1.0,
        "requote_interval": 0.001, "rq_min": 0.001, "rq_max": 0.001,
        "use_bar_pricing": False, "regime_enabled": True,
        "liq_baseline": 200.0, "vol_baseline": 0.0,
        "max_spread_bps": 0.0, "max_exec_book_age_s": 0.0,
        "replay_event_clock": "trade", "trace_decisions_max": 10,
        "trace_quotes_max": 10, "tick_size": 0.1, "lot_size": 0.001,
        "position_timeout": 0.0, "markout_ema_span_fills": 0,
    }
    result = bt.simulate_tick(
        trades, source_ts, np.ones(70), params, var_ti=per_second, bbo_data=bbo,
    )
    rows = [r for r in result["_decision_trace"] if r["ts_ms"] == trade_ts[0]]
    assert rows
    assert all(r["quote_trade_intensity"] == pytest.approx(expected) for r in rows)
    pytest.importorskip("narrowgate_cpp")
    native = bt._simulate_tick_cpp(
        trades, source_ts, np.ones(70), params, var_ti=per_second, bbo_data=bbo,
    )
    assert native["avg_spread"] == pytest.approx(result["avg_spread"], abs=1e-10)
    assert native["avg_final_spread"] == pytest.approx(
        result["avg_final_spread"], abs=1e-10,
    )
    assert len(native["_quote_trace"]) == len(result["_quote_trace"]) > 0
    for py_row, cpp_row in zip(result["_quote_trace"], native["_quote_trace"], strict=True):
        assert py_row["side"] == cpp_row["side"]
        for key in ("price", "mid", "best_bid", "best_ask"):
            assert py_row[key] == pytest.approx(cpp_row[key], abs=1e-10)


def test_circuit_breaker_uses_the_same_quote_currency_threshold() -> None:
    assert circuit_breaker_loss_threshold(9.0, 4.0, 0.01, 8.0) == pytest.approx(0.48)
    assert not circuit_breaker_triggered(-0.48, 9.0, 4.0, 0.01, 8.0)
    assert circuit_breaker_triggered(-0.481, 9.0, 4.0, 0.01, 8.0)


def test_position_value_hard_fuse_caps_exposure_before_submit() -> None:
    # 100 USDC limit at 10,000 USDC/BTC permits at most 0.010 BTC.
    buy = MakerEngine._cap_exposure_qty_by_position_value(
        side=Side.BUY,
        current_qty=0.008,
        mid=10_000.0,
        requested_qty=0.005,
        max_position_value=100.0,
        lot=0.001,
    )
    sell = MakerEngine._cap_exposure_qty_by_position_value(
        side=Side.SELL,
        current_qty=-0.008,
        mid=10_000.0,
        requested_qty=0.005,
        max_position_value=100.0,
        lot=0.001,
    )

    assert buy == pytest.approx(0.002)
    assert sell == pytest.approx(0.002)


def test_live_to_replay_mapping_preserves_time_and_risk_contract() -> None:
    params = build_backtest_base_params(
        {
            "gamma": 0.05,
            "eta_inventory": 0.04,
            "a_spread": 0.03,
            "kappa": 0.1,
            "order_size": 0.001,
            "max_inventory": 0.01,
            "maker_fee": 0.0,
            "quote_horizon_s": 5.0,
            "pnl_volatility_horizon_s": 300.0,
            "circuit_breaker_sigma": 8.0,
            "max_daily_loss": 50.0,
            "max_position_value": 3000.0,
            "emergency_close_dd": 150.0,
        }
    )
    assert params["gamma"] == pytest.approx(0.05)
    assert params["eta_inventory"] == pytest.approx(0.04)
    assert params["a_spread"] == pytest.approx(0.03)
    assert params["quote_horizon_s"] == pytest.approx(5.0)
    assert params["pnl_volatility_horizon_s"] == pytest.approx(300.0)
    assert params["circuit_breaker_sigma"] == pytest.approx(8.0)
    for name in ("max_daily_loss", "max_position_value", "emergency_close_dd"):
        assert params[name] == getattr(Config().risk, name)
    cfg = Config()
    cfg.risk.max_daily_loss = 7.0
    cfg.risk.max_position_value = 80.0
    cfg.risk.emergency_close_dd = 11.0
    mapped = build_backtest_base_params(to_backtest_params(cfg))
    for name in ("max_daily_loss", "max_position_value", "emergency_close_dd"):
        assert mapped[name] == getattr(cfg.risk, name)


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


def test_python_tick_replay_ioc_expires_without_displayed_liquidity() -> None:
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
    assert result["circuit_breaker_close_ioc_fill_count"] == 0
    assert result["circuit_breaker_close_ioc_expire_count"] >= 1
    assert result["circuit_breaker_closing"] is True
    assert result["final_inventory"] == pytest.approx(0.001, abs=1e-12)


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


def test_quote_coefficient_split_preserves_legacy_and_separates_responsibilities() -> None:
    legacy = _cfg(gamma=0.1)
    explicit = _cfg(gamma=0.1, eta_inventory=0.1, a_spread=0.1)
    assert legacy.eta_inventory == pytest.approx(0.1)
    assert legacy.a_spread == pytest.approx(0.1)
    assert _cfg(gamma=1e-15).eta_inventory == pytest.approx(1e-12)
    assert _cfg(gamma=1e-15).a_spread == pytest.approx(1e-12)

    state = QuoteState(mid=100.0, inventory=0.01, sigma_sq=4.0)
    pred = QuotePrediction()
    baseline = compute_quote_core(state, legacy, pred)
    assert baseline == compute_quote_core(state, explicit, pred)

    inventory_only = compute_quote_core(
        state, _cfg(eta_inventory=0.2, a_spread=0.1), pred
    )
    spread_only = compute_quote_core(
        state, _cfg(eta_inventory=0.1, a_spread=0.2), pred
    )
    assert inventory_only.diagnostics["reservation_price"] != pytest.approx(
        baseline.diagnostics["reservation_price"]
    )
    assert inventory_only.diagnostics["delta_raw"] == pytest.approx(
        baseline.diagnostics["delta_raw"]
    )
    assert spread_only.diagnostics["reservation_price"] == pytest.approx(
        baseline.diagnostics["reservation_price"]
    )
    assert spread_only.diagnostics["delta_raw"] != pytest.approx(
        baseline.diagnostics["delta_raw"]
    )


def test_q_ref_and_order_size_contract_preserves_legacy_quotes() -> None:
    state = QuoteState(mid=100.0, inventory=0.01, sigma_sq=4.0)
    pred = QuotePrediction()
    canonical = compute_quote_core(state, _cfg(gamma=0.1), pred)
    normalized_cfg = _cfg(gamma=0.1, inventory_reference_qty=0.001)
    normalized = compute_quote_core(
        state,
        normalized_cfg,
        pred,
    )

    assert normalized.bid_price == canonical.bid_price
    assert normalized.ask_price == canonical.ask_price
    assert normalized.diagnostics["reservation_price"] == pytest.approx(
        canonical.diagnostics["reservation_price"], abs=0.0
    )
    assert normalized.diagnostics["delta_raw"] == pytest.approx(
        canonical.diagnostics["delta_raw"], abs=0.0
    )
    assert normalized.diagnostics["inventory_units"] == pytest.approx(10.0)
    assert normalized.diagnostics["order_units"] == pytest.approx(1.0)
    assert normalized_cfg.eta_inventory == pytest.approx(0.0001)


def test_quantity_aware_quote_is_invariant_to_btc_vs_mbtc_denomination() -> None:
    common = {
        "quote_math_mode": "quantity_aware_v1",
        "gamma": 0.05,
        "cara_risk_aversion": 0.05,
        "maker_fee": 0.0,
        "quote_horizon_s": 5.0,
        "regime_enabled": False,
        "max_spread_bps": 0.0,
    }
    btc_cfg = quote_core_config_from_params(
        {
            **common,
            "kappa": 0.001,
            "order_size": 0.001,
            "max_inventory": 0.01,
        },
        tick_size=0.1,
        lot_size=0.001,
        use_ml=False,
        use_depth_microprice=False,
        use_depth_kappa=False,
    )
    mbtc_cfg = quote_core_config_from_params(
        {
            **common,
            "kappa": 1.0,
            "order_size": 1.0,
            "max_inventory": 10.0,
        },
        tick_size=0.0001,
        lot_size=1.0,
        use_ml=False,
        use_depth_microprice=False,
        use_depth_kappa=False,
    )
    btc = compute_quote_core(
        QuoteState(mid=100_000.0, inventory=0.002, sigma_sq=4.0),
        btc_cfg,
        QuotePrediction(),
    )
    mbtc = compute_quote_core(
        QuoteState(mid=100.0, inventory=2.0, sigma_sq=4.0e-6),
        mbtc_cfg,
        QuotePrediction(),
    )

    assert mbtc.diagnostics["reservation_price"] * 1_000.0 == pytest.approx(
        btc.diagnostics["reservation_price"], abs=1e-9
    )
    assert mbtc.diagnostics["delta_raw"] * 1_000.0 == pytest.approx(
        btc.diagnostics["delta_raw"], abs=1e-9
    )
    assert mbtc.bid_price * 1_000.0 == pytest.approx(btc.bid_price, abs=1e-9)
    assert mbtc.ask_price * 1_000.0 == pytest.approx(btc.ask_price, abs=1e-9)


def test_p3_identity_bound_legacy_pair_projection_preserves_b0() -> None:
    state = QuoteState(
        mid=100.0,
        inventory=0.003,
        sigma_sq=4.0,
        best_bid=99.9,
        best_ask=100.1,
    )
    identity = {
        "event_type": "touch",
        "horizon_s": 10.0,
        "distance_origin": "same_side_best_bid_or_ask_at_window_start",
        "distance_unit": "USDC_per_BTC",
        "side": "pooled_buy_sell",
        "queue_included": False,
        "artifact_sha256": "a" * 64,
    }
    assert validate_p3_touch_identity(identity) == identity
    legacy = compute_quote_core(
        state,
        _cfg(
            regime_enabled=True,
            p3_delta_star=2.0,
            p3_kappa_eff=0.1,
            historical_p3_scalar_adapter_enabled=True,
            p3_event_type=identity["event_type"],
            p3_horizon_s=identity["horizon_s"],
            p3_distance_origin=identity["distance_origin"],
            p3_distance_unit=identity["distance_unit"],
            p3_side=identity["side"],
            p3_queue_included=identity["queue_included"],
            p3_artifact_sha256=identity["artifact_sha256"],
        ),
        QuotePrediction(),
    )
    bound = compute_quote_core(
        state,
        _cfg(
            regime_enabled=True,
            p3_delta_star=2.0,
            p3_kappa_eff=0.1,
            historical_p3_scalar_adapter_enabled=True,
            p3_identity_required=True,
            p3_event_type=identity["event_type"],
            p3_horizon_s=identity["horizon_s"],
            p3_distance_origin=identity["distance_origin"],
            p3_distance_unit=identity["distance_unit"],
            p3_side=identity["side"],
            p3_queue_included=identity["queue_included"],
            p3_artifact_sha256=identity["artifact_sha256"],
        ),
        QuotePrediction(),
    )

    assert (bound.bid_price, bound.ask_price, bound.spread) == (
        legacy.bid_price,
        legacy.ask_price,
        legacy.spread,
    )
    assert bound.quote_context["BUY"]["final_price"] == legacy.quote_context["BUY"][
        "final_price"
    ]
    assert bound.quote_context["SELL"]["final_price"] == legacy.quote_context[
        "SELL"
    ]["final_price"]
    assert bound.diagnostics["p3_floor_mode"] == (
        "legacy_pair_projection_from_same_side_bbo"
    )
    assert bound.diagnostics["p3_pair_floor"] == pytest.approx(4.0)
    assert bound.quote_context["BUY"]["final_quote_delta_to_bbo"] == pytest.approx(
        state.best_bid - bound.bid_price
    )
    assert bound.quote_context["SELL"]["final_quote_delta_to_bbo"] == pytest.approx(
        bound.ask_price - state.best_ask
    )


def test_pre_split_b0_final_quote_golden_vectors_remain_exact() -> None:
    """Freeze final tick outputs captured from the pre-split B0 controller."""

    identity = {
        "p3_event_type": "touch",
        "p3_horizon_s": 10.0,
        "p3_distance_origin": "same_side_best_bid_or_ask_at_window_start",
        "p3_distance_unit": "USDC_per_BTC",
        "p3_side": "pooled_buy_sell",
        "p3_queue_included": False,
        "p3_artifact_sha256": "a" * 64,
    }
    cfg = _cfg(
        gamma=0.046,
        kappa=0.073,
        tick_size=0.1,
        maker_fee=-0.00003,
        max_inventory=0.026,
        quote_horizon_s=1.0,
        regime_enabled=True,
        vol_baseline=3.0,
        liq_baseline=200.0,
        kappa_ratio=0.3,
        p3_delta_star=14.0,
        p3_kappa_eff=0.067,
        max_spread_bps=20.0,
        spread_cap_mode=1,
        historical_p3_scalar_adapter_enabled=True,
        **identity,
    )
    vectors = (
        (
            QuoteState(
                mid=65_000.0,
                inventory=0.0,
                sigma_sq=9.0,
                trade_intensity=200.0,
                best_bid=64_999.9,
                best_ask=65_000.1,
            ),
            QuotePrediction(),
            (64_973.9, 65_026.100000000006),
        ),
        (
            QuoteState(
                mid=71_234.5,
                inventory=0.013,
                sigma_sq=25.0,
                trade_intensity=80.0,
                best_bid=71_234.2,
                best_ask=71_234.8,
                mo_ema_bid=-7.0,
                mo_ema_ask=2.0,
                position_open=True,
                hold_time_s=123.0,
                unrealized_pnl=-3.0,
            ),
            QuotePrediction(
                dir_10s=0.62,
                vol_10s=4.0,
                ret_10s=0.0002,
                tox_bid=0.7,
                tox_ask=0.3,
            ),
            (71_164.7, 71_304.2),
        ),
        (
            QuoteState(
                mid=59_876.3,
                inventory=-0.02,
                sigma_sq=0.7,
                trade_intensity=450.0,
                best_bid=59_876.1,
                best_ask=59_876.5,
                ber_active=True,
                mo_ema_all=-5.0,
                position_open=True,
                hold_time_s=555.0,
                unrealized_pnl=4.0,
            ),
            QuotePrediction(
                dir_10s=0.4,
                vol_10s=1.2,
                ret_10s=-0.0001,
                tox_bid=0.2,
                tox_ask=0.8,
            ),
            (59_859.0, 59_893.600000000006),
        ),
    )

    for state, prediction, expected in vectors:
        quote = compute_quote_core(state, cfg, prediction)
        assert (quote.bid_price, quote.ask_price) == expected


def test_p3_side_bbo_floor_is_side_specific_and_never_compressed_inward() -> None:
    identity = {
        "event_type": "touch",
        "horizon_s": 10.0,
        "distance_origin": "same_side_best_bid_or_ask_at_window_start",
        "distance_unit": "USDC_per_BTC",
        "side": "pooled_buy_sell",
        "queue_included": False,
        "artifact_sha256": "b" * 64,
    }
    state = QuoteState(
        mid=100.0,
        inventory=0.2,
        sigma_sq=1.0,
        best_bid=99.9,
        best_ask=100.1,
    )
    result = compute_quote_core(
        state,
        _cfg(
            max_inventory=1.0,
            p3_delta_star=0.5,
            p3_kappa_eff=0.1,
            p3_side_bbo_floor_enabled=True,
            max_spread_bps=10.0,
            spread_cap_mode="compress",
            p3_event_type=identity["event_type"],
            p3_horizon_s=identity["horizon_s"],
            p3_distance_origin=identity["distance_origin"],
            p3_distance_unit=identity["distance_unit"],
            p3_side=identity["side"],
            p3_queue_included=identity["queue_included"],
            p3_artifact_sha256=identity["artifact_sha256"],
        ),
        QuotePrediction(),
    )
    assert result.bid_price <= 99.4 + 1e-12
    assert result.ask_price >= 100.6 - 1e-12
    assert result.diagnostics["p3_floor_mode"] == "same_side_bbo_floor"
    assert result.quote_flags["cap_exposure_block"] is True


def test_p3_side_bbo_floor_reassertion_is_idempotent_and_b0_is_noop() -> None:
    from strategy.quote_core import apply_p3_side_bbo_floor

    shifted = apply_p3_side_bbo_floor(
        99.7,
        100.3,
        enabled=True,
        delta_star=0.5,
        best_bid=99.9,
        best_ask=100.1,
        tick_size=0.1,
    )
    assert shifted[:4] == pytest.approx((99.4, 100.6, 99.4, 100.6))
    assert shifted[4:] == (True, True)
    assert apply_p3_side_bbo_floor(
        shifted[0],
        shifted[1],
        enabled=True,
        delta_star=0.5,
        best_bid=99.9,
        best_ask=100.1,
        tick_size=0.1,
    )[:2] == pytest.approx(shifted[:2])
    assert apply_p3_side_bbo_floor(
        99.7,
        100.3,
        enabled=False,
        delta_star=0.5,
        best_bid=99.9,
        best_ask=100.1,
        tick_size=0.1,
    )[:2] == (99.7, 100.3)


def test_f03_ret_action_requires_matching_consumer_horizon_but_ml_off_is_noop() -> None:
    state = QuoteState(mid=100.0, inventory=0.0, sigma_sq=1.0)
    pred = QuotePrediction(ret_10s=0.01)
    disabled = compute_quote_core(
        state,
        _cfg(ml_enabled=False, ret_skew=1.0),
        pred,
    )
    baseline = compute_quote_core(state, _cfg(ml_enabled=False, ret_skew=0.0), pred)
    assert (disabled.bid_price, disabled.ask_price) == (
        baseline.bid_price,
        baseline.ask_price,
    )
    with pytest.raises(ValueError, match="F03 ret action horizon"):
        _cfg(
            ml_enabled=True,
            ret_skew=1.0,
            quote_horizon_s=1.0,
            f03_ret_action_horizon_s=10.0,
        )
    with pytest.raises(ValueError, match="F03 ret action horizon"):
        _cfg(
            ml_enabled=True,
            ret_skew=1.0,
            quote_horizon_s=10.0,
            f03_ret_action_horizon_s=10.0,
        )
    admitted = _cfg(
        ml_enabled=True,
        ret_skew=1.0,
        quote_horizon_s=10.0,
        f03_ret_action_horizon_s=10.0,
        f03_ret_action_compatible=True,
    )
    assert admitted.f03_ret_action_horizon_s == pytest.approx(10.0)
    with pytest.raises(ValueError, match="F03 ret action horizon"):
        quote_core_config_from_params(
            {
                "gamma": 0.1,
                "kappa": 1.0,
                "maker_fee": 0.0,
                "order_size": 0.001,
                "max_inventory": 0.01,
                "ret_skew": 1.0,
                "quote_horizon_s": 10.0,
                "f03_ret_action_horizon_s": 10.0,
                "f03_ret_action_compatible": True,
                "strict_calibration": True,
            },
            tick_size=0.1,
            lot_size=0.001,
            use_ml=True,
            use_depth_microprice=False,
            use_depth_kappa=False,
        )


def test_legacy_replay_gamma_override_still_rebinds_both_coefficients() -> None:
    params = build_backtest_base_params(
        {
            "gamma": 0.05,
            "kappa": 0.1,
            "order_size": 0.001,
            "max_inventory": 0.01,
            "maker_fee": 0.0,
        }
    )
    assert "eta_inventory" not in params
    assert "a_spread" not in params
    params["inventory_reference_qty"] = 1.0
    params["gamma"] = 0.07
    cfg = quote_core_config_from_params(
        params,
        tick_size=0.1,
        lot_size=0.001,
        use_ml=False,
        use_depth_microprice=False,
        use_depth_kappa=False,
    )
    assert cfg.eta_inventory == pytest.approx(0.07)
    assert cfg.a_spread == pytest.approx(0.07)


def test_legacy_replay_without_p3_adapter_field_preserves_pair_floor() -> None:
    """Old frozen replay params inherit the pre-split P3 B0 behavior."""

    cfg = quote_core_config_from_params(
        {
            "gamma": 0.046,
            "kappa": 0.073,
            "maker_fee": 0.0,
            "order_size": 0.001,
            "max_inventory": 0.026,
            "regime_enabled": True,
            "p3_delta_star": 14.0,
            "p3_kappa_eff": 0.067,
            "fill_probability_event_type": "touch",
            "fill_probability_horizon_s": 10.0,
            "fill_probability_distance_origin": (
                "same_side_best_bid_or_ask_at_window_start"
            ),
            "fill_probability_distance_unit": "USDC_per_BTC",
            "fill_probability_side": "pooled_buy_sell",
            "fill_probability_queue_included": False,
            "fill_probability_artifact_sha256": "a" * 64,
        },
        tick_size=0.1,
        lot_size=0.001,
        use_ml=False,
        use_depth_microprice=False,
        use_depth_kappa=False,
    )
    result = compute_quote_core(
        QuoteState(
            mid=78_000.0,
            inventory=0.0,
            sigma_sq=0.0,
            best_bid=77_999.9,
            best_ask=78_000.1,
        ),
        cfg,
        QuotePrediction(),
    )

    assert cfg.historical_p3_scalar_adapter_enabled is True
    assert result.diagnostics["p3_pair_floor"] == pytest.approx(28.0)
    assert result.spread >= 28.0


@pytest.mark.parametrize("name", ("eta_inventory", "a_spread"))
@pytest.mark.parametrize("value", (0.0, -1.0, float("nan"), float("inf"), True))
def test_dimensioned_quote_coefficients_must_be_finite_and_positive(
    name: str,
    value: object,
) -> None:
    with pytest.raises(ValueError, match=name):
        _cfg(**{name: value})


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


@pytest.mark.parametrize(
    "source_index",
    (
        [1_783_987_200_000, 1_783_987_202_000],
        [1_783_987_200_000, 1_783_987_200_000],
        [1_783_987_201_000, 1_783_987_200_000],
    ),
)
def test_formal_1s_source_is_rejected_before_completion_repairs_it(
    source_index: list[int],
) -> None:
    bars = pd.DataFrame(
        {"close": [100.0, 102.0], "trade_count": [3, 2]},
        index=pd.Index(source_index),
    )

    with pytest.raises(RuntimeError, match="no sparse or duplicate seconds"):
        causal_complete_1s_bars(bars, require_dense_source=True)


def test_formal_1s_loader_validates_raw_parquet_before_reindex(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import models.backtest_tick as backtest_tick

    source = pd.DataFrame(
        {"close": [100.0, 102.0], "trade_count": [3, 2]},
        index=pd.Index([1_783_987_200_000, 1_783_987_202_000]),
    )
    source.to_parquet(tmp_path / "BTCUSDC-1s-2026-07-16.parquet")
    monkeypatch.setattr(backtest_tick, "BARS_DIR", tmp_path)
    monkeypatch.setattr(
        backtest_tick,
        "filter_paths_for_orderbook_quality",
        lambda paths, *_args, **_kwargs: paths,
    )
    monkeypatch.setattr(
        backtest_tick,
        "filter_frame_for_orderbook_quality",
        lambda frame, *_args, **_kwargs: frame,
    )

    with pytest.raises(RuntimeError, match="no sparse or duplicate seconds"):
        load_1s_bars(
            days=["2026-07-16"],
            require_dense_source=True,
        )


def test_rolling_variance_warmup_never_backfills_from_future_prices() -> None:
    timestamps = pd.Index(
        np.arange(1_800_000_000_000, 1_800_000_012_000, 1_000)
    )
    prefix = pd.DataFrame(
        {
            "close": np.arange(100.0, 112.0),
            "trade_count": np.ones(12),
        },
        index=timestamps,
    )
    extended = pd.concat(
        [
            prefix,
            pd.DataFrame(
                {
                    "close": [1_000.0, 10.0],
                    "trade_count": [1.0, 1.0],
                },
                index=pd.Index([timestamps[-1] + 1_000, timestamps[-1] + 2_000]),
            ),
        ]
    )

    _, prefix_variance = build_rolling_variance(prefix)
    _, extended_variance = build_rolling_variance(extended)

    np.testing.assert_array_equal(
        extended_variance[: len(prefix_variance)],
        prefix_variance,
    )
    np.testing.assert_array_equal(prefix_variance[:9], np.ones(9))
    assert prefix_variance[9] == pytest.approx(1e-6)


@pytest.mark.parametrize("offset_ms,expected", [(999, 1.0), (1_000, 25.0), (1_999, 25.0), (2_000, 100.0)])
def test_replay_variance_left_label_waits_for_complete_second(offset_ms, expected) -> None:
    import models.backtest_tick as bt

    start = 1_000_000
    source_ts = start + np.asarray([0, 1_000], dtype=np.int64)
    timestamps = start + np.asarray([offset_ms, offset_ms + 1], dtype=np.int64)
    trades = pd.DataFrame({
        "transact_time": timestamps, "price": 100.0, "quantity": 0.0,
        "is_buyer_maker": False,
    })
    bbo = HistoricalBBOData(
        timestamps, np.full(2, 99.9), np.full(2, 100.1), np.ones(2), np.ones(2),
    )
    params = {
        "gamma": 0.1, "kappa": 1.0, "maker_fee": 0.0,
        "order_size": 0.001, "max_inventory": 1.0,
        "requote_interval": 0.001, "rq_min": 0.001, "rq_max": 0.001,
        "use_bar_pricing": False, "regime_enabled": False,
        "max_exec_book_age_s": 0.0, "replay_event_clock": "trade",
        "trace_decisions_max": 10, "trace_quotes_max": 10,
        "tick_size": 0.1, "lot_size": 0.001, "position_timeout": 0.0,
        "markout_ema_span_fills": 0,
    }
    variance = np.asarray([25.0, 100.0])
    result = bt.simulate_tick(trades, source_ts, variance, params, bbo_data=bbo)
    rows = [row for row in result["_decision_trace"] if row["ts_ms"] == timestamps[0]]
    assert rows and all(row["sigma_sq_raw"] == expected for row in rows)
    expected_index = -1 if offset_ms < 1_000 else 0 if offset_ms < 2_000 else 1
    assert all(row["feature_ready_generation_index"] == expected_index for row in rows)
    # The first supplied value is not a permitted initial-state shortcut: at
    # 999ms neither implementation may see that bar's 1000ms completed close.
    pytest.importorskip("narrowgate_cpp")
    native = bt._simulate_tick_cpp(trades, source_ts, variance, params, bbo_data=bbo)
    assert native["avg_spread"] == pytest.approx(result["avg_spread"], abs=1e-10)
    assert native["avg_final_spread"] == pytest.approx(result["avg_final_spread"], abs=1e-10)
    assert [row["price"] for row in native["_quote_trace"]] == pytest.approx(
        [row["price"] for row in result["_quote_trace"]], abs=1e-10,
    )


def test_formal_replay_fails_closed_without_dense_1s_timeline() -> None:
    with pytest.raises(RuntimeError, match="canonical dense 1s bars"):
        require_formal_dense_1s_timeline(None, {"strict_calibration": True})
    assert require_formal_dense_1s_timeline(
        None, {"replay_purpose": "exploratory"}
    ) is None

    sparse = pd.DataFrame(
        {"close": [100.0, 101.0], "trade_count": [1.0, 1.0]},
        index=pd.Index([1_800_000_000_000, 1_800_000_002_000]),
    )
    with pytest.raises(RuntimeError, match="no sparse or duplicate seconds"):
        require_formal_dense_1s_timeline(
            sparse, {"replay_purpose": "formal"}
        )

    dense = pd.DataFrame(
        {"close": [100.0, 100.0, 101.0], "trade_count": [1.0, 0.0, 1.0]},
        index=pd.Index(
            [1_800_000_000_000, 1_800_000_001_000, 1_800_000_002_000]
        ),
    )
    assert require_formal_dense_1s_timeline(
        dense,
        {"replay_purpose": "formal"},
        expected_start_ms=1_800_000_000_000,
        expected_end_ms=1_800_000_003_000,
    ) is dense
    with pytest.raises(RuntimeError, match="requested start boundary"):
        require_formal_dense_1s_timeline(
            dense,
            {"replay_purpose": "formal"},
            expected_start_ms=1_799_999_999_000,
            expected_end_ms=1_800_000_003_000,
        )


def test_formal_engine_gate_covers_the_final_merged_event_clock() -> None:
    trades = pd.DataFrame(
        {
            "transact_time": [0, 1_000],
            "price": [100.0, 100.0],
            "quantity": [0.0, 0.0],
            "is_buyer_maker": [False, False],
        }
    )
    formal = {
        "strict_calibration": True,
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 1_000,
        "replay_event_clock_end_ts_ms": 3_000,
    }

    with pytest.raises(RuntimeError, match="does not cover"):
        simulate_tick(
            trades,
            np.array([0, 1_000], dtype=np.int64),
            np.array([1.0, 1.0], dtype=np.float64),
            formal,
        )


def test_formal_variance_timeline_accepts_dense_superset_and_rejects_negative() -> None:
    trades = pd.DataFrame({"transact_time": [1_000, 2_000]})
    timestamps = np.array([0, 1_000, 2_000, 3_000], dtype=np.int64)
    require_formal_dense_variance_timeline(
        timestamps,
        np.ones(4, dtype=np.float64),
        {"replay_purpose": "formal"},
        trades_df=trades,
    )
    with pytest.raises(RuntimeError, match="dense 1s variance timeline"):
        require_formal_dense_variance_timeline(
            timestamps,
            np.array([1.0, 1.0, -1.0, 1.0], dtype=np.float64),
            {"replay_purpose": "formal"},
            trades_df=trades,
        )


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


@pytest.mark.parametrize("mode", ["merged", "empirical"])
def test_explicit_replay_end_adds_nontrade_terminal_clock_between_ticks(mode):
    trades = pd.DataFrame({
        "transact_time": [1_000, 1_100], "price": [100.0, 101.0],
        "quantity": [0.001, 0.002], "is_buyer_maker": [False, True],
    })
    events, count = build_replay_event_clock(
        trades, mode=mode, interval_ms=1_000, end_ts_ms=2_999,
        empirical_ts_ms=np.array([1_000, 2_000]),
        empirical_action=np.array([2, 2]),
    )
    terminal = events.iloc[-1]
    assert count == 2
    assert terminal["transact_time"] == 2_999
    assert terminal["price"] == 101.0
    assert terminal["quantity"] == 0.0
    assert not terminal["_is_execution_trade"]
    assert events["quantity"].sum() == pytest.approx(0.003)


@pytest.mark.parametrize("backend", ["python", "cpp"])
def test_explicit_replay_end_reaches_both_backend_terminal_trace(backend):
    from models import backtest_tick as bt

    trades = pd.DataFrame({
        "transact_time": [1_000, 1_100], "price": [100.0, 100.0],
        "quantity": [0.0, 0.0], "is_buyer_maker": [False, False],
    })
    result = bt._simulate_tick_with_engine(
        backend, trades, np.empty(0, dtype=np.int64), np.empty(0),
        {"replay_event_clock": "merged", "replay_clock_interval_ms": 1_000,
         "replay_event_clock_end_ts_ms": 2_999, "trace_quotes_max": 100,
         "gamma": 0.01, "kappa": 1.0, "order_size": 0.001, "max_inventory": 0.01,
         "maker_fee": 0.0, "taker_fee": 0.0, "tick_size": 0.1, "lot_size": 0.001,
         "requote_interval": 100.0, "rq_min": 100.0, "rq_max": 100.0,
         "max_exec_book_age_s": 0.0, "use_bar_pricing": True},
    )
    assert result["_quote_trace"]
    assert all(row["outcome_ts"] == 2_999 for row in result["_quote_trace"])
    assert result["fills_bid"] + result["fills_ask"] == 0
