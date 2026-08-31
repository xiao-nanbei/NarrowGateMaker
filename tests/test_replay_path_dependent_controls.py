from __future__ import annotations

import hashlib
import json
import math

import numpy as np
import pandas as pd
import pytest

from models import backtest_tick as bt
from models.tick_data_types import HistoricalBBOData, HistoricalExchangeBookEvent
from strategy.dynamic_fill_hazard_model import MODEL_FEATURES
from strategy.replay_controls import (
    LOSS_COOLDOWN_SEMANTICS,
    SYNC_CENSOR_CODE,
    SYNC_DEGRADE_SEMANTICS,
    SYNC_DEGRADE_TAPE_SCHEMA,
    SYNC_EVENT_CODE,
    ConsecutiveLossCooldown,
    load_sync_degrade_events,
)

BASE_MS = 1_700_000_000_000


def _canonical_sha256(value) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _base_params() -> dict[str, object]:
    return {
        "gamma": 0.01,
        "kappa": 1.0,
        "order_size": 0.001,
        "max_inventory": 0.01,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "requote_clock": "fixed",
        "maker_fee": 0.0,
        "taker_fee": 0.0,
        "tick_size": 0.1,
        "lot_size": 0.001,
        "queue_base": 0.0,
        "queue_decay": 0.0,
        "maker_fill_prob": 1.0,
        "use_bar_pricing": True,
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 100,
        "max_exec_book_age_s": 0.0,
        "collect_curves": False,
        "position_timeout": 0.0,
        "markout_ema_span_fills": 0,
        "fill_cooldown_consecutive_reset_policy": "opposite_fill_only",
    }


def _trades(
    timestamps: list[int],
    prices: list[float],
    quantities: list[float] | None = None,
    maker_flags: list[bool] | None = None,
) -> pd.DataFrame:
    count = len(timestamps)
    return pd.DataFrame(
        {
            "transact_time": np.asarray(timestamps, dtype=np.int64),
            "price": np.asarray(prices, dtype=np.float64),
            "quantity": np.asarray(
                quantities if quantities is not None else [0.0] * count,
                dtype=np.float64,
            ),
            "is_buyer_maker": np.asarray(
                maker_flags if maker_flags is not None else [False] * count,
                dtype=np.uint8,
            ),
        }
    )


def test_replay_final_p3_floor_survives_post_fill_shift_and_forces_unsafe_replace() -> None:
    params = _base_params()
    params.update(
        {
            "kappa": 100.0,
            "initial_inventory": 0.004,
            "initial_entry_price": 100.0,
            "requote_threshold_bps": 100.0,
            "replace_min_price_change_ticks": 10.0,
            "replace_min_interval_ms": 10_000.0,
            "trace_quotes_max": 20,
            "post_fill_quote_response_enabled": True,
            "post_fill_quote_response_mode": "inventory_shift",
            "post_fill_inventory_ticks_per_order_unit": 2.0,
            "post_fill_inventory_max_ticks": 20.0,
            "p3_delta_star": 0.5,
            "p3_kappa_eff": 100.0,
            "p3_side_bbo_floor_enabled": True,
            "historical_p3_scalar_adapter_enabled": False,
            "fill_probability_event_type": "touch",
            "fill_probability_horizon_s": 10.0,
            "fill_probability_distance_origin": (
                "same_side_best_bid_or_ask_at_window_start"
            ),
            "fill_probability_distance_unit": "USDC_per_BTC",
            "fill_probability_side": "pooled_buy_sell",
            "fill_probability_queue_included": False,
            "fill_probability_artifact_sha256": "b" * 64,
        }
    )
    trades = _trades(
        [BASE_MS, BASE_MS + 1_000],
        [100.0, 100.1],
    )

    result = bt._simulate_tick_with_engine(
        "python",
        trades,
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        params,
    )

    sell_orders = [row for row in result["_quote_trace"] if row["side"] == "SELL"]
    assert [(row["price"], row["outcome"]) for row in sell_orders] == [
        (pytest.approx(100.5), "cancel"),
        (pytest.approx(100.6), "open_end"),
    ]
    assert result["replace_throttle_count"] == 0


def _write_sync_tape(tmp_path, timestamps: list[int]):
    path = tmp_path / "sync_degrade_events.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": SYNC_DEGRADE_TAPE_SCHEMA,
                "environment": "provider_neutral_test_environment",
                "start_ts_ms": BASE_MS,
                "end_ts_ms": BASE_MS + 86_400_000,
                "events": [
                    {"ts_ms": timestamp, "reason": "position_sync_adjust"}
                    for timestamp in timestamps
                ],
            }
        ),
        encoding="utf-8",
    )
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.parametrize("engine", ["python", "cpp"])
def test_live_replay_max_position_value_order_cap_parity(engine):
    from models.tick_data_types import HistoricalBBOData

    params = _base_params()
    params.update({
        "max_position_value": 100.0, "use_bar_pricing": False,
        "trace_quotes_max": 20, "trace_decisions_max": 20,
    })
    timestamps = [BASE_MS, BASE_MS + 1_000]
    trades = _trades(timestamps, [110_000.0, 110_000.0])
    bbo = HistoricalBBOData(
        np.asarray(timestamps), np.full(2, 109_999.9), np.full(2, 110_000.1),
        np.ones(2), np.ones(2),
    )
    result = bt._simulate_tick_with_engine(
        engine, trades, np.empty(0, dtype=np.int64), np.empty(0), params,
        bbo_data=bbo,
    )
    assert result["_quote_trace"] == []
    assert result["risk_notional_cap_count"] == 4
    assert result["final_inventory"] == 0.0


@pytest.mark.parametrize("engine", ["python", "cpp"])
@pytest.mark.parametrize("reason", ["daily_loss", "position_value"])
def test_live_replay_hard_risk_pauses_without_erasing_inventory(engine, reason):
    from models.tick_data_types import HistoricalBBOData

    params = _base_params()
    params.update({
        "initial_inventory": 0.001, "initial_entry_price": 110_000.0,
        "max_daily_loss": 5.0 if reason == "daily_loss" else 50.0,
        "max_position_value": 1_000.0 if reason == "daily_loss" else 90.0,
        "emergency_close_dd": 150.0, "use_bar_pricing": False,
        "trace_quotes_max": 20, "trace_decisions_max": 20,
    })
    timestamps = [BASE_MS, BASE_MS + 1_000]
    bbo = HistoricalBBOData(
        np.asarray(timestamps), np.full(2, 99_999.9), np.full(2, 100_000.1),
        np.ones(2), np.ones(2),
    )
    result = bt._simulate_tick_with_engine(
        engine, _trades(timestamps, [100_000.0, 100_000.0]),
        np.empty(0, dtype=np.int64), np.empty(0), params, bbo_data=bbo,
    )
    assert result["_quote_trace"] == []
    assert result[f"risk_{reason}_block_count"] == 2
    assert result["final_inventory"] == pytest.approx(0.001)
    assert result["risk_emergency_close_count"] == 0


@pytest.mark.parametrize("engine", ["python", "cpp"])
def test_live_replay_emergency_waits_for_cancel_then_stops_quoting(engine):
    from models.tick_data_types import HistoricalBBOData

    params = _base_params()
    params.update({
        "initial_inventory": 0.001, "initial_entry_price": 100_000.0,
        "max_daily_loss": 100.0, "max_position_value": 1_000.0,
        "emergency_close_dd": 5.0, "use_bar_pricing": False,
        "cancel_order_latency_ms": 200, "new_order_latency_ms": 100,
        "trace_quotes_max": 50, "trace_decisions_max": 50, "trace_fills_max": 50,
    })
    timestamps = np.arange(BASE_MS, BASE_MS + 4_100, 100)
    prices = np.where(timestamps < BASE_MS + 1_000, 100_000.0, 90_000.0)
    bbo = HistoricalBBOData(timestamps, prices - 0.1, prices + 0.1,
                            np.ones(len(prices)), np.ones(len(prices)))
    result = bt._simulate_tick_with_engine(
        engine, _trades(timestamps.tolist(), prices.tolist()),
        np.empty(0, dtype=np.int64), np.empty(0), params, bbo_data=bbo,
    )
    assert result["risk_emergency_close_count"] == 1
    assert result["risk_emergency_latched"]
    assert result["final_inventory"] == pytest.approx(0.0, abs=1e-12)
    cancels = [row for row in result["_quote_trace"] if row["outcome"] == "cancel"]
    fills = result["_fill_trace"]
    assert len(cancels) == 2
    assert len(fills) == 1
    assert fills[0]["fill_ts"] >= max(row["outcome_ts"] for row in cancels)
    assert fills[0]["side"] == "SELL"
    assert fills[0]["fill_fee_rate"] == 0.0
    assert not any(
        row["side"] == "BUY" and row["submit_ts"] >= BASE_MS + 1_000
        for row in result["_quote_trace"]
    )


def _hazard_head(cause: str, *, refill_coefficient: float = 0.0):
    coefficients = [0.0] * len(MODEL_FEATURES)
    coefficients[MODEL_FEATURES.index("visible_refill_size_log1p")] = float(
        refill_coefficient
    )
    return {
        "schema_version": "cause_specific_discrete_cloglog.v1",
        "side": "BUY",
        "cause": cause,
        "feature_names": list(MODEL_FEATURES),
        "feature_mean": [0.0] * len(MODEL_FEATURES),
        "feature_scale": [1.0] * len(MODEL_FEATURES),
        "intercept": math.log(0.1 if cause == "favorable_fill" else 0.2),
        "coefficients": coefficients,
        "baseline_rate_per_second": 0.1,
        "nested_calibrator": {
            "schema_version": "nested_affine_cloglog_calibrator.v1",
            "contract": {"probability_clip": [1e-9, 1.0 - 1e-9]},
            "intercept": 0.0,
            "slope": 1.0,
        },
    }


def _write_q90_artifacts(tmp_path):
    bundle = {
        "schema_version": "dynamic_fill_hazard_bundle.v2",
        "family_id": "test_q90_replay",
        "feature_names": list(MODEL_FEATURES),
        "gates": {},
        "development_days": ["2026-01-01"],
        "models": {
            "BUY": {
                "favorable_fill": _hazard_head("favorable_fill"),
                "adverse_fill": _hazard_head(
                    "adverse_fill",
                    refill_coefficient=-10.0,
                ),
            }
        },
        "repair_models": {},
        "nested_calibration": {"enabled": True},
        "prediction_gate_passed_sides": [],
        "action_experiment_id": "none",
        "action_family_allowed": False,
    }
    bundle["bundle_sha256"] = _canonical_sha256(bundle)
    bundle_path = tmp_path / "hazard_bundle.json"
    bundle_path.write_text(json.dumps(bundle), encoding="utf-8")
    bundle_file_sha = hashlib.sha256(bundle_path.read_bytes()).hexdigest()

    policy = {
        "schema_version": "dynamic_fill_hazard_live_policy.v1",
        "policy_id": "test_buy_q90",
        "model_family_id": "test_q90_replay",
        "model_file_sha256": bundle_file_sha,
        "side": "BUY",
        "eligible_roles": ["opener", "add"],
        "score_formula": "probability_adverse_fill-probability_favorable_fill",
        "entry_threshold": 0.005,
        "entry_action": "cancel",
        "recovery_rule": "score_below_entry_threshold",
        "reentry_action": "baseline_reenter",
        "evaluation_interval_ms": 100.0,
        "reducing_side_unchanged": True,
        "validation_activation_rate": 0.10,
    }
    policy_path = tmp_path / "hazard_policy.json"
    policy_path.write_text(json.dumps(policy), encoding="utf-8")
    policy_file_sha = hashlib.sha256(policy_path.read_bytes()).hexdigest()
    return bundle_path, bundle_file_sha, policy_path, policy_file_sha


def _write_market_data_latency_profile(tmp_path):
    path = tmp_path / "market_data_latency_profile.json"
    path.write_text(
        json.dumps(
            {
                "schema": "market_data_latency_profile.v1",
                "profile_id": "provider_neutral_test_visibility_v1",
                "environment": {
                    "cloud": "AWS",
                    "region": "ap-northeast-1",
                },
                "groups": [
                    {
                        "market_id": "binance:perp:BTCUSDC",
                        "event_type": event_type,
                        "transport": "websocket",
                        "simulation_quantile_probabilities": [0.0, 1.0],
                        "simulation_visibility_lag_ms_quantiles": [
                            delay_ms,
                            delay_ms,
                        ],
                    }
                    for event_type, delay_ms in (("book", 2.0), ("trade", 3.0))
                ],
            }
        ),
        encoding="utf-8",
    )
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def _native_q90_events() -> tuple[HistoricalExchangeBookEvent, ...]:
    return (
        HistoricalExchangeBookEvent(
            market_id="binance_futures:perpetual:BTCUSDC",
            event_type="snapshot",
            exchange_ts_ns=(BASE_MS + 100) * 1_000_000,
            local_receive_ts_ns=(BASE_MS + 101) * 1_000_000,
            event_time_ns=(BASE_MS + 100) * 1_000_000,
            transaction_time_ns=(BASE_MS + 100) * 1_000_000,
            last_update_id=100,
            levels=(
                ("bid", 980, 2.0),
                ("bid", 999, 2.0),
                ("ask", 1001, 2.0),
                ("ask", 1020, 2.0),
            ),
            source_ordinal=1,
        ),
        HistoricalExchangeBookEvent(
            market_id="binance_futures:perpetual:BTCUSDC",
            event_type="delta",
            exchange_ts_ns=(BASE_MS + 400) * 1_000_000,
            local_receive_ts_ns=(BASE_MS + 401) * 1_000_000,
            event_time_ns=(BASE_MS + 400) * 1_000_000,
            transaction_time_ns=(BASE_MS + 400) * 1_000_000,
            first_update_id=101,
            final_update_id=101,
            previous_final_update_id=100,
            levels=(("bid", 990, 5.0),),
            source_ordinal=2,
        ),
    )


def test_consecutive_loss_cooldown_uses_full_round_trips_and_policy_clock():
    state = ConsecutiveLossCooldown(
        max_consecutive_losses=2,
        cooldown_ms=30_000,
    )

    state.on_fill(side="BUY", quantity=0.001, price=100.0, commission=0.0)
    first = state.on_fill(
        side="SELL", quantity=0.001, price=99.0, commission=0.0
    )
    assert first.closed_round_trip
    assert first.consecutive_losses == 1
    assert not first.threshold_pending

    state.on_fill(side="SELL", quantity=0.001, price=100.0, commission=0.0)
    second = state.on_fill(
        side="BUY", quantity=0.001, price=101.0, commission=0.0
    )
    assert second.closed_round_trip
    assert second.consecutive_losses == 2
    assert second.threshold_pending
    assert not state.active(10_000)

    assert state.on_policy_clock(10_000) == "triggered"
    assert state.active(39_999)
    assert state.on_policy_clock(40_000) == "expired"
    assert state.consecutive_losses == 0
    assert state.trigger_count == 1
    assert state.expiry_count == 1


def test_live_to_replay_abi_carries_consecutive_loss_cooldown() -> None:
    from live.config import Config, to_backtest_params

    config = Config()
    config.risk.max_consecutive_losses = 7
    config.risk.cooldown_after_loss = 43.0

    params = to_backtest_params(config)

    assert params["max_consecutive_losses"] == 7
    assert params["cooldown_after_loss"] == 43.0


def test_profitable_round_trip_resets_consecutive_loss_streak():
    state = ConsecutiveLossCooldown(
        max_consecutive_losses=2,
        cooldown_ms=30_000,
    )
    state.on_fill(side="BUY", quantity=0.001, price=100.0, commission=0.0)
    state.on_fill(side="SELL", quantity=0.001, price=99.0, commission=0.0)
    state.on_fill(side="BUY", quantity=0.001, price=100.0, commission=0.0)
    result = state.on_fill(
        side="SELL", quantity=0.001, price=101.0, commission=0.0
    )

    assert result.closed_round_trip
    assert result.round_trip_pnl > 0.0
    assert result.consecutive_losses == 0
    assert not result.threshold_pending


def test_loss_cooldown_flip_splits_fee_and_restores_full_state() -> None:
    state = ConsecutiveLossCooldown(
        max_consecutive_losses=2,
        cooldown_ms=30_000,
    )
    state.on_fill(
        side="BUY",
        quantity=0.001,
        price=100.0,
        commission=0.005,
    )
    result = state.on_fill(
        side="SELL",
        quantity=0.002,
        price=120.0,
        commission=0.02,
    )

    assert result.closed_round_trip
    assert result.round_trip_pnl == pytest.approx(0.005)
    assert state.inventory == pytest.approx(-0.001)
    assert state.avg_entry == pytest.approx(120.0)
    assert state.open_commission == pytest.approx(0.01)
    assert state.round_trip_pnl == pytest.approx(0.0)
    restored = ConsecutiveLossCooldown.restore(state.snapshot())
    assert restored.snapshot() == state.snapshot()


def test_loss_cooldown_flip_preserves_signed_rebate() -> None:
    state = ConsecutiveLossCooldown(
        max_consecutive_losses=2,
        cooldown_ms=30_000,
    )
    state.on_fill(
        side="BUY",
        quantity=0.001,
        price=100.0,
        commission=-0.005,
    )
    result = state.on_fill(
        side="SELL",
        quantity=0.002,
        price=120.0,
        commission=-0.02,
    )

    assert result.round_trip_pnl == pytest.approx(0.035)
    assert state.open_commission == pytest.approx(-0.01)


def test_legacy_partial_loss_cooldown_snapshot_fails_closed() -> None:
    with pytest.raises(ValueError, match="snapshot schema is stale"):
        ConsecutiveLossCooldown.restore(
            {
                "consecutive_losses": 1,
                "cooldown_until_ms": BASE_MS + 30_000,
            }
        )


def test_nonflat_loss_cooldown_snapshot_requires_entry_when_disabled() -> None:
    snapshot = ConsecutiveLossCooldown(
        max_consecutive_losses=0,
        cooldown_ms=0,
    ).snapshot()
    snapshot["inventory"] = 0.001
    with pytest.raises(ValueError, match="requires avg_entry"):
        ConsecutiveLossCooldown.restore(snapshot)


def test_loss_cooldown_snapshot_rejects_unreachable_policy_states() -> None:
    reached_without_transition = ConsecutiveLossCooldown(
        max_consecutive_losses=1,
        cooldown_ms=30_000,
    ).snapshot()
    reached_without_transition.update(
        consecutive_losses=1,
        max_observed_consecutive_losses=1,
        losing_round_trips=1,
        threshold_pending=False,
    )
    with pytest.raises(ValueError, match="pending threshold is inconsistent"):
        ConsecutiveLossCooldown.restore(reached_without_transition)

    active_without_trigger = ConsecutiveLossCooldown(
        max_consecutive_losses=2,
        cooldown_ms=30_000,
    ).snapshot()
    active_without_trigger.update(
        cooldown_until_ms=BASE_MS + 30_000,
        losing_round_trips=1,
    )
    with pytest.raises(ValueError, match="trigger/expiry clock is inconsistent"):
        ConsecutiveLossCooldown.restore(active_without_trigger)

    expiry_without_trigger = ConsecutiveLossCooldown(
        max_consecutive_losses=2,
        cooldown_ms=30_000,
    ).snapshot()
    expiry_without_trigger["expiry_count"] = 1
    with pytest.raises(ValueError, match="trigger/expiry clock is inconsistent"):
        ConsecutiveLossCooldown.restore(expiry_without_trigger)

    streak_without_losses = ConsecutiveLossCooldown(
        max_consecutive_losses=2,
        cooldown_ms=30_000,
    ).snapshot()
    streak_without_losses.update(
        consecutive_losses=1,
        max_observed_consecutive_losses=1,
    )
    with pytest.raises(ValueError, match="maximum streak exceeds losing rounds"):
        ConsecutiveLossCooldown.restore(streak_without_losses)

    disabled_with_cancel_clock = ConsecutiveLossCooldown(
        max_consecutive_losses=0,
        cooldown_ms=0,
    ).snapshot()
    disabled_with_cancel_clock["last_cancel_ts_ms"] = BASE_MS
    with pytest.raises(ValueError, match="disabled.*retained policy state"):
        ConsecutiveLossCooldown.restore(disabled_with_cancel_clock)


def test_active_cooldown_allows_reachable_residual_fill_streak_changes() -> None:
    def triggered_state() -> ConsecutiveLossCooldown:
        state = ConsecutiveLossCooldown(
            max_consecutive_losses=2,
            cooldown_ms=30_000,
        )
        for entry, exit_price in ((100.0, 99.0), (100.0, 99.0)):
            state.on_fill(
                side="BUY",
                quantity=0.001,
                price=entry,
                commission=0.0,
            )
            state.on_fill(
                side="SELL",
                quantity=0.001,
                price=exit_price,
                commission=0.0,
            )
        assert state.on_policy_clock(BASE_MS) == "triggered"
        return state

    state = triggered_state()

    # A pending-cancel residual order can complete profitably while the clock
    # remains active, resetting the streak without expiring the cooldown.
    state.on_fill(
        side="BUY",
        quantity=0.001,
        price=100.0,
        commission=0.0,
    )
    state.on_fill(
        side="SELL",
        quantity=0.001,
        price=101.0,
        commission=0.0,
    )
    assert state.cooldown_until_ms == BASE_MS + 30_000
    assert state.consecutive_losses == 0
    assert not state.threshold_pending
    assert ConsecutiveLossCooldown.restore(state.snapshot()).snapshot() == (
        state.snapshot()
    )

    # A different residual order can close at a loss during the same active
    # clock, advancing the streak and setting pending again without a trigger.
    residual_loss = triggered_state()
    residual_loss.on_fill(
        side="BUY",
        quantity=0.001,
        price=100.0,
        commission=0.0,
    )
    residual_loss.on_fill(
        side="SELL",
        quantity=0.001,
        price=99.0,
        commission=0.0,
    )
    assert residual_loss.consecutive_losses == 3
    assert residual_loss.threshold_pending
    assert ConsecutiveLossCooldown.restore(
        residual_loss.snapshot()
    ).snapshot() == residual_loss.snapshot()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_consecutive_losses", -1),
        ("cooldown_ms", -1),
        ("max_consecutive_losses", 1.0),
        ("cooldown_ms", True),
        ("consecutive_losses", 0.5),
        ("trigger_count", False),
    ],
)
def test_loss_cooldown_rejects_negative_or_noninteger_counts(
    field: str,
    value: object,
) -> None:
    kwargs = {
        "max_consecutive_losses": 1,
        "cooldown_ms": 30_000,
        field: value,
    }
    with pytest.raises(ValueError, match="integer|non-negative"):
        ConsecutiveLossCooldown(**kwargs)


@pytest.mark.parametrize(
    ("inventory", "avg_entry"),
    [
        (float("nan"), 0.0),
        (float("inf"), 100.0),
        (0.0, float("nan")),
        (0.0, 100.0),
        (0.001, 0.0),
    ],
)
def test_loss_cooldown_rejects_invalid_inventory_entry_state(
    inventory: float,
    avg_entry: float,
) -> None:
    with pytest.raises(ValueError, match="finite|avg_entry"):
        ConsecutiveLossCooldown(
            max_consecutive_losses=1,
            cooldown_ms=30_000,
            inventory=inventory,
            avg_entry=avg_entry,
        )


def test_loss_cooldown_rejects_nonboolean_pending_state() -> None:
    with pytest.raises(ValueError, match="must be a boolean"):
        ConsecutiveLossCooldown(
            max_consecutive_losses=1,
            cooldown_ms=30_000,
            threshold_pending="false",  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    "params",
    [
        {"max_consecutive_losses": -1, "cooldown_after_loss": 30.0},
        {"max_consecutive_losses": 1.5, "cooldown_after_loss": 30.0},
        {"max_consecutive_losses": True, "cooldown_after_loss": 30.0},
        {"max_consecutive_losses": 1, "cooldown_after_loss": -1.0},
        {"max_consecutive_losses": 1, "cooldown_after_loss": float("nan")},
    ],
)
def test_backtest_loss_cooldown_config_does_not_clamp_or_truncate(
    params: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="non-negative|integer"):
        bt.loss_cooldown_config_values(params)


def test_cpp_loss_snapshot_partial_abi_fails_closed() -> None:
    cpp = pytest.importorskip("narrowgate_cpp")
    ts = np.asarray([BASE_MS], dtype=np.int64)
    price = np.asarray([100.0], dtype=np.float64)
    quantity = np.asarray([0.0], dtype=np.float64)
    maker = np.asarray([0], dtype=np.uint8)

    partial = cpp.TickReplayParams()
    partial.initial_loss_consecutive_losses = 1
    with pytest.raises(ValueError, match="snapshot state supplied while disabled"):
        cpp.simulate_tick_arrays(ts, price, quantity, maker, partial)

    stale = cpp.TickReplayParams()
    stale.consecutive_loss_snapshot_enabled = True
    stale.consecutive_loss_snapshot_schema = (
        "narrowgate_loss_cooldown_snapshot.v2"
    )
    stale.consecutive_loss_cooldown_semantics = "legacy_fill_count_v0"
    with pytest.raises(ValueError, match="snapshot semantics are stale"):
        cpp.simulate_tick_arrays(ts, price, quantity, maker, stale)

    impossible = cpp.TickReplayParams()
    impossible.max_consecutive_losses = 1
    impossible.cooldown_after_loss_s = 30.0
    impossible.consecutive_loss_cooldown_semantics = LOSS_COOLDOWN_SEMANTICS
    impossible.consecutive_loss_snapshot_enabled = True
    impossible.consecutive_loss_snapshot_schema = (
        "narrowgate_loss_cooldown_snapshot.v2"
    )
    impossible.initial_loss_consecutive_losses = 1
    impossible.initial_loss_max_observed_consecutive_losses = 1
    impossible.initial_loss_losing_round_trips = 1
    with pytest.raises(ValueError, match="pending threshold is inconsistent"):
        cpp.simulate_tick_arrays(ts, price, quantity, maker, impossible)

    def snapshot_params() -> object:
        params = cpp.TickReplayParams()
        params.max_consecutive_losses = 2
        params.cooldown_after_loss_s = 30.0
        params.consecutive_loss_cooldown_semantics = LOSS_COOLDOWN_SEMANTICS
        params.consecutive_loss_snapshot_enabled = True
        params.consecutive_loss_snapshot_schema = (
            "narrowgate_loss_cooldown_snapshot.v2"
        )
        return params

    active_without_trigger = snapshot_params()
    active_without_trigger.initial_loss_cooldown_until_ms = BASE_MS + 30_000
    active_without_trigger.initial_loss_losing_round_trips = 1
    with pytest.raises(ValueError, match="trigger/expiry history is inconsistent"):
        cpp.simulate_tick_arrays(
            ts,
            price,
            quantity,
            maker,
            active_without_trigger,
        )

    expiry_without_trigger = snapshot_params()
    expiry_without_trigger.initial_loss_expiry_count = 1
    with pytest.raises(ValueError, match="trigger/expiry history is inconsistent"):
        cpp.simulate_tick_arrays(
            ts,
            price,
            quantity,
            maker,
            expiry_without_trigger,
        )

    streak_without_losses = snapshot_params()
    streak_without_losses.initial_loss_consecutive_losses = 1
    streak_without_losses.initial_loss_max_observed_consecutive_losses = 1
    with pytest.raises(ValueError, match="trigger/expiry history is inconsistent"):
        cpp.simulate_tick_arrays(
            ts,
            price,
            quantity,
            maker,
            streak_without_losses,
        )

    nonfinite_inventory = snapshot_params()
    nonfinite_inventory.initial_inventory = float("nan")
    with pytest.raises(ValueError, match="snapshot fields are invalid"):
        cpp.simulate_tick_arrays(
            ts,
            price,
            quantity,
            maker,
            nonfinite_inventory,
        )

    flat_with_entry = snapshot_params()
    flat_with_entry.initial_entry_price = 100.0
    with pytest.raises(ValueError, match="inventory/entry is inconsistent"):
        cpp.simulate_tick_arrays(
            ts,
            price,
            quantity,
            maker,
            flat_with_entry,
        )

    nonflat_without_entry = snapshot_params()
    nonflat_without_entry.initial_inventory = 0.001
    with pytest.raises(ValueError, match="inventory/entry is inconsistent"):
        cpp.simulate_tick_arrays(
            ts,
            price,
            quantity,
            maker,
            nonflat_without_entry,
        )

    negative_limit = cpp.TickReplayParams()
    negative_limit.max_consecutive_losses = -1
    with pytest.raises(ValueError, match="config must be finite and non-negative"):
        cpp.simulate_tick_arrays(ts, price, quantity, maker, negative_limit)

    negative_cooldown = cpp.TickReplayParams()
    negative_cooldown.cooldown_after_loss_s = -1.0
    with pytest.raises(ValueError, match="config must be finite and non-negative"):
        cpp.simulate_tick_arrays(ts, price, quantity, maker, negative_cooldown)

def _loss_test_bbo(trades):
    prices = trades["price"].to_numpy(dtype=np.float64)
    return HistoricalBBOData(
        ts_ms=trades["transact_time"].to_numpy(dtype=np.int64),
        best_bid=prices - 0.1, best_ask=prices + 0.1,
        bid_qty=np.ones(len(prices)), ask_qty=np.ones(len(prices)),
    )


def test_signed_rebate_can_turn_gross_loss_into_nonloss_python_cpp() -> None:
    params = _base_params()
    params.update(
        {
            "initial_inventory": 0.001,
            "initial_entry_price": 100.0,
            "circuit_breaker_sigma": 1.0,
            "circuit_breaker_exit_mode": "immediate_taker",
            "pnl_volatility_horizon_s": 1.0,
            "use_bar_pricing": False,
            "replay_clock_interval_ms": 1_000,
            "maker_fee": -0.2,
            "taker_fee": -0.2,
            "max_consecutive_losses": 1,
            "cooldown_after_loss": 30.0,
            "consecutive_loss_cooldown_semantics": LOSS_COOLDOWN_SEMANTICS,
        }
    )
    trades = _trades(
        [BASE_MS, BASE_MS + 1_000, BASE_MS + 2_000],
        [100.0, 90.0, 90.0],
    )
    empty_i64 = np.empty(0, dtype=np.int64)
    empty_f64 = np.empty(0, dtype=np.float64)

    py = bt._simulate_tick_with_engine(
        "python", trades, empty_i64, empty_f64, params, bbo_data=_loss_test_bbo(trades)
    )
    cpp = bt._simulate_tick_with_engine(
        "cpp", trades, empty_i64, empty_f64, params, bbo_data=_loss_test_bbo(trades)
    )

    for result in (py, cpp):
        assert result["pnl"] == pytest.approx(0.008)
        assert result["consecutive_loss_round_trip_loss_count"] == 0
        assert result["consecutive_loss_round_trip_nonloss_count"] == 1
        assert result["consecutive_loss_cooldown_trigger_count"] == 0
        assert result["consecutive_loss_count_end"] == 0
    assert cpp["consecutive_loss_cooldown_state"] == (
        py["consecutive_loss_cooldown_state"]
    )


def test_signed_rebate_fill_trace_is_bound_for_continuous_accounting() -> None:
    params = _base_params()
    params.update(
        {
            "initial_inventory": 0.001,
            "initial_entry_price": 100.0,
            "order_size": 0.002,
            "maker_fee": -0.0001,
            "trace_fills_max": 4,
            "max_consecutive_losses": 2,
            "cooldown_after_loss": 30.0,
            "consecutive_loss_cooldown_semantics": LOSS_COOLDOWN_SEMANTICS,
        }
    )
    trades = _trades(
        [BASE_MS, BASE_MS + 1_000, BASE_MS + 2_000],
        [120.0, 200.0, 200.0],
        quantities=[0.0, 0.002, 0.0],
        maker_flags=[False, False, False],
    )
    empty_i64 = np.empty(0, dtype=np.int64)
    empty_f64 = np.empty(0, dtype=np.float64)

    for engine in ("python", "cpp"):
        result = bt._simulate_tick_with_engine(
            engine,
            trades,
            empty_i64,
            empty_f64,
            params,
        )
        assert len(result["_fill_trace"]) == 1
        fill = result["_fill_trace"][0]
        assert fill["fill_sequence"] == 0
        assert fill["fill_fee_usdc"] < 0.0
        assert fill["fill_fee_asset"] == "USDC"
        assert fill["fill_fee_semantics"] == (
            "signed_fee_positive_cost_negative_rebate_v2"
        )


def test_active_cooldown_resume_preserves_cancel_throttle_python_cpp() -> None:
    resumed = ConsecutiveLossCooldown(
        max_consecutive_losses=1,
        cooldown_ms=30_000,
        consecutive_losses=1,
        cooldown_until_ms=BASE_MS + 30_000,
        last_cancel_ts_ms=BASE_MS,
        trigger_count=1,
        losing_round_trips=1,
        max_observed_consecutive_losses=1,
    )
    params = _base_params()
    params.update(
        {
            "max_consecutive_losses": 1,
            "cooldown_after_loss": 30.0,
            "consecutive_loss_cooldown_semantics": LOSS_COOLDOWN_SEMANTICS,
            "initial_live_state": {
                "reward_path_loss_cooldown": resumed.snapshot(),
            },
        }
    )
    trades = _trades(
        [BASE_MS + 1_000, BASE_MS + 6_000],
        [100.0, 100.0],
    )
    empty_i64 = np.empty(0, dtype=np.int64)
    empty_f64 = np.empty(0, dtype=np.float64)

    py = bt._simulate_tick_with_engine(
        "python", trades, empty_i64, empty_f64, params
    )
    cpp = bt._simulate_tick_with_engine(
        "cpp", trades, empty_i64, empty_f64, params
    )

    assert py["consecutive_loss_cooldown_cancel_count"] == 1
    assert cpp["consecutive_loss_cooldown_cancel_count"] == 1
    assert py["consecutive_loss_cooldown_state"]["last_cancel_ts_ms"] == (
        cpp["consecutive_loss_cooldown_state"]["last_cancel_ts_ms"]
    )
    assert py["consecutive_loss_cooldown_state"]["last_cancel_ts_ms"] == (
        BASE_MS + 5_000
    )


@pytest.mark.parametrize(
    ("consecutive_losses", "pending", "losing", "winning", "maximum"),
    [
        (0, False, 2, 1, 2),
        (3, True, 3, 0, 3),
    ],
    ids=("residual_nonloss_resets_streak", "residual_loss_sets_pending"),
)
def test_active_cooldown_residual_fills_resume_python_cpp(
    consecutive_losses: int,
    pending: bool,
    losing: int,
    winning: int,
    maximum: int,
) -> None:
    resumed = ConsecutiveLossCooldown(
        max_consecutive_losses=2,
        cooldown_ms=30_000,
        consecutive_losses=consecutive_losses,
        cooldown_until_ms=BASE_MS + 30_000,
        last_cancel_ts_ms=-1,
        threshold_pending=pending,
        trigger_count=1,
        losing_round_trips=losing,
        winning_or_flat_round_trips=winning,
        max_observed_consecutive_losses=maximum,
    )
    assert ConsecutiveLossCooldown.restore(
        resumed.snapshot()
    ).last_cancel_ts_ms == -1
    params = _base_params()
    params.update(
        {
            "max_consecutive_losses": 2,
            "cooldown_after_loss": 30.0,
            "consecutive_loss_cooldown_semantics": LOSS_COOLDOWN_SEMANTICS,
            "initial_live_state": {
                "reward_path_loss_cooldown": resumed.snapshot(),
            },
        }
    )
    trades = _trades([BASE_MS + 1_000], [100.0])
    empty_i64 = np.empty(0, dtype=np.int64)
    empty_f64 = np.empty(0, dtype=np.float64)

    py = bt._simulate_tick_with_engine(
        "python", trades, empty_i64, empty_f64, params
    )
    cpp = bt._simulate_tick_with_engine(
        "cpp", trades, empty_i64, empty_f64, params
    )
    assert cpp["consecutive_loss_cooldown_state"] == (
        py["consecutive_loss_cooldown_state"]
    )
    assert py["consecutive_loss_cooldown_state"]["consecutive_losses"] == (
        consecutive_losses
    )
    assert py["consecutive_loss_cooldown_state"]["threshold_pending"] is pending
    assert py["consecutive_loss_cooldown_state"]["trigger_count"] == 1


def test_loss_cooldown_resume_snapshot_is_python_cpp_equal() -> None:
    resumed = ConsecutiveLossCooldown(
        max_consecutive_losses=1,
        cooldown_ms=30_000,
        inventory=0.001,
        avg_entry=100.0,
        open_commission=0.005,
        round_trip_pnl=0.002,
    )
    params = _base_params()
    params.update(
        {
            "initial_inventory": 0.001,
            "initial_entry_price": 100.0,
            "circuit_breaker_sigma": 1.0,
            "circuit_breaker_exit_mode": "immediate_taker",
            "pnl_volatility_horizon_s": 1.0,
            "use_bar_pricing": False,
            "replay_clock_interval_ms": 1_000,
            "max_consecutive_losses": 1,
            "cooldown_after_loss": 30.0,
            "consecutive_loss_cooldown_semantics": LOSS_COOLDOWN_SEMANTICS,
            "initial_live_state": {
                "reward_path_loss_cooldown": resumed.snapshot(),
            },
        }
    )
    trades = _trades(
        [BASE_MS, BASE_MS + 1_000, BASE_MS + 2_000],
        [100.0, 90.0, 90.0],
    )
    empty_i64 = np.empty(0, dtype=np.int64)
    empty_f64 = np.empty(0, dtype=np.float64)

    py = bt._simulate_tick_with_engine(
        "python", trades, empty_i64, empty_f64, params, bbo_data=_loss_test_bbo(trades)
    )
    cpp = bt._simulate_tick_with_engine(
        "cpp", trades, empty_i64, empty_f64, params, bbo_data=_loss_test_bbo(trades)
    )

    py_state = py["consecutive_loss_cooldown_state"]
    cpp_state = cpp["consecutive_loss_cooldown_state"]
    assert cpp_state.keys() == py_state.keys()
    for key in py_state:
        if isinstance(py_state[key], float):
            assert cpp_state[key] == pytest.approx(py_state[key]), key
        else:
            assert cpp_state[key] == py_state[key], key
    assert py_state["inventory"] == pytest.approx(0.0)
    assert py_state["open_commission"] == pytest.approx(0.0)
    assert py_state["round_trip_pnl"] == pytest.approx(0.0)
    assert py_state["losing_round_trips"] == 1


def test_loss_cooldown_flip_fee_path_is_python_cpp_equal() -> None:
    params = _base_params()
    params.update(
        {
            "initial_inventory": 0.001,
            "initial_entry_price": 100.0,
            "order_size": 0.002,
            "maker_fee": 0.0001,
            "max_consecutive_losses": 2,
            "cooldown_after_loss": 30.0,
            "consecutive_loss_cooldown_semantics": LOSS_COOLDOWN_SEMANTICS,
        }
    )
    trades = _trades(
        [BASE_MS, BASE_MS + 1_000, BASE_MS + 2_000],
        [120.0, 200.0, 200.0],
        quantities=[0.0, 0.002, 0.0],
        maker_flags=[False, False, False],
    )
    empty_i64 = np.empty(0, dtype=np.int64)
    empty_f64 = np.empty(0, dtype=np.float64)

    py = bt._simulate_tick_with_engine(
        "python", trades, empty_i64, empty_f64, params
    )
    cpp = bt._simulate_tick_with_engine(
        "cpp", trades, empty_i64, empty_f64, params
    )

    assert py["fills_ask"] == cpp["fills_ask"] == 1
    py_state = py["consecutive_loss_cooldown_state"]
    cpp_state = cpp["consecutive_loss_cooldown_state"]
    for key in py_state:
        if isinstance(py_state[key], float):
            assert cpp_state[key] == pytest.approx(py_state[key]), key
        else:
            assert cpp_state[key] == py_state[key], key
    assert py_state["inventory"] == pytest.approx(-0.001)
    assert py_state["avg_entry"] > 120.0
    assert py_state["open_commission"] > 0.0
    expected_opening_fee = (
        abs(py_state["inventory"])
        * py_state["avg_entry"]
        * params["maker_fee"]
    )
    assert py_state["open_commission"] == pytest.approx(expected_opening_fee)
    assert py_state["round_trip_pnl"] == pytest.approx(0.0)


def test_backtest_flip_zero_boundary_fee_oracle() -> None:
    # BUY 0.001@100 fee .005, then SELL 0.002@120 fee .02.
    post_fill_cash = -0.105 + 0.240 - 0.020
    boundary = bt.campaign_flip_zero_boundary_equity(
        post_fill_cash=post_fill_cash,
        post_fill_position=-0.001,
        fill_price=120.0,
        opening_quantity=0.001,
        fee_rate=0.02 / (0.002 * 120.0),
    )
    assert boundary == pytest.approx(0.005)


def test_backtest_flip_projects_two_campaign_economic_legs() -> None:
    legs = bt.campaign_fill_economic_legs(
        physical_fill_identity="order:17:fill:1",
        side="SELL",
        inventory_before=0.001,
        inventory_after=-0.001,
        fill_quantity=0.002,
        fill_price=120.0,
        fee_rate=0.02 / (0.002 * 120.0),
        closing_campaign_id=4,
        opening_campaign_id=5,
    )

    assert [leg["economic_leg_role"] for leg in legs] == [
        "closing",
        "opening",
    ]
    assert [leg["campaign_id"] for leg in legs] == [4, 5]
    assert [leg["quantity_btc"] for leg in legs] == pytest.approx(
        [0.001, 0.001]
    )
    assert [leg["fee_usdc"] for leg in legs] == pytest.approx([0.01, 0.01])
    assert len({leg["physical_fill_identity"] for leg in legs}) == 1
    assert sum(leg["quantity_btc"] for leg in legs) == pytest.approx(0.002)
    assert sum(leg["fee_usdc"] for leg in legs) == pytest.approx(0.02)


def test_backtest_flip_lifecycle_keeps_one_physical_fill_with_two_legs() -> None:
    params = _base_params()
    params.update(
        {
            "initial_inventory": 0.001,
            "initial_entry_price": 100.0,
            "order_size": 0.002,
            "maker_fee": 0.0001,
            "max_consecutive_losses": 2,
            "cooldown_after_loss": 30.0,
            "consecutive_loss_cooldown_semantics": LOSS_COOLDOWN_SEMANTICS,
            "trace_local_order_lifecycle_max": 16,
            "trace_decisions_max": 200,
        }
    )
    trades = _trades(
        [BASE_MS, BASE_MS + 1_000, BASE_MS + 2_000],
        [120.0, 200.0, 200.0],
        quantities=[0.0, 0.002, 0.0],
        maker_flags=[False, False, False],
    )
    empty_i64 = np.empty(0, dtype=np.int64)
    empty_f64 = np.empty(0, dtype=np.float64)

    result = bt._simulate_tick_with_engine(
        "python", trades, empty_i64, empty_f64, params
    )

    flip_legs = [
        row
        for row in result["_campaign_economic_fill_leg_trace"]
        if row["physical_fill_leg_count"] == 2
    ]
    assert len(flip_legs) == 2
    assert [row["economic_leg_role"] for row in flip_legs] == [
        "closing",
        "opening",
    ]
    assert [row["campaign_id"] for row in flip_legs] == [1, 2]
    assert [row["quantity_btc"] for row in flip_legs] == pytest.approx(
        [0.001, 0.001]
    )
    assert len({row["physical_fill_identity"] for row in flip_legs}) == 1

    lifecycle_fills = [
        row
        for row in result["_local_order_lifecycle_trace"]
        if row["event_type"] in {"partial_fill", "full_fill"}
        and row.get("economic_leg_count") == 2
    ]
    assert len(lifecycle_fills) == 1
    physical = lifecycle_fills[0]
    assert physical["fill_qty"] == pytest.approx(0.002)
    assert physical["campaign_id"] == 1
    assert [leg["campaign_id"] for leg in physical["economic_legs"]] == [1, 2]
    assert physical["physical_fill_identity"] == flip_legs[0][
        "physical_fill_identity"
    ]
    short_trace = [
        row
        for row in result["_decision_trace"]
        if row["campaign_active"] and row["inventory"] < 0.0
    ]
    assert short_trace
    assert {
        (
            row["campaign_side"],
            row["campaign_exposure_increasing_fills_so_far"],
            row["campaign_reducing_fills_so_far"],
        )
        for row in short_trace
    } == {("SHORT", 1, 0)}
    assert (
        result["campaign_exposure_increasing_fills"],
        result["campaign_reducing_fills"],
        result["campaign_buy_fills"],
        result["campaign_sell_fills"],
    ) == (1, 1, 0, 2)


def test_backtest_flat_new_campaign_resets_trace_and_policy_counters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_policy_contexts = []
    original_evaluate = bt.MultiMarketPolicy.evaluate

    def capture_context(policy, context):
        observed_policy_contexts.append(context)
        return original_evaluate(policy, context)

    monkeypatch.setattr(bt.MultiMarketPolicy, "evaluate", capture_context)
    params = _base_params()
    params["trace_decisions_max"] = 200
    trades = _trades(
        [BASE_MS + offset * 1_000 for offset in range(6)],
        [100.0, 50.0, 200.0, 200.0, 400.0, 400.0],
        quantities=[0.0, 0.001, 0.001, 0.0, 0.001, 0.0],
        maker_flags=[False, True, False, False, False, False],
    )
    empty_i64 = np.empty(0, dtype=np.int64)
    empty_f64 = np.empty(0, dtype=np.float64)

    result = bt._simulate_tick_with_engine(
        "python",
        trades,
        empty_i64,
        empty_f64,
        params,
    )

    assert [
        (row["campaign_id"], row["economic_leg_role"], row["side"])
        for row in result["_campaign_economic_fill_leg_trace"]
    ] == [
        (1, "opening", "BUY"),
        (1, "closing", "SELL"),
        (2, "opening", "SELL"),
    ]
    short_trace = [
        row
        for row in result["_decision_trace"]
        if row["campaign_active"] and row["inventory"] < 0.0
    ]
    assert short_trace
    assert {
        (
            row["campaign_side"],
            row["campaign_exposure_increasing_fills_so_far"],
            row["campaign_reducing_fills_so_far"],
        )
        for row in short_trace
    } == {("SHORT", 1, 0)}
    short_policy_contexts = [
        context
        for context in observed_policy_contexts
        if context.campaign_active and context.inventory < 0.0
    ]
    assert short_policy_contexts
    assert {context.campaign_fills for context in short_policy_contexts} == {1}
    assert (
        result["campaign_exposure_increasing_fills"],
        result["campaign_reducing_fills"],
        result["campaign_buy_fills"],
        result["campaign_sell_fills"],
    ) == (2, 1, 1, 2)


def test_backtest_resume_restores_local_campaign_but_run_totals_start_at_zero() -> None:
    params = _base_params()
    params.update(
        {
            "initial_inventory": 0.001,
            "initial_entry_price": 100.0,
            "trace_decisions_max": 20,
            "initial_live_state": {
                "campaign": {
                    "active": True,
                    "start_ts_ms": BASE_MS - 10_000,
                    "pnl": 0.0,
                    "max_inventory": 0.001,
                    "mae": 0.0,
                    "inc_fills": 5,
                    "red_fills": 2,
                    "buy_fills": 5,
                    "sell_fills": 2,
                }
            },
        }
    )
    trades = _trades(
        [BASE_MS, BASE_MS + 1_000],
        [100.0, 100.0],
    )
    empty_i64 = np.empty(0, dtype=np.int64)
    empty_f64 = np.empty(0, dtype=np.float64)

    result = bt._simulate_tick_with_engine(
        "python",
        trades,
        empty_i64,
        empty_f64,
        params,
    )

    assert result["_decision_trace"]
    assert {
        (
            row["campaign_exposure_increasing_fills_so_far"],
            row["campaign_reducing_fills_so_far"],
        )
        for row in result["_decision_trace"]
    } == {(5, 2)}
    assert (
        result["campaign_exposure_increasing_fills"],
        result["campaign_reducing_fills"],
        result["campaign_buy_fills"],
        result["campaign_sell_fills"],
    ) == (0, 0, 0, 0)


def test_sync_tape_is_hashed_outcome_blind_and_stress_is_deterministic(tmp_path):
    tape, digest = _write_sync_tape(tmp_path, [BASE_MS + 1_000])
    frozen = load_sync_degrade_events(
        mode="frozen_tape",
        tape_path=tape,
        expected_sha256=digest,
    )
    assert frozen.event_code == SYNC_EVENT_CODE
    assert frozen.promotion_eligible
    assert frozen.timestamps_ms.tolist() == [BASE_MS + 1_000]
    with pytest.raises(ValueError, match="environment does not match"):
        load_sync_degrade_events(
            mode="frozen_tape",
            tape_path=tape,
            expected_sha256=digest,
            expected_environment="local_macos",
        )
    with pytest.raises(ValueError, match="ends before the replay window"):
        load_sync_degrade_events(
            mode="frozen_tape",
            tape_path=tape,
            expected_sha256=digest,
            start_ts_ms=BASE_MS,
            end_ts_ms=BASE_MS + 86_400_001,
        )

    first = load_sync_degrade_events(
        mode="stress",
        tape_path=None,
        start_ts_ms=BASE_MS,
        end_ts_ms=BASE_MS + 20_000,
        stress_seed=17,
        stress_interval_s=5.0,
    )
    second = load_sync_degrade_events(
        mode="stress",
        tape_path=None,
        start_ts_ms=BASE_MS,
        end_ts_ms=BASE_MS + 20_000,
        stress_seed=17,
        stress_interval_s=5.0,
    )
    assert np.array_equal(first.timestamps_ms, second.timestamps_ms)
    assert not first.promotion_eligible

    payload = json.loads(tape.read_text(encoding="utf-8"))
    payload["events"][0]["terminal_pnl"] = -1.0
    tape.write_text(json.dumps(payload), encoding="utf-8")
    bad_digest = hashlib.sha256(tape.read_bytes()).hexdigest()
    with pytest.raises(ValueError, match="outcome/action fields"):
        load_sync_degrade_events(
            mode="frozen_tape",
            tape_path=tape,
            expected_sha256=bad_digest,
        )


def test_same_ms_market_event_precedes_sync_system_event():
    trades = _trades(
        [BASE_MS, BASE_MS + 1_000, BASE_MS + 2_000],
        [100.0, 99.0, 98.0],
        quantities=[0.001, 0.001, 0.001],
        maker_flags=[False, True, False],
    )
    events, n_execution = bt.build_replay_event_clock(
        trades,
        mode="merged",
        interval_ms=1_000,
        system_event_ts_ms=np.asarray([BASE_MS + 1_000], dtype=np.int64),
        system_event_code=SYNC_EVENT_CODE,
    )
    same_ms = events[events["transact_time"] == BASE_MS + 1_000]

    assert n_execution == 3
    assert same_ms["_is_execution_trade"].tolist() == [True, False]
    assert same_ms["is_buyer_maker"].tolist() == [1, SYNC_EVENT_CODE]


def test_sync_censor_prevents_future_mark_price_in_python_and_cpp(tmp_path):
    tape, digest = _write_sync_tape(tmp_path, [BASE_MS + 1_000])
    params = _base_params()
    params.update(
        {
            "initial_inventory": 0.001,
            "initial_entry_price": 100.0,
            "sync_adjust_degrade_enabled": True,
            "sync_adjust_replay_mode": "censor",
            "sync_adjust_event_tape_path": str(tape),
            "sync_adjust_event_tape_sha256": digest,
            "sync_adjust_event_environment": (
                "provider_neutral_test_environment"
            ),
            "sync_adjust_pause_s": 120.0,
            "sync_adjust_cancel_orders": True,
        }
    )
    trades = _trades(
        [BASE_MS, BASE_MS + 1_000, BASE_MS + 2_000],
        [100.0, 90.0, 500.0],
    )
    empty_i64 = np.empty(0, dtype=np.int64)
    empty_f64 = np.empty(0, dtype=np.float64)

    results = {
        engine: bt._simulate_tick_with_engine(
            engine,
            trades,
            empty_i64,
            empty_f64,
            params,
        )
        for engine in ("python", "cpp")
    }
    for result in results.values():
        assert result["sync_adjust_censored"]
        assert result["sync_adjust_censor_ts_ms"] == BASE_MS + 1_000
        assert result["terminal_mark_price"] == pytest.approx(90.0)
        assert result["pnl"] == pytest.approx(-0.01)


def test_frozen_sync_event_blocks_only_exposure_side_in_python_and_cpp(
    tmp_path,
):
    tape, digest = _write_sync_tape(tmp_path, [BASE_MS + 1_000])
    params = _base_params()
    params.update(
        {
            "initial_inventory": 0.001,
            "initial_entry_price": 100.0,
            "sync_adjust_degrade_enabled": True,
            "sync_adjust_replay_mode": "frozen_tape",
            "sync_adjust_event_tape_path": str(tape),
            "sync_adjust_event_tape_sha256": digest,
            "sync_adjust_event_environment": (
                "provider_neutral_test_environment"
            ),
            "sync_adjust_pause_s": 2.0,
            "sync_adjust_cancel_orders": True,
        }
    )
    trades = _trades(
        [BASE_MS, BASE_MS + 1_000, BASE_MS + 2_000, BASE_MS + 4_000],
        [100.0, 100.0, 100.0, 100.0],
    )
    empty_i64 = np.empty(0, dtype=np.int64)
    empty_f64 = np.empty(0, dtype=np.float64)
    py = bt._simulate_tick_with_engine(
        "python", trades, empty_i64, empty_f64, params
    )
    cpp = bt._simulate_tick_with_engine(
        "cpp", trades, empty_i64, empty_f64, params
    )

    for key in (
        "sync_adjust_degrade_trigger_count",
        "sync_adjust_degrade_block_bid_count",
        "sync_adjust_degrade_block_ask_count",
        "sync_adjust_degrade_until_ms",
        "fills_bid",
        "fills_ask",
    ):
        assert cpp[key] == py[key], key
    assert py["sync_adjust_degrade_trigger_count"] == 1
    assert py["sync_adjust_degrade_block_bid_count"] > 0
    assert py["sync_adjust_degrade_block_ask_count"] == 0
    assert py["sync_adjust_promotion_eligible"] is True


def test_consecutive_loss_cooldown_is_path_dependent_and_python_cpp_equal():
    params = _base_params()
    params.update(
        {
            "initial_inventory": 0.001,
            "initial_entry_price": 100.0,
            "circuit_breaker_sigma": 1.0,
            "circuit_breaker_exit_mode": "immediate_taker",
            "pnl_volatility_horizon_s": 1.0,
            "use_bar_pricing": False,
            "replay_clock_interval_ms": 1_000,
            "max_consecutive_losses": 1,
            "cooldown_after_loss": 30.0,
            "consecutive_loss_cooldown_semantics": LOSS_COOLDOWN_SEMANTICS,
        }
    )
    trades = _trades(
        [BASE_MS, BASE_MS + 1_000, BASE_MS + 2_000, BASE_MS + 35_000],
        [100.0, 90.0, 90.0, 90.0],
    )
    empty_i64 = np.empty(0, dtype=np.int64)
    empty_f64 = np.empty(0, dtype=np.float64)

    py = bt._simulate_tick_with_engine(
        "python", trades, empty_i64, empty_f64, params, bbo_data=_loss_test_bbo(trades)
    )
    cpp = bt._simulate_tick_with_engine(
        "cpp", trades, empty_i64, empty_f64, params, bbo_data=_loss_test_bbo(trades)
    )

    for key in (
        "consecutive_loss_cooldown_trigger_count",
        "consecutive_loss_cooldown_expiry_count",
        "consecutive_loss_round_trip_loss_count",
        "consecutive_loss_count_end",
        "consecutive_loss_count_max",
        "fills_bid",
        "fills_ask",
    ):
        assert cpp[key] == py[key], key
    assert py["consecutive_loss_cooldown_trigger_count"] == 1
    assert py["consecutive_loss_cooldown_expiry_count"] == 1
    assert py["consecutive_loss_round_trip_loss_count"] == 1
    assert cpp["pnl"] == pytest.approx(py["pnl"], abs=1e-12)
    assert cpp["consecutive_loss_cooldown_state"] == (
        py["consecutive_loss_cooldown_state"]
    )


def test_buy_q90_replays_cancel_pre_ack_fill_then_requires_new_placement(tmp_path):
    bundle_path, bundle_sha, policy_path, policy_sha = _write_q90_artifacts(
        tmp_path
    )
    params = _base_params()
    params.update(
        {
            "requote_interval": 100.0,
            "rq_min": 100.0,
            "rq_max": 100.0,
            "cancel_order_latency_ms": 500,
            "exchange_book_queue_mode": "strict",
            "dynamic_fill_hazard_shadow_enabled": True,
            "dynamic_fill_hazard_shadow_model_path": str(bundle_path),
            "dynamic_fill_hazard_shadow_model_sha256": bundle_sha,
            "dynamic_fill_hazard_action_enabled": True,
            "dynamic_fill_hazard_action_policy_path": str(policy_path),
            "dynamic_fill_hazard_action_policy_sha256": policy_sha,
            "dynamic_fill_hazard_shadow_exposure_ms": 100.0,
            "dynamic_fill_hazard_shadow_price_jump_ticks": 1.0,
            "initial_live_state": {
                "active_orders": [
                    {
                        "side": "BUY",
                        "price": 99.0,
                        "quantity": 0.002,
                        "remaining": 0.002,
                        "submit_ts_ms": BASE_MS + 100,
                        "event_ts_ms": BASE_MS + 200,
                        "status": "PENDING_NEW",
                        "mid_at_quote": 100.0,
                    }
                ]
            },
        }
    )
    trades = _trades(
        [BASE_MS + 200, BASE_MS + 300, BASE_MS + 800],
        [100.0, 99.0, 100.0],
        quantities=[0.0, 0.001, 0.0],
        maker_flags=[False, True, False],
    )

    result = bt.simulate_tick(
        trades,
        np.asarray([BASE_MS], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        exchange_book_event_tape=_native_q90_events(),
    )

    assert result["dynamic_fill_hazard_replay_authority"] == (
        "python_native_exchange_book"
    )
    assert result["dynamic_fill_hazard_cancel_request_count"] == 1
    assert result["dynamic_fill_hazard_pre_ack_fill_count"] == 1
    assert result["dynamic_fill_hazard_recovery_count"] == 1
    assert result["dynamic_fill_hazard_cancel_ack_count"] == 1
    assert result["dynamic_fill_hazard_reentry_count"] == 0
    assert result["dynamic_fill_hazard_post_cancel_recovery_count"] == 1
    assert result["dynamic_fill_hazard_hold_active_end"]
    assert result["dynamic_fill_hazard_hold_phase_end"] == (
        "POST_CANCEL_RECOVERY"
    )
    assert result["dynamic_fill_hazard_terminal_cursor_retention_end"] == 0
    lifecycle_journal = result[
        "_dynamic_fill_hazard_lifecycle_journal"
    ]
    assert [row["lifecycle_event"] for row in lifecycle_journal] == [
        "submit",
        "activate",
        "cancel_request",
        "partial_fill",
        "exchange_terminal",
        "post_cancel_recovery",
    ]
    assert lifecycle_journal[3]["phase_after"] == "CANCEL_PENDING"
    assert lifecycle_journal[3]["remaining_quantity_after"] == pytest.approx(
        0.001
    )
    assert lifecycle_journal[-2]["terminal_policy_route"] == (
        "PROSPECTIVE_CANCEL_REENTRY"
    )
    journal_audit = result[
        "_dynamic_fill_hazard_lifecycle_journal_audit"
    ]
    assert journal_audit["terminal_exchange_exposure_complete_count"] == 1
    assert journal_audit["unsupported_terminal_route_count"] == 0
    assert journal_audit["cpp_exposure_authority"] is False


def test_buy_q90_provider_visibility_clock_preserves_truth_and_cpp_parity(
    tmp_path,
):
    bundle_path, bundle_sha, policy_path, policy_sha = _write_q90_artifacts(
        tmp_path
    )
    common = _base_params()
    common.update(
        {
            "requote_interval": 100.0,
            "rq_min": 100.0,
            "rq_max": 100.0,
            "cancel_order_latency_ms": 500,
            "exchange_book_queue_mode": "strict",
            "dynamic_fill_hazard_shadow_enabled": True,
            "dynamic_fill_hazard_shadow_model_path": str(bundle_path),
            "dynamic_fill_hazard_shadow_model_sha256": bundle_sha,
            "dynamic_fill_hazard_action_enabled": True,
            "dynamic_fill_hazard_action_policy_path": str(policy_path),
            "dynamic_fill_hazard_action_policy_sha256": policy_sha,
            "dynamic_fill_hazard_shadow_exposure_ms": 100.0,
            "dynamic_fill_hazard_shadow_price_jump_ticks": 1.0,
            "dynamic_fill_hazard_action_application": "shadow",
            "initial_live_state": {
                "active_orders": [
                    {
                        "side": "BUY",
                        "price": 99.0,
                        "quantity": 0.002,
                        "remaining": 0.002,
                        "submit_ts_ms": BASE_MS + 100,
                        "event_ts_ms": BASE_MS + 100,
                        "status": "OPEN",
                        "mid_at_quote": 100.0,
                    }
                ]
            },
        }
    )
    trades = _trades(
        [BASE_MS + 200, BASE_MS + 300, BASE_MS + 800],
        [100.0, 99.0, 100.0],
        quantities=[0.0, 0.001, 0.0],
        maker_flags=[False, True, False],
    )
    legacy = bt.simulate_tick(
        trades,
        np.asarray([BASE_MS], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        dict(common),
        exchange_book_event_tape=_native_q90_events(),
    )
    causal_params = dict(common)
    causal_params.update(
        {
            "dynamic_fill_hazard_visibility_clock_mode": (
                "provider_receive_sensitivity"
            ),
            "dynamic_fill_hazard_provider_feature_latency_ms": 0.25,
            "dynamic_fill_hazard_provider_trade_delay_ms": 1.0,
            "dynamic_fill_hazard_action_application": "apply",
            "dynamic_fill_hazard_cpp_parity_enabled": True,
        }
    )
    causal_shadow_params = dict(causal_params)
    causal_shadow_params.update(
        {
            "dynamic_fill_hazard_action_application": "shadow",
            "dynamic_fill_hazard_cpp_parity_enabled": False,
        }
    )
    causal_shadow = bt.simulate_tick(
        trades,
        np.asarray([BASE_MS], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        causal_shadow_params,
        exchange_book_event_tape=_native_q90_events(),
    )
    causal = bt.simulate_tick(
        trades,
        np.asarray([BASE_MS], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        causal_params,
        exchange_book_event_tape=_native_q90_events(),
    )

    assert causal["dynamic_fill_hazard_replay_authority"] == (
        "python_exchange_truth_feature_ready_visibility"
    )
    assert causal["dynamic_fill_hazard_future_feature_time_count"] == 0
    assert causal["dynamic_fill_hazard_invalid_attribution_closed"] is True
    assert causal["dynamic_fill_hazard_cpp_parity_passed"] is True
    assert causal["dynamic_fill_hazard_cpp_mismatch_count"] == 0
    assert causal["dynamic_fill_hazard_visible_book_trade_tie_count"] == 0
    assert causal["exchange_book_events_consumed"] == legacy[
        "exchange_book_events_consumed"
    ]
    assert causal["exchange_book_events_accepted"] == legacy[
        "exchange_book_events_accepted"
    ]
    assert causal["dynamic_fill_hazard_truth_state_fingerprint"] == legacy[
        "dynamic_fill_hazard_truth_state_fingerprint"
    ]
    for key in (
        "fills_bid",
        "fills_ask",
        "final_inventory",
        "exchange_book_queue_cancel_ahead_event_count",
        "exchange_book_queue_cancel_ahead_qty",
    ):
        assert causal_shadow[key] == pytest.approx(legacy[key]), key
    assert causal_shadow["dynamic_fill_hazard_truth_state_fingerprint"] == (
        legacy["dynamic_fill_hazard_truth_state_fingerprint"]
    )
    assert causal["dynamic_fill_hazard_visibility_profile_identity"][
        "provider_clock_authority"
    ] is False


def test_buy_q90_aws_profile_visibility_uses_feature_ready_scheduler(tmp_path):
    bundle_path, bundle_sha, policy_path, policy_sha = _write_q90_artifacts(
        tmp_path
    )
    profile_path, profile_sha = _write_market_data_latency_profile(tmp_path)
    params = _base_params()
    params.update(
        {
            "requote_interval": 100.0,
            "rq_min": 100.0,
            "rq_max": 100.0,
            "exchange_book_queue_mode": "strict",
            "dynamic_fill_hazard_shadow_enabled": True,
            "dynamic_fill_hazard_shadow_model_path": str(bundle_path),
            "dynamic_fill_hazard_shadow_model_sha256": bundle_sha,
            "dynamic_fill_hazard_action_enabled": True,
            "dynamic_fill_hazard_action_policy_path": str(policy_path),
            "dynamic_fill_hazard_action_policy_sha256": policy_sha,
            "dynamic_fill_hazard_visibility_clock_mode": "aws_profile",
            "dynamic_fill_hazard_visibility_profile_path": str(profile_path),
            "dynamic_fill_hazard_visibility_profile_sha256": profile_sha,
            "dynamic_fill_hazard_visibility_profile_id": (
                "provider_neutral_test_visibility_v1"
            ),
            "dynamic_fill_hazard_visibility_profile_market_id": (
                "binance:perp:BTCUSDC"
            ),
            "dynamic_fill_hazard_visibility_profile_trade_market_id": (
                "binance:perp:BTCUSDC"
            ),
            "dynamic_fill_hazard_cpp_parity_enabled": True,
            "initial_live_state": {
                "active_orders": [
                    {
                        "side": "BUY",
                        "price": 99.0,
                        "quantity": 0.002,
                        "remaining": 0.002,
                        "submit_ts_ms": BASE_MS + 100,
                        "event_ts_ms": BASE_MS + 100,
                        "status": "OPEN",
                        "mid_at_quote": 100.0,
                    }
                ]
            },
        }
    )
    result = bt.simulate_tick(
        _trades(
            [BASE_MS + 200, BASE_MS + 300, BASE_MS + 800],
            [100.0, 99.0, 100.0],
            quantities=[0.0, 0.001, 0.0],
            maker_flags=[False, True, False],
        ),
        np.asarray([BASE_MS], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        exchange_book_event_tape=_native_q90_events(),
    )

    identity = result["dynamic_fill_hazard_visibility_profile_identity"]
    assert identity["profile_id"] == "provider_neutral_test_visibility_v1"
    assert identity["sha256"] == profile_sha
    assert identity["aws_profile_transport_sensitivity"] is True
    assert result["dynamic_fill_hazard_future_feature_time_count"] == 0
    assert result["dynamic_fill_hazard_cpp_parity_passed"] is True


def test_cpp_fails_closed_when_buy_q90_requires_native_depth():
    params = _base_params()
    params.update(
        {
            "dynamic_fill_hazard_action_enabled": True,
            "dynamic_fill_hazard_shadow_model_path": "model.json",
            "dynamic_fill_hazard_shadow_model_sha256": "a" * 64,
            "dynamic_fill_hazard_action_policy_path": "policy.json",
            "dynamic_fill_hazard_action_policy_sha256": "b" * 64,
        }
    )
    trades = _trades([BASE_MS, BASE_MS + 1_000], [100.0, 100.0])
    with pytest.raises(NotImplementedError, match="Python native"):
        bt._simulate_tick_cpp(
            trades,
            np.empty(0, dtype=np.int64),
            np.empty(0, dtype=np.float64),
            params,
        )


def test_sync_semantics_constants_are_frozen():
    assert SYNC_DEGRADE_SEMANTICS == "system_event_after_market_same_ms_v1"
    assert SYNC_CENSOR_CODE != SYNC_EVENT_CODE


@pytest.mark.parametrize("engine", ["python", "cpp"])
def test_hard_risk_restored_offset_and_peak_survive_utc_rollover(engine):
    midnight = (BASE_MS // 86_400_000 + 1) * 86_400_000
    params = _base_params()
    params.update({
        "max_daily_loss": 100.0,
        "emergency_close_dd": 100.0,
        "initial_live_state": {
            "risk_state": {
                "utc_day": midnight // 86_400_000 - 1,
                "day_start_total_pnl": 8.0,
                "session_peak_pnl": 20.0,
                "last_total_pnl": 10.0,
                "total_pnl_offset": 10.0,
            },
        },
    })
    result = bt._simulate_tick_with_engine(
        engine,
        _trades([midnight - 1_000, midnight, midnight + 1_000], [100.0] * 3),
        np.empty(0, dtype=np.int64), np.empty(0), params,
    )
    assert result["_final_risk_state"] == {
        "utc_day": midnight // 86_400_000,
        "day_start_total_pnl": 10.0,
        "session_peak_pnl": 20.0,
        "last_total_pnl": 10.0,
        "total_pnl_offset": 10.0,
    }
    assert result["risk_daily_loss_block_count"] == 0
    assert result["risk_emergency_close_count"] == 0


@pytest.mark.parametrize("engine", ["python", "cpp"])
@pytest.mark.parametrize("state, error", [
    ({}, "missing fields"),
    ([], "must be a mapping"),
    ({"utc_day": 0}, "missing fields"),
    ({"utc_day": 1.5, "day_start_total_pnl": 0.0, "session_peak_pnl": 0.0,
      "last_total_pnl": 0.0, "total_pnl_offset": 0.0}, "must be an integer"),
    ({"utc_day": 999_999, "day_start_total_pnl": 0.0, "session_peak_pnl": 0.0,
      "last_total_pnl": 0.0, "total_pnl_offset": 0.0}, "future UTC day"),
    ({"utc_day": 0, "day_start_total_pnl": 0.0, "session_peak_pnl": 0.0,
      "last_total_pnl": 0.0, "total_pnl_offset": float("nan")}, "must be finite"),
])
def test_explicit_risk_state_never_silently_defaults(engine, state, error):
    params = _base_params()
    params["initial_live_state"] = {"risk_state": state}
    with pytest.raises(ValueError, match=error):
        bt._simulate_tick_with_engine(
            engine, _trades([BASE_MS, BASE_MS + 1_000], [100.0, 100.0]),
            np.empty(0, dtype=np.int64), np.empty(0), params,
        )
