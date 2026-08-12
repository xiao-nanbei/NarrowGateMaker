from __future__ import annotations

import hashlib
import json
import math

import numpy as np
import pandas as pd
import pytest

from models import backtest_tick as bt
from models.tick_data_types import HistoricalExchangeBookEvent
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


def _write_sync_tape(tmp_path, timestamps: list[int]):
    path = tmp_path / "sync_degrade_events.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": SYNC_DEGRADE_TAPE_SCHEMA,
                "environment": "aws_tokyo_ec2_2vcpu_4g_amazon_linux",
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
                "profile_id": "aws_tokyo_test_visibility_v1",
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
                "aws_tokyo_ec2_2vcpu_4g_amazon_linux"
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
                "aws_tokyo_ec2_2vcpu_4g_amazon_linux"
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
            "position_timeout": 0.5,
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
        "python", trades, empty_i64, empty_f64, params
    )
    cpp = bt._simulate_tick_with_engine(
        "cpp", trades, empty_i64, empty_f64, params
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
                "aws_tokyo_test_visibility_v1"
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
    assert identity["profile_id"] == "aws_tokyo_test_visibility_v1"
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
