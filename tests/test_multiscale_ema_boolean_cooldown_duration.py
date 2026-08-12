from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from models import backtest_tick as bt
from research.families.f05_fill_quality_quote_ev.audit.multiscale_ema_boolean_cooldown_duration import (
    CROSS_AGE_MISSING_SENTINEL_S,
    BooleanEmaSurface,
    apply_predicates,
    atomic_predicate_dictionary,
    duration_seconds_to_milliseconds,
    ema_pairs,
    epoch_milliseconds_to_nanoseconds,
    pair_prefix,
    provider_pair_distance_scales,
)
from strategy.fill_cooldown import update_same_side_fill_units


def _surface_after_trend() -> BooleanEmaSurface:
    surface = BooleanEmaSurface((1.0, 2.0, 4.0))
    surface.update(ts_ns=0, price=100.0)
    for second, price in enumerate((101.0, 102.0, 103.0, 104.0), start=1):
        surface.update(ts_ns=second * 1_000_000_000, price=price)
    return surface


def _feature_row(surface: BooleanEmaSurface, *, side: str) -> dict[str, float | int]:
    ready = surface.feature_ready_ts_ns
    return surface.feature_row(
        side=side,
        causal_volatility_bps=2.0,
        decision_ts_ns=ready,
        volatility_ready_ts_ns=ready,
        snapshot_market_generation=4,
        snapshot_depth_generation=7,
        history_state_complete=True,
    )


def test_all_nonadjacent_pairs_are_exposed_and_side_signed() -> None:
    assert ema_pairs((1.0, 2.0, 4.0)) == (
        (1.0, 2.0),
        (1.0, 4.0),
        (2.0, 4.0),
    )
    surface = _surface_after_trend()
    buy = _feature_row(surface, side="BUY")
    sell = _feature_row(surface, side="SELL")
    prefix = pair_prefix(1.0, 4.0)
    assert buy[f"{prefix}_favorable_ordering"] == 1
    assert sell[f"{prefix}_adverse_ordering"] == 1
    assert buy[f"{prefix}_favorable_distance_bps"] == pytest.approx(
        -sell[f"{prefix}_favorable_distance_bps"]
    )


def test_cross_and_arrangement_clocks_are_causal() -> None:
    surface = _surface_after_trend()
    prefix = pair_prefix(1.0, 4.0)
    before = _feature_row(surface, side="BUY")
    assert before[f"{prefix}_cross_missing"] == 1
    assert before[f"{prefix}_cross_age_s"] == CROSS_AGE_MISSING_SENTINEL_S
    surface.update(ts_ns=20_000_000_000, price=90.0)
    after = _feature_row(surface, side="BUY")
    assert after[f"{prefix}_cross_missing"] == 0
    assert after[f"{prefix}_last_cross_adverse"] == 1
    assert after[f"{prefix}_cross_age_s"] == pytest.approx(0.0)
    surface.update(ts_ns=21_000_000_000, price=89.0)
    later = _feature_row(surface, side="BUY")
    assert later[f"{prefix}_cross_age_s"] == pytest.approx(1.0)
    assert later[f"{prefix}_arrangement_persistence_s"] == pytest.approx(1.0)


def test_provider_encoder_recovers_nonadjacent_pair_scale() -> None:
    names = (
        "ema_rel_mid_bps_h1s",
        "ema_rel_mid_bps_h2s",
        "ema_rel_mid_bps_h4s",
    )
    scales = provider_pair_distance_scales(
        feature_names=names,
        scale=np.ones(3),
        components=np.eye(3),
        eigenvalues=np.ones(3),
        half_lives_s=(1.0, 2.0, 4.0),
    )
    assert scales[pair_prefix(1.0, 4.0)] == pytest.approx(np.sqrt(2.0))


def test_atomic_dictionary_uses_all_pairs_and_not_simple_vote() -> None:
    surface = _surface_after_trend()
    row = _feature_row(surface, side="BUY")
    scales = {pair_prefix(*pair): 0.01 for pair in ema_pairs((1.0, 2.0, 4.0))}
    predicates = atomic_predicate_dictionary(
        pair_distance_scale_bps=scales,
        half_lives_s=(1.0, 2.0, 4.0),
    )
    matrix_row = apply_predicates(row, predicates)
    assert len(predicates) == 8 * 3
    assert matrix_row[f"{pair_prefix(1.0, 4.0)}:favorable"] == 1
    assert any(":cross_age_le_" in name for name in matrix_row)


def test_feature_row_rejects_future_or_incomplete_context() -> None:
    surface = _surface_after_trend()
    ready = surface.feature_ready_ts_ns
    with pytest.raises(ValueError, match="history state"):
        surface.feature_row(
            side="BUY",
            causal_volatility_bps=2.0,
            decision_ts_ns=ready,
            volatility_ready_ts_ns=ready,
            snapshot_market_generation=1,
            snapshot_depth_generation=1,
            history_state_complete=False,
        )
    with pytest.raises(ValueError, match="clocks"):
        surface.feature_row(
            side="BUY",
            causal_volatility_bps=2.0,
            decision_ts_ns=ready,
            volatility_ready_ts_ns=ready + 1,
            snapshot_market_generation=1,
            snapshot_depth_generation=1,
            history_state_complete=True,
        )


def test_duration_and_epoch_units_are_explicit() -> None:
    assert duration_seconds_to_milliseconds(79.0) == 79_000
    assert epoch_milliseconds_to_nanoseconds(1_700_000_000_123) == (
        1_700_000_000_123_000_000
    )
    with pytest.raises(ValueError):
        duration_seconds_to_milliseconds(0.0)
    with pytest.raises(ValueError):
        epoch_milliseconds_to_nanoseconds(-1)


def test_duration_control_counts_partial_and_overshoot_quantity_exactly() -> None:
    buy, sell, first = update_same_side_fill_units(
        side="BUY",
        fill_qty=0.0004,
        order_size=0.001,
        lot_size=0.001,
        buy_units=0.0,
        sell_units=3.0,
    )
    assert first == pytest.approx(0.4)
    assert buy == pytest.approx(0.4)
    assert sell == 0.0

    # A second partial at the same event time is still new quantity.
    buy, sell, second = update_same_side_fill_units(
        side="BUY",
        fill_qty=0.0006,
        order_size=0.001,
        lot_size=0.001,
        buy_units=buy,
        sell_units=sell,
    )
    assert second == pytest.approx(0.6)
    assert buy == pytest.approx(1.0)

    buy, sell, overshoot = update_same_side_fill_units(
        side="BUY",
        fill_qty=0.0014,
        order_size=0.001,
        lot_size=0.001,
        buy_units=buy,
        sell_units=sell,
    )
    assert overshoot == pytest.approx(1.4)
    assert buy == pytest.approx(2.4)

    buy, sell, _ = update_same_side_fill_units(
        side="SELL",
        fill_qty=0.0002,
        order_size=0.001,
        lot_size=0.001,
        buy_units=buy,
        sell_units=sell,
    )
    assert buy == 0.0
    assert sell == pytest.approx(0.2)


def _replay_params() -> dict[str, object]:
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
        "fill_cooldown": 2.0,
        "fill_cooldown_reducing": 0.0,
        "fill_cooldown_apply_reducing": False,
        "fill_cooldown_consecutive_reset_policy": "opposite_fill_only",
        "fill_cooldown_clock_mode": "wall_time",
        "replay_initial_state_mode": "fresh_start",
        "trace_cooldown_duration_opportunities_max": 100,
        "dynamic_fill_hazard_action_enabled": False,
        "buy_fill_selection_live_enabled": False,
    }


def _replay_path() -> pd.DataFrame:
    base_ms = 1_700_000_000_000
    return pd.DataFrame(
        {
            "transact_time": np.asarray(
                [base_ms + offset for offset in range(0, 9_000, 1_000)],
                dtype=np.int64,
            ),
            "price": np.asarray(
                [100.0, 96.6, 96.6, 90.0, 90.0, 90.0, 80.0, 80.0, 80.0],
                dtype=np.float64,
            ),
            "quantity": np.asarray(
                [0.0, 0.001, 0.0, 0.001, 0.0, 0.0, 0.001, 0.0, 0.0],
                dtype=np.float64,
            ),
            "is_buyer_maker": np.asarray(
                [False, True, False, True, False, False, True, False, False],
                dtype=np.uint8,
            ),
        }
    )


def _run_replay(params: dict[str, object]) -> dict[str, object]:
    return bt._simulate_tick_with_engine(
        "python",
        _replay_path(),
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        params,
    )


def test_fill_time_census_and_duration_override_are_separate_from_add_wait() -> None:
    census = _run_replay(_replay_params())
    opportunities = census["_cooldown_duration_opportunity_trace"]
    assert opportunities
    target = opportunities[0]
    assert target["role_at_fill"] in {"opener", "add"}
    assert target["baseline_duration_ms"] > 0.0
    assert target["fill_clock_semantics"] == (
        "native_exchange_event_revealed_at_replay_event_clock_"
        "no_live_receive_time_claim"
    )
    assert target["live_receive_time_authority"] is False

    params = _replay_params()
    params.update(
        {
            "trace_cooldown_duration_opportunities_max": 0,
            "cooldown_duration_fork_enabled": True,
            "cooldown_duration_fork_action": "FIXED_DURATION_MS",
            "cooldown_duration_fork_target_ordinal": target[
                "exposure_fill_ordinal"
            ],
            "cooldown_duration_fork_target_ts_ms": target[
                "fill_visible_ts_ms"
            ],
            "cooldown_duration_fork_target_side": target["side"],
            "cooldown_duration_fork_target_order_id": target["order_id"],
            "cooldown_duration_fork_target_campaign_id": target[
                "campaign_id"
            ],
            "cooldown_duration_fork_expected_baseline_ms": target[
                "baseline_duration_ms"
            ],
            "cooldown_duration_fork_fixed_ms": 500.0,
        }
    )
    candidate = _run_replay(params)
    trace = candidate["_cooldown_duration_fork_trace"]
    assert trace["action"] == "FIXED_DURATION_MS"
    assert trace["applied_duration_ms"] == pytest.approx(500.0)
    assert trace["baseline_duration_ms"] == pytest.approx(
        target["baseline_duration_ms"]
    )
    assert trace["assignment_ts_ms"] == target["fill_visible_ts_ms"]
    assert candidate["pnl"] != pytest.approx(census["pnl"])
    assert trace["right_censored"] is True
    assert trace["assignment_to_washout_value_usdc"] is None
    assert trace["censor_marks_are_terminal_bounds"] is False
    assert candidate["_ema_add_wait_fork_trace"] == {}


def test_disabled_duration_hook_is_a_path_noop() -> None:
    baseline = _run_replay(_replay_params())
    explicit = _replay_params()
    explicit["cooldown_duration_fork_enabled"] = False
    observed = _run_replay(explicit)
    for field in ("pnl", "fills_bid", "fills_ask", "max_inventory"):
        assert observed[field] == baseline[field]
    assert observed["_cooldown_duration_fork_trace"] == {}
