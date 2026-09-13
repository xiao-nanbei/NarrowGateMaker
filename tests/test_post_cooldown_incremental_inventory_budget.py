from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from models import backtest_tick as bt
from research.families.f09_campaign_action_uplift.audit.post_cooldown_incremental_inventory_budget import (
    PostCooldownIncrementalInventoryBudget,
    outcome_blind_budget_grid,
)

BASE_MS = 1_700_000_000_000


def test_reservations_prevent_overlapping_orders_from_overspending_budget() -> None:
    budget = PostCooldownIncrementalInventoryBudget("SELL", 2.0, 0.001)

    first = budget.reserve("a", 0.001, exposure_increasing=True)
    second = budget.reserve("b", 0.001, exposure_increasing=True)
    rejected = budget.reserve("c", 0.001, exposure_increasing=True)

    assert first.allowed and second.allowed
    assert not rejected.allowed
    assert budget.reserved_units == pytest.approx(2.0)
    assert budget.available_units == pytest.approx(0.0)


def test_partial_fill_transfers_reservation_to_consumed_and_ack_releases_rest() -> None:
    budget = PostCooldownIncrementalInventoryBudget("SELL", 2.0, 0.001)
    budget.reserve("a", 0.002, exposure_increasing=True)

    budget.fill("a", 0.0005, exposure_increasing=True)
    assert budget.consumed_units == pytest.approx(0.5)
    assert budget.reserved_units == pytest.approx(1.5)
    assert budget.available_units == pytest.approx(0.0)

    assert budget.release("a") == pytest.approx(1.5)
    assert budget.available_units == pytest.approx(1.5)

    snapshot = budget.snapshot()
    assert snapshot["consumed_units"] == pytest.approx(0.5)
    assert snapshot["reserved_units"] == pytest.approx(0.0)
    assert snapshot["reserved_order_count"] == 0


def test_prepared_reservation_binds_to_actual_order_without_changing_units() -> None:
    budget = PostCooldownIncrementalInventoryBudget("SELL", 2.0, 0.001)
    budget.reserve("prepared:SELL:0", 0.001, exposure_increasing=True)

    assert budget.rename_reservation("prepared:SELL:0", 42) == pytest.approx(1.0)
    assert budget.reservation_units(42) == pytest.approx(1.0)
    assert budget.reservation_units("prepared:SELL:0") == 0.0
    assert budget.reserved_units == pytest.approx(1.0)


def test_reducing_orders_do_not_touch_incremental_inventory_budget() -> None:
    budget = PostCooldownIncrementalInventoryBudget("SELL", 1.0, 0.001)

    decision = budget.reserve("reduce", 0.001, exposure_increasing=False)
    budget.fill("reduce", 0.001, exposure_increasing=False)

    assert decision.reason == "reducing_bypass"
    assert budget.consumed_units == 0.0
    assert budget.reserved_units == 0.0
    assert budget.available_units == 1.0


def test_unlimited_control_preserves_reservation_accounting() -> None:
    budget = PostCooldownIncrementalInventoryBudget("BUY", math.inf, 0.001)
    for index in range(5):
        assert budget.reserve(index, 0.001, exposure_increasing=True).allowed

    assert budget.reserved_units == pytest.approx(5.0)
    assert math.isinf(budget.available_units)


def test_outcome_blind_grid_requires_distinct_nonzero_mechanics_support() -> None:
    assert outcome_blind_budget_grid([1, 1, 2, 2, 3, 4]) == (1, 2, 3)
    assert outcome_blind_budget_grid([1, 1, 1]) == (1,)
    assert outcome_blind_budget_grid([]) == ()


def _replay_params(
    budget_units: float,
    *,
    enabled: bool = True,
    target_side: str = "BOTH",
) -> dict[str, object]:
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
        "fill_cooldown": 1.0,
        "fill_cooldown_reducing": 0.0,
        "fill_cooldown_apply_reducing": False,
        "fill_cooldown_consecutive_reset_policy": "opposite_fill_only",
        "fill_cooldown_clock_mode": "wall_time",
        "decision_trace_profile": "mechanics_only",
        "trace_decisions_max": 100_000,
        "trace_quotes_max": 100_000,
        "replay_initial_state_mode": "fresh_start",
        "post_cooldown_incremental_inventory_budget_enabled": enabled,
        "post_cooldown_incremental_inventory_budget_units": budget_units,
        "post_cooldown_incremental_inventory_budget_target_side": target_side,
        "post_cooldown_incremental_inventory_budget_arm_id": (
            "control_infinity" if math.isinf(budget_units) else "budget_1_units"
        ),
        "trace_post_cooldown_incremental_inventory_budget_max": 100,
    }


def _replay_path() -> pd.DataFrame:
    offsets = list(range(0, 9_000, 1_000))
    return pd.DataFrame(
        {
            "transact_time": np.asarray(
                [BASE_MS + offset for offset in offsets],
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


def _run_replay(budget_units: float) -> dict[str, object]:
    return bt._simulate_tick_with_engine(
        "python",
        _replay_path(),
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        _replay_params(budget_units),
    )


def _mechanics_signature(result: dict[str, object]) -> tuple[object, ...]:
    quote_rows = tuple(
        (
            row.get("side"),
            row.get("submit_ts"),
            row.get("activate_ts"),
            row.get("outcome_ts"),
            row.get("outcome"),
            row.get("price"),
            row.get("quantity"),
            row.get("fill_qty"),
            row.get("remaining"),
        )
        for row in result.get("_quote_trace", ())
    )
    return (
        result["fills_bid"],
        result["fills_ask"],
        result["quote_attempts"],
        result["n_requotes"],
        quote_rows,
    )


def test_full_path_budget_regenerates_orders_and_prevents_overshoot() -> None:
    control = _run_replay(math.inf)
    candidate = _run_replay(1.0)

    assert control["fills_bid"] > candidate["fills_bid"]
    assert candidate[
        "post_cooldown_incremental_inventory_budget_conservation_failures"
    ] == 0
    trace = [
        row
        for row in candidate[
            "_post_cooldown_incremental_inventory_budget_trace"
        ]
        if int(row.get("supported", 0)) == 1
    ]
    assert len(trace) == 1
    row = trace[0]
    assert row["side"] == "BUY"
    assert row["consumed_units"] == pytest.approx(1.0)
    assert row["blocked_submission_count"] > 0
    assert row["one_order_overshoot_count"] == 0
    assert row["final_quote_action_changed"] == 1
    assert row["terminal_reason"] == "day_end_censor"
    for forbidden in ("pnl", "reward", "markout", "toxicity"):
        assert forbidden not in row


def test_full_path_budget_is_python_authoritative_and_mechanics_only() -> None:
    params = _replay_params(1.0)
    with pytest.raises(NotImplementedError, match="Python-authoritative"):
        bt._simulate_tick_with_engine(
            "cpp",
            _replay_path(),
            np.empty(0, dtype=np.int64),
            np.empty(0, dtype=np.float64),
            params,
        )

    params["decision_trace_profile"] = "full"
    with pytest.raises(ValueError, match="mechanics-only"):
        bt._simulate_tick_with_engine(
            "python",
            _replay_path(),
            np.empty(0, dtype=np.int64),
            np.empty(0, dtype=np.float64),
            params,
        )


def test_infinite_budget_control_reproduces_disabled_baseline_path() -> None:
    disabled_params = _replay_params(math.inf, enabled=False)
    disabled = bt._simulate_tick_with_engine(
        "python",
        _replay_path(),
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        disabled_params,
    )
    control = _run_replay(math.inf)

    assert _mechanics_signature(control) == _mechanics_signature(disabled)
    assert control[
        "post_cooldown_incremental_inventory_budget_conservation_failures"
    ] == 0


def test_finite_budget_requires_whole_units_and_can_target_one_side() -> None:
    fractional = _replay_params(1.5)
    with pytest.raises(ValueError, match="whole planned fill units"):
        bt._simulate_tick_with_engine(
            "python",
            _replay_path(),
            np.empty(0, dtype=np.int64),
            np.empty(0, dtype=np.float64),
            fractional,
        )

    sell_only = bt._simulate_tick_with_engine(
        "python",
        _replay_path(),
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        _replay_params(1.0, target_side="SELL"),
    )
    assert sell_only[
        "post_cooldown_incremental_inventory_budget_target_side"
    ] == "SELL"
    assert all(
        row["side"] == "SELL"
        for row in sell_only["_post_cooldown_incremental_inventory_budget_trace"]
    )
