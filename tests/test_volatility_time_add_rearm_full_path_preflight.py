from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from models import backtest_tick as bt
from research.families.f09_campaign_action_uplift.audit import (
    volatility_time_add_rearm_full_path_preflight as preflight,
)
from strategy.fill_cooldown import (
    LINEAGE_CANDIDATE_ACTION,
    LINEAGE_CONTROL_ACTION,
)

BASE_MS = 1_700_000_000_000


def _params() -> dict[str, object]:
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
        "fill_cooldown": 85.0,
        "fill_cooldown_reducing": 0.0,
        "fill_cooldown_apply_reducing": False,
        "fill_cooldown_consecutive_reset_policy": "opposite_fill_only",
        "trace_decisions_max": 100_000,
        "trace_quotes_max": 100_000,
        "decision_trace_profile": "mechanics_only",
    }


def _market_path() -> pd.DataFrame:
    offsets = list(range(0, 11_000, 1_000))
    prices = [100.0, 96.6, *([96.6] * 8), 90.0]
    quantities = [0.0, 0.001, *([0.0] * 8), 0.001]
    maker_flags = [False, True, *([False] * 8), True]
    return pd.DataFrame(
        {
            "transact_time": np.asarray([BASE_MS + offset for offset in offsets], dtype=np.int64),
            "price": np.asarray(prices, dtype=np.float64),
            "quantity": np.asarray(quantities, dtype=np.float64),
            "is_buyer_maker": np.asarray(maker_flags, dtype=np.uint8),
        }
    )


def _variance_data() -> dict[str, np.ndarray]:
    return {
        "feature_ready_ts_ms": np.asarray([BASE_MS], dtype=np.int64),
        "rate_bps2_per_s": np.asarray([100.0], dtype=np.float64),
        "valid": np.asarray([True], dtype=np.bool_),
    }


def _candidate_params() -> dict[str, object]:
    params = _params()
    params.update(
        {
            "fill_cooldown_clock_mode": "variance_time",
            "variance_time_reference_rate_buy_bps2_per_s": 1.0,
            "variance_time_reference_rate_sell_bps2_per_s": 1.0,
            "variance_time_minimum_wall_time_ms": 5_000,
            "variance_time_maximum_wall_time_ms": 600_000,
            "variance_time_max_feature_age_ms": 2_000,
        }
    )
    return params


def _randomized_lineage_params(seed: int) -> dict[str, object]:
    params = _candidate_params()
    params.update(
        {
            "fill_cooldown_clock_mode": "randomized_lineage",
            "variance_time_lineage_randomized_enabled": True,
            "trace_variance_time_lineage_max": 100,
            "variance_time_lineage_seed": int(seed),
            "variance_time_lineage_probabilities": {
                LINEAGE_CONTROL_ACTION: 0.5,
                LINEAGE_CANDIDATE_ACTION: 0.5,
            },
            "variance_time_lineage_markout_max_age_ms": 2_000,
            "variance_time_lineage_fail_on_q90_pre_ack_fill": True,
        }
    )
    return params


def _run(params: dict[str, object], *, variance_time_data=None):
    return bt._simulate_tick_with_engine(
        "python",
        _market_path(),
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        params,
        variance_time_data=variance_time_data,
    )


def _mechanics_signature(result: dict[str, object]) -> tuple[object, ...]:
    decisions = tuple(
        (
            row["ts_ms"],
            row["side"],
            row["action"],
            row["allow_post"],
            row["reason_text"],
            row["final_price"],
        )
        for row in result["_decision_trace"]
    )
    orders = tuple(
        (
            row["order_id"],
            row["side"],
            row["submit_ts"],
            row["price"],
            row.get("outcome"),
            row.get("outcome_ts"),
            row.get("cancel_reason"),
        )
        for row in result["_quote_trace"]
    )
    return (
        result["fills_bid"],
        result["fills_ask"],
        result["quote_attempts"],
        decisions,
        orders,
    )


def test_explicit_wall_time_control_reproduces_legacy_default() -> None:
    legacy = _run(_params())
    explicit_params = _params()
    explicit_params["fill_cooldown_clock_mode"] = "wall_time"
    explicit = _run(explicit_params)

    assert _mechanics_signature(explicit) == _mechanics_signature(legacy)


def test_variance_time_rearm_regenerates_downstream_orders_and_fills() -> None:
    control = _run(_params())
    candidate = _run(_candidate_params(), variance_time_data=_variance_data())

    assert control["fills_bid"] == 1
    assert candidate["fills_bid"] == 2
    control_fills = [row for row in control["_quote_trace"] if row.get("outcome") == "fill"]
    candidate_fills = [row for row in candidate["_quote_trace"] if row.get("outcome") == "fill"]
    assert len(control_fills) == 1
    assert len(candidate_fills) == 2
    assert candidate_fills[-1]["outcome_ts"] == BASE_MS + 10_000
    assert candidate_fills[-1]["price"] == pytest.approx(93.2)

    buy_rows = {
        int(row["ts_ms"]): row for row in candidate["_decision_trace"] if row["side"] == "BUY"
    }
    release = buy_rows[BASE_MS + 6_000]
    assert release["baseline_wall_fill_cooldown_active"] == 1
    assert release["effective_fill_cooldown_active"] == 0
    assert release["variance_time_mechanical_diff_vs_wall"] == 1
    assert release["allow_post"] == 1
    assert release["action"] == "place"
    assert release["variance_time_release_reason"] == "variance_budget"

    for forbidden in (
        "campaign_pnl_so_far",
        "campaign_adverse_excursion_so_far",
        "toxicity",
        "markout_ema",
    ):
        assert forbidden not in release


def test_randomized_lineage_assignment_precedes_and_regenerates_path() -> None:
    candidate = _run(
        _randomized_lineage_params(1),
        variance_time_data=_variance_data(),
    )
    control = _run(
        _randomized_lineage_params(2),
        variance_time_data=_variance_data(),
    )

    candidate_rows = candidate["_variance_time_lineage_trace"]
    control_rows = control["_variance_time_lineage_trace"]
    assert len(candidate_rows) == len(control_rows) == 1
    candidate_row = candidate_rows[0]
    control_row = control_rows[0]
    assert candidate_row["action"] == LINEAGE_CANDIDATE_ACTION
    assert control_row["action"] == LINEAGE_CONTROL_ACTION
    assert candidate_row["behavior_propensity"] == pytest.approx(0.5)
    assert candidate_row["assignment_before_downstream_path"] == 1
    assert candidate_row["assignment_fixed_within_lineage"] == 1
    assert candidate_row["trigger_fill_excluded_from_reward"] == 1
    assert candidate_row["actual_final_action_change_count"] > 0
    assert control_row["actual_final_action_change_count"] == 0
    assert candidate["fills_bid"] == 2
    assert control["fills_bid"] == 1
    assert candidate_row["intervention_fill_count"] == 1
    assert control_row["intervention_fill_count"] == 0
    assert candidate_row["reward_identity_error"] == pytest.approx(0.0)
    assert not candidate["variance_time_lineage_full_cpp_tick_replay_authority"]


def test_opposite_fill_terminates_one_assignment_without_rerandomizing() -> None:
    offsets = list(range(0, 21_000, 1_000))
    trades = pd.DataFrame(
        {
            "transact_time": np.asarray(
                [BASE_MS + offset for offset in offsets], dtype=np.int64
            ),
            "price": np.asarray(
                [100.0, 96.6, *([96.6] * 8), 90.0, *([90.0] * 9), 110.0],
                dtype=np.float64,
            ),
            "quantity": np.asarray(
                [0.0, 0.001, *([0.0] * 8), 0.001, *([0.0] * 9), 0.001],
                dtype=np.float64,
            ),
            "is_buyer_maker": np.asarray(
                [False, True, *([False] * 8), True, *([False] * 9), False],
                dtype=np.uint8,
            ),
        }
    )
    result = bt._simulate_tick_with_engine(
        "python",
        trades,
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        _randomized_lineage_params(1),
        variance_time_data=_variance_data(),
    )

    rows = result["_variance_time_lineage_trace"]
    assert len(rows) == 1
    row = rows[0]
    assert row["action"] == LINEAGE_CANDIDATE_ACTION
    assert row["lineage_terminal_reason"] == "opposite_side_fill"
    assert row["lineage_censored"] == 0
    assert row["intervention_fill_count"] == 2
    assert result["variance_time_lineage_candidate_assignments"] == 1
    assert result["variance_time_lineage_control_assignments"] == 0


def test_variance_time_full_path_fails_closed_on_contract_drift() -> None:
    params = _candidate_params()
    params["fill_cooldown_consecutive_reset_policy"] = "opposite_fill_or_expiry"
    with pytest.raises(ValueError, match="opposite_fill_only"):
        _run(params, variance_time_data=_variance_data())

    with pytest.raises(NotImplementedError, match="Python-authoritative"):
        bt._simulate_tick_with_engine(
            "cpp",
            _market_path(),
            np.empty(0, dtype=np.int64),
            np.empty(0, dtype=np.float64),
            _candidate_params(),
            variance_time_data=_variance_data(),
        )


def _decision_row(
    ts_ms: int,
    *,
    side: str = "BUY",
    action: str = "pause",
    allow_post: int = 0,
    reason_text: str = "fill_cd",
    wall_active: int = 1,
    effective_active: int = 0,
    mechanical_diff: int = 1,
    baseline_ready_ts_ms: int = BASE_MS + 85_000,
    candidate_ready_ts_ms: int = BASE_MS + 20_000,
) -> dict[str, object]:
    return {
        "ts_ms": ts_ms,
        "side": side,
        "action": action,
        "allow_post": allow_post,
        "allow_exposure_increase": allow_post,
        "exposure_increasing": 1,
        "reason_text": reason_text,
        "final_price": 99.0,
        "final_size": 0.001,
        "needs_update": 1,
        "order_active_before": 0,
        "last_side_fill_ts_ms": BASE_MS,
        "fill_cooldown_consecutive_units": 1.0,
        "baseline_wall_fill_cooldown_active": wall_active,
        "effective_fill_cooldown_active": effective_active,
        "variance_time_mechanical_diff_vs_wall": mechanical_diff,
        "variance_time_baseline_ready_ts_ms": baseline_ready_ts_ms,
        "variance_time_candidate_ready_ts_ms": candidate_ready_ts_ms,
        "variance_time_release_reason": "variance_budget",
    }


def test_episode_denominator_counts_lineage_once_and_attributes_blocker() -> None:
    control = [
        _decision_row(BASE_MS + 20_000),
        _decision_row(BASE_MS + 25_000),
    ]
    candidate = [
        _decision_row(
            BASE_MS + 20_000,
            action="pause",
            reason_text="dynamic_fill_hazard_q90",
        ),
        _decision_row(
            BASE_MS + 25_000,
            action="place",
            allow_post=1,
            reason_text="none",
        ),
    ]

    episodes, blockers = preflight.compare_decision_paths(
        "2026-04-17",
        control,
        candidate,
        material_delta_ms=5_000,
    )

    assert len(episodes) == 1
    row = episodes.iloc[0]
    assert row["direction"] == "earlier_ready"
    assert bool(row["material_timing_change"])
    assert bool(row["unmasked_action_effective"])
    assert bool(row["final_quote_action_changed"])
    assert row["binding_blockers"] == "dynamic_fill_hazard_q90"
    assert row["first_action_change_ts_ms"] == BASE_MS + 25_000
    assert row["action_change_authority"] == "candidate_final_gate_stack"
    assert blockers["blocker"].tolist() == ["dynamic_fill_hazard_q90"]


def test_later_rearm_is_attributed_to_variance_clock() -> None:
    control = [
        _decision_row(
            BASE_MS + 85_000,
            action="place",
            allow_post=1,
            reason_text="none",
            wall_active=0,
            effective_active=0,
            mechanical_diff=0,
        )
    ]
    candidate = [
        _decision_row(
            BASE_MS + 85_000,
            action="pause",
            reason_text="fill_cd|defense",
            wall_active=0,
            effective_active=1,
            mechanical_diff=1,
            candidate_ready_ts_ms=0,
        )
    ]

    episodes, blockers = preflight.compare_decision_paths(
        "2026-04-17", control, candidate, material_delta_ms=5_000
    )

    assert episodes.iloc[0]["direction"] == "later_ready"
    assert not bool(episodes.iloc[0]["unmasked_action_effective"])
    assert episodes.iloc[0]["binding_blockers"] == ("variance_time_fill_cd|defense")
    assert blockers["blocker"].tolist() == [
        "variance_time_fill_cd",
        "defense",
    ]


def test_later_rearm_is_unmasked_when_fill_cd_is_the_only_final_blocker() -> None:
    control = [
        _decision_row(
            BASE_MS + 85_000,
            action="place",
            allow_post=1,
            reason_text="none",
            wall_active=0,
            effective_active=0,
            mechanical_diff=0,
        )
    ]
    candidate = [
        _decision_row(
            BASE_MS + 85_000,
            action="pause",
            reason_text="fill_cd",
            wall_active=0,
            effective_active=1,
            mechanical_diff=1,
            candidate_ready_ts_ms=0,
        ),
        _decision_row(
            BASE_MS + 120_000,
            action="place",
            allow_post=1,
            reason_text="none",
            wall_active=0,
            effective_active=0,
            mechanical_diff=0,
            candidate_ready_ts_ms=BASE_MS + 120_000,
        ),
    ]

    episodes, blockers = preflight.compare_decision_paths(
        "2026-04-17", control, candidate, material_delta_ms=5_000
    )

    assert bool(episodes.iloc[0]["unmasked_action_effective"])
    assert episodes.iloc[0]["binding_blockers"] == "variance_time_fill_cd"
    assert bool(episodes.iloc[0]["timing_delta_observed"])
    assert episodes.iloc[0]["timing_delta_ms"] == 35_000
    assert blockers["blocker"].tolist() == ["variance_time_fill_cd"]


def test_mechanics_trace_rejects_economic_fields() -> None:
    control = [_decision_row(BASE_MS + 20_000)]
    candidate = [_decision_row(BASE_MS + 20_000)]
    candidate[0]["campaign_pnl_so_far"] = 1.0
    with pytest.raises(ValueError, match="forbidden economic fields"):
        preflight.compare_decision_paths("2026-04-17", control, candidate, material_delta_ms=5_000)


def test_order_path_difference_uses_mechanics_only_multisets() -> None:
    shared = {
        "side": "BUY",
        "submit_ts": BASE_MS,
        "price": 99.0,
        "outcome": "fill",
        "outcome_ts": BASE_MS + 1_000,
        "fill_qty": 0.001,
    }
    candidate_only = {
        "side": "BUY",
        "submit_ts": BASE_MS + 5_000,
        "price": 98.0,
        "outcome": "fill",
        "outcome_ts": BASE_MS + 10_000,
        "fill_qty": 0.001,
    }
    summary = preflight.compare_order_outcomes([shared], [shared, candidate_only])
    assert summary["candidate_only_order_outcomes"] == 1
    assert summary["control_only_order_outcomes"] == 0
    assert summary["candidate_only_fill_events"] == 1
    assert summary["control_only_fill_events"] == 0


def test_operational_order_state_does_not_treat_missing_decision_as_action() -> None:
    control = [_decision_row(BASE_MS + 10_000, mechanical_diff=0)]
    candidate = [_decision_row(BASE_MS + 20_000)]
    shared_order = {
        "order_id": 1,
        "side": "BUY",
        "submit_ts": BASE_MS,
        "activate_ts": BASE_MS + 100,
        "outcome_ts": BASE_MS + 90_000,
        "outcome": "cancel_ack",
        "price": 99.0,
        "quantity": 0.001,
        "remaining": 0.001,
        "reduce_only": False,
    }

    episodes, _ = preflight.compare_decision_paths(
        "2026-04-17",
        control,
        candidate,
        material_delta_ms=5_000,
        control_order_rows=[shared_order],
        candidate_order_rows=[shared_order],
    )

    assert not bool(episodes.iloc[0]["control_decision_aligned"])
    assert not bool(episodes.iloc[0]["unmasked_action_effective"])
    assert not bool(episodes.iloc[0]["final_quote_action_changed"])
    assert episodes.iloc[0]["action_change_authority"] == ("candidate_final_gate_stack")
    assert not bool(episodes.iloc[0]["regenerated_operational_path_diff"])


def test_operational_order_state_detects_regenerated_exposure() -> None:
    control = [_decision_row(BASE_MS + 20_000)]
    candidate = [_decision_row(BASE_MS + 20_000)]
    control_order = {
        "order_id": 1,
        "side": "BUY",
        "submit_ts": BASE_MS,
        "activate_ts": BASE_MS + 100,
        "outcome_ts": BASE_MS + 90_000,
        "outcome": "cancel_ack",
        "price": 99.0,
        "quantity": 0.001,
        "remaining": 0.001,
        "reduce_only": False,
    }
    candidate_order = dict(control_order, order_id=2, price=98.9)

    episodes, _ = preflight.compare_decision_paths(
        "2026-04-17",
        control,
        candidate,
        material_delta_ms=5_000,
        control_order_rows=[control_order],
        candidate_order_rows=[candidate_order],
    )

    assert not bool(episodes.iloc[0]["unmasked_action_effective"])
    assert not bool(episodes.iloc[0]["final_quote_action_changed"])
    assert not bool(episodes.iloc[0]["changed_quote_opportunity"])
    assert bool(episodes.iloc[0]["regenerated_operational_path_diff"])
