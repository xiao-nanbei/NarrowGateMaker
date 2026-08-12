from __future__ import annotations

import pandas as pd

from research.families.f06_placement_fill_cif.audit.full_curve_fill_cif import (
    ACTION_SOURCE_SUFFIXES,
    STATIC_SOURCE_COLUMNS,
)
from research.families.f06_placement_fill_cif.audit.paired_order_lifecycle import ACTION_ORDER
from research.families.f06_placement_fill_cif.audit.request_state_panel import (
    REQUEST_ACTION_SUFFIXES,
    _expand_request_actions,
    _phase_frame,
)


def _paired_wide_row() -> pd.DataFrame:
    row: dict[str, object] = {name: 0.0 for name in STATIC_SOURCE_COLUMNS}
    row.update(
        {
            "cohort_id": "cohort-1",
            "decision_id": "decision-1",
            "day": "2026-04-13",
            "side": "BUY",
            "inventory_role": "opener",
            "campaign_id": 7,
            "cancel_request_reason": "requote_replace",
            "quantity": 0.001,
            "baseline_price_tick": 990,
            "submit_ts_ns": 1_000_000_000,
            "feature_ready_ts_ns": 900_000_000,
            "observation_end_ts_ns": 10_000_000_000,
            "best_bid": 100.0,
            "best_ask": 100.1,
            "monotonicity_violation_count": 0,
        }
    )
    lifecycle = {
        "closer_1tick": {
            "price_tick": 991,
            "first_fill_ts_ns": 4_000_000_000,
            "terminal_ts_ns": 4_000_000_000,
            "terminal_reason": "strict_through",
            "cancel_acked": 0,
            "first_pending_cancel_fill_ts_ns": 0,
            "fill_while_cancel_pending_qty": 0.0,
            "request_queue_left": 1.0,
        },
        "current": {
            "price_tick": 990,
            "first_fill_ts_ns": 5_500_000_000,
            "terminal_ts_ns": 5_500_000_000,
            "terminal_reason": "strict_through",
            "cancel_acked": 0,
            "first_pending_cancel_fill_ts_ns": 5_500_000_000,
            "fill_while_cancel_pending_qty": 0.001,
            "request_queue_left": 2.0,
        },
        "farther_1tick": {
            "price_tick": 989,
            "first_fill_ts_ns": 0,
            "terminal_ts_ns": 6_000_000_000,
            "terminal_reason": "cancel_ack",
            "cancel_acked": 1,
            "first_pending_cancel_fill_ts_ns": 0,
            "fill_while_cancel_pending_qty": 0.0,
            "request_queue_left": 3.0,
        },
    }
    for action in ACTION_ORDER:
        values: dict[str, object] = {
            suffix: 0 for suffix in ACTION_SOURCE_SUFFIXES
        }
        values.update(
            {
                "price_tick": lifecycle[action]["price_tick"],
                "activation_ts_ns": 2_000_000_000,
                "activation_status": "active",
                "first_fill_ts_ns": lifecycle[action]["first_fill_ts_ns"],
                "cancel_request_ts_ns": 5_000_000_000,
                "cancel_ack_ts_ns": 6_000_000_000,
                "terminal_ts_ns": lifecycle[action]["terminal_ts_ns"],
                "terminal_reason": lifecycle[action]["terminal_reason"],
                "terminal_observed": 1,
            }
        )
        for suffix, value in values.items():
            row[f"{action}__{suffix}"] = value
        request_values: dict[str, object] = {
            suffix: 0 for suffix in REQUEST_ACTION_SUFFIXES
        }
        request_values.update(
            {
                "cancel_acked": lifecycle[action]["cancel_acked"],
                "fill_while_cancel_pending_qty": lifecycle[action][
                    "fill_while_cancel_pending_qty"
                ],
                "first_pending_cancel_fill_ts_ns": lifecycle[action][
                    "first_pending_cancel_fill_ts_ns"
                ],
                "request_state_observed": 1,
                "request_order_state_before": "open",
                "request_order_age_ms": 3_000.0,
                "request_remaining_qty": 0.001,
                "request_queue_left": lifecycle[action]["request_queue_left"],
                "request_queue_path_valid": 1,
            }
        )
        for suffix, value in request_values.items():
            row[f"{action}__{suffix}"] = value
    return pd.DataFrame([row])


def test_request_panel_expands_every_frozen_action() -> None:
    actions = _expand_request_actions(_paired_wide_row())
    assert actions["action"].tolist() == list(ACTION_ORDER)
    assert actions["distance_ticks"].tolist() == [9.0, 10.0, 11.0]
    assert actions["request_queue_left"].tolist() == [1.0, 2.0, 3.0]


def test_phase_labels_separate_pre_pending_and_ack() -> None:
    actions = _expand_request_actions(_paired_wide_row())
    segments = pd.DataFrame(
        {
            "segment_id": [1],
            "start_ts_ms": [0],
            "end_ts_ms_exclusive": [20_000],
        }
    )
    phase = _phase_frame(actions, segments)
    assert phase["pre_request_first_fill"].tolist() == [1, 0, 0]
    assert phase["request_risk_set"].tolist() == [0, 1, 1]
    assert phase["pending_cancel_fill"].tolist() == [0, 1, 0]
    assert phase["cancel_ack_observed"].tolist() == [0, 0, 1]
