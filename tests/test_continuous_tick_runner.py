from pathlib import Path

import pytest

from models.replay.continuous_calendar import CalendarReplayPlan, ReplayMode
from models.replay.continuous_tick_runner import (
    assert_planned_shutdown_drained,
    build_active_segments,
    expected_segment_local_pnl_delta,
    requires_new_campaign_id,
)

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = (
    ROOT
    / "research/shared/replay_lifecycle/docs/"
    "calendar_continuity_manifest_20260417_20260730_v1.json"
)


def test_frozen_anchor_plan_builds_single_day_drained_segments():
    plan = CalendarReplayPlan.from_manifest(
        MANIFEST,
        mode=ReplayMode.ANCHOR_PANEL_CONTINUOUS,
    )
    rows = build_active_segments(plan, cancel_drain_ms=2_000)

    assert len(rows) == 66
    assert {row.day for row in rows} == set(plan.active_days)
    assert sum(row.terminal_censor for row in rows) == 1
    assert all(
        row.terminal_censor
        or row.start_ts_ms < row.planned_quote_stop_ts_ms < row.end_ts_ms
        for row in rows
    )


def test_campaign_identity_is_required_only_on_open_or_flip():
    assert requires_new_campaign_id(0.0, side="BUY", quantity_btc=0.001)
    assert not requires_new_campaign_id(0.002, side="SELL", quantity_btc=0.001)
    assert not requires_new_campaign_id(-0.002, side="BUY", quantity_btc=0.001)
    assert requires_new_campaign_id(-0.0004, side="BUY", quantity_btc=0.001)


def test_shutdown_gate_is_zero_tolerance():
    assert_planned_shutdown_drained(
        {
            "planned_quote_stop_triggered": True,
            "planned_shutdown_open_order_count": 0,
            "planned_shutdown_pending_new_order_count": 0,
            "planned_shutdown_pending_cancel_order_count": 0,
        }
    )
    with pytest.raises(RuntimeError, match="live orders"):
        assert_planned_shutdown_drained(
            {
                "planned_quote_stop_triggered": True,
                "planned_shutdown_pending_cancel_order_count": 1,
            }
        )


def test_segment_local_pnl_removes_carried_entry_mark():
    assert expected_segment_local_pnl_delta(
        terminal_mtm_pnl_usdc=1.5,
        initial_inventory_btc=0.001,
        initial_entry_price=100.0,
        first_mark_price=110.0,
    ) == pytest.approx(1.49)
