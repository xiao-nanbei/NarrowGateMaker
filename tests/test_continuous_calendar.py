import hashlib
import json
from pathlib import Path

import pytest

from data.quality.calendar_gap_manifest import canonical_sha256
from models.replay.continuous_calendar import (
    CalendarReplayPlan,
    ReplayAdapterCapabilities,
    ReplayMode,
    assert_shared_market_timeline,
    identify_continuity_and_governance,
)


def _write_manifest(path: Path) -> None:
    days = ["2026-01-01", "2026-01-02", "2026-01-03"]
    rows = []
    for index, day in enumerate(days):
        active = index != 1
        rows.append(
            {
                "day": day,
                "quality_grade": "A" if active else "F",
                "quality_reason": "",
                "formal_eligible": active,
                "sequence_valid": active,
                "coverage_99_valid": active,
                "l2_path": f"/{day}.parquet" if active else "",
                "expected_sha256": "a" * 64 if active else "",
                "source_quality_path": "/quality.csv",
                "source_quality_sha256": "b" * 64,
                "anchor_target_day": active,
                "native_normalized_l2_file_available": active,
                "strategy_tape_usable": active,
                "actual_sha256": "a" * 64 if active else "",
                "timestamp_rows": 10 if active else 0,
                "first_timestamp_ms": (1767225600000 + index * 86_400_000) if active else None,
                "last_timestamp_ms": (1767311999900 + index * 86_400_000) if active else None,
                "observed_gap_count": 0,
                "official_btcusdc_trade_path": f"/{day}.csv",
                "official_trade_mark_available": True,
                "daily_mark_source": "official_btcusdc_individual_trades",
                "daily_mark_available": True,
                "provider_normalized": {},
                "provider_normalized_tape_usable": False,
            }
        )
    manifest = {
        "schema_version": "calendar_continuity_manifest.v1",
        "identity": "versioned_continuous_replay_substrate_v1",
        "calendar_start_day": days[0],
        "calendar_end_day": days[-1],
        "calendar_day_count": 3,
        "calendar_days": days,
        "anchor_panel_identity": {
            "path": "/anchor.json",
            "sha256": "c" * 64,
            "day_field": "panels.development_days",
            "target_days": [days[0], days[2]],
            "target_day_count": 2,
        },
        "anchor_target_days": [days[0], days[2]],
        "anchor_target_day_count": 2,
        "maximum_contiguous_gap_ms": 5_000,
        "cancel_drain_ms": 2_000,
        "feature_warmup_lookback_s": 3_000,
        "gap_interpretation": "planned_strategy_offline_maintenance_not_market_continuity",
        "utc_midnight_policy": "accounting_slice_only_no_flatten_no_state_reset",
        "observed_data_gaps": [],
        "observed_data_gap_count": 0,
        "quality_grade_counts": {"A": 2, "B": 0, "C": 0, "D": 0, "F": 1},
        "day_sources": rows,
        "data_readiness": {
            "anchor_panel_all_strategy_tapes_usable": True,
            "calendar_all_native_normalized_l2_files_available": False,
            "calendar_all_any_source_normalized_l2_files_available": False,
            "calendar_daily_mark_bridge_complete": True,
            "missing_native_normalized_l2_days": [days[1]],
            "missing_any_source_normalized_l2_days": [days[1]],
            "missing_daily_mark_days": [],
        },
        "authority": {
            "anchor_panel_continuity_comparison": True,
            "continuous_pnl_inventory_campaign_sensitivity": True,
            "tail_governance_causal_attribution_without_on_off_control": False,
            "upgrade_non_a_to_grade_a": False,
            "gap_queue_lifecycle_q90_markout": False,
            "strategy_action_or_live_authority": False,
        },
    }
    manifest["canonical_manifest_sha256"] = canonical_sha256(manifest)
    path.write_text(json.dumps(manifest))


def _all_capabilities() -> ReplayAdapterCapabilities:
    return ReplayAdapterCapabilities(**{
        name: True for name in ReplayAdapterCapabilities.__dataclass_fields__
    })


def test_anchor_plan_bridges_non_target_day_without_midnight_reset(tmp_path) -> None:
    path = tmp_path / "manifest.json"
    _write_manifest(path)
    plan = CalendarReplayPlan.from_manifest(
        path,
        mode=ReplayMode.ANCHOR_PANEL_CONTINUOUS,
    )

    assert plan.active_days == ("2026-01-01", "2026-01-03")
    assert len(plan.restart_intervals) == 1
    assert plan.restart_intervals[0].resume_snapshot_ts_ms == 1767398400000
    assert len(plan.utc_accounting_boundaries_ts_ms) == 3
    assert not plan.full_tick_runner_binding
    with pytest.raises(RuntimeError, match="not bound"):
        plan.validate_for_execution(_all_capabilities())


def test_arm_timeline_must_match(tmp_path) -> None:
    path = tmp_path / "manifest.json"
    _write_manifest(path)
    control = CalendarReplayPlan.from_manifest(path, mode="anchor_panel_continuous")
    candidate = CalendarReplayPlan.from_manifest(path, mode="anchor_panel_continuous")
    assert_shared_market_timeline(control, candidate)

    native = CalendarReplayPlan.from_manifest(path, mode="native_strict_continuous")
    assert native.active_days == control.active_days
    with pytest.raises(RuntimeError, match="same market timeline"):
        assert_shared_market_timeline(control, native)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == control.manifest_sha256


def test_continuity_improvement_does_not_identify_tail_governance() -> None:
    result = identify_continuity_and_governance(
        daily_fresh_governance_on_pnl_usdc=-175.0,
        continuous_governance_on_pnl_usdc=-150.0,
    )
    assert result["continuity_effect_usdc"] == 25.0
    assert not result["continuity_improvement_proves_tail_governance"]
    assert not result["tail_governance_point_identified"]

    identified = identify_continuity_and_governance(
        daily_fresh_governance_on_pnl_usdc=-175.0,
        continuous_governance_on_pnl_usdc=-150.0,
        continuous_governance_off_pnl_usdc=-165.0,
    )
    assert identified["tail_governance_effect_usdc"] == 15.0
    assert identified["tail_governance_point_identified"]
