from __future__ import annotations

import ast
from pathlib import Path

import pytest

from research.families.f10_live_replay_attribution.audit import (
    buy_q90_abi_v4_40day_lockstep as lockstep,
)


ROOT = Path(__file__).resolve().parents[1]


class TrackingResult(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.accessed: list[str] = []

    def get(self, key, default=None):
        self.accessed.append(str(key))
        return super().get(key, default)


def test_mechanics_allowlist_contains_no_economic_keys():
    for key in lockstep.RESULT_ALLOWLIST:
        lowered = key.lower()
        assert not any(
            fragment in lowered
            for fragment in lockstep.FORBIDDEN_ECONOMIC_KEY_FRAGMENTS
        )


def test_mechanics_projection_never_reads_unlisted_economic_outputs():
    result = TrackingResult(
        {
            key: [] if key.startswith("_dynamic") else 0
            for key in lockstep.RESULT_ALLOWLIST
        }
        | {
            "pnl": 123.0,
            "markout_10s": -9.0,
            "campaign_terminal_value": 77.0,
        }
    )
    result["_dynamic_fill_hazard_lifecycle_journal_audit"] = {}
    projected = lockstep._mechanics_only(result)
    assert "pnl" not in result.accessed
    assert "markout_10s" not in result.accessed
    assert "campaign_terminal_value" not in result.accessed
    assert "lifecycle_audit" in projected
    assert "lifecycle_journal" not in projected
    assert projected["lifecycle_journal_row_count"] == 0


def test_mechanics_projection_persists_only_journal_aggregates():
    result = {
        key: [] if key.startswith("_dynamic") else 0
        for key in lockstep.RESULT_ALLOWLIST
    }
    result["_dynamic_fill_hazard_lifecycle_journal_audit"] = {
        "row_count": 2,
    }
    result["_dynamic_fill_hazard_lifecycle_journal"] = [
        {
            "lifecycle_event": "cancel_ack",
            "phase_before": "CANCEL_PENDING",
            "phase_after": "EXCHANGE_TERMINAL",
            "terminal_policy_route": "PROSPECTIVE_CANCEL_REENTRY",
            "terminal_reason": "cancel_ack",
        },
        {
            "lifecycle_event": "post_cancel_recovery",
            "phase_before": "EXCHANGE_TERMINAL",
            "phase_after": "POST_CANCEL_RECOVERY",
            "terminal_policy_route": "",
            "terminal_reason": "",
        },
    ]

    projected = lockstep._mechanics_only(result)

    assert "lifecycle_journal" not in projected
    assert projected["lifecycle_journal_row_count"] == 2
    assert projected["lifecycle_event_counts"] == {
        "cancel_ack": 1,
        "post_cancel_recovery": 1,
    }
    assert projected["terminal_route_counts"] == {
        "PROSPECTIVE_CANCEL_REENTRY": 1,
    }
    assert projected["terminal_reason_counts"] == {"cancel_ack": 1}


def test_authoritative_replay_calls_prospective_recovery_before_q90_block():
    source = (ROOT / "models" / "backtest_tick.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id
        in {
            "_evaluate_dynamic_fill_hazard_prospective_recovery",
            "_dynamic_fill_hazard_buy_blocked",
        }
    ]
    prospective = min(
        node.lineno
        for node in calls
        if isinstance(node.func, ast.Name)
        and node.func.id == "_evaluate_dynamic_fill_hazard_prospective_recovery"
    )
    blocked = max(
        node.lineno
        for node in calls
        if isinstance(node.func, ast.Name)
        and node.func.id == "_dynamic_fill_hazard_buy_blocked"
    )
    assert prospective < blocked


def test_post_terminal_recovery_fails_closed_on_old_path_or_hazard_state():
    source = (ROOT / "models" / "backtest_tick.py").read_text(encoding="utf-8")
    assert "q90 post-terminal active hazard state was retained" in source
    assert "q90 post-terminal depth cursor was retained" in source
    assert "q90 lifecycle received unsupported terminal reason" in source


def test_visibility_boundary_ambiguity_is_propagated_to_cpp_path():
    source = (ROOT / "models" / "backtest_tick.py").read_text(encoding="utf-8")
    assert '"same_ms_exchange_book_ambiguity"' in source
    assert "synchronize_visibility_batch_ambiguity_to_cpp(" in source


def test_aggregate_requires_exact_frozen_40_day_denominator():
    spec = {"development_days": [f"2026-01-{day:02d}" for day in range(1, 41)]}
    with pytest.raises(ValueError, match="denominator"):
        lockstep.aggregate(spec, [])
