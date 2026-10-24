from __future__ import annotations

from types import SimpleNamespace

import pytest

from research.families.f10_live_replay_attribution.audit import (
    buy_q90_abi_v4_40day_lockstep as lockstep,
)


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
            "inventory_lifecycle_terminal_value": 77.0,
        }
    )
    result["_dynamic_fill_hazard_lifecycle_journal_audit"] = {}
    projected = lockstep._mechanics_only(result)
    assert "pnl" not in result.accessed
    assert "markout_10s" not in result.accessed
    assert "inventory_lifecycle_terminal_value" not in result.accessed
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


def test_authoritative_replay_calls_prospective_recovery_before_q90_block(monkeypatch):
    from models.backtest_tick import simulate_tick
    from narrowgate.replay.executor import ReplayExecutor
    from tests.test_tick_runtime_checkpoint import scenario

    calls = []
    recover = ReplayExecutor._evaluate_dynamic_fill_hazard_prospective_recovery
    blocked = ReplayExecutor._dynamic_fill_hazard_buy_blocked

    def record_recovery(self, **kwargs):
        calls.append(("recovery", self.state.t))
        return recover(self, **kwargs)

    def record_block(self):
        assert calls[-1] == ("recovery", self.state.t)
        calls.append(("block", self.state.t))
        return blocked(self)

    monkeypatch.setattr(ReplayExecutor, "_evaluate_dynamic_fill_hazard_prospective_recovery", record_recovery)
    monkeypatch.setattr(ReplayExecutor, "_dynamic_fill_hazard_buy_blocked", record_block)
    args, kwargs = scenario("ordinary")
    simulate_tick(*args, **kwargs)
    assert calls and len(calls) % 2 == 0


@pytest.mark.parametrize("retained", ["hazard", "cursor"])
def test_post_terminal_recovery_rejects_actual_retained_order_state(retained):
    from narrowgate.replay.executor.emergency import EmergencyActions

    owner = EmergencyActions()
    owner.state = SimpleNamespace(
        dynamic_fill_hazard_hold={"phase": "POST_CANCEL_RECOVERY", "client_order_id": "old"},
        dynamic_fill_hazard_runtime=SimpleNamespace(has_active_order_state=lambda _: retained == "hazard"),
        dynamic_fill_hazard_policy=object(),
        dynamic_fill_hazard_lifecycles={"old": object()},
        dynamic_fill_hazard_paths={"old": object()} if retained == "cursor" else {},
        dynamic_fill_hazard_post_terminal_hazard_reuse_count=0,
        dynamic_fill_hazard_post_terminal_cursor_reuse_count=0,
    )
    message = "active hazard state" if retained == "hazard" else "depth cursor"
    with pytest.raises(RuntimeError, match=message):
        owner._evaluate_dynamic_fill_hazard_prospective_recovery(candidate_price=100, now_ts=2000)
    assert getattr(owner.state, f"dynamic_fill_hazard_post_terminal_{retained}_reuse_count") == 1


def test_terminal_callback_rejects_an_unsupported_reason_before_mutation():
    from models import backtest_tick
    from narrowgate.replay.executor.emergency import EmergencyActions

    owner = EmergencyActions()
    hold = {"client_order_id": "old", "phase": "ACTIVE"}
    owner.state = SimpleNamespace(
        dynamic_fill_hazard_hold=hold,
        dynamic_fill_hazard_unsupported_terminal_route_count=0,
    )
    owner.services = backtest_tick
    with pytest.raises(RuntimeError, match="unsupported terminal reason"):
        owner._dynamic_fill_hazard_order_terminal(
            {"trace_id": "old", "remaining": .001}, event="unknown", now_ts=2000,
        )
    assert owner.state.dynamic_fill_hazard_unsupported_terminal_route_count == 1
    assert hold == {"client_order_id": "old", "phase": "ACTIVE"}


def test_aggregate_requires_exact_frozen_40_day_denominator():
    spec = {"development_days": [f"2026-01-{day:02d}" for day in range(1, 41)]}
    with pytest.raises(ValueError, match="denominator"):
        lockstep.aggregate(spec, [])
