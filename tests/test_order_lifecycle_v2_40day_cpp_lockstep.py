from __future__ import annotations

from research.families.f07_active_order_continuation.audit.order_lifecycle_v2_40day_cpp_lockstep import (
    audit_post_terminal_risk_reuse,
)


def _row(
    sequence: int,
    *,
    phase_before: str,
    phase_after: str,
    terminal: str = "NONE",
    queue_source: str = "native_exchange_book",
    exact_queue: bool = True,
    fill_risk_after: bool = False,
) -> dict[str, object]:
    return {
        "lifecycle_id": "lifecycle-1",
        "lifecycle_sequence": sequence,
        "phase_before": phase_before,
        "phase_after": phase_after,
        "terminal_observation": terminal,
        "simulator_queue_source": queue_source,
        "exact_queue_path_valid": exact_queue,
        "fill_risk_active_after": fill_risk_after,
    }


def test_terminal_transition_can_retain_exact_queue_but_recovery_cannot() -> None:
    rows = [
        _row(
            1,
            phase_before="ACTIVE",
            phase_after="CANCEL_PENDING",
            fill_risk_after=True,
        ),
        _row(
            2,
            phase_before="CANCEL_PENDING",
            phase_after="EXCHANGE_TERMINAL",
            terminal="EXCHANGE_TERMINAL",
        ),
        _row(
            3,
            phase_before="EXCHANGE_TERMINAL",
            phase_after="POST_CANCEL_RECOVERY",
            queue_source="not_in_fill_risk_set",
            exact_queue=False,
        ),
    ]
    result = audit_post_terminal_risk_reuse(rows)
    assert result["passed"] is True
    assert result["terminal_transition_from_risk_count"] == 1
    assert result["post_terminal_row_count"] == 1


def test_post_terminal_native_queue_identity_fails_closed() -> None:
    rows = [
        _row(
            1,
            phase_before="ACTIVE",
            phase_after="EXCHANGE_TERMINAL",
            terminal="EXCHANGE_TERMINAL",
        ),
        _row(
            2,
            phase_before="EXCHANGE_TERMINAL",
            phase_after="POST_CANCEL_RECOVERY",
            queue_source="native_exchange_book",
            exact_queue=False,
        ),
    ]
    result = audit_post_terminal_risk_reuse(rows)
    assert result["passed"] is False
    assert result["violation_counts"] == {"post_terminal_native_queue_identity_reuse": 1}
