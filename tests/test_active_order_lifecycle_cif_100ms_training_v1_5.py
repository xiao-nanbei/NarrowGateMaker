from __future__ import annotations

from collections import Counter, defaultdict

import pytest

from research.families.f07_active_order_continuation.audit.active_order_lifecycle_cif_100ms_training_v1_5 import (
    accumulate_exact_native_lifecycle,
    fit_rate_table,
    kernel_rates_from_lifecycle_rates,
)


def _row(
    sequence: int,
    event: str,
    ts_ns: int,
    *,
    before: str,
    after: str,
    remaining_before: float,
    remaining_after: float,
    terminal: str = "NONE",
    reason: str = "",
    queue_source: str = "native_exchange_book",
    exact_queue: bool = True,
) -> dict[str, object]:
    return {
        "lifecycle_id": "life-1",
        "lifecycle_sequence": sequence,
        "lifecycle_event": event,
        "event_visibility_ts_ns": ts_ns,
        "phase_before": before,
        "phase_after": after,
        "initial_quantity": 0.001,
        "remaining_quantity_before": remaining_before,
        "remaining_quantity_after": remaining_after,
        "terminal_observation": terminal,
        "exchange_terminal_reason": reason,
        "event_reason": reason,
        "side": "BUY",
        "simulator_queue_source": queue_source,
        "exact_queue_path_valid": exact_queue,
    }


def test_exact_native_cancel_ack_accumulates_visibility_risk_time() -> None:
    rows = [
        _row(1, "submit", 1_000_000_000, before="NONE", after="SUBMITTED", remaining_before=0.001, remaining_after=0.001, queue_source="pending_activation", exact_queue=False),
        _row(2, "activate", 1_100_000_000, before="SUBMITTED", after="ACTIVE", remaining_before=0.001, remaining_after=0.001),
        _row(3, "cancel_request", 1_700_000_000, before="ACTIVE", after="CANCEL_PENDING", remaining_before=0.001, remaining_after=0.001),
        _row(4, "exchange_terminal", 2_100_000_000, before="CANCEL_PENDING", after="EXCHANGE_TERMINAL", remaining_before=0.001, remaining_after=0.001, terminal="EXCHANGE_TERMINAL", reason="cancel_ack"),
    ]
    exposures = defaultdict(float)
    events = defaultdict(Counter)
    result = accumulate_exact_native_lifecycle(rows, exposures=exposures, events=events)
    assert result == {"eligible": 1, "censored": 0, "partial_spell_boundaries": 0}
    assert sum(exposures.values()) == pytest.approx(1.0)
    assert sum(counter["cancel_ack"] for counter in events.values()) == 1


def test_non_native_risk_transition_censors_entire_lifecycle() -> None:
    rows = [
        _row(1, "activate", 1_100_000_000, before="SUBMITTED", after="ACTIVE", remaining_before=0.001, remaining_after=0.001, queue_source="window_l2", exact_queue=False),
        _row(2, "full_fill", 1_300_000_000, before="ACTIVE", after="EXCHANGE_TERMINAL", remaining_before=0.001, remaining_after=0.0, terminal="EXCHANGE_TERMINAL", reason="full_fill", queue_source="window_l2", exact_queue=False),
    ]
    exposures = defaultdict(float)
    events = defaultdict(Counter)
    result = accumulate_exact_native_lifecycle(rows, exposures=exposures, events=events)
    assert result["eligible"] == 0
    assert result["censored"] == 1
    assert not exposures
    assert not events


def test_rate_table_is_finite_and_kernel_mapping_keeps_unclassified_channel_zero() -> None:
    key = ("BUY", "ACTIVE", 0, "full", 0)
    cells, parents = fit_rate_table(
        {key: 10.0},
        {key: {"full_fill": 2, "cancel_ack": 1, "other_terminal": 0}},
    )
    assert len(cells) == 1
    assert len(parents) == 1
    rates = cells[0]["rates_per_s"]
    assert kernel_rates_from_lifecycle_rates(rates) == pytest.approx(
        [rates["full_fill"], 0.0, rates["cancel_ack"], rates["other_terminal"]]
    )
