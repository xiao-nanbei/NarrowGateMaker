"""Strict mechanics-only journal for quantity-weighted order lifecycles.

This module is intentionally not wired into live or replay yet.  It provides
the lossless cursor/batch boundary needed by both: one source callback owns all
previously unseen lifecycle events, and the cursor advances only after the
whole batch passes validation.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass

from execution.order_lifecycle import QuantityWeightedOrderLifecycle
from execution.order_lifecycle_quantity_contract import (
    PARTIAL_FILL_PROGRESS_ABS_TOLERANCE_BTC,
    QUANTITY_INCREASE_ABS_TOLERANCE_BTC,
    TERMINAL_REMAINDER_ABS_TOLERANCE_BTC,
    persisted_terminal_remainder_is_zero,
)

ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION = "order_lifecycle_journal.v2"
ORDER_LIFECYCLE_JOURNAL_SCHEMA_VERSION = ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION
ORDER_LIFECYCLE_JOURNAL_V2_CURSOR_VERSION = "order_lifecycle_journal_cursor.v2"

_SOURCE_EVENT_FIELDS = (
    "sequence",
    "event",
    "visibility_ts_ns",
    "exchange_ts_ns",
    "phase_before",
    "phase_after",
    "remaining_qty_before",
    "remaining_qty_after",
    "quantity_time_exposure_btc_s",
    "quantity_time_exposure_visible_btc_s",
    "visible_exposure_valid",
    "visible_exposure_complete",
    "visible_exposure_invalid_reason",
    "quantity_time_exposure_exchange_btc_s",
    "exchange_exposure_valid",
    "exchange_exposure_complete",
    "exchange_exposure_invalid_reason",
    "reason",
)

_FILL_RISK_PHASES = frozenset({"ACTIVE", "PARTIALLY_FILLED", "CANCEL_PENDING"})
_LOCAL_SHUTDOWN_REASONS = frozenset(
    {
        "administrative_cancel",
        "local_shutdown_cancel",
        "local_shutdown_unknown_ack",
        "shutdown",
    }
)
_EXCHANGE_TERMINAL_REASONS = frozenset(
    {
        "cancel_ack",
        "cancel_ack_reconciled",
        "expired",
        "filled_before_cancel_ack",
        "full_fill",
        "rejected",
    }
)
_EVENT_REASONS = {
    "submit": frozenset({""}),
    "submit_ack_unknown": frozenset({"submit_response_unknown"}),
    "submit_ack_unknown_censored": frozenset({"local_shutdown_unknown_ack"}),
    "activate": frozenset({""}),
    "activate_unknown_prefix": frozenset(
        {"orphan_adoption", "rest_reconcile_activation_unknown"}
    ),
    "cancel_request": frozenset({""}),
    "cancel_rejected": frozenset({""}),
    "partial_fill": frozenset({""}),
    "full_fill": frozenset({""}),
    "exchange_terminal": _EXCHANGE_TERMINAL_REASONS,
    "local_shutdown_censor": _LOCAL_SHUTDOWN_REASONS,
    "post_cancel_recovery": frozenset({"old_order_risk_set_ended"}),
    "reentry_eligible": frozenset({"prospective_placement_state_supported"}),
}
_EVENT_TRANSITIONS = {
    "submit": frozenset({("SUBMITTED", "SUBMITTED")}),
    "submit_ack_unknown": frozenset({("SUBMITTED", "SUBMITTED")}),
    "submit_ack_unknown_censored": frozenset({("SUBMITTED", "SUBMITTED")}),
    "activate": frozenset(
        {
            ("SUBMITTED", "ACTIVE"),
            ("ACTIVE", "ACTIVE"),
            ("CANCEL_PENDING", "ACTIVE"),
        }
    ),
    "activate_unknown_prefix": frozenset({("SUBMITTED", "ACTIVE")}),
    "cancel_request": frozenset(
        {
            ("ACTIVE", "CANCEL_PENDING"),
            ("PARTIALLY_FILLED", "CANCEL_PENDING"),
        }
    ),
    "cancel_rejected": frozenset(
        {
            ("CANCEL_PENDING", "ACTIVE"),
            ("CANCEL_PENDING", "PARTIALLY_FILLED"),
        }
    ),
    "partial_fill": frozenset(
        {
            ("ACTIVE", "PARTIALLY_FILLED"),
            ("SUBMITTED", "PARTIALLY_FILLED"),
            ("PARTIALLY_FILLED", "PARTIALLY_FILLED"),
            ("CANCEL_PENDING", "CANCEL_PENDING"),
        }
    ),
    "full_fill": frozenset(
        {
            ("ACTIVE", "EXCHANGE_TERMINAL"),
            ("SUBMITTED", "EXCHANGE_TERMINAL"),
            ("PARTIALLY_FILLED", "EXCHANGE_TERMINAL"),
            ("CANCEL_PENDING", "EXCHANGE_TERMINAL"),
        }
    ),
    "exchange_terminal": frozenset(
        {
            ("SUBMITTED", "EXCHANGE_TERMINAL"),
            ("ACTIVE", "EXCHANGE_TERMINAL"),
            ("PARTIALLY_FILLED", "EXCHANGE_TERMINAL"),
            ("CANCEL_PENDING", "EXCHANGE_TERMINAL"),
        }
    ),
    "local_shutdown_censor": frozenset(
        {
            ("SUBMITTED", "SUBMITTED"),
            ("ACTIVE", "ACTIVE"),
            ("PARTIALLY_FILLED", "PARTIALLY_FILLED"),
            ("CANCEL_PENDING", "CANCEL_PENDING"),
        }
    ),
    "post_cancel_recovery": frozenset({("EXCHANGE_TERMINAL", "POST_CANCEL_RECOVERY")}),
    "reentry_eligible": frozenset({("POST_CANCEL_RECOVERY", "REENTRY_ELIGIBLE")}),
}
_EVENTS_REQUIRING_EXCHANGE_CLOCK = frozenset(
    {"activate", "cancel_rejected", "partial_fill", "full_fill"}
)
_FORBIDDEN_COLUMN_FRAGMENTS = ("pnl", "reward", "markout")


@dataclass(frozen=True, slots=True)
class OrderLifecycleJournalV2Row:
    """One immutable lifecycle event, not a latest-snapshot overlay."""

    schema_version: str
    event_id: str
    lifecycle_id: str
    runtime_source: str
    source_callback_id: str
    source_callback_type: str
    source_callback_event_ordinal: int
    source_callback_event_count: int
    source_callback_received_ts_ns: int
    source_callback_exchange_ts_ns: int | None
    source_callback_exchange_clock_valid: bool
    client_order_id: str
    exchange_order_id: str | None
    symbol: str
    side: str
    lifecycle_sequence: int
    lifecycle_event: str
    event_visibility_ts_ns: int
    event_exchange_ts_ns: int | None
    event_exchange_clock_valid: bool
    phase_before: str
    phase_after: str
    event_reason: str
    observation_origin: str
    left_truncated: bool
    left_truncation_reason: str
    terminal_observation: str
    exchange_terminal_reason: str
    local_censor_reason: str
    initial_quantity: float
    remaining_quantity_before: float
    remaining_quantity_after: float
    fill_risk_active_after: bool | None
    quantity_time_exposure_visible_btc_s: float
    visible_exposure_valid: bool
    visible_exposure_complete: bool
    visible_exposure_invalid_reason: str
    quantity_time_exposure_exchange_btc_s: float | None
    exchange_exposure_valid: bool
    exchange_exposure_complete: bool
    exchange_exposure_invalid_reason: str


ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS = tuple(OrderLifecycleJournalV2Row.__dataclass_fields__)


@dataclass(frozen=True, slots=True)
class OrderLifecycleJournalV2SourceCallback:
    """Causal clocks and identity of the callback owning one emitted batch."""

    callback_id: str
    callback_type: str
    received_ts_ns: int
    exchange_ts_ns: int | None = None

    def __post_init__(self) -> None:
        _require_id("source callback id", self.callback_id)
        _require_id("source callback type", self.callback_type)
        if int(self.received_ts_ns) <= 0:
            raise ValueError("source callback received timestamp must be positive")
        if self.exchange_ts_ns is not None:
            if int(self.exchange_ts_ns) <= 0:
                raise ValueError("source callback exchange timestamp must be positive or null")
            if int(self.exchange_ts_ns) > int(self.received_ts_ns):
                raise ValueError("source callback exchange timestamp is after received time")


@dataclass(frozen=True, slots=True)
class OrderLifecycleJournalV2Cursor:
    """Durable high-water mark for one lifecycle event stream."""

    lifecycle_id: str
    client_order_id: str
    last_emitted_sequence: int = 0
    last_event_id: str = ""

    def __post_init__(self) -> None:
        _require_id("lifecycle id", self.lifecycle_id)
        _require_id("client order id", self.client_order_id)
        if int(self.last_emitted_sequence) < 0:
            raise ValueError("last emitted lifecycle sequence cannot be negative")
        if int(self.last_emitted_sequence) == 0 and self.last_event_id:
            raise ValueError("empty lifecycle cursor cannot have a last event id")
        if int(self.last_emitted_sequence) > 0:
            _require_id("last event id", self.last_event_id)

    def checkpoint(self) -> dict[str, object]:
        return {
            "schema_version": ORDER_LIFECYCLE_JOURNAL_V2_CURSOR_VERSION,
            "lifecycle_id": self.lifecycle_id,
            "client_order_id": self.client_order_id,
            "last_emitted_sequence": int(self.last_emitted_sequence),
            "last_event_id": self.last_event_id,
        }

    @classmethod
    def from_checkpoint(
        cls,
        checkpoint: Mapping[str, object],
    ) -> OrderLifecycleJournalV2Cursor:
        expected = (
            "schema_version",
            "lifecycle_id",
            "client_order_id",
            "last_emitted_sequence",
            "last_event_id",
        )
        if tuple(checkpoint) != expected:
            raise ValueError("lifecycle cursor checkpoint schema mismatch")
        if checkpoint["schema_version"] != ORDER_LIFECYCLE_JOURNAL_V2_CURSOR_VERSION:
            raise ValueError("unsupported lifecycle cursor checkpoint version")
        return cls(
            lifecycle_id=str(checkpoint["lifecycle_id"]),
            client_order_id=str(checkpoint["client_order_id"]),
            last_emitted_sequence=int(checkpoint["last_emitted_sequence"]),
            last_event_id=str(checkpoint["last_event_id"]),
        )


@dataclass(frozen=True, slots=True)
class OrderLifecycleJournalV2Batch:
    """All unseen rows from one callback plus the post-commit checkpoint."""

    rows: tuple[OrderLifecycleJournalV2Row, ...]
    checkpoint: Mapping[str, object]

    def __post_init__(self) -> None:
        cursor = OrderLifecycleJournalV2Cursor.from_checkpoint(self.checkpoint)
        if not self.rows:
            return
        payloads = tuple(asdict(row) for row in self.rows)
        for payload in payloads:
            validate_order_lifecycle_journal_v2_payload(payload)
        callback_identity = {
            (
                row.source_callback_id,
                row.source_callback_type,
                row.source_callback_received_ts_ns,
                row.source_callback_exchange_ts_ns,
            )
            for row in self.rows
        }
        if len(callback_identity) != 1:
            raise ValueError("journal batch contains multiple source callbacks")
        if [row.source_callback_event_ordinal for row in self.rows] != list(
            range(1, len(self.rows) + 1)
        ):
            raise ValueError("source callback event ordinals are not contiguous")
        if any(row.source_callback_event_count != len(self.rows) for row in self.rows):
            raise ValueError("source callback event count does not match batch")
        sequences = [row.lifecycle_sequence for row in self.rows]
        if sequences != list(range(sequences[0], sequences[0] + len(sequences))):
            raise ValueError("journal batch lifecycle sequences are not contiguous")
        if len({row.event_id for row in self.rows}) != len(self.rows):
            raise ValueError("journal batch contains duplicate event ids")
        last = self.rows[-1]
        if (
            cursor.lifecycle_id != last.lifecycle_id
            or cursor.client_order_id != last.client_order_id
            or cursor.last_emitted_sequence != last.lifecycle_sequence
            or cursor.last_event_id != last.event_id
        ):
            raise ValueError("journal batch checkpoint does not match last row")

    def payloads(self) -> tuple[dict[str, object], ...]:
        payloads = tuple(asdict(row) for row in self.rows)
        for payload in payloads:
            validate_order_lifecycle_journal_v2_payload(payload)
        return payloads


@dataclass(frozen=True, slots=True)
class _DerivedEventState:
    visible_invalid_reason: str
    exchange_invalid_reason: str
    visible_complete: bool
    exchange_complete: bool
    terminal_observation: str
    exchange_terminal_reason: str
    local_censor_reason: str


def _require_id(label: str, value: object) -> str:
    normalized = str(value).strip()
    if not normalized or normalized.lower() in {"nan", "none", "null"}:
        raise ValueError(f"{label} is required")
    return normalized


def _finite_nonnegative(label: str, value: object) -> float:
    number = float(value)
    if not math.isfinite(number) or number < 0.0:
        raise ValueError(f"{label} must be finite and non-negative")
    return number


def _optional_exchange_ts(value: object) -> int | None:
    timestamp = int(value)
    return timestamp if timestamp > 0 else None


def _float_equal(left: object, right: object) -> bool:
    if left is None or right is None:
        return left is None and right is None
    return math.isclose(
        float(left),
        float(right),
        rel_tol=1e-12,
        abs_tol=1e-15,
    )


def _stable_event_id(
    *,
    lifecycle_id: str,
    client_order_id: str,
    event: Mapping[str, object],
) -> str:
    identity = {
        "schema_version": ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION,
        "lifecycle_id": lifecycle_id,
        "client_order_id": client_order_id,
        "lifecycle_sequence": int(event["sequence"]),
        "lifecycle_event": str(event["event"]),
        "event_visibility_ts_ns": int(event["visibility_ts_ns"]),
        "event_exchange_ts_ns": int(event["exchange_ts_ns"]),
        "phase_before": str(event["phase_before"]),
        "phase_after": str(event["phase_after"]),
        "remaining_quantity_before": float(event["remaining_qty_before"]),
        "remaining_quantity_after": float(event["remaining_qty_after"]),
        "event_reason": str(event["reason"]),
    }
    encoded = json.dumps(
        identity,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _validate_source_events(
    events: Sequence[Mapping[str, object]],
) -> tuple[_DerivedEventState, ...]:
    if not events:
        raise ValueError("order lifecycle has no events")

    derived: list[_DerivedEventState] = []
    previous_visibility = 0
    previous_exchange = 0
    previous_visible_exposure = 0.0
    previous_exchange_exposure = 0.0
    visible_invalid_reason = ""
    exchange_invalid_reason = ""
    visible_complete = False
    exchange_complete = False
    previous_phase_after = ""
    previous_remaining_after: float | None = None

    for index, event in enumerate(events, start=1):
        if tuple(event) != _SOURCE_EVENT_FIELDS:
            raise ValueError("source lifecycle event schema mismatch")
        sequence = int(event["sequence"])
        if sequence != index:
            raise ValueError("lifecycle sequence must be unique and contiguous from one")
        event_name = str(event["event"])
        if event_name not in _EVENT_REASONS:
            raise ValueError(f"unsupported lifecycle event: {event_name}")
        reason = str(event["reason"])
        if event_name == "exchange_terminal" and reason in _LOCAL_SHUTDOWN_REASONS:
            raise ValueError(
                "legacy local shutdown encoded as exchange_terminal is not "
                "authoritative; an explicit local_shutdown_censor event is required"
            )
        if reason not in _EVENT_REASONS[event_name]:
            raise ValueError(f"unsupported reason for lifecycle event {event_name}: {reason}")
        transition = (str(event["phase_before"]), str(event["phase_after"]))
        if transition not in _EVENT_TRANSITIONS[event_name]:
            raise ValueError(
                f"unsupported lifecycle transition for {event_name}: "
                f"{transition[0]} -> {transition[1]}"
            )
        if index == 1 and event_name != "submit":
            raise ValueError("first lifecycle event must be submit")
        if derived and derived[-1].terminal_observation == "LOCAL_SHUTDOWN_CENSOR":
            raise ValueError("lifecycle events cannot follow a local shutdown censor")
        if previous_phase_after and transition[0] != previous_phase_after:
            raise ValueError("lifecycle phase chain is discontinuous")
        previous_phase_after = transition[1]

        visibility_ts = int(event["visibility_ts_ns"])
        if visibility_ts <= 0 or visibility_ts < previous_visibility:
            raise ValueError("lifecycle visibility timestamps regressed")
        previous_visibility = visibility_ts
        exchange_ts = _optional_exchange_ts(event["exchange_ts_ns"])
        exchange_clock_event = (
            f"exchange_terminal:{reason}" if event_name == "exchange_terminal" else event_name
        )
        if exchange_ts is not None and exchange_ts > visibility_ts:
            if not exchange_invalid_reason:
                exchange_invalid_reason = (
                    f"exchange_timestamp_after_visibility:{exchange_clock_event}"
                )
        if exchange_ts is not None and previous_exchange > exchange_ts:
            if not exchange_invalid_reason:
                exchange_invalid_reason = f"exchange_timestamp_regressed:{exchange_clock_event}"
        if exchange_ts is not None:
            previous_exchange = max(previous_exchange, exchange_ts)

        remaining_before = _finite_nonnegative(
            "remaining quantity before", event["remaining_qty_before"]
        )
        remaining_after = _finite_nonnegative(
            "remaining quantity after", event["remaining_qty_after"]
        )
        if remaining_after > remaining_before + QUANTITY_INCREASE_ABS_TOLERANCE_BTC:
            raise ValueError("remaining quantity increased within lifecycle event")
        if previous_remaining_after is not None and not _float_equal(
            remaining_before, previous_remaining_after
        ):
            raise ValueError("remaining quantity chain is discontinuous")
        if event_name not in {"partial_fill", "full_fill"} and not _float_equal(
            remaining_before, remaining_after
        ):
            raise ValueError("non-fill lifecycle event changed remaining quantity")
        if event_name == "partial_fill" and not (
            TERMINAL_REMAINDER_ABS_TOLERANCE_BTC
            < remaining_after
            < remaining_before - PARTIAL_FILL_PROGRESS_ABS_TOLERANCE_BTC
        ):
            raise ValueError("partial fill must strictly reduce remaining quantity")
        if event_name == "full_fill" and not persisted_terminal_remainder_is_zero(
            remaining_after
        ):
            raise ValueError("full fill must persist exact zero remaining quantity")
        if (
            event_name == "exchange_terminal"
            and reason in {"filled_before_cancel_ack", "full_fill"}
            and not persisted_terminal_remainder_is_zero(remaining_after)
        ):
            raise ValueError("fill terminal reason requires exact zero remaining quantity")
        previous_remaining_after = remaining_after

        visible_exposure = _finite_nonnegative(
            "visible quantity-time exposure",
            event["quantity_time_exposure_visible_btc_s"],
        )
        legacy_exposure = _finite_nonnegative(
            "legacy quantity-time exposure",
            event["quantity_time_exposure_btc_s"],
        )
        if not _float_equal(visible_exposure, legacy_exposure):
            raise ValueError("visible and legacy exposure snapshots disagree")
        if visible_exposure + 1e-15 < previous_visible_exposure:
            raise ValueError("visible quantity-time exposure regressed")
        previous_visible_exposure = visible_exposure

        source_visible_valid = bool(event["visible_exposure_valid"])
        source_visible_complete = bool(event["visible_exposure_complete"])
        source_visible_invalid_reason = str(
            event["visible_exposure_invalid_reason"]
        )
        if source_visible_valid:
            if source_visible_invalid_reason:
                raise ValueError("valid visible exposure cannot carry an invalid reason")
            if visible_invalid_reason:
                raise ValueError("visible exposure validity recovered after invalidation")
        else:
            if not source_visible_invalid_reason:
                raise ValueError("invalid visible exposure requires a reason")
            if not visible_invalid_reason:
                visible_invalid_reason = source_visible_invalid_reason
            elif visible_invalid_reason != source_visible_invalid_reason:
                raise ValueError("visible exposure invalid reason changed")
            if source_visible_complete:
                raise ValueError("invalid visible exposure cannot be complete")

        source_exchange_valid = bool(event["exchange_exposure_valid"])
        source_exchange_complete = bool(event["exchange_exposure_complete"])
        source_exchange_invalid_reason = str(
            event["exchange_exposure_invalid_reason"]
        )
        exchange_exposure = event["quantity_time_exposure_exchange_btc_s"]
        if not source_exchange_valid and exchange_exposure is not None:
            raise ValueError("invalid exchange exposure must be null")
        if source_exchange_valid and exchange_exposure is not None:
            exchange_value = _finite_nonnegative(
                "exchange quantity-time exposure", exchange_exposure
            )
            if exchange_value + 1e-15 < previous_exchange_exposure:
                raise ValueError("exchange quantity-time exposure regressed")
            previous_exchange_exposure = exchange_value

        if source_exchange_valid:
            if source_exchange_invalid_reason:
                raise ValueError("valid exchange exposure cannot carry an invalid reason")
        elif not source_exchange_invalid_reason:
            raise ValueError("invalid exchange exposure requires a reason")
        if not source_exchange_valid and not exchange_invalid_reason:
            exchange_invalid_reason = source_exchange_invalid_reason
        elif not source_exchange_valid and (
            exchange_invalid_reason != source_exchange_invalid_reason
        ):
            raise ValueError("exchange exposure invalid reason changed")
        if source_exchange_valid and exchange_invalid_reason:
            raise ValueError("exchange exposure validity recovered after invalidation")

        terminal_observation = "NONE"
        exchange_terminal_reason = ""
        local_censor_reason = ""
        if event_name == "full_fill":
            terminal_observation = "EXCHANGE_TERMINAL"
            exchange_terminal_reason = "full_fill"
            visible_complete = source_visible_complete
            exchange_complete = source_exchange_complete
        elif event_name == "exchange_terminal":
            terminal_observation = "EXCHANGE_TERMINAL"
            exchange_terminal_reason = reason
            visible_complete = source_visible_complete
            exchange_complete = source_exchange_complete
        elif event_name in {"local_shutdown_censor", "submit_ack_unknown_censored"}:
            if exchange_ts is not None:
                raise ValueError("local shutdown censor cannot carry an exchange event timestamp")
            terminal_observation = "LOCAL_SHUTDOWN_CENSOR"
            local_censor_reason = reason
            visible_complete = source_visible_complete
            exchange_complete = source_exchange_complete
        else:
            visible_complete = source_visible_complete
            exchange_complete = source_exchange_complete

        derived.append(
            _DerivedEventState(
                visible_invalid_reason=visible_invalid_reason,
                exchange_invalid_reason=exchange_invalid_reason,
                visible_complete=visible_complete,
                exchange_complete=exchange_complete,
                terminal_observation=terminal_observation,
                exchange_terminal_reason=exchange_terminal_reason,
                local_censor_reason=local_censor_reason,
            )
        )
    return tuple(derived)


def _validate_snapshot(
    *,
    lifecycle: QuantityWeightedOrderLifecycle,
    events: Sequence[Mapping[str, object]],
    derived: Sequence[_DerivedEventState],
) -> Mapping[str, object]:
    snapshot = lifecycle.snapshot()
    events_after_snapshot = lifecycle.events()
    if tuple(events) != events_after_snapshot:
        raise ValueError("lifecycle mutated while journal snapshot was captured")
    last = events[-1]
    last_derived = derived[-1]
    if str(snapshot["phase"]) != str(last["phase_after"]):
        raise ValueError("lifecycle snapshot phase is inconsistent")
    comparisons = {
        "remaining quantity": (
            snapshot["remaining_quantity"],
            last["remaining_qty_after"],
        ),
        "visible exposure": (
            snapshot["quantity_time_exposure_visible_btc_s"],
            last["quantity_time_exposure_visible_btc_s"],
        ),
        "exchange exposure": (
            snapshot["quantity_time_exposure_exchange_btc_s"],
            last["quantity_time_exposure_exchange_btc_s"],
        ),
    }
    for label, (actual, expected) in comparisons.items():
        if not _float_equal(actual, expected):
            raise ValueError(f"lifecycle snapshot {label} is inconsistent")
    if not _float_equal(snapshot["initial_quantity"], events[0]["remaining_qty_before"]):
        raise ValueError("lifecycle snapshot initial quantity is inconsistent")
    if int(snapshot["submitted_ts_ns"]) != int(events[0]["visibility_ts_ns"]):
        raise ValueError("lifecycle snapshot submitted timestamp is inconsistent")
    expected_fill_risk = bool(
        str(last["phase_after"]) in _FILL_RISK_PHASES
        and float(last["remaining_qty_after"])
        > TERMINAL_REMAINDER_ABS_TOLERANCE_BTC
    )
    if bool(snapshot["fill_risk_active"]) != expected_fill_risk:
        raise ValueError("lifecycle snapshot fill-risk state is inconsistent")
    if bool(snapshot["visible_exposure_valid"]) != bool(
        last["visible_exposure_valid"]
    ):
        raise ValueError("lifecycle snapshot visible validity is inconsistent")
    if str(snapshot["visible_exposure_invalid_reason"]) != (
        last_derived.visible_invalid_reason
    ):
        raise ValueError("lifecycle snapshot visible invalid reason is inconsistent")
    if bool(snapshot["visible_exposure_complete"]) != bool(
        last["visible_exposure_complete"]
    ):
        raise ValueError("lifecycle snapshot visible completeness is inconsistent")
    if bool(snapshot["exchange_exposure_valid"]) != bool(last["exchange_exposure_valid"]):
        raise ValueError("lifecycle snapshot exchange validity is inconsistent")
    if not bool(snapshot["exchange_exposure_valid"]):
        if str(snapshot["exchange_exposure_invalid_reason"]) != (
            last_derived.exchange_invalid_reason
        ):
            raise ValueError("lifecycle snapshot exchange invalid reason is inconsistent")
    if bool(snapshot["exchange_exposure_complete"]) != bool(
        last["exchange_exposure_complete"]
    ):
        raise ValueError("lifecycle snapshot exchange completeness is inconsistent")
    return snapshot


def validate_order_lifecycle_journal_v2_payload(
    payload: Mapping[str, object],
) -> None:
    """Reject schema drift and any accidental economic outcome surface."""

    if tuple(payload) != ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS:
        raise ValueError("order lifecycle journal v2 payload schema mismatch")
    for column in payload:
        lowered = column.lower()
        if any(fragment in lowered for fragment in _FORBIDDEN_COLUMN_FRAGMENTS):
            raise ValueError("economic outcome fields are forbidden in lifecycle v2")
    if payload["schema_version"] != ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION:
        raise ValueError("unsupported order lifecycle journal v2 schema version")
    _require_id("event id", payload["event_id"])
    _require_id("lifecycle id", payload["lifecycle_id"])
    _require_id("runtime source", payload["runtime_source"])
    _require_id("source callback id", payload["source_callback_id"])
    _require_id("source callback type", payload["source_callback_type"])
    _require_id("client order id", payload["client_order_id"])
    _require_id("symbol", payload["symbol"])
    if str(payload["side"]) not in {"BUY", "SELL"}:
        raise ValueError("unsupported order side in lifecycle v2 payload")

    event_name = str(payload["lifecycle_event"])
    reason = str(payload["event_reason"])
    if event_name not in _EVENT_REASONS:
        raise ValueError(f"unsupported lifecycle event: {event_name}")
    if event_name == "exchange_terminal" and reason in _LOCAL_SHUTDOWN_REASONS:
        raise ValueError("legacy local shutdown encoded as exchange_terminal is not authoritative")
    if reason not in _EVENT_REASONS[event_name]:
        raise ValueError(f"unsupported reason for lifecycle event {event_name}: {reason}")
    transition = (str(payload["phase_before"]), str(payload["phase_after"]))
    if transition not in _EVENT_TRANSITIONS[event_name]:
        raise ValueError("unsupported lifecycle transition in v2 payload")
    if int(payload["lifecycle_sequence"]) <= 0:
        raise ValueError("lifecycle sequence must be positive")
    reconstructed_event = {
        "sequence": payload["lifecycle_sequence"],
        "event": payload["lifecycle_event"],
        "visibility_ts_ns": payload["event_visibility_ts_ns"],
        "exchange_ts_ns": payload["event_exchange_ts_ns"] or 0,
        "phase_before": payload["phase_before"],
        "phase_after": payload["phase_after"],
        "remaining_qty_before": payload["remaining_quantity_before"],
        "remaining_qty_after": payload["remaining_quantity_after"],
        "reason": payload["event_reason"],
    }
    expected_event_id = _stable_event_id(
        lifecycle_id=str(payload["lifecycle_id"]),
        client_order_id=str(payload["client_order_id"]),
        event=reconstructed_event,
    )
    if str(payload["event_id"]) != expected_event_id:
        raise ValueError("stable lifecycle event id is inconsistent")
    ordinal = int(payload["source_callback_event_ordinal"])
    callback_count = int(payload["source_callback_event_count"])
    if callback_count <= 0 or not 1 <= ordinal <= callback_count:
        raise ValueError("source callback event ordinal is invalid")

    callback_received = int(payload["source_callback_received_ts_ns"])
    if callback_received <= 0:
        raise ValueError("source callback received timestamp must be positive")
    callback_exchange = payload["source_callback_exchange_ts_ns"]
    callback_exchange_valid = bool(payload["source_callback_exchange_clock_valid"])
    if callback_exchange_valid != (callback_exchange is not None):
        raise ValueError("source callback exchange clock validity is inconsistent")
    if callback_exchange is not None:
        if int(callback_exchange) <= 0 or int(callback_exchange) > callback_received:
            raise ValueError("source callback exchange clock is invalid")

    event_visibility = int(payload["event_visibility_ts_ns"])
    if event_visibility < callback_received:
        raise ValueError("lifecycle event predates source callback")
    event_exchange = payload["event_exchange_ts_ns"]
    event_exchange_valid = bool(payload["event_exchange_clock_valid"])
    if event_exchange_valid != (event_exchange is not None):
        raise ValueError("event exchange clock validity is inconsistent")
    if event_exchange is not None:
        if int(event_exchange) <= 0 or int(event_exchange) > event_visibility:
            raise ValueError("event exchange clock is invalid")
        if callback_exchange is None or int(event_exchange) != int(callback_exchange):
            raise ValueError("event and callback exchange clocks disagree")

    exchange_order_id = payload["exchange_order_id"]
    if event_name not in {
        "submit",
        "submit_ack_unknown",
        "submit_ack_unknown_censored",
    }:
        _require_id("exchange order id", exchange_order_id)
    elif exchange_order_id is not None:
        _require_id("exchange order id", exchange_order_id)

    remaining_before = _finite_nonnegative(
        "remaining quantity before", payload["remaining_quantity_before"]
    )
    remaining_after = _finite_nonnegative(
        "remaining quantity after", payload["remaining_quantity_after"]
    )
    if remaining_after > remaining_before + QUANTITY_INCREASE_ABS_TOLERANCE_BTC:
        raise ValueError("remaining quantity increased within lifecycle row")
    if event_name == "partial_fill" and not (
        TERMINAL_REMAINDER_ABS_TOLERANCE_BTC
        < remaining_after
        < remaining_before - PARTIAL_FILL_PROGRESS_ABS_TOLERANCE_BTC
    ):
        raise ValueError("partial fill must strictly reduce remaining quantity")
    if event_name == "full_fill" and not persisted_terminal_remainder_is_zero(
        remaining_after
    ):
        raise ValueError("full fill must persist exact zero remaining quantity")
    _finite_nonnegative("initial quantity", payload["initial_quantity"])
    expected_fill_risk = bool(
        str(payload["phase_after"]) in _FILL_RISK_PHASES
        and remaining_after > TERMINAL_REMAINDER_ABS_TOLERANCE_BTC
    )
    if event_name in {"local_shutdown_censor", "submit_ack_unknown_censored"}:
        if payload["fill_risk_active_after"] is not None:
            raise ValueError("physical fill-risk state must be unknown after local censor")
    elif payload["fill_risk_active_after"] is None or (
        bool(payload["fill_risk_active_after"]) != expected_fill_risk
    ):
        raise ValueError("fill-risk state is inconsistent in lifecycle row")
    _finite_nonnegative(
        "visible quantity-time exposure",
        payload["quantity_time_exposure_visible_btc_s"],
    )
    if not bool(payload["visible_exposure_valid"]):
        _require_id(
            "visible exposure invalid reason",
            payload["visible_exposure_invalid_reason"],
        )
    elif payload["visible_exposure_invalid_reason"]:
        raise ValueError("valid visible exposure cannot have an invalid reason")
    if not bool(payload["visible_exposure_valid"]) and bool(payload["visible_exposure_complete"]):
        raise ValueError("invalid visible exposure cannot be complete")
    exchange_exposure = payload["quantity_time_exposure_exchange_btc_s"]
    if exchange_exposure is not None:
        _finite_nonnegative("exchange quantity-time exposure", exchange_exposure)
    if not bool(payload["exchange_exposure_valid"]):
        if exchange_exposure is not None:
            raise ValueError("invalid exchange exposure must be null")
        _require_id(
            "exchange exposure invalid reason",
            payload["exchange_exposure_invalid_reason"],
        )
    elif payload["exchange_exposure_invalid_reason"]:
        raise ValueError("valid exchange exposure cannot have an invalid reason")
    if not bool(payload["exchange_exposure_valid"]) and bool(payload["exchange_exposure_complete"]):
        raise ValueError("invalid exchange exposure cannot be complete")

    origin = str(payload["observation_origin"])
    left_truncated = bool(payload["left_truncated"])
    if origin == "ORPHAN_ADOPTION":
        if not left_truncated:
            raise ValueError("orphan adoption must be left truncated")
        _require_id("left truncation reason", payload["left_truncation_reason"])
    elif origin == "NATIVE_SUBMIT":
        if left_truncated or payload["left_truncation_reason"]:
            raise ValueError("native submit cannot be left truncated")
    else:
        raise ValueError("unsupported lifecycle observation origin")

    terminal_observation = str(payload["terminal_observation"])
    exchange_reason = str(payload["exchange_terminal_reason"])
    censor_reason = str(payload["local_censor_reason"])
    if terminal_observation == "EXCHANGE_TERMINAL":
        if not exchange_reason or censor_reason:
            raise ValueError("exchange terminal classification is inconsistent")
        expected_reason = "full_fill" if event_name == "full_fill" else reason
        if (
            event_name not in {"full_fill", "exchange_terminal"}
            or exchange_reason != expected_reason
            or exchange_reason not in _EXCHANGE_TERMINAL_REASONS
        ):
            raise ValueError("exchange terminal event and reason disagree")
    elif terminal_observation == "LOCAL_SHUTDOWN_CENSOR":
        if exchange_reason or censor_reason not in _LOCAL_SHUTDOWN_REASONS:
            raise ValueError("local shutdown censor classification is inconsistent")
        if event_name not in {
            "local_shutdown_censor",
            "submit_ack_unknown_censored",
        } or reason != censor_reason:
            raise ValueError("local shutdown event and censor reason disagree")
        if str(payload["phase_after"]) == "EXCHANGE_TERMINAL":
            raise ValueError("local shutdown censor cannot assert exchange-terminal phase")
        if bool(payload["visible_exposure_complete"]) or bool(
            payload["exchange_exposure_complete"]
        ):
            raise ValueError("local shutdown censor cannot complete exposure")
    elif terminal_observation == "NONE":
        if exchange_reason or censor_reason:
            raise ValueError("non-terminal row has a terminal reason")
        if event_name in {
            "full_fill",
            "exchange_terminal",
            "local_shutdown_censor",
            "submit_ack_unknown_censored",
        }:
            raise ValueError("terminal lifecycle event lacks terminal classification")
    else:
        raise ValueError("unsupported terminal observation")


class OrderLifecycleJournalV2BatchEmitter:
    """Emit every unseen event from one lifecycle with atomic cursor advance."""

    def __init__(
        self,
        *,
        lifecycle_id: str,
        runtime_source: str,
        client_order_id: str,
        symbol: str,
        side: str,
        exchange_order_id: str | int | None = None,
        orphan_adoption: bool = False,
        left_truncation_reason: str = "",
        cursor: OrderLifecycleJournalV2Cursor | None = None,
    ) -> None:
        self.lifecycle_id = _require_id("lifecycle id", lifecycle_id)
        self.runtime_source = _require_id("runtime source", runtime_source)
        self.client_order_id = _require_id("client order id", client_order_id)
        self.symbol = _require_id("symbol", symbol).upper()
        normalized_side = _require_id("side", side).upper()
        if normalized_side not in {"BUY", "SELL"}:
            raise ValueError(f"unsupported order side: {side}")
        self.side = normalized_side
        self.exchange_order_id = self._normalize_exchange_order_id(exchange_order_id)
        self.orphan_adoption = bool(orphan_adoption)
        if self.orphan_adoption:
            self.left_truncation_reason = _require_id(
                "left truncation reason",
                left_truncation_reason or "orphan_adoption",
            )
        elif left_truncation_reason:
            raise ValueError("left truncation reason requires orphan adoption")
        else:
            self.left_truncation_reason = ""
        self._cursor = cursor or OrderLifecycleJournalV2Cursor(
            lifecycle_id=self.lifecycle_id,
            client_order_id=self.client_order_id,
        )
        if self._cursor.lifecycle_id != self.lifecycle_id:
            raise ValueError("cursor lifecycle id does not match emitter")
        if self._cursor.client_order_id != self.client_order_id:
            raise ValueError("cursor client order id does not match emitter")

    @staticmethod
    def _normalize_exchange_order_id(value: str | int | None) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        if not normalized or normalized.lower() in {"nan", "none", "null"}:
            raise ValueError("exchange order id must be non-empty or null")
        return normalized

    @classmethod
    def from_checkpoint(
        cls,
        *,
        checkpoint: Mapping[str, object],
        runtime_source: str,
        symbol: str,
        side: str,
        exchange_order_id: str | int | None = None,
        orphan_adoption: bool = False,
        left_truncation_reason: str = "",
    ) -> OrderLifecycleJournalV2BatchEmitter:
        cursor = OrderLifecycleJournalV2Cursor.from_checkpoint(checkpoint)
        return cls(
            lifecycle_id=cursor.lifecycle_id,
            runtime_source=runtime_source,
            client_order_id=cursor.client_order_id,
            symbol=symbol,
            side=side,
            exchange_order_id=exchange_order_id,
            orphan_adoption=orphan_adoption,
            left_truncation_reason=left_truncation_reason,
            cursor=cursor,
        )

    @property
    def cursor(self) -> OrderLifecycleJournalV2Cursor:
        return self._cursor

    def bind_exchange_order_id(self, exchange_order_id: str | int) -> None:
        normalized = self._normalize_exchange_order_id(exchange_order_id)
        if normalized is None:
            raise ValueError("exchange order id is required")
        if self.exchange_order_id not in {None, normalized}:
            raise ValueError("exchange order id changed within lifecycle")
        self.exchange_order_id = normalized

    def emit_unseen(
        self,
        *,
        lifecycle: QuantityWeightedOrderLifecycle,
        callback: OrderLifecycleJournalV2SourceCallback,
    ) -> OrderLifecycleJournalV2Batch:
        """Return all unseen events; do not advance on any validation error."""

        events = lifecycle.events()
        derived = _validate_source_events(events)
        snapshot = _validate_snapshot(
            lifecycle=lifecycle,
            events=events,
            derived=derived,
        )
        last_emitted = int(self._cursor.last_emitted_sequence)
        if last_emitted > len(events):
            raise ValueError("lifecycle event stream is behind journal cursor")
        if last_emitted:
            prior_event_id = _stable_event_id(
                lifecycle_id=self.lifecycle_id,
                client_order_id=self.client_order_id,
                event=events[last_emitted - 1],
            )
            if prior_event_id != self._cursor.last_event_id:
                raise ValueError("previously emitted lifecycle event was mutated")

        unseen = events[last_emitted:]
        if not unseen:
            return OrderLifecycleJournalV2Batch(rows=(), checkpoint=self._cursor.checkpoint())
        for event in unseen:
            if int(event["visibility_ts_ns"]) < int(callback.received_ts_ns):
                raise ValueError("unseen lifecycle event predates its source callback")
            event_exchange_ts = _optional_exchange_ts(event["exchange_ts_ns"])
            if event_exchange_ts is not None:
                if callback.exchange_ts_ns is None:
                    raise ValueError(
                        "exchange lifecycle event lacks source callback exchange clock"
                    )
                if event_exchange_ts != int(callback.exchange_ts_ns):
                    raise ValueError("lifecycle and source callback exchange clocks disagree")

        count = len(unseen)
        rows: list[OrderLifecycleJournalV2Row] = []
        initial_quantity = float(snapshot["initial_quantity"])
        observation_origin = "ORPHAN_ADOPTION" if self.orphan_adoption else "NATIVE_SUBMIT"
        for ordinal, event in enumerate(unseen, start=1):
            sequence = int(event["sequence"])
            state = derived[sequence - 1]
            exchange_ts = _optional_exchange_ts(event["exchange_ts_ns"])
            if self.exchange_order_id is None and str(event["event"]) not in {
                "submit",
                "submit_ack_unknown",
                "submit_ack_unknown_censored",
            }:
                raise ValueError("non-submit lifecycle event lacks exchange order id")
            row = OrderLifecycleJournalV2Row(
                schema_version=ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION,
                event_id=_stable_event_id(
                    lifecycle_id=self.lifecycle_id,
                    client_order_id=self.client_order_id,
                    event=event,
                ),
                lifecycle_id=self.lifecycle_id,
                runtime_source=self.runtime_source,
                source_callback_id=callback.callback_id,
                source_callback_type=callback.callback_type,
                source_callback_event_ordinal=ordinal,
                source_callback_event_count=count,
                source_callback_received_ts_ns=int(callback.received_ts_ns),
                source_callback_exchange_ts_ns=(
                    int(callback.exchange_ts_ns) if callback.exchange_ts_ns is not None else None
                ),
                source_callback_exchange_clock_valid=(callback.exchange_ts_ns is not None),
                client_order_id=self.client_order_id,
                exchange_order_id=self.exchange_order_id,
                symbol=self.symbol,
                side=self.side,
                lifecycle_sequence=sequence,
                lifecycle_event=str(event["event"]),
                event_visibility_ts_ns=int(event["visibility_ts_ns"]),
                event_exchange_ts_ns=exchange_ts,
                event_exchange_clock_valid=exchange_ts is not None,
                phase_before=str(event["phase_before"]),
                phase_after=str(event["phase_after"]),
                event_reason=str(event["reason"]),
                observation_origin=observation_origin,
                left_truncated=self.orphan_adoption,
                left_truncation_reason=self.left_truncation_reason,
                terminal_observation=state.terminal_observation,
                exchange_terminal_reason=state.exchange_terminal_reason,
                local_censor_reason=state.local_censor_reason,
                initial_quantity=initial_quantity,
                remaining_quantity_before=float(event["remaining_qty_before"]),
                remaining_quantity_after=float(event["remaining_qty_after"]),
                fill_risk_active_after=(
                    None
                    if str(event["event"]) in {
                        "local_shutdown_censor",
                        "submit_ack_unknown_censored",
                    }
                    else bool(
                        str(event["phase_after"]) in _FILL_RISK_PHASES
                        and float(event["remaining_qty_after"])
                        > TERMINAL_REMAINDER_ABS_TOLERANCE_BTC
                    )
                ),
                quantity_time_exposure_visible_btc_s=float(
                    event["quantity_time_exposure_visible_btc_s"]
                ),
                visible_exposure_valid=bool(event["visible_exposure_valid"]),
                visible_exposure_complete=bool(event["visible_exposure_complete"]),
                visible_exposure_invalid_reason=str(
                    event["visible_exposure_invalid_reason"]
                ),
                quantity_time_exposure_exchange_btc_s=(
                    float(event["quantity_time_exposure_exchange_btc_s"])
                    if event["quantity_time_exposure_exchange_btc_s"] is not None
                    else None
                ),
                exchange_exposure_valid=bool(event["exchange_exposure_valid"]),
                exchange_exposure_complete=bool(event["exchange_exposure_complete"]),
                exchange_exposure_invalid_reason=str(
                    event["exchange_exposure_invalid_reason"]
                ),
            )
            validate_order_lifecycle_journal_v2_payload(asdict(row))
            rows.append(row)

        next_cursor = OrderLifecycleJournalV2Cursor(
            lifecycle_id=self.lifecycle_id,
            client_order_id=self.client_order_id,
            last_emitted_sequence=rows[-1].lifecycle_sequence,
            last_event_id=rows[-1].event_id,
        )
        self._cursor = next_cursor
        return OrderLifecycleJournalV2Batch(rows=tuple(rows), checkpoint=next_cursor.checkpoint())


OrderLifecycleJournalV2Emitter = OrderLifecycleJournalV2BatchEmitter
