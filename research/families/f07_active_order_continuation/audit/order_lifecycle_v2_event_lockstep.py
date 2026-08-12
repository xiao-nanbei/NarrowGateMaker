"""Mechanics-only lockstep audit for replay lifecycle journal-v2.

The audit compares the authoritative replay journal-v2 stream with the
existing local-order lifecycle trace where the legacy trace has a defined
clock and event meaning. Dual-clock quantity-time exposure is independently
recomputed from journal-v2 rows; missing legacy clock support is reported, not
silently treated as parity.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import tempfile
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

from execution.order_lifecycle_journal_v2 import (
    ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS,
    validate_order_lifecycle_journal_v2_payload,
)
from execution.order_lifecycle_quantity_contract import (
    PARTIAL_FILL_PROGRESS_ABS_TOLERANCE_BTC,
    QUANTITY_INCREASE_ABS_TOLERANCE_BTC,
    TERMINAL_REMAINDER_ABS_TOLERANCE_BTC,
    persisted_terminal_remainder_is_zero,
)

IDENTITY = "f07_order_lifecycle_v2_event_lockstep.v1_1"
REPORT_SCHEMA_VERSION = "f07_order_lifecycle_v2_event_lockstep_report.v1_1"
ATOMIC_ENVELOPE_SCHEMA_VERSION = "f07_order_lifecycle_v2_event_lockstep_atomic_result.v1_1"

_RISK_PHASES = frozenset({"ACTIVE", "PARTIALLY_FILLED", "CANCEL_PENDING"})
_TERMINAL_EVENTS = frozenset({"full_fill", "exchange_terminal", "local_shutdown_censor"})
_QUANTITY_TOLERANCE = QUANTITY_INCREASE_ABS_TOLERANCE_BTC
_EXPOSURE_ABS_TOLERANCE = 1e-14
_CLIENT_ID_RE = re.compile(
    r"^replay-(?P<symbol>[A-Z0-9_]+)-(?P<order_id>[0-9]+)-(?P<submit_ms>[0-9]+)$"
)

_LEGACY_REQUIRED_COLUMNS = (
    "symbol",
    "order_id",
    "event_type",
    "event_ts_ns",
    "event_seq",
    "event_reason",
    "state_before",
    "state_after",
    "order_submit_ts_ns",
    "order_qty",
    "remaining_qty",
)
_LEGACY_OPTIONAL_COLUMNS = (
    "event_visibility_ts_ns",
    "event_exchange_ts_ns",
)

_LEGACY_MECHANICS_EVENTS = frozenset(
    {
        "submit",
        "activate",
        "partial_fill",
        "full_fill",
        "cancel_request",
        "cancel_reject",
        "cancel_rejected",
        "cancel_ack",
        "reject",
        "expiry",
        "expired",
        "day_end_censor",
        "local_shutdown_censor",
    }
)
_LEGACY_VISIBILITY_CLOCK_EVENTS = frozenset(
    {
        "submit",
        "cancel_request",
        "cancel_ack",
        "expiry",
        "expired",
        "day_end_censor",
        "local_shutdown_censor",
    }
)
_LEGACY_EXCHANGE_CLOCK_EVENTS = frozenset({"activate", "reject"})
_LEGACY_BOTH_CLOCK_EVENTS = frozenset({"partial_fill", "full_fill"})
_LEGACY_EXPIRY_REASONS = frozenset({"expired", "ioc_expired", "ioc_no_top_liquidity"})


class LifecycleLockstepInputError(ValueError):
    """Raised when an input cannot support a trustworthy lockstep result."""


@dataclass(frozen=True, slots=True)
class _LegacyEvent:
    client_order_id: str
    event: str
    event_seq: int
    event_ts_ns: int
    visibility_ts_ns: int | None
    exchange_ts_ns: int | None
    state_before: str
    state_after: str
    initial_quantity: float
    remaining_after: float
    terminal_reason: str


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _canonical_sha256(value: object) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _utc_day(timestamp_ns: int) -> str:
    return datetime.fromtimestamp(
        int(timestamp_ns) / 1_000_000_000.0,
        tz=timezone.utc,
    ).strftime("%Y-%m-%d")


def _required_id(value: object, *, label: str) -> str:
    normalized = str(value).strip()
    if not normalized or normalized.lower() in {"nan", "none", "null"}:
        raise LifecycleLockstepInputError(f"{label} is required")
    return normalized


def _finite_nonnegative(value: object, *, label: str) -> float:
    number = float(value)
    if not math.isfinite(number) or number < 0.0:
        raise LifecycleLockstepInputError(f"{label} must be finite and non-negative")
    return number


def _optional_positive_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        timestamp = int(value)
    except (TypeError, ValueError):
        return None
    return timestamp if timestamp > 0 else None


def _close(left: object, right: object, *, absolute: float) -> bool:
    return math.isclose(
        float(left),
        float(right),
        rel_tol=1e-10,
        abs_tol=absolute,
    )


def _normalize_legacy_event(event_type: str, reason: str) -> tuple[str, str]:
    event = str(event_type).strip().lower()
    normalized_reason = str(reason).strip().lower()
    if event in {"cancel_reject", "cancel_rejected"}:
        return "cancel_rejected", ""
    if event == "cancel_ack":
        if normalized_reason in _LEGACY_EXPIRY_REASONS:
            return "expiry", "expired"
        return "cancel_ack", "cancel_ack"
    if event in {"expiry", "expired"}:
        return "expiry", "expired"
    if event == "reject":
        return "reject", "rejected"
    if event in {"day_end_censor", "local_shutdown_censor"}:
        return "shutdown", "shutdown"
    if event == "full_fill":
        return event, "full_fill"
    return event, ""


def _normalize_v2_event(row: Mapping[str, object]) -> tuple[str, str]:
    event = str(row["lifecycle_event"])
    reason = str(row["event_reason"])
    if event == "exchange_terminal":
        if reason in {"cancel_ack", "cancel_ack_reconciled"}:
            return "cancel_ack", "cancel_ack"
        if reason == "expired":
            return "expiry", "expired"
        if reason == "rejected":
            return "reject", "rejected"
        return event, reason
    if event == "local_shutdown_censor":
        return "shutdown", "shutdown"
    if event == "full_fill":
        return event, "full_fill"
    return event, ""


def _legacy_clock(
    row: Mapping[str, object],
    *,
    domain: str,
) -> int | None:
    explicit_column = "event_visibility_ts_ns" if domain == "visibility" else "event_exchange_ts_ns"
    explicit = _optional_positive_int(row.get(explicit_column))
    if explicit is not None:
        return explicit
    event = str(row["event_type"]).strip().lower()
    event_ts = int(row["event_ts_ns"])
    if event in _LEGACY_BOTH_CLOCK_EVENTS:
        return event_ts
    if domain == "visibility" and event in _LEGACY_VISIBILITY_CLOCK_EVENTS:
        return event_ts
    if domain == "exchange" and event in _LEGACY_EXCHANGE_CLOCK_EVENTS:
        return event_ts
    return None


def _legacy_expected_phase(event: _LegacyEvent) -> str | None:
    if event.event == "shutdown":
        return None
    state = event.state_after.strip().lower()
    if state == "pending_new":
        return "SUBMITTED"
    if state == "pending_cancel":
        return "CANCEL_PENDING"
    if state == "open":
        return (
            "PARTIALLY_FILLED"
            if event.remaining_after
            < event.initial_quantity - PARTIAL_FILL_PROGRESS_ABS_TOLERANCE_BTC
            else "ACTIVE"
        )
    if state in {"filled", "cancelled", "rejected"}:
        return "EXCHANGE_TERMINAL"
    if state == "censored":
        return None
    return "UNKNOWN"


def _expected_client_order_id(row: Mapping[str, object]) -> str:
    symbol = _required_id(row["symbol"], label="legacy symbol").upper()
    order_id = int(row["order_id"])
    submit_ns = int(row["order_submit_ts_ns"])
    if order_id < 0 or submit_ns <= 0 or submit_ns % 1_000_000 != 0:
        raise LifecycleLockstepInputError(
            "legacy order identity requires non-negative order_id and millisecond submit time"
        )
    return f"replay-{symbol}-{order_id}-{submit_ns // 1_000_000}"


def _legacy_projection(row: Mapping[str, object]) -> _LegacyEvent | None:
    missing = [column for column in _LEGACY_REQUIRED_COLUMNS if column not in row]
    if missing:
        raise LifecycleLockstepInputError(f"legacy lifecycle row missing columns: {missing}")
    event_type = str(row["event_type"]).strip().lower()
    if event_type not in _LEGACY_MECHANICS_EVENTS:
        return None
    event, terminal_reason = _normalize_legacy_event(
        event_type,
        str(row["event_reason"]),
    )
    event_ts_ns = int(row["event_ts_ns"])
    event_seq = int(row["event_seq"])
    if event_ts_ns <= 0 or event_seq <= 0:
        raise LifecycleLockstepInputError(
            "legacy lifecycle event time and sequence must be positive"
        )
    return _LegacyEvent(
        client_order_id=_expected_client_order_id(row),
        event=event,
        event_seq=event_seq,
        event_ts_ns=event_ts_ns,
        visibility_ts_ns=_legacy_clock(row, domain="visibility"),
        exchange_ts_ns=_legacy_clock(row, domain="exchange"),
        state_before=str(row["state_before"]),
        state_after=str(row["state_after"]),
        initial_quantity=_finite_nonnegative(
            row["order_qty"],
            label="legacy initial quantity",
        ),
        remaining_after=_finite_nonnegative(
            row["remaining_qty"],
            label="legacy remaining quantity",
        ),
        terminal_reason=terminal_reason,
    )


def _client_identity(client_order_id: str) -> tuple[str, int, int]:
    match = _CLIENT_ID_RE.fullmatch(client_order_id)
    if match is None:
        raise LifecycleLockstepInputError(
            "journal-v2 replay client_order_id does not match stable replay identity"
        )
    return (
        match.group("symbol"),
        int(match.group("order_id")),
        int(match.group("submit_ms")),
    )


class LifecycleV2EventLockstepAuditor:
    """Incrementally accept one UTC day's projected mechanics rows."""

    def __init__(self, *, day: str, mismatch_sample_limit: int = 25) -> None:
        datetime.strptime(day, "%Y-%m-%d")
        self.day = day
        self.mismatch_sample_limit = max(1, int(mismatch_sample_limit))
        self._legacy: dict[str, list[_LegacyEvent]] = defaultdict(list)
        self._v2: dict[str, list[dict[str, object]]] = defaultdict(list)
        self._v2_lifecycle_ids: dict[str, str] = {}
        self._v2_clients_by_lifecycle_id: dict[str, str] = {}
        self._legacy_event_sequences: set[int] = set()
        self._legacy_last_event_sequence = 0
        self._v2_event_ids: set[str] = set()
        self._legacy_rows_seen = 0
        self._legacy_mechanics_rows = 0
        self._legacy_ignored_events: Counter[str] = Counter()
        self._v2_rows_seen = 0
        self._mismatch_counts: Counter[str] = Counter()
        self._mismatch_samples: list[dict[str, object]] = []

    def _mismatch(
        self,
        code: str,
        *,
        client_order_id: str = "",
        sequence: int = 0,
        expected: object = None,
        observed: object = None,
    ) -> None:
        self._mismatch_counts[str(code)] += 1
        if len(self._mismatch_samples) >= self.mismatch_sample_limit:
            return
        self._mismatch_samples.append(
            {
                "code": str(code),
                "client_order_id": str(client_order_id),
                "sequence": int(sequence),
                "expected": expected,
                "observed": observed,
            }
        )

    def observe_legacy(self, row: Mapping[str, object]) -> None:
        self._legacy_rows_seen += 1
        event_type = str(row.get("event_type", "")).strip().lower()
        projected = _legacy_projection(row)
        event_seq = int(row.get("event_seq", 0) or 0)
        if event_seq > 0:
            if event_seq in self._legacy_event_sequences:
                self._mismatch(
                    "legacy_duplicate_event_sequence",
                    sequence=event_seq,
                )
            if event_seq <= self._legacy_last_event_sequence:
                self._mismatch(
                    "legacy_event_sequence_not_stream_monotone",
                    sequence=event_seq,
                    expected=f">{self._legacy_last_event_sequence}",
                    observed=event_seq,
                )
            self._legacy_last_event_sequence = max(
                self._legacy_last_event_sequence,
                event_seq,
            )
            self._legacy_event_sequences.add(event_seq)
        if projected is None:
            self._legacy_ignored_events[event_type or "missing"] += 1
            return
        if _utc_day(projected.event_ts_ns) != self.day:
            raise LifecycleLockstepInputError("legacy lifecycle row lies outside requested UTC day")
        self._legacy_mechanics_rows += 1
        self._legacy[projected.client_order_id].append(projected)

    def observe_v2(self, row: Mapping[str, object]) -> None:
        payload = {column: row[column] for column in ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS}
        validate_order_lifecycle_journal_v2_payload(payload)
        if _utc_day(int(payload["event_visibility_ts_ns"])) != self.day:
            raise LifecycleLockstepInputError("journal-v2 row lies outside requested UTC day")
        self._v2_rows_seen += 1
        client_order_id = _required_id(
            payload["client_order_id"],
            label="journal-v2 client order id",
        )
        symbol, _order_id, submit_ms = _client_identity(client_order_id)
        if symbol != str(payload["symbol"]).upper():
            self._mismatch(
                "v2_client_symbol_identity_mismatch",
                client_order_id=client_order_id,
                observed=payload["symbol"],
                expected=symbol,
            )
        if int(payload["lifecycle_sequence"]) == 1:
            expected_submit_ms = int(payload["event_visibility_ts_ns"]) // 1_000_000
            if submit_ms != expected_submit_ms:
                self._mismatch(
                    "v2_client_submit_identity_mismatch",
                    client_order_id=client_order_id,
                    observed=submit_ms,
                    expected=expected_submit_ms,
                )
        lifecycle_id = _required_id(
            payload["lifecycle_id"],
            label="journal-v2 lifecycle id",
        )
        if not lifecycle_id.endswith(f":{client_order_id}"):
            self._mismatch(
                "v2_lifecycle_id_client_suffix_mismatch",
                client_order_id=client_order_id,
                observed=lifecycle_id,
                expected=f"*:{client_order_id}",
            )
        bound_client = self._v2_clients_by_lifecycle_id.get(lifecycle_id)
        if bound_client is not None and bound_client != client_order_id:
            self._mismatch(
                "v2_lifecycle_id_reused_across_clients",
                client_order_id=client_order_id,
                observed=lifecycle_id,
                expected=bound_client,
            )
        self._v2_clients_by_lifecycle_id[lifecycle_id] = client_order_id
        existing = self._v2_lifecycle_ids.get(client_order_id)
        if existing is not None and existing != lifecycle_id:
            self._mismatch(
                "v2_lifecycle_id_changed",
                client_order_id=client_order_id,
                observed=lifecycle_id,
                expected=existing,
            )
        self._v2_lifecycle_ids[client_order_id] = lifecycle_id
        event_id = _required_id(payload["event_id"], label="journal-v2 event id")
        if event_id in self._v2_event_ids:
            self._mismatch(
                "v2_duplicate_event_id",
                client_order_id=client_order_id,
                sequence=int(payload["lifecycle_sequence"]),
                observed=event_id,
            )
        self._v2_event_ids.add(event_id)
        self._v2[client_order_id].append(payload)

    def _audit_v2_trace(
        self,
        client_order_id: str,
        rows: list[dict[str, object]],
    ) -> dict[str, object]:
        ordered = sorted(rows, key=lambda row: int(row["lifecycle_sequence"]))
        sequences = [int(row["lifecycle_sequence"]) for row in ordered]
        if sequences != list(range(1, len(ordered) + 1)):
            self._mismatch(
                "v2_lifecycle_sequence_not_contiguous",
                client_order_id=client_order_id,
                expected=list(range(1, len(ordered) + 1)),
                observed=sequences,
            )

        visible_expected = 0.0
        visible_interval_count = 0
        visible_risk_duration_ns = 0
        risk_spell_count = 0
        cancel_reject_active_count = 0
        cancel_reject_partially_filled_count = 0
        previous: dict[str, object] | None = None
        previous_risk = False
        terminal_index: int | None = None
        local_censor = False

        exchange_expected = 0.0
        exchange_started = False
        exchange_active = False
        exchange_last_ts = 0
        exchange_remaining = 0.0
        exchange_valid = True
        exchange_invalid_reason_reported = True
        previous_exchange_ts = 0

        for index, row in enumerate(ordered):
            sequence = int(row["lifecycle_sequence"])
            visibility_ts = int(row["event_visibility_ts_ns"])
            phase_after = str(row["phase_after"])
            remaining_after = float(row["remaining_quantity_after"])
            current_risk = (
                phase_after in _RISK_PHASES
                and remaining_after > TERMINAL_REMAINDER_ABS_TOLERANCE_BTC
            )

            if previous is not None:
                delta_ns = visibility_ts - int(previous["event_visibility_ts_ns"])
                if delta_ns < 0:
                    self._mismatch(
                        "v2_visibility_clock_regressed",
                        client_order_id=client_order_id,
                        sequence=sequence,
                        observed=delta_ns,
                        expected=">=0",
                    )
                elif previous_risk:
                    visible_expected += (
                        float(previous["remaining_quantity_after"]) * delta_ns / 1_000_000_000.0
                    )
                    visible_interval_count += 1
                    visible_risk_duration_ns += delta_ns
            reported_visible = float(row["quantity_time_exposure_visible_btc_s"])
            if not _close(
                visible_expected,
                reported_visible,
                absolute=_EXPOSURE_ABS_TOLERANCE,
            ):
                self._mismatch(
                    "v2_visible_exposure_mismatch",
                    client_order_id=client_order_id,
                    sequence=sequence,
                    expected=visible_expected,
                    observed=reported_visible,
                )
            if current_risk and not previous_risk:
                risk_spell_count += 1

            event = str(row["lifecycle_event"])
            if event == "cancel_rejected":
                expected_phase = (
                    "PARTIALLY_FILLED"
                    if remaining_after
                    < float(row["initial_quantity"])
                    - PARTIAL_FILL_PROGRESS_ABS_TOLERANCE_BTC
                    else "ACTIVE"
                )
                if phase_after != expected_phase:
                    self._mismatch(
                        "v2_cancel_reject_continuation_phase_mismatch",
                        client_order_id=client_order_id,
                        sequence=sequence,
                        expected=expected_phase,
                        observed=phase_after,
                    )
                if not current_risk:
                    self._mismatch(
                        "v2_cancel_reject_fill_risk_not_resumed",
                        client_order_id=client_order_id,
                        sequence=sequence,
                        expected=True,
                        observed=row["fill_risk_active_after"],
                    )
                if expected_phase == "PARTIALLY_FILLED":
                    cancel_reject_partially_filled_count += 1
                else:
                    cancel_reject_active_count += 1
            if event == "full_fill" and not persisted_terminal_remainder_is_zero(
                remaining_after
            ):
                self._mismatch(
                    "v2_terminal_remainder_not_exact_zero",
                    client_order_id=client_order_id,
                    sequence=sequence,
                    expected=0.0,
                    observed=remaining_after,
                )
            if (
                event == "exchange_terminal"
                and str(row["event_reason"])
                in {"full_fill", "filled_before_cancel_ack"}
                and not persisted_terminal_remainder_is_zero(remaining_after)
            ):
                self._mismatch(
                    "v2_terminal_remainder_not_exact_zero",
                    client_order_id=client_order_id,
                    sequence=sequence,
                    expected=0.0,
                    observed=remaining_after,
                )
            exchange_ts = _optional_positive_int(row["event_exchange_ts_ns"])
            row_exchange_valid = bool(row["exchange_exposure_valid"])
            if not row_exchange_valid:
                exchange_valid = False
                if not str(row["exchange_exposure_invalid_reason"]).strip():
                    exchange_invalid_reason_reported = False
                    self._mismatch(
                        "v2_exchange_invalid_reason_missing",
                        client_order_id=client_order_id,
                        sequence=sequence,
                    )
            if exchange_ts is not None:
                if exchange_ts > visibility_ts:
                    self._mismatch(
                        "v2_exchange_clock_after_visibility",
                        client_order_id=client_order_id,
                        sequence=sequence,
                        expected=f"<={visibility_ts}",
                        observed=exchange_ts,
                    )
                if previous_exchange_ts > exchange_ts:
                    self._mismatch(
                        "v2_exchange_clock_regressed",
                        client_order_id=client_order_id,
                        sequence=sequence,
                        expected=f">={previous_exchange_ts}",
                        observed=exchange_ts,
                    )
                if exchange_started and exchange_active:
                    exchange_expected += (
                        exchange_remaining * (exchange_ts - exchange_last_ts) / 1_000_000_000.0
                    )
                if event == "activate":
                    exchange_started = True
                    exchange_active = True
                if event in {"full_fill", "exchange_terminal"}:
                    exchange_active = False
                exchange_last_ts = exchange_ts
                previous_exchange_ts = max(previous_exchange_ts, exchange_ts)
                exchange_remaining = remaining_after

            reported_exchange = row["quantity_time_exposure_exchange_btc_s"]
            if row_exchange_valid and exchange_started:
                if reported_exchange is None or not _close(
                    exchange_expected,
                    reported_exchange,
                    absolute=_EXPOSURE_ABS_TOLERANCE,
                ):
                    self._mismatch(
                        "v2_exchange_exposure_mismatch",
                        client_order_id=client_order_id,
                        sequence=sequence,
                        expected=exchange_expected,
                        observed=reported_exchange,
                    )
            elif not row_exchange_valid and reported_exchange is not None:
                self._mismatch(
                    "v2_invalid_exchange_exposure_not_null",
                    client_order_id=client_order_id,
                    sequence=sequence,
                    observed=reported_exchange,
                    expected=None,
                )

            if terminal_index is not None:
                self._mismatch(
                    "v2_event_after_terminal_or_censor",
                    client_order_id=client_order_id,
                    sequence=sequence,
                    observed=event,
                )
                if current_risk:
                    self._mismatch(
                        "v2_fill_risk_after_terminal_or_censor",
                        client_order_id=client_order_id,
                        sequence=sequence,
                        observed=phase_after,
                    )
            if event in _TERMINAL_EVENTS and terminal_index is None:
                terminal_index = index
                local_censor = event == "local_shutdown_censor"

            previous = row
            previous_risk = current_risk

        terminal_row = ordered[terminal_index] if terminal_index is not None else None
        exchange_complete = bool(
            terminal_row is not None and terminal_row["exchange_exposure_complete"]
        )
        visible_complete = bool(
            terminal_row is not None and terminal_row["visible_exposure_complete"]
        )
        return {
            "rows": ordered,
            "visible_interval_count": visible_interval_count,
            "visible_risk_duration_ns": visible_risk_duration_ns,
            "visible_exposure_btc_s": visible_expected,
            "risk_spell_count": risk_spell_count,
            "cancel_reject_active_count": cancel_reject_active_count,
            "cancel_reject_partially_filled_count": (
                cancel_reject_partially_filled_count
            ),
            "exchange_exposure_btc_s": (
                exchange_expected if exchange_started and exchange_valid else None
            ),
            "exchange_exposure_valid": exchange_valid,
            "exchange_invalid_reason_reported": exchange_invalid_reason_reported,
            "visible_exposure_complete": visible_complete,
            "exchange_exposure_complete": exchange_complete,
            "terminal_observed": terminal_row is not None,
            "local_shutdown_censor": local_censor,
        }

    def _audit_pair(
        self,
        client_order_id: str,
        legacy_rows: list[_LegacyEvent],
        v2_summary: Mapping[str, object],
    ) -> dict[str, object]:
        legacy = sorted(legacy_rows, key=lambda row: row.event_seq)
        v2_rows = list(v2_summary["rows"])
        if [row.event_seq for row in legacy] != sorted({row.event_seq for row in legacy}):
            self._mismatch(
                "legacy_order_sequence_not_strict",
                client_order_id=client_order_id,
                observed=[row.event_seq for row in legacy],
            )
        if len(legacy) != len(v2_rows):
            self._mismatch(
                "comparable_event_count_mismatch",
                client_order_id=client_order_id,
                expected=len(legacy),
                observed=len(v2_rows),
            )

        visibility_clock_pairs = 0
        exchange_clock_pairs = 0
        full_dual_clock_pairs = 0
        legacy_visibility_complete = len(legacy) == len(v2_rows)
        previous_legacy: _LegacyEvent | None = None
        legacy_visible_exposure = 0.0
        legacy_previous_risk = False
        legacy_risk_spell_count = 0

        for ordinal, (old, new) in enumerate(
            zip(legacy, v2_rows, strict=False),
            start=1,
        ):
            new_event, new_reason = _normalize_v2_event(new)
            if old.event != new_event:
                self._mismatch(
                    "event_type_mismatch",
                    client_order_id=client_order_id,
                    sequence=ordinal,
                    expected=old.event,
                    observed=new_event,
                )
            if old.terminal_reason != new_reason:
                self._mismatch(
                    "terminal_reason_mismatch",
                    client_order_id=client_order_id,
                    sequence=ordinal,
                    expected=old.terminal_reason,
                    observed=new_reason,
                )
            if not _close(
                old.initial_quantity,
                new["initial_quantity"],
                absolute=_QUANTITY_TOLERANCE,
            ):
                self._mismatch(
                    "initial_quantity_mismatch",
                    client_order_id=client_order_id,
                    sequence=ordinal,
                    expected=old.initial_quantity,
                    observed=new["initial_quantity"],
                )
            if not _close(
                old.remaining_after,
                new["remaining_quantity_after"],
                absolute=_QUANTITY_TOLERANCE,
            ):
                self._mismatch(
                    "remaining_quantity_mismatch",
                    client_order_id=client_order_id,
                    sequence=ordinal,
                    expected=old.remaining_after,
                    observed=new["remaining_quantity_after"],
                )
            expected_phase = _legacy_expected_phase(old)
            if expected_phase not in {None, str(new["phase_after"])}:
                self._mismatch(
                    "phase_after_mismatch",
                    client_order_id=client_order_id,
                    sequence=ordinal,
                    expected=expected_phase,
                    observed=new["phase_after"],
                )

            if old.visibility_ts_ns is not None:
                visibility_clock_pairs += 1
                if int(new["event_visibility_ts_ns"]) != old.visibility_ts_ns:
                    self._mismatch(
                        "visibility_clock_mismatch",
                        client_order_id=client_order_id,
                        sequence=ordinal,
                        expected=old.visibility_ts_ns,
                        observed=new["event_visibility_ts_ns"],
                    )
            else:
                legacy_visibility_complete = False
            if old.exchange_ts_ns is not None:
                exchange_clock_pairs += 1
                if new["event_exchange_ts_ns"] != old.exchange_ts_ns:
                    self._mismatch(
                        "exchange_clock_mismatch",
                        client_order_id=client_order_id,
                        sequence=ordinal,
                        expected=old.exchange_ts_ns,
                        observed=new["event_exchange_ts_ns"],
                    )
            if old.visibility_ts_ns is not None and old.exchange_ts_ns is not None:
                full_dual_clock_pairs += 1

            old_phase = _legacy_expected_phase(old)
            old_risk = bool(
                old_phase in _RISK_PHASES
                and old.remaining_after > TERMINAL_REMAINDER_ABS_TOLERANCE_BTC
            )
            if old_risk and not legacy_previous_risk:
                legacy_risk_spell_count += 1
            if previous_legacy is not None and legacy_previous_risk:
                if previous_legacy.visibility_ts_ns is None or old.visibility_ts_ns is None:
                    legacy_visibility_complete = False
                else:
                    legacy_visible_exposure += (
                        previous_legacy.remaining_after
                        * (old.visibility_ts_ns - previous_legacy.visibility_ts_ns)
                        / 1_000_000_000.0
                    )
            previous_legacy = old
            legacy_previous_risk = old_risk

        if legacy_risk_spell_count != int(v2_summary["risk_spell_count"]):
            self._mismatch(
                "risk_spell_count_mismatch",
                client_order_id=client_order_id,
                expected=legacy_risk_spell_count,
                observed=v2_summary["risk_spell_count"],
            )
        if legacy_visibility_complete and not _close(
            legacy_visible_exposure,
            v2_summary["visible_exposure_btc_s"],
            absolute=_EXPOSURE_ABS_TOLERANCE,
        ):
            self._mismatch(
                "legacy_v2_visible_exposure_mismatch",
                client_order_id=client_order_id,
                expected=legacy_visible_exposure,
                observed=v2_summary["visible_exposure_btc_s"],
            )

        return {
            "visibility_clock_pairs": visibility_clock_pairs,
            "exchange_clock_pairs": exchange_clock_pairs,
            "full_dual_clock_pairs": full_dual_clock_pairs,
            "legacy_visible_exposure_comparison_supported": (legacy_visibility_complete),
        }

    def finalize(self) -> dict[str, object]:
        legacy_keys = set(self._legacy)
        v2_keys = set(self._v2)
        for missing in sorted(legacy_keys - v2_keys):
            self._mismatch(
                "legacy_lifecycle_missing_v2",
                client_order_id=missing,
            )
        for missing in sorted(v2_keys - legacy_keys):
            self._mismatch(
                "v2_lifecycle_missing_legacy",
                client_order_id=missing,
            )

        coverage = Counter()
        terminal_counts = Counter()
        visible_intervals = 0
        visible_risk_duration_ns = 0
        visible_exposure_total = 0.0
        exchange_exposure_total = 0.0
        cancel_reject_active_count = 0
        cancel_reject_partially_filled_count = 0
        for client_order_id in sorted(v2_keys):
            summary = self._audit_v2_trace(
                client_order_id,
                self._v2[client_order_id],
            )
            visible_intervals += int(summary["visible_interval_count"])
            visible_risk_duration_ns += int(summary["visible_risk_duration_ns"])
            visible_exposure_total += float(summary["visible_exposure_btc_s"])
            cancel_reject_active_count += int(summary["cancel_reject_active_count"])
            cancel_reject_partially_filled_count += int(
                summary["cancel_reject_partially_filled_count"]
            )
            if summary["exchange_exposure_btc_s"] is not None:
                exchange_exposure_total += float(summary["exchange_exposure_btc_s"])
                coverage["exchange_exposure_available_lifecycles"] += 1
            if summary["exchange_exposure_valid"]:
                coverage["exchange_exposure_valid_lifecycles"] += 1
            if summary["visible_exposure_complete"]:
                coverage["visible_exposure_complete_lifecycles"] += 1
            if summary["exchange_exposure_complete"]:
                coverage["exchange_exposure_complete_lifecycles"] += 1
            if summary["local_shutdown_censor"]:
                terminal_counts["local_shutdown_censor"] += 1
            elif summary["terminal_observed"]:
                terminal_counts["exchange_terminal"] += 1
            else:
                terminal_counts["missing_terminal_or_censor"] += 1
                self._mismatch(
                    "v2_lifecycle_missing_terminal_or_censor",
                    client_order_id=client_order_id,
                )
            if not summary["exchange_invalid_reason_reported"]:
                coverage["exchange_invalid_reason_missing_lifecycles"] += 1

            if client_order_id in self._legacy:
                pair = self._audit_pair(
                    client_order_id,
                    self._legacy[client_order_id],
                    summary,
                )
                coverage.update(
                    {
                        "visibility_clock_pairs": pair["visibility_clock_pairs"],
                        "exchange_clock_pairs": pair["exchange_clock_pairs"],
                        "full_dual_clock_pairs": pair["full_dual_clock_pairs"],
                        "legacy_visible_exposure_supported_lifecycles": int(
                            pair["legacy_visible_exposure_comparison_supported"]
                        ),
                    }
                )

        mismatch_counts = dict(sorted(self._mismatch_counts.items()))
        identity_sequence_codes = {
            "legacy_duplicate_event_sequence",
            "legacy_event_sequence_not_stream_monotone",
            "legacy_order_sequence_not_strict",
            "v2_client_symbol_identity_mismatch",
            "v2_client_submit_identity_mismatch",
            "v2_duplicate_event_id",
            "v2_lifecycle_id_changed",
            "v2_lifecycle_id_client_suffix_mismatch",
            "v2_lifecycle_id_reused_across_clients",
            "v2_lifecycle_sequence_not_contiguous",
            "legacy_lifecycle_missing_v2",
            "v2_lifecycle_missing_legacy",
        }
        event_codes = {
            "comparable_event_count_mismatch",
            "event_type_mismatch",
            "phase_after_mismatch",
        }
        clock_codes = {
            "visibility_clock_mismatch",
            "exchange_clock_mismatch",
            "v2_visibility_clock_regressed",
            "v2_exchange_clock_after_visibility",
            "v2_exchange_clock_regressed",
        }
        quantity_codes = {
            "initial_quantity_mismatch",
            "remaining_quantity_mismatch",
        }
        terminal_codes = {
            "terminal_reason_mismatch",
            "v2_lifecycle_missing_terminal_or_censor",
            "v2_terminal_remainder_not_exact_zero",
        }
        cancel_reject_codes = {
            "v2_cancel_reject_continuation_phase_mismatch",
            "v2_cancel_reject_fill_risk_not_resumed",
        }
        visible_exposure_codes = {
            "v2_visible_exposure_mismatch",
            "legacy_v2_visible_exposure_mismatch",
            "risk_spell_count_mismatch",
        }
        exchange_exposure_codes = {
            "v2_exchange_exposure_mismatch",
            "v2_invalid_exchange_exposure_not_null",
            "v2_exchange_invalid_reason_missing",
        }
        post_terminal_codes = {
            "v2_event_after_terminal_or_censor",
            "v2_fill_risk_after_terminal_or_censor",
        }

        def clean(codes: set[str]) -> bool:
            return not any(self._mismatch_counts[code] for code in codes)

        gates = {
            "identity_and_sequence_lockstep": clean(identity_sequence_codes),
            "event_and_phase_lockstep": clean(event_codes),
            "applicable_clock_lockstep": clean(clock_codes),
            "remaining_quantity_lockstep": clean(quantity_codes),
            "terminal_reason_lockstep": clean(terminal_codes),
            "cancel_reject_risk_set_continuation": clean(cancel_reject_codes),
            "risk_interval_and_visible_eq_accounting": clean(visible_exposure_codes),
            "exchange_eq_accounting": clean(exchange_exposure_codes),
            "zero_post_terminal_events": clean(post_terminal_codes),
            "exchange_invalid_reason_coverage_complete": (
                coverage["exchange_invalid_reason_missing_lifecycles"] == 0
            ),
        }
        mechanics_passed = bool(
            legacy_keys
            and v2_keys
            and legacy_keys == v2_keys
            and all(gates.values())
            and not mismatch_counts
        )
        report: dict[str, object] = {
            "schema_version": REPORT_SCHEMA_VERSION,
            "identity": IDENTITY,
            "day": self.day,
            "scope": {
                "mechanics_only": True,
                "economic_outcomes_read": False,
                "q90_action_enabled": False,
                "pnl_or_policy_authority_granted": False,
            },
            "counts": {
                "legacy_rows_seen": self._legacy_rows_seen,
                "legacy_mechanics_rows": self._legacy_mechanics_rows,
                "legacy_lifecycle_count": len(legacy_keys),
                "v2_rows_seen": self._v2_rows_seen,
                "v2_lifecycle_count": len(v2_keys),
                "matched_lifecycle_count": len(legacy_keys & v2_keys),
                "v2_unique_event_id_count": len(self._v2_event_ids),
                "cancel_reject_to_active_count": cancel_reject_active_count,
                "cancel_reject_to_partially_filled_count": (
                    cancel_reject_partially_filled_count
                ),
                "visible_risk_interval_count": visible_intervals,
                "visible_risk_duration_ns": visible_risk_duration_ns,
            },
            "terminal_counts": dict(sorted(terminal_counts.items())),
            "coverage": {
                **dict(sorted(coverage.items())),
                "visible_quantity_time_exposure_btc_s": visible_exposure_total,
                "exchange_quantity_time_exposure_btc_s": exchange_exposure_total,
                "legacy_ignored_event_counts": dict(sorted(self._legacy_ignored_events.items())),
                "legacy_dual_clock_is_not_assumed": True,
            },
            "mismatch_counts": mismatch_counts,
            "mismatch_samples": sorted(
                self._mismatch_samples,
                key=lambda row: (
                    str(row["code"]),
                    str(row["client_order_id"]),
                    int(row["sequence"]),
                ),
            ),
            "gates": gates,
            "mechanics_lockstep_passed": mechanics_passed,
            "formal_40_day_execution_performed": False,
            "permissions": {
                "cif_training": False,
                "economic_evaluation": False,
                "q90_action": False,
                "live_deployment": False,
            },
        }
        report["canonical_report_sha256"] = _canonical_sha256(report)
        return report


def audit_lifecycle_event_streams(
    *,
    day: str,
    legacy_rows: Iterable[Mapping[str, object]],
    journal_v2_rows: Iterable[Mapping[str, object]],
    mismatch_sample_limit: int = 25,
) -> dict[str, object]:
    auditor = LifecycleV2EventLockstepAuditor(
        day=day,
        mismatch_sample_limit=mismatch_sample_limit,
    )
    for row in legacy_rows:
        auditor.observe_legacy(row)
    for row in journal_v2_rows:
        auditor.observe_v2(row)
    return auditor.finalize()


def _parquet_rows(
    paths: Sequence[Path],
    *,
    required_columns: Sequence[str],
    optional_columns: Sequence[str] = (),
    batch_size: int = 4096,
) -> Iterator[dict[str, object]]:
    for path in paths:
        parquet = pq.ParquetFile(path)
        names = set(parquet.schema_arrow.names)
        missing = sorted(set(required_columns) - names)
        if missing:
            raise LifecycleLockstepInputError(f"{path} missing required columns: {missing}")
        columns = [column for column in (*required_columns, *optional_columns) if column in names]
        for batch in parquet.iter_batches(
            batch_size=max(1, int(batch_size)),
            columns=columns,
        ):
            yield from batch.to_pylist()


def _resolve_parquet_paths(values: Sequence[str | Path]) -> list[Path]:
    resolved: list[Path] = []
    for value in values:
        path = Path(value).expanduser().resolve()
        if path.is_file():
            if path.suffix != ".parquet":
                raise LifecycleLockstepInputError(f"lockstep input must be Parquet: {path}")
            resolved.append(path)
            continue
        if not path.is_dir():
            raise LifecycleLockstepInputError(f"lockstep input does not exist: {path}")
        resolved.extend(sorted(path.rglob("*.parquet")))
    unique = sorted(set(resolved))
    if not unique:
        raise LifecycleLockstepInputError("lockstep input contains no Parquet files")
    return unique


def _artifact_identities(paths: Sequence[Path]) -> list[dict[str, object]]:
    return [
        {
            "path": str(path),
            "size_bytes": path.stat().st_size,
            "sha256": _file_sha256(path),
        }
        for path in paths
    ]


def atomic_write_lockstep_report(path: str | Path, report: Mapping[str, object]) -> Path:
    destination = Path(path).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(report)
    claimed = str(payload.pop("canonical_report_sha256", ""))
    actual = _canonical_sha256(payload)
    if claimed != actual:
        raise LifecycleLockstepInputError("lockstep report canonical hash mismatch")
    envelope = {
        "schema_version": ATOMIC_ENVELOPE_SCHEMA_VERSION,
        "report_sha256": claimed,
        "report": dict(report),
    }
    serialized = _canonical_bytes(envelope) + b"\n"
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=".tmp",
        dir=destination.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        directory_fd = os.open(destination.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)
    return destination


def audit_lifecycle_parquet_day(
    *,
    day: str,
    legacy_paths: Sequence[str | Path],
    journal_v2_paths: Sequence[str | Path],
    output_path: str | Path,
    batch_size: int = 4096,
    mismatch_sample_limit: int = 25,
) -> dict[str, object]:
    legacy = _resolve_parquet_paths(legacy_paths)
    journal = _resolve_parquet_paths(journal_v2_paths)
    report = audit_lifecycle_event_streams(
        day=day,
        legacy_rows=_parquet_rows(
            legacy,
            required_columns=_LEGACY_REQUIRED_COLUMNS,
            optional_columns=_LEGACY_OPTIONAL_COLUMNS,
            batch_size=batch_size,
        ),
        journal_v2_rows=_parquet_rows(
            journal,
            required_columns=ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS,
            batch_size=batch_size,
        ),
        mismatch_sample_limit=mismatch_sample_limit,
    )
    report_without_hash = dict(report)
    report_without_hash.pop("canonical_report_sha256")
    report_without_hash["input_artifacts"] = {
        "legacy": _artifact_identities(legacy),
        "journal_v2": _artifact_identities(journal),
    }
    report_without_hash["canonical_report_sha256"] = _canonical_sha256(report_without_hash)
    atomic_write_lockstep_report(output_path, report_without_hash)
    return report_without_hash


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", required=True)
    parser.add_argument("--legacy-parquet", action="append", required=True)
    parser.add_argument("--journal-v2-parquet", action="append", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--batch-size", type=int, default=4096)
    parser.add_argument("--mismatch-sample-limit", type=int, default=25)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    report = audit_lifecycle_parquet_day(
        day=args.day,
        legacy_paths=args.legacy_parquet,
        journal_v2_paths=args.journal_v2_parquet,
        output_path=args.output,
        batch_size=args.batch_size,
        mismatch_sample_limit=args.mismatch_sample_limit,
    )
    print(json.dumps(report, sort_keys=True, indent=2))
    return 0 if bool(report["mechanics_lockstep_passed"]) else 2


if __name__ == "__main__":
    raise SystemExit(main())
