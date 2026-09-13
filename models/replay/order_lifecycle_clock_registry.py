"""Epoch-scoped order lifecycle clocks and competing-risk estimators.

This module is mechanics-only.  It consumes the authoritative lifecycle
journal, assigns each order to one fully bound baseline epoch, and reports
calendar-time and fill-risk-time Aalen-Johansen tables without reading PnL,
reward, or markout data.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Any

from execution.order_lifecycle import FILL_RISK_PHASES, OrderLifecyclePhase
from execution.order_lifecycle_journal import OrderLifecycleJournalRow
from models.replay.baseline_epoch_manifest import (
    load_and_validate_manifest,
    validate_baseline_epoch_manifest,
)

SCHEMA_VERSION = "order_lifecycle_clock_registry.v1"
ESTIMANDS = frozenset({"first_fill", "exchange_terminal"})
CLOCKS = frozenset({"calendar_visible", "risk_visible"})

JOURNAL_COLUMNS = frozenset(field.name for field in fields(OrderLifecycleJournalRow))
FILL_EVENTS = frozenset({"partial_fill", "full_fill"})


@dataclass(frozen=True)
class OrderLifecycleEpisode:
    baseline_epoch_id: str
    client_order_id: str
    side: str
    submitted_ts_ns: int
    activation_ts_ns: int
    terminal_ts_ns: int
    first_fill_ts_ns: int
    first_fill_risk_time_s: float | None
    terminal_risk_time_s: float
    calendar_first_fill_time_s: float | None
    calendar_terminal_time_s: float
    terminal_competing_risk: str | None
    terminal_reason: str
    censor_type: str
    entered_fill_risk_set: bool
    carryover_crossed_epoch: bool
    left_truncated: bool
    partial_fill_count: int
    cancel_request_count: int
    cancel_reject_count: int
    quantity_time_exposure_visible_btc_s: float
    quantity_time_exposure_exchange_btc_s: float | None
    exchange_exposure_valid: bool
    exchange_exposure_complete: bool


def _forbid_extra_journal_columns(rows: Sequence[Mapping[str, Any]]) -> None:
    for row in rows:
        extras = set(row).difference(JOURNAL_COLUMNS)
        if extras:
            raise ValueError(f"lifecycle journal contains non-contract columns: {sorted(extras)}")


def _epoch_for_timestamp(
    timestamp_ns: int,
    epochs: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any] | None:
    for epoch in epochs:
        if int(epoch["start_ts_ns"]) <= timestamp_ns < int(epoch["end_ts_ns"]):
            return epoch
    return None


def _terminal_risk(reason: str, route: str, remaining: float) -> tuple[str | None, str]:
    normalized = reason.strip().lower()
    if remaining <= 1e-12 or normalized in {"full_fill", "filled_before_cancel_ack"}:
        return "full_fill", ""
    if normalized in {"cancel_ack", "cancel_ack_reconciled"}:
        return "cancel_ack", ""
    if normalized == "rejected":
        return "reject", ""
    if normalized == "expired":
        return "expiry", ""
    if normalized in {"administrative_cancel", "local_shutdown_cancel", "shutdown"}:
        return None, "local_shutdown_without_exchange_terminal_confirmation"
    if route == "TERMINAL_COMPLETE":
        return "full_fill", ""
    raise ValueError(f"unsupported terminal competing risk: reason={reason!r} route={route!r}")


def _risk_time_to_event(rows: Sequence[Mapping[str, Any]], event_index: int) -> float:
    total_ns = 0
    for previous, current in zip(rows[:event_index], rows[1 : event_index + 1], strict=False):
        if str(previous["phase_after"]) in {phase.value for phase in FILL_RISK_PHASES}:
            delta = int(current["visibility_ts_ns"]) - int(previous["visibility_ts_ns"])
            if delta < 0:
                raise ValueError("lifecycle visibility time regressed")
            total_ns += delta
    return total_ns / 1_000_000_000.0


def build_order_lifecycle_episodes(
    rows: Sequence[Mapping[str, Any]],
    manifest: Mapping[str, Any],
) -> tuple[OrderLifecycleEpisode, ...]:
    """Collapse authoritative journal rows into epoch-owned order episodes."""

    validate_baseline_epoch_manifest(manifest)
    _forbid_extra_journal_columns(rows)
    epochs = list(manifest["epochs"])
    by_order: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        order_id = str(row.get("client_order_id", "")).strip()
        if not order_id or order_id.lower() == "nan":
            raise ValueError("lifecycle journal client_order_id must be non-empty")
        if row.get("schema_version") != "order_lifecycle_journal.v1":
            raise ValueError("unsupported lifecycle journal schema")
        by_order.setdefault(order_id, []).append(row)

    episodes: list[OrderLifecycleEpisode] = []
    for order_id, order_rows in by_order.items():
        ordered = sorted(order_rows, key=lambda row: int(row["lifecycle_sequence"]))
        sequences = [int(row["lifecycle_sequence"]) for row in ordered]
        if sequences != list(range(1, len(sequences) + 1)):
            raise ValueError(f"order {order_id} lifecycle sequence is not contiguous")
        submitted = ordered[0]
        if str(submitted["lifecycle_event"]) != "submit":
            raise ValueError(f"order {order_id} does not start with submit")
        epoch = _epoch_for_timestamp(int(submitted["visibility_ts_ns"]), epochs)
        if epoch is None:
            raise ValueError(f"order {order_id} submit is outside a bound epoch")
        if not bool(epoch["lifecycle_estimation_authorized"]):
            raise ValueError(f"order {order_id} belongs to unauthorized epoch")
        epoch_id = str(epoch["epoch_id"])
        carryover_crossed_epoch = False
        for row in ordered:
            owner = _epoch_for_timestamp(int(row["visibility_ts_ns"]), epochs)
            if owner is None:
                raise ValueError(f"order {order_id} leaves manifest scope")
            carryover_crossed_epoch |= str(owner["epoch_id"]) != epoch_id

        activation_indices = [
            index
            for index, row in enumerate(ordered)
            if str(row["lifecycle_event"]) == "activate"
        ]
        if len(activation_indices) > 1:
            raise ValueError(f"order {order_id} contains duplicate activation")
        terminal_indices = [
            index
            for index, row in enumerate(ordered)
            if str(row["phase_after"]) == OrderLifecyclePhase.EXCHANGE_TERMINAL.value
        ]
        if len(terminal_indices) != 1:
            raise ValueError(f"order {order_id} must contain exactly one exchange terminal")
        terminal_index = terminal_indices[0]
        terminal_row = ordered[terminal_index]
        entered_fill_risk_set = bool(activation_indices)
        activation_index = activation_indices[0] if entered_fill_risk_set else None
        if activation_index is not None and terminal_index <= activation_index:
            raise ValueError(f"order {order_id} terminal precedes activation")
        if not entered_fill_risk_set and str(terminal_row["terminal_reason"]).lower() not in {
            "rejected",
            "expired",
            "administrative_cancel",
            "local_shutdown_cancel",
            "shutdown",
        }:
            raise ValueError(f"order {order_id} never entered its fill-risk set")
        first_fill_indices = [
            index
            for index, row in enumerate(ordered[: terminal_index + 1])
            if str(row["lifecycle_event"]) in FILL_EVENTS
        ]
        first_fill_index = first_fill_indices[0] if first_fill_indices else None
        activation_ts = (
            int(ordered[activation_index]["visibility_ts_ns"])
            if activation_index is not None
            else 0
        )
        terminal_ts = int(terminal_row["visibility_ts_ns"])
        if entered_fill_risk_set and terminal_ts < activation_ts:
            raise ValueError(f"order {order_id} has negative calendar duration")
        first_fill_ts = (
            int(ordered[first_fill_index]["visibility_ts_ns"])
            if first_fill_index is not None
            else 0
        )
        exchange_exposure = terminal_row["quantity_time_exposure_exchange_btc_s"]
        terminal_risk, censor_type = _terminal_risk(
            str(terminal_row["terminal_reason"]),
            str(terminal_row["terminal_policy_route"]),
            float(terminal_row["remaining_quantity_after"]),
        )
        episodes.append(
            OrderLifecycleEpisode(
                baseline_epoch_id=epoch_id,
                client_order_id=order_id,
                side=str(terminal_row["side"]).upper(),
                submitted_ts_ns=int(submitted["visibility_ts_ns"]),
                activation_ts_ns=activation_ts,
                terminal_ts_ns=terminal_ts,
                first_fill_ts_ns=first_fill_ts,
                first_fill_risk_time_s=(
                    _risk_time_to_event(ordered, first_fill_index)
                    if first_fill_index is not None
                    else None
                ),
                terminal_risk_time_s=_risk_time_to_event(ordered, terminal_index),
                calendar_first_fill_time_s=(
                    (first_fill_ts - activation_ts) / 1_000_000_000.0
                    if first_fill_index is not None and entered_fill_risk_set
                    else None
                ),
                calendar_terminal_time_s=(
                    (terminal_ts - activation_ts) / 1_000_000_000.0
                    if entered_fill_risk_set
                    else 0.0
                ),
                terminal_competing_risk=terminal_risk,
                terminal_reason=str(terminal_row["terminal_reason"]),
                censor_type=censor_type,
                entered_fill_risk_set=entered_fill_risk_set,
                carryover_crossed_epoch=carryover_crossed_epoch,
                left_truncated=False,
                partial_fill_count=sum(
                    int(str(row["lifecycle_event"]) == "partial_fill")
                    for row in ordered
                ),
                cancel_request_count=sum(
                    int(str(row["lifecycle_event"]) == "cancel_request")
                    for row in ordered
                ),
                cancel_reject_count=sum(
                    int(str(row["lifecycle_event"]) == "cancel_rejected")
                    for row in ordered
                ),
                quantity_time_exposure_visible_btc_s=float(
                    terminal_row["quantity_time_exposure_visible_btc_s"]
                ),
                quantity_time_exposure_exchange_btc_s=(
                    float(exchange_exposure) if exchange_exposure is not None else None
                ),
                exchange_exposure_valid=bool(terminal_row["exchange_exposure_valid"]),
                exchange_exposure_complete=bool(
                    terminal_row["exchange_exposure_complete"]
                ),
            )
        )
    return tuple(sorted(episodes, key=lambda row: (row.baseline_epoch_id, row.activation_ts_ns)))


def _event_observation(
    episode: OrderLifecycleEpisode,
    *,
    estimand: str,
    clock: str,
) -> tuple[float, str]:
    if estimand not in ESTIMANDS:
        raise ValueError(f"unsupported lifecycle estimand: {estimand}")
    if clock not in CLOCKS:
        raise ValueError(f"unsupported lifecycle clock: {clock}")
    if estimand == "first_fill" and episode.first_fill_ts_ns > 0:
        duration = (
            episode.calendar_first_fill_time_s
            if clock == "calendar_visible"
            else episode.first_fill_risk_time_s
        )
        assert duration is not None
        return float(duration), "first_fill"
    duration = (
        episode.calendar_terminal_time_s
        if clock == "calendar_visible"
        else episode.terminal_risk_time_s
    )
    cause = episode.terminal_competing_risk or "right_censor"
    return float(duration), cause


def aalen_johansen_table(
    episodes: Sequence[OrderLifecycleEpisode],
    *,
    estimand: str,
    clock: str,
    grid_ms: int = 100,
    max_time_s: float = 30.0,
) -> list[dict[str, float | int]]:
    """Return a discrete Aalen-Johansen event table on a fixed risk grid."""

    if grid_ms <= 0:
        raise ValueError("grid_ms must be positive")
    if max_time_s <= 0:
        raise ValueError("max_time_s must be positive")
    observations = [
        _event_observation(episode, estimand=estimand, clock=clock)
        for episode in episodes
    ]
    if not observations:
        return []
    causes = sorted({cause for _, cause in observations if cause != "right_censor"})
    survival = 1.0
    cif = {cause: 0.0 for cause in causes}
    table: list[dict[str, float | int]] = []
    steps = int(round(max_time_s * 1_000.0 / grid_ms))
    for step in range(1, steps + 1):
        upper = step * grid_ms / 1_000.0
        lower = (step - 1) * grid_ms / 1_000.0
        at_risk = sum(int(duration > lower) for duration, _ in observations)
        event_counts = {
            cause: sum(
                int(lower < duration <= upper and observed_cause == cause)
                for duration, observed_cause in observations
            )
            for cause in causes
        }
        all_events = sum(event_counts.values())
        survival_before = survival
        if at_risk > 0:
            for cause, count in event_counts.items():
                cif[cause] += survival_before * count / at_risk
            survival *= 1.0 - all_events / at_risk
        row: dict[str, float | int] = {
            "time_s": upper,
            "at_risk": at_risk,
            "all_events": all_events,
            "survival": survival,
        }
        for cause in causes:
            row[f"events_{cause}"] = event_counts[cause]
            row[f"cif_{cause}"] = cif[cause]
        table.append(row)
    return table


def build_clock_registry(
    rows: Sequence[Mapping[str, Any]],
    manifest: Mapping[str, Any],
    *,
    grid_ms: int = 100,
    max_time_s: float = 30.0,
) -> dict[str, Any]:
    """Build epoch-specific mechanics outputs; never pool epochs in v1."""

    episodes = build_order_lifecycle_episodes(rows, manifest)
    outputs: list[dict[str, Any]] = []
    for epoch in manifest["epochs"]:
        if not bool(epoch["lifecycle_estimation_authorized"]):
            continue
        epoch_id = str(epoch["epoch_id"])
        owned = [row for row in episodes if row.baseline_epoch_id == epoch_id]
        selected = [
            row
            for row in owned
            if row.entered_fill_risk_set
            and not row.left_truncated
            and not row.carryover_crossed_epoch
        ]
        outputs.append(
            {
                "baseline_epoch_id": epoch_id,
                "baseline_epoch_identity_sha256": epoch["identity_sha256"],
                "owned_order_count": len(owned),
                "order_count": len(selected),
                "never_activated_count": sum(
                    int(not row.entered_fill_risk_set) for row in owned
                ),
                "carryover_crossed_epoch_count": sum(
                    int(row.carryover_crossed_epoch) for row in owned
                ),
                "local_shutdown_censor_count": sum(
                    int(bool(row.censor_type)) for row in selected
                ),
                "exchange_exposure_valid_count": sum(
                    int(row.exchange_exposure_valid) for row in selected
                ),
                "exchange_exposure_complete_count": sum(
                    int(row.exchange_exposure_complete) for row in selected
                ),
                "terminal_risk_counts": {
                    cause: sum(
                        int(row.terminal_competing_risk == cause) for row in selected
                    )
                    for cause in sorted(
                        {
                            row.terminal_competing_risk
                            for row in selected
                            if row.terminal_competing_risk is not None
                        }
                    )
                },
                "first_fill_calendar_visible": aalen_johansen_table(
                    selected,
                    estimand="first_fill",
                    clock="calendar_visible",
                    grid_ms=grid_ms,
                    max_time_s=max_time_s,
                ),
                "first_fill_risk_visible": aalen_johansen_table(
                    selected,
                    estimand="first_fill",
                    clock="risk_visible",
                    grid_ms=grid_ms,
                    max_time_s=max_time_s,
                ),
                "exchange_terminal_calendar_visible": aalen_johansen_table(
                    selected,
                    estimand="exchange_terminal",
                    clock="calendar_visible",
                    grid_ms=grid_ms,
                    max_time_s=max_time_s,
                ),
                "exchange_terminal_risk_visible": aalen_johansen_table(
                    selected,
                    estimand="exchange_terminal",
                    clock="risk_visible",
                    grid_ms=grid_ms,
                    max_time_s=max_time_s,
                ),
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "baseline_epoch_manifest_sha256": manifest["canonical_manifest_sha256"],
        "grid_ms": int(grid_ms),
        "right_censor_s": float(max_time_s),
        "calendar_time_output": True,
        "risk_time_output": True,
        "competing_risk_identity_preserved": True,
        "economic_outcomes_read": False,
        "pooled_estimation_authorized": False,
        "episode_count": len(episodes),
        "episode_fingerprint_sha256": hashlib.sha256(
            json.dumps(
                [asdict(row) for row in episodes],
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest(),
        "epochs": outputs,
    }


def load_registry_inputs(
    *,
    journal_path: Path,
    manifest_path: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Read a JSON-lines journal and a validated baseline epoch manifest."""

    rows = [
        json.loads(line)
        for line in journal_path.expanduser().resolve().read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    return rows, load_and_validate_manifest(manifest_path)
