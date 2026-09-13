"""Outcome-blind plans for paired restart-aware continuous replay.

This module owns calendar, restart, and state-transfer semantics.  It does not
invoke a strategy engine and deliberately has no result aggregation API.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from data.quality.calendar_gap_manifest import validate_calendar_continuity_manifest
from models.replay.continuous_accounting import (
    SCHEMA_VERSION as CONTINUOUS_ACCOUNTING_CONTRACT_ID,
)

SCHEMA_VERSION = "restart_aware_continuous_ab_plan.v1"
SOURCE_MANIFEST_SCHEMA_VERSION = "restart_aware_continuous_source_manifest.v1"
DAY_MS = 86_400_000


class ContinuousABPreflightError(RuntimeError):
    """Raised before an outcome-bearing replay can start."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def ordered_days(start_day: str, end_day: str) -> tuple[str, ...]:
    start = date.fromisoformat(start_day)
    end = date.fromisoformat(end_day)
    if end < start:
        raise ContinuousABPreflightError("calendar end precedes calendar start")
    return tuple(
        (start + timedelta(days=offset)).isoformat() for offset in range((end - start).days + 1)
    )


def day_start_ms(day: str) -> int:
    return int(datetime.fromisoformat(day).replace(tzinfo=UTC).timestamp() * 1_000)


@dataclass(frozen=True, slots=True)
class FrozenRestartInterval:
    gap_id: str
    offline_start_ts_ms: int
    resume_ts_ms: int
    quote_stop_ts_ms: int | None
    cancel_deadline_ts_ms: int | None
    warmup_lookback_start_ts_ms: int
    clears_orders: bool = True
    clears_queue: bool = True
    clears_pending_cancel: bool = True
    clears_runtime_hazard: bool = True
    preserves_cash: bool = True
    preserves_inventory: bool = True
    preserves_average_entry_price: bool = True
    preserves_economic_campaign: bool = True

    def validate(self, *, panel_start_ms: int, panel_end_ms: int) -> None:
        if not self.gap_id:
            raise ContinuousABPreflightError("restart interval has an empty gap id")
        if not (panel_start_ms <= self.offline_start_ts_ms < self.resume_ts_ms <= panel_end_ms):
            raise ContinuousABPreflightError("restart interval lies outside the 71-day panel")
        if self.quote_stop_ts_ms is None:
            if self.offline_start_ts_ms != panel_start_ms:
                raise ContinuousABPreflightError(
                    "only the initial panel gap may omit an observable cancel drain"
                )
            if self.cancel_deadline_ts_ms is not None:
                raise ContinuousABPreflightError("initial gap cannot have a cancel deadline")
        elif not (
            panel_start_ms
            <= self.quote_stop_ts_ms
            <= int(self.cancel_deadline_ts_ms or -1)
            < self.offline_start_ts_ms
        ):
            raise ContinuousABPreflightError("restart cancel-drain timestamps are invalid")
        if self.warmup_lookback_start_ts_ms >= self.resume_ts_ms:
            raise ContinuousABPreflightError("restart warmup window is empty")
        required = (
            self.clears_orders,
            self.clears_queue,
            self.clears_pending_cancel,
            self.clears_runtime_hazard,
            self.preserves_cash,
            self.preserves_inventory,
            self.preserves_average_entry_price,
            self.preserves_economic_campaign,
        )
        if not all(required):
            raise ContinuousABPreflightError("restart state-transfer contract is incomplete")


@dataclass(frozen=True, slots=True)
class SourceArtifactBinding:
    role: str
    path: str
    size_bytes: int
    sha256: str

    def validate(self, *, verify_hash: bool) -> None:
        if self.role not in {"bbo", "l2", "feature"}:
            raise ContinuousABPreflightError(f"unsupported source artifact role: {self.role}")
        path = Path(self.path).expanduser().resolve()
        if not path.is_file():
            raise ContinuousABPreflightError(f"missing {self.role} source artifact: {path}")
        if int(self.size_bytes) <= 0 or path.stat().st_size != int(self.size_bytes):
            raise ContinuousABPreflightError(f"{self.role} source artifact size mismatch: {path}")
        if len(self.sha256) != 64:
            raise ContinuousABPreflightError(f"{self.role} source artifact lacks SHA256")
        if verify_hash and sha256_file(path) != self.sha256:
            raise ContinuousABPreflightError(f"{self.role} source artifact SHA256 mismatch: {path}")


@dataclass(frozen=True, slots=True)
class CalendarSourceBinding:
    day: str
    book_identity: str
    book_root: str
    bbo_path: str
    l2_path: str
    feature_identity: str
    feature_path: str
    artifacts: tuple[SourceArtifactBinding, ...]
    exact_queue_authority: bool
    exact_lifecycle_authority: bool
    continuous_economic_sensitivity_authority: bool = True

    def validate(self, *, verify_hashes: bool = False) -> None:
        if self.book_identity not in {
            "native_available",
            "provider_normalized_sensitivity",
        }:
            raise ContinuousABPreflightError(f"unsupported execution book: {self.day}")
        for role in ("bbo_path", "l2_path", "feature_path"):
            path = Path(getattr(self, role)).expanduser().resolve()
            if not path.is_file():
                raise ContinuousABPreflightError(f"missing {role} for {self.day}: {path}")
        expected_authority = self.book_identity == "native_available"
        if (
            self.exact_queue_authority is not expected_authority
            or self.exact_lifecycle_authority is not expected_authority
        ):
            raise ContinuousABPreflightError(f"execution authority tier mismatch: {self.day}")
        if self.continuous_economic_sensitivity_authority is not True:
            raise ContinuousABPreflightError(
                f"continuous sensitivity authority is disabled: {self.day}"
            )
        by_role = {artifact.role: artifact for artifact in self.artifacts}
        if set(by_role) != {"bbo", "l2", "feature"} or len(by_role) != len(self.artifacts):
            raise ContinuousABPreflightError(f"source artifact roles are incomplete: {self.day}")
        expected_paths = {
            "bbo": Path(self.bbo_path).expanduser().resolve(),
            "l2": Path(self.l2_path).expanduser().resolve(),
            "feature": Path(self.feature_path).expanduser().resolve(),
        }
        for role, artifact in by_role.items():
            if Path(artifact.path).expanduser().resolve() != expected_paths[role]:
                raise ContinuousABPreflightError(f"{role} source path binding differs: {self.day}")
            artifact.validate(verify_hash=verify_hashes)

    def identity_payload(self) -> dict[str, Any]:
        return {
            "day": self.day,
            "book_identity": self.book_identity,
            "book_root": self.book_root,
            "feature_identity": self.feature_identity,
            "exact_queue_authority": self.exact_queue_authority,
            "exact_lifecycle_authority": self.exact_lifecycle_authority,
            "continuous_economic_sensitivity_authority": (
                self.continuous_economic_sensitivity_authority
            ),
            "artifacts": [asdict(row) for row in self.artifacts],
        }


def source_artifact_manifest_payload(
    bindings: Sequence[CalendarSourceBinding],
) -> dict[str, Any]:
    rows = [binding.identity_payload() for binding in bindings]
    return {
        "schema_version": SOURCE_MANIFEST_SCHEMA_VERSION,
        "day_count": len(rows),
        "native_day_count": sum(row["book_identity"] == "native_available" for row in rows),
        "provider_normalized_sensitivity_day_count": sum(
            row["book_identity"] == "provider_normalized_sensitivity" for row in rows
        ),
        "artifact_count": sum(len(row["artifacts"]) for row in rows),
        "provider_exact_queue_authority": False,
        "provider_exact_lifecycle_authority": False,
        "days": rows,
    }


@dataclass(frozen=True, slots=True)
class ContinuousABPlan:
    calendar_start_day: str
    calendar_end_day: str
    calendar_days: tuple[str, ...]
    source_bindings: tuple[CalendarSourceBinding, ...]
    restart_intervals: tuple[FrozenRestartInterval, ...]
    source_calendar_manifest_path: str
    source_calendar_manifest_sha256: str
    source_artifact_manifest_sha256: str
    restart_timeline_sha256: str
    control_state_namespace: str = "control_v9_10s"
    candidate_state_namespace: str = "candidate_f03_1s"
    utc_midnight_policy: str = "accounting_only_no_flatten_no_state_reset"
    gap_policy: str = "clear_orders_queue_then_past_only_warmup"
    all_calendar_days_trade: bool = True
    same_restart_manifest_both_arms: bool = True
    arm_economic_state_isolated: bool = True
    results_read: bool = False
    schema_version: str = SCHEMA_VERSION

    @property
    def calendar_day_count(self) -> int:
        return len(self.calendar_days)

    def validate(self, *, verify_source_hashes: bool = False) -> None:
        if self.calendar_days != ordered_days(self.calendar_start_day, self.calendar_end_day):
            raise ContinuousABPreflightError("calendar is not a complete ordered UTC range")
        if len(self.calendar_days) != 71:
            raise ContinuousABPreflightError("F03 continuous A/B requires exactly 71 days")
        if tuple(row.day for row in self.source_bindings) != self.calendar_days:
            raise ContinuousABPreflightError("source bindings do not cover all 71 trading days")
        for row in self.source_bindings:
            row.validate(verify_hashes=verify_source_hashes)
        native_count = sum(row.book_identity == "native_available" for row in self.source_bindings)
        provider_count = sum(
            row.book_identity == "provider_normalized_sensitivity" for row in self.source_bindings
        )
        if (native_count, provider_count) != (52, 19):
            raise ContinuousABPreflightError(
                "F03 continuous source strata must remain exactly 52 native and 19 provider"
            )
        source_payload = source_artifact_manifest_payload(self.source_bindings)
        if canonical_sha256(source_payload) != self.source_artifact_manifest_sha256:
            raise ContinuousABPreflightError("source artifact manifest hash mismatch")
        if self.control_state_namespace == self.candidate_state_namespace:
            raise ContinuousABPreflightError("paired arms must not share mutable state")
        if not (
            self.all_calendar_days_trade
            and self.same_restart_manifest_both_arms
            and self.arm_economic_state_isolated
        ):
            raise ContinuousABPreflightError("paired continuous replay invariants are disabled")
        if self.results_read:
            raise ContinuousABPreflightError("execution plan must remain outcome blind")
        panel_start = day_start_ms(self.calendar_start_day)
        panel_end = day_start_ms(self.calendar_end_day) + DAY_MS
        previous_end = panel_start
        for interval in self.restart_intervals:
            interval.validate(panel_start_ms=panel_start, panel_end_ms=panel_end)
            if interval.offline_start_ts_ms < previous_end:
                raise ContinuousABPreflightError("restart intervals overlap")
            previous_end = interval.resume_ts_ms
        payload = restart_timeline_payload(
            calendar_days=self.calendar_days,
            intervals=self.restart_intervals,
        )
        if canonical_sha256(payload) != self.restart_timeline_sha256:
            raise ContinuousABPreflightError("restart timeline hash mismatch")

    def identity_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "calendar_start_day": self.calendar_start_day,
            "calendar_end_day": self.calendar_end_day,
            "calendar_days": list(self.calendar_days),
            "source_bindings": [asdict(row) for row in self.source_bindings],
            "restart_intervals": [asdict(row) for row in self.restart_intervals],
            "source_calendar_manifest_path": self.source_calendar_manifest_path,
            "source_calendar_manifest_sha256": self.source_calendar_manifest_sha256,
            "source_artifact_manifest_sha256": self.source_artifact_manifest_sha256,
            "restart_timeline_sha256": self.restart_timeline_sha256,
            "control_state_namespace": self.control_state_namespace,
            "candidate_state_namespace": self.candidate_state_namespace,
            "utc_midnight_policy": self.utc_midnight_policy,
            "gap_policy": self.gap_policy,
            "all_calendar_days_trade": self.all_calendar_days_trade,
            "same_restart_manifest_both_arms": self.same_restart_manifest_both_arms,
            "arm_economic_state_isolated": self.arm_economic_state_isolated,
            "results_read": self.results_read,
        }


def _merge_gaps(rows: Sequence[tuple[int, int]]) -> tuple[tuple[int, int], ...]:
    merged: list[list[int]] = []
    for start, end in sorted(rows):
        if start >= end:
            continue
        if not merged or start > merged[-1][1]:
            merged.append([start, end])
        else:
            merged[-1][1] = max(merged[-1][1], end)
    return tuple((row[0], row[1]) for row in merged)


def restart_timeline_payload(
    *,
    calendar_days: Sequence[str],
    intervals: Sequence[FrozenRestartInterval],
) -> dict[str, Any]:
    return {
        "calendar_days": list(calendar_days),
        "restart_intervals": [asdict(row) for row in intervals],
        "utc_midnight_policy": "accounting_only_no_flatten_no_state_reset",
        "gap_policy": "clear_orders_queue_then_past_only_warmup",
        "all_calendar_days_trade": True,
    }


def build_complete_calendar_plan(
    *,
    calendar_manifest_path: Path,
    source_rows: Sequence[Mapping[str, Any]],
    start_day: str,
    end_day: str,
) -> ContinuousABPlan:
    """Build an all-days-trade plan without inheriting grade-based day exclusion."""

    path = calendar_manifest_path.expanduser().resolve()
    try:
        raw = path.read_bytes()
        manifest = json.loads(raw)
        validate_calendar_continuity_manifest(manifest)
    except (OSError, json.JSONDecodeError, KeyError, ValueError) as exc:
        raise ContinuousABPreflightError("invalid calendar continuity manifest") from exc
    days = ordered_days(start_day, end_day)
    if len(days) != 71:
        raise ContinuousABPreflightError("requested F03 calendar is not 71 days")
    manifest_days = tuple(
        str(row["day"])
        for row in manifest["day_sources"]
        if start_day <= str(row["day"]) <= end_day
    )
    if manifest_days != days:
        raise ContinuousABPreflightError("source calendar does not contain the exact 71 days")

    rows_by_day = {str(row.get("day", "")): row for row in source_rows}
    if tuple(rows_by_day) != days:
        raise ContinuousABPreflightError("resolved source rows must be exact and chronological")
    bindings = tuple(
        CalendarSourceBinding(
            day=day,
            book_identity=str(rows_by_day[day]["book_identity"]),
            book_root=str(rows_by_day[day]["book_root"]),
            bbo_path=str(rows_by_day[day]["bbo_path"]),
            l2_path=str(rows_by_day[day]["l2_path"]),
            feature_identity=str(rows_by_day[day]["feature_identity"]),
            feature_path=str(rows_by_day[day]["feature_path"]),
            artifacts=tuple(
                SourceArtifactBinding(
                    role=role,
                    path=str(rows_by_day[day]["artifacts"][role]["path"]),
                    size_bytes=int(rows_by_day[day]["artifacts"][role]["size_bytes"]),
                    sha256=str(rows_by_day[day]["artifacts"][role]["sha256"]),
                )
                for role in ("bbo", "l2", "feature")
            ),
            exact_queue_authority=str(rows_by_day[day]["book_identity"]) == "native_available",
            exact_lifecycle_authority=str(rows_by_day[day]["book_identity"]) == "native_available",
        )
        for day in days
    )

    panel_start = day_start_ms(start_day)
    panel_end = day_start_ms(end_day) + DAY_MS
    raw_gaps = [
        (
            max(panel_start, int(row["offline_start_ts_ms"])),
            min(panel_end, int(row["resume_ts_ms"])),
        )
        for row in manifest.get("observed_data_gaps", ())
        if start_day <= str(row["day"]) <= end_day
    ]
    merged = _merge_gaps(raw_gaps)
    for day in days:
        start = day_start_ms(day)
        end = start + DAY_MS
        offline_ms = sum(
            max(0, min(end, gap_end) - max(start, gap_start)) for gap_start, gap_end in merged
        )
        if offline_ms >= DAY_MS:
            raise ContinuousABPreflightError(
                f"frozen restart schedule leaves no trading interval on {day}"
            )
    cancel_drain_ms = int(manifest["cancel_drain_ms"])
    warmup_ms = int(manifest["feature_warmup_lookback_s"]) * 1_000
    intervals: list[FrozenRestartInterval] = []
    for ordinal, (offline_start, resume) in enumerate(merged, start=1):
        initial = offline_start == panel_start
        intervals.append(
            FrozenRestartInterval(
                gap_id=f"f03-71d-restart-G{ordinal:03d}",
                offline_start_ts_ms=offline_start,
                resume_ts_ms=resume,
                quote_stop_ts_ms=None if initial else offline_start - cancel_drain_ms,
                cancel_deadline_ts_ms=None if initial else offline_start - 1,
                warmup_lookback_start_ts_ms=resume - warmup_ms,
            )
        )
    timeline_payload = restart_timeline_payload(
        calendar_days=days,
        intervals=intervals,
    )
    plan = ContinuousABPlan(
        calendar_start_day=start_day,
        calendar_end_day=end_day,
        calendar_days=days,
        source_bindings=bindings,
        restart_intervals=tuple(intervals),
        source_calendar_manifest_path=str(path),
        source_calendar_manifest_sha256=hashlib.sha256(raw).hexdigest(),
        source_artifact_manifest_sha256=canonical_sha256(
            source_artifact_manifest_payload(bindings)
        ),
        restart_timeline_sha256=canonical_sha256(timeline_payload),
    )
    plan.validate(verify_source_hashes=True)
    return plan


@dataclass(frozen=True, slots=True)
class PairedExecutionRequest:
    sequence: int
    day: str
    control_state_namespace: str
    candidate_state_namespace: str
    restart_timeline_sha256: str
    control_policy: str
    candidate_policy: str
    source: CalendarSourceBinding
    source_artifact_manifest_sha256: str
    exact_queue_authority: bool
    exact_lifecycle_authority: bool
    continuous_economic_sensitivity_authority: bool
    restart_boundary_contract_id: str = "restart_boundary_contract.v1"
    continuous_accounting_contract_id: str = CONTINUOUS_ACCOUNTING_CONTRACT_ID
    cancel_drain_requires_terminal_ack_or_fill: bool = True
    warmup_requires_source_coverage: bool = True
    feature_ready_not_after_decision: bool = True
    exact_authority_excludes_frozen_restart_gaps: bool = True
    carry_economic_state_across_midnight: bool = True
    carry_orders_queue_across_midnight: bool = True
    reset_transient_state_only_at_frozen_gaps: bool = True
    execution_plan_skeleton: bool = True
    full_path_executed: bool = False
    read_results: bool = False


def paired_execution_requests(
    plan: ContinuousABPlan,
    *,
    control_policy: str,
    candidate_policy: str,
) -> tuple[PairedExecutionRequest, ...]:
    """Create paired daily source requests for one continuous engine timeline.

    A request is a source-admission unit, not a fresh-start simulation unit.
    The executor must keep each arm's checkpoint alive between requests.
    """

    plan.validate()
    if not control_policy or not candidate_policy or control_policy == candidate_policy:
        raise ContinuousABPreflightError("paired policies must be distinct and non-empty")
    return tuple(
        PairedExecutionRequest(
            sequence=index,
            day=day,
            control_state_namespace=plan.control_state_namespace,
            candidate_state_namespace=plan.candidate_state_namespace,
            restart_timeline_sha256=plan.restart_timeline_sha256,
            control_policy=control_policy,
            candidate_policy=candidate_policy,
            source=source,
            source_artifact_manifest_sha256=plan.source_artifact_manifest_sha256,
            exact_queue_authority=source.exact_queue_authority,
            exact_lifecycle_authority=source.exact_lifecycle_authority,
            continuous_economic_sensitivity_authority=(
                source.continuous_economic_sensitivity_authority
            ),
        )
        for index, (day, source) in enumerate(
            zip(plan.calendar_days, plan.source_bindings, strict=True), start=1
        )
    )
