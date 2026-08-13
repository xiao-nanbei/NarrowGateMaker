"""Natural-canonical confirmation ledger for one exact F05 final artifact.

Only naturally produced canonical data intervals that begin strictly after the
final artifact lock may enter this ledger. Every interval is bound to the exact
artifact, compiler, source, runtime, and executable policy identities; carries a
valid three-clock audit; uses the common coverage/fallback contract; and embeds
one repeated sequential paired-replay receipt. F05 companion, shadow, observer,
or candidate-specific sources are rejected. Historical Development, Validation,
and sealed-holdout panels are rejected by construction.

This is an evidence-admission layer.  Reaching the frozen active-day minimum
only makes the ledger ready for a separate formal evaluation.  It never grants
research, action, or live authority.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_repeated_policy_v1 as repeated,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_full_multiscale_successor_nested_oof_v1 import (
    OOF_EVIDENCE_SCOPE,
)
from strategy.boolean_cooldown_coverage import BooleanCooldownCoverageReason

IDENTITY = (
    "causal_multichannel_window_boolean_cooldown_"
    "full_multiscale_successor_confirmation_v1"
)
SCHEMA_VERSION = f"{IDENTITY}.v1"
CONTRACT_SCHEMA_VERSION = f"{SCHEMA_VERSION}.contract"
THREE_CLOCK_SCHEMA_VERSION = f"{SCHEMA_VERSION}.three_clock_audit"
ARM_COVERAGE_SCHEMA_VERSION = f"{SCHEMA_VERSION}.arm_coverage_audit"
COMMON_COVERAGE_SCHEMA_VERSION = f"{SCHEMA_VERSION}.common_coverage_audit"
SESSION_SCHEMA_VERSION = f"{SCHEMA_VERSION}.session"
LEDGER_SCHEMA_VERSION = f"{SCHEMA_VERSION}.ledger"

MINIMUM_ACTIVE_UTC_DAYS_FLOOR = 30
SOURCE_PANEL_ROLE = "post_lock_natural_canonical_chronological_data"
EXACT_ARTIFACT_EVIDENCE_SCOPE = "exact_final_artifact_prospective_evidence"
LEARNING_ALGORITHM_EVIDENCE_SCOPE = OOF_EVIDENCE_SCOPE
CLOCK_FIELDS = (
    "receive_ts_ns",
    "feature_ready_ts_ns",
    "policy_decision_ts_ns",
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class ConfirmationError(ValueError):
    """Raised when prospective evidence violates the frozen contract."""


class ConfirmationStatus(StrEnum):
    PENDING_MINIMUM_ACTIVE_DAYS = "pending_minimum_active_utc_days"
    MINIMUM_MET_FORMAL_EVALUATION_REQUIRED = (
        "minimum_active_utc_days_met_formal_evaluation_required"
    )


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _require_sha256(value: Any, label: str) -> str:
    normalized = str(value)
    if _SHA256_RE.fullmatch(normalized) is None:
        raise ConfirmationError(f"{label} is not a lowercase SHA256")
    return normalized


def _require_identity(value: Any, label: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ConfirmationError(f"{label} is empty")
    return normalized


def _require_nonnegative_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ConfirmationError(f"{label} must be a nonnegative integer")
    return value


def _parse_utc(value: Any, label: str) -> datetime:
    raw = str(value)
    if not raw.endswith("Z"):
        raise ConfirmationError(f"{label} must be canonical UTC ending in Z")
    try:
        parsed = datetime.fromisoformat(raw[:-1] + "+00:00")
    except ValueError as exc:
        raise ConfirmationError(f"{label} is not an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ConfirmationError(f"{label} is not UTC")
    canonical = parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if canonical != raw:
        raise ConfirmationError(f"{label} is not canonical")
    return parsed.astimezone(UTC)


def _datetime_to_ns(value: datetime) -> int:
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = value - epoch
    return (
        delta.days * 86_400_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
    )


def _normalize_day(value: Any) -> str:
    raw = str(value)
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as exc:
        raise ConfirmationError("UTC day must use YYYY-MM-DD") from exc
    if parsed.isoformat() != raw:
        raise ConfirmationError("UTC day is not canonical")
    return raw


def _hash_body(instance: Any, hash_field: str) -> tuple[dict[str, Any], str]:
    body = asdict(instance)
    supplied = str(body.pop(hash_field))
    return body, supplied


@dataclass(frozen=True, slots=True)
class FinalArtifactConfirmationContract:
    """Frozen identity and admission boundary for one final refit artifact."""

    schema_version: str
    identity: str
    final_artifact_identity: str
    final_artifact_locked_at_utc: str
    final_artifact_manifest_sha256: str
    final_policy_sha256: str
    compiler_identity: str
    compiler_sha256: str
    source_identity: str
    source_sha256: str
    runtime_identity: str
    runtime_sha256: str
    learning_algorithm_identity: str
    learning_algorithm_oof_artifact_sha256: str
    candidate_target_side: str
    coverage_contract_sha256: str
    fallback_contract_sha256: str
    minimum_active_utc_days: int
    exact_artifact_evidence_scope: str
    learning_algorithm_evidence_scope: str
    historical_development_allowed: bool
    validation_allowed: bool
    sealed_holdout_allowed: bool
    research_supported: bool
    action_authorized: bool
    live_policy_authorized: bool
    contract_sha256: str

    def __post_init__(self) -> None:
        if self.schema_version != CONTRACT_SCHEMA_VERSION or self.identity != IDENTITY:
            raise ConfirmationError("confirmation contract identity drifted")
        for name in (
            "final_artifact_identity",
            "compiler_identity",
            "source_identity",
            "runtime_identity",
            "learning_algorithm_identity",
        ):
            _require_identity(getattr(self, name), name)
        forbidden_source_tokens = ("companion", "shadow", "f05_prospective")
        normalized_source = self.source_identity.lower()
        source_tokens = set(re.split(r"[^a-z0-9]+", normalized_source))
        if "canonical" not in source_tokens or any(
            token in normalized_source for token in forbidden_source_tokens
        ):
            raise ConfirmationError(
                "confirmation source must be natural canonical data, not an F05 research stream"
            )
        _parse_utc(self.final_artifact_locked_at_utc, "artifact lock time")
        for name in (
            "final_artifact_manifest_sha256",
            "final_policy_sha256",
            "compiler_sha256",
            "source_sha256",
            "runtime_sha256",
            "learning_algorithm_oof_artifact_sha256",
            "coverage_contract_sha256",
            "fallback_contract_sha256",
            "contract_sha256",
        ):
            _require_sha256(getattr(self, name), name)
        repeated.CandidateTargetSide(self.candidate_target_side)
        minimum_days = _require_nonnegative_int(
            self.minimum_active_utc_days,
            "minimum active UTC days",
        )
        if minimum_days < MINIMUM_ACTIVE_UTC_DAYS_FLOOR:
            raise ConfirmationError("minimum active UTC days cannot be below 30")
        if (
            self.exact_artifact_evidence_scope != EXACT_ARTIFACT_EVIDENCE_SCOPE
            or self.learning_algorithm_evidence_scope
            != LEARNING_ALGORITHM_EVIDENCE_SCOPE
        ):
            raise ConfirmationError("confirmation evidence scopes drifted")
        if any(
            (
                self.historical_development_allowed,
                self.validation_allowed,
                self.sealed_holdout_allowed,
                self.research_supported,
                self.action_authorized,
                self.live_policy_authorized,
            )
        ):
            raise ConfirmationError("confirmation contract claims forbidden authority")
        body, supplied = _hash_body(self, "contract_sha256")
        if _canonical_sha256(body) != supplied:
            raise ConfirmationError("confirmation contract hash drifted")

    @classmethod
    def build(
        cls,
        *,
        final_artifact_identity: str,
        final_artifact_locked_at_utc: str,
        final_artifact_manifest_sha256: str,
        final_policy_sha256: str,
        compiler_identity: str,
        compiler_sha256: str,
        source_identity: str,
        source_sha256: str,
        runtime_identity: str,
        runtime_sha256: str,
        learning_algorithm_identity: str,
        learning_algorithm_oof_artifact_sha256: str,
        candidate_target_side: str,
        coverage_contract_sha256: str,
        fallback_contract_sha256: str,
        minimum_active_utc_days: int = MINIMUM_ACTIVE_UTC_DAYS_FLOOR,
    ) -> FinalArtifactConfirmationContract:
        body = {
            "schema_version": CONTRACT_SCHEMA_VERSION,
            "identity": IDENTITY,
            "final_artifact_identity": final_artifact_identity,
            "final_artifact_locked_at_utc": final_artifact_locked_at_utc,
            "final_artifact_manifest_sha256": final_artifact_manifest_sha256,
            "final_policy_sha256": final_policy_sha256,
            "compiler_identity": compiler_identity,
            "compiler_sha256": compiler_sha256,
            "source_identity": source_identity,
            "source_sha256": source_sha256,
            "runtime_identity": runtime_identity,
            "runtime_sha256": runtime_sha256,
            "learning_algorithm_identity": learning_algorithm_identity,
            "learning_algorithm_oof_artifact_sha256": (
                learning_algorithm_oof_artifact_sha256
            ),
            "candidate_target_side": candidate_target_side,
            "coverage_contract_sha256": coverage_contract_sha256,
            "fallback_contract_sha256": fallback_contract_sha256,
            "minimum_active_utc_days": minimum_active_utc_days,
            "exact_artifact_evidence_scope": EXACT_ARTIFACT_EVIDENCE_SCOPE,
            "learning_algorithm_evidence_scope": (
                LEARNING_ALGORITHM_EVIDENCE_SCOPE
            ),
            "historical_development_allowed": False,
            "validation_allowed": False,
            "sealed_holdout_allowed": False,
            "research_supported": False,
            "action_authorized": False,
            "live_policy_authorized": False,
        }
        return cls(**body, contract_sha256=_canonical_sha256(body))

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def validate_session(self, session: ProspectiveConfirmationSession) -> None:
        _validate_session_against_contract(self, session)

    @classmethod
    def from_dict(
        cls, payload: Mapping[str, Any]
    ) -> FinalArtifactConfirmationContract:
        return cls(**dict(payload))


@dataclass(frozen=True, slots=True)
class ThreeClockAudit:
    """Compact audit of receive <= feature-ready <= policy-decision clocks."""

    schema_version: str
    event_count: int
    receive_clock_present_count: int
    feature_ready_clock_present_count: int
    policy_decision_clock_present_count: int
    ordering_valid_count: int
    missing_clock_count: int
    ordering_violation_count: int
    duplicate_event_id_count: int
    first_receive_ts_ns: int | None
    last_policy_decision_ts_ns: int | None
    clock_trace_sha256: str
    valid: bool
    audit_sha256: str

    def __post_init__(self) -> None:
        if self.schema_version != THREE_CLOCK_SCHEMA_VERSION:
            raise ConfirmationError("three-clock audit schema drifted")
        for name in (
            "event_count",
            "receive_clock_present_count",
            "feature_ready_clock_present_count",
            "policy_decision_clock_present_count",
            "ordering_valid_count",
            "missing_clock_count",
            "ordering_violation_count",
            "duplicate_event_id_count",
        ):
            _require_nonnegative_int(getattr(self, name), name)
        for name in ("clock_trace_sha256", "audit_sha256"):
            _require_sha256(getattr(self, name), name)
        if self.valid:
            if self.event_count <= 0:
                raise ConfirmationError("valid three-clock audit is empty")
            if not (
                self.receive_clock_present_count
                == self.feature_ready_clock_present_count
                == self.policy_decision_clock_present_count
                == self.ordering_valid_count
                == self.event_count
            ):
                raise ConfirmationError("valid three-clock audit has incomplete clocks")
            if any(
                (
                    self.missing_clock_count,
                    self.ordering_violation_count,
                    self.duplicate_event_id_count,
                )
            ):
                raise ConfirmationError("valid three-clock audit has violations")
            if (
                self.first_receive_ts_ns is None
                or self.last_policy_decision_ts_ns is None
                or self.first_receive_ts_ns > self.last_policy_decision_ts_ns
            ):
                raise ConfirmationError("valid three-clock bounds are invalid")
        body, supplied = _hash_body(self, "audit_sha256")
        if _canonical_sha256(body) != supplied:
            raise ConfirmationError("three-clock audit hash drifted")

    @classmethod
    def from_rows(cls, rows: Sequence[Mapping[str, Any]]) -> ThreeClockAudit:
        normalized_rows: list[dict[str, Any]] = []
        event_ids: list[str] = []
        present = {field: 0 for field in CLOCK_FIELDS}
        ordering_valid = 0
        missing = 0
        violations = 0
        receives: list[int] = []
        decisions: list[int] = []
        for index, row in enumerate(rows):
            if not isinstance(row, Mapping):
                raise ConfirmationError("three-clock row is not an object")
            event_id = str(row.get("event_id", "")).strip()
            if not event_id:
                event_id = f"missing-event-id:{index}"
            event_ids.append(event_id)
            values: dict[str, int | None] = {}
            for field in CLOCK_FIELDS:
                raw = row.get(field)
                if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
                    values[field] = None
                else:
                    values[field] = raw
                    present[field] += 1
            if any(values[field] is None for field in CLOCK_FIELDS):
                missing += 1
            else:
                receive = int(values["receive_ts_ns"])
                feature_ready = int(values["feature_ready_ts_ns"])
                decision = int(values["policy_decision_ts_ns"])
                receives.append(receive)
                decisions.append(decision)
                if receive <= feature_ready <= decision:
                    ordering_valid += 1
                else:
                    violations += 1
            normalized_rows.append({"event_id": event_id, **values})
        duplicate_ids = len(event_ids) - len(set(event_ids))
        valid = bool(
            normalized_rows
            and missing == 0
            and violations == 0
            and duplicate_ids == 0
            and ordering_valid == len(normalized_rows)
        )
        body = {
            "schema_version": THREE_CLOCK_SCHEMA_VERSION,
            "event_count": len(normalized_rows),
            "receive_clock_present_count": present["receive_ts_ns"],
            "feature_ready_clock_present_count": present["feature_ready_ts_ns"],
            "policy_decision_clock_present_count": present["policy_decision_ts_ns"],
            "ordering_valid_count": ordering_valid,
            "missing_clock_count": missing,
            "ordering_violation_count": violations,
            "duplicate_event_id_count": duplicate_ids,
            "first_receive_ts_ns": min(receives) if receives else None,
            "last_policy_decision_ts_ns": max(decisions) if decisions else None,
            "clock_trace_sha256": _canonical_sha256(normalized_rows),
            "valid": valid,
        }
        return cls(**body, audit_sha256=_canonical_sha256(body))

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> ThreeClockAudit:
        return cls(**dict(payload))


@dataclass(frozen=True, slots=True)
class ArmCoverageAudit:
    """One arm's counts under the shared coverage and fallback vocabulary."""

    schema_version: str
    arm: str
    coverage_contract_sha256: str
    fallback_contract_sha256: str
    event_count: int
    coverage_reason_counts: Mapping[str, int]
    fallback_reason_counts: Mapping[str, int]
    nonbaseline_action_count: int
    audit_sha256: str

    def __post_init__(self) -> None:
        if self.schema_version != ARM_COVERAGE_SCHEMA_VERSION:
            raise ConfirmationError("arm coverage schema drifted")
        if self.arm not in repeated.ARMS:
            raise ConfirmationError("arm coverage identity is invalid")
        for name in (
            "coverage_contract_sha256",
            "fallback_contract_sha256",
            "audit_sha256",
        ):
            _require_sha256(getattr(self, name), name)
        event_count = _require_nonnegative_int(self.event_count, "coverage event count")
        nonbaseline = _require_nonnegative_int(
            self.nonbaseline_action_count,
            "nonbaseline action count",
        )
        known_reasons = {reason.value for reason in BooleanCooldownCoverageReason}
        observed_reasons = set(self.coverage_reason_counts)
        if not observed_reasons.issubset(known_reasons):
            raise ConfirmationError("coverage audit contains an unknown reason")
        reason_total = 0
        for reason, count in self.coverage_reason_counts.items():
            _require_identity(reason, "coverage reason")
            reason_total += _require_nonnegative_int(count, f"coverage count {reason}")
        if reason_total != event_count:
            raise ConfirmationError("coverage reason census does not match event count")
        fallback_total = 0
        for reason, count in self.fallback_reason_counts.items():
            _require_identity(reason, "fallback reason")
            fallback_total += _require_nonnegative_int(count, f"fallback count {reason}")
        if fallback_total > event_count:
            raise ConfirmationError("fallback reason census exceeds event count")
        active_count = int(
            self.coverage_reason_counts.get(
                BooleanCooldownCoverageReason.ELIGIBLE_FEATURE_READY.value,
                0,
            )
        )
        if nonbaseline > active_count:
            raise ConfirmationError("nonbaseline actions exceed feature-ready support")
        if self.arm == repeated.CONTROL_ARM and nonbaseline != 0:
            raise ConfirmationError("exact B0 cannot carry candidate nonbaseline actions")
        body, supplied = _hash_body(self, "audit_sha256")
        if _canonical_sha256(body) != supplied:
            raise ConfirmationError("arm coverage audit hash drifted")

    @classmethod
    def build(
        cls,
        *,
        arm: str,
        coverage_contract_sha256: str,
        fallback_contract_sha256: str,
        coverage_reason_counts: Mapping[str, int],
        fallback_reason_counts: Mapping[str, int],
        nonbaseline_action_count: int,
    ) -> ArmCoverageAudit:
        reasons = dict(
            sorted((str(key), value) for key, value in coverage_reason_counts.items())
        )
        fallbacks = dict(
            sorted((str(key), value) for key, value in fallback_reason_counts.items())
        )
        body = {
            "schema_version": ARM_COVERAGE_SCHEMA_VERSION,
            "arm": arm,
            "coverage_contract_sha256": coverage_contract_sha256,
            "fallback_contract_sha256": fallback_contract_sha256,
            "event_count": sum(reasons.values()),
            "coverage_reason_counts": reasons,
            "fallback_reason_counts": fallbacks,
            "nonbaseline_action_count": nonbaseline_action_count,
        }
        return cls(**body, audit_sha256=_canonical_sha256(body))

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> ArmCoverageAudit:
        return cls(**dict(payload))


@dataclass(frozen=True, slots=True)
class CommonCoverageAudit:
    schema_version: str
    control: ArmCoverageAudit
    candidate: ArmCoverageAudit
    same_coverage_contract: bool
    same_fallback_contract: bool
    audit_sha256: str

    def __post_init__(self) -> None:
        if self.schema_version != COMMON_COVERAGE_SCHEMA_VERSION:
            raise ConfirmationError("common coverage schema drifted")
        if (
            self.control.arm != repeated.CONTROL_ARM
            or self.candidate.arm != repeated.CANDIDATE_ARM
        ):
            raise ConfirmationError("common coverage arms drifted")
        expected_same_coverage = (
            self.control.coverage_contract_sha256
            == self.candidate.coverage_contract_sha256
        )
        expected_same_fallback = (
            self.control.fallback_contract_sha256
            == self.candidate.fallback_contract_sha256
        )
        if (
            self.same_coverage_contract is not True
            or self.same_fallback_contract is not True
            or not expected_same_coverage
            or not expected_same_fallback
        ):
            raise ConfirmationError("paired arms do not share coverage/fallback contracts")
        body, supplied = _hash_body(self, "audit_sha256")
        if _canonical_sha256(body) != supplied:
            raise ConfirmationError("common coverage audit hash drifted")

    @classmethod
    def build(
        cls,
        *,
        control: ArmCoverageAudit,
        candidate: ArmCoverageAudit,
    ) -> CommonCoverageAudit:
        body = {
            "schema_version": COMMON_COVERAGE_SCHEMA_VERSION,
            "control": control,
            "candidate": candidate,
            "same_coverage_contract": (
                control.coverage_contract_sha256
                == candidate.coverage_contract_sha256
            ),
            "same_fallback_contract": (
                control.fallback_contract_sha256
                == candidate.fallback_contract_sha256
            ),
        }
        hash_body = {
            **body,
            "control": control.to_dict(),
            "candidate": candidate.to_dict(),
        }
        return cls(**body, audit_sha256=_canonical_sha256(hash_body))

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> CommonCoverageAudit:
        values = dict(payload)
        values["control"] = ArmCoverageAudit.from_dict(values["control"])
        values["candidate"] = ArmCoverageAudit.from_dict(values["candidate"])
        return cls(**values)


@dataclass(frozen=True, slots=True)
class ProspectiveConfirmationSession:
    """One post-lock exact-artifact confirmation session."""

    schema_version: str
    identity: str
    session_id: str
    utc_day: str
    session_started_at_utc: str
    session_ended_at_utc: str
    source_panel_role: str
    final_artifact_identity: str
    final_artifact_manifest_sha256: str
    compiler_sha256: str
    source_sha256: str
    runtime_sha256: str
    policy_sha256: str
    learning_algorithm_identity: str
    learning_algorithm_oof_artifact_sha256: str
    evidence_scope: str
    learning_algorithm_oof_evidence_accepted: bool
    exact_final_artifact_oof_available: bool
    paired_receipt: Mapping[str, Any]
    paired_receipt_sha256: str
    three_clock_audit: ThreeClockAudit
    common_coverage_audit: CommonCoverageAudit
    historical_development_read: bool
    validation_read: bool
    sealed_holdout_read: bool
    formal_session_valid: bool
    research_supported: bool
    action_authorized: bool
    live_policy_authorized: bool
    session_evidence_sha256: str

    def __post_init__(self) -> None:
        if self.schema_version != SESSION_SCHEMA_VERSION or self.identity != IDENTITY:
            raise ConfirmationError("confirmation session identity drifted")
        _require_identity(self.session_id, "session id")
        day = _normalize_day(self.utc_day)
        started = _parse_utc(self.session_started_at_utc, "session start")
        ended = _parse_utc(self.session_ended_at_utc, "session end")
        if not started < ended:
            raise ConfirmationError("confirmation session interval is empty")
        if started.date().isoformat() != day or ended.date().isoformat() != day:
            raise ConfirmationError("confirmation session must remain within one UTC day")
        if self.source_panel_role != SOURCE_PANEL_ROLE:
            raise ConfirmationError("confirmation session uses a forbidden panel role")
        for name in (
            "final_artifact_identity",
            "learning_algorithm_identity",
        ):
            _require_identity(getattr(self, name), name)
        for name in (
            "final_artifact_manifest_sha256",
            "compiler_sha256",
            "source_sha256",
            "runtime_sha256",
            "policy_sha256",
            "learning_algorithm_oof_artifact_sha256",
            "paired_receipt_sha256",
            "session_evidence_sha256",
        ):
            _require_sha256(getattr(self, name), name)
        if self.evidence_scope != EXACT_ARTIFACT_EVIDENCE_SCOPE:
            raise ConfirmationError("session is not exact-artifact prospective evidence")
        if any(
            (
                self.learning_algorithm_oof_evidence_accepted,
                self.exact_final_artifact_oof_available,
                self.historical_development_read,
                self.validation_read,
                self.sealed_holdout_read,
                self.research_supported,
                self.action_authorized,
                self.live_policy_authorized,
            )
        ):
            raise ConfirmationError("session claims forbidden evidence or authority")
        if self.formal_session_valid is not True:
            raise ConfirmationError("invalid session cannot enter confirmation")
        try:
            receipt = repeated.PairedSequentialReplayReceipt.from_dict(
                self.paired_receipt
            )
        except (TypeError, ValueError, repeated.RepeatedPolicyBridgeError) as exc:
            raise ConfirmationError("paired sequential receipt is invalid") from exc
        if receipt.receipt_sha256 != self.paired_receipt_sha256:
            raise ConfirmationError("paired receipt hash binding drifted")
        if receipt.utc_day != self.utc_day:
            raise ConfirmationError("paired receipt belongs to a different UTC day")
        if (
            receipt.segment_start_utc != self.session_started_at_utc
            or receipt.segment_end_utc != self.session_ended_at_utc
        ):
            raise ConfirmationError("paired receipt session interval drifted")
        if (
            receipt.executed_artifact_scope
            != repeated.ExecutedArtifactScope.FINAL_FULL_DEVELOPMENT_REFIT
            or receipt.exact_final_artifact_oof_available
            or not receipt.repeated_sequential_policy
            or receipt.one_shot_effect_aggregation_used
            or not receipt.formal_denominator_eligible
        ):
            raise ConfirmationError("receipt is not exact repeated-policy evidence")
        if not self.three_clock_audit.valid:
            raise ConfirmationError("session three-clock audit is invalid")
        start_ns = _datetime_to_ns(started)
        end_ns = _datetime_to_ns(ended)
        first_clock = self.three_clock_audit.first_receive_ts_ns
        last_clock = self.three_clock_audit.last_policy_decision_ts_ns
        if (
            first_clock is None
            or last_clock is None
            or first_clock < start_ns
            or last_clock > end_ns
        ):
            raise ConfirmationError("three-clock evidence falls outside the session")
        if (
            self.common_coverage_audit.control.event_count
            != receipt.control_repeated_policy_evaluations
            or self.common_coverage_audit.candidate.event_count
            != receipt.candidate_repeated_policy_evaluations
            or self.common_coverage_audit.candidate.nonbaseline_action_count
            > receipt.candidate_target_side_evaluations
        ):
            raise ConfirmationError("coverage census and paired receipt disagree")
        body, supplied = _hash_body(self, "session_evidence_sha256")
        if _canonical_sha256(body) != supplied:
            raise ConfirmationError("confirmation session hash drifted")

    @property
    def active_treatment_day(self) -> bool:
        return self.common_coverage_audit.candidate.nonbaseline_action_count > 0

    @classmethod
    def build(
        cls,
        *,
        contract: FinalArtifactConfirmationContract,
        session_id: str,
        utc_day: str,
        session_started_at_utc: str,
        session_ended_at_utc: str,
        final_artifact_identity: str,
        final_artifact_manifest_sha256: str,
        compiler_sha256: str,
        source_sha256: str,
        runtime_sha256: str,
        policy_sha256: str,
        learning_algorithm_identity: str,
        learning_algorithm_oof_artifact_sha256: str,
        paired_receipt: Mapping[str, Any] | repeated.PairedSequentialReplayReceipt,
        three_clock_audit: ThreeClockAudit,
        common_coverage_audit: CommonCoverageAudit,
        source_panel_role: str = SOURCE_PANEL_ROLE,
        historical_development_read: bool = False,
        validation_read: bool = False,
        sealed_holdout_read: bool = False,
    ) -> ProspectiveConfirmationSession:
        receipt_payload = (
            paired_receipt.to_dict()
            if isinstance(paired_receipt, repeated.PairedSequentialReplayReceipt)
            else dict(paired_receipt)
        )
        body = {
            "schema_version": SESSION_SCHEMA_VERSION,
            "identity": IDENTITY,
            "session_id": session_id,
            "utc_day": utc_day,
            "session_started_at_utc": session_started_at_utc,
            "session_ended_at_utc": session_ended_at_utc,
            "source_panel_role": source_panel_role,
            "final_artifact_identity": final_artifact_identity,
            "final_artifact_manifest_sha256": final_artifact_manifest_sha256,
            "compiler_sha256": compiler_sha256,
            "source_sha256": source_sha256,
            "runtime_sha256": runtime_sha256,
            "policy_sha256": policy_sha256,
            "learning_algorithm_identity": learning_algorithm_identity,
            "learning_algorithm_oof_artifact_sha256": (
                learning_algorithm_oof_artifact_sha256
            ),
            "evidence_scope": EXACT_ARTIFACT_EVIDENCE_SCOPE,
            "learning_algorithm_oof_evidence_accepted": False,
            "exact_final_artifact_oof_available": False,
            "paired_receipt": receipt_payload,
            "paired_receipt_sha256": str(receipt_payload.get("receipt_sha256", "")),
            "three_clock_audit": three_clock_audit,
            "common_coverage_audit": common_coverage_audit,
            "historical_development_read": historical_development_read,
            "validation_read": validation_read,
            "sealed_holdout_read": sealed_holdout_read,
            "formal_session_valid": True,
            "research_supported": False,
            "action_authorized": False,
            "live_policy_authorized": False,
        }
        hash_body = {
            **body,
            "three_clock_audit": three_clock_audit.to_dict(),
            "common_coverage_audit": common_coverage_audit.to_dict(),
        }
        session = cls(
            **body,
            session_evidence_sha256=_canonical_sha256(hash_body),
        )
        contract.validate_session(session)
        return session

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> ProspectiveConfirmationSession:
        values = dict(payload)
        values["three_clock_audit"] = ThreeClockAudit.from_dict(
            values["three_clock_audit"]
        )
        values["common_coverage_audit"] = CommonCoverageAudit.from_dict(
            values["common_coverage_audit"]
        )
        return cls(**values)


def _validate_session_against_contract(
    contract: FinalArtifactConfirmationContract,
    session: ProspectiveConfirmationSession,
) -> None:
    locked = _parse_utc(contract.final_artifact_locked_at_utc, "artifact lock time")
    started = _parse_utc(session.session_started_at_utc, "session start")
    if started <= locked:
        raise ConfirmationError("session is not strictly later than artifact lock")
    expected = {
        "final_artifact_identity": contract.final_artifact_identity,
        "final_artifact_manifest_sha256": contract.final_artifact_manifest_sha256,
        "compiler_sha256": contract.compiler_sha256,
        "source_sha256": contract.source_sha256,
        "runtime_sha256": contract.runtime_sha256,
        "policy_sha256": contract.final_policy_sha256,
        "learning_algorithm_identity": contract.learning_algorithm_identity,
        "learning_algorithm_oof_artifact_sha256": (
            contract.learning_algorithm_oof_artifact_sha256
        ),
    }
    for name, expected_value in expected.items():
        if getattr(session, name) != expected_value:
            raise ConfirmationError(f"session {name} drifted from the frozen contract")
    receipt = repeated.PairedSequentialReplayReceipt.from_dict(session.paired_receipt)
    if (
        receipt.candidate_target_side != contract.candidate_target_side
        or receipt.candidate_policy_identity != contract.final_artifact_identity
        or receipt.candidate_policy_sha256 != contract.final_policy_sha256
        or receipt.final_artifact_identity != contract.final_artifact_identity
        or receipt.final_artifact_sha256 != contract.final_policy_sha256
        or receipt.learning_algorithm_identity != contract.learning_algorithm_identity
        or receipt.learning_algorithm_artifact_sha256
        != contract.learning_algorithm_oof_artifact_sha256
    ):
        raise ConfirmationError("paired receipt artifact identity drifted")
    coverage = session.common_coverage_audit
    for arm in (coverage.control, coverage.candidate):
        if (
            arm.coverage_contract_sha256 != contract.coverage_contract_sha256
            or arm.fallback_contract_sha256 != contract.fallback_contract_sha256
        ):
            raise ConfirmationError("coverage/fallback contract hash drifted")


@dataclass(frozen=True, slots=True)
class ConfirmationLedger:
    schema_version: str
    identity: str
    contract: FinalArtifactConfirmationContract
    sessions: tuple[ProspectiveConfirmationSession, ...]
    session_count: int
    active_utc_days: tuple[str, ...]
    active_utc_day_count: int
    minimum_active_utc_days: int
    status: str
    ready_for_formal_evaluation: bool
    evidence_scope: str
    learning_algorithm_oof_evidence_counted: bool
    exact_final_artifact_oof_available: bool
    research_supported: bool
    action_authorized: bool
    live_policy_authorized: bool
    session_chain_sha256: str
    ledger_sha256: str

    def __post_init__(self) -> None:
        if self.schema_version != LEDGER_SCHEMA_VERSION or self.identity != IDENTITY:
            raise ConfirmationError("confirmation ledger identity drifted")
        if self.session_count != len(self.sessions):
            raise ConfirmationError("confirmation ledger session count drifted")
        session_ids: set[str] = set()
        receipt_hashes: set[str] = set()
        previous_end: datetime | None = None
        for session in self.sessions:
            self.contract.validate_session(session)
            if session.session_id in session_ids:
                raise ConfirmationError("confirmation ledger repeats a session id")
            if session.paired_receipt_sha256 in receipt_hashes:
                raise ConfirmationError("confirmation ledger repeats a paired receipt")
            started = _parse_utc(session.session_started_at_utc, "session start")
            if previous_end is not None and started <= previous_end:
                raise ConfirmationError("confirmation sessions are not chronological")
            previous_end = _parse_utc(session.session_ended_at_utc, "session end")
            session_ids.add(session.session_id)
            receipt_hashes.add(session.paired_receipt_sha256)
        expected_active_days = tuple(
            sorted({session.utc_day for session in self.sessions if session.active_treatment_day})
        )
        if (
            self.active_utc_days != expected_active_days
            or self.active_utc_day_count != len(expected_active_days)
            or self.minimum_active_utc_days != self.contract.minimum_active_utc_days
        ):
            raise ConfirmationError("confirmation active-day census drifted")
        expected_ready = len(expected_active_days) >= self.minimum_active_utc_days
        expected_status = (
            ConfirmationStatus.MINIMUM_MET_FORMAL_EVALUATION_REQUIRED
            if expected_ready
            else ConfirmationStatus.PENDING_MINIMUM_ACTIVE_DAYS
        )
        if (
            self.ready_for_formal_evaluation is not expected_ready
            or self.status != expected_status
        ):
            raise ConfirmationError("confirmation status drifted")
        if (
            self.evidence_scope != EXACT_ARTIFACT_EVIDENCE_SCOPE
            or self.learning_algorithm_oof_evidence_counted
            or self.exact_final_artifact_oof_available
            or self.research_supported
            or self.action_authorized
            or self.live_policy_authorized
        ):
            raise ConfirmationError("confirmation ledger claims forbidden authority")
        expected_chain = _canonical_sha256(
            [session.session_evidence_sha256 for session in self.sessions]
        )
        if self.session_chain_sha256 != expected_chain:
            raise ConfirmationError("confirmation session chain drifted")
        body, supplied = _hash_body(self, "ledger_sha256")
        if _canonical_sha256(body) != supplied:
            raise ConfirmationError("confirmation ledger hash drifted")

    @classmethod
    def build(
        cls,
        *,
        contract: FinalArtifactConfirmationContract,
        sessions: Sequence[ProspectiveConfirmationSession],
    ) -> ConfirmationLedger:
        frozen_sessions = tuple(sessions)
        active_days = tuple(
            sorted(
                {
                    session.utc_day
                    for session in frozen_sessions
                    if session.active_treatment_day
                }
            )
        )
        ready = len(active_days) >= contract.minimum_active_utc_days
        body = {
            "schema_version": LEDGER_SCHEMA_VERSION,
            "identity": IDENTITY,
            "contract": contract,
            "sessions": frozen_sessions,
            "session_count": len(frozen_sessions),
            "active_utc_days": active_days,
            "active_utc_day_count": len(active_days),
            "minimum_active_utc_days": contract.minimum_active_utc_days,
            "status": str(
                ConfirmationStatus.MINIMUM_MET_FORMAL_EVALUATION_REQUIRED
                if ready
                else ConfirmationStatus.PENDING_MINIMUM_ACTIVE_DAYS
            ),
            "ready_for_formal_evaluation": ready,
            "evidence_scope": EXACT_ARTIFACT_EVIDENCE_SCOPE,
            "learning_algorithm_oof_evidence_counted": False,
            "exact_final_artifact_oof_available": False,
            "research_supported": False,
            "action_authorized": False,
            "live_policy_authorized": False,
            "session_chain_sha256": _canonical_sha256(
                [session.session_evidence_sha256 for session in frozen_sessions]
            ),
        }
        hash_body = {
            **body,
            "contract": contract.to_dict(),
            "sessions": [session.to_dict() for session in frozen_sessions],
        }
        return cls(**body, ledger_sha256=_canonical_sha256(hash_body))

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> ConfirmationLedger:
        values = dict(payload)
        values["contract"] = FinalArtifactConfirmationContract.from_dict(
            values["contract"]
        )
        values["sessions"] = tuple(
            ProspectiveConfirmationSession.from_dict(row)
            for row in values["sessions"]
        )
        values["active_utc_days"] = tuple(values["active_utc_days"])
        return cls(**values)


def _read_ledger(path: Path) -> ConfirmationLedger:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfirmationError("confirmation ledger is unreadable") from exc
    if not isinstance(payload, Mapping):
        raise ConfirmationError("confirmation ledger is not a JSON object")
    return ConfirmationLedger.from_dict(payload)


def _atomic_write_ledger(path: Path, ledger: ConfirmationLedger) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.partial"
    encoded = (
        json.dumps(
            ledger.to_dict(),
            indent=2,
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    try:
        descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        temporary.unlink(missing_ok=True)


@contextmanager
def _ledger_lock(path: Path):
    lock_path = path.parent / f".{path.name}.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as exc:
        raise ConfirmationError("confirmation ledger is already being updated") from exc
    try:
        os.write(descriptor, f"identity={IDENTITY}\n".encode("ascii"))
        os.fsync(descriptor)
        yield
    finally:
        os.close(descriptor)
        lock_path.unlink(missing_ok=True)


def load_confirmation_ledger(path: str | Path) -> ConfirmationLedger:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise ConfirmationError("confirmation ledger does not exist")
    return _read_ledger(resolved)


def append_confirmation_session(
    path: str | Path,
    *,
    contract: FinalArtifactConfirmationContract,
    session: ProspectiveConfirmationSession,
) -> ConfirmationLedger:
    """Atomically append one unique, chronological, post-lock session."""

    resolved = Path(path).expanduser().resolve()
    contract.validate_session(session)
    with _ledger_lock(resolved):
        if resolved.exists():
            current = _read_ledger(resolved)
            if current.contract.contract_sha256 != contract.contract_sha256:
                raise ConfirmationError("confirmation ledger contract drifted")
        else:
            current = ConfirmationLedger.build(contract=contract, sessions=())
        if any(row.session_id == session.session_id for row in current.sessions):
            raise ConfirmationError("duplicate confirmation session is forbidden")
        if any(
            row.paired_receipt_sha256 == session.paired_receipt_sha256
            for row in current.sessions
        ):
            raise ConfirmationError("duplicate paired receipt is forbidden")
        if current.sessions:
            previous_end = _parse_utc(
                current.sessions[-1].session_ended_at_utc,
                "previous session end",
            )
            new_start = _parse_utc(session.session_started_at_utc, "session start")
            if new_start <= previous_end:
                raise ConfirmationError("session is not a chronological append")
        updated = ConfirmationLedger.build(
            contract=contract,
            sessions=(*current.sessions, session),
        )
        _atomic_write_ledger(resolved, updated)
        return updated


__all__ = [
    "ARM_COVERAGE_SCHEMA_VERSION",
    "CLOCK_FIELDS",
    "COMMON_COVERAGE_SCHEMA_VERSION",
    "CONTRACT_SCHEMA_VERSION",
    "ConfirmationError",
    "ConfirmationLedger",
    "ConfirmationStatus",
    "EXACT_ARTIFACT_EVIDENCE_SCOPE",
    "FinalArtifactConfirmationContract",
    "IDENTITY",
    "LEARNING_ALGORITHM_EVIDENCE_SCOPE",
    "LEDGER_SCHEMA_VERSION",
    "MINIMUM_ACTIVE_UTC_DAYS_FLOOR",
    "ProspectiveConfirmationSession",
    "SCHEMA_VERSION",
    "SESSION_SCHEMA_VERSION",
    "SOURCE_PANEL_ROLE",
    "THREE_CLOCK_SCHEMA_VERSION",
    "ThreeClockAudit",
    "ArmCoverageAudit",
    "CommonCoverageAudit",
    "append_confirmation_session",
    "load_confirmation_ledger",
]
