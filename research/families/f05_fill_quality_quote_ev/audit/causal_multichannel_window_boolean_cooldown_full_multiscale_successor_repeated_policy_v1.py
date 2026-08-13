"""Paired repeated-policy replay bridge for the F05 full-multiscale successor.

The bridge makes the current exact owner policy (B0) the control arm, runs a
candidate only on its frozen target side, and delegates the opposite side to
an independent B0 evaluator.  Both arms must execute the ``backtest_tick``
cooldown evaluator ABI at every exposure-increasing fill and must return a
three-clock transport receipt.  One-shot fork summaries are rejected.

This module is a protocol and mechanics layer.  It does not load historical
panels, read outcomes on its own, or grant research, action, or live authority.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import uuid
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_v1 as successor,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_successor_transport_adapter_v1 as transport,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_nested_oof import (
    BooleanCooldownPolicy,
)

IDENTITY = (
    "causal_multichannel_window_boolean_cooldown_"
    "full_multiscale_successor_repeated_policy_v1"
)
SCHEMA_VERSION = f"{IDENTITY}.v1"
BACKTEST_TICK_EVALUATOR_ABI = "backtest_tick.cooldown_duration_policy_evaluator.v1"
CONTROL_ARM = "control"
CANDIDATE_ARM = "candidate"
ARMS = (CONTROL_ARM, CANDIDATE_ARM)
STATE_COMPONENTS = ("orders", "inventory", "campaign", "cooldown", "ema")
DAY_SUCCESS_MARKER = "_SUCCESS"
SEGMENT_SUCCESS_MARKER = DAY_SUCCESS_MARKER

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SEGMENT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class RepeatedPolicyBridgeError(RuntimeError):
    """Raised when sequential replay or admission violates the frozen contract."""


class CandidateTargetSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class ExecutedArtifactScope(StrEnum):
    LEARNING_ALGORITHM_FOLD_POLICY = "learning_algorithm_fold_specific_policy"
    FINAL_FULL_DEVELOPMENT_REFIT = "final_full_development_refit_artifact"


@runtime_checkable
class FormalDayAdmissionProtocol(Protocol):
    """Hash-bound day admission accepted by formal repeated-policy replay."""

    admission_identity: str
    utc_day: str
    eligible: bool
    receipt_sha256: str

    def canonical_receipt_payload(self) -> Mapping[str, Any]:
        """Return the complete payload whose canonical SHA256 is bound above."""


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
        raise RepeatedPolicyBridgeError(f"{label} is not a lowercase SHA256")
    return normalized


def _require_identity(value: Any, label: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise RepeatedPolicyBridgeError(f"{label} is empty")
    return normalized


def _normalize_utc_day(value: str) -> str:
    try:
        from datetime import date

        parsed = date.fromisoformat(str(value))
    except ValueError as exc:
        raise RepeatedPolicyBridgeError("UTC day must use YYYY-MM-DD") from exc
    normalized = parsed.isoformat()
    if normalized != str(value):
        raise RepeatedPolicyBridgeError("UTC day is not canonical")
    return normalized


def _normalize_utc_timestamp(value: str, label: str) -> str:
    raw = str(value)
    if not raw.endswith("Z"):
        raise RepeatedPolicyBridgeError(f"{label} must use an explicit UTC Z suffix")
    try:
        parsed = datetime.fromisoformat(raw[:-1] + "+00:00")
    except ValueError as exc:
        raise RepeatedPolicyBridgeError(f"{label} is not an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise RepeatedPolicyBridgeError(f"{label} is not UTC")
    return raw


def _normalize_segment_id(value: str) -> str:
    normalized = str(value)
    if _SEGMENT_ID_RE.fullmatch(normalized) is None:
        raise RepeatedPolicyBridgeError("segment id is not portable")
    return normalized


def _freeze_json_mapping(value: Mapping[str, Any], label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise RepeatedPolicyBridgeError(f"{label} is not a mapping")
    try:
        frozen = json.loads(
            json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            )
        )
    except (TypeError, ValueError) as exc:
        raise RepeatedPolicyBridgeError(f"{label} is not canonical JSON") from exc
    if not isinstance(frozen, dict):
        raise RepeatedPolicyBridgeError(f"{label} is not a JSON object")
    return frozen


@dataclass(frozen=True, slots=True)
class FormalDayAdmissionBinding:
    """Validated admission passed through the repeated-policy bridge."""

    admission_identity: str
    utc_day: str
    eligible: bool
    receipt_payload: Mapping[str, Any]
    receipt_sha256: str

    def __post_init__(self) -> None:
        identity = _require_identity(self.admission_identity, "day admission identity")
        day = _normalize_utc_day(self.utc_day)
        if not isinstance(self.eligible, bool):
            raise RepeatedPolicyBridgeError("day admission eligible flag is not Boolean")
        frozen = _freeze_json_mapping(
            self.receipt_payload,
            "day admission receipt payload",
        )
        if frozen.get("admission_identity") != identity:
            raise RepeatedPolicyBridgeError("day admission receipt identity drifted")
        if frozen.get("utc_day") != day:
            raise RepeatedPolicyBridgeError("day admission receipt UTC day drifted")
        if frozen.get("eligible") is not self.eligible:
            raise RepeatedPolicyBridgeError("day admission receipt eligibility drifted")
        receipt_sha256 = _require_sha256(
            self.receipt_sha256,
            "day admission receipt",
        )
        if _canonical_sha256(frozen) != receipt_sha256:
            raise RepeatedPolicyBridgeError("day admission receipt hash mismatch")
        object.__setattr__(self, "admission_identity", identity)
        object.__setattr__(self, "utc_day", day)
        object.__setattr__(self, "receipt_payload", frozen)
        object.__setattr__(self, "receipt_sha256", receipt_sha256)

    @classmethod
    def build(
        cls,
        *,
        admission_identity: str,
        utc_day: str,
        eligible: bool,
        receipt_record: Mapping[str, Any],
    ) -> FormalDayAdmissionBinding:
        identity = _require_identity(admission_identity, "day admission identity")
        day = _normalize_utc_day(utc_day)
        if not isinstance(eligible, bool):
            raise RepeatedPolicyBridgeError("day admission eligible flag is not Boolean")
        payload = {
            "admission_identity": identity,
            "utc_day": day,
            "eligible": eligible,
            "record": _freeze_json_mapping(
                receipt_record,
                "day admission receipt record",
            ),
        }
        return cls(
            admission_identity=identity,
            utc_day=day,
            eligible=eligible,
            receipt_payload=payload,
            receipt_sha256=_canonical_sha256(payload),
        )

    def canonical_receipt_payload(self) -> Mapping[str, Any]:
        return dict(self.receipt_payload)


def _normalize_formal_day_admission(
    value: FormalDayAdmissionProtocol | successor.ProspectiveDayAdmission,
    *,
    expected_utc_day: str,
) -> FormalDayAdmissionBinding:
    expected_day = _normalize_utc_day(expected_utc_day)
    if isinstance(value, successor.ProspectiveDayAdmission):
        binding = FormalDayAdmissionBinding.build(
            admission_identity=(
                f"{successor.IDENTITY}.prospective_day_admission.v1"
            ),
            utc_day=value.utc_day,
            eligible=value.eligible,
            receipt_record=asdict(value),
        )
    else:
        if not isinstance(value, FormalDayAdmissionProtocol):
            raise RepeatedPolicyBridgeError(
                "day admission does not implement the formal protocol"
            )
        try:
            payload = value.canonical_receipt_payload()
        except Exception as exc:
            raise RepeatedPolicyBridgeError(
                "day admission receipt payload is unavailable"
            ) from exc
        binding = FormalDayAdmissionBinding(
            admission_identity=value.admission_identity,
            utc_day=value.utc_day,
            eligible=value.eligible,
            receipt_payload=payload,
            receipt_sha256=value.receipt_sha256,
        )
    if binding.utc_day != expected_day:
        raise RepeatedPolicyBridgeError("day admission does not match replay UTC day")
    if not binding.eligible:
        raise RepeatedPolicyBridgeError("day admission is ineligible for formal replay")
    return binding


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.partial"
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(
                payload,
                handle,
                indent=2,
                sort_keys=True,
                ensure_ascii=True,
                allow_nan=False,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.partial"
    try:
        with temporary.open("w", encoding="ascii") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RepeatedPolicyBridgeError(f"invalid {label}") from exc
    if not isinstance(payload, dict):
        raise RepeatedPolicyBridgeError(f"{label} is not a JSON object")
    return payload


@dataclass(frozen=True, slots=True)
class ArtifactIdentityBinding:
    """Keep learning-algorithm evidence separate from a final refit artifact."""

    executed_artifact_scope: ExecutedArtifactScope
    executed_policy_identity: str
    executed_policy_sha256: str
    executed_predicate_bundle_sha256: str
    learning_algorithm_identity: str
    learning_algorithm_artifact_sha256: str
    final_artifact_identity: str | None = None
    final_artifact_sha256: str | None = None
    exact_final_artifact_oof_available: bool = False

    def __post_init__(self) -> None:
        for name in (
            "executed_policy_identity",
            "learning_algorithm_identity",
        ):
            _require_identity(getattr(self, name), name)
        for name in (
            "executed_policy_sha256",
            "executed_predicate_bundle_sha256",
            "learning_algorithm_artifact_sha256",
        ):
            _require_sha256(getattr(self, name), name)
        final_values = (self.final_artifact_identity, self.final_artifact_sha256)
        if (final_values[0] is None) != (final_values[1] is None):
            raise RepeatedPolicyBridgeError(
                "final artifact identity and SHA256 must be both present or both absent"
            )
        if self.final_artifact_identity is not None:
            _require_identity(self.final_artifact_identity, "final_artifact_identity")
            _require_sha256(self.final_artifact_sha256, "final_artifact_sha256")
        if self.executed_artifact_scope == ExecutedArtifactScope.FINAL_FULL_DEVELOPMENT_REFIT:
            if self.final_artifact_identity is None:
                raise RepeatedPolicyBridgeError(
                    "final-refit execution lacks a final artifact identity"
                )
            if (
                self.executed_policy_identity != self.final_artifact_identity
                or self.executed_policy_sha256 != self.final_artifact_sha256
            ):
                raise RepeatedPolicyBridgeError(
                    "executed final-refit policy does not match the final artifact"
                )
        if self.executed_artifact_scope == ExecutedArtifactScope.LEARNING_ALGORITHM_FOLD_POLICY:
            if self.executed_policy_identity != self.learning_algorithm_identity:
                raise RepeatedPolicyBridgeError(
                    "executed fold policy identity does not match the learning artifact"
                )
        if self.exact_final_artifact_oof_available:
            raise RepeatedPolicyBridgeError(
                "a full-data final refit cannot claim exact outer-OOF evidence"
            )


@dataclass(frozen=True, slots=True)
class ArmLocalStateIdentity:
    order_state_id: str
    inventory_state_id: str
    cooldown_state_id: str
    campaign_state_id: str
    ema_state_id: str

    @classmethod
    def build(cls, *, run_identity_sha256: str, arm: str) -> ArmLocalStateIdentity:
        seed = _require_sha256(run_identity_sha256, "run identity")
        if arm not in ARMS:
            raise RepeatedPolicyBridgeError("unknown replay arm")
        def component_sha256(component: str) -> str:
            return _canonical_sha256(
                {
                    "identity": IDENTITY,
                    "run_identity_sha256": seed,
                    "arm": arm,
                    "component": component,
                }
            )

        return cls(
            order_state_id=component_sha256("orders"),
            inventory_state_id=component_sha256("inventory"),
            campaign_state_id=component_sha256("campaign"),
            cooldown_state_id=component_sha256("cooldown"),
            ema_state_id=component_sha256("ema"),
        )

    def values(self) -> tuple[str, ...]:
        return tuple(asdict(self).values())


@dataclass(frozen=True, slots=True)
class ArmStateSnapshot:
    """Canonical replay state that must survive every segment boundary."""

    arm: str
    payload: Mapping[str, Any]
    state_sha256: str

    def __post_init__(self) -> None:
        if self.arm not in ARMS:
            raise RepeatedPolicyBridgeError("state snapshot arm is invalid")
        frozen = _freeze_json_mapping(self.payload, "arm state payload")
        if set(frozen) != set(STATE_COMPONENTS):
            raise RepeatedPolicyBridgeError(
                "arm state must contain orders/inventory/campaign/cooldown/ema"
            )
        if any(not isinstance(frozen[name], dict) for name in STATE_COMPONENTS):
            raise RepeatedPolicyBridgeError("every arm state component must be a mapping")
        object.__setattr__(self, "payload", frozen)
        supplied = _require_sha256(self.state_sha256, "arm state")
        expected = _canonical_sha256(
            {
                "schema_version": f"{SCHEMA_VERSION}.arm_state",
                "arm": self.arm,
                "payload": frozen,
            }
        )
        if supplied != expected:
            raise RepeatedPolicyBridgeError("arm state hash mismatch")

    @classmethod
    def build(cls, *, arm: str, payload: Mapping[str, Any]) -> ArmStateSnapshot:
        frozen = _freeze_json_mapping(payload, "arm state payload")
        body = {
            "schema_version": f"{SCHEMA_VERSION}.arm_state",
            "arm": str(arm),
            "payload": frozen,
        }
        return cls(
            arm=str(arm),
            payload=frozen,
            state_sha256=_canonical_sha256(body),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "arm": self.arm,
            "payload": dict(self.payload),
            "state_sha256": self.state_sha256,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> ArmStateSnapshot:
        return cls(
            arm=str(value.get("arm", "")),
            payload=dict(value.get("payload", {})),
            state_sha256=str(value.get("state_sha256", "")),
        )


@dataclass(frozen=True, slots=True)
class FullyBoundRestartBinding:
    """Proof that a process restart restored both arm states byte-for-byte."""

    restart_manifest_sha256: str
    restored_state_sha256: Mapping[str, str]
    fully_bound: bool = True
    recovery_complete: bool = True

    def __post_init__(self) -> None:
        _require_sha256(self.restart_manifest_sha256, "restart manifest")
        restored = dict(self.restored_state_sha256)
        if set(restored) != set(ARMS):
            raise RepeatedPolicyBridgeError("restart binding lacks one replay arm")
        for arm in ARMS:
            _require_sha256(restored[arm], f"{arm} restored state")
        if not self.fully_bound or not self.recovery_complete:
            raise RepeatedPolicyBridgeError(
                "restart state is not fully bound and recoverable"
            )
        object.__setattr__(self, "restored_state_sha256", restored)


@dataclass(frozen=True, slots=True)
class ReplaySegmentSpec:
    segment_id: str
    utc_day: str
    segment_start_utc: str
    segment_end_utc: str
    common_input_identity: Mapping[str, Any]
    prospective_day_admission: successor.ProspectiveDayAdmission | None = None
    restart_binding: FullyBoundRestartBinding | None = None
    day_admission: FormalDayAdmissionProtocol | None = None

    def __post_init__(self) -> None:
        segment = _normalize_segment_id(self.segment_id)
        day = _normalize_utc_day(self.utc_day)
        start = _normalize_utc_timestamp(self.segment_start_utc, "segment start")
        end = _normalize_utc_timestamp(self.segment_end_utc, "segment end")
        start_dt = datetime.fromisoformat(start[:-1] + "+00:00")
        end_dt = datetime.fromisoformat(end[:-1] + "+00:00")
        if start_dt >= end_dt:
            raise RepeatedPolicyBridgeError("segment interval is empty or reversed")
        if start_dt.date().isoformat() != day:
            raise RepeatedPolicyBridgeError("segment start does not match UTC day")
        frozen = _freeze_json_mapping(
            self.common_input_identity, "segment common replay identity"
        )
        if (self.prospective_day_admission is None) == (self.day_admission is None):
            raise RepeatedPolicyBridgeError(
                "segment requires exactly one formal day admission"
            )
        raw_admission = (
            self.day_admission
            if self.day_admission is not None
            else self.prospective_day_admission
        )
        if raw_admission is None:
            raise RepeatedPolicyBridgeError("segment lacks a formal day admission")
        admission = _normalize_formal_day_admission(
            raw_admission,
            expected_utc_day=day,
        )
        object.__setattr__(self, "segment_id", segment)
        object.__setattr__(self, "utc_day", day)
        object.__setattr__(self, "segment_start_utc", start)
        object.__setattr__(self, "segment_end_utc", end)
        object.__setattr__(self, "common_input_identity", frozen)
        object.__setattr__(self, "day_admission", admission)


@dataclass(frozen=True, slots=True)
class ArmReplayRequest:
    chain_identity_sha256: str
    segment_index: int
    segment_id: str
    utc_day: str
    segment_start_utc: str
    segment_end_utc: str
    arm: str
    evaluator: Any
    snapshot_emitter: Any
    common_input_identity: Mapping[str, Any]
    common_input_identity_sha256: str
    common_market_source_sha256: str
    common_receive_clock_source_sha256: str
    common_feature_ready_clock_source_sha256: str
    common_random_source_sha256: str
    target_side: CandidateTargetSide
    state_identity: ArmLocalStateIdentity
    input_state: ArmStateSnapshot
    restart_binding: FullyBoundRestartBinding | None

    def backtest_tick_params_overlay(self) -> dict[str, Any]:
        """Return the exact existing evaluator ABI fields for ``backtest_tick``."""

        return {
            "cooldown_v2_snapshot_emitter": self.snapshot_emitter,
            "cooldown_duration_policy_evaluator": self.evaluator,
        }


class SequentialArmExecutor(Protocol):
    def __call__(self, request: ArmReplayRequest) -> Mapping[str, Any]: ...


class TargetSideDelegatingEvaluator:
    """Candidate evaluator: target side uses candidate, other side uses B0."""

    def __init__(
        self,
        *,
        target_side: CandidateTargetSide,
        target_evaluator: successor.ResearchBooleanCooldownPolicyEvaluator,
        b0_evaluator: successor.ResearchBooleanCooldownPolicyEvaluator,
        artifact_binding: ArtifactIdentityBinding,
    ) -> None:
        if target_evaluator is b0_evaluator:
            raise RepeatedPolicyBridgeError("candidate and B0 delegates share an instance")
        self.target_side = CandidateTargetSide(target_side)
        self._target = target_evaluator
        self._b0 = b0_evaluator
        self.policy_identity = artifact_binding.executed_policy_identity
        self.policy_sha256 = artifact_binding.executed_policy_sha256
        self.predicate_bundle_sha256 = (
            artifact_binding.executed_predicate_bundle_sha256
        )
        self._evaluations = 0
        self._target_evaluations = 0
        self._b0_evaluations = 0

    @property
    def binding_valid(self) -> bool:
        return bool(self._target.binding_valid and self._b0.binding_valid)

    @property
    def binding_error(self) -> str | None:
        if self.binding_valid:
            return None
        return "candidate_or_b0_delegate_binding_invalid"

    def _rebind(self, decision: Any) -> Any:
        return replace(
            decision,
            policy_sha256=self.policy_sha256,
            predicate_bundle_sha256=self.predicate_bundle_sha256,
        )

    def _delegate(self, side: str) -> successor.ResearchBooleanCooldownPolicyEvaluator:
        normalized = str(side).upper()
        if normalized not in {"BUY", "SELL"}:
            raise RepeatedPolicyBridgeError("candidate evaluator side is invalid")
        self._evaluations += 1
        if normalized == self.target_side:
            self._target_evaluations += 1
            return self._target
        self._b0_evaluations += 1
        return self._b0

    def evaluate(self, snapshot: Any, baseline_duration_ms: Any) -> Any:
        try:
            side = str(snapshot.feature_row.to_dict().get("side", "")).upper()
        except Exception as exc:
            raise RepeatedPolicyBridgeError(
                "candidate snapshot does not expose a side"
            ) from exc
        delegate = self._delegate(side)
        return self._rebind(delegate.evaluate(snapshot, baseline_duration_ms))

    def evaluate_predicates(
        self,
        *,
        side: str,
        predicate_values: Mapping[str, Any],
        baseline_duration_ms: Any,
        snapshot_id: str,
    ) -> Any:
        delegate = self._delegate(side)
        return self._rebind(
            delegate.evaluate_predicates(
                side=side,
                predicate_values=predicate_values,
                baseline_duration_ms=baseline_duration_ms,
                snapshot_id=snapshot_id,
            )
        )

    def audit(self) -> dict[str, Any]:
        return {
            "identity": self.policy_identity,
            "policy_sha256": self.policy_sha256,
            "predicate_bundle_sha256": self.predicate_bundle_sha256,
            "evaluations": self._evaluations,
            "target_side": str(self.target_side),
            "target_side_evaluations": self._target_evaluations,
            "b0_delegated_evaluations": self._b0_evaluations,
            "target_delegate_audit": self._target.audit(),
            "b0_delegate_audit": self._b0.audit(),
            "opposite_side_delegates_exact_b0": True,
            "research_only": True,
            "action_authorized": False,
            "live_authorized": False,
        }


def build_exact_current_owner_evaluator(
    *,
    expected_identity_hashes: Mapping[str, str] | None = None,
) -> successor.ResearchBooleanCooldownPolicyEvaluator:
    """Build a fresh B0 evaluator with the exact active owner artifact identity."""

    return successor.ResearchBooleanCooldownPolicyEvaluator(
        policies={"BUY": None, "SELL": successor.current_exact_owner_policy()},
        policy_identity=successor.ACTIVE_OWNER_POLICY_IDENTITY,
        policy_sha256=successor.ACTIVE_OWNER_POLICY_SHA256,
        predicate_bundle_sha256=successor.ACTIVE_PREDICATE_BUNDLE_SHA256,
        expected_identity_hashes=expected_identity_hashes,
    )


def build_target_side_candidate_evaluator(
    *,
    target_side: CandidateTargetSide,
    target_policy: BooleanCooldownPolicy,
    artifact_binding: ArtifactIdentityBinding,
    expected_identity_hashes: Mapping[str, str] | None = None,
) -> TargetSideDelegatingEvaluator:
    normalized_side = CandidateTargetSide(target_side)
    if target_policy.side != normalized_side:
        raise RepeatedPolicyBridgeError("candidate policy side does not match target side")
    target = successor.ResearchBooleanCooldownPolicyEvaluator(
        policies={
            "BUY": target_policy if normalized_side == CandidateTargetSide.BUY else None,
            "SELL": target_policy if normalized_side == CandidateTargetSide.SELL else None,
        },
        policy_identity=artifact_binding.executed_policy_identity,
        policy_sha256=artifact_binding.executed_policy_sha256,
        predicate_bundle_sha256=artifact_binding.executed_predicate_bundle_sha256,
        expected_identity_hashes=expected_identity_hashes,
    )
    return TargetSideDelegatingEvaluator(
        target_side=normalized_side,
        target_evaluator=target,
        b0_evaluator=build_exact_current_owner_evaluator(
            expected_identity_hashes=expected_identity_hashes
        ),
        artifact_binding=artifact_binding,
    )


@dataclass(frozen=True, slots=True)
class ArmExecutionEvidence:
    chain_identity_sha256: str
    segment_index: int
    segment_id: str
    arm: str
    policy_sha256: str
    common_input_identity_sha256: str
    common_market_source_sha256: str
    common_receive_clock_source_sha256: str
    common_feature_ready_clock_source_sha256: str
    common_random_source_sha256: str
    state_identity: ArmLocalStateIdentity
    input_state: ArmStateSnapshot
    output_state: ArmStateSnapshot
    input_state_sha256: str
    output_state_sha256: str
    fully_bound_restart_restored: bool
    repeated_policy_evaluation_count: int
    exposure_increasing_fill_count: int
    target_side_evaluation_count: int | None
    b0_delegated_evaluation_count: int | None
    campaign_terminal_value_usdc: float | None
    formal_support_valid: bool
    formal_exclusion_reasons: tuple[str, ...]
    transport_receipt: Mapping[str, Any]
    transport_receipt_sha256: str
    decision_trace_sha256: str
    snapshot_trace_sha256: str
    one_shot_effect_aggregation_used: bool = False
    execution_copied_from_other_arm: bool = False
    checkpoint_reused: bool = False

    def simulator_result(self) -> dict[str, Any]:
        return {
            "arm": self.arm,
            "policy_sha256": self.policy_sha256,
            "common_input_identity_sha256": self.common_input_identity_sha256,
            "input_state_sha256": self.input_state_sha256,
            "output_state_sha256": self.output_state_sha256,
            "execution_copied_from_other_arm": self.execution_copied_from_other_arm,
            "one_shot_effect_aggregation_used": self.one_shot_effect_aggregation_used,
            "formal_support_valid": self.formal_support_valid,
            "formal_exclusion_reasons": list(self.formal_exclusion_reasons),
            "repeated_policy_evaluation_count": (
                self.repeated_policy_evaluation_count
            ),
            "campaign_terminal_value_usdc": self.campaign_terminal_value_usdc,
            "transport_receipt": dict(self.transport_receipt),
        }

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> ArmExecutionEvidence:
        values = dict(payload)
        values["state_identity"] = ArmLocalStateIdentity(
            **dict(values["state_identity"])
        )
        values["input_state"] = ArmStateSnapshot.from_dict(values["input_state"])
        values["output_state"] = ArmStateSnapshot.from_dict(values["output_state"])
        values["formal_exclusion_reasons"] = tuple(
            values["formal_exclusion_reasons"]
        )
        return cls(**values)


def _sequence_rows(value: Any, label: str) -> list[Mapping[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise RepeatedPolicyBridgeError(f"{label} is not a sequence")
    rows = list(value)
    if any(not isinstance(row, Mapping) for row in rows):
        raise RepeatedPolicyBridgeError(f"{label} contains a non-mapping row")
    return rows


def _reject_one_shot_paths(backtest_result: Mapping[str, Any]) -> None:
    if backtest_result.get("ema_add_wait_fork_enabled") is True:
        raise RepeatedPolicyBridgeError("ADD/WAIT one-shot fork is enabled")
    for key in ("_ema_add_wait_fork_trace", "_cooldown_duration_fork_trace"):
        trace = backtest_result.get(key)
        if isinstance(trace, Mapping) and trace:
            raise RepeatedPolicyBridgeError(f"one-shot fork trace is populated: {key}")
        if trace not in (None, {}) and not isinstance(trace, Mapping):
            raise RepeatedPolicyBridgeError(f"one-shot fork trace is malformed: {key}")


def _validate_state_echo(
    raw: Mapping[str, Any],
    *,
    expected: ArmLocalStateIdentity,
    input_state: ArmStateSnapshot,
    restart_binding: FullyBoundRestartBinding | None,
) -> None:
    observed = raw.get("arm_local_state_identity")
    if not isinstance(observed, Mapping) or dict(observed) != asdict(expected):
        raise RepeatedPolicyBridgeError("arm-local state identity was not preserved")
    if raw.get("arm_local_state_fresh") is not False:
        raise RepeatedPolicyBridgeError("fresh-start is forbidden in a state chain")
    if raw.get("state_copied_from_other_arm") is not False:
        raise RepeatedPolicyBridgeError("arm state was copied from the paired arm")
    if raw.get("input_state_restored") is not True:
        raise RepeatedPolicyBridgeError("arm input state was not restored")
    if str(raw.get("input_state_sha256")) != input_state.state_sha256:
        raise RepeatedPolicyBridgeError("arm input state hash drifted")
    restart_receipt = raw.get("restart_recovery")
    if restart_binding is None:
        if restart_receipt not in (None, {}):
            if not isinstance(restart_receipt, Mapping) or (
                restart_receipt.get("restart_detected") is not False
            ):
                raise RepeatedPolicyBridgeError("unexpected restart recovery receipt")
        return
    if not isinstance(restart_receipt, Mapping):
        raise RepeatedPolicyBridgeError("fully-bound restart recovery receipt is missing")
    if (
        restart_receipt.get("restart_detected") is not True
        or restart_receipt.get("fully_bound") is not True
        or restart_receipt.get("recovery_complete") is not True
        or str(restart_receipt.get("restart_manifest_sha256"))
        != restart_binding.restart_manifest_sha256
        or str(restart_receipt.get("restored_input_state_sha256"))
        != input_state.state_sha256
    ):
        raise RepeatedPolicyBridgeError("fully-bound restart did not restore exact state")


def adapt_backtest_tick_arm_result(
    request: ArmReplayRequest,
    raw: Mapping[str, Any],
) -> ArmExecutionEvidence:
    """Validate and project one real ``backtest_tick`` sequential arm result."""

    if not isinstance(raw, Mapping):
        raise RepeatedPolicyBridgeError("arm executor result is not a mapping")
    if raw.get("engine_evaluator_abi") != BACKTEST_TICK_EVALUATOR_ABI:
        raise RepeatedPolicyBridgeError("backtest evaluator ABI drifted")
    if raw.get("repeated_sequential_policy_executed") is not True:
        raise RepeatedPolicyBridgeError("arm did not execute a repeated policy")
    if raw.get("one_shot_effect_aggregation_used") is not False:
        raise RepeatedPolicyBridgeError("one-shot aggregation cannot enter this bridge")
    if raw.get("execution_copied_from_other_arm") is not False:
        raise RepeatedPolicyBridgeError("paired arm execution was copied")
    if str(raw.get("chain_identity_sha256")) != request.chain_identity_sha256:
        raise RepeatedPolicyBridgeError("arm chain identity drifted")
    if raw.get("segment_index") != request.segment_index:
        raise RepeatedPolicyBridgeError("arm segment index drifted")
    if str(raw.get("segment_id")) != request.segment_id:
        raise RepeatedPolicyBridgeError("arm segment identity drifted")
    if str(raw.get("arm")) != request.arm:
        raise RepeatedPolicyBridgeError("arm identity drifted")
    if str(raw.get("policy_sha256")) != str(request.evaluator.policy_sha256):
        raise RepeatedPolicyBridgeError("arm policy SHA256 drifted")
    if (
        str(raw.get("common_input_identity_sha256"))
        != request.common_input_identity_sha256
    ):
        raise RepeatedPolicyBridgeError("arm common input identity drifted")
    if str(raw.get("common_market_source_sha256")) != (
        request.common_market_source_sha256
    ):
        raise RepeatedPolicyBridgeError("arm common market source drifted")
    if str(raw.get("common_receive_clock_source_sha256")) != (
        request.common_receive_clock_source_sha256
    ):
        raise RepeatedPolicyBridgeError("arm common receive clock drifted")
    if str(raw.get("common_feature_ready_clock_source_sha256")) != (
        request.common_feature_ready_clock_source_sha256
    ):
        raise RepeatedPolicyBridgeError("arm common feature-ready clock drifted")
    if str(raw.get("common_random_source_sha256")) != (
        request.common_random_source_sha256
    ):
        raise RepeatedPolicyBridgeError("arm common random source drifted")
    _validate_state_echo(
        raw,
        expected=request.state_identity,
        input_state=request.input_state,
        restart_binding=request.restart_binding,
    )
    if raw.get("state_transition_complete") is not True:
        raise RepeatedPolicyBridgeError("arm state transition is incomplete")
    output_payload = raw.get("output_state")
    if not isinstance(output_payload, Mapping):
        raise RepeatedPolicyBridgeError("arm output state is missing")
    output_state = ArmStateSnapshot.build(arm=request.arm, payload=output_payload)
    if str(raw.get("output_state_sha256")) != output_state.state_sha256:
        raise RepeatedPolicyBridgeError("arm output state hash drifted")
    backtest_result = raw.get("backtest_result")
    if not isinstance(backtest_result, Mapping):
        raise RepeatedPolicyBridgeError("backtest result is missing")
    _reject_one_shot_paths(backtest_result)

    decisions = _sequence_rows(
        backtest_result.get("_cooldown_duration_policy_decisions", ()),
        "cooldown decisions",
    )
    snapshots = _sequence_rows(
        backtest_result.get("_cooldown_v2_snapshot_receipts", ()),
        "cooldown snapshots",
    )
    raw_exposure_count = backtest_result.get("campaign_exposure_increasing_fills")
    if isinstance(raw_exposure_count, bool) or not isinstance(raw_exposure_count, int):
        raise RepeatedPolicyBridgeError("exposure-fill count is invalid")
    exposure_count = int(raw_exposure_count)
    if exposure_count < 0:
        raise RepeatedPolicyBridgeError("exposure-fill count is negative")
    policy_audit = backtest_result.get("_cooldown_duration_policy_audit")
    if not isinstance(policy_audit, Mapping):
        raise RepeatedPolicyBridgeError("backtest result lacks policy audit")
    evaluation_count = policy_audit.get("evaluations")
    if isinstance(evaluation_count, bool) or not isinstance(evaluation_count, int):
        raise RepeatedPolicyBridgeError("policy evaluation count is invalid")
    evaluation_count = int(evaluation_count)
    if not (
        evaluation_count == exposure_count == len(decisions) == len(snapshots)
    ):
        raise RepeatedPolicyBridgeError(
            "every exposure fill must have one snapshot and one policy evaluation"
        )
    if evaluation_count <= 0:
        raise RepeatedPolicyBridgeError("arm has no repeated policy evaluations")
    decision_snapshot_ids = [str(row.get("snapshot_id", "")) for row in decisions]
    snapshot_ids = [str(row.get("snapshot_id", "")) for row in snapshots]
    if (
        any(not value for value in decision_snapshot_ids)
        or decision_snapshot_ids != snapshot_ids
        or len(set(snapshot_ids)) != len(snapshot_ids)
    ):
        raise RepeatedPolicyBridgeError("decision/snapshot identity sequence drifted")
    if any(
        str(row.get("policy_sha256")) != str(request.evaluator.policy_sha256)
        for row in decisions
    ):
        raise RepeatedPolicyBridgeError("decision trace mixes policy identities")

    target_count: int | None = None
    delegated_count: int | None = None
    reasons = [str(value) for value in raw.get("formal_exclusion_reasons", ())]
    support_valid = raw.get("formal_support_valid")
    if type(support_valid) is not bool:
        raise RepeatedPolicyBridgeError("formal support flag is invalid")
    if request.arm == CANDIDATE_ARM:
        if str(policy_audit.get("target_side")) != str(request.target_side):
            raise RepeatedPolicyBridgeError("candidate target side audit drifted")
        target_count = int(policy_audit.get("target_side_evaluations", -1))
        delegated_count = int(policy_audit.get("b0_delegated_evaluations", -1))
        if target_count < 0 or delegated_count < 0:
            raise RepeatedPolicyBridgeError("candidate delegation counts are invalid")
        if target_count + delegated_count != evaluation_count:
            raise RepeatedPolicyBridgeError("candidate delegation does not cover all fills")
        if policy_audit.get("opposite_side_delegates_exact_b0") is not True:
            raise RepeatedPolicyBridgeError("candidate opposite side is not exact B0")
        if target_count == 0:
            support_valid = False
            reasons.append("candidate_target_side_not_evaluated")

    receipt_payload = raw.get("transport_receipt")
    if not isinstance(receipt_payload, Mapping):
        raise RepeatedPolicyBridgeError("arm lacks a three-clock transport receipt")
    try:
        receipt = transport.validate_transport_receipt(
            receipt_payload,
            expected_arm=request.arm,
            expected_common_market_source_sha256=(
                request.common_market_source_sha256
            ),
        )
    except transport.TransportContractError as exc:
        raise RepeatedPolicyBridgeError("three-clock transport receipt is invalid") from exc
    if receipt.live_equivalent:
        raise RepeatedPolicyBridgeError("historical transport cannot be live-equivalent")
    if not receipt.formal_replay_support_valid:
        support_valid = False
        reasons.extend(f"transport:{reason}" for reason in receipt.exclusion_reasons)

    terminal_value: float | None = None
    if support_valid:
        try:
            terminal_value = float(raw["campaign_terminal_value_usdc"])
        except (KeyError, TypeError, ValueError) as exc:
            raise RepeatedPolicyBridgeError(
                "supported arm lacks campaign terminal value"
            ) from exc
        if not math.isfinite(terminal_value):
            raise RepeatedPolicyBridgeError("campaign terminal value is non-finite")
        if reasons:
            raise RepeatedPolicyBridgeError(
                "supported arm carries formal exclusion reasons"
            )
    elif not reasons:
        reasons.append("formal_support_invalid")

    return ArmExecutionEvidence(
        chain_identity_sha256=request.chain_identity_sha256,
        segment_index=request.segment_index,
        segment_id=request.segment_id,
        arm=request.arm,
        policy_sha256=str(request.evaluator.policy_sha256),
        common_input_identity_sha256=request.common_input_identity_sha256,
        common_market_source_sha256=request.common_market_source_sha256,
        common_receive_clock_source_sha256=(
            request.common_receive_clock_source_sha256
        ),
        common_feature_ready_clock_source_sha256=(
            request.common_feature_ready_clock_source_sha256
        ),
        common_random_source_sha256=request.common_random_source_sha256,
        state_identity=request.state_identity,
        input_state=request.input_state,
        output_state=output_state,
        input_state_sha256=request.input_state.state_sha256,
        output_state_sha256=output_state.state_sha256,
        fully_bound_restart_restored=request.restart_binding is not None,
        repeated_policy_evaluation_count=evaluation_count,
        exposure_increasing_fill_count=exposure_count,
        target_side_evaluation_count=target_count,
        b0_delegated_evaluation_count=delegated_count,
        campaign_terminal_value_usdc=terminal_value,
        formal_support_valid=bool(support_valid),
        formal_exclusion_reasons=tuple(dict.fromkeys(reasons)),
        transport_receipt=dict(receipt_payload),
        transport_receipt_sha256=receipt.transport_receipt_sha256,
        decision_trace_sha256=_canonical_sha256(decisions),
        snapshot_trace_sha256=_canonical_sha256(snapshots),
    )


@dataclass(frozen=True, slots=True)
class PairedSequentialReplayReceipt:
    schema_version: str
    identity: str
    chain_identity_sha256: str
    segment_index: int
    segment_id: str
    utc_day: str
    segment_start_utc: str
    segment_end_utc: str
    previous_segment_receipt_sha256: str | None
    day_admission_identity: str
    day_admission_receipt_sha256: str
    candidate_target_side: str
    control_policy_identity: str
    control_policy_sha256: str
    candidate_policy_identity: str
    candidate_policy_sha256: str
    executed_artifact_scope: str
    learning_algorithm_identity: str
    learning_algorithm_artifact_sha256: str
    final_artifact_identity: str | None
    final_artifact_sha256: str | None
    exact_final_artifact_oof_available: bool
    common_input_identity_sha256: str
    common_market_source_sha256: str
    common_receive_clock_source_sha256: str
    common_feature_ready_clock_source_sha256: str
    common_random_source_sha256: str
    paired_exogenous_clock_identity_sha256: str
    control_state_identity_sha256: str
    candidate_state_identity_sha256: str
    control_input_state_sha256: str
    candidate_input_state_sha256: str
    control_output_state_sha256: str
    candidate_output_state_sha256: str
    restart_manifest_sha256: str | None
    fully_bound_restart_restored: bool
    control_repeated_policy_evaluations: int
    candidate_repeated_policy_evaluations: int
    candidate_target_side_evaluations: int
    candidate_b0_delegated_evaluations: int
    control_campaign_terminal_value_usdc: float | None
    candidate_campaign_terminal_value_usdc: float | None
    terminal_value_delta_usdc: float | None
    formal_denominator_eligible: bool
    exclusion_reasons: tuple[str, ...]
    control_transport_receipt_sha256: str
    candidate_transport_receipt_sha256: str
    control_checkpoint_reused: bool
    candidate_checkpoint_reused: bool
    paired_audit_sha256: str
    repeated_sequential_policy: bool
    one_shot_effect_aggregation_used: bool
    same_market_source: bool
    same_receive_and_feature_ready_clocks: bool
    common_random_source: bool
    arm_local_state: bool
    state_chain_contiguous: bool
    fresh_start_used: bool
    live_equivalent: bool
    research_supported: bool
    action_authorized: bool
    live_policy_authorized: bool
    receipt_sha256: str

    def __post_init__(self) -> None:
        if self.schema_version != SCHEMA_VERSION or self.identity != IDENTITY:
            raise RepeatedPolicyBridgeError("paired receipt identity drifted")
        _require_sha256(self.chain_identity_sha256, "chain identity")
        if isinstance(self.segment_index, bool) or self.segment_index < 0:
            raise RepeatedPolicyBridgeError("paired receipt segment index is invalid")
        _normalize_segment_id(self.segment_id)
        _normalize_utc_day(self.utc_day)
        start = _normalize_utc_timestamp(self.segment_start_utc, "segment start")
        end = _normalize_utc_timestamp(self.segment_end_utc, "segment end")
        if datetime.fromisoformat(start[:-1] + "+00:00") >= datetime.fromisoformat(
            end[:-1] + "+00:00"
        ):
            raise RepeatedPolicyBridgeError("paired receipt segment order drifted")
        if self.previous_segment_receipt_sha256 is not None:
            _require_sha256(
                self.previous_segment_receipt_sha256,
                "previous segment receipt",
            )
        _require_identity(self.day_admission_identity, "day admission identity")
        CandidateTargetSide(self.candidate_target_side)
        for name in (
            "control_policy_sha256",
            "candidate_policy_sha256",
            "learning_algorithm_artifact_sha256",
            "day_admission_receipt_sha256",
            "common_input_identity_sha256",
            "common_market_source_sha256",
            "common_receive_clock_source_sha256",
            "common_feature_ready_clock_source_sha256",
            "common_random_source_sha256",
            "paired_exogenous_clock_identity_sha256",
            "control_state_identity_sha256",
            "candidate_state_identity_sha256",
            "control_input_state_sha256",
            "candidate_input_state_sha256",
            "control_output_state_sha256",
            "candidate_output_state_sha256",
            "control_transport_receipt_sha256",
            "candidate_transport_receipt_sha256",
            "paired_audit_sha256",
            "receipt_sha256",
        ):
            _require_sha256(getattr(self, name), name)
        if self.final_artifact_sha256 is not None:
            _require_sha256(self.final_artifact_sha256, "final_artifact_sha256")
        if self.restart_manifest_sha256 is not None:
            _require_sha256(self.restart_manifest_sha256, "restart_manifest_sha256")
        if (self.restart_manifest_sha256 is None) == self.fully_bound_restart_restored:
            raise RepeatedPolicyBridgeError("restart restoration identity is inconsistent")
        expected_exogenous_identity = _canonical_sha256(
            {
                "common_market_source_sha256": self.common_market_source_sha256,
                "common_receive_clock_source_sha256": (
                    self.common_receive_clock_source_sha256
                ),
                "common_feature_ready_clock_source_sha256": (
                    self.common_feature_ready_clock_source_sha256
                ),
                "common_random_source_sha256": self.common_random_source_sha256,
            }
        )
        if self.paired_exogenous_clock_identity_sha256 != expected_exogenous_identity:
            raise RepeatedPolicyBridgeError("paired exogenous clock identity drifted")
        if self.control_policy_sha256 != successor.ACTIVE_OWNER_POLICY_SHA256:
            raise RepeatedPolicyBridgeError("receipt control is not exact B0")
        if self.control_state_identity_sha256 == self.candidate_state_identity_sha256:
            raise RepeatedPolicyBridgeError("paired arms share a state identity")
        if not all(
            (
                self.repeated_sequential_policy,
                self.same_market_source,
                self.same_receive_and_feature_ready_clocks,
                self.common_random_source,
                self.arm_local_state,
                self.state_chain_contiguous,
            )
        ):
            raise RepeatedPolicyBridgeError("paired sequential mechanics are incomplete")
        if any(
            (
                self.one_shot_effect_aggregation_used,
                self.fresh_start_used,
                self.live_equivalent,
                self.research_supported,
                self.action_authorized,
                self.live_policy_authorized,
                self.exact_final_artifact_oof_available,
            )
        ):
            raise RepeatedPolicyBridgeError("receipt claims unsupported authority")
        if self.formal_denominator_eligible:
            if self.exclusion_reasons or self.terminal_value_delta_usdc is None:
                raise RepeatedPolicyBridgeError("eligible receipt is incomplete")
        elif self.terminal_value_delta_usdc is not None:
            raise RepeatedPolicyBridgeError(
                "ineligible receipt cannot expose a paired economic delta"
            )
        body = asdict(self)
        supplied = body.pop("receipt_sha256")
        if _canonical_sha256(body) != supplied:
            raise RepeatedPolicyBridgeError("paired receipt hash mismatch")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> PairedSequentialReplayReceipt:
        values = dict(payload)
        values["exclusion_reasons"] = tuple(values["exclusion_reasons"])
        return cls(**values)


@dataclass(frozen=True, slots=True)
class RestartAwareStateChainReceipt:
    schema_version: str
    identity: str
    chain_identity_sha256: str
    initial_state_manifest_sha256: str
    segment_ids: tuple[str, ...]
    segment_receipt_sha256: tuple[str, ...]
    control_initial_state_sha256: str
    candidate_initial_state_sha256: str
    control_final_state_sha256: str
    candidate_final_state_sha256: str
    restart_count: int
    segment_count: int
    utc_sorted: bool
    state_chain_contiguous: bool
    arm_local_state: bool
    common_exogenous_clocks: bool
    atomic_segment_admission: bool
    repeated_sequential_policy: bool
    one_shot_effect_aggregation_used: bool
    fresh_start_used: bool
    action_authorized: bool
    live_policy_authorized: bool
    receipt_sha256: str

    def __post_init__(self) -> None:
        if (
            self.schema_version != f"{SCHEMA_VERSION}.state_chain"
            or self.identity != IDENTITY
        ):
            raise RepeatedPolicyBridgeError("state-chain receipt identity drifted")
        for name in (
            "chain_identity_sha256",
            "initial_state_manifest_sha256",
            "control_initial_state_sha256",
            "candidate_initial_state_sha256",
            "control_final_state_sha256",
            "candidate_final_state_sha256",
            "receipt_sha256",
        ):
            _require_sha256(getattr(self, name), name)
        if self.segment_count <= 0 or self.segment_count != len(self.segment_ids):
            raise RepeatedPolicyBridgeError("state-chain segment count drifted")
        if self.segment_count != len(self.segment_receipt_sha256):
            raise RepeatedPolicyBridgeError("state-chain receipt count drifted")
        if len(set(self.segment_ids)) != self.segment_count:
            raise RepeatedPolicyBridgeError("state-chain segment identity is duplicated")
        for segment_id in self.segment_ids:
            _normalize_segment_id(segment_id)
        for value in self.segment_receipt_sha256:
            _require_sha256(value, "segment receipt")
        if self.restart_count < 0 or self.restart_count > self.segment_count:
            raise RepeatedPolicyBridgeError("state-chain restart count is invalid")
        if not all(
            (
                self.utc_sorted,
                self.state_chain_contiguous,
                self.arm_local_state,
                self.common_exogenous_clocks,
                self.atomic_segment_admission,
                self.repeated_sequential_policy,
            )
        ):
            raise RepeatedPolicyBridgeError("state-chain mechanics are incomplete")
        if any(
            (
                self.one_shot_effect_aggregation_used,
                self.fresh_start_used,
                self.action_authorized,
                self.live_policy_authorized,
            )
        ):
            raise RepeatedPolicyBridgeError("state-chain receipt claims forbidden behavior")
        body = asdict(self)
        supplied = body.pop("receipt_sha256")
        if _canonical_sha256(body) != supplied:
            raise RepeatedPolicyBridgeError("state-chain receipt hash mismatch")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class AtomicSegmentAdmissionStore:
    """Durable arm checkpoints followed by atomic paired-segment admission."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @contextmanager
    def chain_lock(self, chain_identity_sha256: str):
        chain = _require_sha256(chain_identity_sha256, "chain identity")
        lock = self.root / ".locks" / f"{chain}.lock"
        lock.parent.mkdir(parents=True, exist_ok=True)
        try:
            descriptor = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError as exc:
            raise RepeatedPolicyBridgeError("state chain is already running") from exc
        try:
            os.write(descriptor, f"identity={IDENTITY}\nchain={chain}\n".encode("ascii"))
            os.fsync(descriptor)
            yield
        finally:
            os.close(descriptor)
            lock.unlink(missing_ok=True)

    @staticmethod
    def _segment_key(segment_index: int, segment_id: str) -> str:
        if isinstance(segment_index, bool) or segment_index < 0:
            raise RepeatedPolicyBridgeError("segment index is invalid")
        return f"{segment_index:06d}-{_normalize_segment_id(segment_id)}"

    def _checkpoint_path(
        self,
        *,
        chain_identity_sha256: str,
        segment_index: int,
        segment_id: str,
        arm: str,
    ) -> Path:
        if arm not in ARMS:
            raise RepeatedPolicyBridgeError("checkpoint arm is invalid")
        chain = _require_sha256(chain_identity_sha256, "chain identity")
        key = self._segment_key(segment_index, segment_id)
        return self.root / "checkpoints" / chain / key / f"{arm}.json"

    def write_checkpoint(
        self,
        *,
        chain_identity_sha256: str,
        segment_index: int,
        segment_id: str,
        run_identity_sha256: str,
        evidence: ArmExecutionEvidence,
    ) -> str:
        body = {
            "schema_version": f"{SCHEMA_VERSION}.arm_checkpoint",
            "identity": IDENTITY,
            "chain_identity_sha256": _require_sha256(
                chain_identity_sha256, "chain identity"
            ),
            "segment_index": segment_index,
            "segment_id": _normalize_segment_id(segment_id),
            "run_identity_sha256": _require_sha256(
                run_identity_sha256, "run identity"
            ),
            "arm": evidence.arm,
            "evidence": evidence.to_dict(),
        }
        payload = {**body, "checkpoint_sha256": _canonical_sha256(body)}
        _atomic_json(
            self._checkpoint_path(
                chain_identity_sha256=chain_identity_sha256,
                segment_index=segment_index,
                segment_id=segment_id,
                arm=evidence.arm,
            ),
            payload,
        )
        return str(payload["checkpoint_sha256"])

    def load_checkpoint(
        self,
        *,
        chain_identity_sha256: str,
        segment_index: int,
        segment_id: str,
        arm: str,
        run_identity_sha256: str,
    ) -> tuple[ArmExecutionEvidence, str] | None:
        path = self._checkpoint_path(
            chain_identity_sha256=chain_identity_sha256,
            segment_index=segment_index,
            segment_id=segment_id,
            arm=arm,
        )
        if not path.is_file():
            return None
        payload = _read_json(path, f"{arm} checkpoint")
        supplied = str(payload.pop("checkpoint_sha256", ""))
        if _canonical_sha256(payload) != supplied:
            raise RepeatedPolicyBridgeError("arm checkpoint hash drifted")
        if (
            payload.get("schema_version") != f"{SCHEMA_VERSION}.arm_checkpoint"
            or payload.get("identity") != IDENTITY
            or payload.get("chain_identity_sha256") != chain_identity_sha256
            or payload.get("segment_index") != segment_index
            or payload.get("segment_id") != _normalize_segment_id(segment_id)
            or payload.get("arm") != arm
            or payload.get("run_identity_sha256") != run_identity_sha256
        ):
            raise RepeatedPolicyBridgeError("arm checkpoint identity drifted")
        evidence = ArmExecutionEvidence.from_dict(dict(payload["evidence"]))
        return replace(evidence, checkpoint_reused=True), supplied

    def _segment_path(
        self,
        *,
        chain_identity_sha256: str,
        segment_index: int,
        segment_id: str,
    ) -> Path:
        chain = _require_sha256(chain_identity_sha256, "chain identity")
        return self.root / "segments" / chain / self._segment_key(
            segment_index, segment_id
        )

    def load_admitted_segment(
        self,
        *,
        chain_identity_sha256: str,
        segment_index: int,
        segment_id: str,
        run_identity_sha256: str,
    ) -> PairedSequentialReplayReceipt | None:
        directory = self._segment_path(
            chain_identity_sha256=chain_identity_sha256,
            segment_index=segment_index,
            segment_id=segment_id,
        )
        if not directory.exists():
            return None
        manifest_path = directory / "manifest.json"
        marker_path = directory / SEGMENT_SUCCESS_MARKER
        receipt_path = directory / "receipt.json"
        if not all(path.is_file() for path in (manifest_path, marker_path, receipt_path)):
            raise RepeatedPolicyBridgeError("paired segment admission is incomplete")
        if marker_path.read_text(encoding="ascii").strip() != _sha256_file(manifest_path):
            raise RepeatedPolicyBridgeError("paired segment success marker drifted")
        manifest = _read_json(manifest_path, "paired segment manifest")
        if (
            manifest.get("identity") != IDENTITY
            or manifest.get("chain_identity_sha256") != chain_identity_sha256
            or manifest.get("segment_index") != segment_index
            or manifest.get("segment_id") != _normalize_segment_id(segment_id)
            or manifest.get("run_identity_sha256") != run_identity_sha256
            or manifest.get("receipt_sha256") != _sha256_file(receipt_path)
        ):
            raise RepeatedPolicyBridgeError("paired segment manifest drifted")
        receipt = PairedSequentialReplayReceipt.from_dict(
            _read_json(receipt_path, "paired segment receipt")
        )
        if receipt.receipt_sha256 != manifest.get("logical_receipt_sha256"):
            raise RepeatedPolicyBridgeError("paired segment logical receipt drifted")
        return receipt

    def admit_segment(
        self,
        *,
        chain_identity_sha256: str,
        segment_index: int,
        segment_id: str,
        run_identity_sha256: str,
        receipt: PairedSequentialReplayReceipt,
        checkpoint_sha256: Mapping[str, str],
    ) -> None:
        if set(checkpoint_sha256) != set(ARMS):
            raise RepeatedPolicyBridgeError("paired checkpoint set is incomplete")
        checkpoint_evidence: dict[str, ArmExecutionEvidence] = {}
        for arm in ARMS:
            loaded = self.load_checkpoint(
                chain_identity_sha256=chain_identity_sha256,
                segment_index=segment_index,
                segment_id=segment_id,
                arm=arm,
                run_identity_sha256=run_identity_sha256,
            )
            if loaded is None:
                raise RepeatedPolicyBridgeError(
                    f"{arm} checkpoint disappeared before segment admission"
                )
            evidence, observed_sha256 = loaded
            if observed_sha256 != checkpoint_sha256[arm]:
                raise RepeatedPolicyBridgeError(
                    f"{arm} checkpoint changed before segment admission"
                )
            checkpoint_evidence[arm] = evidence
        control = checkpoint_evidence[CONTROL_ARM]
        candidate = checkpoint_evidence[CANDIDATE_ARM]
        if (
            control.policy_sha256 != receipt.control_policy_sha256
            or candidate.policy_sha256 != receipt.candidate_policy_sha256
            or control.transport_receipt_sha256
            != receipt.control_transport_receipt_sha256
            or candidate.transport_receipt_sha256
            != receipt.candidate_transport_receipt_sha256
            or control.input_state_sha256 != receipt.control_input_state_sha256
            or candidate.input_state_sha256 != receipt.candidate_input_state_sha256
            or control.output_state_sha256 != receipt.control_output_state_sha256
            or candidate.output_state_sha256 != receipt.candidate_output_state_sha256
            or control.repeated_policy_evaluation_count
            != receipt.control_repeated_policy_evaluations
            or candidate.repeated_policy_evaluation_count
            != receipt.candidate_repeated_policy_evaluations
        ):
            raise RepeatedPolicyBridgeError(
                "paired receipt does not match its durable arm checkpoints"
            )
        final = self._segment_path(
            chain_identity_sha256=chain_identity_sha256,
            segment_index=segment_index,
            segment_id=segment_id,
        )
        if final.exists():
            raise RepeatedPolicyBridgeError("paired segment was already admitted")
        final.parent.mkdir(parents=True, exist_ok=True)
        staging = final.parent / f".{final.name}.{uuid.uuid4().hex}.partial"
        staging.mkdir(parents=False, exist_ok=False)
        try:
            receipt_path = staging / "receipt.json"
            _atomic_json(receipt_path, receipt.to_dict())
            manifest = {
                "schema_version": f"{SCHEMA_VERSION}.segment_manifest",
                "identity": IDENTITY,
                "chain_identity_sha256": _require_sha256(
                    chain_identity_sha256, "chain identity"
                ),
                "segment_index": segment_index,
                "segment_id": _normalize_segment_id(segment_id),
                "utc_day": receipt.utc_day,
                "segment_start_utc": receipt.segment_start_utc,
                "segment_end_utc": receipt.segment_end_utc,
                "run_identity_sha256": _require_sha256(
                    run_identity_sha256, "run identity"
                ),
                "receipt_sha256": _sha256_file(receipt_path),
                "logical_receipt_sha256": receipt.receipt_sha256,
                "previous_segment_receipt_sha256": (
                    receipt.previous_segment_receipt_sha256
                ),
                "input_state_sha256": {
                    CONTROL_ARM: receipt.control_input_state_sha256,
                    CANDIDATE_ARM: receipt.candidate_input_state_sha256,
                },
                "output_state_sha256": {
                    CONTROL_ARM: receipt.control_output_state_sha256,
                    CANDIDATE_ARM: receipt.candidate_output_state_sha256,
                },
                "checkpoint_sha256": {
                    arm: _require_sha256(checkpoint_sha256[arm], f"{arm} checkpoint")
                    for arm in ARMS
                },
                "atomic_segment_admission": True,
                "one_shot_effect_aggregation_used": False,
                "fresh_start_used": False,
                "live_equivalent": False,
                "action_authorized": False,
                "live_policy_authorized": False,
            }
            manifest_path = staging / "manifest.json"
            _atomic_json(manifest_path, manifest)
            _atomic_text(
                staging / SEGMENT_SUCCESS_MARKER,
                _sha256_file(manifest_path) + "\n",
            )
            os.replace(staging, final)
        finally:
            if staging.exists():
                shutil.rmtree(staging)


# Backward-compatible class name; the implementation is segment-scoped.
AtomicDayAdmissionStore = AtomicSegmentAdmissionStore


def _state_identity_sha256(value: ArmLocalStateIdentity) -> str:
    return _canonical_sha256(asdict(value))


def _paired_receipt(
    *,
    chain_identity_sha256: str,
    segment_index: int,
    segment_id: str,
    utc_day: str,
    segment_start_utc: str,
    segment_end_utc: str,
    previous_segment_receipt_sha256: str | None,
    day_admission: FormalDayAdmissionBinding,
    target_side: CandidateTargetSide,
    artifact_binding: ArtifactIdentityBinding,
    common_input_sha256: str,
    common_market_sha256: str,
    common_receive_clock_sha256: str,
    common_feature_ready_clock_sha256: str,
    common_random_sha256: str,
    restart_binding: FullyBoundRestartBinding | None,
    control: ArmExecutionEvidence,
    candidate: ArmExecutionEvidence,
    paired_audit: successor.PairedRepeatedPolicyAudit,
) -> PairedSequentialReplayReceipt:
    body: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "chain_identity_sha256": chain_identity_sha256,
        "segment_index": segment_index,
        "segment_id": segment_id,
        "utc_day": utc_day,
        "segment_start_utc": segment_start_utc,
        "segment_end_utc": segment_end_utc,
        "previous_segment_receipt_sha256": previous_segment_receipt_sha256,
        "day_admission_identity": day_admission.admission_identity,
        "day_admission_receipt_sha256": day_admission.receipt_sha256,
        "candidate_target_side": str(target_side),
        "control_policy_identity": successor.ACTIVE_OWNER_POLICY_IDENTITY,
        "control_policy_sha256": successor.ACTIVE_OWNER_POLICY_SHA256,
        "candidate_policy_identity": artifact_binding.executed_policy_identity,
        "candidate_policy_sha256": artifact_binding.executed_policy_sha256,
        "executed_artifact_scope": str(artifact_binding.executed_artifact_scope),
        "learning_algorithm_identity": artifact_binding.learning_algorithm_identity,
        "learning_algorithm_artifact_sha256": (
            artifact_binding.learning_algorithm_artifact_sha256
        ),
        "final_artifact_identity": artifact_binding.final_artifact_identity,
        "final_artifact_sha256": artifact_binding.final_artifact_sha256,
        "exact_final_artifact_oof_available": (
            artifact_binding.exact_final_artifact_oof_available
        ),
        "common_input_identity_sha256": common_input_sha256,
        "common_market_source_sha256": common_market_sha256,
        "common_receive_clock_source_sha256": common_receive_clock_sha256,
        "common_feature_ready_clock_source_sha256": (
            common_feature_ready_clock_sha256
        ),
        "common_random_source_sha256": common_random_sha256,
        "paired_exogenous_clock_identity_sha256": _canonical_sha256(
            {
                "common_market_source_sha256": common_market_sha256,
                "common_receive_clock_source_sha256": common_receive_clock_sha256,
                "common_feature_ready_clock_source_sha256": (
                    common_feature_ready_clock_sha256
                ),
                "common_random_source_sha256": common_random_sha256,
            }
        ),
        "control_state_identity_sha256": _state_identity_sha256(
            control.state_identity
        ),
        "candidate_state_identity_sha256": _state_identity_sha256(
            candidate.state_identity
        ),
        "control_input_state_sha256": control.input_state_sha256,
        "candidate_input_state_sha256": candidate.input_state_sha256,
        "control_output_state_sha256": control.output_state_sha256,
        "candidate_output_state_sha256": candidate.output_state_sha256,
        "restart_manifest_sha256": (
            None
            if restart_binding is None
            else restart_binding.restart_manifest_sha256
        ),
        "fully_bound_restart_restored": (
            control.fully_bound_restart_restored
            and candidate.fully_bound_restart_restored
        ),
        "control_repeated_policy_evaluations": (
            control.repeated_policy_evaluation_count
        ),
        "candidate_repeated_policy_evaluations": (
            candidate.repeated_policy_evaluation_count
        ),
        "candidate_target_side_evaluations": int(
            candidate.target_side_evaluation_count or 0
        ),
        "candidate_b0_delegated_evaluations": int(
            candidate.b0_delegated_evaluation_count or 0
        ),
        "control_campaign_terminal_value_usdc": (
            control.campaign_terminal_value_usdc
        ),
        "candidate_campaign_terminal_value_usdc": (
            candidate.campaign_terminal_value_usdc
        ),
        "terminal_value_delta_usdc": paired_audit.terminal_value_delta_usdc,
        "formal_denominator_eligible": paired_audit.formal_denominator_eligible,
        "exclusion_reasons": tuple(paired_audit.exclusion_reasons),
        "control_transport_receipt_sha256": control.transport_receipt_sha256,
        "candidate_transport_receipt_sha256": candidate.transport_receipt_sha256,
        "control_checkpoint_reused": control.checkpoint_reused,
        "candidate_checkpoint_reused": candidate.checkpoint_reused,
        "paired_audit_sha256": _canonical_sha256(asdict(paired_audit)),
        "repeated_sequential_policy": True,
        "one_shot_effect_aggregation_used": False,
        "same_market_source": (
            control.common_market_source_sha256
            == candidate.common_market_source_sha256
            == common_market_sha256
        ),
        "same_receive_and_feature_ready_clocks": (
            control.common_receive_clock_source_sha256
            == candidate.common_receive_clock_source_sha256
            == common_receive_clock_sha256
            and control.common_feature_ready_clock_source_sha256
            == candidate.common_feature_ready_clock_source_sha256
            == common_feature_ready_clock_sha256
        ),
        "common_random_source": (
            control.common_random_source_sha256
            == candidate.common_random_source_sha256
            == common_random_sha256
        ),
        "arm_local_state": not bool(
            set(control.state_identity.values())
            & set(candidate.state_identity.values())
        ),
        "state_chain_contiguous": True,
        "fresh_start_used": False,
        "live_equivalent": False,
        "research_supported": False,
        "action_authorized": False,
        "live_policy_authorized": False,
    }
    return PairedSequentialReplayReceipt(
        **body,
        receipt_sha256=_canonical_sha256(body),
    )


def build_initial_state_manifest_sha256(
    initial_states: Mapping[str, ArmStateSnapshot],
) -> str:
    if set(initial_states) != set(ARMS):
        raise RepeatedPolicyBridgeError("initial state set is incomplete")
    for arm in ARMS:
        if initial_states[arm].arm != arm:
            raise RepeatedPolicyBridgeError("initial state arm drifted")
    if initial_states[CONTROL_ARM].state_sha256 == initial_states[CANDIDATE_ARM].state_sha256:
        raise RepeatedPolicyBridgeError("paired arms share an initial state hash")
    return _canonical_sha256(
        {
            "schema_version": f"{SCHEMA_VERSION}.initial_state_manifest",
            "identity": IDENTITY,
            "state_sha256": {
                arm: initial_states[arm].state_sha256 for arm in ARMS
            },
            "fresh_start_used": False,
            "state_source": "explicit_fully_bound_replay_initial_state",
        }
    )


def _segment_run_identity_sha256(
    *,
    chain_identity_sha256: str,
    segment: ReplaySegmentSpec,
    segment_index: int,
    target_side: CandidateTargetSide,
    artifact_binding: ArtifactIdentityBinding,
    input_states: Mapping[str, ArmStateSnapshot],
    previous_segment_receipt_sha256: str | None,
) -> str:
    return _canonical_sha256(
        {
            "identity": IDENTITY,
            "chain_identity_sha256": chain_identity_sha256,
            "segment_index": segment_index,
            "segment_id": segment.segment_id,
            "utc_day": segment.utc_day,
            "segment_start_utc": segment.segment_start_utc,
            "segment_end_utc": segment.segment_end_utc,
            "candidate_target_side": str(target_side),
            "common_input_identity_sha256": _canonical_sha256(
                segment.common_input_identity
            ),
            "input_state_sha256": {
                arm: input_states[arm].state_sha256 for arm in ARMS
            },
            "previous_segment_receipt_sha256": previous_segment_receipt_sha256,
            "day_admission_identity": segment.day_admission.admission_identity,
            "day_admission_receipt_sha256": segment.day_admission.receipt_sha256,
            "restart_manifest_sha256": (
                None
                if segment.restart_binding is None
                else segment.restart_binding.restart_manifest_sha256
            ),
            "artifact_binding": asdict(artifact_binding),
        }
    )


def _validate_segment_predecessor(
    *,
    chain_identity_sha256: str,
    segment: ReplaySegmentSpec,
    segment_index: int,
    input_states: Mapping[str, ArmStateSnapshot],
    previous_receipt: PairedSequentialReplayReceipt | None,
) -> None:
    if set(input_states) != set(ARMS):
        raise RepeatedPolicyBridgeError("segment input state set is incomplete")
    for arm in ARMS:
        if input_states[arm].arm != arm:
            raise RepeatedPolicyBridgeError("segment input state arm drifted")
    if segment_index == 0:
        if previous_receipt is not None:
            raise RepeatedPolicyBridgeError("first segment unexpectedly has a predecessor")
    else:
        if previous_receipt is None:
            raise RepeatedPolicyBridgeError("segment predecessor receipt is missing")
        if (
            previous_receipt.chain_identity_sha256 != chain_identity_sha256
            or previous_receipt.segment_index != segment_index - 1
        ):
            raise RepeatedPolicyBridgeError("segment predecessor identity drifted")
        previous_end = datetime.fromisoformat(
            previous_receipt.segment_end_utc[:-1] + "+00:00"
        )
        current_start = datetime.fromisoformat(
            segment.segment_start_utc[:-1] + "+00:00"
        )
        if current_start < previous_end:
            raise RepeatedPolicyBridgeError("segments overlap or are not UTC sorted")
        expected = {
            CONTROL_ARM: previous_receipt.control_output_state_sha256,
            CANDIDATE_ARM: previous_receipt.candidate_output_state_sha256,
        }
        for arm in ARMS:
            if input_states[arm].state_sha256 != expected[arm]:
                raise RepeatedPolicyBridgeError(
                    f"{arm} input_state_sha256 does not equal previous output_state_sha256"
                )
    restart = segment.restart_binding
    if restart is not None:
        for arm in ARMS:
            if restart.restored_state_sha256[arm] != input_states[arm].state_sha256:
                raise RepeatedPolicyBridgeError(
                    f"{arm} fully-bound restart restored a different state hash"
                )


def _execute_repeated_sequential_segment(
    *,
    chain_identity_sha256: str,
    segment_index: int,
    segment: ReplaySegmentSpec,
    input_states: Mapping[str, ArmStateSnapshot],
    previous_receipt: PairedSequentialReplayReceipt | None,
    target_side: CandidateTargetSide,
    target_policy: BooleanCooldownPolicy,
    artifact_binding: ArtifactIdentityBinding,
    arm_executor: SequentialArmExecutor,
    snapshot_emitter_factory: Callable[
        [str, Mapping[str, Any], ArmLocalStateIdentity], Any
    ],
    expected_identity_hashes: Mapping[str, str] | None,
    admission_store: AtomicSegmentAdmissionStore,
) -> tuple[PairedSequentialReplayReceipt, dict[str, ArmExecutionEvidence]]:
    normalized_side = CandidateTargetSide(target_side)
    _validate_segment_predecessor(
        chain_identity_sha256=chain_identity_sha256,
        segment=segment,
        segment_index=segment_index,
        input_states=input_states,
        previous_receipt=previous_receipt,
    )
    frozen_common = dict(segment.common_input_identity)
    if not frozen_common:
        raise RepeatedPolicyBridgeError("common replay identity is empty")
    common_sha = _canonical_sha256(frozen_common)
    common_market_sha = _require_sha256(
        frozen_common.get("transport_common_market_source_sha256"),
        "common market source",
    )
    common_receive_clock_sha = _require_sha256(
        frozen_common.get("common_receive_clock_source_sha256"),
        "common receive clock source",
    )
    common_feature_ready_clock_sha = _require_sha256(
        frozen_common.get("common_feature_ready_clock_source_sha256"),
        "common feature-ready clock source",
    )
    common_random_sha = _require_sha256(
        frozen_common.get("common_random_source_sha256"),
        "common random source",
    )
    if artifact_binding.executed_policy_sha256 == successor.ACTIVE_OWNER_POLICY_SHA256:
        raise RepeatedPolicyBridgeError("candidate cannot reuse the exact B0 artifact")
    previous_sha = None if previous_receipt is None else previous_receipt.receipt_sha256
    run_identity_sha = _segment_run_identity_sha256(
        chain_identity_sha256=chain_identity_sha256,
        segment=segment,
        segment_index=segment_index,
        target_side=normalized_side,
        artifact_binding=artifact_binding,
        input_states=input_states,
        previous_segment_receipt_sha256=previous_sha,
    )
    admitted = admission_store.load_admitted_segment(
        chain_identity_sha256=chain_identity_sha256,
        segment_index=segment_index,
        segment_id=segment.segment_id,
        run_identity_sha256=run_identity_sha,
    )
    if admitted is not None:
        evidence_by_arm: dict[str, ArmExecutionEvidence] = {}
        for arm in ARMS:
            loaded = admission_store.load_checkpoint(
                chain_identity_sha256=chain_identity_sha256,
                segment_index=segment_index,
                segment_id=segment.segment_id,
                arm=arm,
                run_identity_sha256=run_identity_sha,
            )
            if loaded is None:
                raise RepeatedPolicyBridgeError(
                    "admitted segment lacks a durable arm checkpoint"
                )
            evidence_by_arm[arm] = loaded[0]
        return admitted, evidence_by_arm

    control_evaluator = build_exact_current_owner_evaluator(
        expected_identity_hashes=expected_identity_hashes
    )
    candidate_evaluator = build_target_side_candidate_evaluator(
        target_side=normalized_side,
        target_policy=target_policy,
        artifact_binding=artifact_binding,
        expected_identity_hashes=expected_identity_hashes,
    )
    requests: dict[str, ArmReplayRequest] = {}
    emitters: list[Any] = []
    for arm, evaluator in (
        (CONTROL_ARM, control_evaluator),
        (CANDIDATE_ARM, candidate_evaluator),
    ):
        state_identity = ArmLocalStateIdentity.build(
            run_identity_sha256=chain_identity_sha256,
            arm=arm,
        )
        emitter = snapshot_emitter_factory(arm, frozen_common, state_identity)
        if not all(
            callable(getattr(emitter, method, None))
            for method in ("capture_exposure_fill", "audit")
        ):
            raise RepeatedPolicyBridgeError(
                "snapshot emitter does not satisfy the backtest_tick ABI"
            )
        emitters.append(emitter)
        requests[arm] = ArmReplayRequest(
            chain_identity_sha256=chain_identity_sha256,
            segment_index=segment_index,
            segment_id=segment.segment_id,
            utc_day=segment.utc_day,
            segment_start_utc=segment.segment_start_utc,
            segment_end_utc=segment.segment_end_utc,
            arm=arm,
            evaluator=evaluator,
            snapshot_emitter=emitter,
            common_input_identity=frozen_common,
            common_input_identity_sha256=common_sha,
            common_market_source_sha256=common_market_sha,
            common_receive_clock_source_sha256=common_receive_clock_sha,
            common_feature_ready_clock_source_sha256=(
                common_feature_ready_clock_sha
            ),
            common_random_source_sha256=common_random_sha,
            target_side=normalized_side,
            state_identity=state_identity,
            input_state=input_states[arm],
            restart_binding=segment.restart_binding,
        )
    if emitters[0] is emitters[1]:
        raise RepeatedPolicyBridgeError("paired arms share one snapshot emitter")
    if set(requests[CONTROL_ARM].state_identity.values()) & set(
        requests[CANDIDATE_ARM].state_identity.values()
    ):
        raise RepeatedPolicyBridgeError("paired arms share mutable state identities")

    evidence_by_arm = {}
    checkpoint_sha: dict[str, str] = {}

    def simulator(
        arm: str,
        evaluator: Any,
        received_common: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        request = requests[arm]
        if evaluator is not request.evaluator:
            raise RepeatedPolicyBridgeError("paired runner evaluator identity drifted")
        if dict(received_common) != frozen_common:
            raise RepeatedPolicyBridgeError("paired runner common input drifted")
        loaded = admission_store.load_checkpoint(
            chain_identity_sha256=chain_identity_sha256,
            segment_index=segment_index,
            segment_id=segment.segment_id,
            arm=arm,
            run_identity_sha256=run_identity_sha,
        )
        if loaded is not None:
            evidence, checkpoint = loaded
            if evidence.policy_sha256 != str(request.evaluator.policy_sha256):
                raise RepeatedPolicyBridgeError("checkpoint policy identity drifted")
            if evidence.input_state_sha256 != request.input_state.state_sha256:
                raise RepeatedPolicyBridgeError("checkpoint input state identity drifted")
            evidence_by_arm[arm] = evidence
            checkpoint_sha[arm] = checkpoint
            return evidence.simulator_result()
        raw = arm_executor(request)
        evidence = adapt_backtest_tick_arm_result(request, raw)
        evidence_by_arm[arm] = evidence
        checkpoint_sha[arm] = admission_store.write_checkpoint(
            chain_identity_sha256=chain_identity_sha256,
            segment_index=segment_index,
            segment_id=segment.segment_id,
            run_identity_sha256=run_identity_sha,
            evidence=evidence,
        )
        return evidence.simulator_result()

    try:
        paired_audit = successor.execute_paired_repeated_policy(
            utc_day=segment.utc_day,
            common_input_identity=frozen_common,
            exact_owner_evaluator=control_evaluator,
            candidate_evaluator=candidate_evaluator,
            simulator=simulator,
            formal_economic_mode=True,
            prospective_day_admission=segment.day_admission,
        )
    except successor.SuccessorContractError as exc:
        raise RepeatedPolicyBridgeError("paired successor admission failed") from exc
    if set(evidence_by_arm) != set(ARMS):
        raise RepeatedPolicyBridgeError("paired arm evidence is incomplete")
    control = evidence_by_arm[CONTROL_ARM]
    candidate = evidence_by_arm[CANDIDATE_ARM]
    receipt = _paired_receipt(
        chain_identity_sha256=chain_identity_sha256,
        segment_index=segment_index,
        segment_id=segment.segment_id,
        utc_day=segment.utc_day,
        segment_start_utc=segment.segment_start_utc,
        segment_end_utc=segment.segment_end_utc,
        previous_segment_receipt_sha256=previous_sha,
        day_admission=segment.day_admission,
        target_side=normalized_side,
        artifact_binding=artifact_binding,
        common_input_sha256=common_sha,
        common_market_sha256=common_market_sha,
        common_receive_clock_sha256=common_receive_clock_sha,
        common_feature_ready_clock_sha256=common_feature_ready_clock_sha,
        common_random_sha256=common_random_sha,
        restart_binding=segment.restart_binding,
        control=control,
        candidate=candidate,
        paired_audit=paired_audit,
    )
    admission_store.admit_segment(
        chain_identity_sha256=chain_identity_sha256,
        segment_index=segment_index,
        segment_id=segment.segment_id,
        run_identity_sha256=run_identity_sha,
        receipt=receipt,
        checkpoint_sha256=checkpoint_sha,
    )
    return receipt, evidence_by_arm


def execute_restart_aware_repeated_policy_state_chain(
    *,
    segments: Sequence[ReplaySegmentSpec],
    initial_states: Mapping[str, ArmStateSnapshot],
    initial_state_manifest_sha256: str,
    target_side: CandidateTargetSide,
    target_policy: BooleanCooldownPolicy,
    artifact_binding: ArtifactIdentityBinding,
    arm_executor: SequentialArmExecutor,
    snapshot_emitter_factory: Callable[
        [str, Mapping[str, Any], ArmLocalStateIdentity], Any
    ],
    admission_store: AtomicSegmentAdmissionStore,
    expected_identity_hashes: Mapping[str, str] | None = None,
) -> RestartAwareStateChainReceipt:
    """Run UTC-sorted sessions while preserving two independent arm states."""

    ordered = tuple(segments)
    if not ordered:
        raise RepeatedPolicyBridgeError("state chain has no replay segments")
    expected_initial_manifest = build_initial_state_manifest_sha256(initial_states)
    if initial_state_manifest_sha256 != expected_initial_manifest:
        raise RepeatedPolicyBridgeError("initial state manifest hash drifted")
    starts = [
        datetime.fromisoformat(segment.segment_start_utc[:-1] + "+00:00")
        for segment in ordered
    ]
    if starts != sorted(starts):
        raise RepeatedPolicyBridgeError("segments are not UTC sorted")
    for previous, current in zip(ordered, ordered[1:], strict=False):
        previous_end = datetime.fromisoformat(
            previous.segment_end_utc[:-1] + "+00:00"
        )
        current_start = datetime.fromisoformat(
            current.segment_start_utc[:-1] + "+00:00"
        )
        if current_start < previous_end:
            raise RepeatedPolicyBridgeError("segments overlap")
    if len({segment.segment_id for segment in ordered}) != len(ordered):
        raise RepeatedPolicyBridgeError("segment ids are duplicated")
    chain_identity_sha256 = _canonical_sha256(
        {
            "schema_version": f"{SCHEMA_VERSION}.state_chain_identity",
            "identity": IDENTITY,
            "initial_state_manifest_sha256": initial_state_manifest_sha256,
            "candidate_target_side": str(CandidateTargetSide(target_side)),
            "artifact_binding": asdict(artifact_binding),
            "state_components": STATE_COMPONENTS,
            "fresh_start_used": False,
        }
    )
    current_states = dict(initial_states)
    receipts: list[PairedSequentialReplayReceipt] = []
    restart_count = 0
    with admission_store.chain_lock(chain_identity_sha256):
        previous_receipt: PairedSequentialReplayReceipt | None = None
        for index, segment in enumerate(ordered):
            if segment.restart_binding is not None:
                restart_count += 1
            receipt, evidence_by_arm = _execute_repeated_sequential_segment(
                chain_identity_sha256=chain_identity_sha256,
                segment_index=index,
                segment=segment,
                input_states=current_states,
                previous_receipt=previous_receipt,
                target_side=target_side,
                target_policy=target_policy,
                artifact_binding=artifact_binding,
                arm_executor=arm_executor,
                snapshot_emitter_factory=snapshot_emitter_factory,
                expected_identity_hashes=expected_identity_hashes,
                admission_store=admission_store,
            )
            for arm in ARMS:
                if evidence_by_arm[arm].input_state_sha256 != current_states[arm].state_sha256:
                    raise RepeatedPolicyBridgeError(
                        f"{arm} admitted checkpoint breaks the state chain"
                    )
            current_states = {
                arm: evidence_by_arm[arm].output_state for arm in ARMS
            }
            receipts.append(receipt)
            previous_receipt = receipt
    body: dict[str, Any] = {
        "schema_version": f"{SCHEMA_VERSION}.state_chain",
        "identity": IDENTITY,
        "chain_identity_sha256": chain_identity_sha256,
        "initial_state_manifest_sha256": initial_state_manifest_sha256,
        "segment_ids": tuple(segment.segment_id for segment in ordered),
        "segment_receipt_sha256": tuple(
            receipt.receipt_sha256 for receipt in receipts
        ),
        "control_initial_state_sha256": initial_states[CONTROL_ARM].state_sha256,
        "candidate_initial_state_sha256": initial_states[
            CANDIDATE_ARM
        ].state_sha256,
        "control_final_state_sha256": current_states[CONTROL_ARM].state_sha256,
        "candidate_final_state_sha256": current_states[
            CANDIDATE_ARM
        ].state_sha256,
        "restart_count": restart_count,
        "segment_count": len(ordered),
        "utc_sorted": True,
        "state_chain_contiguous": True,
        "arm_local_state": True,
        "common_exogenous_clocks": True,
        "atomic_segment_admission": True,
        "repeated_sequential_policy": True,
        "one_shot_effect_aggregation_used": False,
        "fresh_start_used": False,
        "action_authorized": False,
        "live_policy_authorized": False,
    }
    return RestartAwareStateChainReceipt(
        **body,
        receipt_sha256=_canonical_sha256(body),
    )


__all__ = [
    "ARMS",
    "BACKTEST_TICK_EVALUATOR_ABI",
    "CANDIDATE_ARM",
    "CONTROL_ARM",
    "IDENTITY",
    "ArtifactIdentityBinding",
    "ArmLocalStateIdentity",
    "ArmReplayRequest",
    "ArmStateSnapshot",
    "AtomicDayAdmissionStore",
    "AtomicSegmentAdmissionStore",
    "CandidateTargetSide",
    "ExecutedArtifactScope",
    "FormalDayAdmissionBinding",
    "FormalDayAdmissionProtocol",
    "FullyBoundRestartBinding",
    "PairedSequentialReplayReceipt",
    "ReplaySegmentSpec",
    "RepeatedPolicyBridgeError",
    "RestartAwareStateChainReceipt",
    "TargetSideDelegatingEvaluator",
    "adapt_backtest_tick_arm_result",
    "build_initial_state_manifest_sha256",
    "build_exact_current_owner_evaluator",
    "build_target_side_candidate_evaluator",
    "execute_restart_aware_repeated_policy_state_chain",
]
