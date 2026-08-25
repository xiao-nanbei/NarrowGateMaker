#!/usr/bin/env python3
"""Execution preflight for F03 1s versus v9 10s continuous-calendar A/B.

The runner stops at an immutable sequence of paired execution requests.  It
does not call the tick engine, read an outcome, or write an economic report.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from models.backtest_config import load_operational_baseline_binding
from models.replay import (
    continuous_accounting,
    replay_state_checkpoint,
    restart_boundary,
)
from models.replay.continuous_accounting import ContinuousAccountingLedger
from models.replay.replay_state_checkpoint import ContinuousReplayState
from models.replay.restart_aware_continuous_ab import (
    ContinuousABPlan,
    ContinuousABPreflightError,
    PairedExecutionRequest,
    build_complete_calendar_plan,
    canonical_sha256,
    ordered_days,
    paired_execution_requests,
    sha256_file,
    source_artifact_manifest_payload,
)
from models.replay.restart_boundary import (
    PlannedRestartInterval,
    RestartBoundaryMachine,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_full_schema as one_second_full_schema,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_ml_ab_replay as one_second_replay,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_schema as one_second_schema,
)

ROOT = Path(__file__).resolve().parents[4]
DEFAULT_SPEC = (
    ROOT / "research/families/f03_causal_13_head/docs/"
    "causal_v12_1s_restart_aware_continuous_calendar_ab_v2_preflight_20260825.json"
)
DEFAULT_AMENDMENT = (
    ROOT / "research/families/f03_causal_13_head/docs/"
    "causal_v12_1s_restart_aware_continuous_calendar_ab_v2_"
    "execution_binding_amendment_20260825.json"
)
DEFAULT_IDENTITY = (
    ROOT / "research/families/f03_causal_13_head/docs/"
    "causal_v12_1s_restart_aware_continuous_calendar_ab_v2_"
    "default_identity_20260825.json"
)
DEFAULT_EXECUTION_RUNNER = (
    ROOT / "scripts/run_restart_aware_continuous_baseline.py"
)
FILL_TRACE_ORDERING_IDENTITY = "result_local_physical_fill_sequence.v1"
FILL_TRACE_REQUIRED_FIELDS = (
    "fill_sequence",
    "fill_ts",
    "side",
    "fill_qty",
    "quote_px",
    "fill_fee_usdc",
    "fill_fee_asset",
    "fill_fee_semantics",
    "inventory_before_fill",
    "inventory_after_fill",
)
FILL_TRACE_SOURCE_PATHS = {
    "python_replay_engine": ROOT / "models/backtest_tick.py",
    "cpp_replay_engine": ROOT / "cpp/narrowgate_cpp/tick_replay.cpp",
    "cpp_replay_abi": ROOT / "cpp/narrowgate_cpp/tick_replay.hpp",
    "cpp_python_binding": ROOT / "cpp/narrowgate_cpp/bindings.cpp",
}
CALENDAR_MANIFEST = (
    ROOT / "research/shared/replay_lifecycle/docs/"
    "calendar_continuity_manifest_20260417_20260730_v1.json"
)
START_DAY = "2026-04-17"
END_DAY = "2026-06-26"
SCHEMA_VERSION = "causal_v12_1s_restart_aware_continuous_calendar_ab.v2"
AMENDMENT_SCHEMA_VERSION = (
    "causal_v12_1s_restart_aware_continuous_calendar_ab_execution_binding.v2"
)
DEFAULT_IDENTITY_SCHEMA_VERSION = (
    "causal_v12_1s_restart_aware_continuous_calendar_ab_default_identity.v2"
)
PREFLIGHT_STATUS = "preflight_v2_signed_fee_results_read_closed"
AMENDMENT_STATUS = "execution_plan_v2_candidate_unbound_results_closed"
RESTART_REQUIRED_METHODS = (
    "register_active_order",
    "begin_maintenance",
    "request_cancel",
    "partial_fill",
    "cancel_reject",
    "terminal",
    "enter_offline",
    "begin_restart",
    "complete_warmup",
)
ACCOUNTING_REQUIRED_METHODS = (
    "mark",
    "enter_planned_restart",
    "resume_after_warmup",
    "fill",
    "record_gap",
    "close_utc_day",
    "accounting_audit",
)


class F03ContinuousABPreflightError(ContinuousABPreflightError):
    """Raised when the frozen F03 continuous A/B identity is incomplete."""


@dataclass(frozen=True, slots=True)
class ArtifactBinding:
    path: Path
    sha256: str

    @classmethod
    def from_payload(
        cls,
        payload: Any,
        *,
        role: str,
        require_file: bool = True,
    ) -> ArtifactBinding:
        if not isinstance(payload, Mapping):
            raise F03ContinuousABPreflightError(f"candidate {role} is not bound")
        raw_path = payload.get("path")
        expected = payload.get("sha256")
        if not isinstance(raw_path, str) or not raw_path.strip():
            raise F03ContinuousABPreflightError(f"candidate {role} path is not bound")
        if not isinstance(expected, str) or len(expected) != 64:
            raise F03ContinuousABPreflightError(f"candidate {role} SHA256 is not bound")
        path = Path(raw_path).expanduser()
        if not path.is_absolute():
            path = ROOT / path
        path = path.resolve()
        if require_file and not path.is_file():
            raise F03ContinuousABPreflightError(f"candidate {role} is missing: {path}")
        if require_file and sha256_file(path) != expected:
            raise F03ContinuousABPreflightError(f"candidate {role} SHA256 mismatch")
        return cls(path=path, sha256=expected)


@dataclass(frozen=True, slots=True)
class CandidateArtifacts:
    bundle_meta: ArtifactBinding
    feature_dag: ArtifactBinding
    overlay_index: ArtifactBinding
    overlay_root: Path
    bundle_identity: str
    overlay_index_identity: str


@dataclass(frozen=True, slots=True)
class ExecutionInterfaceBindings:
    restart_boundary_sha256: str
    continuous_accounting_sha256: str
    source_artifact_manifest_sha256: str
    parent_precommit_file_sha256: str
    parent_precommit_canonical_sha256: str
    amendment_sha256: str


@dataclass(frozen=True, slots=True)
class F03ContinuousABPreflight:
    spec_path: Path
    spec_sha256: str
    plan: ContinuousABPlan
    candidate: CandidateArtifacts
    requests: tuple[PairedExecutionRequest, ...]
    control_baseline_id: str
    control_identity_sha256: str
    control_config_sha256: str
    interfaces: ExecutionInterfaceBindings
    outcome_reads_enabled: bool = False
    tick_engine_called: bool = False

    def machine_payload(self) -> dict[str, Any]:
        payload = {
            "schema_version": SCHEMA_VERSION,
            "spec_path": str(self.spec_path),
            "spec_sha256": self.spec_sha256,
            "calendar": {
                "start": self.plan.calendar_start_day,
                "end": self.plan.calendar_end_day,
                "days": self.plan.calendar_day_count,
                "all_days_trade": self.plan.all_calendar_days_trade,
                "restart_intervals": len(self.plan.restart_intervals),
                "restart_timeline_sha256": self.plan.restart_timeline_sha256,
                "source_artifact_manifest_sha256": (self.plan.source_artifact_manifest_sha256),
            },
            "control": {
                "baseline_id": self.control_baseline_id,
                "identity_sha256": self.control_identity_sha256,
                "config_sha256": self.control_config_sha256,
                "cadence_ms": 10_000,
            },
            "candidate": {
                "identity": self.candidate.bundle_identity,
                "bundle_meta_path": str(self.candidate.bundle_meta.path),
                "bundle_meta_sha256": self.candidate.bundle_meta.sha256,
                "feature_dag_path": str(self.candidate.feature_dag.path),
                "feature_dag_sha256": self.candidate.feature_dag.sha256,
                "overlay_index_path": str(self.candidate.overlay_index.path),
                "overlay_index_sha256": self.candidate.overlay_index.sha256,
                "cadence_ms": 1_000,
            },
            "execution_request_count": len(self.requests),
            "execution_binding": {
                "amendment_sha256": self.interfaces.amendment_sha256,
                "parent_precommit_file_sha256": (self.interfaces.parent_precommit_file_sha256),
                "parent_precommit_canonical_sha256": (
                    self.interfaces.parent_precommit_canonical_sha256
                ),
                "restart_boundary_sha256": self.interfaces.restart_boundary_sha256,
                "continuous_accounting_sha256": (self.interfaces.continuous_accounting_sha256),
                "source_artifact_manifest_sha256": (
                    self.interfaces.source_artifact_manifest_sha256
                ),
                "full_path_runner_bound": False,
                "execution_plan_skeleton": True,
            },
            "state_namespaces": {
                "control": self.plan.control_state_namespace,
                "candidate": self.plan.candidate_state_namespace,
            },
            "outcome_reads_enabled": self.outcome_reads_enabled,
            "tick_engine_called": self.tick_engine_called,
            "execution_authority": False,
            "action_authority": False,
            "live_authority": False,
        }
        payload["preflight_identity_sha256"] = canonical_sha256(payload)
        return payload


def _load_json(path: Path, *, role: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise F03ContinuousABPreflightError(f"invalid {role}: {path}") from exc
    if not isinstance(payload, dict):
        raise F03ContinuousABPreflightError(f"{role} must be a JSON object")
    return payload


def _resolve_spec_path(path: Path) -> Path:
    path = path.expanduser()
    if not path.is_absolute():
        path = ROOT / path
    return path.resolve()


def _canonical_document_sha256(payload: Mapping[str, Any], *, identity_field: str) -> str:
    unsigned = dict(payload)
    unsigned.pop(identity_field, None)
    return canonical_sha256(unsigned)


def _validate_fill_trace_ordering_contract(
    payload: Any,
    *,
    execution_runner: ArtifactBinding,
    source_bindings: Mapping[str, ArtifactBinding],
    role: str,
) -> None:
    if not isinstance(payload, Mapping):
        raise F03ContinuousABPreflightError(f"{role} runner binding is absent")
    bound_runner = ArtifactBinding.from_payload(
        payload.get("continuous_execution_runner"),
        role=f"{role} continuous execution runner",
    )
    if bound_runner != execution_runner:
        raise F03ContinuousABPreflightError(f"{role} execution runner drifted")
    contract = payload.get("fill_trace_ordering_contract")
    if not isinstance(contract, Mapping):
        raise F03ContinuousABPreflightError(f"{role} fill trace contract is absent")
    if (
        contract.get("identity") != FILL_TRACE_ORDERING_IDENTITY
        or type(contract.get("sequence_origin")) is not int
        or contract.get("sequence_origin") != 0
        or contract.get("sequence_scope") != "single_tick_replay_result"
        or contract.get("sequence_authority") != "physical_fill_append_order"
        or contract.get("timestamp_or_order_id_tiebreak_authorized") is not False
        or contract.get("complete_path_validated_before_ledger_mutation") is not True
        or tuple(contract.get("required_fields") or ()) != FILL_TRACE_REQUIRED_FIELDS
    ):
        raise F03ContinuousABPreflightError(f"{role} fill trace semantics drifted")
    for name, expected in source_bindings.items():
        observed = ArtifactBinding.from_payload(
            contract.get(name),
            role=f"{role} fill trace {name}",
        )
        if observed != expected:
            raise F03ContinuousABPreflightError(
                f"{role} fill trace {name} drifted"
            )


def validate_default_binding_documents(
    identity_path: Path = DEFAULT_IDENTITY,
) -> dict[str, Any]:
    """Validate the public prospective v2 default without owner-private data."""
    resolved_identity = _resolve_spec_path(identity_path)
    identity = _load_json(resolved_identity, role="F03 v2 default identity")
    if (
        identity.get("schema_version") != DEFAULT_IDENTITY_SCHEMA_VERSION
        or identity.get("status") != "prospective_create_only_results_closed"
    ):
        raise F03ContinuousABPreflightError("F03 v2 default identity is stale")
    expected_canonical = str(identity.get("canonical_identity_sha256", ""))
    if (
        _canonical_document_sha256(
            identity,
            identity_field="canonical_identity_sha256",
        )
        != expected_canonical
    ):
        raise F03ContinuousABPreflightError("F03 v2 default identity is not canonical")

    bindings = identity.get("bindings")
    if not isinstance(bindings, Mapping):
        raise F03ContinuousABPreflightError("F03 v2 default bindings are absent")
    spec_binding = ArtifactBinding.from_payload(
        bindings.get("preflight"), role="v2 default preflight"
    )
    amendment_binding = ArtifactBinding.from_payload(
        bindings.get("amendment"), role="v2 default amendment"
    )
    accounting_binding = ArtifactBinding.from_payload(
        bindings.get("continuous_accounting_contract"),
        role="v2 continuous accounting contract",
    )
    state_binding = ArtifactBinding.from_payload(
        bindings.get("continuous_replay_state_contract"),
        role="v2 continuous replay state contract",
    )
    shared_runner_binding = ArtifactBinding.from_payload(
        bindings.get("shared_runner"), role="v2 shared runner"
    )
    f03_runner_binding = ArtifactBinding.from_payload(
        bindings.get("f03_preflight_runner"), role="v2 F03 preflight runner"
    )
    execution_runner_binding = ArtifactBinding.from_payload(
        bindings.get("continuous_execution_runner"),
        role="v2 continuous execution runner",
    )
    fill_trace_source_bindings = {
        name: ArtifactBinding.from_payload(
            bindings.get(identity_name),
            role=f"v2 {identity_name}",
        )
        for name, identity_name in (
            ("python_replay_engine", "python_tick_replay"),
            ("cpp_replay_engine", "cpp_tick_replay"),
            ("cpp_replay_abi", "cpp_tick_replay_abi"),
            ("cpp_python_binding", "cpp_tick_replay_binding"),
        )
    }
    expected_shared_runner = (
        ROOT / "models/replay/restart_aware_continuous_ab.py"
    ).resolve()
    if (
        spec_binding.path != DEFAULT_SPEC.resolve()
        or amendment_binding.path != DEFAULT_AMENDMENT.resolve()
        or shared_runner_binding.path != expected_shared_runner
        or f03_runner_binding.path != Path(__file__).resolve()
        or execution_runner_binding.path != DEFAULT_EXECUTION_RUNNER.resolve()
        or any(
            fill_trace_source_bindings[name].path != path.resolve()
            for name, path in FILL_TRACE_SOURCE_PATHS.items()
        )
    ):
        raise F03ContinuousABPreflightError("F03 v2 default path binding drifted")

    spec = _load_json(spec_binding.path, role="F03 v2 default preflight")
    amendment = _load_json(
        amendment_binding.path,
        role="F03 v2 default amendment",
    )
    if (
        spec.get("schema_version") != SCHEMA_VERSION
        or spec.get("status") != PREFLIGHT_STATUS
        or amendment.get("schema_version") != AMENDMENT_SCHEMA_VERSION
        or amendment.get("status") != AMENDMENT_STATUS
    ):
        raise F03ContinuousABPreflightError("F03 v2 default schema binding drifted")
    if (
        _canonical_document_sha256(
            amendment,
            identity_field="canonical_amendment_sha256",
        )
        != amendment.get("canonical_amendment_sha256")
    ):
        raise F03ContinuousABPreflightError("F03 v2 amendment is not canonical")
    parent = ArtifactBinding.from_payload(
        amendment.get("parent_preflight"), role="v2 amendment parent"
    )
    if parent != spec_binding:
        raise F03ContinuousABPreflightError("F03 v2 amendment parent drifted")
    _validate_fill_trace_ordering_contract(
        spec.get("runner"),
        execution_runner=execution_runner_binding,
        source_bindings=fill_trace_source_bindings,
        role="F03 v2 preflight",
    )
    _validate_fill_trace_ordering_contract(
        amendment.get("execution_interfaces"),
        execution_runner=execution_runner_binding,
        source_bindings=fill_trace_source_bindings,
        role="F03 v2 amendment",
    )
    _validate_arm_contract(spec)

    accounting = _load_json(
        accounting_binding.path,
        role="continuous accounting contract v2",
    )
    replay_state = _load_json(
        state_binding.path,
        role="continuous replay state contract v2",
    )
    if (
        accounting.get("contract_id") != continuous_accounting.SCHEMA_VERSION
        or accounting.get("fee_accounting_semantics")
        != continuous_accounting.FEE_ACCOUNTING_SEMANTICS
        or replay_state.get("contract_id") != replay_state_checkpoint.SCHEMA_VERSION
    ):
        raise F03ContinuousABPreflightError("F03 v2 economic contract drifted")
    interfaces = amendment.get("execution_interfaces")
    if not isinstance(interfaces, Mapping) or (
        interfaces.get("continuous_accounting_contract_id")
        != continuous_accounting.SCHEMA_VERSION
    ):
        raise F03ContinuousABPreflightError("F03 v2 execution interface drifted")
    if any(identity.get("authority", {}).get(name) is not False for name in (
        "economic_results_read",
        "action_authorized",
        "live_authorized",
        "baseline_replacement_authorized",
    )):
        raise F03ContinuousABPreflightError("F03 v2 default grants authority")
    return {
        "identity_path": str(resolved_identity),
        "identity_sha256": sha256_file(resolved_identity),
        "canonical_identity_sha256": expected_canonical,
        "preflight_sha256": spec_binding.sha256,
        "amendment_sha256": amendment_binding.sha256,
        "continuous_accounting_contract_id": continuous_accounting.SCHEMA_VERSION,
        "continuous_replay_state_contract_id": replay_state_checkpoint.SCHEMA_VERSION,
        "fee_accounting_semantics": continuous_accounting.FEE_ACCOUNTING_SEMANTICS,
        "fill_trace_ordering_contract_id": FILL_TRACE_ORDERING_IDENTITY,
    }


def _artifact_payload(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise F03ContinuousABPreflightError(f"source artifact is missing: {resolved}")
    size = resolved.stat().st_size
    if size <= 0:
        raise F03ContinuousABPreflightError(f"source artifact is empty: {resolved}")
    return {"path": str(resolved), "size_bytes": size, "sha256": sha256_file(resolved)}


def _bind_source_artifacts(
    rows: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], ...]:
    bound: list[dict[str, Any]] = []
    artifact_cache: dict[Path, dict[str, Any]] = {}
    for raw in rows:
        row = dict(raw)
        role_paths = {
            "bbo": Path(str(row.get("bbo_path", ""))),
            "l2": Path(str(row.get("l2_path", ""))),
            "feature": Path(str(row.get("feature_path", ""))),
        }
        observed: dict[str, dict[str, Any]] = {}
        for role, path in role_paths.items():
            resolved = path.expanduser().resolve()
            if resolved not in artifact_cache:
                artifact_cache[resolved] = _artifact_payload(resolved)
            observed[role] = dict(artifact_cache[resolved])
        declared = row.get("artifacts")
        if declared is not None and declared != observed:
            raise F03ContinuousABPreflightError(
                f"declared source artifact identity drifted: {row.get('day', '')}"
            )
        row["artifacts"] = observed
        bound.append(row)
    return tuple(bound)


def _validate_permissions(spec: Mapping[str, Any]) -> None:
    permissions = spec.get("permissions")
    if not isinstance(permissions, Mapping):
        raise F03ContinuousABPreflightError("spec lacks permissions")
    required_false = (
        "economic_outcomes_read",
        "execution_results_read",
        "validation_read",
        "sealed_holdout_read",
        "action_authorized",
        "live_authorized",
        "baseline_replacement_authorized",
    )
    if any(permissions.get(name) is not False for name in required_false):
        raise F03ContinuousABPreflightError("preflight must keep every result/authority closed")
    if permissions.get("execution_plan_materialization_authorized") is not True:
        raise F03ContinuousABPreflightError("execution-plan skeleton is not authorized")


def _validate_parent_precommit(
    *,
    spec_path: Path,
    spec: Mapping[str, Any],
    amendment_path: Path,
) -> tuple[dict[str, Any], ExecutionInterfaceBindings]:
    amendment = _load_json(amendment_path, role="F03 v2 execution-binding amendment")
    if amendment.get("schema_version") != AMENDMENT_SCHEMA_VERSION:
        raise F03ContinuousABPreflightError("F03 execution-binding amendment schema mismatch")
    expected_canonical = str(amendment.get("canonical_amendment_sha256", ""))
    if (
        _canonical_document_sha256(amendment, identity_field="canonical_amendment_sha256")
        != expected_canonical
    ):
        raise F03ContinuousABPreflightError("F03 execution-binding amendment is not canonical")
    if amendment.get("status") != AMENDMENT_STATUS:
        raise F03ContinuousABPreflightError("F03 execution-binding amendment status is unsafe")

    frozen_preflight = ArtifactBinding.from_payload(
        amendment.get("parent_preflight"), role="parent preflight"
    )
    if frozen_preflight.path != spec_path or frozen_preflight.sha256 != sha256_file(spec_path):
        raise F03ContinuousABPreflightError("v1.1 amendment does not bind the original preflight")

    legacy = spec.get("parent_precommit")
    drift = amendment.get("parent_precommit_drift")
    if not isinstance(legacy, Mapping) or not isinstance(drift, Mapping):
        raise F03ContinuousABPreflightError("parent precommit drift resolution is absent")
    if str(legacy.get("sha256", "")) != str(
        drift.get("legacy_sha256_recorded_in_parent_preflight", "")
    ):
        raise F03ContinuousABPreflightError("legacy parent precommit hash was rewritten")
    if drift.get("resolution") != "successor_v2_only_v1_documents_unchanged":
        raise F03ContinuousABPreflightError(
            "parent precommit drift resolution is not successor-only"
        )

    current_parent = ArtifactBinding.from_payload(
        drift.get("current_parent_precommit"), role="current parent precommit"
    )
    parent_payload = _load_json(current_parent.path, role="current parent economic precommit")
    parent_canonical = canonical_sha256(parent_payload)
    if parent_canonical != str(drift.get("current_parent_canonical_sha256", "")):
        raise F03ContinuousABPreflightError("parent precommit canonical identity mismatch")
    if parent_payload.get("identity") != "causal_v12_1s_cadence_policy_successor_v1":
        raise F03ContinuousABPreflightError("parent precommit identity changed")
    continuous = parent_payload.get("continuous_confirmation")
    if not isinstance(continuous, Mapping):
        raise F03ContinuousABPreflightError("parent precommit lacks continuous confirmation")
    expected_days = list(ordered_days(START_DAY, END_DAY))
    parent_requirements = (
        continuous.get("calendar_days") == expected_days,
        continuous.get("calendar_day_count") == 71,
        continuous.get("required_active_trading_day_count") == 71,
        continuous.get("same_restart_manifest_both_arms") is True,
        continuous.get("whole_day_placeholders_allowed") is False,
        continuous.get("queue_or_lifecycle_authority_near_non_native_gaps") is False,
        continuous.get("execution_amendment_required_before_pnl_read") is True,
        continuous.get("full_path_runner_bound_at_freeze") is False,
        continuous.get("continuous_execution_authorized_at_freeze") is False,
    )
    if not all(parent_requirements):
        raise F03ContinuousABPreflightError("parent continuous-confirmation semantics drifted")

    interface_payload = amendment.get("execution_interfaces")
    if not isinstance(interface_payload, Mapping):
        raise F03ContinuousABPreflightError("execution interfaces are not bound")
    restart_binding = ArtifactBinding.from_payload(
        interface_payload.get("restart_boundary"), role="restart boundary interface"
    )
    accounting_binding = ArtifactBinding.from_payload(
        interface_payload.get("continuous_accounting"), role="continuous accounting interface"
    )
    if restart_binding.path != Path(restart_boundary.__file__).resolve():
        raise F03ContinuousABPreflightError("restart boundary implementation path mismatch")
    if accounting_binding.path != Path(continuous_accounting.__file__).resolve():
        raise F03ContinuousABPreflightError("continuous accounting implementation path mismatch")
    if interface_payload.get("restart_boundary_contract_id") != restart_boundary.SCHEMA_VERSION:
        raise F03ContinuousABPreflightError("restart boundary contract ID mismatch")
    if (
        interface_payload.get("continuous_accounting_contract_id")
        != continuous_accounting.SCHEMA_VERSION
    ):
        raise F03ContinuousABPreflightError("continuous accounting contract ID mismatch")
    if tuple(interface_payload.get("restart_required_methods", ())) != RESTART_REQUIRED_METHODS:
        raise F03ContinuousABPreflightError("restart boundary method ABI mismatch")
    if (
        tuple(interface_payload.get("accounting_required_methods", ()))
        != ACCOUNTING_REQUIRED_METHODS
    ):
        raise F03ContinuousABPreflightError("continuous accounting method ABI mismatch")
    for method in RESTART_REQUIRED_METHODS:
        if not callable(getattr(RestartBoundaryMachine, method, None)):
            raise F03ContinuousABPreflightError(f"restart boundary method is absent: {method}")
    for method in ACCOUNTING_REQUIRED_METHODS:
        if not callable(getattr(ContinuousAccountingLedger, method, None)):
            raise F03ContinuousABPreflightError(f"continuous accounting method is absent: {method}")
    _probe_execution_interfaces()

    source_contract = amendment.get("source_artifact_manifest")
    if not isinstance(source_contract, Mapping):
        raise F03ContinuousABPreflightError("source artifact manifest contract is absent")
    source_sha = str(source_contract.get("canonical_sha256", ""))
    if len(source_sha) != 64:
        raise F03ContinuousABPreflightError("source artifact manifest SHA256 is not bound")
    if (
        source_contract.get("day_count"),
        source_contract.get("native_day_count"),
        source_contract.get("provider_normalized_sensitivity_day_count"),
        source_contract.get("artifact_count"),
    ) != (71, 52, 19, 213):
        raise F03ContinuousABPreflightError("source artifact denominator contract drifted")
    if (
        source_contract.get("provider_exact_queue_authority") is not False
        or source_contract.get("provider_exact_lifecycle_authority") is not False
        or source_contract.get("exact_authority_excludes_frozen_restart_gaps") is not True
    ):
        raise F03ContinuousABPreflightError("provider source received exact execution authority")
    return amendment, ExecutionInterfaceBindings(
        restart_boundary_sha256=restart_binding.sha256,
        continuous_accounting_sha256=accounting_binding.sha256,
        source_artifact_manifest_sha256=source_sha,
        parent_precommit_file_sha256=current_parent.sha256,
        parent_precommit_canonical_sha256=parent_canonical,
        amendment_sha256=sha256_file(amendment_path),
    )


def _probe_execution_interfaces() -> None:
    state = ContinuousReplayState(
        arm_id="f03-v2-contract-probe",
        checkpoint_ts_ms=0,
        cash_usdc=0.0,
        position_btc=0.0,
        average_entry_price=0.0,
        cumulative_realized_pnl_usdc=0.0,
        cumulative_fees_usdc=0.0,
        equity_anchor_usdc=0.0,
        last_mark_price=60_000.0,
        cumulative_pnl_usdc=0.0,
        feature_warmup_ready=True,
        quoting_enabled=True,
    )
    interval = PlannedRestartInterval(
        gap_id="f03-v2-contract-probe",
        quote_stop_ts_ms=1_000,
        cancel_deadline_ts_ms=1_500,
        offline_start_ts_ms=2_000,
        resume_snapshot_ts_ms=3_000,
    )
    machine = RestartBoundaryMachine()
    machine.register_active_order(client_order_id="probe-order", remaining_quantity_btc=0.001)
    machine.begin_maintenance(interval, now_ts_ms=1_000)
    machine.request_cancel("probe-order", ts_ms=1_100)
    try:
        machine.enter_offline(ts_ms=2_000, state=state)
    except RuntimeError as exc:
        if "failed to terminate" not in str(exc):
            raise F03ContinuousABPreflightError(
                "restart drain failed for an unexpected reason"
            ) from exc
    else:
        raise F03ContinuousABPreflightError("restart boundary accepted an unacknowledged cancel")
    machine.terminal("probe-order", ts_ms=1_200, reason="CANCEL_ACK")
    clean = machine.enter_offline(ts_ms=2_000, state=state)
    warming = machine.begin_restart(
        ts_ms=3_000,
        snapshot_identity="probe-fresh-snapshot",
        state=clean,
    )
    try:
        machine.complete_warmup(
            feature_ready_ts_ms=3_001,
            decision_ts_ms=3_000,
            state=warming,
        )
    except RuntimeError as exc:
        if "later than" not in str(exc):
            raise F03ContinuousABPreflightError(
                "warmup clock failed for an unexpected reason"
            ) from exc
    else:
        raise F03ContinuousABPreflightError("restart boundary accepted future-ready features")
    ready = machine.complete_warmup(
        feature_ready_ts_ms=3_000,
        decision_ts_ms=3_000,
        state=warming,
    )
    ledger = ContinuousAccountingLedger(ready)
    ledger.mark(3_001, 60_001.0)
    if not ready.restart_safe or ledger.state.equity_usdc != 0.0:
        raise F03ContinuousABPreflightError("continuous interface contract probe failed")


def _validate_control(spec: Mapping[str, Any]) -> tuple[str, str, str]:
    control = spec.get("control")
    if not isinstance(control, Mapping):
        raise F03ContinuousABPreflightError("spec lacks the v9 control binding")
    binding = load_operational_baseline_binding()
    if binding is None:
        raise F03ContinuousABPreflightError("current operational baseline binding is missing")
    pointer = binding["pointer"]
    identity = binding["identity"]
    baseline_id = str(control.get("baseline_id", ""))
    if baseline_id != one_second_replay.EXPECTED_BASELINE_ID:
        raise F03ContinuousABPreflightError("spec does not name the current v9 control")
    if pointer.get("baseline_id") != baseline_id or identity.get("baseline_id") != baseline_id:
        raise F03ContinuousABPreflightError("operational baseline pointer drifted from v9")
    expected_identity = str(control.get("identity_sha256", ""))
    expected_config = str(control.get("config_sha256", ""))
    if str(binding["identity_sha256"]) != expected_identity:
        raise F03ContinuousABPreflightError("v9 control identity SHA256 mismatch")
    if str(pointer.get("live_config_sha256", "")) != expected_config:
        raise F03ContinuousABPreflightError("v9 control config SHA256 mismatch")
    if bool(pointer.get("dynamic_fill_hazard_action_enabled")):
        raise F03ContinuousABPreflightError("q90 action must be OFF in the control")
    if bool(pointer.get("buy_fill_selection_live_enabled")):
        raise F03ContinuousABPreflightError("BUY fill-selection must be OFF in the control")
    return baseline_id, expected_identity, expected_config


def _validate_overlay_index(
    binding: ArtifactBinding,
    *,
    overlay_root: Path,
    expected_days: Sequence[str],
    expected_bundle_sha256: str,
    verify_daily_overlays: bool,
) -> str:
    payload = _load_json(binding.path, role="candidate overlay index")
    if payload.get("schema_version") != "causal_v12_1s_prediction_overlay_index.v1":
        raise F03ContinuousABPreflightError("candidate overlay index schema mismatch")
    identity = str(payload.get("identity", ""))
    if not identity:
        raise F03ContinuousABPreflightError("candidate overlay index identity is empty")
    if payload.get("calendar_days") != list(expected_days):
        raise F03ContinuousABPreflightError("candidate overlays do not cover exact 71 days")
    if payload.get("research_bundle_sha256") != expected_bundle_sha256:
        raise F03ContinuousABPreflightError("overlay index and candidate bundle differ")
    rows = payload.get("overlays")
    if not isinstance(rows, list) or len(rows) != len(expected_days):
        raise F03ContinuousABPreflightError("candidate overlay row denominator is not 71")
    observed_days = [str(row.get("day", "")) for row in rows if isinstance(row, Mapping)]
    if observed_days != list(expected_days):
        raise F03ContinuousABPreflightError("candidate overlay rows are not chronological")
    for row in rows:
        relative = str(row.get("directory", ""))
        expected_manifest_sha = str(row.get("manifest_sha256", ""))
        if not relative or Path(relative).is_absolute() or ".." in Path(relative).parts:
            raise F03ContinuousABPreflightError("overlay index contains an unsafe directory")
        if len(expected_manifest_sha) != 64:
            raise F03ContinuousABPreflightError("overlay index lacks a daily manifest hash")
        if verify_daily_overlays:
            directory = (overlay_root / relative).resolve()
            try:
                directory.relative_to(overlay_root)
            except ValueError as exc:
                raise F03ContinuousABPreflightError("overlay directory escapes its root") from exc
            schedule = one_second_replay.load_admitted_one_second_overlay(directory)
            if schedule.utc_day != row["day"]:
                raise F03ContinuousABPreflightError("daily overlay owns the wrong UTC day")
            if schedule.manifest_sha256 != expected_manifest_sha:
                raise F03ContinuousABPreflightError("daily overlay manifest SHA256 mismatch")
            if schedule.research_bundle_sha256 != expected_bundle_sha256:
                raise F03ContinuousABPreflightError("daily overlay bundle identity drifted")
    return identity


def _validate_candidate(
    spec: Mapping[str, Any],
    *,
    amendment: Mapping[str, Any],
    expected_days: Sequence[str],
    verify_daily_overlays: bool,
) -> CandidateArtifacts:
    candidate = spec.get("candidate")
    if not isinstance(candidate, Mapping):
        raise F03ContinuousABPreflightError("candidate bundle is not bound")
    identity = candidate.get("identity")
    if not isinstance(identity, str) or not identity.strip():
        raise F03ContinuousABPreflightError("candidate identity is not bound")
    bundle = ArtifactBinding.from_payload(candidate.get("bundle_meta"), role="bundle meta")
    bundle_payload = _load_json(bundle.path, role="candidate bundle meta")
    if bundle_payload.get("schema_version") != one_second_replay.training.BUNDLE_SCHEMA_VERSION:
        raise F03ContinuousABPreflightError("candidate bundle schema mismatch")
    if bundle_payload.get("identity") != one_second_replay.schema.IDENTITY:
        raise F03ContinuousABPreflightError("candidate bundle identity mismatch")
    heads = bundle_payload.get("heads")
    if (
        bundle_payload.get("head_count") != len(one_second_replay.training.HEAD_SPECS)
        or not isinstance(heads, Mapping)
        or tuple(heads) != tuple(one_second_replay.training.HEAD_SPECS)
    ):
        raise F03ContinuousABPreflightError("candidate bundle does not bind all 13 heads")
    if bundle_payload.get("atomic_admission") is not True:
        raise F03ContinuousABPreflightError("candidate bundle is not atomically admitted")
    if any(
        bundle_payload.get(field) is not False
        for field in (
            "prediction_outcomes_read",
            "economic_outcomes_read",
            "prediction_authority",
            "action_authority",
            "live_authority",
        )
    ):
        raise F03ContinuousABPreflightError("candidate bundle permissions are contaminated")
    training_identity = bundle_payload.get("training_identity")
    if not isinstance(training_identity, Mapping):
        raise F03ContinuousABPreflightError("candidate bundle lacks its training identity")
    if (
        training_identity.get("inference_cadence_ms") != one_second_schema.CADENCE_MS
        or training_identity.get("feature_order_sha256") != one_second_schema.feature_order_sha256()
        or tuple(training_identity.get("heads", ())) != tuple(one_second_replay.training.HEAD_SPECS)
    ):
        raise F03ContinuousABPreflightError("candidate bundle and 1s schema differ")
    success_path = bundle.path.parent / "_SUCCESS"
    if (
        not success_path.is_file()
        or success_path.read_text(encoding="ascii").strip() != bundle.sha256
    ):
        raise F03ContinuousABPreflightError("candidate bundle _SUCCESS binding is invalid")
    dag = ArtifactBinding.from_payload(candidate.get("feature_dag"), role="Feature DAG")
    dag_payload = _load_json(dag.path, role="candidate Feature DAG")
    expected_dag_payload = one_second_full_schema.full_feature_contract_payload()
    dag_contract = amendment.get("candidate_dag_contract")
    if not isinstance(dag_contract, Mapping):
        raise F03ContinuousABPreflightError("candidate DAG contract is not bound")
    if dag_payload != expected_dag_payload:
        raise F03ContinuousABPreflightError("candidate Feature DAG semantics drifted")
    if (
        dag_payload.get("identity") != one_second_schema.IDENTITY
        or dag_payload.get("feature_dag_id") != one_second_schema.FEATURE_DAG_ID
        or dag_payload.get("cadence_ms") != 1_000
        or dag_payload.get("feature_order_sha256") != training_identity.get("feature_order_sha256")
        or dag_payload.get("head_linkage_sha256")
        != one_second_schema.canonical_sha256(one_second_schema.head_linkage_payload())
        or dag_payload.get("source_manifest_sha256")
        != one_second_schema.canonical_sha256(one_second_schema.source_manifest_payload())
    ):
        raise F03ContinuousABPreflightError("candidate Feature DAG clock/schema contract failed")
    if (
        dag_contract.get("semantic_identity") != one_second_schema.IDENTITY
        or dag_contract.get("feature_dag_id") != one_second_schema.FEATURE_DAG_ID
        or dag_contract.get("cadence_ms") != 1_000
        or dag_contract.get("canonical_payload_sha256")
        != one_second_schema.canonical_sha256(expected_dag_payload)
        or dag_contract.get("bundle_relation")
        != "training_identity_feature_order_heads_and_cadence_must_match"
    ):
        raise F03ContinuousABPreflightError("v2 amendment candidate DAG binding drifted")
    index = ArtifactBinding.from_payload(candidate.get("overlay_index"), role="overlay index")
    raw_root = candidate.get("overlay_root")
    if not isinstance(raw_root, str) or not raw_root.strip():
        raise F03ContinuousABPreflightError("candidate overlay root is not bound")
    overlay_root = Path(raw_root).expanduser()
    if not overlay_root.is_absolute():
        overlay_root = ROOT / overlay_root
    overlay_root = overlay_root.resolve()
    if not overlay_root.is_dir():
        raise F03ContinuousABPreflightError("candidate overlay root is missing")
    index_identity = _validate_overlay_index(
        index,
        overlay_root=overlay_root,
        expected_days=expected_days,
        expected_bundle_sha256=bundle.sha256,
        verify_daily_overlays=verify_daily_overlays,
    )
    return CandidateArtifacts(
        bundle_meta=bundle,
        feature_dag=dag,
        overlay_index=index,
        overlay_root=overlay_root,
        bundle_identity=identity,
        overlay_index_identity=index_identity,
    )


def _validate_arm_contract(spec: Mapping[str, Any]) -> None:
    comparison = spec.get("comparison")
    if not isinstance(comparison, Mapping):
        raise F03ContinuousABPreflightError("spec lacks paired comparison semantics")
    required_true = (
        "all_71_days_trade",
        "same_restart_manifest_both_arms",
        "same_market_timeline_both_arms",
        "separate_mutable_state_both_arms",
        "cash_inventory_entry_and_campaign_cross_midnight",
        "utc_midnight_accounting_only",
        "gap_clears_orders_queue_pending_and_hazard",
        "gap_preserves_economic_state",
        "past_only_warmup_before_resume",
        "control_is_v9_10s",
        "candidate_is_f03_1s",
    )
    if any(comparison.get(name) is not True for name in required_true):
        raise F03ContinuousABPreflightError("paired continuous-state contract is incomplete")
    invariant_false = (
        "utc_midnight_flatten",
        "utc_midnight_state_reset",
        "whole_day_maintenance_placeholder",
        "shared_mutable_arm_state",
        "economic_results_aggregated",
    )
    if any(comparison.get(name) is not False for name in invariant_false):
        raise F03ContinuousABPreflightError("paired continuous-state contract is unsafe")
    accounting = comparison.get("continuous_accounting_contract")
    if not isinstance(accounting, Mapping):
        raise F03ContinuousABPreflightError("continuous accounting contract is not bound")
    contract = ArtifactBinding.from_payload(
        accounting.get("artifact"), role="continuous accounting contract"
    )
    implementation = ArtifactBinding.from_payload(
        accounting.get("implementation"), role="continuous accounting implementation"
    )
    payload = _load_json(contract.path, role="continuous accounting contract")
    if payload.get("contract_id") != continuous_accounting.SCHEMA_VERSION:
        raise F03ContinuousABPreflightError("continuous accounting contract ID mismatch")
    if payload.get("implementation", {}).get("sha256") != implementation.sha256:
        raise F03ContinuousABPreflightError(
            "continuous accounting contract and implementation differ"
        )


def validate_preflight(
    spec_path: Path = DEFAULT_SPEC,
    *,
    amendment_path: Path = DEFAULT_AMENDMENT,
    source_rows: Sequence[Mapping[str, Any]] | None = None,
    verify_daily_overlays: bool = True,
) -> F03ContinuousABPreflight:
    path = _resolve_spec_path(spec_path)
    resolved_amendment = _resolve_spec_path(amendment_path)
    if (
        path == DEFAULT_SPEC.resolve()
        and resolved_amendment == DEFAULT_AMENDMENT.resolve()
    ):
        validate_default_binding_documents()
    spec = _load_json(path, role="F03 continuous A/B preflight spec")
    if spec.get("schema_version") != SCHEMA_VERSION:
        raise F03ContinuousABPreflightError("F03 continuous A/B schema mismatch")
    if spec.get("status") != PREFLIGHT_STATUS:
        raise F03ContinuousABPreflightError("F03 continuous A/B status is not fail-closed")
    _validate_permissions(spec)
    _validate_arm_contract(spec)
    amendment, interfaces = _validate_parent_precommit(
        spec_path=path,
        spec=spec,
        amendment_path=resolved_amendment,
    )
    calendar = spec.get("calendar")
    if not isinstance(calendar, Mapping):
        raise F03ContinuousABPreflightError("spec lacks calendar binding")
    if calendar.get("start_day") != START_DAY or calendar.get("end_day") != END_DAY:
        raise F03ContinuousABPreflightError("spec does not bind the frozen 71-day range")
    expected_days = ordered_days(START_DAY, END_DAY)
    if calendar.get("days") != list(expected_days):
        raise F03ContinuousABPreflightError("spec day denominator is not exact")

    candidate = _validate_candidate(
        spec,
        amendment=amendment,
        expected_days=expected_days,
        verify_daily_overlays=verify_daily_overlays,
    )
    baseline_id, identity_sha, config_sha = _validate_control(spec)
    if source_rows is None:
        from scripts import run_full_calendar_71d_baseline as full_calendar

        source_rows = full_calendar.preflight(list(expected_days))
    source_rows = _bind_source_artifacts(source_rows)
    plan = build_complete_calendar_plan(
        calendar_manifest_path=CALENDAR_MANIFEST,
        source_rows=source_rows,
        start_day=START_DAY,
        end_day=END_DAY,
    )
    expected_calendar_sha = str(calendar.get("source_manifest_sha256", ""))
    expected_timeline_sha = str(calendar.get("restart_timeline_sha256", ""))
    if plan.source_calendar_manifest_sha256 != expected_calendar_sha:
        raise F03ContinuousABPreflightError("calendar source manifest SHA256 mismatch")
    if plan.restart_timeline_sha256 != expected_timeline_sha:
        raise F03ContinuousABPreflightError("frozen restart timeline SHA256 mismatch")
    if plan.source_artifact_manifest_sha256 != interfaces.source_artifact_manifest_sha256:
        raise F03ContinuousABPreflightError("exact 71-day source artifact manifest drifted")
    source_payload = source_artifact_manifest_payload(plan.source_bindings)
    if (
        source_payload["day_count"],
        source_payload["native_day_count"],
        source_payload["provider_normalized_sensitivity_day_count"],
        source_payload["artifact_count"],
    ) != (71, 52, 19, 213):
        raise F03ContinuousABPreflightError("source artifact manifest denominator mismatch")
    requests = paired_execution_requests(
        plan,
        control_policy="current_v9_causal_v12_10s_ml_on",
        candidate_policy=candidate.bundle_identity,
    )
    for request in requests:
        if request.source.book_identity == "provider_normalized_sensitivity" and (
            request.exact_queue_authority or request.exact_lifecycle_authority
        ):
            raise F03ContinuousABPreflightError("provider request received exact queue authority")
        if not (
            request.cancel_drain_requires_terminal_ack_or_fill
            and request.warmup_requires_source_coverage
            and request.feature_ready_not_after_decision
            and request.exact_authority_excludes_frozen_restart_gaps
            and request.continuous_economic_sensitivity_authority
            and request.execution_plan_skeleton
            and not request.full_path_executed
        ):
            raise F03ContinuousABPreflightError("execution request omitted fail-closed invariants")
        if (
            request.restart_boundary_contract_id != restart_boundary.SCHEMA_VERSION
            or request.continuous_accounting_contract_id != continuous_accounting.SCHEMA_VERSION
        ):
            raise F03ContinuousABPreflightError("execution request interface identity mismatch")
    return F03ContinuousABPreflight(
        spec_path=path,
        spec_sha256=sha256_file(path),
        plan=plan,
        candidate=candidate,
        requests=requests,
        control_baseline_id=baseline_id,
        control_identity_sha256=identity_sha,
        control_config_sha256=config_sha,
        interfaces=interfaces,
    )


class F03ContinuousABRunnerSkeleton:
    """Prepare paired requests without calling the engine or consuming results."""

    def __init__(self, preflight: F03ContinuousABPreflight):
        if preflight.outcome_reads_enabled or preflight.tick_engine_called:
            raise F03ContinuousABPreflightError("runner received a contaminated preflight")
        self._preflight = preflight

    @property
    def requests(self) -> tuple[PairedExecutionRequest, ...]:
        return self._preflight.requests

    def execution_plan_payload(self) -> dict[str, Any]:
        payload = self._preflight.machine_payload()
        payload["requests"] = [asdict(row) for row in self.requests]
        payload["request_payload_sha256"] = canonical_sha256(payload["requests"])
        payload["economic_results_schema"] = None
        payload["economic_result_reader"] = None
        return payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC)
    parser.add_argument("--amendment", type=Path, default=DEFAULT_AMENDMENT)
    parser.add_argument(
        "--validate-only",
        action="store_true",
        default=True,
        help="validate and print the outcome-blind execution plan",
    )
    args = parser.parse_args()
    preflight = validate_preflight(args.spec, amendment_path=args.amendment)
    skeleton = F03ContinuousABRunnerSkeleton(preflight)
    print(json.dumps(skeleton.execution_plan_payload(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
