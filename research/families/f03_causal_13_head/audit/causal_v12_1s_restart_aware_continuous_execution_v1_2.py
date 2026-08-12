#!/usr/bin/env python3
"""Outcome-blind preparation for the F03 71-day continuous A/B executor.

The successor binds the real continuous operation tape and durable execution
protocol.  It cannot run until exact 71-day control and candidate policy
artifact manifests are supplied.
"""

from __future__ import annotations

import argparse
import json
import shutil
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from data_paths import external_cache_root
from models.replay import restart_aware_continuous_execution as execution
from models.replay.restart_aware_continuous_ab import (
    ContinuousABPlan,
    build_complete_calendar_plan,
    canonical_sha256,
    ordered_days,
    sha256_file,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_restart_aware_continuous_ab as parent,
)

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = "causal_v12_1s_restart_aware_continuous_execution.v1.2"
PLAN_SCHEMA_VERSION = f"{SCHEMA_VERSION}.plan"
POLICY_ARTIFACT_SCHEMA_VERSION = f"{SCHEMA_VERSION}.policy_artifacts"
IDENTITY = "causal_v12_1s_v9_10s_vs_1s_restart_aware_71d_execution_v1_2"
DEFAULT_CONTRACT = ROOT / (
    "research/families/f03_causal_13_head/docs/"
    "causal_v12_1s_restart_aware_continuous_execution_v1_2_contract_20260805.json"
)
DEFAULT_OUTPUT_ROOT = (
    external_cache_root(ROOT)
    / "replay_dag/f03_causal_v12_1s_restart_aware_continuous_execution_v1_2"
)
PLAN_FILENAME = "execution-plan.json"
PLAN_SUCCESS = "_PLAN_SUCCESS"
EXPECTED_DAYS = ordered_days(parent.START_DAY, parent.END_DAY)
ARMS = ("control", "candidate")
BASE_SEED = 312_071
STORAGE_RESERVE_BYTES = 60 * (1 << 30)
ESTIMATED_OUTPUT_BYTES = 6 * (1 << 30)


class F03ContinuousExecutionError(execution.ContinuousExecutionError):
    """Raised while the execution-only successor is fail closed."""


def _load_json(path: Path, *, role: str) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise F03ContinuousExecutionError(f"missing {role}: {resolved}")
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise F03ContinuousExecutionError(f"invalid {role}: {resolved}") from exc
    if not isinstance(payload, dict):
        raise F03ContinuousExecutionError(f"{role} must be a JSON object")
    return payload


def _artifact(row: Any, *, role: str, verify_hash: bool = True) -> dict[str, Any]:
    if not isinstance(row, Mapping):
        raise F03ContinuousExecutionError(f"{role} artifact binding is missing")
    path = Path(str(row.get("path", ""))).expanduser().resolve()
    expected = str(row.get("sha256", ""))
    if not path.is_file() or len(expected) != 64:
        raise F03ContinuousExecutionError(f"{role} artifact is incomplete: {path}")
    size = path.stat().st_size
    if int(row.get("size_bytes", size)) != size:
        raise F03ContinuousExecutionError(f"{role} artifact size drift")
    if verify_hash and sha256_file(path) != expected:
        raise F03ContinuousExecutionError(f"{role} artifact SHA256 drift")
    return {"path": str(path), "sha256": expected, "size_bytes": size}


@dataclass(frozen=True, slots=True)
class PolicyArtifactSet:
    arm: str
    identity: str
    cadence_ms: int
    manifest_path: Path
    manifest_sha256: str
    bundle_meta: Mapping[str, Any]
    feature_dag: Mapping[str, Any]
    operational_config: Mapping[str, Any]
    baseline_identity: Mapping[str, Any]
    initial_state: Mapping[str, Any]
    latency_profile: Mapping[str, Any]
    engine_state_schema: Mapping[str, Any]
    primary_native_40day_receipt: Mapping[str, Any] | None
    overlay_indices: tuple[Mapping[str, Any], ...]
    days: Mapping[str, Mapping[str, Any]]
    ml_enabled: bool
    q90_action_enabled: bool
    buy_fill_selection_enabled: bool
    execution_abi: str

    def payload(self) -> dict[str, Any]:
        return {
            "arm": self.arm,
            "identity": self.identity,
            "cadence_ms": self.cadence_ms,
            "manifest": {
                "path": str(self.manifest_path),
                "sha256": self.manifest_sha256,
                "size_bytes": self.manifest_path.stat().st_size,
            },
            "bundle_meta": dict(self.bundle_meta),
            "feature_dag": dict(self.feature_dag),
            "operational_config": dict(self.operational_config),
            "baseline_identity": dict(self.baseline_identity),
            "initial_state": dict(self.initial_state),
            "latency_profile": dict(self.latency_profile),
            "engine_state_schema": dict(self.engine_state_schema),
            "primary_native_40day_receipt": (
                dict(self.primary_native_40day_receipt)
                if self.primary_native_40day_receipt is not None
                else None
            ),
            "overlay_indices": [dict(row) for row in self.overlay_indices],
            "days": {day: dict(row) for day, row in self.days.items()},
            "ml_enabled": self.ml_enabled,
            "q90_action_enabled": self.q90_action_enabled,
            "buy_fill_selection_enabled": self.buy_fill_selection_enabled,
            "execution_abi": self.execution_abi,
        }


def load_policy_artifacts(path: Path, *, expected_arm: str) -> PolicyArtifactSet:
    resolved = path.expanduser().resolve()
    payload = _load_json(resolved, role=f"{expected_arm} policy artifact manifest")
    if payload.get("schema_version") != POLICY_ARTIFACT_SCHEMA_VERSION:
        raise F03ContinuousExecutionError(f"{expected_arm} policy artifact schema mismatch")
    if payload.get("arm") != expected_arm:
        raise F03ContinuousExecutionError(f"{expected_arm} policy artifact arm mismatch")
    cadence = int(payload.get("cadence_ms", -1))
    expected_cadence = 10_000 if expected_arm == "control" else 1_000
    if cadence != expected_cadence:
        raise F03ContinuousExecutionError(f"{expected_arm} cadence is not frozen")
    identity = str(payload.get("identity", "")).strip()
    if not identity:
        raise F03ContinuousExecutionError(f"{expected_arm} policy identity is empty")
    if (
        payload.get("ml_enabled") is not True
        or payload.get("q90_action_enabled") is not False
        or payload.get("buy_fill_selection_enabled") is not False
    ):
        raise F03ContinuousExecutionError(
            f"{expected_arm} is not the frozen ML-ON/q90-OFF/selector-OFF policy"
        )
    if payload.get("baseline_id") != parent.one_second_replay.EXPECTED_BASELINE_ID:
        raise F03ContinuousExecutionError(f"{expected_arm} artifact is not based on current v9")
    bundle = _artifact(payload.get("bundle_meta"), role=f"{expected_arm} bundle meta")
    dag = _artifact(payload.get("feature_dag"), role=f"{expected_arm} Feature DAG")
    operational_config = _artifact(
        payload.get("operational_config"), role=f"{expected_arm} operational config"
    )
    baseline_identity = _artifact(
        payload.get("baseline_identity"), role=f"{expected_arm} baseline identity"
    )
    initial_state = _artifact(payload.get("initial_state"), role=f"{expected_arm} initial state")
    latency_profile = _artifact(
        payload.get("latency_profile"), role=f"{expected_arm} latency profile"
    )
    engine_state_schema = _artifact(
        payload.get("engine_state_schema"), role=f"{expected_arm} engine-state schema"
    )
    execution_abi = str(payload.get("execution_abi", "")).strip()
    if not execution_abi:
        raise F03ContinuousExecutionError(f"{expected_arm} execution ABI is empty")
    primary_receipt = None
    if expected_arm == "candidate":
        primary_receipt = _artifact(
            payload.get("primary_native_40day_receipt"),
            role="candidate 40-day exact-native primary receipt",
        )
    raw_indices = payload.get("overlay_indices")
    if not isinstance(raw_indices, list) or not raw_indices:
        raise F03ContinuousExecutionError(f"{expected_arm} overlay index list is empty")
    indices = tuple(
        _artifact(row, role=f"{expected_arm} overlay index {index}")
        for index, row in enumerate(raw_indices, start=1)
    )
    raw_days = payload.get("days")
    if not isinstance(raw_days, Mapping) or tuple(raw_days) != EXPECTED_DAYS:
        raise F03ContinuousExecutionError(
            f"{expected_arm} overlays do not cover the exact chronological 71 days"
        )
    days: dict[str, Mapping[str, Any]] = {}
    for day in EXPECTED_DAYS:
        row = raw_days[day]
        if not isinstance(row, Mapping):
            raise F03ContinuousExecutionError(f"{expected_arm} {day} overlay row is invalid")
        manifest = _artifact(
            row.get("overlay_manifest"), role=f"{expected_arm} {day} overlay manifest"
        )
        data = _artifact(row.get("overlay_data"), role=f"{expected_arm} {day} overlay data")
        source_profile = str(row.get("source_profile", ""))
        expected_profile = (
            "native" if day in set(payload.get("native_days", ())) else "provider_normalized"
        )
        if source_profile != expected_profile:
            raise F03ContinuousExecutionError(
                f"{expected_arm} {day} overlay source profile is not explicit"
            )
        identity_sha = str(row.get("overlay_identity_sha256", ""))
        if len(identity_sha) != 64:
            raise F03ContinuousExecutionError(f"{expected_arm} {day} overlay identity is absent")
        days[day] = {
            "overlay_manifest": manifest,
            "overlay_data": data,
            "overlay_identity_sha256": identity_sha,
            "source_profile": source_profile,
        }
    native_days = tuple(str(day) for day in payload.get("native_days", ()))
    provider_days = tuple(str(day) for day in payload.get("provider_days", ()))
    if tuple(sorted((*native_days, *provider_days))) != tuple(sorted(EXPECTED_DAYS)):
        raise F03ContinuousExecutionError(f"{expected_arm} source strata do not cover 71 days")
    if set(native_days) & set(provider_days):
        raise F03ContinuousExecutionError(f"{expected_arm} source strata overlap")
    return PolicyArtifactSet(
        arm=expected_arm,
        identity=identity,
        cadence_ms=cadence,
        manifest_path=resolved,
        manifest_sha256=sha256_file(resolved),
        bundle_meta=bundle,
        feature_dag=dag,
        operational_config=operational_config,
        baseline_identity=baseline_identity,
        initial_state=initial_state,
        latency_profile=latency_profile,
        engine_state_schema=engine_state_schema,
        primary_native_40day_receipt=primary_receipt,
        overlay_indices=indices,
        days=days,
        ml_enabled=True,
        q90_action_enabled=False,
        buy_fill_selection_enabled=False,
        execution_abi=execution_abi,
    )


def _storage_gate(output_root: Path, *, allow_test_root: bool) -> dict[str, Any]:
    resolved = output_root.expanduser().resolve()
    required_root = external_cache_root(ROOT).resolve()
    if not allow_test_root:
        try:
            resolved.relative_to(required_root)
        except ValueError as exc:
            raise F03ContinuousExecutionError(
                "71-day execution output must stay on the configured external cache"
            ) from exc
    probe = resolved if resolved.exists() else resolved.parent
    while not probe.exists():
        probe = probe.parent
    free = shutil.disk_usage(probe).free
    required = STORAGE_RESERVE_BYTES + int(2.5 * ESTIMATED_OUTPUT_BYTES)
    if not allow_test_root and free < required:
        raise F03ContinuousExecutionError(
            f"external-cache storage gate failed: free={free}, required={required}"
        )
    return {"path": str(resolved), "free_bytes": free, "required_bytes": required, "passed": True}


def _build_frozen_plan(
    *,
    source_rows: Sequence[Mapping[str, Any]] | None,
) -> tuple[ContinuousABPlan, dict[str, Any], dict[str, Any]]:
    spec = _load_json(parent.DEFAULT_SPEC, role="parent 71-day preflight")
    amendment = _load_json(parent.DEFAULT_AMENDMENT, role="parent v1.1 amendment")
    if spec.get("schema_version") != parent.SCHEMA_VERSION or spec.get("calendar", {}).get(
        "days"
    ) != list(EXPECTED_DAYS):
        raise F03ContinuousExecutionError("parent 71-day denominator drifted")
    if amendment.get("schema_version") != parent.AMENDMENT_SCHEMA_VERSION:
        raise F03ContinuousExecutionError("parent execution amendment drifted")
    if source_rows is None:
        from scripts import run_full_calendar_71d_baseline as full_calendar

        source_rows = full_calendar.preflight(list(EXPECTED_DAYS))
    bound_rows = parent._bind_source_artifacts(source_rows)
    plan = build_complete_calendar_plan(
        calendar_manifest_path=parent.CALENDAR_MANIFEST,
        source_rows=bound_rows,
        start_day=parent.START_DAY,
        end_day=parent.END_DAY,
    )
    source_contract = amendment.get("source_artifact_manifest") or {}
    if plan.source_artifact_manifest_sha256 != source_contract.get("canonical_sha256"):
        raise F03ContinuousExecutionError("frozen 213-source identity drifted")
    calendar = spec.get("calendar") or {}
    if (
        plan.source_calendar_manifest_sha256 != calendar.get("source_manifest_sha256")
        or plan.restart_timeline_sha256 != calendar.get("restart_timeline_sha256")
        or len(plan.restart_intervals) != int(calendar.get("restart_interval_count", -1))
    ):
        raise F03ContinuousExecutionError("frozen calendar/restart identity drifted")
    return plan, spec, amendment


def _validate_contract() -> dict[str, Any]:
    contract = _load_json(DEFAULT_CONTRACT, role="v1.2 execution contract")
    if (
        contract.get("schema_version") != f"{SCHEMA_VERSION}.contract"
        or contract.get("identity") != IDENTITY
        or contract.get("status")
        != "execution_layer_implemented_policy_artifacts_unbound_outcomes_closed"
    ):
        raise F03ContinuousExecutionError("v1.2 execution contract identity drifted")
    parent_contract = contract.get("parent") or {}
    for role, path, key in (
        ("parent preflight", parent.DEFAULT_SPEC, "preflight"),
        ("parent amendment", parent.DEFAULT_AMENDMENT, "execution_binding_amendment"),
    ):
        row = parent_contract.get(key) or {}
        if Path(str(row.get("path", ""))).expanduser().resolve() != path.resolve() or row.get(
            "sha256"
        ) != sha256_file(path.resolve()):
            raise F03ContinuousExecutionError(f"{role} binding drifted from v1.2 contract")
    permissions = contract.get("current_permissions") or {}
    if (
        permissions.get("outcome_blind_prepare_authorized") is not True
        or permissions.get("outcome_blind_validate_authorized") is not True
        or any(
            permissions.get(field) is not False
            for field in (
                "full_execution_authorized",
                "economic_outcomes_read",
                "economic_results_aggregated",
                "promotion_authorized",
                "action_authorized",
                "live_authorized",
                "baseline_replacement_authorized",
            )
        )
    ):
        raise F03ContinuousExecutionError("v1.2 contract permissions are not fail closed")
    return contract


def _runtime_artifacts() -> dict[str, Any]:
    paths = {
        "continuous_plan": Path(parent.__file__),
        "shared_executor": Path(execution.__file__),
        "f03_executor": Path(__file__),
        "execution_contract": DEFAULT_CONTRACT,
    }
    return {
        name: {
            "path": str(path.resolve()),
            "sha256": sha256_file(path.resolve()),
            "size_bytes": path.stat().st_size,
        }
        for name, path in paths.items()
    }


def prepare_execution_plan(
    *,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    control_artifacts_path: Path | None = None,
    candidate_artifacts_path: Path | None = None,
    source_rows: Sequence[Mapping[str, Any]] | None = None,
    allow_test_root: bool = False,
) -> dict[str, Any]:
    """Publish an outcome-blind plan; missing policy artifacts remain blockers."""

    storage = _storage_gate(output_root, allow_test_root=allow_test_root)
    contract = _validate_contract()
    plan, parent_spec, parent_amendment = _build_frozen_plan(source_rows=source_rows)
    operations = execution.build_continuous_operations(plan, base_seed=BASE_SEED)
    policy_sets: dict[str, PolicyArtifactSet] = {}
    blockers: list[str] = []
    for arm, artifact_path in (
        ("control", control_artifacts_path),
        ("candidate", candidate_artifacts_path),
    ):
        if artifact_path is None:
            blockers.append(f"{arm}_71_day_policy_artifacts_unbound")
        else:
            policy_sets[arm] = load_policy_artifacts(artifact_path, expected_arm=arm)
    if set(policy_sets) == set(ARMS):
        control_payload = policy_sets["control"].payload()
        candidate_payload = policy_sets["candidate"].payload()
        if policy_sets["control"].execution_abi != policy_sets["candidate"].execution_abi:
            raise F03ContinuousExecutionError("paired arms use different execution ABIs")
        for field in (
            "operational_config",
            "baseline_identity",
            "initial_state",
            "latency_profile",
            "engine_state_schema",
        ):
            if control_payload[field]["sha256"] != candidate_payload[field]["sha256"]:
                raise F03ContinuousExecutionError(
                    f"paired arms do not share the frozen {field} identity"
                )
        control_days = policy_sets["control"].days
        candidate_days = policy_sets["candidate"].days
        for source in plan.source_bindings:
            expected_profile = (
                "native" if source.book_identity == "native_available" else "provider_normalized"
            )
            if (
                control_days[source.day]["source_profile"] != expected_profile
                or candidate_days[source.day]["source_profile"] != expected_profile
            ):
                raise F03ContinuousExecutionError(
                    f"overlay/source authority mismatch on {source.day}"
                )
    identity_payload = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "identity": IDENTITY,
        "parent": {
            "preflight": {
                "path": str(parent.DEFAULT_SPEC.resolve()),
                "sha256": sha256_file(parent.DEFAULT_SPEC.resolve()),
                "identity": parent_spec.get("identity"),
            },
            "amendment": {
                "path": str(parent.DEFAULT_AMENDMENT.resolve()),
                "sha256": sha256_file(parent.DEFAULT_AMENDMENT.resolve()),
                "identity": parent_amendment.get("identity"),
            },
        },
        "contract": {
            "path": str(DEFAULT_CONTRACT.resolve()),
            "sha256": sha256_file(DEFAULT_CONTRACT.resolve()),
            "identity": contract["identity"],
        },
        "continuous_plan": plan.identity_payload(),
        "operations": [asdict(row) for row in operations],
        "operation_count": len(operations),
        "operation_tape_sha256": canonical_sha256([asdict(row) for row in operations]),
        "policy_artifacts": {
            arm: policy_sets[arm].payload() if arm in policy_sets else None for arm in ARMS
        },
        "comparison": {
            "control": "current_v9_10s_ml_on_q90_off_buy_selector_off",
            "candidate": "true_1s_ml_on_q90_off_buy_selector_off",
            "same_restart_schedule": True,
            "same_market_clock": True,
            "same_latency_random_path": True,
            "arm_mutable_state_isolated": True,
            "utc_day_is_cluster_and_accounting_slice_only": True,
            "utc_midnight_state_reset": False,
            "daily_forced_flat": False,
            "gap_trading_allowed": False,
            "panel_terminal_inventory_mtm_once": True,
        },
        "authority": {
            "native_exact_queue_and_lifecycle_only": True,
            "provider_continuous_pnl_inventory_campaign_sensitivity_only": True,
            "provider_exact_queue_authority": False,
            "provider_exact_lifecycle_authority": False,
            "provider_q90_authority": False,
            "gap_exact_authority": False,
            "primary_40_day_exact_native_substitute": False,
        },
        "runtime_artifacts": _runtime_artifacts(),
        "output_root": storage["path"],
        "storage_gate": storage,
        "blockers": blockers,
        "execution_eligible": not blockers,
        "economic_outcomes_read": False,
        "economic_results_aggregated": False,
        "promotion_authorized": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    plan_identity = canonical_sha256(identity_payload)
    payload = {**identity_payload, "plan_identity_sha256": plan_identity}
    root = output_root.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    execution.atomic_json(root / PLAN_FILENAME, payload)
    execution.atomic_text(root / PLAN_SUCCESS, sha256_file(root / PLAN_FILENAME) + "\n")
    return payload


def validate_execution_plan(
    plan_path: Path,
    *,
    verify_source_hashes: bool = False,
    verify_policy_hashes: bool = True,
) -> dict[str, Any]:
    resolved = plan_path.expanduser().resolve()
    payload = _load_json(resolved, role="71-day continuous execution plan")
    marker = resolved.parent / PLAN_SUCCESS
    if not marker.is_file() or marker.read_text(encoding="ascii").strip() != sha256_file(resolved):
        raise F03ContinuousExecutionError("execution plan atomic admission marker drifted")
    if payload.get("schema_version") != PLAN_SCHEMA_VERSION:
        raise F03ContinuousExecutionError("execution plan schema mismatch")
    expected_identity = str(payload.pop("plan_identity_sha256", ""))
    if canonical_sha256(payload) != expected_identity:
        raise F03ContinuousExecutionError("execution plan canonical identity drifted")
    payload["plan_identity_sha256"] = expected_identity
    if payload.get("economic_outcomes_read") is not False or any(
        payload.get(field) is not False
        for field in ("promotion_authorized", "action_authorized", "live_authorized")
    ):
        raise F03ContinuousExecutionError("execution plan exceeded sensitivity authority")
    for name, row in (payload.get("runtime_artifacts") or {}).items():
        _artifact(row, role=f"runtime artifact {name}", verify_hash=True)
    continuous = payload.get("continuous_plan") or {}
    source_bindings = continuous.get("source_bindings") or []
    if len(source_bindings) != 71:
        raise F03ContinuousExecutionError("execution plan lost its 71-day sources")
    if sum(len(row.get("artifacts", ())) for row in source_bindings) != 213:
        raise F03ContinuousExecutionError("execution plan lost its 213 source artifacts")
    if verify_source_hashes:
        for row in source_bindings:
            for artifact in row.get("artifacts", ()):
                _artifact(artifact, role=f"source {row.get('day')} {artifact.get('role')}")
    operations = tuple(execution.ContinuousOperation(**row) for row in payload["operations"])
    for operation in operations:
        operation.validate()
    if canonical_sha256([asdict(row) for row in operations]) != payload.get(
        "operation_tape_sha256"
    ):
        raise F03ContinuousExecutionError("operation tape identity drifted")
    policy_payload = payload.get("policy_artifacts") or {}
    if payload.get("execution_eligible"):
        if set(policy_payload) != set(ARMS) or any(policy_payload[arm] is None for arm in ARMS):
            raise F03ContinuousExecutionError("eligible plan lacks paired policy artifacts")
        if verify_policy_hashes:
            for arm in ARMS:
                manifest = _artifact(
                    policy_payload[arm]["manifest"], role=f"{arm} policy artifact manifest"
                )
                loaded = load_policy_artifacts(Path(manifest["path"]), expected_arm=arm)
                if loaded.payload() != policy_payload[arm]:
                    raise F03ContinuousExecutionError(f"{arm} policy artifact payload drifted")
    elif not payload.get("blockers"):
        raise F03ContinuousExecutionError("ineligible plan does not name its blockers")
    return payload


def run_prepared_plan(
    plan_path: Path,
    *,
    adapter: execution.ContinuousExecutionAdapter,
    max_operations: int | None = None,
) -> dict[str, Any]:
    """Run the prepared tape without aggregating or promoting economic outcomes."""

    plan = validate_execution_plan(
        plan_path,
        verify_source_hashes=True,
        verify_policy_hashes=True,
    )
    if not plan["execution_eligible"]:
        raise F03ContinuousExecutionError(
            "actual run blocked: " + ",".join(plan.get("blockers", ()))
        )
    operations = tuple(execution.ContinuousOperation(**row) for row in plan["operations"])
    policy_artifacts = {arm: plan["policy_artifacts"][arm] for arm in ARMS}
    return execution.execute_continuous_plan(
        plan_identity_sha256=plan["plan_identity_sha256"],
        operations=operations,
        policy_artifacts=policy_artifacts,
        output_root=Path(plan["output_root"]),
        adapter=adapter,
        max_operations=max_operations,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    subcommands = parser.add_subparsers(dest="command", required=True)
    prepare = subcommands.add_parser("prepare")
    prepare.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    prepare.add_argument("--control-artifacts", type=Path)
    prepare.add_argument("--candidate-artifacts", type=Path)
    validate = subcommands.add_parser("validate")
    validate.add_argument("--plan", type=Path, default=DEFAULT_OUTPUT_ROOT / PLAN_FILENAME)
    validate.add_argument("--verify-source-hashes", action="store_true")
    args = parser.parse_args()
    if args.command == "prepare":
        result = prepare_execution_plan(
            output_root=args.output_root,
            control_artifacts_path=args.control_artifacts,
            candidate_artifacts_path=args.candidate_artifacts,
        )
    else:
        result = validate_execution_plan(
            args.plan,
            verify_source_hashes=args.verify_source_hashes,
        )
    summary = {
        "schema_version": result["schema_version"],
        "plan_identity_sha256": result["plan_identity_sha256"],
        "operation_count": result["operation_count"],
        "execution_eligible": result["execution_eligible"],
        "blockers": result["blockers"],
        "economic_outcomes_read": result["economic_outcomes_read"],
        "promotion_authorized": result["promotion_authorized"],
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
