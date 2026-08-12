#!/usr/bin/env python3
"""F03 v1.3 binding for the concrete continuous NarrowGate tick adapter.

This successor preserves the v1.2 framework plan as historical evidence and
adds the missing market-window/overlay bindings plus a production adapter
factory.  Preparation and validation remain outcome blind.
"""

from __future__ import annotations

import argparse
import json
import pickle
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import narrowgate_cpp

from data_paths import external_cache_root, resolve_portable_path
from models.replay import narrowgate_continuous_tick_adapter as adapter_abi
from models.replay.replay_state_checkpoint import ContinuousReplayState
from models.replay.restart_aware_continuous_ab import canonical_sha256, sha256_file
from models.replay.restart_aware_continuous_execution import ContinuousOperation
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_dual_overlay_ml_ab_replay as dual_abi,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_ml_ab_replay as candidate_abi,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_native_40day_full_path_ml_ab as native_runner,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_restart_aware_continuous_execution_v1_2 as framework,
)
from research.governance.public_machine_projection import source_identity_sha256

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = "causal_v12_1s_restart_aware_continuous_execution.v1.3"
PLAN_SCHEMA_VERSION = f"{SCHEMA_VERSION}.plan"
POLICY_SCHEMA_VERSION = f"{SCHEMA_VERSION}.policy_artifacts"
IDENTITY = "causal_v12_1s_71day_authoritative_continuous_tick_ab_v1_3"
ARMS = ("control", "candidate")
EXPECTED_DAYS = framework.EXPECTED_DAYS
DEFAULT_FRAMEWORK_PLAN = framework.DEFAULT_OUTPUT_ROOT / framework.PLAN_FILENAME
DEFAULT_OUTPUT_ROOT = (
    external_cache_root(ROOT)
    / "replay_dag/"
    "f03_causal_v12_1s_restart_aware_continuous_execution_v1_3_calendar_bridge_v1"
)
DEFAULT_CONTRACT = ROOT / (
    "research/families/f03_causal_13_head/docs/"
    "causal_v12_1s_restart_aware_continuous_execution_v1_3_contract_20260805.json"
)
PLAN_FILENAME = "execution-plan.json"
PLAN_SUCCESS = "_PLAN_SUCCESS"


class F03ContinuousExecutionV13Error(RuntimeError):
    """Raised when the concrete F03 replay identity is incomplete."""


def _load_json(path: Path, *, role: str) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise F03ContinuousExecutionV13Error(f"missing {role}: {resolved}")
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise F03ContinuousExecutionV13Error(f"invalid {role}: {resolved}") from exc
    if not isinstance(payload, dict):
        raise F03ContinuousExecutionV13Error(f"{role} must be a JSON object")
    return payload


def _artifact(
    row: Mapping[str, Any],
    *,
    role: str,
    verify_hash: bool = True,
) -> dict[str, Any]:
    path = Path(str(row.get("path", ""))).expanduser().resolve()
    if not path.is_file():
        raise F03ContinuousExecutionV13Error(f"missing {role}: {path}")
    expected = str(row.get("sha256", ""))
    observed = sha256_file(path) if verify_hash else expected
    if verify_hash and observed != expected and source_identity_sha256(path) != expected:
        raise F03ContinuousExecutionV13Error(f"{role} SHA256 drift")
    if int(row.get("size_bytes", path.stat().st_size)) != path.stat().st_size:
        raise F03ContinuousExecutionV13Error(f"{role} size drift")
    return {"path": str(path), "sha256": observed, "size_bytes": path.stat().st_size}


def _validate_contract() -> dict[str, Any]:
    payload = _load_json(DEFAULT_CONTRACT, role="v1.3 contract")
    if (
        payload.get("schema_version") != f"{SCHEMA_VERSION}.contract"
        or payload.get("identity") != IDENTITY
        or payload.get("economic_outcomes_read") is not False
        or payload.get("promotion_authorized") is not False
    ):
        raise F03ContinuousExecutionV13Error("v1.3 contract boundary drifted")
    parent = payload.get("historical_v1_2_framework") or {}
    parent_path = resolve_portable_path(str(parent.get("path", "")), root=ROOT).resolve()
    if parent_path != DEFAULT_FRAMEWORK_PLAN.resolve():
        raise F03ContinuousExecutionV13Error("v1.3 contract changed its v1.2 parent")
    if DEFAULT_FRAMEWORK_PLAN.is_file() and parent.get("sha256") != sha256_file(
        DEFAULT_FRAMEWORK_PLAN
    ):
        raise F03ContinuousExecutionV13Error("historical v1.2 framework plan drifted")
    return payload


def load_historical_framework_plan(plan_path: Path) -> dict[str, Any]:
    """Validate the frozen v1.2 tape without re-binding its old runtime files.

    The v1.2 plan is historical evidence.  Public-document path projection can
    legitimately change files that its original runtime manifest recorded,
    while the admitted plan bytes and operation tape remain immutable.  The
    concrete v1.3 successor binds the current runtime separately.
    """

    resolved = plan_path.expanduser().resolve()
    if resolved != DEFAULT_FRAMEWORK_PLAN.resolve():
        raise F03ContinuousExecutionV13Error("v1.3 framework is not the frozen v1.2 plan")
    payload = _load_json(resolved, role="historical v1.2 framework plan")
    marker = resolved.parent / framework.PLAN_SUCCESS
    if not marker.is_file() or marker.read_text(encoding="ascii").strip() != sha256_file(
        resolved
    ):
        raise F03ContinuousExecutionV13Error("historical v1.2 admission marker drifted")
    if payload.get("schema_version") != framework.PLAN_SCHEMA_VERSION:
        raise F03ContinuousExecutionV13Error("historical v1.2 schema drifted")
    identity_payload = dict(payload)
    expected_identity = str(identity_payload.pop("plan_identity_sha256", ""))
    if canonical_sha256(identity_payload) != expected_identity:
        raise F03ContinuousExecutionV13Error("historical v1.2 identity drifted")
    if payload.get("economic_outcomes_read") is not False or any(
        payload.get(field) is not False
        for field in ("promotion_authorized", "action_authorized", "live_authorized")
    ):
        raise F03ContinuousExecutionV13Error("historical v1.2 exceeded its authority")
    continuous = payload.get("continuous_plan") or {}
    source_bindings = continuous.get("source_bindings") or []
    if len(source_bindings) != 71 or sum(
        len(row.get("artifacts", ())) for row in source_bindings
    ) != 213:
        raise F03ContinuousExecutionV13Error("historical v1.2 source census drifted")
    operations = tuple(ContinuousOperation(**row) for row in payload.get("operations", ()))
    for operation in operations:
        operation.validate()
    if canonical_sha256([adapter_abi.asdict(row) for row in operations]) != payload.get(
        "operation_tape_sha256"
    ):
        raise F03ContinuousExecutionV13Error("historical v1.2 operation tape drifted")
    return payload


def _runtime_artifacts() -> dict[str, dict[str, Any]]:
    """Bind the exact concrete replay implementation used by a prepared plan."""

    paths = {
        "shared_continuous_adapter": Path(adapter_abi.__file__),
        "f03_v1_3_binding": Path(__file__),
        "v1_3_contract": DEFAULT_CONTRACT,
        "tick_replay_python": ROOT / "models/backtest_tick.py",
        "continuous_accounting": ROOT / "models/replay/continuous_accounting.py",
        "replay_state_checkpoint": ROOT / "models/replay/replay_state_checkpoint.py",
        "narrowgate_cpp_extension": Path(narrowgate_cpp.__file__),
        "tick_replay_cpp": ROOT / "cpp/narrowgate_cpp/tick_replay.cpp",
        "tick_replay_header": ROOT / "cpp/narrowgate_cpp/tick_replay.hpp",
        "quote_core_cpp": ROOT / "cpp/narrowgate_cpp/quote_core.cpp",
        "quote_core_header": ROOT / "cpp/narrowgate_cpp/quote_core.hpp",
        "cpp_bindings": ROOT / "cpp/narrowgate_cpp/bindings.cpp",
        "cpp_common": ROOT / "cpp/narrowgate_cpp/common.hpp",
    }
    return {
        name: _artifact(
            {
                "path": str(path.resolve()),
                "sha256": sha256_file(path.resolve()),
                "size_bytes": path.stat().st_size,
            },
            role=f"runtime artifact {name}",
        )
        for name, path in paths.items()
    }


def _load_policy_manifest(
    path: Path,
    *,
    expected_arm: str,
    verify_market_window_hashes: bool = True,
) -> dict[str, Any]:
    payload = _load_json(path, role=f"{expected_arm} v1.3 policy manifest")
    if (
        payload.get("schema_version") != POLICY_SCHEMA_VERSION
        or payload.get("arm") != expected_arm
        or payload.get("ml_enabled") is not True
        or payload.get("q90_action_enabled") is not False
        or payload.get("buy_fill_selection_enabled") is not False
    ):
        raise F03ContinuousExecutionV13Error(f"{expected_arm} policy semantics drifted")
    expected_cadence = 10_000 if expected_arm == "control" else 1_000
    if int(payload.get("cadence_ms", 0)) != expected_cadence:
        raise F03ContinuousExecutionV13Error(f"{expected_arm} cadence drifted")
    for name in (
        "operational_config",
        "baseline_identity",
        "bundle_meta",
        "feature_dag",
        "initial_state",
    ):
        payload[name] = _artifact(payload.get(name) or {}, role=f"{expected_arm} {name}")
    days = payload.get("days")
    if not isinstance(days, Mapping) or tuple(days) != EXPECTED_DAYS:
        raise F03ContinuousExecutionV13Error(
            f"{expected_arm} policy manifest does not bind exact 71-day order"
        )
    normalized: dict[str, Any] = {}
    for day in EXPECTED_DAYS:
        row = days[day]
        if not isinstance(row, Mapping):
            raise F03ContinuousExecutionV13Error(f"{expected_arm} {day} row is invalid")
        source_profile = str(row.get("source_profile", ""))
        if source_profile not in {"native", "provider_normalized"}:
            raise F03ContinuousExecutionV13Error(f"{expected_arm} {day} source is ambiguous")
        market_window = _artifact(
            row.get("market_window") or {},
            role=f"{expected_arm} {day} market window",
            verify_hash=verify_market_window_hashes,
        )
        source_identity = str(row.get("source_identity_sha256", ""))
        if len(source_identity) != 64:
            raise F03ContinuousExecutionV13Error(f"{expected_arm} {day} source hash is absent")
        if expected_arm == "control":
            binding = row.get("control_overlay_binding")
            if not isinstance(binding, Mapping):
                raise F03ContinuousExecutionV13Error(f"control {day} overlay binding is absent")
            overlay = {"kind": "v9_control", "binding": dict(binding)}
        else:
            overlay_dir = Path(str(row.get("candidate_overlay_dir", ""))).expanduser().resolve()
            if not overlay_dir.is_dir():
                raise F03ContinuousExecutionV13Error(
                    f"candidate {day} overlay directory is absent: {overlay_dir}"
                )
            overlay = {
                "kind": "candidate_1s",
                "directory": str(overlay_dir),
                "manifest": _artifact(
                    row.get("candidate_overlay_manifest") or {},
                    role=f"candidate {day} overlay manifest",
                ),
                "data": _artifact(
                    row.get("candidate_overlay_data") or {},
                    role=f"candidate {day} overlay data",
                ),
            }
        normalized[day] = {
            "source_profile": source_profile,
            "source_identity_sha256": source_identity,
            "market_window": market_window,
            "overlay": overlay,
        }
    payload["days"] = normalized
    identity = str(payload.get("policy_identity_sha256", ""))
    if len(identity) != 64:
        raise F03ContinuousExecutionV13Error(f"{expected_arm} policy identity is absent")
    model_identity = str(payload.get("model_bundle_identity_sha256", ""))
    if len(model_identity) != 64:
        raise F03ContinuousExecutionV13Error(
            f"{expected_arm} model bundle content identity is absent"
        )
    payload["manifest"] = {
        "path": str(path.resolve()),
        "sha256": sha256_file(path.resolve()),
        "size_bytes": path.stat().st_size,
    }
    return payload


def prepare_execution_plan(
    *,
    framework_plan: Path = DEFAULT_FRAMEWORK_PLAN,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    control_artifacts: Path | None = None,
    candidate_artifacts: Path | None = None,
) -> dict[str, Any]:
    contract = _validate_contract()
    parent = load_historical_framework_plan(framework_plan)
    policies: dict[str, Any] = {}
    blockers: list[str] = []
    for arm, path in (("control", control_artifacts), ("candidate", candidate_artifacts)):
        if path is None:
            blockers.append(f"{arm}_authoritative_market_window_and_overlay_unbound")
        else:
            policies[arm] = _load_policy_manifest(path.resolve(), expected_arm=arm)
    if set(policies) == set(ARMS):
        for name in ("operational_config", "baseline_identity", "initial_state"):
            if policies["control"][name]["sha256"] != policies["candidate"][name]["sha256"]:
                raise F03ContinuousExecutionV13Error(f"paired {name} differs between arms")
        parent_sources = {
            row["day"]: row for row in parent["continuous_plan"]["source_bindings"]
        }
        if tuple(parent_sources) != EXPECTED_DAYS:
            raise F03ContinuousExecutionV13Error("v1.2 source order drifted")
        for day in EXPECTED_DAYS:
            control = policies["control"]["days"][day]
            candidate = policies["candidate"]["days"][day]
            if (
                control["market_window"]["sha256"]
                != candidate["market_window"]["sha256"]
                or control["source_identity_sha256"] != candidate["source_identity_sha256"]
                or control["source_profile"] != candidate["source_profile"]
            ):
                raise F03ContinuousExecutionV13Error(f"paired market path drifted on {day}")
            frozen_source = parent_sources[day]
            expected_profile = (
                "native"
                if frozen_source.get("book_identity") == "native_available"
                else "provider_normalized"
            )
            expected_identity = canonical_sha256(
                {
                    key: frozen_source[key]
                    for key in (
                        "day",
                        "book_identity",
                        "book_root",
                        "feature_identity",
                        "exact_queue_authority",
                        "exact_lifecycle_authority",
                        "continuous_economic_sensitivity_authority",
                        "artifacts",
                    )
                }
            )
            if (
                control["source_profile"] != expected_profile
                or control["source_identity_sha256"] != expected_identity
            ):
                raise F03ContinuousExecutionV13Error(
                    f"{day} policy source differs from frozen v1.2 source binding"
                )
    operations = [ContinuousOperation(**row) for row in parent["operations"]]
    drain_durations = [
        row.end_ts_ms - row.start_ts_ms for row in operations if row.kind == "cancel_drain"
    ]
    if not drain_durations or min(drain_durations) <= 0:
        raise F03ContinuousExecutionV13Error("framework lacks actual cancel-drain operations")
    # Short visible islands can truncate an individual drain operation to a
    # handful of milliseconds.  The panel shutdown must retain the complete
    # frozen drain allowance, represented by the maximum operation duration.
    panel_cancel_drain_ms = max(drain_durations)
    epochs = adapter_abi.compile_authoritative_epochs(
        operations, panel_cancel_drain_ms=panel_cancel_drain_ms
    )
    identity_payload = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "identity": IDENTITY,
        "historical_v1_2_framework": {
            "path": str(framework_plan.resolve()),
            "sha256": sha256_file(framework_plan.resolve()),
            "plan_identity_sha256": parent["plan_identity_sha256"],
            "classification": "implemented_orchestration_framework_not_executable_replay",
        },
        "contract": {
            "path": str(DEFAULT_CONTRACT.resolve()),
            "sha256": sha256_file(DEFAULT_CONTRACT.resolve()),
        },
        "runtime_artifacts": _runtime_artifacts(),
        "frozen_71_day_identity": {
            "days": list(EXPECTED_DAYS),
            "day_count": 71,
            "source_artifact_count": 213,
            "source_artifact_manifest_sha256": parent["continuous_plan"][
                "source_artifact_manifest_sha256"
            ],
            "restart_timeline_sha256": parent["continuous_plan"][
                "restart_timeline_sha256"
            ],
            "operation_tape_sha256": parent["operation_tape_sha256"],
        },
        "operations": parent["operations"],
        "operation_tape_sha256": parent["operation_tape_sha256"],
        "authoritative_epochs": [adapter_abi.asdict(row) for row in epochs],
        "authoritative_epoch_count": len(epochs),
        "panel_cancel_drain_ms": panel_cancel_drain_ms,
        "policy_artifacts": {arm: policies.get(arm) for arm in ARMS},
        "output_root": str(output_root.expanduser().resolve()),
        "blockers": blockers,
        "execution_eligible": not blockers,
        "economic_outcomes_read": False,
        "economic_results_aggregated": False,
        "promotion_authorized": False,
        "action_authorized": False,
        "live_authorized": False,
        "primary_40_day_exact_native_substitute": False,
        "sensitivity_substrate_only": True,
        "contract_status": contract["status"],
    }
    identity = canonical_sha256(identity_payload)
    payload = {**identity_payload, "plan_identity_sha256": identity}
    root = output_root.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    adapter_abi._atomic_json(root / PLAN_FILENAME, payload)
    adapter_abi._atomic_text(root / PLAN_SUCCESS, sha256_file(root / PLAN_FILENAME) + "\n")
    return payload


def validate_execution_plan(plan_path: Path, *, verify_bound_artifacts: bool = True) -> dict[str, Any]:
    payload = _load_json(plan_path, role="v1.3 execution plan")
    marker = plan_path.parent / PLAN_SUCCESS
    if not marker.is_file() or marker.read_text(encoding="ascii").strip() != sha256_file(plan_path):
        raise F03ContinuousExecutionV13Error("v1.3 plan atomic marker drifted")
    if payload.get("schema_version") != PLAN_SCHEMA_VERSION:
        raise F03ContinuousExecutionV13Error("v1.3 plan schema drifted")
    expected = str(payload.pop("plan_identity_sha256", ""))
    if canonical_sha256(payload) != expected:
        raise F03ContinuousExecutionV13Error("v1.3 plan identity drifted")
    payload["plan_identity_sha256"] = expected
    frozen = payload.get("frozen_71_day_identity") or {}
    if frozen.get("day_count") != 71 or frozen.get("source_artifact_count") != 213:
        raise F03ContinuousExecutionV13Error("v1.3 lost the frozen denominator")
    runtime_artifacts = payload.get("runtime_artifacts")
    if not isinstance(runtime_artifacts, Mapping) or not runtime_artifacts:
        raise F03ContinuousExecutionV13Error("v1.3 runtime artifact chain is absent")
    for name, row in runtime_artifacts.items():
        _artifact(row, role=f"runtime artifact {name}")
    if any(
        payload.get(name) is not False
        for name in (
            "economic_outcomes_read",
            "economic_results_aggregated",
            "promotion_authorized",
            "action_authorized",
            "live_authorized",
        )
    ):
        raise F03ContinuousExecutionV13Error("v1.3 exceeded mechanics authority")
    if payload.get("execution_eligible"):
        if verify_bound_artifacts:
            for arm in ARMS:
                manifest = Path(payload["policy_artifacts"][arm]["manifest"]["path"])
                observed = _load_policy_manifest(manifest, expected_arm=arm)
                if observed != payload["policy_artifacts"][arm]:
                    raise F03ContinuousExecutionV13Error(f"{arm} policy payload drifted")
    elif not payload.get("blockers"):
        raise F03ContinuousExecutionV13Error("blocked v1.3 plan lacks explicit blockers")
    return payload


class _BoundInputProvider:
    def __init__(self, policies: Mapping[str, Mapping[str, Any]]) -> None:
        self.policies = policies

    @staticmethod
    def _load_window(row: Mapping[str, Any]) -> Any:
        artifact = _artifact(row["market_window"], role="bound market window")
        with Path(artifact["path"]).open("rb") as handle:
            window = pickle.load(handle)
        if getattr(window, "ml_data", None) is not None:
            raise F03ContinuousExecutionV13Error("bound market window is not model-free")
        return window

    def load_day(self, *, arm_id: str, day: str) -> adapter_abi.ReplayDayInput:
        policy = self.policies[arm_id]
        row = policy["days"][day]
        window = self._load_window(row)
        overlay = row["overlay"]
        if overlay["kind"] == "v9_control":
            schedule = dual_abi.load_bound_v9_control_overlay(
                overlay["binding"],
                expected_day=day,
                expected_model_bundle_identity_sha256=policy[
                    "model_bundle_identity_sha256"
                ],
            )
            ml_data = schedule.ml_data
            overlay_identity = schedule.identity_sha256
        else:
            schedule = candidate_abi.load_admitted_one_second_overlay(
                Path(overlay["directory"]), allow_test_only=False
            )
            if schedule.utc_day != day:
                raise F03ContinuousExecutionV13Error("candidate overlay day drifted")
            ml_data = schedule.ml_data
            overlay_identity = schedule.overlay_identity_sha256
        native = row["source_profile"] == "native"
        replay_input = adapter_abi.ReplayDayInput(
            day=day,
            window=window,
            ml_data=ml_data,
            market_window_sha256=row["market_window"]["sha256"],
            overlay_identity_sha256=overlay_identity,
            source_identity_sha256=row["source_identity_sha256"],
            source_profile=row["source_profile"],
            exact_queue_authority=native,
            exact_lifecycle_authority=native,
        )
        replay_input.validate()
        return replay_input


def _initial_state(path: Path, *, arm: str) -> ContinuousReplayState:
    payload = _load_json(path, role=f"{arm} initial state")
    raw = payload.get("continuous_replay_state", payload)
    if not isinstance(raw, Mapping):
        raise F03ContinuousExecutionV13Error("initial state payload is invalid")
    normalized = dict(raw)
    normalized["arm_id"] = arm
    state = ContinuousReplayState.from_dict(normalized)
    state.validate(require_restart_safe=True)
    return state


def build_concrete_adapter(plan: Mapping[str, Any]) -> adapter_abi.NarrowGateContinuousTickReplayAdapter:
    if plan.get("execution_eligible") is not True:
        raise F03ContinuousExecutionV13Error(
            "actual run blocked: " + ",".join(plan.get("blockers", ()))
        )
    policies = plan["policy_artifacts"]
    config = Path(policies["control"]["operational_config"]["path"])
    base_params = native_runner._load_formal_base_params(config)
    if bool(base_params.get("dynamic_fill_hazard_action_enabled", False)):
        raise F03ContinuousExecutionV13Error("bound operational config enables q90 action")
    if bool(base_params.get("buy_fill_selection_live_enabled", False)):
        raise F03ContinuousExecutionV13Error(
            "bound operational config enables BUY fill selection"
        )
    base_params["dynamic_fill_hazard_action_enabled"] = False
    base_params["buy_fill_selection_live_enabled"] = False
    bindings = {
        arm: adapter_abi.AdapterArmBinding(
            arm_id=arm,
            params=dict(base_params),
            policy_identity_sha256=str(policies[arm]["policy_identity_sha256"]),
            cadence_ms=int(policies[arm]["cadence_ms"]),
        )
        for arm in ARMS
    }
    states = {
        arm: _initial_state(Path(policies[arm]["initial_state"]["path"]), arm=arm)
        for arm in ARMS
    }
    operations = tuple(ContinuousOperation(**row) for row in plan["operations"])
    return adapter_abi.NarrowGateContinuousTickReplayAdapter(
        plan_identity_sha256=plan["plan_identity_sha256"],
        operations=operations,
        arm_bindings=bindings,
        input_provider=_BoundInputProvider(policies),
        initial_states=states,
        output_root=Path(plan["output_root"]),
        panel_cancel_drain_ms=int(plan["panel_cancel_drain_ms"]),
    )


def run_prepared_plan(
    plan_path: Path,
    *,
    adapter: adapter_abi.NarrowGateContinuousTickReplayAdapter | None = None,
    max_epochs: int | None = None,
) -> dict[str, Any]:
    plan = validate_execution_plan(plan_path, verify_bound_artifacts=True)
    concrete = build_concrete_adapter(plan) if adapter is None else adapter
    return concrete.run(max_epochs=max_epochs)


def main() -> int:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--framework-plan", type=Path, default=DEFAULT_FRAMEWORK_PLAN)
    prepare.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    prepare.add_argument("--control-artifacts", type=Path)
    prepare.add_argument("--candidate-artifacts", type=Path)
    validate = commands.add_parser("validate")
    validate.add_argument("--plan", type=Path, default=DEFAULT_OUTPUT_ROOT / PLAN_FILENAME)
    run = commands.add_parser("run")
    run.add_argument("--plan", type=Path, default=DEFAULT_OUTPUT_ROOT / PLAN_FILENAME)
    run.add_argument("--max-epochs", type=int)
    args = parser.parse_args()
    if args.command == "prepare":
        result = prepare_execution_plan(
            framework_plan=args.framework_plan,
            output_root=args.output_root,
            control_artifacts=args.control_artifacts,
            candidate_artifacts=args.candidate_artifacts,
        )
    elif args.command == "validate":
        result = validate_execution_plan(args.plan)
    else:
        result = run_prepared_plan(args.plan, max_epochs=args.max_epochs)
    print(
        json.dumps(
            {
                key: result.get(key)
                for key in (
                    "schema_version",
                    "plan_identity_sha256",
                    "execution_eligible",
                    "blockers",
                    "epoch_count",
                    "epochs_completed_this_call",
                    "economic_outcomes_read",
                    "promotion_authorized",
                )
                if key in result
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
