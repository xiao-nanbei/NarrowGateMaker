#!/usr/bin/env python3
"""Full-path owner-route replay for role-safe, add-only BER protection."""

from __future__ import annotations

import argparse
import concurrent.futures
import copy
import hashlib
import json
import math
import os
import shutil
import tempfile
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from data_paths import data_root
from models import backtest_tick as bt
from models.audit import experiment_scorecard_v2
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_native_40day_full_path_ml_ab as native_runner,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_v9_10s_control_overlay_repair as control_repair,
)

ROOT = Path(__file__).resolve().parents[4]
DATA_ROOT = data_root(ROOT)
IDENTITY = "ber_guard_role_safe_add_only_current_stack_owner_v1"
SCHEMA_VERSION = f"{IDENTITY}.development"
ARMS = (
    "current_live_held_global_ber_control",
    "ber_exposure_add_only_role_safe",
)
SPEC = ROOT / (
    "research/families/f09_campaign_action_uplift/docs/"
    "ber_guard_role_safe_add_only_current_stack_owner_v1_spec_20260808.json"
)
OFFLINE_PROJECTION = ROOT / (
    "research/families/f09_campaign_action_uplift/docs/"
    "ber_guard_role_safe_add_only_current_stack_owner_v1_"
    "offline_execution_projection_20260808.json"
)
DEFAULT_EXECUTION_AMENDMENT = ROOT / (
    "research/families/f09_campaign_action_uplift/docs/"
    "ber_guard_role_safe_add_only_current_stack_owner_v1_"
    "execution_amendment_v1_20260808.json"
)
DEFAULT_PLAN = DATA_ROOT / (
    "cache/replay_dag/"
    "f03_causal_v12_1s_native_40day_full_path_ml_ab_v3/execution-plan.json"
)
DEFAULT_F03_AMENDMENT = ROOT / (
    "research/families/f03_causal_13_head/docs/"
    "causal_v12_1s_native_40day_full_path_ml_ab_execution_amendment_v4_20260805.json"
)
DEFAULT_OUTPUT = DATA_ROOT / (
    "reports/"
    "ber_guard_role_safe_add_only_current_stack_owner_v1_20260808/"
    "development_execution_v2"
)
DAY_SUCCESS = "_SUCCESS"
ARM_SUCCESS = "_ARM_SUCCESS"
PANEL_SUCCESS = "_PANEL_SUCCESS"
STORAGE_RESERVE_BYTES = 60 * (1 << 30)
ESTIMATED_OUTPUT_BYTES = 1 * (1 << 30)
FLOAT_PARITY_ATOL = 1e-9


class BerRoleSafeError(RuntimeError):
    """Fail closed when the frozen role-safe BER identity drifts."""


def _load_json(path: Path, *, role: str) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise BerRoleSafeError(f"missing {role}: {resolved}")
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BerRoleSafeError(f"invalid {role}: {resolved}") from exc
    if not isinstance(payload, dict):
        raise BerRoleSafeError(f"{role} must be a JSON object")
    return payload


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f".{path.name}.tmp-{uuid.uuid4().hex}"
    try:
        with tmp.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def _atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f".{path.name}.tmp-{uuid.uuid4().hex}"
    try:
        with tmp.open("w", encoding="ascii") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def _validate_artifact(path: Path, expected_sha256: str, *, role: str) -> None:
    resolved = path.expanduser().resolve()
    if not resolved.is_file() or native_runner._sha256_file(resolved) != expected_sha256:
        raise BerRoleSafeError(f"{role} SHA256 drift: {resolved}")


def _canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _mapping_differences(left: Any, right: Any) -> list[str]:
    differences: list[str] = []

    def visit(lhs: Any, rhs: Any, path: str) -> None:
        if isinstance(lhs, Mapping) and isinstance(rhs, Mapping):
            for key in sorted(set(lhs) | set(rhs), key=str):
                child = f"{path}.{key}" if path else str(key)
                if key not in lhs:
                    differences.append(f"{child}:missing_left")
                elif key not in rhs:
                    differences.append(f"{child}:missing_right")
                else:
                    visit(lhs[key], rhs[key], child)
            return
        if isinstance(lhs, Sequence) and not isinstance(lhs, (str, bytes)):
            if not isinstance(rhs, Sequence) or isinstance(rhs, (str, bytes)):
                differences.append(f"{path}:type")
                return
            if len(lhs) != len(rhs):
                differences.append(f"{path}:length")
                return
            for index, (lhs_item, rhs_item) in enumerate(zip(lhs, rhs, strict=True)):
                visit(lhs_item, rhs_item, f"{path}[{index}]")
            return
        if lhs != rhs:
            differences.append(path or "root")

    visit(left, right, "")
    return differences


def _storage_gate(output: Path) -> dict[str, Any]:
    resolved = output.expanduser().resolve()
    required_root = DATA_ROOT.resolve()
    try:
        resolved.relative_to(required_root)
    except ValueError as exc:
        raise BerRoleSafeError(
            "authoritative output must remain on the configured data root"
        ) from exc
    probe = resolved
    while not probe.exists():
        probe = probe.parent
    free = shutil.disk_usage(probe).free
    required = STORAGE_RESERVE_BYTES + int(2.5 * ESTIMATED_OUTPUT_BYTES)
    if free < required:
        raise BerRoleSafeError(f"storage gate failed: free={free}, required={required}")
    return {"free_bytes": free, "required_bytes": required, "passed": True}


def _load_yaml(path: Path, *, role: str) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise BerRoleSafeError(f"missing {role}: {resolved}")
    try:
        payload = yaml.safe_load(resolved.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise BerRoleSafeError(f"invalid {role}: {resolved}") from exc
    if not isinstance(payload, dict):
        raise BerRoleSafeError(f"{role} must be a YAML mapping")
    return payload


def _validate_projection() -> dict[str, Any]:
    contract = _load_json(OFFLINE_PROJECTION, role="offline execution projection")
    if (
        contract.get("schema_version")
        != "ber_guard_role_safe_add_only_current_stack_owner.v1."
        "offline_execution_projection"
        or contract.get("identity") != IDENTITY
        or contract.get("status")
        != "execution_projection_frozen_candidate_outcomes_unread"
    ):
        raise BerRoleSafeError("offline execution projection identity drifted")
    source = contract.get("source_config") or {}
    source_path = ROOT / str(source.get("path", ""))
    _validate_artifact(source_path, str(source.get("sha256", "")), role="raw EC2 config")
    raw = _load_yaml(source_path, role="raw EC2 config")
    if _canonical_sha256(raw) != source.get("canonical_mapping_sha256"):
        raise BerRoleSafeError("raw EC2 config canonical mapping drifted")
    projection = contract.get("projection") or {}
    if projection.get("allowed_difference_paths") != ["lifecycle_journal_v2.enabled"]:
        raise BerRoleSafeError("offline projection allowlist drifted")
    projected = copy.deepcopy(raw)
    lifecycle = projected.get("lifecycle_journal_v2")
    if not isinstance(lifecycle, dict) or lifecycle.get("enabled") is not True:
        raise BerRoleSafeError("raw EC2 config does not enable lifecycle journal v2")
    lifecycle["enabled"] = False
    differences = _mapping_differences(raw, projected)
    if differences != projection["allowed_difference_paths"]:
        raise BerRoleSafeError(f"offline projection escaped allowlist: {differences}")
    if _canonical_sha256(projected) != projection.get(
        "projected_canonical_mapping_sha256"
    ):
        raise BerRoleSafeError("offline projected config identity drifted")
    for field, expected in {
        "raw_remote_config_rewritten": False,
        "remote_mount_validation_monkeypatched": False,
        "strategy_or_execution_parameter_changes": False,
        "order_path_equivalent": True,
    }.items():
        if projection.get(field) is not expected:
            raise BerRoleSafeError(f"offline projection permission drift: {field}")
    provenance = contract.get("baseline_provenance") or {}
    _validate_artifact(
        ROOT / str(provenance.get("operational_identity_path", "")),
        str(provenance.get("operational_identity_sha256", "")),
        role="v10 operational baseline",
    )
    if provenance.get("observability_successor_quote_policy_changed") is not False:
        raise BerRoleSafeError("v10 quote-policy provenance drifted")
    return contract


def _validate_execution_amendment(
    path: Path,
    *,
    requested_days: Sequence[str] | None = None,
) -> dict[str, Any]:
    amendment = _load_json(path, role="role-safe BER execution amendment")
    schema = str(amendment.get("schema_version", ""))
    if (
        not schema.startswith(
            "ber_guard_role_safe_add_only_current_stack_owner.v1.execution_amendment_v"
        )
        or amendment.get("identity") != IDENTITY
        or amendment.get("status")
        not in {
            "execution_bound_one_development_day_read",
            "execution_bound_full_development_read_after_mechanics",
        }
    ):
        raise BerRoleSafeError("role-safe BER execution amendment identity drifted")
    _validate_artifact(
        SPEC,
        str((amendment.get("spec") or {}).get("sha256", "")),
        role="frozen role-safe BER spec",
    )
    _validate_artifact(
        OFFLINE_PROJECTION,
        str((amendment.get("offline_execution_projection") or {}).get("sha256", "")),
        role="offline execution projection",
    )
    for role, row in (amendment.get("implementation") or {}).items():
        if not isinstance(row, dict):
            raise BerRoleSafeError(f"invalid implementation binding: {role}")
        artifact = Path(str(row.get("path", "")))
        if not artifact.is_absolute():
            artifact = ROOT / artifact
        _validate_artifact(artifact, str(row.get("sha256", "")), role=f"BER {role}")
    permissions = amendment.get("permissions") or {}
    allowed_days = list(permissions.get("development_days_read") or ())
    if permissions.get("economic_outcomes_read") is not True:
        raise BerRoleSafeError("execution amendment does not authorize Development read")
    if requested_days is not None and any(day not in allowed_days for day in requested_days):
        raise BerRoleSafeError("requested day escaped execution amendment permission")
    for field in (
        "validation_read",
        "sealed_holdout_read",
        "action_authorized",
        "live_authorized",
        "automatic_deployment",
    ):
        if permissions.get(field) is not False:
            raise BerRoleSafeError(f"execution permission drift: {field}")
    return amendment


def validate_spec(
    *,
    plan_path: Path = DEFAULT_PLAN,
    f03_amendment_path: Path = DEFAULT_F03_AMENDMENT,
    execution_amendment_path: Path = DEFAULT_EXECUTION_AMENDMENT,
    requested_days: Sequence[str] | None = None,
    verify_all_windows: bool = True,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    spec = _load_json(SPEC, role="frozen role-safe BER spec")
    execution = _validate_execution_amendment(
        execution_amendment_path, requested_days=requested_days
    )
    projection = _validate_projection()
    if (
        spec.get("schema_version")
        != "ber_guard_role_safe_add_only_current_stack_owner.v1.spec"
        or spec.get("identity") != IDENTITY
        or spec.get("status_at_freeze")
        != "outcome_informed_owner_development_screen_registered_candidate_outcomes_unread"
    ):
        raise BerRoleSafeError("frozen role-safe BER identity drifted")
    arms = spec.get("arms") or {}
    control = arms.get("control") or {}
    candidate = arms.get("candidate") or {}
    expected = (
        control.get("arm_id") == ARMS[0]
        and candidate.get("arm_id") == ARMS[1]
        and float(control.get("ber_guard_thresh", math.nan)) == 1.2
        and float(candidate.get("ber_guard_thresh", math.nan)) == 1.2
        and float(control.get("ber_spread_mult", math.nan)) == 2.0
        and float(candidate.get("ber_spread_mult", math.nan)) == 2.0
        and control.get("ber_exposure_add_only") is False
        and candidate.get("ber_exposure_add_only") is True
    )
    if not expected:
        raise BerRoleSafeError("frozen role-safe BER arms drifted")
    checks = (
        (ROOT / spec["baseline"]["identity_path"], spec["baseline"]["identity_sha256"], "baseline identity"),
        (ROOT / spec["baseline"]["config_path"], spec["baseline"]["config_sha256"], "EC2 config"),
        (ROOT / spec["model_and_p3"]["bundle_meta_path"], spec["model_and_p3"]["bundle_meta_sha256"], "model bundle"),
        (ROOT / spec["model_and_p3"]["p3_path"], spec["model_and_p3"]["p3_sha256"], "P3 artifact"),
        (plan_path, spec["development_panel"]["reused_execution_plan_sha256"], "F03 execution plan"),
        (ROOT / spec["scorecard"]["implementation_path"], spec["scorecard"]["implementation_sha256"], "scorecard"),
    )
    for artifact, digest, role in checks:
        _validate_artifact(Path(artifact), str(digest), role=role)
    profile = experiment_scorecard_v2.score_profile_contract("action_defense_v2")
    if profile["profile_sha256"] != spec["scorecard"]["profile_sha256"]:
        raise BerRoleSafeError("action_defense_v2 profile drifted")
    plan = _load_json(plan_path, role="reused F03 execution plan")
    identity_payload = plan.get("identity_payload")
    if not isinstance(identity_payload, dict) or plan.get("plan_identity_sha256") != (
        native_runner._canonical_sha256(identity_payload)
    ):
        raise BerRoleSafeError("reused F03 execution plan identity cannot be reproduced")
    marker = plan_path.parent / native_runner.PLAN_SUCCESS
    if not marker.is_file() or marker.read_text(encoding="ascii").strip() != (
        native_runner._sha256_file(plan_path)
    ):
        raise BerRoleSafeError("reused F03 plan marker drifted")
    _validate_artifact(
        f03_amendment_path,
        str((execution.get("reused_input_contract") or {}).get("f03_execution_amendment_sha256", "")),
        role="reused F03 execution amendment",
    )
    if (execution.get("reused_input_contract") or {}).get(
        "f03_execution_plan_sha256"
    ) != spec["development_panel"]["reused_execution_plan_sha256"]:
        raise BerRoleSafeError("execution amendment changed the F03 plan")
    days = list(spec["development_panel"]["days"])
    plan_days = [row["utc_day"] for row in identity_payload.get("days", ())]
    if days != plan_days or len(days) != 40:
        raise BerRoleSafeError("frozen 40-day denominator drifted")
    if verify_all_windows:
        for row in identity_payload.get("days", ()):
            _validate_artifact(
                Path(row["window"]["path"]),
                row["window"]["sha256"],
                role=f"{row['utc_day']} market window",
            )
    if projection["source_config"]["sha256"] != spec["baseline"]["config_sha256"]:
        raise BerRoleSafeError("offline projection escaped the frozen EC2 config")
    return spec, plan, execution


def _load_offline_params(config: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    contract = _validate_projection()
    raw = _load_yaml(config, role="raw EC2 config")
    projected = copy.deepcopy(raw)
    projected["lifecycle_journal_v2"]["enabled"] = False
    if _mapping_differences(raw, projected) != contract["projection"][
        "allowed_difference_paths"
    ]:
        raise BerRoleSafeError("offline projection escaped its allowlist")
    with tempfile.TemporaryDirectory(prefix="narrowgate-ber-role-safe-") as tmp:
        projected_path = Path(tmp) / "config.yaml"
        projected_path.write_text(yaml.safe_dump(projected, sort_keys=False), encoding="utf-8")
        params = native_runner._load_formal_base_params(projected_path)
    if (
        float(params.get("ber_guard_thresh", math.nan)) != 1.2
        or float(params.get("ber_spread_mult", math.nan)) != 2.0
        or bool(params.get("dynamic_fill_hazard_action_enabled", True))
        or bool(params.get("buy_fill_selection_live_enabled", True))
    ):
        raise BerRoleSafeError("offline projection changed the frozen operational stack")
    audit = {
        "source_config_path": str(config.resolve()),
        "source_config_sha256": native_runner._sha256_file(config),
        "source_canonical_mapping_sha256": _canonical_sha256(raw),
        "projected_canonical_mapping_sha256": _canonical_sha256(projected),
        "difference_paths": _mapping_differences(raw, projected),
        "projection_contract_path": str(OFFLINE_PROJECTION),
        "projection_contract_sha256": native_runner._sha256_file(OFFLINE_PROJECTION),
        "ber_clock_identity": (
            "live_held_completed_10s_feature_sampled_on_completed_1s_callback.v1"
        ),
    }
    return params, audit


def _assert_ber_cpp_python_lockstep(
    cpp_result: Mapping[str, Any], python_result: Mapping[str, Any]
) -> None:
    exact_fields = (
        "n_requotes",
        "ber_active_count",
        "ber_feature_publish_count",
        "ber_active_end",
        "ber_role_safe_decision_count",
        "ber_role_safe_buy_add_count",
        "ber_role_safe_sell_add_count",
        "ber_role_safe_flat_bypass_count",
        "ber_role_safe_mixed_fail_closed_count",
        "ber_role_safe_pair_change_count",
        "ber_role_safe_bid_change_count",
        "ber_role_safe_ask_change_count",
        "ber_role_safe_source_mismatch_count",
        "ber_role_safe_cap_collision_count",
        "ber_role_safe_cap_infeasible_count",
    )
    for field in exact_fields:
        if cpp_result.get(field) != python_result.get(field):
            raise BerRoleSafeError(f"Python/C++ BER lockstep mismatch: {field}")
    for field in ("ber_held_input_end", "ber_ema_fast_end", "ber_ema_slow_end"):
        left = float(cpp_result.get(field, math.nan))
        right = float(python_result.get(field, math.nan))
        if not (
            math.isfinite(left)
            and math.isfinite(right)
            and math.isclose(left, right, rel_tol=0.0, abs_tol=FLOAT_PARITY_ATOL)
        ):
            raise BerRoleSafeError(f"Python/C++ BER state mismatch: {field}")
def _simulate_arm(
    *,
    day: str,
    arm: str,
    window: Any,
    base: Mapping[str, Any],
    shared: Mapping[str, Any],
    expected_trace: int,
    projection_audit: Mapping[str, Any],
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    params = dict(base)
    params["ber_guard_thresh"] = 1.2
    params["ber_spread_mult"] = 2.0
    params["ber_exposure_add_only"] = arm == ARMS[1]
    cpp_result = bt._simulate_tick_with_engine(
        "cpp",
        window.trades,
        window.var_ts_ms,
        window.var_ssq,
        params,
        **shared,
    )
    python_result = bt._simulate_tick_with_engine(
        "python",
        window.trades,
        window.var_ts_ms,
        window.var_ssq,
        params,
        campaign_repair_model=native_runner._CampaignMaeTraceProbe(),
        **shared,
    )
    native_runner._assert_cpp_python_fill_path_lockstep(cpp_result, python_result)
    _assert_ber_cpp_python_lockstep(cpp_result, python_result)
    trace = python_result.get("_campaign_repair_trace")
    if not isinstance(trace, list):
        raise BerRoleSafeError("Python campaign MAE probe did not emit a trace")
    result = dict(cpp_result)
    result["_campaign_repair_trace"] = trace
    result["_campaign_mae_trace_audit"] = {
        "source": "python_probe_locked_to_cpp_fill_path_and_ber_state",
        "trace_campaign_repair_max": expected_trace,
        "trace_row_count": len(trace),
        "cpp_python_fill_path_mismatch_count": 0,
        "cpp_python_ber_state_mismatch_count": 0,
    }
    summary, campaigns, fills = native_runner._project_arm(
        day=day,
        arm=arm,
        result=result,
        order_size=float(base["order_size"]),
        campaign_mae_trace_max=expected_trace,
    )
    summary.update(
        {
            "ber_guard_thresh": 1.2,
            "ber_spread_mult": 2.0,
            "ber_exposure_add_only": bool(params["ber_exposure_add_only"]),
            "ber_clock_identity": (
                "live_held_completed_10s_feature_sampled_on_completed_1s_callback.v1"
            ),
            "ber_active_count": int(result.get("ber_active_count", 0)),
            "ber_active_rate": float(result.get("ber_active_rate", 0.0)),
            "ber_feature_publish_count": int(result.get("ber_feature_publish_count", 0)),
            "ber_held_input_end": float(result.get("ber_held_input_end", 0.0)),
            "ber_ema_fast_end": float(result.get("ber_ema_fast_end", 0.0)),
            "ber_ema_slow_end": float(result.get("ber_ema_slow_end", 0.0)),
            "ber_active_end": bool(result.get("ber_active_end", False)),
            "ber_role_safe_decision_count": int(result.get("ber_role_safe_decision_count", 0)),
            "ber_role_safe_buy_add_count": int(result.get("ber_role_safe_buy_add_count", 0)),
            "ber_role_safe_sell_add_count": int(result.get("ber_role_safe_sell_add_count", 0)),
            "ber_role_safe_flat_bypass_count": int(result.get("ber_role_safe_flat_bypass_count", 0)),
            "ber_role_safe_mixed_fail_closed_count": int(result.get("ber_role_safe_mixed_fail_closed_count", 0)),
            "ber_role_safe_pair_change_count": int(result.get("ber_role_safe_pair_change_count", 0)),
            "ber_role_safe_bid_change_count": int(result.get("ber_role_safe_bid_change_count", 0)),
            "ber_role_safe_ask_change_count": int(result.get("ber_role_safe_ask_change_count", 0)),
            "ber_role_safe_source_mismatch_count": int(result.get("ber_role_safe_source_mismatch_count", 0)),
            "ber_role_safe_cap_collision_count": int(result.get("ber_role_safe_cap_collision_count", 0)),
            "ber_role_safe_cap_infeasible_count": int(result.get("ber_role_safe_cap_infeasible_count", 0)),
            "n_requotes": int(result.get("n_requotes", 0)),
            "avg_spread": float(result.get("avg_spread", 0.0)),
            "avg_final_spread": float(result.get("avg_final_spread", 0.0)),
            "terminal_mark_price_usdc_per_btc": float(result.get("terminal_mark_price", 0.0)),
            "cpp_python_ber_state_mismatch_count": 0,
            "offline_projection_sha256": projection_audit["projection_contract_sha256"],
        }
    )
    if arm == ARMS[0] and any(
        int(summary[field]) != 0
        for field in (
            "ber_role_safe_decision_count",
            "ber_role_safe_pair_change_count",
            "ber_role_safe_source_mismatch_count",
        )
    ):
        raise BerRoleSafeError(f"{day} control entered the role-safe policy")
    return summary, campaigns, fills


def _day_manifest(output: Path, day: str) -> Path:
    return output / "days" / day / "manifest.json"


def _admitted_day(
    output: Path,
    day: str,
    *,
    execution_amendment_path: Path,
) -> dict[str, Any] | None:
    manifest_path = _day_manifest(output, day)
    marker = manifest_path.parent / DAY_SUCCESS
    if not manifest_path.is_file() or not marker.is_file():
        return None
    if marker.read_text(encoding="ascii").strip() != native_runner._sha256_file(manifest_path):
        raise BerRoleSafeError(f"{day} atomic admission marker drifted")
    payload = _load_json(manifest_path, role=f"{day} manifest")
    expected = {
        "schema_version": f"{SCHEMA_VERSION}.day",
        "identity": IDENTITY,
        "spec_sha256": native_runner._sha256_file(SPEC),
        "execution_amendment_sha256": native_runner._sha256_file(execution_amendment_path),
        "offline_projection_sha256": native_runner._sha256_file(OFFLINE_PROJECTION),
    }
    for field, value in expected.items():
        if payload.get(field) != value:
            raise BerRoleSafeError(f"{day} manifest {field} drifted")
    for role in ("summary", "campaigns", "fills"):
        row = payload.get(role) or {}
        _validate_artifact(Path(str(row.get("path", ""))), str(row.get("sha256", "")), role=f"{day} {role}")
    return payload


def _checkpoint_dir(output: Path, day: str, arm: str) -> Path:
    return output / ".arm-checkpoints" / day / arm


def _load_checkpoint(
    output: Path,
    *,
    day: str,
    arm: str,
    window_sha256: str,
    execution_amendment_path: Path,
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame] | None:
    directory = _checkpoint_dir(output, day, arm)
    manifest_path = directory / "manifest.json"
    marker = directory / ARM_SUCCESS
    if not manifest_path.is_file() or not marker.is_file():
        return None
    if marker.read_text(encoding="ascii").strip() != native_runner._sha256_file(manifest_path):
        raise BerRoleSafeError(f"{day} {arm} checkpoint marker drifted")
    manifest = _load_json(manifest_path, role=f"{day} {arm} checkpoint")
    expected = {
        "schema_version": f"{SCHEMA_VERSION}.arm_checkpoint",
        "identity": IDENTITY,
        "day": day,
        "arm": arm,
        "spec_sha256": native_runner._sha256_file(SPEC),
        "execution_amendment_sha256": native_runner._sha256_file(execution_amendment_path),
        "offline_projection_sha256": native_runner._sha256_file(OFFLINE_PROJECTION),
        "window_sha256": window_sha256,
    }
    for field, value in expected.items():
        if manifest.get(field) != value:
            raise BerRoleSafeError(f"{day} {arm} checkpoint {field} drifted")
    for role in ("summary", "campaigns", "fills"):
        row = manifest.get(role) or {}
        _validate_artifact(Path(str(row.get("path", ""))), str(row.get("sha256", "")), role=f"{day} {arm} checkpoint {role}")
    return (
        _load_json(Path(manifest["summary"]["path"]), role=f"{day} {arm} summary"),
        pd.read_parquet(manifest["campaigns"]["path"]),
        pd.read_parquet(manifest["fills"]["path"]),
    )


def _write_checkpoint(
    output: Path,
    *,
    day: str,
    arm: str,
    window_sha256: str,
    execution_amendment_path: Path,
    summary: dict[str, Any],
    campaigns: pd.DataFrame,
    fills: pd.DataFrame,
) -> None:
    staging = output / ".arm-checkpoint-staging" / f"{day}-{arm}-{uuid.uuid4().hex}"
    final = _checkpoint_dir(output, day, arm)
    staging.mkdir(parents=True, exist_ok=False)
    try:
        summary_path = staging / "summary.json"
        campaigns_path = staging / "campaigns.parquet"
        fills_path = staging / "fills.parquet"
        _atomic_json(summary_path, summary)
        campaigns.to_parquet(campaigns_path, index=False, compression="zstd")
        fills.to_parquet(fills_path, index=False, compression="zstd")
        manifest = {
            "schema_version": f"{SCHEMA_VERSION}.arm_checkpoint",
            "identity": IDENTITY,
            "day": day,
            "arm": arm,
            "spec_sha256": native_runner._sha256_file(SPEC),
            "execution_amendment_sha256": native_runner._sha256_file(execution_amendment_path),
            "offline_projection_sha256": native_runner._sha256_file(OFFLINE_PROJECTION),
            "window_sha256": window_sha256,
            "summary": {"path": str(final / summary_path.name), "sha256": native_runner._sha256_file(summary_path)},
            "campaigns": {"path": str(final / campaigns_path.name), "sha256": native_runner._sha256_file(campaigns_path)},
            "fills": {"path": str(final / fills_path.name), "sha256": native_runner._sha256_file(fills_path)},
            "temporary_not_finalizer_eligible": True,
        }
        manifest_path = staging / "manifest.json"
        _atomic_json(manifest_path, manifest)
        _atomic_text(staging / ARM_SUCCESS, native_runner._sha256_file(manifest_path) + "\n")
        final.parent.mkdir(parents=True, exist_ok=True)
        if final.exists():
            raise BerRoleSafeError(f"concurrent checkpoint appeared for {day} {arm}")
        os.replace(staging, final)
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def execute_day(
    day: str,
    *,
    plan_path: Path = DEFAULT_PLAN,
    f03_amendment_path: Path = DEFAULT_F03_AMENDMENT,
    execution_amendment_path: Path = DEFAULT_EXECUTION_AMENDMENT,
    output: Path = DEFAULT_OUTPUT,
) -> dict[str, Any]:
    existing = _admitted_day(
        output, day, execution_amendment_path=execution_amendment_path
    )
    if existing is not None:
        return {"day": day, "reused": True}
    spec, plan, _ = validate_spec(
        plan_path=plan_path,
        f03_amendment_path=f03_amendment_path,
        execution_amendment_path=execution_amendment_path,
        requested_days=[day],
        verify_all_windows=False,
    )
    rows = {row["utc_day"]: row for row in plan["identity_payload"]["days"]}
    row = rows[day]
    payload = plan["identity_payload"]
    control_schedule = control_repair.load_admitted_control_schedule(
        Path(payload["control_sources"]["path"]),
        panel_sha256=payload["control_sources"]["sha256"],
        panel_identity_sha256=payload["control_sources"]["panel_identity_sha256"],
        day=day,
    )
    window_path = Path(row["window"]["path"])
    _validate_artifact(window_path, row["window"]["sha256"], role=f"{day} market window")
    window = native_runner._load_bound_window(window_path)
    base, projection_audit = _load_offline_params(ROOT / spec["baseline"]["config_path"])
    expected_trace = int(payload["execution_amendment"]["trace_campaign_repair_max"])
    native_runner._validate_campaign_mae_trace_capacity(base, expected=expected_trace)
    shared = {
        "ml_data": control_schedule.ml_data,
        "bbo_data": window.bbo_data,
        "l2_data": window.l2_data,
        "var_ti": window.var_ti,
        "var_retsq": window.var_retsq,
    }
    summaries: list[dict[str, Any]] = []
    campaign_frames: list[pd.DataFrame] = []
    fill_frames: list[pd.DataFrame] = []
    checkpoint_reuse: dict[str, bool] = {}
    for arm in ARMS:
        checkpoint = _load_checkpoint(
            output,
            day=day,
            arm=arm,
            window_sha256=row["window"]["sha256"],
            execution_amendment_path=execution_amendment_path,
        )
        if checkpoint is None:
            summary, campaigns, fills = _simulate_arm(
                day=day,
                arm=arm,
                window=window,
                base=base,
                shared=shared,
                expected_trace=expected_trace,
                projection_audit=projection_audit,
            )
            _write_checkpoint(
                output,
                day=day,
                arm=arm,
                window_sha256=row["window"]["sha256"],
                execution_amendment_path=execution_amendment_path,
                summary=summary,
                campaigns=campaigns,
                fills=fills,
            )
            reused = False
        else:
            summary, campaigns, fills = checkpoint
            reused = True
        summaries.append(summary)
        campaign_frames.append(campaigns)
        fill_frames.append(fills)
        checkpoint_reuse[arm] = reused
    by_arm = {row["arm"]: row for row in summaries}
    control = by_arm[ARMS[0]]
    candidate = by_arm[ARMS[1]]
    state_fields = (
        "ber_active_count",
        "ber_feature_publish_count",
        "ber_held_input_end",
        "ber_ema_fast_end",
        "ber_ema_slow_end",
        "ber_active_end",
    )
    arm_state_mismatches = sum(control[field] != candidate[field] for field in state_fields)
    requote_mismatch = int(control["n_requotes"] != candidate["n_requotes"])
    day_mechanics = {
        "ber_state_arm_mismatch_count": int(arm_state_mismatches),
        "common_requote_count_mismatch": requote_mismatch,
        "candidate_pair_change_count": int(candidate["ber_role_safe_pair_change_count"]),
        "candidate_bid_change_count": int(candidate["ber_role_safe_bid_change_count"]),
        "candidate_ask_change_count": int(candidate["ber_role_safe_ask_change_count"]),
        "candidate_source_mismatch_count": int(candidate["ber_role_safe_source_mismatch_count"]),
        "candidate_cap_infeasible_count": int(candidate["ber_role_safe_cap_infeasible_count"]),
        "candidate_effective_side_change_rate": (
            (candidate["ber_role_safe_bid_change_count"] + candidate["ber_role_safe_ask_change_count"])
            / max(2 * control["n_requotes"], 1)
        ),
        "python_cpp_ber_state_mismatch_count": int(
            sum(row["cpp_python_ber_state_mismatch_count"] for row in summaries)
        ),
        "python_cpp_fill_path_mismatch_count": int(
            sum(row["campaign_mae_cpp_python_fill_path_mismatch_count"] for row in summaries)
        ),
    }
    staging = output / ".staging" / f"{day}-{uuid.uuid4().hex}"
    staging.mkdir(parents=True, exist_ok=False)
    try:
        summary_path = staging / "summary.json"
        campaigns_path = staging / "campaigns.parquet"
        fills_path = staging / "fills.parquet"
        _atomic_json(
            summary_path,
            {
                "schema_version": f"{SCHEMA_VERSION}.day",
                "identity": IDENTITY,
                "day": day,
                "arms": summaries,
                "mechanics": day_mechanics,
                "arm_checkpoint_reused": checkpoint_reuse,
                "offline_execution_projection": projection_audit,
                "economic_outcomes_read": True,
                "validation_read": False,
                "sealed_holdout_read": False,
                "action_authorized": False,
                "live_authorized": False,
            },
        )
        pd.concat(campaign_frames, ignore_index=True).to_parquet(campaigns_path, index=False, compression="zstd")
        nonempty = [frame for frame in fill_frames if not frame.empty]
        (pd.concat(nonempty, ignore_index=True) if nonempty else pd.DataFrame()).to_parquet(fills_path, index=False, compression="zstd")
        final = output / "days" / day
        manifest = {
            "schema_version": f"{SCHEMA_VERSION}.day",
            "identity": IDENTITY,
            "day": day,
            "spec_sha256": native_runner._sha256_file(SPEC),
            "execution_amendment_sha256": native_runner._sha256_file(execution_amendment_path),
            "offline_projection_sha256": native_runner._sha256_file(OFFLINE_PROJECTION),
            "window_sha256": row["window"]["sha256"],
            "summary": {"path": str(final / summary_path.name), "sha256": native_runner._sha256_file(summary_path)},
            "campaigns": {"path": str(final / campaigns_path.name), "sha256": native_runner._sha256_file(campaigns_path)},
            "fills": {"path": str(final / fills_path.name), "sha256": native_runner._sha256_file(fills_path)},
        }
        manifest_path = staging / "manifest.json"
        _atomic_json(manifest_path, manifest)
        _atomic_text(staging / DAY_SUCCESS, native_runner._sha256_file(manifest_path) + "\n")
        final.parent.mkdir(parents=True, exist_ok=True)
        if final.exists():
            raise BerRoleSafeError(f"concurrent output appeared for {day}")
        os.replace(staging, final)
        shutil.rmtree(output / ".arm-checkpoints" / day, ignore_errors=True)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return {"day": day, "reused": False, "mechanics": day_mechanics}


def mechanics_summary(
    days: Sequence[str],
    *,
    output: Path,
    execution_amendment_path: Path,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for day in days:
        manifest = _admitted_day(
            output, day, execution_amendment_path=execution_amendment_path
        )
        if manifest is None:
            raise BerRoleSafeError(f"missing mechanics day: {day}")
        payload = _load_json(Path(manifest["summary"]["path"]), role=f"{day} summary")
        rows.append(payload["mechanics"])
    aggregate = {
        key: sum(float(row[key]) for row in rows)
        for key in (
            "ber_state_arm_mismatch_count",
            "common_requote_count_mismatch",
            "candidate_pair_change_count",
            "candidate_bid_change_count",
            "candidate_ask_change_count",
            "candidate_source_mismatch_count",
            "candidate_cap_infeasible_count",
            "python_cpp_ber_state_mismatch_count",
            "python_cpp_fill_path_mismatch_count",
        )
    }
    zero_fields = (
        "ber_state_arm_mismatch_count",
        "common_requote_count_mismatch",
        "candidate_source_mismatch_count",
        "candidate_cap_infeasible_count",
        "python_cpp_ber_state_mismatch_count",
        "python_cpp_fill_path_mismatch_count",
    )
    structural_pass = all(aggregate[field] == 0 for field in zero_fields) and (
        aggregate["candidate_pair_change_count"] > 0
        and aggregate["candidate_bid_change_count"] > 0
        and aggregate["candidate_ask_change_count"] > 0
    )
    return {
        "identity": IDENTITY,
        "days": list(days),
        "aggregate": aggregate,
        "structural_mechanics_passed": structural_pass,
        "remaining_40day_support_gates_not_evaluated": True,
        "action_authorized": False,
        "live_authorized": False,
    }


def _paired(
    daily: pd.DataFrame,
    column: str,
    *,
    candidate_minus_control: bool,
    seed: int,
    draws: int,
) -> dict[str, Any]:
    wide = daily.pivot(index="day", columns="arm", values=column).sort_index()
    if set(wide.columns) != set(ARMS) or wide.isna().any().any():
        raise BerRoleSafeError(f"paired metric is incomplete: {column}")
    control = wide[ARMS[0]].to_numpy(dtype=float)
    candidate = wide[ARMS[1]].to_numpy(dtype=float)
    values = candidate - control if candidate_minus_control else control - candidate
    return native_runner._bootstrap(values, draws=draws, seed=seed)


def finalize(
    *,
    plan_path: Path = DEFAULT_PLAN,
    f03_amendment_path: Path = DEFAULT_F03_AMENDMENT,
    execution_amendment_path: Path = DEFAULT_EXECUTION_AMENDMENT,
    output: Path = DEFAULT_OUTPUT,
) -> dict[str, Any]:
    spec, _, _ = validate_spec(
        plan_path=plan_path,
        f03_amendment_path=f03_amendment_path,
        execution_amendment_path=execution_amendment_path,
        requested_days=None,
    )
    days = list(spec["development_panel"]["days"])
    manifests = [
        _admitted_day(output, day, execution_amendment_path=execution_amendment_path)
        for day in days
    ]
    if any(row is None for row in manifests):
        missing = [day for day, row in zip(days, manifests, strict=True) if row is None]
        raise BerRoleSafeError(f"cannot finalize; missing days: {missing}")
    summaries: list[dict[str, Any]] = []
    campaigns: list[pd.DataFrame] = []
    fills: list[pd.DataFrame] = []
    day_mechanics: list[dict[str, Any]] = []
    for manifest in manifests:
        assert manifest is not None
        payload = _load_json(Path(manifest["summary"]["path"]), role="day summary")
        summaries.extend(payload["arms"])
        day_mechanics.append({"day": payload["day"], **payload["mechanics"]})
        campaigns.append(pd.read_parquet(manifest["campaigns"]["path"]))
        fills.append(pd.read_parquet(manifest["fills"]["path"]))
    daily = pd.DataFrame(summaries)
    campaign_frame = pd.concat(campaigns, ignore_index=True)
    fill_frame = pd.concat(fills, ignore_index=True)
    for side in ("LONG", "SHORT"):
        name = f"multi_level_{side.lower()}_terminal_value_usdc"
        values = (
            campaign_frame.loc[
                campaign_frame["multi_level"].astype(bool)
                & campaign_frame["inventory_side"].eq(side)
            ]
            .groupby(["day", "arm"])["terminal_value_usdc"]
            .sum()
        )
        daily[name] = [float(values.get((row.day, row.arm), 0.0)) for row in daily.itertuples()]
    draws = int(spec["comparison"]["bootstrap_draws"])
    seed = int(spec["comparison"]["bootstrap_seed"])
    metric_defs = (
        ("closed_campaign_value", "closed_campaign_value_usdc", True),
        ("conditional_net_value", "terminal_mtm_pnl_usdc", True),
        ("full_panel_continuous_mtm", "terminal_mtm_pnl_usdc", True),
        ("negative_terminal_protection", "negative_campaign_terminal_value_usdc", True),
        ("q10_shortfall_protection", "campaign_q10_usdc", True),
        ("campaign_cvar10_protection", "campaign_cvar10_usdc", True),
        ("campaign_mae_avoidance", "campaign_mae_usdc", True),
        ("maximum_inventory_avoidance", "max_inventory_btc", False),
        ("inventory_time_avoidance", "abs_inventory_time_btc_s", False),
        ("buy_maker_value_protection_bps", "buy_maker_value_30s_bps", True),
        ("sell_maker_value_protection_bps", "sell_maker_value_30s_bps", True),
        ("repair_event", "repair_event_rate", True),
        ("repair_time_avoidance_s", "mean_closed_repair_time_s", False),
        ("multi_level_long_protection", "multi_level_long_terminal_value_usdc", True),
        ("multi_level_short_protection", "multi_level_short_terminal_value_usdc", True),
    )
    metrics = {
        name: _paired(
            daily,
            column,
            candidate_minus_control=direction,
            seed=seed + index,
            draws=draws,
        )
        for index, (name, column, direction) in enumerate(metric_defs)
    }
    totals = daily.groupby("arm", sort=False).sum(numeric_only=True)
    fill_retention = float(totals.loc[ARMS[1], "fills_total"] / max(totals.loc[ARMS[0], "fills_total"], 1.0))
    metrics["fills_retention"] = {"estimate": fill_retention}
    candidate = daily.loc[daily["arm"].eq(ARMS[1])]
    control = daily.loc[daily["arm"].eq(ARMS[0])]
    if not (candidate["n_requotes"].to_numpy() == control["n_requotes"].to_numpy()).all():
        common_requote_mismatch = 1
    else:
        common_requote_mismatch = 0
    control_requotes = int(control["n_requotes"].sum())
    bid_changes = int(candidate["ber_role_safe_bid_change_count"].sum())
    ask_changes = int(candidate["ber_role_safe_ask_change_count"].sum())
    effective_change_rate = (bid_changes + ask_changes) / max(2 * control_requotes, 1)
    bid_days = int((candidate["ber_role_safe_bid_change_count"] > 0).sum())
    ask_days = int((candidate["ber_role_safe_ask_change_count"] > 0).sum())
    mechanics_spec = spec["mechanics_gates"]
    mechanics_gates = {
        "common_requote_denominator_parity": common_requote_mismatch == 0,
        "python_cpp_fill_path_parity": int(daily["campaign_mae_cpp_python_fill_path_mismatch_count"].sum()) == 0,
        "python_cpp_ber_state_parity": int(daily["cpp_python_ber_state_mismatch_count"].sum()) == 0,
        "control_candidate_ber_state_parity": int(sum(row["ber_state_arm_mismatch_count"] for row in day_mechanics)) == 0,
        "role_source_parity": int(candidate["ber_role_safe_source_mismatch_count"].sum()) == 0,
        "cap_infeasible_zero": int(candidate["ber_role_safe_cap_infeasible_count"].sum()) == 0,
        "effective_change_rate_supported": float(mechanics_spec["candidate_pair_change_rate_minimum"]) <= effective_change_rate <= float(mechanics_spec["candidate_pair_change_rate_maximum"]),
        "buy_effective_change_count_supported": bid_changes >= int(mechanics_spec["minimum_effective_changes_per_side"]),
        "sell_effective_change_count_supported": ask_changes >= int(mechanics_spec["minimum_effective_changes_per_side"]),
        "buy_effective_change_days_supported": bid_days >= int(mechanics_spec["minimum_effective_change_days_per_side"]),
        "sell_effective_change_days_supported": ask_days >= int(mechanics_spec["minimum_effective_change_days_per_side"]),
    }
    effective_rows = int(campaign_frame.groupby("arm").size().min())
    last_candidate = candidate.sort_values("day").iloc[-1]
    final_inventory = float(last_candidate["final_inventory_btc"])
    final_mark = float(last_candidate["terminal_mark_price_usdc_per_btc"])
    evidence = {
        "schema_version": experiment_scorecard_v2.CANONICAL_EVIDENCE_SCHEMA_VERSION,
        "experiment_id": IDENTITY,
        "family_id": "F09_campaign_action_uplift",
        "panel_role": "development",
        "input_identity": {
            "spec_sha256": native_runner._sha256_file(SPEC),
            "execution_amendment_sha256": native_runner._sha256_file(execution_amendment_path),
            "reused_execution_plan_sha256": native_runner._sha256_file(plan_path),
        },
        "score_profile_contract": experiment_scorecard_v2.score_profile_contract("action_defense_v2"),
        "validity_failures": ["daily_fresh_start_is_not_continuous_live_promotion_authority"],
        "family_gate_failures": [] if all(mechanics_gates.values()) else ["role_safe_ber_mechanics_gate_failed"],
        "metrics": metrics,
        "n_rows": effective_rows,
        "n_days": len(days),
        "effective_sample_size": float(effective_rows),
        "minimum_behavior_propensity": 0.5,
        "unsupported_mass": 0.0,
        "overlap_violations": 0,
        "candidate_rate": effective_change_rate,
        "invariant_violations": [],
        "continuous_path_accounting": {
            "schema_version": experiment_scorecard_v2.CONTINUOUS_PATH_SCHEMA_VERSION,
            "utc_day_role": "bootstrap_cluster_only",
            "cash_carried_across_utc_days": False,
            "inventory_carried_across_utc_days": False,
            "campaign_state_carried_across_utc_days": False,
            "panel_final_inventory_mtm_included": True,
            "forced_day_end_liquidations": 0,
            "day_end_state_resets": len(days) - 1,
            "day_end_campaign_terminals": 0,
            "daily_pnl_sum_usdc": metrics["conditional_net_value"]["sum_delta"],
            "continuous_panel_pnl_usdc": metrics["conditional_net_value"]["sum_delta"],
            "daily_accounting_identity_max_abs_error_usdc": float(daily["campaign_accounting_error_usdc"].abs().max()),
            "panel_final_inventory_btc": final_inventory,
            "panel_final_mark_price_usdc_per_btc": final_mark,
            "panel_final_inventory_mtm_usdc": final_inventory * final_mark,
        },
    }
    raw_scorecard = experiment_scorecard_v2.score_canonical_evidence(evidence, profile_id="action_defense_v2")
    gates = spec["noncompensable_economic_gates"]
    fill_range = list(gates["fill_retention_range"])
    economic_gates = {
        "terminal_mtm_lcb_positive": metrics["conditional_net_value"]["lower_bound"] > 0.0,
        "closed_campaign_lcb_positive": metrics["closed_campaign_value"]["lower_bound"] > 0.0,
        "daily_positive_rate_pass": metrics["conditional_net_value"]["daily_positive_rate"] >= float(gates["daily_positive_rate_minimum"]),
        "negative_terminal_lcb_nonnegative": metrics["negative_terminal_protection"]["lower_bound"] >= float(gates["negative_terminal_protection_lcb_minimum"]),
        "campaign_q10_lcb_nonnegative": metrics["q10_shortfall_protection"]["lower_bound"] >= float(gates["campaign_q10_delta_lcb_minimum"]),
        "campaign_cvar10_lcb_nonnegative": metrics["campaign_cvar10_protection"]["lower_bound"] >= float(gates["campaign_cvar10_delta_lcb_minimum"]),
        "campaign_mae_lcb_nonnegative": metrics["campaign_mae_avoidance"]["lower_bound"] >= float(gates["campaign_mae_avoidance_lcb_minimum"]),
        "maximum_inventory_lcb_nonnegative": metrics["maximum_inventory_avoidance"]["lower_bound"] >= float(gates["maximum_inventory_avoidance_lcb_minimum"]),
        "inventory_time_lcb_nonnegative": metrics["inventory_time_avoidance"]["lower_bound"] >= float(gates["inventory_time_avoidance_lcb_minimum"]),
        "buy_maker_value_lcb_pass": metrics["buy_maker_value_protection_bps"]["lower_bound"] >= float(gates["buy_maker_value_delta_lcb_minimum_bps"]),
        "sell_maker_value_lcb_pass": metrics["sell_maker_value_protection_bps"]["lower_bound"] >= float(gates["sell_maker_value_delta_lcb_minimum_bps"]),
        "fill_retention_owner_range": float(fill_range[0]) <= fill_retention <= float(fill_range[1]),
        "campaign_accounting_parity": float(daily["campaign_accounting_error_usdc"].abs().max()) <= float(gates["campaign_accounting_max_abs_error_usdc"]),
    }
    screen_passed = all(mechanics_gates.values()) and all(economic_gates.values())
    decision = (
        "advance_to_restart_aware_continuous_owner_confirmation"
        if screen_passed
        else "close_role_safe_ber_candidate_on_development"
    )
    report = {
        "schema_version": f"{SCHEMA_VERSION}.report",
        "identity": IDENTITY,
        "decision": decision,
        "comparison": "role_safe_add_only_minus_global_all_roles_ber",
        "days": days,
        "totals": {
            arm: {
                "terminal_mtm_pnl_usdc": float(totals.loc[arm, "terminal_mtm_pnl_usdc"]),
                "closed_campaign_value_usdc": float(totals.loc[arm, "closed_campaign_value_usdc"]),
                "fills_bid": int(totals.loc[arm, "fills_bid"]),
                "fills_ask": int(totals.loc[arm, "fills_ask"]),
                "fills_total": int(totals.loc[arm, "fills_total"]),
                "multi_level_long_terminal_value_usdc": float(totals.loc[arm, "multi_level_long_terminal_value_usdc"]),
                "multi_level_short_terminal_value_usdc": float(totals.loc[arm, "multi_level_short_terminal_value_usdc"]),
            }
            for arm in ARMS
        },
        "mechanics": {
            "control_requotes": control_requotes,
            "candidate_effective_side_change_rate": effective_change_rate,
            "candidate_bid_change_count": bid_changes,
            "candidate_ask_change_count": ask_changes,
            "candidate_bid_change_days": bid_days,
            "candidate_ask_change_days": ask_days,
            "candidate_pair_change_count": int(candidate["ber_role_safe_pair_change_count"].sum()),
            "candidate_role_safe_decision_count": int(candidate["ber_role_safe_decision_count"].sum()),
            "candidate_buy_add_count": int(candidate["ber_role_safe_buy_add_count"].sum()),
            "candidate_sell_add_count": int(candidate["ber_role_safe_sell_add_count"].sum()),
            "candidate_flat_bypass_count": int(candidate["ber_role_safe_flat_bypass_count"].sum()),
            "candidate_mixed_fail_closed_count": int(candidate["ber_role_safe_mixed_fail_closed_count"].sum()),
            "candidate_cap_collision_count": int(candidate["ber_role_safe_cap_collision_count"].sum()),
            "candidate_cap_infeasible_count": int(candidate["ber_role_safe_cap_infeasible_count"].sum()),
        },
        "metrics": metrics,
        "mechanics_gates": mechanics_gates,
        "economic_gates": economic_gates,
        "development_screen_passed": screen_passed,
        "raw_action_defense_v2_scorecard": raw_scorecard,
        "daily_fresh_start_not_live_authority": True,
        "continuous_confirmation_required_before_config_change": True,
        "owner_risk_accepted_route": True,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
        "ranking_score": None,
    }
    output.mkdir(parents=True, exist_ok=True)
    artifacts = {
        "daily": output / "daily.parquet",
        "campaigns": output / "campaigns.parquet",
        "fills": output / "fills.parquet",
        "scorecard": output / "action-defense-v2-scorecard.json",
        "report": output / "report.json",
    }
    daily.to_parquet(artifacts["daily"], index=False, compression="zstd")
    campaign_frame.to_parquet(artifacts["campaigns"], index=False, compression="zstd")
    fill_frame.to_parquet(artifacts["fills"], index=False, compression="zstd")
    _atomic_json(artifacts["scorecard"], raw_scorecard)
    _atomic_json(artifacts["report"], report)
    manifest = {
        "schema_version": f"{SCHEMA_VERSION}.panel",
        "identity": IDENTITY,
        "spec_sha256": native_runner._sha256_file(SPEC),
        "execution_amendment_sha256": native_runner._sha256_file(execution_amendment_path),
        "artifacts": {
            name: {"path": str(path), "sha256": native_runner._sha256_file(path)}
            for name, path in artifacts.items()
        },
    }
    manifest_path = output / "panel-manifest.json"
    _atomic_json(manifest_path, manifest)
    _atomic_text(output / PANEL_SUCCESS, native_runner._sha256_file(manifest_path) + "\n")
    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("preflight", "run", "mechanics", "finalize", "all"))
    parser.add_argument("--plan", type=Path, default=DEFAULT_PLAN)
    parser.add_argument("--f03-amendment", type=Path, default=DEFAULT_F03_AMENDMENT)
    parser.add_argument("--execution-amendment", type=Path, default=DEFAULT_EXECUTION_AMENDMENT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--days", nargs="*")
    args = parser.parse_args(argv)
    spec = _load_json(SPEC, role="frozen role-safe BER spec")
    days = list(args.days or spec["development_panel"]["days"])
    validate_spec(
        plan_path=args.plan,
        f03_amendment_path=args.f03_amendment,
        execution_amendment_path=args.execution_amendment,
        requested_days=days,
    )
    storage = _storage_gate(args.output)
    if args.command == "preflight":
        result: Any = {
            "identity": IDENTITY,
            "status": "preflight_passed",
            "days": days,
            "storage": storage,
            "action_authorized": False,
            "live_authorized": False,
        }
    elif args.command == "mechanics":
        result = mechanics_summary(
            days,
            output=args.output,
            execution_amendment_path=args.execution_amendment,
        )
    else:
        result = {}
        if args.command in {"run", "all"}:
            workers = max(1, int(args.workers))
            if workers == 1:
                rows = [
                    execute_day(
                        day,
                        plan_path=args.plan,
                        f03_amendment_path=args.f03_amendment,
                        execution_amendment_path=args.execution_amendment,
                        output=args.output,
                    )
                    for day in days
                ]
            else:
                with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as pool:
                    futures = {
                        pool.submit(
                            execute_day,
                            day,
                            plan_path=args.plan,
                            f03_amendment_path=args.f03_amendment,
                            execution_amendment_path=args.execution_amendment,
                            output=args.output,
                        ): day
                        for day in days
                    }
                    rows = []
                    for future in concurrent.futures.as_completed(futures):
                        row = future.result()
                        rows.append(row)
                        print(f"completed {row['day']} reused={row['reused']}", flush=True)
            result = {"status": "run_complete", "days": sorted(rows, key=lambda row: row["day"])}
        if args.command in {"finalize", "all"}:
            result = finalize(
                plan_path=args.plan,
                f03_amendment_path=args.f03_amendment,
                execution_amendment_path=args.execution_amendment,
                output=args.output,
            )
    print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
