#!/usr/bin/env python3
"""Full-path label runner for the Boolean EMA cooldown-duration study.

The formal Development denominator is every legal exposure-increasing fill
opportunity crossed with every frozen, side-specific duration action.  A
``--limit`` run is mechanics-only diagnostic evidence and is physically
separated from the formal checkpoint namespace.

This runner does not fit a model.  In particular, it does not reuse the closed
ADD-vs-WAIT target or its Ridge evaluator.  It reuses only source-bound window,
configuration, and cross-day continuation helpers from those runners; their
file hashes are recorded in every execution identity.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import math
import os
import re
import shutil
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from pyarrow import types as pa_types

from data_paths import data_root, resolve_portable_path
from models import backtest_tick as bt
from models.replay import f05_ema_provider_source_grid as source_grid
from research.families.f05_fill_quality_quote_ev.audit import (
    multiscale_ema_add_wait_incremental_value_v1_1_study as v1_1_helper,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    multiscale_ema_boolean_cooldown_duration as ema_contract,
)
from research.governance.public_machine_projection import (
    PublicMachineProjectionError,
    source_document_path,
    source_identity_sha256,
)

ROOT = Path(__file__).resolve().parents[4]
DATA_ROOT = data_root(ROOT)
IDENTITY = "multiscale_ema_boolean_cooldown_duration_policy_v1"
SCHEMA_VERSION = f"{IDENTITY}.full_path_study.v2"
OUTCOME_BLIND_INPUTS = ROOT / (
    "research/families/f05_fill_quality_quote_ev/docs/"
    "multiscale_ema_boolean_cooldown_duration_policy_v1_"
    "outcome_blind_inputs_20260809.json"
)
FROZEN_SPEC_JSON = ROOT / (
    "research/families/f05_fill_quality_quote_ev/docs/"
    "multiscale_ema_boolean_cooldown_duration_policy_v1_spec_20260809.json"
)
FROZEN_SPEC_MD = ROOT / (
    "research/families/f05_fill_quality_quote_ev/docs/"
    "multiscale_ema_boolean_cooldown_duration_policy_v1_spec_20260809.md"
)
CURRENT_BASELINE_LOCATOR = (
    "${NARROWGATE_PRIVATE_RESEARCH_ROOT}/"
    "current_live_held_ber_replay_baseline_40d_20260809.json"
)
PLAN = DATA_ROOT / (
    "cache/replay_dag/"
    "f03_causal_v12_1s_native_40day_full_path_ml_ab_v3/"
    "execution-plan.json"
)
DEFAULT_OUTPUT = DATA_ROOT / (
    "reports/"
    "multiscale_ema_boolean_cooldown_duration_policy_v1_20260809"
)

OUTCOME_BLIND_INPUTS_SHA256 = "965400c6fe5408a6f49dd4253c96d6673d4621451af561a2bc7921591c2d7035"
EXPECTED_FORMAL_OPPORTUNITIES = 8_600
EXPECTED_ACTIONS_PER_SIDE = 8
EXPECTED_FORMAL_ARM_ROWS = 68_800
EXPECTED_PREDICATE_COLUMNS = 360
FORMAL_REPLAY_ENGINE = "cpp"
PYTHON_PARITY_SCOPE = "diagnostic_limit_only"
FILL_CLOCK_SEMANTICS = (
    "native_exchange_event_revealed_at_replay_event_clock_no_live_receive_time_claim"
)

TRACE_LIMIT = 1_000_000
DEFAULT_CHUNK_SIZE = 8
DIAGNOSTIC_SEED = f"{IDENTITY}|mechanics-diagnostic-limit.v1"
CONTROL_NOOP_ATTESTATION = "control_noop_attestation.json"
FORBIDDEN_RESEARCH_ACTION_FLAGS = (
    "ema_add_wait_fork_enabled",
    "buy_soft_widen_release_probe_enabled",
    "conditional_p3_reach_gate_enabled",
    "conditional_p3_reach_budget_policy_enabled",
    "safe_add_rearm_randomized_enabled",
    "state_conditioned_rearm_enabled",
    "sell_add_skip_ope_enabled",
    "state_conditioned_quote_policy_enabled",
    "local_action_ope_enabled",
    "queue_value_keep_cancel_enabled",
    "variance_time_lineage_randomized_enabled",
)


class StudyError(RuntimeError):
    """Fail closed when a frozen duration study identity drifts."""


def _current_baseline_path() -> Path:
    try:
        return resolve_portable_path(CURRENT_BASELINE_LOCATOR, root=ROOT)
    except (RuntimeError, ValueError) as exc:
        raise StudyError(
            "current replay baseline requires NARROWGATE_PRIVATE_RESEARCH_ROOT"
        ) from exc


@dataclass(frozen=True, slots=True)
class DurationAction:
    policy_id: str
    engine_action: str
    fixed_duration_s: int | None
    duration_semantics: str

    @property
    def fixed_duration_ms(self) -> int | None:
        if self.fixed_duration_s is None:
            return None
        return _seconds_to_milliseconds(self.fixed_duration_s)

    def payload(self) -> dict[str, Any]:
        return {
            "policy_id": self.policy_id,
            "engine_action": self.engine_action,
            "fixed_duration_s": self.fixed_duration_s,
            "fixed_duration_ms": self.fixed_duration_ms,
            "duration_semantics": self.duration_semantics,
        }


def _seconds_to_milliseconds(duration_s: int) -> int:
    if isinstance(duration_s, bool) or not isinstance(duration_s, int) or duration_s <= 0:
        raise StudyError("frozen duration seconds must be a positive integer")
    duration_ms = duration_s * 1_000
    if duration_ms // 1_000 != duration_s:
        raise StudyError("frozen duration seconds overflowed milliseconds")
    return duration_ms


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _canonical_fill_prefix(
    rows: Sequence[Mapping[str, Any]], *, cutoff_ms: int
) -> tuple[tuple[Any, ...], ...]:
    """Project a replay fill path without interpreting its economic value."""

    projected: list[tuple[Any, ...]] = []
    for row in rows:
        if int(row["fill_ts"]) > int(cutoff_ms):
            continue
        projected.append(
            (
                int(row["order_id"]),
                str(row["side"]),
                int(row["fill_ts"]),
                float(row["quote_px"]),
                float(row["fill_qty"]),
                float(row["inventory_before_fill"]),
                float(row["inventory_after_fill"]),
            )
        )
    return tuple(projected)


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise StudyError(f"JSON artifact must be an object: {path}")
    return payload


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.tmp-{uuid.uuid4().hex}"
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.tmp-{uuid.uuid4().hex}"
    try:
        frame.to_parquet(temporary, index=False, compression="zstd")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.tmp-{uuid.uuid4().hex}"
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _source_identity(path: Path, *, role: str) -> str:
    try:
        return source_identity_sha256(path)
    except (OSError, PublicMachineProjectionError) as exc:
        raise StudyError(f"{role} source identity is unavailable: {path}") from exc


def _exact_source_document(path: Path, *, role: str) -> Path:
    try:
        return source_document_path(path, require_private=True)
    except (OSError, PublicMachineProjectionError) as exc:
        raise StudyError(f"{role} exact source is unavailable: {path}") from exc


def _load_source_json(path: Path, *, role: str) -> dict[str, Any]:
    return _load_json(_exact_source_document(path, role=role))


def _validate_file(path: Path, expected_sha256: str, *, role: str) -> None:
    if not path.is_file() or _source_identity(path, role=role) != str(expected_sha256):
        raise StudyError(f"{role} hash drifted: {path}")


def _dependency_bindings() -> list[dict[str, str]]:
    cpp_extension = Path(bt._load_cpp_tick_replay().__file__).resolve()
    current_baseline = _current_baseline_path()
    rows = (
        ("study_runner", Path(__file__)),
        ("python_full_path_replay", Path(bt.__file__)),
        ("cpp_duration_replay_source", ROOT / "cpp/narrowgate_cpp/tick_replay.cpp"),
        ("cpp_duration_replay_header", ROOT / "cpp/narrowgate_cpp/tick_replay.hpp"),
        (
            "cpp_duration_pybind_source",
            ROOT / "cpp/narrowgate_cpp/bindings_tick_replay.cpp",
        ),
        ("loaded_cpp_extension", cpp_extension),
        ("boolean_ema_contract", Path(ema_contract.__file__)),
        ("v1_1_read_only_window_helper", Path(v1_1_helper.__file__)),
        ("source_grid_ema_recurrence_helper", Path(source_grid.__file__)),
        ("outcome_blind_duration_inputs", OUTCOME_BLIND_INPUTS),
        ("frozen_study_spec_json", FROZEN_SPEC_JSON),
        ("frozen_study_spec_md", FROZEN_SPEC_MD),
        ("current_replay_baseline", current_baseline),
        ("native_execution_plan", PLAN),
    )
    bindings = []
    for role, path in rows:
        resolved = path.expanduser().resolve()
        if not resolved.is_file():
            raise StudyError(f"required dependency is missing: {resolved}")
        _exact_source_document(resolved, role=role)
        bindings.append(
            {
                "role": role,
                "path": str(resolved),
                "sha256": _source_identity(resolved, role=role),
            }
        )
    return bindings


def _cpp_duration_abi_status() -> dict[str, Any]:
    required_types = (
        "CooldownDurationOpportunityRow",
        "CooldownDurationForkTrace",
    )
    try:
        cpp = bt._load_cpp_tick_replay()
    except Exception as exc:
        return {
            "ready": False,
            "reason": f"cpp_extension_unavailable:{type(exc).__name__}:{exc}",
            "module_path": None,
            "missing_types": list(required_types),
        }
    missing_types = [name for name in required_types if not hasattr(cpp, name)]
    required_opportunity_fields = {
        "fill_clock_semantics",
        "live_receive_time_authority",
        "exposure_fill_ordinal",
        "fill_visible_ts_ms",
        "fill_exchange_ts_ms",
        "role_at_fill",
        "assignment_equity_usdc",
    }
    required_fork_fields = {
        "assignment_to_washout_value_usdc",
        "censor_time_mid_mark_usdc",
        "censor_time_executable_mark_usdc",
        "censor_marks_are_terminal_bounds",
        "arm_washout_complete",
        "right_censored",
        "pending_ack_count",
        "campaign_active",
        "cursor_owner_count",
        "hazard_owner_count",
        "washout_protocol",
        "control_path_exact_until_quarantine",
        "exposure_permission_change_count",
        "reducing_permission_control_checks",
        "reducing_quote_change_count",
        "second_assignment_count",
        "accounting_residual_usdc",
    }
    opportunity_fields = (
        set(dir(cpp.CooldownDurationOpportunityRow)) if not missing_types else set()
    )
    fork_fields = set(dir(cpp.CooldownDurationForkTrace)) if not missing_types else set()
    missing_fields = sorted(
        (required_opportunity_fields - opportunity_fields) | (required_fork_fields - fork_fields)
    )
    legacy_economic_fields = sorted(
        {
            "decision_to_terminal_value_usdc",
            "closed_campaign_value_usdc",
            "censor_time_marking_lower_usdc",
            "censor_time_marking_upper_usdc",
        }
        & fork_fields
    )
    ready = not missing_types and not missing_fields and not legacy_economic_fields
    return {
        "ready": ready,
        "reason": ("ready" if ready else "cooldown_duration_pybind_schema_incomplete_or_legacy"),
        "module_path": str(Path(getattr(cpp, "__file__", "")).resolve()),
        "missing_types": missing_types,
        "missing_fields": missing_fields,
        "legacy_economic_fields": legacy_economic_fields,
    }


def _require_cpp_duration_abi() -> dict[str, Any]:
    status = _cpp_duration_abi_status()
    if status["ready"] is not True:
        raise StudyError(
            "formal C++ cooldown-duration ABI is not ready; Python full-arm fallback is "
            f"forbidden ({status['reason']})"
        )
    return status


def _parity_manifest_path(output: Path) -> Path:
    return Path(output) / "cpp_python_parity_manifest.json"


def _parity_admission_status(output: Path, execution_identity_sha256: str) -> dict[str, Any]:
    path = _parity_manifest_path(output)
    if not path.is_file():
        return {
            "admitted": False,
            "reason": "parity_manifest_missing",
            "path": str(path),
        }
    manifest = _load_json(path)
    admitted = bool(
        manifest.get("identity") == IDENTITY
        and manifest.get("execution_identity_sha256") == execution_identity_sha256
        and manifest.get("status") == "cpp_python_parity_subset_admitted"
        and manifest.get("all_rows_match") is True
        and manifest.get("required_side_role_cells_present") is True
        and int(manifest.get("parity_arm_rows", 0)) > 0
    )
    return {
        "admitted": admitted,
        "reason": "admitted" if admitted else "parity_manifest_invalid_or_incomplete",
        "path": str(path),
        "sha256": _sha256_file(path),
    }


def _require_parity_admission(output: Path, execution_identity_sha256: str) -> None:
    status = _parity_admission_status(output, execution_identity_sha256)
    if status["admitted"] is not True:
        raise StudyError(
            "formal C++ run requires an admitted explicit Python parity subset "
            f"({status['reason']})"
        )


def _control_noop_admission_status(output: Path) -> dict[str, Any]:
    path = Path(output) / CONTROL_NOOP_ATTESTATION
    if not path.is_file():
        return {
            "admitted": False,
            "reason": "control_noop_attestation_missing",
            "path": str(path),
        }
    receipt = _load_json(path)
    current_baseline = _current_baseline_path()
    baseline = _load_source_json(current_baseline, role="current replay baseline")
    cpp_extension = Path(bt._load_cpp_tick_replay().__file__).resolve()
    cpp_runtime = receipt.get("cpp_runtime") or {}
    admitted = bool(
        receipt.get("identity") == IDENTITY
        and receipt.get("status") == "passed"
        and receipt.get("baseline_sha256")
        == _source_identity(current_baseline, role="current replay baseline")
        and receipt.get("backtest_tick_sha256") == _sha256_file(Path(bt.__file__))
        and int(receipt.get("day_count", 0)) == 40
        and int(receipt.get("fill_count", 0)) == int(baseline["economics"]["fills_total"])
        and receipt.get("all_fill_paths_equal") is True
        and receipt.get("all_daily_accounting_equal") is True
        and receipt.get("candidate_economic_outcomes_read") is False
        and cpp_runtime.get("extension_path") == str(cpp_extension)
        and cpp_runtime.get("extension_sha256") == _sha256_file(cpp_extension)
        and cpp_runtime.get("tick_replay_cpp_sha256")
        == _sha256_file(ROOT / "cpp/narrowgate_cpp/tick_replay.cpp")
        and cpp_runtime.get("tick_replay_hpp_sha256")
        == _sha256_file(ROOT / "cpp/narrowgate_cpp/tick_replay.hpp")
        and cpp_runtime.get("bindings_cpp_sha256")
        == _sha256_file(ROOT / "cpp/narrowgate_cpp/bindings_tick_replay.cpp")
    )
    return {
        "admitted": admitted,
        "reason": "admitted" if admitted else "control_noop_attestation_invalid",
        "path": str(path),
        "sha256": _sha256_file(path),
    }


def _require_control_noop_admission(output: Path) -> None:
    status = _control_noop_admission_status(output)
    if status["admitted"] is not True:
        raise StudyError(
            "formal C++ run requires the 40-day disabled-hook control no-op "
            f"attestation ({status['reason']})"
        )


def _duration_actions(contract: Mapping[str, Any], side: str) -> tuple[DurationAction, ...]:
    normalized_side = str(side).upper()
    raw = contract["duration_source"]["candidate_actions"].get(normalized_side)
    if not isinstance(raw, list) or not raw:
        raise StudyError(f"frozen duration actions are missing for {normalized_side}")
    actions: list[DurationAction] = []
    for row in raw:
        policy_id = str(row.get("policy_id", ""))
        duration = row.get("duration_s")
        if policy_id == "CONTROL_85N":
            if duration is not None:
                raise StudyError("CONTROL_85N must retain lineage-scaled duration")
            action = DurationAction(
                policy_id=policy_id,
                engine_action="CONTROL_85N",
                fixed_duration_s=None,
                duration_semantics=str(row.get("duration_semantics", "")),
            )
        else:
            if (
                not policy_id.startswith("FIXED_")
                or isinstance(duration, bool)
                or not isinstance(duration, int)
                or duration <= 0
            ):
                raise StudyError(f"invalid frozen duration action: {row}")
            action = DurationAction(
                policy_id=policy_id,
                engine_action="FIXED_DURATION_MS",
                fixed_duration_s=int(duration),
                duration_semantics=str(row.get("duration_semantics", "")),
            )
        actions.append(action)
    ids = [action.policy_id for action in actions]
    if ids[0] != "CONTROL_85N" or len(ids) != len(set(ids)):
        raise StudyError(f"{normalized_side} duration action identity drifted")
    return tuple(actions)


def _load_contract() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    _validate_file(
        OUTCOME_BLIND_INPUTS,
        OUTCOME_BLIND_INPUTS_SHA256,
        role="frozen outcome-blind duration inputs",
    )
    contract = _load_source_json(
        OUTCOME_BLIND_INPUTS, role="frozen outcome-blind duration inputs"
    )
    if (
        contract.get("identity") != IDENTITY
        or contract.get("schema_version") != f"{IDENTITY}.outcome_blind_inputs.v1"
    ):
        raise StudyError("outcome-blind duration identity drifted")
    permissions = contract.get("permissions") or {}
    if any(
        permissions.get(field) is not False
        for field in (
            "development_economic_labels_read",
            "validation_read",
            "sealed_holdout_read",
            "action_authorized",
            "live_authorized",
        )
    ):
        raise StudyError("outcome-blind contract already consumed locked evidence")
    provider = contract.get("ema_source") or {}
    if (
        provider.get("provider_training_days") != 66
        or provider.get("provider_sampling_stride") != "none_all_admitted_source_rows"
        or provider.get("provider_economic_outcomes_read") is not False
    ):
        raise StudyError("2025 provider representation boundary drifted")

    current_baseline = _current_baseline_path()
    baseline = _load_source_json(current_baseline, role="current replay baseline")
    expected_baseline_hash = contract["baseline_projection"]["baseline_identity_sha256"]
    _validate_file(
        current_baseline,
        expected_baseline_hash,
        role="current replay baseline",
    )
    if baseline.get("baseline_id") != contract["baseline_projection"]["baseline_id"]:
        raise StudyError("baseline id drifted")
    if (
        baseline["panel"].get("validation_read") is not False
        or baseline["panel"].get("sealed_holdout_read") is not False
    ):
        raise StudyError("baseline panel has consumed locked evidence")

    helper_spec, plan = v1_1_helper._spec_and_plan()
    days = tuple(contract["baseline_projection"]["ordered_utc_days"])
    if (
        days != tuple(baseline["panel"]["ordered_utc_days"])
        or days != tuple(plan["identity_payload"]["ordered_utc_days"])
        or days != tuple(helper_spec["development_denominator"]["ordered_utc_days"])
        or len(days) != 40
    ):
        raise StudyError("native Development day denominator drifted")
    frozen_opportunities = int(contract["duration_source"].get("exposure_increasing_fill_rows", 0))
    if frozen_opportunities != EXPECTED_FORMAL_OPPORTUNITIES:
        raise StudyError("frozen formal opportunity denominator drifted")
    predicates = contract.get("atomic_predicates")
    if not isinstance(predicates, list) or len(predicates) != EXPECTED_PREDICATE_COLUMNS:
        raise StudyError("frozen model interface must contain exactly 360 predicates")
    for side in ("BUY", "SELL"):
        actions = _duration_actions(contract, side)
        if len(actions) != EXPECTED_ACTIONS_PER_SIDE:
            raise StudyError(f"{side} must have exactly eight frozen duration arms")
        for action in actions:
            if action.fixed_duration_s is not None:
                expected_policy_id = f"FIXED_{action.fixed_duration_s}S"
                if action.policy_id != expected_policy_id:
                    raise StudyError(f"{side} duration policy id/seconds drifted")
                if action.fixed_duration_ms != action.fixed_duration_s * 1_000:
                    raise StudyError(f"{side} seconds-to-milliseconds conversion drifted")
    return contract, baseline, plan


def _execution_identity(contract: Mapping[str, Any]) -> dict[str, Any]:
    dependencies = _dependency_bindings()
    action_payload = {
        side: [action.payload() for action in _duration_actions(contract, side)]
        for side in ("BUY", "SELL")
    }
    payload = {
        "identity": IDENTITY,
        "dependencies": dependencies,
        "duration_actions": action_payload,
        "duration_action_universe_sha256": _canonical_sha256(action_payload),
        "ordered_utc_days": contract["baseline_projection"]["ordered_utc_days"],
        "opportunity_semantics": (
            "every legal exposure-increasing fill under the untreated baseline"
        ),
        "formal_sampling": "none_full_cartesian_coverage",
        "joint_outcome_semantics": (
            "all eight side-valid duration arms form one indivisible opportunity outcome"
        ),
        "joint_censoring_semantics": (
            "any right-censored or non-washout arm censors the whole opportunity"
        ),
        "expected_formal_opportunities": EXPECTED_FORMAL_OPPORTUNITIES,
        "expected_actions_per_side": EXPECTED_ACTIONS_PER_SIDE,
        "expected_formal_arm_rows": EXPECTED_FORMAL_ARM_ROWS,
        "formal_replay_engine": FORMAL_REPLAY_ENGINE,
        "python_replay_scope": PYTHON_PARITY_SCOPE,
        "book_source_authority": "native_formal_lifecycle",
        "formal_lifecycle_replay_eligible": True,
        "exact_queue_policy_authority_claimed": False,
        "queue_path_semantics": (
            "native_l2_exact_level_replay_model_without_exchange_queue_authority"
        ),
        "single_action_label_execution_mode": "daily_fresh_start",
        "UTC_midnight_behavior": (
            "right_censor_without_forced_terminal_or_next_day_stitch"
        ),
        "fill_clock_semantics": FILL_CLOCK_SEMANTICS,
        "live_receive_time_authority": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    payload["execution_identity_sha256"] = _canonical_sha256(payload)
    return payload


def _work_estimate(contract: Mapping[str, Any]) -> dict[str, Any]:
    frozen_opportunities = int(contract["duration_source"].get("exposure_increasing_fill_rows", 0))
    action_counts = {side: len(_duration_actions(contract, side)) for side in ("BUY", "SELL")}
    upper_action_count = max(action_counts.values())
    replay_count = frozen_opportunities * upper_action_count
    if replay_count != EXPECTED_FORMAL_ARM_ROWS:
        raise StudyError("frozen formal arm denominator drifted")
    return {
        "outcome_blind_reference_opportunities": frozen_opportunities,
        "actions_per_side": action_counts,
        "full_path_replay_count_reference": replay_count,
        "full_day_replay_equivalents_reference": replay_count,
        "each_arm_restarts_from_day_prefix": True,
        "fill_time_engine_resume_checkpoint_available": False,
        "atomic_chunk_checkpoint_available": True,
        "formal_replay_engine": FORMAL_REPLAY_ENGINE,
        "python_full_arm_execution_allowed": False,
        "python_scope": PYTHON_PARITY_SCOPE,
        "execution_scalability_status": (
            "cpp_authoritative_duration_fork_abi_ready_chunk_resumable"
        ),
        "formal_coverage_may_not_be_replaced_by_sampling": True,
        "diagnostic_limit_has_no_formal_authority": True,
    }


def preflight(*, output: Path = DEFAULT_OUTPUT, write: bool = True) -> dict[str, Any]:
    contract, baseline, _ = _load_contract()
    params, projection = v1_1_helper._offline_params(v1_1_helper._spec_and_plan()[0])
    if bool(params.get("dynamic_fill_hazard_action_enabled", True)):
        raise StudyError("q90 action must remain OFF")
    if bool(params.get("buy_fill_selection_live_enabled", True)):
        raise StudyError("BUY fill selection must remain OFF")
    enabled = [name for name in FORBIDDEN_RESEARCH_ACTION_FLAGS if params.get(name)]
    if enabled:
        raise StudyError(f"baseline contains another research action: {enabled}")
    identity = _execution_identity(contract)
    destination = Path(output).expanduser().resolve()
    probe = destination if destination.exists() else destination.parent
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    free_bytes = shutil.disk_usage(probe).free
    cpp_abi = _cpp_duration_abi_status()
    parity = _parity_admission_status(destination, identity["execution_identity_sha256"])
    control_noop = _control_noop_admission_status(destination)
    payload = {
        "schema_version": f"{SCHEMA_VERSION}.preflight",
        "identity": IDENTITY,
        "execution_identity": identity,
        "baseline_id": baseline["baseline_id"],
        "development_day_count": 40,
        "formal_opportunity_sampling": "none",
        "output_root": str(destination),
        "output_filesystem_free_bytes": int(free_bytes),
        "offline_projection": projection,
        "work_estimate": _work_estimate(contract),
        "formal_cpp_duration_abi": cpp_abi,
        "cpp_python_parity_subset": parity,
        "control_noop_attestation": control_noop,
        "formal_execution_eligible": bool(
            cpp_abi["ready"] and parity["admitted"] and control_noop["admitted"]
        ),
        "economic_outcomes_read": False,
        "model_trained": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    if write:
        _atomic_json(destination / "preflight.json", payload)
    return payload


def _prepare_base_params(raw: Mapping[str, Any], *, trace_opportunities: bool) -> dict[str, Any]:
    params = dict(raw)
    enabled = [name for name in FORBIDDEN_RESEARCH_ACTION_FLAGS if params.get(name)]
    if enabled:
        raise StudyError(f"another research action is enabled: {enabled}")
    if bool(params.get("dynamic_fill_hazard_action_enabled", False)):
        raise StudyError("duration fork requires q90 action OFF")
    if bool(params.get("buy_fill_selection_live_enabled", False)):
        raise StudyError("duration fork requires BUY fill selection OFF")
    for key in tuple(params):
        if key.startswith("cooldown_duration_fork_"):
            params.pop(key)
    params["cooldown_duration_fork_enabled"] = False
    params["trace_cooldown_duration_opportunities_max"] = TRACE_LIMIT if trace_opportunities else 0
    return params


def _load_target_day(
    day: str,
) -> tuple[Any, dict[str, Any], dict[str, Any], dict[str, Any]]:
    spec, plan = v1_1_helper._spec_and_plan()
    window, schedule, params, audit = v1_1_helper._load_day_inputs(day, spec=spec, plan=plan)
    if (
        getattr(window, "book_source_authority", "") != "native_formal_lifecycle"
        or not bool(getattr(window, "formal_lifecycle_replay_eligible", False))
    ):
        raise StudyError(f"{day} lacks native exact-lifecycle authority")
    shared = dict(audit["shared"])
    if shared.get("ml_data") is not schedule.ml_data:
        raise StudyError("control overlay identity drifted")
    return window, _prepare_base_params(params, trace_opportunities=False), shared, audit


def _load_arm_replay(
    day: str,
) -> tuple[Any, dict[str, Any], dict[str, Any], dict[str, Any]]:
    window, params, shared, audit = _load_target_day(day)
    return (
        window,
        params,
        shared,
        {
            "continuation_day": None,
            "continuation_source_bound": False,
            "daily_fresh_start_boundary": True,
            "UTC_midnight_behavior": (
                "right_censor_without_forced_terminal_or_next_day_stitch"
            ),
            "input_projection": audit["projection"],
        },
    )


def _opportunity_identity(day: str, row: Mapping[str, Any]) -> str:
    payload = {
        "schema_version": "cooldown_duration_opportunity_identity.v1",
        "utc_day": day,
        "exposure_fill_ordinal": int(row["exposure_fill_ordinal"]),
        "fill_visible_ts_ms": int(row["fill_visible_ts_ms"]),
        "side": str(row["side"]),
        "role_at_fill": str(row["role_at_fill"]),
        "order_id": int(row["order_id"]),
        "campaign_id": int(row["campaign_id"]),
        "baseline_duration_ms": float(row["baseline_duration_ms"]),
        "fill_clock_semantics": str(row["fill_clock_semantics"]),
        "live_receive_time_authority": bool(row["live_receive_time_authority"]),
    }
    return _canonical_sha256(payload)


def _opportunity_frame(
    day: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    formal_lifecycle_replay_eligible: bool,
    exact_queue_policy_eligible: bool,
) -> pd.DataFrame:
    required = {
        "schema_version",
        "exposure_fill_ordinal",
        "fill_visible_ts_ms",
        "side",
        "role_at_fill",
        "order_id",
        "campaign_id",
        "inventory_before_fill_btc",
        "inventory_after_fill_btc",
        "fill_qty_btc",
        "consecutive_units_after",
        "baseline_duration_ms",
        "baseline_deadline_ts_ms",
        "canonical_mid",
        "decision_visible_bbo_index",
        "decision_visible_l2_index",
        "market_event_index",
        "assignment_equity_usdc",
        "fill_clock_semantics",
        "live_receive_time_authority",
    }
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=[*sorted(required), "utc_day", "opportunity_id"])
    missing = sorted(required - set(frame.columns))
    if missing:
        raise StudyError(f"cooldown opportunity trace is incomplete: {missing}")
    if (
        not frame["schema_version"]
        .eq("multiscale_ema_boolean_cooldown_duration_opportunity.v1")
        .all()
    ):
        raise StudyError("cooldown opportunity schema drifted")
    if not frame["fill_clock_semantics"].eq(FILL_CLOCK_SEMANTICS).all():
        raise StudyError("cooldown opportunity fill-clock semantics drifted")
    if frame["live_receive_time_authority"].astype(bool).any():
        raise StudyError("replay opportunity cannot claim live receive-time authority")
    frame["side"] = frame["side"].astype(str).str.upper()
    if not frame["side"].isin(("BUY", "SELL")).all():
        raise StudyError("cooldown census contains an unsupported side")
    if not frame["role_at_fill"].isin(("opener", "add")).all():
        raise StudyError("cooldown census contains a non-exposure role")
    if frame["exposure_fill_ordinal"].duplicated().any():
        raise StudyError("exposure-fill ordinal is not unique within day")
    frame["utc_day"] = day
    frame["campaign_side_id"] = [
        f"{day}:{int(campaign_id)}:{str(side)}"
        for campaign_id, side in zip(frame["campaign_id"], frame["side"], strict=True)
    ]
    frame["assignment_ts_ns"] = frame["fill_visible_ts_ms"].astype(np.int64) * np.int64(1_000_000)
    frame["opportunity_id"] = [_opportunity_identity(day, row) for row in frame.to_dict("records")]
    if frame["opportunity_id"].duplicated().any():
        raise StudyError("cooldown opportunity identity collided")
    frame["diagnostic_order_sha256"] = [
        hashlib.sha256(f"{DIAGNOSTIC_SEED}|{value}".encode("ascii")).hexdigest()
        for value in frame["opportunity_id"]
    ]
    frame["source_profile"] = "native_formal_lifecycle"
    frame["formal_lifecycle_replay_eligible"] = bool(
        formal_lifecycle_replay_eligible
    )
    frame["exact_queue_policy_eligible"] = bool(exact_queue_policy_eligible)
    frame["queue_path_semantics"] = (
        "native_l2_exact_level_replay_model_without_exchange_queue_authority"
        if not exact_queue_policy_eligible
        else "native_l2_exact_queue_policy_authority"
    )
    return frame.sort_values("exposure_fill_ordinal", kind="stable").reset_index(drop=True)


def _effective_pair_state(
    raw_sign: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    sign = np.asarray(raw_sign, dtype=np.int8)
    indexes = np.arange(len(sign), dtype=np.int64)
    prior_nonzero = np.maximum.accumulate(np.where(sign != 0, indexes, -1))
    effective = np.zeros_like(sign)
    initialized = prior_nonzero >= 0
    effective[initialized] = sign[prior_nonzero[initialized]]
    prior_effective = np.r_[np.int8(0), effective[:-1]]
    arrangement_changed = initialized & (effective != prior_effective)
    arrangement_index = np.maximum.accumulate(np.where(arrangement_changed, indexes, -1))
    crossed = arrangement_changed & (prior_effective != 0)
    cross_index = np.maximum.accumulate(np.where(crossed, indexes, -1))
    return effective, arrangement_index, cross_index


def _half_life_label(value: float) -> str:
    return f"h{float(value):g}s".replace(".", "p")


def _attach_boolean_ema_state(
    opportunities: pd.DataFrame,
    window: Any,
    contract: Mapping[str, Any],
    *,
    tick_size: float,
) -> pd.DataFrame:
    if opportunities.empty:
        return opportunities.copy()
    if not math.isfinite(float(tick_size)) or float(tick_size) <= 0.0:
        raise StudyError("tick size must be positive for EMA state admission")
    bbo = window.bbo_data
    if bbo is None:
        raise StudyError("native BBO is required for canonical EMA state")
    timestamps = np.asarray(bbo.ts_ms, dtype=np.int64)
    if len(timestamps) == 0 or np.any(timestamps[1:] <= timestamps[:-1]):
        raise StudyError("native BBO clock cannot support causal EMA state")
    bid = np.asarray(bbo.best_bid, dtype=np.float64)
    ask = np.asarray(bbo.best_ask, dtype=np.float64)
    if len(bid) != len(timestamps) or len(ask) != len(timestamps):
        raise StudyError("native BBO arrays have inconsistent lengths")
    if np.any(bid <= 0.0) or np.any(ask <= bid):
        raise StudyError("native BBO contains an invalid market")
    mid = 0.5 * (bid + ask)
    sample_index = opportunities["decision_visible_bbo_index"].to_numpy(dtype=np.int64)
    if np.any(sample_index < 0) or np.any(sample_index >= len(timestamps)):
        raise StudyError("fill-time opportunity lacks a decision-visible BBO row")
    fill_ts = opportunities["fill_visible_ts_ms"].to_numpy(dtype=np.int64)
    ready_ts = timestamps[sample_index]
    if np.any(ready_ts > fill_ts):
        raise StudyError("EMA source clock crossed the fill-visible clock")
    sample_mid = mid[sample_index]
    trace_mid = opportunities["canonical_mid"].to_numpy(dtype=np.float64)
    if not np.isfinite(trace_mid).all() or not np.array_equal(sample_mid, trace_mid):
        raise StudyError(
            "fill-time canonical mid does not exactly match its decision-visible BBO generation"
        )

    var_ts = np.asarray(window.var_ts_ms, dtype=np.int64)
    var_ssq = np.asarray(window.var_ssq, dtype=np.float64)
    var_index = np.searchsorted(var_ts, fill_ts, side="right") - 1
    if np.any(var_index < 0) or np.any(var_index >= len(var_ssq)):
        raise StudyError("fill-time opportunity lacks causal volatility state")
    volatility_bps = np.sqrt(np.maximum(var_ssq[var_index], 1e-24)) * 10_000.0 / sample_mid
    if not np.isfinite(volatility_bps).all() or np.any(volatility_bps <= 0.0):
        raise StudyError("causal volatility normalization is invalid")

    half_lives = tuple(float(value) for value in ema_contract.EMA_HALF_LIVES_S)
    pairs = ema_contract.ema_pairs(half_lives)
    full_ema: list[np.ndarray] = []
    sampled_velocity: list[np.ndarray] = []
    features: dict[str, Any] = {
        "ema_surface_feature_ready_ts_ns": ready_ts * 1_000_000,
        "ema_surface_canonical_mid": sample_mid,
        "ema_pair_count": np.full(len(opportunities), len(pairs), dtype=np.int64),
        "ema_causal_volatility_bps": volatility_bps,
        "ema_causal_volatility_ready_ts_ms": var_ts[var_index],
        "ema_source_bbo_index": sample_index,
        "ema_source_bbo_ready_ts_ms": ready_ts,
        "ema_source_bbo_stream": str(getattr(bbo, "source", "bbo")),
        "ema_snapshot_mid_exact_match": np.ones(len(opportunities), dtype=bool),
    }
    side_sign = np.where(opportunities["side"].eq("BUY").to_numpy(), 1.0, -1.0)
    for half_life in half_lives:
        ema, velocity = source_grid._irregular_ema(
            timestamps,
            mid,
            half_life_s=half_life,
        )
        full_ema.append(ema)
        sampled_velocity.append(velocity[sample_index])
        label = _half_life_label(half_life)
        features[f"ema_rel_mid_bps_{label}"] = (
            side_sign * 10_000.0 * (ema[sample_index] - sample_mid) / sample_mid
        )
        features[f"ema_slope_bps_per_s_{label}"] = (
            side_sign * 10_000.0 * sampled_velocity[-1] / sample_mid
        )

    effective_matrix = np.empty((len(timestamps), len(pairs)), dtype=np.int8)
    favorable_count = np.zeros(len(opportunities), dtype=np.int64)
    indexes = np.arange(len(timestamps), dtype=np.int64)
    half_life_index = {value: index for index, value in enumerate(half_lives)}
    for pair_number, (fast, slow) in enumerate(pairs):
        fast_index = half_life_index[fast]
        slow_index = half_life_index[slow]
        raw_distance = 10_000.0 * (full_ema[fast_index] - full_ema[slow_index]) / mid
        effective, arrangement_index, cross_index = _effective_pair_state(np.sign(raw_distance))
        effective_matrix[:, pair_number] = effective
        sampled_effective = effective[sample_index]
        sampled_distance = raw_distance[sample_index]
        favorable_ordering = (side_sign * sampled_effective > 0).astype(np.int8)
        favorable_count += favorable_ordering
        sampled_cross_index = cross_index[sample_index]
        cross_missing = sampled_cross_index < 0
        safe_cross = np.maximum(sampled_cross_index, 0)
        cross_age_s = np.full(
            len(opportunities),
            ema_contract.CROSS_AGE_MISSING_SENTINEL_S,
            dtype=np.float64,
        )
        cross_age_s[~cross_missing] = (
            ready_ts[~cross_missing] - timestamps[safe_cross[~cross_missing]]
        ) / 1_000.0
        sampled_arrangement_index = arrangement_index[sample_index]
        arrangement_missing = sampled_arrangement_index < 0
        safe_arrangement = np.maximum(sampled_arrangement_index, 0)
        persistence_s = np.where(
            arrangement_missing,
            0.0,
            (ready_ts - timestamps[safe_arrangement]) / 1_000.0,
        )
        distance_velocity = (
            side_sign
            * 10_000.0
            * (sampled_velocity[fast_index] - sampled_velocity[slow_index])
            / sample_mid
        )
        prefix = ema_contract.pair_prefix(fast, slow)
        features[f"{prefix}_favorable_ordering"] = favorable_ordering
        features[f"{prefix}_adverse_ordering"] = (side_sign * sampled_effective < 0).astype(np.int8)
        cross_direction = effective[safe_cross]
        features[f"{prefix}_last_cross_favorable"] = (
            (~cross_missing) & (side_sign * cross_direction > 0)
        ).astype(np.int8)
        features[f"{prefix}_last_cross_adverse"] = (
            (~cross_missing) & (side_sign * cross_direction < 0)
        ).astype(np.int8)
        features[f"{prefix}_cross_missing"] = cross_missing.astype(np.int8)
        features[f"{prefix}_cross_age_s"] = cross_age_s
        features[f"{prefix}_arrangement_persistence_s"] = persistence_s
        features[f"{prefix}_favorable_distance_bps"] = side_sign * sampled_distance
        features[f"{prefix}_abs_distance_bps"] = np.abs(sampled_distance)
        features[f"{prefix}_volatility_normalized"] = side_sign * sampled_distance / volatility_bps
        features[f"{prefix}_favorable_distance_velocity_bps_per_s"] = distance_velocity
        raw_velocity = sampled_velocity[fast_index] - sampled_velocity[slow_index]
        features[f"{prefix}_distance_expanding"] = (sampled_distance * raw_velocity > 0.0).astype(
            np.int8
        )
        features[f"{prefix}_distance_converging"] = (sampled_distance * raw_velocity < 0.0).astype(
            np.int8
        )

    features["ema_pair_favorable_fraction"] = favorable_count / len(pairs)
    all_initialized = np.all(effective_matrix != 0, axis=1)
    signature_changed = (
        all_initialized & np.r_[True, np.any(effective_matrix[1:] != effective_matrix[:-1], axis=1)]
    )
    ordering_index = np.maximum.accumulate(np.where(signature_changed, indexes, -1))[sample_index]
    ordering_missing = ordering_index < 0
    safe_ordering = np.maximum(ordering_index, 0)
    features["ema_full_ordering_persistence_s"] = np.where(
        ordering_missing,
        0.0,
        (ready_ts - timestamps[safe_ordering]) / 1_000.0,
    )
    features["ema_full_ordering_missing"] = ordering_missing.astype(np.int8)

    feature_frame = pd.DataFrame(features, index=opportunities.index)
    output = pd.concat((opportunities, feature_frame), axis=1)
    predicate_rows = contract.get("atomic_predicates")
    if not isinstance(predicate_rows, list) or len(predicate_rows) != EXPECTED_PREDICATE_COLUMNS:
        raise StudyError("frozen atomic predicate dictionary must contain exactly 360 rows")
    predicate_columns: dict[str, np.ndarray] = {}
    for raw in predicate_rows:
        feature = str(raw["feature"])
        if feature not in feature_frame:
            raise StudyError(
                f"predicate may reference only EMA feature state, not mechanics metadata: {feature}"
            )
        values = feature_frame[feature].to_numpy(dtype=np.float64)
        threshold = float(raw["threshold"])
        operator = str(raw["operator"])
        if operator == "gt":
            predicate = values > threshold
        elif operator == "ge":
            predicate = values >= threshold
        elif operator == "lt":
            predicate = values < threshold
        elif operator == "le":
            predicate = values <= threshold
        elif operator == "eq":
            predicate = values == threshold
        else:
            raise StudyError(f"unsupported atomic predicate operator: {operator}")
        cross_missing_feature: str | None = None
        if feature.endswith("_cross_age_s"):
            cross_missing_feature = f"{feature[: -len('_cross_age_s')]}_cross_missing"
        elif "_last_cross_" in feature:
            cross_missing_feature = f"{feature.split('_last_cross_', 1)[0]}_cross_missing"
        if cross_missing_feature is not None:
            if cross_missing_feature not in feature_frame:
                raise StudyError(
                    f"cross predicate lacks its missing-state guard: {cross_missing_feature}"
                )
            predicate &= ~feature_frame[cross_missing_feature].to_numpy(dtype=bool)
        name = f"predicate::{raw['name']}"
        if name in predicate_columns:
            raise StudyError(f"duplicate frozen predicate name: {name}")
        predicate_columns[name] = predicate.astype(bool)
    if len(predicate_columns) != EXPECTED_PREDICATE_COLUMNS:
        raise StudyError("frozen predicate projection did not produce exactly 360 columns")
    output = pd.concat((output, pd.DataFrame(predicate_columns, index=output.index)), axis=1)
    if any(output[name].dtype != bool for name in predicate_columns):
        raise StudyError("every predicate column must have boolean dtype")
    output["ema_state_support_valid"] = True
    output["ema_state_price_source"] = "decision_visible_native_bbo_mid"
    return output


def _census_paths(output: Path, day: str) -> tuple[Path, Path]:
    root = Path(output) / "census" / day
    return root / "opportunities.parquet", root / "manifest.json"


def _load_census_day(
    day: str, *, output: Path, contract: Mapping[str, Any]
) -> tuple[pd.DataFrame, dict[str, Any]]:
    data_path, manifest_path = _census_paths(output, day)
    if not data_path.is_file() or not manifest_path.is_file():
        raise StudyError(f"missing census admission for {day}")
    manifest = _load_json(manifest_path)
    expected_identity = _execution_identity(contract)["execution_identity_sha256"]
    if (
        manifest.get("identity") != IDENTITY
        or manifest.get("utc_day") != day
        or manifest.get("execution_identity_sha256") != expected_identity
        or manifest.get("formal_sampling") != "none_full_coverage"
        or manifest.get("formal_replay_engine") != FORMAL_REPLAY_ENGINE
        or manifest.get("fill_clock_semantics") != FILL_CLOCK_SEMANTICS
        or manifest.get("live_receive_time_authority") is not False
        or manifest.get("economic_outcomes_read") is not False
    ):
        raise StudyError(f"{day} census identity drifted")
    _validate_file(data_path, manifest["data_sha256"], role=f"{day} census")
    frame = pd.read_parquet(data_path)
    if len(frame) != int(manifest["opportunity_count"]):
        raise StudyError(f"{day} census row count drifted")
    if frame["opportunity_id"].duplicated().any():
        raise StudyError(f"{day} census opportunity ids are duplicated")
    return frame, manifest


def census_day(day: str, *, output: Path = DEFAULT_OUTPUT) -> dict[str, Any]:
    contract, _, _ = _load_contract()
    cpp_abi = _require_cpp_duration_abi()
    data_path, manifest_path = _census_paths(output, day)
    if data_path.is_file() and manifest_path.is_file():
        return _load_census_day(day, output=output, contract=contract)[1]
    window, raw_params, shared, audit = _load_target_day(day)
    params = _prepare_base_params(raw_params, trace_opportunities=True)
    result = bt._simulate_tick_with_engine(
        FORMAL_REPLAY_ENGINE,
        window.trades,
        window.var_ts_ms,
        window.var_ssq,
        params,
        **shared,
    )
    frame = _opportunity_frame(
        day,
        result.get("_cooldown_duration_opportunity_trace") or (),
        formal_lifecycle_replay_eligible=bool(
            getattr(window, "formal_lifecycle_replay_eligible", False)
        ),
        exact_queue_policy_eligible=bool(
            getattr(window, "exact_queue_policy_eligible", False)
        ),
    )
    if len(frame) >= TRACE_LIMIT:
        raise StudyError(f"{day} census reached the trace ceiling")
    frame = _attach_boolean_ema_state(
        frame,
        window,
        contract,
        tick_size=float(params.get("tick_size", bt.TICK)),
    )
    _atomic_parquet(data_path, frame)
    exact_tasks = sum(len(_duration_actions(contract, side)) for side in frame["side"].astype(str))
    manifest = {
        "schema_version": f"{SCHEMA_VERSION}.census_day",
        "identity": IDENTITY,
        "utc_day": day,
        "execution_identity_sha256": _execution_identity(contract)["execution_identity_sha256"],
        "data_path": str(data_path),
        "data_sha256": _sha256_file(data_path),
        "opportunity_count": int(len(frame)),
        "exact_formal_fork_task_count": int(exact_tasks),
        "counts": [
            {
                "side": str(side),
                "role_at_fill": str(role),
                "opportunities": int(count),
            }
            for (side, role), count in frame.groupby(["side", "role_at_fill"], observed=True)
            .size()
            .items()
        ],
        "all_legal_exposure_increasing_fill_opportunities_included": True,
        "formal_sampling": "none_full_coverage",
        "formal_replay_engine": FORMAL_REPLAY_ENGINE,
        "cpp_duration_abi": cpp_abi,
        "diagnostic_order_only_not_sampling": True,
        "fill_clock_semantics": FILL_CLOCK_SEMANTICS,
        "live_receive_time_authority": False,
        "ema_state_source": "decision_visible_native_bbo_mid",
        "book_source_contract": {
            "source_authority": str(getattr(window, "book_source_authority", "")),
            "formal_lifecycle_replay_eligible": bool(
                getattr(window, "formal_lifecycle_replay_eligible", False)
            ),
            "exact_queue_policy_eligible": bool(
                getattr(window, "exact_queue_policy_eligible", False)
            ),
            "queue_path_semantics": (
                "native_l2_exact_level_replay_model_without_exchange_queue_authority"
                if not bool(getattr(window, "exact_queue_policy_eligible", False))
                else "native_l2_exact_queue_policy_authority"
            ),
        },
        "economic_outcomes_read": False,
        "model_trained": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "input_projection": audit["projection"],
    }
    _atomic_json(manifest_path, manifest)
    return manifest


def _requested_days(contract: Mapping[str, Any], requested: Sequence[str]) -> list[str]:
    frozen = list(contract["baseline_projection"]["ordered_utc_days"])
    if not requested:
        return frozen
    unknown = sorted(set(requested) - set(frozen))
    if unknown:
        raise StudyError(f"requested days are outside Development: {unknown}")
    chosen = set(requested)
    return [day for day in frozen if day in chosen]


def summarize_census(days: Sequence[str], *, output: Path = DEFAULT_OUTPUT) -> dict[str, Any]:
    contract, _, _ = _load_contract()
    rows = []
    total_opportunities = 0
    total_tasks = 0
    for day in days:
        _, manifest = _load_census_day(day, output=output, contract=contract)
        rows.append(
            {
                "utc_day": day,
                "manifest_path": str(_census_paths(output, day)[1]),
                "manifest_sha256": _sha256_file(_census_paths(output, day)[1]),
                "data_path": manifest["data_path"],
                "data_sha256": manifest["data_sha256"],
                "opportunity_count": int(manifest["opportunity_count"]),
                "fork_task_count": int(manifest["exact_formal_fork_task_count"]),
            }
        )
        total_opportunities += int(manifest["opportunity_count"])
        total_tasks += int(manifest["exact_formal_fork_task_count"])
    frozen_days = contract["baseline_projection"]["ordered_utc_days"]
    formal = list(days) == list(frozen_days)
    if formal and (
        total_opportunities != EXPECTED_FORMAL_OPPORTUNITIES
        or total_tasks != EXPECTED_FORMAL_ARM_ROWS
    ):
        raise StudyError("formal census must contain exactly 8,600 opportunities and 68,800 arms")
    payload = {
        "schema_version": f"{SCHEMA_VERSION}.census_manifest",
        "identity": IDENTITY,
        "status": (
            "formal_full_development_census" if formal else "diagnostic_partial_development_census"
        ),
        "execution_identity_sha256": _execution_identity(contract)["execution_identity_sha256"],
        "ordered_utc_days": list(days),
        "day_count": len(days),
        "opportunity_count": total_opportunities,
        "exact_formal_fork_task_count": total_tasks,
        "formal_sampling": "none_full_coverage",
        "formal_replay_engine": FORMAL_REPLAY_ENGINE,
        "fill_clock_semantics": FILL_CLOCK_SEMANTICS,
        "live_receive_time_authority": False,
        "parts": rows,
        "work_estimate": {
            "full_path_replays": total_tasks,
            "each_arm_restarts_from_day_prefix": True,
            "fill_time_engine_resume_checkpoint_available": False,
            "atomic_chunk_checkpoint_available": True,
            "scalability_blocker": None,
            "cpp_authoritative_duration_fork_abi_ready": True,
            "python_full_arm_execution_allowed": False,
        },
        "economic_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    suffix = "" if formal else f".partial-{_canonical_sha256(list(days))[:12]}"
    _atomic_json(Path(output) / f"census_manifest{suffix}.json", payload)
    return payload


def run_census(
    days: Sequence[str], *, workers: int, output: Path = DEFAULT_OUTPUT
) -> dict[str, Any]:
    if workers <= 1:
        for day in days:
            census_day(day, output=output)
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(census_day, day, output=output): day for day in days}
            for future in concurrent.futures.as_completed(futures):
                future.result()
    return summarize_census(days, output=output)


def _diagnostic_opportunities(frame: pd.DataFrame, *, limit: int | None) -> pd.DataFrame:
    if limit is None:
        return frame.sort_values("opportunity_id", kind="stable").reset_index(drop=True)
    if limit <= 0:
        raise StudyError("--limit must be positive")
    return (
        frame.sort_values("diagnostic_order_sha256", kind="stable")
        .head(limit)
        .sort_values("opportunity_id", kind="stable")
        .reset_index(drop=True)
    )


def _task_id(opportunity_id: str, side: str, action: DurationAction) -> str:
    return _canonical_sha256(
        {
            "schema_version": "cooldown_duration_fork_task.v1",
            "identity": IDENTITY,
            "opportunity_id": str(opportunity_id),
            "side": str(side),
            "action": action.payload(),
        }
    )


def _build_tasks(
    opportunities: pd.DataFrame,
    contract: Mapping[str, Any],
    *,
    limit: int | None,
) -> list[tuple[dict[str, Any], DurationAction, str]]:
    selected = _diagnostic_opportunities(opportunities, limit=limit)
    tasks: list[tuple[dict[str, Any], DurationAction, str]] = []
    for opportunity in selected.to_dict("records"):
        side = str(opportunity["side"])
        for action in _duration_actions(contract, side):
            tasks.append(
                (
                    opportunity,
                    action,
                    _task_id(str(opportunity["opportunity_id"]), side, action),
                )
            )
    return sorted(tasks, key=lambda item: item[2])


def _run_duration_arm(
    opportunity: Mapping[str, Any],
    action: DurationAction,
    *,
    window: Any,
    base: Mapping[str, Any],
    shared: Mapping[str, Any],
    engine: str,
    authoritative_control_fills: Sequence[Mapping[str, Any]] | None = None,
    require_control_prefix_parity: bool = True,
    exact_owner_baseline_policy_enabled: bool = False,
    expected_exact_owner_action: str | None = None,
    expected_exact_owner_policy_sha256: str | None = None,
    return_result: bool = False,
) -> tuple[dict[str, Any], float] | tuple[dict[str, Any], float, dict[str, Any]]:
    if engine not in {"cpp", "python"}:
        raise StudyError(f"unsupported duration fork replay engine: {engine}")
    if not isinstance(require_control_prefix_parity, bool):
        raise StudyError("control-prefix parity requirement must be Boolean")
    if not isinstance(exact_owner_baseline_policy_enabled, bool):
        raise StudyError("exact-owner baseline policy requirement must be Boolean")
    if exact_owner_baseline_policy_enabled:
        if (
            base.get("cooldown_v2_snapshot_emitter") is None
            or base.get("cooldown_duration_policy_evaluator") is None
        ):
            raise StudyError("exact-owner duration fork lacks its runtime")
        if not str(expected_exact_owner_action or "").strip():
            raise StudyError("exact-owner duration fork lacks its row-wise action")
        if re.fullmatch(
            r"[0-9a-f]{64}", str(expected_exact_owner_policy_sha256 or "")
        ) is None:
            raise StudyError("exact-owner duration fork lacks its policy SHA256")
        if engine == "cpp" and (
            base.get("cooldown_duration_policy_cpp_runtime") is None
            or not bool(
                base.get("cooldown_duration_policy_cpp_parity_qualified", False)
            )
            or not bool(
                base.get(
                    "cooldown_duration_policy_cpp_event_loop_parity_qualified",
                    False,
                )
            )
        ):
            raise StudyError(
                "exact-owner C++ duration fork lacks a parity-qualified runtime"
            )
    params = _prepare_base_params(base, trace_opportunities=False)
    params.update(
        {
            "cooldown_duration_fork_enabled": True,
            "cooldown_duration_fork_action": action.engine_action,
            "cooldown_duration_fork_target_ordinal": int(opportunity["exposure_fill_ordinal"]),
            "cooldown_duration_fork_target_ts_ms": int(opportunity["fill_visible_ts_ms"]),
            "cooldown_duration_fork_target_side": str(opportunity["side"]),
            "cooldown_duration_fork_target_order_id": int(opportunity["order_id"]),
            "cooldown_duration_fork_target_campaign_id": int(opportunity["campaign_id"]),
            "cooldown_duration_fork_expected_baseline_ms": float(
                opportunity["baseline_duration_ms"]
            ),
            "cooldown_duration_fork_fixed_ms": (
                float(action.fixed_duration_ms) if action.fixed_duration_ms is not None else 0.0
            ),
            "cooldown_duration_fork_baseline_policy_enabled": bool(
                exact_owner_baseline_policy_enabled
            ),
            "cooldown_duration_fork_expected_owner_action": (
                str(expected_exact_owner_action or "")
            ),
            "cooldown_duration_fork_expected_owner_policy_sha256": (
                str(expected_exact_owner_policy_sha256 or "")
            ),
        }
    )
    started = time.perf_counter()
    result = bt._simulate_tick_with_engine(
        engine,
        window.trades,
        window.var_ts_ms,
        window.var_ssq,
        params,
        **shared,
    )
    elapsed = time.perf_counter() - started
    trace = dict(result.get("_cooldown_duration_fork_trace") or {})
    expected = {
        "action": action.engine_action,
        "side": str(opportunity["side"]),
        "campaign_id": int(opportunity["campaign_id"]),
        "target_exposure_fill_ordinal": int(opportunity["exposure_fill_ordinal"]),
        "target_order_id": int(opportunity["order_id"]),
        "assignment_ts_ms": int(opportunity["fill_visible_ts_ms"]),
    }
    for field, value in expected.items():
        if trace.get(field) != value:
            raise StudyError(f"duration fork trace {field} drifted")
    if exact_owner_baseline_policy_enabled:
        if (
            trace.get("exact_owner_baseline_policy_enabled") is not True
            or trace.get("exact_owner_action") != expected_exact_owner_action
            or trace.get("exact_owner_policy_sha256")
            != expected_exact_owner_policy_sha256
            or not math.isfinite(
                float(trace.get("exact_owner_baseline_duration_ms", math.nan))
            )
            or float(trace["exact_owner_baseline_duration_ms"]) <= 0.0
        ):
            raise StudyError("duration fork exact-owner baseline identity drifted")
    if (
        trace.get("washout_protocol")
        != "first_flat_exposure_quarantine_scheduler_drained_v2"
        or trace.get("control_path_exact_until_quarantine") is not True
        or int(trace.get("reducing_quote_change_count", -1)) != 0
        or int(trace.get("second_assignment_count", -1)) != 0
    ):
        raise StudyError("duration fork washout/permission contract drifted")
    assignment_fields = {
        "assignment_ts_ms": int(trace["assignment_ts_ms"]),
        "assignment_inventory_btc": float(trace["assignment_inventory_btc"]),
        "assignment_equity_usdc": float(trace["assignment_equity_usdc"]),
        "baseline_duration_ms": float(trace["baseline_duration_ms"]),
        "target_ordinal": int(trace["target_exposure_fill_ordinal"]),
        "target_order_id": int(trace["target_order_id"]),
        "target_campaign_id": int(trace["campaign_id"]),
        "target_side": str(trace["side"]),
    }
    trace["assignment_state_sha256"] = _canonical_sha256(assignment_fields)
    if not math.isclose(
        assignment_fields["assignment_inventory_btc"],
        float(opportunity["inventory_after_fill_btc"]),
        rel_tol=0.0,
        abs_tol=1e-12,
    ) or not math.isclose(
        assignment_fields["assignment_equity_usdc"],
        float(opportunity["assignment_equity_usdc"]),
        rel_tol=0.0,
        abs_tol=1e-12,
    ):
        raise StudyError("duration fork assignment state differs from the C++ census")
    if not math.isclose(
        float(trace.get("baseline_duration_ms", math.nan)),
        float(opportunity["baseline_duration_ms"]),
        rel_tol=0.0,
        abs_tol=1e-9,
    ):
        raise StudyError("duration fork baseline duration drifted")
    expected_applied = (
        float(opportunity["baseline_duration_ms"])
        if action.fixed_duration_ms is None
        else float(action.fixed_duration_ms)
    )
    if not math.isclose(
        float(trace.get("applied_duration_ms", math.nan)),
        expected_applied,
        rel_tol=0.0,
        abs_tol=1e-9,
    ):
        raise StudyError("duration fork applied a different action")
    right_censored = bool(trace.get("right_censored", False))
    washout_complete = bool(trace.get("arm_washout_complete", False))
    if bool(trace.get("censor_marks_are_terminal_bounds", True)):
        raise StudyError("censor-time marks must never be terminal bounds")
    washout_value = trace.get("assignment_to_washout_value_usdc")
    censor_mid = trace.get("censor_time_mid_mark_usdc")
    censor_executable = trace.get("censor_time_executable_mark_usdc")
    if right_censored:
        if washout_complete or washout_value is not None:
            raise StudyError("right-censored arm cannot carry a completed washout label")
        if censor_mid is None or censor_executable is None:
            raise StudyError("right-censored arm lacks diagnostic censor-time marks")
    else:
        if not washout_complete or str(trace.get("terminal_reason")) != "arm_economic_washout":
            raise StudyError("uncensored arm lacks economic washout")
        if washout_value is None or not math.isfinite(float(washout_value)):
            raise StudyError("completed arm lacks assignment-to-washout value")
        if censor_mid is not None or censor_executable is not None:
            raise StudyError("completed arm unexpectedly carries censor-time marks")
    if washout_complete:
        for field in (
            "active_or_pending_order_count",
            "pending_submit_count",
            "pending_cancel_count",
            "pending_ack_count",
            "cursor_owner_count",
            "hazard_owner_count",
        ):
            if int(trace.get(field, -1)) != 0:
                raise StudyError(f"duration fork washout retained {field}")
        if bool(trace.get("campaign_active", True)):
            raise StudyError("duration fork washout retained an active campaign")
        if abs(float(trace.get("terminal_inventory_btc", math.nan))) > 1e-10:
            raise StudyError("duration fork washout retained inventory")
        if abs(float(trace.get("accounting_residual_usdc", math.nan))) > 1e-6:
            raise StudyError("duration fork accounting residual exceeded 1e-6 USDC")
    parity_requested = require_control_prefix_parity and (
        exact_owner_baseline_policy_enabled or action.policy_id == "CONTROL_85N"
    )
    if parity_requested:
        expected_noop_action = (
            str(expected_exact_owner_action)
            if exact_owner_baseline_policy_enabled
            else "CONTROL_85N"
        )
        if action.policy_id != expected_noop_action:
            raise StudyError("control-prefix parity was requested for a non-B0 action")
        if authoritative_control_fills is None:
            raise StudyError("B0-equivalent fork lacks authoritative control fill path")
        cutoff_ms = (
            int(trace["quarantine_ts_ms"])
            if bool(trace["quarantine_entered"])
            else int(trace["terminal_ts_ms"])
        )
        control_prefix = _canonical_fill_prefix(
            authoritative_control_fills,
            cutoff_ms=cutoff_ms,
        )
        fork_prefix = _canonical_fill_prefix(
            result.get("_fill_trace") or (),
            cutoff_ms=cutoff_ms,
        )
        if fork_prefix != control_prefix:
            common_count = min(len(control_prefix), len(fork_prefix))
            first_difference = next(
                (
                    index
                    for index in range(common_count)
                    if control_prefix[index] != fork_prefix[index]
                ),
                common_count,
            )
            control_fill = (
                control_prefix[first_difference]
                if first_difference < len(control_prefix)
                else None
            )
            fork_fill = (
                fork_prefix[first_difference]
                if first_difference < len(fork_prefix)
                else None
            )
            raise StudyError(
                "B0-equivalent fork diverged before the common washout quarantine: "
                f"cutoff_ms={cutoff_ms}, control_fill_count={len(control_prefix)}, "
                f"fork_fill_count={len(fork_prefix)}, "
                f"first_difference_index={first_difference}, "
                f"control_fill={control_fill!r}, fork_fill={fork_fill!r}"
            )
        trace["control_prefix_parity_match"] = True
        trace["control_prefix_fill_count"] = len(fork_prefix)
    else:
        trace["control_prefix_parity_match"] = None
        trace["control_prefix_fill_count"] = None
    if return_result:
        return trace, float(elapsed), dict(result)
    return trace, float(elapsed)


def _assert_cpp_python_trace_parity(
    cpp_trace: Mapping[str, Any], python_trace: Mapping[str, Any]
) -> str:
    exact_fields = (
        "action",
        "side",
        "campaign_id",
        "target_exposure_fill_ordinal",
        "target_order_id",
        "assignment_ts_ms",
        "applied_deadline_ts_ms",
        "quarantine_entered",
        "quarantine_ts_ms",
        "arm_washout_complete",
        "terminal_ts_ms",
        "terminal_reason",
        "right_censored",
        "post_assignment_buy_fill_count",
        "post_assignment_sell_fill_count",
        "active_or_pending_order_count",
        "pending_submit_count",
        "pending_cancel_count",
        "pending_ack_count",
        "campaign_active",
        "cursor_owner_count",
        "hazard_owner_count",
        "censor_marks_are_terminal_bounds",
        "washout_protocol",
        "control_path_exact_until_quarantine",
        "exposure_permission_change_count",
        "reducing_permission_control_checks",
        "reducing_quote_change_count",
        "second_assignment_count",
    )
    float_abs_tolerances = {
        "assignment_inventory_btc": 1e-12,
        "assignment_equity_usdc": 1e-12,
        "baseline_duration_ms": 1e-9,
        "applied_duration_ms": 1e-9,
        "terminal_inventory_btc": 1e-12,
        "assignment_to_washout_value_usdc": 1e-12,
        "censor_time_mid_mark_usdc": 1e-12,
        "censor_time_executable_mark_usdc": 1e-12,
        "inventory_time_btc_s": 1e-12,
        "mae_usdc": 1e-12,
        "max_abs_inventory_btc": 1e-12,
        "accounting_residual_usdc": 1e-12,
    }
    for field in exact_fields:
        if cpp_trace.get(field) != python_trace.get(field):
            raise StudyError(f"C++/Python cooldown fork mismatch: {field}")
    normalized: dict[str, Any] = {field: cpp_trace.get(field) for field in exact_fields}
    for field, abs_tolerance in float_abs_tolerances.items():
        cpp_value = cpp_trace.get(field)
        python_value = python_trace.get(field)
        if cpp_value is None or python_value is None:
            if cpp_value is not None or python_value is not None:
                raise StudyError(f"C++/Python cooldown fork null mismatch: {field}")
            normalized[field] = None
            continue
        if not math.isclose(
            float(cpp_value),
            float(python_value),
            rel_tol=0.0,
            abs_tol=abs_tolerance,
        ):
            raise StudyError(f"C++/Python cooldown fork numeric mismatch: {field}")
        normalized[field] = float(cpp_value)
    return _canonical_sha256(normalized)


def _assert_cpp_python_opportunity_trace_parity(
    cpp_trace: Mapping[str, Any], python_trace: Mapping[str, Any]
) -> str:
    exact_fields = (
        "schema_version",
        "fill_clock_semantics",
        "live_receive_time_authority",
        "exposure_fill_ordinal",
        "fill_visible_ts_ms",
        "fill_exchange_ts_ms",
        "side",
        "role_at_fill",
        "order_id",
        "campaign_id",
        "prior_deadline_ts_ms",
        "baseline_deadline_ts_ms",
        "decision_visible_bbo_index",
        "decision_visible_l2_index",
        "market_event_index",
    )
    float_abs_tolerances = {
        "inventory_before_fill_btc": 1e-12,
        "inventory_after_fill_btc": 1e-12,
        "fill_qty_btc": 1e-12,
        "unit_qty_btc": 1e-12,
        "consecutive_units_before": 1e-12,
        "consecutive_units_after": 1e-12,
        "baseline_duration_ms": 1e-9,
        "canonical_mid": 1e-9,
        "best_bid": 1e-9,
        "best_ask": 1e-9,
        "assignment_equity_usdc": 1e-12,
    }
    expected_fields = set(exact_fields) | set(float_abs_tolerances)
    if set(cpp_trace) != expected_fields or set(python_trace) != expected_fields:
        raise StudyError("C++/Python cooldown opportunity schema mismatch")

    normalized: dict[str, Any] = {}
    for field in exact_fields:
        if cpp_trace[field] != python_trace[field]:
            raise StudyError(f"C++/Python cooldown opportunity mismatch: {field}")
        normalized[field] = cpp_trace[field]
    for field, abs_tolerance in float_abs_tolerances.items():
        cpp_value = float(cpp_trace[field])
        python_value = float(python_trace[field])
        if not math.isclose(
            cpp_value,
            python_value,
            rel_tol=0.0,
            abs_tol=abs_tolerance,
        ):
            raise StudyError(f"C++/Python cooldown opportunity numeric mismatch: {field}")
        normalized[field] = cpp_value
    return _canonical_sha256(normalized)


def _label_row(
    opportunity: Mapping[str, Any],
    action: DurationAction,
    task_id: str,
    trace: Mapping[str, Any],
    *,
    arm_wall_seconds: float,
    replay_engine: str,
) -> dict[str, Any]:
    row = dict(opportunity)
    row.update(
        {
            "task_id": task_id,
            "replay_engine": replay_engine,
            "duration_policy_id": action.policy_id,
            "duration_engine_action": action.engine_action,
            "fixed_duration_s": action.fixed_duration_s,
            "fixed_duration_ms": action.fixed_duration_ms,
            "duration_semantics": action.duration_semantics,
            "assignment_state_sha256": str(trace["assignment_state_sha256"]),
            "fork_assignment_ts_ms": int(trace["assignment_ts_ms"]),
            "fork_assignment_inventory_btc": float(trace["assignment_inventory_btc"]),
            "fork_assignment_equity_usdc": float(trace["assignment_equity_usdc"]),
            "fork_baseline_duration_ms": float(trace["baseline_duration_ms"]),
            "fork_applied_duration_ms": float(trace["applied_duration_ms"]),
            "fork_applied_deadline_ts_ms": int(trace["applied_deadline_ts_ms"]),
            "fork_quarantine_entered": bool(trace["quarantine_entered"]),
            "fork_quarantine_ts_ms": int(trace["quarantine_ts_ms"]),
            "washout_protocol": str(trace["washout_protocol"]),
            "control_path_exact_until_quarantine": bool(
                trace["control_path_exact_until_quarantine"]
            ),
            "exposure_permission_change_count": int(
                trace["exposure_permission_change_count"]
            ),
            "reducing_permission_control_checks": int(
                trace["reducing_permission_control_checks"]
            ),
            "reducing_quote_change_count": int(trace["reducing_quote_change_count"]),
            "second_assignment_count": int(trace["second_assignment_count"]),
            "control_prefix_parity_match": trace.get("control_prefix_parity_match"),
            "control_prefix_fill_count": trace.get("control_prefix_fill_count"),
            "arm_washout_complete": bool(trace["arm_washout_complete"]),
            "arm_end_ts_ms": int(trace["terminal_ts_ms"]),
            "arm_end_reason": str(trace["terminal_reason"]),
            "right_censored": bool(trace["right_censored"]),
            "arm_end_inventory_btc": float(trace["terminal_inventory_btc"]),
            "assignment_to_washout_value_usdc": trace.get("assignment_to_washout_value_usdc"),
            "censor_time_mid_mark_usdc": trace.get("censor_time_mid_mark_usdc"),
            "censor_time_executable_mark_usdc": trace.get("censor_time_executable_mark_usdc"),
            "censor_marks_are_terminal_bounds": bool(trace["censor_marks_are_terminal_bounds"]),
            "post_assignment_buy_fill_count": int(trace["post_assignment_buy_fill_count"]),
            "post_assignment_sell_fill_count": int(trace["post_assignment_sell_fill_count"]),
            "inventory_time_btc_s": float(trace["inventory_time_btc_s"]),
            "mae_usdc": float(trace["mae_usdc"]),
            "max_abs_inventory_btc": float(trace["max_abs_inventory_btc"]),
            "active_or_pending_order_count": int(trace["active_or_pending_order_count"]),
            "pending_submit_count": int(trace["pending_submit_count"]),
            "pending_cancel_count": int(trace["pending_cancel_count"]),
            "pending_ack_count": int(trace["pending_ack_count"]),
            "campaign_active_at_terminal": bool(trace["campaign_active"]),
            "cursor_owner_count": int(trace["cursor_owner_count"]),
            "hazard_owner_count": int(trace["hazard_owner_count"]),
            "accounting_residual_usdc": float(trace["accounting_residual_usdc"]),
            "arm_wall_seconds": float(arm_wall_seconds),
        }
    )
    return row


def _annotate_joint_outcomes(
    arm_traces: pd.DataFrame,
    contract: Mapping[str, Any],
) -> pd.DataFrame:
    """Bind all side-valid duration arms into one indivisible opportunity outcome."""

    required = {
        "opportunity_id",
        "campaign_side_id",
        "assignment_ts_ns",
        "side",
        "task_id",
        "duration_policy_id",
        "assignment_state_sha256",
        "arm_washout_complete",
        "right_censored",
        "arm_end_ts_ms",
        "assignment_to_washout_value_usdc",
        "censor_time_mid_mark_usdc",
        "censor_time_executable_mark_usdc",
        "censor_marks_are_terminal_bounds",
    }
    missing = sorted(required - set(arm_traces.columns))
    if missing:
        raise StudyError(f"joint outcome arm trace schema is missing: {missing}")
    if arm_traces.empty:
        raise StudyError("joint outcome panel is empty")
    if arm_traces["task_id"].duplicated().any():
        raise StudyError("joint outcome panel contains duplicate arm tasks")
    if arm_traces["censor_marks_are_terminal_bounds"].astype(bool).any():
        raise StudyError("censor-time marks cannot be terminal bounds")

    output = arm_traces.copy()
    output["joint_action_count"] = 0
    output["joint_washout_complete"] = False
    output["joint_censored"] = True
    output["joint_censor_reason"] = "unclassified"
    output["washout_ts_ns"] = pd.Series(pd.NA, index=output.index, dtype="Int64")
    output["washout_ts_is_joint_economic_washout"] = False
    output["training_label_eligible"] = False
    output["joint_outcome_sha256"] = ""

    expected_actions = {
        side: tuple(action.policy_id for action in _duration_actions(contract, side))
        for side in ("BUY", "SELL")
    }
    for (opportunity_id, side), rows in output.groupby(
        ["opportunity_id", "side"], observed=True, sort=False
    ):
        side = str(side)
        expected = set(expected_actions[side])
        if len(rows) != len(expected) or set(rows["duration_policy_id"]) != expected:
            raise StudyError(f"{opportunity_id} lacks the complete {side} duration arm universe")
        if rows["assignment_state_sha256"].nunique() != 1:
            raise StudyError(f"{opportunity_id} duration arms do not share assignment state")
        if rows["campaign_side_id"].nunique() != 1 or rows["assignment_ts_ns"].nunique() != 1:
            raise StudyError(f"{opportunity_id} lineage clock metadata drifted")

        right_censored = rows["right_censored"].astype(bool)
        washout_complete = rows["arm_washout_complete"].astype(bool)
        washout_value_present = rows["assignment_to_washout_value_usdc"].notna()
        censor_mid_present = rows["censor_time_mid_mark_usdc"].notna()
        censor_executable_present = rows["censor_time_executable_mark_usdc"].notna()
        if (
            (right_censored & (washout_complete | washout_value_present)).any()
            or (right_censored & ~(censor_mid_present & censor_executable_present)).any()
            or (
                ~right_censored
                & (
                    ~washout_complete
                    | ~washout_value_present
                    | censor_mid_present
                    | censor_executable_present
                )
            ).any()
        ):
            raise StudyError(f"{opportunity_id} arm censor/washout fields are inconsistent")

        joint_censored = bool(right_censored.any() or (~washout_complete).any())
        joint_washout_complete = not joint_censored
        if right_censored.any():
            reason = "one_or_more_arms_right_censored"
        elif (~washout_complete).any():
            reason = "one_or_more_arms_missing_economic_washout"
        else:
            reason = "joint_economic_washout_complete"
        joint_sha = _canonical_sha256(
            {
                "opportunity_id": str(opportunity_id),
                "side": side,
                "campaign_side_id": str(rows["campaign_side_id"].iloc[0]),
                "assignment_ts_ns": int(rows["assignment_ts_ns"].iloc[0]),
                "assignment_state_sha256": str(rows["assignment_state_sha256"].iloc[0]),
                "arms": [
                    {
                        "task_id": str(row.task_id),
                        "duration_policy_id": str(row.duration_policy_id),
                        "right_censored": bool(row.right_censored),
                        "arm_washout_complete": bool(row.arm_washout_complete),
                    }
                    for row in rows.sort_values("duration_policy_id").itertuples(index=False)
                ],
            }
        )
        index = rows.index
        output.loc[index, "joint_action_count"] = len(rows)
        output.loc[index, "joint_washout_complete"] = joint_washout_complete
        output.loc[index, "joint_censored"] = joint_censored
        output.loc[index, "joint_censor_reason"] = reason
        output.loc[index, "training_label_eligible"] = joint_washout_complete
        output.loc[index, "joint_outcome_sha256"] = joint_sha
        output.loc[index, "washout_ts_ns"] = int(rows["arm_end_ts_ms"].max()) * 1_000_000
        output.loc[index, "washout_ts_is_joint_economic_washout"] = joint_washout_complete
    return output


def _scope_name(limit: int | None, *, python_parity: bool = False) -> str:
    if python_parity:
        if limit is None:
            raise StudyError("Python parity requires an explicit diagnostic --limit")
        return f"parity-limit-{limit}"
    return "formal" if limit is None else f"diagnostic-limit-{limit}"


def _chunk_paths(output: Path, scope: str, day: str, chunk_index: int) -> tuple[Path, Path]:
    root = Path(output) / "runs" / scope / day / "chunks"
    stem = f"chunk-{chunk_index:05d}"
    return root / f"{stem}.parquet", root / f"{stem}.manifest.json"


def _load_chunk(
    *,
    output: Path,
    scope: str,
    day: str,
    chunk_index: int,
    expected_task_ids: Sequence[str],
    execution_identity_sha256: str,
) -> pd.DataFrame:
    data_path, manifest_path = _chunk_paths(output, scope, day, chunk_index)
    if not data_path.is_file() or not manifest_path.is_file():
        raise StudyError(f"missing checkpoint chunk {day}/{chunk_index}")
    manifest = _load_json(manifest_path)
    expected_python_parity = scope.startswith("parity-limit-")
    if (
        manifest.get("identity") != IDENTITY
        or manifest.get("scope") != scope
        or manifest.get("utc_day") != day
        or int(manifest.get("chunk_index", -1)) != chunk_index
        or manifest.get("execution_identity_sha256") != execution_identity_sha256
        or manifest.get("task_set_sha256") != _canonical_sha256(list(expected_task_ids))
        or manifest.get("formal_replay_engine") != FORMAL_REPLAY_ENGINE
        or manifest.get("python_parity") is not expected_python_parity
    ):
        raise StudyError(f"checkpoint chunk identity drifted: {manifest_path}")
    _validate_file(data_path, manifest["data_sha256"], role="fork checkpoint")
    frame = pd.read_parquet(data_path)
    if set(frame["task_id"]) != set(expected_task_ids) or len(frame) != len(expected_task_ids):
        raise StudyError("checkpoint chunk task membership drifted")
    if not frame["replay_engine"].eq(FORMAL_REPLAY_ENGINE).all():
        raise StudyError("checkpoint chunk is not C++ authoritative")
    if expected_python_parity and not (
        frame["python_parity_checked"].astype(bool).all()
        and frame["python_parity_match"].astype(bool).all()
    ):
        raise StudyError("Python parity checkpoint contains a mismatch")
    return frame


def _run_chunk(
    *,
    output: Path,
    scope: str,
    day: str,
    chunk_index: int,
    tasks: Sequence[tuple[dict[str, Any], DurationAction, str]],
    window: Any,
    base: Mapping[str, Any],
    shared: Mapping[str, Any],
    execution_identity_sha256: str,
    continuation_audit: Mapping[str, Any],
    python_parity: bool,
    authoritative_control_fills: Sequence[Mapping[str, Any]],
) -> pd.DataFrame:
    task_ids = [task_id for _, _, task_id in tasks]
    data_path, manifest_path = _chunk_paths(output, scope, day, chunk_index)
    if data_path.is_file() and manifest_path.is_file():
        return _load_chunk(
            output=output,
            scope=scope,
            day=day,
            chunk_index=chunk_index,
            expected_task_ids=task_ids,
            execution_identity_sha256=execution_identity_sha256,
        )
    rows = []
    for opportunity, action, task_id in tasks:
        trace, elapsed = _run_duration_arm(
            opportunity,
            action,
            window=window,
            base=base,
            shared=shared,
            engine=FORMAL_REPLAY_ENGINE,
            authoritative_control_fills=authoritative_control_fills,
        )
        row = _label_row(
            opportunity,
            action,
            task_id,
            trace,
            arm_wall_seconds=elapsed,
            replay_engine=FORMAL_REPLAY_ENGINE,
        )
        row["python_parity_checked"] = False
        row["python_parity_match"] = None
        row["python_parity_trace_sha256"] = None
        row["python_parity_wall_seconds"] = None
        if python_parity:
            python_trace, python_elapsed = _run_duration_arm(
                opportunity,
                action,
                window=window,
                base=base,
                shared=shared,
                engine="python",
                authoritative_control_fills=authoritative_control_fills,
            )
            row["python_parity_checked"] = True
            row["python_parity_trace_sha256"] = _assert_cpp_python_trace_parity(trace, python_trace)
            row["python_parity_match"] = True
            row["python_parity_wall_seconds"] = python_elapsed
        rows.append(row)
    frame = pd.DataFrame(rows)
    _atomic_parquet(data_path, frame)
    manifest = {
        "schema_version": f"{SCHEMA_VERSION}.fork_checkpoint_chunk",
        "identity": IDENTITY,
        "scope": scope,
        "utc_day": day,
        "chunk_index": chunk_index,
        "execution_identity_sha256": execution_identity_sha256,
        "task_set_sha256": _canonical_sha256(task_ids),
        "task_count": len(task_ids),
        "data_path": str(data_path),
        "data_sha256": _sha256_file(data_path),
        "arm_wall_seconds_sum": float(frame["arm_wall_seconds"].sum()),
        "continuation_audit": continuation_audit,
        "formal_replay_engine": FORMAL_REPLAY_ENGINE,
        "python_parity": python_parity,
        "economic_outcomes_generated": True,
        "economic_outcomes_interpreted": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    _atomic_json(manifest_path, manifest)
    return frame


def _day_run_paths(output: Path, scope: str, day: str) -> tuple[Path, Path, Path]:
    root = Path(output) / "runs" / scope / day
    return root / "arm_traces.parquet", root / "manifest.json", root / "_SUCCESS"


def run_day(
    day: str,
    *,
    output: Path = DEFAULT_OUTPUT,
    limit: int | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    chunk_index: int | None = None,
    python_parity: bool = False,
) -> dict[str, Any]:
    if chunk_size <= 0:
        raise StudyError("chunk size must be positive")
    if python_parity and limit is None:
        raise StudyError("Python parity is restricted to an explicit --limit subset")
    _require_cpp_duration_abi()
    contract, _, _ = _load_contract()
    execution = _execution_identity(contract)
    if limit is None:
        _require_parity_admission(output, execution["execution_identity_sha256"])
        _require_control_noop_admission(output)
    opportunities, census_manifest = _load_census_day(day, output=output, contract=contract)
    tasks = _build_tasks(opportunities, contract, limit=limit)
    scope = _scope_name(limit, python_parity=python_parity)
    chunks = [tasks[index : index + chunk_size] for index in range(0, len(tasks), chunk_size)]
    if not chunks:
        raise StudyError(f"{day} has no duration fork tasks")
    if chunk_index is not None and (chunk_index < 0 or chunk_index >= len(chunks)):
        raise StudyError("requested chunk index is outside the task plan")
    window, base, shared, continuation_audit = _load_arm_replay(day)
    control_params = _prepare_base_params(base, trace_opportunities=False)
    control_params["trace_fills_max"] = TRACE_LIMIT
    control_result = bt._simulate_tick_with_engine(
        FORMAL_REPLAY_ENGINE,
        window.trades,
        window.var_ts_ms,
        window.var_ssq,
        control_params,
        **shared,
    )
    authoritative_control_fills = tuple(control_result.get("_fill_trace") or ())
    indexes = range(len(chunks)) if chunk_index is None else (chunk_index,)
    for index in indexes:
        _run_chunk(
            output=output,
            scope=scope,
            day=day,
            chunk_index=index,
            tasks=chunks[index],
            window=window,
            base=base,
            shared=shared,
            execution_identity_sha256=execution["execution_identity_sha256"],
            continuation_audit=continuation_audit,
            python_parity=python_parity,
            authoritative_control_fills=authoritative_control_fills,
        )
    missing_chunks = []
    chunk_frames = []
    for index, chunk in enumerate(chunks):
        try:
            chunk_frames.append(
                _load_chunk(
                    output=output,
                    scope=scope,
                    day=day,
                    chunk_index=index,
                    expected_task_ids=[task_id for _, _, task_id in chunk],
                    execution_identity_sha256=execution["execution_identity_sha256"],
                )
            )
        except StudyError:
            missing_chunks.append(index)
    if missing_chunks:
        return {
            "identity": IDENTITY,
            "utc_day": day,
            "scope": scope,
            "status": "checkpoint_progress_not_admitted",
            "chunk_count": len(chunks),
            "missing_chunk_indexes": missing_chunks,
            "limited_diagnostic": limit is not None,
            "python_parity": python_parity,
            "formal_authority": False,
        }
    labels = pd.concat(chunk_frames, ignore_index=True)
    expected_task_ids = [task_id for _, _, task_id in tasks]
    if len(labels) != len(expected_task_ids) or set(labels["task_id"]) != set(expected_task_ids):
        raise StudyError("day label admission lacks exact task coverage")
    labels = _annotate_joint_outcomes(labels, contract)
    joint_status = labels.drop_duplicates("opportunity_id")
    data_path, manifest_path, success_path = _day_run_paths(output, scope, day)
    _atomic_parquet(data_path, labels)
    manifest = {
        "schema_version": f"{SCHEMA_VERSION}.arm_trace_day_admission",
        "identity": IDENTITY,
        "scope": scope,
        "utc_day": day,
        "execution_identity_sha256": execution["execution_identity_sha256"],
        "census_data_sha256": census_manifest["data_sha256"],
        "census_opportunity_count": int(len(opportunities)),
        "included_opportunity_count": int(labels["opportunity_id"].nunique()),
        "expected_task_count": len(expected_task_ids),
        "arm_trace_row_count": int(len(labels)),
        "task_set_sha256": _canonical_sha256(expected_task_ids),
        "data_path": str(data_path),
        "data_sha256": _sha256_file(data_path),
        "chunk_size": chunk_size,
        "chunk_count": len(chunks),
        "limited_diagnostic": limit is not None,
        "python_parity": python_parity,
        "formal_replay_engine": FORMAL_REPLAY_ENGINE,
        "python_full_arm_execution_allowed": False,
        "fill_clock_semantics": FILL_CLOCK_SEMANTICS,
        "live_receive_time_authority": False,
        "limit": limit,
        "formal_full_opportunity_coverage": bool(
            limit is None and labels["opportunity_id"].nunique() == len(opportunities)
        ),
        "continuation_audit": continuation_audit,
        "right_censored_rows": int(labels["right_censored"].sum()),
        "joint_washout_opportunities": int(
            joint_status["joint_washout_complete"].astype(bool).sum()
        ),
        "joint_censored_opportunities": int(joint_status["joint_censored"].astype(bool).sum()),
        "training_label_opportunities": int(
            joint_status["training_label_eligible"].astype(bool).sum()
        ),
        "censor_marks_are_terminal_bounds": False,
        "joint_complete_case_filtering_allowed": False,
        "economic_outcomes_generated": True,
        "economic_outcomes_interpreted": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    _atomic_json(manifest_path, manifest)
    _atomic_text(success_path, f"{_sha256_file(manifest_path)}\n")
    return manifest


def run_days(
    days: Sequence[str],
    *,
    workers: int,
    output: Path = DEFAULT_OUTPUT,
    limit: int | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    chunk_index: int | None = None,
    python_parity: bool = False,
) -> list[dict[str, Any]]:
    kwargs = {
        "output": output,
        "limit": limit,
        "chunk_size": chunk_size,
        "chunk_index": chunk_index,
        "python_parity": python_parity,
    }
    if workers <= 1:
        return [run_day(day, **kwargs) for day in days]
    results = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(run_day, day, **kwargs): day for day in days}
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
    return sorted(results, key=lambda row: str(row["utc_day"]))


def summarize_parity(
    days: Sequence[str], *, limit: int, output: Path = DEFAULT_OUTPUT
) -> dict[str, Any]:
    if limit <= 0:
        raise StudyError("parity requires a positive diagnostic limit")
    contract, _, _ = _load_contract()
    execution_identity_sha256 = _execution_identity(contract)["execution_identity_sha256"]
    scope = _scope_name(limit, python_parity=True)
    frames = []
    parts = []
    for day in days:
        data_path, manifest_path, success_path = _day_run_paths(output, scope, day)
        if not data_path.is_file() or not manifest_path.is_file() or not success_path.is_file():
            raise StudyError(f"missing parity day admission: {day}")
        manifest = _load_json(manifest_path)
        if (
            manifest.get("identity") != IDENTITY
            or manifest.get("scope") != scope
            or manifest.get("utc_day") != day
            or manifest.get("execution_identity_sha256") != execution_identity_sha256
            or manifest.get("python_parity") is not True
            or manifest.get("formal_replay_engine") != FORMAL_REPLAY_ENGINE
        ):
            raise StudyError(f"parity day identity drifted: {day}")
        _validate_file(data_path, manifest["data_sha256"], role=f"{day} parity arms")
        frame = pd.read_parquet(
            data_path,
            columns=[
                "task_id",
                "opportunity_id",
                "side",
                "role_at_fill",
                "python_parity_checked",
                "python_parity_match",
                "python_parity_trace_sha256",
            ],
        )
        if not (
            frame["python_parity_checked"].astype(bool).all()
            and frame["python_parity_match"].astype(bool).all()
            and frame["python_parity_trace_sha256"].notna().all()
        ):
            raise StudyError(f"parity mismatch is present on {day}")
        frames.append(frame)
        parts.append(
            {
                "utc_day": day,
                "data_path": str(data_path),
                "data_sha256": manifest["data_sha256"],
                "manifest_path": str(manifest_path),
                "manifest_sha256": _sha256_file(manifest_path),
                "arm_rows": int(len(frame)),
            }
        )
    panel = pd.concat(frames, ignore_index=True)
    required_cells = {
        ("BUY", "opener"),
        ("BUY", "add"),
        ("SELL", "opener"),
        ("SELL", "add"),
    }
    observed_cells = {
        (str(side), str(role))
        for side, role in panel[["side", "role_at_fill"]].itertuples(index=False)
    }
    required_cells_present = required_cells <= observed_cells
    payload = {
        "schema_version": f"{SCHEMA_VERSION}.cpp_python_parity_manifest",
        "identity": IDENTITY,
        "status": (
            "cpp_python_parity_subset_admitted"
            if required_cells_present
            else "cpp_python_parity_subset_incomplete"
        ),
        "execution_identity_sha256": execution_identity_sha256,
        "ordered_utc_days": list(days),
        "diagnostic_limit_per_day": limit,
        "subset_selection": "sha256_ordered_diagnostic_opportunities",
        "parity_arm_rows": int(len(panel)),
        "parity_opportunities": int(panel["opportunity_id"].nunique()),
        "all_rows_match": True,
        "required_side_role_cells": sorted(f"{side}:{role}" for side, role in required_cells),
        "observed_side_role_cells": sorted(f"{side}:{role}" for side, role in observed_cells),
        "required_side_role_cells_present": required_cells_present,
        "parts": parts,
        "economic_outcomes_interpreted": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    _atomic_json(_parity_manifest_path(output), payload)
    return payload


def _validate_formal_run_manifest(manifest: Mapping[str, Any], *, day: str) -> None:
    if manifest.get("identity") != IDENTITY or manifest.get("utc_day") != day:
        raise StudyError(f"{day} run identity drifted")
    if manifest.get("scope") != "formal" or manifest.get("limited_diagnostic") is not False:
        raise StudyError("limited diagnostic run cannot enter formal finalize")
    if (
        manifest.get("formal_replay_engine") != FORMAL_REPLAY_ENGINE
        or manifest.get("python_parity") is not False
        or manifest.get("python_full_arm_execution_allowed") is not False
        or manifest.get("fill_clock_semantics") != FILL_CLOCK_SEMANTICS
        or manifest.get("live_receive_time_authority") is not False
    ):
        raise StudyError(f"{day} formal execution authority drifted")
    if manifest.get("formal_full_opportunity_coverage") is not True:
        raise StudyError(f"{day} formal run did not cover every opportunity")
    if (
        manifest.get("censor_marks_are_terminal_bounds") is not False
        or manifest.get("joint_complete_case_filtering_allowed") is not False
    ):
        raise StudyError(f"{day} joint censoring contract drifted")


MECHANICS_COLUMNS = (
    "task_id",
    "replay_engine",
    "opportunity_id",
    "campaign_side_id",
    "campaign_id",
    "assignment_ts_ns",
    "utc_day",
    "side",
    "role_at_fill",
    "duration_policy_id",
    "duration_engine_action",
    "fixed_duration_s",
    "fixed_duration_ms",
    "assignment_state_sha256",
    "fork_assignment_ts_ms",
    "fork_baseline_duration_ms",
    "fork_applied_duration_ms",
    "washout_protocol",
    "control_path_exact_until_quarantine",
    "exposure_permission_change_count",
    "reducing_permission_control_checks",
    "reducing_quote_change_count",
    "second_assignment_count",
    "control_prefix_parity_match",
    "control_prefix_fill_count",
    "arm_washout_complete",
    "right_censored",
    "arm_end_ts_ms",
    "arm_end_inventory_btc",
    "censor_marks_are_terminal_bounds",
    "joint_action_count",
    "joint_washout_complete",
    "joint_censored",
    "joint_censor_reason",
    "washout_ts_ns",
    "washout_ts_is_joint_economic_washout",
    "training_label_eligible",
    "joint_outcome_sha256",
    "active_or_pending_order_count",
    "pending_submit_count",
    "pending_cancel_count",
    "pending_ack_count",
    "campaign_active_at_terminal",
    "cursor_owner_count",
    "hazard_owner_count",
    "accounting_residual_usdc",
    "arm_wall_seconds",
)


def _validate_predicate_schema(path: Path, contract: Mapping[str, Any]) -> str:
    schema = pq.ParquetFile(path).schema_arrow
    actual_names = [name for name in schema.names if name.startswith("predicate::")]
    expected_names = [f"predicate::{row['name']}" for row in contract["atomic_predicates"]]
    if (
        len(actual_names) != EXPECTED_PREDICATE_COLUMNS
        or len(set(actual_names)) != EXPECTED_PREDICATE_COLUMNS
        or set(actual_names) != set(expected_names)
    ):
        raise StudyError(f"{path} does not contain the exact frozen 360-predicate interface")
    for name in actual_names:
        if not pa_types.is_boolean(schema.field(name).type):
            raise StudyError(f"predicate column is not boolean: {name}")
    return _canonical_sha256(actual_names)


def _formal_parts(
    *, output: Path, contract: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], pd.DataFrame]:
    parts = []
    mechanics = []
    for day in contract["baseline_projection"]["ordered_utc_days"]:
        opportunities, census_manifest = _load_census_day(day, output=output, contract=contract)
        data_path, manifest_path, success_path = _day_run_paths(output, "formal", day)
        if not data_path.is_file() or not manifest_path.is_file() or not success_path.is_file():
            raise StudyError(f"missing formal day admission: {day}")
        manifest = _load_json(manifest_path)
        _validate_formal_run_manifest(manifest, day=day)
        _validate_file(data_path, manifest["data_sha256"], role=f"{day} labels")
        predicate_schema_sha256 = _validate_predicate_schema(data_path, contract)
        expected_tasks = _build_tasks(opportunities, contract, limit=None)
        expected_ids = [task_id for _, _, task_id in expected_tasks]
        frame = pd.read_parquet(data_path, columns=list(MECHANICS_COLUMNS))
        if len(frame) != len(expected_ids) or set(frame["task_id"]) != set(expected_ids):
            raise StudyError(f"{day} formal task Cartesian product drifted")
        expected_actions = {
            side: {action.policy_id for action in _duration_actions(contract, side)}
            for side in ("BUY", "SELL")
        }
        for (opportunity_id, side), rows in frame.groupby(
            ["opportunity_id", "side"], observed=True
        ):
            if set(rows["duration_policy_id"]) != expected_actions[str(side)]:
                raise StudyError(f"{day}/{opportunity_id} lacks the frozen action universe")
            if rows["assignment_state_sha256"].nunique() != 1:
                raise StudyError("duration arms do not share assignment state")
            if len(rows) != EXPECTED_ACTIONS_PER_SIDE:
                raise StudyError(f"{day}/{opportunity_id} does not have exactly eight arms")
            for field in (
                "joint_action_count",
                "joint_washout_complete",
                "joint_censored",
                "joint_censor_reason",
                "washout_ts_ns",
                "washout_ts_is_joint_economic_washout",
                "training_label_eligible",
                "joint_outcome_sha256",
            ):
                if rows[field].nunique(dropna=False) != 1:
                    raise StudyError(f"{day}/{opportunity_id} joint field {field} drifted")
            expected_joint_censored = bool(
                rows["right_censored"].astype(bool).any()
                or (~rows["arm_washout_complete"].astype(bool)).any()
            )
            expected_joint_complete = bool(
                not expected_joint_censored and rows["arm_washout_complete"].astype(bool).all()
            )
            if (
                bool(rows["joint_censored"].iloc[0]) != expected_joint_censored
                or bool(rows["joint_washout_complete"].iloc[0]) != expected_joint_complete
                or bool(rows["washout_ts_is_joint_economic_washout"].iloc[0])
                != expected_joint_complete
                or bool(rows["training_label_eligible"].iloc[0]) != expected_joint_complete
            ):
                raise StudyError(f"{day}/{opportunity_id} joint washout status drifted")
            expected_end_ns = int(rows["arm_end_ts_ms"].max()) * 1_000_000
            if int(rows["washout_ts_ns"].iloc[0]) != expected_end_ns:
                raise StudyError(f"{day}/{opportunity_id} joint end clock drifted")
            if not rows["replay_engine"].eq(FORMAL_REPLAY_ENGINE).all():
                raise StudyError(f"{day}/{opportunity_id} is not C++ authoritative")
            if (
                not rows["washout_protocol"].eq(
                    "first_flat_exposure_quarantine_scheduler_drained_v2"
                ).all()
                or not rows["control_path_exact_until_quarantine"].astype(bool).all()
                or rows["reducing_quote_change_count"].astype(np.int64).ne(0).any()
                or rows["second_assignment_count"].astype(np.int64).ne(0).any()
            ):
                raise StudyError(f"{day}/{opportunity_id} permission/washout gate failed")
            control_rows = rows.loc[rows["duration_policy_id"].eq("CONTROL_85N")]
            if (
                len(control_rows) != 1
                or not bool(control_rows["control_prefix_parity_match"].iloc[0])
            ):
                raise StudyError(f"{day}/{opportunity_id} CONTROL prefix parity failed")
            expected_campaign_side = f"{day}:{int(rows['campaign_id'].iloc[0])}:{str(side)}"
            if (
                not rows["campaign_side_id"].eq(expected_campaign_side).all()
                or not (
                    rows["assignment_ts_ns"].astype(np.int64)
                    == rows["fork_assignment_ts_ms"].astype(np.int64) * 1_000_000
                ).all()
            ):
                raise StudyError(f"{day}/{opportunity_id} model lineage interface drifted")
        completed = frame.loc[frame["arm_washout_complete"].astype(bool)]
        zero_fields = (
            "active_or_pending_order_count",
            "pending_submit_count",
            "pending_cancel_count",
            "pending_ack_count",
            "cursor_owner_count",
            "hazard_owner_count",
        )
        if any(int(completed[field].abs().max()) != 0 for field in zero_fields if len(completed)):
            raise StudyError(f"{day} completed arm retained lifecycle state")
        if len(completed) and (
            completed["campaign_active_at_terminal"].astype(bool).any()
            or completed["arm_end_inventory_btc"].abs().max() > 1e-10
        ):
            raise StudyError(f"{day} completed arm retained economic state")
        if len(completed) and completed["accounting_residual_usdc"].abs().max() > 1e-6:
            raise StudyError(f"{day} completed arm accounting residual exceeded 1e-6")
        mechanics.append(frame)
        parts.append(
            {
                "utc_day": day,
                "census_path": census_manifest["data_path"],
                "census_sha256": census_manifest["data_sha256"],
                "arm_trace_path": str(data_path),
                "arm_trace_sha256": manifest["data_sha256"],
                "opportunity_count": int(len(opportunities)),
                "arm_trace_row_count": int(len(frame)),
                "predicate_column_count": EXPECTED_PREDICATE_COLUMNS,
                "predicate_schema_sha256": predicate_schema_sha256,
                "manifest_path": str(manifest_path),
                "manifest_sha256": _sha256_file(manifest_path),
            }
        )
    return parts, pd.concat(mechanics, ignore_index=True)


def _economic_summary(
    *, output: Path, contract: Mapping[str, Any], parts: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    columns = (
        "opportunity_id",
        "utc_day",
        "side",
        "role_at_fill",
        "duration_policy_id",
        "joint_washout_complete",
        "joint_censored",
        "training_label_eligible",
        "assignment_to_washout_value_usdc",
        "post_assignment_buy_fill_count",
        "post_assignment_sell_fill_count",
        "inventory_time_btc_s",
        "mae_usdc",
        "max_abs_inventory_btc",
    )
    frames = [
        pd.read_parquet(Path(str(part["arm_trace_path"])), columns=list(columns)) for part in parts
    ]
    panel = pd.concat(frames, ignore_index=True)
    joint = panel.drop_duplicates("opportunity_id")
    eligible_ids = set(
        joint.loc[joint["training_label_eligible"].astype(bool), "opportunity_id"].astype(str)
    )
    panel = panel.loc[panel["opportunity_id"].astype(str).isin(eligible_ids)].copy()
    if panel.empty:
        raise StudyError("economic panel has no jointly eligible opportunity")
    if (
        panel["joint_censored"].astype(bool).any()
        or not panel["joint_washout_complete"].astype(bool).all()
        or not panel["training_label_eligible"].astype(bool).all()
        or panel["assignment_to_washout_value_usdc"].isna().any()
    ):
        raise StudyError("eligible economic panel contains an incomplete joint outcome")
    reports = []
    for (side, role), cell in panel.groupby(["side", "role_at_fill"], observed=True):
        control = cell.loc[cell["duration_policy_id"].eq("CONTROL_85N")].copy()
        for action in _duration_actions(contract, str(side)):
            if action.policy_id == "CONTROL_85N":
                continue
            candidate = cell.loc[cell["duration_policy_id"].eq(action.policy_id)].copy()
            merged = control.merge(
                candidate,
                on=["opportunity_id", "utc_day", "side", "role_at_fill"],
                suffixes=("_control", "_candidate"),
                validate="one_to_one",
            )
            washout_delta = (
                merged["assignment_to_washout_value_usdc_candidate"]
                - merged["assignment_to_washout_value_usdc_control"]
            )
            reports.append(
                {
                    "side": str(side),
                    "role_at_fill": str(role),
                    "duration_policy_id": action.policy_id,
                    "paired_opportunities": int(len(merged)),
                    "joint_washout_pairs": int(len(merged)),
                    "joint_censored_pairs": 0,
                    "mean_assignment_to_washout_value_delta_usdc": (
                        float(washout_delta.mean()) if len(merged) else None
                    ),
                    "sum_assignment_to_washout_value_delta_usdc": (
                        float(washout_delta.sum()) if len(merged) else None
                    ),
                    "mean_post_buy_fill_count_delta": (
                        float(
                            (
                                merged["post_assignment_buy_fill_count_candidate"]
                                - merged["post_assignment_buy_fill_count_control"]
                            ).mean()
                        )
                        if len(merged)
                        else None
                    ),
                    "mean_post_sell_fill_count_delta": (
                        float(
                            (
                                merged["post_assignment_sell_fill_count_candidate"]
                                - merged["post_assignment_sell_fill_count_control"]
                            ).mean()
                        )
                        if len(merged)
                        else None
                    ),
                    "mean_inventory_time_delta_btc_s": (
                        float(
                            (
                                merged["inventory_time_btc_s_candidate"]
                                - merged["inventory_time_btc_s_control"]
                            ).mean()
                        )
                        if len(merged)
                        else None
                    ),
                    "mean_mae_delta_usdc": (
                        float((merged["mae_usdc_candidate"] - merged["mae_usdc_control"]).mean())
                        if len(merged)
                        else None
                    ),
                    "mean_max_abs_inventory_delta_btc": (
                        float(
                            (
                                merged["max_abs_inventory_btc_candidate"]
                                - merged["max_abs_inventory_btc_control"]
                            ).mean()
                        )
                        if len(merged)
                        else None
                    ),
                }
            )
    return {
        "schema_version": f"{SCHEMA_VERSION}.development_economic_report",
        "identity": IDENTITY,
        "status": "development_economic_outcomes_read_explicitly",
        "joint_outcome_only": True,
        "materialized_opportunities": int(len(joint)),
        "training_label_opportunities": int(len(eligible_ids)),
        "whole_opportunity_censor_exclusions": int(len(joint) - len(eligible_ids)),
        "complete_case_filtering_used": False,
        "censor_time_marks_used_as_terminal_bounds": False,
        "economic_outcomes_read": True,
        "model_trained": False,
        "linear_model_trained": False,
        "add_wait_target_used": False,
        "cell_reports": reports,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }


def finalize(
    *,
    output: Path = DEFAULT_OUTPUT,
    read_economic_outcomes: bool = False,
    limit: int | None = None,
) -> dict[str, Any]:
    if limit is not None:
        raise StudyError("finalize refuses every --limit diagnostic run")
    contract, _, _ = _load_contract()
    census_manifest_path = Path(output) / "census_manifest.json"
    if not census_manifest_path.is_file():
        raise StudyError("formal 40-day census manifest is missing")
    census_manifest = _load_json(census_manifest_path)
    if (
        census_manifest.get("status") != "formal_full_development_census"
        or census_manifest.get("formal_sampling") != "none_full_coverage"
        or census_manifest.get("formal_replay_engine") != FORMAL_REPLAY_ENGINE
        or census_manifest.get("fill_clock_semantics") != FILL_CLOCK_SEMANTICS
        or census_manifest.get("live_receive_time_authority") is not False
        or int(census_manifest.get("opportunity_count", -1)) != EXPECTED_FORMAL_OPPORTUNITIES
        or int(census_manifest.get("exact_formal_fork_task_count", -1)) != EXPECTED_FORMAL_ARM_ROWS
        or census_manifest.get("ordered_utc_days")
        != contract["baseline_projection"]["ordered_utc_days"]
    ):
        raise StudyError("formal census denominator drifted")
    parts, mechanics = _formal_parts(output=output, contract=contract)
    opportunity_count = int(mechanics["opportunity_id"].nunique())
    arm_count = int(len(mechanics))
    if opportunity_count != EXPECTED_FORMAL_OPPORTUNITIES or arm_count != EXPECTED_FORMAL_ARM_ROWS:
        raise StudyError("formal arm panel is not exactly 8,600 opportunities x 8 arms")
    joint_status = mechanics.drop_duplicates("opportunity_id")
    if len(joint_status) != EXPECTED_FORMAL_OPPORTUNITIES:
        raise StudyError("joint opportunity panel denominator drifted")
    joint_censored = joint_status["joint_censored"].astype(bool)
    joint_complete = joint_status["joint_washout_complete"].astype(bool)
    training_eligible = joint_status["training_label_eligible"].astype(bool)
    arm_manifest = {
        "schema_version": f"{SCHEMA_VERSION}.arm_trace_part_manifest",
        "identity": IDENTITY,
        "status": "formal_full_development_arm_traces_admitted",
        "execution_identity_sha256": _execution_identity(contract)["execution_identity_sha256"],
        "census_manifest_path": str(census_manifest_path),
        "census_manifest_sha256": _sha256_file(census_manifest_path),
        "ordered_utc_days": contract["baseline_projection"]["ordered_utc_days"],
        "opportunity_count": opportunity_count,
        "arm_trace_rows": arm_count,
        "expected_actions_per_side": EXPECTED_ACTIONS_PER_SIDE,
        "joint_washout_opportunities": int(joint_complete.sum()),
        "joint_censored_opportunities": int(joint_censored.sum()),
        "training_label_opportunities": int(training_eligible.sum()),
        "parts": parts,
        "formal_sampling": "none_full_cartesian_coverage",
        "formal_replay_engine": FORMAL_REPLAY_ENGINE,
        "python_full_arm_execution_allowed": False,
        "fill_clock_semantics": FILL_CLOCK_SEMANTICS,
        "live_receive_time_authority": False,
        "censor_marks_are_terminal_bounds": False,
        "joint_complete_case_filtering_allowed": False,
        "economic_outcomes_generated": True,
        "economic_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    arm_manifest_path = Path(output) / "arm_trace_manifest.json"
    _atomic_json(arm_manifest_path, arm_manifest)
    completed = mechanics["arm_washout_complete"].astype(bool)
    mechanics_report = {
        "schema_version": f"{SCHEMA_VERSION}.mechanics_report",
        "identity": IDENTITY,
        "status": "formal_mechanics_complete_joint_censoring_reported",
        "opportunity_count": opportunity_count,
        "arm_trace_rows": arm_count,
        "completed_washout_rows": int(completed.sum()),
        "right_censored_rows": int(mechanics["right_censored"].sum()),
        "joint_washout_opportunities": int(joint_complete.sum()),
        "joint_censored_opportunities": int(joint_censored.sum()),
        "training_label_opportunities": int(training_eligible.sum()),
        "formal_replay_engine": FORMAL_REPLAY_ENGINE,
        "python_full_arm_execution_allowed": False,
        "censor_marks_are_terminal_bounds": False,
        "complete_case_filtering_used": False,
        "side_role_action_counts": [
            {
                "side": str(side),
                "role_at_fill": str(role),
                "duration_policy_id": str(action),
                "rows": int(count),
            }
            for (side, role, action), count in mechanics.groupby(
                ["side", "role_at_fill", "duration_policy_id"],
                observed=True,
            )
            .size()
            .items()
        ],
        "arm_wall_seconds_sum": float(mechanics["arm_wall_seconds"].sum()),
        "arm_wall_seconds_median": float(mechanics["arm_wall_seconds"].median()),
        "economic_outcomes_read": False,
        "model_trained": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    _atomic_json(Path(output) / "mechanics_report.json", mechanics_report)
    if not read_economic_outcomes:
        return mechanics_report
    if not (joint_censored == ~training_eligible).all():
        raise StudyError(
            "joint censoring and training eligibility are not exact complements"
        )
    if not (joint_complete == training_eligible).all():
        raise StudyError("joint washout and training eligibility drifted")
    if int(training_eligible.sum()) <= 0:
        raise StudyError("no complete joint outcome is eligible for training")
    training_manifest = {
        "schema_version": f"{SCHEMA_VERSION}.joint_outcome_training_manifest",
        "identity": IDENTITY,
        "status": (
            "formal_joint_outcome_training_panel_admitted_with_whole_"
            "opportunity_censor_exclusion"
        ),
        "execution_identity_sha256": _execution_identity(contract)["execution_identity_sha256"],
        "arm_trace_manifest_path": str(arm_manifest_path),
        "arm_trace_manifest_sha256": _sha256_file(arm_manifest_path),
        "opportunity_count": opportunity_count,
        "arm_rows": arm_count,
        "actions_per_opportunity": EXPECTED_ACTIONS_PER_SIDE,
        "all_materialized_opportunities_have_all_eight_arms": True,
        "all_opportunities_joint_washout_complete": bool(joint_complete.all()),
        "joint_censored_opportunities": int(joint_censored.sum()),
        "training_label_eligible_opportunities": int(training_eligible.sum()),
        "whole_opportunity_censor_exclusion_used": bool(joint_censored.any()),
        "label_field": "assignment_to_washout_value_usdc",
        "censor_time_marks_in_training_label": False,
        "complete_case_filtering_used": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    training_manifest_path = Path(output) / "joint_outcome_training_manifest.json"
    _atomic_json(training_manifest_path, training_manifest)
    economic = _economic_summary(output=output, contract=contract, parts=parts)
    economic["joint_outcome_training_manifest_sha256"] = _sha256_file(training_manifest_path)
    _atomic_json(Path(output) / "economic_report.json", economic)
    return economic


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=("preflight", "census", "run", "parity", "finalize"),
    )
    parser.add_argument("--days", nargs="*", default=())
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="diagnostic-only opportunity limit; never formal evidence",
    )
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--chunk-index", type=int, default=None)
    parser.add_argument(
        "--read-economic-outcomes",
        action="store_true",
        help="explicitly consume Development economic outcomes during finalize",
    )
    args = parser.parse_args()
    contract, _, _ = _load_contract()
    days = _requested_days(contract, args.days)
    if args.command in {"preflight", "census"} and args.limit is not None:
        raise StudyError("--limit is only valid for mechanics diagnostic run")
    if args.command == "parity" and args.limit is None:
        raise StudyError("parity requires an explicit diagnostic --limit")
    if args.command != "finalize" and args.read_economic_outcomes:
        raise StudyError("economic outcomes may only be read by finalize")
    if args.command == "preflight":
        payload = preflight(output=args.output)
    elif args.command == "census":
        payload = run_census(
            days,
            workers=max(1, args.workers),
            output=args.output,
        )
    elif args.command == "parity":
        parity_runs = run_days(
            days,
            workers=max(1, args.workers),
            output=args.output,
            limit=args.limit,
            chunk_size=args.chunk_size,
            chunk_index=args.chunk_index,
            python_parity=True,
        )
        if args.chunk_index is not None and any(
            run.get("status") == "checkpoint_progress_not_admitted" for run in parity_runs
        ):
            payload = {
                "identity": IDENTITY,
                "status": "parity_checkpoint_progress_not_admitted",
                "runs": parity_runs,
                "formal_evidence_admitted": False,
            }
        else:
            payload = summarize_parity(days, limit=args.limit, output=args.output)
            payload["runs"] = parity_runs
    elif args.command == "run":
        payload = {
            "identity": IDENTITY,
            "runs": run_days(
                days,
                workers=max(1, args.workers),
                output=args.output,
                limit=args.limit,
                chunk_size=args.chunk_size,
                chunk_index=args.chunk_index,
                python_parity=False,
            ),
            "limited_diagnostic": args.limit is not None,
            "python_parity": False,
            "formal_replay_engine": FORMAL_REPLAY_ENGINE,
            "economic_outcomes_interpreted": False,
            "formal_scope": args.limit is None,
            "formal_evidence_admitted": False,
        }
    else:
        payload = finalize(
            output=args.output,
            read_economic_outcomes=args.read_economic_outcomes,
            limit=args.limit,
        )
    print(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    main()
