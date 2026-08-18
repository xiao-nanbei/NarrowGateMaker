#!/usr/bin/env python3
"""Qualify the F05 C++ one-shot engine on one complete admitted real day."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import time
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import pandas as pd

from data_paths import resolve_portable_path
from models import backtest_tick as backtest
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_cpp_observation_tape_v21 as observation_tape,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_cpp_runtime_v22 as cpp_runtime,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_orchestrator_v1 as orchestrator,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_replay_adapter_v1 as adapter,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_v1 as offline,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_shared_prefix as shared_prefix,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    multiscale_ema_boolean_cooldown_duration_policy_study as study,
)

IDENTITY = "f05_cpp_one_shot_real_day_all_arm_lockstep_v26"
SCHEMA_VERSION = f"{IDENTITY}.receipt.v1"
QUALIFICATION_DAY_INDEX = 0
WORKER_TOKENS = 10
EXPECTED_PANEL_OPPORTUNITIES = 3_516
BUILDER_PREFLIGHT_IDENTITY = "f05_cpp_target_predicate_builder_all_opportunity_zero_economic_v26"
BUILDER_PREFLIGHT_RECEIPT_NAME = "cpp_target_predicate_builder_walk_receipt.json"
BUILDER_PREFLIGHT_STATUS = "passed_all_3516_zero_economic_builder_walk"
QUICK_PREFLIGHT_IDENTITY = "f05_cpp_first_opportunity_all_arm_lockstep_v26"
QUICK_PREFLIGHT_RECEIPT_NAME = "cpp_first_opportunity_all_arm_preflight_receipt.json"
QUICK_PREFLIGHT_STATUS = "passed_first_opportunity_all_side_specific_arms_lockstep"


class CppRealDayLockstepError(RuntimeError):
    """Raised when C++ cannot earn formal one-shot authority."""


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    ).hexdigest()


def _document_sha256(value: Mapping[str, Any], field: str) -> str:
    payload = dict(value)
    payload.pop(field, None)
    return _canonical_sha256(payload)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True),
        encoding="ascii",
    )
    os.replace(temporary, path)


def _require_clean_bound_commit(bundle: Any) -> None:
    root = Path(bundle.repository_root)
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    status = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if head != bundle.execution_manifest.get("public_base_commit") or status:
        raise CppRealDayLockstepError("real-day C++ qualification requires the clean bound commit")


def _read_qualification_rows(bundle: Any, day: str) -> pd.DataFrame:
    frames: dict[str, pd.DataFrame] = {}
    for role in ("metadata", "exact_owner_actions", "replay_inputs"):
        frame = pd.read_parquet(
            bundle.panel_files[role],
            filters=[("utc_day", "==", day)],
        )
        if frame.empty or frame["opportunity_id"].duplicated().any():
            raise CppRealDayLockstepError(f"qualification {role} denominator drifted")
        frames[role] = frame.set_index("opportunity_id", drop=False)
    index = frames["metadata"].index
    if any(not frame.index.equals(index) for frame in frames.values()):
        raise CppRealDayLockstepError("qualification panel row identity drifted")
    rows = frames["replay_inputs"].copy()
    for column in (
        "assignment_ts_ns",
        "baseline_duration_ms",
        "campaign_age_s",
        "feature::channel_support_valid",
        "feature::support_valid",
        "fill_visible_ts_ns",
        "inventory_after_fill_btc",
        "role_at_fill",
        "side",
    ):
        rows[column] = frames["metadata"][column]
    for column in (
        "exact_owner_action",
        "exact_owner_duration_ms",
        "owner_fallback_reason",
        "owner_matched_rule_index",
        "owner_support_valid",
    ):
        rows[column] = frames["exact_owner_actions"][column]
    visible_ns = rows["fill_visible_ts_ns"].astype("int64")
    if (visible_ns % 1_000_000).ne(0).any():
        raise CppRealDayLockstepError("fill-visible clock is not millisecond aligned")
    rows["fill_visible_ts_ms"] = visible_ns // 1_000_000
    rows["opportunity_id"] = rows.index.astype(str)
    if set(rows["side"].astype(str)) != {"BUY", "SELL"}:
        raise CppRealDayLockstepError("qualification day lacks both sides")
    return rows


def _load_binding(rows: pd.DataFrame) -> Mapping[str, Any]:
    path_values = set(rows["portable_replay_binding_path"].astype(str))
    sha_values = set(rows["portable_replay_binding_sha256"].astype(str))
    if len(path_values) != 1 or len(sha_values) != 1:
        raise CppRealDayLockstepError("portable replay binding is not unique")
    path = resolve_portable_path(next(iter(path_values))).resolve()
    expected_sha = next(iter(sha_values))
    if not path.is_file() or _file_sha256(path) != expected_sha:
        raise CppRealDayLockstepError("portable replay binding byte identity drifted")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise CppRealDayLockstepError("portable replay binding is malformed")
    return payload


def _source_hashes(root: Path, cpp: Any) -> dict[str, str]:
    files = {
        "qualification_runner": Path(__file__).resolve(),
        "observation_tape": Path(observation_tape.__file__).resolve(),
        "cpp_runtime": Path(cpp_runtime.__file__).resolve(),
        "replay_adapter": Path(adapter.__file__).resolve(),
        "shared_prefix": Path(shared_prefix.__file__).resolve(),
        "study": Path(study.__file__).resolve(),
        "backtest_tick": root / "models/backtest_tick.py",
        "tick_replay_cpp": root / "cpp/narrowgate_cpp/tick_replay.cpp",
        "tick_replay_hpp": root / "cpp/narrowgate_cpp/tick_replay.hpp",
        "bindings_cpp": root / "cpp/narrowgate_cpp/bindings.cpp",
        "cpp_extension": Path(cpp.__file__).resolve(),
    }
    return {name: _file_sha256(path) for name, path in files.items()}


def _owner_paths(bundle: Any) -> tuple[Path, Path]:
    artifacts = bundle.panel_manifest.get("owner_artifacts")
    if not isinstance(artifacts, Mapping):
        raise CppRealDayLockstepError("qualification lacks owner artifacts")
    resolved: dict[str, Path] = {}
    for role in ("policy", "predicate_bundle"):
        binding = artifacts.get(role)
        if not isinstance(binding, Mapping):
            raise CppRealDayLockstepError(f"owner {role} binding is malformed")
        path = resolve_portable_path(str(binding.get("path", ""))).resolve()
        if not path.is_file() or _file_sha256(path) != str(binding.get("sha256")):
            raise CppRealDayLockstepError(f"owner {role} byte identity drifted")
        resolved[role] = path
    return resolved["policy"], resolved["predicate_bundle"]


def _write_progress(
    path: Path,
    *,
    stage: str,
    completed: int,
    total: int,
    started: float,
) -> None:
    _atomic_json(
        path,
        {
            "schema_version": f"{IDENTITY}.progress.v1",
            "stage": stage,
            "completed": int(completed),
            "total": int(total),
            "elapsed_s": time.monotonic() - started,
            "economic_values_persisted": False,
            "validation_read": False,
            "sealed_holdout_read": False,
        },
    )


def _admit_immutable_json(path: Path, payload: Mapping[str, Any]) -> None:
    canonical = dict(payload)
    canonical["canonical_receipt_sha256"] = _document_sha256(
        canonical,
        "canonical_receipt_sha256",
    )
    if path.exists():
        existing = json.loads(path.read_text(encoding="ascii"))
        if existing != canonical:
            raise CppRealDayLockstepError(f"immutable receipt drifted: {path.name}")
        return
    _atomic_json(path, canonical)


def preflight_all_panel_target_rows(
    bundle: Any,
    *,
    cpp: Any,
    policy_path: Path,
    predicate_path: Path,
    invariance_receipt: Mapping[str, Any],
    receipt_path: Path,
) -> Mapping[str, Any]:
    """Validate every target-row builder result without executing an economic arm."""

    runtime_config = cpp_runtime.build_cpp_runtime_config(
        cpp,
        policy_path=policy_path,
        predicate_bundle_path=predicate_path,
        qualification_sha256=bundle.execution_manifest["canonical_execution_manifest_sha256"],
    )
    startup_runtime = cpp.F05RepeatedBooleanCooldownRuntime(runtime_config)
    if not bool(startup_runtime.parity_qualified):
        raise CppRealDayLockstepError(
            "builder preflight C++ runtime identity is not parity-qualified: "
            + str(startup_runtime.binding_error or "cpp_parity_not_qualified")
        )
    policy = runtime_config.policy
    expected_predicate_count = len(policy.predicate_columns)
    selected_days = tuple(str(value) for value in bundle.source_manifest["selected_days"])
    if len(selected_days) != 30:
        raise CppRealDayLockstepError("builder preflight day denominator drifted")
    panel_files = bundle.panel_manifest.get("files")
    if not isinstance(panel_files, Mapping):
        raise CppRealDayLockstepError("builder preflight panel census is missing")
    metadata_binding = panel_files.get("metadata")
    if not isinstance(metadata_binding, Mapping):
        raise CppRealDayLockstepError("builder preflight metadata binding is missing")
    day_census = metadata_binding.get("day_census")
    if not isinstance(day_census, Mapping):
        raise CppRealDayLockstepError("builder preflight day census is missing")
    expected_rows_by_day = day_census.get("rows_by_day")
    if not isinstance(expected_rows_by_day, Mapping):
        raise CppRealDayLockstepError("builder preflight row census is missing")
    row_key_sha256_values = {
        str(binding.get("row_key_sha256", ""))
        for binding in panel_files.values()
        if isinstance(binding, Mapping)
    }
    if len(row_key_sha256_values) != 1:
        raise CppRealDayLockstepError("builder preflight panel row-key binding drifted")
    panel_row_key_sha256 = next(iter(row_key_sha256_values))

    seen_opportunities: set[str] = set()
    row_identities: list[dict[str, Any]] = []
    rows_by_day: dict[str, int] = {}
    for day in selected_days:
        rows = _read_qualification_rows(bundle, day)
        expected_day_rows = int(expected_rows_by_day.get(day, -1))
        if len(rows) != expected_day_rows:
            raise CppRealDayLockstepError(f"builder preflight row count drifted for {day}")
        rows_by_day[day] = len(rows)
        for _, opportunity in rows.iterrows():
            payload = opportunity.to_dict()
            opportunity_id = str(payload["opportunity_id"])
            if opportunity_id in seen_opportunities:
                raise CppRealDayLockstepError(
                    "builder preflight opportunity identity is duplicated"
                )
            row = cpp_runtime.build_target_predicate_row(cpp, payload)
            cpp_runtime.validate_target_predicate_row(
                cpp,
                row,
                payload,
                expected_predicate_count=expected_predicate_count,
            )
            cpp.validate_f05_cooldown_predicate_rows(runtime_config, [row])
            if list(row.predicate_values):
                raise CppRealDayLockstepError("builder preflight target row is not sparse")
            seen_opportunities.add(opportunity_id)
            row_identities.append(
                {
                    "opportunity_id": opportunity_id,
                    "utc_day": day,
                    "exposure_fill_ordinal": int(row.exposure_fill_ordinal),
                    "fill_ts_ms": int(row.fill_ts_ms),
                    "campaign_id": int(row.campaign_id),
                    "side": str(payload["side"]).upper(),
                    "predicate_value_count": 0,
                }
            )
    if len(row_identities) != EXPECTED_PANEL_OPPORTUNITIES:
        raise CppRealDayLockstepError("builder preflight opportunity denominator drifted")
    receipt: dict[str, Any] = {
        "schema_version": f"{IDENTITY}.builder_preflight.v1",
        "identity": BUILDER_PREFLIGHT_IDENTITY,
        "status": BUILDER_PREFLIGHT_STATUS,
        "execution_manifest_sha256": bundle.execution_manifest[
            "canonical_execution_manifest_sha256"
        ],
        "source_manifest_sha256": bundle.source_manifest["canonical_manifest_sha256"],
        "panel_manifest_sha256": bundle.panel_manifest["canonical_panel_manifest_sha256"],
        "selected_days": list(selected_days),
        "opportunity_count": len(row_identities),
        "rows_by_day": rows_by_day,
        "opportunity_set_sha256": _canonical_sha256(sorted(seen_opportunities)),
        "target_row_identity_sha256": _canonical_sha256(row_identities),
        "panel_row_key_sha256": panel_row_key_sha256,
        "predicate_columns": list(policy.predicate_columns),
        "predicate_column_count": expected_predicate_count,
        "target_row_semantics": (
            "sparse_target_support_row_with_cpp_runtime_derived_compiled_owner_predicates"
        ),
        "formal_v24_to_v26_invariance_receipt_sha256": invariance_receipt[
            "canonical_receipt_sha256"
        ],
        "cpp_startup_contract_validation": True,
        "cpp_startup_validator": "validate_f05_cooldown_predicate_rows",
        "cpp_startup_validated_row_count": len(row_identities),
        "economic_evaluator_call_count": 0,
        "economic_values_read": False,
        "economic_values_persisted": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    _admit_immutable_json(receipt_path, receipt)
    return json.loads(receipt_path.read_text(encoding="ascii"))


def _run_python_authority(
    *,
    day: str,
    rows: pd.DataFrame,
    request: Any,
    replay: Any,
    identity_hashes: Mapping[str, str],
    staging_root: Path,
    progress_path: Path,
    started: float,
) -> dict[tuple[str, str], Mapping[str, Any]]:
    targets = adapter._shared_prefix_target_contracts(rows)
    completed = 0

    def progress(_index: int, _manifest: Path, _resumed: bool) -> None:
        nonlocal completed
        completed += 1
        _write_progress(
            progress_path,
            stage="python_shared_prefix",
            completed=completed,
            total=len(targets),
            started=started,
        )

    executor = shared_prefix.PosixCooldownSharedPrefixExecutor(
        output_root=staging_root / "python",
        target_day=day,
        source_contract_sha256=_canonical_sha256(
            {
                "identity": IDENTITY,
                "day": day,
                "opportunity_ids": list(rows.index.astype(str)),
            }
        ),
        execution_identity_hashes=identity_hashes,
        max_parallel_arms=8,
        max_inflight_opportunity_snapshots=2,
        require_strict_native=False,
        modeled_queue_economics_authorized=False,
        exact_owner_policy_sha256=offline.ACTIVE_OWNER_POLICY_SHA256,
        target_opportunities=targets,
        global_pool_root=staging_root / "python-global-pool",
        parity_digest_capture=True,
        progress=progress,
    )
    params = study._prepare_base_params(
        adapter._exact_owner_runtime_params(
            request,
            replay,
            utc_day=day,
            identity_hashes=identity_hashes,
        ),
        trace_opportunities=False,
    )
    params["cooldown_duration_shared_prefix_executor"] = executor
    params["cooldown_duration_parent_stop_ts_ms"] = int(
        (pd.Timestamp(day, tz="UTC") + pd.Timedelta(days=1)).timestamp() * 1_000
    )
    params["exchange_book_queue_ambiguity_trace_max"] = 64
    try:
        result = backtest._simulate_tick_with_engine(
            "python",
            replay.trades,
            replay.var_ts_ms,
            replay.var_ssq,
            params,
            ml_data=replay.ml_data,
            bbo_data=replay.bbo_data,
            l2_data=replay.l2_data,
            var_ti=replay.var_ti,
            var_retsq=replay.var_retsq,
        )
    except BaseException:
        executor.abort()
        raise
    audit = dict(result.get("_cooldown_duration_shared_prefix_audit") or {})
    adapter._validate_shared_prefix_day_audit(
        audit,
        target_count=len(rows),
        arms_per_target=8,
        modeled_queue_economics_authorized=False,
    )
    output: dict[tuple[str, str], Mapping[str, Any]] = {}
    for manifest_text in audit["completed_manifest_paths"]:
        manifest_path = Path(manifest_text)
        manifest = json.loads(manifest_path.read_text(encoding="ascii"))
        opportunity_id = str(manifest["opportunity_contract"]["target_binding"]["opportunity_id"])
        for arm in manifest["arms"]:
            payload = json.loads(
                (manifest_path.parent / str(arm["path"])).read_text(encoding="ascii")
            )
            digest = payload.get("lockstep_digest")
            if not isinstance(digest, Mapping):
                raise CppRealDayLockstepError("Python arm lacks lockstep digest")
            output[(opportunity_id, str(arm["arm_id"]))] = dict(digest)
    expected = sum(len(adapter.duration_vocabulary(str(row["side"]))) for _, row in rows.iterrows())
    if len(output) != expected:
        raise CppRealDayLockstepError("Python lockstep arm census drifted")
    return output


def _run_cpp_arm(
    *,
    opportunity: Mapping[str, Any],
    action: Any,
    replay: Any,
    base: Mapping[str, Any],
    runtime_config: Any,
    qualification_sha256: str,
    shared_tape: Any,
    cpp: Any,
) -> tuple[tuple[str, str], Mapping[str, Any], float]:
    predicate_row = cpp_runtime.build_target_predicate_row(cpp, opportunity)
    cpp_runtime.validate_target_predicate_row(
        cpp,
        predicate_row,
        opportunity,
        expected_predicate_count=len(runtime_config.policy.predicate_columns),
    )
    arm_base = dict(base)
    arm_base.update(
        {
            "cooldown_duration_policy_cpp_runtime": (
                cpp.F05RepeatedBooleanCooldownRuntime(runtime_config)
            ),
            "cooldown_duration_policy_cpp_parity_qualified": True,
            "cooldown_duration_policy_cpp_event_loop_parity_qualified": True,
            "cooldown_duration_policy_cpp_parity_receipt_sha256": qualification_sha256,
            "_cooldown_duration_policy_cpp_window_tape_handle": shared_tape,
            "_cooldown_duration_policy_cpp_predicate_rows": [predicate_row],
        }
    )
    shared = {
        "ml_data": replay.ml_data,
        "bbo_data": replay.bbo_data,
        "l2_data": replay.l2_data,
        "var_ti": replay.var_ti,
        "var_retsq": replay.var_retsq,
    }
    trace, elapsed, result = study._run_duration_arm(
        opportunity,
        action,
        window=replay,
        base=arm_base,
        shared=shared,
        engine="cpp",
        require_control_prefix_parity=False,
        exact_owner_baseline_policy_enabled=True,
        expected_exact_owner_action=str(opportunity["exact_owner_action"]),
        expected_exact_owner_policy_sha256=offline.ACTIVE_OWNER_POLICY_SHA256,
        return_result=True,
    )
    del trace
    return (
        (str(opportunity["opportunity_id"]), str(action.policy_id)),
        shared_prefix.build_lockstep_digest(result),
        elapsed,
    )


def _digest_differences(
    actual: Mapping[str, Any],
    expected: Mapping[str, Any],
) -> list[str]:
    return sorted(
        name
        for name in set(actual) | set(expected)
        if actual.get(name) != expected.get(name)
    )


def _run_first_opportunity_all_arm_preflight(
    *,
    bundle: Any,
    day: str,
    rows: pd.DataFrame,
    request: Any,
    replay: Any,
    identity_hashes: Mapping[str, str],
    cpp: Any,
    policy_path: Path,
    predicate_path: Path,
    shared_tape: Any,
    builder_receipt: Mapping[str, Any],
    invariance_receipt: Mapping[str, Any],
    qualification_seed: Mapping[str, Any],
    output_path: Path,
    started: float,
) -> Mapping[str, Any]:
    receipt_path = output_path.parent / QUICK_PREFLIGHT_RECEIPT_NAME
    if receipt_path.exists():
        raise CppRealDayLockstepError("immutable quick lockstep receipt already exists")
    quick_rows = rows.iloc[[0]].copy()
    opportunity = quick_rows.iloc[0].to_dict()
    quick_contract = {
        **dict(qualification_seed),
        "stage": "first_opportunity_all_side_specific_arms",
        "opportunity_id": str(opportunity["opportunity_id"]),
    }
    quick_qualification_sha256 = _canonical_sha256(quick_contract)
    quick_staging = output_path.parent / ".cpp-lockstep-v26-quick-staging"
    quick_progress = output_path.parent / "cpp_first_opportunity_all_arm_progress.json"
    python_digests = _run_python_authority(
        day=day,
        rows=quick_rows,
        request=request,
        replay=replay,
        identity_hashes=identity_hashes,
        staging_root=quick_staging,
        progress_path=quick_progress,
        started=started,
    )
    runtime_config = cpp_runtime.build_cpp_runtime_config(
        cpp,
        policy_path=policy_path,
        predicate_bundle_path=predicate_path,
        qualification_sha256=quick_qualification_sha256,
    )
    cpp_base = adapter._cpp_exact_owner_runtime_params(
        replay,
        identity_hashes=identity_hashes,
        qualification_receipt_sha256=quick_qualification_sha256,
    )
    _, actions_by_side = adapter._load_frozen_duration_action_contract()
    actions = actions_by_side[str(opportunity["side"]).upper()]
    cpp_digests: dict[tuple[str, str], Mapping[str, Any]] = {}
    mismatches: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=len(actions)) as pool:
        futures = [
            pool.submit(
                _run_cpp_arm,
                opportunity=opportunity,
                action=action,
                replay=replay,
                base=cpp_base,
                runtime_config=runtime_config,
                qualification_sha256=quick_qualification_sha256,
                shared_tape=shared_tape,
                cpp=cpp,
            )
            for action in actions
        ]
        for future in as_completed(futures):
            key, digest, _elapsed = future.result()
            cpp_digests[key] = dict(digest)
            expected = python_digests[key]
            differences = _digest_differences(digest, expected)
            if differences:
                mismatches.append(
                    {
                        "opportunity_id": key[0],
                        "action_id": key[1],
                        "different_fields": differences,
                    }
                )
    passed = not mismatches and len(cpp_digests) == len(actions) == 8
    receipt: dict[str, Any] = {
        "schema_version": f"{QUICK_PREFLIGHT_IDENTITY}.receipt.v1",
        "identity": QUICK_PREFLIGHT_IDENTITY,
        "status": (
            QUICK_PREFLIGHT_STATUS
            if passed
            else "failed_closed_first_opportunity_all_arm_lockstep_mismatch"
        ),
        "execution_manifest_sha256": bundle.execution_manifest[
            "canonical_execution_manifest_sha256"
        ],
        "source_manifest_sha256": bundle.source_manifest["canonical_manifest_sha256"],
        "panel_manifest_sha256": bundle.panel_manifest[
            "canonical_panel_manifest_sha256"
        ],
        "qualification_day": day,
        "opportunity_count": 1,
        "arm_count": len(actions),
        "zero_mismatch_arm_count": len(actions) - len(mismatches),
        "mismatch_count": len(mismatches),
        "mismatches": sorted(
            mismatches,
            key=lambda row: (str(row["opportunity_id"]), str(row["action_id"])),
        ),
        "python_digest_set_sha256": _canonical_sha256(
            {
                f"{key[0]}::{key[1]}": value
                for key, value in sorted(python_digests.items())
            }
        ),
        "cpp_digest_set_sha256": _canonical_sha256(
            {
                f"{key[0]}::{key[1]}": value
                for key, value in sorted(cpp_digests.items())
            }
        ),
        "all_panel_builder_preflight_receipt_sha256": builder_receipt[
            "canonical_receipt_sha256"
        ],
        "formal_v24_to_v26_invariance_receipt_sha256": invariance_receipt[
            "canonical_receipt_sha256"
        ],
        "quick_qualification_sha256": quick_qualification_sha256,
        "economic_values_computed_for_lockstep_only": True,
        "economic_values_persisted": False,
        "economic_values_exposed": False,
        "economic_values_used_for_selection": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    receipt["canonical_receipt_sha256"] = _document_sha256(
        receipt,
        "canonical_receipt_sha256",
    )
    _atomic_json(receipt_path, receipt)
    if not passed:
        raise CppRealDayLockstepError(
            "C++/Python first-opportunity all-arm lockstep failed"
        )
    return receipt


def _run_lockstep_impl(
    manifest_path: Path,
    *,
    output_path: Path,
    started: float,
    stage: dict[str, str],
) -> Mapping[str, Any]:
    stage["value"] = "load_bound_manifest"
    bundle = orchestrator.load_formal_offline_bundle_for_cpp_qualification(manifest_path)
    stage["value"] = "verify_clean_bound_commit"
    _require_clean_bound_commit(bundle)
    stage["value"] = "verify_v24_to_v26_invariance"
    invariance_receipt = orchestrator._validate_v24_v26_invariance_receipt(
        manifest_path,
        bundle.execution_manifest,
        source=bundle.source_manifest,
        panel=bundle.panel_manifest,
        repository_root=Path(bundle.repository_root),
        verify_runtime_artifacts=True,
    )
    stage["value"] = "verify_completed_buy_cache_census"
    orchestrator._validate_completed_buy_cache_census_receipt(
        bundle,
        verify_cache_artifacts=True,
    )
    days = tuple(str(value) for value in bundle.source_manifest["selected_days"])
    import narrowgate_cpp as cpp

    policy_path, predicate_path = _owner_paths(bundle)
    stage["value"] = "all_panel_zero_economic_builder_walk"
    builder_receipt_path = output_path.parent / BUILDER_PREFLIGHT_RECEIPT_NAME
    builder_receipt = preflight_all_panel_target_rows(
        bundle,
        cpp=cpp,
        policy_path=policy_path,
        predicate_path=predicate_path,
        invariance_receipt=invariance_receipt,
        receipt_path=builder_receipt_path,
    )
    _write_progress(
        output_path.parent / "cpp_real_day_lockstep_progress.json",
        stage="all_panel_zero_economic_builder_walk_complete",
        completed=int(builder_receipt["opportunity_count"]),
        total=EXPECTED_PANEL_OPPORTUNITIES,
        started=started,
    )

    stage["value"] = "load_qualification_day"
    day = days[QUALIFICATION_DAY_INDEX]
    rows = _read_qualification_rows(bundle, day)
    binding = _load_binding(rows)
    request, replay = adapter._canonical_day_projection_from_rows(
        utc_day=day,
        binding=binding,
        rows=rows,
    )
    identity_hashes = adapter._day_identity_hashes(request)
    stage["value"] = "bind_qualification_sources"
    source_hashes = _source_hashes(Path(bundle.repository_root), cpp)
    tape = observation_tape.load_cpp_observation_tape(
        request.native_observation_root,
        target_day=day,
        continuation_day=replay.continuation_day,
        deep_validate=False,
    )
    qualification_seed = {
        "schema_version": f"{IDENTITY}.contract.v1",
        "execution_manifest_sha256": bundle.execution_manifest[
            "canonical_execution_manifest_sha256"
        ],
        "source_manifest_sha256": bundle.source_manifest["canonical_manifest_sha256"],
        "panel_manifest_sha256": bundle.panel_manifest["canonical_panel_manifest_sha256"],
        "public_base_commit": bundle.execution_manifest["public_base_commit"],
        "annotated_tag": bundle.execution_manifest["annotated_tag"],
        "qualification_day": day,
        "opportunity_count": len(rows),
        "arm_count": len(rows) * 8,
        "opportunity_set_sha256": _canonical_sha256(sorted(rows.index.astype(str))),
        "observation_tape_sha256": tape.receipt["array_sha256"],
        "source_hashes": source_hashes,
        "worker_tokens": WORKER_TOKENS,
        "all_panel_builder_preflight_receipt_sha256": builder_receipt["canonical_receipt_sha256"],
        "all_panel_builder_preflight_opportunity_count": builder_receipt["opportunity_count"],
        "formal_v24_to_v26_invariance_receipt_sha256": invariance_receipt[
            "canonical_receipt_sha256"
        ],
        "python_authority": "posix_cow_shared_prefix_at_fill_callback",
        "cpp_candidate": "full_day_direct_replay_shared_observation_tape",
        "economic_values_persisted": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    shared_tape = cpp_runtime.build_shared_observation_tape(
        cpp,
        tape.arrays,
        content_sha256=str(tape.receipt["array_sha256"]),
    )
    stage["value"] = "first_opportunity_all_arm_preflight"
    quick_receipt = _run_first_opportunity_all_arm_preflight(
        bundle=bundle,
        day=day,
        rows=rows,
        request=request,
        replay=replay,
        identity_hashes=identity_hashes,
        cpp=cpp,
        policy_path=policy_path,
        predicate_path=predicate_path,
        shared_tape=shared_tape,
        builder_receipt=builder_receipt,
        invariance_receipt=invariance_receipt,
        qualification_seed=qualification_seed,
        output_path=output_path,
        started=started,
    )
    qualification_contract = {
        **qualification_seed,
        "first_opportunity_all_arm_preflight_receipt_sha256": quick_receipt[
            "canonical_receipt_sha256"
        ],
    }
    qualification_sha256 = _canonical_sha256(qualification_contract)
    runtime_config = cpp_runtime.build_cpp_runtime_config(
        cpp,
        policy_path=policy_path,
        predicate_bundle_path=predicate_path,
        qualification_sha256=qualification_sha256,
    )
    stage["value"] = "qualification_day_builder_recheck"
    for _, opportunity in rows.iterrows():
        opportunity_payload = opportunity.to_dict()
        predicate_row = cpp_runtime.build_target_predicate_row(
            cpp,
            opportunity_payload,
        )
        cpp_runtime.validate_target_predicate_row(
            cpp,
            predicate_row,
            opportunity_payload,
            expected_predicate_count=len(runtime_config.policy.predicate_columns),
        )
    cpp_base = adapter._cpp_exact_owner_runtime_params(
        replay,
        identity_hashes=identity_hashes,
        qualification_receipt_sha256=qualification_sha256,
    )
    staging_root = output_path.parent / ".cpp-lockstep-v26-staging"
    staging_root.mkdir(parents=True, exist_ok=True)
    progress_path = output_path.parent / "cpp_real_day_lockstep_progress.json"
    stage["value"] = "python_shared_prefix"
    python_digests = _run_python_authority(
        day=day,
        rows=rows,
        request=request,
        replay=replay,
        identity_hashes=identity_hashes,
        staging_root=staging_root,
        progress_path=progress_path,
        started=started,
    )
    _atomic_json(
        staging_root / "python_lockstep_digests.json",
        {
            "schema_version": f"{IDENTITY}.python_digests.v1",
            "qualification_sha256": qualification_sha256,
            "digests": {
                f"{key[0]}::{key[1]}": value for key, value in sorted(python_digests.items())
            },
            "economic_values_persisted": False,
        },
    )
    contract, actions_by_side = adapter._load_frozen_duration_action_contract()
    del contract
    tasks = [
        (row.to_dict(), action)
        for _, row in rows.sort_index().iterrows()
        for action in actions_by_side[str(row["side"]).upper()]
    ]
    mismatches: list[dict[str, Any]] = []
    completed = 0
    cpp_wall_total = 0.0
    stage["value"] = "cpp_all_arm_lockstep"
    with ThreadPoolExecutor(max_workers=WORKER_TOKENS) as pool:
        futures = [
            pool.submit(
                _run_cpp_arm,
                opportunity=opportunity,
                action=action,
                replay=replay,
                base=cpp_base,
                runtime_config=runtime_config,
                qualification_sha256=qualification_sha256,
                shared_tape=shared_tape,
                cpp=cpp,
            )
            for opportunity, action in tasks
        ]
        for future in as_completed(futures):
            key, digest, elapsed = future.result()
            cpp_wall_total += elapsed
            expected = python_digests[key]
            if dict(digest) != dict(expected):
                mismatches.append(
                    {
                        "opportunity_id": key[0],
                        "action_id": key[1],
                        "different_fields": _digest_differences(digest, expected),
                    }
                )
            completed += 1
            _write_progress(
                progress_path,
                stage="cpp_all_arm_lockstep",
                completed=completed,
                total=len(tasks),
                started=started,
            )
    if mismatches:
        failure = {
            "schema_version": SCHEMA_VERSION,
            "identity": IDENTITY,
            "status": "failed_closed_cpp_python_real_day_lockstep_mismatch",
            "qualification_contract": qualification_contract,
            "qualification_sha256": qualification_sha256,
            "mismatch_count": len(mismatches),
            "first_mismatches": mismatches[:16],
            "economic_values_persisted": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        }
        failure["canonical_receipt_sha256"] = _document_sha256(failure, "canonical_receipt_sha256")
        _atomic_json(output_path, failure)
        raise CppRealDayLockstepError(f"C++/Python lockstep failed for {len(mismatches)} arms")
    receipt = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": "passed_real_day_all_opportunity_all_arm_lockstep",
        "qualification_contract": qualification_contract,
        "qualification_sha256": qualification_sha256,
        "opportunity_count": len(rows),
        "arm_count": len(tasks),
        "zero_mismatch_arm_count": len(tasks),
        "python_digest_set_sha256": _canonical_sha256(
            {f"{key[0]}::{key[1]}": value for key, value in sorted(python_digests.items())}
        ),
        "cpp_worker_tokens": WORKER_TOKENS,
        "cpp_arm_wall_time_s_total": cpp_wall_total,
        "wall_time_s": time.monotonic() - started,
        "cpp_one_shot_formal_authorized": True,
        "python_sequential_engine_remains_authoritative": True,
        "economic_values_persisted": False,
        "economic_values_used_for_selection": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    receipt["canonical_receipt_sha256"] = _document_sha256(receipt, "canonical_receipt_sha256")
    _atomic_json(output_path, receipt)
    _write_progress(
        progress_path,
        stage="complete",
        completed=len(tasks),
        total=len(tasks),
        started=started,
    )
    return receipt


def _write_unhandled_failure_receipt(
    *,
    output_path: Path,
    manifest_path: Path,
    stage: str,
    error: BaseException,
    started: float,
) -> None:
    if output_path.exists():
        return
    manifest: Mapping[str, Any] = {}
    if manifest_path.is_file():
        try:
            loaded_manifest = json.loads(manifest_path.read_text(encoding="ascii"))
        except (OSError, ValueError, TypeError):
            loaded_manifest = {}
        if isinstance(loaded_manifest, Mapping):
            manifest = loaded_manifest
    source_binding = manifest.get("source_manifest")
    panel_binding = manifest.get("panel_manifest")
    progress: Mapping[str, Any] = {}
    progress_path = output_path.parent / "cpp_real_day_lockstep_progress.json"
    if progress_path.is_file():
        try:
            loaded_progress = json.loads(progress_path.read_text(encoding="ascii"))
        except (OSError, ValueError, TypeError):
            loaded_progress = {}
        if isinstance(loaded_progress, Mapping):
            progress = loaded_progress
    progress_stage = str(progress.get("stage", ""))
    progress_completed = int(progress.get("completed", 0) or 0)
    progress_total = int(progress.get("total", 0) or 0)
    python_path_started = stage in {
        "first_opportunity_all_arm_preflight",
        "python_shared_prefix",
        "cpp_all_arm_lockstep",
    }
    cpp_path_started = stage in {
        "first_opportunity_all_arm_preflight",
        "cpp_all_arm_lockstep",
    }
    completed_opportunities = progress_completed if progress_stage.startswith("python_") else 0
    total_opportunities = (
        progress_total
        if progress_stage.startswith("python_")
        else EXPECTED_PANEL_OPPORTUNITIES
        if stage == "all_panel_zero_economic_builder_walk"
        else 0
    )
    completed_arms = progress_completed if progress_stage == "cpp_all_arm_lockstep" else 0
    total_arms = progress_total if progress_stage == "cpp_all_arm_lockstep" else 0
    if cpp_path_started and total_arms == 0 and progress_stage.startswith("python_"):
        total_arms = progress_total * 8
    failure: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": "failed_closed_execution_exception",
        "execution_manifest_sha256": manifest.get("canonical_execution_manifest_sha256"),
        "execution_manifest_file_sha256": (
            _file_sha256(manifest_path) if manifest_path.is_file() else None
        ),
        "public_base_commit": manifest.get("public_base_commit"),
        "annotated_tag": manifest.get("annotated_tag"),
        "source_manifest_sha256": (
            source_binding.get("sha256") if isinstance(source_binding, Mapping) else None
        ),
        "panel_manifest_sha256": (
            panel_binding.get("sha256") if isinstance(panel_binding, Mapping) else None
        ),
        "exact_b0_policy_sha256": offline.ACTIVE_OWNER_POLICY_SHA256,
        "exact_b0_predicate_bundle_sha256": offline.ACTIVE_PREDICATE_BUNDLE_SHA256,
        "failing_phase": stage,
        "exception_class": type(error).__name__,
        "sanitized_failure_reason": str(error),
        "completed_opportunity_count": completed_opportunities,
        "total_opportunity_count": total_opportunities,
        "completed_arm_count": completed_arms,
        "total_arm_count": total_arms,
        "python_path_started": python_path_started,
        "cpp_path_started": cpp_path_started,
        "economic_values_computed": python_path_started,
        "wall_time_s": time.monotonic() - started,
        "economic_values_persisted": False,
        "economic_values_exposed": False,
        "economic_values_used_for_selection": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    failure["canonical_receipt_sha256"] = _document_sha256(
        failure,
        "canonical_receipt_sha256",
    )
    try:
        _atomic_json(output_path, failure)
    except Exception as receipt_error:
        quarantine_path = output_path.parent / "cpp_real_day_lockstep_failure_quarantine.json"
        quarantine: dict[str, Any] = {
            "schema_version": f"{IDENTITY}.failure_quarantine.v1",
            "identity": IDENTITY,
            "status": "failure_receipt_write_quarantined",
            "failing_phase": stage,
            "original_exception_class": type(error).__name__,
            "receipt_exception_class": type(receipt_error).__name__,
            "economic_values_persisted": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        }
        quarantine["canonical_receipt_sha256"] = _document_sha256(
            quarantine,
            "canonical_receipt_sha256",
        )
        try:
            _atomic_json(quarantine_path, quarantine)
        except Exception:
            pass
        raise CppRealDayLockstepError(
            "C++ lockstep failure receipt could not be admitted"
        ) from receipt_error


def run_lockstep(
    manifest_path: Path,
    *,
    output_path: Path,
) -> Mapping[str, Any]:
    if output_path.exists():
        raise CppRealDayLockstepError("immutable lockstep receipt already exists")
    started = time.monotonic()
    stage = {"value": "startup"}
    try:
        return _run_lockstep_impl(
            manifest_path,
            output_path=output_path,
            started=started,
            stage=stage,
        )
    except BaseException as error:
        _write_unhandled_failure_receipt(
            output_path=output_path,
            manifest_path=manifest_path,
            stage=stage["value"],
            error=error,
            started=started,
        )
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        if isinstance(error, CppRealDayLockstepError):
            raise
        raise CppRealDayLockstepError(
            f"C++ real-day lockstep failed closed during {stage['value']}"
        ) from error


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_lockstep(args.manifest, output_path=args.output)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
