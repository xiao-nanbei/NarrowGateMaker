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
    causal_multichannel_window_boolean_cooldown_cpp_runtime_v21 as cpp_runtime,
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

IDENTITY = "f05_cpp_one_shot_real_day_all_arm_lockstep_v21"
SCHEMA_VERSION = f"{IDENTITY}.receipt.v1"
QUALIFICATION_DAY_INDEX = 0
WORKER_TOKENS = 10


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
        raise CppRealDayLockstepError(
            "real-day C++ qualification requires the clean bound commit"
        )


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
        opportunity_id = str(
            manifest["opportunity_contract"]["target_binding"]["opportunity_id"]
        )
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
            "_cooldown_duration_policy_cpp_predicate_rows": [
                cpp_runtime.build_target_predicate_row(cpp, opportunity)
            ],
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


def run_lockstep(
    manifest_path: Path,
    *,
    output_path: Path,
) -> Mapping[str, Any]:
    if output_path.exists():
        raise CppRealDayLockstepError("immutable lockstep receipt already exists")
    bundle = orchestrator.load_formal_offline_bundle_for_cpp_qualification(manifest_path)
    _require_clean_bound_commit(bundle)
    days = tuple(str(value) for value in bundle.source_manifest["selected_days"])
    day = days[QUALIFICATION_DAY_INDEX]
    rows = _read_qualification_rows(bundle, day)
    binding = _load_binding(rows)
    request, replay = adapter._canonical_day_projection_from_rows(
        utc_day=day,
        binding=binding,
        rows=rows,
    )
    identity_hashes = adapter._day_identity_hashes(request)
    import narrowgate_cpp as cpp

    source_hashes = _source_hashes(Path(bundle.repository_root), cpp)
    policy_path, predicate_path = _owner_paths(bundle)
    tape = observation_tape.load_cpp_observation_tape(
        request.native_observation_root,
        target_day=day,
        continuation_day=replay.continuation_day,
        deep_validate=False,
    )
    qualification_contract = {
        "schema_version": f"{IDENTITY}.contract.v1",
        "execution_manifest_sha256": bundle.execution_manifest[
            "canonical_execution_manifest_sha256"
        ],
        "source_manifest_sha256": bundle.source_manifest["canonical_manifest_sha256"],
        "panel_manifest_sha256": bundle.panel_manifest[
            "canonical_panel_manifest_sha256"
        ],
        "public_base_commit": bundle.execution_manifest["public_base_commit"],
        "annotated_tag": bundle.execution_manifest["annotated_tag"],
        "qualification_day": day,
        "opportunity_count": len(rows),
        "arm_count": len(rows) * 8,
        "opportunity_set_sha256": _canonical_sha256(sorted(rows.index.astype(str))),
        "observation_tape_sha256": tape.receipt["array_sha256"],
        "source_hashes": source_hashes,
        "worker_tokens": WORKER_TOKENS,
        "python_authority": "posix_cow_shared_prefix_at_fill_callback",
        "cpp_candidate": "full_day_direct_replay_shared_observation_tape",
        "economic_values_persisted": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    qualification_sha256 = _canonical_sha256(qualification_contract)
    shared_tape = cpp_runtime.build_shared_observation_tape(
        cpp,
        tape.arrays,
        content_sha256=str(tape.receipt["array_sha256"]),
    )
    runtime_config = cpp_runtime.build_cpp_runtime_config(
        cpp,
        policy_path=policy_path,
        predicate_bundle_path=predicate_path,
        qualification_sha256=qualification_sha256,
    )
    cpp_base = adapter._cpp_exact_owner_runtime_params(
        replay,
        identity_hashes=identity_hashes,
        qualification_receipt_sha256=qualification_sha256,
    )
    staging_root = output_path.parent / ".cpp-lockstep-v21-staging"
    staging_root.mkdir(parents=True, exist_ok=True)
    progress_path = output_path.parent / "cpp_real_day_lockstep_progress.json"
    started = time.monotonic()
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
                f"{key[0]}::{key[1]}": value
                for key, value in sorted(python_digests.items())
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
                        "different_fields": sorted(
                            name
                            for name in set(digest) | set(expected)
                            if digest.get(name) != expected.get(name)
                        ),
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
        failure["canonical_receipt_sha256"] = _document_sha256(
            failure, "canonical_receipt_sha256"
        )
        _atomic_json(output_path, failure)
        raise CppRealDayLockstepError(
            f"C++/Python lockstep failed for {len(mismatches)} arms"
        )
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
            {
                f"{key[0]}::{key[1]}": value
                for key, value in sorted(python_digests.items())
            }
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
    receipt["canonical_receipt_sha256"] = _document_sha256(
        receipt, "canonical_receipt_sha256"
    )
    _atomic_json(output_path, receipt)
    _write_progress(
        progress_path,
        stage="complete",
        completed=len(tasks),
        total=len(tasks),
        started=started,
    )
    return receipt


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
