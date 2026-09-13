"""Run resumable parallel outer OOF for exploratory Boolean cooldown rules."""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import os
import shutil
import tempfile
import time
from collections.abc import Mapping
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pandas as pd

from research.families.f05_fill_quality_quote_ev.audit import (
    multiscale_ema_boolean_cooldown_duration_policy_training as training,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    multiscale_ema_boolean_cooldown_rule_learner as learner,
)

IDENTITY = learner.EXPLORATORY_IDENTITY
SOURCE_ADMISSION_MANIFEST_SHA256 = (
    "a203efbf985848b7b24486a9e36ac18286e22dc22c057ad73b2d23f561c775cb"
)
SIDES = ("BUY", "SELL")
OUTER_FOLDS = tuple(range(4))
PERMISSIONS = {
    "action_authorized": False,
    "live_authorized": False,
    "f09_registration_authorized": False,
    "validation_read": False,
    "sealed_holdout_read": False,
}


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_json(path: Path, value: Any) -> None:
    path.write_bytes(_canonical_bytes(value) + b"\n")


def _write_json_atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    _write_json(temporary, value)
    os.replace(temporary, path)


def _load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON artifact is not an object: {path}")
    return value


def _implementation_hashes() -> dict[str, str]:
    return {
        "runner": _sha256_file(Path(__file__)),
        "learner": _sha256_file(Path(learner.__file__)),
        "training_loader": _sha256_file(Path(training.__file__)),
    }


def _formal_input_identity() -> learner.FormalInputIdentity:
    contract = learner.load_frozen_search_contract()
    return learner.FormalInputIdentity(
        ordered_utc_days=contract.ordered_development_days,
        opportunity_count=contract.expected_opportunities,
        arm_row_count=contract.expected_arm_rows,
        predicate_schema_sha256=contract.predicate_schema_sha256,
        outer_fold_source_sha256=contract.outer_fold_source_sha256,
        spec_sha256=contract.spec_sha256,
        outcome_blind_sha256=contract.outcome_blind_sha256,
    )


def _identity_from_artifact(value: Mapping[str, Any]) -> learner.FormalInputIdentity:
    return learner.FormalInputIdentity(
        ordered_utc_days=tuple(str(day) for day in value["ordered_utc_days"]),
        opportunity_count=int(value["opportunity_count"]),
        arm_row_count=int(value["arm_row_count"]),
        predicate_schema_sha256=str(value["predicate_schema_sha256"]),
        outer_fold_source_sha256=str(value["outer_fold_source_sha256"]),
        spec_sha256=str(value["spec_sha256"]),
        outcome_blind_sha256=str(value["outcome_blind_sha256"]),
    )


def _archive_source_root(input_root: Path) -> tuple[Path, Path]:
    admission_path = input_root.parent / "admission_manifest.json"
    if _sha256_file(admission_path) != SOURCE_ADMISSION_MANIFEST_SHA256:
        raise ValueError("source admission manifest SHA256 mismatch")
    training_path = input_root / training.TRAINING_MANIFEST_NAME
    training_manifest = _load_json(training_path)
    archived_arm_path = Path(str(training_manifest.get("arm_trace_manifest_path", "")))
    if not archived_arm_path.is_absolute() or archived_arm_path.name != training.ARM_MANIFEST_NAME:
        raise ValueError("source training manifest lacks its absolute execution-root identity")
    return archived_arm_path.parent, admission_path


def _cross_validity_audit(panel: pd.DataFrame) -> dict[str, Any]:
    missing_columns = tuple(sorted(column for column in panel if column.endswith("_cross_missing")))
    if len(missing_columns) != 45:
        raise ValueError("formal panel does not contain all 45 crossover-validity fields")
    opportunities = panel.drop_duplicates("opportunity_id")
    missing_mask = opportunities.loc[:, missing_columns].astype(bool).any(axis=1)
    missing_count = int(missing_mask.sum())
    if missing_count:
        raise ValueError(
            "frozen v1 predicates contain unobserved crossover states; a new predicate schema is required"
        )
    return {
        "opportunity_count": int(len(opportunities)),
        "cross_validity_column_count": len(missing_columns),
        "opportunities_with_any_missing_crossover": missing_count,
        "negated_last_cross_favorable_is_adverse_on_this_panel": True,
        "prospective_missing_state_fail_closed": True,
    }


def _manifest_artifacts(paths: tuple[Path, ...]) -> dict[str, dict[str, int | str]]:
    return {
        path.name: {"sha256": _sha256_file(path), "bytes": path.stat().st_size}
        for path in paths
    }


def _validate_artifacts(root: Path, manifest: Mapping[str, Any]) -> None:
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict) or not artifacts:
        raise ValueError(f"artifact manifest is empty: {root}")
    for name, expected in artifacts.items():
        path = root / str(name)
        if not path.is_file():
            raise ValueError(f"artifact is missing: {path}")
        if _sha256_file(path) != str(expected["sha256"]):
            raise ValueError(f"artifact SHA256 mismatch: {path}")
        if path.stat().st_size != int(expected["bytes"]):
            raise ValueError(f"artifact byte count mismatch: {path}")


def _validate_success(root: Path, manifest: Mapping[str, Any]) -> None:
    success = _load_json(root / "_SUCCESS")
    if (
        success.get("identity") != IDENTITY
        or success.get("manifest_sha256") != _sha256_file(root / "manifest.json")
    ):
        raise ValueError(f"success marker does not bind manifest: {root}")
    _validate_artifacts(root, manifest)


def _prepare_source_projection(input_root: Path, work_root: Path) -> dict[str, Any]:
    source_root = work_root / "source"
    if source_root.exists():
        manifest = _load_json(source_root / "manifest.json")
        _validate_success(source_root, manifest)
        if manifest.get("implementation_hashes") != _implementation_hashes():
            raise ValueError("source projection implementation hashes drifted")
        return manifest

    relocated_from_root, admission_path = _archive_source_root(input_root)
    admitted = training.load_formal_arm_panel(
        input_root,
        relocated_from_root=relocated_from_root,
    )
    attested = learner.attest_formal_input_panel(admitted.frame)
    if attested != _formal_input_identity():
        raise ValueError("source panel attestation differs from the frozen formal identity")
    validity = _cross_validity_audit(admitted.frame)
    contract = learner.load_frozen_search_contract()
    cross_validity_columns = tuple(
        sorted(column for column in admitted.frame if column.endswith("_cross_missing"))
    )
    projection_columns = (
        *learner.COMMON_COLUMNS,
        "candidate_policy_id",
        learner.OUTCOME_VALUE_COLUMN,
        *contract.predicate_columns,
        *cross_validity_columns,
    )
    if len(set(projection_columns)) != len(projection_columns):
        raise ValueError("source projection contains duplicate columns")
    missing = set(projection_columns) - set(admitted.frame.columns)
    if missing:
        raise ValueError(f"source projection is missing columns: {sorted(missing)}")

    work_root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".source.staging.", dir=work_root))
    try:
        projection_paths: list[Path] = []
        side_rows: dict[str, int] = {}
        for side in SIDES:
            projection = admitted.frame.loc[
                admitted.frame["side"].astype(str).eq(side), projection_columns
            ].copy()
            path = staging / f"{side.lower()}_panel.parquet"
            projection.to_parquet(path, index=False, compression="zstd")
            projection_paths.append(path)
            side_rows[side] = len(projection)
        manifest = {
            "schema_version": f"{IDENTITY}.source_projection.v1",
            "identity": IDENTITY,
            "status": "hash_verified_orico_source_projected_for_parallel_oof",
            "source_artifact_root": str(input_root),
            "source_relocated_from_root": str(relocated_from_root),
            "source_admission_manifest_path": str(admission_path),
            "source_admission_manifest_sha256": SOURCE_ADMISSION_MANIFEST_SHA256,
            "source_arm_manifest_sha256": admitted.arm_manifest_sha256,
            "source_training_manifest_sha256": admitted.training_manifest_sha256,
            "formal_input_identity": attested.artifact(),
            "crossover_validity": validity,
            "denominator": {
                "input_opportunities": admitted.opportunity_count,
                "arm_rows": admitted.arm_row_count,
                "joint_censored_opportunities": admitted.joint_censored_opportunities,
                "training_eligible_opportunities": admitted.training_label_opportunities,
                "side_projection_rows": side_rows,
            },
            "projection_columns": list(projection_columns),
            "implementation_hashes": _implementation_hashes(),
            "artifacts": _manifest_artifacts(tuple(projection_paths)),
            "permissions": PERMISSIONS,
        }
        manifest_path = staging / "manifest.json"
        _write_json(manifest_path, manifest)
        _write_json(
            staging / "_SUCCESS",
            {"identity": IDENTITY, "manifest_sha256": _sha256_file(manifest_path)},
        )
        os.replace(staging, source_root)
        return manifest
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    finally:
        del admitted
        gc.collect()


def _job_root(work_root: Path, side: str, outer_fold: int) -> Path:
    return work_root / "jobs" / side.lower() / f"outer_fold_{outer_fold}"


def _load_job(work_root: Path, side: str, outer_fold: int) -> dict[str, Any]:
    root = _job_root(work_root, side, outer_fold)
    manifest = _load_json(root / "manifest.json")
    if (
        manifest.get("identity") != IDENTITY
        or manifest.get("side") != side
        or int(manifest.get("outer_fold", -1)) != outer_fold
        or manifest.get("implementation_hashes") != _implementation_hashes()
    ):
        raise ValueError(f"fold checkpoint identity drifted: {root}")
    _validate_success(root, manifest)
    return manifest


def _run_fold_job(work_root_text: str, side: str, outer_fold: int) -> dict[str, Any]:
    work_root = Path(work_root_text)
    root = _job_root(work_root, side, outer_fold)
    if root.exists():
        manifest = _load_job(work_root, side, outer_fold)
        return {
            "side": side,
            "outer_fold": outer_fold,
            "resumed": True,
            "elapsed_seconds": float(manifest["elapsed_seconds"]),
        }

    source_root = work_root / "source"
    source_manifest = _load_json(source_root / "manifest.json")
    _validate_success(source_root, source_manifest)
    if source_manifest.get("implementation_hashes") != _implementation_hashes():
        raise ValueError("worker implementation differs from source projection")
    projection_name = f"{side.lower()}_panel.parquet"
    projection_path = source_root / projection_name
    expected_projection = source_manifest["artifacts"][projection_name]
    if _sha256_file(projection_path) != expected_projection["sha256"]:
        raise ValueError("worker side projection SHA256 mismatch")
    panel = pd.read_parquet(projection_path)
    formal_identity = _identity_from_artifact(source_manifest["formal_input_identity"])
    started = time.monotonic()
    result = learner.run_nested_chronological_oof(
        panel,
        side=side,
        formal_input_identity=formal_identity,
        selection_mode=learner.EXPLORATORY_NONBASELINE_SELECTION,
        policy_identity=IDENTITY,
        outer_fold_indices=(outer_fold,),
    )
    elapsed = time.monotonic() - started
    if (
        set(result.oof["outer_fold"].astype(int)) != {outer_fold}
        or len(result.outer_policy_artifacts) != 1
        or not result.outer_policy_artifacts[0].get("ordered_rules")
    ):
        raise ValueError("fold worker did not produce one nonbaseline outer policy")

    root.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{root.name}.staging.", dir=root.parent))
    try:
        oof_path = staging / "oof.parquet"
        evidence_path = staging / "complexity_evidence.parquet"
        chronology_path = staging / "chronology_audit.parquet"
        policy_path = staging / "outer_policy.json"
        audit_path = staging / "panel_audit.json"
        result.oof.to_parquet(oof_path, index=False)
        result.complexity_evidence.to_parquet(evidence_path, index=False)
        result.chronology_audit.to_parquet(chronology_path, index=False)
        _write_json(policy_path, result.outer_policy_artifacts[0])
        _write_json(audit_path, asdict(result.panel_audit))
        manifest = {
            "schema_version": f"{IDENTITY}.outer_fold_checkpoint.v1",
            "identity": IDENTITY,
            "status": "nonbaseline_outer_fold_complete",
            "side": side,
            "outer_fold": outer_fold,
            "elapsed_seconds": elapsed,
            "source_projection_sha256": expected_projection["sha256"],
            "implementation_hashes": _implementation_hashes(),
            "artifacts": _manifest_artifacts(
                (oof_path, evidence_path, chronology_path, policy_path, audit_path)
            ),
            "permissions": PERMISSIONS,
        }
        manifest_path = staging / "manifest.json"
        _write_json(manifest_path, manifest)
        _write_json(
            staging / "_SUCCESS",
            {"identity": IDENTITY, "manifest_sha256": _sha256_file(manifest_path)},
        )
        os.replace(staging, root)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return {
        "side": side,
        "outer_fold": outer_fold,
        "resumed": False,
        "elapsed_seconds": elapsed,
    }


def _panel_audit(path: Path) -> learner.PanelAudit:
    value = _load_json(path)
    return learner.PanelAudit(
        input_opportunities=int(value["input_opportunities"]),
        eligible_opportunities=int(value["eligible_opportunities"]),
        joint_censored_opportunities=int(value["joint_censored_opportunities"]),
        excluded_opportunity_ids=tuple(str(item) for item in value["excluded_opportunity_ids"]),
        campaign_weight_min=float(value["campaign_weight_min"]),
        campaign_weight_max=float(value["campaign_weight_max"]),
    )


def _combined_side_result(work_root: Path, side: str) -> learner.NestedChronologicalResult:
    oof_frames: list[pd.DataFrame] = []
    evidence_frames: list[pd.DataFrame] = []
    chronology_frames: list[pd.DataFrame] = []
    artifacts: list[dict[str, Any]] = []
    audits: list[learner.PanelAudit] = []
    for outer_fold in OUTER_FOLDS:
        _load_job(work_root, side, outer_fold)
        root = _job_root(work_root, side, outer_fold)
        oof_frames.append(pd.read_parquet(root / "oof.parquet"))
        evidence_frames.append(pd.read_parquet(root / "complexity_evidence.parquet"))
        chronology_frames.append(pd.read_parquet(root / "chronology_audit.parquet"))
        artifacts.append(_load_json(root / "outer_policy.json"))
        audits.append(_panel_audit(root / "panel_audit.json"))
    if any(audit != audits[0] for audit in audits[1:]):
        raise ValueError(f"{side} fold checkpoints disagree on panel audit")
    return learner.NestedChronologicalResult(
        oof=pd.concat(oof_frames, ignore_index=True).sort_values(
            ["outer_fold", "utc_day", "opportunity_id"], kind="stable"
        ).reset_index(drop=True),
        complexity_evidence=pd.concat(evidence_frames, ignore_index=True).sort_values(
            ["outer_fold", "max_literals_per_clause", "max_clauses"], kind="stable"
        ).reset_index(drop=True),
        chronology_audit=pd.concat(chronology_frames, ignore_index=True).sort_values(
            ["outer_fold", "inner_fold", "max_literals_per_clause", "max_clauses"],
            kind="stable",
        ).reset_index(drop=True),
        outer_policy_artifacts=tuple(artifacts),
        panel_audit=audits[0],
        permissions=PERMISSIONS,
    )


def _side_summary(result: learner.NestedChronologicalResult, *, side: str) -> dict[str, Any]:
    oof = result.oof
    gate = learner.evaluate_outer_oof_gate(result, side=side)
    action = oof["chosen_action"].astype(str)
    artifacts = tuple(result.outer_policy_artifacts)
    if len(artifacts) != 4 or any(not row.get("ordered_rules") for row in artifacts):
        raise ValueError(f"{side} exploratory candidate was cleared before outer OOF")
    return {
        "side": side,
        "outer_oof_rows": int(len(oof)),
        "outer_oof_days": int(oof["utc_day"].nunique()),
        "outer_oof_campaigns": int(oof["campaign_side_id"].nunique()),
        "non_control_action_rate": float(action.ne(learner.CONTROL_ACTION).mean()),
        "chosen_action_counts": {
            str(key): int(value) for key, value in action.value_counts().sort_index().items()
        },
        "point_uplift_usdc_per_campaign_weight": gate.point_uplift_usdc,
        "lower_confidence_bound_usdc_per_campaign_weight": gate.lower_confidence_bound_usdc,
        "deployment_gate_passed": gate.passed,
        "outer_policy_rule_counts": [len(row["ordered_rules"]) for row in artifacts],
        "outer_policy_candidate_family_sizes": [
            int(row["beam_survivor_family_size"]) for row in artifacts
        ],
        "outer_policy_pre_oof_lcb_values_usdc": [
            float(row["beam_survivor_family_conditional_policy_lcb_usdc"])
            for row in artifacts
        ],
        "candidate_policy_was_forced_into_outer_oof": True,
        "deployment_decision_was_applied_only_after_outer_oof": True,
    }


def _write_progress(
    work_root: Path,
    *,
    started_at: float,
    workers: int,
    completed: Mapping[str, Mapping[str, Any]],
) -> None:
    jobs = {}
    for outer_fold in OUTER_FOLDS:
        for side in SIDES:
            key = f"{side}.outer{outer_fold}"
            jobs[key] = dict(completed.get(key, {"status": "pending"}))
    _write_json_atomic(
        work_root / "progress.json",
        {
            "identity": IDENTITY,
            "workers": workers,
            "elapsed_seconds": time.monotonic() - started_at,
            "completed_jobs": sum(row.get("status") == "complete" for row in jobs.values()),
            "total_jobs": len(jobs),
            "jobs": jobs,
        },
    )


def _publish_final(work_root: Path, output_dir: Path) -> dict[str, Any]:
    results = {side: _combined_side_result(work_root, side) for side in SIDES}
    summaries = {side: _side_summary(results[side], side=side) for side in SIDES}
    source_manifest = _load_json(work_root / "source" / "manifest.json")
    staging = Path(
        tempfile.mkdtemp(prefix=f".{output_dir.name}.staging.", dir=output_dir.parent)
    )
    try:
        oof = pd.concat(
            [results[side].oof.assign(training_side=side) for side in SIDES],
            ignore_index=True,
        )
        evidence = pd.concat(
            [results[side].complexity_evidence.assign(training_side=side) for side in SIDES],
            ignore_index=True,
        )
        chronology = pd.concat(
            [results[side].chronology_audit.assign(training_side=side) for side in SIDES],
            ignore_index=True,
        )
        policies = {
            "schema_version": f"{IDENTITY}.outer_policies.v1",
            "identity": IDENTITY,
            "selection_mode": learner.EXPLORATORY_NONBASELINE_SELECTION,
            "policies": {
                side: list(results[side].outer_policy_artifacts) for side in SIDES
            },
            "permissions": PERMISSIONS,
        }
        oof_path = staging / "oof.parquet"
        evidence_path = staging / "complexity_evidence.parquet"
        chronology_path = staging / "chronology_audit.parquet"
        policies_path = staging / "outer_policies.json"
        oof.to_parquet(oof_path, index=False)
        evidence.to_parquet(evidence_path, index=False)
        chronology.to_parquet(chronology_path, index=False)
        _write_json(policies_path, policies)
        report = {
            "schema_version": f"{IDENTITY}.development_report.v1",
            "identity": IDENTITY,
            "status": (
                "exploratory_outer_oof_complete_candidate_not_deployable"
                if not any(bool(row["deployment_gate_passed"]) for row in summaries.values())
                else "exploratory_outer_oof_complete_side_candidate_requires_full_path"
            ),
            "source_identity": learner.IDENTITY,
            "source_artifact_root": source_manifest["source_artifact_root"],
            "source_relocated_from_root": source_manifest["source_relocated_from_root"],
            "source_admission_manifest_path": source_manifest[
                "source_admission_manifest_path"
            ],
            "source_admission_manifest_sha256": SOURCE_ADMISSION_MANIFEST_SHA256,
            "source_arm_manifest_sha256": source_manifest["source_arm_manifest_sha256"],
            "source_training_manifest_sha256": source_manifest[
                "source_training_manifest_sha256"
            ],
            "formal_input_identity": source_manifest["formal_input_identity"],
            "selection_contract": {
                "inner_search_space": "unchanged_frozen_v1_boolean_and_duration_grid",
                "exploratory_candidate_policy": (
                    "best_support_valid_nonbaseline_rule_is_executed_in_outer_oof_even_when_"
                    "inner_or_refit_lcb_crosses_zero"
                ),
                "deployable_policy": (
                    "evaluated_only_after_all_untouched_outer_oof_rows_are_scored"
                ),
                "baseline_is_not_a_complexity_candidate": True,
                "outer_outcomes_used_for_rule_discovery": False,
            },
            "execution": {
                "parallel_unit": "side_x_outer_fold",
                "fold_checkpoints": 8,
                "checkpoint_resume_supported": True,
                "compute_parallelism_does_not_change_chronological_evidence": True,
            },
            "crossover_validity": source_manifest["crossover_validity"],
            "denominator": {
                "input_opportunities": source_manifest["denominator"][
                    "input_opportunities"
                ],
                "joint_censored_opportunities": source_manifest["denominator"][
                    "joint_censored_opportunities"
                ],
                "training_eligible_opportunities": source_manifest["denominator"][
                    "training_eligible_opportunities"
                ],
                "whole_opportunity_censor_exclusion_used": True,
                "arm_level_complete_case_filtering_used": False,
                "censor_time_marks_used_as_labels": False,
            },
            "sides": summaries,
            "next_step": (
                "run_side_specific_policy_full_path_only_for_a_side_whose_outer_oof_gate_passed"
            ),
            "permissions": PERMISSIONS,
        }
        report_path = staging / "report.json"
        _write_json(report_path, report)
        artifacts = _manifest_artifacts(
            (oof_path, evidence_path, chronology_path, policies_path, report_path)
        )
        manifest = {
            "schema_version": f"{IDENTITY}.artifact_manifest.v1",
            "identity": IDENTITY,
            "status": "atomic_development_successor_artifacts_admitted",
            "implementation_hashes": _implementation_hashes(),
            "artifacts": artifacts,
            "permissions": PERMISSIONS,
        }
        manifest_path = staging / "manifest.json"
        _write_json(manifest_path, manifest)
        _write_json(
            staging / "_SUCCESS",
            {"identity": IDENTITY, "manifest_sha256": _sha256_file(manifest_path)},
        )
        os.replace(staging, output_dir)
        return report
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def run_exploratory_oof(
    input_root: Path,
    output_dir: Path,
    *,
    work_root: Path,
    workers: int,
) -> dict[str, Any]:
    input_root = input_root.resolve()
    output_dir = output_dir.resolve()
    work_root = work_root.resolve()
    if output_dir.exists():
        raise FileExistsError(f"refusing to overwrite successor output: {output_dir}")
    if workers < 1 or workers > 4:
        raise ValueError("workers must be in [1,4] for the 24 GB local memory contract")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    _prepare_source_projection(input_root, work_root)
    started_at = time.monotonic()
    completed: dict[str, dict[str, Any]] = {}
    pending: list[tuple[str, int]] = []
    for outer_fold in OUTER_FOLDS:
        for side in SIDES:
            key = f"{side}.outer{outer_fold}"
            root = _job_root(work_root, side, outer_fold)
            if root.exists():
                manifest = _load_job(work_root, side, outer_fold)
                completed[key] = {
                    "status": "complete",
                    "resumed": True,
                    "elapsed_seconds": float(manifest["elapsed_seconds"]),
                }
            else:
                pending.append((side, outer_fold))
    _write_progress(work_root, started_at=started_at, workers=workers, completed=completed)
    print(
        f"parallel_oof start completed={len(completed)}/8 pending={len(pending)} workers={workers}",
        flush=True,
    )
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_run_fold_job, str(work_root), side, outer_fold): (
                side,
                outer_fold,
            )
            for side, outer_fold in pending
        }
        for future in as_completed(futures):
            side, outer_fold = futures[future]
            result = future.result()
            key = f"{side}.outer{outer_fold}"
            completed[key] = {"status": "complete", **result}
            _write_progress(
                work_root,
                started_at=started_at,
                workers=workers,
                completed=completed,
            )
            print(
                f"parallel_oof complete {key} elapsed={result['elapsed_seconds']:.1f}s "
                f"total={len(completed)}/8",
                flush=True,
            )
    if len(completed) != 8:
        raise ValueError("parallel OOF ended without all eight fold checkpoints")
    report = _publish_final(work_root, output_dir)
    _write_progress(work_root, started_at=started_at, workers=workers, completed=completed)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    work_root = args.work_dir or args.output_dir.with_name(f"{args.output_dir.name}.work")
    report = run_exploratory_oof(
        args.input_root,
        args.output_dir,
        work_root=work_root,
        workers=args.workers,
    )
    print(json.dumps(report, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
