"""Fail-closed post-training publication for the Boolean cooldown policy.

The finalizer consumes an already admitted Development training directory and
the original eight-arm panel.  It does not train outer folds, read locked
evidence, run policy replay, or grant action/live authority.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from research.families.f05_fill_quality_quote_ev.audit import (
    multiscale_ema_boolean_cooldown_duration_policy_training as training,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    multiscale_ema_boolean_cooldown_rule_learner as learner,
)
from research.families.f05_fill_quality_quote_ev.audit.multiscale_ema_boolean_cooldown_rule_learner import (
    FORMAL_BOOTSTRAP_SAMPLES,
    FORMAL_CONFIDENCE,
    IDENTITY,
    FormalInputIdentity,
    NestedChronologicalResult,
    PanelAudit,
    attest_formal_input_panel,
    evaluate_outer_oof_gate,
    freeze_final_side_policy,
    serialize_final_policy_bundle,
)

TRAINING_MANIFEST_NAME = "training_artifact_manifest.json"
FINALIZER_MANIFEST_NAME = "post_training_finalizer_manifest.json"
EXPECTED_TRAINING_ARTIFACTS = {
    "oof": "oof.parquet",
    "complexity_evidence": "complexity_evidence.parquet",
    "chronology_audit": "chronology_audit.parquet",
    "outer_policies": "outer_policies.json",
    "development_report": "development_report.json",
}
LEARNER_PERMISSIONS = {
    "action_authorized": False,
    "live_authorized": False,
    "f09_registration_authorized": False,
    "validation_read": False,
    "sealed_holdout_read": False,
}
FINAL_PERMISSIONS = {
    "development_evidence_only": True,
    "validation_read": False,
    "sealed_holdout_read": False,
    "f09_registration_authorized": False,
    "action_authorized": False,
    "live_authorized": False,
    "registry_update_authorized": False,
}


class PostTrainingFinalizerError(RuntimeError):
    """A post-training input or atomic publication violated its contract."""


@dataclass(frozen=True)
class ValidatedPostTrainingInput:
    contract: training.TrainingContract
    admitted: training.AdmittedPanel
    formal_input_identity: FormalInputIdentity
    training_manifest_path: Path
    training_manifest_sha256: str
    training_success_sha256: str
    artifact_paths: Mapping[str, Path]
    artifact_sha256: Mapping[str, str]
    results: Mapping[str, NestedChronologicalResult]


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _is_sha256(value: Any) -> bool:
    if not isinstance(value, str) or len(value) != 64:
        return False
    try:
        int(value, 16)
    except ValueError:
        return False
    return True


def _load_json(path: Path, *, role: str) -> dict[str, Any]:
    if not path.is_file():
        raise PostTrainingFinalizerError(f"missing {role}: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PostTrainingFinalizerError(f"invalid {role}: {path}") from exc
    if not isinstance(value, dict):
        raise PostTrainingFinalizerError(f"{role} must be a JSON object")
    return value


def _write_json(path: Path, value: Any) -> None:
    path.write_bytes(_canonical_bytes(value) + b"\n")


def _require_permissions(
    value: Any,
    *,
    expected: Mapping[str, bool],
    role: str,
) -> None:
    if not isinstance(value, Mapping) or dict(value) != dict(expected):
        raise PostTrainingFinalizerError(f"{role} permission boundary drifted")


def _same_path(left: Any, right: Any, *, base: Path) -> bool:
    if not isinstance(left, str) or not left:
        return False
    left_path = Path(left).expanduser()
    if not left_path.is_absolute():
        left_path = base / left_path
    right_path = Path(str(right)).expanduser()
    if not right_path.is_absolute():
        right_path = base / right_path
    return left_path.resolve() == right_path.resolve()


def _expected_formal_identity(
    admitted: training.AdmittedPanel,
    contract: training.TrainingContract,
) -> FormalInputIdentity:
    if contract.synthetic_test_only:
        return FormalInputIdentity(
            ordered_utc_days=contract.ordered_utc_days,
            opportunity_count=admitted.opportunity_count,
            arm_row_count=admitted.arm_row_count,
            predicate_schema_sha256=contract.predicate_schema_sha256,
            outer_fold_source_sha256=contract.outer_fold_source_sha256,
            spec_sha256=contract.spec_sha256,
            outcome_blind_sha256=contract.outcome_blind_sha256,
        )
    try:
        return attest_formal_input_panel(admitted.frame)
    except ValueError as exc:
        raise PostTrainingFinalizerError(
            f"formal full-panel identity attestation failed: {exc}"
        ) from exc


def _validate_formal_identity_artifact(
    value: Any,
    *,
    expected: FormalInputIdentity,
    role: str,
) -> None:
    if not isinstance(value, Mapping):
        raise PostTrainingFinalizerError(f"{role} is missing")
    artifact = dict(value)
    identity_sha = artifact.get("formal_input_identity_sha256")
    if not _is_sha256(identity_sha):
        raise PostTrainingFinalizerError(f"{role} has no valid self-hash")
    body = {
        key: item for key, item in artifact.items() if key != "formal_input_identity_sha256"
    }
    if _canonical_sha256(body) != identity_sha:
        raise PostTrainingFinalizerError(f"{role} self-hash drifted")
    if artifact != expected.artifact():
        raise PostTrainingFinalizerError(f"{role} differs from the original arm panel")


def _validate_training_artifacts(
    training_dir: Path,
    manifest: Mapping[str, Any],
) -> tuple[dict[str, Path], dict[str, str]]:
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, Mapping) or set(artifacts) != set(
        EXPECTED_TRAINING_ARTIFACTS
    ):
        raise PostTrainingFinalizerError("training artifact set drifted")
    expected_files = {
        TRAINING_MANIFEST_NAME,
        "_SUCCESS",
        *EXPECTED_TRAINING_ARTIFACTS.values(),
    }
    observed_entries = {path.name for path in training_dir.iterdir()}
    if observed_entries != expected_files:
        raise PostTrainingFinalizerError("training directory contains unbound or missing files")

    paths: dict[str, Path] = {}
    hashes: dict[str, str] = {}
    for name, expected_name in EXPECTED_TRAINING_ARTIFACTS.items():
        metadata = artifacts.get(name)
        if not isinstance(metadata, Mapping) or metadata.get("path") != expected_name:
            raise PostTrainingFinalizerError(f"{name} artifact path drifted")
        expected_sha = metadata.get("sha256")
        if not _is_sha256(expected_sha):
            raise PostTrainingFinalizerError(f"{name} artifact hash is invalid")
        path = training_dir / expected_name
        if not path.is_file() or path.is_symlink():
            raise PostTrainingFinalizerError(f"missing training artifact: {path}")
        observed_sha = _file_sha256(path)
        if observed_sha != expected_sha:
            raise PostTrainingFinalizerError(f"{name} artifact SHA256 mismatch")
        rows = metadata.get("rows")
        if path.suffix == ".parquet":
            if not isinstance(rows, int) or rows < 0:
                raise PostTrainingFinalizerError(f"{name} row denominator is invalid")
            if pq.ParquetFile(path).metadata.num_rows != rows:
                raise PostTrainingFinalizerError(f"{name} parquet row denominator drifted")
        elif rows is not None:
            raise PostTrainingFinalizerError(f"{name} JSON rows must remain null")
        paths[name] = path
        hashes[name] = observed_sha
    return paths, hashes


def _validate_input_bindings(
    bindings: Any,
    *,
    admitted: training.AdmittedPanel,
    contract: training.TrainingContract,
    formal_identity: FormalInputIdentity,
    arm_panel_root: Path,
) -> None:
    if not isinstance(bindings, Mapping):
        raise PostTrainingFinalizerError("training input bindings are missing")
    expected_hashes = {
        "spec_sha256": contract.spec_sha256,
        "outcome_blind_sha256": contract.outcome_blind_sha256,
        "outer_fold_source_sha256": contract.outer_fold_source_sha256,
        "outer_fold_binding_sha256": contract.outer_fold_binding_sha256,
        "execution_identity_sha256": admitted.execution_identity_sha256,
        "census_manifest_sha256": admitted.census_manifest_sha256,
        "arm_trace_manifest_sha256": admitted.arm_manifest_sha256,
        "joint_outcome_training_manifest_sha256": admitted.training_manifest_sha256,
        "predicate_schema_sha256": contract.predicate_schema_sha256,
        "part_bindings_sha256": admitted.part_bindings_sha256,
    }
    for field, expected in expected_hashes.items():
        if bindings.get(field) != expected:
            raise PostTrainingFinalizerError(f"training input binding {field} drifted")
    expected_paths = {
        "spec_path": contract.spec_path,
        "outcome_blind_path": contract.outcome_blind_path,
        "outer_fold_source_path": contract.outer_fold_source_path,
        "census_manifest_path": admitted.census_manifest_path,
        "arm_trace_manifest_path": admitted.arm_manifest_path,
        "joint_outcome_training_manifest_path": admitted.training_manifest_path,
    }
    for field, expected in expected_paths.items():
        if not _same_path(bindings.get(field), expected, base=arm_panel_root):
            raise PostTrainingFinalizerError(f"training input path {field} drifted")
    expected_parts = training._json_ready(admitted.part_bindings)
    if bindings.get("part_bindings") != expected_parts:
        raise PostTrainingFinalizerError("training part bindings differ from the arm panel")
    if _canonical_sha256(expected_parts) != admitted.part_bindings_sha256:
        raise PostTrainingFinalizerError("original arm-panel part binding hash is invalid")
    _validate_formal_identity_artifact(
        bindings.get("formal_input_identity"),
        expected=formal_identity,
        role="training-manifest formal input identity",
    )


def _panel_audit(value: Any, *, side: str) -> PanelAudit:
    if not isinstance(value, Mapping):
        raise PostTrainingFinalizerError(f"{side} panel audit is missing")
    required = {
        "input_opportunities",
        "eligible_opportunities",
        "joint_censored_opportunities",
        "excluded_opportunity_ids",
        "campaign_weight_min",
        "campaign_weight_max",
    }
    if set(value) != required:
        raise PostTrainingFinalizerError(f"{side} panel audit schema drifted")
    try:
        return PanelAudit(
            input_opportunities=int(value["input_opportunities"]),
            eligible_opportunities=int(value["eligible_opportunities"]),
            joint_censored_opportunities=int(value["joint_censored_opportunities"]),
            excluded_opportunity_ids=tuple(str(item) for item in value["excluded_opportunity_ids"]),
            campaign_weight_min=float(value["campaign_weight_min"]),
            campaign_weight_max=float(value["campaign_weight_max"]),
        )
    except (TypeError, ValueError) as exc:
        raise PostTrainingFinalizerError(f"{side} panel audit is invalid") from exc


def _summary_matches(observed: Mapping[str, Any], expected: Mapping[str, Any]) -> bool:
    for key, expected_value in expected.items():
        if key not in observed:
            return False
        observed_value = observed[key]
        if isinstance(expected_value, float):
            try:
                if not math.isclose(
                    float(observed_value), expected_value, rel_tol=0.0, abs_tol=1e-12
                ):
                    return False
            except (TypeError, ValueError):
                return False
        elif observed_value != expected_value:
            return False
    return True


def _rebuild_nested_results(
    *,
    artifact_paths: Mapping[str, Path],
    admitted: training.AdmittedPanel,
    contract: training.TrainingContract,
    formal_identity: FormalInputIdentity,
    arm_panel_root: Path,
) -> dict[str, NestedChronologicalResult]:
    oof = pd.read_parquet(artifact_paths["oof"])
    evidence = pd.read_parquet(artifact_paths["complexity_evidence"])
    chronology = pd.read_parquet(artifact_paths["chronology_audit"])
    policies = _load_json(artifact_paths["outer_policies"], role="outer policies")
    report = _load_json(artifact_paths["development_report"], role="development report")

    for name, frame in (
        ("oof", oof),
        ("complexity evidence", evidence),
        ("chronology audit", chronology),
    ):
        if "training_side" not in frame:
            raise PostTrainingFinalizerError(f"{name} lacks its side partition")
        if set(frame["training_side"].astype(str)) != {"BUY", "SELL"}:
            raise PostTrainingFinalizerError(f"{name} side denominator drifted")
    if "side" not in oof:
        raise PostTrainingFinalizerError("OOF artifact lacks its economic side")
    if not (oof["training_side"].astype(str) == oof["side"].astype(str)).all():
        raise PostTrainingFinalizerError("OOF training-side and economic side differ")

    policies_by_side = policies.get("policies")
    if (
        policies.get("identity") != IDENTITY
        or policies.get("development_only") is not True
        or not isinstance(policies_by_side, Mapping)
        or set(policies_by_side) != {"BUY", "SELL"}
    ):
        raise PostTrainingFinalizerError("outer-policy artifact identity drifted")
    _require_permissions(
        policies.get("permissions"), expected=LEARNER_PERMISSIONS, role="outer policies"
    )
    policy_input = policies.get("input_contract")
    expected_policy_input = {
        "spec_sha256": contract.spec_sha256,
        "outcome_blind_sha256": contract.outcome_blind_sha256,
        "outer_fold_source_sha256": contract.outer_fold_source_sha256,
        "outer_fold_binding_sha256": contract.outer_fold_binding_sha256,
        "execution_identity_sha256": admitted.execution_identity_sha256,
        "part_bindings_sha256": admitted.part_bindings_sha256,
        "formal_input_identity_sha256": formal_identity.artifact()[
            "formal_input_identity_sha256"
        ],
    }
    if not isinstance(policy_input, Mapping) or dict(policy_input) != expected_policy_input:
        raise PostTrainingFinalizerError("outer policies bind another training input")

    if (
        report.get("identity") != IDENTITY
        or report.get("status")
        != "development_nested_chronological_oof_complete_no_authority"
        or report.get("economic_outcomes_read") is not True
        or report.get("evidence_role") != "historical_native_development_only"
    ):
        raise PostTrainingFinalizerError("Development report identity/status drifted")
    _require_permissions(
        report.get("permissions"), expected=FINAL_PERMISSIONS, role="Development report"
    )
    report_input = report.get("input")
    if not isinstance(report_input, Mapping):
        raise PostTrainingFinalizerError("Development report input binding is missing")
    expected_report_hashes = {
        "spec_sha256": contract.spec_sha256,
        "outcome_blind_sha256": contract.outcome_blind_sha256,
        "arm_trace_manifest_sha256": admitted.arm_manifest_sha256,
        "joint_outcome_training_manifest_sha256": admitted.training_manifest_sha256,
        "census_manifest_sha256": admitted.census_manifest_sha256,
        "execution_identity_sha256": admitted.execution_identity_sha256,
        "outer_fold_source_sha256": contract.outer_fold_source_sha256,
        "outer_fold_binding_sha256": contract.outer_fold_binding_sha256,
        "predicate_schema_sha256": contract.predicate_schema_sha256,
        "part_bindings_sha256": admitted.part_bindings_sha256,
    }
    for field, expected in expected_report_hashes.items():
        if report_input.get(field) != expected:
            raise PostTrainingFinalizerError(f"Development report input {field} drifted")
    expected_report_paths = {
        "spec_path": contract.spec_path,
        "outcome_blind_path": contract.outcome_blind_path,
        "arm_trace_manifest_path": admitted.arm_manifest_path,
        "joint_outcome_training_manifest_path": admitted.training_manifest_path,
        "census_manifest_path": admitted.census_manifest_path,
        "outer_fold_source_path": contract.outer_fold_source_path,
    }
    for field, expected in expected_report_paths.items():
        if not _same_path(report_input.get(field), expected, base=arm_panel_root):
            raise PostTrainingFinalizerError(f"Development report input {field} drifted")
    if report_input.get("outer_fold_field") != contract.outer_fold_field:
        raise PostTrainingFinalizerError("Development report outer-fold field drifted")
    settings = report.get("training_settings") or {}
    if settings != {
        "bootstrap_samples": FORMAL_BOOTSTRAP_SAMPLES,
        "confidence": FORMAL_CONFIDENCE,
        "economic_epsilon_usdc": 0.0,
        "side_pooling": "forbidden",
    }:
        raise PostTrainingFinalizerError("Development report gate settings drifted")
    _validate_formal_identity_artifact(
        report_input.get("formal_input_identity"),
        expected=formal_identity,
        role="Development-report formal input identity",
    )
    denominator = report.get("denominator") or {}
    expected_denominator = {
        "ordered_utc_days": list(contract.ordered_utc_days),
        "opportunity_count": admitted.opportunity_count,
        "arm_row_count": admitted.arm_row_count,
        "actions_per_opportunity": 8,
        "joint_censored_opportunities": admitted.joint_censored_opportunities,
        "training_label_opportunities": admitted.training_label_opportunities,
        "whole_opportunity_censor_exclusion_used": bool(
            admitted.joint_censored_opportunities
        ),
        "complete_case_filtering_used": False,
    }
    if denominator != expected_denominator:
        raise PostTrainingFinalizerError("Development report denominator drifted")

    side_reports = report.get("side_results")
    if not isinstance(side_reports, Mapping) or set(side_reports) != {"BUY", "SELL"}:
        raise PostTrainingFinalizerError("Development report side denominator drifted")
    results: dict[str, NestedChronologicalResult] = {}
    for side in ("BUY", "SELL"):
        side_oof = oof.loc[oof["training_side"].astype(str).eq(side)].drop(
            columns="training_side"
        )
        side_evidence = evidence.loc[
            evidence["training_side"].astype(str).eq(side)
        ].drop(columns="training_side")
        side_chronology = chronology.loc[
            chronology["training_side"].astype(str).eq(side)
        ].drop(columns="training_side")
        side_report = side_reports[side]
        if not isinstance(side_report, Mapping):
            raise PostTrainingFinalizerError(f"{side} Development summary is invalid")
        audit = _panel_audit(side_report.get("panel_audit"), side=side)
        artifacts = policies_by_side.get(side)
        if not isinstance(artifacts, list) or not all(
            isinstance(artifact, Mapping) for artifact in artifacts
        ):
            raise PostTrainingFinalizerError(f"{side} outer policies are missing")
        learner_sha = _file_sha256(Path(learner.__file__).resolve())
        if any(artifact.get("implementation_sha256") != learner_sha for artifact in artifacts):
            raise PostTrainingFinalizerError(
                f"{side} outer policies bind another learner implementation"
            )
        result = NestedChronologicalResult(
            oof=side_oof.reset_index(drop=True),
            complexity_evidence=side_evidence.reset_index(drop=True),
            chronology_audit=side_chronology.reset_index(drop=True),
            outer_policy_artifacts=tuple(dict(item) for item in artifacts),
            panel_audit=audit,
            permissions=LEARNER_PERMISSIONS,
        )
        side_panel = admitted.frame.loc[admitted.frame["side"].astype(str).eq(side)].copy()
        try:
            training._validate_nested_result(
                result,
                side=side,
                side_panel=side_panel,
                contract=contract,
            )
        except (TypeError, ValueError, training.TrainingAdmissionError) as exc:
            raise PostTrainingFinalizerError(
                f"{side} published nested result failed revalidation: {exc}"
            ) from exc
        expected_summary = training._side_summary(result.oof)
        if not _summary_matches(side_report, expected_summary):
            raise PostTrainingFinalizerError(f"{side} Development summary drifted")
        if int(side_report.get("outer_policy_count", -1)) != len(artifacts):
            raise PostTrainingFinalizerError(f"{side} outer-policy count drifted")
        results[side] = result
    return results


def load_validated_post_training_input(
    training_dir: Path,
    arm_panel_root: Path,
    *,
    contract: training.TrainingContract | None = None,
) -> ValidatedPostTrainingInput:
    """Re-admit both the immutable training output and its original arm panel."""

    training_dir = training_dir.resolve()
    arm_panel_root = arm_panel_root.resolve()
    if not training_dir.is_dir():
        raise PostTrainingFinalizerError(f"training directory is missing: {training_dir}")
    contract = contract or training._load_training_contract()
    try:
        admitted = training.load_formal_arm_panel(arm_panel_root, contract=contract)
    except training.TrainingAdmissionError as exc:
        raise PostTrainingFinalizerError(f"original arm panel admission failed: {exc}") from exc
    formal_identity = _expected_formal_identity(admitted, contract)

    manifest_path = training_dir / TRAINING_MANIFEST_NAME
    if manifest_path.is_symlink():
        raise PostTrainingFinalizerError("training artifact manifest cannot be a symlink")
    manifest = _load_json(manifest_path, role="training artifact manifest")
    manifest_sha = _file_sha256(manifest_path)
    success_path = training_dir / "_SUCCESS"
    if not success_path.is_file() or success_path.is_symlink():
        raise PostTrainingFinalizerError("training _SUCCESS marker is missing")
    try:
        success_value = success_path.read_text(encoding="ascii").strip()
    except (OSError, UnicodeError) as exc:
        raise PostTrainingFinalizerError("training _SUCCESS marker is invalid") from exc
    if success_value != manifest_sha:
        raise PostTrainingFinalizerError("training _SUCCESS does not bind its manifest")
    if (
        manifest.get("identity") != IDENTITY
        or manifest.get("status") != "atomic_development_training_artifacts_admitted"
        or manifest.get("schema_version")
        != f"{training.OUTPUT_SCHEMA_VERSION}.artifact_manifest"
        or manifest.get("input_execution_identity_sha256")
        != admitted.execution_identity_sha256
    ):
        raise PostTrainingFinalizerError("training artifact manifest identity drifted")
    _require_permissions(
        manifest.get("permissions"), expected=FINAL_PERMISSIONS, role="training manifest"
    )
    orchestrator_sha = manifest.get("training_orchestrator_sha256")
    if orchestrator_sha != _file_sha256(Path(training.__file__).resolve()):
        raise PostTrainingFinalizerError("training orchestrator implementation hash drifted")
    _validate_input_bindings(
        manifest.get("input_bindings"),
        admitted=admitted,
        contract=contract,
        formal_identity=formal_identity,
        arm_panel_root=arm_panel_root,
    )
    formal_denominator = manifest.get("formal_denominator")
    expected_formal_denominator = {
        "ordered_utc_days": list(contract.ordered_utc_days),
        "opportunity_count": admitted.opportunity_count,
        "arm_row_count": admitted.arm_row_count,
        "predicate_column_count": len(contract.predicate_names),
        "outer_test_days_by_zero_based_fold": [
            list(values) for values in contract.outer_test_days_by_fold
        ],
    }
    if formal_denominator != expected_formal_denominator:
        raise PostTrainingFinalizerError("training formal denominator drifted")
    artifact_paths, artifact_hashes = _validate_training_artifacts(training_dir, manifest)
    results = _rebuild_nested_results(
        artifact_paths=artifact_paths,
        admitted=admitted,
        contract=contract,
        formal_identity=formal_identity,
        arm_panel_root=arm_panel_root,
    )
    return ValidatedPostTrainingInput(
        contract=contract,
        admitted=admitted,
        formal_input_identity=formal_identity,
        training_manifest_path=manifest_path,
        training_manifest_sha256=manifest_sha,
        training_success_sha256=_file_sha256(success_path),
        artifact_paths=artifact_paths,
        artifact_sha256=artifact_hashes,
        results=results,
    )


def finalize_post_training(
    training_dir: Path,
    arm_panel_root: Path,
    output_dir: Path,
    *,
    contract: training.TrainingContract | None = None,
) -> dict[str, Any]:
    """Evaluate frozen OOF gates and atomically publish closure or side refits."""

    validated = load_validated_post_training_input(
        training_dir,
        arm_panel_root,
        contract=contract,
    )
    output_dir = output_dir.resolve()
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    if output_dir.exists():
        raise PostTrainingFinalizerError(f"refusing to overwrite finalizer output: {output_dir}")

    gates: dict[str, Any] = {}
    freezes: list[Any] = []
    for side in ("BUY", "SELL"):
        result = validated.results[side]
        gate = evaluate_outer_oof_gate(
            result,
            side=side,
            economic_epsilon_usdc=0.0,
            confidence=FORMAL_CONFIDENCE,
            bootstrap_samples=FORMAL_BOOTSTRAP_SAMPLES,
            synthetic_mode=validated.contract.synthetic_test_only,
        )
        gates[side] = gate
        if not gate.passed:
            continue
        side_panel = validated.admitted.frame.loc[
            validated.admitted.frame["side"].astype(str).eq(side)
        ].copy()
        freeze = freeze_final_side_policy(
            side_panel,
            result,
            side=side,
            formal_input_identity=validated.formal_input_identity,
            economic_epsilon_usdc=0.0,
            confidence=FORMAL_CONFIDENCE,
            bootstrap_samples=FORMAL_BOOTSTRAP_SAMPLES,
            synthetic_mode=validated.contract.synthetic_test_only,
        )
        if freeze.outer_oof_gate.artifact() != gate.artifact():
            raise PostTrainingFinalizerError(f"{side} refit re-evaluated a different OOF gate")
        freezes.append(freeze)

    staging = Path(
        tempfile.mkdtemp(prefix=f".{output_dir.name}.staging.", dir=output_dir.parent)
    )
    try:
        gate_body = {
            "schema_version": f"{IDENTITY}.post_training_outer_oof_gates.v1",
            "identity": IDENTITY,
            "gate_contract": {
                "confidence": FORMAL_CONFIDENCE,
                "bootstrap_samples": FORMAL_BOOTSTRAP_SAMPLES,
                "economic_epsilon_usdc": 0.0,
                "side_pooling": "forbidden",
            },
            "side_gates": {side: gates[side].artifact() for side in ("BUY", "SELL")},
            "passing_sides": [freeze.side for freeze in freezes],
            "permissions": FINAL_PERMISSIONS,
        }
        gate_artifact = {
            **gate_body,
            "post_training_outer_oof_gates_sha256": _canonical_sha256(gate_body),
        }
        _write_json(staging / "outer_oof_gates.json", gate_artifact)

        result_artifact_name: str
        if freezes:
            result_artifact_name = "final_policy_bundle.json"
            result_artifact = serialize_final_policy_bundle(freezes)
            _require_permissions(
                result_artifact.get("permissions"),
                expected=LEARNER_PERMISSIONS,
                role="final policy bundle",
            )
        else:
            result_artifact_name = "closure.json"
            closure_body = {
                "schema_version": f"{IDENTITY}.post_training_closure.v1",
                "identity": IDENTITY,
                "status": "closed_no_side_passed_frozen_outer_oof_gate",
                "closed_scope": (
                    "multiscale_ema_boolean_cooldown_duration_policy_v1_"
                    "post_training_development_gate"
                ),
                "reason": "BUY_and_SELL_outer_OOF_LCB_not_above_zero",
                "passing_sides": [],
                "full_development_refit_sides": [],
                "validation_or_holdout_may_rescue": False,
                "permissions": FINAL_PERMISSIONS,
            }
            result_artifact = {
                **closure_body,
                "closure_sha256": _canonical_sha256(closure_body),
            }
        _write_json(staging / result_artifact_name, result_artifact)

        status = (
            "development_side_policy_refit_frozen_no_authority"
            if freezes
            else "development_closed_no_side_policy_frozen"
        )
        report = {
            "schema_version": f"{IDENTITY}.post_training_finalizer_report.v1",
            "identity": IDENTITY,
            "status": status,
            "economic_outcomes_read": True,
            "input": {
                "training_manifest_path": str(validated.training_manifest_path),
                "training_manifest_sha256": validated.training_manifest_sha256,
                "training_success_sha256": validated.training_success_sha256,
                "training_artifact_sha256": dict(validated.artifact_sha256),
                "original_arm_trace_manifest_path": validated.admitted.arm_manifest_path,
                "original_arm_trace_manifest_sha256": (
                    validated.admitted.arm_manifest_sha256
                ),
                "formal_input_identity": validated.formal_input_identity.artifact(),
            },
            "gate_contract": gate_body["gate_contract"],
            "side_gates": gate_body["side_gates"],
            "passing_sides": [freeze.side for freeze in freezes],
            "full_development_refit_sides": [freeze.side for freeze in freezes],
            "result_artifact": result_artifact_name,
            "limitations": {
                "policy_level_full_path_replay_complete": False,
                "restart_aware_confirmation_complete": False,
                "validation_or_holdout_read": False,
                "action_or_live_authority": False,
            },
            "permissions": FINAL_PERMISSIONS,
        }
        _write_json(staging / "post_training_report.json", report)

        output_artifact_names = (
            "outer_oof_gates.json",
            result_artifact_name,
            "post_training_report.json",
        )
        output_manifest = {
            "schema_version": f"{IDENTITY}.post_training_finalizer_manifest.v1",
            "identity": IDENTITY,
            "status": "atomic_post_training_decision_admitted",
            "input_bindings": report["input"],
            "formal_gate_contract": report["gate_contract"],
            "artifacts": {
                name: {
                    "path": name,
                    "sha256": _file_sha256(staging / name),
                }
                for name in output_artifact_names
            },
            "finalizer_implementation_sha256": _file_sha256(Path(__file__).resolve()),
            "permissions": FINAL_PERMISSIONS,
        }
        manifest_path = staging / FINALIZER_MANIFEST_NAME
        _write_json(manifest_path, output_manifest)
        (staging / "_SUCCESS").write_text(
            f"{_file_sha256(manifest_path)}\n", encoding="ascii"
        )
        os.replace(staging, output_dir)
        return report
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--training-dir", type=Path, required=True)
    parser.add_argument("--arm-panel-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    report = finalize_post_training(
        args.training_dir,
        args.arm_panel_root,
        args.output_dir,
    )
    print(json.dumps(report, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
