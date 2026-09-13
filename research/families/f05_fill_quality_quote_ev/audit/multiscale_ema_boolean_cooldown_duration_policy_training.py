"""Disjoint Development training orchestration for the frozen Boolean policy.

This module is deliberately downstream of replay admission.  It neither runs
replay nor imports the replay runner.  It accepts only the complete,
hash-validated formal arm panel, preserves whole-opportunity censoring, and
runs the frozen nested chronological learner separately for BUY and SELL.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import tempfile
from collections.abc import Mapping
from dataclasses import asdict, dataclass, is_dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from pyarrow import types as pa_types

from data_paths import resolve_portable_path
from research.families.f05_fill_quality_quote_ev.audit import (
    multiscale_ema_boolean_cooldown_rule_learner as rule_learner,
)
from research.families.f05_fill_quality_quote_ev.audit.multiscale_ema_boolean_cooldown_rule_learner import (
    IDENTITY,
    OUTCOME_VALUE_COLUMN,
    SPEC_PATH,
    FormalInputIdentity,
    attest_formal_input_panel,
    build_nested_chronological_folds,
    load_frozen_search_contract,
    run_nested_chronological_oof,
)
from research.governance.public_machine_projection import (
    PublicMachineProjectionError,
    source_document_path,
    source_identity_sha256,
)

ARM_MANIFEST_NAME = "arm_trace_manifest.json"
TRAINING_MANIFEST_NAME = "joint_outcome_training_manifest.json"
OUTPUT_SCHEMA_VERSION = f"{IDENTITY}.training_orchestration.v1"
SIDES = ("BUY", "SELL")
EXPECTED_FORMAL_DAYS = 40
EXPECTED_FORMAL_OPPORTUNITIES = 8_600
EXPECTED_FORMAL_ARM_ROWS = 68_800
EXPECTED_ACTIONS_PER_SIDE = 8
EXPECTED_PREDICATE_COLUMNS = 360
EXPECTED_OUTER_FOLDS = 4
FORMAL_BOOTSTRAP_SAMPLES = 500
FORMAL_CONFIDENCE = 0.95
FROZEN_SPEC_SHA256 = "9f8c5abce4817b029d943648a46ab115d6ce7ac7f758b1326a48d75fa446e8ce"
FROZEN_OUTCOME_BLIND_SHA256 = "965400c6fe5408a6f49dd4253c96d6673d4621451af561a2bc7921591c2d7035"
FROZEN_FOLD_SOURCE_SHA256 = "b59f9f5a3c9cbdd1fa714abe6ddf8ef23e19654374c354a6840e6f943a7c6908"
HASH_FIELDS = (
    "arm_trace_sha256",
    "manifest_sha256",
    "census_sha256",
)
FORMAL_BOOLEAN_COLUMNS = (
    "joint_censored",
    "joint_washout_complete",
    "training_label_eligible",
    "right_censored",
    "arm_washout_complete",
    "washout_ts_is_joint_economic_washout",
)
SIDE_CHECKPOINT_SCHEMA_VERSION = f"{OUTPUT_SCHEMA_VERSION}.side_checkpoint.v1"
# This binds the result-producing orchestration used by the admitted BUY/SELL
# checkpoints. Validation-only changes do not invalidate two hours of frozen
# model computation; change this hash whenever pre-validation computation does.
SIDE_CHECKPOINT_PRODUCER_SHA256 = "78f1bc2b58e721da0ee5b9946f22ec2f02fc31e27d6f4a4211dbf6da7eebf2c8"
SIDE_CHECKPOINT_FILES = {
    "oof": "oof.parquet",
    "complexity_evidence": "complexity_evidence.parquet",
    "chronology_audit": "chronology_audit.parquet",
    "outer_policies": "outer_policies.json",
    "panel_audit": "panel_audit.json",
}


class TrainingAdmissionError(RuntimeError):
    """Formal arm data or output admission violated the frozen contract."""


@dataclass(frozen=True)
class TrainingContract:
    spec_path: str
    spec_sha256: str
    outcome_blind_path: str
    outcome_blind_sha256: str
    ordered_utc_days: tuple[str, ...]
    expected_opportunities: int
    expected_arm_rows: int
    actions_by_side: Mapping[str, tuple[str, ...]]
    predicate_names: tuple[str, ...]
    required_outer_folds: int
    outer_fold_source_path: str
    outer_fold_source_sha256: str
    outer_fold_field: str
    outer_test_days_by_fold: tuple[tuple[str, ...], ...]
    outer_fold_binding_sha256: str
    predicate_schema_sha256: str
    synthetic_test_only: bool = False


@dataclass(frozen=True)
class AdmittedPanel:
    frame: pd.DataFrame
    arm_manifest_path: str
    arm_manifest_sha256: str
    training_manifest_path: str
    training_manifest_sha256: str
    census_manifest_path: str
    census_manifest_sha256: str
    execution_identity_sha256: str
    opportunity_count: int
    arm_row_count: int
    joint_censored_opportunities: int
    training_label_opportunities: int
    part_bindings: tuple[Mapping[str, Any], ...]
    part_bindings_sha256: str


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


def _require_sha256(value: Any, *, role: str) -> str:
    if not _is_sha256(value):
        raise TrainingAdmissionError(f"{role} is missing a valid SHA256")
    return str(value)


def _load_json(path: Path, *, role: str) -> dict[str, Any]:
    if not path.is_file():
        raise TrainingAdmissionError(f"missing {role}: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TrainingAdmissionError(f"invalid {role}: {path}") from exc
    if not isinstance(value, dict):
        raise TrainingAdmissionError(f"{role} must be a JSON object")
    return value


def _resolve_path(
    value: Any,
    *,
    root: Path,
    role: str,
    relocated_from_root: Path | None = None,
) -> Path:
    if not isinstance(value, str) or not value:
        raise TrainingAdmissionError(f"{role} path is missing")
    path = resolve_portable_path(value, root=root)
    if not path.is_absolute():
        return root / path
    if relocated_from_root is None:
        return path
    relocated_from_root = relocated_from_root.expanduser().resolve()
    try:
        relative = path.relative_to(relocated_from_root)
    except ValueError:
        return path
    return root / relative


def _validate_file(path: Path, expected_sha256: Any, *, role: str) -> str:
    expected = _require_sha256(expected_sha256, role=f"{role} hash")
    if not path.is_file():
        raise TrainingAdmissionError(f"missing {role}: {path}")
    actual = _file_sha256(path)
    if actual != expected:
        raise TrainingAdmissionError(
            f"{role} SHA256 mismatch: expected {expected}, observed {actual}"
        )
    return actual


def _validate_source_document(path: Path, expected_sha256: Any, *, role: str) -> tuple[Path, str]:
    """Validate a frozen source identity and resolve retained exact bytes safely."""

    expected = _require_sha256(expected_sha256, role=f"{role} hash")
    if not path.is_file():
        raise TrainingAdmissionError(f"missing {role}: {path}")
    try:
        actual = source_identity_sha256(path)
        source = source_document_path(path, require_private=False)
    except (OSError, PublicMachineProjectionError) as exc:
        raise TrainingAdmissionError(f"{role} source identity is unavailable") from exc
    if actual != expected:
        raise TrainingAdmissionError(
            f"{role} source SHA256 mismatch: expected {expected}, observed {actual}"
        )
    return source, actual


def _require_development_only(payload: Mapping[str, Any], *, role: str) -> None:
    _require_locked_evidence_unread(payload, role=role)
    for field in ("action_authorized", "live_authorized"):
        if payload.get(field) is not False:
            raise TrainingAdmissionError(f"{role} unexpectedly grants {field}")


def _require_locked_evidence_unread(payload: Mapping[str, Any], *, role: str) -> None:
    if payload.get("validation_read") is not False:
        raise TrainingAdmissionError(f"{role} does not prove Validation remained unread")
    if payload.get("sealed_holdout_read") is not False:
        raise TrainingAdmissionError(f"{role} does not prove sealed holdout remained unread")


def _validate_success_marker(path: Path, *, manifest_sha256: str, role: str) -> str:
    if not path.is_file():
        raise TrainingAdmissionError(f"missing {role}: {path}")
    try:
        value = path.read_text(encoding="ascii").strip()
    except (OSError, UnicodeError) as exc:
        raise TrainingAdmissionError(f"invalid {role}: {path}") from exc
    if value != manifest_sha256:
        raise TrainingAdmissionError(f"{role} does not bind its admitted manifest")
    return _file_sha256(path)


def _frozen_outer_fold_binding(
    spec: Mapping[str, Any],
    *,
    spec_path: Path,
    ordered_days: tuple[str, ...],
) -> tuple[Path, str, str, tuple[tuple[str, ...], ...], str]:
    chronology_contract = spec.get("nested_chronological_development") or {}
    fold_field = str(chronology_contract.get("outer_fold_field") or "")
    if fold_field != "chronological_oof":
        raise TrainingAdmissionError("frozen outer-fold field drifted")
    fold_path = _resolve_path(
        chronology_contract.get("outer_fold_source_path"),
        root=spec_path.parents[4],
        role="frozen outer-fold source",
    ).resolve()
    fold_document, fold_sha = _validate_source_document(
        fold_path,
        chronology_contract.get("outer_fold_source_sha256"),
        role="frozen outer-fold source",
    )
    if fold_sha != FROZEN_FOLD_SOURCE_SHA256:
        raise TrainingAdmissionError("frozen outer-fold source SHA256 drifted")
    source = _load_json(fold_document, role="frozen outer-fold source")
    permissions = source.get("permissions") or {}
    _require_locked_evidence_unread(permissions, role="frozen outer-fold source permissions")
    source_days = tuple(
        str(value)
        for value in source.get("development_denominator", {}).get("ordered_utc_days", ())
    )
    if source_days != ordered_days:
        raise TrainingAdmissionError("outer-fold source ordered Development days drifted")
    chronology = source.get(fold_field)
    if not isinstance(chronology, Mapping):
        raise TrainingAdmissionError("outer-fold source lacks chronological_oof")
    rows = chronology.get("folds")
    if not isinstance(rows, list) or len(rows) != EXPECTED_OUTER_FOLDS:
        raise TrainingAdmissionError("outer-fold source must contain exactly four folds")
    test_days_by_fold: list[tuple[str, ...]] = []
    initial_history_days = EXPECTED_FORMAL_DAYS - 24
    for offset, row in enumerate(rows):
        if not isinstance(row, Mapping) or int(row.get("fold", -1)) != offset + 1:
            raise TrainingAdmissionError("outer-fold source fold ids drifted")
        expected_history = ordered_days[: initial_history_days + offset * 6]
        expected_test = ordered_days[
            initial_history_days + offset * 6 : initial_history_days + (offset + 1) * 6
        ]
        if tuple(str(value) for value in row.get("history_days", ())) != expected_history:
            raise TrainingAdmissionError(f"outer fold {offset} history-day identity drifted")
        observed_test = tuple(str(value) for value in row.get("test_days", ()))
        if observed_test != expected_test:
            raise TrainingAdmissionError(f"outer fold {offset} test-day identity drifted")
        test_days_by_fold.append(observed_test)
    flattened = tuple(day for fold_days in test_days_by_fold for day in fold_days)
    if flattened != ordered_days[initial_history_days:]:
        raise TrainingAdmissionError("outer-fold test union is not the frozen final 24 days")
    binding = {
        "path": str(fold_path),
        "sha256": fold_sha,
        "field": fold_field,
        "ordered_utc_days": list(ordered_days),
        "test_days_by_zero_based_fold": [list(values) for values in test_days_by_fold],
    }
    return fold_path, fold_sha, fold_field, tuple(test_days_by_fold), _canonical_sha256(binding)


def _load_training_contract(spec_path: Path = SPEC_PATH) -> TrainingContract:
    spec_path = spec_path.resolve()
    spec_document, spec_sha256 = _validate_source_document(
        spec_path,
        FROZEN_SPEC_SHA256,
        role="frozen Boolean cooldown Spec",
    )
    spec = _load_json(spec_document, role="frozen Boolean cooldown Spec")
    if spec.get("identity") != IDENTITY:
        raise TrainingAdmissionError("frozen Spec identity mismatch")
    _require_development_only(spec["permission_boundary"], role="frozen Spec permissions")
    source = spec["outcome_blind_duration_artifact"]
    source_path = _resolve_path(
        source["path"], root=spec_path.parents[4], role="outcome-blind artifact"
    )
    source_document, source_sha256 = _validate_source_document(
        source_path,
        source.get("sha256"),
        role="outcome-blind duration artifact",
    )
    if source_sha256 != FROZEN_OUTCOME_BLIND_SHA256:
        raise TrainingAdmissionError("outcome-blind duration artifact SHA256 drifted")
    frozen = _load_json(source_document, role="outcome-blind duration artifact")
    if frozen.get("identity") != IDENTITY:
        raise TrainingAdmissionError("outcome-blind artifact identity mismatch")
    permissions = frozen.get("permissions") or {}
    _require_development_only(permissions, role="outcome-blind permissions")
    days = tuple(frozen["baseline_projection"]["ordered_utc_days"])
    if (
        len(days) != EXPECTED_FORMAL_DAYS
        or len(days) != len(set(days))
        or tuple(sorted(days)) != days
    ):
        raise TrainingAdmissionError("Development UTC-day denominator is not unique and ordered")
    actions = {side: tuple(source["candidate_actions"][side]) for side in SIDES}
    frozen_actions = {
        side: tuple(
            str(row["policy_id"]) for row in frozen["duration_source"]["candidate_actions"][side]
        )
        for side in SIDES
    }
    if actions != frozen_actions:
        raise TrainingAdmissionError("Spec and outcome-blind action universes differ")
    if any(
        len(values) != EXPECTED_ACTIONS_PER_SIDE or "CONTROL_85N" not in values
        for values in actions.values()
    ):
        raise TrainingAdmissionError("each side must retain all eight frozen actions")
    predicates = tuple(f"predicate::{row['name']}" for row in frozen["atomic_predicates"])
    expected_opportunities = int(
        spec["development_denominator"]["expected_opportunity_count_from_outcome_blind_census"]
    )
    expected_arm_rows = int(spec["development_denominator"]["expected_single_action_fork_count"])
    if (
        expected_opportunities != EXPECTED_FORMAL_OPPORTUNITIES
        or expected_arm_rows != EXPECTED_FORMAL_ARM_ROWS
        or expected_arm_rows != expected_opportunities * EXPECTED_ACTIONS_PER_SIDE
    ):
        raise TrainingAdmissionError("frozen opportunity/arm denominator is inconsistent")
    if (
        len(predicates) != EXPECTED_PREDICATE_COLUMNS
        or len(set(predicates)) != EXPECTED_PREDICATE_COLUMNS
    ):
        raise TrainingAdmissionError("frozen predicate denominator must contain 360 unique columns")
    predicate_source = spec.get("ema_state_contract", {}).get("atomic_predicate_source", {})
    if (
        predicate_source.get("sha256") != source_sha256
        or int(predicate_source.get("predicate_count", -1)) != EXPECTED_PREDICATE_COLUMNS
        or _resolve_path(
            predicate_source.get("path"), root=spec_path.parents[4], role="predicate source"
        ).resolve()
        != source_path.resolve()
    ):
        raise TrainingAdmissionError("frozen predicate-source binding drifted")
    (
        fold_path,
        fold_sha256,
        fold_field,
        test_days_by_fold,
        fold_binding_sha256,
    ) = _frozen_outer_fold_binding(spec, spec_path=spec_path, ordered_days=days)
    required_outer_folds = int(spec["development_gates"]["required_outer_folds"])
    if required_outer_folds != EXPECTED_OUTER_FOLDS:
        raise TrainingAdmissionError("frozen outer-fold denominator drifted")
    return TrainingContract(
        spec_path=str(spec_path),
        spec_sha256=spec_sha256,
        outcome_blind_path=str(source_path),
        outcome_blind_sha256=source_sha256,
        ordered_utc_days=days,
        expected_opportunities=expected_opportunities,
        expected_arm_rows=expected_arm_rows,
        actions_by_side=actions,
        predicate_names=predicates,
        required_outer_folds=required_outer_folds,
        outer_fold_source_path=str(fold_path),
        outer_fold_source_sha256=fold_sha256,
        outer_fold_field=fold_field,
        outer_test_days_by_fold=test_days_by_fold,
        outer_fold_binding_sha256=fold_binding_sha256,
        predicate_schema_sha256=_canonical_sha256(list(predicates)),
    )


def _validate_part_schema(
    path: Path,
    *,
    contract: TrainingContract,
    expected_schema_sha256: Any,
) -> None:
    schema = pq.ParquetFile(path).schema_arrow
    names = [name for name in schema.names if name.startswith("predicate::")]
    if tuple(names) != contract.predicate_names:
        raise TrainingAdmissionError(f"{path} predicate schema/order drifted")
    if any(not pa_types.is_boolean(schema.field(name).type) for name in names):
        raise TrainingAdmissionError(f"{path} contains a non-Boolean predicate")
    for name in FORMAL_BOOLEAN_COLUMNS:
        if name not in schema.names or not pa_types.is_boolean(schema.field(name).type):
            raise TrainingAdmissionError(f"{path} formal Boolean column drifted: {name}")
    observed = _canonical_sha256(names)
    expected = _require_sha256(expected_schema_sha256, role="predicate schema hash")
    if observed != expected or observed != contract.predicate_schema_sha256:
        raise TrainingAdmissionError(f"{path} predicate schema SHA256 mismatch")


def _validate_boolean_frame_columns(frame: pd.DataFrame) -> None:
    for column in (
        *FORMAL_BOOLEAN_COLUMNS,
        *tuple(c for c in frame if c.startswith("predicate::")),
    ):
        values = frame[column]
        if values.isna().any() or not pd.api.types.is_bool_dtype(values.dtype):
            raise TrainingAdmissionError(f"formal panel Boolean column drifted: {column}")


def _validate_census_opportunity_projection(
    census_path: Path,
    arm_frame: pd.DataFrame,
    *,
    day: str,
    contract: TrainingContract,
) -> str:
    columns = ["opportunity_id", "utc_day", "side", *contract.predicate_names]
    try:
        census = pd.read_parquet(census_path, columns=columns)
    except (OSError, ValueError, KeyError) as exc:
        raise TrainingAdmissionError(f"{day} census opportunity projection is invalid") from exc
    if census["opportunity_id"].duplicated().any():
        raise TrainingAdmissionError(f"{day} census contains duplicate opportunity ids")
    arm = arm_frame.drop_duplicates("opportunity_id").loc[:, columns].copy()
    if len(census) != len(arm):
        raise TrainingAdmissionError(f"{day} census/arm opportunity denominator drifted")
    for frame, role in ((census, "census"), (arm, "arm")):
        for name in contract.predicate_names:
            if frame[name].isna().any() or not pd.api.types.is_bool_dtype(frame[name].dtype):
                raise TrainingAdmissionError(f"{day} {role} predicate value drifted: {name}")
    census = census.sort_values("opportunity_id", kind="stable").reset_index(drop=True)
    arm = arm.sort_values("opportunity_id", kind="stable").reset_index(drop=True)
    if not census.equals(arm):
        raise TrainingAdmissionError(f"{day} census/arm opportunity state drifted")
    return _canonical_sha256(census["opportunity_id"].astype(str).tolist())


def _validate_joint_panel(frame: pd.DataFrame, contract: TrainingContract) -> dict[str, int]:
    required = {
        "opportunity_id",
        "utc_day",
        "side",
        "campaign_side_id",
        "assignment_ts_ns",
        "washout_ts_ns",
        "duration_policy_id",
        OUTCOME_VALUE_COLUMN,
        "joint_censored",
        "joint_washout_complete",
        "training_label_eligible",
        "right_censored",
        "arm_washout_complete",
        "washout_ts_is_joint_economic_washout",
        *contract.predicate_names,
    }
    missing = required - set(frame.columns)
    if missing:
        raise TrainingAdmissionError(f"formal arm panel is missing columns: {sorted(missing)}")
    _validate_boolean_frame_columns(frame)
    if len(frame) != contract.expected_arm_rows:
        raise TrainingAdmissionError("formal arm-row denominator drifted")
    if frame.duplicated(["opportunity_id", "duration_policy_id"]).any():
        raise TrainingAdmissionError("formal panel contains duplicate opportunity/action rows")
    observed_day_order = tuple(dict.fromkeys(frame["utc_day"].astype(str)))
    if observed_day_order != contract.ordered_utc_days:
        raise TrainingAdmissionError("formal arm panel Development-day denominator drifted")
    if set(frame["side"].astype(str)) != set(SIDES):
        raise TrainingAdmissionError("formal arm panel contains an unsupported side")
    for column in ("opportunity_id", "campaign_side_id", "duration_policy_id"):
        values = frame[column].astype("string")
        if values.isna().any() or values.str.strip().eq("").any():
            raise TrainingAdmissionError(f"formal arm panel contains an empty {column}")
    assignment = pd.to_numeric(frame["assignment_ts_ns"], errors="coerce")
    washout = pd.to_numeric(frame["washout_ts_ns"], errors="coerce")
    if (
        assignment.isna().any()
        or washout.isna().any()
        or (assignment < 0).any()
        or (washout < assignment).any()
    ):
        raise TrainingAdmissionError("formal arm panel contains invalid assignment/washout clocks")

    opportunity_count = 0
    joint_censored_count = 0
    training_eligible_count = 0
    common_fields = (
        "utc_day",
        "side",
        "campaign_side_id",
        "assignment_ts_ns",
        "washout_ts_ns",
        "joint_censored",
        "joint_washout_complete",
        "training_label_eligible",
        "washout_ts_is_joint_economic_washout",
    )
    for opportunity_id, rows in frame.groupby("opportunity_id", sort=False):
        opportunity_count += 1
        if len(rows) != 8:
            raise TrainingAdmissionError(f"{opportunity_id} does not contain exactly eight arms")
        side = str(rows["side"].iloc[0])
        if set(rows["duration_policy_id"].astype(str)) != set(contract.actions_by_side[side]):
            raise TrainingAdmissionError(f"{opportunity_id} lacks the frozen side action universe")
        for field in (*common_fields, *contract.predicate_names):
            if rows[field].nunique(dropna=False) != 1:
                raise TrainingAdmissionError(f"{opportunity_id} has inconsistent {field}")
        expected_censored = bool(
            rows["right_censored"].any() or (~rows["arm_washout_complete"]).any()
        )
        expected_complete = bool(not expected_censored and rows["arm_washout_complete"].all())
        observed_censored = bool(rows["joint_censored"].iloc[0])
        observed_complete = bool(rows["joint_washout_complete"].iloc[0])
        observed_eligible = bool(rows["training_label_eligible"].iloc[0])
        if (
            observed_censored != expected_censored
            or observed_complete != expected_complete
            or observed_eligible != expected_complete
            or bool(rows["washout_ts_is_joint_economic_washout"].iloc[0]) != expected_complete
        ):
            raise TrainingAdmissionError(f"{opportunity_id} joint-censor contract drifted")
        values = pd.to_numeric(rows[OUTCOME_VALUE_COLUMN], errors="coerce")
        if observed_eligible and not np.isfinite(values.to_numpy(dtype=float)).all():
            raise TrainingAdmissionError(
                f"{opportunity_id} has an incomplete eligible reward vector"
            )
        joint_censored_count += int(observed_censored)
        training_eligible_count += int(observed_eligible)
    if opportunity_count != contract.expected_opportunities:
        raise TrainingAdmissionError("formal opportunity denominator drifted")
    return {
        "opportunity_count": opportunity_count,
        "joint_censored_opportunities": joint_censored_count,
        "training_label_opportunities": training_eligible_count,
    }


def _validate_census_chain(
    census_manifest_path: Path,
    *,
    expected_sha256: Any,
    input_root: Path,
    contract: TrainingContract,
    execution_sha256: str,
    relocated_from_root: Path | None = None,
) -> tuple[str, dict[str, Mapping[str, Any]]]:
    census_sha = _validate_file(
        census_manifest_path,
        expected_sha256,
        role="formal census manifest",
    )
    census = _load_json(census_manifest_path, role="formal census manifest")
    _require_locked_evidence_unread(census, role="formal census manifest")
    if (
        census.get("identity") != IDENTITY
        or census.get("status") != "formal_full_development_census"
        or census.get("execution_identity_sha256") != execution_sha256
        or tuple(str(value) for value in census.get("ordered_utc_days", ()))
        != contract.ordered_utc_days
        or int(census.get("day_count", -1)) != len(contract.ordered_utc_days)
        or int(census.get("opportunity_count", -1)) != contract.expected_opportunities
        or int(census.get("exact_formal_fork_task_count", -1)) != contract.expected_arm_rows
        or census.get("formal_sampling") != "none_full_coverage"
        or census.get("economic_outcomes_read") is not False
    ):
        raise TrainingAdmissionError("formal census identity or denominator drifted")
    rows = census.get("parts")
    if not isinstance(rows, list) or len(rows) != len(contract.ordered_utc_days):
        raise TrainingAdmissionError("formal census part denominator drifted")
    if tuple(str(row.get("utc_day")) for row in rows) != contract.ordered_utc_days:
        raise TrainingAdmissionError("formal census part order/day denominator drifted")

    bindings: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        day = str(row["utc_day"])
        data_path = _resolve_path(
            row.get("data_path"),
            root=input_root,
            role=f"{day} census data",
            relocated_from_root=relocated_from_root,
        )
        manifest_path = _resolve_path(
            row.get("manifest_path"),
            root=input_root,
            role=f"{day} census day manifest",
            relocated_from_root=relocated_from_root,
        )
        data_sha = _validate_file(data_path, row.get("data_sha256"), role=f"{day} census data")
        manifest_sha = _validate_file(
            manifest_path,
            row.get("manifest_sha256"),
            role=f"{day} census day manifest",
        )
        day_manifest = _load_json(manifest_path, role=f"{day} census day manifest")
        _require_locked_evidence_unread(day_manifest, role=f"{day} census day manifest")
        referenced_data = _resolve_path(
            day_manifest.get("data_path"),
            root=input_root,
            role=f"{day} referenced census data",
            relocated_from_root=relocated_from_root,
        )
        opportunity_count = int(row.get("opportunity_count", -1))
        fork_count = int(row.get("fork_task_count", -1))
        if (
            day_manifest.get("identity") != IDENTITY
            or str(day_manifest.get("utc_day")) != day
            or day_manifest.get("execution_identity_sha256") != execution_sha256
            or day_manifest.get("formal_sampling") != "none_full_coverage"
            or day_manifest.get("all_legal_exposure_increasing_fill_opportunities_included")
            is not True
            or day_manifest.get("economic_outcomes_read") is not False
            or referenced_data.resolve() != data_path.resolve()
            or day_manifest.get("data_sha256") != data_sha
            or int(day_manifest.get("opportunity_count", -1)) != opportunity_count
            or int(day_manifest.get("exact_formal_fork_task_count", -1)) != fork_count
            or fork_count != opportunity_count * EXPECTED_ACTIONS_PER_SIDE
        ):
            raise TrainingAdmissionError(f"{day} census admission drifted")
        bindings[day] = {
            "utc_day": day,
            "data_path": str(data_path),
            "data_sha256": data_sha,
            "manifest_path": str(manifest_path),
            "manifest_sha256": manifest_sha,
            "opportunity_count": opportunity_count,
            "fork_task_count": fork_count,
        }
    if (
        sum(int(row["opportunity_count"]) for row in bindings.values())
        != contract.expected_opportunities
        or sum(int(row["fork_task_count"]) for row in bindings.values())
        != contract.expected_arm_rows
    ):
        raise TrainingAdmissionError("formal census part totals drifted")
    return census_sha, bindings


def load_formal_arm_panel(
    input_root: Path,
    *,
    contract: TrainingContract | None = None,
    relocated_from_root: Path | None = None,
) -> AdmittedPanel:
    """Load the complete formal panel after validating every bound artifact."""

    input_root = input_root.resolve()
    contract = contract or _load_training_contract()
    arm_path = input_root / ARM_MANIFEST_NAME
    training_path = input_root / TRAINING_MANIFEST_NAME
    arm = _load_json(arm_path, role="formal arm-trace manifest")
    training = _load_json(training_path, role="joint-outcome training manifest")
    if arm.get("identity") != IDENTITY or training.get("identity") != IDENTITY:
        raise TrainingAdmissionError("formal training identity mismatch")
    if arm.get("status") != "formal_full_development_arm_traces_admitted":
        raise TrainingAdmissionError("arm traces were not formally admitted")
    if training.get("status") != (
        "formal_joint_outcome_training_panel_admitted_with_whole_opportunity_censor_exclusion"
    ):
        raise TrainingAdmissionError("joint-outcome panel was not formally admitted")
    _require_development_only(arm, role="formal arm-trace manifest")
    _require_development_only(training, role="joint-outcome training manifest")
    if arm.get("economic_outcomes_read") is not False:
        raise TrainingAdmissionError("arm manifest was not admitted outcome-blind")
    execution_sha = _require_sha256(arm.get("execution_identity_sha256"), role="execution identity")
    if training.get("execution_identity_sha256") != execution_sha:
        raise TrainingAdmissionError("training and arm execution identities differ")
    expected_arm_hash = _validate_file(
        arm_path,
        training.get("arm_trace_manifest_sha256"),
        role="formal arm-trace manifest",
    )
    referenced_arm = _resolve_path(
        training.get("arm_trace_manifest_path"),
        root=input_root,
        role="referenced arm manifest",
        relocated_from_root=relocated_from_root,
    )
    if referenced_arm.resolve() != arm_path.resolve():
        raise TrainingAdmissionError("training manifest references a different arm manifest")
    census_manifest = _resolve_path(
        arm.get("census_manifest_path"),
        root=input_root,
        role="census manifest",
        relocated_from_root=relocated_from_root,
    )
    census_manifest_sha, census_bindings = _validate_census_chain(
        census_manifest,
        expected_sha256=arm.get("census_manifest_sha256"),
        input_root=input_root,
        contract=contract,
        execution_sha256=execution_sha,
        relocated_from_root=relocated_from_root,
    )
    if tuple(arm.get("ordered_utc_days") or ()) != contract.ordered_utc_days:
        raise TrainingAdmissionError("arm manifest ordered-day denominator drifted")
    if (
        int(arm.get("opportunity_count", -1)) != contract.expected_opportunities
        or int(arm.get("arm_trace_rows", -1)) != contract.expected_arm_rows
        or int(arm.get("expected_actions_per_side", -1)) != EXPECTED_ACTIONS_PER_SIDE
        or arm.get("formal_sampling") != "none_full_cartesian_coverage"
        or arm.get("joint_complete_case_filtering_allowed") is not False
        or arm.get("censor_marks_are_terminal_bounds") is not False
    ):
        raise TrainingAdmissionError("arm manifest denominator or censoring contract drifted")
    if (
        int(training.get("opportunity_count", -1)) != contract.expected_opportunities
        or int(training.get("arm_rows", -1)) != contract.expected_arm_rows
        or int(training.get("actions_per_opportunity", -1)) != EXPECTED_ACTIONS_PER_SIDE
        or training.get("all_materialized_opportunities_have_all_eight_arms") is not True
        or training.get("complete_case_filtering_used") is not False
        or training.get("censor_time_marks_in_training_label") is not False
        or training.get("label_field") != OUTCOME_VALUE_COLUMN
    ):
        raise TrainingAdmissionError("training manifest denominator or censoring contract drifted")

    parts = arm.get("parts")
    if not isinstance(parts, list) or len(parts) != len(contract.ordered_utc_days):
        raise TrainingAdmissionError("formal part denominator is incomplete")
    if tuple(str(part.get("utc_day")) for part in parts) != contract.ordered_utc_days:
        raise TrainingAdmissionError("formal part order/day denominator drifted")

    frames: list[pd.DataFrame] = []
    bindings: list[Mapping[str, Any]] = []
    expected_schema_hash: str | None = None
    for part in parts:
        day = str(part.get("utc_day"))
        for field in HASH_FIELDS:
            _require_sha256(part.get(field), role=f"{day} {field}")
        trace_path = _resolve_path(
            part.get("arm_trace_path"),
            root=input_root,
            role="arm-trace part",
            relocated_from_root=relocated_from_root,
        )
        part_manifest_path = _resolve_path(
            part.get("manifest_path"),
            root=input_root,
            role="arm-trace part manifest",
            relocated_from_root=relocated_from_root,
        )
        census_path = _resolve_path(
            part.get("census_path"),
            root=input_root,
            role="census part",
            relocated_from_root=relocated_from_root,
        )
        trace_sha = _validate_file(
            trace_path, part.get("arm_trace_sha256"), role=f"{day} arm trace"
        )
        manifest_sha = _validate_file(
            part_manifest_path,
            part.get("manifest_sha256"),
            role=f"{day} part manifest",
        )
        census_sha = _validate_file(
            census_path, part.get("census_sha256"), role=f"{day} census part"
        )
        census_binding = census_bindings[day]
        if (
            census_path.resolve() != Path(str(census_binding["data_path"])).resolve()
            or census_sha != census_binding["data_sha256"]
            or int(part.get("opportunity_count", -1)) != int(census_binding["opportunity_count"])
            or int(part.get("arm_trace_row_count", -1)) != int(census_binding["fork_task_count"])
        ):
            raise TrainingAdmissionError(f"{day} arm/census binding drifted")
        part_manifest = _load_json(part_manifest_path, role="arm-trace part manifest")
        _require_development_only(part_manifest, role="arm-trace part manifest")
        referenced_trace = _resolve_path(
            part_manifest.get("data_path"),
            root=input_root,
            role=f"{day} referenced arm trace",
            relocated_from_root=relocated_from_root,
        )
        if (
            part_manifest.get("identity") != IDENTITY
            or part_manifest.get("scope") != "formal"
            or str(part_manifest.get("utc_day")) != day
            or part_manifest.get("limited_diagnostic") is not False
            or part_manifest.get("formal_full_opportunity_coverage") is not True
            or part_manifest.get("execution_identity_sha256") != execution_sha
            or referenced_trace.resolve() != trace_path.resolve()
            or part_manifest.get("data_sha256") != trace_sha
            or part_manifest.get("census_data_sha256") != census_sha
            or int(part_manifest.get("census_opportunity_count", -1))
            != int(part.get("opportunity_count", -2))
            or int(part_manifest.get("included_opportunity_count", -1))
            != int(part.get("opportunity_count", -2))
            or int(part_manifest.get("expected_task_count", -1))
            != int(part.get("arm_trace_row_count", -2))
            or int(part_manifest.get("arm_trace_row_count", -1))
            != int(part.get("arm_trace_row_count", -2))
            or part_manifest.get("python_parity") is not False
            or part_manifest.get("python_full_arm_execution_allowed") is not False
            or part_manifest.get("joint_complete_case_filtering_allowed") is not False
            or part_manifest.get("censor_marks_are_terminal_bounds") is not False
            or part_manifest.get("economic_outcomes_interpreted") is not False
        ):
            raise TrainingAdmissionError(f"{day} part admission drifted")
        success_path = part_manifest_path.parent / "_SUCCESS"
        success_sha = _validate_success_marker(
            success_path,
            manifest_sha256=manifest_sha,
            role=f"{day} part success marker",
        )
        schema_hash = _require_sha256(
            part.get("predicate_schema_sha256"), role="predicate schema hash"
        )
        _validate_part_schema(
            trace_path,
            contract=contract,
            expected_schema_sha256=schema_hash,
        )
        if expected_schema_hash is None:
            expected_schema_hash = schema_hash
        elif schema_hash != expected_schema_hash:
            raise TrainingAdmissionError("predicate schema differs across formal parts")
        frame = pd.read_parquet(trace_path)
        if len(frame) != int(part.get("arm_trace_row_count", -1)):
            raise TrainingAdmissionError(f"{day} part row count drifted")
        if set(frame["utc_day"].astype(str)) != {day}:
            raise TrainingAdmissionError(f"{day} contains another UTC day")
        if frame["opportunity_id"].nunique() != int(part.get("opportunity_count", -1)) or int(
            part.get("predicate_column_count", -1)
        ) != len(contract.predicate_names):
            raise TrainingAdmissionError(f"{day} opportunity or predicate denominator drifted")
        opportunity_projection_sha = _validate_census_opportunity_projection(
            census_path,
            frame,
            day=day,
            contract=contract,
        )
        frames.append(frame)
        bindings.append(
            {
                "utc_day": day,
                "arm_trace_path": str(trace_path),
                "arm_trace_sha256": trace_sha,
                "manifest_path": str(part_manifest_path),
                "manifest_sha256": manifest_sha,
                "success_path": str(success_path),
                "success_sha256": success_sha,
                "census_path": str(census_path),
                "census_sha256": census_sha,
                "census_manifest_path": str(census_binding["manifest_path"]),
                "census_manifest_sha256": str(census_binding["manifest_sha256"]),
                "row_count": int(len(frame)),
                "opportunity_count": int(part["opportunity_count"]),
                "opportunity_projection_sha256": opportunity_projection_sha,
                "predicate_schema_sha256": schema_hash,
            }
        )
    panel = pd.concat(frames, ignore_index=True)
    counts = _validate_joint_panel(panel, contract)
    if (
        int(arm.get("joint_washout_opportunities", -1)) != counts["training_label_opportunities"]
        or int(arm.get("joint_censored_opportunities", -1))
        != counts["joint_censored_opportunities"]
        or int(arm.get("training_label_opportunities", -1))
        != counts["training_label_opportunities"]
        or int(training.get("joint_censored_opportunities", -1))
        != counts["joint_censored_opportunities"]
        or int(training.get("training_label_eligible_opportunities", -1))
        != counts["training_label_opportunities"]
        or training.get("whole_opportunity_censor_exclusion_used")
        is not bool(counts["joint_censored_opportunities"])
    ):
        raise TrainingAdmissionError("manifest and observed joint-censor denominators differ")
    panel = panel.rename(columns={"duration_policy_id": "candidate_policy_id"})
    part_bindings = tuple(bindings)
    return AdmittedPanel(
        frame=panel,
        arm_manifest_path=str(arm_path),
        arm_manifest_sha256=expected_arm_hash,
        training_manifest_path=str(training_path),
        training_manifest_sha256=_file_sha256(training_path),
        census_manifest_path=str(census_manifest),
        census_manifest_sha256=census_manifest_sha,
        execution_identity_sha256=execution_sha,
        opportunity_count=counts["opportunity_count"],
        arm_row_count=len(panel),
        joint_censored_opportunities=counts["joint_censored_opportunities"],
        training_label_opportunities=counts["training_label_opportunities"],
        part_bindings=part_bindings,
        part_bindings_sha256=_canonical_sha256(part_bindings),
    )


def _json_ready(value: Any) -> Any:
    if is_dataclass(value):
        return _json_ready(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    return value


def _write_json(path: Path, value: Any) -> None:
    path.write_bytes(_canonical_bytes(_json_ready(value)) + b"\n")


def _side_summary(oof: pd.DataFrame) -> dict[str, Any]:
    required = {
        "utc_day",
        "campaign_side_id",
        "campaign_weight",
        "chosen_action",
        "policy_minus_control_usdc",
    }
    missing = required - set(oof.columns)
    if missing:
        raise TrainingAdmissionError(f"nested OOF output is missing columns: {sorted(missing)}")
    weights = pd.to_numeric(oof["campaign_weight"], errors="raise").to_numpy(dtype=float)
    uplift = pd.to_numeric(oof["policy_minus_control_usdc"], errors="raise").to_numpy(dtype=float)
    if not np.isfinite(weights).all() or not np.isfinite(uplift).all() or weights.sum() <= 0:
        raise TrainingAdmissionError("nested OOF output contains invalid weights or rewards")
    day_values: list[float] = []
    for _, day in oof.groupby("utc_day", sort=True):
        day_weight = day["campaign_weight"].to_numpy(dtype=float)
        day_uplift = day["policy_minus_control_usdc"].to_numpy(dtype=float)
        day_values.append(float(np.dot(day_weight, day_uplift) / day_weight.sum()))
    return {
        "oof_rows": int(len(oof)),
        "oof_days": int(oof["utc_day"].nunique()),
        "oof_campaigns": int(oof["campaign_side_id"].nunique()),
        "campaign_weighted_uplift_usdc": float(np.dot(weights, uplift) / weights.sum()),
        "mean_daily_campaign_weighted_uplift_usdc": float(np.mean(day_values)),
        "positive_uplift_days": int(np.sum(np.asarray(day_values) > 0.0)),
        "non_control_action_rate": float(
            (~oof["chosen_action"].astype(str).eq("CONTROL_85N")).mean()
        ),
    }


def _validate_nested_result(
    result: Any,
    *,
    side: str,
    side_panel: pd.DataFrame,
    contract: TrainingContract,
) -> None:
    permissions = dict(result.permissions)
    required_permissions = {
        "action_authorized": False,
        "live_authorized": False,
        "f09_registration_authorized": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    if permissions != required_permissions:
        raise TrainingAdmissionError(f"{side} learner permission boundary drifted")
    oof_required = {
        "outer_fold",
        "opportunity_id",
        "side",
        "utc_day",
        "campaign_side_id",
        "campaign_weight",
        "chosen_action",
        "policy_minus_control_usdc",
        "policy_sha256",
    }
    if oof_required - set(result.oof.columns):
        raise TrainingAdmissionError(f"{side} nested OOF schema drifted")
    if result.oof.empty or set(result.oof["side"].astype(str)) != {side}:
        raise TrainingAdmissionError(f"{side} nested OOF is empty or side-pooled")
    if result.oof["opportunity_id"].duplicated().any():
        raise TrainingAdmissionError(f"{side} outer OOF contains duplicate opportunities")
    raw_opportunities = side_panel.drop_duplicates("opportunity_id").copy()
    censored_ids = set(
        raw_opportunities.loc[raw_opportunities["joint_censored"], "opportunity_id"].astype(str)
    )
    eligible = raw_opportunities.loc[~raw_opportunities["joint_censored"]].copy()
    expected_oof_ids: set[str] = set()
    oof = result.oof.copy()
    oof["outer_fold"] = pd.to_numeric(oof["outer_fold"], errors="raise").astype(int)
    for fold, test_days in enumerate(contract.outer_test_days_by_fold):
        expected_rows = eligible.loc[eligible["utc_day"].astype(str).isin(test_days)]
        expected_ids = set(expected_rows["opportunity_id"].astype(str))
        observed_rows = oof.loc[oof["outer_fold"].eq(fold)]
        observed_ids = set(observed_rows["opportunity_id"].astype(str))
        if observed_ids != expected_ids:
            raise TrainingAdmissionError(f"{side} outer fold {fold} opportunity set drifted")
        if set(observed_rows["utc_day"].astype(str)) != set(test_days):
            raise TrainingAdmissionError(f"{side} outer fold {fold} test days drifted")
        expected_oof_ids.update(expected_ids)
    observed_oof_ids = set(oof["opportunity_id"].astype(str))
    if observed_oof_ids != expected_oof_ids or observed_oof_ids & censored_ids:
        raise TrainingAdmissionError(f"{side} outer OOF censor exclusion drifted")
    source_days = raw_opportunities.set_index("opportunity_id")["utc_day"].astype(str)
    observed_days = oof.set_index("opportunity_id")["utc_day"].astype(str)
    if not observed_days.equals(source_days.loc[observed_days.index]):
        raise TrainingAdmissionError(f"{side} outer OOF opportunity/day mapping drifted")

    panel_audit = _json_ready(result.panel_audit)
    if not isinstance(panel_audit, Mapping):
        raise TrainingAdmissionError(f"{side} learner panel audit is invalid")
    if (
        int(panel_audit.get("input_opportunities", -1)) != len(raw_opportunities)
        or int(panel_audit.get("eligible_opportunities", -1)) != len(eligible)
        or int(panel_audit.get("joint_censored_opportunities", -1)) != len(censored_ids)
        or set(str(value) for value in panel_audit.get("excluded_opportunity_ids", ()))
        != censored_ids
    ):
        raise TrainingAdmissionError(f"{side} learner whole-opportunity exclusion drifted")
    chronology = result.chronology_audit
    if chronology.empty:
        raise TrainingAdmissionError(f"{side} chronology audit is empty")
    chronology_required = {
        "outer_fold",
        "train_max_day",
        "test_min_day",
        "future_training_leakage",
        "outer_outcomes_used_for_fit",
    }
    if chronology_required - set(chronology.columns):
        raise TrainingAdmissionError(f"{side} chronology audit schema drifted")
    if chronology["future_training_leakage"].astype(bool).any():
        raise TrainingAdmissionError(f"{side} chronology contains future leakage")
    if chronology["outer_outcomes_used_for_fit"].astype(bool).any():
        raise TrainingAdmissionError(f"{side} outer outcomes entered fitting")
    chronology_outer = pd.to_numeric(chronology["outer_fold"], errors="raise").astype(int)
    if contract.synthetic_test_only:
        for fold, test_days in enumerate(contract.outer_test_days_by_fold):
            fold_rows = chronology.loc[chronology_outer.eq(fold)]
            if (
                fold_rows.empty
                or set(fold_rows["test_min_day"].astype(str)) != {test_days[0]}
                or not (fold_rows["train_max_day"].astype(str) < test_days[0]).all()
            ):
                raise TrainingAdmissionError(f"{side} outer fold {fold} chronology drifted")
    else:
        search_contract = load_frozen_search_contract()
        expected_complexities = {
            (int(max_literals), int(max_clauses))
            for max_literals in search_contract.max_literals_per_clause
            for max_clauses in search_contract.max_clauses
        }
        formal_required = {
            "inner_fold",
            "max_literals_per_clause",
            "max_clauses",
        }
        if formal_required - set(chronology.columns):
            raise TrainingAdmissionError(f"{side} formal chronology audit schema drifted")
        expected_folds = build_nested_chronological_folds(
            side_panel,
            side=side,
            synthetic_mode=False,
        )
        if len(expected_folds) != contract.required_outer_folds:
            raise TrainingAdmissionError(f"{side} formal chronology fold count drifted")
        inner_ids = pd.to_numeric(chronology["inner_fold"], errors="raise").astype(int)
        literal_depths = pd.to_numeric(
            chronology["max_literals_per_clause"], errors="raise"
        ).astype(int)
        clause_depths = pd.to_numeric(chronology["max_clauses"], errors="raise").astype(int)
        for expected_outer in expected_folds:
            fold_rows = chronology.loc[chronology_outer.eq(expected_outer.fold)]
            expected_rows = len(expected_outer.inner_folds) * len(expected_complexities)
            if len(fold_rows) != expected_rows:
                raise TrainingAdmissionError(
                    f"{side} outer fold {expected_outer.fold} chronology row count drifted"
                )
            for expected_inner in expected_outer.inner_folds:
                mask = chronology_outer.eq(expected_outer.fold) & inner_ids.eq(expected_inner.fold)
                inner_rows = chronology.loc[mask]
                observed_complexities = set(
                    zip(literal_depths.loc[mask], clause_depths.loc[mask], strict=True)
                )
                if (
                    len(inner_rows) != len(expected_complexities)
                    or observed_complexities != expected_complexities
                    or set(inner_rows["train_max_day"].astype(str))
                    != {max(expected_inner.train_days)}
                    or set(inner_rows["test_min_day"].astype(str))
                    != {min(expected_inner.test_days)}
                    or not (
                        inner_rows["train_max_day"].astype(str) < min(expected_inner.test_days)
                    ).all()
                ):
                    raise TrainingAdmissionError(
                        f"{side} outer fold {expected_outer.fold} inner fold "
                        f"{expected_inner.fold} chronology drifted"
                    )
    folds = set(oof["outer_fold"])
    if folds != set(range(contract.required_outer_folds)):
        raise TrainingAdmissionError(f"{side} outer-fold denominator drifted")
    evidence = result.complexity_evidence
    if {"outer_fold", "selected"} - set(evidence.columns):
        raise TrainingAdmissionError(f"{side} complexity-evidence schema drifted")
    selected = evidence.loc[evidence["selected"].astype(bool)]
    if selected.groupby("outer_fold").size().to_dict() != {
        fold: 1 for fold in range(contract.required_outer_folds)
    }:
        raise TrainingAdmissionError(f"{side} does not select exactly one complexity per fold")
    if len(result.outer_policy_artifacts) != contract.required_outer_folds:
        raise TrainingAdmissionError(f"{side} policy-artifact denominator drifted")
    expected_predicates = tuple(contract.predicate_names)
    for fold, artifact in enumerate(result.outer_policy_artifacts):
        if not isinstance(artifact, Mapping):
            raise TrainingAdmissionError(f"{side} outer policy artifact is invalid")
        policy_sha = _require_sha256(
            artifact.get("policy_sha256"), role=f"{side} outer policy hash"
        )
        body = {key: value for key, value in artifact.items() if key != "policy_sha256"}
        if _canonical_sha256(body) != policy_sha:
            raise TrainingAdmissionError(f"{side} outer policy self-hash drifted")
        if (
            artifact.get("identity") != IDENTITY
            or artifact.get("side") != side
            or artifact.get("default_action") != "CONTROL_85N"
            or tuple(artifact.get("predicate_columns") or ()) != expected_predicates
            or artifact.get("duration_spec_sha256") != contract.spec_sha256
            or artifact.get("predicate_artifact_sha256") != contract.outcome_blind_sha256
            or tuple(artifact.get("training_fold_identities") or ()) != (f"outer{fold}.full_train",)
            or artifact.get("synthetic_test_only") is not contract.synthetic_test_only
        ):
            raise TrainingAdmissionError(f"{side} outer policy identity drifted")
        if dict(artifact.get("permissions") or {}) != required_permissions:
            raise TrainingAdmissionError(f"{side} outer policy permissions drifted")
        fold_hashes = set(oof.loc[oof["outer_fold"].eq(fold), "policy_sha256"].astype(str))
        if fold_hashes != {policy_sha}:
            raise TrainingAdmissionError(f"{side} outer policy/OOF hash binding drifted")


def _attest_full_panel_identity(
    admitted: AdmittedPanel,
    contract: TrainingContract,
) -> FormalInputIdentity:
    """Create the one full-denominator identity carried into both side fits."""

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
        raise TrainingAdmissionError(
            f"formal full-panel learner attestation failed: {exc}"
        ) from exc


def _side_checkpoint_binding(
    *,
    side: str,
    admitted: AdmittedPanel,
    contract: TrainingContract,
    formal_input_identity: FormalInputIdentity,
    bootstrap_samples: int,
    confidence: float,
) -> dict[str, Any]:
    return {
        "schema_version": SIDE_CHECKPOINT_SCHEMA_VERSION,
        "identity": IDENTITY,
        "side": side,
        "spec_sha256": contract.spec_sha256,
        "outcome_blind_sha256": contract.outcome_blind_sha256,
        "outer_fold_source_sha256": contract.outer_fold_source_sha256,
        "outer_fold_binding_sha256": contract.outer_fold_binding_sha256,
        "execution_identity_sha256": admitted.execution_identity_sha256,
        "arm_trace_manifest_sha256": admitted.arm_manifest_sha256,
        "joint_outcome_training_manifest_sha256": admitted.training_manifest_sha256,
        "census_manifest_sha256": admitted.census_manifest_sha256,
        "part_bindings_sha256": admitted.part_bindings_sha256,
        "formal_input_identity_sha256": formal_input_identity.artifact()[
            "formal_input_identity_sha256"
        ],
        "training_orchestrator_sha256": SIDE_CHECKPOINT_PRODUCER_SHA256,
        "rule_learner_sha256": _file_sha256(Path(rule_learner.__file__)),
        "bootstrap_samples": bootstrap_samples,
        "confidence": confidence,
        "economic_epsilon_usdc": 0.0,
        "development_only": True,
        "authoritative_publication": False,
    }


def _side_checkpoint_path(input_root: Path, binding: Mapping[str, Any]) -> Path:
    binding_sha = _canonical_sha256(dict(binding))
    side = str(binding["side"]).lower()
    return input_root.resolve() / ".training_checkpoints" / f"{side}-{binding_sha[:24]}"


def _write_side_checkpoint(
    checkpoint_dir: Path,
    *,
    binding: Mapping[str, Any],
    result: Any,
) -> None:
    if checkpoint_dir.exists():
        raise TrainingAdmissionError(f"refusing to overwrite side checkpoint: {checkpoint_dir}")
    checkpoint_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(
            prefix=f".{checkpoint_dir.name}.staging.",
            dir=checkpoint_dir.parent,
        )
    )
    try:
        paths = {name: staging / filename for name, filename in SIDE_CHECKPOINT_FILES.items()}
        result.oof.to_parquet(paths["oof"], index=False)
        result.complexity_evidence.to_parquet(paths["complexity_evidence"], index=False)
        result.chronology_audit.to_parquet(paths["chronology_audit"], index=False)
        _write_json(paths["outer_policies"], list(result.outer_policy_artifacts))
        _write_json(paths["panel_audit"], _json_ready(result.panel_audit))
        manifest = {
            "schema_version": SIDE_CHECKPOINT_SCHEMA_VERSION,
            "status": "development_side_checkpoint_non_authoritative",
            "binding": dict(binding),
            "binding_sha256": _canonical_sha256(dict(binding)),
            "artifacts": {
                name: {
                    "path": path.name,
                    "sha256": _file_sha256(path),
                    "rows": (
                        int(len(result.oof))
                        if name == "oof"
                        else int(len(result.complexity_evidence))
                        if name == "complexity_evidence"
                        else int(len(result.chronology_audit))
                        if name == "chronology_audit"
                        else None
                    ),
                }
                for name, path in paths.items()
            },
            "permissions": dict(result.permissions),
            "authority": {
                "development_evidence_only": True,
                "final_training_publication": False,
                "validation_read": False,
                "sealed_holdout_read": False,
                "f09_registration_authorized": False,
                "action_authorized": False,
                "live_authorized": False,
            },
        }
        manifest_path = staging / "checkpoint_manifest.json"
        _write_json(manifest_path, manifest)
        (staging / "_SUCCESS").write_text(f"{_file_sha256(manifest_path)}\n", encoding="ascii")
        os.replace(staging, checkpoint_dir)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def _load_side_checkpoint(
    checkpoint_dir: Path,
    *,
    expected_binding: Mapping[str, Any],
) -> SimpleNamespace | None:
    if not checkpoint_dir.exists():
        return None
    expected_entries = {
        *SIDE_CHECKPOINT_FILES.values(),
        "checkpoint_manifest.json",
        "_SUCCESS",
    }
    if (
        not checkpoint_dir.is_dir()
        or {path.name for path in checkpoint_dir.iterdir()} != expected_entries
    ):
        raise TrainingAdmissionError("side checkpoint file set drifted")
    manifest_path = checkpoint_dir / "checkpoint_manifest.json"
    success_sha = (checkpoint_dir / "_SUCCESS").read_text(encoding="ascii").strip()
    if success_sha != _file_sha256(manifest_path):
        raise TrainingAdmissionError("side checkpoint success marker drifted")
    manifest = _load_json(manifest_path, role="side checkpoint manifest")
    if (
        manifest.get("schema_version") != SIDE_CHECKPOINT_SCHEMA_VERSION
        or manifest.get("status") != "development_side_checkpoint_non_authoritative"
        or manifest.get("binding") != dict(expected_binding)
        or manifest.get("binding_sha256") != _canonical_sha256(dict(expected_binding))
    ):
        raise TrainingAdmissionError("side checkpoint identity binding drifted")
    expected_permissions = {
        "action_authorized": False,
        "live_authorized": False,
        "f09_registration_authorized": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    if dict(manifest.get("permissions") or {}) != expected_permissions:
        raise TrainingAdmissionError("side checkpoint permission boundary drifted")
    artifact_meta = manifest.get("artifacts")
    if not isinstance(artifact_meta, Mapping) or set(artifact_meta) != set(SIDE_CHECKPOINT_FILES):
        raise TrainingAdmissionError("side checkpoint artifact set drifted")
    loaded: dict[str, Any] = {}
    for name, filename in SIDE_CHECKPOINT_FILES.items():
        metadata = artifact_meta.get(name)
        if not isinstance(metadata, Mapping) or metadata.get("path") != filename:
            raise TrainingAdmissionError(f"side checkpoint {name} path drifted")
        path = checkpoint_dir / filename
        if not path.is_file() or path.is_symlink():
            raise TrainingAdmissionError(f"side checkpoint {name} is missing")
        if metadata.get("sha256") != _file_sha256(path):
            raise TrainingAdmissionError(f"side checkpoint {name} hash drifted")
        if path.suffix == ".parquet":
            rows = metadata.get("rows")
            if not isinstance(rows, int) or pq.ParquetFile(path).metadata.num_rows != rows:
                raise TrainingAdmissionError(f"side checkpoint {name} row denominator drifted")
            loaded[name] = pd.read_parquet(path)
        else:
            if metadata.get("rows") is not None:
                raise TrainingAdmissionError(f"side checkpoint {name} rows drifted")
            loaded[name] = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded["outer_policies"], list) or not isinstance(
        loaded["panel_audit"], Mapping
    ):
        raise TrainingAdmissionError("side checkpoint JSON payload drifted")
    return SimpleNamespace(
        oof=loaded["oof"],
        complexity_evidence=loaded["complexity_evidence"],
        chronology_audit=loaded["chronology_audit"],
        outer_policy_artifacts=tuple(loaded["outer_policies"]),
        panel_audit=dict(loaded["panel_audit"]),
        permissions=dict(manifest["permissions"]),
    )


def train_formal_panel(
    input_root: Path,
    output_dir: Path,
    *,
    bootstrap_samples: int = FORMAL_BOOTSTRAP_SAMPLES,
    confidence: float = FORMAL_CONFIDENCE,
) -> dict[str, Any]:
    """Run both side-specific Development learners and atomically publish outputs."""

    if bootstrap_samples != FORMAL_BOOTSTRAP_SAMPLES or confidence != FORMAL_CONFIDENCE:
        raise TrainingAdmissionError("formal bootstrap/confidence settings are frozen")
    contract = _load_training_contract()
    admitted = load_formal_arm_panel(input_root, contract=contract)
    formal_input_identity = _attest_full_panel_identity(admitted, contract)
    formal_input_artifact = formal_input_identity.artifact()
    output_dir = output_dir.resolve()
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    if output_dir.exists():
        raise TrainingAdmissionError(f"refusing to overwrite training output: {output_dir}")
    staging = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.staging.", dir=output_dir.parent))
    used_checkpoint_paths: list[Path] = []
    try:
        results: dict[str, Any] = {}
        for side in SIDES:
            side_panel = admitted.frame.loc[admitted.frame["side"].astype(str).eq(side)].copy()
            if side_panel.empty:
                raise TrainingAdmissionError(f"formal panel contains no {side} opportunities")
            checkpoint_binding = _side_checkpoint_binding(
                side=side,
                admitted=admitted,
                contract=contract,
                formal_input_identity=formal_input_identity,
                bootstrap_samples=bootstrap_samples,
                confidence=confidence,
            )
            checkpoint_path = _side_checkpoint_path(input_root, checkpoint_binding)
            result = _load_side_checkpoint(
                checkpoint_path,
                expected_binding=checkpoint_binding,
            )
            if result is None:
                result = run_nested_chronological_oof(
                    side_panel,
                    side=side,
                    economic_epsilon_usdc=0.0,
                    bootstrap_samples=bootstrap_samples,
                    confidence=confidence,
                    formal_input_identity=formal_input_identity,
                )
                _write_side_checkpoint(
                    checkpoint_path,
                    binding=checkpoint_binding,
                    result=result,
                )
            _validate_nested_result(
                result,
                side=side,
                side_panel=side_panel,
                contract=contract,
            )
            results[side] = result
            used_checkpoint_paths.append(checkpoint_path)

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
            "schema_version": f"{OUTPUT_SCHEMA_VERSION}.outer_policies",
            "identity": IDENTITY,
            "development_only": True,
            "input_contract": {
                "spec_sha256": contract.spec_sha256,
                "outcome_blind_sha256": contract.outcome_blind_sha256,
                "outer_fold_source_sha256": contract.outer_fold_source_sha256,
                "outer_fold_binding_sha256": contract.outer_fold_binding_sha256,
                "execution_identity_sha256": admitted.execution_identity_sha256,
                "part_bindings_sha256": admitted.part_bindings_sha256,
                "formal_input_identity_sha256": formal_input_artifact[
                    "formal_input_identity_sha256"
                ],
            },
            "policies": {side: list(results[side].outer_policy_artifacts) for side in SIDES},
            "permissions": {
                "validation_read": False,
                "sealed_holdout_read": False,
                "f09_registration_authorized": False,
                "action_authorized": False,
                "live_authorized": False,
            },
        }
        report = {
            "schema_version": f"{OUTPUT_SCHEMA_VERSION}.development_report",
            "identity": IDENTITY,
            "status": "development_nested_chronological_oof_complete_no_authority",
            "economic_outcomes_read": True,
            "evidence_role": "historical_native_development_only",
            "input": {
                "spec_path": contract.spec_path,
                "spec_sha256": contract.spec_sha256,
                "outcome_blind_path": contract.outcome_blind_path,
                "outcome_blind_sha256": contract.outcome_blind_sha256,
                "arm_trace_manifest_path": admitted.arm_manifest_path,
                "arm_trace_manifest_sha256": admitted.arm_manifest_sha256,
                "joint_outcome_training_manifest_path": admitted.training_manifest_path,
                "joint_outcome_training_manifest_sha256": admitted.training_manifest_sha256,
                "census_manifest_path": admitted.census_manifest_path,
                "census_manifest_sha256": admitted.census_manifest_sha256,
                "execution_identity_sha256": admitted.execution_identity_sha256,
                "outer_fold_source_path": contract.outer_fold_source_path,
                "outer_fold_source_sha256": contract.outer_fold_source_sha256,
                "outer_fold_field": contract.outer_fold_field,
                "outer_fold_binding_sha256": contract.outer_fold_binding_sha256,
                "predicate_schema_sha256": contract.predicate_schema_sha256,
                "part_bindings_sha256": admitted.part_bindings_sha256,
                "formal_input_identity": formal_input_artifact,
            },
            "denominator": {
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
            },
            "training_settings": {
                "bootstrap_samples": bootstrap_samples,
                "confidence": confidence,
                "economic_epsilon_usdc": 0.0,
                "side_pooling": "forbidden",
            },
            "side_results": {
                side: {
                    **_side_summary(results[side].oof),
                    "panel_audit": _json_ready(results[side].panel_audit),
                    "outer_policy_count": len(results[side].outer_policy_artifacts),
                }
                for side in SIDES
            },
            "limitations": {
                "single_action_label_is_policy_replay": False,
                "policy_level_full_path_replay_complete": False,
                "restart_aware_confirmation_complete": False,
                "validation_or_holdout_can_be_read": False,
            },
            "permissions": {
                "development_evidence_only": True,
                "validation_read": False,
                "sealed_holdout_read": False,
                "f09_registration_authorized": False,
                "action_authorized": False,
                "live_authorized": False,
                "registry_update_authorized": False,
            },
        }

        artifacts = {
            "oof": staging / "oof.parquet",
            "complexity_evidence": staging / "complexity_evidence.parquet",
            "chronology_audit": staging / "chronology_audit.parquet",
            "outer_policies": staging / "outer_policies.json",
            "development_report": staging / "development_report.json",
        }
        oof.to_parquet(artifacts["oof"], index=False)
        evidence.to_parquet(artifacts["complexity_evidence"], index=False)
        chronology.to_parquet(artifacts["chronology_audit"], index=False)
        _write_json(artifacts["outer_policies"], policies)
        _write_json(artifacts["development_report"], report)
        output_manifest = {
            "schema_version": f"{OUTPUT_SCHEMA_VERSION}.artifact_manifest",
            "identity": IDENTITY,
            "status": "atomic_development_training_artifacts_admitted",
            "input_execution_identity_sha256": admitted.execution_identity_sha256,
            "input_bindings": {
                "spec_path": contract.spec_path,
                "spec_sha256": contract.spec_sha256,
                "outcome_blind_path": contract.outcome_blind_path,
                "outcome_blind_sha256": contract.outcome_blind_sha256,
                "outer_fold_source_path": contract.outer_fold_source_path,
                "outer_fold_source_sha256": contract.outer_fold_source_sha256,
                "outer_fold_field": contract.outer_fold_field,
                "outer_fold_binding_sha256": contract.outer_fold_binding_sha256,
                "execution_identity_sha256": admitted.execution_identity_sha256,
                "census_manifest_path": admitted.census_manifest_path,
                "census_manifest_sha256": admitted.census_manifest_sha256,
                "arm_trace_manifest_path": admitted.arm_manifest_path,
                "arm_trace_manifest_sha256": admitted.arm_manifest_sha256,
                "joint_outcome_training_manifest_path": admitted.training_manifest_path,
                "joint_outcome_training_manifest_sha256": admitted.training_manifest_sha256,
                "predicate_schema_sha256": contract.predicate_schema_sha256,
                "part_bindings": list(admitted.part_bindings),
                "part_bindings_sha256": admitted.part_bindings_sha256,
                "formal_input_identity": formal_input_artifact,
            },
            "formal_denominator": {
                "ordered_utc_days": list(contract.ordered_utc_days),
                "opportunity_count": admitted.opportunity_count,
                "arm_row_count": admitted.arm_row_count,
                "predicate_column_count": len(contract.predicate_names),
                "outer_test_days_by_zero_based_fold": [
                    list(values) for values in contract.outer_test_days_by_fold
                ],
            },
            "training_orchestrator_sha256": _file_sha256(Path(__file__)),
            "artifacts": {
                name: {
                    "path": path.name,
                    "sha256": _file_sha256(path),
                    "rows": (
                        int(len(oof))
                        if name == "oof"
                        else int(len(evidence))
                        if name == "complexity_evidence"
                        else int(len(chronology))
                        if name == "chronology_audit"
                        else None
                    ),
                }
                for name, path in artifacts.items()
            },
            "permissions": report["permissions"],
        }
        manifest_path = staging / "training_artifact_manifest.json"
        _write_json(manifest_path, output_manifest)
        (staging / "_SUCCESS").write_text(f"{_file_sha256(manifest_path)}\n", encoding="ascii")
        os.replace(staging, output_dir)
        for checkpoint_path in used_checkpoint_paths:
            shutil.rmtree(checkpoint_path, ignore_errors=True)
        checkpoint_root = input_root.resolve() / ".training_checkpoints"
        try:
            checkpoint_root.rmdir()
        except OSError:
            pass
        return report
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    report = train_formal_panel(args.input_root, args.output_dir)
    print(json.dumps(report, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
