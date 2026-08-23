#!/usr/bin/env python3
"""Bind the complete frozen hierarchy to the immutable BUY E3 owner closeout.

This amendment consumes only the v1 Development closeout and its already
materialized 520 joint OOF daily rows.  It does not replay, fit, select, or read
Validation or sealed holdout evidence.  The v1 closeout and the formal family
remain immutable; this module emits a separately identified v2 amendment.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import stat
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Final

import pandas as pd

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_nested_oof_v1 as nested,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_replay_adapter_v1 as replay_adapter,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_closeout_v1 as v1,
)

IDENTITY: Final = v1.IDENTITY
SCHEMA_VERSION: Final = f"{IDENTITY}.closeout_amendment.v2"
OWNER_DECISION_SCHEMA: Final = f"{IDENTITY}.owner_decision_amendment.v2"
HIERARCHY_SCHEMA: Final = f"{IDENTITY}.complete_joint_hierarchy.v2"

LOCKED_PERMISSIONS: Final = {
    "research_authorized": False,
    "action_authorized": False,
    "live_authorized": False,
    "validation_read": False,
    "sealed_holdout_read": False,
}
LOCKED_REPORT_PERMISSIONS: Final = {
    "final_policy_frozen": False,
    "action_authorized": False,
    "live_authorized": False,
    "validation_read": False,
    "sealed_holdout_read": False,
}
LOCKED_EVIDENCE_BOUNDARY: Final = {
    "panel_role": "Development",
    "learning_algorithm_oof_only": True,
    "exact_final_artifact_oof_available": False,
    "old_oof_estimate_applies_to_exact_owner_artifact": False,
    "validation_read": False,
    "sealed_holdout_read": False,
    "new_economic_arm_run": False,
    "economic_reestimation": False,
    "replay_run": False,
    "training_run": False,
    "shadow_or_companion_created": False,
    "hypothetical_live_actions_scored": False,
}
EXPECTED_V1_IDENTITY: Final = {
    "manifest_file_sha256": "c9a0a20c6f57df85352a46ff8a8549415f2c55b2cc904333a7b9f9a54719b1ae",
    "manifest_canonical_sha256": "0ba45961058da0235f7f8a7dff7e8bd797fe07d2bd254d29ead3808fa6a55e60",
    "joint_report_file_sha256": "b9cdd88feff4fdb002944449e9d64aee5f8dd871989b9cca922d1f4815006653",
    "joint_report_canonical_sha256": "eae295690fea9757748c665dd7c5c5a908338a6fcfdafc72e9e84f1153cb2320",
    "owner_decision_file_sha256": "1005bd26127084a905a09358b3f9c17c80ea095903cafdd3397646a979688640",
    "owner_decision_canonical_sha256": "8e2b86e3d547f76591c13257dced8c49f5756d7a3f9e6c8b2d26fc66a493ad4c",
    "joint_rows_file_sha256": "ec2618419038919f523b9c9fc6b80de642f4aef1794d96649193a86afd430d1f",
    "joint_rows_frame_sha256": "5094e9aa02e59ca451052719a45af0753b954ed28cb701d8d8eb465d8b086515",
}

_POSITIVE_STEPS: Final = (
    (
        "E1_FULL_EMA_BANK",
        (
            "E1-B0",
            "E1-B1",
            "E1-B2",
            "E1-B3",
            "E1_FULL_EMA_BANK-ACTION_MATCHED",
        ),
    ),
    (
        "E2_DIRECTIONAL_EMA",
        ("E2-E1", "E2_DIRECTIONAL_EMA-ACTION_MATCHED"),
    ),
    (
        "E3_HIGHER_ORDER_BOOLEAN",
        ("E3-E2", "E3_HIGHER_ORDER_BOOLEAN-ACTION_MATCHED"),
    ),
    (
        "M2_TRUE_INCREMENTAL",
        ("M2-E3", "M2_TRUE_INCREMENTAL-ACTION_MATCHED"),
    ),
)
_REPRESENTATION_SUFFIXES: Final = tuple(
    f"CONTINUOUS-{candidate}" for candidate in nested.LEARNED_BOOLEAN_ORDER
)
_EXPECTED_SIDES: Final = ("BUY", "SELL")


class OwnerBuyE3CloseoutAmendmentError(RuntimeError):
    """Raised when a v1 binding or complete-hierarchy contract drifts."""


def canonical_sha256(value: Any) -> str:
    return v1.canonical_sha256(value)


def document_sha256(value: Mapping[str, Any], field: str) -> str:
    return v1.document_sha256(value, field)


def file_sha256(path: Path) -> str:
    return v1.file_sha256(path)


def _load_json(path: Path, *, label: str) -> dict[str, Any]:
    try:
        return v1._load_json(path, label=label)
    except v1.OwnerBuyE3CloseoutError as exc:
        raise OwnerBuyE3CloseoutAmendmentError(str(exc)) from exc


def _require_mapping(value: Any, *, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise OwnerBuyE3CloseoutAmendmentError(f"{label} is missing or malformed")
    return value


def _require_false_fields(payload: Mapping[str, Any], fields: Sequence[str], *, label: str) -> None:
    for field in fields:
        if payload.get(field) is not False:
            raise OwnerBuyE3CloseoutAmendmentError(f"{label}.{field} must remain false")


def _validate_file_binding(
    root: Path,
    manifest: Mapping[str, Any],
    filename: str,
) -> Path:
    files = _require_mapping(manifest.get("files"), label="v1 closeout files")
    binding = _require_mapping(files.get(filename), label=f"v1 binding for {filename}")
    path = root / filename
    try:
        observed = path.lstat()
    except FileNotFoundError as exc:
        raise OwnerBuyE3CloseoutAmendmentError(f"v1 file is missing: {filename}") from exc
    if stat.S_ISLNK(observed.st_mode) or not stat.S_ISREG(observed.st_mode):
        raise OwnerBuyE3CloseoutAmendmentError(f"v1 file is not regular: {filename}")
    if (
        binding.get("sha256") != file_sha256(path)
        or int(binding.get("size_bytes", -1)) != observed.st_size
        or binding.get("mode") != "0600"
        or stat.S_IMODE(observed.st_mode) != 0o600
    ):
        raise OwnerBuyE3CloseoutAmendmentError(f"v1 file binding drifted: {filename}")
    return path


def _expected_candidate_keys() -> tuple[str, ...]:
    return tuple(
        f"{side}:{candidate}" for side in _EXPECTED_SIDES for candidate in v1.EXPECTED_CANDIDATES
    )


def _expected_hierarchy_hypotheses() -> tuple[str, ...]:
    return tuple(
        f"successor:{side}:{suffix}"
        for side in _EXPECTED_SIDES
        for suffix in (
            "E1-B0",
            "E2-E1",
            "E3-E2",
            "M2-E3",
            "CONTINUOUS-BOOLEAN",
        )
    )


def _expected_confirmatory_hypotheses() -> tuple[str, ...]:
    return tuple(
        f"successor:{side}:{suffix}"
        for side in _EXPECTED_SIDES
        for suffix, _candidate, _reference in nested.CONFIRMATORY_COMPARISONS
    )


def _expected_risk_hypotheses() -> tuple[str, ...]:
    return tuple(
        f"{candidate}:{metric}"
        for candidate in _expected_candidate_keys()
        for metric in nested.RISK_METRIC_COLUMNS
    )


def _family_bands(
    report: Mapping[str, Any], field: str, expected: Sequence[str]
) -> Mapping[str, Mapping[str, Any]]:
    family = _require_mapping(report.get(field), label=field)
    bands = _require_mapping(family.get("bands"), label=f"{field}.bands")
    if set(bands) != set(expected):
        raise OwnerBuyE3CloseoutAmendmentError(f"{field} hypothesis census drifted")
    output: dict[str, Mapping[str, Any]] = {}
    for hypothesis in expected:
        band = _require_mapping(bands.get(hypothesis), label=f"{field}.{hypothesis}")
        if band.get("hypothesis") != hypothesis:
            raise OwnerBuyE3CloseoutAmendmentError(f"{field} hypothesis identity drifted")
        try:
            values = tuple(
                float(band[name])
                for name in ("mean_usdc", "standard_error_usdc", "lcb_usdc", "ucb_usdc")
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise OwnerBuyE3CloseoutAmendmentError(f"{field} contains a nonnumeric band") from exc
        if not all(math.isfinite(value) for value in values):
            raise OwnerBuyE3CloseoutAmendmentError(f"{field} contains a nonfinite band")
        output[hypothesis] = band
    return output


def validate_v1_closeout(
    v1_closeout_dir: Path,
    *,
    expected_identity: Mapping[str, str] = EXPECTED_V1_IDENTITY,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], pd.DataFrame, dict[str, Path]]:
    """Validate the immutable v1 closeout without reading any other evidence."""

    root = v1_closeout_dir.expanduser().resolve()
    manifest_path = root / "manifest.json"
    manifest = _load_json(manifest_path, label="v1 closeout manifest")
    if file_sha256(manifest_path) != expected_identity.get("manifest_file_sha256") or manifest.get(
        "canonical_manifest_sha256"
    ) != expected_identity.get("manifest_canonical_sha256"):
        raise OwnerBuyE3CloseoutAmendmentError("fixed v1 manifest identity drifted")
    if (
        manifest.get("schema_version") != f"{IDENTITY}.closeout_manifest.v1"
        or manifest.get("identity") != IDENTITY
        or manifest.get("status") != "formal_statistics_rebuilt_owner_override_recorded"
        or manifest.get("canonical_manifest_sha256")
        != document_sha256(manifest, "canonical_manifest_sha256")
        or manifest.get("permissions") != LOCKED_PERMISSIONS
    ):
        raise OwnerBuyE3CloseoutAmendmentError("v1 closeout manifest identity drifted")

    paths = {
        filename: _validate_file_binding(root, manifest, filename)
        for filename in (
            "joint_oof_report.json",
            "owner_decision.json",
            "joint_outer_oof_rows.parquet",
        )
    }
    report = _load_json(paths["joint_oof_report.json"], label="v1 joint OOF report")
    owner = _load_json(paths["owner_decision.json"], label="v1 owner decision")
    if (
        file_sha256(paths["joint_oof_report.json"])
        != expected_identity.get("joint_report_file_sha256")
        or canonical_sha256(report) != expected_identity.get("joint_report_canonical_sha256")
        or file_sha256(paths["owner_decision.json"])
        != expected_identity.get("owner_decision_file_sha256")
        or owner.get("canonical_owner_decision_sha256")
        != expected_identity.get("owner_decision_canonical_sha256")
        or file_sha256(paths["joint_outer_oof_rows.parquet"])
        != expected_identity.get("joint_rows_file_sha256")
    ):
        raise OwnerBuyE3CloseoutAmendmentError("fixed v1 artifact identity drifted")
    if (
        owner.get("schema_version") != f"{IDENTITY}.owner_decision.v1"
        or owner.get("identity") != IDENTITY
        or owner.get("status") != "owner_override_recorded_artifact_not_yet_frozen"
        or owner.get("canonical_owner_decision_sha256")
        != document_sha256(owner, "canonical_owner_decision_sha256")
        or owner.get("research_supported") is not False
        or owner.get("owner_risk_accepted") is not True
        or owner.get("outcome_informed_owner_override") is not True
        or owner.get("formal_closeout_mutated") is not False
        or owner.get("formal_hierarchy_passed") is not False
        or owner.get("formal_hard_gates_passed") is not False
        or owner.get("permissions") != LOCKED_PERMISSIONS
    ):
        raise OwnerBuyE3CloseoutAmendmentError("v1 owner decision identity drifted")
    owner_boundary = _require_mapping(
        owner.get("evidence_boundary"), label="v1 owner evidence boundary"
    )
    _require_false_fields(
        owner_boundary,
        (
            "exact_final_artifact_oof_available",
            "old_oof_estimate_applies_to_exact_owner_artifact",
            "validation_read",
            "sealed_holdout_read",
            "new_economic_arm_run",
        ),
        label="v1 owner evidence boundary",
    )

    if (
        report.get("schema_version") != f"{IDENTITY}.joint_oof_statistics.v1"
        or report.get("oof_evidence_scope") != nested.OOF_EVIDENCE_SCOPE
        or report.get("exact_final_artifact_oof_available") is not False
        or report.get("final_refit_performed") is not False
        or int(report.get("outer_oof_row_count", -1)) != 520
        or report.get("outer_fold_count_by_side") != {"BUY": 4, "SELL": 4}
        or report.get("simultaneous_family_sides") != ["BUY", "SELL"]
        or report.get("permissions") != LOCKED_REPORT_PERMISSIONS
    ):
        raise OwnerBuyE3CloseoutAmendmentError("v1 joint OOF report identity drifted")

    candidate_keys = _expected_candidate_keys()
    for field in ("candidate_reports", "stability", "scorecards"):
        values = _require_mapping(report.get(field), label=field)
        if set(values) != set(candidate_keys):
            raise OwnerBuyE3CloseoutAmendmentError(f"{field} candidate census drifted")
    for key, scorecard in report["scorecards"].items():
        profile = _require_mapping(scorecard, label=f"scorecard {key}").get("profile")
        if not isinstance(profile, Mapping) or profile.get("profile_id") != "action_alpha_v1":
            raise OwnerBuyE3CloseoutAmendmentError("action_alpha_v1 scorecard census drifted")

    _family_bands(report, "candidate_bands", candidate_keys)
    _family_bands(report, "candidate_week_bands", candidate_keys)
    _family_bands(report, "hierarchy_bands", _expected_hierarchy_hypotheses())
    _family_bands(report, "hierarchy_week_bands", _expected_hierarchy_hypotheses())
    _family_bands(report, "confirmatory_bands", _expected_confirmatory_hypotheses())
    _family_bands(report, "confirmatory_week_bands", _expected_confirmatory_hypotheses())
    _family_bands(report, "risk_bands", _expected_risk_hypotheses())
    _family_bands(report, "risk_week_bands", _expected_risk_hypotheses())

    rows = pd.read_parquet(paths["joint_outer_oof_rows.parquet"])
    required_columns = {"side", "utc_day", "candidate_name", "point_identified"}
    if required_columns - set(rows) or len(rows) != 520:
        raise OwnerBuyE3CloseoutAmendmentError("joint OOF row schema or census drifted")
    if rows.duplicated(["side", "utc_day", "candidate_name"]).any():
        raise OwnerBuyE3CloseoutAmendmentError("joint OOF rows contain duplicate slots")
    observed_sides = tuple(sorted(set(rows["side"].astype(str))))
    if observed_sides != _EXPECTED_SIDES:
        raise OwnerBuyE3CloseoutAmendmentError("joint OOF side census drifted")
    side_days: dict[str, set[str]] = {}
    for side in _EXPECTED_SIDES:
        side_rows = rows.loc[rows["side"].astype(str) == side]
        days = set(side_rows["utc_day"].astype(str))
        side_days[side] = days
        if (
            len(side_rows) != 260
            or len(days) != 20
            or set(side_rows["candidate_name"].astype(str)) != set(v1.EXPECTED_CANDIDATES)
            or any(
                len(side_rows.loc[side_rows["candidate_name"].astype(str) == candidate]) != 20
                for candidate in v1.EXPECTED_CANDIDATES
            )
        ):
            raise OwnerBuyE3CloseoutAmendmentError(f"{side} joint OOF census drifted")
    if side_days["BUY"] != side_days["SELL"]:
        raise OwnerBuyE3CloseoutAmendmentError("BUY/SELL joint OOF days differ")

    row_frames = _require_mapping(manifest.get("row_frames"), label="v1 row frames")
    joint_frame = _require_mapping(row_frames.get("joint"), label="v1 joint row frame")
    if (
        int(joint_frame.get("rows", -1)) != len(rows)
        or joint_frame.get("frame_sha256") != replay_adapter._frame_sha256(rows)
        or joint_frame.get("frame_sha256") != expected_identity.get("joint_rows_frame_sha256")
    ):
        raise OwnerBuyE3CloseoutAmendmentError("joint OOF frame binding drifted")
    return manifest, report, owner, rows, paths


def _contrast_receipt(
    *,
    hypothesis: str,
    day_bands: Mapping[str, Mapping[str, Any]],
    week_bands: Mapping[str, Mapping[str, Any]],
    epsilon: float,
    positive_required: bool,
) -> dict[str, Any]:
    day = dict(day_bands[hypothesis])
    week = dict(week_bands[hypothesis])
    if positive_required:
        passed = float(day["lcb_usdc"]) > epsilon and float(week["lcb_usdc"]) > epsilon
        rule = "day_and_week_lcb_above_economic_epsilon"
    else:
        passed = float(day["lcb_usdc"]) <= epsilon and float(week["lcb_usdc"]) <= epsilon
        rule = "continuous_not_proven_superior_on_day_or_week_family"
    return {
        "hypothesis": hypothesis,
        "day_band": day,
        "week_band": week,
        "band_condition_passed": passed,
        "rule": rule,
    }


def build_complete_joint_hierarchy(
    joint_report: Mapping[str, Any], *, epsilon: float = 0.0
) -> dict[str, Any]:
    """Adjudicate the complete frozen hierarchy from one joint max-t family."""

    if not math.isfinite(epsilon):
        raise OwnerBuyE3CloseoutAmendmentError("economic epsilon must be finite")
    expected = _expected_confirmatory_hypotheses()
    day_bands = _family_bands(joint_report, "confirmatory_bands", expected)
    week_bands = _family_bands(joint_report, "confirmatory_week_bands", expected)
    steps: dict[str, list[dict[str, Any]]] = {}
    supported_sides: list[str] = []
    for side in _EXPECTED_SIDES:
        parent_passed = True
        side_steps: list[dict[str, Any]] = []
        for step_name, suffixes in _POSITIVE_STEPS:
            contrasts = [
                _contrast_receipt(
                    hypothesis=f"successor:{side}:{suffix}",
                    day_bands=day_bands,
                    week_bands=week_bands,
                    epsilon=epsilon,
                    positive_required=True,
                )
                for suffix in suffixes
            ]
            band_conditions_passed = all(item["band_condition_passed"] for item in contrasts)
            passed = parent_passed and band_conditions_passed
            if not parent_passed:
                reason = "parent_feature_block_not_supported"
            elif passed:
                reason = "all_required_positive_contrasts_passed"
            else:
                reason = "required_positive_contrast_failed"
            side_steps.append(
                {
                    "step": step_name,
                    "kind": "positive_value",
                    "parent_passed": parent_passed,
                    "tested": parent_passed,
                    "passed": passed,
                    "reason": reason,
                    "required_contrasts": contrasts,
                }
            )
            parent_passed = passed

        representation_contrasts = [
            _contrast_receipt(
                hypothesis=f"successor:{side}:{suffix}",
                day_bands=day_bands,
                week_bands=week_bands,
                epsilon=epsilon,
                positive_required=False,
            )
            for suffix in _REPRESENTATION_SUFFIXES
        ]
        representation_conditions_passed = all(
            item["band_condition_passed"] for item in representation_contrasts
        )
        representation_passed = parent_passed and representation_conditions_passed
        if not parent_passed:
            representation_reason = "parent_feature_block_not_supported"
        elif representation_passed:
            representation_reason = "continuous_not_proven_superior"
        else:
            representation_reason = "continuous_representation_proven_superior"
        side_steps.append(
            {
                "step": "CONTINUOUS_DOMINANCE_BLOCKER",
                "kind": "representation_dominance_blocker",
                "parent_passed": parent_passed,
                "tested": parent_passed,
                "passed": representation_passed,
                "reason": representation_reason,
                "required_contrasts": representation_contrasts,
            }
        )
        steps[side] = side_steps
        if all(step["passed"] for step in side_steps):
            supported_sides.append(side)

    hierarchy = {
        "schema_version": HIERARCHY_SCHEMA,
        "identity": IDENTITY,
        "status": "complete_joint_hierarchy_adjudicated",
        "simultaneous_family": {
            "source_day_field": "confirmatory_bands",
            "source_week_field": "confirmatory_week_bands",
            "joint_side_family": True,
            "sides": list(_EXPECTED_SIDES),
            "hypothesis_count": len(expected),
            "day_family_canonical_sha256": canonical_sha256(joint_report["confirmatory_bands"]),
            "week_family_canonical_sha256": canonical_sha256(
                joint_report["confirmatory_week_bands"]
            ),
        },
        "economic_epsilon_usdc": epsilon,
        "steps": steps,
        "supported_sides": supported_sides,
        "contract_interpretation": {
            "simplified_baselines_are_gates": True,
            "action_matched_controls_are_gates": True,
            "continuous_minus_each_boolean_is_a_dominance_blocker": True,
        },
    }
    return hierarchy


def _input_binding(
    path: Path,
    payload: Mapping[str, Any] | None = None,
    *,
    canonical_field: str | None = None,
) -> dict[str, Any]:
    binding: dict[str, Any] = {
        "filename": path.name,
        "file_sha256": file_sha256(path),
        "size_bytes": path.stat().st_size,
        "mode": format(stat.S_IMODE(path.stat().st_mode), "04o"),
    }
    if payload is not None:
        binding["schema_version"] = payload.get("schema_version")
        binding["canonical_sha256"] = (
            document_sha256(payload, canonical_field)
            if canonical_field is not None
            else canonical_sha256(payload)
        )
    return binding


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            payload,
            sort_keys=True,
            indent=2,
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
        + b"\n"
    )


def _write_private_json(path: Path, payload: Mapping[str, Any]) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(_json_bytes(payload))
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def run_amendment(
    *,
    v1_closeout_dir: Path,
    output_dir: Path,
    expected_v1_identity: Mapping[str, str] = EXPECTED_V1_IDENTITY,
) -> dict[str, Any]:
    """Create a separate immutable amendment from existing v1 bytes only."""

    manifest_v1, report, owner_v1, rows, paths = validate_v1_closeout(
        v1_closeout_dir,
        expected_identity=expected_v1_identity,
    )
    hierarchy = build_complete_joint_hierarchy(report)
    hierarchy_sha = canonical_sha256(hierarchy)
    destination = output_dir.expanduser().resolve()
    if destination.exists():
        raise OwnerBuyE3CloseoutAmendmentError("immutable amendment output already exists")
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{destination.name}.", dir=destination.parent))
    try:
        hierarchy_path = staging / "complete_joint_hierarchy.json"
        _write_private_json(hierarchy_path, hierarchy)
        owner_amendment: dict[str, Any] = {
            "schema_version": OWNER_DECISION_SCHEMA,
            "identity": IDENTITY,
            "status": "owner_override_preserved_complete_joint_hierarchy_bound",
            "amends_owner_decision_v1": _input_binding(
                paths["owner_decision.json"],
                owner_v1,
                canonical_field="canonical_owner_decision_sha256",
            ),
            "v1_closeout_manifest_binding": _input_binding(
                v1_closeout_dir.expanduser().resolve() / "manifest.json",
                manifest_v1,
                canonical_field="canonical_manifest_sha256",
            ),
            "joint_oof_report_binding": _input_binding(paths["joint_oof_report.json"], report),
            "joint_outer_oof_rows_binding": {
                **_input_binding(paths["joint_outer_oof_rows.parquet"]),
                "rows": len(rows),
                "frame_sha256": replay_adapter._frame_sha256(rows),
            },
            "complete_joint_hierarchy_binding": _input_binding(hierarchy_path, hierarchy),
            "complete_joint_hierarchy_sha256": hierarchy_sha,
            "complete_joint_hierarchy_supported_sides": hierarchy["supported_sides"],
            "research_supported": False,
            "owner_risk_accepted": True,
            "outcome_informed_owner_override": True,
            "formal_closeout_mutated": False,
            "formal_hierarchy_passed": False,
            "formal_hard_gates_passed": False,
            "formal_hard_gate_failures": list(owner_v1.get("formal_hard_gate_failures", ())),
            "evidence_boundary": dict(LOCKED_EVIDENCE_BOUNDARY),
            "permissions": dict(LOCKED_PERMISSIONS),
        }
        owner_amendment["canonical_owner_decision_amendment_sha256"] = document_sha256(
            owner_amendment, "canonical_owner_decision_amendment_sha256"
        )
        owner_path = staging / "owner_decision_amendment_v2.json"
        _write_private_json(owner_path, owner_amendment)

        output_files = {
            hierarchy_path.name: _input_binding(hierarchy_path, hierarchy),
            owner_path.name: _input_binding(
                owner_path,
                owner_amendment,
                canonical_field="canonical_owner_decision_amendment_sha256",
            ),
        }
        amendment_manifest: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "identity": IDENTITY,
            "status": "complete_joint_hierarchy_amendment_frozen",
            "amends_without_mutation": {
                "v1_closeout_manifest_file_sha256": file_sha256(
                    v1_closeout_dir.expanduser().resolve() / "manifest.json"
                ),
                "v1_closeout_manifest_canonical_sha256": manifest_v1["canonical_manifest_sha256"],
                "formal_closeout_mutated": False,
            },
            "fixed_v1_identity": dict(expected_v1_identity),
            "files": output_files,
            "joint_oof_report_binding": owner_amendment["joint_oof_report_binding"],
            "complete_joint_hierarchy_sha256": hierarchy_sha,
            "evidence_boundary": dict(LOCKED_EVIDENCE_BOUNDARY),
            "permissions": dict(LOCKED_PERMISSIONS),
        }
        amendment_manifest["canonical_closeout_amendment_sha256"] = document_sha256(
            amendment_manifest, "canonical_closeout_amendment_sha256"
        )
        _write_private_json(staging / "manifest.json", amendment_manifest)
        os.replace(staging, destination)
        return amendment_manifest
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--v1-closeout-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    manifest = run_amendment(
        v1_closeout_dir=args.v1_closeout_dir,
        output_dir=args.output_dir,
    )
    print(json.dumps(manifest, sort_keys=True, indent=2, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
