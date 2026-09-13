#!/usr/bin/env python3
"""Freeze and enforce family-specific evidence splits on existing good days."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "narrowgate_evidence_split.v1"
ACCESS_DECISION_SCHEMA_VERSION = "narrowgate_panel_access_decision.v1"
PREDICTION_ACCESS_DECISION_SCHEMA_VERSION = (
    "dynamic_fill_hazard_validation_admission.v1"
)
PANEL_ORDER = (
    "development",
    "embargo_1",
    "validation",
    "embargo_2",
    "sealed_holdout",
)
DEFAULT_ACTION_PROBABILITIES = {
    "baseline": 0.40,
    "prevent_over_widen": 0.20,
    "widen_1tick": 0.20,
    "recenter_1tick": 0.20,
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(payload: Any) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _normalize_days(values: Any, *, panel: str) -> list[str]:
    if not isinstance(values, list):
        raise ValueError(f"{panel} days must be a list")
    days = [str(value).strip()[:10] for value in values if str(value).strip()]
    if len(days) != len(set(days)):
        raise ValueError(f"{panel} contains duplicate days")
    if days != sorted(days):
        raise ValueError(f"{panel} days must be chronological")
    return days


def validate_evidence_split(payload: dict[str, Any]) -> dict[str, Any]:
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(f"unsupported evidence split schema: {payload.get('schema_version')}")
    if not str(payload.get("family_id", "")).strip():
        raise ValueError("evidence split requires family_id")
    panels = payload.get("panels")
    if not isinstance(panels, dict):
        raise ValueError("evidence split requires a panels object")

    normalized: dict[str, list[str]] = {}
    seen: set[str] = set()
    previous_last = ""
    for name in PANEL_ORDER:
        panel = panels.get(name)
        if not isinstance(panel, dict):
            raise ValueError(f"evidence split is missing panel {name}")
        days = _normalize_days(panel.get("days"), panel=name)
        if not days:
            raise ValueError(f"evidence panel {name} must not be empty")
        overlap = seen.intersection(days)
        if overlap:
            raise ValueError(f"evidence panels overlap on {sorted(overlap)}")
        if previous_last and days[0] <= previous_last:
            raise ValueError(f"evidence panel {name} is not after the preceding panel")
        if bool(panel.get("sealed", False)) != (name == "sealed_holdout"):
            raise ValueError("only sealed_holdout may be sealed")
        normalized[name] = days
        seen.update(days)
        previous_last = days[-1]

    action = payload.get("action_family")
    if not isinstance(action, dict):
        raise ValueError("evidence split requires action_family")
    probabilities = action.get("behavior_probabilities")
    if not isinstance(probabilities, dict) or not probabilities:
        raise ValueError("action family requires behavior_probabilities")
    probability_sum = sum(float(value) for value in probabilities.values())
    if abs(probability_sum - 1.0) > 1e-10:
        raise ValueError("behavior probabilities must sum to one")
    if any(float(value) <= 0.0 for value in probabilities.values()):
        raise ValueError("every registered action needs positive overlap")
    if set(action.get("actions") or ()) != set(probabilities):
        raise ValueError("action registry and behavior-probability vector differ")
    if payload.get("action_family_sha256") != _canonical_sha256(action):
        raise ValueError("action_family_sha256 does not match action_family")

    source = Path(str(payload.get("source_manifest_path", ""))).expanduser()
    expected_source_hash = str(payload.get("source_manifest_sha256", ""))
    if not source.is_file() or sha256_file(source.resolve()) != expected_source_hash:
        raise ValueError("source feature manifest is missing or its hash has changed")
    return {name: list(days) for name, days in normalized.items()}


def build_from_feature_manifest(
    source_manifest_path: Path,
    *,
    family_id: str,
    behavior_probabilities: dict[str, float],
    inventory_role: str = "add",
) -> dict[str, Any]:
    source = source_manifest_path.expanduser().resolve()
    source_payload = json.loads(source.read_text(encoding="utf-8"))
    source_split = source_payload.get("split")
    if not isinstance(source_split, dict):
        raise ValueError("source feature manifest has no split object")
    source_keys = {
        "development": "train",
        "embargo_1": "embargo_1",
        "validation": "validation",
        "embargo_2": "embargo_2",
        "sealed_holdout": "test",
    }
    action_family = {
        "actions": list(behavior_probabilities),
        "behavior_probabilities": {
            str(key): float(value) for key, value in behavior_probabilities.items()
        },
        "eligibility": (
            "first baseline-eligible exposure-increasing add quote per campaign"
        ),
        "inventory_role": str(inventory_role),
        "one_intervention_per_campaign": True,
        "sides": ["BUY", "SELL"],
        "size_modified": False,
        "reducing_side_modified": False,
        "inventory_limit_modified": False,
    }
    panels = {}
    for target, source_key in source_keys.items():
        panels[target] = {
            "days": list(source_split.get(source_key) or ()),
            "sealed": target == "sealed_holdout",
            "role": {
                "development": "fit nuisance/action models and develop hypotheses",
                "validation": "select and freeze side-specific candidates once",
                "sealed_holdout": "one-shot family-specific confirmatory evidence",
            }.get(target, "time embargo; excluded from fitting and evaluation"),
        }
    payload = {
        "schema_version": SCHEMA_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "family_id": str(family_id),
        "split_mode": "chronological_existing_good_days",
        "evidence_scope": (
            "family-specific; dates may have been inspected by unrelated hypotheses"
        ),
        "source_manifest_path": str(source),
        "source_manifest_sha256": sha256_file(source),
        "source_daily_manifest_sha256": str(
            source_payload.get("daily_manifest_sha256", "")
        ),
        "panels": panels,
        "action_family": action_family,
        "action_family_sha256": _canonical_sha256(action_family),
        "holdout_rule": (
            "sealed_holdout requires explicit unseal; changing the family after access "
            "starts a new family and requires new evidence"
        ),
    }
    validate_evidence_split(payload)
    return payload


def build_explicit_evidence_split(
    source_manifest_path: Path,
    *,
    family_id: str,
    panels: dict[str, list[str]],
    behavior_probabilities: dict[str, float],
    sides: list[str],
    inventory_role: str = "add",
    eligibility: str = (
        "first baseline-eligible exposure-increasing add quote per campaign"
    ),
) -> dict[str, Any]:
    """Freeze a new family split without inheriting an older hypothesis split."""

    source = source_manifest_path.expanduser().resolve()
    source_payload = json.loads(source.read_text(encoding="utf-8"))
    source_split = source_payload.get("split")
    if not isinstance(source_split, dict):
        raise ValueError("source feature manifest has no split object")
    universe = {
        str(day)
        for values in source_split.values()
        if isinstance(values, list)
        for day in values
    }
    requested = {
        str(day)
        for values in panels.values()
        for day in values
    }
    outside = sorted(requested - universe)
    if outside:
        raise ValueError(f"explicit evidence split contains unknown days: {outside}")

    normalized_sides = sorted({str(side).strip().upper() for side in sides})
    if not normalized_sides or set(normalized_sides) - {"BUY", "SELL"}:
        raise ValueError("action-family sides must be a non-empty BUY/SELL subset")
    action_family = {
        "actions": list(behavior_probabilities),
        "behavior_probabilities": {
            str(key): float(value) for key, value in behavior_probabilities.items()
        },
        "eligibility": str(eligibility),
        "inventory_role": str(inventory_role),
        "one_intervention_per_campaign": True,
        "sides": normalized_sides,
        "size_modified": False,
        "reducing_side_modified": False,
        "inventory_limit_modified": False,
    }
    panel_payload = {
        name: {
            "days": list(panels.get(name) or ()),
            "sealed": name == "sealed_holdout",
            "role": {
                "development": "development-only chronological OOF learning and gate",
                "validation": "one-shot confirmation after development passes",
                "sealed_holdout": "one-shot confirmation after validation passes",
            }.get(name, "time embargo; excluded from fitting and evaluation"),
        }
        for name in PANEL_ORDER
    }
    payload = {
        "schema_version": SCHEMA_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "family_id": str(family_id),
        "split_mode": "explicit_chronological_existing_good_days",
        "evidence_scope": (
            "family-specific; dates may have been inspected by unrelated hypotheses"
        ),
        "source_manifest_path": str(source),
        "source_manifest_sha256": sha256_file(source),
        "source_daily_manifest_sha256": str(
            source_payload.get("daily_manifest_sha256", "")
        ),
        "panels": panel_payload,
        "action_family": action_family,
        "action_family_sha256": _canonical_sha256(action_family),
        "holdout_rule": (
            "validation stays inaccessible until the development lower-bound "
            "gate passes; sealed_holdout additionally requires explicit unseal"
        ),
    }
    validate_evidence_split(payload)
    return payload


def load_evidence_panel(
    manifest_path: Path,
    panel: str,
    *,
    allow_sealed_holdout: bool = False,
    access_decision_path: Path | None = None,
    queue_model_bundle_path: Path | None = None,
) -> tuple[list[str], dict[str, Any]]:
    path = manifest_path.expanduser().resolve()
    payload = json.loads(path.read_text(encoding="utf-8"))
    panels = validate_evidence_split(payload)
    if panel not in panels:
        raise ValueError(f"unknown evidence panel: {panel}")
    if panel.startswith("embargo"):
        raise PermissionError("embargo days cannot be replayed as evidence")
    if panel in {"validation", "sealed_holdout"}:
        if access_decision_path is None:
            raise PermissionError(
                f"{panel} is locked; provide a hash-bound panel access decision"
            )
        validate_panel_access_decision(
            access_decision_path,
            evidence_split_path=path,
            target_panel=panel,
            queue_model_bundle_path=queue_model_bundle_path,
        )
    if panel == "sealed_holdout" and not allow_sealed_holdout:
        raise PermissionError(
            "sealed_holdout is locked; record the development/validation decision "
            "and pass --allow-sealed-holdout for the one-shot read"
        )
    identity = {
        "manifest_path": str(path),
        "manifest_sha256": sha256_file(path),
        "family_id": str(payload["family_id"]),
        "action_family_sha256": str(payload["action_family_sha256"]),
        "behavior_probabilities": dict(
            payload["action_family"]["behavior_probabilities"]
        ),
        "actions": list(payload["action_family"]["actions"]),
        "sides": list(payload["action_family"]["sides"]),
        "action_family": dict(payload["action_family"]),
        "panel": panel,
        "sealed_access": panel == "sealed_holdout",
        "access_decision_path": (
            str(access_decision_path.expanduser().resolve())
            if access_decision_path is not None
            else ""
        ),
        "access_decision_sha256": (
            sha256_file(access_decision_path.expanduser().resolve())
            if access_decision_path is not None
            else ""
        ),
    }
    return panels[panel], identity


def _require_file_identity(
    payload: dict[str, Any],
    *,
    path_field: str,
    hash_field: str,
) -> Path:
    path = Path(str(payload.get(path_field, ""))).expanduser().resolve()
    expected = str(payload.get(hash_field, ""))
    if not path.is_file() or not expected:
        raise ValueError(f"panel access decision is missing {path_field}")
    if sha256_file(path) != expected:
        raise ValueError(f"panel access decision hash mismatch for {path_field}")
    return path


def validate_panel_access_decision(
    decision_path: Path,
    *,
    evidence_split_path: Path,
    target_panel: str,
    queue_model_bundle_path: Path | None = None,
) -> dict[str, Any]:
    """Validate a one-way, hash-bound Development -> Validation -> Holdout gate."""

    decision_file = decision_path.expanduser().resolve()
    payload = json.loads(decision_file.read_text(encoding="utf-8"))
    if payload.get("schema_version") == PREDICTION_ACCESS_DECISION_SCHEMA_VERSION:
        return _validate_prediction_panel_access_decision(
            payload,
            evidence_split_path=evidence_split_path,
            target_panel=target_panel,
        )
    if payload.get("schema_version") != ACCESS_DECISION_SCHEMA_VERSION:
        raise ValueError("unsupported panel access decision schema")
    if payload.get("decision") != "open" or payload.get("gate_passed") is not True:
        raise PermissionError("panel access decision did not pass")
    target = str(target_panel)
    expected_prior = {
        "validation": "development",
        "sealed_holdout": "validation",
    }.get(target)
    if expected_prior is None:
        raise ValueError(f"panel access decisions do not apply to {target}")
    if str(payload.get("target_panel", "")) != target:
        raise ValueError("panel access decision targets a different panel")
    if str(payload.get("prior_panel", "")) != expected_prior:
        raise ValueError("panel access decision has the wrong prior panel")

    evidence_path = evidence_split_path.expanduser().resolve()
    evidence_payload = json.loads(evidence_path.read_text(encoding="utf-8"))
    validate_evidence_split(evidence_payload)
    if str(payload.get("family_id", "")) != str(evidence_payload["family_id"]):
        raise ValueError("panel access decision family does not match")
    if (
        str(payload.get("evidence_split_sha256", ""))
        != sha256_file(evidence_path)
    ):
        raise ValueError("panel access decision evidence split hash mismatch")

    metadata_path = _require_file_identity(
        payload,
        path_field="prior_metadata_path",
        hash_field="prior_metadata_sha256",
    )
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    if str(metadata.get("panel_role", "")) != expected_prior:
        raise ValueError("prior metadata panel role does not match")
    evidence_identity = metadata.get("evidence_split") or {}
    if (
        str(evidence_identity.get("manifest_sha256", ""))
        != sha256_file(evidence_path)
    ):
        raise ValueError("prior metadata evidence split identity does not match")
    if str(metadata.get("ope_block_reason", "")):
        raise PermissionError("prior panel OPE was blocked")
    if (metadata.get("native_source_integrity") or {}).get("passed") is not True:
        raise PermissionError("prior panel native source integrity did not pass")
    support = metadata.get("native_action_support") or {}
    if (
        int(support.get("rows", -1))
        != int(support.get("outcome_supported_rows", -2))
        or int(support.get("ambiguous_rows", -1)) != 0
        or int(support.get("invalid_path_rows", -1)) != 0
    ):
        raise PermissionError("prior panel contains unsupported native outcomes")

    bundle_path = _require_file_identity(
        payload,
        path_field="queue_model_bundle_path",
        hash_field="queue_model_bundle_sha256",
    )
    if queue_model_bundle_path is not None:
        expected_bundle = queue_model_bundle_path.expanduser().resolve()
        if bundle_path != expected_bundle:
            raise ValueError("panel access decision binds a different queue bundle")
    if (
        str(metadata.get("queue_model_bundle_sha256", ""))
        != sha256_file(bundle_path)
    ):
        raise ValueError("prior metadata queue bundle identity does not match")

    summaries = payload.get("prior_ope_summaries")
    if not isinstance(summaries, list) or not summaries:
        raise ValueError("panel access decision requires prior OPE summaries")
    for row in summaries:
        if not isinstance(row, dict):
            raise ValueError("invalid prior OPE summary identity")
        summary_path = _require_file_identity(
            row,
            path_field="path",
            hash_field="sha256",
        )
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        if summary.get("numerical_ope_gate_passed") is not True:
            raise PermissionError("prior OPE numerical gate did not pass")
        lower = float(
            (summary.get("day_cluster_bootstrap") or {}).get(
                "uplift_p025",
                float("-inf"),
            )
        )
        if lower <= 0.0:
            raise PermissionError(
                "prior OPE uplift lower bound is not strictly positive"
            )
    return payload


def _validate_prediction_panel_access_decision(
    payload: dict[str, Any],
    *,
    evidence_split_path: Path,
    target_panel: str,
) -> dict[str, Any]:
    """Validate prediction-only Development -> Validation admission.

    This deliberately cannot open sealed holdout or authorize an action. It
    exists for a hash-bound, researcher-recorded probability screen whose
    original strict Development result remains preserved.
    """

    if str(target_panel) != "validation":
        raise PermissionError(
            "prediction admission can open Validation only"
        )
    required_scalars = {
        "decision": "admit_buy_to_validation",
        "decision_scope": "prediction_validation_only",
        "prior_panel": "development",
        "target_panel": "validation",
        "validation_status": "admitted_not_yet_read",
    }
    for field, expected in required_scalars.items():
        if str(payload.get(field, "")) != expected:
            raise PermissionError(
                f"prediction admission has invalid {field}"
            )
    if payload.get("gate_passed") is not True:
        raise PermissionError("prediction admission gate did not pass")
    if payload.get("live_change_allowed") is not False:
        raise PermissionError("prediction admission cannot authorize live")
    if payload.get("sealed_holdout_access_allowed") is not False:
        raise PermissionError(
            "prediction admission cannot open sealed holdout"
        )
    if payload.get("sell_validation_access_allowed") is not False:
        raise PermissionError("BUY-only admission cannot open SELL")
    if payload.get("admitted_sides") != ["BUY"]:
        raise PermissionError("prediction admission must be BUY-only")

    evidence_path = evidence_split_path.expanduser().resolve()
    evidence_payload = json.loads(evidence_path.read_text(encoding="utf-8"))
    validate_evidence_split(evidence_payload)
    if str(payload.get("family_id", "")) != str(evidence_payload["family_id"]):
        raise ValueError("prediction admission family does not match")
    if (
        str(payload.get("evidence_split_sha256", ""))
        != sha256_file(evidence_path)
    ):
        raise ValueError("prediction admission evidence split hash mismatch")

    summary_path = _require_file_identity(
        payload,
        path_field="development_summary_path",
        hash_field="development_summary_sha256",
    )
    bundle_path = _require_file_identity(
        payload,
        path_field="model_bundle_path",
        hash_field="model_bundle_sha256",
    )
    spec_path = _require_file_identity(
        payload,
        path_field="family_spec_path",
        hash_field="family_spec_sha256",
    )
    oof_path = _require_file_identity(
        payload,
        path_field="development_oof_predictions_path",
        hash_field="development_oof_predictions_sha256",
    )
    del oof_path

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    family_id = str(payload["family_id"])
    if any(
        str(item.get("family_id", "")) != family_id
        for item in (summary, bundle, spec)
    ):
        raise ValueError("prediction admission artifact family mismatch")
    if summary.get("sealed_holdout_access_allowed") is not False:
        raise PermissionError("Development summary did not preserve holdout")
    if bundle.get("action_family_allowed") is not False:
        raise PermissionError("prediction bundle unexpectedly allows action")
    if spec.get("live_change_allowed") is not False:
        raise PermissionError("prediction spec unexpectedly allows live")

    evidence = payload.get("admission_evidence") or {}
    rule = payload.get("admission_rule") or {}
    point = float(
        evidence.get(
            "favorable_fill_absolute_brier_improvement",
            float("-inf"),
        )
    )
    probability = float(
        evidence.get(
            "favorable_fill_bootstrap_probability_positive",
            float("-inf"),
        )
    )
    minimum_probability = float(
        rule.get(
            "minimum_day_cluster_probability_improvement_positive",
            float("inf"),
        )
    )
    if point <= 0.0 or probability < minimum_probability:
        raise PermissionError(
            "prediction admission probability screen did not pass"
        )
    for field in (
        "adverse_fill_original_strict_gate_passed",
        "repair_original_strict_gate_passed",
    ):
        if evidence.get(field) is not True:
            raise PermissionError(
                f"prediction admission missing prerequisite {field}"
            )
    return payload


def build_panel_access_decision(
    *,
    evidence_split_path: Path,
    target_panel: str,
    prior_metadata_path: Path,
    prior_ope_summary_paths: list[Path],
    queue_model_bundle_path: Path,
) -> dict[str, Any]:
    evidence_path = evidence_split_path.expanduser().resolve()
    evidence_payload = json.loads(evidence_path.read_text(encoding="utf-8"))
    validate_evidence_split(evidence_payload)
    target = str(target_panel)
    prior_panel = {
        "validation": "development",
        "sealed_holdout": "validation",
    }.get(target)
    if prior_panel is None:
        raise ValueError("target_panel must be validation or sealed_holdout")
    metadata_path = prior_metadata_path.expanduser().resolve()
    bundle_path = queue_model_bundle_path.expanduser().resolve()
    payload = {
        "schema_version": ACCESS_DECISION_SCHEMA_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "family_id": str(evidence_payload["family_id"]),
        "decision": "open",
        "gate_passed": True,
        "target_panel": target,
        "prior_panel": prior_panel,
        "evidence_split_path": str(evidence_path),
        "evidence_split_sha256": sha256_file(evidence_path),
        "prior_metadata_path": str(metadata_path),
        "prior_metadata_sha256": sha256_file(metadata_path),
        "queue_model_bundle_path": str(bundle_path),
        "queue_model_bundle_sha256": sha256_file(bundle_path),
        "prior_ope_summaries": [
            {
                "path": str(path.expanduser().resolve()),
                "sha256": sha256_file(path.expanduser().resolve()),
            }
            for path in prior_ope_summary_paths
        ],
    }
    temporary = _write_temporary_access_decision(payload)
    try:
        validate_panel_access_decision(
            temporary,
            evidence_split_path=evidence_path,
            target_panel=target,
            queue_model_bundle_path=bundle_path,
        )
    finally:
        temporary.unlink(missing_ok=True)
    return payload


def _write_temporary_access_decision(payload: dict[str, Any]) -> Path:
    """Validate a payload through the same file-bound path without persistence."""

    import tempfile

    handle = tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".json",
        delete=False,
        encoding="utf-8",
    )
    try:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.close()
        return Path(handle.name)
    finally:
        if not handle.closed:
            handle.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-feature-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--family-id", required=True)
    parser.add_argument("--inventory-role", default="add")
    parser.add_argument(
        "--panels-json",
        default="",
        help="Optional explicit PANEL_ORDER mapping for a new family split.",
    )
    parser.add_argument(
        "--sides",
        default="BUY,SELL",
        help="Comma-separated sides eligible for randomized intervention.",
    )
    parser.add_argument(
        "--eligibility",
        default="first baseline-eligible exposure-increasing add quote per campaign",
        help="Frozen decision surface for an explicit action family.",
    )
    parser.add_argument(
        "--action-probabilities-json",
        default=json.dumps(DEFAULT_ACTION_PROBABILITIES),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    probabilities = {
        str(key): float(value)
        for key, value in json.loads(args.action_probabilities_json).items()
    }
    if args.panels_json:
        raw_panels = json.loads(args.panels_json)
        payload = build_explicit_evidence_split(
            args.source_feature_manifest,
            family_id=args.family_id,
            panels={
                str(key): [str(day) for day in value]
                for key, value in raw_panels.items()
            },
            behavior_probabilities=probabilities,
            sides=[
                value.strip()
                for value in str(args.sides).split(",")
                if value.strip()
            ],
            inventory_role=args.inventory_role,
            eligibility=str(args.eligibility),
        )
    else:
        payload = build_from_feature_manifest(
            args.source_feature_manifest,
            family_id=args.family_id,
            behavior_probabilities=probabilities,
            inventory_role=args.inventory_role,
        )
    output = args.output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        raise FileExistsError(f"refusing to overwrite immutable split: {output}")
    output.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(output)


if __name__ == "__main__":
    main()
