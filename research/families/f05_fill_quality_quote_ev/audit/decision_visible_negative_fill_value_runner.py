#!/usr/bin/env python3
"""Freeze and run the F05 Development-only negative fill-value evidence.

``freeze-fit`` binds the already-produced native F10 bytes without reading the
outcome parquet. ``evaluate`` is a separate command that may read Development
only after that fit identity exists. Neither command can register an action or
grant live authority.
"""

from __future__ import annotations

import argparse
import json
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from research.families.f05_fill_quality_quote_ev.audit import (
    decision_visible_negative_fill_value_evidence as evidence,
)
from research.families.f10_live_replay_attribution.audit import (
    first_add_decision_to_terminal_contract as f10_contract,
)

DEFAULT_METHOD_PATH = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "decision_visible_negative_fill_value_evidence_m0_v1_1_method_20260729.json"
)


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected a JSON object: {path}")
    return payload


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".partial")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _regular_file(path: Path, expected_sha256: str, label: str) -> Path:
    resolved = Path(path).expanduser().resolve()
    if resolved.is_symlink() or not resolved.is_file():
        raise ValueError(f"{label} must be a regular file")
    if evidence.sha256_file(resolved) != str(expected_sha256):
        raise ValueError(f"{label} SHA256 mismatch")
    return resolved


def build_fit_identity(
    *,
    native_manifest_path: Path,
    method_path: Path = DEFAULT_METHOD_PATH,
) -> dict[str, Any]:
    """Bind F10 bytes and support counts without reading outcome rows."""

    method_file = Path(method_path).expanduser().resolve()
    method = _load_json(method_file)
    method_identity = {
        "path": str(method_file),
        "sha256": evidence.sha256_file(method_file),
        "canonical_method_contract_sha256": method.get(
            "canonical_method_contract_sha256"
        ),
    }
    evidence._load_method_contract(
        method_identity,
        expected_identity=str(method.get("identity", "")),
    )

    manifest_file = Path(native_manifest_path).expanduser().resolve()
    manifest = _load_json(manifest_file)
    if manifest.get("identity") != "first_add_decision_to_terminal_native_producer_v1":
        raise ValueError("F05 fit requires the frozen F10 native producer")
    if not bool(manifest.get("complete_development", False)):
        raise ValueError("F05 fit cannot bind a partial F10 producer run")
    if bool(manifest.get("validation_read", True)) or bool(
        manifest.get("sealed_holdout_read", True)
    ):
        raise ValueError("F05 fit detected later-panel access")

    trace_path = _regular_file(
        Path(str(manifest.get("trace_path", ""))),
        str(manifest.get("trace_sha256", "")),
        "F10 native trace",
    )
    audit_path = _regular_file(
        Path(str(manifest.get("producer_audit_path", ""))),
        str(manifest.get("producer_audit_sha256", "")),
        "F10 producer audit",
    )
    producer_audit = pd.read_parquet(audit_path)
    required_audit = {
        "selected_campaign_count",
        "emitted_row_count",
        "exact_join_count",
        "feature_clock_violation_count",
        "open_record_count",
    }
    if not required_audit.issubset(producer_audit.columns):
        raise ValueError("F10 producer audit columns drifted")
    selected = int(producer_audit["selected_campaign_count"].sum())
    emitted = int(producer_audit["emitted_row_count"].sum())
    exact = int(producer_audit["exact_join_count"].sum())
    feature_clock = int(producer_audit["feature_clock_violation_count"].sum())
    open_records = int(producer_audit["open_record_count"].sum())
    if selected < 1 or len({selected, emitted, exact}) != 1:
        raise ValueError("F10 exact native denominator is incomplete")
    if feature_clock != 0 or open_records != 0:
        raise ValueError("F10 producer audit is not causally complete")

    f10_identity = method["f10_spec_identity"]
    f10_spec_path = _regular_file(
        Path(str(f10_identity["path"])),
        str(f10_identity["sha256"]),
        "F10 frozen spec",
    )
    f10_spec = _load_json(f10_spec_path)
    f10_contract.validate_spec(f10_spec)
    if (
        f10_contract.canonical_spec_sha256(f10_spec)
        != f10_identity["canonical_spec_sha256"]
    ):
        raise ValueError("F10 canonical spec identity drifted")

    native_spec = manifest.get("producer_identity", {}).get(
        "native_producer_spec", {}
    )
    native_spec_path = _regular_file(
        Path(str(native_spec.get("path", ""))),
        str(native_spec.get("sha256", "")),
        "F10 native producer spec",
    )
    panels = f10_spec["panels"]
    payload: dict[str, Any] = {
        "schema_version": evidence.FIT_SCHEMA_VERSION,
        "identity": method["identity"],
        "status": "frozen_native_f10_artifact_development_only",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "canonical_fit_identity_sha256": "",
        "method_contract_identity": method_identity,
        "f10_source": {
            "trace_schema_version": f10_contract.TRACE_SCHEMA_VERSION,
            "artifact_path": str(trace_path),
            "artifact_sha256": evidence.sha256_file(trace_path),
            "producer_audit_artifact": {
                "path": str(audit_path),
                "sha256": evidence.sha256_file(audit_path),
            },
            "native_producer_manifest": {
                "path": str(manifest_file),
                "sha256": evidence.sha256_file(manifest_file),
            },
            "native_producer_spec": {
                "path": str(native_spec_path),
                "sha256": evidence.sha256_file(native_spec_path),
            },
            "spec_path": str(f10_spec_path),
            "spec_file_sha256": evidence.sha256_file(f10_spec_path),
            "spec_canonical_sha256": f10_spec["canonical_spec_sha256"],
            "exact_native_join_required": True,
            "feature_ready_clock_required": True,
            "producer_audit": {
                "candidate_campaigns": selected,
                "emitted_rows": emitted,
                "exact_join_rows": exact,
                "nearest_time_match_rows": 0,
                "feature_clock_violation_rows": feature_clock,
            },
        },
        "authoritative_target": method["authoritative_target"],
        "panels": {
            "grade_a_primary_days": panels["development_primary_grade_a_days"],
            "grade_b_sensitivity_days": panels[
                "development_sensitivity_grade_b_days"
            ],
            "pooling": method["panels"]["pooling"],
            "grade_b_role": method["panels"]["grade_b_role"],
        },
        "model": method["model"],
        "chronology": method["chronology"],
        "high_risk": method["high_risk"],
        "inference": method["inference"],
        "outcome_access": method["outcome_access"],
        "permissions": method["permissions"],
    }
    payload["canonical_fit_identity_sha256"] = (
        evidence.canonical_fit_identity_sha256(payload)
    )
    evidence.validate_fit_identity(payload)
    return payload


def write_evaluation(
    *,
    fit_identity_path: Path,
    output_dir: Path,
) -> dict[str, Path]:
    fit_path = Path(fit_identity_path).expanduser().resolve()
    fit_identity = _load_json(fit_path)
    evaluation = evidence.evaluate_frozen_f10_parquet(fit_identity)

    output = Path(output_dir).expanduser().resolve()
    if output.exists():
        raise FileExistsError(f"F05 output already exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(prefix=f".{output.name}.partial.", dir=output.parent)
    )
    try:
        oof_path = staging / "oof_predictions.parquet"
        report_path = staging / "report.json"
        manifest_path = staging / "manifest.json"
        evaluation.oof_predictions.to_parquet(oof_path, index=False)
        report = dict(evaluation.report)
        report.update(
            {
                "created_at_utc": datetime.now(timezone.utc).isoformat(),
                "fit_identity": {
                    "path": str(fit_path),
                    "sha256": evidence.sha256_file(fit_path),
                    "canonical_fit_identity_sha256": fit_identity[
                        "canonical_fit_identity_sha256"
                    ],
                },
                "artifacts": {
                    "oof_predictions": {
                        "path": str(output / oof_path.name),
                        "sha256": evidence.sha256_file(oof_path),
                    }
                },
            }
        )
        _atomic_json(report_path, report)
        manifest = {
            "schema_version": "decision_visible_negative_fill_value_evidence.manifest.v1",
            "identity": fit_identity["identity"],
            "status": "development_prediction_evidence_only",
            "fit_identity": report["fit_identity"],
            "oof_predictions": report["artifacts"]["oof_predictions"],
            "report": {
                "path": str(output / report_path.name),
                "sha256": evidence.sha256_file(report_path),
            },
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_experiment_authorized": False,
            "live_deployment_authorized": False,
            "ranking_score": None,
        }
        _atomic_json(manifest_path, manifest)
        staging.replace(output)
        return {
            "oof_predictions": output / oof_path.name,
            "report": output / report_path.name,
            "manifest": output / manifest_path.name,
        }
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze-fit")
    freeze.add_argument("--native-manifest", type=Path, required=True)
    freeze.add_argument("--method", type=Path, default=DEFAULT_METHOD_PATH)
    freeze.add_argument("--output", type=Path, required=True)
    evaluate = subparsers.add_parser("evaluate")
    evaluate.add_argument("--fit-identity", type=Path, required=True)
    evaluate.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command == "freeze-fit":
        output = args.output.expanduser().resolve()
        if output.exists():
            raise FileExistsError(f"fit identity already exists: {output}")
        output.parent.mkdir(parents=True, exist_ok=True)
        payload = build_fit_identity(
            native_manifest_path=args.native_manifest,
            method_path=args.method,
        )
        _atomic_json(output, payload)
        print(json.dumps({"fit_identity": str(output)}, sort_keys=True))
        return 0
    paths = write_evaluation(
        fit_identity_path=args.fit_identity,
        output_dir=args.output_dir,
    )
    print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
