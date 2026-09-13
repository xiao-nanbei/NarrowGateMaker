#!/usr/bin/env python3
"""Write frozen Development evidence for SELL first-fill conditional value."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from research.families.f05_fill_quality_quote_ev.audit import (
    sell_first_fill_conditional_value as evaluator,
)
from research.families.f10_live_replay_attribution.audit import (
    first_opener_decision_to_terminal_contract as lifecycle_contract,
)
from research.families.f10_live_replay_attribution.audit import (
    first_opener_decision_to_terminal_runner as producer_runner,
)

FIT_SCHEMA_VERSION = "sell_first_fill_conditional_value.fit.v3"
IDENTITY = "sell_first_fill_conditional_value_feasibility_v3"
ARTIFACT_SCHEMA_VERSION = "sell_first_fill_conditional_value.artifact.v3"
ROOT = Path(__file__).resolve().parents[4]
REQUIRED_IMPLEMENTATION_PATHS = frozenset(
    {
        "research/families/f05_fill_quality_quote_ev/audit/sell_first_fill_conditional_value.py",
        "research/families/f05_fill_quality_quote_ev/audit/sell_first_fill_conditional_value_artifact.py",
        "research/families/f10_live_replay_attribution/audit/first_opener_decision_to_terminal_contract.py",
        "research/families/f10_live_replay_attribution/audit/first_opener_decision_to_terminal_runner.py",
        "tests/test_sell_first_fill_conditional_value.py",
        "tests/test_sell_first_fill_conditional_value_artifact.py",
        "tests/test_first_opener_decision_to_terminal_contract.py",
        "tests/test_first_opener_decision_to_terminal_runner.py",
    }
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_fit_sha256(payload: Mapping[str, Any]) -> str:
    normalized = dict(payload)
    normalized.pop("canonical_fit_sha256", None)
    encoded = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("SELL first-fill identity must be a JSON object")
    return payload


def _require_identity(identity: Mapping[str, Any], label: str) -> Path:
    path = Path(str(identity.get("path", ""))).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"{label} is missing: {path}")
    expected = str(identity.get("sha256", ""))
    actual = sha256_file(path)
    if actual != expected:
        raise ValueError(f"{label} hash mismatch: expected {expected}, found {actual}")
    return path


def validate_fit_identity(payload: Mapping[str, Any]) -> None:
    if payload.get("schema_version") != FIT_SCHEMA_VERSION:
        raise ValueError("unexpected SELL first-fill fit schema")
    if payload.get("identity") != IDENTITY:
        raise ValueError("unexpected SELL first-fill fit identity")
    if payload.get("status") != "frozen_after_complete_native_development_artifact":
        raise ValueError("SELL first-fill fit identity is not frozen")
    frozen = str(payload.get("canonical_fit_sha256", ""))
    if len(frozen) != 64 or canonical_fit_sha256(payload) != frozen:
        raise ValueError("SELL first-fill fit identity hash mismatch")
    permissions = payload.get("permissions") or {}
    required_permissions = {
        "development_outcome_read",
        "validation_read",
        "sealed_holdout_read",
        "action_experiment_authorized",
        "live_deployment_authorized",
    }
    if set(permissions) != required_permissions:
        raise ValueError("SELL first-fill permission identity is incomplete")
    if not bool(permissions.get("development_outcome_read", False)):
        raise ValueError("SELL first-fill fit identity cannot read Development")
    forbidden_permissions = {
        key: value
        for key, value in permissions.items()
        if key != "development_outcome_read"
    }
    if any(bool(value) for value in forbidden_permissions.values()):
        raise ValueError("SELL first-fill fit identity grants forbidden authority")

    f10_path = _require_identity(payload.get("f10_spec_identity") or {}, "F10 spec")
    f10_spec = _load_json(f10_path)
    lifecycle_contract.validate_spec(f10_spec)
    producer_path = _require_identity(
        payload.get("producer_manifest_identity") or {}, "producer manifest"
    )
    producer = _load_json(producer_path)
    if producer.get("schema_version") != producer_runner.RUN_SCHEMA_VERSION:
        raise ValueError("SELL first-fill producer manifest schema drifted")
    if not bool(producer.get("complete_development", False)):
        raise ValueError("SELL first-fill producer is not complete Development")
    if producer.get("identity") != producer_runner.IDENTITY:
        raise ValueError("SELL first-fill producer identity drifted")
    if producer.get("mode") != "formal_development_native_production":
        raise ValueError("SELL first-fill cannot consume diagnostic checkpoints")
    producer_identity = producer.get("producer_identity") or {}
    producer_spec_identity = producer_identity.get("native_producer_spec") or {}
    producer_spec_path = _require_identity(
        producer_spec_identity,
        "producer spec",
    )
    producer_spec = _load_json(producer_spec_path)
    producer_runner.validate_producer_spec(producer_spec)
    f10_identity = producer_spec.get("f10_spec_identity") or {}
    if (
        str(Path(str(f10_identity.get("path", ""))).expanduser().resolve())
        != str(f10_path)
        or str(f10_identity.get("sha256", ""))
        != sha256_file(f10_path)
    ):
        raise ValueError("SELL first-fill producer bound a different F10 spec")
    panels = f10_spec["panels"]
    expected_days = sorted(
        {
            *panels["development_primary_grade_a_days"],
            *panels["development_sensitivity_grade_b_days"],
        }
    )
    if sorted(producer.get("expected_days") or ()) != expected_days or sorted(
        producer.get("requested_days") or ()
    ) != expected_days:
        raise ValueError("SELL first-fill producer denominator drifted")
    producer_audit_path = Path(
        str(producer.get("producer_audit_path", ""))
    ).expanduser().resolve()
    if (
        not producer_audit_path.is_file()
        or sha256_file(producer_audit_path)
        != str(producer.get("producer_audit_sha256", ""))
    ):
        raise ValueError("SELL first-fill producer audit identity drifted")
    producer_audit = pd.read_parquet(producer_audit_path)
    if sorted(producer_audit["day"].astype(str).unique()) != expected_days:
        raise ValueError("SELL first-fill producer audit days drifted")
    day_audits = list(producer.get("day_audits") or ())
    audit_days = [str(row.get("day", "")) for row in day_audits]
    if len(day_audits) != len(expected_days) or sorted(audit_days) != expected_days:
        raise ValueError("SELL first-fill per-day audit lineage is incomplete")
    producer_spec_sha256 = sha256_file(producer_spec_path)
    for day_audit in day_audits:
        day = str(day_audit.get("day", ""))
        if str(day_audit.get("producer_spec_sha256", "")) != producer_spec_sha256:
            raise ValueError(f"SELL first-fill checkpoint identity drifted: {day}")
        if day_audit.get("run_mode") != "formal_development_native_production":
            raise ValueError(f"SELL first-fill checkpoint mode drifted: {day}")
        source_payload_audit = (
            day_audit.get("runtime_audit", {}).get("source_payload_audit") or {}
        )
        if (
            str(source_payload_audit.get("day", "")) != day
            or int(source_payload_audit.get("payload_count", 0) or 0) <= 0
            or len(str(source_payload_audit.get("payload_identity_sha256", "")))
            != 64
        ):
            raise ValueError(f"SELL first-fill source payload audit drifted: {day}")
        checkpoint_path = Path(str(day_audit.get("trace_path", ""))).resolve()
        if (
            not checkpoint_path.is_file()
            or sha256_file(checkpoint_path)
            != str(day_audit.get("trace_sha256", ""))
        ):
            raise ValueError(f"SELL first-fill checkpoint trace drifted: {day}")
    feature_support = producer.get("decision_feature_support") or {}
    if not bool(feature_support.get("passed", False)):
        raise ValueError("SELL first-fill producer feature support failed")
    if tuple(feature_support.get("model_features") or ()) != tuple(
        lifecycle_contract.MODEL_FEATURES
    ):
        raise ValueError("SELL first-fill producer feature identity drifted")
    support = producer.get("true_opener_support") or {}
    if not bool(support.get("passed", False)):
        raise ValueError("SELL first-fill true-opener support gate failed")
    if float(support.get("coverage", 0.0)) < float(
        support.get("minimum_coverage", 1.0)
    ):
        raise ValueError("SELL first-fill true-opener coverage is insufficient")
    trace_path = _require_identity(payload.get("trace_identity") or {}, "native trace")
    if str(trace_path) != str(Path(producer["trace_path"]).resolve()):
        raise ValueError("SELL first-fill trace path differs from producer manifest")
    if sha256_file(trace_path) != str(producer.get("trace_sha256", "")):
        raise ValueError("SELL first-fill trace differs from producer manifest")
    implementation = payload.get("implementation_identity") or {}
    if set(implementation) != set(REQUIRED_IMPLEMENTATION_PATHS):
        raise ValueError("SELL first-fill implementation identity is incomplete")
    for relative, expected in implementation.items():
        path = ROOT / str(relative)
        if not path.is_file() or sha256_file(path) != str(expected):
            raise ValueError(f"SELL first-fill implementation drifted: {relative}")


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".partial")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _atomic_parquet(path: Path, frame: pd.DataFrame) -> None:
    temporary = path.with_suffix(path.suffix + ".partial")
    frame.to_parquet(temporary, index=False)
    temporary.replace(path)


def build_artifact(
    fit_identity: Mapping[str, Any],
    *,
    output_dir: Path,
) -> dict[str, Any]:
    validate_fit_identity(fit_identity)
    f10_path = Path(fit_identity["f10_spec_identity"]["path"])
    trace_path = Path(fit_identity["trace_identity"]["path"])
    spec = _load_json(f10_path)
    trace = pd.read_parquet(trace_path)
    result = evaluator.evaluate_native_trace(trace, spec)

    output = Path(output_dir).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    oof_path = output / "oof_predictions.parquet"
    report_path = output / "report.json"
    _atomic_parquet(oof_path, result.oof_predictions)
    report = {
        **result.report,
        "fit_identity_sha256": str(fit_identity["canonical_fit_sha256"]),
        "producer_manifest_sha256": str(
            fit_identity["producer_manifest_identity"]["sha256"]
        ),
        "native_trace_sha256": str(fit_identity["trace_identity"]["sha256"]),
    }
    _atomic_json(report_path, report)
    manifest = {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "identity": IDENTITY,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "fit_identity_sha256": str(fit_identity["canonical_fit_sha256"]),
        "oof_path": str(oof_path),
        "oof_sha256": sha256_file(oof_path),
        "report_path": str(report_path),
        "report_sha256": sha256_file(report_path),
        "prediction_supported": bool(report["prediction_supported"]),
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_experiment_authorized": False,
        "live_deployment_authorized": False,
    }
    _atomic_json(output / "manifest.json", manifest)
    return manifest


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fit-spec", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    fit_identity = _load_json(args.fit_spec.expanduser().resolve())
    manifest = build_artifact(fit_identity, output_dir=args.output_dir)
    print(json.dumps(manifest, indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
