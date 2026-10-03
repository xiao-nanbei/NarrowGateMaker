#!/usr/bin/env python3
"""Apply a provenance-bound owner coverage override to conditional P3 v4."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from research.governance.paths import resolve_research_path

SPEC_SCHEMA_VERSION = (
    "narrowgate_p3_touch_volatility_conditioned.v4_1.coverage_override.spec"
)
RESULT_SCHEMA_VERSION = (
    "narrowgate_p3_touch_volatility_conditioned.v4_1.coverage_override.result"
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(payload: Any) -> str:
    raw = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _canonical_identity(payload: Mapping[str, Any], field: str) -> str:
    normalized = dict(payload)
    normalized.pop(field, None)
    return canonical_sha256(normalized)


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        suffix=".json",
        dir=path.parent,
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
        json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
    os.replace(temporary, path)


def _require_identity(identity: Mapping[str, Any], label: str) -> Path:
    path = resolve_research_path(str(identity["path"]))
    if not path.is_file():
        raise FileNotFoundError(f"{label} missing: {path}")
    observed = sha256_file(path)
    expected = str(identity["sha256"])
    if observed != expected:
        raise ValueError(
            f"{label} hash mismatch: observed={observed} expected={expected}"
        )
    return path


def load_spec(path: Path) -> dict[str, Any]:
    spec = json.loads(path.read_text(encoding="utf-8"))
    if spec.get("schema_version") != SPEC_SCHEMA_VERSION:
        raise ValueError("unsupported conditional P3 coverage override schema")
    field = "canonical_spec_identity_sha256"
    if _canonical_identity(spec, field) != spec.get(field):
        raise ValueError("conditional P3 coverage override canonical hash mismatch")
    for label, identity in spec["identities"].items():
        _require_identity(identity, label)

    adjustment = spec["coverage_threshold_adjustment"]
    if float(adjustment["original_minimum_fraction"]) != 0.98:
        raise ValueError("coverage override must bind the original 98% gate")
    if float(adjustment["successor_minimum_fraction"]) != 0.95:
        raise ValueError("coverage override must set the owner-requested 95% gate")
    if not bool(adjustment.get("outcome_informed", False)):
        raise ValueError("coverage override must remain marked outcome-informed")
    if not bool(adjustment.get("original_identity_immutable", False)):
        raise ValueError("coverage override cannot rewrite the original identity")
    if adjustment.get("changed_fields") != [
        "evaluation.context_coverage_gate.minimum_fraction"
    ]:
        raise ValueError("coverage override changed more than one contract field")

    permissions = spec["permissions"]
    for forbidden in (
        "operational_prediction_authority",
        "quote_mapping_authority",
        "action_authority",
        "live_authority",
        "overwrite_current_v2_artifact",
        "independent_confirmation",
    ):
        if bool(permissions.get(forbidden, False)):
            raise ValueError(f"coverage override cannot grant {forbidden}")
    if not bool(permissions.get("historical_development_prediction_support", False)):
        raise ValueError("successor must explicitly scope prediction support")
    return spec


def _verify_manifest(root: Path, manifest: Mapping[str, Any]) -> int:
    failures: list[str] = []
    files = manifest.get("files", {})
    for relative, identity in files.items():
        path = root / str(relative)
        if (
            not path.is_file()
            or int(path.stat().st_size) != int(identity["size_bytes"])
            or sha256_file(path) != str(identity["sha256"])
        ):
            failures.append(str(relative))
    if failures:
        raise ValueError(f"original v4 manifest verification failed: {failures}")
    return len(files)


def evaluate(spec_path: Path, output_dir: Path) -> dict[str, Any]:
    spec_path = spec_path.expanduser().resolve()
    output_dir = output_dir.expanduser().resolve()
    spec = load_spec(spec_path)
    identities = spec["identities"]

    original_spec_path = resolve_research_path(
        str(identities["original_v4_spec"]["path"])
    )
    original_report_path = resolve_research_path(
        str(identities["original_v4_report"]["path"])
    )
    original_manifest_path = resolve_research_path(
        str(identities["original_v4_manifest"]["path"])
    )
    cache_usage_path = resolve_research_path(
        str(identities["original_v4_cache_usage"]["path"])
    )
    original_spec = json.loads(original_spec_path.read_text(encoding="utf-8"))
    original_report = json.loads(original_report_path.read_text(encoding="utf-8"))
    original_manifest = json.loads(original_manifest_path.read_text(encoding="utf-8"))

    if original_spec.get("identity") != "p3_touch_volatility_conditioned_v4":
        raise ValueError("coverage override requires the frozen conditional v4 spec")
    original_fraction = float(
        original_spec["evaluation"]["context_coverage_gate"]["minimum_fraction"]
    )
    if original_fraction != float(
        spec["coverage_threshold_adjustment"]["original_minimum_fraction"]
    ):
        raise ValueError("original v4 coverage gate does not match the override")
    if original_report.get("decision") != "conditional_v4_prediction_gate_failed_development":
        raise ValueError("original v4 report is not the frozen failed identity")

    original_gates = original_report["gates"]
    required_positive_gates = (
        "proper_score_passed",
        "calibration_passed",
        "source_transport_passed",
        "monotonicity_contract_valid",
    )
    if not all(bool(original_gates.get(gate, False)) for gate in required_positive_gates):
        raise ValueError("coverage was not the only failed v4 component")
    if bool(original_gates.get("context_coverage_passed", True)):
        raise ValueError("original v4 coverage gate unexpectedly passed")
    if bool(original_gates.get("historical_prediction_gate_passed", True)):
        raise ValueError("original v4 aggregate prediction gate unexpectedly passed")

    manifest_files_verified = _verify_manifest(
        original_manifest_path.parent,
        original_manifest,
    )
    cache_usage = pd.read_csv(cache_usage_path)
    expected_columns = {
        "source",
        "panel",
        "day",
        "windows",
        "cache_hit",
        "cache_path",
        "cache_key",
        "coverage_fraction",
    }
    if set(cache_usage.columns) != expected_columns:
        raise ValueError("original v4 cache-usage schema changed")
    if cache_usage.empty or cache_usage[["source", "day"]].duplicated().any():
        raise ValueError("original v4 cache-usage denominator is invalid")
    minimum = float(cache_usage["coverage_fraction"].min())
    reported_minimum = float(
        original_report["context_cache"]["minimum_coverage_fraction"]
    )
    if abs(minimum - reported_minimum) > 1e-15:
        raise ValueError("cache-usage minimum does not match the original report")

    successor_fraction = float(
        spec["coverage_threshold_adjustment"]["successor_minimum_fraction"]
    )
    context_coverage_passed = bool(
        (cache_usage["coverage_fraction"] >= successor_fraction).all()
    )
    if not context_coverage_passed:
        raise ValueError("owner-requested 95% coverage gate still fails")
    below_original = cache_usage.loc[
        cache_usage["coverage_fraction"] < original_fraction,
        ["source", "panel", "day", "windows", "coverage_fraction"],
    ].sort_values(["source", "day"])

    gates = {
        **{gate: bool(original_gates[gate]) for gate in required_positive_gates},
        "context_coverage_passed": True,
        "historical_prediction_gate_passed": True,
    }
    result = {
        "schema_version": RESULT_SCHEMA_VERSION,
        "identity": str(spec["identity"]),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "spec": {"path": str(spec_path), "sha256": sha256_file(spec_path)},
        "predecessor": {
            "identity": "p3_touch_volatility_conditioned_v4",
            "decision": str(original_report["decision"]),
            "spec_sha256": sha256_file(original_spec_path),
            "report_sha256": sha256_file(original_report_path),
            "manifest_sha256": sha256_file(original_manifest_path),
            "immutable": True,
        },
        "adjustment": spec["coverage_threshold_adjustment"],
        "retraining_performed": False,
        "model_artifact_reused": original_report["final_development_artifact"],
        "manifest_files_verified": int(manifest_files_verified),
        "context_cache": {
            "source_day_rows": int(len(cache_usage)),
            "minimum_coverage_fraction": minimum,
            "successor_minimum_fraction": successor_fraction,
            "below_original_gate": below_original.to_dict(orient="records"),
        },
        "inherited_evidence": {
            "proper_score": original_report["proper_score"],
            "calibration_gate": original_report["calibration_gate"],
            "source_prediction_transport": original_report[
                "source_prediction_transport"
            ],
            "monotonicity_contract": original_report["monotonicity_contract"],
        },
        "gates": gates,
        "decision": (
            "historical_development_prediction_supported_owner_coverage_override"
        ),
        "permissions": spec["permissions"],
    }
    _atomic_json(output_dir / "result.json", result)
    manifest = {
        "schema_version": (
            "narrowgate_p3_touch_volatility_conditioned.v4_1."
            "coverage_override.output_manifest"
        ),
        "identity": str(spec["identity"]),
        "files": {
            "result.json": {
                "sha256": sha256_file(output_dir / "result.json"),
                "size_bytes": int((output_dir / "result.json").stat().st_size),
            }
        },
    }
    _atomic_json(output_dir / "manifest.json", manifest)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    result = evaluate(args.spec, args.output_dir)
    print(
        json.dumps(
            {
                "identity": result["identity"],
                "decision": result["decision"],
                "gates": result["gates"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
