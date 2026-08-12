#!/usr/bin/env python3
"""Register an owner progression without rewriting the failed F05 hard gate."""

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

from models.audit.research_progression import (
    SCHEMA_VERSION as PROGRESSION_SCHEMA_VERSION,
)
from models.audit.research_progression import (
    progression_contract_sha256,
    validate_progression_contract,
)
from research.governance.paths import resolve_research_path

SPEC_SCHEMA_VERSION = "conditional_p3_joint_quote.owner_progression.v1.spec"
RESULT_SCHEMA_VERSION = "conditional_p3_joint_quote.owner_progression.v1.result"


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


def _identity(path: Path) -> dict[str, Any]:
    return {
        "path": str(path.resolve()),
        "sha256": sha256_file(path),
        "size_bytes": int(path.stat().st_size),
    }


def _require_identity(identity: Mapping[str, Any], *, label: str) -> Path:
    path = resolve_research_path(str(identity["path"]))
    if not path.is_file():
        raise FileNotFoundError(f"{label} missing: {path}")
    if sha256_file(path) != str(identity["sha256"]):
        raise ValueError(f"{label} SHA256 changed")
    if "size_bytes" in identity and path.stat().st_size != int(identity["size_bytes"]):
        raise ValueError(f"{label} size changed")
    return path


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


def load_spec(path: Path) -> dict[str, Any]:
    spec = json.loads(path.read_text(encoding="utf-8"))
    if spec.get("schema_version") != SPEC_SCHEMA_VERSION:
        raise ValueError("unsupported F05 owner-progression schema")
    identity_field = "canonical_spec_identity_sha256"
    if _canonical_identity(spec, identity_field) != spec.get(identity_field):
        raise ValueError("F05 owner-progression canonical identity mismatch")
    for label, identity in spec["identities"].items():
        _require_identity(identity, label=label)

    contract = spec["progression_contract"]
    if contract.get("schema_version") != PROGRESSION_SCHEMA_VERSION:
        raise ValueError("owner successor uses the wrong shared progression schema")
    validate_progression_contract(contract)
    if contract["hard_gate_path"]["passed"]:
        raise ValueError("this successor requires a failed hard-gate predecessor")
    permissions = contract["current_permissions"]
    if not bool(permissions.get("development_economic_outcomes_read_authorized", False)):
        raise ValueError("owner successor must explicitly authorize Development economics")
    if not bool(permissions.get("sparse_value_diagnostic_authorized", False)):
        raise ValueError("owner successor must explicitly authorize the sparse diagnostic")

    accepted = spec["owner_accepted_support"]
    if accepted != {
        "minimum_supported_days": 28,
        "required_oof_fold_count": 3,
        "minimum_filled_rows_per_side_role_action": 1,
    }:
        raise ValueError("owner continuation must bind exactly the observed support")
    if spec.get("changed_fields") != [
        "owner_progression.minimum_supported_days",
        "owner_progression.required_oof_fold_count",
        "owner_progression.minimum_filled_rows_per_side_role_action",
    ]:
        raise ValueError("owner continuation changed an unapproved contract field")
    return spec


def evaluate(spec_path: Path, output_dir: Path) -> dict[str, Any]:
    spec_path = spec_path.expanduser().resolve()
    output_dir = output_dir.expanduser().resolve()
    spec = load_spec(spec_path)
    identities = spec["identities"]
    report_path = _require_identity(identities["predecessor_report"], label="predecessor report")
    side_path = _require_identity(identities["predecessor_side_support"], label="side support")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    side_support = pd.read_parquet(side_path)

    if report.get("identity") != "conditional_p3_joint_quote_value_preflight_v1":
        raise ValueError("owner successor is not bound to the F05 preflight")
    if bool(report.get("supported", True)):
        raise ValueError("predecessor hard gate unexpectedly passed")
    if report.get("decision") != "stop_before_direct_value_fit_support_insufficient":
        raise ValueError("predecessor decision changed")
    support = report["support"]
    observed = {
        "minimum_supported_days": int(support["supported_day_count"]),
        "required_oof_fold_count": int(len(support["days_per_oof_fold"])),
        "minimum_filled_rows_per_side_role_action": int(
            support["minimum_formal_cell_filled_rows"]
        ),
    }
    if observed != spec["owner_accepted_support"]:
        raise ValueError("observed support no longer matches the owner successor")
    if int(support["paired_joint_quote_buckets"]) != 282:
        raise ValueError("paired joint-quote denominator changed")
    if len(side_support) != int(support["input_side_rows"]):
        raise ValueError("side-support row count changed")

    hard = spec["progression_contract"]["hard_gate_path"]
    if hard["predecessor_spec_sha256"] != identities["predecessor_spec"]["sha256"]:
        raise ValueError("hard path does not bind the predecessor Spec")
    if hard["predecessor_report_sha256"] != identities["predecessor_report"]["sha256"]:
        raise ValueError("hard path does not bind the predecessor report")

    result = {
        "schema_version": RESULT_SCHEMA_VERSION,
        "identity": str(spec["identity"]),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "spec": _identity(spec_path),
        "predecessor": {
            "identity": str(report["identity"]),
            "decision": str(report["decision"]),
            "hard_gate_passed": False,
            "immutable": True,
            "spec_sha256": identities["predecessor_spec"]["sha256"],
            "report_sha256": identities["predecessor_report"]["sha256"],
        },
        "hard_gate_path": {
            "passed": False,
            "required": report["gates"],
            "observed": observed,
            "failed_gates": list(hard["failed_gates"]),
        },
        "owner_progression_path": {
            "support_accepted": True,
            "accepted_support": observed,
            "paired_joint_quote_buckets": 282,
            "outcome_informed": True,
            "next_stage": "conditional_p3_joint_quote_sparse_value_diagnostic_v1",
            "next_stage_authorized": True,
            "promotion_route": spec["progression_contract"]["promotion_routes"][
                "owner_progression_path"
            ],
        },
        "progression_contract": spec["progression_contract"],
        "progression_contract_sha256": progression_contract_sha256(
            spec["progression_contract"]
        ),
        "decision": "owner_progression_authorized_sparse_development_value_diagnostic",
        "permissions": spec["progression_contract"]["current_permissions"],
    }
    _atomic_json(output_dir / "result.json", result)
    manifest = {
        "schema_version": "conditional_p3_joint_quote.owner_progression.v1.output_manifest",
        "identity": str(spec["identity"]),
        "files": {"result.json": _identity(output_dir / "result.json")},
    }
    manifest["files"]["result.json"]["path"] = "result.json"
    _atomic_json(output_dir / "manifest.json", manifest)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    result = evaluate(args.spec, args.output_dir)
    print(
        json.dumps(
            {
                "identity": result["identity"],
                "hard_gate_passed": result["hard_gate_path"]["passed"],
                "owner_progression_accepted": result["owner_progression_path"][
                    "support_accepted"
                ],
                "decision": result["decision"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
