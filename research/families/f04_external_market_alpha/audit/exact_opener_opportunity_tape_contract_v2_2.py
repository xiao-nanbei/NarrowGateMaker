#!/usr/bin/env python3
"""Validate the execution-only F04 exact-opener v2.2 contract."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from data_paths import data_root, resolve_portable_path

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = (
    "external_adverse_quote_edge_guard_exact_opener_execution_contract.v2.2"
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


def canonical_spec_sha256(payload: Mapping[str, Any]) -> str:
    normalized = dict(payload)
    normalized.pop("canonical_spec_identity_sha256", None)
    return canonical_sha256(normalized)


def _resolve(value: str) -> Path:
    path = Path(str(value)).expanduser()
    return path if path.is_absolute() else ROOT / path


def _require_identity(identity: Mapping[str, Any], label: str) -> Path:
    path = _resolve(str(identity.get("path", "")))
    if not path.is_file():
        raise ValueError(f"{label} file missing: {path}")
    actual = sha256_file(path)
    if actual != str(identity.get("sha256", "")):
        raise ValueError(f"{label} SHA256 mismatch")
    return path


def validate_execution_contract_v2_2(path: Path) -> dict[str, Any]:
    contract = json.loads(path.read_text(encoding="utf-8"))
    if contract.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unexpected exact-opener v2.2 schema")
    expected_identity = str(contract.get("canonical_spec_identity_sha256", ""))
    if canonical_spec_sha256(contract) != expected_identity:
        raise ValueError("exact-opener v2.2 canonical hash mismatch")

    predecessor = contract.get("frozen_v2_1_predecessor") or {}
    _require_identity(predecessor, "frozen v2.1 predecessor")
    if predecessor.get("modified") is not False:
        raise ValueError("v2.2 cannot rewrite frozen v2.1")

    for label, identity in (contract.get("implementation_identity") or {}).items():
        _require_identity(identity, label)

    runtime = contract.get("runtime_contract") or {}
    required_true = (
        "runtime_producer_identity_bound",
        "fair_state_and_config_identity_bound",
        "three_venue_anchor_shadow_only_fail_closed",
        "writer_health_complete",
        "hot_start_quarantine",
        "cancel_reject_restores_active_lifecycle",
    )
    if any(runtime.get(key) is not True for key in required_true):
        raise ValueError("v2.2 runtime contract is incomplete")

    admission = contract.get("admission_contract") or {}
    required_admission = (
        "local_session_day_staging",
        "ready_manifest_required",
        "row_schema_file_sha256_verified",
        "atomic_orico_admission",
        "crash_restart_idempotent",
        "overlap_or_half_window_splicing_forbidden",
    )
    if any(admission.get(key) is not True for key in required_admission):
        raise ValueError("v2.2 admission contract is incomplete")
    authoritative_root = resolve_portable_path(
        str(admission.get("authoritative_root", "")), root=ROOT
    ).resolve()
    if authoritative_root != (data_root(ROOT) / "exact_opportunity_tape").resolve():
        raise ValueError("v2.2 authoritative data root drifted")

    public_config_path = _resolve(str(contract["public_config"]["path"]))
    public_config = yaml.safe_load(public_config_path.read_text(encoding="utf-8"))
    enabled = bool(
        ((public_config or {}).get("logging") or {}).get(
            "exact_opportunity_tape_enabled", False
        )
    )
    if enabled:
        raise ValueError("v2.2 prospective tape must remain disabled")

    outcomes = contract.get("outcome_boundary") or {}
    if outcomes.get("economic_outcomes_read") is not False:
        raise ValueError("v2.2 cannot read economic outcomes")
    if outcomes.get("operational_lifecycle_outcomes_read") is not True:
        raise ValueError("v2.2 must disclose lifecycle outcomes")

    permissions = contract.get("permissions") or {}
    if permissions.get("implementation_preflight_eligible") is not True:
        raise ValueError("v2.2 implementation preflight is not eligible")
    for forbidden in (
        "prospective_collection_enabled",
        "prospective_tape_read",
        "economic_outcome_read",
        "prediction_authority",
        "transport_supported",
        "action_experiment_authorized",
        "live_deployment_authorized",
    ):
        if bool(permissions.get(forbidden, False)):
            raise ValueError(f"execution-only v2.2 cannot grant {forbidden}")

    return {
        "schema_version": f"{SCHEMA_VERSION}.audit",
        "valid": True,
        "canonical_spec_identity_sha256": expected_identity,
        "frozen_v2_1_bytes_valid": True,
        "implementation_hashes_valid": True,
        "runtime_contract_valid": True,
        "admission_contract_valid": True,
        "prospective_collection_enabled": False,
        "economic_outcomes_read": False,
        "operational_lifecycle_outcomes_read": True,
        "permissions": permissions,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = validate_execution_contract_v2_2(args.contract.resolve())
    text = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output is None:
        print(text, end="")
        return
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_name(f".{args.output.name}.partial")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(args.output)


if __name__ == "__main__":
    main()
