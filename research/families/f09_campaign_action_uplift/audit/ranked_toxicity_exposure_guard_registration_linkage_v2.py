#!/usr/bin/env python3
"""Validate the current config-to-v12 linkage for frozen toxicity guards."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = "ranked_toxicity_registration_linkage_amendment.v2"


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


def _resolve(path_value: str) -> Path:
    path = Path(str(path_value)).expanduser()
    return path.resolve() if path.is_absolute() else (ROOT / path).resolve()


def _require_identity(identity: Mapping[str, Any], label: str) -> Path:
    path = _resolve(str(identity["path"]))
    expected = str(identity["sha256"])
    actual = sha256_file(path)
    if actual != expected:
        raise ValueError(f"{label} SHA256 mismatch: {actual} != {expected}")
    return path


def _baseline_projection(config: Mapping[str, Any]) -> dict[str, bool]:
    strategy = config.get("strategy") or {}
    ml = config.get("ml") or {}
    return {
        "ml_enabled": bool(ml.get("enabled", False)),
        "q90_shadow_enabled": bool(
            strategy.get("dynamic_fill_hazard_shadow_enabled", False)
        ),
        "q90_action_enabled": bool(
            strategy.get("dynamic_fill_hazard_action_enabled", False)
        ),
        "buy_fill_selection_enabled": bool(
            strategy.get("buy_fill_selection_live_enabled", False)
        ),
    }


def validate_registration_linkage_v2(
    amendment_path: Path,
) -> dict[str, Any]:
    amendment = json.loads(amendment_path.read_text(encoding="utf-8"))
    if amendment.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unexpected registration-linkage amendment schema")
    if canonical_spec_sha256(amendment) != str(
        amendment.get("canonical_spec_identity_sha256", "")
    ):
        raise ValueError("registration-linkage canonical hash mismatch")

    frozen_specs = amendment.get("frozen_v1_registrations") or {}
    if set(frozen_specs) != {"BUY", "SELL"}:
        raise ValueError("registration linkage must bind BUY and SELL v1 specs")

    expected_bundle_identity = amendment["artifact_identities"][
        "v12_bundle_meta"
    ]
    expected_bundle_meta = _require_identity(
        expected_bundle_identity,
        "v12 bundle metadata",
    )
    for side in ("BUY", "SELL"):
        spec_path = _require_identity(
            frozen_specs[side],
            f"{side} frozen v1 registration",
        )
        spec = json.loads(spec_path.read_text(encoding="utf-8"))
        if str(spec.get("side", "")).upper() != side:
            raise ValueError(f"{side} frozen registration side drifted")
        frozen_bundle = spec["artifact_identities"]["v12_bundle_meta"]
        if _resolve(str(frozen_bundle["path"])) != expected_bundle_meta:
            raise ValueError(
                f"{side} frozen registration points to a different v12 bundle"
            )
        if str(frozen_bundle["sha256"]) != str(
            expected_bundle_identity["sha256"]
        ):
            raise ValueError(
                f"{side} frozen registration has a different v12 bundle hash"
            )

    config_path = _require_identity(
        amendment["artifact_identities"]["current_operational_config"],
        "current operational config",
    )
    config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(config, Mapping):
        raise ValueError("current operational config must be a mapping")
    model_dir_value = str((config.get("ml") or {}).get("model_dir") or "").strip()
    if not model_dir_value:
        raise ValueError("current config ml.model_dir is empty")
    configured_model_dir = _resolve(model_dir_value)
    configured_bundle_meta = (configured_model_dir / "bundle_meta.json").resolve()
    if configured_bundle_meta != expected_bundle_meta:
        raise ValueError(
            "config ml.model_dir/bundle_meta.json does not match the frozen "
            f"v12 bundle metadata: {configured_bundle_meta} != "
            f"{expected_bundle_meta}"
        )

    expected_projection = amendment.get("operational_baseline_projection") or {}
    actual_projection = _baseline_projection(config)
    if actual_projection != expected_projection:
        raise ValueError(
            "current operational baseline semantics drifted: "
            f"{actual_projection} != {expected_projection}"
        )

    for label, identity in amendment.get("supporting_identities", {}).items():
        _require_identity(identity, label)
    for label, identity in amendment.get("implementation_identity", {}).items():
        _require_identity(identity, label)

    permissions = amendment.get("permissions") or {}
    if permissions.get("mechanics_registration_linkage_valid") is not True:
        raise ValueError("linkage amendment did not grant its mechanics-only check")
    for forbidden in (
        "mechanics_read",
        "development_economic_outcome_read",
        "validation_read",
        "sealed_holdout_read",
        "action_experiment_authorized",
        "live_deployment_authorized",
    ):
        if bool(permissions.get(forbidden, False)):
            raise ValueError(f"registration linkage cannot grant {forbidden}")
    if amendment.get("economic_outcome_columns_read") != []:
        raise ValueError("registration linkage read economic outcomes")

    return {
        "schema_version": f"{SCHEMA_VERSION}.audit",
        "canonical_spec_identity_sha256": amendment[
            "canonical_spec_identity_sha256"
        ],
        "current_operational_config_path": str(config_path),
        "configured_model_dir": str(configured_model_dir),
        "configured_bundle_meta": str(configured_bundle_meta),
        "frozen_v12_bundle_meta": str(expected_bundle_meta),
        "config_to_v12_bundle_path_link_valid": True,
        "config_to_v12_bundle_hash_link_valid": True,
        "buy_sell_frozen_bundle_identity_equal": True,
        "operational_baseline_projection": actual_projection,
        "stage": "registration_linkage_v2_validated_mechanics_not_run",
        "economic_outcome_columns_read": [],
        "permissions": permissions,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--amendment", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = validate_registration_linkage_v2(args.amendment.resolve())
    text = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output is None:
        print(text, end="")
        return
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_name(f".{args.output.name}.tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(args.output)


if __name__ == "__main__":
    main()
