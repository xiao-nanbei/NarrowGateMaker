#!/usr/bin/env python3
"""Validate mechanics-only registration for the v12 ranked toxicity guards."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

import yaml

from models.audit.experiment_scorecard import score_profile_contract

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = "ranked_toxicity_exposure_guard_registration.v1"


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


def _require_identity(identity: Mapping[str, Any], label: str) -> None:
    path = Path(str(identity["path"])).expanduser()
    if not path.is_absolute():
        path = ROOT / path
    expected = str(identity["sha256"])
    actual = sha256_file(path)
    if actual != expected:
        raise ValueError(f"{label} SHA256 mismatch: {actual} != {expected}")


def validate_registration(spec_path: Path) -> dict[str, Any]:
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    if spec.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unexpected ranked-toxicity registration schema")
    if canonical_spec_sha256(spec) != str(
        spec.get("canonical_spec_identity_sha256", "")
    ):
        raise ValueError("ranked-toxicity registration canonical hash mismatch")
    side = str(spec.get("side", "")).upper()
    if side not in {"BUY", "SELL"}:
        raise ValueError("registration side must be BUY or SELL")
    expected_head = "tox_bid_10s" if side == "BUY" else "tox_ask_10s"
    if spec["prediction_contract"].get("head") != expected_head:
        raise ValueError("side-specific toxicity head is incorrect")
    if spec.get("scorecard_profile") != score_profile_contract(
        "action_execution_selective_v2"
    ):
        raise ValueError("selective v2 score profile was not frozen exactly")

    permissions = spec.get("permissions") or {}
    for forbidden in (
        "development_economic_outcome_read",
        "validation_read",
        "sealed_holdout_read",
        "action_experiment_authorized",
        "live_deployment_authorized",
    ):
        if bool(permissions.get(forbidden, False)):
            raise ValueError(f"mechanics registration cannot grant {forbidden}")
    behavior = spec["behavior_policy"]
    if behavior.get("probabilities") != {
        "baseline_permission": 0.5,
        "ranked_toxicity_guard": 0.5,
    }:
        raise ValueError("behavior propensity must be exactly 0.5/0.5")
    denominator = spec["threshold_contract"]["denominator"]
    if denominator.get("maximum_rows_per_prediction_bucket") != 1:
        raise ValueError("p90 denominator must contain at most one row per bucket")
    if denominator.get("side_specific") is not True:
        raise ValueError("p90 denominator must be side-specific")
    if denominator.get("baseline_eligible_only") is not True:
        raise ValueError("p90 denominator must be baseline-eligible")
    if denominator.get("exposure_increasing_only") is not True:
        raise ValueError("p90 denominator must be exposure-increasing")
    if spec["threshold_contract"].get("history") != "strictly_earlier_UTC_days":
        raise ValueError("threshold history must be strictly past-only")

    artifacts = spec["artifact_identities"]
    required_artifacts = {
        "operational_private_config",
        "operational_baseline_identity",
        "q90_action_suspension_operational_release",
    }
    missing_artifacts = required_artifacts.difference(artifacts)
    if missing_artifacts:
        raise ValueError(
            "ranked-toxicity registration is missing baseline provenance: "
            f"{sorted(missing_artifacts)}"
        )
    for key, identity in artifacts.items():
        _require_identity(identity, key)
    for key, identity in spec["implementation_identity"].items():
        if not isinstance(identity, Mapping):
            continue
        _require_identity(identity, key)

    config_identity = spec["artifact_identities"]["operational_private_config"]
    config_path = Path(str(config_identity["path"])).expanduser()
    if not config_path.is_absolute():
        config_path = ROOT / config_path
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    strategy = config.get("strategy") or {}
    baseline = spec["baseline_contract"]
    if baseline.get("q90_action_suspension_release_hash_bound") is not True:
        raise ValueError("q90 action suspension release must be hash-bound")
    if baseline.get("q90_terminal_riskset_repair_status") != (
        "implemented_and_verified_locally_not_deployed"
    ):
        raise ValueError("q90 terminal risk-set repair status drifted")
    if baseline.get("q90_prospective_placement_recovery_supported") is not False:
        raise ValueError("prospective q90 placement recovery remains unsupported")
    checks = {
        "ml_enabled": bool((config.get("ml") or {}).get("enabled", False)),
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
    expected_checks = {
        "ml_enabled": bool(baseline["ml_enabled"]),
        "q90_shadow_enabled": bool(baseline["q90_shadow_enabled"]),
        "q90_action_enabled": bool(baseline["q90_action_enabled"]),
        "buy_fill_selection_enabled": bool(
            baseline["buy_fill_selection_enabled"]
        ),
    }
    if checks != expected_checks:
        raise ValueError(
            f"operational baseline semantics mismatch: {checks} != {expected_checks}"
        )
    return {
        "schema_version": f"{SCHEMA_VERSION}.audit",
        "family_id": spec["family_id"],
        "side": side,
        "canonical_spec_identity_sha256": spec[
            "canonical_spec_identity_sha256"
        ],
        "artifact_hashes_valid": True,
        "operational_baseline_semantics": checks,
        "prediction_head": expected_head,
        "scorecard_profile": spec["scorecard_profile"],
        "stage": "mechanics_registered_not_run",
        "economic_outcome_columns_read": [],
        "permissions": permissions,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = validate_registration(args.spec.resolve())
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
