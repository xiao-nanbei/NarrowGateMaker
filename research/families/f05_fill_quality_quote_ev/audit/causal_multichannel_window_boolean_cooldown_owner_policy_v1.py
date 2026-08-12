"""Freeze the explicit owner-route SELL M2 cooldown policy for full-path replay.

The v3 research-supported hierarchy did not pass.  This module therefore
creates a separate, permanently outcome-informed owner artifact.  BUY remains
CONTROL_85N; only SELL is refitted, on the frozen common-33 M2 Development
panel, with the small profile selected in all four outer folds.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from pathlib import Path
from typing import Any

import numpy as np

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_modeled_oof as modeled,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_persistent_policy_v3_oof as v3_oof,
)

IDENTITY = "causal_multichannel_window_boolean_cooldown_owner_policy_v1"
SCHEMA_VERSION = f"{IDENTITY}.artifact.v1"
SOURCE_SCOPE = "prefix33_raw_m2_common_support"
SOURCE_SIDE = "SELL"
SOURCE_BLOCK = "M2"
PROFILE_NAME = "small"
RANDOM_SEED = 20260812 + 9001


class OwnerPolicyFreezeError(RuntimeError):
    """Raised when the explicit owner policy cannot be frozen exactly."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("ascii")).hexdigest()


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        value = float(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _read_bound_report(root: Path, expected_manifest_sha256: str) -> dict[str, Any]:
    manifest_path = root / "manifest.json"
    report_path = root / "report.json"
    success_path = root / "_SUCCESS"
    if not all(path.is_file() for path in (manifest_path, report_path, success_path)):
        raise OwnerPolicyFreezeError("v3 inference artifact is incomplete")
    if _sha256(manifest_path) != expected_manifest_sha256:
        raise OwnerPolicyFreezeError("v3 inference manifest SHA256 drifted")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if success_path.read_text(encoding="ascii").strip() != manifest.get(
        "canonical_sha256"
    ):
        raise OwnerPolicyFreezeError("v3 inference success marker drifted")
    expected_report_sha256 = next(
        (
            str(item["sha256"])
            for item in manifest.get("files", [])
            if item.get("relative_path") == "report.json"
        ),
        "",
    )
    if _sha256(report_path) != expected_report_sha256:
        raise OwnerPolicyFreezeError("v3 inference report SHA256 drifted")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    if report.get("identity") != (
        "causal_multichannel_window_boolean_cooldown_persistent_policy_v3"
    ):
        raise OwnerPolicyFreezeError("v3 inference report identity drifted")
    return report


def fit_owner_policy(
    panel: modeled.PreparedPanel,
    *,
    config: modeled.FrozenConfig,
) -> tuple[dict[str, Any], dict[str, Any]]:
    profile = next(
        (item for item in v3_oof.DEFAULT_PROFILES if item.name == PROFILE_NAME),
        None,
    )
    if profile is None:
        raise OwnerPolicyFreezeError("frozen small profile is unavailable")
    days = tuple(config.panel_days[SOURCE_SCOPE])
    train_index = panel.metadata.index[
        (panel.metadata["side"] == SOURCE_SIDE)
        & panel.metadata["utc_day"].isin(days)
    ]
    if len(train_index) == 0 or set(panel.metadata.loc[train_index, "utc_day"]) != set(days):
        raise OwnerPolicyFreezeError("owner refit does not cover the exact common-33 days")
    policy, audit = v3_oof._fit_tree_policy(
        panel,
        config=config,
        side=SOURCE_SIDE,
        feature_block=SOURCE_BLOCK,
        train_index=train_index,
        profile=profile,
        random_seed=RANDOM_SEED,
    )
    actions = policy.choose(
        panel.features.loc[train_index, list(policy.predicate_columns)]
    )
    payload = policy.payload()
    payload["permissions"] = {
        "owner_full_path_candidate": True,
        "research_supported": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    audit_payload = {
        **asdict(audit),
        "source_scope": SOURCE_SCOPE,
        "source_days": list(days),
        "source_day_count": len(days),
        "source_opportunities": int(len(train_index)),
        "refit_nonbaseline_action_rate": float(np.mean(actions != "CONTROL_85N")),
        "refit_action_counts": {
            str(action): int(count)
            for action, count in zip(*np.unique(actions, return_counts=True), strict=True)
        },
    }
    return payload, audit_payload


def publish_owner_policy(
    output: Path,
    *,
    policy: Mapping[str, Any],
    audit: Mapping[str, Any],
    bindings: Mapping[str, Any],
    predecessor_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    destination = output.expanduser().resolve()
    if destination.exists():
        raise OwnerPolicyFreezeError(f"output already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = destination.parent / f".{destination.name}.staging-{uuid.uuid4().hex}"
    staging.mkdir()
    try:
        artifact_body = {
            "schema_version": SCHEMA_VERSION,
            "identity": IDENTITY,
            "evidence_route": "owner_risk_accepted_outcome_informed_successor",
            "selection": {
                "BUY": "CONTROL_85N",
                "SELL": "M2_boolean_small_profile_full_common33_refit",
            },
            "policy": dict(policy),
            "fit_audit": dict(audit),
            "predecessor_evidence": dict(predecessor_evidence),
            "bindings": dict(bindings),
            "permissions": {
                "research_supported": False,
                "repeated_policy_run": False,
                "action_authorized": False,
                "live_authorized": False,
            },
        }
        artifact = {
            **artifact_body,
            "canonical_sha256": _canonical_sha256(artifact_body),
        }
        artifact_path = staging / "policy.json"
        artifact_path.write_text(
            _canonical_json(_json_safe(artifact)) + "\n", encoding="ascii"
        )
        with artifact_path.open("rb") as handle:
            os.fsync(handle.fileno())
        manifest_body = {
            "schema_version": f"{SCHEMA_VERSION}.manifest",
            "identity": IDENTITY,
            "files": [
                {
                    "relative_path": "policy.json",
                    "bytes": artifact_path.stat().st_size,
                    "sha256": _sha256(artifact_path),
                }
            ],
            "permissions": artifact["permissions"],
        }
        manifest = {
            **manifest_body,
            "canonical_sha256": _canonical_sha256(manifest_body),
        }
        manifest_path = staging / "manifest.json"
        manifest_path.write_text(
            _canonical_json(manifest) + "\n", encoding="ascii"
        )
        (staging / "_SUCCESS").write_text(
            manifest["canonical_sha256"] + "\n", encoding="ascii"
        )
        os.replace(staging, destination)
        return {
            "output": str(destination),
            "policy_sha256": _sha256(destination / "policy.json"),
            "manifest_sha256": _sha256(destination / "manifest.json"),
            "canonical_sha256": artifact["canonical_sha256"],
        }
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--config-sha256", required=True)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--spec-sha256", required=True)
    parser.add_argument("--feature-manifest", type=Path, required=True)
    parser.add_argument("--feature-manifest-sha256", required=True)
    parser.add_argument("--execution-amendment", type=Path, required=True)
    parser.add_argument("--execution-amendment-sha256", required=True)
    parser.add_argument("--feature-table-glob", action="append", default=None)
    parser.add_argument("--inference-root", type=Path, required=True)
    parser.add_argument("--inference-manifest-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    globs = tuple(args.feature_table_glob or ("*.parquet", "**/*.parquet"))
    config = modeled.load_frozen_config(
        args.config,
        expected_sha256=args.config_sha256,
        spec_path=args.spec,
        expected_spec_sha256=args.spec_sha256,
        feature_manifest_path=args.feature_manifest,
        feature_manifest_sha256=args.feature_manifest_sha256,
        feature_table_globs=globs,
    )
    config, amendment = v3_oof.load_v3_execution_amendment(
        args.execution_amendment,
        expected_sha256=args.execution_amendment_sha256,
        config=config,
    )
    inference_report = _read_bound_report(
        args.inference_root.expanduser().resolve(),
        args.inference_manifest_sha256,
    )
    evidence = inference_report["hypotheses"][
        "prefix33:SELL:M2-CONTROL"
    ]["simultaneous_band"]
    if float(evidence["lcb_usdc"]) >= 0.0:
        raise OwnerPolicyFreezeError(
            "owner path must not overwrite a research-supported result"
        )
    panel, panel_bindings = modeled.load_bound_panel(
        config,
        execution_amendment=amendment,
    )
    policy, audit = fit_owner_policy(panel, config=config)
    bindings = {
        "panel": panel_bindings,
        "inference_manifest_sha256": args.inference_manifest_sha256,
        "code": {"path": str(Path(__file__).resolve()), "sha256": _sha256(Path(__file__).resolve())},
    }
    result = publish_owner_policy(
        args.output,
        policy=policy,
        audit=audit,
        bindings=bindings,
        predecessor_evidence={
            "hypothesis": "prefix33:SELL:M2-CONTROL",
            "mean_usdc": evidence["mean_usdc"],
            "simultaneous_lcb_usdc": evidence["lcb_usdc"],
            "simultaneous_ucb_usdc": evidence["ucb_usdc"],
            "research_supported": False,
        },
    )
    print(_canonical_json(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
