#!/usr/bin/env python3
"""Freeze owner OOF artifacts, code, and libraries before label-table reads."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from data_paths import data_root
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_modeled_oof as oof,
)

ROOT = Path(__file__).resolve().parents[4]
DATA_ROOT = data_root(ROOT)
DEFAULT_LABEL_MANIFEST = DATA_ROOT / (
    "reports/"
    "multiscale_ema_boolean_cooldown_duration_policy_v1_20260810/"
    "admission_manifest.json"
)
DEFAULT_FEATURE_MANIFEST = DATA_ROOT / (
    "reports/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_20260810/"
    "owner_modeled_queue_feature_panel_v1/panel_manifest.json"
)
DEFAULT_OUTPUT = Path(
    "research/families/f05_fill_quality_quote_ev/docs/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_"
    "owner_modeled_queue_v1_oof_execution_amendment_20260811.json"
)


class OofExecutionFreezeError(RuntimeError):
    """Raised when a pre-economic OOF binding is incomplete or mutable."""


def _manifest_identity(payload: Mapping[str, Any]) -> str:
    for key in ("identity", "artifact_identity", "study_identity"):
        value = payload.get(key)
        if value is not None:
            return str(value)
    raise OofExecutionFreezeError("artifact manifest has no identity")


def _artifact_binding(path: Path, *, expected_identity: str) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    payload = oof._load_json(resolved)
    observed_identity = _manifest_identity(payload)
    if observed_identity != expected_identity:
        raise OofExecutionFreezeError(
            f"artifact identity drifted for {resolved}: {observed_identity}"
        )
    return {
        "path": str(resolved),
        "sha256": oof._sha256(resolved),
        "identity": observed_identity,
    }


def _predicate_artifact_bindings(spec: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    contract = spec.get("outcome_blind_2025_input")
    if not isinstance(contract, Mapping):
        raise OofExecutionFreezeError("owner spec lacks outcome-blind predicate input")
    try:
        bundle = oof.load_2025_predicate_bundle(contract)
    except oof.ModeledOofError as exc:
        raise OofExecutionFreezeError("outcome-blind predicate binding failed") from exc

    bindings = {
        "outcome_blind_2025_predicate_bundle": {
            "path": str(bundle.path),
            "sha256": bundle.file_sha256,
            "identity": oof.PREDICATE_ARTIFACT_IDENTITY,
            "canonical_sha256": bundle.canonical_sha256,
        }
    }
    for name, artifact in sorted(bundle.artifacts.items()):
        bindings[f"outcome_blind_2025_predicate_{name}"] = {
            "path": str(artifact.path),
            "sha256": artifact.file_sha256,
            "identity": oof.PREDICATE_ARTIFACT_IDENTITY,
            "canonical_sha256": artifact.canonical_sha256,
        }
    return bindings


def freeze(
    *,
    config_path: Path,
    spec_path: Path,
    label_manifest_path: Path,
    feature_manifest_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    config = config_path.expanduser().resolve()
    spec = spec_path.expanduser().resolve()
    if oof._sha256(config) != oof.DEFAULT_CONFIG_SHA256:
        raise OofExecutionFreezeError("frozen study config SHA256 drifted")
    if oof._sha256(spec) != oof.DEFAULT_SPEC_SHA256:
        raise OofExecutionFreezeError("frozen owner spec SHA256 drifted")
    spec_payload = oof._load_json(spec)
    feature_manifest = feature_manifest_path.expanduser().resolve()
    feature_success = feature_manifest.parent / oof.FEATURE_PANEL_SUCCESS_NAME
    if (
        not feature_success.is_file()
        or feature_success.read_text(encoding="ascii").strip()
        != oof._sha256(feature_manifest)
    ):
        raise OofExecutionFreezeError("feature panel is not atomically admitted")

    code_bindings = [
        {"path": str(path), "sha256": oof._sha256(path)}
        for path in oof._required_oof_code_paths()
    ]
    artifact_bindings = {
        "frozen_config": _artifact_binding(config, expected_identity=oof.IDENTITY),
        "frozen_owner_spec": _artifact_binding(spec, expected_identity=oof.IDENTITY),
        "modeled_label_manifest": _artifact_binding(
            label_manifest_path,
            expected_identity="multiscale_ema_boolean_cooldown_duration_policy_v1",
        ),
        "feature_panel_manifest": _artifact_binding(
            feature_manifest,
            expected_identity=oof.EXPECTED_FEATURE_MANIFEST_IDENTITY,
        ),
        **_predicate_artifact_bindings(spec_payload),
    }
    payload: dict[str, Any] = {
        "schema_version": oof.EXECUTION_AMENDMENT_SCHEMA,
        "identity": oof.IDENTITY,
        "status": "frozen_before_owner_oof_economic_read",
        "artifact_bindings": artifact_bindings,
        "code_bindings": code_bindings,
        "library_versions": oof.runtime_library_versions(),
        "permissions": {
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    payload["execution_identity_sha256"] = oof._canonical_sha256(payload)
    destination = output_path.expanduser().resolve()
    if destination.exists():
        raise OofExecutionFreezeError(f"execution amendment already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.parent / f".{destination.name}.tmp-{os.getpid()}"
    with temporary.open("x", encoding="ascii") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, destination)
    return {
        "path": str(destination),
        "sha256": oof._sha256(destination),
        "execution_identity_sha256": payload["execution_identity_sha256"],
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=oof.DEFAULT_CONFIG_PATH)
    parser.add_argument("--spec", type=Path, default=oof.DEFAULT_SPEC_PATH)
    parser.add_argument("--label-manifest", type=Path, default=DEFAULT_LABEL_MANIFEST)
    parser.add_argument("--feature-manifest", type=Path, default=DEFAULT_FEATURE_MANIFEST)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    print(
        json.dumps(
            freeze(
                config_path=args.config,
                spec_path=args.spec,
                label_manifest_path=args.label_manifest,
                feature_manifest_path=args.feature_manifest,
                output_path=args.output,
            ),
            sort_keys=True,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
