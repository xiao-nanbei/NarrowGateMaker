#!/usr/bin/env python3
"""Build and validate the exact F03 40-day full-path execution amendment."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = "causal_v12_1s_native_40day_full_path_ml_ab_execution_amendment.v4"
IDENTITY = "causal_v12_1s_native_40day_v9_10s_vs_1s_ml_on_full_path_v3"
DEFAULT_PRECOMMIT = ROOT / (
    "research/families/f03_causal_13_head/docs/"
    "causal_v12_1s_cadence_full_path_economic_precommit_v1_20260805.json"
)
DEFAULT_OUTPUT = ROOT / (
    "research/families/f03_causal_13_head/docs/"
    "causal_v12_1s_native_40day_full_path_ml_ab_execution_amendment_v4_20260805.json"
)
TRAINING_FEATURE_PARITY_GATE = ROOT / (
    "research/families/f03_causal_13_head/docs/"
    "causal_v12_1s_real_day_parity_training_gate_v6_20260805.json"
)


class ExecutionAmendmentError(ValueError):
    """Raised when an execution amendment is missing or not reproducible."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _load_json(path: Path, *, role: str) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise ExecutionAmendmentError(f"missing {role}: {resolved}")
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ExecutionAmendmentError(f"invalid {role}: {resolved}") from exc
    if not isinstance(payload, dict):
        raise ExecutionAmendmentError(f"{role} must be a JSON object")
    return payload


def _file_binding(path: Path, *, role: str) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise ExecutionAmendmentError(f"missing {role}: {resolved}")
    return {
        "path": str(resolved),
        "sha256": _sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.tmp-{uuid.uuid4().hex}"
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _expected_payload(
    *,
    candidate_overlay_panel_manifest: Path,
    control_overlay_panel_manifest: Path,
    precommit_path: Path,
) -> dict[str, Any]:
    # Local import avoids a module cycle: the runner imports this validator.
    from research.families.f03_causal_13_head.audit import (
        causal_v12_1s_native_40day_full_path_ml_ab as runner,
    )

    precommit, precommit_binding = runner._validate_precommit(precommit_path.resolve())
    days = list(precommit["native_development_panel"]["days"])
    candidate = runner._validate_candidate_panel(
        candidate_overlay_panel_manifest.resolve(), expected_days=days
    )
    control = runner._validate_control_sources(
        control_overlay_panel_manifest.resolve(), expected_days=days
    )
    if control["operational_config"]["sha256"] != precommit["baseline"]["config_sha256"]:
        raise ExecutionAmendmentError(
            "control source config differs from the frozen economic precommit"
        )
    trace_max = int(runner.CAMPAIGN_MAE_TRACE_MAX)
    if trace_max <= 0:
        raise ExecutionAmendmentError("campaign MAE trace capacity must be positive")

    parity_payload = _load_json(TRAINING_FEATURE_PARITY_GATE, role="training feature parity gate")
    if (
        parity_payload.get("schema_version")
        != "causal_v12_1s_real_day_parity_training_gate.v6"
        or parity_payload.get("training_authorized") is not True
        or parity_payload.get("economic_outcomes_read") is not False
        or parity_payload.get("predictions_read") is not False
        or parity_payload.get("action_authorized") is not False
        or parity_payload.get("live_authorized") is not False
        or int(parity_payload.get("feature_count", 0)) != 173
        or int(parity_payload.get("native_complete_day_count", 0)) < 2
        or int(parity_payload.get("provider_complete_day_count", 0)) < 2
    ):
        raise ExecutionAmendmentError("training feature parity gate is not admissible")

    runtime_bindings = {
        "panel_runner": _file_binding(Path(runner.__file__), role="panel runner"),
        "dual_overlay_abi": _file_binding(Path(runner.dual_abi.__file__), role="dual-overlay ABI"),
        "panel_runner_test": _file_binding(
            ROOT / "tests/test_causal_v12_1s_native_40day_full_path_ml_ab.py",
            role="panel runner test",
        ),
        "dual_overlay_test": _file_binding(
            ROOT / "tests/test_causal_v12_1s_dual_overlay_ml_ab_replay.py",
            role="dual-overlay test",
        ),
    }
    feature_dag = _file_binding(ROOT / "features/feature_dag.py", role="Feature DAG")
    payload = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": "execution_inputs_bound_outcomes_unread",
        "comparison": "candidate_1s_ml_on_minus_v9_10s_ml_on",
        "runtime_bindings": runtime_bindings,
        "frozen_inputs": {
            "precommit": precommit_binding,
            "operational_config": {
                key: control["operational_config"][key] for key in ("path", "sha256", "size_bytes")
            },
            "feature_dag": feature_dag,
            "control_overlay_panel": {
                "path": control["path"],
                "sha256": control["sha256"],
                "panel_identity_sha256": control["panel_identity_sha256"],
                "v9_model_bundle_identity_sha256": control["v9_model_bundle_identity_sha256"],
            },
            "candidate_overlay_panel": {
                "path": candidate["path"],
                "sha256": candidate["sha256"],
                "panel_identity_sha256": candidate["panel_identity_sha256"],
                "bundle_meta_sha256": candidate["bundle_meta_sha256"],
            },
            "training_feature_parity_gate": {
                **_file_binding(TRAINING_FEATURE_PARITY_GATE, role="training feature parity gate"),
                "parity_gate_identity_sha256": parity_payload["parity_gate_identity_sha256"],
                "status": parity_payload["status"],
                "feature_count": parity_payload["feature_count"],
            },
            "ordered_utc_days": days,
        },
        "campaign_mae_contract": {
            "trace_campaign_repair_max": trace_max,
            "same_value_required_in_both_arms": True,
            "zero_forbidden": True,
            "trace_cap_hit_is_failure": True,
            "missing_or_invalid_trace_is_failure": True,
            "source_trace_field": runner.CAMPAIGN_MAE_TRACE_FIELD,
            "source_trace_semantics": runner.CAMPAIGN_MAE_TRACE_SEMANTICS,
            "source_unit": "USDC",
            "per_campaign_invariant": "nonpositive_monotone_nonincreasing_running_minimum",
            "panel_reducer": "minimum_over_all_campaign_decision_rows_per_day_arm",
            "legacy_campaign_mae_alias_forbidden": True,
            "metric": "minimum_decision_visible_campaign_pnl_excursion_usdc",
            "improvement_direction": "candidate_minus_control",
            "risk_gate": "campaign_mae_avoidance_lcb_nonnegative",
        },
        "governance": {
            "route": "owner_only",
            "promotion_label": "owner_risk_accepted_promotion",
            "only_allowed_override": "fills_retention_0.80_to_1.20",
            "raw_action_alpha_v2_scorecard_preserved": True,
            "continuous_71_day_confirmation_still_required": True,
            "validation_forbidden": True,
            "sealed_holdout_forbidden": True,
        },
        "permissions": {
            "economic_outcomes_read": False,
            "development_pnl_read": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    payload["execution_identity_sha256"] = _canonical_sha256(payload)
    return payload


def build_execution_amendment(
    *,
    candidate_overlay_panel_manifest: Path,
    control_overlay_panel_manifest: Path,
    precommit_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    """Freeze exact pre-outcome inputs; refuse to replace another identity."""

    payload = _expected_payload(
        candidate_overlay_panel_manifest=candidate_overlay_panel_manifest,
        control_overlay_panel_manifest=control_overlay_panel_manifest,
        precommit_path=precommit_path,
    )
    resolved = output_path.expanduser().resolve()
    if resolved.exists():
        existing = _load_json(resolved, role="successor execution amendment")
        if existing != payload:
            raise FileExistsError(f"refusing to replace a different amendment: {resolved}")
        return existing
    _atomic_json(resolved, payload)
    return payload


def validate_execution_amendment(
    amendment_path: Path | None,
    *,
    candidate_overlay_panel_manifest: Path,
    control_overlay_panel_manifest: Path,
    precommit_path: Path,
) -> dict[str, Any]:
    """Rebuild and compare the exact amendment against current code and inputs."""

    if amendment_path is None:
        raise ExecutionAmendmentError("an exact successor execution amendment is required")
    resolved = amendment_path.expanduser().resolve()
    observed = _load_json(resolved, role="successor execution amendment")
    if observed.get("schema_version") != SCHEMA_VERSION or observed.get("identity") != IDENTITY:
        raise ExecutionAmendmentError("unsupported successor execution amendment")
    observed_identity = observed.get("execution_identity_sha256")
    without_identity = dict(observed)
    without_identity.pop("execution_identity_sha256", None)
    if observed_identity != _canonical_sha256(without_identity):
        raise ExecutionAmendmentError("execution amendment identity is not reproducible")
    expected = _expected_payload(
        candidate_overlay_panel_manifest=candidate_overlay_panel_manifest,
        control_overlay_panel_manifest=control_overlay_panel_manifest,
        precommit_path=precommit_path,
    )
    if observed != expected:
        raise ExecutionAmendmentError("execution amendment drifted from current exact inputs")
    return observed


def amendment_reference(path: Path, payload: Mapping[str, Any]) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if _load_json(resolved, role="successor execution amendment") != dict(payload):
        raise ExecutionAmendmentError("execution amendment changed after validation")
    return {
        **_file_binding(resolved, role="successor execution amendment"),
        "execution_identity_sha256": payload["execution_identity_sha256"],
        "trace_campaign_repair_max": payload["campaign_mae_contract"]["trace_campaign_repair_max"],
        "training_feature_parity_gate": dict(
            payload["frozen_inputs"]["training_feature_parity_gate"]
        ),
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("build", "validate"):
        command = sub.add_parser(name)
        command.add_argument("--candidate-overlay-panel-manifest", type=Path, required=True)
        command.add_argument("--control-overlay-panel-manifest", type=Path, required=True)
        command.add_argument("--precommit", type=Path, default=DEFAULT_PRECOMMIT)
        if name == "build":
            command.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
        else:
            command.add_argument("--amendment", type=Path, required=True)
    args = parser.parse_args(argv)
    common = {
        "candidate_overlay_panel_manifest": args.candidate_overlay_panel_manifest,
        "control_overlay_panel_manifest": args.control_overlay_panel_manifest,
        "precommit_path": args.precommit,
    }
    if args.command == "build":
        payload = build_execution_amendment(output_path=args.output, **common)
    else:
        payload = validate_execution_amendment(args.amendment, **common)
    print(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
