#!/usr/bin/env python3
"""Read-only inventory preflight for F03 inference-cadence successors.

This module deliberately has no training, scoring, artifact-writing, or PnL
entrypoint.  It binds the current causal-v12 evidence, inventories the fixed
label and cadence semantics, and reports what is still missing before any
cadence-specific retraining identity may run.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import re
from pathlib import Path
from typing import Any

from research.governance.paths import resolve_research_path

SCHEMA_VERSION = "causal_v12_cadence_successor_preflight.v1"
DESIGN_SCHEMA_VERSION = "causal_v12_cadence_successor_design.v1"
MAX_BOUND_INPUT_BYTES = 64 * 1024 * 1024
REPO_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_DESIGN_PATH = (
    REPO_ROOT / "research/families/f03_causal_13_head/docs/"
    "causal_v12_cadence_successor_preflight_v1_design_20260804.json"
)

EXPECTED_ARTIFACT_KEYS = {
    "baseline_identity",
    "baseline_pointer",
    "bundle_meta",
    "feature_generator",
    "feature_manifest",
    "live_signal_implementation",
    "native_transport_spec",
    "postfit_native_spec",
    "training_spec",
    "training_summary",
}
FORBIDDEN_BOUND_PATH_PARTS = {
    "campaigns",
    "fills_",
    "full_path_ml_ab",
    "markout",
    "order_outcomes",
    "pnl",
    "quote_decisions",
    "reward",
    "trades.csv",
}
EXPECTED_HEADS = (
    "dir_10s",
    "ret_10s",
    "vol_10s",
    "dir_30s",
    "ret_30s",
    "vol_30s",
    "dir_60s",
    "ret_60s",
    "vol_60s",
    "tox_bid_5s",
    "tox_ask_5s",
    "tox_bid_10s",
    "tox_ask_10s",
)


class PreflightError(ValueError):
    """Raised when a bound identity or design invariant fails closed."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _resolve_path(raw_path: str) -> Path:
    return resolve_research_path(raw_path, require_exists=False)


def _load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise PreflightError(f"bound input is missing: {path}")
    if path.stat().st_size > MAX_BOUND_INPUT_BYTES:
        raise PreflightError(f"bound input exceeds bounded read limit: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise PreflightError(f"bound JSON must contain an object: {path}")
    return payload


def _require_exact_keys(actual: set[str], expected: set[str], *, name: str) -> None:
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise PreflightError(f"{name} keys mismatch: missing={missing} extra={extra}")


def _validate_bound_artifacts(
    design: dict[str, Any],
) -> dict[str, Path]:
    artifacts = design.get("artifact_identities")
    if not isinstance(artifacts, dict):
        raise PreflightError("artifact_identities must be an object")
    _require_exact_keys(set(artifacts), EXPECTED_ARTIFACT_KEYS, name="artifact_identities")

    resolved: dict[str, Path] = {}
    for name, identity in artifacts.items():
        if not isinstance(identity, dict):
            raise PreflightError(f"artifact identity must be an object: {name}")
        _require_exact_keys(set(identity), {"path", "sha256"}, name=name)
        raw_path = str(identity["path"])
        lowered = raw_path.lower()
        if any(part in lowered for part in FORBIDDEN_BOUND_PATH_PARTS):
            raise PreflightError(f"economic/lifecycle outcome input is forbidden: {raw_path}")
        path = _resolve_path(raw_path)
        observed = sha256_file(path)
        expected = str(identity["sha256"])
        if observed != expected:
            raise PreflightError(
                f"bound artifact hash mismatch for {name}: expected={expected} observed={observed}"
            )
        resolved[name] = path
    return resolved


def _literal_assignment(path: Path, name: str) -> Any:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        if any(isinstance(target, ast.Name) and target.id == name for target in targets):
            try:
                return ast.literal_eval(node.value)
            except (TypeError, ValueError) as exc:
                raise PreflightError(f"{name} is not a literal assignment in {path}") from exc
    raise PreflightError(f"missing literal assignment {name} in {path}")


def _head_contract(name: str) -> dict[str, Any]:
    match = re.fullmatch(r"(dir|ret|vol)_(10|30|60)s", name)
    if match:
        family, horizon_text = match.groups()
        horizon_s = int(horizon_text)
        if family == "vol":
            return {
                "name": name,
                "family": "fixed_forward_absolute_price_variance",
                "fixed_estimand_horizon_s": horizon_s,
                "maximum_future_dependency_s": horizon_s,
                "fill_conditioned": False,
            }
        return {
            "name": name,
            "family": f"fill_conditioned_{family}",
            "reach_window_s": horizon_s,
            "post_fill_markout_horizon_s": horizon_s,
            "decision_outcome_span_s": [horizon_s, 2 * horizon_s],
            "maximum_future_dependency_s": 2 * horizon_s,
            "fill_conditioned": True,
        }

    match = re.fullmatch(r"tox_(bid|ask)_(5|10)s", name)
    if match:
        side, horizon_text = match.groups()
        horizon_s = int(horizon_text)
        return {
            "name": name,
            "family": "fill_conditioned_side_adverse_markout",
            "side": side.upper(),
            "reach_window_s": horizon_s,
            "post_fill_markout_horizon_s": horizon_s,
            "decision_outcome_span_s": [horizon_s, 2 * horizon_s],
            "maximum_future_dependency_s": 2 * horizon_s,
            "fill_conditioned": True,
        }
    raise PreflightError(f"unsupported causal-v12 head identity: {name}")


def _validate_design(design: dict[str, Any]) -> None:
    if design.get("schema_version") != DESIGN_SCHEMA_VERSION:
        raise PreflightError("unsupported cadence-successor design schema")
    if design.get("identity") != "causal_v12_cadence_successor_preflight_v1":
        raise PreflightError("unexpected cadence-successor design identity")

    boundaries = design.get("authority_boundaries")
    expected_boundaries = {
        "action_authorized": False,
        "artifacts_mutated": False,
        "economic_outcomes_read": False,
        "live_authorized": False,
        "model_training_executed": False,
        "prediction_outcomes_read": False,
        "registry_modified": False,
    }
    if boundaries != expected_boundaries:
        raise PreflightError("authority_boundaries must remain fully read-only")

    current = design.get("current_reference")
    if not isinstance(current, dict):
        raise PreflightError("current_reference must be an object")
    if int(current.get("inference_cadence_ms", 0)) != 10_000:
        raise PreflightError("current reference must remain canonical 10s")
    if current.get("head_names") != list(EXPECTED_HEADS):
        raise PreflightError("current reference head_names mismatch")
    if current.get("label_horizon_change_in_scope") is not False:
        raise PreflightError("cadence preflight may not change label horizons")

    candidates = design.get("candidate_cadence_identities")
    if not isinstance(candidates, list) or len(candidates) != 3:
        raise PreflightError("exactly three cadence successor identities are required")
    cadences = [int(candidate.get("inference_cadence_ms", 0)) for candidate in candidates]
    if cadences != [1_000, 2_000, 5_000]:
        raise PreflightError("candidate cadences must be frozen in 1s/2s/5s order")
    for field in ("identity", "feature_dag_id", "feature_semantics_identity"):
        values = [str(candidate.get(field, "")) for candidate in candidates]
        if any(not value for value in values) or len(set(values)) != len(values):
            raise PreflightError(f"candidate {field} values must be non-empty and unique")
    for candidate in candidates:
        if candidate.get("label_contract_identity") != current.get("label_contract_identity"):
            raise PreflightError("cadence candidates must preserve the label contract")
        if candidate.get("label_horizon_change_in_scope") is not False:
            raise PreflightError("candidate may not alter label horizons")
        if candidate.get("outcome_selected") is not False:
            raise PreflightError("cadence identities may not be selected using outcomes")

    diagnostic = design.get("historical_panel_policy")
    if not isinstance(diagnostic, dict):
        raise PreflightError("historical_panel_policy must be an object")
    if diagnostic.get("all_2026_panels_diagnostic_only") is not True:
        raise PreflightError("previously read 2026 panels must remain diagnostic-only")
    if diagnostic.get("independent_confirmation") is not False:
        raise PreflightError("no 2026 historical panel may claim confirmation authority")


def validate_design(design: dict[str, Any]) -> None:
    """Validate the frozen design without resolving bound host artifacts."""
    _validate_design(design)


def _inventory_feature_basis(
    design: dict[str, Any], training_summary: dict[str, Any]
) -> list[dict[str, Any]]:
    metrics = training_summary.get("metrics")
    if not isinstance(metrics, list) or len(metrics) != len(EXPECTED_HEADS):
        raise PreflightError("training summary must contain exactly 13 metric records")
    feature_sets = [set(metric.get("feature_cols", [])) for metric in metrics]
    if not feature_sets or any(not values for values in feature_sets):
        raise PreflightError("each metric must bind a non-empty feature schema")
    common_features = set.intersection(*feature_sets)

    groups = design.get("feature_basis_inventory")
    if not isinstance(groups, list) or not groups:
        raise PreflightError("feature_basis_inventory must be non-empty")
    result: list[dict[str, Any]] = []
    for group in groups:
        if group.get("is_estimand_horizon") is not False:
            raise PreflightError("feature basis windows cannot be estimand horizons")
        examples = list(group.get("required_feature_examples", []))
        missing = sorted(set(examples) - common_features)
        if missing:
            raise PreflightError(
                f"feature-basis examples missing for {group.get('name')}: {missing}"
            )
        result.append(
            {
                "name": str(group["name"]),
                "window_seconds": list(group["window_seconds"]),
                "required_feature_examples": examples,
                "is_estimand_horizon": False,
                "status": "retained_basis_not_selected_by_this_preflight",
            }
        )
    return result


def _panel_inventory(
    feature_manifest: dict[str, Any],
    native_transport_spec: dict[str, Any],
    postfit_native_spec: dict[str, Any],
) -> dict[str, Any]:
    split = feature_manifest.get("split")
    if not isinstance(split, dict):
        raise PreflightError("feature manifest split is missing")
    transport_panels = native_transport_spec.get("panels")
    postfit_panels = postfit_native_spec.get("panels")
    if not isinstance(transport_panels, list) or len(transport_panels) != 2:
        raise PreflightError("native transport spec must contain two historical panels")
    if not isinstance(postfit_panels, list) or len(postfit_panels) != 2:
        raise PreflightError("postfit native spec must contain two diagnostic panels")

    panel_rows: list[dict[str, Any]] = []
    for panel in [*transport_panels, *postfit_panels]:
        if panel.get("independent_confirmation") is not False:
            raise PreflightError("historical panel incorrectly claims confirmation authority")
        days = list(panel.get("days", []))
        if not days or len(days) != len(set(days)):
            raise PreflightError("historical panel days must be non-empty and unique")
        panel_rows.append(
            {
                "role": str(panel.get("role", "")),
                "days": days,
                "day_count": len(days),
                "diagnostic_only": True,
                "independent_confirmation": False,
            }
        )

    if transport_panels[0].get("days") != split.get("validation"):
        raise PreflightError("transport development days drifted from feature validation")
    if transport_panels[1].get("days") != split.get("test"):
        raise PreflightError("late diagnostic days drifted from feature test")

    all_days = [day for panel in panel_rows for day in panel["days"]]
    duplicate_days = sorted({day for day in all_days if all_days.count(day) > 1})
    if duplicate_days:
        raise PreflightError(f"historical diagnostic panels overlap: {duplicate_days}")
    return {
        "panels": panel_rows,
        "total_previously_read_2026_days": len(all_days),
        "all_2026_panels_diagnostic_only": True,
        "independent_confirmation_available": False,
    }


def _candidate_blockers(design: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    inventory: list[dict[str, Any]] = []
    blockers: list[str] = []
    required_artifacts = (
        "cadence_feature_generator",
        "cadence_feature_manifest",
        "cadence_feature_parity_contract",
        "cadence_source_manifest",
        "cadence_training_spec",
        "model_output_identity",
    )
    for candidate in design["candidate_cadence_identities"]:
        missing = [
            field
            for field in required_artifacts
            if not candidate.get("required_retraining_artifacts", {}).get(field)
        ]
        if candidate.get("overlapping_label_training_contract_frozen") is not True:
            missing.append("overlapping_label_training_contract")
        if candidate.get("chronological_calibration_contract_frozen") is not True:
            missing.append("chronological_calibration_contract")
        identity = str(candidate["identity"])
        blockers.extend(f"{identity}:{item}_missing" for item in missing)
        inventory.append(
            {
                "identity": identity,
                "inference_cadence_ms": int(candidate["inference_cadence_ms"]),
                "feature_dag_id": str(candidate["feature_dag_id"]),
                "feature_semantics_identity": str(candidate["feature_semantics_identity"]),
                "label_contract_identity": str(candidate["label_contract_identity"]),
                "label_horizon_change_in_scope": False,
                "outcome_selected": False,
                "missing_retraining_artifacts": missing,
                "retraining_execution_eligible": not missing,
            }
        )
    return inventory, blockers


def run_preflight(
    design_path: Path | str = DEFAULT_DESIGN_PATH,
) -> dict[str, Any]:
    """Return an in-memory audit report; never write or train anything."""
    design_file = _resolve_path(str(design_path))
    design = _load_json(design_file)
    _validate_design(design)
    paths = _validate_bound_artifacts(design)

    train_spec = _load_json(paths["training_spec"])
    training_summary = _load_json(paths["training_summary"])
    bundle_meta = _load_json(paths["bundle_meta"])
    feature_manifest = _load_json(paths["feature_manifest"])
    baseline_pointer = _load_json(paths["baseline_pointer"])
    native_transport_spec = _load_json(paths["native_transport_spec"])
    postfit_native_spec = _load_json(paths["postfit_native_spec"])

    if tuple(train_spec.get("head_names", [])) != EXPECTED_HEADS:
        raise PreflightError("training spec head identity mismatch")
    if tuple(training_summary.get("targets", [])) != EXPECTED_HEADS:
        raise PreflightError("training summary target identity mismatch")
    if tuple(bundle_meta.get("targets", [])) != EXPECTED_HEADS:
        raise PreflightError("bundle target identity mismatch")
    metric_names = tuple(metric.get("name") for metric in training_summary["metrics"])
    if metric_names != EXPECTED_HEADS:
        raise PreflightError("training metric order/identity mismatch")

    feature_bucket_ms = int(feature_manifest.get("feature_bucket_ms", 0))
    feature_ready_offset_ms = int(feature_manifest.get("feature_ready_offset_ms", 0))
    if feature_bucket_ms != 10_000 or feature_ready_offset_ms != 10_000:
        raise PreflightError("current v12 feature/ready cadence is not canonical 10s")
    if any(
        int(metric.get("feature_bucket_ms", 0)) != 10_000 for metric in training_summary["metrics"]
    ):
        raise PreflightError("per-head training cadence is not uniformly 10s")

    generator_cadence_s = int(_literal_assignment(paths["feature_generator"], "RESAMPLE_SEC"))
    label_horizons = list(_literal_assignment(paths["feature_generator"], "LABEL_HORIZONS"))
    toxicity_horizons = list(_literal_assignment(paths["feature_generator"], "TOXICITY_HORIZONS"))
    if generator_cadence_s != 10:
        raise PreflightError("feature generator RESAMPLE_SEC drifted from 10s")
    if label_horizons != [10, 30, 60] or toxicity_horizons != [5, 10]:
        raise PreflightError("feature generator label horizons drifted")

    signal_text = paths["live_signal_implementation"].read_text(encoding="utf-8")
    for fragment in design["current_reference"]["runtime_source_fragments"]:
        if fragment not in signal_text:
            raise PreflightError(f"live 10s cadence source evidence missing: {fragment}")

    if feature_manifest.get("feature_dag_id") != design["current_reference"]["feature_dag_id"]:
        raise PreflightError("current feature DAG identity mismatch")
    if int(feature_manifest.get("feature_semantics_version", 0)) != 6:
        raise PreflightError("current feature semantics version mismatch")
    if int(feature_manifest.get("label_semantics_version", 0)) != 3:
        raise PreflightError("current label semantics version mismatch")
    if feature_manifest.get("label_window_semantics") != "left_closed_right_open_[t,t+h)":
        raise PreflightError("current label-window semantics mismatch")

    expected_baseline = design["current_reference"]["operational_baseline"]
    for field, expected in expected_baseline.items():
        if baseline_pointer.get(field) != expected:
            raise PreflightError(f"operational baseline drifted at {field}")
    if baseline_pointer.get("identity_sha256") != sha256_file(paths["baseline_identity"]):
        raise PreflightError("baseline pointer does not bind the supplied identity")

    fit_days = list(train_spec.get("fit_days", []))
    selection_days = list(train_spec.get("selection_days", []))
    refit_days = list(train_spec.get("refit_days", []))
    if not fit_days or not selection_days or not refit_days:
        raise PreflightError("current train-only panel identity is incomplete")
    if any(not day.startswith("2025-") for day in refit_days):
        raise PreflightError("current refit identity unexpectedly includes post-2025 days")

    head_contracts = [_head_contract(name) for name in EXPECTED_HEADS]
    maximum_future_dependency_s = max(
        int(head["maximum_future_dependency_s"]) for head in head_contracts
    )
    feature_basis = _inventory_feature_basis(design, training_summary)
    panel_inventory = _panel_inventory(feature_manifest, native_transport_spec, postfit_native_spec)
    candidate_inventory, retraining_blockers = _candidate_blockers(design)

    promotion_blockers = [
        "untouched_chronological_confirmation_panel_not_frozen",
        "cadence_specific_full_path_ml_off_on_identity_not_frozen",
        "continuous_state_accounting_and_replay_parity_not_bound",
    ]
    deployment_blockers = [
        "cadence_specific_live_scheduler_and_runtime_abi_not_implemented",
        "candidate_bundle_and_config_hashes_not_available",
        "rollback_and_runtime_preflight_not_frozen",
    ]

    return {
        "schema_version": SCHEMA_VERSION,
        "identity": str(design["identity"]),
        "design_path": str(design_file),
        "design_sha256": sha256_file(design_file),
        "preflight_implementation_sha256": sha256_file(Path(__file__).resolve()),
        "audit_scope": {
            "economic_outcomes_read": False,
            "prediction_outcomes_read": False,
            "artifacts_mutated": False,
            "model_training_executed": False,
            "current_live_baseline_preserved": True,
        },
        "current_v12": {
            "training_experiment_id": str(train_spec["experiment_id"]),
            "feature_dag_id": str(feature_manifest["feature_dag_id"]),
            "feature_semantics_version": int(feature_manifest["feature_semantics_version"]),
            "label_semantics_version": int(feature_manifest["label_semantics_version"]),
            "feature_bucket_ms": feature_bucket_ms,
            "feature_ready_offset_ms": feature_ready_offset_ms,
            "live_inference_cadence_ms": 10_000,
            "inference_behavior": "completed_10s_bucket_then_sample_and_hold",
            "head_contracts": head_contracts,
            "maximum_label_future_dependency_s": maximum_future_dependency_s,
            "feature_basis_windows": feature_basis,
            "feature_basis_windows_are_estimand_horizons": False,
            "train_only_panel": {
                "fit_days": fit_days,
                "fit_day_count": len(fit_days),
                "selection_days": selection_days,
                "selection_day_count": len(selection_days),
                "refit_days": refit_days,
                "refit_day_count": len(refit_days),
            },
            "operational_baseline": {
                "baseline_id": baseline_pointer["baseline_id"],
                "ml_enabled": bool(baseline_pointer["ml_enabled"]),
                "dynamic_fill_hazard_action_enabled": bool(
                    baseline_pointer["dynamic_fill_hazard_action_enabled"]
                ),
                "buy_fill_selection_live_enabled": bool(
                    baseline_pointer["buy_fill_selection_live_enabled"]
                ),
                "unchanged_by_preflight": True,
            },
        },
        "historical_panel_inventory": panel_inventory,
        "cadence_identities": {
            "reference": design["current_reference"]["identity"],
            "candidates": candidate_inventory,
            "candidate_selection_performed": False,
            "label_horizon_change_in_scope": False,
        },
        "blockers": {
            "retraining": retraining_blockers,
            "promotion": promotion_blockers,
            "deployment": deployment_blockers,
        },
        "inventory_complete": True,
        "retraining_execution_eligible": not retraining_blockers,
        "prediction_authorized": False,
        "action_authorized": False,
        "live_authorized": False,
        "canonical_identity_sha256": canonical_sha256(
            {
                "design_sha256": sha256_file(design_file),
                "current_reference": design["current_reference"]["identity"],
                "candidates": [row["identity"] for row in candidate_inventory],
                "historical_panels": panel_inventory,
                "blockers": {
                    "retraining": retraining_blockers,
                    "promotion": promotion_blockers,
                    "deployment": deployment_blockers,
                },
            }
        ),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--design", type=Path, default=DEFAULT_DESIGN_PATH)
    parser.add_argument("--compact", action="store_true")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    report = run_preflight(args.design)
    print(
        json.dumps(
            report,
            indent=None if args.compact else 2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
