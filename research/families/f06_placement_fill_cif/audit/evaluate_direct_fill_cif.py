#!/usr/bin/env python3
"""Evaluate a frozen direct placement-fill artifact on one later panel."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

from research.families.f06_placement_fill_cif.audit.direct_fill_cif import (
    HORIZONS_MS,
    ROOT,
    _apply_baseline,
    _apply_calibrator,
    _apply_cell_offsets,
    _apply_cell_values,
    _apply_prediction_gates,
    _atomic_json,
    _metrics,
    _pathwise_prediction_violations,
    _predict_raw,
    _sha256,
    expand_placement_panel,
    placement_input_columns,
)
from models.audit.experiment_manifest import (
    git_workspace_identity,
    write_code_checkpoint,
)
from research.families.f06_placement_fill_cif.audit.placement_fill_spec import load_placement_fill_spec


def _load_panel(
    panel_dir: Path,
    *,
    panel_name: str,
    days: Sequence[str],
) -> tuple[pd.DataFrame, Mapping[str, Any]]:
    manifest_path = panel_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("status") != f"{panel_name}_panel_complete":
        raise RuntimeError(f"{panel_name} panel is incomplete")
    if manifest.get("active_order_keep_replace") != "separate_not_built":
        raise RuntimeError("evaluation panel mixed the active-order estimand")
    frames = []
    for day in days:
        directory = panel_dir / "partitions" / f"day={day}"
        part_manifest = json.loads(
            (directory / "manifest.json").read_text(encoding="utf-8")
        )
        if part_manifest.get("panel") != panel_name:
            raise RuntimeError(f"{day} belongs to another panel")
        panel_path = directory / "placement.parquet"
        if part_manifest.get("panel_sha256") != _sha256(panel_path):
            raise RuntimeError(f"{day} placement partition identity changed")
        frames.append(pd.read_parquet(panel_path, columns=placement_input_columns()))
    return pd.concat(frames, ignore_index=True), manifest


def _score(
    panel: pd.DataFrame,
    artifact: Mapping[str, Any],
) -> pd.DataFrame:
    scored_parts = []
    for side in ("BUY", "SELL"):
        group = panel.loc[panel["side"].eq(side)].copy()
        bundle = artifact["models"][side]
        raw = _predict_raw(bundle["model"], group)
        probability = _apply_calibrator(raw, bundle["calibrator"])
        probability = _apply_cell_offsets(
            group, probability, bundle["cell_offsets"]
        )
        group["raw_probability"] = raw
        group["probability"] = probability
        group["baseline_probability"] = _apply_baseline(
            group, bundle["baseline_rates"]
        )
        group["empirical_abs_bias_tolerance"] = _apply_cell_values(
            group, bundle["empirical_drift_envelopes"]
        )
        scored_parts.append(group)
    return pd.concat(scored_parts, ignore_index=True)


def _action_resolution(predictions: pd.DataFrame) -> pd.DataFrame:
    """Measure whether the fitted surface resolves the paired one-tick actions."""

    frame = predictions[
        [
            "cohort_id",
            "day",
            "side",
            "inventory_role",
            "horizon_ms",
            "action",
            "probability",
        ]
    ].copy()
    for column in ("side", "inventory_role", "action"):
        frame[column] = frame[column].astype(str)
    wide = frame.pivot_table(
        index=["cohort_id", "day", "side", "inventory_role", "horizon_ms"],
        columns="action",
        values="probability",
        aggfunc="first",
        observed=True,
    ).dropna(subset=["closer_1tick", "current", "farther_1tick"])
    rows: list[dict[str, Any]] = []
    for side in ("BUY", "SELL"):
        for role in ("opener", "add", "reducing"):
            for horizon_ms in HORIZONS_MS:
                mask = (
                    (wide.index.get_level_values("side") == side)
                    & (wide.index.get_level_values("inventory_role") == role)
                    & (wide.index.get_level_values("horizon_ms") == int(horizon_ms))
                )
                group = wide.loc[mask]
                delta = (
                    group["closer_1tick"] - group["farther_1tick"]
                ).to_numpy(dtype=float)
                rows.append(
                    {
                        "side": side,
                        "inventory_role": role,
                        "horizon_ms": int(horizon_ms),
                        "rows": int(delta.size),
                        "nonzero_action_delta_fraction": float(
                            np.mean(np.abs(delta) > 1e-12)
                        ),
                        "mean_closer_minus_farther_probability": float(
                            np.mean(delta)
                        ),
                        "median_closer_minus_farther_probability": float(
                            np.quantile(delta, 0.50)
                        ),
                        "p90_closer_minus_farther_probability": float(
                            np.quantile(delta, 0.90)
                        ),
                        "p99_closer_minus_farther_probability": float(
                            np.quantile(delta, 0.99)
                        ),
                    }
                )
    return pd.DataFrame(rows)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--development-report", type=Path, required=True)
    parser.add_argument("--panel-dir", type=Path, required=True)
    parser.add_argument(
        "--panel-name",
        choices=("validation", "sealed_holdout", "late_evidence"),
        required=True,
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--bootstrap-samples", type=int, default=2000)
    parser.add_argument("--bootstrap-seed", type=int, default=20260727)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    for name in ("spec", "development_report", "panel_dir", "output_dir"):
        setattr(args, name, getattr(args, name).expanduser().resolve())
    spec = load_placement_fill_spec(args.spec)
    development_report = json.loads(
        args.development_report.read_text(encoding="utf-8")
    )
    if development_report.get("family_id") != spec.get("family_id"):
        raise RuntimeError("Development report belongs to another family")
    if development_report.get("spec_sha256") != _sha256(args.spec):
        raise RuntimeError("Development report belongs to another spec")
    if not bool(development_report.get("development_prediction_gate_passed")):
        raise RuntimeError("Development did not unlock later evaluation")
    if bool(development_report.get("action_or_live_authorization")):
        raise RuntimeError("prediction report unexpectedly authorizes live action")
    artifact_identity = development_report["outputs"]["artifact"]
    artifact_path = Path(str(artifact_identity["path"])).resolve()
    if _sha256(artifact_path) != str(artifact_identity["sha256"]):
        raise RuntimeError("Development artifact identity changed")
    artifact = joblib.load(artifact_path)
    if artifact.get("schema_version") != "direct_placement_fill_cif.v4":
        raise RuntimeError("evaluation requires the v4 frozen artifact")

    panel_name = str(args.panel_name)
    days = [str(day) for day in spec["panels"][panel_name]["days"]]
    wide, panel_manifest = _load_panel(
        args.panel_dir, panel_name=panel_name, days=days
    )
    if int(wide["monotonicity_violation_count"].sum()) != 0:
        raise RuntimeError("evaluation panel violated placement monotonicity")
    long = expand_placement_panel(wide)
    predictions = _score(long, artifact)
    violations = _pathwise_prediction_violations(predictions)
    action_resolution = _action_resolution(predictions)

    metric_rows = []
    for side in ("BUY", "SELL"):
        for role in ("opener", "add", "reducing"):
            for horizon_ms in HORIZONS_MS:
                group = predictions.loc[
                    predictions["side"].eq(side)
                    & predictions["inventory_role"].eq(role)
                    & predictions["horizon_ms"].eq(int(horizon_ms))
                ]
                metric_rows.append(
                    {
                        "side": side,
                        "inventory_role": role,
                        "horizon_ms": int(horizon_ms),
                        **_metrics(
                            group,
                            bootstrap_seed=int(args.bootstrap_seed)
                            + len(metric_rows),
                            bootstrap_samples=int(args.bootstrap_samples),
                        ),
                    }
                )
    metrics = _apply_prediction_gates(
        pd.DataFrame(metric_rows),
        spec["prediction_gates"],
        empirical_calibration=True,
    )
    passed = bool(metrics["prediction_gate_passed"].all()) and violations == 0

    args.output_dir.mkdir(parents=True, exist_ok=True)
    code = git_workspace_identity(ROOT)
    checkpoint = write_code_checkpoint(
        args.output_dir / "code_checkpoint", repo_root=ROOT, code_identity=code
    )
    metrics_path = args.output_dir / "metrics.parquet"
    predictions_path = args.output_dir / "predictions.parquet"
    action_resolution_path = args.output_dir / "action_resolution.parquet"
    metrics.to_parquet(metrics_path, index=False, compression="zstd")
    action_resolution.to_parquet(
        action_resolution_path, index=False, compression="zstd"
    )
    predictions[
        [
            "cohort_id",
            "day",
            "side",
            "inventory_role",
            "action",
            "horizon_ms",
            "distance_ticks",
            "target",
            "raw_probability",
            "probability",
            "baseline_probability",
            "empirical_abs_bias_tolerance",
        ]
    ].to_parquet(predictions_path, index=False, compression="zstd")
    pass_field = f"{panel_name}_prediction_gate_passed"
    report = {
        "schema_version": "direct_placement_fill_cif_evaluation.v1",
        "family_id": str(spec["family_id"]),
        "panel_name": panel_name,
        "days": days,
        "rows_wide": int(len(wide)),
        "rows_model": int(len(long)),
        "prediction_cells_passed": int(metrics["prediction_gate_passed"].sum()),
        pass_field: passed,
        "prediction_qualification": "prediction_transfer_shadow_gate",
        "predicted_pathwise_monotonicity_violations": int(violations),
        "development_prediction_gate_passed": True,
        "validation_read": bool(panel_name in {"validation", "sealed_holdout", "late_evidence"}),
        "sealed_holdout_read": bool(panel_name in {"sealed_holdout", "late_evidence"}),
        "action_or_live_authorization": False,
        "absolute_probability_ev_authorized": False,
        "action_resolution_is_diagnostic_only": True,
        "minimum_nonzero_action_delta_fraction": float(
            action_resolution["nonzero_action_delta_fraction"].min()
        ),
        "maximum_nonzero_action_delta_fraction": float(
            action_resolution["nonzero_action_delta_fraction"].max()
        ),
        "spec_sha256": _sha256(args.spec),
        "development_report": {
            "path": str(args.development_report),
            "sha256": _sha256(args.development_report),
        },
        "artifact": artifact_identity,
        "panel_manifest": {
            "path": str(args.panel_dir / "manifest.json"),
            "sha256": _sha256(args.panel_dir / "manifest.json"),
            "run_identity_sha256": panel_manifest["run_identity_sha256"],
        },
        "git": code,
        "checkpoint": checkpoint,
        "outputs": {
            "artifact": artifact_identity,
            "action_resolution": {
                "path": str(action_resolution_path),
                "sha256": _sha256(action_resolution_path),
            },
            "metrics": {"path": str(metrics_path), "sha256": _sha256(metrics_path)},
            "predictions": {
                "path": str(predictions_path),
                "sha256": _sha256(predictions_path),
            },
        },
    }
    _atomic_json(report, args.output_dir / "report.json")
    print(
        json.dumps(
            {
                pass_field: passed,
                "prediction_cells_passed": int(
                    metrics["prediction_gate_passed"].sum()
                ),
                "action_or_live_authorization": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
