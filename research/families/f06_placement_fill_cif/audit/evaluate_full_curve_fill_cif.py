#!/usr/bin/env python3
"""Evaluate a frozen full-curve placement CIF without horizon-level gates."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from data_paths import data_root
from models.audit.experiment_manifest import git_workspace_identity

ROOT = Path(__file__).resolve().parents[4]
DATA_ROOT = data_root(ROOT)
DEFAULT_REPORT = (
    DATA_ROOT
    / "reports"
    / "placement_fill_full_curve_cif_v3_development_20260727"
    / "report.json"
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(payload: Mapping[str, Any], path: Path) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def _bootstrap_mean(
    values: np.ndarray, *, samples: int, seed: int
) -> dict[str, float]:
    values = np.asarray(values, dtype=float)
    values = values[np.isfinite(values)]
    if values.size == 0:
        return {
            "mean": math.nan,
            "lower": math.nan,
            "upper": math.nan,
            "p_gt_zero": math.nan,
        }
    rng = np.random.default_rng(int(seed))
    draws = rng.choice(values, size=(int(samples), len(values)), replace=True).mean(axis=1)
    return {
        "mean": float(values.mean()),
        "lower": float(np.quantile(draws, 0.025)),
        "upper": float(np.quantile(draws, 0.975)),
        "p_gt_zero": float(np.mean(draws > 0.0)),
    }


def evaluate(
    report_path: Path,
    *,
    bootstrap_samples: int,
    bootstrap_seed: int,
    distance_tolerance: float,
) -> dict[str, Any]:
    report = json.loads(report_path.read_text(encoding="utf-8"))
    oof_identity = report["outputs"]["oof_predictions"]
    oof_path = Path(str(oof_identity["path"])).resolve()
    if _sha256(oof_path) != str(oof_identity["sha256"]):
        raise RuntimeError("full-curve OOF identity changed")
    oof = pd.read_parquet(oof_path)
    empirical_horizons = sorted(
        int(value)
        for value in report["duration_contract"]["report_quantiles"].values()
    )
    empirical = oof.loc[oof["horizon_ms"].isin(empirical_horizons)].copy()
    empirical["model_brier"] = (
        empirical["target"] - empirical["probability"]
    ) ** 2
    empirical["baseline_brier"] = (
        empirical["target"] - empirical["baseline_probability"]
    ) ** 2
    empirical["brier_improvement"] = (
        empirical["baseline_brier"] - empirical["model_brier"]
    )
    empirical["calibration_bias"] = (
        empirical["probability"] - empirical["target"]
    )

    daily = (
        empirical.groupby(
            ["day", "side", "inventory_role", "horizon_ms"], observed=True
        )
        .agg(
            rows=("target", "size"),
            events=("target", "sum"),
            observed_rate=("target", "mean"),
            predicted_rate=("probability", "mean"),
            brier_improvement=("brier_improvement", "mean"),
            calibration_bias=("calibration_bias", "mean"),
        )
        .reset_index()
    )
    cell_rows: list[dict[str, Any]] = []
    for (side, role, horizon), group in daily.groupby(
        ["side", "inventory_role", "horizon_ms"], observed=True
    ):
        cell_rows.append(
            {
                "side": str(side),
                "inventory_role": str(role),
                "horizon_ms": int(horizon),
                "days": int(group["day"].nunique()),
                "rows": int(group["rows"].sum()),
                "events": int(group["events"].sum()),
                "observed_rate": float(
                    np.average(group["observed_rate"], weights=group["rows"])
                ),
                "predicted_rate": float(
                    np.average(group["predicted_rate"], weights=group["rows"])
                ),
                "day_cluster_brier_improvement": _bootstrap_mean(
                    group["brier_improvement"].to_numpy(),
                    samples=bootstrap_samples,
                    seed=bootstrap_seed + len(cell_rows),
                ),
                "day_cluster_calibration_bias": _bootstrap_mean(
                    group["calibration_bias"].to_numpy(),
                    samples=bootstrap_samples,
                    seed=bootstrap_seed + 1000 + len(cell_rows),
                ),
            }
        )

    per_day_integrated = (
        daily.groupby(["day", "side", "inventory_role"], observed=True)
        .agg(
            brier_improvement=("brier_improvement", "mean"),
            calibration_bias=("calibration_bias", "mean"),
        )
        .reset_index()
    )
    integrated_rows: list[dict[str, Any]] = []
    for (side, role), group in per_day_integrated.groupby(
        ["side", "inventory_role"], observed=True
    ):
        integrated_rows.append(
            {
                "side": str(side),
                "inventory_role": str(role),
                "days": int(group["day"].nunique()),
                "empirical_horizons_ms": empirical_horizons,
                "day_cluster_integrated_brier_improvement": _bootstrap_mean(
                    group["brier_improvement"].to_numpy(),
                    samples=bootstrap_samples,
                    seed=bootstrap_seed + 2000 + len(integrated_rows),
                ),
                "day_cluster_integrated_calibration_bias": _bootstrap_mean(
                    group["calibration_bias"].to_numpy(),
                    samples=bootstrap_samples,
                    seed=bootstrap_seed + 3000 + len(integrated_rows),
                ),
            }
        )

    time_ordered = oof.sort_values(
        ["action_lifecycle_id", "horizon_ms"], kind="stable"
    )
    time_difference = time_ordered.groupby(
        "action_lifecycle_id", observed=True
    )["probability"].diff()
    time_violations = time_difference < -float(distance_tolerance)

    distance = oof.pivot_table(
        index=["cohort_id", "side", "inventory_role", "horizon_ms"],
        columns="action",
        values="probability",
        aggfunc="first",
    )
    distance_gap = np.maximum(
        distance["current"] - distance["closer_1tick"],
        distance["farther_1tick"] - distance["current"],
    )
    distance_violations = distance_gap > float(distance_tolerance)

    return {
        "schema_version": "placement_fill_full_curve_evaluation.v1",
        "family_id": str(report["family_id"]),
        "report": {"path": str(report_path), "sha256": _sha256(report_path)},
        "oof": oof_identity,
        "empirical_horizons_ms": empirical_horizons,
        "legacy_horizons_are_report_only": True,
        "horizon_cell_prediction_gate": False,
        "bootstrap": {
            "unit": "UTC day",
            "samples": int(bootstrap_samples),
            "seed": int(bootstrap_seed),
        },
        "cell_diagnostics": cell_rows,
        "integrated_curve_diagnostics": integrated_rows,
        "monotonicity": {
            "tolerance": float(distance_tolerance),
            "time_rows": int(time_difference.notna().sum()),
            "time_violations": int(time_violations.sum()),
            "distance_rows": int(len(distance)),
            "distance_violations": int(distance_violations.sum()),
            "distance_violation_rate": float(distance_violations.mean()),
            "maximum_distance_violation": float(max(0.0, distance_gap.max())),
        },
        "curve_level_status": "development_diagnostic_only_no_frozen_gate",
        "validation_access_allowed": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_or_live_authorization": False,
        "git": git_workspace_identity(ROOT),
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--bootstrap-samples", type=int, default=5000)
    parser.add_argument("--bootstrap-seed", type=int, default=20260727)
    parser.add_argument("--distance-tolerance", type=float, default=1e-5)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    report = args.report.expanduser().resolve()
    output = (
        args.output.expanduser().resolve()
        if args.output is not None
        else report.parent / "curve_evaluation.json"
    )
    payload = evaluate(
        report,
        bootstrap_samples=int(args.bootstrap_samples),
        bootstrap_seed=int(args.bootstrap_seed),
        distance_tolerance=float(args.distance_tolerance),
    )
    _atomic_json(payload, output)
    print(
        json.dumps(
            {
                "family_id": payload["family_id"],
                "empirical_horizons_ms": payload["empirical_horizons_ms"],
                "time_violations": payload["monotonicity"]["time_violations"],
                "distance_violations": payload["monotonicity"]["distance_violations"],
                "validation_read": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
