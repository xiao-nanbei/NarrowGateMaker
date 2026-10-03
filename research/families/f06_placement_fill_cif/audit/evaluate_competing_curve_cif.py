#!/usr/bin/env python3
"""Apply a pre-frozen curve-level gate to fill/cancel-ACK OOF CIFs."""

from __future__ import annotations

import argparse
import json
import math
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from data_paths import data_root
from models.audit.experiment_manifest import git_workspace_identity
from research.families.f06_placement_fill_cif import FAMILY_DOCS
from research.families.f06_placement_fill_cif.audit.full_curve_fill_cif import ROOT, _sha256
from research.families.f06_placement_fill_cif.audit.placement_fill_spec import (
    load_placement_fill_spec,
)

DEFAULT_REPORT = (
    data_root(ROOT)
    / "reports"
    / "placement_fill_full_curve_competing_cif_v4_development_20260727"
    / "report.json"
)


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
    draws = rng.choice(values, size=(int(samples), len(values)), replace=True).mean(
        axis=1
    )
    return {
        "mean": float(values.mean()),
        "lower": float(np.quantile(draws, 0.025)),
        "upper": float(np.quantile(draws, 0.975)),
        "p_gt_zero": float(np.mean(draws > 0.0)),
    }


def _contains_zero(interval: Mapping[str, float]) -> bool:
    return float(interval["lower"]) <= 0.0 <= float(interval["upper"])


def _positive_lower(interval: Mapping[str, float]) -> bool:
    return float(interval["lower"]) > 0.0


def evaluate(
    report_path: Path,
    *,
    bootstrap_samples: int | None = None,
    bootstrap_seed: int | None = None,
) -> dict[str, Any]:
    report = json.loads(report_path.read_text(encoding="utf-8"))
    spec_path = FAMILY_DOCS / "placement_fill_full_curve_competing_cif_v4_spec_20260727.json"
    spec = load_placement_fill_spec(spec_path)
    gate = spec["reporting"]["curve_level_gate"]
    samples = int(bootstrap_samples or gate["bootstrap_samples"])
    seed = int(bootstrap_seed or gate["bootstrap_seed"])
    oof_identity = report["outputs"]["oof_competing_predictions"]
    oof_path = Path(str(oof_identity["path"])).resolve()
    if _sha256(oof_path) != str(oof_identity["sha256"]):
        raise RuntimeError("competing-CIF OOF identity changed")
    oof = pd.read_parquet(oof_path)
    empirical_horizons = sorted(
        int(value)
        for value in spec["reporting"]["frozen_empirical_horizons_ms"].values()
    )
    frame = oof.loc[oof["horizon_ms"].isin(empirical_horizons)].copy()

    probability_columns = [
        "fill_probability",
        "cancel_ack_probability",
        "no_event_probability",
    ]
    target_columns = ["fill_target", "cancel_ack_target", "no_event_target"]
    baseline_columns = [
        "baseline_fill_probability",
        "baseline_cancel_ack_probability",
        "baseline_no_event_probability",
    ]
    probability_sum = frame[probability_columns].sum(axis=1)
    target_sum = frame[target_columns].sum(axis=1)
    simplex_tolerance = float(gate["probability_simplex_tolerance"])
    simplex_pass = bool(
        (frame[probability_columns].to_numpy(dtype=float) >= 0.0).all()
        and (np.abs(probability_sum - 1.0) <= simplex_tolerance).all()
        and (target_sum == 1).all()
    )

    for cause in ("fill", "cancel_ack"):
        frame[f"{cause}_brier_improvement"] = (
            frame[f"{cause}_target"]
            - frame[f"baseline_{cause}_probability"]
        ) ** 2 - (
            frame[f"{cause}_target"] - frame[f"{cause}_probability"]
        ) ** 2
        frame[f"{cause}_calibration_bias"] = (
            frame[f"{cause}_probability"] - frame[f"{cause}_target"]
        )
    model_joint = np.square(
        frame[probability_columns].to_numpy(dtype=float)
        - frame[target_columns].to_numpy(dtype=float)
    ).sum(axis=1)
    baseline_joint = np.square(
        frame[baseline_columns].to_numpy(dtype=float)
        - frame[target_columns].to_numpy(dtype=float)
    ).sum(axis=1)
    frame["joint_brier_improvement"] = baseline_joint - model_joint

    daily = (
        frame.groupby(
            ["day", "side", "inventory_role", "horizon_ms"], observed=True
        )
        .agg(
            rows=("fill_target", "size"),
            fill_events=("fill_target", "sum"),
            cancel_ack_events=("cancel_ack_target", "sum"),
            joint_brier_improvement=("joint_brier_improvement", "mean"),
            fill_brier_improvement=("fill_brier_improvement", "mean"),
            cancel_ack_brier_improvement=(
                "cancel_ack_brier_improvement",
                "mean",
            ),
            fill_calibration_bias=("fill_calibration_bias", "mean"),
            cancel_ack_calibration_bias=("cancel_ack_calibration_bias", "mean"),
        )
        .reset_index()
    )
    integrated_daily = (
        daily.groupby(["day", "side", "inventory_role"], observed=True)
        .agg(
            rows=("rows", "sum"),
            fill_events=("fill_events", "max"),
            cancel_ack_events=("cancel_ack_events", "max"),
            joint_brier_improvement=("joint_brier_improvement", "mean"),
            fill_brier_improvement=("fill_brier_improvement", "mean"),
            cancel_ack_brier_improvement=(
                "cancel_ack_brier_improvement",
                "mean",
            ),
            fill_calibration_bias=("fill_calibration_bias", "mean"),
            cancel_ack_calibration_bias=("cancel_ack_calibration_bias", "mean"),
        )
        .reset_index()
    )

    curve_rows: list[dict[str, Any]] = []
    for (side, role), group in integrated_daily.groupby(
        ["side", "inventory_role"], observed=True
    ):
        row: dict[str, Any] = {
            "side": str(side),
            "inventory_role": str(role),
            "days": int(group["day"].nunique()),
            "fill_events_at_largest_empirical_cut": int(
                group["fill_events"].sum()
            ),
            "cancel_ack_events_at_largest_empirical_cut": int(
                group["cancel_ack_events"].sum()
            ),
        }
        for index, metric in enumerate(
            (
                "joint_brier_improvement",
                "fill_brier_improvement",
                "cancel_ack_brier_improvement",
                "fill_calibration_bias",
                "cancel_ack_calibration_bias",
            )
        ):
            row[metric] = _bootstrap_mean(
                group[metric].to_numpy(),
                samples=samples,
                seed=seed + len(curve_rows) * 100 + index,
            )
        row["support_pass"] = bool(
            row["days"] >= int(gate["minimum_oof_days"])
            and row["fill_events_at_largest_empirical_cut"]
            >= int(gate["minimum_events_per_cause"])
            and row["cancel_ack_events_at_largest_empirical_cut"]
            >= int(gate["minimum_events_per_cause"])
        )
        row["proper_score_pass"] = bool(
            _positive_lower(row["joint_brier_improvement"])
            and _positive_lower(row["fill_brier_improvement"])
            and _positive_lower(row["cancel_ack_brier_improvement"])
        )
        row["calibration_pass"] = bool(
            _contains_zero(row["fill_calibration_bias"])
            and _contains_zero(row["cancel_ack_calibration_bias"])
        )
        row["curve_pass"] = bool(
            row["support_pass"]
            and row["proper_score_pass"]
            and row["calibration_pass"]
        )
        curve_rows.append(row)

    ordered = frame.sort_values(
        ["action_lifecycle_id", "horizon_ms"], kind="stable"
    )
    time_tolerance = float(gate["monotonicity_tolerance"])
    time_violations = 0
    for cause in ("fill", "cancel_ack"):
        difference = ordered.groupby(
            "action_lifecycle_id", observed=True
        )[f"{cause}_probability"].diff()
        time_violations += int((difference < -time_tolerance).sum())
    distance = frame.pivot_table(
        index=["cohort_id", "side", "inventory_role", "horizon_ms"],
        columns="action",
        values="fill_probability",
        aggfunc="first",
    )
    distance_gap = np.maximum(
        distance["current"] - distance["closer_1tick"],
        distance["farther_1tick"] - distance["current"],
    )
    distance_violations = int((distance_gap > time_tolerance).sum())
    identity_pass = bool(
        simplex_pass and time_violations == 0 and distance_violations == 0
    )
    development_pass = bool(identity_pass and all(row["curve_pass"] for row in curve_rows))

    return {
        "schema_version": "placement_competing_curve_evaluation.v1",
        "family_id": str(report["family_id"]),
        "spec": {"path": str(spec_path), "sha256": _sha256(spec_path)},
        "report": {"path": str(report_path), "sha256": _sha256(report_path)},
        "oof": oof_identity,
        "empirical_horizons_ms": empirical_horizons,
        "legacy_horizons_are_report_only": True,
        "horizon_cell_prediction_gate": False,
        "bootstrap": {"unit": "UTC day", "samples": samples, "seed": seed},
        "identity": {
            "probability_simplex_pass": simplex_pass,
            "time_monotonicity_violations": time_violations,
            "fill_distance_monotonicity_violations": distance_violations,
            "maximum_fill_distance_violation": float(
                max(0.0, distance_gap.max())
            ),
            "identity_pass": identity_pass,
        },
        "curve_diagnostics": curve_rows,
        "development_curve_gate_passed": development_pass,
        "validation_access_recommended": development_pass,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_or_live_authorization": False,
        "git": git_workspace_identity(ROOT),
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--bootstrap-samples", type=int)
    parser.add_argument("--bootstrap-seed", type=int)
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
        bootstrap_samples=args.bootstrap_samples,
        bootstrap_seed=args.bootstrap_seed,
    )
    _atomic_json(payload, output)
    print(
        json.dumps(
            {
                "family_id": payload["family_id"],
                "development_curve_gate_passed": payload[
                    "development_curve_gate_passed"
                ],
                "validation_read": False,
                "action_or_live_authorization": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
