#!/usr/bin/env python3
"""Evaluate the Development-only policy-clock placement fill family."""

from __future__ import annotations

import argparse
import json
import math
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from models.audit.experiment_manifest import git_workspace_identity
from research.families.f06_placement_fill_cif.audit.full_curve_fill_cif import ROOT, _sha256
from research.families.f06_placement_fill_cif.audit.placement_fill_spec import load_placement_fill_spec


def _atomic_json(payload: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def _bootstrap_mean(
    values: np.ndarray, *, samples: int, seed: int
) -> dict[str, float]:
    finite = np.asarray(values, dtype=float)
    finite = finite[np.isfinite(finite)]
    if finite.size == 0:
        return {
            "mean": math.nan,
            "lower": math.nan,
            "upper": math.nan,
            "p_gt_zero": math.nan,
        }
    rng = np.random.default_rng(int(seed))
    draws = rng.choice(
        finite, size=(int(samples), finite.size), replace=True
    ).mean(axis=1)
    return {
        "mean": float(finite.mean()),
        "lower": float(np.quantile(draws, 0.025)),
        "upper": float(np.quantile(draws, 0.975)),
        "p_gt_zero": float(np.mean(draws > 0.0)),
    }


def _contains_zero(interval: Mapping[str, float]) -> bool:
    return float(interval["lower"]) <= 0.0 <= float(interval["upper"])


def _positive_lower(interval: Mapping[str, float]) -> bool:
    return float(interval["lower"]) > 0.0


def _verified_frame(identity: Mapping[str, Any]) -> pd.DataFrame:
    path = Path(str(identity["path"])).expanduser().resolve()
    if _sha256(path) != str(identity["sha256"]):
        raise RuntimeError(f"evaluation input identity changed: {path}")
    return pd.read_parquet(path)


def _evaluate_fill_curves(
    frame: pd.DataFrame,
    *,
    gate: Mapping[str, Any],
    samples: int,
    seed: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
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
    tolerance = float(gate["probability_simplex_tolerance"])
    simplex_pass = bool(
        (frame[probability_columns].to_numpy(dtype=float) >= 0.0).all()
        and (np.abs(probability_sum - 1.0) <= tolerance).all()
        and (target_sum == 1).all()
    )
    ordered = frame.sort_values(
        ["action_lifecycle_id", "horizon_ms"], kind="stable"
    )
    time_violations = 0
    for cause in ("fill", "cancel_ack"):
        difference = ordered.groupby("action_lifecycle_id", observed=True)[
            f"{cause}_probability"
        ].diff()
        time_violations += int((difference < -float(gate["monotonicity_tolerance"])).sum())

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
    integrated = (
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

    curves: list[dict[str, Any]] = []
    for (side, role), group in integrated.groupby(
        ["side", "inventory_role"], observed=True
    ):
        row: dict[str, Any] = {
            "side": str(side),
            "inventory_role": str(role),
            "days": int(group["day"].nunique()),
            "fill_events_at_largest_cut": int(group["fill_events"].sum()),
            "cancel_ack_events_at_largest_cut": int(
                group["cancel_ack_events"].sum()
            ),
        }
        metrics = (
            "joint_brier_improvement",
            "fill_brier_improvement",
            "cancel_ack_brier_improvement",
            "fill_calibration_bias",
            "cancel_ack_calibration_bias",
        )
        for index, metric in enumerate(metrics):
            row[metric] = _bootstrap_mean(
                group[metric].to_numpy(),
                samples=samples,
                seed=seed + len(curves) * 100 + index,
            )
        row["support_pass"] = bool(
            row["days"] == int(gate["required_oof_days"])
            and row["fill_events_at_largest_cut"]
            >= int(gate["minimum_events_per_cause"])
            and row["cancel_ack_events_at_largest_cut"]
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
        curves.append(row)
    identity = {
        "probability_simplex_pass": simplex_pass,
        "time_monotonicity_violations": int(time_violations),
        "distance_monotonicity": "not_applicable_current_action_only",
        "identity_pass": bool(simplex_pass and time_violations == 0),
    }
    return curves, identity


def _evaluate_ack_latency(
    frame: pd.DataFrame,
    *,
    gate: Mapping[str, Any],
    samples: int,
    seed: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    frame = frame.copy()
    frame["brier_improvement"] = (
        frame["ack_latency_target"]
        - frame["baseline_ack_latency_probability"]
    ) ** 2 - (
        frame["ack_latency_target"] - frame["ack_latency_probability"]
    ) ** 2
    frame["calibration_bias"] = (
        frame["ack_latency_probability"] - frame["ack_latency_target"]
    )
    ordered = frame.sort_values(
        ["action_lifecycle_id", "latency_threshold_ms"], kind="stable"
    )
    difference = ordered.groupby("action_lifecycle_id", observed=True)[
        "ack_latency_probability"
    ].diff()
    monotonicity_violations = int(
        (difference < -float(gate["monotonicity_tolerance"])).sum()
    )
    daily = (
        frame.groupby(["day", "side", "latency_threshold_ms"], observed=True)
        .agg(
            rows=("ack_latency_target", "size"),
            brier_improvement=("brier_improvement", "mean"),
            calibration_bias=("calibration_bias", "mean"),
        )
        .reset_index()
    )
    curves: list[dict[str, Any]] = []
    for (side, threshold), group in daily.groupby(
        ["side", "latency_threshold_ms"], observed=True
    ):
        row: dict[str, Any] = {
            "side": str(side),
            "latency_threshold_ms": int(threshold),
            "days": int(group["day"].nunique()),
            "rows": int(group["rows"].sum()),
            "brier_improvement": _bootstrap_mean(
                group["brier_improvement"].to_numpy(),
                samples=samples,
                seed=seed + len(curves) * 10,
            ),
            "calibration_bias": _bootstrap_mean(
                group["calibration_bias"].to_numpy(),
                samples=samples,
                seed=seed + len(curves) * 10 + 1,
            ),
        }
        row["support_pass"] = bool(
            row["days"] == int(gate["required_oof_days"])
            and row["rows"] >= int(gate["minimum_ack_rows_per_side_threshold"])
        )
        row["calibration_pass"] = _contains_zero(row["calibration_bias"])
        row["curve_pass"] = bool(row["support_pass"] and row["calibration_pass"])
        curves.append(row)
    identity = {
        "cdf_time_monotonicity_violations": monotonicity_violations,
        "identity_pass": monotonicity_violations == 0,
        "brier_improvement_is_diagnostic_only": True,
    }
    return curves, identity


def evaluate(
    report_path: Path,
    *,
    bootstrap_samples: int | None = None,
    bootstrap_seed: int | None = None,
) -> dict[str, Any]:
    report_path = report_path.expanduser().resolve()
    report = json.loads(report_path.read_text(encoding="utf-8"))
    spec_identity = report["spec"]
    spec_path = Path(str(spec_identity["path"])).expanduser().resolve()
    if _sha256(spec_path) != str(spec_identity["sha256"]):
        raise RuntimeError("policy-clock race fit spec identity changed")
    spec = load_placement_fill_spec(spec_path)
    if spec.get("schema_version") != (
        "narrowgate_placement_fill_policy_clock_race_fit_spec.v1"
    ):
        raise RuntimeError("unsupported policy-clock race evaluation spec")
    for name in ("implementation", "evaluator"):
        source = ROOT / str(spec["lineage"][name])
        if _sha256(source) != str(spec["lineage"][f"{name}_sha256"]):
            raise RuntimeError(f"policy-clock race {name} identity changed")
    gate = spec["reporting"]["curve_level_gate"]
    samples = int(bootstrap_samples or gate["bootstrap_samples"])
    seed = int(bootstrap_seed or gate["bootstrap_seed"])

    oof = _verified_frame(report["outputs"]["oof_predictions"])
    empirical_horizons = sorted(
        int(value)
        for value in spec["reporting"]["frozen_empirical_horizons_ms"].values()
    )
    oof = oof.loc[oof["horizon_ms"].isin(empirical_horizons)].copy()
    fill_curves, fill_identity = _evaluate_fill_curves(
        oof, gate=gate, samples=samples, seed=seed
    )

    latency = _verified_frame(
        report["outputs"]["oof_ack_latency_predictions"]
    )
    latency_curves, latency_identity = _evaluate_ack_latency(
        latency,
        gate=gate["latency_parity"],
        samples=samples,
        seed=seed + 10000,
    )
    policy_parity = report["policy_request_parity"]
    policy_pass = bool(policy_parity.get("passed", False))
    latency_pass = bool(
        latency_identity["identity_pass"]
        and latency_curves
        and all(row["curve_pass"] for row in latency_curves)
    )
    fill_pass = bool(
        fill_identity["identity_pass"]
        and len(fill_curves) == int(gate["curve_count"])
        and all(row["curve_pass"] for row in fill_curves)
    )
    development_pass = bool(policy_pass and latency_pass and fill_pass)
    return {
        "schema_version": "placement_fill_policy_clock_race_evaluation.v1",
        "family_id": str(report["family_id"]),
        "spec": spec_identity,
        "report": {"path": str(report_path), "sha256": _sha256(report_path)},
        "bootstrap": {"unit": "UTC day", "samples": samples, "seed": seed},
        "policy_request_gate": {
            "passed": policy_pass,
            "diagnostics": policy_parity,
        },
        "ack_latency_gate": {
            "passed": latency_pass,
            "identity": latency_identity,
            "curves": latency_curves,
        },
        "fill_cif_gate": {
            "passed": fill_pass,
            "empirical_horizons_ms": empirical_horizons,
            "legacy_horizons_are_report_only": True,
            "identity": fill_identity,
            "curves": fill_curves,
        },
        "development_curve_gate_passed": development_pass,
        "validation_access_recommended": development_pass,
        "validation_access_allowed": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_or_live_authorization": False,
        "prediction_does_not_authorize_action": True,
        "git": git_workspace_identity(ROOT),
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
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
