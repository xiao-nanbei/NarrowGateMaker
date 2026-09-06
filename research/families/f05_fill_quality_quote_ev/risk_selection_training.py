"""Small chronological Ridge models for validated modeled E/C paired labels.

This is an offline training entrypoint, not a replay runner or live adapter.
Overlapping counterfactual values are supervised targets, never portfolio PnL.
The split removes training labels whose terminal outcome reaches validation.
No hyperparameter search, holdout access, imputation, or deployment is performed.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from pathlib import Path
from typing import Any

import numpy as np

from strategy.risk_selection import SCHEMA_VERSION, VALUE_UNIT, RiskSelectionPolicy

SURFACES = tuple(f"{kind}:{side}" for kind in ("E", "C") for side in ("BUY", "SELL"))
LABEL_SCOPE = "modeled_single_intervention_common_terminal_mtm_including_fees_funding"


def _validate_label(row: dict[str, Any]) -> None:
    """Validate the output contract, not a substitute for replay path validation."""
    surface = f"{row['kind']}:{row['side']}"
    if surface not in SURFACES or row["value_scope"] != LABEL_SCOPE:
        raise ValueError("unsupported E/C paired value label")
    if row["additive_portfolio_return"] is not False:
        raise ValueError("paired values must not be declared additive portfolio returns")
    expected = ("POST", "WAIT") if row["kind"] == "E" else ("KEEP", "CANCEL")
    if (row["baseline_action"], row["alternative_action"]) != expected:
        raise ValueError("label actions do not match the E/C surface")
    start = int(row["replay_start_ts_ms"]) * 1_000_000
    end = int(row["terminal_mark_ts_ms"]) * 1_000_000
    decision = int(row["decision_ts_ns"])
    # Feature warmup can precede the replay start; readiness cannot be future.
    if not (0 <= int(row["feature_ready_ts_ns"]) <= decision and start <= decision <= end):
        raise ValueError("label clocks are not causal or have an invalid outcome interval")
    values = [float(row[name]) for name in (
        "baseline_value_usdc", "alternative_value_usdc", "value_difference_usdc",
    )]
    if not all(math.isfinite(value) for value in values) or not math.isclose(
        values[0] - values[1], values[2], rel_tol=1e-10, abs_tol=1e-8,
    ):
        raise ValueError("paired value difference does not reconcile")
    if int(row["matched_opportunity_prefix_count"]) < 1:
        raise ValueError("paired label has no verified common opportunity prefix")


def train_chronological_ridge(
    rows: list[dict[str, Any]], *, feature_units: dict[str, str],
    validation_start_ns: int, alpha: float = 1.0, min_train_rows: int = 8,
    policy_id: str = "ec-development-ridge",
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Fit each side/surface separately, using training-only shared transforms.

    The minimum is an explicit engineering support choice, not significance.
    Validation rows are never used to choose features, scales, alpha or models.
    Missing features are excluded and reported; unsupported surfaces stay absent
    so the shared scorer preserves baseline behavior. One pilot window generally
    has no independent validation and must be labeled training-only.
    """
    if (not feature_units or any(not name or not unit for name, unit in feature_units.items())
            or not math.isfinite(alpha) or alpha <= 0 or min_train_rows < 2
            or validation_start_ns <= 0):
        raise ValueError("declare feature units, a positive alpha and a chronological split")
    features = tuple(feature_units)
    seen: set[str] = set()
    groups: dict[str, list[dict[str, Any]]] = {name: [] for name in ("train", "validation")}
    excluded: Counter[str] = Counter()
    for row in rows:
        _validate_label(row)
        identity = str(row["opportunity_id"])
        if identity in seen:
            raise ValueError("duplicate opportunity labels must not receive extra weight")
        seen.add(identity)
        if int(row["decision_ts_ns"]) >= validation_start_ns:
            group = "validation"
        elif int(row["terminal_mark_ts_ms"]) * 1_000_000 >= validation_start_ns:
            excluded["overlapping_validation_outcome"] += 1
            continue
        else:
            group = "train"
        values = [row["features"].get(name) for name in features]
        if any(value is None or not math.isfinite(float(value)) for value in values):
            excluded[f"{group}_missing_feature"] += 1
            continue
        groups[group].append(row)
    validation_orders = {(row["replay_start_ts_ms"], row.get("order_id"))
                         for row in groups["validation"] if row.get("order_id")}
    retained_train = []
    for row in groups["train"]:
        if (row.get("order_id")
                and (row["replay_start_ts_ms"], row["order_id"]) in validation_orders):
            excluded["shared_order_with_validation"] += 1
        else:
            retained_train.append(row)
    groups["train"] = retained_train
    if groups["train"]:
        matrix = np.asarray([[row["features"][name] for name in features]
                             for row in groups["train"]], dtype=float)
        means = matrix.mean(axis=0)
        scales = matrix.std(axis=0)
        # Repeated decimal values can leave roundoff in mean/std reductions.
        # Identify constants from the inputs, without collapsing small signals.
        constant = np.all(matrix == matrix[0], axis=0)
        means[constant] = matrix[0, constant]
        scales[constant] = 1.0
        scales[scales == 0] = 1.0
    else:
        means, scales = np.zeros(len(features)), np.ones(len(features))
    policy: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION, "value_unit": VALUE_UNIT,
        "policy_id": policy_id,
        "features": {name: {"unit": feature_units[name], "mean": float(means[i]),
                            "scale": float(scales[i])} for i, name in enumerate(features)},
        "models": {},
    }
    report: dict[str, Any] = {
        "scope": "development_model_fit_not_full_path_economic_validation",
        "validation_start_ns": validation_start_ns, "alpha": alpha,
        "min_train_rows": min_train_rows, "input_labels": len(rows),
        "train_rows": len(groups["train"]), "validation_rows": len(groups["validation"]),
        "excluded": dict(excluded), "surfaces": {},
        "live_deployment_performed": False, "portfolio_pnl_estimate": None,
    }
    for surface in SURFACES:
        selected = {group: [row for row in group_rows
                            if f"{row['kind']}:{row['side']}" == surface]
                    for group, group_rows in groups.items()}
        train, validation = selected["train"], selected["validation"]
        details: dict[str, Any] = {"train_rows": len(train), "validation_rows": len(validation)}
        report["surfaces"][surface] = details
        if len(train) < min_train_rows:
            details["status"] = "insufficient_training_rows"
            continue
        x = (np.asarray([[row["features"][name] for name in features] for row in train])
             - means) / scales
        y = np.asarray([row["value_difference_usdc"] for row in train], dtype=float)
        center, average = x.mean(axis=0), float(y.mean())
        coefficients = np.linalg.solve(
            (x - center).T @ (x - center) + alpha * np.eye(len(features)),
            (x - center).T @ (y - average),
        )
        intercept = average - float(center @ coefficients)
        policy["models"][surface] = {
            "intercept_usdc": intercept,
            "coefficients": dict(zip(features, map(float, coefficients), strict=True)),
        }
        details["status"] = "chronological_prediction_diagnostic" if validation else "training_only"
        details["train_outcome_windows"] = len({(r["replay_start_ts_ms"], r["terminal_mark_ts_ms"])
                                                for r in train})
        if validation:
            vx = (np.asarray([[r["features"][name] for name in features] for r in validation])
                  - means) / scales
            actual = np.asarray([r["value_difference_usdc"] for r in validation], dtype=float)
            predicted = intercept + vx @ coefficients
            details.update({
                "validation_mse": float(np.mean((predicted - actual) ** 2)),
                "past_only_intercept_mse": float(np.mean((average - actual) ** 2)),
                "mean_prediction_usdc": float(predicted.mean()),
                "mean_label_usdc": float(actual.mean()),
                "nonbaseline_prediction_fraction": float(np.mean(
                    predicted <= 0 if surface.startswith("E:") else predicted < 0)),
            })
    RiskSelectionPolicy.from_dict(policy)
    report["fitted_surfaces"] = list(policy["models"])
    return policy, report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--labels", type=Path, nargs="+", required=True)
    parser.add_argument("--feature-units", type=Path, required=True,
                        help="JSON object mapping the frozen feature names to units")
    parser.add_argument("--validation-start-ns", type=int, required=True)
    parser.add_argument("--alpha", type=float, default=1.0)
    parser.add_argument("--min-train-rows", type=int, default=8)
    parser.add_argument("--policy-id", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    rows = [json.loads(line) for path in args.labels for line in path.read_text().splitlines()
            if line.strip()]
    policy, report = train_chronological_ridge(
        rows, feature_units=json.loads(args.feature_units.read_text()),
        validation_start_ns=args.validation_start_ns, alpha=args.alpha,
        min_train_rows=args.min_train_rows, policy_id=args.policy_id,
    )
    args.output_dir.mkdir(parents=True, exist_ok=False, mode=0o700)
    for name, value in (("policy.json", policy), ("training_report.json", report)):
        path = args.output_dir / name
        with path.open("x", encoding="utf-8") as stream:
            json.dump(value, stream, indent=2, allow_nan=False)
            stream.write("\n")
        path.chmod(0o600)
    print(json.dumps({"fitted_surfaces": report["fitted_surfaces"],
                      "scope": report["scope"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
