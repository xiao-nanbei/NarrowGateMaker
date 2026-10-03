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
from models.replay.risk_selection import PREFILL_LABEL_CONTRACT, PREFILL_HORIZON_NS

SURFACES = tuple(f"{kind}:{side}" for kind in ("E", "C") for side in ("BUY", "SELL"))
LABEL_SCOPE = "modeled_single_intervention_common_terminal_mtm_including_fees_funding"


def _validate_label(row: dict[str, Any]) -> None:
    """Validate the output contract, not a substitute for replay path validation."""
    surface = f"{row['kind']}:{row['side']}"
    if surface not in SURFACES or row["value_scope"] not in {LABEL_SCOPE, PREFILL_LABEL_CONTRACT}:
        raise ValueError("unsupported E/C paired value label")
    if row['value_scope'] == PREFILL_LABEL_CONTRACT:
        if (row.get('label_contract') != PREFILL_LABEL_CONTRACT
                or row.get('horizon_ns') != PREFILL_HORIZON_NS
                or row.get('continuation_policy') != 'frozen_B0'
                or row.get('selection_scope') != 'visible_inventory'
                or row['terminal_mark_ts_ms']*1_000_000-row['decision_ts_ns'] != PREFILL_HORIZON_NS
                or row.get('fork', {}).get('phase') != 'baseline_intent_before_order_budget'):
            raise ValueError('invalid current prefill label semantics')
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
    validation_start_ns: int | None = None, alpha: float = 1.0, min_train_rows: int = 8,
    policy_id: str = "ec-development-ridge",
    required_label_contract: str = LABEL_SCOPE,
    calendar_revision: str | None = None,
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
            or (calendar_revision is None and (validation_start_ns is None or validation_start_ns <= 0))):
        raise ValueError("declare feature units, a positive alpha and a chronological split")
    if calendar_revision is not None:
        from .prefill_calendar import REVISION, label_group
        if (calendar_revision != REVISION or validation_start_ns is not None
                or required_label_contract != PREFILL_LABEL_CONTRACT):
            raise ValueError('calendar training requires its explicit prefill contract, without chronological cutoff')
    features = tuple(feature_units)
    if required_label_contract not in {LABEL_SCOPE, PREFILL_LABEL_CONTRACT}:
        raise ValueError('unknown required label contract')
    if any(row['value_scope'] != required_label_contract for row in rows):
        raise ValueError('labels do not match the explicitly selected training contract')
    if required_label_contract == PREFILL_LABEL_CONTRACT and (alpha != 1.0 or min_train_rows < 128):
        raise ValueError('prefill first batch requires alpha=1 and at least 128 training rows')
    scopes = {row.get("selection_scope", "reachable_inventory") for row in rows}
    if len(scopes) > 1 or scopes - {"reachable_inventory", "visible_inventory"}:
        raise ValueError("training labels must share one defined selection scope")
    selection_scope = next(iter(scopes), "reachable_inventory")
    seen: set[str] = set()
    seen_slots = set()
    groups: dict[str, list[dict[str, Any]]] = {name: [] for name in ("train", "validation")}
    excluded: Counter[str] = Counter()
    for row in rows:
        _validate_label(row)
        identity = str(row["opportunity_id"])
        if identity in seen:
            raise ValueError("duplicate opportunity labels must not receive extra weight")
        seen.add(identity)
        if calendar_revision is not None:
            group = 'train' if label_group(row) == 'T' else 'validation'
            slot_key = tuple(sorted(row['sampling'].items()))
            if slot_key in seen_slots:
                raise ValueError('duplicate calendar slot must not receive extra weight')
            seen_slots.add(slot_key)
        elif int(row["decision_ts_ns"]) >= validation_start_ns:
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
    def order_key(row):
        parent = (row['sampling']['parent_account_id'] if calendar_revision is not None
                  else row['replay_start_ts_ms'])
        return parent, row.get('order_id')

    validation_orders = {order_key(row)
                         for row in groups["validation"] if row.get("order_id")}
    retained_train = []
    for row in groups["train"]:
        if (row.get("order_id")
                and order_key(row) in validation_orders):
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
        "selection_scope": selection_scope,
        "training_label_contract": required_label_contract,
        "features": {name: {"unit": feature_units[name], "mean": float(means[i]),
                            "scale": float(scales[i])} for i, name in enumerate(features)},
        "models": {},
    }
    report: dict[str, Any] = {
        "scope": "development_model_fit_not_full_path_economic_validation",
        "label_contract": required_label_contract,
        "selection_scope": selection_scope,
        "validation_start_ns": validation_start_ns, "alpha": alpha,
        "min_train_rows": min_train_rows, "input_labels": len(rows),
        "train_rows": len(groups["train"]), "validation_rows": len(groups["validation"]),
        "excluded": dict(excluded), "surfaces": {},
        "live_deployment_performed": False, "portfolio_pnl_estimate": None,
    }
    if calendar_revision is not None:
        report.update(calendar_revision=calendar_revision,
                      diagnostic_scope='used_history_pseudo_out_of_sample_not_past_only',
                      training_days='T100', final_labels_used=False)
    for surface in SURFACES:
        selected = {group: [row for row in group_rows
                            if f"{row['kind']}:{row['side']}" == surface]
                    for group, group_rows in groups.items()}
        train, validation = selected["train"], selected["validation"]
        input_rows = [row for row in rows if f"{row['kind']}:{row['side']}" == surface]
        details: dict[str, Any] = {
            "input_rows": len(input_rows), "train_rows": len(train),
            "validation_rows": len(validation),
            "excluded_rows": len(input_rows) - len(train) - len(validation),
            "train_decision_days": len({int(r["decision_ts_ns"]) // 86_400_000_000_000 for r in train}),
            "train_first_decision_ts_ns": min((int(r["decision_ts_ns"]) for r in train), default=None),
            "train_last_decision_ts_ns": max((int(r["decision_ts_ns"]) for r in train), default=None),
        }
        report["surfaces"][surface] = details
        # Descriptive training support only: never select features or adjust the
        # model using these counts, and never borrow validation variation.
        details["train_outcome_windows"] = len({(r["replay_start_ts_ms"], r["terminal_mark_ts_ms"])
                                                for r in train})
        details["train_decision_utc_hours"] = dict(sorted(Counter(
            str(int(r["decision_ts_ns"]) // 3_600_000_000_000 % 24) for r in train
        ).items()))
        details["train_feature_support"] = {}
        for name in features:
            values = {float(r["features"][name]) for r in train}
            details["train_feature_support"][name] = {
                "unique_values": len(values),
                "minimum": min(values) if values else None,
                "maximum": max(values) if values else None,
                "constant": len(values) == 1 if values else None,
            }
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
            if calendar_revision is not None:
                details['status'] = 'calendar_prediction_diagnostic'
                details['T100_mean_intercept_mse'] = details.pop('past_only_intercept_mse')
                details['diagnostic_groups'] = {}
                for split in ('A', 'B', 'C'):
                    mask = np.asarray([label_group(r) == split for r in validation])
                    details['diagnostic_groups'][split] = {
                        'rows': int(mask.sum()),
                        'mse': float(np.mean((predicted[mask]-actual[mask])**2)) if mask.any() else None,
                        'T100_mean_intercept_mse': float(np.mean((average-actual[mask])**2)) if mask.any() else None,
                    }
    RiskSelectionPolicy.from_dict(policy)
    report["fitted_surfaces"] = list(policy["models"])
    return policy, report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--labels", type=Path, nargs="+", required=True)
    parser.add_argument("--feature-units", type=Path, required=True,
                        help="JSON object mapping the frozen feature names to units")
    split = parser.add_mutually_exclusive_group(required=True)
    split.add_argument("--validation-start-ns", type=int)
    split.add_argument("--calendar-revision")
    parser.add_argument("--alpha", type=float, default=1.0)
    parser.add_argument("--min-train-rows", type=int, default=8)
    parser.add_argument("--policy-id", required=True)
    parser.add_argument("--label-contract", choices=(LABEL_SCOPE, PREFILL_LABEL_CONTRACT),
                        default=LABEL_SCOPE)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    rows = [json.loads(line) for path in args.labels for line in path.read_text().splitlines()
            if line.strip()]
    policy, report = train_chronological_ridge(
        rows, feature_units=json.loads(args.feature_units.read_text()),
        validation_start_ns=args.validation_start_ns, alpha=args.alpha,
        min_train_rows=args.min_train_rows, policy_id=args.policy_id,
        required_label_contract=args.label_contract,
        calendar_revision=args.calendar_revision,
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
