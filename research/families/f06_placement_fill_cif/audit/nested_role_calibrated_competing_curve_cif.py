#!/usr/bin/env python3
"""Fit competing placement CIFs with inner-OOF role calibration."""

from __future__ import annotations

import argparse
import gc
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy.special import expit, logit

from models.audit.experiment_manifest import (
    git_workspace_identity,
    write_code_checkpoint,
)
from research.families.f06_placement_fill_cif import FAMILY_DOCS
from research.families.f06_placement_fill_cif.audit.competing_curve_fill_cif import (
    IDENTITY_COLUMNS,
    apply_competing_baseline,
    competing_labels_at_horizons,
    fit_competing_baseline_rates,
    predict_competing_cif_at_horizons,
)
from research.families.f06_placement_fill_cif.audit.direct_fill_cif import make_expanding_folds
from research.families.f06_placement_fill_cif.audit.full_curve_fill_cif import (
    DATA_ROOT,
    MODEL_FEATURES,
    ROOT,
    _load_partitions,
    _sha256,
    build_sampled_risk_rows,
    derive_duration_contract,
    expand_action_lifecycles,
    fit_activation_contract,
    fit_hazard_model,
)
from research.families.f06_placement_fill_cif.audit.placement_fill_spec import (
    load_placement_fill_spec,
)
from research.governance.historical_reproduction import (
    add_historical_reproduction_argument,
    require_historical_reproduction,
    stamp_historical_reproduction_output,
    verify_frozen_source_identity,
)

DEFAULT_SPEC = (
    FAMILY_DOCS / "placement_fill_nested_role_competing_cif_v6_spec_20260727.json"
)
DEFAULT_OUTPUT = (
    DATA_ROOT
    / "reports"
    / "placement_fill_nested_role_competing_cif_v6_development_20260727"
)

SCHEMA_VERSION = "placement_fill_nested_role_competing_cif.v1"
MODEL_KIND = "side_model_with_inner_oof_role_cause_calibration"


def _atomic_json(payload: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def _risk_kwargs(
    spec: Mapping[str, Any], maximum_support_ms: int
) -> dict[str, Any]:
    contract = spec["development_fit"]
    return {
        "interval_ms": int(contract["risk_interval_ms"]),
        "maximum_support_ms": int(maximum_support_ms),
        "maximum_negative_intervals_per_action": int(
            contract["maximum_negative_intervals_per_action"]
        ),
        "sampling_strategy": str(contract["risk_sampling"]),
        "hazard_causes": tuple(contract["hazard_causes"]),
    }


def _model_contract(spec: Mapping[str, Any]) -> dict[str, Any]:
    contract = spec["development_fit"]
    return {
        **contract["model"],
        "hazard_causes": list(contract["hazard_causes"]),
    }


def _raw_binary_hazards(
    model: Mapping[str, Any], rows: pd.DataFrame
) -> tuple[np.ndarray, np.ndarray]:
    features = rows.loc[:, MODEL_FEATURES]
    fill = np.clip(
        model["fill"].predict_proba(features)[:, 1], 1e-7, 1.0 - 1e-7
    )
    cancel = np.clip(
        model["cancel_ack"].predict_proba(features)[:, 1],
        1e-7,
        1.0 - 1e-7,
    )
    return fill, cancel


def fit_logit_calibrator(
    raw: np.ndarray,
    target: np.ndarray,
    sample_weight: np.ndarray,
    *,
    regularization_c: float,
    minimum_event_intervals: int,
    minimum_non_event_intervals: int,
) -> dict[str, float | int]:
    """Fit a positive-slope Platt map to genuinely out-of-fold scores."""

    probability = np.clip(np.asarray(raw, dtype=float), 1e-7, 1.0 - 1e-7)
    outcome = np.asarray(target, dtype=np.int8)
    weight = np.asarray(sample_weight, dtype=float)
    event_count = int((outcome == 1).sum())
    non_event_count = int((outcome == 0).sum())
    if event_count < int(minimum_event_intervals):
        raise ValueError("inner OOF calibration lacks event support")
    if non_event_count < int(minimum_non_event_intervals):
        raise ValueError("inner OOF calibration lacks non-event support")
    score = logit(probability)
    center = float(np.average(score, weights=weight))
    variance = float(np.average((score - center) ** 2, weights=weight))
    scale = max(float(np.sqrt(variance)), 1e-6)
    normalized = (score - center) / scale
    total_weight = float(weight.sum())
    prevalence = float(np.average(outcome, weights=weight))
    initial = np.asarray(
        [float(logit(np.clip(prevalence, 1e-7, 1.0 - 1e-7))), 0.0]
    )
    ridge = 1.0 / float(regularization_c)

    def objective(parameters: np.ndarray) -> tuple[float, np.ndarray]:
        intercept_standard, log_slope_standard = parameters
        slope_standard = float(np.exp(log_slope_standard))
        linear = intercept_standard + slope_standard * normalized
        loss = float(
            np.sum(weight * (np.logaddexp(0.0, linear) - outcome * linear))
            / total_weight
            + 0.5 * ridge * log_slope_standard**2
        )
        residual = weight * (expit(linear) - outcome) / total_weight
        gradient = np.asarray(
            [
                float(residual.sum()),
                float(
                    np.sum(residual * slope_standard * normalized)
                    + ridge * log_slope_standard
                ),
            ]
        )
        return loss, gradient

    result = minimize(
        objective,
        initial,
        method="L-BFGS-B",
        jac=True,
        bounds=((-20.0, 20.0), (-6.0, 6.0)),
        options={"maxiter": 1000, "ftol": 1e-12, "gtol": 1e-9},
    )
    if not bool(result.success):
        raise RuntimeError(f"inner OOF calibrator failed: {result.message}")
    intercept_standard = float(result.x[0])
    slope = float(np.exp(result.x[1])) / scale
    if not np.isfinite(slope) or slope <= 0.0:
        raise ValueError("inner OOF calibrator has non-positive slope")
    intercept = intercept_standard - slope * center
    return {
        "intercept": intercept,
        "slope": slope,
        "optimizer_iterations": int(result.nit),
        "sampled_rows": int(len(outcome)),
        "event_intervals": event_count,
        "non_event_intervals": non_event_count,
    }


def apply_logit_calibrator(
    raw: np.ndarray, calibrator: Mapping[str, float | int]
) -> np.ndarray:
    score = logit(np.clip(np.asarray(raw, dtype=float), 1e-7, 1.0 - 1e-7))
    return np.clip(
        expit(
            float(calibrator["intercept"])
            + float(calibrator["slope"]) * score
        ),
        1e-7,
        1.0 - 1e-7,
    )


class LogitCalibratedBinaryModel:
    """Expose a calibrated binary model through the sklearn probability API."""

    def __init__(self, model: Any, calibrator: Mapping[str, float | int]) -> None:
        self.model = model
        self.calibrator = dict(calibrator)

    def predict_proba(self, features: pd.DataFrame) -> np.ndarray:
        raw = self.model.predict_proba(features)[:, 1]
        probability = apply_logit_calibrator(raw, self.calibrator)
        return np.column_stack([1.0 - probability, probability])


def _calibrated_competing_model(
    model: Mapping[str, Any],
    calibrators: Mapping[str, Mapping[str, float | int]],
) -> dict[str, LogitCalibratedBinaryModel]:
    return {
        cause: LogitCalibratedBinaryModel(model[cause], calibrators[cause])
        for cause in ("fill", "cancel_ack")
    }


def build_inner_oof_hazard_scores(
    lifecycles: pd.DataFrame,
    *,
    spec: Mapping[str, Any],
    maximum_support_ms: int,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    """Generate one reusable past-only inner-OOF interval-score tape."""

    calibration = spec["development_fit"]["inner_oof_role_calibration"]
    roles = tuple(str(value) for value in calibration["roles"])
    days = sorted(lifecycles["day"].astype(str).unique())
    folds = make_expanding_folds(
        days,
        min_train_days=int(calibration["inner_min_train_days"]),
        embargo_days=int(calibration["inner_embargo_days"]),
        test_days=int(calibration["inner_test_days"]),
    )
    risk_kwargs = _risk_kwargs(spec, maximum_support_ms)
    model_contract = _model_contract(spec)
    pieces: list[pd.DataFrame] = []
    identities: list[dict[str, Any]] = []
    for fold in folds:
        fit_lifecycles = lifecycles.loc[
            lifecycles["day"].isin(fold["train_days"])
        ]
        fit_rows = build_sampled_risk_rows(fit_lifecycles, **risk_kwargs)
        model = fit_hazard_model(fit_rows, model_contract)
        fold_rows = 0
        for day in fold["test_days"]:
            day_frame = lifecycles.loc[lifecycles["day"].eq(day)]
            for role in roles:
                role_frame = day_frame.loc[
                    day_frame["inventory_role"]
                    .astype(str)
                    .str.lower()
                    .eq(role)
                ]
                if role_frame.empty:
                    continue
                rows = build_sampled_risk_rows(role_frame, **risk_kwargs)
                raw_fill, raw_cancel = _raw_binary_hazards(model, rows)
                target = rows["target"].to_numpy(dtype=np.int8)
                part = pd.DataFrame(
                    {
                        "day": str(day),
                        "inventory_role": role,
                        "target": target,
                        "sample_weight": rows["sample_weight"].to_numpy(
                            dtype=np.float32
                        ),
                        "raw_fill": raw_fill.astype(np.float32),
                        "raw_cancel_ack": raw_cancel.astype(np.float32),
                    }
                )
                pieces.append(part)
                fold_rows += int(len(part))
                del rows, part
        identities.append(
            {
                "fold": int(fold["fold"]),
                "train_days": list(fold["train_days"]),
                "embargo_days": list(fold["embargo_days"]),
                "test_days": list(fold["test_days"]),
                "fit_sampled_rows": int(len(fit_rows)),
                "oof_sampled_rows": fold_rows,
                "past_only": bool(
                    max(fold["train_days"]) < min(fold["test_days"])
                ),
            }
        )
        del fit_lifecycles, fit_rows, model
        gc.collect()
    if not pieces:
        raise ValueError("inner expanding calibration produced no OOF scores")
    return pd.concat(pieces, ignore_index=True), identities


def fit_role_calibrators(
    inner_oof: pd.DataFrame,
    *,
    outer_train_days: Sequence[str],
    spec: Mapping[str, Any],
) -> tuple[dict[str, dict[str, dict[str, float | int]]], dict[str, Any]]:
    """Fit role/cause maps using only inner-OOF scores inside outer train."""

    calibration = spec["development_fit"]["inner_oof_role_calibration"]
    roles = tuple(str(value) for value in calibration["roles"])
    allowed_days = {str(day) for day in outer_train_days}
    selected = inner_oof.loc[inner_oof["day"].isin(allowed_days)]
    if selected.empty:
        raise ValueError("outer train has no inner-OOF calibration rows")
    calibrators: dict[str, dict[str, dict[str, float | int]]] = {}
    identity: dict[str, Any] = {}
    for role in roles:
        frame = selected.loc[selected["inventory_role"].eq(role)]
        if frame.empty:
            raise ValueError(f"inner OOF calibration lacks role={role}")
        target = frame["target"].to_numpy(dtype=np.int8)
        weight = frame["sample_weight"].to_numpy(dtype=float)
        role_calibrators = {
            "fill": fit_logit_calibrator(
                frame["raw_fill"].to_numpy(dtype=float),
                (target == 1).astype(np.int8),
                weight,
                regularization_c=float(calibration["regularization_c"]),
                minimum_event_intervals=int(
                    calibration["minimum_event_intervals"]
                ),
                minimum_non_event_intervals=int(
                    calibration["minimum_non_event_intervals"]
                ),
            ),
            "cancel_ack": fit_logit_calibrator(
                frame["raw_cancel_ack"].to_numpy(dtype=float),
                (target == 2).astype(np.int8),
                weight,
                regularization_c=float(calibration["regularization_c"]),
                minimum_event_intervals=int(
                    calibration["minimum_event_intervals"]
                ),
                minimum_non_event_intervals=int(
                    calibration["minimum_non_event_intervals"]
                ),
            ),
        }
        calibrators[role] = role_calibrators
        identity[role] = role_calibrators
    used_days = sorted(selected["day"].astype(str).unique())
    return calibrators, {
        "inner_oof_days": used_days,
        "inner_oof_sampled_rows": int(len(selected)),
        "role_calibration": identity,
        "past_only": bool(max(used_days) <= max(outer_train_days)),
    }


def predict_nested_role_competing_cif(
    model: Mapping[str, Any],
    lifecycles: pd.DataFrame,
    horizons_ms: Sequence[int],
    *,
    role_calibrators: Mapping[
        str, Mapping[str, Mapping[str, float | int]]
    ],
    activation_contract: Mapping[str, Any],
    interval_ms: int,
    maximum_support_ms: int,
    chunk_size: int,
) -> pd.DataFrame:
    roles = set(lifecycles["inventory_role"].astype(str).str.lower().unique())
    missing = sorted(roles - set(role_calibrators))
    if missing:
        raise ValueError(f"missing nested role calibrators: {missing}")
    outputs: list[pd.DataFrame] = []
    for role in sorted(roles):
        subset = lifecycles.loc[
            lifecycles["inventory_role"]
            .astype(str)
            .str.lower()
            .eq(role)
        ]
        calibrated_model = _calibrated_competing_model(
            model, role_calibrators[role]
        )
        outputs.append(
            predict_competing_cif_at_horizons(
                calibrated_model,
                subset,
                horizons_ms,
                activation_contract=activation_contract,
                hazard_offset={"fill": 0.0, "cancel_ack": 0.0},
                interval_ms=int(interval_ms),
                maximum_support_ms=int(maximum_support_ms),
                chunk_size=int(chunk_size),
            )
        )
    return pd.concat(outputs, ignore_index=True)


def _load_spec(path: Path) -> dict[str, Any]:
    payload = load_placement_fill_spec(path)
    if payload.get("schema_version") != (
        "narrowgate_placement_fill_full_curve_spec.v6"
    ):
        raise RuntimeError("unsupported nested role-calibrated placement spec")
    if payload.get("research_status") != (
        "frozen_before_v6_inner_oof_development_fit"
    ):
        raise RuntimeError("nested role-calibrated placement spec is not frozen")
    for name in ("implementation", "evaluator"):
        expected = str(payload["lineage"][f"{name}_sha256"])
        verify_frozen_source_identity(str(payload["lineage"][name]), expected)
    return payload


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    add_historical_reproduction_argument(parser)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--smoke-days", type=int, default=0)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    args.spec = args.spec.expanduser().resolve()
    args.output_dir = args.output_dir.expanduser().resolve()
    reproduction_identity = require_historical_reproduction(
        runner_id="f06.nested_role_calibrated_competing_curve_cif",
        enabled=bool(args.historical_reproduction),
        spec_path=args.spec,
    )
    spec = _load_spec(args.spec)
    wide = _load_partitions(spec)
    if int(args.smoke_days) > 0:
        selected = sorted(wide["day"].astype(str).unique())[: int(args.smoke_days)]
        wide = wide.loc[wide["day"].isin(selected)].copy()
    lifecycles = expand_action_lifecycles(wide)
    fit_contract = spec["development_fit"]
    duration = derive_duration_contract(
        lifecycles,
        interval_ms=int(fit_contract["risk_interval_ms"]),
        report_quantiles=spec["reporting"]["development_exposure_quantiles"],
        maximum_support_quantile=float(
            spec["reporting"]["maximum_support_quantile"]
        ),
    )
    frozen_horizons = {
        str(key): int(value)
        for key, value in spec["reporting"]["frozen_empirical_horizons_ms"].items()
    }
    if not args.smoke_days and frozen_horizons != duration["report_quantiles"]:
        raise RuntimeError("Development exposure quantiles changed after v6 freeze")
    maximum_support_ms = int(duration["maximum_support_ms"])
    if not args.smoke_days and maximum_support_ms != int(
        spec["reporting"]["frozen_maximum_support_ms"]
    ):
        raise RuntimeError("Development maximum support changed after v6 freeze")
    report_horizons = sorted(
        set(frozen_horizons.values())
        | {
            int(value)
            for value in spec["reporting"]["legacy_diagnostic_horizons_ms"]
        }
    )
    days = sorted(lifecycles["day"].astype(str).unique())
    minimum_train_days = int(fit_contract["minimum_train_days"])
    if args.smoke_days:
        minimum_train_days = max(12, min(len(days) - 2, minimum_train_days))
    outer_folds = make_expanding_folds(
        days,
        min_train_days=minimum_train_days,
        embargo_days=int(fit_contract["embargo_days"]),
        test_days=int(fit_contract["outer_test_days"]),
    )

    side_inner_oof: dict[str, pd.DataFrame] = {}
    side_inner_identity: dict[str, list[dict[str, Any]]] = {}
    for side in ("BUY", "SELL"):
        side_lifecycles = lifecycles.loc[lifecycles["side"].eq(side)]
        scores, identity = build_inner_oof_hazard_scores(
            side_lifecycles,
            spec=spec,
            maximum_support_ms=maximum_support_ms,
        )
        side_inner_oof[side] = scores
        side_inner_identity[side] = identity

    risk_kwargs = _risk_kwargs(spec, maximum_support_ms)
    model_contract = _model_contract(spec)
    oof_parts: list[pd.DataFrame] = []
    fold_identity: list[dict[str, Any]] = []
    for fold in outer_folds:
        for side in ("BUY", "SELL"):
            train = lifecycles.loc[
                lifecycles["day"].isin(fold["train_days"])
                & lifecycles["side"].eq(side)
            ]
            test = lifecycles.loc[
                lifecycles["day"].isin(fold["test_days"])
                & lifecycles["side"].eq(side)
            ].copy()
            if train.empty or test.empty:
                continue
            train_rows = build_sampled_risk_rows(train, **risk_kwargs)
            model = fit_hazard_model(train_rows, model_contract)
            role_calibrators, calibration_identity = fit_role_calibrators(
                side_inner_oof[side],
                outer_train_days=fold["train_days"],
                spec=spec,
            )
            if max(calibration_identity["inner_oof_days"]) >= min(
                fold["test_days"]
            ):
                raise RuntimeError("inner OOF calibration reaches outer test")
            activation = fit_activation_contract(train)
            prediction = predict_nested_role_competing_cif(
                model,
                test,
                report_horizons,
                role_calibrators=role_calibrators,
                activation_contract=activation,
                interval_ms=int(fit_contract["risk_interval_ms"]),
                maximum_support_ms=maximum_support_ms,
                chunk_size=int(fit_contract["prediction_chunk_size"]),
            )
            labels = competing_labels_at_horizons(test, report_horizons)
            scored = prediction.merge(
                labels,
                on=list(IDENTITY_COLUMNS) + ["horizon_ms"],
                how="inner",
                validate="one_to_one",
            )
            rates = fit_competing_baseline_rates(train, report_horizons)
            baseline_fill, baseline_cancel = apply_competing_baseline(
                scored, rates
            )
            scored["baseline_fill_probability"] = baseline_fill.astype(np.float32)
            scored["baseline_cancel_ack_probability"] = baseline_cancel.astype(
                np.float32
            )
            scored["baseline_no_event_probability"] = (
                1.0 - baseline_fill - baseline_cancel
            ).astype(np.float32)
            scored["fold"] = int(fold["fold"])
            oof_parts.append(scored)
            fold_identity.append(
                {
                    "fold": int(fold["fold"]),
                    "side": side,
                    "train_days": list(fold["train_days"]),
                    "embargo_days": list(fold["embargo_days"]),
                    "test_days": list(fold["test_days"]),
                    "train_sampled_rows": int(len(train_rows)),
                    **calibration_identity,
                }
            )
            del train, test, train_rows, model, prediction, labels, scored
            gc.collect()
    if not oof_parts:
        raise RuntimeError("v6 nested role-calibrated fit produced no OOF rows")
    oof = pd.concat(oof_parts, ignore_index=True)

    final_models: dict[str, Any] = {}
    final_fit: dict[str, Any] = {}
    for side in ("BUY", "SELL"):
        side_rows = lifecycles.loc[lifecycles["side"].eq(side)]
        sampled = build_sampled_risk_rows(side_rows, **risk_kwargs)
        model = fit_hazard_model(sampled, model_contract)
        calibrators, identity = fit_role_calibrators(
            side_inner_oof[side], outer_train_days=days, spec=spec
        )
        final_models[side] = {
            "model": model,
            "role_calibrators": calibrators,
        }
        final_fit[side] = {
            "train_days": days,
            "train_sampled_rows": int(len(sampled)),
            **identity,
        }
        del sampled
    activation_contract = fit_activation_contract(lifecycles)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    code = git_workspace_identity(ROOT)
    checkpoint = write_code_checkpoint(
        args.output_dir / "code_checkpoint", repo_root=ROOT, code_identity=code
    )
    oof_path = args.output_dir / "oof_nested_role_predictions.parquet"
    artifact_path = args.output_dir / "nested_role_competing_cif.joblib"
    oof.to_parquet(oof_path, index=False, compression="zstd")
    artifact = {
        "schema_version": SCHEMA_VERSION,
        "family_id": str(spec["family_id"]),
        "model_kind": MODEL_KIND,
        "model_features": MODEL_FEATURES,
        "models": final_models,
        "activation_contract": activation_contract,
        "duration_contract": duration,
        "risk_interval_ms": int(fit_contract["risk_interval_ms"]),
        "maximum_support_ms": maximum_support_ms,
        "fixed_horizons_are_report_only": True,
        "active_order_keep_replace": "separate_not_built",
        "campaign_repair": "separate_not_built",
        "action_or_live_authorization": False,
    }
    joblib.dump(artifact, artifact_path)
    report = {
        "schema_version": SCHEMA_VERSION,
        "family_id": str(spec["family_id"]),
        "model_kind": MODEL_KIND,
        "development_days": days,
        "development_cohorts": int(lifecycles["cohort_id"].nunique()),
        "development_action_lifecycles": int(len(lifecycles)),
        "duration_contract": duration,
        "report_horizons_ms": report_horizons,
        "legacy_horizons_are_report_only": True,
        "horizon_cell_prediction_gate": False,
        "curve_level_gate": spec["reporting"]["curve_level_gate"],
        "curve_level_status": "not_evaluated",
        "validation_access_allowed": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_or_live_authorization": False,
        "inner_oof_folds": side_inner_identity,
        "outer_folds": fold_identity,
        "final_fit": final_fit,
        "spec_sha256": _sha256(args.spec),
        "git": code,
        "checkpoint": checkpoint,
        "outputs": {
            "oof_nested_role_predictions": {
                "path": str(oof_path),
                "sha256": _sha256(oof_path),
            },
            "artifact": {
                "path": str(artifact_path),
                "sha256": _sha256(artifact_path),
            },
        },
    }
    report_path = args.output_dir / "report.json"
    _atomic_json(report, report_path)
    stamp_historical_reproduction_output(args.output_dir, reproduction_identity)
    print(
        json.dumps(
            {
                "development_days": len(days),
                "oof_rows": len(oof),
                "report": str(report_path),
                "validation_read": False,
                "action_or_live_authorization": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
