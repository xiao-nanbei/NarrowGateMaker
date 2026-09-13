"""Historical native-source transport audit for a frozen 13-head bundle."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy.stats import spearmanr
from sklearn.metrics import average_precision_score, log_loss, roc_auc_score

from research.families.f03_causal_13_head.ml_model import MODEL_SPECS


SCHEMA_VERSION = "causal_13_head_native_transport_audit.v1"


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _weighted_mean(values: np.ndarray, weights: np.ndarray) -> float:
    return float(np.average(values, weights=weights))


def _classification_calibration(
    target: np.ndarray,
    prediction: np.ndarray,
    weights: np.ndarray,
) -> tuple[float, float]:
    clipped = np.clip(prediction, 1e-6, 1.0 - 1e-6)
    logit = np.log(clipped / (1.0 - clipped))

    def objective(params: np.ndarray) -> tuple[float, np.ndarray]:
        linear = params[0] + params[1] * logit
        probability = 1.0 / (1.0 + np.exp(-np.clip(linear, -40.0, 40.0)))
        loss = -np.sum(
            weights
            * (
                target * np.log(np.clip(probability, 1e-12, 1.0))
                + (1.0 - target)
                * np.log(np.clip(1.0 - probability, 1e-12, 1.0))
            )
        )
        residual = weights * (probability - target)
        gradient = np.array(
            [np.sum(residual), np.sum(residual * logit)],
            dtype=np.float64,
        )
        return float(loss), gradient

    result = minimize(
        lambda value: objective(value),
        np.array([0.0, 1.0], dtype=np.float64),
        jac=True,
        method="L-BFGS-B",
    )
    if not result.success:
        return math.nan, math.nan
    return float(result.x[0]), float(result.x[1])


def _regression_calibration(
    target: np.ndarray,
    prediction: np.ndarray,
    weights: np.ndarray,
) -> tuple[float, float]:
    design = np.column_stack([np.ones(len(prediction)), prediction])
    root_weight = np.sqrt(weights)
    coefficients, *_ = np.linalg.lstsq(
        design * root_weight[:, None],
        target * root_weight,
        rcond=None,
    )
    return float(coefficients[0]), float(coefficients[1])


def classification_metrics(
    target: np.ndarray,
    prediction: np.ndarray,
    weights: np.ndarray,
    baseline_prevalence: float,
) -> dict[str, float | int]:
    prediction = np.clip(prediction, 1e-6, 1.0 - 1e-6)
    prevalence = _weighted_mean(target, weights)
    brier = _weighted_mean((prediction - target) ** 2, weights)
    baseline_brier = _weighted_mean(
        (np.full_like(target, baseline_prevalence) - target) ** 2,
        weights,
    )
    intercept, slope = _classification_calibration(target, prediction, weights)
    has_both_classes = np.unique(target).size == 2
    return {
        "rows": int(len(target)),
        "events": int(np.sum(target > 0.5)),
        "observed_prevalence": prevalence,
        "predicted_prevalence": _weighted_mean(prediction, weights),
        "auc": (
            float(roc_auc_score(target, prediction, sample_weight=weights))
            if has_both_classes
            else math.nan
        ),
        "average_precision": (
            float(average_precision_score(target, prediction, sample_weight=weights))
            if has_both_classes
            else math.nan
        ),
        "brier": brier,
        "baseline_brier": baseline_brier,
        "brier_skill": (
            float(1.0 - brier / baseline_brier) if baseline_brier > 0.0 else math.nan
        ),
        "log_loss": float(log_loss(target, prediction, sample_weight=weights)),
        "calibration_intercept_log_odds": intercept,
        "calibration_slope": slope,
    }


def regression_metrics(
    target: np.ndarray,
    prediction: np.ndarray,
    weights: np.ndarray,
    baseline_mean: float,
) -> dict[str, float | int]:
    residual = prediction - target
    mae = _weighted_mean(np.abs(residual), weights)
    rmse = math.sqrt(_weighted_mean(residual**2, weights))
    baseline_mae = _weighted_mean(np.abs(target - baseline_mean), weights)
    baseline_rmse = math.sqrt(_weighted_mean((target - baseline_mean) ** 2, weights))
    intercept, slope = _regression_calibration(target, prediction, weights)
    finite_variation = np.std(target) > 0.0 and np.std(prediction) > 0.0
    return {
        "rows": int(len(target)),
        "observed_mean": _weighted_mean(target, weights),
        "predicted_mean": _weighted_mean(prediction, weights),
        "mae": mae,
        "baseline_mae": baseline_mae,
        "mae_skill": (
            float(1.0 - mae / baseline_mae) if baseline_mae > 0.0 else math.nan
        ),
        "rmse": rmse,
        "baseline_rmse": baseline_rmse,
        "rmse_skill": (
            float(1.0 - rmse / baseline_rmse) if baseline_rmse > 0.0 else math.nan
        ),
        "spearman_ic": (
            float(spearmanr(target, prediction).statistic)
            if finite_variation
            else math.nan
        ),
        "pearson_ic": (
            float(np.corrcoef(target, prediction)[0, 1])
            if finite_variation
            else math.nan
        ),
        "calibration_intercept": intercept,
        "calibration_slope": slope,
    }


def _finite_head_rows(
    frame: pd.DataFrame,
    label: str,
    prediction: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    target = pd.to_numeric(frame[label], errors="coerce").to_numpy(dtype=np.float64)
    weights = pd.to_numeric(
        frame.get("sample_weight", pd.Series(1.0, index=frame.index)),
        errors="coerce",
    ).to_numpy(dtype=np.float64)
    valid = np.isfinite(target) & np.isfinite(prediction) & np.isfinite(weights) & (weights > 0)
    days = pd.to_datetime(frame.index, utc=True).strftime("%Y-%m-%d").to_numpy()
    return target[valid], prediction[valid], weights[valid], days[valid]


def _daily_metrics(
    *,
    panel_role: str,
    head: str,
    objective: str,
    target: np.ndarray,
    prediction: np.ndarray,
    weights: np.ndarray,
    days: np.ndarray,
    baseline_value: float,
) -> list[dict]:
    rows = []
    for day in sorted(set(days)):
        mask = days == day
        if objective == "binary":
            metric = classification_metrics(
                target[mask], prediction[mask], weights[mask], baseline_value
            )
        else:
            metric = regression_metrics(
                target[mask], prediction[mask], weights[mask], baseline_value
            )
        rows.append({"panel_role": panel_role, "head": head, "day": day, **metric})
    return rows


def _feature_drift(
    train: pd.DataFrame,
    panel: pd.DataFrame,
    feature_cols: list[str],
    panel_role: str,
) -> list[dict]:
    rows = []
    for feature in feature_cols:
        train_value = pd.to_numeric(train[feature], errors="coerce").to_numpy(dtype=np.float64)
        panel_value = pd.to_numeric(panel[feature], errors="coerce").to_numpy(dtype=np.float64)
        train_finite = train_value[np.isfinite(train_value)]
        panel_finite = panel_value[np.isfinite(panel_value)]
        train_mean = float(np.mean(train_finite)) if train_finite.size else math.nan
        train_std = float(np.std(train_finite)) if train_finite.size else math.nan
        panel_mean = float(np.mean(panel_finite)) if panel_finite.size else math.nan
        standardized_shift = (
            float((panel_mean - train_mean) / train_std)
            if np.isfinite(train_std) and train_std > 0.0 and np.isfinite(panel_mean)
            else math.nan
        )
        rows.append(
            {
                "panel_role": panel_role,
                "feature": feature,
                "train_mean": train_mean,
                "train_std": train_std,
                "panel_mean": panel_mean,
                "panel_std": (
                    float(np.std(panel_finite)) if panel_finite.size else math.nan
                ),
                "standardized_mean_shift": standardized_shift,
                "train_missing_fraction": float(1.0 - train_finite.size / len(train_value)),
                "panel_missing_fraction": float(1.0 - panel_finite.size / len(panel_value)),
                "missing_fraction_change": float(
                    train_finite.size / len(train_value)
                    - panel_finite.size / len(panel_value)
                ),
            }
        )
    return rows


def _validate_spec(spec_path: Path) -> dict:
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    if spec.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported native transport audit schema")
    if spec.get("calibrator_fitted") is not False:
        raise ValueError("v12 transport audit must not fit a calibrator")
    for key in ("prediction_authority", "action_authority", "live_authority"):
        if spec.get(key) is not False:
            raise ValueError(f"transport audit must keep {key}=false")
    if sha256_file(Path(__file__)) != spec["audit_implementation_sha256"]:
        raise ValueError("transport audit implementation hash mismatch")

    feature_dir = Path(spec["feature_dir"])
    feature_manifest = feature_dir / "causal_feature_manifest.json"
    if sha256_file(feature_manifest) != spec["feature_manifest_sha256"]:
        raise ValueError("feature manifest hash mismatch")
    feature_payload = json.loads(feature_manifest.read_text(encoding="utf-8"))
    if int(feature_payload.get("feature_semantics_version", 0)) != 6:
        raise ValueError("transport audit requires feature semantics v6")
    if feature_payload.get("feature_dag_id") != spec["feature_dag_id"]:
        raise ValueError("transport audit feature DAG id mismatch")
    if feature_payload.get("feature_dag_sha256") != spec["feature_dag_sha256"]:
        raise ValueError("transport audit feature DAG hash mismatch")

    model_dir = Path(spec["model_dir"])
    training_summary = model_dir / "training_summary.json"
    if sha256_file(training_summary) != spec["training_summary_sha256"]:
        raise ValueError("training summary hash mismatch")
    summary = json.loads(training_summary.read_text(encoding="utf-8"))
    if summary.get("promotion_authority") != "research_only":
        raise ValueError("transport audit only accepts research-only bundles")
    if summary.get("targets") != list(MODEL_SPECS):
        raise ValueError("transport audit requires the ordered 13-head bundle")
    selection = summary.get("train_only_selection") or {}
    if selection.get("spec_sha256") != spec["training_spec_sha256"]:
        raise ValueError("transport audit training spec hash mismatch")

    artifacts = summary.get("artifacts") or []
    artifact_identity = hashlib.sha256(
        json.dumps(artifacts, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    if artifact_identity != spec["model_artifact_manifest_sha256"]:
        raise ValueError("model artifact manifest hash mismatch")
    artifact_by_name = {str(row.get("path")): row for row in artifacts}
    required_names = {
        name
        for head in MODEL_SPECS
        for name in (f"{head}.txt", f"{head}_meta.json")
    }
    if not required_names.issubset(artifact_by_name):
        raise ValueError("training summary is missing required 13-head artifacts")
    for name, identity in artifact_by_name.items():
        path = model_dir / name
        if path.parent != model_dir or not path.is_file():
            raise ValueError(f"invalid model artifact path: {name}")
        if sha256_file(path) != identity["sha256"]:
            raise ValueError(f"model artifact hash mismatch: {name}")
        if path.stat().st_size != int(identity["size_bytes"]):
            raise ValueError(f"model artifact size mismatch: {name}")

    split = feature_payload.get("split") or {}
    for panel in spec["panels"]:
        if panel["days"] != split[panel["split"]]:
            raise ValueError(f"{panel['role']} day identity mismatch")
        if panel.get("independent_confirmation") is not False:
            raise ValueError("reused 2026 panels cannot be independent confirmation")
    return spec


def run_audit(spec_path: Path, output_dir: Path) -> dict:
    spec_path = spec_path.resolve()
    spec = _validate_spec(spec_path)
    feature_dir = Path(spec["feature_dir"])
    model_dir = Path(spec["model_dir"])
    output_dir.mkdir(parents=True, exist_ok=False)

    metadata = {
        head: json.loads((model_dir / f"{head}_meta.json").read_text(encoding="utf-8"))
        for head in MODEL_SPECS
    }
    feature_cols = list(metadata[next(iter(MODEL_SPECS))]["feature_cols"])
    if any(list(value["feature_cols"]) != feature_cols for value in metadata.values()):
        raise ValueError("13-head feature schemas differ")
    models = {
        head: lgb.Booster(model_file=str(model_dir / f"{head}.txt"))
        for head in MODEL_SPECS
    }

    train = pd.read_parquet(feature_dir / "dataset_train.parquet")
    train_baselines = {}
    for head, (label, _objective, _, _) in MODEL_SPECS.items():
        values = pd.to_numeric(train[label], errors="coerce").to_numpy(dtype=np.float64)
        weights = pd.to_numeric(
            train.get("sample_weight", pd.Series(1.0, index=train.index)),
            errors="coerce",
        ).to_numpy(dtype=np.float64)
        valid = np.isfinite(values) & np.isfinite(weights) & (weights > 0)
        train_baselines[head] = _weighted_mean(values[valid], weights[valid])

    head_rows = []
    daily_rows = []
    drift_rows = []
    prediction_outputs = []
    for panel_spec in spec["panels"]:
        role = panel_spec["role"]
        frame = pd.read_parquet(feature_dir / panel_spec["dataset_file"])
        matrix = (
            frame[feature_cols]
            .replace([np.inf, -np.inf], np.nan)
            .astype(np.float32)
        )
        prediction_frame = pd.DataFrame(index=frame.index)
        for head, (label, objective, _, _) in MODEL_SPECS.items():
            prediction = np.asarray(models[head].predict(matrix), dtype=np.float64)
            prediction_frame[f"pred_{head}"] = prediction
            target, valid_prediction, weights, days = _finite_head_rows(
                frame, label, prediction
            )
            if objective == "binary":
                metrics = classification_metrics(
                    target,
                    valid_prediction,
                    weights,
                    train_baselines[head],
                )
            else:
                metrics = regression_metrics(
                    target,
                    valid_prediction,
                    weights,
                    train_baselines[head],
                )
            head_rows.append(
                {
                    "panel_role": role,
                    "head": head,
                    "objective": objective,
                    "baseline_value_from_2025_train": train_baselines[head],
                    **metrics,
                }
            )
            daily_rows.extend(
                _daily_metrics(
                    panel_role=role,
                    head=head,
                    objective=objective,
                    target=target,
                    prediction=valid_prediction,
                    weights=weights,
                    days=days,
                    baseline_value=train_baselines[head],
                )
            )
        prediction_path = output_dir / f"predictions_{role}.parquet"
        prediction_frame.to_parquet(prediction_path)
        prediction_outputs.append(
            {
                "role": role,
                "path": str(prediction_path),
                "sha256": sha256_file(prediction_path),
                "rows": int(len(prediction_frame)),
            }
        )
        drift_rows.extend(_feature_drift(train, frame, feature_cols, role))
        del frame, matrix, prediction_frame

    head_frame = pd.DataFrame(head_rows)
    daily_frame = pd.DataFrame(daily_rows)
    drift_frame = pd.DataFrame(drift_rows)
    head_path = output_dir / "head_metrics.csv"
    daily_path = output_dir / "daily_metrics.csv"
    drift_path = output_dir / "feature_drift.csv"
    head_frame.to_csv(head_path, index=False)
    daily_frame.to_csv(daily_path, index=False)
    drift_frame.to_csv(drift_path, index=False)

    panel_summaries = []
    for role, group in head_frame.groupby("panel_role", sort=False):
        classification = group[group["objective"] == "binary"]
        regression = group[group["objective"] != "binary"]
        drift = drift_frame[drift_frame["panel_role"] == role]
        panel_summaries.append(
            {
                "role": role,
                "classification_head_count": int(len(classification)),
                "classification_auc_median": float(classification["auc"].median()),
                "classification_brier_skill_positive_heads": int(
                    (classification["brier_skill"] > 0.0).sum()
                ),
                "regression_head_count": int(len(regression)),
                "regression_spearman_positive_heads": int(
                    (regression["spearman_ic"] > 0.0).sum()
                ),
                "max_abs_standardized_feature_shift": float(
                    drift["standardized_mean_shift"].abs().max()
                ),
                "max_abs_missing_fraction_change": float(
                    drift["missing_fraction_change"].abs().max()
                ),
            }
        )

    artifacts = []
    for path in (head_path, daily_path, drift_path):
        artifacts.append(
            {"path": str(path), "sha256": sha256_file(path), "size_bytes": path.stat().st_size}
        )
    report = {
        "schema_version": SCHEMA_VERSION,
        "identity": spec["identity"],
        "spec_path": str(spec_path),
        "spec_sha256": sha256_file(spec_path),
        "feature_semantics_version": 6,
        "feature_dag_id": spec["feature_dag_id"],
        "feature_dag_sha256": spec["feature_dag_sha256"],
        "training_summary_sha256": spec["training_summary_sha256"],
        "panels": panel_summaries,
        "prediction_outputs": prediction_outputs,
        "artifacts": artifacts,
        "calibrator_fitted": False,
        "transport_audit_complete": True,
        "independent_confirmation": False,
        "prediction_authority": False,
        "action_authority": False,
        "live_authority": False,
    }
    report_path = output_dir / "report.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    report = run_audit(args.spec, args.output_dir)
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
