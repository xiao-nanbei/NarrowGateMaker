#!/usr/bin/env python3
"""Train and audit the first side-specific local quote policy artifact.

The v1 confirmatory family is fixed to BUY exposure-increasing add quotes and
compares the rolling baseline with a one-tick widen.  Logged actions come from
the randomized Python replay panel with known propensities.  Development uses
past-only chronological folds; the final model is fitted once on development
and evaluated once on a disjoint later panel.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections.abc import Sequence
from dataclasses import asdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge

from research.families.f09_campaign_action_uplift.audit.local_action_uplift import validate_action_panel
from research.families.f09_campaign_action_uplift.audit.offline_policy_evaluation import (
    OPEConfig,
    evaluate_fixed_holdout_policy,
    evaluate_offline_policy,
    make_day_folds,
    write_outputs,
)
from strategy.state_conditioned_quote_policy import (
    LOCAL_QUOTE_ACTIONS,
    PolicyArtifact,
)
from strategy.state_conditioned_quote_policy import (
    SCHEMA_VERSION as ARTIFACT_SCHEMA_VERSION,
)

SCHEMA_VERSION = "state_conditioned_policy_artifact_train.v1"
POLICY_ID = "buy_add_widen_1tick_local_v1"
SIDE = "BUY"
ROLE = "add"
CONTROL_ACTION = "baseline"
CANDIDATE_ACTION = "widen_1tick"
FROZEN_SPEC_PATH = (
    Path(__file__).parents[4] / "docs" / "buy_add_widen_policy_artifact_spec_20260715.md"
)

# These fields are present on the live/replay shared policy surface.  Raw price
# levels are deliberately excluded so the artifact does not learn the BTC
# price regime itself.
FEATURES = (
    "inventory_ratio",
    "campaign_age_s",
    "campaign_max_abs_qty_so_far",
    "campaign_pnl_so_far",
    "campaign_adverse_excursion_so_far",
    "toxicity",
    "markout_ema",
    "microprice_shift_bps",
    "l2_quote_flip_rate",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
    "l2_near_depth_total",
)
TARGETS = ("reward", "terminal", "tail_avoidance")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_panel(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    validate_action_panel(frame)
    frame["day"] = frame["day"].astype(str).str.slice(0, 10)
    return frame


def _panel_metadata(path: Path) -> tuple[Path, dict[str, Any]]:
    suffix = ".action_panel.csv"
    raw = str(path)
    if not raw.endswith(suffix):
        raise ValueError(f"panel path must end with {suffix}: {path}")
    metadata_path = Path(raw[: -len(suffix)] + ".metadata.json")
    payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"panel metadata must be a JSON object: {metadata_path}")
    return metadata_path, payload


def _behavior_vector(frame: pd.DataFrame) -> dict[str, float]:
    vector: dict[str, float] = {}
    for action in LOCAL_QUOTE_ACTIONS:
        column = f"behavior_prob_{action}"
        values = pd.to_numeric(frame[column], errors="coerce")
        if values.isna().any() or values.nunique() != 1:
            raise ValueError(f"behavior probability is not frozen: {column}")
        vector[action] = float(values.iloc[0])
    if not math.isclose(sum(vector.values()), 1.0, abs_tol=1e-10, rel_tol=0.0):
        raise ValueError("behavior probability vector does not sum to one")
    return vector


def _side_panel(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame[
        frame["side"].astype(str).str.upper().eq(SIDE)
        & frame["inventory_role"].astype(str).str.lower().eq(ROLE)
    ].copy()
    if output.empty:
        raise ValueError(f"panel contains no {SIDE}:{ROLE} rows")
    values = output.loc[:, FEATURES].apply(pd.to_numeric, errors="coerce")
    if not np.isfinite(values.to_numpy(dtype=float)).all():
        raise ValueError("artifact features must be finite on every selected row")
    return output


def _target_panel(frame: pd.DataFrame, target: str, tail_threshold: float) -> pd.DataFrame:
    output = frame.copy()
    if target == "reward":
        values = pd.to_numeric(output["reward"], errors="coerce")
    elif target == "terminal":
        values = pd.to_numeric(output["terminal_campaign_pnl"], errors="coerce")
    elif target == "tail_avoidance":
        terminal = pd.to_numeric(output["terminal_campaign_pnl"], errors="coerce")
        values = -(terminal <= float(tail_threshold)).astype(float)
    else:  # pragma: no cover - fixed registry
        raise ValueError(f"unsupported target: {target}")
    output["ope_target"] = values
    return output


def _transforms(frame: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    values = frame.loc[:, FEATURES].to_numpy(dtype=float)
    mean = values.mean(axis=0)
    scale = values.std(axis=0)
    scale = np.where(scale > 1e-12, scale, 1.0)
    return mean, scale


def _fit_action_models(
    frame: pd.DataFrame,
    *,
    alpha: float,
    min_action_rows: int,
) -> dict[str, Any]:
    mean, scale = _transforms(frame)
    x_all = (frame.loc[:, FEATURES].to_numpy(dtype=float) - mean) / scale
    models: dict[str, dict[str, Any]] = {}
    for action in (CONTROL_ACTION, CANDIDATE_ACTION):
        mask = frame["action"].astype(str).eq(action).to_numpy()
        support_rows = int(mask.sum())
        if support_rows < int(min_action_rows):
            raise ValueError(
                f"{SIDE}:{ROLE} {action} has {support_rows} rows; requires {min_action_rows}"
            )
        model = Ridge(alpha=float(alpha), solver="lsqr")
        model.fit(x_all[mask], pd.to_numeric(frame.loc[mask, "reward"]).to_numpy())
        probability_column = f"behavior_prob_{action}"
        probability_floor = float(pd.to_numeric(frame[probability_column], errors="coerce").min())
        models[action] = {
            "intercept": float(model.intercept_),
            "coefficients": {name: float(model.coef_[idx]) for idx, name in enumerate(FEATURES)},
            "support_rows": support_rows,
            "behavior_probability_floor": probability_floor,
            "uplift_lcb": 0.0,
        }
    return {
        "mean": mean,
        "scale": scale,
        "models": models,
    }


def _predict_actions(frame: pd.DataFrame, fitted: dict[str, Any]) -> pd.Series:
    x = (frame.loc[:, FEATURES].to_numpy(dtype=float) - fitted["mean"]) / fitted["scale"]
    scores: dict[str, np.ndarray] = {}
    for action, model in fitted["models"].items():
        coefficients = np.asarray([model["coefficients"][name] for name in FEATURES], dtype=float)
        scores[action] = float(model["intercept"]) + x.dot(coefficients)
    selected = np.where(
        scores[CANDIDATE_ACTION] > scores[CONTROL_ACTION],
        CANDIDATE_ACTION,
        CONTROL_ACTION,
    )
    return pd.Series(selected, index=frame.index, dtype="string")


def _cross_fitted_actions(
    frame: pd.DataFrame,
    *,
    config: OPEConfig,
) -> tuple[pd.Series, pd.DataFrame]:
    selected = pd.Series(CONTROL_ACTION, index=frame.index, dtype="string")
    fold_rows: list[dict[str, Any]] = []
    folds = make_day_folds(frame["day"].tolist(), config)
    for fold in folds:
        train = frame[frame["day"].isin(fold.train_days)]
        test = frame[frame["day"].isin(fold.test_days)]
        if len(train) < config.min_train_rows or test.empty:
            continue
        fitted = _fit_action_models(
            train,
            alpha=config.ridge_alpha,
            min_action_rows=config.min_action_rows,
        )
        fold_selected = _predict_actions(test, fitted)
        selected.loc[test.index] = fold_selected
        fold_rows.append(
            {
                "fold": fold.fold,
                "train_days": len(fold.train_days),
                "test_days": len(fold.test_days),
                "train_rows": len(train),
                "test_rows": len(test),
                "candidate_rows": int(fold_selected.eq(CANDIDATE_ACTION).sum()),
                "candidate_rate": float(fold_selected.eq(CANDIDATE_ACTION).mean()),
            }
        )
    if not fold_rows:
        raise ValueError("no chronological artifact folds were evaluable")
    return selected, pd.DataFrame(fold_rows)


def _evaluate_targets(
    development: pd.DataFrame,
    later: pd.DataFrame,
    *,
    development_actions: pd.Series,
    later_actions: pd.Series,
    output_prefix: Path,
    config: OPEConfig,
    tail_threshold: float,
) -> tuple[dict[str, Any], dict[str, Any]]:
    development_results: dict[str, Any] = {}
    later_results: dict[str, Any] = {}
    for target in TARGETS:
        development_target = _target_panel(development, target, tail_threshold)
        development_target["candidate_action"] = development_actions
        rows, folds, actions, candidate_summary = evaluate_offline_policy(
            development_target,
            feature_names=FEATURES,
            config=config,
        )
        write_outputs(
            output_prefix.parent / f"{output_prefix.name}.development.{target}",
            rows,
            folds,
            actions,
            candidate_summary,
        )
        development_baseline = development_target.copy()
        development_baseline["candidate_action"] = CONTROL_ACTION
        baseline_rows, baseline_folds, baseline_actions, baseline_summary = evaluate_offline_policy(
            development_baseline,
            feature_names=FEATURES,
            config=config,
        )
        write_outputs(
            output_prefix.parent / f"{output_prefix.name}.development.{target}.baseline",
            baseline_rows,
            baseline_folds,
            baseline_actions,
            baseline_summary,
        )
        development_results[target] = _paired_contrast(
            rows,
            baseline_rows,
            candidate_summary=candidate_summary,
            baseline_summary=baseline_summary,
            trials=config.bootstrap_trials,
            seed=config.random_seed,
        )

        later_target = _target_panel(later, target, tail_threshold)
        later_target["candidate_action"] = later_actions
        development_for_holdout = development_target.copy()
        development_for_holdout["candidate_action"] = CONTROL_ACTION
        holdout_rows, holdout_folds, holdout_actions, holdout_summary = (
            evaluate_fixed_holdout_policy(
                development_for_holdout,
                later_target,
                feature_names=FEATURES,
                config=config,
            )
        )
        write_outputs(
            output_prefix.parent / f"{output_prefix.name}.later.{target}",
            holdout_rows,
            holdout_folds,
            holdout_actions,
            holdout_summary,
        )
        later_baseline = later_target.copy()
        later_baseline["candidate_action"] = CONTROL_ACTION
        (
            holdout_baseline_rows,
            holdout_baseline_folds,
            holdout_baseline_actions,
            holdout_baseline_summary,
        ) = evaluate_fixed_holdout_policy(
            development_for_holdout,
            later_baseline,
            feature_names=FEATURES,
            config=config,
        )
        write_outputs(
            output_prefix.parent / f"{output_prefix.name}.later.{target}.baseline",
            holdout_baseline_rows,
            holdout_baseline_folds,
            holdout_baseline_actions,
            holdout_baseline_summary,
        )
        later_results[target] = _paired_contrast(
            holdout_rows,
            holdout_baseline_rows,
            candidate_summary=holdout_summary,
            baseline_summary=holdout_baseline_summary,
            trials=config.bootstrap_trials,
            seed=config.random_seed + 100,
        )
    return development_results, later_results


def _bootstrap_interval(values: pd.DataFrame, *, trials: int, seed: int) -> dict[str, float]:
    clusters = values.groupby("day", sort=True)["uplift"].agg(["sum", "count"])
    if clusters.empty or trials <= 0:
        return {
            "trials": 0,
            "cluster_days": 0,
            "uplift_p025": math.nan,
            "uplift_p50": math.nan,
            "uplift_p975": math.nan,
        }
    sums = clusters["sum"].to_numpy(dtype=float)
    counts = clusters["count"].to_numpy(dtype=float)
    rng = np.random.default_rng(seed)
    samples = np.empty(trials, dtype=float)
    for idx in range(trials):
        chosen = rng.integers(0, len(clusters), size=len(clusters))
        samples[idx] = sums[chosen].sum() / max(counts[chosen].sum(), 1.0)
    return {
        "trials": int(trials),
        "cluster_days": int(len(clusters)),
        "uplift_p025": float(np.quantile(samples, 0.025)),
        "uplift_p50": float(np.quantile(samples, 0.50)),
        "uplift_p975": float(np.quantile(samples, 0.975)),
    }


def _paired_contrast(
    candidate_rows: pd.DataFrame,
    baseline_rows: pd.DataFrame,
    *,
    candidate_summary: dict[str, Any],
    baseline_summary: dict[str, Any],
    trials: int,
    seed: int,
) -> dict[str, Any]:
    candidate = candidate_rows.loc[:, ["decision_id", "day", "ope_dr_value"]].rename(
        columns={"ope_dr_value": "candidate_dr"}
    )
    baseline = baseline_rows.loc[:, ["decision_id", "ope_dr_value"]].rename(
        columns={"ope_dr_value": "baseline_dr"}
    )
    paired = candidate.merge(baseline, on="decision_id", how="inner", validate="one_to_one")
    paired["uplift"] = pd.to_numeric(paired["candidate_dr"], errors="coerce") - pd.to_numeric(
        paired["baseline_dr"], errors="coerce"
    )
    paired = paired[np.isfinite(paired["uplift"])].copy()
    if paired.empty:
        raise ValueError("candidate/baseline OPE rows have no finite paired contrast")
    daily = paired.groupby("day", sort=True)["uplift"].mean()
    interval = _bootstrap_interval(paired, trials=trials, seed=seed)
    return {
        "schema_version": "paired_baseline_dr_contrast.v1",
        "numerical_ope_gate_passed": bool(
            candidate_summary["numerical_ope_gate_passed"]
            and baseline_summary["numerical_ope_gate_passed"]
        ),
        "rows": int(len(paired)),
        "estimators": {"candidate_minus_baseline_dr_uplift": float(paired["uplift"].mean())},
        "day_cluster_bootstrap": interval,
        "daily_uplift": {
            "days": int(len(daily)),
            "positive_days": int((daily > 0.0).sum()),
            "negative_days": int((daily < 0.0).sum()),
            "zero_days": int((daily == 0.0).sum()),
            "positive_rate": float((daily > 0.0).mean()),
            "median": float(daily.median()),
        },
        "overlap": candidate_summary["overlap"],
        "candidate_ope": candidate_summary,
        "baseline_ope": baseline_summary,
    }


def _fixed_candidate_lcb(frame: pd.DataFrame, *, config: OPEConfig) -> tuple[float, dict[str, Any]]:
    fixed = frame.copy()
    fixed["candidate_action"] = CANDIDATE_ACTION
    candidate_rows, _, _, candidate_summary = evaluate_offline_policy(
        fixed,
        feature_names=FEATURES,
        config=config,
    )
    baseline = frame.copy()
    baseline["candidate_action"] = CONTROL_ACTION
    baseline_rows, _, _, baseline_summary = evaluate_offline_policy(
        baseline,
        feature_names=FEATURES,
        config=config,
    )
    contrast = _paired_contrast(
        candidate_rows,
        baseline_rows,
        candidate_summary=candidate_summary,
        baseline_summary=baseline_summary,
        trials=config.bootstrap_trials,
        seed=config.random_seed + 200,
    )
    return (
        float(contrast["day_cluster_bootstrap"]["uplift_p025"]),
        contrast,
    )


def _passes(summary: dict[str, Any], *, lower_bound: float) -> bool:
    return bool(
        summary["numerical_ope_gate_passed"]
        and float(summary["day_cluster_bootstrap"]["uplift_p025"]) >= lower_bound
    )


def _promotion_decision(
    development: dict[str, Any],
    later: dict[str, Any],
    *,
    fixed_candidate_lcb: float,
) -> tuple[str, list[str]]:
    failures: list[str] = []
    if fixed_candidate_lcb <= 0.0:
        failures.append("development_fixed_action_lcb_nonpositive")
    for panel_name, results in (("development", development), ("later", later)):
        for target in TARGETS:
            lower_bound = 1e-12 if target == "reward" else 0.0
            if not _passes(results[target], lower_bound=lower_bound):
                failures.append(f"{panel_name}_{target}_gate_failed")
        positive_rate = float(results["reward"]["daily_uplift"]["positive_rate"])
        required = 0.55 if panel_name == "development" else 0.60
        if not math.isfinite(positive_rate) or positive_rate < required:
            failures.append(f"{panel_name}_daily_sign_failed")
    return ("promotion_eligible" if not failures else "shadow_only"), failures


def _artifact(
    fitted: dict[str, Any],
    *,
    development: pd.DataFrame,
    fixed_candidate_lcb: float,
    promotion_status: str,
    config: OPEConfig,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    models = json.loads(json.dumps(fitted["models"]))
    models[CANDIDATE_ACTION]["uplift_lcb"] = float(fixed_candidate_lcb)
    return {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "policy_id": POLICY_ID,
        "promotion_status": promotion_status,
        "trained_through_day": max(development["day"].astype(str)),
        "input_scope": "local_only",
        "actions": list(LOCAL_QUOTE_ACTIONS),
        "features": [
            {
                "name": name,
                "mean": float(fitted["mean"][idx]),
                "scale": float(fitted["scale"][idx]),
            }
            for idx, name in enumerate(FEATURES)
        ],
        "gates": {
            "min_support_rows": int(config.min_action_rows),
            "min_behavior_probability": float(config.min_behavior_propensity),
            "min_advantage": 0.0,
            "max_feature_age_ms": 1_000.0,
        },
        "models": {f"{SIDE}:{ROLE}": models},
        "training_metadata": metadata,
    }


def _render_report(summary: dict[str, Any]) -> str:
    lines = [
        "# BUY Add Widen-One-Tick Policy Artifact",
        "",
        f"- Promotion status: **{summary['promotion_status']}**",
        f"- Development rows/days: `{summary['development_rows']}` / `{summary['development_days']}`",
        f"- Later rows/days: `{summary['later_rows']}` / `{summary['later_days']}`",
        f"- Fixed-action development LCB: `{summary['fixed_candidate_lcb']:+.6f}` USDC/decision",
        f"- Failures: `{summary['promotion_failures']}`",
        "",
        "| panel | target | DR uplift | 95% interval | ESS | positive days |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for panel_name in ("development", "later"):
        for target in TARGETS:
            result = summary[panel_name][target]
            estimators = result["estimators"]
            interval = result["day_cluster_bootstrap"]
            daily = result["daily_uplift"]
            lines.append(
                f"| {panel_name} | {target} | "
                f"{estimators['candidate_minus_baseline_dr_uplift']:+.6f} | "
                f"[{interval['uplift_p025']:+.6f}, {interval['uplift_p975']:+.6f}] | "
                f"{result['overlap']['effective_sample_size']:.1f} | "
                f"{daily['positive_days']}/{daily['days']} |"
            )
    lines.extend(
        [
            "",
            "> The later panel is evaluated once with a model fitted only on development. "
            "A shadow-only result must not be repaired by changing features, thresholds, or "
            "the candidate action on these same later days.",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--development-panel", type=Path, required=True)
    parser.add_argument("--later-panel", type=Path, required=True)
    parser.add_argument("--output-prefix", type=Path, required=True)
    parser.add_argument("--min-train-days", type=int, default=50)
    parser.add_argument("--test-days", type=int, default=10)
    parser.add_argument("--embargo-days", type=int, default=1)
    parser.add_argument("--min-action-rows", type=int, default=100)
    parser.add_argument("--min-effective-sample-size", type=float, default=100.0)
    parser.add_argument("--min-behavior-propensity", type=float, default=0.10)
    parser.add_argument("--max-importance-weight", type=float, default=10.0)
    parser.add_argument("--ridge-alpha", type=float, default=10.0)
    parser.add_argument("--bootstrap-trials", type=int, default=2_000)
    parser.add_argument("--tail-threshold", type=float, default=-5.0)
    parser.add_argument("--random-seed", type=int, default=20260715)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    development_path = args.development_panel.expanduser().resolve()
    later_path = args.later_panel.expanduser().resolve()
    output_prefix = args.output_prefix.expanduser().resolve()
    output_prefix.parent.mkdir(parents=True, exist_ok=True)

    development_all = _load_panel(development_path)
    later_all = _load_panel(later_path)
    development_metadata_path, development_metadata = _panel_metadata(development_path)
    later_metadata_path, later_metadata = _panel_metadata(later_path)
    if str(development_metadata.get("panel_role", "")) != "development":
        raise ValueError("development panel metadata has the wrong panel_role")
    if str(later_metadata.get("panel_role", "")) != "later":
        raise ValueError("later panel metadata has the wrong panel_role")
    development_behavior = _behavior_vector(development_all)
    later_behavior = _behavior_vector(later_all)
    if development_behavior != later_behavior:
        raise ValueError("development and later behavior policies differ")
    if str(development_metadata.get("config_sha256", "")) != str(
        later_metadata.get("config_sha256", "")
    ):
        raise ValueError("development and later config hashes differ")
    if str(development_metadata.get("latency_profile_id", "")) != str(
        later_metadata.get("latency_profile_id", "")
    ):
        raise ValueError("development and later latency profiles differ")
    development = _side_panel(development_all)
    later = _side_panel(later_all)
    overlap = sorted(set(development["day"]) & set(later["day"]))
    if overlap:
        raise ValueError(f"development and later days overlap: {overlap}")
    if min(later["day"]) <= max(development["day"]):
        raise ValueError("later panel must be strictly after development")

    config = OPEConfig(
        reward_col="ope_target",
        split_mode="chronological",
        min_train_days=int(args.min_train_days),
        test_days=int(args.test_days),
        embargo_days=int(args.embargo_days),
        min_train_rows=max(500, int(args.min_action_rows) * 8),
        min_action_rows=int(args.min_action_rows),
        min_behavior_propensity=float(args.min_behavior_propensity),
        max_importance_weight=float(args.max_importance_weight),
        min_effective_sample_size=float(args.min_effective_sample_size),
        ridge_alpha=float(args.ridge_alpha),
        bootstrap_trials=int(args.bootstrap_trials),
        random_seed=int(args.random_seed),
    )
    development_actions, folds = _cross_fitted_actions(development, config=config)
    final_fitted = _fit_action_models(
        development,
        alpha=config.ridge_alpha,
        min_action_rows=config.min_action_rows,
    )
    later_actions = _predict_actions(later, final_fitted)
    development_results, later_results = _evaluate_targets(
        development,
        later,
        development_actions=development_actions,
        later_actions=later_actions,
        output_prefix=output_prefix,
        config=config,
        tail_threshold=float(args.tail_threshold),
    )
    fixed_lcb, fixed_summary = _fixed_candidate_lcb(
        _target_panel(development, "reward", float(args.tail_threshold)),
        config=config,
    )
    promotion_status, failures = _promotion_decision(
        development_results,
        later_results,
        fixed_candidate_lcb=fixed_lcb,
    )
    training_metadata = {
        "schema_version": SCHEMA_VERSION,
        "frozen_spec": str(FROZEN_SPEC_PATH),
        "frozen_spec_sha256": _sha256(FROZEN_SPEC_PATH),
        "side": SIDE,
        "inventory_role": ROLE,
        "control_action": CONTROL_ACTION,
        "candidate_action": CANDIDATE_ACTION,
        "development_panel": str(development_path),
        "development_panel_sha256": _sha256(development_path),
        "development_metadata": str(development_metadata_path),
        "development_metadata_sha256": _sha256(development_metadata_path),
        "later_panel": str(later_path),
        "later_panel_sha256": _sha256(later_path),
        "later_metadata": str(later_metadata_path),
        "later_metadata_sha256": _sha256(later_metadata_path),
        "development_days": sorted(development["day"].unique()),
        "later_days": sorted(later["day"].unique()),
        "behavior_probabilities": development_behavior,
        "config_sha256": str(development_metadata.get("config_sha256", "")),
        "latency_profile_id": str(development_metadata.get("latency_profile_id", "")),
        "ope_config": asdict(config),
        "fixed_candidate_lcb": fixed_lcb,
        "promotion_failures": failures,
        "code_sha256": {
            "trainer": _sha256(Path(__file__).resolve()),
            "replay_panel": _sha256(Path(__file__).with_name("local_action_uplift.py")),
            "ope": _sha256(Path(__file__).with_name("offline_policy_evaluation.py")),
            "quote_policy": _sha256(
                Path(__file__).parents[4].joinpath("strategy/state_conditioned_quote_policy.py")
            ),
        },
    }
    artifact = _artifact(
        final_fitted,
        development=development,
        fixed_candidate_lcb=fixed_lcb,
        promotion_status=promotion_status,
        config=config,
        metadata=training_metadata,
    )
    artifact_path = output_prefix.with_suffix(".artifact.json")
    artifact_path.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n")
    PolicyArtifact.load(artifact_path)

    summary = {
        "schema_version": SCHEMA_VERSION,
        "policy_id": POLICY_ID,
        "promotion_status": promotion_status,
        "promotion_failures": failures,
        "development_rows": int(len(development)),
        "development_days": int(development["day"].nunique()),
        "later_rows": int(len(later)),
        "later_days": int(later["day"].nunique()),
        "development_candidate_rate": float(development_actions.eq(CANDIDATE_ACTION).mean()),
        "later_candidate_rate": float(later_actions.eq(CANDIDATE_ACTION).mean()),
        "fixed_candidate_lcb": fixed_lcb,
        "fixed_candidate_summary": fixed_summary,
        "development": development_results,
        "later": later_results,
        "artifact": str(artifact_path),
        "artifact_sha256": _sha256(artifact_path),
    }
    output_prefix.with_suffix(".fold_policy.csv").write_text(
        folds.to_csv(index=False), encoding="utf-8"
    )
    output_prefix.with_suffix(".summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    output_prefix.with_suffix(".report.md").write_text(_render_report(summary), encoding="utf-8")
    print(
        json.dumps(
            {
                "artifact": str(artifact_path),
                "promotion_status": promotion_status,
                "promotion_failures": failures,
                "development_candidate_rate": summary["development_candidate_rate"],
                "later_candidate_rate": summary["later_candidate_rate"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
