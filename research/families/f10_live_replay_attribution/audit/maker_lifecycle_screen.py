#!/usr/bin/env python3
"""Freeze and evaluate development-only maker lifecycle M0/M1 models.

The runner is deliberately two-stage.  ``freeze`` records the exact panel
hash, feature families, targets, folds, latency modes, and venue ablations
without scoring outcomes.  ``evaluate`` refuses to run if that identity has
changed.  Results are observational prediction increments, never action uplift.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from dataclasses import asdict
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.compose import TransformedTargetRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.metrics import brier_score_loss, mean_absolute_error, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from research.families.f05_fill_quality_quote_ev.audit.fill_toxicity_incremental import (
    DayFold,
    blocked_day_folds,
    chronological_folds,
)
from research.families.f10_live_replay_attribution.audit.maker_lifecycle_panel import (
    M0_SOURCE_FEATURES,
    SCHEMA_VERSION as PANEL_SCHEMA_VERSION,
    add_external_consensus,
)

SCHEMA_VERSION = "maker_lifecycle_screen.v1"
VENUE_SETS = {
    "full": ("bitget", "bybit", "okx"),
    "leave_bitget_out": ("bybit", "okx"),
    "leave_bybit_out": ("bitget", "okx"),
    "leave_okx_out": ("bitget", "bybit"),
}
TARGETS = (
    ("target_decision_to_terminal_mtm", "regression", "primary"),
    ("target_incremental_campaign_cost", "regression", "primary"),
    ("target_tail", "classification", "primary"),
    ("target_fill", "classification", "secondary"),
    ("target_fill_markout_30s_bps", "regression", "secondary"),
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _artifact_path(prefix: Path, suffix: str) -> Path:
    return Path(f"{prefix}{suffix}")


def _load_frame(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    raise ValueError("panel must end in .parquet or .csv")


def _numeric(frame: pd.DataFrame, name: str) -> pd.Series:
    if name not in frame:
        return pd.Series(np.nan, index=frame.index, dtype=float)
    return pd.to_numeric(frame[name], errors="coerce")


def _feature_columns(frame: pd.DataFrame) -> list[str]:
    features = [name for name in M0_SOURCE_FEATURES if name in frame and name != "action"]
    for name in frame:
        if name.startswith(("m0_l2_", "m0_bridge_")) and not name.endswith(
            "feature_ready_ts_ns"
        ):
            features.append(name)
    return list(dict.fromkeys(features))


def _external_columns(frame: pd.DataFrame, prefix: str) -> list[str]:
    return [
        name
        for name in frame
        if name.startswith(prefix + "_")
        and not name.endswith("feature_ready_ts_ns")
        and pd.api.types.is_numeric_dtype(frame[name])
    ]


def _design(frame: pd.DataFrame, features: Sequence[str]) -> pd.DataFrame:
    output = frame.loc[:, list(features)].apply(pd.to_numeric, errors="coerce").copy()
    actions = frame["action"].astype(str)
    for action in ("r1_rearm", "r2_rearm_widen_1tick"):
        output[f"logged_action_{action}"] = actions.eq(action).astype(float)
    return output


def _usable(train: pd.DataFrame) -> list[str]:
    return [
        name
        for name in train
        if train[name].notna().sum() >= 2 and train[name].nunique(dropna=True) >= 2
    ]


def _model(kind: str):
    if kind == "regression":
        base = Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                ("scale", StandardScaler()),
                ("model", Ridge(alpha=10.0)),
            ]
        )
        return TransformedTargetRegressor(regressor=base, transformer=StandardScaler())
    return Pipeline(
        [
            ("imputer", SimpleImputer(strategy="median")),
            ("scale", StandardScaler()),
            (
                "model",
                LogisticRegression(C=0.25, max_iter=2_000, class_weight="balanced"),
            ),
        ]
    )


def _fit_predict(
    train: pd.DataFrame,
    test: pd.DataFrame,
    *,
    features: Sequence[str],
    target: str,
    kind: str,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, int] | None:
    y_train = _numeric(train, target)
    y_test = _numeric(test, target)
    train_mask = y_train.notna()
    test_mask = y_test.notna()
    if int(train_mask.sum()) < 20 or int(test_mask.sum()) < 5:
        return None
    train_x = _design(train.loc[train_mask], features)
    test_x = _design(test.loc[test_mask], features)
    usable = _usable(train_x)
    if not usable:
        return None
    train_values = y_train.loc[train_mask].to_numpy(dtype=float)
    test_values = y_test.loc[test_mask].to_numpy(dtype=float)
    if kind == "classification":
        if len(np.unique(train_values)) < 2 or len(np.unique(test_values)) < 2:
            return None
    model = _model(kind)
    model.fit(train_x[usable], train_values)
    prediction = (
        model.predict_proba(test_x[usable])[:, 1]
        if kind == "classification"
        else np.asarray(model.predict(test_x[usable]), dtype=float)
    )
    return test_values, prediction, test.loc[test_mask, "day"].astype(str).to_numpy(), len(usable)


def _scores(y: np.ndarray, prediction: np.ndarray, kind: str) -> dict[str, float]:
    if kind == "classification":
        return {
            "brier": float(brier_score_loss(y, prediction)),
            "auc": float(roc_auc_score(y, prediction)) if len(np.unique(y)) > 1 else math.nan,
        }
    corr = spearmanr(y, prediction).statistic if len(y) >= 3 else math.nan
    return {
        "mae": float(mean_absolute_error(y, prediction)),
        "spearman": float(corr) if math.isfinite(float(corr)) else math.nan,
    }


def _daily_improvement(
    y: np.ndarray,
    m0_prediction: np.ndarray,
    m1_prediction: np.ndarray,
    days: np.ndarray,
    kind: str,
) -> list[dict[str, Any]]:
    rows = []
    loss0 = (y - m0_prediction) ** 2 if kind == "classification" else np.abs(y - m0_prediction)
    loss1 = (y - m1_prediction) ** 2 if kind == "classification" else np.abs(y - m1_prediction)
    for day in sorted(set(days)):
        mask = days == day
        rows.append(
            {
                "day": day,
                "rows": int(mask.sum()),
                "loss_improvement_m1_vs_m0": float(np.mean(loss0[mask] - loss1[mask])),
            }
        )
    return rows


def make_development_folds(
    days: Sequence[str],
    *,
    min_train_days: int,
    test_days: int,
    embargo_days: int,
    blocked_folds: int,
) -> list[DayFold]:
    folds = chronological_folds(
        list(days),
        min_train_days=min_train_days,
        test_days=test_days,
        embargo_days=embargo_days,
        late_days=0,
    )
    folds.extend(blocked_day_folds(list(days), folds=blocked_folds, late_days=0))
    return folds


def freeze_spec(
    panel_path: Path,
    panel: pd.DataFrame,
    *,
    output: Path,
    min_train_days: int,
    test_days: int,
    embargo_days: int,
    blocked_folds: int,
) -> dict[str, Any]:
    days = sorted(panel["day"].astype(str).unique())
    folds = make_development_folds(
        days,
        min_train_days=min_train_days,
        test_days=test_days,
        embargo_days=embargo_days,
        blocked_folds=blocked_folds,
    )
    modes = sorted(panel["market_data_latency_mode"].astype(str).unique())
    spec = {
        "schema_version": SCHEMA_VERSION,
        "phase": "frozen_before_outcome_scoring",
        "research_status": "development_only_not_untouched",
        "panel_path": str(panel_path.resolve()),
        "panel_sha256": _sha256(panel_path),
        "panel_schema_version": PANEL_SCHEMA_VERSION,
        "rows": len(panel),
        "days": days,
        "latency_modes": modes,
        "venue_sets": {key: list(value) for key, value in VENUE_SETS.items()},
        "sides": ["BUY", "SELL"],
        "pooled_side_primary": False,
        "targets": [
            {"name": name, "kind": kind, "tier": tier} for name, kind, tier in TARGETS
        ],
        "m0_features": _feature_columns(panel),
        "model": {
            "regression": "median_impute_standardize_ridge_alpha10_target_standardize",
            "classification": "median_impute_standardize_balanced_logit_C0.25",
            "logged_action_covariate": True,
        },
        "folds": [asdict(fold) for fold in folds],
        "future_holdout": {
            "available": False,
            "rule": "complete good days first observed after this frozen family identity",
        },
        "promotion_eligible": False,
        "promotion_blockers": [
            "development dates have already informed earlier research",
            "30-day receive-time train/embargo/test/late denominator is not available",
            "no untouched future good-day holdout exists for this frozen family",
            "prediction increment is not action uplift",
        ],
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(spec, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return spec


def evaluate(panel: pd.DataFrame, spec: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame]:
    fold_rows: list[dict[str, Any]] = []
    daily_rows: list[dict[str, Any]] = []
    m0_features = list(spec["m0_features"])
    folds = [
        DayFold(
            panel=str(row["panel"]),
            fold=int(row["fold"]),
            train_days=tuple(row["train_days"]),
            embargo_days=tuple(row["embargo_days"]),
            test_days=tuple(row["test_days"]),
        )
        for row in spec["folds"]
    ]
    for latency_mode in spec["latency_modes"]:
        latency = panel[panel["market_data_latency_mode"].astype(str) == latency_mode]
        for venue_set_name, venues in VENUE_SETS.items():
            prefix = f"m1_eval_{venue_set_name}"
            candidate = add_external_consensus(
                latency,
                included_venues=venues,
                prefix=prefix,
            )
            external_features = _external_columns(candidate, prefix)
            for side in ("BUY", "SELL"):
                side_frame = candidate[candidate["side"].astype(str).str.upper() == side]
                for fold in folds:
                    train = side_frame[side_frame["day"].astype(str).isin(fold.train_days)]
                    test = side_frame[side_frame["day"].astype(str).isin(fold.test_days)]
                    if len(train) < 100 or len(test) < 20:
                        continue
                    for target, kind, tier in TARGETS:
                        m0 = _fit_predict(
                            train,
                            test,
                            features=m0_features,
                            target=target,
                            kind=kind,
                        )
                        m1 = _fit_predict(
                            train,
                            test,
                            features=[*m0_features, *external_features],
                            target=target,
                            kind=kind,
                        )
                        if m0 is None or m1 is None:
                            continue
                        y0, pred0, days0, count0 = m0
                        y1, pred1, days1, count1 = m1
                        if not np.array_equal(y0, y1) or not np.array_equal(days0, days1):
                            raise ValueError("M0 and M1 did not score the identical test denominator")
                        score0 = _scores(y0, pred0, kind)
                        score1 = _scores(y1, pred1, kind)
                        row = {
                            "panel": fold.panel,
                            "fold": fold.fold,
                            "latency_mode": latency_mode,
                            "venue_set": venue_set_name,
                            "side": side,
                            "target": target,
                            "target_tier": tier,
                            "kind": kind,
                            "train_rows": len(train),
                            "test_rows": len(y0),
                            "m0_feature_count": count0,
                            "m1_feature_count": count1,
                            "train_first_day": fold.train_days[0],
                            "train_last_day": fold.train_days[-1],
                            "test_first_day": fold.test_days[0],
                            "test_last_day": fold.test_days[-1],
                            **{f"m0_{key}": value for key, value in score0.items()},
                            **{f"m1_{key}": value for key, value in score1.items()},
                        }
                        if kind == "classification":
                            row["primary_improvement_m1_vs_m0"] = score0["brier"] - score1["brier"]
                            row["secondary_delta_m1_vs_m0"] = score1["auc"] - score0["auc"]
                        else:
                            row["primary_improvement_m1_vs_m0"] = score0["mae"] - score1["mae"]
                            row["secondary_delta_m1_vs_m0"] = score1["spearman"] - score0["spearman"]
                        fold_rows.append(row)
                        for daily in _daily_improvement(y0, pred0, pred1, days0, kind):
                            daily_rows.append(
                                {
                                    "panel": fold.panel,
                                    "fold": fold.fold,
                                    "latency_mode": latency_mode,
                                    "venue_set": venue_set_name,
                                    "side": side,
                                    "target": target,
                                    **daily,
                                }
                            )
    return pd.DataFrame(fold_rows), pd.DataFrame(daily_rows)


def summarize(spec: dict[str, Any], folds: pd.DataFrame, daily: pd.DataFrame) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "status": "development_screen_complete" if len(folds) else "insufficient_data",
        "fold_metric_rows": len(folds),
        "daily_metric_rows": len(daily),
        "research_status": "development_only_not_untouched",
        "prediction_effect": "M1_minus_M0_observational_increment",
        "action_uplift": "not_estimated",
        "promotion_eligible": False,
        "promotion_blockers": list(spec["promotion_blockers"]),
    }
    if folds.empty:
        return summary
    grouped = []
    for keys, group in folds.groupby(
        ["latency_mode", "venue_set", "side", "target", "panel"], sort=True
    ):
        values = _numeric(group, "primary_improvement_m1_vs_m0").dropna()
        daily_group = daily[
            (daily["latency_mode"] == keys[0])
            & (daily["venue_set"] == keys[1])
            & (daily["side"] == keys[2])
            & (daily["target"] == keys[3])
            & (daily["panel"] == keys[4])
        ]
        day_values = _numeric(daily_group, "loss_improvement_m1_vs_m0").dropna()
        grouped.append(
            {
                "latency_mode": keys[0],
                "venue_set": keys[1],
                "side": keys[2],
                "target": keys[3],
                "panel": keys[4],
                "folds": len(values),
                "median_primary_improvement": float(values.median()) if len(values) else math.nan,
                "positive_fold_rate": float(values.gt(0).mean()) if len(values) else math.nan,
                "daily_rows": len(day_values),
                "positive_day_rate": float(day_values.gt(0).mean()) if len(day_values) else math.nan,
            }
        )
    summary["aggregate"] = grouped
    return summary


def render_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Maker Lifecycle M0/M1 Development Screen",
        "",
        f"- Status: `{summary['status']}`",
        f"- Fold metric rows: `{summary['fold_metric_rows']}`",
        f"- Daily metric rows: `{summary['daily_metric_rows']}`",
        "- Action uplift: `not estimated`",
        "- Promotion eligible: `false`",
        "",
        "This screen compares prediction error on the same side-specific rows. "
        "It does not estimate keep/widen/re-center action value and it does not "
        "reuse these dates as an untouched holdout.",
        "",
        "## Promotion Blockers",
        "",
    ]
    lines.extend(f"- {value}" for value in summary["promotion_blockers"])
    lines.extend(["", "## Aggregate", ""])
    for row in summary.get("aggregate", []):
        lines.append(
            "- `{latency_mode}` / `{venue_set}` / `{side}` / `{target}` / "
            "`{panel}`: median `{median_primary_improvement:.6g}`, fold+ "
            "`{positive_fold_rate:.1%}`, day+ `{positive_day_rate:.1%}`".format(**row)
        )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    freeze = sub.add_parser("freeze")
    freeze.add_argument("--panel", type=Path, required=True)
    freeze.add_argument("--spec", type=Path, required=True)
    freeze.add_argument("--min-train-days", type=int, default=50)
    freeze.add_argument("--test-days", type=int, default=20)
    freeze.add_argument("--embargo-days", type=int, default=1)
    freeze.add_argument("--blocked-folds", type=int, default=5)
    evaluate_parser = sub.add_parser("evaluate")
    evaluate_parser.add_argument("--panel", type=Path, required=True)
    evaluate_parser.add_argument("--spec", type=Path, required=True)
    evaluate_parser.add_argument("--output-prefix", type=Path, required=True)
    args = parser.parse_args()

    panel = _load_frame(args.panel)
    if args.command == "freeze":
        spec = freeze_spec(
            args.panel,
            panel,
            output=args.spec,
            min_train_days=args.min_train_days,
            test_days=args.test_days,
            embargo_days=args.embargo_days,
            blocked_folds=args.blocked_folds,
        )
        print(json.dumps({"spec": str(args.spec), "panel_sha256": spec["panel_sha256"]}, sort_keys=True))
        return 0

    spec = json.loads(args.spec.read_text(encoding="utf-8"))
    if _sha256(args.panel) != spec.get("panel_sha256"):
        raise ValueError("panel hash changed after screening spec freeze")
    folds, daily = evaluate(panel, spec)
    summary = summarize(spec, folds, daily)
    prefix = args.output_prefix
    prefix.parent.mkdir(parents=True, exist_ok=True)
    folds.to_csv(_artifact_path(prefix, ".fold_metrics.csv"), index=False)
    daily.to_csv(_artifact_path(prefix, ".daily_metrics.csv"), index=False)
    _artifact_path(prefix, ".json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    _artifact_path(prefix, ".md").write_text(render_markdown(summary), encoding="utf-8")
    print(json.dumps({key: summary[key] for key in ("status", "fold_metric_rows", "promotion_eligible")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
