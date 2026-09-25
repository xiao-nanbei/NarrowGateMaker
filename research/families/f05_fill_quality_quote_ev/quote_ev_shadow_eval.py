#!/usr/bin/env python3
"""Offline shadow evaluation for quote EV model bundles.

The evaluator scores current and candidate quote_ev models on the same labelled
orders/fills table.  It is intentionally non-trading: the decisive live metric
still needs a tick A/B, but this report checks whether a shadow model improves
filled-quote adverse markout ranking before it is worth wiring into policy.
"""

from __future__ import annotations


import numpy as np
import pandas as pd

from research.families.f05_fill_quality_quote_ev.quote_ev import (  # noqa: E402
    QuoteEVModel,
)

PREDICTED_VALUE_COLUMN = (
    "pred_expected_maker_markout_bps_per_opportunity_30s"
)


def _predicted_value(frame: pd.DataFrame) -> pd.Series:
    return pd.to_numeric(frame[PREDICTED_VALUE_COLUMN], errors="coerce")


def _calibration_bins(actual: pd.Series, pred: pd.Series, n_bins: int = 10) -> list[dict[str, float | int]]:
    y = pd.to_numeric(actual, errors="coerce").fillna(0.0).to_numpy(dtype=np.float64)
    p = pd.to_numeric(pred, errors="coerce").fillna(0.0).clip(0.0, 1.0).to_numpy(dtype=np.float64)
    if len(y) == 0:
        return []
    bins = np.minimum(np.floor(p * n_bins).astype(int), n_bins - 1)
    rows: list[dict[str, float | int]] = []
    for idx in range(n_bins):
        mask = bins == idx
        if not np.any(mask):
            continue
        pred_mean = float(p[mask].mean())
        actual_rate = float(y[mask].mean())
        rows.append({
            "bin": int(idx),
            "count": int(mask.sum()),
            "pred_mean": pred_mean,
            "actual_rate": actual_rate,
            "abs_error": float(abs(pred_mean - actual_rate)),
        })
    return rows


def _brier(actual: pd.Series, pred: pd.Series) -> float | None:
    y = pd.to_numeric(actual, errors="coerce").fillna(0.0).to_numpy(dtype=np.float64)
    p = pd.to_numeric(pred, errors="coerce").fillna(0.0).clip(0.0, 1.0).to_numpy(dtype=np.float64)
    if len(y) == 0:
        return None
    return float(np.mean((p - y) ** 2))


def _ev_bucket_rows(frame: pd.DataFrame, markout_col: str, extreme_col: str) -> list[dict[str, float | int]]:
    if frame.empty or "risk_bucket" not in frame:
        return []
    rows: list[dict[str, float | int]] = []
    for bucket, part in frame.groupby("risk_bucket", dropna=True):
        markout = pd.to_numeric(part[markout_col], errors="coerce").fillna(0.0)
        pred_ev = _predicted_value(part)
        extreme = pd.to_numeric(part[extreme_col], errors="coerce").fillna(0.0)
        rows.append({
            "bucket": int(bucket),
            "count": int(len(part)),
            "pred_ev_mean": float(pred_ev.mean()),
            "actual_markout_30s_mean": float(markout.mean()),
            "extreme_rate": float(extreme.mean()),
        })
    return rows


def _bootstrap_delta_ci(
    frame: pd.DataFrame,
    markout_col: str,
    samples: int,
    seed: int = 7,
) -> dict[str, float | None]:
    if samples <= 0 or frame.empty or "risk_bucket" not in frame:
        return {"bucket_delta_ci_low": None, "bucket_delta_ci_high": None}
    buckets = sorted(int(v) for v in frame["risk_bucket"].dropna().unique())
    if len(buckets) < 2:
        return {"bucket_delta_ci_low": None, "bucket_delta_ci_high": None}
    best_bucket = buckets[0]
    worst_bucket = buckets[-1]
    best = pd.to_numeric(
        frame.loc[frame["risk_bucket"] == best_bucket, markout_col],
        errors="coerce",
    ).fillna(0.0).to_numpy(dtype=np.float64)
    worst = pd.to_numeric(
        frame.loc[frame["risk_bucket"] == worst_bucket, markout_col],
        errors="coerce",
    ).fillna(0.0).to_numpy(dtype=np.float64)
    if len(best) == 0 or len(worst) == 0:
        return {"bucket_delta_ci_low": None, "bucket_delta_ci_high": None}
    rng = np.random.default_rng(seed)
    deltas = np.empty(samples, dtype=np.float64)
    for idx in range(samples):
        best_sample = rng.choice(best, size=len(best), replace=True)
        worst_sample = rng.choice(worst, size=len(worst), replace=True)
        deltas[idx] = best_sample.mean() - worst_sample.mean()
    return {
        "bucket_delta_ci_low": float(np.percentile(deltas, 2.5)),
        "bucket_delta_ci_high": float(np.percentile(deltas, 97.5)),
    }


def _metric_rows(
    labels: pd.DataFrame,
    model: QuoteEVModel,
    side_prefix: str,
    name: str,
    bootstrap_samples: int = 200,
) -> dict[str, float | str | int | list | None]:
    filled_col = f"label_{side_prefix}_filled"
    markout_col = f"label_{side_prefix}_fill_markout_30s"
    extreme_col = f"label_{side_prefix}_extreme_adverse_any"
    scored = labels.copy()
    preds = [model.predict(row.to_dict()) for _, row in scored.iterrows()]
    scored[PREDICTED_VALUE_COLUMN] = [
        pred.expected_maker_markout_bps_per_opportunity_30s for pred in preds
    ]
    scored["pred_fill_prob"] = [pred.lifecycle_fill_probability for pred in preds]
    scored["pred_fill_markout_30s"] = [pred.maker_markout_bps_given_fill_30000ms for pred in preds]
    scored["pred_extreme_adverse"] = [pred.extreme_adverse_probability_given_fill_30000ms for pred in preds]
    fill_actual = pd.to_numeric(scored[filled_col], errors="coerce").fillna(0.0).clip(0.0, 1.0)
    filled = scored.loc[fill_actual > 0].copy()
    if filled.empty:
        return {
            "model": name,
            "quote_rows": int(len(scored)),
            "filled_rows": 0,
            "fill_brier": _brier(fill_actual, scored["pred_fill_prob"]),
            "fill_calibration_bins": _calibration_bins(fill_actual, scored["pred_fill_prob"]),
        }
    actual = pd.to_numeric(filled[markout_col], errors="coerce").fillna(0.0)
    pred_markout = pd.to_numeric(filled["pred_fill_markout_30s"], errors="coerce").fillna(0.0)
    if actual.std() > 1e-12 and pred_markout.std() > 1e-12:
        corr = float(np.corrcoef(actual.to_numpy(), pred_markout.to_numpy())[0, 1])
    else:
        corr = 0.0
    extreme = pd.to_numeric(filled[extreme_col], errors="coerce").fillna(0.0)
    pred_extreme = pd.to_numeric(filled["pred_extreme_adverse"], errors="coerce").fillna(0.0)
    auc = None
    if extreme.nunique() > 1 and pred_extreme.std() > 1e-12:
        try:
            from sklearn.metrics import roc_auc_score

            auc = float(roc_auc_score(extreme, pred_extreme))
        except Exception:
            auc = None
    q = max(1, min(5, int(len(filled) / 20)))
    try:
        filled["risk_bucket"] = pd.qcut(
            -filled[PREDICTED_VALUE_COLUMN], q=5, labels=False, duplicates="drop"
        )
        worst = filled.loc[filled["risk_bucket"] == filled["risk_bucket"].max()]
        best = filled.loc[filled["risk_bucket"] == filled["risk_bucket"].min()]
    except Exception:
        filled["risk_bucket"] = np.nan
        worst = filled.nlargest(q, "pred_extreme_adverse")
        best = filled.nsmallest(q, "pred_extreme_adverse")
    bucket_delta = (
        float(pd.to_numeric(best[markout_col], errors="coerce").fillna(0.0).mean())
        - float(pd.to_numeric(worst[markout_col], errors="coerce").fillna(0.0).mean())
        if len(best) and len(worst) else 0.0
    )
    ci = _bootstrap_delta_ci(filled, markout_col, bootstrap_samples)
    return {
        "model": name,
        "quote_rows": int(len(scored)),
        "filled_rows": int(len(filled)),
        "fill_brier": _brier(fill_actual, scored["pred_fill_prob"]),
        "fill_calibration_bins": _calibration_bins(fill_actual, scored["pred_fill_prob"]),
        "actual_markout_30s_mean": float(actual.mean()),
        "pred_fill_markout_corr": corr,
        "pred_extreme_auc": auc,
        "extreme_brier": _brier(extreme, pred_extreme),
        "extreme_calibration_bins": _calibration_bins(extreme, pred_extreme),
        "worst_bucket_rows": int(len(worst)),
        "worst_bucket_actual_markout_30s": float(pd.to_numeric(worst[markout_col], errors="coerce").fillna(0.0).mean()) if len(worst) else 0.0,
        "best_bucket_actual_markout_30s": float(pd.to_numeric(best[markout_col], errors="coerce").fillna(0.0).mean()) if len(best) else 0.0,
        "best_minus_worst_markout_30s": bucket_delta,
        **ci,
        "worst_bucket_extreme_rate": float(pd.to_numeric(worst[extreme_col], errors="coerce").fillna(0.0).mean()) if len(worst) else 0.0,
        "best_bucket_extreme_rate": float(pd.to_numeric(best[extreme_col], errors="coerce").fillna(0.0).mean()) if len(best) else 0.0,
        "ev_bucket_realized_vs_pred": _ev_bucket_rows(filled, markout_col, extreme_col),
    }
