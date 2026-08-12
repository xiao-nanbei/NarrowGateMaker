#!/usr/bin/env python3
"""Offline shadow evaluation for quote EV model bundles.

The evaluator scores current and candidate quote_ev models on the same labelled
orders/fills table.  It is intentionally non-trading: the decisive live metric
still needs a tick A/B, but this report checks whether a shadow model improves
filled-quote adverse markout ranking before it is worth wiring into policy.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models.symbol_paths import DEFAULT_SYMBOL, paths_for  # noqa: E402
from research.families.f05_fill_quality_quote_ev.quote_ev import (  # noqa: E402
    QuoteEVModel,
    quote_side_prefix,
)
from research.families.f05_fill_quality_quote_ev.train_quote_ev import build_labels  # noqa: E402

PREDICTED_VALUE_COLUMN = (
    "pred_expected_maker_markout_bps_per_opportunity_30s"
)


def _predicted_value(frame: pd.DataFrame) -> pd.Series:
    if PREDICTED_VALUE_COLUMN in frame:
        return pd.to_numeric(frame[PREDICTED_VALUE_COLUMN], errors="coerce").fillna(0.0)
    # Historical reports remain readable, but new output never writes this alias.
    return pd.to_numeric(frame["pred_ev_30s"], errors="coerce").fillna(0.0)


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
    scored["pred_fill_prob"] = [pred.fill_prob for pred in preds]
    scored["pred_fill_markout_30s"] = [pred.fill_markout_30s for pred in preds]
    scored["pred_extreme_adverse"] = [pred.extreme_adverse_given_fill for pred in preds]
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


def run(args: argparse.Namespace) -> pd.DataFrame:
    side_prefix = quote_side_prefix(args.side)
    side_upper = "BUY" if side_prefix == "bid" else "SELL"
    orders = pd.read_csv(args.orders)
    fills = pd.read_csv(args.fills)
    labels = build_labels(orders, fills, max_inventory=args.max_inventory, side=side_upper)
    if args.days:
        labels = labels.loc[labels["day"].astype(str).isin(args.days)].copy()
    current = QuoteEVModel.load(args.current_model_dir, side=side_prefix)
    candidate = QuoteEVModel.load(args.candidate_model_dir, side=side_prefix)
    rows = [
        _metric_rows(labels, current, side_prefix, "current", args.bootstrap_samples),
        _metric_rows(labels, candidate, side_prefix, "candidate", args.bootstrap_samples),
    ]
    frame = pd.DataFrame(rows)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(args.out, index=False)
    args.out.with_suffix(".json").write_text(json.dumps(rows, indent=2), encoding="utf-8")
    return frame


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--side", choices=["bid", "ask", "BUY", "SELL"], required=True)
    parser.add_argument("--orders", type=Path, required=True)
    parser.add_argument("--fills", type=Path, required=True)
    parser.add_argument("--days", nargs="+", default=[])
    parser.add_argument("--current-model-dir", type=Path, default=None)
    parser.add_argument("--candidate-model-dir", type=Path, required=True)
    parser.add_argument("--max-inventory", type=float, default=None)
    parser.add_argument("--bootstrap-samples", type=int, default=200)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()
    args.current_model_dir = args.current_model_dir or paths_for(args.symbol).model_dir
    args.out = args.out or (
        paths_for(args.symbol).results_dir
        / f"quote_ev_shadow_eval_{quote_side_prefix(args.side)}_{args.symbol.lower()}.csv"
    )
    frame = run(args)
    print(frame.to_string(index=False))
    print(f"Saved {args.out}")
    print(f"Saved {args.out.with_suffix('.json')}")


if __name__ == "__main__":
    main()
