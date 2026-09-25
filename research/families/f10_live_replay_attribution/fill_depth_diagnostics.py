#!/usr/bin/env python3
"""F10 fill/depth diagnostics. No training or replay CLI forwarding."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[3]

from data_quality import COMPLETE_DATA_POLICY, excluded_orderbook_days  # noqa: E402
from live.config import load_config  # noqa: E402
from models.backtest_config import (  # noqa: E402
    resolve_backtest_config_path,
)
from models.symbol_paths import paths_for  # noqa: E402

LIVE_TARGETS = [
    "touch_conditioned_price_change_fraction_10000ms",
    "touch_conditioned_price_change_fraction_30000ms",
    "touch_conditioned_price_change_fraction_60000ms",
    "touch_conditioned_up_probability_10000ms",
    "absolute_price_variance_rate_10000ms",
    "touch_side_adverse_probability_bid_10000ms",
    "touch_side_adverse_probability_ask_10000ms",
]

FILL_CONTEXT_COLS = [
    "l2_near_depth_total",
    "l2_quote_flip_rate",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
    "l2_imbalance_l1",
    "l2_imbalance_l3",
    "l2_imbalance_l5",
]

FILL_TRACE_BUCKET_COLS = [
    "book_imb",
    "microprice_shift_bps",
    "near_depth_total",
    "l2_near_depth_total_quote",
    "l2_quote_flip_rate_quote",
    "l2_book_refresh_ratio_quote",
    "l2_book_cancel_ratio_quote",
    "l2_imbalance_l3_quote",
    "d_l2_near_depth_total",
    "d_l2_book_refresh_ratio",
    "d_l2_book_cancel_ratio",
    "queue_init",
    "queue_before",
    "rem_before",
    "age_ms",
    "quote_dist",
    "final_distance_to_mid",
]



def _source_model_dir() -> Path:
    cfg = load_config(resolve_backtest_config_path())
    model_dir = Path(cfg.ml.model_dir)
    return model_dir if model_dir.is_absolute() else ROOT / model_dir


def _data_quality_meta(symbol: str) -> dict[str, Any]:
    return {
        "policy": "COMPLETE_DATA_POLICY",
        "excluded_orderbook_days": sorted(excluded_orderbook_days(symbol)),
        "exclude_source_unresolved_missing_objects": COMPLETE_DATA_POLICY.exclude_source_unresolved_missing_objects,
        "exclude_raw_zero_normalized_missing": COMPLETE_DATA_POLICY.exclude_raw_zero_normalized_missing,
        "exclude_partial_orderbook_days": COMPLETE_DATA_POLICY.exclude_partial_orderbook_days,
        "cross_symbol_orderbook_exclusions": COMPLETE_DATA_POLICY.cross_symbol_orderbook_exclusions,
    }


def _load_feature_context(symbol: str, days: list[str]) -> pd.DataFrame:
    root = paths_for(symbol).feature_dir
    paths = [root / f"features_{day}.parquet" for day in days]
    paths = [path for path in paths if path.exists()]
    chunks: list[pd.DataFrame] = []
    for path in paths:
        schema = set(pq.ParquetFile(path).schema_arrow.names)
        cols = [col for col in FILL_CONTEXT_COLS if col in schema]
        if not cols:
            continue
        frame = pd.read_parquet(path, columns=cols).sort_index()
        frame = frame[~frame.index.duplicated(keep="last")]
        chunks.append(frame)
    if not chunks:
        return pd.DataFrame()
    context = pd.concat(chunks).sort_index()
    return context[~context.index.duplicated(keep="last")]


def _merge_context_asof(fills: pd.DataFrame, context: pd.DataFrame, ts_col: str, suffix: str) -> pd.DataFrame:
    if fills.empty or context.empty:
        return fills
    result = fills.copy()
    result["_row_id"] = np.arange(len(result))
    result["_merge_ts"] = pd.to_datetime(result[ts_col], unit="ms", utc=True)
    right = context.reset_index().rename(columns={context.index.name or "index": "_ctx_ts"})
    right = right.sort_values("_ctx_ts")
    merged = pd.merge_asof(
        result.sort_values("_merge_ts"),
        right,
        left_on="_merge_ts",
        right_on="_ctx_ts",
        direction="backward",
        tolerance=pd.Timedelta("20s"),
    )
    rename = {col: f"{col}_{suffix}" for col in FILL_CONTEXT_COLS if col in merged.columns}
    merged.rename(columns=rename, inplace=True)
    drop_cols = ["_merge_ts", "_ctx_ts"]
    merged.drop(columns=[col for col in drop_cols if col in merged.columns], inplace=True)
    merged.sort_values("_row_id", inplace=True)
    merged.drop(columns=["_row_id"], inplace=True)
    return merged.reset_index(drop=True)


def _enrich_fill_context(fills: pd.DataFrame, context: pd.DataFrame) -> pd.DataFrame:
    enriched = _merge_context_asof(fills, context, "quote_ts", "quote")
    enriched = _merge_context_asof(enriched, context, "fill_ts", "fill")
    for col in FILL_CONTEXT_COLS:
        q_col = f"{col}_quote"
        f_col = f"{col}_fill"
        if q_col in enriched.columns and f_col in enriched.columns:
            enriched[f"d_{col}"] = pd.to_numeric(enriched[f_col], errors="coerce") - pd.to_numeric(enriched[q_col], errors="coerce")
    return enriched


def _feature_buckets(fills: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for (day, side), side_df in fills.groupby(["day", "side"], dropna=False):
        for feature in FILL_TRACE_BUCKET_COLS:
            if feature not in side_df.columns:
                continue
            values = pd.to_numeric(side_df[feature], errors="coerce")
            valid = side_df.loc[values.notna()].copy()
            if len(valid) < 20 or values.nunique(dropna=True) < 2:
                continue
            valid["_bucket"] = pd.qcut(pd.to_numeric(valid[feature], errors="coerce"), q=min(4, values.nunique()), duplicates="drop")
            for bucket, group in valid.groupby("_bucket", observed=True):
                rows.append({
                    "day": day,
                    "side": side,
                    "feature": feature,
                    "bucket": str(bucket),
                    "fills": int(len(group)),
                    "avg_feature": float(pd.to_numeric(group[feature], errors="coerce").mean()),
                    "avg_markout_30s": float(group["markout_30s"].mean()),
                    "avg_ev_30s": float(group["ev_30s"].mean()),
                    "toxic_30s_rate": float(group["toxic_30s"].astype(bool).mean()),
                    "avg_age_ms": float(group["age_ms"].mean()),
                })
    return pd.DataFrame(rows)


def _markout_buckets(fills: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    feature_cols = [col for col in FILL_TRACE_BUCKET_COLS if col in fills.columns]
    for (day, side), side_df in fills.groupby(["day", "side"], dropna=False):
        if len(side_df) < 20 or side_df["markout_30s"].nunique(dropna=True) < 2:
            continue
        temp = side_df.copy()
        temp["markout_bucket"] = pd.qcut(temp["markout_30s"], q=min(4, temp["markout_30s"].nunique()), duplicates="drop")
        for bucket, group in temp.groupby("markout_bucket", observed=True):
            row = {
                "day": day,
                "side": side,
                "markout_bucket": str(bucket),
                "fills": int(len(group)),
                "avg_markout_30s": float(group["markout_30s"].mean()),
                "toxic_30s_rate": float(group["toxic_30s"].astype(bool).mean()),
            }
            for feature in feature_cols:
                row[f"avg_{feature}"] = float(pd.to_numeric(group[feature], errors="coerce").mean())
            rows.append(row)
    return pd.DataFrame(rows)


def _feature_scores(fills: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for (day, side), group in fills.groupby(["day", "side"], dropna=False):
        for feature in FILL_TRACE_BUCKET_COLS:
            if feature not in group.columns:
                continue
            values = pd.to_numeric(group[feature], errors="coerce")
            target = pd.to_numeric(group["markout_30s"], errors="coerce")
            valid = values.notna() & target.notna()
            if valid.sum() < 20 or values[valid].nunique() < 2:
                continue
            rows.append({
                "day": day,
                "side": side,
                "feature": feature,
                "fills": int(valid.sum()),
                "spearman_markout_30s": float(values[valid].corr(target[valid], method="spearman")),
                "pearson_markout_30s": float(values[valid].corr(target[valid], method="pearson")),
            })
    return pd.DataFrame(rows).sort_values(["day", "side", "spearman_markout_30s"], ascending=[True, True, False])


def run_fill_depth_audit(symbol: str, days: list[str], tag: str, trace_fills_max: int) -> None:
    from models import backtest_tick as bt
    from models.tick_ab import (
        base_params,
        clean_result,
        load_window,
        parse_bound,
        slice_window,
    )

    source_dir = _source_model_dir()
    bt.configure_symbol(symbol, model_dir_override=source_dir)
    params = base_params(symbol)
    params["trace_fills_max"] = trace_fills_max
    params["trace_quotes_max"] = 0

    summaries: list[dict[str, Any]] = []
    fill_frames: list[pd.DataFrame] = []
    for day in days:
        if len(day) != 10:
            raise SystemExit(f"Use explicit UTC daily dates YYYY-MM-DD for fill-depth audit: {day}")
        print(f"Running fill-depth audit {symbol} {day} ...")
        # Fill-depth audit 必须按日加载：trace 的 queue/rank/age 需要和日度
        # replay fresh-start 对齐，不能从 day[:7] 的旧月窗口里切片。
        window = load_window(day, params)
        window = slice_window(
            window,
            parse_bound(f"{day} 00:00", is_end=False),
            parse_bound(day, is_end=True),
        )
        result = bt.simulate_tick(
            window["trades"],
            window["var_ts_ms"],
            window["var_ssq"],
            dict(params),
            ml_data=window["ml_data"],
            bbo_data=window["bbo_data"],
            l2_data=window["l2_data"],
            var_ti=window.get("var_ti"),
            var_retsq=window.get("var_retsq"),
            reference_event_tapes=window.get("reference_event_tapes"),
            campaign_repair_data=window.get("campaign_repair_data"),
            campaign_repair_model=window.get("campaign_repair_model"),
            historical_global_flow_data=window.get("historical_global_flow_data"),
        )
        summary = clean_result(result)
        summary["day"] = day
        fills = pd.DataFrame(result.get("_fill_trace", []))
        summary["trace_fills"] = int(len(fills))
        summaries.append(summary)
        if not fills.empty:
            fills.insert(0, "day", day)
            fill_frames.append(fills)
        print(f"  done {day}: pnl={summary.get('pnl', 0.0):.4f} trace_fills={len(fills)}")

    fills_df = pd.concat(fill_frames, ignore_index=True) if fill_frames else pd.DataFrame()
    if fills_df.empty:
        raise SystemExit("No fill trace produced.")
    context = _load_feature_context(symbol, days)
    fills_df = _enrich_fill_context(fills_df, context)

    summary_df = pd.DataFrame(summaries)
    bucket_df = _feature_buckets(fills_df)
    markout_bucket_df = _markout_buckets(fills_df)
    score_df = _feature_scores(fills_df)

    base = bt.RESULTS_DIR / f"execution_depth_fill_audit_{tag}_{symbol.lower()}"
    outputs = {
        "summary": base.with_suffix(".summary.csv"),
        "fills": base.with_suffix(".fills.csv"),
        "feature_buckets": base.with_suffix(".feature_buckets.csv"),
        "markout_buckets": base.with_suffix(".markout_buckets.csv"),
        "scores": base.with_suffix(".scores.csv"),
        "json": base.with_suffix(".json"),
    }
    summary_df.to_csv(outputs["summary"], index=False)
    fills_df.to_csv(outputs["fills"], index=False)
    bucket_df.to_csv(outputs["feature_buckets"], index=False)
    markout_bucket_df.to_csv(outputs["markout_buckets"], index=False)
    score_df.to_csv(outputs["scores"], index=False)
    outputs["json"].write_text(json.dumps({
        "symbol": symbol,
        "days": days,
        "source_model_dir": str(source_dir),
        "outputs": {name: str(path) for name, path in outputs.items()},
    }, indent=2))
    for name, path in outputs.items():
        print(f"Saved {name}: {path}")
