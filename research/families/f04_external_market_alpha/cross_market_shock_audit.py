#!/usr/bin/env python3
"""Cross-market attribution and fill-level shock labels for quote traces.

This is an offline, no-op-to-live diagnostic.  It joins quote decomposition
fills with the feature parquet timeline, then labels each fill as a local
liquidity-shock candidate, reference-confirmed information-shock candidate,
or mixed/unclassified.  The labels are deliberately conservative: they are
for attribution and shadow modelling, not direct live gating.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models.symbol_paths import DEFAULT_SYMBOL, paths_for  # noqa: E402
from data_quality import allowed_timestamp_mask, filter_frame_for_orderbook_quality, mask_valid_horizon  # noqa: E402
from models.quote_context_batch import (  # noqa: E402
    enrich_orders_with_cpp_quote_context,
    merge_cpp_order_context_to_fills,
)


LOCAL_FEATURE_COLS = [
    "close",
    "volume_imbalance",
    "volume_imbalance_30s",
    "volume_imbalance_60s",
    "trade_intensity_60s",
    "vpin_60s",
    "taker_quote_imbalance_5s",
    "taker_quote_imbalance_10s",
    "taker_quote_imbalance_30s",
    "taker_quote_imbalance_60s",
    "taker_buy_sweep_score_10s",
    "taker_buy_sweep_score_30s",
    "taker_sell_sweep_score_10s",
    "taker_sell_sweep_score_30s",
    "l2_spread_bps",
    "l2_microprice_offset_bps",
    "l2_imbalance_l1",
    "l2_imbalance_l3",
    "l2_imbalance_l5",
    "l2_near_depth_total",
    "l2_quote_flip_rate",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
]

CROSS_SUFFIXES = [
    "basis_bps",
    "basis_residual_bps",
    "ret_10s",
    "ret_30s",
    "ret_60s",
    "volatility_60s",
    "volume_imbalance",
    "trade_intensity_60s",
    "vpin_60s",
    "age_s",
    "available",
]
CROSS_PREFIXES = ["cv_ref_perp", "cv_exec_spot", "cv_ref_spot"]
CROSS_FEATURE_COLS = [f"{prefix}_{suffix}" for prefix in CROSS_PREFIXES for suffix in CROSS_SUFFIXES]
FEATURE_COLS = LOCAL_FEATURE_COLS + CROSS_FEATURE_COLS
TRACE_TS_COLS = ("quote_ts", "submit_ts", "activate_ts", "fill_ts", "outcome_ts", "quote_dt")


def _trace_prefix(symbol: str, tag: str) -> Path:
    return paths_for(symbol).results_dir / f"tick_quote_decomposition_{tag}_{symbol.lower()}"


def _to_epoch_ms(series: pd.Series) -> np.ndarray:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().any():
        return numeric.to_numpy(dtype=np.float64, copy=False)

    parsed = pd.to_datetime(series, utc=True, errors="coerce")
    values = parsed.astype("int64").to_numpy(dtype=np.float64, copy=False) / 1_000_000.0
    values[parsed.isna().to_numpy()] = np.nan
    return values


def _filter_trace_for_orderbook_quality(frame: pd.DataFrame, symbol: str, label: str) -> pd.DataFrame:
    if frame is None or frame.empty:
        return frame

    for col in TRACE_TS_COLS:
        if col not in frame.columns:
            continue
        values = _to_epoch_ms(frame[col])
        if not np.isfinite(values).any():
            continue
        keep = allowed_timestamp_mask(values, symbol, label=label)
        if not keep.all():
            return frame.loc[keep].copy()
        return frame
    return frame


def _feature_paths(symbol: str, days: list[str]) -> list[Path]:
    feature_root = paths_for(symbol).feature_dir
    out: list[Path] = []
    for day in days:
        path = feature_root / f"features_{day}.parquet"
        if path.exists():
            out.append(path)
    seen = set()
    uniq = []
    for path in out:
        if path not in seen:
            uniq.append(path)
            seen.add(path)
    return uniq


def _available_columns(path: Path) -> set[str]:
    return set(pq.ParquetFile(path).schema_arrow.names)


def _load_features(symbol: str, days: list[str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in _feature_paths(symbol, days):
        cols = [col for col in FEATURE_COLS if col in _available_columns(path)]
        if not cols:
            continue
        frame = pd.read_parquet(path, columns=cols)
        if not isinstance(frame.index, pd.DatetimeIndex):
            for ts_col in ("timestamp", "ts_ms", "time"):
                if ts_col in frame:
                    frame.index = pd.to_datetime(frame[ts_col], unit="ms", utc=True, errors="coerce")
                    break
        if not isinstance(frame.index, pd.DatetimeIndex):
            raise ValueError(f"{path}: features must have a DatetimeIndex or timestamp column")
        frames.append(frame)
    if not frames:
        raise FileNotFoundError(f"No daily feature parquet files found for {symbol} days={days}")
    features = pd.concat(frames).sort_index()
    features.index = pd.to_datetime(features.index, utc=True, errors="coerce").as_unit("ns")
    features = features[~features.index.duplicated(keep="first")]
    features = filter_frame_for_orderbook_quality(features, symbol, label="cross-market shock feature")
    for col in FEATURE_COLS:
        if col not in features:
            features[col] = 0.0
    return features[FEATURE_COLS]


def _feature_horizon_valid(features: pd.DataFrame, horizon_s: float, max_gap_s: float) -> pd.Series:
    valid = mask_valid_horizon(features.index, horizon_s=horizon_s, max_gap_s=max_gap_s)
    return pd.Series(valid, index=features.index, name=f"_feature_horizon_valid_{int(horizon_s)}s")


def _attach_feature_horizon_valid(frame: pd.DataFrame, valid_by_feature_dt: pd.Series) -> pd.DataFrame:
    out = frame.copy()
    if out.empty or "feature_dt" not in out:
        out[valid_by_feature_dt.name] = False
        return out
    feature_dt = pd.to_datetime(out["feature_dt"], utc=True, errors="coerce")
    matched = valid_by_feature_dt.reindex(pd.DatetimeIndex(feature_dt)).fillna(False).astype(bool)
    out[valid_by_feature_dt.name] = matched.to_numpy(dtype=bool)
    return out


def _read_traces(symbol: str, trace_tag: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    prefix = _trace_prefix(symbol, trace_tag)
    orders_path = prefix.with_suffix(".orders.csv")
    fills_path = prefix.with_suffix(".fills.csv")
    if not orders_path.exists() or not fills_path.exists():
        raise FileNotFoundError(
            f"Missing quote decomposition traces for tag={trace_tag}: "
            f"{orders_path.name}, {fills_path.name}"
        )
    return pd.read_csv(orders_path), pd.read_csv(fills_path)


def _fill_time_index(fills: pd.DataFrame) -> pd.Series:
    ts = pd.to_numeric(fills["fill_ts"], errors="coerce")
    return pd.to_datetime(ts, unit="ms", utc=True, errors="coerce").dt.as_unit("ns")


def _side_position_sign(side: pd.Series) -> pd.Series:
    upper = side.astype(str).str.upper()
    return upper.map({"BUY": 1.0, "BID": 1.0, "SELL": -1.0, "ASK": -1.0}).fillna(0.0)


def _merge_features(fills: pd.DataFrame, features: pd.DataFrame, tolerance_s: float) -> pd.DataFrame:
    left = fills.copy()
    left["fill_dt"] = _fill_time_index(left)
    left = left.sort_values("fill_dt")
    right = features.copy().sort_index()
    right = right.reset_index(names="feature_dt")
    merged = pd.merge_asof(
        left,
        right,
        left_on="fill_dt",
        right_on="feature_dt",
        direction="backward",
        tolerance=pd.Timedelta(seconds=float(tolerance_s)),
    )
    for col in FEATURE_COLS:
        if col not in merged:
            merged[col] = 0.0
        merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0.0)
    return merged.sort_values(["day", "fill_ts", "side", "order_id"], kind="stable")


def _merge_order_features(orders: pd.DataFrame, features: pd.DataFrame, tolerance_s: float) -> pd.DataFrame:
    left = orders.copy()
    ts_col = "quote_ts" if "quote_ts" in left else "submit_ts"
    left["quote_dt"] = pd.to_datetime(
        pd.to_numeric(left[ts_col], errors="coerce"),
        unit="ms",
        utc=True,
        errors="coerce",
    ).dt.as_unit("ns")
    left = left.sort_values("quote_dt")
    right = features.copy().sort_index().reset_index(names="feature_dt")
    merged = pd.merge_asof(
        left,
        right,
        left_on="quote_dt",
        right_on="feature_dt",
        direction="backward",
        tolerance=pd.Timedelta(seconds=float(tolerance_s)),
    )
    for col in FEATURE_COLS:
        if col not in merged:
            merged[col] = 0.0
        merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0.0)
    return merged.sort_values(["day", "quote_ts", "side", "order_id"], kind="stable")


def _max_abs_existing(frame: pd.DataFrame, cols: list[str]) -> pd.Series:
    existing = [col for col in cols if col in frame]
    if not existing:
        return pd.Series(0.0, index=frame.index)
    return frame[existing].abs().max(axis=1)


def _add_labels(fills: pd.DataFrame, *, ret_threshold: float, flow_threshold: float) -> pd.DataFrame:
    out = fills.copy()
    pos = _side_position_sign(out["side"])
    out["position_sign"] = pos

    local_flow_col = "volume_imbalance_60s" if "volume_imbalance_60s" in out else "volume_imbalance"
    out["local_adverse_flow"] = -pos * pd.to_numeric(out.get(local_flow_col, 0.0), errors="coerce").fillna(0.0)
    out["local_adverse_taker_30s"] = -pos * pd.to_numeric(
        out.get("taker_quote_imbalance_30s", 0.0), errors="coerce"
    ).fillna(0.0)
    out["local_adverse_microprice"] = -pos * pd.to_numeric(
        out.get("l2_microprice_offset_bps", out.get("microprice_shift_bps", 0.0)), errors="coerce"
    ).fillna(0.0)
    depth = pd.to_numeric(out.get("l2_near_depth_total", out.get("near_depth_total", 0.0)), errors="coerce").fillna(0.0)
    out["local_near_depth_total"] = depth
    out["local_thin_depth"] = False
    for _, idx in out.groupby("day").groups.items():
        day_depth = depth.loc[idx]
        positive = day_depth[day_depth > 0]
        if not positive.empty:
            out.loc[idx, "local_thin_depth"] = day_depth <= positive.quantile(0.25)
    out["local_pressure_score"] = out[["local_adverse_flow", "local_adverse_taker_30s"]].max(axis=1)
    out["local_pressure"] = (
        (out["local_pressure_score"] >= flow_threshold)
        | ((out["local_adverse_microprice"] > 0.0) & out["local_thin_depth"].astype(bool))
    )

    for horizon in (10, 30, 60):
        col = f"cv_ref_perp_ret_{horizon}s"
        out[f"ref_adverse_ret_{horizon}s"] = -pos * pd.to_numeric(out.get(col, 0.0), errors="coerce").fillna(0.0)
    out["ref_adverse_flow"] = -pos * pd.to_numeric(
        out.get("cv_ref_perp_volume_imbalance", 0.0), errors="coerce"
    ).fillna(0.0)
    out["ref_adverse_ret_max"] = out[["ref_adverse_ret_10s", "ref_adverse_ret_30s", "ref_adverse_ret_60s"]].max(axis=1)
    out["reference_confirmation"] = (
        (out["ref_adverse_ret_max"] >= ret_threshold)
        | ((out["ref_adverse_ret_max"] > 0.0) & (out["ref_adverse_flow"] >= flow_threshold))
    )

    for prefix in ("cv_exec_spot", "cv_ref_spot"):
        for horizon in (10, 30, 60):
            out[f"{prefix}_adverse_ret_{horizon}s"] = -pos * pd.to_numeric(
                out.get(f"{prefix}_ret_{horizon}s", 0.0), errors="coerce"
            ).fillna(0.0)
        out[f"{prefix}_adverse_flow"] = -pos * pd.to_numeric(
            out.get(f"{prefix}_volume_imbalance", 0.0), errors="coerce"
        ).fillna(0.0)
    spot_ret_cols = [
        f"{prefix}_adverse_ret_{horizon}s"
        for prefix in ("cv_exec_spot", "cv_ref_spot")
        for horizon in (10, 30, 60)
    ]
    spot_flow_cols = ["cv_exec_spot_adverse_flow", "cv_ref_spot_adverse_flow"]
    out["spot_adverse_ret_max"] = _max_abs_existing(out, spot_ret_cols)
    out["spot_confirmation"] = (
        (out[spot_ret_cols].max(axis=1) >= ret_threshold if spot_ret_cols else False)
        | (out[spot_flow_cols].max(axis=1) >= flow_threshold if spot_flow_cols else False)
    )
    out["spot_available"] = _max_abs_existing(out, [col for col in CROSS_FEATURE_COLS if col.startswith(("cv_exec_spot", "cv_ref_spot"))]) > 0.0

    for horizon in (1, 5, 30):
        out[f"markout_{horizon}s"] = pd.to_numeric(out.get(f"markout_{horizon}s", 0.0), errors="coerce").fillna(0.0)
        out[f"ttl_recovered_{horizon}s"] = out[f"markout_{horizon}s"] >= 0.0
    out["adverse_tail"] = out.get("toxic_30s", out["markout_30s"] < 0.0).astype(bool) | (out["markout_30s"] <= -10.0)
    out["extreme_adverse_tail"] = out["markout_30s"] <= -50.0
    out["ttl_recovered"] = out["ttl_recovered_5s"] & out["ttl_recovered_30s"]

    queue_before = pd.to_numeric(out.get("queue_before", 0.0), errors="coerce").fillna(0.0)
    rem_before = pd.to_numeric(out.get("rem_before", 0.0), errors="coerce").fillna(0.0)
    queue_init = pd.to_numeric(out.get("queue_init", 0.0), errors="coerce").fillna(0.0)
    out["queue_ahead"] = queue_before
    out["queue_remaining_before"] = rem_before
    out["queue_init"] = queue_init
    out["queue_ahead_ratio"] = queue_before / queue_init.replace(0.0, np.nan)
    out["queue_ahead_ratio"] = out["queue_ahead_ratio"].replace([np.inf, -np.inf], np.nan).fillna(0.0)

    out["ref_explains_adverse"] = out["reference_confirmation"] & out["adverse_tail"]
    labels = np.full(len(out), "mixed_unclassified", dtype=object)
    labels[out["ref_explains_adverse"].to_numpy()] = "reference_information_shock"
    labels[(out["reference_confirmation"] & ~out["adverse_tail"]).to_numpy()] = "reference_confirmed_absorbed"
    labels[(out["local_pressure"] & ~out["reference_confirmation"] & out["ttl_recovered"]).to_numpy()] = "local_liquidity_reversion"
    labels[(out["local_pressure"] & ~out["reference_confirmation"] & ~out["adverse_tail"]).to_numpy()] = "local_liquidity_absorbed"
    labels[(out["local_pressure"] & ~out["reference_confirmation"] & out["adverse_tail"]).to_numpy()] = "local_pressure_adverse_unconfirmed"
    out["shock_label"] = labels
    return out


def _safe_mean(series: pd.Series) -> float:
    if series.empty:
        return 0.0
    return float(pd.to_numeric(series, errors="coerce").mean())


def _summary(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for keys, group in frame.groupby(["day", "side", "shock_label"], dropna=False):
        day, side, label = keys
        rows.append({
            "day": day,
            "side": side,
            "shock_label": label,
            "fills": int(len(group)),
            "avg_markout_1s": _safe_mean(group["markout_1s"]),
            "avg_markout_5s": _safe_mean(group["markout_5s"]),
            "avg_markout_30s": _safe_mean(group["markout_30s"]),
            "adverse_tail_rate": float(group["adverse_tail"].mean()),
            "extreme_adverse_rate": float(group["extreme_adverse_tail"].mean()),
            "ttl_recovery_rate": float(group["ttl_recovered"].mean()),
            "avg_ref_adverse_ret_max": _safe_mean(group["ref_adverse_ret_max"]),
            "avg_ref_adverse_flow": _safe_mean(group["ref_adverse_flow"]),
            "spot_confirmation_rate": float(group["spot_confirmation"].mean()),
            "spot_available_rate": float(group["spot_available"].mean()),
            "avg_spot_adverse_ret_max": _safe_mean(group["spot_adverse_ret_max"]),
            "avg_exec_spot_adverse_flow": _safe_mean(group["cv_exec_spot_adverse_flow"]),
            "avg_ref_spot_adverse_flow": _safe_mean(group["cv_ref_spot_adverse_flow"]),
            "avg_local_pressure_score": _safe_mean(group["local_pressure_score"]),
            "avg_queue_ahead_ratio": _safe_mean(group["queue_ahead_ratio"]),
        })
    return pd.DataFrame(rows).sort_values(["day", "side", "shock_label"])


def _reference_bins(frame: pd.DataFrame) -> pd.DataFrame:
    work = frame.copy()
    work["ref_adverse_ret_bin"] = pd.qcut(
        work["ref_adverse_ret_max"].rank(method="first"),
        q=min(5, max(1, len(work) // 10)),
        labels=False,
        duplicates="drop",
    )
    return (
        work.groupby(["day", "side", "ref_adverse_ret_bin"], dropna=False)
        .agg(
            fills=("order_id", "count"),
            avg_ref_adverse_ret_max=("ref_adverse_ret_max", "mean"),
            avg_ref_adverse_flow=("ref_adverse_flow", "mean"),
            avg_markout_30s=("markout_30s", "mean"),
            adverse_tail_rate=("adverse_tail", "mean"),
            ttl_recovery_rate=("ttl_recovered", "mean"),
        )
        .reset_index()
        .sort_values(["day", "side", "ref_adverse_ret_bin"])
    )


def _correlations(frame: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "local_adverse_flow",
        "local_adverse_taker_30s",
        "local_adverse_microprice",
        "ref_adverse_ret_10s",
        "ref_adverse_ret_30s",
        "ref_adverse_ret_60s",
        "ref_adverse_flow",
        "spot_adverse_ret_max",
        "cv_exec_spot_adverse_flow",
        "cv_ref_spot_adverse_flow",
        "cv_ref_perp_vpin_60s",
        "cv_ref_perp_trade_intensity_60s",
        "queue_ahead_ratio",
    ]
    rows = []
    for side, group in frame.groupby("side"):
        for col in cols:
            if col not in group:
                continue
            x = pd.to_numeric(group[col], errors="coerce")
            for target in ("markout_1s", "markout_5s", "markout_30s", "adverse_tail"):
                y = pd.to_numeric(group[target], errors="coerce")
                if x.notna().sum() < 3 or y.notna().sum() < 3:
                    corr = np.nan
                else:
                    corr = float(x.corr(y.astype(float), method="spearman"))
                rows.append({"side": side, "feature": col, "target": target, "spearman": corr})
    return pd.DataFrame(rows).sort_values(["side", "target", "spearman"], ascending=[True, True, True])


def _to_md(frame: pd.DataFrame, max_rows: int = 80) -> str:
    if frame.empty:
        return "_No rows._"
    shown = frame.head(max_rows).copy()
    for col in shown.columns:
        if pd.api.types.is_float_dtype(shown[col]):
            shown[col] = shown[col].map(lambda value: "" if pd.isna(value) else f"{value:.6g}")
    lines = [
        "| " + " | ".join(shown.columns) + " |",
        "| " + " | ".join("---" for _ in shown.columns) + " |",
    ]
    for _, row in shown.iterrows():
        lines.append("| " + " | ".join(str(row[col]) for col in shown.columns) + " |")
    return "\n".join(lines)


def _write_markdown(path: Path, outputs: dict[str, str], summary: pd.DataFrame,
                    ref_bins: pd.DataFrame, corr: pd.DataFrame, explained: pd.DataFrame) -> None:
    lines = [
        "# Cross-Market Shock Attribution Audit",
        "",
        "Offline diagnostic only.  Labels are conservative candidates, not live gates.",
        "",
        "## Shock Label Summary",
        "",
        _to_md(summary),
        "",
        "## Reference Adverse Return Bins",
        "",
        _to_md(ref_bins),
        "",
        "## Strongest Spearman Links",
        "",
        _to_md(corr.dropna().sort_values("spearman").head(40)),
        "",
        "## BTCUSDT Futures Explained Adverse Fills",
        "",
        _to_md(explained[[
            "day", "side", "fill_ts", "order_id", "markout_30s", "ref_adverse_ret_max",
            "ref_adverse_flow", "local_pressure_score", "queue_ahead_ratio", "shock_label",
        ]].head(60) if not explained.empty else explained),
        "",
        "## Files",
        "",
    ]
    for name, out_path in outputs.items():
        lines.append(f"- {name}: `{out_path}`")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, str]:
    orders, fills = _read_traces(args.symbol, args.trace_tag)
    if "day" not in orders or "day" not in fills:
        raise ValueError("cross-market shock audit requires day-labelled quote decomposition traces")
    days = args.days or sorted(fills["day"].astype(str).unique().tolist())
    orders = orders.loc[orders["day"].astype(str).isin(days)].copy()
    fills = fills.loc[fills["day"].astype(str).isin(days)].copy()
    orders = _filter_trace_for_orderbook_quality(orders, args.symbol, "quote trace orders")
    fills = _filter_trace_for_orderbook_quality(fills, args.symbol, "quote trace fills")
    quote_context = getattr(args, "quote_context", "existing")
    if quote_context == "cpp-batch":
        # cpp-batch quote context 是为了补 quote-time 诊断字段；后续 fill shock label
        # 仍然只做离线归因，不允许反向进入 live policy。
        orders = enrich_orders_with_cpp_quote_context(
            orders,
            symbol=args.symbol,
            days=days,
            cache_dir=getattr(args, "quote_context_cache_dir", None),
            refresh_cache=bool(getattr(args, "refresh_quote_context_cache", False)),
            strict=bool(getattr(args, "strict_cpp_quote_context", False)),
            workers=max(1, int(getattr(args, "quote_context_workers", 1))),
        )
    features = _load_features(args.symbol, days)
    feature_valid_30s = _feature_horizon_valid(
        features,
        horizon_s=30.0,
        max_gap_s=args.max_label_gap_s,
    )
    orders_augmented = _merge_order_features(orders, features, args.feature_tolerance_s)
    orders_augmented = _attach_feature_horizon_valid(orders_augmented, feature_valid_30s)
    labelled = _merge_features(fills, features, args.feature_tolerance_s)
    if quote_context == "cpp-batch":
        labelled = merge_cpp_order_context_to_fills(labelled, orders)
    labelled = _attach_feature_horizon_valid(labelled, feature_valid_30s)
    horizon_col = feature_valid_30s.name
    invalid_horizon = ~labelled[horizon_col].astype(bool)
    if invalid_horizon.any():
        print(f"  Excluding {int(invalid_horizon.sum()):,} fill labels crossing feature horizon/gap")
        labelled = labelled.loc[~invalid_horizon].copy()
    labelled = _add_labels(labelled, ret_threshold=args.ret_threshold, flow_threshold=args.flow_threshold)

    summary = _summary(labelled)
    ref_bins = _reference_bins(labelled)
    corr = _correlations(labelled)
    explained = labelled.loc[labelled["ref_explains_adverse"]].copy()
    explained = explained.sort_values(["markout_30s", "ref_adverse_ret_max"], ascending=[True, False])

    base = paths_for(args.symbol).results_dir / f"cross_market_shock_audit_{args.tag}_{args.symbol.lower()}"
    outputs = {
        "orders_features_csv": str(base.with_suffix(".orders_features.csv")),
        "orders_features_parquet": str(base.with_suffix(".orders_features.parquet")),
        "fill_labels_csv": str(base.with_suffix(".fill_labels.csv")),
        "fill_labels_parquet": str(base.with_suffix(".fill_labels.parquet")),
        "summary": str(base.with_suffix(".summary.csv")),
        "reference_bins": str(base.with_suffix(".reference_bins.csv")),
        "correlations": str(base.with_suffix(".correlations.csv")),
        "explained_fills": str(base.with_suffix(".ref_explained_fills.csv")),
        "json": str(base.with_suffix(".summary.json")),
        "markdown": str(base.with_suffix(".md")),
    }
    orders_augmented.to_csv(outputs["orders_features_csv"], index=False)
    orders_augmented.to_parquet(outputs["orders_features_parquet"], index=False)
    labelled.to_csv(outputs["fill_labels_csv"], index=False)
    labelled.to_parquet(outputs["fill_labels_parquet"], index=False)
    summary.to_csv(outputs["summary"], index=False)
    ref_bins.to_csv(outputs["reference_bins"], index=False)
    corr.to_csv(outputs["correlations"], index=False)
    explained.to_csv(outputs["explained_fills"], index=False)
    Path(outputs["json"]).write_text(json.dumps({
        "symbol": args.symbol.upper(),
        "trace_tag": args.trace_tag,
        "days": days,
        "period_grain": "day",
        "feature_tolerance_s": args.feature_tolerance_s,
        "max_label_gap_s": args.max_label_gap_s,
        "ret_threshold": args.ret_threshold,
        "flow_threshold": args.flow_threshold,
        "outputs": outputs,
        "rows": int(len(labelled)),
        "ref_explained_adverse_fills": int(len(explained)),
        "spot_available_rate": float(labelled["spot_available"].mean()) if len(labelled) else 0.0,
    }, indent=2), encoding="utf-8")
    _write_markdown(Path(outputs["markdown"]), outputs, summary, ref_bins, corr, explained)
    return outputs


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--trace-tag", default="20260530_baseline")
    parser.add_argument("--days", nargs="+", default=None, help="UTC day period filter, e.g. 2026-05-15")
    parser.add_argument("--tag", default=time.strftime("%Y%m%d_%H%M%S"))
    parser.add_argument("--feature-tolerance-s", type=float, default=30.0)
    parser.add_argument(
        "--max-label-gap-s",
        type=float,
        default=15.0,
        help="Max allowed gap on the 10s feature grid when validating future markout horizons.",
    )
    parser.add_argument("--ret-threshold", type=float, default=2e-5)
    parser.add_argument("--flow-threshold", type=float, default=0.35)
    parser.add_argument(
        "--quote-context",
        choices=["existing", "cpp-batch", "off"],
        default="existing",
        help="Optionally refresh quote context from the C++ depth-aware batch core before audit labelling.",
    )
    parser.add_argument("--quote-context-cache-dir", default=None)
    parser.add_argument("--refresh-quote-context-cache", action="store_true")
    parser.add_argument("--strict-cpp-quote-context", action="store_true")
    parser.add_argument(
        "--quote-context-workers",
        type=int,
        default=1,
        help="C++ depth batch workers; keep 1 when outer day/arm workers already saturate the host.",
    )
    args = parser.parse_args()
    outputs = run(args)
    for out_path in outputs.values():
        print(f"Saved {out_path}")


if __name__ == "__main__":
    main()
