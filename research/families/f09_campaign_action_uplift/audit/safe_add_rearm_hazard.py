#!/usr/bin/env python3
"""Audit when an add-side quote can safely rearm before the baseline cooldown.

The runner keeps the configured fill cooldown active.  It submits independent
shadow orders at predeclared elapsed-time bins and at the true cooldown end.
Shadow orders consume the same causal L2/trade tape and queue calibration, but
they never change baseline orders, inventory, RNG streams, or future actions.

This is an opportunity-level action-uplift audit, not a policy.  A selected
state-dependent schedule still requires a separate paired replay before it can
change live behavior.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import math
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models import backtest_tick as bt  # noqa: E402
from research.families.f01_fixed_parameter_racing import daily_smoke_sweep as smoke  # noqa: E402
from research.families.f05_fill_quality_quote_ev.audit.fill_toxicity_incremental import (  # noqa: E402
    blocked_day_folds,
    chronological_folds,
)
from models.backtest_config import (  # noqa: E402
    load_tick_base_params,
    validate_formal_replay_calibration,
)

SCHEMA_VERSION = "safe_add_rearm_hazard.v1.1"
DEFAULT_BINS_S = (5.0, 10.0, 20.0, 40.0, 60.0, 85.0)
MODEL_FEATURES = (
    "actual_elapsed_ms",
    "cooldown_total_ms",
    "cooldown_remaining_ms",
    "consecutive_units",
    "inventory",
    "inventory_ratio",
    "campaign_age_s",
    "campaign_max_abs_qty_so_far",
    "campaign_pnl_so_far",
    "campaign_adverse_excursion_so_far",
    "campaign_exposure_increasing_fills_so_far",
    "campaign_reducing_fills_so_far",
    "distance_to_mid",
    "quote_delta_to_bbo",
    "toxicity",
    "markout_ema",
    "microprice_shift_bps",
    "l2_quote_flip_rate",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
    "l2_near_depth_total",
    "repair_probability",
    "global_flow_pressure",
    "submit_local_rank",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normalize_days(days: list[str]) -> list[str]:
    return smoke._normalize_days(days)


def _live_like_params(base: dict[str, Any]) -> None:
    base["fill_cooldown_reset_consec_on_expiry"] = False
    base["queue_profile_source"] = (
        "queue_calibration_artifact"
        if base.get("queue_calibration_replay_params")
        else "config_without_queue_artifact"
    )


def _run_day(task: tuple[str, str, dict[str, Any]]) -> dict[str, Any]:
    day, symbol, raw_base = task
    base = dict(raw_base)
    model_dir = base.get("resolved_model_dir") or base.get("model_dir")
    bt.configure_symbol(symbol, model_dir_override=model_dir)
    started = time.perf_counter()
    window = smoke._load_window(day, base)
    result = bt._simulate_tick_with_engine(
        "python",
        window["trades"],
        window["var_ts_ms"],
        window["var_ssq"],
        base,
        ml_data=window["ml_data"],
        bbo_data=window["bbo_data"],
        l2_data=window["l2_data"],
        var_ti=window["var_ti"],
        var_retsq=window["var_retsq"],
    )
    probes = [{"day": day, **row} for row in result.get("_safe_add_rearm_trace", [])]
    summary = {
        "day": day,
        "runtime_s": time.perf_counter() - started,
        "pnl": float(result.get("pnl", 0.0) or 0.0),
        "fills_total": int(result.get("fills_total", 0) or 0),
        "fills_bid": int(result.get("fills_bid", 0) or 0),
        "fills_ask": int(result.get("fills_ask", 0) or 0),
        "final_inventory": float(result.get("final_inventory", 0.0) or 0.0),
        "abs_inventory_time_s": float(result.get("abs_inventory_time_s", 0.0) or 0.0),
        "probe_rows": len(probes),
    }
    return {"day": day, "probes": probes, "summary": summary}


def _numeric(frame: pd.DataFrame, name: str, default: float = math.nan) -> pd.Series:
    if name not in frame:
        return pd.Series(default, index=frame.index, dtype=float)
    return pd.to_numeric(frame[name], errors="coerce")


def normalize_probe_frame(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    if output.empty:
        return output
    output["side"] = output["side"].astype(str).str.upper()
    output["actionable"] = _numeric(output, "actionable", 0.0).fillna(0).astype(int)
    output["baseline_cooldown_end"] = (
        _numeric(output, "baseline_cooldown_end", 0.0).fillna(0).astype(int)
    )
    output["would_fill"] = output["outcome"].astype(str).eq("would_fill").astype(int)
    output["markout_30s_bps"] = _numeric(output, "markout_30s_bps")
    output["opportunity_value_30s_bps"] = _numeric(
        output,
        "opportunity_value_30s_bps",
        0.0,
    ).fillna(0.0)
    output["safe_fill_30s"] = (
        (output["would_fill"] == 1) & (output["markout_30s_bps"] > 0.0)
    ).astype(int)
    output["toxic_fill_30s"] = (
        (output["would_fill"] == 1) & (output["markout_30s_bps"] < 0.0)
    ).astype(int)
    output["episode_id"] = (
        output["day"].astype(str)
        + "|"
        + output["side"].astype(str)
        + "|"
        + _numeric(output, "episode_fill_ts_ms", 0.0).fillna(0).astype("int64").astype(str)
    )
    return output


def summarize_probes(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if frame.empty:
        return pd.DataFrame(rows)
    for (side, elapsed_ms, baseline_end), group in frame.groupby(
        ["side", "scheduled_elapsed_ms", "baseline_cooldown_end"],
        dropna=False,
        sort=True,
    ):
        actionable = group[group["actionable"] == 1]
        fills = actionable[actionable["would_fill"] == 1]
        daily = actionable.groupby("day", sort=True)["opportunity_value_30s_bps"].mean()
        rows.append(
            {
                "side": side,
                "scheduled_elapsed_s": float(elapsed_ms) / 1000.0,
                "baseline_cooldown_end": int(baseline_end),
                "rows": len(group),
                "actionable_rows": len(actionable),
                "actionable_rate": len(actionable) / max(len(group), 1),
                "would_fills": len(fills),
                "would_fill_rate": len(fills) / max(len(actionable), 1),
                "safe_fills_30s": int(fills["safe_fill_30s"].sum()) if len(fills) else 0,
                "toxic_fills_30s": int(fills["toxic_fill_30s"].sum()) if len(fills) else 0,
                "conditional_fill_markout_30s_mean_bps": float(
                    fills["markout_30s_bps"].mean()
                )
                if len(fills)
                else math.nan,
                "opportunity_value_30s_mean_bps": float(
                    actionable["opportunity_value_30s_bps"].mean()
                )
                if len(actionable)
                else math.nan,
                "opportunity_value_30s_p10_bps": float(
                    actionable["opportunity_value_30s_bps"].quantile(0.10)
                )
                if len(actionable)
                else math.nan,
                "daily_positive_rate": float((daily > 0.0).mean()) if len(daily) else math.nan,
                "days": int(daily.index.nunique()),
            }
        )
    return pd.DataFrame(rows)


def paired_uplift(frame: pd.DataFrame) -> pd.DataFrame:
    actionable = frame[frame["actionable"] == 1].copy()
    baseline = actionable[actionable["baseline_cooldown_end"] == 1][
        ["episode_id", "opportunity_value_30s_bps"]
    ].rename(columns={"opportunity_value_30s_bps": "baseline_end_value_30s_bps"})
    baseline = baseline.drop_duplicates("episode_id", keep="first")
    early = actionable[actionable["baseline_cooldown_end"] == 0].merge(
        baseline,
        on="episode_id",
        how="inner",
        validate="many_to_one",
    )
    if early.empty:
        return pd.DataFrame()
    early["paired_uplift_30s_bps"] = (
        early["opportunity_value_30s_bps"] - early["baseline_end_value_30s_bps"]
    )
    rows: list[dict[str, Any]] = []
    for (side, elapsed_ms), group in early.groupby(
        ["side", "scheduled_elapsed_ms"],
        sort=True,
    ):
        daily = group.groupby("day", sort=True)["paired_uplift_30s_bps"].mean()
        rows.append(
            {
                "side": side,
                "scheduled_elapsed_s": float(elapsed_ms) / 1000.0,
                "paired_episodes": int(group["episode_id"].nunique()),
                "paired_uplift_30s_mean_bps": float(group["paired_uplift_30s_bps"].mean()),
                "paired_uplift_30s_median_bps": float(group["paired_uplift_30s_bps"].median()),
                "paired_positive_rate": float((group["paired_uplift_30s_bps"] > 0.0).mean()),
                "daily_positive_rate": float((daily > 0.0).mean()),
                "days": int(daily.index.nunique()),
            }
        )
    return pd.DataFrame(rows)


def fit_walk_forward_scores(
    frame: pd.DataFrame,
    *,
    min_train_days: int,
    test_days: int,
    embargo_days: int,
    late_days: int,
    blocked_folds: int,
    min_train_rows: int,
    min_test_rows: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    actionable = frame[
        (frame["actionable"] == 1) & (frame["baseline_cooldown_end"] == 0)
    ].copy()
    if actionable.empty:
        return pd.DataFrame(), pd.DataFrame()
    folds = chronological_folds(
        actionable["day"].astype(str).tolist(),
        min_train_days=min_train_days,
        test_days=test_days,
        embargo_days=embargo_days,
        late_days=late_days,
    )
    folds.extend(
        blocked_day_folds(
            actionable["day"].astype(str).tolist(),
            folds=blocked_folds,
            late_days=late_days,
        )
    )
    predictions: list[pd.DataFrame] = []
    metrics: list[dict[str, Any]] = []
    features = [name for name in MODEL_FEATURES if name in actionable]
    for side in ("BUY", "SELL"):
        side_frame = actionable[actionable["side"] == side].copy()
        for fold in folds:
            train = side_frame[side_frame["day"].astype(str).isin(fold.train_days)].copy()
            test = side_frame[side_frame["day"].astype(str).isin(fold.test_days)].copy()
            if len(train) < min_train_rows or len(test) < min_test_rows:
                metrics.append(
                    {
                        "side": side,
                        "panel": fold.panel,
                        "fold": fold.fold,
                        "status": "insufficient_rows",
                        "train_rows": len(train),
                        "test_rows": len(test),
                    }
                )
                continue
            fold_features = [
                name
                for name in features
                if pd.to_numeric(train[name], errors="coerce").notna().any()
            ]
            if not fold_features:
                continue
            model = Pipeline(
                [
                    ("impute", SimpleImputer(strategy="median", add_indicator=True)),
                    ("scale", StandardScaler()),
                    ("ridge", Ridge(alpha=10.0)),
                ]
            )
            target = "opportunity_value_30s_bps"
            model.fit(train[fold_features], train[target])
            scored = test.copy()
            scored["safe_rearm_score"] = model.predict(test[fold_features])
            threshold = float(scored["safe_rearm_score"].quantile(0.75))
            high = scored[scored["safe_rearm_score"] >= threshold]
            scored["score_high_q25"] = (scored["safe_rearm_score"] >= threshold).astype(int)
            scored["panel"] = fold.panel
            scored["fold"] = fold.fold
            predictions.append(scored)
            metrics.append(
                {
                    "side": side,
                    "panel": fold.panel,
                    "fold": fold.fold,
                    "status": "ok",
                    "train_rows": len(train),
                    "test_rows": len(test),
                    "high_rows": len(high),
                    "all_value_30s_mean_bps": float(scored[target].mean()),
                    "high_value_30s_mean_bps": float(high[target].mean()),
                    "high_minus_all_value_30s_bps": float(
                        high[target].mean() - scored[target].mean()
                    ),
                    "all_would_fill_rate": float(scored["would_fill"].mean()),
                    "high_would_fill_rate": float(high["would_fill"].mean()),
                    "all_toxic_fill_rate": float(scored["toxic_fill_30s"].mean()),
                    "high_toxic_fill_rate": float(high["toxic_fill_30s"].mean()),
                    "test_start_day": min(fold.test_days),
                    "test_end_day": max(fold.test_days),
                }
            )
    return pd.concat(predictions, ignore_index=True) if predictions else pd.DataFrame(), pd.DataFrame(metrics)


def _markdown(
    *,
    metadata: dict[str, Any],
    summary: pd.DataFrame,
    paired: pd.DataFrame,
    model_metrics: pd.DataFrame,
) -> str:
    def _table(frame: pd.DataFrame) -> str:
        if frame.empty:
            return ""
        display = frame.copy()
        for column in display.select_dtypes(include=["float", "float64"]).columns:
            display[column] = display[column].map(
                lambda value: "" if pd.isna(value) else f"{float(value):.4f}"
            )
        headers = [str(column) for column in display.columns]
        rows = [[str(value) for value in row] for row in display.itertuples(index=False, name=None)]
        lines = [
            "| " + " | ".join(headers) + " |",
            "| " + " | ".join("---" for _ in headers) + " |",
        ]
        lines.extend("| " + " | ".join(row) + " |" for row in rows)
        return "\n".join(lines)

    lines = [
        "# Safe add rearm hazard v1",
        "",
        f"- Schema: `{SCHEMA_VERSION}`",
        f"- Baseline cooldown: `{metadata.get('fill_cooldown_s')}s`",
        f"- Config SHA-256: `{metadata.get('config_sha256')}`",
        f"- Days: `{metadata.get('day_count')}`",
        f"- Probe lifetime: `{metadata.get('probe_lifetime_s')}s`",
        "- Policy status: shadow evidence only; no strategy action is changed.",
        "",
        "The primary target is 30s maker-signed markout per actionable rearm opportunity. "
        "No-fill is zero. This is not a complete campaign counterfactual.",
        "",
        "## Probe summary",
        "",
    ]
    if summary.empty:
        lines.append("No probe rows were available.")
    else:
        lines.append(_table(summary))
    lines.extend(["", "## Paired early vs cooldown-end", ""])
    if paired.empty:
        lines.append("No episode had both an actionable early probe and cooldown-end probe.")
    else:
        lines.append(_table(paired))
    lines.extend(["", "## Chronological and blocked-day score", ""])
    if model_metrics.empty:
        lines.append("Insufficient chronological data for score fitting.")
    else:
        lines.append(_table(model_metrics))
    lines.extend(
        [
            "",
            "## Interpretation boundary",
            "",
            "A positive bucket is only a candidate rearm region. Promotion requires a frozen "
            "state-dependent schedule in a separate paired replay, with campaign terminal, tail, "
            "fills, queue/action mix, and untouched later-day gates.",
            "",
        ]
    )
    return "\n".join(lines)


def _write_outputs(
    prefix: Path,
    *,
    probes: pd.DataFrame,
    daily: pd.DataFrame,
    metadata: dict[str, Any],
    args: argparse.Namespace,
) -> dict[str, str]:
    prefix.parent.mkdir(parents=True, exist_ok=True)
    normalized = normalize_probe_frame(probes)
    summary = summarize_probes(normalized)
    paired = paired_uplift(normalized)
    scored, model_metrics = fit_walk_forward_scores(
        normalized,
        min_train_days=args.min_train_days,
        test_days=args.test_days,
        embargo_days=args.embargo_days,
        late_days=args.late_days,
        blocked_folds=args.blocked_folds,
        min_train_rows=args.min_train_rows,
        min_test_rows=args.min_test_rows,
    )
    paths = {
        "probes": str(prefix.with_suffix(".probes.csv")),
        "daily": str(prefix.with_suffix(".daily.csv")),
        "summary": str(prefix.with_suffix(".summary.csv")),
        "paired": str(prefix.with_suffix(".paired.csv")),
        "scored": str(prefix.with_suffix(".scored.csv")),
        "model_metrics": str(prefix.with_suffix(".model_metrics.csv")),
        "json": str(prefix.with_suffix(".json")),
        "markdown": str(prefix.with_suffix(".md")),
    }
    normalized.to_csv(paths["probes"], index=False)
    daily.to_csv(paths["daily"], index=False)
    summary.to_csv(paths["summary"], index=False)
    paired.to_csv(paths["paired"], index=False)
    scored.to_csv(paths["scored"], index=False)
    model_metrics.to_csv(paths["model_metrics"], index=False)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "metadata": metadata,
        "row_counts": {
            "probes": len(normalized),
            "daily": len(daily),
            "paired": len(paired),
            "scored": len(scored),
        },
        "artifacts": paths,
    }
    Path(paths["json"]).write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    Path(paths["markdown"]).write_text(
        _markdown(metadata=metadata, summary=summary, paired=paired, model_metrics=model_metrics),
        encoding="utf-8",
    )
    return paths


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default="BTCUSDC")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--days", nargs="+", required=True)
    parser.add_argument("--output-prefix", type=Path, required=True)
    parser.add_argument("--bins-s", nargs="+", type=float, default=list(DEFAULT_BINS_S))
    parser.add_argument("--probe-lifetime-s", type=float, default=5.0)
    parser.add_argument("--trace-max", type=int, default=100_000)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--window-cache-dir", default=None)
    parser.add_argument("--refresh-window-cache", action="store_true")
    parser.add_argument(
        "--refresh-partials",
        action="store_true",
        help="Ignore completed day partials and rerun their replay.",
    )
    parser.add_argument("--live-like-replay-baseline", action="store_true")
    parser.add_argument("--strict-calibration", action="store_true")
    parser.add_argument("--live-perf-telemetry", type=Path, default=None)
    parser.add_argument("--live-perf-latency-mode", choices=("avg", "max", "sum"), default="avg")
    parser.add_argument(
        "--latency-profile-id",
        default="",
        help=(
            "Operator-defined immutable environment label for empirical latency."
        ),
    )
    parser.add_argument("--min-train-days", type=int, default=30)
    parser.add_argument("--test-days", type=int, default=10)
    parser.add_argument("--embargo-days", type=int, default=1)
    parser.add_argument("--late-days", type=int, default=10)
    parser.add_argument("--blocked-folds", type=int, default=5)
    parser.add_argument("--min-train-rows", type=int, default=200)
    parser.add_argument("--min-test-rows", type=int, default=50)
    args = parser.parse_args(argv)

    if args.trace_max <= 0:
        raise SystemExit("--trace-max must be positive")
    if args.probe_lifetime_s <= 0.0:
        raise SystemExit("--probe-lifetime-s must be positive")
    if args.live_perf_telemetry is not None and not args.latency_profile_id.strip():
        raise SystemExit(
            "--latency-profile-id is required with --live-perf-telemetry"
        )
    days = _normalize_days(args.days)
    config = args.config.expanduser().resolve()
    bt.configure_symbol(args.symbol)
    base = load_tick_base_params(
        symbol=args.symbol,
        config_path=config,
        configure_symbol=bt.configure_symbol,
        require_historical_bbo=True,
        strict_calibration=bool(args.strict_calibration),
    )
    base.update(
        {
            "trace_quotes_max": 0,
            "trace_decisions_max": 0,
            "trace_queue_events_max": 0,
            "trace_fills_max": 0,
            "trace_safe_add_rearm_max": int(args.trace_max),
            "safe_add_rearm_probe_bins_s": list(args.bins_s),
            "safe_add_rearm_probe_lifetime_s": float(args.probe_lifetime_s),
            "queue_regime_calibration_enabled": True,
        }
    )
    if args.live_like_replay_baseline:
        _live_like_params(base)
    if args.live_perf_telemetry is not None:
        samples = bt._load_live_perf_latency_samples(
            args.live_perf_telemetry,
            mode=args.live_perf_latency_mode,
        )
        base["_new_order_latency_samples_ms"] = samples["new_order_latency_samples_ms"]
        base["_cancel_order_latency_samples_ms"] = samples["cancel_order_latency_samples_ms"]
    if args.strict_calibration:
        validate_formal_replay_calibration(base, require_latency=True)
    if args.window_cache_dir:
        base["_window_cache_dir"] = str(Path(args.window_cache_dir).expanduser().resolve())
    if args.refresh_window_cache:
        base["_refresh_window_cache"] = True

    output_prefix = args.output_prefix.expanduser().resolve()
    partial_dir = output_prefix.parent / f"{output_prefix.name}.partial"
    partial_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []
    pending_days: list[str] = []
    for day in days:
        probe_path = partial_dir / f"{day}.probes.csv"
        daily_path = partial_dir / f"{day}.daily.csv"
        if not args.refresh_partials and probe_path.exists() and daily_path.exists():
            probe_frame = pd.read_csv(probe_path) if probe_path.stat().st_size else pd.DataFrame()
            daily_frame = pd.read_csv(daily_path)
            results.append(
                {
                    "day": day,
                    "probes": probe_frame.to_dict("records"),
                    "summary": daily_frame.iloc[0].to_dict(),
                }
            )
            print(f"{day}: reused partial probes={len(probe_frame)}", flush=True)
        else:
            pending_days.append(day)
    tasks = [(day, args.symbol, base) for day in pending_days]
    workers = max(1, min(int(args.workers), max(len(tasks), 1)))
    if len(tasks) == 0:
        pass
    elif workers == 1:
        iterator = map(_run_day, tasks)
        for item in iterator:
            results.append(item)
            pd.DataFrame(item["probes"]).to_csv(partial_dir / f"{item['day']}.probes.csv", index=False)
            pd.DataFrame([item["summary"]]).to_csv(partial_dir / f"{item['day']}.daily.csv", index=False)
            print(f"{item['day']}: probes={len(item['probes'])} fills={item['summary']['fills_total']}", flush=True)
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_run_day, task): task[0] for task in tasks}
            for future in concurrent.futures.as_completed(futures):
                item = future.result()
                results.append(item)
                pd.DataFrame(item["probes"]).to_csv(partial_dir / f"{item['day']}.probes.csv", index=False)
                pd.DataFrame([item["summary"]]).to_csv(partial_dir / f"{item['day']}.daily.csv", index=False)
                print(f"{item['day']}: probes={len(item['probes'])} fills={item['summary']['fills_total']}", flush=True)
    results.sort(key=lambda item: item["day"])
    probes = pd.DataFrame([row for item in results for row in item["probes"]])
    daily = pd.DataFrame([item["summary"] for item in results])
    metadata = {
        "symbol": args.symbol.upper(),
        "config_path": str(config),
        "config_sha256": _sha256(config),
        "day_count": len(days),
        "days": days,
        "fill_cooldown_s": float(base.get("fill_cooldown", 0.0) or 0.0),
        "probe_bins_s": list(args.bins_s),
        "probe_lifetime_s": float(args.probe_lifetime_s),
        "shadow_cancel_ack_latency": True,
        "shadow_fill_while_pending_cancel": True,
        "blocked_folds": int(args.blocked_folds),
        "live_like_replay_baseline": bool(args.live_like_replay_baseline),
        "latency_source": str(args.live_perf_telemetry or "configured_constant"),
        "latency_source_sha256": (
            _sha256(args.live_perf_telemetry.expanduser().resolve())
            if args.live_perf_telemetry is not None
            else ""
        ),
        "latency_profile_id": str(args.latency_profile_id),
        "live_perf_latency_mode": str(args.live_perf_latency_mode),
        "engine": "python_authoritative_shadow_counterfactual",
        "policy_enabled": False,
    }
    paths = _write_outputs(
        output_prefix,
        probes=probes,
        daily=daily,
        metadata=metadata,
        args=args,
    )
    print(json.dumps(paths, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
