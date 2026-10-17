#!/usr/bin/env python3
"""Run source-separated daily C++ core-baseline and 13-head ML A/B replays.

This runner deliberately excludes Python-only BUY q90 lifecycle actions. Its
outputs are diagnostic core-replay evidence, never a live-stack reproduction or
promotion authority. Provider-normalized days remain sensitivity-only.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import tempfile
import time
from datetime import date, timedelta, timezone, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from data_paths import window_cache_root
from models.prewarm_tick_cache import _days, _quality_authorities

ROOT = Path(__file__).resolve().parent.parent

RESULT_FIELDS = (
    "pnl",
    "terminal_mtm_pnl",
    "inventory_adjusted_pnl",
    "fills_bid",
    "fills_ask",
    "fills_total",
    "avg_markout",
    "abs_inventory_time_s",
    "notional_inventory_time_s",
    "time_avg_abs_inventory",
    "max_inventory",
    "final_inventory",
    "circuit_breaker_count",
    "consecutive_loss_cooldown_trigger_count",
    "book_source_authority",
    "book_dataset_version",
    "book_exact_queue_policy_eligible",
    "replay_evidence_scope",
    "replay_promotion_eligible",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    os.replace(temporary, path)


def _task(payload: dict[str, Any]) -> list[dict[str, Any]]:
    from models import backtest_tick as bt
    from models.backtest_config import disable_ml_params, load_tick_base_params
    from models.data_windows import load_tick_window

    day = str(payload["day"])
    authority = str(payload["source_authority"])
    book_root = Path(str(payload["book_root"])).resolve()
    cache_dir = Path(str(payload["cache_dir"])).resolve()
    feature_dir = (
        Path(str(payload["feature_dir"])).resolve()
        if payload.get("feature_dir")
        else None
    )
    model_dir = (
        Path(str(payload["model_dir"])).resolve()
        if payload.get("model_dir")
        else None
    )
    arms = tuple(str(value) for value in payload["arms"])

    bt.BBO_DIR = book_root / "bbo"
    bt.L2_DIR = book_root / "l2"
    bt.configure_symbol("BTCUSDC")
    base = load_tick_base_params(
        symbol="BTCUSDC",
        config_path=Path(str(payload["config_path"])),
        configure_symbol=bt.configure_symbol,
        require_historical_bbo=True,
    )
    base.update(
        {
            "execution_trade_source": "trades",
            "market_context_warmup_days": 1,
            "replay_event_clock": "merged",
            "queue_ahead_mode": (
                "provider_visible_level"
                if authority == "provider_normalized_causal"
                else "exact_level"
            ),
            "queue_l2_cancel_ahead_enabled": False,
            "_formal_quality_allowed_days": [
                (date.fromisoformat(day) - timedelta(days=1)).isoformat(),
                day,
            ],
            "collect_curves": False,
            # C++ has no native exchange-book q90 scheduler. Excluding it in
            # both arms makes this a clean 13-head core-mechanism A/B.
            "dynamic_fill_hazard_action_enabled": False,
            "dynamic_fill_hazard_shadow_enabled": False,
            "dynamic_fill_hazard_cpp_parity_enabled": False,
            "buy_fill_selection_live_enabled": False,
            "sync_adjust_replay_mode": "disabled",
        }
    )
    needs_ml = "ml_on" in arms
    if needs_ml:
        if feature_dir is None or model_dir is None:
            raise ValueError("ml_on requires feature_dir and model_dir")
        base.update(
            {
                "model_dir": str(model_dir),
                "resolved_model_dir": str(model_dir),
            }
        )
        bt.configure_symbol("BTCUSDC", model_dir_override=model_dir)

    window = load_tick_window(
        day,
        base,
        load_ml=needs_ml,
        require_ml=needs_ml,
        run_ml_inference=needs_ml,
        feature_dir=feature_dir,
        require_target_feature_files=needs_ml,
        cross_market_enabled=needs_ml,
        with_ml_cache=False,
        require_historical_bbo=True,
        require_formal_l2=False,
        cache_dir=cache_dir,
        refresh_cache=False,
    )
    rows: list[dict[str, Any]] = []
    for arm in arms:
        params = dict(base)
        if arm == "ml_off":
            params["ml_enabled"] = False
            disable_ml_params(params)
            ml_data = None
        elif arm == "ml_on":
            params["ml_enabled"] = True
            ml_data = window.ml_data
        else:
            raise ValueError(f"unknown arm: {arm}")
        started = time.perf_counter()
        result = bt._simulate_tick_with_engine(
            "cpp",
            window.trades,
            window.var_ts_ms,
            window.var_ssq,
            params,
            ml_data=ml_data,
            bbo_data=window.bbo_data,
            l2_data=window.l2_data,
            var_ti=window.var_ti,
            var_retsq=window.var_retsq,
        )
        row = {key: result.get(key) for key in RESULT_FIELDS}
        row.update(
            {
                "day": day,
                "arm": arm,
                "source_authority": window.book_source_authority,
                "runtime_s": time.perf_counter() - started,
                "cpp_core_excludes_python_q90": True,
                "cpp_core_excludes_buy_fill_selection": True,
                "action_authority": False,
                "live_authority": False,
            }
        )
        rows.append(row)
    return rows


def _summary(frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for (authority, arm), group in frame.groupby(
        ["source_authority", "arm"], sort=True
    ):
        rows.append(
            {
                "source_authority": authority,
                "arm": arm,
                "days": int(group["day"].nunique()),
                "sum_pnl": float(group["pnl"].sum()),
                "mean_daily_pnl": float(group["pnl"].mean()),
                "sum_terminal_mtm_pnl": float(
                    group["terminal_mtm_pnl"].sum()
                ),
                "sum_inventory_adjusted_pnl": float(
                    group["inventory_adjusted_pnl"].sum()
                ),
                "fills_total": int(group["fills_total"].sum()),
                "fills_bid": int(group["fills_bid"].sum()),
                "fills_ask": int(group["fills_ask"].sum()),
                "mean_daily_abs_inventory_time_s": float(
                    group["abs_inventory_time_s"].mean()
                ),
                "mean_daily_avg_markout": float(group["avg_markout"].mean()),
                "runtime_s": float(group["runtime_s"].sum()),
            }
        )
    return rows


def _paired_summary(
    frame: pd.DataFrame,
    *,
    bootstrap_draws: int = 20_000,
    seed: int = 20260731,
) -> dict[str, Any] | None:
    if set(frame["arm"]) != {"ml_off", "ml_on"}:
        return None
    metrics = (
        "pnl",
        "terminal_mtm_pnl",
        "inventory_adjusted_pnl",
        "fills_total",
        "abs_inventory_time_s",
        "avg_markout",
    )
    output: dict[str, Any] = {
        "comparison": "ml_on_minus_ml_off",
        "cluster_unit": "UTC_day",
        "bootstrap_draws": int(bootstrap_draws),
        "bootstrap_seed": int(seed),
        "metrics": {},
    }
    rng = np.random.default_rng(seed)
    for metric in metrics:
        wide = frame.pivot(index="day", columns="arm", values=metric).dropna()
        delta = (
            wide["ml_on"].to_numpy(dtype=float)
            - wide["ml_off"].to_numpy(dtype=float)
        )
        if not len(delta):
            continue
        draws = rng.choice(
            delta,
            size=(bootstrap_draws, len(delta)),
            replace=True,
        ).mean(axis=1)
        output["metrics"][metric] = {
            "days": int(len(delta)),
            "sum_delta": float(delta.sum()),
            "mean_daily_delta": float(delta.mean()),
            "median_daily_delta": float(np.median(delta)),
            "positive_day_rate": float(np.mean(delta > 0.0)),
            "mean_daily_delta_ci95_day_bootstrap": [
                float(np.quantile(draws, 0.025)),
                float(np.quantile(draws, 0.975)),
            ],
        }
    totals = frame.groupby("arm", sort=True)[
        ["fills_total", "abs_inventory_time_s"]
    ].sum()
    output["fill_retention"] = float(
        totals.loc["ml_on", "fills_total"]
        / max(totals.loc["ml_off", "fills_total"], 1.0)
    )
    output["inventory_time_ratio"] = float(
        totals.loc["ml_on", "abs_inventory_time_s"]
        / max(totals.loc["ml_off", "abs_inventory_time_s"], 1e-12)
    )
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days-file", type=Path, required=True)
    parser.add_argument("--book-root", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=window_cache_root(ROOT),
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--arms",
        nargs="+",
        choices=("ml_off", "ml_on"),
        default=("ml_off",),
    )
    parser.add_argument("--feature-dir", type=Path, default=None)
    parser.add_argument("--model-dir", type=Path, default=None)
    parser.add_argument("--workers", type=int, default=1)
    args = parser.parse_args()

    if args.workers < 1 or args.workers > 2:
        raise SystemExit("--workers must be in [1, 2]")
    days_file = args.days_file.expanduser().resolve()
    book_root = args.book_root.expanduser().resolve()
    config_path = args.config.expanduser().resolve()
    cache_dir = args.cache_dir.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    feature_dir = (
        args.feature_dir.expanduser().resolve()
        if args.feature_dir is not None
        else None
    )
    model_dir = (
        args.model_dir.expanduser().resolve()
        if args.model_dir is not None
        else None
    )
    if "ml_on" in args.arms and (feature_dir is None or model_dir is None):
        raise SystemExit("ml_on requires --feature-dir and --model-dir")
    if not config_path.is_file():
        raise SystemExit(f"config missing: {config_path}")

    days = _days(days_file)
    authorities = _quality_authorities(book_root)
    missing = [day for day in days if day not in authorities]
    if missing:
        raise SystemExit(
            "days missing from source contract: " + ", ".join(missing[:10])
        )
    selected_authorities = sorted({authorities[day] for day in days})
    if len(selected_authorities) != 1:
        raise SystemExit(
            "one run may contain only one source authority; split provider and "
            f"native evidence, observed {selected_authorities}"
        )

    payloads = [
        {
            "day": day,
            "source_authority": authorities[day],
            "book_root": str(book_root),
            "cache_dir": str(cache_dir),
            "config_path": str(config_path),
            "arms": list(dict.fromkeys(args.arms)),
            "feature_dir": str(feature_dir) if feature_dir else "",
            "model_dir": str(model_dir) if model_dir else "",
        }
        for day in days
    ]
    if args.workers == 1:
        nested_rows = [_task(payload) for payload in payloads]
    else:
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=args.workers
        ) as executor:
            nested_rows = list(executor.map(_task, payloads))
    rows = [row for daily in nested_rows for row in daily]
    frame = pd.DataFrame(rows).sort_values(["day", "arm"])
    if set(frame["source_authority"]) != set(selected_authorities):
        raise RuntimeError("runtime source authority differs from frozen day file")

    output_dir.mkdir(parents=True, exist_ok=True)
    daily_path = output_dir / "daily.csv"
    frame.to_csv(daily_path, index=False)
    summary_rows = _summary(frame)
    summary_path = output_dir / "summary.csv"
    pd.DataFrame(summary_rows).to_csv(summary_path, index=False)
    paired_summary = _paired_summary(frame)
    paired_path = output_dir / "paired_summary.json"
    if paired_summary is not None:
        _atomic_json(paired_path, paired_summary)
    manifest = {
        "schema_version": "narrowgate.source_aware_cpp_baseline.v1",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "engine": "cpp",
        "source_authority": selected_authorities[0],
        "days": len(days),
        "arms": list(dict.fromkeys(args.arms)),
        "config": {
            "path": str(config_path),
            "sha256": _sha256(config_path),
        },
        "days_file": {
            "path": str(days_file),
            "sha256": _sha256(days_file),
        },
        "book_manifest": {
            "path": str(book_root / "manifest.json"),
            "sha256": _sha256(book_root / "manifest.json"),
        },
        "feature_manifest": (
            {
                "path": str(feature_dir / "causal_feature_manifest.json"),
                "sha256": _sha256(
                    feature_dir / "causal_feature_manifest.json"
                ),
            }
            if feature_dir is not None
            else None
        ),
        "training_summary": (
            {
                "path": str(model_dir / "training_summary.json"),
                "sha256": _sha256(model_dir / "training_summary.json"),
            }
            if model_dir is not None
            else None
        ),
        "daily": {
            "path": str(daily_path),
            "sha256": _sha256(daily_path),
        },
        "summary": {
            "path": str(summary_path),
            "sha256": _sha256(summary_path),
            "rows": summary_rows,
        },
        "paired_summary": (
            {
                "path": str(paired_path),
                "sha256": _sha256(paired_path),
                "result": paired_summary,
            }
            if paired_summary is not None
            else None
        ),
        "permission_boundary": {
            "provider_days_are_sensitivity_only": (
                selected_authorities[0] == "provider_normalized_causal"
            ),
            "cpp_core_excludes_python_q90": True,
            "cpp_core_excludes_buy_fill_selection": True,
            "full_live_stack_authority": False,
            "action_authority": False,
            "live_authority": False,
        },
    }
    _atomic_json(output_dir / "manifest.json", manifest)
    print(json.dumps(manifest["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
