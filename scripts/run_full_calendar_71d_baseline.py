#!/usr/bin/env python3
"""Run the current operational baseline on every day in a 71-day calendar.

This is a daily-fresh-start economic diagnostic.  Every UTC day is traded;
there are no whole-day maintenance placeholders.  Native execution books are
used when available and provider-normalized books are an explicitly reported
sensitivity fallback.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_paths import data_root  # noqa: E402

DATA_ROOT = data_root(ROOT)
DAYS_FILE = Path(
    os.environ.get(
        "NARROWGATE_71D_DAYS_FILE",
        Path(tempfile.gettempdir()) / "narrowgate_71d_days.csv",
    )
).expanduser()
NATIVE_BOOK_ROOT = DATA_ROOT / "normalized_l2_100ms_v2_20260727"
PROVIDER_BOOK_ROOT = DATA_ROOT / "normalized_tardis_l2_100ms_v1"
FROZEN_40_FEATURE_DIR = (
    DATA_ROOT / "features_btcusdc_causal_v12_ranked_toxicity_f09_40d_20260802"
)
EXPANDED_FEATURE_DIR = (
    DATA_ROOT / "features_btcusdc_causal_v12_expanded_source_aware_semantics_v6_20260802"
)
SUPPLEMENT_FEATURE_DIR = (
    DATA_ROOT / "features_btcusdc_causal_v12_full_calendar_71d_missing27_20260803"
)
CURRENT_40_BASELINE = (
    ROOT
    / "research/families/f10_live_replay_attribution/docs/"
    "current_live_held_ber_replay_baseline_40d_20260809.json"
)
DEFAULT_OUTPUT = (
    DATA_ROOT
    / "reports/current_live_held_ber_baseline_full_calendar_71d_v1_20260809"
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
        temporary = Path(handle.name)
    os.replace(temporary, path)


def load_days(days_file: Path = DAYS_FILE) -> list[str]:
    frame = pd.read_csv(days_file, dtype={"day": str})
    days = frame["day"].tolist()
    expected = pd.date_range("2026-04-17", "2026-06-26", freq="D").strftime(
        "%Y-%m-%d"
    ).tolist()
    if days != expected:
        raise RuntimeError("71-day calendar must be the complete ordered UTC range")
    return days


def anchor_days() -> set[str]:
    payload = json.loads(CURRENT_40_BASELINE.read_text(encoding="utf-8"))
    days = (payload.get("panel") or {}).get("ordered_utc_days", [])
    result = {str(day) for day in days}
    if len(result) != 40:
        raise RuntimeError(f"expected 40 frozen anchor days, found {len(result)}")
    return result


def resolve_feature(day: str) -> tuple[Path, str]:
    candidates = (
        (FROZEN_40_FEATURE_DIR, "f09_frozen_40d"),
        (EXPANDED_FEATURE_DIR, "v12_expanded_existing"),
        (SUPPLEMENT_FEATURE_DIR, "provider_supplement_27d"),
    )
    matches = [
        (root, identity)
        for root, identity in candidates
        if (root / f"features_{day}.parquet").is_file()
    ]
    if not matches:
        raise FileNotFoundError(f"missing semantics-v6 feature file for {day}")
    return matches[0]


def resolve_book(day: str) -> tuple[Path, str]:
    native_bbo = NATIVE_BOOK_ROOT / "bbo" / f"BTCUSDC-bbo-{day}.parquet"
    native_l2 = NATIVE_BOOK_ROOT / "l2" / f"BTCUSDC-l2-{day}.parquet"
    if native_bbo.is_file() and native_l2.is_file():
        return NATIVE_BOOK_ROOT, "native_available"
    provider_bbo = PROVIDER_BOOK_ROOT / "bbo" / f"BTCUSDC-bbo-{day}.parquet"
    provider_l2 = PROVIDER_BOOK_ROOT / "l2" / f"BTCUSDC-l2-{day}.parquet"
    if provider_bbo.is_file() and provider_l2.is_file():
        return PROVIDER_BOOK_ROOT, "provider_normalized_sensitivity"
    raise FileNotFoundError(f"no complete execution book pair for {day}")


def preflight(days: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for day in days:
        feature_dir, feature_identity = resolve_feature(day)
        book_root, book_identity = resolve_book(day)
        feature_path = feature_dir / f"features_{day}.parquet"
        bbo_path = book_root / "bbo" / f"BTCUSDC-bbo-{day}.parquet"
        l2_path = book_root / "l2" / f"BTCUSDC-l2-{day}.parquet"
        rows.append(
            {
                "day": day,
                "feature_dir": str(feature_dir),
                "feature_identity": feature_identity,
                "feature_path": str(feature_path),
                "book_root": str(book_root),
                "book_identity": book_identity,
                "bbo_path": str(bbo_path),
                "l2_path": str(l2_path),
            }
        )
    return rows


def _day_task(payload: dict[str, Any]) -> dict[str, Any]:
    from models import backtest_tick as bt
    from models.data_windows import load_tick_window
    from scripts import run_restart_aware_continuous_baseline as shared

    day = str(payload["day"])
    book_root = Path(str(payload["book_root"])).resolve()
    feature_dir = Path(str(payload["feature_dir"])).resolve()
    shared.BOOK_ROOT = book_root
    identities = shared.validate_identities()
    params = shared.build_params(day, identities["config_path"])
    params.update(
        {
            "trace_fills_max": 0,
            "planned_quote_stop_ts_ms": 0,
            "replay_purpose": "full_calendar_71d_daily_fresh_baseline_diagnostic",
            "replay_promotion_eligible": False,
            "window_cache_write_enabled": False,
            "legacy_monolithic_window_cache_write_enabled": False,
        }
    )
    bt.BBO_DIR = book_root / "bbo"
    bt.L2_DIR = book_root / "l2"
    started = time.perf_counter()
    window = load_tick_window(
        day,
        params,
        load_ml=True,
        require_ml=True,
        run_ml_inference=True,
        feature_dir=feature_dir,
        require_target_feature_files=True,
        cross_market_enabled=True,
        with_ml_cache=False,
        require_historical_bbo=True,
        require_formal_l2=False,
        cache_dir=shared.CACHE_DIR,
        refresh_cache=False,
    )
    result = bt._simulate_tick_with_engine(
        "cpp",
        window.trades,
        window.var_ts_ms,
        window.var_ssq,
        params,
        ml_data=window.ml_data,
        bbo_data=window.bbo_data,
        l2_data=window.l2_data,
        var_ti=window.var_ti,
        var_retsq=window.var_retsq,
    )
    return {
        "day": day,
        "anchor_40": bool(payload["anchor_40"]),
        "feature_identity": str(payload["feature_identity"]),
        "feature_dir": str(feature_dir),
        "book_identity": str(payload["book_identity"]),
        "book_root": str(book_root),
        "loader_book_source_authority": str(window.book_source_authority),
        "execution_trade_rows": int(len(window.trades)),
        "terminal_mtm_pnl_usdc": float(result["terminal_mtm_pnl"]),
        "pnl_usdc": float(result["pnl"]),
        "fills_bid": int(result["fills_bid"]),
        "fills_ask": int(result["fills_ask"]),
        "fills_total": int(result["fills_total"]),
        "integer_tick_crossing_recovered_bid_candidates": int(
            result.get("integer_tick_crossing_recovered_bid_candidates", 0)
        ),
        "integer_tick_crossing_recovered_ask_candidates": int(
            result.get("integer_tick_crossing_recovered_ask_candidates", 0)
        ),
        "integer_tick_crossing_recovered_bid_fills": int(
            result.get("integer_tick_crossing_recovered_bid_fills", 0)
        ),
        "integer_tick_crossing_recovered_ask_fills": int(
            result.get("integer_tick_crossing_recovered_ask_fills", 0)
        ),
        "final_inventory_btc": float(result["final_inventory"]),
        "abs_inventory_time_btc_s": float(result["abs_inventory_time_s"]),
        "max_inventory_btc": float(result["max_inventory"]),
        "terminal_mark_price": float(result["terminal_mark_price"]),
        "runtime_s": time.perf_counter() - started,
    }


def bootstrap_mean_ci(values: np.ndarray, seed: int = 20260803) -> list[float]:
    rng = np.random.default_rng(seed)
    draws = rng.choice(values, size=(10_000, len(values)), replace=True).mean(axis=1)
    return [float(np.quantile(draws, 0.025)), float(np.quantile(draws, 0.975))]


def summarize(frame: pd.DataFrame) -> dict[str, Any]:
    values = frame["terminal_mtm_pnl_usdc"].to_numpy(dtype=np.float64)
    return {
        "trading_days": int(len(frame)),
        "total_pnl_usdc": float(values.sum()),
        "mean_pnl_per_trading_day_usdc": float(values.mean()),
        "median_pnl_per_trading_day_usdc": float(np.median(values)),
        "positive_days": int(np.sum(values > 0.0)),
        "positive_day_rate": float(np.mean(values > 0.0)),
        "mean_daily_pnl_ci95_bootstrap_usdc": bootstrap_mean_ci(values),
        "fills_total": int(frame["fills_total"].sum()),
        "fills_bid": int(frame["fills_bid"].sum()),
        "fills_ask": int(frame["fills_ask"].sum()),
        "integer_tick_crossing_recovered_bid_candidates": int(
            frame["integer_tick_crossing_recovered_bid_candidates"].sum()
        ),
        "integer_tick_crossing_recovered_ask_candidates": int(
            frame["integer_tick_crossing_recovered_ask_candidates"].sum()
        ),
        "integer_tick_crossing_recovered_bid_fills": int(
            frame["integer_tick_crossing_recovered_bid_fills"].sum()
        ),
        "integer_tick_crossing_recovered_ask_fills": int(
            frame["integer_tick_crossing_recovered_ask_fills"].sum()
        ),
        "abs_inventory_time_btc_s": float(frame["abs_inventory_time_btc_s"].sum()),
        "days_with_execution_trades": int((frame["execution_trade_rows"] > 0).sum()),
        "days_with_fills": int((frame["fills_total"] > 0).sum()),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--days-file", type=Path, default=DAYS_FILE)
    parser.add_argument("--workers", type=int, choices=(1, 2), default=2)
    parser.add_argument("--max-days", type=int, default=0)
    args = parser.parse_args()
    if args.max_days < 0:
        raise ValueError("max-days cannot be negative")
    os.chdir(ROOT)
    started = time.perf_counter()
    days_file = args.days_file.expanduser().resolve()
    days = load_days(days_file)
    anchors = anchor_days()
    source_rows = preflight(days)
    for row in source_rows:
        row["anchor_40"] = row["day"] in anchors
    selected = source_rows[: args.max_days] if args.max_days else source_rows
    if args.workers == 1:
        results = []
        for payload in selected:
            row = _day_task(payload)
            results.append(row)
            print(
                f"DONE {row['day']} pnl={row['terminal_mtm_pnl_usdc']:+.6f} "
                f"fills={row['fills_total']} book={row['book_identity']} "
                f"runtime={row['runtime_s']:.2f}s",
                flush=True,
            )
    else:
        results = []
        with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as pool:
            for row in pool.map(_day_task, selected):
                results.append(row)
                print(
                    f"DONE {row['day']} pnl={row['terminal_mtm_pnl_usdc']:+.6f} "
                    f"fills={row['fills_total']} book={row['book_identity']} "
                    f"runtime={row['runtime_s']:.2f}s",
                    flush=True,
                )

    daily = pd.DataFrame(results).sort_values("day").reset_index(drop=True)
    output = args.output_root.expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    daily_path = output / "daily.parquet"
    daily.to_parquet(daily_path, index=False)
    complete = len(daily) == 71
    current_40 = json.loads(CURRENT_40_BASELINE.read_text(encoding="utf-8"))
    current_40_pnl = float(current_40["economics"]["terminal_mtm_pnl_usdc"])
    bound_backtest_sha256 = str(
        current_40["implementation"]["backtest_tick_sha256"]
    )
    current_backtest_path = ROOT / "models/backtest_tick.py"
    current_backtest_sha256 = sha256_file(current_backtest_path)
    implementation_identity_matches_bound_control = (
        current_backtest_sha256 == bound_backtest_sha256
    )
    report: dict[str, Any] = {
        "schema_version": "current_live_held_ber_baseline_full_calendar_71d.v1",
        "created_at_utc": datetime.now(UTC).isoformat(),
        "complete_run": complete,
        "estimand": {
            "all_calendar_days_trade": True,
            "whole_day_maintenance_placeholders": 0,
            "utc_day_runtime_state": "daily_fresh_start",
            "pnl_denominator": "71_active_trading_days" if complete else "smoke_subset",
            "continuous_live_parity": False,
        },
        "baseline": {
            "ml": "causal_v12_semantics_v6_on",
            "q90_shadow": True,
            "q90_action": False,
            "buy_fill_selection_shadow": False,
            "buy_fill_selection_action": False,
            "ber_clock_semantics": "held_completed_10s_feature_sampled_on_1s_callback",
        },
        "execution_price_contract": {
            "identity": "executable_price_tick_contract.v1",
            "theoretical_quote_numeric_type": "double",
            "executable_order_price_identity": "int64_price_tick",
            "book_level_identity": "int64_price_tick",
            "trade_crossing": "integer_tick_inequality",
            "exact_queue_level": "integer_tick_equality",
            "queue_price_tolerance_defines_level": False,
        },
        "result": summarize(daily),
        "frozen_40_anchor_subset": summarize(daily[daily["anchor_40"]])
        if daily["anchor_40"].shape[0]
        else {},
        "additional_calendar_days": summarize(daily[~daily["anchor_40"]])
        if daily[~daily["anchor_40"]].shape[0]
        else {},
        "execution_book_strata": {
            key: summarize(group)
            for key, group in daily.groupby("book_identity", sort=True)
        },
        "feature_strata": {
            key: summarize(group)
            for key, group in daily.groupby("feature_identity", sort=True)
        },
        "current_40_day_comparison": {
            "current_live_held_baseline_pnl_usdc": current_40_pnl,
            "rerun_anchor_40_pnl_usdc": float(
                daily.loc[daily["anchor_40"], "terminal_mtm_pnl_usdc"].sum()
            ),
            "difference_usdc": float(
                daily.loc[daily["anchor_40"], "terminal_mtm_pnl_usdc"].sum()
                - current_40_pnl
            ),
            "implementation_identity_matches_bound_control": (
                implementation_identity_matches_bound_control
            ),
            "current_40_backtest_tick_sha256": bound_backtest_sha256,
            "current_backtest_tick_sha256": current_backtest_sha256,
            "exact_reproduction_expected": bool(
                complete and implementation_identity_matches_bound_control
            ),
            "interpretation": (
                "The same 40 dates and operational arm are compared, but an exact "
                "reproduction is not expected when the replay implementation hash differs."
            ),
        },
        "source_identity": {
            "days_file": {"path": str(days_file), "sha256": sha256_file(days_file)},
            "current_40_baseline": {
                "path": str(CURRENT_40_BASELINE),
                "sha256": sha256_file(CURRENT_40_BASELINE),
            },
            "feature_manifests": [
                {
                    "path": str(root / "causal_feature_manifest.json"),
                    "sha256": sha256_file(root / "causal_feature_manifest.json"),
                }
                for root in (
                    FROZEN_40_FEATURE_DIR,
                    EXPANDED_FEATURE_DIR,
                    SUPPLEMENT_FEATURE_DIR,
                )
            ],
            "native_book_manifest": {
                "path": str(NATIVE_BOOK_ROOT / "manifest.json"),
                "sha256": sha256_file(NATIVE_BOOK_ROOT / "manifest.json"),
            },
            "backtest_tick": {
                "path": str(current_backtest_path),
                "sha256": current_backtest_sha256,
            },
            "tick_replay_cpp": {
                "path": str(ROOT / "cpp/narrowgate_cpp/tick_replay.cpp"),
                "sha256": sha256_file(ROOT / "cpp/narrowgate_cpp/tick_replay.cpp"),
            },
            "tick_replay_hpp": {
                "path": str(ROOT / "cpp/narrowgate_cpp/tick_replay.hpp"),
                "sha256": sha256_file(ROOT / "cpp/narrowgate_cpp/tick_replay.hpp"),
            },
            "common_hpp": {
                "path": str(ROOT / "cpp/narrowgate_cpp/common.hpp"),
                "sha256": sha256_file(ROOT / "cpp/narrowgate_cpp/common.hpp"),
            },
            "bindings_cpp": {
                "path": str(ROOT / "cpp/narrowgate_cpp/bindings.cpp"),
                "sha256": sha256_file(ROOT / "cpp/narrowgate_cpp/bindings.cpp"),
            },
            "runner": {
                "path": str(Path(__file__).resolve()),
                "sha256": sha256_file(Path(__file__).resolve()),
            },
        },
        "permissions": {
            "historical_diagnostic_only": True,
            "provider_days_exact_queue_authority": False,
            "independent_confirmation": False,
            "action_authority": False,
            "live_authority": False,
        },
        "runtime_s": time.perf_counter() - started,
    }
    report_path = output / "report.json"
    atomic_json(report_path, report)
    manifest = {
        "schema_version": "current_live_held_ber_baseline_full_calendar_71d.manifest.v1",
        "files": {
            "daily": {"path": str(daily_path), "sha256": sha256_file(daily_path)},
            "report": {"path": str(report_path), "sha256": sha256_file(report_path)},
        },
    }
    atomic_json(output / "manifest.json", manifest)
    print(json.dumps(report["result"], indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
