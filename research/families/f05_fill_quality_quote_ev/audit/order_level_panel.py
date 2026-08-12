#!/usr/bin/env python3
"""Build per-day order-level denominator CSVs for retained daily panels.

This runner is intentionally narrower than ``null_baseline_panel``.  It only
generates the reusable ``order_level`` denominator table needed by downstream
score/calibration tools, then removes large intermediate replay trace files by
default.  Keeping per-day CSVs avoids creating one multi-GB merged table and
keeps reruns restartable.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_paths import data_root  # noqa: E402
from models.audit.support import read_csv_table, write_csv  # noqa: E402

RESULTS_DIR = data_root(ROOT) / "backtest_results_btcusdc"


def _manifest_days(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    if path.suffix.lower() == ".json":
        payload = json.loads(text)
        rows = payload.get("daily_files") or []
        return sorted(
            {
                str(row.get("day", "") or "")[:10]
                for row in rows
                if isinstance(row, dict) and str(row.get("day", "") or "") and _row_is_eligible(row)
            }
        )
    lines = text.splitlines()
    if "," not in lines[0] and lines[0][:4].isdigit():
        return sorted({line.strip()[:10] for line in lines if line.strip()})
    days: set[str] = set()
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not _row_is_eligible(row):
                continue
            value = (
                row.get("day")
                or row.get("date")
                or row.get("utc_day")
                or next((v for v in row.values() if str(v or "").strip()[:4].isdigit()), "")
            )
            text_value = str(value or "").strip()
            if text_value:
                days.add(text_value[:10])
    return sorted(days)


def _row_is_eligible(row: dict[str, Any]) -> bool:
    for key in ("replay_eligible", "formal_eligible", "eligible"):
        if key not in row:
            continue
        value = str(row.get(key, "") or "").strip().lower()
        return value in {"1", "true", "yes", "y"}
    return True


def _normalize_day_args(raw_days: list[str] | None) -> list[str] | None:
    if raw_days is None:
        return None
    days: list[str] = []
    for item in raw_days:
        for token in re.split(r"[\s,]+", str(item).strip()):
            if token:
                days.append(token[:10])
    return sorted(set(days))


def _run(cmd: list[str], *, cwd: Path, dry_run: bool = False) -> None:
    print("+ " + " ".join(cmd), flush=True)
    if dry_run:
        return
    subprocess.run(cmd, cwd=cwd, check=True)


def _cleanup(paths: list[Path]) -> None:
    for path in paths:
        try:
            if path.exists():
                path.unlink()
        except OSError as exc:
            print(f"[WARN] failed to delete {path}: {exc}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default="BTCUSDC")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument(
        "--days", nargs="*", default=None, help="Optional explicit UTC days; overrides manifest."
    )
    parser.add_argument("--max-days", type=int, default=0)
    parser.add_argument("--tag", required=True)
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Optional live config YAML to pass into quote_decomposition_tick for current-baseline order tables.",
    )
    parser.add_argument("--out-prefix", type=Path, default=None)
    parser.add_argument("--window-cache-dir", type=Path, default=None)
    parser.add_argument("--trace-quotes-max", type=int, default=180_000)
    parser.add_argument("--trace-fills-max", type=int, default=50_000)
    parser.add_argument("--engine", choices=("python", "cpp"), default="cpp")
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument(
        "--keep-trace", action="store_true", help="Keep intermediate *.orders/*.fills trace files."
    )
    parser.add_argument(
        "--strict-calibration",
        action="store_true",
        help="Fail fast unless every per-day replay has complete formal calibration identity.",
    )
    parser.add_argument(
        "--execution-trade-source",
        choices=("aggTrades", "trades"),
        default="trades",
        help="Execution event tape; formal order-level evidence defaults to individual trades.",
    )
    parser.add_argument("--individual-trades-manifest-path", type=Path)
    parser.add_argument("--individual-trades-integrity-report-path", type=Path)
    parser.add_argument("--individual-trades-manifest-sha256", default="")
    parser.add_argument(
        "--individual-trades-integrity-report-sha256",
        default="",
    )
    parser.add_argument(
        "--market-context-warmup-days",
        type=int,
        default=1,
        help="Causal BBO/L2/bar context loaded before each target UTC day.",
    )
    parser.add_argument(
        "--require-formal-l2",
        action="store_true",
        help="Reject target/warmup dates outside the normalized formal-L2 universe.",
    )
    parser.add_argument(
        "--verify-formal-l2-hashes",
        action="store_true",
        help="Rehash every normalized formal BBO/L2 input before replay.",
    )
    parser.add_argument(
        "--queue-calibration-path",
        type=Path,
        default=None,
        help="Explicit queue-v3 artifact used by strict replay.",
    )
    parser.add_argument(
        "--live-perf-telemetry",
        type=Path,
        default=None,
        help="Frozen live telemetry CSV/CSV.GZ for empirical REST latency replay.",
    )
    parser.add_argument(
        "--live-perf-latency-mode",
        choices=("avg", "max", "sum"),
        default="avg",
    )
    parser.add_argument("--rng-seed", type=int, default=42)
    parser.add_argument("--latency-seed", type=int, default=59)
    parser.add_argument("--latency-profile-id", default="")
    parser.add_argument("--latency-environment", default="")
    parser.add_argument(
        "--latency-scenario",
        choices=("baseline", "stress"),
        default="baseline",
    )
    parser.add_argument(
        "--disable-buy-fill-selection",
        action="store_true",
        help="Disable the existing BUY scorer while generating a clean retraining denominator.",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.strict_calibration and not args.require_formal_l2:
        raise SystemExit("--strict-calibration order-level panels also require --require-formal-l2")

    days = sorted(_normalize_day_args(args.days) or _manifest_days(args.manifest))
    if args.max_days > 0:
        days = days[: args.max_days]
    if not days:
        raise SystemExit("No retained days to run")

    out_prefix = args.out_prefix or (
        RESULTS_DIR / f"order_level_panel_{args.tag}_{args.symbol.lower()}"
    )
    out_prefix.parent.mkdir(parents=True, exist_ok=True)

    manifest_rows: list[dict[str, Any]] = []
    sanity_rows: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    for idx, day in enumerate(days, start=1):
        print(f"[{idx}/{len(days)}] {day}", flush=True)
        day_trace_prefix = (
            RESULTS_DIR / f"tick_quote_decomposition_{args.tag}_{day}_{args.symbol.lower()}"
        )
        day_audit_prefix = RESULTS_DIR / f"{out_prefix.name}_{day}_{args.symbol.lower()}"
        order_level_path = day_audit_prefix.with_suffix(".order_level.csv")

        if order_level_path.exists() and not args.refresh:
            print(f"  reuse {order_level_path.name}", flush=True)
        else:
            quote_cmd = [
                "python3",
                "models/quote_decomposition_tick.py",
                "--symbol",
                args.symbol,
                "--days",
                day,
                "--tag",
                f"{args.tag}_{day}",
                "--trace-quotes-max",
                str(args.trace_quotes_max),
                "--trace-fills-max",
                str(args.trace_fills_max),
                "--trace-decisions-max",
                "0",
                "--engine",
                args.engine,
                "--execution-trade-source",
                args.execution_trade_source,
                "--market-context-warmup-days",
                str(max(0, int(args.market_context_warmup_days))),
            ]
            if args.config:
                quote_cmd.extend(["--config", str(args.config)])
            if args.strict_calibration:
                quote_cmd.append("--strict-calibration")
                quote_cmd.append("--queue-regime-calibration")
                quote_cmd.extend(
                    [
                        "--replay-purpose",
                        "formal",
                        "--initial-state-mode",
                        "fresh_start",
                        "--rng-seed",
                        str(args.rng_seed),
                        "--latency-seed",
                        str(args.latency_seed),
                        "--latency-profile-id",
                        args.latency_profile_id,
                        "--latency-environment",
                        args.latency_environment,
                        "--latency-scenario",
                        args.latency_scenario,
                    ]
                )
            if args.require_formal_l2:
                quote_cmd.append("--require-formal-l2")
            if args.verify_formal_l2_hashes:
                quote_cmd.append("--verify-formal-l2-hashes")
            if args.queue_calibration_path:
                quote_cmd.extend(
                    [
                        "--queue-calibration-path",
                        str(args.queue_calibration_path),
                    ]
                )
            if args.individual_trades_manifest_path:
                quote_cmd.extend(
                    [
                        "--individual-trades-manifest-path",
                        str(args.individual_trades_manifest_path),
                    ]
                )
            if args.individual_trades_integrity_report_path:
                quote_cmd.extend(
                    [
                        "--individual-trades-integrity-report-path",
                        str(args.individual_trades_integrity_report_path),
                    ]
                )
            if args.individual_trades_manifest_sha256:
                quote_cmd.extend(
                    [
                        "--individual-trades-manifest-sha256",
                        args.individual_trades_manifest_sha256,
                    ]
                )
            if args.individual_trades_integrity_report_sha256:
                quote_cmd.extend(
                    [
                        "--individual-trades-integrity-report-sha256",
                        args.individual_trades_integrity_report_sha256,
                    ]
                )
            if args.live_perf_telemetry:
                quote_cmd.extend(
                    [
                        "--live-perf-telemetry",
                        str(args.live_perf_telemetry),
                        "--live-perf-latency-mode",
                        args.live_perf_latency_mode,
                    ]
                )
            if args.disable_buy_fill_selection:
                quote_cmd.append("--disable-buy-fill-selection")
            if args.window_cache_dir:
                quote_cmd.extend(["--window-cache-dir", str(args.window_cache_dir)])
            audit_cmd = [
                "python3",
                "-m",
                "research.families.f10_live_replay_attribution.audit.runner",
                "--symbol",
                args.symbol,
                "--replay-orders-csv",
                str(day_trace_prefix.with_suffix(".orders.csv")),
                "--replay-fills-csv",
                str(day_trace_prefix.with_suffix(".fills.csv")),
                "--reports",
                "order_level",
                "--out-prefix",
                str(day_audit_prefix),
            ]
            try:
                _run(quote_cmd, cwd=ROOT, dry_run=args.dry_run)
                _run(audit_cmd, cwd=ROOT, dry_run=args.dry_run)
            except subprocess.CalledProcessError as exc:
                failures.append({"day": day, "error": str(exc)})
                print(f"[FAIL] {day}: {exc}", flush=True)
                continue

        summary_path = day_trace_prefix.with_suffix(".summary.csv")
        summary_json_path = day_trace_prefix.with_suffix(".summary.json")
        replay_contract_sha256 = ""
        replay_purpose = ""
        replay_promotion_eligible = False
        if summary_json_path.exists():
            payload = json.loads(summary_json_path.read_text(encoding="utf-8"))
            summary_payload = (payload.get("summary") or [{}])[0]
            replay_contract_sha256 = str(summary_payload.get("replay_contract_sha256", ""))
            replay_purpose = str(summary_payload.get("replay_purpose", ""))
            replay_promotion_eligible = bool(
                summary_payload.get("replay_promotion_eligible", False)
            )
            if args.strict_calibration and (
                replay_purpose != "formal"
                or not replay_contract_sha256
                or not replay_promotion_eligible
            ):
                failures.append(
                    {
                        "day": day,
                        "error": (
                            "strict order panel lacks a promotion-eligible formal replay contract"
                        ),
                    }
                )
                continue
        if summary_path.exists():
            for row in read_csv_table(summary_path):
                sanity_rows.append(
                    {
                        "day": day,
                        "trace_orders": row.get("trace_orders", ""),
                        "trace_fills": row.get("trace_fills", ""),
                        "trace_quotes_truncated": row.get("trace_quotes_truncated", ""),
                        "trace_fills_truncated": row.get("trace_fills_truncated", ""),
                        "pnl": row.get("pnl", ""),
                        "inventory_adjusted_pnl": row.get("inventory_adjusted_pnl", ""),
                    }
                )

        if order_level_path.exists():
            manifest_rows.append(
                {
                    "day": day,
                    "order_level_csv": str(order_level_path),
                    "quote_summary_json": str(summary_json_path),
                    "replay_purpose": replay_purpose,
                    "replay_promotion_eligible": int(replay_promotion_eligible),
                    "replay_contract_sha256": replay_contract_sha256,
                }
            )

        if not args.keep_trace and not args.dry_run:
            _cleanup(
                [
                    day_trace_prefix.with_suffix(".orders.csv"),
                    day_trace_prefix.with_suffix(".fills.csv"),
                    day_trace_prefix.with_suffix(".decisions.csv"),
                    day_trace_prefix.with_suffix(".queue_events.csv"),
                ]
            )

    write_csv(out_prefix.with_suffix(".filelist.csv"), manifest_rows)
    write_csv(out_prefix.with_suffix(".trace_sanity.csv"), sanity_rows)
    write_csv(out_prefix.with_suffix(".failures.csv"), failures)
    out_prefix.with_suffix(".txt").write_text(
        "\n".join(row["order_level_csv"] for row in manifest_rows)
        + ("\n" if manifest_rows else ""),
        encoding="utf-8",
    )
    print(f"filelist={out_prefix.with_suffix('.filelist.csv')}", flush=True)
    print(f"txt={out_prefix.with_suffix('.txt')}", flush=True)
    print(f"failures={len(failures)}", flush=True)
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
