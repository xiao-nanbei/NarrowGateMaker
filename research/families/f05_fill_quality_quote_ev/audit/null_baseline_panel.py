#!/usr/bin/env python3
"""Run null-baseline evidence over retained daily panels.

This is an orchestration wrapper around the canonical replay trace generator
and unified audit runner.  It keeps daily boundaries explicit, writes compact
panel CSVs, and removes large per-day trace/order-level intermediates by
default.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
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
from research.families.f10_live_replay_attribution.audit.metrics import (
    null_baseline_condition_summary_rows,  # noqa: E402
)

RESULTS_DIR = data_root(ROOT) / "backtest_results_btcusdc"


def _manifest_days(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(path)
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    lines = text.splitlines()
    if "," not in lines[0] and lines[0][:4].isdigit():
        return sorted({line.strip()[:10] for line in lines if line.strip()})
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        days: set[str] = set()
        for row in reader:
            value = (
                row.get("day")
                or row.get("date")
                or row.get("utc_day")
                or row.get("good_day")
                or next((v for v in row.values() if str(v).strip()[:4].isdigit()), "")
            )
            text_value = str(value or "").strip()
            if text_value:
                days.add(text_value[:10])
    return sorted(days)


def _normalize_day_args(raw_days: list[str] | None) -> list[str] | None:
    """Accept repeated args or a shell-expanded whitespace/comma date string."""
    if raw_days is None:
        return None
    days: list[str] = []
    for item in raw_days:
        for token in re.split(r"[\s,]+", str(item).strip()):
            if token:
                days.append(token[:10])
    return sorted(set(days))


def _order_level_filelist(
    path: Path,
    *,
    verify_hashes: bool,
) -> dict[str, Path]:
    rows = read_csv_table(path)
    out: dict[str, Path] = {}
    for row in rows:
        day = str(row.get("day", "")).strip()[:10]
        value = row.get("order_level_csv") or row.get("path") or row.get("file")
        if not day or not value:
            continue
        order_path = Path(str(value)).expanduser().resolve()
        if not order_path.is_file():
            raise FileNotFoundError(order_path)
        expected_size = str(row.get("size_bytes", "")).strip()
        if expected_size and order_path.stat().st_size != int(expected_size):
            raise RuntimeError(f"order-level size mismatch for {day}: {order_path}")
        expected_sha = str(row.get("sha256", "")).strip().lower()
        if verify_hashes and expected_sha:
            digest = hashlib.sha256()
            with order_path.open("rb") as f:
                for chunk in iter(lambda: f.read(1024 * 1024), b""):
                    digest.update(chunk)
            if digest.hexdigest() != expected_sha:
                raise RuntimeError(f"order-level SHA256 mismatch for {day}: {order_path}")
        if day in out:
            raise RuntimeError(f"duplicate order-level day in filelist: {day}")
        out[day] = order_path
    if not out:
        raise RuntimeError(f"order-level filelist contains no usable rows: {path}")
    return out


def _run(cmd: list[str], *, cwd: Path, dry_run: bool = False) -> None:
    print("+ " + " ".join(cmd), flush=True)
    if dry_run:
        return
    subprocess.run(cmd, cwd=cwd, check=True)


def _append_csv(target: list[dict[str, Any]], path: Path) -> None:
    if path.exists():
        target.extend(read_csv_table(path))


def _cleanup_paths(paths: list[Path]) -> None:
    for path in paths:
        try:
            if path.exists():
                path.unlink()
        except OSError as exc:
            print(f"[WARN] failed to delete {path}: {exc}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default="BTCUSDC")
    parser.add_argument("--manifest", type=Path)
    parser.add_argument(
        "--order-level-filelist",
        type=Path,
        help=(
            "Reuse a frozen per-day order-level denominator instead of "
            "regenerating quote/fill traces."
        ),
    )
    parser.add_argument("--verify-order-level-hashes", action="store_true")
    parser.add_argument("--days", nargs="*", default=None, help="Optional explicit UTC days; overrides manifest.")
    parser.add_argument("--max-days", type=int, default=0)
    parser.add_argument("--tag", required=True)
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Optional live config YAML to pass into quote_decomposition_tick for current-baseline evidence.",
    )
    parser.add_argument("--out-prefix", type=Path, default=None)
    parser.add_argument("--window-cache-dir", type=Path, default=None)
    parser.add_argument("--trace-quotes-max", type=int, default=160_000)
    parser.add_argument("--trace-fills-max", type=int, default=40_000)
    parser.add_argument("--random-trials", type=int, default=64)
    parser.add_argument("--random-seed", type=int, default=20260725)
    parser.add_argument("--workers-note", default="", help="Reserved note; this runner is intentionally sequential per day.")
    parser.add_argument("--refresh", action="store_true", help="Rerun daily trace/audit even when compact daily outputs exist.")
    parser.add_argument("--keep-trace", action="store_true", help="Keep large *.orders/*.fills and generated order_level CSVs.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if bool(args.manifest) == bool(args.order_level_filelist):
        raise SystemExit("provide exactly one of --manifest or --order-level-filelist")
    order_level_by_day = (
        _order_level_filelist(
            args.order_level_filelist,
            verify_hashes=args.verify_order_level_hashes,
        )
        if args.order_level_filelist
        else {}
    )
    source_days = (
        sorted(order_level_by_day)
        if order_level_by_day
        else _manifest_days(args.manifest)
    )
    requested_days = _normalize_day_args(args.days)
    days = sorted(requested_days or source_days)
    missing_days = sorted(set(days) - set(source_days))
    if missing_days:
        raise SystemExit(f"requested days absent from source identity: {missing_days}")
    if args.max_days > 0:
        days = days[:args.max_days]
    if not days:
        raise SystemExit("No days to run")

    out_prefix = args.out_prefix or (RESULTS_DIR / f"null_baseline_panel_{args.tag}_{args.symbol.lower()}")
    out_prefix.parent.mkdir(parents=True, exist_ok=True)

    all_current: list[dict[str, Any]] = []
    all_random: list[dict[str, Any]] = []
    all_oracle: list[dict[str, Any]] = []
    all_positive: list[dict[str, Any]] = []
    all_condition_daily: list[dict[str, Any]] = []
    all_aggregate: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    truncation_rows: list[dict[str, Any]] = []

    for idx, day in enumerate(days, start=1):
        print(f"[{idx}/{len(days)}] {day}", flush=True)
        day_trace_prefix = RESULTS_DIR / f"tick_quote_decomposition_{args.tag}_{day}_{args.symbol.lower()}"
        day_audit_prefix = RESULTS_DIR / f"{out_prefix.name}_{day}_{args.symbol.lower()}"
        current_path = day_audit_prefix.with_suffix(".null_baseline_current_daily.csv")
        if current_path.exists() and not args.refresh:
            print(f"  reuse {day_audit_prefix.name}", flush=True)
        elif order_level_by_day:
            audit_cmd = [
                "python3",
                "-m",
                "research.families.f10_live_replay_attribution.audit.runner",
                "--symbol",
                args.symbol,
                "--order-level-csv",
                str(order_level_by_day[day]),
                "--reports",
                "null_baseline",
                "--null-random-trials",
                str(args.random_trials),
                "--null-random-seed",
                str(args.random_seed),
                "--out-prefix",
                str(day_audit_prefix),
            ]
            try:
                _run(audit_cmd, cwd=ROOT, dry_run=args.dry_run)
            except subprocess.CalledProcessError as exc:
                failures.append({"day": day, "error": str(exc)})
                print(f"[FAIL] {day}: {exc}", flush=True)
                continue
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
            ]
            if args.config:
                quote_cmd.extend(["--config", str(args.config)])
            if args.window_cache_dir:
                quote_cmd.extend(["--window-cache-dir", str(args.window_cache_dir)])
            try:
                _run(quote_cmd, cwd=ROOT, dry_run=args.dry_run)
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
                    "order_level,null_baseline",
                    "--null-random-trials",
                    str(args.random_trials),
                    "--out-prefix",
                    str(day_audit_prefix),
                ]
                _run(audit_cmd, cwd=ROOT, dry_run=args.dry_run)
            except subprocess.CalledProcessError as exc:
                failures.append({"day": day, "error": str(exc)})
                print(f"[FAIL] {day}: {exc}", flush=True)
                continue

        summary_path = day_trace_prefix.with_suffix(".summary.csv")
        if order_level_by_day:
            truncation_rows.append(
                {
                    "day": day,
                    "order_level_csv": str(order_level_by_day[day]),
                    "source": "frozen_order_level_filelist",
                }
            )
        elif summary_path.exists():
            rows = read_csv_table(summary_path)
            for row in rows:
                truncation_rows.append({
                    "day": day,
                    "trace_orders": row.get("trace_orders", ""),
                    "trace_fills": row.get("trace_fills", ""),
                    "trace_quotes_truncated": row.get("trace_quotes_truncated", ""),
                    "trace_fills_truncated": row.get("trace_fills_truncated", ""),
                    "pnl": row.get("pnl", ""),
                    "inventory_adjusted_pnl": row.get("inventory_adjusted_pnl", ""),
                })

        _append_csv(all_current, day_audit_prefix.with_suffix(".null_baseline_current_daily.csv"))
        _append_csv(all_random, day_audit_prefix.with_suffix(".null_baseline_random_daily.csv"))
        _append_csv(all_oracle, day_audit_prefix.with_suffix(".null_baseline_oracle_daily.csv"))
        _append_csv(all_positive, day_audit_prefix.with_suffix(".null_baseline_positive_intersection_daily.csv"))
        _append_csv(all_condition_daily, day_audit_prefix.with_suffix(".null_baseline_condition_daily.csv"))
        _append_csv(all_aggregate, day_audit_prefix.with_suffix(".null_baseline_aggregate.csv"))

        if not order_level_by_day and not args.keep_trace and not args.dry_run:
            _cleanup_paths([
                day_trace_prefix.with_suffix(".orders.csv"),
                day_trace_prefix.with_suffix(".fills.csv"),
                day_trace_prefix.with_suffix(".decisions.csv"),
                day_trace_prefix.with_suffix(".queue_events.csv"),
                day_audit_prefix.with_suffix(".order_level.csv"),
                day_audit_prefix.with_suffix(".order_level_scores.csv"),
            ])

    condition_summary = null_baseline_condition_summary_rows(
        all_condition_daily,
        min_support_days=5 if len(days) >= 20 else 2,
        min_total_fills=50 if len(days) >= 20 else 10,
    )
    write_csv(out_prefix.with_suffix(".current_daily.csv"), all_current)
    write_csv(out_prefix.with_suffix(".random_daily.csv"), all_random)
    write_csv(out_prefix.with_suffix(".oracle_daily.csv"), all_oracle)
    write_csv(out_prefix.with_suffix(".positive_intersection_daily.csv"), all_positive)
    write_csv(out_prefix.with_suffix(".condition_daily.csv"), all_condition_daily)
    write_csv(out_prefix.with_suffix(".condition_summary.csv"), condition_summary)
    write_csv(out_prefix.with_suffix(".aggregate_daily.csv"), all_aggregate)
    write_csv(out_prefix.with_suffix(".trace_sanity.csv"), truncation_rows)
    write_csv(out_prefix.with_suffix(".failures.csv"), failures)

    md_lines = [
        "# Null Baseline Daily Panel",
        "",
        f"- symbol: `{args.symbol}`",
        f"- tag: `{args.tag}`",
        f"- days requested: `{len(days)}`",
        f"- source: `{'frozen_order_level_filelist' if order_level_by_day else 'replay_trace'}`",
        f"- failures: `{len(failures)}`",
        f"- condition rows: `{len(all_condition_daily)}`",
        "",
        "## Top Condition Summary",
        "",
    ]
    if condition_summary:
        cols = list(condition_summary[0].keys())
        md_lines.append("| " + " | ".join(cols) + " |")
        md_lines.append("| " + " | ".join(["---"] * len(cols)) + " |")
        for row in condition_summary[:40]:
            md_lines.append("| " + " | ".join(str(row.get(c, "")) for c in cols) + " |")
    else:
        md_lines.append("_No condition rows._")
    md_lines.append("")
    md_lines.append("## Output Files")
    for suffix in (
        ".current_daily.csv",
        ".random_daily.csv",
        ".oracle_daily.csv",
        ".positive_intersection_daily.csv",
        ".condition_daily.csv",
        ".condition_summary.csv",
        ".trace_sanity.csv",
        ".failures.csv",
    ):
        md_lines.append(f"- `{out_prefix.with_suffix(suffix)}`")
    out_prefix.with_suffix(".md").write_text("\n".join(md_lines) + "\n", encoding="utf-8")
    print(f"panel={out_prefix.with_suffix('.md')}", flush=True)
    print(f"condition_summary={out_prefix.with_suffix('.condition_summary.csv')}", flush=True)
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
