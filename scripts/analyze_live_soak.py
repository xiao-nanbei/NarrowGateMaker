#!/usr/bin/env python3
"""Create and analyze comparable live Python/native soak windows."""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sys
import time
from collections import Counter
from collections.abc import Iterable
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DEFAULT_FILES = {
    "live_perf": ROOT / "logs/live_perf_telemetry.csv",
    "outcomes": ROOT / "logs/order_outcomes.csv",
    "quotes": ROOT / "logs/quote_decisions.csv",
    "trades": ROOT / "logs/trades.csv",
    "maker_log": ROOT / "logs/maker.log",
}

GLOBAL_FLOW_HEALTH_FIELDS = (
    "globalFlowNative",
    "globalFlowMarkets",
    "globalFlowTradeBatches",
    "globalFlowTradeEvents",
    "globalFlowTradeAccepted",
    "globalFlowBookEvents",
    "globalFlowOOO",
    "globalFlowStaleTrades",
    "globalFlowTradeOverflow",
    "globalFlowBookOverflow",
)
COMPARE_LATENCY_FIELDS = (
    "requote_total_us",
    "update_orders_us",
    "signal_compute_us",
    "compute_quotes_us",
)
_HEALTH_TOKEN = re.compile(r"\b([A-Za-z][A-Za-z0-9_]*)=([^\s]+)")


def _line_count(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("rb") as handle:
        return sum(1 for _ in handle)


def _rows_after(path: Path, start_line: int) -> list[dict[str, str]]:
    if not path.exists():
        return []
    rows = []
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for line_number, row in enumerate(reader, start=2):
            if line_number > start_line:
                rows.append(row)
    return rows


def _text_after(path: Path, start_line: int) -> list[str]:
    if not path.exists():
        return []
    with path.open(errors="replace") as handle:
        return [line.rstrip("\n") for index, line in enumerate(handle, start=1) if index > start_line]


def _number(value: object) -> float:
    try:
        result = float(value)
        return result if math.isfinite(result) else math.nan
    except (TypeError, ValueError):
        return math.nan


def _percentile(values: Iterable[float], q: float) -> float:
    clean = sorted(value for value in values if math.isfinite(value))
    if not clean:
        return math.nan
    if len(clean) == 1:
        return clean[0]
    position = (len(clean) - 1) * q
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return clean[lower]
    weight = position - lower
    return clean[lower] * (1.0 - weight) + clean[upper] * weight


def _distribution(rows: list[dict[str, str]], field: str) -> dict[str, float]:
    values = [_number(row.get(field)) for row in rows]
    clean = [value for value in values if math.isfinite(value)]
    return {
        "count": len(clean),
        "p50": _percentile(clean, 0.50),
        "p95": _percentile(clean, 0.95),
        "p99": _percentile(clean, 0.99),
        "p999": _percentile(clean, 0.999),
        "max": max(clean) if clean else math.nan,
    }


def _global_flow_health(lines: list[str]) -> dict[str, object]:
    rows = []
    for line in lines:
        if "HEALTH pos=" not in line or "globalFlowNative=" not in line:
            continue
        tokens = dict(_HEALTH_TOKEN.findall(line))
        row = {
            name: _number(tokens.get(name)) for name in GLOBAL_FLOW_HEALTH_FIELDS
        }
        if all(math.isfinite(value) for value in row.values()):
            rows.append(row)
    if not rows:
        return {"samples": 0, "native_rate": 0.0, "latest": {}, "delta": {}}

    first = rows[0]
    latest = rows[-1]
    counter_fields = GLOBAL_FLOW_HEALTH_FIELDS[2:]
    return {
        "samples": len(rows),
        "native_rate": sum(row["globalFlowNative"] for row in rows) / len(rows),
        "latest": {name: int(latest[name]) for name in GLOBAL_FLOW_HEALTH_FIELDS},
        "delta": {
            name: max(0, int(latest[name] - first[name])) for name in counter_fields
        },
    }


def create_marker(args: argparse.Namespace) -> dict:
    files = {name: Path(path) for name, path in DEFAULT_FILES.items()}
    marker = {
        "schema_version": 1,
        "profile": args.profile,
        "start_time": time.time(),
        "files": {
            name: {"path": str(path), "start_line": _line_count(path)}
            for name, path in files.items()
        },
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(marker, indent=2) + "\n")
    print(output)
    return marker


def _read_marker(path: Path) -> dict:
    marker = json.loads(path.read_text())
    if int(marker.get("schema_version", 0)) != 1:
        raise ValueError(f"unsupported marker schema: {marker.get('schema_version')}")
    return marker


def _action_mix(rows: list[dict[str, str]]) -> dict[str, dict[str, float]]:
    result = {}
    for side, field in (("BUY", "bid_action"), ("SELL", "ask_action")):
        counts = Counter(row.get(field, "unknown") or "unknown" for row in rows)
        total = sum(counts.values())
        result[side] = {
            action: {"count": count, "rate": count / total if total else 0.0}
            for action, count in sorted(counts.items())
        }
    return result


def analyze(marker_path: Path) -> dict:
    marker = _read_marker(marker_path)
    loaded = {}
    for name, metadata in marker["files"].items():
        path = Path(metadata["path"])
        start_line = int(metadata.get("start_line", 0))
        loaded[name] = (
            _text_after(path, start_line)
            if name == "maker_log"
            else _rows_after(path, start_line)
        )

    perf = loaded["live_perf"]
    outcomes = loaded["outcomes"]
    log_lines = loaded["maker_log"]
    end_times = [_number(row.get("timestamp")) for row in perf + outcomes]
    end_time = max((value for value in end_times if math.isfinite(value)), default=time.time())
    duration_s = max(1.0, end_time - float(marker["start_time"]))

    cpp_values = [_number(row.get("cpp_routing_used")) for row in perf]
    cpp_clean = [value for value in cpp_values if math.isfinite(value)]
    rest_new = [row for row in perf if _number(row.get("rest_new_count")) > 0]
    rest_cancel = [row for row in perf if _number(row.get("rest_cancel_count")) > 0]
    outcome_counts = Counter(row.get("event_type", "unknown") for row in outcomes)
    fill_rows = [row for row in outcomes if row.get("event_type") == "filled"]
    fill_sides = Counter(row.get("side", "unknown") for row in fill_rows)
    placed = sum(outcome_counts.get(name, 0) for name in ("placed", "placed_close"))
    fills = len(fill_rows)
    hours = duration_s / 3600.0

    severity_tokens = {
        "errors": (" ERROR ", " CRITICAL ", "Traceback"),
        "stream_silence": ("silence", "SILENCE"),
        "reconnect": ("reconnect", "RECONNECT"),
        "stale_safety": ("STALE", "stale-data", "stale data"),
        "ttl": ("TTL", "ttl"),
        "cancel_safety": ("CANCEL_ALL", "cancel_open_orders", "QUOTE_BLOCK_CANCEL"),
    }
    log_counts = {
        name: sum(any(token in line for token in tokens) for line in log_lines)
        for name, tokens in severity_tokens.items()
    }

    result = {
        "profile": marker.get("profile", "unknown"),
        "marker": str(marker_path),
        "start_time": marker["start_time"],
        "duration_s": duration_s,
        "rows": {name: len(rows) for name, rows in loaded.items()},
        "cpp_routing_used_rate": sum(cpp_clean) / len(cpp_clean) if cpp_clean else 0.0,
        "action_mix": _action_mix(perf),
        "latency_us": {
            field: _distribution(perf, field)
            for field in (
                "requote_total_us",
                "update_orders_us",
                "signal_compute_us",
                "compute_quotes_us",
            )
        },
        "rest_latency_us": {
            "new_requote_max": _distribution(rest_new, "rest_new_max_us"),
            "cancel_requote_max": _distribution(rest_cancel, "rest_cancel_max_us"),
        },
        "websocket_age_s": {
            field: _distribution(perf, field)
            for field in (
                "exec_trade_age_s",
                "exec_book_age_s",
                "exec_depth_age_s",
                "anchor_trade_max_age_s",
                "anchor_book_max_age_s",
                "spot_trade_max_age_s",
                "spot_book_max_age_s",
            )
        },
        "orders": {
            "placed": placed,
            "fills": fills,
            "placed_per_hour": placed / hours,
            "fills_per_hour": fills / hours,
            "fill_per_placed": fills / placed if placed else 0.0,
            "buy_fills": fill_sides.get("BUY", 0),
            "sell_fills": fill_sides.get("SELL", 0),
        },
        "log_sanity": log_counts,
        "global_flow": _global_flow_health(log_lines),
    }
    return result


def _fmt(value: float) -> str:
    return "n/a" if not math.isfinite(float(value)) else f"{float(value):.2f}"


def render_markdown(result: dict) -> str:
    lines = [
        f"# Live Soak: {result['profile']}",
        "",
        f"- duration: `{result['duration_s'] / 60.0:.1f} min`",
        f"- cpp routing hit: `{result['cpp_routing_used_rate']:.2%}`",
        f"- placed/hour: `{result['orders']['placed_per_hour']:.1f}`",
        f"- fills/hour: `{result['orders']['fills_per_hour']:.1f}`",
        f"- BUY/SELL fills: `{result['orders']['buy_fills']}/{result['orders']['sell_fills']}`",
        "",
        "| latency us | p50 | p95 | p99 | p99.9 | max |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for name, stats in result["latency_us"].items():
        lines.append(
            f"| {name} | {_fmt(stats['p50'])} | {_fmt(stats['p95'])} | "
            f"{_fmt(stats['p99'])} | {_fmt(stats['p999'])} | {_fmt(stats['max'])} |"
        )
    lines.extend(
        [
            "",
            "| REST latency us | p50 | p95 | p99 | p99.9 | max |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for name, stats in result["rest_latency_us"].items():
        lines.append(
            f"| {name} | {_fmt(stats['p50'])} | {_fmt(stats['p95'])} | "
            f"{_fmt(stats['p99'])} | {_fmt(stats['p999'])} | {_fmt(stats['max'])} |"
        )
    lines.extend(["", "## Action Mix", ""])
    for side, actions in result["action_mix"].items():
        summary = ", ".join(f"{name}={data['rate']:.2%}" for name, data in actions.items())
        lines.append(f"- {side}: {summary}")
    lines.extend(["", "## WebSocket Age", ""])
    lines.append("| stream age s | p50 | p99 | p99.9 | max |")
    lines.append("| --- | ---: | ---: | ---: | ---: |")
    for name, stats in result["websocket_age_s"].items():
        lines.append(
            f"| {name} | {_fmt(stats['p50'])} | {_fmt(stats['p99'])} | "
            f"{_fmt(stats['p999'])} | {_fmt(stats['max'])} |"
        )
    lines.append("")
    lines.append(f"- log sanity: `{result['log_sanity']}`")
    global_flow = result.get("global_flow", {})
    if global_flow.get("samples", 0):
        latest = global_flow["latest"]
        delta = global_flow["delta"]
        lines.extend(["", "## Native Global Flow", ""])
        lines.append(
            f"- HEALTH samples/native rate: `{global_flow['samples']}` / "
            f"`{global_flow['native_rate']:.2%}`"
        )
        lines.append(
            "- latest markets/trade batches/trade accepted/book events: "
            f"`{latest['globalFlowMarkets']}/"
            f"{latest['globalFlowTradeBatches']}/"
            f"{latest['globalFlowTradeAccepted']}/"
            f"{latest['globalFlowBookEvents']}`"
        )
        lines.append(
            "- window delta trade seen/accepted/book/OOO/stale: "
            f"`{delta['globalFlowTradeEvents']}/"
            f"{delta['globalFlowTradeAccepted']}/"
            f"{delta['globalFlowBookEvents']}/"
            f"{delta['globalFlowOOO']}/"
            f"{delta['globalFlowStaleTrades']}`"
        )
        lines.append(
            "- latest trade/book fixed-ring overflow: "
            f"`{latest['globalFlowTradeOverflow']}/"
            f"{latest['globalFlowBookOverflow']}`"
        )
    return "\n".join(lines) + "\n"


def _ratio(candidate: float, baseline: float) -> float:
    return candidate / baseline if baseline and math.isfinite(baseline) else math.nan


def _compare_fmt(value: float) -> str:
    return "n/a" if not math.isfinite(value) else f"{value:.3f}"


def compare_reports(baseline: dict, candidate: dict) -> str:
    """Compare two soak reports as system evidence, never alpha evidence."""
    lines = [
        f"# Soak Comparison: {baseline['profile']} -> {candidate['profile']}",
        "",
        "This is execution-engineering evidence, not strategy or PnL evidence.",
        "",
        "| metric | baseline | candidate | ratio |",
        "| --- | ---: | ---: | ---: |",
    ]
    for field in COMPARE_LATENCY_FIELDS:
        for quantile in ("p50", "p99", "p999"):
            before = float(baseline["latency_us"][field][quantile])
            after = float(candidate["latency_us"][field][quantile])
            lines.append(
                f"| {field}.{quantile} | {_compare_fmt(before)} | "
                f"{_compare_fmt(after)} | {_compare_fmt(_ratio(after, before))} |"
            )

    for field in ("placed_per_hour", "fills_per_hour", "fill_per_placed"):
        before = float(baseline["orders"][field])
        after = float(candidate["orders"][field])
        lines.append(
            f"| {field} | {_compare_fmt(before)} | {_compare_fmt(after)} | "
            f"{_compare_fmt(_ratio(after, before))} |"
        )

    update_ratio = _ratio(
        float(candidate["latency_us"]["update_orders_us"]["p99"]),
        float(baseline["latency_us"]["update_orders_us"]["p99"]),
    )
    decision = (
        "native path improved update-orders p99"
        if math.isfinite(update_ratio) and update_ratio < 1.0
        else "no update-orders p99 improvement"
    )
    lines.extend(
        [
            "",
            "## System Decision",
            "",
            f"- native routing hit: `{candidate.get('cpp_routing_used_rate', 0.0):.2%}`",
            f"- recommendation: **{decision}**.",
            "- Non-overlapping placed/fill windows are sanity checks only, not causal estimates.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    mark = subparsers.add_parser("mark")
    mark.add_argument("--profile", required=True)
    mark.add_argument("--output", required=True)
    report = subparsers.add_parser("report")
    report.add_argument("--marker", required=True)
    report.add_argument("--output-json")
    report.add_argument("--output-md")
    compare = subparsers.add_parser("compare")
    compare.add_argument("--baseline", required=True)
    compare.add_argument("--candidate", required=True)
    compare.add_argument("--output")
    args = parser.parse_args()

    if args.command == "mark":
        create_marker(args)
        return
    if args.command == "compare":
        baseline = json.loads(Path(args.baseline).read_text(encoding="utf-8"))
        candidate = json.loads(Path(args.candidate).read_text(encoding="utf-8"))
        comparison = compare_reports(baseline, candidate)
        if args.output:
            output = Path(args.output)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(comparison, encoding="utf-8")
        else:
            print(comparison, end="")
        return

    result = analyze(Path(args.marker))
    payload = json.dumps(result, indent=2, allow_nan=True) + "\n"
    if args.output_json:
        output = Path(args.output_json)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(payload)
    else:
        print(payload, end="")
    if args.output_md:
        output = Path(args.output_md)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(render_markdown(result))


if __name__ == "__main__":
    main()
