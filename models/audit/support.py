"""Shared schema helpers for NarrowGate audit reports.

中文说明：所有 audit report 先经过这里解析时间、数值、side/session。
这样 campaign、shadow avoidance、daily gate 不会各自发明 UTC/CST 和
BUY/SELL sign 口径。
"""

from __future__ import annotations

import csv
import gzip
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TextIO

EPS = 1e-10
INV_THRESHOLDS = (0.006, 0.008, 0.010)
AGE_THRESHOLDS_S = (20 * 60, 40 * 60, 60 * 60)
EARLY_WINDOWS_S = (5 * 60, 10 * 60, 20 * 60)


def parse_ts(value: Any) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        pass
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def utc_text(ts: float) -> str:
    if ts <= 0.0:
        return ""
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def utc_day(ts: float) -> str:
    if ts <= 0.0:
        return ""
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")


def safe_float(row: dict[str, Any], key: str, default: float = 0.0) -> float:
    try:
        value = row.get(key, default)
        if value in ("", None):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(row: dict[str, Any], key: str, default: int = 0) -> int:
    try:
        value = row.get(key, default)
        if value in ("", None):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def norm_side(value: Any) -> str:
    text = str(value or "").upper()
    if text in {"BID", "BUY", "B"}:
        return "BUY"
    if text in {"ASK", "SELL", "S"}:
        return "SELL"
    return text


def session_stack(ts: float) -> str:
    """Coarse UTC session stack for moderation, not direct policy."""
    if ts <= 0.0:
        return "unknown"
    hour = datetime.fromtimestamp(ts, timezone.utc).hour
    labels: list[str] = []
    if 0 <= hour < 9:
        labels.append("asia")
    if 0 <= hour < 6:
        labels.append("tokyo_sg_hk")
    if 7 <= hour < 16:
        labels.append("london")
    if 13 <= hour < 21:
        labels.append("us")
    if 13 <= hour < 16:
        labels.append("london_us_overlap")
    return "|".join(labels) if labels else "off_session"


@contextmanager
def _open_csv(path: Path) -> Iterator[TextIO]:
    if path.suffix.lower() == ".gz":
        with gzip.open(path, mode="rt", encoding="utf-8", newline="") as handle:
            yield handle
        return
    with path.open(encoding="utf-8", newline="") as handle:
        yield handle


def read_csv_table(path: Path | str) -> list[dict[str, Any]]:
    path = Path(path)
    if not path.exists():
        return []
    with _open_csv(path) as handle:
        return list(csv.DictReader(handle))


def read_csv_rows(
    path: Path | str,
    *,
    start_ts: float = 0.0,
    end_ts: float = 0.0,
    timestamp_col: str = "timestamp",
) -> list[dict[str, Any]]:
    path = Path(path)
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with _open_csv(path) as handle:
        for row in csv.DictReader(handle):
            ts = parse_ts(row.get(timestamp_col, "0"))
            if start_ts and ts < start_ts:
                continue
            if end_ts and ts > end_ts:
                continue
            row["_ts"] = ts
            rows.append(row)
    return rows


def default_live_paths(log_dir: Path | str) -> dict[str, Path]:
    log_dir = Path(log_dir)
    return {
        "trades": log_dir / "trades.csv",
        "order_outcomes": log_dir / "order_outcomes.csv",
        "quote_decisions": log_dir / "quote_decisions.csv",
        "inventory_campaign_shadow": log_dir / "inventory_campaign_shadow.csv",
        "sell_resiliency_shadow": log_dir / "sell_resiliency_shadow.csv",
    }


def write_csv(path: Path | str, rows: list[dict[str, Any]]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _fmt(value: Any) -> str:
    if isinstance(value, float):
        return f"{value:.6g}"
    return str(value)


def dict_section(title: str, data: dict[str, Any]) -> list[str]:
    lines = [f"## {title}", ""]
    if not data:
        return [*lines, "- no data", ""]
    for key, value in data.items():
        if isinstance(value, dict):
            lines.append(f"- {key}:")
            lines.extend(
                f"  - {sub_key}: {_fmt(sub_value)}"
                for sub_key, sub_value in value.items()
            )
        else:
            lines.append(f"- {key}: {_fmt(value)}")
    lines.append("")
    return lines


def table_section(
    title: str,
    rows: list[dict[str, Any]],
    *,
    max_rows: int = 20,
) -> list[str]:
    lines = [f"## {title}", ""]
    if not rows:
        return [*lines, "_no rows_", ""]
    columns = list(rows[0])
    lines.append("| " + " | ".join(columns) + " |")
    lines.append("| " + " | ".join(["---"] * len(columns)) + " |")
    for row in rows[:max_rows]:
        lines.append("| " + " | ".join(_fmt(row.get(col, "")) for col in columns) + " |")
    if len(rows) > max_rows:
        lines.append(f"\n_omitted {len(rows) - max_rows} rows_")
    lines.append("")
    return lines


def render_report(
    *,
    title: str,
    metadata: dict[str, Any],
    sections: list[
        tuple[str, dict[str, Any] | list[dict[str, Any]]]
    ],
) -> str:
    lines = [f"# {title}", ""]
    lines.extend(dict_section("Metadata", metadata))
    for name, payload in sections:
        lines.extend(
            table_section(name, payload)
            if isinstance(payload, list)
            else dict_section(name, payload)
        )
    return "\n".join(lines).rstrip() + "\n"
