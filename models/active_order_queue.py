"""Strict sparse active-order queue tape loader for Python replay."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

SUPPORTED_SCHEMAS = frozenset(
    {"active_order_queue_tape_v2", "active_order_queue_tape_v3"}
)


class ActiveOrderQueueCoverageError(RuntimeError):
    """Raised when strict replay cannot identify one active-price queue."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normalize_side(value: str) -> str:
    side = str(value).strip().upper()
    if side in {"BUY", "BID"}:
        return "BUY"
    if side in {"SELL", "ASK"}:
        return "SELL"
    raise ValueError(f"unsupported active-order queue side: {value!r}")


@dataclass(frozen=True)
class ActiveOrderQueueSeed:
    watch_id: str
    side: str
    price_tick: int
    activate_ts_ms: int
    status: str
    reason: str
    quantity: float | None
    ambiguous: bool

    @property
    def strict_usable(self) -> bool:
        return (
            self.status in {"exact", "known_zero"}
            and not self.ambiguous
            and self.quantity is not None
            and math.isfinite(self.quantity)
            and self.quantity >= 0.0
        )


@dataclass(frozen=True)
class HistoricalActiveOrderQueueData:
    day: str
    symbol: str
    tick_size: float
    schema_version: str
    watch_manifest_sha256: str
    seeds_sha256: str
    events_sha256: str
    sequence_sha256: str
    seeds_by_identity: dict[tuple[str, int, int], ActiveOrderQueueSeed]
    summary: dict[str, object]
    sequence_audit: dict[str, object]
    source: str

    def lookup_seed(
        self,
        *,
        side: str,
        price: float,
        activate_ts_ms: int,
    ) -> ActiveOrderQueueSeed | None:
        price_tick = int(round(float(price) / self.tick_size))
        if not math.isclose(
            float(price),
            price_tick * self.tick_size,
            rel_tol=0.0,
            abs_tol=max(1e-9, self.tick_size * 1e-8),
        ):
            raise ActiveOrderQueueCoverageError(
                f"active order price is not tick aligned: {price!r}"
            )
        return self.seeds_by_identity.get(
            (
                _normalize_side(side),
                price_tick,
                int(activate_ts_ms),
            )
        )

    @property
    def strict_usable_count(self) -> int:
        return sum(seed.strict_usable for seed in self.seeds_by_identity.values())

    @property
    def watch_count(self) -> int:
        return len(self.seeds_by_identity)


def load_active_order_queue_data(
    directory: Path,
    *,
    expected_day: str,
    expected_symbol: str,
    expected_tick_size: float,
) -> HistoricalActiveOrderQueueData:
    root = directory.expanduser().resolve()
    summary_path = root / "summary.json"
    sequence_path = root / "sequence_audit.json"
    seeds_path = root / "seeds.parquet"
    events_path = root / "level_events.parquet"
    for path in (summary_path, sequence_path, seeds_path, events_path):
        if not path.is_file():
            raise FileNotFoundError(f"active-order queue artifact missing: {path}")

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    sequence = json.loads(sequence_path.read_text(encoding="utf-8"))
    schema_version = str(summary.get("schema_version", ""))
    if schema_version not in SUPPORTED_SCHEMAS:
        raise ValueError(
            f"unsupported active-order queue schema={schema_version!r}; "
            f"expected one of {sorted(SUPPORTED_SCHEMAS)!r}"
        )
    if str(summary.get("day", "")) != expected_day:
        raise ValueError("active-order queue day does not match replay day")
    if str(summary.get("symbol", "")).upper() != expected_symbol.upper():
        raise ValueError("active-order queue symbol does not match replay symbol")
    tick_size = float(summary.get("tick_size", 0.0) or 0.0)
    if not math.isclose(
        tick_size,
        float(expected_tick_size),
        rel_tol=0.0,
        abs_tol=1e-12,
    ):
        raise ValueError("active-order queue tick size does not match replay")
    if not bool(sequence.get("strict_native_snapshot", False)):
        raise ValueError("active-order queue requires strict native snapshots")
    if bool(sequence.get("delta_bootstrap_allowed", True)):
        raise ValueError("active-order queue must not use delta bootstrap")
    stats = dict(sequence.get("sequence_stats", {}))
    for key in (
        "sequence_gaps",
        "invalid_sequence_messages",
        "message_time_reversals",
    ):
        if int(stats.get(key, 0) or 0) != 0:
            raise ValueError(f"active-order queue sequence audit failed: {key}")
    if int(sequence.get("source_gap_count", 0) or 0) != 0:
        raise ValueError("active-order queue source gap audit failed")
    if int(sequence.get("time_reversal_count", 0) or 0) != 0:
        raise ValueError("active-order queue source time reversal audit failed")
    if summary.get("missing_raw_hours") or summary.get("missing_warmup_hours"):
        raise ValueError("active-order queue has missing raw source hours")

    frame = pd.read_parquet(seeds_path)
    required = {
        "watch_id",
        "side",
        "price_tick",
        "activate_ts_ms",
        "seed_status",
        "seed_reason",
        "seed_qty",
        "ambiguous",
    }
    if schema_version == "active_order_queue_tape_v3":
        required.update(
            {
                "seed_asof_ts_ms",
                "segment_id",
                "seed_best_bid_tick",
                "seed_best_ask_tick",
            }
        )
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"active-order queue seeds missing columns: {missing}")
    if len(frame) != int(summary.get("watch_count", -1)):
        raise ValueError("active-order queue seed/watch count mismatch")

    seeds: dict[tuple[str, int, int], ActiveOrderQueueSeed] = {}
    for row in frame.itertuples(index=False):
        quantity = (
            float(row.seed_qty)
            if pd.notna(row.seed_qty)
            else None
        )
        seed = ActiveOrderQueueSeed(
            watch_id=str(row.watch_id),
            side=_normalize_side(str(row.side)),
            price_tick=int(row.price_tick),
            activate_ts_ms=int(row.activate_ts_ms),
            status=str(row.seed_status),
            reason=str(row.seed_reason),
            quantity=quantity,
            ambiguous=bool(row.ambiguous),
        )
        key = (seed.side, seed.price_tick, seed.activate_ts_ms)
        if key in seeds:
            raise ValueError(f"duplicate active-order queue identity: {key}")
        seeds[key] = seed

    return HistoricalActiveOrderQueueData(
        day=str(summary["day"]),
        symbol=str(summary["symbol"]),
        tick_size=tick_size,
        schema_version=schema_version,
        watch_manifest_sha256=str(summary.get("watch_manifest_sha256", "")),
        seeds_sha256=_sha256(seeds_path),
        events_sha256=_sha256(events_path),
        sequence_sha256=_sha256(sequence_path),
        seeds_by_identity=seeds,
        summary=summary,
        sequence_audit=sequence,
        source=str(root),
    )
