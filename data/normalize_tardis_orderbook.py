#!/usr/bin/env python3
"""Reconstruct source-separated Tardis BTCUSDC top-20 books at 100 ms.

The normalized clock is the provider ``local_timestamp`` (receive time).  A
row stamped at boundary ``b`` contains only L2 messages with local timestamps
strictly before ``b``.  Tardis incremental L2 does not expose Binance
``U/u/pu`` sequence IDs; the resulting book is therefore a provider-normalized
replay candidate, never native-sequence or exact-queue evidence.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import hashlib
import json
import math
import os
import tempfile
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from data.download_cryptohft_orderbook import (
    OrderBookState,
    _bbo_schema,
    _l2_schema,
)
from data.download_tardis_archive import resolve_tardis_artifact_path
from data_paths import data_root, marketdata_root

SOURCE_ID = "tardis.0730-beinan.binance-futures.BTCUSDC.v1"
DATASET_ID = "normalized_tardis_l2_100ms_v1"
BOOK_TICKER = "book_ticker"
INCREMENTAL_L2 = "incremental_book_L2"
DAY_US = 86_400 * 1_000_000
DEFAULT_CADENCE_MS = 100
DEFAULT_FRESHNESS_MS = 500
CROSS_CHANNEL_MAX_AGE_MS = 5_000
CROSS_CHANNEL_MIN_COMPARABLE_RATIO = 0.99
CROSS_CHANNEL_MIN_EXACT_PRICE_RATIO = 0.95
CROSS_CHANNEL_MIN_WITHIN_TICK_RATIO = 0.95
ESTIMATED_NORMALIZED_BYTES_PER_FULL_DAY = 200 * 1024**2
GAP_EDGES_US = (
    1_000,
    2_000,
    5_000,
    10_000,
    25_000,
    50_000,
    100_000,
    250_000,
    500_000,
    1_000_000,
    2_000_000,
    5_000_000,
    10_000_000,
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(payload: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    os.replace(temporary, path)


def _day_start_us(day: str) -> int:
    value = date.fromisoformat(day)
    return int(
        datetime(value.year, value.month, value.day, tzinfo=timezone.utc).timestamp()
        * 1_000_000
    )


def _freshness_union_coverage(
    timestamps_ms: Sequence[int],
    *,
    start_ms: int,
    end_ms: int,
    freshness_ms: int,
) -> tuple[float, float]:
    """Return forward-visible interval coverage and timestamp p99 gap."""

    if not timestamps_ms or end_ms <= start_ms or freshness_ms <= 0:
        return 0.0, float("inf")
    ordered = np.asarray(timestamps_ms, dtype=np.int64)
    ordered.sort()
    covered = 0
    current_start = max(start_ms, int(ordered[0]))
    current_end = min(end_ms, int(ordered[0]) + freshness_ms)
    for timestamp in ordered[1:]:
        interval_start = max(start_ms, int(timestamp))
        interval_end = min(end_ms, int(timestamp) + freshness_ms)
        if interval_end <= interval_start:
            continue
        if interval_start <= current_end:
            current_end = max(current_end, interval_end)
        else:
            covered += max(0, current_end - current_start)
            current_start = interval_start
            current_end = interval_end
    covered += max(0, current_end - current_start)
    gaps = np.diff(ordered).astype(np.float64)
    p99_gap_ms = float(np.quantile(gaps, 0.99)) if len(gaps) else 0.0
    return min(1.0, covered / max(1, end_ms - start_ms)), p99_gap_ms


@dataclass
class GapHistogram:
    counts: list[int] = field(
        default_factory=lambda: [0] * (len(GAP_EDGES_US) + 1)
    )
    observations: int = 0
    maximum_us: int = 0

    def add(self, value_us: int) -> None:
        value = max(0, int(value_us))
        self.observations += 1
        self.maximum_us = max(self.maximum_us, value)
        index = int(np.searchsorted(GAP_EDGES_US, value, side="left"))
        self.counts[index] += 1

    def quantile_upper_us(self, probability: float) -> int | None:
        if not self.observations:
            return None
        target = int(math.ceil(float(probability) * self.observations))
        cumulative = 0
        for index, count in enumerate(self.counts):
            cumulative += count
            if cumulative >= target:
                if index < len(GAP_EDGES_US):
                    return int(GAP_EDGES_US[index])
                return int(self.maximum_us)
        return int(self.maximum_us)

    def as_dict(self) -> dict[str, Any]:
        return {
            "observations": self.observations,
            "maximum_us": self.maximum_us,
            "p99_upper_us": self.quantile_upper_us(0.99),
            "count_gt_500ms": sum(
                count
                for index, count in enumerate(self.counts)
                if index >= int(np.searchsorted(GAP_EDGES_US, 500_000, side="right"))
            ),
            "count_gt_5s": sum(
                count
                for index, count in enumerate(self.counts)
                if index >= int(np.searchsorted(GAP_EDGES_US, 5_000_000, side="right"))
            ),
        }


class _ParquetPairWriter:
    """Write BBO/L2/provider-clock files under one source identity."""

    def __init__(self, root: Path, symbol: str, day: str, levels: int) -> None:
        self.levels = int(levels)
        self.bbo_final = root / "bbo" / f"{symbol}-bbo-{day}.parquet"
        self.l2_final = root / "l2" / f"{symbol}-l2-{day}.parquet"
        self.clock_final = root / "clock" / f"{symbol}-clock-{day}.parquet"
        self.bbo_tmp = self.bbo_final.with_suffix(".parquet.tmp")
        self.l2_tmp = self.l2_final.with_suffix(".parquet.tmp")
        self.clock_tmp = self.clock_final.with_suffix(".parquet.tmp")
        self.bbo_final.parent.mkdir(parents=True, exist_ok=True)
        self.l2_final.parent.mkdir(parents=True, exist_ok=True)
        self.clock_final.parent.mkdir(parents=True, exist_ok=True)
        self.bbo_tmp.unlink(missing_ok=True)
        self.l2_tmp.unlink(missing_ok=True)
        self.clock_tmp.unlink(missing_ok=True)
        self.bbo_writer = pq.ParquetWriter(
            self.bbo_tmp, _bbo_schema(), compression="zstd"
        )
        self.l2_writer = pq.ParquetWriter(
            self.l2_tmp, _l2_schema(self.levels), compression="zstd"
        )
        self.clock_schema = pa.schema(
            [
                ("timestamp", pa.int64()),
                ("exchange_cut_timestamp_us", pa.int64()),
                ("last_provider_local_timestamp_us", pa.int64()),
                ("provider_visibility_delay_us", pa.int64()),
            ]
        )
        self.clock_writer = pq.ParquetWriter(
            self.clock_tmp, self.clock_schema, compression="zstd"
        )
        self.bbo: dict[str, list[float | int]] = {
            "timestamp": [],
            "best_bid": [],
            "best_bid_qty": [],
            "best_ask": [],
            "best_ask_qty": [],
        }
        self.l2: dict[str, list[float | int]] = {"timestamp": []}
        for level in range(1, self.levels + 1):
            for column_prefix in ("bid_px", "bid_qty", "ask_px", "ask_qty"):
                self.l2[f"{column_prefix}_{level}"] = []
        self.clock: dict[str, list[int]] = {
            "timestamp": [],
            "exchange_cut_timestamp_us": [],
            "last_provider_local_timestamp_us": [],
            "provider_visibility_delay_us": [],
        }
        self.rows = 0
        self.closed = False

    def append(
        self,
        timestamp_ms: int,
        bids: list[tuple[float, float]],
        asks: list[tuple[float, float]],
        *,
        exchange_cut_us: int,
        last_provider_local_us: int,
    ) -> None:
        self.bbo["timestamp"].append(timestamp_ms)
        self.bbo["best_bid"].append(bids[0][0])
        self.bbo["best_bid_qty"].append(bids[0][1])
        self.bbo["best_ask"].append(asks[0][0])
        self.bbo["best_ask_qty"].append(asks[0][1])
        self.l2["timestamp"].append(timestamp_ms)
        for offset in range(self.levels):
            bid_price, bid_quantity = bids[offset]
            ask_price, ask_quantity = asks[offset]
            level = offset + 1
            self.l2[f"bid_px_{level}"].append(bid_price)
            self.l2[f"bid_qty_{level}"].append(bid_quantity)
            self.l2[f"ask_px_{level}"].append(ask_price)
            self.l2[f"ask_qty_{level}"].append(ask_quantity)
        boundary_us = int(timestamp_ms) * 1_000
        self.clock["timestamp"].append(timestamp_ms)
        self.clock["exchange_cut_timestamp_us"].append(exchange_cut_us)
        self.clock["last_provider_local_timestamp_us"].append(
            last_provider_local_us
        )
        self.clock["provider_visibility_delay_us"].append(
            boundary_us - last_provider_local_us
        )
        if len(self.bbo["timestamp"]) >= 10_000:
            self.flush()

    def flush(self) -> None:
        count = len(self.bbo["timestamp"])
        if not count:
            return
        self.bbo_writer.write_table(
            pa.Table.from_pydict(self.bbo, schema=_bbo_schema())
        )
        self.l2_writer.write_table(
            pa.Table.from_pydict(self.l2, schema=_l2_schema(self.levels))
        )
        self.clock_writer.write_table(
            pa.Table.from_pydict(self.clock, schema=self.clock_schema)
        )
        self.rows += count
        for values in self.bbo.values():
            values.clear()
        for values in self.l2.values():
            values.clear()
        for values in self.clock.values():
            values.clear()

    def close(self, *, publish: bool) -> None:
        if self.closed:
            return
        self.closed = True
        self.flush()
        self.bbo_writer.close()
        self.l2_writer.close()
        self.clock_writer.close()
        if publish and self.rows:
            os.replace(self.bbo_tmp, self.bbo_final)
            os.replace(self.l2_tmp, self.l2_final)
            os.replace(self.clock_tmp, self.clock_final)
        else:
            self.bbo_tmp.unlink(missing_ok=True)
            self.l2_tmp.unlink(missing_ok=True)
            self.clock_tmp.unlink(missing_ok=True)


@dataclass
class ReconstructionStats:
    raw_rows: int = 0
    logical_messages: int = 0
    snapshot_messages: int = 0
    update_messages: int = 0
    invalid_rows: int = 0
    pre_snapshot_rows: int = 0
    causal_violations: int = 0
    local_clock_reversals: int = 0
    exchange_clock_reversals: int = 0
    emitted_rows: int = 0
    insufficient_depth_buckets: int = 0
    invalid_spread_buckets: int = 0


def _open_csv(path: Path) -> pacsv.CSVStreamingReader:
    return pacsv.open_csv(
        path,
        read_options=pacsv.ReadOptions(block_size=8 * 1024 * 1024),
    )


def reconstruct_l2(
    raw_path: Path,
    *,
    output_root: Path,
    day: str,
    symbol: str = "BTCUSDC",
    levels: int = 20,
    cadence_ms: int = DEFAULT_CADENCE_MS,
    pilot_duration_s: int | None = None,
) -> tuple[Path, Path, dict[str, Any]]:
    started = time.perf_counter()
    day_start = _day_start_us(day)
    day_end = day_start + DAY_US
    requested_end = day_end
    if pilot_duration_s is not None:
        requested_end = min(day_end, day_start + int(pilot_duration_s) * 1_000_000)
    cadence_us = int(cadence_ms) * 1_000
    book = OrderBookState()
    stats = ReconstructionStats()
    gaps = GapHistogram()
    writer = _ParquetPairWriter(output_root, symbol, day, levels)
    initialized = False
    current_message: tuple[int, int, bool] | None = None
    current_bucket: int | None = None
    previous_local_us: int | None = None
    previous_exchange_us: int | None = None
    maximum_applied_exchange_us = 0
    last_applied_local_us = 0
    output_first_ms: int | None = None
    output_last_ms: int | None = None
    output_timestamps_ms: list[int] = []
    first_exchange_cut_us: int | None = None
    last_exchange_cut_us: int | None = None
    snapshot_seen_at_start = False
    stopped_at_cut = False

    def emit(bucket: int | None) -> None:
        nonlocal output_first_ms, output_last_ms
        nonlocal first_exchange_cut_us, last_exchange_cut_us
        if bucket is None or not initialized:
            return
        boundary_us = (int(bucket) + 1) * cadence_us
        if boundary_us > requested_end or boundary_us >= day_end:
            return
        bids, asks = book.top_levels(levels)
        if len(bids) < levels or len(asks) < levels:
            stats.insufficient_depth_buckets += 1
            return
        if bids[0][0] <= 0.0 or asks[0][0] <= bids[0][0]:
            stats.invalid_spread_buckets += 1
            return
        timestamp_ms = boundary_us // 1_000
        writer.append(
            timestamp_ms,
            bids,
            asks,
            exchange_cut_us=maximum_applied_exchange_us,
            last_provider_local_us=last_applied_local_us,
        )
        stats.emitted_rows += 1
        output_timestamps_ms.append(timestamp_ms)
        output_first_ms = timestamp_ms if output_first_ms is None else output_first_ms
        output_last_ms = timestamp_ms
        first_exchange_cut_us = (
            maximum_applied_exchange_us
            if first_exchange_cut_us is None
            else first_exchange_cut_us
        )
        last_exchange_cut_us = maximum_applied_exchange_us

    try:
        for batch in _open_csv(raw_path):
            columns = {
                name: batch.column(name).to_numpy(zero_copy_only=False)
                for name in (
                    "exchange",
                    "symbol",
                    "timestamp",
                    "local_timestamp",
                    "is_snapshot",
                    "side",
                    "price",
                    "amount",
                )
            }
            for exchange, row_symbol, exchange_us, local_us, is_snapshot, side, price, amount in zip(
                columns["exchange"],
                columns["symbol"],
                columns["timestamp"],
                columns["local_timestamp"],
                columns["is_snapshot"],
                columns["side"],
                columns["price"],
                columns["amount"],
                strict=True,
            ):
                stats.raw_rows += 1
                exchange_us = int(exchange_us)
                local_us = int(local_us)
                snapshot = bool(is_snapshot)
                if local_us >= requested_end:
                    stopped_at_cut = requested_end < day_end
                    break
                if (
                    str(exchange) != "binance-futures"
                    or str(row_symbol) != symbol
                    or str(side) not in {"bid", "ask"}
                    or not math.isfinite(float(price))
                    or float(price) <= 0.0
                    or not math.isfinite(float(amount))
                    or float(amount) < 0.0
                    or not (day_start <= local_us < day_end)
                ):
                    stats.invalid_rows += 1
                    continue
                if local_us < exchange_us:
                    stats.causal_violations += 1
                message = (exchange_us, local_us, snapshot)
                if message != current_message:
                    stats.logical_messages += 1
                    if previous_local_us is not None:
                        if local_us < previous_local_us:
                            stats.local_clock_reversals += 1
                        else:
                            gaps.add(local_us - previous_local_us)
                    if (
                        previous_exchange_us is not None
                        and exchange_us < previous_exchange_us
                    ):
                        stats.exchange_clock_reversals += 1
                    previous_local_us = local_us
                    previous_exchange_us = exchange_us
                    next_bucket = local_us // cadence_us
                    if current_bucket is not None and next_bucket != current_bucket:
                        emit(current_bucket)
                    current_bucket = next_bucket
                    current_message = message
                    if snapshot:
                        book.reset()
                        initialized = True
                        stats.snapshot_messages += 1
                        if stats.logical_messages == 1:
                            snapshot_seen_at_start = True
                    else:
                        stats.update_messages += 1
                if not initialized:
                    stats.pre_snapshot_rows += 1
                    continue
                book.apply(str(side), float(price), float(amount))
                maximum_applied_exchange_us = max(
                    maximum_applied_exchange_us, exchange_us
                )
                last_applied_local_us = local_us
            if stopped_at_cut:
                break
        emit(current_bucket)
        writer.close(publish=True)
    except BaseException:
        writer.close(publish=False)
        raise

    duration_us = requested_end - day_start
    # The final day-boundary bin is deliberately not emitted because its
    # causal timestamp belongs to D+1.
    possible_rows = max(1, duration_us // cadence_us - 1)
    bucket_density = float(stats.emitted_rows / possible_rows)
    freshness_coverage, output_p99_gap_ms = _freshness_union_coverage(
        output_timestamps_ms,
        start_ms=day_start // 1_000,
        end_ms=requested_end // 1_000,
        freshness_ms=DEFAULT_FRESHNESS_MS,
    )
    gap_summary = gaps.as_dict()
    quality: dict[str, Any] = {
        "schema_version": "narrowgate.normalized_tardis_l2_day.v1",
        "source_id": SOURCE_ID,
        "dataset_id": DATASET_ID,
        "symbol": symbol,
        "day": day,
        "clock_source": "tardis_provider_local",
        "clock_unit": "microseconds_since_unix_epoch_utc",
        "causal_cut": "raw local_timestamp < normalized right boundary",
        "cadence_ms": cadence_ms,
        "levels": levels,
        "complete_day": pilot_duration_s is None,
        "pilot_duration_s": pilot_duration_s,
        "snapshot_seen_at_start": snapshot_seen_at_start,
        "raw_rows": stats.raw_rows,
        "logical_messages": stats.logical_messages,
        "snapshot_messages": stats.snapshot_messages,
        "update_messages": stats.update_messages,
        "invalid_rows": stats.invalid_rows,
        "pre_snapshot_rows": stats.pre_snapshot_rows,
        "causal_violations": stats.causal_violations,
        "local_clock_reversals": stats.local_clock_reversals,
        "exchange_clock_reversals": stats.exchange_clock_reversals,
        "emitted_rows": stats.emitted_rows,
        "possible_rows": possible_rows,
        "bucket_density": bucket_density,
        "freshness_ms": DEFAULT_FRESHNESS_MS,
        "freshness_union_coverage": freshness_coverage,
        "output_p99_gap_ms": output_p99_gap_ms,
        "first_timestamp_ms": output_first_ms,
        "last_timestamp_ms": output_last_ms,
        "insufficient_depth_buckets": stats.insufficient_depth_buckets,
        "invalid_spread_buckets": stats.invalid_spread_buckets,
        "logical_message_gap": gap_summary,
        "exchange_timestamp_summary": {
            "first_applied_cut_us": first_exchange_cut_us,
            "last_applied_cut_us": last_exchange_cut_us,
            "cut_is_maximum_exchange_timestamp_applied_before_provider_boundary": True,
        },
        "observed_internal_gap_valid": bool(
            gap_summary["p99_upper_us"] is not None
            and int(gap_summary["p99_upper_us"]) <= 500_000
            and int(gap_summary["maximum_us"]) <= 5_000_000
        ),
        "native_binance_sequence_ids_present": False,
        "native_sequence_continuity_proven": False,
        "exact_queue_policy_eligible": False,
        "aws_tokyo_receive_time": False,
        "policy_visible": False,
        "live_transport_eligible": False,
        "normalized_replay_candidate_before_cross_channel": bool(
            pilot_duration_s is None
            and snapshot_seen_at_start
            and stats.causal_violations == 0
            and stats.local_clock_reversals == 0
            and stats.invalid_spread_buckets == 0
            and freshness_coverage >= 0.99
            and output_p99_gap_ms <= DEFAULT_FRESHNESS_MS
            and gap_summary["p99_upper_us"] is not None
            and int(gap_summary["p99_upper_us"]) <= 500_000
            and int(gap_summary["maximum_us"]) <= 5_000_000
        ),
    }
    elapsed = time.perf_counter() - started
    quality["reconstruction_elapsed_s"] = elapsed
    quality["reconstruction_raw_rows_per_s"] = float(
        stats.raw_rows / max(elapsed, 1e-9)
    )
    return writer.bbo_final, writer.l2_final, quality


def audit_book_ticker(
    raw_path: Path,
    normalized_bbo: Path,
    *,
    day: str,
    pilot_duration_s: int | None = None,
    max_age_ms: int = CROSS_CHANNEL_MAX_AGE_MS,
    tick_size: float = 0.1,
) -> dict[str, Any]:
    started = time.perf_counter()
    table = pq.read_table(normalized_bbo)
    boundaries_us = table.column("timestamp").to_numpy() * 1_000
    bid = table.column("best_bid").to_numpy()
    bid_qty = table.column("best_bid_qty").to_numpy()
    ask = table.column("best_ask").to_numpy()
    ask_qty = table.column("best_ask_qty").to_numpy()
    index = 0
    latest: tuple[int, float, float, float, float] | None = None
    comparable = 0
    exact_price = 0
    within_tick = 0
    exact_quantity = 0
    quantity_close = 0
    age = GapHistogram()
    causal_violations = 0
    local_reversals = 0
    previous_local: int | None = None
    stop = False
    mismatch_examples: list[dict[str, Any]] = []
    raw_rows = 0
    day_start = _day_start_us(day)
    requested_end = day_start + DAY_US
    if pilot_duration_s is not None:
        requested_end = min(
            requested_end, day_start + int(pilot_duration_s) * 1_000_000
        )

    def compare_until(boundary_limit: int) -> None:
        nonlocal index, comparable, exact_price, within_tick
        nonlocal exact_quantity, quantity_close
        while index < len(boundaries_us) and boundaries_us[index] <= boundary_limit:
            if latest is not None:
                local_us, ticker_bid, ticker_bid_qty, ticker_ask, ticker_ask_qty = latest
                lag = int(boundaries_us[index] - local_us)
                if 0 <= lag <= max_age_ms * 1_000:
                    comparable += 1
                    age.add(lag)
                    price_error = max(
                        abs(float(bid[index]) - ticker_bid),
                        abs(float(ask[index]) - ticker_ask),
                    )
                    quantity_error = max(
                        abs(float(bid_qty[index]) - ticker_bid_qty),
                        abs(float(ask_qty[index]) - ticker_ask_qty),
                    )
                    if price_error <= 1e-9:
                        exact_price += 1
                    if price_error <= tick_size + 1e-9:
                        within_tick += 1
                    if quantity_error <= 1e-9:
                        exact_quantity += 1
                    quantity_scale = max(
                        abs(ticker_bid_qty), abs(ticker_ask_qty), 1e-9
                    )
                    if quantity_error <= max(0.001, 0.05 * quantity_scale):
                        quantity_close += 1
                    if price_error > tick_size + 1e-9 and len(mismatch_examples) < 20:
                        mismatch_examples.append(
                            {
                                "timestamp_ms": int(boundaries_us[index] // 1_000),
                                "book_ticker_age_us": lag,
                                "l2_bid": float(bid[index]),
                                "ticker_bid": ticker_bid,
                                "l2_ask": float(ask[index]),
                                "ticker_ask": ticker_ask,
                            }
                        )
            index += 1

    for batch in _open_csv(raw_path):
        timestamp = batch.column("timestamp").to_numpy(zero_copy_only=False)
        local = batch.column("local_timestamp").to_numpy(zero_copy_only=False)
        ask_amount = batch.column("ask_amount").to_numpy(zero_copy_only=False)
        ask_price = batch.column("ask_price").to_numpy(zero_copy_only=False)
        bid_price = batch.column("bid_price").to_numpy(zero_copy_only=False)
        bid_amount = batch.column("bid_amount").to_numpy(zero_copy_only=False)
        for exchange_us, local_us, aq, ap, bp, bq in zip(
            timestamp,
            local,
            ask_amount,
            ask_price,
            bid_price,
            bid_amount,
            strict=True,
        ):
            raw_rows += 1
            exchange_us = int(exchange_us)
            local_us = int(local_us)
            if local_us >= requested_end:
                stop = True
                break
            compare_until(local_us)
            if local_us < exchange_us:
                causal_violations += 1
            if previous_local is not None and local_us < previous_local:
                local_reversals += 1
            previous_local = local_us
            latest = (
                local_us,
                float(bp),
                float(bq),
                float(ap),
                float(aq),
            )
        if stop:
            break
    compare_until(2**63 - 1)
    denominator = max(1, comparable)
    elapsed = time.perf_counter() - started
    return {
        "book_ticker_raw_rows": raw_rows,
        "book_ticker_audit_elapsed_s": elapsed,
        "book_ticker_raw_rows_per_s": float(raw_rows / max(elapsed, 1e-9)),
        "book_ticker_rows_compared": comparable,
        "book_ticker_comparable_ratio": float(comparable / max(1, len(boundaries_us))),
        "book_ticker_price_exact_ratio": float(exact_price / denominator),
        "book_ticker_price_within_one_tick_ratio": float(within_tick / denominator),
        "book_ticker_quantity_exact_ratio": float(exact_quantity / denominator),
        "book_ticker_quantity_close_ratio": float(quantity_close / denominator),
        "book_ticker_age": age.as_dict(),
        "book_ticker_causal_violations": causal_violations,
        "book_ticker_local_clock_reversals": local_reversals,
        "price_exact_tolerance": 1e-9,
        "price_one_tick_tolerance": tick_size,
        "quantity_close_tolerance": "max(0.001 BTC, 5% of ticker top-size)",
        "price_gate": (
            f"exact>={CROSS_CHANNEL_MIN_EXACT_PRICE_RATIO} and "
            f"within_one_tick>={CROSS_CHANNEL_MIN_WITHIN_TICK_RATIO}"
        ),
        "threshold_provenance": (
            "v1 engineering QA envelope frozen before the 2025 batch; it "
            "allows independent-channel publication races and is not a "
            "statistical, policy, or exact-queue threshold"
        ),
        "quantity_is_diagnostic_not_gate": True,
        "mismatch_examples": mismatch_examples,
    }


def _sample_l2(path: Path, *, levels: int, stride: int) -> dict[str, np.ndarray]:
    columns = ["timestamp"]
    for level in range(1, levels + 1):
        columns.extend(
            (
                f"bid_px_{level}",
                f"bid_qty_{level}",
                f"ask_px_{level}",
                f"ask_qty_{level}",
            )
        )
    chunks: dict[str, list[np.ndarray]] = {column: [] for column in columns}
    offset = 0
    for batch in pq.ParquetFile(path).iter_batches(batch_size=50_000, columns=columns):
        take = np.arange(batch.num_rows)
        keep = ((take + offset) % stride) == 0
        for name in columns:
            chunks[name].append(
                batch.column(name).to_numpy(zero_copy_only=False)[keep]
            )
        offset += batch.num_rows
    return {
        name: np.concatenate(values) if values else np.asarray([])
        for name, values in chunks.items()
    }


def _l2_columns(levels: int) -> list[str]:
    columns = ["timestamp"]
    for level in range(1, levels + 1):
        columns.extend(
            (
                f"bid_px_{level}",
                f"bid_qty_{level}",
                f"ask_px_{level}",
                f"ask_qty_{level}",
            )
        )
    return columns


def _take_l2_rows(
    path: Path, *, levels: int, row_indices: np.ndarray
) -> tuple[np.ndarray, dict[str, np.ndarray]]:
    requested = np.unique(np.asarray(row_indices, dtype=np.int64))
    requested = requested[requested >= 0]
    columns = _l2_columns(levels)
    chunks: dict[str, list[np.ndarray]] = {name: [] for name in columns}
    selected_indices: list[np.ndarray] = []
    offset = 0
    cursor = 0
    for batch in pq.ParquetFile(path).iter_batches(
        batch_size=50_000, columns=columns
    ):
        end = offset + batch.num_rows
        begin_cursor = cursor
        while cursor < len(requested) and requested[cursor] < end:
            cursor += 1
        chosen = requested[begin_cursor:cursor]
        if len(chosen):
            local = pa.array(chosen - offset)
            selected_indices.append(chosen)
            for name in columns:
                chunks[name].append(
                    batch.column(name).take(local).to_numpy(zero_copy_only=False)
                )
        offset = end
        if cursor == len(requested):
            break
    indices = (
        np.concatenate(selected_indices)
        if selected_indices
        else np.asarray([], dtype=np.int64)
    )
    values = {
        name: np.concatenate(parts) if parts else np.asarray([])
        for name, parts in chunks.items()
    }
    return indices, values


def _top20_metrics(
    left: Mapping[str, np.ndarray],
    right: Mapping[str, np.ndarray],
    *,
    valid: np.ndarray,
    levels: int,
    tick_size: float,
) -> dict[str, Any]:
    price_total = 0
    price_exact = 0
    price_one_tick = 0
    quantity_total = 0
    quantity_exact = 0
    quantity_close = 0
    for level in range(1, levels + 1):
        for side in ("bid", "ask"):
            price_name = f"{side}_px_{level}"
            quantity_name = f"{side}_qty_{level}"
            price_error = np.abs(left[price_name][valid] - right[price_name][valid])
            quantity_left = left[quantity_name][valid]
            quantity_right = right[quantity_name][valid]
            quantity_error = np.abs(quantity_left - quantity_right)
            price_total += len(price_error)
            price_exact += int((price_error <= 1e-9).sum())
            price_one_tick += int((price_error <= tick_size + 1e-9).sum())
            quantity_total += len(quantity_error)
            quantity_exact += int((quantity_error <= 1e-9).sum())
            quantity_close += int(
                (
                    quantity_error
                    <= np.maximum(
                        0.001,
                        0.05 * np.maximum(np.abs(quantity_right), 1e-9),
                    )
                ).sum()
            )
    return {
        "matched_rows": int(valid.sum()),
        "matched_ratio": float(valid.mean()) if len(valid) else 0.0,
        "top20_price_exact_ratio": float(price_exact / max(1, price_total)),
        "top20_price_within_one_tick_ratio": float(
            price_one_tick / max(1, price_total)
        ),
        "top20_quantity_exact_ratio": float(
            quantity_exact / max(1, quantity_total)
        ),
        "top20_quantity_close_ratio": float(
            quantity_close / max(1, quantity_total)
        ),
    }


def compare_normalized_sources(
    tardis_l2: Path,
    cryptohft_l2: Path,
    *,
    tardis_clock: Path | None = None,
    levels: int = 20,
    stride: int = 100,
    max_nearest_lag_ms: int = 100,
    tick_size: float = 0.1,
) -> dict[str, Any]:
    """Compare overlap under causal exchange-time and nearest-clock views."""

    started = time.perf_counter()
    tardis = _sample_l2(tardis_l2, levels=levels, stride=stride)
    left_ts = tardis["timestamp"].astype(np.int64)
    right_ts = (
        pq.read_table(cryptohft_l2, columns=["timestamp"])
        .column("timestamp")
        .to_numpy()
        .astype(np.int64)
    )
    if not len(left_ts) or not len(right_ts):
        return {"dual_source_available": False, "matched_rows": 0}
    if bool(np.any(np.diff(right_ts) < 0)):
        return {
            "dual_source_available": True,
            "cryptohft_timestamp_monotonic": False,
            "comparison_valid": False,
            "cannot_upgrade_native_sequence_or_exact_queue": True,
        }
    insertion = np.searchsorted(right_ts, left_ts)
    hi = np.clip(insertion, 0, len(right_ts) - 1)
    lo = np.clip(insertion - 1, 0, len(right_ts) - 1)
    choose_hi = np.abs(right_ts[hi] - left_ts) < np.abs(right_ts[lo] - left_ts)
    nearest_index = np.where(choose_hi, hi, lo)
    nearest_lag = right_ts[nearest_index] - left_ts
    nearest_valid = np.abs(nearest_lag) <= int(max_nearest_lag_ms)

    causal_index = np.full(len(left_ts), -1, dtype=np.int64)
    causal_lag = np.full(len(left_ts), np.iinfo(np.int64).max, dtype=np.int64)
    causal_valid = np.zeros(len(left_ts), dtype=bool)
    exchange_cut_ms = np.asarray([], dtype=np.int64)
    if tardis_clock is not None and tardis_clock.is_file():
        clock = pq.read_table(
            tardis_clock, columns=["exchange_cut_timestamp_us"]
        ).column("exchange_cut_timestamp_us").to_numpy()
        exchange_cut_ms = clock[::stride].astype(np.int64) // 1_000
        if len(exchange_cut_ms) != len(left_ts):
            raise ValueError("Tardis L2 and clock sidecar row counts differ")
        causal_index = np.searchsorted(
            right_ts, exchange_cut_ms, side="right"
        ).astype(np.int64) - 1
        safe = np.clip(causal_index, 0, len(right_ts) - 1)
        causal_lag = exchange_cut_ms - right_ts[safe]
        causal_valid = (
            (causal_index >= 0)
            & (causal_lag >= 0)
            & (causal_lag <= int(max_nearest_lag_ms))
        )

    requested_indices = np.concatenate(
        (
            nearest_index[nearest_valid],
            causal_index[causal_valid],
        )
    )
    selected_indices, selected = _take_l2_rows(
        cryptohft_l2, levels=levels, row_indices=requested_indices
    )
    positions = {int(index): offset for offset, index in enumerate(selected_indices)}

    def align(indices: np.ndarray, valid: np.ndarray) -> dict[str, np.ndarray]:
        aligned: dict[str, np.ndarray] = {}
        safe_positions = np.zeros(len(indices), dtype=np.int64)
        for offset in np.flatnonzero(valid):
            safe_positions[offset] = positions[int(indices[offset])]
        for name in _l2_columns(levels):
            values = np.zeros(len(indices), dtype=selected[name].dtype)
            if valid.any():
                values[valid] = selected[name][safe_positions[valid]]
            aligned[name] = values
        return aligned

    nearest_values = align(nearest_index, nearest_valid)
    nearest_metrics = _top20_metrics(
        tardis,
        nearest_values,
        valid=nearest_valid,
        levels=levels,
        tick_size=tick_size,
    )
    nearest_metrics.update(
        {
            "clock": "clock-agnostic nearest normalized timestamp",
            "is_causality_proof": False,
            "max_abs_lag_ms": max_nearest_lag_ms,
            "lag_abs_p50_ms": float(
                np.quantile(np.abs(nearest_lag[nearest_valid]), 0.50)
            )
            if nearest_valid.any()
            else None,
            "lag_abs_p99_ms": float(
                np.quantile(np.abs(nearest_lag[nearest_valid]), 0.99)
            )
            if nearest_valid.any()
            else None,
        }
    )
    causal_metrics: dict[str, Any]
    if len(exchange_cut_ms):
        causal_values = align(causal_index, causal_valid)
        causal_metrics = _top20_metrics(
            tardis,
            causal_values,
            valid=causal_valid,
            levels=levels,
            tick_size=tick_size,
        )
        causal_metrics.update(
            {
                "clock": "Tardis applied exchange cut as-of CryptoHFT timestamp",
                "future_crypto_rows_forbidden": True,
                "max_backward_age_ms": max_nearest_lag_ms,
                "backward_age_p50_ms": float(
                    np.quantile(causal_lag[causal_valid], 0.50)
                )
                if causal_valid.any()
                else None,
                "backward_age_p99_ms": float(
                    np.quantile(causal_lag[causal_valid], 0.99)
                )
                if causal_valid.any()
                else None,
            }
        )
    else:
        causal_metrics = {
            "available": False,
            "reason": "missing Tardis exchange-cut clock sidecar",
        }
    output = {
        "dual_source_available": True,
        "cryptohft_timestamp_monotonic": True,
        "sample_stride_rows": stride,
        "tardis_sample_rows": len(left_ts),
        "cryptohft_rows": len(right_ts),
        "exchange_time_causal_asof": causal_metrics,
        "clock_agnostic_nearest": nearest_metrics,
        "price_exact_tolerance": 1e-9,
        "price_one_tick_tolerance": tick_size,
        "quantity_close_tolerance": "max(0.001 BTC, 5% of CryptoHFTData level size)",
        "provider_local_is_aws_receive_time": False,
        "policy_visible": False,
        "cannot_upgrade_native_sequence_or_exact_queue": True,
    }
    output["comparison_elapsed_s"] = time.perf_counter() - started
    return output


def _download_rows(manifest: Path, day: str) -> dict[str, Mapping[str, Any]]:
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    if not payload.get("complete"):
        raise RuntimeError(f"download manifest is incomplete: {manifest}")
    rows = {
        str(row["dataset"]): row
        for row in payload.get("downloads", [])
        if str(row.get("day")) == day
    }
    missing = {BOOK_TICKER, INCREMENTAL_L2}.difference(rows)
    if missing:
        raise RuntimeError(f"{day} is missing Tardis datasets: {sorted(missing)}")
    return rows


def normalize_day(
    manifest: Path,
    *,
    day: str,
    output_root: Path,
    cryptohft_root: Path | None = None,
    pilot_duration_s: int | None = None,
    force: bool = False,
) -> dict[str, Any]:
    rows = _download_rows(manifest, day)
    manifest_sha256 = _sha256(manifest)
    l2_row = rows[INCREMENTAL_L2]
    ticker_row = rows[BOOK_TICKER]
    l2_raw = resolve_tardis_artifact_path(str(l2_row["path"]))
    ticker_raw = resolve_tardis_artifact_path(str(ticker_row["path"]))
    quality_path = output_root / "quality" / f"BTCUSDC-{day}.json"
    if not force and quality_path.is_file():
        cached = json.loads(quality_path.read_text(encoding="utf-8"))
        expected_complete = pilot_duration_s is None
        raw_inputs = cached.get("raw_inputs", {})
        output_identities = [
            cached.get(name, {})
            for name in ("bbo_output", "l2_output", "clock_output")
        ]
        cache_valid = bool(
            bool(cached.get("complete_day")) == expected_complete
            and int(cached.get("pilot_duration_s") or 0)
            == int(pilot_duration_s or 0)
            and cached.get("download_manifest", {}).get("sha256")
            == manifest_sha256
            and raw_inputs.get(INCREMENTAL_L2, {}).get("sha256")
            == str(l2_row["sha256"])
            and raw_inputs.get(BOOK_TICKER, {}).get("sha256")
            == str(ticker_row["sha256"])
            and all(
                identity.get("path")
                and Path(str(identity["path"])).is_file()
                and _sha256(Path(str(identity["path"])))
                == identity.get("sha256")
                for identity in output_identities
            )
        )
        if cache_valid:
            cached["resume_status"] = "validated_existing"
            return cached
    started = time.perf_counter()
    bbo_path, l2_path, quality = reconstruct_l2(
        l2_raw,
        output_root=output_root,
        day=day,
        pilot_duration_s=pilot_duration_s,
    )
    quality["raw_inputs"] = {
        INCREMENTAL_L2: {
            "path": str(l2_raw.resolve()),
            "sha256": str(l2_row["sha256"]),
            "size_bytes": int(l2_row["size_bytes"]),
        },
        BOOK_TICKER: {
            "path": str(ticker_raw.resolve()),
            "sha256": str(ticker_row["sha256"]),
            "size_bytes": int(ticker_row["size_bytes"]),
        },
    }
    quality["download_manifest"] = {
        "path": str(manifest.resolve()),
        "sha256": manifest_sha256,
    }
    quality["book_ticker_audit"] = audit_book_ticker(
        ticker_raw,
        bbo_path,
        day=day,
        pilot_duration_s=pilot_duration_s,
    )
    ticker_audit = quality["book_ticker_audit"]
    quality["cross_channel_contract_valid"] = bool(
        ticker_audit["book_ticker_comparable_ratio"]
        >= CROSS_CHANNEL_MIN_COMPARABLE_RATIO
        and ticker_audit["book_ticker_price_exact_ratio"]
        >= CROSS_CHANNEL_MIN_EXACT_PRICE_RATIO
        and ticker_audit["book_ticker_price_within_one_tick_ratio"]
        >= CROSS_CHANNEL_MIN_WITHIN_TICK_RATIO
        and ticker_audit["book_ticker_causal_violations"] == 0
        and ticker_audit["book_ticker_local_clock_reversals"] == 0
    )
    quality["provider_normalized_replay_candidate"] = bool(
        quality["normalized_replay_candidate_before_cross_channel"]
        and quality["cross_channel_contract_valid"]
    )
    quality["bbo_output"] = {
        "path": str(bbo_path.resolve()),
        "sha256": _sha256(bbo_path),
        "size_bytes": bbo_path.stat().st_size,
    }
    quality["l2_output"] = {
        "path": str(l2_path.resolve()),
        "sha256": _sha256(l2_path),
        "size_bytes": l2_path.stat().st_size,
    }
    writer_clock = (
        output_root / "clock" / f"BTCUSDC-clock-{day}.parquet"
    ).resolve()
    quality["clock_output"] = {
        "path": str(writer_clock),
        "sha256": _sha256(writer_clock),
        "size_bytes": writer_clock.stat().st_size,
    }
    crypto_path = None
    if cryptohft_root is not None:
        candidate = (
            cryptohft_root / "l2" / f"BTCUSDC-l2-{day}.parquet"
        )
        if candidate.is_file():
            crypto_path = candidate
    if crypto_path is not None:
        quality["cryptohft_dual_source"] = compare_normalized_sources(
            l2_path, crypto_path, tardis_clock=writer_clock
        )
        quality["cryptohft_dual_source"]["path"] = str(crypto_path.resolve())
        quality["cryptohft_dual_source"]["sha256"] = _sha256(crypto_path)
    else:
        quality["cryptohft_dual_source"] = {
            "dual_source_available": False,
            "cannot_upgrade_native_sequence_or_exact_queue": True,
        }
    quality["total_elapsed_s"] = time.perf_counter() - started
    quality["resume_status"] = "rebuilt"
    _atomic_json(quality, quality_path)
    return quality


def _normalize_day_task(payload: Mapping[str, Any]) -> dict[str, Any]:
    cryptohft = payload.get("cryptohft_root")
    return normalize_day(
        Path(str(payload["manifest"])),
        day=str(payload["day"]),
        output_root=Path(str(payload["output_root"])),
        cryptohft_root=Path(str(cryptohft)) if cryptohft else None,
        pilot_duration_s=(
            int(payload["pilot_duration_s"])
            if payload.get("pilot_duration_s") is not None
            else None
        ),
        force=bool(payload.get("force")),
    )


def _normalize_day_safe(payload: Mapping[str, Any]) -> dict[str, Any]:
    day = str(payload["day"])
    try:
        return {"day": day, "ok": True, "quality": _normalize_day_task(payload)}
    except Exception as exc:  # noqa: BLE001 - isolate one daily admission
        return {
            "day": day,
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--day", action="append", default=[])
    parser.add_argument(
        "--days-file",
        type=Path,
        help="Optional CSV with a required day column; combined with repeated --day",
    )
    parser.add_argument(
        "--output-root", type=Path, default=data_root() / DATASET_ID
    )
    parser.add_argument(
        "--cryptohft-root", type=Path, default=data_root() / "normalized_l2_100ms_v2"
    )
    parser.add_argument("--pilot-duration-s", type=int)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--summary-json", type=Path)
    return parser


def _requested_days(explicit: Sequence[str], days_file: Path | None) -> list[str]:
    values = list(explicit)
    if days_file is not None:
        with days_file.expanduser().resolve().open(
            newline="", encoding="utf-8-sig"
        ) as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None or "day" not in reader.fieldnames:
                raise ValueError("--days-file must be a CSV with a day column")
            values.extend(str(row["day"]).strip() for row in reader)
    return sorted({date.fromisoformat(day).isoformat() for day in values if day})


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.pilot_duration_s is not None and args.pilot_duration_s <= 0:
        raise SystemExit("--pilot-duration-s must be positive")
    if args.workers < 1 or args.workers > 4:
        raise SystemExit("--workers must be in [1, 4]")
    try:
        days = _requested_days(args.day, args.days_file)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if not days:
        raise SystemExit("at least one --day or --days-file row is required")
    free = os.statvfs(args.output_root.parent if args.output_root.parent.exists() else marketdata_root())
    free_bytes = int(free.f_bavail * free.f_frsize)
    estimated_new = (
        len(days)
        * ESTIMATED_NORMALIZED_BYTES_PER_FULL_DAY
        * (1.0 if args.pilot_duration_s is None else args.pilot_duration_s / 86_400)
    )
    required_free = int(60 * 1024**3 + 2.5 * estimated_new)
    if free_bytes < max(50 * 1024**3, required_free):
        raise SystemExit(
            "storage safety gate failed: "
            f"free={free_bytes} required={required_free}"
        )
    payloads = [
        {
            "manifest": str(args.manifest.expanduser().resolve()),
            "day": day,
            "output_root": str(args.output_root.expanduser().resolve()),
            "cryptohft_root": str(args.cryptohft_root.expanduser().resolve()),
            "pilot_duration_s": args.pilot_duration_s,
            "force": args.force,
        }
        for day in days
    ]
    if args.workers == 1:
        task_results = [_normalize_day_safe(payload) for payload in payloads]
    else:
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=args.workers
        ) as executor:
            task_results = list(executor.map(_normalize_day_safe, payloads))
    task_results.sort(key=lambda row: str(row["day"]))
    results = [row["quality"] for row in task_results if row["ok"]]
    failures = [
        {"day": row["day"], "error": row["error"]}
        for row in task_results
        if not row["ok"]
    ]
    compact_days = [
        {
            "day": row["day"],
            "resume_status": row.get("resume_status"),
            "provider_normalized_replay_candidate": bool(
                row.get("provider_normalized_replay_candidate")
            ),
            "quality_path": str(
                args.output_root.expanduser().resolve()
                / "quality"
                / f"BTCUSDC-{row['day']}.json"
            ),
            "total_elapsed_s": row.get("total_elapsed_s"),
        }
        for row in results
    ]
    manifest_path = args.manifest.expanduser().resolve()
    summary = {
        "schema_version": "narrowgate.normalized_tardis_l2_batch.v1",
        "source_id": SOURCE_ID,
        "dataset_id": DATASET_ID,
        "download_manifest": {
            "path": str(manifest_path),
            "sha256": _sha256(manifest_path),
        },
        "requested_days": days,
        "completed_days": [row["day"] for row in results],
        "failed_days": failures,
        "workers": args.workers,
        "storage_preflight": {
            "free_bytes": free_bytes,
            "estimated_new_bytes": int(estimated_new),
            "required_free_bytes": required_free,
        },
        "provider_normalized_replay_candidates": sum(
            bool(row["provider_normalized_replay_candidate"]) for row in results
        ),
        "native_binance_sequence_ids_present": False,
        "exact_queue_policy_eligible": False,
        "daily_admissions": compact_days,
    }
    if args.summary_json:
        _atomic_json(summary, args.summary_json.expanduser().resolve())
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
