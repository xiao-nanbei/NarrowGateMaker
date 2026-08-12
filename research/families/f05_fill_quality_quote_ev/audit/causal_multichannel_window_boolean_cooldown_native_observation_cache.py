#!/usr/bin/env python3
"""Exact, reusable raw-native 100ms observation cache for cooldown v2.

The raw CryptoHFT book scheduler is intentionally kept sequential: snapshot,
delta, sequence, gap, reset, and top-20 boundary state are order dependent.
This module removes repeated materialization work without weakening that
contract.  It merges official individual trades with a monotone cursor, writes
the authoritative observation stream once, verifies an IEEE-754 row digest
after the Parquet round trip, and atomically admits the daily cache.

The artifact is outcome blind.  It contains no assignment, fill, reward, PnL,
or policy output and never claims receive-time transport authority.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import struct
import uuid
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass, fields, is_dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from data_paths import data_root, marketdata_root
from models import backtest_tick as bt
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_native_features as native,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_features import (
    BASE_WINDOW_WIDTH_NS,
    CausalWindowObservation,
)

IDENTITY = (
    "causal_multichannel_window_boolean_cooldown_duration_v2."
    "raw_native_observation_cache.v1"
)
SCHEMA_VERSION = f"{IDENTITY}.schema.v1"
MANIFEST_SCHEMA_VERSION = f"{IDENTITY}.day_manifest.v1"
SUCCESS_NAME = "_SUCCESS"
PARQUET_NAME = "observations.parquet"
MANIFEST_NAME = "manifest.json"
DAY_NS = 86_400_000_000_000
EXPECTED_FORMAL_WINDOW_COUNT = 2 * DAY_NS // BASE_WINDOW_WIDTH_NS
DEFAULT_DATA_ROOT = data_root(Path(__file__).resolve().parents[4])
DEFAULT_OUTPUT_ROOT = DEFAULT_DATA_ROOT / (
    "cache/replay_dag/"
    "causal_multichannel_window_boolean_cooldown_native_observation_v1"
)
DEFAULT_RAW_NATIVE_ROOT = marketdata_root() / "cryptohftdata"
DEFAULT_NATIVE_BOOK_CACHE = DEFAULT_DATA_ROOT / "cache/replay_dag/native_exchange_book_hour_v1"
DEFAULT_INDIVIDUAL_TRADE_ROOT = Path(bt.RAW_TRADES_DIR)

CORE_COLUMNS = (
    "left_ts_ns",
    "right_ts_ns",
    "feature_ready_ts_ns",
    "market_generation",
    "depth_generation",
    "source_gap",
    "source_stale",
    "warmup_admitted",
)
VALUE_COLUMNS = native.native_m2_observation_channel_names()
ALL_COLUMNS = CORE_COLUMNS + VALUE_COLUMNS
DIGEST_ALGORITHM = "sha256_framed_v1_ieee754_binary64"


class NativeObservationCacheError(RuntimeError):
    """Raised when cache source, semantics, or atomic admission drifts."""


@dataclass(frozen=True, slots=True)
class CacheValidation:
    day_root: Path
    manifest: Mapping[str, Any]
    observation_count: int
    observation_sha256: str


@dataclass(frozen=True, slots=True)
class AdmittedNativeObservationCache:
    """Validated cache handle consumable by the owner feature-panel builder."""

    day_root: Path
    manifest: Mapping[str, Any]

    def observations(self, *, batch_size: int = 65_536) -> Iterator[CausalWindowObservation]:
        return iter_cached_observations(
            self.day_root.parent,
            str(self.manifest["utc_day"]),
            batch_size=batch_size,
        )

    def observations_between(
        self,
        *,
        start_feature_ready_ts_ns: int,
        end_feature_ready_ts_ns: int,
        batch_size: int = 65_536,
    ) -> Iterator[CausalWindowObservation]:
        return iter_cached_observations_between(
            self.day_root.parent,
            str(self.manifest["utc_day"]),
            start_feature_ready_ts_ns=start_feature_ready_ts_ns,
            end_feature_ready_ts_ns=end_feature_ready_ts_ns,
            batch_size=batch_size,
        )

    @property
    def book_audit(self) -> native.NativeM2BookFeatureAudit:
        payload = dict(self.manifest["book_feature_audit"])
        payload["unobserved_reason_counts"] = MappingProxyType(
            dict(payload["unobserved_reason_counts"])
        )
        return native.NativeM2BookFeatureAudit(**payload)

    @property
    def trade_audit(self) -> native.NativeM2TradeMergeAudit:
        return native.NativeM2TradeMergeAudit(**dict(self.manifest["trade_merge_audit"]))


def _canonical_json_bytes(payload: Any) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def canonical_sha256(payload: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_file(path: Path) -> None:
    with Path(path).open("rb") as handle:
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    with Path(path).open("x", encoding="ascii") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="ascii"))
    except (OSError, json.JSONDecodeError) as exc:
        raise NativeObservationCacheError(f"cannot read JSON artifact: {path}") from exc
    if not isinstance(payload, dict):
        raise NativeObservationCacheError(f"JSON root must be an object: {path}")
    return payload


def audit_payload(value: Any) -> dict[str, Any]:
    """Serialize a frozen audit without copying its MappingProxyType fields."""

    if not is_dataclass(value) or isinstance(value, type):
        raise NativeObservationCacheError("audit payload must be a dataclass instance")
    payload: dict[str, Any] = {}
    for field in fields(value):
        item = getattr(value, field.name)
        if isinstance(item, Mapping):
            payload[field.name] = dict(item)
        else:
            payload[field.name] = item
    return payload


def _parse_day(day: str) -> date:
    try:
        parsed = date.fromisoformat(str(day))
    except ValueError as exc:
        raise NativeObservationCacheError(f"invalid UTC day: {day!r}") from exc
    if parsed.isoformat() != str(day):
        raise NativeObservationCacheError(f"UTC day is not canonical ISO format: {day!r}")
    return parsed


def _day_start_ns(day: str) -> int:
    parsed = _parse_day(day)
    return int(datetime(parsed.year, parsed.month, parsed.day, tzinfo=UTC).timestamp()) * 1_000_000_000


def _parquet_schema() -> pa.Schema:
    fields = [
        pa.field("left_ts_ns", pa.int64(), nullable=False),
        pa.field("right_ts_ns", pa.int64(), nullable=False),
        pa.field("feature_ready_ts_ns", pa.int64(), nullable=False),
        pa.field("market_generation", pa.int64(), nullable=False),
        pa.field("depth_generation", pa.int64(), nullable=False),
        pa.field("source_gap", pa.bool_(), nullable=False),
        pa.field("source_stale", pa.bool_(), nullable=False),
        pa.field("warmup_admitted", pa.bool_(), nullable=False),
    ]
    fields.extend(pa.field(name, pa.float64(), nullable=True) for name in VALUE_COLUMNS)
    metadata = {
        b"identity": IDENTITY.encode("ascii"),
        b"schema_version": SCHEMA_VERSION.encode("ascii"),
        b"window_boundary": b"100ms_left_closed_right_open_partial_excluded",
        b"feature_ready_clock": native.EXCHANGE_WINDOW_READY_CLOCK.encode("ascii"),
        b"economic_outcomes_read": b"false",
    }
    return pa.schema(fields, metadata=metadata)


PARQUET_SCHEMA = _parquet_schema()
PARQUET_SCHEMA_SHA256 = canonical_sha256(
    {
        "fields": [
            {"name": field.name, "type": str(field.type), "nullable": field.nullable}
            for field in PARQUET_SCHEMA
        ],
        "metadata": {
            key.decode("ascii"): value.decode("ascii")
            for key, value in sorted((PARQUET_SCHEMA.metadata or {}).items())
        },
    }
)


def _observation_row(observation: CausalWindowObservation) -> dict[str, Any]:
    if set(observation.values) != set(VALUE_COLUMNS):
        missing = sorted(set(VALUE_COLUMNS) - set(observation.values))
        extra = sorted(set(observation.values) - set(VALUE_COLUMNS))
        raise NativeObservationCacheError(
            f"observation value schema drifted: missing={missing}, extra={extra}"
        )
    row: dict[str, Any] = {
        "left_ts_ns": int(observation.left_ts_ns),
        "right_ts_ns": int(observation.right_ts_ns),
        "feature_ready_ts_ns": int(observation.feature_ready_ts_ns),
        "market_generation": int(observation.market_generation),
        "depth_generation": int(observation.depth_generation),
        "source_gap": bool(observation.source_gap),
        "source_stale": bool(observation.source_stale),
        "warmup_admitted": bool(observation.warmup_admitted),
    }
    for name in VALUE_COLUMNS:
        value = observation.values[name]
        if value is None:
            row[name] = None
            continue
        parsed = float(value)
        if not math.isfinite(parsed):
            raise NativeObservationCacheError(f"observation value is non-finite: {name}")
        row[name] = parsed
    return row


def _row_observation(row: Mapping[str, Any]) -> CausalWindowObservation:
    values = {
        name: None if row[name] is None else float(row[name]) for name in VALUE_COLUMNS
    }
    return CausalWindowObservation(
        left_ts_ns=int(row["left_ts_ns"]),
        right_ts_ns=int(row["right_ts_ns"]),
        feature_ready_ts_ns=int(row["feature_ready_ts_ns"]),
        market_generation=int(row["market_generation"]),
        depth_generation=int(row["depth_generation"]),
        values=MappingProxyType(values),
        source_gap=bool(row["source_gap"]),
        source_stale=bool(row["source_stale"]),
        warmup_admitted=bool(row["warmup_admitted"]),
    )


def _update_row_digest(digest: Any, row: Mapping[str, Any]) -> None:
    digest.update(b"\x01")
    for name in CORE_COLUMNS[:5]:
        digest.update(struct.pack(">q", int(row[name])))
    for name in CORE_COLUMNS[5:]:
        digest.update(b"\x01" if bool(row[name]) else b"\x00")
    for name in VALUE_COLUMNS:
        value = row[name]
        if value is None:
            digest.update(b"\x00")
        else:
            digest.update(b"\x01")
            digest.update(struct.pack(">d", float(value)))


def observation_sha256(observations: Iterable[CausalWindowObservation]) -> tuple[str, int]:
    digest = hashlib.sha256()
    digest.update(DIGEST_ALGORITHM.encode("ascii") + b"\x00")
    digest.update(PARQUET_SCHEMA_SHA256.encode("ascii") + b"\x00")
    count = 0
    for observation in observations:
        _update_row_digest(digest, _observation_row(observation))
        count += 1
    return digest.hexdigest(), count


def _validate_observation_order(
    row: Mapping[str, Any],
    *,
    previous_right_ns: int | None,
    formal_exchange_day: bool,
) -> None:
    left_ns = int(row["left_ts_ns"])
    right_ns = int(row["right_ts_ns"])
    if right_ns - left_ns != BASE_WINDOW_WIDTH_NS:
        raise NativeObservationCacheError("cache received a non-100ms observation")
    if previous_right_ns is not None and left_ns != previous_right_ns:
        raise NativeObservationCacheError("cache observations are not contiguous")
    if formal_exchange_day and int(row["feature_ready_ts_ns"]) != right_ns:
        raise NativeObservationCacheError(
            "formal historical cache requires exchange-window feature-ready time"
        )


def _batch_table(rows: list[dict[str, Any]]) -> pa.Table:
    columns = [pa.array([row[name] for row in rows], type=PARQUET_SCHEMA.field(name).type) for name in ALL_COLUMNS]
    return pa.Table.from_arrays(columns, schema=PARQUET_SCHEMA)


def _manifest_without_self_hash(manifest: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(manifest)
    payload.pop("canonical_manifest_sha256", None)
    return payload


def stream_indexed_native_m2_causal_observations(
    *,
    book_windows: Iterable[native.NativeM2BookWindow],
    official_trades: pd.DataFrame,
    audit: native.NativeM2TradeMergeAccumulator | None = None,
) -> Iterator[CausalWindowObservation]:
    """Exact cursor merger equivalent to the reference per-window search.

    The trade rows are still visited in original source order, preserving the
    reference Python floating-point accumulation order.  Only repeated binary
    searches over the full trade timestamp vector are removed.
    """

    normalized = native._normalize_official_individual_trades(official_trades)
    stats = audit if audit is not None else native.NativeM2TradeMergeAccumulator()
    channel_names = native.native_m2_observation_channel_names()
    channel_name_set = frozenset(channel_names)
    book_name_set = frozenset(native._BOOK_TO_M2_CHANNEL)
    trade_name_set = frozenset(native._TRADE_M2_CHANNELS)
    if book_name_set | trade_name_set != channel_name_set:
        raise NativeObservationCacheError("native M2 merge schema drifted")

    timestamps = normalized.exchange_ts_ns
    quantities = normalized.quantity_btc
    aggressive_buys = normalized.aggressive_buy
    previous_right_ns: int | None = None
    policy_start_ns: int | None = None
    trade_cursor = 0
    previous_trade_side: bool | None = None
    terminal_run = 0
    last_buy_ts_ns: int | None = None
    last_sell_ts_ns: int | None = None
    trade_total = len(timestamps)

    for book in book_windows:
        if int(book.right_ts_ns) - int(book.left_ts_ns) != BASE_WINDOW_WIDTH_NS:
            raise NativeObservationCacheError("book merge received a non-100ms window")
        if previous_right_ns is not None and int(book.left_ts_ns) != previous_right_ns:
            raise NativeObservationCacheError(
                "book windows must be contiguous and strictly ordered"
            )
        if policy_start_ns is None:
            policy_start_ns = int(book.policy_start_ns)
            trade_cursor = int(np.searchsorted(timestamps, int(book.left_ts_ns), side="left"))
        elif int(book.policy_start_ns) != policy_start_ns:
            raise NativeObservationCacheError("book policy_start_ns changed mid-stream")

        buy_quantity = 0.0
        sell_quantity = 0.0
        trade_count = 0
        right_ns = int(book.right_ts_ns)
        while trade_cursor < trade_total and int(timestamps[trade_cursor]) < right_ns:
            exchange_ts_ns = int(timestamps[trade_cursor])
            aggressive_buy = bool(aggressive_buys[trade_cursor])
            quantity = float(quantities[trade_cursor])
            if aggressive_buy:
                buy_quantity += quantity
                last_buy_ts_ns = exchange_ts_ns
                stats.aggressive_buy_trade_count += 1
            else:
                sell_quantity += quantity
                last_sell_ts_ns = exchange_ts_ns
                stats.aggressive_sell_trade_count += 1
            terminal_run = terminal_run + 1 if previous_trade_side is aggressive_buy else 1
            previous_trade_side = aggressive_buy
            trade_cursor += 1
            trade_count += 1

        boundary_cursor = trade_cursor
        while boundary_cursor < trade_total and int(timestamps[boundary_cursor]) == right_ns:
            boundary_cursor += 1
        stats.right_boundary_exclusion_count += boundary_cursor - trade_cursor
        stats.official_trade_count += trade_count

        values: dict[str, float | None] = {name: None for name in channel_names}
        if bool(book.support_valid):
            for output_name, raw_name in native._BOOK_TO_M2_CHANNEL.items():
                raw_value = book.values.get(raw_name)
                if raw_value is None:
                    values[output_name] = None
                    continue
                parsed = float(raw_value)
                if not math.isfinite(parsed):
                    raise NativeObservationCacheError(
                        f"raw-native book value is non-finite: {raw_name}"
                    )
                values[output_name] = parsed
            total_quantity = buy_quantity + sell_quantity
            rate = 1_000_000_000.0 / BASE_WINDOW_WIDTH_NS
            values.update(
                {
                    "aggressive_buy_qty_btc_per_s": buy_quantity * rate,
                    "aggressive_sell_qty_btc_per_s": sell_quantity * rate,
                    "signed_flow_imbalance": (
                        (buy_quantity - sell_quantity) / total_quantity
                        if total_quantity > 0.0
                        else 0.0
                    ),
                    "trade_count_per_s": float(trade_count) * rate,
                    "buy_run_length": float(
                        terminal_run if previous_trade_side is True else 0
                    ),
                    "sell_run_length": float(
                        terminal_run if previous_trade_side is False else 0
                    ),
                    "last_aggressive_buy_age_s": (
                        None
                        if last_buy_ts_ns is None
                        else (right_ns - last_buy_ts_ns) / 1_000_000_000.0
                    ),
                    "last_aggressive_sell_age_s": (
                        None
                        if last_sell_ts_ns is None
                        else (right_ns - last_sell_ts_ns) / 1_000_000_000.0
                    ),
                }
            )
        else:
            previous_trade_side = None
            terminal_run = 0
            last_buy_ts_ns = None
            last_sell_ts_ns = None
            stats.source_unobserved_window_count += 1

        if frozenset(values) != channel_name_set:
            raise NativeObservationCacheError("merged M2 observation schema drifted")
        stats.window_count += 1
        stats.warmup_window_count += int(book.phase == "D_MINUS_1_WARMUP")
        stats.policy_window_count += int(book.phase == "POLICY")
        if stats.first_window_left_ts_ns == 0:
            stats.first_window_left_ts_ns = int(book.left_ts_ns)
        stats.last_window_right_ts_ns = right_ns
        previous_right_ns = right_ns

        yield CausalWindowObservation(
            left_ts_ns=int(book.left_ts_ns),
            right_ts_ns=right_ns,
            feature_ready_ts_ns=max(int(book.feature_ready_ts_ns), right_ns),
            market_generation=int(book.market_generation),
            depth_generation=int(book.depth_generation),
            values=MappingProxyType(values),
            source_gap=not bool(book.support_valid),
            source_stale=bool(book.source_stale),
            warmup_admitted=bool(book.warmup_admitted),
        )


def materialize_observation_cache(
    *,
    day: str,
    observations: Iterable[CausalWindowObservation],
    output_root: Path,
    source_binding: Mapping[str, Any],
    book_audit: native.NativeM2BookFeatureAccumulator,
    trade_audit: native.NativeM2TradeMergeAccumulator,
    formal_exchange_day: bool = True,
    batch_size: int = 16_384,
) -> dict[str, Any]:
    """Write, verify, and atomically admit one compact observation day."""

    if batch_size <= 0:
        raise NativeObservationCacheError("batch_size must be positive")
    parsed_day = _parse_day(day)
    root = Path(output_root).expanduser().resolve()
    destination = root / parsed_day.isoformat()
    root.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        raise NativeObservationCacheError(f"cache day already exists: {destination}")
    stale_staging = tuple(sorted(root.glob(f".{day}.staging-*")))
    if stale_staging:
        raise NativeObservationCacheError(
            f"stale cache staging exists; refusing implicit recovery: {stale_staging}"
        )
    lock_path = root / f".{day}.lock"
    try:
        lock_fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as exc:
        raise NativeObservationCacheError(f"cache day build lock already exists: {lock_path}") from exc
    staging = root / f".{day}.staging-{os.getpid()}-{uuid.uuid4().hex}"
    try:
        os.write(lock_fd, f"pid={os.getpid()}\n".encode("ascii"))
        os.fsync(lock_fd)
        os.close(lock_fd)
        staging.mkdir()
        parquet_path = staging / PARQUET_NAME
        writer = pq.ParquetWriter(
            parquet_path,
            PARQUET_SCHEMA,
            compression="zstd",
            use_dictionary=False,
            write_statistics=True,
        )
        digest = hashlib.sha256()
        digest.update(DIGEST_ALGORITHM.encode("ascii") + b"\x00")
        digest.update(PARQUET_SCHEMA_SHA256.encode("ascii") + b"\x00")
        rows: list[dict[str, Any]] = []
        observation_count = 0
        first_left_ns = 0
        last_right_ns = 0
        previous_right_ns: int | None = None
        try:
            for observation in observations:
                row = _observation_row(observation)
                _validate_observation_order(
                    row,
                    previous_right_ns=previous_right_ns,
                    formal_exchange_day=formal_exchange_day,
                )
                if observation_count == 0:
                    first_left_ns = int(row["left_ts_ns"])
                previous_right_ns = int(row["right_ts_ns"])
                last_right_ns = previous_right_ns
                _update_row_digest(digest, row)
                rows.append(row)
                observation_count += 1
                if len(rows) >= batch_size:
                    writer.write_table(_batch_table(rows), row_group_size=len(rows))
                    rows.clear()
            if rows:
                writer.write_table(_batch_table(rows), row_group_size=len(rows))
                rows.clear()
        finally:
            writer.close()
        if observation_count == 0:
            raise NativeObservationCacheError("cannot admit an empty observation cache")

        day_start_ns = _day_start_ns(day)
        if formal_exchange_day:
            expected_first = day_start_ns - DAY_NS
            expected_last = day_start_ns + DAY_NS
            if (
                observation_count != EXPECTED_FORMAL_WINDOW_COUNT
                or first_left_ns != expected_first
                or last_right_ns != expected_last
            ):
                raise NativeObservationCacheError(
                    "formal cache does not cover exact D-1 plus target UTC day"
                )

        _fsync_file(parquet_path)
        stream_sha256 = digest.hexdigest()
        readback_sha256, readback_count, readback_first, readback_last = (
            _parquet_observation_digest(parquet_path)
        )
        if (
            readback_sha256 != stream_sha256
            or readback_count != observation_count
            or readback_first != first_left_ns
            or readback_last != last_right_ns
        ):
            raise NativeObservationCacheError(
                "Parquet round-trip changed observation rows or ordering"
            )

        frozen_book = audit_payload(book_audit.freeze())
        frozen_trade = audit_payload(trade_audit.freeze())
        if int(frozen_book["window_count"]) != observation_count:
            raise NativeObservationCacheError("book audit window count drifted")
        if int(frozen_trade["window_count"]) != observation_count:
            raise NativeObservationCacheError("trade audit window count drifted")
        source_binding_dict = json.loads(_canonical_json_bytes(source_binding))
        manifest: dict[str, Any] = {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "identity": IDENTITY,
            "utc_day": day,
            "status": "atomic_raw_native_observation_cache_admitted",
            "formal_exchange_day": bool(formal_exchange_day),
            "source_semantics": {
                "book_source": "raw_CryptoHFT_snapshot_delta",
                "book_clock": "exchange_transaction_time_ns",
                "official_trade_source": "Binance_Futures_BTCUSDC_individual_trades",
                "official_trade_clock": "exchange_transact_time_ms",
                "feature_ready_clock": native.EXCHANGE_WINDOW_READY_CLOCK,
                "window_width_ns": BASE_WINDOW_WIDTH_NS,
                "window_boundary": "left_closed_right_open",
                "partial_window_policy": "exclude",
                "warmup": "previous_natural_UTC_day_24h",
                "gap_stale_missing": "preserved_fail_closed_from_authoritative_stream",
                "receive_time_transport_authority": False,
            },
            "observation_count": observation_count,
            "first_left_ts_ns": first_left_ns,
            "last_right_ts_ns": last_right_ns,
            "value_columns": list(VALUE_COLUMNS),
            "parquet": {
                "path": PARQUET_NAME,
                "sha256": sha256_file(parquet_path),
                "size_bytes": parquet_path.stat().st_size,
                "schema_sha256": PARQUET_SCHEMA_SHA256,
                "row_count": observation_count,
                "row_group_count": pq.ParquetFile(parquet_path).metadata.num_row_groups,
            },
            "observation_digest_algorithm": DIGEST_ALGORITHM,
            "source_stream_observation_sha256": stream_sha256,
            "cache_readback_observation_sha256": readback_sha256,
            "source_binding": source_binding_dict,
            "source_binding_sha256": canonical_sha256(source_binding_dict),
            "book_feature_audit": frozen_book,
            "trade_merge_audit": frozen_trade,
            "implementation": {
                "cache_builder_sha256": sha256_file(Path(__file__).resolve()),
                "authoritative_native_feature_engine_sha256": sha256_file(
                    Path(native.__file__).resolve()
                ),
                "book_materialization": "authoritative_sequential_scheduler_once",
                "trade_merge": "monotone_cursor_original_row_order",
                "remaining_bottleneck": (
                    "raw snapshot/delta scheduler and per-window top20 depth-shape "
                    "calculation remain sequential by exactness contract"
                ),
            },
            "economic_outcomes_read": False,
            "assignment_inputs_read": False,
            "model_trained": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "exact_queue_policy_eligible": False,
            "action_authorized": False,
            "live_policy_authorized": False,
        }
        manifest["canonical_manifest_sha256"] = canonical_sha256(manifest)
        manifest_path = staging / MANIFEST_NAME
        _write_json(manifest_path, manifest)
        manifest_file_sha256 = sha256_file(manifest_path)
        success_path = staging / SUCCESS_NAME
        success_path.write_text(manifest_file_sha256 + "\n", encoding="ascii")
        _fsync_file(success_path)
        _fsync_directory(staging)
        if destination.exists():
            raise NativeObservationCacheError(
                "cache destination appeared during atomic admission"
            )
        os.replace(staging, destination)
        _fsync_directory(root)
        return manifest
    except BaseException:
        if staging.exists():
            shutil.rmtree(staging)
        raise
    finally:
        try:
            os.close(lock_fd)
        except OSError:
            pass
        lock_path.unlink(missing_ok=True)


def _parquet_observation_digest(path: Path) -> tuple[str, int, int, int]:
    parquet = pq.ParquetFile(path)
    if not parquet.schema_arrow.equals(PARQUET_SCHEMA, check_metadata=True):
        raise NativeObservationCacheError("observation Parquet schema drifted")
    digest = hashlib.sha256()
    digest.update(DIGEST_ALGORITHM.encode("ascii") + b"\x00")
    digest.update(PARQUET_SCHEMA_SHA256.encode("ascii") + b"\x00")
    count = 0
    first_left_ns = 0
    last_right_ns = 0
    previous_right_ns: int | None = None
    for batch in parquet.iter_batches(batch_size=65_536, columns=list(ALL_COLUMNS)):
        columns = batch.to_pydict()
        for offset in range(batch.num_rows):
            row = {name: columns[name][offset] for name in ALL_COLUMNS}
            _validate_observation_order(
                row,
                previous_right_ns=previous_right_ns,
                formal_exchange_day=False,
            )
            if count == 0:
                first_left_ns = int(row["left_ts_ns"])
            previous_right_ns = int(row["right_ts_ns"])
            last_right_ns = previous_right_ns
            _update_row_digest(digest, row)
            count += 1
    return digest.hexdigest(), count, first_left_ns, last_right_ns


def validate_admitted_cache(
    output_root: Path,
    day: str,
    *,
    deep: bool = True,
) -> CacheValidation:
    """Fail closed unless one atomic cache day and all hashes are valid."""

    _parse_day(day)
    day_root = Path(output_root).expanduser().resolve() / day
    manifest_path = day_root / MANIFEST_NAME
    parquet_path = day_root / PARQUET_NAME
    success_path = day_root / SUCCESS_NAME
    if not day_root.is_dir() or not manifest_path.is_file() or not parquet_path.is_file():
        raise NativeObservationCacheError(f"cache day is incomplete: {day_root}")
    if not success_path.is_file():
        raise NativeObservationCacheError("cache success marker is missing")
    manifest = _load_json(manifest_path)
    if manifest.get("identity") != IDENTITY or manifest.get("utc_day") != day:
        raise NativeObservationCacheError("cache manifest identity/day drifted")
    expected_canonical = canonical_sha256(_manifest_without_self_hash(manifest))
    if manifest.get("canonical_manifest_sha256") != expected_canonical:
        raise NativeObservationCacheError("cache canonical manifest hash drifted")
    if success_path.read_text(encoding="ascii").strip() != sha256_file(manifest_path):
        raise NativeObservationCacheError("cache success marker does not bind manifest file")
    parquet_binding = manifest.get("parquet")
    if not isinstance(parquet_binding, dict):
        raise NativeObservationCacheError("cache Parquet binding is absent")
    if parquet_binding.get("schema_sha256") != PARQUET_SCHEMA_SHA256:
        raise NativeObservationCacheError("cache Parquet schema identity drifted")
    if sha256_file(parquet_path) != parquet_binding.get("sha256"):
        raise NativeObservationCacheError("cache Parquet file hash drifted")
    expected_count = int(manifest["observation_count"])
    expected_digest = str(manifest["cache_readback_observation_sha256"])
    if deep:
        digest, count, first_left, last_right = _parquet_observation_digest(parquet_path)
        if (
            digest != expected_digest
            or count != expected_count
            or first_left != int(manifest["first_left_ts_ns"])
            or last_right != int(manifest["last_right_ts_ns"])
        ):
            raise NativeObservationCacheError("cache row digest/count/bounds drifted")
    else:
        count = int(pq.ParquetFile(parquet_path).metadata.num_rows)
        digest = expected_digest
        if count != expected_count:
            raise NativeObservationCacheError("cache Parquet row count drifted")
    return CacheValidation(
        day_root=day_root,
        manifest=MappingProxyType(manifest),
        observation_count=count,
        observation_sha256=digest,
    )


def open_admitted_observation_cache(
    output_root: Path,
    day: str,
    *,
    deep: bool = False,
) -> AdmittedNativeObservationCache:
    validation = validate_admitted_cache(output_root, day, deep=deep)
    return AdmittedNativeObservationCache(
        day_root=validation.day_root,
        manifest=validation.manifest,
    )


def iter_cached_observations(
    output_root: Path,
    day: str,
    *,
    batch_size: int = 65_536,
) -> Iterator[CausalWindowObservation]:
    """Read one admitted cache as the original observation interface."""

    if batch_size <= 0:
        raise NativeObservationCacheError("batch_size must be positive")
    validation = validate_admitted_cache(output_root, day, deep=False)
    parquet = pq.ParquetFile(validation.day_root / PARQUET_NAME)
    digest = hashlib.sha256()
    digest.update(DIGEST_ALGORITHM.encode("ascii") + b"\x00")
    digest.update(PARQUET_SCHEMA_SHA256.encode("ascii") + b"\x00")
    count = 0
    previous_right_ns: int | None = None
    for batch in parquet.iter_batches(batch_size=batch_size, columns=list(ALL_COLUMNS)):
        columns = batch.to_pydict()
        for offset in range(batch.num_rows):
            row = {name: columns[name][offset] for name in ALL_COLUMNS}
            _validate_observation_order(
                row,
                previous_right_ns=previous_right_ns,
                formal_exchange_day=False,
            )
            previous_right_ns = int(row["right_ts_ns"])
            _update_row_digest(digest, row)
            count += 1
            yield _row_observation(row)
    if count != validation.observation_count:
        raise NativeObservationCacheError("cache reader row count drifted")
    if digest.hexdigest() != validation.observation_sha256:
        raise NativeObservationCacheError("cache reader row digest drifted")


def iter_cached_observations_between(
    output_root: Path,
    day: str,
    *,
    start_feature_ready_ts_ns: int,
    end_feature_ready_ts_ns: int,
    batch_size: int = 65_536,
) -> Iterator[CausalWindowObservation]:
    """Read one admitted cache over a half-open feature-ready interval.

    Atomic admission already binds the complete Parquet SHA256. Row-group
    statistics only prune I/O; every returned row is still range-checked and
    order-validated before it reaches the runtime feature stream.
    """

    if batch_size <= 0:
        raise NativeObservationCacheError("batch_size must be positive")
    start_ns = int(start_feature_ready_ts_ns)
    end_ns = int(end_feature_ready_ts_ns)
    if start_ns < 0 or end_ns <= start_ns:
        raise NativeObservationCacheError("cache observation interval is invalid")
    validation = validate_admitted_cache(output_root, day, deep=False)
    parquet = pq.ParquetFile(validation.day_root / PARQUET_NAME)
    ready_index = parquet.schema_arrow.get_field_index("feature_ready_ts_ns")
    if ready_index < 0:
        raise NativeObservationCacheError("cache lacks feature-ready clock")
    row_groups: list[int] = []
    for index in range(parquet.metadata.num_row_groups):
        statistics = parquet.metadata.row_group(index).column(ready_index).statistics
        if statistics is None or not statistics.has_min_max:
            row_groups.append(index)
            continue
        if int(statistics.max) >= start_ns and int(statistics.min) < end_ns:
            row_groups.append(index)
    previous_right_ns: int | None = None
    for batch in parquet.iter_batches(
        batch_size=batch_size,
        row_groups=row_groups,
        columns=list(ALL_COLUMNS),
    ):
        columns = batch.to_pydict()
        for offset in range(batch.num_rows):
            ready_ns = int(columns["feature_ready_ts_ns"][offset])
            if ready_ns < start_ns:
                continue
            if ready_ns >= end_ns:
                break
            row = {name: columns[name][offset] for name in ALL_COLUMNS}
            _validate_observation_order(
                row,
                previous_right_ns=previous_right_ns,
                formal_exchange_day=False,
            )
            previous_right_ns = int(row["right_ts_ns"])
            yield _row_observation(row)


def _individual_trade_paths(root: Path, symbol: str, days: Sequence[str]) -> tuple[Path, ...]:
    trade_root = Path(root).expanduser().resolve() / symbol
    output: list[Path] = []
    for day in days:
        candidates = (
            trade_root / f"{symbol}-trades-{day}.csv",
            trade_root / f"{symbol}-trades-{day}.csv.gz",
        )
        found = tuple(path for path in candidates if path.is_file())
        if len(found) != 1:
            raise NativeObservationCacheError(
                f"official individual trades require exactly one source for {day}: {found}"
            )
        output.append(found[0])
    return tuple(output)


def _load_individual_trade_paths(
    paths: Sequence[Path],
    *,
    symbol: str,
    allowed_days: Sequence[str],
) -> pd.DataFrame:
    frames = [bt._read_individual_trade_csv(path) for path in paths]
    if not frames:
        raise NativeObservationCacheError("official individual trade path set is empty")
    frame = frames[0] if len(frames) == 1 else pd.concat(frames, ignore_index=True, copy=False)
    keep = bt.allowed_timestamp_mask(
        frame["transact_time"].to_numpy(copy=False),
        symbol,
        label="trade",
        explicitly_allowed_days=tuple(allowed_days),
    )
    if not keep.all():
        frame = frame.loc[keep].copy()
    if frame.empty:
        raise NativeObservationCacheError("official individual trade panel is empty")
    if not frame["transact_time"].is_monotonic_increasing:
        frame.sort_values("transact_time", kind="stable", inplace=True)
    frame.reset_index(drop=True, inplace=True)
    return frame


def _real_day_source_binding(
    *,
    day: str,
    raw_native_root: Path,
    native_book_cache: Path,
    individual_trade_root: Path,
    symbol: str,
    book_audit: native.NativeM2BookFeatureAccumulator | None = None,
    load_trades: bool = True,
) -> tuple[
    native.RawNativeM2BookFeatureStream,
    pd.DataFrame | None,
    dict[str, Any],
]:
    parsed = _parse_day(day)
    previous_day = (parsed - timedelta(days=1)).isoformat()
    trade_days = (previous_day, day)
    trade_paths = _individual_trade_paths(individual_trade_root, symbol, trade_days)
    book_stream = native.open_cryptohft_native_m2_book_feature_stream(
        raw_root=Path(raw_native_root),
        day=day,
        symbol=symbol,
        cache_dir=Path(native_book_cache),
        cache_read_only=True,
        require_receive_clock=True,
        feature_ready_clock=native.EXCHANGE_WINDOW_READY_CLOCK,
        audit=book_audit,
    )
    tape_identity = book_stream.tape.identity(include_sha256=True)
    trades = (
        _load_individual_trade_paths(
            trade_paths,
            symbol=symbol,
            allowed_days=trade_days,
        )
        if load_trades
        else None
    )
    binding = {
        "raw_native_tape_identity": tape_identity,
        "official_individual_trades": [
            {
                "utc_day": trade_day,
                "path": str(path.resolve()),
                "sha256": sha256_file(path),
                "size_bytes": path.stat().st_size,
            }
            for trade_day, path in zip(trade_days, trade_paths, strict=True)
        ],
        "symbol": symbol,
        "feature_ready_clock": native.EXCHANGE_WINDOW_READY_CLOCK,
        "receive_time_transport_authority": False,
    }
    return book_stream, trades, binding


def preflight_real_day(
    *,
    day: str,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    raw_native_root: Path = DEFAULT_RAW_NATIVE_ROOT,
    native_book_cache: Path = DEFAULT_NATIVE_BOOK_CACHE,
    individual_trade_root: Path = DEFAULT_INDIVIDUAL_TRADE_ROOT,
    symbol: str = "BTCUSDC",
) -> dict[str, Any]:
    """Resolve source identities without streaming target-day observations."""

    root = Path(output_root).expanduser().resolve()
    destination = root / day
    book_stream, trades, source_binding = _real_day_source_binding(
        day=day,
        raw_native_root=raw_native_root,
        native_book_cache=native_book_cache,
        individual_trade_root=individual_trade_root,
        symbol=symbol,
        load_trades=False,
    )
    contract = book_stream.contract
    day_start_ns = _day_start_ns(day)
    eligible = bool(
        contract.window_start_ns == day_start_ns - DAY_NS
        and contract.window_end_ns == day_start_ns + DAY_NS
        and contract.policy_start_ns == day_start_ns
        and contract.window_width_ns == BASE_WINDOW_WIDTH_NS
        and contract.warmup_hours == 24
        and contract.boundary == "left_closed_right_open"
        and contract.partial_window_policy == "exclude"
        and contract.feature_ready_clock == native.EXCHANGE_WINDOW_READY_CLOCK
        and contract.require_receive_clock
    )
    return {
        "identity": IDENTITY,
        "utc_day": day,
        "eligible": eligible,
        "destination": str(destination),
        "destination_exists": destination.exists(),
        "stale_staging": [str(path) for path in sorted(root.glob(f".{day}.staging-*"))],
        "source_binding": source_binding,
        "source_binding_sha256": canonical_sha256(source_binding),
        "official_trade_row_count": None if trades is None else int(len(trades)),
        "official_trade_rows_scanned": trades is not None,
        "contract": asdict(contract),
        "formal_expected_window_count": EXPECTED_FORMAL_WINDOW_COUNT,
        "economic_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_policy_authorized": False,
    }


def build_real_day_cache(
    *,
    day: str,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    raw_native_root: Path = DEFAULT_RAW_NATIVE_ROOT,
    native_book_cache: Path = DEFAULT_NATIVE_BOOK_CACHE,
    individual_trade_root: Path = DEFAULT_INDIVIDUAL_TRADE_ROOT,
    symbol: str = "BTCUSDC",
    batch_size: int = 16_384,
) -> dict[str, Any]:
    """Build one formal D-1 plus target-day cache from frozen sources."""

    book_audit = native.NativeM2BookFeatureAccumulator()
    trade_audit = native.NativeM2TradeMergeAccumulator()
    book_stream, trades, source_binding = _real_day_source_binding(
        day=day,
        raw_native_root=raw_native_root,
        native_book_cache=native_book_cache,
        individual_trade_root=individual_trade_root,
        symbol=symbol,
        book_audit=book_audit,
        load_trades=True,
    )
    if trades is None:
        raise NativeObservationCacheError("formal build did not load official trades")
    observations = stream_indexed_native_m2_causal_observations(
        book_windows=book_stream,
        official_trades=trades,
        audit=trade_audit,
    )
    return materialize_observation_cache(
        day=day,
        observations=observations,
        output_root=output_root,
        source_binding=source_binding,
        book_audit=book_audit,
        trade_audit=trade_audit,
        formal_exchange_day=True,
        batch_size=batch_size,
    )


def cache_contract() -> dict[str, Any]:
    return {
        "identity": IDENTITY,
        "schema_version": SCHEMA_VERSION,
        "manifest_schema_version": MANIFEST_SCHEMA_VERSION,
        "parquet_schema_sha256": PARQUET_SCHEMA_SHA256,
        "columns": list(ALL_COLUMNS),
        "source_semantics": {
            "warmup": "D-1 natural UTC day, 24h",
            "window": "100ms [left,right), partial excluded",
            "feature_ready_clock": native.EXCHANGE_WINDOW_READY_CLOCK,
            "book": "raw CryptoHFT snapshot/delta through authoritative scheduler",
            "trades": "official Binance Futures BTCUSDC individual trades",
            "gap_stale_missing": "preserved fail closed",
        },
        "materialization": {
            "book": "sequential once; exact batching intentionally rejected",
            "trades": "exact monotone cursor in original row order",
            "admission": "Parquet round-trip row digest plus atomic directory rename",
            "reuse_api": "open_admitted_observation_cache",
        },
        "economic_outcomes_read": False,
        "action_authorized": False,
        "live_policy_authorized": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("contract")
    for command in ("preflight", "build", "validate"):
        child = subparsers.add_parser(command)
        child.add_argument("--day", required=True)
        child.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
        if command in {"preflight", "build"}:
            child.add_argument("--raw-native-root", type=Path, default=DEFAULT_RAW_NATIVE_ROOT)
            child.add_argument("--native-book-cache", type=Path, default=DEFAULT_NATIVE_BOOK_CACHE)
            child.add_argument(
                "--individual-trade-root",
                type=Path,
                default=DEFAULT_INDIVIDUAL_TRADE_ROOT,
            )
            child.add_argument("--symbol", default="BTCUSDC")
        if command == "build":
            child.add_argument("--batch-size", type=int, default=16_384)
        if command == "validate":
            child.add_argument("--shallow", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "contract":
        payload = cache_contract()
    elif args.command == "preflight":
        payload = preflight_real_day(
            day=args.day,
            output_root=args.output_root,
            raw_native_root=args.raw_native_root,
            native_book_cache=args.native_book_cache,
            individual_trade_root=args.individual_trade_root,
            symbol=args.symbol,
        )
    elif args.command == "build":
        payload = build_real_day_cache(
            day=args.day,
            output_root=args.output_root,
            raw_native_root=args.raw_native_root,
            native_book_cache=args.native_book_cache,
            individual_trade_root=args.individual_trade_root,
            symbol=args.symbol,
            batch_size=args.batch_size,
        )
    else:
        validation = validate_admitted_cache(
            args.output_root,
            args.day,
            deep=not args.shallow,
        )
        payload = {
            "utc_day": args.day,
            "valid": True,
            "day_root": str(validation.day_root),
            "observation_count": validation.observation_count,
            "observation_sha256": validation.observation_sha256,
        }
    print(json.dumps(payload, sort_keys=True, allow_nan=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
