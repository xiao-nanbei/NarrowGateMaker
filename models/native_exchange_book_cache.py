"""Persistent, strategy-independent native order-book message cache."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import uuid
from collections.abc import Callable, Iterable, Iterator
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from models.cache_tier_lru import record_cache_access, register_cache_write
from models.replay_cache_dag import REPLAY_WINDOW_CACHE_GRAPH_IDENTITY
from models.tick_data_types import HistoricalExchangeBookEvent

CACHE_SCHEMA_VERSION = "narrowgate.native_exchange_book_hour_cache.v1"
EVENT_SCHEMA_VERSION = "historical_exchange_book_event.v1"
PRODUCER_MAPPING_VERSION = "cryptohft_logical_message_mapping.v1"
_BATCH_EVENTS = 25_000


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def native_book_parser_identity() -> str:
    root = Path(__file__).resolve().parent.parent
    payload = {
        "producer_mapping_version": PRODUCER_MAPPING_VERSION,
        "event_schema_version": EVENT_SCHEMA_VERSION,
        "raw_parser_sha256": _sha256(
            root / "data" / "build_active_order_queue_tape.py"
        ),
        "event_type_sha256": _sha256(root / "models" / "tick_data_types.py"),
    }
    return _canonical_sha256(payload)


@dataclass(frozen=True)
class NativeBookHourCacheArtifact:
    data_path: Path
    manifest_path: Path
    identity_sha256: str
    event_count: int
    level_count: int
    cache_hit: bool


def native_book_hour_identity(
    *,
    source_path: Path,
    symbol: str,
    exchange: str,
    market_id: str,
    tick_size: float,
    parser_identity_sha256: str,
) -> dict[str, Any]:
    source = Path(source_path).expanduser().resolve()
    stat = source.stat()
    return {
        "schema_version": CACHE_SCHEMA_VERSION,
        "dag_identity_sha256": REPLAY_WINDOW_CACHE_GRAPH_IDENTITY,
        "dag_node": "native_orderbook_logical_hour",
        "source_path": str(source),
        "source_size": int(stat.st_size),
        "source_mtime_ns": int(stat.st_mtime_ns),
        "symbol": str(symbol).upper(),
        "exchange": str(exchange),
        "market_id": str(market_id),
        "tick_size": float(tick_size),
        "parser_identity_sha256": str(parser_identity_sha256),
        "event_schema_version": EVENT_SCHEMA_VERSION,
        "exchange_clock": "transaction_with_event_then_receive_fallback",
        "receive_clock": "provider_receive_time_preserved",
    }


def native_book_hour_cache_paths(
    cache_root: Path,
    identity: dict[str, Any],
) -> tuple[Path, Path, Path, str]:
    digest = _canonical_sha256(identity)
    source = Path(str(identity["source_path"]))
    day = source.parent.parent.name
    hour = source.parent.name
    directory = (
        Path(cache_root).expanduser().resolve()
        / str(identity["exchange"])
        / str(identity["symbol"])
        / day
        / hour
    )
    stem = f"logical_messages_{digest[:20]}"
    data_path = directory / f"{stem}.parquet"
    manifest_path = directory / f"{stem}.manifest.json"
    lock_path = directory / f"{stem}.lock"
    return data_path, manifest_path, lock_path, digest


def _logical_native_book_data_path(
    cache_root: Path,
    identity: dict[str, Any],
    identity_sha256: str,
) -> Path:
    source = Path(str(identity["source_path"]))
    directory = (
        Path(cache_root).expanduser().absolute()
        / str(identity["exchange"])
        / str(identity["symbol"])
        / source.parent.parent.name
        / source.parent.name
    )
    return directory / f"logical_messages_{identity_sha256[:20]}.parquet"


def _record_native_book_hit(path: Path, *, identity_sha256: str) -> None:
    with suppress(Exception):
        record_cache_access(path, identity_sha256=identity_sha256)


def _register_native_book_write(path: Path, *, identity_sha256: str) -> None:
    with suppress(Exception):
        register_cache_write(path, identity_sha256=identity_sha256)


def _read_valid_manifest(
    *,
    data_path: Path,
    manifest_path: Path,
    identity_sha256: str,
) -> dict[str, Any] | None:
    if not data_path.is_file() or not manifest_path.is_file():
        return None
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("schema_version") != CACHE_SCHEMA_VERSION:
            return None
        if manifest.get("identity_sha256") != identity_sha256:
            return None
        if int(manifest.get("data_size_bytes", -1)) != data_path.stat().st_size:
            return None
        import pyarrow.parquet as pq

        parquet = pq.ParquetFile(data_path)
        if int(parquet.metadata.num_rows) != int(manifest["event_count"]):
            return None
        return manifest
    except Exception:
        return None


def _arrow_schema():
    import pyarrow as pa

    return pa.schema(
        [
            pa.field("market_id", pa.string(), nullable=False),
            pa.field("event_type", pa.string(), nullable=False),
            pa.field("exchange_ts_ns", pa.int64(), nullable=False),
            pa.field("exchange_ts_source", pa.string(), nullable=False),
            pa.field("local_receive_ts_ns", pa.int64(), nullable=False),
            pa.field("event_time_ns", pa.int64(), nullable=False),
            pa.field("transaction_time_ns", pa.int64(), nullable=False),
            pa.field("first_update_id", pa.int64()),
            pa.field("final_update_id", pa.int64()),
            pa.field("previous_final_update_id", pa.int64()),
            pa.field("last_update_id", pa.int64()),
            pa.field("level_sides", pa.list_(pa.int8()), nullable=False),
            pa.field("level_ticks", pa.list_(pa.int64()), nullable=False),
            pa.field("level_quantities", pa.list_(pa.float64()), nullable=False),
            pa.field("source", pa.string(), nullable=False),
            pa.field("source_ordinal", pa.int64(), nullable=False),
        ]
    )


def _events_to_table(events: list[HistoricalExchangeBookEvent]):
    import pyarrow as pa

    columns: dict[str, list[Any]] = {
        field.name: [] for field in _arrow_schema()
    }
    for event in events:
        columns["market_id"].append(event.market_id)
        columns["event_type"].append(event.event_type)
        columns["exchange_ts_ns"].append(int(event.exchange_ts_ns))
        columns["exchange_ts_source"].append(event.exchange_ts_source)
        columns["local_receive_ts_ns"].append(int(event.local_receive_ts_ns))
        columns["event_time_ns"].append(int(event.event_time_ns))
        columns["transaction_time_ns"].append(int(event.transaction_time_ns))
        columns["first_update_id"].append(event.first_update_id)
        columns["final_update_id"].append(event.final_update_id)
        columns["previous_final_update_id"].append(
            event.previous_final_update_id
        )
        columns["last_update_id"].append(event.last_update_id)
        columns["level_sides"].append(
            [0 if side == "bid" else 1 for side, _, _ in event.levels]
        )
        columns["level_ticks"].append(
            [int(tick) for _, tick, _ in event.levels]
        )
        columns["level_quantities"].append(
            [float(quantity) for _, _, quantity in event.levels]
        )
        columns["source"].append(event.source)
        columns["source_ordinal"].append(int(event.source_ordinal))
    return pa.Table.from_pydict(columns, schema=_arrow_schema())


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _write_cache(
    *,
    data_path: Path,
    manifest_path: Path,
    identity: dict[str, Any],
    identity_sha256: str,
    events: Iterable[HistoricalExchangeBookEvent],
) -> NativeBookHourCacheArtifact:
    import pyarrow.parquet as pq

    temporary = data_path.with_name(
        f".{data_path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )
    writer = None
    event_count = 0
    level_count = 0
    first_exchange_ts_ns = 0
    last_exchange_ts_ns = 0
    batch: list[HistoricalExchangeBookEvent] = []
    try:
        writer = pq.ParquetWriter(
            temporary,
            _arrow_schema(),
            compression="zstd",
            compression_level=6,
            use_dictionary=True,
            write_statistics=True,
        )
        for event in events:
            if event.event_type == "source_gap":
                raise ValueError("source_gap cannot be materialized as a source hour")
            event_count += 1
            level_count += len(event.levels)
            if first_exchange_ts_ns == 0:
                first_exchange_ts_ns = int(event.exchange_ts_ns)
            last_exchange_ts_ns = int(event.exchange_ts_ns)
            batch.append(event)
            if len(batch) >= _BATCH_EVENTS:
                writer.write_table(_events_to_table(batch))
                batch.clear()
        if batch:
            writer.write_table(_events_to_table(batch))
        writer.close()
        writer = None
        os.replace(temporary, data_path)
        manifest = {
            "schema_version": CACHE_SCHEMA_VERSION,
            "identity": identity,
            "identity_sha256": identity_sha256,
            "event_count": event_count,
            "level_count": level_count,
            "first_exchange_ts_ns": first_exchange_ts_ns,
            "last_exchange_ts_ns": last_exchange_ts_ns,
            "data_path": str(data_path),
            "data_size_bytes": data_path.stat().st_size,
            "data_sha256": _sha256(data_path),
        }
        _atomic_json(manifest_path, manifest)
        return NativeBookHourCacheArtifact(
            data_path=data_path,
            manifest_path=manifest_path,
            identity_sha256=identity_sha256,
            event_count=event_count,
            level_count=level_count,
            cache_hit=False,
        )
    finally:
        if writer is not None:
            writer.close()
        temporary.unlink(missing_ok=True)


def ensure_native_book_hour_cache(
    *,
    cache_root: Path,
    identity: dict[str, Any],
    events_factory: Callable[[], Iterable[HistoricalExchangeBookEvent]],
    refresh: bool = False,
) -> NativeBookHourCacheArtifact:
    data_path, manifest_path, lock_path, digest = native_book_hour_cache_paths(
        cache_root,
        identity,
    )
    logical_data_path = _logical_native_book_data_path(cache_root, identity, digest)
    data_path.parent.mkdir(parents=True, exist_ok=True)
    if not refresh:
        manifest = _read_valid_manifest(
            data_path=data_path,
            manifest_path=manifest_path,
            identity_sha256=digest,
        )
        if manifest is not None:
            _record_native_book_hit(logical_data_path, identity_sha256=digest)
            return NativeBookHourCacheArtifact(
                data_path=data_path,
                manifest_path=manifest_path,
                identity_sha256=digest,
                event_count=int(manifest["event_count"]),
                level_count=int(manifest["level_count"]),
                cache_hit=True,
            )

    with lock_path.open("a+b") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        if not refresh:
            manifest = _read_valid_manifest(
                data_path=data_path,
                manifest_path=manifest_path,
                identity_sha256=digest,
            )
            if manifest is not None:
                _record_native_book_hit(logical_data_path, identity_sha256=digest)
                return NativeBookHourCacheArtifact(
                    data_path=data_path,
                    manifest_path=manifest_path,
                    identity_sha256=digest,
                    event_count=int(manifest["event_count"]),
                    level_count=int(manifest["level_count"]),
                    cache_hit=True,
                )
        artifact = _write_cache(
            data_path=data_path,
            manifest_path=manifest_path,
            identity=identity,
            identity_sha256=digest,
            events=events_factory(),
        )
        _register_native_book_write(logical_data_path, identity_sha256=digest)
        return artifact


def require_native_book_hour_cache(
    *,
    cache_root: Path,
    identity: dict[str, Any],
    verify_sha256: bool = False,
) -> NativeBookHourCacheArtifact:
    """Open one already-materialized hour without parsing or writing.

    Strict policy forks use this entry point after a single-owner prebuild.  A
    missing, stale, or corrupt artifact is an execution error; silently
    reparsing the raw source inside a fork would mix cache construction with
    the treatment path.
    """

    data_path, manifest_path, _, digest = native_book_hour_cache_paths(
        cache_root,
        identity,
    )
    manifest = _read_valid_manifest(
        data_path=data_path,
        manifest_path=manifest_path,
        identity_sha256=digest,
    )
    if manifest is None:
        raise FileNotFoundError(
            "native book hour cache is absent or invalid for "
            f"{identity['source_path']}"
        )
    if manifest.get("identity") != identity:
        raise ValueError(
            "native book hour cache manifest identity does not match its "
            "requested source identity"
        )
    expected_data_sha256 = str(manifest.get("data_sha256", ""))
    if verify_sha256:
        if len(expected_data_sha256) != 64:
            raise ValueError("native book hour cache lacks a data SHA256")
        if _sha256(data_path) != expected_data_sha256:
            raise ValueError(
                f"native book hour cache SHA256 drifted: {data_path}"
            )
    logical_data_path = _logical_native_book_data_path(
        cache_root,
        identity,
        digest,
    )
    _record_native_book_hit(logical_data_path, identity_sha256=digest)
    return NativeBookHourCacheArtifact(
        data_path=data_path,
        manifest_path=manifest_path,
        identity_sha256=digest,
        event_count=int(manifest["event_count"]),
        level_count=int(manifest["level_count"]),
        cache_hit=True,
    )


def iter_native_book_hour_cache(
    artifact: NativeBookHourCacheArtifact,
    *,
    batch_size: int = 65_536,
) -> Iterator[HistoricalExchangeBookEvent]:
    import pyarrow.parquet as pq

    parquet = pq.ParquetFile(artifact.data_path)
    yielded = 0
    for batch in parquet.iter_batches(batch_size=batch_size):
        columns = batch.to_pydict()
        for index in range(batch.num_rows):
            sides = columns["level_sides"][index]
            ticks = columns["level_ticks"][index]
            quantities = columns["level_quantities"][index]
            levels = tuple(
                (
                    "bid" if int(side) == 0 else "ask",
                    int(tick),
                    float(quantity),
                )
                for side, tick, quantity in zip(
                    sides,
                    ticks,
                    quantities,
                    strict=True,
                )
            )
            yielded += 1
            yield HistoricalExchangeBookEvent(
                market_id=str(columns["market_id"][index]),
                event_type=str(columns["event_type"][index]),
                exchange_ts_ns=int(columns["exchange_ts_ns"][index]),
                exchange_ts_source=str(
                    columns["exchange_ts_source"][index]
                ),
                local_receive_ts_ns=int(
                    columns["local_receive_ts_ns"][index]
                ),
                event_time_ns=int(columns["event_time_ns"][index]),
                transaction_time_ns=int(
                    columns["transaction_time_ns"][index]
                ),
                first_update_id=columns["first_update_id"][index],
                final_update_id=columns["final_update_id"][index],
                previous_final_update_id=columns[
                    "previous_final_update_id"
                ][index],
                last_update_id=columns["last_update_id"][index],
                levels=levels,
                source=str(columns["source"][index]),
                source_ordinal=int(columns["source_ordinal"][index]),
            )
    if yielded != artifact.event_count:
        raise ValueError(
            "native book cache row count changed while reading: "
            f"{yielded} != {artifact.event_count}"
        )
