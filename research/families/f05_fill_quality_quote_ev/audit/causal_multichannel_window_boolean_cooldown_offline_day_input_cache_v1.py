"""Read-only, hash-bound day input cache for offline F05 replay.

The cache stores immutable NumPy arrays for one replay day.  It is deliberately
independent of the formal replay adapter: callers may admit or open inputs, but
this module never runs a replay, reads an outcome, or changes a live runtime.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import shutil
import tempfile
import uuid
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import Any

import numpy as np
import pandas as pd

from models import cache_tier_lru
from models.tick_data_types import HistoricalBBOData, HistoricalL2Data

CACHE_IDENTITY = "causal_multichannel_window_boolean_cooldown_offline_day_input_cache_v1"
CACHE_SCHEMA_VERSION = f"{CACHE_IDENTITY}.cache.v1"
ADMISSION_SCHEMA_VERSION = f"{CACHE_IDENTITY}.admission.v1"
SOURCE_COMPONENTS = ("trades", "bbo", "l2", "ml_overlay")
COMPONENTS = (*SOURCE_COMPONENTS, "derived")
TRADES_COLUMNS = (
    "trade_id",
    "price",
    "quantity",
    "transact_time",
    "is_buyer_maker",
)
BBO_COLUMNS = ("ts_ms", "best_bid", "best_ask", "bid_qty", "ask_qty")
L2_COLUMNS = ("ts_ms", "bid_px", "bid_qty", "ask_px", "ask_qty")
DERIVED_COLUMNS = ("var_ts_ms", "var_ssq", "var_ti", "var_retsq")

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_UTC_DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ARRAY_CHUNK_BYTES = 8 * 1024 * 1024


class OfflineDayInputCacheError(RuntimeError):
    """Raised when cache identity, content, or admission fails closed."""


def _canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    except (TypeError, ValueError) as exc:
        raise OfflineDayInputCacheError("cache identity is not canonical JSON") from exc


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _identity_value(value: Any) -> Any:
    if isinstance(value, np.ndarray):
        return [_identity_value(item) for item in value.tolist()]
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, Mapping):
        normalized = {str(key): _identity_value(item) for key, item in value.items()}
        if len(normalized) != len(value):
            raise OfflineDayInputCacheError("parameter keys collide after normalization")
        return normalized
    if isinstance(value, (list, tuple)):
        return [_identity_value(item) for item in value]
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    raise OfflineDayInputCacheError(f"unsupported parameter identity value: {type(value).__name__}")


def _document_sha256(payload: Mapping[str, Any], field: str) -> str:
    return _canonical_sha256({key: value for key, value in payload.items() if key != field})


def _admission_receipt_sha256(payload: Mapping[str, Any]) -> str:
    body = dict(payload)
    body.pop("admission_receipt_sha256", None)
    binding = body.get("binding")
    if isinstance(binding, Mapping):
        body["binding"] = dict(binding)
        body["binding"].pop("admission_receipt_sha256", None)
    return _canonical_sha256(body)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _require_sha256(value: str, *, label: str) -> str:
    normalized = str(value).strip().lower()
    if not _SHA256_RE.fullmatch(normalized):
        raise OfflineDayInputCacheError(f"{label} must be a lowercase SHA256")
    return normalized


def _require_label(value: str, *, label: str) -> str:
    normalized = str(value).strip()
    if not normalized or any(ord(character) < 32 for character in normalized):
        raise OfflineDayInputCacheError(f"{label} must be a non-empty printable label")
    return normalized


def _normalize_day(value: str) -> str:
    normalized = str(value).strip()
    if not _UTC_DAY_RE.fullmatch(normalized):
        raise OfflineDayInputCacheError("utc_day must use YYYY-MM-DD")
    try:
        date.fromisoformat(normalized)
    except ValueError as exc:
        raise OfflineDayInputCacheError("utc_day is not a valid calendar date") from exc
    return normalized


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(_canonical_bytes(payload))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _read_json(path: Path, *, label: str) -> dict[str, Any]:
    if path.is_symlink():
        raise OfflineDayInputCacheError(f"{label} cannot be a symlink")
    try:
        payload = json.loads(path.read_text(encoding="ascii"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise OfflineDayInputCacheError(f"cannot read {label}") from exc
    if not isinstance(payload, dict):
        raise OfflineDayInputCacheError(f"{label} must be a JSON object")
    return payload


@contextmanager
def _exclusive_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@dataclass(frozen=True, slots=True)
class DayInputCacheIdentity:
    """Immutable source, clock, and engine identity for one UTC target day."""

    utc_day: str
    continuation_day: str
    market_id: str
    source_receipts: tuple[tuple[str, str], ...]
    clock_identity: str
    clock_identity_sha256: str
    engine_identity: str
    engine_identity_sha256: str
    market_window_identity_sha256: str
    model_overlay_identity_sha256: str
    latency_identity_sha256: str
    queue_random_identity_sha256: str
    replay_input_receipt_sha256: str
    params_identity_sha256: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "utc_day", _normalize_day(self.utc_day))
        object.__setattr__(self, "continuation_day", _normalize_day(self.continuation_day))
        expected_continuation = (date.fromisoformat(self.utc_day) + timedelta(days=1)).isoformat()
        if self.continuation_day != expected_continuation:
            raise OfflineDayInputCacheError("continuation_day must be the natural D+1 UTC day")
        object.__setattr__(self, "market_id", _require_label(self.market_id, label="market_id"))
        object.__setattr__(
            self,
            "clock_identity",
            _require_label(self.clock_identity, label="clock_identity"),
        )
        object.__setattr__(
            self,
            "engine_identity",
            _require_label(self.engine_identity, label="engine_identity"),
        )
        object.__setattr__(
            self,
            "clock_identity_sha256",
            _require_sha256(self.clock_identity_sha256, label="clock_identity_sha256"),
        )
        object.__setattr__(
            self,
            "engine_identity_sha256",
            _require_sha256(self.engine_identity_sha256, label="engine_identity_sha256"),
        )
        for field in (
            "market_window_identity_sha256",
            "model_overlay_identity_sha256",
            "latency_identity_sha256",
            "queue_random_identity_sha256",
            "replay_input_receipt_sha256",
            "params_identity_sha256",
        ):
            object.__setattr__(
                self,
                field,
                _require_sha256(getattr(self, field), label=field),
            )
        normalized_receipts = tuple(
            sorted(
                (
                    _require_label(component, label="source receipt component"),
                    _require_sha256(digest, label=f"source receipt {component}"),
                )
                for component, digest in self.source_receipts
            )
        )
        receipt_components = tuple(component for component, _ in normalized_receipts)
        if len(receipt_components) != len(SOURCE_COMPONENTS) or set(receipt_components) != set(
            SOURCE_COMPONENTS
        ):
            raise OfflineDayInputCacheError(
                "source_receipts must bind exactly trades, bbo, l2, and ml_overlay"
            )
        object.__setattr__(self, "source_receipts", normalized_receipts)

    @classmethod
    def create(
        cls,
        *,
        utc_day: str,
        continuation_day: str,
        market_id: str,
        source_receipts: Mapping[str, str],
        clock_identity: str,
        clock_identity_sha256: str,
        engine_identity: str,
        engine_identity_sha256: str,
        market_window_identity_sha256: str,
        model_overlay_identity_sha256: str,
        latency_identity_sha256: str,
        queue_random_identity_sha256: str,
        replay_input_receipt_sha256: str,
        params_identity_sha256: str,
    ) -> DayInputCacheIdentity:
        return cls(
            utc_day=utc_day,
            continuation_day=continuation_day,
            market_id=market_id,
            source_receipts=tuple(source_receipts.items()),
            clock_identity=clock_identity,
            clock_identity_sha256=clock_identity_sha256,
            engine_identity=engine_identity,
            engine_identity_sha256=engine_identity_sha256,
            market_window_identity_sha256=market_window_identity_sha256,
            model_overlay_identity_sha256=model_overlay_identity_sha256,
            latency_identity_sha256=latency_identity_sha256,
            queue_random_identity_sha256=queue_random_identity_sha256,
            replay_input_receipt_sha256=replay_input_receipt_sha256,
            params_identity_sha256=params_identity_sha256,
        )

    def payload(self) -> dict[str, Any]:
        return {
            "utc_day": self.utc_day,
            "continuation_day": self.continuation_day,
            "market_id": self.market_id,
            "source_receipts": dict(self.source_receipts),
            "clock_identity": self.clock_identity,
            "clock_identity_sha256": self.clock_identity_sha256,
            "engine_identity": self.engine_identity,
            "engine_identity_sha256": self.engine_identity_sha256,
            "market_window_identity_sha256": self.market_window_identity_sha256,
            "model_overlay_identity_sha256": self.model_overlay_identity_sha256,
            "latency_identity_sha256": self.latency_identity_sha256,
            "queue_random_identity_sha256": self.queue_random_identity_sha256,
            "replay_input_receipt_sha256": self.replay_input_receipt_sha256,
            "params_identity_sha256": self.params_identity_sha256,
        }

    @property
    def request_identity_sha256(self) -> str:
        return _canonical_sha256(
            {"schema_version": CACHE_SCHEMA_VERSION, "request": self.payload()}
        )


@dataclass(frozen=True, slots=True)
class ReplayDayInputSchema:
    """Exact logical columns needed to reconstruct the replay inputs."""

    ml_main_array_count: int
    ml_feature_keys: tuple[str, ...]
    trades_columns: tuple[str, ...] = TRADES_COLUMNS
    bbo_columns: tuple[str, ...] = BBO_COLUMNS
    l2_columns: tuple[str, ...] = L2_COLUMNS
    derived_columns: tuple[str, ...] = DERIVED_COLUMNS

    def __post_init__(self) -> None:
        if isinstance(self.ml_main_array_count, bool) or int(self.ml_main_array_count) <= 0:
            raise OfflineDayInputCacheError("ml_main_array_count must be positive")
        object.__setattr__(self, "ml_main_array_count", int(self.ml_main_array_count))
        keys = tuple(
            sorted(_require_label(key, label="ML feature key") for key in self.ml_feature_keys)
        )
        if not keys or len(set(keys)) != len(keys):
            raise OfflineDayInputCacheError("ML feature keys must be non-empty and unique")
        object.__setattr__(self, "ml_feature_keys", keys)
        if self.trades_columns != TRADES_COLUMNS:
            raise OfflineDayInputCacheError("trades schema drifted from the canonical columns")
        if self.bbo_columns != BBO_COLUMNS:
            raise OfflineDayInputCacheError("BBO schema drifted from the canonical columns")
        if self.l2_columns != L2_COLUMNS:
            raise OfflineDayInputCacheError("L2 schema drifted from the canonical columns")
        if self.derived_columns != DERIVED_COLUMNS:
            raise OfflineDayInputCacheError("derived replay schema drifted")

    @property
    def ml_overlay_columns(self) -> tuple[str, ...]:
        main = tuple(f"main_{index:03d}" for index in range(self.ml_main_array_count))
        features = tuple(f"feature::{key}" for key in self.ml_feature_keys)
        return (*main, *features)

    def columns(self, component: str) -> tuple[str, ...]:
        if component == "trades":
            return self.trades_columns
        if component == "bbo":
            return self.bbo_columns
        if component == "l2":
            return self.l2_columns
        if component == "ml_overlay":
            return self.ml_overlay_columns
        if component == "derived":
            return self.derived_columns
        raise OfflineDayInputCacheError(f"unknown cache component: {component}")

    def payload(self) -> dict[str, Any]:
        return {
            "trades_columns": list(self.trades_columns),
            "bbo_columns": list(self.bbo_columns),
            "l2_columns": list(self.l2_columns),
            "derived_columns": list(self.derived_columns),
            "ml_main_array_count": self.ml_main_array_count,
            "ml_feature_keys": list(self.ml_feature_keys),
            "ml_overlay_columns": list(self.ml_overlay_columns),
        }

    @property
    def schema_sha256(self) -> str:
        return _canonical_sha256(self.payload())


@dataclass(frozen=True, slots=True)
class ReplayDayInputArrays:
    """Serializable array views supplied by the existing replay projection."""

    trades: Mapping[str, np.ndarray]
    bbo: Mapping[str, np.ndarray]
    l2: Mapping[str, np.ndarray]
    ml_overlay: Mapping[str, np.ndarray]
    derived: Mapping[str, np.ndarray]
    params: Mapping[str, Any]

    def components(self) -> dict[str, Mapping[str, np.ndarray]]:
        return {
            "trades": self.trades,
            "bbo": self.bbo,
            "l2": self.l2,
            "ml_overlay": self.ml_overlay,
            "derived": self.derived,
        }


@dataclass(frozen=True, slots=True)
class DayInputCacheBinding:
    """Portable binding suitable for a replay execution manifest."""

    request_identity_sha256: str
    cache_identity_sha256: str
    schema_sha256: str
    array_layout_sha256: str
    content_sha256: str
    manifest_sha256: str
    admission_receipt_sha256: str
    estimated_size_bytes: int

    def __post_init__(self) -> None:
        for field in (
            "request_identity_sha256",
            "cache_identity_sha256",
            "schema_sha256",
            "array_layout_sha256",
            "content_sha256",
            "manifest_sha256",
            "admission_receipt_sha256",
        ):
            object.__setattr__(
                self,
                field,
                _require_sha256(getattr(self, field), label=field),
            )
        if isinstance(self.estimated_size_bytes, bool) or int(self.estimated_size_bytes) <= 0:
            raise OfflineDayInputCacheError("estimated_size_bytes must be positive")
        object.__setattr__(self, "estimated_size_bytes", int(self.estimated_size_bytes))

    def payload(self) -> dict[str, str | int]:
        return {field: getattr(self, field) for field in self.__dataclass_fields__}


@dataclass(slots=True)
class ReadOnlyReplayDayInputs:
    """Open read-only memmaps plus the verified cache binding."""

    binding: DayInputCacheBinding
    identity: DayInputCacheIdentity
    schema: ReplayDayInputSchema
    arrays: Mapping[str, Mapping[str, np.memmap]]
    params: Mapping[str, Any]
    _closed: bool = False

    def component(self, name: str) -> Mapping[str, np.memmap]:
        if self._closed:
            raise OfflineDayInputCacheError("day input cache view is closed")
        try:
            return self.arrays[name]
        except KeyError as exc:
            raise OfflineDayInputCacheError(f"unknown cache component: {name}") from exc

    def ml_overlay_tuple(self) -> tuple[Any, ...]:
        overlay = self.component("ml_overlay")
        main = tuple(
            overlay[f"main_{index:03d}"] for index in range(self.schema.ml_main_array_count)
        )
        features = {key: overlay[f"feature::{key}"] for key in self.schema.ml_feature_keys}
        return (*main, MappingProxyType(features))

    def to_replay_inputs(self, replay_inputs_type: type[Any]) -> Any:
        """Rebuild the existing adapter's private ``_ReplayInputs`` type."""

        trades = self.component("trades")
        bbo = self.component("bbo")
        l2 = self.component("l2")
        derived = self.component("derived")
        kwargs = {
            "utc_day": self.identity.utc_day,
            "continuation_day": self.identity.continuation_day,
            "trades": pd.DataFrame({name: trades[name] for name in TRADES_COLUMNS}, copy=False),
            "var_ts_ms": derived["var_ts_ms"],
            "var_ssq": derived["var_ssq"],
            "var_ti": derived["var_ti"],
            "var_retsq": derived["var_retsq"],
            "bbo_data": HistoricalBBOData(
                ts_ms=bbo["ts_ms"],
                best_bid=bbo["best_bid"],
                best_ask=bbo["best_ask"],
                bid_qty=bbo["bid_qty"],
                ask_qty=bbo["ask_qty"],
                source="f05_offline_day_input_cache_v1",
            ),
            "l2_data": HistoricalL2Data(
                ts_ms=l2["ts_ms"],
                bid_px=l2["bid_px"],
                bid_qty=l2["bid_qty"],
                ask_px=l2["ask_px"],
                ask_qty=l2["ask_qty"],
                source="f05_offline_day_input_cache_v1",
            ),
            "ml_data": self.ml_overlay_tuple(),
            "params": self.params,
            "market_window_identity_sha256": self.identity.market_window_identity_sha256,
            "model_overlay_identity_sha256": self.identity.model_overlay_identity_sha256,
            "latency_identity_sha256": self.identity.latency_identity_sha256,
            "queue_random_identity_sha256": self.identity.queue_random_identity_sha256,
            "replay_input_receipt_sha256": self.identity.replay_input_receipt_sha256,
        }
        try:
            return replay_inputs_type(**kwargs)
        except TypeError as exc:
            raise OfflineDayInputCacheError(
                "replay input type is incompatible with the cached target-day bundle"
            ) from exc

    def close(self) -> None:
        if self._closed:
            return
        for component in self.arrays.values():
            for array in component.values():
                mmap = getattr(array, "_mmap", None)
                if mmap is not None:
                    mmap.close()
        self._closed = True

    def __enter__(self) -> ReadOnlyReplayDayInputs:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def _column_array(frame: Any, name: str) -> np.ndarray:
    try:
        column = frame[name]
    except (KeyError, TypeError) as exc:
        raise OfflineDayInputCacheError(f"trades are missing canonical column {name}") from exc
    if hasattr(column, "to_numpy"):
        return np.asarray(column.to_numpy(copy=False))
    return np.asarray(column)


def replay_day_input_arrays(
    *,
    trades: Any,
    bbo_data: Any,
    l2_data: Any,
    ml_data: Sequence[Any],
    ml_main_array_count: int,
    var_ts_ms: Any,
    var_ssq: Any,
    var_ti: Any,
    var_retsq: Any,
    params: Mapping[str, Any],
) -> tuple[ReplayDayInputSchema, ReplayDayInputArrays]:
    """Project existing adapter objects into the cache's narrow array API."""

    if len(ml_data) != int(ml_main_array_count) + 1:
        raise OfflineDayInputCacheError("ML overlay tuple length drifted")
    feature_mapping = ml_data[-1]
    if not isinstance(feature_mapping, Mapping) or not feature_mapping:
        raise OfflineDayInputCacheError("ML overlay feature mapping is missing")
    normalized_features = {str(key): value for key, value in feature_mapping.items()}
    if len(normalized_features) != len(feature_mapping):
        raise OfflineDayInputCacheError("ML overlay feature keys collide after normalization")
    schema = ReplayDayInputSchema(
        ml_main_array_count=int(ml_main_array_count),
        ml_feature_keys=tuple(normalized_features),
    )
    overlay = {
        **{
            f"main_{index:03d}": np.asarray(ml_data[index])
            for index in range(schema.ml_main_array_count)
        },
        **{
            f"feature::{key}": np.asarray(normalized_features[key])
            for key in schema.ml_feature_keys
        },
    }
    arrays = ReplayDayInputArrays(
        trades={name: _column_array(trades, name) for name in TRADES_COLUMNS},
        bbo={name: np.asarray(getattr(bbo_data, name)) for name in BBO_COLUMNS},
        l2={name: np.asarray(getattr(l2_data, name)) for name in L2_COLUMNS},
        ml_overlay=overlay,
        derived={
            "var_ts_ms": np.asarray(var_ts_ms),
            "var_ssq": np.asarray(var_ssq),
            "var_ti": np.asarray(var_ti),
            "var_retsq": np.asarray(var_retsq),
        },
        params=_identity_value(params),
    )
    return schema, arrays


def target_day_context_from_replay_inputs(
    replay: Any,
    *,
    source_receipts: Mapping[str, str],
    clock_identity: str,
    clock_identity_sha256: str,
    engine_identity: str,
    engine_identity_sha256: str,
    market_id: str = "BTCUSDC",
    ml_main_array_count: int,
) -> tuple[DayInputCacheIdentity, ReplayDayInputSchema, ReplayDayInputArrays]:
    """Bind one existing ``_ReplayInputs`` object without importing its private type."""

    schema, arrays = replay_day_input_arrays(
        trades=replay.trades,
        bbo_data=replay.bbo_data,
        l2_data=replay.l2_data,
        ml_data=replay.ml_data,
        ml_main_array_count=ml_main_array_count,
        var_ts_ms=replay.var_ts_ms,
        var_ssq=replay.var_ssq,
        var_ti=replay.var_ti,
        var_retsq=replay.var_retsq,
        params=replay.params,
    )
    params_identity_sha256 = _canonical_sha256(arrays.params)
    identity = DayInputCacheIdentity.create(
        utc_day=replay.utc_day,
        continuation_day=replay.continuation_day,
        market_id=market_id,
        source_receipts=source_receipts,
        clock_identity=clock_identity,
        clock_identity_sha256=clock_identity_sha256,
        engine_identity=engine_identity,
        engine_identity_sha256=engine_identity_sha256,
        market_window_identity_sha256=replay.market_window_identity_sha256,
        model_overlay_identity_sha256=replay.model_overlay_identity_sha256,
        latency_identity_sha256=replay.latency_identity_sha256,
        queue_random_identity_sha256=replay.queue_random_identity_sha256,
        replay_input_receipt_sha256=replay.replay_input_receipt_sha256,
        params_identity_sha256=params_identity_sha256,
    )
    return identity, schema, arrays


def _normalize_array(array: Any, *, label: str) -> np.ndarray:
    normalized = np.asarray(array)
    if normalized.ndim == 0 or normalized.size == 0:
        raise OfflineDayInputCacheError(f"{label} must be a non-empty array")
    if normalized.dtype.hasobject or normalized.dtype.fields is not None:
        raise OfflineDayInputCacheError(f"{label} cannot use object or structured dtype")
    if normalized.dtype.kind not in "biuf":
        raise OfflineDayInputCacheError(f"{label} must use bool, integer, or floating dtype")
    return normalized


def _open_read_only_memmap(path: Path, *, label: str) -> np.memmap:
    try:
        array = np.load(path, mmap_mode="r", allow_pickle=False)
    except (OSError, ValueError) as exc:
        raise OfflineDayInputCacheError(f"cannot open {label} as a NumPy mmap") from exc
    if not isinstance(array, np.memmap) or array.flags.writeable:
        mmap = getattr(array, "_mmap", None)
        if mmap is not None:
            mmap.close()
        raise OfflineDayInputCacheError(f"{label} did not open read-only")
    return array


def _require_aligned_1d(component: Mapping[str, np.ndarray], *, label: str) -> int:
    lengths = {len(value) for value in component.values() if value.ndim == 1}
    if any(value.ndim != 1 for value in component.values()) or len(lengths) != 1:
        raise OfflineDayInputCacheError(f"{label} arrays must be aligned one-dimensional columns")
    return next(iter(lengths))


def _require_timestamp_order(array: np.ndarray, *, label: str, strict: bool) -> None:
    if array.dtype.kind not in "iu" or array.ndim != 1:
        raise OfflineDayInputCacheError(f"{label} must be a one-dimensional integer clock")
    delta = array[1:] - array[:-1]
    invalid = delta <= 0 if strict else delta < 0
    if np.any(invalid):
        comparator = "strictly increasing" if strict else "nondecreasing"
        raise OfflineDayInputCacheError(f"{label} must be {comparator}")


def _validated_components(
    schema: ReplayDayInputSchema,
    inputs: ReplayDayInputArrays,
) -> dict[str, dict[str, np.ndarray]]:
    normalized: dict[str, dict[str, np.ndarray]] = {}
    for component, supplied in inputs.components().items():
        expected = schema.columns(component)
        if set(supplied) != set(expected):
            missing = sorted(set(expected) - set(supplied))
            extra = sorted(set(supplied) - set(expected))
            raise OfflineDayInputCacheError(
                f"{component} schema drifted: missing={missing} extra={extra}"
            )
        normalized[component] = {
            name: _normalize_array(supplied[name], label=f"{component}.{name}") for name in expected
        }

    _require_aligned_1d(normalized["trades"], label="trades")
    _require_aligned_1d(normalized["bbo"], label="BBO")
    _require_aligned_1d(normalized["ml_overlay"], label="ML overlay")
    _require_aligned_1d(normalized["derived"], label="derived replay inputs")
    _require_timestamp_order(
        normalized["trades"]["transact_time"], label="trades.transact_time", strict=False
    )
    _require_timestamp_order(normalized["bbo"]["ts_ms"], label="bbo.ts_ms", strict=True)
    _require_timestamp_order(
        normalized["ml_overlay"]["main_000"], label="ml_overlay.main_000", strict=True
    )

    l2 = normalized["l2"]
    if l2["ts_ms"].ndim != 1:
        raise OfflineDayInputCacheError("l2.ts_ms must be one-dimensional")
    matrix_shapes = {l2[name].shape for name in L2_COLUMNS[1:]}
    if len(matrix_shapes) != 1:
        raise OfflineDayInputCacheError("L2 price and quantity matrix shapes differ")
    matrix_shape = next(iter(matrix_shapes))
    if len(matrix_shape) != 2 or matrix_shape[0] != len(l2["ts_ms"]) or matrix_shape[1] <= 0:
        raise OfflineDayInputCacheError("L2 arrays are not aligned as rows by levels")
    _require_timestamp_order(l2["ts_ms"], label="l2.ts_ms", strict=True)
    _require_timestamp_order(
        normalized["derived"]["var_ts_ms"], label="derived.var_ts_ms", strict=True
    )
    return normalized


def _array_content_sha256(array: np.ndarray) -> str:
    descriptor = {
        "dtype": array.dtype.str,
        "shape": list(array.shape),
        "nbytes": int(array.nbytes),
    }
    digest = hashlib.sha256(_canonical_bytes(descriptor))
    if array.flags.c_contiguous:
        byte_view = memoryview(array).cast("B")
        for offset in range(0, len(byte_view), _ARRAY_CHUNK_BYTES):
            digest.update(byte_view[offset : offset + _ARRAY_CHUNK_BYTES])
    else:
        row_bytes = max(1, int(array[0].nbytes))
        rows_per_chunk = max(1, _ARRAY_CHUNK_BYTES // row_bytes)
        for start in range(0, len(array), rows_per_chunk):
            chunk = np.ascontiguousarray(array[start : start + rows_per_chunk])
            digest.update(memoryview(chunk).cast("B"))
    return digest.hexdigest()


def _array_descriptors(
    components: Mapping[str, Mapping[str, np.ndarray]],
) -> tuple[list[dict[str, Any]], str, str]:
    rows: list[dict[str, Any]] = []
    for component in COMPONENTS:
        for name, array in components[component].items():
            rows.append(
                {
                    "component": component,
                    "name": name,
                    "dtype": array.dtype.str,
                    "shape": list(array.shape),
                    "nbytes": int(array.nbytes),
                    "array_content_sha256": _array_content_sha256(array),
                }
            )
    layout_rows = [
        {key: row[key] for key in ("component", "name", "dtype", "shape", "nbytes")} for row in rows
    ]
    return rows, _canonical_sha256(layout_rows), _canonical_sha256(rows)


def _cache_identity_sha256(
    *,
    request_identity_sha256: str,
    schema_sha256: str,
    array_layout_sha256: str,
    content_sha256: str,
) -> str:
    return _canonical_sha256(
        {
            "schema_version": CACHE_SCHEMA_VERSION,
            "request_identity_sha256": request_identity_sha256,
            "schema_sha256": schema_sha256,
            "array_layout_sha256": array_layout_sha256,
            "content_sha256": content_sha256,
        }
    )


class ReplayDayInputCache:
    """Atomic writer and read-only opener for immutable daily replay arrays."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root).expanduser().resolve()
        self.cache_tier_config = cache_tier_lru.CacheTierConfig.from_environment()
        try:
            self.cache_tier_config.validate(require_roots=True)
        except cache_tier_lru.CacheTierError as exc:
            raise OfflineDayInputCacheError("cache-tier governance is unavailable") from exc
        governed = False
        for tier_root in (
            self.cache_tier_config.hot_root.resolve(),
            self.cache_tier_config.cold_root.resolve(),
        ):
            try:
                relative = self.root.relative_to(tier_root)
            except ValueError:
                continue
            if relative.parts and relative.parts[0] == "replay_dag":
                governed = True
                break
        if not governed:
            raise OfflineDayInputCacheError(
                "day input cache root must remain under a governed replay_dag root"
            )
        self.entries = self.root / "entries"
        self.admissions = self.root / "admissions"
        self.locks = self.root / "locks"

    def _admission_path(self, identity: DayInputCacheIdentity) -> Path:
        return self.admissions / f"{identity.request_identity_sha256}.json"

    def _entry_path(self, cache_identity_sha256: str) -> Path:
        return self.entries / cache_identity_sha256

    def _write_entry(
        self,
        *,
        identity: DayInputCacheIdentity,
        schema: ReplayDayInputSchema,
        components: Mapping[str, Mapping[str, np.ndarray]],
        params: Mapping[str, Any],
        descriptors: Sequence[Mapping[str, Any]],
        array_layout_sha256: str,
        content_sha256: str,
        cache_identity_sha256: str,
        estimated_size_bytes: int,
    ) -> tuple[Path, str]:
        final = self._entry_path(cache_identity_sha256)
        if final.exists():
            raise OfflineDayInputCacheError(
                "unadmitted day input cache entry already occupies the content identity"
            )
        final.parent.mkdir(parents=True, exist_ok=True)
        staging = final.parent / f".{cache_identity_sha256}.{uuid.uuid4().hex}.partial"
        staging.mkdir(parents=False, exist_ok=False)
        try:
            arrays_dir = staging / "arrays"
            arrays_dir.mkdir()
            files: list[dict[str, Any]] = []
            descriptor_by_name = {
                (str(row["component"]), str(row["name"])): dict(row) for row in descriptors
            }
            index = 0
            for component in COMPONENTS:
                for name, array in components[component].items():
                    filename = f"array_{index:04d}.npy"
                    path = arrays_dir / filename
                    with path.open("wb") as handle:
                        np.save(handle, array, allow_pickle=False)
                        handle.flush()
                        os.fsync(handle.fileno())
                    stored = _open_read_only_memmap(path, label=f"serialized {component}.{name}")
                    try:
                        descriptor = descriptor_by_name[(component, name)]
                        if (
                            stored.dtype.str != descriptor["dtype"]
                            or list(stored.shape) != descriptor["shape"]
                        ):
                            raise OfflineDayInputCacheError("serialized array layout drifted")
                        if _array_content_sha256(stored) != descriptor["array_content_sha256"]:
                            raise OfflineDayInputCacheError("serialized array content drifted")
                    finally:
                        mmap = getattr(stored, "_mmap", None)
                        if mmap is not None:
                            mmap.close()
                    files.append(
                        {
                            **descriptor,
                            "file": f"arrays/{filename}",
                            "file_sha256": _file_sha256(path),
                        }
                    )
                    index += 1
            _fsync_directory(arrays_dir)
            manifest: dict[str, Any] = {
                "schema_version": CACHE_SCHEMA_VERSION,
                "cache_identity": CACHE_IDENTITY,
                "request": identity.payload(),
                "request_identity_sha256": identity.request_identity_sha256,
                "schema": schema.payload(),
                "schema_sha256": schema.schema_sha256,
                "params": dict(params),
                "params_identity_sha256": identity.params_identity_sha256,
                "array_layout_sha256": array_layout_sha256,
                "content_sha256": content_sha256,
                "cache_identity_sha256": cache_identity_sha256,
                "estimated_size_bytes": estimated_size_bytes,
                "arrays": files,
                "atomic_admission": True,
                "complete": True,
                "read_only_payload": True,
                "economic_outcomes_read": False,
                "validation_read": False,
                "sealed_holdout_read": False,
                "action_authorized": False,
                "live_authorized": False,
            }
            manifest["manifest_document_sha256"] = _document_sha256(
                manifest, "manifest_document_sha256"
            )
            _atomic_json(staging / "manifest.json", manifest)
            manifest_sha256 = _file_sha256(staging / "manifest.json")
            success = staging / "_SUCCESS"
            with success.open("w", encoding="ascii") as handle:
                handle.write(f"{manifest_sha256}\n")
                handle.flush()
                os.fsync(handle.fileno())
            _fsync_directory(staging)
            os.replace(staging, final)
            _fsync_directory(final.parent)
            self._validate_manifest(final, identity=identity, schema=schema)
            return final, manifest_sha256
        finally:
            if staging.exists():
                shutil.rmtree(staging)

    def _read_admission(
        self, identity: DayInputCacheIdentity
    ) -> tuple[dict[str, Any], DayInputCacheBinding] | None:
        path = self._admission_path(identity)
        if not path.is_file():
            return None
        payload = _read_json(path, label="day input cache admission")
        if (
            payload.get("schema_version") != ADMISSION_SCHEMA_VERSION
            or payload.get("request") != identity.payload()
            or payload.get("request_identity_sha256") != identity.request_identity_sha256
            or payload.get("atomic_admission") is not True
            or payload.get("complete") is not True
            or payload.get("admission_receipt_sha256") != _admission_receipt_sha256(payload)
        ):
            raise OfflineDayInputCacheError("day input cache admission drifted")
        binding_payload = payload.get("binding")
        if not isinstance(binding_payload, Mapping):
            raise OfflineDayInputCacheError("day input cache binding is malformed")
        try:
            binding = DayInputCacheBinding(**dict(binding_payload))
        except (TypeError, OfflineDayInputCacheError) as exc:
            raise OfflineDayInputCacheError("day input cache binding is malformed") from exc
        if binding.admission_receipt_sha256 != payload["admission_receipt_sha256"]:
            raise OfflineDayInputCacheError("day input admission receipt binding drifted")
        return payload, binding

    def _publish_admission(
        self,
        *,
        identity: DayInputCacheIdentity,
        cache_identity_sha256: str,
        schema_sha256: str,
        array_layout_sha256: str,
        content_sha256: str,
        manifest_sha256: str,
        estimated_size_bytes: int,
    ) -> DayInputCacheBinding:
        payload: dict[str, Any] = {
            "schema_version": ADMISSION_SCHEMA_VERSION,
            "request": identity.payload(),
            "request_identity_sha256": identity.request_identity_sha256,
            "atomic_admission": True,
            "complete": True,
        }
        binding_without_receipt = {
            "request_identity_sha256": identity.request_identity_sha256,
            "cache_identity_sha256": cache_identity_sha256,
            "schema_sha256": schema_sha256,
            "array_layout_sha256": array_layout_sha256,
            "content_sha256": content_sha256,
            "manifest_sha256": manifest_sha256,
            "estimated_size_bytes": estimated_size_bytes,
        }
        payload["binding"] = binding_without_receipt
        receipt_sha256 = _admission_receipt_sha256(payload)
        payload["admission_receipt_sha256"] = receipt_sha256
        payload["binding"] = {
            **binding_without_receipt,
            "admission_receipt_sha256": receipt_sha256,
        }
        _atomic_json(self._admission_path(identity), payload)
        return DayInputCacheBinding(**payload["binding"])

    def admit(
        self,
        *,
        identity: DayInputCacheIdentity,
        schema: ReplayDayInputSchema,
        inputs: ReplayDayInputArrays,
    ) -> DayInputCacheBinding:
        components = _validated_components(schema, inputs)
        params = _identity_value(inputs.params)
        if not isinstance(params, Mapping):
            raise OfflineDayInputCacheError("replay params must be a mapping")
        if _canonical_sha256(params) != identity.params_identity_sha256:
            raise OfflineDayInputCacheError("replay params identity drifted")
        descriptors, array_layout_sha256, content_sha256 = _array_descriptors(components)
        estimated_size_bytes = sum(int(row["nbytes"]) for row in descriptors) + len(
            _canonical_bytes(params)
        )
        cache_identity_sha256 = _cache_identity_sha256(
            request_identity_sha256=identity.request_identity_sha256,
            schema_sha256=schema.schema_sha256,
            array_layout_sha256=array_layout_sha256,
            content_sha256=content_sha256,
        )
        lock = self.locks / f"{identity.request_identity_sha256}.lock"
        with _exclusive_lock(lock):
            existing = self._read_admission(identity)
            if existing is not None:
                _, binding = existing
                expected = (
                    cache_identity_sha256,
                    schema.schema_sha256,
                    array_layout_sha256,
                    content_sha256,
                )
                observed = (
                    binding.cache_identity_sha256,
                    binding.schema_sha256,
                    binding.array_layout_sha256,
                    binding.content_sha256,
                )
                if observed != expected:
                    raise OfflineDayInputCacheError(
                        "immutable day input request resolved to different content"
                    )
                self._validate_entry(identity, schema=schema, expected=binding, open_arrays=False)
                return binding
            _, manifest_sha256 = self._write_entry(
                identity=identity,
                schema=schema,
                components=components,
                params=params,
                descriptors=descriptors,
                array_layout_sha256=array_layout_sha256,
                content_sha256=content_sha256,
                cache_identity_sha256=cache_identity_sha256,
                estimated_size_bytes=estimated_size_bytes,
            )
            binding = self._publish_admission(
                identity=identity,
                cache_identity_sha256=cache_identity_sha256,
                schema_sha256=schema.schema_sha256,
                array_layout_sha256=array_layout_sha256,
                content_sha256=content_sha256,
                manifest_sha256=manifest_sha256,
                estimated_size_bytes=estimated_size_bytes,
            )
            cache_tier_lru.register_cache_write(
                self._entry_path(binding.cache_identity_sha256),
                identity_sha256=binding.cache_identity_sha256,
                reference_class="unknown",
                size_bytes=binding.estimated_size_bytes,
                strict=False,
            )
            return binding

    def _validate_manifest(
        self,
        entry: Path,
        *,
        identity: DayInputCacheIdentity,
        schema: ReplayDayInputSchema | None,
    ) -> dict[str, Any]:
        manifest_path = entry / "manifest.json"
        manifest = _read_json(manifest_path, label="day input cache manifest")
        if (
            manifest.get("schema_version") != CACHE_SCHEMA_VERSION
            or manifest.get("cache_identity") != CACHE_IDENTITY
            or manifest.get("request") != identity.payload()
            or manifest.get("request_identity_sha256") != identity.request_identity_sha256
            or manifest.get("complete") is not True
            or manifest.get("atomic_admission") is not True
            or manifest.get("read_only_payload") is not True
            or manifest.get("manifest_document_sha256")
            != _document_sha256(manifest, "manifest_document_sha256")
        ):
            raise OfflineDayInputCacheError("day input cache manifest drifted")
        if any(
            manifest.get(field) is not False
            for field in (
                "economic_outcomes_read",
                "validation_read",
                "sealed_holdout_read",
                "action_authorized",
                "live_authorized",
            )
        ):
            raise OfflineDayInputCacheError("day input cache crossed its permission boundary")
        if schema is not None and (
            manifest.get("schema") != schema.payload()
            or manifest.get("schema_sha256") != schema.schema_sha256
        ):
            raise OfflineDayInputCacheError("day input cache schema drifted")
        params = manifest.get("params")
        if (
            not isinstance(params, Mapping)
            or manifest.get("params_identity_sha256") != identity.params_identity_sha256
            or _canonical_sha256(params) != identity.params_identity_sha256
        ):
            raise OfflineDayInputCacheError("day input cache params identity drifted")
        estimated_size = manifest.get("estimated_size_bytes")
        if (
            isinstance(estimated_size, bool)
            or not isinstance(estimated_size, int)
            or estimated_size <= 0
        ):
            raise OfflineDayInputCacheError("day input cache estimated size is invalid")
        expected_cache_identity = _cache_identity_sha256(
            request_identity_sha256=identity.request_identity_sha256,
            schema_sha256=_require_sha256(manifest.get("schema_sha256", ""), label="schema_sha256"),
            array_layout_sha256=_require_sha256(
                manifest.get("array_layout_sha256", ""), label="array_layout_sha256"
            ),
            content_sha256=_require_sha256(
                manifest.get("content_sha256", ""), label="content_sha256"
            ),
        )
        if (
            manifest.get("cache_identity_sha256") != expected_cache_identity
            or entry.name != expected_cache_identity
        ):
            raise OfflineDayInputCacheError("day input cache identity drifted")
        success = entry / "_SUCCESS"
        if success.is_symlink() or not success.is_file():
            raise OfflineDayInputCacheError("day input cache lacks atomic success receipt")
        try:
            success_value = success.read_text(encoding="ascii").strip()
        except (OSError, UnicodeError) as exc:
            raise OfflineDayInputCacheError("cannot read cache success receipt") from exc
        if success_value != _file_sha256(manifest_path):
            raise OfflineDayInputCacheError("day input cache success receipt drifted")
        return manifest

    def _validate_entry(
        self,
        identity: DayInputCacheIdentity,
        *,
        schema: ReplayDayInputSchema | None,
        expected: DayInputCacheBinding,
        open_arrays: bool,
    ) -> tuple[dict[str, Any], dict[str, dict[str, np.memmap]]]:
        entry = self._entry_path(expected.cache_identity_sha256)
        if entry.is_symlink() or not entry.is_dir():
            raise OfflineDayInputCacheError("day input cache entry is missing or redirected")
        manifest = self._validate_manifest(entry, identity=identity, schema=schema)
        manifest_sha256 = _file_sha256(entry / "manifest.json")
        observed_binding = {
            "request_identity_sha256": manifest["request_identity_sha256"],
            "cache_identity_sha256": manifest["cache_identity_sha256"],
            "schema_sha256": manifest["schema_sha256"],
            "array_layout_sha256": manifest["array_layout_sha256"],
            "content_sha256": manifest["content_sha256"],
            "manifest_sha256": manifest_sha256,
            "admission_receipt_sha256": expected.admission_receipt_sha256,
            "estimated_size_bytes": manifest["estimated_size_bytes"],
        }
        if observed_binding != expected.payload():
            raise OfflineDayInputCacheError("day input cache binding drifted")
        rows = manifest.get("arrays")
        if not isinstance(rows, list) or not rows:
            raise OfflineDayInputCacheError("day input cache array manifest is empty")
        expected_names = {
            (component, name)
            for component in COMPONENTS
            for name in (schema.columns(component) if schema is not None else ())
        }
        observed_names: set[tuple[str, str]] = set()
        opened: dict[str, dict[str, np.memmap]] = {component: {} for component in COMPONENTS}
        layout_rows: list[dict[str, Any]] = []
        content_rows: list[dict[str, Any]] = []
        try:
            for raw_row in rows:
                if not isinstance(raw_row, Mapping):
                    raise OfflineDayInputCacheError("day input cache array row is malformed")
                row = dict(raw_row)
                component = str(row.get("component", ""))
                name = str(row.get("name", ""))
                if component not in COMPONENTS or not name or (component, name) in observed_names:
                    raise OfflineDayInputCacheError("day input cache array identity is malformed")
                observed_names.add((component, name))
                relative = Path(str(row.get("file", "")))
                if (
                    relative.is_absolute()
                    or ".." in relative.parts
                    or relative.parts[:1] != ("arrays",)
                ):
                    raise OfflineDayInputCacheError("day input cache array path escapes its entry")
                path = entry / relative
                if path.is_symlink() or not path.is_file():
                    raise OfflineDayInputCacheError(
                        "day input cache array is missing or redirected"
                    )
                if _file_sha256(path) != _require_sha256(
                    row.get("file_sha256", ""), label=f"{component}.{name} file_sha256"
                ):
                    raise OfflineDayInputCacheError("day input cache array file hash drifted")
                array = _open_read_only_memmap(path, label=f"cached {component}.{name}")
                keep_open = False
                try:
                    descriptor = {
                        "component": component,
                        "name": name,
                        "dtype": array.dtype.str,
                        "shape": list(array.shape),
                        "nbytes": int(array.nbytes),
                        "array_content_sha256": _require_sha256(
                            row.get("array_content_sha256", ""),
                            label=f"{component}.{name} content_sha256",
                        ),
                    }
                    if any(
                        descriptor[field] != row.get(field)
                        for field in ("dtype", "shape", "nbytes")
                    ):
                        raise OfflineDayInputCacheError("day input cache array layout drifted")
                    # Admission proved that this exact NPY byte hash decodes to
                    # the bound array hash, so an open needs one full pass.
                    layout_rows.append(
                        {
                            key: descriptor[key]
                            for key in ("component", "name", "dtype", "shape", "nbytes")
                        }
                    )
                    content_rows.append(descriptor)
                    if open_arrays:
                        opened[component][name] = array
                        keep_open = True
                finally:
                    if not keep_open:
                        mmap = getattr(array, "_mmap", None)
                        if mmap is not None:
                            mmap.close()
            if schema is not None and observed_names != expected_names:
                raise OfflineDayInputCacheError("day input cache columns drifted")
            if _canonical_sha256(layout_rows) != manifest["array_layout_sha256"]:
                raise OfflineDayInputCacheError("day input cache layout identity drifted")
            if _canonical_sha256(content_rows) != manifest["content_sha256"]:
                raise OfflineDayInputCacheError("day input cache content identity drifted")
            return manifest, opened
        except Exception:
            for component in opened.values():
                for array in component.values():
                    mmap = getattr(array, "_mmap", None)
                    if mmap is not None:
                        mmap.close()
            raise

    def _record_lru_access(self, binding: DayInputCacheBinding) -> None:
        # LRU metadata remains authoritative in models.cache_tier_lru.  Its
        # best-effort failure semantics never alter this cache's content hash.
        cache_tier_lru.record_cache_access(
            self._entry_path(binding.cache_identity_sha256),
            identity_sha256=binding.cache_identity_sha256,
            reference_class="unknown",
            strict=False,
        )

    def open(
        self,
        *,
        identity: DayInputCacheIdentity,
        schema: ReplayDayInputSchema,
        expected: DayInputCacheBinding | None = None,
        record_lru_access: bool = True,
    ) -> ReadOnlyReplayDayInputs:
        admission = self._read_admission(identity)
        if admission is None:
            raise OfflineDayInputCacheError("day input cache request is not atomically admitted")
        _, binding = admission
        if expected is not None and binding != expected:
            raise OfflineDayInputCacheError("day input cache expected binding drifted")
        manifest, arrays = self._validate_entry(
            identity,
            schema=schema,
            expected=binding,
            open_arrays=True,
        )
        try:
            if record_lru_access:
                self._record_lru_access(binding)
        except Exception:
            for component in arrays.values():
                for array in component.values():
                    mmap = getattr(array, "_mmap", None)
                    if mmap is not None:
                        mmap.close()
            raise
        protected = MappingProxyType(
            {component: MappingProxyType(dict(values)) for component, values in arrays.items()}
        )
        return ReadOnlyReplayDayInputs(
            binding=binding,
            identity=identity,
            schema=schema,
            arrays=protected,
            params=MappingProxyType(dict(manifest["params"])),
        )


def admit_replay_day_inputs(
    cache_root: Path,
    *,
    identity: DayInputCacheIdentity,
    schema: ReplayDayInputSchema,
    inputs: ReplayDayInputArrays,
) -> DayInputCacheBinding:
    """Narrow adapter API for one atomic cache admission."""

    return ReplayDayInputCache(cache_root).admit(
        identity=identity,
        schema=schema,
        inputs=inputs,
    )


def open_replay_day_inputs(
    cache_root: Path,
    *,
    identity: DayInputCacheIdentity,
    schema: ReplayDayInputSchema,
    expected: DayInputCacheBinding | None = None,
    record_lru_access: bool = True,
) -> ReadOnlyReplayDayInputs:
    """Narrow adapter API returning verified, read-only NumPy memmaps."""

    return ReplayDayInputCache(cache_root).open(
        identity=identity,
        schema=schema,
        expected=expected,
        record_lru_access=record_lru_access,
    )


__all__ = [
    "BBO_COLUMNS",
    "CACHE_IDENTITY",
    "CACHE_SCHEMA_VERSION",
    "COMPONENTS",
    "DERIVED_COLUMNS",
    "DayInputCacheBinding",
    "DayInputCacheIdentity",
    "L2_COLUMNS",
    "OfflineDayInputCacheError",
    "ReadOnlyReplayDayInputs",
    "ReplayDayInputArrays",
    "ReplayDayInputCache",
    "ReplayDayInputSchema",
    "SOURCE_COMPONENTS",
    "TRADES_COLUMNS",
    "admit_replay_day_inputs",
    "open_replay_day_inputs",
    "replay_day_input_arrays",
    "target_day_context_from_replay_inputs",
]
