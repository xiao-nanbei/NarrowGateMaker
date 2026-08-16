"""Compact, hash-bound C++ observation tape for F05 real-day parity."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any

import numpy as np
import pyarrow.parquet as pq

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_native_observation_cache as cache,
)

IDENTITY = "f05_cpp_real_day_observation_tape_v21"
SCHEMA_VERSION = f"{IDENTITY}.v1"
_ARRAY_DTYPES = {
    "left_ts_ns": np.int64,
    "right_ts_ns": np.int64,
    "feature_ready_ts_ns": np.int64,
    "market_generation": np.int64,
    "depth_generation": np.int64,
    "mid_usdc_per_btc": np.float64,
    "source_gap": np.uint8,
    "source_stale": np.uint8,
    "warmup_admitted": np.uint8,
    "channel_support_valid": np.uint8,
}


class CppObservationTapeError(RuntimeError):
    """Raised when the admitted observation stream cannot bind a C++ tape."""


@dataclass(frozen=True, slots=True)
class CppObservationTape:
    arrays: Mapping[str, np.ndarray]
    receipt: Mapping[str, Any]


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _read_cache_arrays(
    day_root: Path,
    *,
    start_feature_ready_ts_ns: int | None = None,
    end_feature_ready_ts_ns: int | None = None,
) -> dict[str, np.ndarray]:
    parquet = pq.ParquetFile(day_root / cache.PARQUET_NAME)
    columns = tuple(cache.CORE_COLUMNS) + tuple(cache.VALUE_COLUMNS)
    chunks: dict[str, list[np.ndarray]] = {name: [] for name in _ARRAY_DTYPES}
    for batch in parquet.iter_batches(batch_size=131_072, columns=list(columns)):
        ready = batch.column(columns.index("feature_ready_ts_ns")).to_numpy(
            zero_copy_only=False
        )
        mask = np.ones(batch.num_rows, dtype=bool)
        if start_feature_ready_ts_ns is not None:
            mask &= ready >= int(start_feature_ready_ts_ns)
        if end_feature_ready_ts_ns is not None:
            mask &= ready < int(end_feature_ready_ts_ns)
        if not mask.any():
            continue

        def values(
            name: str,
            dtype: Any,
            *,
            current_batch: Any = batch,
            current_mask: np.ndarray = mask,
        ) -> np.ndarray:
            column = current_batch.column(columns.index(name))
            if name == "mid_usdc_per_btc":
                output = column.to_numpy(
                    zero_copy_only=False,
                    writable=False,
                ).astype(np.float64, copy=False)
                if column.null_count:
                    output = output.copy()
                    output[~column.is_valid().to_numpy(zero_copy_only=False)] = np.nan
            else:
                output = column.to_numpy(zero_copy_only=False).astype(dtype, copy=False)
            return np.ascontiguousarray(output[current_mask], dtype=dtype)

        for name in (
            "left_ts_ns",
            "right_ts_ns",
            "feature_ready_ts_ns",
            "market_generation",
            "depth_generation",
            "mid_usdc_per_btc",
            "source_gap",
            "source_stale",
            "warmup_admitted",
        ):
            chunks[name].append(values(name, _ARRAY_DTYPES[name]))

        support = np.ones(batch.num_rows, dtype=bool)
        for name in cache.VALUE_COLUMNS:
            support &= batch.column(columns.index(name)).is_valid().to_numpy(
                zero_copy_only=False
            )
        support &= ~batch.column(columns.index("source_gap")).to_numpy(
            zero_copy_only=False
        ).astype(bool, copy=False)
        support &= ~batch.column(columns.index("source_stale")).to_numpy(
            zero_copy_only=False
        ).astype(bool, copy=False)
        chunks["channel_support_valid"].append(
            np.ascontiguousarray(support[mask], dtype=np.uint8)
        )
    arrays = {
        name: np.ascontiguousarray(
            np.concatenate(parts) if parts else np.empty(0, dtype=dtype),
            dtype=dtype,
        )
        for name, dtype in _ARRAY_DTYPES.items()
        for parts in (chunks[name],)
    }
    return arrays


def _array_digest(arrays: Mapping[str, np.ndarray]) -> str:
    digest = hashlib.sha256()
    digest.update(SCHEMA_VERSION.encode("ascii") + b"\0")
    for name in _ARRAY_DTYPES:
        value = np.ascontiguousarray(arrays[name], dtype=_ARRAY_DTYPES[name])
        digest.update(name.encode("ascii") + b"\0")
        digest.update(str(value.dtype).encode("ascii") + b"\0")
        digest.update(int(value.size).to_bytes(8, "big", signed=False))
        digest.update(value.view(np.uint8))
    return digest.hexdigest()


def load_cpp_observation_tape(
    output_root: Path,
    *,
    target_day: str,
    continuation_day: str,
    deep_validate: bool,
) -> CppObservationTape:
    """Load D-1/D/D+1 into compact arrays without materializing Python rows."""

    target_validation = cache.validate_admitted_cache(
        output_root, target_day, deep=deep_validate
    )
    continuation_validation = cache.validate_admitted_cache(
        output_root, continuation_day, deep=deep_validate
    )
    cutoff_ns = (
        int(np.datetime64(target_day, "D").astype(np.int64)) + 1
    ) * cache.DAY_NS
    target = _read_cache_arrays(target_validation.day_root)
    continuation = _read_cache_arrays(
        continuation_validation.day_root,
        start_feature_ready_ts_ns=cutoff_ns,
        end_feature_ready_ts_ns=cutoff_ns + cache.DAY_NS,
    )
    if target["right_ts_ns"].size == 0 or continuation["right_ts_ns"].size == 0:
        raise CppObservationTapeError("real-day C++ observation tape is empty")
    after_overlap = continuation["right_ts_ns"] > target["right_ts_ns"][-1]
    if not after_overlap.any():
        raise CppObservationTapeError("continuation cache has no non-overlap rows")
    continuation = {name: value[after_overlap] for name, value in continuation.items()}

    market_offset = (
        int(target["market_generation"][-1])
        + 1
        - int(continuation["market_generation"][0])
    )
    depth_offset = (
        int(target["depth_generation"][-1])
        + 1
        - int(continuation["depth_generation"][0])
    )
    continuation["market_generation"] = np.ascontiguousarray(
        continuation["market_generation"] + market_offset,
        dtype=np.int64,
    )
    continuation["depth_generation"] = np.ascontiguousarray(
        continuation["depth_generation"] + depth_offset,
        dtype=np.int64,
    )
    arrays = {
        name: np.ascontiguousarray(
            np.concatenate((target[name], continuation[name])),
            dtype=dtype,
        )
        for name, dtype in _ARRAY_DTYPES.items()
    }
    count = arrays["right_ts_ns"].size
    if any(value.ndim != 1 or value.size != count for value in arrays.values()):
        raise CppObservationTapeError("real-day C++ tape array shape drifted")
    if not np.all(
        arrays["right_ts_ns"] - arrays["left_ts_ns"]
        == cache.BASE_WINDOW_WIDTH_NS
    ):
        raise CppObservationTapeError("real-day C++ tape window width drifted")
    if not np.all(arrays["left_ts_ns"][1:] == arrays["right_ts_ns"][:-1]):
        raise CppObservationTapeError("real-day C++ tape is not contiguous")
    if not np.all(np.diff(arrays["feature_ready_ts_ns"]) >= 0):
        raise CppObservationTapeError("real-day C++ feature-ready clock regressed")
    if not np.all(np.diff(arrays["market_generation"]) > 0):
        raise CppObservationTapeError("real-day C++ market generation drifted")
    if not np.all(np.diff(arrays["depth_generation"]) > 0):
        raise CppObservationTapeError("real-day C++ depth generation drifted")

    receipt = {
        "identity": IDENTITY,
        "schema_version": SCHEMA_VERSION,
        "target_day": target_day,
        "continuation_day": continuation_day,
        "observation_count": int(count),
        "first_left_ts_ns": int(arrays["left_ts_ns"][0]),
        "last_right_ts_ns": int(arrays["right_ts_ns"][-1]),
        "target_cache_manifest_sha256": str(
            target_validation.manifest["canonical_manifest_sha256"]
        ),
        "continuation_cache_manifest_sha256": str(
            continuation_validation.manifest["canonical_manifest_sha256"]
        ),
        "array_sha256": _array_digest(arrays),
        "economic_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    receipt["canonical_sha256"] = _canonical_sha256(receipt)
    for value in arrays.values():
        value.flags.writeable = False
    return CppObservationTape(
        arrays=MappingProxyType(arrays),
        receipt=MappingProxyType(receipt),
    )


__all__ = [
    "CppObservationTape",
    "CppObservationTapeError",
    "IDENTITY",
    "SCHEMA_VERSION",
    "load_cpp_observation_tape",
]
