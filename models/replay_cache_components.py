"""Content-addressed replay cache components with atomic directory publication."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import uuid
from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from models.cache_tier_lru import record_cache_access, register_cache_write
from models.replay_cache_dag import REPLAY_WINDOW_CACHE_GRAPH_V2_IDENTITY

MARKET_CONTEXT_SCHEMA_VERSION = "narrowgate.market_context_day.v2.1"
MODEL_OVERLAY_SCHEMA_VERSION = "narrowgate.model_overlay_day.v1.1"
SOURCE_REFERENCE_SCHEMA_VERSION = "narrowgate.replay_source_reference.v2"
DIRECT_SOURCE_HASH_MAX_BYTES = 64 * 1024 * 1024

MARKET_CONTEXT_SCHEMA = {
    "source_identity": "stable_role_logical_source_content_sha256.v1",
    "files": {
        "trades": "trades.parquet:zstd",
        "rolling_arrays": "rolling_arrays.npz:deflate",
        "source_references": "source_references.json",
        "manifest": "manifest.json",
    },
    "rolling_fields": (
        "var_ts_ms",
        "var_ssq",
        "var_ti",
        "var_retsq",
    ),
    "forbidden_fields": (
        "bbo_data",
        "l2_data",
        "orders",
        "queue",
        "fills",
        "inventory",
        "campaign",
        "pnl",
    ),
}

MODEL_OVERLAY_SCHEMA = {
    "source_identity": "stable_role_logical_source_content_sha256.v1",
    "files": {
        "arrays": "model_overlay.npz:deflate",
        "manifest": "manifest.json",
    },
    "payload": "tuple_of_numpy_arrays_with_optional_final_feature_mapping",
    "forbidden_fields": MARKET_CONTEXT_SCHEMA["forbidden_fields"],
}


class ReplayCacheIntegrityError(RuntimeError):
    """Raised when a content-addressed component fails its frozen contract."""


def canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_sha256(value: str, *, field: str) -> str:
    normalized = str(value).strip().lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError(f"{field} must be a lowercase SHA256")
    return normalized


def file_reference(
    path: Path,
    *,
    role: str,
    logical_source: str | None = None,
    sha256: str | None = None,
    hash_provenance: Mapping[str, Any] | None = None,
    direct_hash_max_bytes: int = DIRECT_SOURCE_HASH_MAX_BYTES,
) -> dict[str, Any]:
    """Describe a source with stable content identity and a mutable locator.

    ``path``, size and mtime are retained for locating and auditing the source,
    but only role, logical source and content SHA participate in cache identity.
    Large files must reuse a trusted producer/manifest SHA instead of being
    silently hashed while constructing a replay cache key.
    """

    resolved = Path(path).expanduser().resolve()
    stat = resolved.stat()
    source_name = str(logical_source or f"{role}/{resolved.name}").strip()
    if not source_name:
        raise ValueError("logical_source must be non-empty")
    if sha256 is None:
        if int(stat.st_size) > int(direct_hash_max_bytes):
            raise ValueError(
                "source exceeds direct-hash limit and requires a trusted "
                f"manifest SHA256: {resolved}"
            )
        content_sha256 = file_sha256(resolved)
        provenance = {"kind": "direct_file_sha256"}
    else:
        content_sha256 = _validate_sha256(sha256, field="source sha256")
        provenance = dict(hash_provenance or {"kind": "provided_sha256"})
    return {
        "schema_version": SOURCE_REFERENCE_SCHEMA_VERSION,
        "role": str(role),
        "logical_source": source_name,
        "sha256": content_sha256,
        "hash_provenance": provenance,
        "locator": {
            "path": str(resolved),
            "size_bytes": int(stat.st_size),
            "mtime_ns": int(stat.st_mtime_ns),
        },
    }


def missing_file_reference(
    path: Path,
    *,
    role: str,
    logical_source: str | None = None,
) -> dict[str, Any]:
    resolved = Path(path).expanduser().resolve()
    source_name = str(logical_source or f"{role}/{resolved.name}").strip()
    missing_sha256 = canonical_sha256(
        {"state": "missing", "role": str(role), "logical_source": source_name}
    )
    return {
        "schema_version": SOURCE_REFERENCE_SCHEMA_VERSION,
        "role": str(role),
        "logical_source": source_name,
        "sha256": missing_sha256,
        "hash_provenance": {"kind": "missing_source_sentinel"},
        "locator": {
            "path": str(resolved),
            "size_bytes": -1,
            "mtime_ns": -1,
        },
    }


def stable_reference_identity(reference: Mapping[str, Any]) -> dict[str, str]:
    return {
        "role": str(reference["role"]),
        "logical_source": str(reference["logical_source"]),
        "sha256": _validate_sha256(str(reference["sha256"]), field="source sha256"),
    }


def references_sha256(references: Sequence[Mapping[str, Any]]) -> str:
    normalized = [stable_reference_identity(reference) for reference in references]
    return canonical_sha256(
        sorted(
            normalized,
            key=lambda item: (item["role"], item["logical_source"], item["sha256"]),
        )
    )


def _schema_sha256(schema: Mapping[str, Any]) -> str:
    return canonical_sha256(dict(schema))


MARKET_CONTEXT_SCHEMA_SHA256 = _schema_sha256(MARKET_CONTEXT_SCHEMA)
MODEL_OVERLAY_SCHEMA_SHA256 = _schema_sha256(MODEL_OVERLAY_SCHEMA)


def _strategy_field_matches(names: Sequence[str]) -> list[str]:
    forbidden_tokens = (
        "strategy_order",
        "client_order",
        "order_id",
        "queue_ahead",
        "queue_position",
        "realized_fill",
        "executed_fill",
        "remaining_qty",
        "inventory_btc",
        "inventory_units",
        "cooldown_",
        "campaign_",
        "reward_",
        "markout_",
        "terminal_pnl",
    )
    return sorted(
        name
        for name in (str(value).strip().lower() for value in names)
        if any(token in name for token in forbidden_tokens)
    )


def _reject_strategy_fields(names: Sequence[str], *, component: str) -> None:
    matches = _strategy_field_matches(names)
    if matches:
        raise ValueError(f"{component} cannot persist strategy-dependent fields: {matches}")


@dataclass(frozen=True)
class MarketContextPayload:
    trades: pd.DataFrame
    var_ts_ms: np.ndarray
    var_ssq: np.ndarray
    var_ti: np.ndarray | None
    var_retsq: np.ndarray | None
    metadata: dict[str, Any]
    source_references: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class ComponentArtifact:
    directory: Path
    manifest_path: Path
    identity_sha256: str
    cache_hit: bool


def _array_parity(expected: Any, actual: Any) -> dict[str, Any]:
    if expected is None or actual is None:
        return {
            "passed": expected is None and actual is None,
            "expected_none": expected is None,
            "actual_none": actual is None,
        }
    left = np.asarray(expected)
    right = np.asarray(actual)
    same_dtype = left.dtype == right.dtype
    same_shape = left.shape == right.shape
    same_values = bool(same_shape and np.array_equal(left, right, equal_nan=True))
    return {
        "passed": bool(same_dtype and same_shape and same_values),
        "expected_dtype": str(left.dtype),
        "actual_dtype": str(right.dtype),
        "expected_shape": list(left.shape),
        "actual_shape": list(right.shape),
        "same_values": same_values,
    }


def market_context_parity_report(
    expected: MarketContextPayload,
    actual: MarketContextPayload,
) -> dict[str, Any]:
    """Compare every persisted market-context field without coercion."""

    try:
        pd.testing.assert_frame_equal(
            expected.trades,
            actual.trades,
            check_dtype=True,
            check_exact=True,
        )
        trades_passed = True
        trades_error = ""
    except AssertionError as exc:
        trades_passed = False
        trades_error = str(exc)
    arrays = {
        name: _array_parity(getattr(expected, name), getattr(actual, name))
        for name in MARKET_CONTEXT_SCHEMA["rolling_fields"]
    }
    metadata_passed = expected.metadata == actual.metadata
    references_passed = references_sha256(expected.source_references) == references_sha256(
        actual.source_references
    )
    passed = bool(
        trades_passed
        and all(result["passed"] for result in arrays.values())
        and metadata_passed
        and references_passed
    )
    return {
        "schema_version": "narrowgate.market_context_parity.v1",
        "passed": passed,
        "trades": {
            "passed": trades_passed,
            "error": trades_error,
            "expected_rows": int(len(expected.trades)),
            "actual_rows": int(len(actual.trades)),
            "expected_columns": list(map(str, expected.trades.columns)),
            "actual_columns": list(map(str, actual.trades.columns)),
        },
        "rolling_arrays": arrays,
        "metadata_passed": metadata_passed,
        "source_references_passed": references_passed,
    }


def model_overlay_parity_report(expected: Any, actual: Any) -> dict[str, Any]:
    """Compare all prediction and replay-feature arrays in one overlay."""

    expected_arrays, expected_layout = _overlay_arrays(expected)
    actual_arrays, actual_layout = _overlay_arrays(actual)
    names = sorted(set(expected_arrays).union(actual_arrays))
    fields = {
        name: (
            _array_parity(expected_arrays[name], actual_arrays[name])
            if name in expected_arrays and name in actual_arrays
            else {"passed": False, "missing": True}
        )
        for name in names
    }
    layout_passed = expected_layout == actual_layout
    return {
        "schema_version": "narrowgate.model_overlay_parity.v1",
        "passed": bool(layout_passed and all(field["passed"] for field in fields.values())),
        "layout_passed": layout_passed,
        "fields": fields,
    }


def market_context_identity(
    *,
    symbol: str,
    day: str,
    warmup_days: int,
    source_references: Sequence[Mapping[str, Any]],
    book_source_authority: str,
    book_dataset_version: str,
    transform_identity_sha256: str,
) -> dict[str, Any]:
    if len(str(transform_identity_sha256)) != 64:
        raise ValueError("market-context transform identity must be SHA256")
    return {
        "schema_version": MARKET_CONTEXT_SCHEMA_VERSION,
        "schema_sha256": MARKET_CONTEXT_SCHEMA_SHA256,
        "dag_identity_sha256": REPLAY_WINDOW_CACHE_GRAPH_V2_IDENTITY,
        "dag_node": "market_context_day_v2",
        "symbol": str(symbol).upper(),
        "day": str(day),
        "warmup_days": int(warmup_days),
        "source_references_sha256": references_sha256(source_references),
        "book_source_authority": str(book_source_authority),
        "book_dataset_version": str(book_dataset_version),
        "transform_identity_sha256": str(transform_identity_sha256),
    }


def model_overlay_identity(
    *,
    symbol: str,
    day: str,
    market_context_identity_sha256: str,
    feature_source_identity: Sequence[Mapping[str, Any]],
    model_bundle_identity: Sequence[Mapping[str, Any]],
    toxicity_horizon_s: int,
    cross_market_enabled: bool,
    run_ml_inference: bool,
) -> dict[str, Any]:
    if len(str(market_context_identity_sha256)) != 64:
        raise ValueError("model overlay requires a market-context SHA256")
    return {
        "schema_version": MODEL_OVERLAY_SCHEMA_VERSION,
        "schema_sha256": MODEL_OVERLAY_SCHEMA_SHA256,
        "dag_identity_sha256": REPLAY_WINDOW_CACHE_GRAPH_V2_IDENTITY,
        "dag_node": "model_overlay_day",
        "symbol": str(symbol).upper(),
        "day": str(day),
        "market_context_identity_sha256": str(market_context_identity_sha256),
        "feature_source_identity_sha256": references_sha256(feature_source_identity),
        "model_bundle_identity_sha256": references_sha256(model_bundle_identity),
        "toxicity_horizon_s": int(toxicity_horizon_s),
        "cross_market_enabled": bool(cross_market_enabled),
        "run_ml_inference": bool(run_ml_inference),
    }


def component_directory(
    cache_root: Path,
    *,
    namespace: str,
    symbol: str,
    day: str,
    identity_sha256: str,
) -> Path:
    return (
        Path(cache_root).expanduser().resolve()
        / "components_v2"
        / str(namespace)
        / str(symbol).lower()
        / str(day)
        / str(identity_sha256)
    )


def _logical_component_directory(
    cache_root: Path,
    *,
    namespace: str,
    symbol: str,
    day: str,
    identity_sha256: str,
) -> Path:
    return (
        Path(cache_root).expanduser().absolute()
        / "components_v2"
        / str(namespace)
        / str(symbol).lower()
        / str(day)
        / str(identity_sha256)
    )


def _record_component_hit(path: Path, *, identity_sha256: str) -> None:
    with suppress(Exception):
        record_cache_access(path, identity_sha256=identity_sha256)


def _register_component_write(path: Path, *, identity_sha256: str) -> None:
    with suppress(Exception):
        register_cache_write(path, identity_sha256=identity_sha256)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(dict(payload), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _file_record(path: Path) -> dict[str, Any]:
    return {
        "size_bytes": int(path.stat().st_size),
        "sha256": file_sha256(path),
    }


def _validate_source_references(references: Sequence[Mapping[str, Any]]) -> None:
    for reference in references:
        stable_reference_identity(reference)
        locator = dict(reference["locator"])
        path = Path(str(locator["path"])).expanduser().resolve()
        expected_size = int(locator["size_bytes"])
        if expected_size < 0:
            if path.exists():
                raise ReplayCacheIntegrityError(
                    f"previously missing source reference now exists: {path}"
                )
            continue
        if not path.is_file():
            raise ReplayCacheIntegrityError(f"source reference is missing: {path}")
        stat = path.stat()
        if expected_size != int(stat.st_size):
            raise ReplayCacheIntegrityError(f"source reference size changed: {path}")


def _validate_manifest(
    directory: Path,
    *,
    schema_version: str,
    schema_sha256: str,
    identity: Mapping[str, Any],
) -> dict[str, Any]:
    manifest_path = directory / "manifest.json"
    if not manifest_path.is_file():
        raise ReplayCacheIntegrityError(f"component manifest missing: {directory}")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ReplayCacheIntegrityError(
            f"component manifest is unreadable: {manifest_path}"
        ) from exc
    identity_sha256 = canonical_sha256(dict(identity))
    if manifest.get("schema_version") != schema_version:
        raise ReplayCacheIntegrityError("component schema version mismatch")
    if manifest.get("schema_sha256") != schema_sha256:
        raise ReplayCacheIntegrityError("component schema SHA256 mismatch")
    if manifest.get("identity") != dict(identity):
        raise ReplayCacheIntegrityError("component identity payload mismatch")
    if manifest.get("identity_sha256") != identity_sha256:
        raise ReplayCacheIntegrityError("component identity SHA256 mismatch")
    if directory.name != identity_sha256:
        raise ReplayCacheIntegrityError("component directory identity mismatch")
    for filename, expected in dict(manifest.get("files", {})).items():
        path = directory / str(filename)
        if not path.is_file():
            raise ReplayCacheIntegrityError(f"component file missing: {path}")
        if int(expected["size_bytes"]) != int(path.stat().st_size):
            raise ReplayCacheIntegrityError(f"component file size mismatch: {path}")
        if str(expected["sha256"]) != file_sha256(path):
            raise ReplayCacheIntegrityError(f"component file SHA256 mismatch: {path}")
    return manifest


def _publish_directory(
    directory: Path,
    *,
    build: Any,
) -> None:
    directory.parent.mkdir(parents=True, exist_ok=True)
    lock_path = directory.parent / f".{directory.name}.lock"
    with lock_path.open("a+b") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        if directory.exists():
            return
        temporary = directory.parent / (
            f".{directory.name}.{os.getpid()}.{uuid.uuid4().hex}.partial"
        )
        temporary.mkdir()
        try:
            build(temporary)
            os.replace(temporary, directory)
        finally:
            if temporary.exists():
                for child in temporary.iterdir():
                    child.unlink()
                temporary.rmdir()


def write_market_context(
    *,
    cache_root: Path,
    identity: Mapping[str, Any],
    payload: MarketContextPayload,
) -> ComponentArtifact:
    _reject_strategy_fields(
        [*map(str, payload.trades.columns), *map(str, payload.metadata)],
        component="market_context_day_v2",
    )
    identity_payload = dict(identity)
    if identity_payload.get("source_references_sha256") != references_sha256(
        payload.source_references
    ):
        raise ValueError("market-context source references do not match identity")
    _validate_source_references(payload.source_references)
    identity_sha256 = canonical_sha256(identity_payload)
    directory = component_directory(
        cache_root,
        namespace="market_context_day_v2",
        symbol=str(identity_payload["symbol"]),
        day=str(identity_payload["day"]),
        identity_sha256=identity_sha256,
    )
    logical_directory = _logical_component_directory(
        cache_root,
        namespace="market_context_day_v2",
        symbol=str(identity_payload["symbol"]),
        day=str(identity_payload["day"]),
        identity_sha256=identity_sha256,
    )
    if directory.exists():
        _validate_manifest(
            directory,
            schema_version=MARKET_CONTEXT_SCHEMA_VERSION,
            schema_sha256=MARKET_CONTEXT_SCHEMA_SHA256,
            identity=identity_payload,
        )
        _record_component_hit(logical_directory, identity_sha256=identity_sha256)
        return ComponentArtifact(directory, directory / "manifest.json", identity_sha256, True)

    def build(temporary: Path) -> None:
        trades_path = temporary / "trades.parquet"
        arrays_path = temporary / "rolling_arrays.npz"
        refs_path = temporary / "source_references.json"
        payload.trades.to_parquet(
            trades_path,
            index=True,
            compression="zstd",
        )
        arrays: dict[str, np.ndarray] = {
            "var_ts_ms": np.asarray(payload.var_ts_ms),
            "var_ssq": np.asarray(payload.var_ssq),
        }
        optional_fields: dict[str, bool] = {}
        for name, value in (
            ("var_ti", payload.var_ti),
            ("var_retsq", payload.var_retsq),
        ):
            optional_fields[name] = value is not None
            if value is not None:
                arrays[name] = np.asarray(value)
        np.savez_compressed(arrays_path, **arrays)
        refs_payload = {
            "schema_version": SOURCE_REFERENCE_SCHEMA_VERSION,
            "references": [dict(item) for item in payload.source_references],
            "references_sha256": references_sha256(payload.source_references),
        }
        _write_json(refs_path, refs_payload)
        manifest = {
            "schema_version": MARKET_CONTEXT_SCHEMA_VERSION,
            "schema_sha256": MARKET_CONTEXT_SCHEMA_SHA256,
            "identity": identity_payload,
            "identity_sha256": identity_sha256,
            "metadata": dict(payload.metadata),
            "optional_arrays_present": optional_fields,
            "trade_rows": int(len(payload.trades)),
            "files": {
                path.name: _file_record(path) for path in (trades_path, arrays_path, refs_path)
            },
        }
        _write_json(temporary / "manifest.json", manifest)

    _publish_directory(directory, build=build)
    _validate_manifest(
        directory,
        schema_version=MARKET_CONTEXT_SCHEMA_VERSION,
        schema_sha256=MARKET_CONTEXT_SCHEMA_SHA256,
        identity=identity_payload,
    )
    _register_component_write(logical_directory, identity_sha256=identity_sha256)
    return ComponentArtifact(directory, directory / "manifest.json", identity_sha256, False)


def load_market_context(
    *,
    cache_root: Path,
    identity: Mapping[str, Any],
    source_references: Sequence[Mapping[str, Any]] | None = None,
) -> MarketContextPayload | None:
    identity_payload = dict(identity)
    identity_sha256 = canonical_sha256(identity_payload)
    directory = component_directory(
        cache_root,
        namespace="market_context_day_v2",
        symbol=str(identity_payload["symbol"]),
        day=str(identity_payload["day"]),
        identity_sha256=identity_sha256,
    )
    logical_directory = _logical_component_directory(
        cache_root,
        namespace="market_context_day_v2",
        symbol=str(identity_payload["symbol"]),
        day=str(identity_payload["day"]),
        identity_sha256=identity_sha256,
    )
    if not directory.exists():
        return None
    manifest = _validate_manifest(
        directory,
        schema_version=MARKET_CONTEXT_SCHEMA_VERSION,
        schema_sha256=MARKET_CONTEXT_SCHEMA_SHA256,
        identity=identity_payload,
    )
    refs_payload = json.loads((directory / "source_references.json").read_text(encoding="utf-8"))
    stored_references = tuple(dict(item) for item in refs_payload["references"])
    if refs_payload.get("references_sha256") != references_sha256(stored_references):
        raise ReplayCacheIntegrityError("source-reference SHA256 mismatch")
    current_references = tuple(dict(item) for item in (source_references or stored_references))
    if identity_payload["source_references_sha256"] != references_sha256(current_references):
        raise ReplayCacheIntegrityError("source references do not match component identity")
    _validate_source_references(current_references)
    with np.load(directory / "rolling_arrays.npz", allow_pickle=False) as arrays:
        optional = dict(manifest["optional_arrays_present"])
        payload = MarketContextPayload(
            trades=pd.read_parquet(directory / "trades.parquet"),
            var_ts_ms=np.array(arrays["var_ts_ms"], copy=True),
            var_ssq=np.array(arrays["var_ssq"], copy=True),
            var_ti=(np.array(arrays["var_ti"], copy=True) if optional.get("var_ti") else None),
            var_retsq=(
                np.array(arrays["var_retsq"], copy=True) if optional.get("var_retsq") else None
            ),
            metadata=dict(manifest["metadata"]),
            source_references=current_references,
        )
    _record_component_hit(logical_directory, identity_sha256=identity_sha256)
    return payload


def _overlay_arrays(ml_data: Any) -> tuple[dict[str, np.ndarray], dict[str, Any]]:
    if not isinstance(ml_data, tuple):
        raise TypeError("model overlay cache requires a tuple payload")
    arrays: dict[str, np.ndarray] = {}
    main_values = ml_data
    feature_mapping: Mapping[str, Any] | None = None
    if ml_data and isinstance(ml_data[-1], Mapping):
        main_values = ml_data[:-1]
        feature_mapping = ml_data[-1]
    for index, value in enumerate(main_values):
        array = np.asarray(value)
        if array.dtype.hasobject:
            raise TypeError("model overlay cannot persist object arrays")
        arrays[f"main_{index:03d}"] = array
    feature_keys: list[str] = []
    if feature_mapping is not None:
        feature_keys = sorted(str(key) for key in feature_mapping)
        _reject_strategy_fields(feature_keys, component="model_overlay_day")
        for index, key in enumerate(feature_keys):
            array = np.asarray(feature_mapping[key])
            if array.dtype.hasobject:
                raise TypeError("model overlay cannot persist object arrays")
            arrays[f"feature_{index:04d}"] = array
    return arrays, {
        "main_count": len(main_values),
        "feature_mapping_present": feature_mapping is not None,
        "feature_keys": feature_keys,
    }


def write_model_overlay(
    *,
    cache_root: Path,
    identity: Mapping[str, Any],
    ml_data: Any,
) -> ComponentArtifact:
    identity_payload = dict(identity)
    identity_sha256 = canonical_sha256(identity_payload)
    directory = component_directory(
        cache_root,
        namespace="model_overlay_day",
        symbol=str(identity_payload["symbol"]),
        day=str(identity_payload["day"]),
        identity_sha256=identity_sha256,
    )
    logical_directory = _logical_component_directory(
        cache_root,
        namespace="model_overlay_day",
        symbol=str(identity_payload["symbol"]),
        day=str(identity_payload["day"]),
        identity_sha256=identity_sha256,
    )
    if directory.exists():
        _validate_manifest(
            directory,
            schema_version=MODEL_OVERLAY_SCHEMA_VERSION,
            schema_sha256=MODEL_OVERLAY_SCHEMA_SHA256,
            identity=identity_payload,
        )
        _record_component_hit(logical_directory, identity_sha256=identity_sha256)
        return ComponentArtifact(directory, directory / "manifest.json", identity_sha256, True)
    arrays, layout = _overlay_arrays(ml_data)

    def build(temporary: Path) -> None:
        arrays_path = temporary / "model_overlay.npz"
        np.savez_compressed(arrays_path, **arrays)
        manifest = {
            "schema_version": MODEL_OVERLAY_SCHEMA_VERSION,
            "schema_sha256": MODEL_OVERLAY_SCHEMA_SHA256,
            "identity": identity_payload,
            "identity_sha256": identity_sha256,
            "layout": layout,
            "files": {arrays_path.name: _file_record(arrays_path)},
        }
        _write_json(temporary / "manifest.json", manifest)

    _publish_directory(directory, build=build)
    _validate_manifest(
        directory,
        schema_version=MODEL_OVERLAY_SCHEMA_VERSION,
        schema_sha256=MODEL_OVERLAY_SCHEMA_SHA256,
        identity=identity_payload,
    )
    _register_component_write(logical_directory, identity_sha256=identity_sha256)
    return ComponentArtifact(directory, directory / "manifest.json", identity_sha256, False)


def load_model_overlay(
    *,
    cache_root: Path,
    identity: Mapping[str, Any],
) -> Any | None:
    identity_payload = dict(identity)
    identity_sha256 = canonical_sha256(identity_payload)
    directory = component_directory(
        cache_root,
        namespace="model_overlay_day",
        symbol=str(identity_payload["symbol"]),
        day=str(identity_payload["day"]),
        identity_sha256=identity_sha256,
    )
    logical_directory = _logical_component_directory(
        cache_root,
        namespace="model_overlay_day",
        symbol=str(identity_payload["symbol"]),
        day=str(identity_payload["day"]),
        identity_sha256=identity_sha256,
    )
    if not directory.exists():
        return None
    manifest = _validate_manifest(
        directory,
        schema_version=MODEL_OVERLAY_SCHEMA_VERSION,
        schema_sha256=MODEL_OVERLAY_SCHEMA_SHA256,
        identity=identity_payload,
    )
    layout = dict(manifest["layout"])
    with np.load(directory / "model_overlay.npz", allow_pickle=False) as arrays:
        values: list[Any] = [
            np.array(arrays[f"main_{index:03d}"], copy=True)
            for index in range(int(layout["main_count"]))
        ]
        if layout.get("feature_mapping_present"):
            values.append(
                {
                    key: np.array(arrays[f"feature_{index:04d}"], copy=True)
                    for index, key in enumerate(layout["feature_keys"])
                }
            )
    _record_component_hit(logical_directory, identity_sha256=identity_sha256)
    return tuple(values)
