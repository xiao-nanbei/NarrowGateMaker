#!/usr/bin/env python3
"""Emit authoritative journal-v2 tapes for the frozen F07 40-day panel.

This runner is execution-only.  It deliberately isolates every UTC day in a
fresh Python process, injects the existing replay journal adapter into the
authoritative Python tick replay, and publishes only lifecycle mechanics.  It
never serializes replay PnL, rewards, markouts, or campaign economics.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import json
import os
import shutil
import subprocess
import sys
import uuid
from collections import Counter, defaultdict
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[4]
IDENTITY = "f07_order_lifecycle_v2_40day_replay_emitter_v1_5"
PLAN_SCHEMA_VERSION = "f07_order_lifecycle_v2_40day_replay_plan.v1.5"
DAY_MANIFEST_SCHEMA_VERSION = "f07_order_lifecycle_v2_replay_day_manifest.v1.5"
PANEL_MANIFEST_SCHEMA_VERSION = "f07_order_lifecycle_v2_replay_panel_manifest.v1.5"
WINDOW_CACHE_INDEX_SCHEMA_VERSION = "f07_order_lifecycle_v2_window_cache_index.v1"
WORKER_MODULE = (
    "research.families.f07_active_order_continuation.audit."
    "order_lifecycle_v2_40day_replay_emitter"
)

DEFAULT_FROZEN_V1 = (
    ROOT / "research/families/f07_active_order_continuation/docs/"
    "order_lifecycle_v2_40day_input_admission_v1_contract_20260805.json"
)

_FORBIDDEN_ECONOMIC_FRAGMENTS = (
    "pnl",
    "reward",
    "markout",
    "campaign_economic",
    "profit",
    "value_usdc",
)
_ALLOWED_ECONOMIC_GOVERNANCE_KEYS = frozenset({"economic_outcomes_read"})
_EXCHANGE_CLOCK_EVENTS = frozenset(
    {"activate", "cancel_rejected", "partial_fill", "full_fill", "exchange_terminal"}
)
_TERMINAL_OBSERVATIONS = frozenset({"EXCHANGE_TERMINAL", "LOCAL_SHUTDOWN_CENSOR"})
_DAY_MANIFEST_KEYS = frozenset(
    {
        "schema_version",
        "identity",
        "day",
        "plan_sha256",
        "day_execution_identity_sha256",
        "status",
        "atomic_publish_method",
        "replay",
        "bindings",
        "journal_v2",
        "scope",
        "permissions",
        "canonical_manifest_sha256",
    }
)
_PANEL_MANIFEST_KEYS = frozenset(
    {
        "schema_version",
        "identity",
        "status",
        "generated_at_utc",
        "plan_sha256",
        "ordered_utc_days",
        "day_manifests",
        "mechanics_totals",
        "scope",
        "permissions",
        "canonical_manifest_sha256",
    }
)
_DAY_BINDING_KEYS = frozenset(
    {
        "global_execution_identity_sha256",
        "daily_source_identity_sha256",
        "config_sha256",
        "model_bundle_sha256",
        "model_overlay_identity_sha256",
        "p3_sha256",
        "feature_dag_semantic_sha256",
        "runtime_code_identity_sha256",
        "cpp_abi_version",
        "cpp_module_sha256",
        "latency_profile_sha256",
    }
)
_JOURNAL_MANIFEST_KEYS = frozenset(
    {
        "session_root",
        "writer_runtime_identity_sha256",
        "runtime_identity_artifact",
        "health_artifact",
        "part_manifest_artifacts",
        "part_data_artifacts",
        "row_count",
        "writer",
        "dual_clock",
        "cif_eligibility",
        "counters",
    }
)
_WRITER_KEYS = frozenset(
    {
        "rows_committed",
        "callbacks_committed",
        "rows_dropped",
        "error_count",
        "closed",
        "formal_collection_valid",
    }
)
_DUAL_CLOCK_KEYS = frozenset(
    {
        "required_exchange_event_count",
        "missing_exchange_clock_count",
        "exchange_after_visibility_count",
        "invalid_exchange_exposure_count",
        "passed",
    }
)
_COUNTER_KEYS = frozenset(
    {
        "lifecycle_count",
        "event_count",
        "terminal_observation_count",
        "event_counts",
        "terminal_reason_counts",
        "cancel_reject_count",
        "cancel_reject_to_active_count",
        "cancel_reject_to_partially_filled_count",
        "sub_lot_partial_remaining_count",
        "full_fill_exact_zero_count",
        "terminal_positive_remainder_count",
        "exact_native_lifecycle_count",
        "native_queue_censored_lifecycle_count",
    }
)
_CIF_ELIGIBILITY_KEYS = frozenset(
    {
        "rule",
        "eligible_lifecycle_count",
        "censored_lifecycle_count",
        "censor_reason_counts",
    }
)
_DAY_SCOPE_KEYS = frozenset(
    {
        "mechanics_only",
        "economic_outcomes_read",
        "formal_40day_replay_executed",
        "formal_40day_lockstep_executed",
    }
)
_PERMISSION_KEYS = frozenset(
    {
        "cif_training",
        "economic_evaluation",
        "q90_action",
        "live_transport",
        "live_deployment",
    }
)
_CODE_PATHS = (
    "execution/order_lifecycle.py",
    "execution/order_lifecycle_journal_v2_strict_native.py",
    "execution/order_lifecycle_journal_writer_v2_strict_native.py",
    "execution/order_lifecycle_journal_writer_v2_replay_day_buffered.py",
    "execution/order_lifecycle_quantity_contract.py",
    "models/backtest_config.py",
    "models/backtest_tick.py",
    "models/data_windows.py",
    "models/exchange_book_replay.py",
    "models/replay/order_lifecycle_v2_replay_adapter_strict_native.py",
    "models/replay_contract.py",
    "strategy/fill_cooldown.py",
    "strategy/policy_guards.py",
    "strategy/quote_core.py",
    "strategy/replay_controls.py",
    "strategy/signal.py",
    "cpp/narrowgate_cpp/bindings_research.cpp",
    "research/families/f07_active_order_continuation/cpp/order_lifecycle_journal_v2_mirror.cpp",
    "research/families/f07_active_order_continuation/cpp/order_lifecycle_journal_v2_mirror.hpp",
    "research/families/f07_active_order_continuation/audit/"
    "order_lifecycle_v2_cpp_event_stream_binding_v2.py",
    "research/families/f07_active_order_continuation/audit/"
    "order_lifecycle_v2_40day_replay_emitter.py",
)

_OVERLAY_GAP_POLICY = {
    "canonical_cadence_ms": 10_000,
    "maximum_supported_gap_ms": 20_000,
    "policy": "causal_sample_hold_previous_prediction_up_to_20s",
    "larger_gap": "fail_closed_day",
}


class ReplayEmitterError(RuntimeError):
    """A fail-closed execution-plan, replay, or publication error."""


CommandRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]


def canonical_sha256(value: object) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def canonical_document_sha256(value: Mapping[str, object], hash_field: str) -> str:
    payload = dict(value)
    payload.pop(hash_field, None)
    return canonical_sha256(payload)


def file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _array_fingerprint(value: object) -> dict[str, object]:
    import numpy as np

    array = np.asarray(value)
    if array.dtype.hasobject:
        raise ReplayEmitterError("object arrays cannot enter a replay fingerprint")
    contiguous = np.ascontiguousarray(array)
    payload = {
        "dtype": str(contiguous.dtype),
        "shape": list(contiguous.shape),
        "sha256": hashlib.sha256(contiguous.tobytes(order="C")).hexdigest(),
    }
    payload["identity_sha256"] = canonical_sha256(payload)
    return payload


def _book_payload_fingerprint(value: object, *, kind: str) -> dict[str, object]:
    fields = {
        "bbo": ("ts_ms", "best_bid", "best_ask", "bid_qty", "ask_qty"),
        "l2": ("ts_ms", "bid_px", "bid_qty", "ask_px", "ask_qty"),
    }
    if kind not in fields or value is None:
        raise ReplayEmitterError(f"missing or unsupported {kind} payload")
    arrays = {
        name: _array_fingerprint(getattr(value, name)) for name in fields[kind]
    }
    payload = {
        "kind": kind,
        "source": str(getattr(value, "source", "")),
        "arrays": arrays,
    }
    payload["identity_sha256"] = canonical_sha256(payload)
    return payload


def _book_leaf_identity(
    references: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    leaves = [
        {
            "role": str(item["role"]),
            "logical_source": str(item["logical_source"]),
            "sha256": str(item["sha256"]),
        }
        for item in references
        if str(item.get("role", "")) in {"normalized_bbo", "normalized_l2"}
    ]
    leaves.sort(key=lambda item: (item["role"], item["logical_source"]))
    if not leaves or {item["role"] for item in leaves} != {
        "normalized_bbo",
        "normalized_l2",
    }:
        raise ReplayEmitterError("market context lacks bound BBO/L2 leaf identities")
    payload = {"leaves": leaves}
    payload["identity_sha256"] = canonical_sha256(payload)
    return payload


def _validate_overlay_payload(
    overlay: object,
    *,
    day: str,
    required_feature_keys: Sequence[str],
) -> dict[str, object]:
    import numpy as np

    if not isinstance(overlay, tuple) or len(overlay) != 22:
        raise ReplayEmitterError(f"{day}: operational ML overlay payload differs")
    main = overlay[:-1]
    feature_map = overlay[-1]
    if not isinstance(feature_map, Mapping):
        raise ReplayEmitterError(f"{day}: overlay feature map is missing")
    timestamps = np.asarray(main[0])
    if timestamps.ndim != 1 or timestamps.dtype.kind not in {"i", "u"}:
        raise ReplayEmitterError(f"{day}: overlay canonical timestamps differ")
    if timestamps.size == 0 or np.any(np.diff(timestamps) <= 0):
        raise ReplayEmitterError(f"{day}: overlay timestamps are empty/duplicated")
    cadence = int(_OVERLAY_GAP_POLICY["canonical_cadence_ms"])
    gaps = np.diff(timestamps.astype(np.int64, copy=False))
    if np.any(timestamps % cadence != 0) or np.any(gaps % cadence != 0):
        raise ReplayEmitterError(f"{day}: overlay timestamps are off canonical 10s grid")
    max_gap = int(gaps.max(initial=cadence))
    if max_gap > int(_OVERLAY_GAP_POLICY["maximum_supported_gap_ms"]):
        raise ReplayEmitterError(f"{day}: overlay prediction gap exceeds frozen policy")
    rows = int(timestamps.size)
    main_fingerprints: dict[str, object] = {}
    for index, value in enumerate(main):
        array = np.asarray(value)
        if array.ndim != 1 or int(array.size) != rows:
            raise ReplayEmitterError(f"{day}: overlay main_{index:03d} row alignment differs")
        if not np.issubdtype(array.dtype, np.number) or not bool(np.isfinite(array).all()):
            raise ReplayEmitterError(f"{day}: overlay main_{index:03d} is non-finite")
        main_fingerprints[f"main_{index:03d}"] = _array_fingerprint(array)

    feature_keys = sorted(map(str, feature_map))
    required = sorted(set(map(str, required_feature_keys)))
    missing = sorted(set(required) - set(feature_keys))
    if missing:
        raise ReplayEmitterError(f"{day}: overlay feature map lacks required keys: {missing[:8]}")
    feature_nonfinite_counts: dict[str, int] = {}
    feature_fingerprints: dict[str, object] = {}
    for key in feature_keys:
        array = np.asarray(feature_map[key])
        if array.ndim != 1 or int(array.size) != rows or not np.issubdtype(
            array.dtype, np.number
        ):
            raise ReplayEmitterError(f"{day}: overlay feature {key} schema differs")
        finite = np.isfinite(array)
        if np.any(np.isinf(array)):
            raise ReplayEmitterError(f"{day}: overlay feature {key} contains infinity")
        feature_nonfinite_counts[key] = int((~finite).sum())
        feature_fingerprints[key] = _array_fingerprint(array)
    contract = {
        "row_count": rows,
        "canonical_timestamp_fingerprint": main_fingerprints["main_000"],
        "first_timestamp_ms": int(timestamps[0]),
        "last_timestamp_ms": int(timestamps[-1]),
        "gap_policy": dict(_OVERLAY_GAP_POLICY),
        "twenty_second_gap_count": int((gaps == 20_000).sum()),
        "maximum_observed_gap_ms": max_gap,
        "main_array_count": len(main),
        "main_arrays_finite": True,
        "main_array_fingerprints_sha256": canonical_sha256(main_fingerprints),
        "feature_map_key_count": len(feature_keys),
        "feature_map_keys_sha256": canonical_sha256(feature_keys),
        "required_feature_keys_sha256": canonical_sha256(required),
        "feature_map_nan_policy": "model_native_missing_allowed_in_features_only",
        "feature_nonfinite_counts_sha256": canonical_sha256(feature_nonfinite_counts),
        "feature_array_fingerprints_sha256": canonical_sha256(feature_fingerprints),
    }
    contract["identity_sha256"] = canonical_sha256(contract)
    return contract


def _is_sha256(value: object) -> bool:
    text = str(value)
    return len(text) == 64 and all(char in "0123456789abcdef" for char in text)


def _read_json(path: str | Path) -> dict[str, Any]:
    resolved = Path(path)
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ReplayEmitterError(f"invalid JSON artifact {resolved}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ReplayEmitterError(f"JSON object required: {resolved}")
    return payload


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.partial-{os.getpid()}-{uuid.uuid4().hex}")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(
                payload,
                handle,
                indent=2,
                sort_keys=True,
                ensure_ascii=True,
                allow_nan=False,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def artifact_identity(path: str | Path, *, relative_to: Path | None = None) -> dict[str, object]:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise ReplayEmitterError(f"bound artifact is missing: {resolved}")
    stored_path = (
        str(resolved.relative_to(relative_to.resolve()))
        if relative_to is not None and resolved.is_relative_to(relative_to.resolve())
        else str(resolved)
    )
    return {
        "path": stored_path,
        "size_bytes": int(resolved.stat().st_size),
        "sha256": file_sha256(resolved),
    }


def _resolve_artifact_path(value: Mapping[str, object], *, base: Path) -> Path:
    required = {"path", "size_bytes", "sha256"}
    if not required.issubset(value):
        raise ReplayEmitterError("artifact identity lacks path/size/SHA256")
    path = Path(str(value["path"])).expanduser()
    if not path.is_absolute():
        path = base / path
    path = path.resolve()
    if not path.is_file():
        raise ReplayEmitterError(f"artifact is missing: {path}")
    if int(path.stat().st_size) != int(value["size_bytes"]):
        raise ReplayEmitterError(f"artifact size differs: {path}")
    if file_sha256(path) != str(value["sha256"]):
        raise ReplayEmitterError(f"artifact SHA256 differs: {path}")
    return path


def _assert_mechanics_only(value: object, *, path: str = "root") -> None:
    if isinstance(value, Mapping):
        for raw_key, nested in value.items():
            key = str(raw_key)
            normalized = key.lower()
            if key not in _ALLOWED_ECONOMIC_GOVERNANCE_KEYS and any(
                fragment in normalized for fragment in _FORBIDDEN_ECONOMIC_FRAGMENTS
            ):
                raise ReplayEmitterError(f"economic field is forbidden: {path}.{key}")
            if key in _ALLOWED_ECONOMIC_GOVERNANCE_KEYS and bool(nested):
                raise ReplayEmitterError(f"economic permission must remain false: {path}.{key}")
            _assert_mechanics_only(nested, path=f"{path}.{key}")
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, nested in enumerate(value):
            _assert_mechanics_only(nested, path=f"{path}[{index}]")


def _utc_interval(day: str) -> dict[str, object]:
    start = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    warmup = start - timedelta(days=1)
    payload: dict[str, object] = {
        "day": day,
        "warmup_interval": {
            "start_utc": warmup.isoformat().replace("+00:00", "Z"),
            "end_utc": start.isoformat().replace("+00:00", "Z"),
            "denominator": False,
        },
        "target_interval": {
            "start_utc": start.isoformat().replace("+00:00", "Z"),
            "end_utc": end.isoformat().replace("+00:00", "Z"),
            "denominator": True,
        },
    }
    payload["interval_identity_sha256"] = canonical_sha256(payload)
    return payload


def load_frozen_v1_panel(path: str | Path = DEFAULT_FROZEN_V1) -> tuple[dict[str, Any], Path]:
    """Load v1 panel/baseline/source identity without reviving stale code hashes.

    v1.2 explicitly supersedes the original implementation authority.  The
    emitter therefore preserves and verifies v1's canonical bytes, ordered
    denominator, intervals, baseline, and source identities while binding the
    currently executed code in its own successor plan.
    """

    resolved = Path(path).expanduser().resolve()
    payload = _read_json(resolved)
    if payload.get("identity") != "f07_order_lifecycle_v2_40day_input_admission_v1":
        raise ReplayEmitterError("unexpected frozen v1 identity")
    claimed = str(payload.get("canonical_identity_sha256", ""))
    actual = canonical_document_sha256(payload, "canonical_identity_sha256")
    if claimed != actual:
        raise ReplayEmitterError("frozen v1 canonical identity SHA256 mismatch")
    panel = payload.get("panel")
    if not isinstance(panel, Mapping):
        raise ReplayEmitterError("frozen v1 panel is missing")
    days = list(map(str, panel.get("ordered_utc_days") or ()))
    if len(days) != 40 or days != sorted(set(days)):
        raise ReplayEmitterError("frozen v1 must contain 40 unique chronological days")
    expected_intervals = [_utc_interval(day) for day in days]
    if list(panel.get("day_intervals") or ()) != expected_intervals:
        raise ReplayEmitterError("frozen v1 day intervals changed")
    baseline = payload.get("baseline_runtime_identity")
    source = payload.get("market_data_source_identities")
    if not isinstance(baseline, Mapping) or not isinstance(source, Mapping):
        raise ReplayEmitterError("frozen v1 baseline/source identity is missing")
    if bool(baseline.get("q90_action_enabled")) or bool(baseline.get("economic_outcomes_read")):
        raise ReplayEmitterError("frozen v1 mechanics permissions drifted")
    if baseline.get("initial_state_mode") != "daily_fresh_start":
        raise ReplayEmitterError("frozen v1 initial state is not daily fresh_start")
    _assert_mechanics_only(payload.get("permissions") or {}, path="frozen_v1.permissions")
    return payload, resolved


def _resolve_repo_or_relocated_path(raw: object) -> Path:
    path = Path(str(raw)).expanduser()
    if not path.is_absolute():
        path = ROOT / path
    path = path.resolve()
    if path.exists():
        return path
    try:
        from data_paths import relocate_marketdata_path

        relocated = Path(relocate_marketdata_path(path)).expanduser().resolve()
    except Exception:
        return path
    return relocated


def _require_bound_file(raw: object, expected_sha256: object, *, label: str) -> Path:
    path = _resolve_repo_or_relocated_path(raw)
    if not path.is_file():
        raise ReplayEmitterError(f"{label} is missing: {path}")
    actual = file_sha256(path)
    if actual != str(expected_sha256):
        raise ReplayEmitterError(
            f"{label} SHA256 mismatch: expected={expected_sha256} actual={actual}"
        )
    return path


def _load_source_contract(frozen: Mapping[str, Any]) -> tuple[dict[str, Any], Path]:
    source = frozen["market_data_source_identities"]
    candidates = [
        item
        for item in source.get("artifacts", [])
        if str(item.get("path", "")).endswith(
            "volatility_time_add_rearm_full_path_preflight_v1_spec_20260729.json"
        )
    ]
    if len(candidates) != 1:
        raise ReplayEmitterError("frozen source replay contract is not uniquely bound")
    item = candidates[0]
    path = _require_bound_file(item["path"], item["sha256"], label="source contract")
    return _read_json(path), path


def _load_window_cache_index(path: str | Path | None) -> dict[str, Mapping[str, object]]:
    if path is None:
        return {}
    payload = _read_json(path)
    if payload.get("schema_version") != WINDOW_CACHE_INDEX_SCHEMA_VERSION:
        raise ReplayEmitterError("window cache index schema differs")
    rows = payload.get("days")
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
        raise ReplayEmitterError("window cache index days must be an array")
    result: dict[str, Mapping[str, object]] = {}
    for row in rows:
        if not isinstance(row, Mapping) or set(row) != {
            "day",
            "path",
            "size_bytes",
            "sha256",
        }:
            raise ReplayEmitterError("window cache index row schema differs")
        day = str(row["day"])
        if day in result:
            raise ReplayEmitterError(f"duplicate window cache day: {day}")
        result[day] = {key: row[key] for key in ("path", "size_bytes", "sha256")}
    return result


def _window_cache_artifact(
    *,
    root: Path,
    day: str,
    indexed: Mapping[str, Mapping[str, object]],
) -> dict[str, object]:
    if day in indexed:
        path = _resolve_artifact_path(indexed[day], base=ROOT)
        return artifact_identity(path)
    candidates = sorted(root.glob(f"btcusdc_{day}_tick_window_v13_*.pkl"))
    if len(candidates) != 1:
        raise ReplayEmitterError(
            f"{day}: expected exactly one v13 window cache, found {len(candidates)}; "
            "provide --window-cache-index to disambiguate"
        )
    return artifact_identity(candidates[0])


def _native_book_artifacts(
    *,
    raw_root: Path,
    day: str,
    tick_size: float,
    warmup_hours: int,
) -> list[dict[str, object]]:
    from models.exchange_book_replay import CryptoHFTExchangeBookTape

    tape = CryptoHFTExchangeBookTape(
        raw_root=raw_root,
        day=day,
        symbol="BTCUSDC",
        tick_size=float(tick_size),
        warmup_hours=int(warmup_hours),
        strict_complete=True,
    )
    target = day
    warmup = (datetime.strptime(day, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    rows: list[dict[str, object]] = []
    counts: Counter[str] = Counter()
    for path in tape.source_paths:
        path_text = str(path.resolve())
        if target in path_text:
            role = "native_book_target"
        elif warmup in path_text:
            role = "native_book_warmup"
        else:
            raise ReplayEmitterError(f"{day}: native tape contains an unrelated file: {path}")
        counts[role] += 1
        rows.append({"role": role, **artifact_identity(path)})
    if counts["native_book_warmup"] < 24 or counts["native_book_target"] < 24:
        raise ReplayEmitterError(
            f"{day}: native tape lacks 24-hour warmup/target coverage: {dict(counts)}"
        )
    return rows


def _current_code_artifacts() -> list[dict[str, object]]:
    rows = []
    for relative in _CODE_PATHS:
        path = (ROOT / relative).resolve()
        rows.append({"logical_path": relative, **artifact_identity(path)})
    return rows


def _compiled_cpp_binding() -> dict[str, object]:
    import narrowgate_cpp

    from research.families.f07_active_order_continuation.audit.order_lifecycle_v2_cpp_event_stream_binding_v2 import (
        CPP_EVENT_STREAM_MIRROR_ABI_VERSION,
    )

    module_path = Path(narrowgate_cpp.__file__).resolve()
    observed_abi = str(
        getattr(
            narrowgate_cpp,
            "ORDER_LIFECYCLE_JOURNAL_V2_MIRROR_ABI_VERSION",
            "",
        )
    )
    if observed_abi != CPP_EVENT_STREAM_MIRROR_ABI_VERSION:
        raise ReplayEmitterError(
            "loaded C++ lifecycle mirror ABI differs from Python authority"
        )
    return {
        "abi_version": observed_abi,
        "module_artifact": artifact_identity(module_path),
        "full_cpp_tick_replay_authority": False,
    }


def _independent_book_parity(
    *,
    day: str,
    loaded_window: object,
    context_references: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    from models import backtest_tick as bt

    warmup = (datetime.strptime(day, "%Y-%m-%d") - timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )
    context_days = [warmup, day]
    direct_bbo = bt.load_bbo_data(
        days=context_days,
        quality_allowed_days=context_days,
    )
    direct_l2 = bt.load_l2_data(
        days=context_days,
        quality_allowed_days=context_days,
    )
    window_bbo = _book_payload_fingerprint(loaded_window.bbo_data, kind="bbo")
    window_l2 = _book_payload_fingerprint(loaded_window.l2_data, kind="l2")
    direct_bbo_fingerprint = _book_payload_fingerprint(direct_bbo, kind="bbo")
    direct_l2_fingerprint = _book_payload_fingerprint(direct_l2, kind="l2")
    if window_bbo != direct_bbo_fingerprint or window_l2 != direct_l2_fingerprint:
        raise ReplayEmitterError(f"{day}: window BBO/L2 output differs from bound leaves")
    payload = {
        "leaf_identity": _book_leaf_identity(context_references),
        "bbo_output_fingerprint": window_bbo,
        "l2_output_fingerprint": window_l2,
        "independent_source_reload_parity": True,
    }
    payload["identity_sha256"] = canonical_sha256(payload)
    return payload


def _find_bound_feature_dir(
    *,
    day: str,
    expected_identity_sha256: str,
) -> tuple[Path, list[dict[str, object]]]:
    from data_paths import data_root
    from models import data_windows
    from models.replay_cache_components import references_sha256

    warmup = (datetime.strptime(day, "%Y-%m-%d") - timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )
    candidates = sorted(data_root().glob("features*"))
    matches: list[tuple[Path, list[dict[str, object]]]] = []
    for candidate in candidates:
        if not candidate.is_dir():
            continue
        target_path = candidate / f"features_{day}.parquet"
        manifest_path = candidate / "causal_feature_manifest.json"
        if not target_path.is_file() or not manifest_path.is_file():
            continue
        signatures = data_windows._glob_signatures(
            candidate,
            (
                f"features_{warmup}.parquet",
                f"features_{day}.parquet",
            ),
        )
        signatures.append(data_windows._file_signature(manifest_path))
        references = list(data_windows._signature_references(signatures))
        if references_sha256(references) == expected_identity_sha256:
            matches.append((candidate.resolve(), references))
    if len(matches) != 1:
        raise ReplayEmitterError(
            f"{day}: expected one bound feature directory, found {len(matches)}"
        )
    return matches[0]


def _independent_reinference_receipt(
    *,
    day: str,
    loaded_window: object,
    overlay: tuple[object, ...],
    overlay_identity: Mapping[str, object],
    model_bundle_artifact: Mapping[str, object],
) -> dict[str, object]:
    from models import backtest_tick as bt
    from models.replay_cache_components import model_overlay_parity_report

    feature_dir, feature_references = _find_bound_feature_dir(
        day=day,
        expected_identity_sha256=str(
            overlay_identity["feature_source_identity_sha256"]
        ),
    )
    rerun = bt.load_ml_predictions(
        loaded_window.trades,
        toxicity_horizon_s=int(overlay_identity["toxicity_horizon_s"]),
        cross_market_enabled=bool(overlay_identity["cross_market_enabled"]),
        allow_missing_features=False,
        run_model_inference=True,
        feature_dir=feature_dir,
        require_target_feature_files=True,
    )
    parity = model_overlay_parity_report(overlay, rerun)
    if not bool(parity.get("passed")):
        raise ReplayEmitterError(f"{day}: independent model re-inference parity failed")
    receipt = {
        "schema_version": "f07_model_overlay_generation_receipt.v1",
        "kind": "independent_reinference_exact_array_parity",
        "day": day,
        "feature_directory": str(feature_dir),
        "feature_references": feature_references,
        "feature_source_identity_sha256": overlay_identity[
            "feature_source_identity_sha256"
        ],
        "model_bundle_artifact": dict(model_bundle_artifact),
        "inference_code_artifact": artifact_identity(ROOT / "models/backtest_tick.py"),
        "parity_report_sha256": canonical_sha256(parity),
        "passed": True,
    }
    receipt["identity_sha256"] = canonical_sha256(receipt)
    return receipt


def _model_overlay_artifact(
    *,
    cache_root: Path,
    day: str,
    window_cache: Mapping[str, object],
    expected_model_dir: Path,
    required_feature_keys: Sequence[str],
    require_independent_reinference: bool,
) -> dict[str, object]:
    import numpy as np

    from models import backtest_tick as bt
    from models import data_windows
    from models.replay_cache_components import (
        MARKET_CONTEXT_SCHEMA_SHA256,
        MARKET_CONTEXT_SCHEMA_VERSION,
        MODEL_OVERLAY_SCHEMA_SHA256,
        MODEL_OVERLAY_SCHEMA_VERSION,
        load_market_context,
        load_model_overlay,
        references_sha256,
    )

    if Path(bt.MODEL_DIR).resolve() != expected_model_dir.resolve():
        raise ReplayEmitterError("configured model directory differs from frozen baseline")
    model_references = data_windows._signature_references(
        data_windows._model_artifact_signatures(expected_model_dir)
    )
    expected_model_identity = references_sha256(model_references)
    window_path = _resolve_artifact_path(window_cache, base=ROOT)
    loaded_window = data_windows._load_cached_window(window_path)
    if loaded_window is None:
        raise ReplayEmitterError(f"{day}: bound replay window cache is incompatible")

    candidates: list[dict[str, object]] = []
    day_root = cache_root / "components_v2/model_overlay_day/btcusdc" / day
    for manifest_path in sorted(day_root.glob("*/manifest.json")):
        manifest = _read_json(manifest_path)
        overlay_identity = manifest.get("identity")
        identity_sha = str(manifest.get("identity_sha256", ""))
        if not isinstance(overlay_identity, Mapping) or (
            manifest.get("schema_version") != MODEL_OVERLAY_SCHEMA_VERSION
            or manifest.get("schema_sha256") != MODEL_OVERLAY_SCHEMA_SHA256
            or canonical_sha256(overlay_identity) != identity_sha
            or overlay_identity.get("day") != day
            or overlay_identity.get("symbol") != "BTCUSDC"
            or not bool(overlay_identity.get("run_ml_inference"))
            or overlay_identity.get("model_bundle_identity_sha256")
            != expected_model_identity
        ):
            continue
        layout = manifest.get("layout")
        if not isinstance(layout, Mapping) or (
            int(layout.get("main_count", -1)) != 21
            or not bool(layout.get("feature_mapping_present"))
        ):
            continue
        data_path = manifest_path.parent / "model_overlay.npz"
        files = manifest.get("files")
        if not isinstance(files, Mapping) or set(files) != {data_path.name}:
            continue
        observed_file = artifact_identity(data_path)
        expected_file = files[data_path.name]
        if not isinstance(expected_file, Mapping) or (
            int(expected_file.get("size_bytes", -1)) != observed_file["size_bytes"]
            or str(expected_file.get("sha256", "")) != observed_file["sha256"]
        ):
            continue

        context_sha = str(overlay_identity.get("market_context_identity_sha256", ""))
        context_manifest_path = (
            cache_root
            / "components_v2/market_context_day_v2/btcusdc"
            / day
            / context_sha
            / "manifest.json"
        )
        if not context_manifest_path.is_file():
            continue
        context_manifest = _read_json(context_manifest_path)
        context_identity = context_manifest.get("identity")
        if not isinstance(context_identity, Mapping) or (
            context_manifest.get("schema_version") != MARKET_CONTEXT_SCHEMA_VERSION
            or context_manifest.get("schema_sha256") != MARKET_CONTEXT_SCHEMA_SHA256
            or context_manifest.get("identity_sha256") != context_sha
            or canonical_sha256(context_identity) != context_sha
        ):
            continue
        context = load_market_context(cache_root=cache_root, identity=context_identity)
        if context is None:
            continue
        metadata = context.metadata
        parity_passed = bool(
            loaded_window.trades.equals(context.trades)
            and np.array_equal(loaded_window.var_ts_ms, context.var_ts_ms, equal_nan=True)
            and np.array_equal(loaded_window.var_ssq, context.var_ssq, equal_nan=True)
            and np.array_equal(loaded_window.var_ti, context.var_ti, equal_nan=True)
            and np.array_equal(loaded_window.var_retsq, context.var_retsq, equal_nan=True)
            and metadata.get("execution_trade_source")
            == loaded_window.execution_trade_source
            and metadata.get("book_source_authority")
            == loaded_window.book_source_authority
            and metadata.get("book_dataset_version")
            == loaded_window.book_dataset_version
        )
        if not parity_passed:
            continue
        overlay = load_model_overlay(cache_root=cache_root, identity=overlay_identity)
        overlay_contract = _validate_overlay_payload(
            overlay,
            day=day,
            required_feature_keys=required_feature_keys,
        )
        book_parity = _independent_book_parity(
            day=day,
            loaded_window=loaded_window,
            context_references=context.source_references,
        )
        parity_identity = {
            "window_sha256": str(window_cache["sha256"]),
            "market_context_identity_sha256": context_sha,
            "exact_trades_and_rolling_arrays": True,
            "exact_bbo_l2_leaf_and_output_parity": True,
            "book_parity_identity_sha256": book_parity["identity_sha256"],
        }
        generation_receipt = (
            _independent_reinference_receipt(
                day=day,
                loaded_window=loaded_window,
                overlay=overlay,
                overlay_identity=overlay_identity,
                model_bundle_artifact=artifact_identity(
                    expected_model_dir / "bundle_meta.json"
                ),
            )
            if require_independent_reinference
            else {
                "schema_version": "f07_model_overlay_generation_receipt.v1",
                "kind": "bound_component_schema_and_payload_validation",
                "day": day,
                "passed": True,
            }
        )
        if "identity_sha256" not in generation_receipt:
            generation_receipt["identity_sha256"] = canonical_sha256(
                generation_receipt
            )
        candidates.append(
            {
                "cache_root": str(cache_root),
                "identity": dict(overlay_identity),
                "identity_sha256": identity_sha,
                "manifest": artifact_identity(manifest_path),
                "data": observed_file,
                "market_context_output_parity": {
                    **parity_identity,
                    "identity_sha256": canonical_sha256(parity_identity),
                },
                "book_leaf_output_parity": book_parity,
                "overlay_contract": overlay_contract,
                "generation_receipt": generation_receipt,
            }
        )
    if not candidates:
        raise ReplayEmitterError(f"{day}: no operational model overlay matches frozen window")
    payload_identities = {
        (
            str(item["data"]["sha256"]),
            str(item["identity"]["feature_source_identity_sha256"]),
            str(item["identity"]["model_bundle_identity_sha256"]),
        )
        for item in candidates
    }
    if len(payload_identities) != 1:
        raise ReplayEmitterError(f"{day}: multiple non-equivalent model overlays match")
    candidates.sort(key=lambda item: str(item["identity_sha256"]))
    selected = candidates[0]
    del loaded_window
    return selected


def _plan_day_identity(
    *,
    global_identity_sha256: str,
    day: str,
    interval: Mapping[str, object],
    window_cache: Mapping[str, object],
    model_overlay: Mapping[str, object],
    native_book_artifacts: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    source_sha = canonical_sha256(
        {
            "window_cache": dict(window_cache),
            "model_overlay": dict(model_overlay),
            "native_book_artifacts": [dict(item) for item in native_book_artifacts],
        }
    )
    runtime: dict[str, object] = {
        "schema_version": "f07_order_lifecycle_v2_replay_runtime_identity.v1",
        "identity": IDENTITY,
        "global_execution_identity_sha256": global_identity_sha256,
        "day": day,
        "interval_identity_sha256": interval["interval_identity_sha256"],
        "daily_source_identity_sha256": source_sha,
        "initial_state_mode": "daily_fresh_start",
        "replay_session_scope": "fresh_start_per_target_day",
        "q90_action_enabled": False,
        "strict_native_only": True,
        "journal_writer_identity": (
            "order_lifecycle_journal_writer_v2.replay_day_buffered.v1"
        ),
        "native_unsupported_policy": "explicit_journal_censor_keep_baseline_trajectory",
        "cif_eligibility": "exact_native_spells_only",
        "economic_outcomes_read": False,
    }
    return {
        "day": day,
        "interval": dict(interval),
        "window_cache": dict(window_cache),
        "model_overlay": dict(model_overlay),
        "native_book_artifacts": [dict(item) for item in native_book_artifacts],
        "daily_source_identity_sha256": source_sha,
        "runtime_identity": runtime,
        "runtime_identity_sha256": canonical_sha256(runtime),
        "day_execution_identity_sha256": canonical_sha256(
            {
                "global_execution_identity_sha256": global_identity_sha256,
                "day": day,
                "interval_identity_sha256": interval["interval_identity_sha256"],
                "daily_source_identity_sha256": source_sha,
            }
        ),
    }


def prepare_execution_plan(
    *,
    frozen_v1_path: str | Path,
    cache_root: str | Path,
    window_cache_root: str | Path,
    model_overlay_root: str | Path,
    window_cache_index_path: str | Path | None = None,
) -> dict[str, object]:
    frozen, frozen_path = load_frozen_v1_panel(frozen_v1_path)
    source_contract, source_contract_path = _load_source_contract(frozen)
    baseline = frozen["baseline_runtime_identity"]
    baseline_identity_path = _require_bound_file(
        baseline["identity_artifact"]["path"],
        baseline["identity_artifact"]["sha256"],
        label="baseline identity",
    )
    baseline_identity = _read_json(baseline_identity_path)
    config_meta = baseline_identity["config"]
    model_meta = baseline_identity["model"]
    p3_meta = baseline_identity["p3"]
    config_path = _require_bound_file(
        config_meta["canonical_private_source"],
        baseline["operational_config_sha256"],
        label="operational config",
    )
    model_bundle_path = _require_bound_file(
        Path(str(model_meta["directory"])) / "bundle_meta.json",
        baseline["model_bundle_sha256"],
        label="model bundle",
    )
    p3_path = _require_bound_file(p3_meta["path"], baseline["p3_sha256"], label="P3 artifact")
    feature_dag_path = (ROOT / "features/feature_dag.py").resolve()
    if not feature_dag_path.is_file():
        raise ReplayEmitterError("Feature DAG implementation is missing")
    latency_meta = source_contract["latency_identity"]["samples"]
    latency_path = _require_bound_file(
        latency_meta["path"], latency_meta["sha256"], label="latency profile"
    )
    queue_meta = source_contract["source_identity"]["queue_calibration"]
    queue_path = _require_bound_file(
        queue_meta["path"], queue_meta["sha256"], label="queue calibration"
    )
    raw_root = _resolve_repo_or_relocated_path(
        source_contract["source_identity"]["native_orderbook_root"]
    )
    if not raw_root.is_dir():
        raise ReplayEmitterError(f"native orderbook root is missing: {raw_root}")
    output_root = Path(cache_root).expanduser().resolve()
    input_cache_root = Path(window_cache_root).expanduser().resolve()
    if not input_cache_root.is_dir():
        raise ReplayEmitterError(f"window cache root is missing: {input_cache_root}")
    overlay_cache_root = Path(model_overlay_root).expanduser().resolve()
    if not overlay_cache_root.is_dir():
        raise ReplayEmitterError(f"model overlay root is missing: {overlay_cache_root}")
    indexed = _load_window_cache_index(window_cache_index_path)

    from models import backtest_tick as bt
    from models import data_windows
    from models.backtest_config import load_tick_base_params
    from models.replay_cache_components import references_sha256
    from strategy.model_contract import REQUIRED_MODEL_HEADS, validate_model_bundle

    overlay_params = load_tick_base_params(
        symbol="BTCUSDC",
        config_path=config_path,
        configure_symbol=bt.configure_symbol,
        require_historical_bbo=True,
        queue_calibration_path=queue_path,
        strict_calibration=True,
    )
    if not bool(overlay_params.get("ml_enabled", False)):
        raise ReplayEmitterError("operational overlay preparation requires ML ON")
    overlay_params.update(
        {
            "execution_trade_source": "trades",
            "market_context_warmup_days": 1,
            "_book_source_authority": "native_formal_lifecycle",
            "_book_dataset_version": "normalized_l2_100ms_v2",
        }
    )
    expected_overlay_model_identity = references_sha256(
        data_windows._signature_references(
            data_windows._model_artifact_signatures(model_bundle_path.parent)
        )
    )
    model_metadata = validate_model_bundle(
        model_bundle_path.parent,
        allow_research_only=True,
    )
    if set(model_metadata) != set(REQUIRED_MODEL_HEADS):
        raise ReplayEmitterError("operational bundle lacks the required 13 heads")
    required_feature_keys = sorted(
        {
            str(feature)
            for metadata in model_metadata.values()
            for feature in metadata.get("feature_cols", [])
        }
    )
    if not required_feature_keys:
        raise ReplayEmitterError("operational bundle has no required feature-map keys")

    frozen_artifacts = []
    for item in frozen["market_data_source_identities"]["artifacts"]:
        path = _require_bound_file(item["path"], item["sha256"], label="frozen source")
        frozen_artifacts.append(artifact_identity(path))

    global_identity: dict[str, object] = {
        "identity": IDENTITY,
        "frozen_v1": {
            **artifact_identity(frozen_path),
            "canonical_identity_sha256": frozen["canonical_identity_sha256"],
            "ordered_day_denominator_sha256": canonical_sha256(frozen["panel"]["ordered_utc_days"]),
        },
        "source_contract": artifact_identity(source_contract_path),
        "frozen_source_artifacts": frozen_artifacts,
        "operational_config": artifact_identity(config_path),
        "model_bundle": artifact_identity(model_bundle_path),
        "model_overlay_cache_root": str(overlay_cache_root),
        "model_overlay_bundle_identity_sha256": expected_overlay_model_identity,
        "model_overlay_contract": {
            "required_heads": list(REQUIRED_MODEL_HEADS),
            "required_heads_sha256": canonical_sha256(list(REQUIRED_MODEL_HEADS)),
            "required_feature_keys": required_feature_keys,
            "required_feature_keys_sha256": canonical_sha256(required_feature_keys),
            "overlay_gap_policy": dict(_OVERLAY_GAP_POLICY),
            "independent_reinference_required_day_count": 1,
        },
        "p3_artifact": artifact_identity(p3_path),
        "feature_dag": {
            "semantic_sha256": baseline["feature_dag_sha256"],
            "implementation": artifact_identity(feature_dag_path),
        },
        "runtime_code_artifacts": _current_code_artifacts(),
        "cpp_event_stream": _compiled_cpp_binding(),
        "latency_profile": {
            **artifact_identity(latency_path),
            "profile_id": source_contract["latency_identity"]["profile_id"],
            "mode": source_contract["latency_identity"]["mode"],
            "environment": source_contract["latency_identity"]["environment"],
        },
        "queue_calibration": artifact_identity(queue_path),
        "q90_action_enabled": False,
        "economic_outcomes_read": False,
    }
    global_identity_sha = canonical_sha256(global_identity)
    day_rows = []
    for day_index, interval in enumerate(frozen["panel"]["day_intervals"]):
        day = str(interval["day"])
        window = _window_cache_artifact(
            root=input_cache_root,
            day=day,
            indexed=indexed,
        )
        overlay = _model_overlay_artifact(
            cache_root=overlay_cache_root,
            day=day,
            window_cache=window,
            expected_model_dir=model_bundle_path.parent,
            required_feature_keys=required_feature_keys,
            require_independent_reinference=(day_index == 0),
        )
        native = _native_book_artifacts(
            raw_root=raw_root,
            day=day,
            tick_size=0.1,
            warmup_hours=int(source_contract["replay_contract"]["native_warmup_hours"]),
        )
        day_rows.append(
            _plan_day_identity(
                global_identity_sha256=global_identity_sha,
                day=day,
                interval=interval,
                window_cache=window,
                model_overlay=overlay,
                native_book_artifacts=native,
            )
        )
    plan: dict[str, object] = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": "prepared_not_executed",
        "prepared_at_utc": datetime.now(timezone.utc).isoformat(),
        "cache_root": str(output_root),
        "window_cache_root": str(input_cache_root),
        "model_overlay_root": str(overlay_cache_root),
        "global_execution_identity": global_identity,
        "global_execution_identity_sha256": global_identity_sha,
        "source_contract_path": str(source_contract_path),
        "native_orderbook_root": str(raw_root),
        "ordered_utc_days": list(frozen["panel"]["ordered_utc_days"]),
        "days": day_rows,
        "execution_contract": {
            "engine": "python_authoritative_tick_replay",
            "daily_process_isolation": True,
            "initial_state": "daily_fresh_start",
            "atomic_day_publish": "staging_directory_fsync_os_replace",
            "resume_key": "day_execution_identity_sha256",
            "journal_adapter": (
                "order_lifecycle_journal_v2.python_replay_adapter.strict_native.v1"
            ),
            "journal_storage_format": "parquet",
            "strict_native_only": True,
            "native_unsupported_policy": "explicit_journal_censor_keep_baseline_trajectory",
            "cif_eligibility": "exact_native_spells_only",
        },
        "scope": {
            "mechanics_only": True,
            "economic_outcomes_read": False,
            "formal_40day_replay_executed": False,
            "formal_40day_lockstep_executed": False,
        },
        "permissions": {
            "cif_training": False,
            "economic_evaluation": False,
            "q90_action": False,
            "live_transport": False,
            "live_deployment": False,
        },
    }
    _assert_mechanics_only(plan)
    plan["canonical_plan_sha256"] = canonical_sha256(plan)
    return plan


def validate_execution_plan(plan: Mapping[str, object]) -> dict[str, Mapping[str, object]]:
    _assert_mechanics_only(plan)
    if plan.get("schema_version") != PLAN_SCHEMA_VERSION or plan.get("identity") != IDENTITY:
        raise ReplayEmitterError("execution plan identity differs")
    claimed = str(plan.get("canonical_plan_sha256", ""))
    if claimed != canonical_document_sha256(plan, "canonical_plan_sha256"):
        raise ReplayEmitterError("execution plan canonical SHA256 differs")
    global_identity = plan.get("global_execution_identity")
    if not isinstance(global_identity, Mapping):
        raise ReplayEmitterError("execution plan global identity is missing")
    if plan.get("global_execution_identity_sha256") != canonical_sha256(global_identity):
        raise ReplayEmitterError("execution plan global identity SHA256 differs")
    days = list(map(str, plan.get("ordered_utc_days") or ()))
    if len(days) != 40 or days != sorted(set(days)):
        raise ReplayEmitterError("execution plan must retain the frozen ordered 40 days")
    rows = plan.get("days")
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
        raise ReplayEmitterError("execution plan day rows are missing")
    by_day: dict[str, Mapping[str, object]] = {}
    independent_reinference_count = 0
    for row in rows:
        if not isinstance(row, Mapping):
            raise ReplayEmitterError("execution plan day row is not a mapping")
        day = str(row.get("day", ""))
        if day in by_day:
            raise ReplayEmitterError(f"duplicate execution-plan day: {day}")
        if row.get("interval") != _utc_interval(day):
            raise ReplayEmitterError(f"{day}: execution interval differs")
        expected_source_sha = canonical_sha256(
            {
                "window_cache": dict(row["window_cache"]),
                "model_overlay": dict(row["model_overlay"]),
                "native_book_artifacts": [dict(item) for item in row["native_book_artifacts"]],
            }
        )
        if row.get("daily_source_identity_sha256") != expected_source_sha:
            raise ReplayEmitterError(f"{day}: daily source identity differs")
        overlay = row.get("model_overlay")
        if not isinstance(overlay, Mapping) or set(overlay) != {
            "cache_root",
            "identity",
            "identity_sha256",
            "manifest",
            "data",
            "market_context_output_parity",
            "book_leaf_output_parity",
            "overlay_contract",
            "generation_receipt",
        }:
            raise ReplayEmitterError(f"{day}: model overlay binding schema differs")
        overlay_identity = overlay.get("identity")
        if not isinstance(overlay_identity, Mapping) or (
            canonical_sha256(overlay_identity) != overlay.get("identity_sha256")
            or overlay_identity.get("day") != day
            or overlay_identity.get("model_bundle_identity_sha256")
            != global_identity.get("model_overlay_bundle_identity_sha256")
        ):
            raise ReplayEmitterError(f"{day}: model overlay identity differs")
        parity = overlay.get("market_context_output_parity")
        if not isinstance(parity, Mapping):
            raise ReplayEmitterError(f"{day}: model overlay parity binding is missing")
        parity_payload = dict(parity)
        parity_sha = parity_payload.pop("identity_sha256", None)
        if (
            parity_sha != canonical_sha256(parity_payload)
            or parity_payload.get("window_sha256") != row["window_cache"]["sha256"]
            or parity_payload.get("market_context_identity_sha256")
            != overlay_identity.get("market_context_identity_sha256")
            or not bool(parity_payload.get("exact_trades_and_rolling_arrays"))
            or not bool(
                parity_payload.get("exact_bbo_l2_leaf_and_output_parity")
            )
        ):
            raise ReplayEmitterError(f"{day}: model overlay parity identity differs")
        for contract_key in (
            "book_leaf_output_parity",
            "overlay_contract",
            "generation_receipt",
        ):
            contract = overlay.get(contract_key)
            if not isinstance(contract, Mapping):
                raise ReplayEmitterError(f"{day}: {contract_key} is missing")
            contract_payload = dict(contract)
            contract_sha = contract_payload.pop("identity_sha256", None)
            if contract_sha != canonical_sha256(contract_payload):
                raise ReplayEmitterError(f"{day}: {contract_key} identity differs")
        if not bool(overlay["book_leaf_output_parity"].get("independent_source_reload_parity")):
            raise ReplayEmitterError(f"{day}: BBO/L2 leaf/output parity failed")
        overlay_contract = overlay["overlay_contract"]
        if (
            overlay_contract.get("gap_policy") != _OVERLAY_GAP_POLICY
            or not bool(overlay_contract.get("main_arrays_finite"))
            or overlay_contract.get("required_feature_keys_sha256")
            != global_identity["model_overlay_contract"][
                "required_feature_keys_sha256"
            ]
        ):
            raise ReplayEmitterError(f"{day}: overlay payload contract differs")
        receipt = overlay["generation_receipt"]
        if not bool(receipt.get("passed")):
            raise ReplayEmitterError(f"{day}: overlay generation receipt failed")
        independent_reinference_count += int(
            receipt.get("kind") == "independent_reinference_exact_array_parity"
        )
        runtime = row.get("runtime_identity")
        if not isinstance(runtime, Mapping):
            raise ReplayEmitterError(f"{day}: runtime identity is missing")
        if (
            runtime.get("global_execution_identity_sha256")
            != plan["global_execution_identity_sha256"]
        ):
            raise ReplayEmitterError(f"{day}: runtime global identity differs")
        if runtime.get("daily_source_identity_sha256") != expected_source_sha:
            raise ReplayEmitterError(f"{day}: runtime source identity differs")
        if runtime.get("day") != day or runtime.get("initial_state_mode") != ("daily_fresh_start"):
            raise ReplayEmitterError(f"{day}: runtime day/fresh-start identity differs")
        if row.get("runtime_identity_sha256") != canonical_sha256(row["runtime_identity"]):
            raise ReplayEmitterError(f"{day}: runtime identity SHA256 differs")
        expected_day_sha = canonical_sha256(
            {
                "global_execution_identity_sha256": plan["global_execution_identity_sha256"],
                "day": day,
                "interval_identity_sha256": row["interval"]["interval_identity_sha256"],
                "daily_source_identity_sha256": row["daily_source_identity_sha256"],
            }
        )
        if row.get("day_execution_identity_sha256") != expected_day_sha:
            raise ReplayEmitterError(f"{day}: day execution identity differs")
        by_day[day] = row
    if list(by_day) != days:
        raise ReplayEmitterError("execution plan rows differ from frozen day order")
    if independent_reinference_count < int(
        global_identity["model_overlay_contract"].get(
            "independent_reinference_required_day_count", 1
        )
    ):
        raise ReplayEmitterError("execution plan lacks independent overlay re-inference")
    execution_contract = plan.get("execution_contract")
    if not isinstance(execution_contract, Mapping) or (
        execution_contract.get("strict_native_only") is not True
        or execution_contract.get("cif_eligibility")
        != "exact_native_spells_only"
    ):
        raise ReplayEmitterError("execution plan lacks formal strict-native authority")
    scope = plan.get("scope")
    permissions = plan.get("permissions")
    if not isinstance(scope, Mapping) or not bool(scope.get("mechanics_only")):
        raise ReplayEmitterError("execution plan is not mechanics-only")
    if bool(scope.get("formal_40day_replay_executed")) or bool(
        scope.get("formal_40day_lockstep_executed")
    ):
        raise ReplayEmitterError("prepared plan cannot claim execution")
    if not isinstance(permissions, Mapping) or any(bool(value) for value in permissions.values()):
        raise ReplayEmitterError("prepared plan cannot grant downstream permissions")
    return by_day


def _revalidate_worker_runtime_artifacts(plan: Mapping[str, object]) -> None:
    from features.feature_dag import TEN_SECOND_CAUSAL_GRAPH
    from strategy.model_contract import REQUIRED_MODEL_HEADS, validate_model_bundle

    global_identity = plan["global_execution_identity"]
    for key in (
        "source_contract",
        "operational_config",
        "model_bundle",
        "p3_artifact",
        "latency_profile",
        "queue_calibration",
    ):
        _resolve_artifact_path(global_identity[key], base=ROOT)
    for artifact in global_identity["frozen_source_artifacts"]:
        _resolve_artifact_path(artifact, base=ROOT)
    for artifact in global_identity["runtime_code_artifacts"]:
        path = _resolve_artifact_path(artifact, base=ROOT)
        expected = (ROOT / str(artifact["logical_path"])).resolve()
        if path != expected:
            raise ReplayEmitterError("runtime code artifact path differs from logical path")
    feature_dag = global_identity["feature_dag"]
    _resolve_artifact_path(feature_dag["implementation"], base=ROOT)
    if (
        TEN_SECOND_CAUSAL_GRAPH.sha256() != feature_dag["semantic_sha256"]
        or TEN_SECOND_CAUSAL_GRAPH.sha256()
        != global_identity["model_overlay_contract"].get(
            "feature_dag_semantic_sha256",
            feature_dag["semantic_sha256"],
        )
    ):
        raise ReplayEmitterError("Feature DAG semantic identity changed after planning")
    cpp = global_identity["cpp_event_stream"]
    module_path = _resolve_artifact_path(cpp["module_artifact"], base=ROOT)
    import narrowgate_cpp

    if module_path != Path(narrowgate_cpp.__file__).resolve():
        raise ReplayEmitterError("loaded C++ module differs from the bound artifact")
    if str(
        getattr(
            narrowgate_cpp,
            "ORDER_LIFECYCLE_JOURNAL_V2_MIRROR_ABI_VERSION",
            "",
        )
    ) != str(cpp["abi_version"]):
        raise ReplayEmitterError("loaded C++ lifecycle mirror ABI differs from plan")
    model_dir = Path(global_identity["model_bundle"]["path"]).resolve().parent
    metadata = validate_model_bundle(model_dir, allow_research_only=True)
    if set(metadata) != set(REQUIRED_MODEL_HEADS):
        raise ReplayEmitterError("runtime model bundle no longer has 13 required heads")


@contextlib.contextmanager
def _exclusive_plan_lock(cache_root: Path, plan_sha256: str):
    lock_root = cache_root / ".locks"
    lock_root.mkdir(parents=True, exist_ok=True)
    lock_path = lock_root / f"{plan_sha256}.lock"
    with lock_path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ReplayEmitterError(
                f"execution plan is already held by another process: {plan_sha256}"
            ) from exc
        handle.seek(0)
        handle.truncate()
        handle.write(
            json.dumps(
                {
                    "plan_sha256": plan_sha256,
                    "pid": os.getpid(),
                    "acquired_at_utc": datetime.now(timezone.utc).isoformat(),
                },
                sort_keys=True,
            )
            + "\n"
        )
        handle.flush()
        os.fsync(handle.fileno())
        try:
            yield lock_path
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def build_worker_command(
    *,
    python_executable: str | Path,
    plan_path: str | Path,
    day: str,
    staging_root: str | Path,
) -> list[str]:
    python_path = Path(python_executable).expanduser()
    if not python_path.is_absolute():
        python_path = (ROOT / python_path).absolute()
    command = [
        # Do not resolve this symlink. CPython uses the venv entrypoint path to
        # discover pyvenv.cfg and its site-packages.
        str(python_path),
        "-m",
        WORKER_MODULE,
        "_run-day",
        "--plan",
        str(Path(plan_path).expanduser().resolve()),
        "--day",
        str(day),
        "--staging-root",
        str(Path(staging_root).expanduser().resolve()),
    ]
    if any(fragment in " ".join(command).lower() for fragment in _FORBIDDEN_ECONOMIC_FRAGMENTS):
        raise ReplayEmitterError("worker command contains an economic output argument")
    return command


def _default_command_runner(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    # Module execution keeps the repository root on sys.path in the fresh-day
    # process. An absolute script path would replace it with this audit folder.
    return subprocess.run(
        list(command),
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
    )


def _manifest_artifact(path: Path, *, day_root: Path) -> dict[str, object]:
    return artifact_identity(path, relative_to=day_root)


def _validate_day_manifest(
    path: Path,
    *,
    plan: Mapping[str, object],
    day_row: Mapping[str, object],
) -> dict[str, Any]:
    payload = _read_json(path)
    _assert_mechanics_only(payload, path=f"day[{day_row['day']}]")
    if set(payload) != _DAY_MANIFEST_KEYS:
        raise ReplayEmitterError(f"{day_row['day']}: day manifest keys differ")
    if payload["schema_version"] != DAY_MANIFEST_SCHEMA_VERSION or payload["identity"] != IDENTITY:
        raise ReplayEmitterError(f"{day_row['day']}: day manifest identity differs")
    if payload["day"] != day_row["day"]:
        raise ReplayEmitterError(f"{day_row['day']}: day manifest date differs")
    if payload["plan_sha256"] != plan["canonical_plan_sha256"]:
        raise ReplayEmitterError(f"{day_row['day']}: plan identity differs")
    if payload["day_execution_identity_sha256"] != day_row["day_execution_identity_sha256"]:
        raise ReplayEmitterError(f"{day_row['day']}: execution identity differs")
    if payload["canonical_manifest_sha256"] != canonical_document_sha256(
        payload, "canonical_manifest_sha256"
    ):
        raise ReplayEmitterError(f"{day_row['day']}: canonical manifest hash differs")
    if payload["status"] != "complete" or payload["atomic_publish_method"] != (
        "parent_staging_directory_fsync_os_replace"
    ):
        raise ReplayEmitterError(f"{day_row['day']}: day is not atomically complete")
    replay = payload["replay"]
    if replay != {
        "engine": "python_authoritative_tick_replay",
        "initial_state": "daily_fresh_start",
        "session_scope": "fresh_start_per_target_day",
        "q90_action_enabled": False,
        "strict_native_only": True,
    }:
        raise ReplayEmitterError(f"{day_row['day']}: replay scope differs")
    bindings = payload["bindings"]
    if set(bindings) != _DAY_BINDING_KEYS:
        raise ReplayEmitterError(f"{day_row['day']}: binding schema differs")
    if (
        bindings["global_execution_identity_sha256"] != plan["global_execution_identity_sha256"]
        or bindings["daily_source_identity_sha256"] != day_row["daily_source_identity_sha256"]
    ):
        raise ReplayEmitterError(f"{day_row['day']}: binding identity differs")
    for key in _DAY_BINDING_KEYS - {"cpp_abi_version"}:
        if not _is_sha256(bindings[key]):
            raise ReplayEmitterError(f"{day_row['day']}: invalid binding SHA256: {key}")
    if not str(bindings["cpp_abi_version"]).strip():
        raise ReplayEmitterError(f"{day_row['day']}: C++ ABI version is empty")
    journal = payload["journal_v2"]
    if set(journal) != _JOURNAL_MANIFEST_KEYS:
        raise ReplayEmitterError(f"{day_row['day']}: journal manifest schema differs")
    writer = journal["writer"]
    clocks = journal["dual_clock"]
    cif_eligibility = journal["cif_eligibility"]
    counters = journal["counters"]
    if set(writer) != _WRITER_KEYS:
        raise ReplayEmitterError(f"{day_row['day']}: writer schema differs")
    if set(clocks) != _DUAL_CLOCK_KEYS:
        raise ReplayEmitterError(f"{day_row['day']}: dual-clock schema differs")
    if set(cif_eligibility) != _CIF_ELIGIBILITY_KEYS:
        raise ReplayEmitterError(f"{day_row['day']}: CIF eligibility schema differs")
    if set(counters) != _COUNTER_KEYS:
        raise ReplayEmitterError(f"{day_row['day']}: counter schema differs")
    if not _is_sha256(journal["writer_runtime_identity_sha256"]):
        raise ReplayEmitterError(f"{day_row['day']}: writer runtime SHA256 is invalid")
    if (
        int(writer["rows_dropped"]) != 0
        or int(writer["error_count"]) != 0
        or not bool(writer["closed"])
        or not bool(writer["formal_collection_valid"])
    ):
        raise ReplayEmitterError(f"{day_row['day']}: writer integrity failed")
    if not bool(clocks["passed"]) or any(
        int(clocks[key]) != 0
        for key in (
            "missing_exchange_clock_count",
            "exchange_after_visibility_count",
            "invalid_exchange_exposure_count",
        )
    ):
        raise ReplayEmitterError(f"{day_row['day']}: dual-clock contract failed")
    if int(counters["terminal_positive_remainder_count"]) != 0:
        raise ReplayEmitterError(f"{day_row['day']}: terminal sub-lot remainder is nonzero")
    if int(counters["terminal_observation_count"]) != int(counters["lifecycle_count"]):
        raise ReplayEmitterError(f"{day_row['day']}: terminal cardinality differs")
    if int(journal["row_count"]) != int(writer["rows_committed"]):
        raise ReplayEmitterError(f"{day_row['day']}: journal/writer row counts differ")
    if cif_eligibility["rule"] != "all_fill_risk_rows_exact_native":
        raise ReplayEmitterError(f"{day_row['day']}: CIF eligibility rule differs")
    if int(cif_eligibility["eligible_lifecycle_count"]) != int(
        counters["exact_native_lifecycle_count"]
    ):
        raise ReplayEmitterError(f"{day_row['day']}: exact-native CIF count differs")
    day_root = path.parent
    runtime_path = _resolve_artifact_path(journal["runtime_identity_artifact"], base=day_root)
    runtime_wrapper = _read_json(runtime_path)
    if runtime_wrapper.get("runtime_identity_sha256") != journal["writer_runtime_identity_sha256"]:
        raise ReplayEmitterError(f"{day_row['day']}: writer runtime artifact differs")
    _resolve_artifact_path(journal["health_artifact"], base=day_root)
    for item in (
        *journal["part_manifest_artifacts"],
        *journal["part_data_artifacts"],
    ):
        _resolve_artifact_path(item, base=day_root)
    if len(journal["part_manifest_artifacts"]) != len(journal["part_data_artifacts"]):
        raise ReplayEmitterError(f"{day_row['day']}: journal part cardinality differs")
    part_row_count = 0
    for manifest_identity, data_identity in zip(
        journal["part_manifest_artifacts"],
        journal["part_data_artifacts"],
        strict=True,
    ):
        manifest_path = _resolve_artifact_path(manifest_identity, base=day_root)
        data_path = _resolve_artifact_path(data_identity, base=day_root)
        part_manifest = _read_json(manifest_path)
        if data_path.name != str(part_manifest.get("data_file", "")):
            raise ReplayEmitterError(f"{day_row['day']}: journal part filename differs")
        if data_identity["sha256"] != part_manifest.get("data_sha256"):
            raise ReplayEmitterError(f"{day_row['day']}: journal part SHA256 differs")
        part_row_count += int(part_manifest.get("row_count", -1))
    if part_row_count != int(journal["row_count"]):
        raise ReplayEmitterError(f"{day_row['day']}: journal part row count differs")
    scope = payload["scope"]
    permissions = payload["permissions"]
    if set(scope) != _DAY_SCOPE_KEYS or set(permissions) != _PERMISSION_KEYS:
        raise ReplayEmitterError(f"{day_row['day']}: scope/permission schema differs")
    if not bool(scope["mechanics_only"]) or bool(scope["economic_outcomes_read"]):
        raise ReplayEmitterError(f"{day_row['day']}: mechanics scope drifted")
    if any(bool(value) for value in permissions.values()):
        raise ReplayEmitterError(f"{day_row['day']}: day grants downstream permission")
    return payload


def _publish_staging_day(staging: Path, final: Path) -> None:
    if final.exists():
        raise ReplayEmitterError(f"refusing to replace completed day: {final}")
    _fsync_directory(staging)
    final.parent.mkdir(parents=True, exist_ok=True)
    os.replace(staging, final)
    _fsync_directory(final.parent)


def _execute_plan_locked(
    *,
    plan_path: str | Path,
    python_executable: str | Path,
    days: Sequence[str] | None = None,
    command_runner: CommandRunner = _default_command_runner,
    workers: int = 1,
) -> dict[str, object]:
    resolved_plan = Path(plan_path).expanduser().resolve()
    plan = _read_json(resolved_plan)
    by_day = validate_execution_plan(plan)
    requested = list(map(str, days or plan["ordered_utc_days"]))
    if len(requested) != len(set(requested)):
        raise ReplayEmitterError("requested days are duplicated")
    unknown = sorted(set(requested) - set(by_day))
    if unknown:
        raise ReplayEmitterError(f"requested days are outside frozen v1: {unknown}")
    worker_count = int(workers)
    if worker_count < 1 or worker_count > 8:
        raise ReplayEmitterError("workers must be between 1 and 8")
    cache_root = Path(str(plan["cache_root"])).expanduser().resolve()
    if resolved_plan.parent != cache_root:
        raise ReplayEmitterError("execution plan must live directly under its explicit cache root")
    days_root = cache_root / "days"
    staging_parent = cache_root / ".staging"
    days_root.mkdir(parents=True, exist_ok=True)
    staging_parent.mkdir(parents=True, exist_ok=True)
    def execute_day(day: str) -> dict[str, object]:
        day_row = by_day[day]
        final = days_root / day
        final_manifest = final / "day_manifest.json"
        if final.exists():
            if not final_manifest.is_file():
                raise ReplayEmitterError(f"{day}: final day exists without a manifest")
            _validate_day_manifest(final_manifest, plan=plan, day_row=day_row)
            return {"day": day, "status": "resumed"}
        for stale in staging_parent.glob(f"{day}-*"):
            if stale.is_dir():
                shutil.rmtree(stale)
            else:
                stale.unlink(missing_ok=True)
        staging = staging_parent / f"{day}-{uuid.uuid4().hex}"
        command = build_worker_command(
            python_executable=python_executable,
            plan_path=resolved_plan,
            day=day,
            staging_root=staging,
        )
        try:
            completed = command_runner(command)
            if int(completed.returncode) != 0:
                stderr = str(completed.stderr or "").strip()
                raise ReplayEmitterError(
                    f"{day}: replay worker failed with code {completed.returncode}: "
                    f"{stderr[-2000:]}"
                )
            staged_manifest = staging / "day_manifest.json"
            if not staged_manifest.is_file():
                raise ReplayEmitterError(f"{day}: replay worker did not emit day_manifest.json")
            _validate_day_manifest(staged_manifest, plan=plan, day_row=day_row)
            _publish_staging_day(staging, final)
        except Exception:
            shutil.rmtree(staging, ignore_errors=True)
            raise
        _validate_day_manifest(final_manifest, plan=plan, day_row=day_row)
        return {"day": day, "status": "executed"}

    results_by_day: dict[str, dict[str, object]] = {}
    if worker_count == 1 or len(requested) <= 1:
        for day in requested:
            results_by_day[day] = execute_day(day)
    else:
        with ThreadPoolExecutor(
            max_workers=min(worker_count, len(requested)),
            thread_name_prefix="f07-day-worker",
        ) as executor:
            futures = {executor.submit(execute_day, day): day for day in requested}
            try:
                for future in as_completed(futures):
                    day = futures[future]
                    results_by_day[day] = future.result()
            except Exception:
                for future in futures:
                    future.cancel()
                raise
    results = [results_by_day[day] for day in requested]

    completed_manifests: list[dict[str, object]] = []
    for day in plan["ordered_utc_days"]:
        manifest_path = days_root / str(day) / "day_manifest.json"
        if not manifest_path.is_file():
            continue
        payload = _validate_day_manifest(manifest_path, plan=plan, day_row=by_day[str(day)])
        completed_manifests.append(
            {
                "day": str(day),
                "artifact": artifact_identity(manifest_path, relative_to=cache_root),
                "journal_rows": int(payload["journal_v2"]["row_count"]),
            }
        )
    panel_complete = len(completed_manifests) == 40
    if panel_complete:
        _publish_panel_manifest(
            cache_root=cache_root,
            plan=plan,
            by_day=by_day,
            day_references=completed_manifests,
        )
    return {
        "requested": results,
        "completed_day_count": len(completed_manifests),
        "formal_40day_journal_emission_complete": panel_complete,
        "formal_40day_lockstep_executed": False,
        "economic_outcomes_read": False,
    }


def execute_plan(
    *,
    plan_path: str | Path,
    python_executable: str | Path,
    days: Sequence[str] | None = None,
    command_runner: CommandRunner = _default_command_runner,
    workers: int = 1,
) -> dict[str, object]:
    resolved_plan = Path(plan_path).expanduser().resolve()
    plan = _read_json(resolved_plan)
    validate_execution_plan(plan)
    cache_root = Path(str(plan["cache_root"])).expanduser().resolve()
    if resolved_plan.parent != cache_root:
        raise ReplayEmitterError(
            "execution plan must live directly under its explicit cache root"
        )
    with _exclusive_plan_lock(cache_root, str(plan["canonical_plan_sha256"])):
        return _execute_plan_locked(
            plan_path=resolved_plan,
            python_executable=python_executable,
            days=days,
            command_runner=command_runner,
            workers=workers,
        )


def _publish_panel_manifest(
    *,
    cache_root: Path,
    plan: Mapping[str, object],
    by_day: Mapping[str, Mapping[str, object]],
    day_references: Sequence[Mapping[str, object]],
) -> Path:
    if [str(item["day"]) for item in day_references] != list(plan["ordered_utc_days"]):
        raise ReplayEmitterError("panel publication requires all frozen days in order")
    totals: Counter[str] = Counter()
    for day in plan["ordered_utc_days"]:
        manifest = _read_json(cache_root / "days" / str(day) / "day_manifest.json")
        counters = manifest["journal_v2"]["counters"]
        for key in (
            "lifecycle_count",
            "event_count",
            "terminal_observation_count",
            "cancel_reject_count",
            "cancel_reject_to_active_count",
            "cancel_reject_to_partially_filled_count",
            "sub_lot_partial_remaining_count",
            "terminal_positive_remainder_count",
            "exact_native_lifecycle_count",
            "native_queue_censored_lifecycle_count",
        ):
            totals[key] += int(counters[key])
    panel: dict[str, object] = {
        "schema_version": PANEL_MANIFEST_SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": "journal_emission_complete_lockstep_not_executed",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "plan_sha256": plan["canonical_plan_sha256"],
        "ordered_utc_days": list(plan["ordered_utc_days"]),
        "day_manifests": list(day_references),
        "mechanics_totals": dict(sorted(totals.items())),
        "scope": {
            "mechanics_only": True,
            "economic_outcomes_read": False,
            "formal_40day_journal_emission_complete": True,
            "formal_40day_lockstep_executed": False,
        },
        "permissions": {
            "cif_training": False,
            "economic_evaluation": False,
            "q90_action": False,
            "live_transport": False,
            "live_deployment": False,
        },
    }
    _assert_mechanics_only(panel)
    panel["canonical_manifest_sha256"] = canonical_sha256(panel)
    if set(panel) != _PANEL_MANIFEST_KEYS:
        raise AssertionError("internal panel manifest schema drift")
    path = cache_root / "panel_manifest.json"
    _atomic_write_json(path, panel)
    return path


def _load_replay_window(day_row: Mapping[str, object]) -> tuple[dict[str, Any], Path]:
    from models.data_windows import _load_cached_window

    cache = _resolve_artifact_path(day_row["window_cache"], base=ROOT)
    before = (cache.stat().st_size, cache.stat().st_mtime_ns, file_sha256(cache))
    loaded = _load_cached_window(cache)
    if loaded is None:
        raise ReplayEmitterError(f"bound replay window cache is incompatible: {cache}")
    window = loaded.to_dict()
    after = (cache.stat().st_size, cache.stat().st_mtime_ns, file_sha256(cache))
    if before != after:
        raise ReplayEmitterError(f"window cache changed while loading: {cache}")
    if not bool(window.get("formal_lifecycle_replay_eligible", False)):
        raise ReplayEmitterError(f"{day_row['day']}: window lacks formal lifecycle authority")
    return window, cache


def _load_bound_model_overlay(
    day_row: Mapping[str, object],
    *,
    required_feature_keys: Sequence[str],
) -> tuple[Any, ...]:
    from models.replay_cache_components import load_model_overlay

    day = str(day_row["day"])
    binding = day_row.get("model_overlay")
    if not isinstance(binding, Mapping) or set(binding) != {
        "cache_root",
        "identity",
        "identity_sha256",
        "manifest",
        "data",
        "market_context_output_parity",
        "book_leaf_output_parity",
        "overlay_contract",
        "generation_receipt",
    }:
        raise ReplayEmitterError(f"{day}: model overlay binding schema differs")
    identity = binding["identity"]
    if not isinstance(identity, Mapping):
        raise ReplayEmitterError(f"{day}: model overlay identity is missing")
    if canonical_sha256(identity) != binding["identity_sha256"]:
        raise ReplayEmitterError(f"{day}: model overlay identity SHA256 differs")
    if (
        identity.get("day") != day
        or identity.get("symbol") != "BTCUSDC"
        or not bool(identity.get("run_ml_inference"))
    ):
        raise ReplayEmitterError(f"{day}: model overlay day/symbol/inference differs")
    parity = binding["market_context_output_parity"]
    if not isinstance(parity, Mapping):
        raise ReplayEmitterError(f"{day}: model overlay market-context parity is missing")
    parity_payload = dict(parity)
    claimed_parity_sha = parity_payload.pop("identity_sha256", None)
    if (
        claimed_parity_sha != canonical_sha256(parity_payload)
        or parity_payload.get("window_sha256") != day_row["window_cache"]["sha256"]
        or parity_payload.get("market_context_identity_sha256")
        != identity.get("market_context_identity_sha256")
        or not bool(parity_payload.get("exact_trades_and_rolling_arrays"))
    ):
        raise ReplayEmitterError(f"{day}: model overlay market-context parity differs")
    manifest_path = _resolve_artifact_path(binding["manifest"], base=ROOT)
    data_path = _resolve_artifact_path(binding["data"], base=ROOT)
    if manifest_path.parent != data_path.parent:
        raise ReplayEmitterError(f"{day}: model overlay artifacts are split")
    manifest = _read_json(manifest_path)
    if (
        manifest.get("identity") != dict(identity)
        or manifest.get("identity_sha256") != binding["identity_sha256"]
    ):
        raise ReplayEmitterError(f"{day}: model overlay manifest differs")
    overlay = load_model_overlay(
        cache_root=Path(str(binding["cache_root"])).expanduser().resolve(),
        identity=identity,
    )
    if not required_feature_keys:
        raise ReplayEmitterError(f"{day}: required overlay feature keys are missing")
    observed_contract = _validate_overlay_payload(
        overlay,
        day=day,
        required_feature_keys=required_feature_keys,
    )
    if observed_contract != binding["overlay_contract"]:
        raise ReplayEmitterError(f"{day}: operational ML overlay contract differs")
    return overlay


def _assert_strict_native_queue_authority(
    *,
    day_row: Mapping[str, object],
    tape: object,
    params: Mapping[str, object],
) -> None:
    """Bind exact queue authority to the native snapshot/delta tape only."""

    day = str(day_row["day"])
    if str(params.get("exchange_book_queue_mode", "")) != "strict":
        raise ReplayEmitterError(f"{day}: native queue authority requires strict mode")
    if bool(params.get("queue_l2_cancel_ahead_enabled", True)):
        raise ReplayEmitterError(f"{day}: strict native queue cannot infer cancel-ahead")

    artifacts = day_row.get("native_book_artifacts")
    if not isinstance(artifacts, Sequence) or isinstance(artifacts, (str, bytes)):
        raise ReplayEmitterError(f"{day}: native book artifacts are missing")
    role_counts: Counter[str] = Counter()
    planned_paths: list[str] = []
    for item in artifacts:
        if not isinstance(item, Mapping) or set(item) != {
            "role",
            "path",
            "size_bytes",
            "sha256",
        }:
            raise ReplayEmitterError(f"{day}: native book artifact schema differs")
        role = str(item["role"])
        if role not in {"native_book_warmup", "native_book_target"}:
            raise ReplayEmitterError(f"{day}: unsupported native book role: {role}")
        role_counts[role] += 1
        planned_paths.append(str(_resolve_artifact_path(item, base=ROOT)))
    expected_counts = {"native_book_warmup": 24, "native_book_target": 24}
    if dict(role_counts) != expected_counts:
        raise ReplayEmitterError(
            f"{day}: strict native tape must bind 24 warmup and 24 target files; "
            f"observed={dict(role_counts)}"
        )
    if len(set(planned_paths)) != len(planned_paths):
        raise ReplayEmitterError(f"{day}: native book artifacts contain duplicate paths")

    source_paths = getattr(tape, "source_paths", None)
    if not isinstance(source_paths, Sequence) or isinstance(source_paths, (str, bytes)):
        raise ReplayEmitterError(f"{day}: native tape source paths are missing")
    observed_paths = [str(Path(path).resolve()) for path in source_paths]
    if observed_paths != planned_paths:
        raise ReplayEmitterError(f"{day}: native tape path identity differs from plan")


def _configure_authoritative_params(
    *,
    plan: Mapping[str, object],
    day_row: Mapping[str, object],
    staging_root: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    from models import backtest_tick as bt
    from models.backtest_config import (
        load_tick_base_params,
        validate_formal_replay_calibration,
    )
    from models.replay_contract import configure_fixed_latency_distribution

    global_identity = plan["global_execution_identity"]
    source_contract_path = _resolve_artifact_path(global_identity["source_contract"], base=ROOT)
    if source_contract_path != Path(str(plan["source_contract_path"])).resolve():
        raise ReplayEmitterError("source contract path differs from its bound identity")
    source_contract = _read_json(source_contract_path)
    config_path = _resolve_artifact_path(global_identity["operational_config"], base=ROOT)
    queue_path = _resolve_artifact_path(global_identity["queue_calibration"], base=ROOT)
    latency_path = _resolve_artifact_path(global_identity["latency_profile"], base=ROOT)
    params = load_tick_base_params(
        symbol="BTCUSDC",
        config_path=config_path,
        configure_symbol=bt.configure_symbol,
        require_historical_bbo=True,
        queue_calibration_path=queue_path,
        strict_calibration=True,
    )
    if not bool(params.get("ml_enabled", False)):
        raise ReplayEmitterError("F07 replay must retain the operational ML-on baseline")
    if bool(params.get("dynamic_fill_hazard_action_enabled", False)):
        raise ReplayEmitterError("F07 replay requires q90 action OFF")
    if bool(params.get("buy_fill_selection_live_enabled", False)):
        raise ReplayEmitterError("F07 replay requires the retired BUY fill-selection action OFF")
    replay = source_contract["replay_contract"]
    params.update(
        {
            "execution_trade_source": "trades",
            "market_context_warmup_days": 1,
            "replay_event_clock": "merged",
            "replay_clock_interval_ms": int(replay["clock_interval_ms"]),
            "exchange_book_queue_mode": "strict",
            "queue_l2_cancel_ahead_enabled": False,
            "decision_trace_profile": "mechanics_only",
            "trace_decisions_max": 0,
            "trace_quotes_max": 0,
            "trace_fills_max": 0,
            "trace_first_add_decision_to_terminal_max": 0,
            "trace_first_opener_decision_to_terminal_max": 0,
            "collect_curves": False,
            "rng_seed": int(replay["rng_seed"]),
            "sync_adjust_replay_mode": "disabled",
            "replay_purpose": "f07_order_lifecycle_v2_40day_journal_emission",
            "replay_initial_state_mode": "fresh_start",
            "replay_promotion_eligible": False,
            "initial_inventory": 0.0,
            "initial_entry_price": 0.0,
            "fill_cooldown_clock_mode": "wall_time",
            "window_cache_write_enabled": False,
            "legacy_monolithic_window_cache_write_enabled": False,
            "legacy_component_v1_write_enabled": False,
            "dynamic_fill_hazard_action_enabled": False,
            "order_lifecycle_journal_v2_enabled": True,
            "order_lifecycle_journal_v2_strict_native_only": True,
            "order_lifecycle_journal_v2_root": staging_root / "journal",
            "order_lifecycle_journal_v2_session_id": f"f07-{day_row['day']}",
            "order_lifecycle_journal_v2_storage_format": "parquet",
            "_order_lifecycle_journal_v2_runtime_identity": dict(day_row["runtime_identity"]),
        }
    )
    latency = bt._load_live_perf_latency_samples(
        latency_path,
        mode=str(global_identity["latency_profile"]["mode"]),
    )
    params["_new_order_latency_samples_ms"] = latency["new_order_latency_samples_ms"]
    params["_cancel_order_latency_samples_ms"] = latency["cancel_order_latency_samples_ms"]
    configure_fixed_latency_distribution(
        params,
        scenario="baseline",
        profile_id=str(global_identity["latency_profile"]["profile_id"]),
        environment=str(global_identity["latency_profile"]["environment"]),
        baseline_clip_quantile=float(source_contract["latency_identity"]["baseline_clip_quantile"]),
    )
    validate_formal_replay_calibration(params, require_latency=True)
    return params, source_contract


def _read_journal_parts(
    session_root: Path,
) -> tuple[list[dict[str, Any]], list[Path], list[Path]]:
    import pyarrow.parquet as pq

    from execution.order_lifecycle_journal_v2_strict_native import (
        ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS,
        validate_order_lifecycle_journal_v2_payload,
    )

    rows: list[dict[str, Any]] = []
    manifests = sorted((session_root / "parts").glob("part-*.manifest.json"))
    data_paths: list[Path] = []
    for manifest_path in manifests:
        manifest = _read_json(manifest_path)
        data_path = manifest_path.parent / str(manifest["data_file"])
        if file_sha256(data_path) != str(manifest["data_sha256"]):
            raise ReplayEmitterError(f"journal payload SHA256 differs: {data_path}")
        table = pq.read_table(data_path)
        if tuple(table.column_names) != tuple(ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS):
            raise ReplayEmitterError("journal-v2 Parquet schema differs")
        part_rows = table.to_pylist()
        if len(part_rows) != int(manifest["row_count"]):
            raise ReplayEmitterError("journal part row count differs")
        for row in part_rows:
            validate_order_lifecycle_journal_v2_payload(row)
        rows.extend(part_rows)
        data_paths.append(data_path)
    return rows, manifests, data_paths


def _journal_mechanics_summary(
    *,
    staging_root: Path,
    day_row: Mapping[str, object],
    lot_size_btc: float,
    replay_health: Mapping[str, object] | None = None,
) -> dict[str, object]:
    from models.replay.order_lifecycle_v2_replay_adapter_strict_native import (
        REPLAY_ADAPTER_ID,
    )

    session_root = staging_root / "journal" / f"session-f07-{day_row['day']}"
    runtime_path = session_root / "runtime_identity.json"
    health_path = session_root / "health.json"
    runtime_wrapper = _read_json(runtime_path)
    health = _read_json(health_path)
    runtime = runtime_wrapper.get("runtime_identity")
    expected_runtime = {
        **dict(day_row["runtime_identity"]),
        "replay_adapter_id": REPLAY_ADAPTER_ID,
        "economic_outcomes_read": False,
        "q90_action_authorized": False,
    }
    if runtime != expected_runtime:
        raise ReplayEmitterError(f"{day_row['day']}: writer runtime identity differs")
    writer_runtime_sha = canonical_sha256(expected_runtime)
    if runtime_wrapper.get("runtime_identity_sha256") != writer_runtime_sha:
        raise ReplayEmitterError(f"{day_row['day']}: writer runtime hash differs")
    rows, part_manifests, part_data = _read_journal_parts(session_root)
    if not rows:
        raise ReplayEmitterError(f"{day_row['day']}: journal-v2 emitted zero rows")
    if int(health.get("rows_dropped", -1)) != 0 or int(health.get("error_count", -1)) != 0:
        raise ReplayEmitterError(f"{day_row['day']}: writer reported a drop/error")
    if not bool(health.get("closed")) or health.get("state") != "closed":
        raise ReplayEmitterError(f"{day_row['day']}: writer is not closed")
    if not bool(health.get("formal_collection_valid")):
        raise ReplayEmitterError(f"{day_row['day']}: writer collection is invalid")
    if int(health.get("rows_committed", -1)) != len(rows):
        raise ReplayEmitterError(f"{day_row['day']}: writer row count differs")
    expected_writer = str(day_row["runtime_identity"].get("journal_writer_identity", ""))
    if expected_writer == "order_lifecycle_journal_writer_v2.replay_day_buffered.v1":
        if int(health.get("part_count", -1)) != len(part_manifests):
            raise ReplayEmitterError(f"{day_row['day']}: writer part count differs")
        if health.get("replay_writer_id") != expected_writer:
            raise ReplayEmitterError(f"{day_row['day']}: replay writer identity differs")
        if health.get("atomic_commit_scope") != "unadmitted_day_staging":
            raise ReplayEmitterError(f"{day_row['day']}: replay atomic scope differs")
        if replay_health is None or int(
            replay_health.get("adapter_callback_count", -1)
        ) != int(health.get("callbacks_committed", -2)):
            raise ReplayEmitterError(
                f"{day_row['day']}: adapter/writer callback counts differ"
            )
    elif int(health.get("callbacks_committed", -1)) != len(part_manifests):
        raise ReplayEmitterError(f"{day_row['day']}: writer part count differs")

    event_ids = [str(row["event_id"]) for row in rows]
    if len(event_ids) != len(set(event_ids)):
        raise ReplayEmitterError(f"{day_row['day']}: journal event IDs are duplicated")
    by_lifecycle: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_lifecycle[str(row["lifecycle_id"])].append(row)
    ordered_rows: list[dict[str, Any]] = []
    exact_native_lifecycle_count = 0
    native_queue_censored_lifecycle_count = 0
    native_queue_censor_reasons: Counter[str] = Counter()
    for lifecycle_id, lifecycle_rows in by_lifecycle.items():
        ordered = sorted(lifecycle_rows, key=lambda item: int(item["lifecycle_sequence"]))
        if [int(row["lifecycle_sequence"]) for row in ordered] != list(range(1, len(ordered) + 1)):
            raise ReplayEmitterError(f"{lifecycle_id}: lifecycle sequence is not contiguous")
        terminal_indices = [
            index
            for index, row in enumerate(ordered)
            if str(row["terminal_observation"]) in _TERMINAL_OBSERVATIONS
        ]
        if len(terminal_indices) != 1:
            raise ReplayEmitterError(f"{lifecycle_id}: terminal cardinality is invalid")
        terminal_index = terminal_indices[0]
        post_terminal_events = [
            str(row["lifecycle_event"]) for row in ordered[terminal_index + 1 :]
        ]
        if post_terminal_events not in (
            [],
            ["post_cancel_recovery"],
            ["post_cancel_recovery", "reentry_eligible"],
        ):
            raise ReplayEmitterError(
                f"{lifecycle_id}: unsupported post-terminal lifecycle path"
            )
        activation_indices = [
            index
            for index, row in enumerate(ordered)
            if str(row["lifecycle_event"]) == "activate"
        ]
        if not activation_indices:
            native_queue_censored_lifecycle_count += 1
            native_queue_censor_reasons["no_activation"] += 1
        else:
            risk_phases = {"ACTIVE", "PARTIALLY_FILLED", "CANCEL_PENDING"}
            risk_rows = [
                row
                for row in ordered[activation_indices[0] :]
                if str(row["phase_before"]) in risk_phases
                or str(row["phase_after"]) in risk_phases
            ]
            invalid = next(
                (
                    row
                    for row in risk_rows
                    if str(row["simulator_queue_source"])
                    != "native_exchange_book"
                    or not bool(row["exact_queue_path_valid"])
                ),
                None,
            )
            if invalid is None:
                exact_native_lifecycle_count += 1
            else:
                native_queue_censored_lifecycle_count += 1
                source = str(invalid["simulator_queue_source"])
                reason = (
                    f"queue_source:{source}"
                    if source != "native_exchange_book"
                    else "native_exact_path_invalidated"
                )
                native_queue_censor_reasons[reason] += 1
        ordered_rows.extend(ordered)

    missing_exchange = 0
    exchange_after_visibility = 0
    invalid_exchange_exposure = 0
    required_exchange_rows = 0
    risk_started: dict[str, bool] = defaultdict(bool)
    event_counts: Counter[str] = Counter()
    terminal_reason_counts: Counter[str] = Counter()
    cancel_reject_to: Counter[str] = Counter()
    terminal_count = 0
    full_fill_exact_zero = 0
    sub_lot_partial = 0
    terminal_positive_remainder = 0
    for row in ordered_rows:
        event = str(row["lifecycle_event"])
        event_counts[event] += 1
        lifecycle_id = str(row["lifecycle_id"])
        visibility = int(row["event_visibility_ts_ns"])
        exchange_raw = row["event_exchange_ts_ns"]
        exchange = int(exchange_raw) if exchange_raw is not None else 0
        if event in _EXCHANGE_CLOCK_EVENTS:
            required_exchange_rows += 1
            if exchange <= 0:
                missing_exchange += 1
            elif exchange > visibility:
                exchange_after_visibility += 1
        if event == "activate":
            risk_started[lifecycle_id] = True
        if risk_started[lifecycle_id] and (
            not bool(row["exchange_exposure_valid"])
            or row["quantity_time_exposure_exchange_btc_s"] is None
        ):
            invalid_exchange_exposure += 1
        remaining = float(row["remaining_quantity_after"])
        if event == "cancel_rejected":
            cancel_reject_to[str(row["phase_after"])] += 1
        if event == "partial_fill" and 0.0 < remaining < float(lot_size_btc):
            sub_lot_partial += 1
        if event == "full_fill" and remaining == 0.0:
            full_fill_exact_zero += 1
        terminal_observation = str(row["terminal_observation"])
        if terminal_observation in _TERMINAL_OBSERVATIONS:
            terminal_count += 1
            terminal_reason_counts[str(row["event_reason"])] += 1
            if (
                remaining > 0.0
                and terminal_observation == "EXCHANGE_TERMINAL"
                and event == ("full_fill")
            ):
                terminal_positive_remainder += 1
    dual_clock_passed = not (
        missing_exchange or exchange_after_visibility or invalid_exchange_exposure
    )
    if not dual_clock_passed:
        raise ReplayEmitterError(f"{day_row['day']}: journal-v2 dual-clock gate failed")
    if terminal_positive_remainder:
        raise ReplayEmitterError(f"{day_row['day']}: full-fill terminal remainder is positive")

    return {
        "session_root": str(session_root.relative_to(staging_root)),
        "writer_runtime_identity_sha256": writer_runtime_sha,
        "runtime_identity_artifact": _manifest_artifact(runtime_path, day_root=staging_root),
        "health_artifact": _manifest_artifact(health_path, day_root=staging_root),
        "part_manifest_artifacts": [
            _manifest_artifact(path, day_root=staging_root) for path in part_manifests
        ],
        "part_data_artifacts": [
            _manifest_artifact(path, day_root=staging_root) for path in part_data
        ],
        "row_count": len(rows),
        "writer": {
            "rows_committed": int(health["rows_committed"]),
            "callbacks_committed": int(health["callbacks_committed"]),
            "rows_dropped": int(health["rows_dropped"]),
            "error_count": int(health["error_count"]),
            "closed": bool(health["closed"]),
            "formal_collection_valid": bool(health["formal_collection_valid"]),
        },
        "dual_clock": {
            "required_exchange_event_count": required_exchange_rows,
            "missing_exchange_clock_count": missing_exchange,
            "exchange_after_visibility_count": exchange_after_visibility,
            "invalid_exchange_exposure_count": invalid_exchange_exposure,
            "passed": dual_clock_passed,
        },
        "cif_eligibility": {
            "rule": "all_fill_risk_rows_exact_native",
            "eligible_lifecycle_count": exact_native_lifecycle_count,
            "censored_lifecycle_count": native_queue_censored_lifecycle_count,
            "censor_reason_counts": dict(sorted(native_queue_censor_reasons.items())),
        },
        "counters": {
            "lifecycle_count": len(by_lifecycle),
            "event_count": len(rows),
            "terminal_observation_count": terminal_count,
            "event_counts": dict(sorted(event_counts.items())),
            "terminal_reason_counts": dict(sorted(terminal_reason_counts.items())),
            "cancel_reject_count": int(event_counts["cancel_rejected"]),
            "cancel_reject_to_active_count": int(cancel_reject_to["ACTIVE"]),
            "cancel_reject_to_partially_filled_count": int(cancel_reject_to["PARTIALLY_FILLED"]),
            "sub_lot_partial_remaining_count": sub_lot_partial,
            "full_fill_exact_zero_count": full_fill_exact_zero,
            "terminal_positive_remainder_count": terminal_positive_remainder,
            "exact_native_lifecycle_count": exact_native_lifecycle_count,
            "native_queue_censored_lifecycle_count": (
                native_queue_censored_lifecycle_count
            ),
        },
    }


def _run_authoritative_day(
    *,
    plan_path: Path,
    day: str,
    staging_root: Path,
) -> Path:
    from models import backtest_tick as bt
    from models.exchange_book_replay import CryptoHFTExchangeBookTape
    from models.replay import order_lifecycle_v2_replay_adapter_strict_native as adapter_module

    from execution.order_lifecycle_journal_writer_v2_replay_day_buffered import (
        DayBufferedReplayJournalWriterV2,
    )
    from execution.order_lifecycle_journal_writer_v2_strict_native import (
        OrderLifecycleJournalWriterV2 as StrictNativeJournalWriterV2,
    )

    plan = _read_json(plan_path)
    by_day = validate_execution_plan(plan)
    _revalidate_worker_runtime_artifacts(plan)
    if day not in by_day:
        raise ReplayEmitterError(f"worker day is outside frozen panel: {day}")
    day_row = by_day[day]
    expected_staging_parent = Path(str(plan["cache_root"])).resolve() / ".staging"
    resolved_staging = staging_root.expanduser().resolve()
    if resolved_staging.parent != expected_staging_parent:
        raise ReplayEmitterError("worker staging root is outside the explicit cache root")
    if resolved_staging.exists():
        raise ReplayEmitterError("worker staging root already exists")
    resolved_staging.mkdir(parents=True)
    try:
        params, source_contract = _configure_authoritative_params(
            plan=plan,
            day_row=day_row,
            staging_root=resolved_staging,
        )
        window, _ = _load_replay_window(day_row)
        window["ml_data"] = _load_bound_model_overlay(
            day_row,
            required_feature_keys=plan["global_execution_identity"][
                "model_overlay_contract"
            ]["required_feature_keys"],
        )
        raw_root = Path(str(plan["native_orderbook_root"])).expanduser().resolve()
        tape = CryptoHFTExchangeBookTape(
            raw_root=raw_root,
            day=day,
            symbol="BTCUSDC",
            tick_size=float(params.get("tick_size", bt.TICK)),
            warmup_hours=int(source_contract["replay_contract"]["native_warmup_hours"]),
            strict_complete=True,
        )
        _assert_strict_native_queue_authority(
            day_row=day_row,
            tape=tape,
            params=params,
        )

        if adapter_module.OrderLifecycleJournalWriterV2 is not StrictNativeJournalWriterV2:
            raise ReplayEmitterError(f"{day}: strict-native writer binding was already replaced")
        adapter_module.OrderLifecycleJournalWriterV2 = DayBufferedReplayJournalWriterV2
        try:
            with Path(os.devnull).open("w", encoding="utf-8") as sink:
                with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
                    replay_result = bt._simulate_tick_with_engine(
                        "python",
                        window["trades"],
                        window["var_ts_ms"],
                        window["var_ssq"],
                        params,
                        ml_data=window["ml_data"],
                        bbo_data=window.get("bbo_data"),
                        l2_data=window.get("l2_data"),
                        var_ti=window.get("var_ti"),
                        var_retsq=window.get("var_retsq"),
                        exchange_book_event_tape=tape,
                    )
        finally:
            adapter_module.OrderLifecycleJournalWriterV2 = StrictNativeJournalWriterV2
        health = replay_result.get("_order_lifecycle_journal_v2_health")
        del replay_result
        if not isinstance(health, Mapping):
            raise ReplayEmitterError(f"{day}: authoritative replay omitted journal health")
        if bool(health.get("economic_outcomes_read")):
            raise ReplayEmitterError(f"{day}: replay journal read economic outcomes")
        journal = _journal_mechanics_summary(
            staging_root=resolved_staging,
            day_row=day_row,
            lot_size_btc=float(params.get("lot_size", 0.001)),
            replay_health=health,
        )
        bindings = {
            "global_execution_identity_sha256": plan["global_execution_identity_sha256"],
            "daily_source_identity_sha256": day_row["daily_source_identity_sha256"],
            "config_sha256": plan["global_execution_identity"]["operational_config"]["sha256"],
            "model_bundle_sha256": plan["global_execution_identity"]["model_bundle"]["sha256"],
            "model_overlay_identity_sha256": day_row["model_overlay"]["identity_sha256"],
            "p3_sha256": plan["global_execution_identity"]["p3_artifact"]["sha256"],
            "feature_dag_semantic_sha256": plan["global_execution_identity"]["feature_dag"][
                "semantic_sha256"
            ],
            "runtime_code_identity_sha256": canonical_sha256(
                plan["global_execution_identity"]["runtime_code_artifacts"]
            ),
            "cpp_abi_version": plan["global_execution_identity"]["cpp_event_stream"]["abi_version"],
            "cpp_module_sha256": plan["global_execution_identity"]["cpp_event_stream"][
                "module_artifact"
            ]["sha256"],
            "latency_profile_sha256": plan["global_execution_identity"]["latency_profile"][
                "sha256"
            ],
        }
        manifest: dict[str, object] = {
            "schema_version": DAY_MANIFEST_SCHEMA_VERSION,
            "identity": IDENTITY,
            "day": day,
            "plan_sha256": plan["canonical_plan_sha256"],
            "day_execution_identity_sha256": day_row["day_execution_identity_sha256"],
            "status": "complete",
            "atomic_publish_method": "parent_staging_directory_fsync_os_replace",
            "replay": {
                "engine": "python_authoritative_tick_replay",
                "initial_state": "daily_fresh_start",
                "session_scope": "fresh_start_per_target_day",
                "q90_action_enabled": False,
                "strict_native_only": True,
            },
            "bindings": bindings,
            "journal_v2": journal,
            "scope": {
                "mechanics_only": True,
                "economic_outcomes_read": False,
                "formal_40day_replay_executed": False,
                "formal_40day_lockstep_executed": False,
            },
            "permissions": {
                "cif_training": False,
                "economic_evaluation": False,
                "q90_action": False,
                "live_transport": False,
                "live_deployment": False,
            },
        }
        _assert_mechanics_only(manifest)
        manifest["canonical_manifest_sha256"] = canonical_sha256(manifest)
        manifest_path = resolved_staging / "day_manifest.json"
        _atomic_write_json(manifest_path, manifest)
        _validate_day_manifest(manifest_path, plan=plan, day_row=day_row)
        return manifest_path
    except Exception:
        shutil.rmtree(resolved_staging, ignore_errors=True)
        raise


def _write_plan(cache_root: Path, plan: Mapping[str, object]) -> Path:
    cache_root.mkdir(parents=True, exist_ok=True)
    path = cache_root / "execution_plan.json"
    if path.exists():
        existing = _read_json(path)
        if existing != dict(plan):
            raise ReplayEmitterError("explicit cache root already contains a different plan")
        return path
    _atomic_write_json(path, plan)
    return path


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare = subparsers.add_parser("prepare", help="freeze inputs without replaying days")
    prepare.add_argument("--frozen-v1", type=Path, default=DEFAULT_FROZEN_V1)
    prepare.add_argument("--cache-root", type=Path, required=True)
    prepare.add_argument("--window-cache-root", type=Path, required=True)
    prepare.add_argument("--model-overlay-root", type=Path, required=True)
    prepare.add_argument("--window-cache-index", type=Path)

    run = subparsers.add_parser("run", help="execute or resume the frozen daily plan")
    run.add_argument("--plan", type=Path, required=True)
    run.add_argument("--python", type=Path, default=Path(sys.executable))
    run.add_argument("--days", nargs="*")
    run.add_argument("--workers", type=int, default=1)

    worker = subparsers.add_parser("_run-day", help=argparse.SUPPRESS)
    worker.add_argument("--plan", type=Path, required=True)
    worker.add_argument("--day", required=True)
    worker.add_argument("--staging-root", type=Path, required=True)

    args = parser.parse_args(argv)
    if args.command == "prepare":
        plan = prepare_execution_plan(
            frozen_v1_path=args.frozen_v1,
            cache_root=args.cache_root,
            window_cache_root=args.window_cache_root,
            model_overlay_root=args.model_overlay_root,
            window_cache_index_path=args.window_cache_index,
        )
        path = _write_plan(args.cache_root.expanduser().resolve(), plan)
        print(
            json.dumps(
                {
                    "prepared": True,
                    "formal_40day_replay_executed": False,
                    "ordered_day_count": 40,
                    "plan": str(path),
                    "plan_sha256": plan["canonical_plan_sha256"],
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if args.command == "run":
        result = execute_plan(
            plan_path=args.plan,
            python_executable=args.python,
            days=args.days,
            workers=args.workers,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    manifest = _run_authoritative_day(
        plan_path=args.plan.expanduser().resolve(),
        day=str(args.day),
        staging_root=args.staging_root,
    )
    print(
        json.dumps(
            {
                "day": str(args.day),
                "mechanics_manifest": str(manifest),
                "economic_outcomes_read": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
