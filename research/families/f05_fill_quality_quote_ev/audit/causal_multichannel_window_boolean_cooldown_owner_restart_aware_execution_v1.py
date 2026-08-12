#!/usr/bin/env python3
"""Execute and finalize the owner Boolean cooldown continuous replay.

This driver is deliberately separate from the daily full-path runner and its
runtime-critical policy bridge.  It binds the shared F03 restart calendar,
F03 control market/overlay provider, common initial economics, and the F05
owner cooldown policy into the restart-aware adapter.  Execution resumes from
paired, atomically admitted epoch checkpoints.

The result remains historical exchange-time, modeled-queue owner evidence.
It cannot claim strict queue, receive-time transport, research-supported,
action, or live authority.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import pickle
import shutil
import uuid
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd

from data_paths import data_root
from models import backtest_tick as bt
from models.replay.narrowgate_continuous_tick_adapter import (
    AdapterArmBinding,
    ReplayDayInput,
    compile_authoritative_epochs,
)
from models.replay.restart_aware_continuous_ab import canonical_sha256, sha256_file
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_native_40day_full_path_ml_ab as native_runner,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_restart_aware_continuous_execution_v1_3 as f03_binding,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_v9_10s_control_overlay_repair as control_repair,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_restart_aware_v1 as owner_abi,
)

ROOT = Path(__file__).resolve().parents[4]
IDENTITY = "causal_multichannel_window_boolean_cooldown_owner_restart_aware_execution_v1"
SCHEMA_VERSION = f"{IDENTITY}.v1"
ARMS = owner_abi.ARMS
CONTROL_ARM = owner_abi.CONTROL_ARM
CANDIDATE_ARM = owner_abi.CANDIDATE_ARM
DEFAULT_F03_CONTROL_PLAN = f03_binding.DEFAULT_OUTPUT_ROOT / f03_binding.PLAN_FILENAME
DATA_ROOT = data_root(ROOT)
DEFAULT_OUTPUT_ROOT = DATA_ROOT / (
    "reports/"
    "causal_multichannel_window_boolean_cooldown_owner_restart_aware_execution_v1_"
    "calendar_bridge_v1"
)
PLAN_FILENAME = "execution-plan.json"
PLAN_SUCCESS = "_PLAN_SUCCESS"
EXECUTION_DIRECTORY = "execution"
FINAL_DIRECTORY = "final"
FINAL_SUCCESS = "_FINAL_SUCCESS"
DEFAULT_BOOTSTRAP_DRAWS = 99_999
DEFAULT_BOOTSTRAP_SEED = 20260812
WARMUP_CONTEXT_DIRECTORY = "warmup-context"
WARMUP_CONTEXT_SCHEMA_VERSION = f"{SCHEMA_VERSION}.warmup_context"
DEFAULT_WARMUP_FEATURE_MANIFEST = DATA_ROOT / (
    "cache/"
    "f03_v9_10s_control_overlay_repair_v1/repair_feature_warmup/"
    "repair_feature_warmup_manifest.json"
)


class OwnerContinuousExecutionError(RuntimeError):
    """Raised before a drifted or incomplete owner continuous run advances."""


def _load_json(path: Path, *, role: str) -> dict[str, Any]:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise OwnerContinuousExecutionError(f"missing {role}: {resolved}")
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OwnerContinuousExecutionError(f"invalid {role}: {resolved}") from exc
    if not isinstance(payload, dict):
        raise OwnerContinuousExecutionError(f"{role} must be a JSON object")
    return payload


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.partial"
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.partial"
    try:
        with temporary.open("w", encoding="ascii") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_pickle(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.partial"
    try:
        with temporary.open("wb") as handle:
            pickle.dump(value, handle, protocol=pickle.HIGHEST_PROTOCOL)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _calendar_days(start_ts_ms: int, end_ts_ms: int) -> tuple[str, ...]:
    if end_ts_ms < start_ts_ms:
        raise OwnerContinuousExecutionError("warmup calendar interval is reversed")
    first = datetime.fromtimestamp(start_ts_ms / 1_000.0, tz=UTC).date()
    last = datetime.fromtimestamp(end_ts_ms / 1_000.0, tz=UTC).date()
    return tuple(
        (first + timedelta(days=offset)).isoformat()
        for offset in range((last - first).days + 1)
    )


def _required_warmup_context_days(
    *,
    framework_plan: Path,
    scored_days: Sequence[str],
) -> tuple[str, ...]:
    framework = owner_abi._load_framework_binding(  # noqa: SLF001
        Path(framework_plan).expanduser().resolve()
    )
    operations = tuple(framework["operations"])
    drains = [
        row.end_ts_ms - row.start_ts_ms
        for row in operations
        if row.kind == "cancel_drain"
    ]
    if not drains or min(drains) <= 0:
        raise OwnerContinuousExecutionError("restart framework lacks cancel drain")
    epochs = compile_authoritative_epochs(
        operations,
        panel_cancel_drain_ms=max(drains),
    )
    scored = set(scored_days)
    required: set[str] = set()
    for epoch in epochs:
        required.update(
            day
            for day in _calendar_days(
                epoch.warmup_lookback_start_ts_ms,
                epoch.start_ts_ms,
            )
            if day not in scored
        )
    return tuple(sorted(required))


def _warmup_feature_artifact(day: str) -> tuple[dict[str, Any], dict[str, Any]]:
    manifest = _load_json(
        DEFAULT_WARMUP_FEATURE_MANIFEST,
        role="F03 warmup feature manifest",
    )
    if (
        manifest.get("feature_semantics_version") != 6
        or manifest.get("feature_ready_offset_ms") != 10_000
        or manifest.get("feature_timestamp_semantics")
        != "left_label_bucket_end"
        or manifest.get("feature_cutoff_semantics")
        != "strict_exclusive_completed_bucket_end"
    ):
        raise OwnerContinuousExecutionError("warmup feature semantics drifted")
    rows = manifest.get("daily_files")
    if not isinstance(rows, list):
        raise OwnerContinuousExecutionError("warmup feature manifest lacks days")
    selected = next(
        (row for row in rows if isinstance(row, Mapping) and row.get("day") == day),
        None,
    )
    if selected is None:
        raise OwnerContinuousExecutionError(f"missing warmup features for {day}")
    path = DEFAULT_WARMUP_FEATURE_MANIFEST.parent / str(selected.get("file", ""))
    artifact = _artifact(path, role=f"{day} warmup features")
    if (
        artifact["sha256"] != selected.get("sha256")
        or artifact["size_bytes"] != int(selected.get("size_bytes", -1))
    ):
        raise OwnerContinuousExecutionError(f"{day} warmup feature receipt drifted")
    return artifact, _artifact(
        DEFAULT_WARMUP_FEATURE_MANIFEST,
        role="F03 warmup feature manifest",
    )


def _warmup_context_inputs(
    *,
    day: str,
    control_policy: Mapping[str, Any],
) -> dict[str, Any]:
    feature, feature_manifest = _warmup_feature_artifact(day)
    trade_path = bt.RAW_TRADES_DIR / bt.SYMBOL / f"{bt.SYMBOL}-trades-{day}.csv"
    if not trade_path.is_file():
        gz_path = trade_path.with_suffix(".csv.gz")
        trade_path = gz_path if gz_path.is_file() else trade_path
    trades = _artifact(trade_path, role=f"{day} official individual trades")
    bundle_meta = _verify_artifact(
        control_policy["bundle_meta"],
        role="control bundle metadata",
    )
    model_dir = bundle_meta.parent
    feature_frame = pd.read_parquet(Path(feature["path"]))
    if not isinstance(feature_frame.index, pd.DatetimeIndex):
        raise OwnerContinuousExecutionError("warmup features lack a DatetimeIndex")
    if feature_frame.index.has_duplicates or not feature_frame.index.is_monotonic_increasing:
        raise OwnerContinuousExecutionError("warmup feature clock is not strictly ordered")
    model_artifacts: dict[str, dict[str, Any]] = {}
    for head in control_repair.POLICY_HEADS:
        model = _artifact(model_dir / f"{head}.txt", role=f"warmup {head} model")
        metadata = _artifact(
            model_dir / f"{head}_meta.json",
            role=f"warmup {head} metadata",
        )
        meta = _load_json(Path(metadata["path"]), role=f"warmup {head} metadata")
        columns = tuple(str(value) for value in meta.get("feature_cols", ()))
        if not columns or any(column not in feature_frame.columns for column in columns):
            raise OwnerContinuousExecutionError(
                f"warmup features do not satisfy the {head} model schema"
            )
        model_artifacts[head] = {
            "model": model,
            "metadata": metadata,
            "feature_columns_sha256": canonical_sha256(list(columns)),
        }
    return {
        "day": day,
        "trades": trades,
        "features": feature,
        "feature_manifest": feature_manifest,
        "model_bundle_identity_sha256": str(
            control_policy["model_bundle_identity_sha256"]
        ),
        "models": model_artifacts,
        "source_profile": "native",
        "feature_ready_offset_ms": 10_000,
        "economic_denominator_day": False,
    }


def _materialize_warmup_context(
    *,
    day: str,
    control_policy: Mapping[str, Any],
    output_root: Path,
) -> dict[str, Any]:
    inputs = _warmup_context_inputs(day=day, control_policy=control_policy)
    identity_payload = {
        "schema_version": WARMUP_CONTEXT_SCHEMA_VERSION,
        "identity": IDENTITY,
        "day": day,
        "inputs": inputs,
        "semantics": {
            "role": "past_only_restart_warmup_context",
            "scored_day": False,
            "orders_generated": False,
            "fills_generated": False,
            "economic_outcomes_read": False,
            "ready_clock": "feature_label_plus_10000ms",
            "first_incomplete_cross_midnight_bucket": "excluded",
        },
    }
    identity_sha = canonical_sha256(identity_payload)
    root = (
        Path(output_root).expanduser().resolve()
        / WARMUP_CONTEXT_DIRECTORY
        / day
        / identity_sha
    )
    data_path = root / "context.pkl"
    manifest_path = root / "manifest.json"
    success_path = root / "_SUCCESS"
    if manifest_path.is_file() and data_path.is_file() and success_path.is_file():
        binding = {
            "day": day,
            "kind": "warmup_context_only",
            "identity_sha256": identity_sha,
            "source_identity_sha256": identity_sha,
            "source_profile": "native",
            "manifest": _artifact(manifest_path, role=f"{day} warmup manifest"),
            "data": _artifact(data_path, role=f"{day} warmup data"),
        }
        return _validate_warmup_context_binding(binding)

    features = pd.read_parquet(Path(inputs["features"]["path"])).sort_index()
    labels_ms = (
        features.index.to_numpy(dtype="datetime64[ns]").astype(np.int64) // 1_000_000
    )
    ready_ms = labels_ms + int(inputs["feature_ready_offset_ms"])
    day_start_ms = int(np.datetime64(day, "ms").astype(np.int64))
    day_end_ms = day_start_ms + 86_400_000
    keep = (ready_ms >= day_start_ms) & (ready_ms < day_end_ms)
    features = features.iloc[np.flatnonzero(keep)]
    ready_ms = ready_ms[keep].astype(np.int64, copy=False)
    if len(ready_ms) < 8_000 or np.any(np.diff(ready_ms) <= 0):
        raise OwnerContinuousExecutionError(f"{day} warmup ready grid is incomplete")

    try:
        import lightgbm as lgb
    except ImportError as exc:
        raise OwnerContinuousExecutionError("LightGBM is required for warmup binding") from exc
    predictions: list[np.ndarray] = []
    for head in control_repair.POLICY_HEADS:
        metadata = inputs["models"][head]["metadata"]
        meta = _load_json(Path(metadata["path"]), role=f"warmup {head} metadata")
        columns = list(meta["feature_cols"])
        values = np.asarray(
            lgb.Booster(
                model_file=str(inputs["models"][head]["model"]["path"])
            ).predict(features[columns]),
            dtype=np.float64,
        )
        if values.shape != ready_ms.shape or not np.isfinite(values).all():
            raise OwnerContinuousExecutionError(f"{day} {head} warmup inference failed")
        if head == "vol_10s":
            values = np.maximum(values, 0.0)
        elif head in {"tox_bid_10s", "tox_ask_10s"}:
            values = np.clip(values, 0.0, 1.0)
        predictions.append(values)

    trades = bt._read_individual_trade_csv(Path(inputs["trades"]["path"]))  # noqa: SLF001
    trade_clock = trades["transact_time"].to_numpy(dtype=np.int64, copy=False)
    trades = trades.loc[(trade_clock >= day_start_ms) & (trade_clock < day_end_ms)].copy()
    if trades.empty or not trades["transact_time"].is_monotonic_increasing:
        raise OwnerContinuousExecutionError(f"{day} warmup trades are incomplete")
    payload = {
        "schema_version": WARMUP_CONTEXT_SCHEMA_VERSION,
        "day": day,
        "identity_sha256": identity_sha,
        "window": SimpleNamespace(trades=trades, ml_data=None),
        "ml_data": (ready_ms, *predictions),
    }
    _atomic_pickle(data_path, payload)
    data = _artifact(data_path, role=f"{day} warmup context")
    manifest = {
        **identity_payload,
        "identity_sha256": identity_sha,
        "data": data,
        "trade_row_count": int(len(trades)),
        "model_ready_row_count": int(len(ready_ms)),
        "ready_min_ts_ms": int(ready_ms[0]),
        "ready_max_ts_ms": int(ready_ms[-1]),
        "economic_outcomes_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    _atomic_json(manifest_path, manifest)
    _atomic_text(success_path, sha256_file(manifest_path) + "\n")
    binding = {
        "day": day,
        "kind": "warmup_context_only",
        "identity_sha256": identity_sha,
        "source_identity_sha256": identity_sha,
        "source_profile": "native",
        "manifest": _artifact(manifest_path, role=f"{day} warmup manifest"),
        "data": data,
    }
    return _validate_warmup_context_binding(binding)


def _validate_warmup_context_binding(row: Mapping[str, Any]) -> dict[str, Any]:
    day = str(row.get("day", ""))
    if row.get("kind") != "warmup_context_only" or not day:
        raise OwnerContinuousExecutionError("warmup context binding is invalid")
    manifest_path = _verify_artifact(row.get("manifest") or {}, role=f"{day} warmup manifest")
    data_path = _verify_artifact(row.get("data") or {}, role=f"{day} warmup data")
    manifest = _load_json(manifest_path, role=f"{day} warmup manifest")
    identity_payload = {
        key: manifest[key]
        for key in ("schema_version", "identity", "day", "inputs", "semantics")
    }
    expected = canonical_sha256(identity_payload)
    if (
        manifest.get("identity_sha256") != expected
        or row.get("identity_sha256") != expected
        or row.get("source_identity_sha256") != expected
        or manifest.get("data", {}).get("sha256") != sha256_file(data_path)
        or manifest.get("economic_outcomes_read") is not False
        or manifest.get("action_authorized") is not False
        or manifest.get("live_authorized") is not False
    ):
        raise OwnerContinuousExecutionError(f"{day} warmup context identity drifted")
    marker = manifest_path.parent / "_SUCCESS"
    if not marker.is_file() or marker.read_text(encoding="ascii").strip() != sha256_file(
        manifest_path
    ):
        raise OwnerContinuousExecutionError(f"{day} warmup context admission drifted")
    with data_path.open("rb") as handle:
        payload = pickle.load(handle)
    if (
        not isinstance(payload, Mapping)
        or payload.get("day") != day
        or payload.get("identity_sha256") != expected
        or getattr(payload.get("window"), "ml_data", object()) is not None
        or not isinstance(payload.get("ml_data"), tuple)
        or len(payload["ml_data"]) != 6
    ):
        raise OwnerContinuousExecutionError(f"{day} warmup context payload drifted")
    normalized = dict(row)
    normalized["manifest"] = _artifact(manifest_path, role=f"{day} warmup manifest")
    normalized["data"] = _artifact(data_path, role=f"{day} warmup data")
    return normalized


def _artifact(path: Path, *, role: str) -> dict[str, Any]:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise OwnerContinuousExecutionError(f"missing {role}: {resolved}")
    return {
        "path": str(resolved),
        "sha256": sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def _verify_artifact(row: Mapping[str, Any], *, role: str) -> Path:
    path = Path(str(row.get("path", ""))).expanduser().resolve()
    if not path.is_file():
        raise OwnerContinuousExecutionError(f"missing {role}: {path}")
    if int(row.get("size_bytes", -1)) != path.stat().st_size:
        raise OwnerContinuousExecutionError(f"{role} size drifted")
    if str(row.get("sha256", "")) != sha256_file(path):
        raise OwnerContinuousExecutionError(f"{role} SHA256 drifted")
    return path


def _runtime_artifacts() -> dict[str, dict[str, Any]]:
    paths = {
        "execution_driver": Path(__file__),
        "owner_restart_adapter": Path(owner_abi.__file__),
        "shared_continuous_adapter": ROOT
        / "models/replay/narrowgate_continuous_tick_adapter.py",
        "continuous_accounting": ROOT / "models/replay/continuous_accounting.py",
        "replay_state_checkpoint": ROOT
        / "models/replay/replay_state_checkpoint.py",
        "tick_replay_python": ROOT / "models/backtest_tick.py",
        "owner_runtime_policy": ROOT
        / "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_runtime_policy.py",
        "owner_replay_emitter": ROOT
        / "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_replay_emitter.py",
        "owner_native_observation_cache": ROOT
        / "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_native_observation_cache.py",
        "owner_feature_schema": ROOT
        / "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_features.py",
        "f03_control_binding": Path(f03_binding.__file__),
    }
    return {name: _artifact(path, role=name) for name, path in paths.items()}


def _load_continuation_artifact(
    path: Path,
    *,
    expected_policy_sha256: str,
) -> dict[str, Any]:
    resolved = Path(path).expanduser().resolve()
    payload = _load_json(resolved, role="owner continuation artifact")
    if (
        payload.get("schema_version") != f"{SCHEMA_VERSION}.owner_continuation"
        or payload.get("identity") != IDENTITY
        or payload.get("owner_continue_after_daily") is not True
        or payload.get("daily_economic_outcomes_read_for_decision") is not False
        or payload.get("continuous_economic_outcomes_read") is not False
        or payload.get("policy_sha256") != expected_policy_sha256
    ):
        raise OwnerContinuousExecutionError("owner continuation artifact drifted")
    expected = str(payload.pop("continuation_identity_sha256", ""))
    if canonical_sha256(payload) != expected:
        raise OwnerContinuousExecutionError("owner continuation identity drifted")
    payload["continuation_identity_sha256"] = expected
    return {
        "authorized": True,
        "mode": "bound_owner_continuation_artifact",
        "artifact": _artifact(resolved, role="owner continuation artifact"),
        "continuation_identity_sha256": expected,
        "daily_economic_outcomes_read_for_decision": False,
        "driver_read_daily_economic_outcomes_for_decision": False,
        "continuous_economic_outcomes_read": False,
    }


def _continuation_binding(
    *,
    owner_continue_after_daily: bool,
    continuation_artifact: Path | None,
    expected_policy_sha256: str,
) -> tuple[dict[str, Any] | None, list[str]]:
    if owner_continue_after_daily and continuation_artifact is not None:
        raise OwnerContinuousExecutionError(
            "choose either --owner-continue-after-daily or an artifact, not both"
        )
    if continuation_artifact is not None:
        return (
            _load_continuation_artifact(
                continuation_artifact,
                expected_policy_sha256=expected_policy_sha256,
            ),
            [],
        )
    if owner_continue_after_daily:
        payload = {
            "authorized": True,
            "mode": "explicit_owner_cli_precommit",
            "identity": IDENTITY,
            "policy_sha256": expected_policy_sha256,
            "daily_economic_outcomes_read_for_decision": None,
            "driver_read_daily_economic_outcomes_for_decision": False,
            "owner_outcome_blind_precommit_attested": False,
            "outcome_informed_owner_override_possible": True,
            "continuous_economic_outcomes_read": False,
            "owner_risk_accepted_route": True,
        }
        return payload | {"continuation_identity_sha256": canonical_sha256(payload)}, []
    return None, ["owner_continuation_after_daily_not_precommitted"]


def _raw_f03_plan(path: Path) -> dict[str, Any]:
    resolved = Path(path).expanduser().resolve()
    payload = _load_json(resolved, role="F03 concrete control plan")
    marker = resolved.parent / f03_binding.PLAN_SUCCESS
    if not marker.is_file() or marker.read_text(encoding="ascii").strip() != sha256_file(
        resolved
    ):
        raise OwnerContinuousExecutionError("F03 concrete plan admission drifted")
    if payload.get("schema_version") != f03_binding.PLAN_SCHEMA_VERSION:
        raise OwnerContinuousExecutionError("F03 concrete plan schema drifted")
    normalized = dict(payload)
    expected = str(normalized.pop("plan_identity_sha256", ""))
    if canonical_sha256(normalized) != expected:
        raise OwnerContinuousExecutionError("F03 concrete plan identity drifted")
    payload["plan_identity_sha256"] = expected
    return payload


def _inspect_f03_control_plan(path: Path) -> tuple[dict[str, Any] | None, list[str]]:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        return None, ["f03_shared_control_plan_missing"]
    try:
        payload = _raw_f03_plan(resolved)
    except OwnerContinuousExecutionError as exc:
        return (
            {
                "path": str(resolved),
                "valid": False,
                "error": str(exc),
            },
            ["f03_shared_control_plan_invalid"],
        )
    control = (payload.get("policy_artifacts") or {}).get(CONTROL_ARM)
    if not isinstance(control, Mapping):
        return (
            {
                "path": str(resolved),
                "file_sha256": sha256_file(resolved),
                "plan_identity_sha256": payload["plan_identity_sha256"],
                "operation_tape_sha256": payload.get("operation_tape_sha256"),
                "control_policy_bound": False,
                "f03_plan_blockers": list(payload.get("blockers", ())),
            },
            ["f03_shared_control_market_overlay_initial_state_unbound"],
        )
    manifest_row = control.get("manifest") or {}
    manifest_path = Path(str(manifest_row.get("path", ""))).expanduser().resolve()
    try:
        observed = f03_binding._load_policy_manifest(  # noqa: SLF001
            manifest_path,
            expected_arm=CONTROL_ARM,
            verify_market_window_hashes=False,
        )
    except Exception as exc:  # the imported binding owns its detailed contract
        return (
            {
                "path": str(resolved),
                "file_sha256": sha256_file(resolved),
                "plan_identity_sha256": payload["plan_identity_sha256"],
                "control_policy_bound": False,
                "error": f"{type(exc).__name__}: {exc}",
            },
            ["f03_shared_control_policy_artifacts_invalid"],
        )
    if dict(control) != observed:
        raise OwnerContinuousExecutionError("F03 control policy payload drifted")
    return (
        {
            "path": str(resolved),
            "file_sha256": sha256_file(resolved),
            "plan_identity_sha256": payload["plan_identity_sha256"],
            "operation_tape_sha256": str(payload.get("operation_tape_sha256", "")),
            "control_policy_bound": True,
            "control_policy": observed,
        },
        [],
    )


def preflight(
    *,
    daily_panel: Path = owner_abi.DEFAULT_DAILY_PANEL,
    framework_plan: Path = owner_abi.DEFAULT_FRAMEWORK_PLAN,
    f03_control_plan: Path = DEFAULT_F03_CONTROL_PLAN,
    policy_path: Path = owner_abi.DEFAULT_POLICY,
    policy_sha256: str = owner_abi.DEFAULT_POLICY_SHA256,
    owner_continue_after_daily: bool = False,
    continuation_artifact: Path | None = None,
) -> dict[str, Any]:
    """Inspect real prerequisites without reading continuous outcomes."""

    owner = owner_abi.prepare_preflight(
        daily_panel=daily_panel,
        framework_plan=framework_plan,
        policy_path=policy_path,
        expected_policy_sha256=policy_sha256,
    )
    blockers = list(owner.get("blockers", ()))
    continuation, continuation_blockers = _continuation_binding(
        owner_continue_after_daily=owner_continue_after_daily,
        continuation_artifact=continuation_artifact,
        expected_policy_sha256=policy_sha256,
    )
    blockers.extend(continuation_blockers)
    f03_control, f03_blockers = _inspect_f03_control_plan(f03_control_plan)
    blockers.extend(f03_blockers)
    framework = owner.get("framework") or {}
    warmup_context_preflight: dict[str, Any] = {
        "required_days": [],
        "inputs": {},
        "scored_day_count_unchanged": True,
    }
    if f03_control is not None and f03_control.get("control_policy_bound"):
        if f03_control.get("operation_tape_sha256") != framework.get(
            "operation_tape_sha256"
        ):
            blockers.append("f03_control_and_shared_restart_operation_tapes_differ")
        control_policy = f03_control.get("control_policy")
        try:
            if not isinstance(control_policy, Mapping):
                raise OwnerContinuousExecutionError(
                    "F03 control inspection lacks its policy payload"
                )
            required_days = _required_warmup_context_days(
                framework_plan=Path(str(framework["path"])),
                scored_days=tuple(control_policy["days"]),
            )
            warmup_context_preflight = {
                "required_days": list(required_days),
                "inputs": {
                    day: _warmup_context_inputs(
                        day=day,
                        control_policy=control_policy,
                    )
                    for day in required_days
                },
                "scored_day_count_unchanged": True,
                "economic_denominator_day_count": len(control_policy["days"]),
            }
        except (KeyError, OSError, OwnerContinuousExecutionError) as exc:
            warmup_context_preflight = {
                "required_days": [],
                "inputs": {},
                "scored_day_count_unchanged": True,
                "error": f"{type(exc).__name__}: {exc}",
            }
            blockers.append("initial_restart_warmup_context_unbound")
    unique_blockers = list(dict.fromkeys(str(value) for value in blockers))
    payload = {
        "schema_version": f"{SCHEMA_VERSION}.preflight",
        "identity": IDENTITY,
        "owner_restart_adapter_preflight": owner,
        "f03_shared_control_binding": f03_control,
        "warmup_context_preflight": warmup_context_preflight,
        "owner_continuation": continuation,
        "execution_eligible": not unique_blockers,
        "blockers": unique_blockers,
        "execution_contract": {
            "same_restart_manifest": True,
            "same_market_inputs": True,
            "arm_economic_state_isolated": True,
            "utc_midnight_is_accounting_only": True,
            "planned_restart_requires_cancel_ack_drain": True,
            "candidate_missing_m2_fallback": "exact_control_for_entire_epoch",
            "engine": "python",
            "resume_unit": "paired_authoritative_restart_epoch",
            "atomic_admission": "paired_receipt_plus_two_checkpoint_markers",
        },
        "economic_outcomes_read": False,
        "permissions": {
            "strict_queue_authority": False,
            "receive_time_transport_authority": False,
            "research_supported": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    payload["preflight_identity_sha256"] = canonical_sha256(payload)
    return payload


def _identity_hashes(
    *,
    control_policy: Mapping[str, Any],
    params: Mapping[str, Any],
    runtime_artifacts: Mapping[str, Mapping[str, Any]],
) -> dict[str, str]:
    p3_sha256 = str(params.get("fill_probability_artifact_sha256", ""))
    if len(p3_sha256) != 64:
        raise OwnerContinuousExecutionError("F03 control lacks a bound P3 SHA256")
    model_sha256 = str(control_policy.get("model_bundle_identity_sha256", ""))
    hashes = {
        "config_sha256": str(control_policy["operational_config"]["sha256"]),
        "code_sha256": canonical_sha256(
            {name: row["sha256"] for name, row in sorted(runtime_artifacts.items())}
        ),
        "model_sha256": model_sha256,
        "p3_sha256": p3_sha256,
        "feature_dag_sha256": str(control_policy["feature_dag"]["sha256"]),
        "execution_abi_sha256": canonical_sha256(
            {
                "f03_execution_abi": control_policy.get("execution_abi"),
                "tick_replay_python": runtime_artifacts["tick_replay_python"][
                    "sha256"
                ],
            }
        ),
        "baseline_identity_sha256": str(
            control_policy["baseline_identity"]["sha256"]
        ),
    }
    if any(len(value) != 64 for value in hashes.values()):
        raise OwnerContinuousExecutionError("owner execution identity hashes are incomplete")
    return hashes


def _adapter_plan_identity(
    *,
    owner_preflight: Mapping[str, Any],
    control_policy: Mapping[str, Any],
    owner_policy_sha256: str,
    runtime_identity_sha256: str,
    warmup_context_days: Mapping[str, Mapping[str, Any]],
) -> str:
    initial_path = Path(str(control_policy["initial_state"]["path"]))
    initial_states = {
        arm: f03_binding._initial_state(initial_path, arm=arm).to_dict()  # noqa: SLF001
        for arm in ARMS
    }
    return canonical_sha256(
        {
            "identity": owner_abi.IDENTITY,
            "preflight_identity_sha256": owner_preflight[
                "preflight_identity_sha256"
            ],
            "operation_tape_sha256": owner_preflight["framework"][
                "operation_tape_sha256"
            ],
            "policy_sha256": owner_policy_sha256,
            "arm_policy_identities": {
                CONTROL_ARM: str(control_policy["policy_identity_sha256"]),
                CANDIDATE_ARM: owner_policy_sha256,
            },
            "initial_states": initial_states,
            "runtime_identity_sha256": runtime_identity_sha256,
            "warmup_context_identities": {
                day: row["identity_sha256"]
                for day, row in sorted(warmup_context_days.items())
            },
        }
    )


def prepare_execution_plan(
    *,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    daily_panel: Path = owner_abi.DEFAULT_DAILY_PANEL,
    framework_plan: Path = owner_abi.DEFAULT_FRAMEWORK_PLAN,
    f03_control_plan: Path = DEFAULT_F03_CONTROL_PLAN,
    policy_path: Path = owner_abi.DEFAULT_POLICY,
    policy_sha256: str = owner_abi.DEFAULT_POLICY_SHA256,
    observation_cache_root: Path = owner_abi.DEFAULT_OBSERVATION_CACHE,
    owner_continue_after_daily: bool = False,
    continuation_artifact: Path | None = None,
) -> dict[str, Any]:
    inspection = preflight(
        daily_panel=daily_panel,
        framework_plan=framework_plan,
        f03_control_plan=f03_control_plan,
        policy_path=policy_path,
        policy_sha256=policy_sha256,
        owner_continue_after_daily=owner_continue_after_daily,
        continuation_artifact=continuation_artifact,
    )
    runtime = _runtime_artifacts()
    f03_control = inspection.get("f03_shared_control_binding") or {}
    control_policy = f03_control.get("control_policy")
    root = Path(output_root).expanduser().resolve()
    warmup_context_days: dict[str, dict[str, Any]] = {}
    identity_hashes: dict[str, str] | None = None
    adapter_plan_identity_sha256: str | None = None
    if isinstance(control_policy, Mapping):
        for day in inspection["warmup_context_preflight"]["required_days"]:
            warmup_context_days[str(day)] = _materialize_warmup_context(
                day=str(day),
                control_policy=control_policy,
                output_root=root,
            )
        params = native_runner._load_formal_base_params(  # noqa: SLF001
            Path(str(control_policy["operational_config"]["path"]))
        )
        identity_hashes = _identity_hashes(
            control_policy=control_policy,
            params=params,
            runtime_artifacts=runtime,
        )
        adapter_plan_identity_sha256 = _adapter_plan_identity(
            owner_preflight=inspection["owner_restart_adapter_preflight"],
            control_policy=control_policy,
            owner_policy_sha256=policy_sha256,
            runtime_identity_sha256=identity_hashes["code_sha256"],
            warmup_context_days=warmup_context_days,
        )
    execution_output_root = (
        root / EXECUTION_DIRECTORY / str(adapter_plan_identity_sha256)
        if adapter_plan_identity_sha256 is not None
        else root / EXECUTION_DIRECTORY / "blocked"
    )
    payload = {
        "schema_version": f"{SCHEMA_VERSION}.plan",
        "identity": IDENTITY,
        "created_at_utc": datetime.now(tz=UTC).isoformat(),
        "preflight": inspection,
        "daily_panel": inspection["owner_restart_adapter_preflight"].get(
            "daily_fresh_start_prerequisite"
        ),
        "framework": inspection["owner_restart_adapter_preflight"].get("framework"),
        "f03_shared_control_binding": f03_control,
        "owner_policy": inspection["owner_restart_adapter_preflight"].get(
            "owner_policy"
        ),
        "owner_continuation": inspection.get("owner_continuation"),
        "warmup_context_days": warmup_context_days,
        "observation_cache_root": str(
            Path(observation_cache_root).expanduser().resolve()
        ),
        "owner_feature_identity_hashes": identity_hashes,
        "adapter_plan_identity_sha256": adapter_plan_identity_sha256,
        "runtime_artifacts": runtime,
        "output_root": str(root),
        "execution_output_root": str(execution_output_root),
        "final_output_root": str(root / FINAL_DIRECTORY),
        "execution_eligible": inspection["execution_eligible"],
        "blockers": list(inspection["blockers"]),
        "economic_outcomes_read": False,
        "economic_results_aggregated": False,
        "permissions": inspection["permissions"],
    }
    payload["plan_identity_sha256"] = canonical_sha256(payload)
    root.mkdir(parents=True, exist_ok=True)
    plan_path = root / PLAN_FILENAME
    _atomic_json(plan_path, payload)
    _atomic_text(root / PLAN_SUCCESS, sha256_file(plan_path) + "\n")
    return payload


def validate_execution_plan(
    path: Path,
    *,
    require_eligible: bool,
) -> dict[str, Any]:
    resolved = Path(path).expanduser().resolve()
    payload = _load_json(resolved, role="owner continuous execution plan")
    marker = resolved.parent / PLAN_SUCCESS
    if not marker.is_file() or marker.read_text(encoding="ascii").strip() != sha256_file(
        resolved
    ):
        raise OwnerContinuousExecutionError("owner execution plan admission drifted")
    if (
        payload.get("schema_version") != f"{SCHEMA_VERSION}.plan"
        or payload.get("identity") != IDENTITY
    ):
        raise OwnerContinuousExecutionError("owner execution plan schema drifted")
    normalized = dict(payload)
    expected = str(normalized.pop("plan_identity_sha256", ""))
    if canonical_sha256(normalized) != expected:
        raise OwnerContinuousExecutionError("owner execution plan identity drifted")
    payload["plan_identity_sha256"] = expected
    for name, row in (payload.get("runtime_artifacts") or {}).items():
        _verify_artifact(row, role=f"runtime artifact {name}")
    warmup_context_days = payload.get("warmup_context_days")
    if not isinstance(warmup_context_days, Mapping):
        raise OwnerContinuousExecutionError("owner plan lacks warmup context mapping")
    payload["warmup_context_days"] = {
        str(day): _validate_warmup_context_binding(row)
        for day, row in warmup_context_days.items()
    }
    required_warmup_days = tuple(
        (payload.get("preflight") or {})
        .get("warmup_context_preflight", {})
        .get("required_days", ())
    )
    if tuple(payload["warmup_context_days"]) != required_warmup_days:
        raise OwnerContinuousExecutionError(
            "owner plan warmup context denominator drifted"
        )
    permissions = payload.get("permissions") or {}
    if any(
        permissions.get(name) is not False
        for name in (
            "strict_queue_authority",
            "receive_time_transport_authority",
            "research_supported",
            "action_authorized",
            "live_authorized",
        )
    ):
        raise OwnerContinuousExecutionError("owner execution plan exceeded authority")
    if require_eligible and payload.get("execution_eligible") is not True:
        raise OwnerContinuousExecutionError(
            "owner continuous execution blocked: "
            + ",".join(str(value) for value in payload.get("blockers", ()))
        )
    if payload.get("execution_eligible") is True:
        if payload.get("blockers"):
            raise OwnerContinuousExecutionError("eligible owner plan retains blockers")
        if (payload.get("owner_continuation") or {}).get("authorized") is not True:
            raise OwnerContinuousExecutionError("eligible owner plan lacks continuation")
        if not isinstance(
            (payload.get("f03_shared_control_binding") or {}).get("control_policy"),
            Mapping,
        ):
            raise OwnerContinuousExecutionError("eligible owner plan lacks F03 control")
        if len(str(payload.get("adapter_plan_identity_sha256", ""))) != 64:
            raise OwnerContinuousExecutionError(
                "eligible owner plan lacks shared adapter identity"
            )
    return payload


class SharedF03ControlInputProvider:
    """Use the exact F03 control window/overlay for both economic arms."""

    def __init__(
        self,
        control_policy: Mapping[str, Any],
        warmup_context_days: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> None:
        self._delegate = f03_binding._BoundInputProvider(  # noqa: SLF001
            {CONTROL_ARM: control_policy}
        )
        self._warmup_context_days = {
            str(day): _validate_warmup_context_binding(row)
            for day, row in (warmup_context_days or {}).items()
        }
        self._warmup_cache: dict[str, ReplayDayInput] = {}

    def load_day(self, *, day: str) -> ReplayDayInput:
        if day in self._warmup_context_days:
            cached = self._warmup_cache.get(day)
            if cached is not None:
                return cached
            binding = self._warmup_context_days[day]
            data_path = _verify_artifact(
                binding["data"],
                role=f"{day} warmup context data",
            )
            with data_path.open("rb") as handle:
                payload = pickle.load(handle)
            row = ReplayDayInput(
                day=day,
                window=payload["window"],
                ml_data=payload["ml_data"],
                market_window_sha256=str(binding["data"]["sha256"]),
                overlay_identity_sha256=str(binding["identity_sha256"]),
                source_identity_sha256=str(binding["source_identity_sha256"]),
                source_profile=str(binding["source_profile"]),
                exact_queue_authority=False,
                exact_lifecycle_authority=False,
            )
            row.validate()
            self._warmup_cache[day] = row
            return row
        row = self._delegate.load_day(arm_id=CONTROL_ARM, day=day)
        row.validate()
        # The owner study is modeled-queue sensitivity even when its market
        # source happened to originate from a native day.
        demoted = replace(
            row,
            exact_queue_authority=False,
            exact_lifecycle_authority=False,
        )
        demoted.validate()
        return demoted


def _build_adapter_from_plan(
    plan: Mapping[str, Any],
) -> owner_abi.OwnerBooleanCooldownRestartAwareAdapter:
    control_binding = plan["f03_shared_control_binding"]
    control_policy = control_binding["control_policy"]
    current_f03, blockers = _inspect_f03_control_plan(
        Path(str(control_binding["path"]))
    )
    if blockers or current_f03 is None:
        raise OwnerContinuousExecutionError("F03 shared control binding no longer validates")
    if current_f03.get("plan_identity_sha256") != control_binding.get(
        "plan_identity_sha256"
    ):
        raise OwnerContinuousExecutionError("F03 shared control plan drifted")
    owner_preflight = owner_abi.prepare_preflight(
        daily_panel=Path(str(plan["daily_panel"]["path"])),
        framework_plan=Path(str(plan["framework"]["path"])),
        policy_path=Path(str(plan["owner_policy"]["path"])),
        expected_policy_sha256=str(plan["owner_policy"]["sha256"]),
    )
    owner_abi.validate_preflight(owner_preflight, require_eligible=True)
    if owner_preflight["preflight_identity_sha256"] != (
        plan["preflight"]["owner_restart_adapter_preflight"][
            "preflight_identity_sha256"
        ]
    ):
        raise OwnerContinuousExecutionError("owner restart preflight drifted")
    framework = owner_abi._load_framework_binding(  # noqa: SLF001
        Path(str(plan["framework"]["path"]))
    )
    operations = tuple(framework["operations"])
    if framework["operation_tape_sha256"] != control_binding[
        "operation_tape_sha256"
    ]:
        raise OwnerContinuousExecutionError("F03/control operation tape drifted")
    drains = [
        row.end_ts_ms - row.start_ts_ms
        for row in operations
        if row.kind == "cancel_drain"
    ]
    if not drains or min(drains) <= 0:
        raise OwnerContinuousExecutionError("shared operation tape lacks cancel drain")
    params = native_runner._load_formal_base_params(  # noqa: SLF001
        Path(str(control_policy["operational_config"]["path"]))
    )
    params = dict(params)
    params["dynamic_fill_hazard_action_enabled"] = False
    params["buy_fill_selection_live_enabled"] = False
    bindings = {
        CONTROL_ARM: AdapterArmBinding(
            arm_id=CONTROL_ARM,
            params=dict(params),
            policy_identity_sha256=str(control_policy["policy_identity_sha256"]),
            cadence_ms=int(control_policy["cadence_ms"]),
        ),
        CANDIDATE_ARM: AdapterArmBinding(
            arm_id=CANDIDATE_ARM,
            params=dict(params),
            policy_identity_sha256=str(plan["owner_policy"]["sha256"]),
            cadence_ms=int(control_policy["cadence_ms"]),
        ),
    }
    initial_path = Path(str(control_policy["initial_state"]["path"]))
    initial_states = {
        arm: f03_binding._initial_state(initial_path, arm=arm)  # noqa: SLF001
        for arm in ARMS
    }
    provider = SharedF03ControlInputProvider(
        control_policy,
        plan["warmup_context_days"],
    )
    feature_provider = owner_abi.DailyObservationCacheEpochProvider(
        Path(str(plan["observation_cache_root"])),
        identity_hashes=plan["owner_feature_identity_hashes"],
    )
    return owner_abi.OwnerBooleanCooldownRestartAwareAdapter(
        preflight=owner_preflight,
        plan_identity_sha256=str(plan["adapter_plan_identity_sha256"]),
        operations=operations,
        arm_bindings=bindings,
        shared_input_provider=provider,
        initial_states=initial_states,
        feature_provider=feature_provider,
        output_root=Path(str(plan["execution_output_root"])),
        panel_cancel_drain_ms=max(drains),
        policy_path=Path(str(plan["owner_policy"]["path"])),
        expected_policy_sha256=str(plan["owner_policy"]["sha256"]),
        runtime_identity_sha256=str(
            plan["owner_feature_identity_hashes"]["code_sha256"]
        ),
        warmup_context_identities={
            day: row["identity_sha256"]
            for day, row in plan["warmup_context_days"].items()
        },
    )


def run_prepared_plan(
    plan_path: Path,
    *,
    max_epochs: int | None = None,
    adapter: owner_abi.OwnerBooleanCooldownRestartAwareAdapter | None = None,
) -> dict[str, Any]:
    plan = validate_execution_plan(plan_path, require_eligible=True)
    concrete = _build_adapter_from_plan(plan) if adapter is None else adapter
    result = concrete.run(max_epochs=max_epochs)
    return result | {
        "execution_identity": IDENTITY,
        "owner_continuation_identity_sha256": plan["owner_continuation"][
            "continuation_identity_sha256"
        ],
        "economic_results_aggregated": False,
        "strict_queue_authority": False,
        "receive_time_transport_authority": False,
        "research_supported": False,
        "action_authorized": False,
        "live_authorized": False,
    }


def _verified_json_with_canonical_hash(
    path: Path,
    *,
    marker_path: Path,
    hash_field: str,
    role: str,
) -> dict[str, Any]:
    if not marker_path.is_file() or marker_path.read_text(
        encoding="ascii"
    ).strip() != sha256_file(path):
        raise OwnerContinuousExecutionError(f"{role} atomic marker drifted")
    payload = _load_json(path, role=role)
    normalized = dict(payload)
    expected = str(normalized.pop(hash_field, ""))
    if canonical_sha256(normalized) != expected:
        raise OwnerContinuousExecutionError(f"{role} canonical hash drifted")
    payload[hash_field] = expected
    return payload


def _load_complete_execution(
    plan: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    execution_root = Path(str(plan["execution_output_root"])).expanduser().resolve()
    framework = plan.get("framework") or {}
    expected_epochs = int(framework.get("authoritative_epoch_count", 0))
    if expected_epochs <= 0:
        raise OwnerContinuousExecutionError("plan lacks authoritative epoch count")
    receipt_root = execution_root / "receipts"
    receipts: list[dict[str, Any]] = []
    terminal_checkpoints: dict[str, dict[str, Any]] = {}
    previous = {arm: "" for arm in ARMS}
    for ordinal in range(1, expected_epochs + 1):
        epoch_id = f"epoch-{ordinal:04d}"
        path = receipt_root / f"{epoch_id}.json"
        receipt = _verified_json_with_canonical_hash(
            path,
            marker_path=path.with_suffix(".success"),
            hash_field="receipt_sha256",
            role=f"paired receipt {epoch_id}",
        )
        if receipt.get("plan_identity_sha256") != plan.get(
            "adapter_plan_identity_sha256"
        ):
            raise OwnerContinuousExecutionError("paired receipt plan identity drifted")
        if (receipt.get("epoch") or {}).get("epoch_id") != epoch_id:
            raise OwnerContinuousExecutionError("paired receipt epoch order drifted")
        if set(receipt.get("arms") or {}) != set(ARMS) or set(
            receipt.get("checkpoints") or {}
        ) != set(ARMS):
            raise OwnerContinuousExecutionError("paired receipt lost an arm")
        if receipt.get("same_random_path") is not True:
            raise OwnerContinuousExecutionError("paired receipt used different random paths")
        for arm in ARMS:
            ref = receipt["checkpoints"][arm]
            checkpoint_path = Path(str(ref.get("path", ""))).expanduser().resolve()
            checkpoint = _verified_json_with_canonical_hash(
                checkpoint_path,
                marker_path=checkpoint_path.with_suffix(".success"),
                hash_field="checkpoint_sha256",
                role=f"{arm} checkpoint {epoch_id}",
            )
            if (
                ref.get("file_sha256") != sha256_file(checkpoint_path)
                or ref.get("checkpoint_sha256") != checkpoint["checkpoint_sha256"]
                or checkpoint.get("plan_identity_sha256")
                != plan["adapter_plan_identity_sha256"]
                or checkpoint.get("epoch_id") != epoch_id
                or checkpoint.get("arm_id") != arm
                or checkpoint.get("previous_checkpoint_sha256") != previous[arm]
            ):
                raise OwnerContinuousExecutionError(
                    f"{arm} checkpoint chain drifted at {epoch_id}"
                )
            previous[arm] = str(checkpoint["checkpoint_sha256"])
            terminal_checkpoints[arm] = checkpoint
        receipts.append(receipt)
    extras = sorted(receipt_root.glob("epoch-*.json"))
    if len(extras) != expected_epochs:
        raise OwnerContinuousExecutionError("execution has extra or missing epoch receipts")
    return receipts, terminal_checkpoints


def _lower_tail(values: Sequence[float]) -> tuple[float | None, float | None]:
    if not values:
        return None, None
    array = np.asarray(values, dtype=np.float64)
    q10 = float(np.quantile(array, 0.10, method="linear"))
    tail = array[array <= q10]
    return q10, float(np.mean(tail))


def _arm_economics(
    *,
    arm: str,
    checkpoint: Mapping[str, Any],
    receipts: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, float], dict[str, float]]:
    ledger = checkpoint.get("ledger_state") or {}
    if canonical_sha256(dict(ledger)) != checkpoint.get("ledger_state_sha256"):
        raise OwnerContinuousExecutionError(f"{arm} terminal ledger state drifted")
    state = ledger.get("state") or {}
    campaigns = list(ledger.get("closed_campaigns", ()))
    daily = list(ledger.get("daily_slices", ()))
    gaps = list(ledger.get("gap_carries", ()))
    campaign_values = [float(row["value_usdc"]) for row in campaigns]
    q10, cvar10 = _lower_tail(campaign_values)
    daily_map = {str(row["day"]): float(row["pnl_usdc"]) for row in daily}
    if len(daily_map) != len(daily):
        raise OwnerContinuousExecutionError(f"{arm} has duplicate UTC accounting slices")
    campaign_by_day: dict[str, float] = defaultdict(float)
    for row in campaigns:
        day = datetime.fromtimestamp(int(row["end_ts_ms"]) / 1_000.0, tz=UTC).date()
        campaign_by_day[day.isoformat()] += float(row["value_usdc"])
    inventory_candidates = [abs(float(state.get("position_btc", 0.0)))]
    for row in daily:
        inventory_candidates.extend(
            (
                abs(float(row["start_inventory_btc"])),
                abs(float(row["end_inventory_btc"])),
            )
        )
    inventory_candidates.extend(
        abs(float(row["peak_abs_inventory_btc"])) for row in campaigns
    )
    open_campaign = state.get("economic_campaign")
    if isinstance(open_campaign, Mapping):
        inventory_candidates.append(abs(float(open_campaign["peak_abs_inventory_btc"])))
    fills = sum(int(row["arms"][arm].get("fill_count", 0) or 0) for row in receipts)
    runtime_mode_counts: dict[str, int] = defaultdict(int)
    policy_audit_totals: dict[str, float] = defaultdict(float)
    for receipt in receipts:
        owner_runtime = receipt["arms"][arm].get("owner_runtime") or {}
        runtime_mode_counts[str(owner_runtime.get("mode", "unknown"))] += 1
        for name, value in (owner_runtime.get("policy_audit") or {}).items():
            if isinstance(value, (int, float)) and math.isfinite(float(value)):
                policy_audit_totals[str(name)] += float(value)
    terminal_pnl = float(state["cumulative_pnl_usdc"])
    equity_check = float(state["cash_usdc"]) + float(state["position_btc"]) * float(
        state["last_mark_price"]
    ) - float(state["equity_anchor_usdc"])
    if not math.isclose(terminal_pnl, equity_check, rel_tol=0.0, abs_tol=1e-8):
        raise OwnerContinuousExecutionError(f"{arm} terminal PnL accounting drifted")
    metrics = {
        "terminal_mtm_pnl_usdc": terminal_pnl,
        "terminal_equity_usdc": float(state["cash_usdc"])
        + float(state["position_btc"]) * float(state["last_mark_price"]),
        "final_inventory_btc": float(state["position_btc"]),
        "max_abs_inventory_btc": max(inventory_candidates, default=0.0),
        "closed_campaign_count": len(campaigns),
        "closed_campaign_value_usdc": float(sum(campaign_values)),
        "campaign_q10_usdc": q10,
        "campaign_cvar10_usdc": cvar10,
        "fill_count": fills,
        "utc_day_count": len(daily),
        "gap_count": len(gaps),
        "gap_inventory_pnl_usdc": float(sum(float(row["pnl_usdc"]) for row in gaps)),
        "gap_abs_inventory_time_btc_s": float(
            sum(
                abs(float(row["position_btc"]))
                * (int(row["end_ts_ms"]) - int(row["start_ts_ms"]))
                / 1_000.0
                for row in gaps
            )
        ),
        "full_abs_inventory_time_btc_s": None,
        "full_abs_inventory_time_reason": (
            "shared checkpoint ABI retains campaign peaks and UTC boundary inventory, "
            "not the full intraday inventory integral"
        ),
        "runtime_mode_epoch_counts": dict(sorted(runtime_mode_counts.items())),
        "runtime_policy_audit_totals": dict(sorted(policy_audit_totals.items())),
    }
    return metrics, daily_map, dict(campaign_by_day)


def _paired_bootstrap(
    values: Sequence[float],
    *,
    draws: int,
    seed: int,
) -> dict[str, Any]:
    array = np.asarray(values, dtype=np.float64)
    if array.size == 0:
        raise OwnerContinuousExecutionError("paired bootstrap has no UTC clusters")
    if draws <= 0:
        raise OwnerContinuousExecutionError("bootstrap draws must be positive")
    rng = np.random.default_rng(int(seed))
    means = np.empty(int(draws), dtype=np.float64)
    chunk = min(5_000, int(draws))
    cursor = 0
    while cursor < draws:
        width = min(chunk, draws - cursor)
        indices = rng.integers(0, len(array), size=(width, len(array)))
        means[cursor : cursor + width] = np.mean(array[indices], axis=1)
        cursor += width
    return {
        "day_count": int(len(array)),
        "mean_delta_usdc_per_day": float(np.mean(array)),
        "total_delta_usdc": float(np.sum(array)),
        "ci95_mean_delta_usdc_per_day": [
            float(np.quantile(means, 0.025, method="linear")),
            float(np.quantile(means, 0.975, method="linear")),
        ],
        "bootstrap_draws": int(draws),
        "bootstrap_seed": int(seed),
    }


def _aggregate_economics(
    *,
    receipts: Sequence[Mapping[str, Any]],
    checkpoints: Mapping[str, Mapping[str, Any]],
    bootstrap_draws: int,
    bootstrap_seed: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    arm_metrics: dict[str, Any] = {}
    daily_maps: dict[str, dict[str, float]] = {}
    campaign_maps: dict[str, dict[str, float]] = {}
    for arm in ARMS:
        metrics, daily, campaigns = _arm_economics(
            arm=arm,
            checkpoint=checkpoints[arm],
            receipts=receipts,
        )
        arm_metrics[arm] = metrics
        daily_maps[arm] = daily
        campaign_maps[arm] = campaigns
    if tuple(daily_maps[CONTROL_ARM]) != tuple(daily_maps[CANDIDATE_ARM]):
        raise OwnerContinuousExecutionError("paired UTC accounting denominators differ")
    days = tuple(daily_maps[CONTROL_ARM])
    paired_daily = [
        {
            "day": day,
            "control_pnl_usdc": daily_maps[CONTROL_ARM][day],
            "candidate_pnl_usdc": daily_maps[CANDIDATE_ARM][day],
            "delta_pnl_usdc": daily_maps[CANDIDATE_ARM][day]
            - daily_maps[CONTROL_ARM][day],
            "control_closed_campaign_value_usdc": campaign_maps[CONTROL_ARM].get(
                day, 0.0
            ),
            "candidate_closed_campaign_value_usdc": campaign_maps[
                CANDIDATE_ARM
            ].get(day, 0.0),
            "delta_closed_campaign_value_usdc": campaign_maps[CANDIDATE_ARM].get(
                day, 0.0
            )
            - campaign_maps[CONTROL_ARM].get(day, 0.0),
        }
        for day in days
    ]
    control = arm_metrics[CONTROL_ARM]
    candidate = arm_metrics[CANDIDATE_ARM]
    fill_retention = (
        float(candidate["fill_count"]) / float(control["fill_count"])
        if int(control["fill_count"]) > 0
        else None
    )
    paired = {
        "terminal_mtm_pnl_delta_usdc": candidate["terminal_mtm_pnl_usdc"]
        - control["terminal_mtm_pnl_usdc"],
        "closed_campaign_value_delta_usdc": candidate[
            "closed_campaign_value_usdc"
        ]
        - control["closed_campaign_value_usdc"],
        "campaign_q10_delta_usdc": (
            candidate["campaign_q10_usdc"] - control["campaign_q10_usdc"]
            if candidate["campaign_q10_usdc"] is not None
            and control["campaign_q10_usdc"] is not None
            else None
        ),
        "campaign_cvar10_delta_usdc": (
            candidate["campaign_cvar10_usdc"] - control["campaign_cvar10_usdc"]
            if candidate["campaign_cvar10_usdc"] is not None
            and control["campaign_cvar10_usdc"] is not None
            else None
        ),
        "final_inventory_delta_btc": candidate["final_inventory_btc"]
        - control["final_inventory_btc"],
        "max_abs_inventory_delta_btc": candidate["max_abs_inventory_btc"]
        - control["max_abs_inventory_btc"],
        "fill_retention": fill_retention,
        "fill_count_delta": int(candidate["fill_count"]) - int(control["fill_count"]),
        "daily_terminal_pnl": _paired_bootstrap(
            [row["delta_pnl_usdc"] for row in paired_daily],
            draws=bootstrap_draws,
            seed=bootstrap_seed,
        ),
        "daily_closed_campaign_value": _paired_bootstrap(
            [row["delta_closed_campaign_value_usdc"] for row in paired_daily],
            draws=bootstrap_draws,
            seed=bootstrap_seed + 1,
        ),
    }
    return {"arms": arm_metrics, "paired": paired}, paired_daily


def finalize(
    plan_path: Path,
    *,
    bootstrap_draws: int = DEFAULT_BOOTSTRAP_DRAWS,
    bootstrap_seed: int = DEFAULT_BOOTSTRAP_SEED,
) -> dict[str, Any]:
    plan = validate_execution_plan(plan_path, require_eligible=True)
    receipts, checkpoints = _load_complete_execution(plan)
    economics, paired_daily = _aggregate_economics(
        receipts=receipts,
        checkpoints=checkpoints,
        bootstrap_draws=bootstrap_draws,
        bootstrap_seed=bootstrap_seed,
    )
    final = Path(str(plan["final_output_root"])).expanduser().resolve()
    staging = final.parent / f".{final.name}.{uuid.uuid4().hex}.partial"
    if final.exists():
        marker = final / FINAL_SUCCESS
        manifest_path = final / "manifest.json"
        if marker.is_file() and manifest_path.is_file() and marker.read_text(
            encoding="ascii"
        ).strip() == sha256_file(manifest_path):
            manifest = _load_json(manifest_path, role="admitted final manifest")
            normalized = dict(manifest)
            expected = str(normalized.pop("manifest_identity_sha256", ""))
            if canonical_sha256(normalized) != expected:
                raise OwnerContinuousExecutionError(
                    "admitted final manifest identity drifted"
                )
            for row in manifest.get("files", ()):
                path = final / str(row.get("relative_path", ""))
                if (
                    not path.is_file()
                    or path.stat().st_size != int(row.get("size_bytes", -1))
                    or sha256_file(path) != row.get("sha256")
                ):
                    raise OwnerContinuousExecutionError(
                        "admitted final result file drifted"
                    )
            return _load_json(final / "report.json", role="admitted final report")
        raise OwnerContinuousExecutionError("partial final admission already exists")
    staging.mkdir(parents=True, exist_ok=False)
    try:
        report = {
            "schema_version": f"{SCHEMA_VERSION}.report",
            "identity": IDENTITY,
            "status": "owner_restart_aware_continuous_historical_economics_complete",
            "plan_identity_sha256": plan["plan_identity_sha256"],
            "adapter_plan_identity_sha256": receipts[0]["plan_identity_sha256"],
            "epoch_count": len(receipts),
            "owner_continuation": plan["owner_continuation"],
            "economics": economics,
            "evidence_scope": {
                "exchange_time": True,
                "modeled_queue": True,
                "restart_aware_continuous": True,
                "daily_fresh_start": False,
                "strict_queue": False,
                "receive_time_transport": False,
            },
            "limitations": [
                "No strict exchange-queue authority.",
                "No receive/feature-ready transport authority.",
                "Full intraday absolute inventory-time is not retained by the shared checkpoint ABI.",
                "Owner continuation is not research-supported promotion evidence.",
            ],
            "permissions": {
                "strict_queue_authority": False,
                "receive_time_transport_authority": False,
                "research_supported": False,
                "action_authorized": False,
                "live_authorized": False,
            },
            "economic_outcomes_read": True,
            "economic_results_aggregated": True,
        }
        report_path = staging / "report.json"
        daily_path = staging / "paired_daily.json"
        _atomic_json(report_path, report)
        _atomic_json(
            daily_path,
            {
                "schema_version": f"{SCHEMA_VERSION}.paired_daily",
                "identity": IDENTITY,
                "rows": paired_daily,
            },
        )
        receipt_bindings = [
            {
                "epoch_id": row["epoch"]["epoch_id"],
                "receipt_sha256": row["receipt_sha256"],
            }
            for row in receipts
        ]
        manifest = {
            "schema_version": f"{SCHEMA_VERSION}.manifest",
            "identity": IDENTITY,
            "plan": _artifact(Path(plan_path), role="execution plan"),
            "files": [
                {
                    "relative_path": "report.json",
                    "sha256": sha256_file(report_path),
                    "size_bytes": report_path.stat().st_size,
                },
                {
                    "relative_path": "paired_daily.json",
                    "sha256": sha256_file(daily_path),
                    "size_bytes": daily_path.stat().st_size,
                },
            ],
            "receipts": receipt_bindings,
            "terminal_checkpoints": {
                arm: {
                    "checkpoint_sha256": checkpoints[arm]["checkpoint_sha256"],
                    "state_sha256": checkpoints[arm]["state_sha256"],
                    "ledger_state_sha256": checkpoints[arm]["ledger_state_sha256"],
                }
                for arm in ARMS
            },
            "permissions": report["permissions"],
        }
        manifest["manifest_identity_sha256"] = canonical_sha256(manifest)
        manifest_path = staging / "manifest.json"
        _atomic_json(manifest_path, manifest)
        _atomic_text(staging / FINAL_SUCCESS, sha256_file(manifest_path) + "\n")
        final.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staging, final)
        _fsync_directory(final.parent)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return report | {
        "report_path": str(final / "report.json"),
        "manifest_path": str(final / "manifest.json"),
    }


def status(plan_path: Path) -> dict[str, Any]:
    plan = validate_execution_plan(plan_path, require_eligible=False)
    execution = Path(str(plan["execution_output_root"])).expanduser().resolve()
    receipts = []
    receipt_root = execution / "receipts"
    if receipt_root.is_dir():
        for path in sorted(receipt_root.glob("epoch-*.json")):
            marker = path.with_suffix(".success")
            if marker.is_file() and marker.read_text(
                encoding="ascii"
            ).strip() == sha256_file(path):
                receipts.append(path.stem)
    expected = int((plan.get("framework") or {}).get("authoritative_epoch_count", 0))
    final = Path(str(plan["final_output_root"])).expanduser().resolve()
    return {
        "identity": IDENTITY,
        "execution_eligible": plan["execution_eligible"],
        "blockers": list(plan.get("blockers", ())),
        "expected_epochs": expected,
        "admitted_epochs": len(receipts),
        "remaining_epochs": max(0, expected - len(receipts)),
        "last_admitted_epoch": receipts[-1] if receipts else None,
        "finalized": (final / FINAL_SUCCESS).is_file(),
        "permissions": plan["permissions"],
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    preflight_parser = sub.add_parser("preflight")
    prepare_parser = sub.add_parser("prepare")
    for command in (preflight_parser, prepare_parser):
        command.add_argument("--daily-panel", type=Path, default=owner_abi.DEFAULT_DAILY_PANEL)
        command.add_argument(
            "--framework-plan", type=Path, default=owner_abi.DEFAULT_FRAMEWORK_PLAN
        )
        command.add_argument(
            "--f03-control-plan", type=Path, default=DEFAULT_F03_CONTROL_PLAN
        )
        command.add_argument("--policy-path", type=Path, default=owner_abi.DEFAULT_POLICY)
        command.add_argument("--policy-sha256", default=owner_abi.DEFAULT_POLICY_SHA256)
        command.add_argument("--owner-continue-after-daily", action="store_true")
        command.add_argument("--owner-continuation-artifact", type=Path)
    prepare_parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    prepare_parser.add_argument(
        "--observation-cache-root",
        type=Path,
        default=owner_abi.DEFAULT_OBSERVATION_CACHE,
    )
    for name in ("validate", "run", "finalize", "status"):
        command = sub.add_parser(name)
        command.add_argument("--plan", type=Path, default=DEFAULT_OUTPUT_ROOT / PLAN_FILENAME)
    sub.choices["run"].add_argument("--max-epochs", type=int)
    sub.choices["finalize"].add_argument(
        "--bootstrap-draws", type=int, default=DEFAULT_BOOTSTRAP_DRAWS
    )
    sub.choices["finalize"].add_argument(
        "--bootstrap-seed", type=int, default=DEFAULT_BOOTSTRAP_SEED
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "preflight":
        result = preflight(
            daily_panel=args.daily_panel,
            framework_plan=args.framework_plan,
            f03_control_plan=args.f03_control_plan,
            policy_path=args.policy_path,
            policy_sha256=args.policy_sha256,
            owner_continue_after_daily=args.owner_continue_after_daily,
            continuation_artifact=args.owner_continuation_artifact,
        )
    elif args.command == "prepare":
        result = prepare_execution_plan(
            output_root=args.output_root,
            daily_panel=args.daily_panel,
            framework_plan=args.framework_plan,
            f03_control_plan=args.f03_control_plan,
            policy_path=args.policy_path,
            policy_sha256=args.policy_sha256,
            observation_cache_root=args.observation_cache_root,
            owner_continue_after_daily=args.owner_continue_after_daily,
            continuation_artifact=args.owner_continuation_artifact,
        )
    elif args.command == "validate":
        result = validate_execution_plan(args.plan, require_eligible=False)
    elif args.command == "run":
        result = run_prepared_plan(args.plan, max_epochs=args.max_epochs)
    elif args.command == "finalize":
        result = finalize(
            args.plan,
            bootstrap_draws=args.bootstrap_draws,
            bootstrap_seed=args.bootstrap_seed,
        )
    elif args.command == "status":
        result = status(args.plan)
    else:
        raise AssertionError(args.command)
    print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))
    return 0


__all__ = [
    "ARMS",
    "CANDIDATE_ARM",
    "CONTROL_ARM",
    "IDENTITY",
    "OwnerContinuousExecutionError",
    "SharedF03ControlInputProvider",
    "finalize",
    "prepare_execution_plan",
    "preflight",
    "run_prepared_plan",
    "status",
    "validate_execution_plan",
]


if __name__ == "__main__":
    raise SystemExit(main())
