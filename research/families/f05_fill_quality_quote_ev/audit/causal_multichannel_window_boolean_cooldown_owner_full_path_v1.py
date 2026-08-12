#!/usr/bin/env python3
"""Run the owner SELL-M2 Boolean cooldown policy on the frozen 50-day panel.

This is an independent F05 owner-route repeated-policy replay.  Both arms use
the Python authoritative engine and the exact same frozen F10 market inputs and
random parameters.  The candidate evaluates the frozen cooldown policy at
every exposure-increasing fill when an admitted raw-M2 observation cache is
available.  A missing daily M2 cache produces an exact candidate=control
fallback while preserving that day in the 50-day denominator.

The replay is historical, daily-fresh-start, exchange-time, modeled-queue
evidence.  It has no strict-queue, receive-time transport, action, or live
authority.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import importlib
import json
import math
import os
import shutil
import tempfile
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from data_paths import data_root
from models import backtest_tick as bt
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_native_40day_full_path_ml_ab as native_runner,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_native_observation_cache import (
    AdmittedNativeObservationCache,
    NativeObservationCacheError,
    open_admitted_observation_cache,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_replay_emitter import (
    CooldownV2ReplayEmitter,
)
from research.families.f10_live_replay_attribution.audit import (
    current_live_held_ber_replay_baseline_50d as baseline50,
)

ROOT = Path(__file__).resolve().parents[4]
IDENTITY = "causal_multichannel_window_boolean_cooldown_owner_full_path_v1"
SCHEMA_VERSION = f"{IDENTITY}.v1"
EVIDENCE_LABEL = (
    "historical_exchange_time_native_derived_top20_100ms_"
    "modeled_queue_python_daily_fresh_start_owner_diagnostic"
)
CONTROL_ARM = "current_live_held_global_ber_control"
CANDIDATE_ARM = "sell_m2_boolean_cooldown_owner_policy_v1"
ARMS = (CONTROL_ARM, CANDIDATE_ARM)
DAY_SUCCESS = "_SUCCESS"
PANEL_SUCCESS = "_PANEL_SUCCESS"
DATA_ROOT = data_root(ROOT)
EPHEMERAL_ROOT = Path(tempfile.gettempdir())
DEFAULT_POLICY = EPHEMERAL_ROOT / (
    "causal_multichannel_window_boolean_cooldown_owner_policy_v1/policy.json"
)
DEFAULT_POLICY_SHA256 = (
    "877a20033ff678bd7aa9b58069f37c3dc459b18db78c316b7e50023248f15a29"
)
DEFAULT_OBSERVATION_CACHE = DATA_ROOT / (
    "cache/replay_dag/"
    "causal_multichannel_window_boolean_cooldown_native_observation_v1"
)
DEFAULT_BASELINE_CACHE = baseline50.DEFAULT_CACHE
DEFAULT_OUTPUT = EPHEMERAL_ROOT / (
    "causal_multichannel_window_boolean_cooldown_owner_full_path_v1"
)
DEFAULT_BOOTSTRAP_DRAWS = 99_999
DEFAULT_BOOTSTRAP_SEED = 20260812
DEFAULT_PROGRESS_INTERVAL_EVENTS = 250_000
TRACE_CAPACITY = 1_000_000
REPLAY_ATOL = 1e-9

RUNTIME_POLICY_MODULES = (
    "research.families.f05_fill_quality_quote_ev.audit.runtime_policy",
    (
        "research.families.f05_fill_quality_quote_ev.audit."
        "causal_multichannel_window_boolean_cooldown_runtime_policy"
    ),
)

DECISION_COLUMNS = (
    "day",
    "exposure_fill_ordinal",
    "fill_visible_ts_ms",
    "side",
    "role_at_fill",
    "campaign_id",
    "order_id",
    "baseline_duration_ms",
    "action_id",
    "duration_ms",
    "fallback_reason",
    "matched_rule_index",
    "policy_sha256",
    "predicate_bundle_sha256",
    "snapshot_id",
    "support_valid",
)

PAIR_METRICS = (
    "terminal_mtm_pnl_usdc",
    "closed_campaign_value_usdc",
    "fills_total",
    "fills_bid",
    "fills_ask",
    "abs_inventory_time_btc_s",
    "max_inventory_btc",
    "campaign_mae_usdc",
    "campaign_q10_usdc",
    "campaign_cvar10_usdc",
)


class OwnerFullPathError(RuntimeError):
    """Raised when the owner repeated-policy replay loses its frozen identity."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        value = float(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _load_json(path: Path, *, role: str) -> dict[str, Any]:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise OwnerFullPathError(f"missing {role}: {resolved}")
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OwnerFullPathError(f"invalid {role}: {resolved}") from exc
    if not isinstance(payload, dict):
        raise OwnerFullPathError(f"{role} must be a JSON object")
    return payload


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.partial"
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(
                _json_safe(payload),
                handle,
                indent=2,
                sort_keys=True,
                allow_nan=False,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
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
    finally:
        temporary.unlink(missing_ok=True)


def _fsync_file(path: Path) -> None:
    with Path(path).open("rb") as handle:
        os.fsync(handle.fileno())


def _validate_policy(path: Path, expected_sha256: str) -> Path:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise OwnerFullPathError(f"owner policy is missing: {resolved}")
    observed = _sha256_file(resolved)
    if observed != str(expected_sha256):
        raise OwnerFullPathError(
            f"owner policy SHA256 drifted: expected={expected_sha256}, observed={observed}"
        )
    payload = _load_json(resolved, role="owner policy")
    if payload.get("identity") != (
        "causal_multichannel_window_boolean_cooldown_owner_policy_v1"
    ):
        raise OwnerFullPathError("owner policy identity drifted")
    if payload.get("selection") != {
        "BUY": "CONTROL_85N",
        "SELL": "M2_boolean_small_profile_full_common33_refit",
    }:
        raise OwnerFullPathError("owner policy side selection drifted")
    permissions = payload.get("permissions") or {}
    if any(
        bool(permissions.get(key))
        for key in ("research_supported", "action_authorized", "live_authorized")
    ):
        raise OwnerFullPathError("owner policy unexpectedly carries promotion authority")
    return resolved


def _runtime_policy_module() -> Any:
    failures: list[str] = []
    for name in RUNTIME_POLICY_MODULES:
        try:
            module = importlib.import_module(name)
        except ModuleNotFoundError as exc:
            if exc.name != name:
                raise
            failures.append(name)
            continue
        loader = getattr(module, "load_runtime_policy_evaluator", None)
        if callable(loader):
            return module
        failures.append(f"{name}:loader_missing")
    raise OwnerFullPathError(
        "runtime policy loader is unavailable; checked " + ", ".join(failures)
    )


def _load_runtime_policy_evaluator(
    policy_path: Path,
    *,
    expected_policy_sha256: str,
) -> Any:
    module = _runtime_policy_module()
    evaluator = module.load_runtime_policy_evaluator(
        policy_path,
        expected_policy_sha256=expected_policy_sha256,
    )
    binding_error = getattr(evaluator, "_binding_error", None)
    if binding_error is not None:
        raise OwnerFullPathError(
            f"runtime policy dependencies failed closed: {binding_error}"
        )
    if str(getattr(evaluator, "policy_sha256", "")) != expected_policy_sha256:
        raise OwnerFullPathError("runtime evaluator policy SHA256 drifted")
    return evaluator


def _frozen_context(
    cache_root: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    spec = baseline50._spec()
    prefix_plan = baseline50._prefix_plan(spec)
    prepared_plan = baseline50._load_prepared_plan(cache_root)
    days = baseline50.ordered_days(spec)
    if len(days) != 50 or days[:40] != list(
        spec["immutable_prefix"]["ordered_utc_days"]
    ) or days[40:] != list(spec["added_panel"]["ordered_utc_days"]):
        raise OwnerFullPathError("F10 50-day denominator drifted")
    if prepared_plan.get("ordered_utc_days") != days:
        raise OwnerFullPathError("F10 prepared plan denominator drifted")
    return spec, prefix_plan, prepared_plan


def _panel_membership(spec: Mapping[str, Any], day: str) -> str:
    if day in set(spec["immutable_prefix"]["ordered_utc_days"]):
        return "prefix_40"
    if day in set(spec["added_panel"]["ordered_utc_days"]):
        return "added_10"
    raise OwnerFullPathError(f"day is outside the frozen 50-day panel: {day}")


def _identity_hashes(
    spec: Mapping[str, Any],
    base: Mapping[str, Any],
    *,
    policy_path: Path,
    policy_sha256: str,
) -> dict[str, str]:
    implementation = {
        "runner_sha256": _sha256_file(Path(__file__).resolve()),
        "backtest_tick_sha256": _sha256_file(ROOT / "models/backtest_tick.py"),
        "emitter_sha256": _sha256_file(
            ROOT
            / "research/families/f05_fill_quality_quote_ev/audit/"
            "causal_multichannel_window_boolean_cooldown_replay_emitter.py"
        ),
        "policy_sha256": str(policy_sha256),
        "policy_path": str(policy_path),
    }
    p3_sha = str(base.get("fill_probability_artifact_sha256", ""))
    if len(p3_sha) != 64:
        raise OwnerFullPathError("baseline P3 artifact SHA256 is unavailable")
    hashes = {
        "config_sha256": str(spec["baseline"]["config_sha256"]),
        "code_sha256": _canonical_sha256(implementation),
        "model_sha256": str(spec["sources"]["model_bundle_meta_sha256"]),
        "p3_sha256": p3_sha,
        "feature_dag_sha256": _sha256_file(ROOT / "features/feature_dag.py"),
        "execution_abi_sha256": _sha256_file(ROOT / "models/backtest_tick.py"),
        "baseline_identity_sha256": str(
            spec["immutable_prefix"]["baseline_sha256"]
        ),
    }
    if any(len(value) != 64 for value in hashes.values()):
        raise OwnerFullPathError("snapshot execution identity hash is malformed")
    return hashes


def _open_candidate_cache(
    observation_cache_root: Path,
    day: str,
) -> tuple[AdmittedNativeObservationCache | None, dict[str, Any]]:
    root = Path(observation_cache_root).expanduser().resolve()
    day_root = root / day
    if not day_root.exists():
        return None, {
            "supported": False,
            "reason": "daily_raw_m2_observation_cache_missing",
            "cache_root": str(root),
            "day_root": str(day_root),
        }
    try:
        cache = open_admitted_observation_cache(root, day, deep=False)
    except NativeObservationCacheError as exc:
        raise OwnerFullPathError(
            f"{day} raw M2 cache exists but is not validly admitted"
        ) from exc
    manifest = dict(cache.manifest)
    if (
        manifest.get("formal_exchange_day") is not True
        or manifest.get("exact_queue_policy_eligible") is not False
        or manifest.get("action_authorized") is not False
        or manifest.get("live_policy_authorized") is not False
    ):
        raise OwnerFullPathError(f"{day} raw M2 cache permissions/semantics drifted")
    return cache, {
        "supported": True,
        "reason": "admitted_raw_m2_observation_cache",
        "cache_root": str(root),
        "day_root": str(cache.day_root),
        "manifest_path": str(cache.day_root / "manifest.json"),
        "manifest_sha256": _sha256_file(cache.day_root / "manifest.json"),
        "parquet_path": str(cache.day_root / "observations.parquet"),
        "parquet_sha256": str(manifest["parquet"]["sha256"]),
        "observation_count": int(manifest["observation_count"]),
        "source_binding_sha256": str(manifest["source_binding_sha256"]),
        "receive_time_transport_authority": False,
        "exact_queue_policy_eligible": False,
    }


def _candidate_runtime(
    *,
    day: str,
    observation_cache_root: Path,
    policy_path: Path,
    policy_sha256: str,
    spec: Mapping[str, Any],
    base: Mapping[str, Any],
) -> tuple[CooldownV2ReplayEmitter | None, Any | None, dict[str, Any]]:
    cache, binding = _open_candidate_cache(observation_cache_root, day)
    if cache is None:
        return None, None, binding
    manifest = dict(cache.manifest)
    warmup_cutoff_ts_ns = int(pd.Timestamp(day, tz="UTC").value)
    identity_hashes = _identity_hashes(
        spec,
        base,
        policy_path=policy_path,
        policy_sha256=policy_sha256,
    )
    emitter = CooldownV2ReplayEmitter(
        feature_block="M2",
        observations=cache.observations(),
        warmup_cutoff_ts_ns=warmup_cutoff_ts_ns,
        warmup_identity=str(manifest["source_binding_sha256"]),
        identity_hashes=identity_hashes,
        source_cursor_prefixes={
            "market": f"raw-m2-market:{day}",
            "depth": f"raw-m2-depth:{day}",
            "trade": f"raw-m2-trade:{day}",
        },
        retain_snapshots=False,
    )
    evaluator = _load_runtime_policy_evaluator(
        policy_path,
        expected_policy_sha256=policy_sha256,
    )
    return emitter, evaluator, binding | {
        "feature_block": "M2",
        "warmup_cutoff_ts_ns": warmup_cutoff_ts_ns,
        "warmup_identity": str(manifest["source_binding_sha256"]),
        "policy_path": str(policy_path),
        "policy_sha256": policy_sha256,
    }


def _progress_callback(
    progress_path: Path,
    *,
    day: str,
    arm: str,
) -> Callable[[Mapping[str, Any]], None]:
    def update(row: Mapping[str, Any]) -> None:
        event_index = int(row["event_index"])
        event_count = int(row["event_count"])
        _atomic_json(
            progress_path,
            {
                "identity": IDENTITY,
                "day": day,
                "arm": arm,
                "state": "running",
                "event_index": event_index,
                "event_count": event_count,
                "fraction_complete": (
                    float(event_index) / float(event_count) if event_count else 0.0
                ),
                "event_ts_ms": int(row["event_ts_ms"]),
                "updated_at_utc": datetime.now(tz=UTC).isoformat(),
            },
        )

    return update


def _decision_frame(rows: Sequence[Mapping[str, Any]], *, day: str) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=DECISION_COLUMNS)
    frame = pd.DataFrame([dict(row) for row in rows])
    frame.insert(0, "day", day)
    missing = sorted(set(DECISION_COLUMNS) - set(frame.columns))
    if missing:
        raise OwnerFullPathError(f"{day} candidate decision schema is incomplete: {missing}")
    return frame.loc[:, list(DECISION_COLUMNS)]


def _simulate_python_arm(
    *,
    day: str,
    arm: str,
    window: Any,
    ml_data: tuple[Any, ...],
    base: Mapping[str, Any],
    progress_path: Path,
    progress_interval_events: int,
    emitter: CooldownV2ReplayEmitter | None = None,
    evaluator: Any | None = None,
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if arm == CONTROL_ARM and (emitter is not None or evaluator is not None):
        raise OwnerFullPathError("control arm cannot receive the owner policy runtime")
    if (emitter is None) != (evaluator is None):
        raise OwnerFullPathError("candidate emitter/evaluator must be enabled together")
    if progress_interval_events <= 0:
        raise OwnerFullPathError("progress interval must be positive")
    params = dict(base)
    params["rng_seed"] = int(params.get("rng_seed", 42) or 42)
    params["trace_fills_max"] = max(
        TRACE_CAPACITY, int(params.get("trace_fills_max", 0) or 0)
    )
    params["trace_campaign_repair_max"] = max(
        TRACE_CAPACITY, int(params.get("trace_campaign_repair_max", 0) or 0)
    )
    params["trace_cooldown_duration_opportunities_max"] = TRACE_CAPACITY
    params["_replay_progress_callback"] = _progress_callback(
        progress_path,
        day=day,
        arm=arm,
    )
    params["_replay_progress_interval_events"] = int(progress_interval_events)
    if emitter is not None and evaluator is not None:
        params["cooldown_v2_snapshot_emitter"] = emitter
        params["cooldown_duration_policy_evaluator"] = evaluator
    else:
        params.pop("cooldown_v2_snapshot_emitter", None)
        params.pop("cooldown_duration_policy_evaluator", None)
    result = bt._simulate_tick_with_engine(
        "python",
        window.trades,
        window.var_ts_ms,
        window.var_ssq,
        params,
        ml_data=ml_data,
        bbo_data=window.bbo_data,
        l2_data=window.l2_data,
        var_ti=window.var_ti,
        var_retsq=window.var_retsq,
        campaign_repair_model=native_runner._CampaignMaeTraceProbe(),
    )
    trace = result.get("_campaign_repair_trace")
    if not isinstance(trace, list):
        raise OwnerFullPathError(f"{day} {arm} Python replay did not emit campaign MAE")
    result = dict(result)
    result["_campaign_mae_trace_audit"] = {
        "source": "python_authoritative_owner_repeated_policy_no_cpp_parity_claim",
        "trace_campaign_repair_max": int(params["trace_campaign_repair_max"]),
        "trace_row_count": len(trace),
        "cpp_python_fill_path_mismatch_count": 0,
    }
    summary, campaigns, fills = native_runner._project_arm(
        day=day,
        arm=arm,
        result=result,
        order_size=float(params["order_size"]),
        campaign_mae_trace_max=int(params["trace_campaign_repair_max"]),
    )
    decisions = _decision_frame(
        list(result.get("_cooldown_duration_policy_decisions") or ()),
        day=day,
    )
    snapshot_receipts = list(result.get("_cooldown_v2_snapshot_receipts") or ())
    emitter_audit = dict(result.get("_cooldown_v2_snapshot_emitter_audit") or {})
    policy_audit = dict(result.get("_cooldown_duration_policy_audit") or {})
    if evaluator is not None:
        if len(decisions) != len(snapshot_receipts):
            raise OwnerFullPathError(
                f"{day} candidate did not evaluate every exposure fill: "
                f"decisions={len(decisions)}, snapshots={len(snapshot_receipts)}"
            )
        if int(emitter_audit.get("snapshots_emitted", -1)) != len(decisions):
            raise OwnerFullPathError(f"{day} emitter audit does not bind all decisions")
        if not decisions.empty:
            observed_policy_hashes = {
                str(value) for value in decisions["policy_sha256"].dropna().unique()
            }
            if len(observed_policy_hashes) != 1:
                raise OwnerFullPathError(f"{day} decisions mix policy identities")
    elif len(decisions) != 0 or snapshot_receipts:
        raise OwnerFullPathError(f"{day} no-policy arm emitted cooldown decisions")
    summary.update(
        {
            "engine": "python",
            "python_authoritative": True,
            "cpp_parity_claimed": False,
            "campaign_mae_cpp_python_fill_path_mismatch_count": None,
            "repeated_policy_enabled": evaluator is not None,
            "repeated_policy_decision_count": int(len(decisions)),
            "cooldown_v2_snapshot_count": int(len(snapshot_receipts)),
            "cooldown_v2_fallback_snapshot_count": int(
                emitter_audit.get("fallback_snapshots", 0) or 0
            ),
            "cooldown_v2_emitter_audit": emitter_audit,
            "cooldown_duration_policy_audit": policy_audit,
            "exchange_book_queue_mode": str(
                result.get("exchange_book_queue_mode", "disabled") or "disabled"
            ),
            "exchange_book_queue_scope": str(
                result.get("exchange_book_queue_scope", "disabled") or "disabled"
            ),
            "strict_queue_authority": False,
            "receive_time_transport_authority": False,
        }
    )
    _atomic_json(
        progress_path,
        {
            "identity": IDENTITY,
            "day": day,
            "arm": arm,
            "state": "arm_complete",
            "event_index": None,
            "event_count": None,
            "fraction_complete": 1.0,
            "updated_at_utc": datetime.now(tz=UTC).isoformat(),
        },
    )
    return summary, campaigns, fills, decisions


def _without_arm(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.drop(columns=["arm"], errors="ignore").reset_index(drop=True)


def _assert_exact_fallback(
    control_summary: Mapping[str, Any],
    candidate_summary: Mapping[str, Any],
    control_campaigns: pd.DataFrame,
    candidate_campaigns: pd.DataFrame,
    control_fills: pd.DataFrame,
    candidate_fills: pd.DataFrame,
) -> None:
    for field in (
        "pnl_usdc",
        "terminal_mtm_pnl_usdc",
        "closed_campaign_value_usdc",
        "fills_bid",
        "fills_ask",
        "fills_total",
        "abs_inventory_time_btc_s",
        "max_inventory_btc",
        "final_inventory_btc",
    ):
        left = control_summary[field]
        right = candidate_summary[field]
        if isinstance(left, (int, np.integer)) and isinstance(right, (int, np.integer)):
            equal = int(left) == int(right)
        else:
            equal = math.isclose(
                float(left), float(right), rel_tol=0.0, abs_tol=REPLAY_ATOL
            )
        if not equal:
            raise OwnerFullPathError(f"unsupported candidate fallback drifted: {field}")
    try:
        pd.testing.assert_frame_equal(
            _without_arm(control_campaigns),
            _without_arm(candidate_campaigns),
            check_exact=True,
        )
        pd.testing.assert_frame_equal(
            _without_arm(control_fills),
            _without_arm(candidate_fills),
            check_exact=True,
        )
    except AssertionError as exc:
        raise OwnerFullPathError(
            "unsupported candidate did not reproduce the exact control path"
        ) from exc


def _day_directory(output: Path, day: str) -> Path:
    return Path(output) / "days" / day


def _load_admitted_day(
    output: Path,
    day: str,
    *,
    expected_policy_sha256: str | None = None,
) -> dict[str, Any] | None:
    directory = _day_directory(output, day)
    manifest_path = directory / "manifest.json"
    marker_path = directory / DAY_SUCCESS
    if not directory.exists():
        return None
    if not manifest_path.is_file() or not marker_path.is_file():
        raise OwnerFullPathError(f"{day} day admission is incomplete")
    if marker_path.read_text(encoding="ascii").strip() != _sha256_file(manifest_path):
        raise OwnerFullPathError(f"{day} day admission marker drifted")
    manifest = _load_json(manifest_path, role=f"{day} day manifest")
    if manifest.get("identity") != IDENTITY or manifest.get("day") != day:
        raise OwnerFullPathError(f"{day} day identity drifted")
    if expected_policy_sha256 is not None and (
        (manifest.get("policy") or {}).get("sha256") != expected_policy_sha256
    ):
        raise OwnerFullPathError(f"{day} admitted policy SHA256 drifted")
    for role in ("summary", "campaigns", "fills", "candidate_decisions"):
        row = manifest.get(role) or {}
        path = Path(str(row.get("path", "")))
        if not path.is_file() or _sha256_file(path) != row.get("sha256"):
            raise OwnerFullPathError(f"{day} admitted {role} drifted")
    return manifest


def _acquire_day_lock(output: Path, day: str) -> tuple[int, Path]:
    lock_path = Path(output) / ".locks" / f"{day}.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as exc:
        raise OwnerFullPathError(f"{day} is already running: {lock_path}") from exc
    os.write(descriptor, f"pid={os.getpid()}\n".encode("ascii"))
    os.fsync(descriptor)
    return descriptor, lock_path


def execute_day(
    day: str,
    *,
    cache_root: Path = DEFAULT_BASELINE_CACHE,
    observation_cache_root: Path = DEFAULT_OBSERVATION_CACHE,
    policy_path: Path = DEFAULT_POLICY,
    policy_sha256: str = DEFAULT_POLICY_SHA256,
    output: Path = DEFAULT_OUTPUT,
    progress_interval_events: int = DEFAULT_PROGRESS_INTERVAL_EVENTS,
) -> dict[str, Any]:
    resolved_policy = _validate_policy(policy_path, policy_sha256)
    existing = _load_admitted_day(
        output,
        day,
        expected_policy_sha256=policy_sha256,
    )
    if existing is not None:
        return {
            "day": day,
            "reused": True,
            "candidate_supported": bool(existing["candidate_support"]["supported"]),
        }
    spec, prefix_plan, prepared_plan = _frozen_context(cache_root)
    panel = _panel_membership(spec, day)
    descriptor, lock_path = _acquire_day_lock(output, day)
    progress_path = Path(output) / "progress" / f"{day}.json"
    try:
        if _load_admitted_day(
            output,
            day,
            expected_policy_sha256=policy_sha256,
        ) is not None:
            return {"day": day, "reused": True}
        _atomic_json(
            progress_path,
            {
                "identity": IDENTITY,
                "day": day,
                "state": "loading_inputs",
                "updated_at_utc": datetime.now(tz=UTC).isoformat(),
            },
        )
        window, ml_data, market_binding = baseline50._load_day_inputs(
            day,
            spec=spec,
            prefix_plan=prefix_plan,
            prepared_plan=prepared_plan,
            cache_root=cache_root,
        )
        base, projection_audit = baseline50._base_params(spec)
        base = dict(base)
        base["rng_seed"] = int(base.get("rng_seed", 42) or 42)
        random_parameters = {
            key: value
            for key, value in sorted(base.items())
            if key == "rng_seed" or key.endswith("_seed")
        }
        control = _simulate_python_arm(
            day=day,
            arm=CONTROL_ARM,
            window=window,
            ml_data=ml_data,
            base=base,
            progress_path=progress_path,
            progress_interval_events=progress_interval_events,
        )
        emitter, evaluator, candidate_support = _candidate_runtime(
            day=day,
            observation_cache_root=observation_cache_root,
            policy_path=resolved_policy,
            policy_sha256=policy_sha256,
            spec=spec,
            base=base,
        )
        candidate = _simulate_python_arm(
            day=day,
            arm=CANDIDATE_ARM,
            window=window,
            ml_data=ml_data,
            base=base,
            progress_path=progress_path,
            progress_interval_events=progress_interval_events,
            emitter=emitter,
            evaluator=evaluator,
        )
        control_summary, control_campaigns, control_fills, _ = control
        candidate_summary, candidate_campaigns, candidate_fills, decisions = candidate
        supported = bool(candidate_support["supported"])
        if supported:
            if not decisions.empty and not decisions["policy_sha256"].eq(
                policy_sha256
            ).all():
                raise OwnerFullPathError(f"{day} decision policy SHA256 drifted")
        else:
            if not decisions.empty:
                raise OwnerFullPathError(f"{day} unsupported fallback emitted decisions")
            _assert_exact_fallback(
                control_summary,
                candidate_summary,
                control_campaigns,
                candidate_campaigns,
                control_fills,
                candidate_fills,
            )
        control_summary.update(
            {
                "candidate_supported_day": False,
                "candidate_fallback_reason": "control_arm_not_applicable",
            }
        )
        candidate_summary.update(
            {
                "candidate_supported_day": supported,
                "candidate_fallback_reason": (
                    "" if supported else str(candidate_support["reason"])
                ),
                "candidate_policy_sha256": policy_sha256,
            }
        )
        summaries = [control_summary, candidate_summary]
        campaign_frames = [control_campaigns, candidate_campaigns]
        fill_frames = [control_fills, candidate_fills]
        final = _day_directory(output, day)
        staging = final.parent / f".{day}.{uuid.uuid4().hex}.partial"
        staging.mkdir(parents=True, exist_ok=False)
        try:
            summary_path = staging / "summary.json"
            campaigns_path = staging / "campaigns.parquet"
            fills_path = staging / "fills.parquet"
            decisions_path = staging / "candidate_decisions.parquet"
            _atomic_json(
                summary_path,
                {
                    "schema_version": f"{SCHEMA_VERSION}.day",
                    "identity": IDENTITY,
                    "day": day,
                    "panel": panel,
                    "arms": summaries,
                    "candidate_support": candidate_support,
                    "common_market_binding": market_binding,
                    "common_random_parameters": random_parameters,
                    "offline_execution_projection": projection_audit,
                    "policy_path": str(resolved_policy),
                    "policy_sha256": policy_sha256,
                    "candidate_evaluates_every_exposure_fill": supported,
                    "candidate_decision_count": int(len(decisions)),
                    "unsupported_day_preserved_in_denominator": not supported,
                    "economic_outcomes_read": True,
                    "strict_queue_authority": False,
                    "receive_time_transport_authority": False,
                    "research_supported": False,
                    "action_authorized": False,
                    "live_authorized": False,
                },
            )
            pd.concat(campaign_frames, ignore_index=True).to_parquet(
                campaigns_path, index=False, compression="zstd"
            )
            nonempty_fills = [frame for frame in fill_frames if not frame.empty]
            (
                pd.concat(nonempty_fills, ignore_index=True)
                if nonempty_fills
                else pd.DataFrame()
            ).to_parquet(fills_path, index=False, compression="zstd")
            decisions.to_parquet(decisions_path, index=False, compression="zstd")
            for path in (campaigns_path, fills_path, decisions_path):
                _fsync_file(path)
            manifest = {
                "schema_version": f"{SCHEMA_VERSION}.day_manifest",
                "identity": IDENTITY,
                "day": day,
                "panel": panel,
                "candidate_support": candidate_support,
                "f10_spec": {
                    "path": str(baseline50.SPEC.resolve()),
                    "sha256": _sha256_file(baseline50.SPEC),
                },
                "f10_execution_plan": {
                    "path": str(Path(cache_root) / "execution-plan.json"),
                    "sha256": _sha256_file(Path(cache_root) / "execution-plan.json"),
                },
                "policy": {
                    "path": str(resolved_policy),
                    "sha256": policy_sha256,
                },
                "implementation": {
                    "runner_path": str(Path(__file__).resolve()),
                    "runner_sha256": _sha256_file(Path(__file__).resolve()),
                    "backtest_tick_path": str((ROOT / "models/backtest_tick.py").resolve()),
                    "backtest_tick_sha256": _sha256_file(ROOT / "models/backtest_tick.py"),
                    "replay_emitter_path": str(
                        Path(
                            importlib.import_module(
                                "research.families.f05_fill_quality_quote_ev.audit."
                                "causal_multichannel_window_boolean_cooldown_replay_emitter"
                            ).__file__
                        ).resolve()
                    ),
                    "runtime_policy_path": str(
                        Path(_runtime_policy_module().__file__).resolve()
                    ),
                    "runtime_policy_sha256": _sha256_file(
                        Path(_runtime_policy_module().__file__).resolve()
                    ),
                },
                "summary": {
                    "path": str(final / summary_path.name),
                    "sha256": _sha256_file(summary_path),
                },
                "campaigns": {
                    "path": str(final / campaigns_path.name),
                    "sha256": _sha256_file(campaigns_path),
                },
                "fills": {
                    "path": str(final / fills_path.name),
                    "sha256": _sha256_file(fills_path),
                },
                "candidate_decisions": {
                    "path": str(final / decisions_path.name),
                    "sha256": _sha256_file(decisions_path),
                },
                "permissions": {
                    "strict_queue_authority": False,
                    "receive_time_transport_authority": False,
                    "research_supported": False,
                    "action_authorized": False,
                    "live_authorized": False,
                },
            }
            _atomic_json(staging / "manifest.json", manifest)
            _atomic_text(
                staging / DAY_SUCCESS,
                _sha256_file(staging / "manifest.json") + "\n",
            )
            final.parent.mkdir(parents=True, exist_ok=True)
            if final.exists():
                raise OwnerFullPathError(f"concurrent day admission appeared: {day}")
            os.replace(staging, final)
        finally:
            if staging.exists():
                shutil.rmtree(staging)
        _atomic_json(
            progress_path,
            {
                "identity": IDENTITY,
                "day": day,
                "state": "admitted",
                "candidate_supported": supported,
                "fraction_complete": 1.0,
                "updated_at_utc": datetime.now(tz=UTC).isoformat(),
            },
        )
        return {
            "day": day,
            "reused": False,
            "candidate_supported": supported,
            "candidate_decisions": int(len(decisions)),
        }
    except BaseException as exc:
        _atomic_json(
            progress_path,
            {
                "identity": IDENTITY,
                "day": day,
                "state": "failed",
                "error": f"{type(exc).__name__}: {exc}",
                "updated_at_utc": datetime.now(tz=UTC).isoformat(),
            },
        )
        raise
    finally:
        try:
            os.close(descriptor)
        finally:
            lock_path.unlink(missing_ok=True)


def _selected_days(spec: Mapping[str, Any], days: Sequence[str] | None) -> list[str]:
    frozen = baseline50.ordered_days(spec)
    if days is None or len(days) == 0:
        return frozen
    requested = list(days)
    if len(requested) != len(set(requested)):
        raise OwnerFullPathError("requested days contain duplicates")
    unknown = sorted(set(requested) - set(frozen))
    if unknown:
        raise OwnerFullPathError(f"requested days are outside the frozen panel: {unknown}")
    return [day for day in frozen if day in set(requested)]


def run(
    *,
    days: Sequence[str] | None = None,
    workers: int = 1,
    cache_root: Path = DEFAULT_BASELINE_CACHE,
    observation_cache_root: Path = DEFAULT_OBSERVATION_CACHE,
    policy_path: Path = DEFAULT_POLICY,
    policy_sha256: str = DEFAULT_POLICY_SHA256,
    output: Path = DEFAULT_OUTPUT,
    progress_interval_events: int = DEFAULT_PROGRESS_INTERVAL_EVENTS,
) -> list[dict[str, Any]]:
    if workers <= 0:
        raise OwnerFullPathError("workers must be positive")
    resolved_policy = _validate_policy(policy_path, policy_sha256)
    spec, _, _ = _frozen_context(cache_root)
    selected = _selected_days(spec, days)
    kwargs = {
        "cache_root": Path(cache_root),
        "observation_cache_root": Path(observation_cache_root),
        "policy_path": resolved_policy,
        "policy_sha256": str(policy_sha256),
        "output": Path(output),
        "progress_interval_events": int(progress_interval_events),
    }
    if workers == 1:
        results: list[dict[str, Any]] = []
        for day in selected:
            result = execute_day(day, **kwargs)
            results.append(result)
            print(
                f"owner-full-path complete {day} supported="
                f"{result.get('candidate_supported')}",
                flush=True,
            )
        return results
    results = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(execute_day, day, **kwargs): day for day in selected
        }
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            results.append(result)
            print(
                f"owner-full-path complete {result['day']} supported="
                f"{result.get('candidate_supported')}",
                flush=True,
            )
    return sorted(results, key=lambda row: str(row["day"]))


def _campaign_tail_by_day(campaigns: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if campaigns.empty:
        return pd.DataFrame(
            columns=["day", "arm", "campaign_q10_usdc", "campaign_cvar10_usdc"]
        )
    for (day, arm), frame in campaigns.groupby(["day", "arm"], sort=False):
        values = frame["terminal_value_usdc"].to_numpy(dtype=float)
        q10 = float(np.quantile(values, 0.1)) if len(values) else 0.0
        cvar = float(values[values <= q10].mean()) if len(values) else 0.0
        rows.append(
            {
                "day": str(day),
                "arm": str(arm),
                "campaign_q10_usdc": q10,
                "campaign_cvar10_usdc": cvar,
            }
        )
    return pd.DataFrame(rows)


def _paired_daily(daily: pd.DataFrame, campaigns: pd.DataFrame) -> pd.DataFrame:
    tail = _campaign_tail_by_day(campaigns)
    tail_metrics = ("campaign_q10_usdc", "campaign_cvar10_usdc")
    summary_tail_metrics = {
        metric: f"{metric}_summary" for metric in tail_metrics if metric in daily.columns
    }
    enriched = daily.rename(columns=summary_tail_metrics).merge(
        tail,
        on=["day", "arm"],
        how="left",
        validate="one_to_one",
    )
    for metric, summary_metric in summary_tail_metrics.items():
        summary_values = pd.to_numeric(enriched[summary_metric], errors="raise")
        derived_values = pd.to_numeric(enriched[metric], errors="raise")
        mismatch = ~np.isclose(
            summary_values.to_numpy(dtype=float),
            derived_values.to_numpy(dtype=float),
            rtol=0.0,
            atol=1e-9,
            equal_nan=False,
        )
        if bool(mismatch.any()):
            raise OwnerFullPathError(
                f"daily summary {metric} does not match campaign-derived tail"
            )
        enriched = enriched.drop(columns=[summary_metric])
    rows: list[dict[str, Any]] = []
    for day, frame in enriched.groupby("day", sort=True):
        by_arm = {str(row["arm"]): row for _, row in frame.iterrows()}
        if set(by_arm) != set(ARMS):
            raise OwnerFullPathError(f"{day} does not contain exactly both arms")
        control = by_arm[CONTROL_ARM]
        candidate = by_arm[CANDIDATE_ARM]
        row: dict[str, Any] = {
            "day": str(day),
            "candidate_supported_day": bool(candidate["candidate_supported_day"]),
            "candidate_fallback_reason": str(candidate["candidate_fallback_reason"]),
        }
        for metric in PAIR_METRICS:
            control_value = float(control[metric])
            candidate_value = float(candidate[metric])
            row[f"control_{metric}"] = control_value
            row[f"candidate_{metric}"] = candidate_value
            row[f"delta_{metric}"] = candidate_value - control_value
        rows.append(row)
    return pd.DataFrame(rows).sort_values("day").reset_index(drop=True)


def _bootstrap_day_mean(
    values: np.ndarray,
    *,
    draws: int,
    seed: int,
) -> dict[str, Any]:
    array = np.asarray(values, dtype=float)
    if draws <= 0 or len(array) == 0 or not np.isfinite(array).all():
        raise OwnerFullPathError("day bootstrap input is invalid")
    if np.all(array == array[0]):
        lower = upper = float(array[0])
    else:
        rng = np.random.default_rng(seed)
        means = np.empty(draws, dtype=float)
        chunk = max(1, min(draws, 20_000))
        for start in range(0, draws, chunk):
            stop = min(draws, start + chunk)
            indices = rng.integers(0, len(array), size=(stop - start, len(array)))
            means[start:stop] = array[indices].mean(axis=1)
        lower, upper = np.quantile(means, [0.025, 0.975])
    mean = float(array.mean())
    return {
        "days": int(len(array)),
        "draws": int(draws),
        "seed": int(seed),
        "mean_per_day": mean,
        "ci95_per_day": [float(lower), float(upper)],
        "total": float(array.sum()),
        "ci95_total_scale": [float(lower) * len(array), float(upper) * len(array)],
    }


def _arm_section(
    days: Sequence[str],
    daily: pd.DataFrame,
    campaigns: pd.DataFrame,
    arm: str,
) -> dict[str, Any]:
    selected_daily = daily[daily["day"].isin(days) & daily["arm"].eq(arm)]
    selected_campaigns = campaigns[
        campaigns["day"].isin(days) & campaigns["arm"].eq(arm)
    ]
    if len(selected_daily) != len(days):
        raise OwnerFullPathError(f"{arm} section does not cover all requested days")
    values = selected_campaigns["terminal_value_usdc"].to_numpy(dtype=float)
    q10 = float(np.quantile(values, 0.1)) if len(values) else 0.0
    cvar = float(values[values <= q10].mean()) if len(values) else 0.0
    return {
        "days": len(days),
        "terminal_mtm_pnl_usdc": float(selected_daily["terminal_mtm_pnl_usdc"].sum()),
        "terminal_mtm_pnl_usdc_per_day": float(
            selected_daily["terminal_mtm_pnl_usdc"].mean()
        ),
        "closed_campaign_value_usdc": float(
            selected_daily["closed_campaign_value_usdc"].sum()
        ),
        "fills_total": int(selected_daily["fills_total"].sum()),
        "fills_bid": int(selected_daily["fills_bid"].sum()),
        "fills_ask": int(selected_daily["fills_ask"].sum()),
        "abs_inventory_time_btc_s": float(
            selected_daily["abs_inventory_time_btc_s"].sum()
        ),
        "max_inventory_btc": float(selected_daily["max_inventory_btc"].max()),
        "campaign_mae_usdc": float(selected_daily["campaign_mae_usdc"].min()),
        "campaign_q10_usdc": q10,
        "campaign_cvar10_usdc": cvar,
        "campaigns": int(len(selected_campaigns)),
    }


def _section_report(
    name: str,
    days: Sequence[str],
    daily: pd.DataFrame,
    campaigns: pd.DataFrame,
    paired: pd.DataFrame,
    *,
    bootstrap_draws: int,
    bootstrap_seed: int,
) -> dict[str, Any]:
    selected = paired[paired["day"].isin(days)].copy()
    if list(selected["day"]) != list(days):
        raise OwnerFullPathError(f"{name} paired denominator drifted")
    control = _arm_section(days, daily, campaigns, CONTROL_ARM)
    candidate = _arm_section(days, daily, campaigns, CANDIDATE_ARM)
    deltas = {
        metric: float(candidate[metric]) - float(control[metric])
        for metric in (
            "terminal_mtm_pnl_usdc",
            "closed_campaign_value_usdc",
            "fills_total",
            "fills_bid",
            "fills_ask",
            "abs_inventory_time_btc_s",
            "max_inventory_btc",
            "campaign_mae_usdc",
            "campaign_q10_usdc",
            "campaign_cvar10_usdc",
        )
    }
    bootstrap: dict[str, Any] = {}
    for offset, metric in enumerate(PAIR_METRICS):
        bootstrap[metric] = _bootstrap_day_mean(
            selected[f"delta_{metric}"].to_numpy(dtype=float),
            draws=bootstrap_draws,
            seed=bootstrap_seed + offset,
        )
    return {
        "name": name,
        "days": len(days),
        "ordered_utc_days": list(days),
        "candidate_supported_days": int(selected["candidate_supported_day"].sum()),
        "candidate_unsupported_days": int((~selected["candidate_supported_day"]).sum()),
        "positive_terminal_delta_days": int(
            (selected["delta_terminal_mtm_pnl_usdc"] > 0.0).sum()
        ),
        "control": control,
        "candidate": candidate,
        "delta_candidate_minus_control": deltas,
        "fill_retention": (
            float(candidate["fills_total"]) / float(control["fills_total"])
            if int(control["fills_total"]) > 0
            else None
        ),
        "day_bootstrap": bootstrap,
    }


def finalize(
    *,
    cache_root: Path = DEFAULT_BASELINE_CACHE,
    policy_path: Path = DEFAULT_POLICY,
    policy_sha256: str = DEFAULT_POLICY_SHA256,
    output: Path = DEFAULT_OUTPUT,
    bootstrap_draws: int = DEFAULT_BOOTSTRAP_DRAWS,
    bootstrap_seed: int = DEFAULT_BOOTSTRAP_SEED,
) -> dict[str, Any]:
    resolved_policy = _validate_policy(policy_path, policy_sha256)
    spec, _, _ = _frozen_context(cache_root)
    days = baseline50.ordered_days(spec)
    prefix_days = list(spec["immutable_prefix"]["ordered_utc_days"])
    added_days = list(spec["added_panel"]["ordered_utc_days"])
    manifests = [_load_admitted_day(output, day) for day in days]
    if any(manifest is None for manifest in manifests):
        missing = [
            day
            for day, manifest in zip(days, manifests, strict=True)
            if manifest is None
        ]
        raise OwnerFullPathError(f"cannot finalize; 50-day results are missing: {missing}")
    summaries: list[dict[str, Any]] = []
    campaign_frames: list[pd.DataFrame] = []
    fill_frames: list[pd.DataFrame] = []
    decision_frames: list[pd.DataFrame] = []
    day_manifest_bindings: list[dict[str, str]] = []
    day_implementations: list[dict[str, Any]] = []
    for day, manifest in zip(days, manifests, strict=True):
        assert manifest is not None
        implementation = manifest.get("implementation")
        if not isinstance(implementation, Mapping):
            raise OwnerFullPathError(f"{day} manifest lacks implementation binding")
        day_implementations.append(dict(implementation))
        summary_payload = _load_json(
            Path(manifest["summary"]["path"]), role=f"{day} summary"
        )
        arms = list(summary_payload.get("arms") or ())
        if {str(row.get("arm")) for row in arms} != set(ARMS):
            raise OwnerFullPathError(f"{day} summary does not bind both arms")
        summaries.extend(dict(row) for row in arms)
        campaign_frames.append(pd.read_parquet(manifest["campaigns"]["path"]))
        fill_frames.append(pd.read_parquet(manifest["fills"]["path"]))
        decision_frames.append(
            pd.read_parquet(manifest["candidate_decisions"]["path"])
        )
        day_manifest_path = _day_directory(output, day) / "manifest.json"
        day_manifest_bindings.append(
            {"day": day, "path": str(day_manifest_path), "sha256": _sha256_file(day_manifest_path)}
        )
    implementation_identities = {
        json.dumps(payload, sort_keys=True, separators=(",", ":"))
        for payload in day_implementations
    }
    if len(implementation_identities) != 1:
        raise OwnerFullPathError("daily replay implementation bindings are not identical")
    replay_implementation = day_implementations[0]
    finalizer_path = Path(__file__).resolve()
    finalizer_sha256 = _sha256_file(finalizer_path)
    daily = pd.DataFrame(summaries)
    campaigns = pd.concat(campaign_frames, ignore_index=True)
    nonempty_fills = [frame for frame in fill_frames if not frame.empty]
    fills = pd.concat(nonempty_fills, ignore_index=True) if nonempty_fills else pd.DataFrame()
    nonempty_decisions = [frame for frame in decision_frames if not frame.empty]
    decisions = (
        pd.concat(nonempty_decisions, ignore_index=True)
        if nonempty_decisions
        else pd.DataFrame(columns=DECISION_COLUMNS)
    )
    paired = _paired_daily(daily, campaigns)
    sections = {
        "prefix_40": _section_report(
            "prefix_40",
            prefix_days,
            daily,
            campaigns,
            paired,
            bootstrap_draws=bootstrap_draws,
            bootstrap_seed=bootstrap_seed,
        ),
        "added_10": _section_report(
            "added_10",
            added_days,
            daily,
            campaigns,
            paired,
            bootstrap_draws=bootstrap_draws,
            bootstrap_seed=bootstrap_seed + 100,
        ),
        "pooled_50": _section_report(
            "pooled_50",
            days,
            daily,
            campaigns,
            paired,
            bootstrap_draws=bootstrap_draws,
            bootstrap_seed=bootstrap_seed + 200,
        ),
    }
    supported_days = paired.loc[
        paired["candidate_supported_day"], "day"
    ].astype(str).tolist()
    unsupported_days = paired.loc[
        ~paired["candidate_supported_day"], "day"
    ].astype(str).tolist()
    report = {
        "schema_version": f"{SCHEMA_VERSION}.report",
        "identity": IDENTITY,
        "created_at_utc": datetime.now(tz=UTC).isoformat(),
        "status": "owner_repeated_policy_historical_full_path_economics_complete",
        "evidence_route": "owner_risk_accepted_outcome_informed_successor",
        "evidence_label": EVIDENCE_LABEL,
        "panel": {
            "days": 50,
            "prefix_days": 40,
            "added_days": 10,
            "historical_development_previously_consumed": True,
            "independent_confirmation": False,
            "daily_fresh_start": True,
            "continuous_replay": False,
        },
        "candidate_support": {
            "supported_days": supported_days,
            "supported_day_count": len(supported_days),
            "unsupported_days": unsupported_days,
            "unsupported_day_count": len(unsupported_days),
            "unsupported_day_policy": "exact_candidate_equals_control_fallback",
            "denominator_days_dropped": 0,
        },
        "economics": sections,
        "mechanics": {
            "candidate_decision_count": int(len(decisions)),
            "repeated_evaluation": "every_exposure_increasing_fill_on_supported_days",
            "buy_policy": "CONTROL_85N",
            "sell_policy": "M2_boolean_small_profile_full_common33_refit",
            "control_and_candidate_engine": "python",
            "common_market_inputs": True,
            "common_random_parameters": True,
        },
        "execution_scope": {
            "exchange_book_queue_mode": "disabled_modeled_queue_from_frozen_F10_control",
            "raw_m2_cache_role": "candidate_feature_state_only",
            "raw_snapshot_delta_exact_queue_used": False,
            "receive_time_depth_tape_used": False,
            "python_cpp_policy_parity": False,
            "strict_queue_claimed": False,
            "live_transport_claimed": False,
        },
        "implementation_binding": {
            "replay": replay_implementation,
            "finalizer_path": str(finalizer_path),
            "finalizer_sha256": finalizer_sha256,
            "replay_and_finalizer_runner_hash_differ": bool(
                replay_implementation.get("runner_sha256") != finalizer_sha256
            ),
        },
        "permissions": {
            "research_supported": False,
            "strict_native_queue_authority": False,
            "receive_time_transport_authority": False,
            "continuous_replay_authority": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    panel_final = Path(output) / "panel"
    if panel_final.exists():
        raise OwnerFullPathError(f"final panel already exists: {panel_final}")
    staging = Path(output) / f".panel.{uuid.uuid4().hex}.partial"
    staging.mkdir(parents=True, exist_ok=False)
    try:
        report_path = staging / "report.json"
        daily_path = staging / "daily_arms.parquet"
        paired_path = staging / "daily_paired.parquet"
        campaigns_path = staging / "campaigns.parquet"
        fills_path = staging / "fills.parquet"
        decisions_path = staging / "candidate_decisions.parquet"
        _atomic_json(report_path, report)
        daily.to_parquet(daily_path, index=False, compression="zstd")
        paired.to_parquet(paired_path, index=False, compression="zstd")
        campaigns.to_parquet(campaigns_path, index=False, compression="zstd")
        fills.to_parquet(fills_path, index=False, compression="zstd")
        decisions.to_parquet(decisions_path, index=False, compression="zstd")
        for path in (daily_path, paired_path, campaigns_path, fills_path, decisions_path):
            _fsync_file(path)
        files = []
        for path in (
            report_path,
            daily_path,
            paired_path,
            campaigns_path,
            fills_path,
            decisions_path,
        ):
            files.append(
                {
                    "relative_path": path.name,
                    "sha256": _sha256_file(path),
                    "bytes": path.stat().st_size,
                }
            )
        manifest = {
            "schema_version": f"{SCHEMA_VERSION}.panel_manifest",
            "identity": IDENTITY,
            "f10_spec": {
                "path": str(baseline50.SPEC.resolve()),
                "sha256": _sha256_file(baseline50.SPEC),
            },
            "f10_execution_plan": {
                "path": str(Path(cache_root) / "execution-plan.json"),
                "sha256": _sha256_file(Path(cache_root) / "execution-plan.json"),
            },
            "policy": {"path": str(resolved_policy), "sha256": policy_sha256},
            "day_manifests": day_manifest_bindings,
            "files": files,
            "implementation": {
                "replay": replay_implementation,
                "finalizer_path": str(finalizer_path),
                "finalizer_sha256": finalizer_sha256,
            },
            "permissions": report["permissions"],
        }
        _atomic_json(staging / "manifest.json", manifest)
        _atomic_text(
            staging / PANEL_SUCCESS,
            _sha256_file(staging / "manifest.json") + "\n",
        )
        os.replace(staging, panel_final)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return report | {
        "report_path": str(panel_final / "report.json"),
        "manifest_path": str(panel_final / "manifest.json"),
    }


def status(
    *,
    output: Path = DEFAULT_OUTPUT,
    observation_cache_root: Path = DEFAULT_OBSERVATION_CACHE,
) -> dict[str, Any]:
    spec = baseline50._spec()
    days = baseline50.ordered_days(spec)
    admitted: list[str] = []
    supported: list[str] = []
    unsupported: list[str] = []
    for day in days:
        manifest = _load_admitted_day(output, day)
        if manifest is None:
            continue
        admitted.append(day)
        if bool(manifest["candidate_support"]["supported"]):
            supported.append(day)
        else:
            unsupported.append(day)
    progress_rows: list[dict[str, Any]] = []
    progress_root = Path(output) / "progress"
    if progress_root.is_dir():
        for path in sorted(progress_root.glob("*.json")):
            try:
                progress_rows.append(_load_json(path, role=f"progress {path.stem}"))
            except OwnerFullPathError:
                progress_rows.append(
                    {"day": path.stem, "state": "invalid_progress_artifact"}
                )
    cache_present = [
        day for day in days if (Path(observation_cache_root) / day / DAY_SUCCESS).is_file()
    ]
    prefix = set(spec["immutable_prefix"]["ordered_utc_days"])
    added = set(spec["added_panel"]["ordered_utc_days"])
    return {
        "identity": IDENTITY,
        "total_days": 50,
        "admitted_days": len(admitted),
        "remaining_days": 50 - len(admitted),
        "prefix_40_admitted": sum(day in prefix for day in admitted),
        "added_10_admitted": sum(day in added for day in admitted),
        "candidate_supported_days_admitted": supported,
        "candidate_unsupported_days_admitted": unsupported,
        "raw_m2_cache_days_present": cache_present,
        "raw_m2_cache_day_count": len(cache_present),
        "active_or_latest_progress": progress_rows,
        "panel_finalized": (Path(output) / "panel" / PANEL_SUCCESS).is_file(),
        "permissions": {
            "strict_queue_authority": False,
            "receive_time_transport_authority": False,
            "research_supported": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }


def preflight(
    *,
    cache_root: Path = DEFAULT_BASELINE_CACHE,
    observation_cache_root: Path = DEFAULT_OBSERVATION_CACHE,
    policy_path: Path = DEFAULT_POLICY,
    policy_sha256: str = DEFAULT_POLICY_SHA256,
) -> dict[str, Any]:
    resolved_policy = _validate_policy(policy_path, policy_sha256)
    spec, _, _ = _frozen_context(cache_root)
    days = baseline50.ordered_days(spec)
    support: list[str] = []
    unsupported: list[str] = []
    for day in days:
        cache, _ = _open_candidate_cache(observation_cache_root, day)
        (support if cache is not None else unsupported).append(day)
    runtime_audit: Mapping[str, Any] = {}
    if support:
        evaluator = _load_runtime_policy_evaluator(
            resolved_policy,
            expected_policy_sha256=policy_sha256,
        )
        runtime_audit = dict(evaluator.audit())
    return {
        "identity": IDENTITY,
        "passed": True,
        "days": len(days),
        "prefix_days": 40,
        "added_days": 10,
        "policy_path": str(resolved_policy),
        "policy_sha256": policy_sha256,
        "raw_m2_supported_days": support,
        "raw_m2_unsupported_days": unsupported,
        "runtime_policy_load_audit": runtime_audit,
        "unsupported_day_policy": "exact_candidate_equals_control_fallback",
        "denominator_days_dropped": 0,
        "engine": "python",
        "strict_queue_authority": False,
        "receive_time_transport_authority": False,
        "action_authorized": False,
        "live_authorized": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("preflight", "run", "finalize", "status"):
        command = sub.add_parser(name)
        command.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
        if name != "status":
            command.add_argument("--cache-root", type=Path, default=DEFAULT_BASELINE_CACHE)
            command.add_argument("--policy-path", type=Path, default=DEFAULT_POLICY)
            command.add_argument("--policy-sha256", default=DEFAULT_POLICY_SHA256)
        if name in {"preflight", "run", "status"}:
            command.add_argument(
                "--observation-cache-root",
                type=Path,
                default=DEFAULT_OBSERVATION_CACHE,
            )
    run_parser = sub.choices["run"]
    run_parser.add_argument("--days", nargs="*")
    run_parser.add_argument("--workers", type=int, default=1)
    run_parser.add_argument(
        "--progress-interval-events",
        type=int,
        default=DEFAULT_PROGRESS_INTERVAL_EVENTS,
    )
    final_parser = sub.choices["finalize"]
    final_parser.add_argument("--bootstrap-draws", type=int, default=DEFAULT_BOOTSTRAP_DRAWS)
    final_parser.add_argument("--bootstrap-seed", type=int, default=DEFAULT_BOOTSTRAP_SEED)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "preflight":
        payload = preflight(
            cache_root=args.cache_root,
            observation_cache_root=args.observation_cache_root,
            policy_path=args.policy_path,
            policy_sha256=args.policy_sha256,
        )
    elif args.command == "run":
        payload = run(
            days=args.days,
            workers=args.workers,
            cache_root=args.cache_root,
            observation_cache_root=args.observation_cache_root,
            policy_path=args.policy_path,
            policy_sha256=args.policy_sha256,
            output=args.output,
            progress_interval_events=args.progress_interval_events,
        )
    elif args.command == "finalize":
        payload = finalize(
            cache_root=args.cache_root,
            policy_path=args.policy_path,
            policy_sha256=args.policy_sha256,
            output=args.output,
            bootstrap_draws=args.bootstrap_draws,
            bootstrap_seed=args.bootstrap_seed,
        )
    elif args.command == "status":
        payload = status(
            output=args.output,
            observation_cache_root=args.observation_cache_root,
        )
    else:
        raise AssertionError(args.command)
    print(json.dumps(_json_safe(payload), indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
