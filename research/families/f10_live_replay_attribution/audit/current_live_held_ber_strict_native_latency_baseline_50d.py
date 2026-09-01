#!/usr/bin/env python3
"""Run the 50-day live-held BER baseline with strict native queue and latency."""

from __future__ import annotations

import argparse
import concurrent.futures
import copy
import json
import math
import os
import shutil
import subprocess
import tempfile
import time
import uuid
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from data_paths import data_root, native_exchange_book_cache_root, resolve_portable_path
from models import backtest_tick as bt
from models import data_windows
from models.backtest_config import (
    add_queue_calibration_params,
    validate_formal_replay_calibration,
)
from models.exchange_book_replay import (
    CryptoHFTExchangeBookTape,
    HistoricalMessageDeliverySchedule,
    ReceiveTimeCooldownReplayAdapter,
)
from research.families.f10_live_replay_attribution.audit import (
    current_live_held_ber_replay_baseline_50d as parent,
)

ROOT = Path(__file__).resolve().parents[4]
IDENTITY = "btc_usdc_current_live_held_ber_strict_native_latency_baseline_50d_v1_20260810"
CURRENT_IDENTITY = "btc_usdc_current_policy_strict_native_latency_baseline_50d"
SPEC_LOCATOR = (
    "${NARROWGATE_PRIVATE_RESEARCH_ROOT}/"
    "current_live_held_ber_strict_native_latency_baseline_50d_v1_spec_20260810.json"
)
DEFAULT_OUTPUT = (
    data_root(ROOT)
    / "reports/current_live_held_ber_strict_native_latency_baseline_50d_v1_20260810"
)
DEFAULT_NATIVE_CACHE = native_exchange_book_cache_root(ROOT)
DEFAULT_CURRENT_OUTPUT = data_root(ROOT) / "reports/current_policy_strict_native_baseline_50d"
DEFAULT_CURRENT_CONFIG = ROOT / "docs/private/live_config.current.local.yaml"
DAY_SUCCESS = "_SUCCESS"
PANEL_SUCCESS = "_PANEL_SUCCESS"
ENGINES = ("python", "cpp")
BACKEND_IDENTITY_SCHEMA = f"{CURRENT_IDENTITY}.backend.v1"

NATIVE_COUNTER_FIELDS = (
    "exchange_book_queue_lookup_count",
    "exchange_book_queue_exact_count",
    "exchange_book_queue_known_zero_count",
    "exchange_book_queue_missing_count",
    "exchange_book_queue_invalidated_order_count",
    "exchange_book_queue_cancel_ahead_event_count",
    "exchange_book_queue_cancel_ahead_qty",
    "exchange_book_queue_ambiguous_event_count",
    "exchange_book_cancel_trade_ambiguous_order_count",
    "exchange_book_cancel_book_ambiguous_order_count",
    "exchange_book_events_consumed",
    "exchange_book_events_accepted",
    "exchange_book_events_rejected",
    "exchange_book_source_gap_events",
    "exchange_book_invalid_sequence_messages",
    "exchange_book_snapshot_events",
    "exchange_book_delta_events",
    "exchange_book_delta_bootstrap_events",
    "exchange_book_sequence_gaps",
    "exchange_book_message_time_reversals",
    "exchange_book_transaction_timestamp_events",
    "exchange_book_event_timestamp_fallback_events",
    "exchange_book_receive_timestamp_fallback_events",
    "exchange_book_unknown_timestamp_source_events",
)

LATENCY_FIELDS = (
    "new_order_latency_ms",
    "cancel_order_latency_ms",
    "latency_jitter_ms",
    "new_order_latency_sample_count",
    "cancel_order_latency_sample_count",
    "exec_book_visibility_delay_enabled",
    "exec_book_visibility_delay_sample_count",
    "exec_book_visibility_delay_applied_count",
    "exec_book_visibility_delay_applied_avg_ms",
    "exec_book_visibility_delay_applied_max_ms",
    "exec_depth_visibility_delay_applied_avg_ms",
    "exec_depth_visibility_delay_applied_max_ms",
    "exec_trade_visibility_delay_applied_avg_ms",
    "exec_trade_visibility_delay_applied_max_ms",
    "exec_book_visibility_paired_hit_count",
    "exec_book_visibility_paired_miss_count",
    "latency_sampler_version",
    "latency_profile_id",
    "latency_scenario",
    "latency_seed",
)


class StrictNativeLatencyError(RuntimeError):
    """Raised when strict queue or latency evidence is incomplete."""


def _normalize_engine(engine: str) -> str:
    normalized = str(engine or "").strip().lower()
    if normalized not in ENGINES:
        raise StrictNativeLatencyError("engine must be python or cpp")
    return normalized


def _git_source_identity(*, require_tracked_clean: bool = False) -> dict[str, str]:
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip().lower()
        tree = subprocess.run(
            ["git", "rev-parse", "HEAD^{tree}"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip().lower()
        if require_tracked_clean:
            tracked_status = subprocess.run(
                ["git", "status", "--porcelain", "--untracked-files=no"],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
            if tracked_status:
                raise StrictNativeLatencyError(
                    "formal replay requires a tracked-clean worktree"
                )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise StrictNativeLatencyError("cannot resolve replay source identity") from exc
    if not commit or not tree:
        raise StrictNativeLatencyError("replay source identity is incomplete")
    return {"commit": commit, "tree": tree}


def _backend_contract(
    engine: str,
    *,
    cpp_qualification_receipt: Path | None = None,
    cpp_qualification_receipt_sha256: str | None = None,
) -> dict[str, Any]:
    normalized = _normalize_engine(engine)
    if normalized == "python":
        source_identity = _git_source_identity(require_tracked_clean=True)
        if cpp_qualification_receipt is not None or cpp_qualification_receipt_sha256:
            raise StrictNativeLatencyError(
                "Python replay cannot consume a C++ qualification receipt"
            )
        root = parent._canonical_sha256(
            {
                "schema_version": BACKEND_IDENTITY_SCHEMA,
                "engine": normalized,
                "source_identity": source_identity,
            }
        )
        return {
            "engine": normalized,
            "backend_identity_root": root,
            "source_identity": source_identity,
            "authoritative": True,
            "qualification_under_test": False,
            "qualification_receipt_path": None,
        }

    if cpp_qualification_receipt is None:
        raise StrictNativeLatencyError(
            "C++ current-policy replay requires a qualification receipt path"
        )
    source_identity = _git_source_identity(require_tracked_clean=True)
    receipt_path = cpp_qualification_receipt.expanduser().resolve()
    if not receipt_path.is_file():
        raise StrictNativeLatencyError(
            f"C++ qualification receipt is missing: {receipt_path}"
        )
    expected_root = str(cpp_qualification_receipt_sha256 or "").strip().lower()
    if len(expected_root) != 64 or any(
        character not in "0123456789abcdef" for character in expected_root
    ):
        raise StrictNativeLatencyError(
            "C++ qualification receipt requires an expected SHA256 root"
        )
    if parent._sha256_file(receipt_path) != expected_root:
        raise StrictNativeLatencyError("C++ qualification receipt root drifted")
    return {
        "engine": normalized,
        "backend_identity_root": expected_root,
        "source_identity": source_identity,
        "authoritative": True,
        "qualification_under_test": False,
        "qualification_receipt_path": str(receipt_path),
    }


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (float, np.floating)):
        return float(value) if math.isfinite(float(value)) else None
    if isinstance(value, (int, np.integer)):
        return int(value)
    return value


def _spec_path() -> Path:
    try:
        return resolve_portable_path(SPEC_LOCATOR, root=ROOT)
    except (RuntimeError, ValueError) as exc:
        raise StrictNativeLatencyError(
            "strict-native latency spec requires NARROWGATE_PRIVATE_RESEARCH_ROOT"
        ) from exc


def _spec(*, verify_historical_parent: bool = True) -> dict[str, Any]:
    payload = parent._load_json(_spec_path(), role="strict-native latency spec")
    if payload.get("identity") != IDENTITY:
        raise StrictNativeLatencyError("strict-native latency identity drifted")
    if verify_historical_parent:
        parent_path = parent._resolve_repo_path(payload["parent_diagnostic"]["path"])
        parent._validate_file(
            parent_path,
            payload["parent_diagnostic"]["sha256"],
            role="parent 50-day diagnostic",
        )
    parent_spec = parent._spec()
    if len(parent.ordered_days(parent_spec)) != int(payload["parent_diagnostic"]["days"]):
        raise StrictNativeLatencyError("parent 50-day denominator drifted")
    queue = payload["queue_calibration"]
    parent._validate_file(
        parent._resolve_repo_path(str(queue["path"])),
        str(queue["sha256"]),
        role="queue calibration v3",
    )
    return payload


def _profile_path(spec: Mapping[str, Any]) -> Path:
    visibility = spec["strategy_visibility"]
    gateway = spec["gateway_latency"]
    if visibility["profile_path"] != gateway["profile_path"]:
        raise StrictNativeLatencyError("visibility and gateway profiles diverged")
    if visibility["profile_sha256"] != gateway["profile_sha256"]:
        raise StrictNativeLatencyError("visibility and gateway profile hashes diverged")
    return parent._validate_file(
        parent._resolve_repo_path(str(visibility["profile_path"])),
        str(visibility["profile_sha256"]),
        role="AWS Tokyo latency profile",
    )


def _latency_inputs(spec: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    path = _profile_path(spec)
    rest = bt._load_live_perf_latency_samples(
        path,
        mode=str(spec["gateway_latency"]["sample_mode"]),
    )
    visibility = bt._load_exec_book_visibility_profile(path)
    new_samples = np.ascontiguousarray(
        rest["new_order_latency_samples_ms"], dtype=np.float64
    )
    cancel_samples = np.ascontiguousarray(
        rest["cancel_order_latency_samples_ms"], dtype=np.float64
    )
    book_samples = np.ascontiguousarray(
        visibility["exec_book_visibility_delay_samples_ms"], dtype=np.float64
    )
    expected = spec["gateway_latency"]
    if len(new_samples) != int(expected["new_order_samples"]):
        raise StrictNativeLatencyError("new-order latency sample count drifted")
    if len(cancel_samples) != int(expected["cancel_order_samples"]):
        raise StrictNativeLatencyError("cancel latency sample count drifted")
    if len(book_samples) != int(spec["strategy_visibility"]["observed_visibility_rows"]):
        raise StrictNativeLatencyError("book visibility sample count drifted")
    params = {
        "_new_order_latency_samples_ms": new_samples,
        "_cancel_order_latency_samples_ms": cancel_samples,
        "_exec_book_visibility_delay_samples_ms": book_samples,
        "live_perf_latency_mode": str(expected["sample_mode"]),
        "latency_profile_id": "aws_tokyo_live_perf_20260715_full_avg_v1",
        "latency_environment": str(spec["strategy_visibility"]["environment"]),
        "latency_scenario": "baseline",
        "latency_seed": int(expected["latency_seed"]),
        "exec_book_visibility_mode": str(spec["strategy_visibility"]["mode"]),
        "exec_book_visibility_delay_profile_id": (
            "aws_tokyo_live_perf_20260715_full_visibility_age_v1"
        ),
        "exec_book_visibility_delay_profile_path": str(path),
        "exec_book_visibility_delay_seed": int(
            spec["strategy_visibility"]["visibility_seed"]
        ),
    }
    audit = {
        "profile_path": str(path),
        "profile_sha256": parent._sha256_file(path),
        "new_order_samples": len(new_samples),
        "cancel_order_samples": len(cancel_samples),
        "visibility_samples": len(book_samples),
        "visibility_mean_ms": float(np.mean(book_samples)),
        "visibility_p50_ms": float(np.quantile(book_samples, 0.50)),
        "visibility_p90_ms": float(np.quantile(book_samples, 0.90)),
        "visibility_p99_ms": float(np.quantile(book_samples, 0.99)),
    }
    return params, audit


def _apply_strict_transport(
    params: dict[str, Any],
    spec: Mapping[str, Any],
    *,
    projection_audit: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    latency_params, latency_audit = _latency_inputs(spec)
    params.update(latency_params)
    params.update(
        {
            "replay_purpose": "formal",
            "replay_event_clock": str(spec["policy_clock"]["mode"]),
            "replay_clock_interval_ms": int(
                spec["policy_clock"]["nominal_interval_ms"]
            ),
            "execution_trade_source": "individual_trades",
            "exchange_book_queue_mode": "strict",
            "queue_l2_cancel_ahead_enabled": False,
            "dynamic_fill_hazard_action_enabled": False,
            "dynamic_fill_hazard_shadow_enabled": False,
            "buy_fill_selection_live_enabled": False,
            "buy_fill_selection_shadow_enabled": False,
        }
    )
    queue = spec["queue_calibration"]
    add_queue_calibration_params(
        params,
        symbol="BTCUSDC",
        strict=True,
        path=parent._resolve_repo_path(str(queue["path"])),
    )
    if params.get("queue_calibration_schema_version") != queue["schema_version"]:
        raise StrictNativeLatencyError("queue calibration schema drifted")
    if params.get("queue_calibration_apply_mode") != queue["apply_mode"]:
        raise StrictNativeLatencyError("queue calibration apply mode drifted")
    if params.get("queue_calibration_fit_days") != queue["fit_days"]:
        raise StrictNativeLatencyError("queue calibration fit days drifted")
    if params.get("queue_calibration_replay_params") != queue["replay_params"]:
        raise StrictNativeLatencyError("queue calibration replay parameters drifted")
    validate_formal_replay_calibration(params, require_latency=True)
    return params, {
        "offline_projection": dict(projection_audit),
        "latency": latency_audit,
    }


def _strict_base(spec: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    parent_spec = parent._spec()
    params, projection_audit = parent._base_params(parent_spec)
    return _apply_strict_transport(
        params,
        spec,
        projection_audit=projection_audit,
    )


def _resolved_config_path(config_path: Path, value: object) -> Path:
    candidate = Path(str(value or "")).expanduser()
    if not str(value or "").strip():
        raise StrictNativeLatencyError("current policy config contains an empty artifact path")
    if not candidate.is_absolute():
        candidate = ROOT / candidate
    resolved = candidate.resolve()
    if not resolved.is_file():
        raise StrictNativeLatencyError(
            f"current policy artifact is missing for {config_path}: {resolved}"
        )
    return resolved


def _current_config_payload(config_path: Path) -> dict[str, Any]:
    resolved = config_path.expanduser().resolve()
    if not resolved.is_file():
        raise StrictNativeLatencyError(f"current live config is missing: {resolved}")
    payload = yaml.safe_load(resolved.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get("strategy"), dict):
        raise StrictNativeLatencyError("current live config must contain a strategy mapping")
    return payload


def _project_current_config(config_path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    """Load current economics while disabling live-only storage wiring.

    The two evidence-route names below are deployment vocabulary aliases.  The
    policy bytes and every economic field remain unchanged.
    """

    source = _current_config_payload(config_path)
    projected = copy.deepcopy(source)
    strategy = projected["strategy"]
    strategy["boolean_cooldown_evidence_route"] = "private_deployment_approval"
    strategy["buy_e3_cooldown_evidence_route"] = "private_deployment_buy_e3"
    lifecycle = projected.setdefault("lifecycle_journal_v2", {})
    if not isinstance(lifecycle, dict):
        raise StrictNativeLatencyError("lifecycle_journal_v2 must be a mapping")
    lifecycle["enabled"] = False

    previous = {
        name: os.environ.get(name)
        for name in (
            "NARROWGATE_ALLOW_F05_BOOLEAN_COOLDOWN_PRIVATE_DEPLOY",
            "NARROWGATE_ALLOW_F05_BUY_E3_PRIVATE_DEPLOY",
        )
    }
    os.environ["NARROWGATE_ALLOW_F05_BOOLEAN_COOLDOWN_PRIVATE_DEPLOY"] = "1"
    os.environ["NARROWGATE_ALLOW_F05_BUY_E3_PRIVATE_DEPLOY"] = "1"
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".yaml",
            prefix="narrowgate-current-replay-",
            encoding="utf-8",
            delete=False,
        ) as handle:
            yaml.safe_dump(projected, handle, sort_keys=False)
            temporary_path = Path(handle.name)
        params = parent.native_runner._load_formal_base_params(temporary_path)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    return params, {
        "source_config": str(config_path.expanduser().resolve()),
        "source_config_sha256": parent._sha256_file(config_path.expanduser().resolve()),
        "economic_fields_changed": False,
        "live_storage_disabled": True,
        "deployment_route_aliases_applied": True,
    }


def _current_base(
    spec: Mapping[str, Any],
    config_path: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    params, projection_audit = _project_current_config(config_path)
    params, audit = _apply_strict_transport(
        params,
        spec,
        projection_audit=projection_audit,
    )
    params.update(
        {
            "execution_trade_source": "trades",
            "baseline_selection": CURRENT_IDENTITY,
            "current_e3_mechanics_default": True,
            "trace_cooldown_duration_opportunities_max": 1_000_000,
        }
    )
    return params, audit, _current_config_payload(config_path)


def _native_tape(
    spec: Mapping[str, Any],
    *,
    day: str,
    cache_dir: Path,
) -> CryptoHFTExchangeBookTape:
    truth = spec["exchange_truth"]
    return CryptoHFTExchangeBookTape(
        raw_root=parent._resolve_repo_path(str(truth["raw_root"])),
        day=day,
        symbol=str(truth["symbol"]),
        exchange=str(truth["exchange"]),
        tick_size=0.1,
        warmup_hours=int(truth["warmup_hours"]),
        strict_complete=True,
        cache_dir=cache_dir,
        cache_enabled=True,
    )


def _splitmix64_array(values: np.ndarray) -> np.ndarray:
    data = np.asarray(values, dtype=np.uint64).copy()
    with np.errstate(over="ignore"):
        data += np.uint64(0x9E3779B97F4A7C15)
        data = (data ^ (data >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
        data = (data ^ (data >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
    return data ^ (data >> np.uint64(31))


def _message_visibility_delay_ms(
    timestamps_ms: np.ndarray,
    *,
    samples_ms: np.ndarray,
    seed: int,
) -> np.ndarray:
    timestamps = np.asarray(timestamps_ms, dtype=np.int64)
    samples = np.asarray(samples_ms, dtype=np.float64)
    samples = samples[np.isfinite(samples) & (samples >= 0.0)]
    if timestamps.ndim != 1 or samples.ndim != 1 or not samples.size:
        raise StrictNativeLatencyError("message visibility inputs are incomplete")
    mixed = _splitmix64_array(
        timestamps.astype(np.uint64, copy=False) ^ np.uint64(int(seed))
    )
    delays = np.rint(samples[(mixed % np.uint64(samples.size)).astype(np.int64)])
    delays = np.maximum(delays, 0.0).astype(np.int64)
    for index in {0, len(timestamps) // 2, len(timestamps) - 1}:
        expected = bt._exec_book_visibility_delay_ms(
            int(timestamps[index]),
            mean_ms=0.0,
            jitter_ms=0.0,
            seed=int(seed),
            samples_ms=samples,
        )
        if int(delays[index]) != expected:
            raise StrictNativeLatencyError("vectorized visibility sampler drifted")
    return delays


def _current_policy_adapter(
    *,
    window: Any,
    params: Mapping[str, Any],
    config_path: Path,
    config: Mapping[str, Any],
) -> ReceiveTimeCooldownReplayAdapter:
    from strategy.boolean_cooldown_buy_e3 import LiveBuyE3CooldownPolicy
    from strategy.boolean_cooldown_live import LiveBooleanCooldownPolicy

    strategy = config["strategy"]
    risk = config.get("risk") or {}
    if not bool(strategy.get("boolean_cooldown_policy_enabled", False)):
        raise StrictNativeLatencyError("current SELL owner cooldown is not enabled")
    if not bool(strategy.get("buy_e3_cooldown_policy_enabled", False)):
        raise StrictNativeLatencyError("current BUY E3 cooldown is not enabled")
    max_feature_age_s = float(risk.get("max_exec_book_visible_age_s", 0.0))
    if not math.isfinite(max_feature_age_s) or max_feature_age_s <= 0.0:
        raise StrictNativeLatencyError("current policy feature-age limit is invalid")

    sell = LiveBooleanCooldownPolicy.from_files(
        policy_path=_resolved_config_path(
            config_path, strategy["boolean_cooldown_policy_path"]
        ),
        policy_sha256=str(strategy["boolean_cooldown_policy_sha256"]),
        predicate_bundle_path=_resolved_config_path(
            config_path, strategy["boolean_cooldown_predicate_bundle_path"]
        ),
        predicate_bundle_sha256=str(
            strategy["boolean_cooldown_predicate_bundle_sha256"]
        ),
        warmup_s=float(strategy["boolean_cooldown_ema_warmup_s"]),
        max_feature_age_s=max_feature_age_s,
    )
    buy = LiveBuyE3CooldownPolicy.from_files(
        artifact_manifest_path=_resolved_config_path(
            config_path, strategy["buy_e3_cooldown_artifact_manifest_path"]
        ),
        artifact_manifest_sha256=str(
            strategy["buy_e3_cooldown_artifact_manifest_sha256"]
        ),
        expected_artifact_sha256=str(strategy["buy_e3_cooldown_artifact_sha256"]),
        policy_path=_resolved_config_path(
            config_path, strategy["buy_e3_cooldown_policy_path"]
        ),
        policy_sha256=str(strategy["buy_e3_cooldown_policy_sha256"]),
        predicate_bundle_path=_resolved_config_path(
            config_path, strategy["buy_e3_cooldown_predicate_bundle_path"]
        ),
        predicate_bundle_sha256=str(
            strategy["buy_e3_cooldown_predicate_bundle_sha256"]
        ),
        warmup_s=float(strategy["buy_e3_cooldown_ema_warmup_s"]),
        max_feature_age_s=max_feature_age_s,
    )

    depth = window.l2_data
    if depth is None or not len(depth.ts_ms):
        raise StrictNativeLatencyError("current policy replay requires D-1/target depth")
    exchange_ns = np.asarray(depth.ts_ms, dtype=np.int64) * np.int64(1_000_000)
    visibility_samples = np.asarray(
        params["_exec_book_visibility_delay_samples_ms"], dtype=np.float64
    )
    delays_ms = _message_visibility_delay_ms(
        np.asarray(depth.ts_ms, dtype=np.int64),
        samples_ms=visibility_samples,
        seed=int(params["exec_book_visibility_delay_seed"]),
    )
    receive_ns = exchange_ns + delays_ms * np.int64(1_000_000)
    schedule = HistoricalMessageDeliverySchedule(
        exchange_ns,
        receive_ns,
        receive_ns,
    )
    return ReceiveTimeCooldownReplayAdapter(
        depth,
        schedule,
        policies={"BUY": buy, "SELL": sell},
    )


def _configure_cpp_current_policy(
    params: dict[str, Any],
    *,
    adapter: ReceiveTimeCooldownReplayAdapter,
    backend: Mapping[str, Any],
) -> None:
    if backend.get("engine") != "cpp":
        raise StrictNativeLatencyError("C++ policy setup received a non-C++ backend")
    receipt_root = str(backend.get("backend_identity_root") or "")
    receipt_path = str(backend.get("qualification_receipt_path") or "")
    if bool(backend.get("qualification_under_test", True)) or not bool(
        backend.get("authoritative", False)
    ):
        raise StrictNativeLatencyError("C++ backend is not authoritative")

    cpp = bt._load_cpp_tick_replay()
    runtime = adapter.compile_cpp_runtime(
        cpp,
        parity_qualified=True,
        parity_qualification_sha256=receipt_root,
    )
    params.update(
        {
            "cooldown_duration_policy_cpp_runtime": runtime,
            "cooldown_duration_policy_cpp_parity_qualified": True,
            "cooldown_duration_policy_cpp_parity_receipt_path": receipt_path,
            "cooldown_duration_policy_cpp_parity_receipt_sha256": receipt_root,
            "cooldown_duration_policy_cpp_expected_source_identity": dict(
                backend["source_identity"]
            ),
            "_cooldown_duration_policy_cpp_window_arrays": (
                adapter.cpp_window_arrays()
            ),
            "_cooldown_duration_policy_cpp_window_tape": (),
            "_cooldown_duration_policy_cpp_predicate_rows": (),
        }
    )
    # Use the replay core's single receipt validator.  The engine dispatch
    # repeats this check immediately before native execution; neither caller
    # booleans nor qualification-under-test can grant authority here.
    bt._validate_f05_cpp_cooldown_runtime(params, require_full_replay=True)
    if params.get("_cooldown_duration_policy_cpp_validated_receipt_sha256") != (
        receipt_root
    ):
        raise StrictNativeLatencyError(
            "C++ qualification receipt was not admitted by the replay core"
        )


def _load_current_window(
    day: str,
    *,
    params: dict[str, Any],
    cache_dir: Path,
    feature_dir: Path,
    training_feature_manifest: Path | None,
) -> tuple[Any, tuple[Any, ...], dict[str, Any]]:
    model_dir = Path(str(params.get("resolved_model_dir") or "")).expanduser().resolve()
    if not model_dir.is_dir():
        raise StrictNativeLatencyError("current model directory is missing")
    replay_feature_dir = feature_dir.expanduser().resolve()
    replay_manifest_path = replay_feature_dir / "causal_feature_manifest.json"
    replay_manifest = parent._load_json(
        replay_manifest_path,
        role="current replay feature manifest",
    )

    metadata = []
    for meta_path in sorted(model_dir.glob("*_meta.json")):
        payload = parent._load_json(meta_path, role=f"model metadata {meta_path.name}")
        if str(payload.get("feature_manifest_sha256") or ""):
            metadata.append(payload)
    expected_training_roots = {
        str(payload["feature_manifest_sha256"]) for payload in metadata
    }
    recorded_training_paths = {
        str(payload.get("feature_manifest_path") or "") for payload in metadata
    }
    if len(expected_training_roots) != 1 or len(recorded_training_paths) != 1:
        raise StrictNativeLatencyError("current model feature identity is inconsistent")
    expected_training_root = next(iter(expected_training_roots))
    resolved_training_manifest = (
        training_feature_manifest.expanduser().resolve()
        if training_feature_manifest is not None
        else Path(next(iter(recorded_training_paths))).expanduser().resolve()
    )
    parent._validate_file(
        resolved_training_manifest,
        expected_training_root,
        role="model training feature manifest",
    )
    training_manifest = parent._load_json(
        resolved_training_manifest,
        role="model training feature manifest",
    )
    compatibility_fields = (
        "schema_version",
        "symbol",
        "config_sha256",
        "feature_timestamp_semantics",
        "feature_bucket_ms",
        "feature_ready_offset_ms",
        "feature_semantics_version",
        "feature_dag_id",
        "feature_dag_sha256",
        "feature_cutoff_semantics",
        "calendar_timestamp_semantics",
        "microstructure_5s_semantics",
        "market_stage",
        "reference_symbol",
    )
    mismatches = [
        field
        for field in compatibility_fields
        if replay_manifest.get(field) != training_manifest.get(field)
    ]
    if mismatches:
        raise StrictNativeLatencyError(
            "replay feature panel is incompatible with the model training panel: "
            + ", ".join(mismatches)
        )

    rows = {
        str(row.get("day")): row
        for row in replay_manifest.get("daily_files") or ()
        if isinstance(row, Mapping)
    }
    prior_day = (datetime.fromisoformat(day) - timedelta(days=1)).date().isoformat()
    selected_paths: dict[str, Path] = {}
    for selected_day in (prior_day, day):
        row = rows.get(selected_day)
        if row is None:
            raise StrictNativeLatencyError(
                f"replay feature panel is missing {selected_day}"
            )
        path = replay_feature_dir / str(row.get("file") or "")
        selected_paths[selected_day] = parent._validate_file(
            path,
            str(row.get("sha256") or ""),
            role=f"{selected_day} replay feature file",
        )

    window = data_windows.load_tick_window(
        day,
        params,
        load_ml=False,
        require_ml=False,
        run_ml_inference=False,
        cross_market_enabled=True,
        require_historical_bbo=True,
        require_formal_l2=True,
        cache_dir=cache_dir,
    )
    ml_data = parent.control_overlay._generate_ml_data(
        selected_paths[prior_day],
        selected_paths[day],
        day=day,
        model_dir=model_dir,
    )
    ml_data = parent.control_overlay._validate_ml_data(ml_data, day=day)
    return window, ml_data, {
        "day": day,
        "execution_trade_source": str(window.execution_trade_source),
        "book_source_authority": str(window.book_source_authority),
        "book_dataset_version": str(window.book_dataset_version),
        "formal_lifecycle_replay_eligible": bool(
            window.formal_lifecycle_replay_eligible
        ),
        "exact_queue_policy_eligible": bool(window.exact_queue_policy_eligible),
        "training_feature_manifest_sha256": expected_training_root,
        "replay_feature_manifest_sha256": parent._sha256_file(replay_manifest_path),
        "replay_feature_days": [prior_day, day],
    }


def _execution_fields(result: Mapping[str, Any]) -> dict[str, Any]:
    fields: dict[str, Any] = {
        "exchange_book_queue_mode": str(result.get("exchange_book_queue_mode", "")),
        "exchange_book_queue_scope": str(result.get("exchange_book_queue_scope", "")),
        "book_source_authority": str(result.get("book_source_authority", "")),
        "book_exact_queue_policy_eligible": bool(
            result.get("book_exact_queue_policy_eligible", False)
        ),
    }
    for key in NATIVE_COUNTER_FIELDS:
        value = result.get(key, 0)
        fields[key] = float(value) if key.endswith("_qty") else int(value or 0)
    for key in LATENCY_FIELDS:
        value = result.get(key)
        if isinstance(value, (bool, np.bool_)):
            fields[key] = bool(value)
        elif isinstance(value, (int, np.integer)):
            fields[key] = int(value)
        elif isinstance(value, (float, np.floating)):
            fields[key] = float(value)
        else:
            fields[key] = value
    return fields


def _validate_execution(summary: Mapping[str, Any]) -> None:
    if summary.get("exchange_book_queue_mode") != "strict":
        raise StrictNativeLatencyError("exchange-book queue mode is not strict")
    if summary.get("exchange_book_queue_scope") != (
        "strategy_independent_native_snapshot_delta_exchange_time_v1"
    ):
        raise StrictNativeLatencyError("exchange-book queue scope drifted")
    if int(summary.get("exchange_book_events_consumed", 0)) <= 0:
        raise StrictNativeLatencyError("no native exchange-book events were consumed")
    if int(summary.get("exchange_book_events_accepted", 0)) <= 0:
        raise StrictNativeLatencyError("no native exchange-book events were accepted")
    lookups = int(summary.get("exchange_book_queue_lookup_count", 0))
    accounted = sum(
        int(summary.get(key, 0))
        for key in (
            "exchange_book_queue_exact_count",
            "exchange_book_queue_known_zero_count",
            "exchange_book_queue_missing_count",
        )
    )
    if lookups <= 0 or lookups != accounted:
        raise StrictNativeLatencyError("native queue lookup accounting failed")
    zero_fields = (
        "exchange_book_queue_missing_count",
        "exchange_book_source_gap_events",
        "exchange_book_invalid_sequence_messages",
        "exchange_book_sequence_gaps",
        "exchange_book_message_time_reversals",
    )
    for key in zero_fields:
        if int(summary.get(key, -1)) != 0:
            raise StrictNativeLatencyError(f"strict native gate failed: {key}")
    for key in (
        "new_order_latency_sample_count",
        "cancel_order_latency_sample_count",
        "exec_book_visibility_delay_sample_count",
        "exec_book_visibility_delay_applied_count",
    ):
        if int(summary.get(key, 0)) <= 0:
            raise StrictNativeLatencyError(f"latency gate failed: {key}")
    if not bool(summary.get("exec_book_visibility_delay_enabled", False)):
        raise StrictNativeLatencyError("execution-book visibility delay is disabled")


def preflight(
    *,
    native_cache: Path = DEFAULT_NATIVE_CACHE,
    live_config: Path | None = None,
    engine: str = "python",
    cpp_qualification_receipt: Path | None = None,
    cpp_qualification_receipt_sha256: str | None = None,
) -> dict[str, Any]:
    backend = _backend_contract(
        engine,
        cpp_qualification_receipt=cpp_qualification_receipt,
        cpp_qualification_receipt_sha256=cpp_qualification_receipt_sha256,
    )
    if backend["engine"] == "cpp" and live_config is None:
        raise StrictNativeLatencyError(
            "C++ qualification receipt is limited to the current-policy runner"
        )
    current_mode = live_config is not None
    spec = _spec(verify_historical_parent=not current_mode)
    parent_spec = parent._spec()
    days = parent.ordered_days(parent_spec)
    _, latency_audit = _latency_inputs(spec)
    source_hours = 0
    for day in days:
        tape = _native_tape(spec, day=day, cache_dir=native_cache)
        source_hours += len(tape.source_paths)
    return {
        "identity": CURRENT_IDENTITY if current_mode else IDENTITY,
        "engine": backend["engine"],
        "backend_identity_root": backend["backend_identity_root"],
        "backend_authoritative": bool(backend["authoritative"]),
        "qualification_under_test": bool(backend["qualification_under_test"]),
        "passed": True,
        "days": len(days),
        "strict_complete_days": len(days),
        "target_plus_warmup_source_hours": source_hours,
        "native_cache": str(native_cache),
        "latency": latency_audit,
        "exact_historical_receive_time_authority": False,
    }


def _day_dir(output: Path, day: str) -> Path:
    return output / "days" / day


def _load_day(
    output: Path,
    day: str,
    *,
    engine: str,
    backend_identity_root: str,
    identity: str = IDENTITY,
    config_sha256: str | None = None,
) -> dict[str, Any] | None:
    directory = _day_dir(output, day)
    manifest_path = directory / "manifest.json"
    marker = directory / DAY_SUCCESS
    if not manifest_path.is_file() or not marker.is_file():
        return None
    if marker.read_text(encoding="ascii").strip() != parent._sha256_file(manifest_path):
        raise StrictNativeLatencyError(f"{day} admission marker drifted")
    manifest = parent._load_json(manifest_path, role=f"{day} strict-native manifest")
    if manifest.get("identity") != identity:
        raise StrictNativeLatencyError(f"{day} baseline identity drifted")
    if manifest.get("engine") != _normalize_engine(engine):
        raise StrictNativeLatencyError(f"{day} replay engine drifted")
    if manifest.get("backend_identity_root") != backend_identity_root:
        raise StrictNativeLatencyError(f"{day} replay backend identity drifted")
    if manifest.get("backend_authoritative") is not True:
        raise StrictNativeLatencyError(f"{day} replay backend is not authoritative")
    if manifest.get("qualification_under_test") is not False:
        raise StrictNativeLatencyError(f"{day} replay is qualification-under-test")
    if config_sha256 is not None and manifest.get("current_config_sha256") != config_sha256:
        raise StrictNativeLatencyError(f"{day} current config drifted")
    for name in ("summary", "campaigns", "fills"):
        row = manifest[name]
        parent._validate_file(Path(row["path"]), row["sha256"], role=f"{day} {name}")
    return manifest


def execute_day(
    day: str,
    *,
    output: Path = DEFAULT_OUTPUT,
    native_cache: Path = DEFAULT_NATIVE_CACHE,
    live_config: Path | None = None,
    window_cache: Path | None = None,
    feature_dir: Path | None = None,
    training_feature_manifest: Path | None = None,
    engine: str = "python",
    cpp_qualification_receipt: Path | None = None,
    cpp_qualification_receipt_sha256: str | None = None,
) -> dict[str, Any]:
    backend = _backend_contract(
        engine,
        cpp_qualification_receipt=cpp_qualification_receipt,
        cpp_qualification_receipt_sha256=cpp_qualification_receipt_sha256,
    )
    engine = str(backend["engine"])
    current_mode = live_config is not None
    if engine == "cpp" and not current_mode:
        raise StrictNativeLatencyError(
            "C++ qualification receipt is limited to the current-policy runner"
        )
    identity = CURRENT_IDENTITY if current_mode else IDENTITY
    config_sha256 = (
        parent._sha256_file(live_config.expanduser().resolve())
        if live_config is not None
        else None
    )
    existing = _load_day(
        output,
        day,
        engine=engine,
        backend_identity_root=str(backend["backend_identity_root"]),
        identity=identity,
        config_sha256=config_sha256,
    )
    if existing is not None:
        return {"day": day, "reused": True}
    spec = _spec(verify_historical_parent=not current_mode)
    parent_spec = parent._spec()
    days = parent.ordered_days(parent_spec)
    if day not in days:
        raise StrictNativeLatencyError(f"day is outside frozen 50-day panel: {day}")
    if current_mode:
        assert live_config is not None
        if feature_dir is None:
            raise StrictNativeLatencyError(
                "current baseline requires an explicit compatible replay feature panel"
            )
        params, audit, current_config = _current_base(spec, live_config)
        resolved_window_cache = (
            window_cache.expanduser().resolve()
            if window_cache is not None
            else native_cache.expanduser().resolve().parent / "current-window-cache"
        )
        window, ml_data, binding = _load_current_window(
            day,
            params=params,
            cache_dir=resolved_window_cache,
            feature_dir=feature_dir,
            training_feature_manifest=training_feature_manifest,
        )
        policy_adapter = _current_policy_adapter(
            window=window,
            params=params,
            config_path=live_config,
            config=current_config,
        )
        params["cooldown_v2_snapshot_emitter"] = policy_adapter
        params["cooldown_duration_policy_evaluator"] = policy_adapter
        if engine == "cpp":
            _configure_cpp_current_policy(
                params,
                adapter=policy_adapter,
                backend=backend,
            )
    else:
        prepared = parent.prepare(parent.DEFAULT_CACHE)
        prefix = parent._prefix_plan(parent_spec)
        window, ml_data, binding = parent._load_day_inputs(
            day,
            spec=parent_spec,
            prefix_plan=prefix,
            prepared_plan=prepared,
            cache_root=parent.DEFAULT_CACHE,
        )
        params, audit = _strict_base(spec)
        policy_adapter = None
    tape = _native_tape(spec, day=day, cache_dir=native_cache)
    tape_identity = tape.identity(include_sha256=True)
    result = bt._simulate_tick_with_engine(
        engine,
        window.trades,
        window.var_ts_ms,
        window.var_ssq,
        params,
        ml_data=ml_data,
        bbo_data=window.bbo_data,
        l2_data=window.l2_data,
        var_ti=window.var_ti,
        var_retsq=window.var_retsq,
        exchange_book_event_tape=tape,
    )
    summary, campaigns, fills = parent.native_runner._project_arm(
        day=day,
        arm=(
            "current_buy_e3_sell_owner_baseline"
            if current_mode
            else "current_live_held_global_ber_control"
        ),
        result=result,
        order_size=float(params["order_size"]),
        campaign_mae_trace_max=int(params["trace_campaign_repair_max"]),
    )
    summary.update(_execution_fields(result))
    summary.update(
        {
            "engine": engine,
            "backend_identity_root": backend["backend_identity_root"],
            "backend_authoritative": bool(backend["authoritative"]),
            "qualification_under_test": bool(
                backend["qualification_under_test"]
            ),
            "cooldown_duration_policy_cpp_authoritative": (
                bool(result.get("cooldown_duration_policy_cpp_authoritative", False))
                if engine == "cpp"
                else None
            ),
            "cooldown_duration_policy_cpp_qualification_under_test": (
                bool(
                    result.get(
                        "cooldown_duration_policy_cpp_qualification_under_test",
                        False,
                    )
                )
                if engine == "cpp"
                else None
            ),
            "cooldown_duration_policy_cpp_parity_receipt_sha256": (
                str(
                    result.get(
                        "cooldown_duration_policy_cpp_parity_receipt_sha256",
                        "",
                    )
                    or ""
                )
                if engine == "cpp"
                else None
            ),
            "cooldown_duration_policy_cpp_event_loop_parity_qualified": (
                bool(
                    result.get(
                        "cooldown_duration_policy_cpp_event_loop_parity_qualified",
                        False,
                    )
                )
                if engine == "cpp"
                else None
            ),
            "engine_evidence": f"{engine}_strict_native_latency"
            + ("_current_policy" if current_mode else ""),
            "exact_historical_receive_time_authority": False,
            "latency_profile_sha256": audit["latency"]["profile_sha256"],
            "current_policy_adapter": (
                _json_safe(policy_adapter.audit())
                if policy_adapter is not None
                else None
            ),
        }
    )
    if engine == "cpp":
        if summary.get("cooldown_duration_policy_cpp_authoritative") is not True:
            raise StrictNativeLatencyError(
                "C++ replay did not emit authoritative current-policy evidence"
            )
        if summary.get("cooldown_duration_policy_cpp_qualification_under_test"):
            raise StrictNativeLatencyError(
                "C++ replay emitted qualification-under-test evidence"
            )
        if summary.get("cooldown_duration_policy_cpp_parity_receipt_sha256") != (
            backend["backend_identity_root"]
        ):
            raise StrictNativeLatencyError(
                "C++ result qualification root differs from the backend identity"
            )
        if summary.get(
            "cooldown_duration_policy_cpp_event_loop_parity_qualified"
        ) is not True:
            raise StrictNativeLatencyError(
                "C++ result lacks receipt-derived event-loop authority"
            )
        if params.get("_cooldown_duration_policy_cpp_validated_receipt_sha256") != (
            backend["backend_identity_root"]
        ):
            raise StrictNativeLatencyError(
                "C++ result does not match the admitted qualification root"
            )
    _validate_execution(summary)

    final = _day_dir(output, day)
    staging = final.parent / f".{day}.{uuid.uuid4().hex}.partial"
    staging.mkdir(parents=True, exist_ok=False)
    try:
        summary_path = staging / "summary.json"
        campaigns_path = staging / "campaigns.parquet"
        fills_path = staging / "fills.parquet"
        parent._atomic_json(summary_path, summary)
        campaigns.to_parquet(campaigns_path, index=False, compression="zstd")
        fills.to_parquet(fills_path, index=False, compression="zstd")
        manifest = {
            "schema_version": f"{identity}.day.v1",
            "identity": identity,
            "day": day,
            "engine": engine,
            "backend_identity_root": backend["backend_identity_root"],
            "backend_authoritative": bool(backend["authoritative"]),
            "qualification_under_test": bool(
                backend["qualification_under_test"]
            ),
            "spec_sha256": parent._sha256_file(_spec_path()),
            "current_config_sha256": config_sha256,
            "input_binding": binding,
            "native_tape_identity": tape_identity,
            "latency_audit": audit["latency"],
            "summary": {
                "path": str(final / "summary.json"),
                "sha256": parent._sha256_file(summary_path),
            },
            "campaigns": {
                "path": str(final / "campaigns.parquet"),
                "sha256": parent._sha256_file(campaigns_path),
            },
            "fills": {
                "path": str(final / "fills.parquet"),
                "sha256": parent._sha256_file(fills_path),
            },
        }
        parent._atomic_json(staging / "manifest.json", manifest)
        parent._atomic_text(
            staging / DAY_SUCCESS,
            parent._sha256_file(staging / "manifest.json") + "\n",
        )
        final.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staging, final)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return {"day": day, "reused": False}


def run(
    *,
    days: Sequence[str] | None,
    workers: int,
    output: Path = DEFAULT_OUTPUT,
    native_cache: Path = DEFAULT_NATIVE_CACHE,
    live_config: Path | None = None,
    window_cache: Path | None = None,
    feature_dir: Path | None = None,
    training_feature_manifest: Path | None = None,
    engine: str = "python",
    cpp_qualification_receipt: Path | None = None,
    cpp_qualification_receipt_sha256: str | None = None,
) -> list[dict[str, Any]]:
    backend = _backend_contract(
        engine,
        cpp_qualification_receipt=cpp_qualification_receipt,
        cpp_qualification_receipt_sha256=cpp_qualification_receipt_sha256,
    )
    engine = str(backend["engine"])
    if engine == "cpp" and live_config is None:
        raise StrictNativeLatencyError(
            "C++ qualification receipt is limited to the current-policy runner"
        )
    spec = parent._spec()
    selected = list(days) if days else parent.ordered_days(spec)
    if workers <= 0:
        raise StrictNativeLatencyError("workers must be positive")
    started = time.monotonic()
    results: list[dict[str, Any]] = []

    def progress(result: Mapping[str, Any]) -> None:
        results.append(dict(result))
        count = len(results)
        elapsed = time.monotonic() - started
        eta = elapsed / count * (len(selected) - count) if count else math.nan
        print(
            f"strict-native-latency completed={count}/{len(selected)} "
            f"day={result['day']} elapsed_s={elapsed:.1f} eta_s={eta:.1f}",
            flush=True,
        )

    if workers == 1:
        for day in selected:
            progress(
                execute_day(
                    day,
                    output=output,
                    native_cache=native_cache,
                    live_config=live_config,
                    window_cache=window_cache,
                    feature_dir=feature_dir,
                    training_feature_manifest=training_feature_manifest,
                    engine=engine,
                    cpp_qualification_receipt=cpp_qualification_receipt,
                    cpp_qualification_receipt_sha256=(
                        cpp_qualification_receipt_sha256
                    ),
                )
            )
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(
                    execute_day,
                    day,
                    output=output,
                    native_cache=native_cache,
                    live_config=live_config,
                    window_cache=window_cache,
                    feature_dir=feature_dir,
                    training_feature_manifest=training_feature_manifest,
                    engine=engine,
                    cpp_qualification_receipt=cpp_qualification_receipt,
                    cpp_qualification_receipt_sha256=(
                        cpp_qualification_receipt_sha256
                    ),
                ): day
                for day in selected
            }
            for future in concurrent.futures.as_completed(futures):
                progress(future.result())
    return sorted(results, key=lambda row: str(row["day"]))


def finalize(
    *,
    output: Path = DEFAULT_OUTPUT,
    live_config: Path | None = None,
    engine: str = "python",
    cpp_qualification_receipt: Path | None = None,
    cpp_qualification_receipt_sha256: str | None = None,
) -> dict[str, Any]:
    backend = _backend_contract(
        engine,
        cpp_qualification_receipt=cpp_qualification_receipt,
        cpp_qualification_receipt_sha256=cpp_qualification_receipt_sha256,
    )
    engine = str(backend["engine"])
    if engine == "cpp" and live_config is None:
        raise StrictNativeLatencyError(
            "C++ qualification receipt is limited to the current-policy runner"
        )
    spec = parent._spec()
    days = parent.ordered_days(spec)
    identity = CURRENT_IDENTITY if live_config is not None else IDENTITY
    config_sha256 = (
        parent._sha256_file(live_config.expanduser().resolve())
        if live_config is not None
        else None
    )
    summaries: list[dict[str, Any]] = []
    campaigns: list[pd.DataFrame] = []
    fills: list[pd.DataFrame] = []
    for day in days:
        manifest = _load_day(
            output,
            day,
            engine=engine,
            backend_identity_root=str(backend["backend_identity_root"]),
            identity=identity,
            config_sha256=config_sha256,
        )
        if manifest is None:
            raise StrictNativeLatencyError(f"strict-native day is missing: {day}")
        summary = parent._load_json(Path(manifest["summary"]["path"]), role=f"{day} summary")
        if summary.get("engine") != engine:
            raise StrictNativeLatencyError(f"{day} summary engine drifted")
        if summary.get("backend_identity_root") != backend["backend_identity_root"]:
            raise StrictNativeLatencyError(f"{day} summary backend identity drifted")
        if summary.get("backend_authoritative") is not True:
            raise StrictNativeLatencyError(f"{day} summary is not authoritative")
        if summary.get("qualification_under_test") is not False:
            raise StrictNativeLatencyError(f"{day} summary is qualification-under-test")
        if engine == "cpp":
            if summary.get("cooldown_duration_policy_cpp_authoritative") is not True:
                raise StrictNativeLatencyError(
                    f"{day} C++ current-policy summary is not authoritative"
                )
            if summary.get(
                "cooldown_duration_policy_cpp_parity_receipt_sha256"
            ) != backend["backend_identity_root"]:
                raise StrictNativeLatencyError(
                    f"{day} C++ qualification root drifted"
                )
            if summary.get(
                "cooldown_duration_policy_cpp_event_loop_parity_qualified"
            ) is not True:
                raise StrictNativeLatencyError(
                    f"{day} C++ event-loop authority is missing"
                )
        _validate_execution(summary)
        summaries.append(summary)
        campaigns.append(pd.read_parquet(manifest["campaigns"]["path"]))
        fills.append(pd.read_parquet(manifest["fills"]["path"]))
    daily = pd.DataFrame(summaries).sort_values("day").reset_index(drop=True)
    campaign_frame = pd.concat(campaigns, ignore_index=True)
    fill_frame = pd.concat(fills, ignore_index=True)
    metrics = parent._section_metrics(days, daily, campaign_frame)
    report = {
        "schema_version": f"{identity}.report.v1",
        "identity": identity,
        "engine": engine,
        "backend_identity_root": backend["backend_identity_root"],
        "backend_authoritative": bool(backend["authoritative"]),
        "qualification_under_test": bool(backend["qualification_under_test"]),
        "created_at_utc": datetime.now(tz=UTC).isoformat(),
        "status": "strict_native_latency_development_complete",
        "panel": {
            "days": len(days),
            "ordered_utc_days": days,
            "prefix40_days": days[:40],
            "added10_days": days[40:],
            "daily_fresh_start": True,
            "independent_confirmation": False,
        },
        "economics": metrics,
        "execution": {
            "exchange_book_queue_mode": "strict",
            "native_events_consumed": int(daily["exchange_book_events_consumed"].sum()),
            "queue_lookups": int(daily["exchange_book_queue_lookup_count"].sum()),
            "queue_missing": int(daily["exchange_book_queue_missing_count"].sum()),
            "new_order_latency_samples": int(
                daily["new_order_latency_sample_count"].min()
            ),
            "cancel_order_latency_samples": int(
                daily["cancel_order_latency_sample_count"].min()
            ),
            "visibility_samples": int(
                daily["exec_book_visibility_delay_sample_count"].min()
            ),
            "visibility_applied": int(
                daily["exec_book_visibility_delay_applied_count"].sum()
            ),
            "exact_historical_receive_time_authority": False,
        },
        "permissions": {
            "development_economics_read": True,
            "exact_live_transport_authority": False,
            "action_authority": False,
            "live_action_authority": False,
        },
    }
    output.mkdir(parents=True, exist_ok=True)
    daily_path = output / "daily.parquet"
    campaigns_path = output / "campaigns.parquet"
    fills_path = output / "fills.parquet"
    report_path = output / "report.json"
    daily.to_parquet(daily_path, index=False, compression="zstd")
    campaign_frame.to_parquet(campaigns_path, index=False, compression="zstd")
    fill_frame.to_parquet(fills_path, index=False, compression="zstd")
    parent._atomic_json(report_path, report)
    manifest = {
        "schema_version": f"{identity}.manifest.v1",
        "identity": identity,
        "engine": engine,
        "backend_identity_root": backend["backend_identity_root"],
        "backend_authoritative": bool(backend["authoritative"]),
        "qualification_under_test": bool(backend["qualification_under_test"]),
        "spec": {
            "path": SPEC_LOCATOR,
            "sha256": parent._sha256_file(_spec_path()),
        },
        "report": {"path": str(report_path), "sha256": parent._sha256_file(report_path)},
        "daily": {"path": str(daily_path), "sha256": parent._sha256_file(daily_path)},
        "campaigns": {
            "path": str(campaigns_path),
            "sha256": parent._sha256_file(campaigns_path),
        },
        "fills": {"path": str(fills_path), "sha256": parent._sha256_file(fills_path)},
        "runner": {
            "path": str(Path(__file__).resolve()),
            "sha256": parent._sha256_file(Path(__file__).resolve()),
        },
        "current_config_sha256": config_sha256,
    }
    manifest_path = output / "manifest.json"
    parent._atomic_json(manifest_path, manifest)
    parent._atomic_text(
        output / PANEL_SUCCESS,
        parent._sha256_file(manifest_path) + "\n",
    )
    return report


def status(
    *,
    output: Path = DEFAULT_OUTPUT,
    live_config: Path | None = None,
    engine: str = "python",
    cpp_qualification_receipt: Path | None = None,
    cpp_qualification_receipt_sha256: str | None = None,
) -> dict[str, Any]:
    backend = _backend_contract(
        engine,
        cpp_qualification_receipt=cpp_qualification_receipt,
        cpp_qualification_receipt_sha256=cpp_qualification_receipt_sha256,
    )
    engine = str(backend["engine"])
    if engine == "cpp" and live_config is None:
        raise StrictNativeLatencyError(
            "C++ qualification receipt is limited to the current-policy runner"
        )
    days = parent.ordered_days(parent._spec())
    identity = CURRENT_IDENTITY if live_config is not None else IDENTITY
    config_sha256 = (
        parent._sha256_file(live_config.expanduser().resolve())
        if live_config is not None
        else None
    )
    completed = [
        day
        for day in days
        if _load_day(
            output,
            day,
            engine=engine,
            backend_identity_root=str(backend["backend_identity_root"]),
            identity=identity,
            config_sha256=config_sha256,
        )
        is not None
    ]
    return {
        "identity": identity,
        "engine": engine,
        "backend_identity_root": backend["backend_identity_root"],
        "backend_authoritative": bool(backend["authoritative"]),
        "qualification_under_test": bool(backend["qualification_under_test"]),
        "completed": len(completed),
        "total": len(days),
        "remaining": len(days) - len(completed),
        "completed_days": completed,
    }


def _parser() -> argparse.ArgumentParser:
    def add_backend_args(command_parser: argparse.ArgumentParser) -> None:
        command_parser.add_argument("--engine", choices=ENGINES, default="python")
        command_parser.add_argument("--cpp-qualification-receipt", type=Path)
        command_parser.add_argument("--cpp-qualification-receipt-sha256")

    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    preflight_parser = sub.add_parser("preflight")
    preflight_parser.add_argument("--native-cache", type=Path, default=DEFAULT_NATIVE_CACHE)
    preflight_parser.add_argument("--live-config", type=Path)
    add_backend_args(preflight_parser)
    run_parser = sub.add_parser("run")
    run_parser.add_argument("--days", nargs="*")
    run_parser.add_argument("--workers", type=int, default=1)
    run_parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    run_parser.add_argument("--native-cache", type=Path, default=DEFAULT_NATIVE_CACHE)
    run_parser.add_argument("--window-cache", type=Path)
    run_parser.add_argument("--live-config", type=Path)
    run_parser.add_argument("--feature-dir", type=Path)
    run_parser.add_argument("--training-feature-manifest", type=Path)
    add_backend_args(run_parser)
    final_parser = sub.add_parser("finalize")
    final_parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    final_parser.add_argument("--live-config", type=Path)
    add_backend_args(final_parser)
    status_parser = sub.add_parser("status")
    status_parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    status_parser.add_argument("--live-config", type=Path)
    add_backend_args(status_parser)
    return parser


def main() -> int:
    args = _parser().parse_args()
    if (
        getattr(args, "live_config", None) is not None
        and getattr(args, "output", None) == DEFAULT_OUTPUT
    ):
        args.output = DEFAULT_CURRENT_OUTPUT
    if args.command == "preflight":
        payload = preflight(
            native_cache=args.native_cache,
            live_config=args.live_config,
            engine=args.engine,
            cpp_qualification_receipt=args.cpp_qualification_receipt,
            cpp_qualification_receipt_sha256=(
                args.cpp_qualification_receipt_sha256
            ),
        )
    elif args.command == "run":
        payload = run(
            days=args.days,
            workers=args.workers,
            output=args.output,
            native_cache=args.native_cache,
            live_config=args.live_config,
            window_cache=args.window_cache,
            feature_dir=args.feature_dir,
            training_feature_manifest=args.training_feature_manifest,
            engine=args.engine,
            cpp_qualification_receipt=args.cpp_qualification_receipt,
            cpp_qualification_receipt_sha256=(
                args.cpp_qualification_receipt_sha256
            ),
        )
    elif args.command == "finalize":
        payload = finalize(
            output=args.output,
            live_config=args.live_config,
            engine=args.engine,
            cpp_qualification_receipt=args.cpp_qualification_receipt,
            cpp_qualification_receipt_sha256=(
                args.cpp_qualification_receipt_sha256
            ),
        )
    elif args.command == "status":
        payload = status(
            output=args.output,
            live_config=args.live_config,
            engine=args.engine,
            cpp_qualification_receipt=args.cpp_qualification_receipt,
            cpp_qualification_receipt_sha256=(
                args.cpp_qualification_receipt_sha256
            ),
        )
    else:
        raise AssertionError(args.command)
    print(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
