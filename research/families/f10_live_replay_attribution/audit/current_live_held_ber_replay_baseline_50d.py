#!/usr/bin/env python3
"""Build the current live-held BER daily-fresh-start baseline on 50 native days."""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import math
import os
import shutil
import uuid
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from data_paths import data_root, external_cache_root, resolve_portable_path
from models import backtest_tick as bt
from models import data_windows
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_native_40day_full_path_ml_ab as native_runner,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_v9_10s_control_overlay_repair as control_overlay,
)
from research.families.f09_campaign_action_uplift.audit import (
    ber_guard_role_safe_add_only_current_stack_owner as ber_runner,
)

ROOT = Path(__file__).resolve().parents[4]
IDENTITY = "btc_usdc_current_live_held_ber_replay_baseline_50d_20260810"
EVIDENCE_LABEL = "native_derived_top20_100ms_cpp_daily_fresh_start_diagnostic"
SPEC = ROOT / (
    "research/families/f10_live_replay_attribution/docs/"
    "current_live_held_ber_replay_baseline_50d_spec_20260810.json"
)
DEFAULT_OUTPUT = (
    data_root(ROOT) / "reports/current_live_held_ber_baseline_50d_20260810"
)
DEFAULT_CACHE = (
    external_cache_root(ROOT) / "current_live_held_ber_baseline_50d_20260810"
)
OLD_SOURCE_ROOT = (
    data_root(ROOT)
    / "reports/ber_guard_role_safe_add_only_current_stack_owner_v1_20260808/"
    "development_execution_v2"
)
MODE_CPP = "cpp_screen"
MODE_PARITY = "python_cpp_parity"
DAY_SUCCESS = "_SUCCESS"
FINAL_SUCCESS = "_PANEL_SUCCESS"
ACCOUNTING_TOLERANCE = 1e-6
REPLAY_TOLERANCE = 1e-9


class Baseline50Error(RuntimeError):
    """Raised when the 50-day successor cannot preserve baseline identity."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
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


def _load_json(path: Path, *, role: str) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise Baseline50Error(f"missing {role}: {resolved}")
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Baseline50Error(f"invalid {role}: {resolved}") from exc
    if not isinstance(payload, dict):
        raise Baseline50Error(f"{role} must be a JSON object")
    return payload


def _validate_file(path: Path, expected: str, *, role: str) -> Path:
    resolved = path.expanduser().resolve()
    if not resolved.is_file() or _sha256_file(resolved) != expected:
        raise Baseline50Error(f"{role} SHA256 drift: {resolved}")
    return resolved


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


def _spec() -> dict[str, Any]:
    payload = _load_json(SPEC, role="50-day baseline spec")
    if payload.get("identity") != IDENTITY:
        raise Baseline50Error("50-day spec identity drifted")
    prefix = list(payload["immutable_prefix"]["ordered_utc_days"])
    added = list(payload["added_panel"]["ordered_utc_days"])
    if len(prefix) != 40 or len(added) != 10 or len(set(prefix + added)) != 50:
        raise Baseline50Error("50-day denominator is not 40 immutable plus 10 unique days")
    if prefix != sorted(prefix) or added != sorted(added) or max(prefix) >= min(added):
        raise Baseline50Error("50-day denominator is not chronological")
    if payload["combined_panel"]["days"] != 50:
        raise Baseline50Error("combined panel count drifted")
    return payload


def ordered_days(spec: Mapping[str, Any]) -> list[str]:
    return [
        *list(spec["immutable_prefix"]["ordered_utc_days"]),
        *list(spec["added_panel"]["ordered_utc_days"]),
    ]


def _resolve_repo_path(value: str) -> Path:
    path = resolve_portable_path(value, root=ROOT)
    return path.resolve() if path.is_absolute() else (ROOT / path).resolve()


def _prefix_plan(spec: Mapping[str, Any]) -> dict[str, Any]:
    sources = spec["sources"]
    path = _validate_file(
        _resolve_repo_path(str(sources["prefix_execution_plan_path"])),
        sources["prefix_execution_plan_sha256"],
        role="prefix execution plan",
    )
    payload = _load_json(path, role="prefix execution plan")
    rows = payload.get("identity_payload", {}).get("days", [])
    observed = [row.get("utc_day") for row in rows]
    if observed != list(spec["immutable_prefix"]["ordered_utc_days"]):
        raise Baseline50Error("prefix execution plan denominator drifted")
    return payload


def _added_window_rows(spec: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    sources = spec["sources"]
    path = _validate_file(
        _resolve_repo_path(str(sources["added_window_manifest_path"])),
        sources["added_window_manifest_sha256"],
        role="added-window manifest",
    )
    manifest = _load_json(path, role="added-window manifest")
    rows = {str(row["day"]): dict(row) for row in manifest.get("windows", [])}
    selected: dict[str, dict[str, Any]] = {}
    for day in spec["added_panel"]["ordered_utc_days"]:
        row = rows.get(day)
        if row is None:
            raise Baseline50Error(f"added-window manifest lacks {day}")
        if row.get("source_authority") != "native_formal_lifecycle" or not bool(
            row.get("formal_lifecycle_replay_eligible")
        ):
            raise Baseline50Error(f"{day} lacks native lifecycle authority")
        path = _resolve_repo_path(str(row["cache_path"]))
        if not path.is_file():
            raise Baseline50Error(f"missing added market cache: {path}")
        selected[day] = row | {"cache_path": str(path)}
    return selected


def _feature_receipts(spec: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Path]]:
    sources = spec["sources"]
    path = _validate_file(
        _resolve_repo_path(str(sources["feature_manifest_path"])),
        sources["feature_manifest_sha256"],
        role="v12 feature manifest",
    )
    manifest = _load_json(path, role="v12 feature manifest")
    expected = {
        "feature_semantics_version": 6,
        "feature_cutoff_semantics": "strict_exclusive_completed_bucket_end",
        "feature_timestamp_semantics": "left_label_bucket_end",
        "feature_ready_offset_ms": 10_000,
    }
    for key, value in expected.items():
        if manifest.get(key) != value:
            raise Baseline50Error(f"v12 feature semantics drifted: {key}")
    root = path.parent
    receipts: dict[str, Path] = {}
    for row in manifest.get("daily_files", []):
        day = str(row["day"])
        feature_path = root / str(row["file"])
        _validate_file(feature_path, str(row["sha256"]), role=f"{day} v12 features")
        receipts[day] = feature_path
    for day in spec["added_panel"]["ordered_utc_days"]:
        if day not in receipts:
            raise Baseline50Error(f"v12 feature manifest lacks target {day}")
    return manifest, receipts


def _boundary_path(cache_root: Path, prior_day: str) -> Path:
    return cache_root / "boundary_features" / f"features_{prior_day}.parquet"


def _materialize_boundary_feature(
    *,
    prior_day: str,
    target_day: str,
    feature_manifest: Mapping[str, Any],
    config_path: Path,
    output_path: Path,
) -> Path:
    from features import feature_engineer as fe

    expected_config = str(feature_manifest["config_sha256"])
    _validate_file(config_path, expected_config, role="feature-generation config")
    paths_by_day = {
        path.stem.replace("BTCUSDC-1s-", ""): path
        for path in fe.BARS_DIR.glob("BTCUSDC-1s-*.parquet")
    }
    prior_bar = paths_by_day.get(prior_day)
    target_bar = paths_by_day.get(target_day)
    if prior_bar is None or target_bar is None:
        raise Baseline50Error(f"{target_day} lacks D-1/target 1s bars")
    warmup = fe._contiguous_warmup_paths(
        prior_day,
        paths_by_day,
        warmup_days=int(feature_manifest["warmup_days_requested"]),
    )
    bars = pd.concat([*(pd.read_parquet(path) for path in warmup), pd.read_parquet(target_bar)])
    bars = bars.sort_index()
    bars = fe.filter_frame_for_orderbook_quality(bars, "BTCUSDC", label="1s bar")
    sample_weight = feature_manifest.get("sample_weight") or {}
    frame = fe.process_day(
        bars,
        prior_day,
        "BTCUSDC",
        config_path=config_path,
        market_stage=str(feature_manifest["market_stage"]),
        reference_symbol=str(feature_manifest["reference_symbol"]),
        calendar_tag=None,
        output_day_tag=prior_day,
        sample_weight_reference_date=sample_weight.get("reference_date"),
        sample_weight_lambda=float(sample_weight.get("lambda", 0.1)),
        require_execution_l2=bool(
            (feature_manifest.get("execution_l2_source") or {}).get("required")
        ),
        require_taker_tempo=bool(
            (feature_manifest.get("taker_tempo_source") or {}).get("required")
        ),
    )
    label = pd.Timestamp(prior_day, tz="UTC") + pd.Timedelta(hours=23, minutes=59, seconds=50)
    if label not in frame.index:
        raise Baseline50Error(f"{prior_day} boundary feature lacks 23:59:50")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.parent / f".{output_path.name}.{uuid.uuid4().hex}.partial"
    try:
        frame.loc[[label]].to_parquet(temporary, engine="pyarrow", compression="zstd")
        os.replace(temporary, output_path)
    finally:
        temporary.unlink(missing_ok=True)
    return output_path


def _overlay_directory(cache_root: Path, day: str) -> Path:
    return cache_root / "overlays" / day


def _write_overlay(
    *,
    cache_root: Path,
    day: str,
    prior_path: Path,
    target_path: Path,
    model_dir: Path,
) -> dict[str, Any]:
    final = _overlay_directory(cache_root, day)
    manifest_path = final / "manifest.json"
    marker = final / DAY_SUCCESS
    if manifest_path.is_file() and marker.is_file():
        manifest = _load_json(manifest_path, role=f"{day} overlay manifest")
        if marker.read_text(encoding="ascii").strip() != _sha256_file(manifest_path):
            raise Baseline50Error(f"{day} overlay marker drifted")
        _validate_file(
            final / "model_overlay.npz", manifest["overlay_sha256"], role=f"{day} overlay"
        )
        return manifest
    ml_data = control_overlay._generate_ml_data(
        prior_path,
        target_path,
        day=day,
        model_dir=model_dir,
    )
    ml_data = control_overlay._validate_ml_data(ml_data, day=day)
    arrays, layout = control_overlay._arrays_for_storage(ml_data)
    staging = final.parent / f".{day}.{uuid.uuid4().hex}.partial"
    staging.mkdir(parents=True, exist_ok=False)
    try:
        data_path = staging / "model_overlay.npz"
        with data_path.open("wb") as handle:
            np.savez_compressed(handle, **arrays)
            handle.flush()
            os.fsync(handle.fileno())
        manifest = {
            "schema_version": f"{IDENTITY}.v12_overlay.v1",
            "identity": IDENTITY,
            "utc_day": day,
            "prior_feature_path": str(prior_path.resolve()),
            "prior_feature_sha256": _sha256_file(prior_path),
            "target_feature_path": str(target_path.resolve()),
            "target_feature_sha256": _sha256_file(target_path),
            "model_dir": str(model_dir.resolve()),
            "model_bundle_meta_sha256": _sha256_file(model_dir / "bundle_meta.json"),
            "layout": layout,
            "overlay_sha256": _sha256_file(data_path),
            "old_window_ml_data_used": False,
            "economic_outcomes_read": False,
        }
        _atomic_json(staging / "manifest.json", manifest)
        _atomic_text(staging / DAY_SUCCESS, _sha256_file(staging / "manifest.json") + "\n")
        final.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staging, final)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return _load_json(final / "manifest.json", role=f"{day} overlay manifest")


def prepare(cache_root: Path = DEFAULT_CACHE) -> dict[str, Any]:
    spec = _spec()
    plan_path = cache_root / "execution-plan.json"
    marker_path = cache_root / "_PLAN_SUCCESS"
    if plan_path.exists() or marker_path.exists():
        if not plan_path.is_file() or not marker_path.is_file():
            raise Baseline50Error("50-day execution plan admission is incomplete")
        plan = _load_prepared_plan(cache_root)
        expected_days = ordered_days(spec)
        if plan.get("spec_sha256") != _sha256_file(SPEC):
            raise Baseline50Error("50-day execution plan spec binding drifted")
        if plan.get("ordered_utc_days") != expected_days:
            raise Baseline50Error("50-day execution plan day denominator drifted")
        if plan.get("prefix_days") != spec["immutable_prefix"]["ordered_utc_days"]:
            raise Baseline50Error("50-day execution plan prefix drifted")
        if plan.get("added_days") != spec["added_panel"]["ordered_utc_days"]:
            raise Baseline50Error("50-day execution plan added panel drifted")
        return plan

    _prefix_plan(spec)
    added_windows = _added_window_rows(spec)
    feature_manifest, features = _feature_receipts(spec)
    sources = spec["sources"]
    config_path = _resolve_repo_path(sources["feature_generation_config_path"])
    _validate_file(config_path, sources["feature_generation_config_sha256"], role="feature config")
    model_dir = _resolve_repo_path(sources["model_dir"])
    _validate_file(
        model_dir / "bundle_meta.json",
        sources["model_bundle_meta_sha256"],
        role="v12 model bundle",
    )
    overlays: list[dict[str, Any]] = []
    for day in spec["added_panel"]["ordered_utc_days"]:
        prior_day = (date.fromisoformat(day) - timedelta(days=1)).isoformat()
        prior_path = features.get(prior_day)
        if prior_path is None:
            prior_path = _boundary_path(cache_root, prior_day)
            if not prior_path.is_file():
                _materialize_boundary_feature(
                    prior_day=prior_day,
                    target_day=day,
                    feature_manifest=feature_manifest,
                    config_path=config_path,
                    output_path=prior_path,
                )
        overlays.append(
            _write_overlay(
                cache_root=cache_root,
                day=day,
                prior_path=prior_path,
                target_path=features[day],
                model_dir=model_dir,
            )
        )
    plan = {
        "schema_version": f"{IDENTITY}.execution_plan.v1",
        "identity": IDENTITY,
        "created_at_utc": datetime.now(tz=UTC).isoformat(),
        "spec_path": str(SPEC.resolve()),
        "spec_sha256": _sha256_file(SPEC),
        "ordered_utc_days": ordered_days(spec),
        "prefix_days": list(spec["immutable_prefix"]["ordered_utc_days"]),
        "added_days": list(spec["added_panel"]["ordered_utc_days"]),
        "added_windows": added_windows,
        "added_overlays": overlays,
        "old_v11_window_ml_data_used": False,
        "output_root": str(DEFAULT_OUTPUT),
        "cache_root": str(cache_root.resolve()),
    }
    plan["identity_sha256"] = _canonical_sha256(plan)
    cache_root.mkdir(parents=True, exist_ok=True)
    _atomic_json(cache_root / "execution-plan.json", plan)
    _atomic_text(
        cache_root / "_PLAN_SUCCESS", _sha256_file(cache_root / "execution-plan.json") + "\n"
    )
    return plan


def _load_prepared_plan(cache_root: Path) -> dict[str, Any]:
    path = cache_root / "execution-plan.json"
    marker = cache_root / "_PLAN_SUCCESS"
    plan = _load_json(path, role="50-day execution plan")
    expected = dict(plan)
    identity = expected.pop("identity_sha256", None)
    if identity != _canonical_sha256(expected):
        raise Baseline50Error("50-day execution plan identity drifted")
    if not marker.is_file() or marker.read_text(encoding="ascii").strip() != _sha256_file(path):
        raise Baseline50Error("50-day execution plan marker drifted")
    return plan


def _load_npz_overlay(cache_root: Path, day: str) -> tuple[Any, ...]:
    directory = _overlay_directory(cache_root, day)
    manifest = _load_json(directory / "manifest.json", role=f"{day} overlay")
    _validate_file(
        directory / "model_overlay.npz", manifest["overlay_sha256"], role=f"{day} overlay"
    )
    layout = manifest["layout"]
    with np.load(directory / "model_overlay.npz", allow_pickle=False) as arrays:
        values: list[Any] = [
            np.array(arrays[f"main_{index:03d}"], copy=True)
            for index in range(int(layout["main_count"]))
        ]
        values.append(
            {
                key: np.array(arrays[f"feature_{index:04d}"], copy=True)
                for index, key in enumerate(layout["feature_keys"])
            }
        )
    return control_overlay._validate_ml_data(tuple(values), day=day)


def _load_day_inputs(
    day: str,
    *,
    spec: Mapping[str, Any],
    prefix_plan: Mapping[str, Any],
    prepared_plan: Mapping[str, Any],
    cache_root: Path,
) -> tuple[Any, tuple[Any, ...], dict[str, Any]]:
    prefix_days = set(spec["immutable_prefix"]["ordered_utc_days"])
    if day in prefix_days:
        payload = prefix_plan["identity_payload"]
        row = {item["utc_day"]: item for item in payload["days"]}[day]
        schedule = control_overlay.load_admitted_control_schedule(
            Path(payload["control_sources"]["path"]),
            panel_sha256=payload["control_sources"]["sha256"],
            panel_identity_sha256=payload["control_sources"]["panel_identity_sha256"],
            day=day,
        )
        window_path = Path(row["window"]["path"])
        _validate_file(window_path, row["window"]["sha256"], role=f"{day} prefix window")
        window = native_runner._load_bound_window(window_path)
        binding = {
            "window_path": str(window_path),
            "window_sha256": row["window"]["sha256"],
            "overlay_manifest_sha256": schedule.manifest_sha256,
            "market_cache_ml_detached": False,
        }
        return window, schedule.ml_data, binding

    row = prepared_plan["added_windows"][day]
    window_path = Path(row["cache_path"])
    window_sha = _sha256_file(window_path)
    window = data_windows._load_cached_window(window_path)
    if window is None:
        raise Baseline50Error(f"{day} added market cache is incompatible")
    if getattr(window, "book_source_authority", None) != "native_formal_lifecycle":
        raise Baseline50Error(f"{day} added market cache lost native authority")
    if getattr(window, "ml_data", None) is None:
        raise Baseline50Error(f"{day} expected the legacy v11 overlay to detach explicitly")
    window.ml_data = None
    if hasattr(window, "ml_cache"):
        window.ml_cache = {}
    ml_data = _load_npz_overlay(cache_root, day)
    overlay_manifest = _overlay_directory(cache_root, day) / "manifest.json"
    binding = {
        "window_path": str(window_path),
        "window_sha256": window_sha,
        "overlay_manifest_sha256": _sha256_file(overlay_manifest),
        "market_cache_ml_detached": True,
    }
    return window, ml_data, binding


def _base_params(spec: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    config = _resolve_repo_path(spec["baseline"]["config_path"])
    _validate_file(config, spec["baseline"]["config_sha256"], role="operational config")
    params, audit = ber_runner._load_offline_params(config)
    params = dict(params)
    params.update(
        {
            "ber_guard_thresh": 1.2,
            "ber_spread_mult": 2.0,
            "ber_exposure_add_only": False,
            "dynamic_fill_hazard_action_enabled": False,
            "buy_fill_selection_live_enabled": False,
            # This frozen runner reproduces the historical C++ denominator.
            # Keep its missing raw-queue and latency inputs explicit rather
            # than inheriting silent defaults.
            "exchange_book_queue_mode": "disabled",
            "new_order_latency_ms": 0.0,
            "cancel_order_latency_ms": 0.0,
            "latency_jitter_ms": 0.0,
            "exec_book_visibility_delay_mean_ms": 0.0,
            "exec_book_visibility_delay_jitter_ms": 0.0,
        }
    )
    return params, audit


def _cpp_only(
    *,
    day: str,
    window: Any,
    ml_data: tuple[Any, ...],
    base: Mapping[str, Any],
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    result = bt._simulate_tick_with_engine(
        "cpp",
        window.trades,
        window.var_ts_ms,
        window.var_ssq,
        dict(base),
        ml_data=ml_data,
        bbo_data=window.bbo_data,
        l2_data=window.l2_data,
        var_ti=window.var_ti,
        var_retsq=window.var_retsq,
    )
    summary, campaigns, fills = native_runner._project_arm(
        day=day,
        arm="current_live_held_global_ber_control",
        result=result,
        order_size=float(base["order_size"]),
        campaign_mae_trace_max=1_000_000,
    )
    summary.update(
        {
            "engine_evidence": MODE_CPP,
            "python_cpp_fill_path_mismatch_count": None,
            "python_cpp_ber_state_mismatch_count": None,
            "exchange_book_queue_mode": str(
                result.get("exchange_book_queue_mode", "disabled") or "disabled"
            ),
            "exchange_book_queue_scope": str(
                result.get("exchange_book_queue_scope", "disabled") or "disabled"
            ),
            "exchange_book_queue_lookup_count": int(
                result.get("exchange_book_queue_lookup_count", 0) or 0
            ),
            "exchange_book_events_consumed": int(
                result.get("exchange_book_events_consumed", 0) or 0
            ),
            "new_order_latency_ms": float(result.get("new_order_latency_ms", 0.0) or 0.0),
            "cancel_order_latency_ms": float(
                result.get("cancel_order_latency_ms", 0.0) or 0.0
            ),
            "new_order_latency_sample_count": int(
                result.get("new_order_latency_sample_count", 0) or 0
            ),
            "cancel_order_latency_sample_count": int(
                result.get("cancel_order_latency_sample_count", 0) or 0
            ),
            "exec_book_visibility_delay_enabled": bool(
                result.get("exec_book_visibility_delay_enabled", False)
            ),
            "exec_book_visibility_delay_sample_count": int(
                result.get("exec_book_visibility_delay_sample_count", 0) or 0
            ),
        }
    )
    return summary, campaigns, fills


def _parity(
    *,
    day: str,
    window: Any,
    ml_data: tuple[Any, ...],
    base: Mapping[str, Any],
    audit: Mapping[str, Any],
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    shared = {
        "ml_data": ml_data,
        "bbo_data": window.bbo_data,
        "l2_data": window.l2_data,
        "var_ti": window.var_ti,
        "var_retsq": window.var_retsq,
    }
    summary, campaigns, fills = ber_runner._simulate_arm(
        day=day,
        arm=ber_runner.ARMS[0],
        window=window,
        base=base,
        shared=shared,
        expected_trace=int(base["trace_campaign_repair_max"]),
        projection_audit=audit,
    )
    summary.update(
        {
            "engine_evidence": MODE_PARITY,
            "python_cpp_fill_path_mismatch_count": int(
                summary["campaign_mae_cpp_python_fill_path_mismatch_count"]
            ),
            "python_cpp_ber_state_mismatch_count": int(
                summary["cpp_python_ber_state_mismatch_count"]
            ),
            "exchange_book_queue_mode": str(
                summary.get("exchange_book_queue_mode", "disabled") or "disabled"
            ),
            "exchange_book_queue_scope": str(
                summary.get("exchange_book_queue_scope", "disabled") or "disabled"
            ),
            "exchange_book_queue_lookup_count": int(
                summary.get("exchange_book_queue_lookup_count", 0) or 0
            ),
            "exchange_book_events_consumed": int(
                summary.get("exchange_book_events_consumed", 0) or 0
            ),
            "new_order_latency_ms": float(summary.get("new_order_latency_ms", 0.0) or 0.0),
            "cancel_order_latency_ms": float(
                summary.get("cancel_order_latency_ms", 0.0) or 0.0
            ),
            "new_order_latency_sample_count": int(
                summary.get("new_order_latency_sample_count", 0) or 0
            ),
            "cancel_order_latency_sample_count": int(
                summary.get("cancel_order_latency_sample_count", 0) or 0
            ),
            "exec_book_visibility_delay_enabled": bool(
                summary.get("exec_book_visibility_delay_enabled", False)
            ),
            "exec_book_visibility_delay_sample_count": int(
                summary.get("exec_book_visibility_delay_sample_count", 0) or 0
            ),
        }
    )
    return summary, campaigns, fills


def _day_directory(output: Path, mode: str, day: str) -> Path:
    return output / mode / "days" / day


def _load_admitted_day(output: Path, mode: str, day: str) -> dict[str, Any] | None:
    directory = _day_directory(output, mode, day)
    manifest_path = directory / "manifest.json"
    marker = directory / DAY_SUCCESS
    if not manifest_path.is_file() or not marker.is_file():
        return None
    if marker.read_text(encoding="ascii").strip() != _sha256_file(manifest_path):
        raise Baseline50Error(f"{mode} {day} day marker drifted")
    manifest = _load_json(manifest_path, role=f"{mode} {day} manifest")
    for name in ("summary", "campaigns", "fills"):
        row = manifest[name]
        _validate_file(Path(row["path"]), row["sha256"], role=f"{mode} {day} {name}")
    return manifest


def execute_day(
    day: str,
    *,
    mode: str,
    cache_root: Path = DEFAULT_CACHE,
    output: Path = DEFAULT_OUTPUT,
) -> dict[str, Any]:
    existing = _load_admitted_day(output, mode, day)
    if existing is not None:
        return {"day": day, "mode": mode, "reused": True}
    spec = _spec()
    if day not in ordered_days(spec):
        raise Baseline50Error(f"day is outside the frozen 50-day panel: {day}")
    prefix = _prefix_plan(spec)
    prepared = _load_prepared_plan(cache_root)
    window, ml_data, binding = _load_day_inputs(
        day,
        spec=spec,
        prefix_plan=prefix,
        prepared_plan=prepared,
        cache_root=cache_root,
    )
    base, audit = _base_params(spec)
    if mode == MODE_CPP:
        summary, campaigns, fills = _cpp_only(day=day, window=window, ml_data=ml_data, base=base)
    elif mode == MODE_PARITY:
        summary, campaigns, fills = _parity(
            day=day,
            window=window,
            ml_data=ml_data,
            base=base,
            audit=audit,
        )
    else:
        raise Baseline50Error(f"unsupported mode: {mode}")
    final = _day_directory(output, mode, day)
    staging = final.parent / f".{day}.{uuid.uuid4().hex}.partial"
    staging.mkdir(parents=True, exist_ok=False)
    try:
        summary_path = staging / "summary.json"
        campaigns_path = staging / "campaigns.parquet"
        fills_path = staging / "fills.parquet"
        _atomic_json(summary_path, summary)
        campaigns.to_parquet(campaigns_path, index=False, compression="zstd")
        fills.to_parquet(fills_path, index=False, compression="zstd")
        manifest = {
            "schema_version": f"{IDENTITY}.day.v1",
            "identity": IDENTITY,
            "mode": mode,
            "day": day,
            "spec_sha256": _sha256_file(SPEC),
            "input_binding": binding,
            "summary": {
                "path": str(final / "summary.json"),
                "sha256": _sha256_file(summary_path),
            },
            "campaigns": {
                "path": str(final / "campaigns.parquet"),
                "sha256": _sha256_file(campaigns_path),
            },
            "fills": {
                "path": str(final / "fills.parquet"),
                "sha256": _sha256_file(fills_path),
            },
        }
        _atomic_json(staging / "manifest.json", manifest)
        _atomic_text(staging / DAY_SUCCESS, _sha256_file(staging / "manifest.json") + "\n")
        final.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staging, final)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return {"day": day, "mode": mode, "reused": False}


def run(
    *,
    mode: str,
    days: Sequence[str] | None,
    workers: int,
    cache_root: Path = DEFAULT_CACHE,
    output: Path = DEFAULT_OUTPUT,
) -> list[dict[str, Any]]:
    spec = _spec()
    selected = list(days) if days else ordered_days(spec)
    if workers <= 0:
        raise Baseline50Error("workers must be positive")
    prepare(cache_root)
    if workers == 1:
        return [
            execute_day(day, mode=mode, cache_root=cache_root, output=output) for day in selected
        ]
    results: list[dict[str, Any]] = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                execute_day,
                day,
                mode=mode,
                cache_root=cache_root,
                output=output,
            ): day
            for day in selected
        }
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            results.append(result)
            print(f"baseline50 {mode} complete {result['day']}", flush=True)
    return sorted(results, key=lambda row: row["day"])


def _old_prefix_expected(day: str) -> dict[str, Any]:
    payload = _load_json(
        OLD_SOURCE_ROOT / "days" / day / "summary.json", role=f"{day} old baseline"
    )
    arms = list(payload.get("arms") or [])
    if not arms:
        raise Baseline50Error(f"{day} old baseline lacks control arm")
    return dict(arms[0])


def _section_metrics(
    days: Sequence[str], daily: pd.DataFrame, campaigns: pd.DataFrame
) -> dict[str, Any]:
    selected_daily = daily[daily["day"].isin(days)].copy()
    selected_campaigns = campaigns[campaigns["day"].isin(days)].copy()
    terminal = float(selected_daily["terminal_mtm_pnl_usdc"].sum())
    closed = float(selected_daily["closed_campaign_value_usdc"].sum())
    values = selected_campaigns["terminal_value_usdc"].to_numpy(dtype=float)
    q10 = float(np.quantile(values, 0.1)) if len(values) else 0.0
    cvar = float(values[values <= q10].mean()) if len(values) else 0.0
    multi = selected_campaigns[selected_campaigns["multi_level"].astype(bool)]
    return {
        "days": len(days),
        "terminal_mtm_pnl_usdc": terminal,
        "terminal_mtm_pnl_usdc_per_day": terminal / len(days),
        "closed_campaign_value_usdc": closed,
        "fills_bid": int(selected_daily["fills_bid"].sum()),
        "fills_ask": int(selected_daily["fills_ask"].sum()),
        "fills_total": int(selected_daily["fills_total"].sum()),
        "positive_pnl_days": int((selected_daily["terminal_mtm_pnl_usdc"] > 0.0).sum()),
        "campaign_q10_usdc": q10,
        "campaign_cvar10_usdc": cvar,
        "max_inventory_btc": float(selected_daily["max_inventory_btc"].max()),
        "abs_inventory_time_btc_s": float(selected_daily["abs_inventory_time_btc_s"].sum()),
        "multi_level_long_terminal_value_usdc": float(
            multi.loc[multi["inventory_side"].eq("LONG"), "terminal_value_usdc"].sum()
        ),
        "multi_level_short_terminal_value_usdc": float(
            multi.loc[multi["inventory_side"].eq("SHORT"), "terminal_value_usdc"].sum()
        ),
    }


def finalize(
    *,
    cache_root: Path = DEFAULT_CACHE,
    output: Path = DEFAULT_OUTPUT,
) -> dict[str, Any]:
    spec = _spec()
    days = ordered_days(spec)
    prefix_days = list(spec["immutable_prefix"]["ordered_utc_days"])
    added_days = list(spec["added_panel"]["ordered_utc_days"])
    manifests = [_load_admitted_day(output, MODE_PARITY, day) for day in days]
    if any(manifest is None for manifest in manifests):
        missing = [day for day, row in zip(days, manifests, strict=True) if row is None]
        raise Baseline50Error(f"cannot finalize; parity days are missing: {missing}")
    summaries: list[dict[str, Any]] = []
    campaign_frames: list[pd.DataFrame] = []
    fill_frames: list[pd.DataFrame] = []
    prefix_daily_mismatches: list[dict[str, Any]] = []
    for day, manifest in zip(days, manifests, strict=True):
        assert manifest is not None
        summary = _load_json(Path(manifest["summary"]["path"]), role=f"{day} summary")
        summaries.append(summary)
        campaign_frames.append(pd.read_parquet(manifest["campaigns"]["path"]))
        fill_frames.append(pd.read_parquet(manifest["fills"]["path"]))
        if day in prefix_days:
            old = _old_prefix_expected(day)
            mismatch = {
                "day": day,
                "terminal_mtm_delta_usdc": float(summary["terminal_mtm_pnl_usdc"])
                - float(old["terminal_mtm_pnl_usdc"]),
                "closed_campaign_delta_usdc": float(summary["closed_campaign_value_usdc"])
                - float(old["closed_campaign_value_usdc"]),
                "fill_delta": int(summary["fills_total"]) - int(old["fills_total"]),
            }
            if (
                abs(mismatch["terminal_mtm_delta_usdc"]) > REPLAY_TOLERANCE
                or abs(mismatch["closed_campaign_delta_usdc"]) > REPLAY_TOLERANCE
                or mismatch["fill_delta"] != 0
            ):
                prefix_daily_mismatches.append(mismatch)
    daily = pd.DataFrame(summaries).sort_values("day").reset_index(drop=True)
    campaigns = pd.concat(campaign_frames, ignore_index=True)
    fills = pd.concat(fill_frames, ignore_index=True)
    if prefix_daily_mismatches:
        raise Baseline50Error(
            f"current code failed immutable prefix parity on {len(prefix_daily_mismatches)} days"
        )
    prefix = _section_metrics(prefix_days, daily, campaigns)
    added = _section_metrics(added_days, daily, campaigns)
    pooled = _section_metrics(days, daily, campaigns)
    frozen = spec["immutable_prefix"]
    if (
        not math.isclose(
            prefix["terminal_mtm_pnl_usdc"],
            float(frozen["terminal_mtm_pnl_usdc"]),
            rel_tol=0.0,
            abs_tol=REPLAY_TOLERANCE,
        )
        or not math.isclose(
            prefix["closed_campaign_value_usdc"],
            float(frozen["closed_campaign_value_usdc"]),
            rel_tol=0.0,
            abs_tol=REPLAY_TOLERANCE,
        )
        or prefix["fills_total"] != int(frozen["fills_total"])
    ):
        raise Baseline50Error("immutable 40-day aggregate failed exact reproduction")
    mismatch_fill = int(daily["python_cpp_fill_path_mismatch_count"].sum())
    mismatch_ber = int(daily["python_cpp_ber_state_mismatch_count"].sum())
    if mismatch_fill != 0 or mismatch_ber != 0:
        raise Baseline50Error("50-day Python/C++ parity failed")
    report = {
        "schema_version": f"{IDENTITY}.report.v1",
        "baseline_id": IDENTITY,
        "created_at_utc": datetime.now(tz=UTC).isoformat(),
        "status": "active_native_derived_top20_100ms_daily_fresh_start_diagnostic_control",
        "panel": {
            "days": 50,
            "ordered_utc_days": days,
            "grade_a_days": 34,
            "grade_b_days": 16,
            "historical_panels_previously_read": True,
            "independent_confirmation": False,
            "daily_fresh_start": True,
            "continuous_live_pnl_claimed": False,
        },
        "economics": {
            "prefix_40": prefix,
            "added_10": added,
            "pooled_50": pooled,
        },
        "parity": {
            "prefix_daily_mismatch_count": 0,
            "python_cpp_fill_path_mismatch_count": mismatch_fill,
            "python_cpp_ber_state_mismatch_count": mismatch_ber,
        },
        "execution_scope": {
            "evidence_label": EVIDENCE_LABEL,
            "exchange_book_queue_mode": "disabled",
            "raw_snapshot_delta_tape_used": False,
            "new_cancel_latency_calibration_used": False,
            "exec_book_visibility_latency_used": False,
            "strict_native_latency_successor_required": True,
        },
        "permissions": {
            "backtest_default_control_authorized": True,
            "backtest_default_control_scope": "common_simulator_diagnostic_only",
            "strict_native_queue_authority": False,
            "live_transport_authority": False,
            "order_path_pnl_authority": False,
            "independent_confirmation": False,
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
    campaigns.to_parquet(campaigns_path, index=False, compression="zstd")
    fills.to_parquet(fills_path, index=False, compression="zstd")
    _atomic_json(report_path, report)
    manifest = {
        "schema_version": f"{IDENTITY}.manifest.v1",
        "identity": IDENTITY,
        "spec": {"path": str(SPEC), "sha256": _sha256_file(SPEC)},
        "execution_plan": {
            "path": str(cache_root / "execution-plan.json"),
            "sha256": _sha256_file(cache_root / "execution-plan.json"),
        },
        "report": {"path": str(report_path), "sha256": _sha256_file(report_path)},
        "daily": {"path": str(daily_path), "sha256": _sha256_file(daily_path)},
        "campaigns": {"path": str(campaigns_path), "sha256": _sha256_file(campaigns_path)},
        "fills": {"path": str(fills_path), "sha256": _sha256_file(fills_path)},
        "implementation": {
            "runner_path": str(Path(__file__).resolve()),
            "runner_sha256": _sha256_file(Path(__file__).resolve()),
            "backtest_tick_sha256": _sha256_file(ROOT / "models/backtest_tick.py"),
            "tick_replay_cpp_sha256": _sha256_file(ROOT / "cpp/narrowgate_cpp/tick_replay.cpp"),
            "tick_replay_hpp_sha256": _sha256_file(ROOT / "cpp/narrowgate_cpp/tick_replay.hpp"),
        },
    }
    manifest_path = output / "manifest.json"
    _atomic_json(manifest_path, manifest)
    _atomic_text(output / FINAL_SUCCESS, _sha256_file(manifest_path) + "\n")
    return report | {"report_path": str(report_path), "manifest_path": str(manifest_path)}


def status(
    *,
    output: Path = DEFAULT_OUTPUT,
) -> dict[str, Any]:
    spec = _spec()
    days = ordered_days(spec)
    return {
        "identity": IDENTITY,
        "total_days": len(days),
        "cpp_screen_complete": sum(
            _load_admitted_day(output, MODE_CPP, day) is not None for day in days
        ),
        "python_cpp_parity_complete": sum(
            _load_admitted_day(output, MODE_PARITY, day) is not None for day in days
        ),
        "ema_oof_progress_is_external": True,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("preflight")
    prepare_parser = sub.add_parser("prepare")
    prepare_parser.add_argument("--cache-root", type=Path, default=DEFAULT_CACHE)
    run_parser = sub.add_parser("run")
    run_parser.add_argument("--mode", choices=(MODE_CPP, MODE_PARITY), default=MODE_PARITY)
    run_parser.add_argument("--days", nargs="*")
    run_parser.add_argument("--workers", type=int, default=1)
    run_parser.add_argument("--cache-root", type=Path, default=DEFAULT_CACHE)
    run_parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    final_parser = sub.add_parser("finalize")
    final_parser.add_argument("--cache-root", type=Path, default=DEFAULT_CACHE)
    final_parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    status_parser = sub.add_parser("status")
    status_parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser


def main() -> int:
    args = _parser().parse_args()
    if args.command == "preflight":
        spec = _spec()
        _prefix_plan(spec)
        _added_window_rows(spec)
        _feature_receipts(spec)
        payload = {
            "identity": IDENTITY,
            "days": len(ordered_days(spec)),
            "prefix_days": 40,
            "added_days": 10,
            "passed": True,
        }
    elif args.command == "prepare":
        payload = prepare(args.cache_root)
    elif args.command == "run":
        payload = run(
            mode=args.mode,
            days=args.days,
            workers=args.workers,
            cache_root=args.cache_root,
            output=args.output,
        )
    elif args.command == "finalize":
        payload = finalize(cache_root=args.cache_root, output=args.output)
    elif args.command == "status":
        payload = status(output=args.output)
    else:
        raise AssertionError(args.command)
    print(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
