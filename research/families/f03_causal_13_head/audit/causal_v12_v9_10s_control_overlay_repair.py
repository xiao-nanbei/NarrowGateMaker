#!/usr/bin/env python3
"""Outcome-blind repair and admission for the 40-day v9 10s control panel."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import shutil
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from data_paths import data_root, external_cache_root
from models import backtest_tick as bt
from models import data_windows
from models.backtest_config import load_operational_baseline_binding
from models.replay_cache_components import references_sha256
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_dual_overlay_ml_ab_replay as dual_abi,
)

ROOT = Path(__file__).resolve().parents[4]
IDENTITY = "causal_v12_v9_10s_control_overlay_repair_admission_v1_1"
SCHEMA_VERSION = f"{IDENTITY}.component"
PLAN_SCHEMA_VERSION = f"{IDENTITY}.plan"
PANEL_SCHEMA_VERSION = f"{IDENTITY}.panel"
GRID_SEMANTICS = "left_label_10s_bucket_visible_at_bucket_end"
CADENCE_MS = 10_000
ROWS_PER_DAY = 8_640
MAIN_ARRAY_COUNT = 6 + len(bt.XMARKET_REPLAY_FEATURE_COLUMNS)
EXPECTED_TUPLE_LENGTH = MAIN_ARRAY_COUNT + 1
EXPECTED_HEADS = (
    "dir_10s",
    "ret_10s",
    "vol_10s",
    "dir_30s",
    "ret_30s",
    "vol_30s",
    "dir_60s",
    "ret_60s",
    "vol_60s",
    "tox_bid_5s",
    "tox_ask_5s",
    "tox_bid_10s",
    "tox_ask_10s",
)
POLICY_HEADS = ("dir_10s", "vol_10s", "ret_10s", "tox_bid_10s", "tox_ask_10s")
LABEL_COLUMNS = frozenset(f"label_{head}" for head in EXPECTED_HEADS)
DROP_COLUMNS = frozenset(("open", "high", "low", "vwap", "sample_weight"))
PLAN_FILENAME = "execution-plan.json"
PLAN_SUCCESS = "_PLAN_SUCCESS"
PANEL_FILENAME = "panel-manifest.json"
PANEL_SUCCESS = "_PANEL_SUCCESS"
COMPONENT_SUCCESS = "_SUCCESS"
DEFAULT_SOURCE_PLAN = external_cache_root(ROOT) / (
    "replay_dag/"
    "f07_order_lifecycle_v2_40day_v1_4/execution_plan.json"
)
DEFAULT_TARGET_FEATURE_MANIFEST = data_root(ROOT) / (
    "features_btcusdc_causal_v12_ranked_toxicity_f09_40d_20260802/"
    "causal_feature_manifest.json"
)
DEFAULT_WARMUP_FEATURE_MANIFEST = external_cache_root(ROOT) / (
    "f03_v9_10s_control_overlay_repair_v1/repair_feature_boundary_v1/"
    "repair_feature_boundary_manifest.json"
)
DEFAULT_OUTPUT_ROOT = external_cache_root(ROOT) / (
    "f03_v9_10s_control_overlay_repair_v1/control_overlay_panel_admission_v1_1"
)
DEFAULT_PRECOMMIT = ROOT / (
    "research/families/f03_causal_13_head/docs/"
    "causal_v12_1s_cadence_full_path_economic_precommit_v1_20260805.json"
)
DEFAULT_BOUNDARY_FEATURE_ROOT = DEFAULT_WARMUP_FEATURE_MANIFEST.parent


class ControlOverlayRepairError(ValueError):
    """Raised when a control component cannot be admitted causally."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _load_json(path: Path, *, role: str) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise ControlOverlayRepairError(f"missing {role}: {resolved}")
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ControlOverlayRepairError(f"invalid {role}: {resolved}") from exc
    if not isinstance(payload, dict):
        raise ControlOverlayRepairError(f"{role} must be a JSON object")
    return payload


def _artifact(path_value: Any, sha_value: Any | None = None, *, role: str) -> dict[str, Any]:
    path = Path(str(path_value)).expanduser().resolve()
    if not path.is_file():
        raise ControlOverlayRepairError(f"missing {role}: {path}")
    observed = _sha256_file(path)
    if sha_value is not None and observed != str(sha_value):
        raise ControlOverlayRepairError(f"{role} SHA256 drift")
    return {"path": str(path), "sha256": observed, "size_bytes": path.stat().st_size}


def _fsync_file(path: Path) -> None:
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_json_fsync(path: Path, payload: Any) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def _write_text_fsync(path: Path, value: str) -> None:
    with path.open("w", encoding="ascii") as handle:
        handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.partial"
    try:
        _write_json_fsync(temporary, payload)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.partial"
    try:
        frame.to_parquet(temporary, engine="pyarrow")
        _fsync_file(temporary)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def canonical_visibility_grid(utc_day: str) -> np.ndarray:
    try:
        day_start = int(np.datetime64(utc_day, "ms").astype(np.int64))
    except ValueError as exc:
        raise ControlOverlayRepairError(f"invalid UTC day: {utc_day}") from exc
    return day_start + np.arange(ROWS_PER_DAY, dtype=np.int64) * CADENCE_MS


def canonical_feature_labels(utc_day: str) -> np.ndarray:
    """Return source bucket labels for the canonical visibility grid."""

    return canonical_visibility_grid(utc_day) - CADENCE_MS


def _daily_feature_receipts(
    manifest_path: Path,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    manifest = _load_json(manifest_path, role="feature manifest")
    root = manifest_path.resolve().parent
    rows = manifest.get("daily_files")
    if not isinstance(rows, list):
        raise ControlOverlayRepairError("feature manifest lacks daily receipts")
    by_day: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise ControlOverlayRepairError("feature manifest has invalid daily receipt")
        day = str(row.get("day", ""))
        receipt = _artifact(
            root / str(row.get("file", "")), row.get("sha256"), role=f"{day} features"
        )
        if int(row.get("size_bytes", -1)) != receipt["size_bytes"]:
            raise ControlOverlayRepairError(f"{day} feature size drift")
        by_day[day] = receipt
    return manifest, by_day


def _feature_semantics(manifest: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "generator_sha256": manifest.get("generator_sha256"),
        "config_sha256": manifest.get("config_sha256"),
        "feature_semantics_version": manifest.get("feature_semantics_version"),
        "feature_dag_id": manifest.get("feature_dag_id"),
        "feature_dag_sha256": manifest.get("feature_dag_sha256"),
        "feature_cutoff_semantics": manifest.get("feature_cutoff_semantics"),
        "feature_timestamp_semantics": manifest.get("feature_timestamp_semantics"),
        "feature_ready_offset_ms": manifest.get("feature_ready_offset_ms"),
        "market_stage": manifest.get("market_stage"),
    }


def _validate_feature_semantics(
    target_manifest: Mapping[str, Any], warmup_manifest: Mapping[str, Any]
) -> dict[str, Any]:
    target = _feature_semantics(target_manifest)
    warmup = _feature_semantics(warmup_manifest)
    if target != warmup:
        raise ControlOverlayRepairError("target and D-1 feature semantics differ")
    if (
        target.get("feature_semantics_version") != 6
        or target.get("feature_ready_offset_ms") != CADENCE_MS
        or target.get("feature_timestamp_semantics") != "left_label_bucket_end"
        or target.get("feature_cutoff_semantics") != "strict_exclusive_completed_bucket_end"
    ):
        raise ControlOverlayRepairError("repair features do not implement v9 causal 10s semantics")
    return target


def build_boundary_feature_manifest(
    *,
    source_plan: Path = DEFAULT_SOURCE_PLAN,
    target_feature_manifest: Path = DEFAULT_TARGET_FEATURE_MANIFEST,
    precommit_path: Path = DEFAULT_PRECOMMIT,
    output_root: Path = DEFAULT_BOUNDARY_FEATURE_ROOT,
) -> dict[str, Any]:
    """Materialize D-1 terminal feature buckets using target-day clock closure."""

    from features import feature_engineer as fe

    output = output_root.expanduser().resolve()
    required_root = external_cache_root(ROOT).resolve()
    try:
        output.relative_to(required_root)
    except ValueError as exc:
        raise ControlOverlayRepairError(
            "boundary features must live in the configured external cache"
        ) from exc
    precommit = _load_json(precommit_path, role="frozen F03 precommit")
    days = list((precommit.get("native_development_panel") or {}).get("days") or ())
    source = _load_json(source_plan, role="frozen control source plan")
    rows = _source_rows(source, days)
    model_identity = _model_bundle_binding(source)["content_identity_sha256"]
    invalid_days = [
        day
        for day in days
        if not _old_overlay_is_reference_eligible(
            rows[day]["model_overlay"], day=day, model_identity=model_identity
        )
    ]
    target_manifest = _load_json(target_feature_manifest, role="target feature manifest")
    sample_weight = target_manifest.get("sample_weight") or {}
    paths_by_day = {
        path.stem.replace("BTCUSDC-1s-", ""): path
        for path in fe.BARS_DIR.glob("BTCUSDC-1s-*.parquet")
    }
    output.mkdir(parents=True, exist_ok=True)
    receipts: list[dict[str, Any]] = []
    seen_prior: set[str] = set()
    for target_day in invalid_days:
        prior_day = (date.fromisoformat(target_day) - timedelta(days=1)).isoformat()
        if prior_day in seen_prior:
            continue
        seen_prior.add(prior_day)
        target_bar = paths_by_day.get(target_day)
        prior_bar = paths_by_day.get(prior_day)
        if target_bar is None or prior_bar is None:
            raise ControlOverlayRepairError(f"{target_day} lacks D-1/target 1s bars")
        warmup_paths = fe._contiguous_warmup_paths(
            prior_day,
            paths_by_day,
            warmup_days=int(target_manifest["warmup_days_requested"]),
        )
        source_paths = [*warmup_paths, target_bar]
        source_receipts = [
            _artifact(path, role=f"{prior_day} boundary 1s source") for path in source_paths
        ]
        output_path = output / f"features_{prior_day}.parquet"
        if not output_path.is_file():
            bars = pd.concat([pd.read_parquet(path) for path in source_paths]).sort_index()
            bars = fe.filter_frame_for_orderbook_quality(bars, "BTCUSDC", label="1s bar")
            features = fe.process_day(
                bars,
                prior_day,
                "BTCUSDC",
                config_path=Path(target_manifest["config_path"]),
                market_stage=str(target_manifest["market_stage"]),
                reference_symbol=str(target_manifest["reference_symbol"]),
                calendar_tag=None,
                output_day_tag=prior_day,
                sample_weight_reference_date=sample_weight.get("reference_date"),
                sample_weight_lambda=float(sample_weight.get("lambda", 0.1)),
                require_execution_l2=bool(
                    (target_manifest.get("execution_l2_source") or {}).get("required")
                ),
                require_taker_tempo=bool(
                    (target_manifest.get("taker_tempo_source") or {}).get("required")
                ),
            )
            terminal_label = pd.Timestamp(prior_day, tz="UTC") + pd.Timedelta(
                hours=23, minutes=59, seconds=50
            )
            if terminal_label not in features.index:
                raise ControlOverlayRepairError(
                    f"{prior_day} target-clock closure did not produce 23:59:50"
                )
            _atomic_parquet(output_path, features.loc[[terminal_label]])
        frame = pd.read_parquet(output_path)
        expected_label = pd.Timestamp(prior_day, tz="UTC") + pd.Timedelta(
            hours=23, minutes=59, seconds=50
        )
        if len(frame) != 1 or frame.index[0] != expected_label:
            raise ControlOverlayRepairError(f"{prior_day} boundary artifact is noncanonical")
        receipt = _artifact(output_path, role=f"{prior_day} boundary feature")
        receipts.append(
            {
                "day": prior_day,
                "target_day_clock_closure": target_day,
                "file": output_path.name,
                "sha256": receipt["sha256"],
                "size_bytes": receipt["size_bytes"],
                "source_bars": source_receipts,
            }
        )
    semantics = _feature_semantics(target_manifest)
    manifest = {
        "schema_version": "causal_v12_v9_10s_boundary_feature_repair.v1",
        "identity": "causal_v12_v9_10s_boundary_feature_repair_v1",
        "created_at_utc": datetime.now(tz=UTC).isoformat(),
        **semantics,
        "boundary_builder": _artifact(Path(__file__), role="boundary builder"),
        "target_feature_manifest": _artifact(
            target_feature_manifest, role="target feature manifest"
        ),
        "source_plan": _artifact(source_plan, role="frozen source plan"),
        "precommit": _artifact(precommit_path, role="frozen F03 precommit"),
        "construction": (
            "D-1 contiguous warmup plus target-day clock closure; only D-1 23:59:50 "
            "is admitted; target-day values cannot enter the left-closed D-1 bucket"
        ),
        "invalid_target_days": invalid_days,
        "daily_file_count": len(receipts),
        "daily_files": receipts,
        "economic_outcomes_read": False,
        "training_authorized": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    path = output / "repair_feature_boundary_manifest.json"
    if path.exists():
        existing = _load_json(path, role="boundary feature manifest")
        if existing.get("daily_files") != receipts:
            raise ControlOverlayRepairError("existing boundary manifest has another identity")
        return existing | {"path": str(path), "sha256": _sha256_file(path)}
    _atomic_json(path, manifest)
    return manifest | {"path": str(path), "sha256": _sha256_file(path)}


def _model_bundle_binding(source: Mapping[str, Any]) -> dict[str, Any]:
    global_identity = source.get("global_execution_identity") or {}
    row = global_identity.get("model_bundle")
    if not isinstance(row, Mapping):
        raise ControlOverlayRepairError("source plan lacks v9 model bundle")
    bundle_meta = _artifact(row.get("path"), row.get("sha256"), role="v9 bundle_meta")
    model_dir = Path(bundle_meta["path"]).parent
    payload = _load_json(Path(bundle_meta["path"]), role="v9 bundle_meta")
    if tuple(payload.get("targets") or ()) != EXPECTED_HEADS:
        raise ControlOverlayRepairError("v9 bundle is not the frozen ordered 13-head identity")
    artifacts = []
    head_feature_identities: dict[str, dict[str, Any]] = {}
    for head in EXPECTED_HEADS:
        model = _artifact(model_dir / f"{head}.txt", role=f"v9 {head} model")
        meta = _artifact(model_dir / f"{head}_meta.json", role=f"v9 {head} meta")
        metadata = _load_json(Path(meta["path"]), role=f"v9 {head} metadata")
        feature_columns = metadata.get("feature_cols")
        if (
            not isinstance(feature_columns, list)
            or not feature_columns
            or any(not isinstance(column, str) or not column for column in feature_columns)
        ):
            raise ControlOverlayRepairError(f"v9 {head} lacks a valid feature schema")
        artifacts.extend((model, meta))
        head_feature_identities[head] = {
            "model_sha256": model["sha256"],
            "metadata_sha256": meta["sha256"],
            "feature_count": len(feature_columns),
            "feature_columns_sha256": _canonical_sha256(feature_columns),
        }
    signatures = data_windows._model_artifact_signatures(model_dir)
    content_identity = references_sha256(data_windows._signature_references(signatures))
    if content_identity != global_identity.get("model_overlay_bundle_identity_sha256"):
        raise ControlOverlayRepairError("v9 bundle content identity differs from source plan")
    return {
        "directory": str(model_dir.resolve()),
        "bundle_meta": bundle_meta,
        "content_identity_sha256": content_identity,
        "head_count": len(EXPECTED_HEADS),
        "head_feature_identities": head_feature_identities,
        "head_feature_identity_sha256": _canonical_sha256(head_feature_identities),
        "artifacts": artifacts,
    }


def _global_binding(source_path: Path, source: Mapping[str, Any]) -> dict[str, Any]:
    permissions = source.get("permissions") or {}
    if (
        permissions.get("economic_evaluation", False) is not False
        or permissions.get("economic_outcomes_read", False) is not False
    ):
        raise ControlOverlayRepairError("source plan crossed the outcome boundary")
    operational = load_operational_baseline_binding()
    if operational is None:
        raise ControlOverlayRepairError("operational v9 pointer is missing")
    pointer = operational["pointer"]
    identity = operational["identity"]
    config = identity.get("config") or {}
    if pointer.get("ml_enabled") is not True or config.get("ml_enabled") is not True:
        raise ControlOverlayRepairError("operational v9 must keep ML enabled")
    forbidden = (
        "dynamic_fill_hazard_action_enabled",
        "buy_fill_selection_live_enabled",
    )
    if any(bool(pointer.get(name)) or bool(config.get(name)) for name in forbidden):
        raise ControlOverlayRepairError("q90 action and BUY selector must be OFF")
    global_identity = source.get("global_execution_identity") or {}
    if global_identity.get("q90_action_enabled") is not False:
        raise ControlOverlayRepairError("source plan q90 action is not OFF")
    artifacts: dict[str, Any] = {}
    for name in (
        "operational_config",
        "p3_artifact",
        "queue_calibration",
        "source_contract",
        "latency_profile",
    ):
        row = global_identity.get(name)
        if not isinstance(row, Mapping):
            raise ControlOverlayRepairError(f"source plan lacks {name}")
        artifacts[name] = _artifact(row.get("path"), row.get("sha256"), role=name)
    if artifacts["operational_config"]["sha256"] != pointer.get("live_config_sha256"):
        raise ControlOverlayRepairError("source config differs from operational pointer")
    dag = global_identity.get("feature_dag") or {}
    dag_impl = dag.get("implementation") or {}
    artifacts["feature_dag"] = _artifact(
        dag_impl.get("path"), dag_impl.get("sha256"), role="feature DAG"
    )
    model = _model_bundle_binding(source)
    return {
        "source_plan": _artifact(source_path, role="frozen source plan"),
        "source_global_identity_sha256": source.get("global_execution_identity_sha256"),
        "operational_pointer": _artifact(
            operational["pointer_path"], operational["pointer_sha256"], role="v9 pointer"
        ),
        "operational_identity": _artifact(
            operational["identity_path"], operational["identity_sha256"], role="v9 identity"
        ),
        "operational_config": artifacts["operational_config"],
        "p3_artifact": artifacts["p3_artifact"],
        "queue_calibration": artifacts["queue_calibration"],
        "source_contract": artifacts["source_contract"],
        "latency_profile": artifacts["latency_profile"],
        "feature_dag": {
            **artifacts["feature_dag"],
            "semantic_sha256": dag.get("semantic_sha256"),
        },
        "model_bundle": model,
        "ml_enabled": True,
        "q90_action_enabled": False,
        "buy_fill_selection_enabled": False,
        "economic_outcomes_read": False,
    }


def _window_overlay_parity(row: Mapping[str, Any], *, day: str) -> dict[str, Any]:
    window = row.get("window_cache")
    overlay = row.get("model_overlay")
    if not isinstance(window, Mapping) or not isinstance(overlay, Mapping):
        raise ControlOverlayRepairError(f"{day} lacks window/overlay binding")
    window_receipt = _artifact(window.get("path"), window.get("sha256"), role=f"{day} v13 window")
    parity = overlay.get("market_context_output_parity")
    overlay_identity = overlay.get("identity")
    if not isinstance(parity, Mapping) or not isinstance(overlay_identity, Mapping):
        raise ControlOverlayRepairError(f"{day} lacks market-context parity")
    context = str(overlay_identity.get("market_context_identity_sha256", ""))
    if (
        parity.get("exact_trades_and_rolling_arrays") is not True
        or parity.get("window_sha256") != window_receipt["sha256"]
        or parity.get("market_context_identity_sha256") != context
    ):
        raise ControlOverlayRepairError(f"{day} overlay/window context drift")
    return {
        "window": window_receipt,
        "market_context_identity_sha256": context,
        "exact_trades_and_rolling_arrays": True,
    }


def _source_rows(
    source: Mapping[str, Any], expected_days: Sequence[str]
) -> dict[str, Mapping[str, Any]]:
    rows = source.get("days")
    if not isinstance(rows, list):
        raise ControlOverlayRepairError("source plan lacks daily rows")
    by_day = {str(row.get("day")): row for row in rows if isinstance(row, Mapping)}
    if list(by_day) != list(expected_days):
        raise ControlOverlayRepairError("source plan denominator differs from frozen 40 days")
    return by_day


def _old_overlay_is_reference_eligible(
    binding: Mapping[str, Any], *, day: str, model_identity: str
) -> bool:
    try:
        dual_abi.load_bound_v9_control_overlay(
            binding,
            expected_day=day,
            expected_model_bundle_identity_sha256=model_identity,
        )
    except dual_abi.DualOverlayReplayError:
        return False
    return True


def prepare_plan(
    *,
    source_plan: Path = DEFAULT_SOURCE_PLAN,
    target_feature_manifest: Path = DEFAULT_TARGET_FEATURE_MANIFEST,
    warmup_feature_manifest: Path = DEFAULT_WARMUP_FEATURE_MANIFEST,
    precommit_path: Path = DEFAULT_PRECOMMIT,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    """Freeze reference/regenerate inputs without running inference or outcomes."""

    output = output_root.expanduser().resolve()
    required_root = external_cache_root(ROOT).resolve()
    try:
        output.relative_to(required_root)
    except ValueError as exc:
        raise ControlOverlayRepairError(
            "repair panel must live in the configured external cache"
        ) from exc
    if shutil.disk_usage(output.parent if output.parent.exists() else required_root).free < 60 * (
        1 << 30
    ):
        raise ControlOverlayRepairError(
            "configured external-cache free space is below the 60 GiB reserve"
        )
    precommit = _load_json(precommit_path, role="frozen F03 precommit")
    days = list((precommit.get("native_development_panel") or {}).get("days") or ())
    if len(days) != 40:
        raise ControlOverlayRepairError("frozen F03 denominator is not 40 days")
    source = _load_json(source_plan, role="frozen control source plan")
    if list(source.get("ordered_utc_days") or ()) != days:
        raise ControlOverlayRepairError("source and precommit denominators differ")
    global_binding = _global_binding(source_plan.resolve(), source)
    target_manifest, target_files = _daily_feature_receipts(target_feature_manifest.resolve())
    warmup_manifest, warmup_files = _daily_feature_receipts(warmup_feature_manifest.resolve())
    feature_semantics = _validate_feature_semantics(target_manifest, warmup_manifest)
    rows = _source_rows(source, days)
    daily = []
    for ordinal, day in enumerate(days, start=1):
        source_row = rows[day]
        parity = _window_overlay_parity(source_row, day=day)
        old_overlay = source_row["model_overlay"]
        reference = _old_overlay_is_reference_eligible(
            old_overlay,
            day=day,
            model_identity=global_binding["model_bundle"]["content_identity_sha256"],
        )
        prior_day = (date.fromisoformat(day) - timedelta(days=1)).isoformat()
        if day not in target_files:
            raise ControlOverlayRepairError(f"{day} target feature file is missing")
        prior = warmup_files.get(prior_day) or target_files.get(prior_day)
        if not reference and prior is None:
            raise ControlOverlayRepairError(f"{day} lacks a causal D-1 feature file")
        native = source_row.get("native_book_artifacts")
        if not isinstance(native, list) or len(native) != 48:
            raise ControlOverlayRepairError(f"{day} lacks target/D-1 native receipts")
        daily.append(
            {
                "ordinal": ordinal,
                "utc_day": day,
                "admission_mode": "reference_existing_exact"
                if reference
                else "regenerate_full_day",
                "source_daily_identity_sha256": source_row.get("daily_source_identity_sha256"),
                "window_binding": parity,
                "old_overlay": dict(old_overlay),
                "target_features": target_files[day],
                "prior_features": prior,
                "native_book_artifacts": [dict(item) for item in native],
            }
        )
    payload = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "identity": IDENTITY,
        "canonical_ready_time_contract": {
            "semantics": GRID_SEMANTICS,
            "cadence_ms": CADENCE_MS,
            "rows_per_day": ROWS_PER_DAY,
            "first_visibility": "00:00:00 from D-1 23:59:50 bucket",
            "last_visibility": "23:59:50 from target 23:59:40 bucket",
            "trade_time_clipping_forbidden": True,
        },
        "precommit": _artifact(precommit_path, role="frozen F03 precommit"),
        "global_binding": global_binding,
        "target_feature_manifest": _artifact(
            target_feature_manifest, role="target feature manifest"
        ),
        "warmup_feature_manifest": _artifact(
            warmup_feature_manifest, role="warmup feature manifest"
        ),
        "feature_semantics": feature_semantics,
        "ordered_utc_days": days,
        "daily": daily,
        "output_root": str(output),
    }
    plan = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "identity": IDENTITY,
        "created_at_utc": datetime.now(tz=UTC).isoformat(),
        "status": "prepared_outcomes_unread",
        "plan_identity_sha256": _canonical_sha256(payload),
        "identity_payload": payload,
        "reference_day_count": sum(row["admission_mode"].startswith("reference") for row in daily),
        "regenerate_day_count": sum(
            row["admission_mode"].startswith("regenerate") for row in daily
        ),
        "economic_outcomes_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    output.mkdir(parents=True, exist_ok=True)
    plan_path = output / PLAN_FILENAME
    marker = output / PLAN_SUCCESS
    if plan_path.exists() or marker.exists():
        existing = validate_plan(plan_path)
        if existing["plan_identity_sha256"] != plan["plan_identity_sha256"]:
            raise ControlOverlayRepairError("existing plan has another identity")
        return existing
    _atomic_json(plan_path, plan)
    _atomic_json(marker, {"plan_sha256": _sha256_file(plan_path)})
    return plan | {"path": str(plan_path), "sha256": _sha256_file(plan_path)}


def validate_plan(plan_path: Path) -> dict[str, Any]:
    plan = _load_json(plan_path, role="control overlay repair plan")
    if plan.get("schema_version") != PLAN_SCHEMA_VERSION or plan.get("identity") != IDENTITY:
        raise ControlOverlayRepairError("unsupported repair plan")
    payload = plan.get("identity_payload")
    if not isinstance(payload, Mapping) or plan.get("plan_identity_sha256") != _canonical_sha256(
        payload
    ):
        raise ControlOverlayRepairError("repair plan identity is not reproducible")
    marker = _load_json(plan_path.resolve().parent / PLAN_SUCCESS, role="plan success receipt")
    if marker.get("plan_sha256") != _sha256_file(plan_path.resolve()):
        raise ControlOverlayRepairError("repair plan atomic receipt drift")
    if any(
        plan.get(name) is not False
        for name in ("economic_outcomes_read", "action_authorized", "live_authorized")
    ):
        raise ControlOverlayRepairError("repair plan crossed its permission boundary")
    for name in ("precommit", "target_feature_manifest", "warmup_feature_manifest"):
        row = payload[name]
        _artifact(row["path"], row["sha256"], role=name)
    return plan | {"path": str(plan_path.resolve()), "sha256": _sha256_file(plan_path.resolve())}


def _select_feature_rows(prior_path: Path, target_path: Path, *, day: str) -> pd.DataFrame:
    prior = pd.read_parquet(prior_path)
    target = pd.read_parquet(target_path)
    features = pd.concat((prior, target)).sort_index()
    if features.index.has_duplicates:
        raise ControlOverlayRepairError(f"{day} repair features contain duplicate timestamps")
    labels = pd.to_datetime(canonical_feature_labels(day), unit="ms", utc=True)
    selected = features.reindex(labels)
    if selected.index.has_duplicates or len(selected) != ROWS_PER_DAY:
        raise ControlOverlayRepairError(f"{day} feature selection is not canonical")
    if selected.isna().all(axis=1).any():
        raise ControlOverlayRepairError(f"{day} is missing first/last causal feature bucket")
    return selected


def _generate_ml_data(
    prior_path: Path,
    target_path: Path,
    *,
    day: str,
    model_dir: Path,
) -> tuple[Any, ...]:
    try:
        import lightgbm as lgb
    except ImportError as exc:
        raise ControlOverlayRepairError("LightGBM is required for control repair") from exc
    features = _select_feature_rows(prior_path, target_path, day=day)
    base_columns = [
        column
        for column in features.columns
        if column not in LABEL_COLUMNS and column not in DROP_COLUMNS
    ]
    predictions: dict[str, np.ndarray] = {}
    for head in POLICY_HEADS:
        meta = _load_json(model_dir / f"{head}_meta.json", role=f"{head} metadata")
        columns = list(meta.get("feature_cols") or ())
        missing = [column for column in columns if column not in features.columns]
        if missing:
            raise ControlOverlayRepairError(f"{head} repair features miss {len(missing)} columns")
        values = np.asarray(
            lgb.Booster(model_file=str(model_dir / f"{head}.txt")).predict(features[columns]),
            dtype=np.float64,
        )
        if not np.isfinite(values).all():
            raise ControlOverlayRepairError(f"{head} generated nonfinite predictions")
        predictions[head] = values
    predictions["vol_10s"] = np.maximum(predictions["vol_10s"], 0.0)
    for head in ("tox_bid_10s", "tox_ask_10s"):
        predictions[head] = np.clip(predictions[head], 0.0, 1.0)
    xmarket = []
    for column in bt.XMARKET_REPLAY_FEATURE_COLUMNS:
        if column in features.columns:
            values = pd.to_numeric(features[column], errors="coerce").fillna(0.0).to_numpy(float)
        else:
            values = np.zeros(ROWS_PER_DAY, dtype=np.float64)
        xmarket.append(values)
    feature_mapping = {
        str(column): features[column].to_numpy(copy=False) for column in dict.fromkeys(base_columns)
    }
    return (
        canonical_visibility_grid(day),
        predictions["dir_10s"],
        predictions["vol_10s"],
        predictions["ret_10s"],
        predictions["tox_bid_10s"],
        predictions["tox_ask_10s"],
        *xmarket,
        feature_mapping,
    )


def _validate_ml_data(ml_data: Any, *, day: str) -> tuple[Any, ...]:
    if not isinstance(ml_data, tuple) or len(ml_data) != EXPECTED_TUPLE_LENGTH:
        raise ControlOverlayRepairError(f"{day} overlay is not the full v9 tuple")
    ready = np.asarray(ml_data[0], dtype=np.int64)
    expected = canonical_visibility_grid(day)
    if not np.array_equal(ready, expected):
        missing = np.setdiff1d(expected, ready)
        extra = np.setdiff1d(ready, expected)
        raise ControlOverlayRepairError(
            f"{day} noncanonical visibility grid: missing={len(missing)} extra={len(extra)}"
        )
    for index, value in enumerate(ml_data[:MAIN_ARRAY_COUNT]):
        array = np.asarray(value)
        if array.ndim != 1 or len(array) != ROWS_PER_DAY:
            raise ControlOverlayRepairError(f"{day} main array {index} is misaligned")
        if index and not np.isfinite(array.astype(np.float64, copy=False)).all():
            raise ControlOverlayRepairError(f"{day} main array {index} is nonfinite")
    feature_mapping = ml_data[-1]
    if not isinstance(feature_mapping, Mapping) or not feature_mapping:
        raise ControlOverlayRepairError(f"{day} overlay lacks model feature identity")
    for name, value in feature_mapping.items():
        array = np.asarray(value)
        if array.ndim != 1 or len(array) != ROWS_PER_DAY or array.dtype.hasobject:
            raise ControlOverlayRepairError(f"{day} feature array {name} is invalid")
    return ml_data


def _component_directory(output_root: Path, *, day: str, identity_sha256: str) -> Path:
    return output_root / "components" / "v9_control_overlay_day" / "btcusdc" / day / identity_sha256


def _arrays_for_storage(ml_data: tuple[Any, ...]) -> tuple[dict[str, np.ndarray], dict[str, Any]]:
    arrays = {
        f"main_{index:03d}": np.asarray(value)
        for index, value in enumerate(ml_data[:MAIN_ARRAY_COUNT])
    }
    mapping = ml_data[-1]
    keys = sorted(str(key) for key in mapping)
    for index, key in enumerate(keys):
        arrays[f"feature_{index:04d}"] = np.asarray(mapping[key])
    return arrays, {
        "main_count": MAIN_ARRAY_COUNT,
        "feature_mapping_present": True,
        "feature_keys": keys,
    }


def _publish_component(
    output_root: Path,
    *,
    identity: Mapping[str, Any],
    mode: str,
    old_overlay: Mapping[str, Any] | None = None,
    ml_data: tuple[Any, ...] | None = None,
) -> dict[str, Any]:
    identity_payload = dict(identity)
    identity_sha = _canonical_sha256(identity_payload)
    day = str(identity_payload["utc_day"])
    directory = _component_directory(output_root, day=day, identity_sha256=identity_sha)
    directory.parent.mkdir(parents=True, exist_ok=True)
    lock_path = directory.parent / f".{identity_sha}.lock"
    with lock_path.open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        if directory.exists():
            return _validate_component(directory)
        temporary = directory.parent / f".{identity_sha}.{uuid.uuid4().hex}.partial"
        temporary.mkdir()
        try:
            files: dict[str, dict[str, Any]] = {}
            if mode == "reference_existing_exact":
                if old_overlay is None:
                    raise ControlOverlayRepairError("reference component lacks old overlay")
                reference_path = temporary / "reference.json"
                _write_json_fsync(reference_path, dict(old_overlay))
                receipt = _artifact(reference_path, role="component reference")
                files[reference_path.name] = {
                    "sha256": receipt["sha256"],
                    "size_bytes": receipt["size_bytes"],
                }
                layout = None
            elif mode == "regenerate_full_day":
                admitted = _validate_ml_data(ml_data, day=day)
                arrays, layout = _arrays_for_storage(admitted)
                data_path = temporary / "model_overlay.npz"
                with data_path.open("wb") as handle:
                    np.savez_compressed(handle, **arrays)
                    handle.flush()
                    os.fsync(handle.fileno())
                receipt = _artifact(data_path, role="generated overlay")
                files[data_path.name] = {
                    "sha256": receipt["sha256"],
                    "size_bytes": receipt["size_bytes"],
                }
            else:
                raise ControlOverlayRepairError(f"unsupported component mode: {mode}")
            manifest = {
                "schema_version": SCHEMA_VERSION,
                "identity": identity_payload,
                "identity_sha256": identity_sha,
                "admission_mode": mode,
                "layout": layout,
                "files": files,
                "economic_outcomes_read": False,
                "action_authorized": False,
                "live_authorized": False,
            }
            manifest_path = temporary / "manifest.json"
            _write_json_fsync(manifest_path, manifest)
            _write_text_fsync(temporary / COMPONENT_SUCCESS, _sha256_file(manifest_path) + "\n")
            _fsync_directory(temporary)
            os.replace(temporary, directory)
            _fsync_directory(directory.parent)
        finally:
            if temporary.exists():
                shutil.rmtree(temporary)
    return _validate_component(directory)


def _validate_component(directory: Path) -> dict[str, Any]:
    manifest_path = directory / "manifest.json"
    manifest = _load_json(manifest_path, role="control component manifest")
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise ControlOverlayRepairError("control component schema drift")
    identity = manifest.get("identity")
    if not isinstance(identity, Mapping) or manifest.get("identity_sha256") != _canonical_sha256(
        identity
    ):
        raise ControlOverlayRepairError("control component identity drift")
    if directory.name != manifest["identity_sha256"]:
        raise ControlOverlayRepairError("control component path identity drift")
    success = directory / COMPONENT_SUCCESS
    if not success.is_file() or success.read_text(encoding="ascii").strip() != _sha256_file(
        manifest_path
    ):
        raise ControlOverlayRepairError("control component atomic receipt drift")
    for name, row in (manifest.get("files") or {}).items():
        _artifact(directory / name, row.get("sha256"), role=f"component {name}")
    if any(
        manifest.get(name) is not False
        for name in ("economic_outcomes_read", "action_authorized", "live_authorized")
    ):
        raise ControlOverlayRepairError("control component crossed permission boundary")
    return {
        "directory": str(directory.resolve()),
        "manifest_path": str(manifest_path.resolve()),
        "manifest_sha256": _sha256_file(manifest_path),
        "identity_sha256": manifest["identity_sha256"],
        "utc_day": identity["utc_day"],
        "admission_mode": manifest["admission_mode"],
    }


def _load_generated_component(directory: Path) -> tuple[Any, ...]:
    manifest = _load_json(directory / "manifest.json", role="generated control component")
    layout = manifest.get("layout")
    if not isinstance(layout, Mapping) or int(layout.get("main_count", -1)) != MAIN_ARRAY_COUNT:
        raise ControlOverlayRepairError("generated control component layout drift")
    with np.load(directory / "model_overlay.npz", allow_pickle=False) as arrays:
        values: list[Any] = [
            np.array(arrays[f"main_{index:03d}"], copy=True) for index in range(MAIN_ARRAY_COUNT)
        ]
        values.append(
            {
                key: np.array(arrays[f"feature_{index:04d}"], copy=True)
                for index, key in enumerate(layout["feature_keys"])
            }
        )
    return tuple(values)


def _day_identity(plan: Mapping[str, Any], row: Mapping[str, Any]) -> dict[str, Any]:
    payload = plan["identity_payload"]
    return {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "utc_day": row["utc_day"],
        "admission_mode": row["admission_mode"],
        "canonical_ready_time_contract": payload["canonical_ready_time_contract"],
        "plan_identity_sha256": plan["plan_identity_sha256"],
        "v9_model_bundle_content_identity_sha256": payload["global_binding"]["model_bundle"][
            "content_identity_sha256"
        ],
        "operational_pointer_sha256": payload["global_binding"]["operational_pointer"]["sha256"],
        "operational_config_sha256": payload["global_binding"]["operational_config"]["sha256"],
        "p3_sha256": payload["global_binding"]["p3_artifact"]["sha256"],
        "feature_dag_sha256": payload["global_binding"]["feature_dag"]["sha256"],
        "feature_dag_semantic_sha256": payload["global_binding"]["feature_dag"]["semantic_sha256"],
        "window_sha256": row["window_binding"]["window"]["sha256"],
        "market_context_identity_sha256": row["window_binding"]["market_context_identity_sha256"],
        "target_feature_sha256": row["target_features"]["sha256"],
        "prior_feature_sha256": (row.get("prior_features") or {}).get("sha256"),
        "q90_action_enabled": False,
        "buy_fill_selection_enabled": False,
    }


def run_day(
    plan_path: Path,
    *,
    day: str,
    generator: Callable[..., tuple[Any, ...]] = _generate_ml_data,
) -> dict[str, Any]:
    plan = validate_plan(plan_path)
    payload = plan["identity_payload"]
    rows = {row["utc_day"]: row for row in payload["daily"]}
    if day not in rows:
        raise ControlOverlayRepairError(f"day is outside repair plan: {day}")
    row = rows[day]
    output_root = Path(payload["output_root"])
    identity = _day_identity(plan, row)
    expected = _component_directory(
        output_root, day=day, identity_sha256=_canonical_sha256(identity)
    )
    if expected.exists():
        return _validate_component(expected) | {"reused": True}
    if row["admission_mode"] == "reference_existing_exact":
        result = _publish_component(
            output_root,
            identity=identity,
            mode=row["admission_mode"],
            old_overlay=row["old_overlay"],
        )
    else:
        ml_data = generator(
            Path(row["prior_features"]["path"]),
            Path(row["target_features"]["path"]),
            day=day,
            model_dir=Path(payload["global_binding"]["model_bundle"]["directory"]),
        )
        result = _publish_component(
            output_root,
            identity=identity,
            mode=row["admission_mode"],
            ml_data=ml_data,
        )
    return result | {"reused": False}


def _admit_panel(plan_path: Path) -> dict[str, Any]:
    plan = validate_plan(plan_path)
    payload = plan["identity_payload"]
    output_root = Path(payload["output_root"])
    components = []
    for row in payload["daily"]:
        identity = _day_identity(plan, row)
        directory = _component_directory(
            output_root,
            day=row["utc_day"],
            identity_sha256=_canonical_sha256(identity),
        )
        components.append(_validate_component(directory))
    panel_identity = {
        "schema_version": PANEL_SCHEMA_VERSION,
        "identity": IDENTITY,
        "plan_path": str(plan_path.resolve()),
        "plan_sha256": _sha256_file(plan_path.resolve()),
        "plan_identity_sha256": plan["plan_identity_sha256"],
        "ordered_utc_days": payload["ordered_utc_days"],
        "canonical_ready_time_contract": payload["canonical_ready_time_contract"],
        "global_binding": payload["global_binding"],
        "components": components,
    }
    panel = {
        "schema_version": PANEL_SCHEMA_VERSION,
        "identity": IDENTITY,
        "created_at_utc": datetime.now(tz=UTC).isoformat(),
        "panel_identity_sha256": _canonical_sha256(panel_identity),
        "identity_payload": panel_identity,
        "day_count": len(components),
        "reference_day_count": sum(
            row["admission_mode"].startswith("reference") for row in components
        ),
        "regenerated_day_count": sum(
            row["admission_mode"].startswith("regenerate") for row in components
        ),
        "economic_outcomes_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    path = output_root / PANEL_FILENAME
    marker = output_root / PANEL_SUCCESS
    if path.exists() or marker.exists():
        return validate_panel(path)
    _atomic_json(path, panel)
    _atomic_json(marker, {"panel_sha256": _sha256_file(path)})
    return panel | {"path": str(path), "sha256": _sha256_file(path)}


def run_plan(
    plan_path: Path,
    *,
    day: str | None = None,
    generator: Callable[..., tuple[Any, ...]] = _generate_ml_data,
) -> dict[str, Any]:
    if day is not None:
        return run_day(plan_path, day=day, generator=generator)
    plan = validate_plan(plan_path)
    for utc_day in plan["identity_payload"]["ordered_utc_days"]:
        run_day(plan_path, day=utc_day, generator=generator)
    return _admit_panel(plan_path)


def _validate_component_payload(
    component: Mapping[str, Any], *, model_identity_sha256: str
) -> None:
    directory = Path(str(component["directory"]))
    if component["admission_mode"] == "reference_existing_exact":
        binding = _load_json(directory / "reference.json", role="control overlay reference")
        dual_abi.load_bound_v9_control_overlay(
            binding,
            expected_day=str(component["utc_day"]),
            expected_model_bundle_identity_sha256=model_identity_sha256,
        )
    else:
        _validate_ml_data(
            _load_generated_component(directory),
            day=str(component["utc_day"]),
        )


def validate_panel(panel_path: Path) -> dict[str, Any]:
    panel = _load_json(panel_path, role="control overlay successor panel")
    if panel.get("schema_version") != PANEL_SCHEMA_VERSION or panel.get("identity") != IDENTITY:
        raise ControlOverlayRepairError("unsupported control overlay panel")
    identity = panel.get("identity_payload")
    if not isinstance(identity, Mapping) or panel.get("panel_identity_sha256") != _canonical_sha256(
        identity
    ):
        raise ControlOverlayRepairError("control overlay panel identity drift")
    marker = _load_json(panel_path.resolve().parent / PANEL_SUCCESS, role="panel success receipt")
    if marker.get("panel_sha256") != _sha256_file(panel_path.resolve()):
        raise ControlOverlayRepairError("control overlay panel atomic receipt drift")
    if panel.get("day_count") != 40 or len(identity.get("components") or ()) != 40:
        raise ControlOverlayRepairError("control overlay panel is not complete")
    if list(identity.get("ordered_utc_days") or ()) != [
        row.get("utc_day") for row in identity["components"]
    ]:
        raise ControlOverlayRepairError("control overlay panel order drift")
    for row in identity["components"]:
        observed = _validate_component(Path(row["directory"]))
        if observed["identity_sha256"] != row["identity_sha256"]:
            raise ControlOverlayRepairError("control overlay component receipt drift")
        _validate_component_payload(
            observed,
            model_identity_sha256=identity["global_binding"]["model_bundle"][
                "content_identity_sha256"
            ],
        )
    if any(
        panel.get(name) is not False
        for name in ("economic_outcomes_read", "action_authorized", "live_authorized")
    ):
        raise ControlOverlayRepairError("control panel crossed permission boundary")
    return panel | {"path": str(panel_path.resolve()), "sha256": _sha256_file(panel_path.resolve())}


def load_admitted_control_schedule(
    panel_path: Path,
    *,
    panel_sha256: str,
    panel_identity_sha256: str,
    day: str,
) -> dual_abi.V9ControlSchedule:
    if _sha256_file(panel_path.resolve()) != panel_sha256:
        raise ControlOverlayRepairError("explicit control panel SHA256 drift")
    panel = validate_panel(panel_path)
    if panel["panel_identity_sha256"] != panel_identity_sha256:
        raise ControlOverlayRepairError("explicit control panel identity drift")
    global_binding = panel["identity_payload"]["global_binding"]
    rows = {row["utc_day"]: row for row in panel["identity_payload"]["components"]}
    if day not in rows:
        raise ControlOverlayRepairError(f"control panel lacks day {day}")
    component = _validate_component(Path(rows[day]["directory"]))
    directory = Path(component["directory"])
    manifest = _load_json(directory / "manifest.json", role="control component")
    if component["admission_mode"] == "reference_existing_exact":
        binding = _load_json(directory / "reference.json", role="control overlay reference")
        return dual_abi.load_bound_v9_control_overlay(
            binding,
            expected_day=day,
            expected_model_bundle_identity_sha256=global_binding["model_bundle"][
                "content_identity_sha256"
            ],
        )
    ml_data = _validate_ml_data(_load_generated_component(directory), day=day)
    identity = manifest["identity"]
    ready = np.asarray(ml_data[0], dtype=np.int64)
    ready.setflags(write=False)
    data_path = directory / "model_overlay.npz"
    return dual_abi.V9ControlSchedule(
        utc_day=day,
        ml_data=ml_data,
        ready_ts_ms=ready,
        target_grid_row_count=ROWS_PER_DAY,
        cache_root=directory,
        identity=identity,
        identity_sha256=manifest["identity_sha256"],
        manifest_path=directory / "manifest.json",
        manifest_sha256=_sha256_file(directory / "manifest.json"),
        data_path=data_path,
        data_sha256=_sha256_file(data_path),
        model_bundle_identity_sha256=global_binding["model_bundle"]["content_identity_sha256"],
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    boundaries = sub.add_parser("repair-boundaries")
    boundaries.add_argument("--source-plan", type=Path, default=DEFAULT_SOURCE_PLAN)
    boundaries.add_argument(
        "--target-feature-manifest", type=Path, default=DEFAULT_TARGET_FEATURE_MANIFEST
    )
    boundaries.add_argument("--precommit", type=Path, default=DEFAULT_PRECOMMIT)
    boundaries.add_argument("--output-root", type=Path, default=DEFAULT_BOUNDARY_FEATURE_ROOT)
    prepare = sub.add_parser("prepare")
    prepare.add_argument("--source-plan", type=Path, default=DEFAULT_SOURCE_PLAN)
    prepare.add_argument(
        "--target-feature-manifest", type=Path, default=DEFAULT_TARGET_FEATURE_MANIFEST
    )
    prepare.add_argument(
        "--warmup-feature-manifest", type=Path, default=DEFAULT_WARMUP_FEATURE_MANIFEST
    )
    prepare.add_argument("--precommit", type=Path, default=DEFAULT_PRECOMMIT)
    prepare.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    run = sub.add_parser("run")
    run.add_argument("--plan", type=Path, required=True)
    run.add_argument("--day")
    validate = sub.add_parser("validate")
    validate.add_argument("--panel", type=Path, required=True)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    if args.command == "repair-boundaries":
        result = build_boundary_feature_manifest(
            source_plan=args.source_plan,
            target_feature_manifest=args.target_feature_manifest,
            precommit_path=args.precommit,
            output_root=args.output_root,
        )
    elif args.command == "prepare":
        result = prepare_plan(
            source_plan=args.source_plan,
            target_feature_manifest=args.target_feature_manifest,
            warmup_feature_manifest=args.warmup_feature_manifest,
            precommit_path=args.precommit,
            output_root=args.output_root,
        )
    elif args.command == "run":
        result = run_plan(args.plan, day=args.day)
    else:
        result = validate_panel(args.panel)
    print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    main()
