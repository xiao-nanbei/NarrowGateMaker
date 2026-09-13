#!/usr/bin/env python3
"""Strict external-generation successor for EMA ADD-vs-WAIT evidence."""

from __future__ import annotations

import argparse
import concurrent.futures
import copy
import hashlib
import json
import math
import os
import tempfile
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml
from sklearn.linear_model import Ridge

from data_paths import data_root, resolve_portable_path
from models import backtest_tick as bt
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_native_40day_full_path_ml_ab as native_runner,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_v9_10s_control_overlay_repair as control_repair,
)
from research.families.f05_fill_quality_quote_ev.audit.multiscale_ema_add_wait_incremental_value import (
    ADD_NOW,
    WAIT_ONE_EPOCH,
    ArmWashoutState,
    ContinuousTimeEmaSurface,
    MarketGeneration,
    campaign_unit_weights,
    joint_washout_complete,
    model_feature_names,
    validate_feature_row,
)
from research.governance.public_machine_projection import (
    PublicMachineProjectionError,
    source_document_path,
    source_identity_sha256,
)

ROOT = Path(__file__).resolve().parents[4]
DATA_ROOT = data_root(ROOT)
IDENTITY = "multiscale_ema_add_wait_incremental_value_v1_1"
SCHEMA_VERSION = f"{IDENTITY}.development.v1"
SPEC = ROOT / (
    "research/families/f05_fill_quality_quote_ev/docs/"
    "multiscale_ema_add_wait_incremental_value_v1_1_spec_20260809.json"
)
EXECUTION_AMENDMENT = ROOT / (
    "research/families/f05_fill_quality_quote_ev/docs/"
    "multiscale_ema_add_wait_incremental_value_v1_1_execution_amendment_20260809.json"
)
PLAN = DATA_ROOT / (
    "cache/replay_dag/"
    "f03_causal_v12_1s_native_40day_full_path_ml_ab_v3/execution-plan.json"
)
OUTPUT = DATA_ROOT / (
    "reports/"
    "multiscale_ema_add_wait_incremental_value_v1_1_20260809"
)

REQUIRED_AMENDMENT_ROLES = frozenset(
    {
        "frozen_spec",
        "study_runner",
        "ema_contract",
        "python_replay",
        "operational_config",
        "operational_baseline_pointer",
        "replay_baseline",
        "execution_plan",
        "test_contract",
        "model_bundle_meta",
        "p3_artifact",
        "queue_calibration",
        "latency_profile",
    }
)


def _resolve_bound_path(value: str | Path) -> Path:
    path = resolve_portable_path(value, root=ROOT)
    if not path.is_absolute():
        path = ROOT / path
    return path.resolve()


def _artifact_paths(output: Path) -> dict[str, Path]:
    return {
        "panel_manifest": output / "opportunity_panel_manifest.json",
        "selected_panel": output / "selected_opportunities.parquet",
        "label_panel": output / "paired_labels.parquet",
        "report": output / "report.json",
    }


M0_FEATURES = (
    "baseline_action_is_add",
    "fill_cooldown_elapsed_ms",
    "fill_cooldown_remaining_ms",
    "fill_cooldown_consecutive_units",
    "campaign_age_s",
    "abs_inventory_units",
    "campaign_max_abs_inventory_units",
    "campaign_pnl_so_far",
    "campaign_adverse_excursion_so_far",
    "campaign_exposure_increasing_fills_so_far",
    "campaign_reducing_fills_so_far",
    "toxicity",
    "markout_ema",
    "pred_direction_side_signed",
    "pred_return_side_signed",
    "baseline_distance_ticks",
    "spread_mult",
    "bbo_spread_ticks",
    "microprice_shift_side_signed_bps",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
    "l2_quote_flip_rate",
    "log1p_l2_near_depth_total",
    "decision_queue_ahead_btc",
    "decision_feature_volatility_5s",
    "decision_feature_volatility_30s",
    "decision_feature_volatility_60s",
    "decision_feature_volatility_300s",
    "decision_feature_volume_imbalance_5s_side_signed",
    "decision_feature_volume_imbalance_30s_side_signed",
    "decision_feature_volume_imbalance_60s_side_signed",
    "decision_feature_volume_imbalance_300s_side_signed",
    "decision_feature_trade_intensity_5s",
    "decision_feature_trade_intensity_30s",
    "decision_feature_trade_intensity_60s",
    "decision_feature_trade_intensity_300s",
    "decision_feature_vpin_5s",
    "decision_feature_vpin_30s",
    "decision_feature_vpin_60s",
    "decision_feature_vpin_300s",
    "decision_feature_price_change_5s_side_signed",
    "decision_feature_price_change_30s_side_signed",
    "decision_feature_price_change_60s_side_signed",
    "decision_feature_price_change_300s_side_signed",
    "decision_feature_taker_quote_imbalance_5s_side_signed",
    "decision_feature_taker_quote_imbalance_10s_side_signed",
    "decision_feature_taker_quote_imbalance_30s_side_signed",
    "decision_feature_taker_quote_imbalance_60s_side_signed",
)
M1_FEATURES = (*M0_FEATURES, *model_feature_names())
GENERATION_COLUMNS = {
    "bbo_index": "decision_visible_bbo_index",
    "l2_index": "decision_visible_l2_index",
    "trade_index": "decision_visible_trade_index",
    "feature_ready_index": "feature_ready_generation_index",
    "prediction_index": "prediction_generation_index",
    "snapshot_mid_tick_x2": "quote_snapshot_mid_tick_x2",
}


class StudyError(RuntimeError):
    """Fail closed when a frozen F05 identity or execution path drifts."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.tmp-{uuid.uuid4().hex}"
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.tmp-{uuid.uuid4().hex}"
    try:
        frame.to_parquet(temporary, index=False, compression="zstd")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise StudyError(f"JSON artifact must be an object: {path}")
    return payload


def _validate_hash(path: Path, expected: str, *, role: str) -> None:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file() or _sha256_file(resolved) != str(expected):
        raise StudyError(f"{role} hash drift: {resolved}")


def _source_identity(path: Path, *, role: str) -> str:
    """Return a frozen source identity without confusing it with a projection."""

    try:
        return source_identity_sha256(path)
    except (OSError, PublicMachineProjectionError) as exc:
        raise StudyError(f"{role} source identity is unavailable: {path}") from exc


def _source_document(path: Path, *, role: str) -> Path:
    """Resolve exact owner bytes when retained, otherwise use the public projection."""

    try:
        return source_document_path(path, require_private=False)
    except (OSError, PublicMachineProjectionError) as exc:
        raise StudyError(f"{role} source document is unavailable: {path}") from exc


def _validate_source_identity(path: Path, expected: str, *, role: str) -> None:
    """Validate a normal artifact or the private-source identity of a projection."""

    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file() or _source_identity(resolved, role=role) != str(expected):
        raise StudyError(f"{role} source identity drift: {resolved}")


def _spec_sha256() -> str:
    return _source_identity(SPEC, role="frozen Spec")


def _execution_amendment_sha256() -> str:
    return _source_identity(EXECUTION_AMENDMENT, role="execution amendment")


def _require_output_path(path: Path, *, output: Path, role: str) -> Path:
    resolved = Path(path).expanduser().resolve()
    root = Path(output).expanduser().resolve()
    if resolved != root and root not in resolved.parents:
        raise StudyError(f"{role} escaped the requested output root: {resolved}")
    return resolved


def _validate_cached_table(
    *,
    manifest: Mapping[str, Any],
    expected_path: Path,
    output: Path,
    day: str,
    expected_spec_sha256: str,
    expected_plan_sha256: str,
    role: str,
    expected_amendment_sha256: str | None = None,
    expected_panel_sha256: str | None = None,
) -> None:
    if manifest.get("identity") != IDENTITY or manifest.get("utc_day") != day:
        raise StudyError(f"{role} cached identity/day drifted")
    if manifest.get("spec_sha256") != expected_spec_sha256:
        raise StudyError(f"{role} cached Spec identity drifted")
    if manifest.get("execution_plan_sha256") != expected_plan_sha256:
        raise StudyError(f"{role} cached execution plan drifted")
    if expected_amendment_sha256 is not None and (
        manifest.get("execution_amendment_sha256") != expected_amendment_sha256
    ):
        raise StudyError(f"{role} cached execution amendment drifted")
    if expected_panel_sha256 is not None and (
        manifest.get("panel_sha256") != expected_panel_sha256
    ):
        raise StudyError(f"{role} cached opportunity panel drifted")
    manifest_path = _require_output_path(
        Path(str(manifest.get("data_path", ""))),
        output=output,
        role=f"{role} data path",
    )
    if manifest_path != expected_path.expanduser().resolve():
        raise StudyError(f"{role} cached data path drifted")
    _validate_hash(
        manifest_path,
        str(manifest.get("data_sha256", "")),
        role=role,
    )
    frame = pd.read_parquet(manifest_path)
    if int(manifest.get("row_count", manifest.get("eligible_row_count", -1))) != len(frame):
        raise StudyError(f"{role} cached row count drifted")


def _spec_and_plan() -> tuple[dict[str, Any], dict[str, Any]]:
    public_spec = _load_json(SPEC)
    pointer = public_spec["source_contract"]["operational_baseline_pointer"]
    if pointer.get("exact_bytes_status") != "available":
        raise StudyError(
            "frozen operational baseline pointer exact bytes are missing; "
            "historical execution fails closed and must not substitute the current pointer"
        )
    spec = _load_json(_source_document(SPEC, role="frozen Spec"))
    if spec.get("identity") != IDENTITY:
        raise StudyError("unexpected F05 identity")
    source = spec["source_contract"]
    for key, role in (
        ("denominator_source_spec", "current replay baseline"),
        ("operational_baseline_pointer", "operational baseline pointer"),
        ("operational_config", "operational config"),
        ("execution_plan", "40-day execution plan"),
    ):
        row = source[key]
        path = _resolve_bound_path(str(row["path"]))
        _validate_source_identity(path, row["sha256"], role=role)
    plan = _load_json(PLAN)
    days = tuple(spec["development_denominator"]["ordered_utc_days"])
    plan_days = tuple(row["utc_day"] for row in plan["identity_payload"]["days"])
    if days != plan_days or len(days) != 40:
        raise StudyError("frozen Development denominator drifted")
    return spec, plan


def _offline_params(spec: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    config_row = spec["source_contract"]["operational_config"]
    config_path = _resolve_bound_path(str(config_row["path"]))
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise StudyError("operational config is not a mapping")
    projected = copy.deepcopy(raw)
    journal = projected.get("lifecycle_journal_v2")
    if not isinstance(journal, dict) or journal.get("enabled") is not True:
        raise StudyError("expected live lifecycle journal writer to be enabled")
    journal["enabled"] = False
    changed = []

    def visit(left: Any, right: Any, path: str = "") -> None:
        if isinstance(left, Mapping) and isinstance(right, Mapping):
            for key in sorted(set(left) | set(right), key=str):
                child = f"{path}.{key}" if path else str(key)
                if key not in left or key not in right:
                    changed.append(child)
                else:
                    visit(left[key], right[key], child)
        elif left != right:
            changed.append(path)

    visit(raw, projected)
    if changed != ["lifecycle_journal_v2.enabled"]:
        raise StudyError(f"offline projection escaped allowlist: {changed}")
    with tempfile.TemporaryDirectory(prefix="narrowgate-f05-ema-") as directory:
        path = Path(directory) / "config.yaml"
        path.write_text(yaml.safe_dump(projected, sort_keys=False), encoding="utf-8")
        params = native_runner._load_formal_base_params(path)
    if bool(params.get("dynamic_fill_hazard_action_enabled", True)):
        raise StudyError("F05 requires q90 action OFF")
    if bool(params.get("buy_fill_selection_live_enabled", True)):
        raise StudyError("F05 requires BUY fill selector OFF")
    return params, {
        "source_config_sha256": _sha256_file(config_path),
        "source_mapping_sha256": _canonical_sha256(raw),
        "projected_mapping_sha256": _canonical_sha256(projected),
        "changed_paths": changed,
    }


def _day_row(plan: Mapping[str, Any], day: str) -> dict[str, Any]:
    rows = {row["utc_day"]: row for row in plan["identity_payload"]["days"]}
    if day not in rows:
        raise StudyError(f"day is outside frozen denominator: {day}")
    return rows[day]


def _load_day_inputs(
    day: str,
    *,
    spec: Mapping[str, Any],
    plan: Mapping[str, Any],
) -> tuple[Any, Any, dict[str, Any], dict[str, Any]]:
    row = _day_row(plan, day)
    window_path = Path(row["window"]["path"])
    _validate_hash(window_path, row["window"]["sha256"], role=f"{day} window")
    window = native_runner._load_bound_window(window_path)
    payload = plan["identity_payload"]
    control = payload["control_sources"]
    schedule = control_repair.load_admitted_control_schedule(
        Path(control["path"]),
        panel_sha256=control["sha256"],
        panel_identity_sha256=control["panel_identity_sha256"],
        day=day,
    )
    params, projection = _offline_params(spec)
    params["ber_exposure_add_only"] = False
    shared = {
        "ml_data": schedule.ml_data,
        "bbo_data": window.bbo_data,
        "l2_data": window.l2_data,
        "var_ti": window.var_ti,
        "var_retsq": window.var_retsq,
    }
    return window, schedule, params, {"shared": shared, "projection": projection}


def _generation_from_row(row: Mapping[str, Any]) -> MarketGeneration:
    return MarketGeneration(
        bbo_index=int(row[GENERATION_COLUMNS["bbo_index"]]),
        l2_index=int(row[GENERATION_COLUMNS["l2_index"]]),
        trade_index=int(row[GENERATION_COLUMNS["trade_index"]]),
        feature_ready_index=int(row[GENERATION_COLUMNS["feature_ready_index"]]),
        prediction_index=int(row[GENERATION_COLUMNS["prediction_index"]]),
        snapshot_mid_tick_x2=int(row[GENERATION_COLUMNS["snapshot_mid_tick_x2"]]),
    )


def _attach_external_release(
    frame: pd.DataFrame,
    trace: pd.DataFrame,
) -> pd.DataFrame:
    required = {
        "external_epoch_support_valid",
        "external_epoch_support_reason",
        "release_ts_ms",
        "release_market_event_generation",
        "release_market_generation_identity",
        *(f"release_{name}" for name in GENERATION_COLUMNS),
    }
    missing = sorted(required - set(trace.columns))
    if missing:
        raise StudyError(f"external market release trace schema is incomplete: {missing}")
    output = frame.copy()
    release_columns = sorted(required)
    output.loc[:, release_columns] = trace.loc[output.index, release_columns]
    for row in output.to_dict("records"):
        if not bool(row["external_epoch_support_valid"]):
            continue
        target_generation = _generation_from_row(row)
        target_generation.validate()
        release_generation = MarketGeneration(
            **{name: int(row[f"release_{name}"]) for name in GENERATION_COLUMNS}
        )
        release_generation.validate()
        if not release_generation.is_strictly_after(target_generation):
            raise StudyError("frozen release generation did not advance")
        if int(row["release_ts_ms"]) < int(row["ts_ms"]):
            raise StudyError("frozen release timestamp regressed")
        if int(row["release_market_event_generation"]) <= int(row["market_event_generation"]):
            raise StudyError("frozen release event locator did not advance")
        if row["release_market_generation_identity"] != release_generation.identity:
            raise StudyError("frozen release generation identity drifted")
    return output


def _eligible_opportunities(
    day: str,
    trace: pd.DataFrame,
    *,
    lot_size: float,
    tick_size: float,
) -> pd.DataFrame:
    required = {
        "canonical_external_decision",
        "market_readiness",
        "campaign_active",
        "same_side_pending_before",
        "order_active_before",
        "can_post_before_fill_cooldown",
        "can_post_after_fill_cooldown",
        "inventory",
        "side",
        "action",
        "campaign_id",
        "market_event_generation",
        "ts_ms",
        *GENERATION_COLUMNS.values(),
    }
    missing = sorted(required - set(trace.columns))
    if missing:
        raise StudyError(f"decision trace schema missing fields: {missing}")
    mask = (
        trace["canonical_external_decision"].eq(1)
        & trace["market_readiness"].eq(1)
        & trace["campaign_active"].eq(1)
        & trace["same_side_pending_before"].eq(0)
        & trace["order_active_before"].eq(0)
        & trace["can_post_before_fill_cooldown"].eq(1)
        & (
            (trace["side"].eq("BUY") & trace["inventory"].gt(0.0))
            | (trace["side"].eq("SELL") & trace["inventory"].lt(0.0))
        )
    )
    frame = trace.loc[mask].copy()
    frame["baseline_action"] = np.where(
        frame["can_post_after_fill_cooldown"].eq(1),
        ADD_NOW,
        WAIT_ONE_EPOCH,
    )
    exact_action = (frame["baseline_action"].eq(ADD_NOW) & frame["action"].eq("place")) | (
        frame["baseline_action"].eq(WAIT_ONE_EPOCH) & frame["action"].eq("pause")
    )
    frame = frame.loc[exact_action].copy()
    frame["utc_day"] = day
    frame["cooldown_phase"] = np.where(
        frame["baseline_action"].eq(ADD_NOW),
        "COOLDOWN_EXPIRED",
        "COOLDOWN_ACTIVE",
    )
    frame["prospective_campaign_side_id"] = (
        frame["utc_day"].astype(str)
        + ":"
        + frame["side"].astype(str)
        + ":"
        + frame["campaign_id"].astype(str)
    )
    frame["market_generation_identity"] = [
        _generation_from_row(row).identity for row in frame.to_dict("records")
    ]
    frame = _attach_external_release(frame, trace)
    frame["opportunity_id"] = [
        _canonical_sha256(
            {
                "identity": IDENTITY,
                "utc_day": row["utc_day"],
                "side": row["side"],
                "cooldown_phase": row["cooldown_phase"],
                "prospective_campaign_side_id": row["prospective_campaign_side_id"],
                "decision_ts_ns": int(row["decision_ts_ns"]),
                "market_generation_identity": row["market_generation_identity"],
            }
        )
        for row in frame.to_dict("records")
    ]
    sign = np.where(frame["side"].eq("BUY"), 1.0, -1.0)
    frame["baseline_action_is_add"] = frame["baseline_action"].eq(ADD_NOW).astype(float)
    frame["abs_inventory_units"] = frame["inventory"].abs() / float(lot_size)
    frame["campaign_max_abs_inventory_units"] = frame["campaign_max_abs_qty_so_far"].abs() / float(
        lot_size
    )
    frame["pred_direction_side_signed"] = sign * (2.0 * frame["pred_dir"] - 1.0)
    frame["pred_return_side_signed"] = sign * frame["pred_ret"]
    frame["microprice_shift_side_signed_bps"] = sign * frame["microprice_shift_bps"]
    frame["bbo_spread_ticks"] = (frame["best_ask"] - frame["best_bid"]) / float(tick_size)
    frame["baseline_distance_ticks"] = np.where(
        frame["side"].eq("BUY"),
        (frame["best_bid"] - frame["final_price"]) / float(tick_size),
        (frame["final_price"] - frame["best_ask"]) / float(tick_size),
    )
    frame["log1p_l2_near_depth_total"] = np.log1p(frame["l2_near_depth_total"].clip(lower=0.0))
    signed_families = (
        "volume_imbalance",
        "price_change",
        "taker_quote_imbalance",
    )
    for name in tuple(frame.columns):
        if not name.startswith("decision_feature_"):
            continue
        if any(family in name for family in signed_families):
            frame[f"{name}_side_signed"] = sign * frame[name].astype(float)
    if (frame["feature_ready_ts_ms"] > frame["ts_ms"]).any():
        raise StudyError(f"{day} feature-ready clock crossed decision clock")
    return frame


def _add_ema_features(
    frame: pd.DataFrame,
    bbo_data: Any,
    l2_data: Any,
    *,
    tick_size: float,
) -> pd.DataFrame:
    if frame.empty:
        return frame
    ordered = frame.sort_values(
        ["decision_visible_bbo_index", "ts_ms", "side", "opportunity_id"]
    ).copy()
    surface = ContinuousTimeEmaSurface()
    bbo_ts = np.asarray(bbo_data.ts_ms, dtype=np.int64)
    bids = np.asarray(bbo_data.best_bid, dtype=np.float64)
    asks = np.asarray(bbo_data.best_ask, dtype=np.float64)
    l2_ts = np.asarray(l2_data.ts_ms, dtype=np.int64)
    l2_bid_px = np.asarray(l2_data.bid_px, dtype=np.float64)
    l2_bid_qty = np.asarray(l2_data.bid_qty, dtype=np.float64)
    l2_ask_px = np.asarray(l2_data.ask_px, dtype=np.float64)
    l2_ask_qty = np.asarray(l2_data.ask_qty, dtype=np.float64)
    cursor = -1
    feature_rows: list[dict[str, float | int]] = []
    for row in ordered.to_dict("records"):
        target = int(row["decision_visible_bbo_index"])
        if target < cursor or target >= len(bbo_ts):
            raise StudyError("decision-visible BBO cursor drifted")
        for index in range(cursor + 1, target + 1):
            bid = float(bids[index])
            ask = float(asks[index])
            if bid <= 0.0 or ask <= bid:
                continue
            surface.update(
                ts_ns=int(bbo_ts[index]) * 1_000_000,
                price=0.5 * (bid + ask),
            )
        cursor = target
        mid = float(row["mid"])
        volatility_bps = abs(float(row.get("decision_feature_volatility_5s", 0.0))) * 10_000.0
        ema_row = surface.feature_row(
            side=str(row["side"]),
            causal_volatility_bps=volatility_bps,
            tick_bps=float(tick_size) / mid * 10_000.0,
        )
        validate_feature_row(ema_row)
        if int(ema_row["ema_surface_feature_ready_ts_ns"]) > int(row["decision_ts_ns"]):
            raise StudyError("EMA feature-ready clock crossed decision clock")
        l2_index = int(row["decision_visible_l2_index"])
        queue_ahead = float(row.get("decision_queue_ahead_btc", math.nan))
        queue_source = str(row.get("decision_queue_ahead_source", "trace_queue_unavailable"))
        if not math.isfinite(queue_ahead) and 0 <= l2_index < len(l2_ts):
            if int(l2_ts[l2_index]) > int(row["ts_ms"]):
                raise StudyError("decision-visible L2 clock crossed decision clock")
            if str(row["side"]) == "BUY":
                prices = l2_bid_px[l2_index]
                quantities = l2_bid_qty[l2_index]
            else:
                prices = l2_ask_px[l2_index]
                quantities = l2_ask_qty[l2_index]
            price_ticks = np.rint(prices / float(tick_size)).astype(np.int64)
            target_tick = int(round(float(row["final_price"]) / float(tick_size)))
            matches = np.flatnonzero(price_ticks == target_tick)
            if len(matches):
                visible = float(np.sum(quantities[matches]))
                if math.isfinite(visible) and visible >= 0.0:
                    queue_ahead = visible
                    queue_source = "decision_visible_l2_exact_price"
                else:
                    queue_source = "decision_visible_l2_nonfinite"
            else:
                queue_source = "decision_visible_l2_price_not_in_top_n"
        ema_row["decision_queue_ahead_btc"] = float(queue_ahead)
        ema_row["decision_queue_ahead_source"] = queue_source
        ema_row["ema_surface_support_valid"] = 1
        ema_row["ema_surface_support_reason"] = "supported"
        feature_rows.append(ema_row)
    ema_frame = pd.DataFrame(feature_rows, index=ordered.index)
    for column in ema_frame.columns:
        ordered[column] = ema_frame[column]
    return ordered.sort_index()


def census_day(day: str, *, output: Path = OUTPUT) -> dict[str, Any]:
    spec, plan = _spec_and_plan()
    day_dir = output / "census" / day
    manifest_path = day_dir / "manifest.json"
    data_path = day_dir / "opportunities.parquet"
    if manifest_path.is_file() and data_path.is_file():
        manifest = _load_json(manifest_path)
        _validate_cached_table(
            manifest=manifest,
            expected_path=data_path,
            output=output,
            day=day,
            expected_spec_sha256=_spec_sha256(),
            expected_plan_sha256=_sha256_file(PLAN),
            role=f"{day} census",
        )
        if manifest.get("economic_outcomes_read") is not False:
            raise StudyError(f"{day} census cache read economic outcomes")
        return manifest
    window, _, params, audit = _load_day_inputs(day, spec=spec, plan=plan)
    params = dict(params)
    params["trace_decisions_max"] = 500_000
    params["decision_trace_profile"] = "full"
    params["trace_external_market_release"] = True
    result = bt._simulate_tick_with_engine(
        "python",
        window.trades,
        window.var_ts_ms,
        window.var_ssq,
        params,
        **audit["shared"],
    )
    trace = pd.DataFrame(result.get("_decision_trace") or ())
    lot_size = float(params.get("lot_size", bt.LOT_SIZE))
    tick_size = float(params.get("tick_size", bt.TICK))
    opportunities = _eligible_opportunities(
        day,
        trace,
        lot_size=lot_size,
        tick_size=tick_size,
    )
    opportunities = _add_ema_features(
        opportunities,
        window.bbo_data,
        window.l2_data,
        tick_size=tick_size,
    )
    missing_features = sorted(set(M1_FEATURES) - set(opportunities.columns))
    if missing_features:
        raise StudyError(f"{day} model feature schema incomplete: {missing_features}")
    _atomic_parquet(data_path, opportunities)
    manifest = {
        "schema_version": f"{SCHEMA_VERSION}.census_day",
        "identity": IDENTITY,
        "utc_day": day,
        "spec_sha256": _spec_sha256(),
        "execution_plan_sha256": _sha256_file(PLAN),
        "data_path": str(data_path),
        "data_sha256": _sha256_file(data_path),
        "decision_row_count": int(len(trace)),
        "eligible_row_count": int(len(opportunities)),
        "counts": [
            {
                "side": str(side),
                "cooldown_phase": str(phase),
                "rows": int(count),
            }
            for (side, phase), count in opportunities.groupby(
                ["side", "cooldown_phase"], observed=True
            )
            .size()
            .items()
        ],
        "economic_outcomes_read": False,
        "offline_projection": audit["projection"],
    }
    _atomic_json(manifest_path, manifest)
    return manifest


def run_census(days: Sequence[str], *, workers: int, output: Path = OUTPUT) -> None:
    if workers <= 1:
        for day in days:
            print(json.dumps(census_day(day, output=output), sort_keys=True))
        return
    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(census_day, day, output=output): day for day in days}
        for future in concurrent.futures.as_completed(futures):
            print(json.dumps(future.result(), sort_keys=True))


def freeze_panel(*, output: Path = OUTPUT) -> dict[str, Any]:
    spec, _ = _spec_and_plan()
    paths = _artifact_paths(output)
    panel_manifest_path = paths["panel_manifest"]
    selected_panel_path = paths["selected_panel"]
    if panel_manifest_path.is_file() or selected_panel_path.is_file():
        raise StudyError("opportunity panel is already frozen")
    frames = []
    for day in spec["development_denominator"]["ordered_utc_days"]:
        manifest_path = output / "census" / day / "manifest.json"
        if not manifest_path.is_file():
            raise StudyError(f"missing census day: {day}")
        manifest = _load_json(manifest_path)
        data_path = Path(manifest["data_path"])
        _validate_hash(data_path, manifest["data_sha256"], role=f"{day} census")
        frames.append(pd.read_parquet(data_path))
    census = pd.concat(frames, ignore_index=True)
    support_valid = census["ema_surface_support_valid"].astype(bool) & census[
        "external_epoch_support_valid"
    ].astype(bool)
    unsupported = census.loc[~support_valid].copy()
    census = census.loc[support_valid].copy()
    seed = spec["opportunity_contract"]["stable_sampling"]["seed_text"]
    census["stable_sample_hash"] = [
        hashlib.sha256(f"{seed}|{value}".encode("ascii")).hexdigest()
        for value in census["opportunity_id"]
    ]
    campaign_keys = [
        "utc_day",
        "side",
        "cooldown_phase",
        "prospective_campaign_side_id",
    ]
    representatives = (
        census.sort_values("stable_sample_hash")
        .drop_duplicates(campaign_keys, keep="first")
        .sort_values(["utc_day", "side", "cooldown_phase", "stable_sample_hash"])
    )
    selected = (
        representatives.groupby(
            ["utc_day", "side", "cooldown_phase"],
            observed=True,
            sort=False,
        )
        .head(2)
        .sort_values(["utc_day", "ts_ms", "side"])
        .reset_index(drop=True)
    )
    if len(selected) > int(spec["opportunity_contract"]["maximum_total_sampled_opportunities"]):
        raise StudyError("selected opportunity ceiling exceeded")
    _atomic_parquet(selected_panel_path, selected)
    manifest = {
        "schema_version": f"{SCHEMA_VERSION}.opportunity_panel",
        "identity": IDENTITY,
        "status": "frozen_before_fork_economic_outcomes",
        "spec_sha256": _spec_sha256(),
        "execution_plan_sha256": _sha256_file(PLAN),
        "selected_panel_path": str(selected_panel_path),
        "selected_panel_sha256": _sha256_file(selected_panel_path),
        "selected_row_count": int(len(selected)),
        "selected_campaign_count": int(selected["prospective_campaign_side_id"].nunique()),
        "m1_unsupported_row_count": int(
            (~unsupported["ema_surface_support_valid"].astype(bool)).sum()
        ),
        "m1_unsupported_reasons": {
            str(reason): int(count)
            for reason, count in unsupported.loc[
                ~unsupported["ema_surface_support_valid"].astype(bool),
                "ema_surface_support_reason",
            ]
            .value_counts(dropna=False)
            .items()
        },
        "external_epoch_unsupported_row_count": int(
            (~unsupported["external_epoch_support_valid"].astype(bool)).sum()
        ),
        "external_epoch_unsupported_reasons": {
            str(reason): int(count)
            for reason, count in unsupported.loc[
                ~unsupported["external_epoch_support_valid"].astype(bool),
                "external_epoch_support_reason",
            ]
            .value_counts(dropna=False)
            .items()
        },
        "cell_counts": [
            {
                "utc_day": str(day),
                "side": str(side),
                "cooldown_phase": str(phase),
                "rows": int(count),
            }
            for (day, side, phase), count in selected.groupby(
                ["utc_day", "side", "cooldown_phase"], observed=True
            )
            .size()
            .items()
        ],
        "development_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    _atomic_json(panel_manifest_path, manifest)
    return manifest


def _load_frozen_panel(output: Path) -> tuple[dict[str, Any], Path]:
    paths = _artifact_paths(output)
    manifest_path = paths["panel_manifest"]
    selected_path = paths["selected_panel"]
    manifest = _load_json(manifest_path)
    expected = {
        "identity": IDENTITY,
        "status": "frozen_before_fork_economic_outcomes",
        "spec_sha256": _spec_sha256(),
        "execution_plan_sha256": _sha256_file(PLAN),
        "selected_panel_path": str(selected_path),
    }
    for key, value in expected.items():
        if manifest.get(key) != value:
            raise StudyError(f"frozen opportunity panel {key} drifted")
    _require_output_path(selected_path, output=output, role="selected panel")
    _validate_hash(
        selected_path,
        str(manifest.get("selected_panel_sha256", "")),
        role="selected opportunity panel",
    )
    frame = pd.read_parquet(selected_path, columns=["opportunity_id"])
    if int(manifest.get("selected_row_count", -1)) != len(frame):
        raise StudyError("frozen opportunity panel row count drifted")
    return manifest, selected_path


def _validate_execution_amendment() -> dict[str, Any]:
    if not EXECUTION_AMENDMENT.is_file():
        raise StudyError("execution amendment is required before fork labels")
    amendment = _load_json(EXECUTION_AMENDMENT)
    if amendment.get("identity") != IDENTITY:
        raise StudyError("execution amendment identity drifted")
    rows = amendment.get("artifacts")
    if not isinstance(rows, list) or not rows:
        raise StudyError("execution amendment artifact set is empty")
    roles = [str(row.get("role", "")) for row in rows]
    if len(roles) != len(set(roles)):
        raise StudyError("execution amendment artifact roles are not unique")
    missing_roles = sorted(REQUIRED_AMENDMENT_ROLES - set(roles))
    if missing_roles:
        raise StudyError(f"execution amendment is missing required roles: {missing_roles}")
    for row in rows:
        path = _resolve_bound_path(str(row["path"]))
        _validate_source_identity(path, row["sha256"], role=row["role"])
    source = _spec_and_plan()[0]["source_contract"]
    role_rows = {str(row["role"]): row for row in rows}
    expected_role_sources = {
        "frozen_spec": {
            "path": str(SPEC),
            "sha256": _source_identity(SPEC, role="frozen Spec"),
        },
        "execution_plan": source["execution_plan"],
        "operational_config": source["operational_config"],
        "operational_baseline_pointer": source["operational_baseline_pointer"],
        "replay_baseline": source["denominator_source_spec"],
    }
    for role, expected in expected_role_sources.items():
        actual = role_rows[role]
        expected_path = _resolve_bound_path(str(expected["path"]))
        actual_path = _resolve_bound_path(str(actual["path"]))
        if actual_path.expanduser().resolve() != expected_path.expanduser().resolve() or str(
            actual["sha256"]
        ) != str(expected["sha256"]):
            raise StudyError(f"execution amendment {role} binding drifted")
    return amendment


def _arm_checkpoint(output: Path, opportunity_id: str, action: str) -> Path:
    return output / "arm_checkpoints" / opportunity_id / f"{action}.json"


def _checkpoint_payload(
    *,
    opportunity: Mapping[str, Any],
    action: str,
    trace: Mapping[str, Any],
    panel_sha256: str,
) -> dict[str, Any]:
    return {
        "schema_version": f"{SCHEMA_VERSION}.arm_checkpoint",
        "identity": IDENTITY,
        "opportunity_id": str(opportunity["opportunity_id"]),
        "action": str(action),
        "spec_sha256": _spec_sha256(),
        "execution_amendment_sha256": _execution_amendment_sha256(),
        "selected_panel_sha256": str(panel_sha256),
        "trace": dict(trace),
    }


def _load_arm_checkpoint(
    path: Path,
    *,
    output: Path,
    opportunity: Mapping[str, Any],
    action: str,
    panel_sha256: str,
) -> dict[str, Any]:
    _require_output_path(path, output=output, role="arm checkpoint")
    payload = _load_json(path)
    expected = {
        "identity": IDENTITY,
        "opportunity_id": str(opportunity["opportunity_id"]),
        "action": str(action),
        "spec_sha256": _spec_sha256(),
        "execution_amendment_sha256": _execution_amendment_sha256(),
        "selected_panel_sha256": str(panel_sha256),
    }
    for key, value in expected.items():
        if payload.get(key) != value:
            raise StudyError(f"arm checkpoint {key} drifted: {path}")
    trace = payload.get("trace")
    if not isinstance(trace, dict):
        raise StudyError(f"arm checkpoint trace is missing: {path}")
    return trace


def _run_arm(
    opportunity: Mapping[str, Any],
    action: str,
    *,
    window: Any,
    base: Mapping[str, Any],
    shared: Mapping[str, Any],
) -> dict[str, Any]:
    params = dict(base)
    params.update(
        {
            "ema_add_wait_fork_enabled": True,
            "ema_add_wait_fork_action": action,
            "ema_add_wait_fork_target_ts_ms": int(opportunity["ts_ms"]),
            "ema_add_wait_fork_target_side": str(opportunity["side"]),
            "ema_add_wait_fork_target_campaign_id": int(opportunity["campaign_id"]),
            "ema_add_wait_fork_target_baseline_action": str(opportunity["baseline_action"]),
            "ema_add_wait_fork_target_market_event_index": int(
                opportunity["market_event_generation"]
            ),
            "ema_add_wait_fork_target_generation": {
                target: int(opportunity[source]) for target, source in GENERATION_COLUMNS.items()
            },
            "ema_add_wait_fork_release_ts_ms": int(opportunity["release_ts_ms"]),
            "ema_add_wait_fork_release_market_event_index": int(
                opportunity["release_market_event_generation"]
            ),
            "ema_add_wait_fork_release_generation": {
                target: int(opportunity[f"release_{target}"]) for target in GENERATION_COLUMNS
            },
            "ema_add_wait_fork_release_clock": ("next_ready_market_generation_v1"),
        }
    )
    result = bt._simulate_tick_with_engine(
        "python",
        window.trades,
        window.var_ts_ms,
        window.var_ssq,
        params,
        **shared,
    )
    trace = dict(result.get("_ema_add_wait_fork_trace") or {})
    if trace.get("action") != action or trace.get("side") != opportunity["side"]:
        raise StudyError("fork arm trace identity drifted")
    if int(trace.get("target_market_event_index", -1)) != int(
        opportunity["market_event_generation"]
    ):
        raise StudyError("fork arm target market locator drifted")
    if int(trace.get("frozen_release_ts_ms", 0)) != int(opportunity["release_ts_ms"]) or int(
        trace.get("frozen_release_market_event_index", -1)
    ) != int(opportunity["release_market_event_generation"]):
        raise StudyError("fork arm frozen release locator drifted")
    expected_release_generation = {
        target: int(opportunity[f"release_{target}"]) for target in GENERATION_COLUMNS
    }
    if trace.get("frozen_release_generation") != expected_release_generation:
        raise StudyError("fork arm frozen release generation drifted")
    if trace.get("release_clock") != "next_ready_market_generation_v1":
        raise StudyError("fork arm release clock drifted")
    if bool(trace.get("external_epoch_observed", False)) and (
        int(trace.get("wait_release_ts_ms", 0)) != int(opportunity["release_ts_ms"])
        or trace.get("wait_release_generation") != expected_release_generation
    ):
        raise StudyError("fork arm observed a non-frozen release generation")
    expected_actual = "place" if action == ADD_NOW else "pause"
    if trace.get("target_actual_action") != expected_actual:
        raise StudyError("fork arm did not execute its assigned action")
    if int(trace.get("second_assignment_count", -1)) != 0:
        raise StudyError("fork arm created a second assignment")
    if int(trace.get("terminal_ts_ms", 0)) >= int(opportunity["release_ts_ms"]) and not bool(
        trace.get("external_epoch_observed", False)
    ):
        raise StudyError("fork arm skipped the frozen external epoch")
    if (
        action == WAIT_ONE_EPOCH
        and bool(trace.get("external_epoch_observed", False))
        and not bool(trace.get("wait_released", False))
    ):
        raise StudyError("WAIT arm observed the epoch without releasing")
    if trace.get("arm_washout_complete"):
        for field in (
            "active_or_pending_order_count",
            "pending_submit_count",
            "pending_cancel_count",
            "pending_ack_count",
            "descendant_unterminal_count",
            "cursor_owner_count",
            "hazard_owner_count",
            "hazard_path_count",
        ):
            if int(trace.get(field, -1)) != 0:
                raise StudyError(f"arm washout retained {field}")
        if bool(trace.get("hazard_hold_active", True)):
            raise StudyError("arm washout retained hazard hold")
        if bool(trace.get("campaign_active", True)):
            raise StudyError("arm washout retained an active campaign")
    return trace


def _washout_state(trace: Mapping[str, Any]) -> ArmWashoutState:
    return ArmWashoutState(
        inventory_btc=float(trace["terminal_inventory_btc"]),
        campaign_active=bool(trace["campaign_active"]),
        active_order_count=int(trace["active_or_pending_order_count"]),
        pending_submit_count=int(trace["pending_submit_count"]),
        pending_cancel_count=int(trace["pending_cancel_count"]),
        pending_ack_count=int(trace["pending_ack_count"]),
        descendant_unterminal_count=int(trace["descendant_unterminal_count"]),
        cursor_owner_count=int(trace["cursor_owner_count"]),
        hazard_owner_count=int(trace["hazard_owner_count"]),
        second_assignment_count=int(trace["second_assignment_count"]),
    )


def label_day(day: str, *, output: Path = OUTPUT) -> dict[str, Any]:
    spec, plan = _spec_and_plan()
    _validate_execution_amendment()
    panel_manifest, selected_panel_path = _load_frozen_panel(output)
    panel = pd.read_parquet(selected_panel_path)
    day_panel = panel.loc[panel["utc_day"].eq(day)].copy()
    day_path = output / "labels" / day / "paired_labels.parquet"
    day_manifest_path = output / "labels" / day / "manifest.json"
    if day_path.is_file() and day_manifest_path.is_file():
        manifest = _load_json(day_manifest_path)
        _validate_cached_table(
            manifest=manifest,
            expected_path=day_path,
            output=output,
            day=day,
            expected_spec_sha256=_spec_sha256(),
            expected_plan_sha256=_sha256_file(PLAN),
            expected_amendment_sha256=_execution_amendment_sha256(),
            expected_panel_sha256=panel_manifest["selected_panel_sha256"],
            role=f"{day} labels",
        )
        return manifest
    window, _, base, audit = _load_day_inputs(day, spec=spec, plan=plan)
    paired_rows = []
    panel_sha256 = str(panel_manifest["selected_panel_sha256"])
    for opportunity in day_panel.to_dict("records"):
        arm_traces: dict[str, dict[str, Any]] = {}
        for action in (ADD_NOW, WAIT_ONE_EPOCH):
            checkpoint = _arm_checkpoint(output, opportunity["opportunity_id"], action)
            if checkpoint.is_file():
                arm_traces[action] = _load_arm_checkpoint(
                    checkpoint,
                    output=output,
                    opportunity=opportunity,
                    action=action,
                    panel_sha256=panel_sha256,
                )
                continue
            trace = _run_arm(
                opportunity,
                action,
                window=window,
                base=base,
                shared=audit["shared"],
            )
            _atomic_json(
                checkpoint,
                _checkpoint_payload(
                    opportunity=opportunity,
                    action=action,
                    trace=trace,
                    panel_sha256=panel_sha256,
                ),
            )
            arm_traces[action] = trace
        add = arm_traces[ADD_NOW]
        wait = arm_traces[WAIT_ONE_EPOCH]
        if not math.isclose(
            float(add["assignment_equity_usdc"]),
            float(wait["assignment_equity_usdc"]),
            rel_tol=0.0,
            abs_tol=1e-12,
        ):
            raise StudyError("fork arms do not share assignment equity")
        if (
            bool(add.get("external_epoch_observed", False))
            and bool(wait.get("external_epoch_observed", False))
            and (
                int(add["wait_release_ts_ms"]) != int(wait["wait_release_ts_ms"])
                or add["wait_release_generation"] != wait["wait_release_generation"]
            )
        ):
            raise StudyError("fork arms observed different external releases")
        observed = joint_washout_complete(
            _washout_state(add),
            _washout_state(wait),
        )
        if observed != bool(add["arm_washout_complete"] and wait["arm_washout_complete"]):
            raise StudyError("arm and joint washout contracts disagree")
        add_marking_lower = (
            float(add["decision_to_terminal_value_usdc"])
            if add["arm_washout_complete"]
            else float(add["censor_time_marking_lower_usdc"])
        )
        add_marking_upper = (
            float(add["decision_to_terminal_value_usdc"])
            if add["arm_washout_complete"]
            else float(add["censor_time_marking_upper_usdc"])
        )
        wait_marking_lower = (
            float(wait["decision_to_terminal_value_usdc"])
            if wait["arm_washout_complete"]
            else float(wait["censor_time_marking_lower_usdc"])
        )
        wait_marking_upper = (
            float(wait["decision_to_terminal_value_usdc"])
            if wait["arm_washout_complete"]
            else float(wait["censor_time_marking_upper_usdc"])
        )
        paired = dict(opportunity)
        paired.update(
            {
                "add_arm_terminal_ts_ms": int(add["terminal_ts_ms"]),
                "wait_arm_terminal_ts_ms": int(wait["terminal_ts_ms"]),
                "joint_washout_ts_ms": int(max(add["terminal_ts_ms"], wait["terminal_ts_ms"])),
                "joint_washout_complete": observed,
                "right_censored": not observed,
                "add_value_usdc": add.get("decision_to_terminal_value_usdc"),
                "wait_value_usdc": wait.get("decision_to_terminal_value_usdc"),
                "add_minus_wait_value_usdc": (
                    float(add["decision_to_terminal_value_usdc"])
                    - float(wait["decision_to_terminal_value_usdc"])
                    if observed
                    else math.nan
                ),
                "censor_time_marking_delta_lower_usdc": (
                    add_marking_lower - wait_marking_upper if not observed else math.nan
                ),
                "censor_time_marking_delta_upper_usdc": (
                    add_marking_upper - wait_marking_lower if not observed else math.nan
                ),
                "censor_time_marking_semantics": (
                    "contemporaneous_marks_not_eventual_terminal_bounds"
                    if not observed
                    else "not_applicable"
                ),
                "add_descendant_submit_count": int(add["descendant_submit_count"]),
                "wait_descendant_submit_count": int(wait["descendant_submit_count"]),
                "add_terminal_inventory_btc": float(add["terminal_inventory_btc"]),
                "wait_terminal_inventory_btc": float(wait["terminal_inventory_btc"]),
                "add_boundary_mid_value_usdc": float(add["boundary_mid_value_usdc"]),
                "wait_boundary_mid_value_usdc": float(wait["boundary_mid_value_usdc"]),
                "add_boundary_executable_value_usdc": float(add["boundary_executable_value_usdc"]),
                "wait_boundary_executable_value_usdc": float(
                    wait["boundary_executable_value_usdc"]
                ),
                "add_active_or_pending_order_count": int(add["active_or_pending_order_count"]),
                "wait_active_or_pending_order_count": int(wait["active_or_pending_order_count"]),
                "add_hazard_path_count": int(add["hazard_path_count"]),
                "wait_hazard_path_count": int(wait["hazard_path_count"]),
                "add_hazard_hold_active": bool(add["hazard_hold_active"]),
                "wait_hazard_hold_active": bool(wait["hazard_hold_active"]),
                "joint_terminal_identity": ("absorbing_flat_quarantine_until_later_arm_washout.v1"),
            }
        )
        paired_rows.append(paired)
    frame = pd.DataFrame(paired_rows)
    _atomic_parquet(day_path, frame)
    manifest = {
        "schema_version": f"{SCHEMA_VERSION}.label_day",
        "identity": IDENTITY,
        "utc_day": day,
        "spec_sha256": _spec_sha256(),
        "execution_plan_sha256": _sha256_file(PLAN),
        "execution_amendment_sha256": _execution_amendment_sha256(),
        "panel_sha256": _sha256_file(selected_panel_path),
        "data_path": str(day_path),
        "data_sha256": _sha256_file(day_path),
        "row_count": int(len(frame)),
        "observed_count": int(frame["joint_washout_complete"].sum()) if len(frame) else 0,
        "right_censored_count": int(frame["right_censored"].sum()) if len(frame) else 0,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    _atomic_json(day_manifest_path, manifest)
    return manifest


def run_labels(days: Sequence[str], *, workers: int, output: Path = OUTPUT) -> None:
    if workers <= 1:
        for day in days:
            print(json.dumps(label_day(day, output=output), sort_keys=True))
        return
    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(label_day, day, output=output): day for day in days}
        for future in concurrent.futures.as_completed(futures):
            print(json.dumps(future.result(), sort_keys=True))


def assemble_labels(*, output: Path = OUTPUT) -> pd.DataFrame:
    spec, _ = _spec_and_plan()
    panel_manifest, _ = _load_frozen_panel(output)
    frames = []
    for day in spec["development_denominator"]["ordered_utc_days"]:
        manifest = _load_json(output / "labels" / day / "manifest.json")
        path = Path(manifest["data_path"])
        _validate_cached_table(
            manifest=manifest,
            expected_path=output / "labels" / day / "paired_labels.parquet",
            output=output,
            day=day,
            expected_spec_sha256=_spec_sha256(),
            expected_plan_sha256=_sha256_file(PLAN),
            expected_amendment_sha256=_execution_amendment_sha256(),
            expected_panel_sha256=panel_manifest["selected_panel_sha256"],
            role=f"{day} labels",
        )
        frames.append(pd.read_parquet(path))
    panel = pd.concat(frames, ignore_index=True)
    _atomic_parquet(_artifact_paths(output)["label_panel"], panel)
    return panel


def _weighted_median(values: np.ndarray, weights: np.ndarray) -> float:
    order = np.argsort(values, kind="mergesort")
    ordered_values = values[order]
    ordered_weights = weights[order]
    cutoff = 0.5 * float(ordered_weights.sum())
    index = int(np.searchsorted(np.cumsum(ordered_weights), cutoff, side="left"))
    return float(ordered_values[min(index, len(ordered_values) - 1)])


def _weighted_quantile(values: np.ndarray, weights: np.ndarray, q: float) -> float:
    order = np.argsort(values, kind="mergesort")
    ordered_values = values[order]
    ordered_weights = weights[order]
    cutoff = float(q) * float(ordered_weights.sum())
    index = int(np.searchsorted(np.cumsum(ordered_weights), cutoff, side="left"))
    return float(ordered_values[min(index, len(ordered_values) - 1)])


def _transform(
    train: pd.DataFrame,
    test: pd.DataFrame,
    features: Sequence[str],
    weights: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    train_columns = []
    test_columns = []
    for feature in features:
        train_values = train[feature].to_numpy(dtype=np.float64)
        test_values = test[feature].to_numpy(dtype=np.float64)
        train_missing = ~np.isfinite(train_values)
        test_missing = ~np.isfinite(test_values)
        observed = ~train_missing
        if not observed.any():
            raise StudyError(f"outer train feature is entirely missing: {feature}")
        median = _weighted_median(train_values[observed], weights[observed])
        q25 = _weighted_quantile(train_values[observed], weights[observed], 0.25)
        q75 = _weighted_quantile(train_values[observed], weights[observed], 0.75)
        scale = max(q75 - q25, 1e-12)
        train_filled = np.where(train_missing, median, train_values)
        test_filled = np.where(test_missing, median, test_values)
        train_columns.extend(
            (
                np.clip((train_filled - median) / scale, -8.0, 8.0),
                train_missing.astype(np.float64),
            )
        )
        test_columns.extend(
            (
                np.clip((test_filled - median) / scale, -8.0, 8.0),
                test_missing.astype(np.float64),
            )
        )
    allowed_phases = ("COOLDOWN_ACTIVE", "COOLDOWN_EXPIRED")
    for name, frame in (("outer train", train), ("outer test", test)):
        unknown = sorted(set(frame["cooldown_phase"].astype(str)) - set(allowed_phases))
        if unknown:
            raise StudyError(f"{name} has unknown cooldown phases: {unknown}")
    for phase in allowed_phases:
        train_columns.append(train["cooldown_phase"].eq(phase).to_numpy(dtype=np.float64))
        test_columns.append(test["cooldown_phase"].eq(phase).to_numpy(dtype=np.float64))
    return np.column_stack(train_columns), np.column_stack(test_columns)


def _fit_predict(
    train: pd.DataFrame,
    test: pd.DataFrame,
    features: Sequence[str],
) -> np.ndarray:
    weights = train["campaign_weight"].to_numpy(dtype=np.float64)
    x_train, x_test = _transform(train, test, features, weights)
    model = Ridge(alpha=10.0, fit_intercept=True, solver="svd")
    model.fit(
        x_train,
        train["add_minus_wait_value_usdc"].to_numpy(dtype=np.float64),
        sample_weight=weights,
    )
    return model.predict(x_test)


def _nested_cluster_interval(
    frame: pd.DataFrame,
    column: str,
    *,
    draws: int,
    seed: int,
) -> dict[str, float]:
    rng = np.random.default_rng(seed)
    days = tuple(sorted(frame["utc_day"].unique()))
    estimates = np.empty(draws, dtype=np.float64)
    for draw in range(draws):
        sampled_values = []
        sampled_weights = []
        for day in rng.choice(days, size=len(days), replace=True):
            day_frame = frame.loc[frame["utc_day"].eq(day)]
            campaigns = tuple(day_frame["prospective_campaign_side_id"].unique())
            for campaign in rng.choice(campaigns, size=len(campaigns), replace=True):
                rows = day_frame.loc[day_frame["prospective_campaign_side_id"].eq(campaign)]
                sampled_values.extend(rows[column].to_numpy(dtype=np.float64))
                sampled_weights.extend(rows["campaign_weight"].to_numpy(dtype=np.float64))
        estimates[draw] = np.average(sampled_values, weights=sampled_weights)
    return {
        "mean": float(np.average(frame[column], weights=frame["campaign_weight"])),
        "lcb_95": float(np.quantile(estimates, 0.025)),
        "ucb_95": float(np.quantile(estimates, 0.975)),
    }


def evaluate(*, output: Path = OUTPUT) -> dict[str, Any]:
    spec, _ = _spec_and_plan()
    _validate_execution_amendment()
    panel = assemble_labels(output=output)
    censored = panel.loc[panel["right_censored"].astype(bool)].copy()
    if not censored.empty:
        censor_cells = [
            {
                "side": str(side),
                "cooldown_phase": str(phase),
                "rows": int(count),
            }
            for (side, phase), count in censored.groupby(
                ["side", "cooldown_phase"],
                observed=True,
            )
            .size()
            .items()
        ]
        _atomic_json(
            output / "informative_censoring_failure.json",
            {
                "identity": IDENTITY,
                "right_censored_rows": int(len(censored)),
                "cells": censor_cells,
                "complete_case_training_allowed": False,
            },
        )
        raise StudyError("right-censored labels forbid complete-case M0/M1 evaluation")
    observed = panel.loc[~panel["right_censored"].astype(bool)].copy()
    observed["campaign_weight"] = campaign_unit_weights(
        observed,
        campaign_columns=(
            "utc_day",
            "side",
            "prospective_campaign_side_id",
        ),
    )
    campaign_weight_sums = observed.groupby(
        ["utc_day", "side", "prospective_campaign_side_id"],
        observed=True,
    )["campaign_weight"].sum()
    campaign_weight_error = float((campaign_weight_sums - 1.0).abs().max())
    if campaign_weight_error > 1e-12:
        raise StudyError("campaign total training weight drifted")
    oof_rows = []
    for side in ("SELL", "BUY"):
        side_frame = observed.loc[observed["side"].eq(side)].copy()
        for fold in spec["chronological_oof"]["folds"]:
            test = side_frame.loc[side_frame["utc_day"].isin(fold["test_days"])].copy()
            if test.empty:
                raise StudyError(f"{side} fold {fold['fold']} has no test labels")
            first_test_ts = int(test["ts_ms"].min())
            train = side_frame.loc[
                side_frame["utc_day"].isin(fold["fit_day_candidates_after_day_embargo"])
                & side_frame["joint_washout_ts_ms"].lt(first_test_ts)
            ].copy()
            if train.empty:
                raise StudyError(f"{side} fold {fold['fold']} has no train labels")
            m0 = _fit_predict(train, test, M0_FEATURES)
            m1 = _fit_predict(train, test, M1_FEATURES)
            fold_rows = test[
                [
                    "opportunity_id",
                    "utc_day",
                    "side",
                    "cooldown_phase",
                    "prospective_campaign_side_id",
                    "campaign_weight",
                    "add_minus_wait_value_usdc",
                ]
            ].copy()
            fold_rows["fold"] = int(fold["fold"])
            fold_rows["prediction_m0"] = m0
            fold_rows["prediction_m1"] = m1
            oof_rows.append(fold_rows)
    oof = pd.concat(oof_rows, ignore_index=True)
    if oof["opportunity_id"].duplicated().any():
        raise StudyError("OOF opportunity rows overlap across folds")
    oof["squared_error_reduction"] = (
        oof["add_minus_wait_value_usdc"] - oof["prediction_m0"]
    ) ** 2 - (oof["add_minus_wait_value_usdc"] - oof["prediction_m1"]) ** 2
    oof["absolute_error_reduction"] = (
        oof["add_minus_wait_value_usdc"] - oof["prediction_m0"]
    ).abs() - (oof["add_minus_wait_value_usdc"] - oof["prediction_m1"]).abs()
    oof_path = output / "oof_predictions.parquet"
    _atomic_parquet(oof_path, oof)
    side_reports = {}
    for side in ("SELL", "BUY"):
        rows = oof.loc[oof["side"].eq(side)].copy()
        squared = _nested_cluster_interval(
            rows,
            "squared_error_reduction",
            draws=20_000,
            seed=20_260_809,
        )
        absolute = _nested_cluster_interval(
            rows,
            "absolute_error_reduction",
            draws=20_000,
            seed=20_260_809,
        )
        fold_support = {
            str(fold): int(count)
            for fold, count in rows.groupby("fold", observed=True).size().items()
        }
        side_reports[side] = {
            "oof_rows": int(len(rows)),
            "oof_days": int(rows["utc_day"].nunique()),
            "oof_campaigns": int(rows["prospective_campaign_side_id"].nunique()),
            "fold_support": fold_support,
            "squared_error_reduction": squared,
            "absolute_error_reduction": absolute,
            "m1_incremental_prediction_gate_passed": bool(
                squared["lcb_95"] > 0.0
                and absolute["lcb_95"] > 0.0
                and set(fold_support) == {"1", "2", "3", "4"}
            ),
        }
    paths = _artifact_paths(output)
    report = {
        "schema_version": f"{SCHEMA_VERSION}.report",
        "identity": IDENTITY,
        "status": "development_prediction_evidence_read",
        "spec_sha256": _spec_sha256(),
        "execution_amendment_sha256": _execution_amendment_sha256(),
        "selected_panel_sha256": _sha256_file(paths["selected_panel"]),
        "paired_labels_sha256": _sha256_file(paths["label_panel"]),
        "oof_predictions_path": str(oof_path),
        "oof_predictions_sha256": _sha256_file(oof_path),
        "selected_opportunities": int(len(panel)),
        "observed_labels": int(len(observed)),
        "right_censored_labels": int(panel["right_censored"].sum()),
        "m0_m1_common_row_mismatch_count": 0,
        "campaign_total_weight_max_abs_error": campaign_weight_error,
        "side_reports": side_reports,
        "simultaneous_policy_band_status": "not_run_no_action_authority",
        "f09_registration_authorized": False,
        "action_authorized": False,
        "live_authorized": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    _atomic_json(paths["report"], report)
    return report


def _requested_days(spec: Mapping[str, Any], values: Sequence[str]) -> list[str]:
    frozen = list(spec["development_denominator"]["ordered_utc_days"])
    if not values:
        return frozen
    unknown = sorted(set(values) - set(frozen))
    if unknown:
        raise StudyError(f"requested days outside frozen panel: {unknown}")
    return [day for day in frozen if day in set(values)]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=("preflight", "census", "freeze-panel", "labels", "evaluate"),
    )
    parser.add_argument("--days", nargs="*", default=())
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()
    spec, _ = _spec_and_plan()
    days = _requested_days(spec, args.days)
    if args.command == "preflight":
        amendment = _validate_execution_amendment()
        payload = {
            "identity": IDENTITY,
            "spec_sha256": _spec_sha256(),
            "execution_plan_sha256": _sha256_file(PLAN),
            "days": len(days),
            "economic_outcomes_read": False,
            "execution_amendment_sha256": _execution_amendment_sha256(),
            "execution_artifact_count": len(amendment["artifacts"]),
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
    elif args.command == "census":
        run_census(days, workers=max(1, args.workers), output=args.output)
    elif args.command == "freeze-panel":
        print(json.dumps(freeze_panel(output=args.output), indent=2, sort_keys=True))
    elif args.command == "labels":
        run_labels(days, workers=max(1, args.workers), output=args.output)
    else:
        print(json.dumps(evaluate(output=args.output), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
