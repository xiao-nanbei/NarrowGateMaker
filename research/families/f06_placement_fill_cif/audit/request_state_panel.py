#!/usr/bin/env python3
"""Compile and cache causal cancel-request state for placement lifecycles."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from data_paths import data_root, window_cache_root
from models import backtest_tick as bt
from models.audit.content_addressed_cache import (
    ParquetContentAddressedCache,
    canonical_sha256,
    file_sha256,
)
from models.audit.native_gap_segments import assign_segment
from research.families.f06_placement_fill_cif.audit.full_curve_fill_cif import (
    expand_action_lifecycles,
)
from research.families.f06_placement_fill_cif.audit.paired_order_lifecycle import ACTION_ORDER
from research.families.f06_placement_fill_cif.audit.paired_order_lifecycle_smoke import (
    _individual_trade_identity,
)
from research.families.f06_placement_fill_cif.audit.request_state_features import (
    DEFAULT_WINDOWS_MS,
    SCHEMA_VERSION,
    compute_request_state_features_native,
    flatten_request_state,
)

ROOT = Path(__file__).resolve().parents[4]
DATA_ROOT = data_root(ROOT)
DEFAULT_PLACEMENT_ROOT = (
    DATA_ROOT
    / "reports"
    / "placement_fill_policy_clock_panel_v1_development_20260728"
)
DEFAULT_NORMALIZED_ROOT = (
    DATA_ROOT / "normalized_l2_100ms_v2_minimal141_20260727"
)
DEFAULT_LATENCY_PROFILE = (
    DATA_ROOT
    / "live_calibration_snapshots"
    / "20260719"
    / "day_20260718"
    / "quote_decisions_2026-07-18.csv"
)
DEFAULT_CACHE_ROOT = window_cache_root(ROOT) / "request_state_mechanics_v1"
DEFAULT_GAP_MANIFEST = (
    DATA_ROOT
    / "reports"
    / "minimal_good_day_reaudit_20260727"
    / "order_level_gap_segments_v1"
    / "native_l2_gap_manifest.json"
)
DEFAULT_OUTPUT = (
    DATA_ROOT / "reports" / "placement_fill_request_state_panel_v2_development"
)
PANEL_SCHEMA_VERSION = "placement_fill_request_state_panel.v3"

REQUEST_ACTION_SUFFIXES = (
    "cancel_acked",
    "fill_while_cancel_pending_qty",
    "first_pending_cancel_fill_ts_ns",
    "request_state_observed",
    "request_order_state_before",
    "request_order_age_ms",
    "request_remaining_qty",
    "request_queue_left",
    "request_queue_path_valid",
    "request_native_cancel_count",
    "request_native_cancel_qty",
    "request_native_refill_count",
    "request_native_refill_qty",
    "request_native_level_event_count",
)


def _atomic_json(payload: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _file_identity(path: Path) -> dict[str, Any]:
    path = path.expanduser().resolve()
    stat = path.stat()
    return {
        "path": str(path),
        "size_bytes": int(stat.st_size),
        "sha256": file_sha256(path),
    }


def _native_identity() -> dict[str, Any]:
    import narrowgate_cpp  # type: ignore

    module_path = Path(narrowgate_cpp.__file__).resolve()
    if not hasattr(narrowgate_cpp, "compute_request_state_features"):
        raise RuntimeError("installed native module lacks request_state_features.v1")
    return _file_identity(module_path)


def _normalized_file_identity(
    manifest: Mapping[str, Any],
    *,
    normalized_root: Path,
    day: str,
    kind: str,
) -> dict[str, Any]:
    matches = [
        row
        for row in manifest["files"]
        if str(row.get("day")) == day and str(row.get("kind")) == kind
    ]
    if len(matches) != 1:
        raise RuntimeError(f"normalized manifest has {len(matches)} {kind} rows for {day}")
    row = matches[0]
    path = normalized_root / str(row["destination_relative_path"])
    source = row["source_identity"]
    stat = path.stat()
    if int(stat.st_size) != int(source["size_bytes"]):
        raise RuntimeError(f"normalized {kind} size changed for {day}")
    actual_sha256 = file_sha256(path)
    if actual_sha256 != str(source["sha256"]):
        raise RuntimeError(f"normalized {kind} checksum changed for {day}")
    return {
        "path": str(path),
        "size_bytes": int(stat.st_size),
        "sha256": actual_sha256,
        "manifest_source": str(row["source_label"]),
    }


def _load_market_arrays(
    *,
    day: str,
    normalized_root: Path,
    depth_levels: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    bbo_path = normalized_root / "bbo" / f"BTCUSDC-bbo-{day}.parquet"
    l2_path = normalized_root / "l2" / f"BTCUSDC-l2-{day}.parquet"
    bbo = pd.read_parquet(
        bbo_path,
        columns=[
            "timestamp",
            "best_bid",
            "best_bid_qty",
            "best_ask",
            "best_ask_qty",
        ],
    )
    levels = range(1, int(depth_levels) + 1)
    l2_columns = ["timestamp"]
    for level in levels:
        l2_columns.extend(
            (
                f"bid_px_{level}",
                f"bid_qty_{level}",
                f"ask_px_{level}",
                f"ask_qty_{level}",
            )
        )
    l2 = pd.read_parquet(l2_path, columns=l2_columns)
    bt.configure_symbol("BTCUSDC")
    trades = bt.load_individual_trades(
        days=[day],
        quality_allowed_days=(day,),
    )
    required = {"transact_time", "price", "quantity", "is_buyer_maker"}
    missing = sorted(required - set(trades.columns))
    if missing:
        raise RuntimeError(f"individual trades missing columns: {missing}")
    return bbo, l2, trades


def _request_delay_arrays(
    request_ts_ms: np.ndarray,
    *,
    latency_profile: Path,
    seed: int,
) -> tuple[np.ndarray, np.ndarray, dict[str, Any]]:
    profile = bt._load_exec_book_visibility_profile(latency_profile)
    samples = np.asarray(
        profile["exec_book_visibility_delay_samples_ms"], dtype=np.float64
    )
    delays = np.fromiter(
        (
            bt._exec_book_visibility_delay_ms(
                int(ts),
                mean_ms=float(profile["exec_book_visibility_delay_mean_ms"]),
                jitter_ms=float(profile["exec_book_visibility_delay_jitter_ms"]),
                seed=int(seed),
                samples_ms=samples,
            )
            for ts in request_ts_ms
        ),
        dtype=np.int64,
        count=len(request_ts_ms),
    )
    cutoffs = np.maximum(0, request_ts_ms - delays)
    summary = {
        "profile_id": "aws_tokyo_ec2_2vcpu4g_amazon_linux_20260718",
        "seed": int(seed),
        "sample_count": int(len(samples)),
        "request_count": int(len(request_ts_ms)),
        "delay_mean_ms": float(delays.mean()) if len(delays) else 0.0,
        "delay_p50_ms": float(np.quantile(delays, 0.5)) if len(delays) else 0.0,
        "delay_p95_ms": float(np.quantile(delays, 0.95)) if len(delays) else 0.0,
        "shared_book_trade_delay_path": True,
    }
    return cutoffs, cutoffs.copy(), summary


def _phase_frame(actions: pd.DataFrame, segments: pd.DataFrame) -> pd.DataFrame:
    request = pd.to_numeric(
        actions["cancel_request_ts_ns"], errors="coerce"
    ).fillna(0).to_numpy(np.int64)
    activation = pd.to_numeric(
        actions["activation_ts_ns"], errors="coerce"
    ).fillna(0).to_numpy(np.int64)
    first_fill = pd.to_numeric(
        actions["first_fill_ts_ns"], errors="coerce"
    ).fillna(0).to_numpy(np.int64)
    terminal = pd.to_numeric(
        actions["terminal_ts_ns"], errors="coerce"
    ).fillna(0).to_numpy(np.int64)
    planned_ack = pd.to_numeric(
        actions["cancel_ack_ts_ns"], errors="coerce"
    ).fillna(0).to_numpy(np.int64)
    if "cancel_acked" in actions:
        acked = pd.to_numeric(
            actions["cancel_acked"], errors="coerce"
        ).fillna(0).to_numpy(np.int8) != 0
    else:
        acked = actions["terminal_reason"].astype(str).eq("cancel_ack").to_numpy()
    actual_ack = np.where(acked, planned_ack, 0)
    observation_end = pd.to_numeric(
        actions["observation_end_ts_ns"], errors="coerce"
    ).fillna(0).to_numpy(np.int64)
    terminal_or_end = np.where(terminal > 0, terminal, observation_end)
    active = actions["activation_status"].astype(str).eq("active").to_numpy()
    activation_ms = activation // 1_000_000
    request_ms = request // 1_000_000
    activation_segment = assign_segment(activation_ms, segments)
    request_segment = assign_segment(request_ms, segments)
    segment_end_by_id = {
        int(row.segment_id): int(row.end_ts_ms_exclusive) * 1_000_000
        for row in segments.itertuples(index=False)
    }
    activation_segment_end = np.asarray(
        [segment_end_by_id.get(int(value), 0) for value in activation_segment],
        dtype=np.int64,
    )
    request_segment_end = np.asarray(
        [segment_end_by_id.get(int(value), 0) for value in request_segment],
        dtype=np.int64,
    )
    same_segment = (activation_segment > 0) & (
        activation_segment == request_segment
    )
    scheduled_pre_end = np.minimum(
        np.where(request > 0, request, terminal_or_end),
        np.where(
            activation_segment_end > 0,
            activation_segment_end,
            terminal_or_end,
        ),
    )
    pre_fill_in_segment = (
        active
        & (first_fill > activation)
        & (first_fill > 0)
        & ((request <= 0) | (first_fill < request))
        & (activation_segment > 0)
        & (first_fill < activation_segment_end)
    )
    pre_risk_end = np.minimum(
        scheduled_pre_end,
        np.where(pre_fill_in_segment, first_fill, scheduled_pre_end),
    )
    if "first_pending_cancel_fill_ts_ns" in actions:
        first_pending = pd.to_numeric(
            actions["first_pending_cancel_fill_ts_ns"],
            errors="coerce",
        ).fillna(0).to_numpy(np.int64)
        pending_timestamp_supported = np.ones(len(actions), dtype=np.int8)
    else:
        first_pending = np.where(
            (first_fill >= request) & (first_fill > 0), first_fill, 0
        )
        pending_timestamp_supported = np.zeros(len(actions), dtype=np.int8)
    actual_ack_in_segment = (
        acked
        & (request_segment > 0)
        & (actual_ack >= request)
        & (actual_ack < request_segment_end)
    )
    pending_fill_in_segment = (
        (first_pending > request)
        & (request_segment > 0)
        & (first_pending < request_segment_end)
    )
    pending_event_or_end = np.where(
        pending_fill_in_segment,
        first_pending,
        np.where(actual_ack_in_segment, actual_ack, terminal_or_end),
    )
    pending_end = np.minimum(
        pending_event_or_end,
        np.where(request_segment_end > 0, request_segment_end, pending_event_or_end),
    )
    pending_qty = pd.to_numeric(
        actions["fill_while_cancel_pending_qty"], errors="coerce"
    ).fillna(0.0).to_numpy(float)
    request_observed = pd.to_numeric(
        actions["request_state_observed"], errors="coerce"
    ).fillna(0).to_numpy(np.int8) != 0
    at_request = (
        active
        & request_observed
        & same_segment
        & (request > activation)
        & ((first_fill <= 0) | (first_fill >= request))
        & ((terminal <= 0) | (terminal >= request))
    )
    return pd.DataFrame(
        {
            "activation_segment_id": activation_segment,
            "request_segment_id": request_segment,
            "request_segment_end_ts_ns": request_segment_end,
            "activation_to_request_same_segment": same_segment.astype(np.int8),
            "pre_request_first_fill": (
                pre_fill_in_segment
            ).astype(np.int8),
            "pre_request_observed": (
                active
                & (activation_segment > 0)
                & (pre_risk_end > activation)
            ).astype(np.int8),
            "pre_request_right_censored_by_gap": (
                active
                & (activation_segment_end > 0)
                & (activation_segment_end < request)
            ).astype(np.int8),
            "same_ms_request_fill_ambiguous": (
                (first_fill > 0) & (first_fill == request)
            ).astype(np.int8),
            "request_risk_set": at_request.astype(np.int8),
            "pending_fill_timestamp_supported": pending_timestamp_supported,
            "pending_cancel_fill": (
                (pending_qty > 0.0)
                & pending_fill_in_segment
                & (pending_timestamp_supported != 0)
            ).astype(np.int8),
            "pending_cancel_fill_qty": pending_qty,
            "cancel_ack_observed": actual_ack_in_segment.astype(np.int8),
            "actual_cancel_ack_ts_ns": np.where(
                actual_ack_in_segment, actual_ack, 0
            ),
            "pending_right_censored_by_gap": (
                at_request & (request_segment_end < terminal_or_end)
            ).astype(np.int8),
            "request_to_ack_ms": np.where(
                actual_ack_in_segment,
                np.maximum(0.0, (actual_ack - request) / 1_000_000.0),
                np.nan,
            ),
            "pending_risk_duration_ms": np.maximum(
                0.0, (pending_end - request) / 1_000_000.0
            ),
            "pre_request_exposure_ms": np.maximum(
                0.0, (pre_risk_end - activation) / 1_000_000.0
            ),
        }
    )


def _expand_request_actions(placement: pd.DataFrame) -> pd.DataFrame:
    """Expand a paired cohort once per action with action-specific state."""

    actions = expand_action_lifecycles(placement)
    for name in (
        "decision_id",
        "campaign_id",
        "cancel_request_reason",
        "quantity",
        "baseline_price_tick",
    ):
        if name not in placement:
            raise ValueError(f"placement mechanics lack common field: {name}")
        actions[name] = pd.concat(
            [placement[name]] * len(ACTION_ORDER), ignore_index=True
        )
    for suffix in REQUEST_ACTION_SUFFIXES:
        columns = [f"{action}__{suffix}" for action in ACTION_ORDER]
        missing = sorted(set(columns) - set(placement.columns))
        if missing:
            raise ValueError(
                "placement mechanics lack request lifecycle fields: "
                + ", ".join(missing)
            )
        actions[suffix] = pd.concat(
            [placement[column] for column in columns],
            ignore_index=True,
        )
    actions["distance_delta_ticks"] = actions["action"].map(
        {
            "closer_1tick": -1,
            "current": 0,
            "farther_1tick": 1,
        }
    ).astype(np.int8)
    return actions


def _compile_day(
    day: str,
    *,
    placement_root: Path,
    normalized_root: Path,
    normalized_manifest_path: Path,
    latency_profile: Path,
    gap_manifest_path: Path,
    latency_seed: int,
    cache_root: Path,
    windows_ms: Sequence[int],
    depth_levels: int,
    l2_path_lookback_ms: int,
) -> dict[str, Any]:
    placement_path = placement_root / "partitions" / f"day={day}" / "placement.parquet"
    placement_manifest_path = placement_path.with_name("manifest.json")
    if not placement_path.is_file() or not placement_manifest_path.is_file():
        raise FileNotFoundError(f"missing placement mechanics partition for {day}")
    placement_manifest = json.loads(placement_manifest_path.read_text(encoding="utf-8"))
    if placement_manifest.get("panel_sha256") != file_sha256(placement_path):
        raise RuntimeError(f"placement checksum mismatch for {day}")
    normalized_manifest = json.loads(normalized_manifest_path.read_text(encoding="utf-8"))
    bbo_identity = _normalized_file_identity(
        normalized_manifest, normalized_root=normalized_root, day=day, kind="bbo"
    )
    l2_identity = _normalized_file_identity(
        normalized_manifest, normalized_root=normalized_root, day=day, kind="l2"
    )
    trade_identity = _individual_trade_identity("BTCUSDC", day)
    implementation = {
        name: _file_identity(path)
        for name, path in {
            "request_state_panel": Path(__file__),
            "request_state_features_python": Path(__file__).with_name("request_state_features.py"),
            "request_state_features_cpp": ROOT / "cpp" / "narrowgate_cpp" / "request_state_features.cpp",
            "request_state_features_hpp": ROOT / "cpp" / "narrowgate_cpp" / "request_state_features.hpp",
            "full_curve_fill_cif": Path(__file__).with_name("full_curve_fill_cif.py"),
            "paired_order_lifecycle": Path(__file__).with_name("paired_order_lifecycle.py"),
        }.items()
    }
    identity = {
        "schema_version": PANEL_SCHEMA_VERSION,
        "day": day,
        "placement_panel_sha256": placement_manifest["panel_sha256"],
        "normalized_manifest_sha256": file_sha256(normalized_manifest_path),
        "bbo": bbo_identity,
        "l2": l2_identity,
        "individual_trades": trade_identity,
        "latency_profile": _file_identity(latency_profile),
        "gap_manifest": _file_identity(gap_manifest_path),
        "latency_seed": int(latency_seed),
        "visibility_semantics": "event_ts_strictly_before_request_minus_sampled_delay",
        "windows_ms": [int(value) for value in windows_ms],
        "depth_levels": int(depth_levels),
        "l2_path_lookback_ms": int(l2_path_lookback_ms),
        "native_module": _native_identity(),
        "implementation": implementation,
    }
    cache = ParquetContentAddressedCache(cache_root, namespace="day")
    cached = cache.load(identity)
    if cached is not None:
        return {
            "day": day,
            "cache_key": cached.key,
            "cache_hit": True,
            "rows": int(len(cached.frame)),
            "payload_path": str(cached.entry_dir / "payload.parquet"),
            "payload_sha256": str(cached.manifest["payload_sha256"]),
            "identity_sha256": canonical_sha256(identity),
        }

    placement = pd.read_parquet(placement_path)
    placement = placement.sort_values(
        ["cancel_request_ts_ns", "side", "cohort_id"], kind="stable"
    ).reset_index(drop=True)
    request_ts_ms = (
        pd.to_numeric(placement["cancel_request_ts_ns"], errors="raise").to_numpy(np.int64)
        // 1_000_000
    )
    book_cutoff, trade_cutoff, latency_summary = _request_delay_arrays(
        request_ts_ms,
        latency_profile=latency_profile,
        seed=int(latency_seed),
    )
    gap_manifest = json.loads(gap_manifest_path.read_text(encoding="utf-8"))
    segments_path = Path(gap_manifest["segments_path"])
    if file_sha256(segments_path) != str(gap_manifest["segments_sha256"]):
        raise RuntimeError("gap segment registry checksum mismatch")
    segments = pd.read_csv(segments_path)
    segments = segments.loc[segments["day"].astype(str) == day].copy()
    if segments.empty:
        raise RuntimeError(f"gap segment registry lacks {day}")
    bbo, l2, trades = _load_market_arrays(
        day=day, normalized_root=normalized_root, depth_levels=int(depth_levels)
    )
    bid_px = [f"bid_px_{level}" for level in range(1, int(depth_levels) + 1)]
    bid_qty = [f"bid_qty_{level}" for level in range(1, int(depth_levels) + 1)]
    ask_px = [f"ask_px_{level}" for level in range(1, int(depth_levels) + 1)]
    ask_qty = [f"ask_qty_{level}" for level in range(1, int(depth_levels) + 1)]
    result = compute_request_state_features_native(
        request_ts_ms=request_ts_ms,
        book_cutoff_ts_ms=book_cutoff,
        trade_cutoff_ts_ms=trade_cutoff,
        activation_ts_ms=placement["current__activation_ts_ns"].to_numpy(np.int64) // 1_000_000,
        terminal_ts_ms=placement["current__terminal_ts_ns"].to_numpy(np.int64) // 1_000_000,
        cancel_ack_ts_ms=placement["current__cancel_ack_ts_ns"].to_numpy(np.int64) // 1_000_000,
        bbo_ts_ms=bbo["timestamp"].to_numpy(np.int64),
        bbo_best_bid=bbo["best_bid"].to_numpy(float),
        bbo_best_ask=bbo["best_ask"].to_numpy(float),
        bbo_bid_qty=bbo["best_bid_qty"].to_numpy(float),
        bbo_ask_qty=bbo["best_ask_qty"].to_numpy(float),
        l2_ts_ms=l2["timestamp"].to_numpy(np.int64),
        l2_bid_px=l2[bid_px].to_numpy(float),
        l2_bid_qty=l2[bid_qty].to_numpy(float),
        l2_ask_px=l2[ask_px].to_numpy(float),
        l2_ask_qty=l2[ask_qty].to_numpy(float),
        trade_ts_ms=trades["transact_time"].to_numpy(np.int64),
        trade_price=trades["price"].to_numpy(float),
        trade_qty=trades["quantity"].to_numpy(float),
        is_buyer_maker=trades["is_buyer_maker"].to_numpy(np.uint8),
        windows_ms=windows_ms,
        tick_size=0.1,
        depth_levels=int(depth_levels),
        l2_path_lookback_ms=int(l2_path_lookback_ms),
    )
    features = flatten_request_state(result)
    actions = _expand_request_actions(placement)
    shared_features = pd.concat(
        [features] * len(ACTION_ORDER), ignore_index=True
    )
    frame = pd.concat(
        [
            actions.reset_index(drop=True),
            _phase_frame(actions, segments),
            shared_features,
        ],
        axis=1,
    )
    side = frame["side"].astype(str).str.upper()
    order_price = pd.to_numeric(frame["price_tick"], errors="coerce").fillna(0)
    request_bid_tick = np.rint(
        pd.to_numeric(frame["request_best_bid"], errors="coerce").fillna(0.0)
        / 0.1
    )
    request_ask_tick = np.rint(
        pd.to_numeric(frame["request_best_ask"], errors="coerce").fillna(0.0)
        / 0.1
    )
    frame["request_order_distance_to_same_side_bbo_ticks"] = np.where(
        side.eq("BUY"),
        request_bid_tick - order_price,
        order_price - request_ask_tick,
    ).astype(np.float32)
    frame["request_feature_ready_ts_ns"] = frame["cancel_request_ts_ns"]
    expanded_book_cutoff = np.tile(book_cutoff, len(ACTION_ORDER))
    expanded_trade_cutoff = np.tile(trade_cutoff, len(ACTION_ORDER))
    frame["request_book_cutoff_ts_ms"] = expanded_book_cutoff
    frame["request_trade_cutoff_ts_ms"] = expanded_trade_cutoff
    segment_start_by_id = {
        int(row.segment_id): int(row.start_ts_ms)
        for row in segments.itertuples(index=False)
    }
    request_segment_start = np.asarray(
        [
            segment_start_by_id.get(int(value), 0)
            for value in frame["request_segment_id"].to_numpy(np.int64)
        ],
        dtype=np.int64,
    )
    frame["request_feature_segment_valid"] = (
        (frame["request_risk_set"].to_numpy(np.int8) != 0)
        & (frame["request_valid_book"].to_numpy(np.int8) != 0)
        & (frame["request_book_source_ts_ms"].to_numpy(np.int64) >= request_segment_start)
        & (expanded_book_cutoff >= request_segment_start)
        & (expanded_trade_cutoff >= request_segment_start)
    ).astype(np.int8)
    frame["request_model_risk_set"] = (
        (frame["request_risk_set"].to_numpy(np.int8) != 0)
        & (frame["request_feature_segment_valid"].to_numpy(np.int8) != 0)
        & (frame["pending_fill_timestamp_supported"].to_numpy(np.int8) != 0)
        & (frame["same_ms_request_fill_ambiguous"].to_numpy(np.int8) == 0)
    ).astype(np.int8)
    expanded_request_ts_ms = frame["cancel_request_ts_ns"].to_numpy(
        np.int64
    ) // 1_000_000
    causality_mask = (
        (expanded_request_ts_ms > 0)
        & (frame["request_valid_book"].to_numpy(np.int8) != 0)
    )
    if not bool(
        (
            frame.loc[causality_mask, "request_book_source_ts_ms"].to_numpy(
                np.int64
            )
            < expanded_request_ts_ms[causality_mask]
        ).all()
    ):
        raise RuntimeError(f"same/future-ms book event leaked into request state for {day}")
    stored = cache.store(
        identity,
        frame,
        metadata={
            "engine": "narrowgate_cpp",
            "request_state_schema_version": SCHEMA_VERSION,
            "latency": latency_summary,
            "actions": list(ACTION_ORDER),
            "cohort_rows": int(len(placement)),
            "pre_request_fill_rows": int(frame["pre_request_first_fill"].sum()),
            "pending_cancel_fill_rows": int(frame["pending_cancel_fill"].sum()),
            "cancel_ack_rows": int(frame["cancel_ack_observed"].sum()),
        },
    )
    return {
        "day": day,
        "cache_key": stored.key,
        "cache_hit": bool(stored.hit),
        "rows": int(len(frame)),
        "payload_path": str(stored.entry_dir / "payload.parquet"),
        "payload_sha256": str(stored.manifest["payload_sha256"]),
        "identity_sha256": canonical_sha256(identity),
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--placement-root", type=Path, default=DEFAULT_PLACEMENT_ROOT)
    parser.add_argument("--normalized-root", type=Path, default=DEFAULT_NORMALIZED_ROOT)
    parser.add_argument("--latency-profile", type=Path, default=DEFAULT_LATENCY_PROFILE)
    parser.add_argument("--gap-manifest", type=Path, default=DEFAULT_GAP_MANIFEST)
    parser.add_argument("--latency-seed", type=int, default=20260718)
    parser.add_argument("--cache-root", type=Path, default=DEFAULT_CACHE_ROOT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--days", nargs="*", default=[])
    parser.add_argument("--windows-ms", nargs="+", type=int, default=list(DEFAULT_WINDOWS_MS))
    parser.add_argument("--depth-levels", type=int, default=5)
    parser.add_argument("--l2-path-lookback-ms", type=int, default=1_000)
    parser.add_argument("--workers", type=int, default=1)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    for name in (
        "placement_root",
        "normalized_root",
        "latency_profile",
        "gap_manifest",
        "cache_root",
        "output_dir",
    ):
        setattr(args, name, getattr(args, name).expanduser().resolve())
    normalized_manifest = args.normalized_root / "manifest.json"
    for path in (
        args.placement_root,
        args.normalized_root,
        args.latency_profile,
        args.gap_manifest,
        normalized_manifest,
    ):
        if not path.exists():
            raise SystemExit(f"missing required input: {path}")
    available = sorted(
        path.parent.name.removeprefix("day=")
        for path in args.placement_root.glob("partitions/day=*/placement.parquet")
    )
    days = [str(day) for day in args.days] if args.days else available
    missing = sorted(set(days) - set(available))
    if missing:
        raise SystemExit(f"placement mechanics are unavailable for: {missing}")
    windows = sorted({int(value) for value in args.windows_ms})
    if not windows or windows[0] <= 0:
        raise SystemExit("request-state windows must be positive")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    kwargs = {
        "placement_root": args.placement_root,
        "normalized_root": args.normalized_root,
        "normalized_manifest_path": normalized_manifest,
        "latency_profile": args.latency_profile,
        "gap_manifest_path": args.gap_manifest,
        "latency_seed": int(args.latency_seed),
        "cache_root": args.cache_root,
        "windows_ms": windows,
        "depth_levels": int(args.depth_levels),
        "l2_path_lookback_ms": int(args.l2_path_lookback_ms),
    }
    workers = max(1, min(int(args.workers), len(days)))
    if workers == 1:
        records = [_compile_day(day, **kwargs) for day in days]
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as pool:
            records = list(pool.map(_compile_worker, [(day, kwargs) for day in days]))
    records = sorted(records, key=lambda row: row["day"])
    index = pd.DataFrame(records)
    temporary = args.output_dir / "development_index.csv.tmp"
    index.to_csv(temporary, index=False)
    temporary.replace(args.output_dir / "development_index.csv")
    manifest = {
        "schema_version": PANEL_SCHEMA_VERSION,
        "status": "request_state_mechanics_complete_prediction_not_evaluated",
        "days": days,
        "day_count": int(len(days)),
        "rows": int(index["rows"].sum()),
        "cache_hit_count": int(index["cache_hit"].sum()),
        "cache_root": str(args.cache_root),
        "normalized_manifest": _file_identity(normalized_manifest),
        "latency_profile": _file_identity(args.latency_profile),
        "gap_manifest": _file_identity(args.gap_manifest),
        "latency_seed": int(args.latency_seed),
        "windows_ms": windows,
        "depth_levels": int(args.depth_levels),
        "request_state_schema_version": SCHEMA_VERSION,
        "action_or_live_authorization": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    manifest["manifest_identity_sha256"] = canonical_sha256(manifest)
    _atomic_json(manifest, args.output_dir / "manifest.json")
    print(json.dumps({"days": len(days), "rows": manifest["rows"], "cache_hits": manifest["cache_hit_count"]}, sort_keys=True))
    return 0


def _compile_worker(item: tuple[str, Mapping[str, Any]]) -> dict[str, Any]:
    day, kwargs = item
    return _compile_day(day, **dict(kwargs))


if __name__ == "__main__":
    raise SystemExit(main())
