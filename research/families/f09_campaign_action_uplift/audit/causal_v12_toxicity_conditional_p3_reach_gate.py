#!/usr/bin/env python3
"""Freeze and evaluate a v12-toxicity outward quote action gated by P3 reach.

The independent action proposes a symmetric 16-tick outward move for
exposure-increasing BUY/SELL quotes when the side-specific v12 toxicity score
exceeds a strictly past-only p90. Conditional P3 v4.1 does not generate the
quote. It only admits mechanically supported finite-distance proposals.

The C++ path in this module is a fast full-path screen. It cannot grant live
authority: a positive screen must still pass the authoritative Python replay,
Python/C++ policy parity, production preflight, and rollback gates.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import math
import os
import tempfile
import time
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from data_paths import (
    IMMUTABLE_BACKTEST_V12_CONFIG_SHA256,
    data_root,
    immutable_backtest_v12_config_path,
)
from models.backtest_config import load_operational_baseline_binding

ROOT = Path(__file__).resolve().parents[4]
DATA_ROOT = data_root(ROOT)
IDENTITY = "causal_v12_toxicity_outward_16tick_conditional_p3_reach_gate_v1"
SCHEMA_VERSION = "narrowgate_causal_v12_toxicity_conditional_p3_reach_gate.v1"
CREATED_DATE = "2026-08-04"

BASELINE_POINTER = (
    ROOT
    / "research/families/f10_live_replay_attribution/docs/"
    "operational_baseline_current.json"
)
PANEL_SPEC = (
    ROOT
    / "research/families/f09_campaign_action_uplift/docs/"
    "causal_v12_ranked_toxicity_exposure_guard_carryover_safe_v2_mechanics_spec_20260803.json"
)
P3_TRANSPORT_SPEC = (
    ROOT
    / "research/families/f02_empirical_p3_touch/docs/"
    "p3_touch_policy_visible_decision_cadence_transport_v1_spec_20260803.json"
)
MODEL_DIR = (
    ROOT
    / "models/saved_btcusdc_causal_v12_expanded_source_aware_semantics_v6_20260802_live_canary"
)
FEATURE_DIR = (
    DATA_ROOT / "features_btcusdc_causal_v12_ranked_toxicity_f09_40d_20260802"
)
BOOK_ROOT = DATA_ROOT / "normalized_l2_100ms_v2_20260727"
QUEUE_PATH = (
    DATA_ROOT
    / "reports/formal_recalibration_20260715/"
    "BTCUSDC-queue-calibration-v3-fit-20260710_11-q070.json"
)
LATENCY_PATH = (
    DATA_ROOT
    / "reports/formal_recalibration_20260715/"
    "ec2_aws_tokyo_2vcpu4g_20260710_14_rest_latency.csv.gz"
)
TRADE_MANIFEST = DATA_ROOT / "trade_features_causal_v3_20260727/manifest.json"
TRADE_QUALITY = (
    DATA_ROOT
    / "reports/causal_v9_through_20260725_20260727/execution_trade_quality.csv"
)
CACHE_DIR = DATA_ROOT / "cache/action_bound_p3_v1/window_cache"
OUTPUT_ROOT = (
    DATA_ROOT
    / "reports/causal_v12_toxicity_outward_16tick_conditional_p3_reach_gate_v1_20260804"
)
SPEC_PATH = (
    ROOT
    / "research/families/f09_campaign_action_uplift/docs/"
    "causal_v12_toxicity_outward_16tick_conditional_p3_reach_gate_v1_spec_20260804.json"
)

OUTWARD_TICKS = 16
P3_DISTANCE_MIN_TICKS = 5
P3_DISTANCE_MAX_TICKS = 1_200 - OUTWARD_TICKS
REACH_CHANGE_MIN = 0.0005
REACH_CHANGE_MAX = 0.02
TOXICITY_QUANTILE = 0.90
MINIMUM_PRIOR_DAYS = 5
MINIMUM_PRIOR_BUCKETS = 500
PREDICTED_DELTA_BINS = 5
BOOTSTRAP_DRAWS = 20_000
BOOTSTRAP_SEED = 20260804
ECONOMIC_BOOTSTRAP_DRAWS = 20_000
ECONOMIC_BOOTSTRAP_SEED = 2026080417
TRACE_FILLS_MAX = 100_000
P3_PREDICTION_CONTEXT_CHUNK_ROWS = 96
SIDES = ("BUY", "SELL")
ROLES = ("opener", "add")
ARMS = ("control", "candidate")
BLOCK_INDEX = {
    ("BUY", "opener"): 0,
    ("BUY", "add"): 1,
    ("SELL", "opener"): 2,
    ("SELL", "add"): 3,
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(payload: Any) -> str:
    raw = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
        temporary = Path(handle.name)
    os.replace(temporary, path)


def atomic_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
    try:
        frame.to_parquet(temporary, index=False)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_npz(path: Path, **arrays: np.ndarray) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
        np.savez_compressed(handle, **arrays)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def require_file(path: Path, expected_sha256: str | None = None) -> Path:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(resolved)
    if expected_sha256 is not None and sha256_file(resolved) != expected_sha256:
        raise ValueError(f"SHA256 mismatch: {resolved}")
    return resolved


def load_panel() -> tuple[list[str], set[str], set[str]]:
    payload = json.loads(PANEL_SPEC.read_text(encoding="utf-8"))
    panel = payload["panels"]
    days = [str(value) for value in panel["development_days"]]
    grade_a = {str(value) for value in panel["grade_a_days"]}
    grade_b = {str(value) for value in panel["grade_b_days"]}
    if days != sorted(set(days)) or len(days) != 40:
        raise ValueError("frozen Development panel must contain 40 ordered days")
    if grade_a & grade_b or grade_a | grade_b != set(days):
        raise ValueError("Grade-A/Grade-B partition drift")
    return days, grade_a, grade_b


def validate_current_baseline() -> dict[str, Any]:
    binding = load_operational_baseline_binding(
        root=ROOT,
        pointer_path=BASELINE_POINTER,
    )
    if binding is None or not bool(binding.get("config_exists")):
        raise ValueError("immutable v12 backtest config is unavailable")
    pointer = dict(binding["pointer"])
    if str(pointer.get("live_config_sha256", "")) != (
        IMMUTABLE_BACKTEST_V12_CONFIG_SHA256
    ):
        raise ValueError("normalized backtest pointer does not bind immutable v12")
    identity_path = require_file(
        Path(binding["identity_path"]),
        str(binding["identity_sha256"]),
    )
    config_path = require_file(
        Path(binding["config_path"]),
        IMMUTABLE_BACKTEST_V12_CONFIG_SHA256,
    )
    governance_pointer = binding.get("governance_pointer") or pointer
    if governance_pointer.get("schema_version") == (
        "narrowgate_operational_baseline_pointer.v2"
    ) and (
        binding.get("config_scope") != "immutable_v12_backtest_default"
        or config_path != immutable_backtest_v12_config_path(root=ROOT)
    ):
        raise ValueError("v13 governance did not resolve the immutable v12 replay config")
    require_file(MODEL_DIR / "bundle_meta.json", str(pointer["bundle_meta_sha256"]))
    expected = {
        "ml_enabled": True,
        "dynamic_fill_hazard_shadow_enabled": True,
        "dynamic_fill_hazard_action_enabled": False,
        "buy_fill_selection_shadow_enabled": False,
        "buy_fill_selection_live_enabled": False,
    }
    for key, value in expected.items():
        if bool(pointer.get(key)) != value:
            raise ValueError(f"current operational baseline flag drift: {key}")
    return {
        "pointer": pointer,
        "identity_path": identity_path,
        "config_path": config_path,
    }


def build_params(day: str, config_path: Path) -> dict[str, Any]:
    from models import backtest_tick as bt
    from models.backtest_config import (
        load_tick_base_params,
        validate_formal_replay_calibration,
    )
    from models.replay_contract import configure_fixed_latency_distribution

    bt.BBO_DIR = BOOK_ROOT / "bbo"
    bt.L2_DIR = BOOK_ROOT / "l2"
    bt.configure_symbol("BTCUSDC", model_dir_override=MODEL_DIR)
    params = load_tick_base_params(
        symbol="BTCUSDC",
        config_path=config_path,
        configure_symbol=bt.configure_symbol,
        require_historical_bbo=True,
        queue_calibration_path=QUEUE_PATH,
        strict_calibration=True,
    )
    params.update(
        {
            "execution_trade_source": "trades",
            "market_context_warmup_days": 1,
            "replay_event_clock": "merged",
            "replay_clock_interval_ms": 100,
            "queue_ahead_mode": "exact_level",
            "queue_l2_cancel_ahead_enabled": False,
            "collect_curves": False,
            "trace_quotes_max": 0,
            "trace_fills_max": 0,
            "trace_p3_reach_decisions_max": 25_000,
            "dynamic_fill_hazard_shadow_enabled": False,
            "dynamic_fill_hazard_action_enabled": False,
            "dynamic_fill_hazard_cpp_parity_enabled": False,
            "buy_fill_selection_shadow_enabled": False,
            "buy_fill_selection_live_enabled": False,
            "ml_enabled": True,
            "model_dir": str(MODEL_DIR),
            "resolved_model_dir": str(MODEL_DIR),
            "markout_side_asymmetry_sign": 1.0,
            "window_cache_write_enabled": True,
            "legacy_monolithic_window_cache_write_enabled": False,
            "sync_adjust_replay_mode": "disabled",
            "replay_purpose": "conditional_p3_reach_gate_mechanics",
            "replay_promotion_eligible": False,
            "_formal_quality_allowed_days": [
                (date.fromisoformat(day) - timedelta(days=1)).isoformat(),
                day,
            ],
            "individual_trades_manifest_path": str(TRADE_MANIFEST),
            "individual_trades_manifest_sha256": sha256_file(TRADE_MANIFEST),
            "individual_trades_integrity_report_path": str(TRADE_QUALITY),
            "individual_trades_integrity_report_sha256": sha256_file(TRADE_QUALITY),
        }
    )
    samples = bt._load_live_perf_latency_samples(LATENCY_PATH, mode="avg")
    params["_new_order_latency_samples_ms"] = samples["new_order_latency_samples_ms"]
    params["_cancel_order_latency_samples_ms"] = samples[
        "cancel_order_latency_samples_ms"
    ]
    configure_fixed_latency_distribution(
        params,
        scenario="baseline",
        profile_id="aws_tokyo_2vcpu4g_amzn2023_rest_20260710_14",
        environment="aws-ap-northeast-1-tokyo",
        baseline_clip_quantile=0.99,
    )
    validate_formal_replay_calibration(params, require_latency=True)
    if bool(params.get("buy_fill_selection_live_enabled")):
        raise ValueError("current baseline unexpectedly enables BUY fill selection")
    if bool(params.get("dynamic_fill_hazard_action_enabled")):
        raise ValueError("current baseline unexpectedly enables q90 action")
    return params


def load_window(day: str, params: Mapping[str, Any]):
    from models import backtest_tick as bt
    from models.data_windows import load_tick_window

    bt.BBO_DIR = BOOK_ROOT / "bbo"
    bt.L2_DIR = BOOK_ROOT / "l2"
    bt.configure_symbol("BTCUSDC", model_dir_override=MODEL_DIR)
    return load_tick_window(
        day,
        dict(params),
        load_ml=True,
        require_ml=True,
        run_ml_inference=True,
        feature_dir=FEATURE_DIR,
        require_target_feature_files=True,
        cross_market_enabled=True,
        with_ml_cache=False,
        require_historical_bbo=True,
        require_formal_l2=False,
        cache_dir=CACHE_DIR,
        refresh_cache=False,
    )


def baseline_trace_task(payload: Mapping[str, Any]) -> dict[str, Any]:
    from models import backtest_tick as bt

    day = str(payload["day"])
    output_path = Path(str(payload["output_path"])).resolve()
    config_path = Path(str(payload["config_path"])).resolve()
    params = build_params(day, config_path)
    window = load_window(day, params)
    started = time.perf_counter()
    result = bt._simulate_tick_with_engine(
        "cpp",
        window.trades,
        window.var_ts_ms,
        window.var_ssq,
        params,
        ml_data=window.ml_data,
        bbo_data=window.bbo_data,
        l2_data=window.l2_data,
        var_ti=window.var_ti,
        var_retsq=window.var_retsq,
    )
    trace = pd.DataFrame(result.get("_p3_reach_decision_trace") or [])
    required = {
        "prediction_ts_ms",
        "side",
        "role",
        "baseline_eligible",
        "toxicity_score",
        "baseline_distance_ticks",
    }
    if trace.empty or not required.issubset(trace.columns):
        raise RuntimeError(f"{day} baseline mechanics trace is empty or incomplete")
    trace.insert(0, "day", day)
    trace = trace.loc[
        trace["baseline_eligible"].astype(bool)
        & trace["side"].isin(SIDES)
        & trace["role"].isin(ROLES)
    ].copy()
    if trace.duplicated(["prediction_ts_ms", "side"]).any():
        raise RuntimeError(f"{day} baseline trace duplicates a side/prediction bucket")
    if not np.isfinite(trace["toxicity_score"].to_numpy(dtype=float)).all():
        raise RuntimeError(f"{day} baseline trace contains nonfinite toxicity")
    atomic_parquet(output_path, trace)
    return {
        "day": day,
        "rows": int(len(trace)),
        "buy_rows": int(trace["side"].eq("BUY").sum()),
        "sell_rows": int(trace["side"].eq("SELL").sum()),
        "minimum_distance_ticks": int(trace["baseline_distance_ticks"].min()),
        "maximum_distance_ticks": int(trace["baseline_distance_ticks"].max()),
        "source_authority": str(window.book_source_authority),
        "trace_path": str(output_path),
        "trace_sha256": sha256_file(output_path),
        "runtime_s": time.perf_counter() - started,
        "economic_fields_read": False,
    }


def build_threshold_schedule(days: Sequence[str], trace_dir: Path) -> pd.DataFrame:
    prior: dict[str, list[pd.DataFrame]] = {side: [] for side in SIDES}
    rows: list[dict[str, Any]] = []
    for day in days:
        current = pd.read_parquet(
            trace_dir / f"day={day}.parquet",
            columns=["day", "side", "prediction_ts_ms", "toxicity_score"],
        )
        for side in SIDES:
            past = prior[side]
            prior_days = len(past)
            prior_scores = (
                pd.concat(past, ignore_index=True)["toxicity_score"].to_numpy(dtype=float)
                if past
                else np.asarray([], dtype=np.float64)
            )
            ready = (
                prior_days >= MINIMUM_PRIOR_DAYS
                and len(prior_scores) >= MINIMUM_PRIOR_BUCKETS
            )
            threshold = (
                float(np.quantile(prior_scores, TOXICITY_QUANTILE, method="higher"))
                if ready
                else math.nan
            )
            rows.append(
                {
                    "day": day,
                    "side": side,
                    "prior_days": prior_days,
                    "prior_buckets": int(len(prior_scores)),
                    "quantile": TOXICITY_QUANTILE,
                    "threshold": threshold,
                    "ready": bool(ready),
                }
            )
            part = current.loc[current["side"].eq(side)].copy()
            if not part.empty:
                prior[side].append(part)
    return pd.DataFrame(rows)


def prepare_baseline(*, workers: int) -> None:
    baseline = validate_current_baseline()
    days, _, _ = load_panel()
    trace_dir = OUTPUT_ROOT / "baseline_mechanics"
    trace_dir.mkdir(parents=True, exist_ok=True)
    tasks = [
        {
            "day": day,
            "config_path": str(baseline["config_path"]),
            "output_path": str(trace_dir / f"day={day}.parquet"),
        }
        for day in days
    ]
    summaries: list[dict[str, Any]] = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {pool.submit(baseline_trace_task, task): task["day"] for task in tasks}
        for future in concurrent.futures.as_completed(futures):
            row = future.result()
            summaries.append(row)
            print(
                f"baseline mechanics {len(summaries)}/{len(tasks)} {row['day']} "
                f"rows={row['rows']} runtime={row['runtime_s']:.2f}s",
                flush=True,
            )
    summaries.sort(key=lambda row: str(row["day"]))
    manifest = pd.DataFrame(summaries)
    atomic_parquet(OUTPUT_ROOT / "baseline_mechanics_manifest.parquet", manifest)
    schedule = build_threshold_schedule(days, trace_dir)
    atomic_parquet(OUTPUT_ROOT / "toxicity_p90_schedule.parquet", schedule)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "stage": "baseline_mechanics_before_economic_outcomes",
        "days": days,
        "rows": int(manifest["rows"].sum()),
        "distance_support_observed_ticks": [
            int(manifest["minimum_distance_ticks"].min()),
            int(manifest["maximum_distance_ticks"].max()),
        ],
        "threshold_ready_days": int(
            schedule.groupby("day")["ready"].all().sum()
        ),
        "trace_manifest": {
            "path": str(OUTPUT_ROOT / "baseline_mechanics_manifest.parquet"),
            "sha256": sha256_file(OUTPUT_ROOT / "baseline_mechanics_manifest.parquet"),
        },
        "threshold_schedule": {
            "path": str(OUTPUT_ROOT / "toxicity_p90_schedule.parquet"),
            "sha256": sha256_file(OUTPUT_ROOT / "toxicity_p90_schedule.parquet"),
        },
        "economic_outcomes_read": False,
        "action_or_live_authority": False,
    }
    atomic_json(OUTPUT_ROOT / "baseline_mechanics_report.json", payload)


def p3_band_day_task(payload: Mapping[str, Any]) -> dict[str, Any]:
    from research.families.f02_empirical_p3_touch.audit.p3_touch_decision_cadence_context import (
        DECISION_CONTEXT_FIELDS,
        load_f06_baseline_eligible_decisions,
    )
    from research.families.f02_empirical_p3_touch.audit.p3_touch_decision_cadence_transport import (
        DecisionCadenceOOFModels,
        load_official_aggressive_trades,
        strict_future_aggressive_reach,
    )
    from research.families.f02_empirical_p3_touch.audit.p3_touch_policy_visible_decision_context import (
        FrozenPolicyVisibleBboSource,
        extract_policy_visible_decision_context,
    )

    day = str(payload["day"])
    output_path = Path(str(payload["output_path"])).resolve()
    p3_spec = dict(payload["p3_spec"])
    refs = list(payload["refs"])
    by_kind: dict[str, list[dict[str, Any]]] = {}
    for ref in refs:
        by_kind.setdefault(str(ref["kind"]), []).append(dict(ref))
    if len(by_kind.get("placement", [])) != 1 or len(by_kind.get("bbo", [])) != 1:
        raise ValueError(f"{day} lacks one frozen placement/BBO identity")
    if len(by_kind.get("label_trade", [])) != 2:
        raise ValueError(f"{day} lacks the two frozen strict-future trade tapes")
    placement_ref = by_kind["placement"][0]
    bbo_ref = by_kind["bbo"][0]
    placement_path = require_file(
        Path(str(placement_ref["path"])), str(placement_ref["sha256"])
    )
    bbo_path = require_file(Path(str(bbo_ref["path"])), str(bbo_ref["sha256"]))
    trade_paths = [
        require_file(Path(str(ref["path"])), str(ref["sha256"]))
        for ref in by_kind["label_trade"]
    ]

    inputs = p3_spec["input_identities"]
    profile = inputs["book_visibility_profile"]
    profile_path = require_file(Path(str(profile["path"])), str(profile["sha256"]))
    source_clock = p3_spec["source_clock_boundary"]
    decisions = load_f06_baseline_eligible_decisions(
        placement_path,
        expected_sha256=str(placement_ref["sha256"]),
    )
    batch = extract_policy_visible_decision_context(
        decisions,
        source=FrozenPolicyVisibleBboSource(
            path=bbo_path,
            sha256=str(bbo_ref["sha256"]),
            source_identity=str(source_clock["bbo_source_identity"]),
            visibility_profile_path=profile_path,
            visibility_profile_sha256=str(profile["sha256"]),
            visibility_profile_id=str(source_clock["visibility_profile_id"]),
            visibility_seed=int(source_clock["visibility_seed"]),
        ),
    )
    models = DecisionCadenceOOFModels(
        v4_1_spec=inputs["p3_v4_1_spec"],
        v2_artifact=inputs["p3_v2_artifact"],
    )
    trade_hashes = {str(path): sha256_file(path) for path in trade_paths}
    trade_ts, trade_prices, buyer_maker = load_official_aggressive_trades(
        trade_paths,
        expected_sha256=trade_hashes,
    )
    supported = batch.supported
    reach = strict_future_aggressive_reach(
        supported,
        trade_ts_ms=trade_ts,
        trade_prices=trade_prices,
        buyer_maker=buyer_maker,
    )
    parts: list[pd.DataFrame] = []
    for side in SIDES:
        side_mask = supported["side"].astype(str).eq(side).to_numpy()
        rows = supported.loc[side_mask].reset_index(drop=True)
        row_reach = reach[side_mask]
        if rows.empty:
            continue
        bid_ticks = np.rint(rows["best_bid"].to_numpy(dtype=float) / 0.1).astype(
            np.int64
        )
        ask_ticks = np.rint(rows["best_ask"].to_numpy(dtype=float) / 0.1).astype(
            np.int64
        )
        quote_ticks = rows["baseline_price_tick"].to_numpy(dtype=np.int64)
        current_ticks = (
            bid_ticks - quote_ticks if side == "BUY" else quote_ticks - ask_ticks
        )
        valid = (
            rows["inventory_role"].astype(str).isin(ROLES).to_numpy()
            & (current_ticks >= P3_DISTANCE_MIN_TICKS)
            & (current_ticks <= P3_DISTANCE_MAX_TICKS)
        )
        if not np.any(valid):
            continue
        context = {
            field: rows.loc[valid, field].to_numpy(copy=True)
            for field in DECISION_CONTEXT_FIELDS
        }
        d0 = current_ticks[valid].astype(np.float64) * 0.1
        d1 = d0 + OUTWARD_TICKS * 0.1
        p0 = models.predict_v4(day=day, context=context, side=side, distances=d0)
        p1 = models.predict_v4(day=day, context=context, side=side, distances=d1)
        y0 = row_reach[valid] >= d0
        y1 = row_reach[valid] >= d1
        part = rows.loc[
            valid,
            ["decision_id", "inventory_role", "decision_ts_ms"],
        ].copy()
        part.insert(0, "day", day)
        part.insert(1, "side", side)
        part.rename(columns={"inventory_role": "role"}, inplace=True)
        part["distance_ticks"] = current_ticks[valid]
        part["predicted_delta_reach"] = p1 - p0
        part["observed_delta_reach"] = y1.astype(np.int8) - y0.astype(np.int8)
        parts.append(part)
    if not parts:
        raise RuntimeError(f"{day} has no finite 16-tick P3 band rows")
    output = pd.concat(parts, ignore_index=True)
    if np.any(output["observed_delta_reach"].to_numpy(dtype=int) > 0):
        raise RuntimeError("farther quote produced a positive observed reach delta")
    atomic_parquet(output_path, output)
    return {
        "day": day,
        "rows": int(len(output)),
        "path": str(output_path),
        "sha256": sha256_file(output_path),
        "context_coverage": float(batch.metadata["supported_rows"] / len(decisions)),
        "economic_outcomes_read": False,
    }


def _finite_bin_edges(values: np.ndarray, bins: int) -> list[float]:
    values = np.asarray(values, dtype=np.float64)
    if values.size == 0 or not np.isfinite(values).all():
        raise ValueError("P3 band binning requires finite predictions")
    quantiles = np.quantile(values, np.linspace(0.0, 1.0, int(bins) + 1))
    internal = sorted(set(float(value) for value in quantiles[1:-1]))
    return [-1.0, *internal, 1.0]


def _assign_bin(values: np.ndarray, edges: Sequence[float]) -> np.ndarray:
    internal = np.asarray(list(edges)[1:-1], dtype=np.float64)
    return np.searchsorted(internal, np.asarray(values, dtype=np.float64), side="right")


def build_simultaneous_band(rows: pd.DataFrame) -> dict[str, Any]:
    frame = rows.copy()
    cell_specs: list[dict[str, Any]] = []
    for side in SIDES:
        for role in ROLES:
            mask = frame["side"].eq(side) & frame["role"].eq(role)
            edges = _finite_bin_edges(
                frame.loc[mask, "predicted_delta_reach"].to_numpy(dtype=float),
                PREDICTED_DELTA_BINS,
            )
            frame.loc[mask, "bin_id"] = _assign_bin(
                frame.loc[mask, "predicted_delta_reach"].to_numpy(dtype=float),
                edges,
            )
            cell_specs.append({"side": side, "role": role, "edges": edges})
    frame["bin_id"] = frame["bin_id"].astype(np.int16)
    frame["cell_id"] = (
        frame["side"].astype(str)
        + "|"
        + frame["role"].astype(str)
        + "|B"
        + frame["bin_id"].astype(str)
    )
    days = sorted(frame["day"].astype(str).unique())
    cells = sorted(frame["cell_id"].astype(str).unique())
    day_index = {day: index for index, day in enumerate(days)}
    cell_index = {cell: index for index, cell in enumerate(cells)}
    sums = np.zeros((len(days), len(cells)), dtype=np.float64)
    counts = np.zeros((len(days), len(cells)), dtype=np.int64)
    grouped = frame.groupby(["day", "cell_id"], observed=True)[
        "observed_delta_reach"
    ].agg(["sum", "count"])
    for (day, cell), row in grouped.iterrows():
        sums[day_index[str(day)], cell_index[str(cell)]] = float(row["sum"])
        counts[day_index[str(day)], cell_index[str(cell)]] = int(row["count"])
    point = sums.sum(axis=0) / np.maximum(counts.sum(axis=0), 1)
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    sampled_days = rng.integers(0, len(days), size=(BOOTSTRAP_DRAWS, len(days)))
    draws = np.empty((BOOTSTRAP_DRAWS, len(cells)), dtype=np.float64)
    for draw_index, selected in enumerate(sampled_days):
        draw_counts = counts[selected].sum(axis=0)
        draws[draw_index] = sums[selected].sum(axis=0) / np.maximum(draw_counts, 1)
    standard_error = np.std(draws, axis=0, ddof=1)
    if np.any(standard_error <= 0.0) or not np.isfinite(standard_error).all():
        raise RuntimeError("P3 simultaneous band contains a zero/nonfinite cluster SE")
    max_t = np.max(np.abs((draws - point) / standard_error), axis=1)
    critical = float(np.quantile(max_t, 0.95))
    lcb = point - critical * standard_error
    ucb = point + critical * standard_error

    cells_payload: list[dict[str, Any]] = []
    for index, cell in enumerate(cells):
        side, role, raw_bin = cell.split("|")
        bin_id = int(raw_bin[1:])
        mask = frame["cell_id"].eq(cell)
        rows_count = int(mask.sum())
        supported_days = int(frame.loc[mask, "day"].nunique())
        supported = rows_count >= 500 and supported_days >= 20
        gate_pass = bool(
            supported
            and lcb[index] >= -REACH_CHANGE_MAX
            and ucb[index] <= -REACH_CHANGE_MIN
        )
        cells_payload.append(
            {
                "cell_id": cell,
                "side": side,
                "role": role,
                "bin_id": bin_id,
                "rows": rows_count,
                "days": supported_days,
                "predicted_delta_mean": float(
                    frame.loc[mask, "predicted_delta_reach"].mean()
                ),
                "observed_delta_mean": float(point[index]),
                "cluster_standard_error": float(standard_error[index]),
                "simultaneous_lcb": float(lcb[index]),
                "simultaneous_ucb": float(ucb[index]),
                "supported": supported,
                "gate_pass": gate_pass,
            }
        )
    return {
        "schema_version": "narrowgate_conditional_p3_reach_simultaneous_band.v1",
        "identity": IDENTITY,
        "candidate_universe": {
            "sides": list(SIDES),
            "roles": list(ROLES),
            "outward_ticks": OUTWARD_TICKS,
            "predicted_delta_bins": PREDICTED_DELTA_BINS,
        },
        "bin_specs": cell_specs,
        "cluster_unit": "UTC_day",
        "bootstrap_draws": BOOTSTRAP_DRAWS,
        "bootstrap_seed": BOOTSTRAP_SEED,
        "simultaneous_max_t_critical_95": critical,
        "reach_gate": {
            "minimum_absolute_decrease": REACH_CHANGE_MIN,
            "maximum_absolute_decrease": REACH_CHANGE_MAX,
        },
        "cells": cells_payload,
        "gate_pass_cells": int(sum(row["gate_pass"] for row in cells_payload)),
        "economic_outcomes_read": False,
        "action_or_live_authority": False,
    }


def prepare_band(*, workers: int) -> None:
    p3_spec = json.loads(P3_TRANSPORT_SPEC.read_text(encoding="utf-8"))
    manifest_path = Path(str(p3_spec["output_directory"])) / "input_manifest.json"
    refs = json.loads(require_file(manifest_path).read_text(encoding="utf-8"))
    days = [str(day) for day in p3_spec["days"]]
    rows_by_day = {
        day: [dict(row) for row in refs if str(row["day"]) == day]
        for day in days
    }
    output_dir = OUTPUT_ROOT / "p3_band_rows"
    output_dir.mkdir(parents=True, exist_ok=True)
    tasks = [
        {
            "day": day,
            "output_path": str(output_dir / f"day={day}.parquet"),
            "p3_spec": p3_spec,
            "refs": rows_by_day[day],
        }
        for day in days
        if not (output_dir / f"day={day}.parquet").is_file()
    ]
    generated: dict[str, dict[str, Any]] = {}
    if tasks:
        with concurrent.futures.ProcessPoolExecutor(max_workers=max(1, workers)) as pool:
            futures = {pool.submit(p3_band_day_task, task): task["day"] for task in tasks}
            for future in concurrent.futures.as_completed(futures):
                row = future.result()
                generated[str(row["day"])] = row
                print(
                    f"P3 band {len(generated)}/{len(tasks)} {row['day']} rows={row['rows']}",
                    flush=True,
                )
    prior_manifest_path = OUTPUT_ROOT / "p3_band_input_manifest.parquet"
    prior_rows = (
        {
            str(row["day"]): dict(row)
            for row in pd.read_parquet(prior_manifest_path).to_dict("records")
        }
        if prior_manifest_path.is_file()
        else {}
    )
    summaries: list[dict[str, Any]] = []
    for day in days:
        path = output_dir / f"day={day}.parquet"
        if day in generated:
            summaries.append(generated[day])
            continue
        if day in prior_rows and str(prior_rows[day].get("sha256", "")) == sha256_file(path):
            summaries.append(prior_rows[day])
            continue
        frame = pd.read_parquet(path, columns=["day"])
        summaries.append(
            {
                "day": day,
                "rows": int(len(frame)),
                "path": str(path),
                "sha256": sha256_file(path),
                "context_coverage": math.nan,
                "economic_outcomes_read": False,
            }
        )
    summaries.sort(key=lambda row: str(row["day"]))
    manifest = pd.DataFrame(summaries)
    atomic_parquet(OUTPUT_ROOT / "p3_band_input_manifest.parquet", manifest)
    band_rows = pd.concat(
        [pd.read_parquet(output_dir / f"day={day}.parquet") for day in days],
        ignore_index=True,
    )
    band = build_simultaneous_band(band_rows)
    band["input_manifest"] = {
        "path": str(OUTPUT_ROOT / "p3_band_input_manifest.parquet"),
        "sha256": sha256_file(OUTPUT_ROOT / "p3_band_input_manifest.parquet"),
    }
    band["p3_transport_spec"] = {
        "path": str(P3_TRANSPORT_SPEC),
        "sha256": sha256_file(P3_TRANSPORT_SPEC),
    }
    band["canonical_identity_sha256"] = canonical_sha256(band)
    atomic_json(OUTPUT_ROOT / "p3_reach_simultaneous_band.json", band)


def _transport_refs_by_day(
    p3_spec: Mapping[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    manifest_path = Path(str(p3_spec["output_directory"])) / "input_manifest.json"
    refs = json.loads(require_file(manifest_path).read_text(encoding="utf-8"))
    if not isinstance(refs, list):
        raise ValueError("P3 transport input manifest must be a list")
    result: dict[str, list[dict[str, Any]]] = {}
    for raw in refs:
        row = dict(raw)
        result.setdefault(str(row["day"]), []).append(row)
    return result


def _policy_visible_source(
    *,
    day: str,
    p3_spec: Mapping[str, Any],
    refs: Sequence[Mapping[str, Any]],
):
    from research.families.f02_empirical_p3_touch.audit.p3_touch_policy_visible_decision_context import (
        FrozenPolicyVisibleBboSource,
    )

    bbo_refs = [dict(row) for row in refs if str(row["kind"]) == "bbo"]
    if len(bbo_refs) != 1:
        raise ValueError(f"{day} requires exactly one frozen P3 BBO identity")
    bbo_ref = bbo_refs[0]
    bbo_path = require_file(Path(str(bbo_ref["path"])), str(bbo_ref["sha256"]))
    visibility = dict(p3_spec["input_identities"]["book_visibility_profile"])
    visibility_path = require_file(
        Path(str(visibility["path"])), str(visibility["sha256"])
    )
    clocks = dict(p3_spec["source_clock_boundary"])
    return FrozenPolicyVisibleBboSource(
        path=bbo_path,
        sha256=str(bbo_ref["sha256"]),
        source_identity=str(clocks["bbo_source_identity"]),
        visibility_profile_path=visibility_path,
        visibility_profile_sha256=str(visibility["sha256"]),
        visibility_profile_id=str(clocks["visibility_profile_id"]),
        visibility_seed=int(clocks["visibility_seed"]),
    )


def _canonical_prediction_decisions(
    *,
    day: str,
    prediction_ts_ms: np.ndarray,
    source: Any,
) -> pd.DataFrame:
    """Build the exact sampled policy-visible BBO denominator for ML buckets."""

    from research.families.f02_empirical_p3_touch.audit.p3_touch_decision_cadence_context import (
        FrozenCausalBboSource,
        _load_causal_bbo,
    )
    from research.families.f02_empirical_p3_touch.audit.p3_touch_policy_visible_decision_context import (
        _load_visibility_profile,
        visibility_delay_ms,
    )

    timestamps = np.asarray(prediction_ts_ms, dtype=np.int64).reshape(-1)
    if timestamps.size == 0 or np.any(np.diff(timestamps) <= 0):
        raise ValueError(f"{day} ML timestamps must be non-empty and strictly increasing")
    declared_days = pd.to_datetime(timestamps, unit="ms", utc=True).strftime("%Y-%m-%d")
    if not np.all(declared_days == day):
        raise ValueError(f"{day} ML timestamps escape the declared UTC day")

    profile = _load_visibility_profile(source)
    bbo_ts, bids, asks = _load_causal_bbo(
        FrozenCausalBboSource(
            path=source.path,
            sha256=source.sha256,
            source_identity=source.source_identity,
        )
    )
    delays = visibility_delay_ms(
        timestamps,
        samples_ms=profile.samples_ms,
        seed=int(source.visibility_seed),
    )
    cutoff = timestamps - delays
    indices = np.searchsorted(bbo_ts, cutoff, side="right") - 1
    safe = np.clip(indices, 0, len(bbo_ts) - 1)
    visible_bid = bids[safe]
    visible_ask = asks[safe]
    submit_ns = timestamps * np.int64(1_000_000)
    frames: list[pd.DataFrame] = []
    for side in SIDES:
        baseline_ticks = np.rint(
            (visible_bid if side == "BUY" else visible_ask) / 0.1
        ).astype(np.int64)
        frame = pd.DataFrame(
            {
                "decision_id": [
                    f"{IDENTITY}:{day}:{side}:{int(ts)}" for ts in timestamps
                ],
                "day": day,
                "side": side,
                "inventory_role": "opener",
                "campaign_id": [
                    f"{IDENTITY}:{day}:canonical:{int(ts)}" for ts in timestamps
                ],
                "submit_ts_ns": submit_ns,
                "feature_ready_ts_ns": submit_ns,
                "best_bid": visible_bid,
                "best_ask": visible_ask,
                "baseline_price_tick": baseline_ticks,
                "baseline_action": "keep",
                "allow_post": 1,
                "decision_ts_ms": timestamps,
                "ml_index": np.arange(len(timestamps), dtype=np.int64),
            }
        )
        frames.append(frame)
    return pd.concat(frames, ignore_index=True)


def _band_lookup(
    band: Mapping[str, Any],
) -> tuple[dict[tuple[str, str], list[float]], dict[str, bool]]:
    edges: dict[tuple[str, str], list[float]] = {}
    for raw in band["bin_specs"]:
        row = dict(raw)
        key = (str(row["side"]), str(row["role"]))
        values = [float(value) for value in row["edges"]]
        if len(values) < 2 or values != sorted(values):
            raise ValueError(f"invalid P3 band edges for {key}")
        edges[key] = values
    expected = {(side, role) for side in SIDES for role in ROLES}
    if set(edges) != expected:
        raise ValueError("P3 band lacks a side/role bin specification")
    gate_by_cell = {
        str(row["cell_id"]): bool(row["gate_pass"])
        for row in band["cells"]
    }
    return edges, gate_by_cell


def p3_gate_matrix_task(payload: Mapping[str, Any]) -> dict[str, Any]:
    from research.families.f02_empirical_p3_touch.audit.p3_touch_decision_cadence_context import (
        DECISION_CONTEXT_FIELDS,
    )
    from research.families.f02_empirical_p3_touch.audit.p3_touch_decision_cadence_transport import (
        DecisionCadenceOOFModels,
    )
    from research.families.f02_empirical_p3_touch.audit.p3_touch_policy_visible_decision_context import (
        extract_policy_visible_decision_context,
    )

    day = str(payload["day"])
    output_path = Path(str(payload["output_path"])).resolve()
    config_path = Path(str(payload["config_path"])).resolve()
    p3_spec = dict(payload["p3_spec"])
    band = dict(payload["band"])
    refs = [dict(row) for row in payload.get("refs", [])]
    supported_day = bool(payload["supported_day"])

    params = build_params(day, config_path)
    window = load_window(day, params)
    if window.ml_data is None or len(window.ml_data) < 6:
        raise RuntimeError(f"{day} lacks the frozen v12 ML overlay")
    ml_ts = np.ascontiguousarray(window.ml_data[0], dtype=np.int64)
    grid_ticks = np.arange(
        P3_DISTANCE_MIN_TICKS,
        P3_DISTANCE_MAX_TICKS + 1,
        dtype=np.int64,
    )
    grid_size = int(len(grid_ticks))
    status = np.zeros((len(ml_ts), 4 * grid_size), dtype=np.uint8)
    context_rows = 0
    supported_rows = 0
    monotonicity_violations = 0
    started = time.perf_counter()

    if supported_day:
        source = _policy_visible_source(
            day=day,
            p3_spec=p3_spec,
            refs=refs,
        )
        decisions = _canonical_prediction_decisions(
            day=day,
            prediction_ts_ms=ml_ts,
            source=source,
        )
        batch = extract_policy_visible_decision_context(decisions, source=source)
        models = DecisionCadenceOOFModels(
            v4_1_spec=p3_spec["input_identities"]["p3_v4_1_spec"],
            v2_artifact=p3_spec["input_identities"]["p3_v2_artifact"],
        )
        edge_by_scope, gate_by_cell = _band_lookup(band)
        context_rows = int(len(batch.frame))
        supported_rows = int(batch.frame["supported"].astype(bool).sum())
        distance0 = grid_ticks.astype(np.float64) * 0.1
        distance1 = distance0 + OUTWARD_TICKS * 0.1

        for side in SIDES:
            side_rows = batch.frame.loc[
                batch.frame["supported"].astype(bool)
                & batch.frame["side"].astype(str).eq(side)
            ].reset_index(drop=True)
            for lower in range(
                0, len(side_rows), P3_PREDICTION_CONTEXT_CHUNK_ROWS
            ):
                upper = min(
                    lower + P3_PREDICTION_CONTEXT_CHUNK_ROWS,
                    len(side_rows),
                )
                chunk = side_rows.iloc[lower:upper]
                count = len(chunk)
                if count == 0:
                    continue
                context = {
                    field: np.repeat(
                        chunk[field].to_numpy(copy=True),
                        grid_size,
                    )
                    for field in DECISION_CONTEXT_FIELDS
                }
                tiled0 = np.tile(distance0, count)
                tiled1 = np.tile(distance1, count)
                p0 = models.predict_v4(
                    day=day,
                    context=context,
                    side=side,
                    distances=tiled0,
                )
                p1 = models.predict_v4(
                    day=day,
                    context=context,
                    side=side,
                    distances=tiled1,
                )
                delta = (p1 - p0).reshape(count, grid_size)
                monotonicity_violations += int(np.sum(delta > 1e-12))
                ml_indices = chunk["ml_index"].to_numpy(dtype=np.int64)
                for role in ROLES:
                    edges = edge_by_scope[(side, role)]
                    bins = _assign_bin(delta.reshape(-1), edges).reshape(delta.shape)
                    bin_status = np.asarray(
                        [
                            2
                            if gate_by_cell.get(
                                f"{side}|{role}|B{bin_id}", False
                            )
                            else 1
                            for bin_id in range(len(edges) - 1)
                        ],
                        dtype=np.uint8,
                    )
                    block = BLOCK_INDEX[(side, role)]
                    status[
                        ml_indices,
                        block * grid_size : (block + 1) * grid_size,
                    ] = bin_status[bins]

    if monotonicity_violations:
        raise RuntimeError(
            f"{day} P3 outward move has {monotonicity_violations} monotonicity violations"
        )
    atomic_npz(output_path, ts_ms=ml_ts, status=status)
    return {
        "day": day,
        "supported_day": supported_day,
        "ml_rows": int(len(ml_ts)),
        "grid_min_ticks": P3_DISTANCE_MIN_TICKS,
        "grid_max_ticks": P3_DISTANCE_MAX_TICKS,
        "grid_size": grid_size,
        "context_rows": context_rows,
        "context_supported_rows": supported_rows,
        "context_coverage": (
            float(supported_rows / context_rows) if context_rows else 0.0
        ),
        "status_supported_cells": int(np.sum(status > 0)),
        "status_gate_pass_cells": int(np.sum(status == 2)),
        "monotonicity_violations": monotonicity_violations,
        "path": str(output_path),
        "sha256": sha256_file(output_path),
        "size_bytes": int(output_path.stat().st_size),
        "runtime_s": time.perf_counter() - started,
        "economic_outcomes_read": False,
    }


def prepare_gates(*, workers: int) -> None:
    baseline = validate_current_baseline()
    days, _, _ = load_panel()
    p3_spec = json.loads(P3_TRANSPORT_SPEC.read_text(encoding="utf-8"))
    refs_by_day = _transport_refs_by_day(p3_spec)
    p3_days = set(str(value) for value in p3_spec["days"])
    band_path = OUTPUT_ROOT / "p3_reach_simultaneous_band.json"
    band = json.loads(require_file(band_path).read_text(encoding="utf-8"))
    observed_canonical = str(band.get("canonical_identity_sha256", ""))
    without_identity = dict(band)
    without_identity.pop("canonical_identity_sha256", None)
    if observed_canonical != canonical_sha256(without_identity):
        raise ValueError("P3 simultaneous-band canonical identity drift")

    output_dir = OUTPUT_ROOT / "gate_matrices"
    output_dir.mkdir(parents=True, exist_ok=True)
    tasks = [
        {
            "day": day,
            "output_path": str(output_dir / f"day={day}.npz"),
            "config_path": str(baseline["config_path"]),
            "p3_spec": p3_spec,
            "band": band,
            "refs": refs_by_day.get(day, []),
            "supported_day": day in p3_days,
        }
        for day in days
        if not (output_dir / f"day={day}.npz").is_file()
    ]
    generated: dict[str, dict[str, Any]] = {}
    if tasks:
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=max(1, workers)
        ) as pool:
            futures = {
                pool.submit(p3_gate_matrix_task, task): task["day"]
                for task in tasks
            }
            for future in concurrent.futures.as_completed(futures):
                row = future.result()
                generated[str(row["day"])] = row
                print(
                    f"P3 gate {len(generated)}/{len(tasks)} {row['day']} "
                    f"supported={row['supported_day']} coverage={row['context_coverage']:.4f} "
                    f"runtime={row['runtime_s']:.2f}s",
                    flush=True,
                )

    prior_path = OUTPUT_ROOT / "gate_matrix_manifest.parquet"
    prior_rows = (
        {
            str(row["day"]): dict(row)
            for row in pd.read_parquet(prior_path).to_dict("records")
        }
        if prior_path.is_file()
        else {}
    )
    summaries: list[dict[str, Any]] = []
    for day in days:
        path = output_dir / f"day={day}.npz"
        if day in generated:
            summaries.append(generated[day])
            continue
        observed_sha = sha256_file(path)
        if day in prior_rows and str(prior_rows[day].get("sha256", "")) == observed_sha:
            summaries.append(prior_rows[day])
            continue
        with np.load(path, allow_pickle=False) as payload:
            ts_ms = np.asarray(payload["ts_ms"], dtype=np.int64)
            status = np.asarray(payload["status"], dtype=np.uint8)
        summaries.append(
            {
                "day": day,
                "supported_day": day in p3_days,
                "ml_rows": int(len(ts_ms)),
                "grid_min_ticks": P3_DISTANCE_MIN_TICKS,
                "grid_max_ticks": P3_DISTANCE_MAX_TICKS,
                "grid_size": P3_DISTANCE_MAX_TICKS - P3_DISTANCE_MIN_TICKS + 1,
                "context_rows": 0,
                "context_supported_rows": 0,
                "context_coverage": 0.0,
                "status_supported_cells": int(np.sum(status > 0)),
                "status_gate_pass_cells": int(np.sum(status == 2)),
                "monotonicity_violations": 0,
                "path": str(path),
                "sha256": observed_sha,
                "size_bytes": int(path.stat().st_size),
                "runtime_s": math.nan,
                "economic_outcomes_read": False,
            }
        )
    summaries.sort(key=lambda row: str(row["day"]))
    manifest = pd.DataFrame(summaries)
    atomic_parquet(prior_path, manifest)
    report = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "stage": "p3_gate_matrices_before_economic_outcomes",
        "development_days": days,
        "p3_supported_days": sorted(set(days) & p3_days),
        "p3_unsupported_fallback_days": sorted(set(days) - p3_days),
        "supported_day_count": int(manifest["supported_day"].astype(bool).sum()),
        "status_supported_cells": int(manifest["status_supported_cells"].sum()),
        "status_gate_pass_cells": int(manifest["status_gate_pass_cells"].sum()),
        "matrix_manifest": {
            "path": str(prior_path),
            "sha256": sha256_file(prior_path),
        },
        "simultaneous_band": {
            "path": str(band_path),
            "sha256": sha256_file(band_path),
            "canonical_identity_sha256": observed_canonical,
        },
        "economic_outcomes_read": False,
        "action_or_live_authority": False,
    }
    atomic_json(OUTPUT_ROOT / "gate_matrix_report.json", report)


def _artifact(path: Path) -> dict[str, Any]:
    resolved = require_file(path)
    return {
        "path": str(resolved),
        "sha256": sha256_file(resolved),
        "size_bytes": int(resolved.stat().st_size),
    }


def freeze_spec() -> None:
    import narrowgate_cpp

    from models.audit.experiment_scorecard import score_profile_contract

    if SPEC_PATH.exists():
        raise FileExistsError(f"frozen Spec already exists: {SPEC_PATH}")
    if (OUTPUT_ROOT / "cpp_screen_report.json").exists():
        raise RuntimeError("economic results already exist; refusing to freeze after outcome read")
    baseline = validate_current_baseline()
    days, grade_a, grade_b = load_panel()
    pointer = dict(baseline["pointer"])
    required_outcome_blind = (
        OUTPUT_ROOT / "baseline_mechanics_report.json",
        OUTPUT_ROOT / "baseline_mechanics_manifest.parquet",
        OUTPUT_ROOT / "toxicity_p90_schedule.parquet",
        OUTPUT_ROOT / "p3_reach_simultaneous_band.json",
        OUTPUT_ROOT / "p3_band_input_manifest.parquet",
        OUTPUT_ROOT / "gate_matrix_report.json",
        OUTPUT_ROOT / "gate_matrix_manifest.parquet",
    )
    for path in required_outcome_blind:
        require_file(path)

    implementation_paths = (
        Path(__file__).resolve(),
        ROOT / "models/backtest_tick.py",
        ROOT / "cpp/narrowgate_cpp/tick_replay.hpp",
        ROOT / "cpp/narrowgate_cpp/tick_replay.cpp",
        ROOT / "cpp/narrowgate_cpp/bindings.cpp",
        ROOT / "tests/test_conditional_p3_reach_gate_cpp.py",
        ROOT / "models/audit/action_bound_full_path_promotion.py",
        ROOT
        / "research/shared/experiment_governance/docs/"
        "action_bound_full_path_direct_promotion_contract_v1.md",
        Path(str(narrowgate_cpp.__file__)).resolve(),
    )
    implementation = {
        path.name if path.name not in {"tick_replay.cpp", "tick_replay.hpp"} else str(path.relative_to(ROOT)):
        _artifact(path)
        for path in implementation_paths
    }
    matrix_report = json.loads(
        (OUTPUT_ROOT / "gate_matrix_report.json").read_text(encoding="utf-8")
    )
    if matrix_report.get("economic_outcomes_read") is not False:
        raise ValueError("gate matrices are not outcome-blind")
    if int(matrix_report.get("supported_day_count", -1)) != 26:
        raise ValueError("frozen P3-supported Development-day count drift")

    payload: dict[str, Any] = {
        "schema_version": f"{SCHEMA_VERSION}.spec",
        "identity": IDENTITY,
        "frozen_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "frozen_before_cpp_economic_screen",
        "research_family": "F09_campaign_action_uplift",
        "promotion_route_if_all_downstream_gates_pass": (
            "owner_risk_accepted_promotion"
        ),
        "owner_risk_reason": (
            "conditional P3 v4.1 used an outcome-informed 95% context-coverage override"
        ),
        "baseline": {
            "pointer": _artifact(BASELINE_POINTER),
            "identity": _artifact(baseline["identity_path"]),
            "config": _artifact(baseline["config_path"]),
            "baseline_id": str(pointer["baseline_id"]),
            "ml_enabled": True,
            "q90_shadow_enabled": True,
            "q90_action_enabled": False,
            "buy_fill_selection_shadow_enabled": False,
            "buy_fill_selection_action_enabled": False,
        },
        "action": {
            "candidate_source_identity": (
                "causal_v12_side_specific_toxicity_past_only_p90"
            ),
            "candidate_action": "exposure_quote_outward_16_ticks",
            "intervention_axis": "quote_price",
            "sides": list(SIDES),
            "roles": list(ROLES),
            "outward_ticks": OUTWARD_TICKS,
            "outward_usdc_per_btc": OUTWARD_TICKS * 0.1,
            "reducing_quote_changed": False,
            "lifecycle_ownership_changed": False,
            "requote_schedule_changed": False,
            "cancel_policy_changed": False,
            "cooldown_changed": False,
            "order_size_changed": False,
            "inventory_limit_changed": False,
            "candidate_generated_by_p3": False,
            "p3_role": "required_reach_mechanics_gate",
        },
        "toxicity_threshold": {
            "quantile": TOXICITY_QUANTILE,
            "method": "higher",
            "history": "strictly earlier frozen Development UTC days",
            "denominator": (
                "side-specific baseline-eligible exposure-increasing quote opportunities, "
                "at most one row per completed 10s v12 bucket"
            ),
            "minimum_prior_days": MINIMUM_PRIOR_DAYS,
            "minimum_prior_buckets": MINIMUM_PRIOR_BUCKETS,
            "schedule": _artifact(OUTPUT_ROOT / "toxicity_p90_schedule.parquet"),
            "unready_fallback": "baseline",
        },
        "conditional_p3": {
            "artifact": _artifact(P3_TRANSPORT_SPEC),
            "horizon_s": 10,
            "cadence": "canonical_10s_v12_prediction_bucket",
            "distance_support_ticks": [
                P3_DISTANCE_MIN_TICKS,
                P3_DISTANCE_MAX_TICKS,
            ],
            "reach_change_gate": {
                "minimum_absolute_decrease": REACH_CHANGE_MIN,
                "maximum_absolute_decrease": REACH_CHANGE_MAX,
            },
            "simultaneous_band": _artifact(
                OUTPUT_ROOT / "p3_reach_simultaneous_band.json"
            ),
            "gate_matrix_manifest": _artifact(
                OUTPUT_ROOT / "gate_matrix_manifest.parquet"
            ),
            "gate_matrix_report": _artifact(
                OUTPUT_ROOT / "gate_matrix_report.json"
            ),
            "supported_days": list(matrix_report["p3_supported_days"]),
            "unsupported_days_fallback_baseline": list(
                matrix_report["p3_unsupported_fallback_days"]
            ),
            "unsupported_context_fallback": "baseline",
            "scalar_kappa_or_delta_star_exported": False,
        },
        "panels": {
            "development_days": days,
            "grade_a_days": sorted(grade_a),
            "grade_b_days": sorted(grade_b),
            "daily_initial_state": "fresh_start_due_noncontiguous_retained_panel",
            "validation_read": False,
            "sealed_holdout_read": False,
        },
        "data_and_model": {
            "feature_manifest": _artifact(FEATURE_DIR / "causal_feature_manifest.json"),
            "v12_bundle_meta": _artifact(MODEL_DIR / "bundle_meta.json"),
            "queue_calibration": _artifact(QUEUE_PATH),
            "latency_profile": _artifact(LATENCY_PATH),
            "trade_manifest": _artifact(TRADE_MANIFEST),
            "trade_quality": _artifact(TRADE_QUALITY),
        },
        "replay": {
            "cpp_full_path_screen": True,
            "cpp_screen_can_grant_live_authority": False,
            "authoritative_python_full_path_required_after_cpp_pass": True,
            "baseline_and_candidate_rerun_together": True,
            "shared_market_and_random_seed": True,
            "candidate_path_regenerated": True,
            "required_components": [
                "tick_rounding_gtx_and_spread_cap",
                "activation",
                "exact_level_queue",
                "partial_fill",
                "cancel_request_ack_race",
                "cooldown",
                "inventory",
                "campaign",
            ],
            "trace_fills_max_per_arm_day": TRACE_FILLS_MAX,
        },
        "cpp_screen_gates": {
            "terminal_mtm_pnl_day_cluster_lcb_gt_usdc_per_day": 0.0,
            "fill_retention_range": [0.80, 1.20],
            "candidate_price_change_rate_range": [0.02, 0.20],
            "campaign_q10_noninferior": True,
            "campaign_cvar10_noninferior": True,
            "maximum_inventory_noninferior": True,
            "inventory_time_noninferior": True,
            "maximum_drawdown_noninferior": True,
            "minimum_buy_price_changes": 100,
            "minimum_sell_price_changes": 100,
            "bootstrap_draws": ECONOMIC_BOOTSTRAP_DRAWS,
            "bootstrap_seed": ECONOMIC_BOOTSTRAP_SEED,
        },
        "authoritative_and_production_gates": {
            "assignment_to_terminal_pnl_lcb_positive": True,
            "campaign_q10_noninferior": True,
            "campaign_cvar_noninferior": True,
            "mae_noninferior": True,
            "maximum_inventory_noninferior": True,
            "inventory_time_noninferior": True,
            "fill_and_activity_within_frozen_bounds": True,
            "python_cpp_policy_parity": True,
            "config_model_and_artifact_hash_match": True,
            "live_preflight": True,
            "automatic_rollback": True,
        },
        "score_profile": score_profile_contract("action_execution_selective_v2"),
        "outcome_blind_artifacts": {
            path.name: _artifact(path) for path in required_outcome_blind
        },
        "implementation_identities": implementation,
        "implementation_identity_sha256": canonical_sha256(implementation),
        "permissions_before_result": {
            "economic_results_read": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authority": False,
            "live_authority": False,
        },
    }
    payload["canonical_spec_identity_sha256"] = canonical_sha256(payload)
    atomic_json(SPEC_PATH, payload)


def load_frozen_spec() -> dict[str, Any]:
    spec = json.loads(require_file(SPEC_PATH).read_text(encoding="utf-8"))
    expected = str(spec.get("canonical_spec_identity_sha256", ""))
    without_identity = dict(spec)
    without_identity.pop("canonical_spec_identity_sha256", None)
    if expected != canonical_sha256(without_identity):
        raise ValueError("frozen action Spec canonical identity drift")
    for artifact in spec["implementation_identities"].values():
        require_file(Path(str(artifact["path"])), str(artifact["sha256"]))
    for artifact in spec["outcome_blind_artifacts"].values():
        require_file(Path(str(artifact["path"])), str(artifact["sha256"]))
    return spec


def _load_gate_matrix(
    *,
    day: str,
    expected_ts_ms: np.ndarray,
    manifest_row: Mapping[str, Any],
) -> np.ndarray:
    path = require_file(Path(str(manifest_row["path"])), str(manifest_row["sha256"]))
    with np.load(path, allow_pickle=False) as payload:
        timestamps = np.ascontiguousarray(payload["ts_ms"], dtype=np.int64)
        status = np.ascontiguousarray(payload["status"], dtype=np.uint8)
    expected = np.ascontiguousarray(expected_ts_ms, dtype=np.int64)
    grid_size = P3_DISTANCE_MAX_TICKS - P3_DISTANCE_MIN_TICKS + 1
    if not np.array_equal(timestamps, expected):
        raise ValueError(f"{day} gate matrix timestamp/ML overlay mismatch")
    if status.shape != (len(expected), 4 * grid_size):
        raise ValueError(f"{day} gate matrix shape mismatch: {status.shape}")
    if np.any(status > 2):
        raise ValueError(f"{day} gate matrix contains an invalid status")
    return status


def cpp_screen_day_task(payload: Mapping[str, Any]) -> dict[str, Any]:
    from models import backtest_tick as bt
    from research.families.f03_causal_13_head.audit.full_path_ml_ab import (
        _campaign_day_metrics,
        _side_trace_metrics,
        reconstruct_campaigns,
    )

    day = str(payload["day"])
    panel_role = str(payload["panel_role"])
    output_dir = Path(str(payload["output_dir"])).resolve()
    config_path = Path(str(payload["config_path"])).resolve()
    params_base = build_params(day, config_path)
    params_base.update(
        {
            "trace_fills_max": TRACE_FILLS_MAX,
            "trace_fills_window_s": 30.0,
            "trace_p3_reach_decisions_max": 25_000,
            "replay_purpose": "conditional_p3_reach_gate_cpp_economic_screen",
            "replay_promotion_eligible": False,
        }
    )
    window = load_window(day, params_base)
    if str(window.book_source_authority) != "native_formal_lifecycle":
        raise ValueError(f"{day} is not native_formal_lifecycle")
    ml_ts = np.ascontiguousarray(window.ml_data[0], dtype=np.int64)
    gate_status = _load_gate_matrix(
        day=day,
        expected_ts_ms=ml_ts,
        manifest_row=dict(payload["gate_manifest_row"]),
    )
    thresholds = {
        str(row["side"]): dict(row)
        for row in payload["threshold_rows"]
    }
    if set(thresholds) != set(SIDES):
        raise ValueError(f"{day} threshold schedule lacks BUY/SELL")

    daily_rows: list[dict[str, Any]] = []
    campaign_rows: list[dict[str, Any]] = []
    for arm in ARMS:
        params = dict(params_base)
        if arm == "candidate":
            params.update(
                {
                    "conditional_p3_reach_gate_enabled": True,
                    "conditional_p3_reach_gate_outward_ticks": OUTWARD_TICKS,
                    "conditional_p3_reach_gate_grid_min_ticks": P3_DISTANCE_MIN_TICKS,
                    "conditional_p3_reach_gate_buy_toxicity_threshold": (
                        float(thresholds["BUY"]["threshold"])
                        if bool(thresholds["BUY"]["ready"])
                        else 1.0
                    ),
                    "conditional_p3_reach_gate_sell_toxicity_threshold": (
                        float(thresholds["SELL"]["threshold"])
                        if bool(thresholds["SELL"]["ready"])
                        else 1.0
                    ),
                    "_conditional_p3_reach_gate_ts_ms": ml_ts,
                    "_conditional_p3_reach_gate_status": gate_status,
                }
            )
        else:
            params["conditional_p3_reach_gate_enabled"] = False
            params.pop("_conditional_p3_reach_gate_ts_ms", None)
            params.pop("_conditional_p3_reach_gate_status", None)

        started = time.perf_counter()
        result = bt._simulate_tick_with_engine(
            "cpp",
            window.trades,
            window.var_ts_ms,
            window.var_ssq,
            params,
            ml_data=window.ml_data,
            bbo_data=window.bbo_data,
            l2_data=window.l2_data,
            var_ti=window.var_ti,
            var_retsq=window.var_retsq,
        )
        fill_trace = list(result.get("_fill_trace") or [])
        if len(fill_trace) != int(result["fills_total"]):
            raise RuntimeError(
                f"{day} {arm} fill trace truncated: "
                f"{len(fill_trace)} != {result['fills_total']}"
            )
        campaigns = reconstruct_campaigns(
            fill_trace,
            day=day,
            panel_role=panel_role,
            arm=arm,
            terminal_mark_price=float(result["terminal_mark_price"]),
            order_size=float(params["order_size"]),
        )
        campaign_frame = pd.DataFrame(campaigns)
        campaign_metrics = _campaign_day_metrics(campaign_frame)
        accounting_error = (
            float(campaign_metrics["campaign_terminal_value_usdc"])
            - float(result["terminal_mtm_pnl"])
        )
        if abs(accounting_error) > 1e-6:
            raise RuntimeError(
                f"{day} {arm} campaign accounting mismatch: {accounting_error}"
            )
        buy = _side_trace_metrics(fill_trace, "BUY")
        sell = _side_trace_metrics(fill_trace, "SELL")
        daily_rows.append(
            {
                "day": day,
                "panel_role": panel_role,
                "arm": arm,
                "source_authority": str(window.book_source_authority),
                "pnl_usdc": float(result["pnl"]),
                "terminal_mtm_pnl_usdc": float(result["terminal_mtm_pnl"]),
                "fills_bid": int(result["fills_bid"]),
                "fills_ask": int(result["fills_ask"]),
                "fills_total": int(result["fills_total"]),
                "abs_inventory_time_btc_s": float(result["abs_inventory_time_s"]),
                "max_inventory_btc": float(result["max_inventory"]),
                "final_inventory_btc": float(result["final_inventory"]),
                "max_drawdown_usdc": float(result["max_drawdown"]),
                "buy_maker_value_30s_bps": buy["maker_value_30s_bps"],
                "sell_maker_value_30s_bps": sell["maker_value_30s_bps"],
                "campaign_accounting_error_usdc": accounting_error,
                "p3_eval_count": int(result["conditional_p3_reach_gate_eval_count"]),
                "p3_toxicity_trigger_count": int(
                    result["conditional_p3_reach_gate_toxicity_trigger_count"]
                ),
                "p3_supported_count": int(
                    result["conditional_p3_reach_gate_supported_count"]
                ),
                "p3_gate_pass_count": int(
                    result["conditional_p3_reach_gate_pass_count"]
                ),
                "p3_price_change_count": int(
                    result["conditional_p3_reach_gate_price_change_count"]
                ),
                "p3_buy_price_change_count": int(
                    result["conditional_p3_reach_gate_buy_price_change_count"]
                ),
                "p3_sell_price_change_count": int(
                    result["conditional_p3_reach_gate_sell_price_change_count"]
                ),
                "p3_spread_cap_noop_count": int(
                    result["conditional_p3_reach_gate_spread_cap_noop_count"]
                ),
                "runtime_s": time.perf_counter() - started,
                **campaign_metrics,
            }
        )
        campaign_rows.extend(campaigns)
    daily = pd.DataFrame(daily_rows)
    if int(daily.loc[daily["arm"].eq("control"), "p3_price_change_count"].iloc[0]) != 0:
        raise RuntimeError(f"{day} control unexpectedly changed a P3-gated quote")
    campaigns_frame = pd.DataFrame(campaign_rows)
    atomic_parquet(output_dir / f"day={day}.daily.parquet", daily)
    atomic_parquet(output_dir / f"day={day}.campaigns.parquet", campaigns_frame)
    return {
        "day": day,
        "daily": daily_rows,
        "campaigns_path": str(output_dir / f"day={day}.campaigns.parquet"),
    }


def _bootstrap_paired_daily(values: np.ndarray) -> dict[str, Any]:
    delta = np.asarray(values, dtype=np.float64).reshape(-1)
    if len(delta) != 40 or not np.isfinite(delta).all():
        raise ValueError("economic screen requires 40 finite paired daily deltas")
    rng = np.random.default_rng(ECONOMIC_BOOTSTRAP_SEED)
    sampled = rng.choice(
        delta,
        size=(ECONOMIC_BOOTSTRAP_DRAWS, len(delta)),
        replace=True,
    ).mean(axis=1)
    return {
        "days": int(len(delta)),
        "sum_delta_usdc": float(np.sum(delta)),
        "mean_daily_delta_usdc": float(np.mean(delta)),
        "median_daily_delta_usdc": float(np.median(delta)),
        "positive_day_rate": float(np.mean(delta > 0.0)),
        "ci95_day_cluster_bootstrap_usdc_per_day": [
            float(np.quantile(sampled, 0.025)),
            float(np.quantile(sampled, 0.975)),
        ],
    }


def _campaign_tail_summary(frame: pd.DataFrame) -> dict[str, Any]:
    values = frame["terminal_value_usdc"].to_numpy(dtype=np.float64)
    if len(values) == 0:
        return {"count": 0, "q10_usdc": 0.0, "cvar10_usdc": 0.0}
    q10 = float(np.quantile(values, 0.10))
    return {
        "count": int(len(values)),
        "closed_count": int(frame["closed"].astype(bool).sum()),
        "sum_terminal_value_usdc": float(np.sum(values)),
        "mean_terminal_value_usdc": float(np.mean(values)),
        "q10_usdc": q10,
        "cvar10_usdc": float(np.mean(values[values <= q10])),
    }


def screen_cpp(*, workers: int) -> None:
    if workers not in {1, 2}:
        raise ValueError("workers must be 1 or 2")
    spec = load_frozen_spec()
    report_path = OUTPUT_ROOT / "cpp_screen_report.json"
    if report_path.exists():
        raise FileExistsError(f"C++ economic screen already exists: {report_path}")
    baseline = validate_current_baseline()
    days, grade_a, grade_b = load_panel()
    matrix_manifest = pd.read_parquet(
        OUTPUT_ROOT / "gate_matrix_manifest.parquet"
    )
    matrix_rows = {
        str(row["day"]): dict(row) for row in matrix_manifest.to_dict("records")
    }
    schedule = pd.read_parquet(OUTPUT_ROOT / "toxicity_p90_schedule.parquet")
    threshold_rows = {
        day: schedule.loc[schedule["day"].astype(str).eq(day)].to_dict("records")
        for day in days
    }
    day_dir = OUTPUT_ROOT / "cpp_screen_days"
    day_dir.mkdir(parents=True, exist_ok=True)
    tasks = [
        {
            "day": day,
            "panel_role": "grade_a" if day in grade_a else "grade_b",
            "output_dir": str(day_dir),
            "config_path": str(baseline["config_path"]),
            "gate_manifest_row": matrix_rows[day],
            "threshold_rows": threshold_rows[day],
        }
        for day in days
    ]
    results: list[dict[str, Any]] = []
    if workers == 1:
        for task in tasks:
            row = cpp_screen_day_task(task)
            results.append(row)
            print(f"C++ screen {len(results)}/{len(tasks)} {row['day']}", flush=True)
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(cpp_screen_day_task, task): task["day"] for task in tasks}
            for future in concurrent.futures.as_completed(futures):
                row = future.result()
                results.append(row)
                daily = {item["arm"]: item for item in row["daily"]}
                delta = (
                    daily["candidate"]["terminal_mtm_pnl_usdc"]
                    - daily["control"]["terminal_mtm_pnl_usdc"]
                )
                print(
                    f"C++ screen {len(results)}/{len(tasks)} {row['day']} "
                    f"delta={delta:+.6f}",
                    flush=True,
                )
    daily = pd.DataFrame(
        [item for result in results for item in result["daily"]]
    ).sort_values(["day", "arm"])
    campaigns = pd.concat(
        [pd.read_parquet(result["campaigns_path"]) for result in results],
        ignore_index=True,
    ).sort_values(["day", "arm", "campaign_index"])
    if len(daily) != 80 or daily["day"].nunique() != 40:
        raise RuntimeError("paired C++ screen daily denominator mismatch")
    wide = daily.pivot(index="day", columns="arm", values="terminal_mtm_pnl_usdc")
    delta = wide["candidate"].to_numpy(dtype=float) - wide["control"].to_numpy(dtype=float)
    pnl = _bootstrap_paired_daily(delta)
    totals = daily.groupby("arm", sort=True).sum(numeric_only=True)
    fill_retention = float(
        totals.loc["candidate", "fills_total"]
        / max(float(totals.loc["control", "fills_total"]), 1.0)
    )
    inventory_time_ratio = float(
        totals.loc["candidate", "abs_inventory_time_btc_s"]
        / max(float(totals.loc["control", "abs_inventory_time_btc_s"]), 1e-12)
    )
    campaign_summary = {
        arm: _campaign_tail_summary(campaigns.loc[campaigns["arm"].eq(arm)])
        for arm in ARMS
    }
    candidate = daily.loc[daily["arm"].eq("candidate")]
    action_eval = int(candidate["p3_eval_count"].sum())
    action_changes = int(candidate["p3_price_change_count"].sum())
    action_rate = float(action_changes / max(action_eval, 1))
    gates = dict(spec["cpp_screen_gates"])
    fill_bounds = [float(value) for value in gates["fill_retention_range"]]
    rate_bounds = [float(value) for value in gates["candidate_price_change_rate_range"]]
    hard_gates = {
        "terminal_mtm_pnl_lcb_positive": bool(
            pnl["ci95_day_cluster_bootstrap_usdc_per_day"][0] > 0.0
        ),
        "fill_retention_within_bounds": bool(
            fill_bounds[0] <= fill_retention <= fill_bounds[1]
        ),
        "candidate_price_change_rate_within_bounds": bool(
            rate_bounds[0] <= action_rate <= rate_bounds[1]
        ),
        "campaign_q10_noninferior": bool(
            campaign_summary["candidate"]["q10_usdc"]
            >= campaign_summary["control"]["q10_usdc"]
        ),
        "campaign_cvar10_noninferior": bool(
            campaign_summary["candidate"]["cvar10_usdc"]
            >= campaign_summary["control"]["cvar10_usdc"]
        ),
        "maximum_inventory_noninferior": bool(
            candidate["max_inventory_btc"].max()
            <= daily.loc[daily["arm"].eq("control"), "max_inventory_btc"].max()
        ),
        "inventory_time_noninferior": bool(inventory_time_ratio <= 1.0),
        "maximum_drawdown_noninferior": bool(
            candidate["max_drawdown_usdc"].min()
            >= daily.loc[daily["arm"].eq("control"), "max_drawdown_usdc"].min()
        ),
        "buy_action_support": bool(
            int(candidate["p3_buy_price_change_count"].sum())
            >= int(gates["minimum_buy_price_changes"])
        ),
        "sell_action_support": bool(
            int(candidate["p3_sell_price_change_count"].sum())
            >= int(gates["minimum_sell_price_changes"])
        ),
        "campaign_accounting_identity": bool(
            daily["campaign_accounting_error_usdc"].abs().max() <= 1e-6
        ),
    }
    all_passed = all(hard_gates.values())
    atomic_parquet(OUTPUT_ROOT / "cpp_screen_daily.parquet", daily)
    atomic_parquet(OUTPUT_ROOT / "cpp_screen_campaigns.parquet", campaigns)
    report = {
        "schema_version": f"{SCHEMA_VERSION}.cpp_screen",
        "identity": IDENTITY,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "spec": _artifact(SPEC_PATH),
        "comparison": "candidate_minus_current_v9_control",
        "control_totals": {
            "terminal_mtm_pnl_usdc": float(
                totals.loc["control", "terminal_mtm_pnl_usdc"]
            ),
            "pnl_usdc": float(totals.loc["control", "pnl_usdc"]),
            "fills_total": int(totals.loc["control", "fills_total"]),
        },
        "candidate_totals": {
            "terminal_mtm_pnl_usdc": float(
                totals.loc["candidate", "terminal_mtm_pnl_usdc"]
            ),
            "pnl_usdc": float(totals.loc["candidate", "pnl_usdc"]),
            "fills_total": int(totals.loc["candidate", "fills_total"]),
        },
        "paired_terminal_mtm_pnl": pnl,
        "fill_retention": fill_retention,
        "inventory_time_ratio": inventory_time_ratio,
        "campaign_summary": campaign_summary,
        "mechanics": {
            "evaluations": action_eval,
            "toxicity_triggers": int(candidate["p3_toxicity_trigger_count"].sum()),
            "p3_supported": int(candidate["p3_supported_count"].sum()),
            "p3_gate_pass": int(candidate["p3_gate_pass_count"].sum()),
            "price_changes": action_changes,
            "price_change_rate": action_rate,
            "buy_price_changes": int(candidate["p3_buy_price_change_count"].sum()),
            "sell_price_changes": int(candidate["p3_sell_price_change_count"].sum()),
            "spread_cap_noops": int(candidate["p3_spread_cap_noop_count"].sum()),
        },
        "hard_gates": hard_gates,
        "all_cpp_screen_gates_passed": all_passed,
        "decision": (
            "advance_to_authoritative_python_full_path"
            if all_passed
            else "close_before_authoritative_python_and_do_not_deploy"
        ),
        "permissions": {
            "cpp_screen_only": True,
            "authoritative_economic_authority": False,
            "action_authority": False,
            "live_authority": False,
        },
    }
    atomic_json(report_path, report)
    manifest = {
        "schema_version": f"{SCHEMA_VERSION}.cpp_screen.manifest",
        "identity": IDENTITY,
        "spec_sha256": sha256_file(SPEC_PATH),
        "report": _artifact(report_path),
        "daily": _artifact(OUTPUT_ROOT / "cpp_screen_daily.parquet"),
        "campaigns": _artifact(OUTPUT_ROOT / "cpp_screen_campaigns.parquet"),
    }
    manifest["canonical_identity_sha256"] = canonical_sha256(manifest)
    atomic_json(OUTPUT_ROOT / "cpp_screen_manifest.json", manifest)


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    baseline = subparsers.add_parser("prepare-baseline")
    baseline.add_argument("--workers", type=int, default=2)
    band = subparsers.add_parser("prepare-band")
    band.add_argument("--workers", type=int, default=2)
    gates = subparsers.add_parser("prepare-gates")
    gates.add_argument("--workers", type=int, default=2)
    subparsers.add_parser("freeze-spec")
    screen = subparsers.add_parser("screen-cpp")
    screen.add_argument("--workers", type=int, default=2)
    args = parser.parse_args()
    if args.command == "prepare-baseline":
        prepare_baseline(workers=int(args.workers))
        return 0
    if args.command == "prepare-band":
        prepare_band(workers=int(args.workers))
        return 0
    if args.command == "prepare-gates":
        prepare_gates(workers=int(args.workers))
        return 0
    if args.command == "freeze-spec":
        freeze_spec()
        return 0
    if args.command == "screen-cpp":
        screen_cpp(workers=int(args.workers))
        return 0
    raise ValueError(args.command)


if __name__ == "__main__":
    raise SystemExit(main())
