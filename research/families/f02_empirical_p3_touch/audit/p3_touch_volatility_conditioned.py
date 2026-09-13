#!/usr/bin/env python3
"""Development-only volatility-conditioned P3 touch probability research."""

from __future__ import annotations

import argparse
import concurrent.futures
import gc
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

import lightgbm as lgb
import numpy as np
import pandas as pd
from scipy.optimize import minimize

from data_paths import resolve_portable_path
from features.feature_dag import P3_TOUCH_CONDITIONAL_GRAPH
from research.families.f02_empirical_p3_touch.audit.p3_touch_source_aware_expanded import (
    _verify_2025_selection,
    _verify_2026_inputs,
    sha256_file,
)
from research.families.f02_empirical_p3_touch.audit.p3_touch_window_context import (
    CONTEXT_FIELDS,
    align_contexts,
    apply_source_translation,
    extract_window_context,
    fit_source_translation,
    load_window_context_cache,
    window_context_cache_key,
    write_window_context_cache,
)
from research.families.f02_empirical_p3_touch.fill_probability import (
    FillProbabilityModel,
)
from research.governance.paths import resolve_research_path

ROOT = Path(__file__).resolve().parents[4]
SPEC_SCHEMA_VERSION = "narrowgate_p3_touch_volatility_conditioned.v4.spec"
REPORT_SCHEMA_VERSION = "narrowgate_p3_touch_volatility_conditioned.v4"
MODEL_SCHEMA_VERSION = "narrowgate_p3_touch_volatility_conditioned.model.v4"
SIDES = ("BUY", "SELL")
FEATURE_NAMES = (
    "distance_usdc_per_btc",
    "distance_z_slow_10s",
    "side_sell",
    "spread_usdc_per_btc",
    "spread_bps",
    "log_fast_sigma",
    "log_slow_sigma",
    "log_fast_slow_ratio",
    "regime_calm",
    "regime_shock",
)
MONOTONE_CONSTRAINTS = (-1, -1, 0, 0, 0, 0, 0, 0, 0, 0)


def canonical_sha256(payload: Any) -> str:
    raw = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _canonical_identity(payload: Mapping[str, Any], field: str) -> str:
    normalized = dict(payload)
    normalized.pop(field, None)
    return canonical_sha256(normalized)


def _require_identity(identity: Mapping[str, Any], label: str) -> Path:
    path = resolve_research_path(str(identity["path"]))
    if not path.is_file():
        raise FileNotFoundError(f"{label} missing: {path}")
    observed = sha256_file(path)
    expected = str(identity["sha256"])
    if observed != expected:
        raise ValueError(
            f"{label} hash mismatch: observed={observed} expected={expected}"
        )
    return path


def load_spec(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    path = path.expanduser().resolve()
    spec = json.loads(path.read_text(encoding="utf-8"))
    if spec.get("schema_version") != SPEC_SCHEMA_VERSION:
        raise ValueError("unsupported conditional P3 v4 spec schema")
    if _canonical_identity(spec, "canonical_spec_identity_sha256") != spec.get(
        "canonical_spec_identity_sha256"
    ):
        raise ValueError("conditional P3 v4 canonical spec hash mismatch")
    for label, identity in spec["identities"].items():
        _require_identity(identity, label)
    base_manifest = json.loads(
        resolve_research_path(
            str(spec["identities"]["base_day_manifest"]["path"])
        ).read_text(encoding="utf-8")
    )
    if base_manifest.get("identity") != "p3_touch_source_aware_expanded_v3_day_manifest":
        raise ValueError("conditional P3 v4 requires the frozen v3 day manifest")

    estimand = spec["estimand"]
    if estimand.get("event_type") != "touch":
        raise ValueError("conditional P3 event_type must be touch")
    if float(estimand.get("horizon_s", 0.0)) != 10.0:
        raise ValueError("conditional P3 horizon must be exactly 10 seconds")
    if estimand.get("distance_unit") != "USDC_per_BTC":
        raise ValueError("conditional P3 distance must use USDC_per_BTC")
    if bool(estimand.get("queue_included", True)):
        raise ValueError("conditional P3 cannot include queue conversion")
    if bool(estimand.get("order_lifecycle_included", True)):
        raise ValueError("conditional P3 cannot include order lifecycle")

    graph = spec["feature_graph"]
    if graph.get("graph_id") != P3_TOUCH_CONDITIONAL_GRAPH.graph_id:
        raise ValueError("conditional P3 graph id mismatch")
    if graph.get("sha256") != P3_TOUCH_CONDITIONAL_GRAPH.sha256():
        raise ValueError("conditional P3 graph hash mismatch")
    if tuple(spec["model"]["feature_names"]) != FEATURE_NAMES:
        raise ValueError("conditional P3 feature order mismatch")
    if tuple(spec["model"]["monotone_constraints"]) != MONOTONE_CONSTRAINTS:
        raise ValueError("conditional P3 monotonicity contract mismatch")
    forbidden_features = {"source", "source_identity", "year", "calendar_year"}
    if forbidden_features.intersection(spec["model"]["feature_names"]):
        raise ValueError("source identity or year cannot enter the trading feature vector")

    overlap = spec["source_translation"]["overlap_days"]
    fit_days = list(overlap["fit"])
    diagnostic_days = list(overlap["historical_diagnostic"])
    excluded_days = list(overlap["excluded_unread_or_embargo"])
    all_overlap = [*fit_days, *diagnostic_days, *excluded_days]
    if any(days != sorted(days) for days in (fit_days, diagnostic_days, excluded_days)):
        raise ValueError("source-overlap panels must be chronological")
    if len(all_overlap) != len(set(all_overlap)):
        raise ValueError("source-overlap panels must not overlap")
    if len(fit_days) != 6 or len(diagnostic_days) != 6 or len(excluded_days) != 6:
        raise ValueError("source-overlap contract must freeze 6/6/6 days")

    folds = spec["chronological_oof"]["folds"]
    flattened = [day for fold in folds for day in fold["test_days"]]
    expected = [
        *base_manifest["panels"]["historical_2026_validation"],
        *base_manifest["panels"]["historical_2026_test_diagnostic"],
    ]
    if flattened != expected:
        raise ValueError("chronological OOF folds do not cover the frozen 48 days")
    prior: list[str] = list(base_manifest["panels"]["fit_2026_current"])
    for fold in folds:
        if max(prior) >= min(fold["test_days"]):
            raise ValueError(f"fold {fold['fold_id']} is not chronological")
        prior.extend(fold["test_days"])

    permissions = spec["permissions"]
    for forbidden in (
        "prediction_authority",
        "quote_mapping_authority",
        "action_authority",
        "live_authority",
        "overwrite_current_v2_artifact",
    ):
        if bool(permissions.get(forbidden, False)):
            raise ValueError(f"conditional P3 v4 cannot grant {forbidden}")
    if not bool(permissions.get("historical_panels_previously_read", False)):
        raise ValueError("v4 historical panels must remain marked previously read")
    if bool(permissions.get("sealed_holdout_read", False)):
        raise ValueError("v4 may not read the sealed holdout")
    return spec, base_manifest


def regime_code(
    volatility_ratio: np.ndarray,
    *,
    calm_upper: float = 0.75,
    shock_lower: float = 1.50,
) -> np.ndarray:
    ratio = np.asarray(volatility_ratio, dtype=np.float64)
    if not 0.0 < float(calm_upper) < float(shock_lower):
        raise ValueError("regime boundaries must satisfy 0 < calm < shock")
    out = np.ones(ratio.shape, dtype=np.int8)
    out[ratio < float(calm_upper)] = 0
    out[ratio > float(shock_lower)] = 2
    return out


def build_model_matrix(
    context: Mapping[str, np.ndarray],
    *,
    side: str,
    distances: np.ndarray,
    row_indices: np.ndarray | None = None,
    horizon_s: float = 10.0,
    calm_upper: float = 0.75,
    shock_lower: float = 1.50,
) -> np.ndarray:
    if side not in SIDES:
        raise ValueError(f"unsupported P3 side: {side}")
    distances = np.asarray(distances, dtype=np.float64).reshape(-1)
    if row_indices is None:
        row_indices = np.arange(len(distances), dtype=np.int64)
    else:
        row_indices = np.asarray(row_indices, dtype=np.int64).reshape(-1)
    if distances.shape != row_indices.shape:
        raise ValueError("distances and row_indices must have equal shape")
    if np.any(distances <= 0.0):
        raise ValueError("P3 distances must be positive")

    start = np.asarray(context["start_ts_ms"], dtype=np.int64)[row_indices]
    ready = np.asarray(context["feature_ready_ts_ms"], dtype=np.int64)[row_indices]
    if np.any(ready > start):
        raise ValueError("P3 feature-ready time exceeds decision/window start")
    slow_sigma = np.asarray(context["slow_sigma"], dtype=np.float64)[row_indices]
    fast_sigma = np.asarray(context["fast_sigma"], dtype=np.float64)[row_indices]
    spread = np.asarray(context["spread"], dtype=np.float64)[row_indices]
    mid = np.asarray(context["mid"], dtype=np.float64)[row_indices]
    ratio = fast_sigma / slow_sigma
    regime = regime_code(
        ratio,
        calm_upper=calm_upper,
        shock_lower=shock_lower,
    )
    z = distances / (slow_sigma * math.sqrt(float(horizon_s)))
    matrix = np.column_stack(
        (
            distances,
            z,
            np.full(distances.shape, float(side == "SELL")),
            spread,
            10_000.0 * spread / mid,
            np.log(np.maximum(fast_sigma, 1e-12)),
            np.log(np.maximum(slow_sigma, 1e-12)),
            np.log(np.maximum(ratio, 1e-12)),
            (regime == 0).astype(np.float64),
            (regime == 2).astype(np.float64),
        )
    )
    if matrix.shape[1] != len(FEATURE_NAMES) or not np.all(np.isfinite(matrix)):
        raise ValueError("invalid conditional P3 feature matrix")
    return matrix.astype(np.float32, copy=False)


def deterministic_training_distances(
    *,
    day: str,
    side: str,
    n_windows: int,
    samples_per_window: int,
    distance_min: float,
    distance_max: float,
    seed: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Return deterministic stratified distance samples and source row indices."""

    if samples_per_window <= 0 or n_windows <= 0:
        raise ValueError("training distance sampling requires positive sizes")
    token = hashlib.sha256(
        f"{seed}|{day}|{side}".encode("ascii")
    ).digest()
    offset = int.from_bytes(token[:8], "little") / float(2**64)
    rows = np.arange(n_windows, dtype=np.float64)
    phase = np.mod(offset + rows * 0.6180339887498949, 1.0)
    strata = (
        np.arange(samples_per_window, dtype=np.float64)[None, :]
        + phase[:, None]
    ) / float(samples_per_window)
    distances = float(distance_min) + strata * (
        float(distance_max) - float(distance_min)
    )
    indices = np.repeat(np.arange(n_windows, dtype=np.int64), samples_per_window)
    return distances.reshape(-1), indices


def _fit_positive_platt(
    raw_probabilities: np.ndarray,
    labels: np.ndarray,
    weights: np.ndarray,
) -> dict[str, float]:
    p = np.clip(np.asarray(raw_probabilities, dtype=np.float64), 1e-6, 1.0 - 1e-6)
    y = np.asarray(labels, dtype=np.float64)
    w = np.asarray(weights, dtype=np.float64)
    logits = np.log(p / (1.0 - p))

    def objective(theta: np.ndarray) -> tuple[float, np.ndarray]:
        intercept = float(theta[0])
        slope = math.exp(float(theta[1]))
        eta = np.clip(intercept + slope * logits, -35.0, 35.0)
        q = 1.0 / (1.0 + np.exp(-eta))
        loss = -np.sum(w * (y * np.log(q) + (1.0 - y) * np.log1p(-q))) / np.sum(w)
        residual = (q - y) * w / np.sum(w)
        gradient = np.asarray(
            [np.sum(residual), np.sum(residual * slope * logits)],
            dtype=np.float64,
        )
        return float(loss), gradient

    result = minimize(
        lambda theta: objective(theta)[0],
        x0=np.asarray([0.0, 0.0]),
        jac=lambda theta: objective(theta)[1],
        method="L-BFGS-B",
        options={"maxiter": 200, "ftol": 1e-12},
    )
    if not result.success:
        raise RuntimeError(f"positive Platt calibration failed: {result.message}")
    return {
        "intercept": float(result.x[0]),
        "slope": float(math.exp(float(result.x[1]))),
        "training_logloss": float(result.fun),
    }


def apply_positive_platt(
    raw_probabilities: np.ndarray,
    calibration: Mapping[str, float],
) -> np.ndarray:
    p = np.clip(np.asarray(raw_probabilities, dtype=np.float64), 1e-6, 1.0 - 1e-6)
    logit = np.log(p / (1.0 - p))
    eta = np.clip(
        float(calibration["intercept"])
        + float(calibration["slope"]) * logit,
        -35.0,
        35.0,
    )
    return 1.0 / (1.0 + np.exp(-eta))


class ConditionalTouchModel:
    """LightGBM conditional surface plus one shared positive calibrator."""

    def __init__(
        self,
        booster: lgb.Booster,
        calibration: Mapping[str, float],
        feature_contract: Mapping[str, Any],
    ) -> None:
        self.booster = booster
        self.calibration = dict(calibration)
        self.feature_contract = dict(feature_contract)

    def predict_matrix(self, matrix: np.ndarray) -> np.ndarray:
        raw = self.booster.predict(np.asarray(matrix, dtype=np.float32))
        return apply_positive_platt(raw, self.calibration)

    def predict(
        self,
        context: Mapping[str, np.ndarray],
        *,
        side: str,
        distances: np.ndarray,
        row_indices: np.ndarray,
    ) -> np.ndarray:
        contract = self.feature_contract
        matrix = build_model_matrix(
            context,
            side=side,
            distances=distances,
            row_indices=row_indices,
            horizon_s=float(contract["horizon_s"]),
            calm_upper=float(contract["calm_upper"]),
            shock_lower=float(contract["shock_lower"]),
        )
        return self.predict_matrix(matrix)


def _context_task(payload: Mapping[str, Any]) -> dict[str, Any]:
    cache_path = resolve_portable_path(str(payload["cache_path"]), root=ROOT).resolve()
    expected_key = str(payload["cache_key"])
    context = load_window_context_cache(cache_path, expected_key=expected_key)
    cache_hit = context is not None
    if context is None:
        context = extract_window_context(
            day=str(payload["day"]),
            bbo_path=resolve_portable_path(str(payload["bbo_path"]), root=ROOT),
            trade_path=resolve_portable_path(str(payload["trade_path"]), root=ROOT),
            horizon_s=float(payload["horizon_s"]),
            max_bbo_age_ms=int(payload["max_bbo_age_ms"]),
            fast_window_s=int(payload["fast_window_s"]),
            slow_window_s=int(payload["slow_window_s"]),
            variance_floor=float(payload["variance_floor"]),
        )
        write_window_context_cache(
            cache_path,
            cache_key=expected_key,
            context=context,
        )
    return {
        "source": str(payload["source"]),
        "panel": str(payload["panel"]),
        "day": str(payload["day"]),
        "cache_hit": bool(cache_hit),
        "cache_path": str(cache_path),
        "cache_key": expected_key,
        "context": context,
    }


def _normalize_side(value: Any) -> str:
    text = str(value).upper()
    if "BUY" in text or text.endswith("BID") or text in {"0", "SIDE.BUY"}:
        return "BUY"
    if "SELL" in text or text.endswith("ASK") or text in {"1", "SIDE.SELL"}:
        return "SELL"
    raise ValueError(f"unsupported quote-trace side: {value!r}")


def _policy_distance_day_task(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Rebuild one current-v2 path and retain only quote-distance support."""

    from models import backtest_tick as bt
    from models.backtest_config import (
        add_fill_probability_params,
        load_tick_base_params,
    )
    from models.data_windows import load_tick_window

    day = str(payload["day"])
    book_root = resolve_portable_path(str(payload["book_root"])).resolve()
    cache_dir = resolve_portable_path(str(payload["cache_dir"])).resolve()
    feature_dir = resolve_portable_path(str(payload["feature_dir"])).resolve()
    model_dir = resolve_portable_path(str(payload["model_dir"])).resolve()
    config_path = resolve_portable_path(str(payload["config_path"])).resolve()
    trace_quotes_max = int(payload["trace_quotes_max"])

    bt.BBO_DIR = book_root / "bbo"
    bt.L2_DIR = book_root / "l2"
    bt.configure_symbol("BTCUSDC")
    params = load_tick_base_params(
        symbol="BTCUSDC",
        config_path=config_path,
        configure_symbol=bt.configure_symbol,
        require_historical_bbo=True,
    )
    params.update(
        {
            "execution_trade_source": "trades",
            "market_context_warmup_days": 1,
            "replay_event_clock": "merged",
            "queue_ahead_mode": "exact_level",
            "queue_l2_cancel_ahead_enabled": False,
            "_formal_quality_allowed_days": [
                (date.fromisoformat(day) - timedelta(days=1)).isoformat(),
                day,
            ],
            "collect_curves": False,
            "trace_quotes_max": trace_quotes_max,
            "trace_fills_max": 0,
            "dynamic_fill_hazard_action_enabled": False,
            "dynamic_fill_hazard_shadow_enabled": False,
            "dynamic_fill_hazard_cpp_parity_enabled": False,
            "buy_fill_selection_live_enabled": False,
            "buy_fill_selection_shadow_enabled": False,
            "sync_adjust_replay_mode": "disabled",
            "markout_side_asymmetry_sign": 1.0,
            "window_cache_write_enabled": False,
            "model_dir": str(model_dir),
            "resolved_model_dir": str(model_dir),
            "ml_enabled": True,
        }
    )
    bt.configure_symbol("BTCUSDC", model_dir_override=model_dir)
    window = load_tick_window(
        day,
        params,
        load_ml=True,
        require_ml=True,
        run_ml_inference=True,
        feature_dir=feature_dir,
        require_target_feature_files=True,
        cross_market_enabled=True,
        with_ml_cache=False,
        require_historical_bbo=True,
        require_formal_l2=False,
        cache_dir=cache_dir,
        refresh_cache=False,
    )
    if window.book_source_authority != "native_formal_lifecycle":
        raise ValueError(
            f"{day} policy support lacks native authority: "
            f"{window.book_source_authority}"
        )
    add_fill_probability_params(
        params,
        model_path=resolve_portable_path(
            str(payload["current_v2_p3_path"]), root=ROOT
        ).resolve(),
        label="P3 current v2 policy support",
        strict=True,
    )
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
    trace = list(result.get("_quote_trace") or [])
    if len(trace) >= trace_quotes_max:
        raise RuntimeError(f"policy-support quote trace limit bound on {day}")
    rows: list[dict[str, Any]] = []
    for row in trace:
        distance = row.get("final_quote_delta_to_bbo")
        quote_ts = row.get("quote_ts")
        if distance is None or quote_ts is None:
            continue
        distance_value = float(distance)
        if not np.isfinite(distance_value) or distance_value <= 0.0:
            continue
        if row.get("fill_eligible") is False:
            continue
        rows.append(
            {
                "side": _normalize_side(row.get("side")),
                "quote_ts": int(quote_ts),
                "distance": distance_value,
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        raise RuntimeError(f"policy-support path has no valid quotes on {day}")
    frame.sort_values(["quote_ts", "side"], inplace=True)
    frame["prediction_bucket"] = frame["quote_ts"] // 10_000
    frame = frame.drop_duplicates(["prediction_bucket", "side"], keep="first")
    return {
        "day": day,
        "trace_rows": int(len(trace)),
        "canonical_rows": int(len(frame)),
        "BUY": frame.loc[frame["side"].eq("BUY"), "distance"].to_numpy(
            dtype=np.float64
        ),
        "SELL": frame.loc[frame["side"].eq("SELL"), "distance"].to_numpy(
            dtype=np.float64
        ),
    }


def _distance_weights(
    values: np.ndarray,
    grid: np.ndarray,
) -> np.ndarray:
    values = np.asarray(values, dtype=np.float64)
    grid = np.asarray(grid, dtype=np.float64)
    if values.size == 0:
        raise ValueError("policy support requires non-empty quote distances")
    step = float(grid[1] - grid[0])
    indices = np.rint((values - float(grid[0])) / step).astype(np.int64)
    indices = np.clip(indices, 0, len(grid) - 1)
    counts = np.bincount(indices, minlength=len(grid)).astype(np.float64)
    return counts / np.sum(counts)


def _build_policy_support(
    *,
    spec: Mapping[str, Any],
    grid: np.ndarray,
    workers: int,
) -> tuple[dict[str, np.ndarray], pd.DataFrame, dict[str, Any]]:
    quote_spec = json.loads(
        resolve_research_path(
            str(spec["identities"]["v3_quote_path_spec"]["path"])
        ).read_text(encoding="utf-8")
    )
    tasks: list[dict[str, Any]] = []
    for panel in quote_spec["panels"]:
        for day in panel["days"]:
            tasks.append(
                {
                    "day": str(day),
                    "book_root": quote_spec["paths"]["book_root"],
                    "cache_dir": quote_spec["paths"]["cache_dir"],
                    "feature_dir": quote_spec["paths"]["feature_dir"],
                    "model_dir": quote_spec["paths"]["model_dir"],
                    "config_path": quote_spec["identities"]["operational_config"][
                        "path"
                    ],
                    "current_v2_p3_path": quote_spec["identities"][
                        "current_v2_p3"
                    ]["path"],
                    "trace_quotes_max": int(
                        quote_spec["replay"]["trace_quotes_max_per_arm_day"]
                    ),
                }
            )
    results: list[dict[str, Any]] = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=int(workers)) as pool:
        futures = {pool.submit(_policy_distance_day_task, task): task["day"] for task in tasks}
        for completed, future in enumerate(
            concurrent.futures.as_completed(futures), start=1
        ):
            results.append(future.result())
            print(f"P3 v4 policy support: {completed}/{len(tasks)} days", flush=True)
    results.sort(key=lambda row: row["day"])
    weights: dict[str, np.ndarray] = {}
    daily_rows: list[dict[str, Any]] = []
    for side in SIDES:
        chunks = [row[side] for row in results]
        weights[side] = _distance_weights(np.concatenate(chunks), grid)
        for row in results:
            values = np.asarray(row[side], dtype=np.float64)
            daily_rows.append(
                {
                    "day": row["day"],
                    "side": side,
                    "canonical_prediction_bucket_quotes": int(values.size),
                    "mean_distance_usdc_per_btc": float(np.mean(values)),
                    "p10_distance_usdc_per_btc": float(np.quantile(values, 0.10)),
                    "p50_distance_usdc_per_btc": float(np.quantile(values, 0.50)),
                    "p90_distance_usdc_per_btc": float(np.quantile(values, 0.90)),
                }
            )
    summary = {
        "days": len(results),
        "trace_rows": int(sum(row["trace_rows"] for row in results)),
        "canonical_rows": int(sum(row["canonical_rows"] for row in results)),
        "per_side_rows": {
            side: int(sum(len(row[side]) for row in results)) for side in SIDES
        },
        "one_quote_per_10s_prediction_bucket_side": True,
        "economic_outcomes_consumed": False,
    }
    return weights, pd.DataFrame(daily_rows), summary


def _training_arrays(
    *,
    provider_days: Sequence[str],
    native_days: Sequence[str],
    contexts: Mapping[tuple[str, str], Mapping[str, np.ndarray]],
    spec: Mapping[str, Any],
    source_weights: Mapping[str, float] | None = None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, dict[str, Any]]:
    sampling = spec["model"]["distance_sampling"]
    feature_contract = spec["model"]["feature_contract"]
    sources = {
        "provider_translated": list(provider_days),
        "native": list(native_days),
    }
    shares = dict(
        source_weights
        or spec["model"].get(
            "source_weights",
            {"provider_translated": 0.5, "native": 0.5},
        )
    )
    if set(shares) != set(sources):
        raise ValueError("conditional P3 source-weight keys are incomplete")
    if any(float(value) < 0.0 for value in shares.values()):
        raise ValueError("conditional P3 source weights must be non-negative")
    if not np.isclose(sum(float(value) for value in shares.values()), 1.0):
        raise ValueError("conditional P3 source weights must sum to one")
    x_parts: list[np.ndarray] = []
    y_parts: list[np.ndarray] = []
    w_parts: list[np.ndarray] = []
    source_rows: dict[str, int] = {}
    for source, days in sources.items():
        share = float(shares[source])
        if share == 0.0:
            source_rows[source] = 0
            continue
        if not days:
            raise ValueError(f"conditional P3 training source {source} has no days")
        source_count = 0
        for day in days:
            context = contexts[(source, day)]
            n_windows = len(context["start_ts_ms"])
            for side in SIDES:
                distances, row_indices = deterministic_training_distances(
                    day=day,
                    side=side,
                    n_windows=n_windows,
                    samples_per_window=int(sampling["samples_per_window"]),
                    distance_min=float(sampling["minimum"]),
                    distance_max=float(sampling["maximum"]),
                    seed=int(sampling["seed"]),
                )
                matrix = build_model_matrix(
                    context,
                    side=side,
                    distances=distances,
                    row_indices=row_indices,
                    horizon_s=float(feature_contract["horizon_s"]),
                    calm_upper=float(feature_contract["calm_upper"]),
                    shock_lower=float(feature_contract["shock_lower"]),
                )
                labels = (
                    np.asarray(context[side], dtype=np.float64)[row_indices]
                    >= distances
                ).astype(np.uint8)
                # Each source contributes half the total weight; every day and
                # side contributes equally within its source.
                weight = np.full(
                    labels.shape,
                    share / (len(days) * len(SIDES) * len(labels)),
                    dtype=np.float64,
                )
                x_parts.append(matrix)
                y_parts.append(labels)
                w_parts.append(weight)
                source_count += len(labels)
        source_rows[source] = int(source_count)
    if not x_parts:
        raise ValueError("conditional P3 sampling produced no rows")
    matrix = np.concatenate(x_parts, axis=0)
    labels = np.concatenate(y_parts, axis=0)
    weights = np.concatenate(w_parts, axis=0)
    weights *= float(len(weights)) / float(np.sum(weights))
    return matrix, labels, weights.astype(np.float32), {
        "rows": int(len(labels)),
        "positive_rate": float(np.mean(labels)),
        "source_rows": source_rows,
        "source_weight_sums": {
            source: float(shares[source]) for source in sources
        },
    }


def _fit_conditional_model(
    *,
    provider_days: Sequence[str],
    core_native_days: Sequence[str],
    calibration_native_days: Sequence[str],
    contexts: Mapping[tuple[str, str], Mapping[str, np.ndarray]],
    spec: Mapping[str, Any],
) -> tuple[ConditionalTouchModel, dict[str, Any]]:
    started = time.perf_counter()
    matrix, labels, weights, training_summary = _training_arrays(
        provider_days=provider_days,
        native_days=core_native_days,
        contexts=contexts,
        spec=spec,
    )
    parameters = dict(spec["model"]["lightgbm_parameters"])
    parameters["monotone_constraints"] = list(MONOTONE_CONSTRAINTS)
    dataset = lgb.Dataset(
        matrix,
        label=labels,
        weight=weights,
        feature_name=list(FEATURE_NAMES),
        free_raw_data=True,
    )
    booster = lgb.train(
        parameters,
        dataset,
        num_boost_round=int(spec["model"]["num_boost_round"]),
    )
    del matrix, labels, weights, dataset
    gc.collect()

    calibration_matrix, calibration_labels, calibration_weights, calibration_summary = (
        _training_arrays(
            provider_days=[],
            native_days=calibration_native_days,
            contexts=contexts,
            spec=spec,
            source_weights={"provider_translated": 0.0, "native": 1.0},
        )
    )
    raw_calibration = booster.predict(calibration_matrix)
    calibration = _fit_positive_platt(
        raw_calibration,
        calibration_labels,
        calibration_weights,
    )
    feature_contract = dict(spec["model"]["feature_contract"])
    model = ConditionalTouchModel(booster, calibration, feature_contract)
    summary = {
        "provider_days": list(provider_days),
        "core_native_days": list(core_native_days),
        "calibration_native_days": list(calibration_native_days),
        "training": training_summary,
        "calibration_rows": calibration_summary,
        "positive_platt": calibration,
        "runtime_s": float(time.perf_counter() - started),
    }
    del calibration_matrix, calibration_labels, calibration_weights, raw_calibration
    gc.collect()
    return model, summary


def _predict_grid(
    model: ConditionalTouchModel,
    context: Mapping[str, np.ndarray],
    *,
    side: str,
    grid: np.ndarray,
) -> np.ndarray:
    n_windows = len(context["start_ts_ms"])
    row_indices = np.repeat(np.arange(n_windows, dtype=np.int64), len(grid))
    distances = np.tile(np.asarray(grid, dtype=np.float64), n_windows)
    predictions = model.predict(
        context,
        side=side,
        distances=distances,
        row_indices=row_indices,
    )
    return predictions.reshape(n_windows, len(grid))


def _static_grid(
    model: FillProbabilityModel,
    grid: np.ndarray,
    n_windows: int,
) -> np.ndarray:
    values = np.interp(
        np.asarray(grid, dtype=np.float64),
        np.asarray(model.delta_grid, dtype=np.float64),
        np.asarray(model.probability_grid, dtype=np.float64),
    )
    return np.broadcast_to(values[None, :], (n_windows, len(grid)))


def _quantile_bins(values: np.ndarray, cutpoints: Sequence[float]) -> np.ndarray:
    return np.digitize(
        np.asarray(values, dtype=np.float64),
        np.asarray(cutpoints, dtype=np.float64),
        right=True,
    ).astype(np.int8)


def _slice_masks(
    context: Mapping[str, np.ndarray],
    *,
    fast_cutpoints: Sequence[float],
    slow_cutpoints: Sequence[float],
    calm_upper: float,
    shock_lower: float,
) -> list[tuple[str, str, np.ndarray]]:
    n = len(context["start_ts_ms"])
    fast_bins = _quantile_bins(context["fast_sigma"], fast_cutpoints)
    slow_bins = _quantile_bins(context["slow_sigma"], slow_cutpoints)
    regimes = regime_code(
        context["volatility_ratio"],
        calm_upper=calm_upper,
        shock_lower=shock_lower,
    )
    masks: list[tuple[str, str, np.ndarray]] = [
        ("pooled", "all", np.ones(n, dtype=bool))
    ]
    masks.extend(
        ("fast_volatility_quartile", str(index + 1), fast_bins == index)
        for index in range(4)
    )
    masks.extend(
        ("slow_volatility_quartile", str(index + 1), slow_bins == index)
        for index in range(4)
    )
    masks.extend(
        ("causal_regime", name, regimes == index)
        for index, name in enumerate(("calm", "balanced", "shock"))
    )
    return masks


def _score_day(
    *,
    day: str,
    panel: str,
    fold_id: str,
    model: ConditionalTouchModel,
    current_v2: FillProbabilityModel,
    context: Mapping[str, np.ndarray],
    grid: np.ndarray,
    policy_weights: Mapping[str, np.ndarray],
    volatility_cutpoints: Mapping[str, Sequence[float]],
    feature_contract: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    daily_rows: list[dict[str, Any]] = []
    calibration_rows: list[dict[str, Any]] = []
    monotonicity = {
        "comparisons": 0,
        "violations": 0,
        "max_positive_difference": 0.0,
    }
    for side in SIDES:
        reach = np.asarray(context[side], dtype=np.float64)
        observed = reach[:, None] >= grid[None, :]
        candidate = _predict_grid(model, context, side=side, grid=grid)
        current = _static_grid(current_v2, grid, len(reach))
        differences = np.diff(candidate, axis=1)
        monotonicity["comparisons"] += int(differences.size)
        monotonicity["violations"] += int(np.sum(differences > 1e-12))
        monotonicity["max_positive_difference"] = max(
            float(monotonicity["max_positive_difference"]),
            float(np.max(differences)) if differences.size else 0.0,
        )
        for model_name, predictions in (
            ("current_v2", current),
            ("conditional_v4", candidate),
        ):
            losses = np.square(predictions - observed)
            daily_rows.append(
                {
                    "panel": panel,
                    "fold_id": fold_id,
                    "day": day,
                    "side": side,
                    "model": model_name,
                    "windows": int(len(reach)),
                    "uniform_integrated_brier": float(np.mean(losses)),
                    "policy_support_brier": float(
                        np.mean(losses @ np.asarray(policy_weights[side]))
                    ),
                }
            )
            for slice_type, slice_value, mask in _slice_masks(
                context,
                fast_cutpoints=volatility_cutpoints["fast_sigma"],
                slow_cutpoints=volatility_cutpoints["slow_sigma"],
                calm_upper=float(feature_contract["calm_upper"]),
                shock_lower=float(feature_contract["shock_lower"]),
            ):
                if not np.any(mask):
                    continue
                mean_prediction = np.mean(predictions[mask], axis=0)
                mean_observed = np.mean(observed[mask], axis=0)
                mean_brier = np.mean(losses[mask], axis=0)
                calibration_rows.extend(
                    {
                        "panel": panel,
                        "fold_id": fold_id,
                        "day": day,
                        "side": side,
                        "model": model_name,
                        "slice_type": slice_type,
                        "slice_value": slice_value,
                        "windows": int(np.sum(mask)),
                        "distance_usdc_per_btc": float(distance),
                        "predicted_probability": float(predicted),
                        "observed_probability": float(actual),
                        "prediction_minus_observation": float(predicted - actual),
                        "brier": float(brier),
                    }
                    for distance, predicted, actual, brier in zip(
                        grid,
                        mean_prediction,
                        mean_observed,
                        mean_brier,
                        strict=True,
                    )
                )
        del observed, candidate, current, differences
        gc.collect()
    return daily_rows, calibration_rows, monotonicity


def _bootstrap_day_delta(
    values: np.ndarray,
    *,
    draws: int,
    seed: int,
) -> dict[str, Any]:
    values = np.asarray(values, dtype=np.float64)
    if values.size == 0:
        raise ValueError("day-cluster bootstrap requires non-empty values")
    rng = np.random.default_rng(seed)
    sampled = rng.choice(values, size=(draws, len(values)), replace=True).mean(axis=1)
    return {
        "days": int(len(values)),
        "mean_delta": float(np.mean(values)),
        "median_delta": float(np.median(values)),
        "improved_day_rate": float(np.mean(values < 0.0)),
        "ci95_day_cluster_bootstrap": [
            float(np.quantile(sampled, 0.025)),
            float(np.quantile(sampled, 0.975)),
        ],
    }


def _proper_score_summary(
    daily: pd.DataFrame,
    *,
    draws: int,
    seed: int,
) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for panel_index, panel in enumerate(sorted(daily["panel"].unique())):
        output[panel] = {}
        subset = daily[daily["panel"].eq(panel)]
        for metric_index, metric in enumerate(
            ("uniform_integrated_brier", "policy_support_brier")
        ):
            output[panel][metric] = {}
            wide = subset.pivot(
                index=["day", "side"],
                columns="model",
                values=metric,
            ).dropna()
            wide["delta"] = wide["conditional_v4"] - wide["current_v2"]
            for side_index, side in enumerate((*SIDES, "POOLED")):
                if side == "POOLED":
                    values = wide["delta"].groupby(level="day").mean().to_numpy()
                else:
                    values = wide.xs(side, level="side")["delta"].to_numpy()
                output[panel][metric][side] = _bootstrap_day_delta(
                    values,
                    draws=draws,
                    seed=(
                        seed
                        + panel_index * 100
                        + metric_index * 10
                        + side_index
                    ),
                )
    return output


def _aggregate_calibration(calibration: pd.DataFrame) -> pd.DataFrame:
    keys = [
        "panel",
        "side",
        "model",
        "slice_type",
        "slice_value",
        "distance_usdc_per_btc",
    ]
    frame = calibration.copy()
    for field in ("predicted_probability", "observed_probability", "brier"):
        frame[f"weighted_{field}"] = frame[field] * frame["windows"]
    grouped = frame.groupby(keys, sort=True, as_index=False).agg(
        windows=("windows", "sum"),
        weighted_predicted_probability=("weighted_predicted_probability", "sum"),
        weighted_observed_probability=("weighted_observed_probability", "sum"),
        weighted_brier=("weighted_brier", "sum"),
    )
    grouped["predicted_probability"] = (
        grouped["weighted_predicted_probability"] / grouped["windows"]
    )
    grouped["observed_probability"] = (
        grouped["weighted_observed_probability"] / grouped["windows"]
    )
    grouped["prediction_minus_observation"] = (
        grouped["predicted_probability"] - grouped["observed_probability"]
    )
    grouped["brier"] = grouped["weighted_brier"] / grouped["windows"]
    return grouped[
        keys
        + [
            "windows",
            "predicted_probability",
            "observed_probability",
            "prediction_minus_observation",
            "brier",
        ]
    ]


def _calibration_gate_summary(
    aggregate: pd.DataFrame,
    *,
    minimum_windows: int,
    max_iace_worsening: float,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for keys, group in aggregate.groupby(
        ["panel", "side", "model", "slice_type", "slice_value"],
        sort=True,
    ):
        panel, side, model, slice_type, slice_value = keys
        supported = int(group["windows"].min()) >= int(minimum_windows)
        rows.append(
            {
                "panel": panel,
                "side": side,
                "model": model,
                "slice_type": slice_type,
                "slice_value": slice_value,
                "minimum_distance_windows": int(group["windows"].min()),
                "supported": bool(supported),
                "integrated_absolute_calibration_error": float(
                    np.mean(np.abs(group["prediction_minus_observation"]))
                ),
            }
        )
    frame = pd.DataFrame(rows)
    wide = frame.pivot(
        index=["panel", "side", "slice_type", "slice_value"],
        columns="model",
        values="integrated_absolute_calibration_error",
    ).dropna()
    support_lookup = frame.groupby(
        ["panel", "side", "slice_type", "slice_value"], sort=True
    )["supported"].all()
    wide["supported"] = support_lookup
    wide["delta_candidate_minus_current"] = (
        wide["conditional_v4"] - wide["current_v2"]
    )
    supported = wide[wide["supported"]]
    passing = supported[
        supported["delta_candidate_minus_current"] <= float(max_iace_worsening)
    ]
    return {
        "minimum_windows": int(minimum_windows),
        "max_iace_worsening": float(max_iace_worsening),
        "supported_cells": int(len(supported)),
        "passing_cells": int(len(passing)),
        "all_supported_cells_passed": bool(len(supported) > 0 and len(passing) == len(supported)),
        "maximum_supported_iace_worsening": float(
            supported["delta_candidate_minus_current"].max()
            if len(supported)
            else float("nan")
        ),
        "cells": wide.reset_index().to_dict(orient="records"),
    }


def _verify_overlap_inputs(
    *,
    spec: Mapping[str, Any],
    base_manifest: Mapping[str, Any],
    existing_rows: Sequence[Mapping[str, str]],
) -> tuple[list[dict[str, str]], dict[str, Path]]:
    quality_csv = pd.read_csv(
        resolve_portable_path(
            str(spec["identities"]["tardis_combined_quality"]["path"]),
            root=ROOT,
        ),
        dtype={"day": str},
    )
    candidates = set(
        quality_csv.loc[
            quality_csv["provider_normalized_replay_candidate"].astype(bool),
            "day",
        ]
    )
    overlap = spec["source_translation"]["overlap_days"]
    selected = [*overlap["fit"], *overlap["historical_diagnostic"]]
    if not set(selected).issubset(candidates):
        raise ValueError("source-translation selected a non-candidate Tardis day")

    provider = base_manifest["source_identities"]["fit_2025_provider"]
    bbo_root = resolve_portable_path(str(provider["bbo_root"]), root=ROOT).resolve()
    quality_root = resolve_portable_path(
        str(provider["quality_root"]), root=ROOT
    ).resolve()
    trade_root = resolve_portable_path(str(provider["trade_root"]), root=ROOT).resolve()
    native_trade_hash = {
        str(row["day"]): str(row["sha256"])
        for row in existing_rows
        if str(row["kind"]) == "aggTrades"
    }
    rows: list[dict[str, str]] = []
    for day in selected:
        quality_path = quality_root / f"BTCUSDC-{day}.json"
        bbo_path = bbo_root / f"BTCUSDC-bbo-{day}.parquet"
        trade_path = trade_root / f"BTCUSDC-aggTrades-{day}.csv"
        if not quality_path.is_file() or not bbo_path.is_file() or not trade_path.is_file():
            raise FileNotFoundError(f"missing P3 source-overlap inputs for {day}")
        quality = json.loads(quality_path.read_text(encoding="utf-8"))
        if quality.get("provider_normalized_replay_candidate") is not True:
            raise ValueError(f"P3 source-overlap day is not provider-valid: {day}")
        bbo_hash = sha256_file(bbo_path)
        if bbo_hash != str(quality["bbo_output"]["sha256"]):
            raise ValueError(f"P3 overlap BBO hash mismatch: {day}")
        trade_hash = sha256_file(trade_path)
        if trade_hash != native_trade_hash.get(day):
            raise ValueError(f"P3 overlap official trade identity mismatch: {day}")
        rows.extend(
            [
                {"day": day, "kind": "provider_bbo", "sha256": bbo_hash},
                {"day": day, "kind": "provider_quality", "sha256": sha256_file(quality_path)},
                {"day": day, "kind": "aggTrades", "sha256": trade_hash},
            ]
        )
    return rows, {"bbo": bbo_root, "trade": trade_root}


def _assemble_contexts(
    *,
    spec: Mapping[str, Any],
    base_manifest: Mapping[str, Any],
    cache_dir: Path,
    workers: int,
) -> tuple[
    dict[tuple[str, str], dict[str, np.ndarray]],
    pd.DataFrame,
    list[dict[str, str]],
]:
    rows_2025, roots_2025 = _verify_2025_selection(base_manifest)
    rows_2026, roots_2026 = _verify_2026_inputs(spec, base_manifest)
    rows_overlap, roots_overlap = _verify_overlap_inputs(
        spec=spec,
        base_manifest=base_manifest,
        existing_rows=rows_2026,
    )
    lookup: dict[tuple[str, str, str], str] = {}
    for source, rows in (
        ("provider", rows_2025),
        ("native", rows_2026),
        ("provider_overlap", rows_overlap),
    ):
        for row in rows:
            kind = str(row["kind"])
            if kind == "provider_bbo":
                kind = "bbo"
            if kind == "provider_quality":
                continue
            key = (source, str(row["day"]), kind)
            if key in lookup and lookup[key] != str(row["sha256"]):
                raise ValueError(f"conflicting P3 v4 input identity: {key}")
            lookup[key] = str(row["sha256"])

    window = spec["window_context"]
    extractor_sha = str(spec["identities"]["window_context_implementation"]["sha256"])
    tasks: list[dict[str, Any]] = []

    def append_tasks(
        source: str,
        panel: str,
        days: Sequence[str],
        roots: Mapping[str, Path],
        lookup_source: str,
    ) -> None:
        for day in days:
            bbo_sha = lookup[(lookup_source, day, "bbo")]
            trade_sha = lookup[(lookup_source, day, "aggTrades")]
            key = window_context_cache_key(
                day=day,
                bbo_sha256=bbo_sha,
                trade_sha256=trade_sha,
                extractor_sha256=extractor_sha,
                horizon_s=float(spec["estimand"]["horizon_s"]),
                max_bbo_age_ms=int(spec["estimand"]["max_bbo_age_ms"]),
                fast_window_s=int(window["fast_window_s"]),
                slow_window_s=int(window["slow_window_s"]),
                variance_floor=float(window["variance_floor"]),
            )
            tasks.append(
                {
                    "source": source,
                    "panel": panel,
                    "day": day,
                    "bbo_path": str(roots["bbo"] / f"BTCUSDC-bbo-{day}.parquet"),
                    "trade_path": str(roots["trade"] / f"BTCUSDC-aggTrades-{day}.csv"),
                    "cache_path": str(cache_dir / f"BTCUSDC-{source}-{day}-{key}.npz"),
                    "cache_key": key,
                    "horizon_s": float(spec["estimand"]["horizon_s"]),
                    "max_bbo_age_ms": int(spec["estimand"]["max_bbo_age_ms"]),
                    "fast_window_s": int(window["fast_window_s"]),
                    "slow_window_s": int(window["slow_window_s"]),
                    "variance_floor": float(window["variance_floor"]),
                }
            )

    append_tasks(
        "provider",
        "fit_2025_provider",
        base_manifest["panels"]["fit_2025_provider"],
        roots_2025,
        "provider",
    )
    for panel in (
        "fit_2026_current",
        "historical_2026_validation",
        "historical_2026_test_diagnostic",
    ):
        append_tasks(
            "native",
            panel,
            base_manifest["panels"][panel],
            roots_2026,
            "native",
        )
    overlap = spec["source_translation"]["overlap_days"]
    append_tasks(
        "provider_overlap",
        "source_translation_fit",
        overlap["fit"],
        roots_overlap,
        "provider_overlap",
    )
    append_tasks(
        "provider_overlap",
        "source_translation_historical_diagnostic",
        overlap["historical_diagnostic"],
        roots_overlap,
        "provider_overlap",
    )

    contexts: dict[tuple[str, str], dict[str, np.ndarray]] = {}
    cache_rows: list[dict[str, Any]] = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=int(workers)) as pool:
        futures = {pool.submit(_context_task, task): (task["source"], task["day"]) for task in tasks}
        for completed, future in enumerate(
            concurrent.futures.as_completed(futures), start=1
        ):
            result = future.result()
            key = (str(result["source"]), str(result["day"]))
            if key in contexts:
                raise ValueError(f"duplicate conditional P3 context: {key}")
            contexts[key] = result["context"]
            cache_rows.append(
                {
                    "source": result["source"],
                    "panel": result["panel"],
                    "day": result["day"],
                    "windows": int(len(result["context"]["start_ts_ms"])),
                    "cache_hit": bool(result["cache_hit"]),
                    "cache_path": result["cache_path"],
                    "cache_key": result["cache_key"],
                }
            )
            if completed % 10 == 0 or completed == len(tasks):
                print(f"P3 v4 contexts: {completed}/{len(tasks)} days", flush=True)
    return contexts, pd.DataFrame(cache_rows), [*rows_2025, *rows_2026, *rows_overlap]


def _source_residual_rows(
    *,
    days: Sequence[str],
    contexts: Mapping[tuple[str, str], Mapping[str, np.ndarray]],
    translation: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for day in days:
        native, provider = align_contexts(
            contexts[("native", day)],
            apply_source_translation(contexts[("provider_overlap", day)], translation),
        )
        rows.append(
            {
                "day": day,
                "common_windows": int(len(native["start_ts_ms"])),
                "median_bid_residual_native_minus_corrected_provider": float(
                    np.median(native["best_bid"] - provider["best_bid"])
                ),
                "median_ask_residual_native_minus_corrected_provider": float(
                    np.median(native["best_ask"] - provider["best_ask"])
                ),
                "median_log_fast_sigma_residual": float(
                    np.median(np.log(native["fast_sigma"] / provider["fast_sigma"]))
                ),
                "median_log_slow_sigma_residual": float(
                    np.median(np.log(native["slow_sigma"] / provider["slow_sigma"]))
                ),
            }
        )
    return rows


def _source_prediction_diagnostic(
    *,
    day: str,
    fold_id: str,
    model: ConditionalTouchModel,
    native: Mapping[str, np.ndarray],
    provider: Mapping[str, np.ndarray],
    grid: np.ndarray,
    policy_weights: Mapping[str, np.ndarray],
) -> list[dict[str, Any]]:
    native_aligned, provider_aligned = align_contexts(native, provider)
    rows: list[dict[str, Any]] = []
    for side in SIDES:
        native_prediction = _predict_grid(
            model, native_aligned, side=side, grid=grid
        )
        provider_prediction = _predict_grid(
            model, provider_aligned, side=side, grid=grid
        )
        absolute = np.abs(native_prediction - provider_prediction)
        rows.append(
            {
                "day": day,
                "fold_id": fold_id,
                "side": side,
                "common_windows": int(len(native_aligned["start_ts_ms"])),
                "uniform_mean_absolute_prediction_difference": float(
                    np.mean(absolute)
                ),
                "policy_support_mean_absolute_prediction_difference": float(
                    np.mean(absolute @ policy_weights[side])
                ),
            }
        )
    return rows


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
        temporary = Path(handle.name)
    os.replace(temporary, path)


def _save_model(
    *,
    output_dir: Path,
    name: str,
    model: ConditionalTouchModel,
    summary: Mapping[str, Any],
) -> dict[str, Any]:
    target = output_dir / "models" / name
    target.mkdir(parents=True, exist_ok=False)
    booster_path = target / "model.txt"
    calibration_path = target / "positive_platt.json"
    summary_path = target / "fit_summary.json"
    model.booster.save_model(str(booster_path))
    _atomic_json(calibration_path, model.calibration)
    _atomic_json(summary_path, summary)
    return {
        "directory": str(target),
        "model": {"path": str(booster_path), "sha256": sha256_file(booster_path)},
        "calibration": {
            "path": str(calibration_path),
            "sha256": sha256_file(calibration_path),
        },
        "fit_summary": {
            "path": str(summary_path),
            "sha256": sha256_file(summary_path),
        },
    }


def _representative_curves(
    *,
    model: ConditionalTouchModel,
    contexts: Mapping[tuple[str, str], Mapping[str, np.ndarray]],
    native_days: Sequence[str],
    grid: np.ndarray,
    feature_contract: Mapping[str, Any],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for regime_index, regime_name in enumerate(("calm", "balanced", "shock")):
        selected: tuple[str, Mapping[str, np.ndarray], int] | None = None
        for day in reversed(native_days):
            context = contexts[("native", day)]
            codes = regime_code(
                context["volatility_ratio"],
                calm_upper=float(feature_contract["calm_upper"]),
                shock_lower=float(feature_contract["shock_lower"]),
            )
            indices = np.flatnonzero(codes == regime_index)
            if indices.size:
                ratio = np.asarray(context["volatility_ratio"])[indices]
                center = float(np.median(ratio))
                chosen = int(indices[np.argmin(np.abs(ratio - center))])
                selected = (day, context, chosen)
                break
        if selected is None:
            continue
        day, context, index = selected
        single = {
            field: np.asarray(context[field])[[index]] for field in CONTEXT_FIELDS
        }
        for side in SIDES:
            row_indices = np.zeros(len(grid), dtype=np.int64)
            probabilities = model.predict(
                single,
                side=side,
                distances=grid,
                row_indices=row_indices,
            )
            local_kappa = -np.gradient(
                np.log(np.clip(probabilities, 1e-12, 1.0)),
                grid,
            )
            rows.extend(
                {
                    "source_day": day,
                    "source_start_ts_ms": int(single["start_ts_ms"][0]),
                    "side": side,
                    "regime": regime_name,
                    "spread_usdc_per_btc": float(single["spread"][0]),
                    "fast_sigma": float(single["fast_sigma"][0]),
                    "slow_sigma": float(single["slow_sigma"][0]),
                    "distance_usdc_per_btc": float(distance),
                    "touch_probability": float(probability),
                    "local_kappa_per_usdc_per_btc": float(kappa),
                }
                for distance, probability, kappa in zip(
                    grid,
                    probabilities,
                    local_kappa,
                    strict=True,
                )
            )
    return pd.DataFrame(rows)


def run_audit(args: argparse.Namespace) -> dict[str, Any]:
    spec_path = args.spec.expanduser().resolve()
    spec, base_manifest = load_spec(spec_path)
    output_dir = args.output_dir.expanduser().resolve()
    cache_dir = args.cache_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    if any(output_dir.iterdir()):
        raise FileExistsError(f"conditional P3 output must be empty: {output_dir}")

    contexts, cache_usage, input_rows = _assemble_contexts(
        spec=spec,
        base_manifest=base_manifest,
        cache_dir=cache_dir,
        workers=int(args.workers),
    )
    overlap = spec["source_translation"]["overlap_days"]
    translation_pairs = {
        day: (
            contexts[("native", day)],
            contexts[("provider_overlap", day)],
        )
        for day in overlap["fit"]
    }
    translation = fit_source_translation(translation_pairs)
    for day in base_manifest["panels"]["fit_2025_provider"]:
        contexts[("provider_translated", day)] = apply_source_translation(
            contexts[("provider", day)], translation
        )
    for day in [*overlap["fit"], *overlap["historical_diagnostic"]]:
        contexts[("provider_translated", day)] = apply_source_translation(
            contexts[("provider_overlap", day)], translation
        )
    source_residuals = pd.DataFrame(
        _source_residual_rows(
            days=overlap["historical_diagnostic"],
            contexts=contexts,
            translation=translation,
        )
    )

    evaluation = spec["evaluation"]
    grid_contract = evaluation["distance_grid"]
    grid = np.arange(
        float(grid_contract["minimum"]),
        float(grid_contract["maximum"]) + 0.5 * float(grid_contract["step"]),
        float(grid_contract["step"]),
        dtype=np.float64,
    )
    policy_weights, policy_daily, policy_summary = _build_policy_support(
        spec=spec,
        grid=grid,
        workers=int(args.policy_workers),
    )
    policy_weight_frame = pd.DataFrame(
        {
            "distance_usdc_per_btc": grid,
            "BUY_weight": policy_weights["BUY"],
            "SELL_weight": policy_weights["SELL"],
        }
    )

    initial_native_days = list(base_manifest["panels"]["fit_2026_current"])
    fast_values = np.concatenate(
        [contexts[("native", day)]["fast_sigma"] for day in initial_native_days]
    )
    slow_values = np.concatenate(
        [contexts[("native", day)]["slow_sigma"] for day in initial_native_days]
    )
    volatility_cutpoints = {
        "fast_sigma": np.quantile(fast_values, [0.25, 0.50, 0.75]).tolist(),
        "slow_sigma": np.quantile(slow_values, [0.25, 0.50, 0.75]).tolist(),
        "fit_days": initial_native_days,
        "outcomes_used": False,
    }
    del fast_values, slow_values

    current_v2 = FillProbabilityModel.load(
        resolve_portable_path(
            str(spec["identities"]["current_v2_artifact"]["path"]),
            root=ROOT,
        )
    )
    provider_days = list(base_manifest["panels"]["fit_2025_provider"])
    feature_contract = spec["model"]["feature_contract"]
    calibration_tail_days = int(spec["model"]["nested_calibration"]["tail_days"])
    daily_rows: list[dict[str, Any]] = []
    calibration_rows: list[dict[str, Any]] = []
    source_prediction_rows: list[dict[str, Any]] = []
    fold_summaries: list[dict[str, Any]] = []
    fold_artifacts: dict[str, Any] = {}
    monotonicity = {"comparisons": 0, "violations": 0, "max_positive_difference": 0.0}

    chronological_past_days = list(initial_native_days)
    for fold in spec["chronological_oof"]["folds"]:
        fold_id = str(fold["fold_id"])
        past_native_days = list(chronological_past_days)
        if len(past_native_days) <= calibration_tail_days:
            raise ValueError(f"fold {fold_id} lacks a native calibration tail")
        core_days = past_native_days[:-calibration_tail_days]
        calibration_days = past_native_days[-calibration_tail_days:]
        model, fit_summary = _fit_conditional_model(
            provider_days=provider_days,
            core_native_days=core_days,
            calibration_native_days=calibration_days,
            contexts=contexts,
            spec=spec,
        )
        fold_artifacts[fold_id] = _save_model(
            output_dir=output_dir,
            name=fold_id,
            model=model,
            summary=fit_summary,
        )
        fold_summaries.append({"fold_id": fold_id, **fit_summary})
        for day in fold["test_days"]:
            day_daily, day_calibration, day_monotonicity = _score_day(
                day=day,
                panel=str(fold["panel"]),
                fold_id=fold_id,
                model=model,
                current_v2=current_v2,
                context=contexts[("native", day)],
                grid=grid,
                policy_weights=policy_weights,
                volatility_cutpoints=volatility_cutpoints,
                feature_contract=feature_contract,
            )
            daily_rows.extend(day_daily)
            calibration_rows.extend(day_calibration)
            monotonicity["comparisons"] += day_monotonicity["comparisons"]
            monotonicity["violations"] += day_monotonicity["violations"]
            monotonicity["max_positive_difference"] = max(
                float(monotonicity["max_positive_difference"]),
                float(day_monotonicity["max_positive_difference"]),
            )
            if day in overlap["historical_diagnostic"]:
                source_prediction_rows.extend(
                    _source_prediction_diagnostic(
                        day=day,
                        fold_id=fold_id,
                        model=model,
                        native=contexts[("native", day)],
                        provider=contexts[("provider_translated", day)],
                        grid=grid,
                        policy_weights=policy_weights,
                    )
                )
        del model
        gc.collect()
        chronological_past_days.extend(fold["test_days"])

    all_read_native_days = [
        *initial_native_days,
        *base_manifest["panels"]["historical_2026_validation"],
        *base_manifest["panels"]["historical_2026_test_diagnostic"],
    ]
    final_core_days = all_read_native_days[:-calibration_tail_days]
    final_calibration_days = all_read_native_days[-calibration_tail_days:]
    final_model, final_fit_summary = _fit_conditional_model(
        provider_days=provider_days,
        core_native_days=final_core_days,
        calibration_native_days=final_calibration_days,
        contexts=contexts,
        spec=spec,
    )
    final_artifact = _save_model(
        output_dir=output_dir,
        name="development_fit_through_2026-07-11",
        model=final_model,
        summary=final_fit_summary,
    )
    representative_curves = _representative_curves(
        model=final_model,
        contexts=contexts,
        native_days=all_read_native_days,
        grid=grid,
        feature_contract=feature_contract,
    )

    daily = pd.DataFrame(daily_rows).sort_values(["panel", "day", "side", "model"])
    calibration_daily = pd.DataFrame(calibration_rows)
    calibration_aggregate = _aggregate_calibration(calibration_daily)
    source_prediction = pd.DataFrame(source_prediction_rows).sort_values(["day", "side"])
    proper_score = _proper_score_summary(
        daily,
        draws=int(evaluation["bootstrap"]["draws"]),
        seed=int(evaluation["bootstrap"]["seed"]),
    )
    calibration_gate = _calibration_gate_summary(
        calibration_aggregate,
        minimum_windows=int(evaluation["calibration_gate"]["minimum_windows"]),
        max_iace_worsening=float(
            evaluation["calibration_gate"]["max_iace_worsening"]
        ),
    )

    score_gate_cells: list[dict[str, Any]] = []
    for panel, panel_scores in proper_score.items():
        for metric, metric_scores in panel_scores.items():
            pooled = metric_scores["POOLED"]
            pooled_pass = pooled["ci95_day_cluster_bootstrap"][1] < 0.0
            side_pass = all(metric_scores[side]["mean_delta"] <= 0.0 for side in SIDES)
            score_gate_cells.append(
                {
                    "panel": panel,
                    "metric": metric,
                    "pooled_ci_upper_lt_zero": bool(pooled_pass),
                    "both_side_mean_deltas_lte_zero": bool(side_pass),
                    "passed": bool(pooled_pass and side_pass),
                }
            )
    score_gate_passed = bool(score_gate_cells and all(row["passed"] for row in score_gate_cells))
    context_expected = 8_640 - int(spec["window_context"]["slow_window_s"] / 10)
    cache_usage["coverage_fraction"] = cache_usage["windows"] / context_expected
    context_coverage_passed = bool(
        cache_usage["coverage_fraction"].min()
        >= float(evaluation["context_coverage_gate"]["minimum_fraction"])
    )
    source_limit = float(
        evaluation["source_transport_gate"][
            "max_mean_absolute_prediction_difference"
        ]
    )
    source_transport_passed = bool(
        len(source_prediction)
        and source_prediction[
            [
                "uniform_mean_absolute_prediction_difference",
                "policy_support_mean_absolute_prediction_difference",
            ]
        ].to_numpy().max()
        <= source_limit
    )
    monotonicity_passed = bool(monotonicity["violations"] == 0)
    historical_prediction_gate_passed = bool(
        score_gate_passed
        and calibration_gate["all_supported_cells_passed"]
        and context_coverage_passed
        and source_transport_passed
        and monotonicity_passed
    )

    cache_usage.sort_values(["source", "day"]).to_csv(
        output_dir / "cache_usage.csv", index=False
    )
    policy_daily.sort_values(["day", "side"]).to_csv(
        output_dir / "policy_support_daily.csv", index=False
    )
    policy_weight_frame.to_csv(output_dir / "policy_support_weights.csv", index=False)
    source_residuals.to_csv(output_dir / "source_translation_residuals.csv", index=False)
    source_prediction.to_csv(output_dir / "source_prediction_transport.csv", index=False)
    daily.to_csv(output_dir / "daily_proper_scores.csv", index=False)
    calibration_aggregate.to_parquet(
        output_dir / "calibration_by_slice.parquet", index=False
    )
    representative_curves.to_parquet(
        output_dir / "representative_state_curves.parquet", index=False
    )
    _atomic_json(output_dir / "source_translation.json", translation)
    _atomic_json(output_dir / "volatility_cutpoints.json", volatility_cutpoints)

    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "identity": str(spec["identity"]),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "spec": {"path": str(spec_path), "sha256": sha256_file(spec_path)},
        "feature_graph": {
            "graph_id": P3_TOUCH_CONDITIONAL_GRAPH.graph_id,
            "sha256": P3_TOUCH_CONDITIONAL_GRAPH.sha256(),
        },
        "estimand": spec["estimand"],
        "input_rows": len(input_rows),
        "context_cache": {
            "rows": int(len(cache_usage)),
            "hits": int(cache_usage["cache_hit"].sum()),
            "misses": int((~cache_usage["cache_hit"]).sum()),
            "minimum_coverage_fraction": float(cache_usage["coverage_fraction"].min()),
        },
        "source_translation": translation,
        "source_translation_historical_residuals": source_residuals.to_dict(
            orient="records"
        ),
        "policy_support": policy_summary,
        "volatility_cutpoints": volatility_cutpoints,
        "chronological_oof_fit_summaries": fold_summaries,
        "fold_artifacts": fold_artifacts,
        "final_development_artifact": final_artifact,
        "proper_score": proper_score,
        "calibration_gate": calibration_gate,
        "source_prediction_transport": source_prediction.to_dict(orient="records"),
        "monotonicity_contract": monotonicity,
        "gates": {
            "proper_score_cells": score_gate_cells,
            "proper_score_passed": score_gate_passed,
            "calibration_passed": bool(
                calibration_gate["all_supported_cells_passed"]
            ),
            "context_coverage_passed": context_coverage_passed,
            "source_transport_passed": source_transport_passed,
            "monotonicity_contract_valid": monotonicity_passed,
            "historical_prediction_gate_passed": historical_prediction_gate_passed,
        },
        "decision": (
            "historical_prediction_evidence_supported_no_quote_authority"
            if historical_prediction_gate_passed
            else "conditional_v4_prediction_gate_failed_development"
        ),
        "quote_mapping": {
            "created": False,
            "scalar_kappa_exported": False,
            "conditional_curve_to_quote_identity_registered": False,
        },
        "permissions": spec["permissions"],
    }
    _atomic_json(output_dir / "report.json", report)

    files: dict[str, Any] = {}
    for path in sorted(output_dir.rglob("*")):
        if path.is_file() and path.name != "manifest.json":
            relative = str(path.relative_to(output_dir))
            files[relative] = {
                "sha256": sha256_file(path),
                "size_bytes": int(path.stat().st_size),
            }
    manifest = {
        "schema_version": "narrowgate_p3_touch_volatility_conditioned_output.v4",
        "identity": str(spec["identity"]),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "spec_sha256": sha256_file(spec_path),
        "implementation_sha256": sha256_file(Path(__file__)),
        "window_context_implementation_sha256": sha256_file(
            resolve_research_path(
                str(spec["identities"]["window_context_implementation"]["path"])
            )
        ),
        "feature_graph_sha256": P3_TOUCH_CONDITIONAL_GRAPH.sha256(),
        "files": files,
        "permissions": spec["permissions"],
    }
    _atomic_json(output_dir / "manifest.json", manifest)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--cache-dir", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--policy-workers", type=int, default=4)
    args = parser.parse_args()
    report = run_audit(args)
    print(
        json.dumps(
            {
                "identity": report["identity"],
                "decision": report["decision"],
                "gates": report["gates"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
