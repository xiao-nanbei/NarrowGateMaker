#!/usr/bin/env python3
"""Freeze outcome-blind inputs for the Boolean cooldown-duration study."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import uuid
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from data_paths import data_root, resolve_portable_path
from research.families.f05_fill_quality_quote_ev.audit.multiscale_ema_boolean_cooldown_duration import (
    EMA_HALF_LIVES_S,
    IDENTITY,
    atomic_predicate_dictionary,
    provider_pair_distance_scales,
)

ROOT = Path(__file__).resolve().parents[4]
DATA_ROOT = data_root(ROOT)
BASELINE_LOCATOR = (
    "${NARROWGATE_PRIVATE_RESEARCH_ROOT}/"
    "current_live_held_ber_replay_baseline_40d_20260809.json"
)
PROVIDER_ENCODER = DATA_ROOT / (
    "reports/"
    "multiscale_ema_add_wait_incremental_value_source_aware_v1_2_20260809/"
    "ema_encoder_2025_provider_source_grid.npz"
)
PROVIDER_ENCODER_MANIFEST = PROVIDER_ENCODER.with_name(
    "ema_encoder_2025_provider_source_grid_manifest.json"
)
OUTPUT = ROOT / (
    "research/families/f05_fill_quality_quote_ev/docs/"
    "multiscale_ema_boolean_cooldown_duration_policy_v1_"
    "outcome_blind_inputs_20260809.json"
)
FROZEN_CONFIG_SHA256 = "62a6add8d46c2695205e278ecb41bcaa16dc8199e683ef9114c21f6118b04e18"


class FreezeError(RuntimeError):
    """Fail closed when an outcome-blind source identity drifts."""


def _baseline_path() -> Path:
    try:
        return resolve_portable_path(BASELINE_LOCATOR, root=ROOT)
    except (RuntimeError, ValueError) as exc:
        raise FreezeError(
            "40-day baseline requires NARROWGATE_PRIVATE_RESEARCH_ROOT"
        ) from exc


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _require_frozen_config(path: Path, *, expected_sha256: str) -> Path:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise FreezeError(f"frozen operational config is missing: {resolved}")
    observed = _sha256(resolved)
    if observed != expected_sha256:
        raise FreezeError(
            "frozen operational config identity drifted: "
            f"expected={expected_sha256} actual={observed}"
        )
    return resolved


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


def _weighted_quantile(
    values: np.ndarray,
    weights: np.ndarray,
    quantiles: Sequence[float],
) -> np.ndarray:
    data = np.asarray(values, dtype=np.float64)
    mass = np.asarray(weights, dtype=np.float64)
    probs = np.asarray(quantiles, dtype=np.float64)
    if (
        data.ndim != 1
        or mass.shape != data.shape
        or len(data) == 0
        or not np.isfinite(data).all()
        or not np.isfinite(mass).all()
        or np.any(mass <= 0.0)
        or np.any((probs < 0.0) | (probs > 1.0))
    ):
        raise FreezeError("weighted quantile input is invalid")
    order = np.argsort(data, kind="stable")
    ordered = data[order]
    ordered_mass = mass[order]
    centers = np.cumsum(ordered_mass) - 0.5 * ordered_mass
    centers /= ordered_mass.sum()
    return np.interp(probs, centers, ordered)


def _duration_rows(
    fills: pd.DataFrame,
    *,
    unit_qty_btc: float,
    cooldown_s: float,
) -> pd.DataFrame:
    required = {
        "day",
        "order_id",
        "side",
        "fill_ts",
        "fill_qty",
        "inventory_before_fill",
        "inventory_after_fill",
    }
    if not required.issubset(fills):
        raise FreezeError("baseline fill schema is incomplete")
    frame = fills.loc[
        fills["fill_ts"].gt(0) & fills["fill_qty"].gt(0),
        sorted(required),
    ].copy()
    frame = frame.sort_values(["day", "fill_ts", "order_id", "side"], kind="stable").reset_index(
        drop=True
    )
    rows: list[dict[str, Any]] = []
    for day, day_frame in frame.groupby("day", sort=True, observed=True):
        day_frame = day_frame.reset_index(drop=True)
        day_end_ts_ms = int(
            (pd.Timestamp(str(day), tz="UTC") + pd.Timedelta(days=1)).timestamp() * 1_000
        )
        consecutive = {"BUY": 0.0, "SELL": 0.0}
        records = day_frame.to_dict("records")
        for index, row in enumerate(records):
            side = str(row["side"]).upper()
            opposite = "SELL" if side == "BUY" else "BUY"
            consecutive[side] += float(row["fill_qty"]) / float(unit_qty_btc)
            consecutive[opposite] = 0.0
            before = float(row["inventory_before_fill"])
            exposure = (side == "BUY" and before >= 0.0) or (side == "SELL" and before <= 0.0)
            if not exposure:
                continue
            fill_ts_ms = int(row["fill_ts"])
            if fill_ts_ms >= day_end_ts_ms:
                raise FreezeError("fill clock escaped its UTC day")
            next_state_change_s = float(day_end_ts_ms - fill_ts_ms) / 1_000.0
            next_state_change_observed = False
            if index + 1 < len(records):
                next_state_change_s = max(
                    0.0,
                    float(int(records[index + 1]["fill_ts"]) - int(row["fill_ts"])) / 1_000.0,
                )
                next_state_change_observed = True
            undo_add_s = float(day_end_ts_ms - fill_ts_ms) / 1_000.0
            undo_add_observed = False
            for future in records[index + 1 :]:
                future_inventory = float(future["inventory_after_fill"])
                if abs(future_inventory) <= abs(before) + 1e-10:
                    undo_add_s = max(
                        0.0,
                        float(int(future["fill_ts"]) - int(row["fill_ts"])) / 1_000.0,
                    )
                    undo_add_observed = True
                    break
            rows.append(
                {
                    "day": str(day),
                    "side": side,
                    "role_at_fill": "opener" if abs(before) < 1e-10 else "add",
                    "fill_ts_ms": int(row["fill_ts"]),
                    "control_85n_s": cooldown_s * max(1.0, consecutive[side]),
                    "next_inventory_state_change_s": next_state_change_s,
                    "next_inventory_state_change_observed": (next_state_change_observed),
                    "undo_incremental_inventory_s": undo_add_s,
                    "undo_incremental_inventory_observed": undo_add_observed,
                }
            )
    return pd.DataFrame(rows)


def _weighted_km_quantiles(
    *,
    durations: np.ndarray,
    observed: np.ndarray,
    weights: np.ndarray,
    quantiles: Sequence[float],
) -> np.ndarray:
    time = np.asarray(durations, dtype=np.float64)
    event = np.asarray(observed, dtype=np.bool_)
    mass = np.asarray(weights, dtype=np.float64)
    probs = np.asarray(quantiles, dtype=np.float64)
    if (
        time.ndim != 1
        or event.shape != time.shape
        or mass.shape != time.shape
        or len(time) == 0
        or not np.isfinite(time).all()
        or np.any(time < 0.0)
        or not np.isfinite(mass).all()
        or np.any(mass <= 0.0)
    ):
        raise FreezeError("weighted KM input is invalid")
    order = np.argsort(time, kind="stable")
    time = time[order]
    event = event[order]
    mass = mass[order]
    risk = float(mass.sum())
    survival = 1.0
    output = np.full(len(probs), np.nan, dtype=np.float64)
    for value in np.unique(time):
        at_time = time == value
        event_mass = float(mass[at_time & event].sum())
        removed_mass = float(mass[at_time].sum())
        if event_mass > 0.0:
            if risk <= 0.0 or event_mass > risk + 1e-12:
                raise FreezeError("weighted KM risk set is invalid")
            survival *= max(0.0, 1.0 - event_mass / risk)
            cdf = 1.0 - survival
            output[np.isnan(output) & (cdf >= probs)] = float(value)
        risk = max(0.0, risk - removed_mass)
    if np.isnan(output).any():
        missing = [
            float(probability)
            for probability, value in zip(probs, output, strict=True)
            if not math.isfinite(float(value))
        ]
        raise FreezeError(f"KM quantiles are not identified: {missing}")
    return output


def _clock_quantiles(frame: pd.DataFrame, *, side: str, column: str) -> dict[str, Any]:
    rows = frame.loc[frame["side"].eq(side)].copy()
    if rows.empty:
        raise FreezeError(f"duration source lacks {side} {column} support")
    day_counts = rows.groupby("day", observed=True)[column].transform("size")
    weights = 1.0 / day_counts.to_numpy(dtype=np.float64)
    probabilities = (0.25, 0.50, 0.75, 0.90)
    event_column = f"{column.removesuffix('_s')}_observed"
    if event_column in rows:
        values = _weighted_km_quantiles(
            durations=rows[column].to_numpy(dtype=np.float64),
            observed=rows[event_column].to_numpy(dtype=np.bool_),
            weights=weights,
            quantiles=probabilities,
        )
        event_count = int(rows[event_column].sum())
        censor_count = int((~rows[event_column].astype(bool)).sum())
        estimator = "day_equal_weighted_Kaplan_Meier"
    else:
        values = _weighted_quantile(
            rows[column].to_numpy(dtype=np.float64),
            weights,
            probabilities,
        )
        event_count = int(len(rows))
        censor_count = 0
        estimator = "day_equal_weighted_empirical_quantile"
    return {
        "rows": int(len(rows)),
        "observed_events": event_count,
        "right_censored": censor_count,
        "distinct_utc_days": int(rows["day"].nunique()),
        "estimator": estimator,
        "day_equal_weighted_quantiles_s": {
            f"p{int(probability * 100):02d}": int(round(float(value)))
            for probability, value in zip(probabilities, values, strict=True)
        },
    }


def freeze(*, config: Path, output: Path = OUTPUT) -> dict[str, Any]:
    config = _require_frozen_config(
        config,
        expected_sha256=FROZEN_CONFIG_SHA256,
    )
    baseline_path = _baseline_path()
    baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
    if baseline.get("baseline_id") != (
        "btc_usdc_current_live_held_ber_replay_baseline_40d_20260809"
    ):
        raise FreezeError("authoritative replay baseline identity drifted")
    fills_path = Path(str(baseline["source"]["fills_path"]))
    if _sha256(fills_path) != baseline["source"]["fills_sha256"]:
        raise FreezeError("authoritative fill path hash drifted")
    raw_config = yaml.safe_load(config.read_text(encoding="utf-8"))
    if not isinstance(raw_config, dict) or not isinstance(raw_config.get("strategy"), dict):
        raise FreezeError("operational config schema drifted")
    strategy = raw_config["strategy"]
    order_size = float(strategy["order_size"])
    lot_size = float(raw_config["lot_size"])
    cooldown_s = float(strategy["fill_cooldown"])
    cooldown_semantics = {
        "config_path": str(config),
        "config_sha256": _sha256(config),
        "order_size_btc": order_size,
        "lot_size_btc": lot_size,
        "quantity_unit_btc": max(order_size, lot_size),
        "fill_cooldown_s": cooldown_s,
        "clock_mode": str(strategy.get("fill_cooldown_clock_mode", "wall_time")),
        "consecutive_reset_policy": str(
            strategy.get(
                "fill_cooldown_consecutive_reset_policy",
                "expiry_or_opposite_fill",
            )
        ),
        "adaptive_add_cooldown_enabled": bool(strategy.get("adaptive_add_cooldown_enabled", False)),
        "reducing_cooldown_s": float(strategy.get("fill_cooldown_reducing", 0.0)),
        "buy_fill_selection_live_enabled": bool(
            strategy.get("buy_fill_selection_live_enabled", False)
        ),
        "q90_action_enabled": bool(strategy.get("dynamic_fill_hazard_action_enabled", False)),
    }
    expected_semantics = {
        "clock_mode": "wall_time",
        "consecutive_reset_policy": "opposite_fill_only",
        "adaptive_add_cooldown_enabled": False,
        "reducing_cooldown_s": 0.0,
        "buy_fill_selection_live_enabled": False,
        "q90_action_enabled": False,
    }
    for field, expected in expected_semantics.items():
        if cooldown_semantics[field] != expected:
            raise FreezeError(f"operational cooldown semantics drifted: {field}")
    if not math.isclose(cooldown_s, 85.0, rel_tol=0.0, abs_tol=1e-12):
        raise FreezeError("current control is no longer the 85n baseline")
    columns = [
        "day",
        "order_id",
        "side",
        "fill_ts",
        "fill_qty",
        "inventory_before_fill",
        "inventory_after_fill",
    ]
    durations = _duration_rows(
        pd.read_parquet(fills_path, columns=columns),
        unit_qty_btc=max(order_size, lot_size),
        cooldown_s=cooldown_s,
    )
    clock_columns = (
        "next_inventory_state_change_s",
        "undo_incremental_inventory_s",
    )
    duration_sources: dict[str, Any] = {}
    candidate_actions: dict[str, list[dict[str, Any]]] = {}
    for side in ("BUY", "SELL"):
        source_rows = {
            column: _clock_quantiles(durations, side=side, column=column)
            for column in clock_columns
        }
        source_rows["current_control_85n_s"] = _clock_quantiles(
            durations,
            side=side,
            column="control_85n_s",
        )
        duration_sources[side] = source_rows
        fixed_values = sorted(
            {
                int(value)
                for column in clock_columns
                for value in source_rows[column]["day_equal_weighted_quantiles_s"].values()
            }
        )
        candidate_actions[side] = [
            {
                "policy_id": "CONTROL_85N",
                "duration_semantics": ("current 85 seconds times same-side filled-quantity units"),
                "duration_s": None,
            },
            *(
                {
                    "policy_id": f"FIXED_{duration_s}S",
                    "duration_semantics": "fixed total duration from target fill",
                    "duration_s": duration_s,
                }
                for duration_s in fixed_values
            ),
        ]

    encoder_manifest = json.loads(PROVIDER_ENCODER_MANIFEST.read_text(encoding="utf-8"))
    if (
        _sha256(PROVIDER_ENCODER) != encoder_manifest.get("artifact_sha256")
        or encoder_manifest.get("training_day_count") != 66
        or encoder_manifest.get("sampling_stride") != "none_all_admitted_source_rows"
    ):
        raise FreezeError("2025 provider source-grid encoder identity drifted")
    with np.load(PROVIDER_ENCODER, allow_pickle=False) as values:
        pair_scales = provider_pair_distance_scales(
            feature_names=tuple(str(value) for value in values["feature_names"]),
            scale=np.array(values["scale"], copy=True),
            components=np.array(values["components"], copy=True),
            eigenvalues=np.array(values["eigenvalues"], copy=True),
        )
    predicates = atomic_predicate_dictionary(
        pair_distance_scale_bps=pair_scales,
    )
    payload = {
        "schema_version": f"{IDENTITY}.outcome_blind_inputs.v1",
        "identity": IDENTITY,
        "last_materially_modified": "2026-08-09",
        "research_class": "exploratory",
        "baseline_projection": {
            "baseline_id": baseline["baseline_id"],
            "baseline_identity_sha256": _sha256(baseline_path),
            "operational_identity_path": baseline["operational_baseline"]["identity_path"],
            "operational_identity_sha256": baseline["operational_baseline"]["identity_sha256"],
            "ordered_utc_days": baseline["panel"]["ordered_utc_days"],
            "daily_fresh_start": True,
            "economic_fields_present_in_source_identity_but_unused": True,
        },
        "duration_source": {
            "fills_path": str(fills_path),
            "fills_sha256": _sha256(fills_path),
            "columns_read": columns,
            "economic_outcomes_read": False,
            "exposure_increasing_fill_rows": int(len(durations)),
            "cooldown_semantics": cooldown_semantics,
            "clock_quantiles": duration_sources,
            "candidate_actions": candidate_actions,
            "rounding": "nearest whole second for action serialization only",
            "weighting": "each UTC day has equal total mass within side and clock",
            "right_censoring": (
                "at the natural UTC day end of the frozen daily-fresh-start "
                "Development path; candidate quantiles use weighted Kaplan-Meier"
            ),
        },
        "ema_source": {
            "price": "canonical decision-visible local mid",
            "half_lives_s": list(EMA_HALF_LIVES_S),
            "all_pair_count": len(pair_scales),
            "provider_encoder_path": str(PROVIDER_ENCODER),
            "provider_encoder_sha256": _sha256(PROVIDER_ENCODER),
            "provider_encoder_manifest_path": str(PROVIDER_ENCODER_MANIFEST),
            "provider_encoder_manifest_sha256": _sha256(PROVIDER_ENCODER_MANIFEST),
            "provider_training_days": 66,
            "provider_training_rows": int(encoder_manifest["training_rows_both_sides"]),
            "provider_sampling_stride": "none_all_admitted_source_rows",
            "provider_economic_outcomes_read": False,
            "pair_distance_scale_bps": pair_scales,
        },
        "atomic_predicates": [
            {
                "name": predicate.name,
                "feature": predicate.feature,
                "operator": predicate.operator,
                "threshold": predicate.threshold,
                "provenance": predicate.provenance,
            }
            for predicate in predicates
        ],
        "permissions": {
            "development_economic_labels_read": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    _atomic_json(output, payload)
    return payload


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Exact frozen config bytes; the mutable current-live alias is not a default.",
    )
    parser.add_argument("--output", type=Path, default=OUTPUT)
    return parser.parse_args(argv)


def main() -> None:
    args = _parse_args()
    payload = freeze(config=args.config, output=args.output)
    print(
        json.dumps(
            {
                "output": str(args.output),
                "sha256": _sha256(args.output),
                "candidate_actions": payload["duration_source"]["candidate_actions"],
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
