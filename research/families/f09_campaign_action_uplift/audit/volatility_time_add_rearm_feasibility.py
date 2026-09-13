#!/usr/bin/env python3
"""Development-only feasibility audit for variance-time add rearm.

This module does not read reward, PnL, markout, campaign terminal, or any
action outcome. It asks only whether a causal realized-variance clock produces
supported, two-sided timing variation relative to the current 85-second
same-side fill-unit clock without degenerating into a wall-time cap.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from models.audit.experiment_manifest import git_workspace_identity
from strategy.fill_cooldown import (
    CausalVarianceSample,
    integrate_variance_time_episode,
    price_variance_to_bps2_rate,
    update_same_side_fill_units,
)

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = "volatility_time_add_rearm_feasibility.v1"
SIDES = ("BUY", "SELL")
FORBIDDEN_OUTCOME_TOKENS = (
    "pnl",
    "reward",
    "markout",
    "ev_",
    "toxic",
    "mae",
    "terminal",
)
REQUIRED_FILL_COLUMNS = (
    "day",
    "arm",
    "side",
    "fill_ts",
    "fill_qty",
    "inventory_before_fill",
    "inventory_after_fill",
    "order_id",
)
OPTIONAL_MECHANISM_COLUMNS = (
    "microprice_shift_bps",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
    "queue_before",
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(payload: Any) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(raw).hexdigest()


def _utc_day_bounds(day: str) -> tuple[int, int]:
    start = int(pd.Timestamp(day, tz="UTC").timestamp() * 1000)
    return start, start + 86_400_000


def _bar_path(root: Path, day: str) -> Path:
    return root / f"BTCUSDC-1s-{day}.parquet"


def _previous_day(day: str) -> str:
    return (pd.Timestamp(day) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")


def load_causal_variance_samples(
    bars_root: Path,
    day: str,
    *,
    rolling_window_s: int,
    max_close_carry_ms: int,
    max_abs_return_bps_1s: float,
) -> pd.DataFrame:
    """Build completed-bucket 1s raw price variance without future filling."""

    start_ms, end_ms = _utc_day_bounds(day)
    frames: list[pd.DataFrame] = []
    paths = [_bar_path(bars_root, _previous_day(day)), _bar_path(bars_root, day)]
    for path in paths:
        if not path.is_file():
            continue
        frame = pd.read_parquet(path, columns=["close"])
        ts = pd.to_numeric(frame.index, errors="coerce").to_numpy(dtype=np.float64)
        close = pd.to_numeric(frame["close"], errors="coerce").to_numpy(dtype=float)
        valid = np.isfinite(ts) & np.isfinite(close) & (close > 0.0)
        if valid.any():
            frames.append(
                pd.DataFrame(
                    {
                        "bar_ts_ms": ts[valid].astype(np.int64),
                        "close": close[valid],
                    }
                )
            )
    if not frames:
        raise FileNotFoundError(f"no 1s bar source for {day}")

    raw = (
        pd.concat(frames, ignore_index=True)
        .sort_values("bar_ts_ms", kind="stable")
        .drop_duplicates("bar_ts_ms", keep="last")
    )
    raw = raw[
        (raw["bar_ts_ms"] < end_ms)
        & (raw["bar_ts_ms"] >= start_ms - max(120_000, rolling_window_s * 2_000))
    ]
    if raw.empty:
        raise ValueError(f"1s bars do not overlap {day}")

    grid = np.arange(
        start_ms - max(120_000, rolling_window_s * 2_000),
        end_ms,
        1_000,
        dtype=np.int64,
    )
    raw_ts = raw["bar_ts_ms"].to_numpy(dtype=np.int64)
    raw_close = raw["close"].to_numpy(dtype=float)
    source_index = np.searchsorted(raw_ts, grid, side="right") - 1
    has_source = source_index >= 0
    close = np.full(grid.size, np.nan, dtype=float)
    source_ts = np.full(grid.size, np.iinfo(np.int64).min, dtype=np.int64)
    close[has_source] = raw_close[source_index[has_source]]
    source_ts[has_source] = raw_ts[source_index[has_source]]
    source_age = np.where(has_source, grid - source_ts, np.iinfo(np.int64).max)

    prior = np.roll(close, 1)
    prior[0] = np.nan
    delta = close - prior
    return_bps = np.abs(delta / prior) * 10_000.0
    bucket_valid = (
        has_source
        & np.isfinite(close)
        & np.isfinite(delta)
        & (source_age >= 0)
        & (source_age <= int(max_close_carry_ms))
        & np.isfinite(return_bps)
        & (return_bps <= float(max_abs_return_bps_1s))
    )
    delta_series = pd.Series(delta)
    invalid_series = pd.Series((~bucket_valid).astype(np.int16))
    sigma_sq = (
        delta_series.rolling(rolling_window_s, min_periods=rolling_window_s)
        .var(ddof=0)
        .to_numpy(dtype=float)
    )
    invalid_count = (
        invalid_series.rolling(rolling_window_s, min_periods=rolling_window_s)
        .sum()
        .to_numpy(dtype=float)
    )
    sample_valid = (
        np.isfinite(sigma_sq)
        & np.isfinite(invalid_count)
        & (invalid_count == 0.0)
        & np.isfinite(close)
        & (close > 0.0)
    )
    output = pd.DataFrame(
        {
            "feature_ready_ts_ms": grid + 1_000,
            "price": close,
            "sigma_sq_price_per_s": sigma_sq,
            "valid": sample_valid,
            "source_age_ms": source_age,
        }
    )
    return output[
        (output["feature_ready_ts_ms"] >= start_ms)
        & (output["feature_ready_ts_ms"] <= end_ms)
    ].reset_index(drop=True)


def load_fill_events(path: Path, development_days: Iterable[str]) -> pd.DataFrame:
    header = pd.read_csv(path, nrows=0)
    missing = sorted(set(REQUIRED_FILL_COLUMNS) - set(header.columns))
    if missing:
        raise ValueError(f"baseline fill trace is missing columns: {missing}")
    requested = list(REQUIRED_FILL_COLUMNS) + [
        column for column in OPTIONAL_MECHANISM_COLUMNS if column in header.columns
    ]
    if any(
        any(token in column.lower() for token in FORBIDDEN_OUTCOME_TOKENS)
        for column in requested
    ):
        raise AssertionError("feasibility loader requested an outcome column")
    frame = pd.read_csv(path, usecols=requested)
    for column in OPTIONAL_MECHANISM_COLUMNS:
        if column not in frame.columns:
            frame[column] = np.nan
    frame = frame[frame["arm"].astype(str).eq("baseline")].copy()
    allowed_days = set(str(day) for day in development_days)
    unexpected = sorted(set(frame["day"].astype(str)) - allowed_days)
    if unexpected:
        raise ValueError(f"fill trace includes non-Development days: {unexpected}")
    frame["side"] = frame["side"].astype(str).str.upper()
    if not set(frame["side"]).issubset(set(SIDES)):
        raise ValueError("fill trace has invalid sides")
    numeric = (
        "fill_ts",
        "fill_qty",
        "inventory_before_fill",
        "inventory_after_fill",
        "order_id",
        "microprice_shift_bps",
        "l2_book_refresh_ratio",
        "l2_book_cancel_ratio",
        "queue_before",
    )
    for column in numeric:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    required_finite = frame[
        ["fill_ts", "fill_qty", "inventory_before_fill", "inventory_after_fill"]
    ].to_numpy(dtype=float)
    if not np.isfinite(required_finite).all() or (frame["fill_qty"] <= 0.0).any():
        raise ValueError("fill trace has invalid lifecycle quantities")
    frame["order_id"] = frame["order_id"].fillna(-1.0)
    return frame.sort_values(["day", "fill_ts", "order_id"], kind="stable").reset_index(drop=True)


def build_fill_unit_episodes(
    events: pd.DataFrame,
    *,
    order_size_btc: float,
    lot_size_btc: float,
) -> pd.DataFrame:
    """Reconstruct exact live same-side fill-unit and reset semantics."""

    rows: list[dict[str, Any]] = []
    episode_index = 0
    for day, daily in events.groupby("day", sort=True):
        _, day_end_ms = _utc_day_bounds(str(day))
        buy_units = 0.0
        sell_units = 0.0
        campaign_index = 0
        current_campaign_id = ""
        active: dict[str, int | None] = {"BUY": None, "SELL": None}
        for event in daily.to_dict("records"):
            side = str(event["side"]).upper()
            opposite = "SELL" if side == "BUY" else "BUY"
            ts_ms = int(round(float(event["fill_ts"])))
            qty = float(event["fill_qty"])
            before = float(event["inventory_before_fill"])
            after = float(event["inventory_after_fill"])
            expected_after = before + qty if side == "BUY" else before - qty
            if not math.isclose(expected_after, after, abs_tol=1e-9):
                raise ValueError(f"inventory path mismatch on {day} at {ts_ms}")
            if math.isclose(before, 0.0, abs_tol=1e-12) and not math.isclose(
                after, 0.0, abs_tol=1e-12
            ):
                current_campaign_id = f"{day}:campaign:{campaign_index}"
                campaign_index += 1

            opposite_active = active[opposite]
            if opposite_active is not None:
                rows[opposite_active]["censor_ts_ms"] = ts_ms
                rows[opposite_active]["censor_reason"] = "opposite_fill_reset"
                active[opposite] = None

            buy_units, sell_units, fill_units = update_same_side_fill_units(
                side=side,
                fill_qty=qty,
                order_size=order_size_btc,
                lot_size=lot_size_btc,
                buy_units=buy_units,
                sell_units=sell_units,
            )
            exposure_increasing = (
                (side == "BUY" and before >= 0.0)
                or (side == "SELL" and before <= 0.0)
            )
            if not exposure_increasing:
                if math.isclose(after, 0.0, abs_tol=1e-12):
                    current_campaign_id = ""
                continue

            same_active = active[side]
            if same_active is not None:
                rows[same_active]["censor_ts_ms"] = ts_ms
                rows[same_active]["censor_reason"] = "same_side_add_restart"
            units = buy_units if side == "BUY" else sell_units
            rows.append(
                {
                    "episode_id": f"{day}:{episode_index}",
                    "day": str(day),
                    "side": side,
                    "episode_start_ts_ms": ts_ms,
                    "fill_qty_btc": qty,
                    "fill_units": fill_units,
                    "consecutive_same_side_fill_units": units,
                    "inventory_before_fill": before,
                    "inventory_after_fill": after,
                    "order_id": int(round(float(event["order_id"]))),
                    "inventory_campaign_id": current_campaign_id,
                    "inventory_role_at_fill": (
                        "opener"
                        if math.isclose(before, 0.0, abs_tol=1e-12)
                        else "add"
                    ),
                    "microprice_shift_bps_at_fill": event.get("microprice_shift_bps", np.nan),
                    "refill_edge_at_fill": float(event.get("l2_book_refresh_ratio", np.nan))
                    - float(event.get("l2_book_cancel_ratio", np.nan)),
                    "queue_before_fill": event.get("queue_before", np.nan),
                    "censor_ts_ms": day_end_ms,
                    "censor_reason": "day_end",
                    "campaign_end_behavior": "preserve_state_no_extra_reset",
                }
            )
            active[side] = len(rows) - 1
            episode_index += 1
    return pd.DataFrame(rows)


def _samples_for_episode(
    variance: pd.DataFrame,
    *,
    start_ms: int,
    stop_ms: int,
) -> list[CausalVarianceSample]:
    ready = variance["feature_ready_ts_ms"].to_numpy(dtype=np.int64)
    lo = max(0, int(np.searchsorted(ready, start_ms, side="right")) - 1)
    hi = min(len(variance), int(np.searchsorted(ready, stop_ms, side="left")) + 1)
    subset = variance.iloc[lo:hi]
    return [
        CausalVarianceSample(
            feature_ready_ts_ms=int(row.feature_ready_ts_ms),
            mid_price=float(row.price),
            sigma_sq_price_per_s=float(row.sigma_sq_price_per_s),
            valid=bool(row.valid),
        )
        for row in subset.itertuples(index=False)
    ]


def attach_start_variance_rate(
    episodes: pd.DataFrame,
    bars_root: Path,
    *,
    rolling_window_s: int,
    max_close_carry_ms: int,
    max_abs_return_bps_1s: float,
    max_feature_age_ms: int,
) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for day, daily in episodes.groupby("day", sort=True):
        variance = load_causal_variance_samples(
            bars_root,
            str(day),
            rolling_window_s=rolling_window_s,
            max_close_carry_ms=max_close_carry_ms,
            max_abs_return_bps_1s=max_abs_return_bps_1s,
        )
        ready = variance["feature_ready_ts_ms"].to_numpy(dtype=np.int64)
        rows = daily.copy()
        rates: list[float] = []
        valid_flags: list[bool] = []
        ready_values: list[int] = []
        for start in rows["episode_start_ts_ms"].astype(np.int64):
            index = int(np.searchsorted(ready, start, side="right")) - 1
            valid = False
            rate = np.nan
            ready_ts = -1
            if index >= 0:
                sample = variance.iloc[index]
                ready_ts = int(sample["feature_ready_ts_ms"])
                valid = bool(sample["valid"]) and start - ready_ts <= max_feature_age_ms
                if valid:
                    rate = price_variance_to_bps2_rate(
                        float(sample["sigma_sq_price_per_s"]), float(sample["price"])
                    )
                    valid = math.isfinite(rate) and rate >= 0.0
            rates.append(float(rate))
            valid_flags.append(bool(valid))
            ready_values.append(ready_ts)
        rows["start_variance_rate_bps2_per_s"] = rates
        rows["start_variance_valid"] = valid_flags
        rows["start_feature_ready_ts_ms"] = ready_values
        parts.append(rows)
    return pd.concat(parts, ignore_index=True) if parts else episodes.copy()


def freeze_reference_rates(
    episodes: pd.DataFrame,
    reference_days: Iterable[str],
) -> dict[str, float]:
    reference = episodes[
        episodes["day"].astype(str).isin(set(reference_days))
        & episodes["start_variance_valid"].astype(bool)
    ]
    rates: dict[str, float] = {}
    for side in SIDES:
        values = pd.to_numeric(
            reference.loc[reference["side"].eq(side), "start_variance_rate_bps2_per_s"],
            errors="coerce",
        )
        values = values[np.isfinite(values) & (values > 0.0)]
        if values.empty:
            raise ValueError(f"no valid positive reference variance for {side}")
        rates[side] = float(values.median())
    return rates


def evaluate_episodes(
    episodes: pd.DataFrame,
    bars_root: Path,
    *,
    evaluation_days: Iterable[str],
    reference_rates: dict[str, float],
    base_cooldown_s: float,
    minimum_wall_time_ms: int,
    maximum_wall_time_ms: int,
    max_feature_age_ms: int,
    rolling_window_s: int,
    max_close_carry_ms: int,
    max_abs_return_bps_1s: float,
    cpp_module: Any | None = None,
) -> pd.DataFrame:
    selected = episodes[episodes["day"].astype(str).isin(set(evaluation_days))].copy()
    output: list[dict[str, Any]] = []
    for day, daily in selected.groupby("day", sort=True):
        variance = load_causal_variance_samples(
            bars_root,
            str(day),
            rolling_window_s=rolling_window_s,
            max_close_carry_ms=max_close_carry_ms,
            max_abs_return_bps_1s=max_abs_return_bps_1s,
        )
        for episode in daily.to_dict("records"):
            start = int(episode["episode_start_ts_ms"])
            censor = int(episode["censor_ts_ms"])
            units = max(1.0, float(episode["consecutive_same_side_fill_units"]))
            baseline_deadline = start + int(math.ceil(base_cooldown_s * units * 1000.0))
            budget = float(reference_rates[str(episode["side"])]) * base_cooldown_s * units
            stop = min(censor, start + maximum_wall_time_ms)
            samples = _samples_for_episode(variance, start_ms=start, stop_ms=stop)
            result = integrate_variance_time_episode(
                samples,
                episode_start_ts_ms=start,
                budget_bps2=budget,
                minimum_wall_time_ms=minimum_wall_time_ms,
                maximum_wall_time_ms=maximum_wall_time_ms,
                max_feature_age_ms=max_feature_age_ms,
                censor_ts_ms=censor,
            )
            candidate_end = int(result.rearm_ts_ms) if result.rearm_ts_ms is not None else censor
            baseline_end = min(baseline_deadline, censor)
            cpp_match = True
            if cpp_module is not None:
                native = cpp_module.integrate_variance_time_episode(
                    np.asarray([row.feature_ready_ts_ms for row in samples], dtype=np.int64),
                    np.asarray([row.mid_price for row in samples], dtype=np.float64),
                    np.asarray([row.sigma_sq_price_per_s for row in samples], dtype=np.float64),
                    np.asarray([row.valid for row in samples], dtype=np.uint8),
                    start,
                    budget,
                    minimum_wall_time_ms,
                    maximum_wall_time_ms,
                    max_feature_age_ms,
                    censor,
                )
                native_rearm = native["rearm_ts_ms"]
                cpp_match = bool(
                    native["reason"] == result.reason
                    and native_rearm == result.rearm_ts_ms
                    and math.isclose(
                        float(native["accumulated_qv_bps2"]),
                        result.accumulated_qv_bps2,
                        rel_tol=1e-10,
                        abs_tol=1e-10,
                    )
                    and math.isclose(
                        float(native["stale_frozen_ms"]),
                        result.stale_frozen_ms,
                        rel_tol=1e-10,
                        abs_tol=1e-10,
                    )
                )
            observed_ms = max(0.0, float(candidate_end - start))
            row = dict(episode)
            row.update(
                {
                    "reference_variance_rate_bps2_per_s": reference_rates[str(episode["side"])],
                    "variance_budget_bps2": budget,
                    "baseline_rearm_ts_ms": baseline_deadline,
                    "baseline_effective_end_ts_ms": baseline_end,
                    "candidate_rearm_ts_ms": result.rearm_ts_ms,
                    "candidate_effective_end_ts_ms": candidate_end,
                    "candidate_reason": result.reason,
                    "candidate_accumulated_qv_bps2": result.accumulated_qv_bps2,
                    "candidate_stale_frozen_ms": result.stale_frozen_ms,
                    "candidate_valid_interval_ms": result.valid_interval_ms,
                    "candidate_observed_ms": observed_ms,
                    "candidate_valid_time_rate": result.valid_interval_ms / max(observed_ms, 1.0),
                    "timing_delta_s": (candidate_end - baseline_end) / 1000.0,
                    "cpp_variance_clock_match": cpp_match,
                }
            )
            output.append(row)
    return pd.DataFrame(output)


def summarize_feasibility(
    evaluated: pd.DataFrame,
    *,
    material_delta_s: float,
    gates: dict[str, float],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    cells: list[dict[str, Any]] = []
    for side in SIDES:
        cell = evaluated[evaluated["side"].eq(side)].copy()
        delta = pd.to_numeric(cell["timing_delta_s"], errors="coerce")
        earlier = delta < -material_delta_s
        later = delta > material_delta_s
        equal = ~(earlier | later)
        start_valid = cell["start_variance_valid"].astype(bool)
        max_cap = cell["candidate_reason"].eq("maximum_wall_time")
        valid_time = float(cell["candidate_valid_interval_ms"].sum())
        observed_time = float(cell["candidate_observed_ms"].sum())
        bin_counts = {
            "n1_episodes": int((cell["consecutive_same_side_fill_units"] < 2.0).sum()),
            "n2_episodes": int(
                ((cell["consecutive_same_side_fill_units"] >= 2.0) & (cell["consecutive_same_side_fill_units"] < 3.0)).sum()
            ),
            "n3_episodes": int(
                ((cell["consecutive_same_side_fill_units"] >= 3.0) & (cell["consecutive_same_side_fill_units"] < 4.0)).sum()
            ),
            "n4plus_episodes": int((cell["consecutive_same_side_fill_units"] >= 4.0).sum()),
        }
        row = {
            "side": side,
            "episodes": int(len(cell)),
            "days": int(cell["day"].nunique()),
            "start_variance_valid_rate": float(start_valid.mean()) if len(cell) else 0.0,
            "earlier_material_rate": float(earlier.mean()) if len(cell) else 0.0,
            "later_material_rate": float(later.mean()) if len(cell) else 0.0,
            "equal_within_material_rate": float(equal.mean()) if len(cell) else 0.0,
            "candidate_effective_rate": float((earlier | later).mean()) if len(cell) else 0.0,
            "max_wall_cap_rate": float(max_cap.mean()) if len(cell) else 1.0,
            "aggregate_valid_time_rate": valid_time / max(observed_time, 1.0),
            "median_timing_delta_s": float(delta.median()) if len(cell) else np.nan,
            "p10_timing_delta_s": float(delta.quantile(0.10)) if len(cell) else np.nan,
            "p90_timing_delta_s": float(delta.quantile(0.90)) if len(cell) else np.nan,
            "cpp_mismatch_count": int((~cell["cpp_variance_clock_match"].astype(bool)).sum()),
            **bin_counts,
        }
        row["support_pass"] = bool(
            row["episodes"] >= int(gates["minimum_episodes_per_side"])
            and row["days"] >= int(gates["minimum_days_per_side"])
            and row["n1_episodes"] >= int(gates["minimum_n1_episodes_per_side"])
            and row["n2_episodes"] >= int(gates["minimum_n2_episodes_per_side"])
        )
        row["clock_quality_pass"] = bool(
            row["start_variance_valid_rate"] >= gates["minimum_start_variance_valid_rate"]
            and row["aggregate_valid_time_rate"] >= gates["minimum_aggregate_valid_time_rate"]
            and row["max_wall_cap_rate"] <= gates["maximum_wall_cap_rate"]
            and row["cpp_mismatch_count"] == 0
        )
        row["two_sided_variation_pass"] = bool(
            row["earlier_material_rate"] >= gates["minimum_earlier_material_rate"]
            and row["later_material_rate"] >= gates["minimum_later_material_rate"]
            and row["candidate_effective_rate"] >= gates["minimum_candidate_effective_rate"]
        )
        row["side_feasibility_pass"] = bool(
            row["support_pass"]
            and row["clock_quality_pass"]
            and row["two_sided_variation_pass"]
        )
        cells.append(row)
    cells_frame = pd.DataFrame(cells)

    daily_rows: list[dict[str, Any]] = []
    for (day, side), cell in evaluated.groupby(["day", "side"], sort=True):
        delta = pd.to_numeric(cell["timing_delta_s"], errors="coerce")
        daily_rows.append(
            {
                "day": str(day),
                "side": str(side),
                "episodes": int(len(cell)),
                "earlier_material_rate": float((delta < -material_delta_s).mean()),
                "later_material_rate": float((delta > material_delta_s).mean()),
                "median_timing_delta_s": float(delta.median()),
                "max_wall_cap_rate": float(cell["candidate_reason"].eq("maximum_wall_time").mean()),
                "start_variance_valid_rate": float(cell["start_variance_valid"].astype(bool).mean()),
            }
        )
    daily = pd.DataFrame(daily_rows)
    passed = bool(len(cells_frame) == 2 and cells_frame["side_feasibility_pass"].all())
    decision = (
        "feasibility_pass_register_action_experiment_spec_only"
        if passed
        else "close_variance_time_rearm_on_mechanics"
    )
    summary = {
        "feasibility_passed": passed,
        "decision": decision,
        "reward_or_pnl_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_experiment_created": False,
        "action_or_live_authorization": False,
    }
    return cells_frame, daily, summary


def _load_spec(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unexpected feasibility spec schema")
    if payload.get("outcome_access", {}).get("reward_or_pnl_allowed") is not False:
        raise ValueError("feasibility spec must forbid reward/PnL access")
    return payload


def _write_markdown(path: Path, report: dict[str, Any], cells: pd.DataFrame) -> None:
    lines = [
        "# Volatility-Time Add Rearm Feasibility v1",
        "",
        "Development-only mechanics audit. No reward, PnL, markout, Validation, or holdout was read.",
        "",
        f"- decision: `{report['decision']}`",
        f"- feasibility passed: `{report['feasibility_passed']}`",
        f"- reference variance: `{report['reference_variance_rate_bps2_per_s']}` bps^2/s",
        "",
        "## Side Gates",
        "",
        "```text",
        cells.to_string(index=False),
        "```",
        "",
        "Passing feasibility does not authorize randomized replay or live deployment.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--fill-trace", type=Path, required=True)
    parser.add_argument("--bars-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--require-cpp", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    spec_path = args.spec.expanduser().resolve()
    fill_path = args.fill_trace.expanduser().resolve()
    bars_root = args.bars_root.expanduser().resolve()
    output = args.output_dir.expanduser().resolve()
    spec = _load_spec(spec_path)
    panels = spec["panels"]
    development_days = list(panels["development_days"])
    reference_days = list(panels["reference_days"])
    evaluation_days = list(panels["evaluation_days"])
    if set(reference_days) & set(evaluation_days):
        raise ValueError("reference and evaluation days must be disjoint")
    if sorted(reference_days + evaluation_days) != sorted(development_days):
        raise ValueError("reference/evaluation days must partition Development")
    source = spec["source_identity"]
    if str(fill_path) != str(Path(source["expected_fill_trace_path"]).resolve()):
        raise ValueError("fill trace path differs from frozen spec")
    if str(bars_root) != str(Path(source["bars_root"]).resolve()):
        raise ValueError("bars root differs from frozen spec")

    events = load_fill_events(fill_path, development_days)
    mechanics = spec["mechanics"]
    episodes = build_fill_unit_episodes(
        events,
        order_size_btc=float(mechanics["order_size_btc"]),
        lot_size_btc=float(mechanics["lot_size_btc"]),
    )
    variance = spec["variance_clock"]
    episodes = attach_start_variance_rate(
        episodes,
        bars_root,
        rolling_window_s=int(variance["rolling_window_s"]),
        max_close_carry_ms=int(variance["max_close_carry_ms"]),
        max_abs_return_bps_1s=float(variance["max_abs_return_bps_1s"]),
        max_feature_age_ms=int(variance["max_feature_age_ms"]),
    )
    reference_rates = freeze_reference_rates(episodes, reference_days)

    cpp_module = None
    try:
        import narrowgate_cpp as cpp_module  # type: ignore
    except Exception:
        if args.require_cpp:
            raise
    if args.require_cpp and not hasattr(cpp_module, "integrate_variance_time_episode"):
        raise RuntimeError("narrowgate_cpp lacks the frozen variance-time ABI")

    evaluated = evaluate_episodes(
        episodes,
        bars_root,
        evaluation_days=evaluation_days,
        reference_rates=reference_rates,
        base_cooldown_s=float(mechanics["base_cooldown_s"]),
        minimum_wall_time_ms=int(mechanics["minimum_wall_time_ms"]),
        maximum_wall_time_ms=int(mechanics["maximum_wall_time_ms"]),
        max_feature_age_ms=int(variance["max_feature_age_ms"]),
        rolling_window_s=int(variance["rolling_window_s"]),
        max_close_carry_ms=int(variance["max_close_carry_ms"]),
        max_abs_return_bps_1s=float(variance["max_abs_return_bps_1s"]),
        cpp_module=cpp_module,
    )
    cells, daily, summary = summarize_feasibility(
        evaluated,
        material_delta_s=float(spec["gates"]["material_timing_delta_s"]),
        gates=spec["gates"],
    )

    output.mkdir(parents=True, exist_ok=False)
    episodes_path = output / "development_episode_mechanics.parquet"
    evaluated_path = output / "development_evaluation_episodes.parquet"
    cells_path = output / "side_gate_cells.csv"
    daily_path = output / "daily_timing_metrics.csv"
    report_path = output / "report.json"
    markdown_path = output / "report.md"
    episodes.to_parquet(episodes_path, index=False, compression="zstd")
    evaluated.to_parquet(evaluated_path, index=False, compression="zstd")
    cells.to_csv(cells_path, index=False)
    daily.to_csv(daily_path, index=False)
    report = {
        "schema_version": SCHEMA_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "family_id": "volatility_time_add_rearm_feasibility_v1",
        **summary,
        "spec": {"path": str(spec_path), "sha256": sha256_file(spec_path)},
        "source_fill_trace": {
            "path": str(fill_path),
            "sha256": sha256_file(fill_path),
            "rows_loaded_without_outcomes": int(len(events)),
        },
        "panels": {
            "development_days": development_days,
            "reference_days": reference_days,
            "evaluation_days": evaluation_days,
            "validation_days_read": [],
            "sealed_holdout_days_read": [],
        },
        "reference_variance_rate_bps2_per_s": reference_rates,
        "gates": spec["gates"],
        "side_results": cells.to_dict("records"),
        "artifacts": {
            "episode_mechanics": str(episodes_path),
            "evaluation_episodes": str(evaluated_path),
            "side_gate_cells": str(cells_path),
            "daily_timing_metrics": str(daily_path),
        },
        "implementation": {
            "path": str(Path(__file__).resolve()),
            "sha256": sha256_file(Path(__file__).resolve()),
            "cpp_module_path": str(getattr(cpp_module, "__file__", "")),
            "cpp_module_sha256": (
                sha256_file(Path(cpp_module.__file__))
                if cpp_module is not None and getattr(cpp_module, "__file__", "")
                else ""
            ),
        },
        "workspace": git_workspace_identity(ROOT),
    }
    report["report_payload_sha256"] = canonical_sha256(report)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write_markdown(markdown_path, report, cells)
    manifest = {
        "schema_version": "volatility_time_add_rearm_feasibility_manifest.v1",
        "report": {"path": str(report_path), "sha256": sha256_file(report_path)},
        "artifacts": [
            {"path": str(path), "sha256": sha256_file(path), "bytes": path.stat().st_size}
            for path in (episodes_path, evaluated_path, cells_path, daily_path, markdown_path)
        ],
    }
    (output / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps({"output_dir": str(output), **summary}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
