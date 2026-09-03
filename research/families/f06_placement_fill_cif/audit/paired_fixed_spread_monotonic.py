#!/usr/bin/env python3
"""Paired fixed-spread fill-probability experiment.

Each quote decision creates one shadow cohort containing every configured
distance on one side. Cohort members share activation, cancel request/ACK,
latency draws, TTL opportunity, and the market path. Shadow fills never mutate
inventory or the next quote decision.

Exact-price prints consume visible/calibrated queue by quantity. A print
strictly through an order price forces that better-price order to fill. Formal
runs fail on any pathwise or aggregate deeper-fill/shallow-miss violation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import shutil
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data.normalized_l2_registry import (  # noqa: E402
    DAILY_QUALITY_FILENAME,
    MANIFEST_FILENAME,
    require_formal_days,
)
from data_paths import data_root, normalized_l2_root  # noqa: E402
from models import backtest_tick as bt  # noqa: E402
from research.families.f06_placement_fill_cif.audit.fixed_spread_support import (  # noqa: E402
    DEFAULT_DISTANCES,
    SMOKE_DISTANCES,
    _atomic_write_csv,
    _atomic_write_text,
    _bootstrap_ratio_ci,
    _git_identity,
    _safe_ratio,
    _sha256,
    audit_execution_trade_inputs,
    build_research_params,
    load_quality,
    select_days,
)
from models.data_windows import load_tick_window_dict  # noqa: E402

EXPERIMENT_ID = "paired_fixed_spread_monotonic_v2_20260726"
COUNT_COLUMNS = (
    "submitted_orders",
    "activation_gtx_rejects",
    "cancelled_before_activation",
    "placed_orders",
    "queue_visible_positive_orders",
    "queue_known_zero_orders",
    "queue_fallback_orders",
    "exact_touched_orders",
    "through_touched_orders",
    "any_touched_orders",
    "filled_orders",
    "fully_filled_orders",
    "filled_via_exact_orders",
    "filled_via_through_orders",
    "through_forced_fill_orders",
    "first_fill_pending_cancel_orders",
    "filled_within_1s",
    "filled_within_5s",
    "filled_within_10s",
    "cancelled_unfilled_orders",
    "observed_lifecycle_orders",
    "observed_1s_orders",
    "observed_5s_orders",
    "observed_10s_orders",
    "end_censored_orders",
    "end_censored_before_1s",
    "end_censored_before_5s",
    "end_censored_before_10s",
)


def _normalize_probe_row(
    raw: dict[str, Any],
    *,
    day: str,
    formal_eligible: bool,
    median_price: float,
    runtime_s: float,
    order_size: float,
    tick_size: float,
) -> dict[str, Any]:
    side = str(raw["side"])
    distance_ticks = float(raw["distance_ticks"])
    counts = {name: int(raw.get(name, 0) or 0) for name in COUNT_COLUMNS}
    fill_qty = float(raw.get("fill_qty", 0.0) or 0.0)
    submitted = counts["submitted_orders"]
    rejected = counts["activation_gtx_rejects"]
    pre_activation_cancel = counts["cancelled_before_activation"]
    placed = counts["placed_orders"]
    queue_sources = (
        counts["queue_visible_positive_orders"]
        + counts["queue_known_zero_orders"]
        + counts["queue_fallback_orders"]
    )
    touched = counts["any_touched_orders"]
    filled = counts["filled_orders"]
    if rejected + pre_activation_cancel + placed > submitted:
        raise ValueError(
            f"{day} {side} d={distance_ticks:g}: activation outcomes exceed "
            "submitted cohorts"
        )
    if queue_sources != placed:
        raise ValueError(
            f"{day} {side} d={distance_ticks:g}: queue sources "
            f"{queue_sources} != placed {placed}"
        )
    if not 0 <= filled <= touched <= counts["observed_lifecycle_orders"]:
        raise ValueError(
            f"{day} {side} d={distance_ticks:g}: expected "
            "filled <= touched <= observed lifecycle"
        )
    if (
        counts["filled_via_exact_orders"]
        + counts["filled_via_through_orders"]
        != filled
    ):
        raise ValueError(
            f"{day} {side} d={distance_ticks:g}: first-fill source does not "
            "partition filled orders"
        )
    if not 0 <= counts["fully_filled_orders"] <= filled:
        raise ValueError(
            f"{day} {side} d={distance_ticks:g}: full fills exceed first fills"
        )
    for horizon in ("1s", "5s", "10s"):
        horizon_fills = counts[f"filled_within_{horizon}"]
        horizon_observed = counts[f"observed_{horizon}_orders"]
        if not 0 <= horizon_fills <= horizon_observed:
            raise ValueError(
                f"{day} {side} d={distance_ticks:g}: {horizon} fills exceed "
                "paired denominator"
            )
    max_fill_qty = counts["observed_lifecycle_orders"] * order_size
    if fill_qty < -1e-12 or fill_qty > max_fill_qty + max(1e-12, max_fill_qty * 1e-9):
        raise ValueError(
            f"{day} {side} d={distance_ticks:g}: fill quantity exceeds "
            "closed-cohort capacity"
        )
    distance_price = distance_ticks * tick_size
    return {
        "day": day,
        "formal_eligible": bool(formal_eligible),
        "side": side,
        "distance_ticks": distance_ticks,
        "distance_price": distance_price,
        "distance_bps_at_day_median": (
            distance_price / median_price * 10_000.0
            if median_price > 0.0
            else np.nan
        ),
        "day_median_price": median_price,
        **counts,
        "fill_qty_btc": fill_qty,
        "accepted_qty_btc": counts["observed_lifecycle_orders"] * order_size,
        "activation_probability": _safe_ratio(placed, submitted),
        "exact_touch_probability": _safe_ratio(
            counts["exact_touched_orders"],
            counts["observed_lifecycle_orders"],
        ),
        "through_touch_probability": _safe_ratio(
            counts["through_touched_orders"],
            counts["observed_lifecycle_orders"],
        ),
        "any_touch_probability": _safe_ratio(
            touched,
            counts["observed_lifecycle_orders"],
        ),
        "fill_given_any_touch": _safe_ratio(filled, touched),
        "fill_probability_full_lifecycle": _safe_ratio(
            filled,
            counts["observed_lifecycle_orders"],
        ),
        "fill_probability_1s": _safe_ratio(
            counts["filled_within_1s"],
            counts["observed_1s_orders"],
        ),
        "fill_probability_5s": _safe_ratio(
            counts["filled_within_5s"],
            counts["observed_5s_orders"],
        ),
        "fill_probability_10s": _safe_ratio(
            counts["filled_within_10s"],
            counts["observed_10s_orders"],
        ),
        "first_fill_exact_share": _safe_ratio(
            counts["filled_via_exact_orders"],
            filled,
        ),
        "first_fill_through_share": _safe_ratio(
            counts["filled_via_through_orders"],
            filled,
        ),
        "through_forced_fill_share": _safe_ratio(
            counts["through_forced_fill_orders"],
            filled,
        ),
        "quantity_fill_ratio": _safe_ratio(fill_qty, max_fill_qty),
        "queue_fallback_rate": _safe_ratio(
            counts["queue_fallback_orders"],
            placed,
        ),
        "runtime_s": runtime_s,
        "queue_evidence": "top20_exact_else_frozen_calibrated_fallback",
        "matching_contract": "calibrated_exact_qty_plus_strict_through",
    }


def assert_paired_monotonicity(frame: pd.DataFrame) -> None:
    denominator_columns = (
        "submitted_orders",
        "placed_orders",
        "observed_lifecycle_orders",
        "observed_1s_orders",
        "observed_5s_orders",
        "observed_10s_orders",
    )
    outcome_columns = (
        "filled_orders",
        "fully_filled_orders",
        "filled_within_1s",
        "filled_within_5s",
        "filled_within_10s",
    )
    for (day, side), group in frame.groupby(["day", "side"], sort=False):
        ordered = group.sort_values("distance_ticks")
        for column in denominator_columns:
            if ordered[column].nunique(dropna=False) != 1:
                raise ValueError(
                    f"{day} {side}: paired denominator {column} varies by distance"
                )
        for column in outcome_columns:
            values = ordered[column].to_numpy(dtype=np.int64)
            if np.any(np.diff(values) > 0):
                raise ValueError(
                    f"{day} {side}: deeper distance increases {column}: "
                    f"{values.tolist()}"
                )
        fill_qty = ordered["fill_qty_btc"].to_numpy(dtype=np.float64)
        if np.any(np.diff(fill_qty) > 1e-12):
            raise ValueError(
                f"{day} {side}: deeper distance increases fill_qty_btc: "
                f"{fill_qty.tolist()}"
            )


def run_day(
    *,
    day: str,
    formal_eligible: bool,
    distances: tuple[int, ...],
    base_params: dict[str, Any],
) -> list[dict[str, Any]]:
    window = load_tick_window_dict(
        day,
        base_params,
        load_ml=False,
        require_ml=False,
        cross_market_enabled=False,
        require_historical_bbo=True,
        require_formal_l2=False,
        cache_dir=None,
    )
    params = dict(base_params)
    params["fixed_spread_probe_enabled"] = False
    params["paired_fixed_spread_probe_enabled"] = True
    params["paired_fixed_spread_probe_ticks"] = [float(value) for value in distances]
    params["paired_fixed_spread_fail_on_violation"] = True
    median_price = float(window["trades"]["price"].median())
    started = time.perf_counter()
    result = bt._simulate_tick_with_engine(
        "cpp",
        window["trades"],
        window["var_ts_ms"],
        window["var_ssq"],
        params,
        ml_data=None,
        bbo_data=window["bbo_data"],
        l2_data=window["l2_data"],
        var_ti=window["var_ti"],
        var_retsq=window["var_retsq"],
    )
    runtime_s = time.perf_counter() - started
    violations = result.get("paired_fixed_spread_violations", [])
    if violations:
        raise ValueError(f"{day}: paired replay returned violations: {violations[:3]}")
    rows = [
        _normalize_probe_row(
            raw,
            day=day,
            formal_eligible=formal_eligible,
            median_price=median_price,
            runtime_s=runtime_s,
            order_size=float(params["order_size"]),
            tick_size=float(bt.TICK),
        )
        for raw in result.get("paired_fixed_spread_rows", [])
    ]
    expected_rows = len(distances) * 2
    if len(rows) != expected_rows:
        raise ValueError(
            f"{day}: expected {expected_rows} paired rows, got {len(rows)}"
        )
    frame = pd.DataFrame(rows)
    assert_paired_monotonicity(frame)
    shallow = frame.loc[frame["distance_ticks"] == min(distances)]
    deep = frame.loc[frame["distance_ticks"] == max(distances)]
    shallow_counts = {
        str(side): int(count)
        for side, count in shallow[["side", "filled_orders"]].itertuples(
            index=False,
            name=None,
        )
    }
    deep_counts = {
        str(side): int(count)
        for side, count in deep[["side", "filled_orders"]].itertuples(
            index=False,
            name=None,
        )
    }
    print(
        f"  {day} paired={len(distances)} "
        f"lifecycle shallow={shallow_counts} "
        f"deep={deep_counts} "
        f"{runtime_s:.2f}s"
    )
    return rows


def _aggregate_counts(frame: pd.DataFrame) -> dict[str, float]:
    sum_columns = [*COUNT_COLUMNS, "fill_qty_btc", "accepted_qty_btc"]
    sums = frame[sum_columns].sum()
    out = {name: float(value) for name, value in sums.items()}
    out.update(
        {
            "activation_probability": _safe_ratio(
                sums["placed_orders"],
                sums["submitted_orders"],
            ),
            "exact_touch_probability": _safe_ratio(
                sums["exact_touched_orders"],
                sums["observed_lifecycle_orders"],
            ),
            "through_touch_probability": _safe_ratio(
                sums["through_touched_orders"],
                sums["observed_lifecycle_orders"],
            ),
            "any_touch_probability": _safe_ratio(
                sums["any_touched_orders"],
                sums["observed_lifecycle_orders"],
            ),
            "fill_given_any_touch": _safe_ratio(
                sums["filled_orders"],
                sums["any_touched_orders"],
            ),
            "fill_probability_full_lifecycle": _safe_ratio(
                sums["filled_orders"],
                sums["observed_lifecycle_orders"],
            ),
            "fill_probability_1s": _safe_ratio(
                sums["filled_within_1s"],
                sums["observed_1s_orders"],
            ),
            "fill_probability_5s": _safe_ratio(
                sums["filled_within_5s"],
                sums["observed_5s_orders"],
            ),
            "fill_probability_10s": _safe_ratio(
                sums["filled_within_10s"],
                sums["observed_10s_orders"],
            ),
            "first_fill_exact_share": _safe_ratio(
                sums["filled_via_exact_orders"],
                sums["filled_orders"],
            ),
            "first_fill_through_share": _safe_ratio(
                sums["filled_via_through_orders"],
                sums["filled_orders"],
            ),
            "through_forced_fill_share": _safe_ratio(
                sums["through_forced_fill_orders"],
                sums["filled_orders"],
            ),
            "quantity_fill_ratio": _safe_ratio(
                sums["fill_qty_btc"],
                sums["accepted_qty_btc"],
            ),
            "queue_fallback_rate": _safe_ratio(
                sums["queue_fallback_orders"],
                sums["placed_orders"],
            ),
        }
    )
    return out


def aggregate_daily(
    daily: pd.DataFrame,
    *,
    bootstrap_reps: int,
    bootstrap_seed: int,
    selected_panel: str,
) -> pd.DataFrame:
    if selected_panel == "formal":
        panels = [(f"formal_normalized{daily.day.nunique()}", daily)]
    else:
        panels = [(f"descriptive{daily.day.nunique()}", daily)]
        formal = daily.loc[daily["formal_eligible"]].copy()
        if not formal.empty:
            panels.append((f"formal_normalized{formal.day.nunique()}", formal))
    rows: list[dict[str, Any]] = []
    ci_specs = {
        "exact_touch_probability": (
            "exact_touched_orders",
            "observed_lifecycle_orders",
        ),
        "through_touch_probability": (
            "through_touched_orders",
            "observed_lifecycle_orders",
        ),
        "any_touch_probability": (
            "any_touched_orders",
            "observed_lifecycle_orders",
        ),
        "fill_given_any_touch": (
            "filled_orders",
            "any_touched_orders",
        ),
        "fill_probability_full_lifecycle": (
            "filled_orders",
            "observed_lifecycle_orders",
        ),
        "fill_probability_1s": (
            "filled_within_1s",
            "observed_1s_orders",
        ),
        "fill_probability_5s": (
            "filled_within_5s",
            "observed_5s_orders",
        ),
        "fill_probability_10s": (
            "filled_within_10s",
            "observed_10s_orders",
        ),
    }
    for panel_name, panel_frame in panels:
        for (distance, side), group in panel_frame.groupby(
            ["distance_ticks", "side"],
            sort=True,
        ):
            row: dict[str, Any] = {
                "panel": panel_name,
                "days": int(group["day"].nunique()),
                "side": side,
                "distance_ticks": float(distance),
                "distance_price": float(group["distance_price"].iloc[0]),
                "median_distance_bps": float(
                    group["distance_bps_at_day_median"].median()
                ),
                "runtime_s": float(group["runtime_s"].sum()),
                "queue_evidence": str(group["queue_evidence"].iloc[0]),
                "matching_contract": str(group["matching_contract"].iloc[0]),
                **_aggregate_counts(group),
            }
            for offset, (metric, (numerator, denominator)) in enumerate(
                ci_specs.items()
            ):
                low, high = _bootstrap_ratio_ci(
                    group,
                    numerator=numerator,
                    denominator=denominator,
                    reps=bootstrap_reps,
                    seed=bootstrap_seed + int(distance) * 17 + offset,
                )
                row[f"{metric}_ci_low"] = low
                row[f"{metric}_ci_high"] = high
            rows.append(row)
    output = pd.DataFrame(rows).sort_values(
        ["panel", "side", "distance_ticks"]
    ).reset_index(drop=True)
    assert_paired_monotonicity(
        output.assign(day=output["panel"])[
            [
                "day",
                "side",
                "distance_ticks",
                "submitted_orders",
                "placed_orders",
                "observed_lifecycle_orders",
                "observed_1s_orders",
                "observed_5s_orders",
                "observed_10s_orders",
                "filled_orders",
                "fully_filled_orders",
                "filled_within_1s",
                "filled_within_5s",
                "filled_within_10s",
                "fill_qty_btc",
            ]
        ]
    )
    return output


def write_curve_plot(curve: pd.DataFrame, output_dir: Path) -> Path:
    formal_panels = sorted(
        {
            str(panel)
            for panel in curve["panel"]
            if str(panel).startswith("formal_normalized")
        }
    )
    panel = formal_panels[-1] if formal_panels else str(curve["panel"].iloc[0])
    data = curve.loc[curve["panel"].astype(str) == panel].copy()
    colors = {"BUY": "#0072B2", "SELL": "#D55E00"}
    width = 1_280
    height = 910
    panel_width = 570
    panel_height = 280
    origins = ((75, 100), (690, 100), (75, 500), (690, 500))
    titles = [
        "Lifecycle fill probability",
        "Fill probability by fixed horizon",
        "Exact-price and strictly-through touch",
        "Fill probability conditional on any touch",
    ]
    max_distance = max(1.0, float(data["distance_ticks"].max()))
    x_ticks = [
        tick
        for tick in (0, 1, 5, 20, 80, 220, 600, 1200)
        if tick <= max_distance
    ]

    def sx(value: float, origin_x: float) -> float:
        return origin_x + np.log1p(max(0.0, value)) / np.log1p(
            max_distance
        ) * panel_width

    def sy(value: float, origin_y: float) -> float:
        return origin_y + panel_height * (1.0 - np.clip(value, 0.0, 1.0))

    def polyline(
        group: pd.DataFrame,
        metric: str,
        origin_x: float,
        origin_y: float,
        color: str,
        dash: str = "",
        opacity: float = 1.0,
    ) -> str:
        points = " ".join(
            f"{sx(float(distance), origin_x):.2f},"
            f"{sy(float(value), origin_y):.2f}"
            for distance, value in group[
                ["distance_ticks", metric]
            ].itertuples(index=False, name=None)
        )
        dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
        return (
            f'<polyline points="{points}" fill="none" stroke="{color}" '
            f'stroke-width="2.2" stroke-opacity="{opacity:.2f}"'
            f'{dash_attr} />'
        )

    lines = [
        (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" '
            f'height="{height}" viewBox="0 0 {width} {height}">'
        ),
        '<rect width="100%" height="100%" fill="#ffffff" />',
        (
            '<text x="640" y="34" text-anchor="middle" '
            'font-family="Arial, sans-serif" font-size="22" '
            'font-weight="700">BTCUSDC paired fixed-spread replay</text>'
        ),
        (
            f'<text x="640" y="60" text-anchor="middle" '
            f'font-family="Arial, sans-serif" font-size="14" fill="#555">'
            f'{panel}; log1p distance axis</text>'
        ),
    ]
    for panel_index, (origin_x, origin_y) in enumerate(origins):
        lines.append(
            f'<text x="{origin_x + panel_width / 2:.1f}" '
            f'y="{origin_y - 20}" text-anchor="middle" '
            f'font-family="Arial, sans-serif" font-size="16" '
            f'font-weight="600">{titles[panel_index]}</text>'
        )
        for probability in (0.0, 0.25, 0.5, 0.75, 1.0):
            y = sy(probability, origin_y)
            lines.append(
                f'<line x1="{origin_x}" y1="{y:.2f}" '
                f'x2="{origin_x + panel_width}" y2="{y:.2f}" '
                'stroke="#dddddd" stroke-width="1" />'
            )
            lines.append(
                f'<text x="{origin_x - 9}" y="{y + 4:.2f}" '
                f'text-anchor="end" font-family="Arial, sans-serif" '
                f'font-size="11" fill="#555">{probability:.0%}</text>'
            )
        for tick in x_ticks:
            x = sx(float(tick), origin_x)
            lines.append(
                f'<line x1="{x:.2f}" y1="{origin_y}" x2="{x:.2f}" '
                f'y2="{origin_y + panel_height}" stroke="#eeeeee" '
                'stroke-width="1" />'
            )
            lines.append(
                f'<text x="{x:.2f}" y="{origin_y + panel_height + 19}" '
                f'text-anchor="middle" font-family="Arial, sans-serif" '
                f'font-size="11" fill="#555">{tick:g}</text>'
            )
        lines.append(
            f'<rect x="{origin_x}" y="{origin_y}" width="{panel_width}" '
            f'height="{panel_height}" fill="none" stroke="#777" />'
        )
        if panel_index == 1:
            lines.append(
                f'<text x="{origin_x + panel_width - 8}" '
                f'y="{origin_y + 18}" text-anchor="end" '
                'font-family="Arial, sans-serif" font-size="11" '
                'fill="#555">dotted 1s | dashed 5s | solid 10s</text>'
            )
        elif panel_index == 2:
            lines.append(
                f'<text x="{origin_x + panel_width - 8}" '
                f'y="{origin_y + 18}" text-anchor="end" '
                'font-family="Arial, sans-serif" font-size="11" '
                'fill="#555">solid exact | dashed through</text>'
            )

    for side in ("BUY", "SELL"):
        group = data.loc[data["side"] == side].sort_values("distance_ticks")
        color = colors[side]
        lines.append(
            polyline(
                group,
                "fill_probability_full_lifecycle",
                *origins[0],
                color,
            )
        )
        for metric, dash, opacity in (
            ("fill_probability_1s", "2 5", 0.70),
            ("fill_probability_5s", "8 5", 0.85),
            ("fill_probability_10s", "", 1.00),
        ):
            lines.append(
                polyline(
                    group,
                    metric,
                    *origins[1],
                    color,
                    dash=dash,
                    opacity=opacity,
                )
            )
        lines.append(
            polyline(
                group,
                "exact_touch_probability",
                *origins[2],
                color,
            )
        )
        lines.append(
            polyline(
                group,
                "through_touch_probability",
                *origins[2],
                color,
                dash="8 5",
            )
        )
        lines.append(
            polyline(group, "fill_given_any_touch", *origins[3], color)
        )

    legend_y = 878
    for index, side in enumerate(("BUY", "SELL")):
        x = 430 + index * 180
        lines.extend(
            [
                (
                    f'<line x1="{x}" y1="{legend_y - 5}" x2="{x + 34}" '
                    f'y2="{legend_y - 5}" stroke="{colors[side]}" '
                    'stroke-width="3" />'
                ),
                (
                    f'<text x="{x + 42}" y="{legend_y}" '
                    f'font-family="Arial, sans-serif" font-size="13">'
                    f'{side}</text>'
                ),
            ]
        )
    lines.append(
        '<text x="640" y="832" text-anchor="middle" '
        'font-family="Arial, sans-serif" font-size="12" fill="#555">'
        'Distance from same-side BBO (ticks)</text>'
    )
    lines.append("</svg>")
    svg_path = output_dir / "paired_fixed_spread_curve.svg"
    _atomic_write_text(svg_path, "\n".join(lines) + "\n")
    return svg_path


def build_manifest(
    *,
    args: argparse.Namespace,
    dataset_root: Path,
    quality: pd.DataFrame,
    days: list[str],
    distances: tuple[int, ...],
    params: dict[str, Any],
    execution_trade_quality_path: Path,
    cpp_runtime: dict[str, Any],
) -> dict[str, Any]:
    artifacts: dict[str, str] = {}
    for raw_path in (
        params.get("fill_probability_model_path", ""),
        params.get("queue_calibration_path", ""),
        params.get("live_perf_telemetry_path", ""),
        params.get("dynamic_fill_hazard_action_policy_path", ""),
    ):
        path = Path(str(raw_path or "")).expanduser()
        if path.is_file():
            artifacts[str(path.resolve())] = _sha256(path.resolve())
    implementation_paths = (
        Path(__file__).resolve(),
        ROOT / "models" / "audit" / "fixed_spread_support.py",
        ROOT / "models" / "backtest_tick.py",
        ROOT / "cpp" / "narrowgate_cpp" / "tick_replay.cpp",
        ROOT / "cpp" / "narrowgate_cpp" / "bindings_tick_replay.cpp",
        ROOT / "cpp" / "narrowgate_cpp" / "tick_replay.hpp",
    )
    implementation = {
        str(path): _sha256(path)
        for path in implementation_paths
        if path.is_file()
    }
    cpp_module_path = Path(str(cpp_runtime["archived_path"])).resolve()
    implementation[str(cpp_module_path)] = _sha256(cpp_module_path)
    return {
        "experiment_id": EXPERIMENT_ID,
        "created_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
        "git": _git_identity(),
        "config_path": str(args.config.resolve()),
        "config_sha256": _sha256(args.config.resolve()),
        "dataset_root": str(dataset_root),
        "dataset_manifest_sha256": _sha256(dataset_root / MANIFEST_FILENAME),
        "daily_quality_sha256": _sha256(dataset_root / DAILY_QUALITY_FILENAME),
        "dataset_days": int(len(quality)),
        "formal_days": int(quality["formal_eligible"].sum()),
        "selected_panel": args.panel,
        "selected_days": days,
        "distance_ticks": list(distances),
        "engine": "cpp",
        "matching_contract": "calibrated_exact_qty_plus_strict_through",
        "paired_contract": {
            "activation_support": "all_distances_share_shallowest_gtx_acceptance",
            "common_activation": True,
            "common_cancel_request_ack": True,
            "common_ttl_per_side_cohort": True,
            "shared_market_path": True,
            "fill_feedback_to_next_decision": False,
            "pathwise_monotonicity_fail_fast": True,
            "activation_book_time": "new_order_ack_exchange_time",
            "day_end_censoring": "exclude_incomplete_cohort_at_all_distances",
            "exact_price_queue_consumption": (
                "recorded_trade_qty_times_frozen_queue_depletion_multiplier"
            ),
            "strict_through_behavior": "force_full_fill",
        },
        "execution_trade_source": "trades",
        "execution_trade_quality_path": str(
            execution_trade_quality_path.resolve()
        ),
        "execution_trade_quality_sha256": _sha256(
            execution_trade_quality_path.resolve()
        ),
        "book_visibility": "exchange_time_normalized_100ms",
        "l2_depth": 20,
        "queue_evidence": "top20_exact_else_frozen_calibrated_fallback",
        "strict_calibration": bool(args.strict_calibration),
        "calibration_artifacts": artifacts,
        "implementation_sha256": implementation,
        "cpp_runtime": cpp_runtime,
        "latency_sampler_version": str(
            params.get(
                "latency_sampler_version",
                bt.LATENCY_SAMPLER_VERSION,
            )
        ),
        "latency_profile": params.get("live_perf_telemetry_path", ""),
        "latency_mode": params.get("live_perf_latency_mode", ""),
        "frozen_separate_treatments": list(
            params.get("_frozen_separate_treatments", []) or []
        ),
        "host": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "python": platform.python_version(),
        },
    }


def _archive_cpp_runtime(output_dir: Path) -> dict[str, Any]:
    cpp_module = bt._load_cpp_tick_replay()
    source = Path(str(cpp_module.__file__)).resolve()
    if not source.is_file():
        raise FileNotFoundError(f"C++ replay module is not a file: {source}")
    runtime_dir = output_dir / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    archived = runtime_dir / source.name
    tmp = runtime_dir / f".{source.name}.{time.time_ns()}.tmp"
    shutil.copy2(source, tmp)
    tmp.replace(archived)
    return {
        "source_path": str(source),
        "archived_path": str(archived.resolve()),
        "sha256": _sha256(archived),
        "python_abi": f"cp{sys.version_info.major}{sys.version_info.minor}",
        "paired_abi": bool(
            hasattr(
                cpp_module.TickReplayParams(),
                "paired_fixed_spread_probe_enabled",
            )
        ),
    }


def _freeze_run_identity(manifest: dict[str, Any]) -> dict[str, Any]:
    identity = {
        key: value
        for key, value in manifest.items()
        if key != "created_at_utc"
    }
    encoded = json.dumps(
        identity,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    frozen = dict(manifest)
    frozen["run_identity_sha256"] = hashlib.sha256(encoded).hexdigest()
    return frozen


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument(
        "--dataset-root",
        type=Path,
        default=normalized_l2_root(data_root()),
    )
    parser.add_argument(
        "--panel",
        choices=("descriptive", "formal"),
        default="formal",
    )
    parser.add_argument("--days", nargs="*")
    parser.add_argument("--max-days", type=int)
    parser.add_argument(
        "--distance-ticks",
        nargs="+",
        type=int,
        default=list(DEFAULT_DISTANCES),
    )
    parser.add_argument("--smoke-grid", action="store_true")
    parser.add_argument(
        "--strict-calibration",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--queue-calibration",
        type=Path,
        default=(
            data_root()
            / "reports"
            / "formal_recalibration_20260715"
            / "BTCUSDC-queue-calibration-v3-fit-20260710_11-q070.json"
        ),
    )
    parser.add_argument(
        "--latency-telemetry",
        type=Path,
        required=True,
    )
    parser.add_argument(
        "--latency-mode",
        choices=("avg", "max", "sum"),
        default="avg",
    )
    parser.add_argument("--bootstrap-reps", type=int, default=2_000)
    parser.add_argument("--bootstrap-seed", type=int, default=20260726)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=data_root() / "reports" / EXPERIMENT_ID,
    )
    parser.add_argument("--no-resume", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.config = args.config.expanduser().resolve()
    dataset_root = args.dataset_root.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    quality = load_quality(dataset_root)
    days = select_days(
        quality,
        panel=args.panel,
        requested_days=args.days,
        max_days=args.max_days,
    )
    if args.panel == "formal":
        require_formal_days(dataset_root, days, verify_hashes=True)
    else:
        formal_days = (
            quality.loc[
                quality["day"].astype(str).isin(days)
                & quality["formal_eligible"].astype(bool),
                "day",
            ]
            .astype(str)
            .tolist()
        )
        if formal_days:
            # A descriptive run also publishes a formal subset. Verify that
            # subset against the frozen registry before any outcomes are read.
            require_formal_days(
                dataset_root,
                formal_days,
                verify_hashes=True,
            )
    distances = (
        SMOKE_DISTANCES
        if args.smoke_grid
        else tuple(sorted(set(int(value) for value in args.distance_ticks)))
    )
    if not distances or any(value < 0 for value in distances):
        raise SystemExit("distance ticks must be non-negative")

    bt.BBO_DIR = dataset_root / "bbo"
    bt.L2_DIR = dataset_root / "l2"
    output_dir.mkdir(parents=True, exist_ok=True)
    cpp_runtime = _archive_cpp_runtime(output_dir)
    if not bool(cpp_runtime["paired_abi"]):
        raise RuntimeError(
            "the imported narrowgate_cpp module lacks paired fixed-spread ABI"
        )
    execution_trade_quality_path = output_dir / "execution_trade_quality.csv"
    _atomic_write_csv(
        execution_trade_quality_path,
        audit_execution_trade_inputs(days),
    )
    base_params = build_research_params(
        config_path=args.config,
        strict_calibration=bool(args.strict_calibration),
        queue_calibration_path=(
            args.queue_calibration.expanduser().resolve()
            if args.queue_calibration is not None
            else None
        ),
        latency_telemetry_path=(
            args.latency_telemetry.expanduser().resolve()
            if args.latency_telemetry is not None
            else None
        ),
        latency_mode=str(args.latency_mode),
    )
    base_params["fixed_spread_probe_enabled"] = False
    base_params["paired_fixed_spread_probe_enabled"] = True
    base_params["_historical_bbo_dir"] = str(bt.BBO_DIR)
    base_params["_historical_l2_dir"] = str(bt.L2_DIR)

    daily_path = output_dir / "daily_results.csv"
    frozen_manifest = _freeze_run_identity(
        build_manifest(
            args=args,
            dataset_root=dataset_root,
            quality=quality,
            days=days,
            distances=distances,
            params=base_params,
            execution_trade_quality_path=execution_trade_quality_path,
            cpp_runtime=cpp_runtime,
        )
    )
    prior_manifest_path = output_dir / "manifest.json"
    if (
        not args.no_resume
        and daily_path.is_file()
        and prior_manifest_path.is_file()
    ):
        prior_manifest = json.loads(
            prior_manifest_path.read_text(encoding="utf-8")
        )
        if (
            prior_manifest.get("run_identity_sha256")
            != frozen_manifest["run_identity_sha256"]
        ):
            raise RuntimeError(
                "refusing to resume paired spread results across a different "
                "code/config/data/C++ runtime identity; rerun with --no-resume"
            )
    _atomic_write_text(
        output_dir / "preflight_manifest.json",
        json.dumps(frozen_manifest, indent=2, sort_keys=True) + "\n",
    )
    if args.no_resume:
        for stale_path in (
            daily_path,
            prior_manifest_path,
            output_dir / "paired_spread_fill_curve.csv",
            output_dir / "paired_fixed_spread_curve.svg",
        ):
            stale_path.unlink(missing_ok=True)
    existing = (
        pd.read_csv(daily_path)
        if daily_path.is_file() and not args.no_resume
        else pd.DataFrame()
    )
    completed_days = (
        set(existing["day"].astype(str))
        if not existing.empty
        else set()
    )
    quality_by_day = quality.set_index("day")
    all_rows = existing.to_dict("records") if not existing.empty else []
    for day_index, day in enumerate(days, 1):
        if day in completed_days:
            subset = existing.loc[existing["day"].astype(str) == day]
            if (
                set(subset["distance_ticks"].astype(int)) == set(distances)
                and set(subset["side"].astype(str)) == {"BUY", "SELL"}
            ):
                print(f"[{day_index:03d}/{len(days):03d}] {day}: complete")
                continue
        print(f"[{day_index:03d}/{len(days):03d}] {day}")
        rows = run_day(
            day=day,
            formal_eligible=bool(quality_by_day.loc[day, "formal_eligible"]),
            distances=distances,
            base_params=base_params,
        )
        all_rows = [row for row in all_rows if str(row["day"]) != day]
        all_rows.extend(rows)
        daily = pd.DataFrame(all_rows).sort_values(
            ["day", "side", "distance_ticks"]
        )
        assert_paired_monotonicity(daily)
        _atomic_write_csv(daily_path, daily)

    daily = pd.read_csv(daily_path)
    selected = daily.loc[
        daily["day"].astype(str).isin(days)
        & daily["distance_ticks"].astype(int).isin(distances)
    ].copy()
    assert_paired_monotonicity(selected)
    aggregate = aggregate_daily(
        selected,
        bootstrap_reps=max(0, int(args.bootstrap_reps)),
        bootstrap_seed=int(args.bootstrap_seed),
        selected_panel=args.panel,
    )
    curve_path = output_dir / "paired_spread_fill_curve.csv"
    _atomic_write_csv(curve_path, aggregate)
    plot_svg = write_curve_plot(aggregate, output_dir)
    final_manifest = dict(frozen_manifest)
    final_manifest["outputs"] = {
        str(daily_path.resolve()): _sha256(daily_path.resolve()),
        str(curve_path.resolve()): _sha256(curve_path.resolve()),
        str(plot_svg.resolve()): _sha256(plot_svg.resolve()),
    }
    _atomic_write_text(
        output_dir / "manifest.json",
        json.dumps(final_manifest, indent=2, sort_keys=True) + "\n",
    )
    print(f"Daily rows: {daily_path}")
    print(f"Curve:      {curve_path}")
    print(f"Plot SVG:   {plot_svg}")
    print(f"Manifest:   {output_dir / 'manifest.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
