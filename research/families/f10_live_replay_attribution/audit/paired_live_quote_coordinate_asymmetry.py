"""Audit paired live BUY/SELL quote coordinates without creating an action.

The audit keeps three identities separate:

* paired target quote geometry at one engine decision;
* order-context entry edge for fills with an exact lifecycle join;
* approximate future markout with an explicit observation-freshness bound.

Rows without lifecycle identity are reported but cannot enter the authoritative
entry-edge comparison. This prevents sparse quote logs from silently supplying
a different ``mid`` estimand for unmatched fills.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml
from scipy import stats

IDENTITY = "paired_live_quote_coordinate_asymmetry_audit_v1"
SCHEMA_VERSION = "paired_live_quote_coordinate_asymmetry_audit.v1"

DECISION_COLUMNS = frozenset(
    {
        "timestamp",
        "side",
        "allow_post",
        "allow_exposure_increase",
        "mode",
        "reason_text",
        "inventory_ratio",
        "markout_ema",
        "microprice_shift_bps",
        "spread_mult",
        "mid",
        "base_price",
        "final_price",
        "action",
    }
)
FILL_COLUMNS = frozenset(
    {
        "timestamp",
        "side",
        "role",
        "age_ms",
        "entry_edge_bps",
        "market_move_10s_bps",
        "value_10s_bps",
        "observation_delay_10s",
    }
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _require_columns(
    frame: pd.DataFrame,
    required: frozenset[str],
    name: str,
) -> None:
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"{name} is missing columns: {', '.join(missing)}")


def _finite_numeric(frame: pd.DataFrame, columns: Sequence[str], name: str) -> None:
    for column in columns:
        values = pd.to_numeric(frame[column], errors="coerce")
        if not np.isfinite(values.to_numpy(dtype=float)).all():
            raise ValueError(f"{name}.{column} contains a non-finite value")


def pair_quote_decisions(decisions: pd.DataFrame) -> pd.DataFrame:
    """Pair the engine's adjacent BUY then SELL decision-log writes."""

    _require_columns(decisions, DECISION_COLUMNS, "quote decisions")
    if decisions.empty or len(decisions) % 2:
        raise ValueError("quote decisions must contain complete BUY/SELL pairs")

    frame = decisions.reset_index(drop=True).copy()
    _finite_numeric(
        frame,
        (
            "timestamp",
            "allow_post",
            "allow_exposure_increase",
            "inventory_ratio",
            "markout_ema",
            "microprice_shift_bps",
            "spread_mult",
            "mid",
            "base_price",
            "final_price",
        ),
        "quote decisions",
    )
    buy = frame.iloc[0::2].reset_index(drop=True)
    sell = frame.iloc[1::2].reset_index(drop=True)
    if not buy["side"].eq("BUY").all() or not sell["side"].eq("SELL").all():
        raise ValueError("quote-decision write order is not strict BUY then SELL")
    if not np.allclose(buy["mid"], sell["mid"], rtol=0.0, atol=1e-12):
        raise ValueError("paired quote decisions do not share one mid")
    if not np.allclose(
        buy["inventory_ratio"],
        sell["inventory_ratio"],
        rtol=0.0,
        atol=1e-12,
    ):
        raise ValueError("paired quote decisions do not share one inventory state")
    pair_delay_ms = (
        sell["timestamp"].to_numpy(dtype=float)
        - buy["timestamp"].to_numpy(dtype=float)
    ) * 1000.0
    if np.any(pair_delay_ms < -1e-6) or np.any(pair_delay_ms > 1000.0):
        raise ValueError("BUY/SELL log writes do not belong to one decision cycle")

    return pd.DataFrame(
        {
            "timestamp": buy["timestamp"].to_numpy(dtype=float),
            "sell_timestamp": sell["timestamp"].to_numpy(dtype=float),
            "pair_delay_ms": pair_delay_ms,
            "mid": buy["mid"].to_numpy(dtype=float),
            "inventory_ratio": buy["inventory_ratio"].to_numpy(dtype=float),
            "buy_allow_post": buy["allow_post"].astype(bool).to_numpy(),
            "sell_allow_post": sell["allow_post"].astype(bool).to_numpy(),
            "buy_allow_exposure": buy["allow_exposure_increase"].astype(bool).to_numpy(),
            "sell_allow_exposure": sell["allow_exposure_increase"].astype(bool).to_numpy(),
            "buy_mode": buy["mode"].astype(str).to_numpy(),
            "sell_mode": sell["mode"].astype(str).to_numpy(),
            "buy_reason": buy["reason_text"].astype(str).to_numpy(),
            "sell_reason": sell["reason_text"].astype(str).to_numpy(),
            "buy_action": buy["action"].astype(str).to_numpy(),
            "sell_action": sell["action"].astype(str).to_numpy(),
            "microprice_shift_bps": buy["microprice_shift_bps"].to_numpy(dtype=float),
            "buy_markout_ema": buy["markout_ema"].to_numpy(dtype=float),
            "sell_markout_ema": sell["markout_ema"].to_numpy(dtype=float),
            "buy_spread_mult": buy["spread_mult"].to_numpy(dtype=float),
            "sell_spread_mult": sell["spread_mult"].to_numpy(dtype=float),
            "bid_base": buy["base_price"].to_numpy(dtype=float),
            "ask_base": sell["base_price"].to_numpy(dtype=float),
            "bid_final": buy["final_price"].to_numpy(dtype=float),
            "ask_final": sell["final_price"].to_numpy(dtype=float),
        }
    )


def _add_quote_geometry(frame: pd.DataFrame, *, tick_size: float) -> pd.DataFrame:
    result = frame.copy()
    mid = result["mid"]
    if (mid <= 0.0).any() or tick_size <= 0.0:
        raise ValueError("mid and tick size must be positive")
    result["base_buy_distance_bps"] = (mid - result["bid_base"]) / mid * 1e4
    result["base_sell_distance_bps"] = (result["ask_base"] - mid) / mid * 1e4
    result["final_buy_distance_bps"] = (mid - result["bid_final"]) / mid * 1e4
    result["final_sell_distance_bps"] = (result["ask_final"] - mid) / mid * 1e4
    result["base_sell_minus_buy_bps"] = (
        result["base_sell_distance_bps"] - result["base_buy_distance_bps"]
    )
    result["final_sell_minus_buy_bps"] = (
        result["final_sell_distance_bps"] - result["final_buy_distance_bps"]
    )
    result["final_sell_minus_buy_ticks"] = (
        result["ask_final"] + result["bid_final"] - 2.0 * mid
    ) / tick_size
    result["buy_policy_widen_bps"] = (
        result["bid_base"] - result["bid_final"]
    ) / mid * 1e4
    result["sell_policy_widen_bps"] = (
        result["ask_final"] - result["ask_base"]
    ) / mid * 1e4
    result["day"] = pd.to_datetime(
        result["timestamp"], unit="s", utc=True
    ).dt.strftime("%Y-%m-%d")
    return result


def _metric_summary(frame: pd.DataFrame, columns: Sequence[str]) -> dict[str, Any]:
    result: dict[str, Any] = {"rows": int(len(frame))}
    for column in columns:
        values = frame[column]
        result[column] = {
            "mean": float(values.mean()),
            "median": float(values.median()),
            "p10": float(values.quantile(0.10)),
            "p90": float(values.quantile(0.90)),
        }
    return result


def _day_cluster_t_interval(
    frame: pd.DataFrame,
    column: str,
) -> dict[str, float | int]:
    daily = frame.groupby("day", sort=True)[column].mean()
    if len(daily) < 2:
        raise ValueError("day-clustered interval requires at least two UTC days")
    standard_error = float(daily.std(ddof=1) / math.sqrt(len(daily)))
    critical = float(stats.t.ppf(0.975, len(daily) - 1))
    mean = float(daily.mean())
    return {
        "days": int(len(daily)),
        "equal_day_mean": mean,
        "lower_95": mean - critical * standard_error,
        "upper_95": mean + critical * standard_error,
        "positive_day_rate": float(daily.gt(0.0).mean()),
        "minimum_daily_mean": float(daily.min()),
        "maximum_daily_mean": float(daily.max()),
    }


def _side_fill_summary(frame: pd.DataFrame) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for side, group in frame.groupby("side", sort=True):
        result[str(side)] = {
            "fills": int(len(group)),
            "entry_edge_bps_mean": float(group["entry_edge_bps"].mean()),
            "entry_edge_bps_median": float(group["entry_edge_bps"].median()),
            "market_move_10s_bps_mean": float(group["market_move_10s_bps"].mean()),
            "value_10s_bps_mean": float(group["value_10s_bps"].mean()),
            "value_10s_win_rate": float(group["value_10s_bps"].gt(0.0).mean()),
        }
    if set(result) == {"BUY", "SELL"}:
        result["SELL_minus_BUY_entry_edge_bps"] = (
            result["SELL"]["entry_edge_bps_mean"]
            - result["BUY"]["entry_edge_bps_mean"]
        )
        result["SELL_minus_BUY_value_10s_bps"] = (
            result["SELL"]["value_10s_bps_mean"]
            - result["BUY"]["value_10s_bps_mean"]
        )
    return result


def _mode_rows(frame: pd.DataFrame) -> list[dict[str, Any]]:
    grouped = (
        frame.groupby(["buy_mode", "sell_mode"], sort=True)
        .agg(
            rows=("timestamp", "size"),
            base_sell_minus_buy_bps=("base_sell_minus_buy_bps", "mean"),
            final_sell_minus_buy_bps=("final_sell_minus_buy_bps", "mean"),
            buy_policy_widen_bps=("buy_policy_widen_bps", "mean"),
            sell_policy_widen_bps=("sell_policy_widen_bps", "mean"),
        )
        .reset_index()
        .sort_values("rows", ascending=False)
    )
    return grouped.to_dict(orient="records")


def evaluate_frames(
    decisions: pd.DataFrame,
    fills: pd.DataFrame,
    *,
    start_ts: float,
    end_ts: float,
    tick_size: float,
    markout_spread_scale: float,
    markout_side_asymmetry_sign: float,
    markout_reference: float = 50.0,
    maximum_future_observation_delay_s: float = 10.0,
) -> dict[str, Any]:
    if end_ts <= start_ts:
        raise ValueError("end_ts must be after start_ts")
    if markout_side_asymmetry_sign not in (-1.0, 1.0):
        raise ValueError("markout side-asymmetry sign must be -1 or +1")

    paired = pair_quote_decisions(decisions)
    paired = paired.loc[
        paired["timestamp"].between(start_ts, end_ts, inclusive="both")
    ].copy()
    if paired.empty:
        raise ValueError("no paired quote decisions fall inside the audit window")
    paired = _add_quote_geometry(paired, tick_size=tick_size)
    flat = paired.loc[paired["inventory_ratio"].abs() <= 1e-12].copy()
    common = flat.loc[
        flat["buy_allow_post"]
        & flat["sell_allow_post"]
        & flat["buy_allow_exposure"]
        & flat["sell_allow_exposure"]
    ].copy()
    if common.empty:
        raise ValueError("no flat common-support quote decisions")

    quote_columns = (
        "base_buy_distance_bps",
        "base_sell_distance_bps",
        "base_sell_minus_buy_bps",
        "final_buy_distance_bps",
        "final_sell_distance_bps",
        "final_sell_minus_buy_bps",
        "final_sell_minus_buy_ticks",
        "buy_policy_widen_bps",
        "sell_policy_widen_bps",
    )

    # Positive maker-signed markout means favorable for both BUY and SELL.
    # Therefore a better BUY EMA than SELL EMA must move the quote center up:
    # BUY closer, SELL farther, which is the +1 sign in quote_core.asym.
    markout_diff = common["buy_markout_ema"] - common["sell_markout_ema"]
    pair_spread_bps = (
        common["base_buy_distance_bps"] + common["base_sell_distance_bps"]
    )
    current_markout_coordinate_contribution_bps = pair_spread_bps * (
        markout_side_asymmetry_sign
        * markout_spread_scale
        * np.tanh(markout_diff / max(markout_reference, 1e-12))
        * 0.5
    )
    corrected_markout_coordinate_contribution_bps = pair_spread_bps * (
        1.0
        * markout_spread_scale
        * np.tanh(markout_diff / max(markout_reference, 1e-12))
        * 0.5
    )

    _require_columns(fills, FILL_COLUMNS, "fills")
    fill_frame = fills.loc[
        pd.to_numeric(fills["timestamp"], errors="coerce").between(
            start_ts, end_ts, inclusive="both"
        )
    ].copy()
    if fill_frame.empty:
        raise ValueError("no fills fall inside the audit window")
    for column in (
        "timestamp",
        "age_ms",
        "entry_edge_bps",
        "market_move_10s_bps",
        "value_10s_bps",
        "observation_delay_10s",
    ):
        fill_frame[column] = pd.to_numeric(fill_frame[column], errors="coerce")
    if not fill_frame["side"].isin(("BUY", "SELL")).all():
        raise ValueError("fills contain an invalid side")

    opener = fill_frame.loc[fill_frame["role"].eq("opener")].copy()
    lifecycle_opener = opener.loc[
        opener["age_ms"].notna()
        & np.isfinite(opener["age_ms"])
        & opener["entry_edge_bps"].notna()
        & np.isfinite(opener["entry_edge_bps"])
    ].copy()
    if lifecycle_opener.empty:
        raise ValueError("no exact-lifecycle opener fills")
    unmatched_opener = opener.loc[~opener.index.isin(lifecycle_opener.index)].copy()
    fresh_value = fill_frame.loc[
        fill_frame["age_ms"].notna()
        & np.isfinite(fill_frame["age_ms"])
        & fill_frame["observation_delay_10s"].between(
            0.0,
            maximum_future_observation_delay_s,
            inclusive="both",
        )
        & fill_frame["entry_edge_bps"].notna()
        & fill_frame["market_move_10s_bps"].notna()
        & fill_frame["value_10s_bps"].notna()
    ].copy()
    accounting_error = (
        fresh_value["entry_edge_bps"]
        + fresh_value["market_move_10s_bps"]
        - fresh_value["value_10s_bps"]
    ).abs()
    if fresh_value.empty or float(accounting_error.max()) > 1e-8:
        raise ValueError("fresh fill-value rows fail maker-value accounting")

    ordered_pair_ts = paired["timestamp"].sort_values().drop_duplicates()
    gaps = ordered_pair_ts.diff().dropna()
    largest_gap_index = gaps.idxmax()
    largest_gap_end = float(ordered_pair_ts.loc[largest_gap_index])
    previous_positions = ordered_pair_ts.index.get_loc(largest_gap_index) - 1
    largest_gap_start = float(ordered_pair_ts.iloc[previous_positions])

    all_opener_summary = _side_fill_summary(opener)
    lifecycle_opener_summary = _side_fill_summary(lifecycle_opener)
    old_gap = float(all_opener_summary["SELL_minus_BUY_entry_edge_bps"])
    exact_gap = float(
        lifecycle_opener_summary["SELL_minus_BUY_entry_edge_bps"]
    )
    final_coordinate_mean = float(common["final_sell_minus_buy_bps"].mean())
    final_coordinate_interval = _day_cluster_t_interval(
        common,
        "final_sell_minus_buy_bps",
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "window": {
            "start_ts": float(start_ts),
            "end_ts": float(end_ts),
            "start_utc": pd.to_datetime(start_ts, unit="s", utc=True).isoformat(),
            "end_utc": pd.to_datetime(end_ts, unit="s", utc=True).isoformat(),
        },
        "quote_decision_contract": {
            "paired_rows": int(len(paired)),
            "flat_rows": int(len(flat)),
            "flat_common_support_rows": int(len(common)),
            "pair_delay_ms_max": float(paired["pair_delay_ms"].max()),
            "largest_quote_decision_gap_s": float(gaps.max()),
            "largest_quote_decision_gap_start_utc": pd.to_datetime(
                largest_gap_start, unit="s", utc=True
            ).isoformat(),
            "largest_quote_decision_gap_end_utc": pd.to_datetime(
                largest_gap_end, unit="s", utc=True
            ).isoformat(),
            "gaps_over_60s": int(gaps.gt(60.0).sum()),
        },
        "flat_common_support_quote_geometry": _metric_summary(
            common,
            quote_columns,
        ),
        "final_coordinate_day_clustered_interval": final_coordinate_interval,
        "mode_decomposition": _mode_rows(common),
        "markout_asymmetry_contract": {
            "markout_ema_semantics": "maker_signed_positive_is_favorable_for_both_sides",
            "configured_sign": float(markout_side_asymmetry_sign),
            "required_semantic_sign": 1.0,
            "semantic_contract_valid": bool(markout_side_asymmetry_sign == 1.0),
            "current_coordinate_contribution_bps_mean": float(
                current_markout_coordinate_contribution_bps.mean()
            ),
            "corrected_minus_current_coordinate_bps_mean": float(
                (
                    corrected_markout_coordinate_contribution_bps
                    - current_markout_coordinate_contribution_bps
                ).mean()
            ),
            "corrected_minus_current_coordinate_bps_p10": float(
                (
                    corrected_markout_coordinate_contribution_bps
                    - current_markout_coordinate_contribution_bps
                ).quantile(0.10)
            ),
            "corrected_minus_current_coordinate_bps_p90": float(
                (
                    corrected_markout_coordinate_contribution_bps
                    - current_markout_coordinate_contribution_bps
                ).quantile(0.90)
            ),
        },
        "opener_entry_edge_identity": {
            "all_opener_fills": int(len(opener)),
            "exact_lifecycle_opener_fills": int(len(lifecycle_opener)),
            "missing_lifecycle_opener_fills": int(len(unmatched_opener)),
            "all_rows_mixed_identity_summary": all_opener_summary,
            "exact_lifecycle_summary": lifecycle_opener_summary,
            "mixed_identity_sell_minus_buy_bps": old_gap,
            "exact_lifecycle_sell_minus_buy_bps": exact_gap,
            "mixed_identity_gap_withdrawn": True,
        },
        "fresh_10s_fill_value_sensitivity": {
            "maximum_future_observation_delay_s": float(
                maximum_future_observation_delay_s
            ),
            "fills": int(len(fresh_value)),
            "coverage": float(len(fresh_value) / len(fill_frame)),
            "summary": _side_fill_summary(fresh_value),
            "maker_value_accounting_max_abs_error_bps": float(
                accounting_error.max()
            ),
        },
        "decision": {
            "structural_sell_quote_too_close_supported": False,
            "observed_flat_final_sell_minus_buy_bps": final_coordinate_mean,
            "flat_final_coordinate_interval_contains_zero": bool(
                final_coordinate_interval["lower_95"] <= 0.0
                <= final_coordinate_interval["upper_95"]
            ),
            "historical_0p40bps_fill_edge_gap_valid": False,
            "baseline_markout_asymmetry_semantics_requires_correction": bool(
                markout_side_asymmetry_sign != 1.0
            ),
            "inventory_action_or_alpha_authorized": False,
            "live_deployment_authorized": False,
        },
    }


def evaluate_paths(
    *,
    decisions_path: Path,
    fills_path: Path,
    config_path: Path,
    runtime_config_code_path: Path,
    runtime_quote_core_code_path: Path,
    start_ts: float,
    end_ts: float,
    maximum_future_observation_delay_s: float,
) -> dict[str, Any]:
    decisions_path = Path(decisions_path).expanduser().resolve()
    fills_path = Path(fills_path).expanduser().resolve()
    config_path = Path(config_path).expanduser().resolve()
    runtime_config_code_path = Path(runtime_config_code_path).expanduser().resolve()
    runtime_quote_core_code_path = Path(runtime_quote_core_code_path).expanduser().resolve()
    decisions = pd.read_csv(decisions_path)
    fills = pd.read_parquet(fills_path)
    config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    strategy = config.get("strategy") or {}
    exchange = config.get("exchange") or {}
    configured_sign = strategy.get("markout_side_asymmetry_sign")
    sign_source = "runtime_yaml"
    if configured_sign is None:
        source = runtime_config_code_path.read_text(encoding="utf-8")
        match = re.search(
            r"markout_side_asymmetry_sign:\s*float\s*=\s*([-+]?[0-9.]+)",
            source,
        )
        if match is None:
            raise ValueError("runtime config code does not expose the markout sign default")
        configured_sign = float(match.group(1))
        sign_source = "runtime_config_code_default"
    result = evaluate_frames(
        decisions,
        fills,
        start_ts=start_ts,
        end_ts=end_ts,
        tick_size=float(exchange.get("tick_size", config.get("tick_size", 0.1))),
        markout_spread_scale=float(strategy.get("markout_spread_scale", 0.2)),
        markout_side_asymmetry_sign=float(configured_sign),
        maximum_future_observation_delay_s=maximum_future_observation_delay_s,
    )
    result["inputs"] = {
        "decisions_path": str(decisions_path),
        "decisions_sha256": sha256_file(decisions_path),
        "fills_path": str(fills_path),
        "fills_sha256": sha256_file(fills_path),
        "config_path": str(config_path),
        "config_sha256": sha256_file(config_path),
        "runtime_config_code_path": str(runtime_config_code_path),
        "runtime_config_code_sha256": sha256_file(runtime_config_code_path),
        "runtime_quote_core_code_path": str(runtime_quote_core_code_path),
        "runtime_quote_core_code_sha256": sha256_file(runtime_quote_core_code_path),
        "markout_side_asymmetry_sign_source": sign_source,
    }
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--decisions", type=Path, required=True)
    parser.add_argument("--fills", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--runtime-config-code", type=Path, required=True)
    parser.add_argument("--runtime-quote-core-code", type=Path, required=True)
    parser.add_argument("--start-ts", type=float, required=True)
    parser.add_argument("--end-ts", type=float, required=True)
    parser.add_argument("--maximum-future-observation-delay-s", type=float, default=10.0)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = evaluate_paths(
        decisions_path=args.decisions,
        fills_path=args.fills,
        config_path=args.config,
        runtime_config_code_path=args.runtime_config_code,
        runtime_quote_core_code_path=args.runtime_quote_core_code,
        start_ts=args.start_ts,
        end_ts=args.end_ts,
        maximum_future_observation_delay_s=args.maximum_future_observation_delay_s,
    )
    output = args.output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.write_text(
        json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(output)
    print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
