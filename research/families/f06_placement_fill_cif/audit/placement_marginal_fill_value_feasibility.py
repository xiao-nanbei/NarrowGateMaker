#!/usr/bin/env python3
"""Development-only feasibility audit for paired marginal placement fills.

The audit trains no fill model and grants no action authority. It reuses the
hash-frozen seven-price lifecycle replay, values shallower-only fills on the
same market path, and asks whether baseline-relative two- or four-tick
contrasts have economically resolved ``deeper - shallower`` value. One-tick
contrasts are retained only as a negative control.
"""

from __future__ import annotations

import argparse
import json
import math
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import beta as beta_distribution

from data_paths import data_root
from models.audit.content_addressed_cache import canonical_sha256, file_sha256
from research.families.f06_placement_fill_cif import FAMILY_DOCS

ROOT = Path(__file__).resolve().parents[4]
DATA_ROOT = data_root(ROOT)
DEFAULT_SPEC = (
    FAMILY_DOCS
    / "placement_marginal_fill_value_feasibility_v1_spec_20260729.json"
)
DEFAULT_OUTPUT = (
    DATA_ROOT
    / "reports"
    / "placement_marginal_fill_value_feasibility_v1_development_20260729"
)

SCHEMA_VERSION = "placement_marginal_fill_value_feasibility.v1"
TICK_SIZE = 0.1
LOT_SIZE = 0.001
ACTION_GAPS = (1, 2, 4)
FORMAL_GAPS = (2, 4)
CONTRASTS = ("closer_current", "current_farther")
MARKOUT_HORIZONS_MS = (1_000, 5_000, 30_000)
CELL_COLUMNS = (
    "request_scope",
    "side",
    "inventory_role",
    "gap_ticks",
    "contrast",
)

VALUE_METRICS = (
    "fill_probability_delta_deeper_minus_shallower",
    "filled_quantity_delta_btc_deeper_minus_shallower",
    "shared_execution_improvement_usdc",
    "marginal_shallower_value_1s_usdc",
    "marginal_shallower_value_5s_usdc",
    "marginal_shallower_value_30s_usdc",
    "shared_value_delta_1s_usdc",
    "shared_value_delta_5s_usdc",
    "shared_value_delta_30s_usdc",
    "total_value_delta_1s_usdc",
    "total_value_delta_5s_usdc",
    "total_value_delta_30s_usdc",
    "total_value_delta_common_clock_usdc",
    "campaign_terminal_overlay_delta_usdc",
    "pending_fill_probability_delta",
    "pending_filled_quantity_delta_btc",
    "pending_value_delta_30s_usdc",
    "pending_campaign_overlay_delta_usdc",
    "campaign_tail_event_delta",
)

COUNT_COLUMNS = (
    "decisions",
    "shared_fill_decisions",
    "marginal_fill_decisions",
    "neither_fill_decisions",
    "fill_affected_decisions",
    "campaign_overlay_supported_affected_decisions",
    "campaign_tail_supported_affected_decisions",
    "pending_fill_decisions",
)


def _atomic_json(payload: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _identity(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(resolved)
    return {
        "path": str(resolved),
        "size_bytes": int(resolved.stat().st_size),
        "sha256": file_sha256(resolved),
    }


def _require_identity(identity: Mapping[str, Any], label: str) -> Path:
    path = Path(str(identity["path"])).expanduser().resolve()
    actual = _identity(path)
    if actual["sha256"] != str(identity["sha256"]):
        raise RuntimeError(f"{label} SHA256 changed: {path}")
    if "size_bytes" in identity and actual["size_bytes"] != int(
        identity["size_bytes"]
    ):
        raise RuntimeError(f"{label} size changed: {path}")
    return path


def _pair_actions(gap_ticks: int, contrast: str) -> tuple[str, str]:
    gap = int(gap_ticks)
    if gap not in ACTION_GAPS:
        raise ValueError(f"unsupported action gap: {gap}")
    if contrast == "closer_current":
        return f"closer_{gap}tick", "current"
    if contrast == "current_farther":
        return "current", f"farther_{gap}tick"
    raise ValueError(f"unsupported contrast: {contrast}")


def _common_activation_support(valued_actions: pd.DataFrame) -> pd.DataFrame:
    expected_actions = [
        "closer_4tick",
        "closer_2tick",
        "closer_1tick",
        "current",
        "farther_1tick",
        "farther_2tick",
        "farther_4tick",
    ]
    activation = valued_actions.pivot(
        index="cohort_id", columns="action", values="activation_status"
    ).reindex(columns=expected_actions)
    supported = activation.astype(str).eq("active").all(axis=1)
    supported_ids = activation.index[supported]
    return valued_actions.loc[
        valued_actions["cohort_id"].isin(supported_ids)
    ].copy()


def _maker_signed_value(
    side: pd.Series,
    quantity: pd.Series,
    fill_price: pd.Series,
    future_mid: pd.Series,
) -> pd.Series:
    sign = np.where(side.astype(str).str.upper().eq("BUY"), 1.0, -1.0)
    return pd.Series(
        sign
        * pd.to_numeric(quantity, errors="coerce").to_numpy(float)
        * (
            pd.to_numeric(future_mid, errors="coerce").to_numpy(float)
            - pd.to_numeric(fill_price, errors="coerce").to_numpy(float)
        ),
        index=side.index,
        dtype=float,
    )


def _asof_mid(
    bbo_ts_ms: np.ndarray,
    bbo_mid: np.ndarray,
    target_ts_ns: np.ndarray,
    *,
    max_age_ms: float,
) -> tuple[np.ndarray, np.ndarray]:
    target_ms = np.floor_divide(target_ts_ns.astype(np.int64), 1_000_000)
    positions = np.searchsorted(bbo_ts_ms, target_ms, side="right") - 1
    valid = positions >= 0
    clipped = np.clip(positions, 0, max(0, len(bbo_ts_ms) - 1))
    age = np.full(len(target_ms), np.nan, dtype=float)
    value = np.full(len(target_ms), np.nan, dtype=float)
    if len(bbo_ts_ms):
        age[valid] = target_ms[valid] - bbo_ts_ms[clipped[valid]]
        valid &= age >= 0.0
        valid &= age <= float(max_age_ms)
        value[valid] = bbo_mid[clipped[valid]]
    return value, age


def _load_bbo(
    path: Path,
    *,
    expected_sha256: str,
) -> tuple[np.ndarray, np.ndarray]:
    if file_sha256(path) != str(expected_sha256):
        raise RuntimeError(f"BBO identity changed: {path}")
    frame = pd.read_parquet(path, columns=["timestamp", "best_bid", "best_ask"])
    timestamp = pd.to_numeric(frame["timestamp"], errors="raise").to_numpy(
        dtype=np.int64,
        copy=False,
    )
    bid = pd.to_numeric(frame["best_bid"], errors="raise").to_numpy(float)
    ask = pd.to_numeric(frame["best_ask"], errors="raise").to_numpy(float)
    valid = (timestamp > 0) & (bid > 0.0) & (ask > bid)
    timestamp = timestamp[valid]
    mid = ((bid[valid] + ask[valid]) * 0.5).astype(float, copy=False)
    order = np.argsort(timestamp, kind="stable")
    timestamp = timestamp[order]
    mid = mid[order]
    keep = np.r_[timestamp[1:] != timestamp[:-1], True]
    return timestamp[keep], mid[keep]


def _bbo_identity_by_day(
    registry_manifest: Mapping[str, Any],
    *,
    bbo_root: Path,
    days: Sequence[str],
) -> dict[str, dict[str, Any]]:
    wanted = set(map(str, days))
    rows: dict[str, dict[str, Any]] = {}
    for item in registry_manifest.get("files", []):
        day = str(item.get("day", ""))
        if day not in wanted or str(item.get("kind")) != "bbo":
            continue
        path = bbo_root / str(item["destination_relative_path"]).removeprefix(
            "bbo/"
        )
        source = item["source_identity"]
        rows[day] = {
            "path": str(path.resolve()),
            "sha256": str(source["sha256"]),
            "size_bytes": int(path.stat().st_size),
        }
    missing = sorted(wanted - set(rows))
    if missing:
        raise RuntimeError(f"BBO registry is missing Development days: {missing}")
    return rows


def _campaign_start_table(source: pd.DataFrame) -> pd.DataFrame:
    active = source.loc[source["campaign_id"].gt(0)].copy()
    if active.empty:
        return pd.DataFrame(columns=["campaign_id", "campaign_start_ts_ns"])
    active["campaign_start_ts_ns"] = (
        pd.to_numeric(active["submit_ts_ns"], errors="coerce")
        - pd.to_numeric(active["campaign_age_s"], errors="coerce") * 1e9
    )
    starts = (
        active.groupby("campaign_id", observed=True)["campaign_start_ts_ns"]
        .median()
        .round(-6)
        .astype("int64")
        .reset_index()
    )
    return starts.sort_values("campaign_start_ts_ns", kind="stable")


def _map_cohorts_to_campaigns(
    source: pd.DataFrame,
    campaign_starts: pd.DataFrame,
    *,
    opener_tolerance_ms: float,
) -> pd.DataFrame:
    mapped = source.loc[
        :,
        [
            "cohort_id",
            "campaign_id",
            "current__first_fill_ts_ns",
            "current__fill_qty",
        ],
    ].copy()
    mapped.rename(columns={"campaign_id": "source_campaign_id"}, inplace=True)
    mapped["mapped_campaign_id"] = mapped["source_campaign_id"].astype("int64")
    mapped["campaign_mapping_source"] = np.where(
        mapped["source_campaign_id"].gt(0), "active_campaign_id", "unmapped"
    )
    if campaign_starts.empty:
        return mapped
    target = mapped.loc[
        mapped["source_campaign_id"].eq(0)
        & mapped["current__first_fill_ts_ns"].gt(0)
        & mapped["current__fill_qty"].gt(0)
    ]
    if target.empty:
        return mapped
    starts = campaign_starts["campaign_start_ts_ns"].to_numpy(np.int64)
    ids = campaign_starts["campaign_id"].to_numpy(np.int64)
    fill_ts = target["current__first_fill_ts_ns"].to_numpy(np.int64)
    positions = np.searchsorted(starts, fill_ts)
    selected_id = np.zeros(len(target), dtype=np.int64)
    selected_gap_ms = np.full(len(target), np.inf, dtype=float)
    for offset in (-1, 0, 1):
        candidate = positions + offset
        valid = (candidate >= 0) & (candidate < len(starts))
        if not valid.any():
            continue
        gap_ms = np.full(len(target), np.inf, dtype=float)
        gap_ms[valid] = np.abs(starts[candidate[valid]] - fill_ts[valid]) / 1e6
        better = gap_ms < selected_gap_ms
        selected_gap_ms[better] = gap_ms[better]
        selected_id[better] = ids[candidate[better]]
    accepted = selected_gap_ms <= float(opener_tolerance_ms)
    accepted_index = target.index.to_numpy()[accepted]
    mapped.loc[accepted_index, "mapped_campaign_id"] = selected_id[accepted]
    mapped.loc[accepted_index, "campaign_mapping_source"] = "opener_start_match"
    mapped["campaign_start_match_gap_ms"] = np.nan
    mapped.loc[target.index, "campaign_start_match_gap_ms"] = selected_gap_ms
    return mapped


def reconstruct_campaign_terminals(
    source: pd.DataFrame,
    *,
    bbo_ts_ms: np.ndarray,
    bbo_mid: np.ndarray,
    max_bbo_age_ms: float,
) -> pd.DataFrame:
    fills = source.loc[
        source["campaign_id"].gt(0)
        & source["inventory_role"].astype(str).eq("reducing")
        & source["current__first_fill_ts_ns"].gt(0)
        & source["current__fill_qty"].gt(0)
    ].copy()
    fills = fills.loc[
        pd.to_numeric(fills["inventory"], errors="coerce").abs()
        <= pd.to_numeric(fills["current__fill_qty"], errors="coerce") + 1e-12
    ]
    if fills.empty:
        return pd.DataFrame(
            columns=[
                "campaign_id",
                "campaign_terminal_ts_ns",
                "campaign_terminal_mid",
                "baseline_campaign_terminal_pnl_usdc",
                "campaign_terminal_bbo_age_ms",
            ]
        )
    fills.sort_values(
        ["campaign_id", "current__first_fill_ts_ns"], kind="stable", inplace=True
    )
    fills = fills.drop_duplicates("campaign_id", keep="first").copy()
    terminal_mid, terminal_age = _asof_mid(
        bbo_ts_ms,
        bbo_mid,
        fills["current__first_fill_ts_ns"].to_numpy(np.int64),
        max_age_ms=max_bbo_age_ms,
    )
    fills["campaign_terminal_mid"] = terminal_mid
    fills["campaign_terminal_bbo_age_ms"] = terminal_age
    fill_price = (
        pd.to_numeric(fills["current__price_tick"], errors="coerce") * TICK_SIZE
    )
    inventory = pd.to_numeric(fills["inventory"], errors="coerce")
    decision_mid = pd.to_numeric(fills["mid"], errors="coerce")
    pnl_so_far = pd.to_numeric(fills["campaign_pnl_so_far"], errors="coerce")
    quantity = pd.to_numeric(fills["current__fill_qty"], errors="coerce")
    signed_edge = _maker_signed_value(
        fills["side"], quantity, fill_price, fills["campaign_terminal_mid"]
    )
    fills["baseline_campaign_terminal_pnl_usdc"] = (
        pnl_so_far
        + inventory * (fills["campaign_terminal_mid"] - decision_mid)
        + signed_edge
    )
    fills.rename(
        columns={"current__first_fill_ts_ns": "campaign_terminal_ts_ns"},
        inplace=True,
    )
    return fills.loc[
        :,
        [
            "campaign_id",
            "campaign_terminal_ts_ns",
            "campaign_terminal_mid",
            "baseline_campaign_terminal_pnl_usdc",
            "campaign_terminal_bbo_age_ms",
        ],
    ].reset_index(drop=True)


def _value_actions(
    actions: pd.DataFrame,
    cohort_context: pd.DataFrame,
    campaign_terminals: pd.DataFrame,
    *,
    bbo_ts_ms: np.ndarray,
    bbo_mid: np.ndarray,
    max_bbo_age_ms: float,
    common_clock_ms: int,
) -> pd.DataFrame:
    valued = actions.merge(cohort_context, on="cohort_id", how="left", validate="many_to_one")
    valued = valued.merge(
        campaign_terminals,
        left_on="mapped_campaign_id",
        right_on="campaign_id",
        how="left",
        suffixes=("", "_terminal"),
        validate="many_to_one",
    )
    valued["fill_price"] = pd.to_numeric(valued["price_tick"], errors="coerce") * TICK_SIZE
    valued["filled"] = (
        valued["activation_status"].astype(str).eq("active")
        & valued["first_fill_ts_ns"].gt(0)
        & valued["fill_qty"].gt(0)
    )
    valued["pending_fill"] = (
        valued["filled"]
        & valued["cancel_request_ts_ns"].gt(0)
        & valued["cancel_ack_ts_ns"].gt(valued["cancel_request_ts_ns"])
        & valued["first_fill_ts_ns"].ge(valued["cancel_request_ts_ns"])
        & valued["first_fill_ts_ns"].lt(valued["cancel_ack_ts_ns"])
    )
    valued["pending_fill_qty"] = np.where(
        valued["pending_fill"], valued["fill_qty"], 0.0
    )
    for horizon_ms in MARKOUT_HORIZONS_MS:
        targets = valued["first_fill_ts_ns"].to_numpy(np.int64) + int(horizon_ms) * 1_000_000
        future_mid, age = _asof_mid(
            bbo_ts_ms,
            bbo_mid,
            targets,
            max_age_ms=max_bbo_age_ms,
        )
        future_mid[~valued["filled"].to_numpy(bool)] = np.nan
        valued[f"future_mid_{horizon_ms}ms"] = future_mid
        valued[f"future_mid_age_{horizon_ms}ms"] = age
        action_value = _maker_signed_value(
            valued["side"],
            valued["fill_qty"],
            valued["fill_price"],
            valued[f"future_mid_{horizon_ms}ms"],
        )
        valued[f"value_{horizon_ms}ms_usdc"] = np.where(
            valued["filled"], action_value, 0.0
        )
    common_target = (
        valued["activation_ts_ns"].to_numpy(np.int64)
        + int(common_clock_ms) * 1_000_000
    )
    common_mid, common_age = _asof_mid(
        bbo_ts_ms,
        bbo_mid,
        common_target,
        max_age_ms=max_bbo_age_ms,
    )
    filled_by_clock = valued["filled"] & valued["first_fill_ts_ns"].le(common_target)
    common_value = _maker_signed_value(
        valued["side"],
        valued["fill_qty"],
        valued["fill_price"],
        pd.Series(common_mid, index=valued.index),
    )
    valued["filled_by_common_clock"] = filled_by_clock.astype(np.int8)
    valued["common_clock_mid_age_ms"] = common_age
    valued["value_common_clock_usdc"] = np.where(
        filled_by_clock, common_value, 0.0
    )
    terminal_supported = (
        valued["filled"]
        & valued["campaign_terminal_ts_ns"].notna()
        & valued["campaign_terminal_mid"].notna()
        & valued["first_fill_ts_ns"].le(valued["campaign_terminal_ts_ns"])
    )
    terminal_value = _maker_signed_value(
        valued["side"],
        valued["fill_qty"],
        valued["fill_price"],
        valued["campaign_terminal_mid"],
    )
    valued["campaign_terminal_overlay_usdc"] = np.where(
        ~valued["filled"],
        0.0,
        np.where(terminal_supported, terminal_value, np.nan),
    )
    valued["campaign_terminal_overlay_supported"] = (
        ~valued["filled"] | terminal_supported
    ).astype(np.int8)
    return valued


def build_pair_rows(
    valued_actions: pd.DataFrame,
    *,
    tail_threshold_usdc: float,
    fresh_book_max_age_ms: float,
) -> pd.DataFrame:
    if valued_actions.duplicated(["cohort_id", "action"]).any():
        raise ValueError("valued action rows are not unique")
    valued_actions = _common_activation_support(valued_actions)
    action_index = valued_actions.set_index(["cohort_id", "action"])
    cohort_context = valued_actions.drop_duplicates("cohort_id").set_index("cohort_id")
    current = action_index.xs("current", level="action")
    rows: list[pd.DataFrame] = []
    for gap in ACTION_GAPS:
        for contrast in CONTRASTS:
            shallow_name, deep_name = _pair_actions(gap, contrast)
            shallow = action_index.xs(shallow_name, level="action")
            deep = action_index.xs(deep_name, level="action")
            if not shallow.index.equals(deep.index):
                raise RuntimeError("paired action cohort identity changed")
            pair = pd.DataFrame(index=shallow.index)
            pair["cohort_id"] = pair.index.astype(str)
            for column in (
                "day",
                "side",
                "inventory_role",
                "mapped_campaign_id",
                "campaign_mapping_source",
                "cancel_request_reason",
                "request_model_risk_set",
                "request_valid_book",
                "request_book_age_ms",
                "baseline_campaign_terminal_pnl_usdc",
            ):
                pair[column] = cohort_context.loc[pair.index, column].to_numpy()
            pair["gap_ticks"] = int(gap)
            pair["contrast"] = str(contrast)
            pair["shallow_action"] = shallow_name
            pair["deep_action"] = deep_name
            pair["shallow_fill"] = shallow["filled"].astype(np.int8).to_numpy()
            pair["deep_fill"] = deep["filled"].astype(np.int8).to_numpy()
            if pair["deep_fill"].gt(pair["shallow_fill"]).any():
                raise RuntimeError("full-lifecycle paired fill monotonicity failed")
            pair["shared_fill"] = pair["deep_fill"]
            pair["marginal_shallower_fill"] = pair["shallow_fill"] - pair["deep_fill"]
            pair["neither_fill"] = 1 - pair["shallow_fill"]
            pair["fill_affected"] = pair["shallow_fill"].astype(np.int8)
            pair["fill_probability_delta_deeper_minus_shallower"] = (
                pair["deep_fill"] - pair["shallow_fill"]
            )
            pair["filled_quantity_delta_btc_deeper_minus_shallower"] = (
                pd.to_numeric(deep["fill_qty"], errors="coerce").to_numpy(float)
                - pd.to_numeric(shallow["fill_qty"], errors="coerce").to_numpy(float)
            )
            shared_qty = np.minimum(
                pd.to_numeric(shallow["fill_qty"], errors="coerce").to_numpy(float),
                pd.to_numeric(deep["fill_qty"], errors="coerce").to_numpy(float),
            )
            pair["shared_execution_improvement_usdc"] = (
                shared_qty * float(gap) * TICK_SIZE
            )
            for horizon_ms, label in ((1_000, "1s"), (5_000, "5s"), (30_000, "30s")):
                shallow_value = pd.to_numeric(
                    shallow[f"value_{horizon_ms}ms_usdc"], errors="coerce"
                ).to_numpy(float)
                deep_value = pd.to_numeric(
                    deep[f"value_{horizon_ms}ms_usdc"], errors="coerce"
                ).to_numpy(float)
                marginal = np.where(
                    pair["marginal_shallower_fill"].to_numpy(bool),
                    shallow_value,
                    0.0,
                )
                shared_delta = np.where(
                    pair["shared_fill"].to_numpy(bool),
                    deep_value - shallow_value,
                    0.0,
                )
                total_delta = deep_value - shallow_value
                pair[f"marginal_shallower_value_{label}_usdc"] = marginal
                pair[f"shared_value_delta_{label}_usdc"] = shared_delta
                pair[f"total_value_delta_{label}_usdc"] = total_delta
                finite = np.isfinite(marginal) & np.isfinite(shared_delta) & np.isfinite(total_delta)
                if not np.allclose(
                    total_delta[finite],
                    shared_delta[finite] - marginal[finite],
                    rtol=0.0,
                    atol=1e-12,
                ):
                    raise RuntimeError("marginal/shared value decomposition failed")
            pair["total_value_delta_common_clock_usdc"] = (
                pd.to_numeric(deep["value_common_clock_usdc"], errors="coerce").to_numpy(float)
                - pd.to_numeric(shallow["value_common_clock_usdc"], errors="coerce").to_numpy(float)
            )
            shallow_terminal = pd.to_numeric(
                shallow["campaign_terminal_overlay_usdc"], errors="coerce"
            ).to_numpy(float)
            deep_terminal = pd.to_numeric(
                deep["campaign_terminal_overlay_usdc"], errors="coerce"
            ).to_numpy(float)
            current_terminal = pd.to_numeric(
                current.loc[pair.index, "campaign_terminal_overlay_usdc"], errors="coerce"
            ).to_numpy(float)
            pair["campaign_terminal_overlay_delta_usdc"] = deep_terminal - shallow_terminal
            campaign_supported = np.isfinite(shallow_terminal) & np.isfinite(deep_terminal)
            pair["campaign_overlay_supported"] = campaign_supported.astype(np.int8)
            shallow_adjusted = (
                pd.to_numeric(pair["baseline_campaign_terminal_pnl_usdc"], errors="coerce").to_numpy(float)
                + shallow_terminal
                - current_terminal
            )
            deep_adjusted = (
                pd.to_numeric(pair["baseline_campaign_terminal_pnl_usdc"], errors="coerce").to_numpy(float)
                + deep_terminal
                - current_terminal
            )
            tail_supported = (
                campaign_supported
                & np.isfinite(current_terminal)
                & np.isfinite(shallow_adjusted)
                & np.isfinite(deep_adjusted)
            )
            tail_delta = np.full(len(pair), np.nan, dtype=float)
            tail_delta[tail_supported] = (
                (deep_adjusted[tail_supported] <= float(tail_threshold_usdc)).astype(int)
                - (shallow_adjusted[tail_supported] <= float(tail_threshold_usdc)).astype(int)
            )
            no_fill = pair["fill_affected"].eq(0).to_numpy(bool)
            tail_delta[no_fill] = 0.0
            pair["campaign_tail_event_delta"] = tail_delta
            pair["campaign_tail_supported"] = (tail_supported | no_fill).astype(np.int8)
            shallow_pending = shallow["pending_fill"].astype(np.int8).to_numpy()
            deep_pending = deep["pending_fill"].astype(np.int8).to_numpy()
            pair["pending_fill_probability_delta"] = deep_pending - shallow_pending
            pair["pending_filled_quantity_delta_btc"] = (
                pd.to_numeric(deep["pending_fill_qty"], errors="coerce").to_numpy(float)
                - pd.to_numeric(shallow["pending_fill_qty"], errors="coerce").to_numpy(float)
            )
            shallow_30s = pd.to_numeric(
                shallow["value_30000ms_usdc"], errors="coerce"
            ).to_numpy(float)
            deep_30s = pd.to_numeric(
                deep["value_30000ms_usdc"], errors="coerce"
            ).to_numpy(float)
            pair["pending_value_delta_30s_usdc"] = (
                np.where(deep_pending.astype(bool), deep_30s, 0.0)
                - np.where(shallow_pending.astype(bool), shallow_30s, 0.0)
            )
            pair["pending_campaign_overlay_delta_usdc"] = (
                np.where(deep_pending.astype(bool), deep_terminal, 0.0)
                - np.where(shallow_pending.astype(bool), shallow_terminal, 0.0)
            )
            pair["pending_fill_any"] = (
                shallow_pending.astype(bool) | deep_pending.astype(bool)
            ).astype(np.int8)
            pair["request_scope"] = "all"
            pair["request_reason_group"] = _request_reason_group(
                pair["cancel_request_reason"]
            )
            pair["campaign_cluster_id"] = np.where(
                pd.to_numeric(pair["mapped_campaign_id"], errors="coerce").fillna(0).gt(0),
                pair["day"].astype(str)
                + ":"
                + pair["side"].astype(str)
                + ":campaign:"
                + pd.to_numeric(pair["mapped_campaign_id"], errors="coerce").fillna(0).astype(int).astype(str),
                pair["day"].astype(str) + ":cohort:" + pair["cohort_id"].astype(str),
            )
            rows.append(pair.reset_index(drop=True))
    out = pd.concat(rows, ignore_index=True)
    fresh_sell = out.loc[
        out["side"].astype(str).eq("SELL")
        & pd.to_numeric(out["request_model_risk_set"], errors="coerce").eq(1)
        & pd.to_numeric(out["request_valid_book"], errors="coerce").eq(1)
        & pd.to_numeric(out["request_book_age_ms"], errors="coerce").le(
            float(fresh_book_max_age_ms)
        )
    ].copy()
    fresh_sell["request_scope"] = "fresh_book_sell"
    return pd.concat([out, fresh_sell], ignore_index=True)


def _request_reason_group(values: pd.Series) -> pd.Series:
    text = values.fillna("").astype(str)
    return pd.Series(
        np.select(
            [
                text.eq("requote_replace"),
                text.eq("stale_book"),
                text.eq(""),
            ],
            ["requote_replace", "stale_book", "no_request"],
            default="other_policy",
        ),
        index=values.index,
        dtype="object",
    )


def aggregate_campaign_clusters(pair_rows: pd.DataFrame) -> pd.DataFrame:
    working = pair_rows.copy()
    working["decisions"] = 1
    working["shared_fill_decisions"] = working["shared_fill"]
    working["marginal_fill_decisions"] = working["marginal_shallower_fill"]
    working["neither_fill_decisions"] = working["neither_fill"]
    working["fill_affected_decisions"] = working["fill_affected"]
    working["campaign_overlay_supported_affected_decisions"] = (
        working["fill_affected"] * working["campaign_overlay_supported"]
    )
    working["campaign_tail_supported_affected_decisions"] = (
        working["fill_affected"] * working["campaign_tail_supported"]
    )
    working["pending_fill_decisions"] = working["pending_fill_any"]
    for metric in VALUE_METRICS:
        numeric = pd.to_numeric(working[metric], errors="coerce")
        working[f"sum__{metric}"] = numeric.fillna(0.0)
        working[f"count__{metric}"] = numeric.notna().astype(np.int64)
    group_columns = ["day", *CELL_COLUMNS, "campaign_cluster_id"]
    sum_columns = [
        *COUNT_COLUMNS,
        *(f"sum__{name}" for name in VALUE_METRICS),
        *(f"count__{name}" for name in VALUE_METRICS),
    ]
    return (
        working.groupby(group_columns, observed=True, sort=True)[sum_columns]
        .sum()
        .reset_index()
    )


def pending_partial_pooling_diagnostic(
    pair_rows: pd.DataFrame,
    *,
    prior_strength: float,
    posterior_probability: float,
) -> pd.DataFrame:
    source = pair_rows.loc[pair_rows["request_scope"].eq("all")].copy()
    parent_columns = ["side", "gap_ticks", "contrast"]
    records: list[dict[str, Any]] = []
    for parent_key, parent in source.groupby(parent_columns, observed=True):
        parent_shallow = int(
            (
                parent["pending_fill_any"].eq(1)
                & parent["pending_fill_probability_delta"].le(0)
            ).sum()
        )
        parent_deep = int(
            (
                parent["pending_fill_any"].eq(1)
                & parent["pending_fill_probability_delta"].ge(0)
            ).sum()
        )
        parent_n = max(int(len(parent)), 1)
        shallow_rate = (parent_shallow + 0.5) / (parent_n + 1.0)
        deep_rate = (parent_deep + 0.5) / (parent_n + 1.0)
        for child_key, child in parent.groupby(
            ["inventory_role", "request_reason_group"], observed=True
        ):
            shallow_events = int(
                (
                    child["pending_fill_any"].eq(1)
                    & child["pending_fill_probability_delta"].le(0)
                ).sum()
            )
            deep_events = int(
                (
                    child["pending_fill_any"].eq(1)
                    & child["pending_fill_probability_delta"].ge(0)
                ).sum()
            )
            trials = int(len(child))
            alpha_s = 0.5 + shallow_events + float(prior_strength) * shallow_rate
            beta_s = 0.5 + trials - shallow_events + float(prior_strength) * (1.0 - shallow_rate)
            alpha_d = 0.5 + deep_events + float(prior_strength) * deep_rate
            beta_d = 0.5 + trials - deep_events + float(prior_strength) * (1.0 - deep_rate)
            tail = (1.0 - float(posterior_probability)) * 0.5
            records.append(
                {
                    "side": str(parent_key[0]),
                    "gap_ticks": int(parent_key[1]),
                    "contrast": str(parent_key[2]),
                    "inventory_role": str(child_key[0]),
                    "request_reason_group": str(child_key[1]),
                    "trials": trials,
                    "shallow_events": shallow_events,
                    "deep_events": deep_events,
                    "shallow_posterior_mean": alpha_s / (alpha_s + beta_s),
                    "shallow_posterior_lower": beta_distribution.ppf(tail, alpha_s, beta_s),
                    "shallow_posterior_upper": beta_distribution.ppf(1.0 - tail, alpha_s, beta_s),
                    "deep_posterior_mean": alpha_d / (alpha_d + beta_d),
                    "deep_posterior_lower": beta_distribution.ppf(tail, alpha_d, beta_d),
                    "deep_posterior_upper": beta_distribution.ppf(1.0 - tail, alpha_d, beta_d),
                    "authority": "partial_pooling_diagnostic_only",
                }
            )
    return pd.DataFrame(records)


def _daily_simultaneous_bands(
    clusters: pd.DataFrame,
    *,
    bootstrap_samples: int,
    bootstrap_seed: int,
    confidence: float,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, float]]:
    sum_columns = [
        *COUNT_COLUMNS,
        *(f"sum__{name}" for name in VALUE_METRICS),
        *(f"count__{name}" for name in VALUE_METRICS),
    ]
    daily = (
        clusters.groupby(["day", *CELL_COLUMNS], observed=True)[sum_columns]
        .sum()
        .reset_index()
    )
    cells = daily.loc[:, list(CELL_COLUMNS)].drop_duplicates().sort_values(
        list(CELL_COLUMNS), kind="stable"
    )
    cells.reset_index(drop=True, inplace=True)
    cell_keys = [tuple(row) for row in cells.itertuples(index=False, name=None)]
    cell_index = {key: index for index, key in enumerate(cell_keys)}
    days = sorted(daily["day"].astype(str).unique())
    day_index = {day: index for index, day in enumerate(days)}
    metric_count = len(VALUE_METRICS)
    numerators = np.zeros((len(days), len(cells), metric_count), dtype=float)
    denominators = np.zeros((len(days), len(cells), metric_count), dtype=float)
    decision_counts = np.zeros((len(days), len(cells)), dtype=float)
    auxiliary = {
        name: np.zeros((len(days), len(cells)), dtype=float)
        for name in COUNT_COLUMNS
    }
    for row in daily.itertuples(index=False):
        day_pos = day_index[str(row.day)]
        key = tuple(getattr(row, column) for column in CELL_COLUMNS)
        key = (str(key[0]), str(key[1]), str(key[2]), int(key[3]), str(key[4]))
        cell_pos = cell_index[key]
        for metric_pos, metric in enumerate(VALUE_METRICS):
            numerators[day_pos, cell_pos, metric_pos] = float(
                getattr(row, f"sum__{metric}")
            )
            denominators[day_pos, cell_pos, metric_pos] = float(
                getattr(row, f"count__{metric}")
            )
        for name in COUNT_COLUMNS:
            auxiliary[name][day_pos, cell_pos] = float(getattr(row, name))
        decision_counts[day_pos, cell_pos] = float(row.decisions)
    total_num = numerators.sum(axis=0)
    total_den = denominators.sum(axis=0)
    point = np.divide(
        total_num,
        total_den,
        out=np.full_like(total_num, np.nan),
        where=total_den > 0,
    )
    rng = np.random.default_rng(int(bootstrap_seed))
    bootstrap = np.full(
        (int(bootstrap_samples), len(cells), metric_count), np.nan, dtype=float
    )
    for start in range(0, int(bootstrap_samples), 100):
        stop = min(int(bootstrap_samples), start + 100)
        sample = rng.integers(0, len(days), size=(stop - start, len(days)))
        sampled_num = numerators[sample].sum(axis=1)
        sampled_den = denominators[sample].sum(axis=1)
        bootstrap[start:stop] = np.divide(
            sampled_num,
            sampled_den,
            out=np.full_like(sampled_num, np.nan),
            where=sampled_den > 0,
        )
    alpha = 1.0 - float(confidence)
    lower = np.full_like(point, np.nan)
    upper = np.full_like(point, np.nan)
    critical: dict[str, float] = {}
    for metric_pos, metric in enumerate(VALUE_METRICS):
        deviation = np.abs(bootstrap[:, :, metric_pos] - point[:, metric_pos])
        max_deviation = np.nanmax(deviation, axis=1)
        radius = float(np.nanquantile(max_deviation, 1.0 - alpha))
        critical[metric] = radius
        lower[:, metric_pos] = point[:, metric_pos] - radius
        upper[:, metric_pos] = point[:, metric_pos] + radius
    records: list[dict[str, Any]] = []
    for cell_pos, key in enumerate(cell_keys):
        record: dict[str, Any] = dict(zip(CELL_COLUMNS, key, strict=True))
        for name in COUNT_COLUMNS:
            record[name] = int(auxiliary[name][:, cell_pos].sum())
        affected = record["fill_affected_decisions"]
        record["campaign_overlay_informative_coverage"] = (
            record["campaign_overlay_supported_affected_decisions"] / affected
            if affected
            else 1.0
        )
        record["campaign_tail_informative_coverage"] = (
            record["campaign_tail_supported_affected_decisions"] / affected
            if affected
            else 1.0
        )
        for metric_pos, metric in enumerate(VALUE_METRICS):
            record[metric] = float(point[cell_pos, metric_pos])
            record[f"{metric}_simultaneous_lower"] = float(lower[cell_pos, metric_pos])
            record[f"{metric}_simultaneous_upper"] = float(upper[cell_pos, metric_pos])
            record[f"{metric}_supported_decisions"] = int(total_den[cell_pos, metric_pos])
        primary_daily = np.divide(
            numerators[:, cell_pos, VALUE_METRICS.index("campaign_terminal_overlay_delta_usdc")],
            denominators[:, cell_pos, VALUE_METRICS.index("campaign_terminal_overlay_delta_usdc")],
            out=np.full(len(days), np.nan),
            where=denominators[:, cell_pos, VALUE_METRICS.index("campaign_terminal_overlay_delta_usdc")] > 0,
        )
        finite = primary_daily[np.isfinite(primary_daily)]
        record["primary_supported_days"] = int(len(finite))
        record["primary_positive_day_fraction"] = float(np.mean(finite > 0.0)) if len(finite) else math.nan
        record["primary_negative_day_fraction"] = float(np.mean(finite < 0.0)) if len(finite) else math.nan
        records.append(record)
    return pd.DataFrame(records), daily, critical


def classify_feasibility(
    cells: pd.DataFrame,
    *,
    economic_epsilon_usdc: float,
    minimum_supported_days: int,
    minimum_informative_campaign_coverage: float,
    minimum_daily_direction_fraction: float,
) -> tuple[pd.DataFrame, str]:
    out = cells.copy()
    primary = "campaign_terminal_overlay_delta_usdc"
    lower = out[f"{primary}_simultaneous_lower"]
    upper = out[f"{primary}_simultaneous_upper"]
    out["value_direction"] = np.select(
        [lower.gt(0.0), upper.lt(0.0)],
        ["deeper", "shallower"],
        default="unresolved",
    )
    out["economic_lcb_abs_usdc"] = np.where(
        lower.gt(0.0), lower, np.where(upper.lt(0.0), -upper, 0.0)
    )
    out["economic_epsilon_gate"] = out["economic_lcb_abs_usdc"].gt(
        float(economic_epsilon_usdc)
    )
    out["campaign_support_gate"] = out[
        "campaign_overlay_informative_coverage"
    ].ge(float(minimum_informative_campaign_coverage))
    out["day_support_gate"] = out["primary_supported_days"].ge(
        int(minimum_supported_days)
    )
    aligned_fraction = np.where(
        out["value_direction"].eq("deeper"),
        out["primary_positive_day_fraction"],
        np.where(
            out["value_direction"].eq("shallower"),
            out["primary_negative_day_fraction"],
            0.0,
        ),
    )
    out["daily_direction_fraction"] = aligned_fraction
    out["daily_direction_gate"] = out["daily_direction_fraction"].ge(
        float(minimum_daily_direction_fraction)
    )
    pending_metric = "pending_campaign_overlay_delta_usdc"
    pending_lower = out[f"{pending_metric}_simultaneous_lower"]
    pending_upper = out[f"{pending_metric}_simultaneous_upper"]
    pending_point = out[pending_metric]
    out["pending_differential_uncertainty_usdc"] = np.maximum(
        (pending_point - pending_lower).abs(),
        (pending_upper - pending_point).abs(),
    )
    out["pending_differential_gate"] = out[
        "pending_differential_uncertainty_usdc"
    ].lt(out["economic_lcb_abs_usdc"])
    tail_lower = out["campaign_tail_event_delta_simultaneous_lower"]
    tail_upper = out["campaign_tail_event_delta_simultaneous_upper"]
    out["campaign_tail_gate"] = np.where(
        out["value_direction"].eq("deeper"),
        tail_upper.le(0.0),
        np.where(out["value_direction"].eq("shallower"), tail_lower.ge(0.0), False),
    )
    out["formal_gap"] = out["gap_ticks"].isin(FORMAL_GAPS)
    out["negative_control"] = out["gap_ticks"].eq(1)
    out["feasibility_supported"] = (
        out["request_scope"].eq("all")
        & out["formal_gap"]
        & out["economic_epsilon_gate"]
        & out["campaign_support_gate"]
        & out["day_support_gate"]
        & out["daily_direction_gate"]
        & out["pending_differential_gate"]
        & out["campaign_tail_gate"]
    )
    decision = (
        "marginal_fill_value_feasible_register_value_identity"
        if bool(out["feasibility_supported"].any())
        else "close_placement_distance_value_path_development"
    )
    return out, decision


def _load_spec(spec_path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    path = spec_path.expanduser().resolve()
    spec = json.loads(path.read_text(encoding="utf-8"))
    if spec.get("schema_version") != "placement_marginal_fill_value_feasibility_spec.v1":
        raise RuntimeError("unsupported marginal-fill feasibility Spec")
    permissions = spec["permissions"]
    if permissions["validation_read"] or permissions["sealed_holdout_read"]:
        raise RuntimeError("marginal-fill feasibility must keep later panels sealed")
    implementation = ROOT / str(spec["implementation"]["path"])
    if file_sha256(implementation) != str(spec["implementation"]["sha256"]):
        raise RuntimeError("marginal-fill implementation differs from frozen Spec")
    identities = {
        "spec": _identity(path),
        "implementation": _identity(implementation),
    }
    for label, identity in spec["source_identity"].items():
        identities[label] = _identity(_require_identity(identity, label))
    return spec, identities


def _request_state_by_day(index_path: Path) -> dict[str, dict[str, Any]]:
    index = pd.read_csv(index_path, dtype={"day": str})
    required = {"day", "payload_path", "payload_sha256"}
    missing = required - set(index.columns)
    if missing:
        raise RuntimeError(f"request-state index missing columns: {sorted(missing)}")
    return {
        str(row.day): {
            "path": str(Path(row.payload_path).expanduser().resolve()),
            "sha256": str(row.payload_sha256),
        }
        for row in index.itertuples(index=False)
    }


def _select_current_request_state(request: pd.DataFrame) -> pd.DataFrame:
    current = request.loc[request["action"].astype(str).eq("current")].copy()
    if current["cohort_id"].duplicated().any():
        raise RuntimeError("current request-state rows are not unique")
    current.drop(columns=["action"], inplace=True)
    current.rename(
        columns={"cancel_request_reason": "request_cancel_reason"}, inplace=True
    )
    return current


def _build_day(
    day: str,
    *,
    spec: Mapping[str, Any],
    placement_root: Path,
    resolution_root: Path,
    request_state_identity: Mapping[str, Any],
    bbo_identity: Mapping[str, Any],
    output_dir: Path,
) -> dict[str, Any]:
    source_dir = placement_root / "partitions" / f"day={day}"
    source_manifest_path = source_dir / "manifest.json"
    source_path = source_dir / "placement.parquet"
    source_manifest = json.loads(source_manifest_path.read_text(encoding="utf-8"))
    if file_sha256(source_path) != str(source_manifest["panel_sha256"]):
        raise RuntimeError(f"{day} placement source identity changed")
    resolution_manifest_path = resolution_root / "partitions" / f"day={day}" / "manifest.json"
    resolution_manifest = json.loads(resolution_manifest_path.read_text(encoding="utf-8"))
    mechanics = resolution_manifest["mechanics_cache"]
    mechanics_path = Path(str(mechanics["payload_path"])).expanduser().resolve()
    if file_sha256(mechanics_path) != str(mechanics["payload_sha256"]):
        raise RuntimeError(f"{day} paired mechanics identity changed")
    request_path = Path(str(request_state_identity["path"])).expanduser().resolve()
    if file_sha256(request_path) != str(request_state_identity["sha256"]):
        raise RuntimeError(f"{day} request-state identity changed")
    bbo_path = Path(str(bbo_identity["path"])).expanduser().resolve()
    bbo_ts, bbo_mid = _load_bbo(bbo_path, expected_sha256=str(bbo_identity["sha256"]))
    source_columns = [
        "cohort_id",
        "day",
        "side",
        "inventory_role",
        "campaign_id",
        "submit_ts_ns",
        "campaign_age_s",
        "inventory",
        "mid",
        "campaign_pnl_so_far",
        "cancel_request_reason",
        "current__first_fill_ts_ns",
        "current__fill_qty",
        "current__price_tick",
    ]
    source = pd.read_parquet(source_path, columns=source_columns)
    actions = pd.read_parquet(mechanics_path)
    request = pd.read_parquet(
        request_path,
        columns=[
            "cohort_id",
            "action",
            "request_model_risk_set",
            "request_valid_book",
            "request_book_age_ms",
            "cancel_request_reason",
        ],
    )
    request = _select_current_request_state(request)
    starts = _campaign_start_table(source)
    mapping = _map_cohorts_to_campaigns(
        source,
        starts,
        opener_tolerance_ms=float(spec["campaign_overlay"]["opener_start_match_tolerance_ms"]),
    )
    terminal = reconstruct_campaign_terminals(
        source,
        bbo_ts_ms=bbo_ts,
        bbo_mid=bbo_mid,
        max_bbo_age_ms=float(spec["market_marks"]["max_bbo_age_ms"]),
    )
    context = source.loc[:, ["cohort_id", "cancel_request_reason"]].merge(
        mapping,
        on="cohort_id",
        validate="one_to_one",
    )
    context = context.merge(
        request,
        on="cohort_id",
        how="left",
        validate="one_to_one",
    )
    context["cancel_request_reason"] = context["request_cancel_reason"].fillna(
        context["cancel_request_reason"]
    )
    context.drop(columns=["request_cancel_reason"], inplace=True)
    valued = _value_actions(
        actions,
        context,
        terminal,
        bbo_ts_ms=bbo_ts,
        bbo_mid=bbo_mid,
        max_bbo_age_ms=float(spec["market_marks"]["max_bbo_age_ms"]),
        common_clock_ms=int(spec["common_clock"]["clock_ms"]),
    )
    pairs = build_pair_rows(
        valued,
        tail_threshold_usdc=float(spec["economic_gates"]["tail_threshold_usdc"]),
        fresh_book_max_age_ms=float(
            spec["pending_differential"]["fresh_book_sell_max_age_ms"]
        ),
    )
    clusters = aggregate_campaign_clusters(pairs)
    pending = pending_partial_pooling_diagnostic(
        pairs,
        prior_strength=float(spec["pending_differential"]["partial_pooling_prior_strength"]),
        posterior_probability=float(spec["pending_differential"]["posterior_probability"]),
    )
    final_dir = output_dir / "partitions" / f"day={day}"
    final_dir.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f"marginal-{day}.", dir=final_dir.parent))
    try:
        cluster_path = stage / "campaign_clusters.parquet"
        pending_path = stage / "pending_partial_pooling.parquet"
        clusters.to_parquet(cluster_path, index=False, compression="zstd")
        pending.to_parquet(pending_path, index=False, compression="zstd")
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "day": day,
            "source_partition": _identity(source_manifest_path),
            "source_panel": _identity(source_path),
            "paired_mechanics": _identity(mechanics_path),
            "request_state": _identity(request_path),
            "bbo": _identity(bbo_path),
            "campaign_terminal_rows": int(len(terminal)),
            "mapped_campaign_cohorts": int(mapping["mapped_campaign_id"].gt(0).sum()),
            "pair_rows": int(len(pairs)),
            "campaign_clusters": _identity(cluster_path),
            "pending_partial_pooling": _identity(pending_path),
        }
        for key in ("campaign_clusters", "pending_partial_pooling"):
            manifest[key]["path"] = Path(manifest[key]["path"]).name
        _atomic_json(manifest, stage / "manifest.json")
        (stage / "COMPLETE").write_text(
            canonical_sha256(manifest) + "\n", encoding="ascii"
        )
        if final_dir.exists():
            shutil.rmtree(final_dir)
        stage.replace(final_dir)
    finally:
        if stage.exists():
            shutil.rmtree(stage)
    return json.loads((final_dir / "manifest.json").read_text(encoding="utf-8"))


def _storage_preflight(output_dir: Path, estimated_final_gib: float) -> dict[str, Any]:
    usage = shutil.disk_usage(output_dir.parent)
    free_gib = usage.free / 1024**3
    required_gib = 60.0 + 2.5 * float(estimated_final_gib)
    if free_gib < required_gib:
        raise RuntimeError(
            f"storage gate failed: free={free_gib:.2f}GiB required={required_gib:.2f}GiB"
        )
    return {
        "free_gib": float(free_gib),
        "estimated_final_gib": float(estimated_final_gib),
        "required_gib": float(required_gib),
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    spec, identities = _load_spec(args.spec)
    expected_days = list(map(str, spec["panels"]["development_days"]))
    placement_root = Path(spec["paths"]["placement_root"]).expanduser().resolve()
    resolution_root = Path(spec["paths"]["paired_resolution_root"]).expanduser().resolve()
    bbo_root = Path(spec["paths"]["bbo_root"]).expanduser().resolve()
    request_index_path = Path(spec["paths"]["request_state_index"]).expanduser().resolve()
    request_by_day = _request_state_by_day(request_index_path)
    if sorted(request_by_day) != sorted(expected_days):
        raise RuntimeError("request-state Development days differ from frozen Spec")
    registry_path = _require_identity(
        spec["source_identity"]["bbo_registry_manifest"], "bbo_registry_manifest"
    )
    bbo_registry = json.loads(registry_path.read_text(encoding="utf-8"))
    bbo_by_day = _bbo_identity_by_day(
        bbo_registry, bbo_root=bbo_root, days=expected_days
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    storage = _storage_preflight(
        args.output_dir, float(spec["storage_gate"]["estimated_final_gib"])
    )
    preflight = {
        "schema_version": SCHEMA_VERSION,
        "family_id": spec["family_id"],
        "days": expected_days,
        "identities": identities,
        "storage": storage,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_experiment_authorized": False,
        "live_deployment_authorized": False,
    }
    _atomic_json(preflight, args.output_dir / "preflight_manifest.json")
    manifests = []
    for index, day in enumerate(expected_days, 1):
        print(f"[{index:02d}/{len(expected_days):02d}] {day}", flush=True)
        manifests.append(
            _build_day(
                day,
                spec=spec,
                placement_root=placement_root,
                resolution_root=resolution_root,
                request_state_identity=request_by_day[day],
                bbo_identity=bbo_by_day[day],
                output_dir=args.output_dir,
            )
        )
    cluster_parts = []
    pending_parts = []
    for day in expected_days:
        partition = args.output_dir / "partitions" / f"day={day}"
        manifest = json.loads((partition / "manifest.json").read_text(encoding="utf-8"))
        for key, name in (
            ("campaign_clusters", "campaign_clusters.parquet"),
            ("pending_partial_pooling", "pending_partial_pooling.parquet"),
        ):
            path = partition / name
            if file_sha256(path) != str(manifest[key]["sha256"]):
                raise RuntimeError(f"{day} {key} identity changed")
        cluster_parts.append(pd.read_parquet(partition / "campaign_clusters.parquet"))
        pending_parts.append(pd.read_parquet(partition / "pending_partial_pooling.parquet"))
    clusters = pd.concat(cluster_parts, ignore_index=True)
    pending = pd.concat(pending_parts, ignore_index=True)
    cells, daily, critical = _daily_simultaneous_bands(
        clusters,
        bootstrap_samples=int(spec["inference"]["bootstrap_samples"]),
        bootstrap_seed=int(spec["inference"]["bootstrap_seed"]),
        confidence=float(spec["inference"]["simultaneous_confidence"]),
    )
    cells, decision = classify_feasibility(
        cells,
        economic_epsilon_usdc=float(spec["economic_gates"]["epsilon_usdc_per_decision"]),
        minimum_supported_days=int(spec["economic_gates"]["minimum_supported_days"]),
        minimum_informative_campaign_coverage=float(
            spec["economic_gates"]["minimum_informative_campaign_coverage"]
        ),
        minimum_daily_direction_fraction=float(
            spec["economic_gates"]["minimum_daily_direction_fraction"]
        ),
    )
    cells_path = args.output_dir / "cell_metrics.csv"
    daily_path = args.output_dir / "daily_metrics.csv"
    pending_path = args.output_dir / "pending_partial_pooling_diagnostic.csv"
    cells.to_csv(cells_path, index=False)
    daily.to_csv(daily_path, index=False)
    pending.to_csv(pending_path, index=False)
    formal = cells.loc[cells["request_scope"].eq("all") & cells["formal_gap"]]
    negative = cells.loc[cells["request_scope"].eq("all") & cells["negative_control"]]
    report = {
        "schema_version": SCHEMA_VERSION,
        "family_id": spec["family_id"],
        "spec": identities["spec"],
        "development_days": expected_days,
        "development_day_count": len(expected_days),
        "research_boundary": {
            "trains_fill_model": False,
            "principal_stratum_is_diagnostic_only": True,
            "campaign_value_is_no_policy_feedback_overlay": True,
            "validation_read": False,
            "sealed_holdout_read": False,
            "value_identity_created": False,
            "action_identity_created": False,
            "action_experiment_authorized": False,
            "live_deployment_authorized": False,
        },
        "mechanics": {
            "pair_rows": int(sum(row["pair_rows"] for row in manifests)),
            "campaign_terminal_rows": int(
                sum(row["campaign_terminal_rows"] for row in manifests)
            ),
            "mapped_campaign_cohorts": int(
                sum(row["mapped_campaign_cohorts"] for row in manifests)
            ),
            "full_lifecycle_monotonicity_violations": 0,
        },
        "formal_cells": int(len(formal)),
        "formal_feasibility_cells": int(formal["feasibility_supported"].sum()),
        "negative_control_cells": int(len(negative)),
        "negative_control_feasibility_cells": int(
            negative["feasibility_supported"].sum()
        ),
        "simultaneous_critical_radii": critical,
        "decision": decision,
        "permissions": {
            "prediction_supported": False,
            "transport_supported": False,
            "economic_resolution_supported": bool(
                formal["feasibility_supported"].any()
            ),
            "action_uplift_supported": False,
            "action_experiment_authorized": False,
            "live_deployment_authorized": False,
        },
        "outputs": {
            "cell_metrics": _identity(cells_path),
            "daily_metrics": _identity(daily_path),
            "pending_partial_pooling_diagnostic": _identity(pending_path),
        },
        "input_identities": identities,
    }
    _atomic_json(report, args.output_dir / "report.json")
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    args.spec = args.spec.expanduser().resolve()
    args.output_dir = args.output_dir.expanduser().resolve()
    report = run(args)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
