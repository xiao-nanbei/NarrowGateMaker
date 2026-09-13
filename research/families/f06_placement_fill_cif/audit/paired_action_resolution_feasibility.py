#!/usr/bin/env python3
"""Audit whether raw paired placement paths resolve 1/2/4 tick actions.

This Development-only family trains no prediction model. It reuses the
hash-frozen baseline placement cohorts, expands each cohort to seven prices,
and runs one sparse native-book/C++ lifecycle replay per UTC day. Outcomes are
evaluated on one config-derived clock and never authorize Value, Action, or
Live identities.
"""

from __future__ import annotations

import argparse
import json
import math
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from data.build_active_order_queue_tape import build_active_order_queue_tape
from data_paths import data_root, marketdata_root, window_cache_root
from models import backtest_tick as bt
from models.audit.content_addressed_cache import (
    DirectoryContentAddressedCache,
    ParquetContentAddressedCache,
    canonical_sha256,
    file_sha256,
)
from research.families.f06_placement_fill_cif import FAMILY_DOCS
from research.families.f06_placement_fill_cif.audit import sparse_order_lifecycle as sparse
from research.families.f06_placement_fill_cif.audit.paired_lifecycle_contract import (
    assert_common_prediction_clock,
    prediction_clock_contract_from_spec,
    verify_prediction_clock_source_identity,
)
from research.families.f06_placement_fill_cif.audit.paired_order_lifecycle import (
    PlacementChild,
)

ROOT = Path(__file__).resolve().parents[4]
DATA_ROOT = data_root(ROOT)
DEFAULT_SPEC = (
    FAMILY_DOCS / "paired_action_resolution_feasibility_v1_spec_20260728.json"
)
DEFAULT_OUTPUT = (
    DATA_ROOT
    / "reports"
    / "paired_action_resolution_feasibility_v1_development_20260728"
)
DEFAULT_SOURCE_PANEL = (
    DATA_ROOT
    / "reports"
    / "placement_fill_request_state_race_v2_development_20260728_v3"
    / "placement"
)
DEFAULT_RAW_BOOK = marketdata_root() / "cryptohftdata"
DEFAULT_SPARSE_CACHE = (
    window_cache_root(ROOT) / "paired_action_resolution_sparse_tape_v1"
)
DEFAULT_MECHANICS_CACHE = (
    window_cache_root(ROOT) / "paired_action_resolution_mechanics_v1"
)

SCHEMA_VERSION = "paired_action_resolution_feasibility.v1"
ACTION_OFFSETS = {
    "closer_4tick": -4,
    "closer_2tick": -2,
    "closer_1tick": -1,
    "current": 0,
    "farther_1tick": 1,
    "farther_2tick": 2,
    "farther_4tick": 4,
}
ACTION_ORDER = tuple(ACTION_OFFSETS)
ACTION_GAPS = (1, 2, 4)
CONTRAST_DIRECTIONS = ("closer_current", "current_farther", "closer_farther")
LOT_SIZE = 0.001
TICK_SIZE = 0.1


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


def _native_module_identity() -> dict[str, Any]:
    import narrowgate_cpp  # type: ignore

    if not hasattr(narrowgate_cpp, "simulate_sparse_order_lifecycles"):
        raise RuntimeError("narrowgate_cpp lacks sparse_order_lifecycle.v1")
    identity = _identity(Path(str(narrowgate_cpp.__file__)))
    identity["abi"] = "sparse_order_lifecycle.v1"
    return identity


def _action_name(prefix: str, gap: int) -> str:
    return f"{prefix}_{int(gap)}tick"


def _contrast_actions(gap: int, contrast: str) -> tuple[str, str, int]:
    closer = _action_name("closer", gap)
    farther = _action_name("farther", gap)
    if contrast == "closer_current":
        return closer, "current", int(gap)
    if contrast == "current_farther":
        return "current", farther, int(gap)
    if contrast == "closer_farther":
        return closer, farther, int(2 * gap)
    raise ValueError(f"unsupported contrast={contrast!r}")


@dataclass
class ResolutionCohort:
    cohort_id: str
    day: str
    side: str
    inventory_role: str
    campaign_id: int
    baseline_price_tick: int
    mid: float
    quantity: float
    activate_ts_ns: int
    cancel_request_ts_ns: int
    cancel_ack_ts_ns: int
    observation_end_ts_ns: int
    children: dict[str, PlacementChild]


def _resolution_child(
    *,
    action: str,
    offset: int,
    side: str,
    baseline_price_tick: int,
    quantity: float,
    submit_ts_ns: int,
    activate_ts_ns: int,
    cancel_request_ts_ns: int,
    cancel_ack_ts_ns: int,
    observation_end_ts_ns: int,
    queue_deplete_mult: float,
) -> PlacementChild:
    # PlacementChild belongs to the frozen +/-1 producer. Construct through its
    # neutral action, then assign the new identity without mutating that module.
    child = PlacementChild(
        action="current",
        distance_delta_ticks=int(offset),
        side=side,
        price_tick=int(baseline_price_tick),
        quantity=float(quantity),
        submit_ts_ns=int(submit_ts_ns),
        activate_ts_ns=int(activate_ts_ns),
        cancel_request_ts_ns=int(cancel_request_ts_ns),
        cancel_ack_ts_ns=int(cancel_ack_ts_ns),
        observation_end_ts_ns=int(observation_end_ts_ns),
        queue_deplete_mult=float(queue_deplete_mult),
        lot_size=LOT_SIZE,
    )
    direction = 1 if str(side).upper() == "BUY" else -1
    child.action = str(action)
    child.distance_delta_ticks = int(offset)
    child.price_tick = int(baseline_price_tick) - direction * int(offset)
    return child


def build_resolution_cohorts(source: pd.DataFrame) -> list[ResolutionCohort]:
    required = {
        "cohort_id",
        "day",
        "side",
        "inventory_role",
        "campaign_id",
        "submit_ts_ns",
        "activate_ts_ns",
        "cancel_request_ts_ns",
        "cancel_ack_ts_ns",
        "observation_end_ts_ns",
        "baseline_price_tick",
        "quantity",
        "mid",
        "baseline_queue_deplete_mult",
    }
    missing = sorted(required - set(source.columns))
    if missing:
        raise ValueError(f"source placement panel is missing columns: {missing}")
    if source["cohort_id"].astype(str).duplicated().any():
        raise ValueError("source placement panel has duplicate cohort ids")

    cohorts: list[ResolutionCohort] = []
    for row in source.to_dict("records"):
        quantity = float(row["quantity"])
        if not math.isclose(quantity, LOT_SIZE, rel_tol=0.0, abs_tol=1e-12):
            raise RuntimeError(
                "resolution v1 requires one-lot orders so first fill equals "
                f"filled quantity; cohort={row['cohort_id']} qty={quantity}"
            )
        side = str(row["side"]).upper()
        baseline_tick = int(row["baseline_price_tick"])
        children = {
            action: _resolution_child(
                action=action,
                offset=offset,
                side=side,
                baseline_price_tick=baseline_tick,
                quantity=quantity,
                submit_ts_ns=int(row["submit_ts_ns"]),
                activate_ts_ns=int(row["activate_ts_ns"]),
                cancel_request_ts_ns=int(row["cancel_request_ts_ns"]),
                cancel_ack_ts_ns=int(row["cancel_ack_ts_ns"]),
                observation_end_ts_ns=int(row["observation_end_ts_ns"]),
                queue_deplete_mult=float(row["baseline_queue_deplete_mult"]),
            )
            for action, offset in ACTION_OFFSETS.items()
        }
        cohorts.append(
            ResolutionCohort(
                cohort_id=str(row["cohort_id"]),
                day=str(row["day"]),
                side=side,
                inventory_role=str(row["inventory_role"]),
                campaign_id=int(row["campaign_id"]),
                baseline_price_tick=baseline_tick,
                mid=float(row["mid"]),
                quantity=quantity,
                activate_ts_ns=int(row["activate_ts_ns"]),
                cancel_request_ts_ns=int(row["cancel_request_ts_ns"]),
                cancel_ack_ts_ns=int(row["cancel_ack_ts_ns"]),
                observation_end_ts_ns=int(row["observation_end_ts_ns"]),
                children=children,
            )
        )
    return cohorts


def build_resolution_watch_manifest(
    cohorts: Sequence[ResolutionCohort],
) -> pd.DataFrame:
    if not cohorts:
        raise ValueError("resolution replay requires at least one cohort")
    day = str(cohorts[0].day)
    day_end_ms = int(
        (pd.Timestamp(day, tz="UTC") + pd.Timedelta(days=1)).timestamp() * 1000
    )
    rows: list[dict[str, Any]] = []
    for cohort in cohorts:
        if str(cohort.day) != day:
            raise ValueError("one resolution watch manifest must contain one UTC day")
        for action in ACTION_ORDER:
            child = cohort.children[action]
            activate_ms = int(child.activate_ts_ns // 1_000_000)
            observation_end_ms = int(child.observation_end_ts_ns // 1_000_000)
            cancel_ack_ms = int(child.cancel_ack_ts_ns // 1_000_000)
            terminal_bound = (
                min(observation_end_ms, cancel_ack_ms)
                if cancel_ack_ms > activate_ms
                else observation_end_ms
            )
            rows.append(
                {
                    "day": day,
                    "watch_id": f"{cohort.cohort_id}:{action}",
                    "order_id": f"{cohort.cohort_id}:{action}",
                    "side": str(child.side),
                    "price": float(child.price_tick) * TICK_SIZE,
                    "activate_ts_ms": activate_ms,
                    "stop_ts_ms": min(
                        day_end_ms,
                        max(activate_ms + 1, terminal_bound + 1),
                    ),
                }
            )
    frame = pd.DataFrame(rows)
    if frame["watch_id"].duplicated().any():
        raise RuntimeError("resolution watch ids are not unique")
    return frame


def _simulate_resolution_cohorts(
    cohorts: Sequence[ResolutionCohort],
    *,
    tape_dir: Path,
    trades: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    import narrowgate_cpp  # type: ignore

    if not cohorts:
        raise ValueError("resolution replay requires at least one cohort")
    watch_ids: list[str] = []
    children: list[PlacementChild] = []
    cohort_by_child: list[ResolutionCohort] = []
    for cohort in cohorts:
        for action in ACTION_ORDER:
            watch_ids.append(f"{cohort.cohort_id}:{action}")
            children.append(cohort.children[action])
            cohort_by_child.append(cohort)

    seeds = pd.read_parquet(tape_dir / "seeds.parquet")
    events = pd.read_parquet(tape_dir / "level_events.parquet")
    sparse._required_columns(
        seeds,
        {
            "watch_id",
            "seed_status",
            "seed_reason",
            "seed_qty",
            "seed_asof_ts_ms",
            "segment_id",
            "seed_best_bid_tick",
            "seed_best_ask_tick",
            "ambiguous",
        },
        label="resolution sparse seeds",
    )
    sparse._required_columns(
        events,
        {
            "watch_id",
            "exchange_ts_ms",
            "message_ordinal",
            "qty_after",
            "event_code",
            "state_status",
            "ambiguous",
        },
        label="resolution sparse events",
    )
    sparse._required_columns(
        trades,
        {"transact_time", "price", "quantity", "is_buyer_maker"},
        label="resolution individual trades",
    )
    seed_by_id = seeds.assign(watch_id=seeds["watch_id"].astype(str)).set_index(
        "watch_id"
    )
    if seed_by_id.index.duplicated().any():
        raise RuntimeError("resolution sparse seeds are not unique")
    if set(seed_by_id.index) != set(watch_ids):
        raise RuntimeError("resolution sparse seed identity does not match actions")
    aligned_seeds = seed_by_id.loc[watch_ids]
    unsupported_seed = sorted(
        set(aligned_seeds["seed_status"].astype(str)) - set(sparse._SEED_STATUS)
    )
    if unsupported_seed:
        raise ValueError(f"unsupported sparse seed status: {unsupported_seed}")
    order_index = {watch_id: index for index, watch_id in enumerate(watch_ids)}
    event_order = events["watch_id"].astype(str).map(order_index)
    if event_order.isna().any():
        raise RuntimeError("resolution sparse event belongs to an unknown watch")
    encoded_events = events.assign(_order_index=event_order.astype(np.int64))
    encoded_events.sort_values(
        ["_order_index", "exchange_ts_ms", "message_ordinal"],
        kind="stable",
        inplace=True,
    )
    trade_sort = ["transact_time"]
    if "trade_id" in trades.columns:
        trade_sort.append("trade_id")
    encoded_trades = trades.sort_values(trade_sort, kind="stable")
    trade_ts = pd.to_numeric(
        encoded_trades["transact_time"], errors="raise"
    ).to_numpy(dtype=np.int64, copy=False)
    event_ts = pd.to_numeric(
        encoded_events["exchange_ts_ms"], errors="raise"
    ).to_numpy(dtype=np.int64, copy=False)
    event_ambiguous = (
        encoded_events["ambiguous"].astype(bool).to_numpy(copy=False)
        | sparse._same_ms_trade_mask(event_ts, trade_ts)
    )
    event_codes = encoded_events["event_code"].astype(str).map(
        sparse._EVENT_CODE
    )
    if event_codes.isna().any():
        raise ValueError("resolution sparse tape has an unsupported event code")
    queue_mults = {
        round(float(child.queue_deplete_mult), 12) for child in children
    }
    if len(queue_mults) != 1:
        raise RuntimeError("resolution native ABI requires one queue multiplier")
    queue_mult = float(next(iter(queue_mults)))
    if not math.isfinite(queue_mult) or queue_mult < 0.0:
        raise ValueError("queue depletion multiplier must be finite and non-negative")

    result = narrowgate_cpp.simulate_sparse_order_lifecycles(
        order_side=np.fromiter(
            (1 if child.side == "BUY" else 2 for child in children),
            dtype=np.uint8,
            count=len(children),
        ),
        order_price_tick=np.asarray(
            [child.price_tick for child in children], dtype=np.int64
        ),
        order_quantity=np.asarray(
            [child.quantity for child in children], dtype=np.float64
        ),
        activate_ts_ms=np.asarray(
            [child.activate_ts_ns // 1_000_000 for child in children],
            dtype=np.int64,
        ),
        cancel_request_ts_ms=np.asarray(
            [child.cancel_request_ts_ns // 1_000_000 for child in children],
            dtype=np.int64,
        ),
        cancel_ack_ts_ms=np.asarray(
            [child.cancel_ack_ts_ns // 1_000_000 for child in children],
            dtype=np.int64,
        ),
        stop_ts_ms=np.asarray(
            [child.observation_end_ts_ns // 1_000_000 for child in children],
            dtype=np.int64,
        ),
        seed_status=aligned_seeds["seed_status"]
        .astype(str)
        .map(sparse._SEED_STATUS)
        .to_numpy(dtype=np.uint8),
        seed_qty=pd.to_numeric(aligned_seeds["seed_qty"], errors="coerce")
        .to_numpy(dtype=np.float64),
        seed_best_bid_tick=pd.to_numeric(
            aligned_seeds["seed_best_bid_tick"], errors="coerce"
        )
        .fillna(0)
        .to_numpy(dtype=np.int64),
        seed_best_ask_tick=pd.to_numeric(
            aligned_seeds["seed_best_ask_tick"], errors="coerce"
        )
        .fillna(0)
        .to_numpy(dtype=np.int64),
        seed_ambiguous=aligned_seeds["ambiguous"]
        .astype(bool)
        .to_numpy(dtype=np.uint8),
        event_order_index=encoded_events["_order_index"].to_numpy(
            dtype=np.int64, copy=False
        ),
        event_ts_ms=event_ts,
        event_qty_after=pd.to_numeric(
            encoded_events["qty_after"], errors="coerce"
        ).to_numpy(dtype=np.float64),
        event_code=event_codes.to_numpy(dtype=np.uint8),
        event_state_valid=encoded_events["state_status"]
        .astype(str)
        .isin({"exact", "known_zero"})
        .to_numpy(dtype=np.uint8),
        event_ambiguous=event_ambiguous.astype(np.uint8, copy=False),
        trade_ts_ms=trade_ts,
        trade_price_tick=np.rint(
            pd.to_numeric(encoded_trades["price"], errors="raise").to_numpy(
                dtype=np.float64
            )
            / TICK_SIZE
        ).astype(np.int64),
        trade_qty=pd.to_numeric(
            encoded_trades["quantity"], errors="raise"
        ).to_numpy(dtype=np.float64),
        is_buyer_maker=encoded_trades["is_buyer_maker"]
        .astype(bool)
        .to_numpy(dtype=np.uint8),
        lot_size=LOT_SIZE,
        queue_deplete_mult=queue_mult,
    )
    if str(result.get("schema_version")) != "sparse_order_lifecycle.v1":
        raise RuntimeError("native sparse lifecycle schema changed")
    for index, (child, watch_id) in enumerate(
        zip(children, watch_ids, strict=True)
    ):
        sparse._apply_native_result(child, seed_by_id.loc[watch_id], result, index)

    rows = []
    for cohort, child in zip(cohort_by_child, children, strict=True):
        rows.append(
            {
                "cohort_id": cohort.cohort_id,
                "day": cohort.day,
                "side": cohort.side,
                "inventory_role": cohort.inventory_role,
                "campaign_id": int(cohort.campaign_id),
                "mid": float(cohort.mid),
                "quantity": float(cohort.quantity),
                "baseline_price_tick": int(cohort.baseline_price_tick),
                "action": str(child.action),
                "distance_delta_ticks": int(child.distance_delta_ticks),
                "price_tick": int(child.price_tick),
                "activation_ts_ns": int(child.activate_ts_ns),
                "activation_status": str(child.activation_status),
                "first_fill_ts_ns": int(child.first_fill_ts_ns),
                "fill_qty": float(child.fill_qty),
                "cancel_request_ts_ns": int(child.cancel_request_ts_ns),
                "cancel_ack_ts_ns": int(child.cancel_ack_ts_ns),
                "terminal_reason": str(child.terminal_reason),
            }
        )
    return pd.DataFrame(rows), {
        "cohorts": int(len(cohorts)),
        "children": int(len(children)),
        "level_events": int(len(encoded_events)),
        "trade_rows": int(len(encoded_trades)),
        "native_schema_version": str(result["schema_version"]),
    }


def _parity_audit(actions: pd.DataFrame, source: pd.DataFrame) -> dict[str, Any]:
    checked = ("closer_1tick", "current", "farther_1tick")
    source_by_cohort = source.set_index("cohort_id")
    mismatches = {"activation_status": 0, "first_fill_ts_ns": 0, "fill_qty": 0}
    rows = 0
    for action in checked:
        current = actions.loc[actions["action"].eq(action)].set_index("cohort_id")
        expected = source_by_cohort.loc[current.index]
        rows += len(current)
        mismatches["activation_status"] += int(
            (
                current["activation_status"].astype(str)
                != expected[f"{action}__activation_status"].astype(str)
            ).sum()
        )
        mismatches["first_fill_ts_ns"] += int(
            (
                current["first_fill_ts_ns"].astype(np.int64)
                != expected[f"{action}__first_fill_ts_ns"].astype(np.int64)
            ).sum()
        )
        mismatches["fill_qty"] += int(
            (
                np.abs(
                    current["fill_qty"].to_numpy(float)
                    - expected[f"{action}__fill_qty"].to_numpy(float)
                )
                > 1e-12
            ).sum()
        )
    return {
        "rows": int(rows),
        "mismatches": mismatches,
        "passed": bool(not any(mismatches.values())),
    }


def build_contrast_rows(
    actions: pd.DataFrame,
    *,
    clock_ms: int,
    stress_fill_value_bps: float,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    required_actions = set(ACTION_ORDER)
    observed_actions = set(actions["action"].astype(str).unique())
    if observed_actions != required_actions:
        raise RuntimeError(
            f"resolution actions changed: expected={required_actions} "
            f"observed={observed_actions}"
        )
    actions = actions.copy()
    actions["scheduled_clock_ms"] = float(clock_ms)
    actions["active"] = actions["activation_status"].astype(str).eq("active")
    actions["fill_by_clock"] = (
        actions["active"]
        & actions["first_fill_ts_ns"].gt(0)
        & actions["first_fill_ts_ns"].le(
            actions["activation_ts_ns"] + int(clock_ms) * 1_000_000
        )
    ).astype(np.int8)
    active_count = actions.groupby("cohort_id", observed=True)["active"].sum()
    common_ids = active_count.index[active_count.eq(len(ACTION_ORDER))]
    common = actions.loc[actions["cohort_id"].isin(common_ids)].copy()
    if common.empty:
        raise RuntimeError("resolution replay has no all-grid activated cohorts")

    wide_fill = common.pivot(
        index="cohort_id", columns="action", values="fill_by_clock"
    )
    violations = 0
    for shallow, deep in zip(ACTION_ORDER[:-1], ACTION_ORDER[1:], strict=True):
        violations += int((wide_fill[deep] > wide_fill[shallow]).sum())
    if violations:
        raise RuntimeError(
            f"observed paired outcome monotonicity failed: {violations} rows"
        )

    metadata_columns = [
        "cohort_id",
        "day",
        "side",
        "inventory_role",
        "campaign_id",
        "mid",
        "quantity",
    ]
    metadata = common.loc[:, metadata_columns].drop_duplicates("cohort_id")
    rows: list[pd.DataFrame] = []
    stress_rate = float(stress_fill_value_bps) / 10_000.0
    for gap in ACTION_GAPS:
        for contrast in CONTRAST_DIRECTIONS:
            shallow, deep, price_gap = _contrast_actions(gap, contrast)
            pair = metadata.copy()
            pair["gap_ticks"] = int(gap)
            pair["contrast"] = str(contrast)
            pair["price_gap_ticks"] = int(price_gap)
            pair["shallow_fill"] = wide_fill.loc[pair["cohort_id"], shallow].to_numpy(
                np.int8
            )
            pair["deep_fill"] = wide_fill.loc[pair["cohort_id"], deep].to_numpy(
                np.int8
            )
            pair["fill_difference"] = pair["shallow_fill"] - pair["deep_fill"]
            if pair["fill_difference"].lt(0).any():
                raise RuntimeError("paired fill difference changed sign")
            pair["quantity_difference_btc"] = (
                pair["fill_difference"] * pair["quantity"]
            )
            pair["shared_fill_price_improvement_usdc"] = (
                pair["deep_fill"]
                * pair["quantity"]
                * float(price_gap)
                * TICK_SIZE
            )
            pair["extra_fill_stress_usdc"] = (
                pair["fill_difference"]
                * pair["quantity"]
                * pair["mid"]
                * stress_rate
            )
            pair["conservative_value_lower_usdc"] = (
                pair["shared_fill_price_improvement_usdc"]
                - pair["extra_fill_stress_usdc"]
            )
            pair["conservative_value_upper_usdc"] = (
                pair["shared_fill_price_improvement_usdc"]
                + pair["extra_fill_stress_usdc"]
            )
            pair["campaign_cluster_id"] = np.where(
                pair["campaign_id"].gt(0),
                pair["day"].astype(str)
                + ":"
                + pair["side"].astype(str)
                + ":campaign:"
                + pair["campaign_id"].astype(str),
                pair["day"].astype(str)
                + ":cohort:"
                + pair["cohort_id"].astype(str),
            )
            rows.append(pair)
    contrasts = pd.concat(rows, ignore_index=True)
    diagnostics = {
        "submitted_cohorts": int(actions["cohort_id"].nunique()),
        "all_grid_activated_cohorts": int(len(common_ids)),
        "all_grid_activation_rate": float(
            len(common_ids) / actions["cohort_id"].nunique()
        ),
        "observed_monotonicity_violations": int(violations),
    }
    return contrasts, diagnostics


SUM_COLUMNS = (
    "fill_difference",
    "quantity_difference_btc",
    "shared_fill_price_improvement_usdc",
    "extra_fill_stress_usdc",
    "conservative_value_lower_usdc",
    "conservative_value_upper_usdc",
)
CELL_COLUMNS = ("side", "inventory_role", "gap_ticks", "contrast")


def aggregate_campaign_clusters(contrasts: pd.DataFrame) -> pd.DataFrame:
    grouped = contrasts.groupby(
        ["day", *CELL_COLUMNS, "campaign_cluster_id"],
        observed=True,
        sort=True,
    )
    out = grouped[list(SUM_COLUMNS)].sum().reset_index()
    counts = grouped.size().rename("decisions").reset_index()
    return out.merge(
        counts,
        on=["day", *CELL_COLUMNS, "campaign_cluster_id"],
        validate="one_to_one",
    )


def _day_simultaneous_bands(
    campaign_clusters: pd.DataFrame,
    *,
    bootstrap_samples: int,
    bootstrap_seed: int,
    confidence: float,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, float]]:
    daily = (
        campaign_clusters.groupby(["day", *CELL_COLUMNS], observed=True)[
            [*SUM_COLUMNS, "decisions"]
        ]
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
    numerators = np.zeros((len(days), len(cells), len(SUM_COLUMNS)), dtype=float)
    denominators = np.zeros((len(days), len(cells)), dtype=float)
    for row in daily.itertuples(index=False):
        day_pos = day_index[str(row.day)]
        key = (
            str(row.side),
            str(row.inventory_role),
            int(row.gap_ticks),
            str(row.contrast),
        )
        cell_pos = cell_index[key]
        denominators[day_pos, cell_pos] = float(row.decisions)
        for metric_pos, name in enumerate(SUM_COLUMNS):
            numerators[day_pos, cell_pos, metric_pos] = float(getattr(row, name))

    total_denominator = denominators.sum(axis=0)
    if np.any(total_denominator <= 0):
        raise RuntimeError("resolution cell has no Development decisions")
    point = numerators.sum(axis=0) / total_denominator[:, None]
    rng = np.random.default_rng(int(bootstrap_seed))
    bootstrap = np.empty(
        (int(bootstrap_samples), len(cells), len(SUM_COLUMNS)), dtype=float
    )
    batch_size = 100
    for start in range(0, int(bootstrap_samples), batch_size):
        stop = min(int(bootstrap_samples), start + batch_size)
        sample = rng.integers(0, len(days), size=(stop - start, len(days)))
        sampled_num = numerators[sample].sum(axis=1)
        sampled_den = denominators[sample].sum(axis=1)
        bootstrap[start:stop] = np.divide(
            sampled_num,
            sampled_den[:, :, None],
            out=np.full_like(sampled_num, np.nan),
            where=sampled_den[:, :, None] > 0,
        )

    alpha = 1.0 - float(confidence)
    critical: dict[str, float] = {}
    lower = np.empty_like(point)
    upper = np.empty_like(point)
    for metric_pos, name in enumerate(SUM_COLUMNS):
        deviations = np.abs(bootstrap[:, :, metric_pos] - point[:, metric_pos])
        max_deviation = np.nanmax(deviations, axis=1)
        radius = float(np.nanquantile(max_deviation, 1.0 - alpha))
        critical[name] = radius
        lower[:, metric_pos] = point[:, metric_pos] - radius
        upper[:, metric_pos] = point[:, metric_pos] + radius

    records: list[dict[str, Any]] = []
    for cell_pos, key in enumerate(cell_keys):
        row: dict[str, Any] = dict(zip(CELL_COLUMNS, key, strict=True))
        row["decisions"] = int(total_denominator[cell_pos])
        supported_days = daily.loc[
            (daily["side"].astype(str) == str(key[0]))
            & (daily["inventory_role"].astype(str) == str(key[1]))
            & (daily["gap_ticks"].astype(int) == int(key[2]))
            & (daily["contrast"].astype(str) == str(key[3]))
        ]
        day_delta = supported_days["fill_difference"] / supported_days["decisions"]
        row["supported_days"] = int(len(supported_days))
        row["positive_days"] = int(day_delta.gt(0).sum())
        row["zero_days"] = int(day_delta.eq(0).sum())
        row["negative_days"] = int(day_delta.lt(0).sum())
        row["positive_day_fraction"] = float(day_delta.gt(0).mean())
        for metric_pos, name in enumerate(SUM_COLUMNS):
            row[name] = float(point[cell_pos, metric_pos])
            row[f"{name}_simultaneous_lower"] = float(lower[cell_pos, metric_pos])
            row[f"{name}_simultaneous_upper"] = float(upper[cell_pos, metric_pos])
        records.append(row)
    return pd.DataFrame(records), daily, critical


def classify_resolution(
    cells: pd.DataFrame,
    *,
    nuisance_uncertainty_usdc: float,
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    cells = cells.copy()
    cells["raw_fill_resolution_supported"] = cells[
        "fill_difference_simultaneous_lower"
    ].gt(0.0)
    nuisance = float(nuisance_uncertainty_usdc)
    cells["deeper_value_supported"] = cells[
        "conservative_value_lower_usdc_simultaneous_lower"
    ].gt(nuisance)
    cells["shallower_value_supported"] = cells[
        "conservative_value_upper_usdc_simultaneous_upper"
    ].lt(-nuisance)
    cells["economic_interval_resolution_supported"] = (
        cells["deeper_value_supported"] | cells["shallower_value_supported"]
    )
    one_sided = cells.loc[
        cells["contrast"].isin({"closer_current", "current_farther"})
    ].copy()
    raw_minima = (
        one_sided.loc[one_sided["raw_fill_resolution_supported"]]
        .groupby(["side", "inventory_role", "contrast"], observed=True)[
            "gap_ticks"
        ]
        .min()
    )
    economic_minima = (
        one_sided.loc[one_sided["economic_interval_resolution_supported"]]
        .groupby(["side", "inventory_role", "contrast"], observed=True)[
            "gap_ticks"
        ]
        .min()
    )
    keys = one_sided.loc[
        :, ["side", "inventory_role", "contrast"]
    ].drop_duplicates()
    key_tuples = list(keys.itertuples(index=False, name=None))
    keys["minimum_raw_fill_gap_ticks"] = [
        int(raw_minima.loc[tuple(row)]) if tuple(row) in raw_minima.index else None
        for row in key_tuples
    ]
    keys["minimum_economic_interval_gap_ticks"] = [
        int(economic_minima.loc[tuple(row)])
        if tuple(row) in economic_minima.index
        else None
        for row in key_tuples
    ]
    economic_resolved = keys["minimum_economic_interval_gap_ticks"].dropna()
    if economic_resolved.empty:
        decision = "close_fill_surface_path_economic_resolution_absent"
    elif (economic_resolved.astype(int) == 1).any():
        decision = "one_tick_economic_resolution_exists_model_is_bottleneck"
    else:
        decision = "one_tick_unresolved_wider_action_grid_has_economic_support"
    return cells, keys, decision


def _load_spec(spec_path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    spec_path = spec_path.expanduser().resolve()
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    if spec.get("schema_version") != "paired_action_resolution_feasibility_spec.v1":
        raise RuntimeError("unsupported action-resolution feasibility Spec")
    if spec.get("research_status") != "frozen_before_development_outcomes":
        raise RuntimeError("action-resolution feasibility Spec is not frozen")
    permissions = spec["permissions"]
    if permissions["validation_read"] or permissions["sealed_holdout_read"]:
        raise RuntimeError("feasibility family must keep later panels sealed")
    if spec["action_grid"]["offset_ticks"] != ACTION_OFFSETS:
        raise RuntimeError("frozen action grid differs from the implementation")
    exchange = spec["exchange_contract"]
    if not math.isclose(float(exchange["tick_size_usdc_per_btc"]), TICK_SIZE):
        raise RuntimeError("frozen tick size differs from the implementation")
    if not math.isclose(float(exchange["lot_size_btc"]), LOT_SIZE):
        raise RuntimeError("frozen lot size differs from the implementation")
    implementation = ROOT / str(spec["implementation"]["path"])
    if file_sha256(implementation) != str(spec["implementation"]["sha256"]):
        raise RuntimeError("feasibility implementation differs from frozen Spec")
    source_manifest = _require_identity(
        spec["source_identity"]["placement_panel_manifest"],
        "placement panel manifest",
    )
    source_root = Path(
        str(spec["source_identity"]["placement_panel_root"])
    ).expanduser().resolve()
    if source_manifest != source_root / "manifest.json":
        raise RuntimeError("placement panel root and manifest identity disagree")
    source_index = _require_identity(
        spec["source_identity"]["placement_panel_index"],
        "placement panel index",
    )
    if source_index != source_root / "development_index.csv":
        raise RuntimeError("placement panel root and index identity disagree")
    pending_report = _require_identity(
        spec["source_identity"]["pending_uncertainty_report"],
        "pending uncertainty report",
    )
    dependencies = {
        name: _identity(
            _require_identity(identity, f"implementation dependency {name}")
        )
        for name, identity in spec["implementation"]["dependencies"].items()
    }
    native_module = _native_module_identity()
    expected_native = spec["implementation"]["native_module"]
    if native_module["sha256"] != str(expected_native["sha256"]):
        raise RuntimeError("native lifecycle module differs from the frozen Spec")
    clock = prediction_clock_contract_from_spec(spec)
    clock_source = _require_identity(
        spec["source_identity"]["prediction_clock_producer"],
        "prediction clock producer",
    )
    clock_identity = verify_prediction_clock_source_identity(clock, clock_source)
    config = yaml.safe_load(clock_source.read_text(encoding="utf-8"))
    actual_clock_ms = int(
        round(float(config["strategy"]["requote_interval"]) * 1000)
    )
    if actual_clock_ms != int(spec["common_clock"]["clock_ms"]):
        raise RuntimeError(
            f"config-derived clock changed: {actual_clock_ms}ms"
        )
    return spec, {
        "spec": _identity(spec_path),
        "implementation": _identity(implementation),
        "source_manifest": _identity(source_manifest),
        "source_index": _identity(source_index),
        "pending_uncertainty_report": _identity(pending_report),
        "prediction_clock_producer": clock_identity,
        "dependencies": dependencies,
        "native_module": native_module,
    }


def _verify_parent_day_sources(manifest: Mapping[str, Any]) -> None:
    for source in manifest["native_tape_identity"]["files"]:
        _require_identity(source, "native order-book source")
    _require_identity(
        manifest["input_artifacts"]["individual_trades"],
        "individual trades",
    )


def _build_day(
    day: str,
    *,
    spec: Mapping[str, Any],
    identities: Mapping[str, Any],
    source_panel_root: Path,
    raw_book_root: Path,
    sparse_cache_root: Path,
    mechanics_cache_root: Path,
    output_dir: Path,
) -> dict[str, Any]:
    source_dir = source_panel_root / "partitions" / f"day={day}"
    source_manifest_path = source_dir / "manifest.json"
    source_panel_path = source_dir / "placement.parquet"
    source_manifest = json.loads(source_manifest_path.read_text(encoding="utf-8"))
    if file_sha256(source_panel_path) != str(source_manifest["panel_sha256"]):
        raise RuntimeError(f"{day} source placement panel hash changed")
    source = pd.read_parquet(source_panel_path)
    mechanics_identity = {
        "schema_version": "paired_action_resolution_mechanics.v1",
        "day": str(day),
        "source_panel_sha256": str(source_manifest["panel_sha256"]),
        "source_partition_manifest_sha256": file_sha256(source_manifest_path),
        "actions": ACTION_OFFSETS,
        "clock_ms": int(spec["common_clock"]["clock_ms"]),
        "implementation_sha256": identities["implementation"]["sha256"],
        "dependency_sha256": {
            name: identity["sha256"]
            for name, identity in identities["dependencies"].items()
        },
        "native_module_sha256": identities["native_module"]["sha256"],
        "native_tape_identity": source_manifest["native_tape_identity"],
        "individual_trades": source_manifest["input_artifacts"]["individual_trades"],
    }
    mechanics_cache = ParquetContentAddressedCache(
        mechanics_cache_root, namespace="day"
    )
    record = mechanics_cache.load(mechanics_identity)
    sparse_record = None
    simulation: dict[str, Any] = {}
    parity: dict[str, Any] = {}
    if record is None:
        _verify_parent_day_sources(source_manifest)
        cohorts = build_resolution_cohorts(source)
        stage = Path(tempfile.mkdtemp(prefix=f"resolution-{day}."))
        try:
            watch_path = stage / "watches.parquet"
            build_resolution_watch_manifest(cohorts).to_parquet(
                watch_path, index=False, compression="zstd"
            )
            sparse_identity = {
                "schema_version": "paired_action_resolution_sparse_tape.v1",
                "day": str(day),
                "watch_manifest_sha256": file_sha256(watch_path),
                "source_panel_sha256": str(source_manifest["panel_sha256"]),
                "native_tape_identity": source_manifest["native_tape_identity"],
                "builder_sha256": file_sha256(
                    ROOT / "data" / "build_active_order_queue_tape.py"
                ),
                "implementation_sha256": identities["implementation"]["sha256"],
            }
            sparse_cache = DirectoryContentAddressedCache(
                sparse_cache_root, namespace="day"
            )

            def build_sparse(payload_dir: Path) -> Mapping[str, Any]:
                return build_active_order_queue_tape(
                    watch_manifest=watch_path,
                    raw_root=raw_book_root,
                    output_dir=payload_dir,
                    symbol="BTCUSDC",
                    tick_size=TICK_SIZE,
                    warmup_hours=24,
                    reuse_raw_only=True,
                )

            sparse_record = sparse_cache.get_or_build(sparse_identity, build_sparse)
            bt.configure_symbol("BTCUSDC")
            trades = bt.load_individual_trades(
                days=[day], quality_allowed_days=(day,)
            )
            actions, simulation = _simulate_resolution_cohorts(
                cohorts,
                tape_dir=sparse_record.payload_dir,
                trades=trades,
            )
            parity = _parity_audit(actions, source)
            if not parity["passed"]:
                raise RuntimeError(f"{day} expanded replay failed +/-1 source parity")
            record = mechanics_cache.store(
                mechanics_identity,
                actions,
                metadata={
                    "simulation": simulation,
                    "parity": parity,
                    "sparse_cache_key": sparse_record.key,
                },
            )
        finally:
            shutil.rmtree(stage, ignore_errors=True)
    else:
        simulation = dict(record.manifest.get("metadata", {}).get("simulation", {}))
        parity = dict(record.manifest.get("metadata", {}).get("parity", {}))
    if record is None:
        raise AssertionError("mechanics cache admission failed")
    actions = record.frame
    clock = prediction_clock_contract_from_spec(spec)
    assert_common_prediction_clock(
        actions.assign(scheduled_clock_ms=float(spec["common_clock"]["clock_ms"])),
        clock_contract=clock,
        actions=ACTION_ORDER,
    )
    contrasts, diagnostics = build_contrast_rows(
        actions,
        clock_ms=int(spec["common_clock"]["clock_ms"]),
        stress_fill_value_bps=float(spec["economic_scale"]["stress_fill_value_bps"]),
    )
    clusters = aggregate_campaign_clusters(contrasts)
    activation = (
        actions.assign(active=actions["activation_status"].astype(str).eq("active"))
        .groupby(["day", "side", "inventory_role", "action"], observed=True)["active"]
        .agg(["size", "sum"])
        .reset_index()
        .rename(columns={"size": "submitted", "sum": "activated"})
    )

    final_dir = output_dir / "partitions" / f"day={day}"
    final_dir.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f"day={day}.", dir=final_dir.parent))
    try:
        cluster_path = stage / "campaign_clusters.parquet"
        activation_path = stage / "activation.parquet"
        clusters.to_parquet(cluster_path, index=False, compression="zstd")
        activation.to_parquet(activation_path, index=False, compression="zstd")
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "day": str(day),
            "source_partition": _identity(source_manifest_path),
            "source_panel": _identity(source_panel_path),
            "mechanics_cache": {
                "key": record.key,
                "hit": bool(record.hit),
                "payload_path": str(record.entry_dir / "payload.parquet"),
                "payload_sha256": str(record.manifest["payload_sha256"]),
            },
            "simulation": simulation,
            "parity": parity,
            "common_support": diagnostics,
            "campaign_clusters": _identity(cluster_path),
            "activation": _identity(activation_path),
        }
        # Staged identities use only the basename so the partition remains
        # movable before atomic admission.
        for key in ("campaign_clusters", "activation"):
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
    source_panel_root = Path(
        spec["source_identity"]["placement_panel_root"]
    ).expanduser().resolve()
    if source_panel_root != args.source_panel_root:
        raise RuntimeError("selected placement panel root differs from frozen Spec")
    expected_days = [str(day) for day in spec["panels"]["development_days"]]
    source_manifest = json.loads(
        (source_panel_root / "manifest.json").read_text(encoding="utf-8")
    )
    source_days = [str(day) for day in source_manifest["days"]]
    if source_days != expected_days:
        raise RuntimeError("source placement panel days differ from frozen Development")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    storage = _storage_preflight(
        args.output_dir,
        float(spec["storage_gate"]["estimated_final_gib"]),
    )
    preflight = {
        "schema_version": SCHEMA_VERSION,
        "family_id": str(spec["family_id"]),
        "days": expected_days,
        "identities": identities,
        "actions": ACTION_OFFSETS,
        "common_clock": spec["common_clock"],
        "storage": storage,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    _atomic_json(preflight, args.output_dir / "preflight_manifest.json")

    manifests = []
    for index, day in enumerate(expected_days, 1):
        print(f"[{index:02d}/{len(expected_days):02d}] {day}", flush=True)
        manifests.append(
            _build_day(
                day,
                spec=spec,
                identities=identities,
                source_panel_root=source_panel_root,
                raw_book_root=args.raw_book_root,
                sparse_cache_root=args.sparse_cache_root,
                mechanics_cache_root=args.mechanics_cache_root,
                output_dir=args.output_dir,
            )
        )

    cluster_parts = []
    activation_parts = []
    for day in expected_days:
        partition = args.output_dir / "partitions" / f"day={day}"
        manifest = json.loads((partition / "manifest.json").read_text(encoding="utf-8"))
        for key, name in (
            ("campaign_clusters", "campaign_clusters.parquet"),
            ("activation", "activation.parquet"),
        ):
            path = partition / name
            if file_sha256(path) != str(manifest[key]["sha256"]):
                raise RuntimeError(f"{day} {key} partition hash changed")
        cluster_parts.append(pd.read_parquet(partition / "campaign_clusters.parquet"))
        activation_parts.append(pd.read_parquet(partition / "activation.parquet"))
    clusters = pd.concat(cluster_parts, ignore_index=True)
    activation = pd.concat(activation_parts, ignore_index=True)
    cells, daily, critical = _day_simultaneous_bands(
        clusters,
        bootstrap_samples=int(spec["inference"]["bootstrap_samples"]),
        bootstrap_seed=int(spec["inference"]["bootstrap_seed"]),
        confidence=float(spec["inference"]["simultaneous_confidence"]),
    )
    cells, minima, decision = classify_resolution(
        cells,
        nuisance_uncertainty_usdc=float(
            spec["economic_scale"]["pending_nuisance_uncertainty_usdc"]
        ),
    )
    cells_path = args.output_dir / "cell_metrics.csv"
    daily_path = args.output_dir / "daily_metrics.csv"
    minima_path = args.output_dir / "minimum_resolved_gap.csv"
    activation_path = args.output_dir / "activation_metrics.csv"
    cells.to_csv(cells_path, index=False)
    daily.to_csv(daily_path, index=False)
    minima.to_csv(minima_path, index=False)
    activation.to_csv(activation_path, index=False)
    report = {
        "schema_version": SCHEMA_VERSION,
        "family_id": str(spec["family_id"]),
        "spec": identities["spec"],
        "development_days": expected_days,
        "development_day_count": int(len(expected_days)),
        "common_clock": spec["common_clock"],
        "action_offsets": ACTION_OFFSETS,
        "common_support": {
            "submitted_cohorts": int(
                sum(row["common_support"]["submitted_cohorts"] for row in manifests)
            ),
            "all_grid_activated_cohorts": int(
                sum(
                    row["common_support"]["all_grid_activated_cohorts"]
                    for row in manifests
                )
            ),
            "observed_monotonicity_violations": int(
                sum(
                    row["common_support"]["observed_monotonicity_violations"]
                    for row in manifests
                )
            ),
        },
        "source_parity": {
            "passed": bool(all(row["parity"]["passed"] for row in manifests)),
            "rows": int(sum(row["parity"]["rows"] for row in manifests)),
            "mismatches": {
                name: int(
                    sum(row["parity"]["mismatches"][name] for row in manifests)
                )
                for name in ("activation_status", "first_fill_ts_ns", "fill_qty")
            },
        },
        "inference": {
            "cluster_contract": (
                "campaign-attributed outcomes aggregated within UTC day; "
                "the coarser UTC-day bootstrap preserves all nested campaign dependence"
            ),
            "simultaneous_confidence": float(
                spec["inference"]["simultaneous_confidence"]
            ),
            "bootstrap_samples": int(spec["inference"]["bootstrap_samples"]),
            "critical_absolute_radii": critical,
        },
        "resolution": {
            "raw_fill_supported_cells": int(
                cells["raw_fill_resolution_supported"].sum()
            ),
            "economic_interval_supported_cells": int(
                cells["economic_interval_resolution_supported"].sum()
            ),
            "total_cells": int(len(cells)),
            "one_tick_raw_fill_supported_cells": int(
                cells.loc[
                    cells["gap_ticks"].eq(1), "raw_fill_resolution_supported"
                ].sum()
            ),
            "one_tick_economic_interval_supported_cells": int(
                cells.loc[
                    cells["gap_ticks"].eq(1),
                    "economic_interval_resolution_supported",
                ].sum()
            ),
            "minimum_gap_records": minima.to_dict("records"),
        },
        "artifacts": {
            "cell_metrics": _identity(cells_path),
            "daily_metrics": _identity(daily_path),
            "minimum_resolved_gap": _identity(minima_path),
            "activation_metrics": _identity(activation_path),
        },
        "permissions": {
            "prediction_supported": False,
            "transport_supported": False,
            "economic_resolution_supported": False,
            "action_uplift_supported": False,
            "action_experiment_authorized": False,
            "live_deployment_authorized": False,
            "validation_read": False,
            "sealed_holdout_read": False,
        },
        "decision": decision,
    }
    _atomic_json(report, args.output_dir / "report.json")
    return report


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--source-panel-root", type=Path, default=DEFAULT_SOURCE_PANEL)
    parser.add_argument("--raw-book-root", type=Path, default=DEFAULT_RAW_BOOK)
    parser.add_argument("--sparse-cache-root", type=Path, default=DEFAULT_SPARSE_CACHE)
    parser.add_argument(
        "--mechanics-cache-root", type=Path, default=DEFAULT_MECHANICS_CACHE
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    for name in (
        "spec",
        "output_dir",
        "source_panel_root",
        "raw_book_root",
        "sparse_cache_root",
        "mechanics_cache_root",
    ):
        setattr(args, name, getattr(args, name).expanduser().resolve())
    report = run(args)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
