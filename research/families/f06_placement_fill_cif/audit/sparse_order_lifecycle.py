"""Native lifecycle replay over a sparse active-order price-level tape."""

from __future__ import annotations

import math
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from research.families.f06_placement_fill_cif.audit.paired_order_lifecycle import (
    ACTION_ORDER,
    PlacementChild,
    PlacementCohort,
)

SCHEMA_VERSION = "sparse_order_lifecycle_adapter.v1"

_SEED_STATUS = {"unknown": 0, "exact": 1, "known_zero": 2}
_ACTIVATION_STATUS = {
    1: "active",
    2: "gtx_reject",
    3: "invalid_book",
    4: "cancelled_before_activation",
}
_QUEUE_INVALID_REASON = {
    0: "",
    1: "seed_unavailable",
    2: "same_boundary_activation_book_event",
    3: "same_ms_native_trade_or_activation",
    4: "native_sequence_invalidated",
    5: "native_snapshot_reset",
}
_TOUCH_TYPE = {0: "", 1: "exact", 2: "through"}
_FILL_MECHANISM = {0: "", 1: "exact_queue", 2: "strict_through"}
_ORDER_STATE = {
    0: "",
    1: "open",
    2: "pending_cancel",
    3: "filled",
    4: "cancelled",
    5: "rejected",
    6: "censored",
}
_TERMINAL_REASON = {
    0: "",
    1: "exact_queue",
    2: "strict_through",
    3: "cancel_ack",
    4: "administrative_censor",
    5: "gtx_reject",
    6: "invalid_book",
    7: "cancel_ack_before_activation",
}
_EVENT_CODE = {
    "update": 0,
    "delete": 0,
    "invalidate": 1,
    "snapshot": 2,
}


def _watch_id(cohort: PlacementCohort, action: str) -> str:
    return f"{cohort.cohort_id}:{action}"


def build_watch_manifest(
    cohorts: Sequence[PlacementCohort],
    *,
    tick_size: float,
) -> pd.DataFrame:
    """Create the sparse native-book watch list for one day."""

    if not cohorts:
        raise ValueError("sparse lifecycle requires at least one cohort")
    if not math.isfinite(tick_size) or tick_size <= 0.0:
        raise ValueError("tick_size must be positive")
    days = {str(cohort.day) for cohort in cohorts}
    if len(days) != 1:
        raise ValueError("one sparse lifecycle manifest must contain one UTC day")
    day = next(iter(days))
    day_end_ms = int(
        (pd.Timestamp(day, tz="UTC") + pd.Timedelta(days=1)).timestamp()
        * 1_000
    )

    rows: list[dict[str, Any]] = []
    for cohort in cohorts:
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
            # The authoritative scheduler consumes market events at the
            # terminal millisecond before applying ACK/censor. The sparse tape
            # uses an exclusive stop, so retain that boundary with +1 ms.
            stop_ms = min(
                day_end_ms,
                max(activate_ms + 1, terminal_bound + 1),
            )
            rows.append(
                {
                    "day": str(cohort.day),
                    "watch_id": _watch_id(cohort, action),
                    "order_id": _watch_id(cohort, action),
                    "side": str(child.side),
                    "price": float(child.price_tick) * float(tick_size),
                    "activate_ts_ms": activate_ms,
                    "stop_ts_ms": stop_ms,
                }
            )
    frame = pd.DataFrame(rows)
    if frame["watch_id"].duplicated().any():
        raise RuntimeError("sparse lifecycle watch ids are not unique")
    return frame


def _aligned_children(
    cohorts: Sequence[PlacementCohort],
) -> tuple[list[str], list[PlacementChild]]:
    watch_ids: list[str] = []
    children: list[PlacementChild] = []
    for cohort in cohorts:
        for action in ACTION_ORDER:
            watch_ids.append(_watch_id(cohort, action))
            children.append(cohort.children[action])
    return watch_ids, children


def _required_columns(
    frame: pd.DataFrame,
    required: set[str],
    *,
    label: str,
) -> None:
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"{label} is missing columns: {missing}")


def _same_ms_trade_mask(
    event_ts_ms: np.ndarray,
    trade_ts_ms: np.ndarray,
) -> np.ndarray:
    if len(event_ts_ms) == 0 or len(trade_ts_ms) == 0:
        return np.zeros(len(event_ts_ms), dtype=bool)
    unique_trade_ts = np.unique(trade_ts_ms)
    positions = np.searchsorted(unique_trade_ts, event_ts_ms)
    in_bounds = positions < len(unique_trade_ts)
    out = np.zeros(len(event_ts_ms), dtype=bool)
    out[in_bounds] = unique_trade_ts[positions[in_bounds]] == event_ts_ms[in_bounds]
    return out


def _finite_int(value: object, *, scale: int = 1) -> int:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0
    if not math.isfinite(numeric):
        return 0
    return int(numeric) * int(scale)


def _apply_native_result(
    child: PlacementChild,
    seed: pd.Series,
    result: dict[str, Any],
    row: int,
) -> None:
    activation_status = _ACTIVATION_STATUS[int(result["activation_status"][row])]
    child.activation_status = activation_status
    if activation_status == "active":
        child.activation_queue_status = str(seed["seed_status"])
        child.activation_queue_reason = str(seed["seed_reason"])
        child.activation_queue_asof_ts_ns = _finite_int(
            seed.get("seed_asof_ts_ms"), scale=1_000_000
        )
        child.activation_queue_segment_id = _finite_int(seed.get("segment_id"))
        child.activation_queue_qty = float(seed.get("seed_qty", math.nan))
    child.queue_path_valid = bool(result["queue_path_valid"][row])
    child.queue_invalid_reason = _QUEUE_INVALID_REASON[
        int(result["queue_invalid_reason"][row])
    ]
    if (
        activation_status == "active"
        and str(seed["seed_status"]) == "unknown"
        and child.queue_invalid_reason == "seed_unavailable"
    ):
        child.queue_invalid_reason = str(seed["seed_reason"])
    child.first_touch_ts_ns = int(result["first_touch_ts_ms"][row]) * 1_000_000
    child.first_touch_type = _TOUCH_TYPE[int(result["first_touch_type"][row])]
    child.exact_touch_ts_ns = int(result["exact_touch_ts_ms"][row]) * 1_000_000
    child.through_touch_ts_ns = (
        int(result["through_touch_ts_ms"][row]) * 1_000_000
    )
    child.first_fill_ts_ns = int(result["first_fill_ts_ms"][row]) * 1_000_000
    child.first_fill_mechanism = _FILL_MECHANISM[
        int(result["first_fill_mechanism"][row])
    ]
    child.fill_qty = float(result["fill_qty"][row])
    child.remaining_qty = float(result["remaining_qty"][row])
    child.full_fill_ts_ns = int(result["full_fill_ts_ms"][row]) * 1_000_000
    child.partial_fill_count = int(result["partial_fill_count"][row])
    child.request_state_observed = bool(result["request_state_observed"][row])
    request_state_code = int(result["request_order_state_before"][row])
    child.request_order_state_before = (
        "pending_new"
        if child.request_state_observed and request_state_code == 0
        else _ORDER_STATE[request_state_code]
    )
    child.request_order_age_ms = float(result["request_order_age_ms"][row])
    child.request_remaining_qty = float(result["request_remaining_qty"][row])
    child.request_queue_left = float(result["request_queue_left"][row])
    child.request_queue_path_valid = bool(
        result["request_queue_path_valid"][row]
    )
    child.request_native_cancel_count = int(
        result["request_native_cancel_count"][row]
    )
    child.request_native_cancel_qty = float(
        result["request_native_cancel_qty"][row]
    )
    child.request_native_refill_count = int(
        result["request_native_refill_count"][row]
    )
    child.request_native_refill_qty = float(
        result["request_native_refill_qty"][row]
    )
    child.request_native_level_event_count = int(
        result["request_native_level_event_count"][row]
    )
    child.cancel_requested = bool(child.request_state_observed)
    child.cancel_acked = bool(result["cancel_acked"][row])
    child.fill_while_cancel_pending_qty = float(
        result["fill_while_cancel_pending_qty"][row]
    )
    child.first_pending_cancel_fill_ts_ns = (
        int(result["first_pending_cancel_fill_ts_ms"][row]) * 1_000_000
    )
    child.state = _ORDER_STATE[int(result["terminal_state"][row])]
    child.terminal_ts_ns = int(result["terminal_ts_ms"][row]) * 1_000_000
    child.terminal_reason = _TERMINAL_REASON[
        int(result["terminal_reason"][row])
    ]
    child.native_cancel_count = int(result["native_cancel_count"][row])
    child.native_cancel_qty = float(result["native_cancel_qty"][row])
    child.native_refill_count = int(result["native_refill_count"][row])
    child.native_refill_qty = float(result["native_refill_qty"][row])
    child.native_level_event_count = int(
        result["native_level_event_count"][row]
    )
    child.same_ms_ambiguity_count = int(
        result["same_ms_ambiguity_count"][row]
    )


def simulate_sparse_paired_placements(
    cohorts: Sequence[PlacementCohort],
    *,
    tape_dir: Path,
    trades: pd.DataFrame,
    tick_size: float,
    lot_size: float,
    queue_deplete_mult: float,
    fail_on_monotonicity: bool = True,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Run all placement children in C++ against one sparse native tape."""

    try:
        import narrowgate_cpp  # type: ignore
    except Exception as exc:  # pragma: no cover - environment contract
        raise RuntimeError("narrowgate_cpp is required for sparse lifecycle replay") from exc
    if not hasattr(narrowgate_cpp, "simulate_sparse_order_lifecycles"):
        raise RuntimeError("narrowgate_cpp lacks sparse lifecycle ABI v1")
    if not cohorts:
        raise ValueError("sparse lifecycle requires at least one cohort")
    if not math.isfinite(queue_deplete_mult) or queue_deplete_mult < 0.0:
        raise ValueError("queue_deplete_mult must be finite and non-negative")
    child_mults = {
        round(float(child.queue_deplete_mult), 12)
        for cohort in cohorts
        for child in cohort.children.values()
    }
    if child_mults != {round(float(queue_deplete_mult), 12)}:
        raise ValueError("native v1 requires one shared queue_deplete_mult")

    tape_dir = tape_dir.expanduser().resolve()
    seeds = pd.read_parquet(tape_dir / "seeds.parquet")
    events = pd.read_parquet(tape_dir / "level_events.parquet")
    _required_columns(
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
        label="sparse seeds",
    )
    _required_columns(
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
        label="sparse level events",
    )
    _required_columns(
        trades,
        {"transact_time", "price", "quantity", "is_buyer_maker"},
        label="individual trades",
    )

    watch_ids, children = _aligned_children(cohorts)
    if seeds["watch_id"].astype(str).duplicated().any():
        raise RuntimeError("sparse seed watch ids are not unique")
    seed_by_id = seeds.assign(watch_id=seeds["watch_id"].astype(str)).set_index(
        "watch_id"
    )
    missing = sorted(set(watch_ids) - set(seed_by_id.index))
    extra = sorted(set(seed_by_id.index) - set(watch_ids))
    if missing or extra:
        raise RuntimeError(
            f"sparse seed identity mismatch: missing={missing[:3]} extra={extra[:3]}"
        )
    aligned_seeds = seed_by_id.loc[watch_ids]
    unsupported_seed = sorted(
        set(aligned_seeds["seed_status"].astype(str)) - set(_SEED_STATUS)
    )
    if unsupported_seed:
        raise ValueError(f"unsupported sparse seed status: {unsupported_seed}")
    order_index = {watch_id: index for index, watch_id in enumerate(watch_ids)}

    event_order = events["watch_id"].astype(str).map(order_index)
    if event_order.isna().any():
        unknown = events.loc[event_order.isna(), "watch_id"].astype(str).iloc[0]
        raise RuntimeError(f"level event belongs to unknown watch_id={unknown}")
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
        | _same_ms_trade_mask(event_ts, trade_ts)
    )
    event_codes = encoded_events["event_code"].astype(str).map(_EVENT_CODE)
    if event_codes.isna().any():
        unknown = encoded_events.loc[event_codes.isna(), "event_code"].iloc[0]
        raise ValueError(f"unsupported sparse event_code={unknown!r}")

    side = np.fromiter(
        (1 if child.side == "BUY" else 2 for child in children),
        dtype=np.uint8,
        count=len(children),
    )
    result = narrowgate_cpp.simulate_sparse_order_lifecycles(
        order_side=side,
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
        .map(_SEED_STATUS)
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
            / float(tick_size)
        ).astype(np.int64),
        trade_qty=pd.to_numeric(
            encoded_trades["quantity"], errors="raise"
        ).to_numpy(dtype=np.float64),
        is_buyer_maker=encoded_trades["is_buyer_maker"]
        .astype(bool)
        .to_numpy(dtype=np.uint8),
        lot_size=float(lot_size),
        queue_deplete_mult=float(queue_deplete_mult),
    )
    if str(result.get("schema_version")) != "sparse_order_lifecycle.v1":
        raise RuntimeError("native sparse lifecycle schema changed")
    for row, (child, watch_id) in enumerate(zip(children, watch_ids, strict=True)):
        _apply_native_result(child, seed_by_id.loc[watch_id], result, row)

    rows = pd.DataFrame([cohort.as_wide_record() for cohort in cohorts])
    violations = int(rows["monotonicity_violation_count"].sum())
    if fail_on_monotonicity and violations:
        bad = rows.loc[
            rows["monotonicity_violation_count"] > 0,
            ["cohort_id", "monotonicity_violations"],
        ].head(5)
        raise RuntimeError(
            "sparse native lifecycle monotonicity violation:\n"
            + bad.to_string(index=False)
        )
    return rows, {
        "schema_version": SCHEMA_VERSION,
        "cohorts": int(len(cohorts)),
        "children": int(len(children)),
        "level_events": int(len(encoded_events)),
        "trade_rows": int(len(encoded_trades)),
        "monotonicity_violations": violations,
    }
