#!/usr/bin/env python3
"""
Tick-level event replay using raw aggTrades and FIFO-style fill accounting.

Strategy mechanics are kept aligned with the live engine; this module focuses
on per-trade replay, queue volume and parameter sweeps.

Account boundaries come from the frozen experiment contract. Continuous
accounts retain order, inventory and risk state across UTC midnight.
账户边界遵循冻结实验合同；连续账户跨 UTC 日界保留订单、库存与风险状态。

The sole command entry is ``narrowgate replay``. This module owns the executor.
"""

from types import SimpleNamespace
from strategy.replace_threshold import ReplacePriceThreshold
from strategy.markout_observation import markout_semantics, pause_transition
import array
import gc
import heapq
import hashlib
import json
import math
import operator
import os
import random
import re
import sys
import time
from dataclasses import asdict, replace
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping, Optional
from zoneinfo import ZoneInfo
from narrowgate.replay.event_clock import build_replay_event_clock

# These paths are populated by ``update_symbol_globals`` below before any
# replay entrypoint can call a function that consumes them.  Keep explicit
# module bindings so static checks cover the same maintained runtime surface.
FEATURES_DIR: Path
MODEL_DIR: Path
RESULTS_DIR: Path


from execution.order_lifecycle import (
    OrderLifecyclePhase,
    QuantityWeightedOrderLifecycle,
    TerminalPolicyRoute,
    terminal_policy_route,
)
from execution.order_lifecycle_journal_v2 import (
    audit_order_lifecycle_journal_v2 as audit_order_lifecycle_journal,
    emit_diagnostic_lifecycle_rows,
)
from models.replay.order_lifecycle_v2_replay_adapter import (
    OrderLifecycleV2ReplayAdapter,
)
from models.replay.order_lifecycle_v2_replay_adapter_strict_native import (
    OrderLifecycleV2ReplayAdapter as StrictNativeOrderLifecycleV2ReplayAdapter,
)
from models.replay.continuous_accounting import FEE_ACCOUNTING_SEMANTICS, UtcAccountingMarkRecorder
from models.replay.fill_diagnostics import fill_markout_diagnostics, diagnostic_counts
from models.replay.replay_state_checkpoint import validate_replay_initial_state
from models.replay.risk_selection import (
    ReplayRiskSelection,
    feature_ready_time,
    opportunity_id,
    visible_feature_snapshot,
)
from models.replay_contract import (  # noqa: E402
    _local_work_samples,
    bulk_cancel_sample_rows,
    rest_http_result_statuses,
    rest_return_sample_rows,
    runtime_compute_sample_rows,
)
from strategy.replay_controls import (
    SIGNAL_WARMUP_BARS,
    LOSS_COOLDOWN_SEMANTICS,
    LOSS_COOLDOWN_SNAPSHOT_SCHEMA,
    SYNC_CENSOR_CODE,
    SYNC_DEGRADE_SEMANTICS,
    SYNC_EVENT_CODE,
    ConsecutiveLossCooldown,
    ReplayHardRiskState,
    ReplayOrderDepthPath,
    cap_exposure_qty_by_position_value,
    effective_decision_to_gateway_latency_seed,
    effective_latency_seed,
    hard_risk_reason,
    load_sync_degrade_events,
    replay_hard_risk_limits,
    subtract_lot_quantity,
    synchronize_visibility_batch_ambiguity_to_cpp,
)
from strategy.fill_cooldown import (
    CausalVarianceRateStream,
    LINEAGE_CANDIDATE_ACTION,
    LINEAGE_CONTROL_ACTION,
    OnlineVarianceTimeEpisode,
    StratifiedBernoulliLineageRandomizer,
    choose_lineage_randomized_action,
    cooldown_duration_after_same_side_fill,
    normalize_lineage_randomization_probabilities,
    start_online_variance_time_episode,
)
from strategy.conditional_p3_reach_gate import (
    apply_conditional_p3_outward_quote_action,
    executable_price_tick,
)
from strategy.buy_soft_widen_release import evaluate_buy_soft_widen_release
from strategy.inventory_soft_control import (
    resolve_modes as resolve_inventory_soft_control_modes,
    consecutive_multiplier as inventory_cooldown_multiplier,
    cooldown_blocks_action as inventory_cooldown_blocks_action,
    generic_markout_enabled_for_role,
    pre_generic_sizes,
    pure_reducing_role,
    rounded_release_quantity,
)
from strategy.risk_selection import (
    PREFILL_FEATURE_CONTRACT,
    PendingExposure,
    RiskSelectionCandidate,
    RiskSelectionObservation,
    candidate_role,
    prefill_features,
)

# Force unbuffered output for progress reporting
os.environ["PYTHONUNBUFFERED"] = "1"
VERBOSE = False
INVENTORY_LIFECYCLE_ACCOUNTING_SEMANTICS = "zero_boundary_flip_fee_split_v2"

# Ex-ante cross-market fields carried alongside ML predictions for research
# replay.  These are quote-time features only; offline shock labels remain
# attribution diagnostics and must not become tradable gates.
XMARKET_REPLAY_FEATURE_COLUMNS = (
    "cv_ref_perp_ret_10s",
    "cv_ref_perp_ret_30s",
    "cv_ref_perp_ret_60s",
    "cv_ref_perp_volume_imbalance",
    "cv_ref_perp_available",
    "cv_exec_spot_ret_10s",
    "cv_exec_spot_ret_30s",
    "cv_exec_spot_ret_60s",
    "cv_exec_spot_volume_imbalance",
    "cv_exec_spot_available",
    "cv_ref_spot_ret_10s",
    "cv_ref_spot_ret_30s",
    "cv_ref_spot_ret_60s",
    "cv_ref_spot_volume_imbalance",
    "cv_ref_spot_available",
)


def qprint(*args, **kwargs):
    if VERBOSE:
        print(*args, **kwargs)


def inventory_lifecycle_flip_zero_boundary_equity(
    *,
    post_fill_cash: float,
    post_fill_position: float,
    fill_price: float,
    opening_quantity: float,
    fee_rate: float,
) -> float:
    """Value the closing inventory_lifecycle at zero, excluding the new leg's fee."""
    opening_fee = float(fill_price) * float(opening_quantity) * float(fee_rate)
    return (
        float(post_fill_cash)
        + float(post_fill_position) * float(fill_price)
        + opening_fee
    )


def inventory_lifecycle_fill_economic_legs(
    *,
    physical_fill_identity: str,
    side: str,
    inventory_before: float,
    inventory_after: float,
    fill_quantity: float,
    fill_price: float,
    fee_rate: float,
    closing_inventory_lifecycle_id: int,
    opening_inventory_lifecycle_id: int,
) -> tuple[dict[str, Any], ...]:
    """Project one physical fill into inventory_lifecycle-local economic legs.

    A cross-zero exchange trade remains one physical fill/order transition,
    while its closing and opening quantities are represented as two records
    sharing the same immutable physical identity.
    """
    identity = str(physical_fill_identity).strip()
    normalized_side = str(side).upper()
    before = float(inventory_before)
    after = float(inventory_after)
    quantity = float(fill_quantity)
    price = float(fill_price)
    rate = float(fee_rate)
    if (
        not identity
        or normalized_side not in {"BUY", "SELL"}
        or not all(
            math.isfinite(value)
            for value in (before, after, quantity, price, rate)
        )
        or quantity <= 0.0
        or price <= 0.0
    ):
        raise ValueError("inventory_lifecycle economic fill identity is invalid")
    physical_fee = price * quantity * rate
    crossed_side = before * after < -1e-20
    if not crossed_side:
        inventory_lifecycle_id = (
            int(opening_inventory_lifecycle_id)
            if abs(before) <= 1e-10 and abs(after) > 1e-10
            else int(closing_inventory_lifecycle_id)
        )
        if inventory_lifecycle_id <= 0:
            raise ValueError("economic fill leg requires a inventory_lifecycle id")
        role = (
            "opening"
            if abs(before) <= 1e-10
            else "closing"
            if abs(after) < abs(before) - 1e-10
            else "adding"
        )
        return (
            {
                "schema_version": "inventory_lifecycle_fill_economic_leg.v1",
                "physical_fill_identity": identity,
                "physical_fill_leg_count": 1,
                "economic_leg_index": 0,
                "economic_leg_role": role,
                "inventory_lifecycle_id": inventory_lifecycle_id,
                "side": normalized_side,
                "quantity_btc": quantity,
                "price_usdc_per_btc": price,
                "fee_usdc": physical_fee,
                "inventory_before_btc": before,
                "inventory_after_btc": after,
            },
        )

    closing_qty = abs(before)
    opening_qty = abs(after)
    if (
        int(closing_inventory_lifecycle_id) <= 0
        or int(opening_inventory_lifecycle_id) <= int(closing_inventory_lifecycle_id)
        or not math.isclose(
            closing_qty + opening_qty,
            quantity,
            rel_tol=0.0,
            abs_tol=1e-10,
        )
    ):
        raise ValueError("cross-zero physical fill cannot be split exactly")
    closing_fee = physical_fee * closing_qty / quantity
    opening_fee = physical_fee - closing_fee
    return (
        {
            "schema_version": "inventory_lifecycle_fill_economic_leg.v1",
            "physical_fill_identity": identity,
            "physical_fill_leg_count": 2,
            "economic_leg_index": 0,
            "economic_leg_role": "closing",
            "inventory_lifecycle_id": int(closing_inventory_lifecycle_id),
            "side": normalized_side,
            "quantity_btc": closing_qty,
            "price_usdc_per_btc": price,
            "fee_usdc": closing_fee,
            "inventory_before_btc": before,
            "inventory_after_btc": 0.0,
        },
        {
            "schema_version": "inventory_lifecycle_fill_economic_leg.v1",
            "physical_fill_identity": identity,
            "physical_fill_leg_count": 2,
            "economic_leg_index": 1,
            "economic_leg_role": "opening",
            "inventory_lifecycle_id": int(opening_inventory_lifecycle_id),
            "side": normalized_side,
            "quantity_btc": opening_qty,
            "price_usdc_per_btc": price,
            "fee_usdc": opening_fee,
            "inventory_before_btc": 0.0,
            "inventory_after_btc": after,
        },
    )


def loss_cooldown_config_values(
    params: Mapping[str, Any],
) -> tuple[int, float, int]:
    """Return strict loss-cooldown config without numeric truncation/clamping."""
    raw_limit = params.get("max_consecutive_losses", 0)
    if isinstance(raw_limit, bool):
        raise ValueError("max_consecutive_losses must be a non-negative integer")
    try:
        limit = operator.index(raw_limit)
    except TypeError as exc:
        raise ValueError(
            "max_consecutive_losses must be a non-negative integer"
        ) from exc
    raw_cooldown = params.get("cooldown_after_loss", 0.0)
    try:
        cooldown_s = float(raw_cooldown)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(
            "cooldown_after_loss must be finite and non-negative"
        ) from exc
    if limit < 0 or not math.isfinite(cooldown_s) or cooldown_s < 0.0:
        raise ValueError("loss-cooldown config must be finite and non-negative")
    return limit, cooldown_s, int(round(cooldown_s * 1_000.0))


def _resolve_fill_cooldown_reset_policy(params: Mapping[str, Any]) -> tuple[str, bool]:
    from strategy.fill_cooldown import (
        normalize_consecutive_reset_policy,
        reset_policy_clears_on_expiry,
    )

    legacy = (
        params.get("fill_cooldown_reset_consec_on_expiry")
        if "fill_cooldown_reset_consec_on_expiry" in params
        else None
    )
    policy = normalize_consecutive_reset_policy(
        params.get("fill_cooldown_consecutive_reset_policy"),
        legacy_reset_on_expiry=legacy,
        require_explicit=str(params.get("replay_purpose", "")).lower() == "formal",
    )
    return policy, reset_policy_clears_on_expiry(policy)


def replay_ml_enabled(params: dict[str, Any], ml_data_available: bool, mechanism_active: bool) -> bool:
    """Respect the explicit ML switch even when non-ML guards are active."""
    return bool(params.get("ml_enabled", True)) and bool(ml_data_available) and bool(mechanism_active)


def terminal_pnl_decomposition(
    cash: float,
    inventory: float,
    mark_price: float,
    taker_fee: float,
) -> tuple[float, float, float]:
    """Return terminal MTM, applied fee drag, and final PnL.

    Window end is an accounting mark, not a synthetic taker liquidation.  The
    taker fee remains an input so callers can report a separate liquidation
    estimate, but no fee is applied to the replay PnL here.
    """
    del taker_fee
    terminal_mtm_pnl = float(cash) + float(inventory) * float(mark_price)
    terminal_fee_drag = 0.0
    return terminal_mtm_pnl, terminal_fee_drag, terminal_mtm_pnl


def _load_live_perf_latency_samples(path: "Path", mode: str = "avg") -> dict[str, "np.ndarray"]:
    """Load historical, per-telemetry-row REST-duration proxies.

    ``avg`` returns one sum/count value per row, not the original per-request
    distribution. Rows containing several calls lose their within-row tails.
    ``max`` and ``sum`` are alternative aggregate proxies, not recovered request
    samples. None measures exchange effectiveness or private ACK visibility;
    using these legacy arrays for those clocks is an explicit modeling
    assumption, not a measured one-way delay. In particular, do not halve RTT.

    Async gateways record request timing separately in order_gateway_receipts;
    zero REST counters here do not mean zero network latency. Preserve this
    loader for historical reproduction, not as the current async profile.
    """
    if not path.exists():
        raise FileNotFoundError(f"Live perf telemetry not found: {path}")
    df = pd.read_csv(path)
    samples: dict[str, np.ndarray] = {}

    def _series(prefix: str) -> np.ndarray:
        count_col = f"{prefix}_count"
        sum_col = f"{prefix}_sum_us"
        max_col = f"{prefix}_max_us"
        if mode == "max":
            src = pd.to_numeric(df.get(max_col), errors="coerce")
        elif mode == "sum":
            src = pd.to_numeric(df.get(sum_col), errors="coerce")
        else:
            sums = pd.to_numeric(df.get(sum_col), errors="coerce")
            counts = pd.to_numeric(df.get(count_col), errors="coerce")
            src = sums / counts.replace(0, np.nan)
        arr = src.replace([np.inf, -np.inf], np.nan).dropna().to_numpy(dtype=np.float64)
        arr = arr[arr > 0.0] / 1000.0
        return np.ascontiguousarray(arr, dtype=np.float64)

    samples["new_order_latency_samples_ms"] = _series("rest_new")
    samples["cancel_order_latency_samples_ms"] = _series("rest_cancel")
    if samples["new_order_latency_samples_ms"].size == 0:
        raise ValueError(f"No rest_new latency samples in {path}")
    if samples["cancel_order_latency_samples_ms"].size == 0:
        raise ValueError(f"No rest_cancel latency samples in {path}")
    return samples


def _load_exec_book_visibility_profile(path: "Path") -> dict[str, Any]:
    """Load an environment-specific execution-book visibility-age profile.

    Quote-decision logs expose ``depth_age_s`` while live performance telemetry
    exposes ``exec_trade_age_s``, ``exec_book_age_s``, and
    ``exec_depth_age_s``. These are receive-time ages measured by the live
    process. Replay samples them deterministically and moves only the strategy's
    visible signal/BBO/L2 cursors backward; exchange-time queue and fill truth
    stay current.
    """
    if not path.exists():
        raise FileNotFoundError(f"Execution-book visibility profile not found: {path}")
    frame = pd.read_csv(path)
    if "event" in frame.columns:
        frame = frame[frame["event"].astype(str).str.lower() == "requote"].copy()
    if "status" in frame.columns:
        frame = frame[frame["status"].astype(str).str.lower() == "ok"].copy()
    age_col = next(
        (name for name in ("depth_age_s", "exec_book_age_s") if name in frame.columns),
        None,
    )
    if age_col is None:
        raise ValueError(
            f"{path} has neither depth_age_s nor exec_book_age_s"
        )
    # Live telemetry is appended after synchronous order updates.  Its
    # ``timestamp`` and source ages therefore describe the end of the requote,
    # while ``mid`` and side-policy inputs were frozen just before
    # ``_update_orders`` started.  Move both the observation boundary and the
    # measured ages back by that duration.  Quote-decision CSVs do not expose
    # this timing field and retain their existing timestamp semantics.
    stage_lag_ms = (
        pd.to_numeric(frame["update_orders_us"], errors="coerce") / 1000.0
        if "update_orders_us" in frame.columns
        else pd.Series(0.0, index=frame.index, dtype=np.float64)
    )
    stage_lag_ms = stage_lag_ms.replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(lower=0.0)
    ages_ms_series = (
        pd.to_numeric(frame[age_col], errors="coerce") * 1000.0 - stage_lag_ms
    )
    ages_ms = (
        ages_ms_series.replace([np.inf, -np.inf], np.nan)
        .dropna()
        .clip(lower=0.0)
        .to_numpy(dtype=np.float64)
    )
    ages_ms = np.ascontiguousarray(ages_ms, dtype=np.float64)
    if ages_ms.size == 0:
        raise ValueError(f"No finite execution-book age samples in {path}")
    p10, p90 = np.quantile(ages_ms, [0.10, 0.90])
    output = {
        "exec_book_visibility_delay_samples_ms": ages_ms,
        "exec_book_visibility_delay_mean_ms": float(np.mean(ages_ms)),
        "exec_book_visibility_delay_jitter_ms": float((p90 - p10) * 0.5),
        "exec_book_visibility_delay_p10_ms": float(p10),
        "exec_book_visibility_delay_p50_ms": float(np.quantile(ages_ms, 0.50)),
        "exec_book_visibility_delay_p90_ms": float(p90),
        "exec_book_visibility_delay_p99_ms": float(np.quantile(ages_ms, 0.99)),
        "exec_book_visibility_age_column": age_col,
        "exec_book_visibility_stage_lag_mean_ms": float(stage_lag_ms.mean()),
        "exec_book_visibility_timestamp_semantics": (
            "pre_order_update" if "update_orders_us" in frame.columns else "observed"
        ),
    }
    if "timestamp" not in frame.columns:
        return output

    timestamps = pd.to_numeric(frame["timestamp"], errors="coerce")
    finite_ts = timestamps.replace([np.inf, -np.inf], np.nan).notna()
    paired = frame.loc[finite_ts].copy()
    timestamps = timestamps.loc[finite_ts].to_numpy(dtype=np.float64)
    if paired.empty:
        return output
    scale = 1000.0 if float(np.nanmedian(np.abs(timestamps))) < 1e12 else 1.0
    paired["_stage_lag_ms"] = stage_lag_ms.loc[finite_ts].to_numpy(dtype=np.float64)
    paired["_timestamp_ms"] = np.rint(
        timestamps * scale - paired["_stage_lag_ms"].to_numpy(dtype=np.float64)
    ).astype(np.int64)

    book_age_col = (
        "exec_book_age_s" if "exec_book_age_s" in paired.columns else age_col
    )
    depth_age_col = next(
        (
            name
            for name in ("exec_depth_age_s", "depth_age_s", book_age_col)
            if name in paired.columns
        ),
        book_age_col,
    )
    trade_age_col = (
        "exec_trade_age_s"
        if "exec_trade_age_s" in paired.columns
        else book_age_col
    )
    paired["_book_delay_ms"] = (
        pd.to_numeric(paired[book_age_col], errors="coerce") * 1000.0
        - paired["_stage_lag_ms"]
    )
    paired["_depth_delay_ms"] = (
        pd.to_numeric(paired[depth_age_col], errors="coerce") * 1000.0
        - paired["_stage_lag_ms"]
    )
    paired["_trade_delay_ms"] = (
        pd.to_numeric(paired[trade_age_col], errors="coerce") * 1000.0
        - paired["_stage_lag_ms"]
    )
    paired["_observed_mid"] = (
        pd.to_numeric(paired["mid"], errors="coerce")
        if "mid" in paired.columns
        else np.nan
    )
    paired = paired.replace([np.inf, -np.inf], np.nan)
    # A source can update while synchronous REST work is in flight.  In that
    # case its end-of-requote age implies a source timestamp after the frozen
    # policy boundary.  Do not discard the whole paired observation (the depth
    # source and captured mid may still be valid); clamp that source to the
    # decision boundary independently.
    paired["_book_delay_ms"] = paired["_book_delay_ms"].clip(lower=0.0)
    paired = paired[
        paired["_book_delay_ms"].notna()
    ]
    if paired.empty:
        return output
    paired["_depth_delay_ms"] = paired["_depth_delay_ms"].fillna(
        paired["_book_delay_ms"]
    ).clip(lower=0.0)
    paired["_trade_delay_ms"] = paired["_trade_delay_ms"].fillna(
        paired["_book_delay_ms"]
    ).clip(lower=0.0)
    paired = (
        paired.groupby("_timestamp_ms", as_index=False)
        .agg(
            _book_delay_ms=("_book_delay_ms", "mean"),
            _depth_delay_ms=("_depth_delay_ms", "mean"),
            _trade_delay_ms=("_trade_delay_ms", "mean"),
            _observed_mid=("_observed_mid", "median"),
        )
        .sort_values("_timestamp_ms")
    )
    output.update(
        {
            "exec_book_visibility_paired_ts_ms": np.ascontiguousarray(
                paired["_timestamp_ms"].to_numpy(dtype=np.int64)
            ),
            "exec_book_visibility_paired_delay_ms": np.ascontiguousarray(
                paired["_book_delay_ms"].to_numpy(dtype=np.float64)
            ),
            "exec_depth_visibility_paired_delay_ms": np.ascontiguousarray(
                paired["_depth_delay_ms"].to_numpy(dtype=np.float64)
            ),
            "exec_trade_visibility_paired_delay_ms": np.ascontiguousarray(
                paired["_trade_delay_ms"].to_numpy(dtype=np.float64)
            ),
            "exec_book_visibility_paired_mid": np.ascontiguousarray(
                paired["_observed_mid"].to_numpy(dtype=np.float64)
            ),
            "exec_book_visibility_paired_count": int(len(paired)),
            "exec_book_visibility_book_age_column": book_age_col,
            "exec_book_visibility_depth_age_column": depth_age_col,
            "exec_book_visibility_trade_age_column": trade_age_col,
        }
    )
    return output


def _splitmix64(value: int) -> int:
    """Return a stable 64-bit mix shared with the C++ replay."""
    mask = (1 << 64) - 1
    z = (int(value) + 0x9E3779B97F4A7C15) & mask
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & mask
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & mask
    return (z ^ (z >> 31)) & mask


LATENCY_SAMPLER_VERSION = "keyed_splitmix64_v1"
_LATENCY_NEW = 1
_LATENCY_CANCEL = 2
_LATENCY_TTL_CANCEL = 3
_LATENCY_QUEUE_VALUE_CANCEL = 4
_LATENCY_PRIVATE_FILL = 5
_LATENCY_DECISION_TO_GATEWAY = 6
_REST_GATEWAY_SLOT_NAMES = (
    "cancel_buy",
    "cancel_sell",
    "new_buy",
    "new_sell",
)
RANDOM_PASSIVE_SAMPLER_VERSION = "keyed_splitmix64_v1"
_RANDOM_PASSIVE_TIMING_JITTER = 1
_RANDOM_PASSIVE_SIDE_MIRROR = 2


def _deterministic_decision_to_gateway_latency_ms(
    samples_ms: "np.ndarray",
    *,
    seed: int,
    decision_ts_ms: int,
) -> int:
    """Sample one compute delay shared by every request in a decision.

    The keyed draw is deliberately independent of request side/slot.  This
    keeps cancel BUY, cancel SELL, new BUY, and new SELL on one frozen
    decision snapshot while shifting the entire serial gateway row together.
    """

    if samples_ms.size == 0:
        return 0
    mixed = int(seed)
    for component in (int(decision_ts_ms), _LATENCY_DECISION_TO_GATEWAY):
        mixed = _splitmix64(mixed ^ component)
    sample = max(0.0, float(samples_ms[int(mixed % samples_ms.size)]))
    return max(0, int(math.floor(sample + 0.5)))


def next_no_poll_wait_ms(now_ms: int, deadlines) -> int:
    """Earliest future causal deadline, bounded by idle safety maintenance.

    No mandatory 100ms delay is added to a ready input or quote deadline.
    Past/equal deadlines were handled by the just-completed iteration; pending
    same-time lifecycle continuations remain owned by the normal event loop.
    """
    return min(
        [int(now_ms) + 100]
        + [int(math.ceil(value)) for value in deadlines
           if value is not None and math.isfinite(value) and value > now_ms]
    )


class LocalLifecycleBoundaryScheduler:
    """Deterministic millisecond scheduler for locally visible lifecycle events.

    The scheduler is deliberately independent from the native exchange-book
    clock.  Callers own the event payload and decide how to apply it; this
    class only provides exact boundary ordering and idempotent registration.
    """

    _PHASE_PRIORITY = {
        "exchange_effective": 10,
        "new_ack": 20,
        "private_fill_visible": 30,
        "cancel_ack": 40,
        "rest_return": 50,
    }

    def __init__(self) -> None:
        self._heap: list[tuple[int, int, int, str, Any]] = []
        self._scheduled_ids: set[str] = set()
        self._sequence = 0

    def schedule(
        self,
        *,
        ts_ms: int,
        phase: str,
        event_id: str,
        payload: Any = None,
    ) -> bool:
        normalized_phase = str(phase)
        if normalized_phase not in self._PHASE_PRIORITY:
            raise ValueError(f"unsupported local lifecycle phase={phase!r}")
        normalized_id = str(event_id)
        if not normalized_id:
            raise ValueError("local lifecycle event_id must be non-empty")
        if normalized_id in self._scheduled_ids:
            return False
        timestamp = int(ts_ms)
        if timestamp < 0:
            raise ValueError("local lifecycle timestamp must be non-negative")
        heapq.heappush(
            self._heap,
            (
                timestamp,
                int(self._PHASE_PRIORITY[normalized_phase]),
                int(self._sequence),
                normalized_id,
                payload,
            ),
        )
        self._sequence += 1
        self._scheduled_ids.add(normalized_id)
        return True

    def drain(self, *, through_ts_ms: int, inclusive: bool) -> list[dict[str, Any]]:
        boundary = int(through_ts_ms)
        drained: list[dict[str, Any]] = []
        while self._heap:
            ts_ms, priority, sequence, event_id, payload = self._heap[0]
            if ts_ms > boundary or (ts_ms == boundary and not inclusive):
                break
            heapq.heappop(self._heap)
            drained.append(
                {
                    "ts_ms": int(ts_ms),
                    "priority": int(priority),
                    "sequence": int(sequence),
                    "event_id": str(event_id),
                    "payload": payload,
                }
            )
        return drained

    def __bool__(self) -> bool:
        return bool(self._heap)

    def next_timestamp(self) -> Optional[int]:
        return int(self._heap[0][0]) if self._heap else None


def _load_serial_rest_gateway_timing_profile(path: "Path") -> dict[str, Any]:
    """Load the diagnostic row-paired REST gateway timing profile.

    One row is an observed synchronous request subsequence.  Missing slots and
    missing clocks stay missing; callers may only consume an exact observed
    request mask, so this loader never manufactures a component latency.
    """

    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(
            f"serial REST gateway timing profile not found: {resolved}"
        )
    required = {
        "slot_names",
        "request_present_mask",
        "request_start_offset_ms",
        "exchange_effective_observed_mask",
        "exchange_effective_latency_ms",
        "local_visibility_observed_mask",
        "local_visibility_latency_ms",
    }
    with np.load(resolved, allow_pickle=False) as archive:
        missing = required.difference(archive.files)
        if missing:
            raise ValueError(
                "serial REST gateway timing profile missing arrays: "
                f"{sorted(missing)}"
            )
        slot_names = tuple(str(value) for value in archive["slot_names"].tolist())
        if slot_names != _REST_GATEWAY_SLOT_NAMES:
            raise ValueError(
                "serial REST gateway slot order must be "
                f"{_REST_GATEWAY_SLOT_NAMES}, got {slot_names}"
            )
        profile = {
            "path": str(resolved),
            "request_present_mask": np.asarray(
                archive["request_present_mask"], dtype=np.bool_
            ).copy(),
            "request_start_offset_ms": np.asarray(
                archive["request_start_offset_ms"], dtype=np.float64
            ).copy(),
            "exchange_effective_observed_mask": np.asarray(
                archive["exchange_effective_observed_mask"], dtype=np.bool_
            ).copy(),
            "exchange_effective_latency_ms": np.asarray(
                archive["exchange_effective_latency_ms"], dtype=np.float64
            ).copy(),
            "local_visibility_observed_mask": np.asarray(
                archive["local_visibility_observed_mask"], dtype=np.bool_
            ).copy(),
            "local_visibility_latency_ms": np.asarray(
                archive["local_visibility_latency_ms"], dtype=np.float64
            ).copy(),
        }
        # Optional measured response bounds are separate from private-stream
        # visibility. Older paired profiles intentionally retain their ABI.
        for name in (
            "rest_completion_upper_bound_observed_mask",
            "rest_completion_upper_bound_by_next_request_ms",
            "completion_source_callback_type",
        ):
            if name in archive.files:
                profile[name] = np.asarray(archive[name]).copy()

    present = profile["request_present_mask"]
    expected_shape = present.shape
    if present.ndim != 2 or expected_shape[1:] != (len(_REST_GATEWAY_SLOT_NAMES),):
        raise ValueError(
            "serial REST gateway request_present_mask must have shape [N,4]"
        )
    if expected_shape[0] == 0:
        raise ValueError("serial REST gateway timing profile has no rows")
    for name in (
        "request_start_offset_ms",
        "exchange_effective_observed_mask",
        "exchange_effective_latency_ms",
        "local_visibility_observed_mask",
        "local_visibility_latency_ms",
    ):
        if profile[name].shape != expected_shape:
            raise ValueError(
                f"serial REST gateway {name} must have shape {expected_shape}"
            )
    complete = (
        present
        & profile["exchange_effective_observed_mask"]
        & profile["local_visibility_observed_mask"]
        & np.isfinite(profile["request_start_offset_ms"])
        & np.isfinite(profile["exchange_effective_latency_ms"])
        & np.isfinite(profile["local_visibility_latency_ms"])
        & (profile["request_start_offset_ms"] >= 0.0)
        & (profile["exchange_effective_latency_ms"] >= 0.0)
        & (profile["local_visibility_latency_ms"] >= 0.0)
    )
    if np.any(present & ~complete):
        raise ValueError(
            "serial REST gateway profile has a present request without complete "
            "start/effective/local-visibility clocks"
        )
    for row_index in range(expected_shape[0]):
        offsets = profile["request_start_offset_ms"][row_index, present[row_index]]
        if offsets.size > 1 and np.any(np.diff(offsets) < 0.0):
            raise ValueError(
                "serial REST gateway request starts regress within row "
                f"{row_index}"
            )
    return profile


def _sample_serial_rest_gateway_timing_row(
    profile: Mapping[str, Any],
    *,
    decision_ts_ms: int,
    request_origin_ts_ms: Optional[int] = None,
    requested_slots: tuple[str, ...],
    seed: int,
) -> dict[str, dict[str, int]]:
    """Select one exact-mask empirical row and retain all paired clocks."""

    unknown = set(requested_slots).difference(_REST_GATEWAY_SLOT_NAMES)
    if unknown:
        raise ValueError(f"unknown serial REST gateway slots: {sorted(unknown)}")
    requested = np.asarray(
        [name in requested_slots for name in _REST_GATEWAY_SLOT_NAMES],
        dtype=np.bool_,
    )
    if not np.any(requested):
        return {}
    present = np.asarray(profile["request_present_mask"], dtype=np.bool_)
    candidates = np.flatnonzero(np.all(present == requested, axis=1))
    if candidates.size == 0:
        mask = "-".join(
            name
            for name, enabled in zip(
                _REST_GATEWAY_SLOT_NAMES,
                requested,
                strict=True,
            )
            if enabled
        )
        raise ValueError(
            "serial REST gateway profile has no exact observed request mask "
            f"for {mask}"
        )
    mask_bits = sum(
        (1 << index) for index, enabled in enumerate(requested) if enabled
    )
    mixed = _splitmix64(int(decision_ts_ms) ^ int(seed) ^ int(mask_bits))
    row_index = int(candidates[int(mixed % int(candidates.size))])
    result: dict[str, dict[str, int]] = {}
    for slot_index, slot_name in enumerate(_REST_GATEWAY_SLOT_NAMES):
        if not requested[slot_index]:
            continue
        request_offset_ms = max(
            0,
            int(
                math.floor(
                    float(profile["request_start_offset_ms"][row_index, slot_index])
                    + 0.5
                )
            ),
        )
        effective_latency_ms = max(
            0,
            int(
                math.floor(
                    float(
                        profile["exchange_effective_latency_ms"][
                            row_index, slot_index
                        ]
                    )
                    + 0.5
                )
            ),
        )
        local_latency_ms = max(
            effective_latency_ms,
            int(
                math.floor(
                    float(
                        profile["local_visibility_latency_ms"][
                            row_index, slot_index
                        ]
                    )
                    + 0.5
                )
            ),
        )
        request_origin = (
            int(decision_ts_ms)
            if request_origin_ts_ms is None
            else int(request_origin_ts_ms)
        )
        request_ts_ms = request_origin + request_offset_ms
        result[slot_name] = {
            "row_index": row_index,
            "request_ts_ms": request_ts_ms,
            "exchange_effective_ts_ms": request_ts_ms + effective_latency_ms,
            "local_visibility_ts_ms": request_ts_ms + local_latency_ms,
        }
    return result


def deterministic_random_passive_unit(
    *,
    seed: int,
    event_ts_ms: int,
    action_identity: int,
    operation: int,
) -> float:
    """Return an action-keyed unit draw shared with C++ random-passive replay."""
    if int(operation) not in {
        _RANDOM_PASSIVE_TIMING_JITTER,
        _RANDOM_PASSIVE_SIDE_MIRROR,
    }:
        raise ValueError("random-passive operation must be within [1, 2]")
    mask = (1 << 64) - 1
    mixed = int(seed) & mask
    for component in (
        int(event_ts_ms),
        int(action_identity),
        int(operation),
    ):
        mixed = _splitmix64(mixed ^ (component & mask))
    draw = _splitmix64(mixed ^ 0xA0761D6478BD642F)
    return float(draw >> 11) / float(1 << 53)


def _validate_replayed_live_actions(params: Mapping[str, Any]) -> None:
    if bool(params.get("dynamic_fill_hazard_action_enabled", False)):
        required = (
            "dynamic_fill_hazard_shadow_model_path",
            "dynamic_fill_hazard_action_policy_path",
        )
        missing = [name for name in required if not str(params.get(name, "")).strip()]
        if missing:
            raise ValueError(
                "dynamic fill-hazard replay is missing frozen artifacts: "
                + ", ".join(missing)
            )


def _sync_degrade_events_for_replay(
    params: Mapping[str, Any],
    trades_df: "pd.DataFrame",
):
    mode = str(params.get("sync_adjust_replay_mode", "disabled") or "disabled")
    raw_path = str(params.get("sync_adjust_event_tape_path", "") or "").strip()
    tape_path = None
    if raw_path:
        tape_path = Path(raw_path).expanduser()
        if not tape_path.is_absolute():
            tape_path = ROOT / tape_path
    start_ts = (
        int(trades_df["transact_time"].iloc[0]) if not trades_df.empty else None
    )
    end_ts = (
        int(trades_df["transact_time"].iloc[-1]) if not trades_df.empty else None
    )
    return load_sync_degrade_events(
        mode=mode,
        tape_path=tape_path,
        expected_sha256=str(
            params.get("sync_adjust_event_tape_sha256", "") or ""
        ),
        expected_environment=str(
            params.get("sync_adjust_event_environment", "") or ""
        ),
        start_ts_ms=start_ts,
        end_ts_ms=end_ts,
        stress_seed=int(params.get("sync_adjust_stress_seed", 20260729) or 0),
        stress_interval_s=float(
            params.get("sync_adjust_stress_interval_s", 21_600.0) or 21_600.0
        ),
    )


def deterministic_latency_ms(
    *,
    base_ms: int,
    jitter_ms: int,
    samples_ms: "np.ndarray",
    seed: int,
    event_ts_ms: int,
    side: str,
    operation: int,
    order_ts_ms: int = 0,
    stress_enabled: bool = False,
    stress_spike_probability: float = 0.0,
    stress_spike_multiplier: float = 1.0,
    samples_validated: bool = False,
) -> int:
    """Sample latency from an action-keyed path shared by arms and C++.

    Sequential PRNG consumption makes two arms see different latency after the
    first action-path divergence.  This sampler keys each draw by exogenous
    decision time, side, operation and originating quote time instead.
    """
    mask = (1 << 64) - 1
    side_code = 1 if str(side).upper() == "BUY" else 2
    mixed = int(seed) & mask
    for component in (
        int(event_ts_ms),
        int(order_ts_ms),
        int(side_code),
        int(operation),
    ):
        mixed = _splitmix64(mixed ^ (component & mask))

    samples = np.asarray(samples_ms, dtype=np.float64).ravel()
    if not samples_validated:
        samples = samples[np.isfinite(samples) & (samples >= 0.0)]
    if samples.size:
        latency = float(samples[int(mixed % int(samples.size))])
    else:
        latency = float(max(0, int(base_ms)))

    jitter_draw = _splitmix64(mixed ^ 0xD1B54A32D192ED03)
    unit = float(jitter_draw >> 11) / float(1 << 53)
    if int(jitter_ms) > 0:
        latency += (2.0 * unit - 1.0) * float(jitter_ms)

    if stress_enabled and float(stress_spike_probability) > 0.0:
        spike_draw = _splitmix64(mixed ^ 0x94D049BB133111EB)
        spike_unit = float(spike_draw >> 11) / float(1 << 53)
        if spike_unit < float(stress_spike_probability):
            latency *= max(1.0, float(stress_spike_multiplier))

    return max(0, int(math.floor(max(0.0, latency) + 0.5)))


def _exec_book_visibility_delay_ms(
    decision_ts_ms: int,
    *,
    mean_ms: float,
    jitter_ms: float,
    seed: int,
    samples_ms: Optional["np.ndarray"] = None,
) -> int:
    """Sample a reproducible receive-time visibility age for one decision."""
    mixed = _splitmix64(int(decision_ts_ms) ^ int(seed))
    if samples_ms is not None and samples_ms.size > 0:
        delay = float(samples_ms[mixed % int(samples_ms.size)])
    else:
        unit = float(mixed >> 11) * (1.0 / float(1 << 53))
        delay = float(mean_ms) + (2.0 * unit - 1.0) * float(jitter_ms)
    return max(0, int(round(delay)))


def _paired_exec_book_visibility_state(
    decision_ts_ms: int,
    *,
    timestamps_ms: "np.ndarray",
    book_delays_ms: "np.ndarray",
    depth_delays_ms: "np.ndarray",
    trade_delays_ms: "np.ndarray",
    observed_mid: "np.ndarray",
    max_gap_ms: int,
) -> Optional[tuple[int, int, int, float]]:
    """Return the exact/preceding live decision state within a bounded gap."""
    if timestamps_ms.size == 0:
        return None
    idx = int(np.searchsorted(timestamps_ms, int(decision_ts_ms), side="right") - 1)
    if idx < 0 or int(decision_ts_ms) - int(timestamps_ms[idx]) > int(max_gap_ms):
        return None
    return (
        max(0, int(round(float(book_delays_ms[idx])))),
        max(0, int(round(float(depth_delays_ms[idx])))),
        max(0, int(round(float(trade_delays_ms[idx])))),
        float(observed_mid[idx]),
    )


def _sampled_joint_exec_book_visibility_state(
    decision_ts_ms: int,
    *,
    seed: int,
    book_delays_ms: "np.ndarray",
    depth_delays_ms: "np.ndarray",
    trade_delays_ms: "np.ndarray",
) -> tuple[int, int, int]:
    """Sample one aligned live-perf source-age row without timestamp matching."""
    lengths = {
        book_delays_ms.size,
        depth_delays_ms.size,
        trade_delays_ms.size,
    }
    if len(lengths) != 1 or 0 in lengths:
        raise ValueError(
            "sampled_joint visibility requires aligned book/depth/trade age arrays"
        )
    mixed = _splitmix64(int(decision_ts_ms) ^ int(seed))
    idx = int(mixed % int(book_delays_ms.size))
    return (
        max(0, int(round(float(book_delays_ms[idx])))),
        max(0, int(round(float(depth_delays_ms[idx])))),
        max(0, int(round(float(trade_delays_ms[idx])))),
    )


def _advance_monotonic_visibility_cutoff(
    cutoffs_ms: dict[str, int],
    *,
    feed: str,
    candidate_ts_ms: int,
) -> int:
    """Advance one receive-stream cutoff without forgetting visible events."""
    name = str(feed)
    candidate = int(candidate_ts_ms)
    previous = cutoffs_ms.get(name)
    visible = candidate if previous is None else max(int(previous), candidate)
    cutoffs_ms[name] = visible
    return visible


def _source_stratified_exec_visibility_state(
    decision_ts_ms: int,
    *,
    seed: int,
    simulator: Any,
    market_id: str,
    transport: str,
) -> tuple[int, int, int]:
    """Sample joint book/trade transport visibility without event-age sparsity."""
    bucket_ms = int(simulator.source_stratified_bucket_ms)
    mixed = _splitmix64((int(decision_ts_ms) // bucket_ms) ^ int(seed))
    common = {
        "market_id": str(market_id),
        "transport": str(transport).strip().lower(),
    }
    book_delay_ms = simulator.delay_ms(
        {**common, "event_type": "book"},
        mode="profile_source_stratified",
        rng=random.Random(int(mixed)),
    )
    trade_delay_ms = simulator.delay_ms(
        {**common, "event_type": "trade"},
        mode="profile_source_stratified",
        rng=random.Random(int(mixed)),
    )
    book_delay = max(0, int(round(float(book_delay_ms))))
    return (
        book_delay,
        book_delay,
        max(0, int(round(float(trade_delay_ms)))),
    )


def _load_exec_source_stratified_visibility_profile(
    path: "Path",
    *,
    expected_profile_id: str,
    market_id: str,
    transport: str,
) -> tuple[Any, str]:
    """Load a diagnostic profile by its schema, sampling scope and profile ID."""
    from research.system_engineering.audit.market_data_latency import (
        MarketDataLatencySimulator,
    )

    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(
            f"source-stratified market-data latency profile not found: {resolved}"
        )
    actual_sha = hashlib.sha256(resolved.read_bytes()).hexdigest()
    expected_id = str(expected_profile_id).strip()
    source_market = str(market_id).strip()
    source_transport = str(transport).strip().lower()
    if not expected_id or not source_market or not source_transport:
        raise ValueError(
            "source-stratified latency profile requires profile-id, market, and transport"
        )
    simulator = MarketDataLatencySimulator.load(resolved)
    if simulator.profile_id != expected_id:
        raise ValueError(
            "source-stratified latency profile id mismatch: "
            f"expected={expected_id} actual={simulator.profile_id}"
        )
    sampling = simulator.profile.get("source_stratified_sampling", {})
    if (
        str(sampling.get("authority", "")) != "diagnostic_non_authoritative"
        or sampling.get("promotion_eligible") is not False
    ):
        raise ValueError(
            "source-stratified execution visibility requires a diagnostic, "
            "non-promotional profile"
        )
    _source_stratified_exec_visibility_state(
        0,
        seed=0,
        simulator=simulator,
        market_id=source_market,
        transport=source_transport,
    )
    return simulator, actual_sha


def _paired_exec_source_visible_ts(
    observation_ts_ms: int,
    *,
    timestamps_ms: "np.ndarray",
    source_delays_ms: "np.ndarray",
) -> Optional[int]:
    """Carry the latest measured source-event boundary between observations."""
    if timestamps_ms.size == 0 or source_delays_ms.size == 0:
        return None
    idx = int(
        np.searchsorted(timestamps_ms, int(observation_ts_ms), side="right") - 1
    )
    if idx < 0 or idx >= source_delays_ms.size:
        return None
    return int(
        timestamps_ms[idx] - round(float(source_delays_ms[idx]))
    )


def _load_live_requote_clock(path: "Path") -> dict[str, "np.ndarray"]:
    """Load an empirical live-parity clock from requote telemetry.

    Action 2 is a successful requote and action 3 is a safety block that
    cancels resting orders without evaluating a new quote.  These codes are
    carried in synthetic rows' ``is_buyer_maker`` byte; real executions retain
    the exchange's normal 0/1 maker-side flag.
    """
    if not path.exists():
        raise FileNotFoundError(f"Live perf telemetry not found: {path}")
    available = set(pd.read_csv(path, nrows=0).columns)
    timing_columns = [
        name
        for name in ("update_orders_us", "rest_cancel_all_sum_us")
        if name in available
    ]
    frame = pd.read_csv(
        path,
        usecols=["timestamp", "event", "status", *timing_columns],
    )
    frame = frame[frame["event"].astype(str).str.lower() == "requote"].copy()
    timestamp = pd.to_numeric(frame["timestamp"], errors="coerce")
    status = frame["status"].astype(str).str.lower()
    valid = timestamp.notna() & status.isin(("ok", "stale_book", "risk_block"))
    frame = frame.loc[valid].copy()
    timestamp = timestamp.loc[valid]
    status = status.loc[valid]
    observed_ts_ms = np.rint(
        timestamp.to_numpy(dtype=np.float64) * 1000.0
    ).astype(np.int64)
    stage_lag_ms = np.zeros(len(frame), dtype=np.float64)
    ok = status.to_numpy() == "ok"
    if "update_orders_us" in frame.columns:
        update_ms = (
            pd.to_numeric(frame["update_orders_us"], errors="coerce")
            .fillna(0.0)
            .clip(lower=0.0)
            .to_numpy(dtype=np.float64)
            / 1000.0
        )
        stage_lag_ms[ok] = update_ms[ok]
    if "rest_cancel_all_sum_us" in frame.columns:
        cancel_ms = (
            pd.to_numeric(frame["rest_cancel_all_sum_us"], errors="coerce")
            .fillna(0.0)
            .clip(lower=0.0)
            .to_numpy(dtype=np.float64)
            / 1000.0
        )
        stage_lag_ms[~ok] = cancel_ms[~ok]
    ts_ms = np.rint(observed_ts_ms - stage_lag_ms).astype(np.int64)
    action = np.where(status.to_numpy() == "ok", 2, 3).astype(np.uint8)
    order = np.argsort(ts_ms, kind="stable")
    return {
        "requote_clock_ts_ms": np.ascontiguousarray(ts_ms[order]),
        "requote_clock_action": np.ascontiguousarray(action[order]),
        "requote_clock_observed_ts_ms": np.ascontiguousarray(observed_ts_ms[order]),
        "requote_clock_stage_lag_ms": np.ascontiguousarray(stage_lag_ms[order]),
        "requote_clock_timestamp_semantics": "pre_order_update",
    }

from itertools import product

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from models.active_order_queue import ActiveOrderQueueCoverageError
from models.backtest_utils import attach_selection_scores as _attach_selection_scores, default_backtest_workers
from research.families.f09_inventory_lifecycle_action_uplift.causal_path_features import compute_causal_path_features
from models.queue_calibration import (
    build_daily_queue_arrays,
    build_queue_deplete_lookup,
    build_queue_mo_lookup,
    build_queue_regime_lookup,
    lookup_queue_deplete_multiplier,
    lookup_queue_mo_multiplier,
    lookup_queue_regime_multiplier,
    reason_bucket_from_flags,
)
from models.symbol_paths import ROOT, DEFAULT_SYMBOL, data_root, paths_for
from research.families.f05_fill_quality_quote_ev.quote_ev import materialize_quote_ev_feature_values
from models.replay_policies import (
    LOCAL_ACTIONS,
    apply_local_add_action,
    choose_action,
    normalize_action_probabilities,
)
from models.replay_policies import (
    QUEUE_VALUE_CANCEL_REENTER_ACTIONS,
    QUEUE_VALUE_KEEP_CANCEL_ACTIONS,
    choose_queue_value_cancel_reenter_action,
    choose_queue_value_action,
    normalize_queue_value_cancel_reenter_probabilities,
    normalize_queue_value_probabilities,
)
from models.replay_policies import (
    SAFE_ADD_REARM_ACTIONS,
    apply_safe_add_rearm_action,
    choose_safe_add_rearm_action,
    normalize_safe_add_rearm_probabilities,
)
from models.replay_policies import (
    INVENTORY_LIFECYCLE_STOP_ADD_ACTIONS,
    SELL_ADD_SKIP_ACTIONS,
    choose_inventory_lifecycle_stop_add_action,
    choose_sell_add_skip_action,
    normalize_inventory_lifecycle_stop_add_probabilities,
    normalize_sell_add_skip_probabilities,
)
from models.replay_policies import (
    STATE_CONDITIONED_REARM_ACTIONS,
    RecoveryEventSpec,
    StateConditionedRearmSpec,
    choose_state_conditioned_rearm_action,
    evaluate_recovery_event,
    evaluate_state_conditioned_rearm,
    normalize_state_conditioned_rearm_probabilities,
)
from research.families.f07_active_order_continuation.audit.queue_value_models import (
    EVENT_COLUMNS,
    NATIVE_EXCHANGE_EVENT_COLUMNS,
    EmpiricalMicropriceArtifact,
    QueueReactiveHawkesArtifact,
    QueueReactiveRuntime,
    QueueValueModelBundle,
    QueueValueStateConfig,
    QueueValueStateEvaluator,
)
from research.families.f07_active_order_continuation.audit.queue_value_competing_risk import (
    CompetingRiskBundle,
    QueueValueNetEvaluator,
)
from models.audit.order_lifecycle import OrderLifecycleRecorder
from models.tick_data_types import HistoricalBBOData, HistoricalL2Data, book_observation_times_us, book_observation_status, book_usable_mask
from strategy.fill_selection_model import (
    FillSelectionScoreEnsemble,
    build_fill_selection_feature_row,
    fill_selection_actionable,
)

from data_paths import daily_market_path, normalized_l2_root

from strategy.policy_guards import (
    CommonSidePolicyInput,
    LocalExtremeGuardConfig,
    POLICY_REASON_BURST,
    POLICY_REASON_DEFENSE,
    POLICY_REASON_FILL_COOLDOWN,
    POLICY_REASON_INV_LIMIT,
    POLICY_REASON_STALE_HARD,
    apply_local_extreme_guard_context,
    evaluate_common_side_policy,
)

from strategy.state_conditioned_quote_policy import (
    StateConditionedQuotePolicy,
    inventory_role_for_quote,
)

from data_quality import (
    allowed_timestamp_mask,
    continuous_segment_ids,
    filter_frame_for_orderbook_quality,
    filter_paths_for_orderbook_quality,
)

from strategy.quote_core import (
    DepthSnapshot,
    QuotePrediction,
    QuoteState,
    QUOTE_CORE_UNIT_ABI_FIELDS,
    SPREAD_CAP_COMPRESS,
    SPREAD_CAP_PAUSE_EXPOSURE,
    apply_p3_side_bbo_floor,
    apply_final_spread_cap,
    apply_final_spread_cap_preserve_side,
    trade_intensity_acceleration_inventory_role_for_target,
    circuit_breaker_triggered,
    compose_trade_intensity_acceleration_add_only_quote,
    compute_quote_core,
    _CPP_CFG_FIELDS,
    _exposure_increasing,
    quote_core_config_from_params,
    quote_depth_from_l2_rows,
    spread_cap_mode_name,
)

from strategy.policy_guards import (
    adaptive_add_cooldown_config_from_params,
    adaptive_add_cooldown_multiplier,
    inventory_lifecycle_soft_gate_config_from_params,
    inventory_lifecycle_soft_gate_result,
)

from models.exchange_book_replay import (
    HistoricalExchangeBookScheduler,
    HistoricalExchangeBookVisibilityScheduler,
)
from research.families.f04_external_market_alpha.reference_replay import (
    InventoryLifecycleRepairCursor,
    HistoricalGlobalFlowCursor,
    HistoricalReferenceScheduler,
)
from research.families.f04_external_market_alpha.audit.cross_venue_causal_fair_price import (
    HistoricalFairPriceCursor,
)
from strategy.inventory_lifecycle_repair import (
    InventoryLifecycleRepairProbabilityHistory,
    build_inventory_lifecycle_repair_features,
    inventory_lifecycle_side,
)
from strategy.global_flow import DEFAULT_FLOW_HORIZONS_MS, GlobalFlowEngine
from strategy.multi_market_policy import (
    NOOP_MODE,
    MultiMarketPolicy,
    MultiMarketPolicyConfig,
    MultiMarketPolicyContext,
)
from strategy.post_fill_quote_response import (
    PostFillQuoteResponse,
    PostFillQuoteResponseConfig,
)

DATA_ROOT = data_root(ROOT)
RAW_DIR = DATA_ROOT / "raw"
RAW_TRADES_DIR = Path(
    os.environ.get("MM_RAW_TRADES_DIR", DATA_ROOT / "raw_trades")
).expanduser().resolve()
BARS_DIR = DATA_ROOT / "bars_1s"


def _default_replay_book_dirs(
    *,
    normalized_root: Optional[Path] = None,
    environ: Optional[Mapping[str, str]] = None,
) -> tuple[Path, Path]:
    """Resolve formal BTCUSDC replay inputs, preserving explicit overrides."""

    env = os.environ if environ is None else environ
    dataset_root = (
        normalized_l2_root(ROOT)
        if normalized_root is None
        else Path(normalized_root)
    )
    has_bbo_override = bool(str(env.get("MM_BBO_DIR", "")).strip())
    has_l2_override = bool(str(env.get("MM_L2_DIR", "")).strip())
    if has_bbo_override != has_l2_override:
        raise ValueError(
            "MM_BBO_DIR and MM_L2_DIR must be set together; "
            "mixed book roots are not a valid replay identity"
        )
    bbo_dir = Path(env.get("MM_BBO_DIR", dataset_root / "bbo"))
    l2_dir = Path(env.get("MM_L2_DIR", dataset_root / "l2"))
    return (
        bbo_dir.expanduser().resolve(),
        l2_dir.expanduser().resolve(),
    )


BBO_DIR, L2_DIR = _default_replay_book_dirs()

_INITIAL_SYMBOL_PATHS = paths_for(DEFAULT_SYMBOL)
SYMBOL = _INITIAL_SYMBOL_PATHS.symbol
FEATURES_DIR = _INITIAL_SYMBOL_PATHS.feature_dir
MODEL_DIR = _INITIAL_SYMBOL_PATHS.model_dir
RESULTS_DIR = _INITIAL_SYMBOL_PATHS.results_dir


def configure_symbol(symbol=None, *, model_dir_override=None):
    global SYMBOL, FEATURES_DIR, MODEL_DIR, RESULTS_DIR

    paths = paths_for(symbol)
    SYMBOL = paths.symbol
    FEATURES_DIR = paths.feature_dir
    MODEL_DIR = paths.model_dir
    RESULTS_DIR = paths.results_dir
    if model_dir_override is not None:
        MODEL_DIR = Path(model_dir_override).expanduser().resolve()

TICK = 0.1
LOT_SIZE = 0.001
CAP_AB_FIXED_BPS = (12.0, 16.0, 20.0, 24.0)
HISTORICAL_BBO_STREAM_THRESHOLD_BYTES = 512 * 1024 * 1024
HISTORICAL_BBO_BUCKET_MS = 1000


def _price_tick_alignment_tolerance(price: float, tick_size: float) -> float:
    tick = max(abs(float(tick_size)), 1e-12)
    return max(
        tick * 1e-9,
        64.0 * np.finfo(np.float64).eps * max(1.0, abs(float(price)), tick),
    )


def _price_to_tick(price: float, tick_size: float = TICK) -> int:
    value = float(price)
    tick = abs(float(tick_size))
    if not math.isfinite(value) or value <= 0.0:
        raise ValueError("executable price must be finite and positive")
    if not math.isfinite(tick) or tick <= 0.0:
        raise ValueError("tick_size must be finite and positive")
    units = value / tick
    if units > np.iinfo(np.int64).max:
        raise OverflowError("price tick index exceeds int64 range")
    price_tick = int(round(units))
    reconstructed = float(price_tick) * tick
    if abs(value - reconstructed) > _price_tick_alignment_tolerance(value, tick):
        raise ValueError(
            f"executable price {value!r} is not aligned to tick_size {tick!r}"
        )
    return price_tick


def _price_ticks_unchecked(values: np.ndarray, tick_size: float = TICK) -> np.ndarray:
    return np.rint(np.asarray(values, dtype=np.float64) / float(tick_size)).astype(
        np.int64,
        copy=False,
    )


def _price_tick_mask(
    values: np.ndarray,
    target_tick: int,
    tick_size: float = TICK,
) -> tuple[np.ndarray, np.ndarray]:
    prices = np.asarray(values, dtype=np.float64)
    valid = np.isfinite(prices) & (prices > 0.0)
    mask = np.zeros(prices.shape, dtype=bool)
    valid_ticks = _price_ticks_unchecked(prices[valid], tick_size)
    mask[valid] = valid_ticks == int(target_tick)
    return mask, valid_ticks


def _order_price_tick(order: dict, tick_size: float = TICK) -> int:
    price_tick = order.get("price_tick")
    if price_tick is None:
        price_tick = _price_to_tick(float(order["price"]), tick_size)
        order["price_tick"] = int(price_tick)
    return int(price_tick)


def _trade_crosses_order_tick(
    side: str,
    trade_price: float,
    order_price: float,
    tick_size: float = TICK,
) -> bool:
    trade_tick = _price_to_tick(trade_price, tick_size)
    order_tick = _price_to_tick(order_price, tick_size)
    return _trade_tick_crosses_order_tick(side, trade_tick, order_tick)


def _trade_tick_crosses_order_tick(
    side: str,
    trade_tick: int,
    order_tick: int,
) -> bool:
    return (
        int(trade_tick) <= int(order_tick)
        if str(side).upper() == "BUY"
        else int(trade_tick) >= int(order_tick)
    )


def _order_would_cross_book_tick(
    side: str,
    order_price: float,
    best_bid: float,
    best_ask: float,
    tick_size: float = TICK,
) -> bool:
    order_tick = _price_to_tick(order_price, tick_size)
    if str(side).upper() == "BUY":
        return best_ask > 0.0 and order_tick >= _price_to_tick(best_ask, tick_size)
    return best_bid > 0.0 and order_tick <= _price_to_tick(best_bid, tick_size)


def _match_ioc_order(
    side: str, limit_price: float, quantity: float,
    prices: Any, quantities: Any, *, tick_size: float, lot_size: float, market: bool = False,
) -> tuple[float, float]:
    """Consume only supplied marketable levels; return filled quantity and VWAP.

    A one-level input is a BBO liquidity bound, not a full-depth or exact-queue
    claim. Missing/invalid quantity never implies an unlimited touch fill.
    """
    is_buy = str(side).upper() == "BUY"
    limit_tick = _price_to_tick(limit_price, tick_size)
    levels: dict[int, tuple[float, float]] = {}
    for price, available in zip(prices, quantities, strict=True):
        price, available = float(price), float(available)
        if not (math.isfinite(price) and price > 0.0):
            continue
        price_tick = _price_to_tick(price, tick_size)
        if not market and (price_tick > limit_tick if is_buy else price_tick < limit_tick):
            continue
        if price_tick in levels:
            continue
        # Repeated/padded levels do not create additional displayed liquidity.
        levels[price_tick] = (
            price, available if math.isfinite(available) and available > 0.0 else 0.0,
        )
    requested = max(0.0, float(quantity))
    available = sum(level[1] for level in levels.values())
    filled = math.floor(min(requested, available) / lot_size + 1e-12) * lot_size
    if filled < lot_size:
        return 0.0, 0.0
    remaining, notional = filled, 0.0
    for key in sorted(levels, reverse=not is_buy):
        price, available = levels[key]
        take = min(remaining, available)
        notional += take * price
        remaining = subtract_lot_quantity(remaining, take, lot_size)
        if remaining <= 1e-12:
            break
    return filled, notional / filled


def _coverage_union_seconds(starts: np.ndarray, ends: np.ndarray) -> float:
    if len(starts) == 0:
        return 0.0
    order = np.argsort(starts, kind="stable")
    starts = starts[order]
    ends = ends[order]
    total_ms = 0.0
    cur_start = float(starts[0])
    cur_end = float(ends[0])
    for start, end in zip(starts[1:], ends[1:], strict=True):
        start = float(start)
        end = float(end)
        if start <= cur_end:
            if end > cur_end:
                cur_end = end
        else:
            total_ms += cur_end - cur_start
            cur_start = start
            cur_end = end
    total_ms += cur_end - cur_start
    return max(total_ms, 0.0) / 1000.0


def _historical_book_coverage_ratio(trade_ts: np.ndarray, book_ts_arrays, freshness_ms: int) -> float:
    if len(trade_ts) < 2 or freshness_ms <= 0:
        return 0.0
    start_ts = int(trade_ts[0])
    end_ts = int(trade_ts[-1])
    span_ms = max(end_ts - start_ts, 1)
    interval_starts = []
    interval_ends = []
    for book_ts in book_ts_arrays:
        if book_ts is None or len(book_ts) == 0:
            continue
        ts = np.asarray(book_ts, dtype=np.int64)
        mask = (ts <= end_ts) & ((ts + freshness_ms) >= start_ts)
        if not np.any(mask):
            continue
        active_ts = ts[mask]
        interval_starts.append(np.maximum(active_ts, start_ts))
        interval_ends.append(np.minimum(active_ts + freshness_ms, end_ts))
    if not interval_starts:
        return 0.0
    starts = np.concatenate(interval_starts)
    ends = np.concatenate(interval_ends)
    valid = ends > starts
    if not np.any(valid):
        return 0.0
    covered_s = _coverage_union_seconds(starts[valid], ends[valid])
    return min(1.0, covered_s / (span_ms / 1000.0))


def _historical_book_active_coverage_ratio(trade_ts: np.ndarray, book_ts_arrays, freshness_ms: int) -> float:
    """Coverage on seconds that still have trades after quality filtering.

    Multi-day/debug replays can contain large intentional gaps after
    hard-excluding bad orderbook days. The span-based coverage check should not
    fail solely because those excluded days are absent from both trades and
    books. Promotion evidence should still be built from fresh-start daily rows.
    """
    if len(trade_ts) == 0 or freshness_ms <= 0:
        return 0.0
    active_trade_sec = np.unique((np.asarray(trade_ts, dtype=np.int64) // 1000) * 1000)
    if len(active_trade_sec) == 0:
        return 0.0
    covered = np.zeros(len(active_trade_sec), dtype=bool)
    for book_ts in book_ts_arrays:
        if book_ts is None or len(book_ts) == 0:
            continue
        ts = np.asarray(book_ts, dtype=np.int64)
        ts = np.sort(ts)
        idx = np.searchsorted(ts, active_trade_sec, side="right") - 1
        valid = idx >= 0
        if not np.any(valid):
            continue
        covered[valid] |= (active_trade_sec[valid] - ts[idx[valid]]) <= freshness_ms
    return float(covered.mean())


def _deduplicate_l2_price_levels(px: np.ndarray, qty: np.ndarray) -> tuple[np.ndarray, np.ndarray, int]:
    """Return live-like L2 rows where each price level appears at most once.

    Some historical daily L2 containers are built from shallow book snapshots and
    repeat the same price/qty across several columns. Live orderbook state never
    treats those repeated columns as independent queue ahead, so replay must
    zero duplicate prices once at load time before quote-core/queue logic sees
    them.
    """
    if px.size == 0 or qty.size == 0:
        return px, qty, 0
    src_px = np.asarray(px, dtype=np.float64)
    src_qty = np.asarray(qty, dtype=np.float64)
    if src_px.shape[1] <= 1:
        return src_px, src_qty, 0
    valid_left = np.isfinite(src_px[:, :-1]) & (src_px[:, :-1] > 0.0)
    adjacent_duplicates = valid_left & (src_px[:, :-1] == src_px[:, 1:])
    if not np.any(adjacent_duplicates):
        return src_px, src_qty, 0
    out_px = np.zeros_like(np.asarray(px, dtype=np.float64))
    out_qty = np.zeros_like(np.asarray(qty, dtype=np.float64))
    duplicate_count = 0
    for row in range(src_px.shape[0]):
        seen: set[float] = set()
        dst_col = 0
        for col in range(src_px.shape[1]):
            price = float(src_px[row, col])
            size = float(src_qty[row, col])
            if not np.isfinite(price) or price <= 0.0 or not np.isfinite(size) or size <= 0.0:
                continue
            if price in seen:
                duplicate_count += 1
                continue
            seen.add(price)
            out_px[row, dst_col] = price
            out_qty[row, dst_col] = size
            dst_col += 1
            if dst_col >= out_px.shape[1]:
                break
    return out_px, out_qty, duplicate_count


def _historical_book_coverage_values(trade_ts, bbo_data, l2_data, freshness_ms):
    book_ts_arrays = []
    if bbo_data is not None:
        book_ts_arrays.append(bbo_data.ts_ms)
    if l2_data is not None:
        book_ts_arrays.append(l2_data.ts_ms)
    span_coverage = _historical_book_coverage_ratio(trade_ts, book_ts_arrays, freshness_ms)
    active_coverage = _historical_book_active_coverage_ratio(trade_ts, book_ts_arrays, freshness_ms)
    return span_coverage, active_coverage


def _validate_historical_book_coverage(trade_ts: np.ndarray, bbo_data, l2_data, params,
                                       *, _prepared_owner=None, _clock_key=None) -> None:
    if not bool(params.get("require_historical_bbo", False)):
        return
    min_coverage = float(params.get("min_historical_book_coverage", 0.90))
    if min_coverage <= 0.0:
        return
    freshness_ms = int(float(params.get("max_exec_book_age_s", 5.0)) * 1000.0)
    values = None
    if _prepared_owner is not None and _clock_key is not None:
        values = _prepared_owner.historical_book_coverage(
            _clock_key, bbo_data, l2_data, freshness_ms, _historical_book_coverage_values)
    if values is None:
        values = _historical_book_coverage_values(trade_ts, bbo_data, l2_data, freshness_ms)
    span_coverage, active_coverage = values
    coverage = max(span_coverage, active_coverage)
    if coverage + 1e-12 < min_coverage:
        start_desc = pd.to_datetime(int(trade_ts[0]), unit="ms", utc=True)
        end_desc = pd.to_datetime(int(trade_ts[-1]), unit="ms", utc=True)
        raise ValueError(
            "Historical BBO/L2 coverage too low for live-aligned replay: "
            f"coverage={coverage:.1%}, required={min_coverage:.1%}, "
            f"span_coverage={span_coverage:.1%}, active_coverage={active_coverage:.1%}, "
            f"freshness={freshness_ms / 1000.0:.1f}s, window={start_desc} -> {end_desc}. "
            "Download/rebuild the missing orderbook files, narrow the time window, "
            "or set min_historical_book_coverage=0 for diagnostics only."
        )


def _resolve_cap_params(params):
    max_spread_bps = float(params.get("max_spread_bps", 0.0))
    dynamic_cap_enabled = bool(params.get("dynamic_cap_enabled", False))
    dynamic_cap_base_bps = float(params.get("dynamic_cap_base_bps", max_spread_bps))
    if dynamic_cap_base_bps <= 0.0 and max_spread_bps > 0.0:
        dynamic_cap_base_bps = max_spread_bps
    dynamic_cap_alpha = float(params.get("dynamic_cap_alpha", 0.5))
    dynamic_cap_max_mult = float(params.get("dynamic_cap_max_mult", 2.0))
    if dynamic_cap_max_mult < 1.0:
        dynamic_cap_max_mult = 1.0
    dynamic_cap_var_baseline = float(params.get("dynamic_cap_var_baseline", 0.0))
    if dynamic_cap_var_baseline <= 0.0:
        vol_baseline = float(params.get("vol_baseline", 0.0))
        if vol_baseline > 0.0:
            dynamic_cap_var_baseline = vol_baseline * vol_baseline
    dynamic_cap_liq_beta = max(0.0, float(params.get("dynamic_cap_liq_beta", 0.0)))
    dynamic_cap_liq_baseline = max(0.0, float(params.get("dynamic_cap_liq_baseline", 0.0)))
    dynamic_cap_min_mult = max(0.0, min(1.0, float(params.get("dynamic_cap_min_mult", 1.0))))
    return (
        dynamic_cap_enabled,
        max_spread_bps,
        dynamic_cap_base_bps,
        dynamic_cap_alpha,
        dynamic_cap_max_mult,
        dynamic_cap_var_baseline,
        dynamic_cap_liq_beta,
        dynamic_cap_liq_baseline,
        dynamic_cap_min_mult,
    )


def _cap_label_from_fields(cap_mode, cap_bps):
    if cap_bps <= 0.0:
        return "off"
    if cap_mode == "dynamic":
        return f"D{cap_bps:.0f}"
    return f"{cap_bps:.0f}"


def _cap_cell(result):
    if "cap_label" in result:
        return str(result["cap_label"])
    cap_mode = result.get("cap_mode", "fixed")
    cap_bps = result.get("cap_base_bps", result.get("max_sprd_bps", 0.0))
    return _cap_label_from_fields(cap_mode, cap_bps)


def _build_cap_ab_params(base):
    params_list = []
    for cap in CAP_AB_FIXED_BPS:
        p = dict(base)
        p["dynamic_cap_enabled"] = False
        p["max_spread_bps"] = float(cap)
        p["cap_label"] = f"{cap:.0f}"
        params_list.append(p)

    dyn = dict(base)
    dyn_base = float(dyn.get("dynamic_cap_base_bps", dyn.get("max_spread_bps", 0.0)))
    if dyn_base <= 0.0:
        dyn_base = 16.0
    dyn["dynamic_cap_enabled"] = True
    dyn["dynamic_cap_base_bps"] = dyn_base
    dyn["max_spread_bps"] = dyn_base
    dyn["cap_label"] = _cap_label_from_fields("dynamic", dyn_base)
    params_list.append(dyn)
    return params_list


# ═══════════════════════════════════════════════════════════════════
#  1. DATA — load raw aggTrades
# ═══════════════════════════════════════════════════════════════════

AGGTRADE_COLUMNS = [
    "agg_trade_id", "price", "quantity",
    "first_trade_id", "last_trade_id",
    "transact_time", "is_buyer_maker",
]
AGGTRADE_DTYPES = {
    "price": np.float64,
    # Matching subtracts queue ahead and floors to exchange lots. float32 can
    # turn .009 into .0089999996, losing an entire final .001 lot after queue
    # depletion; promoting the rounded value later cannot recover the source.
    "quantity": np.float64,
    "transact_time": np.int64,
}
AGGTRADE_USECOLS = ["price", "quantity", "transact_time", "is_buyer_maker"]
INDIVIDUAL_TRADE_USECOLS = [
    "id",
    "price",
    "qty",
    "time",
    "is_buyer_maker",
]


def _read_aggtrade_csv(path: Path, *, include_trade_ids: bool = False, time_bounds_ms=None) -> pd.DataFrame:
    identity_columns = ["agg_trade_id", "first_trade_id", "last_trade_id"]
    if path.suffix == ".parquet":
        columns = AGGTRADE_USECOLS + (identity_columns if include_trade_ids else [])
        return _read_daily_trade_parquet(path, aggregate=True, time_bounds_ms=time_bounds_ms)[columns]
    parsed = pd.read_csv(
        path,
        names=AGGTRADE_COLUMNS,
        usecols=AGGTRADE_USECOLS + (identity_columns if include_trade_ids else []),
        dtype={**AGGTRADE_DTYPES, **(
            {name: np.int64 for name in identity_columns} if include_trade_ids else {}
        )},
        true_values=["true", "True", "TRUE"],
        false_values=["false", "False", "FALSE"],
        header=0,
        **({"chunksize": 65_536} if time_bounds_ms is not None else {}),
    )
    if time_bounds_ms is None:
        df = parsed
    else:
        start, end = time_bounds_ms
        frames = [chunk.loc[(chunk.transact_time >= start) & (chunk.transact_time < end)]
                  for chunk in parsed]
        df = pd.concat(frames, ignore_index=True)
    if df["is_buyer_maker"].dtype != np.bool_:
        df["is_buyer_maker"] = (
            df["is_buyer_maker"].astype(str).str.strip().str.lower() == "true"
        )
    df["is_buyer_maker"] = df["is_buyer_maker"].astype(np.bool_, copy=False)
    return df


def _read_individual_trade_csv(path: Path, *, time_bounds_ms=None) -> pd.DataFrame:
    """Load Binance Vision ``trades`` without aggregating matching events."""

    from data.daily_raw import is_tardis_trade_archive, iter_tardis_trade_frames
    if is_tardis_trade_archive(path):
        frames = []
        for frame in iter_tardis_trade_frames(path, symbol=SYMBOL):
            frame = frame.rename(columns={"id": "trade_id", "qty": "quantity", "time": "transact_time"})
            if time_bounds_ms is not None:
                start, end = time_bounds_ms
                frame = frame.loc[(frame.transact_time >= start) & (frame.transact_time < end)]
            frames.append(frame)
        if not frames:
            raise ValueError("Empty purchased trade archive")
        # The fact projection is already typed and millisecond-checked. Legacy
        # normalization sorts equal-time IDs, destroying supplied source order.
        return pd.concat(frames, ignore_index=True)

    if path.suffix == ".parquet":
        frame = _read_daily_trade_parquet(path, aggregate=False, time_bounds_ms=time_bounds_ms)
        return _normalize_individual_trade_frame(frame)
    parsed = pd.read_csv(
        path,
        usecols=INDIVIDUAL_TRADE_USECOLS,
        dtype={
            "id": np.int64,
            "price": np.float64,
            "qty": np.float64,
            "time": np.int64,
        },
        true_values=["true", "True", "TRUE"],
        false_values=["false", "False", "FALSE"],
        **({"chunksize": 65_536} if time_bounds_ms is not None else {}),
    )
    frames = []
    for chunk in ([parsed] if time_bounds_ms is None else parsed):
        frame = _normalize_individual_trade_frame(chunk.rename(
        columns={
            "id": "trade_id",
            "qty": "quantity",
            "time": "transact_time",
        }
        ))
        if time_bounds_ms is not None:
            start, end = time_bounds_ms
            frame = frame.loc[(frame.transact_time >= start) & (frame.transact_time < end)].copy()
        frames.append(frame)
    return _normalize_individual_trade_frame(pd.concat(frames, ignore_index=True))


def _normalize_individual_trade_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame["is_buyer_maker"].dtype != np.bool_:
        frame["is_buyer_maker"] = (
            frame["is_buyer_maker"]
            .astype(str)
            .str.strip()
            .str.lower()
            .eq("true")
        )
    frame["is_buyer_maker"] = frame["is_buyer_maker"].astype(np.bool_)
    timestamps = frame["transact_time"].to_numpy(dtype=np.int64, copy=False)
    if timestamps.size:
        max_timestamp = int(np.max(np.abs(timestamps)))
        if max_timestamp >= 10**17:
            timestamps = timestamps // 1_000_000
        elif max_timestamp >= 10**14:
            timestamps = timestamps // 1_000
        frame["transact_time"] = timestamps
    if len(frame) > 1:
        trade_ids = frame["trade_id"].to_numpy(dtype=np.int64, copy=False)
        out_of_order = (timestamps[1:] < timestamps[:-1]) | (
            (timestamps[1:] == timestamps[:-1])
            & (trade_ids[1:] < trade_ids[:-1])
        )
        if bool(np.any(out_of_order)):
            frame.sort_values(
                ["transact_time", "trade_id"],
                kind="stable",
                inplace=True,
            )
            frame.reset_index(drop=True, inplace=True)
    return frame


def _read_daily_trade_parquet(path: Path, *, aggregate: bool, time_bounds_ms=None) -> pd.DataFrame:
    """Decode the canonical daily schema at the existing replay boundary."""
    if time_bounds_ms is None:
        frame = pd.read_parquet(path)
    else:
        rows = _parquet_window_rows(path, *time_bounds_ms, raw_trade_clock=True)
        frame = _read_parquet_rows(path, rows).to_pandas()
    if set(frame.columns) & {"group_id", "bucket_start_ms", "feature_ready_ts_ms"}:
        raise ValueError("self-aggregated flow features are not a native execution/aggTrade tape")
    aliases = {"amount": "quantity", "qty": "quantity",
               "id": "agg_trade_id" if aggregate else "trade_id"}
    for old, new in aliases.items():
        if old in frame and new not in frame:
            frame = frame.rename(columns={old: new})
    if "timestamp" in frame:
        # Canonical raw timestamps have a fixed microsecond unit; the replay
        # interface remains milliseconds. Never use local_timestamp here.
        frame["transact_time"] = frame["timestamp"].astype(np.int64) // 1000
    if "is_buyer_maker" not in frame and "side" in frame:
        if not frame["side"].isin(["buy", "sell"]).all():
            raise ValueError("canonical trade side must be buy or sell")
        frame["is_buyer_maker"] = frame["side"].eq("sell")
    columns = AGGTRADE_COLUMNS if aggregate else [
        "trade_id", "price", "quantity", "transact_time", "is_buyer_maker",
    ]
    frame = frame[columns].copy()
    for name in ("price", "quantity"):
        frame[name] = frame[name].astype(np.float64)
    for name in ("transact_time", "agg_trade_id", "first_trade_id", "last_trade_id", "trade_id"):
        if name in frame:
            frame[name] = frame[name].astype(np.int64)
    return frame


def _prefer_daily_trade_paths(files, *, channel, days, enabled):
    if not enabled:
        return files
    from data_paths import tardis_raw_root, tardis_market_path
    if channel == "trades" and tardis_raw_root() is not None:
        if days is None:
            from data.daily_raw import market_day_from_path
            days = sorted({market_day_from_path(path) for path in
                           (tardis_raw_root() / "binance-futures" / "trades").glob(f"*/*/*/{SYMBOL}.csv*")})
        # Every requested date must resolve; no old daily-Parquet fallback.
        return [tardis_market_path(day, SYMBOL, "trades") for day in sorted(set(days))]
    if days is None:
        directory = daily_market_path("2000-01-01", SYMBOL, channel).parent.parent
        canonical = sorted(directory.glob(f"*/{channel}.parquet"))
    else:
        canonical = [daily_market_path(day, SYMBOL, channel)
                     for day in sorted(set(days))]
        canonical = [path for path in canonical if path.is_file()]
    selected = {path.parent.name: path for path in canonical}
    for path in files:
        day = _daily_tag_from_name(path, f"{SYMBOL}-{channel}-")
        if day not in selected:
            selected[day] = path
    return [selected[day] for day in sorted(selected)]


def _daily_tag_from_name(path: Path, stem_prefix: str) -> Optional[str]:
    stem = path.name
    for suffix in (".csv.gz", ".parquet", ".csv"):
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
            break
    if not stem.startswith(stem_prefix):
        return None
    tag = stem[len(stem_prefix):]
    parts = tag.split("-")
    if len(parts) == 3 and all(part.isdigit() for part in parts):
        return tag
    return None


def _filter_daily_files(files, stem_prefix: str, days=None):
    """Keep only explicit UTC-day containers named with YYYY-MM-DD."""
    keep_days = set(days or [])
    filtered = []
    skipped_non_daily = []
    for path in files:
        day_tag = _daily_tag_from_name(path, stem_prefix)
        if day_tag is None:
            skipped_non_daily.append(path.name)
            continue
        if keep_days and day_tag not in keep_days:
            continue
        filtered.append(path)

    if skipped_non_daily:
        preview = ", ".join(skipped_non_daily[:3])
        suffix = " ..." if len(skipped_non_daily) > 3 else ""
        qprint(f"  Skipping {len(skipped_non_daily)} non-daily files: {preview}{suffix}")
    return sorted(filtered)


def _index_to_epoch_ms(index: pd.Index) -> np.ndarray:
    """Return epoch milliseconds for DatetimeIndex or numeric timestamp indexes."""
    if isinstance(index, pd.DatetimeIndex):
        return (index.as_unit("ns").asi8 // 1_000_000).astype(np.int64, copy=False)

    values = index.to_numpy()
    if np.issubdtype(values.dtype, np.datetime64):
        ns = values.astype("datetime64[ns]").astype(np.int64)
        return (ns // 1_000_000).astype(np.int64, copy=False)

    numeric = pd.to_numeric(pd.Index(values), errors="coerce").to_numpy(dtype=np.float64)
    if not np.isfinite(numeric).all():
        raise ValueError("Timestamp index contains non-numeric/non-datetime values")

    max_abs = float(np.max(np.abs(numeric))) if len(numeric) else 0.0
    if max_abs >= 1e17:       # epoch nanoseconds
        numeric = numeric / 1_000_000.0
    elif max_abs >= 1e14:     # epoch microseconds
        numeric = numeric / 1_000.0
    elif max_abs >= 1e11:     # epoch milliseconds
        pass
    elif max_abs >= 1e8:      # epoch seconds
        numeric = numeric * 1_000.0
    else:
        raise ValueError("Timestamp index is not recognizable as epoch time")
    return np.rint(numeric).astype(np.int64)


def _collect_snapshot_files(dir_path: Path, stem_variants: tuple[str, ...], days=None):
    files = []
    for variant in stem_variants:
        matched = sorted(dir_path.glob(f"{SYMBOL}-{variant}-*.parquet"))
        matched = _filter_daily_files(matched, f"{SYMBOL}-{variant}-", days=days)
        files.extend(matched)
    return sorted(dict.fromkeys(files))


def _extract_ts_ms(frame: pd.DataFrame) -> np.ndarray:
    if isinstance(frame.index, pd.DatetimeIndex):
        return (frame.index.as_unit("ns").asi8 // 1_000_000).astype(np.int64, copy=False)

    for col in ("timestamp", "ts_ms", "transact_time", "event_time", "time", "E", "T"):
        if col not in frame.columns:
            continue
        series = frame[col]
        if pd.api.types.is_datetime64_any_dtype(series):
            parsed = pd.DatetimeIndex(pd.to_datetime(series, utc=True, errors="coerce"))
            return parsed.as_unit("ns").asi8 // 1_000_000
        arr = series.to_numpy()
        if np.issubdtype(arr.dtype, np.integer):
            return arr.astype(np.int64, copy=False)
        parsed = pd.to_datetime(series, utc=True, errors="coerce")
        if parsed.notna().any():
            return pd.DatetimeIndex(parsed).as_unit("ns").asi8 // 1_000_000

    raise ValueError("No timestamp column/index found for historical snapshot data")


def _parquet_window_rows(path: Path, start_ms: int, end_ms: int, *,
                         keep_predecessor: bool = False, raw_trade_clock: bool = False,
                         predecessor_valid_columns: tuple[str, str] | None = None,
                         extra_predecessor_ms: int | None = None, parquet=None) -> np.ndarray:
    """Locate a time slice from narrow columns, without loading the depth payload.

    Row positions (not timestamp de-duplication) preserve simultaneous executions.
    The narrow scan is bounded in batches; only its integer axis is retained.
    Snapshot loaders may retain the last valid pre-window level-1 state.
    """
    if type(start_ms) is not int or type(end_ms) is not int or start_ms >= end_ms:
        raise ValueError("bounded parquet input requires increasing integer millisecond bounds")
    parquet = pq.ParquetFile(path) if parquet is None else parquet
    names = parquet.schema_arrow.names
    timestamp = _first_existing_name(names, (
        "timestamp", "ts_ms", "transact_time", "event_time", "time", "E", "T",
        "__index_level_0__",
    ))
    if timestamp is None:
        raise ValueError(f"bounded parquet input has no timestamp column: {path.name}")
    columns = [timestamp, *(predecessor_valid_columns or ())]
    axes, valid_parts = [], []
    for batch in parquet.iter_batches(batch_size=65_536, columns=columns, use_threads=False):
        frame = batch.to_pandas()
        if timestamp == "__index_level_0__":
            axis = _index_to_epoch_ms(frame.index)
        else:
            axis = _extract_ts_ms(frame)
        if raw_trade_clock and timestamp == "timestamp":
            axis = axis // 1000  # canonical raw timestamp has a fixed microsecond unit
        axes.append(axis)
        if predecessor_valid_columns:
            bid, ask = (frame[name].to_numpy(dtype=np.float64) for name in predecessor_valid_columns)
            valid_parts.append(np.isfinite(bid) & np.isfinite(ask) & (bid > 0) & (ask > bid))
    axis = np.concatenate(axes) if axes else np.empty(0, dtype=np.int64)
    mask = (axis >= start_ms) & (axis < end_ms)
    if keep_predecessor:
        eligible = axis < start_ms
        if eligible.any():
            mask |= eligible & (axis == axis[eligible].max())
        if valid_parts:
            eligible &= np.concatenate(valid_parts)
            order = np.argsort(axis, kind="stable")
            last = np.r_[axis[order][:-1] != axis[order][1:], True]
            final_row = np.zeros(len(axis), dtype=bool)
            final_row[order[last]] = True
            eligible &= final_row
        if eligible.any():
            previous_ts = axis[eligible].max()
            # Keep equal-time rows so the existing stable-last snapshot rule is unchanged.
            mask |= axis == previous_ts
        if extra_predecessor_ms is not None:
            if extra_predecessor_ms >= start_ms:
                raise ValueError("extra snapshot predecessor is not in the past")
            mask |= axis == extra_predecessor_ms
    return np.flatnonzero(mask)


def _read_parquet_rows(path: Path, rows: np.ndarray, *, columns=None, parquet=None) -> pa.Table:
    """Decode only intersecting row groups, retaining at most one payload batch.

    Even a producer's single huge row group is iterated, never read_table/read().
    """
    parquet = pq.ParquetFile(path) if parquet is None else parquet
    schema = parquet.schema_arrow
    if columns is not None:
        schema = pa.schema([schema.field(name) for name in columns], metadata=schema.metadata)
    if not len(rows):
        return pa.Table.from_batches([], schema=schema)
    batches = []
    offset = 0
    for group in range(parquet.num_row_groups):
        count = parquet.metadata.row_group(group).num_rows
        lo, hi = np.searchsorted(rows, (offset, offset + count))
        if hi > lo:
            position = offset
            for batch in parquet.iter_batches(batch_size=16_384, row_groups=[group],
                                               columns=columns, use_threads=False):
                left, right = np.searchsorted(rows, (position, position + len(batch)))
                selected = rows[left:right] - position
                if len(selected):
                    if selected[-1] - selected[0] + 1 == len(selected):
                        batches.append(batch.slice(int(selected[0]), len(selected)))
                    else:
                        batches.append(batch.take(pa.array(selected)))
                position += len(batch)
                if position > rows[-1]:
                    break
        offset += count
        if offset > rows[-1]:
            break
    return pa.Table.from_batches(batches, schema=schema)


def _snapshot_window_mask(ts_ms: np.ndarray, bounds) -> np.ndarray:
    if bounds is None:
        return np.ones(len(ts_ms), dtype=bool)
    start, end = bounds
    keep = (ts_ms >= start) & (ts_ms < end)
    previous = np.flatnonzero(ts_ms < start)
    if len(previous):
        keep[previous[-1]] = True
    return keep


def _last_bbo_invalidation_before(path: Path, start_ms: int) -> int | None:
    """Read only the narrow invalidation mask, not the full clock payload."""
    inputs = book_observation_inputs(path)
    if len(inputs) < 2:
        return None
    parquet = pq.ParquetFile(inputs[1])
    if "bbo_usable" not in parquet.schema_arrow.names:
        return None
    previous = None
    for batch in parquet.iter_batches(batch_size=65_536, columns=["timestamp", "bbo_usable"], use_threads=False):
        usable = batch.column("bbo_usable")
        if usable.null_count or not pa.types.is_boolean(usable.type):
            raise ValueError("independent top clock lacks its usability mask")
        axis = batch.column("timestamp").to_numpy()
        invalid = axis[(axis < start_ms) & ~usable.to_numpy(zero_copy_only=False)]
        if len(invalid):
            value = int(invalid.max())
            previous = value if previous is None else max(previous, value)
    return previous


def _dedup_sorted_frame(frame: pd.DataFrame, ts_ms: np.ndarray):
    order = np.argsort(ts_ms, kind="stable")
    ts_sorted = ts_ms[order]
    frame_sorted = frame.iloc[order].reset_index(drop=True)
    keep = np.ones(len(ts_sorted), dtype=bool)
    if len(ts_sorted) > 1:
        keep[:-1] = ts_sorted[:-1] != ts_sorted[1:]
    return frame_sorted.loc[keep].reset_index(drop=True), ts_sorted[keep]


def _parse_time_filter_ms(value: Optional[str], tz_name: str) -> Optional[int]:
    if not value:
        return None
    text = value.strip().replace("T", " ")
    ts = pd.Timestamp(text)
    if ts.tzinfo is None:
        ts = ts.tz_localize(ZoneInfo(tz_name))
    else:
        ts = ts.tz_convert(ZoneInfo(tz_name))
    return int(ts.tz_convert("UTC").timestamp() * 1000)


def _days_for_time_window(start_ms: Optional[int], end_ms: Optional[int]):
    if start_ms is None and end_ms is None:
        return None
    start_ts = pd.to_datetime(start_ms if start_ms is not None else end_ms, unit="ms", utc=True)
    end_ts = pd.to_datetime(end_ms if end_ms is not None else start_ms, unit="ms", utc=True)
    if start_ts > end_ts:
        raise ValueError("start-time is after end-time")
    days = []
    cur = start_ts.floor("D")
    last = end_ts.floor("D")
    while cur <= last:
        days.append(cur.strftime("%Y-%m-%d"))
        cur = cur + pd.Timedelta(days=1)
    return days


def _filter_trades_by_time(trades: pd.DataFrame, start_ms: Optional[int], end_ms: Optional[int]) -> pd.DataFrame:
    if start_ms is None and end_ms is None:
        return trades
    mask = np.ones(len(trades), dtype=bool)
    ts = trades["transact_time"].to_numpy(dtype=np.int64, copy=False)
    if start_ms is not None:
        mask &= ts >= start_ms
    if end_ms is not None:
        mask &= ts < end_ms
    filtered = trades.loc[mask].reset_index(drop=True)
    if filtered.empty:
        raise SystemExit("No aggTrades remain after applying --start-time/--end-time")
    return filtered


def _first_existing_column(frame: pd.DataFrame, candidates: tuple[str, ...]):
    for name in candidates:
        if name in frame.columns:
            return name
    return None


def _first_existing_name(names, candidates: tuple[str, ...]):
    for name in candidates:
        if name in names:
            return name
    return None


def _stream_bbo_file(path: Path) -> pd.DataFrame:
    parquet_file = pq.ParquetFile(path)
    schema_names = set(parquet_file.schema_arrow.names)

    ts_col = _first_existing_name(schema_names, ("timestamp", "ts_ms", "transact_time", "event_time", "time", "E", "T"))
    bid_col = _first_existing_name(schema_names, ("best_bid", "bid_price", "bid", "b"))
    ask_col = _first_existing_name(schema_names, ("best_ask", "ask_price", "ask", "a"))
    if ts_col is None or bid_col is None or ask_col is None:
        raise ValueError(f"Historical BBO parquet missing required columns: {path.name}")

    bid_qty_col = _first_existing_name(schema_names, ("bid_qty", "best_bid_qty", "bid_size", "B"))
    ask_qty_col = _first_existing_name(schema_names, ("ask_qty", "best_ask_qty", "ask_size", "A"))
    columns = [ts_col, bid_col, ask_col]
    if bid_qty_col:
        columns.append(bid_qty_col)
    if ask_qty_col:
        columns.append(ask_qty_col)

    reduced_frames = []
    carry = None
    for batch in parquet_file.iter_batches(batch_size=1_000_000, columns=columns):
        frame = batch.to_pandas()
        if frame.empty:
            continue

        ts_ms = _extract_ts_ms(frame)
        out = pd.DataFrame({
            "timestamp": ts_ms,
            "best_bid": frame[bid_col].astype(np.float64, copy=False),
            "best_ask": frame[ask_col].astype(np.float64, copy=False),
            "bid_qty": frame[bid_qty_col].astype(np.float64, copy=False)
            if bid_qty_col else np.zeros(len(frame), dtype=np.float64),
            "ask_qty": frame[ask_qty_col].astype(np.float64, copy=False)
            if ask_qty_col else np.zeros(len(frame), dtype=np.float64),
        })
        valid = (
            (out["timestamp"] > 0)
            & (out["best_bid"] > 0.0)
            & (out["best_ask"] > out["best_bid"])
        )
        if not valid.any():
            continue

        out = out.loc[valid].reset_index(drop=True)
        if carry is not None and not carry.empty:
            out = pd.concat([carry, out], ignore_index=True, copy=False)

        out["bucket_ts"] = (out["timestamp"] // HISTORICAL_BBO_BUCKET_MS) * HISTORICAL_BBO_BUCKET_MS
        reduced = out.groupby("bucket_ts", sort=False, as_index=False).last()
        if len(reduced) == 1:
            carry = reduced.iloc[[-1]][["timestamp", "best_bid", "best_ask", "bid_qty", "ask_qty"]]
            continue

        reduced_frames.append(reduced.iloc[:-1][["timestamp", "best_bid", "best_ask", "bid_qty", "ask_qty"]])
        carry = reduced.iloc[[-1]][["timestamp", "best_bid", "best_ask", "bid_qty", "ask_qty"]]

    if carry is not None and not carry.empty:
        reduced_frames.append(carry)

    if not reduced_frames:
        return pd.DataFrame(columns=["timestamp", "best_bid", "best_ask", "bid_qty", "ask_qty"])

    return pd.concat(reduced_frames, ignore_index=True, copy=False)


def _load_bbo_frame(path: Path) -> pd.DataFrame:
    if path.stat().st_size < HISTORICAL_BBO_STREAM_THRESHOLD_BYTES:
        return pd.read_parquet(path)

    print(
        f"  Streaming large BBO file {path.name} and reducing to "
        f"{HISTORICAL_BBO_BUCKET_MS}ms snapshots"
    )
    return _stream_bbo_file(path)


def _column_level_map(frame: pd.DataFrame, patterns: tuple[re.Pattern, ...]):
    out = {}
    for col in frame.columns:
        for pattern in patterns:
            match = pattern.fullmatch(col)
            if match:
                out[int(match.group(1))] = col
                break
    return out


def load_aggtrades(
    days=None,
    files_filter=None,
    *,
    quality_allowed_days=(),
    time_bounds_ms=None,
):
    """Load raw aggTrades CSVs into a DataFrame.

    Args:
        days: list of UTC days to filter, e.g. ["2026-05-15"]
        files_filter: arbitrary substring to match filenames
    """
    csv_files = sorted(RAW_DIR.glob(f"{SYMBOL}-aggTrades-*.csv"))
    csv_files = _filter_daily_files(csv_files, f"{SYMBOL}-aggTrades-", days=days)
    if files_filter:
        csv_files = [f for f in csv_files if files_filter in f.name]
    csv_files = _prefer_daily_trade_paths(
        csv_files, channel="aggTrades", days=days,
        enabled=not files_filter and RAW_DIR == DATA_ROOT / "raw",
    )
    csv_files = filter_paths_for_orderbook_quality(
        csv_files,
        SYMBOL,
        label="aggTrade",
        explicitly_allowed_days=quality_allowed_days,
    )

    if not csv_files:
        print(f"No matching daily files (days={days}, filter={files_filter})")
        sys.exit(1)

    dfs = []
    ordered = True
    prev_last_ts = None
    for f in csv_files:
        qprint(f"  Loading {f.name} …")
        df = _read_aggtrade_csv(f, **({"time_bounds_ms": time_bounds_ms} if time_bounds_ms else {}))
        if len(df) and prev_last_ts is not None and df["transact_time"].iloc[0] < prev_last_ts:
            ordered = False
        if len(df):
            prev_last_ts = df["transact_time"].iloc[-1]
        dfs.append(df)

    if len(dfs) == 1:
        trades = dfs[0]
    else:
        trades = pd.concat(dfs, ignore_index=True, copy=False)
    del dfs
    gc.collect()
    keep = allowed_timestamp_mask(
        trades["transact_time"].to_numpy(copy=False),
        SYMBOL,
        label="aggTrade",
        explicitly_allowed_days=quality_allowed_days,
    )
    if not keep.all():
        trades = trades.loc[keep].copy()
    if not ordered or not trades["transact_time"].is_monotonic_increasing:
        trades.sort_values("transact_time", inplace=True)
    trades.reset_index(drop=True, inplace=True)
    print(f"  Total: {len(trades):,} aggTrades")
    return trades


def load_individual_trades(
    days=None,
    files_filter=None,
    *,
    quality_allowed_days=(),
    time_bounds_ms=None,
):
    """Load public Binance individual trades for the execution event clock."""

    trade_dir = RAW_TRADES_DIR / SYMBOL
    csv_files = sorted(trade_dir.glob(f"{SYMBOL}-trades-*.csv"))
    csv_files.extend(sorted(trade_dir.glob(f"{SYMBOL}-trades-*.csv.gz")))
    csv_files = _filter_daily_files(
        sorted(dict.fromkeys(csv_files)),
        f"{SYMBOL}-trades-",
        days=days,
    )
    if files_filter:
        csv_files = [path for path in csv_files if files_filter in path.name]
    csv_files = _prefer_daily_trade_paths(
        csv_files, channel="trades", days=days,
        enabled=not files_filter and not os.environ.get("MM_RAW_TRADES_DIR")
        and RAW_TRADES_DIR == DATA_ROOT / "raw_trades",
    )
    csv_files = filter_paths_for_orderbook_quality(
        csv_files,
        SYMBOL,
        label="trade",
        explicitly_allowed_days=quality_allowed_days,
    )
    if not csv_files:
        raise FileNotFoundError(
            f"No {SYMBOL} individual trade files found for days={days} "
            f"under {trade_dir}"
        )

    frames = []
    ordered = True
    previous_last_ts = None
    for path in csv_files:
        qprint(f"  Loading {path.name} ...")
        frame = _read_individual_trade_csv(path, **({"time_bounds_ms": time_bounds_ms} if time_bounds_ms else {}))
        if (
            len(frame)
            and previous_last_ts is not None
            and int(frame["transact_time"].iloc[0]) < previous_last_ts
        ):
            ordered = False
        if len(frame):
            previous_last_ts = int(frame["transact_time"].iloc[-1])
        frames.append(frame)

    trades = (
        frames[0]
        if len(frames) == 1
        else pd.concat(frames, ignore_index=True, copy=False)
    )
    del frames
    gc.collect()
    keep = allowed_timestamp_mask(
        trades["transact_time"].to_numpy(copy=False),
        SYMBOL,
        label="trade",
        explicitly_allowed_days=quality_allowed_days,
    )
    if not keep.all():
        trades = trades.loc[keep].copy()
    if not ordered or not trades["transact_time"].is_monotonic_increasing:
        trades.sort_values("transact_time", kind="stable", inplace=True)
    trades.reset_index(drop=True, inplace=True)
    print(f"  Total: {len(trades):,} individual trades")
    return trades


def load_execution_trades(
    days=None,
    files_filter=None,
    *,
    source="trades",
    quality_allowed_days=(),
    time_bounds_ms=None,
):
    normalized = str(source).strip().lower()
    if normalized in {"aggtrade", "aggtrades"}:
        return load_aggtrades(
            days=days,
            files_filter=files_filter,
            quality_allowed_days=quality_allowed_days,
            **({"time_bounds_ms": time_bounds_ms} if time_bounds_ms else {}),
        )
    if normalized in {"trade", "trades", "individual"}:
        return load_individual_trades(
            days=days,
            files_filter=files_filter,
            quality_allowed_days=quality_allowed_days,
            **({"time_bounds_ms": time_bounds_ms} if time_bounds_ms else {}),
        )
    raise ValueError(
        "execution trade source must be one of: aggTrades, trades"
    )


def causal_complete_1s_bars(
    bars: pd.DataFrame,
    *,
    require_dense_source: bool = False,
) -> pd.DataFrame:
    """Restore the wall-clock 1s grid without looking past an empty second.

    Live emits a flat bar when no execution trade arrives during a second.
    Historical bar files contain only trade-bearing seconds, so using them as-is
    compresses dynamic-requote, BER, and volatility EMA time.  Carrying the last
    close forward and setting trade_count to zero reproduces the live contract.
    """
    if bars.empty:
        return bars.copy()

    if require_dense_source:
        # Validate the bytes as loaded, before sort/de-dup/reindex could repair
        # a malformed formal source and hide its actual clock contract.
        require_formal_dense_1s_timeline(
            bars,
            {"replay_purpose": "formal"},
        )

    out = bars.copy()
    out.index = _index_to_epoch_ms(out.index)
    out = out[~out.index.duplicated(keep="last")].sort_index()
    start_ms = int(out.index[0] // 1000 * 1000)
    end_ms = int(out.index[-1] // 1000 * 1000)
    full_index = pd.Index(
        np.arange(start_ms, end_ms + 1000, 1000, dtype=np.int64),
        name=out.index.name,
    )
    out = out.reindex(full_index)
    out["close"] = pd.to_numeric(out["close"], errors="coerce").ffill()
    out["trade_count"] = pd.to_numeric(
        out["trade_count"], errors="coerce"
    ).fillna(0.0)
    return out


def load_1s_bars(
    days=None,
    *,
    quality_allowed_days=(),
    require_dense_source: bool = False,
):
    """Load causal wall-clock 1s bars for rolling σ² and timer state."""
    files = sorted(BARS_DIR.glob(f"{SYMBOL}-1s-*.parquet"))
    files = _filter_daily_files(files, f"{SYMBOL}-1s-", days=days)
    files = filter_paths_for_orderbook_quality(
        files,
        SYMBOL,
        label="1s bar",
        explicitly_allowed_days=quality_allowed_days,
    )
    if not files:
        return None
    dfs = [
        causal_complete_1s_bars(
            pd.read_parquet(f, columns=["close", "trade_count"]),
            require_dense_source=require_dense_source,
        )
        for f in files
    ]
    bars = pd.concat(dfs, copy=False)
    if require_dense_source:
        # Catch overlap, reversal, or gaps at file boundaries before any
        # normalization could erase the source defect.
        require_formal_dense_1s_timeline(
            bars,
            {"replay_purpose": "formal"},
        )
    else:
        bars = bars.sort_index()
    del dfs
    gc.collect()
    if not require_dense_source:
        bars = bars[~bars.index.duplicated(keep="first")]
    bars = filter_frame_for_orderbook_quality(
        bars,
        SYMBOL,
        label="1s bar",
        explicitly_allowed_days=quality_allowed_days,
    )
    if require_dense_source:
        require_formal_dense_1s_timeline(
            bars,
            {"replay_purpose": "formal"},
        )
    return bars


def require_formal_dense_1s_timeline(
    bars: pd.DataFrame | None,
    params: Mapping[str, Any],
    *,
    expected_start_ms: int | None = None,
    expected_end_ms: int | None = None,
) -> pd.DataFrame | None:
    """Reject the trade-bearing-second fallback for formal evidence.

    Formal volatility, timer, and trade-intensity state requires the canonical
    wall-clock one-second timeline.  The historical sparse-trade fallback is
    retained only for explicitly exploratory runs.
    """
    formal = bool(params.get("strict_calibration", False)) or (
        str(params.get("replay_purpose", "")).strip().lower() == "formal"
    )
    if formal and (bars is None or bars.empty):
        raise RuntimeError(
            "formal replay requires canonical dense 1s bars; "
            "trade-bearing-second variance fallback is forbidden"
        )
    if not formal:
        return bars
    assert bars is not None
    index_ms = np.asarray(_index_to_epoch_ms(bars.index), dtype=np.int64)
    if (
        index_ms.ndim != 1
        or index_ms.size != len(bars)
        or np.any(index_ms % 1000 != 0)
        or np.any(np.diff(index_ms) != 1000)
    ):
        raise RuntimeError(
            "formal replay requires a unique monotonic wall-clock 1s timeline "
            "with no sparse or duplicate seconds"
        )
    if expected_start_ms is not None:
        required_first = int(expected_start_ms // 1000 * 1000)
        if int(index_ms[0]) > required_first:
            raise RuntimeError(
                "formal replay dense 1s bars do not cover the requested start boundary"
            )
    if expected_end_ms is not None:
        required_last = int((expected_end_ms - 1) // 1000 * 1000)
        if int(index_ms[-1]) < required_last:
            raise RuntimeError(
                "formal replay dense 1s bars do not cover the requested end boundary"
            )
    return bars


def require_formal_dense_variance_timeline(
    var_ts_ms: Any,
    var_ssq: Any,
    params: Mapping[str, Any],
    *,
    trades_df: pd.DataFrame | None = None,
) -> None:
    """Enforce the dense wall-clock contract at every replay-engine entry."""

    formal = bool(params.get("strict_calibration", False)) or (
        str(params.get("replay_purpose", "")).strip().lower() == "formal"
    )
    if not formal:
        return
    timestamps = np.asarray(var_ts_ms, dtype=np.int64)
    values = np.asarray(var_ssq, dtype=np.float64)
    if (
        timestamps.ndim != 1
        or timestamps.size == 0
        or values.ndim != 1
        or values.size != timestamps.size
        or np.any(timestamps % 1000 != 0)
        or np.any(np.diff(timestamps) != 1000)
        or not np.isfinite(values).all()
        or np.any(values < 0.0)
    ):
        raise RuntimeError(
            "formal replay requires a finite unique monotonic dense 1s "
            "variance timeline; sparse trade-bearing-second input is forbidden"
        )
    if trades_df is None or trades_df.empty:
        return
    trade_timestamps = trades_df["transact_time"].to_numpy(
        dtype=np.int64,
        copy=False,
    )
    required_first = int(trade_timestamps.min() // 1000 * 1000)
    required_last = int(trade_timestamps.max() // 1000 * 1000)
    if int(timestamps[0]) > required_first or int(timestamps[-1]) < required_last:
        raise RuntimeError(
            "formal replay dense 1s variance timeline does not cover the "
            "execution-event interval"
        )


def build_rolling_variance(bars_1s, sigma_window=60):
    """Pre-compute the causal live-equivalent variance-rate timeline.

    SignalEngine uses the previous at-most ``sigma_window`` one-second close
    differences and returns its explicit 1.0 warmup fallback until ten closes
    exist.  Do the same here.  In particular, never backfill an early replay
    timestamp from a variance observation that becomes available later.
    """
    cl = bars_1s["close"].values.astype(np.float64)
    ssq = np.full(cl.shape, 1.0, dtype=np.float64)
    if cl.size > 1:
        window = max(2, int(sigma_window))
        diffs = pd.Series(cl[1:] - cl[:-1])
        rolling = (
            diffs.rolling(window, min_periods=min(9, window))
            .var()
            .ffill()
            .to_numpy(dtype=np.float64)
        )
        ssq[1:] = np.where(np.isfinite(rolling), rolling, 1.0)
        ssq[: min(9, cl.size)] = 1.0
    ssq = np.maximum(ssq, 1e-6)

    # Preserve the 1s bar's left label. This row's completed close becomes
    # available no earlier than ts_ms + 1000; consumers enforce that boundary.
    ts_ms = (bars_1s.index.to_numpy(dtype=np.int64)
             if bars_1s.attrs.get("availability_clock") == "ready_base_ms_plus_1000"
             else _index_to_epoch_ms(bars_1s.index))
    return ts_ms, ssq


def build_trade_intensity(bars_1s, window=60):
    """Return per-1s count means, left-labelled and available after ts + 1s."""
    tc = bars_1s["trade_count"].values.astype(np.float64)
    ti = (pd.Series(tc)
          .rolling(window, min_periods=1)
          .mean()
          .ffill().bfill()
          .values.astype(np.float64))
    ts_ms = _index_to_epoch_ms(bars_1s.index)
    return ts_ms, ti


def build_squared_returns(bars_1s):
    """Return 1s squared returns, left-labelled and available after ts + 1s."""
    cl = bars_1s["close"].values.astype(np.float64)
    ret_sq = np.empty_like(cl)
    ret_sq[0] = 0.0
    ret_sq[1:] = (cl[1:] - cl[:-1]) ** 2
    ts_ms = _index_to_epoch_ms(bars_1s.index)
    return ts_ms, ret_sq


@lru_cache(maxsize=2048)
def _book_file_digest(path: str, size: int, mtime: int, ctime: int, inode: int) -> str:
    del size, mtime, ctime, inode
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _bound_book_digest(path: Path) -> str:
    stat = path.stat()
    return _book_file_digest(str(path.resolve()), stat.st_size, stat.st_mtime_ns,
                             stat.st_ctime_ns, stat.st_ino)


def book_observation_inputs(path: Path, *, symbol: str | None = None) -> tuple[Path, ...]:
    """Existing quality + clock triplet; no inference from output cadence or mtime."""
    day = path.stem[-10:]
    quality = path.parent.parent / "quality" / f"{symbol or SYMBOL}-{day}.json"
    if not quality.is_file():
        return (quality,)
    value = json.loads(quality.read_text())
    claim = value.get("clock_output", {})
    if not claim.get("path"):
        if value.get("gap_policy") == "carry_forward":
            raise ValueError("carried book requires a bound observation clock")
        return (quality,)
    local = path.parent.parent / "clock" / Path(claim["path"]).name
    return quality, local if local.is_file() else Path(claim["path"])


def _load_book_observations(path: Path, output_ts_ms: np.ndarray, kind: str, *, symbol: str | None = None,
                            return_usable: bool = False, bounded: bool = False):
    inputs = book_observation_inputs(path, symbol=symbol)
    if len(inputs) == 1:
        result = (np.full(len(output_ts_ms), -1, dtype=np.int64), np.zeros(len(output_ts_ms), dtype=bool))
        return (*result, np.ones(len(output_ts_ms), dtype=bool)) if return_usable else result
    quality_path, clock_path = inputs
    quality = json.loads(quality_path.read_text())
    if quality.get("day") != path.stem[-10:] or quality.get("symbol") != (symbol or SYMBOL):
        raise ValueError("book observation quality symbol/day mismatch")
    if (_bound_book_digest(path) != quality.get(f"{kind}_output", {}).get("sha256")
            or _bound_book_digest(clock_path) != quality["clock_output"].get("sha256")):
        raise ValueError("book/observation clock content binding differs")
    if bounded:
        rows = (_parquet_window_rows(clock_path, int(output_ts_ms.min()), int(output_ts_ms.max()) + 1,
                                     keep_predecessor=True)
                if len(output_ts_ms) else np.empty(0, dtype=np.int64))
        # Only the chosen channel's clock is needed, not the entire sidecar.
        names = pq.ParquetFile(clock_path).schema_arrow.names
        desired = {"timestamp", "last_observation_timestamp_us", "exchange_cut_timestamp_us",
                   "exchange_resample_age_us", "last_provider_local_timestamp_us",
                   "provider_visibility_delay_us", "observation_age_us", "observation_kind"}
        if kind == "bbo":
            desired.update(("bbo_last_observation_timestamp_us", "bbo_observation_age_us",
                            "bbo_observation_kind", "bbo_usable"))
        clock = _read_parquet_rows(clock_path, rows, columns=[name for name in names if name in desired])
    else:
        clock = pq.read_table(clock_path)
    names = clock.column_names
    separate_top = kind == "bbo" and "bbo_last_observation_timestamp_us" in names
    observation = ("bbo_last_observation_timestamp_us" if separate_top
                   else "last_observation_timestamp_us" if "last_observation_timestamp_us" in names
                   else "exchange_cut_timestamp_us" if "exchange_resample_age_us" in names
                   else "last_provider_local_timestamp_us" if "provider_visibility_delay_us" in names else None)
    if observation is None:
        raise ValueError("bound book clock lacks observation timestamps")
    def integers(name):
        column = clock[name]
        if column.null_count or not pa.types.is_integer(column.type):
            raise ValueError("book observation clocks require non-null integers")
        return column.to_numpy().astype(np.int64, copy=False)
    axis = integers("timestamp")
    usable = np.ones(len(axis), dtype=bool)
    if separate_top:
        if "bbo_usable" not in names or clock["bbo_usable"].null_count or not pa.types.is_boolean(clock["bbo_usable"].type):
            raise ValueError("independent top clock lacks its usability mask")
        usable = np.asarray(clock["bbo_usable"].to_numpy(), dtype=bool)
        if not pa.types.is_integer(clock[observation].type):
            raise ValueError("book observation clocks require integer timestamps")
        observed = np.asarray([value if value is not None else -1 for value in clock[observation].to_pylist()], dtype=np.int64)
        if np.any(usable & (observed < 0)):
            raise ValueError("unobserved top was marked usable")
    else:
        observed = integers(observation)
    known = observed != -1
    age = axis * 1000 - observed
    if (np.any(np.diff(axis) <= 0) or np.any(np.diff(observed[known]) < 0)
            or np.any(observed[known] <= 0) or np.any(age[known] < 0)):
        raise ValueError("book observation clock regresses or looks into the future")
    age_name = ("bbo_observation_age_us" if separate_top else next((n for n in (
        "observation_age_us", "exchange_resample_age_us", "provider_visibility_delay_us") if n in names), None))
    if age_name:
        if separate_top:
            if not pa.types.is_integer(clock[age_name].type):
                raise ValueError("book observation ages require integers")
            measured = np.asarray([value if value is not None else -1 for value in clock[age_name].to_pylist()], dtype=np.int64)
            if not np.array_equal(measured[known], age[known]) or np.any(measured[~known] != -1):
                raise ValueError("book observation age differs from source clock")
        elif not np.array_equal(integers(age_name), age):
            raise ValueError("book observation age differs from source clock")
    indices = np.searchsorted(axis, output_ts_ms)
    if np.any(indices >= len(axis)) or not np.array_equal(axis[indices], output_ts_ms):
        raise ValueError("book observation clock does not align with output rows")
    source_observed = np.r_[True, observed[1:] != observed[:-1]] if len(axis) else np.empty(0, dtype=bool)
    kind_name = "bbo_observation_kind" if separate_top else "observation_kind"
    if kind_name in names:
        kinds = np.asarray(clock[kind_name].to_pylist())
        allowed = ("source_observed", "carried_forward", "invalid_top", "unknown") if separate_top else ("source_observed", "carried_forward")
        if not np.all(np.isin(kinds, allowed)):
            raise ValueError("unknown book observation kind")
        if separate_top and np.any(usable & np.isin(kinds, ("invalid_top", "unknown"))):
            raise ValueError("unknown/invalid top observation was marked usable")
        source_observed = kinds == "source_observed"
        repeated = np.r_[False, observed[1:] == observed[:-1]]
        if np.any((kinds == "carried_forward")[1:] & ~repeated[1:]):
            raise ValueError("carried book refreshed its observation clock")
    result = observed[indices], source_observed[indices]
    return (*result, usable[indices]) if return_usable else result


def load_book_delivery_prefix(days, kind: str, before_ms: int, *, quality_allowed_days=()):
    """Replay only discarded pre-window source clocks, never their depth matrix.

    The initial message-delivery PRF ordinal and serialized callback watermark
    need the real historical prefix. This is not a strategy warmup or a fake
    snapshot: only the existing loader's level-1 eligibility + bound clocks are
    retained, and no orders/fills/accounting are evaluated here.
    """
    if kind not in {"bbo", "l2"}:
        raise ValueError("unknown book delivery prefix channel")
    directory, variants = ((BBO_DIR, ("bbo", "bookTicker")) if kind == "bbo"
                           else (L2_DIR, ("l2", "depth20")))
    files = filter_paths_for_orderbook_quality(_collect_snapshot_files(directory, variants, days=days),
        SYMBOL, label=kind.upper(), explicitly_allowed_days=quality_allowed_days)
    axes = []
    for path in files:
        names = pq.ParquetFile(path).schema_arrow.names
        timestamp = _first_existing_name(names, ("timestamp", "ts_ms", "transact_time", "event_time", "time", "E", "T"))
        bid = _first_existing_name(names, ("best_bid", "bid_price", "bid", "b") if kind == "bbo"
                                   else ("bid_px_1", "bid_price_1"))
        ask = _first_existing_name(names, ("best_ask", "ask_price", "ask", "a") if kind == "bbo"
                                   else ("ask_px_1", "ask_price_1"))
        if not all((timestamp, bid, ask)):
            raise ValueError("book clock prefix is missing level-1/timestamp columns")
        # Existing large-BBO reduction needs its complete last bucket too.
        large = kind == "bbo" and path.stat().st_size >= HISTORICAL_BBO_STREAM_THRESHOLD_BYTES
        end = ((before_ms + HISTORICAL_BBO_BUCKET_MS - 1) // HISTORICAL_BBO_BUCKET_MS
               * HISTORICAL_BBO_BUCKET_MS) if large else before_ms
        rows = _parquet_window_rows(path, 0, end)
        frame = _read_parquet_rows(path, rows, columns=[timestamp, bid, ask]).to_pandas()
        axis = _extract_ts_ms(frame)
        if large and len(frame):
            valid = (axis > 0) & (frame[bid].to_numpy() > 0) & (frame[ask].to_numpy() > frame[bid].to_numpy())
            frame = frame.loc[valid].copy()
            frame["_bucket"] = axis[valid] // HISTORICAL_BBO_BUCKET_MS
            frame = frame.groupby("_bucket", sort=False, as_index=False).last().drop(columns="_bucket")
            axis = _extract_ts_ms(frame)
        observed, _, usable = _load_book_observations(path, axis, kind, return_usable=True, bounded=True)
        valid = ((frame[bid].to_numpy() > 0) & (frame[ask].to_numpy() > frame[bid].to_numpy()))
        if kind == "bbo":
            valid &= np.isfinite(frame[bid].to_numpy()) & np.isfinite(frame[ask].to_numpy())
            valid |= ~usable
        # Stable-last time de-dup precedes eligibility in the original loaders.
        compact = pd.DataFrame({"timestamp": axis, "observed": observed, "valid": valid})
        axes.append(compact)
    if not axes:
        return {"ts_ms": np.empty(0, dtype=np.int64), "observation_ts_us": np.empty(0, dtype=np.int64)}
    frame = pd.concat(axes, ignore_index=True)
    frame, axis = _dedup_sorted_frame(frame, frame.timestamp.to_numpy())
    keep = (axis < before_ms) & frame.valid.to_numpy(dtype=bool)
    keep &= allowed_timestamp_mask(axis, SYMBOL, label=kind.upper(), explicitly_allowed_days=quality_allowed_days)
    return {"ts_ms": axis[keep], "observation_ts_us": frame.observed.to_numpy(dtype=np.int64)[keep]}


def load_bbo_data(days=None, *, quality_allowed_days=(), time_bounds_ms=None):
    """Load historical best-bid/best-ask snapshots for GTX simulation.

    Interface:
      - directory: data/bbo/
      - file patterns: <SYMBOL>-bbo-YYYY-MM-DD.parquet or <SYMBOL>-bookTicker-YYYY-MM-DD.parquet
      - required columns: timestamp-like field + best_bid/best_ask prices
      - optional columns: bid_qty/ask_qty (used for queue-ahead estimates)
    """
    files = _collect_snapshot_files(BBO_DIR, ("bbo", "bookTicker"), days=days)
    files = filter_paths_for_orderbook_quality(
        files,
        SYMBOL,
        label="BBO",
        explicitly_allowed_days=quality_allowed_days,
    )
    if not files:
        return None

    dfs = []
    for path in files:
        if time_bounds_ms is None:
            frame = _load_bbo_frame(path)
        else:
            names = pq.ParquetFile(path).schema_arrow.names
            bid = _first_existing_name(names, ("best_bid", "bid_price", "bid", "b"))
            ask = _first_existing_name(names, ("best_ask", "ask_price", "ask", "a"))
            read_bounds = time_bounds_ms
            large = path.stat().st_size >= HISTORICAL_BBO_STREAM_THRESHOLD_BYTES
            if large:
                # Preserve the legacy large-file reduction's complete buckets;
                # output timestamps remain the actual last observation, not
                # the bucket label, so no future observation fills a past row.
                start, end = time_bounds_ms
                bucket = HISTORICAL_BBO_BUCKET_MS
                read_bounds = (start // bucket * bucket - bucket, (end + bucket - 1) // bucket * bucket)
            rows = _parquet_window_rows(path, *read_bounds, keep_predecessor=True,
                                       predecessor_valid_columns=(bid, ask) if bid and ask else None,
                                       extra_predecessor_ms=(_last_bbo_invalidation_before(path, read_bounds[0])
                                                             if not large else None))
            frame = _read_parquet_rows(path, rows).to_pandas()
            if large and bid and ask and len(frame):
                ts = _extract_ts_ms(frame)
                valid = (ts > 0) & (frame[bid].to_numpy() > 0) & (frame[ask].to_numpy() > frame[bid].to_numpy())
                frame = frame.loc[valid].copy()
                frame["_bucket"] = ts[valid] // HISTORICAL_BBO_BUCKET_MS
                frame = frame.groupby("_bucket", sort=False, as_index=False).last().drop(columns="_bucket")
        observed, present, usable = _load_book_observations(
            path, _extract_ts_ms(frame), "bbo", return_usable=True, bounded=time_bounds_ms is not None)
        frame["_observation_ts_us"], frame["_source_observed"] = observed, present
        frame["_usable"] = usable
        dfs.append(frame)
    bbo = pd.concat(dfs, ignore_index=False, copy=False)
    del dfs
    gc.collect()

    ts_ms = _extract_ts_ms(bbo)
    bbo, ts_ms = _dedup_sorted_frame(bbo, ts_ms)
    keep = allowed_timestamp_mask(
        ts_ms,
        SYMBOL,
        label="BBO",
        explicitly_allowed_days=quality_allowed_days,
    )
    if not keep.all():
        bbo = bbo.iloc[keep].copy()
        ts_ms = ts_ms[keep]

    bid_col = _first_existing_column(bbo, ("best_bid", "bid_price", "bid", "b"))
    ask_col = _first_existing_column(bbo, ("best_ask", "ask_price", "ask", "a"))
    if bid_col is None or ask_col is None:
        print("  [WARN] Historical BBO files found but missing best bid/ask columns")
        return None

    bid_qty_col = _first_existing_column(bbo, ("bid_qty", "best_bid_qty", "bid_size", "B"))
    ask_qty_col = _first_existing_column(bbo, ("ask_qty", "best_ask_qty", "ask_size", "A"))

    best_bid = bbo[bid_col].astype(np.float64).to_numpy(copy=False)
    best_ask = bbo[ask_col].astype(np.float64).to_numpy(copy=False)
    bid_qty = (bbo[bid_qty_col].astype(np.float64).to_numpy(copy=False)
               if bid_qty_col else np.zeros(len(bbo), dtype=np.float64))
    ask_qty = (bbo[ask_qty_col].astype(np.float64).to_numpy(copy=False)
               if ask_qty_col else np.zeros(len(bbo), dtype=np.float64))

    usable = bbo["_usable"].to_numpy(dtype=bool)
    valid = np.isfinite(best_bid) & np.isfinite(best_ask) & (best_bid > 0.0) & (best_ask > best_bid)
    if not np.any(valid) and not np.any(~usable):
        print("  [WARN] Historical BBO files contain no valid spreads")
        return None

    # Preserve every explicit invalidation point.  Omitting it would make an
    # as-of query silently revive the preceding valid quote until age expiry.
    keep = valid | ~usable
    usable = usable & valid
    selected = np.flatnonzero(keep)
    keep[selected] &= _snapshot_window_mask(ts_ms[selected], time_bounds_ms)

    ts_ms = ts_ms[keep]
    best_bid = best_bid[keep]
    best_ask = best_ask[keep]
    bid_qty = bid_qty[keep]
    ask_qty = ask_qty[keep]
    print(f"  BBO data: {len(ts_ms):,} snapshots")
    return HistoricalBBOData(ts_ms, best_bid, best_ask, bid_qty, ask_qty, source="historical_bbo",
        observation_ts_us=bbo["_observation_ts_us"].to_numpy(dtype=np.int64)[keep],
        source_observed=bbo["_source_observed"].to_numpy(dtype=bool)[keep], usable=usable[keep])


def _bounded_l2_arrays(files, time_bounds_ms):
    """Keep one day's large footer alive only while decoding its bounded rows.

    A daily file can have thousands of tiny row groups: repeatedly constructing
    ParquetFile for each column family may cost more RAM than the whole window.
    No reader, metadata object, or full-day payload escapes this scoped helper.
    """
    maps = None
    levels = None
    parts = []

    def read_day(path):
        nonlocal maps, levels
        parquet = pq.ParquetFile(path)
        try:
            names = parquet.schema_arrow.names
            if maps is None:
                schema = pd.DataFrame(columns=names)
                patterns = {
                    "bid_px": (r"bid_px_(\d+)", r"bid_price_(\d+)"),
                    "bid_qty": (r"bid_qty_(\d+)", r"bid_size_(\d+)"),
                    "ask_px": (r"ask_px_(\d+)", r"ask_price_(\d+)"),
                    "ask_qty": (r"ask_qty_(\d+)", r"ask_size_(\d+)"),
                }
                maps = {key: _column_level_map(schema, tuple(re.compile(p) for p in value))
                        for key, value in patterns.items()}
                levels = sorted(set.intersection(*(set(value) for value in maps.values())))
                if not levels:
                    return None
                maximum = os.environ.get("MM_L2_MAX_LEVELS", "").strip()
                if maximum:
                    if int(maximum) <= 0:
                        raise ValueError("MM_L2_MAX_LEVELS must be > 0")
                    levels = levels[:int(maximum)]
            timestamp = _first_existing_name(names, (
                "timestamp", "ts_ms", "transact_time", "event_time", "time", "E", "T"))
            if timestamp is None:
                raise ValueError(f"No timestamp column found in historical L2 file: {path}")
            rows = _parquet_window_rows(path, *time_bounds_ms, keep_predecessor=True,
                predecessor_valid_columns=(maps["bid_px"][levels[0]], maps["ask_px"][levels[0]]),
                parquet=parquet)
            frame = _read_parquet_rows(path, rows, columns=[timestamp], parquet=parquet).to_pandas()
            axis = _extract_ts_ms(frame)
            observed, present = _load_book_observations(path, axis, "l2", bounded=True)
            matrices = {}
            for key, column_map in maps.items():
                frame = _read_parquet_rows(path, rows,
                    columns=[column_map[level] for level in levels], parquet=parquet).to_pandas()
                # Own the small window's memory, not any Arrow reader/batch owner.
                matrices[key] = frame.to_numpy(dtype=np.float64, copy=True)
            return axis, observed, present, matrices
        finally:
            parquet.close()

    for path in files:
        result = read_day(path)
        if result is None:
            print("  [WARN] Historical L2 files found but missing level-1 price/qty columns")
            return None
        parts.append(result)
    # Only bounded numpy buffers survive into the next day; never cache footers.
    if len(parts) == 1:
        return parts[0]
    return (*[np.concatenate([part[index] for part in parts]) for index in range(3)],
            {key: np.concatenate([part[3][key] for part in parts]) for key in maps})


def _full_l2_arrays(files):
    """Legacy full-day decoding; bounded reads use a scoped footer separately."""
    schema_columns = list(pq.ParquetFile(files[0]).schema.names)
    schema_frame = pd.DataFrame(columns=schema_columns)
    bid_px_map = _column_level_map(schema_frame, (re.compile(r"bid_px_(\d+)"), re.compile(r"bid_price_(\d+)")))
    bid_qty_map = _column_level_map(schema_frame, (re.compile(r"bid_qty_(\d+)"), re.compile(r"bid_size_(\d+)")))
    ask_px_map = _column_level_map(schema_frame, (re.compile(r"ask_px_(\d+)"), re.compile(r"ask_price_(\d+)")))
    ask_qty_map = _column_level_map(schema_frame, (re.compile(r"ask_qty_(\d+)"), re.compile(r"ask_size_(\d+)")))
    common_levels = sorted(set(bid_px_map) & set(bid_qty_map) & set(ask_px_map) & set(ask_qty_map))
    if not common_levels:
        print("  [WARN] Historical L2 files found but missing level-1 price/qty columns")
        return None
    maximum = os.environ.get("MM_L2_MAX_LEVELS", "").strip()
    if maximum:
        if int(maximum) <= 0:
            raise ValueError("MM_L2_MAX_LEVELS must be > 0")
        common_levels = common_levels[:int(maximum)]
    file_rows = [pq.ParquetFile(path).metadata.num_rows for path in files]
    total_rows = int(sum(file_rows))
    ts_ms = np.empty(total_rows, dtype=np.int64)
    observed = np.empty(total_rows, dtype=np.int64)
    present = np.empty(total_rows, dtype=bool)
    maps = {"bid_px": bid_px_map, "bid_qty": bid_qty_map, "ask_px": ask_px_map, "ask_qty": ask_qty_map}
    matrices = {key: np.empty((total_rows, len(common_levels)), dtype=np.float64) for key in maps}
    offset = 0
    for path, row_count in zip(files, file_rows, strict=True):
        stop = offset + row_count
        names = pq.ParquetFile(path).schema.names
        timestamp = _first_existing_name(names, ("timestamp", "ts_ms", "transact_time", "event_time", "time", "E", "T"))
        if timestamp is None:
            raise ValueError(f"No timestamp column found in historical L2 file: {path}")
        frame = pd.read_parquet(path, columns=[timestamp])
        ts_ms[offset:stop] = _extract_ts_ms(frame)
        observed[offset:stop], present[offset:stop] = _load_book_observations(path, ts_ms[offset:stop], "l2")
        del frame
        for key, column_map in maps.items():
            frame = pd.read_parquet(path, columns=[column_map[level] for level in common_levels])
            matrices[key][offset:stop] = frame.to_numpy(dtype=np.float64, copy=False)
            del frame
        offset = stop
    return ts_ms, observed, present, matrices


def load_l2_data(days=None, *, quality_allowed_days=(), time_bounds_ms=None):
    """Load historical L2 snapshots for future live-like queue/BBO simulation.

    Interface:
      - directory: data/l2/
      - file patterns: <SYMBOL>-l2-YYYY-MM-DD.parquet or <SYMBOL>-depth20-YYYY-MM-DD.parquet
      - required columns: timestamp-like field plus bid_px_1/bid_qty_1 and ask_px_1/ask_qty_1
      - optional deeper levels: bid_px_N, bid_qty_N, ask_px_N, ask_qty_N
    """
    files = _collect_snapshot_files(L2_DIR, ("l2", "depth20"), days=days)
    files = filter_paths_for_orderbook_quality(
        files,
        SYMBOL,
        label="L2",
        explicitly_allowed_days=quality_allowed_days,
    )
    if not files:
        return None

    arrays = (_full_l2_arrays(files) if time_bounds_ms is None
              else _bounded_l2_arrays(files, time_bounds_ms))
    if arrays is None:
        return None
    ts_ms, observation_ts_us, source_observed, matrices = arrays

    bid_px = matrices["bid_px"]
    bid_qty = matrices["bid_qty"]
    ask_px = matrices["ask_px"]
    ask_qty = matrices["ask_qty"]
    del matrices

    if len(ts_ms) > 1 and (np.any(ts_ms[1:] < ts_ms[:-1]) or np.any(ts_ms[1:] == ts_ms[:-1])):
        order = np.argsort(ts_ms, kind="stable")
        ts_ms = ts_ms[order]
        observation_ts_us, source_observed = observation_ts_us[order], source_observed[order]
        bid_px = bid_px[order]
        bid_qty = bid_qty[order]
        ask_px = ask_px[order]
        ask_qty = ask_qty[order]
        keep_unique = np.ones(len(ts_ms), dtype=bool)
        keep_unique[:-1] = ts_ms[:-1] != ts_ms[1:]
        ts_ms = ts_ms[keep_unique]
        observation_ts_us, source_observed = observation_ts_us[keep_unique], source_observed[keep_unique]
        bid_px = bid_px[keep_unique]
        bid_qty = bid_qty[keep_unique]
        ask_px = ask_px[keep_unique]
        ask_qty = ask_qty[keep_unique]

    keep = allowed_timestamp_mask(
        ts_ms,
        SYMBOL,
        label="L2",
        explicitly_allowed_days=quality_allowed_days,
    )
    if not keep.all():
        ts_ms = ts_ms[keep]
        observation_ts_us, source_observed = observation_ts_us[keep], source_observed[keep]
        bid_px = bid_px[keep]
        bid_qty = bid_qty[keep]
        ask_px = ask_px[keep]
        ask_qty = ask_qty[keep]

    bid_px, bid_qty, bid_dup_count = _deduplicate_l2_price_levels(bid_px, bid_qty)
    ask_px, ask_qty, ask_dup_count = _deduplicate_l2_price_levels(ask_px, ask_qty)

    valid = (bid_px[:, 0] > 0.0) & (ask_px[:, 0] > bid_px[:, 0])
    if not np.any(valid):
        print("  [WARN] Historical L2 files contain no valid level-1 spreads")
        return None
    if not valid.all():
        ts_ms = ts_ms[valid]
        observation_ts_us, source_observed = observation_ts_us[valid], source_observed[valid]
        bid_px = bid_px[valid]
        bid_qty = bid_qty[valid]
        ask_px = ask_px[valid]
        ask_qty = ask_qty[valid]
    if time_bounds_ms is not None:
        keep = _snapshot_window_mask(ts_ms, time_bounds_ms)
        ts_ms, observation_ts_us, source_observed = ts_ms[keep], observation_ts_us[keep], source_observed[keep]
        bid_px, bid_qty, ask_px, ask_qty = bid_px[keep], bid_qty[keep], ask_px[keep], ask_qty[keep]
    print(f"  L2 data: {len(ts_ms):,} snapshots × {bid_px.shape[1]} levels")
    if bid_dup_count or ask_dup_count:
        print(
            "  L2 de-duplicated repeated price levels: "
            f"bid={bid_dup_count:,}, ask={ask_dup_count:,}"
        )
    return HistoricalL2Data(ts_ms, bid_px, bid_qty, ask_px, ask_qty, source="historical_l2",
                            observation_ts_us=observation_ts_us, source_observed=source_observed)


# ═══════════════════════════════════════════════════════════════════
#  1b. ML PREDICTIONS — generate on the fly for any date range
# ═══════════════════════════════════════════════════════════════════

ML_FEATURE_BUCKET_MS = 10_000


def ml_feature_ready_timestamps_ms(
    index: pd.DatetimeIndex,
    *,
    bucket_ms: int = ML_FEATURE_BUCKET_MS,
) -> np.ndarray:
    """Return causal visibility times for left-labelled completed buckets."""
    if bucket_ms <= 0:
        raise ValueError("ML feature bucket_ms must be > 0")
    values_ns = index.to_numpy(dtype="datetime64[ns]").astype(np.int64)
    return values_ns // 1_000_000 + int(bucket_ms)


def causal_prediction_ready_indices(
    ready_ts_ms: np.ndarray,
    *,
    start_ms: int,
    end_ms: int,
) -> np.ndarray:
    """Keep the latest causal pre-window state plus all in-window updates."""
    ready = np.asarray(ready_ts_ms, dtype=np.int64)
    in_window = np.flatnonzero((ready >= int(start_ms)) & (ready <= int(end_ms)))
    prior = np.flatnonzero(ready < int(start_ms))
    if prior.size:
        return np.concatenate((prior[-1:], in_window)).astype(np.int64, copy=False)
    return in_window.astype(np.int64, copy=False)


def resolve_ml_feature_dir(*, require_training_panel: bool = False) -> Path:
    """Select an inference panel, or explicitly reproduce the training panel.

    Training provenance is not an inference interface: new dates necessarily
    have a different manifest.  ``load_ml_predictions`` validates the actual
    inference inputs, including explicitly supplied directories.  Only exact
    training-panel reproduction requires the model's recorded manifest hash.
    """
    selected = Path(os.environ.get("MM_FEATURE_DIR", FEATURES_DIR)).expanduser().resolve()
    meta_paths = sorted(MODEL_DIR.glob("*_meta.json"))
    identities: set[tuple[str, str]] = set()
    for meta_path in meta_paths:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        expected_sha = str(meta.get("feature_manifest_sha256", "") or "")
        manifest_path = str(meta.get("feature_manifest_path", "") or "")
        if expected_sha:
            identities.add((manifest_path, expected_sha))
    if not identities:
        if require_training_panel:
            raise RuntimeError(f"ML bundle has no training panel identity: {MODEL_DIR}")
        return selected
    if require_training_panel and len(identities) != 1:
        raise RuntimeError(f"ML bundle contains inconsistent feature manifests: {MODEL_DIR}")

    recorded_paths = {path for path, _ in identities}
    recorded_path = next(iter(recorded_paths)) if len(recorded_paths) == 1 else ""
    if "MM_FEATURE_DIR" not in os.environ and recorded_path:
        recorded_dir = Path(recorded_path).expanduser().resolve().parent
        if recorded_dir.exists():
            selected = recorded_dir
    if not require_training_panel:
        return selected
    _, expected_sha = next(iter(identities))
    manifest = selected / "causal_feature_manifest.json"
    if not manifest.exists():
        raise RuntimeError(
            f"Causal ML bundle requires feature manifest {manifest}; "
            "set MM_FEATURE_DIR to the matching copied panel"
        )
    actual_sha = hashlib.sha256(manifest.read_bytes()).hexdigest()
    if actual_sha != expected_sha:
        raise RuntimeError(
            f"Feature manifest hash mismatch for {MODEL_DIR}: "
            f"expected={expected_sha}, actual={actual_sha}"
        )
    return selected


def _validate_ml_inference_panel(
    feature_dir: Path, model_metadata: dict[str, dict],
) -> None:
    """Check the declared input ABI, not training dates, labels or file bytes."""
    from strategy.model_contract import (
        REQUIRED_CALENDAR_TIMESTAMP_SEMANTICS,
        REQUIRED_FEATURE_DAG_ID,
        REQUIRED_FEATURE_DAG_SHA256,
        REQUIRED_FEATURE_SEMANTICS_VERSION,
    )

    manifest_path = feature_dir / "causal_feature_manifest.json"
    panel = json.loads(manifest_path.read_text(encoding="utf-8"))
    expected = {
        "schema_version": 3,
        "symbol": SYMBOL,
        "feature_semantics_version": REQUIRED_FEATURE_SEMANTICS_VERSION,
        "feature_dag_id": REQUIRED_FEATURE_DAG_ID,
        "feature_dag_sha256": REQUIRED_FEATURE_DAG_SHA256,
        "feature_bucket_ms": ML_FEATURE_BUCKET_MS,
        "feature_ready_offset_ms": ML_FEATURE_BUCKET_MS,
        "feature_timestamp_semantics": "left_label_bucket_end",
        "feature_cutoff_semantics": "strict_exclusive_completed_bucket_end",
        "calendar_timestamp_semantics": REQUIRED_CALENDAR_TIMESTAMP_SEMANTICS,
        "microstructure_5s_semantics": (
            "trailing_five_seconds_from_causal_left_labelled_1s_bars"
        ),
    }
    for field, value in expected.items():
        if panel.get(field) != value:
            raise RuntimeError(
                f"Inference panel {manifest_path}: incompatible {field}; "
                f"expected={value!r}, actual={panel.get(field)!r}"
            )
    for name, meta in model_metadata.items():
        # Manifest schema and ready offset are panel fields, not model fields.
        for field in expected:
            if field in {"schema_version", "feature_ready_offset_ms"}:
                continue
            if meta.get(field) != panel[field]:
                raise RuntimeError(f"{name}: incompatible inference {field}")
        for field in ("market_stage", "reference_symbol"):
            if field not in meta or panel.get(field) != meta[field]:
                raise RuntimeError(f"{name}: incompatible inference {field}")


def _load_ml_inference_metadata(
    feature_dir: Path, *, toxicity_horizon_s: int = 10,
) -> dict[str, dict]:
    """Validate actual input metadata once, for prediction or cached reuse."""
    names = [
        "touch_conditioned_up_probability_10000ms", "absolute_price_variance_rate_10000ms", "touch_conditioned_price_change_fraction_10000ms",
        f"touch_side_adverse_probability_bid_{int(toxicity_horizon_s) * 1000}ms", f"touch_side_adverse_probability_ask_{int(toxicity_horizon_s) * 1000}ms",
    ]
    metadata = {
        name: json.loads((MODEL_DIR / f"{name}_meta.json").read_text(encoding="utf-8"))
        for name in names if (MODEL_DIR / f"{name}.txt").is_file()
    }
    _validate_ml_inference_panel(feature_dir, metadata)
    warmup_raw = os.environ.get("MM_FEATURE_WARMUP_DIR", "").strip()
    if warmup_raw:
        warmup = Path(warmup_raw).expanduser().resolve()
        if warmup != feature_dir:
            _validate_ml_inference_panel(warmup, metadata)
    return metadata


def ensure_model_feature_columns(
    features: pd.DataFrame,
    feature_columns: list[str],
    *,
    model_name: str,
    feature_dir: Path,
    allow_missing_features: bool,
) -> pd.DataFrame:
    if not feature_columns or len(feature_columns) != len(set(feature_columns)):
        raise RuntimeError(f"{model_name} requires unique, non-empty feature_cols")
    if not features.columns.is_unique:
        raise RuntimeError(f"Inference panel {feature_dir} has duplicate feature columns")
    missing = [column for column in feature_columns if column not in features.columns]
    if not missing:
        return features
    if not allow_missing_features:
        preview = ", ".join(missing[:12])
        suffix = " ..." if len(missing) > 12 else ""
        raise RuntimeError(
            f"{model_name} requires {len(missing)} feature column(s) absent from "
            f"{feature_dir}: {preview}{suffix}. Rebuild the causal feature panel "
            "or explicitly enable exploratory missing-feature zero fill; formal "
            "replay must not synthesize model inputs."
        )
    output = features.copy()
    output.loc[:, missing] = 0.0
    return output


def load_ml_predictions(
    trades_df,
    toxicity_horizon_s=10,
    cross_market_enabled=True,
    *,
    allow_missing_features=False,
    run_model_inference=True,
    feature_dir=None,
    require_target_feature_files=False,
    prediction_context_start_ms=None,
    prediction_context_end_ms=None,
    public_input_bundle=None,
    public_signal_engine=None,
):
    """
    Generate ML predictions at 10s resolution for the trade date range.
    Loads features from features_YYYY-MM-DD.parquet files and runs
    saved LightGBM models.

    Returns: (pred_ts_ms, pred_dir, pred_vol, pred_ret, tox_bid, tox_ask) numpy arrays
             or None if models not available.
    """
    if public_input_bundle is not None:
        from data.runtime import ConsumerBundle
        from models.replay.public_input import public_predictions
        if public_signal_engine is None:
            raise ValueError("public input requires a contract-bound SignalEngine; no old model fallback")
        return public_predictions(tuple(ConsumerBundle(public_input_bundle).frames()),
                                  public_signal_engine, XMARKET_REPLAY_FEATURE_COLUMNS)
    lgb = None
    if run_model_inference:
        try:
            import lightgbm as lgb
        except ImportError:
            print("  [WARN] LightGBM not installed, ML disabled")
            return None

    # Determine date range from trades
    ts_min = trades_df["transact_time"].iloc[0]
    ts_max = trades_df["transact_time"].iloc[-1]
    dt_min = pd.Timestamp(ts_min, unit="ms", tz="UTC")
    dt_max = pd.Timestamp(ts_max, unit="ms", tz="UTC")
    if prediction_context_start_ms is not None:
        if type(prediction_context_start_ms) is not int or prediction_context_start_ms < 0:
            raise ValueError("prediction_context_start_ms must be a nonnegative integer")
        # The first execution trade can follow several timer/compute decisions.
        # Retain the actual pre-window model state, not just the row preceding
        # that first trade, so a UTC-start clock never invents a warm watermark.
        dt_min = min(dt_min, pd.Timestamp(prediction_context_start_ms, unit="ms", tz="UTC"))
    if prediction_context_end_ms is not None:
        if type(prediction_context_end_ms) is not int or prediction_context_end_ms < 0:
            raise ValueError("prediction_context_end_ms must be a nonnegative integer")
        if prediction_context_end_ms < int(ts_max):
            raise ValueError("prediction context end precedes the final execution trade")
        dt_max = pd.Timestamp(prediction_context_end_ms, unit="ms", tz="UTC")

    # Load feature files covering the date range
    if feature_dir is not None:
        replay_feature_dir = Path(feature_dir).expanduser().resolve()
    elif run_model_inference:
        replay_feature_dir = resolve_ml_feature_dir()
    else:
        replay_feature_dir = Path(
            os.environ.get("MM_FEATURE_DIR", FEATURES_DIR)
        ).expanduser().resolve()
    if not replay_feature_dir.is_dir():
        raise FileNotFoundError(
            f"Replay feature directory does not exist: {replay_feature_dir}"
        )
    model_metadata = (
        _load_ml_inference_metadata(
            replay_feature_dir, toxicity_horizon_s=toxicity_horizon_s,
        ) if run_model_inference else {}
    )
    feature_files = sorted(replay_feature_dir.glob("features_*.parquet"))
    warmup_feature_dir_raw = os.environ.get("MM_FEATURE_WARMUP_DIR", "").strip()
    if warmup_feature_dir_raw:
        warmup_feature_dir = Path(warmup_feature_dir_raw).expanduser().resolve()
        if not warmup_feature_dir.exists():
            raise RuntimeError(
                f"MM_FEATURE_WARMUP_DIR does not exist: {warmup_feature_dir}"
            )
        feature_files = sorted(
            {
                *feature_files,
                *warmup_feature_dir.glob("features_*.parquet"),
            }
        )
    if require_target_feature_files:
        target_days = pd.date_range(
            dt_min.normalize(),
            dt_max.normalize(),
            freq="D",
        ).strftime("%Y-%m-%d")
        missing_target_files = [
            day
            for day in target_days
            if not (replay_feature_dir / f"features_{day}.parquet").is_file()
        ]
        if missing_target_files:
            raise FileNotFoundError(
                "Causal replay feature context is missing target-day files: "
                + ", ".join(missing_target_files)
            )

    dfs = []
    for f in feature_files:
        # Extract date from filename: features_2026-04-06.parquet
        tag = f.stem.replace("features_", "")
        try:
            if len(tag) == 10:  # daily: YYYY-MM-DD
                file_date = pd.Timestamp(tag, tz="UTC")
                file_end = file_date + pd.Timedelta(days=1)
            else:
                continue
        except Exception:
            continue

        if (
            file_end < dt_min - pd.Timedelta(milliseconds=ML_FEATURE_BUCKET_MS)
            or file_date > dt_max
        ):
            continue
        dfs.append(pd.read_parquet(f))

    if not dfs:
        print("  [WARN] No feature files found for trade dates, ML disabled")
        return None

    features = pd.concat(dfs).sort_index()
    features = features[~features.index.duplicated(keep="first")]

    # Feature rows are left-labelled: row t contains [t, t+10s) and is not
    # visible until bucket_end.  Filter and replay by that explicit ready time.
    pred_ts_ms = ml_feature_ready_timestamps_ms(features.index)
    ready_indices = causal_prediction_ready_indices(
        pred_ts_ms,
        start_ms=int(dt_min.value // 1_000_000),
        end_ms=int(dt_max.value // 1_000_000),
    )
    features = features.iloc[ready_indices]
    pred_ts_ms = pred_ts_ms[ready_indices]
    if len(features) == 0:
        print("  [WARN] No causally ready features overlap with trade dates")
        return None

    if not cross_market_enabled:
        cross_cols = [
            c for c in features.columns
            if c.startswith(("cv_", "ref_", "spot_", "cross_")) or "cross_market" in c
        ]
        if cross_cols:
            features.loc[:, cross_cols] = 0.0

    try:
        from research.families.f03_causal_13_head.feature_variants import apply_feature_variant, infer_variant_from_model_dir

        feature_variant = infer_variant_from_model_dir(MODEL_DIR)
        if feature_variant != "base":
            features = apply_feature_variant(features, feature_variant, symbol=SYMBOL)
            print(f"  Feature variant applied: {feature_variant}")
    except Exception as exc:
        print(f"  [WARN] feature variant augmentation failed: {exc}")

    print(f"  Features loaded: {len(features):,} rows "
          f"({features.index[0]} → {features.index[-1]})")

    # Load models
    LABEL_COLS = [
        "label_touch_conditioned_price_change_fraction_10000ms", "label_touch_conditioned_up_probability_10000ms", "label_absolute_price_variance_rate_10000ms",
        "label_touch_conditioned_price_change_fraction_30000ms", "label_touch_conditioned_up_probability_30000ms", "label_absolute_price_variance_rate_30000ms",
        "label_touch_conditioned_price_change_fraction_60000ms", "label_touch_conditioned_up_probability_60000ms", "label_absolute_price_variance_rate_60000ms",
        "label_touch_side_adverse_probability_bid_5000ms", "label_touch_side_adverse_probability_ask_5000ms",
        "label_touch_side_adverse_probability_bid_10000ms", "label_touch_side_adverse_probability_ask_10000ms",
    ]
    DROP_COLS = {"open", "high", "low", "vwap"}
    WEIGHT_COL = "sample_weight"

    # Prepare feature columns (matching training)
    base_feat_cols = [c for c in features.columns
                      if c not in LABEL_COLS and c != WEIGHT_COL
                      and c not in DROP_COLS]

    # Generate predictions for all targets
    pred_dir = np.full(len(features), 0.5, dtype=np.float64)
    pred_vol = np.zeros(len(features), dtype=np.float64)
    pred_ret = np.zeros(len(features), dtype=np.float64)
    tox_horizon = int(toxicity_horizon_s)
    tox_bid_name = f"touch_side_adverse_probability_bid_{tox_horizon * 1000}ms"
    tox_ask_name = f"touch_side_adverse_probability_ask_{tox_horizon * 1000}ms"
    pred_tox_bid = np.full(len(features), np.nan, dtype=np.float64)
    pred_tox_ask = np.full(len(features), np.nan, dtype=np.float64)

    if run_model_inference:
        for name in ["touch_conditioned_up_probability_10000ms", "absolute_price_variance_rate_10000ms", "touch_conditioned_price_change_fraction_10000ms", tox_bid_name, tox_ask_name]:
            model_path = MODEL_DIR / f"{name}.txt"
            if not model_path.exists():
                continue
            booster = lgb.Booster(model_file=str(model_path))
            meta = model_metadata[name]
            feat_cols = meta["feature_cols"]
            if booster.num_feature() != len(feat_cols) or booster.feature_name() != feat_cols:
                raise RuntimeError(f"{name}: model feature width/order differs from metadata")
            features = ensure_model_feature_columns(
                features,
                feat_cols,
                model_name=name,
                feature_dir=replay_feature_dir,
                allow_missing_features=allow_missing_features,
            )
            preds = booster.predict(features[feat_cols])

            if name == "touch_conditioned_up_probability_10000ms":
                pred_dir = preds.astype(np.float64)
            elif name == "absolute_price_variance_rate_10000ms":
                pred_vol = np.maximum(preds.astype(np.float64), 0.0)
            elif name == "touch_conditioned_price_change_fraction_10000ms":
                pred_ret = preds.astype(np.float64)
            elif name == tox_bid_name:
                pred_tox_bid = preds.astype(np.float64)
            elif name == tox_ask_name:
                pred_tox_ask = preds.astype(np.float64)

    if np.isnan(pred_tox_bid).all():
        pred_tox_bid = 1.0 - pred_dir
    if np.isnan(pred_tox_ask).all():
        pred_tox_ask = pred_dir
    pred_tox_bid = np.clip(np.nan_to_num(pred_tox_bid, nan=0.5), 0.0, 1.0)
    pred_tox_ask = np.clip(np.nan_to_num(pred_tox_ask, nan=0.5), 0.0, 1.0)

    print(
        f"  ML predictions: {len(pred_ts_ms):,} points; "
        f"dir=[{pred_dir.min():.4f},{pred_dir.max():.4f}], "
        f"vol=[{pred_vol.min():.6f},{pred_vol.max():.6f}], "
        f"ret=[{pred_ret.min():.8f},{pred_ret.max():.8f}]"
    )
    qprint(f"  tox_bid_{tox_horizon}s range: [{pred_tox_bid.min():.4f}, {pred_tox_bid.max():.4f}]")
    qprint(f"  tox_ask_{tox_horizon}s range: [{pred_tox_ask.min():.4f}, {pred_tox_ask.max():.4f}]")

    xmarket_arrays = []
    for col in XMARKET_REPLAY_FEATURE_COLUMNS:
        if col in features.columns:
            vals = pd.to_numeric(features[col], errors="coerce").fillna(0.0).to_numpy(dtype=np.float64)
        else:
            vals = np.zeros(len(features), dtype=np.float64)
        xmarket_arrays.append(vals)
    if xmarket_arrays:
        ref_avail = xmarket_arrays[XMARKET_REPLAY_FEATURE_COLUMNS.index("cv_ref_perp_available")]
        exec_spot_avail = xmarket_arrays[XMARKET_REPLAY_FEATURE_COLUMNS.index("cv_exec_spot_available")]
        ref_spot_avail = xmarket_arrays[XMARKET_REPLAY_FEATURE_COLUMNS.index("cv_ref_spot_available")]
        qprint(
            "  XMarket replay context availability: "
            f"ref_perp={np.mean(ref_avail > 0):.2%}, "
            f"exec_spot={np.mean(exec_spot_avail > 0):.2%}, "
            f"ref_spot={np.mean(ref_spot_avail > 0):.2%}"
        )

    replay_feature_columns = list(dict.fromkeys(base_feat_cols))
    replay_feature_arrays = {
        str(col): features[col].to_numpy(copy=False)
        for col in replay_feature_columns
        if col in features.columns
    }
    return (
        pred_ts_ms,
        pred_dir,
        pred_vol,
        pred_ret,
        pred_tox_bid,
        pred_tox_ask,
        *xmarket_arrays,
        replay_feature_arrays,
    )


# ═══════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════
#  2. TICK-LEVEL SIMULATION (ML-enhanced, aligned with live engine)
# ═══════════════════════════════════════════════════════════════════

def replay_elapsed_days(timestamps_ms: np.ndarray) -> float:
    """Wall-clock replay duration in days, with a one-second minimum."""
    ts = np.asarray(timestamps_ms, dtype=np.int64)
    if ts.size == 0:
        return 0.0
    elapsed_s = max(1.0, float(ts[-1] - ts[0]) / 1000.0)
    return elapsed_s / 86_400.0

def _configured_cooldown_evaluator(params):
    evaluator = params.get("cooldown_duration_policy_evaluator")
    enabled = [
        name
        for name in (
            "boolean_cooldown_policy_enabled",
            "buy_e3_cooldown_policy_enabled",
        )
        if bool(params.get(name, False))
    ]
    if enabled and evaluator is None:
        raise ValueError(
            "Enabled cooldown policy requires its replay evaluator; "
            f"static cooldown is not a substitute: {', '.join(enabled)}"
        )
    return evaluator


def _baseline_quote_action(*, route_due, forced_cancel, cancel_first,
                           pending_coalesce, enabled, updated, active):
    """The normal quote intention, before any optional E/C veto."""
    if not route_due:
        return "none"
    if forced_cancel:
        return "cancel"
    if cancel_first:
        return "cancel_first"
    if pending_coalesce:
        return "pending_coalesce"
    if enabled and updated:
        return "replace" if active else "place"
    if not enabled:
        return "pause"
    return "keep" if not updated and active else "none"


def simulate_tick(trades_df, var_ts_ms, var_ssq, params,
                  ml_data=None, bbo_data=None, l2_data=None,
                  var_ti=None, var_retsq=None, reference_event_tapes=None,
                  inventory_lifecycle_repair_data=None, inventory_lifecycle_repair_model=None,
                  historical_global_flow_data=None,
                  historical_fair_price_data=None,
                  active_order_queue_data=None,
                  exchange_book_event_tape=None,
                  variance_time_data=None,
                  ranked_toxicity_guard_binding=None, *,
                  checkpoint_at_ts_ms=None, resume_checkpoint=None,
                  resume_input_batch=False, consume_resume_checkpoint=False,
                  public_strategy=None, prefill_fork=None, response_update_fork=None, _prepared_clock_owner=None,
                  _prepared_account=None, _prepared_predictions_owner=None):
    """Compatibility adapter to the single explicit Python replay executor."""
    from narrowgate.replay.executor import ReplayExecutor, ReplayServices

    return ReplayExecutor(
        services=ReplayServices(
            ActiveOrderQueueCoverageError=ActiveOrderQueueCoverageError,
            Any=Any,
            CausalVarianceRateStream=CausalVarianceRateStream,
            CommonSidePolicyInput=CommonSidePolicyInput,
            CompetingRiskBundle=CompetingRiskBundle,
            ConsecutiveLossCooldown=ConsecutiveLossCooldown,
            DEFAULT_FLOW_HORIZONS_MS=DEFAULT_FLOW_HORIZONS_MS,
            DepthSnapshot=DepthSnapshot,
            EVENT_COLUMNS=EVENT_COLUMNS,
            EmpiricalMicropriceArtifact=EmpiricalMicropriceArtifact,
            FEE_ACCOUNTING_SEMANTICS=FEE_ACCOUNTING_SEMANTICS,
            FillSelectionScoreEnsemble=FillSelectionScoreEnsemble,
            GlobalFlowEngine=GlobalFlowEngine,
            HistoricalExchangeBookScheduler=HistoricalExchangeBookScheduler,
            HistoricalExchangeBookVisibilityScheduler=HistoricalExchangeBookVisibilityScheduler,
            HistoricalFairPriceCursor=HistoricalFairPriceCursor,
            HistoricalGlobalFlowCursor=HistoricalGlobalFlowCursor,
            HistoricalReferenceScheduler=HistoricalReferenceScheduler,
            INVENTORY_LIFECYCLE_ACCOUNTING_SEMANTICS=INVENTORY_LIFECYCLE_ACCOUNTING_SEMANTICS,
            INVENTORY_LIFECYCLE_STOP_ADD_ACTIONS=INVENTORY_LIFECYCLE_STOP_ADD_ACTIONS,
            InventoryLifecycleRepairCursor=InventoryLifecycleRepairCursor,
            InventoryLifecycleRepairProbabilityHistory=InventoryLifecycleRepairProbabilityHistory,
            LATENCY_SAMPLER_VERSION=LATENCY_SAMPLER_VERSION,
            LINEAGE_CANDIDATE_ACTION=LINEAGE_CANDIDATE_ACTION,
            LINEAGE_CONTROL_ACTION=LINEAGE_CONTROL_ACTION,
            LOCAL_ACTIONS=LOCAL_ACTIONS,
            LOSS_COOLDOWN_SEMANTICS=LOSS_COOLDOWN_SEMANTICS,
            LOT_SIZE=LOT_SIZE,
            LocalExtremeGuardConfig=LocalExtremeGuardConfig,
            LocalLifecycleBoundaryScheduler=LocalLifecycleBoundaryScheduler,
            Mapping=Mapping,
            MultiMarketPolicy=MultiMarketPolicy,
            MultiMarketPolicyConfig=MultiMarketPolicyConfig,
            MultiMarketPolicyContext=MultiMarketPolicyContext,
            NATIVE_EXCHANGE_EVENT_COLUMNS=NATIVE_EXCHANGE_EVENT_COLUMNS,
            NOOP_MODE=NOOP_MODE,
            OnlineVarianceTimeEpisode=OnlineVarianceTimeEpisode,
            Optional=Optional,
            OrderLifecyclePhase=OrderLifecyclePhase,
            OrderLifecycleRecorder=OrderLifecycleRecorder,
            OrderLifecycleV2ReplayAdapter=OrderLifecycleV2ReplayAdapter,
            POLICY_REASON_BURST=POLICY_REASON_BURST,
            POLICY_REASON_DEFENSE=POLICY_REASON_DEFENSE,
            POLICY_REASON_FILL_COOLDOWN=POLICY_REASON_FILL_COOLDOWN,
            POLICY_REASON_INV_LIMIT=POLICY_REASON_INV_LIMIT,
            POLICY_REASON_STALE_HARD=POLICY_REASON_STALE_HARD,
            PREFILL_FEATURE_CONTRACT=PREFILL_FEATURE_CONTRACT,
            Path=Path,
            PendingExposure=PendingExposure,
            PostFillQuoteResponse=PostFillQuoteResponse,
            PostFillQuoteResponseConfig=PostFillQuoteResponseConfig,
            QUEUE_VALUE_CANCEL_REENTER_ACTIONS=QUEUE_VALUE_CANCEL_REENTER_ACTIONS,
            QUEUE_VALUE_KEEP_CANCEL_ACTIONS=QUEUE_VALUE_KEEP_CANCEL_ACTIONS,
            QuantityWeightedOrderLifecycle=QuantityWeightedOrderLifecycle,
            QueueReactiveHawkesArtifact=QueueReactiveHawkesArtifact,
            QueueReactiveRuntime=QueueReactiveRuntime,
            QueueValueModelBundle=QueueValueModelBundle,
            QueueValueNetEvaluator=QueueValueNetEvaluator,
            QueueValueStateConfig=QueueValueStateConfig,
            QueueValueStateEvaluator=QueueValueStateEvaluator,
            QuotePrediction=QuotePrediction,
            QuoteState=QuoteState,
            RANDOM_PASSIVE_SAMPLER_VERSION=RANDOM_PASSIVE_SAMPLER_VERSION,
            ROOT=ROOT,
            RecoveryEventSpec=RecoveryEventSpec,
            ReplacePriceThreshold=ReplacePriceThreshold,
            ReplayHardRiskState=ReplayHardRiskState,
            ReplayOrderDepthPath=ReplayOrderDepthPath,
            ReplayRiskSelection=ReplayRiskSelection,
            RiskSelectionCandidate=RiskSelectionCandidate,
            RiskSelectionObservation=RiskSelectionObservation,
            SAFE_ADD_REARM_ACTIONS=SAFE_ADD_REARM_ACTIONS,
            SELL_ADD_SKIP_ACTIONS=SELL_ADD_SKIP_ACTIONS,
            SIGNAL_WARMUP_BARS=SIGNAL_WARMUP_BARS,
            SPREAD_CAP_COMPRESS=SPREAD_CAP_COMPRESS,
            SPREAD_CAP_PAUSE_EXPOSURE=SPREAD_CAP_PAUSE_EXPOSURE,
            STATE_CONDITIONED_REARM_ACTIONS=STATE_CONDITIONED_REARM_ACTIONS,
            SYMBOL=SYMBOL,
            SYNC_CENSOR_CODE=SYNC_CENSOR_CODE,
            SYNC_DEGRADE_SEMANTICS=SYNC_DEGRADE_SEMANTICS,
            SYNC_EVENT_CODE=SYNC_EVENT_CODE,
            StateConditionedQuotePolicy=StateConditionedQuotePolicy,
            StateConditionedRearmSpec=StateConditionedRearmSpec,
            StratifiedBernoulliLineageRandomizer=StratifiedBernoulliLineageRandomizer,
            StrictNativeOrderLifecycleV2ReplayAdapter=StrictNativeOrderLifecycleV2ReplayAdapter,
            TICK=TICK,
            TerminalPolicyRoute=TerminalPolicyRoute,
            UtcAccountingMarkRecorder=UtcAccountingMarkRecorder,
            XMARKET_REPLAY_FEATURE_COLUMNS=XMARKET_REPLAY_FEATURE_COLUMNS,
            _LATENCY_CANCEL=_LATENCY_CANCEL,
            _LATENCY_NEW=_LATENCY_NEW,
            _LATENCY_PRIVATE_FILL=_LATENCY_PRIVATE_FILL,
            _LATENCY_QUEUE_VALUE_CANCEL=_LATENCY_QUEUE_VALUE_CANCEL,
            _LATENCY_TTL_CANCEL=_LATENCY_TTL_CANCEL,
            _RANDOM_PASSIVE_SIDE_MIRROR=_RANDOM_PASSIVE_SIDE_MIRROR,
            _RANDOM_PASSIVE_TIMING_JITTER=_RANDOM_PASSIVE_TIMING_JITTER,
            _REST_GATEWAY_SLOT_NAMES=_REST_GATEWAY_SLOT_NAMES,
            _advance_monotonic_visibility_cutoff=_advance_monotonic_visibility_cutoff,
            _baseline_quote_action=_baseline_quote_action,
            _cap_label_from_fields=_cap_label_from_fields,
            _configured_cooldown_evaluator=_configured_cooldown_evaluator,
            _deterministic_decision_to_gateway_latency_ms=_deterministic_decision_to_gateway_latency_ms,
            _exec_book_visibility_delay_ms=_exec_book_visibility_delay_ms,
            _exposure_increasing=_exposure_increasing,
            _load_exec_source_stratified_visibility_profile=_load_exec_source_stratified_visibility_profile,
            _load_serial_rest_gateway_timing_profile=_load_serial_rest_gateway_timing_profile,
            _local_work_samples=_local_work_samples,
            _match_ioc_order=_match_ioc_order,
            _order_price_tick=_order_price_tick,
            _order_would_cross_book_tick=_order_would_cross_book_tick,
            _paired_exec_book_visibility_state=_paired_exec_book_visibility_state,
            _paired_exec_source_visible_ts=_paired_exec_source_visible_ts,
            _price_tick_mask=_price_tick_mask,
            _price_to_tick=_price_to_tick,
            _resolve_cap_params=_resolve_cap_params,
            _resolve_fill_cooldown_reset_policy=_resolve_fill_cooldown_reset_policy,
            _sample_serial_rest_gateway_timing_row=_sample_serial_rest_gateway_timing_row,
            _sampled_joint_exec_book_visibility_state=_sampled_joint_exec_book_visibility_state,
            _source_stratified_exec_visibility_state=_source_stratified_exec_visibility_state,
            _splitmix64=_splitmix64,
            _sync_degrade_events_for_replay=_sync_degrade_events_for_replay,
            _trade_crosses_order_tick=_trade_crosses_order_tick,
            _trade_tick_crosses_order_tick=_trade_tick_crosses_order_tick,
            _validate_historical_book_coverage=_validate_historical_book_coverage,
            _validate_replayed_live_actions=_validate_replayed_live_actions,
            adaptive_add_cooldown_config_from_params=adaptive_add_cooldown_config_from_params,
            adaptive_add_cooldown_multiplier=adaptive_add_cooldown_multiplier,
            apply_conditional_p3_outward_quote_action=apply_conditional_p3_outward_quote_action,
            apply_final_spread_cap=apply_final_spread_cap,
            apply_final_spread_cap_preserve_side=apply_final_spread_cap_preserve_side,
            apply_local_add_action=apply_local_add_action,
            apply_local_extreme_guard_context=apply_local_extreme_guard_context,
            apply_p3_side_bbo_floor=apply_p3_side_bbo_floor,
            apply_safe_add_rearm_action=apply_safe_add_rearm_action,
            asdict=asdict,
            audit_order_lifecycle_journal=audit_order_lifecycle_journal,
            book_observation_status=book_observation_status,
            book_observation_times_us=book_observation_times_us,
            book_usable_mask=book_usable_mask,
            build_daily_queue_arrays=build_daily_queue_arrays,
            build_fill_selection_feature_row=build_fill_selection_feature_row,
            build_inventory_lifecycle_repair_features=build_inventory_lifecycle_repair_features,
            build_queue_deplete_lookup=build_queue_deplete_lookup,
            build_queue_mo_lookup=build_queue_mo_lookup,
            build_queue_regime_lookup=build_queue_regime_lookup,
            build_replay_event_clock=build_replay_event_clock,
            bulk_cancel_sample_rows=bulk_cancel_sample_rows,
            candidate_role=candidate_role,
            cap_exposure_qty_by_position_value=cap_exposure_qty_by_position_value,
            choose_action=choose_action,
            choose_inventory_lifecycle_stop_add_action=choose_inventory_lifecycle_stop_add_action,
            choose_lineage_randomized_action=choose_lineage_randomized_action,
            choose_queue_value_action=choose_queue_value_action,
            choose_queue_value_cancel_reenter_action=choose_queue_value_cancel_reenter_action,
            choose_safe_add_rearm_action=choose_safe_add_rearm_action,
            choose_sell_add_skip_action=choose_sell_add_skip_action,
            choose_state_conditioned_rearm_action=choose_state_conditioned_rearm_action,
            circuit_breaker_triggered=circuit_breaker_triggered,
            compose_trade_intensity_acceleration_add_only_quote=compose_trade_intensity_acceleration_add_only_quote,
            compute_causal_path_features=compute_causal_path_features,
            compute_quote_core=compute_quote_core,
            cooldown_duration_after_same_side_fill=cooldown_duration_after_same_side_fill,
            deterministic_latency_ms=deterministic_latency_ms,
            deterministic_random_passive_unit=deterministic_random_passive_unit,
            diagnostic_counts=diagnostic_counts,
            effective_decision_to_gateway_latency_seed=effective_decision_to_gateway_latency_seed,
            effective_latency_seed=effective_latency_seed,
            emit_diagnostic_lifecycle_rows=emit_diagnostic_lifecycle_rows,
            evaluate_buy_soft_widen_release=evaluate_buy_soft_widen_release,
            evaluate_common_side_policy=evaluate_common_side_policy,
            evaluate_recovery_event=evaluate_recovery_event,
            evaluate_state_conditioned_rearm=evaluate_state_conditioned_rearm,
            executable_price_tick=executable_price_tick,
            feature_ready_time=feature_ready_time,
            fill_markout_diagnostics=fill_markout_diagnostics,
            fill_selection_actionable=fill_selection_actionable,
            generic_markout_enabled_for_role=generic_markout_enabled_for_role,
            hard_risk_reason=hard_risk_reason,
            hashlib=hashlib,
            heapq=heapq,
            inventory_cooldown_blocks_action=inventory_cooldown_blocks_action,
            inventory_cooldown_multiplier=inventory_cooldown_multiplier,
            inventory_lifecycle_fill_economic_legs=inventory_lifecycle_fill_economic_legs,
            inventory_lifecycle_flip_zero_boundary_equity=inventory_lifecycle_flip_zero_boundary_equity,
            inventory_lifecycle_side=inventory_lifecycle_side,
            inventory_lifecycle_soft_gate_config_from_params=inventory_lifecycle_soft_gate_config_from_params,
            inventory_lifecycle_soft_gate_result=inventory_lifecycle_soft_gate_result,
            inventory_role_for_quote=inventory_role_for_quote,
            json=json,
            lookup_queue_deplete_multiplier=lookup_queue_deplete_multiplier,
            lookup_queue_mo_multiplier=lookup_queue_mo_multiplier,
            lookup_queue_regime_multiplier=lookup_queue_regime_multiplier,
            loss_cooldown_config_values=loss_cooldown_config_values,
            markout_semantics=markout_semantics,
            materialize_quote_ev_feature_values=materialize_quote_ev_feature_values,
            math=math,
            next_no_poll_wait_ms=next_no_poll_wait_ms,
            normalize_action_probabilities=normalize_action_probabilities,
            normalize_inventory_lifecycle_stop_add_probabilities=normalize_inventory_lifecycle_stop_add_probabilities,
            normalize_lineage_randomization_probabilities=normalize_lineage_randomization_probabilities,
            normalize_queue_value_cancel_reenter_probabilities=normalize_queue_value_cancel_reenter_probabilities,
            normalize_queue_value_probabilities=normalize_queue_value_probabilities,
            normalize_safe_add_rearm_probabilities=normalize_safe_add_rearm_probabilities,
            normalize_sell_add_skip_probabilities=normalize_sell_add_skip_probabilities,
            normalize_state_conditioned_rearm_probabilities=normalize_state_conditioned_rearm_probabilities,
            np=np,
            opportunity_id=opportunity_id,
            pause_transition=pause_transition,
            pd=pd,
            pre_generic_sizes=pre_generic_sizes,
            prefill_features=prefill_features,
            pure_reducing_role=pure_reducing_role,
            quote_core_config_from_params=quote_core_config_from_params,
            quote_depth_from_l2_rows=quote_depth_from_l2_rows,
            random=random,
            re=re,
            reason_bucket_from_flags=reason_bucket_from_flags,
            replace=replace,
            replay_elapsed_days=replay_elapsed_days,
            replay_hard_risk_limits=replay_hard_risk_limits,
            replay_ml_enabled=replay_ml_enabled,
            require_formal_dense_variance_timeline=require_formal_dense_variance_timeline,
            resolve_inventory_soft_control_modes=resolve_inventory_soft_control_modes,
            rest_http_result_statuses=rest_http_result_statuses,
            rest_return_sample_rows=rest_return_sample_rows,
            rounded_release_quantity=rounded_release_quantity,
            runtime_compute_sample_rows=runtime_compute_sample_rows,
            spread_cap_mode_name=spread_cap_mode_name,
            start_online_variance_time_episode=start_online_variance_time_episode,
            subtract_lot_quantity=subtract_lot_quantity,
            synchronize_visibility_batch_ambiguity_to_cpp=synchronize_visibility_batch_ambiguity_to_cpp,
            terminal_pnl_decomposition=terminal_pnl_decomposition,
            terminal_policy_route=terminal_policy_route,
            trade_intensity_acceleration_inventory_role_for_target=trade_intensity_acceleration_inventory_role_for_target,
            validate_replay_initial_state=validate_replay_initial_state,
            visible_feature_snapshot=visible_feature_snapshot,
        ),
        trades_df=trades_df,
        var_ts_ms=var_ts_ms,
        var_ssq=var_ssq,
        params=params,
        ml_data=ml_data,
        bbo_data=bbo_data,
        l2_data=l2_data,
        var_ti=var_ti,
        var_retsq=var_retsq,
        reference_event_tapes=reference_event_tapes,
        inventory_lifecycle_repair_data=inventory_lifecycle_repair_data,
        inventory_lifecycle_repair_model=inventory_lifecycle_repair_model,
        historical_global_flow_data=historical_global_flow_data,
        historical_fair_price_data=historical_fair_price_data,
        active_order_queue_data=active_order_queue_data,
        exchange_book_event_tape=exchange_book_event_tape,
        variance_time_data=variance_time_data,
        ranked_toxicity_guard_binding=ranked_toxicity_guard_binding,
        checkpoint_at_ts_ms=checkpoint_at_ts_ms,
        resume_checkpoint=resume_checkpoint,
        resume_input_batch=resume_input_batch,
        consume_resume_checkpoint=consume_resume_checkpoint,
        public_strategy=public_strategy,
        prefill_fork=prefill_fork,
        response_update_fork=response_update_fork,
        _prepared_clock_owner=_prepared_clock_owner,
        _prepared_account=_prepared_account,
        _prepared_predictions_owner=_prepared_predictions_owner,
    ).run()


def _empty_i64():
    return np.empty(0, dtype=np.int64)


def _empty_f64():
    return np.empty(0, dtype=np.float64)


def _empty_f64_matrix():
    return np.empty((0, 0), dtype=np.float64)


def _as_i64_array(values):
    if values is None:
        return _empty_i64()
    return np.ascontiguousarray(values, dtype=np.int64)


def _as_f64_array(values):
    if values is None:
        return _empty_f64()
    return np.ascontiguousarray(values, dtype=np.float64)


def _as_u8_array(values):
    if values is None:
        return np.empty(0, dtype=np.uint8)
    return np.ascontiguousarray(values, dtype=np.uint8)


def _as_f64_matrix(values):
    if values is None:
        return _empty_f64_matrix()
    return np.ascontiguousarray(values, dtype=np.float64)


def _load_cpp_tick_replay():
    """Use the same native import/ABI boundary as the live consumers."""
    from strategy.native_runtime import load_native_module, validate_native_capabilities

    module = load_native_module()
    validate_native_capabilities(module, symbols=("simulate_tick_arrays_ext_policy_v3",))
    return module


_F05_REPEATED_BOOLEAN_COOLDOWN_CPP_ABI = (
    "f05_repeated_boolean_cooldown_streaming.v3"
)
_F05_CURRENT_CPP_QUALIFICATION_SCHEMA = (
    "narrowgate_cpp_current_policy_qualification.v1"
)
_F05_CURRENT_CPP_QUALIFICATION_SCOPE = "current_receive_time_full_replay_v1"
_F05_CURRENT_CPP_NATIVE_QUEUE_SCOPE = (
    "strategy_independent_native_snapshot_delta_exchange_time_v1"
)
_F05_CURRENT_CPP_QUOTE_DIAGNOSTIC_ABS_TOLERANCE = 1.0e-10
_F05_CURRENT_CPP_QUOTE_DIAGNOSTIC_ALLOWLIST = (
    "asym",
    "fair",
    "weighted_mid_proxy_shift_bps",
    "raw_asym_shift",
    "raw_distance_to_mid",
    "raw_mid_shift",
    "raw_pair_spread",
    "raw_price",
    "raw_quote_delta_to_bbo",
    "raw_quote_skew",
)

def _validate_current_cpp_qualification_receipt(
    params: Mapping[str, Any],
    *,
    evaluator: Any,
) -> Mapping[str, Any]:
    """Validate actual policy semantics and the paired comparison results."""

    path_text = str(
        params.get("cooldown_duration_policy_cpp_parity_receipt_path", "") or ""
    ).strip()
    if not path_text:
        raise RuntimeError(
            "current C++ cooldown qualification requires a real parity receipt file"
        )
    path = Path(path_text).expanduser().resolve()
    if not path.is_file():
        raise RuntimeError(
            "current C++ cooldown qualification parity receipt file is missing"
        )
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise RuntimeError(
            "current C++ cooldown qualification parity receipt is unreadable"
        ) from exc
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            "current C++ cooldown qualification parity receipt is invalid JSON"
        ) from exc
    if not isinstance(payload, dict):
        raise RuntimeError(
            "current C++ cooldown qualification parity receipt must be an object"
        )
    if payload.get("schema_version") != _F05_CURRENT_CPP_QUALIFICATION_SCHEMA:
        raise RuntimeError("current C++ cooldown qualification receipt schema drifted")
    if payload.get("status") != "qualified":
        raise RuntimeError("current C++ cooldown qualification receipt is not qualified")
    if payload.get("qualification_scope") != _F05_CURRENT_CPP_QUALIFICATION_SCOPE:
        raise RuntimeError("current C++ cooldown qualification receipt scope drifted")
    if payload.get("qualification_under_test") is not False:
        raise RuntimeError(
            "current C++ cooldown qualification receipt is marked under test"
        )

    expected_contract = json.loads(json.dumps(evaluator.checkpoint_policy_contract))
    if payload.get("policy_contract") != expected_contract:
        raise RuntimeError("current C++ cooldown qualification policy contract drifted")

    comparison = payload.get("comparison")
    if not isinstance(comparison, dict):
        raise RuntimeError(
            "current C++ cooldown qualification comparison is missing"
        )
    complete_utc_days = comparison.get("complete_utc_days")
    if (
        isinstance(complete_utc_days, bool)
        or not isinstance(complete_utc_days, int)
        or complete_utc_days < 1
    ):
        raise RuntimeError(
            "current C++ cooldown qualification lacks a complete UTC day"
        )
    if comparison.get("positive_latency") is not True:
        raise RuntimeError(
            "current C++ cooldown qualification lacks positive latency"
        )
    for side in ("buy", "sell"):
        count = comparison.get(f"{side}_policy_trigger_count")
        if isinstance(count, bool) or not isinstance(count, int) or count < 1:
            raise RuntimeError(
                f"current C++ cooldown qualification lacks a {side.upper()} trigger"
            )
    for key in (
        "action_exact",
        "order_exact",
        "fill_exact",
        "policy_exact",
        "economic_exact",
        "native_queue_exact",
    ):
        if comparison.get(key) is not True:
            raise RuntimeError(
                f"current C++ cooldown qualification comparison failed: {key}"
            )
    for key in ("action_difference_count", "order_difference_count"):
        count = comparison.get(key)
        if isinstance(count, bool) or not isinstance(count, int) or count != 0:
            raise RuntimeError(
                f"current C++ cooldown qualification comparison failed: {key}"
            )
    quote_diagnostics = comparison.get("raw_quote_diagnostics")
    if not isinstance(quote_diagnostics, dict):
        raise RuntimeError(
            "current C++ cooldown qualification raw quote diagnostics are missing"
        )
    if quote_diagnostics.get("allowlisted_fields") != list(
        _F05_CURRENT_CPP_QUOTE_DIAGNOSTIC_ALLOWLIST
    ):
        raise RuntimeError(
            "current C++ cooldown qualification raw quote diagnostic allowlist "
            "drifted"
        )
    absolute_tolerance = quote_diagnostics.get("absolute_tolerance")
    max_absolute_error = quote_diagnostics.get("max_absolute_error")
    if (
        isinstance(absolute_tolerance, bool)
        or not isinstance(absolute_tolerance, (int, float))
        or not math.isfinite(float(absolute_tolerance))
        or float(absolute_tolerance)
        != _F05_CURRENT_CPP_QUOTE_DIAGNOSTIC_ABS_TOLERANCE
    ):
        raise RuntimeError(
            "current C++ cooldown qualification raw quote diagnostic tolerance "
            "drifted"
        )
    if (
        isinstance(max_absolute_error, bool)
        or not isinstance(max_absolute_error, (int, float))
        or not math.isfinite(float(max_absolute_error))
        or float(max_absolute_error) < 0.0
        or float(max_absolute_error) > float(absolute_tolerance)
    ):
        raise RuntimeError(
            "current C++ cooldown qualification raw quote diagnostic error "
            "exceeded tolerance"
        )
    difference_count = quote_diagnostics.get("difference_count")
    if (
        isinstance(difference_count, bool)
        or not isinstance(difference_count, int)
        or difference_count < 0
    ):
        raise RuntimeError(
            "current C++ cooldown qualification raw quote diagnostic difference "
            "count is invalid"
        )
    non_numeric_difference_count = quote_diagnostics.get(
        "non_numeric_difference_count"
    )
    if (
        isinstance(non_numeric_difference_count, bool)
        or not isinstance(non_numeric_difference_count, int)
        or non_numeric_difference_count != 0
    ):
        raise RuntimeError(
            "current C++ cooldown qualification raw quote diagnostics contain "
            "non-numeric differences"
        )
    non_allowlisted_difference_count = quote_diagnostics.get(
        "non_allowlisted_difference_count"
    )
    if (
        isinstance(non_allowlisted_difference_count, bool)
        or not isinstance(non_allowlisted_difference_count, int)
        or non_allowlisted_difference_count != 0
    ):
        raise RuntimeError(
            "current C++ cooldown qualification has non-allowlisted quote "
            "differences"
        )
    if quote_diagnostics.get("within_tolerance") is not True:
        raise RuntimeError(
            "current C++ cooldown qualification raw quote diagnostics failed"
        )
    if comparison.get("native_queue_scope") != _F05_CURRENT_CPP_NATIVE_QUEUE_SCOPE:
        raise RuntimeError(
            "current C++ cooldown qualification native queue scope drifted"
        )
    return payload


def _validate_f05_cpp_cooldown_runtime(
    params: dict[str, Any],
    *,
    require_full_replay: bool,
) -> Any | None:
    evaluator = _configured_cooldown_evaluator(params)
    if evaluator is None:
        if any(
            params.get(name) is not None
            for name in (
                "cooldown_duration_policy_cpp_runtime",
                "_cooldown_duration_policy_cpp_window_tape",
                "_cooldown_duration_policy_cpp_window_arrays",
                "_cooldown_duration_policy_cpp_window_tape_handle",
                "_cooldown_duration_policy_cpp_predicate_rows",
            )
        ) or bool(
            params.get("cooldown_duration_policy_cpp_parity_qualified", False)
        ) or bool(
            params.get(
                "cooldown_duration_policy_cpp_qualification_under_test",
                False,
            )
        ):
            raise RuntimeError(
                "C++ cooldown runtime was supplied without the Python-authoritative "
                "policy evaluator"
            )
        return None
    parity_claimed = bool(
        params.get("cooldown_duration_policy_cpp_parity_qualified", False)
    )
    qualification_under_test = bool(
        params.get(
            "cooldown_duration_policy_cpp_qualification_under_test",
            False,
        )
    )
    paired_under_test = bool(
        params.get(
            "cooldown_duration_policy_cpp_paired_execution_under_test",
            False,
        )
    )
    event_loop_claimed = bool(
        params.get(
            "cooldown_duration_policy_cpp_event_loop_parity_qualified",
            False,
        )
    )
    receipt_sha256 = str(
        params.get("cooldown_duration_policy_cpp_parity_receipt_sha256", "")
        or ""
    )
    if qualification_under_test:
        if parity_claimed or event_loop_claimed:
            raise RuntimeError(
                "C++ cooldown qualification-under-test cannot coexist with "
                "parity qualification or a receipt"
            )
        if not paired_under_test:
            raise RuntimeError(
                "C++ cooldown qualification-under-test is paired-execution only"
            )
        if bool(params.get("replay_promotion_eligible", False)):
            raise RuntimeError(
                "C++ cooldown qualification-under-test cannot be promotion eligible"
            )
    elif paired_under_test:
        raise RuntimeError(
            "paired C++ cooldown execution requires qualification-under-test"
        )
    elif not parity_claimed:
        raise NotImplementedError(
            "repeated multichannel Boolean cooldown remains Python-authoritative; "
            "C++ requires an explicit parity-qualified flag and bound runtime"
        )

    cpp = _load_cpp_tick_replay()
    actual_abi = str(
        getattr(cpp, "F05_REPEATED_BOOLEAN_COOLDOWN_ABI_VERSION", "") or ""
    )
    if actual_abi != _F05_REPEATED_BOOLEAN_COOLDOWN_CPP_ABI:
        raise RuntimeError(
            "repeated cooldown C++ ABI drifted: "
            f"expected {_F05_REPEATED_BOOLEAN_COOLDOWN_CPP_ABI}, got {actual_abi!r}"
        )

    runtime = params.get("cooldown_duration_policy_cpp_runtime")
    if runtime is None or not isinstance(
        runtime,
        cpp.F05RepeatedBooleanCooldownRuntime,
    ):
        raise RuntimeError(
            "explicit C++ cooldown qualification lacks its bound streaming runtime"
        )
    if qualification_under_test:
        if (
            not bool(runtime.qualification_under_test)
            or bool(runtime.parity_qualified)
        ):
            binding_error = str(
                runtime.binding_error or "cpp_qualification_under_test_not_admitted"
            )
            raise RuntimeError(
                "bound C++ cooldown runtime is not admitted under test: "
                + binding_error
            )
    elif not bool(runtime.parity_qualified):
        binding_error = str(runtime.binding_error or "cpp_parity_not_qualified")
        raise RuntimeError(
            "bound C++ cooldown runtime is not parity-qualified: " + binding_error
        )

    runtime_config = runtime.config
    qualification_scope = str(runtime_config.qualification_scope or "")
    event_loop_qualified = event_loop_claimed
    if qualification_under_test:
        if bool(runtime_config.parity_qualified):
            raise RuntimeError(
                "C++ cooldown qualification-under-test runtime carries parity authority"
            )
    else:
        if qualification_scope == _F05_CURRENT_CPP_QUALIFICATION_SCOPE:
            _validate_current_cpp_qualification_receipt(
                params,
                evaluator=evaluator,
            )
            # Current-policy event-loop authority is a consequence of the
            # validated paired comparison, never of the caller's boolean.
            event_loop_qualified = True
            params[
                "_cooldown_duration_policy_cpp_validated_receipt_sha256"
            ] = receipt_sha256
            params[
                "_cooldown_duration_policy_cpp_event_loop_receipt_qualified"
            ] = True

    from strategy.native_cooldown import compile_policy

    policies = getattr(evaluator, "_policies", None)
    if policies is not None:
        if set(policies) != {"BUY", "SELL"}:
            raise RuntimeError("C++ cooldown evaluator requires BUY/SELL policies")
        expected_policies = (
            ("SELL", runtime_config.policy, policies["SELL"], False),
            ("BUY", runtime_config.buy_policy, policies["BUY"], True),
        )
    else:
        expected_policies = (
            ("SELL", runtime_config.policy, SimpleNamespace(evaluator=evaluator), False),
        )
    for side, compiled_policy, policy, declarative in expected_policies:
        expected = compile_policy(cpp, policy, declarative=declarative)
        if not compiled_policy.semantic_equal(expected):
            raise RuntimeError(
                f"C++ cooldown runtime differs from Python-authoritative {side} policy semantics"
            )
    if qualification_under_test and qualification_scope != (
        _F05_CURRENT_CPP_QUALIFICATION_SCOPE
    ):
        raise RuntimeError(
            "C++ cooldown qualification-under-test is limited to current receive-time"
        )
    if require_full_replay:
        if qualification_scope not in {
            "synthetic_full_replay_smoke",
            "real_day_all_arm_full_replay_v21",
            "real_day_all_arm_full_replay_v22",
            "real_day_all_arm_full_replay_v23",
            "current_receive_time_full_replay_v1",
        }:
            raise NotImplementedError(
                "the bound C++ cooldown runtime is qualified only for synthetic "
                "mechanics parity, not the synthetic full-replay smoke path"
            )
        if not qualification_under_test and not event_loop_qualified:
            raise NotImplementedError(
                "the C++ cooldown streaming ABI is qualified, but full-replay "
                "selection remains rejected until event-loop parity is explicitly "
                "qualified"
            )
    if qualification_scope not in {
        "synthetic_mechanics_only",
        "synthetic_full_replay_smoke",
        "real_day_all_arm_full_replay_v21",
        "real_day_all_arm_full_replay_v22",
        "real_day_all_arm_full_replay_v23",
        "current_receive_time_full_replay_v1",
    }:
        raise RuntimeError("C++ cooldown qualification scope is invalid")
    return runtime


def _copy_quote_config_to_cpp(py_cfg: Any, cpp_cfg: Any) -> Any:
    missing = [
        name for name in QUOTE_CORE_UNIT_ABI_FIELDS if not hasattr(cpp_cfg, name)
    ]
    if missing:
        raise RuntimeError(
            "narrowgate_cpp QuoteCoreConfig ABI missing fields: "
            + ", ".join(missing)
        )
    for name in _CPP_CFG_FIELDS:
        if hasattr(py_cfg, name) and hasattr(cpp_cfg, name):
            setattr(cpp_cfg, name, getattr(py_cfg, name))
    return cpp_cfg


def _load_cpp_fill_selection_models(cpp: Any, model_path: Any) -> list[Any]:
    """Convert the live JSON fill-selection ensemble into compact C++ folds."""
    path = Path(model_path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    raw_folds = payload.get("folds", [])
    if not raw_folds and isinstance(payload.get("model"), dict):
        raw_folds = [{"model": payload["model"]}]
    out = []
    for raw in raw_folds:
        model = raw.get("model", raw) if isinstance(raw, dict) else {}
        if not isinstance(model, dict):
            continue
        fold = cpp.FillSelectionFoldModel()
        fold.base_logit = float(model.get("base_logit", 0.0) or 0.0)
        fold.contribution_scale = float(model.get("contribution_scale", 0.35) or 0.35)
        fold.numeric_cuts = {
            str(k): [float(x) for x in (v or [])]
            for k, v in (model.get("numeric_cuts", {}) or {}).items()
        }
        fold.contributions = {
            str(feature): {str(bucket): float(value) for bucket, value in (buckets or {}).items()}
            for feature, buckets in (model.get("contributions", {}) or {}).items()
        }
        fold.categorical_features = [str(x) for x in (model.get("categorical_features", []) or [])]
        out.append(fold)
    if not out:
        raise ValueError(f"fill-selection model JSON has no usable folds: {path}")
    return out


_CPP_BUY_FILL_DYNAMIC_NUMERIC_FEATURES = frozenset({
    "inventory_ratio",
    "l2_book_cancel_ratio",
    "l2_book_refresh_ratio",
    "l2_near_depth_total",
    "markout_ema",
    "weighted_mid_proxy_shift_bps",
    "near_depth_total",
    "order_exposure_increasing",
    "queue_local_rank",
    "quote_distance_bps",
    "toxicity",
})
_CPP_BUY_FILL_DYNAMIC_CATEGORICAL_FEATURES = frozenset({
    "side",
    "quote_action",
    "quote_allow_post",
    "quote_allow_exposure_increase",
    "order_exposure_increasing",
    "fill_eligible",
})


def _compile_cpp_fill_selection_static_rows(
    model_path: Any,
    ml_ts: Any,
    prediction_feature_arrays: dict[str, Any],
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Compile non-quote-time scorer contributions by causal ML-ready row.

    Python owns arbitrary ``Prediction.feature_dict`` and categorical
    materialization. C++ owns fields overwritten by the current quote context.
    Keeping the boundary explicit avoids silently treating a future model's
    causal feature as missing while preserving a compact replay hot loop.
    """
    path = Path(model_path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    raw_folds = payload.get("folds", [])
    if not raw_folds and isinstance(payload.get("model"), dict):
        raw_folds = [{"model": payload["model"]}]
    models = [
        raw.get("model", raw)
        for raw in raw_folds
        if isinstance(raw, dict) and isinstance(raw.get("model", raw), dict)
    ]
    if not models:
        raise ValueError(f"fill-selection model JSON has no usable folds: {path}")

    ready_ts = np.asarray(ml_ts, dtype=np.int64).reshape(-1)
    n_ready_rows = int(ready_ts.size)
    n_folds = len(models)
    # Row 0 is the causal cold-start state before the first completed feature
    # bucket. Ready feature row i is stored at i + 1.
    delta = np.zeros((n_ready_rows + 1, n_folds), dtype=np.float64)
    missing = np.zeros((n_ready_rows + 1, n_folds), dtype=np.float64)
    used = np.zeros((n_ready_rows + 1, n_folds), dtype=np.float64)
    arrays = {
        str(name): np.asarray(values)
        for name, values in (prediction_feature_arrays or {}).items()
    }
    bad_lengths = {
        name: int(values.shape[0])
        for name, values in arrays.items()
        if values.ndim == 0 or int(values.shape[0]) != n_ready_rows
    }
    if bad_lengths:
        sample = ", ".join(f"{k}={v}" for k, v in list(bad_lengths.items())[:5])
        raise ValueError(
            "Prediction.feature_dict arrays must align with ml_ts_ms: "
            f"expected={n_ready_rows}, got {sample}"
        )

    for static_row_idx in range(n_ready_rows + 1):
        prediction_idx = static_row_idx - 1
        row: dict[str, Any] = {"side": "BUY"}
        if prediction_idx >= 0:
            for name, values in arrays.items():
                value = values[prediction_idx]
                row[name] = value.item() if isinstance(value, np.generic) else value
        materialize_quote_ev_feature_values(row)

        for fold_idx, model in enumerate(models):
            scale = float(model.get("contribution_scale", 0.35) or 0.35)
            contributions = model.get("contributions", {}) or {}
            for feature, raw_cuts in (model.get("numeric_cuts", {}) or {}).items():
                if feature in _CPP_BUY_FILL_DYNAMIC_NUMERIC_FEATURES:
                    continue
                value = row.get(feature)
                try:
                    numeric = float(value)
                except (TypeError, ValueError):
                    numeric = math.nan
                if not math.isfinite(numeric):
                    missing[static_row_idx, fold_idx] += 1.0
                    bucket = "missing"
                else:
                    cuts = np.asarray(raw_cuts or [], dtype=np.float64)
                    bucket = f"b{int(np.searchsorted(cuts, numeric, side='right')):02d}"
                contribution = (contributions.get(feature, {}) or {}).get(bucket)
                if contribution is not None:
                    delta[static_row_idx, fold_idx] += scale * float(contribution)
                    used[static_row_idx, fold_idx] += 1.0

            for feature in model.get("categorical_features", []) or []:
                if feature in _CPP_BUY_FILL_DYNAMIC_CATEGORICAL_FEATURES:
                    continue
                value = row.get(feature, "")
                text = str(value or "").strip()
                if not text:
                    missing[static_row_idx, fold_idx] += 1.0
                    bucket = "missing"
                else:
                    bucket = text[:80]
                contribution = (contributions.get(feature, {}) or {}).get(bucket)
                if contribution is not None:
                    delta[static_row_idx, fold_idx] += scale * float(contribution)
                    used[static_row_idx, fold_idx] += 1.0

    return (
        np.ascontiguousarray(delta),
        np.ascontiguousarray(missing),
        np.ascontiguousarray(used),
    )


def _normalise_queue_ahead_mode(value) -> str:
    mode = str(value or "exact_level").lower()
    if mode in ("exact", "exact_level"):
        return "exact_level"
    if mode in ("provider", "provider_visible_level"):
        return "provider_visible_level"
    raise ValueError(
        "queue_ahead_mode must be exact_level or provider_visible_level"
    )


def _compile_cpp_queue_calibration_tables(queue_calibration: dict) -> dict:
    """Compile queue calibration JSON into small C++ vector tables.

    Python replay uses dict lookups keyed by side/dist/rank/reason.  The C++
    hot path cannot afford that object shape, so we flatten the default OOS
    calibration payload into deterministic vectors.  Day-specific lookup is
    intentionally resolved before C++ by the canonical loader; if no per-day
    payload exists, queue_calibration.py already installs ``__default__``.
    """
    if not queue_calibration:
        return {}

    out: dict[str, list[float]] = {}
    regime_lookup = build_queue_regime_lookup(queue_calibration)
    regime_payload = regime_lookup.get("__default__")
    if not regime_payload and regime_lookup:
        first_key = sorted(str(k) for k in regime_lookup.keys() if str(k) != "__default__")[0]
        regime_payload = regime_lookup.get(first_key)
    if regime_payload:
        distance_edges = [float(x) for x in regime_payload.get("distance_edges", [])]
        rank_edges = [float(x) for x in regime_payload.get("rank_edges", [])]
        groups = regime_payload.get("groups") or {}
        reasons = ["none", "adverse", "markout", "thin_depth", "markout_thin"]
        dist_bins = len(distance_edges) + 1
        rank_bins = len(rank_edges) + 1
        out["queue_regime_distance_edges"] = distance_edges
        out["queue_regime_rank_edges"] = rank_edges
        for side, key_name in (("BUY", "queue_regime_buy_mult"), ("SELL", "queue_regime_sell_mult")):
            table: list[float] = []
            for dist_bin in range(dist_bins):
                for rank_bin in range(rank_bins):
                    wildcard = float(groups.get(f"{side}|{dist_bin}|{rank_bin}|*", 1.0))
                    for reason in reasons:
                        table.append(float(groups.get(f"{side}|{dist_bin}|{rank_bin}|{reason}", wildcard)))
            out[key_name] = table

    deplete_lookup = build_queue_deplete_lookup(queue_calibration)
    deplete_payload = deplete_lookup.get("__default__")
    if not deplete_payload and deplete_lookup:
        first_key = sorted(str(k) for k in deplete_lookup.keys() if str(k) != "__default__")[0]
        deplete_payload = deplete_lookup.get(first_key)
    if deplete_payload:
        rank_edges = [float(x) for x in deplete_payload.get("rank_edges", [])]
        groups = deplete_payload.get("groups") or {}
        rank_bins = len(rank_edges) + 1
        out["queue_deplete_rank_edges"] = rank_edges
        for side, key_name in (("BUY", "queue_deplete_buy_mult"), ("SELL", "queue_deplete_sell_mult")):
            out[key_name] = [float(groups.get(f"{side}|{rank_bin}", 1.0)) for rank_bin in range(rank_bins)]

    mo_lookup = build_queue_mo_lookup(queue_calibration)
    mo_payload = mo_lookup.get("__default__")
    if not mo_payload and mo_lookup:
        first_key = sorted(str(k) for k in mo_lookup.keys() if str(k) != "__default__")[0]
        mo_payload = mo_lookup.get(first_key)
    if mo_payload:
        mo_edges = [float(x) for x in mo_payload.get("mo_edges", [])]
        groups = mo_payload.get("groups") or {}
        mo_bins = len(mo_edges) + 1
        out["queue_mo_edges"] = mo_edges
        for side, key_name in (("BUY", "queue_mo_buy_mult"), ("SELL", "queue_mo_sell_mult")):
            out[key_name] = [float(groups.get(f"{side}|{mo_bin}", 1.0)) for mo_bin in range(mo_bins)]

    return out


_CONDITIONAL_P3_SCALAR_ADAPTER_RUNNER_ID = (
    "f02.conditional_p3_scalar_compression_adapter"
)
_CONDITIONAL_P3_SCALAR_ADAPTER_SPEC_SHA256 = (
    "c88e99fdeb69b0654a4b4a79c3e31bda190ffdfaf61f3b6b82db78298016c59a"
)


def _require_conditional_p3_historical_reproduction(
    params: Mapping[str, Any],
) -> dict[str, Any]:
    """Fail closed unless the retired scalar adapter reproduces its frozen Spec."""
    runner_id = str(params.get("historical_reproduction_runner_id", "") or "")
    if runner_id != _CONDITIONAL_P3_SCALAR_ADAPTER_RUNNER_ID:
        raise ValueError(
            "conditional P3 scalar compression is closed; set "
            f"historical_reproduction_runner_id={_CONDITIONAL_P3_SCALAR_ADAPTER_RUNNER_ID!r}"
        )
    supplied_sha256 = str(
        params.get("historical_reproduction_spec_sha256", "") or ""
    )
    if supplied_sha256 != _CONDITIONAL_P3_SCALAR_ADAPTER_SPEC_SHA256:
        raise ValueError(
            "conditional P3 historical reproduction requires the exact closed "
            f"Spec SHA256 {_CONDITIONAL_P3_SCALAR_ADAPTER_SPEC_SHA256}"
        )

    from research.governance.historical_reproduction import (
        require_historical_reproduction,
    )

    identity = require_historical_reproduction(
        runner_id=runner_id,
        enabled=bool(params.get("historical_reproduction", False)),
        spec_path=params.get("historical_reproduction_spec_path"),
    )
    if identity["spec_sha256"] != _CONDITIONAL_P3_SCALAR_ADAPTER_SPEC_SHA256:
        raise ValueError("conditional P3 registered historical Spec identity drift")
    if identity["research_authority"] != "historical_evidence_only":
        raise ValueError("conditional P3 historical authority marker drift")
    return identity


def _conditional_p3_historical_output_marker(
    identity: Mapping[str, Any],
) -> dict[str, Any]:
    if identity.get("research_authority") != "historical_evidence_only":
        raise ValueError("conditional P3 output requires historical evidence authority")
    return {
        "historical_reproduction": dict(identity),
        "research_authority": "historical_evidence_only",
        "new_experiment_identity_allowed": False,
        "action_or_live_authorization": False,
        "conditional_p3_adapter_scope": (
            "pair_average_scalar_delta_star_kappa_adapter"
        ),
    }


def _materialize_cpp_native_exchange_book_tape(exchange_book_event_tape):
    """Flatten one re-iterable native tape for the callback-free C++ loop."""

    event_ts_ns = array.array("q")
    event_type = array.array("B")
    receive_ts_ns = array.array("q")
    event_time_ms = array.array("q")
    transaction_time_ms = array.array("q")
    first_update_id = array.array("q")
    final_update_id = array.array("q")
    previous_final_update_id = array.array("q")
    last_update_id = array.array("q")
    level_offsets = array.array("q", [0])
    level_is_bid = array.array("B")
    level_price_tick = array.array("q")
    level_quantity = array.array("d")
    type_code = {"snapshot": 1, "delta": 2, "source_gap": 3}
    previous_ts_ns = 0

    for event in exchange_book_event_tape:
        ts_ns = int(event.exchange_ts_ns)
        if ts_ns <= 0 or ts_ns < previous_ts_ns:
            raise ValueError(
                "native exchange-book tape must be positive and exchange-time sorted"
            )
        previous_ts_ns = ts_ns
        kind = str(event.event_type)
        if kind not in type_code:
            raise ValueError(f"unsupported native exchange-book event_type={kind!r}")
        event_ts_ns.append(ts_ns)
        event_type.append(type_code[kind])
        receive_ts_ns.append(int(event.local_receive_ts_ns or 0))
        event_time_ms.append(int(event.event_time_ns or 0) // 1_000_000)
        transaction_time_ms.append(
            int(event.transaction_time_ns or 0) // 1_000_000
        )
        for output, value in (
            (first_update_id, event.first_update_id),
            (final_update_id, event.final_update_id),
            (previous_final_update_id, event.previous_final_update_id),
            (last_update_id, event.last_update_id),
        ):
            output.append(-1 if value is None else int(value))
        for side, price_tick, quantity in event.levels:
            normalized_side = str(side).strip().lower()
            if normalized_side not in {"bid", "ask", "buy", "sell"}:
                raise ValueError(
                    f"unsupported native exchange-book side={side!r}"
                )
            value = float(quantity)
            if not math.isfinite(value) or value < 0.0:
                raise ValueError(
                    "native exchange-book quantity must be finite and non-negative"
                )
            level_is_bid.append(int(normalized_side in {"bid", "buy"}))
            level_price_tick.append(int(price_tick))
            level_quantity.append(value)
        level_offsets.append(len(level_price_tick))

    if not event_ts_ns:
        raise ValueError("native exchange-book tape is empty")
    return {
        "event_ts_ns": np.frombuffer(event_ts_ns, dtype=np.int64),
        "event_type": np.frombuffer(event_type, dtype=np.uint8),
        "receive_ts_ns": np.frombuffer(receive_ts_ns, dtype=np.int64),
        "event_time_ms": np.frombuffer(event_time_ms, dtype=np.int64),
        "transaction_time_ms": np.frombuffer(
            transaction_time_ms, dtype=np.int64
        ),
        "first_update_id": np.frombuffer(first_update_id, dtype=np.int64),
        "final_update_id": np.frombuffer(final_update_id, dtype=np.int64),
        "previous_final_update_id": np.frombuffer(
            previous_final_update_id, dtype=np.int64
        ),
        "last_update_id": np.frombuffer(last_update_id, dtype=np.int64),
        "level_offsets": np.frombuffer(level_offsets, dtype=np.int64),
        "level_is_bid": np.frombuffer(level_is_bid, dtype=np.uint8),
        "level_price_tick": np.frombuffer(level_price_tick, dtype=np.int64),
        "level_quantity": np.frombuffer(level_quantity, dtype=np.float64),
        "strict_after_ns": int(
            getattr(exchange_book_event_tape, "day_start_ns", 0)
        ),
    }


def _simulate_tick_cpp(trades_df, var_ts_ms, var_ssq, params,
                       ml_data=None, bbo_data=None, l2_data=None,
                       var_ti=None, var_retsq=None,
                       historical_fair_price_data=None,
                       exchange_book_event_tape=None):
    """Run the experimental C++ replay engine and adapt its summary to Python output keys.

    中文说明：C++ replay 是加速/一致性验证工具，不是当前 live 参数选择的
    权威引擎。凡是 Python 新增但 C++ 未补齐 parity 的机制必须 fail-fast。
    """
    if params.get('_research_generic_markout_policy_enabled', True) is not True:
        raise NotImplementedError('generic markout ablation requires the Python replay')
    if resolve_inventory_soft_control_modes(params) != ("original", "original_all_roles"):
        raise NotImplementedError('inventory soft-control study requires the Python replay')
    if params.get('_cooldown_rearm_runtime') is not None:
        raise NotImplementedError('state recovery study requires the Python replay')
    if params.get('_quote_price_runtime') is not None:
        raise NotImplementedError('quote-price policy requires the Python replay')
    if params.get("replay_main_loop_wait_mode", "poll") != "poll":
        raise NotImplementedError("event-deadline main loop requires the Python replay")
    if params.get("defense_urgency_mode", "B0") != "B0":
        raise NotImplementedError("conditional D/U requires the Python replay with scalar quote consumer")
    if params.get("response_action_policy") is not None:
        raise NotImplementedError("frozen response action runtime requires Python replay")
    if params.get("quote_schedule_mode", "existing") != "existing":
        raise NotImplementedError("state-event quote scheduling requires Python replay")
    if params.get("replace_price_threshold_mode", "fixed") != "fixed":
        raise NotImplementedError("directional replace thresholds require Python replay")
    if bbo_data is not None and not np.all(book_usable_mask(bbo_data)):
        raise NotImplementedError("C++ replay does not yet support explicit BBO invalidation masks; use Python replay")
    validate_replay_initial_state(params.get("initial_live_state"), backend="cpp")
    if params.get("signal_cold_start"):
        raise ValueError("cold signal startup is Python-only")
    if (params.get("risk_selection_collect_opportunities", False)
            or params.get("risk_selection_intervention") is not None
            or params.get("_risk_selection_opportunity_sink") is not None
            or params.get("risk_selection_policy") is not None
            or params.get("risk_selection_mode", "B") != "B"
            or params.get("risk_selection_control", "learned") != "learned"):
        raise NotImplementedError("E/C opportunity replay is Python-authoritative")
    if runtime_compute_sample_rows(params):
        raise ValueError("path-conditioned runtime compute continuation is Python-only")
    if "last_requote_ts_ms" in (params.get("initial_live_state") or {}):
        raise ValueError("C++ replay cannot restore last_requote_ts_ms; use Python replay")
    for key in ("_requote_tail_work_samples_ms", "_main_loop_work_samples_ms"):
        work_samples = np.asarray(params.get(key, []), dtype=np.float64)
        if work_samples.size and np.any(work_samples != 0.0):
            raise ValueError("main-loop local work continuation is Python-only")
    pre_snapshot_samples = _as_f64_array(
        params.get("_pre_snapshot_compute_latency_samples_ms")
    )
    if pre_snapshot_samples.size and np.any(pre_snapshot_samples > 0.0):
        raise ValueError("pre-snapshot compute continuation is Python-only")
    private_fill_latency_samples = _as_f64_array(
        params.get("_private_fill_visibility_latency_samples_ms")
    )
    if private_fill_latency_samples.size and np.any(
        private_fill_latency_samples > 0.0
    ):
        raise ValueError(
            "private-fill exchange/local-visibility replay is Python-only"
        )
    decision_to_gateway_latency_samples = _as_f64_array(
        params.get("_decision_to_gateway_latency_samples_ms")
    )
    decision_to_gateway_latency_enabled = bool(
        decision_to_gateway_latency_samples.size
        and np.any(decision_to_gateway_latency_samples > 0.0)
    )
    if (
        decision_to_gateway_latency_enabled
        and str(params.get("replay_purpose", "")).strip().lower()
        != "diagnostic"
    ):
        raise ValueError(
            "decision-to-gateway compute latency is diagnostic-only"
        )
    ber_exposure_add_only = bool(params.get("ber_exposure_add_only", False))
    conditional_p3_requested = bool(
        _as_i64_array(params.get("_conditional_p3_ts_ms")).size
        or _as_f64_array(params.get("_conditional_p3_distance_touch_product_argmax")).size
        or _as_f64_array(params.get("_conditional_p3_touch_log_probability_distance_slope")).size
    )
    p3_reach_gate_enabled = bool(
        params.get("conditional_p3_reach_gate_enabled", False)
    )
    p3_reach_gate_ts = _as_i64_array(
        params.get("_conditional_p3_reach_gate_ts_ms")
    )
    raw_p3_reach_gate_status = params.get("_conditional_p3_reach_gate_status")
    if raw_p3_reach_gate_status is None:
        p3_reach_gate_status = np.empty((0, 0), dtype=np.uint8)
    else:
        p3_reach_gate_status = np.ascontiguousarray(
            raw_p3_reach_gate_status,
            dtype=np.uint8,
        )
        if p3_reach_gate_status.ndim != 2:
            raise ValueError("_conditional_p3_reach_gate_status must be a 2D matrix")
    p3_reach_gate_has_ts = bool(p3_reach_gate_ts.size)
    p3_reach_gate_has_status = bool(p3_reach_gate_status.size)
    if p3_reach_gate_has_ts != p3_reach_gate_has_status:
        raise ValueError(
            "conditional P3 reach gate timestamp and status arrays must be supplied together"
        )
    p3_reach_gate_payload_present = p3_reach_gate_has_ts
    if p3_reach_gate_enabled != p3_reach_gate_payload_present:
        raise ValueError(
            "conditional P3 reach gate requires enabled=true and both frozen arrays, "
            "or enabled=false with no reach-gate payload"
        )
    p3_reach_budget_enabled = bool(
        params.get("conditional_p3_reach_budget_policy_enabled", False)
    )
    p3_reach_budget_ts = _as_i64_array(
        params.get("_conditional_p3_reach_budget_ts_ms")
    )
    raw_p3_reach_budget_selected_k = params.get(
        "_conditional_p3_reach_budget_selected_k"
    )
    if raw_p3_reach_budget_selected_k is None:
        p3_reach_budget_selected_k = np.empty((0, 0), dtype=np.uint8)
    else:
        p3_reach_budget_selected_k = np.ascontiguousarray(
            raw_p3_reach_budget_selected_k,
            dtype=np.uint8,
        )
        if p3_reach_budget_selected_k.ndim != 2:
            raise ValueError(
                "_conditional_p3_reach_budget_selected_k must be a 2D matrix"
            )
    p3_reach_budget_has_ts = bool(p3_reach_budget_ts.size)
    p3_reach_budget_has_selected_k = bool(p3_reach_budget_selected_k.size)
    if p3_reach_budget_has_ts != p3_reach_budget_has_selected_k:
        raise ValueError(
            "conditional P3 reach budget timestamp and selected-k arrays must be "
            "supplied together"
        )
    if p3_reach_budget_enabled != p3_reach_budget_has_ts:
        raise ValueError(
            "conditional P3 reach budget requires enabled=true and both frozen arrays, "
            "or enabled=false with no reach-budget payload"
        )
    if p3_reach_gate_enabled and p3_reach_budget_enabled:
        raise ValueError(
            "fixed conditional-P3 reach gate and adaptive reach budget are mutually "
            "exclusive"
        )
    if conditional_p3_requested and p3_reach_gate_enabled:
        raise ValueError(
            "retired conditional P3 scalar adapter and action-bound reach gate are "
            "mutually exclusive"
        )
    if conditional_p3_requested and p3_reach_budget_enabled:
        raise ValueError(
            "retired conditional P3 scalar adapter and adaptive reach budget are "
            "mutually exclusive"
        )
    fair_center_enabled = bool(
        params.get("cross_venue_fair_center_shift_enabled", False)
    )
    fair_center_payload_present = historical_fair_price_data is not None
    fair_center_ts_ms = np.empty(0, dtype=np.int64)
    fair_center_price = np.empty(0, dtype=np.float64)
    fair_center_gain = np.empty(0, dtype=np.float64)
    fair_center_valid = np.empty(0, dtype=np.uint8)
    if fair_center_payload_present:
        def _fair_field(name):
            if isinstance(historical_fair_price_data, Mapping):
                return historical_fair_price_data[name]
            return getattr(historical_fair_price_data, name)

        fair_center_ts_ns = np.ascontiguousarray(
            _fair_field("feature_ready_ts_ns"), dtype=np.int64
        )
        if fair_center_ts_ns.ndim != 1 or fair_center_ts_ns.size == 0:
            raise ValueError("cross-venue fair-center tape must contain timestamps")
        if np.any(fair_center_ts_ns % 1_000_000 != 0):
            raise ValueError(
                "cross-venue fair-center feature-ready timestamps must align to milliseconds"
            )
        fair_center_ts_ms = np.ascontiguousarray(
            fair_center_ts_ns // 1_000_000, dtype=np.int64
        )
        fair_center_price = np.ascontiguousarray(
            _fair_field("fair_price"), dtype=np.float64
        )
        fair_center_gain = np.ascontiguousarray(
            _fair_field("gain"), dtype=np.float64
        )
        fair_valid_raw = np.ascontiguousarray(_fair_field("valid"), dtype=np.int64)
        if any(
            values.ndim != 1 or values.size != fair_center_ts_ms.size
            for values in (fair_center_price, fair_center_gain, fair_valid_raw)
        ):
            raise ValueError("cross-venue fair-center tape field length mismatch")
        if np.any((fair_valid_raw != 0) & (fair_valid_raw != 1)):
            raise ValueError("cross-venue fair-center valid flag must lie in {0,1}")
        fair_center_valid = np.ascontiguousarray(fair_valid_raw, dtype=np.uint8)
    if fair_center_enabled and not fair_center_payload_present:
        raise ValueError(
            "cross-venue fair-center action enabled without a historical causal tape"
        )
    if fair_center_payload_present and (
        conditional_p3_requested or p3_reach_gate_enabled or p3_reach_budget_enabled
    ):
        raise ValueError(
            "cross-venue fair-center replay cannot be combined with a conditional-P3 "
            "overlay/action in the same experiment identity"
        )
    conditional_p3_reproduction_identity = None
    if conditional_p3_requested:
        conditional_p3_reproduction_identity = (
            _require_conditional_p3_historical_reproduction(params)
        )
    TICK = float(params.get("tick_size", globals()["TICK"]))
    LOT_SIZE = float(params.get("lot_size", globals()["LOT_SIZE"]))
    if not math.isfinite(TICK) or TICK <= 0.0:
        raise ValueError(f"tick_size must be positive, got {TICK!r}")
    if not math.isfinite(LOT_SIZE) or LOT_SIZE <= 0.0:
        raise ValueError(f"lot_size must be positive, got {LOT_SIZE!r}")
    _validate_replayed_live_actions(params)
    if bool(params.get("dynamic_fill_hazard_action_enabled", False)):
        raise NotImplementedError(
            "BUY q90 replay requires the Python native snapshot/delta exchange-book "
            "scheduler; C++ has no equivalent strategy-independent deep-book input"
        )
    if str(
        params.get("rest_gateway_timing_mode", "disabled") or "disabled"
    ).strip().lower() != "disabled":
        raise NotImplementedError(
            "paired serial REST gateway timing is a Python-only diagnostic path"
        )
    if bool(params.get("fixed_spread_probe_enabled", False)):
        raise ValueError(
            "scalar fixed-spread probe was retired after it violated paired "
            "distance monotonicity; use paired_fixed_spread_probe_enabled"
        )
    cpp_visibility_mode = str(
        params.get("exec_book_visibility_mode", "sampled")
    ).lower()
    if cpp_visibility_mode == "paired":
        raise NotImplementedError(
            "paired live execution-book visibility is a Python-only calibration path"
        )
    if cpp_visibility_mode in {"sampled_joint", "profile_source_stratified", "message_schedule"}:
        raise NotImplementedError(
            "joint live execution-book visibility is a Python-only calibration path"
        )
    if bool(params.get("queue_regime_calibration_enabled", False)) and not params.get("_queue_calibration"):
        # C++ supports the compiled per-trade queue arrays used by the daily
        # calibration path.  It still refuses a raw side/regime/day lookup that
        # has not been materialized into params["_queue_calibration"].
        raise NotImplementedError(
            "C++ tick replay requires side/regime queue calibration to be materialized "
            "into params['_queue_calibration'] before replay. Use the canonical daily "
            "loader/runner, or --engine python for raw lookup diagnostics."
        )
    replay_event_clock = str(params.get("replay_event_clock", "trade") or "trade").lower()
    replay_clock_interval_ms = int(params.get("replay_clock_interval_ms", 100) or 100)
    replace_terminal_continuation = bool(
        params.get("replace_terminal_continuation", False)
    )
    if replace_terminal_continuation and (
        replay_event_clock != "merged"
        or replay_clock_interval_ms != 100
        or float(params.get("replay_main_loop_sleep_ms", 0) or 0) != 0.0
    ):
        raise ValueError(
            "replace_terminal_continuation requires merged 100ms replay clock"
        )
    exchange_book_queue_mode = str(
        params.get("exchange_book_queue_mode", "disabled") or "disabled"
    ).strip().lower()
    if (exchange_book_event_tape is not None) != (
        exchange_book_queue_mode != "disabled"
    ):
        raise ValueError(
            "exchange_book_event_tape and a non-disabled "
            "exchange_book_queue_mode are required together"
        )
    if exchange_book_event_tape is not None:
        if exchange_book_queue_mode != "strict":
            raise NotImplementedError(
                "C++ native exchange-book replay currently admits only strict mode"
            )
        if replay_event_clock not in {"merged", "empirical"}:
            raise ValueError(
                "native exchange-book replay requires replay_event_clock=merged "
                "or empirical"
            )
        if bool(params.get("queue_l2_cancel_ahead_enabled", False)):
            raise ValueError(
                "native exchange-book queue updates cannot be combined with "
                "sampled top-N queue_l2_cancel_ahead updates"
            )
    sync_degrade_events = _sync_degrade_events_for_replay(params, trades_df)
    trades_df, n_execution_trades = build_replay_event_clock(
        trades_df,
        mode=replay_event_clock,
        interval_ms=replay_clock_interval_ms,
        start_ts_ms=params.get("replay_event_clock_start_ts_ms"),
        end_ts_ms=params.get("replay_event_clock_end_ts_ms"),
        bbo_data=bbo_data,
        l2_data=l2_data,
        empirical_ts_ms=params.get("_empirical_requote_ts_ms"),
        empirical_action=params.get("_empirical_requote_action"),
        system_event_ts_ms=sync_degrade_events.timestamps_ms,
        system_event_code=sync_degrade_events.event_code,
    )
    require_formal_dense_variance_timeline(
        var_ts_ms,
        var_ssq,
        params,
        trades_df=trades_df,
    )
    if bool(params.get("post_fill_quote_response_enabled", False)):
        raise NotImplementedError(
            "post-fill inventory/flow quote response is Python replay-only until "
            "its discrete-price, causal repair, and order-lifecycle parity are "
            "implemented in C++"
        )
    unsupported = []
    if int(params.get("trace_first_add_decision_to_terminal_max", 0) or 0) > 0:
        unsupported.append("native first-add decision-to-terminal trace")
    if int(params.get("trace_first_opener_decision_to_terminal_max", 0) or 0) > 0:
        unsupported.append("native first-opener decision-to-terminal trace")
    if unsupported:
        raise NotImplementedError(
            "C++ tick replay does not yet support: "
            + ", ".join(unsupported)
            + ". Use --engine python or disable these parameters for C++ parity tests."
        )
    latency_jitter_ms = max(0, int(params.get("latency_jitter_ms", 0.0) or 0.0))
    cpp = _load_cpp_tick_replay()
    cpp_cooldown_runtime = _validate_f05_cpp_cooldown_runtime(
        params,
        require_full_replay=True,
    )

    trade_ts = _as_i64_array(trades_df["transact_time"].to_numpy(dtype=np.int64, copy=False))
    trade_price = _as_f64_array(trades_df["price"].to_numpy(dtype=np.float64, copy=False))
    qty_col = "quantity" if "quantity" in trades_df.columns else "qty"
    trade_qty = _as_f64_array(trades_df[qty_col].to_numpy(dtype=np.float64, copy=False))
    is_buyer_maker = _as_u8_array(
        trades_df["is_buyer_maker"].to_numpy(dtype=np.uint8, copy=False)
    )
    if trade_ts.size == 0:
        raise ValueError("No trades available for C++ tick replay")

    order_size = float(params.get("order_size", 0.001))
    if LOT_SIZE > 0:
        order_size = math.floor(order_size / LOT_SIZE) * LOT_SIZE
        if order_size < LOT_SIZE:
            order_size = LOT_SIZE

    use_bar_pricing = bool(params.get("use_bar_pricing", True))
    historical_l2 = l2_data is not None
    historical_bbo = bbo_data is not None
    use_ml = replay_ml_enabled(
        params,
        ml_data is not None,
        float(params.get("vol_blend", 0.0)) > 0.0
        or float(params.get("skew_strength", 0.0)) > 0.0
        or float(params.get("asym_strength", 0.0)) > 0.0
        or float(params.get("ret_skew", 0.0)) > 0.0
        or float(params.get("inventory_direction_alignment_strength", 0.0)) > 0.0
        or bool(params.get("adverse_guard_enabled", False))
        or bool(params.get("defense_guard_enabled", False))
        or bool(params.get("buy_fill_selection_live_enabled", False)),
    )

    quote_core_params = dict(params)
    quote_core_params["order_size"] = order_size
    quote_core_cfg = quote_core_config_from_params(
        quote_core_params,
        tick_size=TICK,
        lot_size=LOT_SIZE,
        use_ml=use_ml,
        use_depth_weighted_mid_proxy=not use_bar_pricing and historical_l2,
        use_depth_liquidity_scaling=not use_bar_pricing and historical_l2,
    )

    cpp_params = cpp.TickReplayParams()
    cpp_params.quote = _copy_quote_config_to_cpp(quote_core_cfg, cpp.QuoteCoreConfig())
    cpp_params.order_size = order_size
    if exchange_book_event_tape is not None:
        if not hasattr(cpp_params, "set_native_exchange_book_arrays"):
            raise RuntimeError(
                "strict native C++ replay requires the current tape ABI; "
                "rebuild narrowgate_cpp from this source tree"
            )
        native_arrays = _materialize_cpp_native_exchange_book_tape(
            exchange_book_event_tape
        )
        cpp_params.native_exchange_book_strict_after_ns = int(
            native_arrays["strict_after_ns"]
        )
        cpp_params.set_native_exchange_book_arrays(
            native_arrays["event_ts_ns"],
            native_arrays["event_type"],
            native_arrays["receive_ts_ns"],
            native_arrays["event_time_ms"],
            native_arrays["transaction_time_ms"],
            native_arrays["first_update_id"],
            native_arrays["final_update_id"],
            native_arrays["previous_final_update_id"],
            native_arrays["last_update_id"],
            native_arrays["level_offsets"],
            native_arrays["level_is_bid"],
            native_arrays["level_price_tick"],
            native_arrays["level_quantity"],
        )
        # The binding owns compact C++ vectors after this call. Drop the
        # temporary Python buffers before entering a full-day replay instead
        # of retaining a second complete copy of the native tape.
        del native_arrays
    if cpp_cooldown_runtime is not None:
        shared_window_tape = params.get(
            "_cooldown_duration_policy_cpp_window_tape_handle"
        )
        raw_window_arrays = params.get(
            "_cooldown_duration_policy_cpp_window_arrays"
        )
        if shared_window_tape is not None and raw_window_arrays is not None:
            raise RuntimeError(
                "full-replay C++ cooldown cannot bind both shared and raw window tapes"
            )
        if shared_window_tape is not None and not isinstance(
            shared_window_tape, cpp.F05CooldownWindowTape
        ):
            raise RuntimeError(
                "full-replay C++ cooldown shared window tape type drifted"
            )
        if raw_window_arrays is not None and not isinstance(
            raw_window_arrays, Mapping
        ):
            raise RuntimeError(
                "full-replay C++ cooldown window arrays must be a mapping"
            )
        window_tape = (
            []
            if raw_window_arrays is not None or shared_window_tape is not None
            else list(
                params.get("_cooldown_duration_policy_cpp_window_tape", ()) or ()
            )
        )
        predicate_rows = list(
            params.get("_cooldown_duration_policy_cpp_predicate_rows", ()) or ()
        )
        if raw_window_arrays is None and shared_window_tape is None and (
            not window_tape
            or any(
                not isinstance(row, cpp.F05CooldownWindowObservation)
                for row in window_tape
            )
        ):
            raise RuntimeError(
                "full-replay C++ cooldown requires a non-empty bound causal window tape"
            )
        if any(
            not isinstance(row, cpp.F05CooldownPredicateRow)
            for row in predicate_rows
        ):
            raise RuntimeError(
                "full-replay C++ cooldown predicate tape contains an invalid row"
            )
        cpp_params.f05_repeated_cooldown_runtime = cpp_cooldown_runtime
        if shared_window_tape is not None:
            if int(shared_window_tape.size) <= 0:
                raise RuntimeError(
                    "full-replay C++ cooldown shared window tape is empty"
                )
            cpp_params.f05_cooldown_window_tape_shared = shared_window_tape
        elif raw_window_arrays is None:
            cpp_params.f05_cooldown_window_tape = window_tape
        else:
            required_window_arrays = (
                "left_ts_ns",
                "right_ts_ns",
                "feature_ready_ts_ns",
                "market_generation",
                "depth_generation",
                "mid_usdc_per_btc",
                "reset_feature_state",
                "source_gap",
                "source_stale",
                "warmup_admitted",
                "channel_support_valid",
            )
            if set(raw_window_arrays) != set(required_window_arrays):
                raise RuntimeError(
                    "full-replay C++ cooldown window array schema drifted"
                )
            count = cpp_params.set_f05_cooldown_window_arrays(
                *(
                    np.ascontiguousarray(
                        raw_window_arrays[name],
                        dtype=(
                            np.float64
                            if name == "mid_usdc_per_btc"
                            else np.uint8
                            if name
                            in {
                                "reset_feature_state",
                                "source_gap",
                                "source_stale",
                                "warmup_admitted",
                                "channel_support_valid",
                            }
                            else np.int64
                        ),
                    )
                    for name in required_window_arrays
                )
            )
            if int(count) <= 0:
                raise RuntimeError(
                    "full-replay C++ cooldown window array tape is empty"
                )
        cpp_params.f05_cooldown_predicate_rows = predicate_rows
    cpp_params.max_inventory = float(params.get("max_inventory", 0.01))
    cpp_params.eta = float(params.get("eta", 0.0))
    cpp_params.symmetric_size = bool(params.get("symmetric_size", False))
    cpp_params.requote_interval_s = float(params.get("requote_interval", 1.0))
    cpp_params.rq_min_s = float(params.get("rq_min", params.get("requote_interval", 1.0)))
    cpp_params.rq_max_s = float(params.get("rq_max", params.get("requote_interval", 1.0)))
    cpp_params.requote_clock_fixed = str(params.get("requote_clock", "fixed") or "fixed").lower() == "fixed"
    cpp_params.empirical_requote_clock = replay_event_clock == "empirical"
    fixed_spread_probe_enabled = bool(
        params.get("fixed_spread_probe_enabled", False)
    )
    fixed_spread_probe_ticks = float(
        params.get("fixed_spread_probe_ticks", 0.0) or 0.0
    )
    paired_fixed_spread_probe_enabled = bool(
        params.get("paired_fixed_spread_probe_enabled", False)
    )
    paired_fixed_spread_probe_ticks = [
        float(value)
        for value in (params.get("paired_fixed_spread_probe_ticks", []) or [])
    ]
    if fixed_spread_probe_enabled and not hasattr(
        cpp_params, "fixed_spread_probe_enabled"
    ):
        raise RuntimeError(
            "fixed-spread probe requested, but the imported narrowgate_cpp "
            "predates that ABI; rebuild the C++ extension"
        )
    if hasattr(cpp_params, "fixed_spread_probe_enabled"):
        cpp_params.fixed_spread_probe_enabled = fixed_spread_probe_enabled
        cpp_params.fixed_spread_probe_ticks = fixed_spread_probe_ticks
    if paired_fixed_spread_probe_enabled and not hasattr(
        cpp_params, "paired_fixed_spread_probe_enabled"
    ):
        raise RuntimeError(
            "paired fixed-spread probe requested, but the imported "
            "narrowgate_cpp predates that ABI; rebuild the C++ extension"
        )
    if hasattr(cpp_params, "paired_fixed_spread_probe_enabled"):
        cpp_params.paired_fixed_spread_probe_enabled = (
            paired_fixed_spread_probe_enabled
        )
        cpp_params.paired_fixed_spread_probe_ticks = (
            paired_fixed_spread_probe_ticks
        )
        cpp_params.paired_fixed_spread_fail_on_violation = bool(
            params.get("paired_fixed_spread_fail_on_violation", True)
        )
        cpp_params.paired_fixed_spread_max_recorded_violations = max(
            0,
            int(
                params.get(
                    "paired_fixed_spread_max_recorded_violations",
                    100,
                )
                or 0
            ),
        )
    cpp_params.maker_fee = float(params.get("maker_fee", 0.0))
    cpp_params.taker_fee = float(params.get("taker_fee", 0.00036))
    cpp_params.queue_base = float(params.get("queue_base", 5.0))
    cpp_params.queue_decay = float(params.get("queue_decay", 0.1))
    queue_mode = _normalise_queue_ahead_mode(params.get("queue_ahead_mode", "exact_level"))
    cpp_params.queue_price_tolerance = max(
        0.0,
        float(params.get("queue_price_tolerance", 0.051) or 0.0),
    )
    cpp_params.maker_fill_prob = max(0.0, min(1.0, float(params.get("maker_fill_prob", 1.0))))
    cpp_params.buy_fill_prob = max(0.0, min(1.0, float(params.get("buy_fill_prob", 1.0))))
    cpp_params.sell_fill_prob = max(0.0, min(1.0, float(params.get("sell_fill_prob", 1.0))))
    cpp_params.queue_ahead_base_mult = max(0.0, float(params.get("queue_ahead_base_mult", 1.0) or 1.0))
    cpp_params.queue_deplete_base_mult = max(0.0, float(params.get("queue_deplete_base_mult", 1.0) or 1.0))
    cpp_params.queue_l2_cancel_ahead_enabled = bool(
        params.get("queue_l2_cancel_ahead_enabled", False)
    )
    if cpp_params.queue_l2_cancel_ahead_enabled:
        if queue_mode != "exact_level":
            raise ValueError(
                "queue_l2_cancel_ahead_enabled requires queue_ahead_mode=exact_level"
            )
        if replay_event_clock not in {"merged", "empirical"}:
            raise ValueError(
                "queue_l2_cancel_ahead_enabled requires replay_event_clock=merged or empirical"
            )
        if l2_data is None:
            raise ValueError(
                "queue_l2_cancel_ahead_enabled requires strict historical L2 data"
            )
    cpp_params.queue_ahead_buy_exposure_mult = max(
        0.0,
        float(params.get("queue_ahead_buy_exposure_mult", 1.0) or 1.0),
    )
    cpp_params.queue_ahead_buy_reducing_mult = max(
        0.0,
        float(params.get("queue_ahead_buy_reducing_mult", 1.0) or 1.0),
    )
    cpp_params.queue_ahead_sell_exposure_mult = max(
        0.0,
        float(params.get("queue_ahead_sell_exposure_mult", 1.0) or 1.0),
    )
    cpp_params.queue_ahead_sell_reducing_mult = max(
        0.0,
        float(params.get("queue_ahead_sell_reducing_mult", 1.0) or 1.0),
    )
    cpp_params.requote_threshold_bps = max(0.0, float(params.get("requote_threshold_bps", 0.0) or 0.0))
    cpp_params.replace_min_price_change_ticks = max(
        0.0,
        float(params.get("replace_min_price_change_ticks", 0.0) or 0.0),
    )
    cpp_params.replace_min_price_change_ticks_reducing = max(
        0.0,
        float(
            params.get(
                "replace_min_price_change_ticks_reducing",
                cpp_params.replace_min_price_change_ticks,
            )
            or 0.0
        ),
    )
    cpp_params.replace_min_interval_ms = max(0.0, float(params.get("replace_min_interval_ms", 0.0) or 0.0))
    cpp_params.replace_min_interval_ms_reducing = max(
        0.0,
        float(params.get("replace_min_interval_ms_reducing", cpp_params.replace_min_interval_ms) or 0.0),
    )
    cpp_params.replace_pending_coalesce = bool(params.get("replace_pending_coalesce", False))
    if replace_terminal_continuation and not hasattr(
        cpp_params,
        "replace_terminal_continuation",
    ):
        raise RuntimeError(
            "narrowgate_cpp is stale: rebuild for replace_terminal_continuation"
        )
    if hasattr(cpp_params, "replace_terminal_continuation"):
        cpp_params.replace_terminal_continuation = replace_terminal_continuation
    cpp_params.replace_cancel_first_exposure_increasing = bool(
        params.get("replace_cancel_first_exposure_increasing", False)
    )
    cpp_params.buy_fill_selection_live_enabled = bool(params.get("buy_fill_selection_live_enabled", False))
    cpp_params.buy_fill_selection_live_score_threshold = float(
        params.get("buy_fill_selection_live_score_threshold", 0.50) or 0.50
    )
    cpp_params.buy_fill_selection_live_spread_mult_cap = max(
        1.0,
        float(params.get("buy_fill_selection_live_spread_mult_cap", 1.0) or 1.0),
    )
    cpp_params.buy_fill_selection_live_apply_reducing = bool(
        params.get("buy_fill_selection_live_apply_reducing", False)
    )
    cpp_params.buy_fill_selection_live_max_missing_features = int(
        params.get("buy_fill_selection_live_max_missing_features", 99) or 99
    )
    cpp_params.buy_soft_widen_release_probe_enabled = bool(
        params.get("buy_soft_widen_release_probe_enabled", False)
    )
    cpp_params.buy_soft_widen_release_probe_apply_candidate = bool(
        params.get("buy_soft_widen_release_probe_apply_candidate", True)
    )
    cpp_params.buy_soft_widen_release_target_ts_ms = int(
        params.get("buy_soft_widen_release_target_ts_ms", 0) or 0
    )
    cpp_params.buy_soft_widen_release_target_role = str(
        params.get("buy_soft_widen_release_target_role", "") or ""
    ).strip().lower()
    cpp_params.buy_soft_widen_release_spread_mult_cap = float(
        params.get("buy_soft_widen_release_spread_mult_cap", 1.0) or 1.0
    )
    cpp_params.conditional_p3_reach_gate_enabled = p3_reach_gate_enabled
    cpp_params.conditional_p3_reach_gate_outward_ticks = int(
        params.get("conditional_p3_reach_gate_outward_ticks", 16) or 16
    )
    cpp_params.conditional_p3_reach_gate_grid_min_ticks = int(
        params.get("conditional_p3_reach_gate_grid_min_ticks", 5) or 5
    )
    cpp_params.conditional_p3_reach_gate_buy_toxicity_threshold = float(
        params.get("conditional_p3_reach_gate_buy_toxicity_threshold", 2.0)
    )
    cpp_params.conditional_p3_reach_gate_sell_toxicity_threshold = float(
        params.get("conditional_p3_reach_gate_sell_toxicity_threshold", 2.0)
    )
    if p3_reach_budget_enabled and not hasattr(
        cpp_params, "conditional_p3_reach_budget_policy_enabled"
    ):
        raise RuntimeError(
            "adaptive conditional-P3 reach-budget replay requires the v6 C++ ABI; "
            "rebuild narrowgate_cpp from the current source tree"
        )
    if hasattr(cpp_params, "conditional_p3_reach_budget_policy_enabled"):
        cpp_params.conditional_p3_reach_budget_policy_enabled = (
            p3_reach_budget_enabled
        )
        cpp_params.conditional_p3_reach_budget_grid_min_ticks = int(
            params.get("conditional_p3_reach_budget_grid_min_ticks", 5) or 5
        )
        cpp_params.conditional_p3_reach_budget_buy_toxicity_threshold = float(
            params.get(
                "conditional_p3_reach_budget_buy_toxicity_threshold",
                1.0,
            )
        )
        cpp_params.conditional_p3_reach_budget_sell_toxicity_threshold = float(
            params.get(
                "conditional_p3_reach_budget_sell_toxicity_threshold",
                1.0,
            )
        )
    if fair_center_payload_present and not hasattr(
        cpp_params, "cross_venue_fair_center_shift_enabled"
    ):
        raise RuntimeError(
            "cross-venue fair-center replay requires the v7 C++ ABI; rebuild "
            "narrowgate_cpp from the current source tree"
        )
    if hasattr(cpp_params, "cross_venue_fair_center_shift_enabled"):
        cpp_params.cross_venue_fair_center_shift_enabled = fair_center_enabled
        cpp_params.cross_venue_fair_center_max_state_age_ms = float(
            params.get("cross_venue_fair_center_max_state_age_ms", 2_000.0)
        )
    buy_fill_selection_model_path: Optional[Path] = None
    if cpp_params.buy_fill_selection_live_enabled:
        raw_model_path = str(params.get("buy_fill_selection_live_model_path", "") or "").strip()
        if not raw_model_path:
            raise FileNotFoundError("buy_fill_selection_live_enabled=true but buy_fill_selection_live_model_path is empty")
        model_path = Path(raw_model_path).expanduser()
        if not model_path.is_absolute():
            model_path = ROOT / model_path
        buy_fill_selection_model_path = model_path
        cpp_params.buy_fill_selection_models = _load_cpp_fill_selection_models(cpp, model_path)
    cpp_params.new_order_latency_ms = max(0, int(params.get("new_order_latency_ms", 0.0) or 0.0))
    cpp_params.cancel_order_latency_ms = max(0, int(params.get("cancel_order_latency_ms", 0.0) or 0.0))
    cpp_params.latency_jitter_ms = latency_jitter_ms
    if decision_to_gateway_latency_enabled and not hasattr(
        cpp_params, "decision_to_gateway_latency_samples_ms"
    ):
        raise RuntimeError(
            "decision-to-gateway latency replay requires the current C++ ABI; "
            "rebuild narrowgate_cpp"
        )
    if hasattr(cpp_params, "decision_to_gateway_latency_samples_ms"):
        cpp_params.decision_to_gateway_latency_seed = (
            effective_decision_to_gateway_latency_seed(params)
        )
        cpp_params.decision_to_gateway_latency_samples_ms = [
            float(x)
            for x in decision_to_gateway_latency_samples.ravel()
            if np.isfinite(x) and x >= 0.0
        ]
    cpp_params.new_order_latency_samples_ms = [
        float(x)
        for x in np.asarray(params.get("_new_order_latency_samples_ms", []), dtype=np.float64).ravel()
        if np.isfinite(x) and x >= 0.0
    ]
    cpp_params.cancel_order_latency_samples_ms = [
        float(x)
        for x in np.asarray(params.get("_cancel_order_latency_samples_ms", []), dtype=np.float64).ravel()
        if np.isfinite(x) and x >= 0.0
    ]
    split_lifecycle_latency_requested = any(
        np.asarray(params.get(key, []), dtype=np.float64).size > 0
        for key in (
            "_new_order_exchange_effective_latency_samples_ms",
            "_cancel_exchange_effective_latency_samples_ms",
            "_cancel_ack_visibility_latency_samples_ms",
        )
    )
    if split_lifecycle_latency_requested and not hasattr(
        cpp_params, "cancel_ack_visibility_latency_samples_ms"
    ):
        raise RuntimeError(
            "split exchange-effective/local-visible latency replay requires "
            "the current C++ ABI; rebuild narrowgate_cpp"
        )
    if hasattr(cpp_params, "new_order_exchange_effective_latency_samples_ms"):
        cpp_params.new_order_exchange_effective_latency_samples_ms = [
            float(x)
            for x in np.asarray(
                params.get("_new_order_exchange_effective_latency_samples_ms", []),
                dtype=np.float64,
            ).ravel()
            if np.isfinite(x) and x >= 0.0
        ]
        cpp_params.cancel_exchange_effective_latency_samples_ms = [
            float(x)
            for x in np.asarray(
                params.get("_cancel_exchange_effective_latency_samples_ms", []),
                dtype=np.float64,
            ).ravel()
            if np.isfinite(x) and x >= 0.0
        ]
        cpp_params.cancel_ack_visibility_latency_samples_ms = [
            float(x)
            for x in np.asarray(
                params.get("_cancel_ack_visibility_latency_samples_ms", []),
                dtype=np.float64,
            ).ravel()
            if np.isfinite(x) and x >= 0.0
        ]
    cpp_params.exec_book_visibility_delay_samples_ms = [
        float(x)
        for x in np.asarray(
            params.get("_exec_book_visibility_delay_samples_ms", []),
            dtype=np.float64,
        ).ravel()
        if np.isfinite(x) and x >= 0.0
    ]
    cpp_params.exec_book_visibility_delay_mean_ms = max(
        0.0,
        float(params.get("exec_book_visibility_delay_mean_ms", 0.0) or 0.0),
    )
    cpp_params.exec_book_visibility_delay_jitter_ms = max(
        0.0,
        float(params.get("exec_book_visibility_delay_jitter_ms", 0.0) or 0.0),
    )
    cpp_params.exec_book_visibility_delay_seed = int(
        params.get("exec_book_visibility_delay_seed", 20260718)
    )
    cpp_params.max_exec_book_age_ms = max(
        0,
        int(float(params.get("max_exec_book_age_s", 5.0)) * 1000.0),
    )
    cpp_params.ber_guard_thresh = float(params.get("ber_guard_thresh", 0.0))
    if not hasattr(cpp_params, "ber_exposure_add_only"):
        raise RuntimeError(
            "live-held BER replay requires the current C++ ABI; rebuild "
            "narrowgate_cpp from this source tree"
        )
    cpp_params.ber_exposure_add_only = ber_exposure_add_only
    cpp_params.fill_cooldown_s = float(params.get("fill_cooldown", 0.0) or 0.0)
    cpp_params.fill_cooldown_apply_reducing = bool(params.get("fill_cooldown_apply_reducing", False))
    (
        fill_cooldown_consecutive_reset_policy,
        fill_cooldown_reset_consec_on_expiry,
    ) = _resolve_fill_cooldown_reset_policy(params)
    if not hasattr(cpp_params, "fill_cooldown_consecutive_reset_policy"):
        raise RuntimeError(
            "narrowgate_cpp lacks the explicit fill cooldown reset policy ABI; rebuild it"
        )
    cpp_params.fill_cooldown_consecutive_reset_policy = (
        fill_cooldown_consecutive_reset_policy
    )
    cpp_params.fill_cooldown_reset_consec_on_expiry = (
        fill_cooldown_reset_consec_on_expiry
    )
    cpp_params.fill_cooldown_reducing_s = max(
        0.0,
        float(params.get("fill_cooldown_reducing", 0.0) or 0.0),
    )
    cpp_params.fill_cooldown_reducing_inventory_lifecycle_only = bool(
        params.get("fill_cooldown_reducing_inventory_lifecycle_only", False)
    )
    cpp_params.fill_cooldown_reducing_inv_threshold = max(
        0.0,
        float(params.get("fill_cooldown_reducing_inv_threshold", 0.0) or 0.0),
    )
    cpp_params.fill_cooldown_reducing_inv_ratio = max(
        0.0,
        float(params.get("fill_cooldown_reducing_inv_ratio", 0.0) or 0.0),
    )
    cpp_params.fill_cooldown_reducing_age_s = max(
        0.0,
        float(params.get("fill_cooldown_reducing_age_s", 0.0) or 0.0),
    )
    cpp_params.fill_cooldown_reducing_vol_ref = max(
        0.0,
        float(params.get("fill_cooldown_reducing_vol_ref", 0.0) or 0.0),
    )
    cpp_params.fill_cooldown_reducing_vol_min_mult = max(
        0.0,
        float(params.get("fill_cooldown_reducing_vol_min_mult", 0.5) or 0.5),
    )
    cpp_params.fill_cooldown_reducing_vol_max_mult = max(
        cpp_params.fill_cooldown_reducing_vol_min_mult,
        float(params.get("fill_cooldown_reducing_vol_max_mult", 2.0) or 2.0),
    )
    if not hasattr(cpp_params, "consecutive_loss_cooldown_semantics"):
        raise RuntimeError(
            "narrowgate_cpp lacks path-dependent replay controls; rebuild it"
        )
    (
        cpp_params.max_consecutive_losses,
        cpp_params.cooldown_after_loss_s,
        _,
    ) = loss_cooldown_config_values(
        params
    )
    cpp_params.consecutive_loss_cooldown_semantics = LOSS_COOLDOWN_SEMANTICS
    cpp_params.sync_adjust_degrade_enabled = bool(
        params.get("sync_adjust_degrade_enabled", False)
    )
    cpp_params.sync_adjust_pause_s = max(
        0.0,
        float(params.get("sync_adjust_pause_s", 0.0) or 0.0),
    )
    cpp_params.sync_adjust_cancel_orders = bool(
        params.get("sync_adjust_cancel_orders", True)
    )
    cpp_params.sync_adjust_replay_mode = str(sync_degrade_events.mode)
    cpp_params.sync_adjust_semantics = SYNC_DEGRADE_SEMANTICS
    cpp_params.thin_depth_threshold = max(0.0, float(params.get("thin_depth_threshold", 0.0) or 0.0))
    cpp_params.l2_refill_cancel_near_levels = max(
        1,
        int(params.get("l2_refill_cancel_near_levels", 3) or 3),
    )
    cpp_params.l2_policy_depth_levels = max(
        1,
        int(params.get("l2_policy_depth_levels", 10) or 10),
    )
    cpp_params.l2_refill_cancel_lookback_s = max(
        0.0,
        float(params.get("l2_refill_cancel_lookback_s", 10.0) or 0.0),
    )
    markout_span = params.get("markout_ema_span_fills", 0)
    cpp_params.markout_ema_span_fills = max(0, int(markout_span or 0))
    cpp_params.markout_horizon_s = max(1e-6, float(params.get("markout_horizon_s", 10.0)))
    cpp_params.adverse_markout_pause_hybrid = bool(params.get("adverse_markout_pause_hybrid", False))
    cpp_params.adverse_markout_pause_base_s = max(0.0, float(params.get("adverse_markout_pause_base_s", 120.0)))
    cpp_params.adverse_markout_pause_min_s = max(0.0, float(params.get("adverse_markout_pause_min_s", 120.0)))
    cpp_params.adverse_markout_pause_max_s = max(
        cpp_params.adverse_markout_pause_min_s,
        float(params.get("adverse_markout_pause_max_s", 900.0)),
    )
    cpp_params.adverse_markout_decay_tau_s = max(0.0, float(params.get("adverse_markout_decay_tau_s", 0.0)))
    cpp_params.adverse_markout_max_resolve_gap_s = max(
        cpp_params.markout_horizon_s,
        float(params.get("adverse_markout_max_resolve_gap_s", 30.0)),
    )
    cpp_params.use_bar_pricing = use_bar_pricing
    cpp_params.ret_demean_halflife = max(0, int(params.get("ret_demean_halflife", 0) or 0))
    cpp_params.initial_inventory = float(params.get("initial_inventory", 0.0) or 0.0)
    cpp_params.initial_entry_price = float(params.get("initial_entry_price", 0.0) or 0.0)
    raw_initial_live_state = params.get("initial_live_state") or {}
    risk_limits = replay_hard_risk_limits(params)
    if not hasattr(cpp_params, "max_daily_loss"):
        raise RuntimeError("narrowgate_cpp lacks hard-risk ABI; rebuild it")
    cpp_params.max_daily_loss, cpp_params.max_position_value, cpp_params.emergency_close_dd = risk_limits
    initial_risk_cash = (
        -cpp_params.initial_inventory * cpp_params.initial_entry_price
        if cpp_params.initial_entry_price > 0.0 else 0.0
    )
    initial_risk_equity = initial_risk_cash + cpp_params.initial_inventory * float(trade_price[0])
    native_risk_state = ReplayHardRiskState(
        utc_day=int(trade_ts[0]) // 86_400_000,
        session_peak_pnl=max(0.0, initial_risk_equity),
        last_total_pnl=initial_risk_equity,
    )
    supplied_risk_state = raw_initial_live_state.get("risk_state")
    if supplied_risk_state is not None:
        native_risk_state = ReplayHardRiskState.restore(
            supplied_risk_state, start_ts_ms=int(trade_ts[0]),
        )
    cpp_params.initial_risk_state_enabled = True
    for field, value in asdict(native_risk_state).items():
        setattr(cpp_params, "initial_risk_" + field, value)
    raw_loss_snapshot = None
    if isinstance(raw_initial_live_state, Mapping):
        raw_loss_snapshot = raw_initial_live_state.get(
            "reward_path_loss_cooldown"
        ) or raw_initial_live_state.get("loss_cooldown_snapshot")
    if isinstance(raw_loss_snapshot, Mapping) and raw_loss_snapshot:
        if not hasattr(cpp_params, "consecutive_loss_snapshot_enabled"):
            raise RuntimeError(
                "narrowgate_cpp lacks loss-cooldown snapshot ABI; rebuild it"
            )
        restored_loss = ConsecutiveLossCooldown.restore(raw_loss_snapshot)
        if (
            restored_loss.max_consecutive_losses
            != cpp_params.max_consecutive_losses
            or restored_loss.cooldown_ms
            != int(round(cpp_params.cooldown_after_loss_s * 1_000.0))
            or not math.isclose(
                restored_loss.inventory,
                cpp_params.initial_inventory,
                rel_tol=0.0,
                abs_tol=1e-10,
            )
            or not math.isclose(
                restored_loss.avg_entry,
                cpp_params.initial_entry_price,
                rel_tol=0.0,
                abs_tol=1e-10,
            )
        ):
            raise ValueError(
                "restored C++ loss-cooldown state disagrees with replay params"
            )
        cpp_params.consecutive_loss_snapshot_enabled = True
        cpp_params.consecutive_loss_snapshot_schema = (
            LOSS_COOLDOWN_SNAPSHOT_SCHEMA
        )
        cpp_params.initial_loss_open_commission = restored_loss.open_commission
        cpp_params.initial_loss_round_trip_pnl = restored_loss.round_trip_pnl
        cpp_params.initial_loss_consecutive_losses = (
            restored_loss.consecutive_losses
        )
        cpp_params.initial_loss_cooldown_until_ms = (
            restored_loss.cooldown_until_ms
        )
        cpp_params.initial_loss_last_cancel_ts_ms = restored_loss.last_cancel_ts_ms
        cpp_params.initial_loss_threshold_pending = restored_loss.threshold_pending
        cpp_params.initial_loss_trigger_count = restored_loss.trigger_count
        cpp_params.initial_loss_expiry_count = restored_loss.expiry_count
        cpp_params.initial_loss_losing_round_trips = (
            restored_loss.losing_round_trips
        )
        cpp_params.initial_loss_winning_or_flat_round_trips = (
            restored_loss.winning_or_flat_round_trips
        )
        cpp_params.initial_loss_max_observed_consecutive_losses = (
            restored_loss.max_observed_consecutive_losses
        )
    if not hasattr(cpp_params, "planned_quote_stop_ts_ms"):
        if int(params.get("planned_quote_stop_ts_ms", 0) or 0) > 0:
            raise RuntimeError(
                "narrowgate_cpp lacks the planned-maintenance replay ABI; rebuild it"
            )
    else:
        cpp_params.planned_quote_stop_ts_ms = max(
            0,
            int(params.get("planned_quote_stop_ts_ms", 0) or 0),
        )
    cpp_params.local_extreme_guard_enabled = bool(params.get("local_extreme_guard_enabled", False))
    cpp_params.local_extreme_window_s = max(5.0, float(params.get("local_extreme_window_s", 120.0)))
    cpp_params.local_extreme_rank_threshold = min(
        0.999,
        max(0.501, float(params.get("local_extreme_rank_threshold", 0.80))),
    )
    cpp_params.local_extreme_require_thin_depth = bool(params.get("local_extreme_require_thin_depth", True))
    cpp_params.local_extreme_thin_depth_threshold = max(
        0.0,
        float(params.get("local_extreme_thin_depth_threshold", 0.0)),
    )
    cpp_params.local_extreme_spread_mult = max(1.0, float(params.get("local_extreme_spread_mult", 1.0)))
    cpp_params.local_extreme_pause = bool(params.get("local_extreme_pause", False))
    cpp_params.fragile_order_ttl_s = max(0.0, float(params.get("fragile_order_ttl_s", 0.0)))
    cpp_params.adaptive_add_cooldown_enabled = bool(params.get("adaptive_add_cooldown_enabled", False))
    cpp_params.adaptive_add_cooldown_min_mult = float(params.get("adaptive_add_cooldown_min_mult", 0.5))
    cpp_params.adaptive_add_cooldown_max_mult = float(params.get("adaptive_add_cooldown_max_mult", 2.5))
    cpp_params.adaptive_add_cooldown_w_markout = float(params.get("adaptive_add_cooldown_w_markout", 0.0))
    cpp_params.adaptive_add_cooldown_w_flow = float(params.get("adaptive_add_cooldown_w_flow", 0.0))
    cpp_params.adaptive_add_cooldown_w_inventory_lifecycle = float(params.get("adaptive_add_cooldown_w_inventory_lifecycle", 0.0))
    cpp_params.adaptive_add_cooldown_w_trend = float(params.get("adaptive_add_cooldown_w_trend", 0.0))
    cpp_params.adaptive_add_cooldown_w_refill_weak = float(
        params.get("adaptive_add_cooldown_w_refill_weak", 0.0)
    )
    cpp_params.adaptive_add_cooldown_w_refill_good = float(
        params.get("adaptive_add_cooldown_w_refill_good", 0.0)
    )
    cpp_params.adaptive_add_cooldown_w_reversion = float(params.get("adaptive_add_cooldown_w_reversion", 0.0))
    cpp_params.adaptive_add_cooldown_mo_ref = float(params.get("adaptive_add_cooldown_mo_ref", 50.0))
    cpp_params.adaptive_add_cooldown_flow_ref = float(params.get("adaptive_add_cooldown_flow_ref", 2.0))
    cpp_params.adaptive_add_cooldown_inventory_lifecycle_inv_ref = float(
        params.get("adaptive_add_cooldown_inventory_lifecycle_inv_ref", 0.006)
    )
    cpp_params.adaptive_add_cooldown_inventory_lifecycle_age_ref_s = float(
        params.get("adaptive_add_cooldown_inventory_lifecycle_age_ref_s", 3600.0)
    )
    cpp_params.adaptive_add_cooldown_trend_ret_ref = float(
        params.get("adaptive_add_cooldown_trend_ret_ref", 0.00002)
    )
    cpp_params.adaptive_add_cooldown_refill_ref = float(params.get("adaptive_add_cooldown_refill_ref", 0.10))
    cpp_params.adaptive_add_cooldown_reversion_ref = float(
        params.get("adaptive_add_cooldown_reversion_ref", 1.0)
    )
    cpp_params.adaptive_add_cooldown_gate_enabled = bool(
        params.get("adaptive_add_cooldown_gate_enabled", False)
    )
    cpp_params.adaptive_add_cooldown_gate_mult = float(params.get("adaptive_add_cooldown_gate_mult", 1.75))
    cpp_params.adaptive_add_cooldown_gate_inventory_lifecycle_score = float(
        params.get("adaptive_add_cooldown_gate_inventory_lifecycle_score", 1.0)
    )
    cpp_params.adaptive_add_cooldown_gate_trend_score = float(
        params.get("adaptive_add_cooldown_gate_trend_score", 1.0)
    )
    cpp_params.adaptive_add_cooldown_gate_refill_edge_max = float(
        params.get("adaptive_add_cooldown_gate_refill_edge_max", 0.0)
    )
    cpp_params.adaptive_add_cooldown_gate_reversion_max = float(
        params.get("adaptive_add_cooldown_gate_reversion_max", 0.5)
    )
    cpp_params.adaptive_add_cooldown_gate_side = str(
        params.get("adaptive_add_cooldown_gate_side", "BOTH") or "BOTH"
    )
    cpp_params.flat_unilateral_max_s = max(0.0, float(params.get("flat_unilateral_max_s", 0.0) or 0.0))
    cpp_params.inventory_lifecycle_stop_add_enabled = bool(params.get("inventory_lifecycle_stop_add_enabled", False))
    cpp_params.inventory_lifecycle_stop_add_inv_threshold = max(
        0.0,
        float(params.get("inventory_lifecycle_stop_add_inv_threshold", 0.0) or 0.0),
    )
    cpp_params.inventory_lifecycle_stop_add_age_s = max(
        0.0,
        float(params.get("inventory_lifecycle_stop_add_age_s", 0.0) or 0.0),
    )
    cpp_params.inventory_lifecycle_soft_control_enabled = bool(params.get("inventory_lifecycle_soft_control_enabled", False))
    cpp_params.inventory_lifecycle_soft_inv_threshold = max(
        0.0,
        float(params.get("inventory_lifecycle_soft_inv_threshold", 0.0) or 0.0),
    )
    cpp_params.inventory_lifecycle_soft_age_s = max(
        0.0,
        float(params.get("inventory_lifecycle_soft_age_s", 0.0) or 0.0),
    )
    cpp_params.inventory_lifecycle_soft_spread_mult = max(
        1.0,
        float(params.get("inventory_lifecycle_soft_spread_mult", 1.0) or 1.0),
    )
    cpp_params.inventory_lifecycle_soft_gate_enabled = bool(params.get("inventory_lifecycle_soft_gate_enabled", False))
    cpp_params.inventory_lifecycle_soft_gate_inventory_lifecycle_inv_ref = float(
        params.get("inventory_lifecycle_soft_gate_inventory_lifecycle_inv_ref", 0.006)
    )
    cpp_params.inventory_lifecycle_soft_gate_inventory_lifecycle_age_ref_s = float(
        params.get("inventory_lifecycle_soft_gate_inventory_lifecycle_age_ref_s", 3600.0)
    )
    cpp_params.inventory_lifecycle_soft_gate_trend_ret_ref = float(
        params.get("inventory_lifecycle_soft_gate_trend_ret_ref", 0.00002)
    )
    cpp_params.inventory_lifecycle_soft_gate_refill_ref = float(params.get("inventory_lifecycle_soft_gate_refill_ref", 0.10))
    cpp_params.inventory_lifecycle_soft_gate_inventory_lifecycle_score = float(
        params.get("inventory_lifecycle_soft_gate_inventory_lifecycle_score", 1.0)
    )
    cpp_params.inventory_lifecycle_soft_gate_trend_score = float(params.get("inventory_lifecycle_soft_gate_trend_score", 1.0))
    cpp_params.inventory_lifecycle_soft_gate_refill_edge_max = float(
        params.get("inventory_lifecycle_soft_gate_refill_edge_max", 0.0)
    )
    cpp_params.inventory_lifecycle_soft_gate_reversion_max = float(
        params.get("inventory_lifecycle_soft_gate_reversion_max", 0.5)
    )
    cpp_params.inventory_lifecycle_soft_gate_side = str(params.get("inventory_lifecycle_soft_gate_side", "BOTH") or "BOTH")
    cpp_params.position_timeout_s = max(0.0, float(params.get("position_timeout", 0.0) or 0.0))
    cpp_params.circuit_breaker_sigma = max(
        0.0, float(params.get("circuit_breaker_sigma", 0.0) or 0.0)
    )
    circuit_breaker_exit_mode = str(
        params.get("circuit_breaker_exit_mode", "maker_close") or "maker_close"
    ).lower()
    if circuit_breaker_exit_mode not in {"maker_close", "immediate_taker"}:
        raise ValueError(
            "circuit_breaker_exit_mode must be maker_close or immediate_taker"
        )
    cpp_params.circuit_breaker_maker_close = (
        circuit_breaker_exit_mode == "maker_close"
    )
    cpp_params.emergency_taker_close_enabled = bool(params.get("emergency_taker_close_enabled", False))
    cpp_params.trace_fills_max = max(0, int(params.get("trace_fills_max", 0) or 0))
    cpp_params.trace_quotes_max = max(0, int(params.get("trace_quotes_max", 0) or 0))
    cpp_params.trace_p3_reach_decisions_max = max(
        0,
        int(params.get("trace_p3_reach_decisions_max", 0) or 0),
    )
    cooldown_duration_trace_max = int(
        params.get("trace_cooldown_duration_opportunities_max", 0) or 0
    )
    if cooldown_duration_trace_max < 0:
        raise ValueError(
            "trace_cooldown_duration_opportunities_max must be non-negative"
        )
    cooldown_duration_fork_enabled = bool(
        params.get("cooldown_duration_fork_enabled", False)
    )
    cooldown_duration_abi_requested = bool(
        cooldown_duration_trace_max > 0 or cooldown_duration_fork_enabled
    )
    if cooldown_duration_abi_requested and not hasattr(
        cpp_params,
        "cooldown_duration_fork_enabled",
    ):
        raise RuntimeError(
            "cooldown duration fork replay requires the current C++ ABI; "
            "rebuild narrowgate_cpp from this source tree"
        )
    if hasattr(cpp_params, "cooldown_duration_fork_enabled"):
        cpp_params.trace_cooldown_duration_opportunities_max = (
            cooldown_duration_trace_max
        )
        cpp_params.cooldown_duration_fork_enabled = (
            cooldown_duration_fork_enabled
        )
        cpp_params.cooldown_duration_fork_action = str(
            params.get("cooldown_duration_fork_action", "") or ""
        ).strip().upper()
        cpp_params.cooldown_duration_fork_target_ordinal = int(
            params.get("cooldown_duration_fork_target_ordinal", 0) or 0
        )
        cpp_params.cooldown_duration_fork_target_ts_ms = int(
            params.get("cooldown_duration_fork_target_ts_ms", 0) or 0
        )
        cpp_params.cooldown_duration_fork_target_side = str(
            params.get("cooldown_duration_fork_target_side", "") or ""
        ).strip().upper()
        cpp_params.cooldown_duration_fork_target_order_id = int(
            params.get("cooldown_duration_fork_target_order_id", -1)
        )
        cpp_params.cooldown_duration_fork_target_inventory_lifecycle_id = int(
            params.get("cooldown_duration_fork_target_inventory_lifecycle_id", 0) or 0
        )
        cpp_params.cooldown_duration_fork_expected_baseline_ms = float(
            params.get(
                "cooldown_duration_fork_expected_baseline_ms",
                0.0,
            )
            or 0.0
        )
        cpp_params.cooldown_duration_fork_fixed_ms = float(
            params.get("cooldown_duration_fork_fixed_ms", 0.0) or 0.0
        )
        cpp_params.cooldown_duration_fork_baseline_policy_enabled = bool(
            params.get("cooldown_duration_fork_baseline_policy_enabled", False)
        )
        cpp_params.cooldown_duration_fork_expected_owner_action = str(
            params.get("cooldown_duration_fork_expected_owner_action", "") or ""
        )
        cpp_params.cooldown_duration_fork_expected_owner_policy_sha256 = str(
            params.get(
                "cooldown_duration_fork_expected_owner_policy_sha256", ""
            )
            or ""
        )
    cpp_params.trace_window_ms = max(1000, int(float(params.get("trace_fills_window_s", 10.0) or 10.0) * 1000.0))
    cpp_params.collect_curves = bool(params.get("collect_curves", True))
    var_ssq_arr = _as_f64_array(var_ssq)
    # Match SignalEngine/Python warmup. The first supplied variance row may
    # describe a not-yet-completed bar and must not seed current state.
    cpp_params.initial_sigma_sq = 1.0
    cpp_params.rng_seed = int(params.get("rng_seed", 42))
    cpp_params.latency_seed = effective_latency_seed(params)
    cpp_params.replay_contract_sha256 = str(
        params.get("replay_contract_sha256", "")
    )
    cpp_params.latency_sampler_version = str(
        params.get("latency_sampler_version", LATENCY_SAMPLER_VERSION)
        or LATENCY_SAMPLER_VERSION
    )
    if cpp_params.latency_sampler_version != LATENCY_SAMPLER_VERSION:
        raise ValueError(
            "Python/C++ replay requires latency_sampler_version="
            f"{LATENCY_SAMPLER_VERSION}"
        )
    cpp_params.latency_stress_enabled = bool(
        params.get("latency_stress_enabled", False)
    )
    cpp_params.latency_stress_spike_probability = float(
        params.get("latency_stress_spike_probability", 0.0) or 0.0
    )
    cpp_params.latency_stress_spike_multiplier = float(
        params.get("latency_stress_spike_multiplier", 1.0) or 1.0
    )
    cpp_params.random_passive_enabled = bool(params.get("random_passive_enabled", False))
    cpp_params.random_passive_seed = int(params.get("random_passive_seed", cpp_params.rng_seed + 10_003))
    cpp_params.random_passive_side_mirror_prob = min(
        1.0, max(0.0, float(params.get("random_passive_side_mirror_prob", 0.5) or 0.0))
    )
    cpp_params.random_passive_timing_jitter_fraction = min(
        0.95,
        max(0.0, float(params.get("random_passive_timing_jitter_fraction", 0.35) or 0.0)),
    )
    cpp_params.random_passive_preserve_inventory_skew = bool(
        params.get("random_passive_preserve_inventory_skew", True)
    )

    queue_calibration_enabled = bool(params.get("queue_regime_calibration_enabled", False))
    queue_calibration = params.get("_queue_calibration") if queue_calibration_enabled else None
    if queue_calibration:
        cpp_queue_tables = _compile_cpp_queue_calibration_tables(queue_calibration)
        cpp_params.queue_regime_distance_edges = cpp_queue_tables.get("queue_regime_distance_edges", [])
        cpp_params.queue_regime_rank_edges = cpp_queue_tables.get("queue_regime_rank_edges", [])
        cpp_params.queue_regime_buy_mult = cpp_queue_tables.get("queue_regime_buy_mult", [])
        cpp_params.queue_regime_sell_mult = cpp_queue_tables.get("queue_regime_sell_mult", [])
        cpp_params.queue_deplete_rank_edges = cpp_queue_tables.get("queue_deplete_rank_edges", [])
        cpp_params.queue_deplete_buy_mult = cpp_queue_tables.get("queue_deplete_buy_mult", [])
        cpp_params.queue_deplete_sell_mult = cpp_queue_tables.get("queue_deplete_sell_mult", [])
        cpp_params.queue_mo_edges = cpp_queue_tables.get("queue_mo_edges", [])
        cpp_params.queue_mo_buy_mult = cpp_queue_tables.get("queue_mo_buy_mult", [])
        cpp_params.queue_mo_sell_mult = cpp_queue_tables.get("queue_mo_sell_mult", [])
        queue_base_by_trade, queue_decay_by_trade, buy_fill_prob_by_trade, sell_fill_prob_by_trade = (
            build_daily_queue_arrays(
                trade_ts,
                queue_calibration,
                default_queue_base=cpp_params.queue_base,
                default_queue_decay=cpp_params.queue_decay,
                default_buy_fill_prob=cpp_params.buy_fill_prob,
                default_sell_fill_prob=cpp_params.sell_fill_prob,
            )
        )
        buy_queue_deplete_mult_by_trade = None
        sell_queue_deplete_mult_by_trade = None
    else:
        queue_base_by_trade = None
        queue_decay_by_trade = None
        buy_fill_prob_by_trade = None
        sell_fill_prob_by_trade = None
        buy_queue_deplete_mult_by_trade = None
        sell_queue_deplete_mult_by_trade = None

    var_ts_arr = _as_i64_array(var_ts_ms)
    ml_ts = ml_dir = ml_vol = ml_ret = ml_tox_bid = ml_tox_ask = None
    ml_prediction_features: dict[str, Any] = {}
    if ml_data is not None:
        ml_ts, ml_dir, ml_vol, ml_ret = ml_data[:4]
        if len(ml_data) >= 6:
            ml_tox_bid, ml_tox_ask = ml_data[4:6]
        else:
            ml_tox_bid = 1.0 - np.asarray(ml_dir, dtype=np.float64)
            ml_tox_ask = np.asarray(ml_dir, dtype=np.float64)
        feature_context_offset = 6 + len(XMARKET_REPLAY_FEATURE_COLUMNS)
        if (
            len(ml_data) > feature_context_offset
            and isinstance(ml_data[feature_context_offset], dict)
        ):
            ml_prediction_features = ml_data[feature_context_offset]

    buy_fill_static_logit_delta = _empty_f64_matrix()
    buy_fill_static_missing = _empty_f64_matrix()
    buy_fill_static_used = _empty_f64_matrix()
    if cpp_params.buy_fill_selection_live_enabled:
        if ml_ts is None or not ml_prediction_features:
            raise ValueError(
                "buy_fill_selection_live_enabled requires causal Prediction.feature_dict "
                "arrays in replay ML data"
            )
        if buy_fill_selection_model_path is None:
            raise RuntimeError("BUY fill-selection model path was not resolved")
        if not hasattr(cpp, "simulate_tick_arrays_ext_policy_v3"):
            raise RuntimeError(
                "C++ BUY fill-selection replay requires simulate_tick_arrays_ext_policy_v3; "
                "rebuild narrowgate_cpp from the current source tree"
            )
        (
            buy_fill_static_logit_delta,
            buy_fill_static_missing,
            buy_fill_static_used,
        ) = _compile_cpp_fill_selection_static_rows(
            buy_fill_selection_model_path,
            ml_ts,
            ml_prediction_features,
        )

    if historical_bbo:
        bbo_ts = bbo_data.ts_ms
        bbo_best_bid = bbo_data.best_bid
        bbo_best_ask = bbo_data.best_ask
        bbo_bid_qty = bbo_data.bid_qty
        bbo_ask_qty = bbo_data.ask_qty
        book_source = bbo_data.source
    else:
        bbo_ts = bbo_best_bid = bbo_best_ask = bbo_bid_qty = bbo_ask_qty = None
        book_source = "historical_l2" if historical_l2 else "inferred_trade"

    if historical_l2:
        l2_ts = l2_data.ts_ms
        l2_bid_px = l2_data.bid_px
        l2_bid_qty = l2_data.bid_qty
        l2_ask_px = l2_data.ask_px
        l2_ask_qty = l2_data.ask_qty
    else:
        l2_ts = l2_bid_px = l2_bid_qty = l2_ask_px = l2_ask_qty = None

    if any(getattr(book, "observation_ts_us", None) is not None
           for book in (bbo_data, l2_data)):
        if not hasattr(cpp_params, "set_book_observation_clocks"):
            raise RuntimeError("C++ replay requires book observation clock ABI; rebuild narrowgate_cpp")
        cpp_params.set_book_observation_clocks(
            _as_i64_array(book_observation_times_us(bbo_data) if historical_bbo else None),
            _as_i64_array(book_observation_times_us(l2_data) if historical_l2 else None),
        )
    replay_args = (
        trade_ts,
        trade_price,
        trade_qty,
        is_buyer_maker,
        var_ts_arr,
        var_ssq_arr,
        _as_f64_array(var_ti),
        _as_f64_array(var_retsq),
        _as_i64_array(ml_ts),
        _as_f64_array(ml_dir),
        _as_f64_array(ml_vol),
        _as_f64_array(ml_ret),
        _as_f64_array(ml_tox_bid),
        _as_f64_array(ml_tox_ask),
        _as_f64_matrix(buy_fill_static_logit_delta),
        _as_f64_matrix(buy_fill_static_missing),
        _as_f64_matrix(buy_fill_static_used),
        _as_i64_array(bbo_ts),
        _as_f64_array(bbo_best_bid),
        _as_f64_array(bbo_best_ask),
        _as_f64_array(bbo_bid_qty),
        _as_f64_array(bbo_ask_qty),
        _as_i64_array(l2_ts),
        _as_f64_matrix(l2_bid_px),
        _as_f64_matrix(l2_bid_qty),
        _as_f64_matrix(l2_ask_px),
        _as_f64_matrix(l2_ask_qty),
        _as_f64_array(queue_base_by_trade),
        _as_f64_array(queue_decay_by_trade),
        _as_f64_array(buy_fill_prob_by_trade),
        _as_f64_array(sell_fill_prob_by_trade),
        _as_f64_array(buy_queue_deplete_mult_by_trade),
        _as_f64_array(sell_queue_deplete_mult_by_trade),
    )
    p3_ts = _as_i64_array(params.get("_conditional_p3_ts_ms"))
    p3_distance_touch_product_argmax = _as_f64_array(params.get("_conditional_p3_distance_touch_product_argmax"))
    p3_touch_log_probability_distance_slope = _as_f64_array(params.get("_conditional_p3_touch_log_probability_distance_slope"))
    has_conditional_p3 = bool(
        p3_ts.size or p3_distance_touch_product_argmax.size or p3_touch_log_probability_distance_slope.size
    )
    if has_conditional_p3 != conditional_p3_requested:
        raise RuntimeError("conditional P3 overlay request identity changed during replay")
    if fair_center_payload_present:
        if not hasattr(cpp, "simulate_tick_arrays_ext_policy_v7"):
            raise RuntimeError(
                "cross-venue fair-center replay requires "
                "simulate_tick_arrays_ext_policy_v7; rebuild narrowgate_cpp from "
                "the current source tree"
            )
        cpp_result = cpp.simulate_tick_arrays_ext_policy_v7(
            replay_args,
            fair_center_ts_ms,
            fair_center_price,
            fair_center_gain,
            fair_center_valid,
            cpp_params,
        )
    elif p3_reach_budget_enabled:
        if not hasattr(cpp, "simulate_tick_arrays_ext_policy_v6"):
            raise RuntimeError(
                "adaptive conditional-P3 reach-budget replay requires "
                "simulate_tick_arrays_ext_policy_v6; rebuild narrowgate_cpp from "
                "the current source tree"
            )
        cpp_result = cpp.simulate_tick_arrays_ext_policy_v6(
            replay_args,
            p3_reach_budget_ts,
            p3_reach_budget_selected_k,
            cpp_params,
        )
    elif p3_reach_gate_enabled:
        if not hasattr(cpp, "simulate_tick_arrays_ext_policy_v5"):
            raise RuntimeError(
                "conditional P3 reach-gate replay requires "
                "simulate_tick_arrays_ext_policy_v5; rebuild narrowgate_cpp from "
                "the current source tree"
            )
        cpp_result = cpp.simulate_tick_arrays_ext_policy_v5(
            replay_args,
            p3_reach_gate_ts,
            p3_reach_gate_status,
            cpp_params,
        )
    elif has_conditional_p3:
        if not hasattr(cpp, "simulate_tick_arrays_ext_policy_v4"):
            raise RuntimeError(
                "conditional P3 replay requires simulate_tick_arrays_ext_policy_v4; "
                "rebuild narrowgate_cpp from the current source tree"
            )
        cpp_result = cpp.simulate_tick_arrays_ext_policy_v4(
            *replay_args,
            p3_ts,
            p3_distance_touch_product_argmax,
            p3_touch_log_probability_distance_slope,
            cpp_params,
        )
    else:
        cpp_result = cpp.simulate_tick_arrays_ext_policy_v3(
            *replay_args,
            cpp_params,
        )
    summary = cpp_result.summary

    ts_arr = np.asarray(cpp_result.pnl_ts_ms, dtype=np.int64)
    pnl_arr = np.asarray(cpp_result.pnl, dtype=np.float64)
    inv_arr = np.asarray(cpp_result.inventory, dtype=np.float64)
    curves_collected = ts_arr.size > 0
    if ts_arr.size == 0:
        ts_arr = np.array([int(trade_ts[-1])], dtype=np.int64)
        pnl_arr = np.array([float(summary.pnl)], dtype=np.float64)
        inv_arr = np.array([float(summary.final_inventory)], dtype=np.float64)
    else:
        rq_ms = max(1, int(float(params.get("requote_interval", 1.0)) * 1000.0))
        rq_min_ms = max(1, int(float(params.get("rq_min", params.get("requote_interval", 1.0))) * 1000.0))
        rq_max_ms = max(rq_min_ms, int(float(params.get("rq_max", params.get("requote_interval", 1.0))) * 1000.0))
        dynamic_rq = rq_min_ms < rq_max_ms and var_retsq is not None and len(var_retsq) == len(var_ts_ms)
        eff_rq_ms = rq_min_ms if dynamic_rq else rq_ms
        elapsed_ms_for_samples = max(0, int(trade_ts[-1] - trade_ts[0]))
        max_samples = elapsed_ms_for_samples // max(1, eff_rq_ms) + 100
        if ts_arr.size > max_samples:
            ts_arr = ts_arr[:max_samples]
            pnl_arr = pnl_arr[:max_samples]
            inv_arr = inv_arr[:max_samples]

    elapsed_s = max(0.0, float(trade_ts[-1] - trade_ts[0]) / 1000.0)
    n_days = replay_elapsed_days(trade_ts)
    if curves_collected and pnl_arr.size > 1:
        pnl_diff = np.diff(pnl_arr)
        dt_s = np.diff(ts_arr).astype(np.float64) / 1000.0
        valid = dt_s > 0.0
        if np.any(valid):
            dp = pnl_diff[valid]
            dt = dt_s[valid]
            mu_sec = dp.sum() / dt.sum()
            sigma_sec = (dp / np.sqrt(dt)).std()
            sharpe = mu_sec / sigma_sec * math.sqrt(365.25 * 86400.0) if sigma_sec > 0 else 0.0
        else:
            sharpe = 0.0
        max_dd = float((np.maximum.accumulate(pnl_arr) - pnl_arr).max())
    else:
        sharpe = float(getattr(summary, "sharpe", 0.0))
        max_dd = float(getattr(summary, "max_drawdown", 0.0))

    (
        dynamic_cap_enabled,
        max_spread_bps,
        dynamic_cap_base_bps,
        dynamic_cap_alpha,
        dynamic_cap_max_mult,
        dynamic_cap_var_baseline,
        dynamic_cap_liq_beta,
        dynamic_cap_liq_baseline,
        dynamic_cap_min_mult,
    ) = _resolve_cap_params(params)
    if dynamic_cap_enabled and dynamic_cap_base_bps > 0.0:
        cap_mode = "dynamic"
        cap_bps = dynamic_cap_base_bps
    elif max_spread_bps > 0.0:
        cap_mode = "fixed"
        cap_bps = max_spread_bps
    else:
        cap_mode = "off"
        cap_bps = 0.0
    cap_label = params.get("cap_label", _cap_label_from_fields(cap_mode, cap_bps))

    abs_inventory_time_s = float(summary.abs_inventory_time_s)
    signed_inventory_time_s = float(summary.signed_inventory_time_s)
    sq_inventory_time_s = float(summary.sq_inventory_time_s)
    signed_notional_inventory_time_s = float(getattr(summary, "signed_notional_inventory_time_s", 0.0))
    notional_inventory_time_s = float(summary.notional_inventory_time_s)
    inv_time_hours = abs_inventory_time_s / 3600.0
    final_pnl = float(summary.pnl)
    cash_before_terminal = float(summary.cash)
    terminal_mark_price = float(getattr(summary, "terminal_mark_price", trade_price[-1]))
    mtm_before_terminal_fee = float(
        getattr(summary, "mtm_before_terminal_fee", cash_before_terminal + float(summary.final_inventory) * terminal_mark_price)
    )
    terminal_fee_drag = float(
        getattr(summary, "terminal_fee_drag", 0.0)
    )
    terminal_liquidation_fee_estimate = float(
        getattr(
            summary,
            "terminal_liquidation_fee_estimate",
            abs(float(summary.final_inventory))
            * terminal_mark_price
            * max(0.0, float(params.get("taker_fee", 0.0) or 0.0)),
        )
    )
    inventory_pnl = float(summary.inventory_pnl)
    inventory_adjusted_pnl = final_pnl - inventory_pnl
    n_requotes = int(summary.n_requotes)
    fills_total = int(summary.fills_total)
    n_final_spread = int(summary.n_final_spread)

    order_trace_fields = [
        "order_id", "side", "submit_ts", "activate_ts", "quote_ts", "price", "quantity",
        "raw_half_spread", "capped_half_spread", "raw_mid_shift", "raw_reservation_shift",
        "raw_asym_shift", "asym", "inventory", "dir_signal", "pred_dir", "pred_ret",
        "tox_bid", "tox_ask", "book_imb", "weighted_mid_proxy_shift_bps", "near_depth_total",
        "l2_near_depth_total", "l2_quote_flip_rate", "l2_book_refresh_ratio", "l2_book_cancel_ratio",
        "mo_ema_bid", "mo_ema_ask", "fair", "mid", "best_bid", "best_ask",
        "raw_pair_spread", "capped_pair_spread", "final_pair_spread", "raw_price",
        "pre_guard_price", "final_price", "raw_quote_delta_to_bbo",
        "pre_guard_delta_to_bbo", "final_quote_delta_to_bbo", "raw_distance_to_mid",
        "final_distance_to_mid", "raw_quote_skew", "final_quote_skew", "raw_bias_side",
        "final_bias_side", "favored_by_raw_shift", "delta_cap", "mid_guard",
        "post_only", "side_adverse", "side_adverse_pause", "adverse_toxicity",
        "adverse_markout", "adverse_direction", "adverse_ret", "adverse_weighted_mid_proxy",
        "adverse_thin_depth", "local_extreme_guard", "local_extreme_pause",
        "local_extreme_rank", "local_extreme_window_s", "defense_guard",
        "defense_pause", "defense_reducing", "defense_emergency", "defense_markout",
        "defense_direction", "defense_ret", "defense_weighted_mid_proxy", "defense_spread_mult",
        "final_compressed", "bid_adverse", "ask_adverse",
        "buy_fill_selection_live_score", "buy_fill_selection_live_hit",
        "buy_fill_selection_live_missing_features", "random_passive_mirrored",
        "final_guard_changed",
        "any_constraint_changed", "outcome", "outcome_ts", "lifetime_ms",
        "cancel_reason", "fill_qty", "remaining", "queue_init", "queue_left",
        "pending_cancel",
    ]
    fill_trace_fields = [
        "fill_sequence", "side", "fill_ts", "quote_ts", "age_ms", "quote_mid", "quote_px",
        "fill_trade_px", "quote_dist", "quote_window_extreme", "quote_window_move",
        "window5_min", "window5_max", "window10_min", "window10_max",
        "window120_min", "window120_max", "window120_rank",
        "move_from_quote_mid_to_fill", "queue_init", "queue_before", "rem_before",
        "fill_qty", "fill_fee_rate", "fill_fee_usdc", "inventory_before_fill",
        "inventory_after_fill",
        "markout_1s", "markout_5s", "markout_20s", "markout_30s",
        "ev_1s", "ev_5s", "ev_20s", "ev_30s", "toxic_1s", "toxic_5s",
        "toxic_20s", "toxic_30s",
    ]

    def _cpp_row_to_dict(row, fields):
        return {name: getattr(row, name) for name in fields if hasattr(row, name)}

    quote_trace = [_cpp_row_to_dict(row, order_trace_fields) for row in getattr(cpp_result, "quote_trace", [])]
    fill_trace = []
    for row in getattr(cpp_result, "fill_trace", []):
        out = _cpp_row_to_dict(row, fill_trace_fields)
        out["fill_fee_asset"] = "USDC"
        out["fill_fee_semantics"] = FEE_ACCOUNTING_SEMANTICS
        # Normalize even a previously built native module's diagnostic export;
        # no matching, strategy state or accounting value is changed here.
        out.update(fill_markout_diagnostics(
            trade_ts, trade_price, fill_ts_ms=int(out["fill_ts"]),
            quote_px=float(out["quote_px"]),
            side="BUY" if row.side == cpp.Side.Buy else "SELL",
            fee_rate=float(out["fill_fee_rate"]),
            outcome_end_ts_ms=params.get("replay_event_clock_end_ts_ms"),
        ))
        order_row = getattr(row, "order", None)
        if order_row is not None:
            merged = _cpp_row_to_dict(order_row, order_trace_fields)
            merged.update(out)
            out = merged
        fill_trace.append(out)
    p3_reach_decision_fields = [
        "decision_ts_ms",
        "prediction_ts_ms",
        "side",
        "role",
        "exposure_increasing",
        "baseline_eligible",
        "toxicity_score",
        "toxicity_threshold",
        "best_bid",
        "best_ask",
        "inventory_btc",
        "baseline_price",
        "candidate_price",
        "side_policy_spread_mult",
        "side_policy_allow_post",
        "side_policy_allow_exposure_increase",
        "side_policy_reason_mask",
        "baseline_distance_ticks",
        "reach_gate_status",
        "reach_gate_requested",
        "price_changed",
        "spread_cap_noop",
    ]
    p3_reach_decision_trace = [
        _cpp_row_to_dict(row, p3_reach_decision_fields)
        for row in getattr(cpp_result, "p3_reach_decision_trace", [])
    ]
    cooldown_duration_opportunity_fields = [
        "schema_version",
        "fill_clock_semantics",
        "live_receive_time_authority",
        "exposure_fill_ordinal",
        "fill_visible_ts_ms",
        "fill_exchange_ts_ms",
        "side",
        "role_at_fill",
        "order_id",
        "inventory_lifecycle_id",
        "inventory_before_fill_btc",
        "inventory_after_fill_btc",
        "fill_qty_btc",
        "unit_qty_btc",
        "consecutive_units_before",
        "consecutive_units_after",
        "prior_deadline_ts_ms",
        "baseline_duration_ms",
        "baseline_deadline_ts_ms",
        "canonical_mid",
        "best_bid",
        "best_ask",
        "decision_visible_bbo_index",
        "decision_visible_l2_index",
        "market_event_index",
        "assignment_equity_usdc",
    ]
    cooldown_duration_opportunity_trace = [
        _cpp_row_to_dict(row, cooldown_duration_opportunity_fields)
        for row in getattr(
            cpp_result,
            "cooldown_duration_opportunity_trace",
            [],
        )
    ]
    cooldown_duration_fill_path_fields = [
        "path_fill_ordinal",
        "fill_visible_ts_ms",
        "side",
        "order_id",
        "inventory_lifecycle_id",
        "exposure_increasing",
        "target_fill",
        "fill_price_usdc_per_btc",
        "fill_qty_btc",
        "inventory_before_fill_btc",
        "inventory_after_fill_btc",
        "cash_after_fill_usdc",
        "baseline_duration_ms",
        "applied_duration_ms",
        "applied_deadline_ts_ms",
    ]
    cooldown_duration_fill_path = [
        _cpp_row_to_dict(row, cooldown_duration_fill_path_fields)
        for row in getattr(cpp_result, "cooldown_duration_fill_path", [])
    ]
    cooldown_duration_fork_fields = [
        "schema_version",
        "action",
        "side",
        "inventory_lifecycle_id",
        "target_exposure_fill_ordinal",
        "target_order_id",
        "assignment_ts_ms",
        "assignment_inventory_btc",
        "assignment_equity_usdc",
        "baseline_duration_ms",
        "applied_duration_ms",
        "applied_deadline_ts_ms",
        "exact_owner_baseline_policy_enabled",
        "exact_owner_action",
        "exact_owner_policy_sha256",
        "exact_owner_baseline_duration_ms",
        "quarantine_entered",
        "quarantine_ts_ms",
        "washout_protocol",
        "control_path_exact_until_quarantine",
        "exposure_permission_change_count",
        "reducing_permission_control_checks",
        "reducing_quote_change_count",
        "second_assignment_count",
        "arm_washout_complete",
        "terminal_ts_ms",
        "terminal_reason",
        "right_censored",
        "terminal_inventory_btc",
        "terminal_mid_usdc_per_btc",
        "final_cash_usdc",
        "final_pnl_usdc",
        "accounting_residual_usdc",
        "assignment_to_washout_value_usdc",
        "censor_time_mid_mark_usdc",
        "censor_time_executable_mark_usdc",
        "censor_marks_are_terminal_bounds",
        "post_assignment_buy_fill_count",
        "post_assignment_sell_fill_count",
        "inventory_time_btc_s",
        "mae_usdc",
        "max_abs_inventory_btc",
        "active_or_pending_order_count",
        "pending_submit_count",
        "pending_cancel_count",
        "pending_ack_count",
        "inventory_lifecycle_active",
        "cursor_owner_count",
        "hazard_owner_count",
    ]
    cooldown_duration_fork_obj = getattr(
        cpp_result,
        "cooldown_duration_fork_trace",
        None,
    )
    cooldown_duration_fork_trace = (
        _cpp_row_to_dict(
            cooldown_duration_fork_obj,
            cooldown_duration_fork_fields,
        )
        if cooldown_duration_fork_obj is not None
        and bool(getattr(cooldown_duration_fork_obj, "enabled", False))
        else {}
    )
    f05_repeated_cooldown_decisions = []
    for row in getattr(cpp_result, "f05_repeated_cooldown_decisions", []):
        side = "BUY" if row.side == cpp.Side.Buy else "SELL"
        role = {
            cpp.F05CooldownFillRole.OPENER: "opener",
            cpp.F05CooldownFillRole.ADD: "add",
            cpp.F05CooldownFillRole.REDUCING: "reducing",
        }[row.role]
        f05_repeated_cooldown_decisions.append(
            {
                "schema_version": (
                    "causal_multichannel_window_boolean_cooldown_"
                    "repeated_policy_decision.v1"
                ),
                "exposure_fill_ordinal": int(row.exposure_fill_ordinal),
                "fill_visible_ts_ms": int(row.fill_ts_ms),
                "side": side,
                "role_at_fill": role,
                "inventory_lifecycle_id": int(row.inventory_lifecycle_id),
                "consecutive_units_after": float(row.consecutive_units_after),
                "baseline_duration_ms": float(row.baseline_duration_ms),
                "action_id": str(row.action_id),
                "duration_ms": float(row.duration_ms),
                "deadline_ts_ms": int(row.deadline_ts_ms),
                "lineage_revision": int(row.lineage_revision),
                "lineage_applied": bool(row.lineage_applied),
                "coverage_reason_code": str(row.coverage_reason_code),
                "fallback_reason": (
                    None if not row.fallback_reason else str(row.fallback_reason)
                ),
                "matched_rule_index": (
                    None
                    if row.matched_rule_index is None
                    else int(row.matched_rule_index)
                ),
                "policy_sha256": str(row.policy_sha256),
                "predicate_bundle_sha256": str(
                    row.predicate_bundle_sha256
                ),
                "snapshot_id": str(row.snapshot_id),
                "support_valid": bool(row.support_valid),
                "feature_ready_ts_ns": int(row.feature_ready_ts_ns),
                "feature_age_ms": float(row.feature_age_ms),
            }
        )
    f05_repeated_cooldown_checkpoint = getattr(
        cpp_result,
        "f05_repeated_cooldown_checkpoint",
        None,
    )
    f05_repeated_cooldown_audit = {}
    f05_qualification_under_test = False
    f05_cooldown_authoritative = False
    if f05_repeated_cooldown_checkpoint is not None:
        audit = f05_repeated_cooldown_checkpoint.audit
        f05_repeated_cooldown_audit = {
            name: int(getattr(audit, name))
            for name in (
                "window_count",
                "gap_window_count",
                "feature_state_reset_count",
                "evaluation_count",
                "supported_count",
                "fallback_count",
                "nonbaseline_count",
                "buy_control_count",
                "reducing_bypass_count",
                "lineage_count",
                "lineage_clear_count",
            )
        }
        f05_qualification_under_test = bool(
            f05_repeated_cooldown_checkpoint.qualification_under_test
        )
        f05_cooldown_authoritative = bool(
            not f05_qualification_under_test
            and str(
                f05_repeated_cooldown_checkpoint.parity_qualification_sha256
            )
        )
        f05_repeated_cooldown_audit.update(
            {
                "qualification_under_test": f05_qualification_under_test,
                "paired_execution_only": f05_qualification_under_test,
                "authoritative": f05_cooldown_authoritative,
                "qualification_mode": (
                    "paired_non_authoritative_under_test"
                    if f05_qualification_under_test
                    else "external_parity_receipt"
                ),
            }
        )
    paired_fixed_spread_fields = [
        "side",
        "distance_ticks",
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
        "fill_qty",
    ]
    paired_fixed_spread_rows = [
        _cpp_row_to_dict(row, paired_fixed_spread_fields)
        for row in getattr(cpp_result, "paired_fixed_spread_rows", [])
    ]
    paired_fixed_spread_violation_fields = [
        "cohort_id",
        "side",
        "event_ts_ms",
        "shallower_distance_ticks",
        "deeper_distance_ticks",
    ]
    paired_fixed_spread_violations = [
        _cpp_row_to_dict(row, paired_fixed_spread_violation_fields)
        for row in getattr(cpp_result, "paired_fixed_spread_violations", [])
    ]
    cpp_buy_fill_qty = sum(float(r.get("fill_qty", 0.0) or 0.0) for r in fill_trace if r.get("side") == "BUY")
    cpp_sell_fill_qty = sum(float(r.get("fill_qty", 0.0) or 0.0) for r in fill_trace if r.get("side") == "SELL")
    cpp_buy_fill_notional = sum(
        float(r.get("fill_qty", 0.0) or 0.0) * float(r.get("quote_px", r.get("fill_trade_px", 0.0)) or 0.0)
        for r in fill_trace
        if r.get("side") == "BUY"
    )
    cpp_sell_fill_notional = sum(
        float(r.get("fill_qty", 0.0) or 0.0) * float(r.get("quote_px", r.get("fill_trade_px", 0.0)) or 0.0)
        for r in fill_trace
        if r.get("side") == "SELL"
    )

    result = {
        "engine": "cpp",
        "risk_daily_loss_block_count": int(summary.risk_daily_loss_block_count),
        "risk_position_value_block_count": int(summary.risk_position_value_block_count),
        "risk_emergency_close_count": int(summary.risk_emergency_close_count),
        "risk_notional_cap_count": int(summary.risk_notional_cap_count),
        "risk_emergency_latched": bool(summary.risk_emergency_latched),
        "_final_risk_state": {
            "utc_day": int(summary.risk_utc_day),
            "day_start_total_pnl": float(summary.risk_day_start_total_pnl),
            "session_peak_pnl": float(summary.risk_session_peak_pnl),
            "last_total_pnl": float(summary.risk_last_total_pnl),
            "total_pnl_offset": float(summary.risk_total_pnl_offset),
        },
        "engine_policy": "experimental_cpp_tick_replay",
        "strict_calibration": bool(params.get("strict_calibration", False)),
        "strict_calibration_validated": bool(params.get("strict_calibration_validated", False)),
        "random_passive_enabled": bool(cpp_params.random_passive_enabled),
        "random_passive_sampler_version": RANDOM_PASSIVE_SAMPLER_VERSION,
        "random_passive_seed": int(cpp_params.random_passive_seed),
        "random_passive_side_mirror_prob": float(cpp_params.random_passive_side_mirror_prob),
        "random_passive_timing_jitter_fraction": float(cpp_params.random_passive_timing_jitter_fraction),
        "random_passive_preserve_inventory_skew": bool(cpp_params.random_passive_preserve_inventory_skew),
        "random_passive_mirror_count": int(getattr(summary, "random_passive_mirror_count", 0)),
        "random_passive_mirror_eligible_count": int(getattr(summary, "random_passive_mirror_eligible_count", 0)),
        "random_passive_timing_jitter_count": int(getattr(summary, "random_passive_timing_jitter_count", 0)),
        "fixed_spread_probe_enabled": fixed_spread_probe_enabled,
        "fixed_spread_probe_ticks": fixed_spread_probe_ticks,
        "paired_fixed_spread_probe_enabled": (
            paired_fixed_spread_probe_enabled
        ),
        "paired_fixed_spread_probe_ticks": paired_fixed_spread_probe_ticks,
        "paired_fixed_spread_rows": paired_fixed_spread_rows,
        "paired_fixed_spread_violations": (
            paired_fixed_spread_violations
        ),
        "fixed_spread_probe_bid_submitted_orders": int(
            getattr(summary, "fixed_spread_probe_bid_submitted_orders", 0)
        ),
        "fixed_spread_probe_ask_submitted_orders": int(
            getattr(summary, "fixed_spread_probe_ask_submitted_orders", 0)
        ),
        "fixed_spread_probe_bid_activation_gtx_rejects": int(
            getattr(summary, "fixed_spread_probe_bid_activation_gtx_rejects", 0)
        ),
        "fixed_spread_probe_ask_activation_gtx_rejects": int(
            getattr(summary, "fixed_spread_probe_ask_activation_gtx_rejects", 0)
        ),
        "fixed_spread_probe_bid_placed_orders": int(
            getattr(summary, "fixed_spread_probe_bid_placed_orders", 0)
        ),
        "fixed_spread_probe_ask_placed_orders": int(
            getattr(summary, "fixed_spread_probe_ask_placed_orders", 0)
        ),
        "fixed_spread_probe_bid_queue_visible_positive_orders": int(
            getattr(
                summary,
                "fixed_spread_probe_bid_queue_visible_positive_orders",
                0,
            )
        ),
        "fixed_spread_probe_ask_queue_visible_positive_orders": int(
            getattr(
                summary,
                "fixed_spread_probe_ask_queue_visible_positive_orders",
                0,
            )
        ),
        "fixed_spread_probe_bid_queue_known_zero_orders": int(
            getattr(summary, "fixed_spread_probe_bid_queue_known_zero_orders", 0)
        ),
        "fixed_spread_probe_ask_queue_known_zero_orders": int(
            getattr(summary, "fixed_spread_probe_ask_queue_known_zero_orders", 0)
        ),
        "fixed_spread_probe_bid_queue_fallback_orders": int(
            getattr(summary, "fixed_spread_probe_bid_queue_fallback_orders", 0)
        ),
        "fixed_spread_probe_ask_queue_fallback_orders": int(
            getattr(summary, "fixed_spread_probe_ask_queue_fallback_orders", 0)
        ),
        "fixed_spread_probe_bid_active_touched_orders": int(
            getattr(summary, "fixed_spread_probe_bid_active_touched_orders", 0)
        ),
        "fixed_spread_probe_ask_active_touched_orders": int(
            getattr(summary, "fixed_spread_probe_ask_active_touched_orders", 0)
        ),
        "fixed_spread_probe_bid_filled_orders": int(
            getattr(summary, "fixed_spread_probe_bid_filled_orders", 0)
        ),
        "fixed_spread_probe_ask_filled_orders": int(
            getattr(summary, "fixed_spread_probe_ask_filled_orders", 0)
        ),
        "fixed_spread_probe_bid_fully_filled_orders": int(
            getattr(summary, "fixed_spread_probe_bid_fully_filled_orders", 0)
        ),
        "fixed_spread_probe_ask_fully_filled_orders": int(
            getattr(summary, "fixed_spread_probe_ask_fully_filled_orders", 0)
        ),
        "fixed_spread_probe_bid_first_fill_pending_cancel_orders": int(
            getattr(
                summary,
                "fixed_spread_probe_bid_first_fill_pending_cancel_orders",
                0,
            )
        ),
        "fixed_spread_probe_ask_first_fill_pending_cancel_orders": int(
            getattr(
                summary,
                "fixed_spread_probe_ask_first_fill_pending_cancel_orders",
                0,
            )
        ),
        "fixed_spread_probe_bid_filled_within_1s": int(
            getattr(summary, "fixed_spread_probe_bid_filled_within_1s", 0)
        ),
        "fixed_spread_probe_ask_filled_within_1s": int(
            getattr(summary, "fixed_spread_probe_ask_filled_within_1s", 0)
        ),
        "fixed_spread_probe_bid_filled_within_5s": int(
            getattr(summary, "fixed_spread_probe_bid_filled_within_5s", 0)
        ),
        "fixed_spread_probe_ask_filled_within_5s": int(
            getattr(summary, "fixed_spread_probe_ask_filled_within_5s", 0)
        ),
        "fixed_spread_probe_bid_filled_within_10s": int(
            getattr(summary, "fixed_spread_probe_bid_filled_within_10s", 0)
        ),
        "fixed_spread_probe_ask_filled_within_10s": int(
            getattr(summary, "fixed_spread_probe_ask_filled_within_10s", 0)
        ),
        "fixed_spread_probe_bid_end_censored_unfilled": int(
            getattr(summary, "fixed_spread_probe_bid_end_censored_unfilled", 0)
        ),
        "fixed_spread_probe_ask_end_censored_unfilled": int(
            getattr(summary, "fixed_spread_probe_ask_end_censored_unfilled", 0)
        ),
        "fixed_spread_probe_bid_end_censored_before_1s": int(
            getattr(summary, "fixed_spread_probe_bid_end_censored_before_1s", 0)
        ),
        "fixed_spread_probe_ask_end_censored_before_1s": int(
            getattr(summary, "fixed_spread_probe_ask_end_censored_before_1s", 0)
        ),
        "fixed_spread_probe_bid_end_censored_before_5s": int(
            getattr(summary, "fixed_spread_probe_bid_end_censored_before_5s", 0)
        ),
        "fixed_spread_probe_ask_end_censored_before_5s": int(
            getattr(summary, "fixed_spread_probe_ask_end_censored_before_5s", 0)
        ),
        "fixed_spread_probe_bid_end_censored_before_10s": int(
            getattr(summary, "fixed_spread_probe_bid_end_censored_before_10s", 0)
        ),
        "fixed_spread_probe_ask_end_censored_before_10s": int(
            getattr(summary, "fixed_spread_probe_ask_end_censored_before_10s", 0)
        ),
        "fixed_spread_probe_bid_fill_qty": float(
            getattr(summary, "fixed_spread_probe_bid_fill_qty", 0.0)
        ),
        "fixed_spread_probe_ask_fill_qty": float(
            getattr(summary, "fixed_spread_probe_ask_fill_qty", 0.0)
        ),
        "eta_inventory": float(params["eta_inventory"]),
        "risk_per_order": float(params["risk_per_order"]),
        "execution_intensity_slope": float(params.get("execution_intensity_slope", 0.0)),
        "queue_base": float(params.get("queue_base", 5.0)),
        "queue_decay": float(params.get("queue_decay", 0.1)),
        "queue_ahead_mode": queue_mode,
        "book_source_authority": str(
            params.get("_book_source_authority", "unclassified")
        ),
        "book_dataset_version": str(
            params.get("_book_dataset_version", "")
        ),
        "book_exact_queue_policy_eligible": bool(
            params.get("_book_exact_queue_policy_eligible", False)
        ),
        "queue_calibration_enabled": bool(queue_calibration),
        "queue_price_tolerance": cpp_params.queue_price_tolerance,
        "maker_fill_prob": cpp_params.maker_fill_prob,
        "queue_ahead_base_mult": cpp_params.queue_ahead_base_mult,
        "queue_deplete_base_mult": cpp_params.queue_deplete_base_mult,
        "queue_l2_cancel_ahead_enabled": bool(cpp_params.queue_l2_cancel_ahead_enabled),
        "queue_l2_cancel_ahead_event_count": int(
            getattr(summary, "queue_l2_cancel_ahead_event_count", 0)
        ),
        "queue_l2_cancel_ahead_bid_event_count": int(
            getattr(summary, "queue_l2_cancel_ahead_bid_event_count", 0)
        ),
        "queue_l2_cancel_ahead_ask_event_count": int(
            getattr(summary, "queue_l2_cancel_ahead_ask_event_count", 0)
        ),
        "queue_l2_cancel_ahead_qty": float(
            getattr(summary, "queue_l2_cancel_ahead_qty", 0.0)
        ),
        "exchange_book_events_consumed": int(
            getattr(summary, "native_book_events_consumed", 0)
        ),
        "exchange_book_events_accepted": int(
            getattr(summary, "native_book_events_accepted", 0)
        ),
        "exchange_book_events_rejected": int(
            getattr(summary, "native_book_events_rejected", 0)
        ),
        "exchange_book_snapshot_events": int(
            getattr(summary, "native_book_snapshot_events", 0)
        ),
        "exchange_book_sequence_gaps": int(
            getattr(summary, "native_book_sequence_gaps", 0)
        ),
        "exchange_book_queue_lookup_count": int(
            getattr(summary, "native_queue_lookup_count", 0)
        ),
        "exchange_book_queue_exact_count": int(
            getattr(summary, "native_queue_exact_count", 0)
        ),
        "exchange_book_queue_known_zero_count": int(
            getattr(summary, "native_queue_known_zero_count", 0)
        ),
        "exchange_book_queue_missing_count": int(
            getattr(summary, "native_queue_missing_count", 0)
        ),
        "exchange_book_queue_invalidated_order_count": int(
            getattr(summary, "native_queue_invalidated_order_count", 0)
        ),
        "exchange_book_queue_ambiguous_event_count": int(
            getattr(summary, "native_queue_ambiguous_event_count", 0)
        ),
        "exchange_book_queue_cancel_ahead_event_count": int(
            getattr(summary, "native_queue_cancel_ahead_event_count", 0)
        ),
        "exchange_book_queue_cancel_ahead_qty": float(
            getattr(summary, "native_queue_cancel_ahead_qty", 0.0)
        ),
        "exchange_book_queue_mode": exchange_book_queue_mode,
        "exchange_book_queue_scope": (
            "strategy_independent_native_snapshot_delta_exchange_time_v1"
            if exchange_book_event_tape is not None
            else "disabled"
        ),
        "queue_ahead_buy_exposure_mult": cpp_params.queue_ahead_buy_exposure_mult,
        "queue_ahead_buy_reducing_mult": cpp_params.queue_ahead_buy_reducing_mult,
        "queue_ahead_sell_exposure_mult": cpp_params.queue_ahead_sell_exposure_mult,
        "queue_ahead_sell_reducing_mult": cpp_params.queue_ahead_sell_reducing_mult,
        "new_order_latency_ms": cpp_params.new_order_latency_ms,
        "cancel_order_latency_ms": cpp_params.cancel_order_latency_ms,
        "latency_jitter_ms": latency_jitter_ms,
        "new_order_latency_sample_count": len(cpp_params.new_order_latency_samples_ms),
        "cancel_order_latency_sample_count": len(cpp_params.cancel_order_latency_samples_ms),
        **(
            {
                "decision_to_gateway_latency_sample_count": len(
                    cpp_params.decision_to_gateway_latency_samples_ms
                ),
                "decision_to_gateway_latency_seed": int(
                    cpp_params.decision_to_gateway_latency_seed
                ),
                "decision_to_gateway_latency_authority": "diagnostic_only",
                "decision_market_snapshot_clock": "decision_time_frozen",
            }
            if decision_to_gateway_latency_enabled
            else {}
        ),
        **(
            {
                "new_order_exchange_effective_latency_sample_count": len(
                    cpp_params.new_order_exchange_effective_latency_samples_ms
                ),
                "cancel_exchange_effective_latency_sample_count": len(
                    cpp_params.cancel_exchange_effective_latency_samples_ms
                ),
                "cancel_ack_visibility_latency_sample_count": len(
                    cpp_params.cancel_ack_visibility_latency_samples_ms
                ),
                "cancel_latency_split_enabled": bool(
                    cpp_params.cancel_exchange_effective_latency_samples_ms
                    or cpp_params.cancel_ack_visibility_latency_samples_ms
                    or decision_to_gateway_latency_enabled
                ),
            }
            if split_lifecycle_latency_requested
            or decision_to_gateway_latency_enabled
            else {}
        ),
        "replay_contract_sha256": str(cpp_params.replay_contract_sha256),
        "replay_purpose": str(params.get("replay_purpose", "exploratory")),
        "replay_initial_state_mode": str(
            params.get("replay_initial_state_mode", "fresh_start")
        ),
        "replay_promotion_eligible": bool(
            params.get("replay_promotion_eligible", False)
        ),
        "replay_evidence_scope": str(params.get("replay_evidence_scope", "")),
        "latency_sampler_version": str(cpp_params.latency_sampler_version),
        "latency_profile_id": str(params.get("latency_profile_id", "")),
        "latency_environment": params.get("latency_environment", ""),
        "latency_scenario": str(params.get("latency_scenario", "baseline")),
        "latency_seed": int(cpp_params.latency_seed),
        "latency_stress_enabled": bool(cpp_params.latency_stress_enabled),
        "latency_stress_spike_probability": float(
            cpp_params.latency_stress_spike_probability
        ),
        "latency_stress_spike_multiplier": float(
            cpp_params.latency_stress_spike_multiplier
        ),
        "exec_book_visibility_delay_enabled": bool(
            cpp_params.exec_book_visibility_delay_samples_ms
            or cpp_params.exec_book_visibility_delay_mean_ms > 0.0
            or cpp_params.exec_book_visibility_delay_jitter_ms > 0.0
        ),
        "exec_book_visibility_delay_profile_id": str(
            params.get("exec_book_visibility_delay_profile_id", "")
        ),
        "exec_book_visibility_delay_profile_path": str(
            params.get("exec_book_visibility_delay_profile_path", "")
        ),
        "exec_book_visibility_delay_sample_count": len(
            cpp_params.exec_book_visibility_delay_samples_ms
        ),
        "exec_book_visibility_delay_mean_ms": float(
            cpp_params.exec_book_visibility_delay_mean_ms
        ),
        "exec_book_visibility_delay_jitter_ms": float(
            cpp_params.exec_book_visibility_delay_jitter_ms
        ),
        "exec_book_visibility_delay_seed": int(
            cpp_params.exec_book_visibility_delay_seed
        ),
        "exec_book_visibility_delay_applied_count": int(
            getattr(
                summary,
                "exec_book_visibility_delay_applied_count",
                0,
            )
        ),
        "exec_book_visibility_delay_applied_avg_ms": (
            float(
                getattr(
                    summary,
                    "exec_book_visibility_delay_sum_ms",
                    0.0,
                )
            )
            / max(
                int(
                    getattr(
                        summary,
                        "exec_book_visibility_delay_applied_count",
                        0,
                    )
                ),
                1,
            )
        ),
        "exec_book_visibility_delay_applied_max_ms": int(
            getattr(summary, "exec_book_visibility_delay_max_ms", 0)
        ),
        "live_perf_telemetry_path": str(params.get("live_perf_telemetry_path", "")),
        "live_perf_latency_mode": str(params.get("live_perf_latency_mode", "")),
        "replace_min_price_change_ticks": float(params.get("replace_min_price_change_ticks", 0.0) or 0.0),
        "replace_min_price_change_ticks_reducing": float(
            params.get("replace_min_price_change_ticks_reducing", 0.0) or 0.0
        ),
        "replace_min_interval_ms": float(params.get("replace_min_interval_ms", 0.0) or 0.0),
        "replace_min_interval_ms_reducing": float(params.get("replace_min_interval_ms_reducing", 0.0) or 0.0),
        "replace_pending_coalesce": bool(params.get("replace_pending_coalesce", False)),
        "replace_pending_coalesce_replayed": True,
        **(
            {
                "replace_terminal_continuation": True,
                "replace_terminal_continuation_terminal_count": int(
                    summary.replace_terminal_continuation_terminal_count
                ),
                "replace_terminal_continuation_decision_count": int(
                    summary.replace_terminal_continuation_decision_count
                ),
                "replace_terminal_continuation_bid_decision_count": int(
                    summary.replace_terminal_continuation_bid_decision_count
                ),
                "replace_terminal_continuation_ask_decision_count": int(
                    summary.replace_terminal_continuation_ask_decision_count
                ),
                "replace_terminal_continuation_decision_latency_sum_ms": int(
                    summary.replace_terminal_continuation_decision_latency_sum_ms
                ),
                "replace_terminal_continuation_decision_latency_max_ms": int(
                    summary.replace_terminal_continuation_decision_latency_max_ms
                ),
            }
            if replace_terminal_continuation
            else {}
        ),
        "replace_cancel_first_exposure_increasing": bool(
            params.get("replace_cancel_first_exposure_increasing", False)
        ),
        "replace_cancel_first_exposure_increasing_replayed": True,
        "sync_adjust_degrade_config_enabled": bool(
            params.get("sync_adjust_degrade_enabled", False)
        ),
        "sync_adjust_degrade_replayed": bool(
            sync_degrade_events.mode != "disabled"
        ),
        "sync_adjust_replay_mode": str(sync_degrade_events.mode),
        "sync_adjust_semantics": SYNC_DEGRADE_SEMANTICS,
        "sync_adjust_event_tape_path": str(
            sync_degrade_events.artifact_path
        ),
        "sync_adjust_event_tape_sha256": str(
            sync_degrade_events.artifact_sha256
        ),
        "sync_adjust_event_environment": str(
            sync_degrade_events.environment
        ),
        "sync_adjust_event_coverage_start_ts_ms": int(
            sync_degrade_events.coverage_start_ts_ms
        ),
        "sync_adjust_event_coverage_end_ts_ms": int(
            sync_degrade_events.coverage_end_ts_ms
        ),
        "sync_adjust_event_count": int(
            sync_degrade_events.timestamps_ms.size
        ),
        "sync_adjust_degrade_trigger_count": int(
            summary.sync_adjust_degrade_trigger_count
        ),
        "sync_adjust_degrade_block_bid_count": int(
            summary.sync_adjust_degrade_block_bid_count
        ),
        "sync_adjust_degrade_block_ask_count": int(
            summary.sync_adjust_degrade_block_ask_count
        ),
        "sync_adjust_degrade_until_ms": int(
            summary.sync_adjust_degrade_until_ms
        ),
        "sync_adjust_censored": bool(summary.sync_adjust_censored),
        "sync_adjust_censor_ts_ms": int(summary.sync_adjust_censor_ts_ms),
        "sync_adjust_promotion_eligible": bool(
            sync_degrade_events.promotion_eligible
        ),
        "consecutive_loss_cooldown_semantics": LOSS_COOLDOWN_SEMANTICS,
        "consecutive_loss_cooldown_state": {
            "schema_version": str(summary.consecutive_loss_snapshot_schema),
            "semantics": LOSS_COOLDOWN_SEMANTICS,
            "max_consecutive_losses": int(cpp_params.max_consecutive_losses),
            "cooldown_ms": int(
                round(float(cpp_params.cooldown_after_loss_s) * 1_000.0)
            ),
            "inventory": float(summary.consecutive_loss_inventory_end),
            "avg_entry": float(summary.consecutive_loss_avg_entry_end),
            "open_commission": float(
                summary.consecutive_loss_open_commission_end
            ),
            "round_trip_pnl": float(
                summary.consecutive_loss_round_trip_pnl_end
            ),
            "consecutive_losses": int(summary.consecutive_loss_count_end),
            "cooldown_until_ms": int(
                summary.consecutive_loss_cooldown_until_ms
            ),
            "last_cancel_ts_ms": int(
                summary.consecutive_loss_last_cancel_ts_end
            ),
            "threshold_pending": bool(
                summary.consecutive_loss_threshold_pending_end
            ),
            "trigger_count": int(
                summary.consecutive_loss_cooldown_trigger_count
            ),
            "expiry_count": int(
                summary.consecutive_loss_cooldown_expiry_count
            ),
            "losing_round_trips": int(
                summary.consecutive_loss_round_trip_loss_count
            ),
            "winning_or_flat_round_trips": int(
                summary.consecutive_loss_round_trip_nonloss_count
            ),
            "max_observed_consecutive_losses": int(
                summary.consecutive_loss_count_max
            ),
        },
        "max_consecutive_losses": int(cpp_params.max_consecutive_losses),
        "cooldown_after_loss_s": float(cpp_params.cooldown_after_loss_s),
        "consecutive_loss_cooldown_trigger_count": int(
            summary.consecutive_loss_cooldown_trigger_count
        ),
        "consecutive_loss_cooldown_expiry_count": int(
            summary.consecutive_loss_cooldown_expiry_count
        ),
        "consecutive_loss_cooldown_block_count": int(
            summary.consecutive_loss_cooldown_block_count
        ),
        "consecutive_loss_cooldown_cancel_count": int(
            summary.consecutive_loss_cooldown_cancel_count
        ),
        "consecutive_loss_round_trip_loss_count": int(
            summary.consecutive_loss_round_trip_loss_count
        ),
        "consecutive_loss_round_trip_nonloss_count": int(
            summary.consecutive_loss_round_trip_nonloss_count
        ),
        "consecutive_loss_count_end": int(summary.consecutive_loss_count_end),
        "consecutive_loss_count_max": int(summary.consecutive_loss_count_max),
        "consecutive_loss_cooldown_until_ms": int(
            summary.consecutive_loss_cooldown_until_ms
        ),
        "user_stream_mismatch_replayed": False,
        "exchange_order_ack_replayed": bool(
            cpp_params.new_order_exchange_effective_latency_samples_ms
            or cpp_params.cancel_exchange_effective_latency_samples_ms
            or cpp_params.cancel_ack_visibility_latency_samples_ms
            or decision_to_gateway_latency_enabled
        ),
        "multi_market_network_arrival_replayed": False,
        "replay_semantics_note": (
            "C++ replay consumes frozen sync-degrade events and the path-dependent "
            "loss cooldown. It still does not synthesize unrecorded user-stream/REST "
            "mismatches or multi-market network arrival state."
        ),
        "book_source": book_source,
        "book_observation_clock_status": {"bbo": book_observation_status(bbo_data), "l2": book_observation_status(l2_data)},
        "max_sprd_bps": cap_bps,
        "cap_mode": cap_mode,
        "cap_base_bps": cap_bps,
        "cap_alpha": dynamic_cap_alpha,
        "cap_max_mult": dynamic_cap_max_mult,
        "cap_var_baseline": dynamic_cap_var_baseline,
        "cap_liq_beta": dynamic_cap_liq_beta,
        "cap_liq_baseline": dynamic_cap_liq_baseline,
        "cap_min_mult": dynamic_cap_min_mult,
        "depth_liquidity_ratio_floor": float(params.get("depth_liquidity_ratio_floor", 0.3)),
        "initial_inventory": cpp_params.initial_inventory,
        "initial_entry_price": cpp_params.initial_entry_price,
        "planned_quote_stop_ts_ms": int(
            getattr(cpp_params, "planned_quote_stop_ts_ms", 0)
        ),
        "planned_quote_stop_triggered": bool(
            getattr(summary, "planned_quote_stop_triggered", False)
        ),
        "planned_quote_stop_trigger_ts_ms": int(
            getattr(summary, "planned_quote_stop_trigger_ts_ms", 0)
        ),
        "planned_shutdown_orders_at_trigger": int(
            getattr(summary, "planned_shutdown_orders_at_trigger", 0)
        ),
        "planned_shutdown_open_order_count": int(
            getattr(summary, "planned_shutdown_open_order_count", 0)
        ),
        "planned_shutdown_pending_new_order_count": int(
            getattr(summary, "planned_shutdown_pending_new_order_count", 0)
        ),
        "planned_shutdown_pending_cancel_order_count": int(
            getattr(summary, "planned_shutdown_pending_cancel_order_count", 0)
        ),
        "cap_label": cap_label,
        "dynamic_cap_enabled": dynamic_cap_enabled,
        "eta": float(params.get("eta", 0.0)),
        "kappa_ratio": float(params.get("kappa_ratio", 0.3)),
        "vol_blend": float(params.get("vol_blend", 0.0)),
        "skew": float(params.get("skew_strength", 0.0)),
        "asym": float(params.get("asym_strength", 0.0)),
        "gdir": float(params.get("inventory_direction_alignment_strength", 0.0)),
        "ret_skew": float(params.get("ret_skew", 0.0)),
        "inventory_asym_strength": float(params.get("inventory_asym_strength", 0.0)),
        "inventory_signal_fade_strength": float(params.get("inventory_signal_fade_strength", 0.0)),
        "book_imb_strength": float(params.get("book_imb_strength", 0.0)),
        "book_imb_source": str(params.get("book_imb_source", "depth_top")),
        "book_imb_levels": int(params.get("book_imb_levels", 20)),
        "vol_power": float(params.get("vol_power", 1.5)),
        "rq_thresh_bps": float(params.get("requote_threshold_bps", 0.0)),
        "use_bar_pricing": use_bar_pricing,
        "adverse_guard_enabled": bool(params.get("adverse_guard_enabled", False)),
        "adverse_toxicity_threshold": float(params.get("adverse_toxicity_threshold", 0.70)),
        "adverse_markout_threshold": abs(float(params.get("adverse_markout_threshold", 5.0))),
        "adverse_markout_pause_threshold": abs(float(params.get("adverse_markout_pause_threshold", 0.0))),
        "adverse_markout_pause_hybrid": bool(params.get("adverse_markout_pause_hybrid", False)),
        "adverse_markout_pause_base_s": max(0.0, float(params.get("adverse_markout_pause_base_s", 120.0))),
        "adverse_markout_pause_min_s": max(0.0, float(params.get("adverse_markout_pause_min_s", 120.0))),
        "adverse_markout_pause_max_s": max(0.0, float(params.get("adverse_markout_pause_max_s", 900.0))),
        "adverse_markout_decay_tau_s": max(0.0, float(params.get("adverse_markout_decay_tau_s", 0.0))),
        "markout_horizon_s": max(1e-6, float(params.get("markout_horizon_s", 10.0))),
        "adverse_markout_max_resolve_gap_s": max(
            max(1e-6, float(params.get("markout_horizon_s", 10.0))),
            float(params.get("adverse_markout_max_resolve_gap_s", 30.0)),
        ),
        "adverse_dir_threshold": abs(float(params.get("adverse_dir_threshold", 0.0))),
        "adverse_ret_bps_threshold": abs(float(params.get("adverse_ret_bps_threshold", 0.0))),
        "adverse_weighted_mid_proxy_shift_bps": abs(float(params.get("adverse_weighted_mid_proxy_shift_bps", 0.0))),
        "adverse_spread_mult": max(1.0, float(params.get("adverse_spread_mult", 1.10))),
        "thin_depth_threshold": max(0.0, float(params.get("thin_depth_threshold", 0.0))),
        "adverse_thin_depth_threshold": max(0.0, float(params.get("adverse_thin_depth_threshold", 0.0))),
        "adverse_thin_depth_mult": max(1.0, float(params.get("adverse_thin_depth_mult", 1.0))),
        "adverse_pause": bool(params.get("adverse_pause", True)),
        "local_extreme_guard_enabled": bool(params.get("local_extreme_guard_enabled", False)),
        "defense_guard_enabled": bool(params.get("defense_guard_enabled", False)),
        "defense_markout_threshold": abs(float(params.get("defense_markout_threshold", 2.0))),
        "defense_dir_threshold": abs(float(params.get("defense_dir_threshold", 0.05))),
        "defense_ret_bps_threshold": abs(float(params.get("defense_ret_bps_threshold", 0.0))),
        "defense_weighted_mid_proxy_shift_bps": abs(float(params.get("defense_weighted_mid_proxy_shift_bps", 0.0))),
        "defense_spread_mult": max(1.0, float(params.get("defense_spread_mult", 1.35))),
        "defense_pause": bool(params.get("defense_pause", True)),
        "defense_emergency_inventory_ratio": max(0.0, float(params.get("defense_emergency_inventory_ratio", 0.50))),
        "defense_emergency_loss": max(0.0, float(params.get("defense_emergency_loss", 5.0))),
        "flat_unilateral_max_s": max(0.0, float(params.get("flat_unilateral_max_s", 0.0))),
        "flat_unilateral_release_count": int(getattr(summary, "flat_unilateral_release_count", 0)),
        "flat_unilateral_bid_release_count": int(getattr(summary, "flat_unilateral_bid_release_count", 0)),
        "flat_unilateral_ask_release_count": int(getattr(summary, "flat_unilateral_ask_release_count", 0)),
        "selection_score": 0.0,
        "pnl": final_pnl,
        "cash_before_terminal": cash_before_terminal,
        "terminal_mark_price": terminal_mark_price,
        "terminal_mtm_pnl": mtm_before_terminal_fee,
        "mtm_before_terminal_fee": mtm_before_terminal_fee,
        "terminal_fee_drag": terminal_fee_drag,
        "terminal_liquidation_fee_estimate": terminal_liquidation_fee_estimate,
        "terminal_liquidation_applied": False,
        "maker_fee_rate": float(params.get("maker_fee", 0.0) or 0.0),
        "inventory_pnl": inventory_pnl,
        "inventory_cost": -inventory_pnl,
        "inventory_adjusted_pnl": inventory_adjusted_pnl,
        "signed_inventory_time_s": signed_inventory_time_s,
        "abs_inventory_time_s": abs_inventory_time_s,
        "sq_inventory_time_s": sq_inventory_time_s,
        "signed_notional_inventory_time_s": signed_notional_inventory_time_s,
        "notional_inventory_time_s": notional_inventory_time_s,
        "time_avg_abs_inventory": abs_inventory_time_s / max(elapsed_s, 1e-9),
        "time_avg_signed_inventory": signed_inventory_time_s / max(elapsed_s, 1e-9),
        "time_avg_sq_inventory": sq_inventory_time_s / max(elapsed_s, 1e-9),
        "time_avg_notional_inventory": notional_inventory_time_s / max(elapsed_s, 1e-9),
        "pnl_per_abs_inventory_hour": final_pnl / inv_time_hours if inv_time_hours > 1e-12 else 0.0,
        "inv_adj_pnl_per_abs_inventory_hour": (
            inventory_adjusted_pnl / inv_time_hours if inv_time_hours > 1e-12 else 0.0
        ),
        "pnl_per_day": final_pnl / max(n_days, 0.01),
        "sharpe": sharpe,
        "max_drawdown": max_dd,
        "avg_markout": float(summary.avg_markout),
        "avg_markout_bid": float(summary.avg_markout_bid),
        "avg_markout_ask": float(summary.avg_markout_ask),
        "markout_count": int(summary.markout_count),
        "markout_qty_btc": float(getattr(summary, "markout_qty_btc", 0.0)),
        "markout_qty_bid_btc": float(getattr(summary, "markout_qty_bid_btc", 0.0)),
        "markout_qty_ask_btc": float(getattr(summary, "markout_qty_ask_btc", 0.0)),
        "fills_bid": int(summary.fills_bid),
        "fills_ask": int(summary.fills_ask),
        "fills_total": fills_total,
        "integer_tick_crossing_recovered_bid_candidates": int(
            getattr(summary, "integer_tick_crossing_recovered_bid_candidates", 0)
        ),
        "integer_tick_crossing_recovered_ask_candidates": int(
            getattr(summary, "integer_tick_crossing_recovered_ask_candidates", 0)
        ),
        "integer_tick_crossing_recovered_bid_fills": int(
            getattr(summary, "integer_tick_crossing_recovered_bid_fills", 0)
        ),
        "integer_tick_crossing_recovered_ask_fills": int(
            getattr(summary, "integer_tick_crossing_recovered_ask_fills", 0)
        ),
        "fills_bid_final_compressed": int(getattr(summary, "fills_bid_final_compressed", 0)),
        "fills_ask_final_compressed": int(getattr(summary, "fills_ask_final_compressed", 0)),
        "fills_final_compressed": (
            int(getattr(summary, "fills_bid_final_compressed", 0))
            + int(getattr(summary, "fills_ask_final_compressed", 0))
        ),
        "fills_bid_not_final_compressed": int(getattr(summary, "fills_bid_not_final_compressed", 0)),
        "fills_ask_not_final_compressed": int(getattr(summary, "fills_ask_not_final_compressed", 0)),
        "fills_not_final_compressed": (
            int(getattr(summary, "fills_bid_not_final_compressed", 0))
            + int(getattr(summary, "fills_ask_not_final_compressed", 0))
        ),
        "avg_markout_final_compressed": float(getattr(summary, "avg_markout_final_compressed", 0.0)),
        "avg_markout_not_final_compressed": float(getattr(summary, "avg_markout_not_final_compressed", 0.0)),
        "markout_qty_final_compressed_btc": float(
            getattr(summary, "markout_qty_final_compressed_btc", 0.0)
        ),
        "markout_qty_not_final_compressed_btc": float(
            getattr(summary, "markout_qty_not_final_compressed_btc", 0.0)
        ),
        "buy_fill_qty": cpp_buy_fill_qty,
        "sell_fill_qty": cpp_sell_fill_qty,
        "buy_fill_notional": cpp_buy_fill_notional,
        "sell_fill_notional": cpp_sell_fill_notional,
        "buy_avg_fill_price": cpp_buy_fill_notional / cpp_buy_fill_qty if cpp_buy_fill_qty > 1e-12 else 0.0,
        "sell_avg_fill_price": cpp_sell_fill_notional / cpp_sell_fill_qty if cpp_sell_fill_qty > 1e-12 else 0.0,
        "fills_per_day": fills_total / max(n_days, 0.01),
        "fill_rate": fills_total / max(n_requotes, 1),
        "quote_attempts": n_requotes + int(summary.stale_book_skip_count),
        "quote_attempts_per_day": (n_requotes + int(summary.stale_book_skip_count)) / max(n_days, 0.01),
        "requotes_per_day": n_requotes / max(n_days, 0.01),
        "avg_spread": float(summary.avg_spread),
        "avg_final_spread": float(summary.avg_final_spread),
        "n_final_spread": n_final_spread,
        "quote_spread_lt_100_rate": int(summary.quote_spread_lt_100_count) / max(n_requotes, 1),
        "quote_spread_lt_150_rate": int(summary.quote_spread_lt_150_count) / max(n_requotes, 1),
        "final_spread_lt_100_rate": int(summary.final_spread_lt_100_count) / max(n_final_spread, 1),
        "final_spread_lt_150_rate": int(summary.final_spread_lt_150_count) / max(n_final_spread, 1),
        "post_only_guard_hits": int(summary.post_only_guard_hits),
        "adverse_guard_count": int(summary.adverse_guard_count),
        "adverse_pause_count": int(summary.adverse_pause_count),
        "adverse_guard_rate": int(summary.adverse_guard_count) / max(n_requotes * 2, 1),
        "adverse_pause_rate": int(summary.adverse_pause_count) / max(n_requotes * 2, 1),
        "adverse_markout_stale_drop_count": int(getattr(summary, "adverse_markout_stale_drop_count", 0)),
        "bid_adverse_markout_pause_extend_count": int(getattr(summary, "bid_adverse_markout_pause_extend_count", 0)),
        "ask_adverse_markout_pause_extend_count": int(getattr(summary, "ask_adverse_markout_pause_extend_count", 0)),
        "defense_guard_count": int(summary.defense_guard_count),
        "defense_pause_count": int(summary.defense_pause_count),
        "defense_guard_rate": int(summary.defense_guard_count) / max(n_requotes * 2, 1),
        "defense_pause_rate": int(summary.defense_pause_count) / max(n_requotes * 2, 1),
        "local_extreme_guard_count": int(getattr(summary, "local_extreme_guard_count", 0)),
        "local_extreme_pause_count": int(getattr(summary, "local_extreme_pause_count", 0)),
        "local_extreme_guard_rate": int(getattr(summary, "local_extreme_guard_count", 0)) / max(n_requotes * 2, 1),
        "bid_local_extreme_guard_count": int(getattr(summary, "bid_local_extreme_guard_count", 0)),
        "ask_local_extreme_guard_count": int(getattr(summary, "ask_local_extreme_guard_count", 0)),
        "fills_bid_local_extreme_guard": int(getattr(summary, "fills_bid_local_extreme_guard", 0)),
        "fills_ask_local_extreme_guard": int(getattr(summary, "fills_ask_local_extreme_guard", 0)),
        "fragile_ttl_cancel_count": int(getattr(summary, "fragile_ttl_cancel_count", 0)),
        "stale_book_skip_count": int(summary.stale_book_skip_count),
        "stale_book_skip_rate": int(summary.stale_book_skip_count) / max(n_requotes + int(summary.stale_book_skip_count), 1),
        "ber_active_count": int(summary.ber_active_count),
        "ber_active_rate": int(summary.ber_active_count) / max(n_requotes, 1),
        "ber_feature_publish_count": int(
            getattr(summary, "ber_feature_publish_count", 0)
        ),
        "ber_held_input_end": float(
            getattr(summary, "ber_held_input_end", 0.0)
        ),
        "ber_ema_fast_end": float(getattr(summary, "ber_ema_fast_end", 0.0)),
        "ber_ema_slow_end": float(getattr(summary, "ber_ema_slow_end", 0.0)),
        "ber_active_end": bool(getattr(summary, "ber_active_end", False)),
        "ber_role_safe_decision_count": int(
            getattr(summary, "ber_role_safe_decision_count", 0)
        ),
        "ber_role_safe_buy_add_count": int(
            getattr(summary, "ber_role_safe_buy_add_count", 0)
        ),
        "ber_role_safe_sell_add_count": int(
            getattr(summary, "ber_role_safe_sell_add_count", 0)
        ),
        "ber_role_safe_flat_bypass_count": int(
            getattr(summary, "ber_role_safe_flat_bypass_count", 0)
        ),
        "ber_role_safe_mixed_fail_closed_count": int(
            getattr(summary, "ber_role_safe_mixed_fail_closed_count", 0)
        ),
        "ber_role_safe_pair_change_count": int(
            getattr(summary, "ber_role_safe_pair_change_count", 0)
        ),
        "ber_role_safe_bid_change_count": int(
            getattr(summary, "ber_role_safe_bid_change_count", 0)
        ),
        "ber_role_safe_ask_change_count": int(
            getattr(summary, "ber_role_safe_ask_change_count", 0)
        ),
        "ber_role_safe_source_mismatch_count": int(
            getattr(summary, "ber_role_safe_source_mismatch_count", 0)
        ),
        "ber_role_safe_cap_collision_count": int(
            getattr(summary, "ber_role_safe_cap_collision_count", 0)
        ),
        "ber_role_safe_cap_infeasible_count": int(
            getattr(summary, "ber_role_safe_cap_infeasible_count", 0)
        ),
        "fill_cooldown_bid_block_count": int(summary.fill_cooldown_bid_block_count),
        "fill_cooldown_ask_block_count": int(summary.fill_cooldown_ask_block_count),
        "fill_cooldown_block_count": int(summary.fill_cooldown_bid_block_count)
        + int(summary.fill_cooldown_ask_block_count),
        "fill_cooldown_consecutive_reset_policy": (
            cpp_params.fill_cooldown_consecutive_reset_policy
        ),
        "fill_cooldown_reset_consec_on_expiry": cpp_params.fill_cooldown_reset_consec_on_expiry,
        "fill_cooldown_apply_reducing": cpp_params.fill_cooldown_apply_reducing,
        "fill_cooldown_reducing": cpp_params.fill_cooldown_reducing_s,
        "fill_cooldown_reducing_inventory_lifecycle_only": cpp_params.fill_cooldown_reducing_inventory_lifecycle_only,
        "fill_cooldown_reducing_inv_threshold": cpp_params.fill_cooldown_reducing_inv_threshold,
        "fill_cooldown_reducing_inv_ratio": cpp_params.fill_cooldown_reducing_inv_ratio,
        "fill_cooldown_reducing_age_s": cpp_params.fill_cooldown_reducing_age_s,
        "fill_cooldown_reducing_vol_ref": cpp_params.fill_cooldown_reducing_vol_ref,
        "fill_cooldown_reducing_vol_min_mult": cpp_params.fill_cooldown_reducing_vol_min_mult,
        "fill_cooldown_reducing_vol_max_mult": cpp_params.fill_cooldown_reducing_vol_max_mult,
        "adaptive_add_cooldown_enabled": cpp_params.adaptive_add_cooldown_enabled,
        "adaptive_add_cooldown_hit_count": int(getattr(summary, "adaptive_add_cooldown_hit_count", 0)),
        "adaptive_add_cooldown_bid_hit_count": int(
            getattr(summary, "adaptive_add_cooldown_bid_hit_count", 0)
        ),
        "adaptive_add_cooldown_ask_hit_count": int(
            getattr(summary, "adaptive_add_cooldown_ask_hit_count", 0)
        ),
        "inventory_lifecycle_stop_add_enabled": cpp_params.inventory_lifecycle_stop_add_enabled,
        "inventory_lifecycle_stop_add_inv_threshold": cpp_params.inventory_lifecycle_stop_add_inv_threshold,
        "inventory_lifecycle_stop_add_age_s": cpp_params.inventory_lifecycle_stop_add_age_s,
        "inventory_lifecycle_stop_add_count": int(getattr(summary, "inventory_lifecycle_stop_add_count", 0)),
        "bid_inventory_lifecycle_stop_add_count": int(getattr(summary, "bid_inventory_lifecycle_stop_add_count", 0)),
        "ask_inventory_lifecycle_stop_add_count": int(getattr(summary, "ask_inventory_lifecycle_stop_add_count", 0)),
        "inventory_lifecycle_soft_control_enabled": cpp_params.inventory_lifecycle_soft_control_enabled,
        "inventory_lifecycle_soft_inv_threshold": cpp_params.inventory_lifecycle_soft_inv_threshold,
        "inventory_lifecycle_soft_age_s": cpp_params.inventory_lifecycle_soft_age_s,
        "inventory_lifecycle_soft_spread_mult": cpp_params.inventory_lifecycle_soft_spread_mult,
        "inventory_lifecycle_soft_gate_enabled": cpp_params.inventory_lifecycle_soft_gate_enabled,
        "inventory_lifecycle_soft_control_count": int(getattr(summary, "inventory_lifecycle_soft_control_count", 0)),
        "bid_inventory_lifecycle_soft_control_count": int(getattr(summary, "bid_inventory_lifecycle_soft_control_count", 0)),
        "ask_inventory_lifecycle_soft_control_count": int(getattr(summary, "ask_inventory_lifecycle_soft_control_count", 0)),
        "fills_while_pending_cancel": int(summary.pending_cancel_fills),
        "replace_throttle_count": int(getattr(summary, "replace_throttle_count", 0)),
        "replace_throttle_bid_count": int(getattr(summary, "replace_throttle_bid_count", 0)),
        "replace_throttle_ask_count": int(getattr(summary, "replace_throttle_ask_count", 0)),
        "replace_throttle_price_count": int(getattr(summary, "replace_throttle_price_count", 0)),
        "replace_throttle_age_count": int(getattr(summary, "replace_throttle_age_count", 0)),
        "decision_place_count": int(getattr(summary, "decision_place_count", 0)),
        "decision_replace_count": int(getattr(summary, "decision_replace_count", 0)),
        "decision_keep_count": int(getattr(summary, "decision_keep_count", 0)),
        "decision_pause_count": int(getattr(summary, "decision_pause_count", 0)),
        "decision_none_count": int(getattr(summary, "decision_none_count", 0)),
        "decision_pending_coalesce_count": int(getattr(summary, "decision_pending_coalesce_count", 0)),
        "decision_cancel_first_count": int(getattr(summary, "decision_cancel_first_count", 0)),
        "max_pending_new_orders": int(getattr(summary, "max_pending_new_orders", 0)),
        "max_pending_cancel_orders": int(getattr(summary, "max_pending_cancel_orders", 0)),
        "buy_soft_widen_release_probe_enabled": bool(
            cpp_params.buy_soft_widen_release_probe_enabled
        ),
        "buy_soft_widen_release_probe_apply_candidate": bool(
            cpp_params.buy_soft_widen_release_probe_apply_candidate
        ),
        "buy_soft_widen_release_target_ts_ms": int(
            cpp_params.buy_soft_widen_release_target_ts_ms
        ),
        "buy_soft_widen_release_target_role": str(
            cpp_params.buy_soft_widen_release_target_role
        ),
        "buy_soft_widen_release_target_reached_count": int(
            getattr(summary, "buy_soft_widen_release_target_reached_count", 0)
        ),
        "buy_soft_widen_release_eligible_count": int(
            getattr(summary, "buy_soft_widen_release_eligible_count", 0)
        ),
        "buy_soft_widen_release_requested_count": int(
            getattr(summary, "buy_soft_widen_release_requested_count", 0)
        ),
        "buy_soft_widen_release_effective_mult_count": int(
            getattr(summary, "buy_soft_widen_release_effective_mult_count", 0)
        ),
        "buy_soft_widen_release_effective_price_count": int(
            getattr(summary, "buy_soft_widen_release_effective_price_count", 0)
        ),
        "buy_soft_widen_release_role_observed": str(
            getattr(summary, "buy_soft_widen_release_role_observed", "")
        ),
        "buy_soft_widen_release_reason": str(
            getattr(summary, "buy_soft_widen_release_reason", "not_reached")
        ),
        "buy_soft_widen_release_baseline_spread_mult": float(
            getattr(summary, "buy_soft_widen_release_baseline_spread_mult", 0.0)
        ),
        "buy_soft_widen_release_selected_spread_mult": float(
            getattr(summary, "buy_soft_widen_release_selected_spread_mult", 0.0)
        ),
        "buy_soft_widen_release_baseline_bid_price": float(
            getattr(summary, "buy_soft_widen_release_baseline_bid_price", 0.0)
        ),
        "buy_soft_widen_release_candidate_bid_price": float(
            getattr(summary, "buy_soft_widen_release_candidate_bid_price", 0.0)
        ),
        "buy_fill_selection_live_enabled": bool(params.get("buy_fill_selection_live_enabled", False)),
        "buy_fill_selection_live_replayed": bool(params.get("buy_fill_selection_live_enabled", False)),
        "buy_fill_selection_live_score_threshold": float(
            params.get("buy_fill_selection_live_score_threshold", 0.50) or 0.50
        ),
        "buy_fill_selection_live_spread_mult_cap": max(
            1.0,
            float(params.get("buy_fill_selection_live_spread_mult_cap", 1.0) or 1.0),
        ),
        "buy_fill_selection_live_apply_reducing": bool(
            params.get("buy_fill_selection_live_apply_reducing", False)
        ),
        "buy_fill_selection_live_eval_count": int(getattr(summary, "buy_fill_selection_live_eval_count", 0)),
        "buy_fill_selection_live_hit_count": int(getattr(summary, "buy_fill_selection_live_hit_count", 0)),
        "buy_fill_selection_live_hit_rate": (
            int(getattr(summary, "buy_fill_selection_live_hit_count", 0))
            / max(int(getattr(summary, "buy_fill_selection_live_eval_count", 0)), 1)
        ),
        "buy_fill_selection_live_score_mean": (
            float(getattr(summary, "buy_fill_selection_live_score_sum", 0.0))
            / max(int(getattr(summary, "buy_fill_selection_live_eval_count", 0)), 1)
        ),
        "buy_fill_selection_live_score_max": float(getattr(summary, "buy_fill_selection_live_score_max", 0.0)),
        "buy_fill_selection_live_score_ge_042": int(getattr(summary, "buy_fill_selection_live_score_ge_042", 0)),
        "buy_fill_selection_live_score_ge_043": int(getattr(summary, "buy_fill_selection_live_score_ge_043", 0)),
        "buy_fill_selection_live_score_ge_044": int(getattr(summary, "buy_fill_selection_live_score_ge_044", 0)),
        "buy_fill_selection_live_score_ge_045": int(getattr(summary, "buy_fill_selection_live_score_ge_045", 0)),
        "conditional_p3_reach_gate_enabled": p3_reach_gate_enabled,
        "conditional_p3_reach_gate_outward_ticks": int(
            cpp_params.conditional_p3_reach_gate_outward_ticks
        ),
        "conditional_p3_reach_gate_grid_min_ticks": int(
            cpp_params.conditional_p3_reach_gate_grid_min_ticks
        ),
        "conditional_p3_reach_gate_eval_count": int(
            getattr(summary, "p3_reach_gate_eval_count", 0)
        ),
        "conditional_p3_reach_gate_toxicity_trigger_count": int(
            getattr(summary, "p3_reach_gate_toxicity_trigger_count", 0)
        ),
        "conditional_p3_reach_gate_supported_count": int(
            getattr(summary, "p3_reach_gate_supported_count", 0)
        ),
        "conditional_p3_reach_gate_pass_count": int(
            getattr(summary, "p3_reach_gate_pass_count", 0)
        ),
        "conditional_p3_reach_gate_price_change_count": int(
            getattr(summary, "p3_reach_gate_price_change_count", 0)
        ),
        "conditional_p3_reach_gate_spread_cap_noop_count": int(
            getattr(summary, "p3_reach_gate_spread_cap_noop_count", 0)
        ),
        "conditional_p3_reach_gate_buy_price_change_count": int(
            getattr(summary, "p3_reach_gate_buy_price_change_count", 0)
        ),
        "conditional_p3_reach_gate_sell_price_change_count": int(
            getattr(summary, "p3_reach_gate_sell_price_change_count", 0)
        ),
        "conditional_p3_reach_budget_policy_enabled": p3_reach_budget_enabled,
        "conditional_p3_reach_budget_grid_min_ticks": int(
            getattr(cpp_params, "conditional_p3_reach_budget_grid_min_ticks", 5)
        ),
        "conditional_p3_reach_budget_bucket_eval_count": int(
            getattr(summary, "p3_reach_budget_bucket_eval_count", 0)
        ),
        "conditional_p3_reach_budget_toxicity_trigger_count": int(
            getattr(summary, "p3_reach_budget_toxicity_trigger_count", 0)
        ),
        "conditional_p3_reach_budget_activation_count": int(
            getattr(summary, "p3_reach_budget_activation_count", 0)
        ),
        "conditional_p3_reach_budget_buy_activation_count": int(
            getattr(summary, "p3_reach_budget_buy_activation_count", 0)
        ),
        "conditional_p3_reach_budget_sell_activation_count": int(
            getattr(summary, "p3_reach_budget_sell_activation_count", 0)
        ),
        "conditional_p3_reach_budget_no_action_count": int(
            getattr(summary, "p3_reach_budget_no_action_count", 0)
        ),
        "conditional_p3_reach_budget_unsupported_count": int(
            getattr(summary, "p3_reach_budget_unsupported_count", 0)
        ),
        "conditional_p3_reach_budget_reuse_count": int(
            getattr(summary, "p3_reach_budget_reuse_count", 0)
        ),
        "conditional_p3_reach_budget_exposure_decision_count": int(
            getattr(summary, "p3_reach_budget_exposure_decision_count", 0)
        ),
        "conditional_p3_reach_budget_price_change_count": int(
            getattr(summary, "p3_reach_budget_price_change_count", 0)
        ),
        "conditional_p3_reach_budget_buy_price_change_count": int(
            getattr(summary, "p3_reach_budget_buy_price_change_count", 0)
        ),
        "conditional_p3_reach_budget_sell_price_change_count": int(
            getattr(summary, "p3_reach_budget_sell_price_change_count", 0)
        ),
        "conditional_p3_reach_budget_hard_safety_suppressed_count": int(
            getattr(summary, "p3_reach_budget_hard_safety_suppressed_count", 0)
        ),
        "conditional_p3_reach_budget_reducing_unchanged_count": int(
            getattr(summary, "p3_reach_budget_reducing_unchanged_count", 0)
        ),
        "conditional_p3_reach_budget_spread_cap_noop_count": int(
            getattr(summary, "p3_reach_budget_spread_cap_noop_count", 0)
        ),
        "conditional_p3_reach_budget_bucket_expiry_count": int(
            getattr(summary, "p3_reach_budget_bucket_expiry_count", 0)
        ),
        "conditional_p3_reach_budget_flat_reset_count": int(
            getattr(summary, "p3_reach_budget_flat_reset_count", 0)
        ),
        "conditional_p3_reach_budget_selected_k_sum": int(
            getattr(summary, "p3_reach_budget_selected_k_sum", 0)
        ),
        "conditional_p3_reach_budget_selected_k_max": int(
            getattr(summary, "p3_reach_budget_selected_k_max", 0)
        ),
        "conditional_p3_reach_budget_active_end_count": int(
            getattr(summary, "p3_reach_budget_active_end_count", 0)
        ),
        "cross_venue_fair_center_shift_enabled": fair_center_enabled,
        "cross_venue_fair_center_tape_supplied": fair_center_payload_present,
        "cross_venue_fair_center_max_state_age_ms": float(
            getattr(cpp_params, "cross_venue_fair_center_max_state_age_ms", 0.0)
        ),
        "cross_venue_fair_center_eval_count": int(
            getattr(summary, "fair_center_eval_count", 0)
        ),
        "cross_venue_fair_center_valid_count": int(
            getattr(summary, "fair_center_valid_count", 0)
        ),
        "cross_venue_fair_center_invalid_count": int(
            getattr(summary, "fair_center_invalid_count", 0)
        ),
        "cross_venue_fair_center_nonzero_request_count": int(
            getattr(summary, "fair_center_nonzero_request_count", 0)
        ),
        "cross_venue_fair_center_price_change_count": int(
            getattr(summary, "fair_center_price_change_count", 0)
        ),
        "cross_venue_fair_center_gtx_clamp_count": int(
            getattr(summary, "fair_center_gtx_clamp_count", 0)
        ),
        "cross_venue_fair_center_no_pair_support_count": int(
            getattr(summary, "fair_center_no_pair_support_count", 0)
        ),
        "cross_venue_fair_center_effective_shift_ticks_abs_sum": int(
            getattr(summary, "fair_center_effective_shift_ticks_abs_sum", 0)
        ),
        "cross_venue_fair_center_effective_shift_ticks_abs_max": int(
            getattr(summary, "fair_center_effective_shift_ticks_abs_max", 0)
        ),
        "cap_hit_rate": int(summary.cap_hit_count) / max(n_requotes, 1),
        "delta_cap_hit_rate": int(getattr(summary, "delta_cap_hit_count", 0)) / max(n_requotes, 1),
        "final_cap_compress_rate": int(summary.final_cap_compress_count) / max(n_requotes, 1),
        "final_cap_rounding_rate": int(getattr(summary, "final_cap_rounding_count", 0)) / max(n_requotes, 1),
        "final_cap_mid_guard_rate": int(getattr(summary, "final_cap_mid_guard_count", 0)) / max(n_requotes, 1),
        "final_cap_post_only_rate": int(getattr(summary, "final_cap_post_only_count", summary.post_only_guard_hits)) / max(n_requotes, 1),
        "final_cap_delta_rate": int(getattr(summary, "final_cap_delta_count", 0)) / max(n_requotes, 1),
        "spread_cap_mode": spread_cap_mode_name(quote_core_cfg.spread_cap_mode),
        "cap_exposure_block_count": int(getattr(summary, "cap_exposure_block_count", 0)),
        "bid_cap_exposure_block_count": int(getattr(summary, "bid_cap_exposure_block_count", 0)),
        "ask_cap_exposure_block_count": int(getattr(summary, "ask_cap_exposure_block_count", 0)),
        "cap_exposure_block_rate": int(getattr(summary, "cap_exposure_block_count", 0)) / max(n_requotes * 2, 1),
        "max_inventory": float(summary.max_abs_inventory),
        "avg_inventory": abs_inventory_time_s / max(elapsed_s, 1e-9),
        "final_inventory": float(summary.final_inventory),
        "n_requotes": n_requotes,
        "n_timeouts": int(getattr(summary, "position_timeout_count", 0)),
        "circuit_breaker_count": int(
            getattr(summary, "circuit_breaker_count", 0)
        ),
        "circuit_breaker_exit_mode": circuit_breaker_exit_mode,
        "circuit_breaker_closing": bool(
            getattr(summary, "circuit_breaker_closing", False)
        ),
        "circuit_breaker_close_place_count": int(
            getattr(summary, "circuit_breaker_close_place_count", 0)
        ),
        "circuit_breaker_close_keep_count": int(
            getattr(summary, "circuit_breaker_close_keep_count", 0)
        ),
        "circuit_breaker_close_fill_count": int(
            getattr(summary, "circuit_breaker_close_fill_count", 0)
        ),
        "circuit_breaker_close_gtx_reject_count": int(
            getattr(summary, "circuit_breaker_close_gtx_reject_count", 0)
        ),
        "circuit_breaker_close_ioc_place_count": int(
            getattr(summary, "circuit_breaker_close_ioc_place_count", 0)
        ),
        "circuit_breaker_close_ioc_fill_count": int(
            getattr(summary, "circuit_breaker_close_ioc_fill_count", 0)
        ),
        "circuit_breaker_close_ioc_expire_count": int(
            getattr(summary, "circuit_breaker_close_ioc_expire_count", 0)
        ),
        "emergency_close_count": int(getattr(summary, "emergency_close_count", 0)),
        "n_trades": n_execution_trades,
        "n_clock_events": int(trade_ts.size),
        "replay_event_clock": replay_event_clock,
        "replay_clock_interval_ms": replay_clock_interval_ms,
        "n_days": n_days,
        "avg_rq_ms": float(summary.avg_rq_ms),
        "fill_cooldown": float(params.get("fill_cooldown", 0.0)),
        "_pnl_ts": pnl_arr,
        "_inv_ts": inv_arr,
        "_ts": ts_arr,
        "_fill_trace": fill_trace,
        "_fill_diagnostic_counts": diagnostic_counts(fill_trace),
        "_quote_trace": quote_trace,
        "_p3_reach_decision_trace": p3_reach_decision_trace,
        "_cooldown_duration_opportunity_trace": (
            cooldown_duration_opportunity_trace
        ),
        "_cooldown_duration_fill_path": cooldown_duration_fill_path,
        "_cooldown_duration_fork_trace": cooldown_duration_fork_trace,
        "_cooldown_duration_policy_decisions": [
            row
            for row in f05_repeated_cooldown_decisions
            if row["exposure_fill_ordinal"] > 0
        ],
        "_cooldown_duration_policy_audit": f05_repeated_cooldown_audit,
        "cooldown_duration_policy_cpp_qualification_under_test": (
            f05_qualification_under_test
        ),
        "cooldown_duration_policy_cpp_authoritative": (
            f05_cooldown_authoritative
        ),
        "cooldown_duration_policy_cpp_parity_receipt_sha256": str(
            params.get(
                "_cooldown_duration_policy_cpp_validated_receipt_sha256",
                "",
            )
            or ""
        ),
        "cooldown_duration_policy_cpp_event_loop_parity_qualified": bool(
            params.get(
                "_cooldown_duration_policy_cpp_event_loop_receipt_qualified",
                False,
            )
        ),
        "_f05_repeated_cooldown_decisions": (
            f05_repeated_cooldown_decisions
        ),
        "_f05_repeated_cooldown_checkpoint": (
            f05_repeated_cooldown_checkpoint
        ),
    }
    for coefficient_name in ("eta_inventory", "a_spread"):
        if params.get(coefficient_name) is not None:
            result[coefficient_name] = float(
                getattr(quote_core_cfg, coefficient_name)
            )
    if conditional_p3_reproduction_identity is not None:
        result.update(
            _conditional_p3_historical_output_marker(
                conditional_p3_reproduction_identity
            )
        )
    return result


# ═══════════════════════════════════════════════════════════════════
#  3. SWEEP
# ═══════════════════════════════════════════════════════════════════

# Legacy AS-only sweep grid (without ML).  Do not use this as the live
# parameter surface: current live/tick quotes use p3_touch_log_probability_distance_slope when available,
# so raw execution_intensity_slope is only a fallback/legacy AS diagnostic.
SWEEP_GRID = {
    "quote_coefficients": [{"eta_inventory": 0.005, "a_spread": 0.005, "risk_per_order": 0.005}, {"eta_inventory": 0.01, "a_spread": 0.01, "risk_per_order": 0.01}, {"eta_inventory": 0.02, "a_spread": 0.02, "risk_per_order": 0.02}, {"eta_inventory": 0.05, "a_spread": 0.05, "risk_per_order": 0.05}, {"eta_inventory": 0.1, "a_spread": 0.1, "risk_per_order": 0.1}],
    "execution_intensity_slope": [0.02, 0.05, 0.1, 0.5],
    "queue_base": [3.0, 5.0, 10.0],
    "queue_decay": [0.05, 0.1],
}

# ML-enhanced legacy sweep grid. Keep fallback execution_intensity_slope fixed; effective-execution_intensity_slope
# experiments belong in parameter_racing_sweep.py via p3_touch_log_probability_distance_slope/cap arms.
SWEEP_GRID_ML = {
    "quote_coefficients": [{"eta_inventory": 0.005, "a_spread": 0.005, "risk_per_order": 0.005}, {"eta_inventory": 0.01, "a_spread": 0.01, "risk_per_order": 0.01}, {"eta_inventory": 0.02, "a_spread": 0.02, "risk_per_order": 0.02}, {"eta_inventory": 0.05, "a_spread": 0.05, "risk_per_order": 0.05}, {"eta_inventory": 0.1, "a_spread": 0.1, "risk_per_order": 0.1}],
    "execution_intensity_slope": [0.05],
    "queue_base": [5.0],
    "queue_decay": [0.1],
    "vol_blend": [0.0, 0.3, 0.7],
    "skew_strength": [0.0, 0.1, 0.3],
    "asym_strength": [0.0, 0.1, 0.2],
    "inventory_direction_alignment_strength": [0.0, 0.3],
    "ret_skew": [0.0, 100.0, 200.0],
}

# Step 27: Fill cooldown sweep grid
SWEEP_GRID_COOLDOWN = {
    "fill_cooldown": [0.0, 5.0, 10.0, 15.0, 20.0, 30.0, 45.0, 60.0, 90.0, 120.0, 180.0, 300.0, 360.0],
}

_G = {}

SORT_OBJECTIVES = {
    "selection_score": ("selection_score", "Selection"),
    "pnl": ("pnl", "PnL"),
    "avg_markout": ("avg_markout", "AvgMarkout"),
    "sharpe": ("sharpe", "Sharpe"),
    "pnl_per_day": ("pnl_per_day", "$/day"),
}

SORT_TIEBREAKERS = {
    # InvAdj remains a displayed diagnostic, but is intentionally not a sort
    # objective or tie-breaker because it can neutralize adverse-selection PnL.
    "selection_score": ("pnl", "avg_markout"),
    "pnl": ("pnl_per_day", "sharpe"),
    "avg_markout": ("pnl", "sharpe"),
    "sharpe": ("pnl", "avg_markout"),
    "pnl_per_day": ("pnl", "avg_markout"),
}


def _sort_key(result, sort_by):
    primary_field, _ = SORT_OBJECTIVES[sort_by]
    secondary_field, tertiary_field = SORT_TIEBREAKERS[sort_by]
    return (
        result.get(primary_field, float("-inf")),
        result.get(secondary_field, float("-inf")),
        result.get(tertiary_field, float("-inf")),
    )


def _sort_results(results, sort_by):
    return sorted(results, key=lambda r: _sort_key(r, sort_by), reverse=True)


def _best_result(results, sort_by):
    if not results:
        return None
    return _sort_results(results, sort_by)[0]


def _public_default_tick_mechanics_marker():
    """Public replay uses the explicit public config; owner defaults are private."""
    return None

def _simulate_tick_with_engine(engine, trades_df, var_ts_ms, var_ssq, params,
                               ml_data=None, bbo_data=None, l2_data=None,
                               var_ti=None, var_retsq=None, reference_event_tapes=None,
                               inventory_lifecycle_repair_data=None, inventory_lifecycle_repair_model=None,
                               historical_global_flow_data=None,
                               historical_fair_price_data=None,
                               active_order_queue_data=None,
                               exchange_book_event_tape=None,
                               variance_time_data=None,
                               ranked_toxicity_guard_binding=None, *,
                               checkpoint_at_ts_ms=None, resume_checkpoint=None,
                               resume_input_batch=False, consume_resume_checkpoint=False):
    def label_explicit_arm(result):
        selection = params.get("baseline_selection")
        if selection is None:
            return result
        labeled = dict(result)
        labeled["baseline_selection"] = str(selection)
        labeled["current_e3_mechanics_default"] = bool(
            params.get("current_e3_mechanics_default", False)
        )
        labeled["_baseline_selection_receipt"] = {
            "baseline_selection": str(selection),
            "current_e3_mechanics_default": bool(
                params.get("current_e3_mechanics_default", False)
            ),
            "explicit_non_current_arm": True,
        }
        return labeled

    if engine == "cpp":
        if (checkpoint_at_ts_ms is not None or resume_checkpoint is not None
                or resume_input_batch or consume_resume_checkpoint):
            raise NotImplementedError("complete runtime checkpoints currently require Python replay")
        if params.get("cooldown_duration_policy_evaluator") is not None:
            _validate_f05_cpp_cooldown_runtime(
                params,
                require_full_replay=True,
            )
        if ranked_toxicity_guard_binding is not None:
            raise NotImplementedError(
                "ranked-toxicity guard full-path execution is Python-authoritative"
            )
        if bool(
            params.get("multi_short_reducing_buy_aggression_enabled", False)
        ):
            raise NotImplementedError(
                "multi-SHORT reducing-BUY aggression full-path replay is "
                "Python-authoritative"
            )
        if bool(params.get("sell_add_inventory_price_penalty_enabled", False)):
            raise NotImplementedError(
                "SELL-add inventory price-penalty full-path replay is "
                "Python-authoritative"
            )
        if bool(
            params.get(
                "post_cooldown_incremental_inventory_budget_enabled",
                False,
            )
        ):
            raise NotImplementedError(
                "post-cooldown incremental inventory-budget full-path replay "
                "is Python-authoritative; only its isolated accounting kernel "
                "may claim C++ parity"
            )
        if (
            variance_time_data is not None
            or str(
                params.get("fill_cooldown_clock_mode", "wall_time")
                or "wall_time"
            ).strip().lower()
            != "wall_time"
        ):
            raise NotImplementedError(
                "variance-time full counterfactual replay is Python-authoritative "
                "until the candidate path and native q90 scheduler reach C++ parity"
            )
        if active_order_queue_data is not None:
            raise NotImplementedError(
                "strict sparse active-order queue replay is Python-only"
            )
        if int(params.get("trace_local_order_value_max", 0) or 0) > 0:
            raise NotImplementedError(
                "local active-order competing-risk observation requires the "
                "authoritative Python order lifecycle path"
            )
        if bool(params.get("queue_value_keep_cancel_enabled", False)):
            raise NotImplementedError(
                "queue-value keep/cancel randomization requires the "
                "authoritative Python active-order lifecycle path"
            )
        if bool(params.get("local_action_ope_enabled", False)):
            raise NotImplementedError(
                "randomized local quote actions require the authoritative Python "
                "replay inventory_lifecycle and propensity path"
            )
        if bool(params.get("sell_add_skip_ope_enabled", False)):
            raise NotImplementedError(
                "randomized SELL add-skip actions require the authoritative "
                "Python replay competing-risk inventory_lifecycle path"
            )
        if bool(params.get("state_conditioned_rearm_enabled", False)):
            raise NotImplementedError(
                "state-conditioned post-cooldown rearm randomization requires "
                "the authoritative Python inventory_lifecycle and causal-path lifecycle"
            )
        if str(
            params.get("state_conditioned_policy_mode", "disabled") or "disabled"
        ).strip().lower() != "disabled":
            raise NotImplementedError(
                "state-conditioned quote policy is Python replay-only until its "
                "artifact scorer, one-inventory_lifecycle intervention, and trace reach C++ parity"
            )
        if bool(params.get("safe_add_rearm_randomized_enabled", False)):
            raise NotImplementedError(
                "randomized safe-add rearm interventions require the authoritative "
                "Python replay inventory_lifecycle path"
            )
        if int(params.get("trace_safe_add_rearm_max", 0) or 0) > 0:
            raise NotImplementedError(
                "safe-add rearm shadow probes are Python replay-only until a "
                "frozen policy schedule reaches C++ parity"
            )
        if bool(params.get("post_fill_quote_response_enabled", False)):
            raise NotImplementedError(
                "post-fill inventory/flow quote response is Python replay-only "
                "until C++ parity"
            )
        if (
            reference_event_tapes
            or inventory_lifecycle_repair_data is not None
            or inventory_lifecycle_repair_model is not None
            or historical_global_flow_data is not None
            or bool(
            params.get("multi_market_policy_enabled", False)
            )
        ):
            raise NotImplementedError(
                "receive-time multi-market policy replay is Python-only until C++ parity"
            )
        return label_explicit_arm(
            _simulate_tick_cpp(
                trades_df,
                var_ts_ms,
                var_ssq,
                params,
                ml_data=ml_data,
                bbo_data=bbo_data,
                l2_data=l2_data,
                var_ti=var_ti,
                var_retsq=var_retsq,
                historical_fair_price_data=historical_fair_price_data,
                exchange_book_event_tape=exchange_book_event_tape,
            )
        )
    if engine == "python":
        return label_explicit_arm(
            simulate_tick(
                trades_df,
                var_ts_ms,
                var_ssq,
                params,
                ml_data=ml_data,
                bbo_data=bbo_data,
                l2_data=l2_data,
                var_ti=var_ti,
                var_retsq=var_retsq,
                reference_event_tapes=reference_event_tapes,
                inventory_lifecycle_repair_data=inventory_lifecycle_repair_data,
                inventory_lifecycle_repair_model=inventory_lifecycle_repair_model,
                historical_global_flow_data=historical_global_flow_data,
                historical_fair_price_data=historical_fair_price_data,
                active_order_queue_data=active_order_queue_data,
                exchange_book_event_tape=exchange_book_event_tape,
                variance_time_data=variance_time_data,
                ranked_toxicity_guard_binding=ranked_toxicity_guard_binding,
                checkpoint_at_ts_ms=checkpoint_at_ts_ms,
                resume_checkpoint=resume_checkpoint,
                resume_input_batch=resume_input_batch,
                consume_resume_checkpoint=consume_resume_checkpoint,
            )
        )
    raise ValueError(f"Unknown replay engine={engine!r}")


def _quality_calendar_days(trades_df: pd.DataFrame) -> float:
    if trades_df.empty:
        return 0.0
    days = pd.to_datetime(
        trades_df["transact_time"].to_numpy(dtype=np.int64, copy=False),
        unit="ms",
        utc=True,
    ).floor("D")
    return float(pd.Index(days).nunique())


def _full_calendar_days_for_loaded_window(
    trades_df: pd.DataFrame,
    start_ms: Optional[int],
    end_ms: Optional[int],
) -> float:
    if trades_df.empty:
        return 0.0
    ts = trades_df["transact_time"].to_numpy(dtype=np.int64, copy=False)
    if start_ms is not None or end_ms is not None:
        lo = int(start_ms if start_ms is not None else ts[0])
        hi = int(end_ms if end_ms is not None else ts[-1] + 1)
        return max(float(hi - lo) / 86_400_000.0, 1.0 / 86_400.0)

    first_day = pd.to_datetime(int(ts[0]), unit="ms", utc=True).floor("D")
    last_day = pd.to_datetime(int(ts[-1]), unit="ms", utc=True).floor("D")
    return float((last_day - first_day).days + 1)


def _quality_segment_bounds(trades_df: pd.DataFrame, max_gap_s: float) -> list[tuple[int, int]]:
    if trades_df.empty:
        return []
    ts = trades_df["transact_time"].to_numpy(dtype=np.int64, copy=False)
    seg = continuous_segment_ids(ts, max_gap_s=max_gap_s)
    if len(seg) == 0:
        return []
    breaks = np.flatnonzero(seg[1:] != seg[:-1]) + 1
    starts = np.r_[0, breaks]
    ends = np.r_[breaks, len(seg)]
    return [
        (int(s), int(e))
        for s, e in zip(starts, ends, strict=True)
        if int(e) > int(s)
    ]


def _daily_segment_bounds(trades_df: pd.DataFrame) -> list[tuple[str, int, int]]:
    """Return UTC-day replay bounds over the retained trade rows."""
    if trades_df.empty:
        return []
    ts = trades_df["transact_time"].to_numpy(dtype=np.int64, copy=False)
    day_ms = 86_400_000
    day_ids = ts // day_ms
    breaks = np.flatnonzero(day_ids[1:] != day_ids[:-1]) + 1
    starts = np.r_[0, breaks]
    ends = np.r_[breaks, len(ts)]
    out: list[tuple[str, int, int]] = []
    for start, end in zip(starts, ends, strict=True):
        if int(end) <= int(start):
            continue
        label = pd.to_datetime(int(day_ids[int(start)] * day_ms), unit="ms", utc=True).strftime("%Y-%m-%d")
        out.append((label, int(start), int(end)))
    return out


def _weighted_mean(results, field: str, weight_field: str, default: float = 0.0) -> float:
    total_w = 0.0
    total = 0.0
    for r in results:
        w = float(r.get(weight_field, 0.0) or 0.0)
        if w <= 0.0:
            continue
        total += float(r.get(field, default) or 0.0) * w
        total_w += w
    return total / total_w if total_w > 0.0 else default


def _recompute_path_stats(ts_arr: np.ndarray, pnl_arr: np.ndarray) -> tuple[float, float]:
    if len(ts_arr) <= 1 or len(pnl_arr) <= 1:
        return 0.0, 0.0
    pnl_diff = np.diff(pnl_arr)
    dt_s = np.diff(ts_arr).astype(np.float64) / 1000.0
    valid = dt_s > 0.0
    sharpe = 0.0
    if np.any(valid):
        dp = pnl_diff[valid]
        dt = dt_s[valid]
        mu_sec = dp.sum() / max(dt.sum(), 1e-12)
        diff_norm = dp / np.sqrt(dt)
        sigma_sec = diff_norm.std()
        if sigma_sec > 0.0:
            sharpe = mu_sec / sigma_sec * math.sqrt(365.25 * 86400.0)
    cum_max = np.maximum.accumulate(pnl_arr)
    max_dd = float((cum_max - pnl_arr).max()) if len(pnl_arr) else 0.0
    return sharpe, max_dd


def _is_additive_segment_key(key: str) -> bool:
    if key in {
        "pnl",
        "cash_before_terminal",
        "terminal_mtm_pnl",
        "mtm_before_terminal_fee",
        "terminal_fee_drag",
        "terminal_liquidation_fee_estimate",
        "inventory_pnl",
        "inventory_cost",
        "inventory_adjusted_pnl",
        "signed_inventory_time_s",
        "abs_inventory_time_s",
        "sq_inventory_time_s",
        "signed_notional_inventory_time_s",
        "notional_inventory_time_s",
        "markout_count",
        "markout_qty_btc",
        "markout_qty_bid_btc",
        "markout_qty_ask_btc",
        "markout_qty_final_compressed_btc",
        "markout_qty_not_final_compressed_btc",
        "fills_bid",
        "fills_ask",
        "fills_total",
        "buy_fill_qty",
        "sell_fill_qty",
        "buy_fill_notional",
        "sell_fill_notional",
        "quote_attempts",
        "n_final_spread",
        "n_requotes",
        "n_timeouts",
        "n_trades",
        "n_clock_events",
        "fragile_ttl_cancel_count",
        "flat_unilateral_release_count",
        "flat_unilateral_bid_release_count",
        "flat_unilateral_ask_release_count",
        "gtx_rejects",
        "fills_while_pending_cancel",
        "queue_l2_cancel_ahead_qty",
        "inventory_lifecycle_exposure_increasing_fills",
        "inventory_lifecycle_reducing_fills",
        "inventory_lifecycle_buy_fills",
        "inventory_lifecycle_sell_fills",
    }:
        return True
    if key.endswith(("_count", "_hits", "_rejects")):
        return True
    if key.startswith(("fills_bid_", "fills_ask_")):
        return True
    return False


def _aggregate_quality_segment_results(
    results: list[dict],
    *,
    full_calendar_days: float,
    quality_days: float,
    max_gap_s: float,
) -> dict:
    if not results:
        raise ValueError("No segment results to aggregate")
    if len(results) == 1:
        out = dict(results[0])
    else:
        out = {k: v for k, v in results[0].items() if not k.startswith("_")}
        keys = set().union(*(r.keys() for r in results))
        for key in sorted(keys):
            if key.startswith("_") or not _is_additive_segment_key(key):
                continue
            total = 0.0
            seen = False
            for r in results:
                value = r.get(key)
                if isinstance(value, (int, float, np.integer, np.floating)):
                    total += float(value)
                    seen = True
            if seen:
                if (
                    key.startswith("n_")
                    or key.endswith(("_count", "_hits", "_rejects"))
                    or key in {
                        "quote_attempts",
                        "fills_bid",
                        "fills_ask",
                        "fills_total",
                        "inventory_lifecycle_exposure_increasing_fills",
                        "inventory_lifecycle_reducing_fills",
                        "inventory_lifecycle_buy_fills",
                        "inventory_lifecycle_sell_fills",
                    }
                    or key.startswith(("fills_bid_", "fills_ask_"))
                ):
                    out[key] = int(round(total))
                else:
                    out[key] = total

    segment_count = len(results)
    active_days = sum(float(r.get("n_days", 0.0) or 0.0) for r in results)
    elapsed_s = max(active_days * 86_400.0, 1e-9)
    fills = float(out.get("fills_total", 0.0) or 0.0)
    buy_fill_qty = float(out.get("buy_fill_qty", 0.0) or 0.0)
    sell_fill_qty = float(out.get("sell_fill_qty", 0.0) or 0.0)
    buy_fill_notional = float(out.get("buy_fill_notional", 0.0) or 0.0)
    sell_fill_notional = float(out.get("sell_fill_notional", 0.0) or 0.0)
    n_requotes = float(out.get("n_requotes", 0.0) or 0.0)
    quote_attempts = float(
        out.get("quote_attempts", n_requotes + float(out.get("stale_book_skip_count", 0.0) or 0.0))
        or 0.0
    )

    # Weighted averages/rates that cannot be summed directly.
    for field in (
        "avg_spread",
        "quote_spread_lt_100_rate",
        "quote_spread_lt_150_rate",
        "cap_hit_rate",
        "delta_cap_hit_rate",
        "final_cap_compress_rate",
        "final_cap_rounding_rate",
        "final_cap_mid_guard_rate",
        "final_cap_post_only_rate",
        "final_cap_delta_rate",
        "post_policy_cap_rate",
        "mid_guard_bid_rate",
        "mid_guard_ask_rate",
        "post_only_bid_rate",
        "post_only_ask_rate",
        "bid_tighter_rate",
        "ask_tighter_rate",
        "quote_balanced_rate",
    ):
        out[field] = _weighted_mean(results, field, "n_requotes")
    for field in (
        "avg_final_spread",
        "final_spread_lt_100_rate",
        "final_spread_lt_150_rate",
        "avg_delta_after_cap",
        "avg_bid_dist",
        "avg_ask_dist",
        "avg_quote_skew_pct",
        "avg_abs_quote_skew_pct",
        "quote_abs_skew_gt_10_rate",
        "quote_abs_skew_gt_25_rate",
    ):
        out[field] = _weighted_mean(results, field, "n_final_spread")
    out["avg_markout"] = _weighted_mean(results, "avg_markout", "markout_qty_btc")
    out["avg_markout_bid"] = _weighted_mean(results, "avg_markout_bid", "markout_qty_bid_btc")
    out["avg_markout_ask"] = _weighted_mean(results, "avg_markout_ask", "markout_qty_ask_btc")
    out["avg_markout_final_compressed"] = _weighted_mean(
        results, "avg_markout_final_compressed", "markout_qty_final_compressed_btc"
    )
    out["avg_markout_not_final_compressed"] = _weighted_mean(
        results,
        "avg_markout_not_final_compressed",
        "markout_qty_not_final_compressed_btc",
    )

    for field in ("max_inventory", "max_drawdown", "max_pending_new_orders", "max_pending_cancel_orders"):
        values = [float(r.get(field, 0.0) or 0.0) for r in results]
        out[field] = max(values) if values else 0.0
    out["final_inventory"] = float(results[-1].get("final_inventory", 0.0) or 0.0)

    pnl = float(out.get("pnl", 0.0) or 0.0)
    inv_adj = float(out.get("inventory_adjusted_pnl", 0.0) or 0.0)
    abs_inventory_time_s = float(out.get("abs_inventory_time_s", 0.0) or 0.0)
    inv_time_hours = abs_inventory_time_s / 3600.0
    out["n_days"] = active_days
    out["active_trade_day_count"] = active_days
    out["quality_day_count"] = quality_days
    out["full_calendar_day_count"] = full_calendar_days
    out["quality_segment_aware"] = True
    out["quality_segment_count"] = segment_count
    out["quality_segment_max_gap_s"] = float(max_gap_s)
    out["fills_per_active_trade_day"] = fills / max(active_days, 0.01)
    out["fills_per_quality_day"] = fills / max(quality_days, 0.01)
    out["fills_per_full_calendar_day"] = fills / max(full_calendar_days, 0.01)
    out["fills_per_day"] = out["fills_per_active_trade_day"]
    out["buy_avg_fill_price"] = buy_fill_notional / buy_fill_qty if buy_fill_qty > 1e-12 else 0.0
    out["sell_avg_fill_price"] = sell_fill_notional / sell_fill_qty if sell_fill_qty > 1e-12 else 0.0
    out["pnl_per_day"] = pnl / max(active_days, 0.01)
    out["pnl_per_quality_day"] = pnl / max(quality_days, 0.01)
    out["pnl_per_full_calendar_day"] = pnl / max(full_calendar_days, 0.01)
    out["inv_adj_per_active_trade_day"] = inv_adj / max(active_days, 0.01)
    out["inv_adj_per_quality_day"] = inv_adj / max(quality_days, 0.01)
    out["inv_adj_per_full_calendar_day"] = inv_adj / max(full_calendar_days, 0.01)
    out["fill_rate"] = fills / max(n_requotes, 1.0)
    out["quote_attempts_per_day"] = quote_attempts / max(active_days, 0.01)
    out["requotes_per_day"] = n_requotes / max(active_days, 0.01)
    out["quote_attempts_per_quality_day"] = quote_attempts / max(quality_days, 0.01)
    out["requotes_per_quality_day"] = n_requotes / max(quality_days, 0.01)
    out["quote_attempts_per_full_calendar_day"] = quote_attempts / max(full_calendar_days, 0.01)
    out["requotes_per_full_calendar_day"] = n_requotes / max(full_calendar_days, 0.01)
    out["time_avg_abs_inventory"] = abs_inventory_time_s / elapsed_s
    out["time_avg_signed_inventory"] = float(out.get("signed_inventory_time_s", 0.0) or 0.0) / elapsed_s
    out["time_avg_sq_inventory"] = float(out.get("sq_inventory_time_s", 0.0) or 0.0) / elapsed_s
    out["time_avg_notional_inventory"] = float(out.get("notional_inventory_time_s", 0.0) or 0.0) / elapsed_s
    out["avg_inventory"] = out["time_avg_abs_inventory"]
    out["pnl_per_abs_inventory_hour"] = pnl / inv_time_hours if inv_time_hours > 1e-12 else 0.0
    out["inv_adj_pnl_per_abs_inventory_hour"] = inv_adj / inv_time_hours if inv_time_hours > 1e-12 else 0.0

    # Rates from aggregated counts.
    out["adverse_guard_rate"] = float(out.get("adverse_guard_count", 0.0) or 0.0) / max(n_requotes * 2.0, 1.0)
    out["adverse_pause_rate"] = float(out.get("adverse_pause_count", 0.0) or 0.0) / max(n_requotes * 2.0, 1.0)
    out["defense_guard_rate"] = float(out.get("defense_guard_count", 0.0) or 0.0) / max(n_requotes * 2.0, 1.0)
    out["defense_pause_rate"] = float(out.get("defense_pause_count", 0.0) or 0.0) / max(n_requotes * 2.0, 1.0)
    out["local_extreme_guard_rate"] = float(out.get("local_extreme_guard_count", 0.0) or 0.0) / max(n_requotes * 2.0, 1.0)
    out["bid_adverse_guard_rate"] = float(out.get("bid_adverse_guard_count", 0.0) or 0.0) / max(n_requotes, 1.0)
    out["ask_adverse_guard_rate"] = float(out.get("ask_adverse_guard_count", 0.0) or 0.0) / max(n_requotes, 1.0)
    out["bid_defense_guard_rate"] = float(out.get("bid_defense_guard_count", 0.0) or 0.0) / max(n_requotes, 1.0)
    out["ask_defense_guard_rate"] = float(out.get("ask_defense_guard_count", 0.0) or 0.0) / max(n_requotes, 1.0)
    out["stale_book_skip_rate"] = float(out.get("stale_book_skip_count", 0.0) or 0.0) / max(quote_attempts, 1.0)

    # Concatenate diagnostic curves. Segment PnL is offset so drawdown/sharpe
    # reflect the aggregated replay rather than independent zero-based segments.
    ts_parts = []
    pnl_parts = []
    inv_parts = []
    pnl_offset = 0.0
    for r in results:
        ts = np.asarray(r.get("_ts", []), dtype=np.int64)
        pnl_path = np.asarray(r.get("_pnl_ts", []), dtype=np.float64)
        inv_path = np.asarray(r.get("_inv_ts", []), dtype=np.float64)
        if len(ts) and len(pnl_path):
            ts_parts.append(ts)
            pnl_parts.append(pnl_path + pnl_offset)
            inv_parts.append(inv_path if len(inv_path) == len(ts) else np.zeros(len(ts), dtype=np.float64))
        pnl_offset += float(r.get("pnl", 0.0) or 0.0)
    if ts_parts:
        out["_ts"] = np.concatenate(ts_parts)
        out["_pnl_ts"] = np.concatenate(pnl_parts)
        out["_inv_ts"] = np.concatenate(inv_parts)
        out["sharpe"], out["max_drawdown"] = _recompute_path_stats(out["_ts"], out["_pnl_ts"])
    else:
        out["_ts"] = np.empty(0, dtype=np.int64)
        out["_pnl_ts"] = np.empty(0, dtype=np.float64)
        out["_inv_ts"] = np.empty(0, dtype=np.float64)
    out["_fill_trace"] = [row for r in results for row in r.get("_fill_trace", [])]
    out["_quote_trace"] = [row for r in results for row in r.get("_quote_trace", [])]
    out["_variance_time_lineage_trace"] = [
        row
        for r in results
        for row in r.get("_variance_time_lineage_trace", [])
    ]
    out["_variance_time_lineage_event_journal"] = [
        row
        for r in results
        for row in r.get("_variance_time_lineage_event_journal", [])
    ]
    out["_first_add_decision_to_terminal_trace"] = [
        row
        for r in results
        for row in r.get("_first_add_decision_to_terminal_trace", [])
    ]
    first_add_trace_audits = [
        dict(r.get("_first_add_decision_to_terminal_trace_audit") or {})
        for r in results
        if r.get("_first_add_decision_to_terminal_trace_audit")
    ]
    if first_add_trace_audits:
        count_fields = (
            "selected_inventory_lifecycle_count",
            "emitted_row_count",
            "unique_inventory_lifecycle_count",
            "exact_join_count",
            "feature_clock_violation_count",
            "open_record_count",
        )
        out["_first_add_decision_to_terminal_trace_audit"] = {
            "trace_schema_version": "first_add_decision_to_terminal_trace.v1",
            **{
                field: int(
                    sum(int(audit.get(field, 0)) for audit in first_add_trace_audits)
                )
                for field in count_fields
            },
            "coverage_complete": bool(
                all(bool(audit.get("coverage_complete", False)) for audit in first_add_trace_audits)
            ),
        }
    out["_first_opener_decision_to_terminal_trace"] = [
        row
        for r in results
        for row in r.get("_first_opener_decision_to_terminal_trace", [])
    ]
    first_opener_trace_audits = [
        dict(r.get("_first_opener_decision_to_terminal_trace_audit") or {})
        for r in results
        if r.get("_first_opener_decision_to_terminal_trace_audit")
    ]
    if first_opener_trace_audits:
        count_fields = (
            "inventory_lifecycle_count",
            "eligible_true_opener_inventory_lifecycle_count",
            "unsupported_nonopener_open_inventory_lifecycle_count",
            "selected_inventory_lifecycle_count",
            "emitted_row_count",
            "unique_inventory_lifecycle_count",
            "exact_join_count",
            "feature_clock_violation_count",
            "open_record_count",
        )
        out["_first_opener_decision_to_terminal_trace_audit"] = {
            "trace_schema_version": str(
                first_opener_trace_audits[0].get(
                    "trace_schema_version",
                    "first_opener_decision_to_terminal_trace.v1",
                )
            ),
            **{
                field: int(
                    sum(
                        int(audit.get(field, 0))
                        for audit in first_opener_trace_audits
                    )
                )
                for field in count_fields
            },
            "coverage_complete": bool(
                all(
                    bool(audit.get("coverage_complete", False))
                    for audit in first_opener_trace_audits
                )
            ),
            "unsupported_nonopener_open_inventory_lifecycles": [
                row
                for audit in first_opener_trace_audits
                for row in (
                    audit.get("unsupported_nonopener_open_inventory_lifecycles") or []
                )
            ],
        }
        aggregated_opener_audit = out[
            "_first_opener_decision_to_terminal_trace_audit"
        ]
        aggregated_opener_audit["true_opener_inventory_lifecycle_coverage"] = float(
            int(aggregated_opener_audit["eligible_true_opener_inventory_lifecycle_count"])
            / max(int(aggregated_opener_audit["inventory_lifecycle_count"]), 1)
        )
    lineage_audits = [
        dict(r.get("_lineage_randomized_trace_audit") or {})
        for r in results
        if r.get("_lineage_randomized_trace_audit")
    ]
    if lineage_audits:
        denominator_keys = {
            key
            for audit in lineage_audits
            for key in (audit.get("denominator_counts") or {})
        }
        denominator_counts = {
            key: int(
                sum(
                    int((audit.get("denominator_counts") or {}).get(key, 0))
                    for audit in lineage_audits
                )
            )
            for key in denominator_keys
        }
        event_type_keys = {
            key
            for audit in lineage_audits
            for key in (audit.get("event_type_counts") or {})
        }
        event_type_counts = {
            key: int(
                sum(
                    int((audit.get("event_type_counts") or {}).get(key, 0))
                    for audit in lineage_audits
                )
            )
            for key in event_type_keys
        }
        out["_lineage_randomized_trace_audit"] = {
            "trace_schema_version": "narrowgate_lineage_randomized_trace.v2",
            "event_schema_version": "narrowgate_lineage_event_journal.v2",
            "denominator_counts": denominator_counts,
            "open_lineage_count": int(
                sum(int(audit.get("open_lineage_count", 0)) for audit in lineage_audits)
            ),
            "pending_inventory_lifecycle_record_count": int(
                sum(
                    int(audit.get("pending_inventory_lifecycle_record_count", 0))
                    for audit in lineage_audits
                )
            ),
            "event_type_counts": event_type_counts,
            "coverage_complete": bool(
                all(bool(audit.get("coverage_complete", False)) for audit in lineage_audits)
            ),
        }
    out["_adaptive_add_cooldown_trace"] = [
        row for r in results for row in r.get("_adaptive_add_cooldown_trace", [])
    ]
    out["_safe_add_rearm_trace"] = [
        row for r in results for row in r.get("_safe_add_rearm_trace", [])
    ]
    out["_safe_add_rearm_intervention_trace"] = [
        row
        for r in results
        for row in r.get("_safe_add_rearm_intervention_trace", [])
    ]
    out["_state_conditioned_rearm_trace"] = [
        row
        for r in results
        for row in r.get("_state_conditioned_rearm_trace", [])
    ]
    out["_local_action_ope_trace"] = [
        row for r in results for row in r.get("_local_action_ope_trace", [])
    ]
    out["_local_order_value_trace"] = [
        row for r in results for row in r.get("_local_order_value_trace", [])
    ]
    out["_local_order_lifecycle_trace"] = [
        row
        for r in results
        for row in r.get("_local_order_lifecycle_trace", [])
    ]
    out["_inventory_lifecycle_economic_fill_leg_trace"] = [
        row
        for r in results
        for row in r.get("_inventory_lifecycle_economic_fill_leg_trace", [])
    ]
    out["_dynamic_fill_hazard_lifecycle_journal"] = [
        row
        for r in results
        for row in r.get("_dynamic_fill_hazard_lifecycle_journal", [])
    ]
    out["_dynamic_fill_hazard_lifecycle_journal_audit"] = (
        audit_order_lifecycle_journal(
            out["_dynamic_fill_hazard_lifecycle_journal"]
        )
    )
    out["_queue_value_keep_cancel_trace"] = [
        row
        for r in results
        for row in r.get("_queue_value_keep_cancel_trace", [])
    ]
    out["_sell_add_skip_ope_trace"] = [
        row for r in results for row in r.get("_sell_add_skip_ope_trace", [])
    ]
    out["_state_conditioned_policy_trace"] = [
        row
        for r in results
        for row in r.get("_state_conditioned_policy_trace", [])
    ]
    out["_decision_trace"] = [row for r in results for row in r.get("_decision_trace", [])]
    selections = {
        str(result["baseline_selection"])
        for result in results
        if result.get("baseline_selection") is not None
    }
    current_flags = {
        bool(result["current_e3_mechanics_default"])
        for result in results
        if result.get("current_e3_mechanics_default") is not None
    }
    if selections or current_flags:
        if len(selections) != 1 or current_flags != {False}:
            raise RuntimeError("Segmented replay mixed baseline-selection authority")
        out["baseline_selection"] = selections.pop()
        out["current_e3_mechanics_default"] = False
        out["_baseline_selection_receipt"] = {
            "baseline_selection": out["baseline_selection"],
            "current_e3_mechanics_default": False,
            "explicit_non_current_arm": True,
        }
    return out


def _simulate_quality_segmented_with_engine(
    engine,
    trades_df,
    var_ts_ms,
    var_ssq,
    params,
    *,
    ml_data=None,
    bbo_data=None,
    l2_data=None,
    var_ti=None,
    var_retsq=None,
    start_ms: Optional[int] = None,
    end_ms: Optional[int] = None,
    max_gap_s: float = 3600.0,
) -> dict:
    bounds = _quality_segment_bounds(trades_df, max_gap_s=max_gap_s)
    if not bounds:
        raise ValueError("No quality segments available for replay")
    full_days = _full_calendar_days_for_loaded_window(trades_df, start_ms, end_ms)
    quality_days = _quality_calendar_days(trades_df)
    if len(bounds) == 1:
        result = _simulate_tick_with_engine(
            engine,
            trades_df,
            var_ts_ms,
            var_ssq,
            params,
            ml_data=ml_data,
            bbo_data=bbo_data,
            l2_data=l2_data,
            var_ti=var_ti,
            var_retsq=var_retsq,
        )
        result["quality_segment_aware"] = True
        result["quality_segment_count"] = 1
        result["quality_segment_max_gap_s"] = float(max_gap_s)
        result["full_calendar_day_count"] = full_days
        result["quality_day_count"] = quality_days
        result["active_trade_day_count"] = float(result.get("n_days", 0.0) or 0.0)
        fills = float(result.get("fills_total", 0.0) or 0.0)
        result["fills_per_active_trade_day"] = fills / max(result["active_trade_day_count"], 0.01)
        result["fills_per_quality_day"] = fills / max(quality_days, 0.01)
        result["fills_per_full_calendar_day"] = fills / max(full_days, 0.01)
        return result

    print(
        f"  Quality-segment replay: {len(bounds)} segments, "
        f"gap>{max_gap_s:.0f}s resets replay state; "
        f"full_days={full_days:.2f}, quality_days={quality_days:.2f}"
    )
    results = []
    for idx, (start, end) in enumerate(bounds, start=1):
        seg_trades = trades_df.iloc[start:end].reset_index(drop=True)
        if seg_trades.empty:
            continue
        seg_start = pd.to_datetime(int(seg_trades["transact_time"].iloc[0]), unit="ms", utc=True)
        seg_end = pd.to_datetime(int(seg_trades["transact_time"].iloc[-1]), unit="ms", utc=True)
        print(f"    segment {idx:02d}/{len(bounds)}: {len(seg_trades):,} aggTrades, {seg_start} → {seg_end}")
        seg_params = dict(params)
        result = _simulate_tick_with_engine(
            engine,
            seg_trades,
            var_ts_ms,
            var_ssq,
            seg_params,
            ml_data=ml_data,
            bbo_data=bbo_data,
            l2_data=l2_data,
            var_ti=var_ti,
            var_retsq=var_retsq,
        )
        results.append(result)
    return _aggregate_quality_segment_results(
        results,
        full_calendar_days=full_days,
        quality_days=quality_days,
        max_gap_s=max_gap_s,
    )


def _daily_result_row(day_label: str, result: dict) -> dict:
    fields = [
        "pnl",
        "inventory_adjusted_pnl",
        "inventory_pnl",
        "avg_markout",
        "avg_markout_bid",
        "avg_markout_ask",
        "fills_bid",
        "fills_ask",
        "fills_total",
        "buy_fill_qty",
        "sell_fill_qty",
        "buy_fill_notional",
        "sell_fill_notional",
        "buy_avg_fill_price",
        "sell_avg_fill_price",
        "fills_per_day",
        "fill_rate",
        "queue_ahead_base_mult",
        "queue_deplete_base_mult",
        "queue_l2_cancel_ahead_enabled",
        "queue_l2_cancel_ahead_event_count",
        "queue_l2_cancel_ahead_bid_event_count",
        "queue_l2_cancel_ahead_ask_event_count",
        "queue_l2_cancel_ahead_qty",
        "quote_attempts",
        "quote_attempts_per_day",
        "requotes_per_day",
        "avg_final_spread",
        "quote_spread_lt_100_rate",
        "final_spread_lt_100_rate",
        "cap_hit_rate",
        "adverse_guard_rate",
        "adverse_pause_rate",
        "defense_guard_rate",
        "defense_pause_rate",
        "stale_book_skip_rate",
        "fill_cooldown_block_count",
        "replace_min_price_change_ticks",
        "replace_min_price_change_ticks_reducing",
        "replace_min_interval_ms",
        "replace_min_interval_ms_reducing",
        "replace_pending_coalesce",
        "replace_pending_coalesce_replayed",
        "replace_terminal_continuation",
        "replace_terminal_continuation_terminal_count",
        "replace_terminal_continuation_decision_count",
        "replace_terminal_continuation_bid_decision_count",
        "replace_terminal_continuation_ask_decision_count",
        "replace_terminal_continuation_decision_latency_sum_ms",
        "replace_terminal_continuation_decision_latency_max_ms",
        "replace_cancel_first_exposure_increasing",
        "replace_cancel_first_exposure_increasing_replayed",
        "replace_throttle_count",
        "replace_throttle_bid_count",
        "replace_throttle_ask_count",
        "replace_throttle_price_count",
        "replace_throttle_age_count",
        "decision_place_count",
        "decision_replace_count",
        "decision_keep_count",
        "decision_pause_count",
        "decision_none_count",
        "decision_pending_coalesce_count",
        "decision_cancel_first_count",
        "fills_while_pending_cancel",
        "max_pending_new_orders",
        "max_pending_cancel_orders",
        "abs_inventory_time_s",
        "time_avg_abs_inventory",
        "pnl_per_abs_inventory_hour",
        "inv_adj_pnl_per_abs_inventory_hour",
        "max_inventory",
        "final_inventory",
        "n_days",
        "quality_segment_count",
        "new_order_latency_sample_count",
        "cancel_order_latency_sample_count",
        "live_perf_latency_mode",
        "baseline_selection",
        "current_e3_mechanics_default",
    ]
    row = {"day": day_label}
    for field in fields:
        if field in result:
            row[field] = result[field]
    return row


def _write_daily_segment_results(rows: list[dict], *, symbol: str, label: str) -> Optional[Path]:
    if not rows:
        return None
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    safe_label = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in label)
    path = RESULTS_DIR / f"tick_daily_results_{symbol.lower()}_{safe_label}.csv"
    pd.DataFrame(rows).sort_values("day").to_csv(path, index=False, float_format="%.6f")
    return path


def _simulate_daily_segmented_with_engine(
    engine,
    trades_df,
    var_ts_ms,
    var_ssq,
    params,
    *,
    ml_data=None,
    bbo_data=None,
    l2_data=None,
    var_ti=None,
    var_retsq=None,
    start_ms: Optional[int] = None,
    end_ms: Optional[int] = None,
    max_gap_s: float = 3600.0,
    output_label: str = "window",
) -> dict:
    """Replay each retained UTC day as a cold-start segment, then aggregate.

    中文说明：这是当前正式日度研究入口。每个 UTC day 独立初始化 replay
    状态；日内长 gap 继续交给 quality segment reset，防止坏数据污染后续。
    """
    bounds = _daily_segment_bounds(trades_df)
    if not bounds:
        raise ValueError("No daily segments available for replay")
    full_days = _full_calendar_days_for_loaded_window(trades_df, start_ms, end_ms)
    quality_days = _quality_calendar_days(trades_df)
    print(
        f"  Daily replay: {len(bounds)} UTC days, cold-start per day; "
        f"intra-day gap>{max_gap_s:.0f}s resets state; "
        f"full_days={full_days:.2f}, quality_days={quality_days:.2f}"
    )
    results = []
    daily_rows = []
    for idx, (day_label, start, end) in enumerate(bounds, start=1):
        day_trades = trades_df.iloc[start:end].reset_index(drop=True)
        if day_trades.empty:
            continue
        day_start_ms = int(day_trades["transact_time"].iloc[0])
        day_end_ms = int(day_trades["transact_time"].iloc[-1]) + 1
        print(f"    day {idx:02d}/{len(bounds)}: {day_label}, {len(day_trades):,} aggTrades")
        day_result = _simulate_quality_segmented_with_engine(
            engine,
            day_trades,
            var_ts_ms,
            var_ssq,
            dict(params),
            ml_data=ml_data,
            bbo_data=bbo_data,
            l2_data=l2_data,
            var_ti=var_ti,
            var_retsq=var_retsq,
            start_ms=day_start_ms,
            end_ms=day_end_ms,
            max_gap_s=max_gap_s,
        )
        day_result["daily_segment_aware"] = True
        day_result["replay_day"] = day_label
        results.append(day_result)
        daily_rows.append(_daily_result_row(day_label, day_result))

    out = _aggregate_quality_segment_results(
        results,
        full_calendar_days=full_days,
        quality_days=quality_days,
        max_gap_s=max_gap_s,
    )
    out["daily_segment_aware"] = True
    out["daily_segment_count"] = len(results)
    daily_path = _write_daily_segment_results(daily_rows, symbol=SYMBOL, label=output_label)
    if daily_path is not None:
        out["daily_results_csv"] = str(daily_path)
        print(f"  Daily diagnostics saved → {daily_path}")
    return out


def _init_global(trades_df, var_ts_ms, var_ssq,
                 ml_data=None, bbo_data=None, l2_data=None,
                 var_ti=None, var_retsq=None, engine="python"):
    global _G
    _G = {"trades_df": trades_df, "var_ts_ms": var_ts_ms,
           "var_ssq": var_ssq, "ml_data": ml_data,
           "bbo_data": bbo_data, "l2_data": l2_data,
           "var_ti": var_ti, "var_retsq": var_retsq,
           "engine": engine}


def _worker(params):
    r = _simulate_tick_with_engine(
        _G.get("engine", "python"),
        _G["trades_df"], _G["var_ts_ms"], _G["var_ssq"], params,
        ml_data=_G.get("ml_data"),
        bbo_data=_G.get("bbo_data"), l2_data=_G.get("l2_data"),
        var_ti=_G.get("var_ti"), var_retsq=_G.get("var_retsq"),
    )
    return {k: v for k, v in r.items() if not k.startswith("_")}


def run_sweep(trades_df, var_ts_ms, var_ssq, base, n_workers=None,
              ml_data=None, use_ml_grid=False,
              bbo_data=None, l2_data=None,
              var_ti=None, var_retsq=None, sort_by="selection_score",
              engine="python"):
    import multiprocessing as mp

    _init_global(trades_df, var_ts_ms, var_ssq, ml_data, bbo_data, l2_data,
                 var_ti, var_retsq, engine=engine)

    grid = SWEEP_GRID_ML if use_ml_grid else SWEEP_GRID
    keys = list(grid.keys())
    vals = [grid[k] for k in keys]
    combos = list(product(*vals))

    params_list = []
    for combo in combos:
        p = dict(base)
        if engine == "cpp":
            p["collect_curves"] = False
        for k, v in zip(keys, combo, strict=True):
            if k == "quote_coefficients":
                if p["inventory_reference_qty"] != 1.0:
                    raise ValueError("registered coefficient tuples require inventory_reference_qty=1")
                p.update(v)
            else:
                p[k] = v
        params_list.append(p)

    nw = n_workers or default_backtest_workers()
    nc = len(params_list)
    print(f"Sweep: {nc} combos, {nw} workers, engine={engine}\n")

    # Warm up with single run
    t0 = time.perf_counter()
    _worker(params_list[0])
    t_one = time.perf_counter() - t0
    print(f"  Single run: {t_one:.2f}s")

    est_min = t_one * nc / nw / 60
    print(f"  Estimated: {est_min:.1f} min")
    sys.stdout.flush()

    ctx = mp.get_context("fork")
    t0 = time.perf_counter()
    results = []
    batch = nw * 3
    with ctx.Pool(nw) as pool:
        for start in range(0, nc, batch):
            chunk = params_list[start:start + batch]
            chunk_results = pool.map(_worker, chunk)
            results.extend(chunk_results)
            elapsed = time.perf_counter() - t0
            done = start + len(chunk)
            pct = done / nc * 100
            eta_s = elapsed / done * (nc - done) if done > 0 else 0
            best_pnl = max(r["pnl"] for r in results)
            pos = sum(1 for r in results if r["pnl"] > 0)
            print(f"  [{pct:5.1f}%] {done}/{nc}  "
                  f"elapsed={elapsed:.0f}s  ETA={eta_s:.0f}s  "
                  f"best_pnl=${best_pnl:.2f}  positive={pos}")
            sys.stdout.flush()
    t_par = time.perf_counter() - t0

    speedup = t_one * nc / t_par if t_par > 0 else 0
    print(f"\n  Total: {t_par:.1f}s  Speedup: {speedup:.1f}×\n")

    return _sort_results(_attach_selection_scores(results), sort_by)


def run_cap_ab_tick(trades_df, var_ts_ms, var_ssq, base,
                    ml_data=None, bbo_data=None, l2_data=None,
                    var_ti=None, var_retsq=None,
                    sort_by="selection_score", engine="python"):
    params_list = _build_cap_ab_params(base)
    dyn_base = params_list[-1].get("dynamic_cap_base_bps", base.get("max_spread_bps", 0.0))
    dyn_alpha = params_list[-1].get("dynamic_cap_alpha", 0.5)
    dyn_max_mult = params_list[-1].get("dynamic_cap_max_mult", 2.0)
    dyn_var_baseline = params_list[-1].get("dynamic_cap_var_baseline", 0.0)
    dyn_var_desc = f"{dyn_var_baseline:.6f}" if dyn_var_baseline > 0.0 else "vol_baseline^2"

    print(f"\nRunning cap A/B … engine={engine}")
    print(f"  Fixed caps: {'/'.join(str(int(cap)) for cap in CAP_AB_FIXED_BPS)} bps")
    print(
        f"  Dynamic cap: base={dyn_base:.1f}bps, alpha={dyn_alpha:.2f}, "
        f"max_mult={dyn_max_mult:.2f}, var_baseline={dyn_var_desc}"
    )

    results = []
    t0 = time.perf_counter()
    for params in params_list:
        if engine == "cpp":
            params = dict(params, collect_curves=False)
        result = _simulate_tick_with_engine(
            engine,
            trades_df, var_ts_ms, var_ssq, params,
            ml_data=ml_data,
            bbo_data=bbo_data, l2_data=l2_data,
            var_ti=var_ti, var_retsq=var_retsq,
        )
        results.append({k: v for k, v in result.items() if not k.startswith("_")})
    elapsed = time.perf_counter() - t0

    results = _sort_results(_attach_selection_scores(results), sort_by)
    print_results(results, top_n=len(results), sort_by=sort_by)
    print(f"\n  Cap A/B runs: {len(results)}  Time: {elapsed:.1f}s")

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    cap_path = RESULTS_DIR / "tick_cap_ab_results.csv"
    pd.DataFrame(results).to_csv(cap_path, index=False, float_format="%.6f")
    print(f"  Cap A/B results saved → {cap_path}")


# ═══════════════════════════════════════════════════════════════════
#  4. OUTPUT
# ═══════════════════════════════════════════════════════════════════

_HDR = (f"{'Rk':>3s}  {'γ':>6s}  {'κ':>6s}  {'QBase':>6s}  {'QDec':>5s}  {'Cap':>4s}  "
        f"{'η':>4s}  {'VB':>4s}  {'Skw':>4s}  {'Asy':>4s}  {'GD':>4s}  {'RS':>4s}  "
    f"{'Sel':>5s}  {'InvAdj':>10s}  {'Mko':>7s}  {'PnL($)':>10s}  {'$/day':>8s}  {'Sharpe':>7s}  "
        f"{'MaxDD':>8s}  {'F/d':>6s}  {'FR':>5s}  {'Sprd':>7s}  "
        f"{'AvgI':>7s}  {'InvHr':>7s}  {'$/InvHr':>8s}  {'MaxI':>6s}  {'TO':>3s}")


def _row(i, r):
    return (f"{i:3d}  {r['risk_per_order']:6.3f}  {r['execution_intensity_slope']:6.3f}  "
            f"{r.get('queue_base', 5.0):6.1f}  {r.get('queue_decay', 0.1):5.2f}  "
            f"{_cap_cell(r):>4s}  "
            f"{r.get('eta', 0.0):4.1f}  "
            f"{r.get('vol_blend', 0.0):4.1f}  "
            f"{r.get('skew', 0.0):4.1f}  "
            f"{r.get('asym', 0.0):4.1f}  "
            f"{r.get('gdir', 0.0):4.1f}  "
            f"{r.get('ret_skew', 0.0):4.0f}  "
            f"{r.get('selection_score', 0.0):5.2f}  {r.get('inventory_adjusted_pnl', 0.0):10.2f}  {r.get('avg_markout', 0.0):7.2f}  "
            f"{r['pnl']:10.2f}  {r['pnl_per_day']:8.2f}  "
            f"{r['sharpe']:7.2f}  {r['max_drawdown']:8.2f}  "
            f"{r['fills_per_day']:6.1f}  {r['fill_rate']:5.3f}  "
            f"{r['avg_spread']:7.2f}  "
            f"{r['avg_inventory']:7.4f}  {r.get('abs_inventory_time_s', 0.0) / 3600.0:7.4f}  "
            f"{r.get('pnl_per_abs_inventory_hour', 0.0):8.2f}  {r['max_inventory']:6.3f}  "
            f"{r.get('n_timeouts', 0):3d}")


def _pick_robust_best(results, sort_by, top_k=5):
    if not results:
        return None
    metric_field, _ = SORT_OBJECTIVES[sort_by]
    k = min(top_k, len(results))
    top = results[:k]
    metric_values = np.array([r.get(metric_field, 0.0) for r in top], dtype=np.float64)
    metric_median = float(np.median(metric_values))
    return min(
        top,
        key=lambda r: (
            abs(r.get(metric_field, 0.0) - metric_median),
            -r.get("avg_markout", float("-inf")),
            -r["pnl"],
        ),
    )

def _format_pick(label, r):
    ml_str = ""
    if (r.get("vol_blend", 0) > 0 or r.get("skew", 0) > 0
            or r.get("asym", 0) > 0 or r.get("ret_skew", 0) > 0
            or r.get("gdir", 0) > 0):
        ml_str = (f", VB={r.get('vol_blend', 0)}, "
                  f"Skw={r.get('skew', 0)}, "
                  f"Asy={r.get('asym', 0)}, "
                  f"GD={r.get('gdir', 0)}, "
                  f"RS={r.get('ret_skew', 0)}")
    print(f"  {label}: Cap={_cap_cell(r)}, spread-risk={r['risk_per_order']}, κ={r['execution_intensity_slope']}, "
          f"QB={r.get('queue_base', 5)}, QD={r.get('queue_decay', 0.1)}"
          f"{ml_str}")
    print(f"         InvAdj=${r.get('inventory_adjusted_pnl', 0.0):.2f}, PnL=${r['pnl']:.2f}, "
          f"AvgMarkout={r.get('avg_markout', 0.0):.2f}, Sharpe={r['sharpe']:.2f}, F/d={r['fills_per_day']:.0f}, "
          f"TO={r.get('n_timeouts', 0)}")


def print_results(results, top_n=30, sort_by="selection_score"):
    metric_field, metric_label = SORT_OBJECTIVES[sort_by]
    print("=" * 150)
    print(f"Ranked by: {metric_label}")
    print(_HDR)
    print("-" * 150)
    for i, r in enumerate(results[:top_n], 1):
        print(_row(i, r))
    print("=" * 150)

    if results:
        k = min(5, len(results))
        top = results[:k]
        metric_values = np.array([r.get(metric_field, 0.0) for r in top], dtype=np.float64)
        pnl = np.array([r["pnl"] for r in top], dtype=np.float64)
        robust = _pick_robust_best(results, sort_by, top_k=k)
        print(f"\n  Selection-robust (displayed top-{k} by {metric_label}): "
              f"median {metric_label}={np.median(metric_values):.2f}, "
              f"median PnL=${np.median(pnl):.2f}")
        print(f"  Robust pick: Cap={_cap_cell(robust)}, spread-risk={robust['risk_per_order']}, κ={robust['execution_intensity_slope']}, "
              f"{metric_label}={robust.get(metric_field, 0.0):.2f}, "
              f"InvAdj=${robust.get('inventory_adjusted_pnl', 0.0):.2f}, "
              f"PnL=${robust['pnl']:.2f}, Sharpe={robust['sharpe']:.2f}")

    positive = [r for r in results if r["pnl"] > 0]
    print(f"\n  Positive PnL: {len(positive)}/{len(results)} configs")
    selection_pool = positive if positive else results
    if selection_pool:
        best_by_metric = _best_result(selection_pool, sort_by)
        best_sharpe = _best_result(selection_pool, "sharpe")
        best_pnl = _best_result(selection_pool, "pnl")
        best_daily = _best_result(selection_pool, "pnl_per_day")
        picks = [
            (f"Best by {metric_label}", best_by_metric),
            ("Best by Sharpe", best_sharpe),
            ("Best by PnL", best_pnl),
            ("Best by $/day", best_daily),
        ]
        seen = set()
        for label, pick in picks:
            if pick is None or id(pick) in seen:
                continue
            _format_pick(label, pick)
            seen.add(id(pick))

    # ML vs baseline comparison
    baseline = [r for r in results
                if r.get("vol_blend", 0) == 0 and r.get("skew", 0) == 0
                and r.get("asym", 0) == 0 and r.get("ret_skew", 0) == 0
                and r.get("gdir", 0) == 0]
    ml_results = [r for r in results
                  if r.get("vol_blend", 0) > 0 or r.get("skew", 0) > 0
                  or r.get("asym", 0) > 0 or r.get("ret_skew", 0) > 0
                  or r.get("gdir", 0) > 0]
    if baseline and ml_results:
        bl_metric = _best_result(baseline, sort_by)
        ml_metric = _best_result(ml_results, sort_by)
        bl_sharpe = _best_result(baseline, "sharpe")
        ml_sharpe = _best_result(ml_results, "sharpe")
        bl_pnl = _best_result(baseline, "pnl")
        ml_pnl = _best_result(ml_results, "pnl")
        print("\n  ── ML vs Baseline comparison ──")
        print(f"  By {metric_label}: baseline PnL=${bl_metric['pnl']:.2f}, "
              f"Sharpe={bl_metric['sharpe']:.2f}, $/day=${bl_metric['pnl_per_day']:.2f} | "
              f"ML PnL=${ml_metric['pnl']:.2f}, "
              f"Sharpe={ml_metric['sharpe']:.2f}, $/day=${ml_metric['pnl_per_day']:.2f}")
        print(f"  By Sharpe: baseline PnL=${bl_sharpe['pnl']:.2f}, "
              f"Sharpe={bl_sharpe['sharpe']:.2f}, $/day=${bl_sharpe['pnl_per_day']:.2f} | "
              f"ML PnL=${ml_sharpe['pnl']:.2f}, "
              f"Sharpe={ml_sharpe['sharpe']:.2f}, $/day=${ml_sharpe['pnl_per_day']:.2f}")
        print(f"  By PnL:    baseline PnL=${bl_pnl['pnl']:.2f}, "
              f"Sharpe={bl_pnl['sharpe']:.2f}, $/day=${bl_pnl['pnl_per_day']:.2f} | "
              f"ML PnL=${ml_pnl['pnl']:.2f}, "
              f"Sharpe={ml_pnl['sharpe']:.2f}, $/day=${ml_pnl['pnl_per_day']:.2f}")


def save_results(results, suffix=""):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(results)
    path = RESULTS_DIR / f"tick_sweep_results{suffix}.csv"
    df.to_csv(path, index=False, float_format="%.6f")
    print(f"\nResults saved → {path}")


# ═══════════════════════════════════════════════════════════════════
#  5. MAIN
# ═══════════════════════════════════════════════════════════════════

def load_public_inputs(bundle, *, tick_size):
    """Maintained replay entry to the shared parser/observation contract."""
    from models.replay.public_input import load_public_replay_inputs
    return load_public_replay_inputs(bundle, tick_size=tick_size)


def simulate_public_inputs(bundle, params, *, signal_engine=None, public_strategy=None,
                           checkpoint_at_ts_ms=None, resume_checkpoint=None,
                           execution_end_ns=None):
    """Prepare one explicit bundle; multiple candidates can share prepared inputs."""
    if params.get("cross_market_enabled"):
        raise ValueError("public input replay does not support reference-market observations")
    prepared = prepare_public_inputs(bundle, tick_size=params["tick_size"])
    return simulate_prepared_inputs(prepared, params, signal_engine=signal_engine,
                                    public_strategy=public_strategy,
                                    checkpoint_at_ts_ms=checkpoint_at_ts_ms,
                                    resume_checkpoint=resume_checkpoint,
                                    execution_end_ns=execution_end_ns)


def prepare_public_inputs(bundle, *, tick_size, cache_dir=None, require_existing_cache=False):
    if cache_dir is not None:
        from models.replay.prepared_cache import prepare_cached
        return prepare_cached(bundle, tick_size, cache_dir, prepare_public_inputs,
                              require_existing=require_existing_cache)
    if require_existing_cache:
        raise ValueError("require_existing_cache needs an explicit cache directory")
    from models.replay.public_input import PreparedReplayInputs
    inputs = load_public_inputs(bundle, tick_size=tick_size)
    variance_ts, variance = build_rolling_variance(inputs["bars"])
    prepared = PreparedReplayInputs.create(inputs, tick_size, variance_ts, variance)
    return prepared


def simulate_prepared_inputs(prepared, params, *, signal_engine=None, public_strategy=None,
                             checkpoint_at_ts_ms=None, resume_checkpoint=None,
                             execution_end_ns=None, _prefill_fork=None, _response_update_fork=None,
                             prepared_predictions=None, consume_resume_checkpoint=False):
    """Use the existing Python execution engine with bound shared consumers.

    No economics are run by data acceptance. A later explicit replay call must
    declare the unidentified public-volume eligibility assumption. The native
    executor is not silently substituted for this newly integrated Python path.

    Explicit single-successor callers may transfer checkpoint ownership. Once
    transferred, even a failed call consumes that handle; retry requires a
    separately persisted checkpoint. Branching callers keep the default clone.
    """
    from models.replay.public_input import public_predictions
    if type(consume_resume_checkpoint) is not bool:
        raise TypeError("consume_resume_checkpoint must be an explicit bool")
    if _prefill_fork is not None and consume_resume_checkpoint:
        raise ValueError('prefill branches require an independently cloned same-window checkpoint')
    if _response_update_fork is not None and consume_resume_checkpoint:
        raise ValueError('optional-update branches require independently cloned checkpoints')
    if params.get("cross_market_enabled"):
        raise ValueError("public input replay does not support reference-market observations")
    if params.get("public_fill_volume_policy") != "all_public_volume_eligible":
        raise ValueError("normal trade eligibility is unavailable; declare an explicit fill-volume scenario")
    if params.get("exchange_book_queue_mode") != "diagnostic" or params.get("replay_event_clock") != "merged":
        raise ValueError("common inputs require explicit diagnostic exchange-book and merged event clock; no native sequence claim")
    if params["tick_size"] != prepared.tick_size:
        raise ValueError("prepared input tick contract mismatch")
    inputs = prepared.inputs
    if inputs["trades"].attrs["source_clock_policy"] != "strict_exchange" and params.get("replay_purpose") == "formal":
        raise ValueError("source-time proxy is not strict formal clock evidence")
    replay_params = dict(params)
    replay_params["cross_market_enabled"] = False
    # Immutable observations arrive on their ready clock. Independently
    # sampled delivery may reveal an older source state later; preserve its
    # true age rather than sorting/cummax rewriting already-trained inputs.
    replay_params["source_asof_order_policy"] = "modeled_delivery_order"
    replay_params["require_historical_bbo"] = True
    replay_params["input_contract_id"] = inputs["bundle"].manifest["input_contract_id"]
    replay_params["observation_contract_id"] = inputs["bundle"].manifest["observation_contract_id"]
    plan = inputs["bundle"].manifest["plan"]
    if _response_update_fork is not None:
        from strategy.optional_update import OptionalUpdateFork
        if (not isinstance(_response_update_fork, OptionalUpdateFork) or resume_checkpoint is None
                or execution_end_ns is not None or checkpoint_at_ts_ms is not None or _prefill_fork is not None):
            raise ValueError('use the explicit optional-update branch contract')
        _response_update_fork.validate(resume_checkpoint, account_end_ns=plan['end_ns'])
    if _prefill_fork is not None:
        from models.replay.runtime_checkpoint_io import PrefillFork
        if (not isinstance(_prefill_fork, PrefillFork) or resume_checkpoint is None
                or execution_end_ns is not None or checkpoint_at_ts_ms is not None):
            raise ValueError('use the explicit prefill branch entrypoint')
        _prefill_fork.validate(resume_checkpoint, account_end_ns=plan['end_ns'])
    account_start = params.get("account_start_ns", plan["start_ns"])
    if not plan["start_ns"] <= account_start < plan["end_ns"]:
        raise ValueError("account interval must be inside the warmed common-input interval")
    account_end = plan["end_ns"] if execution_end_ns is None else execution_end_ns
    if (type(account_end) is not int or not account_start < account_end <= plan["end_ns"]
            or (execution_end_ns is not None and account_end % 1_000_000)):
        raise ValueError("execution end must be an exact millisecond inside the parent account")
    replay_params["replay_event_clock_start_ts_ms"] = (account_start+999_999)//1_000_000
    replay_params["replay_event_clock_end_ts_ms"] = (account_end-1)//1_000_000
    window_binding = prepared.window_binding
    if window_binding is not None:
        from models.replay.public_input_window import PublicWindowBinding
        if (not isinstance(window_binding, PublicWindowBinding) or execution_end_ns is not None
                or _prefill_fork is not None or _response_update_fork is not None):
            raise ValueError('bounded public windows require the global account path')
        window_binding.admit(prepared, prepared_predictions)
        if window_binding.end_ns < account_end and checkpoint_at_ts_ms is None:
            raise ValueError('a nonterminal input window must produce a recovery cut')
        if checkpoint_at_ts_ms is not None and checkpoint_at_ts_ms*10**6 >= window_binding.end_ns:
            raise ValueError('recovery cut requires input lookahead')
        replay_params['replay_event_clock_start_ts_ms'] = max(
            replay_params['replay_event_clock_start_ts_ms'], (window_binding.begin_ns+999_999)//10**6)
        replay_params['replay_event_clock_end_ts_ms'] = min(
            replay_params['replay_event_clock_end_ts_ms'], (window_binding.end_ns-1)//10**6)
    replay_params['_l2_account_exclusive_end_ns'] = account_end
    if _prefill_fork is not None:
        replay_params['_l2_account_exclusive_end_ns'] = _prefill_fork.execution_end_ns
    if _response_update_fork is not None:
        replay_params['_l2_account_exclusive_end_ns'] = _response_update_fork.execution_end_ns
    if execution_end_ns is not None:
        replay_params['_bounded_execution_end_ns'] = account_end
    account = prepared.account_trades(replay_params['replay_event_clock_start_ts_ms'],
        replay_params['replay_event_clock_end_ts_ms'] if execution_end_ns is not None or window_binding is not None else None,
        quantity_rule=params['public_fill_volume_policy'])
    trades = account.execution_frame()
    # The scenario is explicit and reported; the normalized facts keep this
    # field unknown. Never describe this assumed eligibility as observed.
    if prepared_predictions is not None:
        from models.replay.public_input import PreparedPublicPredictions
        if signal_engine is not None or not isinstance(prepared_predictions, PreparedPublicPredictions):
            raise ValueError('use either a signal engine or prepared public predictions')
        ml = prepared_predictions.for_inputs(prepared)
    else:
        ml = None if signal_engine is None else public_predictions(inputs["frames"], signal_engine, XMARKET_REPLAY_FEATURE_COLUMNS)
    variance_ts, variance = prepared.variance_ts, prepared.variance
    input_manifest_id = prepared.manifest_id
    checkpoint_binding = None
    if checkpoint_at_ts_ms is not None or resume_checkpoint is not None:
        from models.replay.runtime_checkpoint_io import public_checkpoint_binding
        binding_params = dict(params)
        # Output destination is not policy identity; observe-only mode remains
        # bound, and runtime resume separately verifies the journal prefix.
        binding_params.pop("_target_journal", None)
        if execution_end_ns is not None:
            binding_params['_bounded_execution_end_ns'] = account_end
        if window_binding is not None:
            binding_params['_global_public_timelines'] = window_binding.global_identity
            checkpoint_binding = public_checkpoint_binding(input_manifest_id, binding_params, None)
        else:
            checkpoint_binding = public_checkpoint_binding(input_manifest_id, binding_params, ml,
                                                           prediction_owner=prepared_predictions)
        if resume_checkpoint is not None and resume_checkpoint.get("public_binding") != checkpoint_binding:
            raise ValueError("public checkpoint input, parameters, predictions or implementation changed")
    if public_strategy is not None and resume_checkpoint is None:
        public_strategy.start(input_manifest_id)
    from data.response_cursor import using_row_group_cache
    with using_row_group_cache(prepared.response_row_cache()):
        result = simulate_tick(trades, variance_ts, variance, replay_params, ml_data=ml,
            bbo_data=inputs["bbo"], l2_data=inputs["l2"],
            exchange_book_event_tape=inputs["exchange_book_event_tape"], public_strategy=public_strategy,
            checkpoint_at_ts_ms=checkpoint_at_ts_ms, resume_checkpoint=resume_checkpoint,
            consume_resume_checkpoint=consume_resume_checkpoint,
            resume_input_batch=window_binding is not None and resume_checkpoint is not None,
            prefill_fork=_prefill_fork, response_update_fork=_response_update_fork,
            _prepared_clock_owner=prepared, _prepared_account=account,
            _prepared_predictions_owner=prepared_predictions)
    # Local mutually recursive execution helpers capture the runtime. Collect
    # retired call graphs at this cold boundary, before another large restore;
    # the returned checkpoint and all caller-owned branch states remain live.
    if params.get('_replay_phase_callback') is not None:
        params['_replay_phase_callback']('cleanup')
    gc.collect()
    if "_replay_checkpoint" in result:
        result["_replay_checkpoint"]["public_binding"] = checkpoint_binding
        return result
    result["public_input_contract"] = {"input_contract_id": replay_params["input_contract_id"],
        "input_manifest_id": input_manifest_id,
        "observation_contract_id": replay_params["observation_contract_id"],
        "native_observation_parity": "not_proven", "fill_volume_policy": params["public_fill_volume_policy"],
        "source_asof_order_policy": "modeled_delivery_order",
        "clock_policy": inputs["trades"].attrs["source_clock_policy"],
        "account_start_ns": account_start, "market_warmup_start_ns": plan["start_ns"]}
    if execution_end_ns is not None:
        result["public_input_contract"]["account_end_ns"] = account_end
    if _prefill_fork is not None:
        result['public_input_contract']['account_end_ns'] = _prefill_fork.execution_end_ns
    if _response_update_fork is not None:
        result['public_input_contract']['account_end_ns'] = _response_update_fork.execution_end_ns
    # L2/trades cannot establish funding. The legacy engine's MTM diagnostic
    # is not a complete account return and must never be labelled all-in PnL.
    result["funding_cashflow"] = None
    result["all_in_net_pnl"] = None
    result["economic_complete"] = False
    result["economic_admission"] = False
    return result


def simulate_optional_update_branch(prepared, params, *, checkpoint, request,
                                    signal_engine=None, prepared_predictions=None):
    """Apply one explicit optional-price action, then continue original B0.

The intervention namespace must already be enabled in the unchanged parent
binding. Returned execution is bounded and still requires normal settlement.
    """
    return simulate_prepared_inputs(prepared, params, signal_engine=signal_engine,
        resume_checkpoint=checkpoint, _response_update_fork=request,
        prepared_predictions=prepared_predictions)


def simulate_prefill_branch(prepared, params, *, checkpoint, opportunity, action,
                            signal_engine=None, prepared_predictions=None):
    """Resume an earlier B0 checkpoint; intervene only at its exact opportunity.

    All parent parameters/predictions still pass the ordinary public binding.
    Input coverage is not shortened or rewritten. The earlier endpoint is an
    explicit event-loop cut, not a modified ordinary restoration binding.
    """
    from models.replay.runtime_checkpoint_io import PrefillFork
    request = PrefillFork(opportunity_id=opportunity['opportunity_id'],
        decision_ns=opportunity['decision_ts_ns'], decision_sequence=opportunity['decision_sequence'],
        order_id=opportunity['order_id'], kind=opportunity['kind'], action=action,
        execution_end_ns=opportunity['decision_ts_ns']+30_000_000_000)
    return simulate_prepared_inputs(prepared, params, signal_engine=signal_engine,
        resume_checkpoint=checkpoint, _prefill_fork=request,
        prepared_predictions=prepared_predictions)


if __name__ == "__main__":
    raise SystemExit("Use the installed narrowgate replay command")
