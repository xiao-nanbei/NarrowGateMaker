#!/usr/bin/env python3
"""Build the causal active-order competing-risk panel.

``local_order_value_panel_v1`` has one row per preregistered active-order
observation.  It does not nearest-match orders by side and time: the caller
must provide the exact replay ``order_id`` and ``decision_id``.  Decision-time
features are accepted only when ``feature_ready_ts_ns <= decision_ts_ns``.

The first observable event wins:

* favorable or adverse fill, classified at the frozen fill-value horizon;
* cancel acknowledgement;
* adverse price jump;
* inventory-campaign repair; or
* right censoring.

The order-value panel may contain repeated observations of an order for hazard
estimation.  A randomized action panel remains stricter and must contain at
most one intervention per campaign so terminal PnL is never copied to several
decisions.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import math
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

SCHEMA_VERSION = "local_order_value_panel.v1"
COMPETING_RISK_LABEL_IDENTITY = (
    "exact_order_id_mixed_market_policy_campaign_first_event.v2"
)
EVENT_TYPES = (
    "favorable_fill",
    "adverse_fill",
    "cancel",
    "adverse_price_jump",
    "campaign_repair",
    "censored",
)
EVENT_CODE = {name: index for index, name in enumerate(EVENT_TYPES)}

REQUIRED_COLUMNS = {
    "day",
    "decision_id",
    "order_id",
    "campaign_id",
    "side",
    "decision_ts_ns",
    "feature_ready_ts_ns",
    "censor_ts_ns",
}

EVENT_TIMESTAMP_COLUMNS = {
    "fill": "fill_ts_ns",
    "cancel": "cancel_ack_ts_ns",
    "adverse_price_jump": "adverse_price_jump_ts_ns",
    "campaign_repair": "repair_ts_ns",
}


@dataclass(frozen=True)
class PanelFeatureSpec:
    name: str
    available_at: str
    source_timestamp_col: str = ""
    description: str = ""


DEFAULT_FEATURE_SPECS: tuple[PanelFeatureSpec, ...] = (
    PanelFeatureSpec("side", "decision"),
    PanelFeatureSpec("inventory_role", "decision"),
    PanelFeatureSpec("inventory", "decision"),
    PanelFeatureSpec("inventory_ratio", "decision"),
    PanelFeatureSpec("campaign_age_s", "decision"),
    PanelFeatureSpec("campaign_pnl_so_far", "decision"),
    PanelFeatureSpec("campaign_mae_so_far", "decision"),
    PanelFeatureSpec("campaign_add_count_so_far", "decision"),
    PanelFeatureSpec("order_age_ms", "decision"),
    PanelFeatureSpec("order_price", "decision"),
    PanelFeatureSpec("quote_distance_ticks", "decision"),
    PanelFeatureSpec("queue_init", "decision"),
    PanelFeatureSpec("queue_left", "decision"),
    PanelFeatureSpec("queue_fraction_left", "decision"),
    PanelFeatureSpec("queue_local_rank", "decision"),
    PanelFeatureSpec("queue_source", "decision"),
    PanelFeatureSpec("spread_ticks", "decision"),
    PanelFeatureSpec("book_imbalance", "decision"),
    PanelFeatureSpec("book_state_resolution_ms", "decision"),
    PanelFeatureSpec("microprice_shift_bps", "decision"),
    PanelFeatureSpec("l2_book_cancel_ratio", "decision"),
    PanelFeatureSpec("l2_book_refresh_ratio", "decision"),
    PanelFeatureSpec("l2_quote_flip_rate", "decision"),
    PanelFeatureSpec("toxicity", "decision"),
    PanelFeatureSpec("markout_ema", "decision"),
    PanelFeatureSpec("market_order_intensity", "decision"),
    PanelFeatureSpec("cancel_intensity", "decision"),
    PanelFeatureSpec("refill_intensity", "decision"),
    PanelFeatureSpec("empirical_microprice_ticks", "decision"),
    PanelFeatureSpec("empirical_adverse_probability", "decision"),
)

SIMULATOR_AUDIT_SPECS: tuple[PanelFeatureSpec, ...] = (
    PanelFeatureSpec(
        "exchange_book_queue_status",
        "simulator_only",
        "exchange_book_queue_asof_ts_ns",
        "Native exchange-time queue support; never a policy feature.",
    ),
    PanelFeatureSpec(
        "exchange_book_queue_segment_id",
        "simulator_only",
        "exchange_book_queue_asof_ts_ns",
        "Native reconstruction segment; never a policy feature.",
    ),
    PanelFeatureSpec(
        "simulator_queue_init",
        "simulator_only",
        "exchange_book_queue_asof_ts_ns",
        "Exact native queue used by fill mechanics only.",
    ),
    PanelFeatureSpec(
        "simulator_queue_left",
        "simulator_only",
        "exchange_book_queue_asof_ts_ns",
        "Exact native queue path used by fill mechanics only.",
    ),
    PanelFeatureSpec(
        "simulator_queue_source",
        "simulator_only",
        "exchange_book_queue_asof_ts_ns",
        "Native/fallback simulator source; never a policy feature.",
    ),
)


def _numeric(
    frame: pd.DataFrame,
    name: str,
    default: float = math.nan,
) -> pd.Series:
    if name not in frame:
        return pd.Series(default, index=frame.index, dtype=float)
    return pd.to_numeric(frame[name], errors="coerce")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_feature_provenance(
    frame: pd.DataFrame,
    *,
    feature_specs: Sequence[PanelFeatureSpec] = DEFAULT_FEATURE_SPECS,
) -> None:
    """Reject terminal features and feature timestamps later than the action."""

    decision_ts = _numeric(frame, "decision_ts_ns")
    if decision_ts.isna().any():
        raise ValueError("decision_ts_ns must be complete and numeric")
    for spec in feature_specs:
        if spec.name not in frame:
            continue
        if spec.available_at not in {"decision", "submit"}:
            raise ValueError(
                f"{spec.name} is available at {spec.available_at}, not decision time"
            )
        if not spec.source_timestamp_col:
            continue
        if spec.source_timestamp_col not in frame:
            raise ValueError(
                f"{spec.name} requires {spec.source_timestamp_col}"
            )
        source_ts = _numeric(frame, spec.source_timestamp_col)
        invalid = source_ts.notna() & source_ts.gt(decision_ts)
        if invalid.any():
            raise ValueError(
                f"{spec.name} contains {int(invalid.sum())} future observations"
            )


def validate_order_observations(
    frame: pd.DataFrame,
    *,
    feature_specs: Sequence[PanelFeatureSpec] = DEFAULT_FEATURE_SPECS,
) -> None:
    missing = sorted(REQUIRED_COLUMNS - set(frame.columns))
    if missing:
        raise ValueError(f"local order-value source missing columns: {missing}")
    if frame.empty:
        raise ValueError("local order-value source is empty")
    if frame["decision_id"].astype("string").isna().any():
        raise ValueError("decision_id is required")
    if frame["decision_id"].astype(str).duplicated().any():
        raise ValueError("decision_id must be globally unique")
    if set(frame["side"].astype(str).str.upper()) - {"BUY", "SELL"}:
        raise ValueError("side must be BUY or SELL")
    decision_ts = _numeric(frame, "decision_ts_ns")
    ready_ts = _numeric(frame, "feature_ready_ts_ns")
    censor_ts = _numeric(frame, "censor_ts_ns")
    if decision_ts.isna().any() or ready_ts.isna().any() or censor_ts.isna().any():
        raise ValueError("decision, feature-ready, and censor timestamps are required")
    if (ready_ts > decision_ts).any():
        raise ValueError("feature_ready_ts_ns must not exceed decision_ts_ns")
    if (
        "exchange_book_queue_status" in frame
        and "exchange_book_queue_asof_ts_ns" in frame
    ):
        native_status = (
            frame["exchange_book_queue_status"].astype(str).str.lower()
        )
        native_asof = _numeric(
            frame,
            "exchange_book_queue_asof_ts_ns",
            0.0,
        )
        native_usable = native_status.isin({"exact", "known_zero"})
        not_strictly_prior = (
            native_usable
            & native_asof.gt(0.0)
            & native_asof.ge(decision_ts)
        )
        if not_strictly_prior.any():
            raise ValueError(
                "native exchange-book queue state must be strictly earlier "
                "than the order activation/decision timestamp"
            )
    if (censor_ts < decision_ts).any():
        raise ValueError("censor_ts_ns must not precede decision_ts_ns")
    for timestamp_col in EVENT_TIMESTAMP_COLUMNS.values():
        event_ts = _numeric(frame, timestamp_col)
        invalid = event_ts.notna() & event_ts.gt(0.0) & event_ts.lt(decision_ts)
        if invalid.any():
            raise ValueError(
                f"{timestamp_col} precedes the decision on {int(invalid.sum())} rows"
            )
    validate_feature_provenance(frame, feature_specs=feature_specs)


def add_first_mid_hit_labels(
    frame: pd.DataFrame,
    *,
    bbo_dir: Path,
    symbol: str,
    tick_size: float,
    horizon_ms: int,
) -> pd.DataFrame:
    """Label the first one-tick mid move after each decision from historical BBO."""

    if frame.empty:
        raise ValueError("first-hit source frame is empty")
    if tick_size <= 0.0 or horizon_ms <= 0:
        raise ValueError("first-hit tick_size and horizon_ms must be positive")
    required = {
        "day",
        "decision_ts_ns",
        "best_bid",
        "best_ask",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"first-hit source missing columns: {missing}")

    output = frame.reset_index(drop=True).copy()
    direction = np.zeros(len(output), dtype=np.int8)
    hit_ts_ns = np.zeros(len(output), dtype=np.int64)
    censored = np.ones(len(output), dtype=np.int8)
    bbo_root = Path(bbo_dir).expanduser().resolve()

    for day, day_index in output.groupby("day", sort=True).groups.items():
        bbo_path = bbo_root / f"{symbol}-bbo-{day}.parquet"
        if not bbo_path.exists():
            raise FileNotFoundError(f"first-hit BBO is missing: {bbo_path}")
        bbo = pd.read_parquet(
            bbo_path,
            columns=[
                "timestamp",
                "best_bid",
                "best_ask",
            ],
        )
        if bbo.empty:
            raise ValueError(f"first-hit BBO is empty: {bbo_path}")
        timestamps_ms = pd.to_numeric(
            bbo["timestamp"], errors="coerce"
        ).to_numpy(dtype=np.int64)
        best_bid = pd.to_numeric(
            bbo["best_bid"], errors="coerce"
        ).to_numpy(dtype=float)
        best_ask = pd.to_numeric(
            bbo["best_ask"], errors="coerce"
        ).to_numpy(dtype=float)
        if (
            not np.isfinite(best_bid).all()
            or not np.isfinite(best_ask).all()
            or (best_bid <= 0.0).any()
            or (best_ask <= best_bid).any()
            or (np.diff(timestamps_ms) < 0).any()
        ):
            raise ValueError(f"first-hit BBO is invalid: {bbo_path}")
        mids = 0.5 * (best_bid + best_ask)

        positions = np.asarray(list(day_index), dtype=np.int64)
        decisions_ms = (
            pd.to_numeric(
                output.loc[positions, "decision_ts_ns"],
                errors="coerce",
            ).to_numpy(dtype=np.int64)
            // 1_000_000
        )
        decision_mid = 0.5 * (
            pd.to_numeric(
                output.loc[positions, "best_bid"], errors="coerce"
            ).to_numpy(dtype=float)
            + pd.to_numeric(
                output.loc[positions, "best_ask"], errors="coerce"
            ).to_numpy(dtype=float)
        )
        starts = np.searchsorted(
            timestamps_ms,
            decisions_ms,
            side="right",
        )
        ends = np.searchsorted(
            timestamps_ms,
            decisions_ms + int(horizon_ms),
            side="right",
        )
        complete_horizon = decisions_ms + int(horizon_ms) <= timestamps_ms[-1]
        for position, start, end, mid, complete in zip(  # noqa: B905
            positions,
            starts,
            ends,
            decision_mid,
            complete_horizon,
        ):
            if not complete or start >= end:
                continue
            censored[position] = 0
            path = mids[start:end]
            up = np.flatnonzero(path >= mid + float(tick_size) - 1e-12)
            down = np.flatnonzero(path <= mid - float(tick_size) + 1e-12)
            up_index = int(up[0]) if up.size else len(path) + 1
            down_index = int(down[0]) if down.size else len(path) + 1
            if up_index < down_index:
                direction[position] = 1
                hit_ts_ns[position] = int(
                    timestamps_ms[start + up_index]
                ) * 1_000_000
            elif down_index < up_index:
                direction[position] = -1
                hit_ts_ns[position] = int(
                    timestamps_ms[start + down_index]
                ) * 1_000_000

    output["future_mid_first_hit_direction"] = direction
    output["future_mid_first_hit_ts_ns"] = hit_ts_ns
    output["future_mid_first_hit_censored"] = censored
    output["future_mid_first_hit_horizon_ms"] = int(horizon_ms)
    output["future_mid_first_hit_ticks"] = 1.0
    output["future_mid_first_hit_source"] = (
        "historical_bbo_exchange_time_strictly_after_decision"
    )
    return output


def _native_exchange_mid_path(
    *,
    raw_root: Path,
    day: str,
    symbol: str,
    tick_size: float,
    exchange: str,
    warmup_hours: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Reconstruct exchange-time mid changes from the policy-blind scheduler."""

    from models.exchange_book_replay import (
        CryptoHFTExchangeBookTape,
        HistoricalExchangeBookScheduler,
    )

    tape = CryptoHFTExchangeBookTape(
        raw_root=Path(raw_root),
        day=str(day),
        symbol=str(symbol),
        tick_size=float(tick_size),
        exchange=str(exchange),
        warmup_hours=int(warmup_hours),
        strict_complete=True,
    )
    scheduler = HistoricalExchangeBookScheduler(
        tape,
        strict_sequence=True,
        allow_delta_bootstrap=True,
        track_mid_changes=True,
        mid_change_start_ns=tape.day_start_ns,
    )
    scheduler.advance_to(
        tape.day_end_ns - 1,
        inclusive=True,
        emitted_levels=set(),
    )
    mid_changes = scheduler.mid_changes

    stats = scheduler.stats()
    if (
        stats.source_gap_events
        or stats.sequence_gaps
        or stats.invalid_sequence_messages
        or stats.message_time_reversals
        or stats.receive_timestamp_fallback_events
        or stats.unknown_timestamp_source_events
    ):
        raise ValueError(
            f"native first-hit path failed source integrity for {day}: "
            f"{asdict(stats)}"
        )
    return (
        np.asarray(
            [timestamp for timestamp, _ in mid_changes],
            dtype=np.int64,
        ),
        np.asarray(
            [mid_tick * tick_size for _, mid_tick in mid_changes],
            dtype=float,
        ),
    )


def _native_exchange_mid_path_task(
    task: tuple[Path, str, str, float, str, int],
) -> tuple[str, np.ndarray, np.ndarray]:
    raw_root, day, symbol, tick_size, exchange, warmup_hours = task
    timestamps_ns, mids = _native_exchange_mid_path(
        raw_root=raw_root,
        day=day,
        symbol=symbol,
        tick_size=tick_size,
        exchange=exchange,
        warmup_hours=warmup_hours,
    )
    return day, timestamps_ns, mids


def add_native_first_mid_hit_labels(
    frame: pd.DataFrame,
    *,
    raw_root: Path,
    symbol: str,
    tick_size: float,
    horizon_ms: int,
    exchange: str = "binance_futures",
    warmup_hours: int = 24,
    workers: int = 1,
) -> pd.DataFrame:
    """Label first one-tick mid hits from native snapshot/delta exchange time."""

    if frame.empty:
        raise ValueError("native first-hit source frame is empty")
    if tick_size <= 0.0 or horizon_ms <= 0:
        raise ValueError(
            "native first-hit tick_size and horizon_ms must be positive"
        )
    required = {"day", "decision_ts_ns", "best_bid", "best_ask"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(
            f"native first-hit source missing columns: {missing}"
        )

    output = frame.reset_index(drop=True).copy()
    direction = np.zeros(len(output), dtype=np.int8)
    hit_ts_ns = np.zeros(len(output), dtype=np.int64)
    censored = np.ones(len(output), dtype=np.int8)
    horizon_ns = int(horizon_ms) * 1_000_000

    days = sorted(output["day"].astype(str).unique())
    tasks = [
        (
            Path(raw_root),
            day,
            str(symbol),
            float(tick_size),
            str(exchange),
            int(warmup_hours),
        )
        for day in days
    ]
    day_paths: dict[str, tuple[np.ndarray, np.ndarray]] = {}
    if int(workers) <= 1:
        path_results = map(_native_exchange_mid_path_task, tasks)
        for day, timestamps_ns, mids in path_results:
            day_paths[day] = (timestamps_ns, mids)
    else:
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=int(workers)
        ) as executor:
            for day, timestamps_ns, mids in executor.map(
                _native_exchange_mid_path_task,
                tasks,
            ):
                day_paths[day] = (timestamps_ns, mids)

    for day, day_index in output.groupby("day", sort=True).groups.items():
        timestamps_ns, mids = day_paths[str(day)]
        day_start_ns = int(pd.Timestamp(str(day), tz="UTC").value)
        day_end_ns = day_start_ns + 86_400_000_000_000
        positions = np.asarray(list(day_index), dtype=np.int64)
        decisions_ns = pd.to_numeric(
            output.loc[positions, "decision_ts_ns"],
            errors="coerce",
        ).to_numpy(dtype=np.int64)
        decision_mid = 0.5 * (
            pd.to_numeric(
                output.loc[positions, "best_bid"],
                errors="coerce",
            ).to_numpy(dtype=float)
            + pd.to_numeric(
                output.loc[positions, "best_ask"],
                errors="coerce",
            ).to_numpy(dtype=float)
        )
        starts = np.searchsorted(
            timestamps_ns,
            decisions_ns,
            side="right",
        )
        ends = np.searchsorted(
            timestamps_ns,
            decisions_ns + horizon_ns,
            side="right",
        )
        complete_horizon = decisions_ns + horizon_ns <= day_end_ns
        for position, start, end, mid, complete in zip(  # noqa: B905
            positions,
            starts,
            ends,
            decision_mid,
            complete_horizon,
        ):
            if not complete:
                continue
            censored[position] = 0
            if start >= end:
                continue
            path = mids[start:end]
            up = np.flatnonzero(
                path >= mid + float(tick_size) - 1e-12
            )
            down = np.flatnonzero(
                path <= mid - float(tick_size) + 1e-12
            )
            up_index = int(up[0]) if up.size else len(path) + 1
            down_index = int(down[0]) if down.size else len(path) + 1
            if up_index < down_index:
                direction[position] = 1
                hit_ts_ns[position] = int(
                    timestamps_ns[start + up_index]
                )
            elif down_index < up_index:
                direction[position] = -1
                hit_ts_ns[position] = int(
                    timestamps_ns[start + down_index]
                )

    output["future_mid_first_hit_direction"] = direction
    output["future_mid_first_hit_ts_ns"] = hit_ts_ns
    output["future_mid_first_hit_censored"] = censored
    output["future_mid_first_hit_horizon_ms"] = int(horizon_ms)
    output["future_mid_first_hit_ticks"] = 1.0
    output["future_mid_first_hit_source"] = (
        "native_snapshot_delta_exchange_time_strictly_after_decision"
    )
    return output


def _event_candidates(
    row: pd.Series,
    *,
    favorable_fill_threshold_bps: float,
) -> list[tuple[int, int, str]]:
    def event_int(name: str) -> int:
        value = row.get(name, 0)
        if value is None or pd.isna(value):
            return 0
        return int(value)

    decision_ts = int(row["decision_ts_ns"])
    censor_ts = int(row["censor_ts_ns"])
    candidates: list[tuple[int, int, str]] = []

    fill_ts = event_int("fill_ts_ns")
    if fill_ts > 0 and decision_ts <= fill_ts <= censor_ts:
        markout = float(row.get("fill_value_markout_bps", math.nan))
        if not math.isfinite(markout):
            raise ValueError(
                f"{row['decision_id']}: fill requires fill_value_markout_bps"
            )
        event = (
            "favorable_fill"
            if markout >= favorable_fill_threshold_bps
            else "adverse_fill"
        )
        candidates.append(
            (fill_ts, event_int("fill_event_seq"), event)
        )

    for event, timestamp_col in (
        ("cancel", "cancel_ack_ts_ns"),
        ("adverse_price_jump", "adverse_price_jump_ts_ns"),
        ("campaign_repair", "repair_ts_ns"),
    ):
        timestamp = event_int(timestamp_col)
        if timestamp > 0 and decision_ts <= timestamp <= censor_ts:
            sequence = event_int(f"{event}_event_seq")
            candidates.append((timestamp, sequence, event))
    return candidates


def add_competing_risk_labels(
    frame: pd.DataFrame,
    *,
    favorable_fill_threshold_bps: float = 0.0,
    feature_specs: Sequence[PanelFeatureSpec] = DEFAULT_FEATURE_SPECS,
) -> pd.DataFrame:
    """Attach a mutually exclusive first-event label to every observation."""

    validate_order_observations(frame, feature_specs=feature_specs)
    output = frame.copy()
    fill_horizon_censored = (
        _numeric(output, "fill_value_horizon_censored", 0.0)
        .fillna(0.0)
        .astype(bool)
    )
    fill_ts = _numeric(output, "fill_ts_ns", 0.0).fillna(0.0)
    unknown_fill = fill_horizon_censored & fill_ts.gt(0.0)
    output["label_censor_reason"] = ""
    if unknown_fill.any():
        output.loc[unknown_fill, "censor_ts_ns"] = np.minimum(
            _numeric(output.loc[unknown_fill], "censor_ts_ns").to_numpy(
                dtype=np.int64
            ),
            fill_ts.loc[unknown_fill].to_numpy(dtype=np.int64),
        )
        output.loc[unknown_fill, "fill_ts_ns"] = 0
        output.loc[unknown_fill, "label_censor_reason"] = (
            "fill_value_horizon_right_censored"
        )
    first_events: list[str] = []
    first_event_ts: list[int] = []
    label_reasons: list[str] = []
    for _, row in output.iterrows():
        label_reason = str(row.get("label_censor_reason", "") or "")
        candidates = _event_candidates(
            row,
            favorable_fill_threshold_bps=favorable_fill_threshold_bps,
        )
        if not candidates:
            first_events.append("censored")
            first_event_ts.append(int(row["censor_ts_ns"]))
            label_reasons.append(label_reason)
            continue
        candidates.sort(key=lambda item: (item[0], item[1], EVENT_CODE[item[2]]))
        earliest_ts = candidates[0][0]
        tied = [item for item in candidates if item[0] == earliest_ts]
        tied_sequences = [item[1] for item in tied]
        if (
            len(tied) > 1
            and (
                any(sequence <= 0 for sequence in tied_sequences)
                or len(set(tied_sequences)) != len(tied_sequences)
            )
        ):
            first_event_ts.append(int(earliest_ts))
            first_events.append("censored")
            label_reasons.append(
                "same_ms_competing_event_ambiguous"
            )
            continue
        first_event_ts.append(int(candidates[0][0]))
        first_events.append(str(candidates[0][2]))
        label_reasons.append(label_reason)

    output["schema_version"] = SCHEMA_VERSION
    output["label_censor_reason"] = label_reasons
    output["first_event"] = first_events
    output["first_event_code"] = [EVENT_CODE[event] for event in first_events]
    output["first_event_ts_ns"] = np.asarray(first_event_ts, dtype=np.int64)
    output["event_time_ms"] = (
        output["first_event_ts_ns"].astype(np.int64)
        - output["decision_ts_ns"].astype(np.int64)
    ) / 1_000_000.0
    output["event_observed"] = (
        output["first_event"].astype(str) != "censored"
    ).astype(int)
    for event in EVENT_TYPES:
        output[f"event_{event}"] = (
            output["first_event"].astype(str) == event
        ).astype(int)
    output["fill_value_threshold_bps"] = float(
        favorable_fill_threshold_bps
    )
    # This label intentionally mixes market-path outcomes with baseline-policy
    # and campaign transitions.  Native future-mid first-hit is an audit field;
    # it is not the adverse-jump event used by this competing-risk label.
    output["label_identity"] = COMPETING_RISK_LABEL_IDENTITY
    output["adverse_price_jump_timestamp_source"] = (
        "adverse_price_jump_ts_ns"
    )
    output["native_future_mid_first_hit_used_in_competing_risk"] = 0
    output["cancel_event_role"] = "baseline_policy_action_or_censor"
    output["campaign_repair_event_role"] = "post_fill_campaign_transition"
    output["competing_risk_action_independent"] = 0
    return output


def validate_randomized_action_panel(
    frame: pd.DataFrame,
    *,
    actions: Iterable[str] = ("keep", "cancel_until_state_exit"),
) -> None:
    """Validate the stricter one-campaign-one-intervention action panel."""

    required = {
        "day",
        "decision_id",
        "campaign_id",
        "order_id",
        "action",
        "behavior_propensity",
        "reward",
        "fill_value",
        "campaign_cost",
        "queue_cost",
        "reward_identity_error",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"queue-value action panel missing columns: {missing}")
    if frame.groupby(["day", "campaign_id"], sort=False).size().max() != 1:
        raise ValueError("each campaign may contain at most one intervention")
    registered = tuple(str(action) for action in actions)
    if set(frame["action"].astype(str)) - set(registered):
        raise ValueError("action panel contains an unregistered action")
    probabilities = frame[
        [f"behavior_prob_{action}" for action in registered]
    ].apply(pd.to_numeric, errors="coerce")
    if not np.isfinite(probabilities.to_numpy(dtype=float)).all():
        raise ValueError("behavior probabilities must be finite")
    if not np.allclose(
        probabilities.sum(axis=1), 1.0, atol=1e-10, rtol=0.0
    ):
        raise ValueError("behavior probabilities must sum to one")
    selected_index = {action: index for index, action in enumerate(registered)}
    logged = np.asarray(
        [selected_index[action] for action in frame["action"].astype(str)],
        dtype=int,
    )
    expected = probabilities.to_numpy(dtype=float)[np.arange(len(frame)), logged]
    supplied = _numeric(frame, "behavior_propensity").to_numpy(dtype=float)
    if not np.allclose(expected, supplied, atol=1e-10, rtol=0.0):
        raise ValueError("behavior_propensity does not match the logged vector")
    identity = _numeric(frame, "reward") - (
        _numeric(frame, "fill_value")
        - _numeric(frame, "campaign_cost")
        - _numeric(frame, "queue_cost")
    )
    if identity.abs().max() > 1e-9:
        raise ValueError("reward decomposition identity failed")


def _read_frame(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    return pd.read_csv(path)


def write_panel(
    frame: pd.DataFrame,
    output: Path,
    *,
    source_path: Path | None = None,
) -> dict[str, Any]:
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.suffix.lower() in {".parquet", ".pq"}:
        frame.to_parquet(output, index=False)
    else:
        frame.to_csv(output, index=False)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "rows": int(len(frame)),
        "days": int(frame["day"].astype(str).nunique()),
        "orders": int(frame["order_id"].astype(str).nunique()),
        "campaigns": int(
            frame[["day", "campaign_id"]].drop_duplicates().shape[0]
        ),
        "event_counts": {
            str(key): int(value)
            for key, value in frame["first_event"].value_counts().sort_index().items()
        },
        "source": str(source_path) if source_path is not None else "",
        "source_sha256": (
            _sha256(source_path)
            if source_path is not None and source_path.exists()
            else ""
        ),
        "output": str(output),
        "output_sha256": _sha256(output),
        "feature_specs": [asdict(spec) for spec in DEFAULT_FEATURE_SPECS],
        "simulator_audit_specs": [
            asdict(spec) for spec in SIMULATOR_AUDIT_SPECS
        ],
    }
    summary_path = output.with_suffix(output.suffix + ".summary.json")
    summary_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return summary


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--favorable-fill-threshold-bps", type=float, default=0.0)
    parser.add_argument("--bbo-dir", type=Path, default=None)
    parser.add_argument("--symbol", default="BTCUSDC")
    parser.add_argument("--first-hit-horizon-ms", type=int, default=1_000)
    parser.add_argument("--tick-size", type=float, default=0.1)
    parser.add_argument("--exchange-book-raw-root", type=Path, default=None)
    parser.add_argument(
        "--exchange-book-exchange",
        default="binance_futures",
    )
    parser.add_argument("--exchange-book-warmup-hours", type=int, default=24)
    parser.add_argument("--first-hit-workers", type=int, default=1)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    source = args.input.expanduser().resolve()
    output = args.output.expanduser().resolve()
    source_frame = _read_frame(source)
    if args.bbo_dir is not None and args.exchange_book_raw_root is not None:
        raise SystemExit(
            "--bbo-dir and --exchange-book-raw-root are mutually exclusive"
        )
    if args.exchange_book_raw_root is not None:
        source_frame = add_native_first_mid_hit_labels(
            source_frame,
            raw_root=args.exchange_book_raw_root.expanduser().resolve(),
            symbol=str(args.symbol),
            tick_size=float(args.tick_size),
            horizon_ms=int(args.first_hit_horizon_ms),
            exchange=str(args.exchange_book_exchange),
            warmup_hours=int(args.exchange_book_warmup_hours),
            workers=int(args.first_hit_workers),
        )
    if args.bbo_dir is not None:
        source_frame = add_first_mid_hit_labels(
            source_frame,
            bbo_dir=args.bbo_dir,
            symbol=str(args.symbol),
            tick_size=float(args.tick_size),
            horizon_ms=int(args.first_hit_horizon_ms),
        )
    panel = add_competing_risk_labels(
        source_frame,
        favorable_fill_threshold_bps=args.favorable_fill_threshold_bps,
    )
    summary = write_panel(panel, output, source_path=source)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
