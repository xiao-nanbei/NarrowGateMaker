"""Build one native-deep paired placement lifecycle smoke panel.

The input traces come from one frozen baseline replay.  This second pass does
not run quote policy and does not feed shadow fills back into inventory.  Its
only estimand is action-specific activation plus direct fill CIF under a
frozen baseline follow-up schedule.
"""

from __future__ import annotations

import argparse
import bisect
import hashlib
import json
import shutil
from collections import defaultdict
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from models import backtest_tick as bt
from models.exchange_book_replay import (
    CryptoHFTExchangeBookTape,
    HistoricalExchangeBookScheduler,
)
from research.families.f06_placement_fill_cif import FAMILY_DOCS
from research.families.f06_placement_fill_cif.audit.paired_order_lifecycle import (
    SCHEMA_VERSION,
    PlacementCohort,
)

ROOT = Path(__file__).resolve().parents[4]
DEFAULT_SPEC = FAMILY_DOCS / "paired_state_fill_surface_v1_spec_20260726.json"
DECISION_FEATURE_COLUMNS = (
    "mode",
    "allow_post",
    "allow_exposure_increase",
    "exposure_increasing",
    "reason_text",
    "inventory",
    "inventory_ratio",
    "campaign_active",
    "campaign_side",
    "campaign_age_s",
    "campaign_max_abs_qty_so_far",
    "campaign_pnl_so_far",
    "campaign_adverse_excursion_so_far",
    "campaign_exposure_increasing_fills_so_far",
    "campaign_reducing_fills_so_far",
    "toxicity",
    "markout_ema",
    "depth_age_s",
    "pred_dir",
    "pred_ret",
    "sigma_sq_raw",
    "sigma_sq_blended",
    "quote_horizon_s",
    "kappa_used",
    "microprice_shift_bps",
    "l2_quote_flip_rate",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
    "l2_near_depth_total",
    "mid",
    "best_bid",
    "best_ask",
    "final_quote_delta_to_bbo",
    "final_distance_to_mid",
    "final_pair_spread",
    "final_quote_skew",
    "side_adverse_pause",
    "defense_guard",
    "defense_pause",
    "local_extreme_pause",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _file_identity(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(resolved)
    return {
        "path": str(resolved),
        "size_bytes": int(resolved.stat().st_size),
        "sha256": _sha256(resolved),
    }


def _individual_trade_identity(symbol: str, day: str) -> dict[str, Any]:
    trade_dir = bt.RAW_TRADES_DIR / str(symbol)
    candidates = [
        path
        for suffix in ("csv", "csv.gz")
        if (path := trade_dir / f"{symbol}-trades-{day}.{suffix}").is_file()
    ]
    if len(candidates) != 1:
        raise RuntimeError(
            "expected exactly one individual-trade source for "
            f"{symbol} {day}, found {len(candidates)}"
        )
    return _file_identity(candidates[0])


def _event_ns(group: pd.DataFrame, event_type: str) -> int:
    rows = group.loc[group["event_type"].astype(str) == event_type]
    if rows.empty:
        return 0
    return int(rows["event_ts_ns"].min())


def _event_reason(group: pd.DataFrame, event_type: str) -> str:
    rows = group.loc[group["event_type"].astype(str) == event_type]
    if rows.empty or "event_reason" not in rows:
        return ""
    ordered = rows.sort_values("event_seq", kind="stable")
    return str(ordered.iloc[0].get("event_reason", "") or "")


def _first_by_order(frame: pd.DataFrame) -> dict[int, dict[str, Any]]:
    if frame.empty or "order_id" not in frame:
        return {}
    ordered = frame.sort_values(
        ["order_id", "outcome_ts"]
        if "outcome_ts" in frame
        else ["order_id"],
        kind="stable",
    )
    return {
        int(order_id): group.iloc[0].to_dict()
        for order_id, group in ordered.groupby("order_id", sort=False)
    }


def build_cohorts(
    decisions: pd.DataFrame,
    lifecycle: pd.DataFrame,
    quotes: pd.DataFrame,
    *,
    day: str,
    tick_size: float,
    lot_size: float,
    max_cohorts: int,
    max_horizon_ms: int,
) -> tuple[list[PlacementCohort], dict[str, int]]:
    """Join baseline submit events to causal side-decision state."""

    required_lifecycle = {
        "order_id",
        "side",
        "event_type",
        "event_ts_ns",
        "order_price",
        "order_qty",
        "inventory_role",
        "campaign_id",
    }
    missing = required_lifecycle - set(lifecycle.columns)
    if missing:
        raise ValueError(f"lifecycle trace missing columns: {sorted(missing)}")
    if tick_size <= 0.0 or lot_size <= 0.0:
        raise ValueError("tick_size and lot_size must be positive")
    if max_cohorts <= 0 or max_horizon_ms <= 0:
        raise ValueError("max_cohorts and max_horizon_ms must be positive")

    decision_map: dict[tuple[int, str], dict[str, Any]] = {}
    for row in decisions.to_dict("records"):
        ts_ns = int(row.get("decision_ts_ns", 0) or 0)
        side = str(row.get("side", "")).upper()
        if ts_ns > 0 and side in {"BUY", "SELL"}:
            decision_map[(ts_ns, side)] = row
    quote_map = _first_by_order(quotes)
    day_end_ns = int(
        (pd.Timestamp(day, tz="UTC") + pd.Timedelta(days=1)).value
    )
    counters: defaultdict[str, int] = defaultdict(int)
    cohorts: list[PlacementCohort] = []
    grouped = lifecycle.groupby("order_id", sort=False)
    ordered_groups = sorted(
        grouped,
        key=lambda item: int(item[1]["event_ts_ns"].min()),
    )
    for order_id, group in ordered_groups:
        submit_rows = group.loc[group["event_type"].astype(str) == "submit"]
        if submit_rows.empty:
            counters["missing_submit"] += 1
            continue
        submit = submit_rows.sort_values("event_seq", kind="stable").iloc[0]
        submit_ns = int(submit["event_ts_ns"])
        side = str(submit["side"]).upper()
        quote = quote_map.get(int(order_id), {})
        if bool(submit.get("circuit_breaker_close", False)) or bool(
            quote.get("circuit_breaker_close", False)
        ):
            counters["excluded_circuit_breaker_close"] += 1
            continue
        decision = decision_map.get((submit_ns, side))
        if decision is None:
            counters["missing_decision"] += 1
            continue
        if str(decision.get("action", "")) not in {"place", "replace"}:
            counters["non_placement_decision"] += 1
            continue
        ready_ns = int(decision.get("decision_ts_ns", submit_ns) or submit_ns)
        if ready_ns > submit_ns:
            counters["future_feature"] += 1
            continue

        activate_ns = _event_ns(group, "activate")
        if activate_ns <= 0:
            activate_ms = int(quote.get("activate_ts", 0) or 0)
            activate_ns = activate_ms * 1_000_000
        if activate_ns <= 0:
            reject_ns = _event_ns(group, "reject")
            activate_ns = reject_ns
        if activate_ns <= 0:
            counters["missing_activation_boundary"] += 1
            continue

        cancel_request_ns = _event_ns(group, "cancel_request")
        cancel_request_reason = _event_reason(group, "cancel_request")
        cancel_ack_ns = _event_ns(group, "cancel_ack")
        fixed_end_ns = activate_ns + int(max_horizon_ms) * 1_000_000
        observation_end_ns = min(
            day_end_ns,
            max(fixed_end_ns, cancel_ack_ns),
        )
        features = {
            name: decision.get(name)
            for name in DECISION_FEATURE_COLUMNS
            if name in decision
        }
        depth_age_s = max(0.0, float(decision.get("depth_age_s", 0.0) or 0.0))
        features.update(
            {
                "feature_ready_ts_ns": int(submit_ns),
                "feature_source_ts_ns": int(
                    submit_ns - round(depth_age_s * 1_000_000_000.0)
                ),
                "baseline_order_id": int(order_id),
                "baseline_action": str(decision.get("action", "")),
                "baseline_queue_deplete_mult": float(
                    quote.get("queue_deplete_mult", 1.0) or 1.0
                ),
                "baseline_cancel_followup_observed": int(
                    cancel_ack_ns > 0
                ),
            }
        )
        price = float(submit["order_price"])
        quantity = float(submit["order_qty"])
        cohort = PlacementCohort.create(
            cohort_id=f"{day}:{int(order_id)}",
            decision_id=str(decision.get("decision_id", "")),
            day=day,
            side=side,
            inventory_role=str(submit.get("inventory_role", "unknown")),
            campaign_id=int(submit.get("campaign_id", 0) or 0),
            submit_ts_ns=submit_ns,
            activate_ts_ns=activate_ns,
            cancel_request_ts_ns=cancel_request_ns,
            cancel_ack_ts_ns=cancel_ack_ns,
            observation_end_ts_ns=observation_end_ns,
            baseline_price_tick=int(round(price / tick_size)),
            quantity=quantity,
            queue_deplete_mult=float(
                quote.get("queue_deplete_mult", 1.0) or 1.0
            ),
            lot_size=lot_size,
            decision_features=features,
            cancel_request_reason=cancel_request_reason,
        )
        cohorts.append(cohort)
        if len(cohorts) >= int(max_cohorts):
            break
    counters["cohorts"] = len(cohorts)
    return cohorts, dict(counters)


def _group_by_ms(cohorts: list[PlacementCohort], field: str) -> dict[int, list[PlacementCohort]]:
    grouped: defaultdict[int, list[PlacementCohort]] = defaultdict(list)
    for cohort in cohorts:
        ts_ns = int(getattr(cohort, field))
        if ts_ns > 0:
            grouped[ts_ns // 1_000_000].append(cohort)
    return dict(grouped)


def simulate_paired_placements(
    cohorts: list[PlacementCohort],
    *,
    tape: CryptoHFTExchangeBookTape,
    trades: pd.DataFrame,
    tick_size: float,
    fail_on_monotonicity: bool = True,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not cohorts:
        raise ValueError("paired smoke requires at least one cohort")
    required_trades = {"transact_time", "price", "quantity", "is_buyer_maker"}
    missing = required_trades - set(trades.columns)
    if missing:
        raise ValueError(f"trade tape missing columns: {sorted(missing)}")

    start_ms = min(cohort.submit_ts_ns for cohort in cohorts) // 1_000_000
    end_ms = max(cohort.observation_end_ts_ns for cohort in cohorts) // 1_000_000
    scoped = trades.loc[
        (trades["transact_time"].astype(np.int64) >= start_ms)
        & (trades["transact_time"].astype(np.int64) <= end_ms)
    ].copy()
    scoped.sort_values(["transact_time", "trade_id"] if "trade_id" in scoped else ["transact_time"], kind="stable", inplace=True)
    scoped.reset_index(drop=True, inplace=True)
    scoped_ts = scoped["transact_time"].to_numpy(dtype=np.int64, copy=False)
    unique_trade_ms, trade_starts, trade_counts = np.unique(
        scoped_ts,
        return_index=True,
        return_counts=True,
    )
    trade_slices = {
        int(unique_trade_ms[index]): (
            int(trade_starts[index]),
            int(trade_starts[index] + trade_counts[index]),
        )
        for index in range(len(unique_trade_ms))
    }

    scheduler = HistoricalExchangeBookScheduler(
        tape,
        strict_sequence=True,
        strict_after_ns=int(tape.day_start_ns),
        allow_delta_bootstrap=False,
    )
    emitted_levels = {
        ("bid" if cohort.side == "BUY" else "ask", child.price_tick)
        for cohort in cohorts
        for child in cohort.children.values()
    }
    active_children_by_level: dict[tuple[str, int], dict[int, Any]] = {}
    active_ticks: dict[str, list[int]] = {"bid": [], "ask": []}
    cohorts_by_id = {cohort.cohort_id: cohort for cohort in cohorts}
    child_parent: dict[int, PlacementCohort] = {}
    for cohort in cohorts:
        for child in cohort.children.values():
            child_parent[id(child)] = cohort

    def register_child(native_side: str, child: Any) -> None:
        if not child.active:
            return
        key = (native_side, int(child.price_tick))
        bucket = active_children_by_level.get(key)
        if bucket is None:
            bucket = {}
            active_children_by_level[key] = bucket
            bisect.insort(active_ticks[native_side], int(child.price_tick))
        bucket[id(child)] = child

    def unregister_child(native_side: str, child: Any) -> None:
        key = (native_side, int(child.price_tick))
        bucket = active_children_by_level.get(key)
        if bucket is None:
            return
        bucket.pop(id(child), None)
        if bucket:
            return
        active_children_by_level.pop(key, None)
        ticks = active_ticks[native_side]
        index = bisect.bisect_left(ticks, int(child.price_tick))
        if index < len(ticks) and ticks[index] == int(child.price_tick):
            ticks.pop(index)

    def invalidate_active_children(reason: str) -> None:
        for bucket in active_children_by_level.values():
            for child in bucket.values():
                child.invalidate_native_path(reason)

    activations = _group_by_ms(cohorts, "activate_ts_ns")
    cancel_requests = _group_by_ms(cohorts, "cancel_request_ts_ns")
    cancel_acks = _group_by_ms(cohorts, "cancel_ack_ts_ns")
    observation_ends = _group_by_ms(cohorts, "observation_end_ts_ns")
    scheduled_times = sorted(
        set(activations)
        | set(cancel_requests)
        | set(cancel_acks)
        | set(observation_ends)
        | set(trade_slices)
    )
    schedule_index = 0
    scheduler.advance_to(
        int(start_ms) * 1_000_000,
        inclusive=False,
        emitted_levels=emitted_levels,
    )
    processed_native_boundaries = 0
    processed_trade_rows = 0
    while True:
        next_scheduled = (
            scheduled_times[schedule_index]
            if schedule_index < len(scheduled_times)
            else None
        )
        next_native_ns = scheduler.next_exchange_ts_ns
        next_native = (
            int(next_native_ns) // 1_000_000
            if next_native_ns is not None
            and int(next_native_ns) // 1_000_000 <= end_ms
            else None
        )
        candidates = [
            value
            for value in (next_scheduled, next_native)
            if value is not None and value <= end_ms
        ]
        if not candidates:
            break
        now_ms = min(candidates)
        if next_scheduled == now_ms:
            schedule_index += 1
        now_ns = int(now_ms) * 1_000_000
        trade_slice = trade_slices.get(now_ms)
        same_ms_trades = (
            scoped.iloc[trade_slice[0] : trade_slice[1]]
            if trade_slice is not None
            else None
        )

        before = scheduler.advance_to(
            now_ns,
            inclusive=False,
            emitted_levels=emitted_levels,
        )
        if before.snapshot_reset or before.invalidated:
            reason = "native_sequence_invalidated" if before.invalidated else "native_snapshot_reset"
            invalidate_active_children(reason)
        for change in before.level_changes:
            for child in active_children_by_level.get(
                (change.side, change.price_tick), {}
            ).values():
                child.apply_level_change(
                    change,
                    ambiguous_with_trade_or_activation=False,
                )

        for cohort in cancel_requests.get(now_ms, ()):
            for child in cohort.children.values():
                child.request_cancel(now_ns)
        for cohort in activations.get(now_ms, ()):
            preview = scheduler.preview_at(now_ns)
            bids, asks = scheduler.top_levels(1)
            best_bid_tick = int(bids[0][0]) if bids else 0
            best_ask_tick = int(asks[0][0]) if asks else 0
            native_side = "bid" if cohort.side == "BUY" else "ask"
            for child in cohort.children.values():
                child.activate(
                    lookup=scheduler.lookup(native_side, child.price_tick),
                    best_bid_tick=best_bid_tick,
                    best_ask_tick=best_ask_tick,
                    same_boundary_native_event=bool(
                        preview.snapshot_or_gap
                        or (native_side, child.price_tick)
                        in preview.touched_levels
                    ),
                )
                register_child(native_side, child)

        advance = scheduler.advance_to(
            now_ns,
            inclusive=True,
            emitted_levels=emitted_levels,
        )
        processed_native_boundaries += int(
            advance.accepted_events + advance.rejected_events > 0
        )
        if advance.snapshot_reset or advance.invalidated:
            reason = "native_sequence_invalidated" if advance.invalidated else "native_snapshot_reset"
            invalidate_active_children(reason)
        activation_children = {
            id(child)
            for cohort in activations.get(now_ms, ())
            for child in cohort.children.values()
        }
        for change in advance.level_changes:
            for child in active_children_by_level.get(
                (change.side, change.price_tick), {}
            ).values():
                child.apply_level_change(
                    change,
                    ambiguous_with_trade_or_activation=bool(
                        same_ms_trades is not None
                        or id(child) in activation_children
                    ),
                )

        touched_cohorts: set[str] = set()
        if same_ms_trades is not None:
            for trade in same_ms_trades.itertuples(index=False):
                trade_tick = int(round(float(trade.price) / tick_size))
                is_buyer_maker = bool(trade.is_buyer_maker)
                if is_buyer_maker:
                    exact_ticks = [trade_tick]
                    buy_ticks = active_ticks["bid"]
                    through_ticks = buy_ticks[
                        bisect.bisect_right(buy_ticks, trade_tick) :
                    ]
                    native_side = "bid"
                else:
                    exact_ticks = [trade_tick]
                    sell_ticks = active_ticks["ask"]
                    through_ticks = sell_ticks[
                        : bisect.bisect_left(sell_ticks, trade_tick)
                    ]
                    native_side = "ask"
                for tick in (*exact_ticks, *through_ticks):
                    children = list(
                        active_children_by_level.get(
                            (native_side, tick), {}
                        ).values()
                    )
                    for child in children:
                        before_fill = child.fill_qty
                        child.apply_trade(
                            ts_ns=now_ns,
                            trade_price_tick=trade_tick,
                            trade_qty=float(trade.quantity),
                            is_buyer_maker=is_buyer_maker,
                        )
                        if child.fill_qty != before_fill:
                            touched_cohorts.add(child_parent[id(child)].cohort_id)
                        if not child.active:
                            unregister_child(native_side, child)
                processed_trade_rows += 1
            for cohort_id in touched_cohorts:
                cohorts_by_id[cohort_id].check_monotonicity()

        for cohort in cancel_acks.get(now_ms, ()):
            native_side = "bid" if cohort.side == "BUY" else "ask"
            for child in cohort.children.values():
                child.acknowledge_cancel(now_ns)
                if not child.active:
                    unregister_child(native_side, child)
        for cohort in observation_ends.get(now_ms, ()):
            native_side = "bid" if cohort.side == "BUY" else "ask"
            for child in cohort.children.values():
                child.censor(now_ns)
                if not child.active:
                    unregister_child(native_side, child)

    rows = pd.DataFrame([cohort.as_wide_record() for cohort in cohorts])
    violations = int(rows["monotonicity_violation_count"].sum())
    if fail_on_monotonicity and violations:
        bad = rows.loc[
            rows["monotonicity_violation_count"] > 0,
            ["cohort_id", "monotonicity_violations"],
        ].head(5)
        raise RuntimeError(
            "paired lifecycle monotonicity violation:\n"
            + bad.to_string(index=False)
        )
    stats = scheduler.stats()
    summary = {
        "cohorts": int(len(rows)),
        "monotonicity_violations": violations,
        "trade_rows_in_scope": int(len(scoped)),
        "trade_rows_processed": int(processed_trade_rows),
        "native_boundaries_processed": int(processed_native_boundaries),
        "native_events_consumed": int(stats.consumed_events),
        "native_events_accepted": int(stats.accepted_events),
        "native_events_rejected": int(stats.rejected_events),
        "native_sequence_gaps": int(stats.sequence_gaps),
        "native_invalid_sequence_messages": int(
            stats.invalid_sequence_messages
        ),
    }
    return rows, summary


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", required=True)
    parser.add_argument("--symbol", default="BTCUSDC")
    parser.add_argument("--decision-trace", type=Path, required=True)
    parser.add_argument("--lifecycle-trace", type=Path, required=True)
    parser.add_argument("--quote-trace", type=Path, required=True)
    parser.add_argument("--raw-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC)
    parser.add_argument("--max-cohorts", type=int, default=250)
    parser.add_argument("--max-horizon-ms", type=int, default=10_000)
    parser.add_argument("--tick-size", type=float, default=0.1)
    parser.add_argument("--lot-size", type=float, default=0.001)
    parser.add_argument("--warmup-hours", type=int, default=24)
    parser.add_argument("--allow-monotonicity-violations", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    paths = {
        "decision_trace": args.decision_trace.expanduser().resolve(),
        "lifecycle_trace": args.lifecycle_trace.expanduser().resolve(),
        "quote_trace": args.quote_trace.expanduser().resolve(),
        "spec": args.spec.expanduser().resolve(),
    }
    for label, path in paths.items():
        if not path.is_file():
            raise SystemExit(f"missing {label}: {path}")
    spec = json.loads(paths["spec"].read_text(encoding="utf-8"))
    if spec.get("research_status") != "frozen_before_surface_outcomes":
        raise SystemExit("surface spec is not frozen before outcomes")
    development_days = set(spec["panels"]["development"]["days"])
    if args.day not in development_days:
        raise SystemExit("smoke day must come from the frozen Development panel")

    decisions = pd.read_parquet(paths["decision_trace"])
    lifecycle = pd.read_parquet(paths["lifecycle_trace"])
    quotes = pd.read_parquet(paths["quote_trace"])
    cohorts, join_audit = build_cohorts(
        decisions,
        lifecycle,
        quotes,
        day=str(args.day),
        tick_size=float(args.tick_size),
        lot_size=float(args.lot_size),
        max_cohorts=int(args.max_cohorts),
        max_horizon_ms=int(args.max_horizon_ms),
    )
    if not cohorts:
        raise RuntimeError("baseline traces produced no supported cohorts")

    bt.configure_symbol(str(args.symbol))
    trades = bt.load_individual_trades(days=[str(args.day)])
    trade_identity = _individual_trade_identity(
        str(args.symbol), str(args.day)
    )
    tape = CryptoHFTExchangeBookTape(
        raw_root=args.raw_root.expanduser().resolve(),
        day=str(args.day),
        symbol=str(args.symbol),
        tick_size=float(args.tick_size),
        warmup_hours=int(args.warmup_hours),
        strict_complete=True,
    )
    rows, simulation = simulate_paired_placements(
        cohorts,
        tape=tape,
        trades=trades,
        tick_size=float(args.tick_size),
        fail_on_monotonicity=not bool(
            args.allow_monotonicity_violations
        ),
    )

    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    panel_path = output_dir / "paired_order_lifecycle_smoke.parquet"
    temporary = panel_path.with_suffix(".parquet.tmp")
    rows.to_parquet(temporary, index=False, compression="zstd")
    temporary.replace(panel_path)
    disk = shutil.disk_usage(output_dir)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "family_id": str(spec["family_id"]),
        "day": str(args.day),
        "panel_status": "mechanics_smoke_not_training_panel",
        "estimand": "placement_activation_and_direct_fill_cif",
        "followup": "frozen_baseline_followup_shadow",
        "campaign_outcome_available": False,
        "surface_fit_allowed": False,
        "action_or_live_authorization": False,
        "q90_treatment": spec["frozen_separate_treatments"][0],
        "spec_path": str(paths["spec"]),
        "spec_sha256": _sha256(paths["spec"]),
        "input_artifacts": {
            label: {"path": str(path), "sha256": _sha256(path)}
            for label, path in paths.items()
            if label != "spec"
        },
        "implementation_identity": {
            label: _file_identity(path)
            for label, path in {
                "paired_order_lifecycle": Path(__file__).with_name(
                    "paired_order_lifecycle.py"
                ),
                "paired_order_lifecycle_smoke": Path(__file__),
                "exchange_book_replay": ROOT
                / "models"
                / "exchange_book_replay.py",
            }.items()
        },
        "individual_trade_identity": trade_identity,
        "native_tape_identity": tape.identity(include_sha256=True),
        "join_audit": join_audit,
        "simulation": simulation,
        "rows": int(len(rows)),
        "side_counts": {
            str(key): int(value)
            for key, value in rows["side"].value_counts().items()
        },
        "role_counts": {
            str(key): int(value)
            for key, value in rows["inventory_role"].value_counts().items()
        },
        "panel_path": str(panel_path),
        "panel_sha256": _sha256(panel_path),
        "free_gib_after_write": float(disk.free / (1024**3)),
    }
    _atomic_json(manifest, output_dir / "manifest.json")
    print(json.dumps({
        "panel": str(panel_path),
        "rows": int(len(rows)),
        "violations": int(simulation["monotonicity_violations"]),
        "free_gib": round(float(disk.free / (1024**3)), 2),
    }, sort_keys=True))


if __name__ == "__main__":
    main()
