"""Outcome-blind receive-time mechanics for an external adverse-edge guard.

The audit joins actual paired live quote decisions to immutable AWS Tokyo
receive-time market tapes.  It measures whether a defensive, outward-only quote
guard has mechanical support.  It deliberately does not read fills, markouts,
PnL, campaign outcomes, or any other reward.

The historical quote and signed-inventory state timestamps are
millisecond-resolution log-write times, not exact decision-start or
feature-ready timestamps.  Results produced from those logs are therefore
clock-sensitivity evidence; they cannot establish sub-second action transport
or register a policy.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import heapq
import json
import math
import os
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from strategy.cross_venue_fair_price import (
    CrossVenueFairPriceConfig,
    CrossVenueFairPriceEstimator,
    CrossVenueFairPriceState,
    FairPriceSource,
    weighted_median,
)


SCHEMA_VERSION = "external_adverse_quote_edge_guard_mechanics.v1"
PROFILE_OMISSIONS: Mapping[str, str | None] = {
    "all_venues": None,
    "leave_bitget_out": "bitget",
    "leave_bybit_out": "bybit",
    "leave_okx_out": "okx",
}
DEFAULT_DELAYS_MS = (0, 10, 25, 50, 100, 250, 500)
QUOTE_COLUMNS = (
    "timestamp",
    "symbol",
    "side",
    "mode",
    "allow_post",
    "allow_exposure_increase",
    "inventory_ratio",
    "mid",
    "final_price",
    "final_size",
    "can_post_after_inventory",
    "order_active_before",
    "needs_update",
    "action",
)
EXTERNAL_MARKETS = {
    f"{venue}:{market}:BTCUSDT"
    for venue in ("bitget", "bybit", "okx")
    for market in ("spot", "perp")
}
LOCAL_MARKET = "binance:perp:BTCUSDC"
ANCHOR_MARKET = "binance:spot:USDCUSDT"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _atomic_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.to_parquet(temporary, index=False)
    os.replace(temporary, path)


def _finite(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def quote_role(side: str, inventory_q: float, *, zero_tol: float = 1e-12) -> str:
    """Classify a quote using only the decision-visible inventory sign."""

    normalized = str(side).upper()
    inventory = float(inventory_q)
    if normalized not in {"BUY", "SELL"}:
        raise ValueError(f"unsupported side {side!r}")
    if not math.isfinite(inventory):
        raise ValueError("inventory_q must be finite")
    if abs(inventory) <= float(zero_tol):
        return "opener"
    if normalized == "BUY":
        return "add" if inventory > 0.0 else "reducing"
    return "add" if inventory < 0.0 else "reducing"


def _read_paired_quote_file(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, usecols=list(QUOTE_COLUMNS))
    if frame.empty or len(frame) % 2:
        raise ValueError(f"{path} does not contain complete BUY/SELL pairs")
    frame = frame.reset_index(drop=True)
    buy = frame.iloc[0::2].reset_index(drop=True)
    sell = frame.iloc[1::2].reset_index(drop=True)
    if not buy["side"].astype(str).str.upper().eq("BUY").all():
        raise ValueError(f"{path} BUY/SELL write order is invalid")
    if not sell["side"].astype(str).str.upper().eq("SELL").all():
        raise ValueError(f"{path} BUY/SELL write order is invalid")

    numeric = (
        "timestamp",
        "allow_post",
        "allow_exposure_increase",
        "inventory_ratio",
        "mid",
        "final_price",
        "final_size",
        "can_post_after_inventory",
        "order_active_before",
        "needs_update",
    )
    for column in numeric:
        buy[column] = pd.to_numeric(buy[column], errors="raise")
        sell[column] = pd.to_numeric(sell[column], errors="raise")
    if not np.allclose(buy["mid"], sell["mid"], rtol=0.0, atol=1e-12):
        raise ValueError(f"{path} paired decisions do not share one mid")
    if not np.allclose(
        buy["inventory_ratio"], sell["inventory_ratio"], rtol=0.0, atol=1e-12
    ):
        raise ValueError(f"{path} paired decisions do not share inventory")
    pair_delay_ms = (sell["timestamp"] - buy["timestamp"]) * 1_000.0
    if (pair_delay_ms < -1e-6).any() or (pair_delay_ms > 1_000.0).any():
        raise ValueError(f"{path} contains an invalid BUY/SELL pair delay")

    decision_ts_ns = np.rint(buy["timestamp"].to_numpy(float) * 1e9).astype(
        np.int64
    )
    output = pd.DataFrame(
        {
            "decision_ts_ns": decision_ts_ns,
            "decision_log_write_ts_s": buy["timestamp"].to_numpy(float),
            "sell_log_delay_ms": pair_delay_ms.to_numpy(float),
            "symbol": buy["symbol"].astype(str).to_numpy(),
            "mid": buy["mid"].to_numpy(float),
            # This live field is abs(q) / max_inventory.  Keep it as a
            # magnitude diagnostic; it must never be used to infer role.
            "inventory_ratio_abs": buy["inventory_ratio"].to_numpy(float),
            "bid_final": buy["final_price"].to_numpy(float),
            "ask_final": sell["final_price"].to_numpy(float),
        }
    )
    for prefix, source in (("buy", buy), ("sell", sell)):
        for column in (
            "mode",
            "allow_post",
            "allow_exposure_increase",
            "final_size",
            "can_post_after_inventory",
            "order_active_before",
            "needs_update",
            "action",
        ):
            output[f"{prefix}_{column}"] = source[column].to_numpy()
    output["input_path"] = str(path)
    return output


def load_signed_inventory_states(
    paths: Sequence[Path],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Load the signed position visible after each historical state event.

    Only ``timestamp`` and ``position`` are read.  Fill price, quantity, PnL,
    commission, and all post-decision outcomes are deliberately excluded.
    """

    if not paths:
        raise ValueError("at least one signed-inventory state source is required")
    inputs: list[dict[str, Any]] = []
    frames: list[pd.DataFrame] = []
    for raw_path in paths:
        path = Path(raw_path).expanduser().resolve()
        if not path.is_file():
            raise FileNotFoundError(path)
        frame = pd.read_csv(path, usecols=["timestamp", "position"])
        frame["timestamp"] = pd.to_numeric(frame["timestamp"], errors="raise")
        frame["position"] = pd.to_numeric(frame["position"], errors="raise")
        frame = frame.loc[
            np.isfinite(frame["timestamp"]) & np.isfinite(frame["position"])
        ].copy()
        frame["inventory_state_ts_ns"] = np.rint(
            frame["timestamp"].to_numpy(float) * 1e9
        ).astype(np.int64)
        frame = frame.rename(columns={"position": "inventory_q"})[
            ["inventory_state_ts_ns", "inventory_q"]
        ]
        frames.append(frame)
        inputs.append(
            {
                "path": str(path),
                "sha256": sha256_file(path),
                "state_rows": int(len(frame)),
                "columns_read": ["timestamp", "position"],
            }
        )
    combined = pd.concat(frames, ignore_index=True).sort_values(
        "inventory_state_ts_ns", kind="stable"
    )
    conflicts = int(
        (
            combined.groupby("inventory_state_ts_ns", sort=False)["inventory_q"]
            .nunique(dropna=False)
            .gt(1)
        ).sum()
    )
    if conflicts:
        raise ValueError(
            f"overlapping inventory logs contain {conflicts} conflicting states"
        )
    output = combined.drop_duplicates(
        subset=["inventory_state_ts_ns"], keep="first"
    ).reset_index(drop=True)
    if output.empty:
        raise ValueError("signed-inventory state sources are empty")
    return output, {
        "inputs": inputs,
        "input_rows": int(sum(len(frame) for frame in frames)),
        "unique_state_rows": int(len(output)),
        "deduplicated_rows": int(sum(len(frame) for frame in frames) - len(output)),
        "conflicting_rows": conflicts,
        "timestamp_contract": "millisecond_resolution_post_state_update_log_write",
        "columns_read": ["timestamp", "position"],
        "reward_columns_read": [],
    }


def _attach_signed_inventory(
    opportunities: pd.DataFrame,
    inventory_states: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Attach the latest strictly-prior signed position to each quote pair."""

    exact_state_times = set(inventory_states["inventory_state_ts_ns"].astype(int))
    output = pd.merge_asof(
        opportunities.sort_values("decision_ts_ns"),
        inventory_states.sort_values("inventory_state_ts_ns"),
        left_on="decision_ts_ns",
        right_on="inventory_state_ts_ns",
        direction="backward",
        allow_exact_matches=False,
    )
    output["inventory_same_ms_ambiguous"] = output["decision_ts_ns"].isin(
        exact_state_times
    )
    output["inventory_state_available"] = output["inventory_q"].notna()
    output["inventory_state_age_ms"] = (
        output["decision_ts_ns"] - output["inventory_state_ts_ns"]
    ) / 1_000_000.0
    return output, {
        "strictly_prior_state_join": True,
        "state_available_pairs": int(output["inventory_state_available"].sum()),
        "state_missing_pairs": int((~output["inventory_state_available"]).sum()),
        "same_millisecond_ambiguous_pairs": int(
            output["inventory_same_ms_ambiguous"].sum()
        ),
        "role_available_pairs": int(
            (
                output["inventory_state_available"]
                & ~output["inventory_same_ms_ambiguous"]
            ).sum()
        ),
    }


def load_quote_opportunities(
    paths: Sequence[Path],
    *,
    inventory_state_paths: Sequence[Path],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Load strict quote pairs and attach a signed, past-only position state."""

    if not paths:
        raise ValueError("at least one quote-decision source is required")
    inputs: list[dict[str, Any]] = []
    frames: list[pd.DataFrame] = []
    for raw_path in paths:
        path = Path(raw_path).expanduser().resolve()
        if not path.is_file():
            raise FileNotFoundError(path)
        frame = _read_paired_quote_file(path)
        frames.append(frame)
        inputs.append(
            {
                "path": str(path),
                "sha256": sha256_file(path),
                "quote_pairs": int(len(frame)),
            }
        )
    combined = pd.concat(frames, ignore_index=True).sort_values(
        ["decision_ts_ns", "input_path"], kind="stable"
    )
    material = [
        column
        for column in combined.columns
        if column not in {"input_path", "decision_log_write_ts_s"}
    ]
    conflicts = 0
    keep_indices: list[int] = []
    for _, group in combined.groupby("decision_ts_ns", sort=False):
        first = group.iloc[0]
        for _, other in group.iloc[1:].iterrows():
            for column in material:
                left = first[column]
                right = other[column]
                if _finite(left) and _finite(right):
                    equal = math.isclose(
                        float(left), float(right), rel_tol=0.0, abs_tol=1e-12
                    )
                else:
                    equal = str(left) == str(right)
                if not equal:
                    conflicts += 1
                    break
        keep_indices.append(int(group.index[0]))
    if conflicts:
        raise ValueError(
            f"overlapping quote logs contain {conflicts} conflicting decision pairs"
        )
    output = combined.loc[keep_indices].sort_values("decision_ts_ns").reset_index(
        drop=True
    )
    if output["decision_ts_ns"].duplicated().any():
        raise AssertionError("quote opportunity deduplication failed")
    inventory_states, inventory_audit = load_signed_inventory_states(
        inventory_state_paths
    )
    output, join_audit = _attach_signed_inventory(output, inventory_states)
    audit = {
        "inputs": inputs,
        "input_pairs": int(sum(len(frame) for frame in frames)),
        "unique_pairs": int(len(output)),
        "deduplicated_pairs": int(sum(len(frame) for frame in frames) - len(output)),
        "conflicting_pairs": int(conflicts),
        "timestamp_contract": "millisecond_resolution_post_decision_log_write",
        "exact_decision_start_clock_available": False,
        "inventory_ratio_field_semantics": "absolute_magnitude_only",
        "inventory_state": inventory_audit,
        "inventory_join": join_audit,
    }
    return output, audit


@dataclass(frozen=True)
class CaptureWindow:
    capture_id: str
    utc_day: str
    start_ts_ns: int
    end_ts_ns: int
    duration_s: float
    directory: Path
    summary_path: Path
    tape_paths: tuple[Path, ...]


def load_capture_windows(
    ledger_path: Path,
    *,
    minimum_duration_s: float,
) -> tuple[list[CaptureWindow], dict[str, Any]]:
    ledger = Path(ledger_path).expanduser().resolve()
    root = ledger.parent
    windows: list[CaptureWindow] = []
    for line in ledger.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        interval = row.get("interval_utc") or {}
        observed = float(interval.get("duration_s") or 0.0)
        requested = float(row.get("requested_duration_s") or 0.0)
        if not bool(row.get("valid")) or max(observed, requested) < minimum_duration_s:
            continue
        capture_id = str(row["capture_id"])
        directory = root / f"aws_tokyo_{capture_id}"
        # marker/summary.json is the stable capture-manifest schema.  Older
        # root summaries were rewritten by the local sync validator and use
        # absolute pre-relocation ``file`` fields instead of relative paths.
        summary_path = directory / "marker" / "summary.json"
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        summary_valid = bool(summary.get("all_files_valid")) or bool(
            summary.get("valid")
        )
        if not summary_valid or int(summary.get("file_count", 0)) != 7:
            raise ValueError(f"capture {capture_id} does not have seven valid tapes")
        tape_paths = tuple(directory / item["path"] for item in summary["files"])
        if len(tape_paths) != 7 or len(set(tape_paths)) != 7:
            raise ValueError(f"capture {capture_id} tape identity is not unique")
        if any(not path.is_file() for path in tape_paths):
            raise FileNotFoundError(f"capture {capture_id} is missing a local tape")
        start = pd.Timestamp(interval["start"]).value
        end = pd.Timestamp(interval["end"]).value
        windows.append(
            CaptureWindow(
                capture_id=capture_id,
                utc_day=str(row["utc_day"]),
                start_ts_ns=int(start),
                end_ts_ns=int(end),
                duration_s=observed,
                directory=directory,
                summary_path=summary_path,
                tape_paths=tape_paths,
            )
        )
    windows.sort(key=lambda row: (row.start_ts_ns, row.capture_id))
    return windows, {
        "ledger_path": str(ledger),
        "ledger_sha256": sha256_file(ledger),
        "valid_full_windows": int(len(windows)),
        "distinct_valid_full_utc_days": int(len({row.utc_day for row in windows})),
        "minimum_duration_s": float(minimum_duration_s),
    }


def _iter_book_rows(
    path: Path,
    *,
    reorder_window_ns: int,
    audit: dict[str, Any],
) -> Iterator[tuple[int, dict[str, Any]]]:
    """Emit strict feature-ready order from a bounded-disorder recorder tape."""

    max_seen_ready = 0
    last_input_ready = 0
    last_emitted_ready = 0
    input_rows = 0
    book_rows = 0
    regressions = 0
    max_disorder_ns = 0
    ordinal = 0
    heap: list[tuple[int, int, dict[str, Any]]] = []

    def sync_audit() -> None:
        audit.update(
            {
                "path": str(path),
                "input_rows": int(input_rows),
                "book_rows": int(book_rows),
                "input_ready_regressions": int(regressions),
                "max_input_disorder_ms": float(max_disorder_ns / 1_000_000.0),
                "reorder_window_ms": float(reorder_window_ns / 1_000_000.0),
                "last_input_ready_ts_ns": int(last_input_ready),
                "last_emitted_ready_ts_ns": int(last_emitted_ready),
                "reorder_contract_valid": bool(max_disorder_ns <= reorder_window_ns),
            }
        )

    sync_audit()

    def emit_ready(watermark: int) -> Iterator[tuple[int, dict[str, Any]]]:
        nonlocal last_emitted_ready
        while heap and heap[0][0] <= watermark:
            ready, _, row = heapq.heappop(heap)
            if ready < last_emitted_ready:
                raise ValueError(
                    f"{path} exceeds the frozen feature-ready reorder window: "
                    f"{last_emitted_ready - ready}ns"
                )
            last_emitted_ready = ready
            yield ready, row

    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            row = json.loads(line)
            ready = int(row.get("feature_ready_ts_ns") or 0)
            if ready <= 0:
                raise ValueError(f"{path}:{line_number} has no feature-ready time")
            input_rows += 1
            if ready < max_seen_ready:
                regressions += 1
                max_disorder_ns = max(max_disorder_ns, max_seen_ready - ready)
                sync_audit()
            max_seen_ready = max(max_seen_ready, ready)
            last_input_ready = ready
            if str(row.get("event_type", "")).lower() == "book":
                book_rows += 1
                heapq.heappush(heap, (ready, ordinal, row))
                ordinal += 1
            if input_rows % 10_000 == 0:
                sync_audit()
            yield from emit_ready(max_seen_ready - reorder_window_ns)
    yield from emit_ready(max_seen_ready)
    if heap:
        raise AssertionError(f"{path} reorder heap did not drain")
    sync_audit()


def _merge_book_rows(
    paths: Sequence[Path],
    *,
    reorder_window_ms: float,
    tape_audits: list[dict[str, Any]],
) -> Iterator[tuple[int, dict[str, Any]]]:
    reorder_window_ns = int(round(float(reorder_window_ms) * 1_000_000.0))
    if reorder_window_ns <= 0:
        raise ValueError("feature-ready reorder window must be positive")
    tape_audits.extend({} for _ in paths)
    iterators = [
        iter(
            _iter_book_rows(
                path,
                reorder_window_ns=reorder_window_ns,
                audit=tape_audits[index],
            )
        )
        for index, path in enumerate(paths)
    ]
    heap: list[tuple[int, int, int, dict[str, Any]]] = []
    ordinals = [0] * len(iterators)
    for index, iterator in enumerate(iterators):
        try:
            ready, row = next(iterator)
        except StopIteration:
            continue
        heapq.heappush(heap, (ready, index, 0, row))
    while heap:
        ready, index, _, row = heapq.heappop(heap)
        yield ready, row
        try:
            next_ready, next_row = next(iterators[index])
        except StopIteration:
            continue
        ordinals[index] += 1
        heapq.heappush(
            heap,
            (next_ready, index, ordinals[index], next_row),
        )


@dataclass(frozen=True)
class ProfileEdge:
    profile: str
    fair_price: float
    lead_bps: float
    lower_price: float
    upper_price: float
    adverse_side: str
    buy_requested_ticks: int
    sell_requested_ticks: int


@dataclass(frozen=True)
class GuardProjection:
    valid: bool
    reason: str
    adverse_side: str
    requested_ticks: int
    effective_ticks: int
    candidate_bid: float
    candidate_ask: float
    cap_clipped: bool
    loo_consistent: bool
    profile_edges: tuple[ProfileEdge, ...]


def _profile_edge(
    state: CrossVenueFairPriceState,
    *,
    profile: str,
    omitted_venue: str | None,
    baseline_bid: float,
    baseline_ask: float,
    tick_size: float,
) -> ProfileEdge | None:
    rows = [
        row
        for venue, row in sorted(state.venues.items())
        if venue != omitted_venue
    ]
    if len(rows) < 2:
        return None
    fair = weighted_median((row.fair_price, row.weight) for row in rows)
    if not (_finite(fair) and fair > 0.0 and state.local_mid > 0.0):
        return None
    lead_bps = math.log(fair / state.local_mid) * 10_000.0
    tick_bps = float(tick_size) / state.local_mid * 10_000.0
    lower_candidates: list[float] = []
    upper_candidates: list[float] = []
    for row in rows:
        # The basis tracker is past-only.  One additional tick covers deterministic
        # quote rounding without looking at any reward or future price.
        epsilon_bps = math.sqrt(max(0.0, row.tracking_variance_bps2)) + tick_bps
        lower_candidates.append(row.fair_price * math.exp(-epsilon_bps / 10_000.0))
        upper_candidates.append(row.fair_price * math.exp(epsilon_bps / 10_000.0))
    lower = min(lower_candidates)
    upper = max(upper_candidates)
    adverse_side = "SELL" if lead_bps > 0.0 else "BUY" if lead_bps < 0.0 else ""
    buy_ticks = max(0, int(math.ceil((baseline_bid - lower) / tick_size - 1e-12)))
    sell_ticks = max(0, int(math.ceil((upper - baseline_ask) / tick_size - 1e-12)))
    return ProfileEdge(
        profile=profile,
        fair_price=float(fair),
        lead_bps=float(lead_bps),
        lower_price=float(lower),
        upper_price=float(upper),
        adverse_side=adverse_side,
        buy_requested_ticks=buy_ticks,
        sell_requested_ticks=sell_ticks,
    )


def project_adverse_edge_guard(
    state: CrossVenueFairPriceState,
    *,
    baseline_bid: float,
    baseline_ask: float,
    tick_size: float,
    max_pair_spread_bps: float,
) -> GuardProjection:
    """Project an outward-only quote guard with all-LOO direction agreement."""

    bid = float(baseline_bid)
    ask = float(baseline_ask)
    tick = float(tick_size)

    def invalid(reason: str, edges: Iterable[ProfileEdge] = ()) -> GuardProjection:
        return GuardProjection(
            valid=False,
            reason=reason,
            adverse_side="",
            requested_ticks=0,
            effective_ticks=0,
            candidate_bid=bid,
            candidate_ask=ask,
            cap_clipped=False,
            loo_consistent=False,
            profile_edges=tuple(edges),
        )

    if not state.valid:
        return invalid(state.reason)
    if len(state.venues) != 3:
        return invalid("three_venue_common_support_required")
    if not (
        tick > 0.0
        and float(max_pair_spread_bps) > 0.0
        and bid > 0.0
        and ask > bid
        and state.local_mid > 0.0
    ):
        return invalid("invalid_quote_geometry")

    edges: list[ProfileEdge] = []
    for profile, omitted in PROFILE_OMISSIONS.items():
        edge = _profile_edge(
            state,
            profile=profile,
            omitted_venue=omitted,
            baseline_bid=bid,
            baseline_ask=ask,
            tick_size=tick,
        )
        if edge is None:
            return invalid(f"unsupported_profile:{profile}", edges)
        edges.append(edge)
    directions = {edge.adverse_side for edge in edges}
    if len(directions) != 1 or "" in directions:
        return invalid("loo_direction_disagreement", edges)
    adverse_side = next(iter(directions))
    if adverse_side == "BUY":
        requested_ticks = max(edge.buy_requested_ticks for edge in edges)
    else:
        requested_ticks = max(edge.sell_requested_ticks for edge in edges)
    if requested_ticks <= 0:
        return invalid("conservative_edge_nonnegative", edges)

    max_pair_spread = state.local_mid * float(max_pair_spread_bps) / 10_000.0
    if adverse_side == "BUY":
        maximum_outward_ticks = max(
            0,
            int(math.floor((bid - (ask - max_pair_spread)) / tick + 1e-12)),
        )
        effective_ticks = min(requested_ticks, maximum_outward_ticks)
        candidate_bid = bid - effective_ticks * tick
        candidate_ask = ask
    else:
        maximum_outward_ticks = max(
            0,
            int(math.floor(((bid + max_pair_spread) - ask) / tick + 1e-12)),
        )
        effective_ticks = min(requested_ticks, maximum_outward_ticks)
        candidate_bid = bid
        candidate_ask = ask + effective_ticks * tick
    return GuardProjection(
        valid=True,
        reason="valid" if effective_ticks > 0 else "spread_cap_no_room",
        adverse_side=adverse_side,
        requested_ticks=int(requested_ticks),
        effective_ticks=int(effective_ticks),
        candidate_bid=float(candidate_bid),
        candidate_ask=float(candidate_ask),
        cap_clipped=bool(effective_ticks < requested_ticks),
        loo_consistent=True,
        profile_edges=tuple(edges),
    )


def _source_from_book(row: Mapping[str, Any]) -> FairPriceSource:
    venue, market, _ = str(row["market_id"]).split(":", maxsplit=2)
    return FairPriceSource(
        venue=venue,
        market_type=market,
        bid=float(row.get("bid") or 0.0),
        ask=float(row.get("ask") or 0.0),
        exchange_ts_ns=int(row.get("exchange_event_ts_ns") or 0),
        local_receive_ts_ns=int(row.get("local_receive_ts_ns") or 0),
        feature_ready_ts_ns=int(row.get("feature_ready_ts_ns") or 0),
        valid=row.get("gap_flag") is not True,
        source_kind="aws_tokyo_receive_time_bbo",
        transport_supported=True,
    )


def _book_mid(row: Mapping[str, Any] | None) -> float:
    if row is None:
        return math.nan
    bid = float(row.get("bid") or 0.0)
    ask = float(row.get("ask") or 0.0)
    return 0.5 * (bid + ask) if bid > 0.0 and ask > bid else math.nan


def _side_row(
    opportunity: Mapping[str, Any],
    *,
    capture: CaptureWindow,
    delay_ms: int,
    state: CrossVenueFairPriceState,
    projection: GuardProjection,
    side: str,
    local_tape_mid: float,
) -> dict[str, Any]:
    prefix = side.lower()
    inventory_q = float(opportunity["inventory_q"])
    inventory_ratio_abs = float(opportunity["inventory_ratio_abs"])
    inventory_state_available = bool(opportunity["inventory_state_available"])
    inventory_same_ms_ambiguous = bool(
        opportunity["inventory_same_ms_ambiguous"]
    )
    role_observable = bool(
        inventory_state_available and not inventory_same_ms_ambiguous
    )
    role = quote_role(side, inventory_q) if role_observable else "unknown"
    allow_post = bool(int(opportunity[f"{prefix}_allow_post"]))
    allow_exposure = bool(int(opportunity[f"{prefix}_allow_exposure_increase"]))
    final_size = float(opportunity[f"{prefix}_final_size"])
    active_before = bool(int(opportunity[f"{prefix}_order_active_before"]))
    negative_edge = bool(
        projection.valid
        and projection.adverse_side == side
        and projection.requested_ticks > 0
    )
    eligible = bool(
        role_observable
        and role in {"opener", "add"}
        and allow_post
        and allow_exposure
        and final_size > 0.0
    )
    triggered = bool(negative_edge and eligible)
    changed = bool(triggered and projection.effective_ticks > 0)
    potential_action = (
        "replace" if changed and active_before else "place" if changed else "none"
    )
    profile_map = {edge.profile: edge for edge in projection.profile_edges}
    quote_mid = float(opportunity["mid"])
    local_mid_diff_bps = (
        (local_tape_mid - quote_mid) / quote_mid * 10_000.0
        if _finite(local_tape_mid) and quote_mid > 0.0
        else math.nan
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "capture_id": capture.capture_id,
        "day": capture.utc_day,
        "opportunity_id": f"{capture.capture_id}:{int(opportunity['decision_ts_ns'])}",
        "decision_log_write_ts_ns": int(opportunity["decision_ts_ns"]),
        "query_ts_ns": int(opportunity["decision_ts_ns"]) + int(delay_ms) * 1_000_000,
        "delay_ms": int(delay_ms),
        "side": side,
        "role": role,
        "inventory_q": inventory_q,
        "inventory_ratio_abs": inventory_ratio_abs,
        "inventory_state_ts_ns": (
            int(opportunity["inventory_state_ts_ns"])
            if inventory_state_available
            else None
        ),
        "inventory_state_age_ms": (
            float(opportunity["inventory_state_age_ms"])
            if inventory_state_available
            else None
        ),
        "inventory_state_available": int(inventory_state_available),
        "inventory_same_ms_ambiguous": int(inventory_same_ms_ambiguous),
        "role_observable": int(role_observable),
        "quote_mid": quote_mid,
        "local_tape_mid": float(local_tape_mid),
        "local_mid_diff_bps": float(local_mid_diff_bps),
        "baseline_bid": float(opportunity["bid_final"]),
        "baseline_ask": float(opportunity["ask_final"]),
        "baseline_side_price": float(
            opportunity["bid_final"] if side == "BUY" else opportunity["ask_final"]
        ),
        "candidate_side_price": float(
            projection.candidate_bid if side == "BUY" else projection.candidate_ask
        ),
        "allow_post": int(allow_post),
        "allow_exposure_increase": int(allow_exposure),
        "order_active_before": int(active_before),
        "baseline_needs_update": int(opportunity[f"{prefix}_needs_update"]),
        "baseline_action": str(opportunity[f"{prefix}_action"]),
        "state_valid": int(state.valid),
        "state_reason": str(state.reason),
        "valid_venues": int(state.valid_venues),
        "raw_lead_bps": float(state.raw_lead_bps),
        "dispersion_bps": float(state.dispersion_bps),
        "max_source_age_ms": float(state.max_source_age_ms),
        "max_feed_latency_ms": float(state.max_feed_latency_ms),
        "max_feature_latency_ms": float(state.max_feature_latency_ms),
        "guard_projection_valid": int(projection.valid),
        "guard_reason": str(projection.reason),
        "adverse_side": str(projection.adverse_side),
        "loo_consistent": int(projection.loo_consistent),
        "negative_edge_state": int(negative_edge),
        "guard_eligible": int(eligible),
        "guard_trigger": int(triggered),
        "quote_coordinate_changed": int(changed),
        "requested_ticks": int(projection.requested_ticks if negative_edge else 0),
        "effective_ticks": int(projection.effective_ticks if negative_edge else 0),
        "spread_cap_clipped": int(negative_edge and projection.cap_clipped),
        "potential_order_action": potential_action,
        "potential_queue_reset_upper_bound": int(changed and active_before),
        "all_venues_adverse_side": str(
            profile_map.get("all_venues", ProfileEdge("", 0, 0, 0, 0, "", 0, 0)).adverse_side
        ),
        "leave_bitget_out_adverse_side": str(
            profile_map.get("leave_bitget_out", ProfileEdge("", 0, 0, 0, 0, "", 0, 0)).adverse_side
        ),
        "leave_bybit_out_adverse_side": str(
            profile_map.get("leave_bybit_out", ProfileEdge("", 0, 0, 0, 0, "", 0, 0)).adverse_side
        ),
        "leave_okx_out_adverse_side": str(
            profile_map.get("leave_okx_out", ProfileEdge("", 0, 0, 0, 0, "", 0, 0)).adverse_side
        ),
    }


def evaluate_capture(
    capture: CaptureWindow,
    opportunities: pd.DataFrame,
    *,
    delays_ms: Sequence[int],
    fair_config: CrossVenueFairPriceConfig,
    tick_size: float,
    max_pair_spread_bps: float,
    reorder_window_ms: float,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Evaluate one capture while reading each gzip market tape once."""

    selected = opportunities.loc[
        opportunities["decision_ts_ns"].between(
            capture.start_ts_ns, capture.end_ts_ns, inclusive="both"
        )
    ].copy()
    if selected.empty:
        return pd.DataFrame(), {
            "capture_id": capture.capture_id,
            "day": capture.utc_day,
            "quote_pairs": 0,
            "status": "missing_quote_opportunity_denominator",
            "summary_sha256": sha256_file(capture.summary_path),
        }
    selected = selected.sort_values("decision_ts_ns").reset_index(drop=True)
    queries: list[tuple[int, int, int]] = []
    for row_index, decision_ns in enumerate(selected["decision_ts_ns"].astype(np.int64)):
        for delay_ms in delays_ms:
            queries.append((int(decision_ns) + int(delay_ms) * 1_000_000, int(delay_ms), row_index))
    queries.sort()
    estimators = {
        int(delay): CrossVenueFairPriceEstimator(fair_config) for delay in delays_ms
    }
    latest_books: dict[str, dict[str, Any]] = {}
    tape_reorder_audit: list[dict[str, Any]] = []
    book_iterator = iter(
        _merge_book_rows(
            capture.tape_paths,
            reorder_window_ms=reorder_window_ms,
            tape_audits=tape_reorder_audit,
        )
    )
    try:
        pending = next(book_iterator)
    except StopIteration:
        pending = None
    output: list[dict[str, Any]] = []
    consumed_books = 0
    for query_ns, delay_ms, row_index in queries:
        while pending is not None and pending[0] <= query_ns:
            _, book = pending
            latest_books[str(book.get("market_id", ""))] = book
            consumed_books += 1
            try:
                pending = next(book_iterator)
            except StopIteration:
                pending = None
        opportunity = selected.iloc[row_index]
        local_tape_mid = _book_mid(latest_books.get(LOCAL_MARKET))
        local_mid = float(opportunity["mid"]) if delay_ms == 0 else local_tape_mid
        anchor_book = latest_books.get(ANCHOR_MARKET)
        anchor_mid = _book_mid(anchor_book)
        anchor_ready = int((anchor_book or {}).get("feature_ready_ts_ns") or 0)
        sources = [
            _source_from_book(latest_books[market_id])
            for market_id in sorted(EXTERNAL_MARKETS)
            if market_id in latest_books
        ]
        state = estimators[int(delay_ms)].observe(
            decision_ts_ns=query_ns,
            local_mid=local_mid,
            stablecoin_mid=anchor_mid,
            stablecoin_feature_ready_ts_ns=anchor_ready,
            sources=sources,
        )
        projection = project_adverse_edge_guard(
            state,
            baseline_bid=float(opportunity["bid_final"]),
            baseline_ask=float(opportunity["ask_final"]),
            tick_size=tick_size,
            max_pair_spread_bps=max_pair_spread_bps,
        )
        for side in ("BUY", "SELL"):
            output.append(
                _side_row(
                    opportunity,
                    capture=capture,
                    delay_ms=delay_ms,
                    state=state,
                    projection=projection,
                    side=side,
                    local_tape_mid=local_tape_mid,
                )
            )
    panel = pd.DataFrame(output)
    return panel, {
        "capture_id": capture.capture_id,
        "day": capture.utc_day,
        "quote_pairs": int(len(selected)),
        "panel_rows": int(len(panel)),
        "consumed_book_events": int(consumed_books),
        "status": "evaluated",
        "summary_path": str(capture.summary_path),
        "summary_sha256": sha256_file(capture.summary_path),
        "tape_reorder_audit": tape_reorder_audit,
        "reorder_contract_valid": bool(
            tape_reorder_audit
            and all(row.get("reorder_contract_valid") for row in tape_reorder_audit)
        ),
    }


def _quantiles(values: pd.Series) -> dict[str, float | None]:
    numeric = pd.to_numeric(values, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if numeric.empty:
        return {"median": None, "p90": None, "p99": None}
    return {
        "median": float(numeric.quantile(0.50)),
        "p90": float(numeric.quantile(0.90)),
        "p99": float(numeric.quantile(0.99)),
    }


def _episode_durations(base: pd.DataFrame) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for side in ("BUY", "SELL"):
        rows = base.loc[base["side"].eq(side)].sort_values(
            ["capture_id", "decision_log_write_ts_ns"]
        )
        durations_ms: list[float] = []
        episodes = 0
        for _, group in rows.groupby("capture_id", sort=False):
            active_start: int | None = None
            previous_ts: int | None = None
            for row in group.itertuples(index=False):
                timestamp = int(row.decision_log_write_ts_ns)
                trigger = bool(row.guard_trigger)
                if trigger and active_start is None:
                    active_start = timestamp
                    episodes += 1
                if not trigger and active_start is not None:
                    durations_ms.append((timestamp - active_start) / 1_000_000.0)
                    active_start = None
                previous_ts = timestamp
            if active_start is not None and previous_ts is not None:
                durations_ms.append((previous_ts - active_start) / 1_000_000.0)
        result[side] = {
            "episodes": int(episodes),
            "duration_ms": _quantiles(pd.Series(durations_ms, dtype=float)),
            "capture_end_censoring_included": True,
        }
    return result


def summarize_panel(
    panel: pd.DataFrame,
    *,
    capture_audit: Sequence[Mapping[str, Any]],
    quote_audit: Mapping[str, Any],
    ledger_audit: Mapping[str, Any],
    spec: Mapping[str, Any],
) -> dict[str, Any]:
    if panel.empty:
        raise ValueError("mechanics panel is empty")
    base = panel.loc[panel["delay_ms"].eq(0)].copy()
    cells: list[dict[str, Any]] = []
    for (delay, side, role), group in panel.groupby(
        ["delay_ms", "side", "role"], sort=True
    ):
        triggers = group.loc[group["guard_trigger"].eq(1)]
        changed = group.loc[group["quote_coordinate_changed"].eq(1)]
        cells.append(
            {
                "delay_ms": int(delay),
                "side": str(side),
                "role": str(role),
                "opportunities": int(len(group)),
                "state_valid_rate": float(group["state_valid"].mean()),
                "loo_consistent_rate": float(group["loo_consistent"].mean()),
                "negative_edge_state_rate": float(group["negative_edge_state"].mean()),
                "guard_trigger_rate": float(group["guard_trigger"].mean()),
                "quote_coordinate_change_rate": float(
                    group["quote_coordinate_changed"].mean()
                ),
                "trigger_count": int(len(triggers)),
                "coordinate_change_count": int(len(changed)),
                "spread_cap_clip_rate_given_trigger": (
                    float(triggers["spread_cap_clipped"].mean())
                    if len(triggers)
                    else 0.0
                ),
                "requested_ticks": _quantiles(triggers["requested_ticks"]),
                "effective_ticks": _quantiles(changed["effective_ticks"]),
                "potential_replace_count": int(
                    changed["potential_order_action"].eq("replace").sum()
                ),
                "potential_place_count": int(
                    changed["potential_order_action"].eq("place").sum()
                ),
                "potential_queue_reset_upper_bound": int(
                    changed["potential_queue_reset_upper_bound"].sum()
                ),
            }
        )

    survival: list[dict[str, Any]] = []
    keys = ["opportunity_id", "side"]
    base_trigger = base.loc[base["guard_trigger"].eq(1), keys].copy()
    for delay in sorted(set(panel["delay_ms"]) - {0}):
        delayed = panel.loc[panel["delay_ms"].eq(delay), keys + ["guard_trigger"]]
        joined = base_trigger.merge(delayed, on=keys, how="left", validate="one_to_one")
        for side in ("BUY", "SELL"):
            rows = joined.loc[joined["side"].eq(side)]
            survival.append(
                {
                    "delay_ms": int(delay),
                    "side": side,
                    "base_trigger_count": int(len(rows)),
                    "trigger_survival_rate": (
                        float(rows["guard_trigger"].fillna(0).mean())
                        if len(rows)
                        else None
                    ),
                }
            )

    evaluated = [row for row in capture_audit if row["status"] == "evaluated"]
    missing = [
        row for row in capture_audit if row["status"] != "evaluated"
    ]
    role_counts = (
        base.groupby(["side", "role"], dropna=False)
        .size()
        .rename("opportunities")
        .reset_index()
    )
    future_ready_violations = int(
        (panel["max_source_age_ms"].replace([np.inf, -np.inf], np.nan) < 0.0).sum()
    )
    permissions = {
        "prediction_supported": False,
        "transport_supported": False,
        "action_experiment_authorized": False,
        "live_deployment_authorized": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "identity": str(spec["identity"]),
        "status": "development_outcome_blind_mechanics_complete_clock_limited",
        "babel_layer": "E6/P2",
        "decision": "retain_mechanics_evidence_require_exact_decision_clock_and_full_path_replay",
        "ledger": dict(ledger_audit),
        "quote_opportunities": dict(quote_audit),
        "capture_audit": list(capture_audit),
        "evaluated_full_windows": int(len(evaluated)),
        "evaluated_distinct_utc_days": int(len({row["day"] for row in evaluated})),
        "missing_quote_denominator_windows": int(len(missing)),
        "missing_quote_denominator_days": sorted({row["day"] for row in missing}),
        "quote_pairs": int(base["opportunity_id"].nunique()),
        "side_opportunities": int(len(base)),
        "role_support": role_counts.to_dict(orient="records"),
        "mechanics_cells": cells,
        "delay_survival": survival,
        "trigger_episodes": _episode_durations(base),
        "local_quote_mid_vs_receive_tape_mid_bps": _quantiles(
            base["local_mid_diff_bps"].abs()
        ),
        "future_feature_time_violations": future_ready_violations,
        "outcome_columns_read": [],
        "reward_or_pnl_read": False,
        "state_columns_read": ["timestamp", "position"],
        "denominator_scope": "all_paired_live_quote_decisions_joined_to_latest_strictly_prior_signed_inventory_state",
        "formal_exact_opportunity_denominator": False,
        "formal_exact_opportunity_blockers": [
            "no_stable_decision_id",
            "millisecond_post_decision_log_write_clock",
            "millisecond_post_state_update_inventory_clock",
            "missing_quote_rows_on_some_valid_capture_days",
        ],
        "first_add_exact_denominator_available": False,
        "p1_first_add_prediction_directly_transportable_to_this_surface": False,
        "queue_cancel_ack_path_authoritative": False,
        "potential_replace_and_queue_reset_are_upper_bounds_only": True,
        "clock_contract": {
            "source_visibility": "recorded_feature_ready_ts_ns",
            "opportunity_clock": "millisecond_resolution_post_decision_log_write",
            "inventory_clock": "millisecond_resolution_post_state_update_log_write",
            "exact_decision_start_clock_available": False,
            "subsecond_transport_claim_supported": False,
        },
        "permissions": permissions,
    }


def run(
    *,
    spec_path: Path,
    ledger_path: Path,
    quote_paths: Sequence[Path],
    inventory_state_paths: Sequence[Path],
    output_dir: Path,
) -> dict[str, Any]:
    spec_path = Path(spec_path).expanduser().resolve()
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    if spec.get("identity") != "external_adverse_quote_edge_guard_mechanics_v1":
        raise ValueError("unexpected mechanics identity")
    frozen_inputs = spec["input_identity_snapshot"]
    resolved_ledger = Path(ledger_path).expanduser().resolve()
    if sha256_file(resolved_ledger) != str(frozen_inputs["capture_ledger_sha256"]):
        raise ValueError("capture ledger differs from the frozen input identity")
    expected_quote_hashes = {
        str(row["sha256"]) for row in frozen_inputs["quote_decision_sources"]
    }
    actual_quote_hashes = {
        sha256_file(Path(path).expanduser().resolve()) for path in quote_paths
    }
    if actual_quote_hashes != expected_quote_hashes:
        raise ValueError("quote-decision sources differ from the frozen input identity")
    expected_inventory_hashes = {
        str(row["sha256"])
        for row in frozen_inputs["signed_inventory_state_sources"]
    }
    actual_inventory_hashes = {
        sha256_file(Path(path).expanduser().resolve())
        for path in inventory_state_paths
    }
    if actual_inventory_hashes != expected_inventory_hashes:
        raise ValueError("inventory-state sources differ from the frozen input identity")
    fair_price_path = (
        Path(__file__).resolve().parents[4] / "strategy" / "cross_venue_fair_price.py"
    )
    if sha256_file(fair_price_path) != str(
        frozen_inputs["fair_price_implementation_sha256"]
    ):
        raise ValueError("fair-price implementation differs from the frozen identity")
    delays_ms = tuple(int(value) for value in spec["visibility_delays_ms"])
    if delays_ms != DEFAULT_DELAYS_MS:
        raise ValueError("visibility delay ABI differs from the frozen mechanics contract")
    windows, ledger_audit = load_capture_windows(
        resolved_ledger,
        minimum_duration_s=float(spec["minimum_full_window_duration_s"]),
    )
    opportunities, quote_audit = load_quote_opportunities(
        quote_paths,
        inventory_state_paths=inventory_state_paths,
    )
    fair_config = CrossVenueFairPriceConfig(
        minimum_valid_venues=3,
        max_source_age_ms=float(spec["fair_price"]["max_source_age_ms"]),
        max_anchor_age_ms=float(spec["fair_price"]["max_anchor_age_ms"]),
        max_dispersion_bps=float(spec["fair_price"]["max_dispersion_bps"]),
        maximum_abs_basis_bps=float(
            spec["fair_price"]["maximum_abs_basis_bps"]
        ),
        basis_half_life_s=float(spec["fair_price"]["basis_half_life_s"]),
        variance_half_life_s=float(
            spec["fair_price"]["variance_half_life_s"]
        ),
        minimum_basis_samples=int(spec["fair_price"]["minimum_basis_samples"]),
        minimum_gain_samples=int(spec["fair_price"]["minimum_gain_samples"]),
    )
    panels: list[pd.DataFrame] = []
    capture_audit: list[dict[str, Any]] = []
    for capture in windows:
        frame, audit = evaluate_capture(
            capture,
            opportunities,
            delays_ms=delays_ms,
            fair_config=fair_config,
            tick_size=float(spec["quote_contract"]["tick_size"]),
            max_pair_spread_bps=float(
                spec["quote_contract"]["max_pair_spread_bps"]
            ),
            reorder_window_ms=float(spec["clock_contract"]["reorder_window_ms"]),
        )
        capture_audit.append(audit)
        if not frame.empty:
            panels.append(frame)
    if not panels:
        raise ValueError("no capture has an exact quote-opportunity denominator")
    panel = pd.concat(panels, ignore_index=True)
    output = Path(output_dir).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    opportunity_path = output / "quote_opportunities.parquet"
    panel_path = output / "mechanics_panel.parquet"
    report_path = output / "report.json"
    capture_path = output / "capture_audit.json"
    used_ids = set(panel["opportunity_id"].astype(str))
    used_capture_ids = {value.split(":", maxsplit=1)[0] for value in used_ids}
    used_windows = [row for row in windows if row.capture_id in used_capture_ids]
    opportunity_rows = []
    for capture in used_windows:
        selected = opportunities.loc[
            opportunities["decision_ts_ns"].between(
                capture.start_ts_ns, capture.end_ts_ns, inclusive="both"
            )
        ].copy()
        selected.insert(0, "capture_id", capture.capture_id)
        selected.insert(1, "day", capture.utc_day)
        opportunity_rows.append(selected)
    used_opportunities = pd.concat(opportunity_rows, ignore_index=True)
    _atomic_parquet(opportunity_path, used_opportunities)
    _atomic_parquet(panel_path, panel)
    _atomic_json(capture_path, {"captures": capture_audit})
    report = summarize_panel(
        panel,
        capture_audit=capture_audit,
        quote_audit=quote_audit,
        ledger_audit=ledger_audit,
        spec=spec,
    )
    report["identity_hashes"] = {
        "spec_path": str(spec_path),
        "spec_sha256": sha256_file(spec_path),
        "implementation_path": str(Path(__file__).resolve()),
        "implementation_sha256": sha256_file(Path(__file__).resolve()),
        "quote_opportunities_path": str(opportunity_path),
        "quote_opportunities_sha256": sha256_file(opportunity_path),
        "mechanics_panel_path": str(panel_path),
        "mechanics_panel_sha256": sha256_file(panel_path),
        "capture_audit_path": str(capture_path),
        "capture_audit_sha256": sha256_file(capture_path),
    }
    report["report_payload_sha256"] = canonical_sha256(report)
    _atomic_json(report_path, report)
    return report


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument(
        "--quote-decisions",
        type=Path,
        action="append",
        required=True,
        help="Repeat for overlapping quote-decision snapshots; exact duplicates are removed.",
    )
    parser.add_argument(
        "--inventory-state",
        type=Path,
        action="append",
        required=True,
        help="Repeat for overlapping state journals; only timestamp and signed position are read.",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    report = run(
        spec_path=args.spec,
        ledger_path=args.ledger,
        quote_paths=args.quote_decisions,
        inventory_state_paths=args.inventory_state,
        output_dir=args.output_dir,
    )
    print(json.dumps(report, indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
