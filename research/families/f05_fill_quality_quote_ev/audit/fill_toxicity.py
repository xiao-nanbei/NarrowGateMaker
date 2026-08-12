#!/usr/bin/env python3
"""Build sub-second fill-toxicity evidence from receive-time market tapes.

The audit replays only events whose ``feature_ready_ts_ns`` is no later than a
fill timestamp, then labels the fill from future Binance BTCUSDC BBO mids.  It
does not alter quotes and is not a keep/cancel/re-center counterfactual.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import heapq
import json
import math
import random
import sys
from collections import defaultdict
from collections.abc import Iterable, Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.system_engineering.audit.market_data_latency import (  # noqa: E402
    SIMULATION_MODES,
    MarketDataLatencySimulator,
)
from research.system_engineering.audit.receive_time_tape import (  # noqa: E402
    expand_inputs,
    load_book_series,
)
from models.audit.support import norm_side, parse_ts  # noqa: E402
from research.families.f04_external_market_alpha.reference_replay import HistoricalReferenceScheduler  # noqa: E402
from models.tick_data_types import HistoricalReferenceEvent  # noqa: E402
from strategy.global_flow import (  # noqa: E402
    DEFAULT_FLOW_HORIZONS_MS,
    GlobalFlowEngine,
)

DEFAULT_MARKOUT_HORIZONS_MS = (10, 25, 50, 100, 250, 500)
REPLAY_READY_KEY = "_replay_feature_ready_ts_ns"
MARKET_FEATURES = (
    "flow_pressure",
    "mid_move_bps",
    "trade_imbalance",
    "l1_ofi_normalized",
    "book_age_ms",
    "book_fresh",
    "bid_depletion",
    "bid_refill",
    "ask_depletion",
    "ask_refill",
)


def _ready_ns(row: dict[str, Any]) -> int:
    return int(row.get(REPLAY_READY_KEY, row.get("feature_ready_ts_ns", 0)) or 0)


def _iter_path_rows(
    path: Path,
    *,
    latency_simulator: MarketDataLatencySimulator | None = None,
    latency_mode: str = "captured",
    latency_seed: int = 7,
) -> Iterator[dict[str, Any]]:
    fifo_ready_ns: dict[tuple[str, str, str], int] = {}
    reorder_heap: list[tuple[int, int, dict[str, Any]]] = []
    reorder_window_ns = 30_000_000_000
    max_seen_ns = 0
    last_yielded_ns = 0
    path_seed = int.from_bytes(
        hashlib.sha256(str(path).encode("utf-8")).digest()[:8], "big"
    )
    rng = random.Random(int(latency_seed) ^ path_seed)
    opener = gzip.open if path.suffix == ".gz" else Path.open
    kwargs = {"mode": "rt", "encoding": "utf-8"} if path.suffix == ".gz" else {"encoding": "utf-8"}
    with opener(path, **kwargs) as handle:
        line_number = 0
        while True:
            try:
                line = handle.readline()
            except EOFError:
                break
            if not line:
                break
            line_number += 1
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                try:
                    next_byte = handle.read(1)
                except EOFError:
                    next_byte = ""
                if not next_byte:
                    break
                raise ValueError(f"{path}:{line_number}: invalid JSON") from exc
            if not isinstance(row, dict):
                continue
            if latency_mode != "captured":
                if latency_simulator is None and latency_mode != "exchange_zero":
                    raise ValueError("profile latency mode requires a latency profile")
                if latency_mode == "exchange_zero":
                    visible_ns = int(row.get("exchange_event_ts_ns", 0) or 0)
                    visible_ns = visible_ns or int(row.get("feature_ready_ts_ns", 0) or 0)
                    delay_ms = 0.0
                else:
                    visible_ns, delay_ms = latency_simulator.visible_ts_ns(
                        row,
                        mode=latency_mode,
                        rng=rng,
                    )
                row = dict(row)
                if latency_mode.startswith("profile_"):
                    # Each source/channel is FIFO. A sampled tail delay queues
                    # later messages from that stream instead of letting them
                    # jump ahead of it.
                    stream_key = (
                        str(row.get("market_id", "")),
                        str(row.get("event_type", "")),
                        str(row.get("transport", "unknown")),
                    )
                    visible_ns = max(
                        fifo_ready_ns.get(stream_key, 0), int(visible_ns)
                    )
                    fifo_ready_ns[stream_key] = int(visible_ns)
                row[REPLAY_READY_KEY] = int(visible_ns)
                row["_replay_visibility_delay_ms"] = delay_ms
            ready_ns = _ready_ns(row)
            if ready_ns <= 0:
                continue
            max_seen_ns = max(max_seen_ns, ready_ns)
            heapq.heappush(reorder_heap, (ready_ns, line_number, row))
            watermark_ns = max_seen_ns - reorder_window_ns
            while reorder_heap and reorder_heap[0][0] <= watermark_ns:
                ordered_ns, _, ordered_row = heapq.heappop(reorder_heap)
                if ordered_ns < last_yielded_ns:
                    ordered_row = dict(ordered_row)
                    ordered_row[REPLAY_READY_KEY] = last_yielded_ns
                    ordered_row["_replay_reorder_clamped"] = 1
                else:
                    last_yielded_ns = ordered_ns
                yield ordered_row

    while reorder_heap:
        ordered_ns, _, ordered_row = heapq.heappop(reorder_heap)
        if ordered_ns < last_yielded_ns:
            ordered_row = dict(ordered_row)
            ordered_row[REPLAY_READY_KEY] = last_yielded_ns
            ordered_row["_replay_reorder_clamped"] = 1
        else:
            last_yielded_ns = ordered_ns
        yield ordered_row


def iter_causal_rows(
    paths: Iterable[Path],
    *,
    latency_simulator: MarketDataLatencySimulator | None = None,
    latency_mode: str = "captured",
    latency_seed: int = 7,
) -> Iterator[dict[str, Any]]:
    """Merge individually ordered recorder files by feature-ready time."""
    streams = [
        _iter_path_rows(
            path,
            latency_simulator=latency_simulator,
            latency_mode=latency_mode,
            latency_seed=latency_seed,
        )
        for path in paths
    ]
    if not streams:
        return
    yield from heapq.merge(*streams, key=_ready_ns)


def iter_historical_reference_events(
    paths: Iterable[Path],
    *,
    latency_simulator: MarketDataLatencySimulator | None = None,
    latency_mode: str = "captured",
    latency_seed: int = 7,
) -> Iterator[HistoricalReferenceEvent]:
    """Normalize causal recorder rows for the shared tick scheduler."""

    for row in iter_causal_rows(
        paths,
        latency_simulator=latency_simulator,
        latency_mode=latency_mode,
        latency_seed=latency_seed,
    ):
        ready_key = REPLAY_READY_KEY if REPLAY_READY_KEY in row else "feature_ready_ts_ns"
        try:
            yield HistoricalReferenceEvent.from_mapping(row, ready_key=ready_key)
        except ValueError:
            continue


def load_fills(
    path: Path,
    *,
    start_ts: float = 0.0,
    end_ts: float = 0.0,
    include_trade_types: Iterable[str] = ("OPEN", "CLOSE"),
) -> list[dict[str, Any]]:
    fills: list[dict[str, Any]] = []
    allowed_trade_types = {
        str(value).strip().upper() for value in include_trade_types if str(value).strip()
    }
    source = Path(path)
    handle = (
        gzip.open(source, mode="rt", newline="", encoding="utf-8")
        if source.suffix == ".gz"
        else source.open(newline="", encoding="utf-8")
    )
    with handle:
        for index, row in enumerate(csv.DictReader(handle), 1):
            timestamp = parse_ts(row.get("timestamp", ""))
            side = norm_side(row.get("side"))
            trade_type = str(row.get("trade_type", "")).strip().upper()
            try:
                price = float(row.get("price", 0.0) or 0.0)
                qty = float(row.get("qty", 0.0) or 0.0)
            except (TypeError, ValueError):
                continue
            if timestamp <= 0.0 or side not in {"BUY", "SELL"} or price <= 0.0 or qty <= 0.0:
                continue
            if allowed_trade_types and trade_type not in allowed_trade_types:
                continue
            if start_ts and timestamp < start_ts:
                continue
            if end_ts and timestamp > end_ts:
                continue
            fill = dict(row)
            fill.update(
                {
                    "fill_id": index,
                    "fill_ts": timestamp,
                    "fill_ts_ns": int(round(timestamp * 1_000_000_000)),
                    "side": side,
                    "price": price,
                    "qty": qty,
                }
            )
            fills.append(fill)
    fills.sort(key=lambda row: (row["fill_ts_ns"], row["fill_id"]))
    return fills


def _asof_scalar(
    timestamps: np.ndarray,
    values: np.ndarray,
    query_ns: int,
    *,
    max_age_ns: int,
) -> tuple[float, float]:
    index = int(np.searchsorted(timestamps, int(query_ns), side="right") - 1)
    if index < 0:
        return math.nan, math.inf
    age_ns = int(query_ns) - int(timestamps[index])
    if age_ns < 0 or age_ns > max_age_ns:
        return math.nan, age_ns / 1_000_000.0
    return float(values[index]), age_ns / 1_000_000.0


def _flatten_flow(row: dict[str, Any], state, horizons_ms: Iterable[int]) -> None:
    for horizon_ms in horizons_ms:
        window = state.window(horizon_ms)
        spot = window.get("spot", {})
        perp = window.get("perp", {})
        suffix = f"{int(horizon_ms)}ms"
        row[f"global_flow_valid_{suffix}"] = int(window.get("valid", 0))
        row[f"global_flow_pressure_{suffix}"] = window.get(
            "global_flow_pressure", math.nan
        )
        row[f"global_mid_move_bps_{suffix}"] = window.get(
            "global_mid_move_bps", math.nan
        )
        row[f"global_minus_bridge_bps_{suffix}"] = window.get(
            "global_minus_bridge_bps", math.nan
        )
        row[f"global_minus_execution_bps_{suffix}"] = window.get(
            "global_minus_execution_bps", math.nan
        )
        row[f"perp_minus_spot_move_bps_{suffix}"] = window.get(
            "perp_minus_spot_move_bps", math.nan
        )
        row[f"spot_flow_pressure_{suffix}"] = spot.get("flow_pressure", math.nan)
        row[f"perp_flow_pressure_{suffix}"] = perp.get("flow_pressure", math.nan)
        row[f"spot_mid_move_bps_{suffix}"] = spot.get("mid_move_bps", math.nan)
        row[f"perp_mid_move_bps_{suffix}"] = perp.get("mid_move_bps", math.nan)
        row[f"spot_dispersion_bps_{suffix}"] = spot.get("dispersion_bps", math.nan)
        row[f"perp_dispersion_bps_{suffix}"] = perp.get("dispersion_bps", math.nan)
        row[f"spot_trade_imbalance_{suffix}"] = spot.get(
            "trade_imbalance", math.nan
        )
        row[f"perp_trade_imbalance_{suffix}"] = perp.get(
            "trade_imbalance", math.nan
        )
        row[f"spot_l1_ofi_normalized_{suffix}"] = spot.get(
            "l1_ofi_normalized", math.nan
        )
        row[f"perp_l1_ofi_normalized_{suffix}"] = perp.get(
            "l1_ofi_normalized", math.nan
        )
        row[f"spot_venue_agreement_{suffix}"] = spot.get("venue_agreement", 0.0)
        row[f"perp_venue_agreement_{suffix}"] = perp.get("venue_agreement", 0.0)
        row[f"spot_fresh_venues_{suffix}"] = spot.get("fresh_venues", 0)
        row[f"perp_fresh_venues_{suffix}"] = perp.get("fresh_venues", 0)
        row[f"external_bid_depletion_{suffix}"] = (
            float(spot.get("bid_depletion", 0.0))
            + float(perp.get("bid_depletion", 0.0))
        )
        row[f"external_bid_refill_{suffix}"] = (
            float(spot.get("bid_refill", 0.0))
            + float(perp.get("bid_refill", 0.0))
        )
        row[f"external_ask_depletion_{suffix}"] = (
            float(spot.get("ask_depletion", 0.0))
            + float(perp.get("ask_depletion", 0.0))
        )
        row[f"external_ask_refill_{suffix}"] = (
            float(spot.get("ask_refill", 0.0))
            + float(perp.get("ask_refill", 0.0))
        )
        for local_name in ("execution", "local_bridge"):
            market = window.get(local_name, {})
            prefix = "execution" if local_name == "execution" else "bridge"
            for feature in MARKET_FEATURES:
                row[f"{prefix}_{feature}_{suffix}"] = market.get(
                    feature, math.nan
                )
        for factor_name, factor in (("spot", spot), ("perp", perp)):
            for market in factor.get("markets", []):
                market_id = str(market.get("market_id", ""))
                venue = market_id.split(":", 1)[0].strip().lower()
                if not venue:
                    continue
                for feature in MARKET_FEATURES:
                    row[f"{venue}_{factor_name}_{feature}_{suffix}"] = market.get(
                        feature, math.nan
                    )


def build_fill_toxicity_rows(
    *,
    tape_paths: Iterable[Path],
    fills: list[dict[str, Any]],
    execution_market_id: str = "binance:perp:BTCUSDC",
    execution_symbol: str = "BTCUSDC",
    reference_symbol: str = "BTCUSDT",
    flow_horizons_ms: Iterable[int] = DEFAULT_FLOW_HORIZONS_MS,
    markout_horizons_ms: Iterable[int] = DEFAULT_MARKOUT_HORIZONS_MS,
    max_future_book_age_ms: int = 500,
    latency_simulator: MarketDataLatencySimulator | None = None,
    latency_mode: str = "captured",
    latency_seed: int = 7,
) -> list[dict[str, Any]]:
    paths = list(tape_paths)
    flow_horizons = tuple(sorted({int(value) for value in flow_horizons_ms}))
    markout_horizons = tuple(sorted({int(value) for value in markout_horizons_ms}))
    series = load_book_series(paths, {execution_market_id})
    if execution_market_id not in series:
        raise ValueError(f"missing execution BBO tape: {execution_market_id}")
    execution_ts, execution_mid = series[execution_market_id]
    max_age_ns = int(max(1, max_future_book_age_ms) * 1_000_000)
    engine = GlobalFlowEngine(
        execution_symbol=execution_symbol,
        reference_symbol=reference_symbol,
        horizons_ms=flow_horizons,
    )
    scheduler = HistoricalReferenceScheduler(
        {
            "causal_market_tapes": iter_historical_reference_events(
                paths,
                latency_simulator=latency_simulator,
                latency_mode=latency_mode,
                latency_seed=latency_seed,
            )
        },
        engine=engine,
        allow_one_shot=True,
    )
    output: list[dict[str, Any]] = []

    for fill in fills:
        fill_ns = int(fill["fill_ts_ns"])
        scheduler.advance_to(fill_ns)
        state = scheduler.snapshot(now_ns=fill_ns)
        row = {
            "fill_id": fill.get("fill_id"),
            "fill_ts": fill["fill_ts"],
            "fill_ts_utc": datetime.fromtimestamp(
                float(fill["fill_ts"]), tz=timezone.utc
            ).isoformat(),
            "day": datetime.fromtimestamp(
                float(fill["fill_ts"]), tz=timezone.utc
            ).date().isoformat(),
            "side": fill["side"],
            "trade_type": fill.get("trade_type", ""),
            "qty": fill["qty"],
            "fill_price": fill["price"],
            "position_after": fill.get("position", ""),
            "avg_entry_after": fill.get("avg_entry", ""),
            "realized_pnl": fill.get("realized_pnl", ""),
            "unrealized_pnl": fill.get("unrealized_pnl", ""),
            "position_state": fill.get("state", ""),
            "market_data_latency_mode": latency_mode,
            "market_data_latency_profile_id": (
                latency_simulator.profile_id if latency_simulator is not None else ""
            ),
        }
        _flatten_flow(row, state, flow_horizons)
        sign = 1.0 if fill["side"] == "BUY" else -1.0
        for horizon_ms in markout_horizons:
            future_mid, age_ms = _asof_scalar(
                execution_ts,
                execution_mid,
                fill_ns + horizon_ms * 1_000_000,
                max_age_ns=max_age_ns,
            )
            key = f"{horizon_ms}ms"
            markout = (
                sign * (future_mid - float(fill["price"]))
                / float(fill["price"])
                * 10_000.0
                if math.isfinite(future_mid)
                else math.nan
            )
            row[f"future_mid_{key}"] = future_mid
            row[f"future_book_age_{key}"] = age_ms
            row[f"markout_{key}_bps"] = markout
            row[f"toxic_{key}"] = int(markout < 0.0) if math.isfinite(markout) else ""
        output.append(row)
    return output


def _mean(values: Iterable[float]) -> float:
    finite = [float(value) for value in values if math.isfinite(float(value))]
    return float(np.mean(finite)) if finite else math.nan


def summarize_sorting(
    rows: list[dict[str, Any]],
    *,
    flow_horizons_ms: Iterable[int],
    markout_horizons_ms: Iterable[int],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[("ALL", str(row["side"]))].append(row)
        grouped[(str(row["day"]), str(row["side"]))].append(row)

    for (day, side), group in sorted(grouped.items()):
        side_sign = 1.0 if side == "BUY" else -1.0
        for flow_horizon in flow_horizons_ms:
            flow_key = f"global_flow_pressure_{int(flow_horizon)}ms"
            eligible = []
            for row in group:
                try:
                    edge = side_sign * float(row.get(flow_key, math.nan))
                except (TypeError, ValueError):
                    continue
                if math.isfinite(edge):
                    eligible.append((edge, row))
            if len(eligible) < 5:
                continue
            edges = np.asarray([edge for edge, _ in eligible], dtype=np.float64)
            low_cut, high_cut = np.quantile(edges, [0.2, 0.8])
            adverse = [row for edge, row in eligible if edge <= low_cut]
            favorable = [row for edge, row in eligible if edge >= high_cut]
            for markout_horizon in markout_horizons_ms:
                markout_key = f"markout_{int(markout_horizon)}ms_bps"
                adverse_mean = _mean(float(row.get(markout_key, math.nan)) for row in adverse)
                favorable_mean = _mean(
                    float(row.get(markout_key, math.nan)) for row in favorable
                )
                output.append(
                    {
                        "day": day,
                        "side": side,
                        "flow_horizon_ms": int(flow_horizon),
                        "markout_horizon_ms": int(markout_horizon),
                        "eligible_fills": len(eligible),
                        "adverse_fills": len(adverse),
                        "favorable_fills": len(favorable),
                        "side_edge_q20": float(low_cut),
                        "side_edge_q80": float(high_cut),
                        "adverse_markout_bps": adverse_mean,
                        "favorable_markout_bps": favorable_mean,
                        "favorable_minus_adverse_bps": favorable_mean - adverse_mean,
                    }
                )
    return output


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", action="append", required=True)
    parser.add_argument("--fills", type=Path, required=True)
    parser.add_argument("--output-prefix", type=Path, required=True)
    parser.add_argument("--execution-market-id", default="binance:perp:BTCUSDC")
    parser.add_argument("--execution-symbol", default="BTCUSDC")
    parser.add_argument("--reference-symbol", default="BTCUSDT")
    parser.add_argument(
        "--flow-horizons-ms", default=",".join(map(str, DEFAULT_FLOW_HORIZONS_MS))
    )
    parser.add_argument(
        "--markout-horizons-ms",
        default=",".join(map(str, DEFAULT_MARKOUT_HORIZONS_MS)),
    )
    parser.add_argument("--max-future-book-age-ms", type=int, default=500)
    parser.add_argument("--market-data-latency-profile", type=Path)
    parser.add_argument(
        "--market-data-latency-mode",
        choices=SIMULATION_MODES,
        default="captured",
        help=(
            "captured uses recorded feature-ready time; profile_* starts from "
            "exchange time and injects the labeled environment profile"
        ),
    )
    parser.add_argument("--market-data-latency-seed", type=int, default=7)
    parser.add_argument("--start-ts", default="")
    parser.add_argument("--end-ts", default="")
    parser.add_argument(
        "--include-trade-type",
        action="append",
        default=[],
        help="Maker fill trade type to include; defaults to OPEN and CLOSE",
    )
    args = parser.parse_args()

    paths = expand_inputs(args.input)
    if not paths:
        raise FileNotFoundError("no receive-time JSONL files matched")
    fills = load_fills(
        args.fills,
        start_ts=parse_ts(args.start_ts),
        end_ts=parse_ts(args.end_ts),
        include_trade_types=args.include_trade_type or ("OPEN", "CLOSE"),
    )
    flow_horizons = tuple(
        int(value) for value in args.flow_horizons_ms.split(",") if value.strip()
    )
    markout_horizons = tuple(
        int(value) for value in args.markout_horizons_ms.split(",") if value.strip()
    )
    latency_simulator = (
        MarketDataLatencySimulator.load(args.market_data_latency_profile)
        if args.market_data_latency_profile
        else None
    )
    if args.market_data_latency_mode.startswith("profile_") and latency_simulator is None:
        parser.error("profile latency mode requires --market-data-latency-profile")
    rows = build_fill_toxicity_rows(
        tape_paths=paths,
        fills=fills,
        execution_market_id=args.execution_market_id,
        execution_symbol=args.execution_symbol,
        reference_symbol=args.reference_symbol,
        flow_horizons_ms=flow_horizons,
        markout_horizons_ms=markout_horizons,
        max_future_book_age_ms=args.max_future_book_age_ms,
        latency_simulator=latency_simulator,
        latency_mode=args.market_data_latency_mode,
        latency_seed=args.market_data_latency_seed,
    )
    sorting = summarize_sorting(
        rows,
        flow_horizons_ms=flow_horizons,
        markout_horizons_ms=markout_horizons,
    )
    write_csv(args.output_prefix.with_suffix(".fills.csv"), rows)
    write_csv(args.output_prefix.with_suffix(".sorting.csv"), sorting)
    summary = {
        "status": "ok",
        "schema": "fill_toxicity.v1",
        "market_tape_schema": "market_tape.v1",
        "fills_loaded": len(fills),
        "fills_labeled": len(rows),
        "input_files": [str(path) for path in paths],
        "flow_horizons_ms": flow_horizons,
        "markout_horizons_ms": markout_horizons,
        "policy_effect": "none_shadow_only",
        "market_data_latency_mode": args.market_data_latency_mode,
        "market_data_latency_profile_id": (
            latency_simulator.profile_id if latency_simulator is not None else ""
        ),
        "market_data_latency_seed": args.market_data_latency_seed,
        "interpretation": (
            "receive-time fill-toxicity sorting; not a keep/cancel/re-center "
            "counterfactual and not policy uplift"
        ),
    }
    args.output_prefix.with_suffix(".json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
