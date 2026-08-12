"""Audit whether public trades explain fills of the orders live actually placed.

This deliberately holds the live order path fixed. It validates the public
execution tape and the passive price-through rule separately from quote-policy,
inventory, and campaign path divergence in a strategy replay.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


SCHEMA_VERSION = "live_order_fill_closure.v1"


def _timestamp_ms(values: pd.Series) -> np.ndarray:
    numeric = pd.to_numeric(values, errors="coerce").to_numpy(dtype=np.float64)
    finite = numeric[np.isfinite(numeric)]
    if finite.size == 0:
        return np.zeros(len(values), dtype=np.int64)
    scale = 1000.0 if float(np.median(np.abs(finite))) < 1e12 else 1.0
    return np.rint(np.nan_to_num(numeric, nan=0.0) * scale).astype(np.int64)


def _normalize_buyer_maker(values: pd.Series) -> np.ndarray:
    if pd.api.types.is_bool_dtype(values.dtype):
        return values.to_numpy(dtype=np.bool_, copy=False)
    return (
        values.astype(str).str.strip().str.lower().eq("true").to_numpy(
            dtype=np.bool_, copy=False
        )
    )


def build_live_order_fill_closure(
    order_outcomes: pd.DataFrame,
    public_trades: pd.DataFrame,
    *,
    day_end_ms: int | None = None,
) -> pd.DataFrame:
    """Return one row per acknowledged passive order and its trade-through truth."""

    required_orders = {
        "timestamp",
        "event_type",
        "client_order_id",
        "side",
        "price",
    }
    required_trades = {"time", "price", "qty", "is_buyer_maker"}
    missing_orders = required_orders.difference(order_outcomes.columns)
    missing_trades = required_trades.difference(public_trades.columns)
    if missing_orders:
        raise ValueError(f"order outcomes missing columns: {sorted(missing_orders)}")
    if missing_trades:
        raise ValueError(f"public trades missing columns: {sorted(missing_trades)}")

    outcomes = order_outcomes.copy()
    outcomes["_timestamp_ms"] = _timestamp_ms(outcomes["timestamp"])
    outcomes["event_type"] = outcomes["event_type"].astype(str).str.lower()
    outcomes["side"] = outcomes["side"].astype(str).str.upper()

    trades = public_trades.copy()
    trades["_time_ms"] = pd.to_numeric(
        trades["time"], errors="coerce"
    ).fillna(0).astype(np.int64)
    trades["_price"] = pd.to_numeric(trades["price"], errors="coerce")
    trades["_qty"] = pd.to_numeric(trades["qty"], errors="coerce").fillna(0.0)
    trades["_buyer_maker"] = _normalize_buyer_maker(
        trades["is_buyer_maker"]
    )
    trades = trades.loc[
        (trades["_time_ms"] > 0)
        & trades["_price"].notna()
        & (trades["_qty"] >= 0.0)
    ].sort_values("_time_ms")
    if trades.empty:
        raise ValueError("public trade tape is empty after normalization")
    if day_end_ms is None:
        day_end_ms = int(trades["_time_ms"].max()) + 1

    side_tapes: dict[str, tuple[np.ndarray, np.ndarray, np.ndarray]] = {}
    for side, buyer_maker in (("BUY", True), ("SELL", False)):
        selected = trades.loc[trades["_buyer_maker"] == buyer_maker]
        side_tapes[side] = (
            selected["_time_ms"].to_numpy(dtype=np.int64, copy=False),
            selected["_price"].to_numpy(dtype=np.float64, copy=False),
            selected["_qty"].to_numpy(dtype=np.float64, copy=False),
        )

    rows: list[dict[str, object]] = []
    for client_order_id, group in outcomes.groupby("client_order_id", sort=False):
        placed = group.loc[group["event_type"] == "placed"].sort_values(
            "_timestamp_ms"
        )
        if placed.empty:
            # IOC/maker-close orders and orders carried across the UTC boundary
            # are different mechanisms and are not part of passive closure.
            continue
        placement = placed.iloc[0]
        side = str(placement["side"])
        if side not in side_tapes:
            continue
        price = float(placement["price"])
        start_ms = int(placement["_timestamp_ms"])
        terminal = group.loc[
            group["event_type"].isin(("filled", "canceled"))
        ].sort_values("_timestamp_ms")
        terminal_event = str(terminal.iloc[0]["event_type"]) if not terminal.empty else "open"
        end_ms = (
            int(terminal.iloc[0]["_timestamp_ms"])
            if not terminal.empty
            else int(day_end_ms)
        )
        actual_fill = bool((group["event_type"] == "filled").any())

        trade_ts, trade_price, trade_qty = side_tapes[side]
        left = int(np.searchsorted(trade_ts, start_ms, side="left"))
        right = int(np.searchsorted(trade_ts, end_ms, side="right"))
        window_price = trade_price[left:right]
        crossing = (
            window_price <= price + 1e-9
            if side == "BUY"
            else window_price >= price - 1e-9
        )
        crossing_offsets = np.flatnonzero(crossing)
        predicted_fill = bool(crossing_offsets.size)
        first_cross_ms = 0
        crossing_qty = 0.0
        if predicted_fill:
            first_cross_ms = int(trade_ts[left + int(crossing_offsets[0])])
            crossing_qty = float(trade_qty[left:right][crossing].sum())

        rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "client_order_id": str(client_order_id),
                "side": side,
                "price": price,
                "start_ms": start_ms,
                "end_ms": end_ms,
                "lifetime_ms": max(0, end_ms - start_ms),
                "terminal_event": terminal_event,
                "actual_fill": actual_fill,
                "predicted_price_through_fill": predicted_fill,
                "first_cross_ms": first_cross_ms,
                "first_cross_age_ms": (
                    first_cross_ms - start_ms if first_cross_ms else -1
                ),
                "crossing_public_qty": crossing_qty,
            }
        )
    return pd.DataFrame(rows)


def summarize_live_order_fill_closure(panel: pd.DataFrame) -> dict[str, object]:
    actual = panel["actual_fill"].astype(bool)
    predicted = panel["predicted_price_through_fill"].astype(bool)
    tp = int((actual & predicted).sum())
    fp = int((~actual & predicted).sum())
    fn = int((actual & ~predicted).sum())
    tn = int((~actual & ~predicted).sum())

    def ratio(numerator: int, denominator: int) -> float:
        return float(numerator / denominator) if denominator else float("nan")

    by_side: dict[str, dict[str, object]] = {}
    for side, group in panel.groupby("side", sort=True):
        side_actual = group["actual_fill"].astype(bool)
        side_predicted = group["predicted_price_through_fill"].astype(bool)
        side_tp = int((side_actual & side_predicted).sum())
        side_fp = int((~side_actual & side_predicted).sum())
        side_fn = int((side_actual & ~side_predicted).sum())
        by_side[str(side)] = {
            "orders": int(len(group)),
            "actual_fills": int(side_actual.sum()),
            "predicted_fills": int(side_predicted.sum()),
            "true_positive": side_tp,
            "false_positive": side_fp,
            "false_negative": side_fn,
            "recall": ratio(side_tp, side_tp + side_fn),
            "precision": ratio(side_tp, side_tp + side_fp),
        }
    return {
        "schema_version": SCHEMA_VERSION,
        "orders": int(len(panel)),
        "actual_fills": int(actual.sum()),
        "predicted_fills": int(predicted.sum()),
        "true_positive": tp,
        "false_positive": fp,
        "false_negative": fn,
        "true_negative": tn,
        "recall": ratio(tp, tp + fn),
        "precision": ratio(tp, tp + fp),
        "specificity": ratio(tn, tn + fp),
        "by_side": by_side,
        "scope": "acknowledged passive maker orders only",
        "note": (
            "False positives can include cancel-request/ACK races because the "
            "order-outcome schema records cancel ACK time, not exchange cancel time."
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Close live passive fills against public individual trades"
    )
    parser.add_argument("--order-outcomes", type=Path, required=True)
    parser.add_argument("--public-trades", type=Path, required=True)
    parser.add_argument("--out-prefix", type=Path, required=True)
    args = parser.parse_args()

    panel = build_live_order_fill_closure(
        pd.read_csv(args.order_outcomes),
        pd.read_csv(args.public_trades),
    )
    summary = summarize_live_order_fill_closure(panel)
    args.out_prefix.parent.mkdir(parents=True, exist_ok=True)
    panel_path = args.out_prefix.with_suffix(".orders.csv")
    json_path = args.out_prefix.with_suffix(".json")
    panel.to_csv(panel_path, index=False)
    json_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
