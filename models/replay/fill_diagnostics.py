"""Offline fill diagnostics; never an input to order matching or accounting.

The retained price rule is the first replay price row at/after the target. The
replay array can contain timer rows with forward-filled trade prices; it is not
an independent exact-time market-price observation. A
delayed observation is explicitly a variable-endpoint estimate, not an exact
fixed-horizon value. No uncalibrated maximum-gap threshold is invented here.
"""

import math

import numpy as np

HORIZONS_S = (1, 5, 20, 30)


def fill_markout_diagnostics(trade_ts, trade_price, *, fill_ts_ms, quote_px,
                             side, fee_rate, outcome_end_ts_ms=None):
    """Return price-unit diagnostics, preserving unknown outcomes and endpoints.

    ``outcome_end_ts_ms`` is an inclusive allowed observation boundary, not a
    fabricated price. Missing future data never falls back to the last price.
    """
    if side not in ("BUY", "SELL"):
        raise ValueError("BUY or SELL required")
    if len(trade_ts) != len(trade_price):
        raise ValueError("trade timestamp/price lengths differ")
    out = {"fill_diagnostic_schema": "narrowgate.fill_markout.v2",
           "markout_unit": "USDC/BTC", "ev_unit": "USDC/BTC",
           "markout_price_source": "replay_trade_price_array_may_include_clock_rows"}
    for h in HORIZONS_S:
        prefix = f"markout_{h}s"
        target = int(fill_ts_ms) + h * 1000
        index = int(np.searchsorted(trade_ts, target, side="left"))
        actual = int(trade_ts[index]) if index < len(trade_ts) else None
        reason = None
        status = "exact" if actual == target else "delayed"
        if outcome_end_ts_ms is not None and target > outcome_end_ts_ms:
            reason, status = "requested_end_after_allowed_boundary", "censored"
        elif actual is None:
            reason, status = "no_future_trade", "censored"
        elif outcome_end_ts_ms is not None and actual > outcome_end_ts_ms:
            reason, status = "actual_end_after_allowed_boundary", "censored"
        elif not math.isfinite(float(trade_price[index])) or not math.isfinite(float(quote_px)):
            reason, status = "nonfinite_price", "missing"
        value = math.nan
        if reason is None:
            delta = float(trade_price[index]) - float(quote_px)
            value = delta if side == "BUY" else -delta
        out.update({
            prefix: value,
            f"ev_{h}s": value - float(fee_rate) * float(quote_px),
            f"toxic_{h}s": bool(value < 0) if math.isfinite(value) else None,
            f"{prefix}_requested_end_ms": target,
            f"{prefix}_actual_end_ms": actual,
            f"{prefix}_gap_ms": actual - target if actual is not None else None,
            f"{prefix}_method": "first_replay_price_at_or_after_target",
            f"{prefix}_status": status,
            f"{prefix}_censor_reason": reason,
            f"{prefix}_fixed_horizon_valid": status == "exact",
        })
    return out


def diagnostic_counts(rows):
    """Denominators include every retained fill, including unknown outcomes."""
    return {f"{h}s": {
        "total": len(rows),
        **{status: sum(r.get(f"markout_{h}s_status") == status for r in rows)
           for status in ("exact", "delayed", "censored", "missing")},
        "unclassified": sum(f"markout_{h}s_status" not in r for r in rows),
    } for h in HORIZONS_S}
