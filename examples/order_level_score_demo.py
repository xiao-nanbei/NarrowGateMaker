#!/usr/bin/env python3
"""Tiny order-level score example.

This does not claim alpha. It shows how NarrowGate represents quote-time state
as denominator rows before any policy promotion.
"""

from __future__ import annotations

import json


def score_order(row: dict[str, float | str]) -> dict[str, float | str]:
    side = str(row["side"]).upper()
    quote_distance_micro = float(row["quote_distance_micro"])
    trend_inventory_risk = float(row["trend_inventory_risk"])
    local_repair = float(row["local_repair"])

    fill_probability_score = 1.0 / (1.0 + max(0.0, quote_distance_micro))
    fill_quality_score = local_repair - 0.5 * trend_inventory_risk
    lifecycle_risk_score = trend_inventory_risk - local_repair
    return {
        "side": side,
        "fill_probability_score": round(fill_probability_score, 4),
        "fill_quality_score": round(fill_quality_score, 4),
        "lifecycle_risk_score": round(lifecycle_risk_score, 4),
    }


if __name__ == "__main__":
    sample = {
        "side": "BUY",
        "quote_distance_micro": 1.5,
        "trend_inventory_risk": 0.7,
        "local_repair": 0.4,
    }
    print(json.dumps(score_order(sample), indent=2, sort_keys=True))
