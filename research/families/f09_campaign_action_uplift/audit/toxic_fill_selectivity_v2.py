#!/usr/bin/env python3
"""Quantity-weighted selectivity contract for ranked execution guards.

This successor does not alter the frozen ``toxic_fill_selectivity.v1`` helper.
It supplies the common assigned-quantity denominator required by selective
execution actions and propagates missing 10-second BBO labels conservatively.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Any

import numpy as np
import pandas as pd

from research.families.f09_campaign_action_uplift.audit.toxic_fill_selectivity import (
    ToxicFillSelectivity,
    toxic_fill_selectivity,
)

SCHEMA_VERSION = "quantity_weighted_toxic_fill_selectivity.v2"


@dataclass(frozen=True)
class QuantityWeightedRates:
    action: str
    decisions: int
    assigned_qty_btc: float
    filled_qty_btc: float
    known_toxic_filled_qty_btc: float
    unlabeled_filled_qty_btc: float
    mean_fill_fraction: float
    mean_known_toxic_fraction: float
    mean_toxic_fraction_upper: float
    unlabeled_fill_fraction: float

    def to_payload(self) -> dict[str, Any]:
        return {"schema_version": SCHEMA_VERSION, **asdict(self)}


def maker_signed_net_markout_bps(
    *,
    side: str,
    fill_price: float,
    future_bbo_mid: float,
    maker_fee_bps: float = 0.0,
) -> float:
    """Return side-correct 10s BBO markout after known maker fees."""

    normalized = str(side).strip().upper()
    if normalized not in {"BUY", "SELL"}:
        raise ValueError("side must be BUY or SELL")
    fill = float(fill_price)
    future = float(future_bbo_mid)
    fee = float(maker_fee_bps)
    if not all(math.isfinite(value) for value in (fill, future, fee)):
        raise ValueError("markout inputs must be finite")
    if fill <= 0.0 or future <= 0.0 or fee < 0.0:
        raise ValueError("prices must be positive and maker_fee_bps nonnegative")
    signed_change = future - fill if normalized == "BUY" else fill - future
    return signed_change / fill * 10_000.0 - fee


def is_toxic_net_markout(
    net_markout_bps: float,
    *,
    epsilon_toxic_bps: float = 0.0,
) -> bool:
    value = float(net_markout_bps)
    epsilon = float(epsilon_toxic_bps)
    if not math.isfinite(value) or not math.isfinite(epsilon) or epsilon < 0.0:
        raise ValueError("markout and epsilon must be finite; epsilon must be nonnegative")
    return value < -epsilon


def quantity_weighted_rates(
    panel: pd.DataFrame,
    *,
    action: str,
) -> QuantityWeightedRates:
    """Compute E[quantity / assigned quantity] without dropping censored fills."""

    required = {
        "decision_id",
        "action",
        "assigned_qty_btc",
        "filled_qty_btc",
        "known_toxic_filled_qty_btc",
        "unlabeled_filled_qty_btc",
    }
    missing = sorted(required - set(panel.columns))
    if missing:
        raise ValueError(f"quantity selectivity panel missing columns: {missing}")
    if panel.duplicated("decision_id").any():
        raise ValueError("quantity selectivity panel contains duplicate decision_id")
    frame = panel[panel["action"].astype(str) == str(action)].copy()
    if frame.empty:
        raise ValueError(f"no rows for action {action}")
    numeric_columns = (
        "assigned_qty_btc",
        "filled_qty_btc",
        "known_toxic_filled_qty_btc",
        "unlabeled_filled_qty_btc",
    )
    for column in numeric_columns:
        frame[column] = pd.to_numeric(frame[column], errors="raise").astype(float)
        if not np.isfinite(frame[column]).all():
            raise ValueError(f"{column} contains non-finite values")
    assigned = frame["assigned_qty_btc"]
    filled = frame["filled_qty_btc"]
    toxic = frame["known_toxic_filled_qty_btc"]
    unlabeled = frame["unlabeled_filled_qty_btc"]
    tolerance = 1e-12
    if (assigned <= 0.0).any():
        raise ValueError("assigned_qty_btc must be positive")
    if ((filled < -tolerance) | (filled > assigned + tolerance)).any():
        raise ValueError("filled quantity must lie in [0, assigned]")
    if ((toxic < -tolerance) | (unlabeled < -tolerance)).any():
        raise ValueError("toxic and unlabeled quantities must be nonnegative")
    if (toxic + unlabeled > filled + tolerance).any():
        raise ValueError("toxic plus unlabeled quantity exceeds filled quantity")

    fill_fraction = filled / assigned
    toxic_fraction = toxic / assigned
    toxic_upper_fraction = (toxic + unlabeled) / assigned
    unlabeled_fraction = unlabeled / assigned
    return QuantityWeightedRates(
        action=str(action),
        decisions=int(len(frame)),
        assigned_qty_btc=float(assigned.sum()),
        filled_qty_btc=float(filled.sum()),
        known_toxic_filled_qty_btc=float(toxic.sum()),
        unlabeled_filled_qty_btc=float(unlabeled.sum()),
        mean_fill_fraction=float(fill_fraction.mean()),
        mean_known_toxic_fraction=float(toxic_fraction.mean()),
        mean_toxic_fraction_upper=float(toxic_upper_fraction.mean()),
        unlabeled_fill_fraction=float(unlabeled_fraction.mean()),
    )


def conservative_randomized_selectivity(
    panel: pd.DataFrame,
    *,
    baseline_action: str,
    candidate_action: str,
) -> dict[str, Any]:
    """Use the adverse censoring bound for a claimed toxicity improvement.

    Missing future-BBO labels remain in the common assigned-quantity
    denominator.  For the promotion-facing point, unlabeled candidate fills
    are treated as toxic while unlabeled baseline fills are treated as
    non-toxic.  Any positive result therefore survives the direction of
    censoring that is least favorable to the candidate.
    """

    baseline = quantity_weighted_rates(panel, action=baseline_action)
    candidate = quantity_weighted_rates(panel, action=candidate_action)
    selectivity: ToxicFillSelectivity = toxic_fill_selectivity(
        baseline_fill_rate=baseline.mean_fill_fraction,
        candidate_fill_rate=candidate.mean_fill_fraction,
        baseline_toxic_fill_rate=baseline.mean_known_toxic_fraction,
        candidate_toxic_fill_rate=candidate.mean_toxic_fraction_upper,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "estimand": "mean_quantity_fraction_on_common_assigned_decision_denominator",
        "missing_bbo_censor_rule": (
            "candidate_unlabeled_fill_is_toxic_and_baseline_unlabeled_fill_is_nontoxic"
        ),
        "baseline": baseline.to_payload(),
        "candidate": candidate.to_payload(),
        "conservative_selectivity": selectivity.to_payload(),
    }
