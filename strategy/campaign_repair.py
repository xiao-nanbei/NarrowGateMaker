"""Causal campaign-repair probability model shared by replay and shadow policy.

The model deliberately consumes only state available at the current quote
decision.  Terminal campaign labels are training targets and never appear in
``REPAIR_FEATURE_NAMES``.
"""

from __future__ import annotations

import json
import math
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


CAMPAIGN_REPAIR_MODEL_SCHEMA_VERSION = "campaign_repair_probability.v1"

# Keep this list small enough to reproduce exactly inside the live/replay state
# machine.  In particular, no fill outcome, future markout, final duration,
# terminal PnL, or terminal MAE field is allowed here.
REPAIR_FEATURE_NAMES = (
    "abs_inventory",
    "inventory_order_units",
    "inventory_limit_ratio",
    "campaign_age_s",
    "campaign_max_abs_qty_so_far",
    "campaign_pnl_so_far",
    "campaign_adverse_excursion_so_far",
    "campaign_exposure_increasing_fills_so_far",
    "campaign_reducing_fills_so_far",
    "campaign_add_minus_reduce_fills_so_far",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
    "l2_refill_edge",
    "l2_quote_flip_rate",
    "near_depth_total",
    "microprice_shift_inventory_adverse_bps",
    "toxicity",
    "inventory_side_markout_risk",
    "side_quote_fill_probability",
    "side_quote_markout_30s",
)


def _clip(value: float, lo: float, hi: float) -> float:
    return min(hi, max(lo, value))


def _sigmoid(value: float) -> float:
    if value >= 0.0:
        z = math.exp(-value)
        return 1.0 / (1.0 + z)
    z = math.exp(value)
    return z / (1.0 + z)


def _numeric_bin(value: float, cuts: tuple[float, ...]) -> str:
    if not math.isfinite(value):
        return "missing"
    index = 0
    while index < len(cuts) and value >= cuts[index]:
        index += 1
    return f"b{index:02d}"


def inventory_campaign_side(inventory: float) -> str:
    if inventory > 1e-12:
        return "LONG"
    if inventory < -1e-12:
        return "SHORT"
    return "FLAT"


def build_campaign_repair_features(
    *,
    inventory: float,
    order_size: float,
    max_inventory: float,
    campaign_age_s: float,
    campaign_max_abs_qty_so_far: float,
    campaign_pnl_so_far: float,
    campaign_adverse_excursion_so_far: float,
    campaign_exposure_increasing_fills_so_far: int,
    campaign_reducing_fills_so_far: int,
    l2_book_refresh_ratio: float,
    l2_book_cancel_ratio: float,
    l2_quote_flip_rate: float,
    near_depth_total: float,
    microprice_shift_bps: float,
    toxicity: float,
    markout_ema: float,
    side_quote_fill_probability: float,
    side_quote_markout_30s: float,
) -> dict[str, float]:
    """Build the exact decision-time feature vector used by the model."""

    side = inventory_campaign_side(inventory)
    inventory_sign = 1.0 if side == "LONG" else -1.0 if side == "SHORT" else 0.0
    # A positive value means the microprice is moving against the inventory.
    microprice_adverse = -inventory_sign * float(microprice_shift_bps)
    # Existing maker-signed EMA semantics are side-specific.  For a long
    # campaign the add side is BUY; for a short campaign it is SELL.
    side_markout_risk = (
        -float(markout_ema)
        if side == "LONG"
        else float(markout_ema)
        if side == "SHORT"
        else 0.0
    )
    add_fills = max(0, int(campaign_exposure_increasing_fills_so_far))
    reducing_fills = max(0, int(campaign_reducing_fills_so_far))
    return {
        "abs_inventory": abs(float(inventory)),
        "inventory_order_units": abs(float(inventory)) / max(float(order_size), 1e-12),
        "inventory_limit_ratio": abs(float(inventory)) / max(float(max_inventory), 1e-12),
        "campaign_age_s": max(0.0, float(campaign_age_s)),
        "campaign_max_abs_qty_so_far": max(
            abs(float(inventory)), float(campaign_max_abs_qty_so_far)
        ),
        "campaign_pnl_so_far": float(campaign_pnl_so_far),
        "campaign_adverse_excursion_so_far": min(
            0.0, float(campaign_adverse_excursion_so_far)
        ),
        "campaign_exposure_increasing_fills_so_far": float(add_fills),
        "campaign_reducing_fills_so_far": float(reducing_fills),
        "campaign_add_minus_reduce_fills_so_far": float(add_fills - reducing_fills),
        "l2_book_refresh_ratio": float(l2_book_refresh_ratio),
        "l2_book_cancel_ratio": float(l2_book_cancel_ratio),
        "l2_refill_edge": float(l2_book_refresh_ratio) - float(l2_book_cancel_ratio),
        "l2_quote_flip_rate": float(l2_quote_flip_rate),
        "near_depth_total": float(near_depth_total),
        "microprice_shift_inventory_adverse_bps": microprice_adverse,
        "toxicity": float(toxicity),
        "inventory_side_markout_risk": side_markout_risk,
        "side_quote_fill_probability": float(side_quote_fill_probability),
        "side_quote_markout_30s": float(side_quote_markout_30s),
    }


@dataclass(frozen=True)
class CampaignRepairSideModel:
    side: str
    base_rate: float
    base_logit: float
    numeric_cuts: dict[str, tuple[float, ...]]
    contributions: dict[str, dict[str, float]]
    contribution_scale: float = 0.35

    def score(self, features: dict[str, Any]) -> float:
        total = float(self.base_logit)
        used = 0
        for feature in REPAIR_FEATURE_NAMES:
            cuts = self.numeric_cuts.get(feature)
            if cuts is None:
                continue
            try:
                value = float(features.get(feature, math.nan))
            except (TypeError, ValueError):
                value = math.nan
            contribution = self.contributions.get(feature, {}).get(
                _numeric_bin(value, cuts)
            )
            if contribution is not None:
                total += self.contribution_scale * float(contribution)
                used += 1
        if used:
            shrink = math.sqrt(used / (used + 4.0))
            total = self.base_logit + shrink * (total - self.base_logit)
        return _clip(_sigmoid(total), 0.0, 1.0)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "CampaignRepairSideModel":
        return cls(
            side=str(payload["side"]).upper(),
            base_rate=float(payload["base_rate"]),
            base_logit=float(payload["base_logit"]),
            numeric_cuts={
                str(key): tuple(float(value) for value in values)
                for key, values in dict(payload.get("numeric_cuts", {})).items()
            },
            contributions={
                str(feature): {
                    str(bucket): float(value) for bucket, value in dict(buckets).items()
                }
                for feature, buckets in dict(payload.get("contributions", {})).items()
            },
            contribution_scale=float(payload.get("contribution_scale", 0.35)),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "side": self.side,
            "base_rate": self.base_rate,
            "base_logit": self.base_logit,
            "numeric_cuts": {
                key: list(values) for key, values in self.numeric_cuts.items()
            },
            "contributions": self.contributions,
            "contribution_scale": self.contribution_scale,
        }


@dataclass(frozen=True)
class CampaignRepairModel:
    long_model: CampaignRepairSideModel
    short_model: CampaignRepairSideModel
    model_id: str
    training_end_day: str
    metadata: dict[str, Any] = field(default_factory=dict)
    schema_version: str = CAMPAIGN_REPAIR_MODEL_SCHEMA_VERSION

    def score(self, inventory: float, features: dict[str, Any]) -> float:
        side = inventory_campaign_side(inventory)
        if side == "LONG":
            return self.long_model.score(features)
        if side == "SHORT":
            return self.short_model.score(features)
        return math.nan

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "CampaignRepairModel":
        schema = str(payload.get("schema_version", ""))
        if schema != CAMPAIGN_REPAIR_MODEL_SCHEMA_VERSION:
            raise ValueError(
                f"unsupported campaign repair model schema={schema!r}; "
                f"expected {CAMPAIGN_REPAIR_MODEL_SCHEMA_VERSION!r}"
            )
        models = dict(payload.get("side_models", {}))
        return cls(
            long_model=CampaignRepairSideModel.from_dict(models["LONG"]),
            short_model=CampaignRepairSideModel.from_dict(models["SHORT"]),
            model_id=str(payload.get("model_id", "")),
            training_end_day=str(payload.get("training_end_day", "")),
            metadata=dict(payload.get("metadata", {})),
            schema_version=schema,
        )

    @classmethod
    def load(cls, path: str | Path) -> "CampaignRepairModel":
        return cls.from_dict(json.loads(Path(path).read_text(encoding="utf-8")))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "model_id": self.model_id,
            "training_end_day": self.training_end_day,
            "feature_names": list(REPAIR_FEATURE_NAMES),
            "feature_availability": "quote_decision_time_only",
            "metadata": self.metadata,
            "side_models": {
                "LONG": self.long_model.to_dict(),
                "SHORT": self.short_model.to_dict(),
            },
        }


class CampaignRepairProbabilityHistory:
    """Track causal score changes inside one candidate campaign path."""

    def __init__(self, *, max_history_ms: int = 3_600_000) -> None:
        self.max_history_ns = max(1, int(max_history_ms)) * 1_000_000
        self._campaign_id: int | None = None
        self._values: deque[tuple[int, float]] = deque()

    def reset(self) -> None:
        self._campaign_id = None
        self._values.clear()

    def update(
        self,
        *,
        campaign_id: int,
        ts_ns: int,
        probability: float,
        lookback_ms: int,
    ) -> tuple[float, float]:
        if self._campaign_id != int(campaign_id):
            self._campaign_id = int(campaign_id)
            self._values.clear()
        now_ns = int(ts_ns)
        value = float(probability)
        if self._values and now_ns < self._values[-1][0]:
            raise ValueError("campaign repair probability time regressed")
        if self._values and now_ns == self._values[-1][0]:
            self._values[-1] = (now_ns, value)
        else:
            self._values.append((now_ns, value))
        cutoff = now_ns - self.max_history_ns
        while len(self._values) > 1 and self._values[1][0] < cutoff:
            self._values.popleft()

        target_ns = now_ns - max(1, int(lookback_ms)) * 1_000_000
        prior = math.nan
        for sample_ns, sample_value in reversed(self._values):
            if sample_ns <= target_ns:
                prior = sample_value
                break
        change = value - prior if math.isfinite(prior) else math.nan
        return value, change
