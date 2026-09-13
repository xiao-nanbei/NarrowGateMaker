"""Small JSON scorer for quote-time fill-selection evidence.

The live arm uses the same smoothed-bin model written by
research/families/f05_fill_quality_quote_ev/audit/fill_selection_score.py.  Keeping the scorer dependency-free makes
it safe for the live process: no pandas, no sklearn, no training-time imports.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping


def _finite_float(value: Any, default: float = math.nan) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def _sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def _numeric_bin(value: Any, cuts: list[float]) -> str:
    v = _finite_float(value)
    if not math.isfinite(v):
        return "missing"
    idx = 0
    while idx < len(cuts) and v >= cuts[idx]:
        idx += 1
    return f"b{idx:02d}"


def _categorical_bin(value: Any, max_len: int = 80) -> str:
    text = str(value or "").strip()
    return text[:max_len] if text else "missing"


@dataclass(frozen=True)
class FillSelectionScore:
    score: float
    missing_features: int
    used_features: int
    model_count: int


def fill_selection_actionable(
    *,
    threshold_hit: bool,
    allow_post: bool,
    allow_exposure_increase: bool,
    hard_reason_active: bool,
) -> bool:
    """Return the shared live/replay action gate for a scorer hit."""

    return bool(
        threshold_hit
        and allow_post
        and allow_exposure_increase
        and not hard_reason_active
    )


def build_fill_selection_feature_row(
    *,
    prediction_features: Mapping[str, Any] | None,
    quote_context: Mapping[str, Any] | None,
    side: str,
    inventory: float,
    max_inventory: float,
    mid: float,
    base_price: float,
    allow_post: bool,
    allow_exposure_increase: bool,
    exposure_increasing: bool,
    near_depth_total: float,
    toxicity: float,
    markout_ema: float,
    queue_local_rank: float,
    materialize: Callable[[dict[str, Any]], Any] | None = None,
) -> dict[str, Any]:
    """Build the canonical live/replay row consumed by the BUY scorer.

    Prediction features are the causal completed-bucket state. Quote context
    then overrides overlapping fields with the current side-specific state.
    The final fields below match the order-level training contract.
    """

    features = dict(prediction_features or {})
    features.update(dict(quote_context or {}))
    features["side"] = str(side).upper()
    features["inventory_ratio"] = float(inventory) / max(float(max_inventory), 1e-9)
    features["quote_action"] = "place"
    features["quote_allow_post"] = int(bool(allow_post))
    features["quote_allow_exposure_increase"] = int(bool(allow_exposure_increase))
    features["order_exposure_increasing"] = int(bool(exposure_increasing))
    features["fill_eligible"] = bool(allow_post and allow_exposure_increase)
    features["near_depth_total"] = max(0.0, float(near_depth_total))
    features["toxicity"] = float(toxicity)
    features["markout_ema"] = float(markout_ema)
    features["queue_local_rank"] = float(queue_local_rank)
    if mid > 0.0 and base_price > 0.0:
        features["quote_distance_bps"] = abs(float(mid) - float(base_price)) / float(mid) * 10000.0
    else:
        features["quote_distance_bps"] = 0.0
    if materialize is not None:
        materialize(features)
    return features


class FillSelectionScoreEnsemble:
    """Average scorer for the retained-day fold ensemble."""

    def __init__(self, payload: dict[str, Any]):
        folds = payload.get("folds", [])
        self._models = [f.get("model", f) for f in folds if isinstance(f, dict)]
        if not self._models and isinstance(payload.get("model"), dict):
            self._models = [payload["model"]]
        if not self._models:
            raise ValueError("fill-selection model JSON has no fold/model entries")

    @classmethod
    def load(cls, path: str | Path) -> "FillSelectionScoreEnsemble":
        p = Path(path)
        return cls(json.loads(p.read_text(encoding="utf-8")))

    def score(self, row: dict[str, Any]) -> FillSelectionScore:
        scores: list[float] = []
        missing_total = 0
        used_total = 0
        for model in self._models:
            score, missing, used = self._score_one(model, row)
            scores.append(score)
            missing_total += missing
            used_total += used
        n = max(len(scores), 1)
        return FillSelectionScore(
            score=sum(scores) / n if scores else 0.5,
            missing_features=int(round(missing_total / n)),
            used_features=int(round(used_total / n)),
            model_count=n,
        )

    @staticmethod
    def _score_one(model: dict[str, Any], row: dict[str, Any]) -> tuple[float, int, int]:
        base_logit = _finite_float(model.get("base_logit"), 0.0)
        total = base_logit
        used = 0
        missing = 0
        scale = _finite_float(model.get("contribution_scale"), 0.35)
        contributions = model.get("contributions", {}) or {}
        numeric_cuts = model.get("numeric_cuts", {}) or {}
        for feature, raw_cuts in numeric_cuts.items():
            cuts = [float(x) for x in raw_cuts]
            value = row.get(feature)
            if not math.isfinite(_finite_float(value)):
                missing += 1
            bucket = _numeric_bin(value, cuts)
            contrib = (contributions.get(feature, {}) or {}).get(bucket)
            if contrib is not None:
                total += scale * _finite_float(contrib, 0.0)
                used += 1
        for feature in model.get("categorical_features", []):
            value = row.get(feature, "")
            if str(value or "").strip() == "":
                missing += 1
            bucket = _categorical_bin(value)
            contrib = (contributions.get(feature, {}) or {}).get(bucket)
            if contrib is not None:
                total += scale * _finite_float(contrib, 0.0)
                used += 1
        if used:
            shrink = math.sqrt(used / (used + 4.0))
            total = base_logit + shrink * (total - base_logit)
        return _sigmoid(total), missing, used
