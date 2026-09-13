"""Quote-level EV model helpers.

The preferred bundle decomposes quote EV into two online-safe pieces:

    expected EV = P(fill) * E(30s markout | fill)

That avoids training one very sparse regression where almost every unfilled
quote has a zero label. Runtime loading accepts only this canonical bundle;
historical direct-head artifacts remain research records, not executable ABI.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

from calendar_features import quote_calendar_feature_values


def quote_side_prefix(side: str) -> str:
    side_upper = str(side or "bid").upper()
    if side_upper in {"BUY", "BID"}:
        return "bid"
    if side_upper in {"SELL", "ASK"}:
        return "ask"
    raise ValueError(f"Unknown quote EV side={side!r}")


def quote_side_model_names(side: str) -> dict[str, Any]:
    prefix = quote_side_prefix(side)
    return {
        "fill_prob": f"{prefix}_fill_prob",
        "markout_buckets": {
            1: f"{prefix}_fill_markout_bucket_1s",
            5: f"{prefix}_fill_markout_bucket_5s",
            30: f"{prefix}_fill_markout_bucket_30s",
        },
        "extreme_adverse": f"{prefix}_extreme_adverse_given_fill",
    }


DEFAULT_MARKOUT_BUCKET_EDGES = [-100.0, -50.0, -10.0, 10.0, 50.0]
DEFAULT_MARKOUT_BUCKET_VALUES = [-150.0, -75.0, -30.0, 0.0, 30.0, 75.0]

DEFAULT_BID_QUOTE_FEATURES = [
    "raw_half_spread",
    "capped_half_spread",
    "raw_mid_shift",
    "raw_reservation_shift",
    "raw_asym_shift",
    "asym",
    "inventory",
    "inventory_ratio",
    "dir_signal",
    "pred_dir",
    "pred_ret",
    "tox_bid",
    "tox_ask",
    "book_imb",
    "microprice_shift_bps",
    "mo_ema_bid",
    "mo_ema_ask",
    "fair",
    "mid",
    "best_bid",
    "best_ask",
    "raw_pair_spread",
    "capped_pair_spread",
    "final_pair_spread",
    "raw_quote_delta_to_bbo",
    "pre_guard_delta_to_bbo",
    "final_quote_delta_to_bbo",
    "raw_distance_to_mid",
    "final_distance_to_mid",
    "raw_quote_skew",
    "final_quote_skew",
    "favored_by_raw_shift",
    "delta_cap",
    "mid_guard",
    "post_only",
    "final_compressed",
    "final_guard_changed",
    "any_constraint_changed",
]


@dataclass
class QuoteEVPrediction:
    expected_maker_markout_bps_per_opportunity_30s: float = 0.0
    toxic_30s: float = 0.0
    fill_prob: float = 0.0
    fill_markout_1s: float = 0.0
    fill_markout_5s: float = 0.0
    fill_markout_30s: float = 0.0
    toxic_given_fill_30s: float = 0.0
    extreme_adverse_given_fill: float = 0.0
    markout_bucket_probs: dict[int, list[float]] = field(default_factory=dict)

    @property
    def ev_30s(self) -> float:
        """Read-only compatibility alias for historical consumers."""
        return self.expected_maker_markout_bps_per_opportunity_30s


def clean_feature_value(value: Any) -> float:
    if isinstance(value, (bool, np.bool_)):
        return 1.0 if value else 0.0
    try:
        out = float(value)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(out):
        return 0.0
    return out


def feature_array(features: dict[str, Any], feature_cols: list[str]) -> np.ndarray:
    return np.array(
        [clean_feature_value(features.get(col, 0.0)) for col in feature_cols],
        dtype=np.float64,
    ).reshape(1, -1)


def add_quote_time_interaction_feature_values(
    features: dict[str, Any],
    *,
    threshold: float = 2e-5,
) -> dict[str, Any]:
    """Materialize quote-time xmarket interaction features for scalar paths.

    This mirrors research.families.f05_fill_quality_quote_ev.train_quote_ev.add_quote_time_interaction_features()
    without pandas so Python replay and live shadow can score the same quote EV
    bundle used by batch training/precompute.
    """

    side = str(features.get("side", "") or "").upper()
    pos = 1.0 if side in {"BUY", "BID"} else -1.0
    adv_values: list[float] = []
    available = True
    for horizon in (10, 30, 60):
        src = f"cv_ref_perp_ret_{horizon}s"
        raw = features.get(src, None)
        try:
            ret = float(raw)
        except (TypeError, ValueError):
            ret = float("nan")
        if not math.isfinite(ret):
            available = False
        # sign convention: BUY 持仓怕 reference 下跌，SELL 持仓怕 reference 上涨；
        # 因此 adverse = -position_sign * reference_return。
        adv = -pos * ret if math.isfinite(ret) else float("nan")
        features[f"quote_ref_adverse_ret_{horizon}s"] = adv if math.isfinite(adv) else 0.0
        adv_values.append(adv)
    finite_adv = [v for v in adv_values if math.isfinite(v)]
    ref_adv = max(finite_adv) if finite_adv else float("nan")
    features["quote_ref_adverse_ret_max"] = ref_adv if math.isfinite(ref_adv) else 0.0
    ref_fav = 1.0 if available and math.isfinite(ref_adv) and ref_adv < -threshold else 0.0
    features["quote_ref_favorable_gt2e5"] = ref_fav
    features["quote_ref_adverse_gt2e5"] = 1.0 if available and math.isfinite(ref_adv) and ref_adv > threshold else 0.0
    features["quote_ref_low_abs_lt2e5"] = 1.0 if available and math.isfinite(ref_adv) and abs(ref_adv) <= threshold else 0.0

    depth = clean_feature_value(
        features.get("l2_near_depth_total", features.get("near_depth_total", 0.0))
    )
    dist = abs(clean_feature_value(
        features.get("final_distance_to_mid", features.get("raw_distance_to_mid", 0.0))
    ))
    rank = clean_feature_value(features.get("queue_local_rank", 0.0))

    features["quote_depth_ge1"] = 1.0 if depth >= 1.0 else 0.0
    features["quote_depth_ge2"] = 1.0 if depth >= 2.0 else 0.0
    features["quote_depth_ge5"] = 1.0 if depth >= 5.0 else 0.0
    features["quote_dist_ge30"] = 1.0 if dist >= 30.0 else 0.0
    features["quote_dist_ge40"] = 1.0 if dist >= 40.0 else 0.0
    features["quote_dist_ge60"] = 1.0 if dist >= 60.0 else 0.0
    features["quote_dist_40_60"] = 1.0 if 40.0 <= dist < 60.0 else 0.0
    features["quote_rank_front_0_25"] = 1.0 if 0.0 <= rank < 0.25 else 0.0
    features["quote_rank_front_0p1_0p5"] = 1.0 if 0.10 <= rank < 0.50 else 0.0
    features["quote_rank_mid_0p25_0p75"] = 1.0 if 0.25 <= rank < 0.75 else 0.0
    features["quote_rank_back_0p25_0p9"] = 1.0 if 0.25 <= rank < 0.90 else 0.0
    features["quote_rank_back_0p5_0p9"] = 1.0 if 0.50 <= rank < 0.90 else 0.0

    for suffix in [
        "depth_ge1",
        "depth_ge2",
        "depth_ge5",
        "dist_ge30",
        "dist_ge40",
        "dist_ge60",
        "dist_40_60",
        "rank_front_0_25",
        "rank_front_0p1_0p5",
        "rank_mid_0p25_0p75",
        "rank_back_0p25_0p9",
        "rank_back_0p5_0p9",
    ]:
        features[f"quote_ref_fav_x_{suffix}"] = ref_fav * clean_feature_value(features.get(f"quote_{suffix}", 0.0))
    features["quote_ref_fav_x_depth_ge1_dist_ge30"] = (
        ref_fav * features["quote_depth_ge1"] * features["quote_dist_ge30"]
    )
    features["quote_ref_fav_x_depth_ge1_dist_ge40"] = (
        ref_fav * features["quote_depth_ge1"] * features["quote_dist_ge40"]
    )
    features["quote_ref_fav_x_depth_ge2_dist_ge30"] = (
        ref_fav * features["quote_depth_ge2"] * features["quote_dist_ge30"]
    )
    features["quote_ref_fav_x_depth_ge2_dist_ge40"] = (
        ref_fav * features["quote_depth_ge2"] * features["quote_dist_ge40"]
    )
    return features


def _first_feature_value(features: dict[str, Any], names: tuple[str, ...], default: float = 0.0) -> float:
    for name in names:
        if name in features:
            return clean_feature_value(features.get(name, default))
    return float(default)


def _clamp01(value: float) -> float:
    return min(1.0, max(0.0, float(value)))


def add_local_flow_quote_feature_values(features: dict[str, Any]) -> dict[str, Any]:
    """Materialize quote-time local-flow features for scalar scoring paths.

    This mirrors research.families.f05_fill_quality_quote_ev.train_quote_ev.add_local_flow_quote_features().  It is
    intentionally pure feature materialization: no live policy should branch on
    these fields directly unless a separately promoted quote EV model does so.
    """

    side = str(features.get("side", "") or "").upper()
    pos = 1.0 if side in {"BUY", "BID"} else -1.0

    adverse: dict[int, float] = {}
    for horizon in (5, 10, 30, 60):
        raw = clean_feature_value(features.get(f"taker_quote_imbalance_{horizon}s", 0.0))
        adv = -pos * raw
        adverse[horizon] = adv
        features[f"quote_local_adverse_flow_{horizon}s"] = adv

    features["quote_local_flow_deceleration_30s_5s"] = adverse[30] - adverse[5]
    features["quote_local_flow_deceleration_60s_10s"] = adverse[60] - adverse[10]
    features["quote_local_flow_reversal_score"] = -adverse[5] if adverse[30] > 0.10 else 0.0

    features["quote_local_pressure_absent"] = 1.0 if abs(adverse[30]) < 0.10 else 0.0
    features["quote_local_pressure_reversing"] = 1.0 if adverse[30] >= 0.10 and adverse[5] <= -0.10 else 0.0
    features["quote_local_pressure_decelerating"] = (
        1.0 if adverse[30] >= 0.35 and (adverse[30] - adverse[5]) >= 0.25 else 0.0
    )
    features["quote_local_pressure_mild_decelerating"] = (
        1.0 if adverse[30] >= 0.10 and (adverse[30] - adverse[5]) >= 0.15 else 0.0
    )
    features["quote_local_pressure_persistent"] = 1.0 if adverse[30] >= 0.35 and adverse[5] >= 0.35 else 0.0
    features["quote_local_favorable_persistent"] = 1.0 if adverse[30] <= -0.35 and adverse[5] <= -0.10 else 0.0

    refresh = _first_feature_value(
        features,
        ("l2_book_refresh_ratio_y", "l2_book_refresh_ratio_x", "l2_book_refresh_ratio"),
        0.0,
    )
    cancel = _first_feature_value(
        features,
        ("l2_book_cancel_ratio_y", "l2_book_cancel_ratio_x", "l2_book_cancel_ratio"),
        0.0,
    )
    refill_edge = refresh - cancel
    features["quote_local_depth_refill_edge"] = refill_edge
    features["quote_local_refill_dominant"] = 1.0 if refill_edge > 0.10 else 0.0
    features["quote_local_cancel_dominant"] = 1.0 if refill_edge < -0.10 else 0.0
    features["quote_local_depth_balanced"] = 1.0 if -0.10 <= refill_edge <= 0.10 else 0.0

    if "local_adverse_microprice" in features:
        adverse_micro = clean_feature_value(features.get("local_adverse_microprice", 0.0))
    else:
        raw_micro = _first_feature_value(
            features,
            ("l2_microprice_offset_bps", "microprice_shift_bps"),
            0.0,
        )
        adverse_micro = -pos * raw_micro
    features["quote_local_adverse_microprice_bps"] = adverse_micro
    features["quote_local_micro_favorable"] = 1.0 if adverse_micro < -0.25 else 0.0
    features["quote_local_micro_neutral"] = 1.0 if abs(adverse_micro) <= 0.25 else 0.0
    features["quote_local_micro_adverse"] = 1.0 if adverse_micro > 0.25 else 0.0

    dist = abs(clean_feature_value(
        features.get("final_distance_to_mid", features.get("raw_distance_to_mid", 0.0))
    ))
    rank = clean_feature_value(features.get("queue_local_rank", 0.0))
    sell = 1.0 if side in {"SELL", "ASK"} else 0.0
    fav = clean_feature_value(features.get("quote_local_favorable_persistent", 0.0))
    mild_decel = clean_feature_value(features.get("quote_local_pressure_mild_decelerating", 0.0))
    refill = clean_feature_value(features.get("quote_local_refill_dominant", 0.0))
    front_rank = 1.0 if 0.0 <= rank < 0.25 else 0.0
    back_rank = 1.0 if 0.50 <= rank < 0.90 else 0.0
    dist30_40 = 1.0 if 30.0 <= dist < 40.0 else 0.0

    features["quote_local_fav_persistent_x_refill"] = fav * refill
    features["quote_local_fav_persistent_x_front_rank"] = fav * front_rank
    features["quote_local_fav_persistent_x_dist30_40"] = fav * dist30_40
    features["quote_local_fav_persistent_x_sell"] = fav * sell
    features["quote_local_fav_persistent_x_sell_refill"] = fav * sell * refill
    features["quote_local_mild_decel_x_refill"] = mild_decel * refill
    features["quote_local_mild_decel_x_refill_back_rank"] = mild_decel * refill * back_rank
    return features


def add_local_resiliency_quote_feature_values(features: dict[str, Any]) -> dict[str, Any]:
    """Materialize the Stage-S local-resiliency score for scalar quote EV scoring."""

    add_local_flow_quote_feature_values(features)
    side = str(features.get("side", "") or "").upper()
    depth = max(
        0.0,
        _first_feature_value(
            features,
            ("near_depth_for_bucket", "l2_near_depth_total", "near_depth_total", "cpp_near_depth_total"),
            0.0,
        ),
    )
    rank = _clamp01(
        _first_feature_value(
            features,
            ("queue_rank_for_bucket", "queue_local_rank", "queue_ahead_ratio"),
            1.0,
        )
    )
    refill_edge = _first_feature_value(
        features,
        ("depth_refill_edge", "quote_local_depth_refill_edge"),
        0.0,
    )
    flow_decel = _first_feature_value(
        features,
        ("flow_deceleration_30s_5s", "quote_local_flow_deceleration_30s_5s"),
        0.0,
    )
    adverse_5s = _first_feature_value(
        features,
        ("adverse_flow_5s", "quote_local_adverse_flow_5s"),
        0.0,
    )

    depth_score = math.log1p(min(depth, 10.0)) / math.log1p(10.0)
    refill_score = _clamp01((refill_edge + 0.10) / 0.35)
    queue_score = _clamp01(1.0 - rank)
    flow_decay_score = _clamp01((flow_decel + 0.10) / 0.45)
    persistent_penalty = _clamp01(max(0.0, min(adverse_5s, 0.60)) / 0.60) * 0.20
    score = _clamp01(
        0.30 * depth_score
        + 0.25 * refill_score
        + 0.20 * queue_score
        + 0.25 * flow_decay_score
        - persistent_penalty
    )

    features["quote_resil_depth_component"] = depth_score
    features["quote_resil_refill_component"] = refill_score
    features["quote_resil_queue_component"] = queue_score
    features["quote_resil_flow_decay_component"] = flow_decay_score
    features["quote_resil_persistent_adverse_penalty"] = persistent_penalty
    features["quote_local_resiliency_score"] = score
    if score <= 0.25:
        code = 0.0
    elif score <= 0.45:
        code = 1.0
    elif score <= 0.65:
        code = 2.0
    else:
        code = 3.0
    features["quote_local_resiliency_bucket_code"] = code

    features["quote_resil_brittle"] = 1.0 if code == 0.0 else 0.0
    features["quote_resil_weak"] = 1.0 if code == 1.0 else 0.0
    features["quote_resil_mid"] = 1.0 if code == 2.0 else 0.0
    features["quote_resil_strong"] = 1.0 if code == 3.0 else 0.0
    features["quote_resil_depth_low"] = 1.0 if depth_score <= 0.35 else 0.0
    features["quote_resil_depth_mid"] = 1.0 if 0.35 < depth_score <= 0.65 else 0.0
    features["quote_resil_depth_high"] = 1.0 if depth_score > 0.65 else 0.0
    features["quote_resil_refill_low"] = 1.0 if refill_score <= 0.35 else 0.0
    features["quote_resil_refill_mid"] = 1.0 if 0.35 < refill_score <= 0.65 else 0.0
    features["quote_resil_refill_high"] = 1.0 if refill_score > 0.65 else 0.0
    features["quote_resil_queue_low"] = 1.0 if queue_score <= 0.35 else 0.0
    features["quote_resil_queue_mid"] = 1.0 if 0.35 < queue_score <= 0.65 else 0.0
    features["quote_resil_queue_high"] = 1.0 if queue_score > 0.65 else 0.0
    features["quote_resil_flow_decay_low"] = 1.0 if flow_decay_score <= 0.35 else 0.0
    features["quote_resil_flow_decay_mid"] = 1.0 if 0.35 < flow_decay_score <= 0.65 else 0.0
    features["quote_resil_flow_decay_high"] = 1.0 if flow_decay_score > 0.65 else 0.0
    sell = 1.0 if side in {"SELL", "ASK"} else 0.0
    features["quote_sell_x_resil_strong"] = sell * features["quote_resil_strong"]
    features["quote_sell_x_resil_queue_high_flow_high"] = (
        sell * features["quote_resil_queue_high"] * features["quote_resil_flow_decay_high"]
    )
    return features


def add_toxic_risk_quote_feature_values(features: dict[str, Any]) -> dict[str, Any]:
    """Materialize Stage-T toxic-risk features for scalar quote EV scoring.

    This mirrors research.families.f05_fill_quality_quote_ev.train_quote_ev.add_toxic_risk_quote_features() without
    pandas.  The fields are quote-time risk calibration inputs only; direct live
    quote-EV policy remains disabled unless separately promoted.
    """

    add_local_resiliency_quote_feature_values(features)
    side = str(features.get("side", "") or "").upper()
    pos = 1.0 if side in {"BUY", "BID"} else -1.0
    sell = 1.0 if side in {"SELL", "ASK"} else 0.0

    adverse_guard = any(
        _first_feature_value(features, (name,), 0.0) > 0.5
        for name in (
            "side_adverse",
            "side_adverse_pause",
            "adverse_toxicity",
            "adverse_markout",
            "adverse_thin_depth",
            "bid_adverse",
            "ask_adverse",
        )
    )
    defense_guard = any(
        _first_feature_value(features, (name,), 0.0) > 0.5
        for name in (
            "defense_guard",
            "defense_pause",
            "defense_markout",
            "defense_direction",
            "defense_microprice",
        )
    )
    guard_adverse_defense = 1.0 if adverse_guard and defense_guard else 0.0

    adverse_flow_30s = _first_feature_value(
        features,
        ("quote_local_adverse_flow_30s",),
        -pos * _first_feature_value(features, ("taker_quote_imbalance_30s",), 0.0),
    )
    flow_score = _clamp01((adverse_flow_30s - 0.10) / 0.40)
    flow_weak_adverse = 1.0 if 0.10 <= adverse_flow_30s < 0.35 else 0.0
    flow_strong_adverse = 1.0 if adverse_flow_30s >= 0.35 else 0.0

    refill_edge = _first_feature_value(features, ("quote_local_depth_refill_edge",), 0.0)
    refill_dominant = 1.0 if refill_edge > 0.10 else 0.0

    bid_mo = _first_feature_value(features, ("mo_ema_bid",), 0.0)
    ask_mo = _first_feature_value(features, ("mo_ema_ask",), 0.0)
    side_mo = bid_mo if side in {"BUY", "BID"} else ask_mo
    mo_score = _clamp01((-side_mo - 3.0) / 17.0)
    mo_neg10_neg3 = 1.0 if -10.0 <= side_mo < -3.0 else 0.0
    mo_lt_neg10 = 1.0 if side_mo < -10.0 else 0.0

    ref_adv = max(
        _first_feature_value(features, (f"quote_ref_adverse_ret_{horizon}s",), 0.0)
        for horizon in (10, 30, 60)
    )
    spot_adv_values: list[float] = []
    for prefix in ("cv_exec_spot", "cv_ref_spot"):
        for horizon in (10, 30, 60):
            spot_adv_values.append(-pos * _first_feature_value(features, (f"{prefix}_ret_{horizon}s",), 0.0))
    spot_adv = max(spot_adv_values) if spot_adv_values else 0.0

    ref_adverse = 1.0 if ref_adv > 2e-5 else 0.0
    ref_adverse = max(ref_adverse, 1.0 if _first_feature_value(features, ("quote_ref_adverse_gt2e5",), 0.0) > 0.5 else 0.0)
    spot_adverse = 1.0 if spot_adv > 2e-5 else 0.0
    spot_adverse = max(spot_adverse, 1.0 if _first_feature_value(features, ("quote_spot_adverse_gt2e5",), 0.0) > 0.5 else 0.0)
    ref_spot_adverse = ref_adverse * spot_adverse

    depth = _first_feature_value(
        features,
        ("near_depth_for_bucket", "l2_near_depth_total", "near_depth_total", "cpp_near_depth_total"),
        0.0,
    )
    rank = _clamp01(
        _first_feature_value(features, ("queue_rank_for_bucket", "queue_local_rank", "queue_ahead_ratio"), 1.0)
    )
    dist = abs(_first_feature_value(features, ("final_distance_to_mid", "raw_distance_to_mid"), 0.0))
    depth_0p5_1 = 1.0 if 0.5 <= depth < 1.0 else 0.0
    depth_1_2 = 1.0 if 1.0 <= depth < 2.0 else 0.0
    depth_0p5_2 = 1.0 if 0.5 <= depth < 2.0 else 0.0
    rank_front = 1.0 if 0.0 <= rank < 0.25 else 0.0
    rank_back = 1.0 if rank >= 0.75 else 0.0
    dist_30_40 = 1.0 if 30.0 <= dist < 40.0 else 0.0
    dist_40_60 = 1.0 if 40.0 <= dist < 60.0 else 0.0
    dist_30_60 = 1.0 if 30.0 <= dist < 60.0 else 0.0
    queue_depth_score = _clamp01(0.45 * depth_0p5_2 + 0.25 * rank_front + 0.15 * rank_back + 0.15 * dist_30_60)

    guard_flow_score = _clamp01(guard_adverse_defense * refill_dominant * (0.55 * flow_score + 0.45 * mo_score))
    xmarket_score = _clamp01(ref_spot_adverse * (0.50 + 0.30 * depth_0p5_2 + 0.20 * dist_30_60))
    toxic_score = _clamp01(0.42 * guard_flow_score + 0.28 * xmarket_score + 0.18 * queue_depth_score + 0.12 * mo_score)
    guard_flow_flag = 1.0 if (
        guard_adverse_defense > 0.5
        and refill_dominant > 0.5
        and (flow_weak_adverse > 0.5 or flow_strong_adverse > 0.5)
        and (mo_neg10_neg3 > 0.5 or mo_lt_neg10 > 0.5)
    ) else 0.0
    xmarket_local_flag = 1.0 if (
        ref_spot_adverse > 0.5
        and (depth_0p5_1 > 0.5 or depth_1_2 > 0.5 or dist_30_40 > 0.5 or dist_40_60 > 0.5)
    ) else 0.0

    features["quote_toxic_risk_score"] = toxic_score
    features["quote_sell_toxic_risk_score"] = sell * toxic_score
    features["quote_toxic_guard_flow_score"] = guard_flow_score
    features["quote_toxic_xmarket_score"] = xmarket_score
    features["quote_toxic_queue_depth_score"] = queue_depth_score
    features["quote_toxic_side_mo_ema"] = side_mo
    features["quote_toxic_adverse_flow_30s"] = adverse_flow_30s
    features["quote_toxic_ref_adverse_ret_max"] = ref_adv
    features["quote_toxic_spot_adverse_ret_max"] = spot_adv
    features["quote_toxic_guard_adverse_defense"] = guard_adverse_defense
    features["quote_toxic_refill_dominant"] = refill_dominant
    features["quote_toxic_flow_weak_adverse"] = flow_weak_adverse
    features["quote_toxic_flow_strong_adverse"] = flow_strong_adverse
    features["quote_toxic_mo_neg10_neg3"] = mo_neg10_neg3
    features["quote_toxic_mo_lt_neg10"] = mo_lt_neg10
    features["quote_toxic_ref_adverse"] = ref_adverse
    features["quote_toxic_spot_adverse"] = spot_adverse
    features["quote_toxic_ref_spot_adverse"] = ref_spot_adverse
    features["quote_toxic_depth_0p5_1"] = depth_0p5_1
    features["quote_toxic_depth_1_2"] = depth_1_2
    features["quote_toxic_depth_0p5_2"] = depth_0p5_2
    features["quote_toxic_rank_front_0_25"] = rank_front
    features["quote_toxic_rank_back_0_75_1"] = rank_back
    features["quote_toxic_dist_30_40"] = dist_30_40
    features["quote_toxic_dist_40_60"] = dist_40_60
    features["quote_toxic_guard_flow_flag"] = guard_flow_flag
    features["quote_toxic_xmarket_local_flag"] = xmarket_local_flag
    features["quote_sell_x_toxic_guard_flow"] = sell * guard_flow_flag
    features["quote_sell_x_toxic_ref_spot_adverse"] = sell * ref_spot_adverse
    features["quote_sell_x_toxic_xmarket_shallow_rank"] = sell * xmarket_local_flag * (rank_front + rank_back)
    return features


def add_micro_macro_quote_feature_values(features: dict[str, Any]) -> dict[str, Any]:
    """Materialize micro/macro quote features when raw path fields are present.

    中文说明：单笔 live scalar path 没有历史 mid window，因此这里不计算
    range/rv；只在 orders/labels 已经带有 quote-time path 字段时补齐 score。
    这些字段用于 shadow calibration，不是 live policy 开关。
    """

    quote_distance_micro = _first_feature_value(
        features,
        ("quote_distance_micro", "quote_distance_micro_10s"),
        float("nan"),
    )
    if quote_distance_micro == quote_distance_micro:
        micro_reach = 1.0 / (1.0 + max(0.0, quote_distance_micro) / 3.0)
    else:
        micro_reach = 0.0
    micro_macro_range = _first_feature_value(features, ("quote_micro_macro_range_ratio",), 0.0)
    micro_macro_vol = _first_feature_value(features, ("quote_micro_macro_vol_ratio",), 0.0)
    trend_eff_60 = _first_feature_value(features, ("quote_trend_efficiency_60s",), 0.0)
    trend_eff_300 = _first_feature_value(features, ("quote_trend_efficiency_300s",), 0.0)
    side_adv_60 = _first_feature_value(features, ("quote_side_trend_adverse_60s_bps",), 0.0)
    side_adv_300 = _first_feature_value(features, ("quote_side_trend_adverse_300s_bps",), 0.0)
    xmarket = _first_feature_value(features, ("quote_toxic_xmarket_score",), 0.0)
    trend_adverse_score = max(_clamp01(side_adv_60 / 4.0), _clamp01(side_adv_300 / 12.0))
    trend_eff_score = max(_clamp01((trend_eff_60 - 0.35) / 0.45), _clamp01((trend_eff_300 - 0.35) / 0.45))
    macro_dom = _clamp01((0.30 - micro_macro_range) / 0.30) * _clamp01((trend_eff_300 - 0.35) / 0.45)
    trend_inventory_risk = _clamp01(0.45 * trend_adverse_score + 0.25 * trend_eff_score + 0.15 * macro_dom + 0.15 * xmarket)
    micro_noise = _clamp01((micro_macro_range - 0.15) / 0.35) * _clamp01(1.0 - trend_eff_300 / 0.55)
    micro_vol = _clamp01((micro_macro_vol - 0.15) / 0.35)
    refill = _first_feature_value(features, ("quote_resil_refill_component",), 0.0)
    flow = _first_feature_value(features, ("quote_resil_flow_decay_component",), 0.0)
    micro_reversion = _clamp01(0.35 * micro_noise + 0.20 * micro_vol + 0.20 * refill + 0.15 * flow + 0.10 * (1.0 - _clamp01(xmarket)))
    side = str(features.get("side", features.get("quote_side", ""))).upper()
    sell = 1.0 if side in {"SELL", "ASK"} else 0.0
    features["quote_micro_fill_reach_score"] = micro_reach
    features["quote_trend_inventory_risk_score"] = trend_inventory_risk
    features["quote_micro_reversion_score"] = micro_reversion
    features["quote_sell_x_trend_inventory_risk"] = sell * trend_inventory_risk
    features["quote_sell_x_micro_reversion"] = sell * micro_reversion
    return features


def materialize_quote_ev_feature_values(features: dict[str, Any]) -> dict[str, Any]:
    """Materialize all optional scalar quote EV features used by new bundles."""

    add_quote_time_interaction_feature_values(features)
    quote_calendar_feature_values(features)
    add_toxic_risk_quote_feature_values(features)
    add_micro_macro_quote_feature_values(features)
    return features


class QuoteEVModel:
    """LightGBM quote EV/toxicity model bundle for one side (bid or ask)."""

    def __init__(self, fill_prob_model=None,
                 bucket_models: dict[int, Any] | None = None,
                 extreme_adverse_model=None,
                 fill_prob_features: list[str] | None = None,
                 bucket_features: dict[int, list[str]] | None = None,
                 bucket_values: dict[int, list[float]] | None = None,
                 bucket_classes: dict[int, list[int]] | None = None,
                 extreme_adverse_features: list[str] | None = None,
                 side: str = "bid"):
        self.side = quote_side_prefix(side)
        self.fill_prob_model = fill_prob_model
        self.bucket_models = bucket_models or {}
        self.extreme_adverse_model = extreme_adverse_model
        self.fill_prob_features = fill_prob_features or list(DEFAULT_BID_QUOTE_FEATURES)
        self.bucket_features = bucket_features or {}
        self.bucket_values = bucket_values or {}
        self.bucket_classes = bucket_classes or {}
        self.extreme_adverse_features = extreme_adverse_features or list(DEFAULT_BID_QUOTE_FEATURES)

    @classmethod
    def load(cls, model_dir: str | Path, side: str = "bid") -> QuoteEVModel:
        import lightgbm as lgb

        path = Path(model_dir).expanduser()
        names = quote_side_model_names(side)
        prefix = quote_side_prefix(side)
        fill_prob_path = path / f"{names['fill_prob']}.txt"
        bucket_paths = {
            horizon: path / f"{name}.txt"
            for horizon, name in names["markout_buckets"].items()
        }
        extreme_adverse_path = path / f"{names['extreme_adverse']}.txt"
        required_paths = [fill_prob_path, extreme_adverse_path, *bucket_paths.values()]
        missing_paths = [p.name for p in required_paths if not p.exists()]
        if missing_paths:
            raise FileNotFoundError(
                f"incomplete canonical {prefix} quote EV bundle in {path}: "
                + ", ".join(missing_paths)
            )

        def _load_meta(name: str) -> dict[str, Any]:
            meta_path = path / f"{name}_meta.json"
            if not meta_path.exists():
                return {}
            with open(meta_path) as f:
                return json.load(f)

        def _load_features(name: str) -> list[str]:
            meta = _load_meta(name)
            cols = meta.get("feature_cols") or meta.get("feature_columns") or []
            return list(cols) if cols else list(DEFAULT_BID_QUOTE_FEATURES)

        fill_prob_model = lgb.Booster(model_file=str(fill_prob_path))
        bucket_models = {
            horizon: lgb.Booster(model_file=str(model_path))
            for horizon, model_path in bucket_paths.items()
        }
        bucket_features = {}
        bucket_values = {}
        bucket_classes = {}
        for horizon, name in names["markout_buckets"].items():
            meta = _load_meta(name)
            bucket_features[horizon] = _load_features(name)
            values = meta.get("bucket_values") or DEFAULT_MARKOUT_BUCKET_VALUES
            bucket_values[horizon] = [float(v) for v in values]
            classes = meta.get("classes")
            if classes is None:
                classes = list(range(len(bucket_values[horizon])))
            bucket_classes[horizon] = [int(c) for c in classes]
        extreme_adverse_model = lgb.Booster(model_file=str(extreme_adverse_path))
        return cls(
            fill_prob_model=fill_prob_model,
            bucket_models=bucket_models,
            extreme_adverse_model=extreme_adverse_model,
            fill_prob_features=_load_features(names["fill_prob"]),
            bucket_features=bucket_features,
            bucket_values=bucket_values,
            bucket_classes=bucket_classes,
            extreme_adverse_features=_load_features(names["extreme_adverse"]),
            side=prefix,
        )

    @staticmethod
    def _bucket_expected_value(probs: np.ndarray, classes: list[int], values: list[float]) -> float:
        probs = np.asarray(probs, dtype=np.float64).reshape(-1)
        if not len(probs):
            return 0.0
        total = 0.0
        for idx, prob in enumerate(probs):
            cls = int(classes[idx]) if idx < len(classes) else idx
            value = values[cls] if 0 <= cls < len(values) else values[-1]
            total += float(prob) * float(value)
        return total

    def predict(self, features: dict[str, Any]) -> QuoteEVPrediction:
        ev = 0.0
        toxic = 0.0
        fill_prob = 0.0
        fill_markout = 0.0
        fill_markout_1s = 0.0
        fill_markout_5s = 0.0
        toxic_given_fill = 0.0
        extreme_adverse_given_fill = 0.0
        bucket_probs: dict[int, list[float]] = {}

        if self.fill_prob_model is not None:
            fill_prob = float(self.fill_prob_model.predict(feature_array(features, self.fill_prob_features))[0])
            fill_prob = max(0.0, min(1.0, fill_prob))
        if self.fill_prob_model is not None and self.bucket_models:
            for horizon, model in self.bucket_models.items():
                raw = np.asarray(model.predict(
                    feature_array(features, self.bucket_features.get(horizon, DEFAULT_BID_QUOTE_FEATURES))
                ))
                probs = raw[0] if raw.ndim == 2 else raw.reshape(-1)
                bucket_probs[horizon] = [float(p) for p in probs]
                expected = self._bucket_expected_value(
                    probs,
                    self.bucket_classes.get(horizon, list(range(len(probs)))),
                    self.bucket_values.get(horizon, DEFAULT_MARKOUT_BUCKET_VALUES),
                )
                if horizon == 1:
                    fill_markout_1s = expected
                elif horizon == 5:
                    fill_markout_5s = expected
                elif horizon == 30:
                    fill_markout = expected
            ev = fill_prob * fill_markout
            extreme_adverse_given_fill = float(self.extreme_adverse_model.predict(
                feature_array(features, self.extreme_adverse_features)
            )[0])
            extreme_adverse_given_fill = max(0.0, min(1.0, extreme_adverse_given_fill))
            toxic_given_fill = extreme_adverse_given_fill
            toxic = fill_prob * extreme_adverse_given_fill
        toxic = max(0.0, min(1.0, toxic))
        return QuoteEVPrediction(
            expected_maker_markout_bps_per_opportunity_30s=ev,
            toxic_30s=toxic,
            fill_prob=fill_prob,
            fill_markout_1s=fill_markout_1s,
            fill_markout_5s=fill_markout_5s,
            fill_markout_30s=fill_markout,
            toxic_given_fill_30s=toxic_given_fill,
            extreme_adverse_given_fill=extreme_adverse_given_fill,
            markout_bucket_probs=bucket_probs,
        )
