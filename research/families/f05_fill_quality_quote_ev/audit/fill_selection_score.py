"""Train a quote-time fill-selection quality score from order-level rows.

The goal is deliberately different from fill-probability modeling.  The input
table contains every placed order, but the supervised label is only defined for
filled orders: did this fill look less toxic or more repairable after execution?
All placed orders are still scored and reported as the denominator, so the
result can answer:

    "When this score is high, do the fills we receive close the selection gap?"

This module uses an explainable smoothed-bin log-odds model instead of a heavy
ML dependency.  It is intended as the first calibration layer before quote EV or
any tiny policy arm.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from collections import defaultdict
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field, replace
from datetime import date
from pathlib import Path
from typing import Any

TARGET_MODES = (
    "non_toxic",
    "beats_opportunity",
    "campaign_repair",
    "opportunity_or_campaign",
    "opportunity_and_campaign",
)

SPLIT_MODES = (
    "blocked_day",
    "walk_forward",
)


NUMERIC_FEATURES = (
    "quote_distance_bps",
    "quote_distance_micro",
    "near_depth_total",
    "exact_l2_spread_bps",
    "queue_init",
    "queue_left",
    "queue_local_rank",
    "queue_deplete_mult",
    "queue_mo_mult",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
    "microprice_shift_bps",
    "toxicity",
    "markout_ema",
    "side_quote_fill_prob",
    "side_quote_fill_markout_30s",
    "q_before",
    "campaign_age_s",
    "campaign_duration_s",
    "campaign_max_abs_qty",
    "campaign_total_pnl",
    "campaign_adverse_excursion",
    "campaign_exposure_increasing_fills",
    "campaign_reducing_fills",
    "fill_quality_score",
    "toxic_risk_score",
    "campaign_outcome_risk_score",
    "campaign_repair_weak_score",
    "campaign_lifecycle_intervention_score",
    "resiliency_score",
    "micro_reversion_score",
    "trend_inventory_risk_score",
    "reducing_burst_risk_score",
    "lifecycle_risk_score",
    "sell_resil_flow_decel",
    "sell_resil_rank",
    "sell_resil_refill_edge",
    "sell_resil_ref_adv",
    "sell_resil_spot_adv",
    "quote_distance_micro_5s",
    "quote_distance_micro_10s",
    "micro_macro_range_ratio",
    "micro_macro_vol_ratio",
    "inventory_horizon_range_ratio",
    "trend_efficiency_60s",
    "trend_efficiency_300s",
    "side_trend_adverse_60s_bps",
    "side_trend_adverse_300s_bps",
)

CATEGORICAL_FEATURES = (
    "side",
    "session_stack",
    "micro_macro_regime",
    "quote_action",
    "quote_allow_post",
    "quote_allow_exposure_increase",
    "order_exposure_increasing",
    "fill_eligible",
    "sell_resil_spot_available",
)


def _iter_rows(paths: Iterable[Path]) -> Iterator[dict[str, str]]:
    for path in paths:
        with path.open(newline="") as f:
            reader = csv.DictReader(f)
            yield from reader


def _side_allowed(row: dict[str, str], side_filter: str) -> bool:
    """Return whether a row belongs to the requested side-specific panel."""
    if side_filter == "ALL":
        return True
    return str(row.get("side", "")).upper() == side_filter


def _exposure_increasing(row: dict[str, str]) -> bool:
    return _int(row, "order_exposure_increasing", 0) > 0


def _row_allowed(row: dict[str, str], side_filter: str, exposure_increasing_only: bool) -> bool:
    if not _side_allowed(row, side_filter):
        return False
    if exposure_increasing_only and not _exposure_increasing(row):
        return False
    return True


def _float(row: dict[str, str], key: str, default: float = math.nan) -> float:
    try:
        value = row.get(key, "")
        return float(value) if value not in ("", None) else default
    except (TypeError, ValueError):
        return default


def _int(row: dict[str, str], key: str, default: int = 0) -> int:
    try:
        value = row.get(key, "")
        return int(float(value)) if value not in ("", None) else default
    except (TypeError, ValueError):
        return default


def _clip(v: float, lo: float, hi: float) -> float:
    return min(hi, max(lo, v))


def _logit(p: float) -> float:
    p = _clip(p, 1e-6, 1.0 - 1e-6)
    return math.log(p / (1.0 - p))


def _sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def _quantiles(values: list[float], bins: int) -> list[float]:
    values = sorted(v for v in values if math.isfinite(v))
    if not values or bins <= 1:
        return []
    out: list[float] = []
    n = len(values)
    for i in range(1, bins):
        idx = min(n - 1, max(0, int(i * n / bins)))
        q = values[idx]
        if not out or q > out[-1]:
            out.append(q)
    return out


def _numeric_bin(value: float, cuts: list[float]) -> str:
    if not math.isfinite(value):
        return "missing"
    idx = 0
    while idx < len(cuts) and value >= cuts[idx]:
        idx += 1
    return f"b{idx:02d}"


def _categorical_bin(value: str, max_len: int = 80) -> str:
    text = str(value or "").strip()
    if not text:
        return "missing"
    return text[:max_len]


def _day(row: dict[str, str]) -> str:
    raw = row.get("day")
    day = str(raw).strip() if raw is not None else ""
    if not day:
        raise ValueError(
            "order-level row is missing required non-empty 'day' identity; "
            "timestamp-derived day fallback is not allowed"
        )
    try:
        parsed = date.fromisoformat(day)
    except ValueError as exc:
        raise ValueError(f"order-level row has invalid ISO day identity: {day!r}") from exc
    if parsed.isoformat() != day:
        raise ValueError(f"order-level row has non-canonical ISO day identity: {day!r}")
    return day


@dataclass
class LabelConfig:
    target_mode: str
    target_horizon_s: int
    min_markout_20s_bps: float
    min_markout_30s_bps: float
    tail_markout_30s_bps: float
    require_campaign_not_bad: bool
    opportunity_margin_bps: float
    min_terminal_pnl: float
    max_early_drawdown: float
    opportunity_benchmark: dict[tuple[str, str], float]


def _filled_order(row: dict[str, str]) -> bool:
    return _int(row, "filled") > 0


def _campaign_repair_good(row: dict[str, str], cfg: LabelConfig) -> bool | None:
    if not _filled_order(row):
        return None
    if not row.get("terminal_campaign_label", ""):
        return None
    repaired = _int(row, "terminal_campaign_repaired", 0) > 0
    bad = _int(row, "terminal_campaign_bad", 0) > 0
    tail = _int(row, "terminal_campaign_tail_loss", 0) > 0
    terminal_pnl = _float(row, "terminal_final_total_pnl_delta", math.nan)
    early_drawdown = _float(row, "terminal_early_drawdown_20m", math.nan)
    if math.isfinite(terminal_pnl) and terminal_pnl < cfg.min_terminal_pnl:
        return False
    if math.isfinite(early_drawdown) and early_drawdown > cfg.max_early_drawdown:
        return False
    return bool(repaired and not bad and not tail)


def _beats_opportunity_good(row: dict[str, str], cfg: LabelConfig) -> bool | None:
    if not _filled_order(row):
        return None
    day = _day(row)
    side = str(row.get("side", "")).upper()
    benchmark = cfg.opportunity_benchmark.get((day, side))
    actual = _float(row, f"markout_{cfg.target_horizon_s}s_bps", math.nan)
    if benchmark is None or not math.isfinite(benchmark) or not math.isfinite(actual):
        return None
    return actual >= benchmark + cfg.opportunity_margin_bps


def _non_toxic_good(row: dict[str, str], cfg: LabelConfig) -> bool | None:
    if not _filled_order(row):
        return None
    mo20 = _float(row, "markout_20s_bps")
    mo30 = _float(row, "markout_30s_bps")
    if not math.isfinite(mo20) and not math.isfinite(mo30):
        return None
    good_markout = True
    if math.isfinite(mo20):
        good_markout = good_markout and mo20 >= cfg.min_markout_20s_bps
    if math.isfinite(mo30):
        good_markout = good_markout and mo30 >= cfg.min_markout_30s_bps
    toxic_tail = math.isfinite(mo30) and mo30 <= cfg.tail_markout_30s_bps
    campaign_bad = _int(row, "terminal_campaign_bad", 0) > 0
    if cfg.require_campaign_not_bad and campaign_bad:
        good_markout = False
    return bool(good_markout and not toxic_tail)


def fill_selection_label(row: dict[str, str], cfg: LabelConfig) -> int | None:
    """Return 1 for desirable fills and 0 for undesirable fills.

    Unfilled orders are intentionally unlabeled.  They still receive a score and
    stay in denominator reports, but they must not teach the model that "not
    filled" is good.  The target mode decides what "desirable" means:

    - non_toxic: old markout/tail/campaign-bad label.
    - beats_opportunity: actual fill beats same-day/same-side random opportunity.
    - campaign_repair: filled order belongs to a repaired/non-bad campaign.
    - opportunity_or_campaign: union of the two stronger objectives.
    - opportunity_and_campaign: stricter intersection for diagnostics.
    """
    if cfg.target_mode == "non_toxic":
        result = _non_toxic_good(row, cfg)
    elif cfg.target_mode == "beats_opportunity":
        result = _beats_opportunity_good(row, cfg)
    elif cfg.target_mode == "campaign_repair":
        result = _campaign_repair_good(row, cfg)
    elif cfg.target_mode in {"opportunity_or_campaign", "opportunity_and_campaign"}:
        opportunity = _beats_opportunity_good(row, cfg)
        campaign = _campaign_repair_good(row, cfg)
        if opportunity is None and campaign is None:
            return None
        if cfg.target_mode == "opportunity_or_campaign":
            result = bool(opportunity) or bool(campaign)
        else:
            result = bool(opportunity) and bool(campaign)
    else:
        raise ValueError(f"unknown target mode: {cfg.target_mode}")
    if result is None:
        return None
    return 1 if result else 0


def _build_opportunity_benchmark(
    paths: list[Path],
    *,
    side_filter: str,
    exposure_increasing_only: bool,
    horizon_s: int,
    stat: str,
    allowed_days: set[str] | None = None,
) -> dict[tuple[str, str], float]:
    grouped: dict[tuple[str, str], list[float]] = defaultdict(list)
    key = f"opportunity_markout_{horizon_s}s_bps"
    for row in _iter_rows(paths):
        day = _day(row)
        if allowed_days is not None and day not in allowed_days:
            continue
        if not _row_allowed(row, side_filter, exposure_increasing_only):
            continue
        side = str(row.get("side", "")).upper()
        value = _float(row, key, math.nan)
        if side and math.isfinite(value):
            grouped[(day, side)].append(value)
    out: dict[tuple[str, str], float] = {}
    for k, values in grouped.items():
        if not values:
            continue
        values = sorted(values)
        if stat == "median":
            mid = len(values) // 2
            out[k] = values[mid] if len(values) % 2 else 0.5 * (values[mid - 1] + values[mid])
        elif stat == "mean":
            out[k] = sum(values) / len(values)
        else:
            raise ValueError(f"unsupported opportunity stat: {stat}")
    return out


@dataclass
class BinStat:
    n: int = 0
    good: int = 0


@dataclass
class ScoreModel:
    base_rate: float
    base_logit: float
    numeric_cuts: dict[str, list[float]]
    contributions: dict[str, dict[str, float]]
    contribution_scale: float
    clip_contribution: float

    def score(self, row: dict[str, str]) -> float:
        total = self.base_logit
        used = 0
        for feature, cuts in self.numeric_cuts.items():
            b = _numeric_bin(_float(row, feature), cuts)
            contrib = self.contributions.get(feature, {}).get(b)
            if contrib is not None:
                total += self.contribution_scale * contrib
                used += 1
        for feature in CATEGORICAL_FEATURES:
            b = _categorical_bin(row.get(feature, ""))
            contrib = self.contributions.get(feature, {}).get(b)
            if contrib is not None:
                total += self.contribution_scale * contrib
                used += 1
        # A tiny shrink toward the base rate prevents sparse rows from looking
        # overconfident while preserving rank.
        if used:
            shrink = math.sqrt(used / (used + 4.0))
            total = self.base_logit + shrink * (total - self.base_logit)
        return _sigmoid(total)

    def as_json(self) -> dict[str, Any]:
        return {
            "base_rate": self.base_rate,
            "base_logit": self.base_logit,
            "numeric_cuts": self.numeric_cuts,
            "contributions": self.contributions,
            "contribution_scale": self.contribution_scale,
            "clip_contribution": self.clip_contribution,
            "numeric_features": list(self.numeric_cuts),
            "categorical_features": list(CATEGORICAL_FEATURES),
        }


def fit_model(
    paths: list[Path],
    train_days: set[str],
    label_cfg: LabelConfig,
    *,
    side_filter: str,
    exposure_increasing_only: bool,
    bins: int,
    alpha: float,
    contribution_scale: float,
    clip_contribution: float,
) -> tuple[ScoreModel, list[dict[str, str]]]:
    values: dict[str, list[float]] = {f: [] for f in NUMERIC_FEATURES}
    labels: list[int] = []
    for row in _iter_rows(paths):
        if not _row_allowed(row, side_filter, exposure_increasing_only):
            continue
        if _day(row) not in train_days:
            continue
        label = fill_selection_label(row, label_cfg)
        if label is None:
            continue
        labels.append(label)
        for feature in NUMERIC_FEATURES:
            v = _float(row, feature)
            if math.isfinite(v):
                values[feature].append(v)

    base_rate = (sum(labels) + alpha) / (len(labels) + 2.0 * alpha) if labels else 0.5
    base_logit = _logit(base_rate)
    numeric_cuts = {f: _quantiles(v, bins) for f, v in values.items() if v}
    stats: dict[str, dict[str, BinStat]] = defaultdict(lambda: defaultdict(BinStat))
    for row in _iter_rows(paths):
        if not _row_allowed(row, side_filter, exposure_increasing_only):
            continue
        if _day(row) not in train_days:
            continue
        label = fill_selection_label(row, label_cfg)
        if label is None:
            continue
        for feature, cuts in numeric_cuts.items():
            b = _numeric_bin(_float(row, feature), cuts)
            s = stats[feature][b]
            s.n += 1
            s.good += label
        for feature in CATEGORICAL_FEATURES:
            b = _categorical_bin(row.get(feature, ""))
            s = stats[feature][b]
            s.n += 1
            s.good += label

    contributions: dict[str, dict[str, float]] = {}
    effect_rows: list[dict[str, str]] = []
    for feature, by_bin in sorted(stats.items()):
        contributions[feature] = {}
        for b, s in sorted(by_bin.items()):
            rate = (s.good + alpha * base_rate) / (s.n + alpha)
            contrib = _clip(_logit(rate) - base_logit, -clip_contribution, clip_contribution)
            contributions[feature][b] = contrib
            effect_rows.append(
                {
                    "feature": feature,
                    "bin": b,
                    "train_labeled_fills": str(s.n),
                    "train_good_fills": str(s.good),
                    "train_good_rate": f"{s.good / max(s.n, 1):.10f}",
                    "smoothed_good_rate": f"{rate:.10f}",
                    "logit_contribution": f"{contrib:.10f}",
                }
            )
    return (
        ScoreModel(
            base_rate=base_rate,
            base_logit=base_logit,
            numeric_cuts=numeric_cuts,
            contributions=contributions,
            contribution_scale=contribution_scale,
            clip_contribution=clip_contribution,
        ),
        effect_rows,
    )


@dataclass
class Acc:
    orders: int = 0
    fills: int = 0
    labeled_fills: int = 0
    good_fills: int = 0
    markout_20_sum: float = 0.0
    markout_30_sum: float = 0.0
    markout_20_weight: float = 0.0
    markout_30_weight: float = 0.0
    terminal_labeled_orders: int = 0
    terminal_labeled_campaigns: int = 0
    terminal_bad: int = 0
    terminal_tail: int = 0
    terminal_pnl_sum: float = 0.0
    early_drawdown_sum: float = 0.0
    score_sum: float = 0.0
    terminal_campaign_keys: set[tuple[str, str, str]] = field(
        default_factory=set,
        repr=False,
    )

    def add(self, row: dict[str, str], score: float, label: int | None) -> None:
        self.orders += 1
        self.score_sum += score
        filled = _int(row, "filled") > 0
        if filled:
            self.fills += 1
            fill_weight = _float(row, "filled_qty", 1.0)
            if not math.isfinite(fill_weight) or fill_weight <= 0.0:
                fill_weight = 1.0
            mo20 = _float(row, "markout_20s_bps")
            mo30 = _float(row, "markout_30s_bps")
            if math.isfinite(mo20):
                self.markout_20_sum += mo20 * fill_weight
                self.markout_20_weight += fill_weight
            if math.isfinite(mo30):
                self.markout_30_sum += mo30 * fill_weight
                self.markout_30_weight += fill_weight
        if label is not None:
            self.labeled_fills += 1
            self.good_fills += label
        if row.get("terminal_campaign_label", ""):
            self.terminal_labeled_orders += 1
            campaign_id = str(row.get("campaign_id", "")).strip()
            campaign_key = (
                _day(row),
                str(row.get("arm", "")).strip(),
                campaign_id,
            )
            if campaign_id and campaign_key in self.terminal_campaign_keys:
                return
            if campaign_id:
                self.terminal_campaign_keys.add(campaign_key)
            self.terminal_labeled_campaigns += 1
            self.terminal_bad += 1 if _int(row, "terminal_campaign_bad") > 0 else 0
            self.terminal_tail += 1 if _int(row, "terminal_campaign_tail_loss") > 0 else 0
            self.terminal_pnl_sum += _float(row, "terminal_final_total_pnl_delta", 0.0)
            self.early_drawdown_sum += _float(row, "terminal_early_drawdown_20m", 0.0)

    def as_row(self) -> dict[str, str]:
        return {
            "orders": str(self.orders),
            "fills": str(self.fills),
            "fill_rate": f"{self.fills / max(self.orders, 1):.10f}",
            "labeled_fills": str(self.labeled_fills),
            "target_good_rate": f"{self.good_fills / max(self.labeled_fills, 1):.10f}"
            if self.labeled_fills
            else "0.0000000000",
            "avg_score": f"{self.score_sum / max(self.orders, 1):.10f}",
            "avg_markout_20s_bps_filled": f"{self.markout_20_sum / self.markout_20_weight:.10f}"
            if self.markout_20_weight
            else "0.0000000000",
            "avg_markout_30s_bps_filled": f"{self.markout_30_sum / self.markout_30_weight:.10f}"
            if self.markout_30_weight
            else "0.0000000000",
            "terminal_labeled_orders": str(self.terminal_labeled_orders),
            "terminal_labeled_campaigns": str(self.terminal_labeled_campaigns),
            "terminal_bad_rate": f"{self.terminal_bad / self.terminal_labeled_campaigns:.10f}"
            if self.terminal_labeled_campaigns
            else "0.0000000000",
            "terminal_tail_rate": f"{self.terminal_tail / self.terminal_labeled_campaigns:.10f}"
            if self.terminal_labeled_campaigns
            else "0.0000000000",
            "avg_terminal_pnl": f"{self.terminal_pnl_sum / self.terminal_labeled_campaigns:.10f}"
            if self.terminal_labeled_campaigns
            else "0.0000000000",
            "avg_early_20m_drawdown": f"{self.early_drawdown_sum / self.terminal_labeled_campaigns:.10f}"
            if self.terminal_labeled_campaigns
            else "0.0000000000",
        }


def _score_bucket(score: float) -> str:
    idx = min(9, max(0, int(score * 10.0)))
    return f"score_{idx * 10:02d}_{(idx + 1) * 10:02d}"


def _json_identity(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _feature_contract_payload() -> dict[str, Any]:
    return {
        "contract_version": "fill_selection_quote_time_v1",
        "numeric_features": list(NUMERIC_FEATURES),
        "categorical_features": list(CATEGORICAL_FEATURES),
        "required_day_identity": "explicit_iso_day",
    }


def _model_spec_payload(
    args: argparse.Namespace,
    *,
    side_filter: str,
) -> dict[str, Any]:
    return {
        "model_type": "smoothed_bin_log_odds_v1",
        "model_spec_source": "cli_frozen_before_validation",
        "side_filter": side_filter,
        "exposure_increasing_only": bool(args.exposure_increasing_only),
        "bins": int(args.bins),
        "alpha": float(args.alpha),
        "contribution_scale": float(args.contribution_scale),
        "clip_contribution": float(args.clip_contribution),
        "target_mode": str(args.target_mode),
        "target_horizon_s": int(args.target_horizon_s),
        "opportunity_stat": str(args.opportunity_stat),
        "opportunity_margin_bps": float(args.opportunity_margin_bps),
        "min_markout_20s_bps": float(args.min_markout_20s_bps),
        "min_markout_30s_bps": float(args.min_markout_30s_bps),
        "tail_markout_30s_bps": float(args.tail_markout_30s_bps),
        "require_campaign_not_bad": bool(args.require_campaign_not_bad),
        "min_terminal_pnl": float(args.min_terminal_pnl),
        "max_early_drawdown": float(args.max_early_drawdown),
    }


def _select_score_threshold(
    development_oof_scores: list[float],
    *,
    explicit_threshold: float | None,
    quantile: float,
) -> tuple[float, str]:
    if explicit_threshold is not None:
        threshold = float(explicit_threshold)
        if not math.isfinite(threshold) or not 0.0 <= threshold <= 1.0:
            raise ValueError("score_threshold must be finite and within [0, 1]")
        return threshold, "pre_registered_cli"

    if not 0.0 <= quantile <= 1.0:
        raise ValueError("development_threshold_quantile must be within [0, 1]")
    scores = sorted(float(value) for value in development_oof_scores if math.isfinite(value))
    if not scores:
        raise ValueError("development OOF produced no finite scores for threshold selection")
    index = max(0, min(len(scores) - 1, math.ceil(quantile * len(scores)) - 1))
    return scores[index], "development_oof_score_quantile"


def _evaluate_frozen_model(
    paths: list[Path],
    *,
    evaluation_days_by_role: dict[str, set[str]],
    model: ScoreModel,
    label_cfg: LabelConfig,
    side_filter: str,
    exposure_increasing_only: bool,
    emit_all_side: bool,
    threshold: float,
    threshold_source: str,
    train_max_day: str,
    feature_identity: str,
    model_identity: str,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    role_by_day = {
        day: role for role, role_days in evaluation_days_by_role.items() for day in role_days
    }
    score_rows: list[dict[str, str]] = []
    metrics: dict[tuple[str, str, str], Acc] = defaultdict(Acc)
    for row_index, row in enumerate(_iter_rows(paths)):
        day = _day(row)
        day_role = role_by_day.get(day)
        if day_role is None:
            continue
        if not _row_allowed(row, side_filter, exposure_increasing_only):
            continue
        side = str(row.get("side", "UNKNOWN")).upper()
        value = model.score(row)
        label = fill_selection_label(row, label_cfg)
        selected = value >= threshold
        order_identity = (
            row.get("decision_id")
            or row.get("client_order_id")
            or row.get("order_id")
            or f"{day}:{row_index}"
        )
        score_rows.append(
            {
                "day": day,
                "day_role": day_role,
                "order_identity": str(order_identity),
                "side": side,
                "score": f"{value:.12f}",
                "score_bucket": _score_bucket(value),
                "threshold": f"{threshold:.12f}",
                "threshold_hit": "1" if selected else "0",
                "filled": "1" if _filled_order(row) else "0",
                "target_label": "" if label is None else str(label),
                "threshold_source": threshold_source,
                "train_max_day": train_max_day,
                "feature_identity": feature_identity,
                "model_identity": model_identity,
            }
        )
        sides = (side, "ALL") if emit_all_side else (side,)
        for metric_side in sides:
            metrics[(day_role, metric_side, "all")].add(row, value, label)
            cohort = "threshold_hit" if selected else "threshold_miss"
            metrics[(day_role, metric_side, cohort)].add(row, value, label)

    metric_rows = [
        {
            "day_role": day_role,
            "side": side,
            "threshold_group": threshold_group,
            "threshold": f"{threshold:.12f}",
            "threshold_source": threshold_source,
            "train_max_day": train_max_day,
            "feature_identity": feature_identity,
            "model_identity": model_identity,
            **acc.as_row(),
        }
        for (day_role, side, threshold_group), acc in sorted(metrics.items())
    ]
    return score_rows, metric_rows


def _fold_for_day(day: str, folds: int) -> int:
    digits = "".join(ch for ch in day if ch.isdigit())
    seed = int(digits or "0")
    return seed % max(folds, 1)


@dataclass(frozen=True)
class SplitFold:
    fold: int
    train_days: tuple[str, ...]
    embargo_days: tuple[str, ...]
    test_days: tuple[str, ...]


@dataclass(frozen=True)
class DayPartition:
    development_days: tuple[str, ...]
    embargo_before_validation_days: tuple[str, ...] = ()
    validation_days: tuple[str, ...] = ()
    embargo_before_holdout_days: tuple[str, ...] = ()
    holdout_days: tuple[str, ...] = ()

    def role_for_day(self, day: str) -> str:
        if day in self.development_days:
            return "development"
        if day in self.embargo_before_validation_days:
            return "embargo_before_validation"
        if day in self.validation_days:
            return "validation"
        if day in self.embargo_before_holdout_days:
            return "embargo_before_holdout"
        if day in self.holdout_days:
            return "holdout"
        raise KeyError(f"day {day!r} is not present in the frozen partition")

    def as_json(self) -> dict[str, list[str]]:
        return {
            "development_days": list(self.development_days),
            "embargo_before_validation_days": list(self.embargo_before_validation_days),
            "validation_days": list(self.validation_days),
            "embargo_before_holdout_days": list(self.embargo_before_holdout_days),
            "holdout_days": list(self.holdout_days),
        }


@dataclass(frozen=True)
class SplitPlan:
    mode: str
    partition: DayPartition
    folds: tuple[SplitFold, ...]

    @property
    def development_oof_days(self) -> tuple[str, ...]:
        return tuple(sorted({day for fold in self.folds for day in fold.test_days}))


def _validated_ordered_days(days: Iterable[str]) -> list[str]:
    ordered = sorted(set(days))
    if not ordered:
        raise ValueError("at least one day is required to build a split")
    for day in ordered:
        _day({"day": day})
    return ordered


def _take_tail(
    ordered: list[str],
    cursor: int,
    count: int,
    *,
    role: str,
) -> tuple[tuple[str, ...], int]:
    if count < 0:
        raise ValueError(f"{role} day count must be non-negative")
    if count > cursor:
        raise ValueError(f"not enough days to reserve {count} {role} days from {cursor} remaining")
    start = cursor - count
    return tuple(ordered[start:cursor]), start


def _walk_forward_partition(
    days: list[str],
    *,
    embargo_days: int,
    validation_days: int,
    holdout_days: int,
) -> DayPartition:
    if embargo_days < 0:
        raise ValueError("embargo_days must be non-negative")

    cursor = len(days)
    holdout, cursor = _take_tail(days, cursor, holdout_days, role="holdout")
    embargo_before_holdout: tuple[str, ...] = ()
    if holdout:
        embargo_before_holdout, cursor = _take_tail(
            days, cursor, embargo_days, role="pre-holdout embargo"
        )

    validation, cursor = _take_tail(days, cursor, validation_days, role="validation")
    embargo_before_validation: tuple[str, ...] = ()
    if validation:
        embargo_before_validation, cursor = _take_tail(
            days, cursor, embargo_days, role="pre-validation embargo"
        )

    development = tuple(days[:cursor])
    if not development:
        raise ValueError("walk-forward split leaves no development days")
    return DayPartition(
        development_days=development,
        embargo_before_validation_days=embargo_before_validation,
        validation_days=validation,
        embargo_before_holdout_days=embargo_before_holdout,
        holdout_days=holdout,
    )


def _walk_forward_folds(
    development_days: tuple[str, ...],
    *,
    min_train_days: int,
    embargo_days: int,
    test_days: int,
) -> tuple[SplitFold, ...]:
    if min_train_days < 1:
        raise ValueError("min_train_days must be at least 1")
    if embargo_days < 0:
        raise ValueError("embargo_days must be non-negative")
    if test_days < 1:
        raise ValueError("test_days must be at least 1")

    ordered = list(development_days)
    cursor = min_train_days
    folds: list[SplitFold] = []
    while cursor + embargo_days < len(ordered):
        test_start = cursor + embargo_days
        test_end = min(len(ordered), test_start + test_days)
        train = tuple(ordered[:cursor])
        embargo = tuple(ordered[cursor:test_start])
        test = tuple(ordered[test_start:test_end])
        if not train or not test or max(train) >= min(test):
            raise ValueError("walk-forward fold is not strictly chronological")
        if embargo and not (max(train) < min(embargo) <= max(embargo) < min(test)):
            raise ValueError("walk-forward embargo is not strictly chronological")
        folds.append(
            SplitFold(
                fold=len(folds),
                train_days=train,
                embargo_days=embargo,
                test_days=test,
            )
        )
        cursor = test_end

    if not folds:
        required = min_train_days + embargo_days + 1
        raise ValueError(
            "walk-forward development panel has no OOF fold: "
            f"need at least {required} days, got {len(ordered)}"
        )
    return tuple(folds)


def build_split_plan(
    days: Iterable[str],
    *,
    split_mode: str,
    blocked_folds: int = 5,
    min_train_days: int = 30,
    embargo_days: int = 1,
    test_days: int = 10,
    validation_days: int = 0,
    holdout_days: int = 0,
) -> SplitPlan:
    ordered = _validated_ordered_days(days)
    if split_mode not in SPLIT_MODES:
        raise ValueError(f"unknown split mode {split_mode!r}; expected one of {SPLIT_MODES}")

    if split_mode == "blocked_day":
        if validation_days or holdout_days:
            raise ValueError("validation/holdout roles require split_mode='walk_forward'")
        fold_count = max(1, min(int(blocked_folds), len(ordered)))
        folds: list[SplitFold] = []
        all_days = set(ordered)
        for fold in range(fold_count):
            test = tuple(d for d in ordered if _fold_for_day(d, fold_count) == fold)
            train = tuple(sorted(all_days - set(test)))
            if not train:
                train = tuple(ordered)
            folds.append(
                SplitFold(
                    fold=fold,
                    train_days=train,
                    embargo_days=(),
                    test_days=test,
                )
            )
        return SplitPlan(
            mode=split_mode,
            partition=DayPartition(development_days=tuple(ordered)),
            folds=tuple(folds),
        )

    partition = _walk_forward_partition(
        ordered,
        embargo_days=int(embargo_days),
        validation_days=int(validation_days),
        holdout_days=int(holdout_days),
    )
    return SplitPlan(
        mode=split_mode,
        partition=partition,
        folds=_walk_forward_folds(
            partition.development_days,
            min_train_days=int(min_train_days),
            embargo_days=int(embargo_days),
            test_days=int(test_days),
        ),
    )


def _day_role_rows(
    plan: SplitPlan,
    *,
    validation_evaluated: bool = False,
    holdout_evaluated: bool = False,
) -> list[dict[str, str]]:
    oof_days = set(plan.development_oof_days)
    ordered = sorted(
        (
            *plan.partition.development_days,
            *plan.partition.embargo_before_validation_days,
            *plan.partition.validation_days,
            *plan.partition.embargo_before_holdout_days,
            *plan.partition.holdout_days,
        )
    )
    rows: list[dict[str, str]] = []
    for day in ordered:
        role = plan.partition.role_for_day(day)
        if role == "development":
            outcome_status = "development_read"
            threshold_eligible = day in oof_days
        elif role.startswith("embargo_"):
            outcome_status = "embargo_unread"
            threshold_eligible = False
        elif role == "validation" and validation_evaluated:
            outcome_status = "frozen_model_evaluated"
            threshold_eligible = False
        elif role == "holdout" and holdout_evaluated:
            outcome_status = "sealed_holdout_evaluated"
            threshold_eligible = False
        else:
            outcome_status = "frozen_unread"
            threshold_eligible = False
        rows.append(
            {
                "day": day,
                "role": role,
                "outcome_status": outcome_status,
                "development_oof_threshold_eligible": ("1" if threshold_eligible else "0"),
            }
        )
    return rows


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _collect_days_for_side(
    paths: list[Path], side_filter: str, exposure_increasing_only: bool
) -> list[str]:
    days: set[str] = set()
    for row in _iter_rows(paths):
        day = _day(row)
        if _row_allowed(row, side_filter, exposure_increasing_only):
            days.add(day)
    return sorted(days)


def _load_day_universe(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        raise ValueError(f"day universe is empty: {path}")
    lines = text.splitlines()
    if lines[0].strip() == "day" or "," in lines[0]:
        with path.open(newline="") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames or "day" not in reader.fieldnames:
                raise ValueError(f"day-universe CSV requires a 'day' column: {path}")
            days = [str(row.get("day", "")).strip() for row in reader]
    else:
        days = [line.strip() for line in lines]
    return _validated_ordered_days(day for day in days if day)


def run(args: argparse.Namespace) -> dict[str, Any]:
    paths = [Path(p) for p in args.order_level_csv]
    side_filter = str(args.side).upper()
    emit_all_side = side_filter == "ALL"
    available_days = _collect_days_for_side(paths, side_filter, args.exposure_increasing_only)
    if not available_days:
        raise RuntimeError(f"no day values found in order-level CSV for side={side_filter}")
    day_universe_path = getattr(args, "day_universe_file", None)
    days = (
        _load_day_universe(Path(day_universe_path))
        if day_universe_path is not None
        else available_days
    )
    extra_days = sorted(set(available_days) - set(days))
    if extra_days:
        raise RuntimeError(
            "order-level inputs contain days outside the frozen day universe: "
            + ", ".join(extra_days)
        )
    split_plan = build_split_plan(
        days,
        split_mode=str(getattr(args, "split_mode", "blocked_day")),
        blocked_folds=int(args.folds),
        min_train_days=int(getattr(args, "min_train_days", 30)),
        embargo_days=int(getattr(args, "embargo_days", 1)),
        test_days=int(getattr(args, "test_days", 10)),
        validation_days=int(getattr(args, "validation_days", 0)),
        holdout_days=int(getattr(args, "holdout_days", 0)),
    )
    evaluate_holdout = bool(getattr(args, "evaluate_sealed_holdout", False))
    if evaluate_holdout and split_plan.mode != "walk_forward":
        raise ValueError("sealed holdout evaluation is only available in split_mode='walk_forward'")
    if evaluate_holdout and not split_plan.partition.holdout_days:
        raise ValueError("--evaluate-sealed-holdout requires at least one frozen holdout day")
    required_input_days = {
        *split_plan.partition.development_days,
        *split_plan.partition.validation_days,
    }
    if evaluate_holdout:
        required_input_days.update(split_plan.partition.holdout_days)
    missing_input_days = sorted(required_input_days - set(available_days))
    if missing_input_days:
        raise RuntimeError(
            "order-level inputs are missing required Development/Validation"
            + ("/Holdout" if evaluate_holdout else "")
            + " days from the frozen universe: "
            + ", ".join(missing_input_days)
        )
    development_days = set(split_plan.partition.development_days)
    opportunity_benchmark = _build_opportunity_benchmark(
        paths,
        side_filter=side_filter,
        exposure_increasing_only=args.exposure_increasing_only,
        horizon_s=args.target_horizon_s,
        stat=args.opportunity_stat,
        allowed_days=development_days,
    )
    label_cfg = LabelConfig(
        target_mode=args.target_mode,
        target_horizon_s=args.target_horizon_s,
        min_markout_20s_bps=args.min_markout_20s_bps,
        min_markout_30s_bps=args.min_markout_30s_bps,
        tail_markout_30s_bps=args.tail_markout_30s_bps,
        require_campaign_not_bad=args.require_campaign_not_bad,
        opportunity_margin_bps=args.opportunity_margin_bps,
        min_terminal_pnl=args.min_terminal_pnl,
        max_early_drawdown=args.max_early_drawdown,
        opportunity_benchmark=opportunity_benchmark,
    )

    calibration: dict[tuple[str, str, str], Acc] = defaultdict(Acc)
    daily: dict[tuple[str, str, str], Acc] = defaultdict(Acc)
    fold_summary: dict[tuple[str, str], Acc] = defaultdict(Acc)
    effect_rows_all: list[dict[str, str]] = []
    development_oof_scores: list[float] = []
    frozen_score_rows: list[dict[str, str]] = []
    frozen_metric_rows: list[dict[str, str]] = []
    frozen_artifact: dict[str, Any] | None = None
    selection_split = "all_test_days" if split_plan.mode == "blocked_day" else "development_oof"
    selection_scope = {
        "threshold_selection_role": "development_oof",
        "threshold_selection_days": list(split_plan.development_oof_days),
        "validation_outcomes_read": False,
        "holdout_outcomes_read": False,
    }
    model_json: dict[str, Any] = {
        "split_mode": split_plan.mode,
        "day_partition": split_plan.partition.as_json(),
        "selection_scope": selection_scope,
        "folds": [],
    }

    for fold_spec in split_plan.folds:
        train_days = set(fold_spec.train_days)
        test_days = set(fold_spec.test_days)
        model, effect_rows = fit_model(
            paths,
            train_days,
            label_cfg,
            side_filter=side_filter,
            exposure_increasing_only=args.exposure_increasing_only,
            bins=args.bins,
            alpha=args.alpha,
            contribution_scale=args.contribution_scale,
            clip_contribution=args.clip_contribution,
        )
        for r in effect_rows:
            effect_rows_all.append({"fold": str(fold_spec.fold), **r})
        model_json["folds"].append(
            {
                "fold": fold_spec.fold,
                "train_days": list(fold_spec.train_days),
                "embargo_days": list(fold_spec.embargo_days),
                "test_days": list(fold_spec.test_days),
                "model": model.as_json(),
            }
        )
        for row in _iter_rows(paths):
            if not _row_allowed(row, side_filter, args.exposure_increasing_only):
                continue
            day = _day(row)
            if day not in test_days:
                continue
            side = row.get("side", "UNKNOWN")
            score = model.score(row)
            development_oof_scores.append(score)
            bucket = _score_bucket(score)
            label = fill_selection_label(row, label_cfg)
            calibration[(side, bucket, selection_split)].add(row, score, label)
            if emit_all_side:
                calibration[("ALL", bucket, selection_split)].add(row, score, label)
            daily[(day, side, bucket)].add(row, score, label)
            if emit_all_side:
                daily[(day, "ALL", bucket)].add(row, score, label)
            fold_summary[(str(fold_spec.fold), side)].add(row, score, label)
            if emit_all_side:
                fold_summary[(str(fold_spec.fold), "ALL")].add(row, score, label)

    if split_plan.mode == "walk_forward":
        threshold, threshold_source = _select_score_threshold(
            development_oof_scores,
            explicit_threshold=getattr(args, "score_threshold", None),
            quantile=float(getattr(args, "development_threshold_quantile", 0.90)),
        )
        frozen_model, frozen_effect_rows = fit_model(
            paths,
            development_days,
            label_cfg,
            side_filter=side_filter,
            exposure_increasing_only=args.exposure_increasing_only,
            bins=args.bins,
            alpha=args.alpha,
            contribution_scale=args.contribution_scale,
            clip_contribution=args.clip_contribution,
        )
        for row in frozen_effect_rows:
            effect_rows_all.append({"fold": "frozen_development", **row})

        train_days = list(split_plan.partition.development_days)
        train_max_day = max(train_days)
        feature_contract = _feature_contract_payload()
        feature_identity = _json_identity(feature_contract)
        model_spec = _model_spec_payload(args, side_filter=side_filter)
        model_spec_identity = _json_identity(model_spec)
        frozen_model_payload = frozen_model.as_json()
        model_identity = _json_identity(
            {
                "feature_identity": feature_identity,
                "model_spec_identity": model_spec_identity,
                "train_days": train_days,
                "model": frozen_model_payload,
            }
        )
        threshold_metadata = {
            "value": threshold,
            "source": threshold_source,
            "development_oof_quantile": (
                None
                if getattr(args, "score_threshold", None) is not None
                else float(getattr(args, "development_threshold_quantile", 0.90))
            ),
            "selection_days": list(split_plan.development_oof_days),
        }
        frozen_artifact = {
            "artifact_version": "fill_selection_frozen_v1",
            "split_mode": split_plan.mode,
            "day_partition": split_plan.partition.as_json(),
            "train_days": train_days,
            "train_max_day": train_max_day,
            "feature_contract": feature_contract,
            "feature_identity": feature_identity,
            "model_spec": model_spec,
            "model_spec_identity": model_spec_identity,
            "model_identity": model_identity,
            "threshold": threshold_metadata,
            "holdout_gate": ("explicitly_open" if evaluate_holdout else "closed_by_default"),
            "model": frozen_model_payload,
            "folds": [
                {
                    "fold": "frozen_development",
                    "train_days": train_days,
                    "embargo_days": [],
                    "test_days": [],
                    "model": frozen_model_payload,
                }
            ],
        }
        model_json["frozen_model"] = frozen_artifact

        validation_days = set(split_plan.partition.validation_days)
        holdout_days = set(split_plan.partition.holdout_days) if evaluate_holdout else set()
        evaluation_days = validation_days | holdout_days
        evaluation_benchmark: dict[tuple[str, str], float] = {}
        if evaluation_days and args.target_mode in {
            "beats_opportunity",
            "opportunity_or_campaign",
            "opportunity_and_campaign",
        }:
            evaluation_benchmark = _build_opportunity_benchmark(
                paths,
                side_filter=side_filter,
                exposure_increasing_only=args.exposure_increasing_only,
                horizon_s=args.target_horizon_s,
                stat=args.opportunity_stat,
                allowed_days=evaluation_days,
            )
        evaluation_label_cfg = replace(
            label_cfg,
            opportunity_benchmark=evaluation_benchmark,
        )
        frozen_score_rows, frozen_metric_rows = _evaluate_frozen_model(
            paths,
            evaluation_days_by_role={
                "validation": validation_days,
                "holdout": holdout_days,
            },
            model=frozen_model,
            label_cfg=evaluation_label_cfg,
            side_filter=side_filter,
            exposure_increasing_only=args.exposure_increasing_only,
            emit_all_side=emit_all_side,
            threshold=threshold,
            threshold_source=threshold_source,
            train_max_day=train_max_day,
            feature_identity=feature_identity,
            model_identity=model_identity,
        )
        selection_scope.update(
            {
                "threshold": threshold,
                "threshold_source": threshold_source,
                "model_spec_source": model_spec["model_spec_source"],
                "frozen_train_max_day": train_max_day,
                "feature_identity": feature_identity,
                "model_identity": model_identity,
                "validation_outcomes_read": bool(validation_days),
                "holdout_gate": ("explicitly_open" if evaluate_holdout else "closed_by_default"),
                "holdout_outcomes_read": bool(holdout_days),
            }
        )

    cal_rows = [
        {"side": side, "score_bucket": bucket, "split": split, **acc.as_row()}
        for (side, bucket, split), acc in sorted(calibration.items())
    ]
    daily_rows = [
        {"day": day, "side": side, "score_bucket": bucket, **acc.as_row()}
        for (day, side, bucket), acc in sorted(daily.items())
    ]
    fold_rows = [
        {"fold": fold, "side": side, **acc.as_row()}
        for (fold, side), acc in sorted(fold_summary.items())
    ]

    out_prefix = Path(args.out_prefix)
    _write_csv(out_prefix.with_suffix(".fill_selection_calibration.csv"), cal_rows)
    _write_csv(out_prefix.with_suffix(".fill_selection_daily.csv"), daily_rows)
    _write_csv(out_prefix.with_suffix(".fill_selection_feature_effects.csv"), effect_rows_all)
    _write_csv(out_prefix.with_suffix(".fill_selection_folds.csv"), fold_rows)
    _write_csv(
        out_prefix.with_suffix(".fill_selection_day_roles.csv"),
        _day_role_rows(
            split_plan,
            validation_evaluated=(
                split_plan.mode == "walk_forward" and bool(split_plan.partition.validation_days)
            ),
            holdout_evaluated=(split_plan.mode == "walk_forward" and evaluate_holdout),
        ),
    )
    out_prefix.with_suffix(".fill_selection_model.json").write_text(
        json.dumps(model_json, indent=2, sort_keys=True), encoding="utf-8"
    )
    outputs = {
        "calibration": str(out_prefix.with_suffix(".fill_selection_calibration.csv")),
        "daily": str(out_prefix.with_suffix(".fill_selection_daily.csv")),
        "feature_effects": str(out_prefix.with_suffix(".fill_selection_feature_effects.csv")),
        "folds": str(out_prefix.with_suffix(".fill_selection_folds.csv")),
        "day_roles": str(out_prefix.with_suffix(".fill_selection_day_roles.csv")),
        "model": str(out_prefix.with_suffix(".fill_selection_model.json")),
    }
    if frozen_artifact is not None:
        frozen_model_path = out_prefix.with_suffix(".fill_selection_frozen_model.json")
        frozen_scores_path = out_prefix.with_suffix(".fill_selection_frozen_scores.csv")
        frozen_metrics_path = out_prefix.with_suffix(".fill_selection_frozen_metrics.csv")
        frozen_model_path.write_text(
            json.dumps(frozen_artifact, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        _write_csv(frozen_scores_path, frozen_score_rows)
        _write_csv(frozen_metrics_path, frozen_metric_rows)
        outputs.update(
            {
                "frozen_model": str(frozen_model_path),
                "frozen_scores": str(frozen_scores_path),
                "frozen_metrics": str(frozen_metrics_path),
            }
        )

    summary = {
        "order_level_csv": [str(p) for p in paths],
        "day_universe_file": (None if day_universe_path is None else str(day_universe_path)),
        "available_input_days": available_days,
        "days": days,
        "folds": len(split_plan.folds),
        "split_mode": split_plan.mode,
        "day_partition": split_plan.partition.as_json(),
        "selection_scope": selection_scope,
        "frozen_evaluation": (
            None
            if frozen_artifact is None
            else {
                "threshold": frozen_artifact["threshold"],
                "train_max_day": frozen_artifact["train_max_day"],
                "feature_identity": frozen_artifact["feature_identity"],
                "model_identity": frozen_artifact["model_identity"],
                "holdout_gate": frozen_artifact["holdout_gate"],
                "validation_score_rows": sum(
                    row["day_role"] == "validation" for row in frozen_score_rows
                ),
                "holdout_score_rows": sum(
                    row["day_role"] == "holdout" for row in frozen_score_rows
                ),
            }
        ),
        "side_filter": side_filter,
        "target": {
            "target_mode": args.target_mode,
            "target_horizon_s": args.target_horizon_s,
            "opportunity_stat": args.opportunity_stat,
            "opportunity_margin_bps": args.opportunity_margin_bps,
            "min_markout_20s_bps": args.min_markout_20s_bps,
            "min_markout_30s_bps": args.min_markout_30s_bps,
            "tail_markout_30s_bps": args.tail_markout_30s_bps,
            "require_campaign_not_bad": args.require_campaign_not_bad,
            "min_terminal_pnl": args.min_terminal_pnl,
            "max_early_drawdown": args.max_early_drawdown,
            "opportunity_benchmark_keys": len(opportunity_benchmark),
            "exposure_increasing_only": args.exposure_increasing_only,
        },
        "outputs": outputs,
    }
    out_prefix.with_suffix(".fill_selection_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8"
    )
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--order-level-csv",
        action="append",
        default=[],
        help="Order-level denominator CSV. Can be repeated.",
    )
    ap.add_argument(
        "--order-level-filelist",
        type=Path,
        default=None,
        help="Text file or CSV containing order-level CSV paths. CSV may use order_level_csv/path/file columns.",
    )
    ap.add_argument(
        "--day-universe-file",
        type=Path,
        default=None,
        help=(
            "Frozen chronological day universe, as one ISO day per line or a CSV "
            "with a day column. Closed holdout and embargo days may be listed "
            "without supplying their order-level files."
        ),
    )
    ap.add_argument("--out-prefix", required=True, help="Output prefix for calibration/model CSVs.")
    ap.add_argument(
        "--side",
        choices=["ALL", "BUY", "SELL"],
        default="ALL",
        help="Train and score only one side.",
    )
    ap.add_argument(
        "--exposure-increasing-only",
        action="store_true",
        help=(
            "Restrict the denominator/scoring panel to exposure-increasing orders. "
            "For --side SELL this isolates the add-short/open-short path instead "
            "of mixing it with sell-to-reduce-long repairs."
        ),
    )
    ap.add_argument(
        "--split-mode",
        choices=SPLIT_MODES,
        default="blocked_day",
        help=(
            "blocked_day preserves the historical date-hash diagnostic; "
            "walk_forward uses only past development days for expanding OOF fits."
        ),
    )
    ap.add_argument(
        "--folds",
        type=int,
        default=5,
        help="Number of folds for split_mode=blocked_day.",
    )
    ap.add_argument(
        "--min-train-days",
        type=int,
        default=30,
        help="Initial expanding training window for split_mode=walk_forward.",
    )
    ap.add_argument(
        "--embargo-days",
        type=int,
        default=1,
        help=(
            "Days excluded between walk-forward train/test blocks and between "
            "development, validation, and holdout panels."
        ),
    )
    ap.add_argument(
        "--test-days",
        type=int,
        default=10,
        help="Consecutive development OOF days per walk-forward fold.",
    )
    ap.add_argument(
        "--validation-days",
        type=int,
        default=0,
        help=(
            "Tail days frozen as validation; read only after Development OOF "
            "selection and full-Development model fitting."
        ),
    )
    ap.add_argument(
        "--holdout-days",
        type=int,
        default=0,
        help="Final tail days frozen as holdout and not read by development scoring.",
    )
    ap.add_argument(
        "--score-threshold",
        type=float,
        default=None,
        help=(
            "Pre-registered frozen score threshold. If omitted, select it from "
            "the Development OOF score quantile only."
        ),
    )
    ap.add_argument(
        "--development-threshold-quantile",
        type=float,
        default=0.90,
        help="Development OOF score quantile used when --score-threshold is omitted.",
    )
    ap.add_argument(
        "--evaluate-sealed-holdout",
        action="store_true",
        help=(
            "Explicitly open and evaluate the sealed holdout with the already "
            "frozen Development model and threshold. Default is closed."
        ),
    )
    ap.add_argument("--bins", type=int, default=5, help="Quantile bins per numeric feature.")
    ap.add_argument(
        "--alpha", type=float, default=20.0, help="Smoothing strength for bin target rates."
    )
    ap.add_argument(
        "--contribution-scale",
        type=float,
        default=0.35,
        help="Shrink factor for additive bin log-odds.",
    )
    ap.add_argument(
        "--clip-contribution", type=float, default=1.25, help="Per-bin logit contribution clip."
    )
    ap.add_argument(
        "--target-mode",
        choices=TARGET_MODES,
        default="non_toxic",
        help=(
            "Training label: old non-toxic fill, actual fill beating same-day/side "
            "random opportunity, terminal campaign repair, or a union/intersection."
        ),
    )
    ap.add_argument(
        "--target-horizon-s",
        type=int,
        choices=[20, 30],
        default=30,
        help="Horizon used by beats_opportunity target and opportunity benchmark.",
    )
    ap.add_argument(
        "--opportunity-stat",
        choices=["mean", "median"],
        default="mean",
        help="Same-day/same-side random opportunity benchmark statistic.",
    )
    ap.add_argument(
        "--opportunity-margin-bps",
        type=float,
        default=0.0,
        help="Extra bps actual fill must beat above the same-day/side opportunity benchmark.",
    )
    ap.add_argument(
        "--min-terminal-pnl",
        type=float,
        default=0.0,
        help="Minimum terminal campaign PnL delta for campaign_repair target.",
    )
    ap.add_argument(
        "--max-early-drawdown",
        type=float,
        default=1e9,
        help="Maximum early 20m drawdown allowed for campaign_repair target.",
    )
    ap.add_argument("--min-markout-20s-bps", type=float, default=0.0)
    ap.add_argument("--min-markout-30s-bps", type=float, default=-2.0)
    ap.add_argument("--tail-markout-30s-bps", type=float, default=-25.0)
    ap.add_argument("--require-campaign-not-bad", action="store_true")
    args = ap.parse_args(argv)
    if args.order_level_filelist:
        text = args.order_level_filelist.read_text(encoding="utf-8").strip()
        if text:
            lines = text.splitlines()
            if "," in lines[0] and not Path(lines[0]).exists():
                with args.order_level_filelist.open(newline="") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        value = (
                            row.get("order_level_csv") or row.get("path") or row.get("file") or ""
                        )
                        if value:
                            args.order_level_csv.append(value)
            else:
                args.order_level_csv.extend(line.strip() for line in lines if line.strip())
    if not args.order_level_csv:
        ap.error("provide --order-level-csv or --order-level-filelist")
    return args


def main(argv: list[str] | None = None) -> None:
    summary = run(parse_args(argv))
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
