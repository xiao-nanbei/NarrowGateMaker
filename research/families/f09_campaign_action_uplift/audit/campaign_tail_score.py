#!/usr/bin/env python3
"""Train a side-specific tail-risk score for exposure-increasing add-on orders.

The denominator is every placed order whose quote-time ``inventory_role`` is
``add``.  Supervision is available only when that order fills while it is still
an add and its campaign has a terminal label.  Models are trained separately
for BUY-long additions and SELL-short additions with contiguous UTC-day folds.

This is an offline evidence tool.  Its output is an OOS score extension keyed
by ``(day, client_order_id)``; it does not alter replay or live policy.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from research.families.f10_live_replay_attribution.audit.metrics import inventory_role

# Every feature below is visible when the order is submitted.  Fill age,
# realized markout and terminal campaign fields are intentionally excluded.
NUMERIC_FEATURES = (
    "quote_distance_bps",
    "quote_distance_micro",
    "quote_distance_micro_5s",
    "quote_distance_micro_10s",
    "near_depth_total",
    "exact_l2_spread_bps",
    "queue_init",
    "queue_left",
    "queue_local_rank",
    "queue_regime_mult",
    "queue_mo_mult",
    "queue_deplete_mult",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
    "l2_quote_flip_rate",
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
    "micro_macro_range_ratio",
    "micro_macro_vol_ratio",
    "inventory_horizon_range_ratio",
    "trend_efficiency_60s",
    "trend_efficiency_300s",
    "side_trend_adverse_60s_bps",
    "side_trend_adverse_300s_bps",
    "micro_reversion_score",
    "trend_inventory_risk_score",
    "campaign_outcome_risk_score",
)

CATEGORICAL_FEATURES = (
    "session_stack",
    "micro_macro_regime",
    "quote_action",
    "quote_allow_post",
    "quote_allow_exposure_increase",
    "fill_eligible",
)

TARGET_MODES = ("loss_tail", "tail_or_open")


def _float(row: dict[str, Any], key: str, default: float = math.nan) -> float:
    try:
        value = row.get(key, "")
        return float(value) if value not in ("", None) else default
    except (TypeError, ValueError):
        return default


def _int(row: dict[str, Any], key: str, default: int = 0) -> int:
    try:
        value = row.get(key, "")
        return int(float(value)) if value not in ("", None) else default
    except (TypeError, ValueError):
        return default


def _day(row: dict[str, Any]) -> str:
    value = str(row.get("day", ""))
    if value:
        return value[:10]
    return str(row.get("utc", ""))[:10]


def _side(row: dict[str, Any]) -> str:
    value = str(row.get("side", "")).upper()
    return value if value in {"BUY", "SELL"} else ""


def _first_finite(row: dict[str, Any], *keys: str) -> float:
    for key in keys:
        value = _float(row, key)
        if math.isfinite(value):
            return value
    return math.nan


def _fill_ts(row: dict[str, Any]) -> float:
    value = _float(row, "fill_ts", 0.0)
    return value / 1000.0 if value > 10_000_000_000 else value


def _read_rows(path: Path, initial_inventory: float) -> list[dict[str, str]]:
    with path.open(newline="") as f:
        rows = list(csv.DictReader(f))

    for row in rows:
        role = str(row.get("inventory_role", ""))
        if role not in {"opener", "add", "reducing"}:
            role = inventory_role(_side(row), _float(row, "q_before"))
        row["inventory_role"] = role
        row["inventory_role_quote"] = role
        row["order_add_on"] = "1" if role == "add" else "0"

    # Old retained tables predate the exact fill-role fields.  Daily replay is
    # fresh-start, so chronological fills reconstruct the role without looking
    # at any future outcome.  New tables keep the exact trace value instead.
    running_q = initial_inventory
    fills = [row for row in rows if _int(row, "filled") > 0 and _fill_ts(row) > 0.0]
    fills.sort(key=lambda row: (_fill_ts(row), str(row.get("client_order_id", ""))))
    for row in fills:
        side = _side(row)
        qty = _float(row, "filled_qty", _float(row, "quantity", 0.0))
        exact_q = _first_finite(row, "fill_q_before", "inventory_before_fill", "q_before_fill")
        if math.isfinite(exact_q):
            q_before = exact_q
            running_q = exact_q
            source = str(row.get("fill_role_source") or "exact_trace")
        else:
            q_before = running_q
            source = "reconstructed_daily"
        role = inventory_role(side, q_before)
        row["fill_q_before"] = f"{q_before:.10f}"
        row["fill_inventory_role"] = role
        row["fill_role_source"] = source
        row["inventory_role_drift"] = "1" if role != row.get("inventory_role") else "0"
        if side == "BUY":
            running_q = q_before + qty
        elif side == "SELL":
            running_q = q_before - qty
    return rows


def _iter_paths(filelist: Path | None, paths: Iterable[str]) -> list[Path]:
    out = [Path(value) for value in paths]
    if filelist is not None:
        text = filelist.read_text(encoding="utf-8").strip()
        if text:
            lines = text.splitlines()
            if "," in lines[0]:
                with filelist.open(newline="") as f:
                    for row in csv.DictReader(f):
                        value = row.get("order_level_csv") or row.get("path") or row.get("file") or ""
                        if value:
                            out.append(Path(value))
            else:
                out.extend(Path(line.strip()) for line in lines if line.strip())
    unique: list[Path] = []
    seen: set[Path] = set()
    for path in out:
        resolved = path.expanduser()
        if resolved not in seen:
            seen.add(resolved)
            unique.append(resolved)
    return unique


def _tail_target(row: dict[str, Any], target_mode: str) -> int | None:
    label = str(row.get("terminal_campaign_label", ""))
    if not label:
        return None
    if target_mode == "loss_tail":
        # An open campaign at the UTC boundary is unresolved, not a safe zero.
        if label == "open_risk":
            return None
        return 1 if label == "loss_tail" else 0
    if target_mode == "tail_or_open":
        return 1 if label in {"loss_tail", "open_risk"} else 0
    raise ValueError(f"unknown target mode: {target_mode}")


def _preexisting_tail_state(row: dict[str, Any]) -> bool:
    max_inventory = _float(row, "campaign_max_abs_qty", 0.0)
    adverse_excursion = _float(row, "campaign_adverse_excursion", 0.0)
    return max_inventory >= 0.010 - 1e-12 or adverse_excursion <= -1.0 + 1e-12


def _is_labeled_add_fill(
    row: dict[str, Any],
    *,
    target_mode: str,
    include_preexisting_tail_state: bool,
) -> bool:
    return (
        row.get("inventory_role") == "add"
        and _int(row, "filled") > 0
        and row.get("fill_inventory_role") == "add"
        and _tail_target(row, target_mode) is not None
        and (include_preexisting_tail_state or not _preexisting_tail_state(row))
        and _side(row) in {"BUY", "SELL"}
    )


@dataclass
class Sample:
    day: str
    side: str
    campaign_id: str
    target: int
    row: dict[str, str]
    weight: float = 1.0


@dataclass
class WeightedStat:
    rows: int = 0
    weight: float = 0.0
    target_weight: float = 0.0


def _clip(value: float, lo: float, hi: float) -> float:
    return min(hi, max(lo, value))


def _logit(probability: float) -> float:
    probability = _clip(probability, 1e-6, 1.0 - 1e-6)
    return math.log(probability / (1.0 - probability))


def _sigmoid(value: float) -> float:
    if value >= 0.0:
        z = math.exp(-value)
        return 1.0 / (1.0 + z)
    z = math.exp(value)
    return z / (1.0 + z)


def _quantile_cuts(values: list[float], bins: int) -> list[float]:
    clean = sorted(value for value in values if math.isfinite(value))
    cuts: list[float] = []
    for idx in range(1, bins):
        if not clean:
            break
        value = clean[min(len(clean) - 1, int(idx * len(clean) / bins))]
        if not cuts or value > cuts[-1]:
            cuts.append(value)
    return cuts


def _numeric_bin(value: float, cuts: list[float]) -> str:
    if not math.isfinite(value):
        return "missing"
    idx = 0
    while idx < len(cuts) and value >= cuts[idx]:
        idx += 1
    return f"b{idx:02d}"


def _category(value: Any) -> str:
    text = str(value or "").strip()
    return text[:80] if text else "missing"


@dataclass
class TailModel:
    side: str
    base_rate: float
    base_logit: float
    numeric_cuts: dict[str, list[float]]
    contributions: dict[str, dict[str, float]]
    contribution_scale: float

    def score(self, row: dict[str, Any]) -> float:
        total = self.base_logit
        used = 0
        for feature, cuts in self.numeric_cuts.items():
            contribution = self.contributions.get(feature, {}).get(_numeric_bin(_float(row, feature), cuts))
            if contribution is not None:
                total += self.contribution_scale * contribution
                used += 1
        for feature in CATEGORICAL_FEATURES:
            contribution = self.contributions.get(feature, {}).get(_category(row.get(feature, "")))
            if contribution is not None:
                total += self.contribution_scale * contribution
                used += 1
        if used:
            shrink = math.sqrt(used / (used + 4.0))
            total = self.base_logit + shrink * (total - self.base_logit)
        return _sigmoid(total)

    def as_json(self) -> dict[str, Any]:
        return {
            "side": self.side,
            "base_rate": self.base_rate,
            "base_logit": self.base_logit,
            "numeric_cuts": self.numeric_cuts,
            "contributions": self.contributions,
            "contribution_scale": self.contribution_scale,
            "numeric_features": list(self.numeric_cuts),
            "categorical_features": list(CATEGORICAL_FEATURES),
        }


def _fit_model(
    samples: list[Sample],
    *,
    side: str,
    train_days: set[str],
    bins: int,
    alpha: float,
    contribution_scale: float,
    clip_contribution: float,
) -> tuple[TailModel, list[dict[str, Any]]]:
    train = [sample for sample in samples if sample.side == side and sample.day in train_days]
    total_weight = sum(sample.weight for sample in train)
    target_weight = sum(sample.weight * sample.target for sample in train)
    base_rate = (target_weight + alpha * 0.5) / (total_weight + alpha) if total_weight else 0.5
    base_logit = _logit(base_rate)
    numeric_cuts = {
        feature: cuts
        for feature in NUMERIC_FEATURES
        if (cuts := _quantile_cuts([_float(sample.row, feature) for sample in train], bins))
    }
    stats: dict[str, dict[str, WeightedStat]] = defaultdict(lambda: defaultdict(WeightedStat))
    for sample in train:
        for feature, cuts in numeric_cuts.items():
            bucket = _numeric_bin(_float(sample.row, feature), cuts)
            stat = stats[feature][bucket]
            stat.rows += 1
            stat.weight += sample.weight
            stat.target_weight += sample.weight * sample.target
        for feature in CATEGORICAL_FEATURES:
            bucket = _category(sample.row.get(feature, ""))
            stat = stats[feature][bucket]
            stat.rows += 1
            stat.weight += sample.weight
            stat.target_weight += sample.weight * sample.target

    contributions: dict[str, dict[str, float]] = {}
    effects: list[dict[str, Any]] = []
    for feature, by_bucket in sorted(stats.items()):
        contributions[feature] = {}
        for bucket, stat in sorted(by_bucket.items()):
            rate = (stat.target_weight + alpha * base_rate) / (stat.weight + alpha)
            contribution = _clip(_logit(rate) - base_logit, -clip_contribution, clip_contribution)
            contributions[feature][bucket] = contribution
            effects.append({
                "side": side,
                "feature": feature,
                "bin": bucket,
                "train_rows": stat.rows,
                "campaign_balanced_weight": f"{stat.weight:.10f}",
                "tail_target_rate": f"{stat.target_weight / max(stat.weight, 1e-12):.10f}",
                "smoothed_tail_rate": f"{rate:.10f}",
                "logit_contribution": f"{contribution:.10f}",
            })
    return TailModel(side, base_rate, base_logit, numeric_cuts, contributions, contribution_scale), effects


def _fold_map(days: list[str], folds: int) -> dict[str, int]:
    folds = max(1, min(folds, len(days)))
    return {day: min(folds - 1, idx * folds // len(days)) for idx, day in enumerate(days)}


def _quantile(values: list[float], probability: float) -> float:
    clean = sorted(value for value in values if math.isfinite(value))
    if not clean:
        return math.nan
    position = (len(clean) - 1) * _clip(probability, 0.0, 1.0)
    lo = int(math.floor(position))
    hi = int(math.ceil(position))
    if lo == hi:
        return clean[lo]
    return clean[lo] * (hi - position) + clean[hi] * (position - lo)


def _score_bucket(score: float, cuts: list[float]) -> str:
    labels = ("q1_low", "q2", "q3", "q4", "q5_high")
    idx = 0
    while idx < len(cuts) and score >= cuts[idx]:
        idx += 1
    return labels[min(idx, len(labels) - 1)]


@dataclass
class ReportAcc:
    orders: int = 0
    fills: int = 0
    add_fills: int = 0
    role_drift_fills: int = 0
    labeled: int = 0
    tails: int = 0
    score_sum: float = 0.0
    terminal_pnl_sum: float = 0.0
    early_drawdown_sum: float = 0.0
    duration_sum: float = 0.0
    max_inventory_sum: float = 0.0
    campaigns: set[str] = field(default_factory=set)

    def add(self, row: dict[str, Any]) -> None:
        self.orders += 1
        self.score_sum += _float(row, "addon_campaign_tail_score_oos", 0.0)
        filled = _int(row, "filled") > 0
        self.fills += int(filled)
        self.add_fills += int(filled and row.get("fill_inventory_role") == "add")
        self.role_drift_fills += int(filled and _int(row, "inventory_role_drift") > 0)
        target = row.get("addon_campaign_tail_target", "")
        if target not in ("", None):
            self.labeled += 1
            self.tails += _int(row, "addon_campaign_tail_target")
            self.terminal_pnl_sum += _float(row, "terminal_final_total_pnl_delta", 0.0)
            self.early_drawdown_sum += _float(row, "terminal_early_drawdown_20m", 0.0)
            self.duration_sum += _float(row, "terminal_campaign_duration_s", 0.0)
            self.max_inventory_sum += _float(row, "terminal_campaign_max_abs_inventory", 0.0)
            campaign = f"{row.get('day', '')}:{row.get('campaign_id', '')}"
            if campaign != ":":
                self.campaigns.add(campaign)

    def as_row(self) -> dict[str, Any]:
        return {
            "orders": self.orders,
            "fills": self.fills,
            "fill_rate": f"{self.fills / max(self.orders, 1):.10f}",
            "actual_add_fills": self.add_fills,
            "role_drift_fills": self.role_drift_fills,
            "labeled_add_fills": self.labeled,
            "unique_campaigns": len(self.campaigns),
            "tail_rate": f"{self.tails / max(self.labeled, 1):.10f}" if self.labeled else "",
            "avg_score": f"{self.score_sum / max(self.orders, 1):.10f}",
            "avg_terminal_pnl": f"{self.terminal_pnl_sum / max(self.labeled, 1):.10f}" if self.labeled else "",
            "avg_early_20m_drawdown": f"{self.early_drawdown_sum / max(self.labeled, 1):.10f}" if self.labeled else "",
            "avg_campaign_duration_s": f"{self.duration_sum / max(self.labeled, 1):.10f}" if self.labeled else "",
            "avg_campaign_max_inventory": f"{self.max_inventory_sum / max(self.labeled, 1):.10f}" if self.labeled else "",
        }


def _auc(records: list[dict[str, Any]]) -> float:
    labeled = [
        (_float(row, "addon_campaign_tail_score_oos"), _int(row, "addon_campaign_tail_target"))
        for row in records
        if row.get("addon_campaign_tail_target", "") not in ("", None)
    ]
    positives = sum(target for _, target in labeled)
    negatives = len(labeled) - positives
    if positives == 0 or negatives == 0:
        return math.nan
    labeled.sort(key=lambda item: item[0])
    rank_sum = 0.0
    idx = 0
    while idx < len(labeled):
        end = idx + 1
        while end < len(labeled) and labeled[end][0] == labeled[idx][0]:
            end += 1
        average_rank = 0.5 * ((idx + 1) + end)
        rank_sum += average_rank * sum(target for _, target in labeled[idx:end])
        idx = end
    return (rank_sum - positives * (positives + 1) / 2.0) / (positives * negatives)


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def run(args: argparse.Namespace) -> dict[str, Any]:
    paths = _iter_paths(args.order_level_filelist, args.order_level_csv)
    if not paths:
        raise RuntimeError("no order-level CSV paths supplied")
    missing = [str(path) for path in paths if not path.exists()]
    if missing:
        raise FileNotFoundError(f"missing order-level CSVs: {missing[:5]}")

    samples: list[Sample] = []
    days: set[str] = set()
    denominator = defaultdict(int)
    for path in paths:
        rows = _read_rows(path, args.initial_inventory)
        for row in rows:
            day = _day(row)
            if day:
                days.add(day)
            if row.get("inventory_role") == "add" and _side(row):
                side_key = _side(row).lower()
                denominator[f"{side_key}_add_orders"] += 1
                inventory_tail_state = _float(row, "campaign_max_abs_qty", 0.0) >= 0.010 - 1e-12
                mae_tail_state = _float(row, "campaign_adverse_excursion", 0.0) <= -1.0 + 1e-12
                denominator[f"{side_key}_preexisting_inventory_tail_orders"] += int(inventory_tail_state)
                denominator[f"{side_key}_preexisting_mae_tail_orders"] += int(mae_tail_state)
                denominator[f"{side_key}_preexisting_both_tail_orders"] += int(
                    inventory_tail_state and mae_tail_state
                )
                if _preexisting_tail_state(row):
                    denominator[f"{side_key}_preexisting_tail_state_orders"] += 1
                else:
                    denominator[f"{side_key}_model_eligible_add_orders"] += 1
                if _int(row, "filled") > 0:
                    denominator[f"{side_key}_fills"] += 1
                if _is_labeled_add_fill(
                    row,
                    target_mode=args.target_mode,
                    include_preexisting_tail_state=args.include_preexisting_tail_state,
                ):
                    samples.append(Sample(
                        day,
                        _side(row),
                        str(row.get("campaign_id", "")),
                        _tail_target(row, args.target_mode) or 0,
                        row,
                    ))

    campaign_counts: dict[tuple[str, str, str], int] = defaultdict(int)
    for sample in samples:
        campaign_counts[(sample.day, sample.side, sample.campaign_id)] += 1
    for sample in samples:
        sample.weight = 1.0 / max(campaign_counts[(sample.day, sample.side, sample.campaign_id)], 1)

    sorted_days = sorted(days)
    if len(sorted_days) < 2:
        raise RuntimeError("campaign-tail OOS requires at least two UTC days")
    fold_by_day = _fold_map(sorted_days, args.folds)
    folds = max(fold_by_day.values()) + 1
    models: dict[tuple[str, int], TailModel] = {}
    model_json: dict[str, Any] = {
        "target": (
            "closed terminal campaign label == loss_tail"
            if args.target_mode == "loss_tail"
            else "terminal campaign label in {loss_tail, open_risk}"
        ),
        "eligibility": "quote-time inventory_role=add; filled supervision requires fill_inventory_role=add",
        "exclude_preexisting_tail_state": not args.include_preexisting_tail_state,
        "fold_mode": "contiguous_utc_day_blocks",
        "folds": [],
    }
    effects: list[dict[str, Any]] = []
    for fold in range(folds):
        test_days = {day for day, value in fold_by_day.items() if value == fold}
        train_days = set(sorted_days) - test_days
        fold_entry: dict[str, Any] = {"fold": fold, "train_days": sorted(train_days), "test_days": sorted(test_days), "models": {}}
        for side in ("BUY", "SELL"):
            model, side_effects = _fit_model(
                samples,
                side=side,
                train_days=train_days,
                bins=args.bins,
                alpha=args.alpha,
                contribution_scale=args.contribution_scale,
                clip_contribution=args.clip_contribution,
            )
            models[(side, fold)] = model
            fold_entry["models"][side] = model.as_json()
            effects.extend({"fold": fold, **row} for row in side_effects)
        model_json["folds"].append(fold_entry)

    full_models: dict[str, Any] = {}
    for side in ("BUY", "SELL"):
        model, _ = _fit_model(
            samples,
            side=side,
            train_days=set(sorted_days),
            bins=args.bins,
            alpha=args.alpha,
            contribution_scale=args.contribution_scale,
            clip_contribution=args.clip_contribution,
        )
        full_models[side] = model.as_json()
    model_json["full_shadow_models"] = full_models

    compact_rows: list[dict[str, Any]] = []
    for path in paths:
        for row in _read_rows(path, args.initial_inventory):
            side = _side(row)
            day = _day(row)
            if row.get("inventory_role") != "add" or side not in {"BUY", "SELL"} or day not in fold_by_day:
                continue
            fold = fold_by_day[day]
            preexisting_tail_state = _preexisting_tail_state(row)
            model_eligible = args.include_preexisting_tail_state or not preexisting_tail_state
            score = models[(side, fold)].score(row) if model_eligible else math.nan
            target = (
                _tail_target(row, args.target_mode)
                if _is_labeled_add_fill(
                    row,
                    target_mode=args.target_mode,
                    include_preexisting_tail_state=args.include_preexisting_tail_state,
                )
                else None
            )
            compact_rows.append({
                "day": day,
                "client_order_id": row.get("client_order_id", ""),
                "side": side,
                "fold": fold,
                "inventory_role": "add",
                "fill_inventory_role": row.get("fill_inventory_role", ""),
                "fill_role_source": row.get("fill_role_source", ""),
                "inventory_role_drift": row.get("inventory_role_drift", "0"),
                "preexisting_tail_state": int(preexisting_tail_state),
                "addon_campaign_tail_score_eligible": int(model_eligible),
                "filled": row.get("filled", "0"),
                "fill_age_ms": row.get("fill_age_ms", ""),
                "campaign_id": row.get("campaign_id", ""),
                "addon_campaign_tail_score_oos": f"{score:.10f}" if math.isfinite(score) else "",
                "addon_campaign_tail_target": "" if target is None else str(target),
                "terminal_campaign_label": row.get("terminal_campaign_label", ""),
                "terminal_final_total_pnl_delta": row.get("terminal_final_total_pnl_delta", ""),
                "terminal_early_drawdown_20m": row.get("terminal_early_drawdown_20m", ""),
                "terminal_campaign_duration_s": row.get("terminal_campaign_duration_s", ""),
                "terminal_campaign_max_abs_inventory": row.get("terminal_campaign_max_abs_inventory", ""),
                "markout_20s_bps": row.get("markout_20s_bps", ""),
                "markout_30s_bps": row.get("markout_30s_bps", ""),
            })

    side_cuts = {
        side: [
            _quantile([
                _float(row, "addon_campaign_tail_score_oos")
                for row in compact_rows
                if row["side"] == side and _int(row, "addon_campaign_tail_score_eligible") > 0
            ], q)
            for q in (0.2, 0.4, 0.6, 0.8)
        ]
        for side in ("BUY", "SELL")
    }
    aggregate: dict[tuple[str, str], ReportAcc] = defaultdict(ReportAcc)
    daily: dict[tuple[str, str, str], ReportAcc] = defaultdict(ReportAcc)
    fold_acc: dict[tuple[int, str], ReportAcc] = defaultdict(ReportAcc)
    for row in compact_rows:
        if _int(row, "addon_campaign_tail_score_eligible") <= 0:
            continue
        side = str(row["side"])
        bucket = _score_bucket(_float(row, "addon_campaign_tail_score_oos"), side_cuts[side])
        row["score_rank_bucket"] = bucket
        aggregate[(side, bucket)].add(row)
        daily[(str(row["day"]), side, bucket)].add(row)
        fold_acc[(int(row["fold"]), side)].add(row)

    calibration_rows = [
        {"side": side, "score_rank_bucket": bucket, **acc.as_row()}
        for (side, bucket), acc in sorted(aggregate.items())
    ]
    daily_rows = [
        {"day": day, "side": side, "score_rank_bucket": bucket, **acc.as_row()}
        for (day, side, bucket), acc in sorted(daily.items())
    ]
    fold_rows = [
        {"fold": fold, "side": side, **acc.as_row()}
        for (fold, side), acc in sorted(fold_acc.items())
    ]

    side_summary: dict[str, Any] = {}
    for side in ("BUY", "SELL"):
        all_side_records = [row for row in compact_rows if row["side"] == side]
        side_records = [row for row in all_side_records if _int(row, "addon_campaign_tail_score_eligible") > 0]
        labeled = [row for row in side_records if row["addon_campaign_tail_target"] != ""]
        brier = (
            sum((_float(row, "addon_campaign_tail_score_oos") - _int(row, "addon_campaign_tail_target")) ** 2 for row in labeled)
            / len(labeled)
            if labeled else math.nan
        )
        low = aggregate.get((side, "q1_low"), ReportAcc()).as_row()
        high = aggregate.get((side, "q5_high"), ReportAcc()).as_row()
        daily_deltas: list[float] = []
        for day in sorted_days:
            low_day = daily.get((day, side, "q1_low"))
            high_day = daily.get((day, side, "q5_high"))
            if low_day is None or high_day is None or low_day.labeled < 3 or high_day.labeled < 3:
                continue
            daily_deltas.append(
                high_day.tails / high_day.labeled - low_day.tails / low_day.labeled
            )
        side_summary[side] = {
            "add_orders": len(all_side_records),
            "model_eligible_add_orders": len(side_records),
            "preexisting_tail_state_orders": len(all_side_records) - len(side_records),
            "labeled_add_fills": len(labeled),
            "unique_labeled_campaigns": len({(row["day"], row["campaign_id"]) for row in labeled}),
            "oos_auc_tail_risk": _auc(side_records),
            "oos_brier": brier,
            "daily_high_vs_low": {
                "comparable_days_min_3_labels_each": len(daily_deltas),
                "high_risk_worse_days": sum(delta > 0.0 for delta in daily_deltas),
                "ties": sum(delta == 0.0 for delta in daily_deltas),
                "inverted_days": sum(delta < 0.0 for delta in daily_deltas),
                "median_tail_rate_delta": _quantile(daily_deltas, 0.5),
            },
            "low_risk_bucket": low,
            "high_risk_bucket": high,
        }

    prefix = Path(args.out_prefix)
    score_path = prefix.with_suffix(".addon_campaign_tail_scores.csv")
    calibration_path = prefix.with_suffix(".addon_campaign_tail_calibration.csv")
    daily_path = prefix.with_suffix(".addon_campaign_tail_daily.csv")
    fold_path = prefix.with_suffix(".addon_campaign_tail_folds.csv")
    effects_path = prefix.with_suffix(".addon_campaign_tail_feature_effects.csv")
    model_path = prefix.with_suffix(".addon_campaign_tail_model.json")
    summary_path = prefix.with_suffix(".addon_campaign_tail_summary.json")
    _write_csv(score_path, compact_rows)
    _write_csv(calibration_path, calibration_rows)
    _write_csv(daily_path, daily_rows)
    _write_csv(fold_path, fold_rows)
    _write_csv(effects_path, effects)
    model_path.parent.mkdir(parents=True, exist_ok=True)
    model_path.write_text(json.dumps(model_json, indent=2, sort_keys=True), encoding="utf-8")
    summary = {
        "days": sorted_days,
        "paths": [str(path) for path in paths],
        "folds": folds,
        "denominator": dict(sorted(denominator.items())),
        "target": model_json["target"],
        "target_mode": args.target_mode,
        "eligibility": model_json["eligibility"],
        "exclude_preexisting_tail_state": not args.include_preexisting_tail_state,
        "campaign_balanced_training": True,
        "side_summary": side_summary,
        "outputs": {
            "score_extension": str(score_path),
            "calibration": str(calibration_path),
            "daily": str(daily_path),
            "folds": str(fold_path),
            "feature_effects": str(effects_path),
            "model": str(model_path),
        },
    }
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--order-level-csv", action="append", default=[])
    parser.add_argument("--order-level-filelist", type=Path)
    parser.add_argument("--out-prefix", required=True)
    parser.add_argument("--folds", type=int, default=5)
    parser.add_argument("--bins", type=int, default=5)
    parser.add_argument("--alpha", type=float, default=10.0)
    parser.add_argument("--contribution-scale", type=float, default=0.35)
    parser.add_argument("--clip-contribution", type=float, default=1.25)
    parser.add_argument("--initial-inventory", type=float, default=0.0)
    parser.add_argument("--target-mode", choices=TARGET_MODES, default="loss_tail")
    parser.add_argument(
        "--include-preexisting-tail-state",
        action="store_true",
        help="Allow training rows that had already crossed the campaign tail inventory/MAE definition.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    summary = run(parse_args(argv))
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
