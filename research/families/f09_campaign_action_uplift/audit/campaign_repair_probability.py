#!/usr/bin/env python3
"""Train and audit causal campaign-repair probability sequences.

The scorer is trained on terminal campaign outcomes, but every input feature is
an as-of quote-time snapshot.  Formal policy replay loads the fitted model and
recomputes the score on each arm's own inventory path; the CSV sequence emitted
here is a baseline-path calibration artifact, not a counterfactual shortcut.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from research.families.f01_fixed_parameter_racing.parameter_racing_sweep import DEFAULT_RETAINED39_DAYS
from strategy.campaign_repair import (
    CAMPAIGN_REPAIR_MODEL_SCHEMA_VERSION,
    REPAIR_FEATURE_NAMES,
    CampaignRepairModel,
    CampaignRepairSideModel,
    build_campaign_repair_features,
    inventory_campaign_side,
)


GOOD_REPAIR_LABELS = {"positive_flat", "repaired_after_drawdown"}
BAD_REPAIR_LABELS = {"negative_flat", "loss_tail"}
LATE4_DAYS = ("2026-07-03", "2026-07-04", "2026-07-05", "2026-07-06")


def _float(row: dict[str, Any], key: str, default: float = math.nan) -> float:
    try:
        value = row.get(key, "")
        return float(value) if value not in ("", None) else default
    except (TypeError, ValueError):
        return default


def _int(row: dict[str, Any], key: str, default: int = 0) -> int:
    value = _float(row, key, math.nan)
    return int(value) if math.isfinite(value) else default


def _clip(value: float, lo: float, hi: float) -> float:
    return min(hi, max(lo, value))


def _logit(probability: float) -> float:
    p = _clip(probability, 1e-6, 1.0 - 1e-6)
    return math.log(p / (1.0 - p))


def _numeric_bin(value: float, cuts: tuple[float, ...]) -> str:
    if not math.isfinite(value):
        return "missing"
    index = 0
    while index < len(cuts) and value >= cuts[index]:
        index += 1
    return f"b{index:02d}"


def _campaign_key(day: str, campaign_id: str) -> str:
    return f"{day}:{campaign_id}"


@dataclass
class RepairSample:
    day: str
    ts_ns: int
    campaign_id: str
    campaign_side: str
    target: int
    features: dict[str, float]
    weight: float = 1.0


@dataclass
class WeightedStat:
    rows: int = 0
    weight: float = 0.0
    target_weight: float = 0.0


def _iter_paths(filelist: Path, extra_paths: Iterable[str]) -> list[Path]:
    paths = [Path(value).expanduser() for value in extra_paths]
    text = filelist.read_text(encoding="utf-8").strip()
    if text:
        lines = text.splitlines()
        if "," in lines[0]:
            with filelist.open(newline="", encoding="utf-8") as handle:
                for row in csv.DictReader(handle):
                    value = (
                        row.get("order_level_csv")
                        or row.get("path")
                        or row.get("file")
                        or ""
                    )
                    if value:
                        paths.append(Path(value).expanduser())
        else:
            paths.extend(Path(line.strip()).expanduser() for line in lines if line.strip())
    unique: list[Path] = []
    seen: set[Path] = set()
    for path in paths:
        if path not in seen:
            seen.add(path)
            unique.append(path)
    missing = [str(path) for path in unique if not path.exists()]
    if missing:
        raise FileNotFoundError(f"missing order-level files: {missing[:5]}")
    return unique


def _features_from_row(
    row: dict[str, Any],
    *,
    order_size: float,
    max_inventory: float,
) -> tuple[str, dict[str, float]] | None:
    inventory = _float(row, "q_before")
    campaign_side = inventory_campaign_side(inventory)
    side = str(row.get("side", "")).upper()
    add_side = "BUY" if campaign_side == "LONG" else "SELL" if campaign_side == "SHORT" else ""
    if side != add_side:
        return None
    return campaign_side, build_campaign_repair_features(
        inventory=inventory,
        order_size=order_size,
        max_inventory=max_inventory,
        campaign_age_s=_float(row, "campaign_age_s", 0.0),
        campaign_max_abs_qty_so_far=_float(row, "campaign_max_abs_qty", abs(inventory)),
        campaign_pnl_so_far=_float(row, "campaign_total_pnl", 0.0),
        campaign_adverse_excursion_so_far=_float(
            row, "campaign_adverse_excursion", 0.0
        ),
        campaign_exposure_increasing_fills_so_far=_int(
            row, "campaign_exposure_increasing_fills"
        ),
        campaign_reducing_fills_so_far=_int(row, "campaign_reducing_fills"),
        l2_book_refresh_ratio=_float(row, "l2_book_refresh_ratio", 0.0),
        l2_book_cancel_ratio=_float(row, "l2_book_cancel_ratio", 0.0),
        l2_quote_flip_rate=_float(row, "l2_quote_flip_rate", 0.0),
        near_depth_total=_float(row, "near_depth_total", 0.0),
        microprice_shift_bps=_float(row, "microprice_shift_bps", 0.0),
        toxicity=_float(row, "toxicity", 0.5),
        markout_ema=_float(row, "markout_ema", 0.0),
        side_quote_fill_probability=_float(row, "side_quote_fill_prob", 0.0),
        side_quote_markout_30s=_float(row, "side_quote_fill_markout_30s", 0.0),
    )


def load_samples(
    paths: Iterable[Path],
    *,
    sample_interval_s: int,
    order_size: float,
    max_inventory: float,
) -> tuple[list[RepairSample], dict[str, Any]]:
    interval_ns = max(1, int(sample_interval_s)) * 1_000_000_000
    sampled: dict[tuple[str, str, int], RepairSample] = {}
    counters: dict[str, int] = defaultdict(int)
    for path in paths:
        with path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                counters["order_rows"] += 1
                day = str(row.get("day", ""))[:10]
                campaign_id = str(row.get("campaign_id", "")).strip()
                label = str(row.get("terminal_campaign_label", "")).strip()
                if not day or not campaign_id or _int(row, "campaign_active") <= 0:
                    counters["ineligible_no_campaign"] += 1
                    continue
                if label == "open_risk":
                    counters["censored_open_risk"] += 1
                    continue
                if label not in GOOD_REPAIR_LABELS | BAD_REPAIR_LABELS:
                    counters["missing_or_unknown_target"] += 1
                    continue
                feature_result = _features_from_row(
                    row,
                    order_size=order_size,
                    max_inventory=max_inventory,
                )
                if feature_result is None:
                    counters["non_inventory_direction_row"] += 1
                    continue
                campaign_side, features = feature_result
                ts_s = _float(row, "timestamp", 0.0)
                if ts_s <= 0.0:
                    counters["invalid_timestamp"] += 1
                    continue
                ts_ns = int(round(ts_s * 1_000_000_000.0))
                bucket = ts_ns // interval_ns
                key = (day, campaign_id, bucket)
                # Keep the latest state inside the causal bucket.  The choice is
                # based only on timestamp, never on fill or terminal outcome.
                sample = RepairSample(
                    day=day,
                    ts_ns=ts_ns,
                    campaign_id=campaign_id,
                    campaign_side=campaign_side,
                    target=int(label in GOOD_REPAIR_LABELS),
                    features=features,
                )
                previous = sampled.get(key)
                if previous is None or sample.ts_ns > previous.ts_ns:
                    sampled[key] = sample

    samples = sorted(sampled.values(), key=lambda row: (row.day, row.ts_ns, row.campaign_id))
    by_campaign: dict[str, int] = defaultdict(int)
    for sample in samples:
        by_campaign[_campaign_key(sample.day, sample.campaign_id)] += 1
    for sample in samples:
        sample.weight = 1.0 / max(
            by_campaign[_campaign_key(sample.day, sample.campaign_id)], 1
        )
    counters["sampled_rows"] = len(samples)
    counters["sampled_campaigns"] = len(by_campaign)
    counters["positive_campaigns"] = len(
        {
            _campaign_key(sample.day, sample.campaign_id)
            for sample in samples
            if sample.target == 1
        }
    )
    counters["negative_campaigns"] = len(by_campaign) - counters["positive_campaigns"]
    return samples, dict(counters)


def _weighted_quantile_cuts(
    samples: list[RepairSample], feature: str, bins: int
) -> tuple[float, ...]:
    values = sorted(
        (
            float(sample.features.get(feature, math.nan)),
            float(sample.weight),
        )
        for sample in samples
        if math.isfinite(float(sample.features.get(feature, math.nan)))
        and sample.weight > 0.0
    )
    if not values:
        return ()
    total = sum(weight for _, weight in values)
    cuts: list[float] = []
    index = 0
    cumulative = 0.0
    for quantile_index in range(1, max(2, int(bins))):
        target = total * quantile_index / max(2, int(bins))
        while index < len(values) and cumulative < target:
            cumulative += values[index][1]
            index += 1
        value = values[min(max(index - 1, 0), len(values) - 1)][0]
        if not cuts or value > cuts[-1]:
            cuts.append(value)
    return tuple(cuts)


def fit_model(
    samples: list[RepairSample],
    *,
    train_days: set[str],
    bins: int,
    alpha: float,
    contribution_scale: float,
    clip_contribution: float,
    model_id: str,
) -> CampaignRepairModel:
    side_models: dict[str, CampaignRepairSideModel] = {}
    for side in ("LONG", "SHORT"):
        train = [
            sample
            for sample in samples
            if sample.day in train_days and sample.campaign_side == side
        ]
        total_weight = sum(sample.weight for sample in train)
        target_weight = sum(sample.weight * sample.target for sample in train)
        base_rate = (
            (target_weight + alpha * 0.5) / (total_weight + alpha)
            if total_weight > 0.0
            else 0.5
        )
        base_logit = _logit(base_rate)
        cuts = {
            feature: feature_cuts
            for feature in REPAIR_FEATURE_NAMES
            if (feature_cuts := _weighted_quantile_cuts(train, feature, bins))
        }
        stats: dict[str, dict[str, WeightedStat]] = defaultdict(
            lambda: defaultdict(WeightedStat)
        )
        for sample in train:
            for feature, feature_cuts in cuts.items():
                bucket = _numeric_bin(
                    float(sample.features.get(feature, math.nan)), feature_cuts
                )
                stat = stats[feature][bucket]
                stat.rows += 1
                stat.weight += sample.weight
                stat.target_weight += sample.weight * sample.target
        contributions: dict[str, dict[str, float]] = {}
        for feature, by_bucket in stats.items():
            contributions[feature] = {}
            for bucket, stat in by_bucket.items():
                rate = (stat.target_weight + alpha * base_rate) / (stat.weight + alpha)
                contributions[feature][bucket] = _clip(
                    _logit(rate) - base_logit,
                    -clip_contribution,
                    clip_contribution,
                )
        side_models[side] = CampaignRepairSideModel(
            side=side,
            base_rate=base_rate,
            base_logit=base_logit,
            numeric_cuts=cuts,
            contributions=contributions,
            contribution_scale=contribution_scale,
        )
    training_end = max(train_days) if train_days else ""
    return CampaignRepairModel(
        long_model=side_models["LONG"],
        short_model=side_models["SHORT"],
        model_id=model_id,
        training_end_day=training_end,
        metadata={
            "target": "terminal label in {positive_flat, repaired_after_drawdown}",
            "censored": "open_risk excluded",
            "training_days": sorted(train_days),
            "training_day_count": len(train_days),
            "campaign_balanced_weights": True,
            "feature_availability": "quote_decision_time_only",
        },
    )


def _model_id(panel: str, train_days: Iterable[str], sequence: int) -> str:
    days = sorted(train_days)
    digest = hashlib.sha256((panel + "|" + "|".join(days)).encode()).hexdigest()[:12]
    return f"repair_{panel}_{sequence:02d}_{digest}"


def build_panel_models(
    samples: list[RepairSample],
    *,
    panel: str,
    test_days: list[str],
    fixed_train_days: list[str] | None,
    all_days: list[str],
    min_train_days: int,
    test_block_days: int,
    embargo_days: int,
    bins: int,
    alpha: float,
    contribution_scale: float,
    clip_contribution: float,
) -> dict[str, Any]:
    models: dict[str, Any] = {}
    day_to_model_id: dict[str, str] = {}
    folds: list[dict[str, Any]] = []
    if fixed_train_days is not None:
        model_id = _model_id(panel, fixed_train_days, 0)
        model = fit_model(
            samples,
            train_days=set(fixed_train_days),
            bins=bins,
            alpha=alpha,
            contribution_scale=contribution_scale,
            clip_contribution=clip_contribution,
            model_id=model_id,
        )
        models[model_id] = model.to_dict()
        day_to_model_id.update({day: model_id for day in test_days})
        folds.append(
            {
                "fold": 0,
                "train_days": fixed_train_days,
                "test_days": test_days,
                "model_id": model_id,
            }
        )
    else:
        day_index = {day: index for index, day in enumerate(all_days)}
        eligible = [day for day in test_days if day in day_index]
        block_size = max(1, int(test_block_days))
        sequence = 0
        for start in range(0, len(eligible), block_size):
            block = eligible[start : start + block_size]
            first_index = day_index[block[0]]
            train_end_index = first_index - max(0, int(embargo_days))
            train_days = all_days[:train_end_index]
            if len(train_days) < max(1, int(min_train_days)):
                continue
            model_id = _model_id(panel, train_days, sequence)
            sequence += 1
            model = fit_model(
                samples,
                train_days=set(train_days),
                bins=bins,
                alpha=alpha,
                contribution_scale=contribution_scale,
                clip_contribution=clip_contribution,
                model_id=model_id,
            )
            models[model_id] = model.to_dict()
            day_to_model_id.update({day: model_id for day in block})
            folds.append(
                {
                    "fold": len(folds),
                    "train_days": train_days,
                    "test_days": block,
                    "model_id": model_id,
                }
            )
    return {
        "panel": panel,
        "models": models,
        "day_to_model_id": day_to_model_id,
        "folds": folds,
        "test_days_requested": test_days,
        "test_days_scored": sorted(day_to_model_id),
    }


def _auc(rows: list[dict[str, Any]]) -> float:
    pairs = sorted((float(row["repair_probability"]), int(row["target"])) for row in rows)
    positives = sum(target for _, target in pairs)
    negatives = len(pairs) - positives
    if positives == 0 or negatives == 0:
        return math.nan
    rank_sum = 0.0
    index = 0
    while index < len(pairs):
        end = index + 1
        while end < len(pairs) and pairs[end][0] == pairs[index][0]:
            end += 1
        average_rank = 0.5 * ((index + 1) + end)
        rank_sum += average_rank * sum(target for _, target in pairs[index:end])
        index = end
    return (rank_sum - positives * (positives + 1) / 2.0) / (
        positives * negatives
    )


def score_panel(samples: list[RepairSample], panel_payload: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    model_cache = {
        model_id: CampaignRepairModel.from_dict(payload)
        for model_id, payload in panel_payload["models"].items()
    }
    day_map = panel_payload["day_to_model_id"]
    rows: list[dict[str, Any]] = []
    for sample in samples:
        model_id = day_map.get(sample.day)
        if not model_id:
            continue
        model = model_cache[model_id]
        probability = model.score(
            1.0 if sample.campaign_side == "LONG" else -1.0,
            sample.features,
        )
        rows.append(
            {
                "panel": panel_payload["panel"],
                "day": sample.day,
                "ts_ns": sample.ts_ns,
                "timestamp": sample.ts_ns / 1_000_000_000.0,
                "campaign_id": sample.campaign_id,
                "campaign_side": sample.campaign_side,
                "target": sample.target,
                "repair_probability": probability,
                "sample_weight": sample.weight,
                "model_id": model_id,
                "training_end_day": model.training_end_day,
            }
        )
    total_weight = sum(float(row["sample_weight"]) for row in rows)
    brier = (
        sum(
            float(row["sample_weight"])
            * (float(row["repair_probability"]) - int(row["target"])) ** 2
            for row in rows
        )
        / total_weight
        if total_weight > 0.0
        else math.nan
    )
    probabilities = sorted(float(row["repair_probability"]) for row in rows)
    q20 = probabilities[int(0.20 * (len(probabilities) - 1))] if probabilities else math.nan
    q80 = probabilities[int(0.80 * (len(probabilities) - 1))] if probabilities else math.nan
    low = [row for row in rows if float(row["repair_probability"]) <= q20]
    high = [row for row in rows if float(row["repair_probability"]) >= q80]

    def _weighted_rate(group: list[dict[str, Any]]) -> float:
        weight = sum(float(row["sample_weight"]) for row in group)
        return (
            sum(float(row["sample_weight"]) * int(row["target"]) for row in group)
            / weight
            if weight > 0.0
            else math.nan
        )

    summary = {
        "rows": len(rows),
        "days": len({row["day"] for row in rows}),
        "campaigns": len(
            {(row["day"], row["campaign_id"]) for row in rows}
        ),
        "auc": _auc(rows),
        "campaign_balanced_brier": brier,
        "low_q20_repair_rate": _weighted_rate(low),
        "high_q20_repair_rate": _weighted_rate(high),
        "high_minus_low_repair_rate": _weighted_rate(high) - _weighted_rate(low),
    }
    return rows, summary


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def run(args: argparse.Namespace) -> dict[str, Any]:
    paths = _iter_paths(args.order_level_filelist, args.order_level_csv)
    samples, denominator = load_samples(
        paths,
        sample_interval_s=args.sample_interval_s,
        order_size=args.order_size,
        max_inventory=args.max_inventory,
    )
    all_days = sorted({sample.day for sample in samples})
    retained39 = [day for day in DEFAULT_RETAINED39_DAYS if day in all_days]
    late_train = [day for day in all_days if day < LATE4_DAYS[0]]
    blocked71 = [
        day
        for day in all_days
        if day not in set(DEFAULT_RETAINED39_DAYS) and day not in set(LATE4_DAYS)
    ]
    chronological_test_days = all_days[
        min(len(all_days), args.min_train_days + args.embargo_days) :
    ]
    common = {
        "samples": samples,
        "all_days": all_days,
        "min_train_days": args.min_train_days,
        "test_block_days": args.test_block_days,
        "embargo_days": args.embargo_days,
        "bins": args.bins,
        "alpha": args.alpha,
        "contribution_scale": args.contribution_scale,
        "clip_contribution": args.clip_contribution,
    }
    panels = {
        "chronological": build_panel_models(
            panel="chronological",
            test_days=chronological_test_days,
            fixed_train_days=None,
            **common,
        ),
        "blocked71": build_panel_models(
            panel="blocked71",
            test_days=blocked71,
            fixed_train_days=retained39,
            **common,
        ),
        "late4": build_panel_models(
            panel="late4",
            test_days=list(LATE4_DAYS),
            fixed_train_days=late_train,
            **common,
        ),
    }
    prefix = Path(args.out_prefix).expanduser()
    sequence_rows: list[dict[str, Any]] = []
    panel_summaries: dict[str, Any] = {}
    for panel, payload in panels.items():
        rows, summary = score_panel(samples, payload)
        sequence_rows.extend(rows)
        panel_summaries[panel] = summary
    bundle = {
        "schema_version": CAMPAIGN_REPAIR_MODEL_SCHEMA_VERSION,
        "bundle_schema_version": "campaign_repair_model_bundle.v1",
        "target": "good terminal repair: positive_flat or repaired_after_drawdown",
        "censoring": "open_risk excluded from training and calibration",
        "feature_names": list(REPAIR_FEATURE_NAMES),
        "feature_provenance": {
            feature: "quote_decision_time_asof" for feature in REPAIR_FEATURE_NAMES
        },
        "sample_interval_s": args.sample_interval_s,
        "campaign_balanced_weights": True,
        "order_size": args.order_size,
        "max_inventory": args.max_inventory,
        "all_order_level_days": all_days,
        "retained39_train_days_for_blocked": retained39,
        "blocked71_test_days": blocked71,
        "late4_train_days": late_train,
        "late4_test_days": list(LATE4_DAYS),
        "panels": panels,
    }
    model_path = prefix.with_suffix(".model_bundle.json")
    sequence_path = prefix.with_suffix(".causal_sequence.csv")
    summary_path = prefix.with_suffix(".summary.json")
    model_path.parent.mkdir(parents=True, exist_ok=True)
    model_path.write_text(json.dumps(bundle, indent=2, sort_keys=True), encoding="utf-8")
    _write_csv(sequence_path, sequence_rows)
    summary = {
        "status": "ok",
        "denominator": denominator,
        "days": all_days,
        "panel_summaries": panel_summaries,
        "outputs": {
            "model_bundle": str(model_path),
            "causal_sequence": str(sequence_path),
        },
        "interpretation": (
            "sequence scores use only quote-time as-of features; policy replay must "
            "recompute the same model on each arm's own campaign path"
        ),
    }
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--order-level-filelist", type=Path, required=True)
    parser.add_argument("--order-level-csv", action="append", default=[])
    parser.add_argument("--out-prefix", required=True)
    parser.add_argument("--sample-interval-s", type=int, default=30)
    parser.add_argument("--order-size", type=float, default=0.001)
    parser.add_argument("--max-inventory", type=float, default=0.026)
    parser.add_argument("--min-train-days", type=int, default=40)
    parser.add_argument("--test-block-days", type=int, default=10)
    parser.add_argument("--embargo-days", type=int, default=1)
    parser.add_argument("--bins", type=int, default=5)
    parser.add_argument("--alpha", type=float, default=10.0)
    parser.add_argument("--contribution-scale", type=float, default=0.35)
    parser.add_argument("--clip-contribution", type=float, default=1.25)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    summary = run(parse_args(argv))
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
