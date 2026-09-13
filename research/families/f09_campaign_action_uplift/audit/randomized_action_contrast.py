#!/usr/bin/env python3
"""Randomized ITT evidence for one-campaign keep/cancel interventions.

The behavior action is randomized before the replay outcome is observed, so
the primary diagnostic is a logged intention-to-treat contrast.  No outcome
model is needed for identification.  The estimator uses inverse-propensity
weighted Hajek arm means and resamples complete UTC days.

Rows with incomplete native exchange-book support are never filtered.  Their
mass is reported as a support failure because path completeness is affected by
the action itself.  This keeps the ITT diagnostic honest without presenting a
simulator fallback as strict native-order evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from models.audit.experiment_scorecard import (
    CANONICAL_EVIDENCE_SCHEMA_VERSION,
    render_scorecard_markdown,
    score_canonical_evidence,
)
from research.families.f09_campaign_action_uplift.audit.toxic_fill_selectivity import (
    randomized_panel_selectivity,
    selectivity_metric_from_summary,
)

SCHEMA_VERSION = "randomized_action_itt.v1"
BASELINE_ACTION = "keep"
CANDIDATE_ACTION = "cancel_until_state_exit"
REPAIR_HORIZON_S = 1_800.0

OUTCOMES = (
    "reward",
    "terminal_campaign_value",
    "campaign_cost_avoidance",
    "negative_terminal_protection",
    "q10_shortfall_protection",
    "campaign_mae_avoidance",
    "repair_event",
    "repair_time_avoidance_s",
    "censoring_avoidance",
    "queue_reset_value",
    "latency_adjusted_value",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    values = pd.to_numeric(frame[column], errors="coerce")
    if values.isna().any() or not np.isfinite(values.to_numpy(dtype=float)).all():
        raise ValueError(f"randomized action panel has non-finite {column}")
    return values.astype(float)


def validate_randomized_panel(
    frame: pd.DataFrame,
    *,
    baseline_action: str = BASELINE_ACTION,
    candidate_action: str = CANDIDATE_ACTION,
) -> None:
    required = {
        "day",
        "decision_id",
        "campaign_id",
        "side",
        "action",
        "behavior_propensity",
        "reward",
        "fill_value",
        "campaign_cost",
        "queue_cost",
        "terminal_campaign_pnl",
        "campaign_mae",
        "decision_to_terminal_s",
        "repair_event",
        "campaign_censored",
        "intervention_fill_count",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"randomized action panel is missing columns: {missing}")
    if frame.empty:
        raise ValueError("randomized action panel is empty")
    if frame.duplicated(["day", "decision_id"]).any():
        raise ValueError("randomized action panel has duplicate decision ids")
    if frame.duplicated(["day", "campaign_id"]).any():
        raise ValueError("randomized action panel violates one intervention per campaign")
    actions = set(frame["action"].astype(str))
    expected = {str(baseline_action), str(candidate_action)}
    if actions != expected:
        raise ValueError(
            f"randomized action support differs: expected={expected}, actual={actions}"
        )
    sides = set(frame["side"].astype(str).str.upper())
    if not sides.issubset({"BUY", "SELL"}) or not sides:
        raise ValueError(f"unexpected randomized sides: {sorted(sides)}")
    propensity = _numeric(frame, "behavior_propensity")
    if (propensity <= 0.0).any() or (propensity > 1.0).any():
        raise ValueError("behavior propensity must be in (0, 1]")
    if "one_intervention_per_campaign" in frame.columns:
        invariant = _numeric(frame, "one_intervention_per_campaign")
        if not invariant.eq(1.0).all():
            raise ValueError("one-intervention-per-campaign invariant was violated")
    identity_error = pd.to_numeric(
        frame.get("reward_identity_error", pd.Series(0.0, index=frame.index)),
        errors="coerce",
    )
    if identity_error.isna().any() or float(identity_error.abs().max()) > 1e-9:
        raise ValueError("reward accounting identity is invalid")


def derive_outcomes(
    frame: pd.DataFrame,
    *,
    development_q10_usdc: float,
    repair_horizon_s: float = REPAIR_HORIZON_S,
) -> pd.DataFrame:
    output = frame.copy()
    terminal = _numeric(output, "terminal_campaign_pnl")
    duration = _numeric(output, "decision_to_terminal_s").clip(
        lower=0.0,
        upper=float(repair_horizon_s),
    )
    repair = _numeric(output, "repair_event").clip(lower=0.0, upper=1.0)
    censored = _numeric(output, "campaign_censored").clip(lower=0.0, upper=1.0)
    output["reward"] = _numeric(output, "reward")
    output["terminal_campaign_value"] = terminal
    output["campaign_cost_avoidance"] = -_numeric(output, "campaign_cost")
    output["negative_terminal_protection"] = terminal.clip(upper=0.0)
    output["q10_shortfall_protection"] = (
        terminal - float(development_q10_usdc)
    ).clip(upper=0.0)
    output["campaign_mae_avoidance"] = _numeric(output, "campaign_mae")
    output["repair_event"] = repair
    output["repair_time_avoidance_s"] = -np.where(
        repair.gt(0.5),
        duration,
        float(repair_horizon_s),
    )
    output["censoring_avoidance"] = -censored
    output["queue_reset_value"] = -_numeric(output, "queue_cost")
    # Reward is measured after the replayed ACK/wait/re-entry path and already
    # includes queue cost. Keep this explicit alias so the scorecard does not
    # mistake a pre-latency hazard score for realized action value.
    output["latency_adjusted_value"] = output["reward"]
    values = output.loc[:, OUTCOMES].to_numpy(dtype=float)
    if not np.isfinite(values).all():
        raise ValueError("derived randomized ITT outcomes are non-finite")
    return output


def _arm_day_statistics(
    frame: pd.DataFrame,
    *,
    outcome: str,
    actions: tuple[str, str],
) -> tuple[tuple[str, ...], dict[str, dict[str, np.ndarray]]]:
    work = frame[["day", "action", "behavior_propensity", outcome]].copy()
    work["day"] = work["day"].astype(str)
    work["action"] = work["action"].astype(str)
    work["weight"] = 1.0 / _numeric(work, "behavior_propensity")
    work["weighted_value"] = work["weight"] * _numeric(work, outcome)
    work["weight_sq"] = np.square(work["weight"])
    days = tuple(sorted(work["day"].unique()))
    grouped = work.groupby(["day", "action"], sort=True).agg(
        weighted_sum=("weighted_value", "sum"),
        weight_sum=("weight", "sum"),
        weight_sq_sum=("weight_sq", "sum"),
        rows=(outcome, "count"),
    )
    arrays: dict[str, dict[str, np.ndarray]] = {}
    for action in actions:
        values: dict[str, list[float]] = {
            "weighted_sum": [],
            "weight_sum": [],
            "weight_sq_sum": [],
            "rows": [],
        }
        for day in days:
            key = (day, action)
            row = grouped.loc[key] if key in grouped.index else None
            for field in values:
                values[field].append(float(row[field]) if row is not None else 0.0)
        arrays[action] = {
            field: np.asarray(entries, dtype=float)
            for field, entries in values.items()
        }
    return days, arrays


def randomized_itt_contrast(
    frame: pd.DataFrame,
    *,
    outcome: str,
    baseline_action: str = BASELINE_ACTION,
    candidate_action: str = CANDIDATE_ACTION,
    bootstrap_trials: int = 5_000,
    random_seed: int = 20260722,
) -> dict[str, Any]:
    actions = (str(baseline_action), str(candidate_action))
    days, arrays = _arm_day_statistics(frame, outcome=outcome, actions=actions)

    def arm_mean(action: str, indices: np.ndarray) -> float:
        arm = arrays[action]
        denominator = float(arm["weight_sum"][indices].sum())
        if denominator <= 0.0:
            return math.nan
        return float(arm["weighted_sum"][indices].sum() / denominator)

    all_indices = np.arange(len(days), dtype=int)
    baseline_mean = arm_mean(actions[0], all_indices)
    candidate_mean = arm_mean(actions[1], all_indices)
    if not math.isfinite(baseline_mean) or not math.isfinite(candidate_mean):
        raise ValueError(f"both actions need support for {outcome}")
    uplift = candidate_mean - baseline_mean

    rng = np.random.default_rng(int(random_seed))
    samples = np.empty(int(bootstrap_trials), dtype=float)
    for trial in range(int(bootstrap_trials)):
        chosen = rng.integers(0, len(days), size=len(days))
        samples[trial] = arm_mean(actions[1], chosen) - arm_mean(actions[0], chosen)
    if not np.isfinite(samples).all():
        raise ValueError(f"day bootstrap lost action support for {outcome}")

    daily_values: list[float] = []
    for index in all_indices:
        baseline_day = arm_mean(actions[0], np.asarray([index], dtype=int))
        candidate_day = arm_mean(actions[1], np.asarray([index], dtype=int))
        if math.isfinite(baseline_day) and math.isfinite(candidate_day):
            daily_values.append(candidate_day - baseline_day)

    arm_support: dict[str, dict[str, Any]] = {}
    for action in actions:
        weight_sum = float(arrays[action]["weight_sum"].sum())
        weight_sq_sum = float(arrays[action]["weight_sq_sum"].sum())
        arm_support[action] = {
            "rows": int(arrays[action]["rows"].sum()),
            "hajek_mean": arm_mean(action, all_indices),
            "effective_sample_size": (
                weight_sum * weight_sum / weight_sq_sum
                if weight_sq_sum > 0.0
                else 0.0
            ),
        }
    return {
        "schema_version": "randomized_itt_contrast.v1",
        "outcome": outcome,
        "estimand": f"{actions[1]}_minus_{actions[0]}",
        "uplift": float(uplift),
        "interval": {
            "p025": float(np.quantile(samples, 0.025)),
            "p50": float(np.quantile(samples, 0.50)),
            "p975": float(np.quantile(samples, 0.975)),
        },
        "daily_positive_days": int(sum(value > 0.0 for value in daily_values)),
        "daily_negative_days": int(sum(value < 0.0 for value in daily_values)),
        "daily_tied_days": int(sum(value == 0.0 for value in daily_values)),
        "daily_contrast_days": int(len(daily_values)),
        "daily_positive_rate": (
            float(np.mean(np.asarray(daily_values) > 0.0))
            if daily_values
            else math.nan
        ),
        "bootstrap": {
            "cluster": "UTC_day",
            "trials": int(bootstrap_trials),
            "random_seed": int(random_seed),
        },
        "arms": arm_support,
    }


def _scope_report(
    frame: pd.DataFrame,
    *,
    q10: float,
    bootstrap_trials: int,
    random_seed: int,
) -> dict[str, Any]:
    derived = derive_outcomes(frame, development_q10_usdc=q10)
    outcomes = {
        outcome: randomized_itt_contrast(
            derived,
            outcome=outcome,
            bootstrap_trials=bootstrap_trials,
            random_seed=int(random_seed) + index,
        )
        for index, outcome in enumerate(OUTCOMES)
    }
    action = derived["action"].astype(str)
    propensity = _numeric(derived, "behavior_propensity")
    support: dict[str, Any] = {
        "rows": int(len(derived)),
        "days": int(derived["day"].astype(str).nunique()),
        "action_rows": {
            name: int(action.eq(name).sum())
            for name in (BASELINE_ACTION, CANDIDATE_ACTION)
        },
        "minimum_behavior_propensity": float(propensity.min()),
        "minimum_arm_effective_sample_size": float(
            min(
                row["effective_sample_size"]
                for row in outcomes["reward"]["arms"].values()
            )
        ),
    }
    return {"support": support, "outcomes": outcomes}


def build_randomized_itt_report(
    panel: pd.DataFrame,
    *,
    metadata: Mapping[str, Any],
    daily: pd.DataFrame | None = None,
    bootstrap_trials: int = 5_000,
    random_seed: int = 20260722,
) -> dict[str, Any]:
    validate_randomized_panel(panel)
    baseline_terminal = _numeric(
        panel.loc[panel["action"].astype(str).eq(BASELINE_ACTION)],
        "terminal_campaign_pnl",
    )
    q10 = float(baseline_terminal.quantile(0.10))
    scopes = {
        "pooled": panel,
        "buy": panel.loc[panel["side"].astype(str).str.upper().eq("BUY")],
        "sell": panel.loc[panel["side"].astype(str).str.upper().eq("SELL")],
    }
    reports = {
        name: _scope_report(
            scoped,
            q10=q10,
            bootstrap_trials=bootstrap_trials,
            random_seed=int(random_seed) + 100 * index,
        )
        for index, (name, scoped) in enumerate(scopes.items())
        if not scoped.empty
    }
    selectivity = {
        name: randomized_panel_selectivity(
            scoped,
            candidate_action=CANDIDATE_ACTION,
            baseline_action=BASELINE_ACTION,
            bootstrap_trials=bootstrap_trials,
            random_seed=int(random_seed) + 1_000 + index,
        )
        for index, (name, scoped) in enumerate(scopes.items())
        if not scoped.empty
    }
    native = dict(metadata.get("native_action_support") or {})
    total_strategy: dict[str, Any] = {}
    candidate_rate = math.nan
    if daily is not None and not daily.empty:
        control_fills = float(_numeric(daily, "control_fills_total").sum())
        candidate_fills = float(_numeric(daily, "randomized_fills_total").sum())
        randomized_campaigns = float(
            _numeric(daily, "randomized_campaign_count").sum()
        )
        total_strategy = {
            "control_fills": int(control_fills),
            "candidate_fills": int(candidate_fills),
            "fills_retention": candidate_fills / max(control_fills, 1.0),
            "pnl_delta_usdc": float(_numeric(daily, "pnl_delta").sum()),
            "pnl_positive_days": int((_numeric(daily, "pnl_delta") > 0.0).sum()),
            "pnl_positive_rate": float((_numeric(daily, "pnl_delta") > 0.0).mean()),
            "randomized_campaigns": int(randomized_campaigns),
        }
        candidate_rate = float(len(panel) / max(randomized_campaigns, 1.0))
    return {
        "schema_version": SCHEMA_VERSION,
        "family_id": "queue_value_net_hazard_keep_cancel_v2",
        "panel_role": str(metadata.get("panel_role", "development")),
        "estimand": (
            "logged randomized intention-to-treat within the frozen replay; "
            "native-incomplete rows retained"
        ),
        "development_q10_usdc": q10,
        "development_q10_definition": (
            "10th percentile of logged keep terminal campaign PnL; diagnostic "
            "and frozen before any later panel access"
        ),
        "candidate_rate": candidate_rate,
        "scopes": reports,
        "toxic_fill_selectivity": selectivity,
        "native_action_support": native,
        "native_strict_gate_passed": bool(
            native.get("seed_gate", False) and native.get("path_gate", False)
        ),
        "total_strategy": total_strategy,
        "validation_accessed": False,
        "sealed_holdout_accessed": False,
    }


def _score_metric(row: Mapping[str, Any]) -> dict[str, Any]:
    interval = row.get("interval") or {}
    return {
        "estimate": float(row["uplift"]),
        "lower_bound": float(interval["p025"]),
        "upper_bound": float(interval["p975"]),
        "daily_positive_rate": float(row["daily_positive_rate"]),
        "source": f"randomized_itt.{row['outcome']}",
    }


def canonical_score_evidence(
    report: Mapping[str, Any],
    *,
    family_spec: Mapping[str, Any],
    input_identity: Mapping[str, Any],
) -> dict[str, Any]:
    pooled = (report.get("scopes") or {}).get("pooled") or {}
    outcomes = pooled.get("outcomes") or {}
    support = pooled.get("support") or {}
    selectivity = (
        (report.get("toxic_fill_selectivity") or {}).get("pooled") or {}
    )
    point = selectivity.get("point") or {}
    metrics = {
        "conditional_net_value": _score_metric(outcomes["reward"]),
        "negative_terminal_protection": _score_metric(
            outcomes["negative_terminal_protection"]
        ),
        "q10_shortfall_protection": _score_metric(
            outcomes["q10_shortfall_protection"]
        ),
        "campaign_mae_avoidance": _score_metric(
            outcomes["campaign_mae_avoidance"]
        ),
        "repair_event": _score_metric(outcomes["repair_event"]),
        "repair_time_avoidance_s": _score_metric(
            outcomes["repair_time_avoidance_s"]
        ),
        "censoring_avoidance": _score_metric(
            outcomes["censoring_avoidance"]
        ),
        "queue_reset_value": _score_metric(outcomes["queue_reset_value"]),
        "latency_adjusted_value": _score_metric(
            outcomes["latency_adjusted_value"]
        ),
        # This is intentionally intervention-level retention. Aggregate daily
        # fills are a separate drift diagnostic and cannot justify an action
        # that simply suppresses almost every eligible order.
        "fills_retention": {
            "estimate": float(point["fills_retention"]),
            "source": "randomized_toxic_fill_selectivity.point",
        },
        "toxic_fill_selectivity_log_ratio": {
            **selectivity_metric_from_summary(
                selectivity,
                "toxic_selectivity_log_ratio",
            ),
            "source": "randomized_toxic_fill_selectivity",
        },
        "toxic_reduction_surplus": {
            **selectivity_metric_from_summary(
                selectivity,
                "toxic_reduction_surplus",
            ),
            "source": "randomized_toxic_fill_selectivity",
        },
    }
    native = report.get("native_action_support") or {}
    support_failures: list[str] = []
    if not bool(native.get("seed_gate", False)):
        support_failures.append("native_exchange_seed_support_below_gate")
    if not bool(native.get("path_gate", False)):
        support_failures.append("native_exchange_path_support_below_gate")
    family_failures: list[str] = []
    if float(metrics["conditional_net_value"]["lower_bound"]) <= 0.0:
        family_failures.append("randomized_itt_reward_lower_bound_not_positive")
    return {
        "schema_version": CANONICAL_EVIDENCE_SCHEMA_VERSION,
        "experiment_id": str(report.get("family_id", "")),
        "family_id": str(report.get("family_id", "")),
        "panel_role": str(report.get("panel_role", "development")),
        "score_profile_contract": dict(
            family_spec.get("score_profile_contract") or {}
        ),
        "input_identity": dict(input_identity),
        "validity_failures": [],
        "support": {
            "n_rows": int(support.get("rows", 0)),
            "n_days": int(support.get("days", 0)),
            "effective_sample_size": float(
                support.get("minimum_arm_effective_sample_size", 0.0)
            ),
            "minimum_behavior_propensity": float(
                support.get("minimum_behavior_propensity", 0.0)
            ),
            "unsupported_mass": 0.0,
            "overlap_violations": 0,
            "failures": support_failures,
        },
        "candidate_rate": float(report.get("candidate_rate", math.nan)),
        "invariant_violations": [],
        "family_gate_failures": family_failures,
        "metrics": metrics,
    }


def render_report_markdown(
    report: Mapping[str, Any],
    scorecard: Mapping[str, Any],
) -> str:
    selectivity = report["toxic_fill_selectivity"]["pooled"]
    point = selectivity["point"]
    interval = selectivity["day_cluster_bootstrap"]["intervals"]
    strategy = report.get("total_strategy") or {}
    lines = [
        "# Queue Value Net Hazard Keep/Cancel v2",
        "",
        "## Decision",
        "",
        f"- Status: **{scorecard['promotion_status']}**",
        "- Validation: unread",
        "- Sealed holdout: unread",
        "- Live: not eligible",
        "",
        "## Selectivity",
        "",
        "| Metric | Point | 95% UTC-day interval |",
        "|---|---:|---:|",
        (
            "| Intervention fills retention | "
            f"{point['fills_retention']:.2%} | "
            f"[{interval['fills_retention']['p025']:.2%}, "
            f"{interval['fills_retention']['p975']:.2%}] |"
        ),
        (
            "| Toxic fills retention | "
            f"{point['toxic_fills_retention']:.2%} | "
            f"[{interval['toxic_fills_retention']['p025']:.2%}, "
            f"{interval['toxic_fills_retention']['p975']:.2%}] |"
        ),
        (
            "| Toxic-reduction surplus | "
            f"{point['toxic_reduction_surplus']:+.4f} | "
            f"[{interval['toxic_reduction_surplus']['p025']:+.4f}, "
            f"{interval['toxic_reduction_surplus']['p975']:+.4f}] |"
        ),
        (
            "| Toxic selectivity log ratio | "
            f"{point['toxic_selectivity_log_ratio']:+.4f} | "
            f"[{interval['toxic_selectivity_log_ratio']['p025']:+.4f}, "
            f"{interval['toxic_selectivity_log_ratio']['p975']:+.4f}] |"
        ),
        (
            "| Reduction leverage (diagnostic) | "
            f"{float(point['toxic_reduction_leverage']):.4f} | n/a |"
        ),
        "",
        (
            "Positive selectivity requires toxic fills to fall faster than all "
            "fills. Here the surplus and log ratio are both negative at the "
            "point estimate, and their lower confidence bounds fail the gate."
        ),
        "",
        "## Randomized ITT",
        "",
        "| Scope | Reward uplift | 95% UTC-day interval | Daily positive |",
        "|---|---:|---:|---:|",
    ]
    for scope in ("pooled", "buy", "sell"):
        reward = report["scopes"][scope]["outcomes"]["reward"]
        lines.append(
            f"| {scope.upper()} | {reward['uplift']:+.6f} USDC | "
            f"[{reward['interval']['p025']:+.6f}, "
            f"{reward['interval']['p975']:+.6f}] | "
            f"{reward['daily_positive_rate']:.1%} |"
        )
    if strategy:
        lines.extend(
            [
                "",
                "## Aggregate Replay",
                "",
                f"- Strategy fills retention: {strategy['fills_retention']:.2%}",
                f"- PnL delta: {strategy['pnl_delta_usdc']:+.6f} USDC",
                f"- Positive PnL days: {strategy['pnl_positive_rate']:.1%}",
                f"- Eligible campaign rate: {float(report['candidate_rate']):.2%}",
            ]
        )
    native = report.get("native_action_support") or {}
    lines.extend(
        [
            "",
            "## Support",
            "",
            f"- Native seed support: {float(native.get('seed_support_ratio', 0.0)):.2%}",
            f"- Native outcome support: {float(native.get('outcome_support_ratio', 0.0)):.2%}",
            (
                "- Strict DR/OPE remains blocked; unsupported rows were retained "
                "in the randomized ITT rather than selected away."
            ),
            "",
            "## Conclusion",
            "",
            (
                "This exact family mostly removes fill opportunity rather than "
                "selectively removing toxic fills. It is closed at Development; "
                "neither Validation nor sealed holdout should be opened."
            ),
            "",
        ]
    )
    return "\n".join(lines)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--panel", type=Path, required=True)
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--daily", type=Path, required=True)
    parser.add_argument("--family-spec", type=Path, required=True)
    parser.add_argument("--output-prefix", type=Path, required=True)
    parser.add_argument("--bootstrap-trials", type=int, default=5_000)
    parser.add_argument("--random-seed", type=int, default=20260722)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    panel_path = args.panel.expanduser().resolve()
    metadata_path = args.metadata.expanduser().resolve()
    daily_path = args.daily.expanduser().resolve()
    family_spec_path = args.family_spec.expanduser().resolve()
    output_prefix = args.output_prefix.expanduser().resolve()
    output_prefix.parent.mkdir(parents=True, exist_ok=True)
    panel = pd.read_csv(panel_path, low_memory=False)
    daily = pd.read_csv(daily_path)
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    family_spec = json.loads(family_spec_path.read_text(encoding="utf-8"))
    report = build_randomized_itt_report(
        panel,
        metadata=metadata,
        daily=daily,
        bootstrap_trials=int(args.bootstrap_trials),
        random_seed=int(args.random_seed),
    )
    report_path = output_prefix.with_suffix(".itt.json")
    report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    identity = {
        "panel": {"path": str(panel_path), "sha256": _sha256(panel_path)},
        "metadata": {
            "path": str(metadata_path),
            "sha256": _sha256(metadata_path),
        },
        "daily": {"path": str(daily_path), "sha256": _sha256(daily_path)},
        "family_spec": {
            "path": str(family_spec_path),
            "sha256": _sha256(family_spec_path),
        },
        "queue_model_bundle_sha256": str(
            metadata.get("queue_model_bundle_sha256", "")
        ),
        "config_sha256": str(metadata.get("config_sha256", "")),
        "workspace_sha256": str(metadata.get("workspace_sha256", "")),
    }
    evidence = canonical_score_evidence(
        report,
        family_spec=family_spec,
        input_identity=identity,
    )
    scorecard = score_canonical_evidence(
        evidence,
        profile_id="action_execution_selective_v1",
    )
    scorecard_path = output_prefix.with_suffix(".scorecard.json")
    scorecard_path.write_text(
        json.dumps(scorecard, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    output_prefix.with_suffix(".scorecard.md").write_text(
        render_scorecard_markdown(scorecard),
        encoding="utf-8",
    )
    output_prefix.with_suffix(".md").write_text(
        render_report_markdown(report, scorecard),
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "report": str(report_path),
                "scorecard": str(scorecard_path),
                "promotion_status": scorecard["promotion_status"],
                "ranking_eligible": scorecard["ranking_eligible"],
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
