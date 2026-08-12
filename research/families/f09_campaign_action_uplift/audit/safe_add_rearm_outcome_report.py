#!/usr/bin/env python3
"""Evaluate the frozen safe-add family on development and later holdout.

The report estimates R1/R2 minus R0 directly.  It does not compare candidates
only with the randomized behavior mixture.  Development uses chronological
cross-fitting with embargo; later models are fitted exclusively on the frozen
development panel.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from research.families.f09_campaign_action_uplift.audit.offline_policy_evaluation import (
    OPEConfig,
    evaluate_fixed_holdout_policy,
    evaluate_offline_policy,
    write_outputs,
)
from research.families.f09_campaign_action_uplift.audit.safe_add_rearm_ope_panel import (
    OPE_FEATURES,
    validate_randomized_panel,
)
from models.replay_policies import SAFE_ADD_REARM_ACTIONS

SCHEMA_VERSION = "safe_add_rearm_outcome_report.v1"
CONTROL_ACTION = "r0_block"
CANDIDATE_ACTIONS = ("r1_rearm", "r2_rearm_widen_1tick")
TARGETS = ("reward", "terminal", "tail_avoidance")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _days(path: Path) -> list[str]:
    frame = pd.read_csv(path)
    if "day" not in frame:
        raise ValueError(f"day manifest lacks day column: {path}")
    days = frame["day"].astype(str).str.slice(0, 10).tolist()
    if not days or len(days) != len(set(days)) or days != sorted(days):
        raise ValueError(f"day manifest must be non-empty, unique, and sorted: {path}")
    return days


def _verify_identity(
    development: pd.DataFrame,
    later: pd.DataFrame,
    *,
    development_days: list[str],
    later_days: list[str],
    development_metadata: dict[str, Any],
    later_metadata: dict[str, Any],
    family: dict[str, Any],
) -> dict[str, Any]:
    if not family.get("family_frozen"):
        raise ValueError("safe-add action family is not frozen")
    elapsed_s = float(family.get("selected_elapsed_s"))
    if elapsed_s != 5.0:
        raise ValueError(f"expected frozen 5s family, found {elapsed_s:g}s")
    if tuple(family.get("actions", ())) != tuple(SAFE_ADD_REARM_ACTIONS):
        raise ValueError("frozen action set differs from replay registry")
    probabilities = {
        str(key): float(value)
        for key, value in family.get("behavior_probabilities", {}).items()
    }
    if set(probabilities) != set(SAFE_ADD_REARM_ACTIONS):
        raise ValueError("frozen family lacks a complete propensity vector")

    expected_config_hash = str(
        family.get("preflight_spec", {})
        .get("content", {})
        .get("rolling_baseline_config_sha256", "")
    )
    expected_latency = str(
        family.get("preflight_spec", {})
        .get("content", {})
        .get("latency_profile_id", "")
    )
    for name, panel, metadata, expected_days in (
        ("development", development, development_metadata, development_days),
        ("later", later, later_metadata, later_days),
    ):
        validate_randomized_panel(panel)
        observed_days = sorted(panel["day"].astype(str).str.slice(0, 10).unique())
        if observed_days != expected_days:
            raise ValueError(f"{name} panel days differ from its frozen manifest")
        if float(metadata.get("elapsed_s", math.nan)) != elapsed_s:
            raise ValueError(f"{name} elapsed differs from frozen family")
        if metadata.get("behavior_probabilities") != probabilities:
            raise ValueError(f"{name} propensity vector differs from frozen family")
        if str(metadata.get("config_sha256", "")) != expected_config_hash:
            raise ValueError(f"{name} config hash differs from preflight identity")
        if str(metadata.get("latency_profile_id", "")) != expected_latency:
            raise ValueError(f"{name} latency profile differs from preflight identity")
        if metadata.get("support_only"):
            raise ValueError(f"{name} panel is support-only and has no outcomes")
        if metadata.get("control_replay_run"):
            raise ValueError(f"{name} unexpectedly ran a redundant daily control")
        elapsed_ms = pd.to_numeric(panel["target_elapsed_ms"], errors="coerce")
        if elapsed_ms.isna().any() or not np.allclose(
            elapsed_ms, elapsed_s * 1000.0, atol=1e-8, rtol=0.0
        ):
            raise ValueError(f"{name} rows differ from frozen elapsed threshold")
        for action, probability in probabilities.items():
            values = pd.to_numeric(
                panel[f"behavior_prob_{action}"], errors="coerce"
            ).to_numpy(dtype=float)
            if not np.allclose(values, probability, atol=1e-12, rtol=0.0):
                raise ValueError(f"{name} row propensity differs for {action}")
        if pd.to_numeric(panel["external_reference_used"], errors="coerce").fillna(0).any():
            raise ValueError(f"{name} family must remain local-only")
    if set(development_days) & set(later_days):
        raise ValueError("development and later manifests overlap")
    if max(development_days) >= min(later_days):
        raise ValueError("later holdout must be chronologically after development")
    return {
        "elapsed_s": elapsed_s,
        "actions": list(SAFE_ADD_REARM_ACTIONS),
        "behavior_probabilities": probabilities,
        "config_sha256": expected_config_hash,
        "latency_profile_id": expected_latency,
    }


def _verify_outcome_spec(
    spec: dict[str, Any], *, paths: dict[str, Path], identity: dict[str, Any]
) -> None:
    checks = {
        "frozen_family_sha256": _sha256(paths["frozen_family"]),
        "development_manifest_sha256": _sha256(paths["development_days"]),
        "later_manifest_sha256": _sha256(paths["later_days"]),
        "rolling_baseline_config_sha256": identity["config_sha256"],
        "latency_profile_id": identity["latency_profile_id"],
    }
    for field, observed in checks.items():
        if str(spec.get(field, "")) != str(observed):
            raise ValueError(f"outcome spec identity mismatch for {field}")
    action_family = spec.get("action_family", {})
    if float(action_family.get("elapsed_s", math.nan)) != identity["elapsed_s"]:
        raise ValueError("outcome spec elapsed differs from frozen family")
    if tuple(action_family.get("actions", ())) != tuple(identity["actions"]):
        raise ValueError("outcome spec actions differ from frozen family")


def _target_panel(
    panel: pd.DataFrame, target: str, *, tail_threshold: float
) -> pd.DataFrame:
    output = panel.copy()
    if target == "reward":
        values = pd.to_numeric(output["reward"], errors="coerce")
    elif target == "terminal":
        values = pd.to_numeric(output["terminal_campaign_pnl"], errors="coerce")
    elif target == "tail_avoidance":
        terminal = pd.to_numeric(output["terminal_campaign_pnl"], errors="coerce")
        values = -(terminal <= float(tail_threshold)).astype(float)
    else:  # pragma: no cover - internal registry is fixed
        raise ValueError(f"unknown target: {target}")
    output["ope_target"] = values
    return output


def _bootstrap_contrast(
    frame: pd.DataFrame, *, trials: int, seed: int
) -> dict[str, Any]:
    valid = frame[np.isfinite(frame["dr_contrast"].to_numpy(dtype=float))]
    if valid.empty:
        return {
            "trials": 0,
            "cluster_days": 0,
            "p025": math.nan,
            "p50": math.nan,
            "p975": math.nan,
        }
    clusters = valid.groupby("day", sort=True)["dr_contrast"].agg(["sum", "count"])
    sums = clusters["sum"].to_numpy(dtype=float)
    counts = clusters["count"].to_numpy(dtype=float)
    rng = np.random.default_rng(seed)
    samples = np.empty(int(trials), dtype=float)
    for idx in range(int(trials)):
        chosen = rng.integers(0, len(clusters), size=len(clusters))
        samples[idx] = sums[chosen].sum() / max(counts[chosen].sum(), 1.0)
    return {
        "trials": int(trials),
        "cluster_days": int(len(clusters)),
        "p025": float(np.quantile(samples, 0.025)),
        "p50": float(np.quantile(samples, 0.50)),
        "p975": float(np.quantile(samples, 0.975)),
    }


def _contrast(
    candidate_rows: pd.DataFrame,
    control_rows: pd.DataFrame,
    *,
    stage: str,
    scope: str,
    target: str,
    candidate: str,
    candidate_summary: dict[str, Any],
    control_summary: dict[str, Any],
    trials: int,
    seed: int,
) -> tuple[dict[str, Any], pd.DataFrame]:
    left = candidate_rows[
        ["decision_id", "day", "ope_dr_value", "ope_prediction_valid"]
    ].rename(
        columns={
            "ope_dr_value": "candidate_dr",
            "ope_prediction_valid": "candidate_valid",
        }
    )
    right = control_rows[
        ["decision_id", "ope_dr_value", "ope_prediction_valid"]
    ].rename(
        columns={
            "ope_dr_value": "control_dr",
            "ope_prediction_valid": "control_valid",
        }
    )
    paired = left.merge(right, on="decision_id", how="inner", validate="one_to_one")
    paired = paired[
        (paired["candidate_valid"] == 1)
        & (paired["control_valid"] == 1)
        & np.isfinite(pd.to_numeric(paired["candidate_dr"], errors="coerce"))
        & np.isfinite(pd.to_numeric(paired["control_dr"], errors="coerce"))
    ].copy()
    paired["dr_contrast"] = paired["candidate_dr"] - paired["control_dr"]
    bootstrap = _bootstrap_contrast(paired, trials=trials, seed=seed)
    daily = paired.groupby("day", sort=True)["dr_contrast"].mean()
    candidate_ess = float(
        candidate_summary["overlap"]["effective_sample_size"]
    )
    control_ess = float(control_summary["overlap"]["effective_sample_size"])
    row = {
        "stage": stage,
        "scope": scope,
        "target": target,
        "unit": "probability" if target == "tail_avoidance" else "USDC",
        "candidate": candidate,
        "control": CONTROL_ACTION,
        "paired_rows": int(len(paired)),
        "days": int(paired["day"].nunique()),
        "candidate_dr_value": float(
            candidate_summary["estimators"]["candidate_clipped_dr_value"]
        ),
        "control_dr_value": float(
            control_summary["estimators"]["candidate_clipped_dr_value"]
        ),
        "dr_contrast": float(paired["dr_contrast"].mean()),
        "dr_contrast_p025": bootstrap["p025"],
        "dr_contrast_p50": bootstrap["p50"],
        "dr_contrast_p975": bootstrap["p975"],
        "candidate_ess": candidate_ess,
        "control_ess": control_ess,
        "contrast_min_ess": min(candidate_ess, control_ess),
        "candidate_unsupported_mass": float(
            candidate_summary["overlap"]["mean_unsupported_candidate_mass"]
        ),
        "control_unsupported_mass": float(
            control_summary["overlap"]["mean_unsupported_candidate_mass"]
        ),
        "daily_positive_days": int((daily > 0.0).sum()),
        "daily_negative_days": int((daily < 0.0).sum()),
        "daily_zero_days": int((daily == 0.0).sum()),
        "daily_positive_rate": (
            float((daily > 0.0).mean()) if len(daily) else math.nan
        ),
    }
    paired.insert(0, "stage", stage)
    paired.insert(1, "scope", scope)
    paired.insert(2, "target", target)
    paired.insert(3, "candidate", candidate)
    return row, paired


def _campaign_summary(
    panel: pd.DataFrame, *, stage: str, tail_threshold: float
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for scope, scoped in (
        ("pooled", panel),
        ("buy", panel[panel["side"].astype(str).str.upper() == "BUY"]),
        ("sell", panel[panel["side"].astype(str).str.upper() == "SELL"]),
    ):
        for action in SAFE_ADD_REARM_ACTIONS:
            cell = scoped[scoped["action"].astype(str) == action]
            terminal = pd.to_numeric(cell["terminal_campaign_pnl"], errors="coerce")
            tail = terminal <= float(tail_threshold)
            rows.append(
                {
                    "stage": stage,
                    "scope": scope,
                    "action": action,
                    "rows": int(len(cell)),
                    "days": int(cell["day"].nunique()),
                    "reward_mean": float(
                        pd.to_numeric(cell["reward"], errors="coerce").mean()
                    ),
                    "terminal_mean": float(terminal.mean()),
                    "tail_events": int(tail.sum()),
                    "tail_rate": float(tail.mean()) if len(cell) else math.nan,
                    "tail_expected_shortfall": (
                        float(terminal[tail].mean()) if tail.any() else math.nan
                    ),
                    "campaign_min_pnl_mean": float(
                        pd.to_numeric(cell["campaign_mae"], errors="coerce").mean()
                    ),
                    "campaign_mae_abs_mean": float(
                        pd.to_numeric(cell["campaign_mae"], errors="coerce")
                        .clip(upper=0.0)
                        .abs()
                        .mean()
                    ),
                    "campaign_duration_mean_s": float(
                        pd.to_numeric(
                            cell["campaign_duration_s"], errors="coerce"
                        ).mean()
                    ),
                    "campaign_max_abs_inventory_mean": float(
                        pd.to_numeric(
                            cell["campaign_max_abs_inventory"], errors="coerce"
                        ).mean()
                    ),
                    "censored_rate": float(
                        pd.to_numeric(
                            cell["campaign_censored"], errors="coerce"
                        ).mean()
                    ),
                    "intervention_filled_rate": float(
                        (
                            pd.to_numeric(
                                cell["intervention_fill_count"], errors="coerce"
                            )
                            > 0
                        ).mean()
                    ),
                }
            )
    return pd.DataFrame(rows)


def _markdown(contrasts: pd.DataFrame, campaign: pd.DataFrame, metadata: dict) -> str:
    lines = [
        "# Safe Add-Rearm Frozen-Family Outcome Report",
        "",
        "- Family: `5s / R0-R1-R2 / 1/3 each`",
        f"- Development days: `{metadata['development_days']}`",
        f"- Later days: `{metadata['later_days']}`",
        f"- Tail threshold: `{metadata['tail_threshold']}` USDC terminal MTM",
        "- Contrast: candidate DR pseudo-outcome minus R0 DR pseudo-outcome on the same decision rows",
        "- Later fitting: development only; later rows were not used for Q fitting",
        "",
        "## Decision",
        "",
        "| candidate | development | later | strict promotion |",
        "|---|---|---|---:|",
    ]
    for row in metadata["promotion_decisions"]:
        lines.append(
            f"| `{row['candidate']}` | {row['development_status']} | "
            f"{row['later_status']} | {bool(row['strict_promotion_pass'])} |"
        )
    lines.extend(
        [
            "",
            "## DR Contrasts",
            "",
        "| stage | scope | target | candidate | DR vs R0 [2.5%,97.5%] | ESS | daily + / - |",
        "|---|---|---|---|---:|---:|---:|",
        ]
    )
    for row in contrasts.to_dict("records"):
        lines.append(
            f"| {row['stage']} | {row['scope']} | {row['target']} | "
            f"`{row['candidate']}` | {row['dr_contrast']:+.6f} "
            f"[{row['dr_contrast_p025']:+.6f},{row['dr_contrast_p975']:+.6f}] | "
            f"{row['contrast_min_ess']:.1f} | "
            f"{row['daily_positive_days']} / {row['daily_negative_days']} |"
        )
    lines.extend(
        [
            "",
            "## Campaign Tail",
            "",
            "| stage | scope | action | rows | terminal mean | tail events/rate | tail ES | MAE | duration s | censored |",
            "|---|---|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in campaign.to_dict("records"):
        lines.append(
            f"| {row['stage']} | {row['scope']} | `{row['action']}` | "
            f"{row['rows']} | {row['terminal_mean']:+.6f} | "
            f"{row['tail_events']} / {row['tail_rate']:.4f} | "
            f"{row['tail_expected_shortfall']:+.6f} | "
            f"{row['campaign_mae_abs_mean']:.6f} | "
            f"{row['campaign_duration_mean_s']:.1f} | {row['censored_rate']:.4f} |"
        )
    lines.extend(
        [
            "",
            "> The intervention is randomized once per campaign, but its path may change "
            "later shared state such as markout EMA. Day-clustered uncertainty reduces, "
            "but does not prove away, cross-campaign interference.",
            "",
        ]
    )
    return "\n".join(lines)


def _promotion_decisions(
    contrasts: pd.DataFrame, campaign: pd.DataFrame
) -> list[dict[str, Any]]:
    decisions: list[dict[str, Any]] = []
    for candidate in CANDIDATE_ACTIONS:
        selected = contrasts[
            (contrasts["scope"] == "pooled")
            & (contrasts["candidate"] == candidate)
        ].set_index(["stage", "target"])
        required = [
            (stage, target)
            for stage in ("development", "later")
            for target in TARGETS
        ]
        if any(key not in selected.index for key in required):
            raise ValueError(f"missing pooled contrast for {candidate}")
        dev_reward = selected.loc[("development", "reward")]
        dev_terminal = selected.loc[("development", "terminal")]
        dev_tail = selected.loc[("development", "tail_avoidance")]
        later_reward = selected.loc[("later", "reward")]
        later_terminal = selected.loc[("later", "terminal")]
        later_tail = selected.loc[("later", "tail_avoidance")]
        dev_campaign = campaign[
            (campaign["stage"] == "development")
            & (campaign["scope"] == "pooled")
            & (campaign["action"] == candidate)
        ].iloc[0]
        later_campaign = campaign[
            (campaign["stage"] == "later")
            & (campaign["scope"] == "pooled")
            & (campaign["action"] == candidate)
        ].iloc[0]
        dev_control_campaign = campaign[
            (campaign["stage"] == "development")
            & (campaign["scope"] == "pooled")
            & (campaign["action"] == CONTROL_ACTION)
        ].iloc[0]
        later_control_campaign = campaign[
            (campaign["stage"] == "later")
            & (campaign["scope"] == "pooled")
            & (campaign["action"] == CONTROL_ACTION)
        ].iloc[0]
        development_effect_gate = bool(
            dev_reward["dr_contrast_p025"] > 0.0
            and dev_terminal["dr_contrast_p025"] >= 0.0
            and dev_tail["dr_contrast_p025"] >= 0.0
        )
        development_support_gate = bool(
            min(
                dev_reward["contrast_min_ess"],
                dev_terminal["contrast_min_ess"],
                dev_tail["contrast_min_ess"],
            )
            >= 100.0
            and int(dev_campaign["tail_events"]) >= 5
            and int(dev_control_campaign["tail_events"]) >= 5
        )
        later_direction_gate = bool(
            later_reward["dr_contrast"] > 0.0
            and later_terminal["dr_contrast"] >= 0.0
            and later_tail["dr_contrast"] >= 0.0
        )
        later_interval_gate = bool(
            later_reward["dr_contrast_p025"] > 0.0
            and later_terminal["dr_contrast_p025"] >= 0.0
            and later_tail["dr_contrast_p025"] >= 0.0
        )
        later_support_gate = bool(
            min(
                later_reward["contrast_min_ess"],
                later_terminal["contrast_min_ess"],
                later_tail["contrast_min_ess"],
            )
            >= 100.0
            and int(later_campaign["tail_events"]) >= 5
            and int(later_control_campaign["tail_events"]) >= 5
        )
        decisions.append(
            {
                "candidate": candidate,
                "development_effect_gate": development_effect_gate,
                "development_support_gate": development_support_gate,
                "development_status": (
                    "effect_failed"
                    if not development_effect_gate
                    else (
                        "support_failed"
                        if not development_support_gate
                        else "passed"
                    )
                ),
                "later_direction_gate": later_direction_gate,
                "later_interval_gate": later_interval_gate,
                "later_support_gate": later_support_gate,
                "later_status": (
                    "direction_failed"
                    if not later_direction_gate
                    else (
                        "interval_failed"
                        if not later_interval_gate
                        else (
                            "support_failed"
                            if not later_support_gate
                            else "passed"
                        )
                    )
                ),
                "development_tail_events": int(dev_campaign["tail_events"]),
                "development_control_tail_events": int(
                    dev_control_campaign["tail_events"]
                ),
                "later_tail_events": int(later_campaign["tail_events"]),
                "later_control_tail_events": int(
                    later_control_campaign["tail_events"]
                ),
                "strict_promotion_pass": bool(
                    development_effect_gate
                    and development_support_gate
                    and later_direction_gate
                    and later_interval_gate
                    and later_support_gate
                ),
            }
        )
    return decisions


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--development-panel", type=Path, required=True)
    parser.add_argument("--later-panel", type=Path, required=True)
    parser.add_argument("--development-metadata", type=Path, required=True)
    parser.add_argument("--later-metadata", type=Path, required=True)
    parser.add_argument("--development-days", type=Path, required=True)
    parser.add_argument("--later-days", type=Path, required=True)
    parser.add_argument("--frozen-family", type=Path, required=True)
    parser.add_argument("--outcome-spec", type=Path, required=True)
    parser.add_argument("--output-prefix", type=Path, required=True)
    parser.add_argument("--min-train-days", type=int, default=50)
    parser.add_argument("--test-days", type=int, default=10)
    parser.add_argument("--embargo-days", type=int, default=1)
    parser.add_argument("--min-action-rows", type=int, default=100)
    parser.add_argument("--min-effective-sample-size", type=float, default=100.0)
    parser.add_argument("--max-importance-weight", type=float, default=20.0)
    parser.add_argument("--bootstrap-trials", type=int, default=1000)
    parser.add_argument("--tail-threshold", type=float, default=-5.0)
    parser.add_argument("--random-seed", type=int, default=20260714)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    paths = {
        name: value.expanduser().resolve()
        for name, value in {
            "development_panel": args.development_panel,
            "later_panel": args.later_panel,
            "development_metadata": args.development_metadata,
            "later_metadata": args.later_metadata,
            "development_days": args.development_days,
            "later_days": args.later_days,
            "frozen_family": args.frozen_family,
            "outcome_spec": args.outcome_spec,
        }.items()
    }
    development = pd.read_csv(paths["development_panel"])
    later = pd.read_csv(paths["later_panel"])
    development_days = _days(paths["development_days"])
    later_days = _days(paths["later_days"])
    development_metadata = json.loads(
        paths["development_metadata"].read_text(encoding="utf-8")
    )
    later_metadata = json.loads(paths["later_metadata"].read_text(encoding="utf-8"))
    family = json.loads(paths["frozen_family"].read_text(encoding="utf-8"))
    outcome_spec = json.loads(paths["outcome_spec"].read_text(encoding="utf-8"))
    identity = _verify_identity(
        development,
        later,
        development_days=development_days,
        later_days=later_days,
        development_metadata=development_metadata,
        later_metadata=later_metadata,
        family=family,
    )
    _verify_outcome_spec(outcome_spec, paths=paths, identity=identity)

    prefix = args.output_prefix.expanduser().resolve()
    prefix.parent.mkdir(parents=True, exist_ok=True)
    all_contrasts: list[dict[str, Any]] = []
    all_paired_rows: list[pd.DataFrame] = []
    for scope, development_scope, later_scope in (
        ("pooled", development, later),
        (
            "buy",
            development[development["side"].astype(str).str.upper() == "BUY"],
            later[later["side"].astype(str).str.upper() == "BUY"],
        ),
        (
            "sell",
            development[development["side"].astype(str).str.upper() == "SELL"],
            later[later["side"].astype(str).str.upper() == "SELL"],
        ),
    ):
        for target_idx, target in enumerate(TARGETS):
            development_target = _target_panel(
                development_scope, target, tail_threshold=args.tail_threshold
            )
            later_target = _target_panel(
                later_scope, target, tail_threshold=args.tail_threshold
            )
            stage_results: dict[str, dict[str, tuple[pd.DataFrame, dict[str, Any]]]] = {
                "development": {},
                "later": {},
            }
            for action_idx, action in enumerate(SAFE_ADD_REARM_ACTIONS):
                development_candidate = development_target.copy()
                later_candidate = later_target.copy()
                development_candidate["candidate_action"] = action
                later_candidate["candidate_action"] = action
                cfg = OPEConfig(
                    reward_col="ope_target",
                    split_mode="chronological",
                    min_train_days=int(args.min_train_days),
                    test_days=int(args.test_days),
                    embargo_days=int(args.embargo_days),
                    min_train_rows=max(500, int(args.min_action_rows) * 8),
                    min_action_rows=int(args.min_action_rows),
                    min_effective_sample_size=float(
                        args.min_effective_sample_size
                    ),
                    max_importance_weight=float(args.max_importance_weight),
                    bootstrap_trials=int(args.bootstrap_trials),
                    random_seed=int(args.random_seed + target_idx * 10 + action_idx),
                )
                dev_rows, dev_folds, dev_actions, dev_summary = (
                    evaluate_offline_policy(
                        development_candidate,
                        feature_names=OPE_FEATURES,
                        config=cfg,
                    )
                )
                later_rows, later_folds, later_actions, later_summary = (
                    evaluate_fixed_holdout_policy(
                        development_candidate,
                        later_candidate,
                        feature_names=OPE_FEATURES,
                        config=cfg,
                    )
                )
                stage_results["development"][action] = (dev_rows, dev_summary)
                stage_results["later"][action] = (later_rows, later_summary)
                write_outputs(
                    prefix.parent
                    / f"{prefix.name}_development_{scope}_{target}_{action}",
                    dev_rows,
                    dev_folds,
                    dev_actions,
                    dev_summary,
                )
                write_outputs(
                    prefix.parent / f"{prefix.name}_later_{scope}_{target}_{action}",
                    later_rows,
                    later_folds,
                    later_actions,
                    later_summary,
                )
            for stage in ("development", "later"):
                control_rows, control_summary = stage_results[stage][CONTROL_ACTION]
                for candidate_idx, candidate in enumerate(CANDIDATE_ACTIONS):
                    candidate_rows, candidate_summary = stage_results[stage][candidate]
                    row, paired = _contrast(
                        candidate_rows,
                        control_rows,
                        stage=stage,
                        scope=scope,
                        target=target,
                        candidate=candidate,
                        candidate_summary=candidate_summary,
                        control_summary=control_summary,
                        trials=int(args.bootstrap_trials),
                        seed=int(
                            args.random_seed
                            + target_idx * 100
                            + candidate_idx * 10
                            + (1 if stage == "later" else 0)
                        ),
                    )
                    all_contrasts.append(row)
                    all_paired_rows.append(paired)

    contrasts = pd.DataFrame(all_contrasts)
    paired_rows = pd.concat(all_paired_rows, ignore_index=True)
    campaign = pd.concat(
        [
            _campaign_summary(
                development, stage="development", tail_threshold=args.tail_threshold
            ),
            _campaign_summary(later, stage="later", tail_threshold=args.tail_threshold),
        ],
        ignore_index=True,
    )
    promotion_decisions = _promotion_decisions(contrasts, campaign)
    source_identity = {
        name: {"path": str(path), "sha256": _sha256(path), "bytes": path.stat().st_size}
        for name, path in paths.items()
    }
    metadata = {
        "schema_version": SCHEMA_VERSION,
        "identity": identity,
        "development_days": len(development_days),
        "later_days": len(later_days),
        "development_first_last": [development_days[0], development_days[-1]],
        "later_first_last": [later_days[0], later_days[-1]],
        "later_used_for_fit": False,
        "tail_threshold": float(args.tail_threshold),
        "targets": list(TARGETS),
        "contrast": "candidate deterministic policy DR minus R0 deterministic policy DR",
        "source_identity": source_identity,
        "warning": (
            "One intervention per campaign avoids duplicated terminal labels, but "
            "candidate actions can still affect later shared strategy state."
        ),
        "promotion_decisions": promotion_decisions,
    }
    contrast_path = prefix.with_suffix(".dr_contrasts.csv")
    paired_path = prefix.with_suffix(".dr_paired_rows.csv")
    campaign_path = prefix.with_suffix(".campaign_actions.csv")
    metadata_path = prefix.with_suffix(".metadata.json")
    decision_path = prefix.with_suffix(".decision.json")
    markdown_path = prefix.with_suffix(".report.md")
    contrasts.to_csv(contrast_path, index=False)
    paired_rows.to_csv(paired_path, index=False)
    campaign.to_csv(campaign_path, index=False)
    metadata_path.write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    decision_path.write_text(
        json.dumps(
            {
                "schema_version": SCHEMA_VERSION,
                "promotion_decisions": promotion_decisions,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(
        _markdown(contrasts, campaign, metadata), encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "dr_contrasts": str(contrast_path),
                "dr_paired_rows": str(paired_path),
                "campaign_actions": str(campaign_path),
                "metadata": str(metadata_path),
                "decision": str(decision_path),
                "report": str(markdown_path),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
