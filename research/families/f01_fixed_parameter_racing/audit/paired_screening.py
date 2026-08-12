"""Canonical paired evidence screening and ranking.

This module is the sole ranking authority for paired parameter/replay screens.
It does not decide whether a later panel may be opened and it never promotes a
strategy to live.
"""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np
import pandas as pd

from models.audit.experiment_scorecard import (
    paired_screen_v2_score_evidence,
    score_canonical_evidence,
    score_profile_contract,
)
from research.families.f01_fixed_parameter_racing.parameter_selection import build_paired_daily_evidence

PROFILE_ID = "paired_screen_v2"
RANKING_AUTHORITY = "paired_screen_v2.scorecard_ranking_score"

_PARETO_OBJECTIVES: tuple[tuple[str, bool], ...] = (
    ("raw_delta_sum", True),
    ("terminal_delta_sum", True),
    ("inv_adj_delta_sum", True),
    ("activity_adjusted_raw_delta", True),
    ("campaign_adjusted_terminal_delta", True),
    ("tail_campaign_delta", False),
    ("bad_campaign_rate_delta", False),
)


def _attach_pareto_diagnostic(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    result["pareto_front"] = False
    eligible = result.index[result["scorecard_gate_pass"]].tolist()
    for idx in eligible:
        target = result.loc[idx]
        dominated = False
        for other_idx in eligible:
            if other_idx == idx:
                continue
            other = result.loc[other_idx]
            at_least_as_good = all(
                float(other[column]) >= float(target[column])
                if maximize
                else float(other[column]) <= float(target[column])
                for column, maximize in _PARETO_OBJECTIVES
            )
            strictly_better = any(
                float(other[column]) > float(target[column])
                if maximize
                else float(other[column]) < float(target[column])
                for column, maximize in _PARETO_OBJECTIVES
            )
            if at_least_as_good and strictly_better:
                dominated = True
                break
        result.loc[idx, "pareto_front"] = not dominated
    return result


def rank_paired_daily_evidence(evidence: pd.DataFrame) -> pd.DataFrame:
    """Apply paired_screen_v2 and order arms by its ranking score only."""

    if evidence.empty:
        return evidence.copy()
    required = {
        "arm",
        "baseline_arm",
        "n_days",
        "raw_t_stat",
        "terminal_t_stat",
        "inv_adj_t_stat",
        "fills_ratio",
    }
    missing = sorted(required.difference(evidence.columns))
    if missing:
        raise ValueError(f"paired evidence missing required columns: {missing}")
    forbidden = {
        "selection_tier",
        "candidate_for_blocked_oos",
        "promotion_status",
    }.intersection(evidence.columns)
    if forbidden:
        raise ValueError(
            "paired_screen_v2 requires pure evidence, not prior selection output: "
            f"{sorted(forbidden)}"
        )

    profile_contract = score_profile_contract(PROFILE_ID)
    scorecards = [
        score_canonical_evidence(
            paired_screen_v2_score_evidence(
                row,
                score_profile_contract_value=profile_contract,
            ),
            profile_id=PROFILE_ID,
        )
        for row in evidence.to_dict("records")
    ]
    result = evidence.copy()
    result["scorecard_profile_id"] = PROFILE_ID
    result["scorecard_profile_sha256"] = profile_contract["profile_sha256"]
    result["scorecard_total_score"] = [row["total_score"] for row in scorecards]
    result["scorecard_ranking_score"] = [row["ranking_score"] for row in scorecards]
    result["scorecard_ranking_eligible"] = [
        bool(row["ranking_eligible"]) for row in scorecards
    ]
    result["scorecard_validity_pass"] = [
        bool(row["validity"]["passed"]) for row in scorecards
    ]
    result["scorecard_support_pass"] = [
        bool(row["support"]["passed"]) for row in scorecards
    ]
    result["scorecard_hard_gate_pass"] = [
        bool(row["hard_gates"]["passed"]) for row in scorecards
    ]
    result["scorecard_gate_pass"] = (
        result["scorecard_validity_pass"]
        & result["scorecard_support_pass"]
        & result["scorecard_hard_gate_pass"]
    )
    result["scorecard_gate_notes"] = [
        ",".join(
            [
                *row["validity"]["failures"],
                *row["support"]["failures"],
                *row["hard_gates"]["failures"],
            ]
        )
        or "pass"
        for row in scorecards
    ]
    result["scorecard_candidate_class"] = [
        row["candidate_class"] for row in scorecards
    ]
    result["scorecard_economic_class"] = [
        row["economic_classification"] for row in scorecards
    ]
    result["scorecard_screening_status"] = [
        row["promotion_status"] for row in scorecards
    ]
    result["scorecard_sha256"] = [row["scorecard_sha256"] for row in scorecards]
    result["ranking_authority"] = RANKING_AUTHORITY
    result["promotion_authority"] = False
    result = _attach_pareto_diagnostic(result)
    result["pareto_role"] = "diagnostic_tiebreak_only"

    result["_ranking_score"] = pd.to_numeric(
        result["scorecard_ranking_score"], errors="coerce"
    ).fillna(-np.inf)
    result["_eligible_order"] = (~result["scorecard_ranking_eligible"]).astype(int)
    result = result.sort_values(
        ["_eligible_order", "_ranking_score", "pareto_front", "joint_paired_t", "arm"],
        ascending=[True, False, False, False, True],
        kind="mergesort",
    ).reset_index(drop=True)
    result["screening_rank"] = pd.Series(pd.NA, index=result.index, dtype="Int64")
    eligible_positions = result.index[result["scorecard_ranking_eligible"]]
    result.loc[eligible_positions, "screening_rank"] = np.arange(
        1, len(eligible_positions) + 1
    )
    return result.drop(columns=["_ranking_score", "_eligible_order"])


def screen_paired_daily_arms(
    daily: pd.DataFrame,
    *,
    baseline_arm: str = "baseline",
) -> pd.DataFrame:
    """Build pure evidence, apply canonical gates, and rank comparable arms."""

    return rank_paired_daily_evidence(
        build_paired_daily_evidence(daily, baseline_arm=baseline_arm)
    )


def screening_columns() -> Sequence[str]:
    """Return the stable authority columns expected by downstream runners."""

    return (
        "screening_rank",
        "scorecard_ranking_score",
        "scorecard_gate_pass",
        "scorecard_candidate_class",
        "scorecard_economic_class",
        "ranking_authority",
        "promotion_authority",
    )
