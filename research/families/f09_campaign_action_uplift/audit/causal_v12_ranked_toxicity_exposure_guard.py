#!/usr/bin/env python3
"""Mechanics contract for the side-specific ranked-toxicity quote guard.

The guard is deliberately separate from the raw ``adverse_toxicity_threshold``
path.  It consumes one causal v12 toxicity score per completed 10-second
prediction bucket, compares it with a side-specific past-only opportunity
quantile, and changes only permission for exposure-increasing quotes.

This module contains no reward, fill markout, or promotion logic.  It is the
shared mechanics layer for the independently registered BUY and SELL actions.
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any

import numpy as np
import pandas as pd

SCHEMA_VERSION = "causal_v12_ranked_toxicity_exposure_guard.v1"
CONTROL_ACTION = "baseline_permission"
CANDIDATE_ACTION = "ranked_toxicity_guard"
VALID_ACTIONS = frozenset((CONTROL_ACTION, CANDIDATE_ACTION))
VALID_SIDES = frozenset(("BUY", "SELL"))


class GuardState(str, Enum):
    BASELINE = "BASELINE"
    GUARD_ACTIVE = "GUARD_ACTIVE"
    CANCEL_PENDING = "CANCEL_PENDING"
    SUPPRESSING = "SUPPRESSING"
    RELEASED = "RELEASED"


@dataclass(frozen=True)
class GuardAssignment:
    action: str
    behavior_propensity: float
    uniform_draw: float
    randomization_stratum: str
    campaign_opportunity_id: int

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class GuardTransition:
    prior_state: str
    state: str
    prediction_bucket_ts_ms: int
    score: float
    threshold: float
    duplicate_bucket: bool
    activated: bool
    released: bool
    request_cancel_once: bool
    suppress_exposure_submission: bool
    release_waiting_for_cancel_ack: bool

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


def _normalize_side(side: str) -> str:
    normalized = str(side).strip().upper()
    if normalized not in VALID_SIDES:
        raise ValueError("side must be BUY or SELL")
    return normalized


def _normalize_action(action: str) -> str:
    normalized = str(action).strip()
    if normalized not in VALID_ACTIONS:
        raise ValueError(f"unsupported ranked-toxicity action: {normalized}")
    return normalized


def deterministic_campaign_side_assignment(
    *,
    seed: int,
    utc_day: str,
    side: str,
    campaign_opportunity_id: int,
    candidate_probability: float = 0.5,
) -> GuardAssignment:
    """Assign once before a campaign-side downstream path is generated."""

    normalized_side = _normalize_side(side)
    probability = float(candidate_probability)
    if not math.isfinite(probability) or not 0.0 < probability < 1.0:
        raise ValueError("candidate_probability must be strictly inside (0, 1)")
    opportunity_id = int(campaign_opportunity_id)
    if opportunity_id <= 0:
        raise ValueError("campaign_opportunity_id must be positive")
    stratum = f"{str(utc_day)}|{normalized_side}"
    identity = (
        f"{SCHEMA_VERSION}|{int(seed)}|{stratum}|{opportunity_id}"
    ).encode("ascii")
    integer = int.from_bytes(hashlib.sha256(identity).digest()[:8], "big")
    draw = (integer + 0.5) / float(1 << 64)
    action = CANDIDATE_ACTION if draw < probability else CONTROL_ACTION
    propensity = probability if action == CANDIDATE_ACTION else 1.0 - probability
    return GuardAssignment(
        action=action,
        behavior_propensity=float(propensity),
        uniform_draw=float(draw),
        randomization_stratum=stratum,
        campaign_opportunity_id=opportunity_id,
    )


def collapse_eligible_prediction_buckets(
    decisions: pd.DataFrame,
    *,
    side: str,
) -> pd.DataFrame:
    """Return one baseline-eligible exposure opportunity per 10s prediction.

    The first eligible decision in a prediction bucket is authoritative.  All
    repeated 100ms decisions in that bucket must carry the same score and
    feature-ready timestamp, otherwise the supposedly sample-and-held model
    output is internally inconsistent and the function fails closed.
    """

    normalized_side = _normalize_side(side)
    required = {
        "day",
        "side",
        "decision_ts_ms",
        "prediction_bucket_ts_ms",
        "feature_ready_ts_ms",
        "toxicity_score",
        "baseline_eligible",
        "exposure_increasing",
        "role",
    }
    missing = sorted(required - set(decisions.columns))
    if missing:
        raise ValueError(f"ranked-toxicity decisions missing columns: {missing}")

    frame = decisions.copy()
    frame["side"] = frame["side"].astype(str).str.upper()
    frame = frame[
        (frame["side"] == normalized_side)
        & frame["baseline_eligible"].astype(bool)
        & frame["exposure_increasing"].astype(bool)
    ].copy()
    if frame.empty:
        return frame.sort_values(
            ["day", "prediction_bucket_ts_ms", "decision_ts_ms"]
        ).reset_index(drop=True)

    for column in (
        "decision_ts_ms",
        "prediction_bucket_ts_ms",
        "feature_ready_ts_ms",
    ):
        frame[column] = pd.to_numeric(frame[column], errors="raise").astype("int64")
    frame["toxicity_score"] = pd.to_numeric(
        frame["toxicity_score"], errors="raise"
    ).astype(float)
    if not np.isfinite(frame["toxicity_score"]).all():
        raise ValueError("toxicity_score contains non-finite values")
    if ((frame["toxicity_score"] < 0.0) | (frame["toxicity_score"] > 1.0)).any():
        raise ValueError("toxicity_score must be in [0, 1]")
    if (frame["feature_ready_ts_ms"] > frame["decision_ts_ms"]).any():
        raise ValueError("feature_ready_ts_ms exceeds decision_ts_ms")
    if (frame["prediction_bucket_ts_ms"] > frame["feature_ready_ts_ms"]).any():
        raise ValueError("prediction bucket becomes visible before it exists")

    key = ["day", "side", "prediction_bucket_ts_ms"]
    grouped = frame.groupby(key, sort=False, observed=True)
    score_spread = grouped["toxicity_score"].agg(lambda values: values.max() - values.min())
    if (score_spread > 1e-12).any():
        raise ValueError("one prediction bucket carries multiple toxicity scores")
    ready_count = grouped["feature_ready_ts_ms"].nunique()
    if (ready_count > 1).any():
        raise ValueError("one prediction bucket carries multiple ready timestamps")

    frame = frame.sort_values(key + ["decision_ts_ms"], kind="stable")
    return frame.drop_duplicates(key, keep="first").reset_index(drop=True)


def build_past_only_quantile_schedule(
    opportunities: pd.DataFrame,
    *,
    quantile: float = 0.90,
    minimum_prior_days: int = 5,
    minimum_prior_buckets: int = 500,
) -> pd.DataFrame:
    """Freeze one threshold per UTC day using strictly earlier days only."""

    required = {"day", "toxicity_score"}
    missing = sorted(required - set(opportunities.columns))
    if missing:
        raise ValueError(f"opportunity panel missing columns: {missing}")
    q = float(quantile)
    if not math.isfinite(q) or not 0.0 < q < 1.0:
        raise ValueError("quantile must be strictly inside (0, 1)")
    min_days = int(minimum_prior_days)
    min_buckets = int(minimum_prior_buckets)
    if min_days < 1 or min_buckets < 1:
        raise ValueError("past-only support minima must be positive")

    frame = opportunities[["day", "toxicity_score"]].copy()
    frame["day"] = frame["day"].astype(str)
    frame["toxicity_score"] = pd.to_numeric(
        frame["toxicity_score"], errors="raise"
    ).astype(float)
    if not np.isfinite(frame["toxicity_score"]).all():
        raise ValueError("toxicity_score contains non-finite values")
    days = tuple(sorted(frame["day"].unique()))
    prior_scores: list[float] = []
    prior_days: list[str] = []
    rows: list[dict[str, Any]] = []
    for day in days:
        ready = len(prior_days) >= min_days and len(prior_scores) >= min_buckets
        threshold = (
            float(np.quantile(np.asarray(prior_scores), q, method="higher"))
            if ready
            else math.nan
        )
        rows.append(
            {
                "day": day,
                "quantile": q,
                "threshold": threshold,
                "threshold_ready": bool(ready),
                "prior_days": int(len(prior_days)),
                "prior_buckets": int(len(prior_scores)),
                "latest_history_day": prior_days[-1] if prior_days else None,
            }
        )
        current = frame.loc[frame["day"] == day, "toxicity_score"].tolist()
        prior_scores.extend(float(value) for value in current)
        prior_days.append(day)
    return pd.DataFrame(rows)


class RankedToxicityGuardRuntime:
    """State machine for one fixed campaign-side assignment."""

    def __init__(self, *, side: str, action: str, threshold: float) -> None:
        self.side = _normalize_side(side)
        self.action = _normalize_action(action)
        self.threshold = float(threshold)
        if not math.isfinite(self.threshold) or not 0.0 <= self.threshold <= 1.0:
            raise ValueError("threshold must be finite and in [0, 1]")
        self.state = GuardState.BASELINE
        self.last_prediction_bucket_ts_ms: int | None = None
        self.release_waiting_for_cancel_ack = False
        self.guard_episode_count = 0
        self.cancel_request_count = 0

    @property
    def suppresses_exposure(self) -> bool:
        return self.action == CANDIDATE_ACTION and self.state in {
            GuardState.GUARD_ACTIVE,
            GuardState.CANCEL_PENDING,
            GuardState.SUPPRESSING,
        }

    def on_completed_prediction(
        self,
        *,
        prediction_bucket_ts_ms: int,
        score: float,
        baseline_eligible: bool,
        exposure_increasing: bool,
        active_exposure_order: bool,
    ) -> GuardTransition:
        bucket = int(prediction_bucket_ts_ms)
        value = float(score)
        if not math.isfinite(value) or not 0.0 <= value <= 1.0:
            raise ValueError("score must be finite and in [0, 1]")
        if (
            self.last_prediction_bucket_ts_ms is not None
            and bucket < self.last_prediction_bucket_ts_ms
        ):
            raise ValueError("prediction bucket clock moved backward")
        prior = self.state
        if bucket == self.last_prediction_bucket_ts_ms:
            return GuardTransition(
                prior_state=prior.value,
                state=self.state.value,
                prediction_bucket_ts_ms=bucket,
                score=value,
                threshold=self.threshold,
                duplicate_bucket=True,
                activated=False,
                released=False,
                request_cancel_once=False,
                suppress_exposure_submission=self.suppresses_exposure,
                release_waiting_for_cancel_ack=self.release_waiting_for_cancel_ack,
            )
        self.last_prediction_bucket_ts_ms = bucket

        activated = False
        released = False
        request_cancel = False
        if self.action == CANDIDATE_ACTION:
            if self.suppresses_exposure and value < self.threshold:
                if self.state == GuardState.CANCEL_PENDING:
                    self.release_waiting_for_cancel_ack = True
                else:
                    self.state = GuardState.RELEASED
                    released = True
            elif (
                value >= self.threshold
                and bool(baseline_eligible)
                and bool(exposure_increasing)
                and not self.suppresses_exposure
            ):
                self.state = GuardState.GUARD_ACTIVE
                self.guard_episode_count += 1
                activated = True
                if bool(active_exposure_order):
                    self.state = GuardState.CANCEL_PENDING
                    self.cancel_request_count += 1
                    request_cancel = True
                else:
                    self.state = GuardState.SUPPRESSING

        return GuardTransition(
            prior_state=prior.value,
            state=self.state.value,
            prediction_bucket_ts_ms=bucket,
            score=value,
            threshold=self.threshold,
            duplicate_bucket=False,
            activated=activated,
            released=released,
            request_cancel_once=request_cancel,
            suppress_exposure_submission=self.suppresses_exposure,
            release_waiting_for_cancel_ack=self.release_waiting_for_cancel_ack,
        )

    def on_cancel_ack(self) -> GuardState:
        """Leave the old active-order risk set at exchange terminality."""

        if self.state != GuardState.CANCEL_PENDING:
            raise RuntimeError("cancel ACK observed outside CANCEL_PENDING")
        if self.release_waiting_for_cancel_ack:
            self.state = GuardState.RELEASED
            self.release_waiting_for_cancel_ack = False
        else:
            self.state = GuardState.SUPPRESSING
        return self.state

    def reset_campaign_side(self) -> None:
        """End permission state without carrying old order state forward."""

        if self.state == GuardState.CANCEL_PENDING:
            raise RuntimeError("cannot reset guard while cancel ACK is pending")
        self.state = GuardState.BASELINE
        self.release_waiting_for_cancel_ack = False

    def permission(self, *, exposure_increasing: bool) -> bool:
        if not bool(exposure_increasing):
            return True
        return not self.suppresses_exposure


def summarize_mechanics_journal(journal: pd.DataFrame) -> dict[str, Any]:
    """Summarize an outcome-blind guard journal with explicit denominators."""

    required = {
        "day",
        "action",
        "prediction_bucket_observed",
        "prediction_bucket_exceeded",
        "eligible_decision",
        "eligible_decision_exceeded",
        "campaign_assigned",
        "campaign_activated",
        "final_quote_action_changed",
        "behavior_propensity",
    }
    missing = sorted(required - set(journal.columns))
    if missing:
        raise ValueError(f"guard mechanics journal missing columns: {missing}")
    frame = journal.copy()
    if frame.empty:
        raise ValueError("guard mechanics journal is empty")

    def ratio(numerator: str, denominator: str) -> float:
        den = float(pd.to_numeric(frame[denominator], errors="raise").sum())
        num = float(pd.to_numeric(frame[numerator], errors="raise").sum())
        return num / den if den > 0.0 else math.nan

    assigned = frame[pd.to_numeric(frame["campaign_assigned"], errors="raise") > 0]
    if assigned.empty:
        raise ValueError("guard mechanics journal contains no assignments")
    propensity = pd.to_numeric(
        assigned["behavior_propensity"], errors="raise"
    )
    if ((propensity <= 0.0) | (propensity > 1.0)).any():
        raise ValueError("behavior propensity must be in (0, 1]")
    weights = 1.0 / propensity
    ess = float(weights.sum() ** 2 / np.square(weights).sum())
    assignment_counts = {
        action: int((assigned["action"].astype(str) == action).sum())
        for action in sorted(VALID_ACTIONS)
    }
    return {
        "schema_version": f"{SCHEMA_VERSION}.mechanics_summary",
        "days": int(frame["day"].astype(str).nunique()),
        "prediction_bucket_exceedance_rate": ratio(
            "prediction_bucket_exceeded", "prediction_bucket_observed"
        ),
        "eligible_decision_exceedance_rate": ratio(
            "eligible_decision_exceeded", "eligible_decision"
        ),
        "campaign_activation_rate": ratio(
            "campaign_activated", "campaign_assigned"
        ),
        "final_quote_action_change_rate": ratio(
            "final_quote_action_changed", "campaign_assigned"
        ),
        "assignment_counts": assignment_counts,
        "minimum_behavior_propensity": float(propensity.min()),
        "effective_sample_size": ess,
        "economic_outcome_columns_read": [],
    }
