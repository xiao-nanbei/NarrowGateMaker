#!/usr/bin/env python3
"""Outcome-blind threshold and unready-day binding for guard replay v1.5.

The v1.4 two-pass binding retained the untreated quote denominator, but it did
not retain the held toxicity prediction attached to each quote decision.  This
successor records that prediction so the frozen past-only p90 can be built from
the exact baseline-eligible opportunity population.  It also supplies the
required no-assignment second pass for threshold-unready Development days.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from execution.chunked_parquet_journal import iter_chunked_parquet_journal
from research.families.f09_campaign_action_uplift.audit.causal_v12_ranked_toxicity_exposure_guard_full_path_adapter import (
    AdapterContractViolation,
    BaselineShadowSnapshot,
)
from research.families.f09_campaign_action_uplift.audit.ranked_toxicity_guard_authoritative_replay import (
    BASELINE_ELIGIBLE_ACTIONS,
    BASELINE_SHADOW_EVENT,
    BaselineShadowDecisionRecord,
    BaselineShadowTapeIndexV14,
    RankedToxicityBaselineShadowCaptureV14,
    RankedToxicityGuardAuthoritativeReplayV14,
    ReplayGuardDirective,
    _fingerprint_tokens,
)

SCHEMA_VERSION = "ranked_toxicity_guard_authoritative_replay_binding.v1.5"
VALID_ROLES = frozenset({"opener", "add"})


def _canonical_sha256(payload: Any) -> str:
    raw = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


class RankedToxicityBaselineShadowCaptureV15(
    RankedToxicityBaselineShadowCaptureV14
):
    """Capture the exact untreated denominator and its held prediction."""

    mode = "baseline_shadow_capture_with_held_prediction"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._held_prediction: dict[str, Any] | None = None

    def on_prediction_bucket(
        self,
        *,
        prediction_bucket_ts_ms: int,
        feature_ready_ts_ms: int,
        observation_ts_ms: int,
        tox_bid: float,
        tox_ask: float,
    ) -> None:
        super().on_prediction_bucket(
            prediction_bucket_ts_ms=prediction_bucket_ts_ms,
            feature_ready_ts_ms=feature_ready_ts_ms,
            observation_ts_ms=observation_ts_ms,
            tox_bid=tox_bid,
            tox_ask=tox_ask,
        )
        bucket = int(prediction_bucket_ts_ms)
        ready = int(feature_ready_ts_ms)
        observed = int(observation_ts_ms)
        scores = {"BUY": float(tox_bid), "SELL": float(tox_ask)}
        if bucket <= 0 or bucket % 10_000 != 0:
            raise AdapterContractViolation(
                "baseline prediction bucket is not a completed 10-second boundary"
            )
        if ready < bucket or ready > observed:
            raise AdapterContractViolation(
                "baseline prediction violates source/ready/observation clock order"
            )
        if any(
            not math.isfinite(value) or value < 0.0 or value > 1.0
            for value in scores.values()
        ):
            raise AdapterContractViolation("baseline toxicity score is outside [0, 1]")
        self._held_prediction = {
            "prediction_bucket_ts_ms": bucket,
            "feature_ready_ts_ms": ready,
            "observation_ts_ms": observed,
            "scores": scores,
        }

    def on_quote_decision(
        self,
        *,
        decision_id: str,
        decision_ts_ns: int,
        side: str,
        role: str,
        baseline_eligible: bool,
        exposure_increasing: bool,
        can_post: bool,
        allow_exposure_increase: bool,
        active_exposure_order_id: str,
        quote_price: float,
        quote_quantity: float,
        blocker_reasons: list[str] | tuple[str, ...],
        policy_fingerprint: str,
        untreated_lineage_ordinal: int,
    ) -> ReplayGuardDirective:
        normalized_side = str(side).upper()
        key = str(decision_id)
        if normalized_side not in self.sides:
            return ReplayGuardDirective(key, False, "", True)
        if key in self._pending:
            raise AdapterContractViolation(
                "baseline capture decision_id is not unique"
            )
        ordinal = int(untreated_lineage_ordinal)
        if ordinal <= 0:
            raise ValueError("untreated_lineage_ordinal must be positive")
        prospective_id = (
            f"{self.lineage_namespace}|{normalized_side}|lineage-{ordinal:012d}"
        )
        utc_day = datetime.fromtimestamp(
            int(decision_ts_ns) / 1_000_000_000.0,
            tz=timezone.utc,
        ).date().isoformat()
        snapshot = BaselineShadowSnapshot(
            decision_id=key,
            utc_day=utc_day,
            decision_ts_ns=int(decision_ts_ns),
            side=normalized_side,
            role=str(role),
            baseline_eligible=bool(baseline_eligible),
            exposure_increasing=bool(exposure_increasing),
            can_post=bool(can_post),
            allow_exposure_increase=bool(allow_exposure_increase),
            active_exposure_order_id=str(active_exposure_order_id or ""),
            quote_price=float(quote_price),
            quote_quantity=float(quote_quantity),
            blocker_fingerprint=_fingerprint_tokens(tuple(blocker_reasons)),
            policy_fingerprint=str(policy_fingerprint),
        )
        self._pending[key] = (snapshot, prospective_id)
        self._quote_decision_count += 1
        return ReplayGuardDirective(key, False, "", True)

    def on_final_quote_action(
        self,
        *,
        decision_id: str,
        side: str,
        role: str,
        exposure_increasing: bool,
        candidate_action: str,
        candidate_price: float,
        candidate_quantity: float,
        candidate_order_id: str,
        event_ts_ns: int,
    ) -> bool:
        normalized_side = str(side).upper()
        if normalized_side not in self.sides:
            return False
        pending = self._pending.pop(str(decision_id), None)
        if pending is None:
            raise AdapterContractViolation(
                "baseline final action has no preceding quote decision"
            )
        snapshot, prospective_id = pending
        decision_ts_ms = int(snapshot.decision_ts_ns) // 1_000_000
        held_prediction = self._held_prediction
        if held_prediction is not None:
            ready_ts_ms = int(held_prediction["feature_ready_ts_ms"])
            if ready_ts_ms > decision_ts_ms:
                raise AdapterContractViolation(
                    "held toxicity prediction is not visible at baseline decision"
                )
        final_eligible = bool(
            exposure_increasing
            and str(candidate_action) in BASELINE_ELIGIBLE_ACTIONS
        )
        snapshot = BaselineShadowSnapshot(
            **{
                **asdict(snapshot),
                "role": str(role),
                "baseline_eligible": final_eligible,
                "can_post": bool(str(candidate_action) in BASELINE_ELIGIBLE_ACTIONS),
            }
        )
        record = BaselineShadowDecisionRecord(
            snapshot=snapshot,
            prospective_campaign_side_id=prospective_id,
            baseline_action=str(candidate_action),
            baseline_price=float(candidate_price),
            baseline_quantity=float(candidate_quantity),
            baseline_order_id=str(candidate_order_id or ""),
        )
        prediction = (
            {
                "prediction_bucket_ts_ms": int(
                    held_prediction["prediction_bucket_ts_ms"]
                ),
                "feature_ready_ts_ms": int(held_prediction["feature_ready_ts_ms"]),
                "observation_ts_ms": int(held_prediction["observation_ts_ms"]),
                "toxicity_score": float(held_prediction["scores"][normalized_side]),
            }
            if held_prediction is not None
            else None
        )
        payload = record.to_payload()
        payload["held_prediction"] = prediction
        self._sequence += 1
        self.writer.append(
            {
                "sequence": self._sequence,
                "event_type": BASELINE_SHADOW_EVENT,
                "event_ts_ns": int(event_ts_ns),
                "side": normalized_side,
                "decision_id": str(decision_id),
                "prospective_campaign_side_id": prospective_id,
                "record": payload,
                **(prediction or {"prediction_unready": True}),
            }
        )
        return False


def baseline_opportunities_from_manifests(
    manifests_by_day: Mapping[str, str | Path],
) -> pd.DataFrame:
    """Return one exact eligible opportunity per side and prediction bucket."""

    rows: list[dict[str, Any]] = []
    for expected_day, manifest_path in sorted(manifests_by_day.items()):
        for row in iter_chunked_parquet_journal(manifest_path):
            if row.get("event_type") != BASELINE_SHADOW_EVENT:
                continue
            record_payload = dict(row.get("record") or {})
            prediction = dict(record_payload.get("held_prediction") or {})
            if not prediction:
                continue
            record = BaselineShadowDecisionRecord.from_payload(record_payload)
            snapshot = record.snapshot
            if snapshot.utc_day != str(expected_day):
                raise ValueError("baseline tape day differs from its panel identity")
            if not snapshot.baseline_eligible or not snapshot.exposure_increasing:
                continue
            role = str(snapshot.role).lower()
            if role not in VALID_ROLES:
                continue
            rows.append(
                {
                    "day": snapshot.utc_day,
                    "side": snapshot.side,
                    "role": role,
                    "decision_id": snapshot.decision_id,
                    "decision_ts_ns": int(snapshot.decision_ts_ns),
                    "prediction_bucket_ts_ms": int(
                        prediction["prediction_bucket_ts_ms"]
                    ),
                    "feature_ready_ts_ms": int(
                        prediction["feature_ready_ts_ms"]
                    ),
                    "toxicity_score": float(prediction["toxicity_score"]),
                }
            )
    frame = pd.DataFrame(rows)
    if frame.empty:
        raise ValueError("baseline tapes contain no eligible toxicity opportunities")
    frame.sort_values(
        ["day", "side", "prediction_bucket_ts_ms", "decision_ts_ns"],
        kind="stable",
        inplace=True,
    )
    keys = ["day", "side", "prediction_bucket_ts_ms"]
    grouped = frame.groupby(keys, sort=False, observed=True)
    if (grouped["toxicity_score"].agg(lambda x: x.max() - x.min()) > 1e-12).any():
        raise ValueError("one held prediction bucket has multiple toxicity scores")
    if (grouped["feature_ready_ts_ms"].nunique() > 1).any():
        raise ValueError("one held prediction bucket has multiple ready timestamps")
    frame = frame.drop_duplicates(keys, keep="first").reset_index(drop=True)
    if (frame["feature_ready_ts_ms"] * 1_000_000 > frame["decision_ts_ns"]).any():
        raise ValueError("baseline opportunity used a prediction before feature ready")
    return frame


def build_past_only_threshold_schedule_v15(
    opportunities: pd.DataFrame,
    *,
    development_days: list[str] | tuple[str, ...],
    quantile: float = 0.90,
    minimum_prior_days: int = 5,
    minimum_prior_buckets: int = 500,
) -> tuple[dict[str, dict[str, tuple[float, str]]], pd.DataFrame]:
    """Build side-specific thresholds from exact prior-day opportunities."""

    q = float(quantile)
    if not 0.0 < q < 1.0:
        raise ValueError("quantile must be inside (0, 1)")
    days = [str(day) for day in development_days]
    if days != sorted(set(days)):
        raise ValueError("Development days must be unique and chronological")
    schedules: dict[str, dict[str, tuple[float, str]]] = {
        "BUY": {},
        "SELL": {},
    }
    report_rows: list[dict[str, Any]] = []
    for side in ("BUY", "SELL"):
        side_frame = opportunities[opportunities["side"].eq(side)].copy()
        prior_parts: list[pd.DataFrame] = []
        prior_days_with_support: list[str] = []
        for day in days:
            prior = (
                pd.concat(prior_parts, ignore_index=True)
                if prior_parts
                else side_frame.iloc[0:0].copy()
            )
            ready = bool(
                len(prior_days_with_support) >= int(minimum_prior_days)
                and len(prior) >= int(minimum_prior_buckets)
            )
            threshold = (
                float(
                    np.quantile(
                        prior["toxicity_score"].to_numpy(dtype=float),
                        q,
                        method="higher",
                    )
                )
                if ready
                else math.nan
            )
            source_rows = (
                prior[
                    [
                        "day",
                        "side",
                        "role",
                        "decision_id",
                        "decision_ts_ns",
                        "prediction_bucket_ts_ms",
                        "feature_ready_ts_ms",
                        "toxicity_score",
                    ]
                ].to_dict("records")
                if ready
                else []
            )
            source_hash = _canonical_sha256(source_rows) if ready else ""
            if ready:
                schedules[side][day] = (threshold, source_hash)
            report_rows.append(
                {
                    "day": day,
                    "side": side,
                    "quantile": q,
                    "threshold": threshold,
                    "threshold_ready": ready,
                    "prior_days": int(len(prior_days_with_support)),
                    "prior_buckets": int(len(prior)),
                    "source_identity_sha256": source_hash,
                }
            )
            current = side_frame[side_frame["day"].eq(day)]
            if not current.empty:
                prior_parts.append(current)
                prior_days_with_support.append(day)
    return schedules, pd.DataFrame(report_rows)


class RankedToxicityGuardAuthoritativeReplayV15(
    RankedToxicityGuardAuthoritativeReplayV14
):
    """Candidate binding with exact no-treatment prediction warmup."""

    mode = "candidate_execution_with_prediction_warmup"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._prediction_ready = False
        self._preprediction_pending: dict[str, BaselineShadowDecisionRecord] = {}
        self._preprediction_decision_count = 0

    def on_prediction_bucket(self, **prediction: Any) -> None:
        super().on_prediction_bucket(**prediction)
        self._prediction_ready = True

    def on_quote_decision(
        self, **current: Any
    ) -> Any:
        if self._prediction_ready:
            return super().on_quote_decision(**current)
        side = str(current["side"]).upper()
        decision_id = str(current["decision_id"])
        if side not in self.adapters:
            return ReplayGuardDirective(decision_id, False, "", True)
        record = self.baseline.consume(decision_id, side)
        candidate = BaselineShadowSnapshot(
            decision_id=decision_id,
            utc_day=datetime.fromtimestamp(
                int(current["decision_ts_ns"]) / 1_000_000_000.0,
                tz=timezone.utc,
            ).date().isoformat(),
            decision_ts_ns=int(current["decision_ts_ns"]),
            side=side,
            role=str(current["role"]),
            baseline_eligible=bool(current["baseline_eligible"]),
            exposure_increasing=bool(current["exposure_increasing"]),
            can_post=bool(current["can_post"]),
            allow_exposure_increase=bool(current["allow_exposure_increase"]),
            active_exposure_order_id=str(
                current.get("active_exposure_order_id", "") or ""
            ),
            quote_price=float(current["quote_price"]),
            quote_quantity=float(current["quote_quantity"]),
            blocker_fingerprint=_fingerprint_tokens(
                tuple(current.get("blocker_reasons", ()))
            ),
            policy_fingerprint=str(current["policy_fingerprint"]),
        )
        if candidate != record.snapshot:
            raise AdapterContractViolation(
                "prediction-warmup candidate diverged from untreated baseline"
            )
        if decision_id in self._preprediction_pending:
            raise AdapterContractViolation(
                "prediction-warmup decision identity was reused"
            )
        self._preprediction_pending[decision_id] = record
        self._preprediction_decision_count += 1
        return ReplayGuardDirective(decision_id, False, "", True)

    def on_final_quote_action(self, **candidate: Any) -> bool:
        decision_id = str(candidate["decision_id"])
        record = self._preprediction_pending.pop(decision_id, None)
        if record is None:
            return super().on_final_quote_action(**candidate)
        actual = (
            str(candidate["side"]).upper(),
            str(candidate["role"]).lower(),
            bool(candidate["exposure_increasing"]),
            str(candidate["candidate_action"]),
            float(candidate["candidate_price"]),
            float(candidate["candidate_quantity"]),
            str(candidate.get("candidate_order_id", "") or ""),
        )
        expected = (
            record.snapshot.side,
            str(record.snapshot.role).lower(),
            bool(record.snapshot.exposure_increasing),
            record.baseline_action,
            float(record.baseline_price),
            float(record.baseline_quantity),
            record.baseline_order_id,
        )
        if actual != expected:
            raise AdapterContractViolation(
                "prediction-warmup second pass changed a quote action"
            )
        return False

    def finish_replay(self, *, event_ts_ns: int) -> dict[str, Any]:
        if self._preprediction_pending:
            raise AdapterContractViolation(
                "prediction-warmup replay has unfinished quote decisions"
            )
        audit = super().finish_replay(event_ts_ns=event_ts_ns)
        audit["preprediction_passthrough_decision_count"] = int(
            self._preprediction_decision_count
        )
        return audit


class RankedToxicityThresholdUnreadyReplayV15:
    """Exact no-assignment second pass for a threshold-unready UTC day."""

    mode = "threshold_unready_candidate_passthrough"

    def __init__(self, *, baseline_manifest_path: str | Path) -> None:
        self.baseline = BaselineShadowTapeIndexV14(baseline_manifest_path)
        self._pending: dict[str, BaselineShadowDecisionRecord] = {}
        self._prediction_signatures: dict[int, tuple[int, int, float, float]] = {}
        self._last_bucket = 0
        self._prediction_count = 0
        self._decision_count = 0

    def validate_replay_start(self, *, params: Mapping[str, Any], ml_data: Any) -> None:
        if ml_data is None:
            raise ValueError("threshold-unready replay still requires causal ML data")
        if bool(params.get("dynamic_fill_hazard_action_enabled", False)):
            raise ValueError("threshold-unready replay requires q90 action OFF")

    def on_prediction_bucket(
        self,
        *,
        prediction_bucket_ts_ms: int,
        feature_ready_ts_ms: int,
        observation_ts_ms: int,
        tox_bid: float,
        tox_ask: float,
    ) -> None:
        bucket = int(prediction_bucket_ts_ms)
        signature = (
            int(feature_ready_ts_ms),
            int(observation_ts_ms),
            float(tox_bid),
            float(tox_ask),
        )
        if bucket in self._prediction_signatures or bucket <= self._last_bucket:
            raise AdapterContractViolation(
                "threshold-unready replay received duplicate/backward prediction"
            )
        self._prediction_signatures[bucket] = signature
        self._last_bucket = bucket
        self._prediction_count += 1

    def on_quote_decision(self, **current: Any) -> ReplayGuardDirective:
        side = str(current["side"]).upper()
        decision_id = str(current["decision_id"])
        record = self.baseline.consume(decision_id, side)
        snapshot = record.snapshot
        candidate = BaselineShadowSnapshot(
            decision_id=decision_id,
            utc_day=datetime.fromtimestamp(
                int(current["decision_ts_ns"]) / 1_000_000_000.0,
                tz=timezone.utc,
            ).date().isoformat(),
            decision_ts_ns=int(current["decision_ts_ns"]),
            side=side,
            role=str(current["role"]),
            baseline_eligible=bool(current["baseline_eligible"]),
            exposure_increasing=bool(current["exposure_increasing"]),
            can_post=bool(current["can_post"]),
            allow_exposure_increase=bool(current["allow_exposure_increase"]),
            active_exposure_order_id=str(
                current.get("active_exposure_order_id", "") or ""
            ),
            quote_price=float(current["quote_price"]),
            quote_quantity=float(current["quote_quantity"]),
            blocker_fingerprint=_fingerprint_tokens(
                tuple(current.get("blocker_reasons", ()))
            ),
            policy_fingerprint=str(current["policy_fingerprint"]),
        )
        if candidate != snapshot:
            raise AdapterContractViolation(
                "threshold-unready candidate diverged from untreated baseline"
            )
        self._pending[decision_id] = record
        self._decision_count += 1
        return ReplayGuardDirective(decision_id, False, "", True)

    def on_final_quote_action(self, **candidate: Any) -> bool:
        decision_id = str(candidate["decision_id"])
        record = self._pending.pop(decision_id, None)
        if record is None:
            raise AdapterContractViolation(
                "threshold-unready final action lacks baseline decision"
            )
        actual = (
            str(candidate["candidate_action"]),
            float(candidate["candidate_price"]),
            float(candidate["candidate_quantity"]),
        )
        expected = (
            record.baseline_action,
            float(record.baseline_price),
            float(record.baseline_quantity),
        )
        if actual != expected:
            raise AdapterContractViolation(
                "threshold-unready second pass changed a quote action"
            )
        return False

    def on_order_submitted(self, **_: Any) -> None:
        return None

    def on_order_activated(self, **_: Any) -> None:
        return None

    def on_cancel_requested(self, **_: Any) -> None:
        return None

    def on_cancel_rejected(self, **_: Any) -> None:
        return None

    def on_order_fill(self, **_: Any) -> None:
        return None

    def on_exchange_terminal(self, **_: Any) -> None:
        return None

    def on_campaign_terminal(self, **_: Any) -> None:
        return None

    def finish_replay(self, *, event_ts_ns: int) -> dict[str, Any]:
        del event_ts_ns
        if self._pending:
            raise AdapterContractViolation(
                "threshold-unready replay has unfinished quote decisions"
            )
        baseline_audit = self.baseline.audit()
        if not baseline_audit["complete"]:
            raise AdapterContractViolation(
                "threshold-unready replay did not consume the exact denominator"
            )
        return {
            "schema_version": f"{SCHEMA_VERSION}.threshold_unready_audit",
            "mode": self.mode,
            "prediction_bucket_count": int(self._prediction_count),
            "quote_decision_count": int(self._decision_count),
            "baseline_shadow": baseline_audit,
            "assignment_count": 0,
            "treatment_event_count": 0,
            "mechanics_results_read": False,
            "economic_outcomes_read": False,
            "action_experiment_authorized": False,
            "live_deployment_authorized": False,
        }


def authoritative_replay_binding_contract_v1_5() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "v1_4_action_semantics_unchanged": True,
        "exact_denominator_records_held_prediction": True,
        "past_only_threshold_source": (
            "baseline_eligible_exposure_opportunity_one_row_per_10s_bucket"
        ),
        "threshold_unready_second_pass": "no_assignment_no_treatment",
        "economic_outcomes_read": False,
        "permissions": {
            "action_experiment_authorized": False,
            "live_deployment_authorized": False,
        },
    }
