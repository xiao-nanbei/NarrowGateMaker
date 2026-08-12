#!/usr/bin/env python3
"""Execution-only full-path adapter for the ranked-toxicity guard.

The frozen v1 guard defines the action.  This v1.1 adapter supplies the
execution contracts that a regenerated replay path must obey before mechanics
may be read: prospective assignment, an untreated baseline-shadow denominator,
canonical prediction buckets, complete cancel/terminal routing, and strict
retirement of terminal order risk state.

No reward, markout, PnL, or promotion logic belongs in this module.
"""

from __future__ import annotations

import hashlib
import math
import re
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any

from execution.order_lifecycle import (
    FILL_RISK_PHASES,
    OrderLifecyclePhase,
    QuantityWeightedOrderLifecycle,
    terminal_policy_route,
)
from research.families.f09_campaign_action_uplift.audit.causal_v12_ranked_toxicity_exposure_guard import (
    CANDIDATE_ACTION,
    CONTROL_ACTION,
    GuardAssignment,
    GuardState,
    GuardTransition,
    RankedToxicityGuardRuntime,
    deterministic_campaign_side_assignment,
)

SCHEMA_VERSION = (
    "causal_v12_ranked_toxicity_exposure_guard_full_path_adapter.v1.1"
)
VALID_SIDES = frozenset(("BUY", "SELL"))
VALID_ROLES = frozenset(("opener", "add", "reducing"))
FROZEN_RANDOM_SEEDS = {"BUY": 2026080201, "SELL": 2026080202}
ZERO_TOLERANCE_KEYS = (
    "assignment_after_treatment",
    "duplicate_or_inconsistent_prediction_bucket",
    "post_terminal_hazard_or_cursor_reuse",
    "reducing_quote_changes",
    "control_candidate_baseline_shadow_mismatch",
    "campaign_side_rerandomization",
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class AdapterContractViolation(RuntimeError):
    """Fail-closed execution-contract violation."""


@dataclass(frozen=True)
class DailyPastOnlyThreshold:
    utc_day: str
    threshold: float
    source_identity_sha256: str


@dataclass(frozen=True)
class CanonicalPredictionBucket:
    utc_day: str
    prediction_bucket_ts_ms: int
    feature_ready_ts_ms: int
    decision_ts_ms: int
    score: float
    model_sha256: str

    def signature(self) -> tuple[Any, ...]:
        return (
            self.utc_day,
            int(self.prediction_bucket_ts_ms),
            int(self.feature_ready_ts_ms),
            int(self.decision_ts_ms),
            float(self.score),
            self.model_sha256,
        )


@dataclass(frozen=True)
class BaselineShadowSnapshot:
    """Untreated quote opportunity computed independently in each arm."""

    decision_id: str
    utc_day: str
    decision_ts_ns: int
    side: str
    role: str
    baseline_eligible: bool
    exposure_increasing: bool
    can_post: bool
    allow_exposure_increase: bool
    active_exposure_order_id: str
    quote_price: float
    quote_quantity: float
    blocker_fingerprint: str
    policy_fingerprint: str


@dataclass(frozen=True)
class ProspectiveCampaignSideAssignment:
    prospective_campaign_side_id: str
    assignment_utc_day: str
    assignment_ts_ns: int
    first_opportunity_decision_id: str
    assignment_sequence: int
    assignment: GuardAssignment

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["assignment"] = self.assignment.to_payload()
        return payload


@dataclass(frozen=True)
class GuardExecutionDirective:
    decision_id: str
    prospective_campaign_side_id: str
    action: str
    behavior_propensity: float
    state: str
    threshold_utc_day: str
    threshold: float
    request_cancel_once: bool
    cancel_order_id: str
    allow_exposure_submission: bool
    duplicate_bucket: bool


@dataclass
class _TrackedOrder:
    order_id: str
    exposure_increasing: bool
    lifecycle: QuantityWeightedOrderLifecycle
    hazard_attached: bool = False
    cursor_attached: bool = False


def _normalize_side(side: str) -> str:
    normalized = str(side).strip().upper()
    if normalized not in VALID_SIDES:
        raise ValueError("side must be BUY or SELL")
    return normalized


def _normalize_role(role: str) -> str:
    normalized = str(role).strip().lower()
    if normalized not in VALID_ROLES:
        raise ValueError(f"unsupported inventory role: {normalized}")
    return normalized


def _validate_day(value: str) -> str:
    normalized = str(value).strip()
    if date.fromisoformat(normalized).isoformat() != normalized:
        raise ValueError("UTC day must use YYYY-MM-DD")
    return normalized


def _validate_sha256(value: str, label: str) -> str:
    normalized = str(value).strip().lower()
    if _SHA256_RE.fullmatch(normalized) is None:
        raise ValueError(f"{label} must be a lowercase SHA256")
    return normalized


class RankedToxicityGuardFullPathAdapterV11:
    """Bind one side's frozen guard to regenerated order-lifecycle events.

    The adapter accepts two independently maintained *untreated* baseline
    shadows on every decision.  The selected candidate path is separate and is
    affected only through the returned permission/cancel directive.
    """

    def __init__(
        self,
        *,
        side: str,
        random_seed: int,
        frozen_model_sha256: str,
        candidate_probability: float = 0.5,
    ) -> None:
        self.side = _normalize_side(side)
        self.random_seed = int(random_seed)
        if self.random_seed != FROZEN_RANDOM_SEEDS[self.side]:
            raise ValueError(
                f"{self.side} adapter requires frozen random seed "
                f"{FROZEN_RANDOM_SEEDS[self.side]}"
            )
        self.frozen_model_sha256 = _validate_sha256(
            frozen_model_sha256,
            "frozen_model_sha256",
        )
        self.candidate_probability = float(candidate_probability)
        if not math.isclose(
            self.candidate_probability,
            0.5,
            rel_tol=0.0,
            abs_tol=0.0,
        ):
            raise ValueError("candidate_probability is frozen at 0.5")

        self._thresholds: dict[str, DailyPastOnlyThreshold] = {}
        self._bucket_signatures: dict[int, tuple[Any, ...]] = {}
        self._last_bucket_ts_ms: int | None = None
        self._assignment_sequence = 0
        self._current_assignment: ProspectiveCampaignSideAssignment | None = None
        self._closed_assignment_ids: set[str] = set()
        self._runtime: RankedToxicityGuardRuntime | None = None
        self._orders: dict[str, _TrackedOrder] = {}
        self._terminal_order_ids: set[str] = set()
        self._guard_target_order_id = ""
        self._pending_directives: dict[str, GuardExecutionDirective] = {}
        self._journal: list[dict[str, Any]] = []
        self._violations = {key: 0 for key in ZERO_TOLERANCE_KEYS}
        self._treatment_event_count = 0
        self._candidate_path_submit_count = 0
        self._candidate_path_fill_event_count = 0

    @property
    def current_assignment(self) -> ProspectiveCampaignSideAssignment | None:
        return self._current_assignment

    @property
    def state(self) -> GuardState:
        return self._runtime.state if self._runtime is not None else GuardState.BASELINE

    @property
    def suppresses_exposure(self) -> bool:
        return bool(self._runtime is not None and self._runtime.suppresses_exposure)

    def _violate(self, key: str, message: str) -> None:
        if key not in self._violations:
            raise KeyError(f"unknown adapter violation key: {key}")
        self._violations[key] += 1
        raise AdapterContractViolation(message)

    def _append_journal(
        self,
        event_type: str,
        event_ts_ns: int,
        **payload: Any,
    ) -> None:
        assignment = self._current_assignment
        runtime = self._runtime
        row: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "sequence": len(self._journal) + 1,
            "event_type": str(event_type),
            "event_ts_ns": int(event_ts_ns),
            "side": self.side,
            "prospective_campaign_side_id": (
                assignment.prospective_campaign_side_id if assignment else ""
            ),
            "assignment_utc_day": (
                assignment.assignment_utc_day if assignment else ""
            ),
            "action": (
                assignment.assignment.action if assignment else "unassigned"
            ),
            "behavior_propensity": (
                float(assignment.assignment.behavior_propensity)
                if assignment
                else 0.0
            ),
            "guard_state": runtime.state.value if runtime else GuardState.BASELINE.value,
            "guard_episode_count": int(runtime.guard_episode_count) if runtime else 0,
            "cancel_request_count": int(runtime.cancel_request_count) if runtime else 0,
            "release_waiting_for_terminal": bool(
                runtime.release_waiting_for_cancel_ack if runtime else False
            ),
            "economic_outcome_columns_read": [],
        }
        row.update(payload)
        self._journal.append(row)

    def register_daily_threshold(
        self,
        *,
        utc_day: str,
        threshold: float,
        source_identity_sha256: str,
    ) -> DailyPastOnlyThreshold:
        day = _validate_day(utc_day)
        value = float(threshold)
        if not math.isfinite(value) or not 0.0 <= value <= 1.0:
            raise ValueError("daily p90 threshold must be finite and in [0, 1]")
        source_hash = _validate_sha256(
            source_identity_sha256,
            "threshold source identity",
        )
        candidate = DailyPastOnlyThreshold(day, value, source_hash)
        existing = self._thresholds.get(day)
        if existing is not None and existing != candidate:
            raise AdapterContractViolation(
                f"past-only threshold drifted within UTC day {day}"
            )
        self._thresholds[day] = candidate
        return candidate

    def _validate_shadow_snapshot(self, snapshot: BaselineShadowSnapshot) -> None:
        if not str(snapshot.decision_id).strip():
            raise ValueError("baseline-shadow decision_id is required")
        if _validate_day(snapshot.utc_day) != snapshot.utc_day:
            raise ValueError("baseline-shadow UTC day is invalid")
        if int(snapshot.decision_ts_ns) <= 0:
            raise ValueError("baseline-shadow decision timestamp must be positive")
        if _normalize_side(snapshot.side) != self.side:
            raise ValueError("baseline-shadow side differs from adapter side")
        role = _normalize_role(snapshot.role)
        if role == "reducing" and bool(snapshot.exposure_increasing):
            raise ValueError("reducing shadow decision cannot increase exposure")
        if role in {"opener", "add"} and not bool(snapshot.exposure_increasing):
            raise ValueError("opener/add shadow decision must increase exposure")
        if float(snapshot.quote_price) < 0.0 or float(snapshot.quote_quantity) < 0.0:
            raise ValueError("baseline-shadow quote coordinates must be non-negative")

    def _validate_baseline_shadow_pair(
        self,
        control_shadow: BaselineShadowSnapshot,
        candidate_shadow: BaselineShadowSnapshot,
    ) -> None:
        self._validate_shadow_snapshot(control_shadow)
        self._validate_shadow_snapshot(candidate_shadow)
        if control_shadow != candidate_shadow:
            self._violate(
                "control_candidate_baseline_shadow_mismatch",
                "control and candidate untreated baseline-shadow snapshots differ",
            )

    def _validate_prediction(
        self,
        prediction: CanonicalPredictionBucket,
        *,
        shadow: BaselineShadowSnapshot,
    ) -> DailyPastOnlyThreshold:
        day = _validate_day(prediction.utc_day)
        if day != shadow.utc_day:
            raise AdapterContractViolation(
                "prediction and baseline-shadow UTC days differ"
            )
        bucket = int(prediction.prediction_bucket_ts_ms)
        ready = int(prediction.feature_ready_ts_ms)
        decision = int(prediction.decision_ts_ms)
        if bucket <= 0 or bucket % 10_000 != 0:
            raise ValueError("prediction bucket must be a positive 10-second boundary")
        if ready < bucket:
            raise ValueError("feature-ready time precedes prediction bucket")
        if ready > decision:
            raise AdapterContractViolation(
                "feature-ready time exceeds decision time"
            )
        if decision * 1_000_000 != int(shadow.decision_ts_ns):
            raise AdapterContractViolation(
                "prediction decision clock differs from baseline shadow"
            )
        score = float(prediction.score)
        if not math.isfinite(score) or not 0.0 <= score <= 1.0:
            raise ValueError("toxicity score must be finite and in [0, 1]")
        model_hash = _validate_sha256(prediction.model_sha256, "model SHA256")
        if model_hash != self.frozen_model_sha256:
            raise AdapterContractViolation("prediction model identity drifted")
        if self._last_bucket_ts_ms is not None and bucket < self._last_bucket_ts_ms:
            raise AdapterContractViolation("prediction bucket clock moved backward")
        signature = prediction.signature()
        previous = self._bucket_signatures.get(bucket)
        if previous is not None:
            relation = "duplicate" if previous == signature else "inconsistent duplicate"
            self._violate(
                "duplicate_or_inconsistent_prediction_bucket",
                f"{relation} canonical prediction bucket {bucket}",
            )
        self._bucket_signatures[bucket] = signature
        self._last_bucket_ts_ms = bucket
        threshold = self._thresholds.get(day)
        if threshold is None:
            raise AdapterContractViolation(
                f"no frozen past-only threshold registered for {day}"
            )
        return threshold

    def _prospective_id(self, snapshot: BaselineShadowSnapshot) -> str:
        identity = (
            f"{SCHEMA_VERSION}|{self.side}|{snapshot.decision_id}|"
            f"{int(snapshot.decision_ts_ns)}"
        ).encode("ascii")
        return f"{self.side.lower()}-{hashlib.sha256(identity).hexdigest()[:24]}"

    def _ensure_assignment(
        self,
        snapshot: BaselineShadowSnapshot,
        *,
        prospective_campaign_side_id: str | None,
        threshold: DailyPastOnlyThreshold,
    ) -> ProspectiveCampaignSideAssignment:
        requested_id = str(prospective_campaign_side_id or "").strip()
        if self._current_assignment is not None:
            current_id = self._current_assignment.prospective_campaign_side_id
            if requested_id and requested_id != current_id:
                self._violate(
                    "campaign_side_rerandomization",
                    "a second prospective campaign-side identity was introduced "
                    "before the first assignment ended",
                )
            return self._current_assignment
        prospective_id = requested_id or self._prospective_id(snapshot)
        if prospective_id in self._closed_assignment_ids:
            self._violate(
                "campaign_side_rerandomization",
                "a closed prospective campaign-side identity was randomized again",
            )
        self._assignment_sequence += 1
        assignment = deterministic_campaign_side_assignment(
            seed=self.random_seed,
            utc_day=snapshot.utc_day,
            side=self.side,
            campaign_opportunity_id=self._assignment_sequence,
            candidate_probability=self.candidate_probability,
        )
        prospective = ProspectiveCampaignSideAssignment(
            prospective_campaign_side_id=prospective_id,
            assignment_utc_day=snapshot.utc_day,
            assignment_ts_ns=int(snapshot.decision_ts_ns),
            first_opportunity_decision_id=str(snapshot.decision_id),
            assignment_sequence=self._assignment_sequence,
            assignment=assignment,
        )
        self._current_assignment = prospective
        self._runtime = RankedToxicityGuardRuntime(
            side=self.side,
            action=assignment.action,
            threshold=threshold.threshold,
        )
        self._append_journal(
            "prospective_campaign_side_assignment",
            snapshot.decision_ts_ns,
            decision_id=snapshot.decision_id,
            assignment_sequence=int(self._assignment_sequence),
            assignment_before_treatment=True,
            candidate_path_submit_count_at_assignment=int(
                self._candidate_path_submit_count
            ),
            candidate_path_fill_count_at_assignment=int(
                self._candidate_path_fill_event_count
            ),
        )
        return prospective

    def observe_prediction_decision(
        self,
        *,
        prediction: CanonicalPredictionBucket,
        control_shadow: BaselineShadowSnapshot,
        candidate_shadow: BaselineShadowSnapshot,
        candidate_active_exposure_order_id: str = "",
        prospective_campaign_side_id: str | None = None,
    ) -> GuardExecutionDirective:
        """Evaluate one canonical prediction against an untreated denominator."""

        self._validate_baseline_shadow_pair(control_shadow, candidate_shadow)
        threshold = self._validate_prediction(prediction, shadow=control_shadow)
        active_order_id = str(candidate_active_exposure_order_id or "").strip()
        if active_order_id:
            tracked = self._orders.get(active_order_id)
            if (
                tracked is None
                or not tracked.exposure_increasing
                or not tracked.lifecycle.fill_risk_active
            ):
                raise AdapterContractViolation(
                    "candidate active exposure order is not in the real fill-risk set"
                )

        eligible_exposure = bool(
            control_shadow.baseline_eligible
            and control_shadow.exposure_increasing
        )
        if self._current_assignment is None and eligible_exposure:
            self._ensure_assignment(
                control_shadow,
                prospective_campaign_side_id=prospective_campaign_side_id,
                threshold=threshold,
            )
        elif (
            self._current_assignment is not None
            and prospective_campaign_side_id
            and prospective_campaign_side_id
            != self._current_assignment.prospective_campaign_side_id
        ):
            self._violate(
                "campaign_side_rerandomization",
                "prospective campaign-side identity changed within an assignment",
            )

        if self._runtime is None or self._current_assignment is None:
            directive = GuardExecutionDirective(
                decision_id=control_shadow.decision_id,
                prospective_campaign_side_id="",
                action=CONTROL_ACTION,
                behavior_propensity=1.0,
                state=GuardState.BASELINE.value,
                threshold_utc_day=threshold.utc_day,
                threshold=threshold.threshold,
                request_cancel_once=False,
                cancel_order_id="",
                allow_exposure_submission=True,
                duplicate_bucket=False,
            )
            self._pending_directives[directive.decision_id] = directive
            self._append_journal(
                "prediction_decision_unassigned",
                control_shadow.decision_ts_ns,
                decision_id=control_shadow.decision_id,
                prediction_bucket_ts_ms=prediction.prediction_bucket_ts_ms,
                feature_ready_ts_ms=prediction.feature_ready_ts_ms,
                model_sha256=prediction.model_sha256,
                toxicity_score=prediction.score,
                threshold_utc_day=threshold.utc_day,
                threshold=threshold.threshold,
                baseline_shadow_eligible=bool(control_shadow.baseline_eligible),
                exposure_increasing=bool(control_shadow.exposure_increasing),
            )
            return directive

        self._runtime.threshold = float(threshold.threshold)
        transition: GuardTransition = self._runtime.on_completed_prediction(
            prediction_bucket_ts_ms=prediction.prediction_bucket_ts_ms,
            score=prediction.score,
            baseline_eligible=control_shadow.baseline_eligible,
            exposure_increasing=control_shadow.exposure_increasing,
            active_exposure_order=bool(active_order_id),
        )
        if transition.duplicate_bucket:
            self._violate(
                "duplicate_or_inconsistent_prediction_bucket",
                "v1 runtime observed a duplicate after adapter canonicalization",
            )
        treatment_event = bool(
            transition.activated
            or transition.request_cancel_once
            or transition.suppress_exposure_submission
        )
        if treatment_event:
            if self._current_assignment is None:
                self._violate(
                    "assignment_after_treatment",
                    "guard treatment began before prospective assignment",
                )
            self._treatment_event_count += 1

        directive = GuardExecutionDirective(
            decision_id=control_shadow.decision_id,
            prospective_campaign_side_id=(
                self._current_assignment.prospective_campaign_side_id
            ),
            action=self._current_assignment.assignment.action,
            behavior_propensity=float(
                self._current_assignment.assignment.behavior_propensity
            ),
            state=transition.state,
            threshold_utc_day=threshold.utc_day,
            threshold=threshold.threshold,
            request_cancel_once=bool(transition.request_cancel_once),
            cancel_order_id=(active_order_id if transition.request_cancel_once else ""),
            allow_exposure_submission=not bool(
                transition.suppress_exposure_submission
            ),
            duplicate_bucket=False,
        )
        self._pending_directives[directive.decision_id] = directive
        self._append_journal(
            "prediction_decision",
            control_shadow.decision_ts_ns,
            decision_id=control_shadow.decision_id,
            prediction_bucket_ts_ms=prediction.prediction_bucket_ts_ms,
            feature_ready_ts_ms=prediction.feature_ready_ts_ms,
            model_sha256=prediction.model_sha256,
            toxicity_score=prediction.score,
            threshold_utc_day=threshold.utc_day,
            threshold=threshold.threshold,
            baseline_shadow_eligible=bool(control_shadow.baseline_eligible),
            exposure_increasing=bool(control_shadow.exposure_increasing),
            role=control_shadow.role,
            active_exposure_order_id=active_order_id,
            prior_guard_state=transition.prior_state,
            resulting_guard_state=transition.state,
            guard_activated=bool(transition.activated),
            guard_released=bool(transition.released),
            request_cancel_once=bool(transition.request_cancel_once),
            allow_exposure_submission=bool(directive.allow_exposure_submission),
        )
        if transition.request_cancel_once:
            if not active_order_id:
                raise AdapterContractViolation(
                    "guard requested cancel without a candidate active order"
                )
            self.on_cancel_requested(
                order_id=active_order_id,
                visibility_ts_ns=control_shadow.decision_ts_ns,
                reason="ranked_toxicity_guard",
                guard_initiated=True,
            )
        return directive

    def observe_final_quote_action(
        self,
        *,
        decision_id: str,
        role: str,
        exposure_increasing: bool,
        baseline_action: str,
        candidate_action: str,
        baseline_price: float,
        candidate_price: float,
        baseline_quantity: float,
        candidate_quantity: float,
        event_ts_ns: int,
        baseline_order_id: str = "",
        candidate_order_id: str = "",
    ) -> bool:
        """Validate that the guard changes permission, never quote economics."""

        key = str(decision_id)
        directive = self._pending_directives.pop(key, None)
        if directive is None:
            raise AdapterContractViolation(
                "final quote action has no preceding canonical decision"
            )
        normalized_role = _normalize_role(role)
        increasing = bool(exposure_increasing)
        baseline_tuple = (
            str(baseline_action),
            float(baseline_price),
            float(baseline_quantity),
        )
        candidate_tuple = (
            str(candidate_action),
            float(candidate_price),
            float(candidate_quantity),
        )
        changed = baseline_tuple != candidate_tuple
        if normalized_role == "reducing" or not increasing:
            if changed:
                self._violate(
                    "reducing_quote_changes",
                    "ranked-toxicity guard changed a reducing quote",
                )
        elif directive.action == CONTROL_ACTION and changed:
            raise AdapterContractViolation("control assignment changed quote action")
        elif directive.action == CANDIDATE_ACTION:
            if directive.allow_exposure_submission:
                if changed:
                    raise AdapterContractViolation(
                        "guard changed price, quantity, or action while permission was open"
                    )
            else:
                allowed_suppressed_actions = {
                    "cancel",
                    "cancel_pending",
                    "pause",
                    "none",
                }
                if str(candidate_action) in {"keep", "keep_after_cancel_reject"}:
                    target = self._orders.get(str(candidate_order_id))
                    cancel_reject_keep = bool(
                        target is not None
                        and target.order_id == self._guard_target_order_id
                        and target.lifecycle.phase
                        in {
                            OrderLifecyclePhase.ACTIVE,
                            OrderLifecyclePhase.PARTIALLY_FILLED,
                        }
                        and target.lifecycle.fill_risk_active
                    )
                    if not cancel_reject_keep:
                        raise AdapterContractViolation(
                            "suppressed guard kept an order not restored by cancel reject"
                        )
                elif str(candidate_action) not in allowed_suppressed_actions:
                    raise AdapterContractViolation(
                        "suppressed exposure decision still posts or keeps an order"
                    )
        self._append_journal(
            "final_quote_action",
            event_ts_ns,
            decision_id=key,
            role=normalized_role,
            exposure_increasing=increasing,
            baseline_action=str(baseline_action),
            candidate_action=str(candidate_action),
            baseline_price=float(baseline_price),
            candidate_price=float(candidate_price),
            baseline_quantity=float(baseline_quantity),
            candidate_quantity=float(candidate_quantity),
            baseline_order_id=str(baseline_order_id),
            candidate_order_id=str(candidate_order_id),
            final_quote_action_changed=bool(changed),
        )
        return bool(changed)

    def on_order_submitted(
        self,
        *,
        order_id: str,
        initial_quantity: float,
        visibility_ts_ns: int,
        exposure_increasing: bool,
    ) -> None:
        key = str(order_id).strip()
        if not key or key in self._orders or key in self._terminal_order_ids:
            raise AdapterContractViolation("order identity is empty or reused")
        if bool(exposure_increasing) and self.suppresses_exposure:
            raise AdapterContractViolation(
                "candidate submitted an exposure order while guard suppressed permission"
            )
        self._orders[key] = _TrackedOrder(
            order_id=key,
            exposure_increasing=bool(exposure_increasing),
            lifecycle=QuantityWeightedOrderLifecycle(
                initial_quantity=float(initial_quantity),
                submitted_ts_ns=int(visibility_ts_ns),
            ),
        )
        self._candidate_path_submit_count += 1
        self._append_journal(
            "order_submitted",
            visibility_ts_ns,
            order_id=key,
            initial_quantity=float(initial_quantity),
            exposure_increasing=bool(exposure_increasing),
        )

    def on_order_activated(
        self,
        *,
        order_id: str,
        visibility_ts_ns: int,
        exchange_ts_ns: int,
    ) -> None:
        tracked = self._require_order(order_id)
        tracked.lifecycle.activate(
            visibility_ts_ns,
            exchange_ts_ns=exchange_ts_ns,
        )
        if tracked.exposure_increasing:
            tracked.hazard_attached = True
            tracked.cursor_attached = True
        self._append_journal(
            "order_activated",
            visibility_ts_ns,
            order_id=tracked.order_id,
            exchange_ts_ns=int(exchange_ts_ns),
            lifecycle_phase=tracked.lifecycle.phase.value,
            hazard_attached=bool(tracked.hazard_attached),
            cursor_attached=bool(tracked.cursor_attached),
        )

    def on_cancel_requested(
        self,
        *,
        order_id: str,
        visibility_ts_ns: int,
        reason: str,
        guard_initiated: bool = False,
    ) -> None:
        tracked = self._require_order(order_id)
        if tracked.lifecycle.phase not in {
            OrderLifecyclePhase.ACTIVE,
            OrderLifecyclePhase.PARTIALLY_FILLED,
            OrderLifecyclePhase.CANCEL_PENDING,
        }:
            raise AdapterContractViolation(
                "cancel request observed outside an active order risk set"
            )
        tracked.lifecycle.request_cancel(visibility_ts_ns)
        if guard_initiated:
            if self._runtime is None or self._runtime.state != GuardState.CANCEL_PENDING:
                raise AdapterContractViolation(
                    "guard cancel dispatch observed outside guard CANCEL_PENDING"
                )
            if not tracked.exposure_increasing:
                raise AdapterContractViolation(
                    "ranked-toxicity guard cannot cancel a reducing order"
                )
            self._guard_target_order_id = tracked.order_id
        self._append_journal(
            "cancel_requested",
            visibility_ts_ns,
            order_id=tracked.order_id,
            cancel_reason=str(reason),
            guard_initiated=bool(guard_initiated),
            lifecycle_phase=tracked.lifecycle.phase.value,
            remaining_quantity=float(tracked.lifecycle.remaining_quantity),
        )

    def on_cancel_rejected(
        self,
        *,
        order_id: str,
        visibility_ts_ns: int,
        exchange_ts_ns: int = 0,
    ) -> GuardState:
        tracked = self._require_order(order_id)
        tracked.lifecycle.cancel_rejected(
            visibility_ts_ns,
            exchange_ts_ns=exchange_ts_ns,
        )
        if tracked.order_id == self._guard_target_order_id:
            if self._runtime is None or self._runtime.state != GuardState.CANCEL_PENDING:
                raise AdapterContractViolation(
                    "guard cancel reject observed outside CANCEL_PENDING"
                )
            if self._runtime.release_waiting_for_cancel_ack:
                self._runtime.state = GuardState.RELEASED
                self._runtime.release_waiting_for_cancel_ack = False
                self._guard_target_order_id = ""
            else:
                self._runtime.state = GuardState.GUARD_ACTIVE
        self._append_journal(
            "cancel_rejected",
            visibility_ts_ns,
            order_id=tracked.order_id,
            exchange_ts_ns=int(exchange_ts_ns),
            lifecycle_phase=tracked.lifecycle.phase.value,
            remaining_quantity=float(tracked.lifecycle.remaining_quantity),
            old_order_fill_risk_active=bool(tracked.lifecycle.fill_risk_active),
            hazard_attached=bool(tracked.hazard_attached),
            cursor_attached=bool(tracked.cursor_attached),
        )
        return self.state

    def on_order_fill(
        self,
        *,
        order_id: str,
        remaining_after: float,
        visibility_ts_ns: int,
        exchange_ts_ns: int = 0,
        full_fill: bool = False,
    ) -> GuardState:
        tracked = self._require_order(order_id)
        phase_before = tracked.lifecycle.phase
        tracked.lifecycle.observe_fill(
            remaining_after=remaining_after,
            visibility_ts_ns=visibility_ts_ns,
            exchange_ts_ns=exchange_ts_ns,
            full_fill=full_fill,
        )
        self._candidate_path_fill_event_count += 1
        if tracked.lifecycle.phase == OrderLifecyclePhase.EXCHANGE_TERMINAL:
            return self._retire_exchange_terminal(
                tracked,
                event_ts_ns=visibility_ts_ns,
                reason="full_fill",
                exchange_ts_ns=exchange_ts_ns,
                lifecycle_already_terminal=True,
                phase_before=phase_before,
            )
        self._append_journal(
            "partial_fill",
            visibility_ts_ns,
            order_id=tracked.order_id,
            exchange_ts_ns=int(exchange_ts_ns),
            lifecycle_phase_before=phase_before.value,
            lifecycle_phase_after=tracked.lifecycle.phase.value,
            remaining_quantity=float(tracked.lifecycle.remaining_quantity),
            pending_cancel_preserved=(
                phase_before == OrderLifecyclePhase.CANCEL_PENDING
                and tracked.lifecycle.phase == OrderLifecyclePhase.CANCEL_PENDING
            ),
            old_order_fill_risk_active=bool(tracked.lifecycle.fill_risk_active),
        )
        return self.state

    def on_exchange_terminal(
        self,
        *,
        order_id: str,
        reason: str,
        visibility_ts_ns: int,
        exchange_ts_ns: int = 0,
    ) -> GuardState:
        tracked = self._require_order(order_id)
        if str(reason).strip().lower() in {"full_fill", "filled_before_cancel_ack"}:
            raise AdapterContractViolation(
                "full-fill terminality must use on_order_fill so remaining quantity "
                "is updated before risk-set retirement"
            )
        phase_before = tracked.lifecycle.phase
        tracked.lifecycle.exchange_terminal(
            visibility_ts_ns,
            reason=reason,
            exchange_ts_ns=exchange_ts_ns,
        )
        return self._retire_exchange_terminal(
            tracked,
            event_ts_ns=visibility_ts_ns,
            reason=reason,
            exchange_ts_ns=exchange_ts_ns,
            lifecycle_already_terminal=True,
            phase_before=phase_before,
        )

    def _retire_exchange_terminal(
        self,
        tracked: _TrackedOrder,
        *,
        event_ts_ns: int,
        reason: str,
        exchange_ts_ns: int,
        lifecycle_already_terminal: bool,
        phase_before: OrderLifecyclePhase,
    ) -> GuardState:
        if not lifecycle_already_terminal:
            raise AssertionError("terminal retirement requires terminal lifecycle")
        if tracked.lifecycle.phase != OrderLifecyclePhase.EXCHANGE_TERMINAL:
            raise AdapterContractViolation("order did not reach EXCHANGE_TERMINAL")
        tracked.hazard_attached = False
        tracked.cursor_attached = False
        self._terminal_order_ids.add(tracked.order_id)
        if tracked.order_id == self._guard_target_order_id:
            if self._runtime is None or self._runtime.state not in {
                GuardState.CANCEL_PENDING,
                GuardState.GUARD_ACTIVE,
            }:
                raise AdapterContractViolation(
                    "guard target reached terminal outside an active guard state"
                )
            if self._runtime.release_waiting_for_cancel_ack:
                self._runtime.state = GuardState.RELEASED
            else:
                self._runtime.state = GuardState.SUPPRESSING
            self._runtime.release_waiting_for_cancel_ack = False
            self._guard_target_order_id = ""
        self._append_journal(
            "exchange_terminal",
            event_ts_ns,
            order_id=tracked.order_id,
            exchange_ts_ns=int(exchange_ts_ns),
            terminal_reason=str(reason),
            terminal_policy_route=terminal_policy_route(
                reason,
                tracked.lifecycle.remaining_quantity,
            ).value,
            lifecycle_phase_before=phase_before.value,
            lifecycle_phase_after=tracked.lifecycle.phase.value,
            remaining_quantity=float(tracked.lifecycle.remaining_quantity),
            old_order_fill_risk_active=bool(tracked.lifecycle.fill_risk_active),
            hazard_attached=False,
            cursor_attached=False,
        )
        return self.state

    def observe_active_order_hazard(
        self,
        *,
        order_id: str,
        event_ts_ns: int,
    ) -> None:
        tracked = self._orders.get(str(order_id))
        if (
            tracked is None
            or tracked.order_id in self._terminal_order_ids
            or not tracked.lifecycle.fill_risk_active
            or not tracked.hazard_attached
        ):
            self._violate(
                "post_terminal_hazard_or_cursor_reuse",
                "active-order hazard evaluated outside the real fill-risk set",
            )
        self._append_journal(
            "active_order_hazard_evaluation",
            event_ts_ns,
            order_id=tracked.order_id,
            lifecycle_phase=tracked.lifecycle.phase.value,
        )

    def observe_active_depth_cursor(
        self,
        *,
        order_id: str,
        event_ts_ns: int,
    ) -> None:
        tracked = self._orders.get(str(order_id))
        if (
            tracked is None
            or tracked.order_id in self._terminal_order_ids
            or not tracked.lifecycle.fill_risk_active
            or not tracked.cursor_attached
        ):
            self._violate(
                "post_terminal_hazard_or_cursor_reuse",
                "active-order depth cursor used outside the real fill-risk set",
            )
        self._append_journal(
            "active_depth_cursor_observation",
            event_ts_ns,
            order_id=tracked.order_id,
            lifecycle_phase=tracked.lifecycle.phase.value,
        )

    def _require_order(self, order_id: str) -> _TrackedOrder:
        key = str(order_id).strip()
        tracked = self._orders.get(key)
        if tracked is None:
            raise AdapterContractViolation(f"unknown candidate order: {key}")
        if key in self._terminal_order_ids:
            raise AdapterContractViolation(f"candidate order already terminal: {key}")
        return tracked

    def end_prospective_campaign_side(
        self,
        *,
        prospective_campaign_side_id: str,
        event_ts_ns: int,
        reason: str,
    ) -> None:
        assignment = self._current_assignment
        requested_id = str(prospective_campaign_side_id).strip()
        if assignment is None or requested_id != assignment.prospective_campaign_side_id:
            self._violate(
                "campaign_side_rerandomization",
                "campaign-side terminal does not match the active prospective assignment",
            )
        if self._guard_target_order_id:
            raise AdapterContractViolation(
                "cannot end prospective campaign while guard target is nonterminal"
            )
        if self._pending_directives:
            raise AdapterContractViolation(
                "cannot end prospective campaign with unobserved final quote actions"
            )
        active_exposure_orders = [
            order.order_id
            for order in self._orders.values()
            if order.exposure_increasing and order.lifecycle.fill_risk_active
        ]
        if active_exposure_orders:
            raise AdapterContractViolation(
                "cannot end prospective campaign with active exposure orders: "
                f"{active_exposure_orders}"
            )
        self._append_journal(
            "prospective_campaign_side_terminal",
            event_ts_ns,
            terminal_reason=str(reason),
            assignment_retained_without_submit_or_fill=True,
        )
        self._closed_assignment_ids.add(assignment.prospective_campaign_side_id)
        if self._runtime is not None:
            self._runtime.reset_campaign_side()
        self._current_assignment = None
        self._runtime = None
        self._guard_target_order_id = ""

    def contract_audit(self) -> dict[str, Any]:
        lingering_terminal_risk = sum(
            int(order.hazard_attached or order.cursor_attached)
            for order_id, order in self._orders.items()
            if order_id in self._terminal_order_ids
        )
        counts = dict(self._violations)
        counts["post_terminal_hazard_or_cursor_reuse"] += int(
            lingering_terminal_risk
        )
        completeness = {
            "pending_final_quote_directives": int(len(self._pending_directives)),
            "nonterminal_guard_target": int(bool(self._guard_target_order_id)),
        }
        return {
            "schema_version": f"{SCHEMA_VERSION}.contract_audit",
            "side": self.side,
            "journal_events": int(len(self._journal)),
            "assignment_count": int(self._assignment_sequence),
            "closed_assignment_count": int(len(self._closed_assignment_ids)),
            "treatment_event_count": int(self._treatment_event_count),
            "terminal_order_count": int(len(self._terminal_order_ids)),
            "zero_tolerance_counts": counts,
            "zero_tolerance_passed": all(value == 0 for value in counts.values()),
            "execution_completeness": completeness,
            "execution_complete": all(
                value == 0 for value in completeness.values()
            ),
            "economic_outcome_columns_read": [],
            "mechanics_results_read": False,
            "development_economic_outcome_read": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_experiment_authorized": False,
            "live_deployment_authorized": False,
        }

    def assert_zero_tolerance(self) -> dict[str, Any]:
        audit = self.contract_audit()
        if not audit["zero_tolerance_passed"]:
            raise AdapterContractViolation(
                "full-path adapter zero-tolerance audit failed: "
                f"{audit['zero_tolerance_counts']}"
            )
        return audit

    def assert_execution_complete(self) -> dict[str, Any]:
        audit = self.assert_zero_tolerance()
        if not audit["execution_complete"]:
            raise AdapterContractViolation(
                "full-path adapter journal is incomplete: "
                f"{audit['execution_completeness']}"
            )
        return audit

    def journal(self) -> tuple[dict[str, Any], ...]:
        return tuple(dict(row) for row in self._journal)

    def order_snapshot(self, order_id: str) -> dict[str, Any]:
        tracked = self._orders.get(str(order_id))
        if tracked is None:
            raise KeyError(order_id)
        payload = tracked.lifecycle.snapshot()
        payload.update(
            {
                "order_id": tracked.order_id,
                "exposure_increasing": bool(tracked.exposure_increasing),
                "hazard_attached": bool(tracked.hazard_attached),
                "cursor_attached": bool(tracked.cursor_attached),
            }
        )
        return payload


def lifecycle_branch_contract() -> dict[str, Any]:
    """Machine-readable routing matrix frozen by the v1.1 amendment."""

    return {
        "schema_version": SCHEMA_VERSION,
        "cancel_reject": {
            "exchange_phase": "ACTIVE_or_PARTIALLY_FILLED",
            "fill_risk_set_ends": False,
            "queue_age_hazard_retained": True,
        },
        "cancel_pending_partial_fill": {
            "exchange_phase": OrderLifecyclePhase.CANCEL_PENDING.value,
            "remaining_quantity_updated": True,
            "fill_risk_set_ends": False,
        },
        "exchange_terminal": {
            "reasons": [
                "cancel_ack",
                "full_fill",
                "expired",
                "rejected",
                "shutdown",
            ],
            "old_queue_age_hazard_cursor_cleared": True,
            "recovered_while_waiting": GuardState.RELEASED.value,
            "not_recovered_while_waiting": GuardState.SUPPRESSING.value,
        },
        "fill_risk_phases": sorted(phase.value for phase in FILL_RISK_PHASES),
    }
