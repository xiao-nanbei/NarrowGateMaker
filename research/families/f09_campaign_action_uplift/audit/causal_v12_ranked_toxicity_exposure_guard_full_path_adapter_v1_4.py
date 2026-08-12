#!/usr/bin/env python3
"""v1.4 execution binding for the frozen ranked-toxicity guard action.

The v1.1 implementation remains byte-for-byte frozen. This successor separates
the 10-second prediction clock from quote decisions, derives randomization from
the stable prospective campaign-side identity, fails closed on unknown terminal
routes, and supports bounded-memory mechanics journals.

No reward, markout, PnL, or promotion logic belongs in this module.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping
from typing import Any

from execution.chunked_parquet_journal import ChunkedParquetJournalWriter
from execution.order_lifecycle import (
    OrderLifecyclePhase,
    TerminalPolicyRoute,
    terminal_policy_route,
)
from research.families.f09_campaign_action_uplift.audit.causal_v12_ranked_toxicity_exposure_guard import (
    CANDIDATE_ACTION,
    CONTROL_ACTION,
    GuardState,
    GuardTransition,
    RankedToxicityGuardRuntime,
    deterministic_campaign_side_assignment,
)
from research.families.f09_campaign_action_uplift.audit.causal_v12_ranked_toxicity_exposure_guard_full_path_adapter import (
    ZERO_TOLERANCE_KEYS,
    AdapterContractViolation,
    BaselineShadowSnapshot,
    CanonicalPredictionBucket,
    DailyPastOnlyThreshold,
    GuardExecutionDirective,
    ProspectiveCampaignSideAssignment,
    RankedToxicityGuardFullPathAdapterV11,
    _normalize_role,
    _validate_day,
    _validate_sha256,
)

SCHEMA_VERSION = "causal_v12_ranked_toxicity_exposure_guard_full_path_adapter.v1.4"
SUPPORTED_NONFILL_TERMINAL_REASONS = frozenset(
    {"cancel_ack", "expired", "rejected", "shutdown"}
)


def stable_campaign_opportunity_id(
    *,
    side: str,
    prospective_campaign_side_id: str,
) -> int:
    """Map a stable lineage identity to the frozen integer PRF ABI."""

    normalized_id = str(prospective_campaign_side_id).strip()
    if not normalized_id or normalized_id.lower() == "nan":
        raise ValueError("prospective_campaign_side_id must be stable and non-empty")
    identity = f"{SCHEMA_VERSION}|{str(side).upper()}|{normalized_id}".encode()
    value = int.from_bytes(hashlib.sha256(identity).digest()[:8], "big")
    value &= (1 << 63) - 1
    return value if value > 0 else 1


class RankedToxicityGuardFullPathAdapterV14(
    RankedToxicityGuardFullPathAdapterV11
):
    """Execution-only successor with independent prediction and decision clocks."""

    def __init__(
        self,
        *,
        side: str,
        random_seed: int,
        frozen_model_sha256: str,
        candidate_probability: float = 0.5,
        journal_writer: ChunkedParquetJournalWriter | None = None,
        retain_journal: bool = True,
    ) -> None:
        super().__init__(
            side=side,
            random_seed=random_seed,
            frozen_model_sha256=frozen_model_sha256,
            candidate_probability=candidate_probability,
        )
        self._journal_writer = journal_writer
        self._retain_journal = bool(retain_journal)
        if not self._retain_journal:
            self._journal.clear()
        self._journal_event_count = 0
        self._held_prediction: CanonicalPredictionBucket | None = None
        self._held_threshold: DailyPastOnlyThreshold | None = None
        self._runtime_applied_bucket_ts_ms: int | None = None
        self._quote_decision_count = 0
        self._held_prediction_reuse_count = 0
        self._held_bucket_quote_decision_count = 0
        self._stable_assignment_ids: dict[str, int] = {}
        self._pre_activation_cancel_requests: dict[str, tuple[int, str]] = {}

    def _append_journal(
        self,
        event_type: str,
        event_ts_ns: int,
        **payload: Any,
    ) -> None:
        assignment = self._current_assignment
        runtime = self._runtime
        self._journal_event_count += 1
        row: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "sequence": int(self._journal_event_count),
            "event_type": str(event_type),
            "event_ts_ns": int(event_ts_ns),
            "side": self.side,
            "decision_id": str(payload.get("decision_id", "")),
            "prospective_campaign_side_id": (
                assignment.prospective_campaign_side_id if assignment else ""
            ),
            "assignment_utc_day": (
                assignment.assignment_utc_day if assignment else ""
            ),
            "action": assignment.assignment.action if assignment else "unassigned",
            "behavior_propensity": (
                float(assignment.assignment.behavior_propensity) if assignment else 0.0
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
        if self._retain_journal:
            self._journal.append(row)
        if self._journal_writer is not None:
            self._journal_writer.append(row)

    def on_prediction_bucket(
        self,
        prediction: CanonicalPredictionBucket,
    ) -> DailyPastOnlyThreshold:
        """Advance the held model output exactly once per completed 10s bucket."""

        day = _validate_day(prediction.utc_day)
        bucket = int(prediction.prediction_bucket_ts_ms)
        ready = int(prediction.feature_ready_ts_ms)
        observed = int(prediction.decision_ts_ms)
        if bucket <= 0 or bucket % 10_000 != 0:
            raise ValueError("prediction bucket must be a positive 10-second boundary")
        if ready < bucket:
            raise ValueError("feature-ready time precedes prediction bucket")
        if ready > observed:
            raise AdapterContractViolation("feature-ready time exceeds observation time")
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
        threshold = self._thresholds.get(day)
        if threshold is None:
            raise AdapterContractViolation(
                f"no frozen past-only threshold registered for {day}"
            )
        self._bucket_signatures[bucket] = signature
        self._last_bucket_ts_ms = bucket
        self._held_prediction = prediction
        self._held_threshold = threshold
        self._runtime_applied_bucket_ts_ms = None
        self._held_bucket_quote_decision_count = 0
        self._append_journal(
            "prediction_bucket",
            observed * 1_000_000,
            prediction_bucket_ts_ms=bucket,
            feature_ready_ts_ms=ready,
            prediction_observed_ts_ms=observed,
            model_sha256=model_hash,
            toxicity_score=score,
            threshold_utc_day=threshold.utc_day,
            threshold=threshold.threshold,
        )

        # Recovery is a prediction-clock transition and must not wait for a
        # later eligible quote decision. A new high bucket while already
        # suppressing is also consumed here without creating another episode.
        if self._runtime is not None and self._runtime.suppresses_exposure:
            self._runtime.threshold = float(threshold.threshold)
            transition = self._runtime.on_completed_prediction(
                prediction_bucket_ts_ms=bucket,
                score=score,
                baseline_eligible=False,
                exposure_increasing=False,
                active_exposure_order=bool(self._guard_target_order_id),
            )
            if transition.duplicate_bucket:
                self._violate(
                    "duplicate_or_inconsistent_prediction_bucket",
                    "runtime consumed a duplicate after adapter canonicalization",
                )
            self._runtime_applied_bucket_ts_ms = bucket
            self._append_transition("prediction_clock_transition", transition, observed)
        return threshold

    def _append_transition(
        self,
        event_type: str,
        transition: GuardTransition,
        event_ts_ms: int,
        **payload: Any,
    ) -> None:
        self._append_journal(
            event_type,
            int(event_ts_ms) * 1_000_000,
            **transition.to_payload(),
            **payload,
        )

    def _ensure_assignment(
        self,
        snapshot: BaselineShadowSnapshot,
        *,
        prospective_campaign_side_id: str | None,
        threshold: DailyPastOnlyThreshold,
    ) -> ProspectiveCampaignSideAssignment:
        requested_id = str(prospective_campaign_side_id or "").strip()
        if not requested_id or requested_id.lower() == "nan":
            raise AdapterContractViolation(
                "authoritative assignment requires a stable prospective_campaign_side_id"
            )
        if self._current_assignment is not None:
            if requested_id != self._current_assignment.prospective_campaign_side_id:
                self._violate(
                    "campaign_side_rerandomization",
                    "prospective campaign-side identity changed within an assignment",
                )
            return self._current_assignment
        if requested_id in self._closed_assignment_ids:
            self._violate(
                "campaign_side_rerandomization",
                "a closed prospective campaign-side identity was randomized again",
            )
        stable_integer = stable_campaign_opportunity_id(
            side=self.side,
            prospective_campaign_side_id=requested_id,
        )
        previous_integer = self._stable_assignment_ids.get(requested_id)
        if previous_integer is not None and previous_integer != stable_integer:
            self._violate(
                "campaign_side_rerandomization",
                "stable prospective identity changed its PRF integer",
            )
        self._stable_assignment_ids[requested_id] = stable_integer
        assignment = deterministic_campaign_side_assignment(
            seed=self.random_seed,
            utc_day=snapshot.utc_day,
            side=self.side,
            campaign_opportunity_id=stable_integer,
            candidate_probability=self.candidate_probability,
        )
        self._assignment_sequence += 1
        prospective = ProspectiveCampaignSideAssignment(
            prospective_campaign_side_id=requested_id,
            assignment_utc_day=snapshot.utc_day,
            assignment_ts_ns=int(snapshot.decision_ts_ns),
            first_opportunity_decision_id=str(snapshot.decision_id),
            assignment_sequence=int(self._assignment_sequence),
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
            stable_campaign_opportunity_id=int(stable_integer),
            assignment_before_treatment=True,
            candidate_path_submit_count_at_assignment=int(
                self._candidate_path_submit_count
            ),
            candidate_path_fill_count_at_assignment=int(
                self._candidate_path_fill_event_count
            ),
        )
        return prospective

    def on_quote_decision(
        self,
        *,
        control_shadow: BaselineShadowSnapshot,
        candidate_shadow: BaselineShadowSnapshot,
        candidate_active_exposure_order_id: str = "",
        prospective_campaign_side_id: str | None = None,
    ) -> GuardExecutionDirective:
        """Apply the held score to every quote decision in the bucket."""

        immutable_fields = (
            "decision_id",
            "utc_day",
            "decision_ts_ns",
            "side",
            "policy_fingerprint",
        )
        if any(
            getattr(control_shadow, field) != getattr(candidate_shadow, field)
            for field in immutable_fields
        ):
            self._violate(
                "control_candidate_baseline_shadow_mismatch",
                "untreated denominator and candidate decision identity differ",
            )
        if self._held_prediction is None or self._held_threshold is None:
            raise AdapterContractViolation(
                "quote decision arrived before a canonical held prediction"
            )
        prediction = self._held_prediction
        threshold = self._held_threshold
        if int(control_shadow.decision_ts_ns) < int(prediction.feature_ready_ts_ms) * 1_000_000:
            raise AdapterContractViolation("quote decision precedes feature-ready time")
        if self._held_bucket_quote_decision_count > 0:
            self._held_prediction_reuse_count += 1
        self._held_bucket_quote_decision_count += 1
        decision_id = str(control_shadow.decision_id)
        if decision_id in self._pending_directives:
            raise AdapterContractViolation("quote decision_id was reused before final action")
        active_order_id = str(candidate_active_exposure_order_id or "").strip()
        if active_order_id != str(
            candidate_shadow.active_exposure_order_id or ""
        ).strip():
            raise AdapterContractViolation(
                "candidate active exposure order identity is inconsistent"
            )
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
            control_shadow.baseline_eligible and control_shadow.exposure_increasing
        )
        if self._current_assignment is None and eligible_exposure:
            self._ensure_assignment(
                control_shadow,
                prospective_campaign_side_id=prospective_campaign_side_id,
                threshold=threshold,
            )
        elif self._current_assignment is not None:
            requested_id = str(prospective_campaign_side_id or "").strip()
            if requested_id != self._current_assignment.prospective_campaign_side_id:
                self._violate(
                    "campaign_side_rerandomization",
                    "prospective campaign-side identity changed within an assignment",
                )

        transition: GuardTransition | None = None
        if self._runtime is not None and self._current_assignment is not None:
            self._runtime.threshold = float(threshold.threshold)
            if self._runtime_applied_bucket_ts_ms != int(
                prediction.prediction_bucket_ts_ms
            ):
                transition = self._runtime.on_completed_prediction(
                    prediction_bucket_ts_ms=prediction.prediction_bucket_ts_ms,
                    score=prediction.score,
                    baseline_eligible=control_shadow.baseline_eligible,
                    exposure_increasing=control_shadow.exposure_increasing,
                    active_exposure_order=bool(active_order_id),
                )
                if transition.duplicate_bucket:
                    self._violate(
                        "duplicate_or_inconsistent_prediction_bucket",
                        "runtime observed duplicate held prediction",
                    )
                self._runtime_applied_bucket_ts_ms = int(
                    prediction.prediction_bucket_ts_ms
                )
                if (
                    transition.activated
                    or transition.request_cancel_once
                    or transition.suppress_exposure_submission
                ):
                    self._treatment_event_count += 1
        assignment = self._current_assignment
        runtime = self._runtime
        request_cancel = bool(transition and transition.request_cancel_once)
        directive = GuardExecutionDirective(
            decision_id=decision_id,
            prospective_campaign_side_id=(
                assignment.prospective_campaign_side_id if assignment else ""
            ),
            action=assignment.assignment.action if assignment else CONTROL_ACTION,
            behavior_propensity=(
                float(assignment.assignment.behavior_propensity) if assignment else 1.0
            ),
            state=runtime.state.value if runtime else GuardState.BASELINE.value,
            threshold_utc_day=threshold.utc_day,
            threshold=float(threshold.threshold),
            request_cancel_once=request_cancel,
            cancel_order_id=active_order_id if request_cancel else "",
            allow_exposure_submission=(
                runtime.permission(
                    exposure_increasing=candidate_shadow.exposure_increasing
                )
                if runtime
                else True
            ),
            duplicate_bucket=False,
        )
        self._quote_decision_count += 1
        self._pending_directives[decision_id] = directive
        self._append_journal(
            "quote_decision",
            control_shadow.decision_ts_ns,
            decision_id=decision_id,
            prediction_bucket_ts_ms=int(prediction.prediction_bucket_ts_ms),
            feature_ready_ts_ms=int(prediction.feature_ready_ts_ms),
            held_prediction_reused=bool(transition is None),
            toxicity_score=float(prediction.score),
            threshold_utc_day=threshold.utc_day,
            threshold=float(threshold.threshold),
            baseline_shadow_eligible=bool(control_shadow.baseline_eligible),
            baseline_shadow_exposure_increasing=bool(
                control_shadow.exposure_increasing
            ),
            baseline_shadow_role=control_shadow.role,
            candidate_path_eligible=bool(candidate_shadow.baseline_eligible),
            candidate_path_exposure_increasing=bool(
                candidate_shadow.exposure_increasing
            ),
            candidate_path_role=candidate_shadow.role,
            active_exposure_order_id=active_order_id,
            request_cancel_once=request_cancel,
            allow_exposure_submission=bool(directive.allow_exposure_submission),
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
        key = str(decision_id)
        directive = self._pending_directives.pop(key, None)
        if directive is None:
            raise AdapterContractViolation(
                "final quote action has no preceding held-score decision"
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
            if directive.allow_exposure_submission and changed:
                raise AdapterContractViolation(
                    "guard changed price, quantity, or action while permission was open"
                )
            if not directive.allow_exposure_submission:
                allowed = {
                    "cancel",
                    "cancel_first",
                    "cancel_pending",
                    "pending_coalesce",
                    "pause",
                    "none",
                }
                if str(candidate_action) not in allowed:
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

    def on_cancel_requested(
        self,
        *,
        order_id: str,
        visibility_ts_ns: int,
        reason: str,
        guard_initiated: bool = False,
    ) -> None:
        tracked = self._require_order(order_id)
        if tracked.lifecycle.phase != OrderLifecyclePhase.SUBMITTED:
            super().on_cancel_requested(
                order_id=order_id,
                visibility_ts_ns=visibility_ts_ns,
                reason=reason,
                guard_initiated=guard_initiated,
            )
            return
        if guard_initiated:
            raise AdapterContractViolation(
                "guard cancel target must already be in the exchange fill-risk set"
            )
        key = tracked.order_id
        if key in self._pre_activation_cancel_requests:
            raise AdapterContractViolation(
                "pre-activation cancel request was observed more than once"
            )
        self._pre_activation_cancel_requests[key] = (
            int(visibility_ts_ns),
            str(reason),
        )
        self._append_journal(
            "cancel_requested_before_activation",
            visibility_ts_ns,
            order_id=key,
            cancel_reason=str(reason),
            guard_initiated=False,
            lifecycle_phase=tracked.lifecycle.phase.value,
            lifecycle_transition_deferred_until_activation=True,
            remaining_quantity=float(tracked.lifecycle.remaining_quantity),
        )

    def on_order_activated(
        self,
        *,
        order_id: str,
        visibility_ts_ns: int,
        exchange_ts_ns: int,
    ) -> None:
        super().on_order_activated(
            order_id=order_id,
            visibility_ts_ns=visibility_ts_ns,
            exchange_ts_ns=exchange_ts_ns,
        )
        deferred = self._pre_activation_cancel_requests.pop(str(order_id), None)
        if deferred is not None:
            request_ts_ns, reason = deferred
            super().on_cancel_requested(
                order_id=order_id,
                visibility_ts_ns=max(int(visibility_ts_ns), int(request_ts_ns)),
                reason=reason,
                guard_initiated=False,
            )

    def on_exchange_terminal(
        self,
        *,
        order_id: str,
        reason: str,
        visibility_ts_ns: int,
        exchange_ts_ns: int = 0,
    ) -> GuardState:
        normalized = str(reason).strip().lower()
        if normalized in {"full_fill", "filled_before_cancel_ack"}:
            raise AdapterContractViolation(
                "full-fill terminality must use on_order_fill"
            )
        if normalized not in SUPPORTED_NONFILL_TERMINAL_REASONS:
            raise AdapterContractViolation(
                f"unsupported exchange terminal reason: {normalized or '<empty>'}"
            )
        tracked = self._require_order(order_id)
        self._pre_activation_cancel_requests.pop(tracked.order_id, None)
        route = terminal_policy_route(normalized, tracked.lifecycle.remaining_quantity)
        if route == TerminalPolicyRoute.UNSUPPORTED:
            raise AdapterContractViolation(
                f"terminal reason has no frozen policy route: {normalized}"
            )
        return super().on_exchange_terminal(
            order_id=order_id,
            reason=normalized,
            visibility_ts_ns=visibility_ts_ns,
            exchange_ts_ns=exchange_ts_ns,
        )

    def contract_audit(self) -> dict[str, Any]:
        audit = super().contract_audit()
        audit.update(
            {
                "schema_version": f"{SCHEMA_VERSION}.contract_audit",
                "journal_events": int(self._journal_event_count),
                "journal_rows_retained_in_memory": int(len(self._journal)),
                "journal_streaming_enabled": self._journal_writer is not None,
                "quote_decision_count": int(self._quote_decision_count),
                "held_prediction_reuse_count": int(
                    self._held_prediction_reuse_count
                ),
                "stable_assignment_count": int(len(self._stable_assignment_ids)),
                "deferred_pre_activation_cancel_count": int(
                    len(self._pre_activation_cancel_requests)
                ),
                "prediction_decision_interfaces_separated": True,
            }
        )
        audit["execution_completeness"][
            "deferred_pre_activation_cancel_requests"
        ] = int(len(self._pre_activation_cancel_requests))
        audit["execution_complete"] = all(
            value == 0 for value in audit["execution_completeness"].values()
        )
        return audit

    def close_journal(self) -> Mapping[str, Any] | None:
        if self._journal_writer is None:
            return None
        return self._journal_writer.close()

    def journal(self) -> tuple[dict[str, Any], ...]:
        if not self._retain_journal:
            raise RuntimeError("journal rows are streamed and not retained in memory")
        return tuple(dict(row) for row in self._journal)


def execution_binding_contract_v1_4() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "prediction_interface": "on_prediction_bucket_once_per_completed_10s_bucket",
        "decision_interface": "on_quote_decision_for_every_authoritative_quote_loop",
        "held_score_reuse_within_bucket": True,
        "baseline_assignment_candidate_permission_separated": True,
        "duplicate_prediction_bucket_fail_fast": True,
        "assignment_prf_input": "stable_prospective_campaign_side_id_sha256_integer",
        "unknown_terminal_reason_fail_fast": True,
        "journal": {
            "format": "atomic_chunked_parquet",
            "bounded_memory": True,
            "formal_replay_retains_full_journal_in_memory": False,
        },
        "zero_tolerance_keys": list(ZERO_TOLERANCE_KEYS),
        "mechanics_results_read": False,
        "economic_outcomes_read": False,
        "permissions": {
            "action_experiment_authorized": False,
            "live_deployment_authorized": False,
        },
    }
