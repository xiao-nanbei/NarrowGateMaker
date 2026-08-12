#!/usr/bin/env python3
"""Carryover-safe assignment episodes for the ranked-toxicity guard.

Inventory campaign terminality is not an assignment boundary when an order or
guard state created under the old arm remains live.  This successor keeps the
arm fixed across untreated campaign lineages until a later untreated boundary
at which every owned order and policy state has naturally washed out.

This module is mechanics-only.  It never reads reward, PnL, markout, or other
economic outcomes.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from typing import Any

from execution.chunked_parquet_journal import ChunkedParquetJournalWriter
from execution.order_lifecycle import OrderLifecyclePhase
from research.families.f09_campaign_action_uplift.audit.causal_v12_ranked_toxicity_exposure_guard import (
    GuardState,
    RankedToxicityGuardRuntime,
    deterministic_campaign_side_assignment,
)
from research.families.f09_campaign_action_uplift.audit.causal_v12_ranked_toxicity_exposure_guard_full_path_adapter import (
    AdapterContractViolation,
    BaselineShadowSnapshot,
    DailyPastOnlyThreshold,
    FROZEN_RANDOM_SEEDS,
    GuardExecutionDirective,
    ProspectiveCampaignSideAssignment,
)
from research.families.f09_campaign_action_uplift.audit.causal_v12_ranked_toxicity_exposure_guard_full_path_adapter_v1_4 import (
    RankedToxicityGuardFullPathAdapterV14,
)

SCHEMA_VERSION = (
    "causal_v12_ranked_toxicity_exposure_guard_full_path_adapter.v2"
)


def stable_assignment_episode_id(
    *,
    side: str,
    initial_untreated_lineage_id: str,
    first_opportunity_decision_id: str,
    assignment_ts_ns: int,
) -> str:
    """Return a checkpoint-stable identifier for a new assignment episode."""

    normalized_side = str(side).strip().upper()
    lineage = str(initial_untreated_lineage_id).strip()
    decision = str(first_opportunity_decision_id).strip()
    timestamp = int(assignment_ts_ns)
    if normalized_side not in FROZEN_RANDOM_SEEDS:
        raise ValueError("side must be BUY or SELL")
    if not lineage or lineage.lower() == "nan":
        raise ValueError("initial untreated lineage identity is required")
    if not decision or decision.lower() == "nan" or timestamp <= 0:
        raise ValueError("stable first-opportunity identity is required")
    payload = (
        f"{SCHEMA_VERSION}|{normalized_side}|{lineage}|{decision}|{timestamp}"
    ).encode("utf-8")
    return f"{normalized_side.lower()}-episode-{hashlib.sha256(payload).hexdigest()[:24]}"


def stable_assignment_episode_integer(episode_id: str) -> int:
    """Map an episode identity to the frozen integer PRF input ABI."""

    normalized = str(episode_id).strip()
    if not normalized or normalized.lower() == "nan":
        raise ValueError("assignment_episode_id must be non-empty")
    value = int.from_bytes(hashlib.sha256(normalized.encode("utf-8")).digest()[:8], "big")
    value &= (1 << 63) - 1
    return value if value > 0 else 1


class RankedToxicityGuardFullPathAdapterV2(
    RankedToxicityGuardFullPathAdapterV14
):
    """Keep one arm until an untreated boundary has a complete natural washout."""

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
            journal_writer=journal_writer,
            retain_journal=retain_journal,
        )
        self._current_untreated_lineage_id = ""
        self._episode_lineages: dict[str, list[str]] = {}
        self._episode_order_ids: dict[str, set[str]] = {}
        self._order_episode_owner: dict[str, str] = {}
        self._completed_episode_ids: set[str] = set()
        self._censored_episode_ids: set[str] = set()
        self._lineage_transition_count = 0
        self._carryover_transition_count = 0
        self._clean_washout_count = 0
        self._cross_arm_order_ownership_count = 0
        self._forced_washout_cancel_count = 0
        self._active_order_role_transition_to_exposure_count = 0

    @property
    def assignment_episode_id(self) -> str:
        assignment = self._current_assignment
        return assignment.prospective_campaign_side_id if assignment else ""

    @property
    def current_untreated_lineage_id(self) -> str:
        return self._current_untreated_lineage_id

    def _append_journal(
        self,
        event_type: str,
        event_ts_ns: int,
        **payload: Any,
    ) -> None:
        payload.setdefault("assignment_episode_id", self.assignment_episode_id)
        payload.setdefault(
            "untreated_campaign_side_id", self._current_untreated_lineage_id
        )
        payload.setdefault("assignment_unit", "carryover_safe_assignment_episode")
        super()._append_journal(event_type, event_ts_ns, **payload)

    def _episode_nonterminal_orders(self, episode_id: str) -> list[str]:
        result: list[str] = []
        for order_id in sorted(self._episode_order_ids.get(episode_id, set())):
            tracked = self._orders.get(order_id)
            if tracked is None or order_id not in self._terminal_order_ids:
                result.append(order_id)
                continue
            if (
                tracked.lifecycle.fill_risk_active
                or tracked.hazard_attached
                or tracked.cursor_attached
            ):
                result.append(order_id)
        return result

    def _episode_washout_snapshot(self) -> dict[str, Any]:
        episode_id = self.assignment_episode_id
        nonterminal = self._episode_nonterminal_orders(episode_id) if episode_id else []
        runtime = self._runtime
        guard_neutral = bool(
            runtime is None
            or (
                not runtime.suppresses_exposure
                and runtime.state not in {GuardState.CANCEL_PENDING, GuardState.GUARD_ACTIVE}
                and not runtime.release_waiting_for_cancel_ack
            )
        )
        owned_deferred_cancel = sorted(
            order_id
            for order_id in self._pre_activation_cancel_requests
            if self._order_episode_owner.get(order_id) == episode_id
        )
        return {
            "assignment_episode_id": episode_id,
            "nonterminal_owned_order_ids": nonterminal,
            "nonterminal_owned_order_count": len(nonterminal),
            "guard_target_order_id": str(self._guard_target_order_id),
            "guard_neutral": guard_neutral,
            "pending_quote_directive_count": len(self._pending_directives),
            "owned_deferred_cancel_order_ids": owned_deferred_cancel,
            "washout_complete": bool(
                episode_id
                and not nonterminal
                and not self._guard_target_order_id
                and guard_neutral
                and not self._pending_directives
                and not owned_deferred_cancel
            ),
        }

    def _adopt_live_orders(self, episode_id: str, *, event_ts_ns: int) -> None:
        owned = self._episode_order_ids.setdefault(episode_id, set())
        for order_id, tracked in self._orders.items():
            if order_id in self._terminal_order_ids:
                continue
            prior_owner = self._order_episode_owner.get(order_id, "")
            if prior_owner and prior_owner != episode_id:
                self._cross_arm_order_ownership_count += 1
                raise AdapterContractViolation(
                    f"order {order_id} cannot transfer from {prior_owner} to {episode_id}"
                )
            self._order_episode_owner[order_id] = episode_id
            owned.add(order_id)
            self._append_journal(
                "pre_assignment_order_adopted",
                int(event_ts_ns),
                order_id=order_id,
                lifecycle_phase=tracked.lifecycle.phase.value,
                fill_risk_active=bool(tracked.lifecycle.fill_risk_active),
            )

    def _ensure_assignment(
        self,
        snapshot: BaselineShadowSnapshot,
        *,
        prospective_campaign_side_id: str | None,
        threshold: DailyPastOnlyThreshold,
    ) -> ProspectiveCampaignSideAssignment:
        initial_lineage = str(prospective_campaign_side_id or "").strip()
        if not initial_lineage or initial_lineage.lower() == "nan":
            raise AdapterContractViolation(
                "carryover-safe assignment requires an untreated lineage identity"
            )
        if self._current_assignment is not None:
            return self._current_assignment

        episode_id = stable_assignment_episode_id(
            side=self.side,
            initial_untreated_lineage_id=initial_lineage,
            first_opportunity_decision_id=snapshot.decision_id,
            assignment_ts_ns=snapshot.decision_ts_ns,
        )
        if episode_id in self._closed_assignment_ids:
            raise AdapterContractViolation("closed assignment episode was reused")
        stable_integer = stable_assignment_episode_integer(episode_id)
        assignment = deterministic_campaign_side_assignment(
            seed=self.random_seed,
            utc_day=snapshot.utc_day,
            side=self.side,
            campaign_opportunity_id=stable_integer,
            candidate_probability=self.candidate_probability,
        )
        self._assignment_sequence += 1
        prospective = ProspectiveCampaignSideAssignment(
            prospective_campaign_side_id=episode_id,
            assignment_utc_day=snapshot.utc_day,
            assignment_ts_ns=int(snapshot.decision_ts_ns),
            first_opportunity_decision_id=str(snapshot.decision_id),
            assignment_sequence=int(self._assignment_sequence),
            assignment=assignment,
        )
        self._current_assignment = prospective
        self._stable_assignment_ids[episode_id] = stable_integer
        self._current_untreated_lineage_id = initial_lineage
        self._episode_lineages[episode_id] = [initial_lineage]
        self._episode_order_ids[episode_id] = set()
        self._runtime = RankedToxicityGuardRuntime(
            side=self.side,
            action=assignment.action,
            threshold=threshold.threshold,
        )
        self._runtime_applied_bucket_ts_ms = None
        self._adopt_live_orders(
            episode_id,
            event_ts_ns=int(snapshot.decision_ts_ns),
        )
        self._append_journal(
            "assignment_episode_started",
            snapshot.decision_ts_ns,
            decision_id=snapshot.decision_id,
            initial_untreated_campaign_side_id=initial_lineage,
            assignment_sequence=int(self._assignment_sequence),
            stable_assignment_episode_integer=int(stable_integer),
            assignment_before_treatment=True,
            candidate_path_submit_count_at_assignment=int(
                self._candidate_path_submit_count
            ),
            candidate_path_fill_count_at_assignment=int(
                self._candidate_path_fill_event_count
            ),
        )
        return prospective

    def _finish_episode_at_clean_boundary(
        self,
        *,
        event_ts_ns: int,
        next_untreated_lineage_id: str,
    ) -> None:
        assignment = self._current_assignment
        if assignment is None:
            return
        washout = self._episode_washout_snapshot()
        if not washout["washout_complete"]:
            raise AdapterContractViolation("assignment episode ended before washout")
        episode_id = assignment.prospective_campaign_side_id
        self._append_journal(
            "assignment_episode_completed",
            event_ts_ns,
            next_untreated_campaign_side_id=str(next_untreated_lineage_id),
            member_untreated_campaign_side_ids=list(
                self._episode_lineages.get(episode_id, [])
            ),
            member_campaign_count=len(self._episode_lineages.get(episode_id, [])),
            owned_order_count=len(self._episode_order_ids.get(episode_id, set())),
            **washout,
        )
        self._closed_assignment_ids.add(episode_id)
        self._completed_episode_ids.add(episode_id)
        self._clean_washout_count += 1
        if self._runtime is not None:
            self._runtime.reset_campaign_side()
        self._current_assignment = None
        self._runtime = None
        self._guard_target_order_id = ""
        self._current_untreated_lineage_id = ""

    def _observe_untreated_lineage_boundary(
        self,
        *,
        next_lineage_id: str,
        event_ts_ns: int,
    ) -> None:
        normalized = str(next_lineage_id).strip()
        if not normalized or normalized.lower() == "nan":
            raise AdapterContractViolation("untreated lineage identity is empty")
        if self._current_assignment is None:
            self._current_untreated_lineage_id = normalized
            return
        if normalized == self._current_untreated_lineage_id:
            return
        self._lineage_transition_count += 1
        washout = self._episode_washout_snapshot()
        if washout["washout_complete"]:
            self._finish_episode_at_clean_boundary(
                event_ts_ns=event_ts_ns,
                next_untreated_lineage_id=normalized,
            )
            self._current_untreated_lineage_id = normalized
            return

        episode_id = self.assignment_episode_id
        members = self._episode_lineages.setdefault(episode_id, [])
        if normalized not in members:
            members.append(normalized)
        self._carryover_transition_count += 1
        prior_lineage = self._current_untreated_lineage_id
        self._current_untreated_lineage_id = normalized
        self._append_journal(
            "assignment_episode_carried_over",
            event_ts_ns,
            prior_untreated_campaign_side_id=prior_lineage,
            next_untreated_campaign_side_id=normalized,
            member_campaign_count=len(members),
            **washout,
        )

    def on_quote_decision(
        self,
        *,
        control_shadow: BaselineShadowSnapshot,
        candidate_shadow: BaselineShadowSnapshot,
        candidate_active_exposure_order_id: str = "",
        prospective_campaign_side_id: str | None = None,
    ) -> GuardExecutionDirective:
        lineage_id = str(prospective_campaign_side_id or "").strip()
        self._observe_untreated_lineage_boundary(
            next_lineage_id=lineage_id,
            event_ts_ns=int(control_shadow.decision_ts_ns),
        )
        active_order_id = str(candidate_active_exposure_order_id or "").strip()
        if active_order_id:
            tracked = self._orders.get(active_order_id)
            terminal = active_order_id in self._terminal_order_ids
            fill_risk_active = (
                bool(tracked.lifecycle.fill_risk_active)
                if tracked is not None
                else False
            )
            if tracked is None or terminal or not fill_risk_active:
                phase = (
                    tracked.lifecycle.phase.value if tracked is not None else "UNTRACKED"
                )
                raise AdapterContractViolation(
                    "candidate active exposure order is not in the real fill-risk set: "
                    f"order_id={active_order_id}, phase={phase}, terminal={terminal}, "
                    f"submitted_as_exposure_increasing="
                    f"{bool(tracked.exposure_increasing) if tracked is not None else False}, "
                    f"current_decision_exposure_increasing="
                    f"{bool(candidate_shadow.exposure_increasing)}, "
                    f"fill_risk_active={fill_risk_active}, "
                    f"hazard_attached="
                    f"{bool(tracked.hazard_attached) if tracked is not None else False}, "
                    f"cursor_attached="
                    f"{bool(tracked.cursor_attached) if tracked is not None else False}, "
                    f"owned_episode={self._order_episode_owner.get(active_order_id, '')}, "
                    f"current_episode={self.assignment_episode_id}"
                )
            if not bool(candidate_shadow.exposure_increasing):
                raise AdapterContractViolation(
                    "candidate active exposure order was supplied for a reducing decision"
                )
            if not tracked.exposure_increasing:
                prior_phase = tracked.lifecycle.phase.value
                tracked.exposure_increasing = True
                tracked.hazard_attached = True
                tracked.cursor_attached = True
                self._active_order_role_transition_to_exposure_count += 1
                self._append_journal(
                    "active_order_role_transition_to_exposure",
                    int(control_shadow.decision_ts_ns),
                    order_id=active_order_id,
                    lifecycle_phase=prior_phase,
                    submitted_as_exposure_increasing=False,
                    current_decision_exposure_increasing=True,
                    fill_risk_active=True,
                    hazard_attached=True,
                    cursor_attached=True,
                )
        assignment_id = self.assignment_episode_id
        return super().on_quote_decision(
            control_shadow=control_shadow,
            candidate_shadow=candidate_shadow,
            candidate_active_exposure_order_id=candidate_active_exposure_order_id,
            prospective_campaign_side_id=(assignment_id or lineage_id),
        )

    def on_order_submitted(self, **payload: Any) -> None:
        order_id = str(payload.get("order_id", "")).strip()
        super().on_order_submitted(**payload)
        episode_id = self.assignment_episode_id
        if not episode_id:
            return
        prior_owner = self._order_episode_owner.get(order_id, "")
        if prior_owner and prior_owner != episode_id:
            self._cross_arm_order_ownership_count += 1
            raise AdapterContractViolation(
                f"order {order_id} was submitted under two assignment episodes"
            )
        self._order_episode_owner[order_id] = episode_id
        self._episode_order_ids.setdefault(episode_id, set()).add(order_id)

    def end_prospective_campaign_side(self, **_: Any) -> None:
        raise AdapterContractViolation(
            "campaign-side terminal cannot end a carryover-safe assignment episode"
        )

    def censor_assignment_episode(self, *, event_ts_ns: int, reason: str) -> None:
        assignment = self._current_assignment
        if assignment is None:
            return
        if self._pending_directives:
            raise AdapterContractViolation(
                "cannot censor assignment episode with unfinished quote decisions"
            )
        episode_id = assignment.prospective_campaign_side_id
        washout = self._episode_washout_snapshot()
        self._append_journal(
            "assignment_episode_censored",
            event_ts_ns,
            censor_reason=str(reason),
            member_untreated_campaign_side_ids=list(
                self._episode_lineages.get(episode_id, [])
            ),
            member_campaign_count=len(self._episode_lineages.get(episode_id, [])),
            owned_order_count=len(self._episode_order_ids.get(episode_id, set())),
            **washout,
        )
        self._closed_assignment_ids.add(episode_id)
        self._censored_episode_ids.add(episode_id)
        self._current_assignment = None
        self._runtime = None
        self._guard_target_order_id = ""
        self._current_untreated_lineage_id = ""

    def contract_audit(self) -> dict[str, Any]:
        audit = super().contract_audit()
        owner_missing = [
            order_id
            for episode_id, order_ids in self._episode_order_ids.items()
            for order_id in order_ids
            if self._order_episode_owner.get(order_id) != episode_id
        ]
        audit.update(
            {
                "schema_version": f"{SCHEMA_VERSION}.contract_audit",
                "assignment_unit": "carryover_safe_assignment_episode",
                "completed_assignment_episode_count": len(
                    self._completed_episode_ids
                ),
                "censored_assignment_episode_count": len(
                    self._censored_episode_ids
                ),
                "untreated_lineage_transition_count": int(
                    self._lineage_transition_count
                ),
                "carryover_transition_count": int(
                    self._carryover_transition_count
                ),
                "clean_washout_count": int(self._clean_washout_count),
                "cross_arm_order_ownership_count": int(
                    self._cross_arm_order_ownership_count
                ),
                "forced_washout_cancel_count": int(
                    self._forced_washout_cancel_count
                ),
                "active_order_role_transition_to_exposure_count": int(
                    self._active_order_role_transition_to_exposure_count
                ),
                "order_owner_mismatch_count": len(owner_missing),
                "episode_campaign_membership": {
                    episode_id: list(lineages)
                    for episode_id, lineages in sorted(
                        self._episode_lineages.items()
                    )
                },
            }
        )
        audit["execution_completeness"]["uncensored_assignment_episode"] = int(
            self._current_assignment is not None
        )
        audit["execution_completeness"]["order_owner_mismatch"] = len(owner_missing)
        audit["execution_complete"] = all(
            value == 0 for value in audit["execution_completeness"].values()
        )
        audit["carryover_contract_valid"] = bool(
            self._cross_arm_order_ownership_count == 0
            and self._forced_washout_cancel_count == 0
            and not owner_missing
        )
        return audit


def carryover_safe_assignment_contract_v2() -> Mapping[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "assignment_unit": "carryover_safe_assignment_episode",
        "boundary_clock": "untreated_baseline_lineage_transition",
        "clean_boundary_requires": [
            "every_episode_owned_order_exchange_terminal",
            "terminal_cursor_and_hazard_cleared",
            "no_cancel_pending_or_deferred_cancel",
            "no_pending_quote_directive",
            "guard_permission_released_or_neutral",
        ],
        "carryover_rule": (
            "if_clean_boundary_is_false_the_next_inventory_campaign_remains_in_"
            "the_same_assignment_episode"
        ),
        "order_ownership_transfer": "forbidden",
        "active_order_role_transition": (
            "a_live_fill_risk_order_may_become_exposure_increasing_after_inventory_"
            "changes_and_is_then_bound_to_the_current_guard_risk_set"
        ),
        "forced_cancel_for_washout": "forbidden",
        "panel_end": "right_censor_assignment_episode",
        "economic_outcomes_read": False,
        "permissions": {
            "mechanics_execution_eligible": False,
            "action_experiment_authorized": False,
            "live_deployment_authorized": False,
        },
    }
