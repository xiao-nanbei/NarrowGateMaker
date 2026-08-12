#!/usr/bin/env python3
"""Authoritative replay binding for carryover-safe guard assignments.

The untreated baseline tape still supplies the ex-ante opportunity and lineage
clock.  Candidate inventory campaign terminals never rerandomize.  A new arm
may begin only at an untreated lineage boundary where the prior episode has no
owned order, queue cursor, hazard, cancel lifecycle, or suppressing guard state.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from execution.chunked_parquet_journal import ChunkedParquetJournalWriter
from research.families.f09_campaign_action_uplift.audit.causal_v12_ranked_toxicity_exposure_guard_full_path_adapter import (
    AdapterContractViolation,
    BaselineShadowSnapshot,
    FROZEN_RANDOM_SEEDS,
)
from research.families.f09_campaign_action_uplift.audit.causal_v12_ranked_toxicity_exposure_guard_full_path_adapter_v2 import (
    RankedToxicityGuardFullPathAdapterV2,
)
from research.families.f09_campaign_action_uplift.audit.ranked_toxicity_guard_authoritative_replay import (
    BaselineShadowDecisionRecord,
    BaselineShadowTapeIndexV14,
    ReplayGuardDirective,
    _fingerprint_tokens,
)
from research.families.f09_campaign_action_uplift.audit.ranked_toxicity_guard_authoritative_replay_v1_5 import (
    RankedToxicityGuardAuthoritativeReplayV15,
)

SCHEMA_VERSION = "ranked_toxicity_guard_authoritative_replay_binding.v2"
VALID_SIDES = frozenset({"BUY", "SELL"})


def _utc_day_from_ns(timestamp_ns: int) -> str:
    return datetime.fromtimestamp(
        int(timestamp_ns) / 1_000_000_000.0,
        tz=timezone.utc,
    ).date().isoformat()


class RankedToxicityGuardAuthoritativeReplayV2(
    RankedToxicityGuardAuthoritativeReplayV15
):
    """Two-pass candidate binding with carryover-safe assignment ownership."""

    mode = "candidate_execution_carryover_safe_assignment"

    def __init__(
        self,
        *,
        baseline_manifest_path: str | Path,
        output_root: str | Path,
        frozen_model_sha256: str,
        threshold_schedule: Mapping[str, Mapping[str, tuple[float, str]]],
        sides: tuple[str, ...] = ("BUY", "SELL"),
        chunk_rows: int = 50_000,
    ) -> None:
        self.sides = frozenset(str(side).upper() for side in sides)
        if not self.sides or not self.sides <= VALID_SIDES:
            raise ValueError("candidate sides must be BUY and/or SELL")
        self.baseline = BaselineShadowTapeIndexV14(baseline_manifest_path)
        root = Path(output_root).expanduser().resolve()
        self.adapters: dict[str, RankedToxicityGuardFullPathAdapterV2] = {}
        for side in sorted(self.sides):
            writer = ChunkedParquetJournalWriter(
                root / side.lower(),
                journal_id=f"{SCHEMA_VERSION}.{side.lower()}",
                chunk_rows=chunk_rows,
            )
            adapter = RankedToxicityGuardFullPathAdapterV2(
                side=side,
                random_seed=FROZEN_RANDOM_SEEDS[side],
                frozen_model_sha256=frozen_model_sha256,
                journal_writer=writer,
                retain_journal=False,
            )
            for day, threshold_identity in threshold_schedule.get(side, {}).items():
                threshold, source_hash = threshold_identity
                adapter.register_daily_threshold(
                    utc_day=str(day),
                    threshold=float(threshold),
                    source_identity_sha256=str(source_hash),
                )
            self.adapters[side] = adapter
        self.frozen_model_sha256 = str(frozen_model_sha256)
        self._pending_records: dict[str, BaselineShadowDecisionRecord] = {}
        self._replay_audit: dict[str, Any] | None = None
        self._candidate_campaign_terminal_count = 0
        self._prediction_ready = False
        self._preprediction_pending: dict[str, BaselineShadowDecisionRecord] = {}
        self._preprediction_decision_count = 0

    def on_quote_decision(self, **current: Any) -> Any:
        if not self._prediction_ready:
            return RankedToxicityGuardAuthoritativeReplayV15.on_quote_decision(
                self, **current
            )

        side = str(current["side"]).upper()
        adapter = self.adapters.get(side)
        decision_id = str(current["decision_id"])
        if adapter is None:
            return ReplayGuardDirective(decision_id, False, "", True)
        record = self.baseline.consume(decision_id, side)
        snapshot = record.snapshot
        if int(snapshot.decision_ts_ns) != int(current["decision_ts_ns"]):
            raise AdapterContractViolation("baseline/candidate decision clock mismatch")
        candidate_snapshot = BaselineShadowSnapshot(
            decision_id=decision_id,
            utc_day=_utc_day_from_ns(int(current["decision_ts_ns"])),
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
        if decision_id in self._pending_records:
            raise AdapterContractViolation("candidate decision identity was reused")
        self._pending_records[decision_id] = record
        return adapter.on_quote_decision(
            control_shadow=snapshot,
            candidate_shadow=candidate_snapshot,
            candidate_active_exposure_order_id=str(
                current.get("active_exposure_order_id", "") or ""
            ),
            prospective_campaign_side_id=record.prospective_campaign_side_id,
        )

    def on_campaign_terminal(
        self,
        *,
        event_ts_ns: int,
        candidate_campaign_ordinal: int,
    ) -> None:
        del event_ts_ns
        if int(candidate_campaign_ordinal) <= 0:
            raise ValueError("candidate_campaign_ordinal must be positive")
        self._candidate_campaign_terminal_count += 1

    def finish_replay(self, *, event_ts_ns: int) -> dict[str, Any]:
        if self._preprediction_pending:
            raise AdapterContractViolation(
                "prediction-warmup replay has unfinished quote decisions"
            )
        if self._pending_records:
            raise AdapterContractViolation(
                f"candidate replay has {len(self._pending_records)} unfinished decisions"
            )
        baseline_audit = self.baseline.audit()
        if not baseline_audit["complete"]:
            raise AdapterContractViolation(
                "candidate path did not visit every untreated quote decision"
            )
        adapter_audits: dict[str, Any] = {}
        manifests: dict[str, Any] = {}
        for side, adapter in self.adapters.items():
            adapter.censor_assignment_episode(
                event_ts_ns=int(event_ts_ns),
                reason="authoritative_replay_panel_end",
            )
            audit = adapter.assert_execution_complete()
            if not bool(audit.get("carryover_contract_valid", False)):
                raise AdapterContractViolation(
                    f"{side} carryover-safe assignment contract failed"
                )
            adapter_audits[side] = audit
            manifests[side] = adapter.close_journal()
        self._replay_audit = {
            "schema_version": f"{SCHEMA_VERSION}.candidate_audit",
            "mode": self.mode,
            "baseline_shadow": baseline_audit,
            "adapters": adapter_audits,
            "journal_manifests": manifests,
            "candidate_campaign_terminal_count": int(
                self._candidate_campaign_terminal_count
            ),
            "preprediction_passthrough_decision_count": int(
                self._preprediction_decision_count
            ),
            "mechanics_results_read": False,
            "economic_outcomes_read": False,
            "action_experiment_authorized": False,
            "live_deployment_authorized": False,
        }
        return dict(self._replay_audit)


def authoritative_replay_binding_contract_v2() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "passes": ["untreated_baseline_shadow", "candidate_regenerated_path"],
        "assignment_unit": "carryover_safe_assignment_episode",
        "assignment_boundary_source": "untreated_baseline_lineage_transition_only",
        "clean_boundary_requires_natural_order_and_guard_washout": True,
        "candidate_campaign_terminal_rerandomizes": False,
        "cross_campaign_order_ownership_transfer": "forbidden",
        "forced_cancel_for_assignment_boundary": "forbidden",
        "panel_end_assignment_treatment": "right_censor",
        "baseline_denominator_is_treatment_independent": True,
        "mechanics_results_read": False,
        "economic_outcomes_read": False,
        "permissions": {
            "action_experiment_authorized": False,
            "live_deployment_authorized": False,
        },
    }
