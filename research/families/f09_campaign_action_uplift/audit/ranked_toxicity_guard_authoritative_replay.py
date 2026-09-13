#!/usr/bin/env python3
"""Authoritative tick-replay binding for ranked-toxicity guard mechanics v1.4."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from execution.chunked_parquet_journal import (
    ChunkedParquetJournalWriter,
    iter_chunked_parquet_journal,
)
from research.families.f09_campaign_action_uplift.audit.causal_v12_ranked_toxicity_exposure_guard_full_path_adapter import (
    FROZEN_RANDOM_SEEDS,
    AdapterContractViolation,
    BaselineShadowSnapshot,
    CanonicalPredictionBucket,
    GuardExecutionDirective,
)
from research.families.f09_campaign_action_uplift.audit.causal_v12_ranked_toxicity_exposure_guard_full_path_adapter_v1_4 import (
    RankedToxicityGuardFullPathAdapterV14,
)

SCHEMA_VERSION = "ranked_toxicity_guard_authoritative_replay_binding.v1.4"
BASELINE_SHADOW_EVENT = "baseline_shadow_decision"
VALID_SIDES = frozenset({"BUY", "SELL"})
BASELINE_ELIGIBLE_ACTIONS = frozenset({"place", "replace", "keep"})


def _utc_day_from_ns(timestamp_ns: int) -> str:
    return datetime.fromtimestamp(
        int(timestamp_ns) / 1_000_000_000.0,
        tz=timezone.utc,
    ).date().isoformat()


def _fingerprint_tokens(values: list[str] | tuple[str, ...]) -> str:
    normalized = "|".join(sorted({str(value) for value in values if str(value)}))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ReplayGuardDirective:
    decision_id: str
    request_cancel_once: bool
    cancel_order_id: str
    allow_exposure_submission: bool


@dataclass(frozen=True)
class BaselineShadowDecisionRecord:
    snapshot: BaselineShadowSnapshot
    prospective_campaign_side_id: str
    baseline_action: str
    baseline_price: float
    baseline_quantity: float
    baseline_order_id: str

    def to_payload(self) -> dict[str, Any]:
        return {
            "snapshot": asdict(self.snapshot),
            "prospective_campaign_side_id": self.prospective_campaign_side_id,
            "baseline_action": self.baseline_action,
            "baseline_price": float(self.baseline_price),
            "baseline_quantity": float(self.baseline_quantity),
            "baseline_order_id": self.baseline_order_id,
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> BaselineShadowDecisionRecord:
        return cls(
            snapshot=BaselineShadowSnapshot(**dict(payload["snapshot"])),
            prospective_campaign_side_id=str(
                payload["prospective_campaign_side_id"]
            ),
            baseline_action=str(payload["baseline_action"]),
            baseline_price=float(payload["baseline_price"]),
            baseline_quantity=float(payload["baseline_quantity"]),
            baseline_order_id=str(payload.get("baseline_order_id", "")),
        )


class RankedToxicityBaselineShadowCaptureV14:
    """First-pass binding that records the untreated authoritative denominator."""

    mode = "baseline_shadow_capture"

    def __init__(
        self,
        *,
        output_dir: str | Path,
        lineage_namespace: str,
        sides: tuple[str, ...] = ("BUY", "SELL"),
        chunk_rows: int = 50_000,
    ) -> None:
        self.sides = frozenset(str(side).upper() for side in sides)
        if not self.sides or not self.sides <= VALID_SIDES:
            raise ValueError("baseline capture sides must be BUY and/or SELL")
        self.lineage_namespace = str(lineage_namespace).strip()
        if not self.lineage_namespace:
            raise ValueError("lineage_namespace is required")
        self.writer = ChunkedParquetJournalWriter(
            output_dir,
            journal_id=f"{SCHEMA_VERSION}.baseline_shadow",
            chunk_rows=chunk_rows,
        )
        self._pending: dict[str, tuple[BaselineShadowSnapshot, str]] = {}
        self._sequence = 0
        self._prediction_bucket_count = 0
        self._prediction_signatures: dict[int, tuple[int, int, float, float]] = {}
        self._last_prediction_bucket_ts_ms = 0
        self._quote_decision_count = 0
        self._closed = False

    def validate_replay_start(self, *, params: Mapping[str, Any], ml_data: Any) -> None:
        if ml_data is None:
            raise ValueError("ranked-toxicity baseline capture requires causal ML data")
        if bool(params.get("dynamic_fill_hazard_action_enabled", False)):
            raise ValueError("ranked-toxicity baseline requires q90 action OFF")

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
        if bucket in self._prediction_signatures:
            raise AdapterContractViolation(
                "baseline capture received a duplicate prediction bucket"
            )
        if bucket <= self._last_prediction_bucket_ts_ms:
            raise AdapterContractViolation(
                "baseline prediction bucket clock did not advance"
            )
        self._prediction_signatures[bucket] = signature
        self._last_prediction_bucket_ts_ms = bucket
        self._prediction_bucket_count += 1

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
        if self._last_prediction_bucket_ts_ms <= 0:
            raise AdapterContractViolation(
                "baseline quote decision arrived before the first prediction bucket"
            )
        if key in self._pending:
            raise AdapterContractViolation("baseline capture decision_id is not unique")
        ordinal = int(untreated_lineage_ordinal)
        if ordinal <= 0:
            raise ValueError("untreated_lineage_ordinal must be positive")
        prospective_id = (
            f"{self.lineage_namespace}|{normalized_side}|lineage-{ordinal:012d}"
        )
        snapshot = BaselineShadowSnapshot(
            decision_id=key,
            utc_day=_utc_day_from_ns(decision_ts_ns),
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
        self._sequence += 1
        self.writer.append(
            {
                "sequence": self._sequence,
                "event_type": BASELINE_SHADOW_EVENT,
                "event_ts_ns": int(event_ts_ns),
                "side": normalized_side,
                "decision_id": str(decision_id),
                "prospective_campaign_side_id": prospective_id,
                "record": record.to_payload(),
            }
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
                f"baseline capture has {len(self._pending)} unfinished decisions"
            )
        manifest = self.writer.close()
        self._closed = True
        return {
            "schema_version": f"{SCHEMA_VERSION}.baseline_capture_audit",
            "mode": self.mode,
            "prediction_bucket_count": int(self._prediction_bucket_count),
            "quote_decision_count": int(self._quote_decision_count),
            "baseline_shadow_rows": int(self._sequence),
            "journal_manifest": manifest,
            "mechanics_results_read": False,
            "economic_outcomes_read": False,
        }


class BaselineShadowTapeIndexV14:
    """Hash-verified one-panel baseline shadow index."""

    def __init__(self, manifest_path: str | Path) -> None:
        self.records: dict[str, BaselineShadowDecisionRecord] = {}
        self._consumed: set[str] = set()
        utc_days: set[str] = set()
        for row in iter_chunked_parquet_journal(manifest_path):
            if row["event_type"] != BASELINE_SHADOW_EVENT:
                continue
            key = str(row["decision_id"])
            if not key or key.lower() == "nan" or key in self.records:
                raise ValueError("baseline shadow decision IDs must be non-empty and unique")
            record = BaselineShadowDecisionRecord.from_payload(row["record"])
            if record.snapshot.decision_id != key:
                raise ValueError("baseline shadow payload decision ID mismatch")
            utc_days.add(str(record.snapshot.utc_day))
            self.records[key] = record
        if not self.records:
            raise ValueError("baseline shadow tape contains no decisions")
        if len(utc_days) != 1:
            raise ValueError(
                "baseline shadow index must contain exactly one UTC-day panel"
            )
        self.utc_day = next(iter(utc_days))

    def consume(self, decision_id: str, side: str) -> BaselineShadowDecisionRecord:
        key = str(decision_id)
        if key in self._consumed:
            raise AdapterContractViolation("baseline shadow decision was consumed twice")
        record = self.records.get(key)
        if record is None:
            raise AdapterContractViolation(
                f"candidate decision is absent from untreated baseline shadow: {key}"
            )
        if record.snapshot.side != str(side).upper():
            raise AdapterContractViolation("baseline shadow side mismatch")
        self._consumed.add(key)
        return record

    def audit(self) -> dict[str, Any]:
        missing = sorted(set(self.records) - self._consumed)
        return {
            "utc_day": self.utc_day,
            "rows": int(len(self.records)),
            "consumed": int(len(self._consumed)),
            "unconsumed": int(len(missing)),
            "first_unconsumed_decision_ids": missing[:10],
            "complete": not missing,
        }


class RankedToxicityGuardAuthoritativeReplayV14:
    """Second-pass binding that applies guard permission to regenerated paths."""

    mode = "candidate_execution"

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
        self.adapters: dict[str, RankedToxicityGuardFullPathAdapterV14] = {}
        for side in sorted(self.sides):
            writer = ChunkedParquetJournalWriter(
                root / side.lower(),
                journal_id=f"{SCHEMA_VERSION}.{side.lower()}",
                chunk_rows=chunk_rows,
            )
            adapter = RankedToxicityGuardFullPathAdapterV14(
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
        self._untreated_lineage_transition_count = 0

    def validate_replay_start(self, *, params: Mapping[str, Any], ml_data: Any) -> None:
        if ml_data is None or len(ml_data) < 6:
            raise ValueError("ranked-toxicity execution requires tox_bid/tox_ask ML data")
        if bool(params.get("dynamic_fill_hazard_action_enabled", False)):
            raise ValueError("ranked-toxicity execution requires q90 action OFF")
        forbidden = (
            "variance_time_lineage_randomized_enabled",
            "sell_add_inventory_price_penalty_enabled",
            "multi_short_reducing_buy_aggression_enabled",
            "cross_venue_fair_center_shift_enabled",
            "safe_add_rearm_randomized_enabled",
            "state_conditioned_rearm_enabled",
            "sell_add_skip_ope_enabled",
            "queue_value_keep_cancel_enabled",
            "local_action_ope_enabled",
        )
        active = [name for name in forbidden if bool(params.get(name, False))]
        if active:
            raise ValueError(
                "ranked-toxicity full path must run alone: " + ", ".join(active)
            )

    def on_prediction_bucket(
        self,
        *,
        prediction_bucket_ts_ms: int,
        feature_ready_ts_ms: int,
        observation_ts_ms: int,
        tox_bid: float,
        tox_ask: float,
    ) -> None:
        day = datetime.fromtimestamp(
            int(prediction_bucket_ts_ms) / 1000.0,
            tz=timezone.utc,
        ).date().isoformat()
        for side, score in (("BUY", tox_bid), ("SELL", tox_ask)):
            adapter = self.adapters.get(side)
            if adapter is None:
                continue
            adapter.on_prediction_bucket(
                CanonicalPredictionBucket(
                    utc_day=day,
                    prediction_bucket_ts_ms=int(prediction_bucket_ts_ms),
                    feature_ready_ts_ms=int(feature_ready_ts_ms),
                    decision_ts_ms=int(observation_ts_ms),
                    score=float(score),
                    model_sha256=self.frozen_model_sha256,
                )
            )

    def on_quote_decision(self, **current: Any) -> GuardExecutionDirective | ReplayGuardDirective:
        side = str(current["side"]).upper()
        adapter = self.adapters.get(side)
        decision_id = str(current["decision_id"])
        if adapter is None:
            return ReplayGuardDirective(decision_id, False, "", True)
        record = self.baseline.consume(decision_id, side)
        snapshot = record.snapshot
        if int(snapshot.decision_ts_ns) != int(current["decision_ts_ns"]):
            raise AdapterContractViolation("baseline/candidate decision clock mismatch")
        assignment = adapter.current_assignment
        if (
            assignment is not None
            and assignment.prospective_campaign_side_id
            != record.prospective_campaign_side_id
        ):
            adapter.end_prospective_campaign_side(
                prospective_campaign_side_id=(
                    assignment.prospective_campaign_side_id
                ),
                event_ts_ns=int(current["decision_ts_ns"]),
                reason="untreated_baseline_lineage_transition",
            )
            self._untreated_lineage_transition_count += 1
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
        self._pending_records[decision_id] = record
        return adapter.on_quote_decision(
            control_shadow=snapshot,
            candidate_shadow=candidate_snapshot,
            candidate_active_exposure_order_id=str(
                current.get("active_exposure_order_id", "") or ""
            ),
            prospective_campaign_side_id=record.prospective_campaign_side_id,
        )

    def on_final_quote_action(self, **candidate: Any) -> bool:
        side = str(candidate["side"]).upper()
        adapter = self.adapters.get(side)
        if adapter is None:
            return False
        decision_id = str(candidate["decision_id"])
        record = self._pending_records.pop(decision_id, None)
        if record is None:
            raise AdapterContractViolation(
                "candidate final action lacks consumed baseline shadow"
            )
        return adapter.observe_final_quote_action(
            decision_id=decision_id,
            role=str(candidate["role"]),
            exposure_increasing=bool(candidate["exposure_increasing"]),
            baseline_action=record.baseline_action,
            candidate_action=str(candidate["candidate_action"]),
            baseline_price=float(record.baseline_price),
            candidate_price=float(candidate["candidate_price"]),
            baseline_quantity=float(record.baseline_quantity),
            candidate_quantity=float(candidate["candidate_quantity"]),
            event_ts_ns=int(candidate["event_ts_ns"]),
            baseline_order_id=record.baseline_order_id,
            candidate_order_id=str(candidate.get("candidate_order_id", "") or ""),
        )

    def on_order_submitted(self, *, side: str, **payload: Any) -> None:
        adapter = self.adapters.get(str(side).upper())
        if adapter is not None:
            adapter.on_order_submitted(**payload)

    def on_order_activated(self, *, side: str, **payload: Any) -> None:
        adapter = self.adapters.get(str(side).upper())
        if adapter is not None:
            adapter.on_order_activated(**payload)

    def on_cancel_requested(self, *, side: str, **payload: Any) -> None:
        adapter = self.adapters.get(str(side).upper())
        if adapter is not None:
            adapter.on_cancel_requested(**payload)

    def on_cancel_rejected(self, *, side: str, **payload: Any) -> None:
        adapter = self.adapters.get(str(side).upper())
        if adapter is not None:
            adapter.on_cancel_rejected(**payload)

    def on_order_fill(self, *, side: str, **payload: Any) -> None:
        adapter = self.adapters.get(str(side).upper())
        if adapter is not None:
            adapter.on_order_fill(**payload)

    def on_exchange_terminal(self, *, side: str, **payload: Any) -> None:
        adapter = self.adapters.get(str(side).upper())
        if adapter is not None:
            adapter.on_exchange_terminal(**payload)

    def on_campaign_terminal(
        self,
        *,
        event_ts_ns: int,
        candidate_campaign_ordinal: int,
    ) -> None:
        del event_ts_ns
        ordinal = int(candidate_campaign_ordinal)
        if ordinal <= 0:
            raise ValueError("candidate_campaign_ordinal must be positive")
        # Candidate campaign boundaries are treatment-dependent. They are
        # recorded for mechanics only and never terminate or rerandomize the
        # prospective assignment. Assignment boundaries come exclusively from
        # stable untreated-lineage changes in the baseline shadow tape.
        self._candidate_campaign_terminal_count += 1

    def finish_replay(self, *, event_ts_ns: int) -> dict[str, Any]:
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
            assignment = adapter.current_assignment
            if assignment is not None:
                adapter.end_prospective_campaign_side(
                    prospective_campaign_side_id=(
                        assignment.prospective_campaign_side_id
                    ),
                    event_ts_ns=int(event_ts_ns),
                    reason="authoritative_replay_panel_end_censor",
                )
            adapter_audits[side] = adapter.assert_execution_complete()
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
            "untreated_lineage_transition_count": int(
                self._untreated_lineage_transition_count
            ),
            "mechanics_results_read": False,
            "economic_outcomes_read": False,
            "action_experiment_authorized": False,
            "live_deployment_authorized": False,
        }
        return dict(self._replay_audit)


def authoritative_replay_binding_contract_v1_4() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "passes": ["untreated_baseline_shadow", "candidate_regenerated_path"],
        "baseline_denominator_is_treatment_independent": True,
        "candidate_permission_uses_regenerated_role": True,
        "prediction_bucket_and_quote_decision_interfaces_separate": True,
        "stable_assignment_source": "baseline_shadow_prospective_campaign_side_id",
        "assignment_boundary_source": "untreated_baseline_lineage_transition_only",
        "candidate_campaign_terminal_rerandomizes": False,
        "baseline_index_scope": "exactly_one_UTC_day_panel",
        "journal_storage": "local_atomic_chunked_parquet",
        "mechanics_results_read": False,
        "economic_outcomes_read": False,
        "permissions": {
            "action_experiment_authorized": False,
            "live_deployment_authorized": False,
        },
    }
