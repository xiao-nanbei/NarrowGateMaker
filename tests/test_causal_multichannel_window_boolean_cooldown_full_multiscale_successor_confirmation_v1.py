from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_confirmation_v1 as confirmation,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_repeated_policy_v1 as repeated,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_v1 as successor,
)
from strategy.boolean_cooldown_coverage import BooleanCooldownCoverageReason


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("ascii")).hexdigest()


def _contract(
    *,
    locked_at: str = "2026-08-13T00:00:00Z",
    minimum_days: int = 30,
    source_identity: str = "f05_natural_canonical_source_bundle_v1",
) -> confirmation.FinalArtifactConfirmationContract:
    return confirmation.FinalArtifactConfirmationContract.build(
        final_artifact_identity="f05_successor_final_refit_sell_v1",
        final_artifact_locked_at_utc=locked_at,
        final_artifact_manifest_sha256=_sha("artifact-manifest"),
        final_policy_sha256=_sha("compiled-policy"),
        compiler_identity="f05_boolean_compiler_v1",
        compiler_sha256=_sha("compiler"),
        source_identity=source_identity,
        source_sha256=_sha("source"),
        runtime_identity="f05_confirmation_runtime_v1",
        runtime_sha256=_sha("runtime"),
        learning_algorithm_identity="f05_successor_nested_oof_v1",
        learning_algorithm_oof_artifact_sha256=_sha("learning-oof"),
        candidate_target_side="SELL",
        coverage_contract_sha256=_sha("coverage-contract"),
        fallback_contract_sha256=_sha("fallback-contract"),
        minimum_active_utc_days=minimum_days,
    )


@pytest.mark.parametrize(
    "source_identity",
    (
        "f05_cooldown_companion_v1",
        "f05_shadow_writer_v1",
        "f05_prospective_source_bundle_v1",
        "ordinary_noncanonical_input",
    ),
)
def test_confirmation_rejects_research_specific_or_noncanonical_sources(
    source_identity: str,
) -> None:
    with pytest.raises(confirmation.ConfirmationError, match="natural canonical"):
        _contract(source_identity=source_identity)


def _paired_receipt(
    contract: confirmation.FinalArtifactConfirmationContract,
    *,
    utc_day: str,
    scope: repeated.ExecutedArtifactScope = (
        repeated.ExecutedArtifactScope.FINAL_FULL_DEVELOPMENT_REFIT
    ),
) -> repeated.PairedSequentialReplayReceipt:
    exact = scope == repeated.ExecutedArtifactScope.FINAL_FULL_DEVELOPMENT_REFIT
    candidate_identity = (
        contract.final_artifact_identity
        if exact
        else contract.learning_algorithm_identity
    )
    candidate_sha = (
        contract.final_policy_sha256
        if exact
        else contract.learning_algorithm_oof_artifact_sha256
    )
    market_sha = _sha(f"market:{utc_day}")
    receive_clock_sha = _sha(f"receive-clock:{utc_day}")
    feature_clock_sha = _sha(f"feature-clock:{utc_day}")
    random_sha = _sha(f"random:{utc_day}")
    exogenous_sha = _canonical_sha256(
        {
            "common_market_source_sha256": market_sha,
            "common_receive_clock_source_sha256": receive_clock_sha,
            "common_feature_ready_clock_source_sha256": feature_clock_sha,
            "common_random_source_sha256": random_sha,
        }
    )
    body = {
        "schema_version": repeated.SCHEMA_VERSION,
        "identity": repeated.IDENTITY,
        "chain_identity_sha256": _sha(f"chain:{utc_day}"),
        "segment_index": 0,
        "segment_id": f"segment-{utc_day}",
        "utc_day": utc_day,
        "segment_start_utc": f"{utc_day}T00:00:01Z",
        "segment_end_utc": f"{utc_day}T00:00:10Z",
        "previous_segment_receipt_sha256": None,
        "day_admission_identity": "f05_test_formal_day_admission_v1",
        "day_admission_receipt_sha256": _sha(f"day-admission:{utc_day}"),
        "candidate_target_side": contract.candidate_target_side,
        "control_policy_identity": successor.ACTIVE_OWNER_POLICY_IDENTITY,
        "control_policy_sha256": successor.ACTIVE_OWNER_POLICY_SHA256,
        "candidate_policy_identity": candidate_identity,
        "candidate_policy_sha256": candidate_sha,
        "executed_artifact_scope": str(scope),
        "learning_algorithm_identity": contract.learning_algorithm_identity,
        "learning_algorithm_artifact_sha256": (
            contract.learning_algorithm_oof_artifact_sha256
        ),
        "final_artifact_identity": contract.final_artifact_identity if exact else None,
        "final_artifact_sha256": contract.final_policy_sha256 if exact else None,
        "exact_final_artifact_oof_available": False,
        "common_input_identity_sha256": _sha(f"common-input:{utc_day}"),
        "common_market_source_sha256": market_sha,
        "common_receive_clock_source_sha256": receive_clock_sha,
        "common_feature_ready_clock_source_sha256": feature_clock_sha,
        "common_random_source_sha256": random_sha,
        "paired_exogenous_clock_identity_sha256": exogenous_sha,
        "control_state_identity_sha256": _sha(f"control-state:{utc_day}"),
        "candidate_state_identity_sha256": _sha(f"candidate-state:{utc_day}"),
        "control_input_state_sha256": _sha(f"control-input:{utc_day}"),
        "candidate_input_state_sha256": _sha(f"candidate-input:{utc_day}"),
        "control_output_state_sha256": _sha(f"control-output:{utc_day}"),
        "candidate_output_state_sha256": _sha(f"candidate-output:{utc_day}"),
        "restart_manifest_sha256": None,
        "fully_bound_restart_restored": False,
        "control_repeated_policy_evaluations": 3,
        "candidate_repeated_policy_evaluations": 3,
        "candidate_target_side_evaluations": 2,
        "candidate_b0_delegated_evaluations": 1,
        "control_campaign_terminal_value_usdc": -1.0,
        "candidate_campaign_terminal_value_usdc": -0.8,
        "terminal_value_delta_usdc": 0.2,
        "formal_denominator_eligible": True,
        "exclusion_reasons": (),
        "control_transport_receipt_sha256": _sha(f"control-transport:{utc_day}"),
        "candidate_transport_receipt_sha256": _sha(
            f"candidate-transport:{utc_day}"
        ),
        "control_checkpoint_reused": False,
        "candidate_checkpoint_reused": False,
        "paired_audit_sha256": _sha(f"paired-audit:{utc_day}"),
        "repeated_sequential_policy": True,
        "one_shot_effect_aggregation_used": False,
        "same_market_source": True,
        "same_receive_and_feature_ready_clocks": True,
        "common_random_source": True,
        "arm_local_state": True,
        "state_chain_contiguous": True,
        "fresh_start_used": False,
        "live_equivalent": False,
        "research_supported": False,
        "action_authorized": False,
        "live_policy_authorized": False,
    }
    return repeated.PairedSequentialReplayReceipt(
        **body,
        receipt_sha256=_canonical_sha256(body),
    )


def _clock_audit(day: str, *, valid: bool = True) -> confirmation.ThreeClockAudit:
    start = datetime.fromisoformat(day).replace(tzinfo=UTC) + timedelta(seconds=2)
    base = int(start.timestamp() * 1_000_000_000)
    rows = [
        {
            "event_id": f"event-{ordinal}",
            "receive_ts_ns": base + ordinal * 1_000_000,
            "feature_ready_ts_ns": base + ordinal * 1_000_000 + 100,
            "policy_decision_ts_ns": (
                base + ordinal * 1_000_000 + (200 if valid else -100)
            ),
        }
        for ordinal in range(3)
    ]
    return confirmation.ThreeClockAudit.from_rows(rows)


def _coverage_audit(
    contract: confirmation.FinalArtifactConfirmationContract,
    *,
    candidate_actions: int = 1,
    coverage_sha: str | None = None,
    fallback_sha: str | None = None,
) -> confirmation.CommonCoverageAudit:
    coverage_hash = coverage_sha or contract.coverage_contract_sha256
    fallback_hash = fallback_sha or contract.fallback_contract_sha256
    control = confirmation.ArmCoverageAudit.build(
        arm=repeated.CONTROL_ARM,
        coverage_contract_sha256=coverage_hash,
        fallback_contract_sha256=fallback_hash,
        coverage_reason_counts={
            BooleanCooldownCoverageReason.ELIGIBLE_FEATURE_READY.value: 3,
        },
        fallback_reason_counts={},
        nonbaseline_action_count=0,
    )
    candidate = confirmation.ArmCoverageAudit.build(
        arm=repeated.CANDIDATE_ARM,
        coverage_contract_sha256=coverage_hash,
        fallback_contract_sha256=fallback_hash,
        coverage_reason_counts={
            BooleanCooldownCoverageReason.ELIGIBLE_FEATURE_READY.value: 2,
            BooleanCooldownCoverageReason.POLICY_CONTROL.value: 1,
        },
        fallback_reason_counts={
            "policy_control": 1,
        },
        nonbaseline_action_count=candidate_actions,
    )
    return confirmation.CommonCoverageAudit.build(
        control=control,
        candidate=candidate,
    )


def _session(
    contract: confirmation.FinalArtifactConfirmationContract,
    *,
    utc_day: str = "2026-08-14",
    session_id: str | None = None,
    candidate_actions: int = 1,
    receipt_scope: repeated.ExecutedArtifactScope = (
        repeated.ExecutedArtifactScope.FINAL_FULL_DEVELOPMENT_REFIT
    ),
    source_panel_role: str = confirmation.SOURCE_PANEL_ROLE,
    historical_development_read: bool = False,
    validation_read: bool = False,
    sealed_holdout_read: bool = False,
    observed_overrides: dict[str, str] | None = None,
    coverage_audit: confirmation.CommonCoverageAudit | None = None,
    clock_audit: confirmation.ThreeClockAudit | None = None,
) -> confirmation.ProspectiveConfirmationSession:
    observed = {
        "final_artifact_identity": contract.final_artifact_identity,
        "final_artifact_manifest_sha256": contract.final_artifact_manifest_sha256,
        "compiler_sha256": contract.compiler_sha256,
        "source_sha256": contract.source_sha256,
        "runtime_sha256": contract.runtime_sha256,
        "policy_sha256": contract.final_policy_sha256,
        "learning_algorithm_identity": contract.learning_algorithm_identity,
        "learning_algorithm_oof_artifact_sha256": (
            contract.learning_algorithm_oof_artifact_sha256
        ),
    }
    observed.update(observed_overrides or {})
    return confirmation.ProspectiveConfirmationSession.build(
        contract=contract,
        session_id=session_id or f"session-{utc_day}",
        utc_day=utc_day,
        session_started_at_utc=f"{utc_day}T00:00:01Z",
        session_ended_at_utc=f"{utc_day}T00:00:10Z",
        paired_receipt=_paired_receipt(
            contract,
            utc_day=utc_day,
            scope=receipt_scope,
        ),
        three_clock_audit=clock_audit or _clock_audit(utc_day),
        common_coverage_audit=coverage_audit
        or _coverage_audit(contract, candidate_actions=candidate_actions),
        source_panel_role=source_panel_role,
        historical_development_read=historical_development_read,
        validation_read=validation_read,
        sealed_holdout_read=sealed_holdout_read,
        **observed,
    )


def test_contract_freezes_thirty_active_days_and_permissions() -> None:
    contract = _contract()

    assert contract.minimum_active_utc_days == 30
    assert contract.exact_artifact_evidence_scope == (
        confirmation.EXACT_ARTIFACT_EVIDENCE_SCOPE
    )
    assert contract.learning_algorithm_evidence_scope == (
        confirmation.LEARNING_ALGORITHM_EVIDENCE_SCOPE
    )
    assert not contract.research_supported
    assert not contract.action_authorized
    assert not contract.live_policy_authorized

    with pytest.raises(confirmation.ConfirmationError, match="below 30"):
        _contract(minimum_days=29)


def test_only_sessions_strictly_after_final_artifact_lock_are_accepted() -> None:
    contract = _contract(locked_at="2026-08-14T00:00:01Z")

    with pytest.raises(confirmation.ConfirmationError, match="strictly later"):
        _session(contract, utc_day="2026-08-14")

    later = _session(contract, utc_day="2026-08-15")
    assert later.utc_day == "2026-08-15"


@pytest.mark.parametrize(
    "field",
    [
        "final_artifact_manifest_sha256",
        "compiler_sha256",
        "source_sha256",
        "runtime_sha256",
        "policy_sha256",
    ],
)
def test_artifact_compiler_source_runtime_and_policy_hash_drift_fail_closed(
    field: str,
) -> None:
    contract = _contract()

    with pytest.raises(confirmation.ConfirmationError, match=field):
        _session(contract, observed_overrides={field: _sha(f"drift:{field}")})


def test_learning_algorithm_oof_receipt_is_not_exact_artifact_evidence() -> None:
    contract = _contract()

    with pytest.raises(confirmation.ConfirmationError, match="exact repeated-policy"):
        _session(
            contract,
            receipt_scope=repeated.ExecutedArtifactScope.LEARNING_ALGORITHM_FOLD_POLICY,
        )


def test_three_clock_ordering_and_session_bounds_are_required() -> None:
    contract = _contract()
    invalid = _clock_audit("2026-08-14", valid=False)
    assert not invalid.valid

    with pytest.raises(confirmation.ConfirmationError, match="three-clock"):
        _session(contract, clock_audit=invalid)

    outside_rows = [
        {
            "event_id": "outside",
            "receive_ts_ns": int(
                datetime(2026, 8, 14, 1, tzinfo=UTC).timestamp() * 1_000_000_000
            ),
            "feature_ready_ts_ns": int(
                datetime(2026, 8, 14, 1, tzinfo=UTC).timestamp() * 1_000_000_000
            ),
            "policy_decision_ts_ns": int(
                datetime(2026, 8, 14, 1, tzinfo=UTC).timestamp() * 1_000_000_000
            ),
        }
    ]
    outside = confirmation.ThreeClockAudit.from_rows(outside_rows)
    with pytest.raises(confirmation.ConfirmationError, match="outside"):
        _session(contract, clock_audit=outside)


def test_common_coverage_and_fallback_contract_hashes_are_enforced() -> None:
    contract = _contract()
    drifted = _coverage_audit(
        contract,
        coverage_sha=_sha("other-coverage"),
        fallback_sha=_sha("other-fallback"),
    )

    with pytest.raises(confirmation.ConfirmationError, match="coverage/fallback"):
        _session(contract, coverage_audit=drifted)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("source_panel_role", "historical_71d_development"),
        ("historical_development_read", True),
        ("validation_read", True),
        ("sealed_holdout_read", True),
    ],
)
def test_consumed_development_validation_and_holdout_are_rejected(
    field: str,
    value: str | bool,
) -> None:
    contract = _contract()

    with pytest.raises(confirmation.ConfirmationError):
        _session(contract, **{field: value})


def test_atomic_ledger_rejects_duplicate_session_and_receipt(tmp_path: Path) -> None:
    contract = _contract()
    session = _session(contract)
    path = tmp_path / "confirmation-ledger.json"

    ledger = confirmation.append_confirmation_session(
        path,
        contract=contract,
        session=session,
    )
    assert ledger.status == confirmation.ConfirmationStatus.PENDING_MINIMUM_ACTIVE_DAYS
    assert ledger.active_utc_day_count == 1
    assert not ledger.ready_for_formal_evaluation
    assert path.is_file()
    assert not tuple(tmp_path.glob("*.partial"))

    with pytest.raises(confirmation.ConfirmationError, match="duplicate"):
        confirmation.append_confirmation_session(
            path,
            contract=contract,
            session=session,
        )

    body = session.to_dict()
    body["session_id"] = "different-id"
    body.pop("session_evidence_sha256")
    duplicate_receipt = confirmation.ProspectiveConfirmationSession.from_dict(
        {**body, "session_evidence_sha256": _canonical_sha256(body)}
    )
    with pytest.raises(confirmation.ConfirmationError, match="duplicate paired receipt"):
        confirmation.append_confirmation_session(
            path,
            contract=contract,
            session=duplicate_receipt,
        )


def test_ledger_requires_chronological_append(tmp_path: Path) -> None:
    contract = _contract()
    path = tmp_path / "confirmation-ledger.json"
    confirmation.append_confirmation_session(
        path,
        contract=contract,
        session=_session(contract, utc_day="2026-08-16"),
    )

    with pytest.raises(confirmation.ConfirmationError, match="chronological"):
        confirmation.append_confirmation_session(
            path,
            contract=contract,
            session=_session(contract, utc_day="2026-08-15"),
        )


def test_distinct_active_days_reach_minimum_without_granting_authority(
    tmp_path: Path,
) -> None:
    contract = _contract()
    path = tmp_path / "confirmation-ledger.json"
    first_day = date(2026, 8, 14)
    ledger = None
    for offset in range(30):
        day = (first_day + timedelta(days=offset)).isoformat()
        ledger = confirmation.append_confirmation_session(
            path,
            contract=contract,
            session=_session(contract, utc_day=day),
        )
    assert ledger is not None
    assert ledger.active_utc_day_count == 30
    assert ledger.status == (
        confirmation.ConfirmationStatus.MINIMUM_MET_FORMAL_EVALUATION_REQUIRED
    )
    assert ledger.ready_for_formal_evaluation
    assert not ledger.learning_algorithm_oof_evidence_counted
    assert not ledger.exact_final_artifact_oof_available
    assert not ledger.research_supported
    assert not ledger.action_authorized
    assert not ledger.live_policy_authorized

    loaded = confirmation.load_confirmation_ledger(path)
    assert loaded.ledger_sha256 == ledger.ledger_sha256
    assert loaded.session_count == 30


def test_zero_action_session_does_not_count_as_active_day(tmp_path: Path) -> None:
    contract = _contract()
    path = tmp_path / "confirmation-ledger.json"
    ledger = confirmation.append_confirmation_session(
        path,
        contract=contract,
        session=_session(contract, candidate_actions=0),
    )

    assert ledger.session_count == 1
    assert ledger.active_utc_day_count == 0
    assert ledger.status == confirmation.ConfirmationStatus.PENDING_MINIMUM_ACTIVE_DAYS


def test_tampered_ledger_hash_fails_closed(tmp_path: Path) -> None:
    contract = _contract()
    path = tmp_path / "confirmation-ledger.json"
    confirmation.append_confirmation_session(
        path,
        contract=contract,
        session=_session(contract),
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["active_utc_day_count"] = 29
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(confirmation.ConfirmationError):
        confirmation.load_confirmation_ledger(path)
