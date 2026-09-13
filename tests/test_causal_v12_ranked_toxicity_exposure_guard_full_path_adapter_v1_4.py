from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest

from execution.chunked_parquet_journal import (
    ChunkedParquetJournalWriter,
    iter_chunked_parquet_journal,
)
from research.families.f09_campaign_action_uplift.audit.causal_v12_ranked_toxicity_exposure_guard import (
    CANDIDATE_ACTION,
    deterministic_campaign_side_assignment,
)
from research.families.f09_campaign_action_uplift.audit.causal_v12_ranked_toxicity_exposure_guard_full_path_adapter import (
    FROZEN_RANDOM_SEEDS,
    AdapterContractViolation,
    BaselineShadowSnapshot,
    CanonicalPredictionBucket,
)
from research.families.f09_campaign_action_uplift.audit.causal_v12_ranked_toxicity_exposure_guard_full_path_adapter_v1_4 import (
    RankedToxicityGuardFullPathAdapterV14,
    stable_campaign_opportunity_id,
)

DAY = "2026-08-03"
MODEL_SHA256 = "a" * 64
THRESHOLD_SHA256 = "b" * 64


def _day_start_ms() -> int:
    return int(
        datetime.fromisoformat(DAY)
        .replace(tzinfo=timezone.utc)
        .timestamp()
        * 1000
    )


def _prediction(score: float = 0.9) -> CanonicalPredictionBucket:
    bucket = _day_start_ms() + 10_000
    return CanonicalPredictionBucket(
        utc_day=DAY,
        prediction_bucket_ts_ms=bucket,
        feature_ready_ts_ms=bucket + 100,
        decision_ts_ms=bucket + 200,
        score=score,
        model_sha256=MODEL_SHA256,
    )


def _shadow(decision_id: str, decision_ts_ns: int, *, eligible: bool) -> BaselineShadowSnapshot:
    return BaselineShadowSnapshot(
        decision_id=decision_id,
        utc_day=DAY,
        decision_ts_ns=decision_ts_ns,
        side="BUY",
        role="opener",
        baseline_eligible=eligible,
        exposure_increasing=True,
        can_post=eligible,
        allow_exposure_increase=eligible,
        active_exposure_order_id="",
        quote_price=100.0,
        quote_quantity=0.001,
        blocker_fingerprint="none",
        policy_fingerprint="baseline-v5",
    )


def _adapter(**kwargs) -> RankedToxicityGuardFullPathAdapterV14:
    adapter = RankedToxicityGuardFullPathAdapterV14(
        side="BUY",
        random_seed=FROZEN_RANDOM_SEEDS["BUY"],
        frozen_model_sha256=MODEL_SHA256,
        **kwargs,
    )
    adapter.register_daily_threshold(
        utc_day=DAY,
        threshold=0.8,
        source_identity_sha256=THRESHOLD_SHA256,
    )
    return adapter


def _candidate_identity(prefix: str = "prospective") -> str:
    for ordinal in range(1, 10_000):
        identity = f"{prefix}-{ordinal}"
        assignment = deterministic_campaign_side_assignment(
            seed=FROZEN_RANDOM_SEEDS["BUY"],
            utc_day=DAY,
            side="BUY",
            campaign_opportunity_id=stable_campaign_opportunity_id(
                side="BUY",
                prospective_campaign_side_id=identity,
            ),
            candidate_probability=0.5,
        )
        if assignment.action == CANDIDATE_ACTION:
            return identity
    raise AssertionError("failed to find deterministic candidate identity")


def test_one_prediction_supports_multiple_quote_decisions() -> None:
    adapter = _adapter()
    prediction = _prediction()
    adapter.on_prediction_bucket(prediction)
    prospective_id = _candidate_identity()

    first = _shadow(
        "d-1",
        prediction.decision_ts_ms * 1_000_000,
        eligible=False,
    )
    first_directive = adapter.on_quote_decision(
        control_shadow=first,
        candidate_shadow=first,
        prospective_campaign_side_id=prospective_id,
    )
    assert first_directive.allow_exposure_submission
    adapter.observe_final_quote_action(
        decision_id="d-1",
        role="opener",
        exposure_increasing=True,
        baseline_action="pause",
        candidate_action="pause",
        baseline_price=100.0,
        candidate_price=100.0,
        baseline_quantity=0.001,
        candidate_quantity=0.001,
        event_ts_ns=first.decision_ts_ns,
    )

    second = _shadow(
        "d-2",
        first.decision_ts_ns + 100_000_000,
        eligible=True,
    )
    second_directive = adapter.on_quote_decision(
        control_shadow=second,
        candidate_shadow=second,
        prospective_campaign_side_id=prospective_id,
    )
    assert second_directive.action == CANDIDATE_ACTION
    assert not second_directive.allow_exposure_submission
    adapter.observe_final_quote_action(
        decision_id="d-2",
        role="opener",
        exposure_increasing=True,
        baseline_action="place",
        candidate_action="pause",
        baseline_price=100.0,
        candidate_price=100.0,
        baseline_quantity=0.001,
        candidate_quantity=0.001,
        event_ts_ns=second.decision_ts_ns,
    )

    audit = adapter.contract_audit()
    assert audit["quote_decision_count"] == 2
    assert audit["held_prediction_reuse_count"] == 1
    assert audit["assignment_count"] == 1


def test_untreated_assignment_does_not_suppress_candidate_reducing_quote() -> None:
    adapter = _adapter()
    prediction = _prediction()
    adapter.on_prediction_bucket(prediction)
    prospective_id = _candidate_identity("role-divergence")
    control = _shadow(
        "role-divergence",
        prediction.decision_ts_ms * 1_000_000,
        eligible=True,
    )
    candidate = replace(
        control,
        role="reducing",
        exposure_increasing=False,
    )

    directive = adapter.on_quote_decision(
        control_shadow=control,
        candidate_shadow=candidate,
        prospective_campaign_side_id=prospective_id,
    )

    assert directive.action == CANDIDATE_ACTION
    assert directive.allow_exposure_submission is True
    assert directive.request_cancel_once is False
    adapter.observe_final_quote_action(
        decision_id=control.decision_id,
        role="reducing",
        exposure_increasing=False,
        baseline_action="place",
        candidate_action="place",
        baseline_price=100.0,
        candidate_price=100.0,
        baseline_quantity=0.001,
        candidate_quantity=0.001,
        event_ts_ns=control.decision_ts_ns,
    )
    assert adapter.contract_audit()["zero_tolerance_passed"] is True


def test_duplicate_prediction_and_unknown_terminal_fail_immediately() -> None:
    adapter = _adapter()
    prediction = _prediction()
    adapter.on_prediction_bucket(prediction)
    with pytest.raises(AdapterContractViolation, match="duplicate"):
        adapter.on_prediction_bucket(prediction)

    adapter = _adapter()
    adapter.on_order_submitted(
        order_id="buy-1",
        initial_quantity=0.001,
        visibility_ts_ns=1,
        exposure_increasing=True,
    )
    with pytest.raises(AdapterContractViolation, match="unsupported"):
        adapter.on_exchange_terminal(
            order_id="buy-1",
            reason="mystery_terminal",
            visibility_ts_ns=2,
            exchange_ts_ns=2,
        )
    assert adapter.order_snapshot("buy-1")["phase"] == "SUBMITTED"


def test_stable_identity_is_checkpoint_order_independent() -> None:
    identity = _candidate_identity("stable")
    prediction = _prediction()

    adapter_a = _adapter()
    adapter_a.on_prediction_bucket(prediction)
    shadow_a = _shadow(
        "a",
        prediction.decision_ts_ms * 1_000_000,
        eligible=True,
    )
    action_a = adapter_a.on_quote_decision(
        control_shadow=shadow_a,
        candidate_shadow=shadow_a,
        prospective_campaign_side_id=identity,
    ).action

    adapter_b = _adapter()
    adapter_b.on_prediction_bucket(prediction)
    shadow_b = _shadow(
        "b",
        prediction.decision_ts_ms * 1_000_000,
        eligible=True,
    )
    action_b = adapter_b.on_quote_decision(
        control_shadow=shadow_b,
        candidate_shadow=shadow_b,
        prospective_campaign_side_id=identity,
    ).action

    assert action_a == action_b == CANDIDATE_ACTION


def test_streaming_journal_does_not_retain_full_rows(tmp_path) -> None:
    writer = ChunkedParquetJournalWriter(
        tmp_path / "journal",
        journal_id="ranked-toxicity-v1.4",
        chunk_rows=1,
    )
    adapter = _adapter(journal_writer=writer, retain_journal=False)
    adapter.on_prediction_bucket(_prediction())
    manifest = adapter.close_journal()

    assert manifest is not None
    assert manifest["row_count"] == 1
    assert adapter.contract_audit()["journal_rows_retained_in_memory"] == 0
    with pytest.raises(RuntimeError, match="not retained"):
        adapter.journal()
    rows = list(
        iter_chunked_parquet_journal(tmp_path / "journal" / "manifest.json")
    )
    assert rows[0]["event_type"] == "prediction_bucket"
