from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence

import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_nested_oof_v1 as nested,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_repeated_policy_backend_v1 as backend,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_v1 as offline,
)

_DAYS = ("2026-09-01", "2026-09-02")


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("ascii")).hexdigest()


def _candidate(
    name: str, *, training_days: tuple[str, ...] = ("2026-08-20",)
) -> nested.FittedCandidate:
    policy_sha256 = (
        offline.ACTIVE_OWNER_POLICY_SHA256
        if name == "B0_CURRENT_EXACT"
        else _sha(f"policy::{name}")
    )
    return nested.FittedCandidate(
        ladder_name=name,
        side="SELL",
        policy=None,
        selected_profile=f"profile::{name}",
        training_days=training_days,
        training_row_sha256=_sha(f"training::{name}"),
        policy_payload={"kind": "synthetic", "name": name},
        policy_sha256=policy_sha256,
        fit_audit={},
        feature_pool_audit=None,
    )


def _request(
    candidate: nested.FittedCandidate, *, stage: str = "outer_oof"
) -> nested.EvaluationRequest:
    return nested.EvaluationRequest(
        candidate=candidate,
        side="SELL",
        days=_DAYS,
        fold_id="outer1",
        stage=stage,  # type: ignore[arg-type]
        panel_role=offline.PANEL_ROLE,
    )


def _valid_rows(request: nested.EvaluationRequest) -> pd.DataFrame:
    is_b0 = request.candidate.ladder_name == "B0_CURRENT_EXACT"
    candidate_value = 1.0 if not is_b0 else 0.5
    rows: list[dict[str, object]] = []
    for day in request.days:
        rows.append(
            {
                "utc_day": day,
                "side": request.side,
                "panel_role": request.panel_role,
                "candidate_terminal_value_usdc": candidate_value,
                "exact_owner_terminal_value_usdc": 0.5,
                "point_identified": True,
                "policy_assignment_count": 1,
                "nonbaseline_action_count": 0 if is_b0 else 1,
                "feature_ready_active_treatment_events": 1,
                "repeated_sequential_policy": True,
                "one_shot_effect_aggregation_used": False,
                "exact_current_owner_row_wise_baseline": True,
                "candidate_executed_policy_sha256": (
                    request.candidate.expected_executed_policy_sha256
                ),
                "exact_owner_executed_policy_sha256": offline.ACTIVE_OWNER_POLICY_SHA256,
                "paired_replay_receipt_sha256": _sha(f"paired::{request.request_sha256}::{day}"),
                "candidate_target_side": request.side,
                "same_market_source": True,
                "common_random_source": True,
                "arm_local_state": True,
                "common_row_count": 1,
                "common_campaign_count": 1,
                "candidate_closed_campaign_value_usdc": candidate_value,
                "exact_owner_closed_campaign_value_usdc": 0.5,
                "candidate_campaign_q10_usdc": -0.1,
                "exact_owner_campaign_q10_usdc": -0.1,
                "candidate_campaign_cvar10_usdc": -0.2,
                "exact_owner_campaign_cvar10_usdc": -0.2,
                "candidate_inventory_time_btc_s": 1.0,
                "exact_owner_inventory_time_btc_s": 1.0,
                "candidate_max_abs_inventory_btc": 0.001,
                "exact_owner_max_abs_inventory_btc": 0.001,
                "candidate_fill_count": 1,
                "exact_owner_fill_count": 1,
                "candidate_negative_terminal_rate": 0.0,
                "exact_owner_negative_terminal_rate": 0.0,
                "candidate_campaign_mae_usdc": 0.1,
                "exact_owner_campaign_mae_usdc": 0.1,
                "candidate_repair_event_rate": 0.0,
                "exact_owner_repair_event_rate": 0.0,
                "candidate_mean_repair_time_s": 1.0,
                "exact_owner_mean_repair_time_s": 1.0,
                "candidate_censoring_rate": 0.0,
                "exact_owner_censoring_rate": 0.0,
                "action_count::CONTROL_85N": 1,
                "role_count::add": 1,
                "consecutive_units_count::1": 1,
                "fallback_count::none": 1,
            }
        )
    return pd.DataFrame(rows)


class _DirectBatchEvaluator:
    def __init__(self, *, mode: str = "valid") -> None:
        self.mode = mode
        self.single_requests: list[nested.EvaluationRequest] = []
        self.batch_requests: list[tuple[nested.EvaluationRequest, ...]] = []

    def __call__(self, request: nested.EvaluationRequest) -> pd.DataFrame:
        self.single_requests.append(request)
        return _valid_rows(request)

    def evaluate_many(
        self,
        requests: Sequence[nested.EvaluationRequest],
    ) -> tuple[nested.SequentialEvaluationResult, ...]:
        request_tuple = tuple(requests)
        self.batch_requests.append(request_tuple)
        results = [
            nested.SequentialEvaluationResult(
                request_sha256=request.request_sha256,
                rows=_valid_rows(request),
            )
            for request in reversed(request_tuple)
        ]
        if self.mode == "missing":
            results.pop()
        elif self.mode == "duplicate":
            results[-1] = results[0]
        elif self.mode == "mismatched_rows":
            results[0] = nested.SequentialEvaluationResult(
                request_sha256=results[0].request_sha256,
                rows=_valid_rows(request_tuple[0]),
            )
        return tuple(results)


def test_dependency_ready_outer_wave_batches_and_restores_candidate_order() -> None:
    candidates = {
        "B0_CURRENT_EXACT": _candidate("B0_CURRENT_EXACT", training_days=()),
        "E1_FULL_EMA_BANK": _candidate("E1_FULL_EMA_BANK"),
        "ACTION_MATCHED_CONTROLS::E1_FULL_EMA_BANK": _candidate(
            "ACTION_MATCHED_CONTROLS::E1_FULL_EMA_BANK"
        ),
        nested.CONTINUOUS_COMPARATOR: _candidate(nested.CONTINUOUS_COMPARATOR),
    }
    waves = nested._outer_evaluation_waves(candidates)

    assert [[name for name, _candidate_value in wave] for wave in waves] == [
        ["B0_CURRENT_EXACT", "E1_FULL_EMA_BANK", nested.CONTINUOUS_COMPARATOR],
        ["ACTION_MATCHED_CONTROLS::E1_FULL_EMA_BANK"],
    ]

    evaluator = _DirectBatchEvaluator()
    collected = nested._evaluate_outer_candidates(
        evaluator,
        candidates,
        side="SELL",
        days=_DAYS,
        fold_id="outer1",
        panel_role=offline.PANEL_ROLE,
    )

    assert evaluator.single_requests == []
    assert len(evaluator.batch_requests) == 2
    assert tuple(collected) == (
        "B0_CURRENT_EXACT",
        "E1_FULL_EMA_BANK",
        "ACTION_MATCHED_CONTROLS::E1_FULL_EMA_BANK",
        nested.CONTINUOUS_COMPARATOR,
    )
    assert all(set(rows["candidate_name"]) == {name} for name, rows in collected.items())


def test_serial_fallback_preserves_order_and_inner_uses_single_request() -> None:
    class _SerialEvaluator:
        def __init__(self) -> None:
            self.requests: list[nested.EvaluationRequest] = []

        def __call__(self, request: nested.EvaluationRequest) -> pd.DataFrame:
            self.requests.append(request)
            return _valid_rows(request)

    candidates = (
        ("B0_CURRENT_EXACT", _candidate("B0_CURRENT_EXACT", training_days=())),
        ("E1_FULL_EMA_BANK", _candidate("E1_FULL_EMA_BANK")),
    )
    serial = _SerialEvaluator()
    results = nested._evaluate_outer_candidates(
        serial,
        dict(candidates),
        side="SELL",
        days=_DAYS,
        fold_id="outer1",
        panel_role=offline.PANEL_ROLE,
    )
    assert list(results) == [name for name, _candidate_value in candidates]
    assert [request.candidate.ladder_name for request in serial.requests] == [
        name for name, _candidate_value in candidates
    ]

    batch = _DirectBatchEvaluator()
    nested._evaluate(
        batch,
        _candidate("E1_FULL_EMA_BANK"),
        side="SELL",
        days=_DAYS,
        fold_id="inner1",
        stage="inner_oof",
        panel_role=offline.PANEL_ROLE,
    )
    assert len(batch.single_requests) == 1
    assert batch.batch_requests == []


def test_batch_request_census_and_row_mismatch_fail_closed() -> None:
    first = _candidate("E1_FULL_EMA_BANK")
    second = _candidate("E2_DIRECTIONAL_EMA")
    common = {
        "side": "SELL",
        "days": _DAYS,
        "fold_id": "outer1",
        "stage": "outer_oof",
        "panel_role": offline.PANEL_ROLE,
    }

    with pytest.raises(nested.NestedOofExecutionError, match="duplicate request"):
        nested._evaluate_many(
            _DirectBatchEvaluator(),
            ((first.ladder_name, first), (first.ladder_name, first)),
            **common,
        )
    for mode, message in (
        ("missing", "request census drifted"),
        ("duplicate", "duplicate request"),
        ("mismatched_rows", "candidate executed-policy identity drifted"),
    ):
        with pytest.raises(nested.NestedOofExecutionError, match=message):
            nested._evaluate_many(
                _DirectBatchEvaluator(mode=mode),
                ((first.ladder_name, first), (second.ladder_name, second)),
                **common,
            )


def _mechanics() -> backend.OutcomeBlindMechanics:
    bindings = backend.FormalExecutionBindings(
        execution_manifest_sha256="1" * 64,
        source_manifest_sha256="2" * 64,
        panel_manifest_sha256="3" * 64,
        fold_manifest_sha256="4" * 64,
        nested_fold_manifest_sha256="5" * 64,
        exact_owner_policy_sha256=offline.ACTIVE_OWNER_POLICY_SHA256,
        exact_owner_predicate_bundle_sha256=offline.ACTIVE_PREDICATE_BUNDLE_SHA256,
        exact_owner_private_config_sha256=offline.ACTIVE_PRIVATE_CONFIG_SHA256,
    )
    replay_inputs = pd.DataFrame(
        {
            "utc_day": [*_DAYS, *_DAYS],
            "side": ["SELL", "SELL", "BUY", "BUY"],
            "opportunity_id": ["s1", "s2", "b1", "b2"],
        }
    )
    return backend.OutcomeBlindMechanics(
        panel=None,  # type: ignore[arg-type]
        replay_inputs=replay_inputs,
        selected_days=_DAYS,
        fold_manifest=None,  # type: ignore[arg-type]
        bindings=bindings,
        file_sha256={},
        mechanics_receipt_sha256="6" * 64,
    )


def _adapter_result(
    adapter_identity: str,
    adapter_sha256: str,
    request: backend.CanonicalSequentialReplayBatchRequest,
) -> backend.CanonicalSequentialReplayResult:
    rows = _valid_rows(request.replay_request.evaluation_request)
    receipt = backend.build_sequential_replay_receipt(
        request.replay_request,
        adapter_identity=adapter_identity,
        adapter_artifact_sha256=adapter_sha256,
    )
    bindings = request.replay_request.bindings
    rows["sequential_batch_receipt_sha256"] = receipt["receipt_sha256"]
    rows["execution_manifest_sha256"] = bindings.execution_manifest_sha256
    rows["source_manifest_sha256"] = bindings.source_manifest_sha256
    rows["panel_manifest_sha256"] = bindings.panel_manifest_sha256
    rows["fold_manifest_sha256"] = bindings.fold_manifest_sha256
    return backend.CanonicalSequentialReplayResult(rows=rows, receipt=receipt)


class _AdapterBase:
    identity = backend.CANONICAL_REPLAY_ADAPTER_IDENTITY
    artifact_sha256 = "a" * 64

    def preflight_formal_panel_schema(self, **_kwargs: object) -> Mapping[str, object]:
        raise AssertionError

    def build_search_contract(self, _mechanics: object) -> object:
        raise AssertionError

    def preflight_formal_economics(self, _mechanics: object) -> Mapping[str, object]:
        raise AssertionError

    def run_exact_owner_one_day_mechanics(self, _mechanics: object) -> Mapping[str, object]:
        raise AssertionError

    def generate_outer_train_one_shot_labels(self, _request: object, _inputs: object) -> object:
        raise AssertionError


class _BulkAdapter(_AdapterBase):
    def __init__(self, *, mode: str = "valid") -> None:
        self.mode = mode
        self.single_calls = 0
        self.batch_calls: list[tuple[backend.CanonicalSequentialReplayBatchRequest, ...]] = []

    def evaluate_repeated_policy(self, _request: object, _inputs: object) -> object:
        self.single_calls += 1
        raise AssertionError("single replay must not run when bulk capability is present")

    def evaluate_repeated_policies(
        self,
        requests: Sequence[backend.CanonicalSequentialReplayBatchRequest],
    ) -> tuple[backend.CanonicalSequentialReplayBatchResult, ...]:
        request_tuple = tuple(requests)
        self.batch_calls.append(request_tuple)
        results = [
            backend.CanonicalSequentialReplayBatchResult(
                request_sha256=request.request_sha256,
                result=_adapter_result(self.identity, self.artifact_sha256, request),
            )
            for request in reversed(request_tuple)
        ]
        if self.mode == "missing":
            results.pop()
        elif self.mode == "duplicate":
            results[-1] = results[0]
        return tuple(results)


class _SingleAdapter(_AdapterBase):
    def __init__(self) -> None:
        self.single_calls: list[str] = []

    def evaluate_repeated_policy(
        self,
        replay_request: backend.CanonicalSequentialReplayRequest,
        replay_inputs: pd.DataFrame,
    ) -> backend.CanonicalSequentialReplayResult:
        request = backend.CanonicalSequentialReplayBatchRequest(
            request_sha256=replay_request.evaluation_request.request_sha256,
            replay_request=replay_request,
            replay_inputs=replay_inputs,
            expected_receipt={},
        )
        self.single_calls.append(request.request_sha256)
        return _adapter_result(self.identity, self.artifact_sha256, request)


def test_canonical_backend_bulk_call_binds_and_orders_results_and_receipts() -> None:
    adapter = _BulkAdapter()
    evaluator = backend.CanonicalSequentialEvaluator(_mechanics(), adapter)
    requests = (
        _request(_candidate("E1_FULL_EMA_BANK")),
        _request(_candidate("E2_DIRECTIONAL_EMA")),
    )

    results = evaluator.evaluate_many(requests)

    assert adapter.single_calls == 0
    assert len(adapter.batch_calls) == 1
    assert [result.request_sha256 for result in results] == [
        request.request_sha256 for request in requests
    ]
    assert [receipt["candidate_policy_sha256"] for receipt in evaluator.receipts] == [
        request.candidate.expected_executed_policy_sha256 for request in requests
    ]
    assert [
        batch_request.replay_request.replay_input_sha256 for batch_request in adapter.batch_calls[0]
    ] == [
        backend._frame_sha256(batch_request.replay_inputs)
        for batch_request in adapter.batch_calls[0]
    ]


def test_canonical_backend_hides_bulk_capability_and_falls_back_to_single_calls() -> None:
    adapter = _SingleAdapter()
    evaluator = backend.CanonicalSequentialEvaluator(_mechanics(), adapter)
    candidates = (
        ("E1_FULL_EMA_BANK", _candidate("E1_FULL_EMA_BANK")),
        ("E2_DIRECTIONAL_EMA", _candidate("E2_DIRECTIONAL_EMA")),
    )

    assert getattr(evaluator, "evaluate_many", None) is None
    results = nested._evaluate_many(
        evaluator,
        candidates,
        side="SELL",
        days=_DAYS,
        fold_id="outer1",
        stage="outer_oof",
        panel_role=offline.PANEL_ROLE,
    )

    assert [name for name, _rows in results] == [name for name, _candidate_value in candidates]
    assert len(adapter.single_calls) == 2
    assert len(evaluator.receipts) == 2


@pytest.mark.parametrize("mode", ("missing", "duplicate"))
def test_canonical_backend_bulk_failure_admits_no_receipts(mode: str) -> None:
    adapter = _BulkAdapter(mode=mode)
    evaluator = backend.CanonicalSequentialEvaluator(_mechanics(), adapter)
    requests = (
        _request(_candidate("E1_FULL_EMA_BANK")),
        _request(_candidate("E2_DIRECTIONAL_EMA")),
    )

    with pytest.raises(backend.OfflineRepeatedPolicyBackendError):
        evaluator.evaluate_many(requests)
    assert evaluator.receipts == []
