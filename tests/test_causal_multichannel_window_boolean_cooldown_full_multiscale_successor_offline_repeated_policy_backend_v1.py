from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_nested_oof_v1 as nested,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_orchestrator_v1 as orchestrator,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_repeated_policy_backend_v1 as backend,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_v1 as offline,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_nested_oof import (
    CONTROL_ACTION,
    duration_vocabulary,
)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _parquet_binding(path: Path) -> dict[str, object]:
    parquet = pq.ParquetFile(path)
    schema = parquet.schema_arrow
    return {
        "path": str(path),
        "sha256": backend._file_sha256(path),
        "size_bytes": path.stat().st_size,
        "rows": parquet.metadata.num_rows,
        "schema": {
            "columns": list(schema.names),
            "types": [str(schema.field(index).type) for index in range(len(schema))],
        },
    }


def _fold_manifest(days: tuple[str, ...]) -> dict[str, object]:
    rows = []
    for position, train_end in enumerate((10, 15, 20, 25), start=1):
        rows.append(
            {
                "fold": position,
                "train_days": list(days[:train_end]),
                "test_days": list(days[train_end : train_end + 5]),
                "purge_and_washout_required": True,
            }
        )
    payload: dict[str, object] = {
        "schema_version": f"{offline.IDENTITY}.fold_manifest.v1",
        "panel_role": offline.PANEL_ROLE,
        "selection_sha256": "1" * 64,
        "active_days": list(days),
        "outer_folds": rows,
    }
    payload["fold_manifest_sha256"] = backend._document_sha256(
        payload, "fold_manifest_sha256"
    )
    return payload


def _bundle_fixture(
    tmp_path: Path,
    *,
    economic_file_role: bool = False,
    economic_column: bool = False,
    b0_drift: bool = False,
    replay_side: bool = True,
) -> orchestrator.FormalOfflineBundle:
    days = tuple(pd.date_range("2026-06-27", periods=30, freq="D").strftime("%Y-%m-%d"))
    identifiers = [f"{day}:{side}" for day in days for side in ("BUY", "SELL")]
    day_column = [day for day in days for _side in ("BUY", "SELL")]
    side_column = [side for _day in days for side in ("BUY", "SELL")]
    slots = np.arange(len(identifiers), dtype=np.int64)
    metadata = pd.DataFrame(
        {
            "utc_day": day_column,
            "opportunity_id": identifiers,
            "panel_role": offline.PANEL_ROLE,
            "side": side_column,
            "campaign_cluster_id": [f"campaign:{value}" for value in identifiers],
            "assignment_ts_ns": 1_000_000_000 + slots * 1_000,
            "observation_end_ts_ns": 1_000_000_500 + slots * 1_000,
            "fill_visible_ts_ns": 2_000_000_000 + slots * 1_000_000,
            "baseline_duration_ms": 85_000 + slots,
            "role_at_fill": ["opener" if side == "BUY" else "add" for side in side_column],
            "inventory_after_fill_btc": slots.astype(float) / 1_000.0,
        }
    )
    boolean = pd.DataFrame(
        {
            "utc_day": day_column,
            "opportunity_id": identifiers,
            "predicate::ema_pair_h1s_h2s:ordering": (slots % 3 - 1).astype(np.int8),
        }
    )
    continuous = pd.DataFrame(
        {
            "utc_day": day_column,
            "opportunity_id": identifiers,
            "continuous::ema_distance": slots.astype(float) / 100.0,
        }
    )
    owner = pd.DataFrame(
        {
            "utc_day": day_column,
            "opportunity_id": identifiers,
            "exact_owner_action": [
                CONTROL_ACTION if side == "BUY" else "FIXED_166S" for side in side_column
            ],
        }
    )
    replay = pd.DataFrame(
        {
            "utc_day": day_column,
            "opportunity_id": identifiers,
            "replay_input_receipt_sha256": [
                hashlib.sha256(value.encode("ascii")).hexdigest() for value in identifiers
            ],
            "market_tape_binding": [f"tape:{day}" for day in day_column],
        }
    )
    if replay_side:
        replay["side"] = side_column
    if economic_column:
        replay["candidate_terminal_value_usdc"] = 0.0
    frames = {
        "metadata": metadata,
        "boolean_features": boolean,
        "continuous_features": continuous,
        "exact_owner_actions": owner,
        "replay_inputs": replay,
    }
    paths: dict[str, Path] = {}
    bindings: dict[str, object] = {}
    for role, frame in frames.items():
        path = tmp_path / f"{role}.parquet"
        frame.to_parquet(path, index=False)
        paths[role] = path
        bindings[role] = _parquet_binding(path)
    if economic_file_role:
        economic_path = tmp_path / "action_outcomes.parquet"
        pd.DataFrame({"action_outcome": [0.0]}).to_parquet(economic_path, index=False)
        bindings["action_outcomes"] = _parquet_binding(economic_path)

    folds = _fold_manifest(days)
    source: dict[str, object] = {
        "identity": offline.IDENTITY,
        "selected_days": list(days),
        "fold_manifest": folds,
        "canonical_manifest_sha256": "2" * 64,
        "exact_current_owner_baseline": {
            "policy_sha256": offline.ACTIVE_OWNER_POLICY_SHA256,
        },
    }
    nested_folds = offline.derive_bound_nested_fold_manifest(source)
    source_path = tmp_path / "source.json"
    _write_json(source_path, source)
    panel: dict[str, object] = {
        "schema_version": orchestrator.PANEL_SCHEMA_VERSION,
        "identity": offline.IDENTITY,
        "selected_days": list(days),
        "panel_role": offline.PANEL_ROLE,
        "queue_identity": offline.QUEUE_IDENTITY,
        "economic_outcomes_present": False,
        "one_shot_training_labels_precomputed": False,
        "outer_train_label_generation_required": True,
        "one_shot_effect_aggregation_used": False,
        "repeated_sequential_policy_required": True,
        "exact_current_owner_policy_sha256": (
            "0" * 64 if b0_drift else offline.ACTIVE_OWNER_POLICY_SHA256
        ),
        "exact_current_predicate_bundle_sha256": offline.ACTIVE_PREDICATE_BUNDLE_SHA256,
        "exact_current_private_config_sha256": offline.ACTIVE_PRIVATE_CONFIG_SHA256,
        "predicate_view": {
            "mode": "preexpanded_bound_panel_v1",
            "boolean_features_sha256": bindings["boolean_features"]["sha256"],
            "expanded_predicate_count": len(boolean.columns) - 2,
            "economic_outcomes_read": False,
        },
        "files": bindings,
    }
    panel["canonical_panel_manifest_sha256"] = backend._document_sha256(
        panel, "canonical_panel_manifest_sha256"
    )
    panel_path = tmp_path / "panel.json"
    _write_json(panel_path, panel)
    execution: dict[str, object] = {
        "schema_version": orchestrator.SCHEMA_VERSION,
        "identity": orchestrator.IDENTITY,
        "backend": {
            "module": orchestrator.CANONICAL_BACKEND_MODULE,
            "function": orchestrator.CANONICAL_BACKEND_FUNCTION,
            "custom_evaluator_allowed": False,
        },
        "execution_contract": {
            "control": "B0_CURRENT_EXACT",
            "sequential_repeated_policy": True,
            "one_shot_effect_aggregation_used": False,
            "outer_test_candidate_freeze_required": True,
            "action_alpha_v1_required": True,
        },
        "executor": orchestrator.formal_executor_contract(),
        "cpp_one_shot_qualification": orchestrator._cpp_qualification_contract(),
        "fold_manifest_sha256": folds["fold_manifest_sha256"],
        "nested_fold_manifest": nested_folds,
        "nested_fold_manifest_sha256": nested_folds[
            "nested_fold_manifest_sha256"
        ],
        "source_manifest": {
            "path": str(source_path),
            "sha256": backend._file_sha256(source_path),
        },
        "panel_manifest": {
            "path": str(panel_path),
            "sha256": backend._file_sha256(panel_path),
        },
    }
    execution["canonical_execution_manifest_sha256"] = backend._document_sha256(
        execution, "canonical_execution_manifest_sha256"
    )
    execution_path = tmp_path / "execution.json"
    _write_json(execution_path, execution)
    qualification = {
        "execution_manifest_sha256": execution["canonical_execution_manifest_sha256"],
        "public_base_commit": execution.get("public_base_commit"),
        "annotated_tag": execution.get("annotated_tag"),
        "opportunity_count": 2,
        "arm_count": 16,
        "source_hashes": orchestrator._current_cpp_qualification_source_hashes(),
    }
    receipt: dict[str, object] = {
        "identity": orchestrator.CPP_QUALIFICATION_IDENTITY,
        "status": "passed_real_day_all_opportunity_all_arm_lockstep",
        "qualification_contract": qualification,
        "qualification_sha256": backend._canonical_sha256(qualification),
        "opportunity_count": 2,
        "arm_count": 16,
        "zero_mismatch_arm_count": 16,
        "cpp_one_shot_formal_authorized": True,
        "python_sequential_engine_remains_authoritative": True,
        "economic_values_persisted": False,
        "economic_values_used_for_selection": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    receipt["canonical_receipt_sha256"] = backend._document_sha256(
        receipt, "canonical_receipt_sha256"
    )
    _write_json(
        execution_path.parent / orchestrator.CPP_QUALIFICATION_RECEIPT_NAME,
        receipt,
    )
    return orchestrator._new_formal_offline_bundle(
        execution_manifest_path=execution_path,
        execution_manifest=execution,
        source_manifest_path=source_path,
        source_manifest=source,
        panel_manifest_path=panel_path,
        panel_manifest=panel,
        panel_files=paths,
        repository_root=tmp_path,
    )


class _NeverCalledAdapter:
    identity = backend.CANONICAL_REPLAY_ADAPTER_IDENTITY
    artifact_sha256 = "a" * 64

    def __init__(self) -> None:
        self.label_called = False
        self.sequential_called = False

    def preflight_formal_panel_schema(
        self,
        *,
        metadata_columns,
        replay_input_columns,
        exact_owner_action_columns,
    ) -> dict[str, object]:
        assert metadata_columns
        assert replay_input_columns
        assert exact_owner_action_columns
        return {
            "identity": self.identity,
            "status": backend.FORMAL_PANEL_SCHEMA_READY_STATUS,
            "adapter_artifact_sha256": self.artifact_sha256,
            "missing_canonical_fields": [],
            "permissions": {
                "economic_outcomes_read": False,
                "validation_read": False,
                "sealed_holdout_read": False,
                "action_authorized": False,
                "live_authorized": False,
            },
        }

    def preflight_formal_economics(
        self,
        _mechanics: backend.OutcomeBlindMechanics,
    ) -> dict[str, object]:
        return {
            "identity": self.identity,
            "status": backend.MECHANICS_READY_STATUS,
            "adapter_artifact_sha256": self.artifact_sha256,
            "all_fold_zero_economic_contract_walk": {
                "status": "all_fold_zero_economic_contract_walk_complete",
                "side_count": 2,
                "outer_fold_count": 4,
                "inner_fold_count": 12,
                "side_outer_contract_count": 8,
                "side_inner_contract_count": 24,
                "fold_day_slots_checked": 1,
                "candidate_ladder_count": len(nested.SUCCESSOR_CANDIDATE_LADDER),
                "continuous_comparator_bound": nested.CONTINUOUS_COMPARATOR,
                "global_worker_tokens": orchestrator.EXECUTOR_GLOBAL_WORKER_TOKENS,
                "mmap_acceleration_bound": True,
                "one_shot_topology": dict(orchestrator.EXECUTOR_ONE_SHOT_TOPOLOGY),
                "day_input_materialization_workers": (
                    orchestrator.EXECUTOR_DAY_INPUT_MATERIALIZATION_WORKERS
                ),
                "economic_outcomes_read": False,
            },
            "permissions": {
                "economic_outcomes_read": False,
                "validation_read": False,
                "sealed_holdout_read": False,
                "action_authorized": False,
                "live_authorized": False,
            },
        }

    def build_search_contract(self, _mechanics: backend.OutcomeBlindMechanics):
        raise AssertionError("search contract must not be requested in this test")

    def run_exact_owner_one_day_mechanics(self, _mechanics):
        raise AssertionError("one-day mechanics must not be requested in this test")

    def generate_outer_train_one_shot_labels(self, _request, _replay_inputs):
        self.label_called = True
        raise AssertionError("outer-test request reached the replay adapter")

    def evaluate_repeated_policy(self, _request, _replay_inputs):
        self.sequential_called = True
        raise AssertionError("invalid sequential request reached replay")


@pytest.mark.parametrize("empty_blockers", (None, [], ()))
def test_ready_adapter_preflight_accepts_only_empty_blocker_census(
    tmp_path: Path,
    empty_blockers: object,
) -> None:
    mechanics = backend.load_outcome_blind_mechanics(_bundle_fixture(tmp_path))

    class _ReadyAdapter(_NeverCalledAdapter):
        def preflight_formal_economics(self, _mechanics):
            result = dict(super().preflight_formal_economics(_mechanics))
            if empty_blockers is not None:
                result["missing_canonical_fields"] = empty_blockers
            return result

    result = backend._preflight_adapter(mechanics, _ReadyAdapter())

    assert result["status"] == backend.MECHANICS_READY_STATUS


@pytest.mark.parametrize("invalid_blockers", (["field"], "field", {"field"}))
def test_ready_adapter_preflight_rejects_nonempty_or_malformed_blockers(
    tmp_path: Path,
    invalid_blockers: object,
) -> None:
    mechanics = backend.load_outcome_blind_mechanics(_bundle_fixture(tmp_path))

    class _ReadyAdapter(_NeverCalledAdapter):
        def preflight_formal_economics(self, _mechanics):
            return {
                **super().preflight_formal_economics(_mechanics),
                "missing_canonical_fields": invalid_blockers,
            }

    with pytest.raises(
        backend.OfflineRepeatedPolicyBackendError,
        match="ready adapter preflight carries blockers",
    ):
        backend._preflight_adapter(mechanics, _ReadyAdapter())


def test_mechanics_loader_accepts_only_outcome_blind_aligned_panel(tmp_path: Path) -> None:
    mechanics = backend.load_outcome_blind_mechanics(_bundle_fixture(tmp_path))
    assert len(mechanics.selected_days) == 30
    assert mechanics.panel.action_outcomes is None
    assert mechanics.panel.action_supported is None
    assert mechanics.replay_inputs.index.equals(mechanics.panel.metadata.index)
    assert mechanics.bindings.exact_owner_policy_sha256 == offline.ACTIVE_OWNER_POLICY_SHA256


@pytest.mark.parametrize("mode", ("role", "column"))
def test_preinjected_economic_files_or_columns_fail_closed(tmp_path: Path, mode: str) -> None:
    bundle = _bundle_fixture(
        tmp_path,
        economic_file_role=mode == "role",
        economic_column=mode == "column",
    )
    with pytest.raises(backend.OfflineRepeatedPolicyBackendError, match="economic"):
        backend.load_outcome_blind_mechanics(bundle)


def test_strict_false_outcome_blind_replay_declarations_are_not_economic_columns(
    tmp_path: Path,
) -> None:
    path = tmp_path / "replay_inputs.parquet"
    pd.DataFrame(
        {
            "candidate_actions_generated": [False, False],
            "economic_outcomes_read": [False, False],
            "labels_read": [False, False],
        }
    ).to_parquet(path, index=False)

    frame = backend._verify_bound_panel_file(
        "replay_inputs",
        path,
        _parquet_binding(path),
    )

    assert frame.shape == (2, 3)


@pytest.mark.parametrize(
    ("column", "values"),
    (
        ("candidate_actions_generated", [False, True]),
        ("economic_outcomes_read", [False, True]),
        ("labels_read", [False, True]),
        ("economic_outcomes_read", [False, None]),
        ("economic_outcomes_read", ["false", "false"]),
    ),
)
def test_outcome_blind_replay_declarations_fail_closed_unless_strict_false(
    tmp_path: Path,
    column: str,
    values: list[object],
) -> None:
    path = tmp_path / "replay_inputs.parquet"
    pd.DataFrame({column: values}).to_parquet(path, index=False)

    with pytest.raises(
        backend.OfflineRepeatedPolicyBackendError,
        match="replay declaration",
    ):
        backend._verify_bound_panel_file(
            "replay_inputs",
            path,
            _parquet_binding(path),
        )


def test_outcome_blind_declaration_is_forbidden_outside_replay_inputs(
    tmp_path: Path,
) -> None:
    path = tmp_path / "metadata.parquet"
    pd.DataFrame({"economic_outcomes_read": [False]}).to_parquet(path, index=False)

    with pytest.raises(backend.OfflineRepeatedPolicyBackendError, match="economic"):
        backend._verify_bound_panel_file("metadata", path, _parquet_binding(path))


def test_exact_b0_drift_fails_closed(tmp_path: Path) -> None:
    with pytest.raises(backend.OfflineRepeatedPolicyBackendError, match="exact B0"):
        backend.load_outcome_blind_mechanics(_bundle_fixture(tmp_path, b0_drift=True))


def test_outer_test_row_cannot_enter_fold_scoped_labels(tmp_path: Path) -> None:
    mechanics = backend.load_outcome_blind_mechanics(_bundle_fixture(tmp_path))
    adapter = _NeverCalledAdapter()
    provider = backend.CanonicalFoldScopedLabelProvider(mechanics, adapter)
    outer = mechanics.fold_manifest.outer_folds[0]
    test_day = outer["test_days"][0]
    row_id = str(
        mechanics.panel.metadata.index[
            (mechanics.panel.metadata["utc_day"] == test_day)
            & (mechanics.panel.metadata["side"] == "SELL")
        ][0]
    )
    request = nested.FoldScopedOneShotLabelRequest(
        side="SELL",
        outer_fold_id=str(outer["fold_id"]),
        train_days=tuple(outer["train_days"]),
        row_ids=(row_id,),
        row_sha256=backend._canonical_sha256([row_id]),
        mechanics_sha256="b" * 64,
        duration_vocabulary=duration_vocabulary("SELL"),
        request_sha256="c" * 64,
    )
    with pytest.raises(backend.OfflineRepeatedPolicyBackendError, match="outer-test"):
        provider(request)
    assert adapter.label_called is False


def test_custom_evaluator_cannot_be_injected(tmp_path: Path) -> None:
    bundle = _bundle_fixture(tmp_path)
    with pytest.raises(TypeError):
        backend.run_canonical_offline_economics(bundle, evaluator=lambda _request: None)


def test_backend_rejects_manually_supplied_bundle(tmp_path: Path) -> None:
    with pytest.raises(TypeError, match="execution-manifest Path"):
        backend.run_canonical_offline_economics(_bundle_fixture(tmp_path))


def test_replay_fields_are_bound_from_same_panel_metadata_when_not_duplicated(
    tmp_path: Path,
) -> None:
    mechanics = backend.load_outcome_blind_mechanics(
        _bundle_fixture(tmp_path, replay_side=False)
    )

    assert mechanics.replay_inputs["side"].equals(mechanics.panel.metadata["side"])
    assert mechanics.replay_inputs["assignment_ts_ns"].equals(
        pd.Series(
            1_000_000_000 + np.arange(60, dtype=np.int64) * 1_000,
            index=mechanics.replay_inputs.index,
        )
    )
    assert mechanics.replay_inputs["fill_visible_ts_ms"].tolist() == list(
        2_000 + np.arange(60, dtype=np.int64)
    )
    assert set(mechanics.replay_inputs["role_at_fill"]) == {"opener", "add"}


def test_formal_provider_adds_fold_scope_without_mutating_admitted_rows() -> None:
    source = pd.DataFrame(
        {"utc_day": ["2026-06-27"], "side": ["SELL"]},
        index=pd.Index(["row-1"], name="opportunity_id"),
    )

    scoped = backend._bind_outer_train_replay_scope(
        source,
        outer_fold_id="outer1",
    )

    assert "outer_fold_id" not in source
    assert "fold_row_role" not in source
    assert scoped["outer_fold_id"].tolist() == ["outer1"]
    assert scoped["fold_row_role"].tolist() == ["outer_train"]


@pytest.mark.parametrize("reserved", ["outer_fold_id", "fold_row_role"])
def test_formal_provider_rejects_preinjected_fold_scope(reserved: str) -> None:
    source = pd.DataFrame(
        {
            "utc_day": ["2026-06-27"],
            "side": ["SELL"],
            reserved: ["caller-controlled"],
        },
        index=pd.Index(["row-1"], name="opportunity_id"),
    )

    with pytest.raises(
        backend.OfflineRepeatedPolicyBackendError,
        match="pre-injected formal fold scope",
    ):
        backend._bind_outer_train_replay_scope(source, outer_fold_id="outer1")


def test_missing_real_replay_adapter_returns_schema_bound_blocker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle = _bundle_fixture(tmp_path)

    def _missing(_bundle):
        raise backend.OfflineRepeatedPolicyBackendError(
            "fixed historical replay adapter is not implemented"
        )

    monkeypatch.setattr(backend, "_load_canonical_replay_adapter", _missing)
    monkeypatch.setattr(
        orchestrator,
        "load_formal_offline_bundle",
        lambda _path: bundle,
    )
    with pytest.raises(backend.OfflineRepeatedPolicyBackendIncomplete) as raised:
        backend.run_canonical_offline_economics(bundle.execution_manifest_path)
    result = raised.value.result_manifest
    assert result["schema_version"] == orchestrator.FORMAL_RESULT_SCHEMA
    assert result["status"] == backend.BLOCKED_STATUS
    assert result["economic_outcomes_read"] is False
    assert result["repeated_sequential_policy"] is False
    assert set(result["permissions"].values()) == {False}
    assert result["exact_owner_policy_sha256"] == offline.ACTIVE_OWNER_POLICY_SHA256


def test_formal_preflight_reports_missing_canonical_replay_fields_without_economics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle = _bundle_fixture(tmp_path)
    monkeypatch.setattr(
        orchestrator,
        "load_formal_offline_bundle",
        lambda _path: bundle,
    )

    result = backend.preflight_canonical_offline_economics(
        bundle.execution_manifest_path
    )

    assert result["status"] == backend.CANONICAL_FIELDS_BLOCKED_STATUS
    assert result["economic_outcomes_read"] is False
    assert result["repeated_sequential_policy"] is False
    assert set(result["permissions"].values()) == {False}
    adapter_preflight = result["adapter_preflight"]
    missing = adapter_preflight["missing_canonical_fields"]
    assert result["missing_canonical_fields"] == missing
    assert {
        "assignment_equity_usdc",
        "campaign_id",
        "candidate_actions_generated",
        "d_plus_1_market_identity_sha256",
        "economic_outcomes_read",
        "order_id",
        "portable_replay_binding_path",
    } <= set(missing)


def test_backend_one_day_mechanics_keeps_identity_and_permissions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle = _bundle_fixture(tmp_path)
    mechanics = backend.load_outcome_blind_mechanics(bundle)

    class _MechanicsAdapter(_NeverCalledAdapter):
        def run_exact_owner_one_day_mechanics(self, _mechanics):
            assert _mechanics is mechanics
            return {
                "identity": self.identity,
                "status": "exact_owner_one_day_mechanics_complete",
                "adapter_artifact_sha256": self.artifact_sha256,
                "execution_manifest_sha256": (
                    mechanics.bindings.execution_manifest_sha256
                ),
                "source_manifest_sha256": mechanics.bindings.source_manifest_sha256,
                "panel_manifest_sha256": mechanics.bindings.panel_manifest_sha256,
                "exact_owner_policy_sha256": offline.ACTIVE_OWNER_POLICY_SHA256,
                "worker_count": 1,
                "opportunity_count": 2,
                "exact_owner_noop_parity_count": 2,
                "economic_values_persisted": False,
                "economic_values_used_for_selection": False,
                "validation_read": False,
                "sealed_holdout_read": False,
                "action_authorized": False,
                "live_authorized": False,
            }

    adapter = _MechanicsAdapter()
    monkeypatch.setattr(
        orchestrator,
        "load_formal_offline_bundle",
        lambda _path: bundle,
    )
    monkeypatch.setattr(
        backend,
        "_load_canonical_replay_adapter",
        lambda _bundle: adapter,
    )
    monkeypatch.setattr(
        backend,
        "load_outcome_blind_mechanics",
        lambda _bundle: mechanics,
    )

    result = backend.run_exact_owner_one_day_mechanics(
        bundle.execution_manifest_path
    )

    assert result["worker_count"] == 1
    assert result["exact_owner_noop_parity_count"] == 2
    assert result["economic_values_persisted"] is False
    assert result["live_authorized"] is False


def test_sequential_receipt_drift_is_rejected_before_rows_are_used(tmp_path: Path) -> None:
    mechanics = backend.load_outcome_blind_mechanics(_bundle_fixture(tmp_path))
    adapter = _NeverCalledAdapter()

    class _BadReceiptAdapter(_NeverCalledAdapter):
        def evaluate_repeated_policy(self, _request, _replay_inputs):
            self.sequential_called = True
            return backend.CanonicalSequentialReplayResult(
                rows=pd.DataFrame({"candidate_terminal_value_usdc": [999.0]}),
                receipt={"receipt_sha256": "0" * 64},
            )

    adapter = _BadReceiptAdapter()
    evaluator = backend.CanonicalSequentialEvaluator(mechanics, adapter)
    candidate = nested.FittedCandidate(
        ladder_name="B0_CURRENT_EXACT",
        side="SELL",
        policy=None,
        selected_profile="exact_owner",
        training_days=(),
        training_row_sha256=backend._canonical_sha256([]),
        policy_payload={"kind": "exact_owner"},
        policy_sha256=offline.ACTIVE_OWNER_POLICY_SHA256,
        fit_audit={},
        feature_pool_audit=None,
    )
    request = nested.EvaluationRequest(
        candidate=candidate,
        side="SELL",
        days=(mechanics.selected_days[10],),
        fold_id="outer1",
        stage="outer_oof",
        panel_role=offline.PANEL_ROLE,
    )
    with pytest.raises(backend.OfflineRepeatedPolicyBackendError, match="receipt drifted"):
        evaluator(request)
    assert adapter.sequential_called is True


def test_only_formal_bundle_type_is_accepted() -> None:
    with pytest.raises(TypeError, match="FormalOfflineBundle"):
        backend.load_outcome_blind_mechanics(object())
