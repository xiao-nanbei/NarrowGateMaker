from __future__ import annotations

import hashlib
import importlib
import json
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_nested_oof_v1 as nested,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_native_observation_batch_v1 as observation_batch,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_panel_builder_v1 as panel_builder,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_repeated_policy_backend_v1 as backend,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_replay_adapter_v1 as adapter_module,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_v1 as offline,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_v1 as successor,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_nested_oof import (
    duration_vocabulary,
)

SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64
DAY = "2026-07-01"


@pytest.fixture(autouse=True)
def _governed_test_data_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        offline,
        "default_layout",
        lambda: SimpleNamespace(project_data_root=tmp_path),
    )


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_panel_schema_preflight_reports_metadata_and_execution_field_gaps() -> None:
    adapter = adapter_module.build_canonical_replay_adapter()
    result = adapter.preflight_formal_panel_schema(
        metadata_columns=("utc_day", "panel_role", "side", "assignment_ts_ns"),
        replay_input_columns=tuple(adapter_module._COMMON_REPLAY_COLUMNS - {"side"}),
    )

    assert result["status"] == backend.CANONICAL_FIELDS_BLOCKED_STATUS
    missing = set(result["missing_canonical_fields"])
    assert "side" not in missing
    assert {
        "campaign_cluster_id",
        "observation_end_ts_ns",
        "campaign_id",
        "assignment_equity_usdc",
        "portable_replay_binding_path",
    } <= missing


def _bindings(**updates: str) -> backend.FormalExecutionBindings:
    values = {
        "execution_manifest_sha256": SHA_A,
        "source_manifest_sha256": SHA_B,
        "panel_manifest_sha256": SHA_C,
        "fold_manifest_sha256": "d" * 64,
        "nested_fold_manifest_sha256": "e" * 64,
        "exact_owner_policy_sha256": offline.ACTIVE_OWNER_POLICY_SHA256,
        "exact_owner_predicate_bundle_sha256": offline.ACTIVE_PREDICATE_BUNDLE_SHA256,
        "exact_owner_private_config_sha256": offline.ACTIVE_PRIVATE_CONFIG_SHA256,
    }
    values.update(updates)
    return backend.FormalExecutionBindings(**values)


def _complete_boolean_columns(*, include_m2: bool = True) -> tuple[str, ...]:
    columns: list[str] = []
    for prefix in successor.full_ema_pair_prefixes():
        columns.extend(
            (
                f"predicate::{prefix}:ordering_favorable",
                f"predicate::{prefix}:last_cross_direction_golden",
                f"predicate::{prefix}:cross_age_recency",
                f"predicate::{prefix}:persistence",
                f"predicate::{prefix}:signed_distance",
                f"predicate::{prefix}:volatility_normalized_distance",
                f"predicate::{prefix}:slope",
                f"predicate::{prefix}:curvature",
                f"predicate::{prefix}:converging",
                f"predicate::{prefix}:expanding",
            )
        )
    columns.extend(
        (
            successor.CURRENT_CAMPAIGN_AGE,
            successor.CURRENT_SHORT_CROSS,
            successor.CURRENT_LONG_CROSS,
            "tri::m0::role::add",
        )
    )
    if include_m2:
        columns.append(
            "tri::signed_flow_imbalance__h1s__h2s::positive_ordering"
        )
    return tuple(dict.fromkeys(columns))


def _mechanics(*, include_m2: bool = True) -> backend.OutcomeBlindMechanics:
    index = pd.Index(("buy-row", "sell-row"), name="opportunity_id")
    metadata = pd.DataFrame(
        {
            "utc_day": (DAY, DAY),
            "panel_role": (offline.PANEL_ROLE, offline.PANEL_ROLE),
            "side": ("BUY", "SELL"),
            "campaign_cluster_id": ("buy-campaign", "sell-campaign"),
            "assignment_ts_ns": (1, 2),
            "observation_end_ts_ns": (10, 20),
        },
        index=index,
    )
    boolean = pd.DataFrame(
        0,
        index=index,
        columns=_complete_boolean_columns(include_m2=include_m2),
        dtype=np.int8,
    )
    continuous = pd.DataFrame(
        {"continuous::mid_state": (0.1, -0.1)}, index=index
    )
    panel = nested.NestedOofPanel(
        metadata=metadata,
        boolean_features=boolean,
        continuous_features=continuous,
        exact_owner_actions=pd.Series(
            ("CONTROL_85N", "FIXED_166S"), index=index, name="exact_owner_action"
        ),
    )
    days = tuple(pd.date_range("2026-06-27", periods=30).strftime("%Y-%m-%d"))
    return backend.OutcomeBlindMechanics(
        panel=panel,
        replay_inputs=pd.DataFrame(index=index),
        selected_days=days,
        fold_manifest=successor.ProspectiveFoldManifest(
            active_days=days,
            outer_folds=(),
            manifest_sha256="f" * 64,
        ),
        bindings=_bindings(),
        file_sha256={},
        mechanics_receipt_sha256="9" * 64,
    )


def _label_request(
    row_ids: tuple[str, ...],
    *,
    side: str = "SELL",
    train_days: tuple[str, ...] = (DAY,),
    outer_fold_id: str = "outer1",
) -> nested.FoldScopedOneShotLabelRequest:
    vocabulary = duration_vocabulary(side)
    row_sha = adapter_module._canonical_sha256(list(row_ids))
    mechanics_sha = "7" * 64
    body = {
        "schema": f"{nested.IDENTITY}.fold_scoped_one_shot_label_request.v1",
        "side": side,
        "outer_fold_id": outer_fold_id,
        "train_days": list(train_days),
        "row_ids": list(row_ids),
        "row_sha256": row_sha,
        "mechanics_sha256": mechanics_sha,
        "duration_vocabulary": list(vocabulary),
    }
    return nested.FoldScopedOneShotLabelRequest(
        side=side,
        outer_fold_id=outer_fold_id,
        train_days=train_days,
        row_ids=row_ids,
        row_sha256=row_sha,
        mechanics_sha256=mechanics_sha,
        duration_vocabulary=vocabulary,
        request_sha256=adapter_module._canonical_sha256(body),
    )


def _common_replay_inputs(
    row_ids: tuple[str, ...] = ("row-1",),
    *,
    side: str = "SELL",
    days: tuple[str, ...] | None = None,
) -> pd.DataFrame:
    observed_days = days or tuple(DAY for _ in row_ids)
    index = pd.Index(row_ids, name="opportunity_id")
    return pd.DataFrame(
        {
            "utc_day": observed_days,
            "opportunity_id": row_ids,
            "side": tuple(side for _ in row_ids),
            "replay_engine": tuple(adapter_module.REPLAY_ENGINE for _ in row_ids),
            "queue_identity": tuple(adapter_module.QUEUE_IDENTITY for _ in row_ids),
            "same_millisecond_ambiguity_policy": tuple(
                adapter_module.SAME_MILLISECOND_AMBIGUITY_POLICY for _ in row_ids
            ),
            "exact_owner_policy_sha256": tuple(
                offline.ACTIVE_OWNER_POLICY_SHA256 for _ in row_ids
            ),
            "exact_owner_predicate_bundle_sha256": tuple(
                offline.ACTIVE_PREDICATE_BUNDLE_SHA256 for _ in row_ids
            ),
            "exact_owner_private_config_sha256": tuple(
                offline.ACTIVE_PRIVATE_CONFIG_SHA256 for _ in row_ids
            ),
            "replay_input_receipt_sha256": tuple("8" * 64 for _ in row_ids),
            "economic_outcomes_read": tuple(False for _ in row_ids),
            "labels_read": tuple(False for _ in row_ids),
            "candidate_actions_generated": tuple(False for _ in row_ids),
        },
        index=index,
    )


def _outer_train_request(
    rows: pd.DataFrame,
    *,
    label: nested.FoldScopedOneShotLabelRequest | None = None,
    bindings: backend.FormalExecutionBindings | None = None,
) -> backend.CanonicalOuterTrainReplayRequest:
    return backend.CanonicalOuterTrainReplayRequest(
        label_request=label or _label_request(tuple(str(value) for value in rows.index)),
        replay_input_sha256=adapter_module._frame_sha256(rows),
        bindings=bindings or _bindings(),
    )


def _fitted_candidate() -> nested.FittedCandidate:
    return nested.FittedCandidate(
        ladder_name="B0_CURRENT_EXACT",
        side="SELL",
        policy=None,
        selected_profile="exact_owner",
        training_days=(DAY,),
        training_row_sha256=SHA_A,
        policy_payload={"identity": "exact-owner"},
        policy_sha256=SHA_B,
        fit_audit={},
        feature_pool_audit=None,
    )


def test_fold_policy_identity_is_shared_by_executed_and_learning_artifacts() -> None:
    fitted = _fitted_candidate()
    identity = adapter_module._fold_policy_identity(fitted)
    repeated = importlib.import_module(adapter_module.FIXED_REPEATED_POLICY_BRIDGE_MODULE)

    binding = repeated.ArtifactIdentityBinding(
        executed_artifact_scope=(
            repeated.ExecutedArtifactScope.LEARNING_ALGORITHM_FOLD_POLICY
        ),
        executed_policy_identity=identity,
        executed_policy_sha256=fitted.expected_executed_policy_sha256,
        executed_predicate_bundle_sha256=adapter_module._canonical_sha256(
            fitted.policy_payload
        ),
        learning_algorithm_identity=identity,
        learning_algorithm_artifact_sha256=fitted.policy_sha256,
    )

    assert binding.executed_policy_identity == binding.learning_algorithm_identity


def _sequential_request(
    rows: pd.DataFrame,
    *,
    bindings: backend.FormalExecutionBindings | None = None,
) -> backend.CanonicalSequentialReplayRequest:
    evaluation = nested.EvaluationRequest(
        candidate=_fitted_candidate(),
        side="SELL",
        days=tuple(dict.fromkeys(str(value) for value in rows["utc_day"])),
        fold_id="outer1",
        stage="outer_oof",
        panel_role=offline.PANEL_ROLE,
    )
    return backend.CanonicalSequentialReplayRequest(
        evaluation_request=evaluation,
        replay_input_sha256=adapter_module._frame_sha256(rows),
        bindings=bindings or _bindings(),
    )


def _cache_key(**updates: Any) -> adapter_module.DayReplayCacheKey:
    values: dict[str, Any] = {
        "adapter_artifact_sha256": SHA_A,
        "source_manifest_sha256": SHA_B,
        "panel_manifest_sha256": SHA_C,
        "fold_manifest_sha256": "d" * 64,
        "execution_manifest_sha256": "e" * 64,
        "exact_owner_policy_sha256": offline.ACTIVE_OWNER_POLICY_SHA256,
        "candidate_policy_sha256": "f" * 64,
        "side": "SELL",
        "stage": "outer_oof",
        "fold_id": "outer1",
        "utc_day": DAY,
        "day_input_sha256": "9" * 64,
    }
    values.update(updates)
    return adapter_module.DayReplayCacheKey(**values)


def test_factory_identity_and_artifact_hash_match_backend_constant() -> None:
    adapter = adapter_module.build_canonical_replay_adapter()
    assert adapter.identity == backend.CANONICAL_REPLAY_ADAPTER_IDENTITY
    assert adapter.artifact_sha256 == _sha256_file(Path(adapter_module.__file__))
    assert backend._load_canonical_replay_adapter().artifact_sha256 == (
        adapter.artifact_sha256
    )


def test_duration_action_contract_does_not_open_historical_baseline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract = {
        "identity": "frozen-duration-study",
        "schema_version": "frozen-duration-study.outcome_blind_inputs.v1",
        "permissions": {
            "development_economic_labels_read": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    actions_by_side: dict[str, tuple[SimpleNamespace, ...]] = {}
    for side in ("BUY", "SELL"):
        actions: list[SimpleNamespace] = []
        for policy_id in duration_vocabulary(side):
            fixed_s = None if policy_id == "CONTROL_85N" else int(
                policy_id.removeprefix("FIXED_").removesuffix("S")
            )
            payload = {
                "policy_id": policy_id,
                "engine_action": (
                    "CONTROL_85N" if fixed_s is None else "FIXED_DURATION_MS"
                ),
                "fixed_duration_s": fixed_s,
                "fixed_duration_ms": None if fixed_s is None else fixed_s * 1_000,
                "duration_semantics": "frozen test semantics",
            }
            actions.append(
                SimpleNamespace(
                    **payload,
                    payload=lambda value=payload: dict(value),
                )
            )
        actions_by_side[side] = tuple(actions)
    fake_study = SimpleNamespace(
        IDENTITY="frozen-duration-study",
        OUTCOME_BLIND_INPUTS=Path("unused-private-source.json"),
        OUTCOME_BLIND_INPUTS_SHA256=SHA_A,
        _validate_file=lambda *_args, **_kwargs: None,
        _load_source_json=lambda *_args, **_kwargs: contract,
        _duration_actions=lambda _contract, side: actions_by_side[side],
        _load_contract=lambda: pytest.fail("historical baseline loader was called"),
    )
    real_import = importlib.import_module
    monkeypatch.setattr(
        adapter_module.importlib,
        "import_module",
        lambda name: (
            fake_study
            if name == adapter_module.FIXED_ONE_SHOT_REPLAY_MODULE
            else real_import(name)
        ),
    )

    binding, actions = adapter_module._load_frozen_duration_action_contract()

    assert binding["source_sha256"] == SHA_A
    assert binding["historical_operational_baseline_read"] is False
    assert binding["historical_execution_plan_read"] is False
    assert tuple(action.policy_id for action in actions["BUY"]) == duration_vocabulary("BUY")
    assert tuple(action.policy_id for action in actions["SELL"]) == duration_vocabulary(
        "SELL"
    )


def test_formal_preflight_reports_missing_admitted_replay_inputs_not_fixed_api() -> None:
    adapter = adapter_module.build_canonical_replay_adapter()

    result = adapter.preflight_formal_economics(_mechanics())

    assert result["status"] == adapter_module.MECHANICS_MISSING_STATUS
    assert "portable_replay_binding_path" in result["missing_canonical_fields"]
    assert "canonical_backtest_tick_arm_executor_binding" not in result[
        "missing_canonical_fields"
    ]
    assert set(result["fixed_canonical_api_bindings"]) == set(
        adapter_module._FIXED_CANONICAL_API_SYMBOLS
    )
    assert result["duration_action_contract"][
        "historical_operational_baseline_read"
    ] is False
    assert set(result["permissions"].values()) == {False}


def test_formal_preflight_returns_backend_mechanics_ready_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = _common_replay_inputs(("sell-row",), side="SELL")
    mechanics = replace(_mechanics(), replay_inputs=rows)
    monkeypatch.setattr(
        adapter_module,
        "_validate_replay_input_frame",
        lambda frame, **_kwargs: frame,
    )
    monkeypatch.setattr(
        adapter_module,
        "_require_executable_replay_inputs",
        lambda _rows, **_kwargs: None,
    )
    monkeypatch.setattr(
        adapter_module,
        "_validate_d_plus_one_contract",
        lambda _rows: None,
    )
    monkeypatch.setattr(
        adapter_module,
        "_resolve_execution_options",
        lambda _rows: SimpleNamespace(binding={}),
    )
    monkeypatch.setattr(
        adapter_module,
        "_canonical_day_request",
        lambda **_kwargs: None,
    )

    result = adapter_module.build_canonical_replay_adapter().preflight_formal_economics(
        mechanics
    )

    assert result["status"] == backend.MECHANICS_READY_STATUS
    assert result["missing_canonical_fields"] == []
    assert result["duration_action_contract"]["historical_execution_plan_read"] is False
    assert set(result["permissions"].values()) == {False}


def test_factory_and_backend_reject_custom_adapter_injection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(TypeError):
        adapter_module.build_canonical_replay_adapter(object())  # type: ignore[call-arg]
    monkeypatch.setattr(adapter_module, "build_canonical_replay_adapter", lambda: object())
    with pytest.raises(backend.OfflineRepeatedPolicyBackendError, match="identity drifted"):
        backend._load_canonical_replay_adapter()


def test_search_contract_exposes_full_ladder_and_complete_feature_universes() -> None:
    ladder, continuous = adapter_module.build_canonical_replay_adapter().build_search_contract(
        _mechanics()
    )
    assert tuple(item.name for item in ladder) == successor.SUCCESSOR_CANDIDATE_LADDER
    by_name = {item.name: item for item in ladder}
    for side in ("BUY", "SELL"):
        e1 = by_name["E1_FULL_EMA_BANK"].feature_names_by_side[side]
        assert successor.audit_full_ema_universe(e1)["all_45_pairs_present"] is True
        e2 = by_name["E2_DIRECTIONAL_EMA"].feature_names_by_side[side]
        assert successor.CURRENT_SHORT_CROSS not in e2
        assert successor.CURRENT_LONG_CROSS not in e2
        for prefix in successor.full_ema_pair_prefixes():
            pair = tuple(name for name in e2 if prefix in name)
            assert len(pair) >= len(adapter_module._E2_SEMANTIC_TOKENS)
            for tokens in adapter_module._E2_SEMANTIC_TOKENS.values():
                assert any(any(token in name for token in tokens) for name in pair)
        e3_profile = by_name["E3_HIGHER_ORDER_BOOLEAN"].profiles[0]
        assert e3_profile.max_rules > 1
        assert e3_profile.max_clauses_per_rule > 1
        assert e3_profile.max_literals_per_clause > 2
        e3 = set(by_name["E3_HIGHER_ORDER_BOOLEAN"].feature_names_by_side[side])
        m2 = set(by_name["M2_TRUE_INCREMENTAL"].feature_names_by_side[side])
        increment = m2 - e3
        assert increment
        assert all(adapter_module._is_true_m2_predicate(name) for name in increment)
    assert continuous.name == nested.CONTINUOUS_COMPARATOR
    assert continuous.feature_names_by_side["BUY"] == ("continuous::mid_state",)


def test_search_contract_fails_when_real_trade_depth_increment_is_missing() -> None:
    with pytest.raises(
        adapter_module.OfflineReplayAdapterMechanicsMissing,
        match="true_trade_or_depth_predicate",
    ):
        adapter_module.build_canonical_replay_adapter().build_search_contract(
            _mechanics(include_m2=False)
        )


def test_outer_test_rows_are_rejected_before_mechanics_fallback() -> None:
    rows = _common_replay_inputs()
    rows["outer_fold_id"] = "outer1"
    rows["fold_row_role"] = "outer_test"
    request = _outer_train_request(rows)
    with pytest.raises(adapter_module.OfflineReplayAdapterError, match="row role"):
        adapter_module.build_canonical_replay_adapter().generate_outer_train_one_shot_labels(
            request, rows
        )


def test_current_replay_schema_fails_closed_and_names_d_plus_one_fields() -> None:
    rows = _common_replay_inputs()
    rows["outer_fold_id"] = "outer1"
    rows["fold_row_role"] = "outer_train"
    request = _outer_train_request(rows)
    with pytest.raises(adapter_module.OfflineReplayAdapterMechanicsMissing) as captured:
        adapter_module.build_canonical_replay_adapter().generate_outer_train_one_shot_labels(
            request, rows
        )
    assert captured.value.status == adapter_module.MECHANICS_MISSING_STATUS
    assert "portable_replay_binding_path" in captured.value.missing
    assert "d_plus_1_context_receipt_sha256" in captured.value.missing


def test_one_shot_replay_rejects_custom_evaluator_column() -> None:
    rows = _common_replay_inputs()
    rows["custom_evaluator"] = "do-not-call"
    request = _outer_train_request(rows)
    with pytest.raises(adapter_module.OfflineReplayAdapterError, match="injection"):
        adapter_module.build_canonical_replay_adapter().generate_outer_train_one_shot_labels(
            request, rows
        )


def test_unsupported_one_shot_target_must_be_nan_not_neutral_zero() -> None:
    rows = _common_replay_inputs(("row-1", "row-2"))
    label = _label_request(("row-1", "row-2"))
    request = _outer_train_request(rows, label=label)
    actions = label.duration_vocabulary
    outcomes = pd.DataFrame(0.0, index=rows.index, columns=actions)
    supported = pd.DataFrame(True, index=rows.index, columns=actions)
    supported.loc["row-2", actions[-1]] = False
    with pytest.raises(nested.NestedOofExecutionError, match="neutral-zero"):
        adapter_module._validate_one_shot_frames(request, outcomes, supported)
    outcomes.loc["row-2", actions[-1]] = np.nan
    adapter_module._validate_one_shot_frames(request, outcomes, supported)


def test_b0_binding_drift_is_rejected_before_sequential_replay() -> None:
    rows = _common_replay_inputs()
    request = _sequential_request(
        rows,
        bindings=_bindings(exact_owner_policy_sha256="0" * 64),
    )
    with pytest.raises(adapter_module.OfflineReplayAdapterError, match="exact B0"):
        adapter_module.build_canonical_replay_adapter().evaluate_repeated_policy(
            request, rows
        )


@pytest.mark.parametrize(
    ("column", "value", "message"),
    (
        ("one_shot_effect_aggregation_used", True, "one-shot aggregation"),
        ("repeated_sequential_policy", False, "non-sequential"),
        ("exact_current_owner_row_wise_baseline", False, "exact B0"),
    ),
)
def test_sequential_result_rejects_forbidden_economic_shortcuts(
    column: str,
    value: bool,
    message: str,
) -> None:
    rows = _common_replay_inputs()
    request = _sequential_request(rows)
    result = pd.DataFrame(
        {
            "one_shot_effect_aggregation_used": (False,),
            "repeated_sequential_policy": (True,),
            "exact_current_owner_row_wise_baseline": (True,),
        }
    )
    result[column] = value
    with pytest.raises(adapter_module.OfflineReplayAdapterError, match=message):
        adapter_module._validate_sequential_rows(request, result)


def test_day_cache_key_isolated_by_fold_candidate_stage_and_day_input() -> None:
    base = _cache_key()
    variants = (
        replace(base, fold_id="outer2"),
        replace(base, candidate_policy_sha256="1" * 64),
        replace(base, stage="inner_oof"),
        replace(base, day_input_sha256="2" * 64),
        replace(base, adapter_artifact_sha256="3" * 64),
        replace(base, exact_owner_policy_sha256="4" * 64),
    )
    hashes = {base.cache_key_sha256, *(item.cache_key_sha256 for item in variants)}
    assert len(hashes) == 1 + len(variants)


def test_atomic_day_cache_round_trip_and_progress_receipt(tmp_path: Path) -> None:
    cache = adapter_module.DayReplayCache(tmp_path / "cache")
    key = _cache_key()
    index = pd.Index(("row-1",), name="opportunity_id")
    outcomes = pd.DataFrame({"CONTROL_85N": (0.25,)}, index=index)
    supported = pd.DataFrame({"CONTROL_85N": (True,)}, index=index)
    cache.write_progress(key, state="running")
    cache.admit_one_shot(key, outcomes, supported)
    cache.write_progress(key, state="complete")
    loaded = cache.load_one_shot(key)
    assert loaded is not None
    pd.testing.assert_frame_equal(loaded[0], outcomes)
    pd.testing.assert_frame_equal(loaded[1], supported)
    manifest = json.loads(
        (tmp_path / "cache" / "entries" / key.cache_key_sha256 / "manifest.json").read_text()
    )
    assert manifest["complete"] is True
    assert manifest["atomic_admission"] is True
    progress = json.loads(
        (tmp_path / "cache" / "progress" / f"{key.cache_key_sha256}.json").read_text()
    )
    assert progress["state"] == "complete"
    assert progress["receipt_sha256"] == adapter_module._document_sha256(
        progress, "receipt_sha256"
    )


@pytest.mark.parametrize("workers", (1, 6, 8))
def test_day_worker_count_accepts_only_frozen_range(workers: int) -> None:
    assert adapter_module._validated_worker_count(workers) == workers


@pytest.mark.parametrize("workers", (0, 9, True, "six"))
def test_day_worker_count_rejects_out_of_contract_values(workers: Any) -> None:
    with pytest.raises(adapter_module.OfflineReplayAdapterError):
        adapter_module._validated_worker_count(workers)


def test_d_plus_one_contract_rejects_day_end_terminal_and_new_assignments() -> None:
    rows = pd.DataFrame(
        {
            "utc_day": (DAY,),
            "d_plus_1_utc_day": ("2026-07-02",),
            "d_plus_1_market_identity_sha256": (SHA_A,),
            "d_plus_1_feature_identity_sha256": (SHA_B,),
            "d_plus_1_native_observation_sha256": (SHA_C,),
            "d_plus_1_context_receipt_sha256": ("d" * 64,),
            "d_plus_1_new_target_assignments_allowed": (False,),
            "target_day_end_terminalized": (False,),
            "assignment_to_common_washout_required": (True,),
            "assignment_ts_ns": (pd.Timestamp(DAY, tz="UTC").value + 1_000_000,),
            "observation_end_ts_ns": (
                (pd.Timestamp(DAY, tz="UTC") + pd.Timedelta(days=2)).value,
            ),
        }
    )
    adapter_module._validate_d_plus_one_contract(rows)
    rows.loc[0, "target_day_end_terminalized"] = True
    with pytest.raises(adapter_module.OfflineReplayAdapterError, match="day-end"):
        adapter_module._validate_d_plus_one_contract(rows)
    rows.loc[0, "target_day_end_terminalized"] = False
    rows.loc[0, "d_plus_1_new_target_assignments_allowed"] = True
    with pytest.raises(adapter_module.OfflineReplayAdapterError, match="target assignments"):
        adapter_module._validate_d_plus_one_contract(rows)


@pytest.mark.parametrize("delta_ns", (-1, 1))
def test_observation_end_rejects_one_nanosecond_drift_from_d_plus_two(
    delta_ns: int,
) -> None:
    expected_end = (pd.Timestamp(DAY, tz="UTC") + pd.Timedelta(days=2)).value
    rows = pd.DataFrame(
        {
            "utc_day": (DAY,),
            "d_plus_1_utc_day": ("2026-07-02",),
            "d_plus_1_market_identity_sha256": (SHA_A,),
            "d_plus_1_feature_identity_sha256": (SHA_B,),
            "d_plus_1_native_observation_sha256": (SHA_C,),
            "d_plus_1_context_receipt_sha256": ("d" * 64,),
            "d_plus_1_new_target_assignments_allowed": (False,),
            "target_day_end_terminalized": (False,),
            "assignment_to_common_washout_required": (True,),
            "assignment_ts_ns": (pd.Timestamp(DAY, tz="UTC").value + 1_000_000,),
            "observation_end_ts_ns": (expected_end + delta_ns,),
        }
    )

    with pytest.raises(
        adapter_module.OfflineReplayAdapterError,
        match=r"common outcome-blind D\+1 bound",
    ):
        adapter_module._validate_d_plus_one_contract(rows)


def _canonical_projection_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[dict[str, Any], pd.DataFrame, dict[str, str]]:
    files: dict[str, Path] = {}
    for name in (
        "bbo",
        "l2",
        "features",
        "source_manifest",
        "book_view_manifest",
        "features_manifest",
        "private_config",
    ):
        path = tmp_path / f"{name}.fixture"
        path.write_bytes(f"canonical-{name}".encode("ascii"))
        files[name] = path
    native_observation_root = tmp_path / "native-observations"
    native_observation_root.mkdir()
    payload: dict[str, Any] = {
        "utc_day": DAY,
        "panel_role": offline.PANEL_ROLE,
        "queue_identity": adapter_module.QUEUE_IDENTITY,
        "same_millisecond_ambiguity_policy": (
            adapter_module.SAME_MILLISECOND_AMBIGUITY_POLICY
        ),
        **{
            f"{name}_path": str(path)
            for name, path in files.items()
        },
        **{
            f"{name}_sha256": _sha256_file(path)
            for name, path in files.items()
        },
        "native_observation_root": str(native_observation_root),
        "source_receipts": {},
        "input_binding_sha256": "e" * 64,
    }
    payload["projection_receipt_sha256"] = adapter_module._canonical_sha256(payload)
    expected_context = {
        "d_plus_1_utc_day": "2026-07-02",
        "d_plus_1_market_identity_sha256": SHA_A,
        "d_plus_1_feature_identity_sha256": SHA_B,
        "d_plus_1_native_observation_sha256": SHA_C,
        "d_plus_1_context_receipt_sha256": "d" * 64,
    }
    monkeypatch.setattr(
        panel_builder,
        "_sequential_context_bindings",
        lambda _request: expected_context,
    )
    rows = pd.DataFrame(
        {
            "day_input_sha256": (payload["projection_receipt_sha256"],),
            **{name: (value,) for name, value in expected_context.items()},
        }
    )
    return {"day_projections": {DAY: payload}}, rows, expected_context


def test_canonical_day_request_rejects_day_input_receipt_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    binding, rows, _expected_context = _canonical_projection_fixture(
        tmp_path,
        monkeypatch,
    )
    rows.loc[0, "day_input_sha256"] = "0" * 64

    with pytest.raises(
        adapter_module.OfflineReplayAdapterError,
        match="canonical projection receipt",
    ):
        adapter_module._canonical_day_request(
            binding=binding,
            utc_day=DAY,
            replay_inputs=rows,
        )


@pytest.mark.parametrize(
    "column",
    (
        "d_plus_1_market_identity_sha256",
        "d_plus_1_feature_identity_sha256",
        "d_plus_1_native_observation_sha256",
        "d_plus_1_context_receipt_sha256",
    ),
)
def test_canonical_day_request_rejects_any_valid_format_d_plus_one_sha_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    column: str,
) -> None:
    binding, rows, expected_context = _canonical_projection_fixture(
        tmp_path,
        monkeypatch,
    )
    replacement = "0" * 64
    assert replacement != expected_context[column]
    rows.loc[0, column] = replacement

    with pytest.raises(
        adapter_module.OfflineReplayAdapterError,
        match=f"canonical D\\+1 receipt drifted: {column}",
    ):
        adapter_module._canonical_day_request(
            binding=binding,
            utc_day=DAY,
            replay_inputs=rows,
        )


def _fixed_bridge() -> dict[str, Any]:
    return adapter_module._expected_fixed_bridge()


def _write_observation_manifest(
    tmp_path: Path,
    *,
    continuation_days: tuple[str, ...] = adapter_module.REQUIRED_ADDITIONAL_CONTEXT_DAYS,
    continuation_assignment_eligible: bool = False,
    continuation_economic_eligible: bool = False,
) -> tuple[Path, dict[str, Any]]:
    selected = tuple(pd.date_range("2026-05-01", periods=30).strftime("%Y-%m-%d"))
    context = tuple(sorted((*selected, *continuation_days)))
    day_rows = []
    for day in context:
        is_continuation = day in continuation_days
        day_rows.append(
            {
                "utc_day": day,
                "observation_role": (
                    "continuation_only" if is_continuation else "selected_target"
                ),
                "target_assignment_eligible": (
                    continuation_assignment_eligible if is_continuation else True
                ),
                "economic_test_row_eligible": (
                    continuation_economic_eligible if is_continuation else True
                ),
                "washout_continuation_eligible": is_continuation,
            }
        )
    payload: dict[str, Any] = {
        "schema_version": observation_batch.SCHEMA_VERSION,
        "identity": observation_batch.IDENTITY,
        "selected_target_days": list(selected),
        "selected_target_day_count": 30,
        "observation_context_days": list(context),
        "observation_context_day_count": len(context),
        "continuation_only_days": list(continuation_days),
        "continuation_days_create_target_assignments": False,
        "continuation_days_create_economic_test_rows": False,
        "permissions": {
            "action_authorized": False,
            "actions_read": False,
            "economic_outcomes_read": False,
            "labels_read": False,
            "live_authorized": False,
            "model_trained": False,
            "sealed_holdout_read": False,
            "validation_read": False,
        },
        "days": day_rows,
    }
    payload["canonical_manifest_sha256"] = adapter_module._document_sha256(
        payload, "canonical_manifest_sha256"
    )
    path = tmp_path / f"observation-{hash(tuple(continuation_days))}.json"
    path.write_text(json.dumps(payload, sort_keys=True), encoding="ascii")
    return path, payload


def _portable_binding_rows(
    tmp_path: Path,
    observation_path: Path,
    observation_payload: Mapping[str, Any],
) -> pd.DataFrame:
    selected = tuple(observation_payload["selected_target_days"])
    binding = {
        "schema_version": f"{adapter_module.IDENTITY}.portable_replay_binding.v1",
        "identity": (
            "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
            "offline_sequential_replay_input_v2"
        ),
        "panel_identity": (
            "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
            "offline_v1.offline_sequential_panel_v2"
        ),
        "selected_days": list(selected),
        "selected_day_count": len(selected),
        "fixed_bridge": _fixed_bridge(),
        "target_day_end_terminalized": False,
        "d_plus_1_new_target_assignments_allowed": False,
        "assignment_to_common_washout_required": True,
        "native_observation_batch_manifest": {
            "path": str(observation_path),
            "file_sha256": _sha256_file(observation_path),
            "canonical_manifest_sha256": observation_payload[
                "canonical_manifest_sha256"
            ],
        },
        "day_projections": {},
    }
    path = tmp_path / "binding.json"
    path.write_text(json.dumps(binding, sort_keys=True), encoding="ascii")
    return pd.DataFrame(
        {
            "portable_replay_binding_path": (str(path),),
            "portable_replay_binding_sha256": (_sha256_file(path),),
            "portable_day_cache_root": (str(tmp_path / "cache" / "replay_dag" / "f05"),),
            "day_replay_workers": (6,),
        }
    )


def test_portable_binding_requires_all_four_additional_context_days(
    tmp_path: Path,
) -> None:
    continuation = (*adapter_module.REQUIRED_ADDITIONAL_CONTEXT_DAYS[:-1], "2026-08-07")
    observation_path, observation_payload = _write_observation_manifest(
        tmp_path, continuation_days=continuation
    )
    rows = _portable_binding_rows(tmp_path, observation_path, observation_payload)
    with pytest.raises(adapter_module.OfflineReplayAdapterMechanicsMissing) as captured:
        adapter_module._resolve_execution_options(rows)
    assert captured.value.missing == ("D+1:2026-08-06",)


@pytest.mark.parametrize(
    ("assignment_eligible", "economic_eligible"), ((True, False), (False, True))
)
def test_continuation_only_rows_cannot_create_assignments_or_economic_rows(
    tmp_path: Path,
    assignment_eligible: bool,
    economic_eligible: bool,
) -> None:
    observation_path, observation_payload = _write_observation_manifest(
        tmp_path,
        continuation_assignment_eligible=assignment_eligible,
        continuation_economic_eligible=economic_eligible,
    )
    rows = _portable_binding_rows(tmp_path, observation_path, observation_payload)
    with pytest.raises(
        adapter_module.OfflineReplayAdapterError,
        match="assignment/economic eligible",
    ):
        adapter_module._resolve_execution_options(rows)


def test_valid_observation_binding_has_30_targets_34_context_and_six_workers(
    tmp_path: Path,
) -> None:
    observation_path, observation_payload = _write_observation_manifest(tmp_path)
    rows = _portable_binding_rows(tmp_path, observation_path, observation_payload)
    options = adapter_module._resolve_execution_options(rows)
    assert options.workers == 6
    assert options.cache.root == (tmp_path / "cache" / "replay_dag" / "f05").resolve()
    assert observation_payload["selected_target_day_count"] == 30
    assert observation_payload["observation_context_day_count"] == 34


def test_legacy_observation_semantics_are_derived_only_from_false_permissions(
    tmp_path: Path,
) -> None:
    observation_path, observation_payload = _write_observation_manifest(tmp_path)
    observation_payload.pop("continuation_days_create_economic_test_rows")
    for row in observation_payload["days"]:
        row.pop("economic_test_row_eligible")
        row.pop("washout_continuation_eligible")
    observation_payload["canonical_manifest_sha256"] = adapter_module._document_sha256(
        observation_payload,
        "canonical_manifest_sha256",
    )
    observation_path.write_text(
        json.dumps(observation_payload, sort_keys=True),
        encoding="ascii",
    )
    rows = _portable_binding_rows(tmp_path, observation_path, observation_payload)

    options = adapter_module._resolve_execution_options(rows)

    assert options.workers == 6


def test_legacy_observation_semantics_reject_any_enabled_permission(
    tmp_path: Path,
) -> None:
    observation_path, observation_payload = _write_observation_manifest(tmp_path)
    observation_payload.pop("continuation_days_create_economic_test_rows")
    for row in observation_payload["days"]:
        row.pop("economic_test_row_eligible")
        row.pop("washout_continuation_eligible")
    observation_payload["permissions"]["economic_outcomes_read"] = True
    observation_payload["canonical_manifest_sha256"] = adapter_module._document_sha256(
        observation_payload,
        "canonical_manifest_sha256",
    )
    observation_path.write_text(
        json.dumps(observation_payload, sort_keys=True),
        encoding="ascii",
    )
    rows = _portable_binding_rows(tmp_path, observation_path, observation_payload)

    with pytest.raises(adapter_module.OfflineReplayAdapterError, match="permissions"):
        adapter_module._resolve_execution_options(rows)


def test_portable_day_cache_cannot_escape_governed_replay_dag_root(
    tmp_path: Path,
) -> None:
    observation_path, observation_payload = _write_observation_manifest(tmp_path)
    rows = _portable_binding_rows(tmp_path, observation_path, observation_payload)
    rows.loc[0, "portable_day_cache_root"] = str(tmp_path / "outside-replay-dag")

    with pytest.raises(
        adapter_module.OfflineReplayAdapterError,
        match="cache escaped the governed replay_dag root",
    ):
        adapter_module._resolve_execution_options(rows)


def test_missing_portable_binding_fails_closed_before_replay(tmp_path: Path) -> None:
    rows = pd.DataFrame(
        {
            "portable_replay_binding_path": (str(tmp_path / "missing.json"),),
            "portable_replay_binding_sha256": (SHA_A,),
            "portable_day_cache_root": (str(tmp_path / "cache"),),
            "day_replay_workers": (1,),
        }
    )
    with pytest.raises(adapter_module.OfflineReplayAdapterMechanicsMissing) as captured:
        adapter_module._resolve_execution_options(rows)
    assert captured.value.missing == ("valid_portable_replay_binding",)


def test_portable_binding_missing_fixed_api_role_fails_closed(tmp_path: Path) -> None:
    observation_path, observation_payload = _write_observation_manifest(tmp_path)
    rows = _portable_binding_rows(tmp_path, observation_path, observation_payload)
    path = Path(str(rows.loc[0, "portable_replay_binding_path"]))
    payload = json.loads(path.read_text(encoding="ascii"))
    del payload["fixed_bridge"]["canonical_api_bindings"][
        "canonical_snapshot_emitter_factory_binding"
    ]
    path.write_text(json.dumps(payload, sort_keys=True), encoding="ascii")
    rows.loc[0, "portable_replay_binding_sha256"] = _sha256_file(path)
    with pytest.raises(adapter_module.OfflineReplayAdapterMechanicsMissing) as captured:
        adapter_module._resolve_execution_options(rows)
    assert captured.value.missing == ("canonical_snapshot_emitter_factory_binding",)


def test_portable_binding_rejects_symbol_or_source_hash_substitution(
    tmp_path: Path,
) -> None:
    observation_path, observation_payload = _write_observation_manifest(tmp_path)
    rows = _portable_binding_rows(tmp_path, observation_path, observation_payload)
    path = Path(str(rows.loc[0, "portable_replay_binding_path"]))
    payload = json.loads(path.read_text(encoding="ascii"))
    payload["fixed_bridge"]["canonical_api_bindings"][
        "canonical_backtest_tick_arm_executor_binding"
    ]["symbol"] = "caller_executor"
    path.write_text(json.dumps(payload, sort_keys=True), encoding="ascii")
    rows.loc[0, "portable_replay_binding_sha256"] = _sha256_file(path)
    with pytest.raises(
        adapter_module.OfflineReplayAdapterError,
        match="module, symbol, or source SHA",
    ):
        adapter_module._resolve_execution_options(rows)


def test_day_job_rejects_caller_executor_injection_before_projection() -> None:
    job = adapter_module._DayReplayJob(
        kind="one_shot",
        utc_day=DAY,
        cache_key=_cache_key(stage=adapter_module.ONE_SHOT_STAGE),
        payload={
            "fixed_bridge": _fixed_bridge(),
            "executor_callable": lambda: None,
        },
    )
    with pytest.raises(adapter_module.OfflineReplayAdapterError, match="injection"):
        adapter_module._execute_fixed_day_job(job)
