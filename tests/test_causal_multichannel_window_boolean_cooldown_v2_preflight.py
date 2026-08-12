from __future__ import annotations

import json
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_v2_preflight as preflight,
)

EXPECTED_COMPONENTS = {
    "features",
    "windows",
    "native_features",
    "snapshot",
    "source_manifest",
    "predicate_materializer",
    "strict_checkpoint",
    "replay_emitter",
    "shared_prefix",
    "strict_labels",
    "label_panel",
    "mechanics_receipt",
    "predicates",
    "nested_oof",
    "strict_label_panel_runner",
    "native_sequence_support_mapping",
    "study",
    "execution_replay_abi",
    "native_queue_scheduler",
}


def test_durable_benchmark_evidence_precedes_ephemeral_search() -> None:
    assert preflight.STRICT_BENCHMARK_SEARCH_ROOTS[0] == (
        preflight.DURABLE_BENCHMARK_ROOT
    )
    assert preflight.STRICT_BENCHMARK_SEARCH_ROOTS[-1] == Path(tempfile.gettempdir())


def test_durable_benchmark_is_preferred_over_newer_ephemeral_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    durable = tmp_path / "durable"
    ephemeral = tmp_path / "ephemeral"
    run_name = (
        "causal_multichannel_window_boolean_cooldown_duration_v2_benchmark_v10"
    )
    _write_benchmark_arm(
        durable / run_name / "labels" / "opportunity",
        preflight.features.BUY_DURATION_POLICY_IDS[0],
    )
    _write_benchmark_arm(
        ephemeral / run_name / "labels" / "opportunity",
        preflight.features.BUY_DURATION_POLICY_IDS[0],
    )
    monkeypatch.setattr(preflight, "DURABLE_BENCHMARK_ROOT", durable)
    monkeypatch.setattr(
        preflight,
        "STRICT_BENCHMARK_SEARCH_ROOTS",
        (durable, ephemeral),
    )

    audit = preflight._latest_strict_benchmark_queue_audit()

    assert audit["run_path"] == str(durable / run_name)


def _write_benchmark_arm(
    directory: Path,
    arm_id: str,
    *,
    ambiguity: int = 0,
    invalidated: int = 0,
    eligible: bool = True,
) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    unsupported_reasons = []
    if ambiguity:
        unsupported_reasons.append("exchange_book_queue_ambiguous_event_count")
    if invalidated:
        unsupported_reasons.append(
            "exchange_book_queue_invalidated_order_count"
        )
    payload = {
        "identity": preflight.IDENTITY,
        "arm_id": arm_id,
        "strict_execution_contract": {
            "exchange_book_queue_missing_count": 0,
            "exchange_book_queue_invalidated_order_count": invalidated,
            "exchange_book_queue_ambiguous_event_count": ambiguity,
            "exchange_book_cancel_trade_ambiguous_order_count": 0,
            "exchange_book_cancel_book_ambiguous_order_count": 0,
            "strict_native_label_eligible": eligible,
            "strict_native_label_unsupported_reasons": unsupported_reasons,
        },
    }
    (directory / f"arm-{arm_id}.json").write_text(
        json.dumps(payload), encoding="utf-8"
    )


def _write_benchmark_parent_completion(run: Path) -> None:
    day = "2026-04-17"
    day_root = (
        run
        / "support_identity=full_D_minus_1_D_D_plus_1"
        / "feature_block=M2"
        / "days"
        / day
    )
    day_root.mkdir(parents=True)
    assignment_ts_ns = int(
        datetime(2026, 4, 17, 12, tzinfo=UTC).timestamp() * 1_000_000_000
    )
    side_role_cells = (
        ("BUY", "opener"),
        ("SELL", "opener"),
        ("BUY", "add"),
        ("SELL", "add"),
    )
    snapshots = pd.DataFrame(
        {
            "m0_context_json": [
                json.dumps(
                    {
                        "assignment_ts_ns": assignment_ts_ns + index,
                        "side": side,
                        "role_at_fill": role,
                    }
                )
                for index, (side, role) in enumerate(
                    side_role_cells * 12
                )
            ]
        }
    )
    snapshots_path = day_root / "assignment_snapshots.parquet"
    snapshots.to_parquet(snapshots_path, index=False)
    manifest = {
        "target_day": day,
        "max_opportunities": 48,
        "assignment_snapshots": {
            "rows": 48,
            "sha256": preflight._sha256(snapshots_path),
        },
        "shared_prefix_execution_audit": {
            "opportunities_dispatched": 48,
            "supervisor_processes_completed": 48,
            "pending_supervisors": 0,
        },
        "parent_stop_audit": {
            "configured_stop_ts_ms": int(
                datetime(2026, 4, 18, tzinfo=UTC).timestamp() * 1_000
            ),
            "triggered": True,
            "trigger_ts_ms": int(
                datetime(2026, 4, 18, tzinfo=UTC).timestamp() * 1_000
            ),
            "new_assignments_after_target_day_boundary": 0,
        },
        "strict_native_queue": {
            "missing_queue_seed_count": 0,
            "missing_queue_seed_trace": [],
            "source_gap_events": 0,
        },
        "one_shot_label_manifests": [{} for _ in range(48)],
    }
    manifest_path = day_root / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    (day_root / "_SUCCESS").write_text(
        json.dumps({"manifest_sha256": preflight._sha256(manifest_path)}),
        encoding="utf-8",
    )


def test_preflight_binds_all_execution_components() -> None:
    spec = preflight._load_json(preflight.SPEC)
    amendments = preflight._validate_amendments(spec)
    audit = preflight._validate_component_bindings(spec, amendments)

    assert set(audit) == EXPECTED_COMPONENTS
    assert all(row["identity_verified"] for row in audit.values())
    assert all(row["executable_binding_verified"] for row in audit.values())
    assert all(Path(row["path"]).is_file() for row in audit.values())
    assert all(Path(row["test_path"]).is_file() for row in audit.values())
    assert audit["nested_oof"]["declared_in_execution_amendment"] is True
    assert (
        audit["strict_label_panel_runner"]["declared_in_execution_amendment"]
        is True
    )
    assert audit["study"]["declared_in_execution_amendment"] is True
    assert audit["shared_prefix"]["declared_in_execution_amendment"] is True
    assert audit["strict_labels"]["declared_in_execution_amendment"] is True
    assert audit["label_panel"]["declared_in_execution_amendment"] is True
    assert audit["execution_replay_abi"]["declared_in_execution_amendment"] is True
    assert audit["features"]["declared_in_execution_amendment"] is True
    assert (
        audit["native_sequence_support_mapping"][
            "declared_in_execution_amendment"
        ]
        is True
    )


def test_preflight_binds_feature_and_execution_amendments() -> None:
    spec = preflight._load_json(preflight.SPEC)

    audit = preflight._validate_amendments(spec)

    assert audit["feature_semantics"]["sha256"] == (
        preflight.FEATURE_SEMANTICS_AMENDMENT_SHA256
    )
    assert audit["execution"]["sha256"] == preflight.EXECUTION_AMENDMENT_SHA256
    assert audit["execution_v2"]["sha256"] == (
        preflight.EXECUTION_AMENDMENT_V2_SHA256
    )
    assert audit["execution_v3"]["sha256"] == (
        preflight.EXECUTION_AMENDMENT_V3_SHA256
    )
    assert audit["execution_v4"]["sha256"] == (
        preflight.EXECUTION_AMENDMENT_V4_SHA256
    )
    assert audit["execution_v5"]["sha256"] == (
        preflight.EXECUTION_AMENDMENT_V5_SHA256
    )
    assert audit["execution_v6"]["sha256"] == (
        preflight.EXECUTION_AMENDMENT_V6_SHA256
    )
    assert audit["execution_v7"]["sha256"] == (
        preflight.EXECUTION_AMENDMENT_V7_SHA256
    )
    assert audit["execution_v8"]["sha256"] == (
        preflight.EXECUTION_AMENDMENT_V8_SHA256
    )
    assert audit["execution_v9"]["sha256"] == (
        preflight.EXECUTION_AMENDMENT_V9_SHA256
    )
    assert (
        audit["execution"]["payload"]["reporting_contract"]
        ["exact_label_economic_denominator"]["pooled"]
        == 41
    )
    assert (
        audit["execution_v2"]["payload"]["parallelism_contract"]
        ["prebuild_worker_cap"]
        == 4
    )
    assert (
        audit["execution_v3"]["payload"]["post_outer_oof_gate_contract"]
        ["feature_family_selection"]["incremental_comparison_count"]
        == 2
    )
    assert (
        audit["execution_v4"]["payload"]["continuous_state_comparator"]
        ["model_family"]
        == "raw_state_multioutput_regression_tree_diagnostic"
    )
    assert (
        audit["execution_v5"]["payload"]["raw_warmup_admission_contract"]
        ["calendar_cutoff_inference_allowed"]
        is False
    )
    assert (
        audit["execution_v6"]["sequence_support"]["counts"]
        ["formal_sequence_supported_days"]
        == 41
    )
    assert audit["execution_v6"]["source_union"]["formal_segment_count"] == 8
    assert audit["execution_v7"]["target_receipt_schema"].endswith(
        "native_cache_target_72h_receipt.v3"
    )
    assert (
        audit["execution_v8"]["payload"]["strict_arm_admission_contract"]
        ["treatment_queue_missing_seed"]
        == "retain_arm_as_unsupported"
    )
    assert (
        audit["execution_v9"]["payload"]["formal_execution_identity"]
        ["formal_run_directory"]
        == "formal_full_support_41d_v9"
    )
    assert (
        audit["execution_v9"]["payload"]["strict_queue_trace_contract"]
        ["trace_must_be_unbounded_and_untruncated"]
        is True
    )


def test_component_hash_drift_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    spec = preflight._load_json(preflight.SPEC)
    amendments = preflight._validate_amendments(spec)
    monkeypatch.setitem(
        preflight.IMPLEMENTATION_BINDINGS["replay_emitter"],
        "sha256",
        "0" * 64,
    )

    with pytest.raises(preflight.PreflightError, match="replay_emitter.*drifted"):
        preflight._validate_component_bindings(spec, amendments)


def test_execution_amendment_hash_drift_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = preflight._load_json(preflight.SPEC)
    monkeypatch.setattr(preflight, "EXECUTION_AMENDMENT_SHA256", "0" * 64)

    with pytest.raises(preflight.PreflightError, match="execution amendment.*drifted"):
        preflight._validate_amendments(spec)


def test_execution_amendment_v2_hash_drift_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = preflight._load_json(preflight.SPEC)
    monkeypatch.setattr(preflight, "EXECUTION_AMENDMENT_V2_SHA256", "0" * 64)

    with pytest.raises(
        preflight.PreflightError,
        match="execution amendment successor.*drifted",
    ):
        preflight._validate_amendments(spec)


def test_execution_amendment_v3_hash_drift_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = preflight._load_json(preflight.SPEC)
    monkeypatch.setattr(preflight, "EXECUTION_AMENDMENT_V3_SHA256", "0" * 64)

    with pytest.raises(
        preflight.PreflightError,
        match="identity-hardening successor.*drifted",
    ):
        preflight._validate_amendments(spec)


def test_execution_amendment_v4_hash_drift_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = preflight._load_json(preflight.SPEC)
    monkeypatch.setattr(preflight, "EXECUTION_AMENDMENT_V4_SHA256", "0" * 64)

    with pytest.raises(
        preflight.PreflightError,
        match="continuous-comparator execution successor.*drifted",
    ):
        preflight._validate_amendments(spec)


def test_execution_amendment_v6_hash_drift_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = preflight._load_json(preflight.SPEC)
    monkeypatch.setattr(preflight, "EXECUTION_AMENDMENT_V6_SHA256", "0" * 64)

    with pytest.raises(
        preflight.PreflightError,
        match="native-sequence execution successor.*drifted",
    ):
        preflight._validate_amendments(spec)


def test_execution_amendment_v7_hash_drift_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = preflight._load_json(preflight.SPEC)
    monkeypatch.setattr(preflight, "EXECUTION_AMENDMENT_V7_SHA256", "0" * 64)

    with pytest.raises(
        preflight.PreflightError,
        match="target-receipt ABI successor.*drifted",
    ):
        preflight._validate_amendments(spec)


def test_execution_amendment_v8_hash_drift_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = preflight._load_json(preflight.SPEC)
    monkeypatch.setattr(preflight, "EXECUTION_AMENDMENT_V8_SHA256", "0" * 64)

    with pytest.raises(
        preflight.PreflightError,
        match="strict-arm admission successor.*drifted",
    ):
        preflight._validate_amendments(spec)


def test_execution_amendment_v9_hash_drift_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = preflight._load_json(preflight.SPEC)
    monkeypatch.setattr(preflight, "EXECUTION_AMENDMENT_V9_SHA256", "0" * 64)

    with pytest.raises(
        preflight.PreflightError,
        match="queue-trace and formal-schema successor.*drifted",
    ):
        preflight._validate_amendments(spec)


def test_formal_artifact_paths_are_v9_isolated() -> None:
    assert preflight.FORMAL_PANEL_MANIFEST.parent.name == "formal_full_support_41d_v9"
    assert (
        preflight.FORMAL_SOURCE_PREBUILD_MANIFEST.parent.parent.name
        == "formal_full_support_41d"
    )
    assert preflight.STUDY_ADMISSION_ROOT.name == (
        "nested_chronological_oof_v2_execution_v9"
    )


def test_latest_benchmark_queue_ambiguity_is_excluded_without_blocking_denominator(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = tmp_path / (
        "causal_multichannel_window_boolean_cooldown_duration_v2_benchmark_test"
    )
    opportunity = run / "labels" / "opportunity"
    for index, arm_id in enumerate(preflight.features.BUY_DURATION_POLICY_IDS):
        _write_benchmark_arm(
            opportunity,
            arm_id,
            ambiguity=1 if index == 0 else 0,
            invalidated=1 if index == 0 else 0,
            eligible=index != 0,
        )
    _write_benchmark_arm(run / ".inflight.staging", "CONTROL_85N")
    monkeypatch.setattr(preflight, "STRICT_BENCHMARK_SEARCH_ROOTS", (tmp_path,))

    blocked = preflight._latest_strict_benchmark_queue_audit()

    assert blocked["arm_count"] == 8
    assert blocked["complete_opportunity_bundle_count"] == 1
    assert blocked["queue_totals"]["queue_ambiguous_event_count"] == 1
    assert blocked["queue_totals"]["queue_invalidated_order_count"] == 1
    assert blocked["denominator_generation_mechanics_verified"] is True
    assert blocked["execution_admission_verified"] is False
    assert blocked["all_bundles_strict_exact"] is False
    assert blocked["strict_exact_opportunity_bundle_count"] == 0

    _write_benchmark_arm(opportunity, preflight.features.BUY_DURATION_POLICY_IDS[0])
    resolved = preflight._latest_strict_benchmark_queue_audit()
    assert resolved["denominator_generation_mechanics_verified"] is True
    assert resolved["all_bundles_strict_exact"] is True
    assert resolved["execution_admission_verified"] is False


def test_benchmark_execution_admission_requires_48_bundles_and_one_exact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = tmp_path / (
        "causal_multichannel_window_boolean_cooldown_duration_v2_benchmark_test"
    )
    for bundle_index in range(48):
        opportunity = run / "labels" / f"opportunity-{bundle_index:02d}"
        for arm_index, arm_id in enumerate(
            preflight.features.BUY_DURATION_POLICY_IDS
        ):
            exact = bundle_index == 47
            bad = not exact and arm_index == 0
            _write_benchmark_arm(
                opportunity,
                arm_id,
                ambiguity=1 if bad else 0,
                invalidated=1 if bad else 0,
                eligible=not bad,
            )
    _write_benchmark_parent_completion(run)
    monkeypatch.setattr(preflight, "STRICT_BENCHMARK_SEARCH_ROOTS", (tmp_path,))

    audit = preflight._latest_strict_benchmark_queue_audit()

    assert audit["complete_opportunity_bundle_count"] == 48
    assert audit["strict_exact_opportunity_bundle_count"] == 1
    assert audit["denominator_generation_mechanics_verified"] is True
    assert audit["execution_admission_verified"] is True


def test_preflight_separates_one_shot_readiness_from_formal_blockers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        preflight.strict_baseline,
        "preflight",
        lambda: {"days": 50, "strict_complete_days": 50},
    )
    monkeypatch.setattr(
        preflight,
        "_latest_strict_benchmark_queue_audit",
        lambda: {
            "evidence_found": True,
            "receipt_identity": preflight.mechanics_receipt.RECEIPT_IDENTITY,
            "receipt_schema_version": (
                preflight.mechanics_receipt.RECEIPT_SCHEMA_VERSION
            ),
            "denominator_generation_mechanics_verified": True,
            "execution_admission_verified": True,
            "queue_totals": {"queue_ambiguous_event_count": 20},
        },
    )
    monkeypatch.setattr(
        preflight,
        "_artifact_readiness",
        lambda: {
            "formal_source_prebuild": {"admitted": False},
            "formal_label_panel": {"admitted": False},
            "predicate_admission": {"admitted": False},
            "nested_oof": {"completed": False},
        },
    )
    monkeypatch.setattr(
        preflight.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=123_456_789),
    )

    payload = preflight.preflight()

    readiness = payload["readiness"]
    assert readiness["component_bindings_verified"] is True
    assert readiness["cooldown_assignment_snapshot_replay_emitter_bound"] is True
    assert readiness["posix_copy_on_write_shared_prefix_ready"] is True
    assert readiness["strict_one_shot_execution_eligible"] is True
    assert readiness["strict_label_execution_eligible"] is True
    assert readiness["formal_source_prebuild_admitted"] is False
    assert readiness["portable_simulator_state_serialization_ready"] is False
    assert readiness["live_assignment_snapshot_emitter_ready"] is False
    assert readiness["formal_research_execution_eligible"] is False

    assert payload["blockers"] == [
        "generate and atomically admit the frozen 41-day strict-native labels",
        "fit and bind real predicate artifacts from the frozen 2025 reference populations",
        "run nested chronological OOF after labels and predicate artifacts are frozen",
    ]
    assert not any("serializ" in blocker for blocker in payload["blockers"])
    assert not any("live" in blocker for blocker in payload["blockers"])
    assert payload["authority"] == {
        "research_supported": False,
        "action_authorized": False,
        "live_authorized": False,
    }


def test_artifact_readiness_validates_formal_source_prebuild(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prebuild_manifest = tmp_path / "prebuild" / "manifest.json"
    prebuild_manifest.parent.mkdir(parents=True)
    payload = {
        "day_count": 41,
        "prefix40_full_support_count": 33,
        "added10_full_support_count": 8,
        "unique_source_day_count": 57,
        "unique_source_hours": 1_368,
        "segment_count": 8,
        "days": [{} for _ in range(41)],
        "strict_zero_counters": {"sequence_gaps": 0},
        "economic_outcomes_read": False,
        "arms_run": False,
    }
    prebuild_manifest.write_text(
        json.dumps(payload, sort_keys=True) + "\n",
        encoding="ascii",
    )
    monkeypatch.setattr(
        preflight,
        "FORMAL_SOURCE_PREBUILD_MANIFEST",
        prebuild_manifest,
    )
    monkeypatch.setattr(
        preflight,
        "FORMAL_PANEL_MANIFEST",
        tmp_path / "missing-panel.json",
    )
    monkeypatch.setattr(
        preflight,
        "PREDICATE_ADMISSION_ROOT",
        tmp_path / "missing-predicates",
    )
    monkeypatch.setattr(
        preflight,
        "STUDY_ADMISSION_ROOT",
        tmp_path / "missing-study",
    )
    monkeypatch.setattr(
        preflight.panel_runner,
        "_formal_day_universe",
        lambda _spec: (("2026-04-17",) * 41, (), (), ()),
    )
    expected_plan = SimpleNamespace(target_days=("2026-04-17",) * 41)
    monkeypatch.setattr(
        preflight.panel_runner,
        "_source_union_plan",
        lambda _days, *, formal: expected_plan if formal else None,
    )
    validated: dict[str, Any] = {}

    def fake_validate(
        manifest: dict[str, Any],
        *,
        plan: Any,
        formal: bool,
        native_cache: Path,
    ) -> None:
        validated.update(
            {
                "manifest": manifest,
                "plan": plan,
                "formal": formal,
                "native_cache": native_cache,
            }
        )

    monkeypatch.setattr(
        preflight.panel_runner,
        "_validate_prebuild_manifest",
        fake_validate,
    )

    artifacts = preflight._artifact_readiness()

    source = artifacts["formal_source_prebuild"]
    assert source["admitted"] is True
    assert source["target_day_count"] == 41
    assert source["source_day_count"] == 57
    assert source["source_hour_count"] == 1_368
    assert source["segment_count"] == 8
    assert source["target_receipt_count"] == 41
    assert source["economic_outcomes_read"] is False
    assert source["arms_run"] is False
    assert validated["plan"] is expected_plan
    assert validated["formal"] is True
    assert validated["native_cache"] == preflight.panel_runner.DEFAULT_NATIVE_CACHE


def test_missing_portable_and_live_emitters_do_not_block_strict_one_shot() -> None:
    capabilities = preflight._validate_implementation_capabilities()

    assert capabilities["replay_assignment_snapshot_emitter_implemented"] is True
    assert capabilities["posix_copy_on_write_shared_prefix_implemented"] is True
    assert capabilities["continuous_state_comparator_implemented"] is True
    assert capabilities["portable_simulator_state_serialization_implemented"] is False
    assert capabilities["live_assignment_snapshot_emitter_implemented"] is False
    assert capabilities["portable_serialization_blocks_strict_one_shot"] is False
    assert capabilities["live_emitter_blocks_strict_one_shot"] is False
