from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from models.replay.restart_aware_continuous_ab import (
    ContinuousABPreflightError,
    ordered_days,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_restart_aware_continuous_ab as subject,
)


def _write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _refresh_amendment_parent(amendment_path: Path, spec_path: Path) -> None:
    amendment = json.loads(amendment_path.read_text(encoding="utf-8"))
    amendment["parent_preflight"] = {
        "path": str(spec_path),
        "sha256": subject.sha256_file(spec_path),
    }
    amendment["canonical_amendment_sha256"] = subject._canonical_document_sha256(
        amendment,
        identity_field="canonical_amendment_sha256",
    )
    _write_json(amendment_path, amendment)


def _bound_spec(tmp_path: Path) -> tuple[Path, Path, list[dict[str, object]]]:
    spec = json.loads(subject.DEFAULT_SPEC.read_text(encoding="utf-8"))
    days = list(ordered_days(subject.START_DAY, subject.END_DAY))
    bundle = tmp_path / "bundle_meta.json"
    dag = tmp_path / "feature_dag.json"
    overlay_root = tmp_path / "overlays"
    overlay_root.mkdir()
    _write_json(
        bundle,
        {
            "schema_version": subject.one_second_replay.training.BUNDLE_SCHEMA_VERSION,
            "identity": subject.one_second_replay.schema.IDENTITY,
            "heads": {
                head: {"model": {}, "metadata": {}}
                for head in subject.one_second_replay.training.HEAD_SPECS
            },
            "head_count": 13,
            "training_identity": {
                "inference_cadence_ms": subject.one_second_schema.CADENCE_MS,
                "feature_order_sha256": subject.one_second_schema.feature_order_sha256(),
                "heads": list(subject.one_second_replay.training.HEAD_SPECS),
            },
            "atomic_admission": True,
            "prediction_outcomes_read": False,
            "economic_outcomes_read": False,
            "prediction_authority": False,
            "action_authority": False,
            "live_authority": False,
        },
    )
    _write_json(dag, subject.one_second_full_schema.full_feature_contract_payload())
    index = tmp_path / "overlay_index.json"
    bundle_sha = subject.sha256_file(bundle)
    (tmp_path / "_SUCCESS").write_text(bundle_sha + "\n", encoding="ascii")
    _write_json(
        index,
        {
            "schema_version": "causal_v12_1s_prediction_overlay_index.v1",
            "identity": "test_71d_overlay_index",
            "calendar_days": days,
            "research_bundle_sha256": bundle_sha,
            "overlays": [
                {
                    "day": day,
                    "directory": day,
                    "manifest_sha256": "c" * 64,
                }
                for day in days
            ],
        },
    )
    spec["candidate"] = {
        "identity": "test_f03_1s_candidate",
        "bundle_meta": {"path": str(bundle), "sha256": bundle_sha},
        "feature_dag": {"path": str(dag), "sha256": subject.sha256_file(dag)},
        "overlay_root": str(overlay_root),
        "overlay_index": {"path": str(index), "sha256": subject.sha256_file(index)},
        "cadence_ms": 1000,
        "all_13_heads_required": True,
        "feature_ready_not_after_decision": True,
    }
    spec_path = tmp_path / "spec.json"
    _write_json(spec_path, spec)

    market_file = tmp_path / "market.parquet"
    feature_file = tmp_path / "feature.parquet"
    market_file.write_bytes(b"market")
    feature_file.write_bytes(b"feature")
    source_rows: list[dict[str, object]] = []
    for index_number, day in enumerate(days):
        source_rows.append(
            {
                "day": day,
                "book_identity": (
                    "native_available" if index_number < 52 else "provider_normalized_sensitivity"
                ),
                "book_root": str(tmp_path),
                "bbo_path": str(market_file),
                "l2_path": str(market_file),
                "feature_identity": "test_feature",
                "feature_path": str(feature_file),
            }
        )
    bound_rows = subject._bind_source_artifacts(source_rows)
    plan = subject.build_complete_calendar_plan(
        calendar_manifest_path=subject.CALENDAR_MANIFEST,
        source_rows=bound_rows,
        start_day=subject.START_DAY,
        end_day=subject.END_DAY,
    )
    amendment = json.loads(subject.DEFAULT_AMENDMENT.read_text(encoding="utf-8"))
    amendment["parent_preflight"] = {
        "path": str(spec_path),
        "sha256": subject.sha256_file(spec_path),
    }
    amendment["source_artifact_manifest"]["canonical_sha256"] = plan.source_artifact_manifest_sha256
    amendment["canonical_amendment_sha256"] = subject._canonical_document_sha256(
        amendment,
        identity_field="canonical_amendment_sha256",
    )
    amendment_path = tmp_path / "amendment.json"
    _write_json(amendment_path, amendment)
    return spec_path, amendment_path, source_rows


def _baseline_binding() -> dict[str, object]:
    return {
        "pointer": {
            "baseline_id": subject.one_second_replay.EXPECTED_BASELINE_ID,
            "live_config_sha256": "889f605dc6a057874a8070fd86cbd21a0c8eb050156315c1dc6f48ec9acb48f5",
            "dynamic_fill_hazard_action_enabled": False,
            "buy_fill_selection_live_enabled": False,
        },
        "identity": {"baseline_id": subject.one_second_replay.EXPECTED_BASELINE_ID},
        "identity_sha256": "bfe835bf4b76fc675cd450eccf248cd1a3d179e2f9755425b40889f042c44638",
    }


def test_default_frozen_spec_fails_closed_without_candidate_bundle() -> None:
    with pytest.raises(subject.F03ContinuousABPreflightError, match="candidate identity"):
        subject.validate_preflight()


def test_preflight_builds_all_71_trading_days_with_one_shared_restart_timeline(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    spec_path, amendment_path, source_rows = _bound_spec(tmp_path)
    monkeypatch.setattr(subject, "load_operational_baseline_binding", _baseline_binding)
    result = subject.validate_preflight(
        spec_path,
        amendment_path=amendment_path,
        source_rows=source_rows,
        verify_daily_overlays=False,
    )
    assert result.plan.calendar_day_count == 71
    assert len(result.plan.restart_intervals) == 107
    assert len(result.requests) == 71
    assert sum(row.source.book_identity == "native_available" for row in result.requests) == 52
    assert (
        sum(
            row.source.book_identity == "provider_normalized_sensitivity" for row in result.requests
        )
        == 19
    )
    assert {row.restart_timeline_sha256 for row in result.requests} == {
        result.plan.restart_timeline_sha256
    }
    assert all(row.carry_economic_state_across_midnight for row in result.requests)
    assert all(row.carry_orders_queue_across_midnight for row in result.requests)
    assert result.outcome_reads_enabled is False
    assert result.tick_engine_called is False


def test_daily_request_is_not_a_daily_fresh_start(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    spec_path, amendment_path, source_rows = _bound_spec(tmp_path)
    monkeypatch.setattr(subject, "load_operational_baseline_binding", _baseline_binding)
    result = subject.validate_preflight(
        spec_path,
        amendment_path=amendment_path,
        source_rows=source_rows,
        verify_daily_overlays=False,
    )
    first, second = result.requests[:2]
    assert first.control_state_namespace == second.control_state_namespace
    assert first.candidate_state_namespace == second.candidate_state_namespace
    assert first.control_state_namespace != first.candidate_state_namespace
    assert first.read_results is False


def test_restart_clears_transient_order_state_and_preserves_economic_state(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    spec_path, amendment_path, source_rows = _bound_spec(tmp_path)
    monkeypatch.setattr(subject, "load_operational_baseline_binding", _baseline_binding)
    result = subject.validate_preflight(
        spec_path,
        amendment_path=amendment_path,
        source_rows=source_rows,
        verify_daily_overlays=False,
    )
    for interval in result.plan.restart_intervals:
        assert interval.clears_orders
        assert interval.clears_queue
        assert interval.clears_pending_cancel
        assert interval.clears_runtime_hazard
        assert interval.preserves_cash
        assert interval.preserves_inventory
        assert interval.preserves_average_entry_price
        assert interval.preserves_economic_campaign
        assert interval.warmup_lookback_start_ts_ms < interval.resume_ts_ms


def test_shared_mutable_state_namespace_is_rejected(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    spec_path, amendment_path, source_rows = _bound_spec(tmp_path)
    monkeypatch.setattr(subject, "load_operational_baseline_binding", _baseline_binding)
    result = subject.validate_preflight(
        spec_path,
        amendment_path=amendment_path,
        source_rows=source_rows,
        verify_daily_overlays=False,
    )
    contaminated = replace(
        result.plan,
        candidate_state_namespace=result.plan.control_state_namespace,
    )
    with pytest.raises(ContinuousABPreflightError, match="share mutable state"):
        contaminated.validate()


def test_candidate_artifact_hash_drift_fails_before_source_loading(tmp_path: Path) -> None:
    spec_path, amendment_path, source_rows = _bound_spec(tmp_path)
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    spec["candidate"]["bundle_meta"]["sha256"] = "0" * 64
    _write_json(spec_path, spec)
    _refresh_amendment_parent(amendment_path, spec_path)
    with pytest.raises(subject.F03ContinuousABPreflightError, match="bundle meta SHA256"):
        subject.validate_preflight(
            spec_path,
            amendment_path=amendment_path,
            source_rows=source_rows,
            verify_daily_overlays=False,
        )


def test_outcome_permission_cannot_be_enabled_in_preflight(tmp_path: Path) -> None:
    spec_path, amendment_path, source_rows = _bound_spec(tmp_path)
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    spec["permissions"]["economic_outcomes_read"] = True
    _write_json(spec_path, spec)
    with pytest.raises(subject.F03ContinuousABPreflightError, match="result/authority"):
        subject.validate_preflight(
            spec_path,
            amendment_path=amendment_path,
            source_rows=source_rows,
            verify_daily_overlays=False,
        )


def test_overlay_index_requires_exact_71_day_denominator(tmp_path: Path) -> None:
    spec_path, amendment_path, source_rows = _bound_spec(tmp_path)
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    index_path = Path(spec["candidate"]["overlay_index"]["path"])
    index = json.loads(index_path.read_text(encoding="utf-8"))
    index["calendar_days"] = index["calendar_days"][:-1]
    _write_json(index_path, index)
    spec["candidate"]["overlay_index"]["sha256"] = subject.sha256_file(index_path)
    _write_json(spec_path, spec)
    _refresh_amendment_parent(amendment_path, spec_path)
    with pytest.raises(subject.F03ContinuousABPreflightError, match="exact 71 days"):
        subject.validate_preflight(
            spec_path,
            amendment_path=amendment_path,
            source_rows=source_rows,
            verify_daily_overlays=False,
        )


def test_formal_overlay_verification_checks_each_daily_identity(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    spec_path, amendment_path, source_rows = _bound_spec(tmp_path)
    monkeypatch.setattr(subject, "load_operational_baseline_binding", _baseline_binding)

    def load_overlay(path: Path) -> SimpleNamespace:
        return SimpleNamespace(
            utc_day=path.name,
            manifest_sha256="c" * 64,
            research_bundle_sha256=json.loads(spec_path.read_text())["candidate"]["bundle_meta"][
                "sha256"
            ],
        )

    monkeypatch.setattr(
        subject.one_second_replay,
        "load_admitted_one_second_overlay",
        load_overlay,
    )
    result = subject.validate_preflight(
        spec_path,
        amendment_path=amendment_path,
        source_rows=source_rows,
        verify_daily_overlays=True,
    )
    assert len(result.requests) == 71


def test_runner_skeleton_has_no_result_reader_or_engine_entry(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    spec_path, amendment_path, source_rows = _bound_spec(tmp_path)
    monkeypatch.setattr(subject, "load_operational_baseline_binding", _baseline_binding)
    result = subject.validate_preflight(
        spec_path,
        amendment_path=amendment_path,
        source_rows=source_rows,
        verify_daily_overlays=False,
    )
    runner = subject.F03ContinuousABRunnerSkeleton(result)
    payload = runner.execution_plan_payload()
    assert payload["economic_results_schema"] is None
    assert payload["economic_result_reader"] is None
    assert payload["outcome_reads_enabled"] is False
    assert payload["tick_engine_called"] is False
    assert not hasattr(runner, "run")


def test_parent_precommit_current_canonical_identity_is_enforced(tmp_path: Path) -> None:
    spec_path, amendment_path, source_rows = _bound_spec(tmp_path)
    amendment = json.loads(amendment_path.read_text(encoding="utf-8"))
    amendment["parent_precommit_drift"]["current_parent_canonical_sha256"] = "0" * 64
    amendment["canonical_amendment_sha256"] = subject._canonical_document_sha256(
        amendment,
        identity_field="canonical_amendment_sha256",
    )
    _write_json(amendment_path, amendment)
    with pytest.raises(subject.F03ContinuousABPreflightError, match="canonical identity"):
        subject.validate_preflight(
            spec_path,
            amendment_path=amendment_path,
            source_rows=source_rows,
            verify_daily_overlays=False,
        )


def test_source_strata_must_remain_exactly_52_native_19_provider(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    spec_path, amendment_path, source_rows = _bound_spec(tmp_path)
    monkeypatch.setattr(subject, "load_operational_baseline_binding", _baseline_binding)
    source_rows[52]["book_identity"] = "native_available"
    with pytest.raises(ContinuousABPreflightError, match="52 native and 19 provider"):
        subject.validate_preflight(
            spec_path,
            amendment_path=amendment_path,
            source_rows=source_rows,
            verify_daily_overlays=False,
        )


def test_source_artifact_size_and_sha_drift_fail_closed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    spec_path, amendment_path, source_rows = _bound_spec(tmp_path)
    monkeypatch.setattr(subject, "load_operational_baseline_binding", _baseline_binding)
    Path(str(source_rows[0]["bbo_path"])).write_bytes(b"market-drift")
    with pytest.raises(subject.F03ContinuousABPreflightError, match="source artifact manifest"):
        subject.validate_preflight(
            spec_path,
            amendment_path=amendment_path,
            source_rows=source_rows,
            verify_daily_overlays=False,
        )


def test_provider_requests_explicitly_deny_exact_queue_and_lifecycle_authority(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    spec_path, amendment_path, source_rows = _bound_spec(tmp_path)
    monkeypatch.setattr(subject, "load_operational_baseline_binding", _baseline_binding)
    result = subject.validate_preflight(
        spec_path,
        amendment_path=amendment_path,
        source_rows=source_rows,
        verify_daily_overlays=False,
    )
    providers = [
        request
        for request in result.requests
        if request.source.book_identity == "provider_normalized_sensitivity"
    ]
    assert len(providers) == 19
    assert all(not request.exact_queue_authority for request in providers)
    assert all(not request.exact_lifecycle_authority for request in providers)
    assert all(request.continuous_economic_sensitivity_authority for request in providers)
    assert {
        request.continuous_accounting_contract_id for request in providers
    } == {"continuous_accounting_contract.v2"}
    assert all(request.cancel_drain_requires_terminal_ack_or_fill for request in providers)
    assert all(request.warmup_requires_source_coverage for request in providers)
    assert all(request.feature_ready_not_after_decision for request in providers)
    assert all(request.exact_authority_excludes_frozen_restart_gaps for request in providers)
    assert all(request.execution_plan_skeleton for request in providers)
    assert all(not request.full_path_executed for request in providers)
    assert all(len(request.source.artifacts) == 3 for request in providers)


def test_candidate_dag_semantics_and_bundle_relation_are_enforced(tmp_path: Path) -> None:
    spec_path, amendment_path, source_rows = _bound_spec(tmp_path)
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    dag_path = Path(spec["candidate"]["feature_dag"]["path"])
    dag = json.loads(dag_path.read_text(encoding="utf-8"))
    dag["cadence_ms"] = 10_000
    _write_json(dag_path, dag)
    spec["candidate"]["feature_dag"]["sha256"] = subject.sha256_file(dag_path)
    _write_json(spec_path, spec)
    _refresh_amendment_parent(amendment_path, spec_path)
    with pytest.raises(subject.F03ContinuousABPreflightError, match="DAG semantics"):
        subject.validate_preflight(
            spec_path,
            amendment_path=amendment_path,
            source_rows=source_rows,
            verify_daily_overlays=False,
        )


def test_restart_interface_implementation_hash_is_enforced(tmp_path: Path) -> None:
    spec_path, amendment_path, source_rows = _bound_spec(tmp_path)
    amendment = json.loads(amendment_path.read_text(encoding="utf-8"))
    amendment["execution_interfaces"]["restart_boundary"]["sha256"] = "0" * 64
    amendment["canonical_amendment_sha256"] = subject._canonical_document_sha256(
        amendment,
        identity_field="canonical_amendment_sha256",
    )
    _write_json(amendment_path, amendment)
    with pytest.raises(
        subject.F03ContinuousABPreflightError,
        match="restart boundary interface SHA256",
    ):
        subject.validate_preflight(
            spec_path,
            amendment_path=amendment_path,
            source_rows=source_rows,
            verify_daily_overlays=False,
        )
