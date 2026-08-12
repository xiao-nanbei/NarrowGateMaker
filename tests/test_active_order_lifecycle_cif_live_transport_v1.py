from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from execution.order_lifecycle import QuantityWeightedOrderLifecycle
from execution.order_lifecycle_journal_v2 import (
    ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS,
    ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION,
    OrderLifecycleJournalV2BatchEmitter,
    OrderLifecycleJournalV2SourceCallback,
)
from research.families.f07_active_order_continuation.audit import (
    active_order_lifecycle_cif_live_transport_v1 as audit,
)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _seal(payload: dict[str, object], field: str) -> dict[str, object]:
    payload[field] = audit.canonical_sha256(payload)
    return payload


def _callback(
    callback_id: str,
    received_ts_ns: int,
    exchange_ts_ns: int | None = None,
) -> OrderLifecycleJournalV2SourceCallback:
    return OrderLifecycleJournalV2SourceCallback(
        callback_id=callback_id,
        callback_type="execution_report",
        received_ts_ns=received_ts_ns,
        exchange_ts_ns=exchange_ts_ns,
    )


def _lifecycle_rows(
    *, lifecycle_id: str, side: str, base_ns: int
) -> tuple[list[dict[str, object]], dict[str, int]]:
    lifecycle = QuantityWeightedOrderLifecycle(0.001, base_ns)
    emitter = OrderLifecycleJournalV2BatchEmitter(
        lifecycle_id=lifecycle_id,
        runtime_source="live",
        client_order_id=f"client-{lifecycle_id}",
        exchange_order_id=f"exchange-{lifecycle_id}",
        symbol="BTCUSDC",
        side=side,
    )
    batches = [
        emitter.emit_unseen(
            lifecycle=lifecycle,
            callback=_callback(f"{lifecycle_id}-submit", base_ns),
        )
    ]
    lifecycle.activate(base_ns + 200_000_000, exchange_ts_ns=base_ns + 100_000_000)
    batches.append(
        emitter.emit_unseen(
            lifecycle=lifecycle,
            callback=_callback(
                f"{lifecycle_id}-activate",
                base_ns + 100_000_000,
                base_ns + 100_000_000,
            ),
        )
    )
    lifecycle.request_cancel(base_ns + 1_200_000_000)
    batches.append(
        emitter.emit_unseen(
            lifecycle=lifecycle,
            callback=_callback(f"{lifecycle_id}-cancel-request", base_ns + 1_200_000_000),
        )
    )
    lifecycle.exchange_terminal(
        base_ns + 2_200_000_000,
        reason="cancel_ack",
        exchange_ts_ns=base_ns + 2_100_000_000,
    )
    batches.append(
        emitter.emit_unseen(
            lifecycle=lifecycle,
            callback=_callback(
                f"{lifecycle_id}-cancel-ack",
                base_ns + 2_100_000_000,
                base_ns + 2_100_000_000,
            ),
        )
    )
    rows = [asdict(row) for batch in batches for row in batch.rows]
    return rows, {
        "lifecycle_id": lifecycle_id,
        "feature_source_exchange_ts_ns": base_ns - 300_000_000,
        "feature_ready_ts_ns": base_ns - 200_000_000,
        "decision_ts_ns": base_ns - 100_000_000,
    }


def _reference_files(root: Path) -> dict[str, Path]:
    report_root = root / "reference"
    lockstep_path = report_root / "lockstep.json"
    artifact_path = report_root / "artifact.json"
    training_path = report_root / "training.json"

    lockstep = {
        "schema_version": "f07_order_lifecycle_v2_40day_cpp_event_lockstep_report.v1_6",
        "identity": "f07_order_lifecycle_v2_40day_cpp_event_lockstep_v1_6",
        "status": "passed",
        "formal_40day_lockstep_passed": True,
        "scope": {"mechanics_only": True, "economic_outcomes_read": False},
        "permissions": {"live_transport": False},
    }
    _seal(lockstep, "canonical_report_sha256")
    _write_json(lockstep_path, lockstep)

    cells = []
    parent_rates = []
    for side in ("BUY", "SELL"):
        for phase in ("ACTIVE", "PARTIALLY_FILLED", "CANCEL_PENDING"):
            for age_bin in range(8):
                parent_counts = {
                    "full_fill": 0,
                    "cancel_ack": 100 if phase == "CANCEL_PENDING" and age_bin == 2 else 0,
                    "other_terminal": 0,
                }
                parent_rates.append(
                    {
                        "side": side,
                        "phase": phase,
                        "risk_age_bin": age_bin,
                        "exposure_s": 1_000.0,
                        "event_counts": parent_counts,
                        "rates_per_s": {
                            cause: count / 1_000.0 for cause, count in parent_counts.items()
                        },
                    }
                )
                for remaining in ("full", "partial"):
                    for hour_bin in range(4):
                        counts = {
                            "full_fill": 0,
                            "cancel_ack": (
                                25
                                if phase == "CANCEL_PENDING"
                                and age_bin == 2
                                and remaining == "full"
                                else 0
                            ),
                            "other_terminal": 0,
                        }
                        cells.append(
                            {
                                "side": side,
                                "phase": phase,
                                "risk_age_bin": age_bin,
                                "remaining_class": remaining,
                                "utc_hour_bin": hour_bin,
                                "exposure_s": 250.0,
                                "event_counts": counts,
                                "rates_per_s": {
                                    cause: count / 250.0 for cause, count in counts.items()
                                },
                                "fallback_parent": {
                                    "side": side,
                                    "phase": phase,
                                    "risk_age_bin": age_bin,
                                },
                            }
                        )
    artifact = {
        "schema_version": "active_order_lifecycle_competing_risk_cif_artifact.v1_6",
        "identity": "active_order_lifecycle_competing_risk_cif_100ms_v1_6",
        "status": "trained_mechanics_only",
        "input_artifacts": {
            "python_cpp_lockstep": {
                "sha256": audit.file_sha256(lockstep_path),
            }
        },
        "conditioning": {
            "side": ["BUY", "SELL"],
            "phase": ["ACTIVE", "CANCEL_PENDING", "PARTIALLY_FILLED"],
            "risk_age_bin_edges_s": [0.0, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0, "inf"],
            "remaining_classes": ["full", "partial"],
            "utc_hour_bin_width": 6,
            "cell_prior_exposure_s": 30.0,
            "unseen_cell_fallback": "side_phase_risk_age_parent",
        },
        "training_counts": {
            "eligible_lifecycle_count": 200,
            "censored_lifecycle_count": 0,
            "risk_exposure_s": 10_000.0,
        },
        "parent_rates": parent_rates,
        "cells": cells,
        "scope": {
            "mechanics_only": True,
            "economic_outcomes_read": False,
            "markout_read": False,
        },
        "permissions": {"live_transport": False},
    }
    _seal(artifact, "canonical_artifact_sha256")
    _write_json(artifact_path, artifact)

    training = {
        "schema_version": "active_order_lifecycle_competing_risk_cif_training_report.v1_6",
        "identity": "active_order_lifecycle_competing_risk_cif_100ms_v1_6_training",
        "status": "passed",
        "model_artifact": {"sha256": audit.file_sha256(artifact_path)},
        "input_artifacts": {
            "python_cpp_lockstep": {"sha256": audit.file_sha256(lockstep_path)}
        },
        "training_counts": artifact["training_counts"],
        "gates": {"synthetic_reference_valid": True},
        "scope": {
            "mechanics_only": True,
            "economic_outcomes_read": False,
            "markout_read": False,
        },
        "permissions": {"live_transport": False},
    }
    _seal(training, "canonical_report_sha256")
    _write_json(training_path, training)
    return {
        "lockstep": lockstep_path,
        "artifact": artifact_path,
        "training": training_path,
    }


def _admission(root: Path) -> Path:
    admission = root / "admission"
    session = admission / "source/session"
    epoch_root = admission / "source/epoch"
    parts = session / "parts"
    parts.mkdir(parents=True)
    epoch_root.mkdir(parents=True)

    rows = []
    contexts = []
    first_rows, first_context = _lifecycle_rows(
        lifecycle_id="lifecycle-buy", side="BUY", base_ns=1_800_000_000_000_000_000
    )
    second_rows, second_context = _lifecycle_rows(
        lifecycle_id="lifecycle-sell",
        side="SELL",
        base_ns=1_800_003_600_000_000_000,
    )
    rows.extend(first_rows)
    rows.extend(second_rows)
    contexts.extend((first_context, second_context))

    data_path = parts / "part-test.parquet"
    pq.write_table(
        pa.Table.from_pylist(rows).select(list(ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS)),
        data_path,
    )
    part_manifest = {
        "schema_version": "order_lifecycle_journal_part.v2",
        "batch_id": "a" * 64,
        "runtime_identity_sha256": "pending",
        "journal_schema_version": ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION,
        "storage_format": "parquet",
        "data_file": data_path.name,
        "data_sha256": audit.file_sha256(data_path),
        "row_count": len(rows),
        "event_ids": [row["event_id"] for row in rows],
        "economic_outcomes_read": False,
    }

    clock_semantics = {
        "exchange_clock": "exchange_event_time_ns",
        "visibility_clock": "local_callback_receive_time_ns",
        "feature_visibility_rule": "feature_ready_ts_ns<=decision_ts_ns",
        "missing_exchange_clock_policy": "null_physical_exposure_and_invalidate_tape_row",
    }
    identity_evidence = {"clock_semantics": clock_semantics}
    evidence_path = epoch_root / "identity_evidence.json"
    _write_json(evidence_path, identity_evidence)
    initial_state = {"schema_version": "synthetic_initial_state.v1", "flat": True}
    initial_path = epoch_root / "initial_runtime_state.json"
    _write_json(initial_path, initial_state)
    epoch_identity = {
        "runtime_code_sha256": "1" * 64,
        "config_sha256": "2" * 64,
        "model_bundle_sha256": "3" * 64,
        "p3_sha256": "4" * 64,
        "feature_dag_sha256": "5" * 64,
        "execution_abi_sha256": "6" * 64,
        "action_enablement_sha256": "7" * 64,
        "initial_runtime_state_sha256": audit.canonical_sha256(initial_state),
        "data_source_identity_sha256": "8" * 64,
        "clock_semantics_sha256": audit.canonical_sha256(clock_semantics),
    }
    epoch_id = "prospective-test-epoch"
    epoch_manifest = {
        "schema_version": "narrowgate_prospective_baseline_epoch.v1",
        "epoch_id": epoch_id,
        "binding_status": "fully_bound",
        "identity": epoch_identity,
        "identity_sha256": audit.canonical_sha256(epoch_identity),
        "identity_evidence": {
            "path": "identity_evidence.json",
            "canonical_sha256": audit.canonical_sha256(identity_evidence),
        },
        "initial_runtime_state": {
            "path": "initial_runtime_state.json",
            "canonical_sha256": audit.canonical_sha256(initial_state),
        },
    }
    epoch_path = epoch_root / "epoch_manifest.json"
    _write_json(epoch_path, epoch_manifest)
    runtime_identity = {
        "baseline_epoch_id": epoch_id,
        "baseline_epoch_identity_sha256": epoch_manifest["identity_sha256"],
        "storage_profile": "bounded_remote_spool",
    }
    runtime_hash = audit.canonical_sha256(runtime_identity)
    runtime_path = session / "runtime_identity.json"
    _write_json(
        runtime_path,
        {
            "runtime_identity": runtime_identity,
            "runtime_identity_sha256": runtime_hash,
        },
    )
    part_manifest["runtime_identity_sha256"] = runtime_hash
    part_manifest_path = parts / "part-test.manifest.json"
    _write_json(part_manifest_path, part_manifest)
    context_path = session / "feature_visibility_context.parquet"
    pq.write_table(
        pa.Table.from_pylist(contexts).select(list(audit.FEATURE_CONTEXT_COLUMNS)),
        context_path,
    )
    core_health = {
        "closed": True,
        "rows_committed": len(rows),
        "rows_dropped": 0,
        "error_count": 0,
        "formal_collection_valid": True,
        "runtime_identity_sha256": runtime_hash,
        "orphan_payload_count": 0,
    }
    core_path = session / "health.json"
    _write_json(core_path, core_health)
    live_health = {
        "session_id": epoch_id,
        "baseline_epoch_id": epoch_id,
        "state": "closed",
        "remote_spool_valid": True,
        "formal_collection_valid": False,
        "drop_count": 0,
        "error_count": 0,
        "queue_depth": 0,
        "queue_hwm": 2,
        "callbacks_enqueued": 8,
        "callbacks_processed": 8,
        "rows_committed": len(rows),
        "enqueue_latency_p99_us": 10.0,
        "write_latency_p99_ms": 2.0,
    }
    live_path = session / "live_health.json"
    _write_json(live_path, live_health)

    roles = {
        runtime_path: "runtime_identity",
        live_path: "live_health",
        core_path: "core_health",
        epoch_path: "epoch_manifest",
        evidence_path: "epoch_identity_evidence",
        initial_path: "epoch_initial_runtime_state",
        part_manifest_path: "journal_part_manifest",
        data_path: "journal_part_data",
        context_path: "feature_visibility_context",
    }
    files = []
    for path, role in roles.items():
        files.append(
            {
                "role": role,
                "relative_path": path.relative_to(admission).as_posix(),
                "size_bytes": path.stat().st_size,
                "sha256": audit.file_sha256(path),
            }
        )
    manifest = {
        "schema_version": audit.ADMISSION_SCHEMA_VERSION,
        "identity": audit.IDENTITY,
        "feature_context_schema_version": audit.FEATURE_CONTEXT_SCHEMA_VERSION,
        "atomic_admission": True,
        "admission_complete": True,
        "economic_outcomes_read": False,
        "files": files,
    }
    _seal(manifest, "manifest_sha256")
    _write_json(admission / "admission_manifest.json", manifest)
    return admission


def _spec(root: Path, refs: dict[str, Path], **gate_overrides: object) -> Path:
    gates = {
        "valid_fraction_abs_delta_lte": 0.05,
        "composition_total_variation_lte": 0.15,
        "minimum_observed_hours": 0.0,
        "minimum_lifecycle_count": 2,
        "minimum_terminal_count_per_side": 1,
        "minimum_risk_exposure_s": 1.0,
        "unsupported_exposure_fraction_lte": 0.05,
    }
    gates.update(gate_overrides)
    spec = {
        "schema_version": audit.SPEC_SCHEMA_VERSION,
        "identity": audit.IDENTITY,
        "status": "frozen_before_prospective_tape_read",
        "frozen_at_utc": "2026-08-08T00:00:00Z",
        "last_materially_modified": "2026-08-08",
        "purpose": "synthetic contract test",
        "scope": {"outcome_blind": True, "economic_outcomes_read": False},
        "input_contract": {"admission_schema_version": audit.ADMISSION_SCHEMA_VERSION},
        "reference_artifacts": {
            "cif_artifact": {
                "sha256": audit.file_sha256(refs["artifact"]),
                "schema_version": "active_order_lifecycle_competing_risk_cif_artifact.v1_6",
                "identity": "active_order_lifecycle_competing_risk_cif_100ms_v1_6",
            },
            "training_report": {
                "sha256": audit.file_sha256(refs["training"]),
                "schema_version": "active_order_lifecycle_competing_risk_cif_training_report.v1_6",
                "identity": "active_order_lifecycle_competing_risk_cif_100ms_v1_6_training",
            },
            "lockstep_report": {
                "sha256": audit.file_sha256(refs["lockstep"]),
                "schema_version": "f07_order_lifecycle_v2_40day_cpp_event_lockstep_report.v1_6",
                "identity": "f07_order_lifecycle_v2_40day_cpp_event_lockstep_v1_6",
            },
        },
        "clock_contract": {"feature_ready_causal_ordering_required": True},
        "support_contract": {"cell_identity": "side_phase_age_remaining_hour"},
        "transport_gates": gates,
        "output_contract": {"transport_supported_emitted": True},
        "implementation_identity": {
            "audit_sha256": audit.file_sha256(Path(audit.__file__).resolve()),
            "test_sha256": "0" * 64,
        },
        "permissions": {
            "action_authorized": False,
            "economic_evaluation_authorized": False,
            "live_policy_authorized": False,
        },
    }
    _seal(spec, "canonical_spec_sha256")
    path = root / "spec.json"
    _write_json(path, spec)
    return path


def _run(tmp_path: Path, *, spec_overrides: dict[str, object] | None = None):
    refs = _reference_files(tmp_path)
    admission = _admission(tmp_path)
    spec = _spec(tmp_path, refs, **(spec_overrides or {}))
    output = tmp_path / "report.json"
    report = audit.run_transport_audit(
        spec_path=spec,
        admission_dir=admission,
        cif_artifact_path=refs["artifact"],
        training_report_path=refs["training"],
        lockstep_report_path=refs["lockstep"],
        output_path=output,
    )
    return report, output, admission, spec, refs


def _reseal_admission(admission: Path, changed_path: Path) -> None:
    manifest_path = admission / "admission_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.pop("manifest_sha256")
    relative = changed_path.relative_to(admission).as_posix()
    for record in manifest["files"]:
        if record["relative_path"] == relative:
            record["size_bytes"] = changed_path.stat().st_size
            record["sha256"] = audit.file_sha256(changed_path)
            break
    _seal(manifest, "manifest_sha256")
    _write_json(manifest_path, manifest)


def test_complete_outcome_blind_transport_passes_and_keeps_policy_locked(
    tmp_path: Path,
) -> None:
    report, output, *_ = _run(tmp_path)

    assert report["transport_supported"] is True
    assert report["status"] == "passed"
    assert report["permissions"] == {
        "transport_supported": True,
        "action_authorized": False,
        "economic_evaluation_authorized": False,
        "live_policy_authorized": False,
    }
    assert report["reference_comparison"]["valid_fraction_abs_delta"] == 0.0
    assert report["reference_comparison"][
        "cancel_role_composition_total_variation"
    ] == 0.0
    assert report["live_support"]["unsupported_fraction"] == 0.0
    assert output.is_file()


def test_missing_exact_feature_context_fails_closed(tmp_path: Path) -> None:
    report, _, admission, spec, refs = _run(tmp_path)
    assert report["transport_supported"] is True
    manifest_path = admission / "admission_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    context = next(
        row for row in manifest["files"] if row["role"] == "feature_visibility_context"
    )
    (admission / context["relative_path"]).unlink()
    manifest["files"].remove(context)
    manifest.pop("manifest_sha256")
    _seal(manifest, "manifest_sha256")
    _write_json(manifest_path, manifest)

    failed = audit.run_transport_audit(
        spec_path=spec,
        admission_dir=admission,
        cif_artifact_path=refs["artifact"],
        training_report_path=refs["training"],
        lockstep_report_path=refs["lockstep"],
        output_path=tmp_path / "failed.json",
    )
    assert failed["transport_supported"] is False
    assert "feature_visibility_context" in failed["failure_reason"]


def test_feature_ready_after_decision_fails_closed(tmp_path: Path) -> None:
    _, _, admission, spec, refs = _run(tmp_path)
    context_path = admission / "source/session/feature_visibility_context.parquet"
    rows = pq.read_table(context_path).to_pylist()
    rows[0]["feature_ready_ts_ns"] = rows[0]["decision_ts_ns"] + 1
    pq.write_table(pa.Table.from_pylist(rows).select(list(audit.FEATURE_CONTEXT_COLUMNS)), context_path)
    _reseal_admission(admission, context_path)

    report = audit.run_transport_audit(
        spec_path=spec,
        admission_dir=admission,
        cif_artifact_path=refs["artifact"],
        training_report_path=refs["training"],
        lockstep_report_path=refs["lockstep"],
        output_path=tmp_path / "bad-clock.json",
    )
    assert report["transport_supported"] is False
    assert "feature-ready causal ordering" in report["failure_reason"]


def test_partial_epoch_and_writer_drop_each_fail_closed(tmp_path: Path) -> None:
    _, _, admission, spec, refs = _run(tmp_path)
    epoch_path = admission / "source/epoch/epoch_manifest.json"
    epoch = json.loads(epoch_path.read_text(encoding="utf-8"))
    epoch["binding_status"] = "partially_bound"
    _write_json(epoch_path, epoch)
    _reseal_admission(admission, epoch_path)
    report = audit.run_transport_audit(
        spec_path=spec,
        admission_dir=admission,
        cif_artifact_path=refs["artifact"],
        training_report_path=refs["training"],
        lockstep_report_path=refs["lockstep"],
        output_path=tmp_path / "partial.json",
    )
    assert report["transport_supported"] is False
    assert "not fully_bound" in report["failure_reason"]

    fresh = tmp_path / "fresh"
    _, _, admission, spec, refs = _run(fresh)
    health_path = admission / "source/session/live_health.json"
    health = json.loads(health_path.read_text(encoding="utf-8"))
    health["drop_count"] = 1
    _write_json(health_path, health)
    _reseal_admission(admission, health_path)
    report = audit.run_transport_audit(
        spec_path=spec,
        admission_dir=admission,
        cif_artifact_path=refs["artifact"],
        training_report_path=refs["training"],
        lockstep_report_path=refs["lockstep"],
        output_path=fresh / "drop.json",
    )
    assert report["transport_supported"] is False
    assert "zero_drop" in report["failure_reason"]


def test_insufficient_frozen_denominator_fails_without_relaxation(tmp_path: Path) -> None:
    report, *_ = _run(tmp_path, spec_overrides={"minimum_lifecycle_count": 3})

    assert report["transport_supported"] is False
    assert report["gates"]["minimum_lifecycle_count"] is False
    assert report["reference_comparison"]["valid_fraction_abs_delta_limit"] == 0.05
    assert report["reference_comparison"]["composition_total_variation_limit"] == 0.15


def test_cli_returns_zero_for_supported_input(tmp_path: Path) -> None:
    _, _, admission, spec, refs = _run(tmp_path)
    assert (
        audit.main(
            [
                "--spec",
                str(spec),
                "--admission-dir",
                str(admission),
                "--cif-artifact",
                str(refs["artifact"]),
                "--training-report",
                str(refs["training"]),
                "--lockstep-report",
                str(refs["lockstep"]),
                "--out",
                str(tmp_path / "cli.json"),
            ]
        )
        == 0
    )
