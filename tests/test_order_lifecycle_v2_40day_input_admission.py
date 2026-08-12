from __future__ import annotations

import gzip
import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from execution.order_lifecycle_quantity_contract import (
    ORDER_LIFECYCLE_QUANTITY_CONTRACT_ID,
    TERMINAL_REMAINDER_ABS_TOLERANCE_BTC,
)
from models.replay.order_lifecycle_v2_replay_adapter import (
    OrderLifecycleV2ReplayAdapter,
)
from research.families.f07_active_order_continuation.audit.order_lifecycle_v2_40day_input_admission import (
    CPP_BINDING_SCHEMA_VERSION,
    DAY_ADMISSION_SCHEMA_VERSION,
    DEFAULT_IDENTITY,
    FROZEN_IDENTITY_SCHEMA_VERSION,
    IDENTITY,
    PANEL_ADMISSION_SCHEMA_VERSION,
    REPLAY_CAPABILITY_SCHEMA_VERSION,
    InputAdmissionError,
    artifact_identity,
    canonical_document_sha256,
    canonical_sha256,
    expected_day_interval,
    expected_runtime_identity,
    file_sha256,
    journal_schema_sha256,
    legacy_required_schema_sha256,
    preflight_40day_admission,
    validate_frozen_identity,
)
from research.families.f07_active_order_continuation.audit.order_lifecycle_v2_cpp_event_stream_binding import (
    CPP_EVENT_STREAM_MIRROR_ABI_VERSION,
    projection_schema_sha256,
)


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, sort_keys=True, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    return path


def _write_canonical(
    path: Path, payload: dict[str, object], hash_field: str
) -> Path:
    value = deepcopy(payload)
    value[hash_field] = canonical_document_sha256(value, hash_field)
    return _write_json(path, value)


def _schema_sha(path: Path) -> str:
    schema = pq.ParquetFile(path).schema_arrow
    return canonical_sha256(
        [
            {
                "name": field.name,
                "type": str(field.type),
                "nullable": bool(field.nullable),
            }
            for field in schema
        ]
    )


def _days() -> list[str]:
    start = datetime(2026, 4, 17, tzinfo=timezone.utc)
    return [(start + timedelta(days=index)).strftime("%Y-%m-%d") for index in range(40)]


def _identity(root: Path) -> dict[str, object]:
    baseline_file = _write_json(
        root / "baseline_identity.json",
        {
            "baseline_id": "synthetic-v1",
            "config": {
                "sha256": "1" * 64,
                "dynamic_fill_hazard_action_enabled": False,
            },
            "model": {
                "bundle_meta_sha256": "2" * 64,
                "feature_dag_sha256": "4" * 64,
            },
            "p3": {"sha256": "3" * 64},
        },
    )
    pointer = _write_json(
        root / "baseline_pointer.json",
        {
            "baseline_id": "synthetic-v1",
            "identity_sha256": file_sha256(baseline_file),
            "live_config_sha256": "1" * 64,
        },
    )
    source_contract = _write_json(root / "source_contract.json", {"identity": "source-v1"})
    implementation = {
        str(
            Path(
                "research/families/f07_active_order_continuation/audit/"
                "order_lifecycle_v2_40day_input_admission.py"
            ).resolve()
        ): file_sha256(
            Path(
                "research/families/f07_active_order_continuation/audit/"
                "order_lifecycle_v2_40day_input_admission.py"
            ).resolve()
        ),
        str(Path("models/replay/order_lifecycle_v2_replay_adapter.py").resolve()): (
            file_sha256(Path("models/replay/order_lifecycle_v2_replay_adapter.py").resolve())
        ),
    }
    baseline: dict[str, object] = {
        "pointer_artifact": artifact_identity(pointer),
        "identity_artifact": artifact_identity(baseline_file),
        "baseline_id": "synthetic-v1",
        "operational_config_sha256": "1" * 64,
        "model_bundle_sha256": "2" * 64,
        "p3_sha256": "3" * 64,
        "feature_dag_sha256": "4" * 64,
        "execution_abi": "synthetic-execution-v1",
        "initial_state_mode": "daily_fresh_start",
        "replay_session_scope": "fresh_start_per_target_day",
        "q90_action_enabled": False,
        "economic_outcomes_read": False,
    }
    baseline["canonical_sha256"] = canonical_sha256(baseline)
    source: dict[str, object] = {
        "artifacts": [artifact_identity(source_contract)],
        "path_relocation_contract": "absolute_test_paths",
        "market_source_manifest_canonical_sha256": "5" * 64,
    }
    source["canonical_sha256"] = canonical_sha256(source)
    identity: dict[str, object] = {
        "schema_version": FROZEN_IDENTITY_SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": "synthetic_frozen_before_mechanics_execution",
        "frozen_at_utc": "2026-08-05T00:00:00Z",
        "scope": {
            "mechanics_only": True,
            "economic_outcomes_read": False,
            "lockstep_executed": False,
        },
        "panel": {
            "ordered_utc_days": _days(),
            "day_intervals": [expected_day_interval(day) for day in _days()],
            "denominator_authority": artifact_identity(source_contract),
        },
        "baseline_runtime_identity": baseline,
        "implementation_identities": implementation,
        "schema_identities": {
            "journal_v2_schema_version": "order_lifecycle_journal.v2",
            "journal_v2_schema_sha256": journal_schema_sha256(),
            "legacy_required_schema_sha256": legacy_required_schema_sha256(),
            "writer_part_schema_version": "order_lifecycle_journal_part.v2",
            "writer_health_schema_version": "order_lifecycle_journal_writer_health.v2",
        },
        "market_data_source_identities": source,
        "admission_contract": {
            "replay_session_scope": "fresh_start_per_target_day",
            "lifecycle_sequence_starts_at_one_per_target_day": True,
            "prospective_live_epoch_transport_supported": False,
            "daily_source_role_contracts": [
                {
                    "role": "native_book_warmup",
                    "minimum_files": 1,
                    "coverage": "warmup_exact",
                },
                {
                    "role": "native_book_target",
                    "minimum_files": 1,
                    "coverage": "target_exact",
                },
                {
                    "role": "market_context_window_cache",
                    "minimum_files": 1,
                    "coverage": "warmup_and_target_exact",
                },
            ],
        },
        "current_producer_status": {
            "legacy_explicit_dual_clock": True,
            "cancel_reject_route": True,
            "sub_lot_terminal_contract": True,
            "cpp_event_stream_binding": True,
        },
        "permissions": {
            "cif_training": False,
            "economic_evaluation": False,
            "q90_action": False,
            "prospective_live_epoch_transport": False,
            "live_deployment": False,
        },
    }
    identity["canonical_identity_sha256"] = canonical_document_sha256(
        identity, "canonical_identity_sha256"
    )
    return identity


def _legacy_rows(day: str, order: dict[str, object]) -> list[dict[str, object]]:
    base_ms = int(
        datetime.strptime(day, "%Y-%m-%d")
        .replace(tzinfo=timezone.utc)
        .timestamp()
        * 1_000
    )
    events = [
        ("submit", 1_000, None, "not_submitted", "pending_new"),
        ("activate", 1_020, 1_010, "pending_new", "open"),
        ("cancel_request", 1_100, None, "open", "pending_cancel"),
        ("cancel_reject", 1_140, 1_130, "pending_cancel", "open"),
        ("cancel_request", 1_180, None, "open", "pending_cancel"),
        ("cancel_ack", 1_220, 1_210, "pending_cancel", "cancelled"),
    ]
    rows: list[dict[str, object]] = []
    for sequence, (event, visible_delta, exchange_delta, before, after) in enumerate(
        events, start=1
    ):
        visible_ms = base_ms + visible_delta
        rows.append(
            {
                "symbol": "BTCUSDC",
                "order_id": int(order["trace_id"]),
                "event_type": event,
                "event_ts_ns": visible_ms * 1_000_000,
                "event_seq": sequence,
                "event_reason": "requote" if event == "cancel_ack" else "",
                "state_before": before,
                "state_after": after,
                "order_submit_ts_ns": int(order["submit_ts"]) * 1_000_000,
                "order_qty": float(order["quantity"]),
                "remaining_qty": float(order["quantity"]),
                "event_visibility_ts_ns": visible_ms * 1_000_000,
                "event_exchange_ts_ns": (
                    (base_ms + exchange_delta) * 1_000_000
                    if exchange_delta is not None
                    else None
                ),
            }
        )
    return rows


def _source_artifact(
    *,
    role: str,
    path: Path,
    format_name: str,
    compression: str,
    start: str,
    end: str,
) -> dict[str, object]:
    return {
        "role": role,
        **artifact_identity(path),
        "format": format_name,
        "compression": compression,
        "interval_start_utc": start,
        "interval_end_utc": end,
    }


def _build_panel(root: Path) -> tuple[dict[str, object], Path]:
    identity = _identity(root / "identity")
    implementation_sha = canonical_sha256(identity["implementation_identities"])
    capability = _write_json(
        root / "capabilities.json",
        {
            "schema_version": REPLAY_CAPABILITY_SCHEMA_VERSION,
            "runtime_code_identity_sha256": implementation_sha,
            "replay_session_scope": "fresh_start_per_target_day",
            "cancel_reject_route_supported": True,
        },
    )
    binding_source = Path(
        "research/families/f07_active_order_continuation/cpp/"
        "order_lifecycle_journal_v2_mirror.cpp"
    ).resolve()
    cpp_binding_payload: dict[str, object] = {
        "schema_version": CPP_BINDING_SCHEMA_VERSION,
        "status": "bound",
        "abi_version": CPP_EVENT_STREAM_MIRROR_ABI_VERSION,
        "runtime_code_identity_sha256": implementation_sha,
        "journal_schema_version": "order_lifecycle_journal.v2",
        "journal_schema_sha256": journal_schema_sha256(),
        "projection_schema_sha256": projection_schema_sha256(),
        "quantity_contract_id": ORDER_LIFECYCLE_QUANTITY_CONTRACT_ID,
        "terminal_remainder_abs_tolerance_btc": (
            TERMINAL_REMAINDER_ABS_TOLERANCE_BTC
        ),
        "persisted_terminal_remainder_btc": 0.0,
        "cancel_reject_active_branch_observed": True,
        "cancel_reject_partially_filled_branch_observed": True,
        "lockstep_report_sha256": "6" * 64,
        "implementation_artifacts": [artifact_identity(binding_source)],
        "mechanics_only": True,
        "economic_outcomes_read": False,
        "formal_40day_lockstep_executed": False,
    }
    cpp_binding_payload["canonical_binding_sha256"] = canonical_document_sha256(
        cpp_binding_payload, "canonical_binding_sha256"
    )
    cpp_binding = _write_json(root / "cpp_binding.json", cpp_binding_payload)
    day_references: list[dict[str, object]] = []
    for day_index, day in enumerate(_days(), start=1):
        day_root = root / "days" / day
        session = f"day-{day}"
        runtime = expected_runtime_identity(identity, day)
        base_ms = int(
            datetime.strptime(day, "%Y-%m-%d")
            .replace(tzinfo=timezone.utc)
            .timestamp()
            * 1_000
        )
        order = {
            "trace_id": day_index,
            "side": "BUY",
            "submit_ts": base_ms + 1_000,
            "quote_ts": base_ms + 1_000,
            "quantity": 0.001,
            "remaining": 0.001,
        }
        adapter = OrderLifecycleV2ReplayAdapter(
            root=day_root / "journal",
            session_id=session,
            runtime_identity=runtime,
            symbol="BTCUSDC",
        )
        adapter.submit(order, base_ms + 1_000)
        adapter.activate(
            order,
            visibility_ts_ms=base_ms + 1_020,
            exchange_ts_ms=base_ms + 1_010,
        )
        adapter.request_cancel(order, base_ms + 1_100)
        adapter.cancel_reject(
            order,
            visibility_ts_ms=base_ms + 1_140,
            exchange_ts_ms=base_ms + 1_130,
        )
        adapter.request_cancel(order, base_ms + 1_180)
        adapter.cancel_ack(
            order,
            visibility_ts_ms=base_ms + 1_220,
            exchange_ts_ms=base_ms + 1_210,
        )
        adapter.close()
        session_root = day_root / "journal" / f"session-{session}"

        legacy_path = day_root / "legacy.parquet"
        pq.write_table(pa.Table.from_pylist(_legacy_rows(day, order)), legacy_path)
        interval = expected_day_interval(day)
        warmup_path = day_root / "native_warmup.csv.gz"
        target_path = day_root / "native_target.csv.gz"
        with gzip.open(warmup_path, "wb") as handle:
            handle.write(f"warmup,{day}\n".encode())
        with gzip.open(target_path, "wb") as handle:
            handle.write(f"target,{day}\n".encode())
        window_path = day_root / "window.parquet"
        pq.write_table(pa.table({"ts_ns": [base_ms * 1_000_000]}), window_path)
        source_artifacts = [
            _source_artifact(
                role="native_book_warmup",
                path=warmup_path,
                format_name="csv",
                compression="gzip",
                start=str(interval["warmup_interval"]["start_utc"]),
                end=str(interval["warmup_interval"]["end_utc"]),
            ),
            _source_artifact(
                role="native_book_target",
                path=target_path,
                format_name="csv",
                compression="gzip",
                start=str(interval["target_interval"]["start_utc"]),
                end=str(interval["target_interval"]["end_utc"]),
            ),
            _source_artifact(
                role="market_context_window_cache",
                path=window_path,
                format_name="parquet",
                compression="none",
                start=str(interval["warmup_interval"]["start_utc"]),
                end=str(interval["target_interval"]["end_utc"]),
            ),
        ]
        part_manifests = sorted((session_root / "parts").glob("part-*.manifest.json"))
        day_manifest: dict[str, object] = {
            "schema_version": DAY_ADMISSION_SCHEMA_VERSION,
            "day": day,
            "frozen_input_identity_sha256": identity["canonical_identity_sha256"],
            "interval_identity_sha256": interval["interval_identity_sha256"],
            "baseline_runtime_identity_sha256": identity["baseline_runtime_identity"][
                "canonical_sha256"
            ],
            "admission_state": "complete",
            "atomic_publish_method": "fsync_tempfile_replace",
            "journal_v2": {
                "session_root": str(session_root),
                "runtime_identity_artifact": artifact_identity(
                    session_root / "runtime_identity.json"
                ),
                "health_artifact": artifact_identity(session_root / "health.json"),
                "live_health_artifact": None,
                "part_manifest_artifacts": [
                    artifact_identity(path) for path in part_manifests
                ],
                "expected_part_count": len(part_manifests),
                "expected_row_count": 6,
                "expected_order_count": 1,
                "expected_event_count": 6,
            },
            "legacy_trace": {
                "artifact": artifact_identity(legacy_path),
                "schema_sha256": _schema_sha(legacy_path),
                "row_count": 6,
                "order_count": 1,
                "event_count": 6,
                "clock_semantics": "explicit_dual_clock",
            },
            "market_data_artifacts": source_artifacts,
            "producer_capabilities": {
                "replay_capability_artifact": artifact_identity(capability),
                "replay_session_scope": "fresh_start_per_target_day",
                "carry_in_lifecycle_count": 0,
                "left_truncation_supported": False,
                "prospective_live_epoch_transport_authorized": False,
                "cancel_reject_route_supported": True,
                "cancel_reject_observed_event_count": 1,
                "sub_lot_terminal_remainder_contract": (
                    ORDER_LIFECYCLE_QUANTITY_CONTRACT_ID
                ),
                "sub_lot_terminal_remainder_count": 0,
                "cpp_event_stream_binding": {
                    "status": "bound",
                    "artifact": artifact_identity(cpp_binding),
                    "abi_version": CPP_EVENT_STREAM_MIRROR_ABI_VERSION,
                },
            },
            "economic_outcomes_read": False,
        }
        manifest_path = _write_canonical(
            day_root / "admission.json", day_manifest, "canonical_manifest_sha256"
        )
        day_references.append(
            {"day": day, "artifact": artifact_identity(manifest_path)}
        )
    panel: dict[str, object] = {
        "schema_version": PANEL_ADMISSION_SCHEMA_VERSION,
        "identity": IDENTITY,
        "frozen_input_identity_sha256": identity["canonical_identity_sha256"],
        "admission_state": "complete",
        "atomic_publish_method": "fsync_tempfile_replace",
        "ordered_utc_days": _days(),
        "day_manifests": day_references,
        "economic_outcomes_read": False,
        "lockstep_executed": False,
    }
    panel_path = _write_canonical(
        root / "panel_admission.json", panel, "canonical_manifest_sha256"
    )
    return identity, panel_path


@pytest.fixture(scope="module")
def admitted_panel(tmp_path_factory: pytest.TempPathFactory) -> tuple[dict[str, object], Path]:
    return _build_panel(tmp_path_factory.mktemp("f07-admission"))


def _replace_day_manifest(
    tmp_path: Path,
    panel_path: Path,
    mutate,
) -> Path:
    panel = json.loads(panel_path.read_text(encoding="utf-8"))
    reference = panel["day_manifests"][0]
    original = Path(reference["artifact"]["path"])
    day = json.loads(original.read_text(encoding="utf-8"))
    mutate(day)
    replacement = _write_canonical(
        tmp_path / "day_admission.json", day, "canonical_manifest_sha256"
    )
    reference["artifact"] = artifact_identity(replacement)
    return _write_canonical(
        tmp_path / "panel_admission.json", panel, "canonical_manifest_sha256"
    )


def _codes(report: dict[str, object]) -> set[str]:
    return {str(row["code"]) for row in report["failure_reasons"]}


def test_complete_40_day_admission_enables_only_daily_lockstep(
    admitted_panel: tuple[dict[str, object], Path],
) -> None:
    identity, panel = admitted_panel
    report = preflight_40day_admission(
        frozen_identity=identity, panel_manifest_path=panel
    )

    assert report["lockstep_execution_eligible"] is True
    assert report["live_transport_execution_eligible"] is False
    assert report["failure_reasons"] == []
    assert report["counts"]["daily_reports_completed"] == 40
    assert report["coverage_and_limitations"]["legacy_clock_semantics_day_counts"] == {
        "explicit_dual_clock": 40
    }
    assert report["scope"]["replay_session_scope"] == "fresh_start_per_target_day"
    assert report["scope"]["prospective_live_epoch_transport_compatible"] is False
    assert report["gates"]["daily_fresh_start_session_scope"] is True
    assert report["coverage_and_limitations"]["daily_sequence_origin"] == 1
    assert report["coverage_and_limitations"]["carry_in_lifecycle_supported"] is False
    assert report["coverage_and_limitations"]["left_truncation_supported"] is False
    assert report["permissions"]["prospective_live_epoch_transport"] is False
    assert all(value is False for value in report["permissions"].values())


def test_frozen_identity_requires_daily_sequence_origin_one(
    admitted_panel: tuple[dict[str, object], Path],
) -> None:
    identity, _panel_path = admitted_panel
    changed = deepcopy(identity)
    changed["admission_contract"][
        "lifecycle_sequence_starts_at_one_per_target_day"
    ] = False
    changed["canonical_identity_sha256"] = canonical_document_sha256(
        changed, "canonical_identity_sha256"
    )

    with pytest.raises(InputAdmissionError) as error:
        validate_frozen_identity(changed)

    assert error.value.code == "daily_lifecycle_sequence_origin_not_frozen"


def test_shipped_contract_freezes_daily_scope_without_claiming_current_support() -> None:
    path = DEFAULT_IDENTITY
    identity = json.loads(path.read_text(encoding="utf-8"))

    assert path.name == (
        "order_lifecycle_v2_40day_input_admission_v1_contract_20260805.json"
    )
    assert identity["canonical_identity_sha256"] == canonical_document_sha256(
        identity, "canonical_identity_sha256"
    )
    assert len(identity["panel"]["ordered_utc_days"]) == 40
    assert identity["admission_contract"]["replay_session_scope"] == (
        "fresh_start_per_target_day"
    )
    assert identity["admission_contract"][
        "prospective_live_epoch_transport_supported"
    ] is False
    assert all(value is False for value in identity["current_producer_status"].values())
    assert identity["permissions"]["prospective_live_epoch_transport"] is False


def test_duplicate_day_fails_closed(
    tmp_path: Path,
    admitted_panel: tuple[dict[str, object], Path],
) -> None:
    identity, panel_path = admitted_panel
    panel = json.loads(panel_path.read_text(encoding="utf-8"))
    panel["ordered_utc_days"][1] = panel["ordered_utc_days"][0]
    changed = _write_canonical(
        tmp_path / "panel.json", panel, "canonical_manifest_sha256"
    )

    report = preflight_40day_admission(
        frozen_identity=identity, panel_manifest_path=changed
    )

    assert report["lockstep_execution_eligible"] is False
    assert "duplicate_day" in _codes(report)


@pytest.mark.parametrize(
    ("field", "expected_code"),
    [
        ("hash", "artifact_sha256_mismatch"),
        ("row_count", "legacy_row_count_mismatch"),
    ],
)
def test_hash_and_row_mismatch_fail_closed(
    tmp_path: Path,
    admitted_panel: tuple[dict[str, object], Path],
    field: str,
    expected_code: str,
) -> None:
    identity, panel_path = admitted_panel

    def mutate(day: dict[str, object]) -> None:
        if field == "hash":
            day["legacy_trace"]["artifact"]["sha256"] = "0" * 64
        else:
            day["legacy_trace"]["row_count"] += 1

    changed = _replace_day_manifest(tmp_path, panel_path, mutate)
    report = preflight_40day_admission(
        frozen_identity=identity, panel_manifest_path=changed
    )

    assert report["lockstep_execution_eligible"] is False
    assert expected_code in _codes(report)


@pytest.mark.parametrize(
    ("field", "expected_code"),
    [("rows_dropped", "writer_drop_nonzero"), ("error_count", "writer_error_nonzero")],
)
def test_drop_or_error_health_fails_closed(
    tmp_path: Path,
    admitted_panel: tuple[dict[str, object], Path],
    field: str,
    expected_code: str,
) -> None:
    identity, panel_path = admitted_panel

    def mutate(day: dict[str, object]) -> None:
        source = Path(day["journal_v2"]["health_artifact"]["path"])
        health = json.loads(source.read_text(encoding="utf-8"))
        health[field] = 1
        replacement = _write_json(tmp_path / f"health-{field}.json", health)
        day["journal_v2"]["health_artifact"] = artifact_identity(replacement)

    changed = _replace_day_manifest(tmp_path, panel_path, mutate)
    report = preflight_40day_admission(
        frozen_identity=identity, panel_manifest_path=changed
    )

    assert report["lockstep_execution_eligible"] is False
    assert expected_code in _codes(report)


def test_missing_cancel_reject_capability_is_reported(
    tmp_path: Path,
    admitted_panel: tuple[dict[str, object], Path],
) -> None:
    identity, panel_path = admitted_panel

    def mutate(day: dict[str, object]) -> None:
        capability = json.loads(
            Path(day["producer_capabilities"]["replay_capability_artifact"]["path"])
            .read_text(encoding="utf-8")
        )
        capability["cancel_reject_route_supported"] = False
        replacement = _write_json(tmp_path / "capability.json", capability)
        day["producer_capabilities"]["replay_capability_artifact"] = artifact_identity(
            replacement
        )
        day["producer_capabilities"]["cancel_reject_route_supported"] = False

    changed = _replace_day_manifest(tmp_path, panel_path, mutate)
    report = preflight_40day_admission(
        frozen_identity=identity, panel_manifest_path=changed
    )

    assert report["lockstep_execution_eligible"] is False
    assert "cancel_reject_route_coverage_incomplete" in _codes(report)
    assert report["coverage_and_limitations"]["cancel_reject_supported_days"] == 39


def test_sub_lot_terminal_remainder_is_not_hidden(
    tmp_path: Path,
    admitted_panel: tuple[dict[str, object], Path],
) -> None:
    identity, panel_path = admitted_panel

    def mutate(day: dict[str, object]) -> None:
        source = Path(day["legacy_trace"]["artifact"]["path"])
        rows = pq.read_table(source).to_pylist()
        rows[-1]["event_type"] = "full_fill"
        rows[-1]["state_after"] = "filled"
        rows[-1]["remaining_qty"] = 0.0004
        replacement = tmp_path / "legacy-sub-lot.parquet"
        pq.write_table(pa.Table.from_pylist(rows), replacement)
        day["legacy_trace"]["artifact"] = artifact_identity(replacement)
        day["legacy_trace"]["schema_sha256"] = _schema_sha(replacement)
        day["producer_capabilities"]["sub_lot_terminal_remainder_count"] = 1

    changed = _replace_day_manifest(tmp_path, panel_path, mutate)
    report = preflight_40day_admission(
        frozen_identity=identity, panel_manifest_path=changed
    )

    assert report["lockstep_execution_eligible"] is False
    assert "sub_lot_terminal_remainder_nonzero" in _codes(report)
    assert report["coverage_and_limitations"]["sub_lot_terminal_remainder_count"] == 1


def test_mixed_legacy_clock_is_explicitly_reported(
    tmp_path: Path,
    admitted_panel: tuple[dict[str, object], Path],
) -> None:
    identity, panel_path = admitted_panel

    def mutate(day: dict[str, object]) -> None:
        source = Path(day["legacy_trace"]["artifact"]["path"])
        rows = pq.read_table(source).to_pylist()
        rows[-1]["event_exchange_ts_ns"] = None
        replacement = tmp_path / "legacy-mixed-clock.parquet"
        pq.write_table(pa.Table.from_pylist(rows), replacement)
        day["legacy_trace"]["artifact"] = artifact_identity(replacement)
        day["legacy_trace"]["schema_sha256"] = _schema_sha(replacement)
        day["legacy_trace"]["clock_semantics"] = "mixed_clock_coverage"

    changed = _replace_day_manifest(tmp_path, panel_path, mutate)
    report = preflight_40day_admission(
        frozen_identity=identity, panel_manifest_path=changed
    )

    assert report["lockstep_execution_eligible"] is False
    assert "legacy_dual_clock_coverage_incomplete" in _codes(report)
    assert report["coverage_and_limitations"]["legacy_clock_semantics_day_counts"] == {
        "explicit_dual_clock": 39,
        "mixed_clock_coverage": 1,
    }


def test_continuous_live_epoch_scope_cannot_enter_daily_lockstep(
    tmp_path: Path,
    admitted_panel: tuple[dict[str, object], Path],
) -> None:
    identity, panel_path = admitted_panel

    def mutate(day: dict[str, object]) -> None:
        day["producer_capabilities"]["replay_session_scope"] = (
            "continuous_baseline_epoch"
        )

    changed = _replace_day_manifest(tmp_path, panel_path, mutate)
    report = preflight_40day_admission(
        frozen_identity=identity, panel_manifest_path=changed
    )

    assert report["lockstep_execution_eligible"] is False
    assert report["live_transport_execution_eligible"] is False
    assert "replay_session_scope_mismatch" in _codes(report)
    assert report["permissions"]["prospective_live_epoch_transport"] is False
