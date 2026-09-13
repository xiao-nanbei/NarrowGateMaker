from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import test_order_lifecycle_v2_40day_input_admission as v1_fixture

from research.families.f07_active_order_continuation.audit.order_lifecycle_v2_40day_input_admission import (
    artifact_identity,
    canonical_document_sha256,
    file_sha256,
    preflight_40day_admission,
)
from research.families.f07_active_order_continuation.audit.order_lifecycle_v2_40day_journal_authority_amendment import (
    DEFAULT_AMENDMENT,
    HISTORICAL_V1_2_AMENDMENT,
    HISTORICAL_V1_2_IDENTITY,
    HISTORICAL_V1_2_MARKDOWN,
    IDENTITY,
    JournalAuthorityAmendmentError,
    preflight_40day_journal_authority,
    validate_amendment,
    validate_authoritative_journal_dual_clock,
)


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, sort_keys=True, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    return path


def _write_canonical(path: Path, payload: dict[str, object], field: str) -> Path:
    value = deepcopy(payload)
    value[field] = canonical_document_sha256(value, field)
    return _write_json(path, value)


def _synthetic_amendment(root: Path, frozen_identity: dict[str, object]) -> dict[str, object]:
    amendment = json.loads(DEFAULT_AMENDMENT.read_text(encoding="utf-8"))
    identity_path = _write_json(root / "synthetic-v1-identity.json", frozen_identity)
    amendment["amends"].update(
        {
            "path": str(identity_path),
            "file_sha256": file_sha256(identity_path),
            "canonical_identity_sha256": frozen_identity["canonical_identity_sha256"],
        }
    )
    amendment["canonical_amendment_sha256"] = canonical_document_sha256(
        amendment, "canonical_amendment_sha256"
    )
    return amendment


def _rewrite_all_days(
    root: Path,
    panel_path: Path,
    mutate,
) -> Path:
    panel = json.loads(panel_path.read_text(encoding="utf-8"))
    for index, reference in enumerate(panel["day_manifests"]):
        original = Path(reference["artifact"]["path"])
        day = json.loads(original.read_text(encoding="utf-8"))
        mutate(day, root / f"day-{index:02d}")
        replacement = _write_canonical(
            root / f"day-{index:02d}.json", day, "canonical_manifest_sha256"
        )
        reference["artifact"] = artifact_identity(replacement)
    return _write_canonical(root / "panel.json", panel, "canonical_manifest_sha256")


def _codes(report: dict[str, object]) -> set[str]:
    return {str(row["code"]) for row in report["failure_reasons"]}


@pytest.fixture(scope="module")
def admitted_panel(tmp_path_factory: pytest.TempPathFactory) -> tuple[dict[str, object], Path]:
    return v1_fixture._build_panel(tmp_path_factory.mktemp("f07-journal-authority"))


def test_shipped_v1_3_amendment_binds_unchanged_v1_contract() -> None:
    amendment = json.loads(DEFAULT_AMENDMENT.read_text(encoding="utf-8"))
    v1_path = Path(amendment["amends"]["path"])
    frozen_identity = json.loads(v1_path.read_text(encoding="utf-8"))

    state = validate_amendment(amendment, frozen_identity=frozen_identity)

    assert amendment["identity"] == IDENTITY
    assert state["amended_identity_sha256"] == frozen_identity["canonical_identity_sha256"]
    assert (
        state["superseded_v1_2_canonical_sha256"]
        == amendment["supersedes"]["canonical_amendment_sha256"]
    )
    assert state["historical_writer_sha256"] == amendment["writer_successor"]["historical_sha256"]
    assert state["current_writer_sha256"] == amendment["writer_successor"]["current_sha256"]
    assert amendment["authority"]["legacy_role"] == "diagnostic_reconciliation_only"
    assert amendment["permissions"] == {
        "formal_40day_lockstep": False,
        "cif_training": False,
        "economic_evaluation": False,
        "q90_action": False,
        "prospective_live_epoch_transport": False,
        "live_deployment": False,
    }


def test_frozen_v1_2_bytes_and_current_writer_failure_evidence_are_preserved() -> None:
    successor = json.loads(DEFAULT_AMENDMENT.read_text(encoding="utf-8"))
    historical = json.loads(HISTORICAL_V1_2_AMENDMENT.read_text(encoding="utf-8"))
    lineage = successor["supersedes"]
    writer = successor["writer_successor"]

    assert historical["identity"] == HISTORICAL_V1_2_IDENTITY
    assert file_sha256(HISTORICAL_V1_2_AMENDMENT) == lineage["json_file_sha256"]
    assert file_sha256(HISTORICAL_V1_2_MARKDOWN) == lineage["markdown_file_sha256"]
    assert historical["canonical_amendment_sha256"] == lineage["canonical_amendment_sha256"]
    assert historical["implementation_artifacts"][writer["path"]] == writer["historical_sha256"]
    assert file_sha256(Path(writer["path"])) == writer["current_sha256"]
    assert writer["historical_sha256"] != writer["current_sha256"]
    assert lineage["historical_current_validation_status"] == (
        "preserved_failure_current_writer_hash_mismatch"
    )
    assert lineage["historical_failure_code"] == "implementation_hash_mismatch"


def test_complete_40_day_journal_panel_is_execution_eligible(
    tmp_path: Path,
    admitted_panel: tuple[dict[str, object], Path],
) -> None:
    identity, panel = admitted_panel
    amendment = _synthetic_amendment(tmp_path, identity)

    report = preflight_40day_journal_authority(
        frozen_identity=identity,
        amendment=amendment,
        panel_manifest_path=panel,
    )

    assert report["lockstep_execution_eligible"] is True
    assert report["mechanics_authority_eligible"] is False
    assert report["failure_reasons"] == []
    assert report["counts"]["daily_reports_completed"] == 40
    assert all(report["gates"].values())
    assert report["legacy_reconciliation"]["status_day_counts"] == {"available": 40}
    assert all(value is False for value in report["permissions"].values())


def test_missing_legacy_trace_is_diagnostic_and_does_not_block(
    tmp_path: Path,
    admitted_panel: tuple[dict[str, object], Path],
) -> None:
    identity, panel = admitted_panel
    changed = _rewrite_all_days(
        tmp_path / "missing",
        panel,
        lambda day, _root: day.pop("legacy_trace"),
    )
    amendment = _synthetic_amendment(tmp_path / "amendment", identity)

    report = preflight_40day_journal_authority(
        frozen_identity=identity,
        amendment=amendment,
        panel_manifest_path=changed,
    )

    assert report["lockstep_execution_eligible"] is True
    assert report["legacy_reconciliation"]["status_day_counts"] == {"missing": 40}
    assert report["legacy_reconciliation"]["missing_or_single_clock_blocks_execution"] is False


def test_single_clock_legacy_is_diagnostic_but_v1_still_fails(
    tmp_path: Path,
    admitted_panel: tuple[dict[str, object], Path],
) -> None:
    identity, panel = admitted_panel

    def single_clock(day: dict[str, object], root: Path) -> None:
        source = Path(day["legacy_trace"]["artifact"]["path"])
        table = pq.read_table(source)
        keep = [
            name
            for name in table.column_names
            if name not in {"event_visibility_ts_ns", "event_exchange_ts_ns"}
        ]
        replacement = root / "legacy-single-clock.parquet"
        replacement.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table.select(keep), replacement)
        day["legacy_trace"].update(
            {
                "artifact": artifact_identity(replacement),
                "schema_sha256": v1_fixture._schema_sha(replacement),
                "clock_semantics": "single_event_ts_only",
            }
        )

    changed = _rewrite_all_days(tmp_path / "single", panel, single_clock)
    amendment = _synthetic_amendment(tmp_path / "amendment", identity)

    successor = preflight_40day_journal_authority(
        frozen_identity=identity,
        amendment=amendment,
        panel_manifest_path=changed,
    )
    historical = preflight_40day_admission(frozen_identity=identity, panel_manifest_path=changed)

    assert successor["lockstep_execution_eligible"] is True
    assert successor["legacy_reconciliation"]["clock_semantics_day_counts"] == {
        "single_event_ts_only": 40
    }
    assert historical["lockstep_execution_eligible"] is False
    assert "legacy_dual_clock_coverage_incomplete" in _codes(historical)


def test_invalid_legacy_artifact_is_diagnostic_only(
    tmp_path: Path,
    admitted_panel: tuple[dict[str, object], Path],
) -> None:
    identity, panel = admitted_panel

    def invalidate(day: dict[str, object], _root: Path) -> None:
        day["legacy_trace"]["artifact"]["sha256"] = "0" * 64

    changed = _rewrite_all_days(tmp_path / "invalid", panel, invalidate)
    amendment = _synthetic_amendment(tmp_path / "amendment", identity)
    report = preflight_40day_journal_authority(
        frozen_identity=identity,
        amendment=amendment,
        panel_manifest_path=changed,
    )

    assert report["lockstep_execution_eligible"] is True
    assert report["legacy_reconciliation"]["status_day_counts"] == {"invalid": 40}


def test_writer_error_remains_a_hard_gate(
    tmp_path: Path,
    admitted_panel: tuple[dict[str, object], Path],
) -> None:
    identity, panel = admitted_panel

    def writer_error(day: dict[str, object], root: Path) -> None:
        health = json.loads(
            Path(day["journal_v2"]["health_artifact"]["path"]).read_text(encoding="utf-8")
        )
        health["error_count"] = 1
        replacement = _write_json(root / "health.json", health)
        day["journal_v2"]["health_artifact"] = artifact_identity(replacement)

    changed = _rewrite_all_days(tmp_path / "writer", panel, writer_error)
    amendment = _synthetic_amendment(tmp_path / "amendment", identity)
    report = preflight_40day_journal_authority(
        frozen_identity=identity,
        amendment=amendment,
        panel_manifest_path=changed,
    )

    assert report["lockstep_execution_eligible"] is False
    assert "writer_error_nonzero" in _codes(report)


@pytest.mark.parametrize(
    ("field", "expected_code"),
    [
        ("cancel_reject_route_supported", "cancel_reject_capability_mismatch"),
        ("sub_lot_terminal_remainder_count", "sub_lot_terminal_remainder_count_mismatch"),
    ],
)
def test_route_and_terminal_quantity_contracts_remain_hard_gates(
    tmp_path: Path,
    admitted_panel: tuple[dict[str, object], Path],
    field: str,
    expected_code: str,
) -> None:
    identity, panel = admitted_panel

    def mutate(day: dict[str, object], _root: Path) -> None:
        if field == "cancel_reject_route_supported":
            day["producer_capabilities"][field] = False
        else:
            day["producer_capabilities"][field] = 1

    changed = _rewrite_all_days(tmp_path / field, panel, mutate)
    amendment = _synthetic_amendment(tmp_path / "amendment", identity)
    report = preflight_40day_journal_authority(
        frozen_identity=identity,
        amendment=amendment,
        panel_manifest_path=changed,
    )

    assert report["lockstep_execution_eligible"] is False
    assert expected_code in _codes(report)


def test_cpp_event_stream_binding_remains_a_hard_gate(
    tmp_path: Path,
    admitted_panel: tuple[dict[str, object], Path],
) -> None:
    identity, panel = admitted_panel

    def unbind(day: dict[str, object], _root: Path) -> None:
        day["producer_capabilities"]["cpp_event_stream_binding"]["status"] = "unbound"

    changed = _rewrite_all_days(tmp_path / "cpp", panel, unbind)
    amendment = _synthetic_amendment(tmp_path / "amendment", identity)
    report = preflight_40day_journal_authority(
        frozen_identity=identity,
        amendment=amendment,
        panel_manifest_path=changed,
    )

    assert report["lockstep_execution_eligible"] is False
    assert "cpp_binding_status_mismatch" in _codes(report)


def test_authoritative_journal_requires_exchange_clock_and_exposure() -> None:
    valid = {
        "lifecycle_event": "activate",
        "event_visibility_ts_ns": 200,
        "event_exchange_ts_ns": 100,
        "event_exchange_clock_valid": True,
        "source_callback_exchange_ts_ns": 100,
        "source_callback_exchange_clock_valid": True,
        "exchange_exposure_valid": True,
        "quantity_time_exposure_exchange_btc_s": 0.0,
    }
    assert validate_authoritative_journal_dual_clock([valid])["exchange_required_rows"] == 1

    missing = dict(valid)
    missing["event_exchange_ts_ns"] = None
    missing["event_exchange_clock_valid"] = False
    with pytest.raises(JournalAuthorityAmendmentError) as error:
        validate_authoritative_journal_dual_clock([missing])
    assert error.value.code == "journal_dual_clock_coverage_incomplete"

    invalid_exposure = dict(valid)
    invalid_exposure["exchange_exposure_valid"] = False
    invalid_exposure["quantity_time_exposure_exchange_btc_s"] = None
    with pytest.raises(JournalAuthorityAmendmentError) as error:
        validate_authoritative_journal_dual_clock([invalid_exposure])
    assert error.value.code == "journal_exchange_exposure_invalid"
