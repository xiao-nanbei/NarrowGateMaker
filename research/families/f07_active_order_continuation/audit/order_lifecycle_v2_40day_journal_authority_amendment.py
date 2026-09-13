"""Admit F07 mechanics from journal-v2 without requiring legacy dual clocks.

This successor leaves the frozen v1 and v1.2 identities untouched. It reuses
the v1 40-day denominator, source, and runtime contracts while binding the
recovery-scheduling writer successor. Journal-v2 plus the C++ event-stream
binding remain authoritative. A legacy trace is optional reconciliation
evidence and never grants or removes mechanics eligibility.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path

from execution.order_lifecycle_quantity_contract import (
    ORDER_LIFECYCLE_QUANTITY_CONTRACT_ID,
)
from research.families.f07_active_order_continuation.audit import (
    order_lifecycle_v2_40day_input_admission as v1,
)

ROOT = Path(__file__).resolve().parents[4]
IDENTITY = "f07_order_lifecycle_v2_40day_input_admission_v1_3_journal_authority_amendment"
AMENDMENT_SCHEMA_VERSION = "f07_order_lifecycle_v2_40day_journal_authority_amendment.v2"
REPORT_SCHEMA_VERSION = "f07_order_lifecycle_v2_40day_journal_authority_report.v1_3"
DEFAULT_AMENDMENT = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "order_lifecycle_v2_40day_input_admission_v1_3_journal_authority_amendment_20260805.json"
)
HISTORICAL_V1_2_IDENTITY = (
    "f07_order_lifecycle_v2_40day_input_admission_v1_2_journal_authority_amendment"
)
HISTORICAL_V1_2_AMENDMENT = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "order_lifecycle_v2_40day_input_admission_v1_2_journal_authority_amendment_20260805.json"
)
HISTORICAL_V1_2_MARKDOWN = HISTORICAL_V1_2_AMENDMENT.with_suffix(".md")

_AMENDMENT_KEYS = frozenset(
    {
        "schema_version",
        "identity",
        "status",
        "last_materially_modified",
        "amends",
        "supersedes",
        "writer_successor",
        "authority",
        "hard_gates",
        "legacy_reconciliation",
        "implementation_artifacts",
        "permissions",
        "canonical_amendment_sha256",
    }
)
_SUPERSEDES_KEYS = frozenset(
    {
        "identity",
        "json_path",
        "json_file_sha256",
        "canonical_amendment_sha256",
        "markdown_path",
        "markdown_file_sha256",
        "historical_bytes_unchanged",
        "historical_current_validation_status",
        "historical_failure_code",
    }
)
_WRITER_SUCCESSOR_KEYS = frozenset(
    {
        "path",
        "historical_sha256",
        "current_sha256",
        "drift_class",
        "change_scope",
        "journal_schema_changed",
        "atomic_publish_protocol_changed",
        "storage_format_changed",
        "economic_scope_changed",
        "permissions_changed",
    }
)
_AMENDS_KEYS = frozenset(
    {
        "path",
        "file_sha256",
        "canonical_identity_sha256",
        "ordered_40_day_denominator_unchanged",
        "historical_contract_bytes_unchanged",
    }
)
_AUTHORITY_KEYS = frozenset(
    {
        "mechanics_primary",
        "python_cpp_authority",
        "legacy_role",
        "legacy_missing_blocks_execution",
        "legacy_single_clock_blocks_execution",
    }
)
_HARD_GATE_KEYS = frozenset(
    {
        "journal_v2_explicit_dual_clock",
        "writer_integrity",
        "cancel_reject_route",
        "terminal_sub_lot_zero",
        "cpp_event_stream_binding",
        "ordered_40_day_sources_and_identity",
        "daily_fresh_start_scope",
    }
)
_PERMISSION_KEYS = frozenset(
    {
        "formal_40day_lockstep",
        "cif_training",
        "economic_evaluation",
        "q90_action",
        "prospective_live_epoch_transport",
        "live_deployment",
    }
)
_LEGACY_RECONCILIATION_KEYS = frozenset(
    {
        "missing_status",
        "single_clock_status",
        "invalid_artifact_status",
        "order_identity_mismatch_status",
        "affects_lockstep_execution_eligibility",
        "may_grant_mechanics_authority",
    }
)
_DAY_REQUIRED_KEYS = frozenset(
    {
        "schema_version",
        "day",
        "frozen_input_identity_sha256",
        "interval_identity_sha256",
        "baseline_runtime_identity_sha256",
        "admission_state",
        "atomic_publish_method",
        "journal_v2",
        "market_data_artifacts",
        "producer_capabilities",
        "economic_outcomes_read",
        "canonical_manifest_sha256",
    }
)
_DAY_OPTIONAL_KEYS = frozenset({"legacy_trace"})
_EXCHANGE_CLOCK_EVENTS = frozenset(
    {"activate", "cancel_rejected", "partial_fill", "full_fill", "exchange_terminal"}
)


class JournalAuthorityAmendmentError(v1.InputAdmissionError):
    """A malformed amendment or authoritative mechanics artifact."""


def _failure(
    code: str,
    detail: str,
    *,
    day: str = "",
    artifact: str = "",
) -> dict[str, str]:
    return {
        "code": str(code),
        "day": str(day),
        "artifact": str(artifact),
        "detail": str(detail),
    }


def validate_amendment(
    amendment: Mapping[str, object],
    *,
    frozen_identity: Mapping[str, object],
    base: Path = ROOT,
) -> dict[str, str]:
    """Validate the successor and its immutable link to the frozen v1 identity."""

    v1._require_exact_keys(amendment, _AMENDMENT_KEYS, label="journal authority amendment")
    if amendment["schema_version"] != AMENDMENT_SCHEMA_VERSION:
        raise JournalAuthorityAmendmentError(
            "amendment_schema_mismatch", "unsupported journal authority amendment"
        )
    if amendment["identity"] != IDENTITY:
        raise JournalAuthorityAmendmentError(
            "amendment_identity_mismatch", "journal authority amendment identity differs"
        )
    canonical = v1._validate_canonical_document(
        amendment,
        hash_field="canonical_amendment_sha256",
        label="journal authority amendment",
    )
    amends = amendment["amends"]
    if not isinstance(amends, Mapping):
        raise JournalAuthorityAmendmentError(
            "amended_identity_missing", "amended v1 identity is required"
        )
    v1._require_exact_keys(amends, _AMENDS_KEYS, label="amended v1 identity")
    frozen_sha = v1.canonical_document_sha256(frozen_identity, "canonical_identity_sha256")
    if amends["canonical_identity_sha256"] != frozen_sha:
        raise JournalAuthorityAmendmentError(
            "amended_identity_hash_mismatch", "amendment does not bind this v1 identity"
        )
    if not bool(amends["ordered_40_day_denominator_unchanged"]) or not bool(
        amends["historical_contract_bytes_unchanged"]
    ):
        raise JournalAuthorityAmendmentError(
            "historical_identity_mutation_claimed", "v1 bytes and denominator must remain unchanged"
        )
    amended_path = v1._resolve_path(amends["path"], base=base)
    if not amended_path.is_file() or v1.file_sha256(amended_path) != amends["file_sha256"]:
        raise JournalAuthorityAmendmentError(
            "amended_artifact_hash_mismatch",
            "amended v1 artifact bytes differ",
            artifact=str(amended_path),
        )

    supersedes = amendment["supersedes"]
    if not isinstance(supersedes, Mapping):
        raise JournalAuthorityAmendmentError(
            "historical_predecessor_missing", "frozen v1.2 predecessor is required"
        )
    v1._require_exact_keys(supersedes, _SUPERSEDES_KEYS, label="historical predecessor")
    if supersedes["identity"] != HISTORICAL_V1_2_IDENTITY:
        raise JournalAuthorityAmendmentError(
            "historical_predecessor_identity_mismatch", "v1.2 predecessor identity differs"
        )
    historical_json = v1._resolve_path(supersedes["json_path"], base=base)
    historical_markdown = v1._resolve_path(supersedes["markdown_path"], base=base)
    if historical_json != HISTORICAL_V1_2_AMENDMENT.resolve() or not historical_json.is_file():
        raise JournalAuthorityAmendmentError(
            "historical_predecessor_path_mismatch",
            "v1.2 predecessor JSON path differs",
            artifact=str(historical_json),
        )
    if (
        historical_markdown != HISTORICAL_V1_2_MARKDOWN.resolve()
        or not historical_markdown.is_file()
    ):
        raise JournalAuthorityAmendmentError(
            "historical_predecessor_path_mismatch",
            "v1.2 predecessor Markdown path differs",
            artifact=str(historical_markdown),
        )
    if v1.file_sha256(historical_json) != supersedes["json_file_sha256"]:
        raise JournalAuthorityAmendmentError(
            "historical_predecessor_hash_mismatch",
            "frozen v1.2 JSON bytes differ",
            artifact=str(historical_json),
        )
    if v1.file_sha256(historical_markdown) != supersedes["markdown_file_sha256"]:
        raise JournalAuthorityAmendmentError(
            "historical_predecessor_hash_mismatch",
            "frozen v1.2 Markdown bytes differ",
            artifact=str(historical_markdown),
        )
    historical_payload = v1._load_json(historical_json)
    historical_canonical = v1._validate_canonical_document(
        historical_payload,
        hash_field="canonical_amendment_sha256",
        label="historical v1.2 amendment",
    )
    if historical_payload.get("identity") != HISTORICAL_V1_2_IDENTITY or (
        historical_canonical != supersedes["canonical_amendment_sha256"]
    ):
        raise JournalAuthorityAmendmentError(
            "historical_predecessor_identity_mismatch",
            "frozen v1.2 canonical identity differs",
            artifact=str(historical_json),
        )
    if (
        not bool(supersedes["historical_bytes_unchanged"])
        or supersedes["historical_current_validation_status"]
        != "preserved_failure_current_writer_hash_mismatch"
        or supersedes["historical_failure_code"] != "implementation_hash_mismatch"
    ):
        raise JournalAuthorityAmendmentError(
            "historical_failure_evidence_drift",
            "v1.2 frozen failure evidence changed",
        )

    writer_successor = amendment["writer_successor"]
    if not isinstance(writer_successor, Mapping):
        raise JournalAuthorityAmendmentError(
            "writer_successor_missing", "writer successor provenance is required"
        )
    v1._require_exact_keys(writer_successor, _WRITER_SUCCESSOR_KEYS, label="writer successor")
    expected_change_scope = [
        "recover_on_start_explicit_reconcile_or_prior_commit_failure",
        "failed_commit_marks_next_commit_for_recovery",
        "remove_adapter_pre_callback_reconcile",
    ]
    if (
        writer_successor["path"] != "execution/order_lifecycle_journal_writer_v2.py"
        or writer_successor["drift_class"] != "recovery_scheduling_performance_successor"
        or writer_successor["change_scope"] != expected_change_scope
        or any(
            bool(writer_successor[field])
            for field in (
                "journal_schema_changed",
                "atomic_publish_protocol_changed",
                "storage_format_changed",
                "economic_scope_changed",
                "permissions_changed",
            )
        )
    ):
        raise JournalAuthorityAmendmentError(
            "writer_successor_contract_drift", "writer successor semantics changed"
        )
    historical_writer_sha = str(
        historical_payload.get("implementation_artifacts", {}).get(
            "execution/order_lifecycle_journal_writer_v2.py", ""
        )
    )
    if (
        writer_successor["historical_sha256"] != historical_writer_sha
        or writer_successor["historical_sha256"] == writer_successor["current_sha256"]
    ):
        raise JournalAuthorityAmendmentError(
            "writer_successor_hash_lineage_mismatch", "writer hash lineage differs"
        )
    current_writer = v1._resolve_path(writer_successor["path"], base=base)
    if (
        not current_writer.is_file()
        or v1.file_sha256(current_writer) != writer_successor["current_sha256"]
    ):
        raise JournalAuthorityAmendmentError(
            "writer_successor_hash_mismatch",
            "current writer bytes differ",
            artifact=str(current_writer),
        )

    authority = amendment["authority"]
    if not isinstance(authority, Mapping):
        raise JournalAuthorityAmendmentError(
            "authority_contract_missing", "authority contract is required"
        )
    v1._require_exact_keys(authority, _AUTHORITY_KEYS, label="authority contract")
    if authority != {
        "mechanics_primary": "authoritative_journal_v2",
        "python_cpp_authority": "per_event_event_stream_lockstep",
        "legacy_role": "diagnostic_reconciliation_only",
        "legacy_missing_blocks_execution": False,
        "legacy_single_clock_blocks_execution": False,
    }:
        raise JournalAuthorityAmendmentError(
            "authority_contract_drift", "journal/legacy authority semantics changed"
        )
    hard_gates = amendment["hard_gates"]
    if not isinstance(hard_gates, Mapping):
        raise JournalAuthorityAmendmentError(
            "hard_gate_contract_missing", "hard gates are required"
        )
    v1._require_exact_keys(hard_gates, _HARD_GATE_KEYS, label="hard gates")
    if not all(bool(value) for value in hard_gates.values()):
        raise JournalAuthorityAmendmentError(
            "hard_gate_contract_drift", "all successor mechanics gates must remain required"
        )
    legacy_contract = amendment["legacy_reconciliation"]
    if not isinstance(legacy_contract, Mapping):
        raise JournalAuthorityAmendmentError(
            "legacy_reconciliation_contract_missing",
            "legacy reconciliation contract is required",
        )
    v1._require_exact_keys(
        legacy_contract,
        _LEGACY_RECONCILIATION_KEYS,
        label="legacy reconciliation contract",
    )
    if legacy_contract != {
        "missing_status": "diagnostic_unavailable",
        "single_clock_status": "diagnostic_partial",
        "invalid_artifact_status": "diagnostic_invalid",
        "order_identity_mismatch_status": "diagnostic_mismatch",
        "affects_lockstep_execution_eligibility": False,
        "may_grant_mechanics_authority": False,
    }:
        raise JournalAuthorityAmendmentError(
            "legacy_authority_drift",
            "legacy diagnostic and authority semantics changed",
        )
    permissions = amendment["permissions"]
    if not isinstance(permissions, Mapping):
        raise JournalAuthorityAmendmentError(
            "permission_contract_missing", "permission contract is required"
        )
    v1._require_exact_keys(permissions, _PERMISSION_KEYS, label="permissions")
    if any(bool(value) for value in permissions.values()):
        raise JournalAuthorityAmendmentError(
            "permission_drift", "input amendment cannot grant downstream authority"
        )
    implementations = amendment["implementation_artifacts"]
    if not isinstance(implementations, Mapping) or not implementations:
        raise JournalAuthorityAmendmentError(
            "implementation_identity_missing", "implementation identities are required"
        )
    for raw_path, expected_sha in implementations.items():
        path = v1._resolve_path(raw_path, base=base)
        if not path.is_file() or v1.file_sha256(path) != expected_sha:
            raise JournalAuthorityAmendmentError(
                "implementation_hash_mismatch",
                f"implementation differs: {raw_path}",
                artifact=str(path),
            )
    if implementations.get(writer_successor["path"]) != writer_successor["current_sha256"]:
        raise JournalAuthorityAmendmentError(
            "writer_successor_implementation_binding_mismatch",
            "implementation artifacts do not bind the successor writer",
        )
    return {
        "canonical_amendment_sha256": canonical,
        "amended_identity_sha256": frozen_sha,
        "superseded_v1_2_canonical_sha256": historical_canonical,
        "historical_writer_sha256": str(writer_successor["historical_sha256"]),
        "current_writer_sha256": str(writer_successor["current_sha256"]),
    }


def validate_authoritative_journal_dual_clock(
    rows: Sequence[Mapping[str, object]],
) -> dict[str, int]:
    """Require explicit causal clocks in journal-v2, never infer them from legacy."""

    missing_exchange = 0
    exchange_after_visibility = 0
    invalid_exchange_exposure = 0
    ordered_rows = sorted(
        rows,
        key=lambda row: (
            str(row.get("lifecycle_id", "single-lifecycle")),
            int(row.get("lifecycle_sequence", 0)),
        ),
    )
    exchange_risk_started: dict[str, bool] = {}
    for row in ordered_rows:
        lifecycle_id = str(row.get("lifecycle_id", "single-lifecycle"))
        started = exchange_risk_started.get(lifecycle_id, False)
        visibility = int(row.get("event_visibility_ts_ns", 0) or 0)
        if visibility <= 0:
            raise JournalAuthorityAmendmentError(
                "journal_visibility_clock_missing", "journal visibility clock is required"
            )
        event = str(row.get("lifecycle_event", ""))
        if event in _EXCHANGE_CLOCK_EVENTS:
            exchange = row.get("event_exchange_ts_ns")
            callback_exchange = row.get("source_callback_exchange_ts_ns")
            if (
                exchange is None
                or int(exchange) <= 0
                or not bool(row.get("event_exchange_clock_valid", False))
                or callback_exchange is None
                or int(callback_exchange) <= 0
                or not bool(row.get("source_callback_exchange_clock_valid", False))
            ):
                missing_exchange += 1
            elif int(exchange) > visibility:
                exchange_after_visibility += 1
            started = True
            exchange_risk_started[lifecycle_id] = True
        if not bool(row.get("exchange_exposure_valid", False)):
            invalid_exchange_exposure += 1
        elif started and row.get("quantity_time_exposure_exchange_btc_s") is None:
            invalid_exchange_exposure += 1
    if missing_exchange:
        raise JournalAuthorityAmendmentError(
            "journal_dual_clock_coverage_incomplete",
            f"exchange-required rows missing clocks={missing_exchange}",
        )
    if exchange_after_visibility:
        raise JournalAuthorityAmendmentError(
            "journal_exchange_after_visibility",
            f"exchange timestamp after visibility rows={exchange_after_visibility}",
        )
    if invalid_exchange_exposure:
        raise JournalAuthorityAmendmentError(
            "journal_exchange_exposure_invalid",
            f"rows with invalid exchange-time exposure={invalid_exchange_exposure}",
        )
    return {
        "rows": len(rows),
        "exchange_required_rows": sum(
            int(str(row.get("lifecycle_event", "")) in _EXCHANGE_CLOCK_EVENTS) for row in rows
        ),
    }


def _validate_authoritative_capabilities(
    section: Mapping[str, object],
    *,
    identity: Mapping[str, object],
    journal: Mapping[str, object],
    base: Path,
) -> dict[str, object]:
    """Reuse v1 capability checks with journal-v2 as the sole event authority."""

    journal_only_reconciliation = {
        "cancel_reject_count": int(journal["cancel_reject_count"]),
        "terminal_sub_lot_rows": [],
    }
    result = v1._validate_capabilities(
        section,
        identity=identity,
        journal=journal,
        legacy=journal_only_reconciliation,
        base=base,
    )
    if not bool(result["cancel_reject_route_supported"]):
        raise JournalAuthorityAmendmentError(
            "cancel_reject_route_unsupported", "cancel-reject route is not bound"
        )
    if int(result["sub_lot_terminal_remainder_count"]) != 0:
        raise JournalAuthorityAmendmentError(
            "sub_lot_terminal_remainder_nonzero",
            "journal-v2 contains a terminal positive remainder",
        )
    if result["sub_lot_terminal_remainder_contract"] != (ORDER_LIFECYCLE_QUANTITY_CONTRACT_ID):
        raise JournalAuthorityAmendmentError(
            "sub_lot_terminal_contract_unsupported", "terminal quantity contract differs"
        )
    if result["cpp_event_stream_binding_status"] != "bound":
        raise JournalAuthorityAmendmentError(
            "cpp_event_stream_binding_incomplete", "C++ event stream is not bound"
        )
    return result


def _diagnose_legacy(
    section: object,
    *,
    day: str,
    base: Path,
    journal: Mapping[str, object],
) -> dict[str, object]:
    if section is None:
        return {
            "status": "missing",
            "clock_semantics": "unavailable",
            "order_identity_match": None,
            "row_count": 0,
            "diagnostic_code": "legacy_trace_unavailable",
        }
    if not isinstance(section, Mapping):
        return {
            "status": "invalid",
            "clock_semantics": "unavailable",
            "order_identity_match": None,
            "row_count": 0,
            "diagnostic_code": "legacy_section_not_mapping",
        }
    try:
        legacy = v1._validate_legacy(section, day=day, base=base)
    except v1.InputAdmissionError as exc:
        return {
            "status": "invalid",
            "clock_semantics": "unavailable",
            "order_identity_match": None,
            "row_count": 0,
            "diagnostic_code": exc.code,
        }
    order_match = legacy["client_order_ids"] == journal["client_order_ids"]
    return {
        "status": "available",
        "clock_semantics": str(legacy["clock_semantics"]),
        "order_identity_match": bool(order_match),
        "row_count": int(legacy["row_count"]),
        "diagnostic_code": "matched" if order_match else "order_identity_mismatch",
    }


def _validate_day(
    manifest_path: Path,
    *,
    identity: Mapping[str, object],
    expected_day: str,
) -> dict[str, object]:
    payload = v1._load_json(manifest_path)
    v1._assert_mechanics_only(payload, path=f"day[{expected_day}]")
    keys = set(payload)
    if not _DAY_REQUIRED_KEYS.issubset(keys) or keys - (_DAY_REQUIRED_KEYS | _DAY_OPTIONAL_KEYS):
        raise JournalAuthorityAmendmentError(
            "day_manifest_schema_mismatch", "successor daily manifest keys differ"
        )
    if payload["schema_version"] != v1.DAY_ADMISSION_SCHEMA_VERSION:
        raise JournalAuthorityAmendmentError(
            "day_manifest_schema_mismatch", "unsupported daily manifest"
        )
    v1._validate_canonical_document(
        payload, hash_field="canonical_manifest_sha256", label="daily admission manifest"
    )
    if str(payload["day"]) != expected_day:
        raise JournalAuthorityAmendmentError(
            "day_manifest_day_mismatch", "daily manifest day differs"
        )
    frozen_sha = v1.canonical_document_sha256(identity, "canonical_identity_sha256")
    if payload["frozen_input_identity_sha256"] != frozen_sha:
        raise JournalAuthorityAmendmentError(
            "day_frozen_identity_mismatch", "daily frozen identity differs"
        )
    interval = v1.expected_day_interval(expected_day)
    if payload["interval_identity_sha256"] != interval["interval_identity_sha256"]:
        raise JournalAuthorityAmendmentError(
            "day_interval_identity_mismatch", "daily interval differs"
        )
    if (
        payload["baseline_runtime_identity_sha256"]
        != identity["baseline_runtime_identity"]["canonical_sha256"]
    ):
        raise JournalAuthorityAmendmentError(
            "day_baseline_identity_mismatch", "daily baseline differs"
        )
    if (
        payload["admission_state"] != "complete"
        or payload["atomic_publish_method"] != "fsync_tempfile_replace"
    ):
        raise JournalAuthorityAmendmentError(
            "day_admission_incomplete", "daily admission is incomplete or non-atomic"
        )
    if bool(payload["economic_outcomes_read"]):
        raise JournalAuthorityAmendmentError(
            "economic_outcome_access", "daily admission read economics"
        )

    expected_runtime = v1.expected_runtime_identity(identity, expected_day)
    journal = v1._validate_journal(
        payload["journal_v2"],
        day=expected_day,
        expected_runtime=expected_runtime,
        base=manifest_path.parent,
    )
    clock = validate_authoritative_journal_dual_clock(journal["rows"])
    source_artifacts = payload["market_data_artifacts"]
    if not isinstance(source_artifacts, Sequence) or isinstance(source_artifacts, (str, bytes)):
        raise JournalAuthorityAmendmentError(
            "market_data_artifacts_missing", "market artifacts are required"
        )
    source_counts = v1._validate_source_coverage(
        source_artifacts,
        identity=identity,
        day=expected_day,
        base=manifest_path.parent,
    )
    capabilities = _validate_authoritative_capabilities(
        payload["producer_capabilities"],
        identity=identity,
        journal=journal,
        base=manifest_path.parent,
    )
    legacy = _diagnose_legacy(
        payload.get("legacy_trace"),
        day=expected_day,
        base=manifest_path.parent,
        journal=journal,
    )
    return {
        "day": expected_day,
        "journal_rows": int(journal["row_count"]),
        "orders": int(journal["order_count"]),
        "journal_parts": int(journal["part_count"]),
        "journal_exchange_required_rows": int(clock["exchange_required_rows"]),
        "cancel_reject_route_supported": bool(capabilities["cancel_reject_route_supported"]),
        "cancel_reject_observed_event_count": int(
            capabilities["cancel_reject_observed_event_count"]
        ),
        "sub_lot_terminal_remainder_count": int(capabilities["sub_lot_terminal_remainder_count"]),
        "cpp_event_stream_binding_status": str(capabilities["cpp_event_stream_binding_status"]),
        "replay_session_scope": str(capabilities["replay_session_scope"]),
        "carry_in_lifecycle_count": int(capabilities["carry_in_lifecycle_count"]),
        "market_data_artifact_counts": source_counts,
        "legacy_reconciliation": legacy,
    }


def preflight_40day_journal_authority(
    *,
    frozen_identity: Mapping[str, object],
    amendment: Mapping[str, object],
    panel_manifest_path: str | Path,
) -> dict[str, object]:
    """Admit the frozen 40-day mechanics inputs under successor authority."""

    identity_state = v1.validate_frozen_identity(frozen_identity, base=ROOT)
    amendment_state = validate_amendment(amendment, frozen_identity=frozen_identity, base=ROOT)
    panel_path = Path(panel_manifest_path).expanduser().resolve()
    failures: list[dict[str, str]] = []
    day_reports: list[dict[str, object]] = []
    panel_hash = ""
    references: list[Mapping[str, object]] = []
    panel_days: list[str] = []
    try:
        panel = v1._load_json(panel_path)
        v1._assert_mechanics_only(panel, path="panel_admission")
        v1._require_exact_keys(panel, v1._PANEL_MANIFEST_KEYS, label="panel admission manifest")
        if panel["schema_version"] != v1.PANEL_ADMISSION_SCHEMA_VERSION:
            raise JournalAuthorityAmendmentError(
                "panel_manifest_schema_mismatch", "unsupported panel manifest"
            )
        if panel["identity"] != v1.IDENTITY:
            raise JournalAuthorityAmendmentError(
                "panel_identity_mismatch", "panel must retain its frozen v1 identity"
            )
        panel_hash = v1._validate_canonical_document(
            panel,
            hash_field="canonical_manifest_sha256",
            label="panel admission manifest",
        )
        if panel["frozen_input_identity_sha256"] != identity_state["canonical_identity_sha256"]:
            raise JournalAuthorityAmendmentError(
                "panel_frozen_identity_mismatch", "panel frozen identity differs"
            )
        if (
            panel["admission_state"] != "complete"
            or panel["atomic_publish_method"] != "fsync_tempfile_replace"
        ):
            raise JournalAuthorityAmendmentError(
                "panel_admission_incomplete", "panel admission is incomplete or non-atomic"
            )
        if bool(panel["economic_outcomes_read"]) or bool(panel["lockstep_executed"]):
            raise JournalAuthorityAmendmentError(
                "preflight_scope_violation",
                "input preflight cannot read economics or claim lockstep",
            )
        panel_days = list(map(str, panel["ordered_utc_days"]))
        if len(panel_days) != len(set(panel_days)):
            raise JournalAuthorityAmendmentError("duplicate_day", "panel days are duplicated")
        if panel_days != list(identity_state["days"]):
            raise JournalAuthorityAmendmentError(
                "ordered_day_denominator_mismatch", "panel days differ from frozen 40 days"
            )
        references = list(panel["day_manifests"])
        if len(references) != v1.REQUIRED_DAY_COUNT:
            raise JournalAuthorityAmendmentError(
                "daily_manifest_count_mismatch", "panel must reference exactly 40 days"
            )
    except v1.InputAdmissionError as exc:
        failures.append(_failure(exc.code, exc.detail, artifact=exc.artifact or str(panel_path)))

    for ordinal, day in enumerate(identity_state["days"]):
        if ordinal >= len(references):
            failures.append(_failure("daily_manifest_missing", "daily manifest missing", day=day))
            continue
        reference = references[ordinal]
        try:
            if not isinstance(reference, Mapping) or set(reference) != {"day", "artifact"}:
                raise JournalAuthorityAmendmentError(
                    "daily_manifest_reference_schema_mismatch", "daily reference is invalid"
                )
            if str(reference["day"]) != day:
                raise JournalAuthorityAmendmentError(
                    "daily_manifest_reference_order_mismatch", "daily reference order differs"
                )
            artifact = reference["artifact"]
            if not isinstance(artifact, Mapping):
                raise JournalAuthorityAmendmentError(
                    "daily_manifest_artifact_missing", "daily manifest artifact is required"
                )
            path = v1._validate_artifact(
                artifact, base=panel_path.parent, label=f"daily manifest {day}"
            )
            day_reports.append(_validate_day(path, identity=frozen_identity, expected_day=day))
        except v1.InputAdmissionError as exc:
            failures.append(_failure(exc.code, exc.detail, day=day, artifact=exc.artifact))

    all_days_reported = [str(row["day"]) for row in day_reports] == list(identity_state["days"])
    cancel_days = sum(int(bool(row["cancel_reject_route_supported"])) for row in day_reports)
    cpp_days = sum(int(row["cpp_event_stream_binding_status"] == "bound") for row in day_reports)
    fresh_days = sum(
        int(
            row["replay_session_scope"] == "fresh_start_per_target_day"
            and int(row["carry_in_lifecycle_count"]) == 0
        )
        for row in day_reports
    )
    sub_lot_count = sum(int(row["sub_lot_terminal_remainder_count"]) for row in day_reports)
    legacy_status_counts = Counter(
        str(row["legacy_reconciliation"]["status"]) for row in day_reports
    )
    legacy_clock_counts = Counter(
        str(row["legacy_reconciliation"]["clock_semantics"]) for row in day_reports
    )
    gates = {
        "frozen_ordered_40_day_denominator": panel_days == list(identity_state["days"]),
        "all_daily_atomic_manifests_admitted": all_days_reported,
        "journal_v2_explicit_dual_clock": all_days_reported
        and not any(row["code"].startswith("journal_") for row in failures),
        "writer_integrity_and_zero_drop_error": all_days_reported
        and not any("writer" in row["code"] or "journal_part" in row["code"] for row in failures),
        "cancel_reject_route_bound": cancel_days == v1.REQUIRED_DAY_COUNT,
        "sub_lot_terminal_remainder_zero": sub_lot_count == 0 and all_days_reported,
        "cpp_event_stream_bound": cpp_days == v1.REQUIRED_DAY_COUNT,
        "daily_fresh_start_session_scope": fresh_days == v1.REQUIRED_DAY_COUNT,
        "market_source_intervals_and_identity": all_days_reported
        and not any(row["code"].startswith("source_") for row in failures),
        "mechanics_only_scope": not any(
            row["code"] in {"economic_outcome_access", "economic_field_forbidden"}
            for row in failures
        ),
    }
    failures = sorted(
        failures,
        key=lambda row: (row["code"], row["day"], row["artifact"], row["detail"]),
    )
    eligible = bool(all(gates.values()) and not failures)
    report: dict[str, object] = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "identity": IDENTITY,
        "scope": {
            "mechanics_only": True,
            "economic_outcomes_read": False,
            "formal_40day_lockstep_executed": False,
            "legacy_is_diagnostic_only": True,
            "prospective_live_epoch_transport_compatible": False,
        },
        "frozen_v1_identity_sha256": identity_state["canonical_identity_sha256"],
        "amendment_sha256": amendment_state["canonical_amendment_sha256"],
        "panel_admission_manifest": {"path": str(panel_path), "canonical_sha256": panel_hash},
        "ordered_utc_days": list(identity_state["days"]),
        "counts": {
            "required_days": v1.REQUIRED_DAY_COUNT,
            "daily_reports_completed": len(day_reports),
            "journal_rows": sum(int(row["journal_rows"]) for row in day_reports),
            "orders": sum(int(row["orders"]) for row in day_reports),
            "journal_parts": sum(int(row["journal_parts"]) for row in day_reports),
        },
        "legacy_reconciliation": {
            "authority": "diagnostic_only",
            "status_day_counts": dict(sorted(legacy_status_counts.items())),
            "clock_semantics_day_counts": dict(sorted(legacy_clock_counts.items())),
            "missing_or_single_clock_blocks_execution": False,
        },
        "day_reports": day_reports,
        "failure_reasons": failures,
        "gates": gates,
        "lockstep_execution_eligible": eligible,
        "mechanics_authority_eligible": False,
        "mechanics_authority_next_requirement": (
            "execute_and_bind_formal_40day_python_cpp_event_stream_lockstep"
        ),
        "live_transport_execution_eligible": False,
        "permissions": dict(amendment["permissions"]),
    }
    report["canonical_report_sha256"] = v1.canonical_sha256(report)
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--identity", default=str(v1.DEFAULT_IDENTITY))
    parser.add_argument("--amendment", default=str(DEFAULT_AMENDMENT))
    parser.add_argument("--admission-manifest", required=True)
    parser.add_argument("--output", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    frozen_identity, _ = v1.load_frozen_identity(args.identity)
    amendment = v1._load_json(Path(args.amendment).expanduser().resolve())
    report = preflight_40day_journal_authority(
        frozen_identity=frozen_identity,
        amendment=amendment,
        panel_manifest_path=args.admission_manifest,
    )
    v1.atomic_write_report(args.output, report)
    print(json.dumps(report, sort_keys=True, indent=2))
    return 0 if bool(report["lockstep_execution_eligible"]) else 2


if __name__ == "__main__":
    raise SystemExit(main())
