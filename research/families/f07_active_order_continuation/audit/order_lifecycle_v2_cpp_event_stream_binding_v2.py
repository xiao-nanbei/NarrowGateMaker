"""Mechanics-only Python/C++ lockstep for journal-v2 event streams."""

from __future__ import annotations

import hashlib
import json
import math
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from execution.order_lifecycle import TerminalPolicyRoute, terminal_policy_route
from execution.order_lifecycle_journal_v2_strict_native import (
    ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS,
    ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION,
    validate_order_lifecycle_journal_v2_payload,
)
from execution.order_lifecycle_quantity_contract import (
    ORDER_LIFECYCLE_QUANTITY_CONTRACT_ID,
    TERMINAL_REMAINDER_ABS_TOLERANCE_BTC,
    persisted_terminal_remainder_is_zero,
)

CPP_EVENT_STREAM_BINDING_SCHEMA_VERSION = (
    "f07_order_lifecycle_v2_cpp_event_stream_binding.v1"
)
CPP_EVENT_STREAM_MIRROR_ABI_VERSION = (
    "order_lifecycle_journal_v2_cpp_event_stream_mirror.v2"
)
CPP_EVENT_STREAM_LOCKSTEP_REPORT_VERSION = (
    "f07_order_lifecycle_v2_cpp_event_stream_lockstep_report.v1"
)

_PROJECTION_COLUMNS = (
    "event_id",
    "lifecycle_id",
    "client_order_id",
    "lifecycle_sequence",
    "lifecycle_event",
    "event_visibility_ts_ns",
    "event_exchange_ts_ns",
    "phase_before",
    "phase_after",
    "event_reason",
    "terminal_observation",
    "exchange_terminal_reason",
    "local_censor_reason",
    "terminal_policy_route",
    "initial_quantity",
    "remaining_quantity_before",
    "remaining_quantity_after",
    "fill_risk_active_after",
    "simulator_queue_source",
    "exact_queue_path_valid",
)
_FLOAT_COLUMNS = frozenset(
    {
        "initial_quantity",
        "remaining_quantity_before",
        "remaining_quantity_after",
    }
)


def _canonical_sha256(value: object) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def journal_schema_sha256() -> str:
    return _canonical_sha256(
        {
            "schema_version": ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION,
            "columns": list(ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS),
        }
    )


def projection_schema_sha256() -> str:
    return _canonical_sha256(list(_PROJECTION_COLUMNS))


def _expected_terminal_route(row: Mapping[str, object]) -> str:
    event = str(row["lifecycle_event"])
    if event == "local_shutdown_censor":
        return TerminalPolicyRoute.SHUTDOWN_NO_REENTRY.value
    if event == "full_fill":
        return TerminalPolicyRoute.TERMINAL_COMPLETE.value
    if event != "exchange_terminal":
        return "NONE"
    route = terminal_policy_route(
        str(row["event_reason"]),
        float(row["remaining_quantity_after"]),
    )
    if route == TerminalPolicyRoute.UNSUPPORTED:
        raise ValueError("Python terminal policy route is unsupported")
    return route.value


def _expected_projection(row: Mapping[str, object]) -> dict[str, object]:
    projection = {column: row[column] for column in _PROJECTION_COLUMNS if column in row}
    projection["terminal_policy_route"] = _expected_terminal_route(row)
    return projection


def _equal(column: str, left: object, right: object) -> bool:
    if column in _FLOAT_COLUMNS:
        return math.isclose(
            float(left),
            float(right),
            rel_tol=1e-12,
            abs_tol=1e-15,
        )
    return left == right


def audit_cpp_event_stream_lockstep(
    rows: Sequence[Mapping[str, object]],
    *,
    require_cancel_reject_branches: bool = False,
    mismatch_sample_limit: int = 25,
) -> dict[str, object]:
    """Compare every authoritative Python row with the native C++ projection."""

    import narrowgate_cpp as cpp

    canonical_rows: list[dict[str, object]] = []
    by_lifecycle: dict[str, list[dict[str, object]]] = defaultdict(list)
    for source in rows:
        payload = {
            column: source[column] for column in ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS
        }
        validate_order_lifecycle_journal_v2_payload(payload)
        canonical_rows.append(payload)
        by_lifecycle[str(payload["lifecycle_id"])].append(payload)
    if not canonical_rows:
        raise ValueError("C++ event-stream lockstep requires at least one journal row")

    mismatches: Counter[str] = Counter()
    samples: list[dict[str, object]] = []
    counts: Counter[str] = Counter()

    def mismatch(
        code: str,
        *,
        row: Mapping[str, object] | None = None,
        expected: object = None,
        observed: object = None,
    ) -> None:
        mismatches[code] += 1
        if len(samples) >= max(1, int(mismatch_sample_limit)):
            return
        samples.append(
            {
                "code": code,
                "lifecycle_id": str(row["lifecycle_id"]) if row else "",
                "lifecycle_sequence": int(row["lifecycle_sequence"]) if row else 0,
                "expected": expected,
                "observed": observed,
            }
        )

    for lifecycle_id in sorted(by_lifecycle):
        lifecycle_rows = sorted(
            by_lifecycle[lifecycle_id],
            key=lambda row: int(row["lifecycle_sequence"]),
        )
        native = cpp.mirror_order_lifecycle_journal_v2_event_stream(lifecycle_rows)
        if native["abi_version"] != CPP_EVENT_STREAM_MIRROR_ABI_VERSION:
            mismatch(
                "cpp_abi_version_mismatch",
                expected=CPP_EVENT_STREAM_MIRROR_ABI_VERSION,
                observed=native["abi_version"],
            )
        if native["journal_schema_version"] != ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION:
            mismatch(
                "cpp_journal_schema_version_mismatch",
                expected=ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION,
                observed=native["journal_schema_version"],
            )
        if tuple(native["journal_columns"]) != ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS:
            mismatch(
                "cpp_journal_schema_columns_mismatch",
                expected=list(ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS),
                observed=list(native["journal_columns"]),
            )
        if native["quantity_contract_id"] != ORDER_LIFECYCLE_QUANTITY_CONTRACT_ID:
            mismatch(
                "cpp_quantity_contract_mismatch",
                expected=ORDER_LIFECYCLE_QUANTITY_CONTRACT_ID,
                observed=native["quantity_contract_id"],
            )
        if float(native["terminal_remainder_abs_tolerance_btc"]) != (
            TERMINAL_REMAINDER_ABS_TOLERANCE_BTC
        ):
            mismatch(
                "cpp_terminal_tolerance_mismatch",
                expected=TERMINAL_REMAINDER_ABS_TOLERANCE_BTC,
                observed=native["terminal_remainder_abs_tolerance_btc"],
            )

        native_rows = list(native["rows"])
        if len(native_rows) != len(lifecycle_rows):
            mismatch(
                "cpp_event_count_mismatch",
                expected=len(lifecycle_rows),
                observed=len(native_rows),
            )
        for source, projected in zip(lifecycle_rows, native_rows, strict=False):
            expected = _expected_projection(source)
            if tuple(projected) != _PROJECTION_COLUMNS:
                mismatch(
                    "cpp_projection_schema_mismatch",
                    row=source,
                    expected=list(_PROJECTION_COLUMNS),
                    observed=list(projected),
                )
                continue
            for column in _PROJECTION_COLUMNS:
                if not _equal(column, expected[column], projected[column]):
                    mismatch(
                        f"cpp_projection_{column}_mismatch",
                        row=source,
                        expected=expected[column],
                        observed=projected[column],
                    )
            event = str(source["lifecycle_event"])
            if event == "cancel_rejected":
                phase = str(source["phase_after"])
                counts[f"cancel_reject_to_{phase}"] += 1
                if not bool(source["fill_risk_active_after"]):
                    mismatch(
                        "cancel_reject_fill_risk_not_resumed",
                        row=source,
                        expected=True,
                        observed=source["fill_risk_active_after"],
                    )
            if event == "full_fill" and not persisted_terminal_remainder_is_zero(
                source["remaining_quantity_after"]
            ):
                mismatch(
                    "terminal_remainder_not_exact_zero",
                    row=source,
                    expected=0.0,
                    observed=source["remaining_quantity_after"],
                )
            counts[f"event_{event}"] += 1

        counts["cpp_cancel_reject_to_ACTIVE"] += int(
            native["cancel_reject_active_count"]
        )
        counts["cpp_cancel_reject_to_PARTIALLY_FILLED"] += int(
            native["cancel_reject_partially_filled_count"]
        )

    if counts["cancel_reject_to_ACTIVE"] != counts["cpp_cancel_reject_to_ACTIVE"]:
        mismatch(
            "cpp_cancel_reject_active_count_mismatch",
            expected=counts["cancel_reject_to_ACTIVE"],
            observed=counts["cpp_cancel_reject_to_ACTIVE"],
        )
    if counts["cancel_reject_to_PARTIALLY_FILLED"] != counts[
        "cpp_cancel_reject_to_PARTIALLY_FILLED"
    ]:
        mismatch(
            "cpp_cancel_reject_partial_count_mismatch",
            expected=counts["cancel_reject_to_PARTIALLY_FILLED"],
            observed=counts["cpp_cancel_reject_to_PARTIALLY_FILLED"],
        )

    branch_support = bool(
        counts["cancel_reject_to_ACTIVE"] > 0
        and counts["cancel_reject_to_PARTIALLY_FILLED"] > 0
    )
    if require_cancel_reject_branches and not branch_support:
        mismatch(
            "cancel_reject_branch_support_incomplete",
            expected="ACTIVE and PARTIALLY_FILLED",
            observed={
                "ACTIVE": counts["cancel_reject_to_ACTIVE"],
                "PARTIALLY_FILLED": counts[
                    "cancel_reject_to_PARTIALLY_FILLED"
                ],
            },
        )

    mismatch_counts = dict(sorted(mismatches.items()))
    gates = {
        "journal_schema_lockstep": not any(
            code.startswith("cpp_journal_schema")
            or code == "cpp_projection_schema_mismatch"
            for code in mismatches
        ),
        "event_sequence_and_terminal_route_lockstep": not any(
            code.startswith("cpp_projection_")
            or code in {"cpp_event_count_mismatch", "cpp_abi_version_mismatch"}
            for code in mismatches
        ),
        "cancel_reject_risk_set_continuation": not any(
            "cancel_reject" in code for code in mismatches
        )
        and (branch_support or not require_cancel_reject_branches),
        "terminal_remainder_zero_contract": not any(
            code in {
                "cpp_quantity_contract_mismatch",
                "cpp_terminal_tolerance_mismatch",
                "terminal_remainder_not_exact_zero",
            }
            for code in mismatches
        ),
    }
    passed = bool(all(gates.values()) and not mismatch_counts)
    report: dict[str, Any] = {
        "schema_version": CPP_EVENT_STREAM_LOCKSTEP_REPORT_VERSION,
        "identity": "f07_order_lifecycle_v2_cpp_event_stream_lockstep_v1",
        "scope": {
            "mechanics_only": True,
            "economic_outcomes_read": False,
            "formal_40day_lockstep_executed": False,
            "live_deployed": False,
        },
        "abi_version": CPP_EVENT_STREAM_MIRROR_ABI_VERSION,
        "journal_schema_version": ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION,
        "journal_schema_sha256": journal_schema_sha256(),
        "projection_schema_sha256": projection_schema_sha256(),
        "quantity_contract": {
            "identity": ORDER_LIFECYCLE_QUANTITY_CONTRACT_ID,
            "terminal_remainder_abs_tolerance_btc": (
                TERMINAL_REMAINDER_ABS_TOLERANCE_BTC
            ),
            "persisted_terminal_remainder_btc": 0.0,
            "exchange_lot_size_is_not_terminal_tolerance": True,
        },
        "counts": {
            "lifecycle_count": len(by_lifecycle),
            "event_count": len(canonical_rows),
            **dict(sorted(counts.items())),
        },
        "cancel_reject_branch_support_complete": branch_support,
        "mismatch_counts": mismatch_counts,
        "mismatch_samples": samples,
        "gates": gates,
        "mechanics_lockstep_passed": passed,
        "permissions": {
            "cif_training": False,
            "economic_evaluation": False,
            "q90_action": False,
            "live_deployment": False,
        },
    }
    report["canonical_report_sha256"] = _canonical_sha256(report)
    return report


def build_cpp_event_stream_binding_artifact(
    *,
    lockstep_report: Mapping[str, object],
    runtime_code_identity_sha256: str,
    implementation_paths: Sequence[str | Path],
) -> dict[str, object]:
    """Build the immutable binding payload consumed by 40-day admission."""

    if not bool(lockstep_report.get("mechanics_lockstep_passed", False)):
        raise ValueError("C++ event-stream binding requires a passing lockstep report")
    if not bool(lockstep_report.get("cancel_reject_branch_support_complete", False)):
        raise ValueError("binding evidence must cover both cancel-reject continuation phases")
    runtime_sha = str(runtime_code_identity_sha256)
    if len(runtime_sha) != 64 or any(char not in "0123456789abcdef" for char in runtime_sha):
        raise ValueError("runtime code identity SHA256 is invalid")

    implementations = []
    for value in implementation_paths:
        path = Path(value).expanduser().resolve()
        implementations.append(
            {
                "path": str(path),
                "size_bytes": path.stat().st_size,
                "sha256": _file_sha256(path),
            }
        )
    payload: dict[str, object] = {
        "schema_version": CPP_EVENT_STREAM_BINDING_SCHEMA_VERSION,
        "status": "bound",
        "abi_version": CPP_EVENT_STREAM_MIRROR_ABI_VERSION,
        "runtime_code_identity_sha256": runtime_sha,
        "journal_schema_version": ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION,
        "journal_schema_sha256": journal_schema_sha256(),
        "projection_schema_sha256": projection_schema_sha256(),
        "quantity_contract_id": ORDER_LIFECYCLE_QUANTITY_CONTRACT_ID,
        "terminal_remainder_abs_tolerance_btc": (
            TERMINAL_REMAINDER_ABS_TOLERANCE_BTC
        ),
        "persisted_terminal_remainder_btc": 0.0,
        "cancel_reject_active_branch_observed": True,
        "cancel_reject_partially_filled_branch_observed": True,
        "lockstep_report_sha256": str(lockstep_report["canonical_report_sha256"]),
        "implementation_artifacts": implementations,
        "mechanics_only": True,
        "economic_outcomes_read": False,
        "formal_40day_lockstep_executed": False,
    }
    payload["canonical_binding_sha256"] = _canonical_sha256(payload)
    return payload
