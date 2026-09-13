#!/usr/bin/env python3
"""Bind synthetic ACTIVE/PARTIALLY_FILLED cancel-reject C++ lockstep evidence.

This audit covers branches absent from the retained 40-day empirical panel.  It
is mechanics-only synthetic evidence and must never be described as historical
or live transport support.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
import uuid
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from models.replay.order_lifecycle_v2_replay_adapter_strict_native import (
    OrderLifecycleV2ReplayAdapter,
)
from research.families.f07_active_order_continuation.audit import (
    order_lifecycle_v2_cpp_event_stream_binding_v2 as binding,
)
from research.families.f07_active_order_continuation.audit import (
    order_lifecycle_v2_runtime_compatibility_v1_6 as successor,
)

ROOT = Path(__file__).resolve().parents[4]
IDENTITY = "f07_order_lifecycle_v2_cancel_reject_synthetic_lockstep_v1_6"
SCHEMA_VERSION = "f07_order_lifecycle_v2_cancel_reject_synthetic_lockstep_report.v1_6"
INPUT_SCHEMA_VERSION = "f07_cancel_reject_synthetic_scenarios.v1"
SCOPE = {
    "mechanics_only": True,
    "synthetic_branch_support_only": True,
    "empirical_transport_support": False,
    "economic_outcomes_read": False,
}
PERMISSIONS = dict(successor.DENIED_PERMISSIONS)

SYNTHETIC_INPUT_CONTRACT: dict[str, object] = {
    "schema_version": INPUT_SCHEMA_VERSION,
    "require_cancel_reject_branches": True,
    "scenarios": [
        {
            "scenario": "cancel_reject_to_active",
            "trace_id": 1,
            "submit_ts_ms": 1_000,
            "activate_visibility_ts_ms": 1_020,
            "activate_exchange_ts_ms": 1_010,
            "cancel_request_ts_ms": 1_040,
            "cancel_reject_visibility_ts_ms": 1_060,
            "cancel_reject_exchange_ts_ms": 1_050,
            "terminal_cancel_request_ts_ms": 1_080,
            "terminal_cancel_ack_visibility_ts_ms": 1_100,
            "terminal_cancel_ack_exchange_ts_ms": 1_090,
        },
        {
            "scenario": "cancel_reject_to_partially_filled",
            "trace_id": 2,
            "submit_ts_ms": 2_000,
            "activate_visibility_ts_ms": 2_020,
            "activate_exchange_ts_ms": 2_010,
            "partial_fill_visibility_ts_ms": 2_040,
            "partial_fill_exchange_ts_ms": 2_030,
            "remaining_after_btc": 0.0004,
            "cancel_request_ts_ms": 2_060,
            "cancel_reject_visibility_ts_ms": 2_080,
            "cancel_reject_exchange_ts_ms": 2_070,
            "terminal_cancel_request_ts_ms": 2_100,
            "terminal_cancel_ack_visibility_ts_ms": 2_120,
            "terminal_cancel_ack_exchange_ts_ms": 2_110,
        },
    ],
}

REPORT_KEYS = frozenset(
    {
        "schema_version",
        "identity",
        "status",
        "generated_at_utc",
        "require_cancel_reject_branches",
        "plan",
        "cpp_abi_version",
        "cpp_module",
        "synthetic_input_contract",
        "synthetic_input_contract_sha256",
        "synthetic_rows",
        "lockstep_summary",
        "implementation_artifacts",
        "gates",
        "scope",
        "permissions",
        "canonical_report_sha256",
    }
)
GATE_KEYS = frozenset(
    {
        "require_cancel_reject_branches_true",
        "active_cancel_reject_recovery_observed",
        "partially_filled_cancel_reject_recovery_observed",
        "branch_counts_exact",
        "zero_python_cpp_mismatch",
        "risk_set_resumed_after_cancel_reject",
        "cpp_module_hash_bound",
        "synthetic_support_not_transport_support",
        "economic_outcomes_not_read",
    }
)
IMPLEMENTATION_ROLES = frozenset(
    {
        "synthetic_audit",
        "replay_adapter",
        "order_lifecycle",
        "cpp_event_stream_binding",
    }
)
SYNTHETIC_ROWS_KEYS = frozenset({"row_count", "lifecycle_count", "rows_sha256"})
LOCKSTEP_SUMMARY_KEYS = frozenset(
    {
        "canonical_report_sha256",
        "mechanics_lockstep_passed",
        "cancel_reject_branch_support_complete",
        "counts",
        "mismatch_counts",
        "gates",
    }
)


def _canonical_sha256(value: object) -> str:
    return successor.canonical_sha256(value)


def _atomic_write_json(path: Path, payload: Mapping[str, object]) -> None:
    resolved = path.expanduser().resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    temporary = resolved.with_name(f".{resolved.name}.partial-{os.getpid()}-{uuid.uuid4().hex}")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(
                payload,
                handle,
                indent=2,
                sort_keys=True,
                ensure_ascii=True,
                allow_nan=False,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, resolved)
    finally:
        temporary.unlink(missing_ok=True)


def _order(trace_id: int, submit_ms: int) -> dict[str, object]:
    return {
        "trace_id": trace_id,
        "side": "BUY" if trace_id % 2 else "SELL",
        "submit_ts": submit_ms,
        "quote_ts": submit_ms,
        "quantity": 0.001,
        "remaining": 0.001,
        "price": 100.0,
        "inventory_at_submit": 0.0,
        "inventory_role_at_submit": "opener",
        "campaign_id_at_submit": 0,
        "state": "PENDING_NEW",
        "fill_eligible": True,
        "simulator_queue_source": "native_exchange_book",
        "exact_queue_path_valid": True,
    }


def _read_rows(root: Path, session_id: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    parts = root / f"session-{session_id}" / "parts"
    for manifest_path in sorted(parts.glob("part-*.manifest.json")):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        rows.extend(pq.read_table(parts / str(manifest["data_file"])).to_pylist())
    return sorted(
        rows,
        key=lambda row: (str(row["lifecycle_id"]), int(row["lifecycle_sequence"])),
    )


def _validate_plan(plan_path: Path) -> tuple[dict[str, Any], Path]:
    path = plan_path.expanduser().resolve()
    plan = successor._read_json(path, label="successor execution plan")
    if plan.get("canonical_plan_sha256") != successor.canonical_document_sha256(
        plan, "canonical_plan_sha256"
    ):
        raise successor.RuntimeCompatibilityError("synthetic lockstep plan SHA256 differs")
    global_identity = plan.get("global_execution_identity")
    if not isinstance(global_identity, Mapping):
        raise successor.RuntimeCompatibilityError("synthetic lockstep global identity is missing")
    if plan.get("global_execution_identity_sha256") != _canonical_sha256(global_identity):
        raise successor.RuntimeCompatibilityError("synthetic lockstep global identity differs")
    cpp = global_identity.get("cpp_event_stream")
    if not isinstance(cpp, Mapping) or not isinstance(cpp.get("module_artifact"), Mapping):
        raise successor.RuntimeCompatibilityError("synthetic lockstep C++ binding is missing")
    module_reference = cpp["module_artifact"]
    module_path = successor._validate_artifact(
        {key: module_reference[key] for key in ("path", "size_bytes", "sha256")},
        label="synthetic lockstep C++ module",
    )
    if cpp.get("abi_version") != binding.CPP_EVENT_STREAM_MIRROR_ABI_VERSION:
        raise successor.RuntimeCompatibilityError("synthetic lockstep C++ ABI differs")
    import narrowgate_cpp

    if Path(str(narrowgate_cpp.__file__)).expanduser().resolve() != module_path:
        raise successor.RuntimeCompatibilityError(
            "loaded C++ module differs from successor execution plan"
        )
    return plan, module_path


def _generate_rows(root: Path, plan: Mapping[str, object]) -> list[dict[str, object]]:
    session_id = "f07-v1-6-cancel-reject"
    adapter = OrderLifecycleV2ReplayAdapter(
        root=root,
        session_id=session_id,
        runtime_identity={
            "baseline_epoch_id": "synthetic-cancel-reject-v1-6",
            "runtime_code_sha256": str(plan["global_execution_identity_sha256"]),
            "config_sha256": "0" * 64,
            "execution_abi": "order-lifecycle-v2",
        },
        symbol="BTCUSDC",
        strict_native_only=True,
    )

    active_spec = SYNTHETIC_INPUT_CONTRACT["scenarios"][0]
    active = _order(int(active_spec["trace_id"]), int(active_spec["submit_ts_ms"]))
    adapter.submit(active, int(active_spec["submit_ts_ms"]))
    adapter.activate(
        active,
        visibility_ts_ms=int(active_spec["activate_visibility_ts_ms"]),
        exchange_ts_ms=int(active_spec["activate_exchange_ts_ms"]),
    )
    adapter.request_cancel(active, int(active_spec["cancel_request_ts_ms"]))
    adapter.cancel_reject(
        active,
        visibility_ts_ms=int(active_spec["cancel_reject_visibility_ts_ms"]),
        exchange_ts_ms=int(active_spec["cancel_reject_exchange_ts_ms"]),
    )
    adapter.request_cancel(active, int(active_spec["terminal_cancel_request_ts_ms"]))
    adapter.cancel_ack(
        active,
        visibility_ts_ms=int(active_spec["terminal_cancel_ack_visibility_ts_ms"]),
        exchange_ts_ms=int(active_spec["terminal_cancel_ack_exchange_ts_ms"]),
    )

    partial_spec = SYNTHETIC_INPUT_CONTRACT["scenarios"][1]
    partial = _order(int(partial_spec["trace_id"]), int(partial_spec["submit_ts_ms"]))
    adapter.submit(partial, int(partial_spec["submit_ts_ms"]))
    adapter.activate(
        partial,
        visibility_ts_ms=int(partial_spec["activate_visibility_ts_ms"]),
        exchange_ts_ms=int(partial_spec["activate_exchange_ts_ms"]),
    )
    adapter.fill(
        partial,
        remaining_after=float(partial_spec["remaining_after_btc"]),
        visibility_ts_ms=int(partial_spec["partial_fill_visibility_ts_ms"]),
        exchange_ts_ms=int(partial_spec["partial_fill_exchange_ts_ms"]),
        full_fill=False,
    )
    adapter.request_cancel(partial, int(partial_spec["cancel_request_ts_ms"]))
    adapter.cancel_reject(
        partial,
        visibility_ts_ms=int(partial_spec["cancel_reject_visibility_ts_ms"]),
        exchange_ts_ms=int(partial_spec["cancel_reject_exchange_ts_ms"]),
    )
    adapter.request_cancel(partial, int(partial_spec["terminal_cancel_request_ts_ms"]))
    adapter.cancel_ack(
        partial,
        visibility_ts_ms=int(partial_spec["terminal_cancel_ack_visibility_ts_ms"]),
        exchange_ts_ms=int(partial_spec["terminal_cancel_ack_exchange_ts_ms"]),
    )
    health = adapter.close()
    if int(health["rows_dropped"]) != 0 or int(health["error_count"]) != 0:
        raise successor.RuntimeCompatibilityError("synthetic lifecycle journal lost rows")
    return _read_rows(root, session_id)


def _rows_fingerprint(rows: Sequence[Mapping[str, object]]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(
            json.dumps(
                row,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            ).encode("utf-8")
        )
        digest.update(b"\n")
    return digest.hexdigest()


def _implementation_artifacts() -> dict[str, dict[str, object]]:
    import models.replay.order_lifecycle_v2_replay_adapter_strict_native as replay_adapter

    return {
        "synthetic_audit": successor.artifact_identity(Path(__file__)),
        "replay_adapter": successor.artifact_identity(Path(str(replay_adapter.__file__))),
        "order_lifecycle": successor.artifact_identity(ROOT / "execution/order_lifecycle.py"),
        "cpp_event_stream_binding": successor.artifact_identity(Path(str(binding.__file__))),
    }


def _evaluate(plan: Mapping[str, object]) -> tuple[list[dict[str, object]], dict[str, object]]:
    with tempfile.TemporaryDirectory(prefix="f07-cancel-reject-v1-6-") as directory:
        rows = _generate_rows(Path(directory), plan)
    lockstep = binding.audit_cpp_event_stream_lockstep(
        rows,
        require_cancel_reject_branches=True,
    )
    return rows, lockstep


def run_synthetic_lockstep(
    *,
    plan_path: Path,
    output_path: Path,
) -> dict[str, object]:
    plan, cpp_path = _validate_plan(plan_path)
    rows, lockstep = _evaluate(plan)
    counts = lockstep["counts"]
    gates = {
        "require_cancel_reject_branches_true": True,
        "active_cancel_reject_recovery_observed": int(counts["cancel_reject_to_ACTIVE"]) == 1,
        "partially_filled_cancel_reject_recovery_observed": int(
            counts["cancel_reject_to_PARTIALLY_FILLED"]
        )
        == 1,
        "branch_counts_exact": int(counts["event_cancel_rejected"]) == 2,
        "zero_python_cpp_mismatch": lockstep["mismatch_counts"] == {},
        "risk_set_resumed_after_cancel_reject": bool(
            lockstep["gates"]["cancel_reject_risk_set_continuation"]
        ),
        "cpp_module_hash_bound": True,
        "synthetic_support_not_transport_support": True,
        "economic_outcomes_not_read": True,
    }
    passed = bool(lockstep["mechanics_lockstep_passed"] and all(gates.values()))
    report: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": "passed" if passed else "failed_closed",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "require_cancel_reject_branches": True,
        "plan": {
            **successor.artifact_identity(plan_path),
            "canonical_plan_sha256": plan["canonical_plan_sha256"],
            "global_execution_identity_sha256": plan["global_execution_identity_sha256"],
        },
        "cpp_abi_version": binding.CPP_EVENT_STREAM_MIRROR_ABI_VERSION,
        "cpp_module": successor.artifact_identity(cpp_path),
        "synthetic_input_contract": SYNTHETIC_INPUT_CONTRACT,
        "synthetic_input_contract_sha256": _canonical_sha256(SYNTHETIC_INPUT_CONTRACT),
        "synthetic_rows": {
            "row_count": len(rows),
            "lifecycle_count": len({str(row["lifecycle_id"]) for row in rows}),
            "rows_sha256": _rows_fingerprint(rows),
        },
        "lockstep_summary": {
            "canonical_report_sha256": lockstep["canonical_report_sha256"],
            "mechanics_lockstep_passed": lockstep["mechanics_lockstep_passed"],
            "cancel_reject_branch_support_complete": lockstep[
                "cancel_reject_branch_support_complete"
            ],
            "counts": lockstep["counts"],
            "mismatch_counts": lockstep["mismatch_counts"],
            "gates": lockstep["gates"],
        },
        "implementation_artifacts": _implementation_artifacts(),
        "gates": gates,
        "scope": dict(SCOPE),
        "permissions": dict(PERMISSIONS),
    }
    report["canonical_report_sha256"] = _canonical_sha256(report)
    _atomic_write_json(output_path, report)
    if not passed:
        raise successor.RuntimeCompatibilityError(
            "synthetic cancel-reject C++ lockstep failed closed"
        )
    return report


def validate_synthetic_lockstep_report(
    path: Path,
    *,
    plan_path: Path,
    reproduce: bool,
) -> dict[str, Any]:
    report = successor._read_json(path, label="synthetic cancel-reject lockstep")
    successor._require_exact_keys(report, REPORT_KEYS, label="synthetic lockstep report")
    if (
        report.get("schema_version") != SCHEMA_VERSION
        or report.get("identity") != IDENTITY
        or report.get("status") != "passed"
        or report.get("require_cancel_reject_branches") is not True
    ):
        raise successor.RuntimeCompatibilityError("synthetic lockstep identity differs")
    if report.get("canonical_report_sha256") != successor.canonical_document_sha256(
        report, "canonical_report_sha256"
    ):
        raise successor.RuntimeCompatibilityError("synthetic lockstep SHA256 differs")
    if report.get("scope") != SCOPE or report.get("permissions") != PERMISSIONS:
        raise successor.RuntimeCompatibilityError("synthetic lockstep authority differs")
    gates = report.get("gates")
    if not isinstance(gates, Mapping):
        raise successor.RuntimeCompatibilityError("synthetic lockstep gates are missing")
    successor._require_exact_keys(gates, GATE_KEYS, label="synthetic lockstep gates")
    if not all(value is True for value in gates.values()):
        raise successor.RuntimeCompatibilityError("synthetic lockstep gates did not pass")
    if report.get("synthetic_input_contract") != SYNTHETIC_INPUT_CONTRACT or report.get(
        "synthetic_input_contract_sha256"
    ) != _canonical_sha256(SYNTHETIC_INPUT_CONTRACT):
        raise successor.RuntimeCompatibilityError("synthetic input contract differs")

    plan, cpp_path = _validate_plan(plan_path)
    plan_reference = report.get("plan")
    if not isinstance(plan_reference, Mapping):
        raise successor.RuntimeCompatibilityError("synthetic lockstep plan is missing")
    successor._require_exact_keys(
        plan_reference,
        {
            "path",
            "size_bytes",
            "sha256",
            "canonical_plan_sha256",
            "global_execution_identity_sha256",
        },
        label="synthetic lockstep plan",
    )
    artifact = {key: plan_reference[key] for key in ("path", "size_bytes", "sha256")}
    if (
        successor._validate_artifact(artifact, label="synthetic plan")
        != plan_path.expanduser().resolve()
    ):
        raise successor.RuntimeCompatibilityError("synthetic lockstep plan path differs")
    if (
        plan_reference["canonical_plan_sha256"] != plan["canonical_plan_sha256"]
        or plan_reference["global_execution_identity_sha256"]
        != plan["global_execution_identity_sha256"]
    ):
        raise successor.RuntimeCompatibilityError("synthetic lockstep plan identity differs")
    if successor._validate_artifact(report["cpp_module"], label="synthetic C++ module") != cpp_path:
        raise successor.RuntimeCompatibilityError("synthetic C++ module differs")
    if report.get("cpp_abi_version") != binding.CPP_EVENT_STREAM_MIRROR_ABI_VERSION:
        raise successor.RuntimeCompatibilityError("synthetic C++ ABI differs")

    implementations = report.get("implementation_artifacts")
    if not isinstance(implementations, Mapping):
        raise successor.RuntimeCompatibilityError("synthetic implementations are missing")
    successor._require_exact_keys(
        implementations,
        IMPLEMENTATION_ROLES,
        label="synthetic implementation artifacts",
    )
    expected_implementations = _implementation_artifacts()
    if implementations != expected_implementations:
        raise successor.RuntimeCompatibilityError("synthetic implementation identity differs")
    for label, reference in implementations.items():
        successor._validate_artifact(reference, label=f"synthetic {label}")

    summary = report.get("lockstep_summary")
    rows_summary = report.get("synthetic_rows")
    if not isinstance(summary, Mapping) or not isinstance(rows_summary, Mapping):
        raise successor.RuntimeCompatibilityError("synthetic lockstep evidence is missing")
    successor._require_exact_keys(
        summary,
        LOCKSTEP_SUMMARY_KEYS,
        label="synthetic lockstep summary",
    )
    successor._require_exact_keys(
        rows_summary,
        SYNTHETIC_ROWS_KEYS,
        label="synthetic rows summary",
    )
    counts = summary.get("counts")
    if (
        not bool(summary.get("mechanics_lockstep_passed"))
        or not bool(summary.get("cancel_reject_branch_support_complete"))
        or summary.get("mismatch_counts") != {}
        or not isinstance(counts, Mapping)
        or int(counts.get("cancel_reject_to_ACTIVE", 0)) != 1
        or int(counts.get("cancel_reject_to_PARTIALLY_FILLED", 0)) != 1
    ):
        raise successor.RuntimeCompatibilityError("synthetic cancel-reject branches differ")

    if reproduce:
        rows, observed = _evaluate(plan)
        expected_rows = {
            "row_count": len(rows),
            "lifecycle_count": len({str(row["lifecycle_id"]) for row in rows}),
            "rows_sha256": _rows_fingerprint(rows),
        }
        expected_summary = {
            "canonical_report_sha256": observed["canonical_report_sha256"],
            "mechanics_lockstep_passed": observed["mechanics_lockstep_passed"],
            "cancel_reject_branch_support_complete": observed[
                "cancel_reject_branch_support_complete"
            ],
            "counts": observed["counts"],
            "mismatch_counts": observed["mismatch_counts"],
            "gates": observed["gates"],
        }
        if rows_summary != expected_rows or summary != expected_summary:
            raise successor.RuntimeCompatibilityError("synthetic lockstep reproduction differs")
    return report


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    report = run_synthetic_lockstep(plan_path=args.plan, output_path=args.out)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
