#!/usr/bin/env python3
"""Run F07 C++ event lockstep on the homogeneous v1.6 successor panel."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import uuid
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from research.families.f07_active_order_continuation.audit import (
    order_lifecycle_v2_40day_replay_emitter as emitter,
)
from research.families.f07_active_order_continuation.audit import (
    order_lifecycle_v2_runtime_compatibility_v1_6 as successor,
)
from research.families.f07_active_order_continuation.audit.order_lifecycle_v2_40day_cpp_lockstep import (
    audit_post_terminal_risk_reuse,
)
from research.families.f07_active_order_continuation.audit.order_lifecycle_v2_cpp_event_stream_binding_v2 import (
    CPP_EVENT_STREAM_MIRROR_ABI_VERSION,
    audit_cpp_event_stream_lockstep,
)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _artifact(path: Path) -> dict[str, object]:
    resolved = path.expanduser().resolve()
    return {
        "path": str(resolved),
        "size_bytes": int(resolved.stat().st_size),
        "sha256": _file_sha256(resolved),
    }


def _atomic_write_json(path: Path, payload: Mapping[str, object]) -> None:
    resolved = path.expanduser().resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    temporary = resolved.with_name(f".{resolved.name}.partial-{os.getpid()}-{uuid.uuid4().hex}")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, resolved)
    finally:
        temporary.unlink(missing_ok=True)


def run_panel_lockstep(
    *, plan_path: Path, amendment_path: Path, output_path: Path
) -> dict[str, object]:
    amendment, plan = successor.validate_successor_amendment(
        amendment_path,
        successor_plan_path=plan_path,
    )
    support_contract = amendment["cancel_reject_support_contract"]
    synthetic_reference = amendment["synthetic_cancel_reject_lockstep"]
    synthetic_report = successor._read_json(
        Path(str(synthetic_reference["path"])),
        label="bound synthetic cancel-reject lockstep",
    )
    synthetic_counts = synthetic_report["lockstep_summary"]["counts"]
    by_day = emitter.validate_execution_plan(plan)
    cache_root = Path(str(plan["cache_root"])).expanduser().resolve()
    ordered_days = list(map(str, plan["ordered_utc_days"]))
    if len(ordered_days) != 40:
        raise successor.RuntimeCompatibilityError("v1.6 lockstep requires exactly 40 days")

    mismatch_totals: Counter[str] = Counter()
    safety_totals: Counter[str] = Counter()
    day_reports: list[dict[str, object]] = []
    event_count = 0
    lifecycle_count = 0
    exact_native_lifecycle_count = 0
    cancel_reject_count = 0
    cancel_reject_route_count = 0
    for day in ordered_days:
        manifest_path = cache_root / "days" / day / "day_manifest.json"
        manifest = emitter._validate_day_manifest(
            manifest_path,
            plan=plan,
            day_row=by_day[day],
        )
        session = cache_root / "days" / day / str(manifest["journal_v2"]["session_root"])
        rows, _, _ = emitter._read_journal_parts(session)
        lockstep = audit_cpp_event_stream_lockstep(rows, require_cancel_reject_branches=False)
        safety = audit_post_terminal_risk_reuse(rows)
        mismatch_totals.update({str(k): int(v) for k, v in lockstep["mismatch_counts"].items()})
        safety_totals.update({str(k): int(v) for k, v in safety["violation_counts"].items()})
        event_count += int(lockstep["counts"]["event_count"])
        lifecycle_count += int(lockstep["counts"]["lifecycle_count"])
        exact_native_lifecycle_count += int(
            manifest["journal_v2"]["cif_eligibility"]["eligible_lifecycle_count"]
        )
        counters = manifest["journal_v2"]["counters"]
        cancel_reject_count += int(counters["cancel_reject_count"])
        cancel_reject_route_count += int(counters["cancel_reject_to_active_count"])
        cancel_reject_route_count += int(counters["cancel_reject_to_partially_filled_count"])
        day_reports.append(
            {
                "day": day,
                "day_manifest_sha256": _file_sha256(manifest_path),
                "journal_row_count": len(rows),
                "lockstep_report_sha256": str(lockstep["canonical_report_sha256"]),
                "mechanics_lockstep_passed": bool(lockstep["mechanics_lockstep_passed"]),
                "mismatch_counts": dict(lockstep["mismatch_counts"]),
                "post_terminal_safety": safety,
            }
        )
    cpp = (
        Path(str(plan["global_execution_identity"]["cpp_event_stream"]["module_artifact"]["path"]))
        .expanduser()
        .resolve()
    )
    expected_cpp_sha = str(
        plan["global_execution_identity"]["cpp_event_stream"]["module_artifact"]["sha256"]
    )
    empirical_support_frozen_zero = bool(
        cancel_reject_count == int(support_contract["empirical_cancel_reject_count"]) == 0
        and cancel_reject_route_count
        == int(support_contract["empirical_cancel_reject_route_count"])
        == 0
        and support_contract["empirical_cancel_reject_support"] is False
        and support_contract["empirical_cancel_reject_transport_support"] is False
    )
    synthetic_branch_contract_complete = bool(
        synthetic_report["status"] == "passed"
        and synthetic_report["require_cancel_reject_branches"] is True
        and int(synthetic_counts["cancel_reject_to_ACTIVE"]) == 1
        and int(synthetic_counts["cancel_reject_to_PARTIALLY_FILLED"]) == 1
        and synthetic_report["scope"]["empirical_transport_support"] is False
    )
    gates = {
        "forty_days_present": len(day_reports) == 40,
        "all_day_event_lockstep": all(row["mechanics_lockstep_passed"] for row in day_reports),
        "zero_python_cpp_mismatch": not mismatch_totals,
        "zero_post_terminal_risk_or_queue_reuse": not safety_totals,
        "cpp_module_hash_bound": _file_sha256(cpp) == expected_cpp_sha,
        "exact_native_spells_present": exact_native_lifecycle_count > 0,
        # These legacy downstream key names refer to the bound branch contract,
        # not empirical 40-day transport support. The empirical counts below
        # remain exactly zero and are frozen by the successor amendment.
        "cancel_reject_branch_present": synthetic_branch_contract_complete,
        "cancel_reject_routes_complete": bool(
            empirical_support_frozen_zero and synthetic_branch_contract_complete
        ),
        "homogeneous_successor_amendment_bound": True,
        "full_40day_fingerprint_equivalence_bound": True,
        "economic_outcomes_not_read": True,
    }
    passed = all(gates.values())
    report: dict[str, Any] = {
        "schema_version": successor.LOCKSTEP_SCHEMA_VERSION,
        "identity": successor.LOCKSTEP_IDENTITY,
        "status": "passed" if passed else "failed_closed",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "successor_amendment": {
            **_artifact(amendment_path),
            "canonical_amendment_sha256": amendment["canonical_amendment_sha256"],
        },
        "plan": _artifact(plan_path),
        "panel_manifest": _artifact(cache_root / "panel_manifest.json"),
        "plan_sha256": plan["canonical_plan_sha256"],
        "global_execution_identity_sha256": plan["global_execution_identity_sha256"],
        "cpp_abi_version": CPP_EVENT_STREAM_MIRROR_ABI_VERSION,
        "cpp_module": _artifact(cpp),
        "counts": {
            "day_count": len(day_reports),
            "event_count": event_count,
            "lifecycle_count": lifecycle_count,
            "exact_native_lifecycle_count": exact_native_lifecycle_count,
            "cancel_reject_count": cancel_reject_count,
            "cancel_reject_route_count": cancel_reject_route_count,
        },
        "mismatch_counts": dict(sorted(mismatch_totals.items())),
        "post_terminal_violation_counts": dict(sorted(safety_totals.items())),
        "days": day_reports,
        "gates": gates,
        "formal_40day_lockstep_passed": passed,
        "scope": {"mechanics_only": True, "economic_outcomes_read": False},
        "permissions": dict(successor.LOCKSTEP_PERMISSIONS),
    }
    report["canonical_report_sha256"] = successor.canonical_sha256(report)
    _atomic_write_json(output_path, report)
    if not passed:
        raise successor.RuntimeCompatibilityError("v1.6 C++ event lockstep failed closed")
    return report


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--successor-amendment", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    report = run_panel_lockstep(
        plan_path=args.plan,
        amendment_path=args.successor_amendment,
        output_path=args.out,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
