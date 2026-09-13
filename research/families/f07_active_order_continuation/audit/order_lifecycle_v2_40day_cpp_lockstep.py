#!/usr/bin/env python3
"""Run the formal outcome-blind 40-day Python/C++ journal-v2 lockstep."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import uuid
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from execution.order_lifecycle import FILL_RISK_PHASES
from research.families.f07_active_order_continuation.audit import (
    order_lifecycle_v2_40day_replay_emitter as emitter,
)
from research.families.f07_active_order_continuation.audit import (
    order_lifecycle_v2_downstream_execution_amendment_v1_5 as provenance,
)
from research.families.f07_active_order_continuation.audit.order_lifecycle_v2_cpp_event_stream_binding_v2 import (
    CPP_EVENT_STREAM_MIRROR_ABI_VERSION,
    audit_cpp_event_stream_lockstep,
)

IDENTITY = provenance.LOCKSTEP_IDENTITY
SCHEMA_VERSION = provenance.LOCKSTEP_SCHEMA_VERSION


def _canonical_sha256(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


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
        descriptor = os.open(resolved.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def audit_post_terminal_risk_reuse(
    rows: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    """Reject queue/fill-risk reuse after, but not on, a terminal transition."""

    by_lifecycle: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for row in rows:
        by_lifecycle[str(row["lifecycle_id"])].append(row)
    violations: Counter[str] = Counter()
    risk_transition_rows = 0
    post_terminal_rows = 0
    for lifecycle_rows in by_lifecycle.values():
        ordered = sorted(lifecycle_rows, key=lambda item: int(item["lifecycle_sequence"]))
        terminal = [
            index
            for index, row in enumerate(ordered)
            if str(row["terminal_observation"]) in {
                "EXCHANGE_TERMINAL",
                "LOCAL_SHUTDOWN_CENSOR",
            }
        ]
        if len(terminal) != 1:
            violations["terminal_cardinality"] += 1
            continue
        terminal_index = terminal[0]
        terminal_row = ordered[terminal_index]
        if (
            str(terminal_row["phase_before"]) in FILL_RISK_PHASES
            or str(terminal_row["phase_after"]) in FILL_RISK_PHASES
        ):
            risk_transition_rows += 1
        for row in ordered[terminal_index + 1 :]:
            post_terminal_rows += 1
            if (
                str(row["phase_before"]) in FILL_RISK_PHASES
                or str(row["phase_after"]) in FILL_RISK_PHASES
                or bool(row["fill_risk_active_after"])
            ):
                violations["post_terminal_fill_risk_reuse"] += 1
            if bool(row["exact_queue_path_valid"]):
                violations["post_terminal_exact_queue_reuse"] += 1
            if str(row["simulator_queue_source"]) == "native_exchange_book":
                violations["post_terminal_native_queue_identity_reuse"] += 1
    return {
        "lifecycle_count": len(by_lifecycle),
        "terminal_transition_from_risk_count": risk_transition_rows,
        "post_terminal_row_count": post_terminal_rows,
        "violation_counts": dict(sorted(violations.items())),
        "passed": not violations,
    }


def run_panel_lockstep(
    *,
    plan_path: Path,
    amendment_path: Path,
    output_path: Path,
) -> dict[str, object]:
    plan_file = plan_path.expanduser().resolve()
    amendment_file = amendment_path.expanduser().resolve()
    amendment, plan = provenance.validate_downstream_execution_amendment(
        amendment_file,
        plan_path=plan_file,
    )
    by_day = emitter.validate_execution_plan(plan)
    cache_root = Path(str(plan["cache_root"])).expanduser().resolve()
    panel_path = cache_root / "panel_manifest.json"
    panel = provenance.validate_panel_manifest_strict(panel_path, plan=plan)
    ordered_days = list(map(str, plan["ordered_utc_days"]))
    if len(ordered_days) != 40 or list(map(str, panel.get("ordered_utc_days", []))) != ordered_days:
        raise RuntimeError("lockstep requires the complete frozen 40-day panel")
    if not bool(panel.get("scope", {}).get("formal_40day_journal_emission_complete", False)):
        raise RuntimeError("journal panel is not formally complete")
    if panel.get("plan_sha256") != plan["canonical_plan_sha256"]:
        raise RuntimeError("journal panel and plan identities differ")

    day_reports: list[dict[str, object]] = []
    mismatch_totals: Counter[str] = Counter()
    safety_totals: Counter[str] = Counter()
    event_count = 0
    lifecycle_count = 0
    exact_native_lifecycle_count = 0
    censored_lifecycle_count = 0
    for day in ordered_days:
        manifest_path = cache_root / "days" / day / "day_manifest.json"
        day_manifest = emitter._validate_day_manifest(
            manifest_path,
            plan=plan,
            day_row=by_day[day],
        )
        session_root = cache_root / "days" / day / str(
            day_manifest["journal_v2"]["session_root"]
        )
        rows, _, _ = emitter._read_journal_parts(session_root)
        lockstep = audit_cpp_event_stream_lockstep(
            rows,
            require_cancel_reject_branches=False,
        )
        safety = audit_post_terminal_risk_reuse(rows)
        for code, count in lockstep["mismatch_counts"].items():
            mismatch_totals[str(code)] += int(count)
        for code, count in safety["violation_counts"].items():
            safety_totals[str(code)] += int(count)
        event_count += int(lockstep["counts"]["event_count"])
        lifecycle_count += int(lockstep["counts"]["lifecycle_count"])
        eligibility = day_manifest["journal_v2"]["cif_eligibility"]
        exact_native_lifecycle_count += int(eligibility["eligible_lifecycle_count"])
        censored_lifecycle_count += int(eligibility["censored_lifecycle_count"])
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

    cpp_module = Path(
        str(plan["global_execution_identity"]["cpp_event_stream"]["module_artifact"]["path"])
    ).expanduser().resolve()
    expected_cpp_sha = str(
        plan["global_execution_identity"]["cpp_event_stream"]["module_artifact"]["sha256"]
    )
    if _file_sha256(cpp_module) != expected_cpp_sha:
        raise RuntimeError("loaded C++ module bytes differ from the frozen plan")
    gates = {
        "forty_days_present": len(day_reports) == 40,
        "all_day_event_lockstep": all(
            bool(day["mechanics_lockstep_passed"]) for day in day_reports
        ),
        "zero_python_cpp_mismatch": not mismatch_totals,
        "zero_post_terminal_risk_or_queue_reuse": not safety_totals,
        "cpp_module_hash_bound": True,
        "exact_native_spells_present": exact_native_lifecycle_count > 0,
        "panel_canonical_hash_bound": True,
        "runtime_identity_hash_bound": True,
        "downstream_implementation_hashes_bound": True,
    }
    passed = bool(all(gates.values()))
    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": "passed" if passed else "failed_closed",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "downstream_execution_amendment": provenance.amendment_reference(
            amendment_file,
            amendment,
        ),
        "plan": _artifact(plan_file),
        "panel_manifest": _artifact(panel_path),
        "plan_sha256": plan["canonical_plan_sha256"],
        "global_execution_identity_sha256": plan[
            "global_execution_identity_sha256"
        ],
        "cpp_abi_version": CPP_EVENT_STREAM_MIRROR_ABI_VERSION,
        "cpp_module": _artifact(cpp_module),
        "counts": {
            "day_count": len(day_reports),
            "lifecycle_count": lifecycle_count,
            "event_count": event_count,
            "exact_native_lifecycle_count": exact_native_lifecycle_count,
            "native_queue_censored_lifecycle_count": censored_lifecycle_count,
        },
        "mismatch_counts": dict(sorted(mismatch_totals.items())),
        "post_terminal_violation_counts": dict(sorted(safety_totals.items())),
        "days": day_reports,
        "gates": gates,
        "formal_40day_lockstep_passed": passed,
        "scope": dict(provenance.LOCKSTEP_SCOPE),
        "permissions": (
            dict(provenance.LOCKSTEP_PERMISSIONS)
            if passed
            else {
                **dict(provenance.LOCKSTEP_PERMISSIONS),
                "cif_training": False,
            }
        ),
    }
    report["canonical_report_sha256"] = _canonical_sha256(report)
    _atomic_write_json(output_path, report)
    if not passed:
        raise RuntimeError("40-day Python/C++ event lockstep failed closed")
    return report


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--execution-amendment", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    report = run_panel_lockstep(
        plan_path=args.plan,
        amendment_path=args.execution_amendment,
        output_path=args.out,
    )
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
