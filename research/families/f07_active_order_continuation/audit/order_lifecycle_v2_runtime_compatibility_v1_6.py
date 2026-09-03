#!/usr/bin/env python3
"""Attest and eliminate the F07 lifecycle-v2 mixed-runtime boundary.

The frozen v1.5 plan names the predecessor ``order_lifecycle.py`` bytes.  A
later live-journal optimization added two read-only helpers while the 40-day
emitter was still running.  The corresponding v1.6 successor bytes are kept
as a public frozen fixture because the active lifecycle runtime has continued
to evolve.  This module does not relax that plan's hash gate.  It instead:

1. reconstructs the exact predecessor from the current successor;
2. proves the source delta and bound F07 call surface are closed;
3. builds a new homogeneous successor execution plan; and
4. compares deterministic journal row fingerprints after re-emission.

No command reads PnL, markout, reward, or campaign economic outcomes.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import sys
import types
import uuid
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from research.families.f07_active_order_continuation.audit import (
    order_lifecycle_v2_40day_replay_emitter as emitter,
)

ROOT = Path(__file__).resolve().parents[4]
IDENTITY = "f07_order_lifecycle_v2_runtime_compatibility_v1_6"
ATTESTATION_SCHEMA_VERSION = "f07_order_lifecycle_v2_runtime_compatibility.v1_6"
FINGERPRINT_SCHEMA_VERSION = "f07_order_lifecycle_v2_panel_fingerprint_equivalence.v1"
SUCCESSOR_AMENDMENT_SCHEMA_VERSION = (
    "f07_order_lifecycle_v2_downstream_execution_successor_amendment.v1_6"
)
LOCKSTEP_IDENTITY = "f07_order_lifecycle_v2_40day_cpp_event_lockstep_v1_6"
LOCKSTEP_SCHEMA_VERSION = "f07_order_lifecycle_v2_40day_cpp_event_lockstep_report.v1_6"
TRAINING_IDENTITY = "active_order_lifecycle_competing_risk_cif_100ms_v1_6"
TRAINING_SCHEMA_VERSION = "active_order_lifecycle_competing_risk_cif_artifact.v1_6"
TRAINING_REPORT_SCHEMA_VERSION = "active_order_lifecycle_competing_risk_cif_training_report.v1_6"
PARITY_IDENTITY = "active_order_lifecycle_cif_cpp_inference_parity_v1_6"
PARITY_SCHEMA_VERSION = "active_order_lifecycle_cif_cpp_inference_parity_report.v1_6"

PREDECESSOR_SHA256 = "447c21ef8150891e8faffefbb16665d33cf4325e214ac1e5538b902ccf137ec8"
PREDECESSOR_SIZE_BYTES = 23_802
SUCCESSOR_LOGICAL_PATH = "execution/order_lifecycle.py"
FROZEN_SUCCESSOR_FIXTURE_PATH = (
    "research/families/f07_active_order_continuation/fixtures/"
    "order_lifecycle_v2_runtime_v1_6.py"
)
FROZEN_SUCCESSOR_SHA256 = "2981b6154f8e7e5aaa2af6c8f2e2720877f7ad214b2a2692f31f8af291496d33"
FROZEN_SUCCESSOR_SIZE_BYTES = 24_333
HELPER_NAMES = frozenset({"journal_snapshot", "latest_event"})

_COPY_IMPORT = b"from copy import copy\n"
_HELPER_BLOCK = b'''\n    def journal_snapshot(self) -> QuantityWeightedOrderLifecycle:\n        """Return an immutable-in-practice copy for the async journal worker."""\n\n        snapshot = copy(self)\n        snapshot._events = list(self._events)\n        return snapshot\n\n    def latest_event(self) -> OrderLifecycleEvent:\n        """Return the latest frozen lifecycle event without serializing history."""\n\n        if not self._events:\n            raise ValueError("order lifecycle has no events")\n        return self._events[-1]\n'''

DENIED_PERMISSIONS = {
    "economic_evaluation": False,
    "q90_action": False,
    "action": False,
    "live_transport": False,
    "live_deployment": False,
}
LOCKSTEP_PERMISSIONS = {"cif_training": True, **DENIED_PERMISSIONS}

ATTESTATION_KEYS = frozenset(
    {
        "schema_version",
        "identity",
        "status",
        "generated_at_utc",
        "predecessor_execution_plan",
        "predecessor_source",
        "successor_source",
        "mechanical_reconstruction",
        "other_bound_runtime_artifacts",
        "semantic_ast_diff",
        "bound_runtime_call_surface",
        "deterministic_behavior_equivalence",
        "gates",
        "scope",
        "permissions",
        "canonical_attestation_sha256",
    }
)
ATTESTATION_GATE_KEYS = frozenset(
    {
        "predecessor_plan_hash_bound",
        "predecessor_reconstructed_exactly",
        "other_bound_runtime_artifacts_match_plan",
        "source_delta_only_adds_frozen_helpers",
        "bound_f07_runtime_does_not_reference_helpers",
        "deterministic_lifecycle_payloads_match",
        "economic_outcomes_not_read",
    }
)
ATTESTATION_SCOPE = {
    "mechanics_only": True,
    "economic_outcomes_read": False,
    "source_compatibility_only": True,
}
FINGERPRINT_KEYS = frozenset(
    {
        "schema_version",
        "identity",
        "status",
        "generated_at_utc",
        "predecessor_root",
        "successor_root",
        "predecessor_execution_plan",
        "successor_execution_plan",
        "ordered_utc_days",
        "coverage",
        "days",
        "gates",
        "scope",
        "permissions",
        "canonical_report_sha256",
    }
)
FINGERPRINT_GATE_KEYS = frozenset(
    {
        "all_requested_days_present",
        "all_semantic_rows_exact_after_plan_namespace_normalization",
        "all_physical_differences_are_plan_namespace_only",
        "event_ids_unique_in_each_panel",
        "economic_outcomes_not_read",
    }
)
FINGERPRINT_SCOPE = {"mechanics_only": True, "economic_outcomes_read": False}
FINGERPRINT_DAY_KEYS = frozenset(
    {
        "day",
        "predecessor_row_count",
        "successor_row_count",
        "predecessor_fingerprint_sha256",
        "successor_fingerprint_sha256",
        "predecessor_day_manifest_sha256",
        "successor_day_manifest_sha256",
        "physical_exact_row_match",
        "semantic_exact_after_plan_namespace_normalization",
        "first_physical_mismatch_index",
        "first_semantic_mismatch_index",
        "identity_only_mismatch_row_count",
        "unexpected_mismatch_fields",
        "event_ids_unique_in_each_panel",
        "predecessor_semantic_fingerprint_sha256",
        "successor_semantic_fingerprint_sha256",
    }
)
SUCCESSOR_AMENDMENT_KEYS = frozenset(
    {
        "schema_version",
        "identity",
        "status",
        "generated_at_utc",
        "predecessor_execution_plan",
        "successor_execution_plan",
        "successor_panel_manifest",
        "runtime_compatibility_attestation",
        "full_40day_fingerprint_equivalence",
        "synthetic_cancel_reject_lockstep",
        "cancel_reject_support_contract",
        "successor_global_execution_identity_sha256",
        "implementation_artifacts",
        "compiled_cpp_module",
        "output_contract",
        "stage_contract",
        "scope",
        "permissions",
        "canonical_amendment_sha256",
    }
)
CANCEL_REJECT_SUPPORT_KEYS = frozenset(
    {
        "empirical_panel_day_count",
        "empirical_cancel_reject_count",
        "empirical_cancel_reject_route_count",
        "empirical_cancel_reject_support",
        "empirical_cancel_reject_transport_support",
        "synthetic_branch_contract_required",
        "synthetic_branch_contract_is_not_transport_support",
    }
)


class RuntimeCompatibilityError(RuntimeError):
    """Fail-closed source, execution, or provenance incompatibility."""


def _require_exact_keys(
    value: Mapping[str, object], expected: frozenset[str] | set[str], *, label: str
) -> None:
    observed = set(value)
    if observed != set(expected):
        missing = sorted(set(expected) - observed)
        extra = sorted(observed - set(expected))
        raise RuntimeCompatibilityError(f"{label} schema differs: missing={missing}, extra={extra}")


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def canonical_document_sha256(payload: Mapping[str, object], field: str) -> str:
    body = dict(payload)
    body.pop(field, None)
    return canonical_sha256(body)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def artifact_identity(path: Path) -> dict[str, object]:
    resolved = path.expanduser().resolve()
    return {
        "path": str(resolved),
        "size_bytes": int(resolved.stat().st_size),
        "sha256": file_sha256(resolved),
    }


def frozen_successor_path() -> Path:
    """Return the immutable v1.6 source fixture after byte-level validation."""

    path = (ROOT / FROZEN_SUCCESSOR_FIXTURE_PATH).resolve()
    if not path.is_file():
        raise RuntimeCompatibilityError(f"frozen v1.6 successor source is missing: {path}")
    if path.stat().st_size != FROZEN_SUCCESSOR_SIZE_BYTES:
        raise RuntimeCompatibilityError("frozen v1.6 successor source size differs")
    if file_sha256(path) != FROZEN_SUCCESSOR_SHA256:
        raise RuntimeCompatibilityError("frozen v1.6 successor source SHA256 differs")
    return path


def _validate_artifact(value: Mapping[str, object], *, label: str) -> Path:
    if set(value) != {"path", "size_bytes", "sha256"}:
        raise RuntimeCompatibilityError(f"{label} artifact schema differs")
    path = Path(str(value["path"])).expanduser().resolve()
    if not path.is_file():
        raise RuntimeCompatibilityError(f"{label} artifact is missing: {path}")
    if path.stat().st_size != int(value["size_bytes"]):
        raise RuntimeCompatibilityError(f"{label} artifact size differs")
    if file_sha256(path) != str(value["sha256"]):
        raise RuntimeCompatibilityError(f"{label} artifact SHA256 differs")
    return path


def _read_json(path: Path, *, label: str) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RuntimeCompatibilityError(f"invalid {label}: {resolved}: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeCompatibilityError(f"{label} must be a JSON object")
    return payload


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
    finally:
        temporary.unlink(missing_ok=True)


def reconstruct_predecessor(successor_bytes: bytes) -> bytes:
    """Remove only the frozen helper delta and reproduce the predecessor bytes."""

    if successor_bytes.count(_COPY_IMPORT) != 1:
        raise RuntimeCompatibilityError("successor copy import cardinality differs")
    if successor_bytes.count(_HELPER_BLOCK) != 1:
        raise RuntimeCompatibilityError("successor helper block cardinality differs")
    predecessor = successor_bytes.replace(_COPY_IMPORT, b"", 1).replace(_HELPER_BLOCK, b"", 1)
    if len(predecessor) != PREDECESSOR_SIZE_BYTES:
        raise RuntimeCompatibilityError("mechanically reconstructed predecessor size differs")
    if hashlib.sha256(predecessor).hexdigest() != PREDECESSOR_SHA256:
        raise RuntimeCompatibilityError("mechanically reconstructed predecessor SHA256 differs")
    return predecessor


def _function_ast_map(source: bytes) -> dict[tuple[str, ...], str]:
    tree = ast.parse(source.decode("utf-8"))
    rows: dict[tuple[str, ...], str] = {}

    def visit(body: Sequence[ast.stmt], prefix: tuple[str, ...]) -> None:
        for node in body:
            if isinstance(node, ast.ClassDef):
                visit(node.body, (*prefix, node.name))
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                key = (*prefix, node.name)
                rows[key] = ast.dump(node, include_attributes=False)

    visit(tree.body, ())
    return rows


def _source_semantic_diff(predecessor: bytes, successor: bytes) -> dict[str, object]:
    predecessor_map = _function_ast_map(predecessor)
    successor_map = _function_ast_map(successor)
    added = sorted(".".join(key) for key in successor_map.keys() - predecessor_map.keys())
    removed = sorted(".".join(key) for key in predecessor_map.keys() - successor_map.keys())
    changed = sorted(
        ".".join(key)
        for key in predecessor_map.keys() & successor_map.keys()
        if predecessor_map[key] != successor_map[key]
    )
    expected_added = sorted(f"QuantityWeightedOrderLifecycle.{name}" for name in HELPER_NAMES)
    return {
        "added_definitions": added,
        "removed_definitions": removed,
        "changed_shared_definitions": changed,
        "expected_added_definitions": expected_added,
        "passed": added == expected_added and not removed and not changed,
    }


def _scan_helper_references(paths: Sequence[Path]) -> dict[str, object]:
    references: list[dict[str, object]] = []
    for path in paths:
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        for node in ast.walk(tree):
            name = ""
            if isinstance(node, ast.Attribute):
                name = node.attr
            elif isinstance(node, ast.Name):
                name = node.id
            if name in HELPER_NAMES:
                references.append(
                    {
                        "path": str(path),
                        "line": int(getattr(node, "lineno", 0)),
                        "symbol": name,
                    }
                )
    return {
        "scanned_artifacts": [artifact_identity(path) for path in paths],
        "forbidden_helper_references": references,
        "passed": not references,
    }


def _load_lifecycle_class(source: bytes, module_name: str) -> type[Any]:
    module = types.ModuleType(module_name)
    module.__file__ = f"<{module_name}>"
    previous = sys.modules.get(module_name)
    sys.modules[module_name] = module
    try:
        exec(compile(source, module.__file__, "exec"), module.__dict__)
        return module.QuantityWeightedOrderLifecycle
    finally:
        if previous is None:
            sys.modules.pop(module_name, None)
        else:
            sys.modules[module_name] = previous


def _scenario_payloads(lifecycle_class: type[Any]) -> list[dict[str, object]]:
    second = 1_000_000_000
    outputs: list[dict[str, object]] = []

    full = lifecycle_class(0.001, second)
    full.activate(2 * second, exchange_ts_ns=1_900_000_000)
    full.observe_fill(
        remaining_after=0.0,
        visibility_ts_ns=3 * second,
        exchange_ts_ns=2_850_000_000,
        full_fill=True,
    )
    outputs.append({"scenario": "full_fill", "snapshot": full.snapshot(), "events": full.events()})

    cancel = lifecycle_class(0.001, 10 * second)
    cancel.activate(11 * second, exchange_ts_ns=10_900_000_000)
    cancel.observe_fill(
        remaining_after=0.0006,
        visibility_ts_ns=12 * second,
        exchange_ts_ns=11_900_000_000,
    )
    cancel.request_cancel(13 * second)
    cancel.observe_fill(
        remaining_after=0.0004,
        visibility_ts_ns=13_500_000_000,
        exchange_ts_ns=13_400_000_000,
    )
    cancel.cancel_rejected(14 * second, exchange_ts_ns=13_900_000_000)
    cancel.request_cancel(15 * second)
    cancel.exchange_terminal(
        16 * second,
        reason="cancel_ack",
        exchange_ts_ns=15_900_000_000,
    )
    cancel.enter_post_cancel_recovery(17 * second)
    cancel.mark_reentry_eligible(18 * second)
    outputs.append(
        {
            "scenario": "partial_cancel_reject_recovery",
            "snapshot": cancel.snapshot(),
            "events": cancel.events(),
        }
    )

    missing = lifecycle_class(0.001, 20 * second)
    missing.activate(21 * second)
    missing.request_cancel(22 * second)
    missing.exchange_terminal(23 * second, reason="cancel_ack")
    outputs.append(
        {
            "scenario": "missing_exchange_clock",
            "snapshot": missing.snapshot(),
            "events": missing.events(),
        }
    )

    rejected = lifecycle_class(0.001, 30 * second)
    rejected.exchange_terminal(31 * second, reason="rejected", exchange_ts_ns=30_900_000_000)
    outputs.append(
        {
            "scenario": "preactivation_reject",
            "snapshot": rejected.snapshot(),
            "events": rejected.events(),
        }
    )
    return outputs


def _deterministic_behavior_equivalence(predecessor: bytes, successor: bytes) -> dict[str, object]:
    predecessor_rows = _scenario_payloads(
        _load_lifecycle_class(predecessor, "_f07_order_lifecycle_predecessor")
    )
    successor_rows = _scenario_payloads(
        _load_lifecycle_class(successor, "_f07_order_lifecycle_successor")
    )
    return {
        "scenario_count": len(predecessor_rows),
        "predecessor_fingerprint_sha256": canonical_sha256(predecessor_rows),
        "successor_fingerprint_sha256": canonical_sha256(successor_rows),
        "exact_payload_match": predecessor_rows == successor_rows,
        "passed": predecessor_rows == successor_rows,
    }


def _validate_predecessor_plan_structure(plan: Mapping[str, object]) -> dict[str, object]:
    claimed = str(plan.get("canonical_plan_sha256", ""))
    if claimed != canonical_document_sha256(plan, "canonical_plan_sha256"):
        raise RuntimeCompatibilityError("predecessor plan canonical SHA256 differs")
    global_identity = plan.get("global_execution_identity")
    if not isinstance(global_identity, Mapping):
        raise RuntimeCompatibilityError("predecessor plan global identity is missing")
    if plan.get("global_execution_identity_sha256") != canonical_sha256(global_identity):
        raise RuntimeCompatibilityError("predecessor plan global identity SHA256 differs")
    rows = global_identity.get("runtime_code_artifacts")
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
        raise RuntimeCompatibilityError("predecessor runtime code artifacts are missing")
    matches = [row for row in rows if row.get("logical_path") == SUCCESSOR_LOGICAL_PATH]
    if len(matches) != 1:
        raise RuntimeCompatibilityError("predecessor lifecycle artifact cardinality differs")
    lifecycle = dict(matches[0])
    if (
        lifecycle.get("sha256") != PREDECESSOR_SHA256
        or int(lifecycle.get("size_bytes", -1)) != PREDECESSOR_SIZE_BYTES
    ):
        raise RuntimeCompatibilityError("predecessor plan binds a different lifecycle source")
    return lifecycle


def build_source_attestation(
    *, predecessor_plan_path: Path, output_path: Path
) -> dict[str, object]:
    plan_path = predecessor_plan_path.expanduser().resolve()
    plan = _read_json(plan_path, label="predecessor execution plan")
    predecessor_artifact = _validate_predecessor_plan_structure(plan)
    successor_path = frozen_successor_path()
    successor = successor_path.read_bytes()
    predecessor = reconstruct_predecessor(successor)
    semantic_diff = _source_semantic_diff(predecessor, successor)

    scan_paths: list[Path] = []
    bound_runtime_integrity: list[dict[str, object]] = []
    for row in plan["global_execution_identity"]["runtime_code_artifacts"]:
        logical_path = str(row["logical_path"])
        if logical_path == SUCCESSOR_LOGICAL_PATH:
            continue
        path = (ROOT / logical_path).resolve()
        current = artifact_identity(path)
        exact = (
            Path(str(row["path"])).expanduser().resolve() == path
            and int(row["size_bytes"]) == current["size_bytes"]
            and str(row["sha256"]) == current["sha256"]
        )
        bound_runtime_integrity.append(
            {"logical_path": logical_path, "exact_plan_byte_match": exact, **current}
        )
        if not exact:
            raise RuntimeCompatibilityError(
                f"bound runtime artifact drifted before compatibility scan: {logical_path}"
            )
        if path.suffix == ".py":
            scan_paths.append(path)
    call_surface = _scan_helper_references(scan_paths)
    behavior = _deterministic_behavior_equivalence(predecessor, successor)
    gates = {
        "predecessor_plan_hash_bound": True,
        "predecessor_reconstructed_exactly": True,
        "other_bound_runtime_artifacts_match_plan": all(
            bool(row["exact_plan_byte_match"]) for row in bound_runtime_integrity
        ),
        "source_delta_only_adds_frozen_helpers": bool(semantic_diff["passed"]),
        "bound_f07_runtime_does_not_reference_helpers": bool(call_surface["passed"]),
        "deterministic_lifecycle_payloads_match": bool(behavior["passed"]),
        "economic_outcomes_not_read": True,
    }
    payload: dict[str, object] = {
        "schema_version": ATTESTATION_SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": "passed" if all(gates.values()) else "failed_closed",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "predecessor_execution_plan": {
            **artifact_identity(plan_path),
            "canonical_plan_sha256": plan["canonical_plan_sha256"],
        },
        "predecessor_source": predecessor_artifact,
        "successor_source": artifact_identity(successor_path),
        "mechanical_reconstruction": {
            "removed_import_sha256": hashlib.sha256(_COPY_IMPORT).hexdigest(),
            "removed_helper_block_sha256": hashlib.sha256(_HELPER_BLOCK).hexdigest(),
            "removed_byte_count": len(_COPY_IMPORT) + len(_HELPER_BLOCK),
            "reconstructed_size_bytes": len(predecessor),
            "reconstructed_sha256": hashlib.sha256(predecessor).hexdigest(),
        },
        "other_bound_runtime_artifacts": bound_runtime_integrity,
        "semantic_ast_diff": semantic_diff,
        "bound_runtime_call_surface": call_surface,
        "deterministic_behavior_equivalence": behavior,
        "gates": gates,
        "scope": dict(ATTESTATION_SCOPE),
        "permissions": dict(DENIED_PERMISSIONS),
    }
    payload["canonical_attestation_sha256"] = canonical_sha256(payload)
    _atomic_write_json(output_path, payload)
    if payload["status"] != "passed":
        raise RuntimeCompatibilityError("source compatibility attestation failed closed")
    return payload


def validate_source_attestation(
    path: Path,
    *,
    predecessor_plan_path: Path,
) -> dict[str, Any]:
    payload = _read_json(path, label="runtime compatibility attestation")
    _require_exact_keys(payload, ATTESTATION_KEYS, label="runtime compatibility attestation")
    if (
        payload.get("schema_version") != ATTESTATION_SCHEMA_VERSION
        or payload.get("identity") != IDENTITY
    ):
        raise RuntimeCompatibilityError("runtime compatibility attestation identity differs")
    if payload.get("canonical_attestation_sha256") != canonical_document_sha256(
        payload, "canonical_attestation_sha256"
    ):
        raise RuntimeCompatibilityError("runtime compatibility attestation SHA256 differs")
    gates = payload.get("gates")
    if not isinstance(gates, Mapping):
        raise RuntimeCompatibilityError("runtime compatibility attestation gates are missing")
    _require_exact_keys(gates, ATTESTATION_GATE_KEYS, label="runtime compatibility gates")
    if payload.get("status") != "passed" or not all(value is True for value in gates.values()):
        raise RuntimeCompatibilityError("runtime compatibility attestation did not pass")
    if payload.get("scope") != ATTESTATION_SCOPE:
        raise RuntimeCompatibilityError("runtime compatibility attestation scope differs")
    if payload.get("permissions") != DENIED_PERMISSIONS:
        raise RuntimeCompatibilityError("runtime compatibility attestation permissions differ")

    plan_path = predecessor_plan_path.expanduser().resolve()
    reference = payload.get("predecessor_execution_plan")
    if not isinstance(reference, Mapping):
        raise RuntimeCompatibilityError("predecessor execution-plan reference is missing")
    _require_exact_keys(
        reference,
        {"path", "size_bytes", "sha256", "canonical_plan_sha256"},
        label="predecessor execution-plan reference",
    )
    plan_artifact = {key: reference[key] for key in ("path", "size_bytes", "sha256")}
    if _validate_artifact(plan_artifact, label="predecessor execution plan") != plan_path:
        raise RuntimeCompatibilityError("attestation predecessor execution-plan path differs")
    plan = _read_json(plan_path, label="predecessor execution plan")
    predecessor_source = _validate_predecessor_plan_structure(plan)
    if reference["canonical_plan_sha256"] != plan["canonical_plan_sha256"]:
        raise RuntimeCompatibilityError("attestation predecessor plan identity differs")
    if payload.get("predecessor_source") != predecessor_source:
        raise RuntimeCompatibilityError("attestation predecessor source differs from plan")

    successor_reference = payload.get("successor_source")
    if not isinstance(successor_reference, Mapping):
        raise RuntimeCompatibilityError("attestation successor source is missing")
    successor_path = _validate_artifact(successor_reference, label="successor source")
    if successor_path != frozen_successor_path():
        raise RuntimeCompatibilityError("attestation successor source path differs")
    successor_bytes = successor_path.read_bytes()
    reconstructed = reconstruct_predecessor(successor_bytes)
    if hashlib.sha256(reconstructed).hexdigest() != predecessor_source["sha256"]:
        raise RuntimeCompatibilityError("attestation successor cannot reconstruct predecessor")

    expected_other: dict[str, Mapping[str, object]] = {
        str(row["logical_path"]): row
        for row in plan["global_execution_identity"]["runtime_code_artifacts"]
        if str(row["logical_path"]) != SUCCESSOR_LOGICAL_PATH
    }
    observed_other = payload.get("other_bound_runtime_artifacts")
    if not isinstance(observed_other, list) or len(observed_other) != len(expected_other):
        raise RuntimeCompatibilityError("attestation bound runtime artifact count differs")
    observed_paths: set[str] = set()
    for row in observed_other:
        if not isinstance(row, Mapping):
            raise RuntimeCompatibilityError("attestation bound runtime artifact is invalid")
        _require_exact_keys(
            row,
            {"logical_path", "exact_plan_byte_match", "path", "size_bytes", "sha256"},
            label="attestation bound runtime artifact",
        )
        logical_path = str(row["logical_path"])
        if logical_path in observed_paths or logical_path not in expected_other:
            raise RuntimeCompatibilityError("attestation bound runtime artifact set differs")
        observed_paths.add(logical_path)
        expected = expected_other[logical_path]
        current_path = (ROOT / logical_path).resolve()
        if not bool(row["exact_plan_byte_match"]):
            raise RuntimeCompatibilityError("attestation bound runtime artifact is not exact")
        if (
            Path(str(row["path"])).expanduser().resolve() != current_path
            or Path(str(expected["path"])).expanduser().resolve() != current_path
            or int(row["size_bytes"]) != int(expected["size_bytes"])
            or str(row["sha256"]) != str(expected["sha256"])
        ):
            raise RuntimeCompatibilityError("attestation bound runtime artifact identity differs")
        _validate_artifact(
            {key: row[key] for key in ("path", "size_bytes", "sha256")},
            label=f"bound runtime artifact {logical_path}",
        )
    if observed_paths != set(expected_other):
        raise RuntimeCompatibilityError("attestation bound runtime artifact coverage differs")

    semantic = payload.get("semantic_ast_diff")
    call_surface = payload.get("bound_runtime_call_surface")
    behavior = payload.get("deterministic_behavior_equivalence")
    reconstruction = payload.get("mechanical_reconstruction")
    if not all(
        isinstance(value, Mapping) for value in (semantic, call_surface, behavior, reconstruction)
    ):
        raise RuntimeCompatibilityError("attestation source evidence is incomplete")
    if (
        not bool(semantic.get("passed"))
        or semantic.get("changed_shared_definitions") != []
        or semantic.get("removed_definitions") != []
        or set(semantic.get("added_definitions", []))
        != {f"QuantityWeightedOrderLifecycle.{name}" for name in HELPER_NAMES}
    ):
        raise RuntimeCompatibilityError("attestation semantic source delta differs")
    if (
        not bool(call_surface.get("passed"))
        or call_surface.get("forbidden_helper_references") != []
    ):
        raise RuntimeCompatibilityError("attestation bound call surface differs")
    if (
        not bool(behavior.get("passed"))
        or not bool(behavior.get("exact_payload_match"))
        or behavior.get("predecessor_fingerprint_sha256")
        != behavior.get("successor_fingerprint_sha256")
    ):
        raise RuntimeCompatibilityError("attestation deterministic behavior differs")
    if (
        int(reconstruction.get("reconstructed_size_bytes", -1)) != PREDECESSOR_SIZE_BYTES
        or reconstruction.get("reconstructed_sha256") != PREDECESSOR_SHA256
    ):
        raise RuntimeCompatibilityError("attestation mechanical reconstruction differs")
    return payload


def build_successor_plan(
    *, predecessor_plan_path: Path, attestation_path: Path, cache_root: Path
) -> dict[str, object]:
    validate_source_attestation(
        attestation_path,
        predecessor_plan_path=predecessor_plan_path,
    )
    predecessor = _read_json(predecessor_plan_path, label="predecessor execution plan")
    _validate_predecessor_plan_structure(predecessor)
    plan = json.loads(json.dumps(predecessor))
    resolved_cache = cache_root.expanduser().resolve()
    plan["cache_root"] = str(resolved_cache)
    plan["prepared_at_utc"] = datetime.now(timezone.utc).isoformat()

    current_successor_path = (ROOT / SUCCESSOR_LOGICAL_PATH).resolve()
    current_successor = artifact_identity(current_successor_path)
    if (
        current_successor["sha256"] != FROZEN_SUCCESSOR_SHA256
        or current_successor["size_bytes"] != FROZEN_SUCCESSOR_SIZE_BYTES
    ):
        raise RuntimeCompatibilityError(
            "historical v1.6 successor plan cannot be rebuilt from the evolved lifecycle runtime"
        )
    successor_artifact = current_successor
    successor_artifact["logical_path"] = SUCCESSOR_LOGICAL_PATH
    runtime_rows = plan["global_execution_identity"]["runtime_code_artifacts"]
    for index, row in enumerate(runtime_rows):
        if row["logical_path"] == SUCCESSOR_LOGICAL_PATH:
            runtime_rows[index] = successor_artifact
            break
    else:
        raise RuntimeCompatibilityError("successor lifecycle runtime row is missing")
    global_sha = canonical_sha256(plan["global_execution_identity"])
    plan["global_execution_identity_sha256"] = global_sha
    for row in plan["days"]:
        row["runtime_identity"]["global_execution_identity_sha256"] = global_sha
        row["runtime_identity_sha256"] = canonical_sha256(row["runtime_identity"])
        row["day_execution_identity_sha256"] = canonical_sha256(
            {
                "global_execution_identity_sha256": global_sha,
                "day": row["day"],
                "interval_identity_sha256": row["interval"]["interval_identity_sha256"],
                "daily_source_identity_sha256": row["daily_source_identity_sha256"],
            }
        )
    plan["canonical_plan_sha256"] = canonical_document_sha256(plan, "canonical_plan_sha256")
    emitter.validate_execution_plan(plan)
    resolved_cache.mkdir(parents=True, exist_ok=True)
    output = resolved_cache / "execution_plan.json"
    if output.exists() and _read_json(output, label="existing successor plan") != plan:
        raise FileExistsError(f"refusing to replace a different successor plan: {output}")
    _atomic_write_json(output, plan)
    return plan


def _day_rows(cache_root: Path, day: str) -> list[dict[str, object]]:
    manifest = _read_json(cache_root / "days" / day / "day_manifest.json", label=f"{day} manifest")
    session = cache_root / "days" / day / str(manifest["journal_v2"]["session_root"])
    rows, _, _ = emitter._read_journal_parts(session)
    return rows


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


_PLAN_NAMESPACE_FIELDS = frozenset({"event_id", "lifecycle_id", "source_callback_id"})


def _normalize_plan_namespace_row(row: Mapping[str, object]) -> dict[str, object]:
    normalized = dict(row)
    lifecycle_id = str(row["lifecycle_id"])
    if ":" not in lifecycle_id:
        raise RuntimeCompatibilityError("lifecycle id lacks plan namespace delimiter")
    _, lifecycle_suffix = lifecycle_id.split(":", 1)
    callback_id = str(row["source_callback_id"])
    callback_prefix = f"{lifecycle_id}:"
    if not callback_id.startswith(callback_prefix):
        raise RuntimeCompatibilityError("source callback id is outside lifecycle namespace")
    callback_suffix = callback_id[len(callback_prefix) :]
    normalized_lifecycle = f"<PLAN_NAMESPACE>:{lifecycle_suffix}"
    normalized["lifecycle_id"] = normalized_lifecycle
    normalized["source_callback_id"] = f"{normalized_lifecycle}:{callback_suffix}"
    normalized.pop("event_id", None)
    return normalized


def _compare_row_streams(
    predecessor_rows: Sequence[Mapping[str, object]],
    successor_rows: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    physical_mismatch_index: int | None = None
    semantic_mismatch_index: int | None = None
    unexpected_fields: set[str] = set()
    identity_only_mismatch_count = 0
    old_event_ids: set[str] = set()
    new_event_ids: set[str] = set()
    normalized_old: list[dict[str, object]] = []
    normalized_new: list[dict[str, object]] = []
    for index, (old, new) in enumerate(zip(predecessor_rows, successor_rows, strict=False)):
        old_event_ids.add(str(old["event_id"]))
        new_event_ids.add(str(new["event_id"]))
        differences = {key for key in old.keys() | new.keys() if old.get(key) != new.get(key)}
        if differences and physical_mismatch_index is None:
            physical_mismatch_index = index
        if differences:
            unexpected_fields.update(differences - _PLAN_NAMESPACE_FIELDS)
            if differences.issubset(_PLAN_NAMESPACE_FIELDS):
                identity_only_mismatch_count += 1
        old_normalized = _normalize_plan_namespace_row(old)
        new_normalized = _normalize_plan_namespace_row(new)
        normalized_old.append(old_normalized)
        normalized_new.append(new_normalized)
        if old_normalized != new_normalized and semantic_mismatch_index is None:
            semantic_mismatch_index = index
    row_counts_equal = len(predecessor_rows) == len(successor_rows)
    event_ids_unique = len(old_event_ids) == len(predecessor_rows) and len(new_event_ids) == len(
        successor_rows
    )
    physical_exact = row_counts_equal and physical_mismatch_index is None
    semantic_exact = (
        row_counts_equal
        and semantic_mismatch_index is None
        and not unexpected_fields
        and event_ids_unique
    )
    return {
        "physical_exact_row_match": physical_exact,
        "semantic_exact_after_plan_namespace_normalization": semantic_exact,
        "first_physical_mismatch_index": physical_mismatch_index,
        "first_semantic_mismatch_index": semantic_mismatch_index,
        "identity_only_mismatch_row_count": identity_only_mismatch_count,
        "unexpected_mismatch_fields": sorted(unexpected_fields),
        "event_ids_unique_in_each_panel": event_ids_unique,
        "predecessor_semantic_fingerprint_sha256": _rows_fingerprint(normalized_old),
        "successor_semantic_fingerprint_sha256": _rows_fingerprint(normalized_new),
    }


def compare_panel_fingerprints(
    *, predecessor_root: Path, successor_root: Path, days: Sequence[str], output_path: Path
) -> dict[str, object]:
    old_root = predecessor_root.expanduser().resolve()
    new_root = successor_root.expanduser().resolve()
    predecessor_plan_path = old_root / "execution_plan.json"
    successor_plan_path = new_root / "execution_plan.json"
    predecessor_plan = _read_json(predecessor_plan_path, label="predecessor plan")
    successor_plan = _read_json(successor_plan_path, label="successor plan")
    _validate_predecessor_plan_structure(predecessor_plan)
    emitter.validate_execution_plan(successor_plan)
    if Path(str(predecessor_plan["cache_root"])).expanduser().resolve() != old_root:
        raise RuntimeCompatibilityError("predecessor fingerprint root differs from its plan")
    if Path(str(successor_plan["cache_root"])).expanduser().resolve() != new_root:
        raise RuntimeCompatibilityError("successor fingerprint root differs from its plan")
    frozen_days = list(map(str, successor_plan["ordered_utc_days"]))
    if any(day not in frozen_days for day in days):
        raise RuntimeCompatibilityError("fingerprint day is outside the successor plan")
    if list(days) != [day for day in frozen_days if day in set(days)]:
        raise RuntimeCompatibilityError("fingerprint days do not preserve frozen order")
    reports: list[dict[str, object]] = []
    for day in days:
        predecessor_manifest = old_root / "days" / day / "day_manifest.json"
        successor_manifest = new_root / "days" / day / "day_manifest.json"
        predecessor_rows = _day_rows(old_root, day)
        successor_rows = _day_rows(new_root, day)
        comparison = _compare_row_streams(predecessor_rows, successor_rows)
        reports.append(
            {
                "day": day,
                "predecessor_row_count": len(predecessor_rows),
                "successor_row_count": len(successor_rows),
                "predecessor_fingerprint_sha256": _rows_fingerprint(predecessor_rows),
                "successor_fingerprint_sha256": _rows_fingerprint(successor_rows),
                "predecessor_day_manifest_sha256": file_sha256(predecessor_manifest),
                "successor_day_manifest_sha256": file_sha256(successor_manifest),
                **comparison,
            }
        )
    full_denominator = len(days) == 40 and list(days) == sorted(set(days))
    gates = {
        "all_requested_days_present": len(reports) == len(days),
        "all_semantic_rows_exact_after_plan_namespace_normalization": all(
            bool(row["semantic_exact_after_plan_namespace_normalization"]) for row in reports
        ),
        "all_physical_differences_are_plan_namespace_only": all(
            not row["unexpected_mismatch_fields"] for row in reports
        ),
        "event_ids_unique_in_each_panel": all(
            bool(row["event_ids_unique_in_each_panel"]) for row in reports
        ),
        "economic_outcomes_not_read": True,
    }
    payload: dict[str, object] = {
        "schema_version": FINGERPRINT_SCHEMA_VERSION,
        "identity": "f07_order_lifecycle_v2_panel_fingerprint_equivalence_v1",
        "status": "passed" if all(gates.values()) else "failed_closed",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "predecessor_root": str(old_root),
        "successor_root": str(new_root),
        "predecessor_execution_plan": {
            **artifact_identity(predecessor_plan_path),
            "canonical_plan_sha256": predecessor_plan["canonical_plan_sha256"],
        },
        "successor_execution_plan": {
            **artifact_identity(successor_plan_path),
            "canonical_plan_sha256": successor_plan["canonical_plan_sha256"],
        },
        "ordered_utc_days": list(days),
        "coverage": "full_40day" if full_denominator else "diagnostic_subset",
        "days": reports,
        "gates": gates,
        "scope": dict(FINGERPRINT_SCOPE),
        "permissions": dict(DENIED_PERMISSIONS),
    }
    payload["canonical_report_sha256"] = canonical_sha256(payload)
    _atomic_write_json(output_path, payload)
    if payload["status"] != "passed":
        raise RuntimeCompatibilityError("journal fingerprint comparison failed closed")
    return payload


def validate_fingerprint_report(
    path: Path,
    *,
    predecessor_plan_path: Path,
    successor_plan_path: Path,
    require_full_40day: bool,
) -> dict[str, Any]:
    report = _read_json(path, label="fingerprint report")
    _require_exact_keys(report, FINGERPRINT_KEYS, label="fingerprint report")
    if (
        report.get("schema_version") != FINGERPRINT_SCHEMA_VERSION
        or report.get("identity") != "f07_order_lifecycle_v2_panel_fingerprint_equivalence_v1"
        or report.get("status") != "passed"
    ):
        raise RuntimeCompatibilityError("fingerprint report identity or status differs")
    if report.get("canonical_report_sha256") != canonical_document_sha256(
        report, "canonical_report_sha256"
    ):
        raise RuntimeCompatibilityError("fingerprint report canonical SHA256 differs")
    gates = report.get("gates")
    if not isinstance(gates, Mapping):
        raise RuntimeCompatibilityError("fingerprint report gates are missing")
    _require_exact_keys(gates, FINGERPRINT_GATE_KEYS, label="fingerprint report gates")
    if not all(value is True for value in gates.values()):
        raise RuntimeCompatibilityError("fingerprint report gates did not all pass")
    if report.get("scope") != FINGERPRINT_SCOPE:
        raise RuntimeCompatibilityError("fingerprint report scope differs")
    if report.get("permissions") != DENIED_PERMISSIONS:
        raise RuntimeCompatibilityError("fingerprint report permissions differ")

    predecessor_path = predecessor_plan_path.expanduser().resolve()
    successor_path = successor_plan_path.expanduser().resolve()
    predecessor = _read_json(predecessor_path, label="predecessor plan")
    _validate_predecessor_plan_structure(predecessor)
    successor = _read_json(successor_path, label="successor plan")
    emitter.validate_execution_plan(successor)
    for key, plan_path, plan in (
        ("predecessor_execution_plan", predecessor_path, predecessor),
        ("successor_execution_plan", successor_path, successor),
    ):
        reference = report.get(key)
        if not isinstance(reference, Mapping):
            raise RuntimeCompatibilityError(f"fingerprint report {key} is missing")
        _require_exact_keys(
            reference,
            {"path", "size_bytes", "sha256", "canonical_plan_sha256"},
            label=f"fingerprint report {key}",
        )
        artifact = {name: reference[name] for name in ("path", "size_bytes", "sha256")}
        if _validate_artifact(artifact, label=key) != plan_path:
            raise RuntimeCompatibilityError(f"fingerprint report {key} path differs")
        if reference["canonical_plan_sha256"] != plan["canonical_plan_sha256"]:
            raise RuntimeCompatibilityError(f"fingerprint report {key} identity differs")

    predecessor_root = Path(str(predecessor["cache_root"])).expanduser().resolve()
    successor_root = Path(str(successor["cache_root"])).expanduser().resolve()
    if (
        Path(str(report.get("predecessor_root", ""))).expanduser().resolve() != predecessor_root
        or Path(str(report.get("successor_root", ""))).expanduser().resolve() != successor_root
    ):
        raise RuntimeCompatibilityError("fingerprint report roots differ from bound plans")

    frozen_days = list(map(str, successor.get("ordered_utc_days", [])))
    report_days = list(map(str, report.get("ordered_utc_days", [])))
    expected_coverage = "full_40day" if require_full_40day else report.get("coverage")
    if require_full_40day:
        if len(frozen_days) != 40 or report_days != frozen_days:
            raise RuntimeCompatibilityError("fingerprint report 40-day denominator differs")
    elif report_days != [day for day in frozen_days if day in set(report_days)]:
        raise RuntimeCompatibilityError("fingerprint report day order differs")
    if report.get("coverage") != expected_coverage:
        raise RuntimeCompatibilityError("fingerprint report coverage differs")

    day_rows = report.get("days")
    if not isinstance(day_rows, list) or len(day_rows) != len(report_days):
        raise RuntimeCompatibilityError("fingerprint report day rows are incomplete")
    if len({str(row.get("day")) for row in day_rows if isinstance(row, Mapping)}) != len(day_rows):
        raise RuntimeCompatibilityError("fingerprint report days are duplicated")
    for expected_day, row in zip(report_days, day_rows, strict=True):
        if not isinstance(row, Mapping):
            raise RuntimeCompatibilityError("fingerprint report day row is invalid")
        _require_exact_keys(row, FINGERPRINT_DAY_KEYS, label="fingerprint day row")
        if str(row["day"]) != expected_day:
            raise RuntimeCompatibilityError("fingerprint report day order differs")
        if (
            int(row["predecessor_row_count"]) != int(row["successor_row_count"])
            or int(row["predecessor_row_count"]) <= 0
            or row["semantic_exact_after_plan_namespace_normalization"] is not True
            or row["first_semantic_mismatch_index"] is not None
            or row["unexpected_mismatch_fields"] != []
            or row["event_ids_unique_in_each_panel"] is not True
            or row["predecessor_semantic_fingerprint_sha256"]
            != row["successor_semantic_fingerprint_sha256"]
        ):
            raise RuntimeCompatibilityError(f"{expected_day}: semantic fingerprint differs")
        predecessor_manifest = predecessor_root / "days" / expected_day / "day_manifest.json"
        successor_manifest = successor_root / "days" / expected_day / "day_manifest.json"
        if file_sha256(predecessor_manifest) != row["predecessor_day_manifest_sha256"]:
            raise RuntimeCompatibilityError(f"{expected_day}: predecessor manifest changed")
        if file_sha256(successor_manifest) != row["successor_day_manifest_sha256"]:
            raise RuntimeCompatibilityError(f"{expected_day}: successor manifest changed")
    return report


def _empirical_cancel_reject_support(
    *,
    plan: Mapping[str, object],
) -> dict[str, object]:
    by_day = emitter.validate_execution_plan(plan)
    cache_root = Path(str(plan["cache_root"])).expanduser().resolve()
    days = list(map(str, plan["ordered_utc_days"]))
    if len(days) != 40 or len(set(days)) != 40:
        raise RuntimeCompatibilityError(
            "cancel-reject empirical support requires the frozen 40-day denominator"
        )
    cancel_reject_count = 0
    route_count = 0
    for day in days:
        manifest = emitter._validate_day_manifest(
            cache_root / "days" / day / "day_manifest.json",
            plan=plan,
            day_row=by_day[day],
        )
        counters = manifest["journal_v2"]["counters"]
        cancel_reject_count += int(counters["cancel_reject_count"])
        route_count += int(counters["cancel_reject_to_active_count"])
        route_count += int(counters["cancel_reject_to_partially_filled_count"])
    if cancel_reject_count != 0 or route_count != 0:
        raise RuntimeCompatibilityError(
            "v1.6 successor identity expects zero empirical cancel-reject support"
        )
    return {
        "empirical_panel_day_count": len(days),
        "empirical_cancel_reject_count": cancel_reject_count,
        "empirical_cancel_reject_route_count": route_count,
        "empirical_cancel_reject_support": False,
        "empirical_cancel_reject_transport_support": False,
        "synthetic_branch_contract_required": True,
        "synthetic_branch_contract_is_not_transport_support": True,
    }


def build_successor_amendment(
    *,
    predecessor_plan_path: Path,
    successor_plan_path: Path,
    attestation_path: Path,
    fingerprint_report_path: Path,
    synthetic_cancel_reject_report_path: Path,
    output_path: Path,
) -> dict[str, object]:
    predecessor = _read_json(predecessor_plan_path, label="predecessor plan")
    _validate_predecessor_plan_structure(predecessor)
    attestation = validate_source_attestation(
        attestation_path,
        predecessor_plan_path=predecessor_plan_path,
    )
    successor = _read_json(successor_plan_path, label="successor plan")
    emitter.validate_execution_plan(successor)
    report = validate_fingerprint_report(
        fingerprint_report_path,
        predecessor_plan_path=predecessor_plan_path,
        successor_plan_path=successor_plan_path,
        require_full_40day=True,
    )
    from research.families.f07_active_order_continuation.audit import (
        order_lifecycle_v2_cancel_reject_synthetic_lockstep_v1_6 as synthetic,
    )

    synthetic_report = synthetic.validate_synthetic_lockstep_report(
        synthetic_cancel_reject_report_path,
        plan_path=successor_plan_path,
        reproduce=True,
    )
    support_contract = _empirical_cancel_reject_support(plan=successor)
    panel_path = Path(str(successor["cache_root"])) / "panel_manifest.json"
    from research.families.f07_active_order_continuation.audit import (
        order_lifecycle_v2_downstream_execution_amendment_v1_5 as frozen_v1_5,
    )

    frozen_v1_5.validate_panel_manifest_strict(panel_path, plan=successor)
    payload: dict[str, object] = {
        "schema_version": SUCCESSOR_AMENDMENT_SCHEMA_VERSION,
        "identity": "f07_order_lifecycle_v2_downstream_execution_successor_amendment_v1_6",
        "status": "frozen_homogeneous_successor_mechanics_only",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "predecessor_execution_plan": artifact_identity(predecessor_plan_path),
        "successor_execution_plan": artifact_identity(successor_plan_path),
        "successor_panel_manifest": artifact_identity(panel_path),
        "runtime_compatibility_attestation": {
            **artifact_identity(attestation_path),
            "canonical_attestation_sha256": attestation["canonical_attestation_sha256"],
        },
        "full_40day_fingerprint_equivalence": {
            **artifact_identity(fingerprint_report_path),
            "canonical_report_sha256": report["canonical_report_sha256"],
        },
        "synthetic_cancel_reject_lockstep": {
            **artifact_identity(synthetic_cancel_reject_report_path),
            "canonical_report_sha256": synthetic_report["canonical_report_sha256"],
        },
        "cancel_reject_support_contract": support_contract,
        "successor_global_execution_identity_sha256": successor["global_execution_identity_sha256"],
        "implementation_artifacts": {
            "compatibility_builder_validator": artifact_identity(Path(__file__)),
            "lockstep_wrapper": artifact_identity(
                ROOT / "research/families/f07_active_order_continuation/audit/"
                "order_lifecycle_v2_40day_cpp_lockstep_v1_6.py"
            ),
            "synthetic_cancel_reject_lockstep": artifact_identity(
                ROOT / "research/families/f07_active_order_continuation/audit/"
                "order_lifecycle_v2_cancel_reject_synthetic_lockstep_v1_6.py"
            ),
            "cpp_event_stream_binding": artifact_identity(
                ROOT / "research/families/f07_active_order_continuation/audit/"
                "order_lifecycle_v2_cpp_event_stream_binding_v2.py"
            ),
            "post_terminal_safety_audit": artifact_identity(
                ROOT / "research/families/f07_active_order_continuation/audit/"
                "order_lifecycle_v2_40day_cpp_lockstep.py"
            ),
            "cif_successor_provenance": artifact_identity(
                ROOT / "research/families/f07_active_order_continuation/audit/"
                "order_lifecycle_v2_cif_successor_v1_6.py"
            ),
            "cif_provenance_core": artifact_identity(
                ROOT / "research/families/f07_active_order_continuation/audit/"
                "order_lifecycle_v2_downstream_execution_amendment_v1_5.py"
            ),
            "cif_training_wrapper": artifact_identity(
                ROOT / "research/families/f07_active_order_continuation/audit/"
                "active_order_lifecycle_cif_100ms_training_v1_6.py"
            ),
            "cif_training_core": artifact_identity(
                ROOT / "research/families/f07_active_order_continuation/audit/"
                "active_order_lifecycle_cif_100ms_training_v1_5.py"
            ),
            "cif_parity_wrapper": artifact_identity(
                ROOT / "research/families/f07_active_order_continuation/audit/"
                "active_order_lifecycle_cif_cpp_parity_v1_6.py"
            ),
            "cif_parity_core": artifact_identity(
                ROOT / "research/families/f07_active_order_continuation/audit/"
                "active_order_lifecycle_cif_cpp_parity_v1_5.py"
            ),
            "cif_python_inference": artifact_identity(
                ROOT / "research/families/f07_active_order_continuation/audit/"
                "active_order_competing_risk_cif_inference_v1_1.py"
            ),
            "cif_python_base": artifact_identity(
                ROOT / "research/families/f07_active_order_continuation/audit/"
                "active_order_competing_risk_cif.py"
            ),
            "cif_cpp_source": artifact_identity(
                ROOT / "research/families/f07_active_order_continuation/cpp/"
                "active_order_competing_risk_cif.cpp"
            ),
            "cif_cpp_header": artifact_identity(
                ROOT / "research/families/f07_active_order_continuation/cpp/"
                "active_order_competing_risk_cif.hpp"
            ),
            "cpp_pybind_source": artifact_identity(
                ROOT / "cpp/narrowgate_cpp/bindings_research.cpp"
            ),
            "cpp_build_contract": artifact_identity(ROOT / "cpp/CMakeLists.txt"),
        },
        "compiled_cpp_module": dict(
            successor["global_execution_identity"]["cpp_event_stream"]["module_artifact"]
        ),
        "output_contract": {
            "lockstep_identity": LOCKSTEP_IDENTITY,
            "lockstep_schema_version": LOCKSTEP_SCHEMA_VERSION,
            "training_identity": TRAINING_IDENTITY,
            "training_schema_version": TRAINING_SCHEMA_VERSION,
            "training_report_schema_version": TRAINING_REPORT_SCHEMA_VERSION,
            "parity_identity": PARITY_IDENTITY,
            "parity_schema_version": PARITY_SCHEMA_VERSION,
            "synthetic_cancel_reject_identity": synthetic.IDENTITY,
            "synthetic_cancel_reject_schema_version": synthetic.SCHEMA_VERSION,
        },
        "stage_contract": {
            "homogeneous_successor_panel_required": True,
            "full_40day_exact_fingerprint_equivalence_required": True,
            "old_mixed_panel_not_admitted_for_lockstep": True,
            "empirical_cancel_reject_support_required": False,
            "synthetic_cancel_reject_lockstep_required": True,
            "synthetic_cancel_reject_is_not_transport_support": True,
            "next_stage": "formal_40day_cpp_event_lockstep_v1_6",
        },
        "scope": {"mechanics_only": True, "economic_outcomes_read": False},
        "permissions": dict(DENIED_PERMISSIONS),
    }
    payload["canonical_amendment_sha256"] = canonical_sha256(payload)
    _atomic_write_json(output_path, payload)
    return payload


def validate_successor_amendment(
    amendment_path: Path,
    *,
    successor_plan_path: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = _read_json(amendment_path, label="successor amendment")
    _require_exact_keys(payload, SUCCESSOR_AMENDMENT_KEYS, label="successor amendment")
    if (
        payload.get("schema_version") != SUCCESSOR_AMENDMENT_SCHEMA_VERSION
        or payload.get("identity")
        != "f07_order_lifecycle_v2_downstream_execution_successor_amendment_v1_6"
        or payload.get("status") != "frozen_homogeneous_successor_mechanics_only"
    ):
        raise RuntimeCompatibilityError("successor amendment identity differs")
    if payload.get("canonical_amendment_sha256") != canonical_document_sha256(
        payload, "canonical_amendment_sha256"
    ):
        raise RuntimeCompatibilityError("successor amendment canonical SHA256 differs")
    if payload.get("scope") != {"mechanics_only": True, "economic_outcomes_read": False}:
        raise RuntimeCompatibilityError("successor amendment scope differs")
    if payload.get("permissions") != DENIED_PERMISSIONS:
        raise RuntimeCompatibilityError("successor amendment permissions differ")

    predecessor_plan_path = _validate_artifact(
        payload["predecessor_execution_plan"], label="predecessor execution plan"
    )
    predecessor_plan = _read_json(predecessor_plan_path, label="predecessor execution plan")
    _validate_predecessor_plan_structure(predecessor_plan)

    plan_path = _validate_artifact(
        payload["successor_execution_plan"], label="successor execution plan"
    )
    if plan_path != successor_plan_path.expanduser().resolve():
        raise RuntimeCompatibilityError("successor amendment plan path differs")
    plan = _read_json(plan_path, label="successor execution plan")
    emitter.validate_execution_plan(plan)
    if (
        payload.get("successor_global_execution_identity_sha256")
        != plan["global_execution_identity_sha256"]
    ):
        raise RuntimeCompatibilityError("successor amendment runtime identity differs")

    attestation_reference = payload["runtime_compatibility_attestation"]
    if not isinstance(attestation_reference, Mapping):
        raise RuntimeCompatibilityError("successor attestation reference is missing")
    _require_exact_keys(
        attestation_reference,
        {"path", "size_bytes", "sha256", "canonical_attestation_sha256"},
        label="successor attestation reference",
    )
    attestation_path = _validate_artifact(
        {key: attestation_reference[key] for key in ("path", "size_bytes", "sha256")},
        label="runtime compatibility attestation",
    )
    attestation = validate_source_attestation(
        attestation_path,
        predecessor_plan_path=predecessor_plan_path,
    )
    if (
        payload["runtime_compatibility_attestation"].get("canonical_attestation_sha256")
        != attestation["canonical_attestation_sha256"]
    ):
        raise RuntimeCompatibilityError("successor amendment attestation identity differs")
    fingerprint_reference = payload["full_40day_fingerprint_equivalence"]
    if not isinstance(fingerprint_reference, Mapping):
        raise RuntimeCompatibilityError("successor fingerprint reference is missing")
    _require_exact_keys(
        fingerprint_reference,
        {"path", "size_bytes", "sha256", "canonical_report_sha256"},
        label="successor fingerprint reference",
    )
    report_path = _validate_artifact(
        {key: fingerprint_reference[key] for key in ("path", "size_bytes", "sha256")},
        label="full 40-day fingerprint equivalence",
    )
    report = validate_fingerprint_report(
        report_path,
        predecessor_plan_path=predecessor_plan_path,
        successor_plan_path=plan_path,
        require_full_40day=True,
    )
    if (
        report.get("canonical_report_sha256")
        != payload["full_40day_fingerprint_equivalence"].get("canonical_report_sha256")
        or report.get("coverage") != "full_40day"
    ):
        raise RuntimeCompatibilityError("full 40-day fingerprint evidence differs")
    from research.families.f07_active_order_continuation.audit import (
        order_lifecycle_v2_cancel_reject_synthetic_lockstep_v1_6 as synthetic,
    )

    synthetic_reference = payload["synthetic_cancel_reject_lockstep"]
    if not isinstance(synthetic_reference, Mapping):
        raise RuntimeCompatibilityError("successor synthetic cancel-reject reference is missing")
    _require_exact_keys(
        synthetic_reference,
        {"path", "size_bytes", "sha256", "canonical_report_sha256"},
        label="successor synthetic cancel-reject reference",
    )
    synthetic_path = _validate_artifact(
        {key: synthetic_reference[key] for key in ("path", "size_bytes", "sha256")},
        label="synthetic cancel-reject lockstep",
    )
    synthetic_report = synthetic.validate_synthetic_lockstep_report(
        synthetic_path,
        plan_path=plan_path,
        reproduce=True,
    )
    if (
        payload["synthetic_cancel_reject_lockstep"].get("canonical_report_sha256")
        != synthetic_report["canonical_report_sha256"]
    ):
        raise RuntimeCompatibilityError("synthetic cancel-reject report identity differs")
    support_contract = _empirical_cancel_reject_support(plan=plan)
    observed_support = payload.get("cancel_reject_support_contract")
    if not isinstance(observed_support, Mapping):
        raise RuntimeCompatibilityError("cancel-reject support contract is missing")
    _require_exact_keys(
        observed_support,
        CANCEL_REJECT_SUPPORT_KEYS,
        label="cancel-reject support contract",
    )
    if observed_support != support_contract:
        raise RuntimeCompatibilityError("cancel-reject support contract differs")
    panel_path = _validate_artifact(
        payload["successor_panel_manifest"], label="successor panel manifest"
    )
    from research.families.f07_active_order_continuation.audit import (
        order_lifecycle_v2_downstream_execution_amendment_v1_5 as frozen_v1_5,
    )

    frozen_v1_5.validate_panel_manifest_strict(panel_path, plan=plan)
    implementation_artifacts = payload.get("implementation_artifacts", {})
    if set(implementation_artifacts) != {
        "compatibility_builder_validator",
        "lockstep_wrapper",
        "synthetic_cancel_reject_lockstep",
        "cpp_event_stream_binding",
        "post_terminal_safety_audit",
        "cif_successor_provenance",
        "cif_provenance_core",
        "cif_training_wrapper",
        "cif_training_core",
        "cif_parity_wrapper",
        "cif_parity_core",
        "cif_python_inference",
        "cif_python_base",
        "cif_cpp_source",
        "cif_cpp_header",
        "cpp_pybind_source",
        "cpp_build_contract",
    }:
        raise RuntimeCompatibilityError("successor implementation artifact roles differ")
    for label, artifact in implementation_artifacts.items():
        _validate_artifact(artifact, label=str(label))
    _validate_artifact(payload["compiled_cpp_module"], label="compiled C++ module")
    if payload.get("output_contract") != {
        "lockstep_identity": LOCKSTEP_IDENTITY,
        "lockstep_schema_version": LOCKSTEP_SCHEMA_VERSION,
        "training_identity": TRAINING_IDENTITY,
        "training_schema_version": TRAINING_SCHEMA_VERSION,
        "training_report_schema_version": TRAINING_REPORT_SCHEMA_VERSION,
        "parity_identity": PARITY_IDENTITY,
        "parity_schema_version": PARITY_SCHEMA_VERSION,
        "synthetic_cancel_reject_identity": synthetic.IDENTITY,
        "synthetic_cancel_reject_schema_version": synthetic.SCHEMA_VERSION,
    }:
        raise RuntimeCompatibilityError("successor lockstep output contract differs")
    if payload.get("stage_contract") != {
        "homogeneous_successor_panel_required": True,
        "full_40day_exact_fingerprint_equivalence_required": True,
        "old_mixed_panel_not_admitted_for_lockstep": True,
        "empirical_cancel_reject_support_required": False,
        "synthetic_cancel_reject_lockstep_required": True,
        "synthetic_cancel_reject_is_not_transport_support": True,
        "next_stage": "formal_40day_cpp_event_lockstep_v1_6",
    }:
        raise RuntimeCompatibilityError("successor stage contract differs")
    return payload, plan


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    attest = subparsers.add_parser("attest-source")
    attest.add_argument("--predecessor-plan", type=Path, required=True)
    attest.add_argument("--out", type=Path, required=True)
    prepare = subparsers.add_parser("prepare-successor-plan")
    prepare.add_argument("--predecessor-plan", type=Path, required=True)
    prepare.add_argument("--attestation", type=Path, required=True)
    prepare.add_argument("--cache-root", type=Path, required=True)
    compare = subparsers.add_parser("compare-fingerprints")
    compare.add_argument("--predecessor-root", type=Path, required=True)
    compare.add_argument("--successor-root", type=Path, required=True)
    comparison_days = compare.add_mutually_exclusive_group(required=True)
    comparison_days.add_argument("--days", nargs="+")
    comparison_days.add_argument("--successor-plan", type=Path)
    compare.add_argument("--out", type=Path, required=True)
    amendment = subparsers.add_parser("build-successor-amendment")
    amendment.add_argument("--predecessor-plan", type=Path, required=True)
    amendment.add_argument("--successor-plan", type=Path, required=True)
    amendment.add_argument("--attestation", type=Path, required=True)
    amendment.add_argument("--fingerprint-report", type=Path, required=True)
    amendment.add_argument(
        "--synthetic-cancel-reject-lockstep",
        type=Path,
        required=True,
    )
    amendment.add_argument("--out", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "attest-source":
        result = build_source_attestation(
            predecessor_plan_path=args.predecessor_plan,
            output_path=args.out,
        )
    elif args.command == "prepare-successor-plan":
        result = build_successor_plan(
            predecessor_plan_path=args.predecessor_plan,
            attestation_path=args.attestation,
            cache_root=args.cache_root,
        )
    elif args.command == "compare-fingerprints":
        if args.successor_plan is not None:
            comparison_plan = _read_json(args.successor_plan, label="successor plan")
            emitter.validate_execution_plan(comparison_plan)
            if Path(str(comparison_plan["cache_root"])).expanduser().resolve() != (
                args.successor_root.expanduser().resolve()
            ):
                raise RuntimeCompatibilityError(
                    "fingerprint successor root differs from successor plan"
                )
            days = list(map(str, comparison_plan["ordered_utc_days"]))
        else:
            days = list(map(str, args.days))
        result = compare_panel_fingerprints(
            predecessor_root=args.predecessor_root,
            successor_root=args.successor_root,
            days=days,
            output_path=args.out,
        )
    else:
        result = build_successor_amendment(
            predecessor_plan_path=args.predecessor_plan,
            successor_plan_path=args.successor_plan,
            attestation_path=args.attestation,
            fingerprint_report_path=args.fingerprint_report,
            synthetic_cancel_reject_report_path=args.synthetic_cancel_reject_lockstep,
            output_path=args.out,
        )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
