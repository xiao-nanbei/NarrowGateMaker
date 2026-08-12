from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import narrowgate_cpp
import pytest

from research.families.f07_active_order_continuation.audit import (
    order_lifecycle_v2_cancel_reject_synthetic_lockstep_v1_6 as synthetic,
)
from research.families.f07_active_order_continuation.audit import (
    order_lifecycle_v2_cpp_event_stream_binding_v2 as binding,
)
from research.families.f07_active_order_continuation.audit import (
    order_lifecycle_v2_runtime_compatibility_v1_6 as successor,
)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _plan(tmp_path: Path) -> Path:
    global_identity = {
        "cpp_event_stream": {
            "abi_version": binding.CPP_EVENT_STREAM_MIRROR_ABI_VERSION,
            "module_artifact": successor.artifact_identity(Path(str(narrowgate_cpp.__file__))),
        }
    }
    payload: dict[str, object] = {
        "global_execution_identity": global_identity,
        "global_execution_identity_sha256": successor.canonical_sha256(global_identity),
    }
    payload["canonical_plan_sha256"] = successor.canonical_document_sha256(
        payload, "canonical_plan_sha256"
    )
    path = tmp_path / "execution_plan.json"
    _write_json(path, payload)
    return path


def _resign(payload: dict[str, object]) -> None:
    payload["canonical_report_sha256"] = successor.canonical_document_sha256(
        payload, "canonical_report_sha256"
    )


def test_synthetic_cancel_reject_lockstep_covers_both_recovery_phases(
    tmp_path: Path,
) -> None:
    plan_path = _plan(tmp_path)
    report_path = tmp_path / "synthetic-lockstep.json"
    report = synthetic.run_synthetic_lockstep(
        plan_path=plan_path,
        output_path=report_path,
    )
    validated = synthetic.validate_synthetic_lockstep_report(
        report_path,
        plan_path=plan_path,
        reproduce=True,
    )
    counts = validated["lockstep_summary"]["counts"]
    assert counts["cancel_reject_to_ACTIVE"] == 1
    assert counts["cancel_reject_to_PARTIALLY_FILLED"] == 1
    assert report["require_cancel_reject_branches"] is True
    assert report["scope"]["empirical_transport_support"] is False
    assert report["permissions"] == successor.DENIED_PERMISSIONS


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (lambda report: report.__setitem__("hidden_outcome", 1), "schema differs"),
        (
            lambda report: report["scope"].__setitem__("empirical_transport_support", True),
            "authority differs",
        ),
        (
            lambda report: report["lockstep_summary"]["counts"].__setitem__(
                "cancel_reject_to_ACTIVE", 0
            ),
            "branches differ",
        ),
    ],
)
def test_synthetic_cancel_reject_lockstep_rejects_tampering(
    tmp_path: Path,
    mutation: object,
    match: str,
) -> None:
    plan_path = _plan(tmp_path)
    report_path = tmp_path / "synthetic-lockstep.json"
    original = synthetic.run_synthetic_lockstep(
        plan_path=plan_path,
        output_path=report_path,
    )
    payload = deepcopy(original)
    mutation(payload)
    _resign(payload)
    _write_json(report_path, payload)
    with pytest.raises(successor.RuntimeCompatibilityError, match=match):
        synthetic.validate_synthetic_lockstep_report(
            report_path,
            plan_path=plan_path,
            reproduce=False,
        )


def test_synthetic_report_binds_exact_plan_and_cpp_module(tmp_path: Path) -> None:
    plan_path = _plan(tmp_path)
    report_path = tmp_path / "synthetic-lockstep.json"
    original = synthetic.run_synthetic_lockstep(
        plan_path=plan_path,
        output_path=report_path,
    )
    other_plan = tmp_path / "other-plan.json"
    other_plan.write_bytes(plan_path.read_bytes())
    with pytest.raises(successor.RuntimeCompatibilityError, match="plan path differs"):
        synthetic.validate_synthetic_lockstep_report(
            report_path,
            plan_path=other_plan,
            reproduce=False,
        )

    payload = deepcopy(original)
    payload["cpp_module"]["sha256"] = "0" * 64
    _resign(payload)
    _write_json(report_path, payload)
    with pytest.raises(successor.RuntimeCompatibilityError, match="SHA256 differs"):
        synthetic.validate_synthetic_lockstep_report(
            report_path,
            plan_path=plan_path,
            reproduce=False,
        )
