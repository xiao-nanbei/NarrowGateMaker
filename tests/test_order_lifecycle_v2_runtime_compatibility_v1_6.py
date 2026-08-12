from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pytest

from research.families.f07_active_order_continuation.audit import (
    active_order_lifecycle_cif_100ms_training_v1_5 as frozen_training,
)
from research.families.f07_active_order_continuation.audit import (
    active_order_lifecycle_cif_100ms_training_v1_6 as successor_training,
)
from research.families.f07_active_order_continuation.audit import (
    active_order_lifecycle_cif_cpp_parity_v1_6 as successor_parity,
)
from research.families.f07_active_order_continuation.audit import (
    order_lifecycle_v2_40day_cpp_lockstep_v1_6 as lockstep,
)
from research.families.f07_active_order_continuation.audit import (
    order_lifecycle_v2_cif_successor_v1_6 as cif_successor,
)
from research.families.f07_active_order_continuation.audit import (
    order_lifecycle_v2_runtime_compatibility_v1_6 as subject,
)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _build_plan_and_attestation(tmp_path: Path) -> tuple[Path, Path]:
    predecessor = {
        "global_execution_identity": {
            "runtime_code_artifacts": [
                {
                    "logical_path": subject.SUCCESSOR_LOGICAL_PATH,
                    "path": str(subject.ROOT / subject.SUCCESSOR_LOGICAL_PATH),
                    "sha256": subject.PREDECESSOR_SHA256,
                    "size_bytes": subject.PREDECESSOR_SIZE_BYTES,
                }
            ]
        }
    }
    predecessor["global_execution_identity_sha256"] = subject.canonical_sha256(
        predecessor["global_execution_identity"]
    )
    predecessor["canonical_plan_sha256"] = subject.canonical_document_sha256(
        predecessor, "canonical_plan_sha256"
    )
    plan_path = tmp_path / "predecessor-plan.json"
    _write_json(plan_path, predecessor)
    attestation_path = tmp_path / "attestation.json"
    subject.build_source_attestation(
        predecessor_plan_path=plan_path,
        output_path=attestation_path,
    )
    return plan_path, attestation_path


def _resign(payload: dict[str, Any], field: str) -> None:
    payload[field] = subject.canonical_document_sha256(payload, field)


def _build_fingerprint_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Path, Path, dict[str, Any]]:
    old_root = tmp_path / "old"
    new_root = tmp_path / "new"
    days = [(date(2026, 1, 1) + timedelta(days=index)).isoformat() for index in range(40)]
    predecessor = {
        "cache_root": str(old_root),
        "canonical_plan_sha256": "old-plan",
        "ordered_utc_days": days,
    }
    successor = {
        "cache_root": str(new_root),
        "canonical_plan_sha256": "new-plan",
        "ordered_utc_days": days,
    }
    predecessor_path = old_root / "execution_plan.json"
    successor_path = new_root / "execution_plan.json"
    _write_json(predecessor_path, predecessor)
    _write_json(successor_path, successor)
    monkeypatch.setattr(subject, "_validate_predecessor_plan_structure", lambda _plan: {})
    monkeypatch.setattr(subject.emitter, "validate_execution_plan", lambda _plan: {})
    rows: list[dict[str, Any]] = []
    for day in days:
        old_manifest = old_root / "days" / day / "day_manifest.json"
        new_manifest = new_root / "days" / day / "day_manifest.json"
        _write_json(old_manifest, {"day": day, "root": "old"})
        _write_json(new_manifest, {"day": day, "root": "new"})
        rows.append(
            {
                "day": day,
                "predecessor_row_count": 10,
                "successor_row_count": 10,
                "predecessor_fingerprint_sha256": "1" * 64,
                "successor_fingerprint_sha256": "2" * 64,
                "predecessor_day_manifest_sha256": subject.file_sha256(old_manifest),
                "successor_day_manifest_sha256": subject.file_sha256(new_manifest),
                "physical_exact_row_match": False,
                "semantic_exact_after_plan_namespace_normalization": True,
                "first_physical_mismatch_index": 0,
                "first_semantic_mismatch_index": None,
                "identity_only_mismatch_row_count": 10,
                "unexpected_mismatch_fields": [],
                "event_ids_unique_in_each_panel": True,
                "predecessor_semantic_fingerprint_sha256": "3" * 64,
                "successor_semantic_fingerprint_sha256": "3" * 64,
            }
        )
    report: dict[str, Any] = {
        "schema_version": subject.FINGERPRINT_SCHEMA_VERSION,
        "identity": "f07_order_lifecycle_v2_panel_fingerprint_equivalence_v1",
        "status": "passed",
        "generated_at_utc": "2026-08-05T00:00:00+00:00",
        "predecessor_root": str(old_root),
        "successor_root": str(new_root),
        "predecessor_execution_plan": {
            **subject.artifact_identity(predecessor_path),
            "canonical_plan_sha256": "old-plan",
        },
        "successor_execution_plan": {
            **subject.artifact_identity(successor_path),
            "canonical_plan_sha256": "new-plan",
        },
        "ordered_utc_days": days,
        "coverage": "full_40day",
        "days": rows,
        "gates": {key: True for key in subject.FINGERPRINT_GATE_KEYS},
        "scope": dict(subject.FINGERPRINT_SCOPE),
        "permissions": dict(subject.DENIED_PERMISSIONS),
    }
    report["canonical_report_sha256"] = subject.canonical_sha256(report)
    report_path = tmp_path / "fingerprint.json"
    _write_json(report_path, report)
    return predecessor_path, successor_path, report_path, report


def test_mechanical_reconstruction_matches_frozen_predecessor() -> None:
    successor = (subject.ROOT / subject.SUCCESSOR_LOGICAL_PATH).read_bytes()
    predecessor = subject.reconstruct_predecessor(successor)
    assert len(predecessor) == subject.PREDECESSOR_SIZE_BYTES
    assert hashlib.sha256(predecessor).hexdigest() == subject.PREDECESSOR_SHA256

    semantic = subject._source_semantic_diff(predecessor, successor)
    assert semantic["passed"] is True
    assert semantic["changed_shared_definitions"] == []
    assert semantic["added_definitions"] == [
        "QuantityWeightedOrderLifecycle.journal_snapshot",
        "QuantityWeightedOrderLifecycle.latest_event",
    ]


def test_source_attestation_is_canonical_bound_and_mechanics_only(
    tmp_path: Path,
) -> None:
    plan_path, attestation_path = _build_plan_and_attestation(tmp_path)
    payload = subject.validate_source_attestation(
        attestation_path,
        predecessor_plan_path=plan_path,
    )
    assert payload["predecessor_source"]["sha256"] == subject.PREDECESSOR_SHA256
    assert payload["successor_source"]["sha256"] == subject.file_sha256(
        subject.ROOT / subject.SUCCESSOR_LOGICAL_PATH
    )
    assert payload["scope"]["economic_outcomes_read"] is False
    assert payload["permissions"] == subject.DENIED_PERMISSIONS


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (lambda payload: payload.__setitem__("hidden_outcome", 1), "schema differs"),
        (
            lambda payload: payload["permissions"].__setitem__("action", True),
            "permissions differ",
        ),
        (
            lambda payload: payload["successor_source"].__setitem__("sha256", "0" * 64),
            "SHA256 differs",
        ),
    ],
)
def test_source_attestation_rejects_schema_authority_and_source_drift(
    tmp_path: Path,
    mutation: Any,
    match: str,
) -> None:
    plan_path, attestation_path = _build_plan_and_attestation(tmp_path)
    payload = json.loads(attestation_path.read_text(encoding="utf-8"))
    mutation(payload)
    _resign(payload, "canonical_attestation_sha256")
    _write_json(attestation_path, payload)
    with pytest.raises(subject.RuntimeCompatibilityError, match=match):
        subject.validate_source_attestation(
            attestation_path,
            predecessor_plan_path=plan_path,
        )


def test_source_attestation_rejects_different_predecessor_plan_path(
    tmp_path: Path,
) -> None:
    plan_path, attestation_path = _build_plan_and_attestation(tmp_path)
    other_plan = tmp_path / "other-plan.json"
    other_plan.write_bytes(plan_path.read_bytes())
    with pytest.raises(subject.RuntimeCompatibilityError, match="path differs"):
        subject.validate_source_attestation(
            attestation_path,
            predecessor_plan_path=other_plan,
        )


def test_mechanical_reconstruction_rejects_any_helper_drift() -> None:
    successor = (subject.ROOT / subject.SUCCESSOR_LOGICAL_PATH).read_bytes()
    drifted = successor.replace(b"return self._events[-1]", b"return self._events[0]", 1)
    with pytest.raises(subject.RuntimeCompatibilityError, match="helper block"):
        subject.reconstruct_predecessor(drifted)


def test_bound_call_surface_fails_on_helper_reference(tmp_path: Path) -> None:
    clean = tmp_path / "clean.py"
    clean.write_text("value = lifecycle.events()\n", encoding="utf-8")
    assert subject._scan_helper_references([clean])["passed"] is True

    bad = tmp_path / "bad.py"
    bad.write_text("value = lifecycle.latest_event()\n", encoding="utf-8")
    report = subject._scan_helper_references([bad])
    assert report["passed"] is False
    assert report["forbidden_helper_references"][0]["symbol"] == "latest_event"


def test_deterministic_scenario_fingerprints_match() -> None:
    successor = (subject.ROOT / subject.SUCCESSOR_LOGICAL_PATH).read_bytes()
    predecessor = subject.reconstruct_predecessor(successor)
    result = subject._deterministic_behavior_equivalence(predecessor, successor)
    assert result["passed"] is True
    assert result["scenario_count"] == 4
    assert result["predecessor_fingerprint_sha256"] == result["successor_fingerprint_sha256"]


def test_panel_fingerprint_comparison_is_exact_and_marks_subset_diagnostic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_row = {
        "sequence": 1,
        "event": "submit",
        "event_id": "event-1",
        "lifecycle_id": "plan:order-1",
        "source_callback_id": "plan:order-1:00000001:order_submit",
    }
    rows = {
        ("old", "2026-01-01"): [dict(canonical_row)],
        ("new", "2026-01-01"): [dict(canonical_row)],
    }

    def fake_rows(root: Path, day: str) -> list[dict[str, object]]:
        return rows[(root.name, day)]

    monkeypatch.setattr(subject, "_day_rows", fake_rows)
    monkeypatch.setattr(subject, "_validate_predecessor_plan_structure", lambda _plan: {})
    monkeypatch.setattr(subject.emitter, "validate_execution_plan", lambda _plan: {})
    for name in ("old", "new"):
        root = tmp_path / name
        plan = {
            "cache_root": str(root),
            "canonical_plan_sha256": f"{name}-plan",
            "ordered_utc_days": ["2026-01-01"],
        }
        _write_json(root / "execution_plan.json", plan)
        _write_json(
            root / "days" / "2026-01-01" / "day_manifest.json",
            {"day": "2026-01-01", "root": name},
        )
    output = tmp_path / "report.json"
    report = subject.compare_panel_fingerprints(
        predecessor_root=tmp_path / "old",
        successor_root=tmp_path / "new",
        days=["2026-01-01"],
        output_path=output,
    )
    assert report["status"] == "passed"
    assert report["coverage"] == "diagnostic_subset"
    assert report["days"][0]["physical_exact_row_match"] is True
    assert report["days"][0]["semantic_exact_after_plan_namespace_normalization"] is True

    rows[("new", "2026-01-01")] = [{**canonical_row, "event": "activate"}]
    with pytest.raises(subject.RuntimeCompatibilityError, match="failed closed"):
        subject.compare_panel_fingerprints(
            predecessor_root=tmp_path / "old",
            successor_root=tmp_path / "new",
            days=["2026-01-01"],
            output_path=tmp_path / "failed.json",
        )


def test_fingerprint_report_requires_exact_40day_schema_and_semantics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    predecessor, successor, report_path, report = _build_fingerprint_fixture(
        tmp_path,
        monkeypatch,
    )
    validated = subject.validate_fingerprint_report(
        report_path,
        predecessor_plan_path=predecessor,
        successor_plan_path=successor,
        require_full_40day=True,
    )
    assert len(validated["days"]) == 40

    extra = deepcopy(report)
    extra["hidden_label"] = 1
    _resign(extra, "canonical_report_sha256")
    _write_json(report_path, extra)
    with pytest.raises(subject.RuntimeCompatibilityError, match="schema differs"):
        subject.validate_fingerprint_report(
            report_path,
            predecessor_plan_path=predecessor,
            successor_plan_path=successor,
            require_full_40day=True,
        )

    bad_day = deepcopy(report)
    bad_day["days"][17]["semantic_exact_after_plan_namespace_normalization"] = False
    _resign(bad_day, "canonical_report_sha256")
    _write_json(report_path, bad_day)
    with pytest.raises(subject.RuntimeCompatibilityError, match="semantic fingerprint"):
        subject.validate_fingerprint_report(
            report_path,
            predecessor_plan_path=predecessor,
            successor_plan_path=successor,
            require_full_40day=True,
        )

    missing_day = deepcopy(report)
    missing_day["ordered_utc_days"] = missing_day["ordered_utc_days"][:-1]
    missing_day["days"] = missing_day["days"][:-1]
    _resign(missing_day, "canonical_report_sha256")
    _write_json(report_path, missing_day)
    with pytest.raises(subject.RuntimeCompatibilityError, match="40-day denominator"):
        subject.validate_fingerprint_report(
            report_path,
            predecessor_plan_path=predecessor,
            successor_plan_path=successor,
            require_full_40day=True,
        )


def test_plan_namespace_rekey_is_explicitly_normalized() -> None:
    common = {
        "source_callback_type": "order_submit",
        "lifecycle_sequence": 1,
        "lifecycle_event": "submit",
    }
    old = {
        **common,
        "event_id": "old-event",
        "lifecycle_id": "old-plan:order-1",
        "source_callback_id": "old-plan:order-1:00000001:order_submit",
    }
    new = {
        **common,
        "event_id": "new-event",
        "lifecycle_id": "new-plan:order-1",
        "source_callback_id": "new-plan:order-1:00000001:order_submit",
    }
    result = subject._compare_row_streams([old], [new])
    assert result["physical_exact_row_match"] is False
    assert result["semantic_exact_after_plan_namespace_normalization"] is True
    assert result["unexpected_mismatch_fields"] == []


def test_successor_amendment_cli_requires_bound_synthetic_cancel_reject() -> None:
    parser = subject._build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "build-successor-amendment",
                "--predecessor-plan",
                "old.json",
                "--successor-plan",
                "new.json",
                "--attestation",
                "attestation.json",
                "--fingerprint-report",
                "fingerprint.json",
                "--out",
                "amendment.json",
            ]
        )
    args = parser.parse_args(
        [
            "build-successor-amendment",
            "--predecessor-plan",
            "old.json",
            "--successor-plan",
            "new.json",
            "--attestation",
            "attestation.json",
            "--fingerprint-report",
            "fingerprint.json",
            "--synthetic-cancel-reject-lockstep",
            "synthetic.json",
            "--out",
            "amendment.json",
        ]
    )
    assert str(args.synthetic_cancel_reject_lockstep) == "synthetic.json"


def test_v1_6_lockstep_requires_successor_amendment() -> None:
    args = lockstep._build_parser().parse_args(
        [
            "--plan",
            "successor-plan.json",
            "--successor-amendment",
            "successor-amendment.json",
            "--out",
            "lockstep.json",
        ]
    )
    assert str(args.successor_amendment) == "successor-amendment.json"
    assert subject.LOCKSTEP_IDENTITY.endswith("v1_6")


def test_empirical_cancel_reject_support_is_explicitly_frozen_zero(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    days = [f"2026-01-{index:02d}" for index in range(1, 41)]
    plan = {"cache_root": str(tmp_path), "ordered_utc_days": days}
    monkeypatch.setattr(
        subject.emitter,
        "validate_execution_plan",
        lambda _plan: {day: {"day": day} for day in days},
    )
    counters = {
        "cancel_reject_count": 0,
        "cancel_reject_to_active_count": 0,
        "cancel_reject_to_partially_filled_count": 0,
    }
    monkeypatch.setattr(
        subject.emitter,
        "_validate_day_manifest",
        lambda *_args, **_kwargs: {"journal_v2": {"counters": dict(counters)}},
    )
    support = subject._empirical_cancel_reject_support(plan=plan)
    assert support["empirical_cancel_reject_count"] == 0
    assert support["empirical_cancel_reject_support"] is False
    assert support["empirical_cancel_reject_transport_support"] is False
    assert support["synthetic_branch_contract_is_not_transport_support"] is True

    counters["cancel_reject_count"] = 1
    with pytest.raises(subject.RuntimeCompatibilityError, match="zero empirical"):
        subject._empirical_cancel_reject_support(plan=plan)


def test_fingerprint_cli_can_take_frozen_successor_denominator() -> None:
    args = subject._build_parser().parse_args(
        [
            "compare-fingerprints",
            "--predecessor-root",
            "old",
            "--successor-root",
            "new",
            "--successor-plan",
            "new/execution_plan.json",
            "--out",
            "comparison.json",
        ]
    )
    assert args.days is None
    assert str(args.successor_plan) == "new/execution_plan.json"


def test_v1_6_lockstep_authorizes_only_mechanics_cif_training() -> None:
    assert subject.LOCKSTEP_PERMISSIONS["cif_training"] is True
    assert all(
        value is False
        for key, value in subject.LOCKSTEP_PERMISSIONS.items()
        if key != "cif_training"
    )
    assert cif_successor.TRAINING_PERMISSIONS["cif_cpp_parity"] is True
    assert cif_successor.PARITY_PERMISSIONS == subject.DENIED_PERMISSIONS
    assert "cancel_reject_branch_present" in cif_successor._LOCKSTEP_GATE_KEYS
    assert "cancel_reject_routes_complete" in cif_successor._LOCKSTEP_GATE_KEYS


def test_v1_6_training_and_parity_clis_require_successor_amendment() -> None:
    training = successor_training._build_parser().parse_args(
        [
            "--plan",
            "plan.json",
            "--successor-amendment",
            "amendment.json",
            "--lockstep-report",
            "lockstep.json",
            "--artifact",
            "model.json",
            "--report",
            "training.json",
        ]
    )
    parity = successor_parity._build_parser().parse_args(
        [
            "--artifact",
            "model.json",
            "--training-report",
            "training.json",
            "--successor-amendment",
            "amendment.json",
            "--out",
            "parity.json",
        ]
    )
    assert str(training.successor_amendment) == "amendment.json"
    assert str(parity.successor_amendment) == "amendment.json"


def test_successor_training_context_does_not_rewrite_frozen_v1_5_identity() -> None:
    frozen_identity = frozen_training.IDENTITY
    with successor_training._successor_contract():
        assert frozen_training.IDENTITY == cif_successor.TRAINING_IDENTITY
    assert frozen_training.IDENTITY == frozen_identity
