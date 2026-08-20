from __future__ import annotations

import json
from pathlib import Path

import pytest

from models.audit.experiment_scorecard import ACTION_ALPHA_V1
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_formal_component_closeout_v1 as subject,
)


def _scorecard(name: str) -> dict[str, object]:
    profile_payload = ACTION_ALPHA_V1.payload()
    profile = {
        **profile_payload,
        "profile_sha256": subject.canonical_sha256(profile_payload),
        "frozen_before_outcome": True,
    }
    payload: dict[str, object] = {
        "schema_version": subject.SCORECARD_SCHEMA,
        "experiment_id": name,
        "family_id": "f05",
        "panel_role": "development",
        "profile": profile,
        "input_identity": {"candidate": name},
        "input_identity_sha256": subject.canonical_sha256({"candidate": name}),
        "scorecard_sha256": "",
    }
    payload["scorecard_sha256"] = subject.document_sha256(
        payload, "scorecard_sha256"
    )
    return payload


def _report(side: str) -> dict[str, object]:
    names = [f"{side}:candidate_{index:02d}" for index in range(13)]
    return {
        "schema_version": subject.REPORT_SCHEMA,
        "oof_evidence_scope": subject.REPORT_SCOPE,
        "exact_final_artifact_oof_available": False,
        "final_refit_performed": False,
        "candidate_reports": {name: {"candidate": name} for name in names},
        "scorecards": {name: _scorecard(name) for name in names},
        "score_profile_contract": {
            "schema_version": "narrowgate_score_profile.v1",
            "profile_id": subject.SCORE_PROFILE_ID,
            "profile_sha256": subject.canonical_sha256(ACTION_ALPHA_V1.payload()),
        },
        "outer_oof_row_count": 260,
        "outer_fold_count": 4,
        "permissions": dict(subject.EXPECTED_REPORT_PERMISSIONS),
    }


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, sort_keys=True, indent=2, allow_nan=False) + "\n",
        encoding="ascii",
    )
    path.chmod(0o600)


def _cache(tmp_path: Path, *, manifest_sha: str, side: str) -> Path:
    root = tmp_path / "cache"
    payload_path = root / "payload.bin"
    payload_path.parent.mkdir(parents=True)
    payload_path.write_bytes(b"payload")
    payload_path.chmod(0o600)
    entry_schema = "test.day_cache.v2"
    key = {
        "execution_manifest_sha256": manifest_sha,
        "side": side,
        "stage": "outer_oof",
        "fold_id": "outer_1",
        "utc_day": "2026-08-01",
    }
    key_sha = subject.canonical_sha256(
        {"schema_version": entry_schema, **key}
    )
    entry_dir = root / "entries" / key_sha
    bound_payload = entry_dir / "payload.bin"
    entry_dir.mkdir(parents=True)
    bound_payload.write_bytes(payload_path.read_bytes())
    bound_payload.chmod(0o600)
    entry: dict[str, object] = {
        "schema_version": entry_schema,
        "cache_key_sha256": key_sha,
        "cache_key": key,
        "complete": True,
        "atomic_admission": True,
        "files": {
            "payload": {
                "file": "payload.bin",
                "sha256": subject.file_sha256(bound_payload),
                "size_bytes": bound_payload.stat().st_size,
            }
        },
        "receipt_sha256": "",
    }
    entry["receipt_sha256"] = subject.document_sha256(entry, "receipt_sha256")
    _write_json(entry_dir / "manifest.json", entry)
    progress: dict[str, object] = {
        "schema_version": "test.day_progress.v2",
        "cache_key_sha256": key_sha,
        "cache_key": key,
        "state": "complete",
        "receipt_sha256": "",
    }
    progress["receipt_sha256"] = subject.document_sha256(
        progress, "receipt_sha256"
    )
    _write_json(root / "progress" / f"{key_sha}.json", progress)
    return root


def test_validate_and_publish_nested_report(tmp_path: Path) -> None:
    report = _report("BUY")

    validation = subject.validate_nested_report(report, expected_side="BUY")
    published = subject.publish_nested_report(
        report,
        expected_side="BUY",
        output_dir=tmp_path / "published",
    )

    assert validation["scorecard_count"] == 13
    assert published["artifact_count"] == 14
    assert (tmp_path / "published" / "nested_oof_report.json").is_file()
    assert (tmp_path / "published" / "manifest.json").is_file()

    drifted = json.loads(json.dumps(report))
    drifted["permissions"]["live_authorized"] = True
    with pytest.raises(subject.FormalComponentCloseoutError):
        subject.validate_nested_report(drifted, expected_side="BUY")


def test_audit_cache_validates_atomic_entry_and_payload(tmp_path: Path) -> None:
    manifest_sha = "a" * 64
    root = _cache(tmp_path, manifest_sha=manifest_sha, side="SELL")

    receipt = subject.audit_cache(
        root,
        execution_manifest_sha256=manifest_sha,
        expected_side="SELL",
        expected_stage_counts={"outer_oof": 1},
    )

    assert receipt["complete_cache_units"] == 1
    assert receipt["stage_counts"] == {"outer_oof": 1}
    assert receipt["canonical_cache_audit_sha256"] == subject.document_sha256(
        receipt, "canonical_cache_audit_sha256"
    )

    payload = next((root / "entries").glob("*/payload.bin"))
    payload.write_bytes(b"drift")
    with pytest.raises(subject.FormalComponentCloseoutError):
        subject.audit_cache(
            root,
            execution_manifest_sha256=manifest_sha,
            expected_side="SELL",
            expected_stage_counts={"outer_oof": 1},
        )


def test_preload_cpp_extension_rejects_wrong_bytes(tmp_path: Path) -> None:
    extension = tmp_path / "narrowgate_cpp.so"
    extension.write_bytes(b"not-an-extension")

    with pytest.raises(subject.FormalComponentCloseoutError):
        subject._preload_cpp_extension(extension, expected_sha256="a" * 64)


def test_compose_components_binds_without_reestimation(tmp_path: Path) -> None:
    shared = {
        "source_manifest_sha256": "1" * 64,
        "panel_manifest_sha256": "2" * 64,
        "fold_manifest_sha256": "3" * 64,
        "nested_fold_manifest_sha256": "4" * 64,
        "exact_owner_policy_sha256": "5" * 64,
        "exact_owner_predicate_bundle_sha256": "6" * 64,
        "exact_owner_private_config_sha256": "7" * 64,
    }
    buy: dict[str, object] = {
        **shared,
        "formal_sides": ["BUY"],
        "execution_manifest_sha256": "8" * 64,
        "source_execution": {
            "public_base_commit": "buy-commit",
            "annotated_tag": "buy-tag",
        },
        "nested_oof_report": _report("BUY"),
        "canonical_component_result_sha256": "",
    }
    buy["canonical_component_result_sha256"] = subject.document_sha256(
        buy, "canonical_component_result_sha256"
    )
    sell: dict[str, object] = {
        **shared,
        "formal_sides": ["SELL"],
        "execution_manifest_sha256": "9" * 64,
        "nested_oof_report": _report("SELL"),
        "canonical_result_sha256": "",
    }
    sell["canonical_result_sha256"] = subject.document_sha256(
        sell, "canonical_result_sha256"
    )
    buy_validation: dict[str, object] = {
        "formal_side": "BUY",
        "canonical_validation_receipt_sha256": "",
    }
    buy_validation["canonical_validation_receipt_sha256"] = (
        subject.document_sha256(
            buy_validation, "canonical_validation_receipt_sha256"
        )
    )
    sell_validation: dict[str, object] = {
        "formal_side": "SELL",
        "source_execution": {
            "public_base_commit": "sell-commit",
            "annotated_tag": "sell-tag",
        },
        "canonical_validation_receipt_sha256": "",
    }
    sell_validation["canonical_validation_receipt_sha256"] = (
        subject.document_sha256(
            sell_validation, "canonical_validation_receipt_sha256"
        )
    )
    paths = {
        "buy": tmp_path / "buy.json",
        "buy_validation": tmp_path / "buy_validation.json",
        "sell": tmp_path / "sell.json",
        "sell_validation": tmp_path / "sell_validation.json",
    }
    _write_json(paths["buy"], buy)
    _write_json(paths["buy_validation"], buy_validation)
    _write_json(paths["sell"], sell)
    _write_json(paths["sell_validation"], sell_validation)

    receipt = subject.compose_components(
        buy_result_path=paths["buy"],
        buy_validation_path=paths["buy_validation"],
        sell_result_path=paths["sell"],
        sell_validation_path=paths["sell_validation"],
        output_path=tmp_path / "composition.json",
    )

    assert receipt["status"] == "passed_separate_side_composition_without_reestimation"
    assert receipt["composition_semantics"]["economic_reestimation"] is False
    assert receipt["permissions"] == subject.EXPECTED_COMPONENT_PERMISSIONS
