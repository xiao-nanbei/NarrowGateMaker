from __future__ import annotations

import hashlib
import json
from datetime import date, timedelta
from pathlib import Path

import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_d1_support as denominator,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_sequence_support as support,
)


def _audit_row(*, eligible: bool = True) -> dict[str, object]:
    return {
        "eligible": eligible,
        "exclusion_reasons": [] if eligible else ["target_sequence_gap"],
        "target_initialized_at_start": True,
        "target_initialization_source_at_start": "snapshot",
        "target_accepted_updates": 1,
        "target_sequence_gaps": 0 if eligible else 1,
        "target_invalid_sequence_messages": 0,
        "target_message_time_reversals": 0,
    }


def _fixture() -> tuple[dict[str, object], dict[str, object]]:
    spec = json.loads(support.DEFAULT_SPEC.read_text(encoding="utf-8"))
    required_days = sorted(
        {
            day
            for target in (*denominator.PREFIX40, *denominator.ADDED10)
            for day in (
                target,
                (date.fromisoformat(target) + timedelta(days=1)).isoformat(),
            )
        }
    )
    audit: dict[str, object] = {
        "schema_version": support.UPSTREAM_SCHEMA,
        "audit_csv_sha256": "b" * 64,
        "day_audits": {day: _audit_row() for day in required_days},
    }
    audit["day_audits"]["2026-04-21"] = _audit_row(eligible=False)
    audit["day_audits"]["2026-04-24"] = _audit_row(eligible=False)
    audit["identity_sha256"] = support._upstream_identity(audit)
    return spec, audit


def test_mapping_preserves_41_days_and_locates_warmup_only_gap() -> None:
    spec, audit = _fixture()
    report = support.build_mapping(
        spec=spec,
        upstream_audit=audit,
        spec_path=Path("spec.json"),
        spec_sha256="a" * 64,
        audit_json_path=Path("audit.json"),
        audit_json_sha256="c" * 64,
        audit_csv_path=Path("audit.csv"),
        audit_csv_sha256="b" * 64,
    )

    assert report["counts"] == {
        "requested_days": 50,
        "frozen_formal_days": 41,
        "frozen_reduced_days": 9,
        "formal_sequence_supported_days": 41,
        "reduced_days_with_sequence_failure": 2,
        "reduced_days_sequence_unconfirmed": 0,
    }
    assert report["sequence_reduced_days"] == ["2026-04-20", "2026-04-23"]
    target = next(
        row for row in report["days"] if row["target_day"] == "2026-04-22"
    )
    assert target["target_sequence_eligible"] is True
    assert target["continuation_sequence_eligible"] is True
    assert report["permissions"]["economic_outcomes_read"] is False


def test_mapping_rejects_a_formal_day_sequence_failure() -> None:
    spec, audit = _fixture()
    audit["day_audits"]["2026-04-22"] = _audit_row(eligible=False)
    audit["identity_sha256"] = support._upstream_identity(audit)

    with pytest.raises(support.SequenceSupportError, match="2026-04-22"):
        support.build_mapping(
            spec=spec,
            upstream_audit=audit,
            spec_path=Path("spec.json"),
            spec_sha256="a" * 64,
            audit_json_path=Path("audit.json"),
            audit_json_sha256="c" * 64,
            audit_csv_path=Path("audit.csv"),
            audit_csv_sha256="b" * 64,
        )


def test_run_hash_binds_both_upstream_artifacts(tmp_path: Path) -> None:
    spec, audit = _fixture()
    spec_path = tmp_path / "spec.json"
    audit_path = tmp_path / "audit.json"
    csv_path = tmp_path / "audit.csv"
    output_path = tmp_path / "report.json"
    spec_path.write_text(json.dumps(spec), encoding="utf-8")
    csv_path.write_text("day,eligible\n", encoding="utf-8")

    def digest(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    audit["audit_csv_sha256"] = digest(csv_path)
    audit["identity_sha256"] = support._upstream_identity(audit)
    audit_path.write_text(json.dumps(audit), encoding="utf-8")
    report = support.run(
        spec_path=spec_path,
        audit_json_path=audit_path,
        audit_csv_path=csv_path,
        output_path=output_path,
        expected_spec_sha256=digest(spec_path),
        expected_audit_json_sha256=digest(audit_path),
        expected_audit_csv_sha256=digest(csv_path),
    )
    assert output_path.is_file()
    assert report["canonical_report_sha256"] == json.loads(
        output_path.read_text(encoding="utf-8")
    )["canonical_report_sha256"]
