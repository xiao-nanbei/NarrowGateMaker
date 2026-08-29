from __future__ import annotations

import json
from pathlib import Path

import pytest

from research.families.f10_live_replay_attribution.audit import (
    first_add_decision_to_terminal_runner as runner,
)
from research.governance.paths import resolve_research_path

ROOT = Path(__file__).resolve().parents[1]
HISTORICAL_SPEC_PATH = (
    ROOT
    / "research"
    / "families"
    / "f10_live_replay_attribution"
    / "docs"
    / "first_add_decision_to_terminal_native_producer_v1_spec_20260729.json"
)
SPEC_PATH = (
    ROOT
    / "research"
    / "families"
    / "f10_live_replay_attribution"
    / "docs"
    / "first_add_decision_to_terminal_native_reproduction_v1_spec_20260730.json"
)


def _producer_spec() -> dict:
    return json.loads(SPEC_PATH.read_text(encoding="utf-8"))


def _complete_audit() -> dict:
    return {
        "coverage_complete": True,
        "selected_campaign_count": 4,
        "emitted_row_count": 4,
        "unique_campaign_count": 4,
        "exact_join_count": 4,
        "feature_clock_violation_count": 0,
        "open_record_count": 0,
    }


def test_native_producer_identity_is_frozen_and_fail_closed() -> None:
    historical = json.loads(HISTORICAL_SPEC_PATH.read_text(encoding="utf-8"))
    with pytest.raises((FileNotFoundError, ValueError)):
        runner.validate_producer_spec(historical)

    spec = _producer_spec()
    with pytest.raises(
        (FileNotFoundError, ValueError),
        match=(
            "baseline spec hash mismatch|"
            "normalized L2 manifest|"
            "operational config (?:is missing|hash mismatch)"
        ),
    ):
        runner.validate_producer_spec(spec)
    assert spec["permissions"]
    assert not any(spec["permissions"].values())

    drifted = json.loads(json.dumps(spec))
    drifted["permissions"]["validation_read"] = True
    with pytest.raises(ValueError, match="hash mismatch|cannot read"):
        runner.validate_producer_spec(drifted)


def test_native_producer_audit_requires_exact_complete_denominator() -> None:
    runner._validate_trace_audit(_complete_audit(), "2026-04-20")

    incomplete = _complete_audit()
    incomplete["exact_join_count"] = 3
    with pytest.raises(RuntimeError, match="denominator drifted"):
        runner._validate_trace_audit(incomplete, "2026-04-20")


def test_native_producer_day_contract_is_exactly_24a_plus_16b() -> None:
    producer = _producer_spec()
    f10_spec = json.loads(
        resolve_research_path(producer["f10_spec_identity"]["path"]).read_text(
            encoding="utf-8"
        )
    )
    grades = runner._grade_by_day(f10_spec)

    assert len(grades) == 40
    assert list(grades.values()).count("A") == 24
    assert list(grades.values()).count("B") == 16
