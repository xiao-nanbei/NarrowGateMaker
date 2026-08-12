from __future__ import annotations

import copy
import json
from pathlib import Path

import pandas as pd
import pytest

from data_paths import relocate_marketdata_path
from research.families.f10_live_replay_attribution.audit import (
    first_opener_decision_to_terminal_runner as runner,
)
from research.governance.paths import resolve_research_path

ROOT = Path(__file__).resolve().parents[1]
SPEC_PATH = (
    ROOT
    / "research"
    / "families"
    / "f10_live_replay_attribution"
    / "docs"
    / "first_opener_decision_to_terminal_native_producer_v3_spec_20260730.json"
)


def _producer_spec() -> dict:
    return json.loads(SPEC_PATH.read_text(encoding="utf-8"))


def _complete_audit() -> dict:
    return {
        "coverage_complete": True,
        "campaign_count": 4,
        "eligible_true_opener_campaign_count": 4,
        "true_opener_campaign_coverage": 1.0,
        "unsupported_nonopener_open_campaign_count": 0,
        "unsupported_nonopener_open_campaigns": [],
        "selected_campaign_count": 4,
        "emitted_row_count": 4,
        "unique_campaign_count": 4,
        "exact_join_count": 4,
        "feature_clock_violation_count": 0,
        "open_record_count": 0,
    }


def test_opener_native_producer_identity_and_profile_are_frozen() -> None:
    spec = _producer_spec()
    with pytest.raises(ValueError, match="normalized L2 manifest identity drifted"):
        runner.validate_producer_spec(spec)

    assert runner.IDENTITY == "first_opener_decision_to_terminal_native_producer_v3"
    assert runner.SCHEMA_VERSION == (
        "first_opener_decision_to_terminal_native_producer.v3"
    )
    assert runner.RUN_SCHEMA_VERSION == (
        "first_opener_decision_to_terminal_native_run.v3"
    )
    assert spec["replay_contract"]["trace_schema_version"] == (
        "first_opener_decision_to_terminal_trace.v2"
    )
    assert set(spec["implementation_identity"]) == set(
        runner.REQUIRED_IMPLEMENTATION_PATHS
    )


def test_opener_native_producer_requires_every_campaign() -> None:
    runner._validate_trace_audit(_complete_audit(), "2026-04-20")

    incomplete = _complete_audit()
    incomplete["campaign_count"] = 5
    with pytest.raises(RuntimeError, match="denominator drifted"):
        runner._validate_trace_audit(incomplete, "2026-04-20")


def test_opener_native_producer_uses_exactly_22a_plus_11b() -> None:
    producer = _producer_spec()
    f10_spec = json.loads(
        resolve_research_path(producer["f10_spec_identity"]["path"]).read_text(
            encoding="utf-8"
        )
    )
    grades = runner._grade_by_day(f10_spec)

    assert len(grades) == 33
    assert list(grades.values()).count("A") == 22
    assert list(grades.values()).count("B") == 11


def test_opener_checkpoint_run_modes_cannot_be_reused(tmp_path: Path) -> None:
    result = {
        "_first_opener_decision_to_terminal_trace": pd.DataFrame(
            {"day": ["2026-04-20"]}
        ),
        "_first_opener_decision_to_terminal_trace_audit": _complete_audit(),
        "_producer_runtime_audit": {"day": "2026-04-20"},
    }
    runner._checkpoint_day(
        tmp_path,
        "2026-04-20",
        result,
        1.0,
        "a" * 64,
        "partial_diagnostic_only",
    )

    assert runner._load_checkpoint(
        tmp_path,
        "2026-04-20",
        "a" * 64,
        "partial_diagnostic_only",
    ) is not None
    with pytest.raises(ValueError, match="run mode drifted"):
        runner._load_checkpoint(
            tmp_path,
            "2026-04-20",
            "a" * 64,
            "formal_development_native_production",
        )


def test_opener_native_producer_preflights_every_target_and_d_minus_1(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    producer = _producer_spec()
    f10_spec = json.loads(
        resolve_research_path(producer["f10_spec_identity"]["path"]).read_text(
            encoding="utf-8"
        )
    )
    base = json.loads(
        resolve_research_path(
            producer["baseline_contract_identity"]["path"]
        ).read_text(encoding="utf-8")
    )
    base["source_identity"]["normalized_l2_root"] = str(
        relocate_marketdata_path(
            base["source_identity"]["normalized_l2_root"]
        )
    )
    checked_contexts: list[tuple[str, ...]] = []

    def record_formal_days(
        _root: Path,
        days: tuple[str, ...],
        *,
        verify_hashes: bool,
    ) -> None:
        assert not verify_hashes
        checked_contexts.append(tuple(days))
        if days[-1] == "2026-04-17":
            raise runner.l2_registry.FormalEligibilityError(
                "frozen excluded day"
            )

    monkeypatch.setattr(
        runner.l2_registry,
        "require_formal_days",
        record_formal_days,
    )
    audit = runner.validate_formal_day_universe(f10_spec, base)

    assert audit["target_day_count"] == 33
    assert len(audit["contexts"]) == 33
    assert {row["target_day"] for row in audit["contexts"]} == set(
        runner._grade_by_day(f10_spec)
    )
    assert len(checked_contexts) == 33
    assert all(len(days) == 2 for days in checked_contexts)
    assert all(days[1] in runner._grade_by_day(f10_spec) for days in checked_contexts)
    assert producer["identity"] == runner.IDENTITY

    invalid = copy.deepcopy(f10_spec)
    invalid["panels"]["development_sensitivity_grade_b_days"].append(
        "2026-04-17"
    )
    with pytest.raises(ValueError, match="not native-formal with D-1"):
        runner.validate_formal_day_universe(invalid, base)


def test_opener_worker_converts_system_exit_to_day_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_day(**_: object) -> dict:
        raise SystemExit("formal day rejected")

    monkeypatch.setattr(runner, "run_day", fail_day)
    with pytest.raises(RuntimeError, match="failed on 2026-04-20"):
        runner._run_day_for_pool(
            str(SPEC_PATH),
            "2026-04-20",
            "A",
            {},
        )
