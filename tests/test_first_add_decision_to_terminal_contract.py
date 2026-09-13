from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from research.families.f10_live_replay_attribution.audit import (
    first_add_decision_to_terminal_contract as contract,
)

ROOT = Path(__file__).resolve().parents[1]
SPEC_PATH = (
    ROOT
    / "research"
    / "families"
    / "f10_live_replay_attribution"
    / "docs"
    / "first_add_decision_to_terminal_loss_diagnostic_v1_spec_20260729.json"
)


def _spec() -> dict:
    return json.loads(SPEC_PATH.read_text(encoding="utf-8"))


def _row() -> dict:
    return {
        "trace_schema_version": contract.TRACE_SCHEMA_VERSION,
        "day": "2026-04-20",
        "quality_grade": "A",
        "campaign_id": 7,
        "decision_id": "decision-7",
        "decision_ts_ms": 100,
        "order_id": "order-7",
        "order_submit_ts_ms": 100,
        "fill_ts_ms": 150,
        "campaign_terminal_ts_ms": 500,
        "side": "BUY",
        "inventory_role": "add",
        "exact_decision_order_fill_join": 1,
        "decision_visible_feature_ready_ts_max_ms": 100,
        "decision_equity_usdc": 2.0,
        "campaign_terminal_equity_usdc": 1.5,
        contract.PRIMARY_ESTIMAND: -0.5,
    }


def test_f10_spec_is_hash_frozen_and_grants_no_authority() -> None:
    spec = _spec()
    contract.validate_spec(spec)
    assert len(spec["panels"]["development_primary_grade_a_days"]) == 24
    assert len(spec["panels"]["development_sensitivity_grade_b_days"]) == 16
    assert not any(spec["permissions"].values())


def test_f10_native_trace_requires_exact_causal_accounting() -> None:
    validated = contract.validate_native_trace(pd.DataFrame([_row()]), _spec())
    assert validated.loc[0, contract.PRIMARY_ESTIMAND] == pytest.approx(-0.5)

    future = _row()
    future["decision_visible_feature_ready_ts_max_ms"] = 101
    with pytest.raises(ValueError, match="non-causal"):
        contract.validate_native_trace(pd.DataFrame([future]), _spec())

    nearest_match = _row()
    nearest_match["exact_decision_order_fill_join"] = 0
    with pytest.raises(ValueError, match="not exact"):
        contract.validate_native_trace(pd.DataFrame([nearest_match]), _spec())


def test_f10_primary_and_grade_b_sensitivity_cannot_be_pooled_silently() -> None:
    wrong_grade = _row()
    wrong_grade["quality_grade"] = "B"
    with pytest.raises(ValueError, match="quality grade drifted"):
        contract.validate_native_trace(pd.DataFrame([wrong_grade]), _spec())

    later_panel = _row()
    later_panel["day"] = "2026-07-01"
    with pytest.raises(ValueError, match="outside frozen Development"):
        contract.validate_native_trace(pd.DataFrame([later_panel]), _spec())
