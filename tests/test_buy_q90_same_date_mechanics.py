from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from research.families.f10_live_replay_attribution.audit import (
    buy_q90_same_date_mechanics as audit,
)


def test_same_date_spec_is_grade_b_and_permission_closed() -> None:
    spec = audit.load_spec(audit.DEFAULT_SPEC_PATH)
    assert spec["day"] == "2026-07-25"
    assert spec["quality_grade"] == "B"
    assert not any(spec["permissions"].values())


def test_mechanics_projection_does_not_read_economic_keys() -> None:
    result = {
        "dynamic_fill_hazard_eval_count": 100,
        "dynamic_fill_hazard_valid_eval_count": 80,
        "dynamic_fill_hazard_cancel_request_count": 4,
        "terminal_mtm_pnl": -999.0,
        "pnl": -888.0,
    }
    projected = audit._mechanics_only(result)
    assert "terminal_mtm_pnl" not in projected
    assert "pnl" not in projected
    assert projected["valid_probability_per_evaluation"] == pytest.approx(
        0.8
    )
    assert projected["cancel_probability_per_evaluation"] == pytest.approx(
        0.04
    )


def test_same_date_spec_rejects_permission_escalation(
    tmp_path: Path,
) -> None:
    spec = copy.deepcopy(audit.load_spec(audit.DEFAULT_SPEC_PATH))
    spec["permissions"]["transport_supported"] = True
    spec["canonical_spec_sha256"] = audit.canonical_spec_sha256(spec)
    path = tmp_path / "spec.json"
    path.write_text(json.dumps(spec), encoding="utf-8")
    with pytest.raises(ValueError, match="cannot grant permissions"):
        audit.load_spec(path)
