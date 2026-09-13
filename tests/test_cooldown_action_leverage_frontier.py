from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from research.families.f09_campaign_action_uplift.audit import (
    cooldown_action_leverage_frontier as audit,
)

ROOT = Path(__file__).resolve().parents[1]
SPEC_PATH = (
    ROOT
    / "research"
    / "families"
    / "f09_campaign_action_uplift"
    / "docs"
    / "cooldown_action_leverage_frontier_v1_spec_20260730.json"
)


def test_optional_ratio_reports_one_cycle_required_value_scale() -> None:
    assert audit._optional_ratio(0.014364963291585165, 0.0715771951727008) == pytest.approx(
        0.2006918999
    )
    assert audit._optional_ratio(1.0, 0.0) is None


def test_exhaustion_requires_no_supported_frontier_row() -> None:
    frame = pd.DataFrame(
        [
            {
                "historical_closure_authoritative": True,
                "current_metric_authority": True,
                "activity_supported": True,
                "economic_lower_bound_positive": False,
                "lifecycle_supported": True,
            },
            {
                "historical_closure_authoritative": True,
                "current_metric_authority": False,
                "activity_supported": False,
                "economic_lower_bound_positive": True,
                "lifecycle_supported": False,
            },
        ]
    )
    result = audit.synthesize_decision(frame)
    assert result["tested_subspace_exhausted"]
    assert not result["f09_family_closed"]
    assert result["rows_with_activity_and_positive_economic_and_lifecycle_support"] == 0

    frame.loc[0, "economic_lower_bound_positive"] = True
    result = audit.synthesize_decision(frame)
    assert not result["tested_subspace_exhausted"]


def test_frozen_spec_forbids_pooling_and_action_registration() -> None:
    spec = json.loads(SPEC_PATH.read_text(encoding="utf-8"))
    audit.validate_spec(spec)

    pooled = json.loads(json.dumps(spec))
    pooled["audit_contract"]["cross_source_pooled_estimate_allowed"] = True
    pooled["canonical_spec_sha256"] = audit.canonical_spec_sha256(pooled)
    with pytest.raises(ValueError, match="cannot be pooled"):
        audit.validate_spec(pooled)

    action = json.loads(json.dumps(spec))
    action["audit_contract"]["may_register_action"] = True
    action["canonical_spec_sha256"] = audit.canonical_spec_sha256(action)
    with pytest.raises(ValueError, match="cannot register"):
        audit.validate_spec(action)


def test_withdrawn_metrics_cannot_be_marked_current_without_new_identity() -> None:
    spec = json.loads(SPEC_PATH.read_text(encoding="utf-8"))
    drifted = json.loads(json.dumps(spec))
    drifted["source_identities"]["recovery_event_sell_historical"][
        "evidence_status"
    ] = "historical_metric_promoted"
    drifted["canonical_spec_sha256"] = audit.canonical_spec_sha256(drifted)
    with pytest.raises(ValueError, match="unsupported evidence status"):
        audit.validate_spec(drifted)
