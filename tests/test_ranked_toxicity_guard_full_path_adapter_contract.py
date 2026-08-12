from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from research.families.f09_campaign_action_uplift.audit.ranked_toxicity_guard_full_path_adapter_contract import (
    validate_execution_amendment,
)
from research.families.f09_campaign_action_uplift.audit.ranked_toxicity_guard_full_path_adapter_contract_v1_2 import (
    validate_execution_amendment_v1_2,
)

ROOT = Path(__file__).resolve().parents[1]
V1_1_AMENDMENT = ROOT / (
    "research/families/f09_campaign_action_uplift/docs/"
    "causal_v12_ranked_toxicity_exposure_guard_full_path_adapter_v1_1_"
    "execution_amendment_20260802.json"
)
V1_2_AMENDMENT = ROOT / (
    "research/families/f09_campaign_action_uplift/docs/"
    "causal_v12_ranked_toxicity_exposure_guard_full_path_adapter_v1_2_"
    "execution_amendment_20260802.json"
)


def test_v1_1_is_preserved_as_historical_dependency_identity() -> None:
    assert hashlib.sha256(V1_1_AMENDMENT.read_bytes()).hexdigest() == (
        "313958662518468475e01c0c1a58ce36931c5a6b42f10738711ae510f295ee87"
    )
    with pytest.raises(ValueError, match="shared_order_lifecycle SHA256 mismatch"):
        validate_execution_amendment(V1_1_AMENDMENT)


def test_v1_2_is_preserved_but_rejects_current_lifecycle_bytes() -> None:
    with pytest.raises(ValueError, match="shared_order_lifecycle SHA256 mismatch"):
        validate_execution_amendment_v1_2(V1_2_AMENDMENT)
