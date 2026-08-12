from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXECUTION_ERRATA = ROOT / (
    "research/families/f09_campaign_action_uplift/docs/"
    "ber_guard_role_safe_add_only_current_stack_owner_v1_"
    "execution_estimand_errata_v1_20260808.json"
)


def test_execution_errata_changes_denominator_not_policy() -> None:
    payload = json.loads(EXECUTION_ERRATA.read_text(encoding="utf-8"))
    assert payload["unchanged_policy"]["threshold_search"] is False
    assert payload["unchanged_policy"]["multiplier_search"] is False
    assert payload["unchanged_policy"]["role_change"] is False
    assert payload["superseded_execution_requirements"][
        "equal_control_candidate_n_requotes"
    ] is False
    assert payload["authoritative_requirements"][
        "candidate_effective_change_rate_denominator"
    ] == "two candidate-arm canonical side quote decisions per candidate-arm requote"
