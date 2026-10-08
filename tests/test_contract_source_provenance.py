"""Source provenance does not pin the implementation of semantic validators."""

import json
from pathlib import Path

import pytest

from research.families.f09_inventory_lifecycle_action_uplift.audit import (
    lineage_randomized_outcome_contract as lineage,
)
from research.families.f10_live_replay_attribution.audit import (
    first_add_decision_to_terminal_contract as first_add,
    first_opener_decision_to_terminal_contract as first_opener,
)


@pytest.mark.parametrize(
    "relative,validator,digest_key,digest",
    [
        (
            "f09_inventory_lifecycle_action_uplift/docs/lineage_randomized_outcome_contract_v2.json",
            lineage.validate_foundation_contract,
            "canonical_contract_sha256",
            lineage.canonical_contract_sha256,
        ),
        (
            "f10_live_replay_attribution/docs/first_add_decision_to_terminal_loss_diagnostic_v1_spec_20260729.json",
            first_add.validate_spec,
            "canonical_spec_sha256",
            first_add.canonical_spec_sha256,
        ),
    ],
)
def test_current_semantics_do_not_require_historical_validator_bytes(
    relative, validator, digest_key, digest,
):
    root = Path(__file__).resolve().parents[1] / "research" / "families"
    # Construct a current-schema test input; the stored research record is untouched.
    payload = json.loads((root / relative).read_text().replace("campaign", "inventory_lifecycle"))
    payload["implementation_identity"] = {
        "contract_module_sha256": "0" * 64,
        "contract_test_sha256": "1" * 64,
    }
    payload[digest_key] = digest(payload)
    validator(payload)

    payload["permissions"]["live"] = True
    with pytest.raises(ValueError, match="hash mismatch"):
        validator(payload)
    payload[digest_key] = digest(payload)
    with pytest.raises(ValueError, match="cannot grant"):
        validator(payload)


def test_first_opener_validates_inputs_not_historical_implementation(tmp_path):
    root = Path(__file__).resolve().parents[1] / "research" / "families"
    path = root / "f10_live_replay_attribution/docs/sell_first_fill_conditional_value_feasibility_v3_spec_20260730.json"
    payload = json.loads(
        path.read_text().replace("campaign", "inventory_lifecycle")
        .replace("microprice_shift_bps", "weighted_mid_proxy_shift_bps")
    )
    evidence = tmp_path / "evidence.json"
    evidence.write_text("{}")
    identity = {"path": str(evidence), "sha256": first_opener.sha256_file(evidence)}
    for section, keys in (
        ("hypothesis_source", ("report_identity", "errata_identity")),
        ("quality_identity", ("normalized_l2_manifest_identity", "normalized_l2_daily_quality_identity")),
    ):
        for key in keys:
            payload[section][key] = dict(identity)
    payload["implementation_identity"] = {key: "0" * 64 for key in payload["implementation_identity"]}
    payload["canonical_spec_sha256"] = first_opener.canonical_spec_sha256(payload)
    first_opener.validate_spec(payload)
    evidence.write_text('{"changed": true}')
    with pytest.raises(ValueError, match="hypothesis report identity drifted"):
        first_opener.validate_spec(payload)
