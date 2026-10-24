"""Contract-only audits never certify unexamined runtime implementation bytes."""
import hashlib
import json

import pytest

from research.families.f04_external_market_alpha.audit import (
    exact_opener_opportunity_tape_contract_v2_1 as contract,
)


def test_v2_1_scope_uses_synthetic_predecessor_not_retired_archive(tmp_path):
    predecessor = tmp_path / "predecessor.json"
    predecessor.write_text("{}")
    identity = dict(path=str(predecessor), sha256=hashlib.sha256(b"{}").hexdigest())
    payload = dict(
        schema_version=contract.SCHEMA_VERSION,
        frozen_v2_predecessor=dict(
            spec=identity, registration=identity,
            identity="external_adverse_quote_edge_guard_exact_opener_mechanics_v2",
            v2_files_modified=False),
        validator_contract=contract.exact_opener_validator_contract(),
        research_provenance=dict(successor_class="mechanics_informed_successor",
                                 same_as_v1_estimand=False, first_add_or_P1_rows_in_scope=False),
        outcome_boundary=dict(economic_outcomes_read=False, operational_lifecycle_outcomes_read=True,
                              external_outcome_tables_allowed=False),
        contract_tests=dict(passed=True, full_repository_suite_passed=True),
        permissions=dict(prospective_collection_eligible=True),
        implementation_identity=dict(runtime_writer=dict(path="absent.py", sha256="0" * 64)),
    )
    payload["canonical_spec_identity_sha256"] = contract.canonical_spec_sha256(payload)
    amendment = tmp_path / "amendment.json"
    amendment.write_text(json.dumps(payload))
    audit = contract.validate_execution_amendment_v2_1(amendment)
    assert audit["frozen_v2_bytes_valid"] is True
    assert audit["implementation_hashes_checked"] is False
    assert audit["implementation_hashes_valid"] is None
    predecessor.write_text('{"changed": true}')
    with pytest.raises(ValueError, match="SHA256 mismatch"):
        contract.validate_execution_amendment_v2_1(amendment)
