from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = (
    ROOT
    / "research/shared/replay_lifecycle/docs"
    / "time_scale_evidence_classification_v1_20260804.json"
)


def _canonical_sha256(payload: object) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def test_time_scale_classification_is_exhaustive_and_fail_closed() -> None:
    payload = json.loads(CONTRACT.read_text(encoding="utf-8"))
    observed = payload.pop("canonical_contract_sha256")
    assert observed == _canonical_sha256(payload)
    assert [row["name"] for row in payload["classes"]] == [
        "estimand_horizon",
        "policy_clock",
        "transport_limit",
        "feature_basis",
        "governance_threshold",
    ]
    assert len({row["authoritative_method"] for row in payload["classes"]}) == 5
    assert payload["non_boundary_events"] == ["utc_midnight"]
    assert len(payload["baseline_epoch_key_fields"]) == 8
    assert all(value is False for value in payload["permissions"].values())


def test_lifecycle_registry_keeps_epoch_and_competing_risk_semantics() -> None:
    payload = json.loads(CONTRACT.read_text(encoding="utf-8"))
    requirements = payload["lifecycle_requirements"]
    assert requirements["calendar_time_and_risk_time_both_required"] is True
    assert requirements["inactive_or_terminal_intervals_excluded_from_risk_exposure"] is True
    assert requirements["competing_risk_event_identity_preserved"] is True
    assert requirements["pooling_across_baseline_epochs_is_primary"] is False
    assert requirements["epoch_specific_curves_required_before_pooled_sensitivity"] is True
