from __future__ import annotations

import hashlib
import json

from research.governance.public_machine_projection import (
    PROJECT_ROOT,
    projection_for,
    source_document_path,
    source_identity_sha256,
)

PUBLIC_SPEC = PROJECT_ROOT / (
    "research/families/f02_empirical_p3_touch/docs/"
    "p3_touch_conditional_curve_quote_mapping_v1_spec_20260803.json"
)
F05_ADD_WAIT_SPEC = PROJECT_ROOT / (
    "research/families/f05_fill_quality_quote_ev/docs/"
    "multiscale_ema_add_wait_incremental_value_v1_1_spec_20260809.json"
)
F10_40DAY_BASELINE = PROJECT_ROOT / (
    "research/families/f10_live_replay_attribution/docs/"
    "current_live_held_ber_replay_baseline_40d_20260809.json"
)


def test_projection_keeps_public_and_executed_source_identities_distinct() -> None:
    projection = projection_for(PUBLIC_SPEC)

    assert projection is not None
    assert (
        projection.public_projection_sha256 == hashlib.sha256(PUBLIC_SPEC.read_bytes()).hexdigest()
    )
    assert source_identity_sha256(PUBLIC_SPEC) == projection.source_private_sha256
    assert projection.public_projection_sha256 != projection.source_private_sha256


def test_owner_private_source_preserves_embedded_canonical_identity() -> None:
    projection = projection_for(PUBLIC_SPEC)
    assert projection is not None
    if not projection.private_source_available:
        return

    source_path = source_document_path(PUBLIC_SPEC, require_private=True)
    payload = json.loads(source_path.read_text(encoding="utf-8"))
    expected = payload.pop("canonical_spec_identity_sha256")
    observed = hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
    ).hexdigest()
    assert observed == expected


def test_cross_unit_public_projection_resolves_owner_private_source() -> None:
    projection = projection_for(PROJECT_ROOT / "research/registry.json")
    assert projection is not None
    assert projection.unit_id == "research/shared/experiment_governance"
    assert "original_public_machine_records/cross_unit/research/registry.json" in str(
        projection.require_private_source()
    )


def test_f05_add_wait_spec_keeps_projection_and_frozen_source_identities_distinct() -> None:
    projection = projection_for(F05_ADD_WAIT_SPEC)
    assert projection is not None
    assert (
        projection.public_projection_sha256
        == hashlib.sha256(F05_ADD_WAIT_SPEC.read_bytes()).hexdigest()
    )
    assert projection.source_private_sha256 == (
        "b59f9f5a3c9cbdd1fa714abe6ddf8ef23e19654374c354a6840e6f943a7c6908"
    )
    assert source_identity_sha256(F05_ADD_WAIT_SPEC) == projection.source_private_sha256

    public_payload = json.loads(F05_ADD_WAIT_SPEC.read_text(encoding="utf-8"))
    source = public_payload["source_contract"]
    assert source["operational_config"]["path"] == "${NARROWGATE_LIVE_CONFIG}"
    pointer = source["operational_baseline_pointer"]
    assert pointer["exact_bytes_status"] == "missing_from_governed_private_evidence_store"
    assert pointer["missing_exact_bytes_policy"].startswith("fail_closed")


def test_f05_denominator_binding_names_f10_private_source_and_public_projection() -> None:
    f10_projection = projection_for(F10_40DAY_BASELINE)
    assert f10_projection is not None
    public_payload = json.loads(F05_ADD_WAIT_SPEC.read_text(encoding="utf-8"))
    binding = public_payload["source_contract"]["denominator_source_spec"]

    assert binding["sha256_identity_kind"] == "private_source_sha256_for_public_projection"
    assert binding["sha256"] == f10_projection.source_private_sha256
    assert binding["public_projection_sha256"] == f10_projection.public_projection_sha256
    assert binding["availability"] == "public_repository"
    assert binding["source_private_availability"] == "private_not_distributed"
