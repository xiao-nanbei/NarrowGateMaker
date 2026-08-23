from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from research.governance import public_machine_projection as projection_module
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


def test_nonpublished_private_source_may_be_materialized_at_runtime_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_bytes = b'{"private_locator":"owner-only"}\n'
    projection_bytes = b'{"private_locator":"${NARROWGATE_PRIVATE_ROOT}"}\n'
    runtime_path = tmp_path / "models/saved_bundle/record.json"
    runtime_path.parent.mkdir(parents=True)
    runtime_path.write_bytes(source_bytes)
    index = (
        tmp_path
        / "models/private/nonpublished_machine_document_projections.current.local.json"
    )
    index.parent.mkdir(parents=True)
    index.write_text(
        json.dumps(
            {
                "schema_version": (
                    "narrowgate_nonpublished_machine_document_projections_v1"
                ),
                "entries": [
                    {
                        "availability": (
                            "private_working_tree_projection_not_distributed"
                        ),
                        "public_path": "models/saved_bundle/record.json",
                        "public_projection_sha256": hashlib.sha256(
                            projection_bytes
                        ).hexdigest(),
                        "source_private_sha256": hashlib.sha256(source_bytes).hexdigest(),
                        "unit_id": "research/families/f03_causal_13_head",
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(projection_module, "PROJECT_ROOT", tmp_path)

    projection = projection_module.projection_for(runtime_path)

    assert projection is not None
    assert projection.materialized_identity == "private_source"
    assert projection.require_private_source() == runtime_path
    assert projection_module.source_document_path(
        runtime_path, require_private=True
    ) == runtime_path
    assert projection_module.source_identity_sha256(runtime_path) == hashlib.sha256(
        source_bytes
    ).hexdigest()


def test_nonpublished_runtime_path_still_rejects_unregistered_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime_path = tmp_path / "models/saved_bundle/record.json"
    runtime_path.parent.mkdir(parents=True)
    runtime_path.write_bytes(b"tampered\n")
    index = (
        tmp_path
        / "models/private/nonpublished_machine_document_projections.current.local.json"
    )
    index.parent.mkdir(parents=True)
    index.write_text(
        json.dumps(
            {
                "schema_version": (
                    "narrowgate_nonpublished_machine_document_projections_v1"
                ),
                "entries": [
                    {
                        "availability": (
                            "private_working_tree_projection_not_distributed"
                        ),
                        "public_path": "models/saved_bundle/record.json",
                        "public_projection_sha256": "1" * 64,
                        "source_private_sha256": "2" * 64,
                        "unit_id": "research/families/f03_causal_13_head",
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(projection_module, "PROJECT_ROOT", tmp_path)

    with pytest.raises(
        projection_module.PublicMachineProjectionError,
        match="public projection SHA256 mismatch",
    ):
        projection_module.projection_for(runtime_path)
