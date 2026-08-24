from __future__ import annotations

import fcntl
import hashlib
import json
import os
import subprocess
from pathlib import Path

import pytest

import scripts.f05_reconcile_live_config_locator_v1 as subject


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _private_write(path: Path, raw: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    path.chmod(0o600)


def _audit(*findings: dict) -> dict:
    return {
        "schema_version": subject.audit_private_evidence.AUDIT_SCHEMA,
        "mode": subject.audit_private_evidence.METADATA_ONLY,
        "deny_locked": True,
        "validation_read": False,
        "sealed_holdout_read": False,
        "findings": list(findings),
    }


def _canonical_document(payload: dict, field: str) -> bytes:
    payload[field] = subject._document_sha(payload, field)
    return subject._render(payload)


def _transaction_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> dict:
    metadata_root = tmp_path / "metadata"
    private = metadata_root / "docs/private"
    private.mkdir(parents=True)
    private.chmod(0o700)
    old_config = b"old-v12-control\n"
    active_config = b"release-v3-no-shadow\n"
    alias = private / subject.CURRENT_ALIAS_FILENAME
    _private_write(alias, old_config)

    activation = {
        "schema_version": "test.activation.v6",
        "status": "completed_active_release_v3_no_shadow_evidence_closed",
    }
    activation_raw = _canonical_document(
        activation,
        "canonical_replacement_activation_receipt_sha256",
    )
    activation_path = private / "immutable-v6-activation-receipt.test.local.json"
    _private_write(activation_path, activation_raw)
    monkeypatch.setattr(
        subject,
        "V6_ACTIVATION",
        {
            "schema_version": activation["schema_version"],
            "status": activation["status"],
            "file_sha256": _sha(activation_raw),
            "canonical_field": "canonical_replacement_activation_receipt_sha256",
            "canonical_sha256": activation["canonical_replacement_activation_receipt_sha256"],
            "size_bytes": len(activation_raw),
            "mode": "0600",
        },
    )
    monkeypatch.setattr(subject, "OLD_CONFIG_SHA256", _sha(old_config))
    monkeypatch.setattr(subject, "OLD_CONFIG_SIZE", len(old_config))
    monkeypatch.setattr(subject, "ACTIVE_CONFIG_SHA256", _sha(active_config))
    monkeypatch.setattr(subject, "ACTIVE_CONFIG_SIZE", len(active_config))

    pointer_path = private / "live_remote.current.local.json"
    pointer = {
        "schema_version": subject.POINTER_SCHEMA,
        "status": "current_active",
        "config_sha256": _sha(active_config),
        "current_config_locator_reconciliation": None,
        "current_activation_receipt": {
            "path": str(activation_path),
            "sha256": _sha(activation_raw),
            "canonical_sha256": activation["canonical_replacement_activation_receipt_sha256"],
            "bytes": len(activation_raw),
        },
        "current_buy_e3_release": {
            "active_release_file_sha256": subject.RELEASE_V3["file_sha256"],
            "active_release_canonical_sha256": subject.RELEASE_V3["canonical_sha256"],
            "active_config_sha256": _sha(active_config),
            "execution_commit": subject.RUNTIME_V3["commit"],
            "execution_tree": subject.RUNTIME_V3["tree"],
            "annotated_tag_object": subject.RUNTIME_V3["annotated_tag_object"],
            "external_venues_enabled": False,
            "global_flow_shadow_enabled": False,
            "global_reference_shadow_enabled": False,
        },
        "current_query_policy": {},
    }
    pointer_raw = subject._render(pointer)
    _private_write(pointer_path, pointer_raw)
    monkeypatch.setattr(subject, "PREDECESSOR_POINTER_SHA256", _sha(pointer_raw))
    monkeypatch.setattr(subject, "PREDECESSOR_POINTER_SIZE", len(pointer_raw))

    catalog_path = private / "catalog.current.local.json"
    catalog = {
        "schema_version": subject.CATALOG_SCHEMA,
        "visibility": "local_only_do_not_publish",
        "unit_id": "repository",
        "entries": [
            {
                "artifact_id": subject.CURRENT_CONFIG_ARTIFACT_ID,
                "role": "current_live_config",
                "local_path": str(alias),
                "sha256": _sha(old_config),
                "bytes": len(old_config),
                "panel_role": "operational",
                "read_gate": "owner_authorized_locator_resolution_only",
            },
            {
                "artifact_id": subject.CURRENT_POINTER_ARTIFACT_ID,
                "role": "current_live_remote_pointer",
                "local_path": str(pointer_path),
                "sha256": _sha(pointer_raw),
                "bytes": len(pointer_raw),
                "panel_role": "operational",
                "read_gate": "owner_authorized_locator_resolution_only",
            },
            {
                "artifact_id": subject.V6_ACTIVATION_ARTIFACT_ID,
                "role": "immutable_v6_activation",
                "local_path": str(activation_path),
                "sha256": _sha(activation_raw),
                "bytes": len(activation_raw),
                "panel_role": "operational",
                "read_gate": "owner_only",
            },
        ],
    }
    catalog_raw = subject._render(catalog)
    _private_write(catalog_path, catalog_raw)
    monkeypatch.setattr(subject, "PREDECESSOR_CATALOG_SHA256", _sha(catalog_raw))
    monkeypatch.setattr(subject, "PREDECESSOR_CATALOG_SIZE", len(catalog_raw))

    evidence_root = tmp_path / "evidence"
    active_source = evidence_root / subject.ACTIVE_CONFIG_SOURCE_RELATIVE
    _private_write(active_source, active_config)
    manifest_path = evidence_root / subject.FORMAL_MANIFEST_RELATIVE
    baseline = subject._audit_baseline(_audit())
    publisher_source = {
        "module_route": subject.PUBLISHER_MODULE_ROUTE,
        "annotated_tag": subject.PUBLISHER_TAG,
        "annotated_tag_object": "1" * 40,
        "commit": "2" * 40,
        "tree": "3" * 40,
        "script_sha256": "4" * 64,
    }
    manifest = {
        "schema_version": subject.MANIFEST_SCHEMA,
        "status": subject.MANIFEST_STATUS,
        "generated_utc": "2026-08-25T00:00:00.000000000Z",
        "receipt_id": subject.MANIFEST_RECEIPT_ID,
        "publisher_root": str(tmp_path / "publisher"),
        "metadata_repository_root": str(metadata_root),
        "publisher_source": publisher_source,
        "tracked_successor_files": {
            "scripts/audit_private_evidence.py": "5" * 64,
        },
        "metadata_audit_baseline": baseline,
        "transaction": subject._transaction_contract(
            metadata_root,
            active_source,
            activation_path=activation_path,
        ),
        "permissions": subject.NO_NEW_AUTHORITY,
        "evidence_boundary": subject.EVIDENCE_BOUNDARY,
    }
    manifest[subject.MANIFEST_CANONICAL_FIELD] = subject._document_sha(
        manifest, subject.MANIFEST_CANONICAL_FIELD
    )
    manifest_binding = {
        "path": str(manifest_path),
        "schema_version": subject.MANIFEST_SCHEMA,
        "status": subject.MANIFEST_STATUS,
        "file_sha256": "6" * 64,
        "canonical_field": subject.MANIFEST_CANONICAL_FIELD,
        "canonical_sha256": manifest[subject.MANIFEST_CANONICAL_FIELD],
        "size_bytes": 1,
        "mode": "0600",
    }
    monkeypatch.setattr(subject, "_active_config_source_path", lambda: active_source)
    monkeypatch.setattr(
        subject,
        "_load_manifest_for_execute",
        lambda _path: (manifest, manifest_binding),
    )
    monkeypatch.setattr(
        subject,
        "_load_manifest_for_staging_preflight",
        lambda _path: manifest,
    )
    monkeypatch.setattr(
        subject,
        "_full_candidate_owner_root_audit",
        lambda **_kwargs: {
            "status": "full_metadata_only_candidate_owner_root_audit_passed",
            "new_finding_count": 0,
            "passed": True,
        },
    )
    monkeypatch.setattr(subject, "_validate_execution_source_identity", lambda _manifest: None)
    monkeypatch.setattr(
        subject,
        "_validate_execution_immutable_authorities",
        lambda **_kwargs: {
            "manifest": manifest_binding,
            "v6_activation": {"path": str(activation_path)},
            "private_evidence_layout_exact": True,
        },
    )
    return {
        "metadata_root": metadata_root,
        "private": private,
        "manifest": manifest,
        "manifest_path": manifest_path,
        "old_config": old_config,
        "active_config": active_config,
        "pointer_raw": pointer_raw,
        "catalog_raw": catalog_raw,
        "activation_raw": activation_raw,
        "activation_path": activation_path,
        "audit": lambda _root: _audit(),
    }


def test_v6_activation_locator_is_derived_and_confined_to_private_root(
    tmp_path: Path,
) -> None:
    private = tmp_path / "metadata/docs/private"
    private.mkdir(parents=True)
    expected = private / "opaque-immutable-activation.local.json"
    pointer = {"current_activation_receipt": {"path": str(expected)}}
    assert subject._v6_activation_path_from_pointer(pointer, private) == expected

    for escaped in (
        tmp_path / "metadata/docs/outside.json",
        private / "nested/activation.json",
    ):
        pointer["current_activation_receipt"]["path"] = str(escaped)
        with pytest.raises(
            subject.ConfigLocatorReconciliationError,
            match="escapes docs/private",
        ):
            subject._v6_activation_path_from_pointer(pointer, private)


@pytest.mark.parametrize("mutation", ["mode", "hardlink", "symlink"])
def test_dynamic_v6_activation_locator_rejects_unsafe_file_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    fixture = _transaction_fixture(tmp_path, monkeypatch)
    activation = fixture["activation_path"]
    subject._validate_current_activation_crossbinding(fixture["metadata_root"], activation)
    if mutation == "mode":
        activation.chmod(0o644)
    elif mutation == "hardlink":
        activation.with_name("second-link.local.json").hardlink_to(activation)
    else:
        original = activation.with_name("activation-original.local.json")
        activation.replace(original)
        activation.symlink_to(original)
    with pytest.raises(subject.ConfigLocatorReconciliationError, match="unsafe|unavailable"):
        subject._validate_current_activation_crossbinding(fixture["metadata_root"], activation)


def test_dynamic_activation_pointer_and_catalog_must_crossbind(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _transaction_fixture(tmp_path, monkeypatch)
    activation = fixture["activation_path"]
    alternate = activation.with_name("alternate-activation.local.json")
    _private_write(alternate, fixture["activation_raw"])
    pointer_path = fixture["private"] / "live_remote.current.local.json"
    pointer = json.loads(fixture["pointer_raw"])
    pointer["current_activation_receipt"]["path"] = str(alternate)
    _private_write(pointer_path, subject._render(pointer))
    with pytest.raises(subject.ConfigLocatorReconciliationError, match="locator drifted"):
        subject._validate_current_activation_crossbinding(fixture["metadata_root"], activation)

    _private_write(pointer_path, fixture["pointer_raw"])
    catalog_path = fixture["private"] / "catalog.current.local.json"
    catalog = json.loads(fixture["catalog_raw"])
    activation_row = next(
        row for row in catalog["entries"] if row["artifact_id"] == subject.V6_ACTIVATION_ARTIFACT_ID
    )
    activation_row["local_path"] = str(alternate)
    _private_write(catalog_path, subject._render(catalog))
    with pytest.raises(subject.ConfigLocatorReconciliationError, match="catalog activation"):
        subject._validate_current_activation_crossbinding(fixture["metadata_root"], activation)


def test_manifest_build_uses_last_validated_activation_path_without_pointer_reopen(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _transaction_fixture(tmp_path, monkeypatch)
    evidence_root = fixture["manifest_path"].parents[3]
    monkeypatch.setattr(subject, "_private_evidence_root", lambda: evidence_root)
    arguments = {
        "publisher_root": tmp_path / "publisher",
        "metadata_root": fixture["metadata_root"],
        "active_config_source": (evidence_root / subject.ACTIVE_CONFIG_SOURCE_RELATIVE),
        "publisher_source": {
            "module_route": subject.PUBLISHER_MODULE_ROUTE,
            "annotated_tag": subject.PUBLISHER_TAG,
            "annotated_tag_object": "1" * 40,
            "commit": "2" * 40,
            "tree": "3" * 40,
            "script_sha256": "4" * 64,
        },
        "tracked_successor_files": {
            relative: "5" * 64 for relative in subject.TRACKED_SUCCESSOR_FILES
        },
        "metadata_audit_baseline": subject._audit_baseline(_audit()),
        "activation_path": fixture["activation_path"],
        "generated_utc": "2026-08-25T00:00:00.000000000Z",
    }
    first = subject._render(subject._build_manifest(**arguments))
    pointer_path = fixture["private"] / "live_remote.current.local.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    pointer["current_activation_receipt"]["path"] = str(
        fixture["private"] / "attacker-selected.local.json"
    )
    _private_write(pointer_path, subject._render(pointer))
    second = subject._render(subject._build_manifest(**arguments))
    assert second == first


def _manifest_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> dict:
    evidence_root = tmp_path / "evidence"
    evidence_unit = evidence_root / subject.E3_V6_EVIDENCE_RELATIVE
    evidence_unit.mkdir(parents=True)
    evidence_unit.chmod(0o700)
    metadata_root = tmp_path / "metadata"
    private = metadata_root / "docs/private"
    private.mkdir(parents=True)
    private.chmod(0o700)
    predecessor_pointer = b'{"test":"predecessor-pointer"}\n'
    _private_write(private / "live_remote.current.local.json", predecessor_pointer)
    monkeypatch.setattr(subject, "PREDECESSOR_POINTER_SHA256", _sha(predecessor_pointer))
    monkeypatch.setattr(subject, "PREDECESSOR_POINTER_SIZE", len(predecessor_pointer))
    publisher_root = tmp_path / "publisher"
    publisher_root.mkdir()
    activation_path = private / "opaque-immutable-activation.local.json"
    active_source = evidence_root / subject.ACTIVE_CONFIG_SOURCE_RELATIVE
    _private_write(active_source, b"active\n")
    active_source.parent.chmod(0o700)
    formal = evidence_root / subject.FORMAL_MANIFEST_RELATIVE
    publisher = {
        "module_route": subject.PUBLISHER_MODULE_ROUTE,
        "annotated_tag": subject.PUBLISHER_TAG,
        "annotated_tag_object": "1" * 40,
        "commit": "2" * 40,
        "tree": "3" * 40,
        "script_sha256": "4" * 64,
    }
    tracked = {relative: "5" * 64 for relative in subject.TRACKED_SUCCESSOR_FILES}
    calls: list[str] = []

    monkeypatch.setattr(subject, "_private_evidence_root", lambda: evidence_root)
    monkeypatch.setattr(
        subject,
        "_validate_predecessor",
        lambda _root: {"activation_binding": {"path": str(activation_path)}},
    )
    monkeypatch.setattr(
        subject,
        "_validate_current_activation_crossbinding",
        lambda _root, _path: {
            "path": str(activation_path),
            **subject.V6_ACTIVATION,
        },
    )
    monkeypatch.setattr(subject, "_validate_active_config_source", lambda _path: b"active\n")
    monkeypatch.setattr(subject, "_observe_publisher", lambda _root: dict(publisher))
    monkeypatch.setattr(
        subject,
        "_tracked_successor_tree_files",
        lambda _root, _commit: dict(tracked),
    )

    def validate_pair(**_kwargs: object) -> dict[str, str]:
        calls.append("tracked")
        return dict(tracked)

    monkeypatch.setattr(subject, "_validate_tracked_source_pair", validate_pair)
    monkeypatch.setattr(subject, "_audit_fn", lambda _root: _audit())
    return {
        "evidence_root": evidence_root,
        "metadata_root": metadata_root,
        "private": private,
        "publisher_root": publisher_root,
        "active_source": active_source,
        "formal": formal,
        "pending": subject._pending(formal, "create"),
        "publisher": publisher,
        "tracked": tracked,
        "calls": calls,
    }


def _prepare_manifest(fixture: dict) -> dict:
    return subject.prepare_manifest(
        publisher_root=fixture["publisher_root"],
        metadata_repository_root=fixture["metadata_root"],
        active_config_source=fixture["active_source"],
        receipt_id=subject.MANIFEST_RECEIPT_ID,
        output_path=fixture["formal"],
    )


def test_manifest_first_writer_creates_only_authorized_leaf_and_is_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _manifest_fixture(tmp_path, monkeypatch)
    assert not fixture["formal"].parent.exists()

    first = _prepare_manifest(fixture)

    assert first["write_semantics"] == "create_only_first_writer"
    assert fixture["formal"].parent.stat().st_mode & 0o777 == 0o700
    assert fixture["formal"].stat().st_mode & 0o777 == 0o600
    assert fixture["formal"].stat().st_nlink == 1
    raw = fixture["formal"].read_bytes()
    repeated = _prepare_manifest(fixture)
    assert repeated["write_semantics"] == "create_only_idempotent_existing_exact_reused"
    assert fixture["formal"].read_bytes() == raw
    assert len(fixture["calls"]) >= 4


@pytest.mark.parametrize("recovery", ["pending_only", "nlink2"])
def test_manifest_crash_states_resume_same_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    recovery: str,
) -> None:
    fixture = _manifest_fixture(tmp_path, monkeypatch)
    _prepare_manifest(fixture)
    raw = fixture["formal"].read_bytes()
    if recovery == "pending_only":
        fixture["formal"].replace(fixture["pending"])
    else:
        os.link(fixture["formal"], fixture["pending"])
    observed = _prepare_manifest(fixture)
    assert observed["write_semantics"] in {
        "create_only_pending_recovered",
        "create_only_idempotent_existing_exact_reused",
    }
    assert fixture["formal"].read_bytes() == raw
    assert fixture["formal"].stat().st_nlink == 1
    assert not fixture["pending"].exists()


def test_manifest_short_staging_residue_is_discarded_before_fresh_first_writer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _manifest_fixture(tmp_path, monkeypatch)
    original_write_new = subject._write_new
    interrupted_data: bytes | None = None
    base_ns = subject._now_utc_ns()
    clock_tick = 0

    def advancing_clock() -> int:
        nonlocal clock_tick
        clock_tick += 1
        return base_ns + clock_tick

    def interrupt_staging(path: Path, data: bytes) -> None:
        nonlocal interrupted_data
        if subject._is_manifest_staging_path(path, fixture["formal"]):
            interrupted_data = data
            original_write_new(path, data[: len(data) // 2])
            raise RuntimeError("interrupted-manifest-staging-write")
        original_write_new(path, data)

    monkeypatch.setattr(subject, "_now_utc_ns", advancing_clock)
    monkeypatch.setattr(subject, "_write_new", interrupt_staging)
    with pytest.raises(RuntimeError, match="interrupted-manifest-staging-write"):
        _prepare_manifest(fixture)
    monkeypatch.setattr(subject, "_write_new", original_write_new)

    assert interrupted_data is not None
    staging = subject._manifest_staging_paths(fixture["formal"])
    assert len(staging) == 1
    residue = staging[0].read_bytes()
    assert len(residue) < len(interrupted_data)
    assert interrupted_data.startswith(residue)
    assert not fixture["formal"].exists()
    assert not fixture["pending"].exists()
    tracked_calls_before_resume = len(fixture["calls"])

    resumed = _prepare_manifest(fixture)

    assert resumed["write_semantics"] == "create_only_first_writer"
    assert len(fixture["calls"]) > tracked_calls_before_resume
    assert not subject._manifest_staging_paths(fixture["formal"])
    assert not fixture["pending"].exists()
    interrupted_payload = json.loads(interrupted_data)
    resumed_payload = json.loads(fixture["formal"].read_bytes())
    assert resumed_payload["generated_utc"] != interrupted_payload["generated_utc"]


@pytest.mark.parametrize("unsafe", ["mode", "hardlink"])
def test_manifest_unsafe_orphan_staging_is_preserved_failclosed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    unsafe: str,
) -> None:
    fixture = _manifest_fixture(tmp_path, monkeypatch)
    subject._ensure_private_directory(fixture["formal"].parent)
    staging = subject._manifest_staging_path(fixture["formal"], "0" * 32)
    _private_write(staging, b"partial-manifest")
    if unsafe == "mode":
        staging.chmod(0o640)
    else:
        os.link(staging, staging.with_name("attacker-second-link"))

    with pytest.raises(subject.ConfigLocatorReconciliationError, match="unsafe file"):
        _prepare_manifest(fixture)

    assert not fixture["formal"].exists()
    assert not fixture["pending"].exists()
    assert staging.read_bytes() == b"partial-manifest"


def test_existing_manifest_never_cleans_orphan_staging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _manifest_fixture(tmp_path, monkeypatch)
    _prepare_manifest(fixture)
    published = fixture["formal"].read_bytes()
    staging = subject._manifest_staging_path(fixture["formal"], "1" * 32)
    _private_write(staging, b"partial-manifest")

    with pytest.raises(subject.ConfigLocatorReconciliationError, match="ambiguous"):
        _prepare_manifest(fixture)

    assert fixture["formal"].read_bytes() == published
    assert staging.read_bytes() == b"partial-manifest"


def test_manifest_completed_staging_link_transfer_resumes_exact_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _manifest_fixture(tmp_path, monkeypatch)
    _prepare_manifest(fixture)
    published = fixture["formal"].read_bytes()
    staging = subject._manifest_staging_path(fixture["formal"], "2" * 32)
    fixture["formal"].replace(staging)
    os.link(staging, fixture["pending"])
    assert staging.stat().st_nlink == 2

    resumed = _prepare_manifest(fixture)

    assert resumed["write_semantics"] == "create_only_pending_recovered"
    assert fixture["formal"].read_bytes() == published
    assert fixture["formal"].stat().st_nlink == 1
    assert not staging.exists()
    assert not fixture["pending"].exists()


@pytest.mark.parametrize("state", ["existing", "pending_only"])
def test_manifest_rejects_self_consistent_superset_audit_baseline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    state: str,
) -> None:
    fixture = _manifest_fixture(tmp_path, monkeypatch)
    _prepare_manifest(fixture)
    payload = json.loads(fixture["formal"].read_text(encoding="utf-8"))
    payload["metadata_audit_baseline"] = subject._audit_baseline(
        _audit({"kind": "preauthorized", "path": "docs/private/poisoned.local.json"})
    )
    poisoned = _canonical_document(payload, subject.MANIFEST_CANONICAL_FIELD)
    fixture["formal"].write_bytes(poisoned)
    fixture["formal"].chmod(0o600)
    generated_ns = subject._timestamp_ns(
        payload["generated_utc"],
        "manifest generated_utc",
    )
    os.utime(fixture["formal"], ns=(generated_ns, generated_ns))
    if state == "pending_only":
        fixture["formal"].replace(fixture["pending"])

    with pytest.raises(
        subject.ConfigLocatorReconciliationError,
        match="baseline drifted from current unfinished state",
    ):
        _prepare_manifest(fixture)

    if state == "existing":
        assert fixture["formal"].read_bytes() == poisoned
        assert not fixture["pending"].exists()
    else:
        assert not fixture["formal"].exists()
        assert fixture["pending"].read_bytes() == poisoned


def test_manifest_recursive_drift_fails_before_first_byte(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _manifest_fixture(tmp_path, monkeypatch)
    calls = 0

    def validate_pair(**_kwargs: object) -> dict[str, str]:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise subject.ConfigLocatorReconciliationError("late tracked drift")
        return dict(fixture["tracked"])

    monkeypatch.setattr(subject, "_validate_tracked_source_pair", validate_pair)
    with pytest.raises(subject.ConfigLocatorReconciliationError, match="late tracked drift"):
        _prepare_manifest(fixture)
    assert not fixture["formal"].exists()
    assert not fixture["pending"].exists()


@pytest.mark.parametrize("mutation", ["bytes", "mode", "hardlink"])
def test_execution_manifest_authority_rejects_exact_identity_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    fixture = _manifest_fixture(tmp_path, monkeypatch)
    _prepare_manifest(fixture)
    payload, binding = subject._load_manifest_for_execute(fixture["formal"])
    subject._validate_execution_immutable_authorities(
        manifest_path=fixture["formal"],
        expected_manifest=payload,
        expected_manifest_binding=binding,
        activation_path=Path(
            payload["transaction"]["predecessor"]["v6_activation_receipt"]["path"]
        ),
    )
    if mutation == "bytes":
        fixture["formal"].write_bytes(fixture["formal"].read_bytes() + b" ")
        fixture["formal"].chmod(0o600)
    elif mutation == "mode":
        fixture["formal"].chmod(0o644)
    else:
        fixture["formal"].with_name("manifest-second-link.json").hardlink_to(fixture["formal"])
    with pytest.raises(subject.ConfigLocatorReconciliationError):
        subject._validate_execution_immutable_authorities(
            manifest_path=fixture["formal"],
            expected_manifest=payload,
            expected_manifest_binding=binding,
            activation_path=Path(
                payload["transaction"]["predecessor"]["v6_activation_receipt"]["path"]
            ),
        )


def test_manifest_write_apis_reject_nonrecursive_before_any_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _manifest_fixture(tmp_path, monkeypatch)
    with pytest.raises(subject.ConfigLocatorReconciliationError, match="requires recursion"):
        subject.prepare_manifest(
            publisher_root=fixture["publisher_root"],
            metadata_repository_root=fixture["metadata_root"],
            active_config_source=fixture["active_source"],
            receipt_id=subject.MANIFEST_RECEIPT_ID,
            output_path=fixture["formal"],
            recursive=False,
        )
    assert not fixture["formal"].exists()
    assert not fixture["pending"].exists()


def test_manifest_wrong_pending_and_symlink_leaf_fail_without_final(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _manifest_fixture(tmp_path, monkeypatch)
    subject._ensure_private_directory(fixture["formal"].parent)
    _private_write(fixture["pending"], b"not-json\n")
    with pytest.raises(subject.ConfigLocatorReconciliationError):
        _prepare_manifest(fixture)
    assert not fixture["formal"].exists()
    assert fixture["pending"].read_bytes() == b"not-json\n"

    fixture["pending"].unlink()
    fixture["formal"].parent.rmdir()
    redirect = tmp_path / "redirect"
    redirect.mkdir()
    fixture["formal"].parent.symlink_to(redirect, target_is_directory=True)
    with pytest.raises(subject.ConfigLocatorReconciliationError, match="symlink"):
        _prepare_manifest(fixture)
    assert not (redirect / fixture["formal"].name).exists()


def test_manifest_rejects_overlapping_roots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _manifest_fixture(tmp_path, monkeypatch)
    _prepare_manifest(fixture)
    payload = json.loads(fixture["formal"].read_text(encoding="utf-8"))
    payload["metadata_repository_root"] = payload["publisher_root"]
    payload[subject.MANIFEST_CANONICAL_FIELD] = subject._document_sha(
        payload, subject.MANIFEST_CANONICAL_FIELD
    )
    raw = subject._render(payload)
    with pytest.raises(subject.ConfigLocatorReconciliationError, match="distinct"):
        subject._validate_manifest_payload(
            payload,
            fixture["formal"],
            raw,
            recursive=False,
        )


def test_create_only_pending_and_nlink2_recovery_and_wrong_pending(
    tmp_path: Path,
) -> None:
    private = tmp_path / "private"
    private.mkdir()
    private.chmod(0o700)
    path = private / "immutable.json"
    pending = subject._pending(path, "create")
    raw = b"exact\n"

    subject._write_new(pending, raw)
    subject._publish_create_only(path, raw)
    assert path.read_bytes() == raw
    assert path.stat().st_nlink == 1
    assert not pending.exists()

    os.link(path, pending)
    subject._publish_create_only(path, raw)
    assert path.stat().st_nlink == 1
    assert not pending.exists()

    path.unlink()
    subject._write_new(pending, b"wrong\n")
    with pytest.raises(subject.ConfigLocatorReconciliationError, match="bytes drifted"):
        subject._publish_create_only(path, raw)
    assert not path.exists()
    assert pending.read_bytes() == b"wrong\n"


@pytest.mark.parametrize("residue", [b"", b"expected-"])
def test_create_only_uncommitted_staging_recovers_only_under_lock(
    tmp_path: Path,
    residue: bytes,
) -> None:
    metadata = tmp_path / "metadata"
    private = metadata / "docs/private"
    private.mkdir(parents=True)
    private.chmod(0o700)
    path = private / "immutable.json"
    pending = subject._pending(path, "create")
    staging = subject._staging_path(path, "create", "0" * 32)
    expected = b"expected-create-only-bytes\n"
    _private_write(staging, residue)

    with pytest.raises(subject.ConfigLocatorReconciliationError, match="transaction lock"):
        subject._validate_create_state(path, expected)
    assert staging.read_bytes() == residue

    locked = subject._open_transaction_lock(metadata)
    try:
        assert subject._validate_create_state(path, expected) == "staging_recoverable_uncommitted"
        assert staging.read_bytes() == residue
        subject._cleanup_uncommitted_staging(
            path,
            expected,
            pending_kind="create",
            staging_kind="create",
            expected_current=None,
        )
        subject._publish_create_only(path, expected)
    finally:
        subject._close_transaction_lock(locked)

    assert path.read_bytes() == expected
    assert path.stat().st_nlink == 1
    assert not pending.exists()
    assert not staging.exists()


@pytest.mark.parametrize(
    "residue",
    [
        b"",
        b"expected-",
        b"wrong-short-prefix",
        b"x" * len(b"expected-create-only-bytes\n"),
        b"expected-create-only-bytes\nextra",
    ],
)
def test_create_only_incomplete_deterministic_pending_remains_failclosed(
    tmp_path: Path,
    residue: bytes,
) -> None:
    metadata = tmp_path / "metadata"
    private = metadata / "docs/private"
    private.mkdir(parents=True)
    private.chmod(0o700)
    path = private / "immutable.json"
    pending = subject._pending(path, "create")
    expected = b"expected-create-only-bytes\n"
    _private_write(pending, residue)

    locked = subject._open_transaction_lock(metadata)
    try:
        with pytest.raises(subject.ConfigLocatorReconciliationError, match="bytes drifted"):
            subject._publish_create_only(path, expected)
    finally:
        subject._close_transaction_lock(locked)

    assert not path.exists()
    assert pending.read_bytes() == residue


@pytest.mark.parametrize(
    "residue",
    [
        b"wrong-short-prefix",
        b"x" * len(b"expected-create-only-bytes\n"),
        b"expected-create-only-bytes\nextra",
    ],
)
def test_create_only_mismatched_staging_remains_failclosed(
    tmp_path: Path,
    residue: bytes,
) -> None:
    metadata = tmp_path / "metadata"
    private = metadata / "docs/private"
    private.mkdir(parents=True)
    private.chmod(0o700)
    path = private / "immutable.json"
    staging = subject._staging_path(path, "create", "1" * 32)
    expected = b"expected-create-only-bytes\n"
    _private_write(staging, residue)

    locked = subject._open_transaction_lock(metadata)
    try:
        with pytest.raises(subject.ConfigLocatorReconciliationError, match="staging bytes drifted"):
            subject._publish_create_only(path, expected)
    finally:
        subject._close_transaction_lock(locked)

    assert not path.exists()
    assert not subject._pending(path, "create").exists()
    assert staging.read_bytes() == residue


@pytest.mark.parametrize("unsafe", ["mode", "hardlink"])
def test_create_only_unsafe_staging_is_never_recovered(
    tmp_path: Path,
    unsafe: str,
) -> None:
    metadata = tmp_path / "metadata"
    private = metadata / "docs/private"
    private.mkdir(parents=True)
    private.chmod(0o700)
    path = private / "immutable.json"
    staging = subject._staging_path(path, "create", "2" * 32)
    residue = b"expected-"
    expected = b"expected-create-only-bytes\n"
    _private_write(staging, residue)
    if unsafe == "mode":
        staging.chmod(0o644)
    else:
        os.link(staging, private / "attacker-second-link")

    locked = subject._open_transaction_lock(metadata)
    try:
        with pytest.raises(subject.ConfigLocatorReconciliationError, match="unsafe file"):
            subject._publish_create_only(path, expected)
    finally:
        subject._close_transaction_lock(locked)

    assert not path.exists()
    assert staging.read_bytes() == residue


def test_existing_create_only_final_never_cleans_uncommitted_staging(
    tmp_path: Path,
) -> None:
    metadata = tmp_path / "metadata"
    private = metadata / "docs/private"
    private.mkdir(parents=True)
    private.chmod(0o700)
    path = private / "immutable.json"
    staging = subject._staging_path(path, "create", "3" * 32)
    expected = b"expected-create-only-bytes\n"
    residue = expected[:9]
    _private_write(path, expected)
    _private_write(staging, residue)

    locked = subject._open_transaction_lock(metadata)
    try:
        with pytest.raises(subject.ConfigLocatorReconciliationError, match="ambiguous staging"):
            subject._publish_create_only(path, expected)
    finally:
        subject._close_transaction_lock(locked)

    assert path.read_bytes() == expected
    assert staging.read_bytes() == residue


def test_mutable_replace_uncommitted_staging_recovers_exact_predecessor(
    tmp_path: Path,
) -> None:
    metadata = tmp_path / "metadata"
    private = metadata / "docs/private"
    private.mkdir(parents=True)
    private.chmod(0o700)
    path = private / "mutable.json"
    pending = subject._pending(path, "alias")
    staging = subject._staging_path(path, "alias", "4" * 32)
    predecessor = b"predecessor\n"
    successor = b"successor-release-v3\n"
    residue = successor[:7]
    _private_write(path, predecessor)
    _private_write(staging, residue)

    locked = subject._open_transaction_lock(metadata)
    try:
        assert (
            subject._validate_replace_pending(
                path,
                successor,
                "alias",
                target_new=False,
            )
            == "staging_recoverable_uncommitted"
        )
        assert staging.read_bytes() == residue
        subject._cleanup_uncommitted_staging(
            path,
            successor,
            pending_kind="alias",
            staging_kind="alias",
            expected_current=predecessor,
        )
        subject._atomic_replace(
            path,
            successor,
            kind="alias",
            expected_current=predecessor,
        )
    finally:
        subject._close_transaction_lock(locked)

    assert path.read_bytes() == successor
    assert path.stat().st_nlink == 1
    assert not pending.exists()
    assert not staging.exists()


@pytest.mark.parametrize("publication", ["create", "replace"])
def test_completed_staging_link_transfer_resumes_without_rewriting_pending(
    tmp_path: Path,
    publication: str,
) -> None:
    metadata = tmp_path / "metadata"
    private = metadata / "docs/private"
    private.mkdir(parents=True)
    private.chmod(0o700)
    path = private / "target.json"
    kind = "create" if publication == "create" else "alias"
    pending = subject._pending(path, kind)
    staging = subject._staging_path(path, kind, "6" * 32)
    predecessor = b"predecessor\n"
    successor = b"successor-release-v3\n"
    if publication == "replace":
        _private_write(path, predecessor)
    _private_write(staging, successor)
    os.link(staging, pending)
    assert staging.stat().st_nlink == 2

    locked = subject._open_transaction_lock(metadata)
    try:
        if publication == "create":
            assert (
                subject._validate_create_state(path, successor) == "pending_staging_transfer_nlink2"
            )
            with pytest.raises(
                subject.ConfigLocatorReconciliationError,
                match="fresh transaction restart",
            ):
                subject._publish_create_only(path, successor)
            assert staging.stat().st_nlink == 2
            subject._recover_staging_transfer(
                path,
                successor,
                pending_kind="create",
                staging_kind="create",
                expected_current=None,
            )
            subject._publish_create_only(path, successor)
        else:
            assert (
                subject._validate_replace_pending(
                    path,
                    successor,
                    "alias",
                    target_new=False,
                )
                == "pending_staging_transfer_nlink2"
            )
            with pytest.raises(
                subject.ConfigLocatorReconciliationError,
                match="fresh transaction restart",
            ):
                subject._atomic_replace(
                    path,
                    successor,
                    kind="alias",
                    expected_current=predecessor,
                )
            assert staging.stat().st_nlink == 2
            subject._recover_staging_transfer(
                path,
                successor,
                pending_kind="alias",
                staging_kind="alias",
                expected_current=predecessor,
            )
            subject._atomic_replace(
                path,
                successor,
                kind="alias",
                expected_current=predecessor,
            )
    finally:
        subject._close_transaction_lock(locked)

    assert path.read_bytes() == successor
    assert path.stat().st_nlink == 1
    assert not pending.exists()
    assert not staging.exists()


@pytest.mark.parametrize(
    "state",
    ["unsafe_mode", "hardlink", "successor_final", "deterministic_partial"],
)
def test_mutable_replace_unsafe_or_published_state_preserves_uncommitted_bytes(
    tmp_path: Path,
    state: str,
) -> None:
    metadata = tmp_path / "metadata"
    private = metadata / "docs/private"
    private.mkdir(parents=True)
    private.chmod(0o700)
    path = private / "mutable.json"
    pending = subject._pending(path, "alias")
    staging = subject._staging_path(path, "alias", "5" * 32)
    predecessor = b"predecessor\n"
    successor = b"successor-release-v3\n"
    target = successor if state == "successor_final" else predecessor
    residue = successor[:7]
    _private_write(path, target)
    candidate = pending if state == "deterministic_partial" else staging
    _private_write(candidate, residue)
    if state == "unsafe_mode":
        staging.chmod(0o640)
    elif state == "hardlink":
        os.link(staging, private / "attacker-second-link")

    locked = subject._open_transaction_lock(metadata)
    try:
        if state == "successor_final":
            with pytest.raises(subject.ConfigLocatorReconciliationError, match="after publication"):
                subject._validate_replace_pending(
                    path,
                    successor,
                    "alias",
                    target_new=True,
                )
            with pytest.raises(subject.ConfigLocatorReconciliationError, match="bytes drifted"):
                subject._atomic_replace(
                    path,
                    successor,
                    kind="alias",
                    expected_current=predecessor,
                )
        else:
            message = "bytes drifted" if state == "deterministic_partial" else "unsafe file"
            with pytest.raises(subject.ConfigLocatorReconciliationError, match=message):
                subject._atomic_replace(
                    path,
                    successor,
                    kind="alias",
                    expected_current=predecessor,
                )
    finally:
        subject._close_transaction_lock(locked)

    assert path.read_bytes() == target
    assert candidate.read_bytes() == residue


def test_secure_reader_rejects_mode_hardlink_and_symlink_components(
    tmp_path: Path,
) -> None:
    private = tmp_path / "private"
    private.mkdir()
    private.chmod(0o700)
    exact = private / "exact.json"
    _private_write(exact, b"{}\n")
    assert subject._read_regular(exact)[0] == b"{}\n"

    exact.chmod(0o644)
    with pytest.raises(subject.ConfigLocatorReconciliationError, match="unsafe file"):
        subject._read_regular(exact)
    exact.chmod(0o600)

    hardlink = private / "hardlink.json"
    os.link(exact, hardlink)
    with pytest.raises(subject.ConfigLocatorReconciliationError, match="unsafe file"):
        subject._read_regular(exact)
    hardlink.unlink()

    final_symlink = private / "final-symlink.json"
    final_symlink.symlink_to(exact)
    with pytest.raises(subject.ConfigLocatorReconciliationError, match="unsafe file"):
        subject._read_regular(final_symlink)

    ancestor = tmp_path / "ancestor-link"
    ancestor.symlink_to(private, target_is_directory=True)
    with pytest.raises(subject.ConfigLocatorReconciliationError, match="contains a symlink"):
        subject._read_regular(ancestor / exact.name)


def test_transaction_flock_blocks_competing_descriptor(tmp_path: Path) -> None:
    metadata = tmp_path / "metadata"
    private = metadata / "docs/private"
    private.mkdir(parents=True)
    private.chmod(0o700)
    locked = subject._open_transaction_lock(metadata)
    competing = os.open(private, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(BlockingIOError):
            fcntl.flock(competing, fcntl.LOCK_EX | fcntl.LOCK_NB)
    finally:
        os.close(competing)
        subject._close_transaction_lock(locked)


def test_dry_run_is_write_free_and_apply_is_exact_and_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _transaction_fixture(tmp_path, monkeypatch)
    private = fixture["private"]
    before = {path.name: path.read_bytes() for path in private.iterdir() if path.is_file()}

    planned = subject.execute(
        fixture["manifest_path"],
        audit_fn=fixture["audit"],
    )

    assert planned["mode"] == "dry_run"
    assert planned["writes_performed"] is False
    assert planned["state_before"]["stable_alias"] == "predecessor"
    assert planned["state_before"]["immutable"] == {
        "backtest_archive": "missing",
        "release_v3_config": "missing",
        "pointer_snapshot": "missing",
        "catalog_snapshot": "missing",
    }
    assert before == {path.name: path.read_bytes() for path in private.iterdir() if path.is_file()}

    applied = subject.execute(
        fixture["manifest_path"],
        apply=True,
        audit_fn=fixture["audit"],
    )
    assert applied["status"] == "completed_exact_transaction"
    assert applied["transaction_committed"] is True
    assert (private / subject.CURRENT_ALIAS_FILENAME).read_bytes() == fixture["active_config"]
    for name in (
        subject.BACKTEST_V12_ARCHIVE_FILENAME,
        subject.RELEASE_V3_CONFIG_FILENAME,
        subject.PREDECESSOR_POINTER_SNAPSHOT_FILENAME,
        subject.PREDECESSOR_CATALOG_SNAPSHOT_FILENAME,
        subject.RECEIPT_FILENAME,
    ):
        path = private / name
        assert path.is_file()
        assert path.stat().st_mode & 0o777 == 0o600
        assert path.stat().st_nlink == 1
    activation_path = fixture["activation_path"]
    assert activation_path.read_bytes() == fixture["activation_raw"]

    repeated = subject.execute(
        fixture["manifest_path"],
        apply=True,
        audit_fn=fixture["audit"],
    )
    assert repeated["status"] == "completed_exact_transaction"
    assert repeated["state_before"]["stable_alias"] == "successor"
    assert not list(private.glob(".*pending-config-reconciliation-v1"))
    assert not list(private.glob(".*-staging-*-uncommitted-config-reconciliation-v1"))


@pytest.mark.parametrize(
    ("role", "kind"),
    [
        ("backtest_archive", "create"),
        ("receipt", "create"),
        ("alias", "alias"),
        ("pointer", "pointer"),
        ("catalog", "catalog"),
    ],
)
def test_interrupted_staging_write_resumes_full_transaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    role: str,
    kind: str,
) -> None:
    fixture = _transaction_fixture(tmp_path, monkeypatch)
    paths = subject._paths(fixture["metadata_root"])
    pending = subject._pending(paths[role], kind)
    original_write_new = subject._write_new
    interrupted_data: bytes | None = None
    interrupted_path: Path | None = None

    def interrupt_selected_pending(path: Path, data: bytes) -> None:
        nonlocal interrupted_data, interrupted_path
        if subject._is_staging_path(path, paths[role], kind) and interrupted_data is None:
            interrupted_data = data
            interrupted_path = path
            original_write_new(path, data[: len(data) // 2])
            raise RuntimeError(f"interrupted-write-{role}")
        original_write_new(path, data)

    monkeypatch.setattr(subject, "_write_new", interrupt_selected_pending)
    with pytest.raises(RuntimeError, match=f"interrupted-write-{role}"):
        subject.execute(
            fixture["manifest_path"],
            apply=True,
            audit_fn=fixture["audit"],
        )
    monkeypatch.setattr(subject, "_write_new", original_write_new)

    assert interrupted_data is not None
    assert interrupted_path is not None
    assert not pending.exists()
    residue = interrupted_path.read_bytes()
    assert len(residue) < len(interrupted_data)
    assert interrupted_data.startswith(residue)
    if kind == "create":
        assert not paths[role].exists()
    else:
        assert paths[role].exists()

    pending_metadata = interrupted_path.stat()
    before_dry_run = (
        residue,
        pending_metadata.st_dev,
        pending_metadata.st_ino,
        pending_metadata.st_mode,
        pending_metadata.st_uid,
        pending_metadata.st_nlink,
        pending_metadata.st_size,
        pending_metadata.st_mtime_ns,
        pending_metadata.st_ctime_ns,
    )
    planned = subject.execute(
        fixture["manifest_path"],
        audit_fn=fixture["audit"],
    )
    if role in {
        "backtest_archive",
        "release_v3_config",
        "pointer_snapshot",
        "catalog_snapshot",
    }:
        observed_state = planned["state_before"]["immutable"][role]
    elif role == "receipt":
        observed_state = planned["state_before"]["receipt"]
    else:
        observed_state = planned["state_before"]["pending"][role]
    assert observed_state == "staging_recoverable_uncommitted"
    pending_metadata = interrupted_path.stat()
    after_dry_run = (
        interrupted_path.read_bytes(),
        pending_metadata.st_dev,
        pending_metadata.st_ino,
        pending_metadata.st_mode,
        pending_metadata.st_uid,
        pending_metadata.st_nlink,
        pending_metadata.st_size,
        pending_metadata.st_mtime_ns,
        pending_metadata.st_ctime_ns,
    )
    assert after_dry_run == before_dry_run

    resumed = subject.execute(
        fixture["manifest_path"],
        apply=True,
        audit_fn=fixture["audit"],
    )
    assert resumed["transaction_committed"] is True
    assert not pending.exists()
    assert not interrupted_path.exists()


@pytest.mark.parametrize(("role", "kind"), [("backtest_archive", "create"), ("alias", "alias")])
def test_staging_cleanup_forces_fresh_source_gate_before_any_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    role: str,
    kind: str,
) -> None:
    fixture = _transaction_fixture(tmp_path, monkeypatch)
    paths = subject._paths(fixture["metadata_root"])
    original_write_new = subject._write_new
    interrupted_path: Path | None = None

    def interrupt_selected_staging(path: Path, data: bytes) -> None:
        nonlocal interrupted_path
        if subject._is_staging_path(path, paths[role], kind) and interrupted_path is None:
            interrupted_path = path
            original_write_new(path, data[: len(data) // 2])
            raise RuntimeError(f"interrupted-write-{role}")
        original_write_new(path, data)

    monkeypatch.setattr(subject, "_write_new", interrupt_selected_staging)
    with pytest.raises(RuntimeError, match=f"interrupted-write-{role}"):
        subject.execute(
            fixture["manifest_path"],
            apply=True,
            audit_fn=fixture["audit"],
        )
    monkeypatch.setattr(subject, "_write_new", original_write_new)
    assert interrupted_path is not None
    official_before = {
        path.name: path.read_bytes()
        for path in fixture["private"].iterdir()
        if path.is_file() and path != interrupted_path
    }

    original_preflight = subject._cleanup_execution_staging_preflight
    source_drifted = False

    def cleanup_then_drift_source(manifest_path: Path) -> bool:
        nonlocal source_drifted
        cleaned = original_preflight(manifest_path)
        if cleaned and not source_drifted:
            source_drifted = True
            _private_write(subject._active_config_source_path(), b"drift-after-cleanup\n")
        return cleaned

    monkeypatch.setattr(
        subject,
        "_cleanup_execution_staging_preflight",
        cleanup_then_drift_source,
    )
    with pytest.raises(
        subject.ConfigLocatorReconciliationError, match="frozen source bytes drifted"
    ):
        subject.execute(
            fixture["manifest_path"],
            apply=True,
            audit_fn=fixture["audit"],
        )

    assert source_drifted is True
    assert not interrupted_path.exists()
    assert official_before == {
        path.name: path.read_bytes() for path in fixture["private"].iterdir() if path.is_file()
    }


@pytest.mark.parametrize(
    "crash_after",
    [
        "backtest_archive",
        "release_v3_config",
        "pointer_snapshot",
        "catalog_snapshot",
        "receipt",
        "alias",
        "pointer",
        "catalog",
    ],
)
def test_each_completed_prefix_crash_resumes_without_rebaselining(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_after: str,
) -> None:
    fixture = _transaction_fixture(tmp_path, monkeypatch)

    def crash(step: str) -> None:
        if step == crash_after:
            raise RuntimeError(f"crash-after-{step}")

    with pytest.raises(RuntimeError, match=f"crash-after-{crash_after}"):
        subject.execute(
            fixture["manifest_path"],
            apply=True,
            audit_fn=fixture["audit"],
            failure_hook=crash,
        )

    resumed = subject.execute(
        fixture["manifest_path"],
        apply=True,
        audit_fn=fixture["audit"],
    )
    assert resumed["transaction_committed"] is True
    receipt = json.loads(
        (fixture["private"] / subject.RECEIPT_FILENAME).read_text(encoding="utf-8")
    )
    assert receipt["metadata_audit_baseline"] == fixture["manifest"]["metadata_audit_baseline"]


@pytest.mark.parametrize(
    ("create_states", "receipt_state", "mutable", "pending"),
    [
        (
            {
                "backtest_archive": "missing",
                "release_v3_config": "published_nlink1",
                "pointer_snapshot": "missing",
                "catalog_snapshot": "missing",
            },
            "missing",
            (False, False, False),
            {"alias": "absent", "pointer": "absent", "catalog": "absent"},
        ),
        (
            {
                "backtest_archive": "pending_create_only",
                "release_v3_config": "pending_create_only",
                "pointer_snapshot": "missing",
                "catalog_snapshot": "missing",
            },
            "missing",
            (False, False, False),
            {"alias": "absent", "pointer": "absent", "catalog": "absent"},
        ),
        (
            {
                "backtest_archive": "published_nlink1",
                "release_v3_config": "published_nlink1",
                "pointer_snapshot": "published_nlink1",
                "catalog_snapshot": "missing",
            },
            "missing",
            (True, False, False),
            {"alias": "absent", "pointer": "absent", "catalog": "absent"},
        ),
    ],
)
def test_impossible_transaction_prefixes_fail_closed(
    create_states: dict[str, str],
    receipt_state: str,
    mutable: tuple[bool, bool, bool],
    pending: dict[str, str],
) -> None:
    with pytest.raises(subject.ConfigLocatorReconciliationError, match="prefix|precede"):
        subject._validate_transaction_prefix(
            immutable_states=create_states,
            receipt_state=receipt_state,
            alias_new=mutable[0],
            pointer_new=mutable[1],
            catalog_new=mutable[2],
            replace_pending=pending,
        )


def test_prewrite_new_finding_blocks_all_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _transaction_fixture(tmp_path, monkeypatch)
    private = fixture["private"]
    before = {path.name: path.read_bytes() for path in private.iterdir() if path.is_file()}

    with pytest.raises(
        subject.ConfigLocatorReconciliationError,
        match="baseline drifted from current unfinished state",
    ):
        subject.execute(
            fixture["manifest_path"],
            apply=True,
            audit_fn=lambda _root: _audit({"kind": "new", "path": "docs/private"}),
        )

    assert before == {path.name: path.read_bytes() for path in private.iterdir() if path.is_file()}
    assert not (private / subject.RECEIPT_FILENAME).exists()


def test_execute_rejects_self_consistent_superset_manifest_baseline_before_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _transaction_fixture(tmp_path, monkeypatch)
    fixture["manifest"]["metadata_audit_baseline"] = subject._audit_baseline(
        _audit({"kind": "preauthorized", "path": "docs/private/poisoned.local.json"})
    )
    private = fixture["private"]
    before = {path.name: path.read_bytes() for path in private.iterdir() if path.is_file()}

    with pytest.raises(
        subject.ConfigLocatorReconciliationError,
        match="baseline drifted from current unfinished state",
    ):
        subject.execute(
            fixture["manifest_path"],
            apply=True,
            audit_fn=fixture["audit"],
        )

    assert before == {path.name: path.read_bytes() for path in private.iterdir() if path.is_file()}
    assert not (private / subject.RECEIPT_FILENAME).exists()


def test_full_candidate_failure_blocks_the_first_official_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _transaction_fixture(tmp_path, monkeypatch)
    private = fixture["private"]
    before = {path.name: path.read_bytes() for path in private.iterdir() if path.is_file()}

    def reject(**_kwargs: object) -> dict:
        raise subject.ConfigLocatorReconciliationError("candidate introduced 1 finding")

    monkeypatch.setattr(subject, "_full_candidate_owner_root_audit", reject)
    with pytest.raises(subject.ConfigLocatorReconciliationError, match="candidate introduced"):
        subject.execute(
            fixture["manifest_path"],
            apply=True,
            audit_fn=fixture["audit"],
        )

    assert before == {path.name: path.read_bytes() for path in private.iterdir() if path.is_file()}
    assert not (private / subject.RECEIPT_FILENAME).exists()


def test_fresh_real_root_audit_after_candidate_blocks_first_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _transaction_fixture(tmp_path, monkeypatch)
    calls = 0

    def audit_fn(_root: Path) -> dict:
        nonlocal calls
        calls += 1
        return _audit() if calls == 1 else _audit({"kind": "late", "path": "docs/private"})

    with pytest.raises(
        subject.ConfigLocatorReconciliationError,
        match="baseline drifted from current unfinished state",
    ):
        subject.execute(
            fixture["manifest_path"],
            apply=True,
            audit_fn=audit_fn,
        )
    assert calls == 2
    assert not (fixture["private"] / subject.RECEIPT_FILENAME).exists()


def test_activation_drift_during_candidate_blocks_first_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _transaction_fixture(tmp_path, monkeypatch)
    activation = fixture["activation_path"]

    def mutate_activation(**_kwargs: object) -> dict:
        activation.write_bytes(b"drifted-after-candidate\n")
        activation.chmod(0o600)
        return {
            "status": "full_metadata_only_candidate_owner_root_audit_passed",
            "new_finding_count": 0,
            "passed": True,
        }

    def validate_authorities(**_kwargs: object) -> dict:
        subject._validate_current_activation_crossbinding(fixture["metadata_root"], activation)
        return {"private_evidence_layout_exact": True}

    monkeypatch.setattr(subject, "_full_candidate_owner_root_audit", mutate_activation)
    monkeypatch.setattr(
        subject,
        "_validate_execution_immutable_authorities",
        validate_authorities,
    )
    with pytest.raises(subject.ConfigLocatorReconciliationError):
        subject.execute(
            fixture["manifest_path"],
            apply=True,
            audit_fn=fixture["audit"],
        )
    assert not (fixture["private"] / subject.RECEIPT_FILENAME).exists()


def test_post_commit_audit_error_raises_committed_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _transaction_fixture(tmp_path, monkeypatch)
    calls = 0

    def audit_fn(_root: Path) -> dict:
        nonlocal calls
        calls += 1
        if calls <= 2:
            return _audit()
        raise RuntimeError("post-audit-unavailable")

    with pytest.raises(subject.CommittedPostAuditError) as raised:
        subject.execute(
            fixture["manifest_path"],
            apply=True,
            audit_fn=audit_fn,
        )

    result = raised.value.result
    assert result["transaction_committed"] is True
    assert result["status"] == ("committed_exact_transaction_with_post_audit_diagnostic_error")
    assert result["metadata_audit"]["diagnostic_error_type"] == "RuntimeError"

    repeated = subject.execute(
        fixture["manifest_path"],
        apply=True,
        audit_fn=fixture["audit"],
    )
    assert repeated["status"] == "completed_exact_transaction"


def test_post_commit_new_finding_raises_committed_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _transaction_fixture(tmp_path, monkeypatch)
    calls = 0

    def audit_fn(_root: Path) -> dict:
        nonlocal calls
        calls += 1
        if calls <= 2:
            return _audit()
        return _audit({"kind": "post", "path": "docs/private"})

    with pytest.raises(subject.CommittedPostAuditError) as raised:
        subject.execute(
            fixture["manifest_path"],
            apply=True,
            audit_fn=audit_fn,
        )

    assert raised.value.result["transaction_committed"] is True
    assert raised.value.result["status"] == (
        "committed_exact_transaction_with_unattributed_post_audit_drift"
    )
    assert raised.value.result["metadata_audit"]["new_finding_count"] == 1


def test_metadata_tracked_source_is_rechecked_before_and_after_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _transaction_fixture(tmp_path, monkeypatch)
    calls = 0

    def validate_source(_manifest: object) -> None:
        nonlocal calls
        calls += 1
        if calls == 3:
            raise subject.ConfigLocatorReconciliationError("post tracked drift")

    monkeypatch.setattr(subject, "_validate_execution_source_identity", validate_source)
    with pytest.raises(subject.CommittedPostAuditError) as raised:
        subject.execute(
            fixture["manifest_path"],
            apply=True,
            audit_fn=fixture["audit"],
        )

    assert calls == 3
    assert raised.value.result["transaction_committed"] is True
    assert raised.value.result["post_write_source_identity"] == {
        "passed": False,
        "diagnostic_error_type": "ConfigLocatorReconciliationError",
        "diagnostic_error_message": "post tracked drift",
    }
    assert raised.value.result["status"] == (
        "committed_exact_transaction_with_post_source_identity_diagnostic_error"
    )


def test_manifest_or_activation_post_write_drift_is_committed_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _transaction_fixture(tmp_path, monkeypatch)
    calls = 0

    def validate_authorities(**_kwargs: object) -> dict:
        nonlocal calls
        calls += 1
        if calls == 3:
            raise subject.ConfigLocatorReconciliationError("post immutable authority drift")
        return {"private_evidence_layout_exact": True}

    monkeypatch.setattr(
        subject,
        "_validate_execution_immutable_authorities",
        validate_authorities,
    )
    with pytest.raises(subject.CommittedPostAuditError) as raised:
        subject.execute(
            fixture["manifest_path"],
            apply=True,
            audit_fn=fixture["audit"],
        )
    assert calls == 3
    assert raised.value.result["transaction_committed"] is True
    assert raised.value.result["post_write_source_identity"] == {
        "passed": False,
        "diagnostic_error_type": "ConfigLocatorReconciliationError",
        "diagnostic_error_message": "post immutable authority drift",
    }


def test_cli_returns_nonzero_and_prints_committed_diagnostics(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    diagnostic = {
        "status": "committed_exact_transaction_with_post_audit_diagnostic_error",
        "transaction_committed": True,
    }

    def committed(*_args: object, **_kwargs: object) -> dict:
        raise subject.CommittedPostAuditError(diagnostic)

    monkeypatch.setattr(subject, "execute", committed)
    assert subject.main(["run", "--manifest", "/not/read", "--apply"]) == 3
    assert json.loads(capsys.readouterr().out) == diagnostic


def _init_candidate_repo(path: Path) -> str:
    subprocess.run(["git", "init", "-q"], cwd=path, check=True)
    (path / ".gitignore").write_text("docs/private/\nmodels/private/\n", encoding="utf-8")
    for relative in (
        "docs/public_machine_document_projections.json",
        "research/public_machine_document_projections.json",
    ):
        target = path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(
                {
                    "schema_version": subject.audit_private_evidence.PROJECTION_SCHEMA,
                    "entries": [],
                }
            )
            + "\n",
            encoding="utf-8",
        )
    auditor = path / "scripts/audit_private_evidence.py"
    auditor.parent.mkdir(parents=True, exist_ok=True)
    auditor.write_bytes(Path(subject.audit_private_evidence.__file__).read_bytes())
    subprocess.run(["git", "add", "."], cwd=path, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "-qm",
            "candidate",
        ],
        cwd=path,
        check=True,
    )
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=path,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def test_full_candidate_owner_root_audit_runs_real_auditor_without_payload_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publisher = tmp_path / "publisher"
    metadata = tmp_path / "metadata"
    publisher.mkdir()
    metadata.mkdir()
    commit = _init_candidate_repo(publisher)
    _init_candidate_repo(metadata)
    monkeypatch.setattr(
        subject.audit_private_evidence,
        "PRIVATE_OWNER_ROOTS",
        (Path("docs/private"),),
    )
    publisher_auditor = publisher / "scripts/audit_private_evidence.py"
    monkeypatch.setattr(
        subject.audit_private_evidence,
        "__file__",
        str(publisher_auditor),
    )
    audit_contract = {
        "auditor_module": "scripts.audit_private_evidence",
        "auditor_source_sha256": hashlib.sha256(publisher_auditor.read_bytes()).hexdigest(),
        "required_new_finding_count": 0,
    }
    private = metadata / "docs/private"
    private.mkdir(parents=True)
    private.chmod(0o700)
    marker = private / "README.local.md"
    _private_write(marker, "Local only — do not publish.\n".encode())
    catalog = private / "catalog.current.local.json"
    catalog_payload = {
        "schema_version": subject.audit_private_evidence.CATALOG_SCHEMA,
        "visibility": "local_only_do_not_publish",
        "unit_id": "repository",
        "entries": [],
    }
    _private_write(catalog, subject._render(catalog_payload))
    nested = private / "nested/catalog.current.local.json"
    _private_write(nested, b"LOCKED-PAYLOAD-MUST-NOT-BE-READ")
    nonpublished = metadata / subject.audit_private_evidence.NONPUBLISHED_INDEX
    _private_write(
        nonpublished,
        subject._render(
            {
                "schema_version": subject.audit_private_evidence.NONPUBLISHED_SCHEMA,
                "entries": [],
            }
        ),
    )
    baseline_audit = subject.audit_private_evidence.audit(metadata)
    baseline = subject._audit_baseline(baseline_audit)
    planned_catalog_payload = dict(catalog_payload)
    planned_catalog_payload["entries"] = [
        {
            "artifact_id": "planned",
            "local_path": str(private / "planned.json"),
            "panel_role": "operational",
            "read_gate": "owner_authorized_locator_resolution_only",
        }
    ]
    planned_catalog = subject._render(planned_catalog_payload)
    original_read_regular = subject._read_regular

    def guarded_read(path: Path, **kwargs: object) -> tuple[bytes, os.stat_result]:
        if subject._absolute(path) == subject._absolute(nested):
            raise AssertionError("nested basename lookalike payload was read")
        return original_read_regular(path, **kwargs)

    monkeypatch.setattr(subject, "_read_regular", guarded_read)

    result = subject._full_candidate_owner_root_audit(
        metadata_root=metadata,
        publisher_root=publisher,
        publisher_source={"commit": commit},
        audit_baseline=baseline,
        audit_contract=audit_contract,
        planned_files={
            "catalog": (catalog, planned_catalog),
            "planned": (private / "planned.json", b"planned\n"),
        },
    )

    assert result["passed"] is True
    assert result["new_finding_count"] == 0
    assert result["payload_files_opened"] == 0
    assert result["catalog_path_remapping_enabled"] is True
    assert nested.read_bytes() == b"LOCKED-PAYLOAD-MUST-NOT-BE-READ"

    invalid_catalog_payload = dict(planned_catalog_payload)
    invalid_catalog_payload["entries"] = [dict(planned_catalog_payload["entries"][0])]
    invalid_catalog_payload["entries"][0]["read_gate"] = "unknown_candidate_gate"
    with pytest.raises(subject.ConfigLocatorReconciliationError, match="introduced 1"):
        subject._full_candidate_owner_root_audit(
            metadata_root=metadata,
            publisher_root=publisher,
            publisher_source={"commit": commit},
            audit_baseline=baseline,
            audit_contract=audit_contract,
            planned_files={
                "catalog": (catalog, subject._render(invalid_catalog_payload)),
                "planned": (private / "planned.json", b"planned\n"),
            },
        )


def test_candidate_audit_rejects_loaded_auditor_origin_or_source_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publisher = tmp_path / "publisher"
    publisher.mkdir()
    commit = _init_candidate_repo(publisher)
    publisher_auditor = publisher / "scripts/audit_private_evidence.py"
    contract = {
        "auditor_module": "scripts.audit_private_evidence",
        "auditor_source_sha256": hashlib.sha256(publisher_auditor.read_bytes()).hexdigest(),
        "required_new_finding_count": 0,
    }
    monkeypatch.setattr(
        subject.audit_private_evidence,
        "__file__",
        str(tmp_path / "mixed-checkout/audit_private_evidence.py"),
    )
    with pytest.raises(subject.ConfigLocatorReconciliationError, match="origin drifted"):
        subject._validate_candidate_auditor_identity(
            publisher_root=publisher,
            publisher_source={"commit": commit},
            audit_contract=contract,
        )

    monkeypatch.setattr(
        subject.audit_private_evidence,
        "__file__",
        str(publisher_auditor),
    )
    drifted = dict(contract)
    drifted["auditor_source_sha256"] = "0" * 64
    with pytest.raises(subject.ConfigLocatorReconciliationError, match="source identity"):
        subject._validate_candidate_auditor_identity(
            publisher_root=publisher,
            publisher_source={"commit": commit},
            audit_contract=drifted,
        )


def test_candidate_skeleton_rejects_owner_root_inode_swap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metadata = tmp_path / "metadata"
    candidate = tmp_path / "candidate"
    private = metadata / "docs/private"
    private.mkdir(parents=True)
    private.chmod(0o700)
    candidate.mkdir()
    monkeypatch.setattr(
        subject.audit_private_evidence,
        "PRIVATE_OWNER_ROOTS",
        (Path("docs/private"),),
    )
    original_walk = os.walk
    swapped = False

    def swapping_walk(path: object, *args: object, **kwargs: object):
        nonlocal swapped
        target = Path(os.fspath(path))
        if not swapped and target == private:
            swapped = True
            target.rename(target.with_name("private-before-swap"))
            target.mkdir()
            target.chmod(0o700)
        return original_walk(path, *args, **kwargs)

    monkeypatch.setattr(subject.os, "walk", swapping_walk)
    locked = subject._open_transaction_lock(metadata)
    try:
        with pytest.raises(subject.ConfigLocatorReconciliationError, match="changed|drifted"):
            subject._copy_private_metadata_skeleton(metadata, candidate)
    finally:
        subject._close_transaction_lock(locked)
    assert swapped is True


def test_candidate_skeleton_rejects_nested_topology_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metadata = tmp_path / "metadata"
    candidate = tmp_path / "candidate"
    private = metadata / "docs/private"
    nested = private / "nested"
    nested.mkdir(parents=True)
    private.chmod(0o700)
    nested.chmod(0o700)
    candidate.mkdir()
    first = nested / "a.local.json"
    second = nested / "b.local.json"
    _private_write(first, b"first\n")
    _private_write(second, b"second\n")
    monkeypatch.setattr(
        subject.audit_private_evidence,
        "PRIVATE_OWNER_ROOTS",
        (Path("docs/private"),),
    )
    original_write = subject._write_skeleton_file
    drifted = False

    def mutate_nested(
        source: Path,
        destination: Path,
        metadata_row: os.stat_result,
        **kwargs: object,
    ) -> None:
        nonlocal drifted
        original_write(source, destination, metadata_row, **kwargs)
        if not drifted and source == first:
            drifted = True
            second.write_bytes(b"second-drifted\n")
            second.chmod(0o600)

    monkeypatch.setattr(subject, "_write_skeleton_file", mutate_nested)
    with pytest.raises(subject.ConfigLocatorReconciliationError, match="topology drifted|changed"):
        subject._copy_private_metadata_skeleton(metadata, candidate)
    assert drifted is True


def test_candidate_governance_read_must_match_walk_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metadata = tmp_path / "metadata"
    candidate = tmp_path / "candidate"
    private = metadata / "docs/private"
    private.mkdir(parents=True)
    private.chmod(0o700)
    candidate.mkdir()
    marker = private / "README.local.md"
    _private_write(marker, b"Local only -- do not publish.\n")
    monkeypatch.setattr(
        subject.audit_private_evidence,
        "PRIVATE_OWNER_ROOTS",
        (Path("docs/private"),),
    )
    original_read = subject._read_regular
    drifted = False

    def mutate_before_read(path: Path, **kwargs: object) -> tuple[bytes, os.stat_result]:
        nonlocal drifted
        if not drifted and path == marker:
            drifted = True
            marker.write_bytes(b"changed after directory walk\n")
            marker.chmod(0o600)
        return original_read(path, **kwargs)

    monkeypatch.setattr(subject, "_read_regular", mutate_before_read)
    with pytest.raises(
        subject.ConfigLocatorReconciliationError,
        match="changed between walk and secure read",
    ):
        subject._copy_private_metadata_skeleton(metadata, candidate)
    assert drifted is True


def test_private_evidence_root_rejects_ephemeral_spellings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    filesystem_root = Path("/")
    for root in (
        filesystem_root / "tmp",
        filesystem_root / "private" / "tmp",
        filesystem_root / "private" / "var" / "tmp",
    ):
        monkeypatch.setenv(subject.PRIVATE_EVIDENCE_ROOT_ENV, str(root / "evidence"))
        with pytest.raises(subject.ConfigLocatorReconciliationError, match="non-ephemeral"):
            subject._private_evidence_root()


def test_private_evidence_root_and_unit_permissions_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    unrelated_temp = tmp_path / "declared-temp"
    unrelated_temp.mkdir()
    monkeypatch.setattr(subject.tempfile, "gettempdir", lambda: str(unrelated_temp))
    evidence = tmp_path / "durable-evidence"
    evidence.mkdir()
    monkeypatch.setenv(subject.PRIVATE_EVIDENCE_ROOT_ENV, str(evidence))
    evidence.chmod(0o770)
    with pytest.raises(subject.ConfigLocatorReconciliationError, match="writable"):
        subject._private_evidence_root()

    evidence.chmod(0o750)
    assert subject._private_evidence_root() == evidence

    fixture = _manifest_fixture(tmp_path / "layout", monkeypatch)
    fixture["active_source"].parent.chmod(0o750)
    with pytest.raises(subject.ConfigLocatorReconciliationError, match="owner-only"):
        _prepare_manifest(fixture)
    assert not fixture["formal"].exists()
    assert not fixture["pending"].exists()


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value.__setitem__("finding_count", 99),
        lambda value: value.__setitem__("finding_set_sha256", "0" * 64),
        lambda value: value["finding_fingerprints"].append(value["finding_fingerprints"][0]),
        lambda value: value["finding_fingerprints"].reverse(),
        lambda value: value.__setitem__("unexpected", True),
    ],
)
def test_manifest_audit_baseline_requires_exact_internal_identity(
    mutate: object,
) -> None:
    baseline = subject._audit_baseline(
        _audit(
            {"kind": "one", "path": "docs/private"},
            {"kind": "two", "path": "models/private"},
        )
    )
    mutation = mutate
    assert callable(mutation)
    mutation(baseline)
    with pytest.raises(subject.ConfigLocatorReconciliationError, match="baseline"):
        subject._validate_audit_baseline_exact(baseline)
