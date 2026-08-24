from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from scripts.live_remote_pointer import (
    CURRENT_ACTIVATION_RECEIPT_CANONICAL_SHA256,
    CURRENT_ACTIVATION_RECEIPT_FILE_SHA256,
    CURRENT_RELEASE_CANONICAL_SHA256,
    CURRENT_RELEASE_FILE_SHA256,
    CURRENT_RUNTIME_COMMIT,
    CURRENT_RUNTIME_TAG_OBJECT,
    CURRENT_RUNTIME_TREE,
    LiveRemotePointerError,
    active_live_locator_fields,
    active_live_remote_fields,
    require_remote_matches_source,
)

SSH_USER = "ec2-user"
CURRENT_EXAMPLE_IPV4 = "192.0.2.10"
PREDECESSOR_EXAMPLE_IPV4 = "198.51.100.20"
OVERRIDE_EXAMPLE_IPV4 = "203.0.113.30"
CURRENT_EXAMPLE_SSH = f"{SSH_USER}@{CURRENT_EXAMPLE_IPV4}"
PREDECESSOR_EXAMPLE_SSH = f"{SSH_USER}@{PREDECESSOR_EXAMPLE_IPV4}"
OVERRIDE_EXAMPLE_SSH = f"{SSH_USER}@{OVERRIDE_EXAMPLE_IPV4}"
EXAMPLE_REPO_ROOT = "/srv/example-live/NarrowGate_BTCUSDC"


def _write_pointer(path: Path, *, status: str) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": "narrowgate_live_remote_pointer.v1",
                "status": status,
                "provider": "AWS",
                "region": "ap-northeast-1",
                "city": "Tokyo",
                "ssh_target": CURRENT_EXAMPLE_SSH,
                "public_ipv4": CURRENT_EXAMPLE_IPV4,
                "repo_root": EXAMPLE_REPO_ROOT,
            }
        ),
        encoding="utf-8",
    )
    path.chmod(0o600)


def _write_authority_pointer(
    path: Path,
    config: Path,
    *,
    config_sha256: str,
) -> None:
    payload = {
        "schema_version": "narrowgate_live_remote_pointer.v1",
        "status": "current_active",
        "provider": "AWS",
        "region": "ap-northeast-1",
        "city": "Tokyo",
        "ssh_target": CURRENT_EXAMPLE_SSH,
        "public_ipv4": CURRENT_EXAMPLE_IPV4,
        "repo_root": EXAMPLE_REPO_ROOT,
        "config_sha256": config_sha256,
        "current_activation_receipt": {
            "sha256": CURRENT_ACTIVATION_RECEIPT_FILE_SHA256,
            "canonical_sha256": CURRENT_ACTIVATION_RECEIPT_CANONICAL_SHA256,
        },
        "current_buy_e3_release": {
            "active_config_sha256": config_sha256,
            "active_release_file_sha256": CURRENT_RELEASE_FILE_SHA256,
            "active_release_canonical_sha256": CURRENT_RELEASE_CANONICAL_SHA256,
            "execution_commit": CURRENT_RUNTIME_COMMIT,
            "execution_tree": CURRENT_RUNTIME_TREE,
            "annotated_tag_object": CURRENT_RUNTIME_TAG_OBJECT,
            "external_venues_enabled": False,
            "global_flow_shadow_enabled": False,
            "global_reference_shadow_enabled": False,
        },
        "current_config_locator_reconciliation": {
            "status": "completed_release_v3_no_shadow_current_config_locator_reconciled",
            "immutable_v6_activation_receipt_preserved": True,
            "backtest_v12_config_may_resolve_to_live_alias": False,
            "stable_live_config_alias": {
                "path": str(config),
                "sha256": config_sha256,
                "bytes": config.stat().st_size,
            },
            "receipt": {"sha256": "a" * 64},
        },
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    path.chmod(0o600)


def test_active_pointer_resolves_one_host_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pointer = tmp_path / "pointer.json"
    _write_pointer(pointer, status="current_active")
    monkeypatch.setenv("NARROWGATE_LIVE_REMOTE_POINTER", str(pointer))
    monkeypatch.delenv("NARROWGATE_LIVE_REMOTE", raising=False)

    assert active_live_remote_fields(tmp_path) == {
        "ssh_target": CURRENT_EXAMPLE_SSH,
        "provider": "AWS",
        "region": "ap-northeast-1",
        "city": "Tokyo",
        "public_ipv4": CURRENT_EXAMPLE_IPV4,
        "repo_root": EXAMPLE_REPO_ROOT,
    }


def test_non_active_pointer_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pointer = tmp_path / "pointer.json"
    _write_pointer(pointer, status="migration_pending")
    monkeypatch.setenv("NARROWGATE_LIVE_REMOTE_POINTER", str(pointer))
    monkeypatch.delenv("NARROWGATE_LIVE_REMOTE", raising=False)

    assert active_live_remote_fields(tmp_path) == {}


def test_explicit_different_remote_does_not_inherit_pointer_provenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pointer = tmp_path / "pointer.json"
    _write_pointer(pointer, status="current_active")
    monkeypatch.setenv("NARROWGATE_LIVE_REMOTE_POINTER", str(pointer))
    monkeypatch.setenv("NARROWGATE_LIVE_REMOTE", OVERRIDE_EXAMPLE_SSH)

    assert active_live_remote_fields(tmp_path) == {"ssh_target": OVERRIDE_EXAMPLE_SSH}


def test_remote_source_mismatch_fails_closed() -> None:
    with pytest.raises(LiveRemotePointerError, match="mixed-host provenance"):
        require_remote_matches_source(
            CURRENT_EXAMPLE_SSH,
            {"ssh_target": PREDECESSOR_EXAMPLE_SSH},
        )


def test_active_live_locator_cross_binds_remote_release_and_alias(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = tmp_path / "live_config.current.local.yaml"
    config.write_bytes(b"active\n")
    config.chmod(0o600)
    monkeypatch.setattr(
        "scripts.live_remote_pointer.CURRENT_LIVE_CONFIG_SHA256",
        hashlib.sha256(config.read_bytes()).hexdigest(),
    )
    digest = hashlib.sha256(config.read_bytes()).hexdigest()
    pointer = tmp_path / "pointer.json"
    _write_authority_pointer(pointer, config, config_sha256=digest)
    monkeypatch.setenv("NARROWGATE_LIVE_REMOTE_POINTER", str(pointer))
    monkeypatch.setenv("NARROWGATE_LIVE_CONFIG", str(config))

    result = active_live_locator_fields(tmp_path)

    assert result["ssh_target"] == CURRENT_EXAMPLE_SSH
    assert result["live_config_path"] == str(config)
    assert result["live_config_sha256"] == digest
    assert result["resolution_scope"] == "locator_only_not_evidence_authority"


def test_active_live_locator_rejects_release_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = tmp_path / "live_config.current.local.yaml"
    config.write_bytes(b"active\n")
    config.chmod(0o600)
    digest = hashlib.sha256(config.read_bytes()).hexdigest()
    monkeypatch.setattr("scripts.live_remote_pointer.CURRENT_LIVE_CONFIG_SHA256", digest)
    pointer = tmp_path / "pointer.json"
    _write_authority_pointer(pointer, config, config_sha256=digest)
    monkeypatch.setenv("NARROWGATE_LIVE_REMOTE_POINTER", str(pointer))
    monkeypatch.setenv("NARROWGATE_LIVE_CONFIG", str(config))
    assert active_live_locator_fields(tmp_path)["live_config_sha256"] == digest
    payload = json.loads(pointer.read_text(encoding="utf-8"))
    payload["current_buy_e3_release"]["execution_tree"] = "0" * 40
    pointer.write_text(json.dumps(payload), encoding="utf-8")
    pointer.chmod(0o600)
    assert active_live_locator_fields(tmp_path) == {}


def test_live_pointer_and_alias_symlinks_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = tmp_path / "config.real.yaml"
    config.write_bytes(b"active\n")
    config.chmod(0o600)
    digest = hashlib.sha256(config.read_bytes()).hexdigest()
    monkeypatch.setattr("scripts.live_remote_pointer.CURRENT_LIVE_CONFIG_SHA256", digest)
    pointer = tmp_path / "pointer.real.json"
    _write_authority_pointer(pointer, config, config_sha256=digest)
    monkeypatch.setenv("NARROWGATE_LIVE_REMOTE_POINTER", str(pointer))
    monkeypatch.setenv("NARROWGATE_LIVE_CONFIG", str(config))
    assert active_live_locator_fields(tmp_path)["live_config_sha256"] == digest
    pointer_link = tmp_path / "pointer.json"
    pointer_link.symlink_to(pointer)
    monkeypatch.setenv("NARROWGATE_LIVE_REMOTE_POINTER", str(pointer_link))
    monkeypatch.setenv("NARROWGATE_LIVE_CONFIG", str(config))
    assert active_live_locator_fields(tmp_path) == {}

    config_link = tmp_path / "config.yaml"
    config_link.symlink_to(config)
    _write_authority_pointer(pointer, config_link, config_sha256=digest)
    monkeypatch.setenv("NARROWGATE_LIVE_REMOTE_POINTER", str(pointer))
    monkeypatch.setenv("NARROWGATE_LIVE_CONFIG", str(config_link))
    assert active_live_locator_fields(tmp_path) == {}


def test_live_pointer_and_alias_ancestor_symlinks_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = tmp_path / "real"
    real.mkdir()
    config = real / "live_config.current.local.yaml"
    config.write_bytes(b"active\n")
    config.chmod(0o600)
    digest = hashlib.sha256(config.read_bytes()).hexdigest()
    monkeypatch.setattr("scripts.live_remote_pointer.CURRENT_LIVE_CONFIG_SHA256", digest)
    pointer = real / "pointer.json"
    _write_authority_pointer(pointer, config, config_sha256=digest)
    monkeypatch.setenv("NARROWGATE_LIVE_REMOTE_POINTER", str(pointer))
    monkeypatch.setenv("NARROWGATE_LIVE_CONFIG", str(config))
    assert active_live_locator_fields(tmp_path)["live_config_sha256"] == digest

    linked_parent = tmp_path / "linked"
    linked_parent.symlink_to(real, target_is_directory=True)
    monkeypatch.setenv("NARROWGATE_LIVE_REMOTE_POINTER", str(linked_parent / pointer.name))
    assert active_live_locator_fields(tmp_path) == {}

    linked_config = linked_parent / config.name
    _write_authority_pointer(pointer, linked_config, config_sha256=digest)
    monkeypatch.setenv("NARROWGATE_LIVE_REMOTE_POINTER", str(pointer))
    monkeypatch.setenv("NARROWGATE_LIVE_CONFIG", str(linked_config))
    assert active_live_locator_fields(tmp_path) == {}


def test_live_alias_hardlink_and_mode_drift_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = tmp_path / "live_config.current.local.yaml"
    config.write_bytes(b"active\n")
    config.chmod(0o600)
    digest = hashlib.sha256(config.read_bytes()).hexdigest()
    monkeypatch.setattr("scripts.live_remote_pointer.CURRENT_LIVE_CONFIG_SHA256", digest)
    pointer = tmp_path / "pointer.json"
    _write_authority_pointer(pointer, config, config_sha256=digest)
    monkeypatch.setenv("NARROWGATE_LIVE_REMOTE_POINTER", str(pointer))
    monkeypatch.setenv("NARROWGATE_LIVE_CONFIG", str(config))

    assert active_live_locator_fields(tmp_path)["live_config_sha256"] == digest

    hardlink = tmp_path / "config.hardlink.yaml"
    hardlink.hardlink_to(config)
    assert active_live_locator_fields(tmp_path) == {}
    hardlink.unlink()
    config.chmod(0o644)
    assert active_live_locator_fields(tmp_path) == {}


@pytest.mark.parametrize(
    "raw",
    [
        (
            b'{"schema_version":"narrowgate_live_remote_pointer.v1",'
            b'"status":"current_active","status":"retired"}'
        ),
        (
            b'{"schema_version":"narrowgate_live_remote_pointer.v1",'
            b'"status":"current_active","nested":{"sha256":"a","sha256":"b"}}'
        ),
        b"\xff\xfe\x00",
    ],
)
def test_live_pointer_rejects_ambiguous_or_invalid_json(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    raw: bytes,
) -> None:
    pointer = tmp_path / "pointer.json"
    pointer.write_bytes(raw)
    pointer.chmod(0o600)
    monkeypatch.setenv("NARROWGATE_LIVE_REMOTE_POINTER", str(pointer))

    assert active_live_remote_fields(tmp_path) == {}


def test_live_pointer_rejects_oversize_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pointer = tmp_path / "pointer.json"
    _write_pointer(pointer, status="current_active")
    monkeypatch.setattr("scripts.live_remote_pointer.MAX_PRIVATE_AUTHORITY_BYTES", 8)
    monkeypatch.setenv("NARROWGATE_LIVE_REMOTE_POINTER", str(pointer))

    assert active_live_remote_fields(tmp_path) == {}
