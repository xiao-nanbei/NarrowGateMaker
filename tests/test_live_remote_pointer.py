from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.live_remote_pointer import (
    LiveRemotePointerError,
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


def test_non_active_pointer_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
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
