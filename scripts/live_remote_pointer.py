"""Fail-closed resolver for the local current-live-host pointer."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any


POINTER_SCHEMA_VERSION = "narrowgate_live_remote_pointer.v1"
ACTIVE_STATUS = "current_active"


class LiveRemotePointerError(ValueError):
    """Raised when no unambiguous active deployment host can be resolved."""


def load_live_remote_pointer(root: Path) -> dict[str, Any]:
    pointer_path = Path(
        os.environ.get(
            "NARROWGATE_LIVE_REMOTE_POINTER",
            str(root / "docs/private/live_remote.current.local.json"),
        )
    ).expanduser()
    if not pointer_path.is_file():
        raise LiveRemotePointerError(
            "current live remote pointer is unavailable; pass --remote explicitly"
        )
    payload = json.loads(pointer_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise LiveRemotePointerError("current live remote pointer must be a JSON object")
    if payload.get("schema_version") != POINTER_SCHEMA_VERSION:
        raise LiveRemotePointerError("unsupported current live remote pointer schema")
    return payload


def active_live_remote_fields(root: Path) -> dict[str, str]:
    """Return one internally consistent active-host identity.

    An explicit ``NARROWGATE_LIVE_REMOTE`` is allowed for migration and recovery
    commands. Capture-source fields are not inferred from that single override;
    callers that need provenance must pass the corresponding source fields too.
    """

    explicit_remote = os.environ.get("NARROWGATE_LIVE_REMOTE", "").strip()
    try:
        payload = load_live_remote_pointer(root)
    except (OSError, json.JSONDecodeError, LiveRemotePointerError):
        if explicit_remote:
            return {"ssh_target": explicit_remote}
        return {}

    if payload.get("status") != ACTIVE_STATUS:
        if explicit_remote:
            return {"ssh_target": explicit_remote}
        return {}

    required = ("ssh_target", "provider", "region", "city", "public_ipv4")
    resolved = {field: str(payload.get(field, "")).strip() for field in required}
    if not all(resolved.values()):
        if explicit_remote:
            return {"ssh_target": explicit_remote}
        return {}
    if explicit_remote and explicit_remote != resolved["ssh_target"]:
        return {"ssh_target": explicit_remote}
    repo_root = str(payload.get("repo_root", "")).strip()
    if repo_root:
        resolved["repo_root"] = repo_root
    return resolved


def require_remote_matches_source(
    remote: str, source_identity: Mapping[str, Any]
) -> None:
    source_remote = str(source_identity.get("ssh_target", "")).strip()
    if not remote.strip():
        raise LiveRemotePointerError(
            "no current_active remote is resolved; pass --remote explicitly"
        )
    if source_remote != remote.strip():
        raise LiveRemotePointerError(
            "remote and capture source ssh_target differ; refusing mixed-host provenance"
        )
