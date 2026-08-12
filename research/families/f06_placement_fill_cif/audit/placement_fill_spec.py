"""Load immutable placement-fill specs with hash-locked inheritance."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from research.governance.public_machine_projection import (
    PublicMachineProjectionError,
    source_document_path,
    source_identity_sha256,
)


def _source_document(path: Path, *, role: str) -> Path:
    try:
        return source_document_path(path, require_private=False)
    except (OSError, PublicMachineProjectionError) as exc:
        raise RuntimeError(f"placement-fill {role} source document is unavailable") from exc


def _source_identity(path: Path, *, role: str) -> str:
    try:
        return source_identity_sha256(path)
    except (OSError, PublicMachineProjectionError) as exc:
        raise RuntimeError(f"placement-fill {role} source identity is unavailable") from exc


def _merge(base: Mapping[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], Mapping) and isinstance(value, Mapping):
            merged[key] = _merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_placement_fill_spec(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    source = _source_document(resolved, role="Spec")
    payload = json.loads(source.read_text(encoding="utf-8"))
    parent_name = payload.pop("extends", None)
    expected_hash = payload.pop("extends_sha256", None)
    if parent_name is None:
        return payload
    parent = Path(str(parent_name))
    if not parent.is_absolute():
        parent = resolved.parent / parent
    parent = parent.resolve()
    if expected_hash is None or _source_identity(parent, role="parent Spec") != str(expected_hash):
        raise RuntimeError("placement-fill parent spec identity changed")
    merged = _merge(load_placement_fill_spec(parent), payload)
    merged["resolved_spec_lineage"] = {
        "child": str(resolved),
        "parent": str(parent),
        "parent_sha256": str(expected_hash),
    }
    return merged
