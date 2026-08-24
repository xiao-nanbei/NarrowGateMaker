#!/usr/bin/env python3
"""Create the direct-owner no-shadow config pair without changing strategy semantics."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

SCHEMA = "f05_buy_e3_no_shadow_config_successor.v1"
STATUS = "no_shadow_config_pair_verified"
PARENT_DISABLED_SHA256 = "10158a92177cd87b77fdb24a2a477dcab4b41cfb29208cf96c19953edafe166f"
PARENT_ACTIVE_SHA256 = "ad153012b14e725a3ac24f0ddbe02bc353168a13ec827b777cc94761020524ec"
SHADOW_FIELDS = (
    "global_flow_shadow_enabled",
    "global_reference_shadow_enabled",
)
PAIR_ACTION_PATH = ("strategy", "buy_e3_cooldown_policy_enabled")


def canonical_sha256(payload: Mapping[str, Any], *, field: str) -> str:
    unsigned = dict(payload)
    unsigned.pop(field, None)
    raw = json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def file_identity(path: Path) -> dict[str, Any]:
    raw = path.read_bytes()
    return {
        "path": str(path.resolve()),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "size_bytes": len(raw),
        "mode": stat.S_IMODE(path.stat().st_mode),
    }


def _mapping(path: Path) -> dict[str, Any]:
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"config root must be a mapping: {path}")
    return value


def _without_path(value: Mapping[str, Any], path: tuple[str, ...]) -> dict[str, Any]:
    clone = json.loads(json.dumps(value))
    target = clone
    for name in path[:-1]:
        child = target.get(name)
        if not isinstance(child, dict):
            raise ValueError("config pair action path is missing")
        target = child
    if path[-1] not in target:
        raise ValueError("config pair action leaf is missing")
    del target[path[-1]]
    return clone


def _validate_parent_pair(disabled: Path, active: Path) -> tuple[dict, dict]:
    disabled_identity = file_identity(disabled)
    active_identity = file_identity(active)
    if disabled_identity["sha256"] != PARENT_DISABLED_SHA256:
        raise ValueError("parent disabled config SHA256 drifted")
    if active_identity["sha256"] != PARENT_ACTIVE_SHA256:
        raise ValueError("parent active config SHA256 drifted")
    disabled_cfg = _mapping(disabled)
    active_cfg = _mapping(active)
    if _without_path(disabled_cfg, PAIR_ACTION_PATH) != _without_path(active_cfg, PAIR_ACTION_PATH):
        raise ValueError("parent configs differ outside BUY E3 enabled bit")
    if disabled_cfg["strategy"][PAIR_ACTION_PATH[-1]] is not False:
        raise ValueError("parent disabled config has BUY E3 enabled")
    if active_cfg["strategy"][PAIR_ACTION_PATH[-1]] is not True:
        raise ValueError("parent active config has BUY E3 disabled")
    for cfg in (disabled_cfg, active_cfg):
        external = cfg.get("external_venues")
        if not isinstance(external, dict) or external.get("enabled") is not False:
            raise ValueError("parent config does not disable external venues")
        multi = cfg.get("multi_market")
        if not isinstance(multi, dict):
            raise ValueError("parent config has no multi_market mapping")
        if any(name in multi for name in SHADOW_FIELDS):
            raise ValueError("parent config already contains successor shadow fields")
    return disabled_cfg, active_cfg


def _insert_shadow_fields(raw: bytes) -> bytes:
    text = raw.decode("utf-8")
    lines = text.splitlines(keepends=True)
    starts = [index for index, line in enumerate(lines) if line.rstrip("\r\n") == "multi_market:"]
    if len(starts) != 1:
        raise ValueError("config must contain exactly one root multi_market mapping")
    start = starts[0]
    end = len(lines)
    for index in range(start + 1, len(lines)):
        stripped = lines[index].strip()
        if not stripped or not lines[index].startswith((" ", "\t")):
            end = index
            break
    newline = "\r\n" if any(line.endswith("\r\n") for line in lines) else "\n"
    insertion = [
        f"  global_flow_shadow_enabled: false{newline}",
        f"  global_reference_shadow_enabled: false{newline}",
    ]
    return "".join(lines[:end] + insertion + lines[end:]).encode("utf-8")


def _publish_create_only(path: Path, raw: bytes, *, mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() or path.is_symlink():
        raise FileExistsError(f"refusing to replace existing output: {path}")
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, mode)
        os.link(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)


def build_successor_pair(
    *,
    parent_disabled: Path,
    parent_active: Path,
    output_disabled: Path,
    output_active: Path,
) -> dict[str, Any]:
    parent_disabled_cfg, parent_active_cfg = _validate_parent_pair(parent_disabled, parent_active)
    disabled_raw = _insert_shadow_fields(parent_disabled.read_bytes())
    active_raw = _insert_shadow_fields(parent_active.read_bytes())
    disabled_cfg = yaml.safe_load(disabled_raw)
    active_cfg = yaml.safe_load(active_raw)
    for parent, successor in (
        (parent_disabled_cfg, disabled_cfg),
        (parent_active_cfg, active_cfg),
    ):
        projected = json.loads(json.dumps(successor))
        multi = projected["multi_market"]
        for name in SHADOW_FIELDS:
            if multi.pop(name, None) is not False:
                raise ValueError(f"successor must explicitly disable multi_market.{name}")
        if projected != parent:
            raise ValueError("successor config changed outside two shadow flags")
    if _without_path(disabled_cfg, PAIR_ACTION_PATH) != _without_path(active_cfg, PAIR_ACTION_PATH):
        raise ValueError("successor configs differ outside BUY E3 enabled bit")

    _publish_create_only(
        output_disabled,
        disabled_raw,
        mode=stat.S_IMODE(parent_disabled.stat().st_mode),
    )
    try:
        _publish_create_only(
            output_active,
            active_raw,
            mode=stat.S_IMODE(parent_active.stat().st_mode),
        )
    except Exception:
        output_disabled.unlink(missing_ok=True)
        raise
    result = {
        "schema_version": SCHEMA,
        "status": STATUS,
        "parent": {
            "disabled": file_identity(parent_disabled),
            "active": file_identity(parent_active),
        },
        "successor": {
            "disabled": file_identity(output_disabled),
            "active": file_identity(output_active),
        },
        "contract": {
            "external_venues_enabled": False,
            "global_flow_shadow_enabled": False,
            "global_reference_shadow_enabled": False,
            "active_disabled_only_difference": ".".join(PAIR_ACTION_PATH),
            "yaml_active_release_fields_added": False,
        },
    }
    result["canonical_config_successor_sha256"] = canonical_sha256(
        result, field="canonical_config_successor_sha256"
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--parent-disabled", type=Path, required=True)
    parser.add_argument("--parent-active", type=Path, required=True)
    parser.add_argument("--output-disabled", type=Path, required=True)
    parser.add_argument("--output-active", type=Path, required=True)
    parser.add_argument("--receipt", type=Path)
    args = parser.parse_args()
    result = build_successor_pair(
        parent_disabled=args.parent_disabled,
        parent_active=args.parent_active,
        output_disabled=args.output_disabled,
        output_active=args.output_active,
    )
    raw = (json.dumps(result, sort_keys=True, indent=2) + "\n").encode("utf-8")
    if args.receipt is None:
        print(raw.decode("utf-8"), end="")
    else:
        _publish_create_only(args.receipt, raw, mode=0o600)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
