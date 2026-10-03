#!/usr/bin/env python3
"""Create presentation-only public projections for private machine fields.

The source bytes are copied unchanged into the owning ignored ``private/``
tree before a public JSON record is rewritten.  This command never edits an
existing private source and fails closed when an existing projection binding
does not verify.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
RESEARCH_MANIFEST = Path("research/public_machine_document_projections.json")
REPOSITORY_MANIFEST = Path("docs/public_machine_document_projections.json")
PROJECTION_SCHEMA = "narrowgate_public_machine_document_projections_v1"
PID_KEY_RE = re.compile(
    r"(?:^|_)(?:pid|process_id|process_pid|maker_pid|parent_pid|child_pid)$",
    re.IGNORECASE,
)
PRIVATE_CONFIG_RE = re.compile(
    r"(?:\$\{NARROWGATE_ROOT\}/)?docs/private/([A-Za-z0-9_.-]+)"
)
SECRET_KEY_RE = re.compile(
    r"^(?:api[_-]?key|api[_-]?secret|secret|private[_-]?key|signing[_-]?key|"
    r"password|passphrase|access[_-]?token|refresh[_-]?token)$",
    re.IGNORECASE,
)
REDACTED_PROCESS_ID = "<private-process-id>"


@dataclass
class Change:
    public_path: str
    unit_id: str
    new_projection: bool
    private_locators_redacted: int
    process_identifiers_redacted: int
    source_private_sha256: str
    public_projection_sha256: str


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _public_json_paths(repo_root: Path) -> list[Path]:
    output = subprocess.check_output(
        ["git", "ls-files", "-co", "--exclude-standard", "*.json"],
        cwd=repo_root,
        text=True,
    )
    return sorted(
        repo_root / item
        for item in output.splitlines()
        if (repo_root / item).is_file()
    )


def _unit_id(relative_path: Path) -> str:
    parts = relative_path.parts
    if parts[:2] == ("research", "families") and len(parts) >= 3:
        return Path(*parts[:3]).as_posix()
    if parts[:2] == ("research", "shared") and len(parts) >= 3:
        return Path(*parts[:3]).as_posix()
    if parts[:2] == ("research", "system_engineering"):
        return "research/system_engineering"
    if parts and parts[0] == "docs":
        return "repository"
    raise RuntimeError(f"no private evidence owner for public record: {relative_path}")


def _private_source_path(repo_root: Path, unit_id: str, public_path: Path) -> Path:
    if unit_id == "repository":
        within_unit = public_path.relative_to("docs")
        return repo_root / "docs/private/original_public_machine_records" / within_unit
    unit = Path(unit_id)
    try:
        within_unit = public_path.relative_to(unit)
    except ValueError:
        return (
            repo_root
            / unit
            / "private/original_public_machine_records/cross_unit"
            / public_path
        )
    return repo_root / unit / "private/original_public_machine_records" / within_unit


def _load_manifests(repo_root: Path) -> dict[Path, dict[str, Any]]:
    manifests: dict[Path, dict[str, Any]] = {}
    for relative in (RESEARCH_MANIFEST, REPOSITORY_MANIFEST):
        path = repo_root / relative
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("schema_version") != PROJECTION_SCHEMA:
            raise RuntimeError(f"unsupported projection manifest: {relative}")
        manifests[relative] = payload
    return manifests


def _entry_index(
    manifests: dict[Path, dict[str, Any]],
) -> dict[str, tuple[Path, dict[str, Any]]]:
    result: dict[str, tuple[Path, dict[str, Any]]] = {}
    for manifest_path, payload in manifests.items():
        for entry in payload.get("entries", []):
            public_path = str(entry["public_path"])
            if public_path in result:
                raise RuntimeError(f"duplicate projection entry: {public_path}")
            result[public_path] = (manifest_path, entry)
    return result


def _contains_secret(value: object, location: str = "") -> list[str]:
    findings: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            child = f"{location}/{key}"
            if (
                SECRET_KEY_RE.search(str(key))
                and isinstance(item, str)
                and item not in ("", "<redacted>")
            ):
                findings.append(child)
            findings.extend(_contains_secret(item, child))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            findings.extend(_contains_secret(item, f"{location}/{index}"))
    return findings


def _redact_string(value: str) -> tuple[str, int]:
    count = 0

    def replace(match: re.Match[str]) -> str:
        nonlocal count
        count += 1
        filename = match.group(1)
        if filename == "live_config.current.local.yaml":
            return "${NARROWGATE_LIVE_CONFIG}"
        if filename == "live_remote.current.local.json":
            return "${NARROWGATE_LIVE_REMOTE_POINTER}"
        return f"${{NARROWGATE_PRIVATE_CONFIG_ROOT}}/{filename}"

    return PRIVATE_CONFIG_RE.sub(replace, value), count


def _sanitize(value: object, key: str | None = None) -> tuple[object, int, int]:
    if key is not None and PID_KEY_RE.search(key):
        if value is not None and value != REDACTED_PROCESS_ID:
            return REDACTED_PROCESS_ID, 0, 1
        return value, 0, 0
    if isinstance(value, str):
        redacted, locator_count = _redact_string(value)
        return redacted, locator_count, 0
    if isinstance(value, list):
        output: list[object] = []
        locators = 0
        pids = 0
        for item in value:
            new_item, item_locators, item_pids = _sanitize(item)
            output.append(new_item)
            locators += item_locators
            pids += item_pids
        return output, locators, pids
    if isinstance(value, dict):
        output_dict: dict[str, object] = {}
        locators = 0
        pids = 0
        for child_key, item in value.items():
            new_item, item_locators, item_pids = _sanitize(item, str(child_key))
            output_dict[str(child_key)] = new_item
            locators += item_locators
            pids += item_pids
        return output_dict, locators, pids
    return value, 0, 0


def govern(repo_root: Path, *, apply: bool) -> list[Change]:
    manifests = _load_manifests(repo_root)
    entries = _entry_index(manifests)
    manifest_relatives = {RESEARCH_MANIFEST.as_posix(), REPOSITORY_MANIFEST.as_posix()}
    changes: list[Change] = []
    pending_writes: list[tuple[Path, bytes, int, int]] = []

    for path in _public_json_paths(repo_root):
        relative = path.relative_to(repo_root)
        relative_text = relative.as_posix()
        if relative_text in manifest_relatives:
            continue
        source_bytes = path.read_bytes()
        payload = json.loads(source_bytes.decode("utf-8"))
        sanitized, locator_count, pid_count = _sanitize(payload)
        if locator_count == 0 and pid_count == 0:
            continue
        if not isinstance(sanitized, (dict, list)):
            raise RuntimeError(f"top-level JSON object expected: {relative_text}")
        secret_fields = _contains_secret(payload)
        if secret_fields:
            raise RuntimeError(
                f"refusing to copy possible secret fields from {relative_text}: {secret_fields}"
            )
        public_bytes = (
            json.dumps(sanitized, indent=2, ensure_ascii=False).encode("utf-8") + b"\n"
        )
        existing = entries.get(relative_text)
        unit_id = _unit_id(relative)
        private_source = _private_source_path(repo_root, unit_id, relative)
        source_sha = sha256_bytes(source_bytes)
        new_projection = existing is None
        if existing is not None:
            manifest_relative, entry = existing
            unit_id = str(entry["unit_id"])
            private_source = _private_source_path(repo_root, unit_id, relative)
            expected_source = str(entry["source_private_sha256"])
            if not private_source.is_file():
                raise RuntimeError(f"private source is missing: {private_source}")
            observed_private = sha256_bytes(private_source.read_bytes())
            if observed_private != expected_source:
                raise RuntimeError(
                    f"private source drift for {relative_text}: "
                    f"expected={expected_source} observed={observed_private}"
                )
            source_sha = expected_source
        else:
            manifest_relative = (
                RESEARCH_MANIFEST if relative.parts[0] == "research" else REPOSITORY_MANIFEST
            )
            entry = {
                "public_path": relative_text,
                "unit_id": unit_id,
                "source_private_sha256": source_sha,
                "public_projection_sha256": "",
                "transformation": "locator_and_operational_identifier_redaction_only",
                "availability": "public_repository_projection_private_source_not_distributed",
                "private_locator_count": locator_count,
                "operational_identifier_redaction_count": pid_count,
            }
            manifests[manifest_relative].setdefault("entries", []).append(entry)
            entries[relative_text] = (manifest_relative, entry)

        public_sha = sha256_bytes(public_bytes)
        entry["public_projection_sha256"] = public_sha
        entry["private_locator_count"] = max(
            int(entry.get("private_locator_count", 0)), locator_count
        )
        entry["operational_identifier_redaction_count"] = max(
            int(entry.get("operational_identifier_redaction_count", 0)), pid_count
        )
        if pid_count:
            entry["transformation"] = "locator_and_operational_identifier_redaction_only"
        changes.append(
            Change(
                public_path=relative_text,
                unit_id=unit_id,
                new_projection=new_projection,
                private_locators_redacted=locator_count,
                process_identifiers_redacted=pid_count,
                source_private_sha256=source_sha,
                public_projection_sha256=public_sha,
            )
        )
        pending_writes.append((path, public_bytes, path.stat().st_atime_ns, path.stat().st_mtime_ns))
        if new_projection and apply:
            private_source.parent.mkdir(parents=True, exist_ok=True)
            if private_source.exists():
                raise RuntimeError(f"refusing to replace unregistered private source: {private_source}")
            private_source.write_bytes(source_bytes)
            os.chmod(private_source, 0o600)

    if not apply:
        return changes
    for path, payload, atime_ns, mtime_ns in pending_writes:
        path.write_bytes(payload)
        os.utime(path, ns=(atime_ns, mtime_ns))
    for relative, payload in manifests.items():
        payload["entries"] = sorted(payload.get("entries", []), key=lambda row: row["public_path"])
        path = repo_root / relative
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return changes


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    changes = govern(args.repo_root.resolve(), apply=args.apply)
    result = {
        "schema_version": "narrowgate_public_machine_record_governance_v1",
        "applied": args.apply,
        "files_changed": len(changes),
        "new_projections": sum(row.new_projection for row in changes),
        "private_locators_redacted": sum(row.private_locators_redacted for row in changes),
        "process_identifiers_redacted": sum(
            row.process_identifiers_redacted for row in changes
        ),
        "changes": [row.__dict__ for row in changes],
    }
    rendered = json.dumps(result, indent=2, ensure_ascii=False) + "\n"
    if args.output:
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
