#!/usr/bin/env python3
"""Audit private-evidence governance without opening locked payloads by default."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import subprocess
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
PRIVATE_MARKER = "Local only — do not publish."
CATALOG_SCHEMA = "narrowgate_private_artifact_catalog_v1"
PROJECTION_SCHEMA = "narrowgate_public_machine_document_projections_v1"
NONPUBLISHED_SCHEMA = "narrowgate_nonpublished_machine_document_projections_v1"
AUDIT_SCHEMA = "narrowgate_private_evidence_audit_v2"
ALLOWLIST_SCHEMA = "narrowgate_private_evidence_authorized_read_allowlist_v1"

METADATA_ONLY = "metadata-only"
AUTHORIZED_CONTENT = "authorized-content"
AUDIT_MODES = (METADATA_ONLY, AUTHORIZED_CONTENT)

KNOWN_READ_GATES = frozenset(
    {
        "owner_only",
        "owner_authorized_locator_resolution_only",
        "owner_authorized_content_only",
        "owner_authorized_development_only",
        "development_authorized_only",
        "development_only",
    }
)
LOCKED_PANEL_TOKENS = ("validation", "holdout", "sealed")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
MAX_GOVERNANCE_METADATA_BYTES = 64 << 20

RESEARCH_OWNER_ROOTS = (
    *(
        Path(f"research/families/f{index:02d}_{name}/private")
        for index, name in (
            (1, "fixed_parameter_racing"),
            (2, "empirical_p3_touch"),
            (3, "causal_13_head"),
            (4, "external_market_alpha"),
            (5, "fill_quality_quote_ev"),
            (6, "placement_fill_cif"),
            (7, "active_order_continuation"),
            (8, "side_taker_lifecycle"),
            (9, "campaign_action_uplift"),
            (10, "live_replay_attribution"),
        )
    ),
    Path("research/shared/data_identity/private"),
    Path("research/shared/experiment_governance/private"),
    Path("research/shared/replay_lifecycle/private"),
    Path("research/shared/strategy_semantics/private"),
    Path("research/system_engineering/private"),
)
PRIVATE_OWNER_ROOTS = (
    Path("docs/private"),
    Path("live/private"),
    Path("data/private"),
    Path("models/private"),
    Path("execution/private"),
    *RESEARCH_OWNER_ROOTS,
)
PUBLIC_PROJECTION_MANIFESTS = (
    Path("docs/public_machine_document_projections.json"),
    Path("research/public_machine_document_projections.json"),
)
NONPUBLISHED_INDEX = Path(
    "models/private/nonpublished_machine_document_projections.current.local.json"
)


class PrivateEvidenceAuditError(RuntimeError):
    """Raised when an audit mode or authorization manifest is unsafe."""


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _document_sha256(payload: Mapping[str, Any], field: str) -> str:
    body = dict(payload)
    body.pop(field, None)
    return _canonical_sha256(body)


def _stat_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _open_parent_nofollow(path: Path) -> tuple[Path, int]:
    target = Path(os.path.abspath(os.fspath(path.expanduser())))
    parts = target.parts
    if not target.is_absolute() or len(parts) < 2 or parts[-1] in {"", ".", ".."}:
        raise PrivateEvidenceAuditError("secure file path is malformed")
    if not hasattr(os, "O_DIRECTORY") or not hasattr(os, "O_NOFOLLOW"):
        raise PrivateEvidenceAuditError("secure no-follow file reads are unsupported")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    flags |= getattr(os, "O_CLOEXEC", 0)
    directory_fd: int | None = None
    try:
        directory_fd = os.open(target.anchor, flags)
        for component in parts[1:-1]:
            next_fd = os.open(component, flags, dir_fd=directory_fd)
            os.close(directory_fd)
            directory_fd = next_fd
    except OSError as exc:
        if directory_fd is not None:
            os.close(directory_fd)
        raise PrivateEvidenceAuditError(
            f"secure file parent is unavailable or contains a symlink: {target.name}"
        ) from exc
    if directory_fd is None:
        raise PrivateEvidenceAuditError(f"secure file parent is unavailable: {target.name}")
    return target, directory_fd


def _secure_file_record(
    path: Path,
    *,
    capture_bytes: bool,
    hash_content: bool,
    require_private: bool,
    exact_mode: int | None = None,
    max_capture_bytes: int = MAX_GOVERNANCE_METADATA_BYTES,
) -> dict[str, Any]:
    target, directory_fd = _open_parent_nofollow(path)
    descriptor: int | None = None
    try:
        if not hasattr(os, "O_NOFOLLOW"):
            raise PrivateEvidenceAuditError("secure no-follow file reads are unsupported")
        flags = os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
        descriptor = os.open(target.name, flags, dir_fd=directory_fd)
        before = os.fstat(descriptor)
        mode = stat.S_IMODE(before.st_mode)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.getuid()
            or before.st_nlink != 1
            or before.st_size < 0
            or (exact_mode is not None and mode != exact_mode)
            or (require_private and mode & 0o077)
            or (capture_bytes and before.st_size > max_capture_bytes)
        ):
            raise PrivateEvidenceAuditError(
                f"secure file ownership, mode, links, or size are unsafe: {target.name}"
            )
        digest = hashlib.sha256() if hash_content or capture_bytes else None
        chunks: list[bytes] | None = [] if capture_bytes else None
        total = 0
        if digest is not None:
            while True:
                chunk = os.read(descriptor, 1 << 20)
                if not chunk:
                    break
                total += len(chunk)
                if capture_bytes and total > max_capture_bytes:
                    raise PrivateEvidenceAuditError(
                        f"governance metadata is too large: {target.name}"
                    )
                digest.update(chunk)
                if chunks is not None:
                    chunks.append(chunk)
        after = os.fstat(descriptor)
        try:
            directory_entry = os.stat(
                target.name,
                dir_fd=directory_fd,
                follow_symlinks=False,
            )
        except OSError as exc:
            raise PrivateEvidenceAuditError(
                f"secure file path changed while it was read: {target.name}"
            ) from exc
        identity = _stat_identity(before)
        if (
            (digest is not None and total != before.st_size)
            or _stat_identity(after) != identity
            or directory_entry.st_dev != before.st_dev
            or directory_entry.st_ino != before.st_ino
        ):
            raise PrivateEvidenceAuditError(
                f"secure file path or bytes changed while it was read: {target.name}"
            )
        return {
            "path": target,
            "raw": b"".join(chunks) if chunks is not None else None,
            "sha256": digest.hexdigest() if digest is not None else None,
            "size": before.st_size,
            "identity": identity,
        }
    except OSError as exc:
        raise PrivateEvidenceAuditError(f"secure file is unavailable: {target.name}") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(directory_fd)


def _secure_text(
    path: Path,
    *,
    require_private: bool,
    exact_mode: int | None = None,
) -> tuple[str, dict[str, Any]]:
    record = _secure_file_record(
        path,
        capture_bytes=True,
        hash_content=True,
        require_private=require_private,
        exact_mode=exact_mode,
    )
    try:
        return record["raw"].decode("utf-8"), record
    except UnicodeDecodeError as exc:
        raise PrivateEvidenceAuditError(f"governance metadata is not UTF-8: {path.name}") from exc


def _secure_json(
    path: Path,
    *,
    require_private: bool,
    exact_mode: int | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    text, record = _secure_text(
        path,
        require_private=require_private,
        exact_mode=exact_mode,
    )
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise PrivateEvidenceAuditError(f"governance metadata is not JSON: {path.name}") from exc
    if not isinstance(payload, dict):
        raise PrivateEvidenceAuditError(f"governance metadata root is malformed: {path.name}")
    return payload, record


def _sha256(path: Path, *, require_private: bool = False) -> str:
    record = _secure_file_record(
        path,
        capture_bytes=False,
        hash_content=True,
        require_private=require_private,
    )
    return str(record["sha256"])


def _ignored(repo_root: Path, relative_path: Path) -> bool:
    return (
        subprocess.run(
            ["git", "check-ignore", "-q", "--", relative_path.as_posix()],
            cwd=repo_root,
            check=False,
        ).returncode
        == 0
    )


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _contains_symlink(path: Path) -> bool:
    """Check existing path components without opening the target payload."""

    current = Path(path.anchor) if path.is_absolute() else Path()
    parts = path.parts[1:] if path.is_absolute() else path.parts
    for part in parts:
        current /= part
        try:
            mode = current.lstat().st_mode
        except FileNotFoundError:
            return False
        if stat.S_ISLNK(mode):
            return True
    return False


def _safe_repo_path(repo_root: Path, raw: object) -> Path | None:
    if not isinstance(raw, str) or not raw or "..." in raw:
        return None
    relative = Path(raw)
    if relative.is_absolute() or ".." in relative.parts:
        return None
    candidate = repo_root / relative
    if _contains_symlink(candidate):
        return None
    resolved = candidate.resolve(strict=False)
    return resolved if _is_relative_to(resolved, repo_root) else None


def _safe_metadata_file(repo_root: Path, path: Path) -> bool:
    try:
        record = _secure_file_record(
            path,
            capture_bytes=False,
            hash_content=False,
            require_private=False,
        )
    except PrivateEvidenceAuditError:
        return False
    return _is_relative_to(record["path"], repo_root)


def _catalog_locator(
    repo_root: Path,
    raw: object,
    *,
    content_roots: tuple[Path, ...],
    content_read_requested: bool,
) -> tuple[Path | None, str | None]:
    if not isinstance(raw, str) or not raw or "..." in raw:
        return None, "private_catalog_path_invalid"
    path = Path(raw).expanduser()
    if not path.is_absolute():
        if ".." in path.parts:
            return None, "private_catalog_path_escape"
        path = repo_root / path
    if _contains_symlink(path):
        return None, "private_catalog_symlink_escape"
    resolved = path.resolve(strict=False)
    if not content_read_requested:
        if not _is_relative_to(resolved, repo_root):
            return None, "private_catalog_path_escape"
        return resolved, None
    if not any(_is_relative_to(resolved, root) for root in content_roots):
        return None, "private_catalog_path_escape"
    return resolved, None


def _private_source_path(repo_root: Path, entry: Mapping[str, Any]) -> Path | None:
    public_relative = Path(str(entry.get("public_path", "")))
    if public_relative.is_absolute() or ".." in public_relative.parts:
        return None
    unit_id = str(entry.get("unit_id", ""))
    if unit_id == "repository":
        try:
            within_unit = public_relative.relative_to("docs")
        except ValueError:
            return None
        candidate = repo_root / "docs/private/original_public_machine_records" / within_unit
    else:
        unit = Path(unit_id)
        if not unit_id or unit.is_absolute() or ".." in unit.parts:
            return None
        try:
            within_unit = public_relative.relative_to(unit)
        except ValueError:
            candidate = (
                repo_root
                / unit
                / "private/original_public_machine_records/cross_unit"
                / public_relative
            )
        else:
            candidate = repo_root / unit / "private/original_public_machine_records" / within_unit
    if _contains_symlink(candidate):
        return None
    resolved = candidate.resolve(strict=False)
    return resolved if _is_relative_to(resolved, repo_root) else None


def _locked_panel_role(value: object) -> bool:
    normalized = str(value).strip().lower()
    return any(token in normalized for token in LOCKED_PANEL_TOKENS)


def _require_string_list(payload: Mapping[str, Any], field: str) -> tuple[str, ...]:
    raw = payload.get(field)
    if not isinstance(raw, list) or any(not isinstance(value, str) or not value for value in raw):
        raise PrivateEvidenceAuditError(f"authorization allowlist {field} is malformed")
    if len(set(raw)) != len(raw):
        raise PrivateEvidenceAuditError(f"authorization allowlist {field} is duplicated")
    return tuple(raw)


def _load_authorization_allowlist(repo_root: Path, path: Path) -> dict[str, Any]:
    try:
        payload, record = _secure_json(
            path,
            require_private=True,
            exact_mode=0o600,
        )
    except PrivateEvidenceAuditError as exc:
        raise PrivateEvidenceAuditError(
            f"authorization allowlist is unsafe or unreadable: {exc}"
        ) from exc
    fields = {
        "schema_version",
        "authorization_id",
        "repository_root",
        "catalog_artifact_ids",
        "private_projection_public_paths",
        "nonpublished_projection_paths",
        "content_roots",
        "allowed_read_gates",
        "validation_read",
        "sealed_holdout_read",
        "canonical_allowlist_sha256",
    }
    if not isinstance(payload, dict) or set(payload) != fields:
        raise PrivateEvidenceAuditError("authorization allowlist fields drifted")
    if (
        payload.get("schema_version") != ALLOWLIST_SCHEMA
        or not str(payload.get("authorization_id", "")).strip()
        or payload.get("repository_root") != str(repo_root)
        or payload.get("validation_read") is not False
        or payload.get("sealed_holdout_read") is not False
        or payload.get("canonical_allowlist_sha256")
        != _document_sha256(payload, "canonical_allowlist_sha256")
    ):
        raise PrivateEvidenceAuditError("authorization allowlist identity drifted")
    catalog_ids = _require_string_list(payload, "catalog_artifact_ids")
    projection_paths = _require_string_list(payload, "private_projection_public_paths")
    nonpublished_paths = _require_string_list(payload, "nonpublished_projection_paths")
    if any(_locked_panel_role(value) for value in (*projection_paths, *nonpublished_paths)):
        raise PrivateEvidenceAuditError(
            "authorization projection paths cannot target Validation, holdout, or sealed data"
        )
    read_gates = _require_string_list(payload, "allowed_read_gates")
    if not read_gates or not set(read_gates).issubset(KNOWN_READ_GATES):
        raise PrivateEvidenceAuditError("authorization allowlist read gate is unknown")
    roots: list[Path] = []
    for raw in _require_string_list(payload, "content_roots"):
        if _locked_panel_role(raw):
            raise PrivateEvidenceAuditError(
                "authorization content root cannot target Validation, holdout, or sealed data"
            )
        allowed_root = Path(raw).expanduser()
        if not allowed_root.is_absolute():
            allowed_root = repo_root / allowed_root
        if _contains_symlink(allowed_root):
            raise PrivateEvidenceAuditError("authorization content root contains a symlink")
        roots.append(allowed_root.resolve(strict=False))
    if not roots:
        raise PrivateEvidenceAuditError("authorization allowlist lacks content roots")
    return {
        "path": str(record["path"]),
        "file_sha256": record["sha256"],
        "canonical_sha256": payload["canonical_allowlist_sha256"],
        "catalog_artifact_ids": frozenset(catalog_ids),
        "private_projection_public_paths": frozenset(projection_paths),
        "nonpublished_projection_paths": frozenset(nonpublished_paths),
        "content_roots": tuple(roots),
        "allowed_read_gates": frozenset(read_gates),
    }


def _iter_private_files(root: Path) -> Iterable[Path]:
    for directory, names, files in os.walk(root, followlinks=False):
        directory_path = Path(directory)
        for name in tuple(names):
            candidate = directory_path / name
            try:
                if stat.S_ISLNK(candidate.lstat().st_mode):
                    yield candidate
                    names.remove(name)
            except FileNotFoundError:
                continue
        for name in files:
            yield directory_path / name


def audit(
    repo_root: Path,
    *,
    mode: str = METADATA_ONLY,
    deny_locked: bool = True,
    allowlist_manifest: Path | None = None,
) -> dict[str, object]:
    root = repo_root.expanduser().resolve(strict=True)
    if mode not in AUDIT_MODES:
        raise PrivateEvidenceAuditError(f"unsupported audit mode: {mode}")
    if deny_locked is not True:
        raise PrivateEvidenceAuditError("locked evidence cannot be enabled by this audit")
    if mode == AUTHORIZED_CONTENT:
        if allowlist_manifest is None:
            raise PrivateEvidenceAuditError(
                "authorized-content mode requires an explicit allowlist manifest"
            )
        authorization = _load_authorization_allowlist(root, allowlist_manifest)
    elif allowlist_manifest is not None:
        raise PrivateEvidenceAuditError("metadata-only mode cannot accept a content allowlist")
    else:
        authorization = None

    findings: list[dict[str, object]] = []
    catalog_entries = 0
    semantic_catalog_entries = 0
    verified_catalog_files = 0
    metadata_catalog_files_seen = 0
    artifact_ids: dict[str, str] = {}
    private_files_seen = 0
    payload_files_opened = 0
    external_locator_count = 0
    catalog_text_fragments: list[str] = []
    content_roots = tuple(authorization["content_roots"]) if authorization is not None else (root,)

    for relative_root in PRIVATE_OWNER_ROOTS:
        owner_root = root / relative_root
        if _contains_symlink(owner_root):
            findings.append(
                {"kind": "private_owner_root_symlink_forbidden", "path": relative_root.as_posix()}
            )
            continue
        if not owner_root.is_dir():
            findings.append(
                {"kind": "private_owner_root_missing", "path": relative_root.as_posix()}
            )
            continue
        if not _ignored(root, relative_root):
            findings.append(
                {"kind": "private_owner_root_not_ignored", "path": relative_root.as_posix()}
            )
        if owner_root.stat().st_mode & 0o077:
            findings.append(
                {"kind": "private_owner_root_permissions", "path": relative_root.as_posix()}
            )
        marker = owner_root / "README.local.md"
        try:
            marker_text, _marker_record = _secure_text(marker, require_private=True)
            marker_lines = marker_text.splitlines()
        except PrivateEvidenceAuditError:
            marker_lines = []
        if not marker_lines or marker_lines[0] != PRIVATE_MARKER:
            findings.append(
                {"kind": "private_owner_marker_invalid", "path": relative_root.as_posix()}
            )
        catalog_path = owner_root / "catalog.current.local.json"
        try:
            catalog_text, _catalog_record = _secure_text(
                catalog_path,
                require_private=True,
            )
            catalog = json.loads(catalog_text)
            if not isinstance(catalog, dict):
                raise PrivateEvidenceAuditError("private catalog root is malformed")
        except (PrivateEvidenceAuditError, json.JSONDecodeError):
            findings.append(
                {"kind": "private_catalog_missing_or_unsafe", "path": relative_root.as_posix()}
            )
            continue
        catalog_text_fragments.append(catalog_text)
        unit_id = str(catalog.get("unit_id", relative_root.parent.as_posix()))
        catalog_relative = str(catalog_path.relative_to(root))
        if catalog.get("schema_version") != CATALOG_SCHEMA:
            findings.append({"kind": "private_catalog_schema", "path": catalog_relative})
        if catalog.get("visibility") != "local_only_do_not_publish":
            findings.append({"kind": "private_catalog_visibility", "path": catalog_relative})
        entries = catalog.get("entries", [])
        if not isinstance(entries, list):
            findings.append({"kind": "private_catalog_entries", "path": catalog_relative})
            entries = []
        for entry in entries:
            catalog_entries += 1
            if not isinstance(entry, dict):
                findings.append(
                    {"kind": "private_catalog_entry_malformed", "path": catalog_relative}
                )
                continue
            artifact_id = str(entry.get("artifact_id", ""))
            if not artifact_id:
                findings.append({"kind": "private_artifact_id_missing", "path": catalog_relative})
                continue
            qualified_id = f"{unit_id}:{artifact_id}"
            if qualified_id in artifact_ids:
                findings.append(
                    {
                        "kind": "private_artifact_id_duplicate",
                        "path": catalog_relative,
                        "artifact_id": qualified_id,
                        "first_path": artifact_ids[qualified_id],
                    }
                )
            artifact_ids[qualified_id] = catalog_relative
            panel_role = str(entry.get("panel_role", ""))
            read_gate = str(entry.get("read_gate", ""))
            if not panel_role or not read_gate:
                findings.append(
                    {
                        "kind": "private_read_governance_missing",
                        "path": catalog_relative,
                        "artifact_id": artifact_id,
                    }
                )
            elif read_gate not in KNOWN_READ_GATES:
                findings.append(
                    {
                        "kind": "private_read_gate_unknown",
                        "path": catalog_relative,
                        "artifact_id": artifact_id,
                        "read_gate": read_gate,
                    }
                )
            locked_entry = _locked_panel_role(panel_role) or _locked_panel_role(
                entry.get("local_path")
            )
            if locked_entry:
                findings.append(
                    {
                        "kind": "locked_panel_denied",
                        "path": catalog_relative,
                        "artifact_id": artifact_id,
                        "panel_role": panel_role,
                    }
                )
            expected_sha = entry.get("sha256")
            expected_bytes = entry.get("bytes")
            if expected_sha is not None or expected_bytes is not None:
                semantic_catalog_entries += 1
            expected_sha_valid = expected_sha is None or (
                _SHA256_RE.fullmatch(str(expected_sha)) is not None
            )
            if not expected_sha_valid:
                findings.append(
                    {
                        "kind": "private_catalog_sha_invalid",
                        "path": catalog_relative,
                        "artifact_id": artifact_id,
                    }
                )
            expected_bytes_valid = expected_bytes is None or not (
                type(expected_bytes) is not int or expected_bytes < 0
            )
            if not expected_bytes_valid:
                findings.append(
                    {
                        "kind": "private_catalog_size_invalid",
                        "path": catalog_relative,
                        "artifact_id": artifact_id,
                    }
                )
            content_requested = bool(
                authorization is not None and qualified_id in authorization["catalog_artifact_ids"]
            )
            if locked_entry:
                content_requested = False
            if content_requested and read_gate not in authorization["allowed_read_gates"]:
                findings.append(
                    {
                        "kind": "private_read_gate_not_authorized",
                        "path": catalog_relative,
                        "artifact_id": artifact_id,
                    }
                )
                content_requested = False
            resolved, path_finding = _catalog_locator(
                root,
                entry.get("local_path"),
                content_roots=content_roots,
                content_read_requested=content_requested,
            )
            if path_finding is not None:
                findings.append(
                    {
                        "kind": path_finding,
                        "path": catalog_relative,
                        "artifact_id": artifact_id,
                    }
                )
                continue
            if resolved is None:
                continue
            if resolved.is_absolute() and not _is_relative_to(resolved, root):
                external_locator_count += 1
            if not resolved.is_file():
                continue
            metadata_catalog_files_seen += 1
            if not content_requested:
                continue
            if not expected_sha_valid or not expected_bytes_valid:
                continue
            if expected_sha is None and expected_bytes is None:
                continue
            if expected_sha is not None:
                payload_files_opened += 1
            try:
                payload_record = _secure_file_record(
                    resolved,
                    capture_bytes=False,
                    hash_content=expected_sha is not None,
                    require_private=True,
                )
            except PrivateEvidenceAuditError:
                findings.append(
                    {
                        "kind": "private_catalog_payload_unsafe",
                        "path": catalog_relative,
                        "artifact_id": artifact_id,
                    }
                )
                continue
            if expected_sha is not None and payload_record["sha256"] != expected_sha:
                findings.append(
                    {
                        "kind": "private_catalog_sha_mismatch",
                        "path": catalog_relative,
                        "artifact_id": artifact_id,
                    }
                )
            elif expected_bytes is not None and payload_record["size"] != expected_bytes:
                findings.append(
                    {
                        "kind": "private_catalog_size_mismatch",
                        "path": catalog_relative,
                        "artifact_id": artifact_id,
                    }
                )
            elif expected_sha is not None or expected_bytes is not None:
                verified_catalog_files += 1

        for path in _iter_private_files(owner_root):
            private_files_seen += 1
            try:
                metadata = path.lstat()
            except FileNotFoundError:
                continue
            relative = str(path.relative_to(root))
            if stat.S_ISLNK(metadata.st_mode):
                findings.append({"kind": "private_symlink_forbidden", "path": relative})
            elif metadata.st_mode & 0o077:
                findings.append({"kind": "private_file_permissions", "path": relative})

    projection_entries = 0
    public_projection_files_verified = 0
    projection_private_sources_verified = 0
    for relative_manifest in PUBLIC_PROJECTION_MANIFESTS:
        manifest_path = root / relative_manifest
        try:
            payload, _manifest_record = _secure_json(
                manifest_path,
                require_private=False,
            )
        except PrivateEvidenceAuditError:
            findings.append(
                {
                    "kind": "projection_manifest_missing_or_unsafe",
                    "path": relative_manifest.as_posix(),
                }
            )
            continue
        if payload.get("schema_version") != PROJECTION_SCHEMA:
            findings.append(
                {"kind": "projection_manifest_schema", "path": relative_manifest.as_posix()}
            )
        for entry in payload.get("entries", []):
            projection_entries += 1
            public_name = str(entry.get("public_path", ""))
            public_path = _safe_repo_path(root, public_name)
            if public_path is None:
                findings.append({"kind": "public_projection_path_escape", "path": public_name})
                continue
            if not public_path.is_file():
                findings.append({"kind": "public_projection_missing", "path": public_name})
                continue
            source_path = _private_source_path(root, entry)
            if source_path is None:
                findings.append(
                    {"kind": "projection_private_source_path_escape", "path": public_name}
                )
                continue
            if not source_path.is_file():
                findings.append({"kind": "projection_private_source_missing", "path": public_name})
                continue
            content_requested = bool(
                authorization is not None
                and public_name in authorization["private_projection_public_paths"]
            )
            locked_projection = any(
                _locked_panel_role(value)
                for value in (public_name, entry.get("unit_id"), source_path)
            )
            if locked_projection:
                findings.append({"kind": "locked_panel_denied", "path": public_name})
                content_requested = False
            if not content_requested:
                continue
            expected_public_sha = entry.get("public_projection_sha256")
            expected_private_sha = entry.get("source_private_sha256")
            if _SHA256_RE.fullmatch(str(expected_public_sha)) is None:
                findings.append({"kind": "public_projection_sha_invalid", "path": public_name})
                continue
            if _SHA256_RE.fullmatch(str(expected_private_sha)) is None:
                findings.append(
                    {"kind": "projection_private_source_sha_invalid", "path": public_name}
                )
                continue
            payload_files_opened += 1
            try:
                public_record = _secure_file_record(
                    public_path,
                    capture_bytes=False,
                    hash_content=True,
                    require_private=False,
                )
            except PrivateEvidenceAuditError:
                findings.append({"kind": "public_projection_unsafe", "path": public_name})
                continue
            if public_record["sha256"] != expected_public_sha:
                findings.append({"kind": "public_projection_sha_mismatch", "path": public_name})
            else:
                public_projection_files_verified += 1
            payload_files_opened += 1
            try:
                source_record = _secure_file_record(
                    source_path,
                    capture_bytes=False,
                    hash_content=True,
                    require_private=True,
                )
            except PrivateEvidenceAuditError:
                findings.append({"kind": "projection_private_source_unsafe", "path": public_name})
                continue
            if source_record["sha256"] != expected_private_sha:
                findings.append(
                    {"kind": "projection_private_source_sha_mismatch", "path": public_name}
                )
            else:
                projection_private_sources_verified += 1

    nonpublished_projection_entries = 0
    nonpublished_projection_files_verified = 0
    nonpublished_path = root / NONPUBLISHED_INDEX
    try:
        payload, _nonpublished_record = _secure_json(
            nonpublished_path,
            require_private=True,
        )
    except PrivateEvidenceAuditError:
        findings.append(
            {
                "kind": "nonpublished_projection_index_missing_or_unsafe",
                "path": str(NONPUBLISHED_INDEX),
            }
        )
    else:
        if payload.get("schema_version") != NONPUBLISHED_SCHEMA:
            findings.append(
                {"kind": "nonpublished_projection_schema", "path": str(NONPUBLISHED_INDEX)}
            )
        for entry in payload.get("entries", []):
            nonpublished_projection_entries += 1
            public_name = str(entry.get("public_path", ""))
            path = _safe_repo_path(root, public_name)
            if path is None:
                findings.append(
                    {"kind": "nonpublished_projection_path_escape", "path": public_name}
                )
                continue
            if not _ignored(root, Path(public_name)):
                findings.append(
                    {"kind": "nonpublished_projection_not_ignored", "path": public_name}
                )
            content_requested = bool(
                authorization is not None
                and public_name in authorization["nonpublished_projection_paths"]
            )
            if _locked_panel_role(public_name):
                findings.append({"kind": "locked_panel_denied", "path": public_name})
                content_requested = False
            if not path.is_file():
                findings.append({"kind": "nonpublished_projection_missing", "path": public_name})
            elif content_requested:
                expected_sha = entry.get("public_projection_sha256")
                if _SHA256_RE.fullmatch(str(expected_sha)) is None:
                    findings.append(
                        {"kind": "nonpublished_projection_sha_invalid", "path": public_name}
                    )
                    continue
                payload_files_opened += 1
                try:
                    projection_record = _secure_file_record(
                        path,
                        capture_bytes=False,
                        hash_content=True,
                        require_private=True,
                    )
                except PrivateEvidenceAuditError:
                    findings.append({"kind": "nonpublished_projection_unsafe", "path": public_name})
                    continue
                if projection_record["sha256"] != expected_sha:
                    findings.append(
                        {"kind": "nonpublished_projection_sha_mismatch", "path": public_name}
                    )
                else:
                    nonpublished_projection_files_verified += 1

    if "live/.env" in "\n".join(catalog_text_fragments):
        findings.append({"kind": "secret_surface_cataloged", "path": "live/.env"})

    return {
        "schema_version": AUDIT_SCHEMA,
        "mode": mode,
        "deny_locked": True,
        "authorization_manifest_sha256": (
            authorization["canonical_sha256"] if authorization is not None else None
        ),
        "owner_roots_expected": len(PRIVATE_OWNER_ROOTS),
        "private_files_seen": private_files_seen,
        "catalog_entries": catalog_entries,
        "semantic_catalog_entries": semantic_catalog_entries,
        "metadata_catalog_files_seen": metadata_catalog_files_seen,
        "verified_catalog_files": verified_catalog_files,
        "external_locator_count": external_locator_count,
        "public_projection_entries": projection_entries,
        "public_projection_files_verified": public_projection_files_verified,
        "projection_private_sources_verified": projection_private_sources_verified,
        "nonpublished_projection_entries": nonpublished_projection_entries,
        "nonpublished_projection_files_verified": nonpublished_projection_files_verified,
        "payload_files_opened": payload_files_opened,
        "validation_read": False,
        "sealed_holdout_read": False,
        "findings": findings,
        "passed": not findings,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--mode", choices=AUDIT_MODES, default=METADATA_ONLY)
    parser.add_argument("--allowlist-manifest", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        result = audit(
            args.repo_root,
            mode=args.mode,
            deny_locked=True,
            allowlist_manifest=args.allowlist_manifest,
        )
    except PrivateEvidenceAuditError as exc:
        result = {
            "schema_version": AUDIT_SCHEMA,
            "mode": args.mode,
            "deny_locked": True,
            "findings": [{"kind": "audit_configuration_rejected", "detail": str(exc)}],
            "passed": False,
        }
    rendered = json.dumps(result, indent=2, ensure_ascii=False) + "\n"
    if args.output:
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
