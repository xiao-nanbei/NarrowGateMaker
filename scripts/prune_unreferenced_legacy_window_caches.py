#!/usr/bin/env python3
"""Explicitly authorized pruning for unreferenced legacy replay-window caches."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import stat
import sys
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models.replay_cache_audit import (  # noqa: E402
    LEGACY_NAME_RE,
    SELF_AUDIT_PREFIXES,
    TEXT_SUFFIXES,
    audit_json,
    audit_legacy_window_caches,
)

AUDIT_SCHEMA = "narrowgate.legacy_window_cache_reference_audit.v1"
AUDIT_MODE = "read_only_zero_write"
CANDIDATE_STATUS = "unreferenced_candidate_requires_user_approval"
PRESERVE_STATUS = "preserve_frozen_reference"
DRY_RUN_SCHEMA = "narrowgate.legacy_window_cache_prune_dry_run_receipt.v1"
EXECUTION_RECEIPT_SCHEMA = "narrowgate.legacy_window_cache_prune_receipt.v1"
TOKEN_PREFIX = "OWNER-APPROVE-LEGACY-WINDOW-CACHE-PRUNE-V1-"
REFERENCE_SUFFIXES = frozenset(TEXT_SUFFIXES) | {".manifest", ".sha256"}
MAX_REFERENCE_FILE_BYTES = 64 * 1024 * 1024
HEX_SHA256_RE = re.compile(r"[0-9a-fA-F]{64}")


class PruneValidationError(RuntimeError):
    """Raised before deletion when any frozen contract no longer holds."""


@dataclass(frozen=True)
class Candidate:
    path: Path
    basename: str
    cache_key_prefix: str
    size_bytes: int
    mtime_ns: int
    payload_sha256: str = ""

    def authorization_record(self) -> dict[str, Any]:
        return {
            "basename": self.basename,
            "cache_key_prefix": self.cache_key_prefix,
            "mtime_ns": self.mtime_ns,
            "path": str(self.path),
            "payload_sha256": self.payload_sha256,
            "size_bytes": self.size_bytes,
        }


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _canonical_bytes(payload: Any) -> bytes:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise PruneValidationError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _load_verified_audit(path: Path, expected_sha256: str) -> tuple[dict[str, Any], str]:
    expected = expected_sha256.strip().lower()
    if HEX_SHA256_RE.fullmatch(expected) is None:
        raise PruneValidationError("--audit-sha256 must be exactly 64 hexadecimal characters")
    audit_path = path.expanduser().resolve(strict=True)
    raw = audit_path.read_bytes()
    actual = _sha256_bytes(raw)
    if not hmac.compare_digest(actual, expected):
        raise PruneValidationError(f"audit SHA256 mismatch: expected={expected} actual={actual}")
    try:
        payload = json.loads(raw.decode("utf-8"), object_pairs_hook=_strict_object)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PruneValidationError(f"invalid audit JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise PruneValidationError("audit root must be a JSON object")
    return payload, actual


def _exact_int(value: Any, field: str) -> int:
    if type(value) is not int or value < 0:
        raise PruneValidationError(f"{field} must be a non-negative integer")
    return value


def _candidate_from_record(record: dict[str, Any], root: Path, index: int) -> Candidate:
    field = f"files[{index}]"
    basename = record.get("basename")
    raw_path = record.get("path")
    prefix = record.get("cache_key_prefix")
    if not all(isinstance(value, str) and value for value in (basename, raw_path, prefix)):
        raise PruneValidationError(f"{field} has an invalid basename/path/cache_key_prefix")
    match = LEGACY_NAME_RE.fullmatch(basename)
    if match is None or match.group("digest") != prefix:
        raise PruneValidationError(f"{field} basename and cache_key_prefix disagree")
    path = Path(raw_path)
    expected_path = root / basename
    if not path.is_absolute() or path != expected_path or path.parent != root:
        raise PruneValidationError(f"{field} escapes the frozen window_cache root")
    return Candidate(
        path=path,
        basename=basename,
        cache_key_prefix=prefix,
        size_bytes=_exact_int(record.get("size_bytes"), f"{field}.size_bytes"),
        mtime_ns=_exact_int(record.get("mtime_ns"), f"{field}.mtime_ns"),
    )


def _validate_candidate_metadata(candidate: Candidate) -> os.stat_result:
    try:
        current = candidate.path.lstat()
    except FileNotFoundError as exc:
        raise PruneValidationError(f"candidate disappeared: {candidate.path}") from exc
    if stat.S_ISLNK(current.st_mode) or not stat.S_ISREG(current.st_mode):
        raise PruneValidationError(f"candidate is not a regular non-symlink file: {candidate.path}")
    if current.st_size != candidate.size_bytes:
        raise PruneValidationError(
            f"candidate size drift: {candidate.basename} "
            f"audit={candidate.size_bytes} current={current.st_size}"
        )
    if current.st_mtime_ns != candidate.mtime_ns:
        raise PruneValidationError(
            f"candidate mtime drift: {candidate.basename} "
            f"audit={candidate.mtime_ns} current={current.st_mtime_ns}"
        )
    return current


def _hash_candidate(candidate: Candidate) -> Candidate:
    before = _validate_candidate_metadata(candidate)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(candidate.path, flags)
    digest = hashlib.sha256()
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise PruneValidationError(f"candidate changed while opening: {candidate.path}")
        while chunk := os.read(descriptor, 8 * 1024 * 1024):
            digest.update(chunk)
        after_open = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    after_path = _validate_candidate_metadata(candidate)
    if (after_open.st_dev, after_open.st_ino) != (after_path.st_dev, after_path.st_ino):
        raise PruneValidationError(f"candidate replaced while hashing: {candidate.path}")
    if after_open.st_size != candidate.size_bytes or after_open.st_mtime_ns != candidate.mtime_ns:
        raise PruneValidationError(f"candidate changed while hashing: {candidate.path}")
    return Candidate(
        path=candidate.path,
        basename=candidate.basename,
        cache_key_prefix=candidate.cache_key_prefix,
        size_bytes=candidate.size_bytes,
        mtime_ns=candidate.mtime_ns,
        payload_sha256=digest.hexdigest(),
    )


def _validate_audit(
    payload: dict[str, Any],
    requested_cache_root: Path,
) -> tuple[Path, list[Candidate]]:
    if payload.get("schema_version") != AUDIT_SCHEMA:
        raise PruneValidationError(f"unsupported audit schema: {payload.get('schema_version')!r}")
    if payload.get("mode") != AUDIT_MODE:
        raise PruneValidationError(f"unsafe audit mode: {payload.get('mode')!r}")
    for field in ("pickle_payloads_opened", "cache_files_modified", "cache_files_deleted"):
        if payload.get(field) != 0:
            raise PruneValidationError(f"audit {field} must be exactly zero")

    root = requested_cache_root.expanduser().resolve(strict=True)
    raw_audit_root = payload.get("cache_root")
    if not isinstance(raw_audit_root, str) or not Path(raw_audit_root).is_absolute():
        raise PruneValidationError("audit cache_root must be an absolute path")
    try:
        resolved_audit_root = Path(raw_audit_root).resolve(strict=True)
    except OSError as exc:
        raise PruneValidationError(f"audit cache_root is unavailable: {raw_audit_root}") from exc
    if resolved_audit_root != root or raw_audit_root != str(root):
        raise PruneValidationError("audit cache_root does not equal the frozen cache root")

    records = payload.get("files")
    summary = payload.get("summary")
    if not isinstance(records, list) or not isinstance(summary, dict):
        raise PruneValidationError("audit files/summary fields are invalid")
    if summary.get("file_count") != len(records):
        raise PruneValidationError("audit summary file_count does not match files")
    if summary.get("size_bytes") != sum(
        _exact_int(record.get("size_bytes"), "files[].size_bytes")
        for record in records
        if isinstance(record, dict)
    ):
        raise PruneValidationError("audit summary size_bytes does not match files")

    candidates: list[Candidate] = []
    seen_paths: set[str] = set()
    seen_basenames: set[str] = set()
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise PruneValidationError(f"files[{index}] must be an object")
        status = record.get("governance_status")
        if status not in {CANDIDATE_STATUS, PRESERVE_STATUS}:
            raise PruneValidationError(f"files[{index}] has unknown governance_status: {status!r}")
        candidate = _candidate_from_record(record, root, index)
        if str(candidate.path) in seen_paths or candidate.basename in seen_basenames:
            raise PruneValidationError(f"duplicate audit file identity: {candidate.basename}")
        seen_paths.add(str(candidate.path))
        seen_basenames.add(candidate.basename)
        if status != CANDIDATE_STATUS:
            continue
        if record.get("frozen_reference") is not False:
            raise PruneValidationError(
                f"candidate is not explicitly frozen_reference=false: {candidate.basename}"
            )
        if record.get("references") != [] or record.get("reference_classes") != []:
            raise PruneValidationError(
                f"candidate still carries audit references: {candidate.basename}"
            )
        candidates.append(candidate)
    candidates.sort(key=lambda item: item.basename)
    return root, candidates


def _reference_files(root: Path, excluded_paths: set[Path]) -> list[Path]:
    if root.is_file():
        paths = [root]
    else:
        paths = [path for path in root.rglob("*") if path.is_file()]
    result: list[Path] = []
    for path in sorted(paths):
        resolved = path.resolve(strict=True)
        if resolved in excluded_paths:
            continue
        if any(path.name.startswith(prefix) for prefix in SELF_AUDIT_PREFIXES):
            continue
        if root.is_dir() and path.suffix.lower() not in REFERENCE_SUFFIXES:
            continue
        if path.is_symlink():
            raise PruneValidationError(f"symlink in frozen reference roots: {path}")
        size = path.stat().st_size
        if size > MAX_REFERENCE_FILE_BYTES:
            raise PruneValidationError(f"frozen reference file exceeds scan limit: {path}")
        result.append(resolved)
    return result


def _alternation(values: list[str], *, hexadecimal: bool = False) -> re.Pattern[str] | None:
    if not values:
        return None
    body = "|".join(
        re.escape(value) for value in sorted(values, key=lambda item: (-len(item), item))
    )
    if hexadecimal:
        return re.compile(rf"(?<![0-9a-fA-F])(?:{body})(?![0-9a-fA-F])", re.IGNORECASE)
    return re.compile(body)


def _scan_frozen_references(
    roots: list[Path],
    candidates: list[Candidate],
    *,
    excluded_paths: set[Path],
) -> dict[str, Any]:
    canonical_roots: list[Path] = []
    for raw_root in roots:
        root = raw_root.expanduser().resolve(strict=True)
        if root not in canonical_roots:
            canonical_roots.append(root)
    if not canonical_roots:
        raise PruneValidationError("at least one --frozen-reference-root is required")

    by_basename = {candidate.basename: [candidate.basename] for candidate in candidates}
    by_prefix: dict[str, list[str]] = {}
    by_payload: dict[str, list[str]] = {}
    for candidate in candidates:
        by_prefix.setdefault(candidate.cache_key_prefix.lower(), []).append(candidate.basename)
        by_payload.setdefault(candidate.payload_sha256.lower(), []).append(candidate.basename)
    basename_pattern = _alternation(list(by_basename))
    prefix_pattern = _alternation(list(by_prefix), hexadecimal=True)
    payload_pattern = _alternation(list(by_payload), hexadecimal=True)

    hits: set[tuple[str, str, str, str, str]] = set()
    identities: list[dict[str, Any]] = []
    for root in canonical_roots:
        index: list[dict[str, Any]] = []
        for path in _reference_files(root, excluded_paths):
            raw = path.read_bytes()
            content_sha = _sha256_bytes(raw)
            relative = path.name if root.is_file() else str(path.relative_to(root))
            file_stat = path.stat()
            index.append(
                {
                    "content_sha256": content_sha,
                    "mtime_ns": file_stat.st_mtime_ns,
                    "relative_path": relative,
                    "size_bytes": file_stat.st_size,
                }
            )
            text = raw.decode("utf-8", errors="ignore")
            match_groups = (
                ("exact_basename", basename_pattern, by_basename),
                ("exact_cache_key_prefix", prefix_pattern, by_prefix),
                ("exact_payload_sha256", payload_pattern, by_payload),
            )
            for match_type, pattern, identity_map in match_groups:
                if pattern is None:
                    continue
                for match in pattern.finditer(text):
                    value = match.group(0)
                    names = identity_map.get(value.lower(), identity_map.get(value, []))
                    for basename in names:
                        hits.add((basename, match_type, value, str(path), str(root)))
        identities.append(
            {
                "identity_sha256": _sha256_bytes(_canonical_bytes(index)),
                "path": str(root),
                "scanned_file_count": len(index),
                "scanned_size_bytes": sum(item["size_bytes"] for item in index),
                "type": "file" if root.is_file() else "directory",
            }
        )
    rendered_hits = [
        {
            "candidate_basename": basename,
            "match_type": match_type,
            "matched_value": value,
            "reference_path": reference_path,
            "reference_root": reference_root,
        }
        for basename, match_type, value, reference_path, reference_root in sorted(hits)
    ]
    return {
        "candidate_hit_count": len({hit["candidate_basename"] for hit in rendered_hits}),
        "hit_count": len(rendered_hits),
        "hits": rendered_hits,
        "roots": identities,
        "scan_contract": {
            "exact_identities": ["basename", "cache_key_prefix", "payload_sha256"],
            "max_reference_file_bytes": MAX_REFERENCE_FILE_BYTES,
            "reference_suffixes": sorted(REFERENCE_SUFFIXES),
            "self_audit_prefixes_excluded": list(SELF_AUDIT_PREFIXES),
        },
    }


def _candidate_fingerprints(payload: dict[str, Any]) -> dict[str, tuple[Any, ...]]:
    fingerprints: dict[str, tuple[Any, ...]] = {}
    for record in payload.get("files", []):
        if record.get("governance_status") != CANDIDATE_STATUS:
            continue
        basename = record["basename"]
        fingerprints[basename] = (
            record["path"],
            record["cache_key_prefix"],
            record["size_bytes"],
            record["mtime_ns"],
            record["governance_status"],
        )
    return fingerprints


def _fresh_audit_comparison(
    root: Path,
    audit_payload: dict[str, Any],
    reference_roots: list[Path],
) -> dict[str, Any]:
    if not reference_roots:
        return {"candidate_set_matches": None, "enabled": False}
    resolved_roots: list[Path] = []
    for raw_root in reference_roots:
        resolved = raw_root.expanduser().resolve(strict=True)
        if resolved not in resolved_roots:
            resolved_roots.append(resolved)
    fresh = audit_legacy_window_caches(root, reference_roots=resolved_roots)
    frozen_candidates = _candidate_fingerprints(audit_payload)
    fresh_candidates = _candidate_fingerprints(fresh)
    frozen_names = set(frozen_candidates)
    fresh_names = set(fresh_candidates)
    changed = sorted(
        name
        for name in frozen_names & fresh_names
        if frozen_candidates[name] != fresh_candidates[name]
    )
    return {
        "added_candidates": sorted(fresh_names - frozen_names),
        "candidate_set_matches": frozen_candidates == fresh_candidates,
        "changed_candidates": changed,
        "enabled": True,
        "fresh_audit_sha256": _sha256_bytes(audit_json(fresh).encode("utf-8")),
        "reference_roots": [str(path) for path in resolved_roots],
        "removed_candidates": sorted(frozen_names - fresh_names),
    }


def _authorization_material(plan: dict[str, Any]) -> dict[str, Any]:
    return {
        "audit_sha256": plan["audit"]["sha256"],
        "cache_root": plan["cache_root"],
        "candidate_version_filter": plan["candidate_version_filter"],
        "candidates": plan["eligible_candidates"],
        "fresh_reference_audit": plan["fresh_reference_audit"],
        "frozen_reference_roots": plan["frozen_reference_scan"]["roots"],
        "schema_version": "narrowgate.legacy_window_cache_prune_authorization.v1",
    }


def build_dry_run_receipt(
    *,
    audit_path: Path,
    audit_sha256: str,
    cache_root: Path,
    frozen_reference_roots: list[Path],
    fresh_reference_roots: list[Path] | None = None,
    include_versions: set[int] | None = None,
) -> dict[str, Any]:
    audit_payload, verified_sha = _load_verified_audit(audit_path, audit_sha256)
    root, unhashed_candidates = _validate_audit(audit_payload, cache_root)
    requested_versions = set(include_versions or {10, 11, 12, 13})
    if not requested_versions or not requested_versions.issubset({10, 11, 12, 13}):
        raise PruneValidationError("include_versions must be a non-empty subset of 10,11,12,13")
    unhashed_candidates = [
        candidate
        for candidate in unhashed_candidates
        if int(LEGACY_NAME_RE.fullmatch(candidate.basename).group("version")) in requested_versions
    ]
    if not unhashed_candidates:
        raise PruneValidationError("version filter selected no deletion candidates")
    candidates = [_hash_candidate(candidate) for candidate in unhashed_candidates]
    scan = _scan_frozen_references(
        frozen_reference_roots,
        candidates,
        excluded_paths={audit_path.expanduser().resolve(strict=True)},
    )
    fresh = _fresh_audit_comparison(
        root,
        audit_payload,
        fresh_reference_roots or [],
    )
    blocked_names = {hit["candidate_basename"] for hit in scan["hits"]}
    eligible = [
        candidate.authorization_record()
        for candidate in candidates
        if candidate.basename not in blocked_names
    ]
    execution_eligible = not blocked_names and fresh.get("candidate_set_matches") is not False
    plan: dict[str, Any] = {
        "audit": {
            "path": str(audit_path.expanduser().resolve(strict=True)),
            "schema_version": AUDIT_SCHEMA,
            "sha256": verified_sha,
        },
        "audit_candidate_count": len(candidates),
        "audit_candidate_size_bytes": sum(candidate.size_bytes for candidate in candidates),
        "blocked_candidate_basenames": sorted(blocked_names),
        "cache_root": str(root),
        "candidate_version_filter": sorted(requested_versions),
        "candidate_governance_status_required": CANDIDATE_STATUS,
        "computed_candidates": [candidate.authorization_record() for candidate in candidates],
        "eligible_candidates": eligible,
        "execution_eligible": execution_eligible,
        "fresh_reference_audit": fresh,
        "frozen_reference_scan": scan,
        "mode": "dry_run_no_delete",
        "requires_reaudit": not execution_eligible,
        "schema_version": DRY_RUN_SCHEMA,
    }
    if execution_eligible:
        digest = _sha256_bytes(_canonical_bytes(_authorization_material(plan)))
        token: str | None = f"{TOKEN_PREFIX}{digest}"
    else:
        token = None
    plan["authorization"] = {
        "owner_approval_token": token,
        "token_binds": [
            "audit_sha256",
            "cache_root",
            "candidate_version_filter",
            "candidate_payload_sha256",
            "candidate_size_mtime_and_prefix",
            "frozen_reference_root_identities",
            "fresh_audit_identity_when_enabled",
        ],
    }
    plan["dry_run_receipt_sha256"] = _sha256_bytes(_canonical_bytes(plan))
    return plan


def _path_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _atomic_write(path: Path, payload: dict[str, Any], *, require_new: bool) -> None:
    target = path.expanduser().resolve()
    if require_new and target.exists():
        raise PruneValidationError(f"receipt already exists: {target}")
    if not require_new and not target.exists():
        raise PruneValidationError(f"receipt disappeared during execution: {target}")
    if not target.parent.is_dir():
        raise PruneValidationError(f"receipt parent does not exist: {target.parent}")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=target.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        if require_new:
            try:
                os.link(temporary, target)
            except FileExistsError as exc:
                raise PruneValidationError(f"receipt already exists: {target}") from exc
            temporary.unlink()
        else:
            os.replace(temporary, target)
        directory_fd = os.open(target.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)


def _execution_receipt(
    plan: dict[str, Any],
    owner_approval_token: str,
    deleted: list[dict[str, Any]],
    *,
    status_value: str,
    error: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "audit": plan["audit"],
        "authorization_token_sha256": _sha256_bytes(owner_approval_token.encode("utf-8")),
        "cache_root": plan["cache_root"],
        "deleted_count": len(deleted),
        "deleted_files": deleted,
        "deleted_size_bytes": sum(item["size_bytes"] for item in deleted),
        "dry_run_receipt_sha256": plan["dry_run_receipt_sha256"],
        "frozen_reference_scan": plan["frozen_reference_scan"],
        "mode": "execute_owner_authorized",
        "schema_version": EXECUTION_RECEIPT_SCHEMA,
        "status": status_value,
        "updated_at_utc": _utc_now(),
    }
    if error is not None:
        payload["error"] = error
    payload["receipt_sha256"] = _sha256_bytes(_canonical_bytes(payload))
    return payload


def execute_prune(
    plan: dict[str, Any],
    *,
    owner_approval_token: str,
    receipt_path: Path,
) -> dict[str, Any]:
    if plan.get("schema_version") != DRY_RUN_SCHEMA or not plan.get("execution_eligible"):
        raise PruneValidationError(
            "dry-run receipt is not execution eligible; re-audit is required"
        )
    expected_token = plan.get("authorization", {}).get("owner_approval_token")
    if not isinstance(expected_token, str) or not hmac.compare_digest(
        owner_approval_token, expected_token
    ):
        raise PruneValidationError(
            "owner approval token is missing or does not match this exact plan"
        )

    root = Path(plan["cache_root"]).resolve(strict=True)
    receipt = receipt_path.expanduser().resolve()
    protected_roots = [root] + [
        Path(item["path"]).resolve(strict=True) for item in plan["frozen_reference_scan"]["roots"]
    ]
    if any(
        receipt == protected or _path_within(receipt, protected) for protected in protected_roots
    ):
        raise PruneValidationError("receipt must be outside cache and frozen reference roots")
    if receipt.exists() or not receipt.parent.is_dir():
        raise PruneValidationError("receipt path must be new and its parent must already exist")

    candidates = [
        Candidate(
            path=Path(item["path"]),
            basename=item["basename"],
            cache_key_prefix=item["cache_key_prefix"],
            size_bytes=item["size_bytes"],
            mtime_ns=item["mtime_ns"],
            payload_sha256=item["payload_sha256"],
        )
        for item in plan["eligible_candidates"]
    ]
    if not candidates:
        raise PruneValidationError("refusing execute with an empty candidate set")

    # Rehash after authorization and before the first unlink. This intentionally makes
    # execution expensive: content identity, not basename alone, owns deletion authority.
    for candidate in candidates:
        current = _hash_candidate(candidate)
        if not hmac.compare_digest(current.payload_sha256, candidate.payload_sha256):
            raise PruneValidationError(f"candidate payload SHA drift: {candidate.basename}")

    deleted: list[dict[str, Any]] = []
    directory_fd = os.open(root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        for candidate in candidates:
            current = os.stat(candidate.basename, dir_fd=directory_fd, follow_symlinks=False)
            if not stat.S_ISREG(current.st_mode):
                raise PruneValidationError(f"candidate type drift: {candidate.basename}")
            if current.st_size != candidate.size_bytes or current.st_mtime_ns != candidate.mtime_ns:
                raise PruneValidationError(f"candidate metadata drift: {candidate.basename}")
        prepared = _execution_receipt(
            plan,
            owner_approval_token,
            deleted,
            status_value="authorized_prepared",
        )
        _atomic_write(receipt, prepared, require_new=True)
        try:
            for candidate in candidates:
                os.unlink(candidate.basename, dir_fd=directory_fd)
                os.fsync(directory_fd)
                deleted.append(candidate.authorization_record())
                progress = _execution_receipt(
                    plan,
                    owner_approval_token,
                    deleted,
                    status_value="in_progress",
                )
                _atomic_write(receipt, progress, require_new=False)
        except Exception as exc:
            failed = _execution_receipt(
                plan,
                owner_approval_token,
                deleted,
                status_value="failed_partial",
                error=f"{type(exc).__name__}: {exc}",
            )
            _atomic_write(receipt, failed, require_new=False)
            raise
    finally:
        os.close(directory_fd)

    receipt_payload = _execution_receipt(
        plan,
        owner_approval_token,
        deleted,
        status_value="complete",
    )
    receipt_payload["completed_at_utc"] = _utc_now()
    receipt_payload["receipt_sha256"] = _sha256_bytes(
        _canonical_bytes(
            {key: value for key, value in receipt_payload.items() if key != "receipt_sha256"}
        )
    )
    _atomic_write(receipt, receipt_payload, require_new=False)
    return receipt_payload


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-json", required=True, type=Path)
    parser.add_argument("--audit-sha256", required=True)
    parser.add_argument("--cache-root", required=True, type=Path)
    parser.add_argument(
        "--frozen-reference-root",
        action="append",
        required=True,
        type=Path,
        help="Frozen text manifest/report root; repeatable and identity-bound.",
    )
    parser.add_argument(
        "--fresh-reference-root",
        action="append",
        default=[],
        type=Path,
        help="Optional root for a fresh basename audit; repeatable.",
    )
    parser.add_argument(
        "--include-version",
        action="append",
        choices=(10, 11, 12, 13),
        type=int,
        help="Restrict the exact authorized deletion set to these legacy versions.",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Print a no-delete receipt (default).")
    mode.add_argument("--execute", action="store_true", help="Delete only with the frozen token.")
    parser.add_argument("--owner-approval-token")
    parser.add_argument("--receipt", type=Path, help="Required new receipt path for --execute.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        plan = build_dry_run_receipt(
            audit_path=args.audit_json,
            audit_sha256=args.audit_sha256,
            cache_root=args.cache_root,
            frozen_reference_roots=args.frozen_reference_root,
            fresh_reference_roots=args.fresh_reference_root,
            include_versions=set(args.include_version) if args.include_version else None,
        )
        if not args.execute:
            print(json.dumps(plan, indent=2, sort_keys=True))
            return 0 if plan["execution_eligible"] else 3
        if args.receipt is None or args.owner_approval_token is None:
            raise PruneValidationError(
                "--execute requires --receipt and --owner-approval-token from this exact dry run"
            )
        receipt = execute_prune(
            plan,
            owner_approval_token=args.owner_approval_token,
            receipt_path=args.receipt,
        )
        print(json.dumps(receipt, indent=2, sort_keys=True))
        return 0
    except (OSError, PruneValidationError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
