from __future__ import annotations

import contextlib
import csv
import dataclasses
import fcntl
import hashlib
import hmac
import json
import os
import shutil
import sqlite3
import stat
import tempfile
import time
import uuid
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Literal

from data_paths import cache_root as configured_cache_root
from data_paths import external_cache_root

SCHEMA_VERSION = "narrowgate.cache_tier_lru.ledger.v1"
PLAN_SCHEMA_VERSION = "narrowgate.cache_tier_lru.plan.v1"
RECEIPT_SCHEMA_VERSION = "narrowgate.cache_tier_lru.receipt.v1"
ACTIVE_MANIFEST_SCHEMA_VERSION = "narrowgate.cache_tier_lru.active_manifest.v1"

DEFAULT_HOT_ROOT = configured_cache_root()
DEFAULT_COLD_ROOT = external_cache_root()
DEFAULT_STATE_ROOT = DEFAULT_HOT_ROOT / ".cache_tier_lru"
DEFAULT_LEDGER_PATH = DEFAULT_STATE_ROOT / "access_ledger.sqlite3"
DEFAULT_HOT_SAFETY_RESERVE_BYTES = 60 * 1024**3
DEFAULT_HOT_TARGET_FREE_BYTES = 70 * 1024**3
DEFAULT_COLD_TTL_DAYS = 14

DEFAULT_ALLOWED_CACHE_ROOTS = (
    "window_cache",
    "replay_dag",
    "p3_touch_window_context_v1",
    "p3_touch_reaches_v1",
    "p3_conditional_quote_overlay_v1",
)
FORBIDDEN_TOP_LEVELS = frozenset(
    {
        "artifact",
        "artifacts",
        "frozen",
        "frozen_artifacts",
        "raw",
        "raw_data",
        "report",
        "reports",
    }
)
REFERENCE_CLASSES = frozenset({"unknown", "unreferenced", "referenced", "frozen"})
TIERS = frozenset({"hot", "cold"})
OPERATIONS = frozenset({"migrate", "delete"})
GIB = 1024**3
REFERENCE_AUDIT_CLASS_MAP = {
    "currently_referenced": "referenced",
    "frozen_historical_referenced": "frozen",
    "superseded_but_referenced": "frozen",
    "safely_unreferenced_deletion_candidate": "unreferenced",
    "unknown_manual_review": "unknown",
}
REFERENCE_PROTECTION_RANK = {
    "unreferenced": 0,
    "unknown": 1,
    "referenced": 2,
    "frozen": 3,
}


class CacheTierError(RuntimeError):
    """Base error for fail-closed cache-tier governance operations."""


class CacheTierValidationError(CacheTierError):
    """Raised when a frozen cache-tier contract is invalid or has drifted."""


class CacheTierAuthorizationError(CacheTierError):
    """Raised when an executing owner token does not bind the frozen plan."""


def _more_protective_reference_class(left: str, right: str) -> str:
    left = _validate_reference_class(left)
    right = _validate_reference_class(right)
    return left if REFERENCE_PROTECTION_RANK[left] >= REFERENCE_PROTECTION_RANK[right] else right


@dataclasses.dataclass(frozen=True)
class CacheTierConfig:
    hot_root: Path = DEFAULT_HOT_ROOT
    cold_root: Path = DEFAULT_COLD_ROOT
    ledger_path: Path = DEFAULT_LEDGER_PATH
    hot_safety_reserve_bytes: int = DEFAULT_HOT_SAFETY_RESERVE_BYTES
    hot_target_free_bytes: int = DEFAULT_HOT_TARGET_FREE_BYTES
    cold_ttl_days: int = DEFAULT_COLD_TTL_DAYS
    allowed_cache_roots: tuple[str, ...] = DEFAULT_ALLOWED_CACHE_ROOTS
    symlink_mode: Literal["relative", "absolute"] = "relative"
    allow_unknown_migration: bool = False
    lock_timeout_s: float = 5.0

    @property
    def state_root(self) -> Path:
        return self.ledger_path.parent

    @property
    def lock_path(self) -> Path:
        return self.state_root / "cache_tier_lru.lock"

    @property
    def health_fallback_path(self) -> Path:
        return self.state_root / "health_errors.jsonl"

    @property
    def receipt_root(self) -> Path:
        return self.state_root / "receipts"

    @property
    def active_manifest_root(self) -> Path:
        return self.state_root / "active_manifests"

    @property
    def cold_ttl_ns(self) -> int:
        return int(self.cold_ttl_days * 86_400 * 1_000_000_000)

    @classmethod
    def from_environment(cls, *, cache_root: Path | None = None) -> CacheTierConfig:
        hot_root = Path(os.environ.get("NARROWGATE_CACHE_HOT_ROOT", DEFAULT_HOT_ROOT))
        cold_root = Path(os.environ.get("NARROWGATE_CACHE_COLD_ROOT", DEFAULT_COLD_ROOT))
        if cache_root is not None:
            lexical = _absolute_lexical(cache_root)
            if not (
                _is_relative_to(lexical, hot_root) or _is_relative_to(lexical, cold_root)
            ):
                hot_root = lexical
        default_state = hot_root / ".cache_tier_lru"
        ledger_path = Path(
            os.environ.get("NARROWGATE_CACHE_LEDGER_PATH", default_state / "access_ledger.sqlite3")
        )
        return cls(hot_root=hot_root, cold_root=cold_root, ledger_path=ledger_path)

    def validate(self, *, require_roots: bool = True) -> None:
        hot = _absolute_lexical(self.hot_root)
        cold = _absolute_lexical(self.cold_root)
        ledger = _absolute_lexical(self.ledger_path)
        if hot == cold or _is_relative_to(hot, cold) or _is_relative_to(cold, hot):
            raise CacheTierValidationError("hot and cold roots must be disjoint")
        if _is_relative_to(ledger, cold):
            raise CacheTierValidationError("the SQLite access ledger must remain on the hot filesystem")
        if self.hot_safety_reserve_bytes <= 0:
            raise CacheTierValidationError("hot safety reserve must be positive")
        if self.hot_target_free_bytes < self.hot_safety_reserve_bytes:
            raise CacheTierValidationError("hot target free bytes must be at least the safety reserve")
        if self.cold_ttl_days <= 0:
            raise CacheTierValidationError("cold TTL must be positive")
        if self.symlink_mode not in {"relative", "absolute"}:
            raise CacheTierValidationError("symlink_mode must be relative or absolute")
        if not self.allowed_cache_roots:
            raise CacheTierValidationError("at least one allowed cache root is required")
        for root in self.allowed_cache_roots:
            _validate_allowed_root(root)
        if require_roots:
            if not hot.is_dir():
                raise CacheTierValidationError(f"hot cache root is missing: {hot}")
            if not cold.is_dir():
                raise CacheTierValidationError(f"cold cache root is missing (fail-closed): {cold}")


@dataclasses.dataclass(frozen=True)
class CacheAccessRecord:
    logical_id: str
    relative_path: str
    tier: Literal["hot", "cold"]
    access_count: int
    last_access_ns: int
    size_bytes: int
    reference_class: str
    identity_sha256: str | None
    physical_path: str
    hot_link_path: str | None
    tier_since_ns: int
    cold_admitted_ns: int | None
    last_explicit_access_ns: int | None
    managed_by_lru: bool


def _absolute_lexical(path: str | os.PathLike[str]) -> Path:
    return Path(os.path.abspath(os.path.expanduser(os.fspath(path))))


def _is_relative_to(path: Path, root: Path) -> bool:
    path = _absolute_lexical(path)
    root = _absolute_lexical(root)
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _validate_sha256(value: str | None, *, field: str = "identity_sha256") -> str | None:
    if value is None:
        return None
    normalized = value.lower()
    if len(normalized) != 64 or any(ch not in "0123456789abcdef" for ch in normalized):
        raise CacheTierValidationError(f"{field} must be a lowercase-compatible SHA256")
    return normalized


def _validate_reference_class(value: str) -> str:
    if value not in REFERENCE_CLASSES:
        raise CacheTierValidationError(
            f"reference_class must be one of {sorted(REFERENCE_CLASSES)}, got {value!r}"
        )
    return value


def _validate_allowed_root(value: str) -> str:
    path = PurePosixPath(value)
    if path.is_absolute() or len(path.parts) != 1 or path.parts[0] in {"", ".", ".."}:
        raise CacheTierValidationError(f"allowed cache root must be one path component: {value!r}")
    if path.parts[0].lower() in FORBIDDEN_TOP_LEVELS:
        raise CacheTierValidationError(f"forbidden cache root: {value!r}")
    return path.parts[0]


def _validate_relative_path(relative_path: str, allowed_roots: Sequence[str]) -> str:
    value = PurePosixPath(relative_path)
    if value.is_absolute() or not value.parts or any(part in {"", ".", ".."} for part in value.parts):
        raise CacheTierValidationError(f"unsafe cache relative path: {relative_path!r}")
    if value.parts[0] not in allowed_roots:
        raise CacheTierValidationError(
            f"cache path {relative_path!r} is outside allowed roots {tuple(allowed_roots)!r}"
        )
    if value.parts[0].lower() in FORBIDDEN_TOP_LEVELS:
        raise CacheTierValidationError(f"forbidden cache path: {relative_path!r}")
    for part in value.parts[1:]:
        if part in allowed_roots:
            raise CacheTierValidationError(
                f"cache path recursively nests a managed root: {relative_path!r}"
            )
    return value.as_posix()


def _assert_no_forbidden_artifact_content(path: Path, allowed_roots: Sequence[str]) -> None:
    if not path.is_dir():
        return
    for entry in path.rglob("*"):
        if not entry.is_dir():
            continue
        is_recursive_root = entry.name in allowed_roots
        is_forbidden_material_root = entry.name.lower() in FORBIDDEN_TOP_LEVELS
        if (is_recursive_root or is_forbidden_material_root) and any(entry.iterdir()):
            raise CacheTierValidationError(
                f"artifact contains a forbidden or recursive material subtree: {entry}"
            )


def _strict_default() -> bool:
    return os.environ.get("NARROWGATE_CACHE_LEDGER_STRICT", "0").strip().lower() in {
        "1",
        "true",
        "yes",
    }


@contextlib.contextmanager
def _advisory_lock(
    path: Path,
    *,
    exclusive: bool,
    timeout_s: float,
) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    lock_kind = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
    deadline = time.monotonic() + timeout_s
    try:
        while True:
            try:
                fcntl.flock(fd, lock_kind | fcntl.LOCK_NB)
                break
            except BlockingIOError as error:
                if time.monotonic() >= deadline:
                    raise CacheTierValidationError(
                        f"cache-tier lock timed out: {path}"
                    ) from error
                time.sleep(0.01)
        yield
    finally:
        with contextlib.suppress(OSError):
            fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def _connect_ledger(config: CacheTierConfig, *, create: bool) -> sqlite3.Connection:
    if create:
        config.state_root.mkdir(parents=True, exist_ok=True)
    elif not config.ledger_path.is_file():
        raise CacheTierValidationError(f"cache access ledger is missing: {config.ledger_path}")
    connection = sqlite3.connect(config.ledger_path, timeout=config.lock_timeout_s)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute(f"PRAGMA busy_timeout={int(config.lock_timeout_s * 1000)}")
    return connection


def _initialize_ledger(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS artifacts (
            relative_path TEXT PRIMARY KEY,
            logical_id TEXT NOT NULL UNIQUE,
            tier TEXT NOT NULL CHECK (tier IN ('hot', 'cold')),
            access_count INTEGER NOT NULL CHECK (access_count >= 0),
            last_access_ns INTEGER NOT NULL CHECK (last_access_ns >= 0),
            size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
            reference_class TEXT NOT NULL,
            identity_sha256 TEXT,
            physical_path TEXT NOT NULL,
            hot_link_path TEXT,
            tier_since_ns INTEGER NOT NULL,
            cold_admitted_ns INTEGER,
            last_explicit_access_ns INTEGER,
            managed_by_lru INTEGER NOT NULL DEFAULT 0 CHECK (managed_by_lru IN (0, 1)),
            first_seen_ns INTEGER NOT NULL,
            updated_ns INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS artifacts_lru_idx
            ON artifacts(tier, last_access_ns, access_count, relative_path);
        CREATE INDEX IF NOT EXISTS artifacts_reference_idx
            ON artifacts(reference_class, tier, last_access_ns);
        CREATE TABLE IF NOT EXISTS health_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_ns INTEGER NOT NULL,
            operation TEXT NOT NULL,
            severity TEXT NOT NULL,
            path TEXT,
            error_type TEXT,
            detail TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS operation_receipts (
            receipt_sha256 TEXT PRIMARY KEY,
            created_ns INTEGER NOT NULL,
            operation TEXT NOT NULL,
            plan_sha256 TEXT NOT NULL,
            receipt_path TEXT NOT NULL,
            status TEXT NOT NULL
        );
        """
    )
    existing_columns = {
        row[1] for row in connection.execute("PRAGMA table_info(artifacts)").fetchall()
    }
    migrations = {
        "tier_since_ns": "ALTER TABLE artifacts ADD COLUMN tier_since_ns INTEGER NOT NULL DEFAULT 0",
        "cold_admitted_ns": "ALTER TABLE artifacts ADD COLUMN cold_admitted_ns INTEGER",
        "last_explicit_access_ns": (
            "ALTER TABLE artifacts ADD COLUMN last_explicit_access_ns INTEGER"
        ),
        "managed_by_lru": (
            "ALTER TABLE artifacts ADD COLUMN managed_by_lru INTEGER NOT NULL DEFAULT 0"
        ),
    }
    for column, statement in migrations.items():
        if column not in existing_columns:
            connection.execute(statement)
    row = connection.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone()
    if row is not None and row["value"] != SCHEMA_VERSION:
        raise CacheTierValidationError(
            f"ledger schema mismatch: expected {SCHEMA_VERSION}, got {row['value']}"
        )
    connection.execute(
        "INSERT OR REPLACE INTO metadata(key, value) VALUES('schema_version', ?)",
        (SCHEMA_VERSION,),
    )
    connection.commit()


def _record_health_best_effort(
    config: CacheTierConfig,
    *,
    operation: str,
    error: BaseException,
    path: str | None,
) -> None:
    payload = {
        "event_ns": time.time_ns(),
        "operation": operation,
        "severity": "error",
        "path": path,
        "error_type": type(error).__name__,
        "detail": str(error),
    }
    try:
        connection = _connect_ledger(config, create=True)
        try:
            _initialize_ledger(connection)
            connection.execute(
                """
                INSERT INTO health_events(
                    event_ns, operation, severity, path, error_type, detail
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                tuple(payload[key] for key in payload),
            )
            connection.commit()
            return
        finally:
            connection.close()
    except Exception:
        pass
    try:
        config.state_root.mkdir(parents=True, exist_ok=True)
        fd = os.open(
            config.health_fallback_path,
            os.O_APPEND | os.O_CREAT | os.O_WRONLY,
            0o600,
        )
        try:
            os.write(fd, _canonical_json(payload) + b"\n")
            os.fsync(fd)
        finally:
            os.close(fd)
    except Exception:
        pass


def _path_size_bytes(path: Path) -> int:
    if path.is_symlink():
        raise CacheTierValidationError(f"internal symlink is not a material cache artifact: {path}")
    if path.is_file():
        return path.stat().st_size
    if not path.is_dir():
        raise CacheTierValidationError(f"cache artifact is neither file nor directory: {path}")
    total = 0
    for entry in path.rglob("*"):
        if entry.is_symlink():
            raise CacheTierValidationError(f"internal artifact symlink is forbidden: {entry}")
        if entry.is_file():
            total += entry.stat().st_size
    return total


def _artifact_location(
    path: Path,
    *,
    config: CacheTierConfig,
    cache_root: Path | None,
) -> tuple[str, Literal["hot", "cold"], Path, Path | None]:
    lexical = _absolute_lexical(path)
    hot = _absolute_lexical(config.hot_root)
    cold = _absolute_lexical(config.cold_root)
    explicit_root = _absolute_lexical(cache_root) if cache_root is not None else None

    if explicit_root is not None:
        if not _is_relative_to(lexical, explicit_root):
            raise CacheTierValidationError(f"cache path is outside cache_root: {lexical}")
        tail = lexical.relative_to(explicit_root).as_posix()
        if explicit_root.name in config.allowed_cache_roots:
            relative = f"{explicit_root.name}/{tail}" if tail != "." else explicit_root.name
        else:
            relative = tail
    elif _is_relative_to(lexical, hot):
        relative = lexical.relative_to(hot).as_posix()
    elif _is_relative_to(lexical, cold):
        relative = lexical.relative_to(cold).as_posix()
    else:
        raise CacheTierValidationError(f"cache path is outside the configured tiers: {lexical}")
    relative = _validate_relative_path(relative, config.allowed_cache_roots)

    if _is_relative_to(lexical, cold):
        hot_link = hot / relative
        if not (
            hot_link.is_symlink()
            and hot_link.resolve(strict=False) == lexical.resolve(strict=False)
        ):
            hot_link = None
        return relative, "cold", lexical, hot_link
    resolved = lexical.resolve(strict=False)
    if _is_relative_to(resolved, cold):
        return relative, "cold", resolved, lexical
    if _is_relative_to(resolved, hot):
        return relative, "hot", lexical, None
    raise CacheTierValidationError(
        f"cache path resolves outside the configured tiers: {lexical} -> {resolved}"
    )


def _logical_id(relative_path: str, identity_sha256: str | None) -> str:
    if identity_sha256 is not None:
        return f"sha256:{identity_sha256}"
    return f"path-sha256:{_sha256_bytes(relative_path.encode())}"


def _row_to_record(row: sqlite3.Row) -> CacheAccessRecord:
    return CacheAccessRecord(
        logical_id=row["logical_id"],
        relative_path=row["relative_path"],
        tier=row["tier"],
        access_count=int(row["access_count"]),
        last_access_ns=int(row["last_access_ns"]),
        size_bytes=int(row["size_bytes"]),
        reference_class=row["reference_class"],
        identity_sha256=row["identity_sha256"],
        physical_path=row["physical_path"],
        hot_link_path=row["hot_link_path"],
        tier_since_ns=int(row["tier_since_ns"]),
        cold_admitted_ns=(
            int(row["cold_admitted_ns"]) if row["cold_admitted_ns"] is not None else None
        ),
        last_explicit_access_ns=(
            int(row["last_explicit_access_ns"])
            if row["last_explicit_access_ns"] is not None
            else None
        ),
        managed_by_lru=bool(row["managed_by_lru"]),
    )


def _upsert_access(
    config: CacheTierConfig,
    *,
    path: Path,
    cache_root: Path | None,
    identity_sha256: str | None,
    reference_class: str,
    increment_access: bool,
    size_bytes: int | None,
    event_ns: int | None = None,
) -> CacheAccessRecord:
    identity_sha256 = _validate_sha256(identity_sha256)
    reference_class = _validate_reference_class(reference_class)
    relative, tier, physical, hot_link = _artifact_location(
        path,
        config=config,
        cache_root=cache_root,
    )
    if not physical.exists():
        raise CacheTierValidationError(f"cache artifact does not exist: {physical}")
    now_ns = time.time_ns() if event_ns is None else int(event_ns)
    with _advisory_lock(config.lock_path, exclusive=False, timeout_s=config.lock_timeout_s):
        connection = _connect_ledger(config, create=True)
        try:
            _initialize_ledger(connection)
            prior = connection.execute(
                "SELECT * FROM artifacts WHERE relative_path=?", (relative,)
            ).fetchone()
            coalesced_to_ancestor = False
            if prior is None:
                requested = PurePosixPath(relative)
                ancestors = [
                    row
                    for row in connection.execute("SELECT * FROM artifacts")
                    if PurePosixPath(row["relative_path"]) in requested.parents
                ]
                if ancestors:
                    prior = max(
                        ancestors,
                        key=lambda row: len(PurePosixPath(row["relative_path"]).parts),
                    )
                    relative = prior["relative_path"]
                    tier = prior["tier"]
                    physical = Path(prior["physical_path"])
                    hot_link = (
                        Path(prior["hot_link_path"])
                        if prior["hot_link_path"] is not None
                        else None
                    )
                    identity_sha256 = prior["identity_sha256"]
                    coalesced_to_ancestor = True
            if not physical.exists():
                raise CacheTierValidationError(
                    f"managed cache artifact does not exist: {physical}"
                )
            if coalesced_to_ancestor and increment_access:
                measured_size = int(prior["size_bytes"])
            elif coalesced_to_ancestor:
                measured_size = _path_size_bytes(physical)
            else:
                measured_size = (
                    _path_size_bytes(physical) if size_bytes is None else int(size_bytes)
                )
            if measured_size < 0:
                raise CacheTierValidationError("size_bytes must be non-negative")
            logical_id = _logical_id(relative, identity_sha256)
            if prior is None:
                access_count = 1 if increment_access else 0
                last_access_ns = now_ns
                first_seen_ns = now_ns
                tier_since_ns = now_ns
                cold_admitted_ns = None
                last_explicit_access_ns = now_ns if increment_access else None
                managed_by_lru = 0
            else:
                access_count = int(prior["access_count"]) + (1 if increment_access else 0)
                last_access_ns = now_ns if increment_access else int(prior["last_access_ns"])
                first_seen_ns = int(prior["first_seen_ns"])
                tier_since_ns = (
                    int(prior["tier_since_ns"]) if prior["tier"] == tier else now_ns
                )
                cold_admitted_ns = prior["cold_admitted_ns"]
                last_explicit_access_ns = (
                    now_ns if increment_access else prior["last_explicit_access_ns"]
                )
                managed_by_lru = int(prior["managed_by_lru"])
                if identity_sha256 is None:
                    identity_sha256 = prior["identity_sha256"]
                    logical_id = _logical_id(relative, identity_sha256)
                reference_class = (
                    prior["reference_class"]
                    if reference_class == "unknown"
                    else _more_protective_reference_class(
                        prior["reference_class"], reference_class
                    )
                )
            connection.execute(
                """
                INSERT INTO artifacts(
                    relative_path, logical_id, tier, access_count, last_access_ns,
                    size_bytes, reference_class, identity_sha256, physical_path,
                    hot_link_path, tier_since_ns, cold_admitted_ns,
                    last_explicit_access_ns, managed_by_lru, first_seen_ns, updated_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(relative_path) DO UPDATE SET
                    logical_id=excluded.logical_id,
                    tier=excluded.tier,
                    access_count=excluded.access_count,
                    last_access_ns=excluded.last_access_ns,
                    size_bytes=excluded.size_bytes,
                    reference_class=excluded.reference_class,
                    identity_sha256=excluded.identity_sha256,
                    physical_path=excluded.physical_path,
                    hot_link_path=excluded.hot_link_path,
                    tier_since_ns=excluded.tier_since_ns,
                    cold_admitted_ns=excluded.cold_admitted_ns,
                    last_explicit_access_ns=excluded.last_explicit_access_ns,
                    managed_by_lru=excluded.managed_by_lru,
                    updated_ns=excluded.updated_ns
                """,
                (
                    relative,
                    logical_id,
                    tier,
                    access_count,
                    last_access_ns,
                    measured_size,
                    reference_class,
                    identity_sha256,
                    str(physical),
                    str(hot_link) if hot_link is not None else None,
                    tier_since_ns,
                    cold_admitted_ns,
                    last_explicit_access_ns,
                    managed_by_lru,
                    first_seen_ns,
                    now_ns,
                ),
            )
            connection.commit()
            row = connection.execute(
                "SELECT * FROM artifacts WHERE relative_path=?", (relative,)
            ).fetchone()
            assert row is not None
            return _row_to_record(row)
        finally:
            connection.close()


def record_cache_access(
    path: str | os.PathLike[str],
    *,
    cache_root: str | os.PathLike[str] | None = None,
    identity_sha256: str | None = None,
    reference_class: str = "unknown",
    strict: bool | None = None,
) -> CacheAccessRecord | None:
    """Record a cache hit without changing cache read semantics on ledger failure."""

    strict = _strict_default() if strict is None else strict
    root = Path(cache_root) if cache_root is not None else None
    config = CacheTierConfig.from_environment(cache_root=root)
    try:
        config.validate(require_roots=False)
        return _upsert_access(
            config,
            path=Path(path),
            cache_root=root,
            identity_sha256=identity_sha256,
            reference_class=reference_class,
            increment_access=True,
            size_bytes=None,
        )
    except Exception as error:
        _record_health_best_effort(
            config,
            operation="record_cache_access",
            error=error,
            path=os.fspath(path),
        )
        if strict:
            raise
        return None


def register_cache_write(
    path: str | os.PathLike[str],
    *,
    cache_root: str | os.PathLike[str] | None = None,
    identity_sha256: str | None = None,
    reference_class: str = "unknown",
    size_bytes: int | None = None,
    strict: bool | None = None,
) -> CacheAccessRecord | None:
    """Register a completed cache write; ledger failure is non-semantic by default."""

    strict = _strict_default() if strict is None else strict
    root = Path(cache_root) if cache_root is not None else None
    config = CacheTierConfig.from_environment(cache_root=root)
    try:
        config.validate(require_roots=False)
        return _upsert_access(
            config,
            path=Path(path),
            cache_root=root,
            identity_sha256=identity_sha256,
            reference_class=reference_class,
            increment_access=True,
            size_bytes=size_bytes,
        )
    except Exception as error:
        _record_health_best_effort(
            config,
            operation="register_cache_write",
            error=error,
            path=os.fspath(path),
        )
        if strict:
            raise
        return None


def _manifest_paths(path: Path) -> list[Path]:
    if path.is_file():
        return [path] if "manifest" in path.name.lower() and path.suffix == ".json" else []
    return sorted(
        entry
        for entry in path.rglob("*.json")
        if "manifest" in entry.name.lower() and entry.is_file()
    )


def _direct_manifest_paths(path: Path) -> list[Path]:
    if path.is_file():
        return _manifest_paths(path)
    return sorted(
        entry
        for entry in path.iterdir()
        if entry.is_file()
        and entry.suffix == ".json"
        and "manifest" in entry.name.lower()
    )


def _nested_manifest_artifact_roots(
    path: Path,
    *,
    forced_artifacts: frozenset[Path] = frozenset(),
) -> list[Path]:
    if _absolute_lexical(path) in forced_artifacts:
        return [path]
    if path.is_symlink() or path.is_file():
        return [path]
    if _direct_manifest_paths(path):
        return [path]
    roots: list[Path] = []
    for entry in sorted(path.iterdir()):
        if entry.is_symlink():
            roots.append(entry)
        elif entry.is_dir():
            roots.extend(
                _nested_manifest_artifact_roots(
                    entry,
                    forced_artifacts=forced_artifacts,
                )
            )
    return roots


def _scan_artifact_candidates(
    path: Path,
    *,
    forced_artifacts: frozenset[Path] = frozenset(),
) -> list[Path]:
    if _absolute_lexical(path) in forced_artifacts:
        return [path]
    if path.is_symlink() or path.is_file():
        return [path]
    children = sorted(path.iterdir())
    if not children:
        return [path]
    candidates: list[Path] = []
    for child in children:
        if child.is_symlink() or child.is_file():
            candidates.append(child)
            continue
        manifest_roots = _nested_manifest_artifact_roots(
            child,
            forced_artifacts=forced_artifacts,
        )
        candidates.extend(manifest_roots or [child])
    return candidates


def _cold_boundaries_published_through_hot_symlinks(
    config: CacheTierConfig,
) -> frozenset[Path]:
    boundaries: set[Path] = set()
    for allowed_root in config.allowed_cache_roots:
        root = config.hot_root / allowed_root
        if not _lexists(root):
            continue
        entries = [root]
        if root.is_dir() and not root.is_symlink():
            entries.extend(root.rglob("*"))
        for entry in entries:
            if not entry.is_symlink():
                continue
            resolved = entry.resolve(strict=False)
            if not _is_relative_to(resolved, config.cold_root):
                continue
            relative = entry.relative_to(config.hot_root)
            boundaries.add(_absolute_lexical(config.cold_root / relative))
    return frozenset(boundaries)


def _manifest_declared_files(payload: object) -> list[Mapping[str, object]]:
    if not isinstance(payload, Mapping):
        return []
    records: list[Mapping[str, object]] = []
    for key in ("files", "artifacts", "outputs", "parts"):
        value = payload.get(key)
        if isinstance(value, list):
            records.extend(item for item in value if isinstance(item, Mapping))
    return records


def _validate_manifests(path: Path) -> list[dict[str, object]]:
    root = path if path.is_dir() else path.parent
    results: list[dict[str, object]] = []
    for manifest in _manifest_paths(path):
        try:
            payload = json.loads(manifest.read_text())
        except (OSError, json.JSONDecodeError) as error:
            raise CacheTierValidationError(f"invalid cache manifest {manifest}: {error}") from error
        checked_files = 0
        for record in _manifest_declared_files(payload):
            relative = record.get("path") or record.get("relative_path") or record.get("name")
            expected = record.get("sha256") or record.get("payload_sha256")
            if not isinstance(relative, str) or not isinstance(expected, str):
                continue
            expected = _validate_sha256(expected, field=f"manifest SHA256 in {manifest}")
            candidate = _absolute_lexical(root / relative)
            if not _is_relative_to(candidate, root) or not candidate.is_file():
                raise CacheTierValidationError(
                    f"manifest references a missing or escaping file: {manifest} -> {relative}"
                )
            actual = _sha256_file(candidate)
            if actual != expected:
                raise CacheTierValidationError(
                    f"manifest payload SHA256 mismatch: {candidate}: {actual} != {expected}"
                )
            checked_files += 1
        results.append(
            {
                "relative_path": manifest.relative_to(root).as_posix(),
                "payload_sha256": _sha256_file(manifest),
                "declared_files_checked": checked_files,
            }
        )
    return results


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normalize_audit_path(path_value: str, config: CacheTierConfig) -> str | None:
    path = _absolute_lexical(path_value)
    if not _is_relative_to(path, config.hot_root):
        return None
    return _validate_relative_path(
        path.relative_to(_absolute_lexical(config.hot_root)).as_posix(),
        config.allowed_cache_roots,
    )


def load_reference_audit_csv(
    path: Path,
    config: CacheTierConfig,
) -> tuple[dict[str, str], dict[str, object]]:
    path = _absolute_lexical(path)
    if not path.is_file():
        raise CacheTierValidationError(f"reference audit CSV is missing: {path}")
    digest = _sha256_file(path)
    classifications: dict[str, str] = {}
    unknown_rows: list[dict[str, str]] = []
    ignored_out_of_scope_paths: list[str] = []
    row_count = 0
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"classification", "path"}
        if reader.fieldnames is None or not required.issubset(reader.fieldnames):
            raise CacheTierValidationError(
                "reference audit CSV requires classification and path columns"
            )
        for row in reader:
            row_count += 1
            source_class = (row.get("classification") or "").strip()
            mapped = REFERENCE_AUDIT_CLASS_MAP.get(source_class)
            if mapped is None:
                mapped = "unknown"
                unknown_rows.append(
                    {"classification": source_class, "path": (row.get("path") or "").strip()}
                )
            path_values: list[str] = []
            primary = (row.get("path") or "").strip()
            if primary:
                path_values.append(primary)
            paths_json = (row.get("paths_json") or "").strip()
            if paths_json:
                try:
                    parsed = json.loads(paths_json)
                except json.JSONDecodeError as error:
                    raise CacheTierValidationError(
                        f"invalid paths_json in reference audit row {row_count}"
                    ) from error
                if not isinstance(parsed, list) or not all(isinstance(item, str) for item in parsed):
                    raise CacheTierValidationError(
                        f"paths_json must be a string list in reference audit row {row_count}"
                    )
                path_values.extend(parsed)
            if not path_values:
                unknown_rows.append({"classification": source_class, "path": ""})
                continue
            for raw_path in path_values:
                relative = _normalize_audit_path(raw_path, config)
                if relative is None:
                    ignored_out_of_scope_paths.append(str(_absolute_lexical(raw_path)))
                    continue
                classifications[relative] = _more_protective_reference_class(
                    classifications.get(relative, "unreferenced"), mapped
                )
    metadata = {
        "path": str(path),
        "sha256": digest,
        "size_bytes": path.stat().st_size,
        "row_count": row_count,
        "normalized_path_count": len(classifications),
        "ignored_out_of_scope_path_count": len(set(ignored_out_of_scope_paths)),
        "ignored_out_of_scope_paths": sorted(set(ignored_out_of_scope_paths)),
        "unknown_classification_rows": unknown_rows,
        "classification_map": REFERENCE_AUDIT_CLASS_MAP,
    }
    return classifications, metadata


def _reference_class_for_artifact(
    relative_path: str,
    *,
    audit_classes: Mapping[str, str],
    manual_classes: Mapping[str, str],
) -> tuple[str, str]:
    result = manual_classes.get(relative_path, "unknown")
    provenance = "manual" if relative_path in manual_classes else "unknown"
    if relative_path in audit_classes:
        audit_class = audit_classes[relative_path]
        result = (
            _more_protective_reference_class(result, audit_class)
            if relative_path in manual_classes
            else audit_class
        )
        provenance = "reference_audit_exact"
    artifact = PurePosixPath(relative_path)
    for audit_path, audit_class in audit_classes.items():
        if audit_path == relative_path:
            continue
        audited = PurePosixPath(audit_path)
        nested = artifact in audited.parents or audited in artifact.parents
        if not nested:
            continue
        if audit_class in {"referenced", "frozen"}:
            result = _more_protective_reference_class(result, audit_class)
            provenance = "reference_audit_nested_protective"
        else:
            result = _more_protective_reference_class(result, "unknown")
            provenance = "reference_audit_nested_unknown"
    return result, provenance


def artifact_fingerprint(path: Path) -> dict[str, object]:
    path = _absolute_lexical(path)
    if path.is_symlink():
        raise CacheTierValidationError(f"fingerprint expects material content, not symlink: {path}")
    if path.is_file():
        records = [
            {
                "path": ".",
                "mode": stat.S_IMODE(path.stat().st_mode),
                "size_bytes": path.stat().st_size,
                "sha256": _sha256_file(path),
            }
        ]
        kind = "file"
    elif path.is_dir():
        records = []
        for entry in sorted(path.rglob("*"), key=lambda item: item.relative_to(path).as_posix()):
            relative = entry.relative_to(path).as_posix()
            if entry.is_symlink():
                raise CacheTierValidationError(f"internal artifact symlink is forbidden: {entry}")
            if entry.is_dir():
                records.append(
                    {
                        "path": relative + "/",
                        "mode": stat.S_IMODE(entry.stat().st_mode),
                        "size_bytes": 0,
                        "sha256": None,
                    }
                )
            elif entry.is_file():
                records.append(
                    {
                        "path": relative,
                        "mode": stat.S_IMODE(entry.stat().st_mode),
                        "size_bytes": entry.stat().st_size,
                        "sha256": _sha256_file(entry),
                    }
                )
            else:
                raise CacheTierValidationError(f"unsupported cache entry type: {entry}")
        kind = "directory"
    else:
        raise CacheTierValidationError(f"cache artifact is missing: {path}")
    manifests = _validate_manifests(path)
    payload = {"kind": kind, "entries": records, "manifests": manifests}
    return {
        "kind": kind,
        "size_bytes": sum(int(record["size_bytes"]) for record in records),
        "entry_count": len(records),
        "manifest_count": len(manifests),
        "content_sha256": _sha256_bytes(_canonical_json(payload)),
    }


def _infer_identity_sha256(path: Path) -> str | None:
    for manifest in _direct_manifest_paths(path):
        try:
            payload = json.loads(manifest.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, Mapping):
            continue
        for key in ("identity_sha256", "identity_hash", "artifact_sha256"):
            value = payload.get(key)
            if isinstance(value, str):
                with contextlib.suppress(CacheTierValidationError):
                    return _validate_sha256(value, field=key)
    return None


def _lexists(path: Path) -> bool:
    return os.path.lexists(path)


def scan_cache_tiers(
    config: CacheTierConfig,
    *,
    reference_classes: Mapping[str, str] | None = None,
    reference_audit_csv: Path | None = None,
) -> dict[str, object]:
    config.validate(require_roots=True)
    reference_classes = dict(reference_classes or {})
    for relative, value in reference_classes.items():
        _validate_relative_path(relative, config.allowed_cache_roots)
        _validate_reference_class(value)
    audit_classes: dict[str, str] = {}
    audit_metadata: dict[str, object] | None = None
    if reference_audit_csv is not None:
        audit_classes, audit_metadata = load_reference_audit_csv(reference_audit_csv, config)
    scanned: dict[str, CacheAccessRecord] = {}
    provenance_by_relative: dict[str, str] = {}
    physical_by_relative: dict[str, Path] = {}
    identity_by_relative: dict[str, str | None] = {}
    skipped_managed_parents: dict[str, list[str]] = {}
    identical_identity_tier_duplicates: dict[str, list[str]] = {}
    superseded_managed_parents: list[str] = []
    superseded_managed_descendants: list[str] = []
    with _advisory_lock(config.lock_path, exclusive=True, timeout_s=config.lock_timeout_s):
        candidates: list[tuple[Path, Path]] = []
        candidate_relatives: set[str] = set()
        forced_candidate_relatives: set[str] = set()
        cold_forced_artifacts = _cold_boundaries_published_through_hot_symlinks(config)
        for root in (config.hot_root, config.cold_root):
            for allowed_root in config.allowed_cache_roots:
                path = root / allowed_root
                if not _lexists(path):
                    continue
                forced_artifacts = (
                    cold_forced_artifacts if root == config.cold_root else frozenset()
                )
                for artifact in _scan_artifact_candidates(
                    path,
                    forced_artifacts=forced_artifacts,
                ):
                    relative, _, _, _ = _artifact_location(
                        artifact,
                        config=config,
                        cache_root=root,
                    )
                    candidates.append((root, artifact))
                    candidate_relatives.add(relative)
                    if _absolute_lexical(artifact) in forced_artifacts:
                        forced_candidate_relatives.add(relative)
        connection = _connect_ledger(config, create=True)
        try:
            _initialize_ledger(connection)
            existing_rows = list(
                connection.execute(
                    "SELECT relative_path, physical_path FROM artifacts"
                )
            )
            for row in existing_rows:
                existing = PurePosixPath(row["relative_path"])
                if row["relative_path"] in candidate_relatives:
                    continue
                supersedes_parent = any(
                    existing in PurePosixPath(item).parents
                    for item in candidate_relatives
                )
                supersedes_descendant = any(
                    PurePosixPath(item) in existing.parents
                    for item in forced_candidate_relatives
                )
                if supersedes_parent or supersedes_descendant:
                    connection.execute(
                        "DELETE FROM artifacts WHERE relative_path=?",
                        (row["relative_path"],),
                    )
                    if supersedes_parent:
                        superseded_managed_parents.append(row["relative_path"])
                    else:
                        superseded_managed_descendants.append(row["relative_path"])
            connection.commit()
            managed_paths = {
                row["relative_path"]
                for row in connection.execute(
                    "SELECT relative_path, physical_path FROM artifacts"
                )
                if _lexists(Path(row["physical_path"]))
            }
        finally:
            connection.close()
        for root, artifact in candidates:
            relative, _, physical, _ = _artifact_location(
                artifact,
                config=config,
                cache_root=root,
            )
            relative_path = PurePosixPath(relative)
            managed_descendants = sorted(
                candidate
                for candidate in managed_paths
                if relative_path in PurePosixPath(candidate).parents
            )
            if managed_descendants:
                skipped_managed_parents[relative] = managed_descendants
                continue
            _assert_no_forbidden_artifact_content(physical, config.allowed_cache_roots)
            identity = _infer_identity_sha256(physical)
            prior_physical = physical_by_relative.get(relative)
            if (
                prior_physical is not None
                and prior_physical.resolve(strict=False) != physical.resolve(strict=False)
            ):
                if identity is None or identity != identity_by_relative.get(relative):
                    raise CacheTierValidationError(
                        "duplicate cache artifact exists in both tiers with differing identity: "
                        f"{relative} -> {prior_physical} and {physical}"
                    )
                identical_identity_tier_duplicates[relative] = [
                    str(prior_physical),
                    str(physical),
                ]
                continue
            physical_by_relative[relative] = physical
            identity_by_relative[relative] = identity
            reference_class, provenance = _reference_class_for_artifact(
                relative,
                audit_classes=audit_classes,
                manual_classes=reference_classes,
            )
            record = _upsert_access_without_lock(
                config,
                path=artifact,
                cache_root=root,
                identity_sha256=identity,
                reference_class=reference_class,
            )
            scanned[relative] = record
            provenance_by_relative[relative] = provenance
            managed_paths.add(relative)
        connection = _connect_ledger(config, create=False)
        try:
            rows = connection.execute("SELECT relative_path, physical_path FROM artifacts").fetchall()
            stale = [row["relative_path"] for row in rows if not Path(row["physical_path"]).exists()]
            for relative in stale:
                connection.execute("DELETE FROM artifacts WHERE relative_path=?", (relative,))
            if audit_metadata is not None:
                connection.execute(
                    "INSERT OR REPLACE INTO metadata(key, value) VALUES('reference_audit', ?)",
                    (_canonical_json(audit_metadata).decode(),),
                )
            connection.commit()
        finally:
            connection.close()
    return {
        "schema_version": SCHEMA_VERSION,
        "hot_root": str(config.hot_root),
        "cold_root": str(config.cold_root),
        "ledger_path": str(config.ledger_path),
        "scanned_count": len(scanned),
        "skipped_managed_parent_artifacts": skipped_managed_parents,
        "superseded_managed_parent_rows_removed": sorted(
            set(superseded_managed_parents)
        ),
        "superseded_managed_descendant_rows_removed": sorted(
            set(superseded_managed_descendants)
        ),
        "identical_identity_tier_duplicates": identical_identity_tier_duplicates,
        "stale_ledger_rows_removed": sorted(stale),
        "reference_audit": audit_metadata,
        "artifacts": [
            {
                **dataclasses.asdict(scanned[key]),
                "reference_provenance": provenance_by_relative[key],
            }
            for key in sorted(scanned)
        ],
    }


def _upsert_access_without_lock(
    config: CacheTierConfig,
    *,
    path: Path,
    cache_root: Path,
    identity_sha256: str | None,
    reference_class: str,
) -> CacheAccessRecord:
    relative, tier, physical, hot_link = _artifact_location(
        path,
        config=config,
        cache_root=cache_root,
    )
    identity_sha256 = _validate_sha256(identity_sha256)
    reference_class = _validate_reference_class(reference_class)
    now_ns = time.time_ns()
    size_bytes = _path_size_bytes(physical)
    connection = _connect_ledger(config, create=True)
    try:
        _initialize_ledger(connection)
        prior = connection.execute(
            "SELECT * FROM artifacts WHERE relative_path=?", (relative,)
        ).fetchone()
        access_count = int(prior["access_count"]) if prior else 0
        last_access_ns = int(prior["last_access_ns"]) if prior else physical.stat().st_mtime_ns
        first_seen_ns = int(prior["first_seen_ns"]) if prior else now_ns
        tier_since_ns = (
            int(prior["tier_since_ns"])
            if prior is not None and prior["tier"] == tier
            else now_ns
        )
        cold_admitted_ns = prior["cold_admitted_ns"] if prior else None
        last_explicit_access_ns = prior["last_explicit_access_ns"] if prior else None
        managed_by_lru = int(prior["managed_by_lru"]) if prior else 0
        if prior and identity_sha256 is None:
            identity_sha256 = prior["identity_sha256"]
        if prior:
            reference_class = (
                prior["reference_class"]
                if reference_class == "unknown"
                else _more_protective_reference_class(
                    prior["reference_class"], reference_class
                )
            )
        logical_id = _logical_id(relative, identity_sha256)
        logical_conflict = connection.execute(
            "SELECT relative_path FROM artifacts WHERE logical_id=? AND relative_path<>?",
            (logical_id, relative),
        ).fetchone()
        if logical_conflict is not None:
            raise CacheTierValidationError(
                "cache logical identity is reused by multiple artifact paths: "
                f"{logical_id} -> {logical_conflict['relative_path']} and {relative}"
            )
        connection.execute(
            """
            INSERT INTO artifacts(
                relative_path, logical_id, tier, access_count, last_access_ns,
                size_bytes, reference_class, identity_sha256, physical_path,
                hot_link_path, tier_since_ns, cold_admitted_ns,
                last_explicit_access_ns, managed_by_lru, first_seen_ns, updated_ns
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(relative_path) DO UPDATE SET
                logical_id=excluded.logical_id,
                tier=excluded.tier,
                size_bytes=excluded.size_bytes,
                reference_class=excluded.reference_class,
                identity_sha256=excluded.identity_sha256,
                physical_path=excluded.physical_path,
                hot_link_path=excluded.hot_link_path,
                tier_since_ns=excluded.tier_since_ns,
                cold_admitted_ns=excluded.cold_admitted_ns,
                last_explicit_access_ns=excluded.last_explicit_access_ns,
                managed_by_lru=excluded.managed_by_lru,
                updated_ns=excluded.updated_ns
            """,
            (
                relative,
                logical_id,
                tier,
                access_count,
                last_access_ns,
                size_bytes,
                reference_class,
                identity_sha256,
                str(physical),
                str(hot_link) if hot_link else None,
                tier_since_ns,
                cold_admitted_ns,
                last_explicit_access_ns,
                managed_by_lru,
                first_seen_ns,
                now_ns,
            ),
        )
        connection.commit()
        row = connection.execute(
            "SELECT * FROM artifacts WHERE relative_path=?", (relative,)
        ).fetchone()
        assert row is not None
        return _row_to_record(row)
    finally:
        connection.close()


def list_artifacts(config: CacheTierConfig) -> list[CacheAccessRecord]:
    config.validate(require_roots=True)
    connection = _connect_ledger(config, create=False)
    try:
        _initialize_ledger(connection)
        return [
            _row_to_record(row)
            for row in connection.execute(
                "SELECT * FROM artifacts ORDER BY last_access_ns, access_count, relative_path"
            )
        ]
    finally:
        connection.close()


def _reference_audit_metadata(config: CacheTierConfig) -> dict[str, object] | None:
    connection = _connect_ledger(config, create=False)
    try:
        _initialize_ledger(connection)
        row = connection.execute(
            "SELECT value FROM metadata WHERE key='reference_audit'"
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        return None
    payload = json.loads(row["value"])
    if not isinstance(payload, dict):
        raise CacheTierValidationError("ledger reference_audit metadata is invalid")
    audit_path = Path(str(payload.get("path", "")))
    if not audit_path.is_file() or _sha256_file(audit_path) != payload.get("sha256"):
        raise CacheTierValidationError("reference audit CSV is missing or has drifted since scan")
    return payload


def _validate_no_overlapping_records(records: Sequence[CacheAccessRecord]) -> None:
    ordered = sorted(
        [(PurePosixPath(record.relative_path), record) for record in records],
        key=lambda item: item[0].parts,
    )
    for index, (path, _) in enumerate(ordered):
        for other, _ in ordered[index + 1 :]:
            if len(other.parts) <= len(path.parts):
                continue
            if path in other.parents:
                raise CacheTierValidationError(
                    "overlapping managed cache artifacts are forbidden: "
                    f"{path.as_posix()} is an ancestor of {other.as_posix()}"
                )


def _plan_body(plan: Mapping[str, object]) -> dict[str, object]:
    return {key: value for key, value in plan.items() if key not in {"plan_sha256", "authorization"}}


def compute_plan_sha256(plan: Mapping[str, object]) -> str:
    return _sha256_bytes(_canonical_json(_plan_body(plan)))


def owner_token_for_plan(plan: Mapping[str, object], operation: str) -> str:
    if operation not in OPERATIONS:
        raise CacheTierValidationError(f"unsupported operation: {operation}")
    plan_sha = str(plan.get("plan_sha256") or compute_plan_sha256(plan))
    return f"OWNER-{operation.upper()}-{plan_sha}"


def _operation_record(record: CacheAccessRecord, config: CacheTierConfig) -> dict[str, object]:
    relative = _validate_relative_path(record.relative_path, config.allowed_cache_roots)
    physical = Path(record.physical_path)
    _assert_no_forbidden_artifact_content(physical, config.allowed_cache_roots)
    fingerprint = artifact_fingerprint(physical)
    return {
        "relative_path": relative,
        "logical_id": record.logical_id,
        "tier": record.tier,
        "source_path": str(physical),
        "destination_path": str(config.cold_root / relative),
        "hot_link_path": str(config.hot_root / relative),
        "size_bytes": record.size_bytes,
        "access_count": record.access_count,
        "last_access_ns": record.last_access_ns,
        "reference_class": record.reference_class,
        "identity_sha256": record.identity_sha256,
        "tier_since_ns": record.tier_since_ns,
        "cold_admitted_ns": record.cold_admitted_ns,
        "last_explicit_access_ns": record.last_explicit_access_ns,
        "managed_by_lru": record.managed_by_lru,
        **fingerprint,
    }


def _active_manifest_sha256(payload: Mapping[str, object]) -> str:
    body = {key: value for key, value in payload.items() if key != "manifest_sha256"}
    return _sha256_bytes(_canonical_json(body))


def _validate_active_run_id(value: str) -> str:
    allowed = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.")
    if not value or value.startswith(".") or any(character not in allowed for character in value):
        raise CacheTierValidationError(f"unsafe active cache run id: {value!r}")
    return value


def _active_relative_path(
    value: str | os.PathLike[str],
    config: CacheTierConfig,
) -> str:
    path = Path(value).expanduser()
    if not path.is_absolute():
        return _validate_relative_path(path.as_posix(), config.allowed_cache_roots)
    return _artifact_location(path, config=config, cache_root=None)[0]


def write_active_cache_manifest(
    config: CacheTierConfig,
    *,
    run_id: str,
    protected_paths: Sequence[str | os.PathLike[str]],
    ttl_s: float,
    identity_sha256: str | None = None,
    now_ns: int | None = None,
) -> Path:
    """Atomically publish cache roots that a running task must retain in place."""

    config.validate(require_roots=False)
    run_id = _validate_active_run_id(run_id)
    if ttl_s <= 0:
        raise CacheTierValidationError("active cache manifest TTL must be positive")
    if identity_sha256 is not None and (
        len(identity_sha256) != 64
        or any(character not in "0123456789abcdef" for character in identity_sha256)
    ):
        raise CacheTierValidationError("active cache identity must be a lowercase SHA256")
    relative_paths = sorted(
        {_active_relative_path(value, config) for value in protected_paths}
    )
    if not relative_paths:
        raise CacheTierValidationError("active cache manifest protects no paths")
    now_ns = time.time_ns() if now_ns is None else int(now_ns)
    payload: dict[str, object] = {
        "schema_version": ACTIVE_MANIFEST_SCHEMA_VERSION,
        "run_id": run_id,
        "identity_sha256": identity_sha256,
        "heartbeat_ns": now_ns,
        "expires_ns": now_ns + int(ttl_s * 1_000_000_000),
        "protected_relative_paths": relative_paths,
    }
    payload["manifest_sha256"] = _active_manifest_sha256(payload)
    path = config.active_manifest_root / f"{run_id}.json"
    with _advisory_lock(config.lock_path, exclusive=True, timeout_s=config.lock_timeout_s):
        _atomic_write_json(path, payload)
    return path


def refresh_active_cache_manifest(
    config: CacheTierConfig,
    path: Path,
    *,
    ttl_s: float,
    now_ns: int | None = None,
) -> Path:
    """Refresh one valid active manifest without changing its protected path set."""

    payload = _load_active_cache_manifest(path, config=config)
    return write_active_cache_manifest(
        config,
        run_id=str(payload["run_id"]),
        protected_paths=list(payload["protected_relative_paths"]),
        ttl_s=ttl_s,
        identity_sha256=(
            str(payload["identity_sha256"])
            if payload.get("identity_sha256") is not None
            else None
        ),
        now_ns=now_ns,
    )


def remove_active_cache_manifest(config: CacheTierConfig, path: Path) -> None:
    """Remove one active manifest after validating its bytes and governed location."""

    resolved = path.expanduser().resolve()
    root = config.active_manifest_root.expanduser().resolve()
    if resolved.parent != root:
        raise CacheTierValidationError("active cache manifest escaped its governed root")
    _load_active_cache_manifest(resolved, config=config)
    with _advisory_lock(config.lock_path, exclusive=True, timeout_s=config.lock_timeout_s):
        resolved.unlink(missing_ok=False)
        _fsync_directory(root)


@contextlib.contextmanager
def active_cache_manifest(
    config: CacheTierConfig,
    *,
    run_id: str,
    protected_paths: Sequence[str | os.PathLike[str]],
    ttl_s: float,
    identity_sha256: str | None = None,
) -> Iterator[Path]:
    """Protect cache paths for a scoped task and remove the marker on clean exit."""

    path = write_active_cache_manifest(
        config,
        run_id=run_id,
        protected_paths=protected_paths,
        ttl_s=ttl_s,
        identity_sha256=identity_sha256,
    )
    try:
        yield path
    finally:
        with contextlib.suppress(FileNotFoundError):
            remove_active_cache_manifest(config, path)


def _load_active_cache_manifest(
    path: Path,
    *,
    config: CacheTierConfig,
) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CacheTierValidationError(f"invalid active cache manifest: {path}") from exc
    if not isinstance(payload, dict):
        raise CacheTierValidationError(f"active cache manifest must be an object: {path}")
    if payload.get("schema_version") != ACTIVE_MANIFEST_SCHEMA_VERSION:
        raise CacheTierValidationError("active cache manifest schema drifted")
    run_id = _validate_active_run_id(str(payload.get("run_id", "")))
    if path.name != f"{run_id}.json":
        raise CacheTierValidationError("active cache manifest filename drifted")
    if payload.get("manifest_sha256") != _active_manifest_sha256(payload):
        raise CacheTierValidationError("active cache manifest SHA256 drifted")
    heartbeat_ns = payload.get("heartbeat_ns")
    expires_ns = payload.get("expires_ns")
    if (
        not isinstance(heartbeat_ns, int)
        or not isinstance(expires_ns, int)
        or heartbeat_ns < 0
        or expires_ns <= heartbeat_ns
    ):
        raise CacheTierValidationError("active cache manifest clock is malformed")
    paths = payload.get("protected_relative_paths")
    if not isinstance(paths, list) or not paths:
        raise CacheTierValidationError("active cache manifest protects no paths")
    normalized = [
        _validate_relative_path(str(value), config.allowed_cache_roots) for value in paths
    ]
    if normalized != sorted(set(normalized)):
        raise CacheTierValidationError("active cache manifest paths are not canonical")
    identity = payload.get("identity_sha256")
    if identity is not None and (
        not isinstance(identity, str)
        or len(identity) != 64
        or any(character not in "0123456789abcdef" for character in identity)
    ):
        raise CacheTierValidationError("active cache manifest identity is malformed")
    return payload


def active_cache_protection_snapshot(
    config: CacheTierConfig,
    *,
    now_ns: int | None = None,
) -> dict[str, object]:
    """Return the active semantic protection set; malformed rows fail closed."""

    now_ns = time.time_ns() if now_ns is None else int(now_ns)
    root = config.active_manifest_root
    if not root.exists():
        return {"active_manifests": [], "protected_relative_paths": []}
    if not root.is_dir():
        raise CacheTierValidationError("active cache manifest root is not a directory")
    active: list[dict[str, object]] = []
    protected: set[str] = set()
    for path in sorted(root.glob("*.json")):
        payload = _load_active_cache_manifest(path, config=config)
        if int(payload["expires_ns"]) <= now_ns:
            continue
        paths = [str(value) for value in payload["protected_relative_paths"]]
        protected.update(paths)
        active.append(
            {
                "run_id": str(payload["run_id"]),
                "identity_sha256": payload.get("identity_sha256"),
                "protected_relative_paths": paths,
            }
        )
    return {
        "active_manifests": active,
        "protected_relative_paths": sorted(protected),
    }


def _overlaps_active_protection(
    relative_path: str,
    protected_relative_paths: Sequence[str],
) -> bool:
    artifact = PurePosixPath(relative_path)
    for value in protected_relative_paths:
        protected = PurePosixPath(value)
        if artifact == protected or artifact in protected.parents or protected in artifact.parents:
            return True
    return False


def build_cache_tier_plan(
    config: CacheTierConfig,
    *,
    include_migrations: bool = True,
    include_deletions: bool = True,
    now_ns: int | None = None,
    hot_free_bytes: int | None = None,
    cold_free_bytes: int | None = None,
) -> dict[str, object]:
    config.validate(require_roots=True)
    now_ns = time.time_ns() if now_ns is None else int(now_ns)
    records = list_artifacts(config)
    _validate_no_overlapping_records(records)
    reference_audit = _reference_audit_metadata(config)
    active_protection = active_cache_protection_snapshot(config, now_ns=now_ns)
    protected_relative_paths = [
        str(value) for value in active_protection["protected_relative_paths"]
    ]
    hot_usage = shutil.disk_usage(config.hot_root)
    cold_usage = shutil.disk_usage(config.cold_root)
    actual_hot_free = int(hot_usage.free if hot_free_bytes is None else hot_free_bytes)
    actual_cold_free = int(cold_usage.free if cold_free_bytes is None else cold_free_bytes)

    migrations: list[dict[str, object]] = []
    migration_exclusions: list[dict[str, object]] = []
    bytes_to_reclaim = 0
    if include_migrations and actual_hot_free < config.hot_safety_reserve_bytes:
        bytes_to_reclaim = config.hot_target_free_bytes - actual_hot_free
        reclaimed = 0
        migration_candidates: list[CacheAccessRecord] = []
        for record in records:
            if record.tier != "hot":
                continue
            if _overlaps_active_protection(record.relative_path, protected_relative_paths):
                migration_exclusions.append(
                    {
                        "relative_path": record.relative_path,
                        "reason": "active_manifest_protection",
                        "detail": "artifact overlaps a cache path protected by an active run",
                    }
                )
                continue
            if record.reference_class == "unknown" and not config.allow_unknown_migration:
                continue
            if Path(record.physical_path).exists() and not Path(
                record.physical_path
            ).is_symlink():
                migration_candidates.append(record)
        migration_candidates = sorted(
            migration_candidates,
            key=lambda record: (record.last_access_ns, record.access_count, record.relative_path),
        )
        for record in migration_candidates:
            try:
                operation = _operation_record(record, config)
            except CacheTierValidationError as error:
                migration_exclusions.append(
                    {
                        "relative_path": record.relative_path,
                        "reason": "artifact_validation_failed",
                        "detail": str(error),
                    }
                )
                continue
            destination = Path(str(operation["destination_path"]))
            if destination.exists() and not _same_fingerprint(destination, operation):
                migration_exclusions.append(
                    {
                        "relative_path": record.relative_path,
                        "reason": "cold_destination_content_conflict",
                        "detail": (
                            "cold destination already exists with a different "
                            f"physical fingerprint: {destination}"
                        ),
                    }
                )
                continue
            operation["operation"] = "migrate"
            operation["reason"] = "hot_free_below_safety_reserve_lru"
            migrations.append(operation)
            reclaimed += record.size_bytes
            if reclaimed >= bytes_to_reclaim:
                break
        if reclaimed < bytes_to_reclaim:
            exclusion_detail = "; ".join(
                str(item["detail"]) for item in migration_exclusions
            )
            raise CacheTierValidationError(
                "insufficient eligible hot cache bytes to reach the configured target free space"
                + (f"; excluded artifacts: {exclusion_detail}" if exclusion_detail else "")
            )
        if sum(int(item["size_bytes"]) for item in migrations) > actual_cold_free:
            raise CacheTierValidationError("cold tier lacks space for the frozen migration plan")

    deletions: list[dict[str, object]] = []
    deletion_exclusions: list[dict[str, object]] = []
    if include_deletions:
        cutoff_ns = now_ns - config.cold_ttl_ns
        deletion_candidates: list[CacheAccessRecord] = []
        for record in records:
            if not (
                record.tier == "cold"
                and record.reference_class == "unreferenced"
                and record.managed_by_lru
                and record.cold_admitted_ns is not None
                and record.cold_admitted_ns <= cutoff_ns
                and (
                    record.last_explicit_access_ns is None
                    or record.last_explicit_access_ns <= cutoff_ns
                )
                and record.hot_link_path is not None
                and Path(record.hot_link_path).is_symlink()
                and Path(record.hot_link_path).resolve(strict=False)
                == Path(record.physical_path).resolve(strict=False)
                and Path(record.physical_path).exists()
            ):
                continue
            if _overlaps_active_protection(record.relative_path, protected_relative_paths):
                deletion_exclusions.append(
                    {
                        "relative_path": record.relative_path,
                        "reason": "active_manifest_protection",
                        "detail": "artifact overlaps a cache path protected by an active run",
                    }
                )
                continue
            deletion_candidates.append(record)
        deletion_candidates = sorted(
            deletion_candidates,
            key=lambda record: (record.last_access_ns, record.access_count, record.relative_path),
        )
        for record in deletion_candidates:
            try:
                operation = _operation_record(record, config)
            except CacheTierValidationError as error:
                deletion_exclusions.append(
                    {
                        "relative_path": record.relative_path,
                        "reason": "artifact_validation_failed",
                        "detail": str(error),
                    }
                )
                continue
            operation["operation"] = "delete"
            operation["reason"] = "cold_unreferenced_inactive_beyond_ttl"
            deletions.append(operation)

    plan: dict[str, object] = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "created_ns": now_ns,
        "dry_run": True,
        "hot_root": str(_absolute_lexical(config.hot_root)),
        "cold_root": str(_absolute_lexical(config.cold_root)),
        "ledger_path": str(_absolute_lexical(config.ledger_path)),
        "allowed_cache_roots": list(config.allowed_cache_roots),
        "symlink_mode": config.symlink_mode,
        "allow_unknown_migration": config.allow_unknown_migration,
        "reference_provenance": reference_audit,
        "active_cache_protection": active_protection,
        "thresholds": {
            "hot_safety_reserve_bytes": config.hot_safety_reserve_bytes,
            "hot_target_free_bytes": config.hot_target_free_bytes,
            "cold_ttl_days": config.cold_ttl_days,
        },
        "filesystem_snapshot": {
            "hot_free_bytes": actual_hot_free,
            "cold_free_bytes": actual_cold_free,
            "hot_bytes_to_reclaim": max(0, bytes_to_reclaim),
        },
        "migrations": migrations,
        "migration_exclusions": migration_exclusions,
        "deletions": deletions,
        "deletion_exclusions": deletion_exclusions,
        "permissions": {
            "migration_execution_authorized": False,
            "deletion_execution_authorized": False,
        },
    }
    plan_sha = compute_plan_sha256(plan)
    plan["plan_sha256"] = plan_sha
    plan["authorization"] = {
        "migration_owner_token_sha256": _sha256_bytes(
            owner_token_for_plan(plan, "migrate").encode()
        ),
        "deletion_owner_token_sha256": _sha256_bytes(owner_token_for_plan(plan, "delete").encode()),
        "token_format": "OWNER-<OPERATION>-<plan_sha256>",
    }
    return plan


def validate_plan(plan: Mapping[str, object], config: CacheTierConfig) -> None:
    config.validate(require_roots=True)
    if plan.get("schema_version") != PLAN_SCHEMA_VERSION:
        raise CacheTierValidationError("cache-tier plan schema mismatch")
    expected_sha = compute_plan_sha256(plan)
    if plan.get("plan_sha256") != expected_sha:
        raise CacheTierValidationError("cache-tier plan SHA256 drift")
    if plan.get("hot_root") != str(_absolute_lexical(config.hot_root)):
        raise CacheTierValidationError("plan hot root does not match runtime config")
    if plan.get("cold_root") != str(_absolute_lexical(config.cold_root)):
        raise CacheTierValidationError("plan cold root does not match runtime config")
    if plan.get("ledger_path") != str(_absolute_lexical(config.ledger_path)):
        raise CacheTierValidationError("plan ledger path does not match runtime config")
    if plan.get("allowed_cache_roots") != list(config.allowed_cache_roots):
        raise CacheTierValidationError("plan allowed cache roots drift")
    if plan.get("allow_unknown_migration") != config.allow_unknown_migration:
        raise CacheTierValidationError("plan unknown-migration permission drift")
    if plan.get("reference_provenance") != _reference_audit_metadata(config):
        raise CacheTierValidationError("plan reference-audit provenance drift")
    if plan.get("active_cache_protection") != active_cache_protection_snapshot(config):
        raise CacheTierValidationError("plan active-cache protection drift")
    planned_paths: list[PurePosixPath] = []
    for operation_name, key in (("migrate", "migrations"), ("delete", "deletions")):
        rows = plan.get(key)
        if not isinstance(rows, list):
            raise CacheTierValidationError(f"plan {key} must be a list")
        for row in rows:
            if not isinstance(row, Mapping) or row.get("operation") != operation_name:
                raise CacheTierValidationError(f"invalid {operation_name} plan row")
            relative = _validate_relative_path(str(row.get("relative_path")), config.allowed_cache_roots)
            planned_paths.append(PurePosixPath(relative))
            expected_source_root = config.hot_root if operation_name == "migrate" else config.cold_root
            expected_source = _absolute_lexical(expected_source_root / relative)
            if row.get("source_path") != str(expected_source):
                raise CacheTierValidationError(f"plan source path drift for {relative}")
            if row.get("destination_path") != str(_absolute_lexical(config.cold_root / relative)):
                raise CacheTierValidationError(f"plan destination path drift for {relative}")
    for index, path in enumerate(sorted(planned_paths, key=lambda value: value.parts)):
        for other in sorted(planned_paths, key=lambda value: value.parts)[index + 1 :]:
            if path in other.parents:
                raise CacheTierValidationError(
                    "plan contains overlapping ancestor/descendant artifacts: "
                    f"{path.as_posix()} and {other.as_posix()}"
                )


def _fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_tree(path: Path) -> None:
    if path.is_file():
        with path.open("rb") as handle:
            os.fsync(handle.fileno())
        _fsync_directory(path.parent)
        return
    directories = [path]
    for entry in path.rglob("*"):
        if entry.is_symlink():
            raise CacheTierValidationError(f"internal artifact symlink is forbidden: {entry}")
        if entry.is_file():
            with entry.open("rb") as handle:
                os.fsync(handle.fileno())
        elif entry.is_dir():
            directories.append(entry)
    for directory in reversed(directories):
        _fsync_directory(directory)


def _copy_artifact(source: Path, destination: Path) -> None:
    if source.is_file():
        shutil.copy2(source, destination)
    else:
        shutil.copytree(source, destination, symlinks=False)


def _remove_artifact(path: Path) -> None:
    if path.is_symlink() or path.is_file():
        path.unlink()
    elif path.is_dir():
        shutil.rmtree(path)


def _same_fingerprint(path: Path, row: Mapping[str, object]) -> bool:
    fingerprint = artifact_fingerprint(path)
    return (
        fingerprint["content_sha256"] == row.get("content_sha256")
        and fingerprint["size_bytes"] == row.get("size_bytes")
        and fingerprint["entry_count"] == row.get("entry_count")
    )


def _validate_ledger_row(
    connection: sqlite3.Connection,
    row: Mapping[str, object],
    *,
    expected_tier: str,
) -> sqlite3.Row:
    current = connection.execute(
        "SELECT * FROM artifacts WHERE relative_path=?", (row["relative_path"],)
    ).fetchone()
    if current is None:
        raise CacheTierValidationError(f"plan drift: missing ledger row {row['relative_path']}")
    for field in (
        "logical_id",
        "tier",
        "access_count",
        "last_access_ns",
        "size_bytes",
        "reference_class",
        "identity_sha256",
        "tier_since_ns",
        "cold_admitted_ns",
        "last_explicit_access_ns",
        "managed_by_lru",
    ):
        if current[field] != row.get(field):
            raise CacheTierValidationError(
                f"plan drift for {row['relative_path']}: ledger field {field} changed"
            )
    if current["tier"] != expected_tier:
        raise CacheTierValidationError(
            f"plan drift for {row['relative_path']}: expected tier {expected_tier}"
        )
    return current


def _symlink_target(hot_path: Path, cold_path: Path, mode: str) -> str:
    if mode == "absolute":
        return str(cold_path)
    return os.path.relpath(cold_path, start=hot_path.parent)


@dataclasses.dataclass
class _PreparedMigration:
    relative_path: str
    source: Path
    destination: Path
    backup: Path
    partial: Path
    link_temp: Path
    published_new_destination: bool
    row: Mapping[str, object]

    def rollback(self) -> None:
        if self.source.is_symlink():
            self.source.unlink()
        if _lexists(self.backup):
            os.replace(self.backup, self.source)
            _fsync_directory(self.source.parent)
        if self.published_new_destination and _lexists(self.destination):
            _remove_artifact(self.destination)
            _fsync_directory(self.destination.parent)
        for path in (self.partial, self.link_temp):
            if _lexists(path):
                _remove_artifact(path)

    def finalize(self) -> None:
        if _lexists(self.backup):
            _remove_artifact(self.backup)
            _fsync_directory(self.source.parent)

    def result(self) -> dict[str, object]:
        return {
            "operation": "migrate",
            "relative_path": self.relative_path,
            "source_replaced_by_symlink": True,
            "cold_destination_created": self.published_new_destination,
            "content_sha256": self.row["content_sha256"],
            "size_bytes": self.row["size_bytes"],
            "reference_class": self.row["reference_class"],
            "hot_link_path": str(self.source),
            "cold_path": str(self.destination),
        }


@dataclasses.dataclass
class _PreparedDeletion:
    relative_path: str
    hot: Path
    cold: Path
    tombstone: Path
    link_backup: Path
    row: Mapping[str, object]

    def rollback(self) -> None:
        if _lexists(self.tombstone) and not _lexists(self.cold):
            os.replace(self.tombstone, self.cold)
            _fsync_directory(self.cold.parent)
        if _lexists(self.link_backup) and not _lexists(self.hot):
            os.replace(self.link_backup, self.hot)
            _fsync_directory(self.hot.parent)

    def finalize(self) -> None:
        if _lexists(self.tombstone):
            _remove_artifact(self.tombstone)
            _fsync_directory(self.cold.parent)
        if _lexists(self.link_backup):
            _remove_artifact(self.link_backup)
            _fsync_directory(self.hot.parent)

    def result(self) -> dict[str, object]:
        return {
            "operation": "delete",
            "relative_path": self.relative_path,
            "hot_symlink_removed": not _lexists(self.hot),
            "content_sha256": self.row["content_sha256"],
            "size_bytes": self.row["size_bytes"],
            "reference_class": self.row["reference_class"],
            "cold_admitted_ns": self.row["cold_admitted_ns"],
            "last_explicit_access_ns": self.row["last_explicit_access_ns"],
        }


def _prepare_migration(
    row: Mapping[str, object],
    *,
    config: CacheTierConfig,
) -> _PreparedMigration:
    relative = str(row["relative_path"])
    source = _absolute_lexical(config.hot_root / relative)
    destination = _absolute_lexical(config.cold_root / relative)
    if source.is_symlink() or not source.exists():
        raise CacheTierValidationError(f"migration source drift: {source}")
    if not _same_fingerprint(source, row):
        raise CacheTierValidationError(f"migration source fingerprint drift: {source}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    prepared = _PreparedMigration(
        relative_path=relative,
        source=source,
        destination=destination,
        backup=source.parent / f".{source.name}.cache-tier-lru.hot-backup.{uuid.uuid4().hex}",
        partial=(
            destination.parent
            / f".{destination.name}.cache-tier-lru.partial.{uuid.uuid4().hex}"
        ),
        link_temp=source.parent / f".{source.name}.cache-tier-lru.link.{uuid.uuid4().hex}",
        published_new_destination=False,
        row=row,
    )
    try:
        if destination.exists():
            if not _same_fingerprint(destination, row):
                raise CacheTierValidationError(
                    f"cold destination conflicts with plan: {destination}"
                )
        else:
            _copy_artifact(source, prepared.partial)
            if not _same_fingerprint(prepared.partial, row):
                raise CacheTierValidationError(
                    f"copied cache failed content verification: {prepared.partial}"
                )
            _fsync_tree(prepared.partial)
            os.replace(prepared.partial, destination)
            _fsync_directory(destination.parent)
            prepared.published_new_destination = True
        os.replace(source, prepared.backup)
        _fsync_directory(source.parent)
        os.symlink(
            _symlink_target(source, destination, config.symlink_mode),
            prepared.link_temp,
        )
        os.replace(prepared.link_temp, source)
        _fsync_directory(source.parent)
        if source.resolve(strict=True) != destination.resolve(strict=True):
            raise CacheTierValidationError(
                "installed hot symlink does not resolve to cold artifact"
            )
        if not _same_fingerprint(destination, row):
            raise CacheTierValidationError(
                "cold artifact changed during hot symlink publication"
            )
        return prepared
    except Exception:
        with contextlib.suppress(Exception):
            prepared.rollback()
        raise


def _validate_deletion_authority(
    current: sqlite3.Row,
    *,
    config: CacheTierConfig,
    plan_created_ns: int,
) -> None:
    cutoff_ns = plan_created_ns - config.cold_ttl_ns
    if current["reference_class"] != "unreferenced":
        raise CacheTierValidationError("cold deletion requires reference_class=unreferenced")
    if not bool(current["managed_by_lru"]):
        raise CacheTierValidationError("cold deletion requires managed migration provenance")
    if current["cold_admitted_ns"] is None or int(current["cold_admitted_ns"]) > cutoff_ns:
        raise CacheTierValidationError("cold residence TTL is not satisfied")
    if (
        current["last_explicit_access_ns"] is not None
        and int(current["last_explicit_access_ns"]) > cutoff_ns
    ):
        raise CacheTierValidationError("explicit access TTL is not satisfied")
    if current["hot_link_path"] is None:
        raise CacheTierValidationError("cold deletion requires managed hot-link provenance")


def _prepare_deletion(
    row: Mapping[str, object],
    *,
    config: CacheTierConfig,
) -> _PreparedDeletion:
    relative = str(row["relative_path"])
    cold = _absolute_lexical(config.cold_root / relative)
    hot = _absolute_lexical(config.hot_root / relative)
    if not cold.exists() or not _same_fingerprint(cold, row):
        raise CacheTierValidationError(f"cold deletion source fingerprint drift: {cold}")
    if not hot.is_symlink() or hot.resolve(strict=True) != cold.resolve(strict=True):
        raise CacheTierValidationError(f"hot path is not the expected managed cold symlink: {hot}")
    prepared = _PreparedDeletion(
        relative_path=relative,
        hot=hot,
        cold=cold,
        tombstone=cold.parent / f".{cold.name}.cache-tier-lru.delete.{uuid.uuid4().hex}",
        link_backup=(
            hot.parent / f".{hot.name}.cache-tier-lru.link-backup.{uuid.uuid4().hex}"
        ),
        row=row,
    )
    try:
        os.replace(hot, prepared.link_backup)
        _fsync_directory(hot.parent)
        os.replace(cold, prepared.tombstone)
        _fsync_directory(cold.parent)
        return prepared
    except Exception:
        with contextlib.suppress(Exception):
            prepared.rollback()
        raise


def _commit_connection(connection: sqlite3.Connection) -> None:
    connection.commit()


def _ledger_operation_committed(
    config: CacheTierConfig,
    *,
    relative_path: str,
    operation: str,
) -> bool:
    connection = _connect_ledger(config, create=False)
    try:
        row = connection.execute(
            "SELECT tier FROM artifacts WHERE relative_path=?", (relative_path,)
        ).fetchone()
    finally:
        connection.close()
    if operation == "migrate":
        return row is not None and row["tier"] == "cold"
    return row is None


def _apply_one_artifact(
    row: Mapping[str, object],
    *,
    config: CacheTierConfig,
    operation: str,
    plan_created_ns: int,
) -> tuple[dict[str, object], dict[str, object] | None]:
    connection = _connect_ledger(config, create=False)
    prepared: _PreparedMigration | _PreparedDeletion | None = None
    committed = False
    try:
        _initialize_ledger(connection)
        connection.execute("BEGIN IMMEDIATE")
        expected_tier = "hot" if operation == "migrate" else "cold"
        current = _validate_ledger_row(connection, row, expected_tier=expected_tier)
        if operation == "migrate":
            prepared = _prepare_migration(row, config=config)
            admitted_ns = time.time_ns()
            connection.execute(
                """
                UPDATE artifacts SET
                    tier='cold', physical_path=?, hot_link_path=?, tier_since_ns=?,
                    cold_admitted_ns=?, managed_by_lru=1, updated_ns=?
                WHERE relative_path=?
                """,
                (
                    str(prepared.destination),
                    str(prepared.source),
                    admitted_ns,
                    admitted_ns,
                    admitted_ns,
                    prepared.relative_path,
                ),
            )
        else:
            _validate_deletion_authority(
                current,
                config=config,
                plan_created_ns=plan_created_ns,
            )
            prepared = _prepare_deletion(row, config=config)
            connection.execute(
                "DELETE FROM artifacts WHERE relative_path=?", (prepared.relative_path,)
            )
        try:
            _commit_connection(connection)
            committed = True
        except Exception:
            with contextlib.suppress(Exception):
                connection.rollback()
            committed = _ledger_operation_committed(
                config,
                relative_path=str(row["relative_path"]),
                operation=operation,
            )
            if not committed:
                assert prepared is not None
                prepared.rollback()
                raise
    finally:
        connection.close()
    assert prepared is not None and committed
    result = prepared.result()
    warning: dict[str, object] | None = None
    try:
        prepared.finalize()
    except Exception as error:
        warning = {
            "type": type(error).__name__,
            "detail": str(error),
            "reconciliation_required": True,
            "relative_path": str(row["relative_path"]),
        }
        result["cleanup_pending"] = True
    else:
        result["cleanup_pending"] = False
    return result, warning


def _atomic_write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, raw_temp = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temp = Path(raw_temp)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(_canonical_json(payload) + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
        _fsync_directory(path.parent)
    finally:
        with contextlib.suppress(FileNotFoundError):
            temp.unlink()


def write_plan(path: Path, plan: Mapping[str, object]) -> None:
    _atomic_write_json(path, plan)


def load_plan(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise CacheTierValidationError(f"invalid cache-tier plan {path}: {error}") from error
    if not isinstance(payload, dict):
        raise CacheTierValidationError("cache-tier plan must be a JSON object")
    return payload


def apply_cache_tier_plan(
    plan: Mapping[str, object],
    *,
    config: CacheTierConfig,
    operation: Literal["migrate", "delete"],
    owner_token: str | None = None,
    execute: bool = False,
    receipt_path: Path | None = None,
) -> dict[str, object]:
    validate_plan(plan, config)
    if operation not in OPERATIONS:
        raise CacheTierValidationError(f"unsupported apply operation: {operation}")
    rows_key = "migrations" if operation == "migrate" else "deletions"
    rows = plan[rows_key]
    assert isinstance(rows, list)
    plan_sha = str(plan["plan_sha256"])
    if execute:
        expected = owner_token_for_plan(plan, operation)
        if owner_token is None or not hmac.compare_digest(owner_token, expected):
            raise CacheTierAuthorizationError(
                f"{operation} execution requires the exact owner token bound to plan {plan_sha}"
            )
    started_ns = time.time_ns()
    results: list[dict[str, object]] = []
    if receipt_path is None:
        receipt_path = config.receipt_root / f"{plan_sha}.{operation}.json"
    errors: list[dict[str, object]] = []
    warnings: list[dict[str, object]] = []

    def persist(status: str) -> dict[str, object]:
        receipt: dict[str, object] = {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "plan_sha256": plan_sha,
            "operation": operation,
            "executed": execute,
            "atomicity_scope": "per_artifact",
            "status": status,
            "started_ns": started_ns,
            "completed_ns": time.time_ns(),
            "planned_count": len(rows),
            "completed_count": len(results),
            "remaining_count": len(rows) - len(results),
            "results": results,
            "errors": errors,
            "warnings": warnings,
            "reconcilable_partial_state": status == "partial",
        }
        receipt_sha = _sha256_bytes(_canonical_json(receipt))
        receipt["receipt_sha256"] = receipt_sha
        _atomic_write_json(receipt_path, receipt)
        return receipt

    if not execute:
        return persist("dry_run")

    receipt = persist("in_progress")
    with _advisory_lock(config.lock_path, exclusive=True, timeout_s=config.lock_timeout_s):
        for row in rows:
            assert isinstance(row, Mapping)
            try:
                result, warning = _apply_one_artifact(
                    row,
                    config=config,
                    operation=operation,
                    plan_created_ns=int(plan["created_ns"]),
                )
            except Exception as error:
                errors.append(
                    {
                        "type": type(error).__name__,
                        "detail": str(error),
                        "relative_path": row.get("relative_path"),
                    }
                )
                receipt = persist("partial" if results else "failed")
                break
            results.append(result)
            if warning is not None:
                warnings.append(warning)
                receipt = persist("partial")
                break
            receipt = persist("in_progress")
        else:
            receipt = persist("complete")
    if execute:
        connection = _connect_ledger(config, create=False)
        try:
            _initialize_ledger(connection)
            connection.execute(
                """
                INSERT INTO operation_receipts(
                    receipt_sha256, created_ns, operation, plan_sha256,
                    receipt_path, status
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    receipt["receipt_sha256"],
                    int(receipt["completed_ns"]),
                    operation,
                    plan_sha,
                    str(receipt_path),
                    receipt["status"],
                ),
            )
            connection.commit()
        finally:
            connection.close()
    return receipt


def cleanup_stale_partials(config: CacheTierConfig, *, older_than_s: float = 86_400) -> list[str]:
    config.validate(require_roots=True)
    cutoff = time.time() - older_than_s
    removed: list[str] = []
    patterns = (
        ".*.cache-tier-lru.partial.*",
        ".*.cache-tier-lru.link.*",
    )
    with _advisory_lock(config.lock_path, exclusive=True, timeout_s=config.lock_timeout_s):
        for root in (config.hot_root, config.cold_root):
            for allowed_root in config.allowed_cache_roots:
                parent = root / allowed_root
                if not parent.is_dir():
                    continue
                for pattern in patterns:
                    for path in parent.rglob(pattern):
                        if path.lstat().st_mtime > cutoff:
                            continue
                        _remove_artifact(path)
                        removed.append(str(path))
    return sorted(removed)


def validate_cache_tiers(config: CacheTierConfig) -> dict[str, object]:
    config.validate(require_roots=True)
    connection = _connect_ledger(config, create=False)
    issues: list[dict[str, object]] = []
    records: list[CacheAccessRecord] = []
    try:
        _initialize_ledger(connection)
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        if integrity != "ok":
            issues.append({"kind": "sqlite_integrity", "detail": integrity})
        records = [_row_to_record(row) for row in connection.execute("SELECT * FROM artifacts")]
    finally:
        connection.close()
    for record in records:
        try:
            _validate_relative_path(record.relative_path, config.allowed_cache_roots)
            physical = Path(record.physical_path)
            if not physical.exists():
                raise CacheTierValidationError("physical artifact is missing")
            measured = _path_size_bytes(physical)
            if measured != record.size_bytes:
                raise CacheTierValidationError(
                    f"size drift: ledger={record.size_bytes}, actual={measured}"
                )
            if record.tier == "hot" and not _is_relative_to(physical, config.hot_root):
                raise CacheTierValidationError("hot artifact physical path is outside hot root")
            if record.tier == "cold":
                if not _is_relative_to(physical, config.cold_root):
                    raise CacheTierValidationError("cold artifact physical path is outside cold root")
                hot = config.hot_root / record.relative_path
                if record.managed_by_lru and not _lexists(hot):
                    raise CacheTierValidationError(
                        "managed cold artifact is missing its hot compatibility symlink"
                    )
                if _lexists(hot) and (
                    not hot.is_symlink() or hot.resolve(strict=True) != physical.resolve(strict=True)
                ):
                    raise CacheTierValidationError("cold artifact hot path is not a valid symlink")
        except Exception as error:
            issues.append(
                {
                    "kind": "artifact_validation",
                    "relative_path": record.relative_path,
                    "detail": str(error),
                }
            )
    partials: list[str] = []
    for root in (config.hot_root, config.cold_root):
        for allowed_root in config.allowed_cache_roots:
            parent = root / allowed_root
            if parent.is_dir():
                partials.extend(
                    str(path)
                    for path in parent.rglob(".*.cache-tier-lru.*")
                    if _lexists(path)
                )
    return {
        "schema_version": SCHEMA_VERSION,
        "valid": not issues,
        "artifact_count": len(records),
        "issues": issues,
        "partials": sorted(partials),
        "hot_free_bytes": shutil.disk_usage(config.hot_root).free,
        "cold_free_bytes": shutil.disk_usage(config.cold_root).free,
    }


def ledger_health_events(config: CacheTierConfig) -> list[dict[str, object]]:
    connection = _connect_ledger(config, create=False)
    try:
        _initialize_ledger(connection)
        return [dict(row) for row in connection.execute("SELECT * FROM health_events ORDER BY event_id")]
    finally:
        connection.close()


__all__ = [
    "CacheAccessRecord",
    "CacheTierAuthorizationError",
    "CacheTierConfig",
    "CacheTierError",
    "CacheTierValidationError",
    "DEFAULT_ALLOWED_CACHE_ROOTS",
    "DEFAULT_COLD_ROOT",
    "DEFAULT_HOT_ROOT",
    "DEFAULT_LEDGER_PATH",
    "apply_cache_tier_plan",
    "artifact_fingerprint",
    "build_cache_tier_plan",
    "cleanup_stale_partials",
    "compute_plan_sha256",
    "ledger_health_events",
    "list_artifacts",
    "load_plan",
    "owner_token_for_plan",
    "record_cache_access",
    "register_cache_write",
    "scan_cache_tiers",
    "validate_cache_tiers",
    "validate_plan",
    "write_plan",
]
