"""Small content-addressed Parquet cache for immutable research mechanics."""

from __future__ import annotations

import fcntl
import hashlib
import json
import shutil
import tempfile
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from models.cache_tier_lru import record_cache_access, register_cache_write

CACHE_SCHEMA_VERSION = "narrowgate_content_addressed_parquet.v1"
DIRECTORY_CACHE_SCHEMA_VERSION = "narrowgate_content_addressed_directory.v1"


def canonical_sha256(value: Any) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _record_content_cache_hit(path: Path, *, identity_sha256: str) -> None:
    with suppress(Exception):
        record_cache_access(path, identity_sha256=identity_sha256)


def _register_content_cache_write(path: Path, *, identity_sha256: str) -> None:
    with suppress(Exception):
        register_cache_write(path, identity_sha256=identity_sha256)


@dataclass(frozen=True)
class CacheRecord:
    key: str
    entry_dir: Path
    frame: pd.DataFrame
    manifest: Mapping[str, Any]
    hit: bool


@dataclass(frozen=True)
class DirectoryCacheRecord:
    key: str
    entry_dir: Path
    payload_dir: Path
    manifest: Mapping[str, Any]
    hit: bool


class ParquetContentAddressedCache:
    """Cache a DataFrame under the hash of its complete mechanics identity."""

    def __init__(self, root: Path, *, namespace: str) -> None:
        self._logical_root = root.expanduser().absolute() / str(namespace)
        self.root = root.expanduser().resolve() / str(namespace)
        self.root.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def key(identity: Mapping[str, Any]) -> str:
        return canonical_sha256(identity)

    def _entry(self, key: str) -> Path:
        return self.root / key[:2] / key

    def _logical_entry(self, key: str) -> Path:
        return self._logical_root / key[:2] / key

    def _load_key(
        self,
        key: str,
        identity: Mapping[str, Any],
    ) -> CacheRecord | None:
        entry = self._entry(key)
        manifest_path = entry / "manifest.json"
        payload_path = entry / "payload.parquet"
        complete_path = entry / "COMPLETE"
        if not (
            manifest_path.is_file()
            and payload_path.is_file()
            and complete_path.is_file()
        ):
            return None
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("schema_version") != CACHE_SCHEMA_VERSION:
            raise RuntimeError(f"cache schema mismatch: {entry}")
        if manifest.get("key") != key:
            raise RuntimeError(f"cache key mismatch: {entry}")
        if manifest.get("identity_sha256") != canonical_sha256(identity):
            raise RuntimeError(f"cache identity mismatch: {entry}")
        if complete_path.read_text(encoding="ascii").strip() != key:
            raise RuntimeError(f"cache completion marker mismatch: {entry}")
        if manifest.get("payload_sha256") != file_sha256(payload_path):
            raise RuntimeError(f"cache payload checksum mismatch: {entry}")
        frame = pd.read_parquet(payload_path)
        if int(manifest.get("rows", -1)) != len(frame):
            raise RuntimeError(f"cache row count mismatch: {entry}")
        return CacheRecord(key, entry, frame, manifest, True)

    def load(self, identity: Mapping[str, Any]) -> CacheRecord | None:
        key = self.key(identity)
        record = self._load_key(key, identity)
        if record is not None:
            _record_content_cache_hit(self._logical_entry(key), identity_sha256=key)
        return record

    def store(
        self,
        identity: Mapping[str, Any],
        frame: pd.DataFrame,
        *,
        metadata: Mapping[str, Any] | None = None,
    ) -> CacheRecord:
        key = self.key(identity)
        entry = self._entry(key)
        entry.parent.mkdir(parents=True, exist_ok=True)
        lock_path = entry.parent / f"{key}.lock"
        with lock_path.open("a+", encoding="ascii") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            existing = self._load_key(key, identity)
            if existing is not None:
                _record_content_cache_hit(self._logical_entry(key), identity_sha256=key)
                return existing
            if entry.exists():
                shutil.rmtree(entry)
            stage = Path(tempfile.mkdtemp(prefix=f".{key}.", dir=entry.parent))
            try:
                payload_path = stage / "payload.parquet"
                frame.to_parquet(payload_path, index=False, compression="zstd")
                manifest = {
                    "schema_version": CACHE_SCHEMA_VERSION,
                    "key": key,
                    "identity_sha256": canonical_sha256(identity),
                    "identity": identity,
                    "rows": int(len(frame)),
                    "columns": list(frame.columns),
                    "payload_sha256": file_sha256(payload_path),
                    "metadata": dict(metadata or {}),
                }
                (stage / "manifest.json").write_text(
                    json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                (stage / "COMPLETE").write_text(key + "\n", encoding="ascii")
                stage.replace(entry)
            finally:
                if stage.exists():
                    shutil.rmtree(stage)
            stored = self._load_key(key, identity)
            if stored is None:
                raise RuntimeError(f"cache admission failed: {entry}")
            _register_content_cache_write(self._logical_entry(key), identity_sha256=key)
            return CacheRecord(
                stored.key,
                stored.entry_dir,
                stored.frame,
                stored.manifest,
                False,
            )


class DirectoryContentAddressedCache:
    """Cache a deterministic multi-file artifact under its full identity."""

    def __init__(self, root: Path, *, namespace: str) -> None:
        self._logical_root = root.expanduser().absolute() / str(namespace)
        self.root = root.expanduser().resolve() / str(namespace)
        self.root.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def key(identity: Mapping[str, Any]) -> str:
        return canonical_sha256(identity)

    def _entry(self, key: str) -> Path:
        return self.root / key[:2] / key

    def _logical_entry(self, key: str) -> Path:
        return self._logical_root / key[:2] / key

    @staticmethod
    def _payload_files(payload_dir: Path) -> list[Path]:
        return sorted(
            path
            for path in payload_dir.rglob("*")
            if path.is_file() and not path.is_symlink()
        )

    def _load_key(
        self,
        key: str,
        identity: Mapping[str, Any],
    ) -> DirectoryCacheRecord | None:
        entry = self._entry(key)
        payload_dir = entry / "payload"
        manifest_path = entry / "manifest.json"
        complete_path = entry / "COMPLETE"
        if not (
            payload_dir.is_dir()
            and manifest_path.is_file()
            and complete_path.is_file()
        ):
            return None
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("schema_version") != DIRECTORY_CACHE_SCHEMA_VERSION:
            raise RuntimeError(f"directory cache schema mismatch: {entry}")
        if manifest.get("key") != key:
            raise RuntimeError(f"directory cache key mismatch: {entry}")
        if manifest.get("identity_sha256") != canonical_sha256(identity):
            raise RuntimeError(f"directory cache identity mismatch: {entry}")
        if complete_path.read_text(encoding="ascii").strip() != key:
            raise RuntimeError(f"directory cache completion mismatch: {entry}")
        expected = {
            str(item["path"]): item for item in manifest.get("files", [])
        }
        actual_paths = self._payload_files(payload_dir)
        actual = {
            str(path.relative_to(payload_dir)): path for path in actual_paths
        }
        if set(actual) != set(expected):
            raise RuntimeError(f"directory cache file set mismatch: {entry}")
        for relative, path in actual.items():
            item = expected[relative]
            if int(item.get("size_bytes", -1)) != int(path.stat().st_size):
                raise RuntimeError(
                    f"directory cache file size mismatch: {path}"
                )
            if str(item.get("sha256")) != file_sha256(path):
                raise RuntimeError(
                    f"directory cache file checksum mismatch: {path}"
                )
        return DirectoryCacheRecord(
            key=key,
            entry_dir=entry,
            payload_dir=payload_dir,
            manifest=manifest,
            hit=True,
        )

    def load(
        self,
        identity: Mapping[str, Any],
    ) -> DirectoryCacheRecord | None:
        key = self.key(identity)
        record = self._load_key(key, identity)
        if record is not None:
            _record_content_cache_hit(self._logical_entry(key), identity_sha256=key)
        return record

    def get_or_build(
        self,
        identity: Mapping[str, Any],
        builder: Callable[[Path], Mapping[str, Any] | None],
    ) -> DirectoryCacheRecord:
        key = self.key(identity)
        entry = self._entry(key)
        entry.parent.mkdir(parents=True, exist_ok=True)
        lock_path = entry.parent / f"{key}.lock"
        with lock_path.open("a+", encoding="ascii") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            existing = self._load_key(key, identity)
            if existing is not None:
                _record_content_cache_hit(self._logical_entry(key), identity_sha256=key)
                return existing
            if entry.exists():
                shutil.rmtree(entry)
            stage = Path(tempfile.mkdtemp(prefix=f".{key}.", dir=entry.parent))
            try:
                payload_dir = stage / "payload"
                payload_dir.mkdir()
                metadata = dict(builder(payload_dir) or {})
                payload_files = self._payload_files(payload_dir)
                if not payload_files:
                    raise RuntimeError("directory cache builder produced no files")
                files = [
                    {
                        "path": str(path.relative_to(payload_dir)),
                        "size_bytes": int(path.stat().st_size),
                        "sha256": file_sha256(path),
                    }
                    for path in payload_files
                ]
                manifest = {
                    "schema_version": DIRECTORY_CACHE_SCHEMA_VERSION,
                    "key": key,
                    "identity_sha256": canonical_sha256(identity),
                    "identity": identity,
                    "files": files,
                    "metadata": metadata,
                }
                (stage / "manifest.json").write_text(
                    json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                (stage / "COMPLETE").write_text(key + "\n", encoding="ascii")
                stage.replace(entry)
            finally:
                if stage.exists():
                    shutil.rmtree(stage)
            stored = self._load_key(key, identity)
            if stored is None:
                raise RuntimeError(f"directory cache admission failed: {entry}")
            _register_content_cache_write(self._logical_entry(key), identity_sha256=key)
            return DirectoryCacheRecord(
                key=stored.key,
                entry_dir=stored.entry_dir,
                payload_dir=stored.payload_dir,
                manifest=stored.manifest,
                hit=False,
            )
