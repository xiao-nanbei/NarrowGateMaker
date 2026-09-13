"""Crash-safe fixed-size storage for the live fill-cooldown checkpoint.

The store alternates between two fixed slots in one preallocated file.  A
checkpoint write overwrites only the older slot and then data-syncs the file;
the other slot remains a recovery point if the new write is torn or corrupt.

This module deliberately owns only the durable-record envelope.  Validation
of the fill-cooldown payload's economic fields remains the caller's
responsibility.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import struct
import threading
import zlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

FILL_COOLDOWN_WAL_MAGIC = b"NGFCWAL1"
FILL_COOLDOWN_WAL_VERSION = 1
FILL_COOLDOWN_WAL_MODE = 0o600
FILL_COOLDOWN_WAL_MAX_PAYLOAD_BYTES = 64 * 1024

# magic, version, reserved, sequence, payload length, SHA256(payload)
_HEADER_PREFIX = struct.Struct(">8sB7xQI32s")
_HEADER_CRC = struct.Struct(">I")
FILL_COOLDOWN_WAL_HEADER_BYTES = _HEADER_PREFIX.size + _HEADER_CRC.size
FILL_COOLDOWN_WAL_SLOT_BYTES = (
    FILL_COOLDOWN_WAL_HEADER_BYTES + FILL_COOLDOWN_WAL_MAX_PAYLOAD_BYTES
)
FILL_COOLDOWN_WAL_FILE_BYTES = 2 * FILL_COOLDOWN_WAL_SLOT_BYTES

FaultInjector = Callable[[str], None]


class FillCooldownCheckpointError(RuntimeError):
    """Base exception for the durable checkpoint envelope."""


class FillCooldownCheckpointCorruptionError(FillCooldownCheckpointError):
    """Raised when no valid recovery slot remains."""


@dataclass(frozen=True)
class FillCooldownCheckpointRecord:
    """The highest valid durable record and its recovery diagnostics."""

    payload: dict[str, Any]
    sequence: int
    slot_index: int
    payload_sha256: str
    ignored_invalid_slots: tuple[int, ...]


@dataclass(frozen=True)
class FillCooldownCheckpointWriteReceipt:
    """Proof returned after the record has passed the data-sync boundary."""

    sequence: int
    slot_index: int
    payload_sha256: str
    bytes_written: int
    sync_primitive: str


@dataclass(frozen=True)
class _DecodedSlot:
    status: str
    record: FillCooldownCheckpointRecord | None = None


def canonical_checkpoint_bytes(payload: Mapping[str, Any]) -> bytes:
    """Serialize a payload with the same deterministic JSON rules as live."""

    return (
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("ascii")


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate fill cooldown checkpoint key: {key}")
        result[key] = value
    return result


def _sync_data(descriptor: int) -> str:
    fdatasync = getattr(os, "fdatasync", None)
    if fdatasync is not None:
        fdatasync(descriptor)
        return "fdatasync"
    # macOS does not expose fdatasync through Python.  fsync is the stronger
    # portable fallback used by local development and tests; Linux live uses
    # fdatasync.
    os.fsync(descriptor)
    return "fsync"


class FillCooldownCheckpointWAL:
    """Two-slot, preallocated, crash-recoverable checkpoint store.

    The descriptor is kept open after first use so hot-path writes do not
    create, rename, or directory-sync a file for every fill.  The directory is
    synced only when the WAL is first created.
    """

    def __init__(
        self,
        path: Path | str,
        *,
        fault_injector: FaultInjector | None = None,
    ) -> None:
        expanded = Path(path).expanduser()
        if not expanded.is_absolute():
            raise ValueError("fill cooldown checkpoint WAL path must be absolute")
        self.path = expanded
        self._fault_injector = fault_injector
        self._descriptor: int | None = None
        self._last_sequence: int | None = None
        self._lock = threading.RLock()

    def __enter__(self) -> FillCooldownCheckpointWAL:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def _fault(self, stage: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(stage)

    @staticmethod
    def _reject_symlink_components(path: Path) -> None:
        absolute = path.absolute()
        current = Path(absolute.anchor)
        for part in absolute.parts[1:]:
            current /= part
            try:
                component_stat = os.lstat(current)
            except FileNotFoundError:
                continue
            if stat.S_ISLNK(component_stat.st_mode):
                raise PermissionError(
                    f"fill cooldown checkpoint WAL path contains a symlink: {current}"
                )

    @staticmethod
    def _stat_identity(file_stat: os.stat_result) -> tuple[int, ...]:
        return (
            file_stat.st_dev,
            file_stat.st_ino,
            file_stat.st_mode,
            file_stat.st_uid,
        )

    @classmethod
    def _read_identity(cls, file_stat: os.stat_result) -> tuple[int, ...]:
        return (
            *cls._stat_identity(file_stat),
            file_stat.st_size,
            file_stat.st_mtime_ns,
            file_stat.st_ctime_ns,
        )

    @staticmethod
    def _validate_file_stat(file_stat: os.stat_result) -> None:
        if not stat.S_ISREG(file_stat.st_mode):
            raise ValueError("fill cooldown checkpoint WAL is not a regular file")
        if file_stat.st_uid != os.getuid():
            raise PermissionError(
                "fill cooldown checkpoint WAL owner differs from runtime user"
            )
        if stat.S_IMODE(file_stat.st_mode) != FILL_COOLDOWN_WAL_MODE:
            raise PermissionError("fill cooldown checkpoint WAL mode must be 0600")
        if file_stat.st_size < 0 or file_stat.st_size > FILL_COOLDOWN_WAL_FILE_BYTES:
            raise ValueError("fill cooldown checkpoint WAL size is invalid")

    def _verify_descriptor_path_identity(self, descriptor: int) -> os.stat_result:
        descriptor_stat = os.fstat(descriptor)
        self._validate_file_stat(descriptor_stat)
        try:
            path_stat = os.lstat(self.path)
        except OSError as exc:
            raise RuntimeError(
                "fill cooldown checkpoint WAL path changed while open"
            ) from exc
        if self._stat_identity(descriptor_stat) != self._stat_identity(path_stat):
            raise RuntimeError("fill cooldown checkpoint WAL path identity changed")
        return descriptor_stat

    def _prepare_parent(self) -> None:
        parent = self.path.parent
        self._reject_symlink_components(parent)
        parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._reject_symlink_components(parent)
        parent_stat = os.lstat(parent)
        if not stat.S_ISDIR(parent_stat.st_mode) or parent_stat.st_uid != os.getuid():
            raise PermissionError(
                "fill cooldown checkpoint WAL parent is not an owned directory"
            )

    def _open_existing(self) -> int | None:
        self._reject_symlink_components(self.path.parent)
        flags = os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(self.path, flags)
        except FileNotFoundError:
            return None
        try:
            self._verify_descriptor_path_identity(descriptor)
        except Exception:
            os.close(descriptor)
            raise
        return descriptor

    def _ensure_open(self, *, create: bool) -> tuple[int, bool]:
        if self._descriptor is not None:
            self._verify_descriptor_path_identity(self._descriptor)
            return self._descriptor, False

        descriptor = self._open_existing()
        if descriptor is not None:
            self._descriptor = descriptor
            return descriptor, False
        if not create:
            raise FileNotFoundError(self.path)

        self._prepare_parent()
        flags = (
            os.O_RDWR
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        try:
            descriptor = os.open(self.path, flags, FILL_COOLDOWN_WAL_MODE)
        except FileExistsError:
            descriptor = self._open_existing()
            if descriptor is None:  # pragma: no cover - adversarial path race.
                raise RuntimeError(
                    "fill cooldown checkpoint WAL creation raced"
                ) from None
            self._descriptor = descriptor
            return descriptor, False

        try:
            os.fchmod(descriptor, FILL_COOLDOWN_WAL_MODE)
            os.ftruncate(descriptor, FILL_COOLDOWN_WAL_FILE_BYTES)
            self._fault("after_preallocate")
            _sync_data(descriptor)
            self._fault("after_preallocate_sync")
            self._verify_descriptor_path_identity(descriptor)
            directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
            directory_descriptor = os.open(self.path.parent, directory_flags)
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
            self._fault("after_directory_sync")
        except Exception:
            os.close(descriptor)
            raise
        self._descriptor = descriptor
        return descriptor, True

    @staticmethod
    def _slot_index(sequence: int) -> int:
        # Sequence one starts in slot zero.  This also makes a truncation in
        # the newer second slot recoverable from the original first slot.
        return (sequence - 1) % 2

    @staticmethod
    def _encode_record(
        payload: Mapping[str, Any],
        *,
        sequence: int,
    ) -> tuple[bytes, str]:
        raw = canonical_checkpoint_bytes(payload)
        if len(raw) > FILL_COOLDOWN_WAL_MAX_PAYLOAD_BYTES:
            raise ValueError("fill cooldown checkpoint WAL payload is too large")
        digest = hashlib.sha256(raw).digest()
        prefix = _HEADER_PREFIX.pack(
            FILL_COOLDOWN_WAL_MAGIC,
            FILL_COOLDOWN_WAL_VERSION,
            sequence,
            len(raw),
            digest,
        )
        header = prefix + _HEADER_CRC.pack(zlib.crc32(prefix) & 0xFFFFFFFF)
        return header + raw, digest.hex()

    @classmethod
    def _decode_slot(cls, raw: bytes, *, slot_index: int) -> _DecodedSlot:
        if not raw or not raw.strip(b"\0"):
            return _DecodedSlot(status="empty")
        if len(raw) < FILL_COOLDOWN_WAL_HEADER_BYTES:
            return _DecodedSlot(status="invalid")
        prefix = raw[: _HEADER_PREFIX.size]
        try:
            magic, version, sequence, payload_size, expected_digest = (
                _HEADER_PREFIX.unpack(prefix)
            )
            (expected_crc,) = _HEADER_CRC.unpack(
                raw[_HEADER_PREFIX.size : FILL_COOLDOWN_WAL_HEADER_BYTES]
            )
        except struct.error:
            return _DecodedSlot(status="invalid")
        if (
            magic != FILL_COOLDOWN_WAL_MAGIC
            or version != FILL_COOLDOWN_WAL_VERSION
            or expected_crc != (zlib.crc32(prefix) & 0xFFFFFFFF)
            or sequence <= 0
            or cls._slot_index(sequence) != slot_index
            or payload_size <= 0
            or payload_size > FILL_COOLDOWN_WAL_MAX_PAYLOAD_BYTES
        ):
            return _DecodedSlot(status="invalid")
        payload_end = FILL_COOLDOWN_WAL_HEADER_BYTES + payload_size
        if payload_end > len(raw):
            return _DecodedSlot(status="invalid")
        payload_raw = raw[FILL_COOLDOWN_WAL_HEADER_BYTES:payload_end]
        actual_digest = hashlib.sha256(payload_raw).digest()
        if actual_digest != expected_digest:
            return _DecodedSlot(status="invalid")
        try:
            payload = json.loads(
                payload_raw,
                object_pairs_hook=_reject_duplicate_keys,
            )
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            return _DecodedSlot(status="invalid")
        if not isinstance(payload, dict):
            return _DecodedSlot(status="invalid")
        if canonical_checkpoint_bytes(payload) != payload_raw:
            return _DecodedSlot(status="invalid")
        payload_sequence = payload.get("checkpoint_sequence")
        if (
            not isinstance(payload_sequence, int)
            or isinstance(payload_sequence, bool)
            or payload_sequence != sequence
        ):
            return _DecodedSlot(status="invalid")
        return _DecodedSlot(
            status="valid",
            record=FillCooldownCheckpointRecord(
                payload=payload,
                sequence=sequence,
                slot_index=slot_index,
                payload_sha256=actual_digest.hex(),
                ignored_invalid_slots=(),
            ),
        )

    def read_latest(self) -> FillCooldownCheckpointRecord | None:
        """Return the highest valid record, falling back across a torn slot."""

        with self._lock:
            try:
                descriptor, _created = self._ensure_open(create=False)
            except FileNotFoundError:
                return None
            before = self._verify_descriptor_path_identity(descriptor)
            if before.st_size == 0:
                raise FillCooldownCheckpointCorruptionError(
                    "fill cooldown checkpoint WAL is empty or truncated"
                )

            decoded: list[_DecodedSlot] = []
            for slot_index in range(2):
                offset = slot_index * FILL_COOLDOWN_WAL_SLOT_BYTES
                if offset >= before.st_size:
                    slot_raw = b""
                else:
                    slot_raw = os.pread(
                        descriptor,
                        min(
                            FILL_COOLDOWN_WAL_SLOT_BYTES,
                            before.st_size - offset,
                        ),
                        offset,
                    )
                decoded.append(self._decode_slot(slot_raw, slot_index=slot_index))
            after = self._verify_descriptor_path_identity(descriptor)
            if self._read_identity(before) != self._read_identity(after):
                raise RuntimeError(
                    "fill cooldown checkpoint WAL changed while being read"
                )

            valid = [item.record for item in decoded if item.record is not None]
            invalid_slots = tuple(
                index for index, item in enumerate(decoded) if item.status == "invalid"
            )
            if not valid:
                if invalid_slots:
                    raise FillCooldownCheckpointCorruptionError(
                        "fill cooldown checkpoint WAL has no valid recovery slot"
                    )
                self._last_sequence = 0
                return None
            valid.sort(key=lambda record: record.sequence)
            if len(valid) == 2 and valid[0].sequence == valid[1].sequence:
                raise FillCooldownCheckpointCorruptionError(
                    "fill cooldown checkpoint WAL has duplicate sequence records"
                )
            selected = valid[-1]
            self._last_sequence = selected.sequence
            return FillCooldownCheckpointRecord(
                payload=selected.payload,
                sequence=selected.sequence,
                slot_index=selected.slot_index,
                payload_sha256=selected.payload_sha256,
                ignored_invalid_slots=invalid_slots,
            )

    def write(
        self,
        payload: Mapping[str, Any],
    ) -> FillCooldownCheckpointWriteReceipt:
        """Write and data-sync one sequence, preserving the other slot."""

        sequence = payload.get("checkpoint_sequence")
        if (
            not isinstance(sequence, int)
            or isinstance(sequence, bool)
            or sequence <= 0
        ):
            raise ValueError(
                "fill cooldown checkpoint WAL requires a positive checkpoint_sequence"
            )
        record, payload_sha256 = self._encode_record(payload, sequence=sequence)
        slot_index = self._slot_index(sequence)
        offset = slot_index * FILL_COOLDOWN_WAL_SLOT_BYTES

        with self._lock:
            descriptor, _created = self._ensure_open(create=True)
            if self._last_sequence is None:
                self.read_latest()
            if sequence <= int(self._last_sequence or 0):
                raise ValueError(
                    "fill cooldown checkpoint WAL sequence must strictly increase"
                )
            before = self._verify_descriptor_path_identity(descriptor)
            if before.st_size != FILL_COOLDOWN_WAL_FILE_BYTES:
                # Repair a file whose newer tail was truncated.  The following
                # data-sync covers both the size repair and the new record.
                os.ftruncate(descriptor, FILL_COOLDOWN_WAL_FILE_BYTES)
                self._fault("after_size_repair")
            try:
                self._fault("before_pwrite")
                written_total = 0
                while written_total < len(record):
                    written = os.pwrite(
                        descriptor,
                        record[written_total:],
                        offset + written_total,
                    )
                    if written <= 0:
                        raise OSError(
                            "fill cooldown checkpoint WAL write made no progress"
                        )
                    written_total += written
                self._fault("after_pwrite")
                self._fault("before_fdatasync")
                sync_primitive = _sync_data(descriptor)
                self._fault("after_fdatasync")
                self._verify_descriptor_path_identity(descriptor)
            except Exception:
                # A fault after pwrite may have left a structurally complete
                # record.  Force the next attempt to re-read durable state.
                self._last_sequence = None
                raise
            self._last_sequence = sequence
            return FillCooldownCheckpointWriteReceipt(
                sequence=sequence,
                slot_index=slot_index,
                payload_sha256=payload_sha256,
                bytes_written=written_total,
                sync_primitive=sync_primitive,
            )

    def close(self) -> None:
        with self._lock:
            if self._descriptor is None:
                return
            descriptor = self._descriptor
            self._descriptor = None
            self._last_sequence = None
            os.close(descriptor)


__all__ = [
    "FILL_COOLDOWN_WAL_FILE_BYTES",
    "FILL_COOLDOWN_WAL_HEADER_BYTES",
    "FILL_COOLDOWN_WAL_MAX_PAYLOAD_BYTES",
    "FILL_COOLDOWN_WAL_MODE",
    "FILL_COOLDOWN_WAL_SLOT_BYTES",
    "FillCooldownCheckpointCorruptionError",
    "FillCooldownCheckpointError",
    "FillCooldownCheckpointRecord",
    "FillCooldownCheckpointWAL",
    "FillCooldownCheckpointWriteReceipt",
    "canonical_checkpoint_bytes",
]
