"""Production-safe runtime writer for the F04 exact-opportunity tape."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import queue
import re
import shutil
import threading
import time
import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import yaml

from execution.exact_opportunity_tape import (
    EXACT_OPPORTUNITY_TAPE_SCHEMA_VERSION,
    ExactQuoteOpportunityTapeRow,
)
from features.feature_dag import CROSS_VENUE_FAIR_PRICE_GRAPH
from strategy.cross_venue_fair_price import CrossVenueFairPriceConfig

EXACT_OPPORTUNITY_RUNTIME_SCHEMA_VERSION = "exact_opportunity_runtime.v2.2"
EXACT_OPPORTUNITY_HEALTH_SCHEMA_VERSION = "exact_opportunity_writer_health.v2.2"
EXACT_OPPORTUNITY_CHUNK_MANIFEST_SCHEMA_VERSION = (
    "exact_opportunity_chunk_manifest.v2.2"
)
REQUIRED_EXTERNAL_VENUES = frozenset({"bitget", "bybit", "okx"})
_SAFE_SESSION = re.compile(r"^[A-Za-z0-9_.-]+$")
_SENTINEL = object()


def _canonical_json(payload: Any) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def canonical_sha256(payload: Any) -> str:
    return hashlib.sha256(_canonical_json(payload)).hexdigest()


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def exact_tape_schema_sha256() -> str:
    return canonical_sha256(
        {
            "schema_version": EXACT_OPPORTUNITY_TAPE_SCHEMA_VERSION,
            "columns": list(ExactQuoteOpportunityTapeRow.__dataclass_fields__),
        }
    )


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.partial")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(
            payload,
            handle,
            sort_keys=True,
            indent=2,
            ensure_ascii=True,
            allow_nan=False,
        )
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    _fsync_directory(path.parent)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _resolve_project_path(repo_root: Path, value: str) -> Path:
    path = Path(str(value)).expanduser()
    return path.resolve() if path.is_absolute() else (repo_root / path).resolve()


def exact_opportunity_config_payload(cfg: Any) -> dict[str, Any]:
    """Return the non-secret config surface that defines collection semantics."""

    external = getattr(cfg, "external_venues", None)
    multi = getattr(cfg, "multi_market", None)
    logging_cfg = getattr(cfg, "logging", None)
    strategy = getattr(cfg, "strategy", None)
    sources = []
    for source in getattr(external, "sources", ()):
        if not bool(getattr(source, "enabled", False)):
            continue
        sources.append(
            {
                "venue": str(getattr(source, "venue", "")).strip().lower(),
                "transport": str(getattr(source, "transport", "")).strip().lower(),
                "symbol": str(getattr(source, "symbol", "")).strip().upper(),
                "instrument_type": str(
                    getattr(source, "instrument_type", "")
                ).strip().lower(),
                "product_type": str(getattr(source, "product_type", "")).strip(),
                "max_source_age_s": float(
                    getattr(source, "max_source_age_s", 0.0) or 0.0
                ),
                "record_enabled": bool(getattr(source, "record_enabled", False)),
            }
        )
    sources.sort(
        key=lambda row: (
            row["venue"],
            row["instrument_type"],
            row["symbol"],
        )
    )
    return {
        "schema_version": EXACT_OPPORTUNITY_RUNTIME_SCHEMA_VERSION,
        "symbol": str(getattr(cfg, "symbol", "")).strip().upper(),
        "tick_size": float(getattr(cfg, "tick_size", 0.0) or 0.0),
        "max_spread_bps": float(
            getattr(strategy, "max_spread_bps", 0.0) or 0.0
        ),
        "exact_opportunity_tape_enabled": bool(
            getattr(logging_cfg, "exact_opportunity_tape_enabled", False)
        ),
        "writer": {
            "queue_size": int(
                getattr(logging_cfg, "exact_opportunity_tape_queue_size", 20_000)
                or 20_000
            ),
            "flush_rows": int(
                getattr(logging_cfg, "exact_opportunity_tape_flush_rows", 1_000)
                or 1_000
            ),
            "flush_interval_s": float(
                getattr(
                    logging_cfg,
                    "exact_opportunity_tape_flush_interval_s",
                    1.0,
                )
                or 1.0
            ),
            "heartbeat_interval_s": float(
                getattr(
                    logging_cfg,
                    "exact_opportunity_tape_heartbeat_interval_s",
                    5.0,
                )
                or 5.0
            ),
        },
        "external_venues_enabled": bool(getattr(external, "enabled", False)),
        "external_venues_shadow_only": bool(
            getattr(external, "shadow_only", True)
        ),
        "external_sources": sources,
        "multi_market_enabled": bool(getattr(multi, "enabled", False)),
        "reference_symbol": str(
            getattr(multi, "reference_symbol", "")
        ).strip().upper(),
        "stablecoin_anchor_symbol": str(
            getattr(multi, "stablecoin_anchor_symbol", "")
        ).strip().upper(),
        "fair_price_shadow_action_surface": "evidence_only",
    }


def validate_exact_opportunity_runtime_config(
    cfg: Any,
    *,
    require_enabled: bool = False,
) -> dict[str, Any]:
    """Fail closed when an enabled tape lacks its causal input surface."""

    payload = exact_opportunity_config_payload(cfg)
    enabled = bool(payload["exact_opportunity_tape_enabled"])
    if require_enabled and not enabled:
        raise ValueError("exact-opportunity prospective collection is disabled")
    if not enabled:
        return {
            "enabled": False,
            "valid": True,
            "config_identity_sha256": canonical_sha256(payload),
            "enabled_venues": [],
        }
    if not payload["external_venues_enabled"]:
        raise ValueError(
            "exact-opportunity tape requires external_venues.enabled=true"
        )
    if not payload["external_venues_shadow_only"]:
        raise ValueError(
            "exact-opportunity external inputs must remain shadow_only=true"
        )
    if not payload["multi_market_enabled"]:
        raise ValueError("exact-opportunity tape requires multi_market.enabled=true")
    if not payload["stablecoin_anchor_symbol"]:
        raise ValueError("exact-opportunity tape requires a stablecoin anchor")
    enabled_venues = {
        str(source["venue"]) for source in payload["external_sources"]
    }
    missing = sorted(REQUIRED_EXTERNAL_VENUES - enabled_venues)
    if missing:
        raise ValueError(
            "exact-opportunity tape requires Bitget, Bybit, and OKX; "
            f"missing={missing}"
        )
    return {
        "enabled": True,
        "valid": True,
        "config_identity_sha256": canonical_sha256(payload),
        "enabled_venues": sorted(enabled_venues),
    }


def build_exact_opportunity_runtime_identity(
    cfg: Any,
    *,
    repo_root: str | Path,
) -> dict[str, Any]:
    """Bind producer, fair-state graph, and the active non-secret config."""

    root = Path(repo_root).expanduser().resolve()
    config_payload = exact_opportunity_config_payload(cfg)
    components = {}
    for relative in (
        "execution/exact_opportunity_tape.py",
        "execution/exact_opportunity_tape_runtime.py",
        "features/feature_dag.py",
        "live/config.py",
        "strategy/cross_venue_fair_price.py",
        "strategy/external_adverse_quote_edge_guard.py",
        "strategy/maker_engine.py",
        "strategy/order_manager.py",
    ):
        path = root / relative
        if not path.is_file():
            raise FileNotFoundError(path)
        components[relative] = sha256_file(path)
    source_path = str(getattr(cfg, "_config_source_path", "") or "")
    if not source_path:
        from live import config as live_config

        source_path = str(live_config._cfg_path)
    source_sha256 = sha256_file(source_path) if source_path else ""
    if not source_path or not Path(source_path).is_file():
        raise ValueError("exact-opportunity runtime requires a config artifact path")
    if sha256_file(source_path) != source_sha256:
        raise ValueError("exact-opportunity config artifact hash mismatch")
    from live.config import _parse

    artifact_raw = yaml.safe_load(Path(source_path).read_text(encoding="utf-8")) or {}
    artifact_payload = exact_opportunity_config_payload(_parse(artifact_raw))
    if canonical_sha256(artifact_payload) != canonical_sha256(config_payload):
        raise ValueError(
            "exact-opportunity in-memory config does not match its artifact"
        )
    identity = {
        "schema_version": EXACT_OPPORTUNITY_RUNTIME_SCHEMA_VERSION,
        "tape_schema_sha256": exact_tape_schema_sha256(),
        "feature_graph_id": CROSS_VENUE_FAIR_PRICE_GRAPH.graph_id,
        "feature_graph_sha256": CROSS_VENUE_FAIR_PRICE_GRAPH.sha256(),
        "fair_state_config": asdict(CrossVenueFairPriceConfig()),
        "config_payload": config_payload,
        "config_payload_sha256": canonical_sha256(config_payload),
        "config_artifact_path": source_path,
        "config_artifact_sha256": source_sha256,
        "components": components,
    }
    identity["runtime_identity_sha256"] = canonical_sha256(identity)
    return identity


def _event_utc_day(payload: Mapping[str, Any]) -> str:
    event_ts_ns = int(payload.get("event_ts_ns", 0) or 0)
    if event_ts_ns <= 0:
        raise ValueError("exact-opportunity row requires positive event_ts_ns")
    return datetime.fromtimestamp(
        event_ts_ns / 1_000_000_000.0,
        tz=timezone.utc,
    ).strftime("%Y-%m-%d")


class ExactOpportunityDailyWriter:
    """Asynchronous daily/session writer with quarantine and health telemetry."""

    def __init__(
        self,
        staging_root: str | Path,
        *,
        runtime_identity: Mapping[str, Any],
        initial_active_order_ids: set[str] | frozenset[str] = frozenset(),
        session_id: str | None = None,
        queue_size: int = 20_000,
        flush_rows: int = 1_000,
        flush_interval_s: float = 1.0,
        heartbeat_interval_s: float = 5.0,
    ) -> None:
        self.staging_root = Path(staging_root).expanduser().resolve()
        if str(self.staging_root).startswith("/Volumes/"):
            raise ValueError(
                "exact-opportunity runtime staging must use local temporary storage"
            )
        self.staging_root.mkdir(parents=True, exist_ok=True)
        self.session_id = session_id or self._default_session_id()
        if not _SAFE_SESSION.fullmatch(self.session_id):
            raise ValueError("unsafe exact-opportunity session_id")
        self.runtime_identity = dict(runtime_identity)
        expected_identity = str(
            self.runtime_identity.get("runtime_identity_sha256", "")
        )
        normalized = dict(self.runtime_identity)
        normalized.pop("runtime_identity_sha256", None)
        if canonical_sha256(normalized) != expected_identity:
            raise ValueError("exact-opportunity runtime identity hash mismatch")
        self.queue_size = int(queue_size)
        self.flush_rows = int(flush_rows)
        self.flush_interval_s = float(flush_interval_s)
        self.heartbeat_interval_s = float(heartbeat_interval_s)
        if self.queue_size <= 0 or self.flush_rows <= 0:
            raise ValueError("writer queue_size and flush_rows must be positive")
        if self.flush_interval_s <= 0.0 or self.heartbeat_interval_s <= 0.0:
            raise ValueError("writer flush and heartbeat intervals must be positive")

        self.session_root = self.staging_root / f"session-{self.session_id}"
        if self.session_root.exists():
            raise FileExistsError(
                f"exact-opportunity session already exists: {self.session_root}"
            )
        self.session_root.mkdir(parents=True)
        self.health_path = self.session_root / "health.json"
        self.identity_path = self.session_root / "runtime_identity.json"
        _atomic_json(self.identity_path, self.runtime_identity)

        self._lock = threading.Lock()
        self._queue: queue.Queue[object] = queue.Queue(maxsize=self.queue_size)
        self._quarantine_ids = {
            str(value) for value in initial_active_order_ids if str(value)
        }
        self._state = "quarantine" if self._quarantine_ids else "collecting"
        self._rows_enqueued = 0
        self._rows_written = 0
        self._rows_dropped = 0
        self._rows_quarantined = 0
        self._error_count = 0
        self._last_error = ""
        self._last_enqueue_ts_ns = 0
        self._last_flush_ts_ns = 0
        self._last_heartbeat_ts_ns = 0
        self._active_day = ""
        self._active_partial_path = ""
        self._closed = False
        self._submission_owner = ""
        self._direct_io_lock = threading.Lock()
        self._direct_handle = None
        self._direct_writer = None
        self._direct_partial: Path | None = None
        self._direct_day = ""
        self._direct_rows = 0
        self._direct_digest = hashlib.sha256()
        self._direct_first_event_ts_ns = 0
        self._direct_last_event_ts_ns = 0
        self._worker = threading.Thread(
            target=self._run,
            name=f"exact-opportunity-{self.session_id}",
            daemon=True,
        )
        self._write_health(force=True)
        self._worker.start()

    @staticmethod
    def _default_session_id() -> str:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        return f"{stamp}Z-p{os.getpid()}-{uuid.uuid4().hex[:8]}"

    @property
    def collecting(self) -> bool:
        with self._lock:
            return self._state == "collecting" and not self._closed

    def append(self, payload: Mapping[str, Any]) -> bool:
        row = asdict(ExactQuoteOpportunityTapeRow(**dict(payload)))
        with self._lock:
            if self._closed:
                raise RuntimeError("exact-opportunity writer is closed")
            if self._submission_owner not in {"", "internal_queue"}:
                raise RuntimeError("exact-opportunity submission owner changed")
            self._submission_owner = "internal_queue"
            if self._state == "quarantine":
                self._rows_quarantined += 1
                self._write_health_locked(force=False)
                return False
        try:
            self._queue.put_nowait(row)
        except queue.Full:
            with self._lock:
                self._rows_dropped += 1
                self._last_error = "queue_full"
                self._write_health_locked(force=True)
            return False
        with self._lock:
            self._rows_enqueued += 1
            self._last_enqueue_ts_ns = time.time_ns()
        return True

    def commit_frozen(self, payload: Mapping[str, Any]) -> bool:
        """Synchronously commit an immutable row from an owning FIFO worker."""

        row = asdict(ExactQuoteOpportunityTapeRow(**dict(payload)))
        with self._lock:
            if self._closed:
                raise RuntimeError("exact-opportunity writer is closed")
            if self._submission_owner not in {"", "external_fifo"}:
                raise RuntimeError("exact-opportunity submission owner changed")
            self._submission_owner = "external_fifo"
            if self._state == "quarantine":
                self._rows_quarantined += 1
                self._write_health_locked(force=False)
                return False
            self._rows_enqueued += 1
            self._last_enqueue_ts_ns = time.time_ns()
        # Direct commits are owned by RuntimeEvidenceWriter's sole worker.
        # The lock also prevents accidental mixing with the legacy adapter.
        try:
            with self._direct_io_lock:
                self._commit_direct_row(row)
        except Exception as exc:
            self.report_error(f"direct_commit:{type(exc).__name__}:{exc}")
            raise
        return True

    def _commit_direct_row(self, row: dict[str, Any]) -> None:
        day = _event_utc_day(row)
        if self._direct_day and day != self._direct_day:
            self._finalize_direct_chunk()
        if self._direct_handle is None:
            (
                self._direct_handle,
                self._direct_writer,
                self._direct_partial,
                self._direct_day,
                _drop_start,
                _error_start,
            ) = self._open_chunk(day)
        self._direct_writer.writerow(row)
        self._direct_digest.update(_canonical_json(row))
        event_ts_ns = int(row["event_ts_ns"])
        if (
            self._direct_first_event_ts_ns == 0
            or event_ts_ns < self._direct_first_event_ts_ns
        ):
            self._direct_first_event_ts_ns = event_ts_ns
        self._direct_last_event_ts_ns = max(
            self._direct_last_event_ts_ns,
            event_ts_ns,
        )
        self._direct_rows += 1
        # The legacy worker flushed sparse streams on its periodic wake.  A
        # direct commit has no second worker deadline, so make completion carry
        # the same durable-write meaning immediately.
        self._flush_handle(self._direct_handle)
        with self._lock:
            self._rows_written += 1
            self._write_health_locked(force=False)

    def _finalize_direct_chunk(self) -> None:
        if self._direct_handle is None:
            return
        self._finalize_chunk(
            handle=self._direct_handle,
            partial_path=self._direct_partial,
            utc_day=self._direct_day,
            row_count=self._direct_rows,
            row_sha256=self._direct_digest.hexdigest(),
            first_event_ts_ns=self._direct_first_event_ts_ns,
            last_event_ts_ns=self._direct_last_event_ts_ns,
            drop_start=0,
            error_start=0,
        )
        self._direct_handle = None
        self._direct_writer = None
        self._direct_partial = None
        self._direct_day = ""
        self._direct_rows = 0
        self._direct_digest = hashlib.sha256()
        self._direct_first_event_ts_ns = 0
        self._direct_last_event_ts_ns = 0

    def observe_order_terminal(self, client_order_id: str) -> None:
        cid = str(client_order_id)
        with self._lock:
            if self._closed or self._state != "quarantine":
                return
            self._quarantine_ids.discard(cid)
            if not self._quarantine_ids:
                self._state = "collecting"
            self._write_health_locked(force=True)

    def report_error(self, reason: str) -> None:
        """Invalidate the current session after a producer-side write error."""

        with self._lock:
            if self._closed:
                return
            self._error_count += 1
            self._last_error = str(reason)
            self._write_health_locked(force=True)

    def health_snapshot(self) -> dict[str, Any]:
        with self._lock:
            return self._health_payload_locked()

    def close(self, *, timeout_s: float = 10.0) -> dict[str, Any]:
        deadline = time.monotonic() + max(0.1, float(timeout_s))
        try:
            with self._direct_io_lock:
                self._finalize_direct_chunk()
        except Exception as exc:
            self.report_error(f"direct_finalize:{type(exc).__name__}:{exc}")
        with self._lock:
            if self._closed:
                return self._health_payload_locked()
            self._state = "closing"
        try:
            self._queue.put(
                _SENTINEL,
                timeout=max(0.0, deadline - time.monotonic()),
            )
        except queue.Full:
            with self._lock:
                self._error_count += 1
                self._last_error = "writer_close_queue_full"
                self._state = "error"
                self._closed = True
                self._write_health_locked(force=True)
            return self.health_snapshot()
        self._worker.join(timeout=max(0.0, deadline - time.monotonic()))
        if self._worker.is_alive():
            with self._lock:
                self._error_count += 1
                self._last_error = "writer_close_timeout"
                self._state = "error"
                self._closed = True
                self._write_health_locked(force=True)
        else:
            with self._lock:
                if (
                    not self._closed
                    or self._state != "closed"
                    or self._rows_written != self._rows_enqueued
                    or not self._queue.empty()
                ):
                    if self._error_count == 0:
                        self._error_count = 1
                        self._last_error = "writer_close_incomplete"
                    self._state = "error"
                    self._closed = True
                    self._write_health_locked(force=True)
        return self.health_snapshot()

    def _health_payload_locked(self) -> dict[str, Any]:
        return {
            "schema_version": EXACT_OPPORTUNITY_HEALTH_SCHEMA_VERSION,
            "session_id": self.session_id,
            "state": self._state,
            "closed": bool(self._closed),
            "runtime_identity_sha256": self.runtime_identity[
                "runtime_identity_sha256"
            ],
            "queue_size": self.queue_size,
            "queue_depth": self._queue.qsize(),
            "rows_enqueued": self._rows_enqueued,
            "rows_written": self._rows_written,
            "rows_dropped": self._rows_dropped,
            "rows_quarantined": self._rows_quarantined,
            "error_count": self._error_count,
            "last_error": self._last_error,
            "last_enqueue_ts_ns": self._last_enqueue_ts_ns,
            "last_flush_ts_ns": self._last_flush_ts_ns,
            "last_heartbeat_ts_ns": self._last_heartbeat_ts_ns,
            "quarantine_order_ids": sorted(self._quarantine_ids),
            "active_day": self._active_day,
            "active_partial_path": self._active_partial_path,
            "formal_collection_valid": bool(
                self._rows_dropped == 0 and self._error_count == 0
            ),
        }

    def _write_health(self, *, force: bool) -> None:
        with self._lock:
            self._write_health_locked(force=force)

    def _write_health_locked(self, *, force: bool) -> None:
        now_ns = time.time_ns()
        if (
            not force
            and self._last_heartbeat_ts_ns > 0
            and now_ns - self._last_heartbeat_ts_ns
            < self.heartbeat_interval_s * 1_000_000_000.0
        ):
            return
        self._last_heartbeat_ts_ns = now_ns
        _atomic_json(self.health_path, self._health_payload_locked())

    def _run(self) -> None:
        handle = None
        writer = None
        row_digest = hashlib.sha256()
        current_rows = 0
        current_day = ""
        current_partial: Path | None = None
        first_event_ts_ns = 0
        last_event_ts_ns = 0
        chunk_drop_start = 0
        chunk_error_start = 0
        last_flush = time.monotonic()
        try:
            while True:
                timeout = min(self.flush_interval_s, self.heartbeat_interval_s)
                try:
                    item = self._queue.get(timeout=timeout)
                except queue.Empty:
                    item = None
                if item is _SENTINEL:
                    break
                if isinstance(item, dict):
                    day = _event_utc_day(item)
                    if current_day and day != current_day:
                        self._finalize_chunk(
                            handle=handle,
                            partial_path=current_partial,
                            utc_day=current_day,
                            row_count=current_rows,
                            row_sha256=row_digest.hexdigest(),
                            first_event_ts_ns=first_event_ts_ns,
                            last_event_ts_ns=last_event_ts_ns,
                            drop_start=chunk_drop_start,
                            error_start=chunk_error_start,
                        )
                        handle = None
                        writer = None
                        current_rows = 0
                        row_digest = hashlib.sha256()
                        current_day = ""
                        current_partial = None
                        first_event_ts_ns = 0
                        last_event_ts_ns = 0
                    if handle is None:
                        (
                            handle,
                            writer,
                            current_partial,
                            current_day,
                            chunk_drop_start,
                            chunk_error_start,
                        ) = self._open_chunk(day)
                    writer.writerow(item)
                    row_digest.update(_canonical_json(item))
                    event_ts_ns = int(item["event_ts_ns"])
                    if first_event_ts_ns == 0 or event_ts_ns < first_event_ts_ns:
                        first_event_ts_ns = event_ts_ns
                    last_event_ts_ns = max(last_event_ts_ns, event_ts_ns)
                    current_rows += 1
                    with self._lock:
                        self._rows_written += 1
                now = time.monotonic()
                if handle is not None and (
                    current_rows % self.flush_rows == 0
                    or now - last_flush >= self.flush_interval_s
                ):
                    self._flush_handle(handle)
                    last_flush = now
                self._write_health(force=False)
            if handle is not None:
                self._finalize_chunk(
                    handle=handle,
                    partial_path=current_partial,
                    utc_day=current_day,
                    row_count=current_rows,
                    row_sha256=row_digest.hexdigest(),
                    first_event_ts_ns=first_event_ts_ns,
                    last_event_ts_ns=last_event_ts_ns,
                    drop_start=chunk_drop_start,
                    error_start=chunk_error_start,
                )
        except Exception as exc:
            if handle is not None:
                try:
                    handle.close()
                except Exception:
                    pass
            with self._lock:
                self._error_count += 1
                self._last_error = f"{type(exc).__name__}:{exc}"
                self._state = "error"
        finally:
            with self._lock:
                if self._state != "error":
                    self._state = "closed"
                self._closed = True
                self._active_day = ""
                self._active_partial_path = ""
                self._write_health_locked(force=True)

    def _open_chunk(self, day: str):
        day_root = self.session_root / f"utc_day={day}"
        day_root.mkdir(parents=True, exist_ok=True)
        stem = f"exact-opportunity-{day}-{self.session_id}"
        partial = day_root / f"{stem}.csv.partial"
        handle = partial.open("x", newline="", encoding="utf-8")
        writer = csv.DictWriter(
            handle,
            fieldnames=list(ExactQuoteOpportunityTapeRow.__dataclass_fields__),
        )
        writer.writeheader()
        with self._lock:
            self._active_day = day
            self._active_partial_path = str(partial)
            # Drops/errors are sticky for the rest of a session. A restart gets
            # a new session identity; a damaged session never self-heals into a
            # later apparently-valid chunk.
            drop_start = 0
            error_start = 0
            self._write_health_locked(force=True)
        return handle, writer, partial, day, drop_start, error_start

    def _flush_handle(self, handle: Any) -> None:
        handle.flush()
        os.fsync(handle.fileno())
        with self._lock:
            self._last_flush_ts_ns = time.time_ns()

    def _finalize_chunk(
        self,
        *,
        handle: Any,
        partial_path: Path | None,
        utc_day: str,
        row_count: int,
        row_sha256: str,
        first_event_ts_ns: int,
        last_event_ts_ns: int,
        drop_start: int,
        error_start: int,
    ) -> None:
        if partial_path is None or handle is None:
            return
        self._flush_handle(handle)
        handle.close()
        with self._lock:
            chunk_drops = self._rows_dropped - drop_start
            chunk_errors = self._error_count - error_start
        ready = partial_path.with_suffix("")
        valid = row_count > 0 and chunk_drops == 0 and chunk_errors == 0
        if valid:
            os.replace(partial_path, ready)
            _fsync_directory(ready.parent)
            file_path = ready
        else:
            invalid = partial_path.with_name(
                partial_path.name.replace(".csv.partial", ".invalid.csv.partial")
            )
            os.replace(partial_path, invalid)
            _fsync_directory(invalid.parent)
            file_path = invalid
        manifest = {
            "schema_version": EXACT_OPPORTUNITY_CHUNK_MANIFEST_SCHEMA_VERSION,
            "chunk_id": f"{utc_day}:{self.session_id}",
            "utc_day": utc_day,
            "session_id": self.session_id,
            "complete": bool(valid),
            "valid": bool(valid),
            "file_name": file_path.name,
            "row_count": int(row_count),
            "first_event_ts_ns": int(first_event_ts_ns),
            "last_event_ts_ns": int(last_event_ts_ns),
            "row_sha256": row_sha256,
            "schema_sha256": exact_tape_schema_sha256(),
            "file_sha256": sha256_file(file_path),
            "file_bytes": file_path.stat().st_size,
            "rows_dropped": int(chunk_drops),
            "error_count": int(chunk_errors),
            "runtime_identity_sha256": self.runtime_identity[
                "runtime_identity_sha256"
            ],
            "runtime_identity_file": os.path.relpath(
                self.identity_path,
                file_path.parent,
            ),
            "closed_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        suffix = ".ready.manifest.json" if valid else ".invalid.manifest.json"
        manifest_path = file_path.parent / (
            file_path.name.split(".csv", 1)[0] + suffix
        )
        _atomic_json(manifest_path, manifest)


def copy_file_fsync(source: Path, destination_partial: Path) -> None:
    destination_partial.parent.mkdir(parents=True, exist_ok=True)
    with source.open("rb") as src, destination_partial.open("xb") as dst:
        shutil.copyfileobj(src, dst, length=1024 * 1024)
        dst.flush()
        os.fsync(dst.fileno())
