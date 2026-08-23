#!/usr/bin/env python3
"""Run the zero-economic BUY E3 concurrency, mmap, and cache durability gate."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import secrets
import stat
import subprocess
import sys
import threading
import time
from collections.abc import Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from research.families.f05_fill_quality_quote_ev.audit import (  # noqa: E402
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_replay_adapter_v1 as replay_adapter,
)
from scripts import f05_buy_e3_stability_receipts as stability  # noqa: E402

IDENTITY = stability.OWNER_IDENTITY
SCHEMA_VERSION = stability.DURABILITY_HARNESS_SCHEMA
MEASUREMENT_SCHEMA = stability.DURABILITY_MEASUREMENT_SCHEMA
STATUS = "durability_harness_passed"
EVIDENCE_BOUNDARY = stability.EVIDENCE_BOUNDARY
PERMISSIONS = stability.PERMISSIONS
CONFIGURED_WORKER_COUNT = stability.EXPECTED_WORKER_COUNT
TASKS_PER_CASE = CONFIGURED_WORKER_COUNT

RUN_MANIFEST_SCHEMA = stability.DURABILITY_PROBE_RUN_MANIFEST_SCHEMA
CACHE_NAMESPACE_SCHEMA = stability.DURABILITY_PROBE_CACHE_NAMESPACE_SCHEMA
PROBE_SCHEMA = stability.DURABILITY_PROBE_SCHEMA
CACHE_PROBE_SCHEMA = stability.DURABILITY_CACHE_PROBE_SCHEMA
RECEIPT_CANONICAL_FIELD = "canonical_receipt_sha256"
MAX_RECEIPT_BYTES = 16 << 20
PROBE_TIMEOUT_SECONDS = 60

TESTED_SOURCE_RELATIVE_PATHS = stability.DURABILITY_RUNTIME_SOURCE_FILES
HARNESS_TEST_FILE = stability.DURABILITY_HARNESS_TEST_FILE
GATE_NODEIDS = stability.DURABILITY_GATE_NODEIDS
HARNESS_NODEIDS = stability.DURABILITY_HARNESS_NODEIDS

MEASUREMENT_FIELDS = frozenset(
    {
        "schema_version",
        "identity",
        "status",
        "configured_worker_count",
        "peak_concurrent_worker_count",
        "submitted_task_count",
        "terminal_task_count",
        "repeated_run_count",
        "interruption_resume_count",
        "cache_entry_count",
        "cache_hit_count",
        "mmap_open_count",
        "mmap_close_count",
        "checks",
        "failure_counts",
        "tested_source_manifest",
        "tested_source_manifest_sha256",
        "probe_run_manifest",
        "probe_run_manifest_sha256",
        "probe_cache_namespace",
        "probe_cache_namespace_sha256",
        "event_series_sha256",
        "probe_measurements",
        "cache_measurements",
        "evidence_boundary",
        "permissions",
        "economic_outcomes_read",
        "economic_values_exposed",
        "economic_values_used_for_selection",
        "validation_read",
        "sealed_holdout_read",
    }
)

RECEIPT_FIELDS = frozenset(
    {
        "schema_version",
        "identity",
        "status",
        "python_executable",
        "python_file_sha256",
        "run_command",
        "nodeids",
        "nodeid_manifest_sha256",
        "gate_nodeids",
        "counts",
        "test_files",
        "runtime_sources",
        "tested_source_manifest_sha256",
        "measurement",
        "measurement_sha256",
        "observations",
        "failure_counts",
        "probe_cache_namespace_sha256",
        "probe_run_manifest_sha256",
        "event_series_sha256",
        "evidence_boundary",
        "permissions",
        RECEIPT_CANONICAL_FIELD,
    }
)


class DurabilityGateError(RuntimeError):
    """Raised when durability evidence is incomplete or inconsistent."""


class _InjectedWorkerFailure(RuntimeError):
    """Expected worker failure used to exercise sibling cancellation and joining."""


class _InjectedCacheInterruption(RuntimeError):
    """Expected cache interruption used to exercise staging cleanup and resume."""


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    ).hexdigest()


def _canonical_json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
        + b"\n"
    )


def _document_sha256(payload: Mapping[str, Any], field: str) -> str:
    body = dict(payload)
    body.pop(field, None)
    return canonical_sha256(body)


def _file_sha256(path: Path) -> str:
    try:
        return stability._file_sha256(path, label=f"durability source {path}")  # noqa: SLF001
    except stability.StabilityReceiptError as exc:
        raise DurabilityGateError(str(exc)) from exc


def _require_sha256(value: Any, label: str) -> str:
    normalized = str(value).strip().lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise DurabilityGateError(f"{label} is not a lowercase SHA256")
    return normalized


def _fixture_array() -> np.ndarray:
    return np.arange(64 * 1024, dtype=np.int64)


def _fixture_sha256() -> str:
    return hashlib.sha256(_fixture_array().tobytes(order="C")).hexdigest()


def _tested_source_hashes(repository_root: Path) -> dict[str, str]:
    try:
        manifest = stability.durability_tested_source_manifest(repository_root)
    except stability.StabilityReceiptError as exc:
        raise DurabilityGateError(str(exc)) from exc
    return dict(manifest["runtime_sources"])


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_all(descriptor: int, raw: bytes) -> None:
    view = memoryview(raw)
    offset = 0
    while offset < len(view):
        written = os.write(descriptor, view[offset:])
        if written <= 0:
            raise DurabilityGateError("private receipt write stopped early")
        offset += written


def _write_private_receipt_no_replace(path: Path, payload: Mapping[str, Any]) -> str:
    destination = path.expanduser().absolute()
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    parent_stat = destination.parent.lstat()
    if not stat.S_ISDIR(parent_stat.st_mode) or stat.S_ISLNK(parent_stat.st_mode):
        raise DurabilityGateError("receipt parent must be a real directory")
    if parent_stat.st_uid != os.geteuid():
        raise DurabilityGateError("receipt parent is not owned by the current user")
    if destination.exists() or destination.is_symlink():
        raise DurabilityGateError("durability receipt already exists")

    raw = _canonical_json_bytes(payload)
    temporary = destination.parent / (
        f".{destination.name}.{os.getpid()}.{secrets.token_hex(8)}.tmp"
    )
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(temporary, flags, 0o600)
    linked = False
    try:
        os.fchmod(descriptor, 0o600)
        _write_all(descriptor, raw)
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        try:
            os.link(temporary, destination, follow_symlinks=False)
        except FileExistsError as exc:
            raise DurabilityGateError("durability receipt already exists") from exc
        linked = True
        _fsync_directory(destination.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)
        if linked:
            _fsync_directory(destination.parent)
    observed = destination.stat()
    if (
        not stat.S_ISREG(observed.st_mode)
        or stat.S_IMODE(observed.st_mode) != 0o600
        or observed.st_uid != os.geteuid()
        or observed.st_nlink != 1
    ):
        raise DurabilityGateError("published durability receipt is not private and immutable")
    return hashlib.sha256(raw).hexdigest()


def _read_private_receipt(path: Path) -> dict[str, Any]:
    candidate = path.expanduser().absolute()
    try:
        lexical = candidate.lstat()
    except FileNotFoundError as exc:
        raise DurabilityGateError("durability receipt does not exist") from exc
    if stat.S_ISLNK(lexical.st_mode):
        raise DurabilityGateError("durability receipt must not be a symbolic link")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(candidate, flags)
    try:
        observed = os.fstat(descriptor)
        if (
            (lexical.st_dev, lexical.st_ino) != (observed.st_dev, observed.st_ino)
            or not stat.S_ISREG(observed.st_mode)
            or stat.S_IMODE(observed.st_mode) != 0o600
            or observed.st_uid != os.geteuid()
            or observed.st_nlink != 1
            or observed.st_size <= 0
            or observed.st_size > MAX_RECEIPT_BYTES
        ):
            raise DurabilityGateError("durability receipt is not an admitted private file")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            raw = handle.read(MAX_RECEIPT_BYTES + 1)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(raw) != observed.st_size or len(raw) > MAX_RECEIPT_BYTES:
        raise DurabilityGateError("durability receipt changed while it was read")
    try:
        payload = json.loads(raw.decode("ascii"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DurabilityGateError("durability receipt is not ASCII JSON") from exc
    if not isinstance(payload, dict):
        raise DurabilityGateError("durability receipt root is not an object")
    return payload


def _worker_result_sha256(case: str, task_id: int, compared_count: int) -> str:
    return canonical_sha256(
        {
            "schema_version": PROBE_SCHEMA,
            "case": case,
            "task_id": task_id,
            "compared_count": compared_count,
            "economic_outcome": None,
        }
    )


def _exercise_mmap_case(case: str, work_root: Path) -> dict[str, Any]:
    if case not in {"success", "injected_exception"}:
        raise DurabilityGateError("unknown mmap probe case")
    topology = replay_adapter.OneShotProcessTopology(total_worker_tokens=CONFIGURED_WORKER_COUNT)
    if topology.arm_workers != CONFIGURED_WORKER_COUNT:
        raise DurabilityGateError("production one-shot topology no longer has ten arm workers")

    work_root.mkdir(parents=True, exist_ok=True)
    fixture_path = work_root / f"{case}.int64"
    _fixture_array().tofile(fixture_path)
    os.chmod(fixture_path, 0o400)

    lock = threading.Lock()
    barrier = threading.Barrier(TASKS_PER_CASE)
    release_siblings = threading.Event()
    state: dict[str, Any] = {
        "active": 0,
        "peak": 0,
        "mapping_open": True,
        "mmap_use_after_close_count": 0,
        "cancel_request_count": 0,
        "task_results": {},
    }
    mapping = np.memmap(fixture_path, dtype=np.int64, mode="r")
    if mapping.flags.writeable:
        raise DurabilityGateError("mmap probe did not open read-only")

    prefix = f"f05-buy-e3-durability-{case}"
    futures: list[Future[Any]] = []
    consumed: list[str] = []
    expected_exception_observed = False
    unexpected_exception: BaseException | None = None

    def worker(task_id: int) -> str:
        with lock:
            state["active"] += 1
            state["peak"] = max(state["peak"], state["active"])
        try:
            barrier.wait(timeout=10.0)
            if case == "injected_exception" and task_id == 0:
                raise _InjectedWorkerFailure("injected worker failure")
            if case == "injected_exception" and not release_siblings.wait(timeout=10.0):
                raise DurabilityGateError("injected-failure siblings were not released")
            with lock:
                if not state["mapping_open"]:
                    state["mmap_use_after_close_count"] += 1
            start = task_id * 1024
            values = mapping[start : start + 4096]
            compared_count = int(np.count_nonzero(values >= (task_id * 1024 + 512)))
            result_sha256 = _worker_result_sha256(case, task_id, compared_count)
            with lock:
                state["task_results"][task_id] = {
                    "task_id": task_id,
                    "compared_count": compared_count,
                    "result_sha256": result_sha256,
                }
            return result_sha256
        finally:
            with lock:
                state["active"] -= 1

    timer: threading.Timer | None = None
    lifecycle_events: list[dict[str, Any]] = []
    pool = ThreadPoolExecutor(
        max_workers=topology.arm_workers,
        thread_name_prefix=prefix,
    )
    lifecycle_events.append({"sequence": 0, "event": "pool_created"})
    pool_shutdown_call_count = 0
    mapping_closed = False
    terminal_before_pool_shutdown_count = -1
    mmap_handle = getattr(mapping, "_mmap", None)
    if mmap_handle is None:
        pool.shutdown(wait=True, cancel_futures=True)
        raise DurabilityGateError("NumPy memmap does not expose its mmap handle")

    try:
        futures = [pool.submit(worker, task_id) for task_id in range(TASKS_PER_CASE)]
        lifecycle_events.append({"sequence": 1, "event": "tasks_submitted"})
        for future in futures:
            original_cancel = future.cancel

            def tracked_cancel(original: Any = original_cancel) -> bool:
                with lock:
                    state["cancel_request_count"] += 1
                return bool(original())

            future.cancel = tracked_cancel  # type: ignore[method-assign]
        if case == "injected_exception":
            timer = threading.Timer(0.05, release_siblings.set)
            timer.start()
        try:
            replay_adapter._consume_arm_futures_before_mmap_close(  # noqa: SLF001
                futures,
                consume=lambda result: consumed.append(str(result)),
            )
            lifecycle_events.append({"sequence": 2, "event": "helper_returned"})
        except _InjectedWorkerFailure:
            expected_exception_observed = case == "injected_exception"
            if not expected_exception_observed:
                raise
            lifecycle_events.append({"sequence": 2, "event": "helper_raised_expected"})
        except BaseException as exc:  # pragma: no cover - fail-closed diagnostic path
            unexpected_exception = exc
            lifecycle_events.append({"sequence": 2, "event": "helper_raised_unexpected"})

        terminal_before_pool_shutdown_count = sum(int(future.done()) for future in futures)
        if terminal_before_pool_shutdown_count != len(futures):
            raise DurabilityGateError(
                "production mmap drain helper returned before every future was terminal"
            )
        lifecycle_events.append(
            {"sequence": 3, "event": "all_futures_terminal_before_pool_shutdown"}
        )
        with lock:
            state["mapping_open"] = False
        mmap_handle.close()
        mapping_closed = True
        lifecycle_events.append({"sequence": 4, "event": "mmap_closed"})
        pool.shutdown(wait=True, cancel_futures=True)
        pool_shutdown_call_count += 1
        lifecycle_events.append({"sequence": 5, "event": "pool_shutdown_complete"})
    finally:
        release_siblings.set()
        if timer is not None:
            timer.join(timeout=2.0)
        if pool_shutdown_call_count == 0:
            for future in futures:
                future.cancel()
            pool.shutdown(wait=True, cancel_futures=True)
            pool_shutdown_call_count += 1
        if not mapping_closed:
            with lock:
                state["mapping_open"] = False
            mmap_handle.close()
            mapping_closed = True

    terminal_count = sum(int(future.done()) for future in futures)
    mmap_close_before_terminal_count = int(terminal_before_pool_shutdown_count != len(futures))
    pool_shutdown_complete = not any(
        thread.name.startswith(prefix) for thread in threading.enumerate()
    )

    if unexpected_exception is not None:
        raise DurabilityGateError(
            f"unexpected worker exception: {type(unexpected_exception).__name__}"
        ) from unexpected_exception
    if case == "success" and expected_exception_observed:
        raise DurabilityGateError("success case observed an injected exception")
    if case == "injected_exception" and not expected_exception_observed:
        raise DurabilityGateError("injected exception did not reach the production drain helper")

    task_results = [state["task_results"][task_id] for task_id in sorted(state["task_results"])]
    result_hashes = [item["result_sha256"] for item in task_results]
    return {
        "case": case,
        "configured_worker_count": topology.arm_workers,
        "submitted_task_count": len(futures),
        "terminal_task_count": terminal_count,
        "terminal_before_pool_shutdown_count": terminal_before_pool_shutdown_count,
        "peak_concurrent_worker_count": int(state["peak"]),
        "cancel_request_count": int(state["cancel_request_count"]),
        "consumed_result_count": len(consumed),
        "produced_result_count": len(result_hashes),
        "task_results": task_results,
        "task_result_set_sha256": canonical_sha256(result_hashes),
        "expected_exception_observed": expected_exception_observed,
        "unexpected_worker_exception_count": 0,
        "pool_shutdown_call_count": pool_shutdown_call_count,
        "pool_shutdown_complete": pool_shutdown_complete,
        "mmap_mode": "read_only",
        "mmap_open_count": 1,
        "mmap_close_count": 1,
        "mmap_close_before_terminal_count": mmap_close_before_terminal_count,
        "mmap_use_after_close_count": int(state["mmap_use_after_close_count"]),
        "lifecycle_events": lifecycle_events,
    }


def _build_probe_payload(work_root: Path) -> dict[str, Any]:
    cases = {
        case: _exercise_mmap_case(case, work_root / case)
        for case in ("success", "injected_exception")
    }
    return {
        "schema_version": PROBE_SCHEMA,
        "configured_worker_count": CONFIGURED_WORKER_COUNT,
        "tasks_per_case": TASKS_PER_CASE,
        "fixture_sha256": _fixture_sha256(),
        "cases": cases,
        "economic_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }


def _run_probe_subprocess(work_root: Path, repository_root: Path) -> dict[str, Any]:
    work_root.mkdir(parents=True, exist_ok=True)
    environment = dict(os.environ)
    python_path = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = (
        f"{repository_root}{os.pathsep}{python_path}" if python_path else str(repository_root)
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "internal-probe",
            "--work-root",
            str(work_root),
        ],
        cwd=repository_root,
        env=environment,
        capture_output=True,
        timeout=PROBE_TIMEOUT_SECONDS,
        check=False,
    )
    if completed.returncode != 0:
        if completed.returncode < 0:
            raise DurabilityGateError(
                f"mmap probe terminated by native signal {-completed.returncode}"
            )
        detail = completed.stderr.decode("utf-8", errors="replace")[-500:].strip()
        raise DurabilityGateError(f"mmap probe failed closed: {detail or 'no diagnostic'}")
    try:
        payload = json.loads(completed.stdout.decode("ascii"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DurabilityGateError("mmap probe did not return canonical JSON") from exc
    if not isinstance(payload, dict):
        raise DurabilityGateError("mmap probe payload is not an object")
    payload["subprocess_returncode"] = completed.returncode
    return payload


class _InterruptingFrame(pd.DataFrame):
    @property
    def _constructor(self) -> type[pd.DataFrame]:
        return pd.DataFrame

    def to_parquet(self, path: Any, *args: Any, **kwargs: Any) -> None:
        target = Path(path)
        with target.open("wb") as handle:
            handle.write(b"injected-incomplete-parquet")
            handle.flush()
            os.fsync(handle.fileno())
        raise _InjectedCacheInterruption("injected cache interruption")


class _ObservableSlowFrame(pd.DataFrame):
    _metadata = ["partial_written", "observer_acknowledged"]

    def __init__(
        self,
        *args: Any,
        partial_written: threading.Event,
        observer_acknowledged: threading.Event,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.partial_written = partial_written
        self.observer_acknowledged = observer_acknowledged

    @property
    def _constructor(self) -> type[pd.DataFrame]:
        return pd.DataFrame

    def to_parquet(self, path: Any, *args: Any, **kwargs: Any) -> None:
        target = Path(path)
        with target.open("wb") as handle:
            handle.write(b"observable-incomplete-parquet")
            handle.flush()
            os.fsync(handle.fileno())
        self.partial_written.set()
        if not self.observer_acknowledged.wait(timeout=10.0):
            raise DurabilityGateError("cache observer did not inspect staging before publish")
        pd.DataFrame(self).to_parquet(target, *args, **kwargs)


def _synthetic_cache_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "task_id": np.arange(CONFIGURED_WORKER_COUNT, dtype=np.int64),
            "zero_economic_token_sha256": [
                canonical_sha256({"task_id": task_id, "economic_outcome": None})
                for task_id in range(CONFIGURED_WORKER_COUNT)
            ],
        }
    )


def _cache_key(
    *,
    probe_run_manifest_sha256: str,
    probe_cache_namespace_sha256: str,
    source_hashes: Mapping[str, str],
) -> replay_adapter.DayReplayCacheKey:
    adapter_sha256 = source_hashes[TESTED_SOURCE_RELATIVE_PATHS[-1]]
    return replay_adapter.DayReplayCacheKey(
        adapter_artifact_sha256=adapter_sha256,
        source_manifest_sha256=probe_run_manifest_sha256,
        panel_manifest_sha256=_fixture_sha256(),
        fold_manifest_sha256=canonical_sha256(
            {"scope": "synthetic_zero_economic", "fold": "durability"}
        ),
        execution_manifest_sha256=probe_cache_namespace_sha256,
        exact_owner_policy_sha256=canonical_sha256(
            {"scope": "synthetic_zero_economic", "role": "owner_control"}
        ),
        candidate_policy_sha256=canonical_sha256(
            {"scope": "synthetic_zero_economic", "role": "candidate_disabled"}
        ),
        side="BUY",
        stage="zero_economic_durability",
        fold_id="synthetic_no_economic",
        utc_day="2000-01-01",
        day_input_sha256=_fixture_sha256(),
    )


def _assert_exact_cache_namespace(root: Path, namespace_sha256: str) -> None:
    expected = _require_sha256(namespace_sha256, "cache namespace")
    resolved = root.resolve()
    if resolved.name != expected or root.is_symlink():
        raise DurabilityGateError("cache root escaped the exact execution namespace")
    for path in resolved.rglob("*"):
        if path.is_symlink() or resolved not in path.resolve().parents:
            raise DurabilityGateError("cache payload escaped or redirected its namespace")


def _cache_run_hash(frame: pd.DataFrame, key: replay_adapter.DayReplayCacheKey) -> str:
    return canonical_sha256(
        {
            "cache_key_sha256": key.cache_key_sha256,
            "frame_sha256": replay_adapter._frame_sha256(frame),  # noqa: SLF001
            "economic_outcome": None,
        }
    )


def _exercise_cache_lifecycle(
    *,
    work_root: Path,
    probe_cache_namespace_sha256: str,
    probe_run_manifest_sha256: str,
    source_hashes: Mapping[str, str],
) -> dict[str, Any]:
    key = _cache_key(
        probe_run_manifest_sha256=probe_run_manifest_sha256,
        probe_cache_namespace_sha256=probe_cache_namespace_sha256,
        source_hashes=source_hashes,
    )
    roots = tuple(
        work_root / run_name / probe_cache_namespace_sha256 for run_name in ("run-a", "run-b")
    )
    if any(root.exists() or root.is_symlink() for root in roots):
        raise DurabilityGateError("durability cache run root already exists")
    caches = tuple(replay_adapter.DayReplayCache(root) for root in roots)
    normal_frame = _synthetic_cache_frame()
    evidence = {
        "schema_version": CACHE_PROBE_SCHEMA,
        "probe_cache_namespace_sha256": probe_cache_namespace_sha256,
        "probe_run_manifest_sha256": probe_run_manifest_sha256,
        "synthetic_zero_economic": True,
        "economic_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }

    interruption_observed = False
    try:
        caches[0].admit_sequential(
            key,
            _InterruptingFrame(normal_frame),
            evidence=evidence,
        )
    except _InjectedCacheInterruption:
        interruption_observed = True
    final_a = caches[0]._entry(key)  # noqa: SLF001
    stale_after_interruption = list(caches[0].entries.glob(".*.partial"))
    interrupted_entry_visible = caches[0].load_sequential(key) is not None

    partial_written = threading.Event()
    observer_acknowledged = threading.Event()
    final_complete_observed = threading.Event()
    stop_observer = threading.Event()
    observer_state = {
        "staging_observed": False,
        "public_partial_load_attempt_count": 0,
        "public_partial_load_none_count": 0,
        "public_partial_load_visible_count": 0,
        "public_partial_load_exception_count": 0,
    }

    def observer() -> None:
        while not stop_observer.is_set():
            staging = list(caches[0].entries.glob(".*.partial"))
            if (
                partial_written.is_set()
                and not observer_acknowledged.is_set()
                and staging
                and not final_a.exists()
            ):
                observer_state["staging_observed"] = True
                observer_state["public_partial_load_attempt_count"] += 1
                try:
                    partial_loaded = caches[0].load_sequential(key)
                except BaseException:
                    observer_state["public_partial_load_exception_count"] += 1
                else:
                    if partial_loaded is None:
                        observer_state["public_partial_load_none_count"] += 1
                    else:
                        observer_state["public_partial_load_visible_count"] += 1
                observer_acknowledged.set()
            if final_a.exists():
                try:
                    loaded = caches[0].load_sequential(key)
                except BaseException:
                    observer_state["public_partial_load_exception_count"] += 1
                else:
                    if loaded is not None:
                        final_complete_observed.set()
            time.sleep(0.001)

    observer_thread = threading.Thread(
        target=observer,
        name="f05-buy-e3-cache-observer",
        daemon=False,
    )
    observer_thread.start()
    try:
        caches[0].admit_sequential(
            key,
            _ObservableSlowFrame(
                normal_frame,
                partial_written=partial_written,
                observer_acknowledged=observer_acknowledged,
            ),
            evidence=evidence,
        )
        if not final_complete_observed.wait(timeout=10.0):
            raise DurabilityGateError("cache observer never saw the complete atomic entry")
    finally:
        stop_observer.set()
        observer_thread.join(timeout=10.0)
    if observer_thread.is_alive():
        raise DurabilityGateError("cache observer did not join")

    loaded_a = caches[0].load_sequential(key)
    cache_hit_count = int(loaded_a is not None)
    loaded_a_again = caches[0].load_sequential(key)
    cache_hit_count += int(loaded_a_again is not None)
    caches[1].admit_sequential(key, normal_frame, evidence=evidence)
    loaded_b = caches[1].load_sequential(key)
    if loaded_a is None or loaded_a_again is None or loaded_b is None:
        raise DurabilityGateError("admitted durability cache entry could not be loaded")

    for root in roots:
        _assert_exact_cache_namespace(root, probe_cache_namespace_sha256)
    run_hashes = (_cache_run_hash(loaded_a, key), _cache_run_hash(loaded_b, key))
    remaining_partial = sum(len(list(cache.entries.glob(".*.partial"))) for cache in caches)
    partial_visibility_count = int(observer_state["public_partial_load_visible_count"]) + int(
        observer_state["public_partial_load_exception_count"]
    )
    raw_atomic_observations = {
        "staging_observed_before_publish": bool(observer_state["staging_observed"]),
        "final_complete_observed": final_complete_observed.is_set(),
        "public_partial_load_attempt_count": int(
            observer_state["public_partial_load_attempt_count"]
        ),
        "public_partial_load_none_count": int(observer_state["public_partial_load_none_count"]),
        "public_partial_load_visible_count": int(
            observer_state["public_partial_load_visible_count"]
        ),
        "public_partial_load_exception_count": int(
            observer_state["public_partial_load_exception_count"]
        ),
        "observer_join_failure_count": int(observer_thread.is_alive()),
    }
    cache_measurement = {
        "schema_version": CACHE_PROBE_SCHEMA,
        "probe_cache_namespace_sha256": probe_cache_namespace_sha256,
        "probe_run_manifest_sha256": probe_run_manifest_sha256,
        "cache_key_sha256": key.cache_key_sha256,
        "cache_key_probe_namespace_sha256": key.execution_manifest_sha256,
        "cache_root_namespace_count": len(roots),
        "cache_entry_count": sum(int(cache._manifest(key) is not None) for cache in caches),  # noqa: SLF001
        "cache_hit_count": cache_hit_count,
        "interruption_resume_count": int(interruption_observed),
        "interrupted_entry_visible_count": int(interrupted_entry_visible),
        "stale_partial_after_interruption_count": len(stale_after_interruption),
        "remaining_partial_entry_count": remaining_partial,
        **raw_atomic_observations,
        "partial_cache_visibility_count": partial_visibility_count,
        "atomic_publish_failure_count": -1,
        "repeated_run_count": len(run_hashes),
        "repeated_run_result_sha256s": list(run_hashes),
        "economic_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    cache_measurement["atomic_publish_failure_count"] = (
        stability.derive_atomic_publish_failure_count(cache_measurement)
    )
    return cache_measurement


def _build_run_manifest(
    *,
    tested_source_manifest_sha256: str,
    synthetic_fixture_sha256: str,
) -> dict[str, Any]:
    return stability.durability_probe_run_manifest(
        tested_source_manifest_sha256=tested_source_manifest_sha256,
        synthetic_fixture_sha256=synthetic_fixture_sha256,
    )


def _build_cache_namespace(
    *,
    tested_source_manifest_sha256: str,
    probe_run_manifest_sha256: str,
) -> dict[str, Any]:
    return stability.durability_probe_cache_namespace(
        tested_source_manifest_sha256=tested_source_manifest_sha256,
        probe_run_manifest_sha256=probe_run_manifest_sha256,
    )


def _derive_contract(
    probe: Mapping[str, Any], cache: Mapping[str, Any]
) -> tuple[dict[str, bool], dict[str, int], dict[str, int]]:
    try:
        return stability.durability_measurement_contract(probe, cache)
    except stability.StabilityReceiptError as exc:
        raise DurabilityGateError(str(exc)) from exc


def _validate_measurement_boundaries(probe: Mapping[str, Any], cache: Mapping[str, Any]) -> None:
    for label, payload in (("probe", probe), ("cache", cache)):
        if (
            payload.get("economic_outcomes_read") is not False
            or payload.get("validation_read") is not False
            or payload.get("sealed_holdout_read") is not False
        ):
            raise DurabilityGateError(f"{label} crossed the zero-economic evidence boundary")


def build_measurement_record(
    *,
    work_root: Path,
    repository_root: Path = REPOSITORY_ROOT,
    expected_probe_cache_namespace_sha256: str | None = None,
) -> dict[str, Any]:
    try:
        tested_source_manifest = stability.durability_tested_source_manifest(repository_root)
    except stability.StabilityReceiptError as exc:
        raise DurabilityGateError(str(exc)) from exc
    tested_source_manifest_sha256 = canonical_sha256(tested_source_manifest)
    source_hashes = dict(tested_source_manifest["runtime_sources"])
    run_manifest = _build_run_manifest(
        tested_source_manifest_sha256=tested_source_manifest_sha256,
        synthetic_fixture_sha256=_fixture_sha256(),
    )
    run_manifest_sha256 = canonical_sha256(run_manifest)
    cache_namespace = _build_cache_namespace(
        tested_source_manifest_sha256=tested_source_manifest_sha256,
        probe_run_manifest_sha256=run_manifest_sha256,
    )
    cache_namespace_sha256 = canonical_sha256(cache_namespace)
    if expected_probe_cache_namespace_sha256 is not None:
        expected = _require_sha256(
            expected_probe_cache_namespace_sha256,
            "expected probe cache namespace SHA256",
        )
        if expected != cache_namespace_sha256:
            raise DurabilityGateError(
                "expected probe cache namespace does not match the tested source manifest"
            )

    root = work_root.expanduser().absolute()
    root.mkdir(parents=True, exist_ok=True)
    probe = _run_probe_subprocess(root / "mmap-probe", repository_root.resolve())
    cache = _exercise_cache_lifecycle(
        work_root=root / "cache-probe",
        probe_cache_namespace_sha256=cache_namespace_sha256,
        probe_run_manifest_sha256=run_manifest_sha256,
        source_hashes=source_hashes,
    )
    _validate_measurement_boundaries(probe, cache)
    checks, failures, counts = _derive_contract(probe, cache)
    if set(checks) != set(stability.DURABILITY_CHECKS) or set(failures) != set(
        stability.DURABILITY_FAILURE_COUNTS
    ):
        raise DurabilityGateError("durability wrapper field contract drifted")
    if not all(checks.values()) or any(failures.values()):
        raise DurabilityGateError("durability gate failed closed")

    measurement = {
        "schema_version": MEASUREMENT_SCHEMA,
        "identity": IDENTITY,
        "status": "durability_measurements_complete",
        **counts,
        "checks": checks,
        "failure_counts": failures,
        "tested_source_manifest": tested_source_manifest,
        "tested_source_manifest_sha256": tested_source_manifest_sha256,
        "probe_run_manifest": run_manifest,
        "probe_run_manifest_sha256": run_manifest_sha256,
        "probe_cache_namespace": cache_namespace,
        "probe_cache_namespace_sha256": cache_namespace_sha256,
        "event_series_sha256": "",
        "probe_measurements": probe,
        "cache_measurements": cache,
        "evidence_boundary": EVIDENCE_BOUNDARY,
        "permissions": PERMISSIONS,
        "economic_outcomes_read": False,
        "economic_values_exposed": False,
        "economic_values_used_for_selection": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    measurement["event_series_sha256"] = canonical_sha256(
        stability.durability_event_series(measurement)
    )
    return validate_measurement_record(measurement, repository_root=repository_root)


def validate_measurement_record(
    measurement: Mapping[str, Any],
    *,
    repository_root: Path = REPOSITORY_ROOT,
) -> dict[str, Any]:
    if set(measurement) != MEASUREMENT_FIELDS:
        raise DurabilityGateError("durability measurement fields drifted")
    try:
        return stability.validate_durability_measurement(
            measurement,
            repository_root=repository_root,
        )
    except stability.StabilityReceiptError as exc:
        raise DurabilityGateError(str(exc)) from exc


def _repository_file_map(
    repository_root: Path,
    relative_paths: Sequence[str],
) -> dict[str, str]:
    root = repository_root.expanduser().resolve()
    result: dict[str, str] = {}
    for relative in sorted(set(relative_paths)):
        path = root / relative
        if not path.is_file() or path.is_symlink():
            raise DurabilityGateError(f"harness source is missing or redirected: {relative}")
        result[relative] = _file_sha256(path)
    return result


def _focused_pytest_command(python_executable: Path) -> list[str]:
    return [str(python_executable), "-m", "pytest", "-q", *HARNESS_NODEIDS]


def _run_focused_pytest(
    *,
    repository_root: Path,
    python_executable: Path,
) -> dict[str, int]:
    command = _focused_pytest_command(python_executable)
    environment = dict(os.environ)
    import_paths = [entry for entry in sys.path if entry]
    existing_python_path = environment.get("PYTHONPATH")
    if existing_python_path:
        import_paths.extend(existing_python_path.split(os.pathsep))
    environment["PYTHONPATH"] = os.pathsep.join(dict.fromkeys(import_paths))
    environment["VIRTUAL_ENV"] = sys.prefix
    completed = subprocess.run(
        command,
        cwd=repository_root,
        env=environment,
        capture_output=True,
        check=False,
    )
    stdout = completed.stdout.decode("utf-8", errors="replace")
    summaries = re.findall(r"(?m)^(\d+) passed(?: in [0-9.]+s)?$", stdout.strip())
    passed = int(summaries[-1]) if summaries else -1
    if completed.returncode != 0 or passed != len(HARNESS_NODEIDS):
        detail = (stdout + completed.stderr.decode("utf-8", errors="replace"))[-1000:]
        raise DurabilityGateError(
            f"focused durability pytest failed closed: {detail.strip() or 'no diagnostic'}"
        )
    return {
        "collected": len(HARNESS_NODEIDS),
        "executed": len(HARNESS_NODEIDS),
        "passed": passed,
        "failed": 0,
        "errors": 0,
        "skipped": 0,
        "return_code": completed.returncode,
    }


def _measurement_observations(measurement: Mapping[str, Any]) -> dict[str, int]:
    try:
        return stability.durability_measurement_observations(measurement)
    except stability.StabilityReceiptError as exc:
        raise DurabilityGateError(str(exc)) from exc


def build_harness_receipt(
    *,
    measurement: Mapping[str, Any],
    pytest_counts: Mapping[str, int],
    repository_root: Path = REPOSITORY_ROOT,
    python_executable: Path | None = None,
) -> dict[str, Any]:
    measurement = validate_measurement_record(
        measurement,
        repository_root=repository_root,
    )
    executable = (python_executable or Path(sys.executable)).expanduser().resolve(strict=True)
    test_files = _repository_file_map(repository_root, (HARNESS_TEST_FILE,))
    runtime_sources = _repository_file_map(
        repository_root,
        TESTED_SOURCE_RELATIVE_PATHS,
    )
    counts = {name: int(pytest_counts[name]) for name in stability.DURABILITY_TEST_COUNT_FIELDS}
    if (
        counts["collected"] != len(HARNESS_NODEIDS)
        or counts["executed"] != len(HARNESS_NODEIDS)
        or counts["passed"] != len(HARNESS_NODEIDS)
        or any(counts[name] != 0 for name in ("failed", "errors", "skipped", "return_code"))
    ):
        raise DurabilityGateError("focused pytest counts do not prove all harness gates")
    observations = _measurement_observations(measurement)
    failures = measurement.get("failure_counts")
    if not isinstance(failures, Mapping):
        raise DurabilityGateError("durability failure counts are missing")
    normalized_failures = {
        name: int(failures[name]) for name in stability.DURABILITY_FAILURE_COUNTS
    }
    derived = stability._derived_durability_checks(  # noqa: SLF001
        observations,
        normalized_failures,
    )
    if not all(derived.values()) or any(normalized_failures.values()):
        raise DurabilityGateError("durability observations fail the source wrapper gates")
    source_manifest = {
        "schema_version": stability.DURABILITY_TESTED_SOURCE_MANIFEST_SCHEMA,
        "test_files": test_files,
        "runtime_sources": runtime_sources,
    }
    if source_manifest != measurement["tested_source_manifest"]:
        raise DurabilityGateError("harness tested source manifest drifted")
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": STATUS,
        "python_executable": str(executable),
        "python_file_sha256": _file_sha256(executable),
        "run_command": _focused_pytest_command(executable),
        "nodeids": list(HARNESS_NODEIDS),
        "nodeid_manifest_sha256": canonical_sha256(list(HARNESS_NODEIDS)),
        "gate_nodeids": dict(GATE_NODEIDS),
        "counts": counts,
        "test_files": test_files,
        "runtime_sources": runtime_sources,
        "tested_source_manifest_sha256": canonical_sha256(source_manifest),
        "measurement": dict(measurement),
        "measurement_sha256": canonical_sha256(measurement),
        "observations": observations,
        "failure_counts": normalized_failures,
        "probe_cache_namespace_sha256": measurement["probe_cache_namespace_sha256"],
        "probe_run_manifest_sha256": measurement["probe_run_manifest_sha256"],
        "event_series_sha256": measurement["event_series_sha256"],
        "evidence_boundary": EVIDENCE_BOUNDARY,
        "permissions": PERMISSIONS,
    }
    payload[RECEIPT_CANONICAL_FIELD] = _document_sha256(
        payload,
        RECEIPT_CANONICAL_FIELD,
    )
    return payload


def validate_receipt(
    path: Path,
    *,
    repository_root: Path = REPOSITORY_ROOT,
) -> dict[str, Any]:
    payload = _read_private_receipt(path)
    if set(payload) != RECEIPT_FIELDS:
        raise DurabilityGateError("durability receipt fields drifted")
    observations = payload.get("observations")
    failures = payload.get("failure_counts")
    if not isinstance(observations, Mapping) or not isinstance(failures, Mapping):
        raise DurabilityGateError("durability harness observations are missing")
    checks = stability._derived_durability_checks(observations, failures)  # noqa: SLF001
    if not all(checks.values()) or any(failures.values()):
        raise DurabilityGateError("durability harness observations fail closed")
    try:
        stability._validate_durability_harness_payload(  # noqa: SLF001
            payload,
            repository_root,
        )
    except stability.StabilityReceiptError as exc:
        raise DurabilityGateError("durability harness receipt drifted") from exc
    return payload


def run_gate(
    *,
    output: Path,
    work_root: Path,
    repository_root: Path = REPOSITORY_ROOT,
    expected_probe_cache_namespace_sha256: str | None = None,
) -> dict[str, Any]:
    executable = Path(sys.executable).expanduser().resolve(strict=True)
    pytest_counts = _run_focused_pytest(
        repository_root=repository_root.resolve(),
        python_executable=executable,
    )
    measurement = build_measurement_record(
        work_root=work_root,
        repository_root=repository_root,
        expected_probe_cache_namespace_sha256=expected_probe_cache_namespace_sha256,
    )
    payload = build_harness_receipt(
        measurement=measurement,
        pytest_counts=pytest_counts,
        repository_root=repository_root,
        python_executable=executable,
    )
    _write_private_receipt_no_replace(output, payload)
    return validate_receipt(output, repository_root=repository_root)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    run = subparsers.add_parser("run", help="run the material zero-economic durability gate")
    run.add_argument("--output", type=Path, required=True)
    run.add_argument("--work-root", type=Path, required=True)
    run.add_argument("--expected-probe-cache-namespace-sha256")

    validate = subparsers.add_parser("validate", help="revalidate one private source receipt")
    validate.add_argument("--receipt", type=Path, required=True)

    probe = subparsers.add_parser("internal-probe", help=argparse.SUPPRESS)
    probe.add_argument("--work-root", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "internal-probe":
        payload = _build_probe_payload(args.work_root)
        sys.stdout.buffer.write(_canonical_json_bytes(payload))
        return 0
    if args.command == "validate":
        payload = validate_receipt(args.receipt)
        print(
            json.dumps(
                {
                    "status": payload["status"],
                    "canonical_receipt_sha256": payload[RECEIPT_CANONICAL_FIELD],
                },
                sort_keys=True,
            )
        )
        return 0
    payload = run_gate(
        output=args.output,
        work_root=args.work_root,
        expected_probe_cache_namespace_sha256=(args.expected_probe_cache_namespace_sha256),
    )
    print(
        json.dumps(
            {
                "status": payload["status"],
                "probe_cache_namespace_sha256": payload["probe_cache_namespace_sha256"],
                "canonical_receipt_sha256": payload[RECEIPT_CANONICAL_FIELD],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
