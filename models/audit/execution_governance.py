#!/usr/bin/env python3
"""Host-wide worker leases and common progress receipts for long experiments."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import tempfile
import time
import uuid
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from data_paths import cache_root

WORKER_GOVERNOR_IDENTITY = "narrowgate_host_worker_governor.v1"
WORKER_LEASE_SCHEMA = "narrowgate_host_worker_lease.v1"
PROGRESS_SCHEMA = "narrowgate_execution_progress.v1"
DEFAULT_HOST_WORKER_TOKENS = 10
DEFAULT_GOVERNOR_DIRNAME = "execution_governor_v1"
GOVERNOR_ROOT_ENV = "NARROWGATE_EXECUTION_GOVERNOR_ROOT"
ACTIVE_LEASE_ENV = "NARROWGATE_EXECUTION_WORKER_LEASE_ID"
PROGRESS_STATES = frozenset({"queued", "dispatched", "running", "complete", "failed"})
JOB_STATES = ("queued", "dispatched", "running", "completed", "failed")


class ExecutionGovernanceError(RuntimeError):
    """Raised when a worker lease or progress receipt violates its contract."""


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _document_sha256(value: Mapping[str, Any], field: str) -> str:
    payload = dict(value)
    payload.pop(field, None)
    return _canonical_sha256(payload)


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    destination = Path(path).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=destination.parent,
        prefix=f".{destination.name}.",
        suffix=".tmp",
        delete=False,
    )
    staging = Path(handle.name)
    try:
        with handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(staging, destination)
    finally:
        staging.unlink(missing_ok=True)


def _positive_int(value: object, *, name: str, allow_zero: bool = False) -> int:
    if isinstance(value, bool):
        raise ExecutionGovernanceError(f"{name} cannot be Boolean")
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise ExecutionGovernanceError(f"{name} must be an integer") from exc
    minimum = 0 if allow_zero else 1
    if normalized < minimum:
        raise ExecutionGovernanceError(f"{name} must be >= {minimum}")
    return normalized


def _timestamp(value: str, *, name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError as exc:
        raise ExecutionGovernanceError(f"{name} is not an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise ExecutionGovernanceError(f"{name} must include a timezone")
    return parsed.astimezone(UTC)


def default_governor_root() -> Path:
    configured = os.environ.get(GOVERNOR_ROOT_ENV)
    if configured:
        return Path(configured).expanduser().resolve()
    return (cache_root() / DEFAULT_GOVERNOR_DIRNAME).resolve()


@dataclass(frozen=True, slots=True)
class StageSpec:
    name: str
    depends_on: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.name or not self.name.replace("_", "").replace("-", "").isalnum():
            raise ExecutionGovernanceError("stage name is empty or unsafe")
        if len(self.depends_on) != len(set(self.depends_on)):
            raise ExecutionGovernanceError(f"stage {self.name} repeats a dependency")
        if self.name in self.depends_on:
            raise ExecutionGovernanceError(f"stage {self.name} depends on itself")

    def payload(self) -> dict[str, Any]:
        return {"name": self.name, "depends_on": list(self.depends_on)}


def validate_stage_dag(stages: Sequence[StageSpec]) -> tuple[StageSpec, ...]:
    normalized = tuple(stages)
    if not normalized:
        raise ExecutionGovernanceError("stage DAG must not be empty")
    names = tuple(stage.name for stage in normalized)
    if len(names) != len(set(names)):
        raise ExecutionGovernanceError("stage DAG contains duplicate names")
    known = set(names)
    for stage in normalized:
        missing = sorted(set(stage.depends_on) - known)
        if missing:
            raise ExecutionGovernanceError(
                f"stage {stage.name} has unknown dependencies: {missing}"
            )

    visiting: set[str] = set()
    visited: set[str] = set()
    by_name = {stage.name: stage for stage in normalized}

    def visit(name: str) -> None:
        if name in visited:
            return
        if name in visiting:
            raise ExecutionGovernanceError("stage DAG contains a cycle")
        visiting.add(name)
        for dependency in by_name[name].depends_on:
            visit(dependency)
        visiting.remove(name)
        visited.add(name)

    for name in names:
        visit(name)
    return normalized


@dataclass(slots=True)
class WorkerLease:
    root: Path
    run_id: str
    execution_identity: str
    lease_id: str
    capacity: int
    requested_tokens: int
    slot_ids: tuple[int, ...]
    owner_pid: int
    acquired_at_utc: str
    receipt_path: Path
    _handles: tuple[Any, ...]
    _released: bool = False
    _prior_environment_value: str | None = None

    def _payload(self, *, state: Literal["active", "released"]) -> dict[str, Any]:
        body: dict[str, Any] = {
            "schema_version": WORKER_LEASE_SCHEMA,
            "governor_identity": WORKER_GOVERNOR_IDENTITY,
            "run_id": self.run_id,
            "execution_identity": self.execution_identity,
            "lease_id": self.lease_id,
            "state": state,
            "capacity": self.capacity,
            "requested_tokens": self.requested_tokens,
            "slot_ids": list(self.slot_ids),
            "owner_pid": self.owner_pid,
            "acquired_at_utc": self.acquired_at_utc,
            "released_at_utc": (
                datetime.now(UTC).isoformat() if state == "released" else None
            ),
        }
        body["receipt_sha256"] = _document_sha256(body, "receipt_sha256")
        return body

    @property
    def receipt(self) -> Mapping[str, Any]:
        payload = json.loads(self.receipt_path.read_text(encoding="utf-8"))
        if payload.get("receipt_sha256") != _document_sha256(payload, "receipt_sha256"):
            raise ExecutionGovernanceError("worker lease receipt hash drifted")
        return payload

    def release(self) -> None:
        if self._released:
            return
        _atomic_json(self.receipt_path, self._payload(state="released"))
        for handle in reversed(self._handles):
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            finally:
                handle.close()
        if os.environ.get(ACTIVE_LEASE_ENV) == self.lease_id:
            if self._prior_environment_value is None:
                os.environ.pop(ACTIVE_LEASE_ENV, None)
            else:
                os.environ[ACTIVE_LEASE_ENV] = self._prior_environment_value
        self._released = True

    def __enter__(self) -> WorkerLease:
        return self

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        self.release()


def acquire_worker_lease(
    *,
    run_id: str,
    execution_identity: str,
    requested_tokens: int,
    capacity: int = DEFAULT_HOST_WORKER_TOKENS,
    root: Path | None = None,
    timeout_s: float = 0.0,
    poll_interval_s: float = 0.1,
) -> WorkerLease:
    if not str(run_id).strip() or not str(execution_identity).strip():
        raise ExecutionGovernanceError("worker lease identity is empty")
    total = _positive_int(capacity, name="worker-token capacity")
    requested = _positive_int(requested_tokens, name="requested worker tokens")
    if requested > total:
        raise ExecutionGovernanceError("requested worker tokens exceed host capacity")
    inherited = os.environ.get(ACTIVE_LEASE_ENV)
    if inherited:
        raise ExecutionGovernanceError(
            "nested governed worker pools are forbidden; inherited lease=" + inherited
        )
    try:
        timeout = float(timeout_s)
        poll = float(poll_interval_s)
    except (TypeError, ValueError) as exc:
        raise ExecutionGovernanceError("worker lease timeout is invalid") from exc
    if timeout < 0 or poll <= 0:
        raise ExecutionGovernanceError("worker lease timeout/poll interval is invalid")

    governor_root = Path(root or default_governor_root()).expanduser().resolve()
    slots_root = governor_root / "slots"
    slots_root.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + timeout
    handles: list[Any] = []
    slot_ids: list[int] = []
    while True:
        handles.clear()
        slot_ids.clear()
        for slot_id in range(total):
            path = slots_root / f"slot-{slot_id:03d}.lock"
            handle = path.open("a+b")
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                handle.close()
                continue
            handles.append(handle)
            slot_ids.append(slot_id)
            if len(handles) == requested:
                break
        if len(handles) == requested:
            break
        for handle in reversed(handles):
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()
        if time.monotonic() >= deadline:
            raise ExecutionGovernanceError(
                f"only {len(slot_ids)} of {requested} worker tokens were available"
            )
        time.sleep(min(poll, max(0.0, deadline - time.monotonic())))

    lease_id = uuid.uuid4().hex
    acquired = datetime.now(UTC).isoformat()
    receipt_path = governor_root / "leases" / f"{lease_id}.json"
    prior = os.environ.get(ACTIVE_LEASE_ENV)
    lease = WorkerLease(
        root=governor_root,
        run_id=str(run_id),
        execution_identity=str(execution_identity),
        lease_id=lease_id,
        capacity=total,
        requested_tokens=requested,
        slot_ids=tuple(slot_ids),
        owner_pid=os.getpid(),
        acquired_at_utc=acquired,
        receipt_path=receipt_path,
        _handles=tuple(handles),
        _prior_environment_value=prior,
    )
    try:
        _atomic_json(receipt_path, lease._payload(state="active"))
        os.environ[ACTIVE_LEASE_ENV] = lease_id
    except Exception:
        for handle in reversed(handles):
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()
        raise
    return lease


@contextmanager
def worker_lease(**kwargs: Any) -> Iterator[WorkerLease]:
    lease = acquire_worker_lease(**kwargs)
    try:
        yield lease
    finally:
        lease.release()


def validate_worker_topology(
    *,
    total_worker_tokens: int,
    outer_pool_workers: int,
    nested_pool_workers: int = 0,
) -> Mapping[str, Any]:
    total = _positive_int(total_worker_tokens, name="total worker tokens")
    outer = _positive_int(outer_pool_workers, name="outer pool workers")
    nested = _positive_int(
        nested_pool_workers,
        name="nested pool workers",
        allow_zero=True,
    )
    if outer > total:
        raise ExecutionGovernanceError("outer pool exceeds the worker-token budget")
    if outer > 1 and nested > 1:
        raise ExecutionGovernanceError("nested parallel worker pools are forbidden")
    if nested > total:
        raise ExecutionGovernanceError("nested pool exceeds the worker-token budget")
    return {
        "total_worker_tokens": total,
        "outer_pool_workers": outer,
        "nested_pool_workers": nested,
        "nested_parallel_pool": outer > 1 and nested > 1,
    }


def _normalize_stage_counts(
    stages: Sequence[StageSpec],
    stage_progress: Mapping[str, Mapping[str, int]],
) -> dict[str, dict[str, int]]:
    expected = {stage.name for stage in stages}
    if set(stage_progress) != expected:
        raise ExecutionGovernanceError("stage progress does not match the stage DAG")
    result: dict[str, dict[str, int]] = {}
    for stage in stages:
        raw = stage_progress[stage.name]
        total = _positive_int(raw.get("total", 0), name=f"{stage.name}.total", allow_zero=True)
        counts = {
            name: _positive_int(
                raw.get(name, 0),
                name=f"{stage.name}.{name}",
                allow_zero=True,
            )
            for name in JOB_STATES
        }
        if sum(counts.values()) > total:
            raise ExecutionGovernanceError(f"{stage.name} job-state counts exceed total")
        result[stage.name] = {"total": total, **counts}
    return result


def _normalize_cache_metrics(
    cache_metrics: Mapping[str, Mapping[str, int]] | None,
) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {}
    for cache_name, raw in sorted(dict(cache_metrics or {}).items()):
        if not str(cache_name).strip() or not isinstance(raw, Mapping):
            raise ExecutionGovernanceError("cache metrics are malformed")
        result[str(cache_name)] = {
            str(name): _positive_int(
                value,
                name=f"cache metric {cache_name}.{name}",
                allow_zero=True,
            )
            for name, value in sorted(raw.items())
        }
    return result


def build_progress_receipt(
    *,
    run_id: str,
    execution_identity: str,
    state: Literal["queued", "dispatched", "running", "complete", "failed"],
    stages: Sequence[StageSpec],
    current_stage: str,
    stage_progress: Mapping[str, Mapping[str, int]],
    worker_capacity: int,
    requested_worker_tokens: int,
    worker_pids: Sequence[int] = (),
    cache_metrics: Mapping[str, Mapping[str, int]] | None = None,
    started_at_utc: str,
    stage_started_at_utc: str | None = None,
    observed_at_utc: str | None = None,
    detail: str | None = None,
) -> dict[str, Any]:
    if state not in PROGRESS_STATES:
        raise ExecutionGovernanceError("progress state is invalid")
    if not str(run_id).strip() or not str(execution_identity).strip():
        raise ExecutionGovernanceError("progress identity is empty")
    dag = validate_stage_dag(stages)
    by_stage = _normalize_stage_counts(dag, stage_progress)
    if current_stage not in by_stage:
        raise ExecutionGovernanceError("current stage is absent from the stage DAG")
    capacity = _positive_int(worker_capacity, name="worker capacity")
    requested = _positive_int(requested_worker_tokens, name="requested worker tokens")
    if requested > capacity:
        raise ExecutionGovernanceError("requested worker tokens exceed capacity")
    pids = tuple(sorted({_positive_int(pid, name="worker PID") for pid in worker_pids}))
    if len(pids) > requested:
        raise ExecutionGovernanceError("actual worker PID count exceeds requested tokens")

    started = _timestamp(started_at_utc, name="started_at_utc")
    observed_raw = observed_at_utc or datetime.now(UTC).isoformat()
    observed = _timestamp(observed_raw, name="observed_at_utc")
    if observed < started:
        raise ExecutionGovernanceError("progress observation predates run start")
    stage_started = _timestamp(
        stage_started_at_utc or started_at_utc,
        name="stage_started_at_utc",
    )
    if stage_started < started or stage_started > observed:
        raise ExecutionGovernanceError("current stage start lies outside the run interval")

    aggregate = {
        "total": sum(row["total"] for row in by_stage.values()),
        **{
            name: sum(row[name] for row in by_stage.values())
            for name in JOB_STATES
        },
    }
    current = by_stage[current_stage]
    terminal = current["completed"] + current["failed"]
    remaining = max(0, current["total"] - terminal)
    elapsed_s = max(0.0, (observed - stage_started).total_seconds())
    throughput: float | None = None
    eta_s: float | None = None
    eta_reason = "insufficient_completed_jobs"
    if state == "complete":
        eta_s = 0.0
        eta_reason = "run_complete"
    elif terminal >= 2 and elapsed_s > 0:
        throughput = terminal / elapsed_s
        eta_s = remaining / throughput if throughput > 0 else None
        eta_reason = "stage_terminal_jobs_over_elapsed_time"

    body: dict[str, Any] = {
        "schema_version": PROGRESS_SCHEMA,
        "run_id": str(run_id),
        "execution_identity": str(execution_identity),
        "state": state,
        "detail": detail,
        "stage_dag": [stage.payload() for stage in dag],
        "current_stage": current_stage,
        "stage_progress": by_stage,
        "job_totals": aggregate,
        "workers": {
            "capacity": capacity,
            "requested_tokens": requested,
            "actual_worker_slots": len(pids),
            "worker_pids": list(pids),
        },
        "cache_metrics": _normalize_cache_metrics(cache_metrics),
        "timing": {
            "started_at_utc": started.isoformat(),
            "current_stage_started_at_utc": stage_started.isoformat(),
            "observed_at_utc": observed.isoformat(),
            "current_stage_elapsed_s": elapsed_s,
            "current_stage_throughput_jobs_per_s": throughput,
            "eta_seconds": eta_s,
            "eta_basis": eta_reason,
        },
    }
    body["receipt_sha256"] = _document_sha256(body, "receipt_sha256")
    return body


def write_progress_receipt(path: Path, payload: Mapping[str, Any]) -> None:
    if payload.get("schema_version") != PROGRESS_SCHEMA:
        raise ExecutionGovernanceError("progress receipt schema drifted")
    if payload.get("receipt_sha256") != _document_sha256(payload, "receipt_sha256"):
        raise ExecutionGovernanceError("progress receipt hash drifted")
    _atomic_json(path, payload)


def load_progress_receipt(path: Path) -> Mapping[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ExecutionGovernanceError("progress receipt is unreadable") from exc
    if not isinstance(payload, Mapping) or payload.get("schema_version") != PROGRESS_SCHEMA:
        raise ExecutionGovernanceError("progress receipt schema drifted")
    if payload.get("receipt_sha256") != _document_sha256(payload, "receipt_sha256"):
        raise ExecutionGovernanceError("progress receipt hash drifted")
    return payload


def summarize_job_progress(
    progress_root: Path,
    *,
    run_id: str,
    execution_identity: str,
    worker_capacity: int,
    requested_worker_tokens: int,
    expected_jobs_by_stage: Mapping[str, int] | None = None,
    stage_dependencies: Mapping[str, Sequence[str]] | None = None,
    output_path: Path | None = None,
) -> Mapping[str, Any]:
    root = Path(progress_root).expanduser().resolve()
    receipts: list[Mapping[str, Any]] = []
    for path in sorted(root.glob("*.json")):
        if output_path is not None and path.resolve() == Path(output_path).resolve():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ExecutionGovernanceError(f"job progress is unreadable: {path}") from exc
        if not isinstance(payload, Mapping) or payload.get("state") not in PROGRESS_STATES:
            raise ExecutionGovernanceError(f"job progress is malformed: {path}")
        cache_key = payload.get("cache_key")
        if not isinstance(cache_key, Mapping) or not str(cache_key.get("stage", "")):
            raise ExecutionGovernanceError(f"job progress lacks a stage: {path}")
        receipts.append(payload)
    if not receipts:
        raise ExecutionGovernanceError("no atomic job-progress receipts were found")

    expected = {
        str(name): _positive_int(value, name=f"expected jobs {name}", allow_zero=True)
        for name, value in dict(expected_jobs_by_stage or {}).items()
    }
    observed_stage_names = {str(row["cache_key"]["stage"]) for row in receipts}
    stage_names = tuple(sorted(observed_stage_names | set(expected)))
    dependencies = dict(stage_dependencies or {})
    stages = tuple(
        StageSpec(name=name, depends_on=tuple(str(value) for value in dependencies.get(name, ())))
        for name in stage_names
    )
    stage_progress: dict[str, dict[str, int]] = {}
    worker_pids: set[int] = set()
    cache_metrics: dict[str, dict[str, int]] = {}
    starts: list[datetime] = []
    stage_starts: dict[str, list[datetime]] = {name: [] for name in stage_names}
    for stage in stage_names:
        rows = [row for row in receipts if str(row["cache_key"]["stage"]) == stage]
        counts = {name: 0 for name in JOB_STATES}
        inferred_total = len(rows)
        for row in rows:
            raw_state = str(row["state"])
            state_name = "completed" if raw_state == "complete" else raw_state
            counts[state_name] += 1
            raw_counters = row.get("counters")
            if isinstance(raw_counters, Mapping):
                batch_total = raw_counters.get("batch_total_jobs")
                if batch_total is not None:
                    inferred_total = max(
                        inferred_total,
                        _positive_int(
                            batch_total,
                            name="batch_total_jobs",
                            allow_zero=True,
                        ),
                    )
                for raw_name, raw_value in raw_counters.items():
                    name = str(raw_name)
                    if name.endswith("_cache_hit"):
                        cache_name, metric = name[: -len("_cache_hit")], "hits"
                    elif name.endswith("_cache_miss"):
                        cache_name, metric = name[: -len("_cache_miss")], "misses"
                    elif name.endswith("_cache_bytes_read"):
                        cache_name, metric = name[: -len("_cache_bytes_read")], "bytes_read"
                    elif name.endswith("_cache_bytes_written"):
                        cache_name, metric = name[: -len("_cache_bytes_written")], "bytes_written"
                    else:
                        continue
                    cache_metrics.setdefault(cache_name, {}).setdefault(metric, 0)
                    cache_metrics[cache_name][metric] += _positive_int(
                        raw_value,
                        name=f"cache counter {name}",
                        allow_zero=True,
                    )
            if raw_state == "running" and row.get("worker_pid") is not None:
                worker_pids.add(_positive_int(row["worker_pid"], name="worker PID"))
            queued_at = row.get("queued_at_utc")
            if queued_at:
                parsed = _timestamp(str(queued_at), name="queued_at_utc")
                starts.append(parsed)
                stage_starts[stage].append(parsed)
        total = max(expected.get(stage, 0), inferred_total)
        stage_progress[stage] = {"total": total, **counts}

    current_stage = stage_names[-1]
    for stage in stage_names:
        row = stage_progress[stage]
        if row["completed"] + row["failed"] < row["total"]:
            current_stage = stage
            break
    any_failed = any(row["state"] == "failed" for row in receipts)
    all_terminal = all(
        row["completed"] + row["failed"] >= row["total"]
        for row in stage_progress.values()
    )
    if any_failed:
        state = "failed"
    elif all_terminal:
        state = "complete"
    elif any(row["state"] == "running" for row in receipts):
        state = "running"
    elif any(row["state"] == "dispatched" for row in receipts):
        state = "dispatched"
    else:
        state = "queued"
    started = min(starts) if starts else datetime.now(UTC)
    current_starts = stage_starts.get(current_stage) or [started]
    receipt = build_progress_receipt(
        run_id=run_id,
        execution_identity=execution_identity,
        state=state,
        stages=stages,
        current_stage=current_stage,
        stage_progress=stage_progress,
        worker_capacity=worker_capacity,
        requested_worker_tokens=requested_worker_tokens,
        worker_pids=tuple(worker_pids),
        cache_metrics=cache_metrics,
        started_at_utc=started.isoformat(),
        stage_started_at_utc=min(current_starts).isoformat(),
        detail=f"atomic_job_receipts_scanned={len(receipts)}",
    )
    if output_path is not None:
        write_progress_receipt(output_path, receipt)
    return receipt


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("progress_root", type=Path)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--execution-identity", required=True)
    parser.add_argument("--worker-capacity", type=int, default=DEFAULT_HOST_WORKER_TOKENS)
    parser.add_argument("--requested-worker-tokens", type=int, required=True)
    parser.add_argument("--output", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = summarize_job_progress(
        args.progress_root,
        run_id=args.run_id,
        execution_identity=args.execution_identity,
        worker_capacity=args.worker_capacity,
        requested_worker_tokens=args.requested_worker_tokens,
        output_path=args.output,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
