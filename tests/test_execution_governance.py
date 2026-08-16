from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from models.audit import execution_governance as governance


def test_worker_lease_holds_real_slots_and_rejects_nested_pool(tmp_path: Path) -> None:
    with governance.worker_lease(
        run_id="run-a",
        execution_identity="test-executor",
        requested_tokens=2,
        capacity=2,
        root=tmp_path,
    ) as lease:
        assert lease.receipt["state"] == "active"
        assert lease.receipt["slot_ids"] == [0, 1]
        assert os.environ[governance.ACTIVE_LEASE_ENV] == lease.lease_id
        with pytest.raises(governance.ExecutionGovernanceError, match="nested"):
            governance.acquire_worker_lease(
                run_id="run-b",
                execution_identity="nested-executor",
                requested_tokens=1,
                capacity=2,
                root=tmp_path,
            )

    assert lease.receipt["state"] == "released"
    assert governance.ACTIVE_LEASE_ENV not in os.environ
    with governance.worker_lease(
        run_id="run-c",
        execution_identity="next-executor",
        requested_tokens=2,
        capacity=2,
        root=tmp_path,
    ):
        pass


def test_worker_topology_rejects_nested_parallel_pools() -> None:
    assert governance.validate_worker_topology(
        total_worker_tokens=10,
        outer_pool_workers=10,
        nested_pool_workers=0,
    )["nested_parallel_pool"] is False
    with pytest.raises(governance.ExecutionGovernanceError, match="nested"):
        governance.validate_worker_topology(
            total_worker_tokens=10,
            outer_pool_workers=6,
            nested_pool_workers=2,
        )


def test_progress_receipt_reports_real_workers_cache_and_eta(tmp_path: Path) -> None:
    started = datetime.now(UTC) - timedelta(seconds=20)
    payload = governance.build_progress_receipt(
        run_id="formal-v-next",
        execution_identity="formal-executor",
        state="running",
        stages=(
            governance.StageSpec("one_shot"),
            governance.StageSpec("sequential", depends_on=("one_shot",)),
        ),
        current_stage="one_shot",
        stage_progress={
            "one_shot": {
                "total": 4,
                "queued": 0,
                "dispatched": 0,
                "running": 1,
                "completed": 2,
                "failed": 0,
            },
            "sequential": {
                "total": 3,
                "queued": 3,
                "dispatched": 0,
                "running": 0,
                "completed": 0,
                "failed": 0,
            },
        },
        worker_capacity=10,
        requested_worker_tokens=10,
        worker_pids=(os.getpid(),),
        cache_metrics={
            "day_input_mmap": {"hits": 2, "misses": 1, "bytes_read": 4096},
        },
        started_at_utc=started.isoformat(),
        stage_started_at_utc=started.isoformat(),
    )

    assert payload["workers"]["actual_worker_slots"] == 1
    assert payload["cache_metrics"]["day_input_mmap"]["hits"] == 2
    assert payload["timing"]["eta_seconds"] is not None
    assert payload["timing"]["eta_basis"] == "stage_terminal_jobs_over_elapsed_time"
    path = tmp_path / "progress.json"
    governance.write_progress_receipt(path, payload)
    assert governance.load_progress_receipt(path) == payload

    drifted = json.loads(path.read_text(encoding="utf-8"))
    drifted["state"] = "failed"
    path.write_text(json.dumps(drifted), encoding="utf-8")
    with pytest.raises(governance.ExecutionGovernanceError, match="hash"):
        governance.load_progress_receipt(path)


def test_job_progress_summary_does_not_call_dispatched_work_running(
    tmp_path: Path,
) -> None:
    root = tmp_path / "progress"
    root.mkdir()
    started = datetime.now(UTC) - timedelta(seconds=30)
    for index, state in enumerate(("complete", "complete", "running")):
        body = {
            "cache_key": {"stage": "outer_train_one_shot"},
            "state": state,
            "counters": {
                "batch_total_jobs": 4,
                "day_input_mmap_cache_hit": int(index < 2),
                "day_input_mmap_cache_miss": int(index == 2),
            },
            "queued_at_utc": (started + timedelta(seconds=index)).isoformat(),
            "worker_pid": os.getpid() if state == "running" else None,
        }
        (root / f"job-{index}.json").write_text(
            json.dumps(body),
            encoding="utf-8",
        )

    summary = governance.summarize_job_progress(
        root,
        run_id="formal-v-next",
        execution_identity="formal-executor",
        worker_capacity=10,
        requested_worker_tokens=10,
        expected_jobs_by_stage={"outer_train_one_shot": 4},
    )

    assert summary["state"] == "running"
    assert summary["job_totals"] == {
        "total": 4,
        "queued": 0,
        "dispatched": 0,
        "running": 1,
        "completed": 2,
        "failed": 0,
    }
    assert summary["workers"]["actual_worker_slots"] == 1
    assert summary["cache_metrics"]["day_input_mmap"] == {"hits": 2, "misses": 1}
    assert summary["timing"]["eta_seconds"] is not None
