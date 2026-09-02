from __future__ import annotations

import hashlib
import json
import threading
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import pytest

import execution.order_lifecycle_live_writer_v2 as live_writer_module
from execution.order_lifecycle import (
    OrderLifecyclePhase,
    QuantityWeightedOrderLifecycle,
)
from execution.order_lifecycle_journal_storage_v2 import BOUNDED_REMOTE_SPOOL
from execution.order_lifecycle_live_writer_v2 import OrderLifecycleLiveWriterV2
from strategy.maker_engine import MakerEngine


@dataclass
class _Order:
    client_order_id: str
    symbol: str
    side: object
    lifecycle: QuantityWeightedOrderLifecycle
    order_id: int = 0


def _order() -> _Order:
    return _Order(
        client_order_id="client-1",
        symbol="BTCUSDC",
        side=SimpleNamespace(value="BUY"),
        lifecycle=QuantityWeightedOrderLifecycle(0.001, 1_000_000_000),
    )


def _wait_for(runtime: OrderLifecycleLiveWriterV2, count: int) -> dict:
    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline:
        health = runtime.health_snapshot()
        if int(health["callbacks_processed"]) >= count:
            return health
        time.sleep(0.01)
    raise AssertionError(runtime.health_snapshot())


def test_live_adapter_commits_callbacks_off_thread_and_reports_latency(
    tmp_path: Path,
) -> None:
    order = _order()
    runtime = OrderLifecycleLiveWriterV2(
        tmp_path,
        session_id="epoch-1",
        baseline_epoch_id="epoch-1",
        runtime_identity={"baseline_epoch_id": "epoch-1", "hash": "a" * 64},
        queue_size=8,
        storage_format="jsonl",
        heartbeat_interval_s=0.05,
    )
    assert runtime.enqueue_order_event(order, "submit") is True
    _wait_for(runtime, 1)

    order.order_id = 42
    order.lifecycle.activate(2_200_000_000, exchange_ts_ns=2_000_000_000)
    assert runtime.enqueue_order_event(
        order,
        "rest_ack",
        {
            "_local_receive_ts_ns": 2_200_000_000,
            "_exchange_ts_ns": 2_000_000_000,
        },
    )
    health = _wait_for(runtime, 2)
    assert health["drop_count"] == 0
    assert health["error_count"] == 0
    assert health["rows_committed"] == 2
    assert health["queue_hwm"] >= 1
    assert health["enqueue_latency_p99_us"] >= 0.0
    assert health["write_latency_p99_ms"] >= 0.0
    assert health["process_max_rss_mb"] > 0.0
    final = runtime.close(drain_timeout_s=1.0)
    assert final["formal_collection_valid"] is True


def test_preactivation_gtx_reject_commits_complete_zero_exchange_exposure(
    tmp_path: Path,
) -> None:
    order = _order()
    runtime = OrderLifecycleLiveWriterV2(
        tmp_path,
        session_id="epoch-gtx-reject",
        baseline_epoch_id="epoch-gtx-reject",
        runtime_identity={
            "baseline_epoch_id": "epoch-gtx-reject",
            "hash": "9" * 64,
        },
        queue_size=8,
        storage_format="jsonl",
        heartbeat_interval_s=0.05,
    )
    assert runtime.enqueue_order_event(order, "submit") is True
    _wait_for(runtime, 1)

    order.lifecycle.exchange_terminal(2_000_000_000, reason="rejected")
    assert order.order_id == 0
    assert runtime.enqueue_order_event(
        order,
        "rejected",
        {
            "_local_receive_ts_ns": 2_000_000_000,
            "_reason": "APIError(code=-5022): Post Only order will be rejected",
        },
    )
    health = _wait_for(runtime, 2)
    assert health["drop_count"] == 0
    assert health["error_count"] == 0
    assert health["rows_committed"] == 2
    final = runtime.close(drain_timeout_s=1.0)
    assert final["formal_collection_valid"] is True

    rows = [
        json.loads(line)
        for path in (
            tmp_path / "session-epoch-gtx-reject" / "parts"
        ).glob("*.jsonl")
        for line in path.read_text(encoding="utf-8").splitlines()
        if line
    ]
    rows.sort(key=lambda row: int(row["lifecycle_sequence"]))
    assert [row["lifecycle_event"] for row in rows] == [
        "submit",
        "exchange_terminal",
    ]
    terminal = rows[-1]
    assert terminal["exchange_order_id"] is None
    assert terminal["event_exchange_ts_ns"] is None
    assert terminal["phase_before"] == "SUBMITTED"
    assert terminal["phase_after"] == "EXCHANGE_TERMINAL"
    assert terminal["exchange_terminal_reason"] == "rejected"
    assert terminal["quantity_time_exposure_visible_btc_s"] == 0.0
    assert terminal["visible_exposure_valid"] is True
    assert terminal["visible_exposure_complete"] is True
    assert terminal["quantity_time_exposure_exchange_btc_s"] == 0.0
    assert terminal["exchange_exposure_valid"] is True
    assert terminal["exchange_exposure_complete"] is True


def test_submit_ack_unknown_is_durable_without_quarantine_or_writer_error(
    tmp_path: Path,
) -> None:
    order = _order()
    runtime = OrderLifecycleLiveWriterV2(
        tmp_path,
        session_id="epoch-unknown-ack",
        baseline_epoch_id="epoch-unknown-ack",
        runtime_identity={
            "baseline_epoch_id": "epoch-unknown-ack",
            "hash": "f" * 64,
        },
        queue_size=8,
        storage_format="jsonl",
        heartbeat_interval_s=0.05,
    )
    assert runtime.enqueue_order_event(order, "submit") is True
    _wait_for(runtime, 1)
    order.lifecycle.mark_submit_ack_unknown(
        2_000_000_000,
        reason="submit_response_unknown",
    )
    assert runtime.enqueue_order_event(
        order,
        "submit_ack_unknown",
        {"_local_receive_ts_ns": 2_000_000_000},
    )
    _wait_for(runtime, 2)
    order.lifecycle.censor_submit_ack_unknown(
        3_000_000_000,
        reason="local_shutdown_unknown_ack",
    )
    assert runtime.enqueue_order_event(
        order,
        "submit_ack_unknown_censored",
        {
            "_local_receive_ts_ns": 3_000_000_000,
            "_reason": "local_shutdown_unknown_ack",
        },
    )
    health = _wait_for(runtime, 3)
    assert health["core_health"]["callbacks_quarantined"] == 0
    assert health["drop_count"] == 0
    assert health["error_count"] == 0
    assert health["rows_committed"] == 3
    final = runtime.close(drain_timeout_s=1.0)
    assert final["formal_collection_valid"] is True

    rows = [
        json.loads(line)
        for path in (
            tmp_path / "session-epoch-unknown-ack" / "parts"
        ).glob("*.jsonl")
        for line in path.read_text(encoding="utf-8").splitlines()
        if line
    ]
    rows.sort(key=lambda row: int(row["lifecycle_sequence"]))
    assert [row["lifecycle_event"] for row in rows] == [
        "submit",
        "submit_ack_unknown",
        "submit_ack_unknown_censored",
    ]
    assert rows[-1]["visible_exposure_valid"] is False
    assert rows[-1]["exchange_exposure_valid"] is False


def test_submit_ack_unknown_survives_async_close_restart_in_remote_spool(
    tmp_path: Path,
) -> None:
    order = _order()
    journal_root = tmp_path / "journal"
    epoch_root = tmp_path / "epochs" / "epoch-unknown-restart"
    epoch_root.mkdir(parents=True)
    identity = {
        "baseline_epoch_id": "epoch-unknown-restart",
        "storage_profile": BOUNDED_REMOTE_SPOOL,
        "hash": "9" * 64,
    }

    first = OrderLifecycleLiveWriterV2(
        journal_root,
        session_id="epoch-unknown-restart",
        baseline_epoch_id="epoch-unknown-restart",
        runtime_identity=identity,
        queue_size=8,
        storage_format="jsonl",
        heartbeat_interval_s=0.05,
        storage_profile=BOUNDED_REMOTE_SPOOL,
        epoch_root=epoch_root,
        session_max_duration_s=120.0,
        session_max_bytes=1024 * 1024,
    )
    engine = MakerEngine.__new__(MakerEngine)
    engine._order_lifecycle_live_writer_v2 = first
    engine._order_lifecycle_journal_path = "/must/not/be-written.csv"
    engine._append_row = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        AssertionError("synchronous lifecycle writer was reached")
    )
    engine._record_order_lifecycle_journal(order, "submit", {})
    _wait_for(first, 1)
    order.lifecycle.mark_submit_ack_unknown(
        2_000_000_000,
        reason="submit_response_unknown",
    )
    engine._record_order_lifecycle_journal(
        order,
        "submit_ack_unknown",
        {"_local_receive_ts_ns": 2_000_000_000},
    )
    _wait_for(first, 2)
    first_final = first.close(drain_timeout_s=1.0)
    assert first_final["remote_spool_valid"] is True
    assert first_final["drop_count"] == 0
    assert first_final["error_count"] == 0

    restarted = OrderLifecycleLiveWriterV2(
        journal_root,
        session_id="epoch-unknown-restart",
        baseline_epoch_id="epoch-unknown-restart",
        runtime_identity=identity,
        queue_size=8,
        storage_format="jsonl",
        heartbeat_interval_s=0.05,
        storage_profile=BOUNDED_REMOTE_SPOOL,
        epoch_root=epoch_root,
        session_max_duration_s=120.0,
        session_max_bytes=1024 * 1024,
    )
    assert restarted.health_snapshot()["core_health"]["restart_count"] == 1
    engine._order_lifecycle_live_writer_v2 = restarted
    order.lifecycle.censor_submit_ack_unknown(
        3_000_000_000,
        reason="local_shutdown_unknown_ack",
    )
    engine._record_order_lifecycle_journal(
        order,
        "submit_ack_unknown_censored",
        {
            "_local_receive_ts_ns": 3_000_000_000,
            "_reason": "local_shutdown_unknown_ack",
        },
    )
    _wait_for(restarted, 1)
    restarted_final = restarted.close(drain_timeout_s=1.0)
    assert restarted_final["remote_spool_valid"] is True
    assert restarted_final["drop_count"] == 0
    assert restarted_final["error_count"] == 0
    assert restarted_final["core_health"]["callbacks_quarantined"] == 0

    rows = [
        json.loads(line)
        for path in (
            journal_root / "session-epoch-unknown-restart" / "parts"
        ).glob("*.jsonl")
        for line in path.read_text(encoding="utf-8").splitlines()
        if line
    ]
    rows.sort(key=lambda row: int(row["lifecycle_sequence"]))
    assert [row["lifecycle_event"] for row in rows] == [
        "submit",
        "submit_ack_unknown",
        "submit_ack_unknown_censored",
    ]
    assert len({row["event_id"] for row in rows}) == 3
    assert rows[-1]["terminal_observation"] == "LOCAL_SHUTDOWN_CENSOR"
    assert rows[-1]["visible_exposure_complete"] is False
    assert rows[-1]["exchange_exposure_complete"] is False


def test_worker_failure_invalidates_tape_without_raising_to_producer(
    tmp_path: Path,
) -> None:
    order = _order()
    order.side = SimpleNamespace(value="INVALID")
    runtime = OrderLifecycleLiveWriterV2(
        tmp_path,
        session_id="epoch-bad",
        baseline_epoch_id="epoch-bad",
        runtime_identity={"baseline_epoch_id": "epoch-bad", "hash": "b" * 64},
        queue_size=4,
        storage_format="jsonl",
        heartbeat_interval_s=0.05,
    )
    assert runtime.enqueue_order_event(order, "submit") is True
    deadline = time.monotonic() + 3.0
    while runtime.health_snapshot()["error_count"] == 0 and time.monotonic() < deadline:
        time.sleep(0.01)
    health = runtime.health_snapshot()
    assert health["error_count"] >= 1
    assert health["formal_collection_valid"] is False
    assert "lifecycle_id=epoch-bad:client-1" in health["last_error"]
    assert "client_order_id=client-1" in health["last_error"]
    runtime.close(drain_timeout_s=1.0)


def test_invalid_callback_still_fails_on_producer_with_drop_and_error(
    tmp_path: Path,
) -> None:
    order = _order()
    runtime = OrderLifecycleLiveWriterV2(
        tmp_path,
        session_id="epoch-invalid-callback",
        baseline_epoch_id="epoch-invalid-callback",
        runtime_identity={
            "baseline_epoch_id": "epoch-invalid-callback",
            "hash": "5" * 64,
        },
        queue_size=4,
        storage_format="jsonl",
        heartbeat_interval_s=0.05,
    )
    assert runtime.enqueue_order_event(order, "   ") is False
    health = runtime.health_snapshot()
    assert health["callbacks_enqueued"] == 0
    assert health["drop_count"] == 1
    assert health["error_count"] == 1
    assert "source callback type is required" in health["last_error"]
    assert "lifecycle_id=epoch-invalid-callback:client-1" in health["last_error"]
    final = runtime.close(drain_timeout_s=1.0)
    assert final["formal_collection_valid"] is False


def test_callback_materialization_runs_on_worker_and_preserves_identity(
    tmp_path: Path,
) -> None:
    order = _order()
    runtime = OrderLifecycleLiveWriterV2(
        tmp_path,
        session_id="epoch-worker",
        baseline_epoch_id="epoch-worker",
        runtime_identity={"baseline_epoch_id": "epoch-worker", "hash": "2" * 64},
        queue_size=4,
        storage_format="jsonl",
        heartbeat_interval_s=0.05,
    )
    producer_thread_id = threading.get_ident()
    worker_thread_ids: list[int] = []
    entered = threading.Event()
    release = threading.Event()
    original = runtime._prepare_work_item

    def blocked_prepare(item):
        worker_thread_ids.append(threading.get_ident())
        entered.set()
        assert release.wait(timeout=2.0)
        return original(item)

    runtime._prepare_work_item = blocked_prepare
    assert runtime.enqueue_order_event(order, "submit") is True
    assert entered.wait(timeout=1.0)
    assert worker_thread_ids == [runtime._thread.ident]
    assert worker_thread_ids[0] != producer_thread_id
    assert runtime.health_snapshot()["callbacks_processed"] == 0
    release.set()
    _wait_for(runtime, 1)
    order.order_id = 42
    order.lifecycle.activate(2_200_000_000, exchange_ts_ns=2_000_000_000)
    assert runtime.enqueue_order_event(
        order,
        "rest_ack",
        {
            "_local_receive_ts_ns": 2_200_000_000,
            "_exchange_ts_ns": 2_000_000_000,
        },
    )
    _wait_for(runtime, 2)
    final = runtime.close(drain_timeout_s=1.0)
    assert final["formal_collection_valid"] is True

    rows = [
        json.loads(line)
        for path in (tmp_path / "session-epoch-worker" / "parts").glob("*.jsonl")
        for line in path.read_text(encoding="utf-8").splitlines()
        if line
    ]
    expected_payloads = {
        b"epoch-worker|client-1|1|submit|1000000000|0",
        b"epoch-worker|client-1|2|rest_ack|2200000000|2000000000",
    }
    expected_callback_ids = {hashlib.sha256(payload).hexdigest() for payload in expected_payloads}
    assert {row["source_callback_id"] for row in rows} == expected_callback_ids
    assert {row["source_callback_type"] for row in rows} == {"submit", "rest_ack"}


@pytest.mark.parametrize(
    ("mutate", "expected_reason"),
    [
        (
            lambda snapshot: setattr(
                snapshot,
                "phase",
                OrderLifecyclePhase.PARTIALLY_FILLED,
            ),
            "phase_mismatch",
        ),
        (
            lambda snapshot: setattr(
                snapshot,
                "remaining_quantity",
                snapshot.remaining_quantity / 2.0,
            ),
            "remaining_quantity_mismatch",
        ),
        (
            lambda snapshot: setattr(
                snapshot,
                "quantity_time_exposure_btc_s",
                snapshot.quantity_time_exposure_btc_s + 1.0,
            ),
            "visible_exposure_mismatch",
        ),
        (
            lambda snapshot: setattr(
                snapshot,
                "quantity_time_exposure_exchange_accumulated_btc_s",
                snapshot.quantity_time_exposure_exchange_accumulated_btc_s + 1.0,
            ),
            "exchange_exposure_mismatch",
        ),
        (
            lambda snapshot: setattr(snapshot, "exchange_exposure_valid", False),
            "exchange_valid_mismatch",
        ),
    ],
)
def test_snapshot_consistency_checks_latest_event_state(
    mutate,
    expected_reason: str,
) -> None:
    order = _order()
    order.lifecycle.activate(2_200_000_000, exchange_ts_ns=2_000_000_000)
    snapshot = order.lifecycle.journal_snapshot()
    mutate(snapshot)
    assert live_writer_module._snapshot_inconsistency_reason(snapshot) == expected_reason


def test_incremental_latency_summary_preserves_exact_rolling_percentiles() -> None:
    history = deque(maxlen=4)
    ordered = []
    live_writer_module._extend_rolling_ordered(
        history,
        ordered,
        (10, 20, 30, 40),
    )
    assert live_writer_module._latency_summary(ordered, scale=1.0) == (
        20.0,
        40.0,
        40.0,
    )

    live_writer_module._extend_rolling_ordered(history, ordered, (50, 5))
    assert tuple(history) == (30, 40, 50, 5)
    assert ordered == [5, 30, 40, 50]
    assert live_writer_module._latency_summary(ordered, scale=1.0) == (
        30.0,
        50.0,
        50.0,
    )


def test_snapshot_capture_retries_once_then_enqueues_consistent_copy(
    tmp_path: Path,
    monkeypatch,
) -> None:
    order = _order()
    original = order.lifecycle.journal_snapshot
    calls = 0

    def racing_snapshot():
        nonlocal calls
        calls += 1
        snapshot = original()
        if calls == 1:
            snapshot.remaining_quantity /= 2.0
        return snapshot

    monkeypatch.setattr(order.lifecycle, "journal_snapshot", racing_snapshot)
    runtime = OrderLifecycleLiveWriterV2(
        tmp_path,
        session_id="epoch-retry",
        baseline_epoch_id="epoch-retry",
        runtime_identity={"baseline_epoch_id": "epoch-retry", "hash": "3" * 64},
        queue_size=4,
        storage_format="jsonl",
        heartbeat_interval_s=0.05,
    )
    assert runtime.enqueue_order_event(order, "submit") is True
    assert calls == 2
    health = _wait_for(runtime, 1)
    assert health["drop_count"] == 0
    assert health["error_count"] == 0
    final = runtime.close(drain_timeout_s=1.0)
    assert final["formal_collection_valid"] is True


def test_snapshot_capture_permanent_race_fails_closed_with_identity(
    tmp_path: Path,
    monkeypatch,
) -> None:
    order = _order()
    original = order.lifecycle.journal_snapshot
    calls = 0

    def always_inconsistent_snapshot():
        nonlocal calls
        calls += 1
        snapshot = original()
        snapshot.quantity_time_exposure_btc_s += 1.0
        return snapshot

    monkeypatch.setattr(
        order.lifecycle,
        "journal_snapshot",
        always_inconsistent_snapshot,
    )
    runtime = OrderLifecycleLiveWriterV2(
        tmp_path,
        session_id="epoch-race",
        baseline_epoch_id="epoch-race",
        runtime_identity={"baseline_epoch_id": "epoch-race", "hash": "4" * 64},
        queue_size=4,
        storage_format="jsonl",
        heartbeat_interval_s=0.05,
    )
    assert runtime.enqueue_order_event(order, "partial_fill") is False
    assert calls == live_writer_module._SNAPSHOT_CAPTURE_MAX_ATTEMPTS
    health = runtime.health_snapshot()
    assert health["callbacks_enqueued"] == 0
    assert health["drop_count"] == 1
    assert health["error_count"] == 1
    assert "lifecycle_snapshot_inconsistent" in health["last_error"]
    assert "lifecycle_id=epoch-race:client-1" in health["last_error"]
    assert "client_order_id=client-1" in health["last_error"]
    assert "last_reason=visible_exposure_mismatch" in health["last_error"]
    final = runtime.close(drain_timeout_s=1.0)
    assert final["formal_collection_valid"] is False


def test_bounded_producer_does_not_wait_for_worker_metrics_lock(
    tmp_path: Path,
) -> None:
    order = _order()
    epoch_root = tmp_path / "epochs" / "epoch-contention"
    epoch_root.mkdir(parents=True)
    runtime = OrderLifecycleLiveWriterV2(
        tmp_path / "journal",
        session_id="epoch-contention",
        baseline_epoch_id="epoch-contention",
        runtime_identity={
            "baseline_epoch_id": "epoch-contention",
            "storage_profile": BOUNDED_REMOTE_SPOOL,
            "hash": "7" * 64,
        },
        queue_size=8,
        storage_format="jsonl",
        heartbeat_interval_s=60.0,
        storage_profile=BOUNDED_REMOTE_SPOOL,
        epoch_root=epoch_root,
        session_max_duration_s=120.0,
        session_max_bytes=1024 * 1024,
    )
    lock_held = threading.Event()
    release_lock = threading.Event()

    def hold_worker_metrics_lock() -> None:
        with runtime._metrics_lock:
            lock_held.set()
            assert release_lock.wait(timeout=2.0)

    holder = threading.Thread(target=hold_worker_metrics_lock)
    holder.start()
    assert lock_held.wait(timeout=1.0)

    result: list[bool] = []
    producer_done = threading.Event()

    def produce() -> None:
        result.append(runtime.enqueue_order_event(order, "submit"))
        producer_done.set()

    producer = threading.Thread(target=produce)
    producer.start()
    assert producer_done.wait(timeout=0.2), "producer waited for worker/health metrics"
    assert result == [True]

    release_lock.set()
    holder.join(timeout=1.0)
    producer.join(timeout=1.0)
    _wait_for(runtime, 1)
    final = runtime.close(drain_timeout_s=1.0)
    assert final["drop_count"] == 0
    assert final["error_count"] == 0


def test_queued_snapshot_does_not_include_later_live_transition(
    tmp_path: Path,
) -> None:
    order = _order()
    runtime = OrderLifecycleLiveWriterV2(
        tmp_path,
        session_id="epoch-causal-snapshot",
        baseline_epoch_id="epoch-causal-snapshot",
        runtime_identity={
            "baseline_epoch_id": "epoch-causal-snapshot",
            "hash": "8" * 64,
        },
        queue_size=8,
        storage_format="jsonl",
        heartbeat_interval_s=60.0,
    )
    assert runtime.enqueue_order_event(order, "submit") is True
    _wait_for(runtime, 1)

    entered = threading.Event()
    release = threading.Event()
    original = runtime._prepare_work_item

    def blocked_prepare(item):
        entered.set()
        assert release.wait(timeout=2.0)
        return original(item)

    runtime._prepare_work_item = blocked_prepare
    order.order_id = 42
    order.lifecycle.activate(2_200_000_000, exchange_ts_ns=2_000_000_000)
    assert runtime.enqueue_order_event(
        order,
        "rest_ack",
        {
            "_local_receive_ts_ns": 2_200_000_000,
            "_exchange_ts_ns": 2_000_000_000,
        },
    )
    assert entered.wait(timeout=1.0)

    order.lifecycle.request_cancel(2_300_000_000)
    release.set()
    _wait_for(runtime, 2)
    runtime._prepare_work_item = original
    assert runtime.enqueue_order_event(
        order,
        "cancel_request",
        {"_local_receive_ts_ns": 2_300_000_000},
    )
    _wait_for(runtime, 3)
    final = runtime.close(drain_timeout_s=1.0)
    assert final["formal_collection_valid"] is True

    rows = [
        json.loads(line)
        for path in (tmp_path / "session-epoch-causal-snapshot" / "parts").glob("*.jsonl")
        for line in path.read_text(encoding="utf-8").splitlines()
        if line
    ]
    events_by_callback = {
        callback: [
            row["lifecycle_event"] for row in rows if row["source_callback_type"] == callback
        ]
        for callback in {row["source_callback_type"] for row in rows}
    }
    assert events_by_callback == {
        "submit": ["submit"],
        "rest_ack": ["activate"],
        "cancel_request": ["cancel_request"],
    }


def test_frozen_callback_can_cross_central_fifo_without_state_drift(
    tmp_path: Path,
) -> None:
    order = _order()
    runtime = OrderLifecycleLiveWriterV2(
        tmp_path,
        session_id="epoch-central-fifo",
        baseline_epoch_id="epoch-central-fifo",
        runtime_identity={
            "baseline_epoch_id": "epoch-central-fifo",
            "hash": "c" * 64,
        },
        queue_size=8,
        storage_format="jsonl",
        heartbeat_interval_s=60.0,
    )

    frozen = runtime.freeze_order_event(order, "submit")
    assert frozen is not None
    order.order_id = 42
    order.lifecycle.activate(2_200_000_000, exchange_ts_ns=2_000_000_000)

    assert runtime.enqueue_frozen_order_event(frozen) is True
    _wait_for(runtime, 1)
    final = runtime.close(drain_timeout_s=1.0)
    assert final["formal_collection_valid"] is True

    rows = [
        json.loads(line)
        for path in (tmp_path / "session-epoch-central-fifo" / "parts").glob(
            "*.jsonl"
        )
        for line in path.read_text(encoding="utf-8").splitlines()
        if line
    ]
    assert len(rows) == 1
    assert rows[0]["source_callback_type"] == "submit"
    assert rows[0]["lifecycle_event"] == "submit"
    assert rows[0]["exchange_order_id"] is None


def test_bounded_remote_spool_never_claims_local_admission_and_stops_cleanly(
    tmp_path: Path,
) -> None:
    order = _order()
    epoch_root = tmp_path / "epochs" / "epoch-remote"
    epoch_root.mkdir(parents=True)
    runtime = OrderLifecycleLiveWriterV2(
        tmp_path / "journal",
        session_id="epoch-remote",
        baseline_epoch_id="epoch-remote",
        runtime_identity={
            "baseline_epoch_id": "epoch-remote",
            "storage_profile": BOUNDED_REMOTE_SPOOL,
            "hash": "f" * 64,
        },
        queue_size=4,
        storage_format="jsonl",
        heartbeat_interval_s=0.01,
        storage_profile=BOUNDED_REMOTE_SPOOL,
        epoch_root=epoch_root,
        session_max_duration_s=120.0,
        session_max_bytes=1024 * 1024,
    )
    try:
        assert runtime.enqueue_order_event(order, "submit") is True
        _wait_for(runtime, 1)
        # Advance the deadline deterministically.  A 20 ms constructor-to-enqueue
        # deadline races slow CI hosts and can leave the heartbeat thread alive
        # when the first assertion fails.
        runtime._session_max_duration_s = 0.02
        runtime._collection_deadline_monotonic_ns = time.monotonic_ns() - 1
        assert runtime.enqueue_order_event(order, "after-bound") is False
    finally:
        final = runtime.close(drain_timeout_s=1.0)
    assert final["state"] == "closed"
    assert final["storage_profile"] == BOUNDED_REMOTE_SPOOL
    assert final["collection_bound_reason"] == "max_duration_reached"
    assert final["callbacks_ignored_after_bound"] == 1
    assert final["remote_spool_valid"] is True
    assert final["formal_collection_valid"] is False
    assert final["local_admission_complete"] is False
    assert (
        final["formal_collection_valid_reason"]
        == "remote_spool_requires_rsync_and_local_orico_admission"
    )


def test_remote_spool_tree_is_scanned_on_heartbeat_not_each_callback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    import execution.order_lifecycle_live_writer_v2 as live_writer_module

    scans = 0
    tracked_roots: set[Path] = set()
    original_tree_size = live_writer_module._tree_size_bytes

    def counted_tree_size(path: Path) -> int:
        nonlocal scans
        if path.resolve() in tracked_roots:
            scans += 1
        return original_tree_size(path)

    monkeypatch.setattr(live_writer_module, "_tree_size_bytes", counted_tree_size)
    order = _order()
    epoch_root = tmp_path / "epochs" / "epoch-scan"
    epoch_root.mkdir(parents=True)
    runtime = OrderLifecycleLiveWriterV2(
        tmp_path / "journal",
        session_id="epoch-scan",
        baseline_epoch_id="epoch-scan",
        runtime_identity={
            "baseline_epoch_id": "epoch-scan",
            "storage_profile": BOUNDED_REMOTE_SPOOL,
            "hash": "1" * 64,
        },
        queue_size=8,
        storage_format="jsonl",
        heartbeat_interval_s=60.0,
        storage_profile=BOUNDED_REMOTE_SPOOL,
        epoch_root=epoch_root,
        session_max_duration_s=120.0,
        session_max_bytes=1024 * 1024,
    )
    tracked_roots.update((runtime._writer.session_root.resolve(), epoch_root.resolve()))
    try:
        assert runtime.enqueue_order_event(order, "submit") is True
        _wait_for(runtime, 1)
        order.order_id = 42
        order.lifecycle.activate(2_200_000_000, exchange_ts_ns=2_000_000_000)
        assert runtime.enqueue_order_event(order, "rest_ack") is True
        _wait_for(runtime, 2)

        assert scans == 0
    finally:
        runtime.close(drain_timeout_s=1.0)
    assert scans == 2


def test_enqueue_does_not_wait_for_slow_writer(tmp_path: Path) -> None:
    order = _order()
    runtime = OrderLifecycleLiveWriterV2(
        tmp_path,
        session_id="epoch-slow",
        baseline_epoch_id="epoch-slow",
        runtime_identity={"baseline_epoch_id": "epoch-slow", "hash": "c" * 64},
        queue_size=4,
        storage_format="jsonl",
        heartbeat_interval_s=0.05,
    )
    original = runtime._bridge.submit_callback

    def slow_submit_callback(**kwargs):
        time.sleep(0.2)
        return original(**kwargs)

    runtime._bridge.submit_callback = slow_submit_callback
    started = time.perf_counter()
    assert runtime.enqueue_order_event(order, "submit") is True
    elapsed = time.perf_counter() - started
    assert elapsed < 0.05
    _wait_for(runtime, 1)
    runtime.close(drain_timeout_s=1.0)


def test_bounded_queue_drop_invalidates_tape_without_blocking(tmp_path: Path) -> None:
    order = _order()
    runtime = OrderLifecycleLiveWriterV2(
        tmp_path,
        session_id="epoch-full",
        baseline_epoch_id="epoch-full",
        runtime_identity={"baseline_epoch_id": "epoch-full", "hash": "d" * 64},
        queue_size=1,
        storage_format="jsonl",
        heartbeat_interval_s=0.05,
    )
    entered = threading.Event()
    release = threading.Event()
    original = runtime._bridge.submit_callback

    def blocked_submit_callback(**kwargs):
        entered.set()
        assert release.wait(timeout=2.0)
        return original(**kwargs)

    runtime._bridge.submit_callback = blocked_submit_callback
    assert runtime.enqueue_order_event(order, "submit") is True
    assert entered.wait(timeout=1.0)
    assert runtime.enqueue_order_event(order, "duplicate_submit_callback") is True
    started = time.perf_counter()
    assert runtime.enqueue_order_event(order, "queue_full_callback") is False
    assert time.perf_counter() - started < 0.05
    health = runtime.health_snapshot()
    assert health["drop_count"] == 1
    assert health["formal_collection_valid"] is False
    release.set()
    _wait_for(runtime, 2)
    runtime.close(drain_timeout_s=1.0)


def test_local_shutdown_is_persisted_as_censor_not_exchange_terminal(
    tmp_path: Path,
) -> None:
    order = _order()
    runtime = OrderLifecycleLiveWriterV2(
        tmp_path,
        session_id="epoch-shutdown",
        baseline_epoch_id="epoch-shutdown",
        runtime_identity={"baseline_epoch_id": "epoch-shutdown", "hash": "e" * 64},
        queue_size=4,
        storage_format="jsonl",
        heartbeat_interval_s=0.05,
    )
    assert runtime.enqueue_order_event(order, "submit") is True
    _wait_for(runtime, 1)
    order.order_id = 42
    order.lifecycle.activate(2_200_000_000, exchange_ts_ns=2_000_000_000)
    assert runtime.enqueue_order_event(
        order,
        "rest_ack",
        {
            "_local_receive_ts_ns": 2_200_000_000,
            "_exchange_ts_ns": 2_000_000_000,
        },
    )
    _wait_for(runtime, 2)
    order.lifecycle.local_shutdown_censor(3_000_000_000)
    assert runtime.enqueue_order_event(
        order,
        "local_shutdown_cancel",
        {"_local_receive_ts_ns": 3_000_000_000},
    )
    health = _wait_for(runtime, 3)
    assert health["error_count"] == 0
    final = runtime.close(drain_timeout_s=1.0)
    assert final["formal_collection_valid"] is True
    rows = [
        line
        for path in (tmp_path / "session-epoch-shutdown" / "parts").glob("*.jsonl")
        for line in path.read_text(encoding="utf-8").splitlines()
        if line
    ]
    assert any('"lifecycle_event":"local_shutdown_censor"' in line for line in rows)
    assert not any(
        '"event_reason":"local_shutdown_cancel","observation_origin"' in line
        and '"lifecycle_event":"exchange_terminal"' in line
        for line in rows
    )


def test_maker_v2_hook_bypasses_synchronous_csv_writer() -> None:
    order = _order()
    calls = []
    frozen = object()
    runtime = SimpleNamespace(
        freeze_order_event=lambda *args: calls.append(("freeze", args)) or frozen,
        enqueue_frozen_order_event=lambda item: calls.append(("enqueue", item)) or True,
    )
    engine = MakerEngine.__new__(MakerEngine)
    engine._order_lifecycle_live_writer_v2 = runtime
    engine._runtime_evidence_writer = None
    engine._order_lifecycle_journal_path = "/must/not/be/written.csv"
    engine._append_row = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        AssertionError("synchronous CSV writer was reached")
    )

    engine._record_order_lifecycle_journal(order, "submit", {})
    assert calls == [
        ("freeze", (order, "submit", {})),
        ("enqueue", frozen),
    ]


def test_direct_frozen_commit_bypasses_secondary_queue_and_drains(tmp_path: Path) -> None:
    order = _order()
    runtime = OrderLifecycleLiveWriterV2(
        tmp_path,
        session_id="direct-fifo",
        baseline_epoch_id="epoch-direct",
        runtime_identity={"baseline_epoch_id": "epoch-direct", "hash": "a" * 64},
        queue_size=1,
        storage_format="jsonl",
        heartbeat_interval_s=0.05,
    )
    frozen = runtime.freeze_order_event(order, "submit", {})
    assert frozen is not None
    assert runtime.commit_frozen_order_event(frozen) is True
    assert runtime._queue.qsize() == 0

    health = runtime.close(drain_timeout_s=1.0)
    assert health["callbacks_enqueued"] == 1
    assert health["callbacks_processed"] == 1
    assert health["drop_count"] == 0
    assert health["formal_collection_valid"] is True


def test_lifecycle_submission_owner_cannot_mix_queue_and_external_fifo(
    tmp_path: Path,
) -> None:
    order = _order()
    runtime = OrderLifecycleLiveWriterV2(
        tmp_path,
        session_id="owner-latch",
        baseline_epoch_id="epoch-owner",
        runtime_identity={"baseline_epoch_id": "epoch-owner", "hash": "b" * 64},
        storage_format="jsonl",
    )
    frozen = runtime.freeze_order_event(order, "submit", {})
    assert frozen is not None
    assert runtime.commit_frozen_order_event(frozen) is True
    assert runtime.enqueue_frozen_order_event(frozen) is False
    health = runtime.close()
    assert health["error_count"] == 1
    assert health["formal_collection_valid"] is False


def test_maker_publishes_rest_reconciled_lifecycle_transition() -> None:
    order = _order()
    order.lifecycle.activate(2_200_000_000, exchange_ts_ns=2_000_000_000)
    order.lifecycle.request_cancel(2_300_000_000)
    order.lifecycle.cancel_rejected(2_400_000_000)
    calls = []
    engine = MakerEngine.__new__(MakerEngine)
    engine.orders = SimpleNamespace(get_order=lambda _cid: order)
    engine._record_exact_order_event = lambda *args, **kwargs: calls.append((args, kwargs))

    engine.record_reconciled_order_lifecycle(
        order.client_order_id,
        "cancel_rejected_reconciled",
    )

    assert len(calls) == 1
    args, _ = calls[0]
    assert args[0] is order
    assert args[1] == "cancel_rejected_reconciled"
    assert args[2]["_local_receive_ts_ns"] == 2_400_000_000
