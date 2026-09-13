from __future__ import annotations

from pathlib import Path

import pytest

from execution.order_lifecycle import QuantityWeightedOrderLifecycle
from execution.order_lifecycle_journal_v2_strict_native import (
    OrderLifecycleJournalV2SourceCallback,
)
from execution.order_lifecycle_journal_writer_v2_replay_single_owner import (
    REPLAY_WRITER_ID,
    SingleOwnerReplayJournalWriterV2,
)
from execution.order_lifecycle_journal_writer_v2_strict_native import (
    OrderLifecycleJournalRuntimeBridgeV2,
)
from execution.order_lifecycle_journal_writer_v2_strict_native import (
    OrderLifecycleJournalWriterV2 as StrictNativeJournalWriterV2,
)


def test_single_owner_replay_commits_without_repeated_disk_recovery(tmp_path: Path) -> None:
    writer = SingleOwnerReplayJournalWriterV2(
        tmp_path,
        session_id="replay-day",
        runtime_identity={
            "identity": REPLAY_WRITER_ID,
            "economic_outcomes_read": False,
        },
        storage_format="parquet",
        initial_active_order_ids=(),
        heartbeat_interval_s=60.0,
        start_heartbeat=False,
    )
    bridge = OrderLifecycleJournalRuntimeBridgeV2(writer)
    bridge.register_lifecycle(
        lifecycle_id="day:order-1",
        runtime_source="authoritative_python_replay",
        client_order_id="order-1",
        exchange_order_id="exchange-1",
        symbol="BTCUSDC",
        side="BUY",
    )
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)

    # Construction already performed the full recovery. Any later disk scan is
    # a regression back to quadratic replay behavior.
    writer._recover_locked = lambda: (_ for _ in ()).throw(AssertionError("disk rescan"))

    submit = bridge.submit_callback(
        lifecycle_id="day:order-1",
        lifecycle=lifecycle,
        callback=OrderLifecycleJournalV2SourceCallback(
            callback_id="submit",
            callback_type="order_submit",
            received_ts_ns=1_000_000_000,
            simulator_queue_source="pending_activation",
            exact_queue_path_valid=False,
        ),
    )
    assert submit.status == "committed"

    lifecycle.activate(1_200_000_000, exchange_ts_ns=1_100_000_000)
    activation = bridge.submit_callback(
        lifecycle_id="day:order-1",
        lifecycle=lifecycle,
        callback=OrderLifecycleJournalV2SourceCallback(
            callback_id="activate",
            callback_type="order_activation",
            received_ts_ns=1_200_000_000,
            exchange_ts_ns=1_100_000_000,
            simulator_queue_source="native_exchange_book",
            exact_queue_path_valid=True,
        ),
    )
    assert activation.status == "committed"

    health = writer.close()
    assert health["callbacks_committed"] == 2
    assert health["rows_committed"] == 2
    assert health["rows_dropped"] == 0
    assert health["error_count"] == 0
    assert health["formal_collection_valid"] is True
    assert len(list((tmp_path / "session-replay-day" / "parts").glob("*.parquet"))) == 2


def test_single_owner_replay_requires_held_process_lock(tmp_path: Path) -> None:
    writer = SingleOwnerReplayJournalWriterV2(
        tmp_path,
        session_id="replay-day",
        runtime_identity={"identity": REPLAY_WRITER_ID},
        start_heartbeat=False,
    )
    writer._release_process_lock()
    try:
        try:
            writer.reconcile()
        except RuntimeError as exc:
            assert "lost its process lock" in str(exc)
        else:
            raise AssertionError("missing process lock did not fail closed")
    finally:
        writer._closed = True


def test_strict_native_normal_callbacks_do_not_rescan_disk(tmp_path: Path) -> None:
    writer = StrictNativeJournalWriterV2(
        tmp_path,
        session_id="strict-day",
        runtime_identity={"identity": "strict-normal-callback"},
        start_heartbeat=False,
    )
    bridge = OrderLifecycleJournalRuntimeBridgeV2(writer)
    bridge.register_lifecycle(
        lifecycle_id="strict:order-1",
        runtime_source="authoritative_python_replay",
        client_order_id="order-1",
        exchange_order_id="exchange-1",
        symbol="BTCUSDC",
        side="BUY",
    )
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    writer._recover_locked = lambda: (_ for _ in ()).throw(AssertionError("disk rescan"))

    result = bridge.submit_callback(
        lifecycle_id="strict:order-1",
        lifecycle=lifecycle,
        callback=OrderLifecycleJournalV2SourceCallback(
            callback_id="submit",
            callback_type="order_submit",
            received_ts_ns=1_000_000_000,
            simulator_queue_source="pending_activation",
            exact_queue_path_valid=False,
        ),
    )

    assert result.status == "committed"
    assert writer.close()["formal_collection_valid"] is True


def test_strict_native_recovers_before_building_batch_after_cursor_write_failure(
    tmp_path: Path,
) -> None:
    failed = False

    def fail_before_cursor_replace(point: str, _context: object) -> None:
        nonlocal failed
        if point == "before_cursor_replace" and not failed:
            failed = True
            raise OSError("before_cursor_replace")

    writer = StrictNativeJournalWriterV2(
        tmp_path,
        session_id="strict-recovery",
        runtime_identity={"identity": "strict-failure-recovery"},
        start_heartbeat=False,
        fault_injector=fail_before_cursor_replace,
    )
    bridge = OrderLifecycleJournalRuntimeBridgeV2(writer)
    bridge.register_lifecycle(
        lifecycle_id="strict:order-1",
        runtime_source="authoritative_python_replay",
        client_order_id="order-1",
        exchange_order_id="exchange-1",
        symbol="BTCUSDC",
        side="BUY",
    )
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    submit_callback = OrderLifecycleJournalV2SourceCallback(
        callback_id="submit",
        callback_type="order_submit",
        received_ts_ns=1_000_000_000,
        simulator_queue_source="pending_activation",
        exact_queue_path_valid=False,
    )
    with pytest.raises(OSError, match="before_cursor_replace"):
        bridge.submit_callback(
            lifecycle_id="strict:order-1",
            lifecycle=lifecycle,
            callback=submit_callback,
        )
    assert len(list(writer.parts_root.glob("part-*.manifest.json"))) == 1

    writer.set_fault_injector(None)
    recoveries = 0
    recover = writer._recover_locked

    def count_recovery() -> None:
        nonlocal recoveries
        recoveries += 1
        recover()

    writer._recover_locked = count_recovery
    lifecycle.activate(1_200_000_000, exchange_ts_ns=1_100_000_000)
    activation_callback = OrderLifecycleJournalV2SourceCallback(
        callback_id="activate",
        callback_type="order_activation",
        received_ts_ns=1_200_000_000,
        exchange_ts_ns=1_100_000_000,
        simulator_queue_source="native_exchange_book",
        exact_queue_path_valid=True,
    )
    activation = bridge.submit_callback(
        lifecycle_id="strict:order-1",
        lifecycle=lifecycle,
        callback=activation_callback,
    )
    repeated = bridge.submit_callback(
        lifecycle_id="strict:order-1",
        lifecycle=lifecycle,
        callback=activation_callback,
    )

    assert activation.status == "committed"
    assert repeated.status == "noop"
    assert recoveries == 1
    assert writer.cursor_for(
        lifecycle_id="strict:order-1",
        client_order_id="order-1",
    ).last_emitted_sequence == 2
    assert len(list(writer.parts_root.glob("part-*.manifest.json"))) == 2
    writer.close()

    restarted = StrictNativeJournalWriterV2(
        tmp_path,
        session_id="strict-recovery",
        runtime_identity={"identity": "strict-failure-recovery"},
        start_heartbeat=False,
    )
    assert restarted.cursor_for(
        lifecycle_id="strict:order-1",
        client_order_id="order-1",
    ).last_emitted_sequence == 2
    recovered_health = restarted.health_snapshot()
    assert recovered_health["rows_committed"] == 2
    assert recovered_health["error_count"] == 1
    assert recovered_health["formal_collection_valid"] is False
    restarted.close()
