from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq

from execution.order_lifecycle import QuantityWeightedOrderLifecycle
from execution.order_lifecycle_journal_v2_strict_native import (
    OrderLifecycleJournalV2SourceCallback,
)
from execution.order_lifecycle_journal_writer_v2_replay_day_buffered import (
    REPLAY_WRITER_ID,
    DayBufferedReplayJournalWriterV2,
)
from execution.order_lifecycle_journal_writer_v2_strict_native import (
    OrderLifecycleJournalRuntimeBridgeV2,
)


def _writer(root: Path) -> DayBufferedReplayJournalWriterV2:
    return DayBufferedReplayJournalWriterV2(
        root,
        session_id="replay-day",
        runtime_identity={"identity": REPLAY_WRITER_ID, "economic_outcomes_read": False},
        storage_format="parquet",
        initial_active_order_ids=(),
        heartbeat_interval_s=60.0,
        start_heartbeat=False,
    )


def _bridge(writer: DayBufferedReplayJournalWriterV2) -> OrderLifecycleJournalRuntimeBridgeV2:
    bridge = OrderLifecycleJournalRuntimeBridgeV2(writer)
    bridge.register_lifecycle(
        lifecycle_id="day:order-1",
        runtime_source="authoritative_python_replay",
        client_order_id="order-1",
        exchange_order_id="exchange-1",
        symbol="BTCUSDC",
        side="BUY",
    )
    return bridge


def test_day_buffered_replay_publishes_one_atomic_part_at_close(tmp_path: Path) -> None:
    writer = _writer(tmp_path)
    bridge = _bridge(writer)
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)

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
    assert submit.status == activation.status == "committed"
    parts = tmp_path / "session-replay-day" / "parts"
    assert list(parts.iterdir()) == []
    collecting = writer.health_snapshot()
    assert collecting["callbacks_committed"] == 0
    assert collecting["callbacks_buffered"] == 2
    assert collecting["rows_buffered"] == 2

    health = writer.close()
    assert health["callbacks_committed"] == 2
    assert health["rows_committed"] == 2
    assert health["part_count"] == 1
    assert health["rows_dropped"] == 0
    assert health["error_count"] == 0
    assert health["formal_collection_valid"] is True
    manifests = list(parts.glob("part-*.manifest.json"))
    payloads = list(parts.glob("part-*.parquet"))
    assert len(manifests) == len(payloads) == 1
    manifest = json.loads(manifests[0].read_text(encoding="utf-8"))
    assert manifest["identity"] == REPLAY_WRITER_ID
    assert manifest["callback_count"] == 2
    assert manifest["row_count"] == 2
    assert [row["lifecycle_event"] for row in pq.read_table(payloads[0]).to_pylist()] == [
        "submit",
        "activate",
    ]


def test_day_buffered_replay_requires_held_process_lock(tmp_path: Path) -> None:
    writer = _writer(tmp_path)
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
