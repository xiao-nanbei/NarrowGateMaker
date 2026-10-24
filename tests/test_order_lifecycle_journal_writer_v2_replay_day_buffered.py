from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

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
    OrderLifecycleJournalWriterV2,
)
from models.replay.order_lifecycle_v2_replay_adapter_strict_native import (
    OrderLifecycleV2ReplayAdapter,
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
    assert submit.commit_scope == activation.commit_scope == "memory_buffer"
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


def test_unpublished_day_buffer_cannot_be_resumed(tmp_path: Path) -> None:
    writer = _writer(tmp_path)
    bridge = _bridge(writer)
    result = bridge.submit_callback(
        lifecycle_id="day:order-1",
        lifecycle=QuantityWeightedOrderLifecycle(0.001, 1_000_000_000),
        callback=OrderLifecycleJournalV2SourceCallback(
            callback_id="submit", callback_type="order_submit", received_ts_ns=1_000_000_000,
            simulator_queue_source="pending_activation", exact_queue_path_valid=False),
    )
    assert result.commit_scope == "memory_buffer"
    assert writer.health_snapshot()["rows_committed"] == 0
    writer._release_process_lock()  # simulate loss before close, not a day admission
    writer._closed = True
    assert not list(writer.parts_root.iterdir())
    with pytest.raises(RuntimeError, match="cannot resume"):
        _writer(tmp_path)


def test_day_and_callback_publication_have_identical_logical_rows(tmp_path: Path):
    rows = []
    for name, writer_type in [("day", DayBufferedReplayJournalWriterV2),
                              ("callback", OrderLifecycleJournalWriterV2)]:
        writer = writer_type(tmp_path/name, session_id="same", runtime_identity={"case": "same"},
                             storage_format="parquet", start_heartbeat=False)
        bridge = _bridge(writer)
        lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
        result = bridge.submit_callback(lifecycle_id="day:order-1", lifecycle=lifecycle,
            callback=OrderLifecycleJournalV2SourceCallback(callback_id="submit", callback_type="order_submit",
                received_ts_ns=1_000_000_000, simulator_queue_source="pending_activation",
                exact_queue_path_valid=False))
        assert result.commit_scope == ("memory_buffer" if name == "day" else "callback_durable")
        lifecycle.activate(1_200_000_000, exchange_ts_ns=1_100_000_000)
        bridge.submit_callback(lifecycle_id="day:order-1", lifecycle=lifecycle,
            callback=OrderLifecycleJournalV2SourceCallback(callback_id="activate", callback_type="order_activation",
                received_ts_ns=1_200_000_000, exchange_ts_ns=1_100_000_000,
                simulator_queue_source="native_exchange_book", exact_queue_path_valid=True))
        writer.close()
        rows.append(sorted([row for p in writer.parts_root.glob('*.parquet')
                            for row in pq.read_table(p).to_pylist()], key=lambda row: row['lifecycle_sequence']))
    assert rows[0] == rows[1]


def test_strict_adapter_selects_day_buffered_writer_without_global_rebinding(
    tmp_path: Path,
) -> None:
    adapter = OrderLifecycleV2ReplayAdapter(
        root=tmp_path,
        session_id="adapter-day",
        runtime_identity={"identity": REPLAY_WRITER_ID},
        symbol="BTCUSDC",
        writer_mode="day_buffered",
    )
    assert isinstance(adapter.writer, DayBufferedReplayJournalWriterV2)
    adapter.submit(
        {
            "trace_id": 1,
            "side": "BUY",
            "submit_ts": 1_000,
            "quote_ts": 1_000,
            "quantity": 0.001,
            "remaining": 0.001,
        },
        1_000,
    )
    health = adapter.close()
    assert health["part_count"] == 1
    assert health["callbacks_committed"] == 1
