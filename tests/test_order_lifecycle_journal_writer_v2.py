from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from execution.order_lifecycle import QuantityWeightedOrderLifecycle
from execution.order_lifecycle_journal_v2 import OrderLifecycleJournalV2SourceCallback
from execution.order_lifecycle_journal_writer_v2 import (
    ORDER_LIFECYCLE_JOURNAL_WRITER_V2_HEALTH_VERSION,
    OrderLifecycleJournalRuntimeBridgeV2,
    OrderLifecycleJournalWriterV2,
)


class _RaiseOnce:
    def __init__(self, target: str) -> None:
        self.target = target
        self.raised = False

    def __call__(self, point: str, context) -> None:
        if point == self.target and not self.raised:
            self.raised = True
            raise OSError(f"injected:{point}")


def _identity() -> dict[str, object]:
    return {
        "baseline_epoch_id": "epoch-v9",
        "runtime_code_sha256": "a" * 64,
        "execution_abi": "order-lifecycle-v2",
    }


def _writer(
    root: Path,
    *,
    session_id: str = "test-session",
    storage_format: str = "parquet",
    initial_active_order_ids=(),
) -> OrderLifecycleJournalWriterV2:
    return OrderLifecycleJournalWriterV2(
        root,
        session_id=session_id,
        runtime_identity=_identity(),
        storage_format=storage_format,
        initial_active_order_ids=initial_active_order_ids,
        heartbeat_interval_s=60.0,
        start_heartbeat=False,
    )


def _register(
    bridge: OrderLifecycleJournalRuntimeBridgeV2,
    *,
    lifecycle_id: str = "epoch-v9:client-17",
    client_order_id: str = "client-17",
    exchange_order_id: str | None = "exchange-42",
) -> None:
    bridge.register_lifecycle(
        lifecycle_id=lifecycle_id,
        runtime_source="replay",
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
        symbol="BTCUSDC",
        side="BUY",
    )


def _callback(
    callback_id: str,
    received_ts_ns: int,
    exchange_ts_ns: int | None = None,
) -> OrderLifecycleJournalV2SourceCallback:
    return OrderLifecycleJournalV2SourceCallback(
        callback_id=callback_id,
        callback_type="execution_report",
        received_ts_ns=received_ts_ns,
        exchange_ts_ns=exchange_ts_ns,
    )


def _submit(
    bridge: OrderLifecycleJournalRuntimeBridgeV2,
    lifecycle: QuantityWeightedOrderLifecycle,
    *,
    lifecycle_id: str = "epoch-v9:client-17",
):
    return bridge.submit_callback(
        lifecycle_id=lifecycle_id,
        lifecycle=lifecycle,
        callback=_callback("callback-submit", 1_000_000_000),
    )


def _manifest_paths(root: Path) -> list[Path]:
    return sorted(root.glob("session-test-session/parts/part-*.manifest.json"))


def _data_paths(root: Path, suffix: str) -> list[Path]:
    return sorted(root.glob(f"session-test-session/parts/part-*.{suffix}"))


def _read_health(root: Path) -> dict[str, object]:
    return json.loads((root / "session-test-session" / "health.json").read_text(encoding="utf-8"))


def test_callback_batch_is_one_atomic_part_and_preserves_dual_clocks(
    tmp_path: Path,
) -> None:
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    with _writer(tmp_path) as writer:
        bridge = OrderLifecycleJournalRuntimeBridgeV2(writer)
        _register(bridge)
        assert _submit(bridge, lifecycle).status == "committed"

        lifecycle.activate(2_200_000_000, exchange_ts_ns=2_000_000_000)
        lifecycle.observe_fill(
            remaining_after=0.0004,
            visibility_ts_ns=2_300_000_000,
            exchange_ts_ns=2_000_000_000,
        )
        result = bridge.submit_callback(
            lifecycle_id="epoch-v9:client-17",
            lifecycle=lifecycle,
            callback=_callback(
                "callback-activate-and-partial",
                2_000_000_000,
                2_000_000_000,
            ),
        )
        assert result.status == "committed"
        assert result.row_count == 2
        assert result.checkpoint["last_emitted_sequence"] == 3

        health = writer.health_snapshot()
        assert health["schema_version"] == ORDER_LIFECYCLE_JOURNAL_WRITER_V2_HEALTH_VERSION
        assert health["callbacks_committed"] == 2
        assert health["rows_committed"] == 3
        assert health["rows_dropped"] == 0
        assert health["error_count"] == 0
        assert health["last_flush_ts_ns"] > 0
        assert health["last_heartbeat_ts_ns"] > 0
        assert health["economic_outcomes_read"] is False
        assert health["q90_action_authorized"] is False

    manifests = [json.loads(path.read_text()) for path in _manifest_paths(tmp_path)]
    two_row = next(item for item in manifests if item["row_count"] == 2)
    rows = pq.read_table(
        tmp_path / "session-test-session" / "parts" / two_row["data_file"]
    ).to_pylist()
    assert [row["lifecycle_event"] for row in rows] == ["activate", "partial_fill"]
    assert [row["source_callback_event_ordinal"] for row in rows] == [1, 2]
    assert {row["source_callback_event_count"] for row in rows} == {2}
    partial = rows[-1]
    assert partial["event_visibility_ts_ns"] == 2_300_000_000
    assert partial["event_exchange_ts_ns"] == 2_000_000_000
    assert partial["quantity_time_exposure_visible_btc_s"] == pytest.approx(0.0001)
    assert partial["quantity_time_exposure_exchange_btc_s"] == pytest.approx(0.0)


@pytest.mark.parametrize(
    "storage_format,fault_point",
    [
        ("parquet", "before_payload_replace"),
        ("parquet", "after_payload_replace"),
        ("parquet", "after_manifest_replace"),
        ("jsonl", "before_health_replace"),
        ("jsonl", "after_health_replace"),
        ("jsonl", "before_cursor_replace"),
        ("jsonl", "after_cursor_replace"),
    ],
)
def test_fault_boundaries_retry_without_lost_or_duplicate_events(
    tmp_path: Path,
    storage_format: str,
    fault_point: str,
) -> None:
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    writer = _writer(tmp_path, storage_format=storage_format)
    bridge = OrderLifecycleJournalRuntimeBridgeV2(writer)
    _register(bridge)
    injector = _RaiseOnce(fault_point)
    writer.set_fault_injector(injector)
    with pytest.raises(OSError, match=fault_point):
        _submit(bridge, lifecycle)

    cursor = writer.cursor_for(
        lifecycle_id="epoch-v9:client-17",
        client_order_id="client-17",
    )
    if fault_point == "after_cursor_replace":
        assert cursor.last_emitted_sequence == 1
    else:
        assert cursor.last_emitted_sequence == 0
    if fault_point == "after_payload_replace":
        health_after_failure = writer.health_snapshot()
        assert health_after_failure["orphan_payload_count"] == 1
        assert health_after_failure["formal_collection_valid"] is False
    durable_health = _read_health(tmp_path)
    cursor_files = list((tmp_path / "session-test-session" / "cursors").glob("cursor-*.json"))
    if fault_point in {"after_health_replace", "before_cursor_replace"}:
        assert durable_health["rows_committed"] == 1
        assert cursor_files == []
    elif fault_point == "after_cursor_replace":
        assert durable_health["rows_committed"] == 1
        assert len(cursor_files) == 1
    writer.set_fault_injector(None)
    writer.close()

    restarted = _writer(tmp_path, storage_format=storage_format)
    restarted_bridge = OrderLifecycleJournalRuntimeBridgeV2(restarted)
    _register(restarted_bridge)
    replay = _submit(restarted_bridge, lifecycle)
    assert replay.status in {"committed", "noop", "duplicate"}
    assert (
        restarted.cursor_for(
            lifecycle_id="epoch-v9:client-17",
            client_order_id="client-17",
        ).last_emitted_sequence
        == 1
    )
    restarted.close()

    assert len(_manifest_paths(tmp_path)) == 1
    suffix = "parquet" if storage_format == "parquet" else "jsonl"
    assert len(_data_paths(tmp_path, suffix)) == 1
    manifest = json.loads(_manifest_paths(tmp_path)[0].read_text())
    assert manifest["row_count"] == 1
    assert len(manifest["event_ids"]) == 1
    assert _read_health(tmp_path)["rows_committed"] == 1


@pytest.mark.parametrize("storage_format", ["parquet", "jsonl"])
def test_restart_and_repeated_callback_are_idempotent(
    tmp_path: Path,
    storage_format: str,
) -> None:
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    first = _writer(tmp_path, storage_format=storage_format)
    first_bridge = OrderLifecycleJournalRuntimeBridgeV2(first)
    _register(first_bridge)
    committed = _submit(first_bridge, lifecycle)
    assert committed.status == "committed"
    assert _submit(first_bridge, lifecycle).status == "noop"
    first.close()

    second = _writer(tmp_path, storage_format=storage_format)
    second_bridge = OrderLifecycleJournalRuntimeBridgeV2(second)
    _register(second_bridge)
    assert _submit(second_bridge, lifecycle).status == "noop"
    health = second.health_snapshot()
    assert health["restart_count"] == 1
    assert health["callbacks_committed"] == 1
    assert health["rows_committed"] == 1
    second.close()
    assert len(_manifest_paths(tmp_path)) == 1


def test_normal_callbacks_do_not_rescan_durable_parts(tmp_path: Path) -> None:
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    writer = _writer(tmp_path, storage_format="jsonl")
    bridge = OrderLifecycleJournalRuntimeBridgeV2(writer)
    _register(bridge)
    recover_calls = 0
    original_recover = writer._recover_locked

    def counted_recover() -> None:
        nonlocal recover_calls
        recover_calls += 1
        original_recover()

    writer._recover_locked = counted_recover
    assert _submit(bridge, lifecycle).status == "committed"
    lifecycle.activate(2_200_000_000, exchange_ts_ns=2_000_000_000)
    assert (
        bridge.submit_callback(
            lifecycle_id="epoch-v9:client-17",
            lifecycle=lifecycle,
            callback=_callback("callback-activate", 2_200_000_000, 2_000_000_000),
        ).status
        == "committed"
    )
    assert recover_calls == 0

    writer.reconcile()
    assert recover_calls == 1
    writer.close()


def test_failed_commit_recovers_before_same_process_retry(tmp_path: Path) -> None:
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    writer = _writer(tmp_path, storage_format="jsonl")
    bridge = OrderLifecycleJournalRuntimeBridgeV2(writer)
    _register(bridge)
    writer.set_fault_injector(_RaiseOnce("after_manifest_replace"))

    with pytest.raises(OSError, match="after_manifest_replace"):
        _submit(bridge, lifecycle)

    writer.set_fault_injector(None)
    retry = _submit(bridge, lifecycle)
    assert retry.status in {"duplicate", "noop"}
    cursor = writer.cursor_for(
        lifecycle_id="epoch-v9:client-17",
        client_order_id="client-17",
    )
    assert cursor.last_emitted_sequence == 1
    assert writer.health_snapshot()["rows_committed"] == 1
    writer.close()


def test_hot_start_quarantine_excludes_pre_cutover_lifecycles(tmp_path: Path) -> None:
    writer = _writer(tmp_path, initial_active_order_ids=("old-live-order",))
    bridge = OrderLifecycleJournalRuntimeBridgeV2(writer)
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    _register(
        bridge,
        lifecycle_id="epoch-v9:pre-cutover",
        client_order_id="pre-cutover",
    )
    result = _submit(bridge, lifecycle, lifecycle_id="epoch-v9:pre-cutover")
    assert result.status == "quarantined"
    assert result.reason == "hot_start_quarantine"
    with pytest.raises(ValueError, match="cannot release"):
        writer.observe_quarantined_exchange_terminal(
            "old-live-order",
            terminal_reason="local_shutdown_cancel",
        )

    writer.observe_quarantined_exchange_terminal(
        "old-live-order",
        terminal_reason="cancel_ack",
    )
    assert writer.collecting is True

    post = QuantityWeightedOrderLifecycle(0.001, 2_000_000_000)
    _register(
        bridge,
        lifecycle_id="epoch-v9:post-cutover",
        client_order_id="post-cutover",
    )
    post_result = bridge.submit_callback(
        lifecycle_id="epoch-v9:post-cutover",
        lifecycle=post,
        callback=_callback("post-cutover-submit", 2_000_000_000),
    )
    assert post_result.status == "committed"

    pre_again = _submit(bridge, lifecycle, lifecycle_id="epoch-v9:pre-cutover")
    assert pre_again.status == "quarantined"
    assert pre_again.reason == "pre_cutover_lifecycle"
    health = writer.health_snapshot()
    assert health["callbacks_quarantined"] == 2
    assert health["rows_committed"] == 1
    assert "epoch-v9:pre-cutover" in health["excluded_lifecycle_ids"]
    writer.close()


def test_explicit_local_shutdown_censor_is_durable_and_closes_observation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    writer = _writer(tmp_path)
    bridge = OrderLifecycleJournalRuntimeBridgeV2(writer)
    _register(bridge)
    assert _submit(bridge, lifecycle).status == "committed"
    lifecycle.activate(2_200_000_000, exchange_ts_ns=2_000_000_000)
    assert (
        bridge.submit_callback(
            lifecycle_id="epoch-v9:client-17",
            lifecycle=lifecycle,
            callback=_callback("callback-activate", 2_000_000_000, 2_000_000_000),
        ).status
        == "committed"
    )

    events = [dict(event) for event in lifecycle.events()]
    events.append(
        {
            "sequence": 3,
            "event": "local_shutdown_censor",
            "visibility_ts_ns": 3_200_000_000,
            "exchange_ts_ns": 0,
            "phase_before": "ACTIVE",
            "phase_after": "ACTIVE",
            "remaining_qty_before": 0.001,
            "remaining_qty_after": 0.001,
            "quantity_time_exposure_btc_s": 0.001,
            "quantity_time_exposure_visible_btc_s": 0.001,
            "quantity_time_exposure_exchange_btc_s": 0.0,
            "exchange_exposure_valid": True,
            "reason": "local_shutdown_cancel",
        }
    )
    snapshot = dict(lifecycle.snapshot())
    snapshot["quantity_time_exposure_btc_s"] = 0.001
    snapshot["quantity_time_exposure_visible_btc_s"] = 0.001
    snapshot["quantity_time_exposure_visibility_minus_exchange_btc_s"] = 0.001
    monkeypatch.setattr(lifecycle, "events", lambda: tuple(events))
    monkeypatch.setattr(lifecycle, "snapshot", lambda: dict(snapshot))

    censored = bridge.submit_callback(
        lifecycle_id="epoch-v9:client-17",
        lifecycle=lifecycle,
        callback=_callback("callback-local-censor", 3_200_000_000),
    )
    assert censored.status == "committed"
    assert writer.health_snapshot()["local_shutdown_censor_count"] == 1
    with pytest.raises(ValueError, match="after local_shutdown_censor"):
        bridge.submit_callback(
            lifecycle_id="epoch-v9:client-17",
            lifecycle=lifecycle,
            callback=_callback("callback-after-censor", 3_300_000_000),
        )
    writer.close()


def test_unknown_terminal_reason_fails_closed_without_part_or_cursor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    lifecycle.exchange_terminal(
        2_200_000_000,
        reason="rejected",
        exchange_ts_ns=2_000_000_000,
    )
    events = [dict(event) for event in lifecycle.events()]
    events[-1]["reason"] = "mystery_terminal"
    monkeypatch.setattr(lifecycle, "events", lambda: tuple(events))
    writer = _writer(tmp_path)
    bridge = OrderLifecycleJournalRuntimeBridgeV2(writer)
    _register(bridge)
    with pytest.raises(ValueError, match="unsupported reason"):
        bridge.submit_callback(
            lifecycle_id="epoch-v9:client-17",
            lifecycle=lifecycle,
            callback=_callback("callback-bad-terminal", 2_000_000_000, 2_000_000_000),
        )
    assert (
        writer.cursor_for(
            lifecycle_id="epoch-v9:client-17",
            client_order_id="client-17",
        ).last_emitted_sequence
        == 0
    )
    assert _manifest_paths(tmp_path) == []
    assert writer.health_snapshot()["error_count"] == 1
    writer.close()


def test_runtime_identity_rejects_economic_fields(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="economic field"):
        OrderLifecycleJournalWriterV2(
            tmp_path,
            session_id="bad-identity",
            runtime_identity={"reward_usdc": 1.0},
            start_heartbeat=False,
        )
