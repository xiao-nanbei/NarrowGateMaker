from __future__ import annotations

import copy
import csv
import io
import json
import logging
import queue
import signal
import stat
import sys
import threading
import time
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import live.main as live_main
import strategy.maker_engine as maker_engine_module
from execution.runtime_evidence_writer import (
    RuntimeEvidenceQueueFull,
    RuntimeEvidenceWorkerFailed,
    RuntimeEvidenceWriter,
    RuntimeEvidenceWriterError,
)
from live.config import Config, _validate_config
from live.main import (
    EXECUTION_STATE_UNCERTAIN_EXIT_CODE,
    _AsyncLoggingRuntime,
    _cleanup_failed_live_startup,
    _close_runtime_evidence_writer_with_final_health,
    _create_live_runtime_components,
    _DrainingQueueListener,
    _note_startup_cleanup_errors,
    _OrderedQueueHandler,
    _quiesce_callbacks_then_stop_engine,
    _safe_runtime_log,
    _ShutdownSignalFlag,
    arm_order_gateway_experiment_runtime_guard,
    arm_websocket_order_ab_runtime_guard,
    collect_runtime_safety_health,
    create_rest_client,
    create_rest_clients,
    create_websocket_order_ab_gateway,
    maintain_optional_deep_book,
    optional_deep_book_health,
    record_startup_runtime_identity,
    resolve_live_shutdown_exit,
    runtime_safety_health_payload_factory,
    setup_logging,
    shutdown_logging,
    start_engine_with_prospective_collection,
)
from live.runtime_policy import (
    admit_runtime_policies,
    require_f05_boolean_cooldown_restart,
    require_f05_buy_e3_restart,
    require_q90_action_restart,
    write_runtime_identity,
)
from live.ws_handler import WSHandler
from strategy.inventory_manager import InventoryManager, PositionState
from strategy.maker_engine import MakerEngine

ROOT = Path(__file__).resolve().parents[1]
POLICY_CASES = (
    ("q90_action", "dynamic_fill_hazard_action_enabled", "NARROWGATE_ALLOW_Q90_PRIVATE_DEPLOY"),
    (
        "f05_boolean_cooldown", "boolean_cooldown_policy_enabled",
        "NARROWGATE_ALLOW_F05_BOOLEAN_COOLDOWN_PRIVATE_DEPLOY",
    ),
    ("f05_buy_e3", "buy_e3_cooldown_policy_enabled", "NARROWGATE_ALLOW_F05_BUY_E3_PRIVATE_DEPLOY"),
)


def test_failed_live_startup_closes_every_constructed_owner_without_engine_stop() -> None:
    closed: list[str] = []

    class Engine:
        signal = SimpleNamespace(stop=lambda: closed.append("signal"))
        _exact_opportunity_tape_runtime = SimpleNamespace(
            close=lambda: closed.append("exact")
        )

        @staticmethod
        def close_fill_cooldown_checkpoint_store() -> None:
            closed.append("checkpoint")

        @staticmethod
        def stop() -> None:  # pragma: no cover - must never be called
            raise AssertionError("unstarted engine cleanup must not issue exchange writes")

    writer = SimpleNamespace(
        close=lambda *, drain_timeout_s: closed.append(
            f"writer:{drain_timeout_s:.1f}"
        )
    )
    ws = SimpleNamespace(stop=lambda: closed.append("ws"))
    gateway = SimpleNamespace(close=lambda: closed.append("gateway"))
    rest_clients = SimpleNamespace(close=lambda: closed.append("rest"))
    logging_runtime = SimpleNamespace(close=lambda: closed.append("logging"))

    assert (
        _cleanup_failed_live_startup(
            logging_runtime=logging_runtime,
            rest_clients=rest_clients,
            order_gateway=gateway,
            engine=Engine(),
            ws=ws,
            runtime_evidence_writer=writer,
        )
        == ()
    )
    assert closed == [
        "writer:1.0",
        "ws",
        "signal",
        "checkpoint",
        "exact",
        "gateway",
        "rest",
        "logging",
    ]


def test_failed_live_startup_continues_after_engine_subresource_error() -> None:
    closed: list[str] = []

    def fail_signal() -> None:
        closed.append("signal")
        raise OSError("signal stop failed")

    engine = SimpleNamespace(
        signal=SimpleNamespace(stop=fail_signal),
        close_fill_cooldown_checkpoint_store=lambda: closed.append("checkpoint"),
        _exact_opportunity_tape_runtime=SimpleNamespace(
            close=lambda: closed.append("exact")
        ),
    )
    failures = _cleanup_failed_live_startup(
        logging_runtime=SimpleNamespace(close=lambda: closed.append("logging")),
        rest_clients=SimpleNamespace(close=lambda: closed.append("rest")),
        engine=engine,
    )

    assert closed == ["signal", "checkpoint", "exact", "rest", "logging"]
    assert len(failures) == 1
    assert failures[0][0] == "unstarted-engine-resources"
    assert "1 unstarted engine resource" in str(failures[0][1])


def test_failed_live_startup_preserves_primary_error_and_all_cleanup_failures() -> None:
    primary = RuntimeError("constructor failed")
    closed: list[str] = []

    def fail_writer(*, drain_timeout_s: float) -> None:
        assert drain_timeout_s == pytest.approx(1.0)
        closed.append("writer")
        raise OSError("writer stuck")

    failures = _cleanup_failed_live_startup(
        logging_runtime=SimpleNamespace(close=lambda: closed.append("logging")),
        rest_clients=SimpleNamespace(close=lambda: closed.append("rest")),
        runtime_evidence_writer=SimpleNamespace(close=fail_writer),
    )
    _note_startup_cleanup_errors(primary, failures)

    assert closed == ["writer", "rest", "logging"]
    assert [(name, str(error)) for name, error in failures] == [
        ("runtime-evidence-writer", "writer stuck")
    ]
    assert primary.__notes__ == [
        "startup cleanup failed for runtime-evidence-writer: "
        "OSError: writer stuck"
    ]


def test_live_component_transaction_closes_earlier_owners_on_attach_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    closed: list[str] = []
    client = SimpleNamespace()
    rest_clients = SimpleNamespace(
        order=client,
        order_buy=SimpleNamespace(),
        order_sell=SimpleNamespace(),
        order_safety=SimpleNamespace(),
        reconciliation=SimpleNamespace(),
        reconciliation_worker=SimpleNamespace(),
        metrics=SimpleNamespace(),
        identity=lambda: {"roles": ("order",)},
        close=lambda: closed.append("rest"),
    )
    websocket_gateway = SimpleNamespace(close=lambda: closed.append("ws-order"))

    class Gateway:
        active_transport = "rest"
        async_order_lanes_enabled = False
        cross_side_order_lanes_enabled = False

        def set_runtime_evidence_writer(self, _writer, _path) -> None:
            raise RuntimeError("synthetic attach failure")

        @staticmethod
        def close() -> None:
            closed.append("gateway")

    class Engine:
        signal = SimpleNamespace(stop=lambda: closed.append("signal"))
        _exact_opportunity_tape_runtime = None

        @staticmethod
        def set_replace_terminal_continuation_wakeup(_callback) -> None:
            return None

        @staticmethod
        def restore_fill_cooldown_checkpoint() -> dict[str, object]:
            return {
                "restore_mode": "test",
                "checkpoint_loaded": 0,
                "checkpoint_sequence": 0,
                "buy_deadline_identity": "B0",
                "buy_remaining_ms": 0,
            }

        @staticmethod
        def set_event_source(_source) -> None:
            return None

        @staticmethod
        def set_runtime_evidence_writer(_writer) -> None:
            raise AssertionError("gateway attachment must fail first")

        @staticmethod
        def close_fill_cooldown_checkpoint_store() -> None:
            closed.append("checkpoint")

    class Wakeup:
        @staticmethod
        def notify_replacement_terminal() -> None:
            return None

    class WebsocketHandler:
        def __init__(self, _engine, _cfg) -> None:
            pass

        @staticmethod
        def stop() -> None:
            closed.append("ws-handler")

    class Writer:
        def close(self, *, drain_timeout_s: float) -> None:
            closed.append(f"writer:{drain_timeout_s:.1f}")

    monkeypatch.setattr(live_main, "create_rest_clients", lambda _cfg: rest_clients)
    monkeypatch.setattr(
        live_main,
        "create_websocket_order_ab_gateway",
        lambda _cfg: websocket_gateway,
    )
    monkeypatch.setattr(live_main, "BinanceUsdMOrderGateway", lambda **_kw: Gateway())
    monkeypatch.setattr(live_main, "MakerEngine", lambda *_a, **_kw: Engine())
    monkeypatch.setattr(live_main, "_LiveMainLoopWakeup", Wakeup)
    monkeypatch.setattr(live_main, "WSHandler", WebsocketHandler)
    monkeypatch.setattr(live_main, "RuntimeEvidenceWriter", Writer)
    logging_runtime = SimpleNamespace(close=lambda: closed.append("logging"))
    cfg = SimpleNamespace(
        api=SimpleNamespace(
            async_order_lanes_enabled=False,
            cross_side_order_lanes_enabled=False,
            async_order_lane_capacity=8,
            async_order_lane_drain_timeout_s=10.0,
            async_order_lane_max_runtime_s=900.0,
        ),
        logging=SimpleNamespace(order_gateway_receipt_log="receipts.csv"),
    )

    with pytest.raises(RuntimeError, match="synthetic attach failure"):
        _create_live_runtime_components(
            cfg=cfg,
            safety_authority={},
            logging_runtime=logging_runtime,
        )

    assert closed == [
        "writer:1.0",
        "ws-handler",
        "signal",
        "checkpoint",
        "gateway",
        "rest",
        "logging",
    ]


@pytest.mark.parametrize("enabled", (False, True))
def test_main_loop_skips_disabled_deep_book_work(enabled: bool) -> None:
    ws = SimpleNamespace(
        cfg=SimpleNamespace(
            websocket=SimpleNamespace(deep_book_enabled=enabled)
        ),
        maintain_deep_book=Mock(),
        maintain_active_order_depth_paths=Mock(),
    )

    assert maintain_optional_deep_book(ws, now_ns=123) is enabled

    expected_calls = 1 if enabled else 0
    assert ws.maintain_deep_book.call_count == expected_calls
    assert ws.maintain_active_order_depth_paths.call_count == expected_calls


def test_disabled_deep_book_health_does_not_read_handler_state() -> None:
    ws = SimpleNamespace(
        cfg=SimpleNamespace(
            websocket=SimpleNamespace(deep_book_enabled=False)
        ),
        deep_book_snapshot=Mock(side_effect=AssertionError("must not read")),
        active_order_depth_snapshot=Mock(
            side_effect=AssertionError("must not read")
        ),
    )

    deep_book, active_order_depth = optional_deep_book_health(ws)

    assert deep_book["enabled"] == 0
    assert deep_book["generation"] == 0
    assert active_order_depth["tracked"] == 0
    ws.deep_book_snapshot.assert_not_called()
    ws.active_order_depth_snapshot.assert_not_called()


class _BlockingLogSink(logging.Handler):
    def __init__(self, block_message: str) -> None:
        super().__init__()
        self.block_message = block_message
        self.entered = threading.Event()
        self.release_event = threading.Event()
        self.messages: list[str] = []
        self.emit_thread_ids: dict[str, int] = {}

    def emit(self, record: logging.LogRecord) -> None:
        message = record.getMessage()
        if message == self.block_message:
            self.entered.set()
            if not self.release_event.wait(timeout=2.0):
                raise TimeoutError("test log sink was not released")
        self.messages.append(message)
        self.emit_thread_ids[message] = threading.get_ident()


def _start_ordered_test_logger(
    sink: logging.Handler,
    *,
    queue_capacity: int,
) -> tuple[logging.Logger, _OrderedQueueHandler]:
    record_queue: queue.Queue[logging.LogRecord | None] = queue.Queue(
        maxsize=queue_capacity
    )
    listener = _DrainingQueueListener(record_queue, sink)
    handler = _OrderedQueueHandler(record_queue, listener)
    logger = logging.Logger(f"ordered-test-{id(handler)}", level=logging.INFO)
    logger.addHandler(handler)
    listener.start()
    return logger, handler


def test_async_logging_fans_out_fifo_and_drains_normal_shutdown(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    console = io.StringIO()
    log_path = tmp_path / "maker.log"
    cfg = Config()
    cfg.logging.level = "INFO"
    cfg.logging.console = True
    cfg.logging.file = str(log_path)
    monkeypatch.setattr(sys, "stdout", console)

    runtime = setup_logging(cfg, queue_capacity=128)
    logger = logging.getLogger("async-logging-fifo-test")
    expected = [f"sequence={sequence}" for sequence in range(64)]
    try:
        for message in expected:
            logger.info(message)
    finally:
        shutdown_logging(runtime)

    assert runtime.listener._thread is None
    assert runtime.queue_handler.queue.empty()
    assert runtime.queue_handler.queue.maxsize == 128
    console_lines = [
        line
        for line in console.getvalue().splitlines()
        if "[async-logging-fifo-test]" in line
    ]
    file_lines = [
        line
        for line in log_path.read_text(encoding="utf-8").splitlines()
        if "[async-logging-fifo-test]" in line
    ]
    assert [line.rsplit(" ", 1)[-1] for line in console_lines] == expected
    assert [line.rsplit(" ", 1)[-1] for line in file_lines] == expected


def test_logging_setup_failure_stops_listener_and_restores_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[_DrainingQueueListener] = []
    original_listener = live_main._DrainingQueueListener

    class TrackingListener(original_listener):
        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            created.append(self)

    def fail_runtime(**_kwargs):
        raise RuntimeError("synthetic logging runtime constructor failure")

    root = logging.getLogger()
    previous_handlers = tuple(root.handlers)
    previous_level = root.level
    cfg = Config()
    cfg.logging.console = False
    cfg.logging.file = ""
    monkeypatch.setattr(live_main, "_DrainingQueueListener", TrackingListener)
    monkeypatch.setattr(live_main, "_AsyncLoggingRuntime", fail_runtime)

    with pytest.raises(
        RuntimeError,
        match="synthetic logging runtime constructor failure",
    ):
        setup_logging(cfg, queue_capacity=2)

    assert len(created) == 1
    assert created[0]._thread is None
    assert tuple(root.handlers) == previous_handlers
    assert root.level == previous_level


def test_async_logging_full_queue_records_failure_without_interrupting_caller(
    capfd: pytest.CaptureFixture[str],
) -> None:
    sink = _BlockingLogSink("one")
    logger, handler = _start_ordered_test_logger(sink, queue_capacity=1)
    producer_started = threading.Event()
    producer_returned = threading.Event()
    producer_errors: list[BaseException] = []

    def emit_third() -> None:
        producer_started.set()
        try:
            logger.info("three")
        except BaseException as exc:
            producer_errors.append(exc)
        finally:
            producer_returned.set()

    producer = threading.Thread(target=emit_third)
    try:
        logger.info("one")
        assert sink.entered.wait(timeout=1.0)
        logger.info("two")
        producer.start()
        assert producer_started.wait(timeout=1.0)
        assert producer_returned.wait(timeout=1.0)
        assert producer_errors == []
        with pytest.raises(RuntimeError, match="async logging handler failed"):
            handler.raise_if_failed()
        sink.release_event.set()
        producer.join(timeout=1.0)
        handler.stop_and_drain()
    finally:
        sink.release_event.set()
        producer.join(timeout=1.0)
        handler.stop_and_drain()
        handler.close()
        sink.close()

    assert sink.messages == ["one", "two"]
    assert producer_errors == []
    captured = capfd.readouterr()
    assert "ASYNC_LOG_QUEUE_FULL" in captured.err
    assert "capacity=1 action=fail_closed" in captured.err
    assert "message=three" in captured.err


def test_async_logging_shutdown_drains_an_already_full_queue() -> None:
    sink = _BlockingLogSink("one")
    logger, handler = _start_ordered_test_logger(sink, queue_capacity=1)
    shutdown_returned = threading.Event()

    def stop_handler() -> None:
        handler.stop_and_drain()
        shutdown_returned.set()

    shutdown = threading.Thread(target=stop_handler)
    try:
        logger.info("one")
        assert sink.entered.wait(timeout=1.0)
        logger.info("two")
        shutdown.start()
        assert not shutdown_returned.wait(timeout=0.05)
        sink.release_event.set()
        assert shutdown_returned.wait(timeout=2.0)
        shutdown.join(timeout=1.0)
    finally:
        sink.release_event.set()
        shutdown.join(timeout=1.0)
        handler.stop_and_drain()
        handler.close()
        sink.close()

    assert sink.messages == ["one", "two"]


def test_async_logging_critical_has_independent_sync_stderr_fallback(
    capfd: pytest.CaptureFixture[str],
) -> None:
    sink = _BlockingLogSink("queued-before-critical")
    logger, handler = _start_ordered_test_logger(sink, queue_capacity=2)
    critical_returned = threading.Event()

    def emit_critical() -> None:
        logger.critical("critical-fallback")
        critical_returned.set()

    producer = threading.Thread(target=emit_critical)
    try:
        logger.info("queued-before-critical")
        assert sink.entered.wait(timeout=1.0)
        producer.start()
        assert critical_returned.wait(timeout=1.0)
        captured = capfd.readouterr()
        assert "ASYNC_LOG_CRITICAL" in captured.err
        assert "message=critical-fallback" in captured.err
        sink.release_event.set()
        producer.join(timeout=1.0)
        handler.stop_and_drain()
    finally:
        sink.release_event.set()
        producer.join(timeout=1.0)
        handler.stop_and_drain()
        handler.close()
        sink.close()

    assert sink.messages == ["queued-before-critical", "critical-fallback"]
    assert sink.messages.count("critical-fallback") == 1
    assert sink.emit_thread_ids["critical-fallback"] != producer.ident


def test_async_logging_linearizes_producers_before_handler_filters() -> None:
    sink = _BlockingLogSink("never-block")
    logger, handler = _start_ordered_test_logger(sink, queue_capacity=4)
    first_in_filter = threading.Event()
    release_first = threading.Event()

    class PauseFirstFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            if record.getMessage() == "first":
                first_in_filter.set()
                assert release_first.wait(timeout=1.0)
            return True

    handler.addFilter(PauseFirstFilter())
    first = threading.Thread(target=logger.info, args=("first",))
    second = threading.Thread(target=logger.info, args=("second",))
    try:
        first.start()
        assert first_in_filter.wait(timeout=1.0)
        second.start()
        time.sleep(0.05)
        assert second.is_alive()
        release_first.set()
        first.join(timeout=1.0)
        second.join(timeout=1.0)
        handler.stop_and_drain()
    finally:
        release_first.set()
        first.join(timeout=1.0)
        second.join(timeout=1.0)
        handler.stop_and_drain()
        handler.close()
        sink.close()

    assert sink.messages == ["first", "second"]


def test_async_logging_formats_only_on_listener_thread() -> None:
    sink = _BlockingLogSink("never-block")
    logger, handler = _start_ordered_test_logger(sink, queue_capacity=4)
    producer_thread_id = threading.get_ident()
    format_thread_ids: list[int] = []

    class DeferredValue:
        def __str__(self) -> str:
            format_thread_ids.append(threading.get_ident())
            return "deferred-value"

    try:
        logger.info("value=%s", DeferredValue())
        handler.stop_and_drain()
    finally:
        handler.stop_and_drain()
        handler.close()
        sink.close()

    assert sink.messages == ["value=deferred-value"]
    assert format_thread_ids
    assert producer_thread_id not in format_thread_ids


def test_async_logging_runtime_preserves_shutdown_race(
    capfd: pytest.CaptureFixture[str],
) -> None:
    async_sink = _BlockingLogSink("queued")
    previous_sink = _BlockingLogSink("never-block")
    record_queue: queue.Queue[logging.LogRecord | None] = queue.Queue(maxsize=4)
    listener = _DrainingQueueListener(record_queue, async_sink)
    queue_handler = _OrderedQueueHandler(record_queue, listener)
    logger = logging.Logger("shutdown-race", level=logging.INFO)
    logger.addHandler(queue_handler)
    runtime = _AsyncLoggingRuntime(
        root_logger=logger,
        queue_handler=queue_handler,
        listener=listener,
        sink_handlers=(async_sink,),
        previous_handlers=(previous_sink,),
        previous_level=logging.INFO,
    )
    listener.start()
    runtime.raise_if_failed()
    assert runtime.health_snapshot()["valid"] is True
    closed = threading.Event()

    def close_runtime() -> None:
        runtime.close()
        closed.set()

    closer = threading.Thread(target=close_runtime)
    try:
        logger.info("queued")
        assert async_sink.entered.wait(timeout=1.0)
        closer.start()
        deadline = time.monotonic() + 1.0
        while queue_handler._accepting and time.monotonic() < deadline:
            time.sleep(0.001)
        assert not queue_handler._accepting
        logger.warning("during-shutdown")
        captured = capfd.readouterr()
        assert "ASYNC_LOG_AFTER_SHUTDOWN" in captured.err
        assert "message=during-shutdown" in captured.err
        async_sink.release_event.set()
        assert closed.wait(timeout=2.0)
        closer.join(timeout=1.0)
        logger.info("after-shutdown")
    finally:
        async_sink.release_event.set()
        closer.join(timeout=1.0)
        runtime.close()
        previous_sink.close()

    assert async_sink.messages == ["queued"]
    assert previous_sink.messages == ["after-shutdown"]


def test_async_logging_listener_failure_is_visible_and_stop_is_bounded(
    capfd: pytest.CaptureFixture[str],
) -> None:
    class ThrowingSink(logging.Handler):
        def __init__(self) -> None:
            super().__init__()
            self.attempted = threading.Event()

        def emit(self, record: logging.LogRecord) -> None:
            self.attempted.set()
            raise OSError(f"synthetic sink failure: {record.getMessage()}")

    sink = ThrowingSink()
    logger, handler = _start_ordered_test_logger(sink, queue_capacity=1)
    runtime = _AsyncLoggingRuntime(
        root_logger=logger,
        queue_handler=handler,
        listener=handler._listener,
        sink_handlers=(sink,),
        previous_handlers=(),
        previous_level=logging.INFO,
    )
    logger.info("explode")
    assert sink.attempted.wait(timeout=1.0)
    deadline = time.monotonic() + 1.0
    while (
        handler._listener.failure is None
        or handler._listener._thread.is_alive()
    ) and time.monotonic() < deadline:
        time.sleep(0.001)
    health = handler._listener.health_snapshot()
    assert health == {
        "valid": False,
        "worker_alive": False,
        "failure_type": "OSError",
        "failure_message": "synthetic sink failure: explode",
    }
    assert runtime.health_snapshot()["valid"] is False
    with pytest.raises(RuntimeError, match="listener worker failed"):
        runtime.raise_if_failed()
    logger.info("fail-closed-after-worker-failure")
    with pytest.raises(RuntimeError, match="handler failed"):
        runtime.raise_if_failed()

    late_record = logging.LogRecord(
        name="dead-listener-late-record",
        level=logging.INFO,
        pathname=__file__,
        lineno=0,
        msg="preserve-after-worker-failure",
        args=(),
        exc_info=None,
    )
    handler.queue.put_nowait(late_record)
    start = time.monotonic()
    with pytest.raises(RuntimeError, match="listener worker failed"):
        runtime.close()
    elapsed = time.monotonic() - start
    try:
        assert elapsed < 0.5
        assert handler.queue.empty()
        assert handler.queue.unfinished_tasks == 0
    finally:
        runtime.close()

    captured = capfd.readouterr()
    assert "ASYNC_LOG_LISTENER_FAILED" in captured.err
    assert "message=explode" in captured.err
    assert "ASYNC_LOG_LISTENER_UNDELIVERED" in captured.err
    assert "message=preserve-after-worker-failure" in captured.err
    assert "ASYNC_LOG_HANDLER_FAILED" in captured.err


def test_failed_logging_cannot_skip_fatal_or_cleanup_control_flow(
    capfd: pytest.CaptureFixture[str],
) -> None:
    class ThrowingSink(logging.Handler):
        def __init__(self) -> None:
            super().__init__()
            self.attempted = threading.Event()

        def emit(self, record: logging.LogRecord) -> None:
            self.attempted.set()
            raise OSError(f"synthetic sink failure: {record.getMessage()}")

    sink = ThrowingSink()
    logger, handler = _start_ordered_test_logger(sink, queue_capacity=2)
    actions: list[str] = []
    try:
        logger.info("break-worker")
        assert sink.attempted.wait(timeout=1.0)
        deadline = time.monotonic() + 1.0
        while handler._listener.failure is None and time.monotonic() < deadline:
            time.sleep(0.001)
        assert handler._listener.failure is not None

        # Logging after the worker fault must not become an exception edge in
        # the fatal/shutdown transaction.  Health polling is the sole edge.
        logger.critical("fatal-diagnostic")
        actions.append("fatal-latch")
        logger.info("shutdown-diagnostic")
        actions.append("engine-cleanup")
        actions.append("gateway-cleanup")
        with pytest.raises(RuntimeError, match="handler failed"):
            handler.raise_if_failed()
    finally:
        try:
            handler.stop_and_drain()
        except RuntimeError:
            pass
        handler.close()
        sink.close()

    assert actions == ["fatal-latch", "engine-cleanup", "gateway-cleanup"]
    captured = capfd.readouterr()
    assert "ASYNC_LOG_HANDLER_FAILED" in captured.err
    assert "message=shutdown-diagnostic" in captured.err


def test_safe_runtime_log_swallows_a_third_party_handler_failure(
    capfd: pytest.CaptureFixture[str],
) -> None:
    class RaisingHandler(logging.Handler):
        def emit(self, _record: logging.LogRecord) -> None:
            raise OSError("synthetic direct handler failure")

    logger = logging.Logger("safe-runtime-log-test", level=logging.INFO)
    handler = RaisingHandler()
    logger.addHandler(handler)
    try:
        _safe_runtime_log(logger, logging.CRITICAL, "fatal=%s", "synthetic")
    finally:
        handler.close()

    captured = capfd.readouterr()
    assert "RUNTIME_LOG_CALL_FAILED" in captured.err
    assert "message=fatal=synthetic" in captured.err


def test_shutdown_signal_callback_only_sets_plain_flag() -> None:
    shutdown_signal = _ShutdownSignalFlag()

    shutdown_signal(signal.SIGTERM, None)
    shutdown_signal(signal.SIGINT, None)

    assert shutdown_signal.requested is True
    assert set(_ShutdownSignalFlag.__call__.__code__.co_names) == {"requested"}


def test_runtime_fatal_cancel_precedes_and_survives_logging_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RaisingLogger:
        @staticmethod
        def critical(*_args, **_kwargs) -> None:
            raise OSError("synthetic maker logger failure")

    cancel_open_orders = Mock(return_value={})
    engine = object.__new__(MakerEngine)
    engine.cfg = SimpleNamespace(symbol="BTCUSDC")
    engine._order_transport = lambda: SimpleNamespace(
        cancel_open_orders=cancel_open_orders
    )
    engine._clear_all_replace_terminal_continuations = Mock()
    engine._running = True
    engine._runtime_fatal_error = None
    engine._runtime_reconciliation_required = False
    monkeypatch.setattr(maker_engine_module, "logger", RaisingLogger())

    root_error = RuntimeError("synthetic fatal")
    engine.latch_runtime_fatal(
        reason="TEST_FATAL",
        error=root_error,
        reconciliation_required=False,
    )

    cancel_open_orders.assert_called_once_with(symbol="BTCUSDC")
    assert engine._running is False
    assert engine._runtime_fatal_error is root_error


def test_async_logging_close_does_not_close_sink_under_live_worker(
    capfd: pytest.CaptureFixture[str],
) -> None:
    class IndefinitelyBlockingSink(logging.Handler):
        def __init__(self) -> None:
            super().__init__()
            self.entered = threading.Event()
            self.release_event = threading.Event()
            self.closed_for_test = False
            self.messages: list[str] = []

        def emit(self, record: logging.LogRecord) -> None:
            self.entered.set()
            self.release_event.wait()
            assert not self.closed_for_test
            self.messages.append(record.getMessage())

        def close(self) -> None:
            self.closed_for_test = True
            super().close()

    sink = IndefinitelyBlockingSink()
    previous_sink = _BlockingLogSink("never-block")
    record_queue: queue.Queue[logging.LogRecord | None] = queue.Queue(maxsize=2)
    listener = _DrainingQueueListener(
        record_queue,
        sink,
        stop_timeout_s=0.05,
    )
    handler = _OrderedQueueHandler(record_queue, listener)
    logger = logging.Logger("blocked-close", level=logging.INFO)
    logger.addHandler(handler)
    runtime = _AsyncLoggingRuntime(
        root_logger=logger,
        queue_handler=handler,
        listener=listener,
        sink_handlers=(sink,),
        previous_handlers=(previous_sink,),
        previous_level=logging.INFO,
    )
    listener.start()
    logger.info("blocked-record")
    assert sink.entered.wait(timeout=1.0)

    start = time.monotonic()
    with pytest.raises(RuntimeError, match="did not stop before deadline"):
        runtime.close()
    elapsed = time.monotonic() - start
    assert elapsed < 0.5
    assert listener._thread is not None
    assert listener._thread.is_alive()
    assert sink.closed_for_test is False
    assert logger.handlers == [previous_sink]
    captured = capfd.readouterr()
    assert "ASYNC_LOG_LISTENER_STOP_TIMEOUT" in captured.err
    assert "ASYNC_LOG_SINK_CLOSE_DEFERRED" in captured.err

    sink.release_event.set()
    listener._thread.join(timeout=1.0)
    assert not listener._thread.is_alive()
    runtime.close()
    previous_sink.close()

    assert sink.messages == ["blocked-record"]
    assert sink.closed_for_test is True


def test_async_logging_late_selected_producer_never_writes_closed_sink(
    capfd: pytest.CaptureFixture[str],
) -> None:
    class ClosedSink(logging.Handler):
        def __init__(self) -> None:
            super().__init__()
            self.closed_for_test = False
            self.messages = []

        def emit(self, record: logging.LogRecord) -> None:
            assert not self.closed_for_test
            self.messages.append(record.getMessage())

        def close(self) -> None:
            self.closed_for_test = True
            super().close()

    sink = ClosedSink()
    _logger, handler = _start_ordered_test_logger(sink, queue_capacity=2)
    selected = threading.Event()
    release_selected = threading.Event()

    def emit_after_selection_pause() -> None:
        record = logging.LogRecord(
            name="late-selected-producer",
            level=logging.INFO,
            pathname=__file__,
            lineno=0,
            msg="preserve-me",
            args=(),
            exc_info=None,
        )
        selected.set()
        assert release_selected.wait(timeout=1.0)
        handler.handle(record)

    producer = threading.Thread(target=emit_after_selection_pause)
    producer.start()
    assert selected.wait(timeout=1.0)
    handler.stop_and_drain()
    handler.close()
    sink.close()
    release_selected.set()
    producer.join(timeout=1.0)

    assert not producer.is_alive()
    assert sink.messages == []
    captured = capfd.readouterr()
    assert "ASYNC_LOG_AFTER_SHUTDOWN" in captured.err
    assert "message=preserve-me" in captured.err


def test_runtime_evidence_writer_preserves_fifo_and_drains_shutdown(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "evidence.csv"
    health_path = tmp_path / "runtime_health.json"
    observed_after_first_row: list[str] = []
    writer = RuntimeEvidenceWriter(queue_capacity=8)

    first = writer.enqueue_csv(csv_path, {"sequence": 1, "value": "first"})
    task = writer.enqueue_task(
        "observe_first_row",
        lambda: observed_after_first_row.append(
            csv_path.read_text(encoding="utf-8")
        ),
    )
    second = writer.enqueue_csv(csv_path, {"sequence": 2, "value": "second"})
    health = writer.enqueue_json_snapshot(
        health_path,
        {"schemaVersion": "test", "sequence": 2},
    )
    closed = writer.close(drain_timeout_s=2.0)

    assert [first, task, second, health] == [1, 2, 3, 4]
    assert observed_after_first_row == ["1,first\n"]
    with csv_path.open(newline="", encoding="utf-8") as handle:
        assert list(csv.reader(handle)) == [
            ["1", "first"],
            ["2", "second"],
        ]
    assert json.loads(health_path.read_text(encoding="utf-8")) == {
        "schemaVersion": "test",
        "sequence": 2,
    }
    assert closed["accepted_count"] == 4
    assert closed["committed_count"] == 4
    assert closed["last_committed_sequence"] == 4
    assert closed["queue_full_count"] == 0
    assert closed["valid"] is True
    assert closed["worker_alive"] is False


def test_runtime_evidence_writer_accepts_flattened_immutable_csv_rows(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "flat.csv"
    writer = RuntimeEvidenceWriter(queue_capacity=4)

    sequence = writer.enqueue_csv_values(
        csv_path,
        fieldnames=("side", "price", "quantity"),
        values=("BUY", 100.1, 0.001),
    )
    with pytest.raises(TypeError, match="immutable scalars"):
        writer.enqueue_csv_values(
            csv_path,
            fieldnames=("side", "payload", "quantity"),
            values=("BUY", {"mutable": True}, 0.001),
        )
    with pytest.raises(TypeError, match="immutable scalars"):
        writer.enqueue_csv_values(
            csv_path,
            fieldnames=("side", "payload", "quantity"),
            values=("BUY", ("nested", ["mutable"]), 0.001),
        )
    writer.close(drain_timeout_s=2.0)

    assert sequence == 1
    with csv_path.open(newline="", encoding="utf-8") as handle:
        assert list(csv.reader(handle)) == [["BUY", "100.1", "0.001"]]


def test_runtime_evidence_writer_recursively_freezes_payloads_at_admission(
    tmp_path: Path,
) -> None:
    worker_started = threading.Event()
    release_worker = threading.Event()
    writer = RuntimeEvidenceWriter(queue_capacity=4)

    def block_worker() -> None:
        worker_started.set()
        assert release_worker.wait(timeout=2.0)

    writer.enqueue_task("block", block_worker)
    assert worker_started.wait(timeout=1.0)
    payload = {"nested": {"value": 1}, "rows": [{"sequence": 1}]}
    writer.enqueue_json_snapshot(tmp_path / "health.json", payload)
    csv_payload = {
        "sequence": 1,
        "nested": ["frozen"],
        "mapping": {"value": ["frozen"]},
    }
    writer.enqueue_csv(tmp_path / "evidence.csv", csv_payload)
    payload["nested"]["value"] = 2
    payload["rows"][0]["sequence"] = 2
    csv_payload["nested"].append("mutated")
    csv_payload["mapping"]["value"].append("mutated")
    release_worker.set()
    writer.close(drain_timeout_s=2.0)

    assert json.loads((tmp_path / "health.json").read_text(encoding="utf-8")) == {
        "nested": {"value": 1},
        "rows": [{"sequence": 1}],
    }
    with (tmp_path / "evidence.csv").open(newline="", encoding="utf-8") as handle:
        assert list(csv.reader(handle)) == [
            ["1", "['frozen']", "{'value': ['frozen']}"],
        ]


def test_runtime_evidence_writer_queue_full_is_explicit_and_invalidates_health(
    tmp_path: Path,
) -> None:
    worker_started = threading.Event()
    release_worker = threading.Event()
    writer = RuntimeEvidenceWriter(queue_capacity=1)

    def block_worker() -> None:
        worker_started.set()
        assert release_worker.wait(timeout=2.0)

    writer.enqueue_task("block", block_worker)
    assert worker_started.wait(timeout=1.0)
    writer.enqueue_csv(tmp_path / "accepted.csv", {"sequence": 1})
    started = time.perf_counter()
    with pytest.raises(RuntimeEvidenceQueueFull, match="queue is full"):
        writer.enqueue_csv(tmp_path / "rejected.csv", {"sequence": 2})
    assert time.perf_counter() - started < 0.05

    health = writer.health_snapshot()
    assert health["accepting"] is False
    assert health["accepted_count"] == 2
    assert health["committed_count"] == 0
    assert health["uncommitted_count"] == 2
    assert health["queue_full_count"] == 1
    assert health["error_count"] == 1
    assert health["valid"] is False
    with pytest.raises(RuntimeEvidenceQueueFull, match="collection is invalid"):
        writer.raise_if_failed()
    with pytest.raises(RuntimeEvidenceQueueFull, match="collection is invalid"):
        writer.enqueue_csv(tmp_path / "after_full.csv", {"sequence": 3})
    release_worker.set()
    with pytest.raises(
        RuntimeEvidenceWriterError,
        match=r"accepted=2 committed=2 uncommitted=0",
    ):
        writer.close(drain_timeout_s=2.0)
    final_health = writer.health_snapshot()
    assert final_health["accepted_count"] == 2
    assert final_health["committed_count"] == 2
    assert final_health["uncommitted_count"] == 0


def test_runtime_evidence_writer_barrier_has_committed_admission_boundary(
    tmp_path: Path,
) -> None:
    observed: list[dict[str, object]] = []
    writer = RuntimeEvidenceWriter(queue_capacity=4)

    writer.enqueue_task("observe-admission", lambda: observed.append(writer.health_snapshot()))
    writer.enqueue_csv(tmp_path / "before-barrier.csv", {"sequence": 2})
    barrier_health = writer.barrier(timeout_s=2.0)

    assert observed[0]["accepted_count"] >= 1
    assert observed[0]["uncommitted_count"] >= 1
    assert int(barrier_health["last_committed_sequence"]) == 3
    assert int(barrier_health["accepted_count"]) == 3
    assert int(barrier_health["committed_count"]) == 3
    assert int(barrier_health["uncommitted_count"]) == 0
    writer.close(drain_timeout_s=2.0)


def test_runtime_evidence_writer_orders_concurrent_producers_by_admission(
    tmp_path: Path,
) -> None:
    paths = (tmp_path / "stream-a.csv", tmp_path / "stream-b.csv")
    writer = RuntimeEvidenceWriter(queue_capacity=256)
    admitted: dict[Path, list[tuple[int, str]]] = {path: [] for path in paths}
    admitted_lock = threading.Lock()

    def produce(prefix: str) -> None:
        for index in range(50):
            label = f"{prefix}-{index}"
            path = paths[(int(prefix) + index) % len(paths)]
            sequence = writer.enqueue_csv(path, {"label": label})
            with admitted_lock:
                admitted[path].append((sequence, label))

    threads = [
        threading.Thread(target=produce, args=(str(index),))
        for index in range(4)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    closed = writer.close(drain_timeout_s=2.0)

    for path in paths:
        expected = [label for _sequence, label in sorted(admitted[path])]
        actual = path.read_text(encoding="utf-8").splitlines()
        assert actual == expected
    assert closed["accepted_count"] == 200
    assert closed["committed_count"] == 200
    assert closed["uncommitted_count"] == 0


def test_runtime_evidence_writer_worker_failure_reports_commit_boundary(
    tmp_path: Path,
) -> None:
    writer = RuntimeEvidenceWriter(queue_capacity=8)
    first_task_ran = threading.Event()
    writer.enqueue_task("first", first_task_ran.set)
    assert first_task_ran.wait(timeout=1.0)
    deadline = time.monotonic() + 1.0
    while writer.health_snapshot()["committed_count"] < 1:
        assert time.monotonic() < deadline
        time.sleep(0.001)

    failure_started = threading.Event()
    release_failure = threading.Event()

    def fail() -> None:
        failure_started.set()
        assert release_failure.wait(timeout=2.0)
        raise OSError("simulated writer failure")

    writer.enqueue_task("fail", fail)
    assert failure_started.wait(timeout=1.0)
    writer.enqueue_csv(tmp_path / "uncommitted.csv", {"sequence": 3})
    writer.enqueue_json_snapshot(tmp_path / "uncommitted.json", {"sequence": 4})
    before_failure = writer.health_snapshot()
    assert before_failure["accepted_count"] == 4
    assert before_failure["committed_count"] == 1
    assert before_failure["uncommitted_count"] == 3
    release_failure.set()
    deadline = time.monotonic() + 1.0
    while not writer.health_snapshot()["fatal_error"] and time.monotonic() < deadline:
        time.sleep(0.001)

    with pytest.raises(
        RuntimeEvidenceWorkerFailed,
        match=r"accepted=4 committed=1 uncommitted=3",
    ):
        writer.raise_if_failed()
    health = writer.health_snapshot()
    assert health["worker_alive"] is False
    assert health["accepted_count"] == 4
    assert health["committed_count"] == 1
    assert health["uncommitted_count"] == 3
    assert health["last_committed_sequence"] == 1
    assert health["error_count"] == 1
    assert health["valid"] is False
    with pytest.raises(
        RuntimeEvidenceWriterError,
        match=r"accepted=4 committed=1 uncommitted=3",
    ):
        writer.close(drain_timeout_s=1.0)


def test_runtime_health_factory_collects_on_writer_worker(
    tmp_path: Path,
) -> None:
    main_thread_id = threading.get_ident()
    worker_started = threading.Event()
    release_worker = threading.Event()
    collection_calls: list[tuple[int, int]] = []
    state = {"generation": 1}

    def quote_safety(**_kwargs):
        collection_calls.append((threading.get_ident(), state["generation"]))
        return {
            "quote_loop_running": True,
            "ownership_conflict_latched": False,
            "fatal_runtime_latched": False,
            "reconciliation_required": False,
            "reconciliation_pending": False,
            "fatal_runtime_reason": "",
            "last_tick_age_s": 0.1,
            "replace_terminal_continuation": {},
        }

    engine = SimpleNamespace(runtime_safety_snapshot=quote_safety)
    ws = SimpleNamespace(
        user_event_safety_snapshot=lambda **_kwargs: {
            "last_user_event_age_s": 0.2,
            "user_event_count": 3,
            "user_stream_connected": True,
            "user_stream_generation": state["generation"],
        }
    )
    path = tmp_path / "runtime_health.json"
    writer = RuntimeEvidenceWriter(queue_capacity=4)

    def block_worker() -> None:
        worker_started.set()
        assert release_worker.wait(timeout=2.0)

    writer.enqueue_task("block", block_worker)
    assert worker_started.wait(timeout=1.0)
    writer.enqueue_json_snapshot_factory(
        path,
        runtime_safety_health_payload_factory(engine=engine, ws=ws),
    )
    state["generation"] = 9
    release_worker.set()
    closed = writer.close(drain_timeout_s=2.0)

    assert len(collection_calls) == 1
    assert collection_calls[0][1] == 9
    assert collection_calls[0][0] != main_thread_id
    assert json.loads(path.read_text(encoding="utf-8"))[
        "userStreamGeneration"
    ] == 9
    assert closed["json_snapshots_committed"] == 1


def test_poisoned_evidence_writer_publishes_truthful_final_health_directly(
    tmp_path: Path,
) -> None:
    cfg = Config()
    cfg.logging.file = str(tmp_path / "maker.log")
    health_path = tmp_path / "runtime_health.json"
    health_path.write_text(
        json.dumps(
            {
                "schemaVersion": "narrowgate.live_runtime_health.v1",
                "fatalRuntimeLatched": False,
                "runtimeEvidenceWriterValid": True,
            }
        ),
        encoding="utf-8",
    )
    writer = RuntimeEvidenceWriter(queue_capacity=4)

    def poison_worker() -> None:
        raise RuntimeError("simulated lifecycle rejection")

    writer.enqueue_task("poison", poison_worker)
    deadline = time.monotonic() + 1.0
    while not writer.health_snapshot()["fatal_error"]:
        assert time.monotonic() < deadline
        time.sleep(0.001)

    engine = SimpleNamespace(
        runtime_safety_snapshot=lambda **_kwargs: {
            "quote_loop_running": False,
            "ownership_conflict_latched": False,
            "fatal_runtime_latched": True,
            "reconciliation_required": False,
            "reconciliation_pending": False,
            "fatal_runtime_reason": "UNCAUGHT_LIVE_FATAL",
            "last_tick_age_s": 0.0,
            "replace_terminal_continuation": {},
        },
        runtime_evidence_writer_health_snapshot=lambda: {
            "enabled": True,
            **writer.health_snapshot(),
        },
    )
    ws = SimpleNamespace(
        user_event_safety_snapshot=lambda **_kwargs: {
            "last_user_event_age_s": 0.0,
            "user_event_count": 1,
            "user_stream_connected": False,
            "user_stream_generation": 1,
        }
    )
    gateway = SimpleNamespace(
        health_snapshot=lambda: {
            "active_transport": "websocket_api",
            "shutdown_complete": True,
            "websocket_api": {"enabled": True},
        }
    )
    cleanup_errors: list[BaseException] = []

    result = _close_runtime_evidence_writer_with_final_health(
        cfg=cfg,
        runtime_evidence_writer=writer,
        engine=engine,
        ws=ws,
        order_gateway=gateway,
        gc_pause_monitor=None,
        logging_runtime=None,
        cleanup_errors=cleanup_errors,
    )

    payload = json.loads(health_path.read_text(encoding="utf-8"))
    assert result is None
    assert cleanup_errors
    assert payload["fatalRuntimeLatched"] is True
    assert payload["fatalReason"] == "UNCAUGHT_LIVE_FATAL"
    assert payload["runtimeEvidenceWriterValid"] is False
    assert "simulated lifecycle rejection" in payload[
        "runtimeEvidenceWriterFatalError"
    ]
    assert stat.S_IMODE(health_path.stat().st_mode) == 0o600


def test_maker_engine_ordinary_evidence_uses_single_async_writer(
    tmp_path: Path,
) -> None:
    @dataclass(frozen=True)
    class _Row:
        sequence: int
        value: str

    path = tmp_path / "quote_decisions.csv"
    writer = RuntimeEvidenceWriter(queue_capacity=4)
    engine = object.__new__(MakerEngine)
    engine._runtime_evidence_writer = writer

    engine._append_row(str(path), _Row(sequence=1, value="first"))
    engine._append_row(str(path), _Row(sequence=2, value="second"))
    closed = writer.close(drain_timeout_s=2.0)

    assert path.read_text(encoding="utf-8") == "1,first\n2,second\n"
    assert closed["csv_rows_committed"] == 2


def test_maker_engine_evidence_admission_failure_propagates() -> None:
    @dataclass(frozen=True)
    class _Row:
        sequence: int

    class _FullWriter:
        @staticmethod
        def enqueue_csv(_path, _payload) -> None:
            raise RuntimeEvidenceQueueFull("simulated full queue")

    engine = object.__new__(MakerEngine)
    engine._runtime_evidence_writer = _FullWriter()

    with pytest.raises(RuntimeEvidenceQueueFull, match="simulated full queue"):
        engine._append_row("evidence.csv", _Row(sequence=1))


def test_exact_opportunity_rejection_fails_central_worker_off_callback() -> None:
    worker_started = threading.Event()
    release_worker = threading.Event()
    writer = RuntimeEvidenceWriter(queue_capacity=4)

    def block_worker() -> None:
        worker_started.set()
        assert release_worker.wait(timeout=2.0)

    class _RejectingExactWriter:
        def __init__(self) -> None:
            self.payloads: list[dict[str, object]] = []

        def append(self, payload) -> bool:
            self.payloads.append(dict(payload))
            return False

    writer.enqueue_task("block", block_worker)
    assert worker_started.wait(timeout=1.0)
    exact = _RejectingExactWriter()
    engine = object.__new__(MakerEngine)
    engine._runtime_evidence_writer = writer
    engine._exact_opportunity_tape_runtime = exact
    payload: dict[str, object] = {"sequence": 1, "nested": {"value": 1}}

    engine._append_exact_opportunity_payload(payload)
    payload["nested"]["value"] = 9
    assert exact.payloads == []
    release_worker.set()
    deadline = time.monotonic() + 1.0
    while not writer.health_snapshot()["fatal_error"]:
        assert time.monotonic() < deadline
        time.sleep(0.001)

    assert exact.payloads == [{"sequence": 1, "nested": {"value": 1}}]
    with pytest.raises(RuntimeEvidenceWorkerFailed, match="rejected a frozen payload"):
        writer.raise_if_failed()
    with pytest.raises(RuntimeEvidenceWriterError):
        writer.close(drain_timeout_s=1.0)


def test_lifecycle_queue_full_does_not_unwind_callback_before_safety() -> None:
    worker_started = threading.Event()
    release_worker = threading.Event()
    writer = RuntimeEvidenceWriter(queue_capacity=1)

    def block_worker() -> None:
        worker_started.set()
        assert release_worker.wait(timeout=2.0)

    class _LifecycleWriter:
        enqueued = False

        @staticmethod
        def freeze_order_event(_order, source_event_type, raw_event):
            return (str(source_event_type), copy.deepcopy(dict(raw_event)))

        def enqueue_frozen_order_event(self, _event) -> bool:
            self.enqueued = True
            return True

    writer.enqueue_task("block", block_worker)
    assert worker_started.wait(timeout=1.0)
    writer.enqueue_task("occupy-capacity", lambda: None)
    lifecycle_writer = _LifecycleWriter()
    engine = object.__new__(MakerEngine)
    engine._runtime_evidence_writer = writer
    engine._order_lifecycle_live_writer_v2 = lifecycle_writer
    order = SimpleNamespace(lifecycle=object())

    # Admission fails immediately, but the callback stack is allowed to reach
    # the exchange-risk cancellation that follows lifecycle observation.
    engine._record_order_lifecycle_journal(
        order,
        "partial_fill",
        {"_fill_qty": 0.001},
    )
    assert lifecycle_writer.enqueued is False
    with pytest.raises(RuntimeEvidenceQueueFull):
        writer.raise_if_failed()
    release_worker.set()
    with pytest.raises(RuntimeEvidenceWriterError):
        writer.close(drain_timeout_s=1.0)


def test_lifecycle_rejection_fails_central_worker_after_frozen_admission() -> None:
    writer = RuntimeEvidenceWriter(queue_capacity=4)

    class _RejectingLifecycleWriter:
        @staticmethod
        def freeze_order_event(_order, source_event_type, raw_event):
            return (str(source_event_type), copy.deepcopy(dict(raw_event)))

        @staticmethod
        def enqueue_frozen_order_event(_event) -> bool:
            return False

    engine = object.__new__(MakerEngine)
    engine._runtime_evidence_writer = writer
    engine._order_lifecycle_live_writer_v2 = _RejectingLifecycleWriter()
    order = SimpleNamespace(lifecycle=object())

    engine._record_order_lifecycle_journal(order, "rest_ack", {"generation": 1})
    deadline = time.monotonic() + 1.0
    while not writer.health_snapshot()["fatal_error"]:
        assert time.monotonic() < deadline
        time.sleep(0.001)

    with pytest.raises(RuntimeEvidenceWorkerFailed, match="rejected a frozen callback"):
        writer.raise_if_failed()
    with pytest.raises(RuntimeEvidenceWriterError):
        writer.close(drain_timeout_s=1.0)


def test_central_writer_commits_specialized_items_directly_in_fifo_order() -> None:
    calls: list[tuple[str, object]] = []
    writer = RuntimeEvidenceWriter(queue_capacity=8)

    class _LifecycleWriter:
        @staticmethod
        def freeze_order_event(_order, source_event_type, raw_event):
            return (str(source_event_type), copy.deepcopy(dict(raw_event)))

        @staticmethod
        def enqueue_frozen_order_event(_event) -> bool:
            raise AssertionError("secondary lifecycle queue was used")

        @staticmethod
        def commit_frozen_order_event(event) -> bool:
            calls.append(("lifecycle", event))
            return True

    class _ExactWriter:
        @staticmethod
        def append(_payload) -> bool:
            raise AssertionError("secondary exact-opportunity queue was used")

        @staticmethod
        def commit_frozen(payload) -> bool:
            calls.append(("exact", copy.deepcopy(dict(payload))))
            return True

    engine = object.__new__(MakerEngine)
    engine._runtime_evidence_writer = writer
    engine._order_lifecycle_live_writer_v2 = _LifecycleWriter()
    engine._exact_opportunity_tape_runtime = _ExactWriter()
    order = SimpleNamespace(lifecycle=object())

    engine._record_order_lifecycle_journal(order, "submit", {"sequence": 1})
    engine._append_exact_opportunity_payload({"sequence": 2})
    writer.close(drain_timeout_s=1.0)

    assert calls == [
        ("lifecycle", ("submit", {"sequence": 1})),
        ("exact", {"sequence": 2}),
    ]


def test_inventory_trade_rows_share_runtime_evidence_writer(
    tmp_path: Path,
) -> None:
    path = tmp_path / "trades.csv"
    inventory = InventoryManager(trade_log_path=str(path))
    inventory._qty = 0.001
    inventory._avg_entry = 100_000.0
    inventory._realized_pnl = -0.25
    inventory._unrealized_pnl = 0.10
    inventory._state = PositionState.OPEN
    writer = RuntimeEvidenceWriter(queue_capacity=4)
    inventory.set_runtime_evidence_writer(writer)

    inventory._log_trade(
        1_750_000_000.125,
        "BUY",
        "OPEN",
        0.001,
        100_000.0,
        0.0,
    )
    writer.close(drain_timeout_s=2.0)

    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.reader(handle))
    assert rows[0] == [
        "timestamp",
        "side",
        "trade_type",
        "qty",
        "price",
        "commission",
        "position",
        "avg_entry",
        "realized_pnl",
        "unrealized_pnl",
        "state",
    ]
    assert rows[1] == [
        "1750000000.125",
        "BUY",
        "OPEN",
        "0.0010",
        "100000.0",
        "0.0000",
        "+0.0010",
        "100000.0",
        "-0.25",
        "0.10",
        "OPEN",
    ]


def _q90_action_config() -> Config:
    cfg = Config()
    cfg.websocket.deep_book_enabled = True
    cfg.strategy.dynamic_fill_hazard_shadow_enabled = True
    cfg.strategy.dynamic_fill_hazard_shadow_model_path = "model.json"
    cfg.strategy.dynamic_fill_hazard_shadow_model_sha256 = "a" * 64
    cfg.strategy.dynamic_fill_hazard_shadow_sides = "BUY"
    cfg.strategy.dynamic_fill_hazard_action_enabled = True
    cfg.strategy.dynamic_fill_hazard_action_policy_path = "policy.json"
    cfg.strategy.dynamic_fill_hazard_action_policy_sha256 = "b" * 64
    return cfg


@pytest.mark.parametrize(
    ("section", "field"),
    (
        ("strategy", "markout_horizon_s"),
        ("risk", "pnl_volatility_horizon_s"),
        ("risk", "max_exec_book_visible_age_s"),
        ("risk", "max_exec_book_source_lag_s"),
    ),
)
def test_duration_contracts_reject_nonfinite_values(
    section: str,
    field: str,
) -> None:
    cfg = Config()
    setattr(getattr(cfg, section), field, float("nan"))

    with pytest.raises(ValueError, match="positive and finite"):
        _validate_config(cfg)


def test_side_bbo_floor_rejects_later_inward_spread_compression() -> None:
    cfg = Config()
    cfg.strategy.historical_p3_scalar_adapter_enabled = False
    cfg.strategy.p3_side_bbo_floor_enabled = True
    cfg.strategy.spread_cap_mode = "compress"

    with pytest.raises(ValueError, match="side-BBO floor cannot be combined"):
        _validate_config(cfg)


@pytest.mark.parametrize("policy, field, obsolete_env", POLICY_CASES)
def test_obsolete_environment_flags_cannot_approve_a_policy(
    monkeypatch, policy, field, obsolete_env,
):
    for _, _, env_name in POLICY_CASES:
        monkeypatch.setenv(env_name, "1")
    assert live_main.os.environ[obsolete_env] == "1"
    with pytest.raises(ValueError, match=f"does not approve enabled policy: {policy}"):
        admit_runtime_policies({field: True}, deployment_authority={"policy_approvals": []})
    with pytest.raises(ValueError, match=f"does not approve enabled policy: {policy}"):
        admit_runtime_policies(
            {field: True},
            deployment_authority={
                "policy_approvals": [name for name, _, _ in POLICY_CASES if name != policy],
            },
        )


@pytest.mark.parametrize("policy, field, obsolete_env", POLICY_CASES)
def test_verified_policy_admission_ignores_research_annotations(
    monkeypatch, policy, field, obsolete_env,
):
    monkeypatch.delenv(obsolete_env, raising=False)
    strategy = {field: True}
    authority = {"policy_approvals": [name for name, _, _ in POLICY_CASES]}
    expected = {"approved_policies": [policy], "authorization_source": "deployment_envelope"}
    assert admit_runtime_policies(strategy, deployment_authority=authority) == expected
    strategy.update({
        "boolean_cooldown_evidence_route": "arbitrary_research_annotation",
        "buy_e3_cooldown_evidence_route": "not_an_authorization_channel",
        "research_supported": False,
        "hard_gates_passed": False,
    })
    authority.update({"research_verdict": "closed", "evidence_route": "diagnostic_only"})
    monkeypatch.setenv(obsolete_env, "not-an-approval")
    assert admit_runtime_policies(strategy, deployment_authority=authority) == expected


def test_disabled_policies_do_not_acquire_runtime_action_approval():
    for authority in (
        {"policy_approvals": []},
        {"policy_approvals": [name for name, _, _ in POLICY_CASES]},
    ):
        assert admit_runtime_policies({}, deployment_authority=authority) == {
            "approved_policies": [], "authorization_source": "deployment_envelope",
        }


def test_direct_runtime_config_validation_does_not_grant_q90_approval(monkeypatch):
    monkeypatch.delenv("NARROWGATE_ALLOW_Q90_PRIVATE_DEPLOY", raising=False)
    cfg = _q90_action_config()
    _validate_config(cfg)
    with pytest.raises(ValueError, match="does not approve enabled policy"):
        admit_runtime_policies(vars(cfg.strategy), deployment_authority={"policy_approvals": []})


def test_admitted_policy_identity_is_persisted_privately(tmp_path: Path) -> None:
    policy = admit_runtime_policies(
        {"dynamic_fill_hazard_action_enabled": True},
        deployment_authority={"policy_approvals": ["q90_action"]},
    )
    path = tmp_path / "runtime_identity.json"
    write_runtime_identity(path, policy)

    persisted = json.loads(path.read_text(encoding="utf-8"))
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert persisted == policy


def test_runtime_identity_rejects_symlink_destination(tmp_path: Path) -> None:
    target = tmp_path / "target.json"
    target.write_text("sentinel\n", encoding="utf-8")
    destination = tmp_path / "runtime_identity.json"
    destination.symlink_to(target)

    with pytest.raises(ValueError, match="must not be a symlink"):
        write_runtime_identity(destination, {"trusted": True})

    assert target.read_text(encoding="utf-8") == "sentinel\n"


def test_run_sh_preflights_before_background_launch() -> None:
    script = (ROOT / "live/run.sh").read_text(encoding="utf-8")
    supervisor = script.split("supervise() {", 1)[1].split(
        "\n_require_quiescent_maker() {", 1
    )[0]
    launch_body = script.split("_launch_manual_supervisor() {", 1)[1].split(
        "\nstart() {", 1
    )[0]
    start_body = script.split("start() {", 1)[1].split("\nstop() {", 1)[0]
    restart_body = script.split("restart() {", 1)[1].split("\nstatus() {", 1)[0]

    assert "_run_deploy_preflight" in supervisor
    assert "scripts/preflight_live_deploy.py" in script
    assert "nohup " in launch_body
    assert "_run_deploy_preflight" not in start_body
    assert supervisor.index("_run_deploy_preflight") < supervisor.index(
        '"$PYTHON_BIN" -I -B "$MAIN_PY"'
    )
    assert '[[ ! -s "$CHILD_PID_FILE" ]]' in launch_body
    assert restart_body.index("_run_deploy_preflight") < restart_body.index("\n    stop\n")
    assert "stop ||" not in restart_body
    assert restart_body.index("\n    stop\n") < restart_body.index(
        "_launch_manual_supervisor"
    )
    assert restart_body.count("_run_deploy_preflight") == 1


def test_rest_client_applies_one_finite_timeout_to_every_sync_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import binance.um_futures

    captured = {}

    class FakeUMFutures:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def sign_request(self, method, path, params):
            captured["position_request"] = (method, path, params)
            return []

    monkeypatch.setattr(binance.um_futures, "UMFutures", FakeUMFutures)
    cfg = Config()
    cfg.api.timeout_s = 2.5

    client = create_rest_client(cfg)

    assert captured["timeout"] == pytest.approx(2.5)
    assert client.get_position_risk(symbol="BTCUSDC") == []
    assert captured["position_request"] == (
        "GET",
        "/fapi/v2/positionRisk",
        {"symbol": "BTCUSDC"},
    )


def test_live_rest_roles_bind_complete_position_query_only_to_reconciliation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import live.main as live_main

    captured = {}

    class Client:
        def sign_request(self, method, path, params):
            captured["position_request"] = (method, path, params)
            return []

    clients = SimpleNamespace(
        order=Client(),
        order_buy=Client(),
        order_sell=Client(),
        order_safety=Client(),
        reconciliation=Client(),
        reconciliation_worker=Client(),
        market_snapshot=Client(),
        metrics=Client(),
        listen_key=Client(),
    )

    def factory(**kwargs):
        captured["factory"] = kwargs
        return clients

    monkeypatch.setattr(live_main, "create_binance_usdm_rest_clients", factory)
    cfg = Config()
    cfg.api.testnet = True
    cfg.api.timeout_s = 2.5

    result = create_rest_clients(cfg)

    assert result is clients
    assert callable(clients.reconciliation_worker.get_position_risk)
    assert captured["factory"]["base_url"] == "https://demo-fapi.binance.com"
    assert captured["factory"]["timeout_s"] == pytest.approx(2.5)
    assert not hasattr(clients.order, "get_position_risk")
    assert clients.reconciliation.get_position_risk(symbol="BTCUSDC") == []
    assert captured["position_request"] == (
        "GET",
        "/fapi/v2/positionRisk",
        {"symbol": "BTCUSDC"},
    )


def test_websocket_order_ab_config_is_default_off_and_bounded():
    cfg = Config()
    _validate_config(cfg)
    assert cfg.api.order_transport == "rest"
    assert cfg.api.websocket_order_ab.max_runtime_s == pytest.approx(900.0)
    assert cfg.api.websocket_order_ab.url == (
        "wss://testnet.binancefuture.com/ws-fapi/v1"
    )
    assert create_websocket_order_ab_gateway(cfg) is None


def test_websocket_order_ab_config_rejects_endpoint_environment_mismatch():
    cfg = Config()
    cfg.api.testnet = True
    cfg.api.order_transport = "websocket_api_ab"
    cfg.api.websocket_order_ab.url = "wss://ws-fapi.binance.com/ws-fapi/v1"

    with pytest.raises(ValueError, match="matching api.testnet"):
        _validate_config(cfg)


def test_websocket_order_ab_runtime_guard_preconnects_and_arms_hard_stop():
    class Gateway:
        def __init__(self):
            self.starts = 0

        def start(self):
            self.starts += 1

    class FakeTimer:
        def __init__(self, interval, callback):
            self.interval = interval
            self.callback = callback
            self.daemon = False
            self.started = False
            self.canceled = False

        def start(self):
            self.started = True

        def cancel(self):
            self.canceled = True

    gateway = Gateway()
    expirations = []
    timer = arm_websocket_order_ab_runtime_guard(
        gateway=gateway,
        max_runtime_s=900.0,
        on_expire=lambda: expirations.append("expired"),
        timer_factory=FakeTimer,
    )

    assert gateway.starts == 1
    assert timer.interval == pytest.approx(900.0)
    assert timer.daemon is True
    assert timer.started is True
    timer.callback()
    assert expirations == ["expired"]


def test_combined_order_gateway_guard_uses_earliest_deadline():
    class Gateway:
        def __init__(self):
            self.starts = 0

        def start(self):
            self.starts += 1

    class FakeTimer:
        def __init__(self, interval, callback):
            self.interval = interval
            self.callback = callback
            self.daemon = False
            self.started = False

        def start(self):
            self.started = True

    gateway = Gateway()
    expirations = []
    result = arm_order_gateway_experiment_runtime_guard(
        websocket_gateway=gateway,
        websocket_max_runtime_s=900.0,
        async_order_lanes_enabled=True,
        async_order_lane_max_runtime_s=300.0,
        on_expire=lambda arms, seconds: expirations.append((arms, seconds)),
        timer_factory=FakeTimer,
    )

    assert result is not None
    timer, runtime_s, limiting_arms = result
    assert gateway.starts == 1
    assert runtime_s == pytest.approx(300.0)
    assert limiting_arms == ("async_order_lanes",)
    assert timer.interval == pytest.approx(300.0)
    assert timer.daemon is True
    assert timer.started is True
    timer.callback()
    assert expirations == [(('async_order_lanes',), 300.0)]


def test_order_gateway_guard_reuses_transport_absolute_deadline() -> None:
    class FakeTimer:
        def __init__(self, interval, callback):
            self.interval = interval
            self.callback = callback
            self.daemon = False
            self.started = False

        def start(self):
            self.started = True

    expirations = []
    result = arm_order_gateway_experiment_runtime_guard(
        websocket_gateway=None,
        websocket_max_runtime_s=0.0,
        async_order_lanes_enabled=True,
        async_order_lane_max_runtime_s=900.0,
        async_order_lane_deadline_monotonic=1_450.0,
        on_expire=lambda arms, seconds: expirations.append((arms, seconds)),
        timer_factory=FakeTimer,
        monotonic=lambda: 1_000.0,
    )

    assert result is not None
    timer, runtime_s, limiting_arms = result
    assert runtime_s == pytest.approx(450.0)
    assert timer.interval == pytest.approx(450.0)
    assert limiting_arms == ("async_order_lanes",)
    assert timer.started is True
    timer.callback()
    assert expirations == [(('async_order_lanes',), 450.0)]


def test_async_only_order_gateway_guard_arms_active_shutdown_timer():
    class FakeTimer:
        def __init__(self, interval, callback):
            self.interval = interval
            self.callback = callback
            self.daemon = False
            self.started = False

        def start(self):
            self.started = True

    expirations = []
    result = arm_order_gateway_experiment_runtime_guard(
        websocket_gateway=None,
        websocket_max_runtime_s=0.0,
        async_order_lanes_enabled=True,
        async_order_lane_max_runtime_s=900.0,
        on_expire=lambda arms, seconds: expirations.append((arms, seconds)),
        timer_factory=FakeTimer,
    )

    assert result is not None
    timer, runtime_s, limiting_arms = result
    assert runtime_s == pytest.approx(900.0)
    assert limiting_arms == ("async_order_lanes",)
    assert timer.started is True
    timer.callback()
    assert expirations == [(('async_order_lanes',), 900.0)]


def test_live_start_routes_snapshot_and_listen_key_clients_independently(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import live.main as live_main

    rest = object()
    market_snapshot_client = object()
    listen_key_client = object()
    engine = SimpleNamespace(
        start=Mock(),
        signal=SimpleNamespace(start_metrics_polling=Mock()),
        sync_position=Mock(),
        reconcile_fill_cooldown_checkpoint_gap=Mock(
            return_value={"mode": "cursor_current", "recovered_fill_count": 0}
        ),
    )
    ws = SimpleNamespace(
        start_private_user_stream=Mock(),
        start_public_market_streams=Mock(),
        wait_for_user_stream_ready=Mock(return_value=True),
        hold_user_event_callbacks=Mock(side_effect=nullcontext),
        user_event_safety_snapshot=Mock(
            return_value={
                "user_event_count": 0,
                "user_stream_connected": True,
                "user_stream_generation": 1,
            }
        ),
    )
    cfg = SimpleNamespace(symbol="BTCUSDC")
    monkeypatch.setattr(
        live_main,
        "_initial_exchange_open_orders",
        lambda *_args, **_kwargs: [],
    )
    startup_order = []
    ws.start_private_user_stream.side_effect = (
        lambda *_args, **_kwargs: startup_order.append("private")
    )
    ws.start_public_market_streams.side_effect = (
        lambda *_args, **_kwargs: startup_order.append("public")
    )

    def initialize_collection(**_kwargs):
        assert not ws.start_public_market_streams.called
        startup_order.append("epoch")
        return None, None

    monkeypatch.setattr(
        live_main,
        "initialize_prospective_lifecycle_collection",
        initialize_collection,
    )

    start_engine_with_prospective_collection(
        cfg=cfg,
        engine=engine,
        ws=ws,
        rest=rest,
        config_path=tmp_path / "config.yaml",
        native_runtime={},
        safety_authority={},
        dry_run=False,
        market_snapshot_client=market_snapshot_client,
        listen_key_client=listen_key_client,
    )

    engine.start.assert_called_once_with()
    assert engine.sync_position.call_count == 2
    engine.sync_position.assert_called_with(required=True)
    engine.reconcile_fill_cooldown_checkpoint_gap.assert_called_once_with()
    ws.start_private_user_stream.assert_called_once_with(
        rest,
        listen_key_client=listen_key_client,
    )
    ws.start_public_market_streams.assert_called_once_with(
        rest,
        market_snapshot_client=market_snapshot_client,
        expected_user_stream_generation=1,
    )
    assert startup_order == ["private", "epoch", "public"]
    ws.wait_for_user_stream_ready.assert_called_once()
    ready_timeout = float(ws.wait_for_user_stream_ready.call_args.args[0])
    assert 0.0 < ready_timeout <= live_main.STARTUP_USER_STREAM_READY_TIMEOUT_S
    engine.signal.start_metrics_polling.assert_called_once_with()


def test_live_start_attaches_epoch_before_releasing_waiting_private_callback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import live.main as live_main

    writer_attached = threading.Event()
    callback_observations = []
    orders = SimpleNamespace(
        on_order_update=Mock(
            side_effect=lambda _payload: callback_observations.append(
                writer_attached.is_set()
            )
        )
    )
    engine = SimpleNamespace(
        orders=orders,
        start=Mock(),
        signal=SimpleNamespace(start_metrics_polling=Mock()),
        sync_position=Mock(),
        reconcile_fill_cooldown_checkpoint_gap=Mock(
            return_value={"mode": "cursor_current", "recovered_fill_count": 0}
        ),
    )
    ws = WSHandler(engine, Config())
    ws_app = object()
    with ws._user_event_stats_lock:
        ws._running = True
        ws._user_stream_active = True
        ws._private_user_stream_started = True
        ws._ws_user = ws_app
        ws._user_stream_session_token = 13
        ws._user_stream_connected = True
        ws._user_stream_generation = 4
    monkeypatch.setattr(ws, "start_private_user_stream", Mock())
    monkeypatch.setattr(ws, "wait_for_user_stream_ready", Mock(return_value=True))
    monkeypatch.setattr(ws, "start_public_market_streams", Mock())
    monkeypatch.setattr(
        live_main,
        "_initial_exchange_open_orders",
        lambda *_args, **_kwargs: [],
    )
    callback_thread = None

    def initialize_collection(**_kwargs):
        nonlocal callback_thread
        callback_thread = threading.Thread(
            target=ws._on_user_message,
            args=(
                ws_app,
                {"e": "ORDER_TRADE_UPDATE", "o": {"c": "cid-epoch"}},
                13,
            ),
        )
        callback_thread.start()
        time.sleep(0.02)
        assert callback_thread.is_alive()
        orders.on_order_update.assert_not_called()
        writer_attached.set()
        return object(), object()

    monkeypatch.setattr(
        live_main,
        "initialize_prospective_lifecycle_collection",
        initialize_collection,
    )

    start_engine_with_prospective_collection(
        cfg=SimpleNamespace(symbol="BTCUSDC"),
        engine=engine,
        ws=ws,
        rest=object(),
        config_path=tmp_path / "config.yaml",
        native_runtime={},
        safety_authority={},
        dry_run=False,
    )

    assert callback_thread is not None
    callback_thread.join(timeout=1.0)
    assert callback_observations == [True]
    ws.start_public_market_streams.assert_called_once()


def test_live_start_fails_closed_before_quote_admission_without_user_stream(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import live.main as live_main

    engine = SimpleNamespace(
        start=Mock(),
        signal=SimpleNamespace(start_metrics_polling=Mock()),
        sync_position=Mock(),
        reconcile_fill_cooldown_checkpoint_gap=Mock(
            return_value={"mode": "cursor_current", "recovered_fill_count": 0}
        ),
    )
    ws = SimpleNamespace(
        start_private_user_stream=Mock(),
        start_public_market_streams=Mock(),
        wait_for_user_stream_ready=Mock(return_value=False),
        hold_user_event_callbacks=Mock(side_effect=nullcontext),
        user_event_safety_snapshot=Mock(),
    )
    monkeypatch.setattr(
        live_main,
        "_initial_exchange_open_orders",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        live_main,
        "initialize_prospective_lifecycle_collection",
        lambda **_kwargs: (None, None),
    )

    with pytest.raises(RuntimeError, match="user stream did not become ready"):
        start_engine_with_prospective_collection(
            cfg=SimpleNamespace(symbol="BTCUSDC"),
            engine=engine,
            ws=ws,
            rest=object(),
            config_path=tmp_path / "config.yaml",
            native_runtime={},
            safety_authority={},
            dry_run=False,
        )

    engine.sync_position.assert_called_once_with(required=True)
    assert engine.reconcile_fill_cooldown_checkpoint_gap.call_count == 1
    ws.start_private_user_stream.assert_called_once()
    ws.start_public_market_streams.assert_not_called()


def test_live_start_repeats_exact_barrier_when_user_stream_generation_changes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import live.main as live_main

    engine = SimpleNamespace(
        start=Mock(),
        signal=SimpleNamespace(start_metrics_polling=Mock()),
        sync_position=Mock(),
        reconcile_fill_cooldown_checkpoint_gap=Mock(
            return_value={"mode": "cursor_current", "recovered_fill_count": 0}
        ),
    )
    observed_states = [
        {
            "user_event_count": 0,
            "user_stream_connected": True,
            "user_stream_generation": 1,
        },
        {
            "user_event_count": 0,
            "user_stream_connected": False,
            "user_stream_generation": 1,
        },
    ]

    def next_state():
        if observed_states:
            return observed_states.pop(0)
        return {
            "user_event_count": 0,
            "user_stream_connected": True,
            "user_stream_generation": 2,
        }

    ws = SimpleNamespace(
        start_private_user_stream=Mock(),
        start_public_market_streams=Mock(),
        wait_for_user_stream_ready=Mock(return_value=True),
        hold_user_event_callbacks=Mock(side_effect=nullcontext),
        user_event_safety_snapshot=Mock(side_effect=next_state),
    )
    monkeypatch.setattr(
        live_main,
        "_initial_exchange_open_orders",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        live_main,
        "initialize_prospective_lifecycle_collection",
        lambda **_kwargs: (None, None),
    )

    _epoch, _writer, _exchange, generation = (
        start_engine_with_prospective_collection(
            cfg=SimpleNamespace(symbol="BTCUSDC"),
            engine=engine,
            ws=ws,
            rest=object(),
            config_path=tmp_path / "config.yaml",
            native_runtime={},
            safety_authority={},
            dry_run=False,
        )
    )

    assert generation == 2
    # Durable crash-gap recovery runs once; connected-generation barriers use
    # normal exact accountTrades reconciliation and its fill dedupe.
    engine.reconcile_fill_cooldown_checkpoint_gap.assert_called_once_with()
    assert engine.sync_position.call_count == 3
    ws.start_private_user_stream.assert_called_once()
    ws.start_public_market_streams.assert_called_once()
    assert ws.start_public_market_streams.call_args.kwargs == {
        "market_snapshot_client": None,
        "expected_user_stream_generation": 2,
    }
    engine.signal.start_metrics_polling.assert_called_once_with()


def test_maker_engine_latches_when_admitted_user_stream_generation_is_lost() -> None:
    engine = MakerEngine.__new__(MakerEngine)
    engine._admitted_user_stream_generation = 7
    engine._event_source = SimpleNamespace(
        user_event_safety_snapshot=Mock(
            return_value={
                "user_stream_connected": False,
                "user_stream_generation": 7,
            }
        )
    )
    engine.latch_runtime_fatal = Mock()

    assert engine._enforce_private_user_stream_authority() is False
    engine.latch_runtime_fatal.assert_called_once()
    assert (
        engine.latch_runtime_fatal.call_args.kwargs["reason"]
        == "PRIVATE_USER_STREAM_AUTHORITY_LOST"
    )
    assert engine.latch_runtime_fatal.call_args.kwargs["reconciliation_required"] is True


@pytest.mark.parametrize("timeout", (0.0, -1.0, float("nan"), float("inf"), True))
def test_rest_timeout_must_be_positive_finite_and_non_boolean(timeout) -> None:
    cfg = Config()
    cfg.api.timeout_s = timeout

    with pytest.raises(ValueError, match="api.timeout_s"):
        _validate_config(cfg)


@pytest.mark.parametrize(
    ("field_name", "value"),
    (
        ("order_size", 0.0),
        ("max_inventory", float("nan")),
    ),
)
def test_base_quantity_limits_must_be_positive_finite(
    field_name: str,
    value: float,
) -> None:
    cfg = Config()
    setattr(cfg.strategy, field_name, value)

    with pytest.raises(ValueError, match=rf"strategy\.{field_name}"):
        _validate_config(cfg)


def test_order_size_cannot_exceed_base_inventory_fuse() -> None:
    cfg = Config()
    cfg.strategy.order_size = cfg.strategy.max_inventory + 0.001

    with pytest.raises(ValueError, match="order_size cannot exceed"):
        _validate_config(cfg)


@pytest.mark.parametrize(
    "field_name",
    ("max_daily_loss", "max_position_value", "emergency_close_dd"),
)
def test_quote_currency_risk_fuses_must_be_positive_finite(
    field_name: str,
) -> None:
    cfg = Config()
    setattr(cfg.risk, field_name, float("inf"))

    with pytest.raises(ValueError, match=rf"risk\.{field_name}"):
        _validate_config(cfg)


def test_reconnect_watchdog_must_outlive_quote_stop_thresholds() -> None:
    cfg = Config()
    cfg.risk.max_exec_book_visible_age_s = 3.0
    cfg.risk.max_exec_book_source_lag_s = 5.0
    cfg.websocket.exec_stream_silence_timeout_s = 5.0

    with pytest.raises(ValueError, match="reconnect watchdog.*exceed"):
        _validate_config(cfg)

    cfg.websocket.exec_stream_silence_timeout_s = 5.001
    _validate_config(cfg)


def test_runtime_health_exposes_only_general_loop_and_stream_safety_facts() -> None:
    engine = SimpleNamespace(
        runtime_safety_snapshot=lambda **_kwargs: {
            "quote_loop_running": False,
            "ownership_conflict_latched": True,
            "fatal_runtime_latched": True,
            "reconciliation_required": True,
            "reconciliation_pending": True,
            "fatal_runtime_reason": "ORDER_MANAGER_FATAL",
            "last_tick_age_s": 3.0,
            "replace_terminal_continuation": {
                "arm_count": 9,
                "publish_count": 8,
                "decision_count": 6,
                "drop_count": 3,
                "pending_count": 0,
                "in_flight_count": 0,
                "buy_decision_count": 4,
                "sell_decision_count": 2,
                "decision_latency_sum_ns": 1200,
                "decision_latency_max_ns": 500,
            },
        },
        runtime_evidence_writer_health_snapshot=lambda: {
            "enabled": True,
            "valid": False,
            "queue_depth": 7,
            "queue_high_watermark": 11,
            "queue_full_count": 1,
            "uncommitted_count": 3,
            "error_count": 2,
            "fatal_error": "simulated",
        },
    )
    ws = SimpleNamespace(
        user_event_safety_snapshot=lambda **_kwargs: {
            "last_user_event_age_s": 4.0,
            "user_event_count": 7,
            "user_stream_connected": True,
            "user_stream_generation": 3,
        }
    )
    order_gateway = SimpleNamespace(
        health_snapshot=lambda: {
            "active_transport": "websocket_api",
            "websocket_api": {
                "enabled": True,
                "last_receipt": {"client_order_id": "ng-order-1"},
            },
        }
    )
    logging_health = {
        "valid": False,
        "configured": True,
        "failure_message": "simulated logging failure",
    }

    health = collect_runtime_safety_health(
        engine=engine,
        ws=ws,
        order_gateway=order_gateway,
        gc_pause_monitor=SimpleNamespace(
            snapshot=lambda: {
                "count": 4,
                "total_ns": 1_200,
                "max_ns": 700,
                "last_ns": 200,
                "generation_counts": (2, 1, 1),
                "pause_bucket_upper_ns": (100, 500, 1_000),
                "pause_bucket_counts": (1, 2, 1, 0),
            }
        ),
        logging_runtime=SimpleNamespace(
            health_snapshot=lambda: logging_health,
        ),
        now_monotonic_s=10.0,
    )

    assert health["quoteLoopRunning"] is False
    assert health["ownershipConflictLatched"] is True
    assert health["lastTickAge"] == pytest.approx(3.0)
    assert health["orderGateway"]["active_transport"] == "websocket_api"
    assert health["orderGateway"]["websocket_api"]["last_receipt"] == {
        "client_order_id": "ng-order-1"
    }
    assert health["lastUserEventAge"] == pytest.approx(4.0)
    assert health["userStreamConnected"] is True
    assert health["userStreamGeneration"] == 3
    assert health["reconciliationRequired"] is True
    assert health["reconciliationPending"] is True
    assert health["fatalReason"] == "ORDER_MANAGER_FATAL"
    assert health["replaceTerminalContinuationArmCount"] == 9
    assert health["replaceTerminalContinuationPublishCount"] == 8
    assert health["replaceTerminalContinuationDecisionCount"] == 6
    assert health["replaceTerminalContinuationDropCount"] == 3
    assert health["replaceTerminalContinuationPendingCount"] == 0
    assert health["replaceTerminalContinuationInFlightCount"] == 0
    assert health["replaceTerminalContinuationBuyDecisionCount"] == 4
    assert health["replaceTerminalContinuationSellDecisionCount"] == 2
    assert health["replaceTerminalContinuationDecisionLatencySumNs"] == 1200
    assert health["replaceTerminalContinuationDecisionLatencyMaxNs"] == 500
    assert health["runtimeEvidenceWriterEnabled"] is True
    assert health["runtimeEvidenceWriterValid"] is False
    assert health["runtimeEvidenceWriterQueueDepth"] == 7
    assert health["runtimeEvidenceWriterQueueHighWatermark"] == 11
    assert health["runtimeEvidenceWriterQueueFullCount"] == 1
    assert health["runtimeEvidenceWriterUncommittedCount"] == 3
    assert health["runtimeEvidenceWriterErrorCount"] == 2
    assert health["runtimeEvidenceWriterFatalError"] == "simulated"
    assert health["gcPauseCount"] == 4
    assert health["gcPauseTotalNs"] == 1_200
    assert health["gcPauseMaxNs"] == 700
    assert health["gcPauseLastNs"] == 200
    assert health["gcPauseGenerationCounts"] == [2, 1, 1]
    assert health["gcPauseBucketUpperNs"] == [100, 500, 1_000]
    assert health["gcPauseBucketCounts"] == [1, 2, 1, 0]
    assert health["logging"] == logging_health


def test_normal_cleanup_with_final_reconciliation_pending_exits_78() -> None:
    engine = SimpleNamespace(
        runtime_safety_snapshot=lambda: {
            "ownership_conflict_latched": False,
            "reconciliation_required": False,
            "reconciliation_pending": True,
            "fatal_runtime_reason": "LATE_USER_CALLBACK",
        }
    )

    assert (
        resolve_live_shutdown_exit(
            engine=engine,
            fatal_error=None,
            fatal_traceback=None,
            cleanup_errors=[],
        )
        == EXECUTION_STATE_UNCERTAIN_EXIT_CODE
    )


def test_ws_stop_waits_for_late_user_callback_and_listen_key_thread() -> None:
    close_requested = threading.Event()
    callback_latched = threading.Event()
    release_callback = threading.Event()
    callback_exited = threading.Event()
    listen_key_entered = threading.Event()
    release_listen_key = threading.Event()
    sync_calls: list[bool] = []
    thread_errors: dict[str, BaseException] = {}

    engine = object.__new__(MakerEngine)
    engine.cfg = Config()
    engine.rest = SimpleNamespace(cancel_open_orders=Mock(return_value={}))
    engine.signal = SimpleNamespace(stop=Mock())
    engine._persist_fill_cooldown_checkpoint = Mock()
    engine._runtime_fatal_lock = threading.Lock()
    engine._runtime_fatal_reason = ""
    engine._runtime_fatal_error = None
    engine._runtime_reconciliation_required = False
    engine._runtime_reconciliation_pending = False
    engine._runtime_reconciliation_inflight = False
    engine._runtime_reconciliation_generation = 0
    engine._running = True
    engine._order_submit_fail_closed = False
    engine._order_lifecycle_live_writer_v2 = None
    engine._exact_opportunity_tape_runtime = None

    def run(name: str, target) -> None:
        try:
            target()
        except BaseException as exc:  # pragma: no cover - asserted below
            thread_errors[name] = exc

    user_thread: threading.Thread

    engine.orders = SimpleNamespace(
        fatal_status=lambda: {
            "latched": False,
            "reason": "",
            "reconciliation_required": False,
        },
        in_callback_dispatch=lambda: threading.current_thread() is user_thread,
    )

    def sync_position(*, required: bool = False) -> bool:
        assert required is True
        assert not user_thread.is_alive()
        sync_calls.append(required)
        return True

    engine.sync_position = sync_position

    def late_user_callback() -> None:
        assert close_requested.wait(timeout=2.0)
        engine.latch_runtime_fatal(
            reason="LATE_USER_CALLBACK",
            error=RuntimeError("late user callback"),
            reconciliation_required=True,
        )
        callback_latched.set()
        assert release_callback.wait(timeout=2.0)
        callback_exited.set()

    def listen_key_worker() -> None:
        listen_key_entered.set()
        assert release_listen_key.wait(timeout=2.0)

    user_thread = threading.Thread(target=late_user_callback)
    listen_key_thread = threading.Thread(target=listen_key_worker)
    user_thread.start()
    listen_key_thread.start()
    assert listen_key_entered.wait(timeout=1.0)

    handler = WSHandler(engine, Config())
    handler._running = True
    handler._ws_user = SimpleNamespace(close=close_requested.set)
    handler._user_thread = user_thread
    handler._listen_key_thread = listen_key_thread

    stop_thread = threading.Thread(target=run, args=("ws_stop", handler.stop))
    stop_thread.start()
    assert callback_latched.wait(timeout=1.0)
    assert stop_thread.is_alive()
    assert sync_calls == []

    release_callback.set()
    assert callback_exited.wait(timeout=1.0)
    assert stop_thread.is_alive()
    release_listen_key.set()
    stop_thread.join(timeout=2.0)

    assert not stop_thread.is_alive()
    assert not user_thread.is_alive()
    assert not listen_key_thread.is_alive()
    assert thread_errors == {}
    assert engine._runtime_reconciliation_pending is True

    engine.stop()

    assert sync_calls == [True]
    assert engine._runtime_reconciliation_pending is False


def test_ws_stop_join_timeout_latches_execution_uncertainty_and_raises() -> None:
    release_user_thread = threading.Event()
    user_thread = threading.Thread(target=release_user_thread.wait)
    user_thread.start()
    engine = SimpleNamespace(latch_runtime_fatal=Mock())
    handler = WSHandler(engine, Config())
    handler._running = True
    handler._user_stream_shutdown_join_timeout_s = 0.01
    handler._ws_user = SimpleNamespace(close=Mock())
    handler._user_thread = user_thread

    try:
        with pytest.raises(RuntimeError, match="callback quiescence"):
            handler.stop()
    finally:
        release_user_thread.set()
        user_thread.join(timeout=1.0)

    assert not user_thread.is_alive()
    engine.latch_runtime_fatal.assert_called_once()
    call = engine.latch_runtime_fatal.call_args
    assert call.kwargs["reason"].startswith(
        "USER_STREAM_SHUTDOWN_NOT_QUIESCENT:user-data WebSocket thread"
    )
    assert call.kwargs["reconciliation_required"] is True
    assert call.kwargs["defer_reconciliation"] is True


def test_unquiesced_user_callback_defers_engine_and_evidence_shutdown(
    tmp_path: Path,
) -> None:
    callback_started = threading.Event()
    release_callback = threading.Event()
    callback_finished = threading.Event()
    evidence_path = tmp_path / "late-user-callback.csv"
    evidence = RuntimeEvidenceWriter(queue_capacity=8)
    thread_errors: list[BaseException] = []

    def late_callback() -> None:
        callback_started.set()
        try:
            assert release_callback.wait(timeout=2.0)
            evidence.enqueue_csv(
                evidence_path,
                {"event": "late_terminal", "sequence": 1},
            )
        except BaseException as exc:  # pragma: no cover - asserted below
            thread_errors.append(exc)
        finally:
            callback_finished.set()

    callback_thread = threading.Thread(target=late_callback)
    callback_thread.start()
    assert callback_started.wait(timeout=1.0)

    class UnquiescedWs:
        @staticmethod
        def stop() -> None:
            assert callback_thread.is_alive()
            raise RuntimeError("user callback did not quiesce")

    shutdown_order: list[str] = []
    engine = SimpleNamespace(_running=True, stop=Mock())

    def revoke_new_order_authority_for_shutdown() -> None:
        engine._running = False
        shutdown_order.append("new_order_admission_revoked")

    def latch_runtime_fatal(**_kwargs) -> None:
        engine._running = False
        shutdown_order.append("fatal_latched")

    def cancel_all() -> bool:
        assert engine._running is False
        shutdown_order.append("exchange_cancel_all")
        return True

    engine.revoke_new_order_authority_for_shutdown = Mock(
        side_effect=revoke_new_order_authority_for_shutdown
    )
    engine.latch_runtime_fatal = Mock(side_effect=latch_runtime_fatal)
    engine._emergency_cancel_all_exchange_orders = Mock(side_effect=cancel_all)
    cleanup_errors: list[BaseException] = []

    callbacks_quiesced = _quiesce_callbacks_then_stop_engine(
        ws=UnquiescedWs(),
        engine=engine,
        cleanup_errors=cleanup_errors,
    )

    assert callbacks_quiesced is False
    engine.stop.assert_not_called()
    engine.revoke_new_order_authority_for_shutdown.assert_called_once()
    engine.latch_runtime_fatal.assert_called_once()
    engine._emergency_cancel_all_exchange_orders.assert_called_once()
    assert shutdown_order == [
        "new_order_admission_revoked",
        "fatal_latched",
        "exchange_cancel_all",
    ]
    assert engine._running is False
    assert cleanup_errors
    # The real shutdown caller uses this false result to skip gateway, REST,
    # WAL, and evidence closure.  Prove a callback that was already alive can
    # still publish safely after the failed quiescence boundary.
    assert evidence.health_snapshot()["accepting"] is True
    release_callback.set()
    assert callback_finished.wait(timeout=1.0)
    callback_thread.join(timeout=1.0)
    assert thread_errors == []
    evidence.barrier(timeout_s=1.0)
    health = evidence.close(drain_timeout_s=1.0)
    assert health["csv_rows_committed"] == 1


def test_user_stream_restart_uses_dedicated_listen_key_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = WSHandler(SimpleNamespace(), Config())
    compatibility_client = object()
    listen_key_client = object()
    handler._running = True
    handler._rest_client = compatibility_client
    handler._listen_key_client = listen_key_client
    started = []
    monkeypatch.setattr(
        handler,
        "_start_user_stream",
        lambda client: started.append(client),
    )

    handler.restart_user_stream("test")

    assert started == [listen_key_client]


def test_listen_key_expiry_restarts_outside_callback_without_self_join(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import websocket

    state_lock = threading.Lock()
    second_stream_started = threading.Event()
    active_streams = 0
    max_active_streams = 0
    created_apps = []

    class _FakeWebSocketApp:
        def __init__(self, _url, *, on_open, on_message, on_error, on_close):
            del on_error, on_close
            self.on_open = on_open
            self.on_message = on_message
            self.closed = threading.Event()
            self.index = len(created_apps)
            created_apps.append(self)

        def run_forever(self, **_kwargs) -> None:
            nonlocal active_streams, max_active_streams
            with state_lock:
                active_streams += 1
                max_active_streams = max(max_active_streams, active_streams)
            try:
                self.on_open(self)
                if self.index == 0:
                    self.on_message(self, json.dumps({"e": "listenKeyExpired"}))
                else:
                    second_stream_started.set()
                assert self.closed.wait(timeout=2.0)
            finally:
                with state_lock:
                    active_streams -= 1

        def close(self) -> None:
            self.closed.set()

    monkeypatch.setattr(websocket, "WebSocketApp", _FakeWebSocketApp)
    rest = SimpleNamespace(
        new_listen_key=Mock(
            side_effect=(
                {"listenKey": "first-listen-key"},
                {"listenKey": "second-listen-key"},
            )
        )
    )
    engine = SimpleNamespace(latch_runtime_fatal=Mock())
    handler = WSHandler(engine, Config())
    handler._running = True
    handler._rest_client = rest

    handler._start_user_stream(rest)
    assert second_stream_started.wait(timeout=2.0)
    connected = handler.user_event_safety_snapshot()
    assert connected["user_stream_connected"] is True
    assert connected["user_stream_generation"] == 2
    handler.stop()

    assert rest.new_listen_key.call_count == 2
    assert max_active_streams == 1
    assert active_streams == 0
    assert handler._user_thread is None
    assert handler._user_restart_thread is None
    assert handler.user_event_safety_snapshot()["user_stream_connected"] is False
    engine.latch_runtime_fatal.assert_not_called()


def test_concurrent_restart_and_renewal_leave_at_most_one_user_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import websocket

    state_lock = threading.Lock()
    active_streams = 0
    max_active_streams = 0
    created_apps = []

    class _FakeWebSocketApp:
        def __init__(self, _url, **_callbacks):
            self.closed = threading.Event()
            self.on_open = _callbacks["on_open"]
            created_apps.append(self)

        def run_forever(self, **_kwargs) -> None:
            nonlocal active_streams, max_active_streams
            with state_lock:
                active_streams += 1
                max_active_streams = max(max_active_streams, active_streams)
            try:
                self.on_open(self)
                assert self.closed.wait(timeout=2.0)
            finally:
                with state_lock:
                    active_streams -= 1

        def close(self) -> None:
            self.closed.set()

    monkeypatch.setattr(websocket, "WebSocketApp", _FakeWebSocketApp)
    next_key = 0
    key_lock = threading.Lock()

    def new_listen_key() -> dict[str, str]:
        nonlocal next_key
        with key_lock:
            next_key += 1
            return {"listenKey": f"listen-key-{next_key}"}

    rest = SimpleNamespace(new_listen_key=new_listen_key)
    engine = SimpleNamespace(latch_runtime_fatal=Mock())
    handler = WSHandler(engine, Config())
    handler._running = True
    handler._rest_client = rest
    handler._start_user_stream(rest)

    start_gate = threading.Event()
    workers = (
        threading.Thread(
            target=lambda: (start_gate.wait(), handler.restart_user_stream("manual")),
        ),
        threading.Thread(
            target=lambda: (start_gate.wait(), handler._start_user_stream(rest)),
        ),
    )
    for worker in workers:
        worker.start()
    start_gate.set()
    for worker in workers:
        worker.join(timeout=2.0)

    assert all(not worker.is_alive() for worker in workers)
    assert max_active_streams == 1
    assert active_streams == 1
    assert handler.user_event_safety_snapshot()["user_stream_connected"] is True
    handler.stop()
    assert active_streams == 0
    assert handler._user_thread is None
    engine.latch_runtime_fatal.assert_not_called()


def test_verified_user_event_updates_monotonic_health_and_callback_failure_latches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import live.ws_handler as ws_handler_module

    engine = SimpleNamespace(
        orders=SimpleNamespace(
            on_order_update=Mock(side_effect=RuntimeError("inventory callback failed"))
        ),
        latch_runtime_fatal=Mock(),
    )
    handler = WSHandler(engine, Config())
    handler._running = True
    handler._user_stream_active = True
    ws = object()
    token = handler._install_user_stream_app(ws)
    assert token is not None
    handler._on_user_open(ws, token)
    monkeypatch.setattr(
        ws_handler_module,
        "time",
        SimpleNamespace(monotonic=lambda: 10.0, time_ns=lambda: 123_000_000),
    )

    handler._on_user_message(
        ws,
        json.dumps(
            {
                "e": "ORDER_TRADE_UPDATE",
                "o": {"c": "mm_B_1", "X": "NEW", "i": 41},
            }
        ),
        token,
    )

    assert handler.user_event_safety_snapshot(now_monotonic_s=14.0) == {
        "user_event_count": 1,
        "last_user_event_age_s": 4.0,
        "user_stream_connected": True,
        "user_stream_generation": 1,
    }
    engine.latch_runtime_fatal.assert_called_once()
    assert engine.latch_runtime_fatal.call_args.kwargs["reconciliation_required"] is True


def test_user_order_update_records_raw_private_visibility_before_ledger_transition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import live.ws_handler as ws_handler_module

    observed: list[tuple[str, int]] = []

    def record_private(event, *, receive_ts_ns):
        observed.append((f"evidence:{event['X']}", receive_ts_ns))

    def update_order(event):
        observed.append((f"ledger:{event['X']}", event["_local_receive_ts_ns"]))

    engine = SimpleNamespace(
        order_gateway=SimpleNamespace(record_private_order_visibility=record_private),
        orders=SimpleNamespace(on_order_update=update_order),
        latch_runtime_fatal=Mock(),
    )
    handler = WSHandler(engine, Config())
    handler._running = True
    handler._user_stream_active = True
    ws = object()
    token = handler._install_user_stream_app(ws)
    assert token is not None
    handler._on_user_open(ws, token)
    monkeypatch.setattr(
        ws_handler_module,
        "time",
        SimpleNamespace(monotonic=lambda: 10.0, time_ns=lambda: 456_000_000),
    )

    handler._on_user_message(
        ws,
        json.dumps(
            {
                "e": "ORDER_TRADE_UPDATE",
                "o": {"c": "mm_B_2", "x": "NEW", "X": "NEW", "i": 42},
            }
        ),
        token,
    )

    assert observed == [
        ("evidence:NEW", 456_000_000),
        ("ledger:NEW", 456_000_000),
    ]
    engine.latch_runtime_fatal.assert_not_called()


def test_private_visibility_failure_still_updates_ledger_before_fatal_latch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import live.ws_handler as ws_handler_module

    observed: list[str] = []

    def fail_evidence(_event, *, receive_ts_ns):
        assert receive_ts_ns == 789_000_000
        observed.append("evidence_failed")
        raise RuntimeError("evidence queue full")

    def update_order(event):
        assert event["_local_receive_ts_ns"] == 789_000_000
        observed.append("ledger_updated")

    engine = SimpleNamespace(
        order_gateway=SimpleNamespace(record_private_order_visibility=fail_evidence),
        orders=SimpleNamespace(on_order_update=update_order),
        latch_runtime_fatal=Mock(),
    )
    handler = WSHandler(engine, Config())
    handler._running = True
    handler._user_stream_active = True
    ws = object()
    token = handler._install_user_stream_app(ws)
    assert token is not None
    handler._on_user_open(ws, token)
    monkeypatch.setattr(
        ws_handler_module,
        "time",
        SimpleNamespace(monotonic=lambda: 10.0, time_ns=lambda: 789_000_000),
    )

    handler._on_user_message(
        ws,
        json.dumps(
            {
                "e": "ORDER_TRADE_UPDATE",
                "o": {"c": "mm_B_3", "x": "TRADE", "X": "FILLED", "i": 43},
            }
        ),
        token,
    )

    assert observed == ["evidence_failed", "ledger_updated"]
    engine.latch_runtime_fatal.assert_called_once()
    fatal = engine.latch_runtime_fatal.call_args.kwargs
    assert fatal["reason"] == "USER_EVENT_CALLBACK_FAILURE:ORDER_TRADE_UPDATE"
    assert fatal["reconciliation_required"] is True
    assert isinstance(fatal["error"], RuntimeError)
    assert str(fatal["error"]) == "private order visibility evidence admission failed"


def test_user_stream_session_fences_stale_callbacks_messages_and_late_install() -> None:
    orders = SimpleNamespace(on_order_update=Mock())
    handler = WSHandler(
        SimpleNamespace(orders=orders, latch_runtime_fatal=Mock()), Config()
    )
    handler._running = True
    handler._user_stream_active = True
    ws1 = SimpleNamespace(close=Mock())
    ws2 = SimpleNamespace(close=Mock())

    token1 = handler._install_user_stream_app(ws1)
    assert token1 is not None
    handler._on_user_open(ws1, token1)
    handler._on_user_open(ws1, token1)
    assert handler.user_event_safety_snapshot()["user_stream_generation"] == 1

    token2 = handler._install_user_stream_app(ws2)
    assert token2 is not None
    handler._on_user_open(ws2, token2)
    handler._on_user_message(
        ws1,
        json.dumps({"e": "ORDER_TRADE_UPDATE", "o": {"i": 1}}),
        token1,
    )
    handler._on_user_error(ws1, RuntimeError("stale"), token1)
    handler._on_user_close(ws1, 1000, "stale", token1)
    handler._release_user_stream_app(ws1, token1)
    snapshot = handler.user_event_safety_snapshot()
    assert snapshot["user_stream_connected"] is True
    assert snapshot["user_stream_generation"] == 2
    orders.on_order_update.assert_not_called()

    handler._on_user_message(
        ws2,
        json.dumps({"e": "ORDER_TRADE_UPDATE", "o": {"i": 2}}),
        token2,
    )
    orders.on_order_update.assert_called_once()
    handler._on_user_error(ws2, RuntimeError("current"), token2)
    assert handler.user_event_safety_snapshot()["user_stream_connected"] is False
    assert handler._ws_user is ws2
    handler._on_user_open(ws2, token2)
    assert handler.user_event_safety_snapshot()["user_stream_generation"] == 3
    assert handler._release_user_stream_app(ws2, token2) is True
    assert handler._ws_user is None

    ws3 = SimpleNamespace(close=Mock())
    token3 = handler._install_user_stream_app(ws3)
    assert token3 is not None
    handler._stop_user_stream_locked()
    assert ws3.close.call_count == 1
    assert handler._install_user_stream_app(SimpleNamespace(close=Mock())) is None
    assert handler.user_event_safety_snapshot()["user_stream_connected"] is False


def test_wait_for_user_stream_ready_tracks_current_connected_generation() -> None:
    handler = WSHandler(
        SimpleNamespace(orders=SimpleNamespace(), latch_runtime_fatal=Mock()), Config()
    )
    assert handler.wait_for_user_stream_ready(0.0) is False

    handler._running = True
    handler._user_stream_active = True
    ws = SimpleNamespace(close=Mock())
    token = handler._install_user_stream_app(ws)
    assert token is not None
    waiter_result: list[bool] = []
    waiter = threading.Thread(
        target=lambda: waiter_result.append(handler.wait_for_user_stream_ready(1.0))
    )
    waiter.start()

    handler._on_user_open(ws, token)
    waiter.join(timeout=1.0)

    assert not waiter.is_alive()
    assert waiter_result == [True]
    assert handler.user_event_safety_snapshot()["user_stream_generation"] == 1
    assert handler._user_stream_ready_event.is_set()

    handler._on_user_close(ws, 1000, "closed", token)
    assert handler._user_stream_ready_event.is_set() is False
    assert handler.wait_for_user_stream_ready(0.01) is False

    handler._on_user_open(ws, token)
    assert handler.wait_for_user_stream_ready(0.0) is True
    assert handler.user_event_safety_snapshot()["user_stream_generation"] == 2

    assert handler._release_user_stream_app(ws, token) is True
    assert handler._user_stream_ready_event.is_set() is False
    assert handler.wait_for_user_stream_ready(0.0) is False


@pytest.mark.parametrize("timeout_s", (-1.0, float("inf"), float("nan")))
def test_wait_for_user_stream_ready_rejects_invalid_timeout(timeout_s: float) -> None:
    handler = WSHandler(SimpleNamespace(), Config())

    with pytest.raises(ValueError, match="finite and non-negative"):
        handler.wait_for_user_stream_ready(timeout_s)


def test_user_stream_stop_drains_final_current_order_update_before_retire() -> None:
    orders = SimpleNamespace(on_order_update=Mock())
    handler = WSHandler(
        SimpleNamespace(orders=orders, latch_runtime_fatal=Mock()), Config()
    )
    handler._running = True
    handler._user_stream_active = True
    session: dict[str, object] = {}

    def close() -> None:
        handler._on_user_message(
            session["ws"],
            json.dumps({"e": "ORDER_TRADE_UPDATE", "o": {"i": 7}}),
            session["token"],
        )

    ws = SimpleNamespace(close=close)
    token = handler._install_user_stream_app(ws)
    assert token is not None
    session.update(ws=ws, token=token)
    handler._on_user_open(ws, token)

    handler._stop_user_stream_locked()

    orders.on_order_update.assert_called_once()
    assert handler._ws_user is None
    assert handler.user_event_safety_snapshot()["user_stream_connected"] is False


def test_user_stream_run_failure_retires_connected_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import websocket

    opened = threading.Event()

    class _FailingWebSocketApp:
        def __init__(self, _url, **callbacks):
            self.on_open = callbacks["on_open"]

        def run_forever(self, **_kwargs) -> None:
            self.on_open(self)
            opened.set()
            handler._running = False
            raise RuntimeError("socket loop failed")

        def close(self) -> None:
            return None

    monkeypatch.setattr(websocket, "WebSocketApp", _FailingWebSocketApp)
    handler = WSHandler(SimpleNamespace(latch_runtime_fatal=Mock()), Config())
    handler._running = True
    handler._start_user_stream(
        SimpleNamespace(new_listen_key=Mock(return_value={"listenKey": "test-key"}))
    )

    assert opened.wait(timeout=1.0)
    assert handler._user_thread is not None
    handler._user_thread.join(timeout=1.0)
    assert not handler._user_thread.is_alive()
    assert handler._ws_user is None
    assert handler.user_event_safety_snapshot()["user_stream_connected"] is False
    handler.stop()


def test_supervisor_never_restarts_execution_state_uncertainty() -> None:
    script = (ROOT / "live/run.sh").read_text(encoding="utf-8")
    supervisor = script.split("supervise() {", 1)[1].split("\nstart() {", 1)[0]

    assert f"EXECUTION_STATE_UNCERTAIN_EXIT_CODE={EXECUTION_STATE_UNCERTAIN_EXIT_CODE}" in script
    assert "reconciliation_required_no_restart" in supervisor
    assert "fatal_exit_no_restart" in supervisor
    assert "((++restart_count))" not in supervisor
    assert "NARROWGATE_SUPERVISOR_MAX_RESTARTS:-0" in script


def test_supervisor_does_not_restart_unclassified_exit_one() -> None:
    script = (ROOT / "live/run.sh").read_text(encoding="utf-8")
    supervisor = script.split("supervise() {", 1)[1].split("\nstart() {", 1)[0]

    assert "if [[ $child_exit -eq 0 ]]" in supervisor
    assert "if [[ $child_exit -eq $EXECUTION_STATE_UNCERTAIN_EXIT_CODE ]]" in supervisor
    assert '_record_supervisor_state "fatal_exit_no_restart"' in supervisor
    assert "restart_backoff" not in supervisor


def test_fatal_cancel_bypasses_latched_ledger_and_stop_preserves_ownership() -> None:
    engine = object.__new__(MakerEngine)
    engine.cfg = Config()
    engine.rest = SimpleNamespace(cancel_open_orders=Mock(return_value={}))
    engine.orders = SimpleNamespace(
        fatal_status=Mock(
            return_value={
                "latched": True,
                "reason": "callback_delivery_failed:on_fill",
                "reconciliation_required": True,
            }
        ),
        cancel_all_local=Mock(),
        get_active_orders=Mock(return_value=[]),
    )
    engine.signal = SimpleNamespace(stop=Mock())
    engine._persist_fill_cooldown_checkpoint = Mock()
    engine._running = True
    engine._order_submit_fail_closed = False
    engine.sync_position = Mock(side_effect=RuntimeError("ledger is fatal"))

    engine.latch_runtime_fatal(
        reason="ORDER_MANAGER_FATAL",
        error=RuntimeError("inventory callback delivery failed"),
        reconciliation_required=True,
    )
    engine.stop()

    assert engine.rest.cancel_open_orders.call_count == 2
    engine.orders.cancel_all_local.assert_not_called()
    assert engine.orders.fatal_status()["reconciliation_required"] is True


def test_normal_stop_does_not_invent_terminal_orders_after_cancel_all_ack() -> None:
    engine = object.__new__(MakerEngine)
    engine.cfg = Config()
    engine.orders = SimpleNamespace(
        fatal_status=Mock(return_value={"latched": False, "reconciliation_required": False}),
        cancel_all_local=Mock(),
        get_active_orders=Mock(return_value=[]),
    )
    engine.signal = SimpleNamespace(stop=Mock())
    engine._persist_fill_cooldown_checkpoint = Mock()
    engine._cancel_all_orders = Mock(return_value=True)
    engine.sync_position = Mock(return_value=True)
    engine._running = True
    engine._order_submit_fail_closed = False
    engine._shutdown_new_order_admission_revoked = False
    engine._order_ref_lock = threading.RLock()
    engine.order_gateway = SimpleNamespace(revoke_new_order_admission=Mock())
    engine._order_lifecycle_live_writer_v2 = None
    engine._exact_opportunity_tape_runtime = None

    cleanup_errors: list[BaseException] = []
    assert _quiesce_callbacks_then_stop_engine(
        ws=SimpleNamespace(stop=Mock()),
        engine=engine,
        cleanup_errors=cleanup_errors,
    ) is True

    assert cleanup_errors == []
    engine.order_gateway.revoke_new_order_admission.assert_called_once_with()
    assert engine._shutdown_new_order_admission_revoked is True
    engine._cancel_all_orders.assert_called_once_with()
    engine.sync_position.assert_called_once_with(required=True)
    engine.orders.cancel_all_local.assert_not_called()
    assert engine._execution_state_uncertain() is False
    assert engine.runtime_safety_snapshot()["ownership_conflict_latched"] is False
    assert resolve_live_shutdown_exit(
        engine=engine, fatal_error=None, fatal_traceback=None, cleanup_errors=[]
    ) == 0


def test_normal_stop_fails_when_specialized_evidence_health_is_incomplete() -> None:
    engine = object.__new__(MakerEngine)
    engine.cfg = Config()
    engine.orders = SimpleNamespace(
        fatal_status=Mock(
            return_value={"latched": False, "reconciliation_required": False}
        ),
        cancel_all_local=Mock(),
        get_active_orders=Mock(return_value=[]),
    )
    engine.signal = SimpleNamespace(stop=Mock())
    engine._persist_fill_cooldown_checkpoint = Mock()
    engine._cancel_all_orders = Mock(return_value=True)
    engine.sync_position = Mock(return_value=True)
    engine._running = True
    engine._order_submit_fail_closed = False
    engine._order_lifecycle_live_writer_v2 = None
    engine._exact_opportunity_tape_runtime = SimpleNamespace(
        close=Mock(
            return_value={
                "rows_written": 4,
                "rows_dropped": 0,
                "error_count": 0,
                "formal_collection_valid": True,
            }
        )
    )

    with pytest.raises(
        RuntimeError,
        match="specialized evidence writer shutdown was invalid",
    ):
        engine.stop()

    assert engine._exact_opportunity_tape_runtime is None


def test_normal_stop_accepts_valid_bounded_remote_lifecycle_spool() -> None:
    engine = object.__new__(MakerEngine)
    engine.cfg = Config()
    engine.orders = SimpleNamespace(
        fatal_status=Mock(
            return_value={"latched": False, "reconciliation_required": False}
        ),
        cancel_all_local=Mock(),
        get_active_orders=Mock(return_value=[]),
    )
    engine.signal = SimpleNamespace(stop=Mock())
    engine._persist_fill_cooldown_checkpoint = Mock()
    engine._cancel_all_orders = Mock(return_value=True)
    engine.sync_position = Mock(return_value=True)
    engine._running = True
    engine._order_submit_fail_closed = False
    engine._order_lifecycle_live_writer_v2_shutdown_timeout_s = 1.0
    engine._order_lifecycle_live_writer_v2 = SimpleNamespace(
        close=Mock(
            return_value={
                "rows_committed": 4,
                "drop_count": 0,
                "error_count": 0,
                "state": "closed",
                "worker_alive": False,
                "queue_depth": 0,
                "callbacks_enqueued": 4,
                "callbacks_processed": 4,
                "formal_collection_valid": False,
                "remote_spool_valid": True,
            }
        )
    )
    engine._exact_opportunity_tape_runtime = None

    engine.stop()

    assert engine._order_lifecycle_live_writer_v2 is None


def test_specialized_close_failure_still_closes_every_writer_and_checkpoint() -> None:
    close_calls: list[str] = []

    class _FailingLifecycleWriter:
        def close(self, *, drain_timeout_s: float) -> dict[str, object]:
            assert drain_timeout_s == pytest.approx(1.0)
            close_calls.append("lifecycle")
            raise OSError("lifecycle close failed")

    class _ExactWriter:
        def close(self) -> dict[str, object]:
            close_calls.append("exact")
            return {
                "rows_written": 3,
                "rows_enqueued": 3,
                "rows_dropped": 0,
                "error_count": 0,
                "queue_depth": 0,
                "state": "closed",
                "closed": True,
                "formal_collection_valid": True,
            }

    engine = object.__new__(MakerEngine)
    engine.cfg = Config()
    engine.orders = SimpleNamespace(
        fatal_status=Mock(
            return_value={"latched": False, "reconciliation_required": False}
        ),
        cancel_all_local=Mock(),
        get_active_orders=Mock(return_value=[]),
    )
    engine.signal = SimpleNamespace(stop=Mock())
    engine._persist_fill_cooldown_checkpoint = Mock()
    engine.close_fill_cooldown_checkpoint_store = Mock(
        side_effect=lambda: close_calls.append("checkpoint")
    )
    engine._cancel_all_orders = Mock(return_value=True)
    engine.sync_position = Mock(return_value=True)
    engine._running = True
    engine._order_submit_fail_closed = False
    engine._order_lifecycle_live_writer_v2_shutdown_timeout_s = 1.0
    engine._order_lifecycle_live_writer_v2 = _FailingLifecycleWriter()
    engine._exact_opportunity_tape_runtime = _ExactWriter()

    with pytest.raises(
        RuntimeError,
        match="specialized evidence writer shutdown was invalid",
    ):
        engine.stop()

    assert close_calls == ["lifecycle", "exact", "checkpoint"]
    assert engine._order_lifecycle_live_writer_v2 is None
    assert engine._exact_opportunity_tape_runtime is None


def test_q90_action_state_cannot_change_via_sighup() -> None:
    require_q90_action_restart(False, False)
    require_q90_action_restart(True, True)
    with pytest.raises(ValueError, match="restart through live/run.sh"):
        require_q90_action_restart(False, True)


def test_startup_identity_records_admission_without_rechecking_policy_or_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("project_name: NarrowGate\n", encoding="utf-8")
    cfg = _q90_action_config()
    cfg.logging.file = str(tmp_path / "maker.log")
    admission = admit_runtime_policies(
        vars(cfg.strategy), deployment_authority={"policy_approvals": ["q90_action"]},
    )

    def forbidden(*_args, **_kwargs):
        raise AssertionError("startup recording must not re-evaluate policy approval")

    class EnvironmentWithoutApprovalReads(dict):
        def get(self, key, default=None):
            if key in {env for _, _, env in POLICY_CASES}:
                forbidden()
            return super().get(key, default)

    monkeypatch.setattr(live_main, "admit_runtime_policies", forbidden)
    monkeypatch.setattr(
        live_main.os, "environ", EnvironmentWithoutApprovalReads(live_main.os.environ)
    )

    path, identity = record_startup_runtime_identity(
        cfg=cfg,
        config_path=config_path,
        native_runtime={"profile": "test"},
        policy_admission=admission,
        dry_run=True,
    )

    assert path == tmp_path / "runtime_identity.json"
    assert identity["schema_version"] == "narrowgate_live_runtime_identity.v1"
    assert identity["policy_admission"] == admission
    assert identity["dynamic_fill_hazard_action_enabled"] is True
    assert json.loads(path.read_text())["policy_admission"] == admission


def test_enabled_boolean_cooldown_config_does_not_require_yaml_leaf_hashes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("NARROWGATE_ALLOW_F05_BOOLEAN_COOLDOWN_PRIVATE_DEPLOY", raising=False)
    cfg = Config()
    cfg.strategy.fill_cooldown = 85.0
    cfg.strategy.boolean_cooldown_policy_enabled = True
    cfg.strategy.boolean_cooldown_policy_path = "/private/policy.json"
    cfg.strategy.boolean_cooldown_predicate_bundle_path = "/private/bundle.json"
    cfg.strategy.boolean_cooldown_policy_sha256 = ""
    cfg.strategy.boolean_cooldown_predicate_bundle_sha256 = ""
    cfg.strategy.boolean_cooldown_evidence_route = "research_annotation_only"

    _validate_config(cfg)


def test_f05_boolean_cooldown_identity_is_restart_only() -> None:
    base = {
        "boolean_cooldown_policy_enabled": False,
        "boolean_cooldown_policy_path": "",
        "boolean_cooldown_policy_sha256": "",
        "boolean_cooldown_predicate_bundle_path": "",
        "boolean_cooldown_predicate_bundle_sha256": "",
        "boolean_cooldown_ema_warmup_s": 2048.0,
        "boolean_cooldown_evidence_route": "private_deployment_approval",
    }
    require_f05_boolean_cooldown_restart(base, dict(base))
    changed = {**base, "boolean_cooldown_policy_enabled": True}
    with pytest.raises(ValueError, match="restart-only"):
        require_f05_boolean_cooldown_restart(base, changed)
    require_f05_boolean_cooldown_restart(
        base,
        {
            **base,
            "boolean_cooldown_policy_sha256": "a" * 64,
            "boolean_cooldown_predicate_bundle_sha256": "b" * 64,
        },
    )


def test_enabled_buy_e3_config_does_not_require_yaml_leaf_hashes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("NARROWGATE_ALLOW_F05_BUY_E3_PRIVATE_DEPLOY", raising=False)
    cfg = Config()
    cfg.strategy.fill_cooldown = 85.0
    cfg.strategy.buy_e3_cooldown_policy_enabled = True
    cfg.strategy.buy_e3_cooldown_artifact_manifest_path = "/private/manifest.json"
    cfg.strategy.buy_e3_cooldown_policy_path = "/private/policy.json"
    cfg.strategy.buy_e3_cooldown_predicate_bundle_path = "/private/bundle.json"
    cfg.strategy.buy_e3_cooldown_artifact_manifest_sha256 = ""
    cfg.strategy.buy_e3_cooldown_artifact_sha256 = ""
    cfg.strategy.buy_e3_cooldown_policy_sha256 = ""
    cfg.strategy.buy_e3_cooldown_predicate_bundle_sha256 = ""
    cfg.strategy.buy_e3_cooldown_evidence_route = "research_annotation_only"

    _validate_config(cfg)


def test_f05_buy_e3_identity_is_restart_only() -> None:
    base = {
        "buy_e3_cooldown_policy_enabled": False,
        "buy_e3_cooldown_artifact_manifest_path": "",
        "buy_e3_cooldown_artifact_manifest_sha256": "",
        "buy_e3_cooldown_artifact_sha256": "",
        "buy_e3_cooldown_policy_path": "",
        "buy_e3_cooldown_policy_sha256": "",
        "buy_e3_cooldown_predicate_bundle_path": "",
        "buy_e3_cooldown_predicate_bundle_sha256": "",
        "buy_e3_cooldown_ema_warmup_s": 2048.0,
        "buy_e3_cooldown_evidence_route": "private_deployment_buy_e3",
    }
    require_f05_buy_e3_restart(base, dict(base))
    with pytest.raises(ValueError, match="restart-only"):
        require_f05_buy_e3_restart(
            base,
            {**base, "buy_e3_cooldown_policy_enabled": True},
        )
    require_f05_buy_e3_restart(
        base,
        {
            **base,
            "buy_e3_cooldown_artifact_manifest_sha256": "a" * 64,
            "buy_e3_cooldown_artifact_sha256": "b" * 64,
            "buy_e3_cooldown_policy_sha256": "c" * 64,
            "buy_e3_cooldown_predicate_bundle_sha256": "d" * 64,
        },
    )


def _maker_engine_reload_fixture(
    tmp_path: Path,
    *,
    enabled: bool,
) -> tuple[MakerEngine, Config, object | None]:
    cfg = Config()
    checkpoint = tmp_path / "fill_cooldown_state.json"
    cfg.logging.fill_cooldown_checkpoint = str(checkpoint)
    strategy = cfg.strategy
    strategy.buy_e3_cooldown_policy_enabled = enabled
    strategy.buy_e3_cooldown_artifact_manifest_path = str(tmp_path / "artifact_manifest.json")
    strategy.buy_e3_cooldown_artifact_manifest_sha256 = "1" * 64
    strategy.buy_e3_cooldown_artifact_sha256 = "2" * 64
    strategy.buy_e3_cooldown_policy_path = str(tmp_path / "policy.json")
    strategy.buy_e3_cooldown_policy_sha256 = "3" * 64
    strategy.buy_e3_cooldown_predicate_bundle_path = str(tmp_path / "predicates.json")
    strategy.buy_e3_cooldown_predicate_bundle_sha256 = "4" * 64
    strategy.buy_e3_cooldown_ema_warmup_s = 2048.0
    strategy.buy_e3_cooldown_evidence_route = "private_deployment_buy_e3"

    engine = object.__new__(MakerEngine)
    engine.cfg = cfg
    engine._fill_cooldown_checkpoint_path = checkpoint
    policy = object() if enabled else None
    engine._buy_e3_cooldown_policy = policy
    return engine, cfg, policy


@pytest.mark.parametrize(
    ("previous_enabled", "candidate_enabled"),
    ((True, False), (False, True)),
)
def test_maker_engine_reload_rejects_buy_e3_enablement_change_before_mutation(
    tmp_path: Path,
    previous_enabled: bool,
    candidate_enabled: bool,
) -> None:
    engine, previous, previous_policy = _maker_engine_reload_fixture(
        tmp_path,
        enabled=previous_enabled,
    )
    previous_strategy = vars(previous.strategy).copy()
    candidate = copy.deepcopy(previous)
    candidate.strategy.buy_e3_cooldown_policy_enabled = candidate_enabled

    with pytest.raises(ValueError, match="F05 BUY E3 cooldown policy is restart-only"):
        engine.on_config_reload(candidate)

    assert engine.cfg is previous
    assert vars(engine.cfg.strategy) == previous_strategy
    assert engine._buy_e3_cooldown_policy is previous_policy


@pytest.mark.parametrize(
    ("field", "replacement"),
    (
        ("buy_e3_cooldown_artifact_manifest_path", "/changed/manifest.json"),
        ("buy_e3_cooldown_policy_path", "/changed/policy.json"),
        ("buy_e3_cooldown_predicate_bundle_path", "/changed/predicates.json"),
        ("buy_e3_cooldown_ema_warmup_s", 4096.0),
        ("buy_e3_cooldown_evidence_route", "changed_owner_route"),
    ),
)
def test_maker_engine_reload_rejects_buy_e3_binding_drift_before_mutation(
    tmp_path: Path,
    field: str,
    replacement: object,
) -> None:
    engine, previous, previous_policy = _maker_engine_reload_fixture(
        tmp_path,
        enabled=True,
    )
    previous_strategy = vars(previous.strategy).copy()
    candidate = copy.deepcopy(previous)
    setattr(candidate.strategy, field, replacement)

    with pytest.raises(ValueError, match=field):
        engine.on_config_reload(candidate)

    assert engine.cfg is previous
    assert vars(engine.cfg.strategy) == previous_strategy
    assert engine._buy_e3_cooldown_policy is previous_policy


def test_cross_side_order_lanes_require_async_isolation() -> None:
    cfg = Config()
    cfg.api.cross_side_order_lanes_enabled = True
    with pytest.raises(ValueError, match="requires api.async_order_lanes_enabled"):
        _validate_config(cfg)


def test_async_order_lane_experiment_runtime_must_be_positive() -> None:
    cfg = Config()
    cfg.api.async_order_lane_max_runtime_s = 0.0
    with pytest.raises(ValueError, match="max_runtime_s"):
        _validate_config(cfg)


@pytest.mark.parametrize(
    ("field_name", "new_value"),
    (
        ("async_order_lanes_enabled", True),
        ("cross_side_order_lanes_enabled", True),
        ("async_order_lane_capacity", 9),
        ("async_order_lane_drain_timeout_s", 11.0),
        ("async_order_lane_max_runtime_s", 901.0),
    ),
)
def test_order_lane_experiment_config_is_restart_only(
    field_name: str,
    new_value: object,
) -> None:
    engine = MakerEngine.__new__(MakerEngine)
    engine.cfg = Config()
    candidate = copy.deepcopy(engine.cfg)
    setattr(candidate.api, field_name, new_value)

    with pytest.raises(ValueError, match=field_name):
        engine.on_config_reload(candidate)
    assert getattr(engine.cfg.api, field_name) != new_value


def test_spot_subscription_reload_is_rejected_before_engine_mutation() -> None:
    previous = Config()
    candidate = copy.deepcopy(previous)
    candidate.multi_market.enabled = True
    candidate.multi_market.market_stage = "enhanced"
    engine = MakerEngine.__new__(MakerEngine)
    engine.cfg = previous
    event_source = WSHandler(SimpleNamespace(signal=object()), previous)
    event_source._running = True
    event_source._public_market_streams_started = True
    engine._event_source = event_source
    engine._ws_handler = event_source

    with pytest.raises(ValueError, match="spot market subscriptions are restart-only"):
        engine.on_config_reload(candidate)

    assert engine.cfg is previous
    assert event_source.cfg is previous


def test_testnet_reload_is_rejected_before_engine_mutation() -> None:
    previous = Config()
    candidate = copy.deepcopy(previous)
    candidate.api.testnet = not previous.api.testnet
    engine = MakerEngine.__new__(MakerEngine)
    engine.cfg = previous

    with pytest.raises(ValueError, match="api.testnet"):
        engine.on_config_reload(candidate)

    assert engine.cfg is previous
