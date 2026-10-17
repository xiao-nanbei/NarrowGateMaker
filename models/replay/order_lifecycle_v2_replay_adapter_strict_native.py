"""Native-queue provenance and pre-activation cancellation over one replay adapter."""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from execution.order_lifecycle import OrderLifecyclePhase
from execution.order_lifecycle_journal_v2_strict_native import OrderLifecycleJournalV2SourceCallback
from execution.order_lifecycle_journal_writer_v2_strict_native import (
    OrderLifecycleJournalRuntimeBridgeV2,
    OrderLifecycleJournalWriterV2,
)
from models.replay.order_lifecycle_v2_replay_adapter import (
    OrderLifecycleV2ReplayAdapter as SharedReplayAdapter,
    _positive_ms,
)

REPLAY_ADAPTER_ID = "order_lifecycle_journal_v2.python_replay_adapter.strict_native.v1"


class OrderLifecycleV2ReplayAdapter(SharedReplayAdapter):
    adapter_id = REPLAY_ADAPTER_ID
    callback_factory = OrderLifecycleJournalV2SourceCallback
    writer_factory = OrderLifecycleJournalWriterV2
    bridge_factory = OrderLifecycleJournalRuntimeBridgeV2

    def __init__(self, *, strict_native_only: bool = False,
                 writer_mode: str = "callback_durable", **kwargs: Any) -> None:
        self.strict_native_only = bool(strict_native_only)
        normalized = str(writer_mode).strip()
        if normalized == "day_buffered":
            from execution.order_lifecycle_journal_writer_v2_replay_day_buffered import DayBufferedReplayJournalWriterV2
            self.writer_factory = DayBufferedReplayJournalWriterV2
        elif normalized != "callback_durable":
            raise ValueError("unsupported strict-native lifecycle journal writer mode")
        self._pre_activation_cancel_requested_at_ms: dict[int, int] = {}
        self._pre_activation_cancel_request_count = 0
        self._pre_activation_cancel_ack_count = 0
        super().__init__(**kwargs)

    @classmethod
    def _parameter_options(cls, params: Mapping[str, Any]) -> dict[str, Any]:
        return dict(
            strict_native_only=bool(params.get("order_lifecycle_journal_v2_strict_native_only", False)),
            writer_mode=str(params.get("order_lifecycle_journal_v2_writer_mode", "callback_durable") or "callback_durable"),
        )

    def _callback_extensions(self, order: Mapping[str, Any], callback_type: str) -> dict[str, Any]:
        source = str(order.get("simulator_queue_source", "pending_activation") or "pending_activation")
        exact = bool(order.get("exact_queue_path_valid", order.get("exchange_book_queue_path_valid", False)))
        if self.strict_native_only and callback_type == "order_activation" and source in {"", "pending_activation", "not_recorded"}:
            raise ValueError("strict-native lifecycle activation lacks an explicit queue source")
        return dict(simulator_queue_source=source, exact_queue_path_valid=exact)

    def _after_activation(self, key, lifecycle, visibility_ts_ms) -> None:
        if key in self._pre_activation_cancel_requested_at_ms:
            # Before activation there was no active queue to accrue exposure.
            lifecycle.request_cancel(_positive_ms("pre-activation cancel activation timestamp", visibility_ts_ms) * 1_000_000)
            del self._pre_activation_cancel_requested_at_ms[key]

    def request_cancel(self, order: Mapping[str, Any], visibility_ts_ms: int) -> None:
        key, lifecycle = self._lifecycle(order)
        if lifecycle.phase == OrderLifecyclePhase.SUBMITTED:
            timestamp = _positive_ms("pre-activation cancel request timestamp", visibility_ts_ms)
            self._pre_activation_cancel_requested_at_ms.setdefault(key, timestamp)
            self._pre_activation_cancel_request_count += 1
            return
        super().request_cancel(order, visibility_ts_ms)

    def cancel_reject(self, order: Mapping[str, Any], **kwargs: Any) -> None:
        _, lifecycle = self._lifecycle(order)
        if lifecycle.phase == OrderLifecyclePhase.SUBMITTED:
            raise ValueError("pre-activation cancel reject is unsupported by lifecycle ABI v2")
        super().cancel_reject(order, **kwargs)

    def _after_exchange_terminal(self, key: int, reason: str) -> None:
        pending = key in self._pre_activation_cancel_requested_at_ms
        if pending and reason in {"cancel_ack", "cancel_ack_reconciled"}:
            self._pre_activation_cancel_ack_count += 1
        self._pre_activation_cancel_requested_at_ms.pop(key, None)

    def shutdown_censor(self, order: Mapping[str, Any], visibility_ts_ms: int) -> None:
        key, lifecycle = self._lifecycle(order)
        terminal = lifecycle.phase == OrderLifecyclePhase.EXCHANGE_TERMINAL
        super().shutdown_censor(order, visibility_ts_ms)
        if not terminal:
            self._pre_activation_cancel_requested_at_ms.pop(key, None)

    def close(self) -> dict[str, Any]:
        was_closed = self._closed
        health = super().close()
        if not was_closed:
            health.update(
                adapter_pre_activation_cancel_request_count=self._pre_activation_cancel_request_count,
                adapter_pre_activation_cancel_ack_count=self._pre_activation_cancel_ack_count,
                adapter_pre_activation_cancel_pending_count=len(self._pre_activation_cancel_requested_at_ms),
            )
        return health
