"""Mechanics-only order-lifecycle journal-v2 adapter for Python replay.

The adapter mirrors authoritative replay lifecycle callbacks into the shared
quantity-weighted lifecycle and atomically publishes every unseen event through
the journal-v2 writer.  It never reads value outcomes and never changes replay
order state.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from execution.order_lifecycle import (
    FILL_RISK_PHASES,
    OrderLifecyclePhase,
    QuantityWeightedOrderLifecycle,
)
from execution.order_lifecycle_journal_v2_strict_native import (
    OrderLifecycleJournalV2SourceCallback,
)
from execution.order_lifecycle_journal_writer_v2_strict_native import (
    LifecycleJournalCommitResult,
    OrderLifecycleJournalRuntimeBridgeV2,
    OrderLifecycleJournalWriterV2,
)
from execution.order_lifecycle_quantity_contract import (
    TERMINAL_REMAINDER_ABS_TOLERANCE_BTC,
    validate_fill_terminal_claim,
)

REPLAY_ADAPTER_ID = "order_lifecycle_journal_v2.python_replay_adapter.strict_native.v1"


def _canonical_sha256(value: object) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _positive_ms(label: str, value: object) -> int:
    timestamp = int(value)
    if timestamp <= 0:
        raise ValueError(f"{label} must be positive")
    return timestamp


def _order_key(order: Mapping[str, Any]) -> int:
    raw = order.get("trace_id", -1)
    value = int(-1 if raw is None else raw)
    if value < 0:
        raise ValueError("journal-v2 replay order requires a stable trace_id")
    return value


class OrderLifecycleV2ReplayAdapter:
    """Mirror replay callbacks into one durable mechanics-only journal."""

    def __init__(
        self,
        *,
        root: str | Path,
        session_id: str,
        runtime_identity: Mapping[str, Any],
        symbol: str,
        storage_format: str = "parquet",
        strict_native_only: bool = False,
    ) -> None:
        identity = {
            **dict(runtime_identity),
            "replay_adapter_id": REPLAY_ADAPTER_ID,
            "economic_outcomes_read": False,
            "q90_action_authorized": False,
        }
        self.symbol = str(symbol).strip().upper()
        if not self.symbol:
            raise ValueError("journal-v2 replay symbol is required")
        self.runtime_identity = identity
        self.runtime_identity_sha256 = _canonical_sha256(identity)
        self.strict_native_only = bool(strict_native_only)
        self.writer = OrderLifecycleJournalWriterV2(
            root,
            session_id=str(session_id),
            runtime_identity=identity,
            storage_format=storage_format,
            initial_active_order_ids=(),
            heartbeat_interval_s=60.0,
            start_heartbeat=False,
        )
        self.bridge = OrderLifecycleJournalRuntimeBridgeV2(self.writer)
        self._lifecycles: dict[int, QuantityWeightedOrderLifecycle] = {}
        self._lifecycle_ids: dict[int, str] = {}
        self._censored: set[int] = set()
        self._callback_count = 0
        self._result_status_counts: Counter[str] = Counter()
        self._terminal_reason_counts: Counter[str] = Counter()
        self._pre_activation_cancel_requested_at_ms: dict[int, int] = {}
        self._pre_activation_cancel_request_count = 0
        self._pre_activation_cancel_ack_count = 0
        self._closed = False

    @classmethod
    def from_replay_params(
        cls,
        params: Mapping[str, Any],
        *,
        symbol: str,
    ) -> OrderLifecycleV2ReplayAdapter | None:
        if not bool(params.get("order_lifecycle_journal_v2_enabled", False)):
            return None
        root = params.get("order_lifecycle_journal_v2_root")
        session_id = str(params.get("order_lifecycle_journal_v2_session_id", "") or "")
        identity = params.get("_order_lifecycle_journal_v2_runtime_identity")
        if root is None:
            raise ValueError("enabled journal-v2 replay requires an output root")
        if not session_id:
            raise ValueError("enabled journal-v2 replay requires a session id")
        if not isinstance(identity, Mapping):
            raise ValueError("enabled journal-v2 replay requires a runtime identity mapping")
        return cls(
            root=root,
            session_id=session_id,
            runtime_identity=identity,
            symbol=symbol,
            storage_format=str(
                params.get("order_lifecycle_journal_v2_storage_format", "parquet") or "parquet"
            ),
            strict_native_only=bool(
                params.get("order_lifecycle_journal_v2_strict_native_only", False)
            ),
        )

    def _identity(self, order: Mapping[str, Any]) -> tuple[int, str, str, str]:
        key = _order_key(order)
        submit_ms = _positive_ms(
            "order submit timestamp",
            order.get("submit_ts", order.get("quote_ts", 0)),
        )
        client_order_id = f"replay-{self.symbol}-{key}-{submit_ms}"
        lifecycle_id = f"{self.runtime_identity_sha256[:16]}:{client_order_id}"
        exchange_order_id = f"simulated-exchange-{key}-{submit_ms}"
        return key, lifecycle_id, client_order_id, exchange_order_id

    def _lifecycle(self, order: Mapping[str, Any]) -> tuple[int, QuantityWeightedOrderLifecycle]:
        key = _order_key(order)
        lifecycle = self._lifecycles.get(key)
        if lifecycle is None:
            raise KeyError("journal-v2 lifecycle callback arrived before submit")
        if key in self._censored:
            raise RuntimeError("journal-v2 lifecycle callback arrived after shutdown censor")
        return key, lifecycle

    def _commit(
        self,
        *,
        order: Mapping[str, Any],
        key: int,
        callback_type: str,
        received_ts_ms: int,
        exchange_ts_ms: int | None,
    ) -> LifecycleJournalCommitResult:
        lifecycle = self._lifecycles[key]
        sequence = len(lifecycle.events())
        durable_cursor = self.writer.cursor_for(
            lifecycle_id=self._lifecycle_ids[key],
            client_order_id=self._identity_from_key(key)[0],
        )
        if durable_cursor.last_emitted_sequence > sequence:
            result = LifecycleJournalCommitResult(
                status="noop",
                batch_id="",
                row_count=0,
                checkpoint=durable_cursor.checkpoint(),
                reason="replay_rebuild_before_durable_cursor",
            )
            self._callback_count += 1
            self._result_status_counts[result.status] += 1
            return result
        callback_id = f"{self._lifecycle_ids[key]}:{sequence:08d}:{str(callback_type).strip()}"
        queue_source = str(
            order.get("simulator_queue_source", "pending_activation")
            or "pending_activation"
        )
        exact_queue_path_valid = bool(
            order.get(
                "exact_queue_path_valid",
                order.get("exchange_book_queue_path_valid", False),
            )
        )
        if self.strict_native_only and callback_type == "order_activation":
            if queue_source in {"", "pending_activation", "not_recorded"}:
                raise ValueError(
                    "strict-native lifecycle activation lacks an explicit queue source"
                )
        callback = OrderLifecycleJournalV2SourceCallback(
            callback_id=callback_id,
            callback_type=str(callback_type),
            received_ts_ns=_positive_ms("callback received timestamp", received_ts_ms) * 1_000_000,
            exchange_ts_ns=(
                _positive_ms("callback exchange timestamp", exchange_ts_ms) * 1_000_000
                if exchange_ts_ms is not None
                else None
            ),
            simulator_queue_source=queue_source,
            exact_queue_path_valid=exact_queue_path_valid,
        )
        result = self.bridge.submit_callback(
            lifecycle_id=self._lifecycle_ids[key],
            lifecycle=lifecycle,
            callback=callback,
        )
        self._callback_count += 1
        self._result_status_counts[result.status] += 1
        return result

    def _identity_from_key(self, key: int) -> tuple[str, str]:
        lifecycle_id = self._lifecycle_ids[key]
        marker = f"{self.runtime_identity_sha256[:16]}:"
        if not lifecycle_id.startswith(marker):
            raise ValueError("replay lifecycle identity prefix drift")
        return lifecycle_id[len(marker) :], lifecycle_id

    def submit(self, order: Mapping[str, Any], visibility_ts_ms: int) -> None:
        key, lifecycle_id, client_order_id, exchange_order_id = self._identity(order)
        if key in self._lifecycles:
            lifecycle = self._lifecycles[key]
            if self._lifecycle_ids[key] != lifecycle_id:
                raise ValueError("stable replay lifecycle identity changed")
        else:
            quantity = float(order.get("quantity", order.get("remaining", 0.0)) or 0.0)
            lifecycle = QuantityWeightedOrderLifecycle(
                initial_quantity=quantity,
                submitted_ts_ns=_positive_ms("submit timestamp", visibility_ts_ms) * 1_000_000,
            )
            self._lifecycles[key] = lifecycle
            self._lifecycle_ids[key] = lifecycle_id
            self.bridge.register_lifecycle(
                lifecycle_id=lifecycle_id,
                runtime_source="authoritative_python_replay",
                client_order_id=client_order_id,
                symbol=self.symbol,
                side=str(order.get("side", "")).upper(),
                exchange_order_id=exchange_order_id,
            )
        self._commit(
            order=order,
            key=key,
            callback_type="order_submit",
            received_ts_ms=visibility_ts_ms,
            exchange_ts_ms=None,
        )

    def activate(
        self,
        order: Mapping[str, Any],
        *,
        visibility_ts_ms: int,
        exchange_ts_ms: int,
    ) -> None:
        key, lifecycle = self._lifecycle(order)
        lifecycle.activate(
            _positive_ms("activation visibility timestamp", visibility_ts_ms) * 1_000_000,
            exchange_ts_ns=_positive_ms("activation exchange timestamp", exchange_ts_ms)
            * 1_000_000,
        )
        if key in self._pre_activation_cancel_requested_at_ms:
            # A cancel requested before exchange activation owns no fill-risk
            # time.  Once activation becomes visible, the order enters the
            # cancel-pending risk state immediately rather than backdating the
            # request across a period in which no active queue existed.
            lifecycle.request_cancel(
                _positive_ms(
                    "pre-activation cancel activation timestamp",
                    visibility_ts_ms,
                )
                * 1_000_000
            )
            del self._pre_activation_cancel_requested_at_ms[key]
        self._commit(
            order=order,
            key=key,
            callback_type="order_activation",
            received_ts_ms=visibility_ts_ms,
            exchange_ts_ms=exchange_ts_ms,
        )

    def request_cancel(self, order: Mapping[str, Any], visibility_ts_ms: int) -> None:
        key, lifecycle = self._lifecycle(order)
        if lifecycle.phase == OrderLifecyclePhase.SUBMITTED:
            timestamp = _positive_ms(
                "pre-activation cancel request timestamp",
                visibility_ts_ms,
            )
            self._pre_activation_cancel_requested_at_ms.setdefault(key, timestamp)
            self._pre_activation_cancel_request_count += 1
            return
        before = len(lifecycle.events())
        lifecycle.request_cancel(
            _positive_ms("cancel request timestamp", visibility_ts_ms) * 1_000_000
        )
        if len(lifecycle.events()) == before:
            return
        self._commit(
            order=order,
            key=key,
            callback_type="cancel_request",
            received_ts_ms=visibility_ts_ms,
            exchange_ts_ms=None,
        )

    def cancel_reject(
        self,
        order: Mapping[str, Any],
        *,
        visibility_ts_ms: int,
        exchange_ts_ms: int,
    ) -> None:
        key, lifecycle = self._lifecycle(order)
        if lifecycle.phase == OrderLifecyclePhase.SUBMITTED:
            raise ValueError(
                "pre-activation cancel reject is unsupported by lifecycle ABI v2"
            )
        lifecycle.cancel_rejected(
            _positive_ms("cancel reject visibility timestamp", visibility_ts_ms) * 1_000_000,
            exchange_ts_ns=_positive_ms("cancel reject exchange timestamp", exchange_ts_ms)
            * 1_000_000,
        )
        self._commit(
            order=order,
            key=key,
            callback_type="cancel_reject",
            received_ts_ms=visibility_ts_ms,
            exchange_ts_ms=exchange_ts_ms,
        )

    def fill(
        self,
        order: Mapping[str, Any],
        *,
        remaining_after: float,
        visibility_ts_ms: int,
        exchange_ts_ms: int,
        full_fill: bool,
    ) -> None:
        key, lifecycle = self._lifecycle(order)
        canonical_remaining, canonical_full_fill = validate_fill_terminal_claim(
            remaining_after=remaining_after,
            full_fill_claimed=full_fill,
        )
        lifecycle.observe_fill(
            remaining_after=canonical_remaining,
            visibility_ts_ns=_positive_ms("fill visibility timestamp", visibility_ts_ms)
            * 1_000_000,
            exchange_ts_ns=_positive_ms("fill exchange timestamp", exchange_ts_ms) * 1_000_000,
            full_fill=canonical_full_fill,
        )
        self._commit(
            order=order,
            key=key,
            callback_type="full_fill" if canonical_full_fill else "partial_fill",
            received_ts_ms=visibility_ts_ms,
            exchange_ts_ms=exchange_ts_ms,
        )
        if canonical_full_fill:
            self._terminal_reason_counts["full_fill"] += 1

    def _exchange_terminal(
        self,
        order: Mapping[str, Any],
        *,
        reason: str,
        callback_type: str,
        visibility_ts_ms: int,
        exchange_ts_ms: int,
    ) -> None:
        key, lifecycle = self._lifecycle(order)
        pre_activation_cancel = key in self._pre_activation_cancel_requested_at_ms
        submitted_without_activation = (
            lifecycle.phase == OrderLifecyclePhase.SUBMITTED
            and lifecycle.activation_exchange_ts_ns <= 0
        )
        if submitted_without_activation:
            lifecycle._invalidate_exchange_exposure("source_lifecycle_marked_invalid")
        lifecycle.exchange_terminal(
            _positive_ms("terminal visibility timestamp", visibility_ts_ms) * 1_000_000,
            reason=reason,
            exchange_ts_ns=_positive_ms("terminal exchange timestamp", exchange_ts_ms) * 1_000_000,
        )
        if submitted_without_activation:
            lifecycle.exchange_exposure_complete = False
        if pre_activation_cancel and reason in {"cancel_ack", "cancel_ack_reconciled"}:
            self._pre_activation_cancel_ack_count += 1
        self._pre_activation_cancel_requested_at_ms.pop(key, None)
        self._commit(
            order=order,
            key=key,
            callback_type=callback_type,
            received_ts_ms=visibility_ts_ms,
            exchange_ts_ms=exchange_ts_ms,
        )
        self._terminal_reason_counts[reason] += 1

    def cancel_ack(
        self,
        order: Mapping[str, Any],
        *,
        visibility_ts_ms: int,
        exchange_ts_ms: int,
    ) -> None:
        self._exchange_terminal(
            order,
            reason="cancel_ack",
            callback_type="cancel_ack",
            visibility_ts_ms=visibility_ts_ms,
            exchange_ts_ms=exchange_ts_ms,
        )

    def reject(
        self,
        order: Mapping[str, Any],
        *,
        visibility_ts_ms: int,
        exchange_ts_ms: int,
    ) -> None:
        self._exchange_terminal(
            order,
            reason="rejected",
            callback_type="order_reject",
            visibility_ts_ms=visibility_ts_ms,
            exchange_ts_ms=exchange_ts_ms,
        )

    def expire(
        self,
        order: Mapping[str, Any],
        *,
        visibility_ts_ms: int,
        exchange_ts_ms: int,
    ) -> None:
        self._exchange_terminal(
            order,
            reason="expired",
            callback_type="order_expiry",
            visibility_ts_ms=visibility_ts_ms,
            exchange_ts_ms=exchange_ts_ms,
        )

    def shutdown_censor(self, order: Mapping[str, Any], visibility_ts_ms: int) -> None:
        key, lifecycle = self._lifecycle(order)
        if lifecycle.phase == OrderLifecyclePhase.EXCHANGE_TERMINAL:
            return
        timestamp_ns = _positive_ms("shutdown censor timestamp", visibility_ts_ms) * 1_000_000
        before = lifecycle.phase
        remaining_before, _ = lifecycle._accrue(timestamp_ns)
        lifecycle._record(
            event="local_shutdown_censor",
            visibility_ts_ns=timestamp_ns,
            exchange_ts_ns=0,
            phase_before=before,
            remaining_before=remaining_before,
            reason="shutdown",
        )
        self._commit(
            order=order,
            key=key,
            callback_type="local_shutdown_censor",
            received_ts_ms=visibility_ts_ms,
            exchange_ts_ms=None,
        )
        self._censored.add(key)
        self._pre_activation_cancel_requested_at_ms.pop(key, None)
        self._terminal_reason_counts["shutdown_censor"] += 1

    def shutdown_censor_all(self, visibility_ts_ms: int) -> None:
        for key in sorted(self._lifecycles):
            lifecycle = self._lifecycles[key]
            if key in self._censored or lifecycle.phase == OrderLifecyclePhase.EXCHANGE_TERMINAL:
                continue
            order = {"trace_id": key}
            self.shutdown_censor(order, visibility_ts_ms)

    def close(self) -> dict[str, Any]:
        if self._closed:
            return self.writer.health_snapshot()
        health = self.writer.close()
        self._closed = True
        health.update(
            {
                "replay_adapter_id": REPLAY_ADAPTER_ID,
                "runtime_identity_sha256": self.runtime_identity_sha256,
                "adapter_callback_count": self._callback_count,
                "adapter_result_status_counts": dict(sorted(self._result_status_counts.items())),
                "adapter_terminal_reason_counts": dict(
                    sorted(self._terminal_reason_counts.items())
                ),
                "adapter_lifecycle_count": len(self._lifecycles),
                "adapter_fill_risk_active_count": sum(
                    int(
                        lifecycle.phase in FILL_RISK_PHASES
                        and lifecycle.remaining_quantity
                        > TERMINAL_REMAINDER_ABS_TOLERANCE_BTC
                        and key not in self._censored
                    )
                    for key, lifecycle in self._lifecycles.items()
                ),
                "adapter_pre_activation_cancel_request_count": (
                    self._pre_activation_cancel_request_count
                ),
                "adapter_pre_activation_cancel_ack_count": (
                    self._pre_activation_cancel_ack_count
                ),
                "adapter_pre_activation_cancel_pending_count": len(
                    self._pre_activation_cancel_requested_at_ms
                ),
                "economic_outcomes_read": False,
                "q90_action_authorized": False,
            }
        )
        return health
