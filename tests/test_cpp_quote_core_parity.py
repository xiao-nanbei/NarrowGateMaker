import platform
import struct
import sys
from dataclasses import fields, is_dataclass

import numpy as np
import pytest

from strategy import quote_core as qc
from strategy.policy_guards import CommonSidePolicyInput, evaluate_common_side_policy

narrowgate_cpp = pytest.importorskip("narrowgate_cpp")


def _assert_recursive_exact(actual, expected):
    assert type(actual) is type(expected)
    if isinstance(expected, float):
        assert struct.pack(">d", actual) == struct.pack(">d", expected)
        return
    if is_dataclass(expected):
        for item in fields(expected):
            _assert_recursive_exact(
                getattr(actual, item.name), getattr(expected, item.name)
            )
        return
    if isinstance(expected, dict):
        assert list(actual) == list(expected)
        for key in expected:
            _assert_recursive_exact(actual[key], expected[key])
        return
    if isinstance(expected, (list, tuple)):
        assert len(actual) == len(expected)
        for actual_item, expected_item in zip(actual, expected, strict=True):
            _assert_recursive_exact(actual_item, expected_item)
        return
    assert actual == expected


def test_cpp_live_build_profile_is_queryable_and_consistent():
    profile = narrowgate_cpp.NATIVE_LIVE_BUILD_PROFILE
    options = narrowgate_cpp.NATIVE_LIVE_BUILD_COMPILE_OPTIONS.split()
    production = narrowgate_cpp.NATIVE_LIVE_BUILD_IS_PRODUCTION
    vector_width = narrowgate_cpp.NATIVE_LIVE_BUILD_VECTOR_WIDTH_BITS

    assert profile in {"portable", "ec2-cascadelake-avx2", "host-native"}
    assert "-ffp-contract=off" in options
    assert not any(option in {"-ffast-math", "-Ofast"} for option in options)
    if profile == "ec2-cascadelake-avx2":
        assert options == [
            "-O3",
            "-march=haswell",
            "-mtune=cascadelake",
            "-mprefer-vector-width=256",
            "-fno-fast-math",
            "-ffp-contract=off",
            "-fno-lto",
        ]
        assert production is True
        assert vector_width == 256
    else:
        assert production is False


def test_cpp_transport_contract_is_usdm_only_and_native_backends_default_off():
    assert narrowgate_cpp.TRANSPORT_CONTRACT_ABI_VERSION == 1
    assert (
        narrowgate_cpp.TRANSPORT_CONTRACT_SCHEMA_VERSION
        == "narrowgate_cpp_transport_contract.v1"
    )
    assert narrowgate_cpp.DEFAULT_TRANSPORT_BACKEND == (
        narrowgate_cpp.TransportBackendKind.PythonUsdmLegacy
    )
    assert narrowgate_cpp.CPP_USDM_FIX_AVAILABLE is False
    assert narrowgate_cpp.transport_backend_available(
        narrowgate_cpp.TransportBackendKind.PythonUsdmLegacy
    )
    for backend in (
        narrowgate_cpp.TransportBackendKind.CppUsdmWebSocket,
        narrowgate_cpp.TransportBackendKind.CppUsdmRest,
        narrowgate_cpp.TransportBackendKind.CppUsdmFix,
    ):
        assert not narrowgate_cpp.transport_backend_available(backend)

    reason = narrowgate_cpp.transport_backend_unavailable_reason(
        narrowgate_cpp.TransportBackendKind.CppUsdmFix
    )
    assert "USD-M Futures FIX is unavailable" in reason
    assert "Spot-only" in reason

    header = narrowgate_cpp.CanonicalEventHeader()
    header.backend = narrowgate_cpp.TransportBackendKind.PythonUsdmLegacy
    header.event_kind = narrowgate_cpp.CanonicalEventKind.OrderUpdate
    header.symbol = "BTCUSDC"
    header.generation = 7
    header.exchange_event_time_ns = 101
    header.local_receive_time_ns = 102
    header.feature_ready_time_ns = 103
    header.source_sequence = 11
    header.ingress_sequence = 12
    assert header.product == narrowgate_cpp.TransportProduct.UsdMFutures
    assert header.venue == "BINANCE"
    assert header.symbol == "BTCUSDC"
    assert header.generation == 7
    assert header.ingress_sequence == 12

    intent = narrowgate_cpp.CanonicalOrderIntent()
    assert not intent.is_structurally_valid()
    assert intent.validation_error() == "request_id is required"
    intent.request_id = "request-1"
    intent.decision_id = "decision-1"
    intent.client_order_id = "cid-1"
    intent.symbol = "BTCUSDC"
    intent.side = narrowgate_cpp.CanonicalSide.Buy
    intent.order_type = narrowgate_cpp.CanonicalOrderType.Limit
    intent.time_in_force = narrowgate_cpp.CanonicalTimeInForce.Gtx
    intent.price = 100_000.1
    intent.quantity = 0.001
    intent.post_only = True
    intent.expected_ownership_generation = 7
    assert intent.product == narrowgate_cpp.TransportProduct.UsdMFutures
    assert intent.order_type == narrowgate_cpp.CanonicalOrderType.Limit
    assert intent.time_in_force == narrowgate_cpp.CanonicalTimeInForce.Gtx
    assert intent.expected_ownership_generation == 7
    assert intent.is_structurally_valid()
    assert intent.validation_error() == ""
    intent.abi_version += 1
    assert not intent.is_structurally_valid()
    assert intent.validation_error() == "unsupported transport ABI version"
    intent.abi_version = narrowgate_cpp.TRANSPORT_CONTRACT_ABI_VERSION
    assert intent.is_structurally_valid()

    emergency = narrowgate_cpp.CanonicalOrderIntent()
    emergency.request_id = "emergency-1"
    emergency.client_order_id = "cid-emergency-1"
    emergency.symbol = "BTCUSDC"
    emergency.side = narrowgate_cpp.CanonicalSide.Buy
    emergency.order_type = narrowgate_cpp.CanonicalOrderType.Market
    emergency.quantity = 0.001
    emergency.reduce_only = True
    assert emergency.order_type == narrowgate_cpp.CanonicalOrderType.Market
    assert emergency.is_structurally_valid()

    emergency.post_only = True
    assert not emergency.is_structurally_valid()
    assert emergency.validation_error() == "post_only requires a GTX limit order"

    cancel = narrowgate_cpp.CanonicalCancelIntent()
    cancel.request_id = "cancel-1"
    cancel.client_order_id = "cid-1"
    cancel.exchange_order_id = 123
    cancel.symbol = "BTCUSDC"
    cancel.reason = "replacement"
    cancel.expected_ownership_generation = 7
    assert cancel.product == narrowgate_cpp.TransportProduct.UsdMFutures
    assert cancel.exchange_order_id == 123
    assert cancel.expected_ownership_generation == 7
    assert cancel.is_structurally_valid()
    cancel.abi_version += 1
    assert not cancel.is_structurally_valid()
    assert cancel.validation_error() == "unsupported transport ABI version"

    cancel_all = narrowgate_cpp.CanonicalCancelAllIntent()
    cancel_all.request_id = "cancel-all-1"
    cancel_all.symbol = "BTCUSDC"
    cancel_all.reason = "risk_stop"
    cancel_all.expected_ownership_generation = 8
    assert cancel_all.product == narrowgate_cpp.TransportProduct.UsdMFutures
    assert cancel_all.expected_ownership_generation == 8
    assert cancel_all.is_structurally_valid()
    cancel_all.abi_version += 1
    assert not cancel_all.is_structurally_valid()
    assert cancel_all.validation_error() == "unsupported transport ABI version"


def test_cpp_transport_unknown_dispatch_blocks_cross_backend_retry():
    receipt = narrowgate_cpp.TransportReceipt()
    receipt.request_id = "request-1"
    receipt.backend = narrowgate_cpp.TransportBackendKind.CppUsdmRest
    receipt.phase = narrowgate_cpp.TransportPhase.LocalValidated
    receipt.unknown_state = (
        narrowgate_cpp.TransportUnknownState.ConfirmedNotDispatched
    )
    assert receipt.allows_cross_backend_retry()

    receipt.phase = narrowgate_cpp.TransportPhase.WireDispatched
    assert not receipt.allows_cross_backend_retry()

    receipt.phase = narrowgate_cpp.TransportPhase.Enqueued
    receipt.unknown_state = (
        narrowgate_cpp.TransportUnknownState.MayHaveBeenDispatched
    )
    assert not receipt.allows_cross_backend_retry()

    receipt.unknown_state = (
        narrowgate_cpp.TransportUnknownState.AwaitingReconciliation
    )
    assert not receipt.allows_cross_backend_retry()


def _native_limit_intent(*, request_id="request-1", client_order_id="cid-1"):
    intent = narrowgate_cpp.CanonicalOrderIntent()
    intent.request_id = request_id
    intent.decision_id = f"decision-{request_id}"
    intent.client_order_id = client_order_id
    intent.symbol = "BTCUSDC"
    intent.side = narrowgate_cpp.CanonicalSide.Buy
    intent.order_type = narrowgate_cpp.CanonicalOrderType.Limit
    intent.time_in_force = narrowgate_cpp.CanonicalTimeInForce.Gtx
    intent.price = 100_000.1
    intent.quantity = 0.001
    intent.post_only = True
    intent.recv_window_ms = 5_000
    intent.deadline_time_ns = 9_000
    intent.expected_ownership_generation = 7
    return intent


def _native_cancel_intent(*, request_id="cancel-1", client_order_id="cid-1"):
    intent = narrowgate_cpp.CanonicalCancelIntent()
    intent.request_id = request_id
    intent.decision_id = f"decision-{request_id}"
    intent.client_order_id = client_order_id
    intent.symbol = "BTCUSDC"
    intent.reason = "safety"
    intent.exchange_order_id = 123
    intent.expected_ownership_generation = 7
    return intent


def test_native_order_gateway_core_preserves_fifo_identity_and_stage_timestamps():
    assert narrowgate_cpp.NATIVE_ORDER_GATEWAY_CORE_AVAILABLE is True
    assert narrowgate_cpp.NATIVE_ORDER_GATEWAY_WIRE_ADAPTER_AVAILABLE is False
    assert narrowgate_cpp.NATIVE_ORDER_GATEWAY_QUEUE_CAPACITY == 256
    assert narrowgate_cpp.NATIVE_ORDER_GATEWAY_SAFETY_RESERVE == 16
    assert narrowgate_cpp.NATIVE_ORDER_GATEWAY_WIRE_REQUEST_BYTES <= 512
    expected_isolation = (
        128
        if platform.system() == "Darwin" and platform.machine() == "arm64"
        else 64
    )
    assert narrowgate_cpp.NATIVE_ORDER_GATEWAY_CACHE_LINE_BYTES == expected_isolation

    gateway = narrowgate_cpp.NativeUsdMOrderGatewayCore(
        narrowgate_cpp.TransportBackendKind.CppUsdmRest
    )
    enqueued = gateway.enqueue_order(
        _native_limit_intent(),
        decision_time_ns=1_000,
        enqueue_time_ns=1_100,
    )
    assert enqueued.admitted is True
    assert enqueued.phase == narrowgate_cpp.TransportPhase.Enqueued
    assert enqueued.request_id == "request-1"
    assert enqueued.generation == 7
    assert gateway.pending_count == 1

    result = gateway.begin_next(
        dequeue_time_ns=1_200,
        generation=3,
        current_ownership_generation=7,
    )
    assert result.invalidations == []
    request = result.request
    assert request is not None
    assert request.operation == narrowgate_cpp.NativeGatewayOperation.Place
    assert request.request_id == "request-1"
    assert request.client_order_id == "cid-1"
    assert request.symbol == "BTCUSDC"
    assert request.price == pytest.approx(100_000.1)
    assert request.quantity == pytest.approx(0.001)
    assert request.decision_time_ns == 1_000
    assert request.enqueue_time_ns == 1_100
    assert request.dequeue_time_ns == 1_200
    assert request.generation == 3

    attempted = gateway.mark_send_attempted(dispatch_time_ns=1_300)
    assert attempted.send_attempted is True
    assert attempted.generation == 3
    dispatched = gateway.mark_wire_dispatched(
        dispatch_time_ns=1_300,
        wire_time_ns=1_350,
    )
    assert dispatched.phase == narrowgate_cpp.TransportPhase.WireDispatched
    assert dispatched.dispatch_time_ns == 1_300
    assert dispatched.wire_time_ns == 1_350
    assert not dispatched.allows_cross_backend_retry()

    accepted = gateway.mark_exchange_ack(
        True,
        response_time_ns=1_600,
        completion_time_ns=1_650,
        # The venue timestamp is deliberately from a different epoch/domain.
        exchange_time_ns=42,
        reason="NEW",
    )
    assert accepted.phase == narrowgate_cpp.TransportPhase.ExchangeAckAccepted
    assert accepted.request_id == "request-1"
    assert accepted.response_time_ns == 1_600
    assert accepted.completion_time_ns == 1_650
    assert accepted.exchange_time_ns == 42
    assert gateway.has_active_request is False
    assert gateway.enqueued_count == 1
    assert gateway.dequeued_count == 1
    assert gateway.dispatched_count == 1
    assert gateway.accepted_count == 1


def test_native_order_gateway_unknown_after_dispatch_latches_reconciliation():
    gateway = narrowgate_cpp.NativeUsdMOrderGatewayCore(
        narrowgate_cpp.TransportBackendKind.CppUsdmRest
    )
    gateway.enqueue_order(
        _native_limit_intent(),
        decision_time_ns=1_000,
        enqueue_time_ns=1_100,
    )
    stale_pending = gateway.enqueue_order(
        _native_limit_intent(
            request_id="request-stale",
            client_order_id="cid-stale",
        ),
        decision_time_ns=1_050,
        enqueue_time_ns=1_150,
    )
    assert stale_pending.admitted is True
    gateway.begin_next(
        dequeue_time_ns=1_200,
        generation=4,
        current_ownership_generation=7,
    )
    gateway.mark_send_attempted(dispatch_time_ns=1_300)
    gateway.mark_wire_dispatched(dispatch_time_ns=1_300, wire_time_ns=1_350)
    unknown = gateway.mark_transport_unknown(
        completion_time_ns=2_000,
        reason="response_timeout",
    )
    assert unknown.phase == narrowgate_cpp.TransportPhase.WireDispatched
    assert unknown.unknown_state == (
        narrowgate_cpp.TransportUnknownState.AwaitingReconciliation
    )
    assert unknown.reconciliation_required is True
    assert not unknown.allows_cross_backend_retry()
    assert gateway.reconciliation_required is True
    reconciliation_epoch = gateway.reconciliation_epoch
    assert reconciliation_epoch == 1

    blocked = gateway.enqueue_order(
        _native_limit_intent(request_id="request-2", client_order_id="cid-2"),
        decision_time_ns=2_100,
        enqueue_time_ns=2_200,
    )
    assert blocked.admitted is False
    assert blocked.reason == "reconciliation_required"
    assert blocked.unknown_state == (
        narrowgate_cpp.TransportUnknownState.ConfirmedNotDispatched
    )
    assert blocked.request_id == "request-2"
    assert blocked.decision_id == "decision-request-2"
    assert blocked.client_order_id == "cid-2"
    assert blocked.retry_permitted is False
    assert not blocked.allows_cross_backend_retry()

    gateway.mark_reconciled(
        generation=5,
        expected_reconciliation_epoch=reconciliation_epoch,
    )
    assert gateway.reconciliation_required is False
    assert gateway.reconciled_generation == 5
    # Anything admitted before the ambiguous wire write is invalidated.  It
    # cannot execute merely because an external reconciliation later clears
    # the latch; the strategy must issue a fresh intent from current state.
    drained = gateway.begin_next(
        dequeue_time_ns=2_250,
        generation=5,
        current_ownership_generation=7,
    )
    assert drained.request is None
    assert len(drained.invalidations) == 1
    assert gateway.invalidated_count == 1
    resumed = gateway.enqueue_order(
        _native_limit_intent(request_id="request-3", client_order_id="cid-3"),
        decision_time_ns=2_300,
        enqueue_time_ns=2_400,
    )
    assert resumed.admitted is True


def test_native_gateway_timestamp_and_send_attempt_contract():
    gateway = narrowgate_cpp.NativeUsdMOrderGatewayCore(
        narrowgate_cpp.TransportBackendKind.CppUsdmRest
    )
    with pytest.raises(ValueError, match="decision<=enqueue"):
        gateway.enqueue_order(_native_limit_intent(), 2_000, 1_000)
    gateway.enqueue_order(_native_limit_intent(), 1_000, 1_100)
    with pytest.raises(ValueError, match="enqueue<=dequeue"):
        gateway.begin_next(1_050, 1, 7)
    request = gateway.begin_next(1_200, 1, 7).request
    assert request is not None
    with pytest.raises(ValueError, match="dequeue<=dispatch"):
        gateway.mark_send_attempted(1_100)
    attempted = gateway.mark_send_attempted(1_300)
    assert attempted.send_attempted is True
    with pytest.raises(ValueError, match="original send-attempt"):
        gateway.mark_wire_dispatched(1_301, 1_350)
    gateway.mark_wire_dispatched(1_300, 1_350)
    with pytest.raises(ValueError, match="monotonic"):
        gateway.mark_exchange_ack(True, 1_340, 1_400, 0, "NEW")
    accepted = gateway.mark_exchange_ack(True, 1_500, 1_600, 42, "NEW")
    assert accepted.exchange_time_ns == 42


def test_native_gateway_presend_retry_is_explicit_and_unknown_is_not():
    gateway = narrowgate_cpp.NativeUsdMOrderGatewayCore(
        narrowgate_cpp.TransportBackendKind.CppUsdmRest
    )
    gateway.enqueue_order(_native_limit_intent(), 1_000, 1_100)
    gateway.begin_next(1_200, 1, 7)
    retryable = gateway.mark_confirmed_not_dispatched(1_250, "socket_not_open")
    assert retryable.send_attempted is False
    assert retryable.allows_cross_backend_retry()

    gateway.enqueue_order(
        _native_limit_intent(request_id="sent", client_order_id="sent-cid"),
        1_300,
        1_400,
    )
    gateway.begin_next(1_500, 2, 7)
    gateway.mark_send_attempted(1_600)
    with pytest.raises(RuntimeError, match="after a send attempt"):
        gateway.mark_confirmed_not_dispatched(1_700, "cannot_prove")
    unknown = gateway.mark_transport_unknown(1_700, "write_result_unknown")
    assert unknown.reconciliation_required
    assert not unknown.allows_cross_backend_retry()


def test_native_gateway_generation_invalidation_is_auditable():
    gateway = narrowgate_cpp.NativeUsdMOrderGatewayCore(
        narrowgate_cpp.TransportBackendKind.CppUsdmRest
    )
    gateway.enqueue_order(_native_limit_intent(), 1_000, 1_100)
    result = gateway.begin_next(1_200, 1, 8)
    assert result.request is None
    assert len(result.invalidations) == 1
    invalidated = result.invalidations[0]
    assert invalidated.request_id == "request-1"
    assert invalidated.reason == "ownership_generation_mismatch"
    assert invalidated.retry_permitted is False


def test_native_gateway_cancel_all_fences_queued_place_until_release():
    gateway = narrowgate_cpp.NativeUsdMOrderGatewayCore(
        narrowgate_cpp.TransportBackendKind.CppUsdmRest
    )
    gateway.enqueue_order(_native_limit_intent(), 1_000, 1_100)
    cancel_all = narrowgate_cpp.CanonicalCancelAllIntent()
    cancel_all.request_id = "risk-stop"
    cancel_all.symbol = "BTCUSDC"
    cancel_all.reason = "risk_stop"
    cancel_all.expected_ownership_generation = 7
    receipt = gateway.enqueue_cancel_all(cancel_all, 1_150, 1_160)
    assert receipt.safety_barrier_latched
    blocked = gateway.enqueue_order(
        _native_limit_intent(request_id="reopen", client_order_id="reopen-cid"),
        1_170,
        1_180,
    )
    assert not blocked.admitted
    assert not blocked.allows_cross_backend_retry()
    result = gateway.begin_next(1_200, 1, 7)
    assert len(result.invalidations) == 1
    assert result.request.operation == narrowgate_cpp.NativeGatewayOperation.CancelAll
    gateway.mark_confirmed_not_dispatched(1_250, "test_terminal")
    gateway.release_safety_barrier(1, safety_action_resolved=True)
    assert not gateway.safety_barrier_latched
    assert gateway.enqueue_order(
        _native_limit_intent(request_id="fresh", client_order_id="fresh-cid"),
        1_300,
        1_400,
    ).admitted


def test_native_gateway_reconciliation_requires_exact_epoch_token():
    gateway = narrowgate_cpp.NativeUsdMOrderGatewayCore(
        narrowgate_cpp.TransportBackendKind.CppUsdmRest
    )
    gateway.enqueue_order(_native_limit_intent(), 1_000, 1_100)
    gateway.begin_next(1_200, 1, 7)
    gateway.mark_send_attempted(1_300)
    gateway.mark_transport_unknown(1_400, "write_unknown")

    with pytest.raises(ValueError, match="epoch changed"):
        gateway.mark_reconciled(1, gateway.reconciliation_epoch + 1)
    assert gateway.reconciliation_required
    gateway.mark_reconciled(1, gateway.reconciliation_epoch)
    assert not gateway.reconciliation_required


def test_native_gateway_unwritten_safety_cancel_requires_escalation():
    gateway = narrowgate_cpp.NativeUsdMOrderGatewayCore(
        narrowgate_cpp.TransportBackendKind.CppUsdmRest
    )
    cancel = gateway.enqueue_cancel(
        _native_cancel_intent(),
        1_000,
        1_100,
        True,
    )
    assert cancel.admitted
    assert gateway.begin_next(1_200, 1, 7).request is not None
    not_written = gateway.mark_confirmed_not_dispatched(
        1_250,
        "socket_not_open",
    )
    assert not_written.safety_escalation_required
    assert not not_written.allows_cross_backend_retry()
    assert gateway.safety_escalation_pending
    with pytest.raises(RuntimeError, match="before resolving escalation"):
        gateway.release_safety_barrier(1)
    gateway.release_safety_barrier(1, safety_action_resolved=True)
    assert not gateway.safety_escalation_pending


def test_native_gateway_long_unknown_reason_is_bounded_and_latches():
    gateway = narrowgate_cpp.NativeUsdMOrderGatewayCore(
        narrowgate_cpp.TransportBackendKind.CppUsdmRest
    )
    gateway.enqueue_order(_native_limit_intent(), 1_000, 1_100)
    gateway.begin_next(1_200, 1, 7)
    gateway.mark_send_attempted(1_300)
    long_reason = "tls-error:" + ("x" * 4_096)
    unknown = gateway.mark_transport_unknown(1_400, long_reason)
    assert unknown.reason_truncated
    assert len(unknown.reason) == 127
    assert gateway.reconciliation_required
    assert not gateway.has_active_request


def test_native_gateway_rejected_safety_cancel_requires_escalation():
    gateway = narrowgate_cpp.NativeUsdMOrderGatewayCore(
        narrowgate_cpp.TransportBackendKind.CppUsdmRest
    )
    gateway.enqueue_cancel(_native_cancel_intent(), 1_000, 1_100, True)
    request = gateway.begin_next(1_200, 1, 7).request
    assert request.safety_fence
    gateway.mark_send_attempted(1_300)
    gateway.mark_wire_dispatched(1_300, 1_350)
    rejected = gateway.mark_exchange_ack(
        False,
        response_time_ns=1_400,
        completion_time_ns=1_450,
        exchange_time_ns=1_375,
        reason="cancel_rejected",
    )
    assert rejected.safety_escalation_required
    assert gateway.safety_escalation_pending


def test_native_gateway_returns_stale_receipts_before_future_queue_head():
    gateway = narrowgate_cpp.NativeUsdMOrderGatewayCore(
        narrowgate_cpp.TransportBackendKind.CppUsdmRest
    )
    gateway.enqueue_order(_native_limit_intent(), 1_000, 1_100)
    cancel_all = narrowgate_cpp.CanonicalCancelAllIntent()
    cancel_all.request_id = "future-cancel-all"
    cancel_all.symbol = "BTCUSDC"
    cancel_all.reason = "risk_stop"
    cancel_all.expected_ownership_generation = 7
    gateway.enqueue_cancel_all(cancel_all, 1_900, 2_000)

    partial = gateway.begin_next(1_500, 1, 7)
    assert partial.request is None
    assert len(partial.invalidations) == 1
    assert partial.invalidations[0].reason == "invalidated_by_safety_barrier"
    assert gateway.pending_count == 1

    later = gateway.begin_next(2_100, 2, 7)
    assert later.request is not None
    assert later.request.operation == narrowgate_cpp.NativeGatewayOperation.CancelAll


def test_native_gateway_safety_work_survives_reconciliation_and_place_backlog():
    gateway = narrowgate_cpp.NativeUsdMOrderGatewayCore(
        narrowgate_cpp.TransportBackendKind.CppUsdmRest
    )
    place_capacity = (
        narrowgate_cpp.NATIVE_ORDER_GATEWAY_QUEUE_CAPACITY
        - narrowgate_cpp.NATIVE_ORDER_GATEWAY_SAFETY_RESERVE
    )
    for index in range(place_capacity):
        assert gateway.enqueue_order(
            _native_limit_intent(
                request_id=f"place-{index}",
                client_order_id=f"place-cid-{index}",
            ),
            1_000,
            1_100,
        ).admitted
    full = gateway.enqueue_order(
        _native_limit_intent(request_id="place-full", client_order_id="place-full"),
        1_000,
        1_100,
    )
    assert not full.admitted
    assert full.reason == "place_queue_full"

    cancel_all = narrowgate_cpp.CanonicalCancelAllIntent()
    cancel_all.request_id = "cancel-all-reserved"
    cancel_all.symbol = "BTCUSDC"
    cancel_all.reason = "risk_stop"
    cancel_all.expected_ownership_generation = 7
    cancel_receipt = gateway.enqueue_cancel_all(cancel_all, 1_150, 1_160)
    assert cancel_receipt.admitted
    assert not cancel_receipt.safety_escalation_required
    drained = gateway.begin_next(1_200, 1, 7)
    assert len(drained.invalidations) == place_capacity
    assert drained.request.operation == narrowgate_cpp.NativeGatewayOperation.CancelAll


def test_native_gateway_active_place_is_fenced_before_send():
    gateway = narrowgate_cpp.NativeUsdMOrderGatewayCore(
        narrowgate_cpp.TransportBackendKind.CppUsdmRest
    )
    gateway.enqueue_order(_native_limit_intent(), 1_000, 1_100)
    assert gateway.begin_next(1_200, 1, 7).request is not None

    cancel_all = narrowgate_cpp.CanonicalCancelAllIntent()
    cancel_all.request_id = "risk-stop-active"
    cancel_all.symbol = "BTCUSDC"
    cancel_all.reason = "risk_stop"
    cancel_all.expected_ownership_generation = 7
    assert gateway.enqueue_cancel_all(cancel_all, 1_210, 1_220).admitted

    fenced = gateway.mark_send_attempted(1_230)
    assert not fenced.admitted
    assert fenced.unknown_state == (
        narrowgate_cpp.TransportUnknownState.ConfirmedNotDispatched
    )
    assert fenced.reason == "invalidated_by_safety_barrier_before_send"
    assert not gateway.has_active_request
    next_request = gateway.begin_next(1_240, 2, 7).request
    assert next_request.operation == narrowgate_cpp.NativeGatewayOperation.CancelAll


def test_native_gateway_allows_cancel_while_reconciliation_is_required():
    gateway = narrowgate_cpp.NativeUsdMOrderGatewayCore(
        narrowgate_cpp.TransportBackendKind.CppUsdmRest
    )
    gateway.enqueue_order(_native_limit_intent(), 1_000, 1_100)
    gateway.begin_next(1_200, 1, 7)
    gateway.mark_send_attempted(1_300)
    gateway.mark_transport_unknown(1_400, "write_unknown")
    assert gateway.reconciliation_required

    cancel = gateway.enqueue_cancel(
        _native_cancel_intent(),
        1_410,
        1_420,
        True,
    )
    assert cancel.admitted
    request = gateway.begin_next(1_430, 2, 7).request
    assert request.operation == narrowgate_cpp.NativeGatewayOperation.Cancel


def test_native_order_gateway_refuses_nonexistent_usdm_fix_backend():
    with pytest.raises(ValueError, match="USD-M FIX is not an official Binance product"):
        narrowgate_cpp.NativeUsdMOrderGatewayCore(
            narrowgate_cpp.TransportBackendKind.CppUsdmFix
        )


def _cfg(**overrides):
    values = dict(
        gamma=0.01,
        kappa=1.0,
        tick_size=0.1,
        lot_size=0.001,
        maker_fee=0.0,
        order_size=0.001,
        max_inventory=0.02,
        ml_enabled=True,
        vol_blend=0.2,
        skew_strength=0.15,
        asym_strength=0.20,
        ret_skew=0.05,
        dynamic_cap_enabled=True,
        dynamic_cap_base_bps=20.0,
        dynamic_cap_alpha=0.5,
        dynamic_cap_var_baseline=1.0,
        max_spread_bps=20.0,
    )
    values.update(overrides)
    values.setdefault(
        "f03_ret_action_horizon_s", values.get("quote_horizon_s", 1.0)
    )
    values.setdefault("f03_ret_action_compatible", True)
    return qc.QuoteCoreConfig(**values)


def test_markout_asymmetry_default_uses_maker_signed_direction():
    assert _cfg().markout_side_asymmetry_sign == 1.0
    assert narrowgate_cpp.QuoteCoreConfig().markout_side_asymmetry_sign == 1.0


def test_spread_cap_missing_field_defaults_fail_closed_in_python_and_cpp():
    assert _cfg().spread_cap_mode == qc.SPREAD_CAP_PAUSE_EXPOSURE
    assert narrowgate_cpp.QuoteCoreConfig().spread_cap_mode == (
        qc.SPREAD_CAP_PAUSE_EXPOSURE
    )
    assert qc.quote_core_config_from_params(
        {
            "gamma": 0.01,
            "kappa": 1.0,
            "maker_fee": 0.0,
            "order_size": 0.001,
            "max_inventory": 0.01,
        },
        tick_size=0.1,
        lot_size=0.001,
        use_ml=False,
        use_depth_microprice=False,
        use_depth_kappa=False,
    ).spread_cap_mode == (
        qc.SPREAD_CAP_PAUSE_EXPOSURE
    )


@pytest.mark.parametrize(
    ("side", "inventory", "quantity", "expected"),
    (
        ("BUY", 0.0, 0.001, True),
        ("BUY", -0.002, 0.001, False),
        ("BUY", -0.001, 0.001, False),
        ("BUY", -0.0005, 0.001, True),
        ("SELL", 0.0, 0.001, True),
        ("SELL", 0.002, 0.001, False),
        ("SELL", 0.001, 0.001, False),
        ("SELL", 0.0005, 0.001, True),
    ),
)
def test_exposure_role_is_quantity_aware_and_cross_zero_fails_closed(
    side, inventory, quantity, expected
):
    assert qc._exposure_increasing(side, inventory, quantity, 0.001) is expected


@pytest.mark.parametrize("lot_size", (0.0, float("nan"), float("inf")))
def test_exposure_role_fails_closed_when_lot_size_is_invalid(lot_size):
    assert qc._exposure_increasing("BUY", -0.001, 0.001, lot_size) is True
    assert qc._exposure_increasing("SELL", 0.001, 0.001, lot_size) is True


def test_exact_one_lot_close_avoids_adverse_price_size_and_reason_with_cpp_parity(
    monkeypatch,
):
    cfg = _cfg(
        ml_enabled=False,
        dynamic_cap_enabled=False,
        max_spread_bps=0.0,
        adverse_guard_enabled=True,
        adverse_toxicity_threshold=0.7,
        adverse_spread_mult=2.0,
        adverse_pause=False,
        markout_spread_scale=0.0,
    )
    pred = qc.QuotePrediction(tox_bid=1.0, tox_ask=0.0)
    exact_close = _state(inventory=-0.001, mo_ema_bid=0.0, mo_ema_ask=0.0)
    cross_zero = _state(inventory=-0.0005, mo_ema_bid=0.0, mo_ema_ask=0.0)

    monkeypatch.delenv("NARROWGATE_CPP_QUOTE_CORE", raising=False)
    exact_py = qc.compute_quote_core(exact_close, cfg, pred, qc.DepthSnapshot())
    cross_py = qc.compute_quote_core(cross_zero, cfg, pred, qc.DepthSnapshot())
    exact_context = exact_py.quote_context["BUY"]
    cross_context = cross_py.quote_context["BUY"]
    exact_policy = evaluate_common_side_policy(
        CommonSidePolicyInput(
            exposure_increasing=False,
            side_adverse=exact_context["side_adverse"],
            side_adverse_pause=exact_context["side_adverse_pause"],
        )
    )
    cross_policy = evaluate_common_side_policy(
        CommonSidePolicyInput(
            exposure_increasing=True,
            side_adverse=cross_context["side_adverse"],
            side_adverse_pause=cross_context["side_adverse_pause"],
        )
    )

    assert exact_context["side_adverse"] is False
    assert exact_policy.size_mult == 1.0
    assert exact_policy.reason_mask == 0
    assert cross_context["side_adverse"] is True
    assert cross_policy.size_mult == pytest.approx(0.7)
    assert cross_policy.reason_mask != 0
    assert cross_py.bid_price < exact_py.bid_price

    monkeypatch.setenv("NARROWGATE_CPP_QUOTE_CORE", "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    exact_cpp = qc.compute_quote_core(exact_close, cfg, pred, qc.DepthSnapshot())
    cross_cpp = qc.compute_quote_core(cross_zero, cfg, pred, qc.DepthSnapshot())
    assert exact_cpp.quote_context["BUY"]["side_adverse"] is False
    assert cross_cpp.quote_context["BUY"]["side_adverse"] is True
    assert exact_cpp.bid_price == pytest.approx(exact_py.bid_price, abs=cfg.tick_size * 0.51)
    assert cross_cpp.bid_price == pytest.approx(cross_py.bid_price, abs=cfg.tick_size * 0.51)


def _state(i=0, **overrides):
    values = dict(
        mid=100.0 + i,
        inventory=(i - 2) * 0.001,
        sigma_sq=1.0 + i * 0.1,
        trade_intensity=100.0,
        best_bid=99.9 + i,
        best_ask=100.1 + i,
        mo_ema_bid=-1.0,
        mo_ema_ask=-0.5,
    )
    values.update(overrides)
    return qc.QuoteState(**values)


def _pred(i=0):
    return qc.QuotePrediction(
        dir_10s=0.45 + i * 0.02,
        vol_10s=1.5,
        ret_10s=(i - 2) * 1e-5,
        tox_bid=0.55,
        tox_ask=0.52,
    )


def _depth():
    return qc.DepthSnapshot(
        bids=((99.9, 2.0), (99.8, 3.0), (99.7, 4.0)),
        asks=((100.1, 2.5), (100.2, 2.0), (100.3, 5.0)),
    )


def test_cpp_quote_core_scalar_parity(monkeypatch):
    cases = [
        (_state(0), _cfg(), _pred(0), qc.DepthSnapshot()),
        (_state(1), _cfg(use_depth_microprice=True, use_depth_kappa=True), _pred(1), _depth()),
        (
            _state(1),
            _cfg(
                eta_inventory=0.02,
                a_spread=0.03,
                quote_horizon_s=5.0,
            ),
            _pred(1),
            qc.DepthSnapshot(),
        ),
        (
            _state(2, inventory=0.004, mo_ema_bid=-6.0, mo_ema_ask=-7.0),
            _cfg(adverse_guard_enabled=True, adverse_markout_threshold=5.0, adverse_pause=False),
            _pred(2),
            _depth(),
        ),
        (
            _state(1, inventory=0.004),
            _cfg(
                p3_delta_star=0.5,
                p3_kappa_eff=0.1,
                p3_side_bbo_floor_enabled=True,
                p3_event_type="touch",
                p3_horizon_s=10.0,
                p3_distance_origin=(
                    "same_side_best_bid_or_ask_at_window_start"
                ),
                p3_distance_unit="USDC_per_BTC",
                p3_side="pooled_buy_sell",
                p3_queue_included=False,
                p3_artifact_sha256="c" * 64,
            ),
            _pred(1),
            qc.DepthSnapshot(),
        ),
    ]
    for state, cfg, pred, depth in cases:
        monkeypatch.delenv("NARROWGATE_CPP_QUOTE_CORE", raising=False)
        py = qc.compute_quote_core(state, cfg, pred, depth)
        monkeypatch.setenv("NARROWGATE_CPP_QUOTE_CORE", "1")
        monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
        cpp = qc.compute_quote_core(state, cfg, pred, depth)

        assert cpp.bid_price == pytest.approx(py.bid_price, abs=cfg.tick_size * 0.51)
        assert cpp.ask_price == pytest.approx(py.ask_price, abs=cfg.tick_size * 0.51)
        assert cpp.spread == pytest.approx(py.spread, abs=cfg.tick_size * 1.01)


def test_cpp_p3_side_floor_constraint_flags_are_side_specific(monkeypatch):
    cfg = _cfg(
        kappa=0.1,
        ml_enabled=False,
        dynamic_cap_enabled=False,
        max_spread_bps=0.0,
        inventory_skew_strength=2.0,
        p3_delta_star=0.1,
        p3_side_bbo_floor_enabled=True,
        p3_event_type="touch",
        p3_horizon_s=10.0,
        p3_distance_origin="same_side_best_bid_or_ask_at_window_start",
        p3_distance_unit="USDC_per_BTC",
        p3_side="pooled_buy_sell",
        p3_queue_included=False,
        p3_artifact_sha256="c" * 64,
    )
    state = _state(0, inventory=-0.02, sigma_sq=1.0)
    pred = qc.QuotePrediction()

    monkeypatch.delenv("NARROWGATE_CPP_QUOTE_CORE", raising=False)
    py = qc.compute_quote_core(state, cfg, pred, qc.DepthSnapshot())
    monkeypatch.setenv("NARROWGATE_CPP_QUOTE_CORE", "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    cpp = qc.compute_quote_core(state, cfg, pred, qc.DepthSnapshot())

    assert py.bid_price == pytest.approx(cpp.bid_price, abs=cfg.tick_size * 0.51)
    assert py.ask_price == pytest.approx(cpp.ask_price, abs=cfg.tick_size * 0.51)
    assert py.quote_context["BUY"]["any_constraint_changed"] is True
    assert py.quote_context["SELL"]["any_constraint_changed"] is False
    assert cpp.quote_context["BUY"]["any_constraint_changed"] is True
    assert cpp.quote_context["SELL"]["any_constraint_changed"] is False


def test_cpp_direct_legacy_gamma_fallback_preserves_old_callers():
    cfg = _cfg(gamma=0.02)
    state = _state(1)
    pred = _pred(1)
    depth = qc.DepthSnapshot()
    expected = qc.compute_quote_core(state, cfg, pred, depth)

    cpp_cfg = narrowgate_cpp.QuoteCoreConfig()
    assert np.isnan(cpp_cfg.eta_inventory)
    assert np.isnan(cpp_cfg.a_spread)
    for name in qc._CPP_CFG_FIELDS:
        if name not in {"eta_inventory", "a_spread"}:
            setattr(cpp_cfg, name, getattr(cfg, name))
    cpp_state = qc._copy_attrs(state, narrowgate_cpp.QuoteState(), qc._CPP_STATE_FIELDS)
    cpp_pred = qc._copy_attrs(pred, narrowgate_cpp.QuotePrediction(), qc._CPP_PRED_FIELDS)
    actual = narrowgate_cpp.compute_quote_core(
        cpp_state,
        cpp_cfg,
        cpp_pred,
        qc._to_cpp_depth(narrowgate_cpp, depth),
    )

    assert actual.bid_price == pytest.approx(expected.bid_price, abs=cfg.tick_size * 0.51)
    assert actual.ask_price == pytest.approx(expected.ask_price, abs=cfg.tick_size * 0.51)
    cpp_cfg.a_spread = 0.0
    with pytest.raises(ValueError, match="a_spread"):
        narrowgate_cpp.compute_quote_core(
            cpp_state,
            cpp_cfg,
            cpp_pred,
            qc._to_cpp_depth(narrowgate_cpp, qc.DepthSnapshot()),
        )


def test_cpp_f03_ret_action_requires_explicit_consumer_compatibility():
    cfg = _cfg(
        ml_enabled=True,
        ret_skew=0.1,
        quote_horizon_s=10.0,
        f03_ret_action_horizon_s=10.0,
        f03_ret_action_compatible=True,
    )
    cpp_cfg = qc._copy_attrs(
        cfg,
        narrowgate_cpp.QuoteCoreConfig(),
        qc._CPP_CFG_FIELDS,
    )
    cpp_cfg.f03_ret_action_compatible = False
    with pytest.raises(ValueError, match="F03 ret action horizon"):
        narrowgate_cpp.compute_quote_core(
            qc._copy_attrs(
                _state(4),
                narrowgate_cpp.QuoteState(),
                qc._CPP_STATE_FIELDS,
            ),
            cpp_cfg,
            qc._copy_attrs(
                _pred(4),
                narrowgate_cpp.QuotePrediction(),
                qc._CPP_PRED_FIELDS,
            ),
            qc._to_cpp_depth(narrowgate_cpp, qc.DepthSnapshot()),
        )


def test_cpp_quote_core_horizon_and_absolute_price_risk_contract(monkeypatch):
    state = _state(
        mid=100.0,
        inventory=0.01,
        sigma_sq=4.0,
        position_open=True,
        unrealized_pnl=-0.1,
    )
    cfg = _cfg(
        quote_horizon_s=5.0,
        pnl_volatility_horizon_s=25.0,
        exit_urgency_strength=1.0,
        urgency_time_weight=0.0,
        urgency_pnl_weight=1.0,
        urgency_signal_weight=0.0,
        ml_enabled=False,
    )
    monkeypatch.delenv("NARROWGATE_CPP_QUOTE_CORE", raising=False)
    py = qc.compute_quote_core(state, cfg, qc.QuotePrediction())
    monkeypatch.setenv("NARROWGATE_CPP_QUOTE_CORE", "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    cpp = qc.compute_quote_core(state, cfg, qc.QuotePrediction())
    assert cpp.diagnostics["sigma_sq_horizon"] == pytest.approx(20.0)
    assert cpp.diagnostics["reservation_price"] == pytest.approx(
        py.diagnostics["reservation_price"]
    )
    assert cpp.diagnostics["delta_raw"] == pytest.approx(py.diagnostics["delta_raw"])
    assert cpp.diagnostics["asym"] == pytest.approx(-0.9)
    assert cpp.bid_price == pytest.approx(py.bid_price, abs=cfg.tick_size * 0.51)
    assert cpp.ask_price == pytest.approx(py.ask_price, abs=cfg.tick_size * 0.51)


def test_markout_asymmetry_three_arm_direction_and_cpp_parity(monkeypatch):
    state = _state(
        mid=100.0,
        best_bid=99.9,
        best_ask=100.1,
        inventory=0.0,
        mo_ema_bid=10.0,
        mo_ema_ask=-10.0,
    )
    pred = qc.QuotePrediction()
    configs = {
        "historical": _cfg(
            ml_enabled=False,
            dynamic_cap_enabled=False,
            max_spread_bps=0.0,
            markout_spread_scale=0.4,
            markout_side_asymmetry_sign=-1.0,
        ),
        "off": _cfg(
            ml_enabled=False,
            dynamic_cap_enabled=False,
            max_spread_bps=0.0,
            markout_spread_scale=0.0,
            markout_side_asymmetry_sign=-1.0,
        ),
        "corrected": _cfg(
            ml_enabled=False,
            dynamic_cap_enabled=False,
            max_spread_bps=0.0,
            markout_spread_scale=0.4,
            markout_side_asymmetry_sign=1.0,
        ),
    }
    results = {}
    for name, cfg in configs.items():
        monkeypatch.delenv("NARROWGATE_CPP_QUOTE_CORE", raising=False)
        py = qc.compute_quote_core(state, cfg, pred, qc.DepthSnapshot())
        monkeypatch.setenv("NARROWGATE_CPP_QUOTE_CORE", "1")
        monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
        cpp = qc.compute_quote_core(state, cfg, pred, qc.DepthSnapshot())
        assert cpp.bid_price == pytest.approx(py.bid_price, abs=cfg.tick_size * 0.51)
        assert cpp.ask_price == pytest.approx(py.ask_price, abs=cfg.tick_size * 0.51)
        results[name] = py

    historical_bid_dist = state.mid - results["historical"].bid_price
    off_bid_dist = state.mid - results["off"].bid_price
    corrected_bid_dist = state.mid - results["corrected"].bid_price
    assert historical_bid_dist > off_bid_dist > corrected_bid_dist


@pytest.mark.parametrize(
    ("mode", "capped", "blocked"),
    [
        (qc.SPREAD_CAP_COMPRESS, True, False),
        (qc.SPREAD_CAP_PAUSE_EXPOSURE, False, True),
        (qc.SPREAD_CAP_OBSERVE, False, False),
    ],
)
def test_spread_cap_action_three_arm_cpp_parity(monkeypatch, mode, capped, blocked):
    state = _state(
        mid=100.0,
        best_bid=99.9,
        best_ask=100.1,
        inventory=0.0,
        sigma_sq=25.0,
        mo_ema_bid=0.0,
        mo_ema_ask=0.0,
    )
    cfg = _cfg(
        ml_enabled=False,
        dynamic_cap_enabled=False,
        max_spread_bps=2.0,
        spread_cap_mode=mode,
        markout_spread_scale=0.0,
    )
    pred = qc.QuotePrediction()
    monkeypatch.delenv("NARROWGATE_CPP_QUOTE_CORE", raising=False)
    py = qc.compute_quote_core(state, cfg, pred, qc.DepthSnapshot())
    monkeypatch.setenv("NARROWGATE_CPP_QUOTE_CORE", "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    cpp = qc.compute_quote_core(state, cfg, pred, qc.DepthSnapshot())

    assert cpp.bid_price == pytest.approx(py.bid_price, abs=cfg.tick_size * 0.51)
    assert cpp.ask_price == pytest.approx(py.ask_price, abs=cfg.tick_size * 0.51)
    max_spread = state.mid * cfg.max_spread_bps / 10000.0
    if capped:
        assert cpp.spread <= max_spread + 2.0 * cfg.tick_size
    else:
        assert cpp.spread > max_spread
    assert cpp.quote_flags["cap_exposure_block"] is blocked
    assert cpp.quote_context["BUY"]["cap_exposure_block"] is blocked
    assert cpp.quote_context["SELL"]["cap_exposure_block"] is blocked


def test_tick_rounding_snaps_numerical_noise_at_boundary():
    tick = 0.1
    boundary = 79_196.7
    below_by_float_noise = boundary - 1e-11
    above_by_float_noise = boundary + 1e-11

    assert qc._floor_tick(below_by_float_noise, tick) == pytest.approx(boundary)
    assert qc._ceil_tick(above_by_float_noise, tick) == pytest.approx(boundary)


def test_cpp_live_quote_binding_matches_object_binding():
    cfg = _cfg(use_depth_microprice=True, use_depth_kappa=True)
    state = _state(1)
    pred = _pred(1)
    depth = _depth()
    cpp_cfg = qc._copy_attrs(cfg, narrowgate_cpp.QuoteCoreConfig(), qc._CPP_CFG_FIELDS)
    cpp_state = qc._copy_attrs(state, narrowgate_cpp.QuoteState(), qc._CPP_STATE_FIELDS)
    cpp_pred = qc._copy_attrs(pred, narrowgate_cpp.QuotePrediction(), qc._CPP_PRED_FIELDS)
    cpp_depth = qc._to_cpp_depth(narrowgate_cpp, depth)
    expected = narrowgate_cpp.compute_quote_core(cpp_state, cpp_cfg, cpp_pred, cpp_depth)
    actual = narrowgate_cpp.compute_quote_core_live(
        tuple(getattr(state, name) for name in qc._CPP_STATE_FIELDS),
        cpp_cfg,
        tuple(getattr(pred, name) for name in qc._CPP_PRED_FIELDS),
        depth.bids,
        depth.asks,
    )
    assert actual.bid_price == pytest.approx(expected.bid_price)
    assert actual.ask_price == pytest.approx(expected.ask_price)
    assert actual.book_imb == pytest.approx(expected.book_imb)
    assert actual.near_depth_total == pytest.approx(expected.near_depth_total)


def test_cpp_live_routing_compact_tuple_contract():
    input_values = (
        100.0, 0.0, 99.9, 100.1, 99.9, 100.1,
        0.1, 0.001, 0.001, 0.0, 0.001, 0.01,
        0.0, False, 1.0, 0.5,
        True, 99.9, 500.0, True, 100.1, 500.0,
    )
    bid_policy = (True, False, 1.0, 1.0, 1_000.0)
    ask_policy = (True, True, 1.0, 1.0, 1_000.0)

    result = narrowgate_cpp.compute_live_routing_decision(
        input_values, bid_policy, ask_policy
    )

    assert isinstance(result, tuple)
    assert len(result) == 11
    assert result[0] == pytest.approx(99.9)
    assert result[1] == pytest.approx(100.1)
    assert result[2] is False
    assert result[3:7] == (True, True, False, True)
    assert result[7:9] == (False, False)
    assert result[9] == pytest.approx(0.001)
    assert result[10] == pytest.approx(0.001)

    expired = list(input_values)
    expired[18] = 1_000.0
    expired_result = narrowgate_cpp.compute_live_routing_decision(
        tuple(expired), bid_policy, ask_policy
    )
    assert expired_result[7] is True


def test_cpp_live_routing_does_not_enlarge_invalid_base_order():
    input_values = (
        100.0, 0.0, 99.9, 100.1, 99.9, 100.1,
        0.1, 0.001, 0.001, 10.0, 0.001, 0.01,
        0.0, False, 1.0, 0.5,
        False, 0.0, 0.0, False, 0.0, 0.0,
    )
    policy = (True, True, 1.0, 1.0, 1_000.0)

    result = narrowgate_cpp.compute_live_routing_decision(
        input_values, policy, policy
    )

    # 0.001 BTC at 100 USDC is below the 10 USDC notional minimum.  Python
    # leaves this invalid so the final exchange filter skips it; C++ must not
    # silently turn it into a 0.101 BTC order.
    assert result[9] == pytest.approx(0.001)
    assert result[10] == pytest.approx(0.001)


def test_cpp_live_routing_rejects_wrong_compact_shape():
    with pytest.raises(ValueError, match="compact input length mismatch"):
        narrowgate_cpp.compute_live_routing_decision((1.0,), (True,) * 5, (True,) * 5)


def test_cpp_live_compact_context_preserves_policy_fields(monkeypatch):
    state = _state(1, inventory=0.006, mo_ema_bid=-6.0, mo_ema_ask=-4.0)
    cfg = _cfg(
        use_depth_microprice=True,
        use_depth_kappa=True,
        adverse_guard_enabled=True,
        adverse_markout_threshold=5.0,
        adverse_pause=True,
        defense_guard_enabled=True,
        defense_markout_threshold=2.0,
    )
    pred = _pred(1)
    monkeypatch.setenv("NARROWGATE_CPP_QUOTE_CORE", "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    full = qc.compute_quote_core(state, cfg, pred, _depth())
    compact = qc.compute_quote_core_live(
        state, cfg, pred, _depth(), require_full_context=False
    )
    assert compact.bid_price == pytest.approx(full.bid_price)
    assert compact.ask_price == pytest.approx(full.ask_price)
    for side in ("BUY", "SELL"):
        for key in (
            "side_adverse", "side_adverse_pause", "defense_guard",
            "defense_pause", "defense_spread_mult", "near_depth_total",
            "final_quote_delta_to_bbo",
        ):
            assert compact.quote_context[side][key] == pytest.approx(
                full.quote_context[side][key]
            )
    for key in ("max_spread", "kappa_used", "asym", "delta_after_cap"):
        assert compact.diagnostics[key] == pytest.approx(full.diagnostics[key])

    requested_full = qc.compute_quote_core_live(
        state, cfg, pred, _depth(), require_full_context=True
    )
    assert "raw_asym_shift" in requested_full.quote_context["BUY"]
    assert requested_full.quote_context["BUY"]["raw_asym_shift"] == pytest.approx(
        full.quote_context["BUY"]["raw_asym_shift"]
    )


def test_deferred_live_quote_pod_reads_do_not_materialize(monkeypatch):
    state = _state(1, inventory=0.006, mo_ema_bid=-6.0, mo_ema_ask=-4.0)
    cfg = _cfg(
        use_depth_microprice=True,
        use_depth_kappa=True,
        adverse_guard_enabled=True,
        adverse_markout_threshold=5.0,
        adverse_pause=True,
        defense_guard_enabled=True,
        defense_markout_threshold=2.0,
    )
    pred = _pred(1)
    depth = _depth()
    monkeypatch.setenv("NARROWGATE_CPP_QUOTE_CORE", "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")

    deferred = qc.compute_quote_core_live_deferred(state, cfg, pred, depth)
    eager = qc.compute_quote_core_live(state, cfg, pred, depth)

    assert isinstance(deferred, qc.DeferredNativeQuoteCoreResult)
    assert not deferred.is_materialized
    _assert_recursive_exact(deferred.bid_price, eager.bid_price)
    _assert_recursive_exact(deferred.ask_price, eager.ask_price)
    _assert_recursive_exact(deferred.spread, eager.spread)
    for side in ("BUY", "SELL"):
        for key in (
            "order_ttl_ms",
            "side_adverse",
            "bid_adverse",
            "ask_adverse",
            "side_adverse_pause",
            "local_extreme_guard",
            "local_extreme_spread_mult",
            "local_extreme_pause",
            "defense_guard",
            "defense_spread_mult",
            "defense_pause",
            "cap_exposure_block",
        ):
            _assert_recursive_exact(
                deferred.side_value(side, key), eager.quote_context[side][key]
            )
    for key in (
        "max_spread",
        "kappa_before_depth",
        "kappa_used",
        "asym",
        "p3_side_bbo_floor_enabled",
        "p3_touch_delta_star",
    ):
        _assert_recursive_exact(
            deferred.diagnostic_value(key), eager.diagnostics[key]
        )
    assert not deferred.is_materialized

    materialized = deferred.materialize()
    _assert_recursive_exact(materialized, eager)
    assert deferred.materialize() is materialized


def test_deferred_live_quote_preserves_eager_public_and_full_context_apis(
    monkeypatch,
):
    state = _state(2)
    cfg = _cfg(use_depth_microprice=True, use_depth_kappa=True)
    pred = _pred(2)
    monkeypatch.setenv("NARROWGATE_CPP_QUOTE_CORE", "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")

    public_result = qc.compute_quote_core_live(state, cfg, pred, _depth())
    full_result = qc.compute_quote_core_live_deferred(
        state,
        cfg,
        pred,
        _depth(),
        require_full_context=True,
    )

    assert type(public_result) is qc.QuoteCoreResult
    assert type(full_result) is qc.QuoteCoreResult
    assert "raw_asym_shift" in full_result.quote_context["BUY"]

    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "0")
    non_strict_result = qc.compute_quote_core_live_deferred(
        state, cfg, pred, _depth()
    )
    assert type(non_strict_result) is qc.QuoteCoreResult


def test_deferred_live_quote_rejects_incomplete_native_abi_before_return(
    monkeypatch,
):
    class IncompleteNativeResult:
        pass

    incomplete = IncompleteNativeResult()
    monkeypatch.setenv("NARROWGATE_CPP_QUOTE_CORE", "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    monkeypatch.setattr(
        qc,
        "_call_cpp_quote_core",
        lambda *_args, **_kwargs: (incomplete, (0.5, 0.0, 0.0, 0.5, 0.5)),
    )

    with pytest.raises(RuntimeError, match="deferred quote result ABI is incomplete"):
        qc.compute_quote_core_live_deferred(_state(3), _cfg(), _pred(3), _depth())


def test_cpp_quote_core_diagnostics_and_defense_context_parity(monkeypatch):
    state = _state(
        1,
        mid=100.0,
        best_bid=99.9,
        best_ask=100.1,
        inventory=0.006,
        mo_ema_bid=-3.0,
        mo_ema_ask=-4.0,
        unrealized_pnl=-2.0,
    )
    cfg = _cfg(
        use_depth_microprice=True,
        use_depth_kappa=True,
        book_imb_strength=0.3,
        trace_book_imb_levels=3,
        depth_tox_enabled=True,
        depth_tox_levels=3,
        depth_tox_imbalance_threshold=0.01,
        depth_tox_spread_mult=1.5,
        dynamic_cap_liq_beta=0.3,
        dynamic_cap_liq_baseline=10.0,
        adverse_guard_enabled=True,
        adverse_markout_threshold=2.0,
        adverse_pause=False,
        defense_guard_enabled=True,
        defense_markout_threshold=2.0,
        defense_dir_threshold=0.01,
        defense_ret_bps_threshold=0.01,
        defense_microprice_shift_bps=0.01,
        defense_pause=True,
        defense_spread_mult=1.4,
        defense_emergency_inventory_ratio=0.9,
        defense_emergency_loss=20.0,
    )
    pred = qc.QuotePrediction(
        dir_10s=0.56,
        vol_10s=1.8,
        ret_10s=2e-5,
        tox_bid=0.8,
        tox_ask=0.4,
    )

    monkeypatch.delenv("NARROWGATE_CPP_QUOTE_CORE", raising=False)
    py = qc.compute_quote_core(state, cfg, pred, _depth())
    monkeypatch.setenv("NARROWGATE_CPP_QUOTE_CORE", "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    cpp = qc.compute_quote_core(state, cfg, pred, _depth())

    common_fields = [
        "raw_half_spread",
        "capped_half_spread",
        "raw_mid_shift",
        "raw_reservation_shift",
        "raw_asym_shift",
        "asym",
        "book_imb",
        "microprice_shift_bps",
        "near_depth_total",
        "raw_quote_skew",
    ]
    for field in common_fields:
        assert cpp.quote_context["BUY"][field] == pytest.approx(py.quote_context["BUY"][field])
        assert cpp.quote_context["SELL"][field] == pytest.approx(py.quote_context["SELL"][field])

    diagnostic_fields = [
        "reservation_price",
        "sigma_sq_raw",
        "sigma_sq_blended",
        "delta_raw",
        "delta_after_regime",
        "delta_pre_cap",
        "delta_after_cap",
        "half_d",
        "asym",
        "kappa_before_depth",
        "kappa_used",
        "depth_tox_mult",
        "final_cap_excess",
        "mid_guard_bid",
        "mid_guard_ask",
        "post_only_bid",
        "post_only_ask",
        "final_cap_rounding",
        "final_cap_mid_guard",
        "final_cap_post_only",
        "final_cap_delta",
    ]
    for field in diagnostic_fields:
        if isinstance(py.diagnostics[field], bool):
            assert cpp.diagnostics[field] is py.diagnostics[field]
        else:
            assert cpp.diagnostics[field] == pytest.approx(py.diagnostics[field])

    defense_fields = [
        "defense_guard",
        "defense_pause",
        "defense_reducing",
        "defense_emergency",
        "defense_markout",
        "defense_direction",
        "defense_ret",
        "defense_microprice",
        "defense_spread_mult",
    ]
    for side in ("BUY", "SELL"):
        for field in defense_fields:
            if isinstance(py.quote_context[side][field], bool):
                assert cpp.quote_context[side][field] is py.quote_context[side][field]
            else:
                assert cpp.quote_context[side][field] == pytest.approx(
                    py.quote_context[side][field]
                )


def test_cpp_quote_core_uses_shared_loader_and_validates_capabilities_once(monkeypatch):
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    monkeypatch.setattr(qc, "_CPP_QUOTE_CORE", None)
    monkeypatch.setattr(qc, "_CPP_QUOTE_CORE_IMPORT_FAILED", False)
    calls = []
    original_validator = qc.validate_native_capabilities

    def load(*, optional):
        calls.append(("load", optional))
        return narrowgate_cpp

    def validate(module, **kwargs):
        calls.append(("validate", module))
        return original_validator(module, **kwargs)

    monkeypatch.setattr(qc, "load_native_module", load)
    monkeypatch.setattr(qc, "validate_native_capabilities", validate)
    assert qc._load_cpp_quote_core() is narrowgate_cpp
    assert qc._load_cpp_quote_core() is narrowgate_cpp
    assert calls == [("load", False), ("validate", narrowgate_cpp)]


def test_cpp_quote_config_cache_is_bound_to_module_and_config(monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setattr(qc, "_CPP_CFG_CACHE_KEY", None)
    monkeypatch.setattr(qc, "_CPP_CFG_CACHE_REF", None)
    monkeypatch.setattr(qc, "_CPP_CFG_CACHE_VALUE", None)
    cfg = _cfg()
    first_module = SimpleNamespace(QuoteCoreConfig=narrowgate_cpp.QuoteCoreConfig)
    second_module = SimpleNamespace(QuoteCoreConfig=narrowgate_cpp.QuoteCoreConfig)
    first_config = qc._cached_cpp_config(first_module, cfg)
    assert qc._cached_cpp_config(first_module, cfg) is first_config
    second_config = qc._cached_cpp_config(second_module, cfg)
    assert second_config is not first_config
    assert qc._cached_cpp_config(second_module, cfg) is second_config
    assert second_config.gamma == first_config.gamma


def test_direct_cpp_p3_projection_requires_complete_touch_identity() -> None:
    cpp_cfg = narrowgate_cpp.QuoteCoreConfig()
    cpp_cfg.p3_delta_star = 0.5
    cpp_cfg.p3_side_bbo_floor_enabled = True
    cpp_state = qc._copy_attrs(
        _state(1), narrowgate_cpp.QuoteState(), qc._CPP_STATE_FIELDS
    )
    cpp_pred = qc._copy_attrs(
        _pred(1), narrowgate_cpp.QuotePrediction(), qc._CPP_PRED_FIELDS
    )
    cpp_depth = qc._to_cpp_depth(narrowgate_cpp, qc.DepthSnapshot())

    with pytest.raises(ValueError, match="complete touch identity"):
        narrowgate_cpp.compute_quote_core(
            cpp_state,
            cpp_cfg,
            cpp_pred,
            cpp_depth,
        )

    cpp_cfg.p3_identity_required = True
    cpp_cfg.p3_event_type = "touch"
    cpp_cfg.p3_horizon_s = 10.0
    cpp_cfg.p3_distance_origin = "same_side_best_bid_or_ask_at_window_start"
    cpp_cfg.p3_distance_unit = "USDC_per_BTC"
    cpp_cfg.p3_side = "pooled_buy_sell"
    cpp_cfg.p3_queue_included = False
    cpp_cfg.p3_artifact_sha256 = "d" * 64
    result = narrowgate_cpp.compute_quote_core(
        cpp_state,
        cpp_cfg,
        cpp_pred,
        cpp_depth,
    )
    assert result.bid_price > 0.0
    assert result.ask_price > result.bid_price


def test_scalar_cpp_route_rejects_stale_quote_config_abi() -> None:
    class StaleCppModule:
        class QuoteCoreConfig:
            gamma = 0.01

    qc._CPP_CFG_CACHE_KEY = None
    qc._CPP_CFG_CACHE_REF = None
    qc._CPP_CFG_CACHE_VALUE = None
    with pytest.raises(RuntimeError, match="p3_identity_required"):
        qc._cached_cpp_config(StaleCppModule(), _cfg())


def test_cpp_quote_core_batch_parity(monkeypatch):
    cfg = _cfg()
    cpp_cfg = narrowgate_cpp.QuoteCoreConfig()
    for name in qc._CPP_CFG_FIELDS:
        if hasattr(cpp_cfg, name):
            setattr(cpp_cfg, name, getattr(cfg, name))

    n = 256
    mid = np.linspace(100.0, 101.0, n, dtype=np.float64)
    inventory = np.linspace(-0.004, 0.004, n, dtype=np.float64)
    sigma_sq = np.linspace(0.5, 2.0, n, dtype=np.float64)
    trade_intensity = np.full(n, 100.0, dtype=np.float64)
    best_bid = mid - 0.1
    best_ask = mid + 0.1
    dir_10s = np.linspace(0.45, 0.55, n, dtype=np.float64)
    vol_10s = np.full(n, 1.0, dtype=np.float64)
    ret_10s = np.linspace(-2e-5, 2e-5, n, dtype=np.float64)
    tox_bid = np.full(n, 0.5, dtype=np.float64)
    tox_ask = np.full(n, 0.5, dtype=np.float64)

    out = narrowgate_cpp.compute_quote_core_batch(
        mid,
        inventory,
        sigma_sq,
        trade_intensity,
        best_bid,
        best_ask,
        dir_10s,
        vol_10s,
        ret_10s,
        tox_bid,
        tox_ask,
        cpp_cfg,
    )

    monkeypatch.delenv("NARROWGATE_CPP_QUOTE_CORE", raising=False)
    for i in range(0, n, 17):
        py = qc.compute_quote_core(
            qc.QuoteState(
                mid=float(mid[i]),
                inventory=float(inventory[i]),
                sigma_sq=float(sigma_sq[i]),
                trade_intensity=float(trade_intensity[i]),
                best_bid=float(best_bid[i]),
                best_ask=float(best_ask[i]),
            ),
            cfg,
            qc.QuotePrediction(
                dir_10s=float(dir_10s[i]),
                vol_10s=float(vol_10s[i]),
                ret_10s=float(ret_10s[i]),
                tox_bid=float(tox_bid[i]),
                tox_ask=float(tox_ask[i]),
            ),
        )
        assert out["bid_price"][i] == pytest.approx(py.bid_price, abs=cfg.tick_size * 0.51)
        assert out["ask_price"][i] == pytest.approx(py.ask_price, abs=cfg.tick_size * 0.51)


def test_cpp_quote_core_batch_depth_parity(monkeypatch):
    cfg = _cfg(
        use_depth_microprice=True,
        use_depth_kappa=True,
        book_imb_strength=0.05,
        trace_book_imb_levels=3,
        depth_tox_enabled=True,
        depth_tox_levels=3,
        depth_tox_imbalance_threshold=0.55,
        depth_tox_microprice_shift_bps=0.01,
    )
    n = 4_097
    mid = np.linspace(100.0, 100.6, n, dtype=np.float64)
    inventory = np.linspace(-0.004, 0.004, n, dtype=np.float64)
    sigma_sq = np.linspace(0.5, 2.0, n, dtype=np.float64)
    trade_intensity = np.full(n, 100.0, dtype=np.float64)
    best_bid = mid - 0.1
    best_ask = mid + 0.1
    dir_10s = np.linspace(0.45, 0.55, n, dtype=np.float64)
    vol_10s = np.full(n, 1.0, dtype=np.float64)
    ret_10s = np.linspace(-2e-5, 2e-5, n, dtype=np.float64)
    tox_bid = np.full(n, 0.55, dtype=np.float64)
    tox_ask = np.full(n, 0.52, dtype=np.float64)

    bid_offsets = np.array([0.1, 0.2, 0.3], dtype=np.float64)
    ask_offsets = np.array([0.1, 0.2, 0.3], dtype=np.float64)
    l2_bid_px = mid[:, None] - bid_offsets[None, :]
    l2_ask_px = mid[:, None] + ask_offsets[None, :]
    l2_bid_qty = np.column_stack([
        np.full(n, 2.0),
        np.linspace(2.0, 4.0, n),
        np.full(n, 5.0),
    ])
    l2_ask_qty = np.column_stack([
        np.full(n, 2.5),
        np.linspace(4.0, 2.0, n),
        np.full(n, 4.0),
    ])

    out = qc.compute_quote_core_batch_depth_cpp(
        mid=mid,
        inventory=inventory,
        sigma_sq=sigma_sq,
        trade_intensity=trade_intensity,
        best_bid=best_bid,
        best_ask=best_ask,
        dir_10s=dir_10s,
        vol_10s=vol_10s,
        ret_10s=ret_10s,
        tox_bid=tox_bid,
        tox_ask=tox_ask,
        cfg=cfg,
        mo_ema_bid=np.full(n, -1.0),
        mo_ema_ask=np.full(n, -0.5),
        l2_bid_px=l2_bid_px,
        l2_bid_qty=l2_bid_qty,
        l2_ask_px=l2_ask_px,
        l2_ask_qty=l2_ask_qty,
        strict=True,
    )
    out_parallel = qc.compute_quote_core_batch_depth_cpp(
        mid=mid,
        inventory=inventory,
        sigma_sq=sigma_sq,
        trade_intensity=trade_intensity,
        best_bid=best_bid,
        best_ask=best_ask,
        dir_10s=dir_10s,
        vol_10s=vol_10s,
        ret_10s=ret_10s,
        tox_bid=tox_bid,
        tox_ask=tox_ask,
        cfg=cfg,
        mo_ema_bid=np.full(n, -1.0),
        mo_ema_ask=np.full(n, -0.5),
        l2_bid_px=l2_bid_px,
        l2_bid_qty=l2_bid_qty,
        l2_ask_px=l2_ask_px,
        l2_ask_qty=l2_ask_qty,
        strict=True,
        workers=4,
    )
    for key in ("bid_price", "ask_price", "near_depth_total", "book_imb"):
        assert out_parallel[key] == pytest.approx(out[key], abs=1e-12), key

    monkeypatch.delenv("NARROWGATE_CPP_QUOTE_CORE", raising=False)
    for i in range(0, n, max(11, n // 6)):
        depth = qc.quote_depth_from_l2_rows(
            l2_bid_px[i],
            l2_bid_qty[i],
            l2_ask_px[i],
            l2_ask_qty[i],
        )
        py = qc.compute_quote_core(
            qc.QuoteState(
                mid=float(mid[i]),
                inventory=float(inventory[i]),
                sigma_sq=float(sigma_sq[i]),
                trade_intensity=float(trade_intensity[i]),
                best_bid=float(best_bid[i]),
                best_ask=float(best_ask[i]),
                mo_ema_bid=-1.0,
                mo_ema_ask=-0.5,
            ),
            cfg,
            qc.QuotePrediction(
                dir_10s=float(dir_10s[i]),
                vol_10s=float(vol_10s[i]),
                ret_10s=float(ret_10s[i]),
                tox_bid=float(tox_bid[i]),
                tox_ask=float(tox_ask[i]),
            ),
            depth,
        )
        assert out["bid_price"][i] == pytest.approx(py.bid_price, abs=cfg.tick_size * 0.51)
        assert out["ask_price"][i] == pytest.approx(py.ask_price, abs=cfg.tick_size * 0.51)
        assert out["near_depth_total"][i] == pytest.approx(
            py.quote_context["BUY"]["near_depth_total"], rel=1e-12
        )
        assert out["book_imb"][i] == pytest.approx(py.quote_context["BUY"]["book_imb"], abs=1e-12)
        assert "bid_defense_guard" in out


def _native_depth20_update(*, side, prices, quantities, timestamp_ns, generation=1):
    update = narrowgate_cpp.Depth20SideUpdate()
    tick_size = 0.1
    lot_size = 0.001
    price_ticks = [0] * 20
    quantity_lots = [0] * 20
    for index, (price, quantity) in enumerate(zip(prices, quantities, strict=True)):
        price_ticks[index] = round(price / tick_size)
        quantity_lots[index] = round(quantity / lot_size)
    update.price_ticks = price_ticks
    update.quantity_lots = quantity_lots
    update.size = len(prices)
    update.clock.source_ts_ns = timestamp_ns - 3
    update.clock.exchange_ts_ns = timestamp_ns - 2
    update.clock.receive_ts_ns = timestamp_ns - 1
    update.clock.visible_ts_ns = timestamp_ns
    update.clock.generation = generation
    return update


def _cpp_depth_snapshot(prices_bid, quantities_bid, prices_ask, quantities_ask):
    depth = narrowgate_cpp.DepthSnapshot()

    def _levels(prices, quantities):
        result = []
        for price, quantity in zip(prices, quantities, strict=True):
            level = narrowgate_cpp.DepthLevel()
            level.price = price
            level.qty = quantity
            result.append(level)
        return result

    depth.bids = _levels(prices_bid, quantities_bid)
    depth.asks = _levels(prices_ask, quantities_ask)
    return depth


def _native_value_bits(value, path=""):
    """Canonicalize every bound result field without numeric tolerance."""
    if isinstance(value, bool):
        return ((path, "bool", value),)
    if isinstance(value, int):
        return ((path, "int", value),)
    if isinstance(value, float):
        return ((path, "double", struct.pack("!d", value)),)

    rows = []
    for name in sorted(item for item in dir(value) if not item.startswith("_")):
        item = getattr(value, name)
        if callable(item):
            continue
        rows.extend(_native_value_bits(item, f"{path}.{name}" if path else name))
    return tuple(rows)


def _native_runtime_with_valid_input():
    cpp_cfg = qc._copy_attrs(
        _cfg(ml_enabled=False, use_bar_pricing=False),
        narrowgate_cpp.QuoteCoreConfig(),
        qc._CPP_CFG_FIELDS,
    )
    runtime = narrowgate_cpp.NativeLiveRuntimeCore(cpp_cfg)
    bids = _native_depth20_update(
        side="BUY",
        prices=(99.9,),
        quantities=(1.0,),
        timestamp_ns=1_000_000_000,
    )
    asks = _native_depth20_update(
        side="SELL",
        prices=(100.1,),
        quantities=(1.0,),
        timestamp_ns=1_000_000_000,
    )
    assert runtime.publish_book(bids, asks) == (
        narrowgate_cpp.MarketStateUpdateStatus.Applied
    )
    input_value = narrowgate_cpp.NativeLiveDecisionInput()
    input_value.quote_state.mid = 100.0
    input_value.quote_state.sigma_sq = 1.0
    input_value.quote_state.trade_intensity = 100.0
    input_value.min_qty = 0.001
    input_value.min_notional = 5.0
    input_value.decision_ts_ns = 1_200_000_000
    input_value.max_book_age_ns = 1_000_000_000
    input_value.expected_market_publication_sequence = 2
    input_value.expected_bid_generation = 1
    input_value.expected_ask_generation = 1
    return runtime, input_value, bids, asks


@pytest.mark.parametrize("bad_value", [float("nan"), float("inf"), -float("inf")])
@pytest.mark.parametrize(
    ("owner", "field"),
    [
        ("quote_state", "mid"),
        ("quote_state", "inventory"),
        ("quote_state", "sigma_sq"),
        ("quote_state", "trade_intensity"),
        ("quote_state", "best_bid"),
        ("quote_state", "best_ask"),
        ("quote_state", "mo_ema_all"),
        ("quote_state", "mo_ema_bid"),
        ("quote_state", "mo_ema_ask"),
        ("quote_state", "mo_ref"),
        ("quote_state", "hold_time_s"),
        ("quote_state", "unrealized_pnl"),
        ("prediction", "dir_10s"),
        ("prediction", "vol_10s"),
        ("prediction", "ret_10s"),
        ("prediction", "tox_bid"),
        ("prediction", "tox_ask"),
        ("buy_policy", "inventory_ratio"),
        ("buy_policy", "depth_age_s"),
        ("buy_policy", "max_book_age_s"),
        ("buy_policy", "toxicity"),
        ("buy_policy", "markout_ema"),
        ("buy_policy", "markout_spread_scale"),
        ("buy_policy", "markout_reference"),
        ("buy_policy", "microprice_shift_bps"),
        ("buy_policy", "l2_quote_flip_rate"),
        ("buy_policy", "l2_book_cancel_ratio"),
        ("buy_policy", "l2_near_depth_total"),
        ("buy_policy", "thin_depth_threshold"),
        ("buy_policy", "kappa_depth_baseline"),
        ("buy_policy", "local_extreme_spread_mult"),
        ("buy_policy", "defense_spread_mult"),
        ("sell_policy", "microprice_shift_bps"),
        ("input", "min_qty"),
        ("input", "min_notional"),
        ("input", "size_eta"),
        ("input", "requote_threshold_bps"),
        ("input", "routing_max_spread"),
        ("input", "bid_active_price"),
        ("input", "bid_age_ms"),
        ("input", "ask_active_price"),
        ("input", "ask_age_ms"),
        ("input", "bid_order_ttl_ms"),
        ("input", "ask_order_ttl_ms"),
    ],
)
def test_fused_native_live_runtime_rejects_nonfinite_dynamic_abi(
    owner,
    field,
    bad_value,
):
    runtime, input_value, _bids, _asks = _native_runtime_with_valid_input()
    target = input_value if owner == "input" else getattr(input_value, owner)
    setattr(target, field, bad_value)
    assert runtime.decide(input_value).status == (
        narrowgate_cpp.NativeLiveDecisionStatus.InvalidInput
    )
    assert runtime.decision_count == 0


@pytest.mark.parametrize(
    ("owner", "field", "bad_value"),
    [
        ("quote_state", "sigma_sq", -1.0),
        ("quote_state", "trade_intensity", -1.0),
        ("quote_state", "mo_ref", 0.0),
        ("quote_state", "hold_time_s", -1.0),
        ("prediction", "dir_10s", 1.01),
        ("prediction", "vol_10s", -1.0),
        ("prediction", "tox_bid", -0.01),
        ("prediction", "tox_ask", 1.01),
        ("buy_policy", "l2_quote_flip_rate", 1.01),
        ("buy_policy", "l2_book_cancel_ratio", -0.01),
        ("buy_policy", "local_extreme_spread_mult", 0.0),
        ("input", "size_eta", -0.01),
        ("input", "requote_threshold_bps", -0.01),
    ],
)
def test_fused_native_live_runtime_rejects_out_of_range_dynamic_abi(
    owner,
    field,
    bad_value,
):
    runtime, input_value, _bids, _asks = _native_runtime_with_valid_input()
    target = input_value if owner == "input" else getattr(input_value, owner)
    setattr(target, field, bad_value)
    assert runtime.decide(input_value).status == (
        narrowgate_cpp.NativeLiveDecisionStatus.InvalidInput
    )


def test_fused_native_live_runtime_rejects_nonfinite_computed_output():
    runtime, input_value, _bids, _asks = _native_runtime_with_valid_input()
    # Every ABI field is individually finite, but this combination overflows
    # the reservation-risk arithmetic. It must not cross the Applied boundary.
    input_value.quote_state.inventory = sys.float_info.max
    input_value.quote_state.sigma_sq = sys.float_info.max
    assert runtime.decide(input_value).status == (
        narrowgate_cpp.NativeLiveDecisionStatus.InvalidOutput
    )
    assert runtime.decision_count == 0


def test_fused_native_live_runtime_rejects_invalid_active_order_price():
    runtime, input_value, _bids, _asks = _native_runtime_with_valid_input()
    input_value.bid_active = True
    input_value.bid_active_price = 99.95
    assert runtime.decide(input_value).status == (
        narrowgate_cpp.NativeLiveDecisionStatus.InvalidInput
    )


def test_fused_native_live_runtime_latches_rejected_feed_until_full_resync():
    runtime, input_value, _bids, _asks = _native_runtime_with_valid_input()
    assert not runtime.feed_fault_latched

    crossed_asks = _native_depth20_update(
        side="SELL",
        prices=(99.8,),
        quantities=(1.0,),
        timestamp_ns=1_100_000_000,
        generation=2,
    )
    next_bids = _native_depth20_update(
        side="BUY",
        prices=(99.9,),
        quantities=(1.0,),
        timestamp_ns=1_100_000_000,
        generation=2,
    )
    assert runtime.publish_book(next_bids, crossed_asks) == (
        narrowgate_cpp.MarketStateUpdateStatus.CrossedBook
    )
    assert runtime.feed_fault_latched
    assert runtime.feed_fault_epoch == 1
    assert runtime.feed_resync_epoch == 0
    assert runtime.decide(input_value).status == (
        narrowgate_cpp.NativeLiveDecisionStatus.FeedFault
    )

    # An accepted empty/incomplete state is not a recovery publication.
    empty_bids = _native_depth20_update(
        side="BUY",
        prices=(),
        quantities=(),
        timestamp_ns=1_100_000_000,
        generation=2,
    )
    empty_asks = _native_depth20_update(
        side="SELL",
        prices=(),
        quantities=(),
        timestamp_ns=1_100_000_000,
        generation=2,
    )
    assert runtime.publish_book(empty_bids, empty_asks) == (
        narrowgate_cpp.MarketStateUpdateStatus.Applied
    )
    assert runtime.feed_fault_latched

    recovered_bids = _native_depth20_update(
        side="BUY",
        prices=(99.9,),
        quantities=(1.0,),
        timestamp_ns=1_200_000_000,
        generation=3,
    )
    recovered_asks = _native_depth20_update(
        side="SELL",
        prices=(100.1,),
        quantities=(1.0,),
        timestamp_ns=1_200_000_000,
        generation=3,
    )
    assert runtime.publish_book(recovered_bids, recovered_asks) == (
        narrowgate_cpp.MarketStateUpdateStatus.Applied
    )
    assert not runtime.feed_fault_latched
    assert runtime.feed_resync_epoch == runtime.feed_fault_epoch == 1

    input_value.decision_ts_ns = 1_300_000_000
    input_value.expected_market_publication_sequence = 6
    input_value.expected_bid_generation = 3
    input_value.expected_ask_generation = 3
    assert runtime.decide(input_value).status == (
        narrowgate_cpp.NativeLiveDecisionStatus.Applied
    )


def test_fused_native_live_runtime_matches_separate_quote_policy_and_routing(
    monkeypatch,
):
    cfg = _cfg(
        ml_enabled=False,
        use_bar_pricing=False,
        use_depth_microprice=True,
        use_depth_kappa=True,
        dynamic_cap_enabled=False,
        max_spread_bps=20.0,
        markout_spread_scale=0.2,
        adverse_guard_enabled=True,
        adverse_markout_threshold=5.0,
        adverse_pause=False,
    )
    cpp_cfg = qc._copy_attrs(
        cfg,
        narrowgate_cpp.QuoteCoreConfig(),
        qc._CPP_CFG_FIELDS,
    )
    runtime = narrowgate_cpp.NativeLiveRuntimeCore(cpp_cfg)
    bids = _native_depth20_update(
        side="BUY",
        prices=(99.9, 99.8, 99.7),
        quantities=(2.0, 3.0, 4.0),
        timestamp_ns=1_000_000_000,
    )
    asks = _native_depth20_update(
        side="SELL",
        prices=(100.1, 100.2, 100.3),
        quantities=(2.5, 2.0, 5.0),
        timestamp_ns=1_000_000_000,
    )
    assert runtime.publish_book(bids, asks) == (
        narrowgate_cpp.MarketStateUpdateStatus.Applied
    )

    state = _state(
        0,
        mid=100.0,
        inventory=0.001,
        best_bid=99.9,
        best_ask=100.1,
        mo_ema_bid=-6.0,
        mo_ema_ask=-0.5,
    )
    prediction = qc.QuotePrediction()
    input_value = narrowgate_cpp.NativeLiveDecisionInput()
    input_value.quote_state = qc._copy_attrs(
        state,
        narrowgate_cpp.QuoteState(),
        qc._CPP_STATE_FIELDS,
    )
    input_value.prediction = qc._copy_attrs(
        prediction,
        narrowgate_cpp.QuotePrediction(),
        qc._CPP_PRED_FIELDS,
    )
    input_value.decision_ts_ns = 1_100_000_000
    input_value.max_book_age_ns = 1_000_000_000
    input_value.expected_market_publication_sequence = 2
    input_value.expected_bid_generation = 1
    input_value.expected_ask_generation = 1
    input_value.min_qty = 0.001
    input_value.min_notional = 5.0
    input_value.requote_threshold_bps = 0.1
    input_value.buy_policy.exposure_increasing = True
    input_value.sell_policy.exposure_increasing = False
    input_value.buy_policy.max_book_age_s = 1.0
    input_value.sell_policy.max_book_age_s = 1.0

    monkeypatch.delenv("NARROWGATE_CPP_QUOTE_CORE", raising=False)
    expected_quote = qc.compute_quote_core(state, cfg, prediction, _depth())
    result = runtime.decide(input_value)
    assert result.status == narrowgate_cpp.NativeLiveDecisionStatus.Applied
    assert result.book_age_ns == 100_000_000
    assert result.decision_sequence == 1
    assert result.quote.bid_price == pytest.approx(expected_quote.bid_price)
    assert result.quote.ask_price == pytest.approx(expected_quote.ask_price)

    for side, policy, exposure in (
        ("BUY", result.buy_policy, True),
        ("SELL", result.sell_policy, False),
    ):
        context = expected_quote.quote_context[side]
        expected_policy = evaluate_common_side_policy(
            CommonSidePolicyInput(
                exposure_increasing=exposure,
                inventory_ratio=0.05,
                depth_age_s=0.1,
                max_book_age_s=1.0,
                toxicity=0.5,
                markout_ema=-6.0 if side == "BUY" else -0.5,
                markout_spread_scale=0.2,
                markout_reference=50.0,
                microprice_shift_bps=context["microprice_shift_bps"],
                side_adverse=context["side_adverse"],
                side_adverse_pause=context["side_adverse_pause"],
                defense_guard=context["defense_guard"],
                defense_spread_mult=context["defense_spread_mult"],
                defense_pause=context["defense_pause"],
            )
        )
        assert policy.allow_post is expected_policy.allow_post
        assert (
            policy.allow_exposure_increase
            is expected_policy.allow_exposure_increase
        )
        assert policy.spread_mult == pytest.approx(expected_policy.spread_mult)
        assert policy.size_mult == pytest.approx(expected_policy.size_mult)
        assert policy.reason_mask == expected_policy.reason_mask

    expected_route = narrowgate_cpp.compute_live_routing_decision(
        (
            state.mid,
            state.inventory,
            result.quote.bid_price,
            result.quote.ask_price,
            state.best_bid,
            state.best_ask,
            cfg.tick_size,
            cfg.lot_size,
            0.001,
            5.0,
            cfg.order_size,
            cfg.max_inventory,
            0.0,
            False,
            0.1,
            (
                result.quote.max_spread
                if cfg.spread_cap_mode == qc.SPREAD_CAP_COMPRESS
                else 0.0
            ),
            False,
            0.0,
            0.0,
            False,
            0.0,
            0.0,
        ),
        (
            result.buy_policy.allow_post,
            result.buy_policy.allow_exposure_increase,
            result.buy_policy.spread_mult,
            result.buy_policy.size_mult,
            0.0,
        ),
        (
            result.sell_policy.allow_post,
            result.sell_policy.allow_exposure_increase,
            result.sell_policy.spread_mult,
            result.sell_policy.size_mult,
            0.0,
        ),
    )
    assert result.routing.bid_price == pytest.approx(expected_route[0])
    assert result.routing.ask_price == pytest.approx(expected_route[1])
    assert result.routing.bid_size == pytest.approx(expected_route[9])
    assert result.routing.ask_size == pytest.approx(expected_route[10])


@pytest.mark.parametrize(
    "cfg",
    (
        _cfg(
            regime_enabled=True,
            use_bar_pricing=False,
            use_depth_microprice=True,
            use_depth_kappa=True,
            historical_p3_scalar_adapter_enabled=True,
            p3_delta_star=0.2,
            p3_kappa_eff=0.7,
            p3_event_type="touch",
            p3_horizon_s=10.0,
            p3_distance_origin="same_side_best_bid_or_ask_at_window_start",
            p3_distance_unit="USDC_per_BTC",
            p3_side="pooled_buy_sell",
            p3_queue_included=False,
            p3_artifact_sha256="c" * 64,
        ),
        _cfg(
            use_bar_pricing=False,
            use_depth_microprice=True,
            p3_delta_star=0.2,
            p3_side_bbo_floor_enabled=True,
            p3_event_type="touch",
            p3_horizon_s=10.0,
            p3_distance_origin="same_side_best_bid_or_ask_at_window_start",
            p3_distance_unit="USDC_per_BTC",
            p3_side="pooled_buy_sell",
            p3_queue_included=False,
            p3_artifact_sha256="d" * 64,
        ),
        _cfg(
            use_bar_pricing=False,
            eta_inventory=0.046,
            a_spread=0.046,
            risk_per_order=0.031,
            execution_intensity_slope=0.83,
            risk_horizon_s=5.0,
            quote_horizon_s=5.0,
            f03_ret_action_horizon_s=5.0,
            trade_intensity_acceleration_spread_mult=1.7,
        ),
    ),
)
def test_fused_native_quote_hot_plan_is_bitwise_identical_to_public_core(cfg):
    cpp_cfg = qc._copy_attrs(
        cfg,
        narrowgate_cpp.QuoteCoreConfig(),
        qc._CPP_CFG_FIELDS,
    )
    runtime = narrowgate_cpp.NativeLiveRuntimeCore(cpp_cfg)
    # Use the exact integer-tick/lot materialization performed by the fused
    # market state, so this test isolates hot-plan effects rather than Python
    # literal-vs-integer-grid rounding.
    prices_bid = tuple(value * cpp_cfg.tick_size for value in (999, 998, 997))
    quantities_bid = tuple(
        value * cpp_cfg.lot_size for value in (2000, 3000, 4000)
    )
    prices_ask = tuple(value * cpp_cfg.tick_size for value in (1001, 1002, 1003))
    quantities_ask = tuple(
        value * cpp_cfg.lot_size for value in (2500, 2000, 5000)
    )
    timestamp_ns = 1_000_000_000
    assert runtime.publish_book(
        _native_depth20_update(
            side="BUY",
            prices=prices_bid,
            quantities=quantities_bid,
            timestamp_ns=timestamp_ns,
        ),
        _native_depth20_update(
            side="SELL",
            prices=prices_ask,
            quantities=quantities_ask,
            timestamp_ns=timestamp_ns,
        ),
    ) == narrowgate_cpp.MarketStateUpdateStatus.Applied

    state = _state(
        mid=100.0,
        inventory=0.003,
        sigma_sq=1.23456789,
        trade_intensity=137.0,
        best_bid=prices_bid[0],
        best_ask=prices_ask[0],
        ber_active=True,
        mo_ema_all=-0.75,
        mo_ema_bid=-1.25,
        mo_ema_ask=-0.25,
        position_open=True,
        hold_time_s=7.0,
        unrealized_pnl=-0.125,
    )
    prediction = qc.QuotePrediction(
        dir_10s=0.57,
        vol_10s=1.75,
        ret_10s=1.25e-5,
        tox_bid=0.61,
        tox_ask=0.43,
    )
    cpp_state = qc._copy_attrs(
        state,
        narrowgate_cpp.QuoteState(),
        qc._CPP_STATE_FIELDS,
    )
    cpp_prediction = qc._copy_attrs(
        prediction,
        narrowgate_cpp.QuotePrediction(),
        qc._CPP_PRED_FIELDS,
    )
    public_result = narrowgate_cpp.compute_quote_core(
        cpp_state,
        cpp_cfg,
        cpp_prediction,
        _cpp_depth_snapshot(
            prices_bid,
            quantities_bid,
            prices_ask,
            quantities_ask,
        ),
    )

    input_value = narrowgate_cpp.NativeLiveDecisionInput()
    input_value.quote_state = cpp_state
    input_value.prediction = cpp_prediction
    input_value.decision_ts_ns = 1_100_000_000
    input_value.max_book_age_ns = 1_000_000_000
    input_value.expected_market_publication_sequence = 2
    input_value.expected_bid_generation = 1
    input_value.expected_ask_generation = 1
    input_value.min_qty = 0.001
    input_value.min_notional = 5.0
    fused_result = runtime.decide(input_value)
    assert fused_result.status == narrowgate_cpp.NativeLiveDecisionStatus.Applied
    assert _native_value_bits(fused_result.quote) == _native_value_bits(public_result)


@pytest.mark.parametrize(
    ("level_count", "updates"),
    (
        (
            20,
            {
                "use_depth_microprice": True,
                "microprice_levels": 7,
                "use_depth_kappa": True,
                "kappa_levels": 13,
                "depth_tox_enabled": True,
                "depth_tox_levels": 17,
                "book_imb_strength": 0.02,
                "book_imb_levels": 20,
                "trace_book_imb_levels": 11,
            },
        ),
        (
            20,
            {
                "use_depth_microprice": True,
                "microprice_levels": -7,
                "use_depth_kappa": True,
                "kappa_levels": 99,
                "depth_tox_enabled": True,
                "depth_tox_levels": -3,
                "book_imb_strength": 0.02,
                "book_imb_levels": 999,
                "trace_book_imb_levels": -5,
            },
        ),
        (
            4,
            {
                "use_depth_microprice": False,
                "use_depth_kappa": False,
                "depth_tox_enabled": False,
                "book_imb_strength": 0.0,
                "trace_book_imb_levels": 3,
            },
        ),
    ),
)
def test_fused_native_tick_lot_prefix_is_bitwise_depth_fallback(
    level_count,
    updates,
):
    """The direct tick/lot prefix must equal the generic double-depth path."""

    cfg = _cfg(ml_enabled=False, use_bar_pricing=False, **updates)
    cpp_cfg = qc._copy_attrs(
        cfg,
        narrowgate_cpp.QuoteCoreConfig(),
        qc._CPP_CFG_FIELDS,
    )
    runtime = narrowgate_cpp.NativeLiveRuntimeCore(cpp_cfg)
    bid_ticks = tuple(1000 - index for index in range(level_count))
    ask_ticks = tuple(1002 + index for index in range(level_count))
    # Irregular lot counts make reassociation/integer-sum shortcuts visible.
    bid_lots = tuple(
        (index * index * 17 + index * 3 + 1) % 997 + 1
        for index in range(level_count)
    )
    ask_lots = tuple(
        (index * index * 29 + index * 11 + 7) % 991 + 1
        for index in range(level_count)
    )
    bid_prices = tuple(value * cpp_cfg.tick_size for value in bid_ticks)
    ask_prices = tuple(value * cpp_cfg.tick_size for value in ask_ticks)
    bid_quantities = tuple(value * cpp_cfg.lot_size for value in bid_lots)
    ask_quantities = tuple(value * cpp_cfg.lot_size for value in ask_lots)
    timestamp_ns = 1_000_000_000
    assert runtime.publish_book(
        _native_depth20_update(
            side="BUY",
            prices=bid_prices,
            quantities=bid_quantities,
            timestamp_ns=timestamp_ns,
        ),
        _native_depth20_update(
            side="SELL",
            prices=ask_prices,
            quantities=ask_quantities,
            timestamp_ns=timestamp_ns,
        ),
    ) == narrowgate_cpp.MarketStateUpdateStatus.Applied

    state = _state(
        mid=100.1,
        inventory=0.003,
        sigma_sq=1.23456789,
        trade_intensity=137.0,
        best_bid=bid_prices[0],
        best_ask=ask_prices[0],
        mo_ema_all=-0.75,
        mo_ema_bid=-1.25,
        mo_ema_ask=-0.25,
        position_open=True,
        hold_time_s=7.0,
        unrealized_pnl=-0.125,
    )
    cpp_state = qc._copy_attrs(
        state,
        narrowgate_cpp.QuoteState(),
        qc._CPP_STATE_FIELDS,
    )
    cpp_prediction = qc._copy_attrs(
        qc.QuotePrediction(),
        narrowgate_cpp.QuotePrediction(),
        qc._CPP_PRED_FIELDS,
    )
    public_result = narrowgate_cpp.compute_quote_core(
        cpp_state,
        cpp_cfg,
        cpp_prediction,
        _cpp_depth_snapshot(
            bid_prices,
            bid_quantities,
            ask_prices,
            ask_quantities,
        ),
    )
    python_result = qc.compute_quote_core(
        state,
        cfg,
        qc.QuotePrediction(),
        qc.DepthSnapshot(
            bids=tuple(zip(bid_prices, bid_quantities, strict=True)),
            asks=tuple(zip(ask_prices, ask_quantities, strict=True)),
        ),
    )

    input_value = narrowgate_cpp.NativeLiveDecisionInput()
    input_value.quote_state = cpp_state
    input_value.prediction = cpp_prediction
    input_value.decision_ts_ns = 1_100_000_000
    input_value.max_book_age_ns = 1_000_000_000
    input_value.expected_market_publication_sequence = 2
    input_value.expected_bid_generation = 1
    input_value.expected_ask_generation = 1
    input_value.min_qty = 0.001
    input_value.min_notional = 5.0
    fused_result = runtime.decide(input_value)
    assert fused_result.status == narrowgate_cpp.NativeLiveDecisionStatus.Applied
    assert _native_value_bits(fused_result.quote) == _native_value_bits(public_result)
    assert struct.pack("!d", fused_result.quote.bid_price) == struct.pack(
        "!d", python_result.bid_price
    )
    assert struct.pack("!d", fused_result.quote.ask_price) == struct.pack(
        "!d", python_result.ask_price
    )


def test_fused_native_tick_lot_prefix_never_consumes_invalid_level():
    runtime, input_value, _bids, _asks = _native_runtime_with_valid_input()
    bad_bids = _native_depth20_update(
        side="BUY",
        prices=(99.9, 99.8, 99.7),
        quantities=(1.0, 2.0, 3.0),
        timestamp_ns=1_100_000_000,
        generation=2,
    )
    bad_lots = list(bad_bids.quantity_lots)
    bad_lots[1] = 0
    bad_bids.quantity_lots = bad_lots
    next_asks = _native_depth20_update(
        side="SELL",
        prices=(100.1, 100.2, 100.3),
        quantities=(1.0, 2.0, 3.0),
        timestamp_ns=1_100_000_000,
        generation=2,
    )
    assert runtime.publish_book(bad_bids, next_asks) == (
        narrowgate_cpp.MarketStateUpdateStatus.InvalidLevel
    )
    assert runtime.feed_fault_latched
    assert runtime.decide(input_value).status == (
        narrowgate_cpp.NativeLiveDecisionStatus.FeedFault
    )

    # The rejected publication cannot mutate/publish the previously admitted
    # book; its sequence remains the original paired publication.
    assert runtime.market_publication_sequence == 2


@pytest.mark.parametrize(
    ("updates", "message"),
    (
        ({"gamma": 0.0}, "gamma must be positive and finite"),
        ({"risk_horizon_s": -1.0}, "risk_horizon_s must be positive and finite"),
        (
            {
                "historical_p3_scalar_adapter_enabled": True,
                "p3_delta_star": 0.1,
            },
            "active P3 projection requires the complete touch identity",
        ),
        (
            {
                "ml_enabled": True,
                "ret_skew": 0.1,
                "quote_horizon_s": 1.0,
                "f03_ret_action_horizon_s": 10.0,
                "f03_ret_action_compatible": True,
            },
            "F03 ret action horizon is not compatible with the quote consumer",
        ),
    ),
)
def test_fused_native_quote_hot_plan_rejects_invalid_config_at_construction(
    updates,
    message,
):
    cfg = qc._copy_attrs(
        _cfg(ml_enabled=False, ret_skew=0.0),
        narrowgate_cpp.QuoteCoreConfig(),
        qc._CPP_CFG_FIELDS,
    )
    for field, value in updates.items():
        setattr(cfg, field, value)
    with pytest.raises(ValueError, match=message):
        narrowgate_cpp.NativeLiveRuntimeCore(cfg)


def test_native_quote_policy_stage_matches_separate_quote_and_policy_bits():
    cfg = _cfg(ml_enabled=False, use_bar_pricing=False)
    cpp_cfg = qc._copy_attrs(
        cfg, narrowgate_cpp.QuoteCoreConfig(), qc._CPP_CFG_FIELDS
    )
    state = _state(
        mid=100.0,
        inventory=0.003,
        sigma_sq=1.25,
        trade_intensity=137.0,
        best_bid=99.9,
        best_ask=100.1,
        mo_ema_bid=-2.0,
        mo_ema_ask=1.0,
    )
    prediction = qc.QuotePrediction(
        dir_10s=0.61, vol_10s=0.2, ret_10s=0.0, tox_bid=0.7, tox_ask=0.2
    )
    depth = qc.DepthSnapshot(
        bids=((99.9, 2.0), (99.8, 3.0)),
        asks=((100.1, 4.0), (100.2, 5.0)),
    )
    cpp_state = qc._copy_attrs(state, narrowgate_cpp.QuoteState(), qc._CPP_STATE_FIELDS)
    cpp_prediction = qc._copy_attrs(
        prediction, narrowgate_cpp.QuotePrediction(), qc._CPP_PRED_FIELDS
    )
    cpp_depth = _cpp_depth_snapshot(
        (99.9, 99.8), (2.0, 3.0), (100.1, 100.2), (4.0, 5.0)
    )
    expected_quote = narrowgate_cpp.compute_quote_core(
        cpp_state, cpp_cfg, cpp_prediction, cpp_depth
    )
    buy_input = narrowgate_cpp.CommonSidePolicyInputPod()
    buy_input.exposure_increasing = True
    buy_input.depth_age_s = 0.2
    buy_input.max_book_age_s = 1.0
    buy_input.l2_quote_flip_rate = 0.4
    buy_input.l2_book_cancel_ratio = 0.05
    buy_input.l2_near_depth_total = 5.0
    buy_input.thin_depth_threshold = 1.0
    sell_input = narrowgate_cpp.CommonSidePolicyInputPod()
    sell_input.depth_age_s = 0.2
    sell_input.max_book_age_s = 1.0
    sell_input.l2_near_depth_total = 9.0
    sell_input.thin_depth_threshold = 1.0

    stage = narrowgate_cpp.NativeQuotePolicyStage(cpp_cfg)
    actual = stage.compute(
        tuple(getattr(state, name) for name in qc._CPP_STATE_FIELDS),
        tuple(getattr(prediction, name) for name in qc._CPP_PRED_FIELDS),
        depth.bids,
        depth.asks,
        tuple(getattr(buy_input, name) for name in qc._CPP_COMMON_POLICY_FIELDS),
        tuple(getattr(sell_input, name) for name in qc._CPP_COMMON_POLICY_FIELDS),
    )
    assert _native_value_bits(actual.quote) == _native_value_bits(expected_quote)
    for side, source in (("BUY", buy_input), ("SELL", sell_input)):
        quote_side = expected_quote.buy if side == "BUY" else expected_quote.sell
        expected = evaluate_common_side_policy(
            CommonSidePolicyInput(
                exposure_increasing=source.exposure_increasing,
                fill_cooldown_active=source.fill_cooldown_active,
                inventory_ratio=min(abs(state.inventory) / cfg.max_inventory, 1.0),
                depth_age_s=source.depth_age_s,
                max_book_age_s=source.max_book_age_s,
                toxicity=prediction.tox_bid if side == "BUY" else prediction.tox_ask,
                markout_ema=state.mo_ema_bid if side == "BUY" else state.mo_ema_ask,
                markout_spread_scale=cfg.markout_spread_scale,
                markout_reference=state.mo_ref,
                microprice_shift_bps=expected_quote.microprice_shift_bps,
                l2_quote_flip_rate=source.l2_quote_flip_rate,
                l2_book_cancel_ratio=source.l2_book_cancel_ratio,
                l2_near_depth_total=source.l2_near_depth_total,
                thin_depth_threshold=source.thin_depth_threshold,
                kappa_depth_baseline=cfg.kappa_depth_baseline,
                side_adverse=quote_side.side_adverse,
                side_adverse_pause=quote_side.side_adverse_pause,
                defense_guard=quote_side.defense_guard,
                defense_spread_mult=quote_side.defense_spread_mult,
                defense_pause=quote_side.defense_pause,
            )
        )
        observed = actual.buy_policy if side == "BUY" else actual.sell_policy
        assert (
            observed.allow_post,
            observed.allow_exposure_increase,
            struct.pack("!d", observed.spread_mult),
            struct.pack("!d", observed.size_mult),
            observed.reason_mask,
        ) == (
            expected.allow_post,
            expected.allow_exposure_increase,
            struct.pack("!d", expected.spread_mult),
            struct.pack("!d", expected.size_mult),
            expected.reason_mask,
        )


def test_native_quote_policy_stage_reserve_does_not_truncate_deeper_books():
    cfg = _cfg(
        ml_enabled=False,
        use_bar_pricing=False,
        book_imb_strength=0.75,
        book_imb_levels=25,
    )
    cpp_cfg = qc._copy_attrs(
        cfg, narrowgate_cpp.QuoteCoreConfig(), qc._CPP_CFG_FIELDS
    )
    state = _state(mid=100.0, best_bid=99.9, best_ask=100.1)
    prediction = qc.QuotePrediction()
    bid_prices = tuple(99.9 - 0.1 * index for index in range(25))
    ask_prices = tuple(100.1 + 0.1 * index for index in range(25))
    bid_quantities = (1.0,) * 20 + (100.0,) * 5
    ask_quantities = (1.0,) * 25
    cpp_state = qc._copy_attrs(
        state, narrowgate_cpp.QuoteState(), qc._CPP_STATE_FIELDS
    )
    cpp_prediction = qc._copy_attrs(
        prediction, narrowgate_cpp.QuotePrediction(), qc._CPP_PRED_FIELDS
    )
    expected = narrowgate_cpp.compute_quote_core(
        cpp_state,
        cpp_cfg,
        cpp_prediction,
        _cpp_depth_snapshot(
            bid_prices,
            bid_quantities,
            ask_prices,
            ask_quantities,
        ),
    )
    truncated = narrowgate_cpp.compute_quote_core(
        cpp_state,
        cpp_cfg,
        cpp_prediction,
        _cpp_depth_snapshot(
            bid_prices[:20],
            bid_quantities[:20],
            ask_prices[:20],
            ask_quantities[:20],
        ),
    )
    assert _native_value_bits(expected) != _native_value_bits(truncated)
    buy_policy = narrowgate_cpp.CommonSidePolicyInputPod()
    sell_policy = narrowgate_cpp.CommonSidePolicyInputPod()

    actual = narrowgate_cpp.NativeQuotePolicyStage(cpp_cfg).compute(
        tuple(getattr(state, name) for name in qc._CPP_STATE_FIELDS),
        tuple(getattr(prediction, name) for name in qc._CPP_PRED_FIELDS),
        tuple(zip(bid_prices, bid_quantities, strict=True)),
        tuple(zip(ask_prices, ask_quantities, strict=True)),
        tuple(getattr(buy_policy, name) for name in qc._CPP_COMMON_POLICY_FIELDS),
        tuple(getattr(sell_policy, name) for name in qc._CPP_COMMON_POLICY_FIELDS),
    )

    assert _native_value_bits(actual.quote) == _native_value_bits(expected)


def test_native_quote_policy_stage_validates_lengths_before_depth_iteration():
    cfg = qc._copy_attrs(
        _cfg(ml_enabled=False),
        narrowgate_cpp.QuoteCoreConfig(),
        qc._CPP_CFG_FIELDS,
    )

    class ExplodingDepth:
        def __iter__(self):
            raise AssertionError("depth must not be consumed before length validation")

    with pytest.raises(ValueError, match="input length mismatch"):
        narrowgate_cpp.NativeQuotePolicyStage(cfg).compute(
            (),
            (),
            ExplodingDepth(),
            ExplodingDepth(),
            (),
            (),
        )


def test_fused_native_live_runtime_fails_closed_on_stale_or_regressed_clock():
    cpp_cfg = qc._copy_attrs(
        _cfg(ml_enabled=False, use_bar_pricing=False),
        narrowgate_cpp.QuoteCoreConfig(),
        qc._CPP_CFG_FIELDS,
    )
    runtime = narrowgate_cpp.NativeLiveRuntimeCore(cpp_cfg)
    runtime.publish_book(
        _native_depth20_update(
            side="BUY",
            prices=(99.9,),
            quantities=(1.0,),
            timestamp_ns=1_000_000_000,
        ),
        _native_depth20_update(
            side="SELL",
            prices=(100.1,),
            quantities=(1.0,),
            timestamp_ns=1_000_000_000,
        ),
    )
    input_value = narrowgate_cpp.NativeLiveDecisionInput()
    input_value.quote_state.mid = 100.0
    input_value.quote_state.sigma_sq = 1.0
    input_value.quote_state.trade_intensity = 100.0
    input_value.min_qty = 0.001
    input_value.min_notional = 5.0
    input_value.decision_ts_ns = 2_000_000_000
    input_value.max_book_age_ns = 500_000_000
    input_value.expected_market_publication_sequence = 2
    input_value.expected_bid_generation = 1
    input_value.expected_ask_generation = 1
    assert runtime.decide(input_value).status == (
        narrowgate_cpp.NativeLiveDecisionStatus.StaleBook
    )
    input_value.decision_ts_ns = 999_999_999
    assert runtime.decide(input_value).status == (
        narrowgate_cpp.NativeLiveDecisionStatus.DecisionClockRegressed
    )


def test_fused_native_live_runtime_rejects_mixed_market_generation():
    cpp_cfg = qc._copy_attrs(
        _cfg(ml_enabled=False, use_bar_pricing=False),
        narrowgate_cpp.QuoteCoreConfig(),
        qc._CPP_CFG_FIELDS,
    )
    runtime = narrowgate_cpp.NativeLiveRuntimeCore(cpp_cfg)
    bids = _native_depth20_update(
        side="BUY",
        prices=(99.9,),
        quantities=(1.0,),
        timestamp_ns=1_000_000_000,
        generation=1,
    )
    asks = _native_depth20_update(
        side="SELL",
        prices=(100.1,),
        quantities=(1.0,),
        timestamp_ns=1_000_000_000,
        generation=1,
    )
    assert runtime.publish_book(bids, asks) == (
        narrowgate_cpp.MarketStateUpdateStatus.Applied
    )

    input_value = narrowgate_cpp.NativeLiveDecisionInput()
    input_value.quote_state.mid = 100.0
    input_value.quote_state.sigma_sq = 1.0
    input_value.quote_state.trade_intensity = 100.0
    input_value.min_qty = 0.001
    input_value.min_notional = 5.0
    input_value.decision_ts_ns = 1_200_000_000
    input_value.max_book_age_ns = 1_000_000_000
    input_value.expected_market_publication_sequence = 2
    input_value.expected_bid_generation = 1
    input_value.expected_ask_generation = 1

    bids.clock.generation = 2
    bids.clock.source_ts_ns += 1
    bids.clock.exchange_ts_ns += 1
    bids.clock.receive_ts_ns += 1
    bids.clock.visible_ts_ns += 1
    asks.clock.generation = 2
    asks.clock.source_ts_ns += 1
    asks.clock.exchange_ts_ns += 1
    asks.clock.receive_ts_ns += 1
    asks.clock.visible_ts_ns += 1
    assert runtime.publish_book(bids, asks) == (
        narrowgate_cpp.MarketStateUpdateStatus.Applied
    )
    result = runtime.decide(input_value)
    assert result.status == (
        narrowgate_cpp.NativeLiveDecisionStatus.MarketIdentityMismatch
    )
    assert result.market_publication_sequence == 4
