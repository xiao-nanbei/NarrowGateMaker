import csv
import hashlib
import hmac
import json
import threading
import time

import pytest
import requests

from execution.runtime_evidence_writer import RuntimeEvidenceWriter
from live.binance_usdm_transport import (
    BinanceUsdMOrderAdmissionRejected,
    BinanceUsdMOrderGateway,
    BinanceUsdMOrderLaneFull,
    BinanceUsdMOrderProtocolUnknown,
    BinanceUsdMRestRole,
    BinanceUsdMWebSocketApiError,
    BinanceUsdMWebSocketExperimentExpired,
    BinanceUsdMWebSocketOrderConfig,
    BinanceUsdMWebSocketOrderUnknown,
    create_binance_usdm_rest_clients,
    create_binance_usdm_websocket_order_gateway,
)


class _RestClient:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.session = requests.Session()


class _FakeConnection:
    def __init__(self, responder):
        self.responder = responder
        self.sent = []
        self.timeout = None
        self.connected = True
        self._response = None
        self._condition = threading.Condition()

    def settimeout(self, timeout):
        self.timeout = timeout

    def send(self, payload):
        self.sent.append(payload)
        with self._condition:
            self._response = self.responder(json.loads(payload))
            self._condition.notify_all()

    def recv(self):
        with self._condition:
            while self._response is None and self.connected:
                self._condition.wait(timeout=0.05)
            if not self.connected:
                raise ConnectionError("closed")
            response, self._response = self._response, None
        if isinstance(response, BaseException):
            raise response
        return json.dumps(response)

    def close(self):
        with self._condition:
            self.connected = False
            self._condition.notify_all()


class _ConnectionFactory:
    def __init__(self, connections):
        self.connections = list(connections)
        self.calls = []

    def __call__(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if not self.connections:
            raise AssertionError("unexpected extra WebSocket connection")
        return self.connections.pop(0)


class _IdleTimeoutConnection(_FakeConnection):
    def recv(self):
        with self._condition:
            if self._response is None and self.connected:
                self._condition.wait(timeout=0.01)
            if not self.connected:
                raise ConnectionError("closed")
            if self._response is None:
                raise TimeoutError("idle receive wake-up")
            response, self._response = self._response, None
        return json.dumps(response)


class _MonotonicClock:
    def __init__(self):
        self.value = 1_000_000_000
        self.lock = threading.Lock()

    def __call__(self):
        with self.lock:
            self.value += 1_000_000
            return self.value


def _enabled_config(**kwargs):
    values = {"enabled": True, "request_timeout_s": 1.25}
    values.update(kwargs)
    return BinanceUsdMWebSocketOrderConfig(**values)


def test_rest_roles_have_independent_persistent_zero_retry_sessions():
    clients = create_binance_usdm_rest_clients(
        key="key",
        secret="secret",
        timeout_s=2.5,
        client_factory=_RestClient,
    )
    try:
        sessions = [clients.by_role(role).session for role in BinanceUsdMRestRole]
        assert len({id(session) for session in sessions}) == 8
        assert clients.identity() == {
            "schema_version": "narrowgate.binance_usdm_rest_roles.v2",
            "roles": (
                "order_buy",
                "order_sell",
                "order_safety",
                "reconciliation",
                "reconciliation_worker",
                "market_snapshot",
                "metrics",
                "listen_key",
            ),
            "independent_sessions": True,
        }
        for role, session in zip(BinanceUsdMRestRole, sessions, strict=True):
            client = clients.by_role(role)
            assert client._narrowgate_transport_role == role.value
            policy = session.get_adapter("https://").max_retries
            assert policy.total == 0
            assert policy.connect == 0
            assert policy.read == 0
            assert policy.status == 0
        assert clients.order.kwargs["timeout"] == 2.5
    finally:
        clients.close()


def test_rest_role_construction_preserves_factory_error_and_closes_all_partials():
    closed: list[int] = []
    created = 0

    class TrackingSession(requests.Session):
        def __init__(self, index: int) -> None:
            super().__init__()
            self.index = index

        def close(self) -> None:
            closed.append(self.index)
            super().close()
            if self.index == 0:
                raise OSError("first session close failed")

    class FailingClient:
        def __init__(self, **_kwargs) -> None:
            nonlocal created
            index = created
            created += 1
            if index == 2:
                raise ValueError("third role constructor failed")
            self.session = TrackingSession(index)

    with pytest.raises(ValueError, match="third role constructor failed") as caught:
        create_binance_usdm_rest_clients(
            key="key",
            secret="secret",
            timeout_s=2.5,
            client_factory=FailingClient,
        )

    assert closed == [0, 1]
    assert caught.value.__notes__ == [
        "REST role construction cleanup failed: RuntimeError: "
        "1 Binance USD-M REST session(s) failed to close"
    ]


def test_websocket_order_gateway_is_default_off():
    assert create_binance_usdm_websocket_order_gateway(key="key", secret="secret") is None


@pytest.mark.parametrize(
    "url",
    (
        "ws://ws-fapi.binance.com/ws-fapi/v1",
        "wss://evil.example/ws-fapi/v1",
        "wss://ws-fapi.binance.com/ws-fapi/v1?token=secret",
        "wss://user@ws-fapi.binance.com/ws-fapi/v1",
    ),
)
def test_websocket_order_config_accepts_only_exact_official_wss_urls(url):
    with pytest.raises(ValueError, match="exact official"):
        BinanceUsdMWebSocketOrderConfig(enabled=True, url=url)


@pytest.mark.parametrize("max_runtime_s", (0.0, 3600.1, float("inf")))
def test_websocket_order_config_bounds_experiment_runtime(max_runtime_s):
    with pytest.raises(ValueError, match="max_runtime_s"):
        BinanceUsdMWebSocketOrderConfig(enabled=True, max_runtime_s=max_runtime_s)


def test_websocket_gateway_rejects_new_write_after_ab_runtime_without_dispatch():
    now_ns = [0]
    connection = _FakeConnection(
        lambda request: {
            "id": request["id"],
            "status": 200,
            "result": {"orderId": 1, "status": "NEW"},
        }
    )
    gateway = create_binance_usdm_websocket_order_gateway(
        key="key",
        secret="secret",
        config=_enabled_config(max_runtime_s=1.0),
        connection_factory=_ConnectionFactory([connection]),
        monotonic_ns=lambda: now_ns[0],
    )
    assert gateway is not None
    gateway.start()
    now_ns[0] = 1_000_000_000

    with pytest.raises(BinanceUsdMWebSocketExperimentExpired):
        gateway.new_order(symbol="BTCUSDC", side="BUY", type="LIMIT")

    assert connection.sent == []
    assert gateway.health_snapshot()["runtime_expired"] is True


def test_websocket_order_place_is_signed_correlated_and_compatible():
    def responder(request):
        return {
            "id": request["id"],
            "status": 200,
            "result": {
                "orderId": 17,
                "clientOrderId": request["params"]["newClientOrderId"],
                "symbol": request["params"]["symbol"],
                "side": request["params"]["side"],
                "origQty": request["params"]["quantity"],
                "executedQty": "0",
                "status": "NEW",
            },
        }

    connection = _FakeConnection(responder)
    factory = _ConnectionFactory([connection])
    gateway = create_binance_usdm_websocket_order_gateway(
        key="api-key",
        secret="api-secret",
        config=_enabled_config(),
        connection_factory=factory,
        request_id_factory=lambda: "request-1",
        wall_time_ms=lambda: 1_725_000_000_123,
        monotonic_ns=_MonotonicClock(),
    )
    assert gateway is not None

    result = gateway.new_order(
        symbol="BTCUSDC",
        side="BUY",
        type="LIMIT",
        timeInForce="GTX",
        quantity="0.001",
        price="63000.1",
        newClientOrderId="ng-1",
        newOrderRespType="RESULT",
    )

    assert result["orderId"] == 17
    assert factory.calls == [
        (
            "wss://ws-fapi.binance.com/ws-fapi/v1",
            {"timeout": 3.0, "enable_multithread": True},
        )
    ]
    assert connection.timeout == 1.0
    assert len(connection.sent) == 1
    payload = json.loads(connection.sent[0])
    assert payload["id"] == "request-1"
    assert payload["method"] == "order.place"
    signed = dict(payload["params"])
    signature = signed.pop("signature")
    expected = hmac.new(
        b"api-secret",
        "&".join(f"{key}={value}" for key, value in sorted(signed.items())).encode(),
        hashlib.sha256,
    ).hexdigest()
    assert signature == expected
    assert signed["apiKey"] == "api-key"
    assert signed["timestamp"] == 1_725_000_000_123
    assert signed["recvWindow"] == 5_000
    assert "api-key" not in json.dumps(gateway.health_snapshot())
    health = gateway.health_snapshot()
    assert health["automatic_write_retries"] == 0
    assert health["counters"]["successes"] == 1
    assert health["method_counts"] == {"order.place": 1}
    assert health["last_receipt"]["client_order_id"] == "ng-1"
    gateway.close()


def test_rest_and_websocket_requests_share_one_async_receipt_schema(tmp_path):
    receipt_path = tmp_path / "order_gateway_receipts.csv"
    writer = RuntimeEvidenceWriter(queue_capacity=32)

    class ReceiptRestClient:
        def new_order(self, **params):
            return {
                "clientOrderId": params["newClientOrderId"],
                "status": "NEW",
            }

        def cancel_order(self, **_params):
            return {"status": "CANCELED"}

        def cancel_open_orders(self, **_params):
            return []

    rest_gateway = BinanceUsdMOrderGateway(
        rest_order_client=ReceiptRestClient(),
        request_id_factory=lambda: "rest-request-1",
    )
    rest_gateway.set_runtime_evidence_writer(writer, str(receipt_path))
    assert rest_gateway.new_order(
        symbol="BTCUSDC",
        side="BUY",
        newClientOrderId="rest-cid",
        _narrowgate_decision_ts_ns=100,
        _narrowgate_decision_id="decision-rest",
    )["status"] == "NEW"

    connection = _FakeConnection(
        lambda request: {
            "id": request["id"],
            "status": 200,
            "result": {
                "clientOrderId": request["params"]["newClientOrderId"],
                "status": "NEW",
            },
        }
    )
    websocket = create_binance_usdm_websocket_order_gateway(
        key="key",
        secret="secret",
        config=_enabled_config(),
        connection_factory=_ConnectionFactory([connection]),
        request_id_factory=lambda: "ws-request-1",
    )
    assert websocket is not None
    websocket_gateway = BinanceUsdMOrderGateway(
        rest_order_client=ReceiptRestClient(),
        websocket_order_gateway=websocket,
    )
    websocket_gateway.set_runtime_evidence_writer(writer, str(receipt_path))
    assert websocket_gateway.new_order(
        symbol="BTCUSDC",
        side="SELL",
        newClientOrderId="ws-cid",
        _narrowgate_decision_ts_ns=200,
        _narrowgate_decision_id="decision-ws",
    )["status"] == "NEW"
    websocket_gateway.close()
    writer.close(drain_timeout_s=2.0)

    with receipt_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert [row["transport"] for row in rows] == [
        "rest",
        "binance_usdm_websocket_api",
    ]
    assert [row["request_id"] for row in rows] == [
        "rest-request-1",
        "ws-request-1",
    ]
    assert [row["client_order_id"] for row in rows] == ["rest-cid", "ws-cid"]
    assert [row["decision_id"] for row in rows] == [
        "decision-rest",
        "decision-ws",
    ]
    assert [row["decision_ts_ns"] for row in rows] == ["100", "200"]
    assert all(row["execution_status"] == "authoritative_success" for row in rows)
    assert rows[0]["connection_generation"] == "0"
    assert rows[0]["wire_ts_ns"] == "0"
    assert int(rows[1]["connection_generation"]) == 1
    assert int(rows[1]["dispatch_ts_ns"]) > 0
    assert int(rows[1]["wire_ts_ns"]) > 0
    assert int(rows[1]["response_ts_ns"]) > 0


def test_private_new_visibility_is_recorded_before_response_and_joined_by_request_id(
    tmp_path,
):
    receipt_path = tmp_path / "order_gateway_receipts.csv"
    writer = RuntimeEvidenceWriter(queue_capacity=8)
    gateway = None

    def respond(request):
        assert gateway is not None
        assert gateway.record_private_order_visibility(
            {
                "c": request["params"]["newClientOrderId"],
                "x": "NEW",
                "X": "NEW",
                "T": 1_725_000_000_111,
            },
            receive_ts_ns=1_725_000_000_222_000_000,
        )
        return {
            "id": request["id"],
            "status": 200,
            "result": {
                "clientOrderId": request["params"]["newClientOrderId"],
                "status": "NEW",
            },
        }

    websocket = create_binance_usdm_websocket_order_gateway(
        key="key",
        secret="secret",
        config=_enabled_config(),
        connection_factory=_ConnectionFactory([_FakeConnection(respond)]),
        request_id_factory=lambda: "ws-private-visible-1",
    )
    assert websocket is not None
    gateway = BinanceUsdMOrderGateway(
        rest_order_client=object(),
        websocket_order_gateway=websocket,
    )
    gateway.set_runtime_evidence_writer(writer, str(receipt_path))

    response = gateway.new_order(
        symbol="BTCUSDC",
        side="BUY",
        type="LIMIT",
        newClientOrderId="private-visible-cid",
        _narrowgate_decision_ts_ns=1_725_000_000_000_000_000,
        _narrowgate_decision_id="private-visible-decision",
    )
    assert response["status"] == "NEW"
    gateway.close()
    writer.close(drain_timeout_s=2.0)

    with receipt_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert [row["record_type"] for row in rows] == [
        "private_visibility",
        "gateway_completion",
    ]
    assert {row["request_id"] for row in rows} == {"ws-private-visible-1"}
    private_row = rows[0]
    assert private_row["client_order_id"] == "private-visible-cid"
    assert private_row["decision_id"] == "private-visible-decision"
    assert private_row["method"] == "order.place"
    assert private_row["private_event_type"] == "NEW"
    assert private_row["private_order_status"] == "NEW"
    assert private_row["private_exchange_ts_ns"] == "1725000000111000000"
    assert private_row["private_visibility_ts_ns"] == "1725000000222000000"
    assert private_row["correlation_found"] == "1"
    assert private_row["execution_status"] == "private_visibility_observed"
    health = gateway.health_snapshot()
    assert health["private_visibility_counts"] == {
        "attempts": 1,
        "correlated": 1,
        "admitted": 1,
    }


def test_websocket_unknown_request_emits_joinable_async_receipt(tmp_path):
    receipt_path = tmp_path / "unknown_gateway_receipts.csv"
    writer = RuntimeEvidenceWriter(queue_capacity=8)
    connection = _FakeConnection(lambda _request: TimeoutError("response timed out"))
    websocket = create_binance_usdm_websocket_order_gateway(
        key="key",
        secret="secret",
        config=_enabled_config(request_timeout_s=0.05),
        connection_factory=_ConnectionFactory([connection]),
        request_id_factory=lambda: "unknown-request",
    )
    assert websocket is not None
    gateway = BinanceUsdMOrderGateway(
        rest_order_client=object(),
        websocket_order_gateway=websocket,
    )
    gateway.set_runtime_evidence_writer(writer, str(receipt_path))

    with pytest.raises(BinanceUsdMWebSocketOrderUnknown):
        gateway.cancel_order(
            symbol="BTCUSDC",
            origClientOrderId="unknown-cid",
            _narrowgate_decision_ts_ns=300,
            _narrowgate_decision_id="decision-unknown",
        )
    gateway.close()
    writer.close(drain_timeout_s=2.0)

    with receipt_path.open(newline="", encoding="utf-8") as handle:
        row = next(csv.DictReader(handle))
    assert row["request_id"] == "unknown-request"
    assert row["client_order_id"] == "unknown-cid"
    assert row["decision_id"] == "decision-unknown"
    assert row["execution_status"] == "unknown"
    assert row["may_have_been_dispatched"] == "1"
    assert row["response_authoritative"] == "0"
    assert int(row["unknown_ts_ns"]) > 0


def test_persistent_reader_survives_idle_receive_timeouts_before_order() -> None:
    connection = _IdleTimeoutConnection(
        lambda request: {
            "id": request["id"],
            "status": 200,
            "result": {"orderId": 19, "status": "NEW"},
        }
    )
    gateway = create_binance_usdm_websocket_order_gateway(
        key="key",
        secret="secret",
        config=_enabled_config(),
        connection_factory=_ConnectionFactory([connection]),
    )
    assert gateway is not None
    gateway.start()
    time.sleep(0.04)

    result = gateway.new_order(
        symbol="BTCUSDC",
        side="BUY",
        type="LIMIT",
        newClientOrderId="ng-idle-reader",
    )

    assert result["orderId"] == 19
    assert gateway.health_snapshot()["counters"]["successes"] == 1
    gateway.close()


def test_websocket_boolean_params_are_normalized_for_json_and_signature():
    connection = _FakeConnection(
        lambda request: {
            "id": request["id"],
            "status": 200,
            "result": {"orderId": 18, "status": "NEW"},
        }
    )
    gateway = create_binance_usdm_websocket_order_gateway(
        key="api-key",
        secret="api-secret",
        config=_enabled_config(),
        connection_factory=_ConnectionFactory([connection]),
        request_id_factory=lambda: "boolean-1",
        wall_time_ms=lambda: 1_725_000_000_123,
    )
    assert gateway is not None

    gateway.new_order(
        symbol="BTCUSDC",
        side="BUY",
        type="LIMIT",
        reduceOnly="true",
    )

    params = json.loads(connection.sent[0])["params"]
    assert params["reduceOnly"] is True
    signature = params.pop("signature")
    signature_payload = "&".join(
        f"{key}={'true' if value is True else 'false' if value is False else value}"
        for key, value in sorted(params.items())
    )
    assert (
        signature
        == hmac.new(
            b"api-secret",
            signature_payload.encode(),
            hashlib.sha256,
        ).hexdigest()
    )


def test_websocket_boolean_params_reject_ambiguous_text():
    gateway = create_binance_usdm_websocket_order_gateway(
        key="key",
        secret="secret",
        config=_enabled_config(),
        connection_factory=_ConnectionFactory(
            [_FakeConnection(lambda _request: AssertionError("must not send"))]
        ),
    )
    assert gateway is not None

    with pytest.raises(ValueError, match="reduceOnly"):
        gateway.new_order(
            symbol="BTCUSDC",
            side="BUY",
            type="LIMIT",
            reduceOnly="yes",
        )


def test_websocket_cancel_uses_expected_method():
    methods = []

    def responder(request):
        methods.append(request["method"])
        result = {
            "orderId": 19,
            "status": "CANCELED",
        }
        return {"id": request["id"], "status": 200, "result": result}

    connection = _FakeConnection(responder)
    gateway = create_binance_usdm_websocket_order_gateway(
        key="key",
        secret="secret",
        config=_enabled_config(),
        connection_factory=_ConnectionFactory([connection]),
        request_id_factory=lambda: "cancel-1",
    )
    assert gateway is not None

    assert gateway.cancel_order(symbol="BTCUSDC", origClientOrderId="ng-1")["status"] == "CANCELED"
    assert methods == ["order.cancel"]
    assert len(connection.sent) == 1


def test_hybrid_gateway_keeps_unsupported_cancel_all_on_isolated_rest():
    class RestOrderClient:
        def __init__(self):
            self.calls = []

        def new_order(self, **params):
            self.calls.append(("new", params))
            return {"transport": "rest"}

        def cancel_order(self, **params):
            self.calls.append(("cancel", params))
            return {"transport": "rest"}

        def cancel_open_orders(self, **params):
            self.calls.append(("cancel_all", params))
            return []

    class WebSocketOrderClient:
        def __init__(self):
            self.calls = []

        def new_order(self, **params):
            self.calls.append(("new", params))
            return {"transport": "websocket"}

        def cancel_order(self, **params):
            self.calls.append(("cancel", params))
            return {"transport": "websocket"}

        def health_snapshot(self):
            return {"enabled": True}

    rest = RestOrderClient()
    websocket = WebSocketOrderClient()
    gateway = BinanceUsdMOrderGateway(
        rest_order_client=rest,
        websocket_order_gateway=websocket,
    )

    assert gateway.new_order(symbol="BTCUSDC", side="BUY") == {
        "transport": "websocket"
    }
    assert gateway.cancel_order(
        symbol="BTCUSDC", _narrowgate_order_side="BUY"
    ) == {"transport": "websocket"}
    assert gateway.cancel_open_orders(symbol="BTCUSDC") == []
    assert [call[0] for call in websocket.calls] == ["new", "cancel"]
    assert [call[0] for call in rest.calls] == ["cancel_all"]
    assert gateway.health_snapshot()["cancel_all_transport"] == "rest"


def test_shutdown_admission_barrier_rejects_new_but_preserves_cancel_paths():
    class Client:
        def __init__(self):
            self.calls = []

        def new_order(self, **params):
            self.calls.append(("new", params))
            return {"status": "NEW"}

        def cancel_order(self, **params):
            self.calls.append(("cancel", params))
            return {"status": "CANCELED"}

        def cancel_open_orders(self, **params):
            self.calls.append(("cancel_all", params))
            return []

    client = Client()
    gateway = BinanceUsdMOrderGateway(rest_order_client=client)
    assert gateway.new_order(
        symbol="BTCUSDC",
        side="BUY",
        newClientOrderId="buy-before-shutdown",
    ) == {"status": "NEW"}

    gateway.revoke_new_order_admission()

    with pytest.raises(BinanceUsdMOrderAdmissionRejected, match="revoked"):
        gateway.new_order(
            symbol="BTCUSDC",
            side="SELL",
            newClientOrderId="sell-after-shutdown",
        )
    assert gateway.cancel_order(
        symbol="BTCUSDC",
        origClientOrderId="buy-before-shutdown",
        _narrowgate_order_side="BUY",
    ) == {"status": "CANCELED"}
    assert gateway.cancel_open_orders(symbol="BTCUSDC") == []
    assert [name for name, _params in client.calls] == [
        "new",
        "cancel",
        "cancel_all",
    ]
    assert gateway.health_snapshot()["new_order_admission_revoked"] is True
    gateway.close()


def test_gateway_default_preserves_legacy_global_write_order():
    class BlockingRestOrderClient:
        def __init__(self, tracker):
            self.tracker = tracker

        def new_order(self, **_params):
            with self.tracker["lock"]:
                self.tracker["active"] += 1
                self.tracker["max_active"] = max(
                    self.tracker["max_active"], self.tracker["active"]
                )
            time.sleep(0.02)
            with self.tracker["lock"]:
                self.tracker["active"] -= 1
            return {"status": "NEW"}

    tracker = {"lock": threading.Lock(), "active": 0, "max_active": 0}
    fallback = BlockingRestOrderClient(tracker)
    gateway = BinanceUsdMOrderGateway(
        rest_order_client=fallback,
        rest_buy_order_client=BlockingRestOrderClient(tracker),
        rest_sell_order_client=BlockingRestOrderClient(tracker),
    )
    threads = [
        threading.Thread(
            target=gateway.new_order,
            kwargs={"symbol": "BTCUSDC", "side": side},
        )
        for side in ("BUY", "SELL")
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert tracker["max_active"] == 1


@pytest.mark.parametrize("async_enabled", [False, True])
def test_gateway_without_cross_side_arm_preserves_one_legacy_rest_session(
    async_enabled,
):
    calls = []

    class Client:
        def __init__(self, name):
            self.name = name

        def new_order(self, **params):
            calls.append((self.name, params["side"]))
            return {"status": "NEW"}

    legacy = Client("legacy")
    gateway = BinanceUsdMOrderGateway(
        rest_order_client=legacy,
        rest_buy_order_client=Client("buy"),
        rest_sell_order_client=Client("sell"),
        async_order_lanes_enabled=async_enabled,
    )
    submit = gateway.new_order_async if async_enabled else gateway.new_order
    buy = submit(symbol="BTCUSDC", side="BUY")
    sell = submit(symbol="BTCUSDC", side="SELL")
    if async_enabled:
        assert buy.result(timeout=1.0)["status"] == "NEW"
        assert sell.result(timeout=1.0)["status"] == "NEW"
    assert calls == [("legacy", "BUY"), ("legacy", "SELL")]
    gateway.close()


def test_gateway_serializes_same_side_but_allows_cross_side_writes_when_enabled():
    class BlockingRestOrderClient:
        def __init__(self, tracker):
            self.tracker = tracker

        def new_order(self, **_params):
            with self.tracker["lock"]:
                self.tracker["active"] += 1
                self.tracker["max_active"] = max(
                    self.tracker["max_active"], self.tracker["active"]
                )
            time.sleep(0.02)
            with self.tracker["lock"]:
                self.tracker["active"] -= 1
            return {"status": "NEW"}

        cancel_order = new_order
        cancel_open_orders = new_order

    tracker = {
        "lock": threading.Lock(),
        "active": 0,
        "max_active": 0,
    }
    fallback = BlockingRestOrderClient(tracker)
    gateway = BinanceUsdMOrderGateway(
        rest_order_client=fallback,
        rest_buy_order_client=BlockingRestOrderClient(tracker),
        rest_sell_order_client=BlockingRestOrderClient(tracker),
        rest_safety_order_client=BlockingRestOrderClient(tracker),
        async_order_lanes_enabled=True,
        cross_side_order_lanes_enabled=True,
    )
    threads = [
        threading.Thread(
            target=gateway.new_order,
            kwargs={"symbol": "BTCUSDC", "side": "BUY"},
        ),
        threading.Thread(
            target=gateway.cancel_order,
            kwargs={
                "symbol": "BTCUSDC",
                "_narrowgate_order_side": "BUY",
            },
        ),
        threading.Thread(
            target=gateway.new_order,
            kwargs={"symbol": "BTCUSDC", "side": "SELL"},
        ),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    # The two BUY writes are strict FIFO, while the independent SELL lane can
    # overlap one of them.
    assert tracker["max_active"] == 2
    gateway.close()


def test_cancel_all_is_exclusive_against_both_side_lanes():
    release = threading.Event()
    both_started = threading.Event()
    lock = threading.Lock()
    active_sides = 0
    safety_called = threading.Event()

    class SideClient:
        def new_order(self, **_params):
            nonlocal active_sides
            with lock:
                active_sides += 1
                if active_sides == 2:
                    both_started.set()
            assert release.wait(1.0)
            with lock:
                active_sides -= 1
            return {"status": "NEW"}

    class SafetyClient:
        def cancel_open_orders(self, **_params):
            with lock:
                assert active_sides == 0
            safety_called.set()
            return []

    fallback = SafetyClient()
    gateway = BinanceUsdMOrderGateway(
        rest_order_client=fallback,
        rest_buy_order_client=SideClient(),
        rest_sell_order_client=SideClient(),
        rest_safety_order_client=fallback,
        async_order_lanes_enabled=True,
        cross_side_order_lanes_enabled=True,
    )
    buy = threading.Thread(
        target=gateway.new_order,
        kwargs={"symbol": "BTCUSDC", "side": "BUY"},
    )
    sell = threading.Thread(
        target=gateway.new_order,
        kwargs={"symbol": "BTCUSDC", "side": "SELL"},
    )
    buy.start()
    sell.start()
    assert both_started.wait(1.0)
    barrier = threading.Thread(
        target=gateway.cancel_open_orders,
        kwargs={"symbol": "BTCUSDC"},
    )
    barrier.start()
    time.sleep(0.01)
    assert not safety_called.is_set()
    release.set()
    buy.join()
    sell.join()
    barrier.join()
    assert safety_called.is_set()
    gateway.close()


def test_cancel_all_drains_admitted_fifo_and_rejects_barrier_arrivals():
    first_started = threading.Event()
    release_first = threading.Event()
    barrier_started = threading.Event()
    calls = []

    class BuyClient:
        def new_order(self, **params):
            client_id = params["newClientOrderId"]
            calls.append(client_id)
            if client_id == "buy-1":
                first_started.set()
                assert release_first.wait(1.0)
            return {"status": "NEW"}

    class SafetyClient:
        def cancel_open_orders(self, **_params):
            calls.append("cancel-all")
            return []

    buy_client = BuyClient()
    safety = SafetyClient()
    gateway = BinanceUsdMOrderGateway(
        rest_order_client=safety,
        rest_buy_order_client=buy_client,
        rest_sell_order_client=buy_client,
        rest_safety_order_client=safety,
        async_order_lanes_enabled=True,
        cross_side_order_lanes_enabled=True,
        async_order_lane_capacity=1,
    )
    first = gateway.new_order_async(
        symbol="BTCUSDC", side="BUY", newClientOrderId="buy-1"
    )
    assert first_started.wait(1.0)
    second = gateway.new_order_async(
        symbol="BTCUSDC", side="BUY", newClientOrderId="buy-2"
    )

    def cancel_all() -> None:
        barrier_started.set()
        gateway.cancel_open_orders(symbol="BTCUSDC")

    barrier = threading.Thread(target=cancel_all)
    barrier.start()
    assert barrier_started.wait(1.0)
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        with gateway._write_admission_state_lock:
            if gateway._write_admission_barrier_active:
                break
        time.sleep(0.001)
    else:
        raise AssertionError("cancel-all did not establish its admission barrier")

    with pytest.raises(BinanceUsdMOrderAdmissionRejected) as rejected:
        gateway.new_order_async(
            symbol="BTCUSDC", side="BUY", newClientOrderId="buy-after-barrier"
        )
    assert rejected.value.may_have_been_dispatched is False
    release_first.set()
    assert first.result(timeout=1.0)["status"] == "NEW"
    assert second.result(timeout=1.0)["status"] == "NEW"
    barrier.join(timeout=1.0)
    assert not barrier.is_alive()
    assert calls == ["buy-1", "buy-2", "cancel-all"]
    gateway.close()


def test_pre_barrier_ticket_waiting_outside_admission_is_rejected():
    calls = []

    class Client:
        def new_order(self, **_params):
            calls.append("new")
            return {"status": "NEW"}

        def cancel_open_orders(self, **_params):
            calls.append("cancel-all")
            return []

    client = Client()
    gateway = BinanceUsdMOrderGateway(rest_order_client=client)
    ticket_captured = threading.Event()
    original_capture = gateway._capture_write_admission_ticket

    def capture_ticket(**kwargs):
        ticket = original_capture(**kwargs)
        ticket_captured.set()
        return ticket

    gateway._capture_write_admission_ticket = capture_ticket
    failure = []

    def submit_waiting_write() -> None:
        try:
            gateway.new_order(symbol="BTCUSDC", side="BUY")
        except BaseException as exc:
            failure.append(exc)

    with gateway._write_admission_lock:
        writer = threading.Thread(target=submit_waiting_write)
        writer.start()
        assert ticket_captured.wait(1.0)
        gateway.cancel_open_orders(symbol="BTCUSDC")
    writer.join(timeout=1.0)
    assert not writer.is_alive()
    assert len(failure) == 1
    assert isinstance(failure[0], BinanceUsdMOrderAdmissionRejected)
    assert failure[0].may_have_been_dispatched is False
    assert calls == ["cancel-all"]
    gateway.close()


def test_async_order_lanes_are_bounded_fifo_without_cross_side_hol():
    buy_release = threading.Event()
    buy_started = threading.Event()
    sell_completed = threading.Event()
    buy_calls = []

    class BuyClient:
        def new_order(self, **params):
            buy_calls.append(params["newClientOrderId"])
            if len(buy_calls) == 1:
                buy_started.set()
                assert buy_release.wait(1.0)
            return {"status": "NEW", "cid": params["newClientOrderId"]}

    class SellClient:
        def new_order(self, **params):
            sell_completed.set()
            return {"status": "NEW", "cid": params["newClientOrderId"]}

    class SafetyClient:
        def cancel_open_orders(self, **_params):
            return []

    safety = SafetyClient()
    gateway = BinanceUsdMOrderGateway(
        rest_order_client=safety,
        rest_buy_order_client=BuyClient(),
        rest_sell_order_client=SellClient(),
        rest_safety_order_client=safety,
        async_order_lanes_enabled=True,
        cross_side_order_lanes_enabled=True,
        async_order_lane_capacity=1,
    )
    first = gateway.new_order_async(
        symbol="BTCUSDC", side="BUY", newClientOrderId="buy-1"
    )
    assert buy_started.wait(1.0)
    second = gateway.new_order_async(
        symbol="BTCUSDC", side="BUY", newClientOrderId="buy-2"
    )
    with pytest.raises(BinanceUsdMOrderLaneFull):
        gateway.new_order_async(
            symbol="BTCUSDC", side="BUY", newClientOrderId="buy-3"
        )
    sell = gateway.new_order_async(
        symbol="BTCUSDC", side="SELL", newClientOrderId="sell-1"
    )
    assert sell_completed.wait(1.0)
    assert sell.result(timeout=1.0)["cid"] == "sell-1"
    assert not first.done()
    buy_release.set()
    assert first.result(timeout=1.0)["cid"] == "buy-1"
    assert second.result(timeout=1.0)["cid"] == "buy-2"
    assert buy_calls == ["buy-1", "buy-2"]
    health = gateway.health_snapshot()["async_order_lanes"]
    assert health["BUY"]["submitted"] == 2
    assert health["BUY"]["queue_high_watermark"] == 1
    gateway.close()


@pytest.mark.parametrize("transport", ["rest", "websocket_api"])
def test_async_response_isolation_preserves_global_cross_side_fifo_by_default(transport):
    release_buy = threading.Event()
    buy_started = threading.Event()
    calls = []

    class Client:
        def new_order(self, **params):
            calls.append(params["newClientOrderId"])
            if params["side"] == "BUY":
                buy_started.set()
                assert release_buy.wait(1.0)
            return {"status": "NEW"}

    client = Client()
    websocket = None
    if transport == "websocket_api":
        def respond(request):
            return {
                "id": request["id"],
                "status": 200,
                "result": client.new_order(**request["params"]),
            }

        websocket = create_binance_usdm_websocket_order_gateway(
            key="key",
            secret="secret",
            config=_enabled_config(),
            connection_factory=_ConnectionFactory([_FakeConnection(respond)]),
        )
    gateway = BinanceUsdMOrderGateway(
        rest_order_client=client,
        websocket_order_gateway=websocket,
        async_order_lanes_enabled=True,
        cross_side_order_lanes_enabled=False,
    )
    try:
        buy = gateway.new_order_async(
            symbol="BTCUSDC", side="BUY", newClientOrderId="buy-first"
        )
        assert buy_started.wait(1.0)
        sell = gateway.new_order_async(
            symbol="BTCUSDC", side="SELL", newClientOrderId="sell-second"
        )
        # The caller regains control while the first response is blocked.
        # Changing protocol must not silently add another in-flight write.
        assert not buy.done()
        assert not sell.done()
        time.sleep(0.01)
        assert calls == ["buy-first"]
        release_buy.set()
        assert buy.result(timeout=1.0)["status"] == "NEW"
        assert sell.result(timeout=1.0)["status"] == "NEW"
        assert calls == ["buy-first", "sell-second"]
        assert set(gateway.health_snapshot()["async_order_lanes"]) == {"GLOBAL"}
    finally:
        release_buy.set()
        gateway.close()


def test_cross_side_lanes_require_async_response_isolation():
    with pytest.raises(ValueError, match="require asynchronous"):
        BinanceUsdMOrderGateway(
            rest_order_client=object(),
            cross_side_order_lanes_enabled=True,
        )


def test_prebound_completion_never_runs_on_admitting_thread():
    callback_thread = []
    callback_done = threading.Event()

    class Client:
        def new_order(self, **_params):
            return {"status": "NEW"}

    client = Client()
    gateway = BinanceUsdMOrderGateway(
        rest_order_client=client,
        async_order_lanes_enabled=True,
    )
    caller_ident = threading.get_ident()

    def completed(_future):
        callback_thread.append(threading.get_ident())
        callback_done.set()

    future = gateway.new_order_async(
        symbol="BTCUSDC",
        side="BUY",
        newClientOrderId="prebound",
        _narrowgate_done_callback=completed,
    )
    assert future.result(timeout=1.0)["status"] == "NEW"
    assert callback_done.wait(1.0)
    assert callback_thread != [caller_ident]
    gateway.close()


def test_cross_side_completion_dispatchers_do_not_head_of_line_block():
    release_buy_callback = threading.Event()
    buy_callback_started = threading.Event()
    sell_callback_completed = threading.Event()

    class Client:
        def new_order(self, **_params):
            return {"status": "NEW"}

    client = Client()
    gateway = BinanceUsdMOrderGateway(
        rest_order_client=client,
        async_order_lanes_enabled=True,
        cross_side_order_lanes_enabled=True,
    )

    def buy_callback(_future):
        buy_callback_started.set()
        assert release_buy_callback.wait(1.0)

    def sell_callback(_future):
        sell_callback_completed.set()

    buy = gateway.new_order_async(
        symbol="BTCUSDC",
        side="BUY",
        newClientOrderId="buy-blocking-callback",
        _narrowgate_done_callback=buy_callback,
    )
    assert buy_callback_started.wait(1.0)
    sell = gateway.new_order_async(
        symbol="BTCUSDC",
        side="SELL",
        newClientOrderId="sell-independent-callback",
        _narrowgate_done_callback=sell_callback,
    )
    assert sell_callback_completed.wait(1.0)
    assert sell.result(timeout=1.0)["status"] == "NEW"
    release_buy_callback.set()
    assert buy.result(timeout=1.0)["status"] == "NEW"
    gateway.close()


@pytest.mark.parametrize("response", [None, {}, {"orderId": 42}])
def test_rest_order_response_without_status_is_unknown(response):
    class Client:
        def new_order(self, **_params):
            return response

    gateway = BinanceUsdMOrderGateway(rest_order_client=Client())
    with pytest.raises(BinanceUsdMOrderProtocolUnknown) as caught:
        gateway.new_order(symbol="BTCUSDC", side="BUY")
    assert caught.value.may_have_been_dispatched is True
    assert caught.value.requires_reconciliation is True
    gateway.close()


def test_rest_cancel_response_without_status_is_unknown():
    class Client:
        def cancel_order(self, **_params):
            return {"orderId": 42}

    gateway = BinanceUsdMOrderGateway(rest_order_client=Client())
    with pytest.raises(BinanceUsdMOrderProtocolUnknown):
        gateway.cancel_order(
            symbol="BTCUSDC",
            origClientOrderId="unknown-cancel",
            _narrowgate_order_side="SELL",
        )
    gateway.close()


def test_async_order_lane_experiment_has_bounded_runtime():
    calls = []

    class Client:
        def new_order(self, **params):
            calls.append(("new", params))
            return {"status": "NEW"}

        def cancel_order(self, **params):
            calls.append(("cancel", params))
            return {"status": "CANCELED"}

        def cancel_open_orders(self, **params):
            calls.append(("cancel-all", params))
            return []

    client = Client()
    gateway = BinanceUsdMOrderGateway(
        rest_order_client=client,
        async_order_lanes_enabled=True,
        async_order_lane_max_runtime_s=1.0,
    )
    gateway._async_order_lane_deadline_monotonic -= 2.0
    with pytest.raises(BinanceUsdMOrderAdmissionRejected, match="expired"):
        gateway.new_order_async(symbol="BTCUSDC", side="BUY")
    with pytest.raises(BinanceUsdMOrderAdmissionRejected, match="expired"):
        gateway.new_order(symbol="BTCUSDC", side="SELL")

    # Expiry is an experiment risk limit, not a kill switch for safety writes.
    assert gateway.cancel_order(
        symbol="BTCUSDC",
        origClientOrderId="owned-buy",
        _narrowgate_order_side="BUY",
    )["status"] == "CANCELED"
    assert gateway.cancel_order_async(
        symbol="BTCUSDC",
        origClientOrderId="owned-sell",
        _narrowgate_order_side="SELL",
    ).result(timeout=1.0)["status"] == "CANCELED"
    assert gateway.cancel_open_orders(symbol="BTCUSDC") == []
    assert gateway.new_order(
        symbol="BTCUSDC",
        side="BUY",
        reduceOnly=True,
    )["status"] == "NEW"
    assert gateway.new_order_async(
        symbol="BTCUSDC",
        side="SELL",
        reduceOnly="true",
    ).result(timeout=1.0)["status"] == "NEW"
    assert gateway.health_snapshot()["async_order_lane_runtime_expired"] is True
    assert [kind for kind, _params in calls] == [
        "cancel",
        "cancel",
        "cancel-all",
        "new",
        "new",
    ]
    gateway.close()


def test_sync_side_write_cannot_overtake_admitted_async_write():
    first_started = threading.Event()
    release_first = threading.Event()
    calls = []

    class BuyClient:
        def new_order(self, **params):
            calls.append(params["newClientOrderId"])
            if params["newClientOrderId"] == "first":
                first_started.set()
                assert release_first.wait(1.0)
            return {"status": "NEW"}

    client = BuyClient()
    gateway = BinanceUsdMOrderGateway(
        rest_order_client=client,
        rest_buy_order_client=client,
        rest_sell_order_client=client,
        rest_safety_order_client=client,
        async_order_lanes_enabled=True,
    )
    first = gateway.new_order_async(
        symbol="BTCUSDC",
        side="BUY",
        newClientOrderId="first",
    )
    assert first_started.wait(1.0)
    sync_done = threading.Event()

    def submit_sync() -> None:
        gateway.new_order(
            symbol="BTCUSDC",
            side="BUY",
            newClientOrderId="second",
        )
        sync_done.set()

    thread = threading.Thread(target=submit_sync)
    thread.start()
    time.sleep(0.01)
    assert calls == ["first"]
    assert not sync_done.is_set()
    release_first.set()
    assert first.result(timeout=1.0) == {"status": "NEW"}
    thread.join(timeout=1.0)
    assert sync_done.is_set()
    assert calls == ["first", "second"]
    gateway.close()


def test_async_cancel_uses_original_order_side_lane():
    calls = []

    class Client:
        def new_order(self, **params):
            calls.append(("new", params["newClientOrderId"]))
            return {"status": "NEW"}

        def cancel_order(self, **params):
            calls.append(("cancel", params["origClientOrderId"]))
            return {"status": "CANCELED"}

        def cancel_open_orders(self, **_params):
            return []

    client = Client()
    gateway = BinanceUsdMOrderGateway(
        rest_order_client=client,
        rest_buy_order_client=client,
        rest_sell_order_client=client,
        rest_safety_order_client=client,
        async_order_lanes_enabled=True,
    )
    assert gateway.new_order_async(
        symbol="BTCUSDC", side="BUY", newClientOrderId="buy-owned"
    ).result(timeout=1.0)["status"] == "NEW"
    assert gateway.cancel_order_async(
        symbol="BTCUSDC", origClientOrderId="buy-owned"
    ).result(timeout=1.0)["status"] == "CANCELED"
    assert calls == [("new", "buy-owned"), ("cancel", "buy-owned")]
    gateway.close()


def test_async_completion_callback_can_enter_cancel_all_without_self_deadlock():
    request_started = threading.Event()
    release_request = threading.Event()
    cancel_all_completed = threading.Event()

    class Client:
        def new_order(self, **_params):
            request_started.set()
            assert release_request.wait(1.0)
            return {"status": "NEW"}

        def cancel_open_orders(self, **_params):
            cancel_all_completed.set()
            return []

    client = Client()
    gateway = BinanceUsdMOrderGateway(
        rest_order_client=client,
        rest_buy_order_client=client,
        rest_sell_order_client=client,
        rest_safety_order_client=client,
        async_order_lanes_enabled=True,
    )
    future = gateway.new_order_async(
        symbol="BTCUSDC",
        side="BUY",
        newClientOrderId="callback-cancel-all",
    )
    assert request_started.wait(1.0)
    future.add_done_callback(
        lambda _completed: gateway.cancel_open_orders(symbol="BTCUSDC")
    )

    release_request.set()
    assert cancel_all_completed.wait(1.0)
    assert future.result(timeout=1.0)["status"] == "NEW"
    gateway.close()


def test_async_completion_drain_waits_until_callback_returns():
    callback_started = threading.Event()
    release_callback = threading.Event()
    callback_completed = threading.Event()
    drain_completed = threading.Event()
    drain_errors = []

    class Client:
        def new_order(self, **_params):
            return {"status": "NEW"}

        def cancel_open_orders(self, **_params):
            return []

    def callback(_completed) -> None:
        callback_started.set()
        assert release_callback.wait(1.0)
        callback_completed.set()

    client = Client()
    gateway = BinanceUsdMOrderGateway(
        rest_order_client=client,
        async_order_lanes_enabled=True,
    )
    future = gateway.new_order_async(
        symbol="BTCUSDC",
        side="BUY",
        _narrowgate_done_callback=callback,
    )
    assert callback_started.wait(1.0)

    def drain() -> None:
        try:
            gateway.drain_async_order_completions(timeout_s=1.0)
        except BaseException as exc:
            drain_errors.append(exc)
        finally:
            drain_completed.set()

    drain_thread = threading.Thread(target=drain)
    drain_thread.start()
    time.sleep(0.01)
    assert not drain_completed.is_set()
    release_callback.set()
    drain_thread.join(timeout=1.0)

    assert callback_completed.is_set()
    assert drain_completed.is_set()
    assert drain_errors == []
    assert future.result(timeout=1.0)["status"] == "NEW"
    gateway.close()


def test_async_completion_callback_cannot_wait_for_its_own_delivery():
    callback_completed = threading.Event()
    callback_errors = []

    class Client:
        def new_order(self, **_params):
            return {"status": "NEW"}

    client = Client()
    gateway = BinanceUsdMOrderGateway(
        rest_order_client=client,
        async_order_lanes_enabled=True,
    )

    def callback(_completed) -> None:
        try:
            gateway.drain_async_order_completions(timeout_s=0.1)
        except BaseException as exc:
            callback_errors.append(exc)
        finally:
            callback_completed.set()

    future = gateway.new_order_async(
        symbol="BTCUSDC",
        side="BUY",
        _narrowgate_done_callback=callback,
    )
    assert callback_completed.wait(1.0)
    assert future.result(timeout=1.0)["status"] == "NEW"
    assert len(callback_errors) == 1
    assert isinstance(callback_errors[0], RuntimeError)
    assert "completion callback" in str(callback_errors[0])
    gateway.drain_async_order_completions(timeout_s=1.0)
    gateway.close()


def test_callback_and_shutdown_cancel_all_attempts_are_serialized_not_dropped():
    first_cancel_started = threading.Event()
    release_first_cancel = threading.Event()
    callback_completed = threading.Event()
    shutdown_completed = threading.Event()
    callback_errors = []
    shutdown_errors = []
    call_lock = threading.Lock()
    cancel_calls = 0

    class Client:
        def new_order(self, **_params):
            return {"status": "NEW"}

        def cancel_open_orders(self, **_params):
            nonlocal cancel_calls
            with call_lock:
                cancel_calls += 1
                call_index = cancel_calls
            if call_index == 1:
                first_cancel_started.set()
                assert release_first_cancel.wait(1.0)
            return []

    client = Client()
    gateway = BinanceUsdMOrderGateway(
        rest_order_client=client,
        async_order_lanes_enabled=True,
    )

    def callback(_completed) -> None:
        try:
            gateway.cancel_open_orders(symbol="BTCUSDC")
        except BaseException as exc:
            callback_errors.append(exc)
        finally:
            callback_completed.set()

    gateway.new_order_async(
        symbol="BTCUSDC",
        side="BUY",
        _narrowgate_done_callback=callback,
    )
    assert first_cancel_started.wait(1.0)

    def shutdown_cancel() -> None:
        try:
            gateway.cancel_open_orders(symbol="BTCUSDC")
        except BaseException as exc:
            shutdown_errors.append(exc)
        finally:
            shutdown_completed.set()

    shutdown_thread = threading.Thread(target=shutdown_cancel)
    shutdown_thread.start()
    time.sleep(0.01)
    assert not shutdown_completed.is_set()
    release_first_cancel.set()
    shutdown_thread.join(timeout=1.0)

    assert callback_completed.wait(1.0)
    assert shutdown_completed.is_set()
    assert callback_errors == []
    assert shutdown_errors == []
    assert cancel_calls == 2
    gateway.drain_async_order_completions(timeout_s=1.0)
    gateway.close()


def test_async_callback_cancel_all_drains_later_same_side_write_without_deadlock():
    first_started = threading.Event()
    release_first = threading.Event()
    callback_completed = threading.Event()
    calls = []
    callback_errors = []

    class Client:
        def new_order(self, **params):
            client_id = params["newClientOrderId"]
            calls.append(client_id)
            if client_id == "buy-1":
                first_started.set()
                assert release_first.wait(1.0)
            return {"status": "NEW"}

        def cancel_open_orders(self, **_params):
            calls.append("cancel-all")
            return []

    client = Client()
    gateway = BinanceUsdMOrderGateway(
        rest_order_client=client,
        rest_buy_order_client=client,
        rest_sell_order_client=client,
        rest_safety_order_client=client,
        async_order_lanes_enabled=True,
        async_order_lane_capacity=1,
    )
    first = gateway.new_order_async(
        symbol="BTCUSDC", side="BUY", newClientOrderId="buy-1"
    )
    assert first_started.wait(1.0)
    second = gateway.new_order_async(
        symbol="BTCUSDC", side="BUY", newClientOrderId="buy-2"
    )

    def callback(_completed) -> None:
        try:
            gateway.cancel_open_orders(symbol="BTCUSDC")
        except BaseException as exc:
            callback_errors.append(exc)
        finally:
            callback_completed.set()

    first.add_done_callback(callback)
    release_first.set()
    assert callback_completed.wait(1.0)
    assert not callback_errors
    assert first.result(timeout=1.0)["status"] == "NEW"
    assert second.result(timeout=1.0)["status"] == "NEW"
    assert calls == ["buy-1", "buy-2", "cancel-all"]
    gateway.close()


def test_constructor_failure_closes_started_lanes_and_owned_websocket_gateway():
    prior_threads = set(threading.enumerate())

    class Client:
        def cancel_open_orders(self, **_params):
            return []

    class WebSocketGateway:
        def __init__(self):
            self.closed = False

        def set_request_correlation_sink(self, _sink):
            raise RuntimeError("correlation sink rejected")

        def close(self):
            self.closed = True

    websocket_gateway = WebSocketGateway()
    with pytest.raises(RuntimeError, match="correlation sink rejected"):
        BinanceUsdMOrderGateway(
            rest_order_client=Client(),
            websocket_order_gateway=websocket_gateway,
            async_order_lanes_enabled=True,
        )

    assert websocket_gateway.closed is True
    leaked = [
        thread
        for thread in threading.enumerate()
        if thread not in prior_threads
        and thread.name.startswith("ng-order-")
        and thread.is_alive()
    ]
    assert leaked == []


@pytest.mark.parametrize("async_enabled", [False, True])
def test_gateway_rejects_every_write_admission_after_close(async_enabled):
    calls = []

    class Client:
        def new_order(self, **_params):
            calls.append("new")
            return {"status": "NEW"}

        def cancel_order(self, **_params):
            calls.append("cancel")
            return {"status": "CANCELED"}

        def cancel_open_orders(self, **_params):
            calls.append("cancel-all")
            return []

    client = Client()
    gateway = BinanceUsdMOrderGateway(
        rest_order_client=client,
        rest_buy_order_client=client,
        rest_sell_order_client=client,
        rest_safety_order_client=client,
        async_order_lanes_enabled=async_enabled,
    )
    gateway.close()

    writes = (
        lambda: gateway.new_order(symbol="BTCUSDC", side="BUY"),
        lambda: gateway.cancel_order(
            symbol="BTCUSDC", _narrowgate_order_side="BUY"
        ),
        lambda: gateway.cancel_open_orders(symbol="BTCUSDC"),
    )
    for write in writes:
        with pytest.raises(BinanceUsdMOrderAdmissionRejected) as rejected:
            write()
        assert rejected.value.may_have_been_dispatched is False
    if async_enabled:
        with pytest.raises(BinanceUsdMOrderAdmissionRejected):
            gateway.new_order_async(symbol="BTCUSDC", side="BUY")
        with pytest.raises(BinanceUsdMOrderAdmissionRejected):
            gateway.cancel_order_async(
                symbol="BTCUSDC", _narrowgate_order_side="BUY"
            )
    assert calls == []


def test_close_has_one_bounded_deadline_when_full_lane_worker_is_stuck():
    first_started = threading.Event()
    release_first = threading.Event()

    class Client:
        def new_order(self, **_params):
            if not first_started.is_set():
                first_started.set()
                assert release_first.wait(1.0)
            return {"status": "NEW"}

        def cancel_open_orders(self, **_params):
            return []

    client = Client()
    gateway = BinanceUsdMOrderGateway(
        rest_order_client=client,
        rest_buy_order_client=client,
        rest_sell_order_client=client,
        rest_safety_order_client=client,
        async_order_lanes_enabled=True,
        async_order_lane_capacity=1,
        async_order_lane_drain_timeout_s=0.05,
    )
    first = gateway.new_order_async(symbol="BTCUSDC", side="BUY")
    assert first_started.wait(1.0)
    second = gateway.new_order_async(symbol="BTCUSDC", side="BUY")

    started = time.monotonic()
    with pytest.raises((RuntimeError, TimeoutError)):
        gateway.close()
    elapsed = time.monotonic() - started
    assert elapsed < 0.20
    with pytest.raises(BinanceUsdMOrderAdmissionRejected):
        gateway.new_order(symbol="BTCUSDC", side="SELL")

    release_first.set()
    assert first.result(timeout=1.0)["status"] == "NEW"
    assert second.result(timeout=1.0)["status"] == "NEW"
    gateway.close()


def test_authoritative_exchange_error_preserves_code_without_reconnect():
    def responder(request):
        return {
            "id": request["id"],
            "status": 400,
            "error": {"code": -5022, "msg": "Post Only order will be rejected"},
        }

    connection = _FakeConnection(responder)
    factory = _ConnectionFactory([connection])
    gateway = create_binance_usdm_websocket_order_gateway(
        key="key",
        secret="secret",
        config=_enabled_config(),
        connection_factory=factory,
        request_id_factory=lambda: "reject-1",
    )
    assert gateway is not None

    with pytest.raises(BinanceUsdMWebSocketApiError) as caught:
        gateway.new_order(symbol="BTCUSDC", side="BUY", type="LIMIT")
    assert caught.value.error_code == -5022
    assert caught.value.exchange_response_authoritative is True
    assert connection.connected is True
    assert len(connection.sent) == 1
    assert gateway.health_snapshot()["counters"]["authoritative_errors"] == 1


def test_timeout_after_send_is_unknown_and_never_retries_same_request():
    timed_out = _FakeConnection(lambda _request: TimeoutError("response timed out"))

    def succeeding_response(request):
        return {
            "id": request["id"],
            "status": 200,
            "result": {"orderId": 20, "status": "CANCELED"},
        }

    recovered = _FakeConnection(succeeding_response)
    factory = _ConnectionFactory([timed_out, recovered])
    ids = iter(("unknown-1", "new-request-2"))
    gateway = create_binance_usdm_websocket_order_gateway(
        key="key",
        secret="secret",
        config=_enabled_config(),
        connection_factory=factory,
        request_id_factory=lambda: next(ids),
    )
    assert gateway is not None

    with pytest.raises(BinanceUsdMWebSocketOrderUnknown) as caught:
        gateway.cancel_order(symbol="BTCUSDC", origClientOrderId="ng-1")
    assert caught.value.request_id == "unknown-1"
    assert caught.value.may_have_been_dispatched is True
    assert caught.value.requires_reconciliation is True
    assert len(timed_out.sent) == 1
    assert timed_out.connected is False
    assert not recovered.sent

    # A later independent action may reconnect.  The UNKNOWN action itself was
    # not retransmitted and its request ID is never reused.
    result = gateway.cancel_order(symbol="BTCUSDC", origClientOrderId="ng-2")
    assert result["status"] == "CANCELED"
    assert len(timed_out.sent) == 1
    assert len(recovered.sent) == 1
    assert json.loads(recovered.sent[0])["id"] == "new-request-2"
    health = gateway.health_snapshot()
    assert health["counters"]["timeouts"] == 1
    assert health["counters"]["successes"] == 1
    assert health["counters"]["connect_attempts"] == 2


def test_mismatched_response_identity_is_unknown_and_socket_is_discarded():
    connection = _FakeConnection(
        lambda _request: {"id": "some-other-request", "status": 200, "result": {}}
    )
    gateway = create_binance_usdm_websocket_order_gateway(
        key="key",
        secret="secret",
        config=_enabled_config(),
        connection_factory=_ConnectionFactory([connection]),
        request_id_factory=lambda: "expected-request",
    )
    assert gateway is not None

    with pytest.raises(BinanceUsdMWebSocketOrderUnknown, match="does not match"):
        gateway.cancel_order(symbol="BTCUSDC", origClientOrderId="ng-1")
    assert connection.connected is False
    assert gateway.health_snapshot()["counters"]["protocol_errors"] == 1
