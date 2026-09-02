import hashlib
import hmac
import json
import threading
import time

import pytest
import requests

from live.binance_usdm_transport import (
    BinanceUsdMOrderGateway,
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
        assert len({id(session) for session in sessions}) == 5
        assert clients.identity() == {
            "schema_version": "narrowgate.binance_usdm_rest_roles.v1",
            "roles": (
                "order",
                "reconciliation",
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

    assert gateway.new_order(symbol="BTCUSDC") == {"transport": "websocket"}
    assert gateway.cancel_order(symbol="BTCUSDC") == {"transport": "websocket"}
    assert gateway.cancel_open_orders(symbol="BTCUSDC") == []
    assert [call[0] for call in websocket.calls] == ["new", "cancel"]
    assert [call[0] for call in rest.calls] == ["cancel_all"]
    assert gateway.health_snapshot()["cancel_all_transport"] == "rest"


def test_hybrid_gateway_serializes_all_order_writes():
    class BlockingRestOrderClient:
        def __init__(self):
            self.lock = threading.Lock()
            self.active = 0
            self.max_active = 0

        def new_order(self, **_params):
            with self.lock:
                self.active += 1
                self.max_active = max(self.max_active, self.active)
            time.sleep(0.02)
            with self.lock:
                self.active -= 1
            return {"status": "NEW"}

        cancel_order = new_order
        cancel_open_orders = new_order

    rest = BlockingRestOrderClient()
    gateway = BinanceUsdMOrderGateway(rest_order_client=rest)
    threads = [
        threading.Thread(target=gateway.new_order, kwargs={"symbol": "BTCUSDC"}),
        threading.Thread(target=gateway.cancel_order, kwargs={"symbol": "BTCUSDC"}),
        threading.Thread(
            target=gateway.cancel_open_orders,
            kwargs={"symbol": "BTCUSDC"},
        ),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert rest.max_active == 1


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
