"""Isolated Binance USD-M REST sessions and an optional WebSocket order gateway.

The REST client set deliberately gives latency-sensitive order traffic its own
connection pool.  Reconciliation, public snapshots, metrics and listen-key
maintenance cannot occupy that pool.  Requests are never retried by the HTTP
adapter: a write whose response is lost must be reconciled, not replayed.

The WebSocket API gateway implements the small synchronous transport surface
consumed by :class:`strategy.maker_engine.MakerEngine`.  It is disabled by
default and is intended for measured A/B qualification.  There is at most one
in-flight request per gateway.  Once a frame may have been dispatched, any
timeout, disconnect or protocol ambiguity is reported as UNKNOWN and the
request is never sent again automatically.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import threading
import time
import uuid
from collections import Counter, deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any
from urllib.parse import urlsplit

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

USD_M_REST_BASE_URL = "https://fapi.binance.com"
USD_M_REST_TESTNET_BASE_URL = "https://demo-fapi.binance.com"
USD_M_WEBSOCKET_API_URL = "wss://ws-fapi.binance.com/ws-fapi/v1"
USD_M_WEBSOCKET_API_TESTNET_URL = "wss://testnet.binancefuture.com/ws-fapi/v1"
_ALLOWED_WEBSOCKET_API_URLS = frozenset(
    {
        USD_M_WEBSOCKET_API_URL,
        USD_M_WEBSOCKET_API_TESTNET_URL,
    }
)


class BinanceUsdMRestRole(StrEnum):
    """One connection-pool ownership domain."""

    ORDER = "order"
    RECONCILIATION = "reconciliation"
    MARKET_SNAPSHOT = "market_snapshot"
    METRICS = "metrics"
    LISTEN_KEY = "listen_key"


_REST_ROLES = tuple(BinanceUsdMRestRole)
_WEBSOCKET_BOOLEAN_PARAMS = frozenset(
    {
        "closePosition",
        "priceProtect",
        "reduceOnly",
        "returnRateLimits",
    }
)


def binance_usdm_rest_base_url(*, testnet: bool) -> str:
    return USD_M_REST_TESTNET_BASE_URL if testnet else USD_M_REST_BASE_URL


def binance_usdm_websocket_api_url(*, testnet: bool) -> str:
    return USD_M_WEBSOCKET_API_TESTNET_URL if testnet else USD_M_WEBSOCKET_API_URL


def _zero_retry_policy() -> Retry:
    """Return an explicit no-retry policy, including for connection errors."""

    return Retry(
        total=0,
        connect=0,
        read=0,
        redirect=0,
        status=0,
        other=0,
        raise_on_redirect=False,
        raise_on_status=False,
    )


def _configure_isolated_session(client: Any, *, role: BinanceUsdMRestRole) -> None:
    session = getattr(client, "session", None)
    if session is None or not callable(getattr(session, "mount", None)):
        raise TypeError(f"Binance USD-M {role.value} client has no requests session")
    adapter = HTTPAdapter(
        pool_connections=1,
        pool_maxsize=1,
        max_retries=_zero_retry_policy(),
        pool_block=True,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    # Keep the role local.  Adding an application-specific HTTP header to a
    # signed exchange request provides no isolation and can needlessly expand
    # the WAF-visible request surface.
    client._narrowgate_transport_role = role.value


@dataclass(frozen=True)
class BinanceUsdMRestClients:
    """Five independently pooled clients for mutually blocking REST roles."""

    order: Any
    reconciliation: Any
    market_snapshot: Any
    metrics: Any
    listen_key: Any

    def by_role(self, role: BinanceUsdMRestRole | str) -> Any:
        normalized = BinanceUsdMRestRole(role)
        return getattr(self, normalized.value)

    def close(self) -> None:
        """Close every independent session exactly once."""

        seen: set[int] = set()
        for role in _REST_ROLES:
            client = self.by_role(role)
            session = getattr(client, "session", None)
            if session is None or id(session) in seen:
                continue
            seen.add(id(session))
            close = getattr(session, "close", None)
            if callable(close):
                close()

    def identity(self) -> dict[str, object]:
        sessions = {
            role.value: id(getattr(self.by_role(role), "session", None)) for role in _REST_ROLES
        }
        return {
            "schema_version": "narrowgate.binance_usdm_rest_roles.v1",
            "roles": tuple(role.value for role in _REST_ROLES),
            "independent_sessions": len(set(sessions.values())) == len(sessions),
        }


def create_binance_usdm_rest_clients(
    *,
    key: str,
    secret: str,
    base_url: str = USD_M_REST_BASE_URL,
    timeout_s: float,
    client_factory: Callable[..., Any] | None = None,
) -> BinanceUsdMRestClients:
    """Create one persistent, zero-retry HTTP session for each traffic role."""

    if not math.isfinite(float(timeout_s)) or float(timeout_s) <= 0.0:
        raise ValueError("timeout_s must be finite and positive")
    if client_factory is None:
        from binance.um_futures import UMFutures

        client_factory = UMFutures

    clients: dict[str, Any] = {}
    try:
        for role in _REST_ROLES:
            client = client_factory(
                key=key,
                secret=secret,
                base_url=str(base_url),
                timeout=float(timeout_s),
            )
            _configure_isolated_session(client, role=role)
            clients[role.value] = client
    except Exception:
        BinanceUsdMRestClients(
            order=clients.get("order"),
            reconciliation=clients.get("reconciliation"),
            market_snapshot=clients.get("market_snapshot"),
            metrics=clients.get("metrics"),
            listen_key=clients.get("listen_key"),
        ).close()
        raise

    result = BinanceUsdMRestClients(**clients)
    if not bool(result.identity()["independent_sessions"]):
        result.close()
        raise RuntimeError("Binance USD-M REST roles unexpectedly share a session")
    return result


@dataclass(frozen=True)
class BinanceUsdMWebSocketOrderConfig:
    """Restart-only settings for the optional USD-M WebSocket API gateway."""

    enabled: bool = False
    url: str = USD_M_WEBSOCKET_API_URL
    connect_timeout_s: float = 3.0
    request_timeout_s: float = 2.0
    recv_window_ms: int = 5_000
    latency_sample_limit: int = 4_096
    max_runtime_s: float = 900.0

    def __post_init__(self) -> None:
        url = str(self.url).strip()
        parsed = urlsplit(url)
        if (
            url not in _ALLOWED_WEBSOCKET_API_URLS
            or parsed.scheme != "wss"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                "USD-M WebSocket API URL must be an exact official wss:// endpoint"
            )
        for field_name in ("connect_timeout_s", "request_timeout_s"):
            value = float(getattr(self, field_name))
            if not math.isfinite(value) or not 0.05 <= value <= 30.0:
                raise ValueError(f"{field_name} must be finite and in [0.05, 30]")
        if not 1 <= int(self.recv_window_ms) <= 60_000:
            raise ValueError("recv_window_ms must be in [1, 60000]")
        if not 32 <= int(self.latency_sample_limit) <= 1_000_000:
            raise ValueError("latency_sample_limit must be in [32, 1000000]")
        max_runtime_s = float(self.max_runtime_s)
        if not math.isfinite(max_runtime_s) or not 1.0 <= max_runtime_s <= 3_600.0:
            raise ValueError("max_runtime_s must be finite and in [1, 3600]")


class BinanceUsdMWebSocketApiError(RuntimeError):
    """An authoritative error response correlated to one request ID."""

    exchange_response_authoritative = True
    may_have_been_dispatched = True

    def __init__(
        self,
        *,
        request_id: str,
        method: str,
        status_code: int,
        error_code: int | None,
        error_message: str,
    ) -> None:
        super().__init__(
            f"USD-M WebSocket API {method} failed "
            f"status={status_code} code={error_code}: {error_message}"
        )
        self.request_id = request_id
        self.method = method
        self.status_code = int(status_code)
        self.error_code = error_code
        self.code = error_code
        self.error_message = error_message


class BinanceUsdMWebSocketOrderUnknown(TimeoutError):
    """A write may have reached the exchange but has no correlated response."""

    exchange_response_authoritative = False
    may_have_been_dispatched = True
    requires_reconciliation = True

    def __init__(
        self,
        *,
        request_id: str,
        method: str,
        reason: str,
    ) -> None:
        super().__init__(
            f"USD-M WebSocket API {method} response UNKNOWN request_id={request_id}: {reason}"
        )
        self.request_id = request_id
        self.method = method
        self.reason = reason


class BinanceUsdMWebSocketUnavailable(ConnectionError):
    """The persistent socket could not be established before dispatch."""

    exchange_response_authoritative = False
    may_have_been_dispatched = False
    requires_reconciliation = False


class BinanceUsdMWebSocketExperimentExpired(BinanceUsdMWebSocketUnavailable):
    """The bounded A/B interval ended before a new write was dispatched."""


def _percentile(values: list[float], probability: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * probability
    lower = int(math.floor(index))
    upper = int(math.ceil(index))
    if lower == upper:
        return float(ordered[lower])
    fraction = index - lower
    return float(ordered[lower] + (ordered[upper] - ordered[lower]) * fraction)


def _normalize_websocket_param_value(key: str, value: Any) -> Any:
    """Normalize known JSON booleans without coercing numeric/string fields."""

    if key not in _WEBSOCKET_BOOLEAN_PARAMS:
        return value
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized == "true":
            return True
        if normalized == "false":
            return False
    raise ValueError(f"WebSocket API {key} must be boolean or 'true'/'false'")


def _websocket_signature_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


class BinanceUsdMWebSocketOrderGateway:
    """Synchronous, strictly correlated USD-M WebSocket API order transport."""

    transport_name = "binance_usdm_websocket_api"
    automatic_write_retries = 0

    def __init__(
        self,
        *,
        key: str,
        secret: str,
        config: BinanceUsdMWebSocketOrderConfig,
        connection_factory: Callable[..., Any] | None = None,
        request_id_factory: Callable[[], str] | None = None,
        wall_time_ms: Callable[[], int] | None = None,
        monotonic_ns: Callable[[], int] | None = None,
    ) -> None:
        if not config.enabled:
            raise ValueError("USD-M WebSocket order gateway is disabled")
        if not key or not secret:
            raise ValueError("USD-M WebSocket order gateway requires API credentials")
        if connection_factory is None:
            import websocket

            connection_factory = websocket.create_connection
        self._key = str(key)
        self._secret = str(secret).encode("utf-8")
        self.config = config
        self._connection_factory = connection_factory
        self._request_id_factory = request_id_factory or (lambda: uuid.uuid4().hex)
        self._wall_time_ms = wall_time_ms or (lambda: time.time_ns() // 1_000_000)
        self._monotonic_ns = monotonic_ns or time.monotonic_ns
        self._io_lock = threading.Lock()
        self._response_condition = threading.Condition()
        self._health_lock = threading.Lock()
        self._connection = None
        self._closed = False
        self._connection_generation = 0
        self._experiment_started_ns: int | None = None
        self._counters: Counter[str] = Counter()
        self._method_counts: Counter[str] = Counter()
        self._latency_ms: deque[float] = deque(maxlen=int(config.latency_sample_limit))
        self._last_receipt: dict[str, object] | None = None
        self._last_error = ""
        self._pending_request_id: str | None = None
        self._pending_response: Mapping[str, Any] | None = None
        self._reader_failure: tuple[int, str] | None = None
        self._reader_thread: threading.Thread | None = None

    def __enter__(self) -> BinanceUsdMWebSocketOrderGateway:
        self.start()
        return self

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        self.close()

    @property
    def connected(self) -> bool:
        with self._io_lock:
            connection = self._connection
            return bool(connection is not None and getattr(connection, "connected", True))

    def start(self) -> None:
        """Preconnect so the first admitted order does not pay the handshake."""

        with self._io_lock:
            if self._experiment_started_ns is None:
                self._experiment_started_ns = self._monotonic_ns()
            self._ensure_connection_locked()

    def close(self) -> None:
        with self._io_lock:
            self._closed = True
            self._close_connection_locked()

    def _ensure_connection_locked(self) -> Any:
        if self._closed:
            raise BinanceUsdMWebSocketUnavailable("WebSocket order gateway is closed")
        self._ensure_experiment_active_locked()
        connection = self._connection
        if connection is not None and bool(getattr(connection, "connected", True)):
            return connection

        started_ns = self._monotonic_ns()
        with self._health_lock:
            self._counters["connect_attempts"] += 1
        try:
            connection = self._connection_factory(
                self.config.url,
                timeout=float(self.config.connect_timeout_s),
                enable_multithread=True,
            )
            settimeout = getattr(connection, "settimeout", None)
            if callable(settimeout):
                # A persistent reader must remain alive while the strategy is
                # idle so websocket-client can consume Binance ping frames and
                # emit the required pong.  A short socket timeout is only a
                # wake-up for the reader; request deadlines are enforced by
                # the response condition below.
                settimeout(min(1.0, float(self.config.request_timeout_s)))
        except Exception as exc:
            with self._health_lock:
                self._counters["connect_failures"] += 1
                self._last_error = f"{type(exc).__name__}: {exc}"
            raise BinanceUsdMWebSocketUnavailable(
                f"could not connect to USD-M WebSocket API: {exc}"
            ) from exc

        self._connection = connection
        self._connection_generation += 1
        generation = self._connection_generation
        reader = threading.Thread(
            target=self._reader_loop,
            args=(connection, generation),
            name=f"binance-usdm-ws-reader-{generation}",
            daemon=True,
        )
        self._reader_thread = reader
        reader.start()
        with self._health_lock:
            self._counters["connect_successes"] += 1
            self._last_receipt = {
                "outcome": "connected",
                "connection_generation": self._connection_generation,
                "latency_ms": (self._monotonic_ns() - started_ns) / 1_000_000.0,
            }
        return connection

    @staticmethod
    def _is_socket_timeout(exc: BaseException) -> bool:
        return isinstance(exc, TimeoutError) or "timeout" in type(exc).__name__.lower()

    def _reader_loop(self, connection: Any, generation: int) -> None:
        """Continuously receive frames so protocol ping/pong remains live."""

        while True:
            try:
                raw_response = connection.recv()
            except Exception as exc:
                if self._is_socket_timeout(exc):
                    with self._response_condition:
                        if self._closed or generation != self._connection_generation:
                            return
                    continue
                reason = f"{type(exc).__name__}: {exc}"
                with self._response_condition:
                    if generation != self._connection_generation:
                        return
                    self._reader_failure = (generation, reason)
                    self._response_condition.notify_all()
                try:
                    connection.close()
                except Exception:
                    pass
                return

            try:
                if isinstance(raw_response, bytes):
                    raw_response = raw_response.decode("utf-8")
                response = json.loads(raw_response)
                if not isinstance(response, Mapping):
                    raise ValueError("response is not an object")
                response_id = str(response.get("id", ""))
            except Exception as exc:
                reason = f"protocol error: {exc}"
                with self._response_condition:
                    if generation != self._connection_generation:
                        return
                    self._reader_failure = (generation, reason)
                    self._response_condition.notify_all()
                try:
                    connection.close()
                except Exception:
                    pass
                return

            with self._response_condition:
                if generation != self._connection_generation:
                    return
                expected = self._pending_request_id
                if not expected or response_id != expected:
                    self._reader_failure = (
                        generation,
                        f"protocol error: response ID {response_id!r} "
                        f"does not match {expected!r}",
                    )
                else:
                    self._pending_response = dict(response)
                self._response_condition.notify_all()
                if self._reader_failure is not None:
                    try:
                        connection.close()
                    except Exception:
                        pass
                    return

    def _ensure_experiment_active_locked(self) -> None:
        started_ns = self._experiment_started_ns
        if started_ns is None:
            started_ns = self._monotonic_ns()
            self._experiment_started_ns = started_ns
        elapsed_s = max(0.0, (self._monotonic_ns() - started_ns) / 1_000_000_000.0)
        if elapsed_s >= float(self.config.max_runtime_s):
            raise BinanceUsdMWebSocketExperimentExpired(
                "USD-M WebSocket order A/B max runtime elapsed before dispatch"
            )

    def _close_connection_locked(self) -> None:
        connection, self._connection = self._connection, None
        with self._response_condition:
            self._response_condition.notify_all()
        if connection is None:
            return
        try:
            connection.close()
        except Exception:
            pass

    def _signed_params(self, params: Mapping[str, Any]) -> dict[str, Any]:
        reserved = {"apiKey", "timestamp", "recvWindow", "signature"}
        overlap = reserved.intersection(params)
        if overlap:
            raise ValueError(
                "caller cannot provide WebSocket authentication fields: "
                + ", ".join(sorted(overlap))
            )
        signed = {
            key: _normalize_websocket_param_value(key, value)
            for key, value in params.items()
            if value is not None
        }
        signed.update(
            {
                "apiKey": self._key,
                "recvWindow": int(self.config.recv_window_ms),
                "timestamp": int(self._wall_time_ms()),
            }
        )
        # The WebSocket API signs the raw, alphabetically sorted name/value
        # pairs.  Do not URL-encode values: the JSON frame carries the raw
        # values too, and a legal client order ID may contain ':' or '/'.
        signature_payload = "&".join(
            f"{key}={_websocket_signature_value(value)}" for key, value in sorted(signed.items())
        )
        signed["signature"] = hmac.new(
            self._secret,
            signature_payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return signed

    def _record_receipt(
        self,
        *,
        request_id: str,
        method: str,
        started_ns: int,
        outcome: str,
        status_code: int | None = None,
        error: str = "",
        client_order_id: str = "",
    ) -> None:
        latency_ms = max(0.0, (self._monotonic_ns() - started_ns) / 1_000_000.0)
        with self._health_lock:
            self._counters["requests"] += 1
            self._counters[outcome] += 1
            self._method_counts[method] += 1
            self._latency_ms.append(latency_ms)
            self._last_error = error
            self._last_receipt = {
                "request_id": request_id,
                "method": method,
                "outcome": outcome,
                "status_code": status_code,
                "latency_ms": latency_ms,
                "connection_generation": self._connection_generation,
                "client_order_id": client_order_id,
            }

    def _request(self, method: str, params: Mapping[str, Any]) -> Any:
        request_id = str(self._request_id_factory())
        if not request_id:
            raise ValueError("request ID factory returned an empty identity")
        with self._io_lock:
            connection = self._ensure_connection_locked()
            request = {
                "id": request_id,
                "method": method,
                "params": self._signed_params(params),
            }
            serialized = json.dumps(request, separators=(",", ":"), sort_keys=True)
            started_ns = self._monotonic_ns()
            client_order_id = str(
                params.get("newClientOrderId")
                or params.get("origClientOrderId")
                or ""
            )
            try:
                with self._response_condition:
                    self._pending_request_id = request_id
                    self._pending_response = None
                    self._reader_failure = None
                connection.send(serialized)
                deadline = time.monotonic() + float(self.config.request_timeout_s)
                with self._response_condition:
                    while self._pending_response is None and self._reader_failure is None:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0.0:
                            raise TimeoutError("correlated response timed out")
                        self._response_condition.wait(timeout=remaining)
                    if self._reader_failure is not None:
                        _, reason = self._reader_failure
                        raise ConnectionError(reason)
                    response = self._pending_response
            except Exception as exc:
                self._close_connection_locked()
                reason = f"{type(exc).__name__}: {exc}"
                outcome = (
                    "timeouts"
                    if self._is_socket_timeout(exc)
                    else "protocol_errors"
                    if "protocol error:" in reason
                    else "disconnects"
                )
                self._record_receipt(
                    request_id=request_id,
                    method=method,
                    started_ns=started_ns,
                    outcome=outcome,
                    error=reason,
                    client_order_id=client_order_id,
                )
                raise BinanceUsdMWebSocketOrderUnknown(
                    request_id=request_id,
                    method=method,
                    reason=reason,
                ) from exc

            finally:
                with self._response_condition:
                    self._pending_request_id = None
                    self._pending_response = None
                    self._reader_failure = None

            try:
                if not isinstance(response, Mapping):
                    raise ValueError("response is not an object")
                if str(response.get("id", "")) != request_id:
                    raise ValueError(
                        f"response ID {response.get('id')!r} does not match {request_id!r}"
                    )
                status_code = int(response["status"])
            except Exception as exc:
                self._close_connection_locked()
                reason = f"protocol error: {exc}"
                self._record_receipt(
                    request_id=request_id,
                    method=method,
                    started_ns=started_ns,
                    outcome="protocol_errors",
                    error=reason,
                    client_order_id=client_order_id,
                )
                raise BinanceUsdMWebSocketOrderUnknown(
                    request_id=request_id,
                    method=method,
                    reason=reason,
                ) from exc

            if not 200 <= status_code < 300:
                error_payload = response.get("error")
                if not isinstance(error_payload, Mapping):
                    error_payload = {}
                raw_code = error_payload.get("code")
                try:
                    error_code = int(raw_code) if raw_code is not None else None
                except (TypeError, ValueError):
                    error_code = None
                error_message = str(error_payload.get("msg", "exchange rejected request"))
                execution_unknown = status_code in {408, 504} or (
                    status_code == 503 and "unknown error" in error_message.lower()
                )
                if execution_unknown:
                    self._record_receipt(
                        request_id=request_id,
                        method=method,
                        started_ns=started_ns,
                        outcome="exchange_unknown",
                        status_code=status_code,
                        error=f"{error_code}: {error_message}",
                        client_order_id=client_order_id,
                    )
                    raise BinanceUsdMWebSocketOrderUnknown(
                        request_id=request_id,
                        method=method,
                        reason=(
                            f"exchange timeout status={status_code} "
                            f"code={error_code}: {error_message}"
                        ),
                    )
                self._record_receipt(
                    request_id=request_id,
                    method=method,
                    started_ns=started_ns,
                    outcome="authoritative_errors",
                    status_code=status_code,
                    error=f"{error_code}: {error_message}",
                    client_order_id=client_order_id,
                )
                raise BinanceUsdMWebSocketApiError(
                    request_id=request_id,
                    method=method,
                    status_code=status_code,
                    error_code=error_code,
                    error_message=error_message,
                )

            if "result" not in response:
                self._close_connection_locked()
                reason = "protocol error: successful response has no result"
                self._record_receipt(
                    request_id=request_id,
                    method=method,
                    started_ns=started_ns,
                    outcome="protocol_errors",
                    status_code=status_code,
                    error=reason,
                    client_order_id=client_order_id,
                )
                raise BinanceUsdMWebSocketOrderUnknown(
                    request_id=request_id,
                    method=method,
                    reason=reason,
                )

            result = response["result"]
            if not isinstance(result, Mapping):
                self._close_connection_locked()
                reason = "protocol error: successful order result is not an object"
                self._record_receipt(
                    request_id=request_id,
                    method=method,
                    started_ns=started_ns,
                    outcome="protocol_errors",
                    status_code=status_code,
                    error=reason,
                    client_order_id=client_order_id,
                )
                raise BinanceUsdMWebSocketOrderUnknown(
                    request_id=request_id,
                    method=method,
                    reason=reason,
                )

            self._record_receipt(
                request_id=request_id,
                method=method,
                started_ns=started_ns,
                outcome="successes",
                status_code=status_code,
                client_order_id=client_order_id,
            )
            return result

    def new_order(self, **params: Any) -> Mapping[str, Any]:
        return self._request("order.place", params)

    def cancel_order(self, **params: Any) -> Mapping[str, Any]:
        return self._request("order.cancel", params)

    def health_snapshot(self) -> dict[str, object]:
        with self._health_lock:
            counters = dict(self._counters)
            method_counts = dict(self._method_counts)
            latencies = list(self._latency_ms)
            last_receipt = dict(self._last_receipt or {})
            last_error = self._last_error
        connection = self._connection
        started_ns = self._experiment_started_ns
        elapsed_s = (
            max(0.0, (self._monotonic_ns() - started_ns) / 1_000_000_000.0)
            if started_ns is not None
            else 0.0
        )
        return {
            "schema_version": "narrowgate.binance_usdm_websocket_order_health.v1",
            "enabled": True,
            "transport": self.transport_name,
            "url": self.config.url,
            "connected": bool(connection is not None and getattr(connection, "connected", True)),
            "automatic_write_retries": self.automatic_write_retries,
            "max_runtime_s": float(self.config.max_runtime_s),
            "elapsed_runtime_s": elapsed_s,
            "runtime_expired": elapsed_s >= float(self.config.max_runtime_s),
            "connection_generation": self._connection_generation,
            "counters": counters,
            "method_counts": method_counts,
            "latency_sample_count": len(latencies),
            "latency_ms": {
                "p50": _percentile(latencies, 0.50),
                "p90": _percentile(latencies, 0.90),
                "p99": _percentile(latencies, 0.99),
                "max": max(latencies) if latencies else None,
            },
            "last_receipt": last_receipt,
            "last_error": last_error,
        }


class BinanceUsdMOrderGateway:
    """Complete MakerEngine order transport with an optional WS hot path.

    USD-M WebSocket API documents individual ``order.place`` and
    ``order.cancel`` methods, but not the REST ``cancel_open_orders`` method.
    Symbol-level cancel-all therefore remains on the isolated order REST
    session.  This adapter prevents a staged WebSocket experiment from
    accidentally inventing an unsupported exchange method.
    """

    def __init__(
        self,
        *,
        rest_order_client: Any,
        websocket_order_gateway: BinanceUsdMWebSocketOrderGateway | None = None,
    ) -> None:
        if rest_order_client is None:
            raise ValueError("rest_order_client is required for cancel-all safety")
        self.rest_order_client = rest_order_client
        self.websocket_order_gateway = websocket_order_gateway
        # One owner serializes every write, including REST cancel-all.  This
        # prevents a fill callback, the quote loop and a fatal cleanup from
        # concurrently occupying the hot connection or reordering writes.
        self._write_lock = threading.Lock()

    @property
    def active_transport(self) -> str:
        return "websocket_api" if self.websocket_order_gateway is not None else "rest"

    def new_order(self, **params: Any) -> Any:
        with self._write_lock:
            client = self.websocket_order_gateway or self.rest_order_client
            return client.new_order(**params)

    def cancel_order(self, **params: Any) -> Any:
        with self._write_lock:
            client = self.websocket_order_gateway or self.rest_order_client
            return client.cancel_order(**params)

    def cancel_open_orders(self, **params: Any) -> Any:
        with self._write_lock:
            return self.rest_order_client.cancel_open_orders(**params)

    def close(self) -> None:
        websocket_gateway = self.websocket_order_gateway
        if websocket_gateway is not None:
            websocket_gateway.close()

    def health_snapshot(self) -> dict[str, object]:
        websocket_health = (
            self.websocket_order_gateway.health_snapshot()
            if self.websocket_order_gateway is not None
            else {"enabled": False, "transport": "binance_usdm_websocket_api"}
        )
        return {
            "schema_version": "narrowgate.binance_usdm_order_gateway.v1",
            "active_transport": self.active_transport,
            "cancel_all_transport": "rest",
            "websocket_api": websocket_health,
        }


def create_binance_usdm_websocket_order_gateway(
    *,
    key: str,
    secret: str,
    config: BinanceUsdMWebSocketOrderConfig | None = None,
    connection_factory: Callable[..., Any] | None = None,
    request_id_factory: Callable[[], str] | None = None,
    wall_time_ms: Callable[[], int] | None = None,
    monotonic_ns: Callable[[], int] | None = None,
) -> BinanceUsdMWebSocketOrderGateway | None:
    """Build the optional gateway; omitted configuration remains safely off."""

    effective = config or BinanceUsdMWebSocketOrderConfig()
    if not effective.enabled:
        return None
    return BinanceUsdMWebSocketOrderGateway(
        key=key,
        secret=secret,
        config=effective,
        connection_factory=connection_factory,
        request_id_factory=request_id_factory,
        wall_time_ms=wall_time_ms,
        monotonic_ns=monotonic_ns,
    )
