"""Isolated Binance USD-M REST sessions and an optional WebSocket order gateway.

The REST client set allocates independent persistent pools for BUY, SELL and
safety traffic.  The behavior-identical default still routes every order write
through one legacy order pool; the side-specific pools become active only in
the explicit cross-side A/B arm.  Reconciliation, public snapshots, metrics
and listen-key maintenance cannot occupy an active order pool.  Requests are
never retried by the HTTP adapter: a write whose response is lost must be
reconciled, not replayed.

The WebSocket API gateway implements the small synchronous transport surface
consumed by :class:`strategy.maker_engine.MakerEngine`.  It is disabled by
default and is intended for measured A/B qualification.  There is at most one
in-flight request per gateway.  Once a frame may have been dispatched, any
timeout, disconnect or protocol ambiguity is reported as UNKNOWN and the
request is never sent again automatically.
"""

from __future__ import annotations

import csv
import hashlib
import hmac
import json
import math
import queue
import threading
import time
import uuid
from collections import Counter, OrderedDict, deque
from collections.abc import Callable, Mapping
from concurrent.futures import Future
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
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
ORDER_GATEWAY_RECEIPT_SCHEMA_VERSION = (
    "narrowgate.binance_usdm_order_gateway_receipt.v2"
)
_ORDER_GATEWAY_CORRELATION_LIMIT = 16_384


class BinanceUsdMRestRole(StrEnum):
    """One connection-pool ownership domain."""

    ORDER_BUY = "order_buy"
    ORDER_SELL = "order_sell"
    ORDER_SAFETY = "order_safety"
    RECONCILIATION = "reconciliation"
    RECONCILIATION_WORKER = "reconciliation_worker"
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
    """Independently pooled clients for hot lanes and cold REST roles."""

    order_buy: Any
    order_sell: Any
    order_safety: Any
    reconciliation: Any
    reconciliation_worker: Any
    market_snapshot: Any
    metrics: Any
    listen_key: Any

    @property
    def order(self) -> Any:
        """Compatibility alias for the behavior-identical global order pool."""

        return self.order_safety

    def by_role(self, role: BinanceUsdMRestRole | str) -> Any:
        normalized = BinanceUsdMRestRole(role)
        return getattr(self, normalized.value)

    def close(self) -> None:
        """Close every independent session exactly once."""

        seen: set[int] = set()
        failures: list[BaseException] = []
        for role in _REST_ROLES:
            client = self.by_role(role)
            session = getattr(client, "session", None)
            if session is None or id(session) in seen:
                continue
            seen.add(id(session))
            close = getattr(session, "close", None)
            if callable(close):
                try:
                    close()
                except BaseException as exc:
                    failures.append(exc)
        if failures:
            error = RuntimeError(
                f"{len(failures)} Binance USD-M REST session(s) failed to close"
            )
            add_note = getattr(error, "add_note", None)
            if callable(add_note):
                for failure in failures:
                    add_note(f"{type(failure).__name__}: {failure}")
            raise error from failures[0]

    def identity(self) -> dict[str, object]:
        sessions = {
            role.value: id(getattr(self.by_role(role), "session", None)) for role in _REST_ROLES
        }
        return {
            "schema_version": "narrowgate.binance_usdm_rest_roles.v2",
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
    except Exception as primary_error:
        partial = BinanceUsdMRestClients(
            order_buy=clients.get("order_buy"),
            order_sell=clients.get("order_sell"),
            order_safety=clients.get("order_safety"),
            reconciliation=clients.get("reconciliation"),
            reconciliation_worker=clients.get("reconciliation_worker"),
            market_snapshot=clients.get("market_snapshot"),
            metrics=clients.get("metrics"),
            listen_key=clients.get("listen_key"),
        )
        try:
            partial.close()
        except BaseException as cleanup_error:
            add_note = getattr(primary_error, "add_note", None)
            if callable(add_note):
                add_note(
                    "REST role construction cleanup failed: "
                    f"{type(cleanup_error).__name__}: {cleanup_error}"
                )
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


def _order_gateway_receipt_payload(
    *,
    transport: str,
    recorded_at_ns: int,
    request_id: str,
    client_order_id: str,
    decision_id: str,
    method: str,
    connection_generation: int,
    decision_ts_ns: int,
    gateway_call_ts_ns: int,
    dispatch_ts_ns: int,
    wire_ts_ns: int,
    response_ts_ns: int,
    outcome: str,
    status_code: int | None,
    exchange_order_status: str,
    error: str,
) -> dict[str, object]:
    """Build the shared REST/WebSocket per-request evidence schema."""

    may_have_been_dispatched = int(dispatch_ts_ns > 0)
    response_authoritative = outcome in {"successes", "authoritative_errors"}
    execution_status = (
        "authoritative_success"
        if outcome == "successes"
        else "authoritative_reject"
        if outcome == "authoritative_errors"
        else "not_dispatched"
        if not may_have_been_dispatched
        else "unknown"
    )
    return {
        "schema_version": ORDER_GATEWAY_RECEIPT_SCHEMA_VERSION,
        "record_type": "gateway_completion",
        "recorded_at_ns": int(recorded_at_ns),
        "transport": str(transport),
        "request_id": str(request_id),
        "client_order_id": str(client_order_id),
        "decision_id": str(decision_id),
        "method": str(method),
        "connection_generation": int(connection_generation),
        "decision_ts_ns": max(0, int(decision_ts_ns)),
        "gateway_call_ts_ns": max(0, int(gateway_call_ts_ns)),
        "dispatch_ts_ns": max(0, int(dispatch_ts_ns)),
        "wire_ts_ns": max(0, int(wire_ts_ns)),
        "response_ts_ns": max(0, int(response_ts_ns)),
        "unknown_ts_ns": int(recorded_at_ns) if execution_status == "unknown" else 0,
        "completion_ts_ns": int(recorded_at_ns),
        "may_have_been_dispatched": may_have_been_dispatched,
        "response_authoritative": int(response_authoritative),
        "outcome": str(outcome),
        "execution_status": execution_status,
        "http_status_code": "" if status_code is None else int(status_code),
        "exchange_order_status": str(exchange_order_status),
        "private_event_type": "",
        "private_order_status": "",
        "private_exchange_ts_ns": 0,
        "private_visibility_ts_ns": 0,
        "correlation_found": 0,
        "error": str(error),
        "gateway_call_to_dispatch_us": (
            max(0.0, (dispatch_ts_ns - gateway_call_ts_ns) / 1_000.0)
            if dispatch_ts_ns > 0 and gateway_call_ts_ns > 0
            else 0.0
        ),
        "dispatch_to_wire_us": (
            max(0.0, (wire_ts_ns - dispatch_ts_ns) / 1_000.0)
            if wire_ts_ns > 0 and dispatch_ts_ns > 0
            else 0.0
        ),
        "wire_to_response_us": (
            max(0.0, (response_ts_ns - wire_ts_ns) / 1_000.0)
            if response_ts_ns > 0 and wire_ts_ns > 0
            else 0.0
        ),
        "gateway_call_to_completion_us": (
            max(0.0, (recorded_at_ns - gateway_call_ts_ns) / 1_000.0)
            if gateway_call_ts_ns > 0
            else 0.0
        ),
    }


def _private_visibility_receipt_payload(
    *,
    transport: str,
    recorded_at_ns: int,
    request_id: str,
    client_order_id: str,
    decision_id: str,
    method: str,
    connection_generation: int,
    private_event_type: str,
    private_order_status: str,
    private_exchange_ts_ns: int,
    correlation_found: bool,
) -> dict[str, object]:
    """Build a joinable private-stream observation row.

    This is intentionally a separate row rather than a mutation of the
    gateway-completion row: Binance may publish ``NEW`` before the synchronous
    REST/WebSocket response returns.  The process-wide FIFO therefore records
    the order in which the process actually observed the two facts, while
    ``request_id`` (or, on a miss, ``client_order_id``) permits an offline join.
    """

    payload = _order_gateway_receipt_payload(
        transport=transport,
        recorded_at_ns=recorded_at_ns,
        request_id=request_id,
        client_order_id=client_order_id,
        decision_id=decision_id,
        method=method,
        connection_generation=connection_generation,
        decision_ts_ns=0,
        gateway_call_ts_ns=0,
        dispatch_ts_ns=0,
        wire_ts_ns=0,
        response_ts_ns=0,
        outcome="private_visibility",
        status_code=None,
        exchange_order_status=private_order_status,
        error="",
    )
    payload.update(
        {
            "record_type": "private_visibility",
            "completion_ts_ns": 0,
            "may_have_been_dispatched": int(correlation_found),
            "response_authoritative": 0,
            "execution_status": "private_visibility_observed",
            "private_event_type": private_event_type,
            "private_order_status": private_order_status,
            "private_exchange_ts_ns": max(0, int(private_exchange_ts_ns)),
            "private_visibility_ts_ns": int(recorded_at_ns),
            "correlation_found": int(correlation_found),
        }
    )
    return payload


def _initialize_order_gateway_receipt_log(path: str) -> None:
    """Create or validate the one stable CSV header before live starts."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.is_symlink():
        raise ValueError(f"order gateway receipt path must not be a symlink: {target}")
    expected = list(
        _order_gateway_receipt_payload(
            transport="",
            recorded_at_ns=0,
            request_id="",
            client_order_id="",
            decision_id="",
            method="",
            connection_generation=0,
            decision_ts_ns=0,
            gateway_call_ts_ns=0,
            dispatch_ts_ns=0,
            wire_ts_ns=0,
            response_ts_ns=0,
            outcome="",
            status_code=None,
            exchange_order_status="",
            error="",
        ).keys()
    )
    if target.exists() and target.stat().st_size > 0:
        with target.open(newline="", encoding="utf-8") as handle:
            actual = next(csv.reader(handle), [])
        if actual != expected:
            raise ValueError(f"order gateway receipt CSV schema mismatch: {target}")
        return
    with target.open("w", newline="", encoding="utf-8") as handle:
        csv.writer(handle).writerow(expected)


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
        wall_time_ns: Callable[[], int] | None = None,
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
        self._wall_time_ns = wall_time_ns or time.time_ns
        self._wall_time_ms = wall_time_ms or (
            lambda: self._wall_time_ns() // 1_000_000
        )
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
        self._pending_response_ts_ns = 0
        self._reader_failure: tuple[int, str] | None = None
        self._reader_thread: threading.Thread | None = None
        self._runtime_evidence_writer = None
        self._receipt_path = ""
        self._receipt_failure = ""
        self._receipt_failure_sink: Callable[[Exception], None] | None = None
        self._request_correlation_sink: Callable[..., None] | None = None

    def set_request_correlation_sink(self, sink: Callable[..., None]) -> None:
        """Bind the in-process private-stream correlation sink before use."""

        if not callable(sink):
            raise ValueError("WebSocket order request correlation sink must be callable")
        with self._health_lock:
            if self._request_correlation_sink is not None:
                raise RuntimeError("WebSocket order request correlation sink is already attached")
            if int(self._counters.get("requests", 0)) > 0:
                raise RuntimeError(
                    "request correlation sink must be attached before the first request"
                )
            self._request_correlation_sink = sink

    def set_runtime_evidence_writer(
        self,
        writer: Any,
        receipt_path: str,
        *,
        failure_sink: Callable[[Exception], None] | None = None,
    ) -> None:
        """Route immutable per-request rows through the process-wide FIFO.

        The gateway does not own or close the writer. A failed receipt revokes
        new-order admission, but must not turn a known exchange response into
        an unknown submit. The shared gateway surfaces the local failure
        separately from the network result.
        """

        normalized_path = str(receipt_path).strip()
        if writer is None:
            raise ValueError("runtime evidence writer is required")
        if not normalized_path:
            raise ValueError("WebSocket order receipt path is required")
        _initialize_order_gateway_receipt_log(normalized_path)
        with self._health_lock:
            if self._runtime_evidence_writer is not None:
                raise RuntimeError("WebSocket order receipt writer is already attached")
            if int(self._counters.get("requests", 0)) > 0:
                raise RuntimeError("receipt writer must be attached before the first request")
            self._runtime_evidence_writer = writer
            self._receipt_path = normalized_path
            self._receipt_failure_sink = failure_sink

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
                    self._pending_response_ts_ns = int(self._wall_time_ns())
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
        request_call_ts_ns: int,
        decision_ts_ns: int,
        decision_id: str,
        connection_generation: int,
        outcome: str,
        dispatch_ts_ns: int = 0,
        wire_ts_ns: int = 0,
        response_ts_ns: int = 0,
        status_code: int | None = None,
        exchange_order_status: str = "",
        error: str = "",
        client_order_id: str = "",
    ) -> None:
        completed_ts_ns = int(self._wall_time_ns())
        latency_ms = max(
            0.0,
            (self._monotonic_ns() - started_ns) / 1_000_000.0,
        )
        receipt = _order_gateway_receipt_payload(
            transport=self.transport_name,
            recorded_at_ns=completed_ts_ns,
            request_id=request_id,
            client_order_id=client_order_id,
            decision_id=decision_id,
            method=method,
            connection_generation=connection_generation,
            decision_ts_ns=decision_ts_ns,
            gateway_call_ts_ns=request_call_ts_ns,
            dispatch_ts_ns=dispatch_ts_ns,
            wire_ts_ns=wire_ts_ns,
            response_ts_ns=response_ts_ns,
            outcome=outcome,
            status_code=status_code,
            exchange_order_status=exchange_order_status,
            error=error,
        )
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
                "connection_generation": int(connection_generation),
                "client_order_id": client_order_id,
            }
            writer = self._runtime_evidence_writer
            receipt_path = self._receipt_path
        if writer is not None:
            try:
                writer.enqueue_csv(receipt_path, receipt)
            except Exception as exc:
                with self._health_lock:
                    self._counters["receipt_failures"] += 1
                    if not self._receipt_failure:
                        self._receipt_failure = f"{type(exc).__name__}: {exc}"
                    failure_sink = self._receipt_failure_sink
                if failure_sink is not None:
                    failure_sink(exc)

    def _request(
        self,
        method: str,
        params: Mapping[str, Any],
        *,
        decision_ts_ns: int = 0,
        decision_id: str = "",
    ) -> Any:
        request_id = str(self._request_id_factory())
        if not request_id:
            raise ValueError("request ID factory returned an empty identity")
        request_call_ts_ns = int(self._wall_time_ns())
        started_ns = self._monotonic_ns()
        client_order_id = str(
            params.get("newClientOrderId")
            or params.get("origClientOrderId")
            or ""
        )
        with self._io_lock:
            with self._health_lock:
                receipt_failed = bool(self._receipt_failure)
            if method == "order.place" and receipt_failed:
                raise BinanceUsdMOrderAdmissionRejected(
                    "order receipt collection failed; new order rejected before dispatch"
                )
            try:
                connection = self._ensure_connection_locked()
            except Exception as exc:
                self._record_receipt(
                    request_id=request_id,
                    method=method,
                    started_ns=started_ns,
                    request_call_ts_ns=request_call_ts_ns,
                    decision_ts_ns=decision_ts_ns,
                    decision_id=decision_id,
                    connection_generation=self._connection_generation,
                    outcome=(
                        "experiment_expired"
                        if isinstance(exc, BinanceUsdMWebSocketExperimentExpired)
                        else "pre_dispatch_unavailable"
                    ),
                    error=f"{type(exc).__name__}: {exc}",
                    client_order_id=client_order_id,
                )
                raise
            connection_generation = int(self._connection_generation)
            try:
                signed_params = self._signed_params(params)
            except Exception as exc:
                self._record_receipt(
                    request_id=request_id,
                    method=method,
                    started_ns=started_ns,
                    request_call_ts_ns=request_call_ts_ns,
                    decision_ts_ns=decision_ts_ns,
                    decision_id=decision_id,
                    connection_generation=connection_generation,
                    outcome="local_errors",
                    error=f"{type(exc).__name__}: {exc}",
                    client_order_id=client_order_id,
                )
                raise
            request = {
                "id": request_id,
                "method": method,
                "params": signed_params,
            }
            serialized = json.dumps(request, separators=(",", ":"), sort_keys=True)
            dispatch_ts_ns = 0
            wire_ts_ns = 0
            response_ts_ns = 0
            try:
                correlation_sink = self._request_correlation_sink
                if correlation_sink is not None and client_order_id:
                    # Register before the frame can reach the wire.  Binance is
                    # allowed to publish the private NEW event before the
                    # synchronous method response reaches this connection.
                    correlation_sink(
                        request_id=request_id,
                        client_order_id=client_order_id,
                        decision_id=decision_id,
                        method=method,
                        transport=self.transport_name,
                        connection_generation=connection_generation,
                    )
                with self._response_condition:
                    self._pending_request_id = request_id
                    self._pending_response = None
                    self._pending_response_ts_ns = 0
                    self._reader_failure = None
                dispatch_ts_ns = int(self._wall_time_ns())
                connection.send(serialized)
                wire_ts_ns = int(self._wall_time_ns())
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
                    response_ts_ns = int(self._pending_response_ts_ns)
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
                    request_call_ts_ns=request_call_ts_ns,
                    decision_ts_ns=decision_ts_ns,
                    decision_id=decision_id,
                    connection_generation=connection_generation,
                    outcome=outcome,
                    dispatch_ts_ns=dispatch_ts_ns,
                    wire_ts_ns=wire_ts_ns,
                    response_ts_ns=response_ts_ns,
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
                    self._pending_response_ts_ns = 0
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
                    request_call_ts_ns=request_call_ts_ns,
                    decision_ts_ns=decision_ts_ns,
                    decision_id=decision_id,
                    connection_generation=connection_generation,
                    outcome="protocol_errors",
                    dispatch_ts_ns=dispatch_ts_ns,
                    wire_ts_ns=wire_ts_ns,
                    response_ts_ns=response_ts_ns,
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
                        request_call_ts_ns=request_call_ts_ns,
                        decision_ts_ns=decision_ts_ns,
                        decision_id=decision_id,
                        connection_generation=connection_generation,
                        outcome="exchange_unknown",
                        dispatch_ts_ns=dispatch_ts_ns,
                        wire_ts_ns=wire_ts_ns,
                        response_ts_ns=response_ts_ns,
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
                    request_call_ts_ns=request_call_ts_ns,
                    decision_ts_ns=decision_ts_ns,
                    decision_id=decision_id,
                    connection_generation=connection_generation,
                    outcome="authoritative_errors",
                    dispatch_ts_ns=dispatch_ts_ns,
                    wire_ts_ns=wire_ts_ns,
                    response_ts_ns=response_ts_ns,
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
                    request_call_ts_ns=request_call_ts_ns,
                    decision_ts_ns=decision_ts_ns,
                    decision_id=decision_id,
                    connection_generation=connection_generation,
                    outcome="protocol_errors",
                    dispatch_ts_ns=dispatch_ts_ns,
                    wire_ts_ns=wire_ts_ns,
                    response_ts_ns=response_ts_ns,
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
                    request_call_ts_ns=request_call_ts_ns,
                    decision_ts_ns=decision_ts_ns,
                    decision_id=decision_id,
                    connection_generation=connection_generation,
                    outcome="protocol_errors",
                    dispatch_ts_ns=dispatch_ts_ns,
                    wire_ts_ns=wire_ts_ns,
                    response_ts_ns=response_ts_ns,
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
                request_call_ts_ns=request_call_ts_ns,
                decision_ts_ns=decision_ts_ns,
                decision_id=decision_id,
                connection_generation=connection_generation,
                outcome="successes",
                dispatch_ts_ns=dispatch_ts_ns,
                wire_ts_ns=wire_ts_ns,
                response_ts_ns=response_ts_ns,
                status_code=status_code,
                exchange_order_status=str(result.get("status", "")),
                client_order_id=client_order_id,
            )
            return result

    def new_order(
        self,
        *,
        _narrowgate_decision_ts_ns: int = 0,
        _narrowgate_decision_id: str = "",
        **params: Any,
    ) -> Mapping[str, Any]:
        return self._request(
            "order.place",
            params,
            decision_ts_ns=_narrowgate_decision_ts_ns,
            decision_id=_narrowgate_decision_id,
        )

    def cancel_order(
        self,
        *,
        _narrowgate_decision_ts_ns: int = 0,
        _narrowgate_decision_id: str = "",
        **params: Any,
    ) -> Mapping[str, Any]:
        return self._request(
            "order.cancel",
            params,
            decision_ts_ns=_narrowgate_decision_ts_ns,
            decision_id=_narrowgate_decision_id,
        )

    def health_snapshot(self) -> dict[str, object]:
        with self._health_lock:
            counters = dict(self._counters)
            method_counts = dict(self._method_counts)
            latencies = list(self._latency_ms)
            last_receipt = dict(self._last_receipt or {})
            last_error = self._last_error
            receipt_failure = self._receipt_failure
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
            "receipt_failure": receipt_failure,
        }


class BinanceUsdMOrderLaneFull(RuntimeError):
    """Raised before dispatch when a bounded asynchronous side lane is full."""

    may_have_been_dispatched = False
    requires_reconciliation = False


class BinanceUsdMOrderAdmissionRejected(RuntimeError):
    """Raised before dispatch when shutdown or a safety barrier rejects a write."""

    may_have_been_dispatched = False
    requires_reconciliation = False


class BinanceUsdMOrderProtocolUnknown(RuntimeError):
    """A dispatched write returned a response that cannot prove its state."""

    may_have_been_dispatched = True
    requires_reconciliation = True


@dataclass(frozen=True, slots=True)
class _QueuedOrderWrite:
    operation: Callable[[], Any]
    future: Future[Any]


@dataclass(frozen=True, slots=True)
class _OrderWriteCompletion:
    future: Future[Any]
    result: Any
    failure: BaseException | None
    delivered: Callable[[], None]


class _OrderedFutureCompletionDispatcher:
    """Resolve Futures off the side workers without dropping or reordering them."""

    _STOP = object()

    def __init__(self, *, capacity: int) -> None:
        if int(capacity) <= 0:
            raise ValueError("order completion dispatcher capacity must be positive")
        self._queue: queue.Queue[_OrderWriteCompletion | object] = queue.Queue(
            maxsize=int(capacity)
        )
        self._lock = threading.Lock()
        self._closed = False
        self._stop_enqueued = False
        self._thread = threading.Thread(
            target=self._run,
            name="ng-order-completion-dispatcher",
            daemon=True,
        )
        self._thread.start()

    def submit(self, completion: _OrderWriteCompletion) -> None:
        with self._lock:
            if self._closed:
                raise RuntimeError("order completion dispatcher is closed")
            try:
                # Side-lane admission slots bound unresolved completions to no
                # more than this queue's capacity.  Full therefore indicates
                # an internal accounting violation, not runtime backpressure.
                self._queue.put_nowait(completion)
            except queue.Full as exc:  # pragma: no cover - invariant guard
                raise RuntimeError("order completion dispatcher overflow") from exc

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            try:
                if item is self._STOP:
                    return
                assert isinstance(item, _OrderWriteCompletion)
                try:
                    if item.failure is None:
                        item.future.set_result(item.result)
                    else:
                        item.future.set_exception(item.failure)
                finally:
                    item.delivered()
            finally:
                self._queue.task_done()

    def in_dispatch_thread(self) -> bool:
        """Return whether the caller is this dispatcher's callback thread."""

        return threading.current_thread() is self._thread

    def close(self, *, deadline: float) -> None:
        with self._lock:
            self._closed = True
            stop_enqueued = self._stop_enqueued
        if not stop_enqueued:
            remaining = max(0.0, float(deadline) - time.monotonic())
            try:
                self._queue.put(self._STOP, timeout=remaining)
            except queue.Full as exc:
                raise TimeoutError(
                    "order completion dispatcher did not admit its stop marker"
                ) from exc
            with self._lock:
                self._stop_enqueued = True
        remaining = max(0.0, float(deadline) - time.monotonic())
        self._thread.join(timeout=remaining)
        if self._thread.is_alive():
            raise TimeoutError(
                "order completion dispatcher did not drain before shutdown"
            )


class _ExclusiveWriteBarrier:
    """Allow BUY/SELL concurrency while making cancel-all globally exclusive."""

    def __init__(self) -> None:
        self._condition = threading.Condition(threading.Lock())
        self._barrier_owner = threading.Lock()
        self._barrier_requested = False
        self._active_side_writes = 0

    def enter_side_write(self) -> None:
        with self._condition:
            while self._barrier_requested:
                self._condition.wait()
            self._active_side_writes += 1

    def leave_side_write(self) -> None:
        with self._condition:
            self._active_side_writes -= 1
            if self._active_side_writes < 0:  # pragma: no cover - invariant guard
                self._active_side_writes = 0
                raise RuntimeError("order write barrier active count underflow")
            if self._active_side_writes == 0:
                self._condition.notify_all()

    def run_exclusive(self, operation: Callable[[], Any]) -> Any:
        with self._barrier_owner:
            with self._condition:
                self._barrier_requested = True
                while self._active_side_writes:
                    self._condition.wait()
            try:
                return operation()
            finally:
                with self._condition:
                    self._barrier_requested = False
                    self._condition.notify_all()

    def wait_idle(self, *, deadline: float) -> bool:
        with self._condition:
            while self._active_side_writes:
                remaining = float(deadline) - time.monotonic()
                if remaining <= 0.0:
                    return False
                self._condition.wait(timeout=remaining)
            return True


class _StrictOrderWriteLane:
    """One bounded no-drop FIFO worker for a single order side."""

    _STOP = object()

    def __init__(
        self,
        *,
        side: str,
        capacity: int,
        completion_dispatcher: _OrderedFutureCompletionDispatcher,
    ) -> None:
        if int(capacity) <= 0:
            raise ValueError("asynchronous order lane capacity must be positive")
        self.side = str(side)
        self._queue: queue.Queue[_QueuedOrderWrite | object] = queue.Queue(
            maxsize=int(capacity)
        )
        self._lock = threading.Lock()
        self._idle_condition = threading.Condition(self._lock)
        # One active operation plus ``capacity`` queued operations are the
        # maximum admitted-but-not-yet-delivered writes for this side.  Holding
        # this slot until Future callbacks return keeps the shared completion
        # dispatcher mathematically bounded without ever blocking a side
        # worker behind a callback that is waiting on cancel-all.
        self._admission_slots = threading.BoundedSemaphore(int(capacity) + 1)
        self._completion_dispatcher = completion_dispatcher
        self._closed = False
        self._stop_enqueued = False
        self._submitted = 0
        self._completed = 0
        self._delivered = 0
        self._failed = 0
        self._high_watermark = 0
        self._thread = threading.Thread(
            target=self._run,
            name=f"ng-order-{self.side.lower()}-lane",
            daemon=True,
        )
        self._thread.start()

    def submit(
        self,
        operation: Callable[[], Any],
        *,
        done_callback: Callable[[Future[Any]], None] | None = None,
    ) -> Future[Any]:
        future: Future[Any] = Future()
        # Bind the consumer before queue admission.  A fast network response
        # can therefore never make add_done_callback execute synchronously on
        # the decision thread.
        if done_callback is not None:
            future.add_done_callback(done_callback)
        item = _QueuedOrderWrite(operation=operation, future=future)
        if not self._admission_slots.acquire(blocking=False):
            raise BinanceUsdMOrderLaneFull(
                f"{self.side} asynchronous order lane is full"
            )
        with self._lock:
            if self._closed:
                self._admission_slots.release()
                raise RuntimeError(f"{self.side} order lane is closed")
            try:
                self._queue.put_nowait(item)
            except queue.Full as exc:
                self._admission_slots.release()
                raise BinanceUsdMOrderLaneFull(
                    f"{self.side} asynchronous order lane is full"
                ) from exc
            self._submitted += 1
            self._high_watermark = max(
                self._high_watermark,
                self._queue.qsize(),
            )
        return future

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            if item is self._STOP:
                self._queue.task_done()
                return
            assert isinstance(item, _QueuedOrderWrite)
            if not item.future.set_running_or_notify_cancel():
                self._queue.task_done()
                with self._idle_condition:
                    self._completed += 1
                    self._idle_condition.notify_all()
                self._mark_completion_delivered()
                continue
            result: Any = None
            failure: BaseException | None = None
            try:
                result = item.operation()
            except BaseException as exc:
                failure = exc
            completion = _OrderWriteCompletion(
                future=item.future,
                result=result,
                failure=failure,
                delivered=self._mark_completion_delivered,
            )
            self._completion_dispatcher.submit(completion)
            # The network operation and its handoff are both complete before
            # the lane advertises progress.  Future callbacks execute on the
            # independent completion dispatcher, so this worker can drain a
            # later admitted write while an earlier callback enters cancel-all.
            self._queue.task_done()
            with self._idle_condition:
                if failure is not None:
                    self._failed += 1
                self._completed += 1
                self._idle_condition.notify_all()

    def _mark_completion_delivered(self) -> None:
        self._admission_slots.release()
        with self._idle_condition:
            self._delivered += 1
            self._idle_condition.notify_all()

    def close(self, *, deadline: float) -> None:
        with self._lock:
            self._closed = True
            stop_enqueued = self._stop_enqueued
        if not stop_enqueued:
            # STOP admission and thread join consume one shared deadline.  A
            # full queue with a hung worker therefore cannot make shutdown wait
            # once for space and then wait the full timeout a second time.
            remaining = max(0.0, float(deadline) - time.monotonic())
            try:
                self._queue.put(self._STOP, timeout=remaining)
            except queue.Full as exc:
                raise TimeoutError(
                    f"{self.side} order lane did not admit its stop marker"
                ) from exc
            with self._lock:
                self._stop_enqueued = True
        remaining = max(0.0, float(deadline) - time.monotonic())
        self._thread.join(timeout=remaining)
        if self._thread.is_alive():
            raise TimeoutError(f"{self.side} order lane did not drain before shutdown")

    def wait_idle(self, *, deadline: float) -> bool:
        with self._idle_condition:
            while self._completed < self._submitted:
                remaining = float(deadline) - time.monotonic()
                if remaining <= 0.0:
                    return False
                self._idle_condition.wait(timeout=remaining)
            return True

    def wait_completions_delivered(self, *, deadline: float) -> bool:
        """Wait until every admitted Future and its callbacks have returned."""

        with self._idle_condition:
            while self._delivered < self._submitted:
                remaining = float(deadline) - time.monotonic()
                if remaining <= 0.0:
                    return False
                self._idle_condition.wait(timeout=remaining)
            return True

    def health_snapshot(self) -> dict[str, object]:
        with self._lock:
            return {
                "side": self.side,
                "closed": self._closed,
                "submitted": self._submitted,
                "completed": self._completed,
                "future_results_delivered": self._delivered,
                "failed": self._failed,
                "queue_depth": self._queue.qsize(),
                "queue_high_watermark": self._high_watermark,
                "worker_alive": self._thread.is_alive(),
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
        rest_buy_order_client: Any | None = None,
        rest_sell_order_client: Any | None = None,
        rest_safety_order_client: Any | None = None,
        websocket_order_gateway: BinanceUsdMWebSocketOrderGateway | None = None,
        request_id_factory: Callable[[], str] | None = None,
        wall_time_ns: Callable[[], int] | None = None,
        async_order_lanes_enabled: bool = False,
        cross_side_order_lanes_enabled: bool = False,
        async_order_lane_capacity: int = 8,
        async_order_lane_drain_timeout_s: float = 10.0,
        async_order_lane_max_runtime_s: float = 900.0,
    ) -> None:
        if rest_order_client is None:
            raise ValueError("rest_order_client is required for cancel-all safety")
        self.rest_order_client = rest_order_client
        self.rest_buy_order_client = rest_buy_order_client or rest_order_client
        self.rest_sell_order_client = rest_sell_order_client or rest_order_client
        self.rest_safety_order_client = rest_safety_order_client or rest_order_client
        self.websocket_order_gateway = websocket_order_gateway
        self._request_id_factory = request_id_factory or (
            lambda: f"rest-{uuid.uuid4().hex}"
        )
        self._wall_time_ns = wall_time_ns or time.time_ns
        self._runtime_evidence_writer = None
        self._receipt_path = ""
        self._correlation_lock = threading.Lock()
        self._request_correlations: OrderedDict[
            tuple[str, str], dict[str, object]
        ] = OrderedDict()
        self._client_order_sides: OrderedDict[str, str] = OrderedDict()
        self._private_visibility_counts: Counter[str] = Counter()
        self._side_write_locks = {
            "BUY": threading.Lock(),
            "SELL": threading.Lock(),
        }
        # The deployed baseline serialized every write behind one lock.  Keep
        # that exact arrival order unless the restart-only asynchronous-lane
        # experiment is explicitly enabled; side parallelism is itself an
        # economic timing change and must not leak into the switch-off path.
        self._legacy_write_lock = threading.Lock()
        self._close_lock = threading.Lock()
        self._write_admission_lock = threading.RLock()
        self._write_admission_state_lock = threading.Lock()
        self._write_admission_epoch = 0
        self._write_admission_barrier_active = False
        self._new_order_admission_revoked = False
        self._receipt_failure = ""
        self._receipt_failure_count = 0
        self._write_barrier = _ExclusiveWriteBarrier()
        self._async_order_lane_drain_timeout_s = float(
            async_order_lane_drain_timeout_s
        )
        if (
            not math.isfinite(self._async_order_lane_drain_timeout_s)
            or self._async_order_lane_drain_timeout_s <= 0.0
        ):
            raise ValueError("async order lane drain timeout must be positive")
        self._async_order_lane_max_runtime_s = float(
            async_order_lane_max_runtime_s
        )
        if (
            not math.isfinite(self._async_order_lane_max_runtime_s)
            or not 1.0 <= self._async_order_lane_max_runtime_s <= 3_600.0
        ):
            raise ValueError("async order lane max runtime must be in [1, 3600]")
        self._async_order_lane_started_monotonic = time.monotonic()
        self._async_order_lane_deadline_monotonic = (
            self._async_order_lane_started_monotonic
            + self._async_order_lane_max_runtime_s
        )
        self._cross_side_order_lanes_enabled = bool(
            cross_side_order_lanes_enabled
        )
        if self._cross_side_order_lanes_enabled and not bool(
            async_order_lanes_enabled
        ):
            raise ValueError(
                "cross-side order lanes require asynchronous order lanes"
            )
        async_capacity = int(async_order_lane_capacity)
        if bool(async_order_lanes_enabled) and async_capacity <= 0:
            raise ValueError("asynchronous order lane capacity must be positive")
        self._completion_dispatchers: dict[
            str, _OrderedFutureCompletionDispatcher
        ] = {}
        self._async_order_lanes = {}
        self._async_lane_by_side: dict[str, _StrictOrderWriteLane] = {}
        self._closed = False
        self._shutdown_complete = False
        try:
            if bool(async_order_lanes_enabled):
                lane_names = (
                    ("BUY", "SELL")
                    if self._cross_side_order_lanes_enabled
                    else ("GLOBAL",)
                )
                for lane_name in lane_names:
                    dispatcher = _OrderedFutureCompletionDispatcher(
                        capacity=async_capacity + 1
                    )
                    # Record the dispatcher before constructing its lane so a
                    # later constructor failure cannot orphan this thread.
                    self._completion_dispatchers[lane_name] = dispatcher
                    lane = _StrictOrderWriteLane(
                        side=lane_name,
                        capacity=async_capacity,
                        completion_dispatcher=dispatcher,
                    )
                    self._async_order_lanes[lane_name] = lane
                if self._cross_side_order_lanes_enabled:
                    self._async_lane_by_side = dict(self._async_order_lanes)
                else:
                    global_lane = self._async_order_lanes["GLOBAL"]
                    self._async_lane_by_side = {
                        "BUY": global_lane,
                        "SELL": global_lane,
                    }
            if websocket_order_gateway is not None:
                correlation_setter = getattr(
                    websocket_order_gateway, "set_request_correlation_sink", None
                )
                if callable(correlation_setter):
                    correlation_setter(self._register_request_correlation)
        except BaseException as primary_error:
            # __init__ has already taken ownership of the supplied WS gateway
            # and any lane threads it started.  The caller cannot close a
            # half-constructed object, so unwind them here without hiding the
            # construction failure.
            try:
                self.close()
            except BaseException as cleanup_error:
                primary_error.add_note(
                    "order gateway constructor cleanup also failed: "
                    f"{type(cleanup_error).__name__}: {cleanup_error}"
                )
            raise

    supports_narrowgate_request_metadata = True

    @property
    def active_transport(self) -> str:
        return "websocket_api" if self.websocket_order_gateway is not None else "rest"

    @property
    def async_order_lanes_enabled(self) -> bool:
        return bool(self._async_order_lanes)

    @property
    def cross_side_order_lanes_enabled(self) -> bool:
        return bool(self._cross_side_order_lanes_enabled)

    @property
    def async_order_lane_deadline_monotonic(self) -> float | None:
        """Absolute experiment deadline shared with the process hard-stop timer."""

        if not self.async_order_lanes_enabled:
            return None
        return float(self._async_order_lane_deadline_monotonic)

    @property
    def shutdown_complete(self) -> bool:
        with self._write_admission_state_lock:
            return bool(self._shutdown_complete)

    supports_prebound_async_callback = True

    def _reject_expired_async_experiment(self) -> None:
        if not self.async_order_lanes_enabled:
            return
        if time.monotonic() >= self._async_order_lane_deadline_monotonic:
            raise BinanceUsdMOrderAdmissionRejected(
                "bounded asynchronous order-lane experiment expired"
            )

    @staticmethod
    def _new_order_is_risk_adding(params: Mapping[str, Any]) -> bool:
        """Conservatively classify new orders for experiment-expiry admission."""

        def enabled(value: Any) -> bool:
            if isinstance(value, bool):
                return value
            return str(value or "").strip().lower() == "true"

        return not (
            enabled(params.get("reduceOnly"))
            or enabled(params.get("closePosition"))
        )

    def _capture_write_admission_ticket(
        self,
        *,
        reject_if_async_experiment_expired: bool = False,
        reject_if_new_order_revoked: bool = False,
    ) -> int:
        """Capture an attempt epoch without waiting behind an active barrier."""

        if reject_if_async_experiment_expired:
            self._reject_expired_async_experiment()
        with self._write_admission_state_lock:
            if self._closed:
                raise BinanceUsdMOrderAdmissionRejected(
                    "order gateway is closed; write rejected before dispatch"
                )
            if reject_if_new_order_revoked and self._new_order_admission_revoked:
                raise BinanceUsdMOrderAdmissionRejected(
                    "new-order admission was revoked; write rejected before dispatch"
                )
            if self._write_admission_barrier_active:
                raise BinanceUsdMOrderAdmissionRejected(
                    "exclusive safety barrier is active; write rejected before dispatch"
                )
            return int(self._write_admission_epoch)

    def _validate_write_admission_ticket(
        self,
        ticket: int,
        *,
        reject_if_async_experiment_expired: bool = False,
        reject_if_new_order_revoked: bool = False,
    ) -> None:
        if reject_if_async_experiment_expired:
            self._reject_expired_async_experiment()
        with self._write_admission_state_lock:
            if self._closed:
                raise BinanceUsdMOrderAdmissionRejected(
                    "order gateway is closed; write rejected before dispatch"
                )
            if reject_if_new_order_revoked and self._new_order_admission_revoked:
                raise BinanceUsdMOrderAdmissionRejected(
                    "new-order admission was revoked; write rejected before dispatch"
                )
            if (
                self._write_admission_barrier_active
                or int(ticket) != self._write_admission_epoch
            ):
                raise BinanceUsdMOrderAdmissionRejected(
                    "write attempt crossed an exclusive safety barrier and was "
                    "rejected before dispatch"
                )

    def _activate_write_admission_barrier(self, ticket: int) -> None:
        self._validate_write_admission_ticket(ticket)
        with self._write_admission_state_lock:
            # Invalidate every attempt that observed the pre-barrier epoch but
            # had not yet reached the admission critical section.
            self._write_admission_epoch += 1
            self._write_admission_barrier_active = True

    def _release_write_admission_barrier(self) -> None:
        with self._write_admission_state_lock:
            self._write_admission_barrier_active = False

    def revoke_new_order_admission(self) -> None:
        """Permanently reject new orders while preserving safety cancellations."""

        with self._write_admission_lock:
            with self._write_admission_state_lock:
                if self._new_order_admission_revoked:
                    return
                self._new_order_admission_revoked = True
                # Invalidate new-order attempts that captured an earlier epoch
                # but have not yet crossed the admission critical section.
                self._write_admission_epoch += 1

    def _latch_receipt_failure(self, exc: Exception) -> None:
        # This may run on a network worker while cancel-all owns the admission
        # lock and waits for that worker. Only take the short state lock: the
        # response and safety barrier must still be able to finish.
        with self._write_admission_state_lock:
            self._receipt_failure_count += 1
            if not self._receipt_failure:
                self._receipt_failure = f"{type(exc).__name__}: {exc}"
            self._new_order_admission_revoked = True

    def raise_if_evidence_failed(self) -> None:
        """Surface local receipt failure independently of exchange authority."""

        with self._write_admission_state_lock:
            failure = self._receipt_failure
        if failure:
            raise RuntimeError(f"order gateway receipt collection failed: {failure}")

    def _reject_receipt_failed_new_order_before_dispatch(self) -> None:
        # An earlier request can poison the writer after this request was
        # admitted to the GLOBAL FIFO. Cancel-all still drains the same FIFO;
        # its epoch is intentionally not rechecked here.
        with self._write_admission_state_lock:
            if self._receipt_failure:
                raise BinanceUsdMOrderAdmissionRejected(
                    "new-order admission was revoked; write rejected before dispatch"
                )

    def _enqueue_receipt(self, receipt: Mapping[str, Any]) -> None:
        writer = self._runtime_evidence_writer
        if writer is not None:
            try:
                writer.enqueue_csv(self._receipt_path, receipt)
            except Exception as exc:
                self._latch_receipt_failure(exc)

    @staticmethod
    def _normalized_order_side(side: Any) -> str:
        normalized = str(side or "").strip().upper()
        if normalized not in {"BUY", "SELL"}:
            raise ValueError("order side must be BUY or SELL")
        return normalized

    def _remember_client_order_side(self, client_order_id: str, side: str) -> None:
        normalized_cid = str(client_order_id).strip()
        if not normalized_cid:
            return
        normalized_side = self._normalized_order_side(side)
        with self._correlation_lock:
            self._client_order_sides.pop(normalized_cid, None)
            self._client_order_sides[normalized_cid] = normalized_side
            while len(self._client_order_sides) > _ORDER_GATEWAY_CORRELATION_LIMIT:
                self._client_order_sides.popitem(last=False)

    def _resolve_cancel_side(
        self,
        *,
        client_order_id: str,
        explicit_side: Any,
    ) -> str:
        if str(explicit_side or "").strip():
            return self._normalized_order_side(explicit_side)
        with self._correlation_lock:
            remembered = self._client_order_sides.get(str(client_order_id))
        if remembered is None:
            raise ValueError(
                "cancel side is unknown; pass _narrowgate_order_side so the "
                "request can enter the correct strict FIFO lane"
            )
        return remembered

    def _rest_client_for_side(self, side: str) -> Any:
        # Preserve the deployed B0 network path unless the separate
        # cross-side experiment is explicitly enabled.  Merely constructing
        # isolated clients must not move BUY/SELL onto different TCP/TLS
        # congestion and keepalive histories while both experiment switches
        # are off (or while async response isolation keeps one GLOBAL FIFO).
        if not self._cross_side_order_lanes_enabled:
            return self.rest_order_client
        return (
            self.rest_buy_order_client
            if self._normalized_order_side(side) == "BUY"
            else self.rest_sell_order_client
        )

    def _run_side_write(
        self,
        side: str,
        operation: Callable[[], Any],
        *,
        admission_already_counted: bool = False,
    ) -> Any:
        normalized_side = self._normalized_order_side(side)
        write_lock = (
            self._side_write_locks[normalized_side]
            if self._cross_side_order_lanes_enabled
            else self._legacy_write_lock
        )
        if not admission_already_counted:
            self._write_barrier.enter_side_write()
        with write_lock:
            try:
                return operation()
            finally:
                self._write_barrier.leave_side_write()

    def _run_exclusive_write(
        self,
        operation: Callable[[], Any],
        *,
        admission_ticket: int,
    ) -> Any:
        """Run cancel-all/unknown-side writes against the active lane model."""

        with self._write_admission_lock:
            self._activate_write_admission_barrier(admission_ticket)
            try:
                deadline = (
                    time.monotonic() + self._async_order_lane_drain_timeout_s
                )
                for lane in self._async_order_lanes.values():
                    if not lane.wait_idle(deadline=deadline):
                        raise TimeoutError(
                            "asynchronous order lane did not drain before "
                            "exclusive write"
                        )
                return self._write_barrier.run_exclusive(operation)
            finally:
                self._release_write_admission_barrier()

    def _run_safety_exclusive_write(self, operation: Callable[[], Any]) -> Any:
        """Serialize safety writes instead of rejecting a concurrent attempt."""

        deadline = time.monotonic() + self._async_order_lane_drain_timeout_s
        remaining = max(0.0, deadline - time.monotonic())
        if not self._write_admission_lock.acquire(timeout=remaining):
            raise TimeoutError(
                "safety write did not acquire exclusive admission before deadline"
            )
        barrier_active = False
        try:
            with self._write_admission_state_lock:
                if self._closed:
                    raise BinanceUsdMOrderAdmissionRejected(
                        "order gateway is closed; write rejected before dispatch"
                    )
                admission_ticket = int(self._write_admission_epoch)
            self._activate_write_admission_barrier(admission_ticket)
            barrier_active = True
            for lane in self._async_order_lanes.values():
                if not lane.wait_idle(deadline=deadline):
                    raise TimeoutError(
                        "asynchronous order lane did not drain before safety write"
                    )
            return self._write_barrier.run_exclusive(operation)
        finally:
            if barrier_active:
                self._release_write_admission_barrier()
            self._write_admission_lock.release()

    def drain_async_order_completions(
        self,
        *,
        timeout_s: float | None = None,
    ) -> None:
        """Boundedly wait for all admitted Future callbacks to return.

        This is deliberately separate from the cancel-all lane barrier: that
        barrier drains network writes but must not wait for a callback that may
        itself issue a safety cancel-all.  Call this only after cancel-all has
        released its barrier.
        """

        if not self._async_order_lanes:
            return
        if any(
            dispatcher.in_dispatch_thread()
            for dispatcher in self._completion_dispatchers.values()
        ):
            raise RuntimeError(
                "cannot drain asynchronous order completions from a completion callback"
            )
        timeout = (
            self._async_order_lane_drain_timeout_s
            if timeout_s is None
            else float(timeout_s)
        )
        if not math.isfinite(timeout) or timeout <= 0.0:
            raise ValueError("async order completion drain timeout must be positive")
        deadline = time.monotonic() + timeout
        for lane in self._async_order_lanes.values():
            if not lane.wait_completions_delivered(deadline=deadline):
                raise TimeoutError(
                    "asynchronous order completion callbacks did not drain"
                )

    def _run_or_enqueue_side_write(
        self,
        side: str,
        operation: Callable[[], Any],
        *,
        admission_ticket: int,
        reject_if_async_experiment_expired: bool = False,
        reject_if_new_order_revoked: bool = False,
    ) -> Any:
        """Preserve strict same-side admission order for synchronous callers."""

        normalized_side = self._normalized_order_side(side)
        with self._write_admission_lock:
            self._validate_write_admission_ticket(
                admission_ticket,
                reject_if_async_experiment_expired=(
                    reject_if_async_experiment_expired
                ),
                reject_if_new_order_revoked=reject_if_new_order_revoked,
            )
            if not self._async_order_lanes:
                # Count the write as admitted before it can wait behind the
                # baseline's global serialization lock.  A later cancel-all
                # must drain it rather than allowing it to escape afterward.
                self._write_barrier.enter_side_write()
                run_inline = True
                future = None
            else:
                run_inline = False
                future = self._async_lane_by_side[normalized_side].submit(
                    lambda: self._run_side_write(
                        normalized_side,
                        operation,
                    )
                )
        if run_inline:
            return self._run_side_write(
                normalized_side,
                operation,
                admission_already_counted=True,
            )
        assert future is not None
        return future.result()

    def set_runtime_evidence_writer(self, writer: Any, receipt_path: str) -> None:
        normalized_path = str(receipt_path).strip()
        if writer is None:
            raise ValueError("runtime evidence writer is required")
        if not normalized_path:
            raise ValueError("order gateway receipt path is required")
        if self._runtime_evidence_writer is not None:
            raise RuntimeError("order gateway receipt writer is already attached")
        _initialize_order_gateway_receipt_log(normalized_path)
        self._runtime_evidence_writer = writer
        self._receipt_path = normalized_path
        websocket_gateway = self.websocket_order_gateway
        if websocket_gateway is not None:
            websocket_gateway.set_runtime_evidence_writer(
                writer, normalized_path, failure_sink=self._latch_receipt_failure
            )

    def _register_request_correlation(
        self,
        *,
        request_id: str,
        client_order_id: str,
        decision_id: str,
        method: str,
        transport: str,
        connection_generation: int,
    ) -> None:
        """Remember a bounded request identity before it can reach Binance."""

        client_order_id = str(client_order_id)
        method = str(method)
        if not client_order_id or not method:
            return
        key = (client_order_id, method)
        entry: dict[str, object] = {
            "request_id": str(request_id),
            "decision_id": str(decision_id),
            "method": method,
            "transport": str(transport),
            "connection_generation": int(connection_generation),
        }
        with self._correlation_lock:
            self._request_correlations.pop(key, None)
            self._request_correlations[key] = entry
            while len(self._request_correlations) > _ORDER_GATEWAY_CORRELATION_LIMIT:
                self._request_correlations.popitem(last=False)
                self._private_visibility_counts["correlation_evictions"] += 1

    @staticmethod
    def _private_visibility_method_candidates(
        *, order_status: str, event_type: str
    ) -> tuple[str, ...]:
        if order_status == "NEW" or event_type == "NEW":
            return ("order.place",)
        if order_status in {"CANCELED", "EXPIRED"} or event_type in {
            "CANCELED",
            "EXPIRED",
        }:
            return ("order.cancel", "order.place")
        return ("order.place", "order.cancel")

    def record_private_order_visibility(
        self,
        event: Mapping[str, Any],
        *,
        receive_ts_ns: int,
    ) -> bool:
        """Record every private order update independently of state de-duplication.

        The callback deliberately does not acquire ``_write_lock``: a private
        event may arrive while the synchronous order request still owns that
        lock.  Only the tiny bounded correlation map is touched before the row
        is admitted to the process-wide evidence FIFO.
        """

        client_order_id = str(event.get("c", "") or "")
        if not client_order_id:
            return False
        order_status = str(event.get("X", "") or "").strip().upper()
        event_type = str(event.get("x", "") or "").strip().upper()
        correlation: dict[str, object] | None = None
        with self._correlation_lock:
            for candidate in self._private_visibility_method_candidates(
                order_status=order_status,
                event_type=event_type,
            ):
                correlation = self._request_correlations.get(
                    (client_order_id, candidate)
                )
                if correlation is not None:
                    break
            found = correlation is not None
            self._private_visibility_counts["attempts"] += 1
            self._private_visibility_counts[
                "correlated" if found else "uncorrelated"
            ] += 1
        correlation = correlation or {}
        writer = self._runtime_evidence_writer
        if writer is None:
            raise RuntimeError("private order visibility writer is not attached")
        fallback_transport = (
            str(
                getattr(
                    self.websocket_order_gateway,
                    "transport_name",
                    "binance_usdm_websocket_api",
                )
            )
            if self.active_transport == "websocket_api"
            else "rest"
        )
        exchange_ts_ns = max(0, int(event.get("T", 0) or 0)) * 1_000_000
        row = _private_visibility_receipt_payload(
            transport=str(correlation.get("transport", fallback_transport)),
            recorded_at_ns=int(receive_ts_ns),
            request_id=str(correlation.get("request_id", "")),
            client_order_id=client_order_id,
            decision_id=str(correlation.get("decision_id", "")),
            method=str(correlation.get("method", "private.order_update")),
            connection_generation=int(
                correlation.get("connection_generation", 0) or 0
            ),
            private_event_type=event_type,
            private_order_status=order_status,
            private_exchange_ts_ns=exchange_ts_ns,
            correlation_found=found,
        )
        try:
            writer.enqueue_csv(self._receipt_path, row)
        except Exception as exc:
            self._latch_receipt_failure(exc)
            raise
        with self._correlation_lock:
            self._private_visibility_counts["admitted"] += 1
        return found

    @staticmethod
    def _rest_error_status_code(exc: BaseException) -> int | None:
        raw_status = getattr(exc, "status_code", None)
        try:
            return int(raw_status) if raw_status is not None else None
        except (TypeError, ValueError):
            return None

    def _rest_request(
        self,
        *,
        method: str,
        operation: Callable[..., Any],
        params: Mapping[str, Any],
        decision_ts_ns: int,
        decision_id: str,
    ) -> Any:
        request_id = str(self._request_id_factory())
        if not request_id:
            raise ValueError("REST request ID factory returned an empty identity")
        client_order_id = str(
            params.get("newClientOrderId")
            or params.get("origClientOrderId")
            or ""
        )
        gateway_call_ts_ns = int(self._wall_time_ns())
        dispatch_ts_ns = int(self._wall_time_ns())
        self._register_request_correlation(
            request_id=request_id,
            client_order_id=client_order_id,
            decision_id=decision_id,
            method=method,
            transport="rest",
            connection_generation=0,
        )
        try:
            response = operation(**dict(params))
        except Exception as exc:
            completed_ts_ns = int(self._wall_time_ns())
            status_code = self._rest_error_status_code(exc)
            response_authoritative = bool(
                getattr(exc, "exchange_response_authoritative", False)
            ) or bool(
                status_code is not None
                and 400 <= status_code < 500
                and status_code != 408
            )
            outcome = (
                "authoritative_errors"
                if response_authoritative
                else "transport_unknown"
            )
            receipt = _order_gateway_receipt_payload(
                transport="rest",
                recorded_at_ns=completed_ts_ns,
                request_id=request_id,
                client_order_id=client_order_id,
                decision_id=decision_id,
                method=method,
                connection_generation=0,
                decision_ts_ns=decision_ts_ns,
                gateway_call_ts_ns=gateway_call_ts_ns,
                dispatch_ts_ns=dispatch_ts_ns,
                wire_ts_ns=0,
                response_ts_ns=completed_ts_ns if response_authoritative else 0,
                outcome=outcome,
                status_code=status_code,
                exchange_order_status="",
                error=f"{type(exc).__name__}: {exc}",
            )
            self._enqueue_receipt(receipt)
            raise

        completed_ts_ns = int(self._wall_time_ns())
        result = response if isinstance(response, Mapping) else {}
        if method in {"order.place", "order.cancel"} and (
            not isinstance(response, Mapping)
            or not str(result.get("status", "")).strip()
        ):
            protocol_error = BinanceUsdMOrderProtocolUnknown(
                f"{method} returned no authoritative order status"
            )
            receipt = _order_gateway_receipt_payload(
                transport="rest",
                recorded_at_ns=completed_ts_ns,
                request_id=request_id,
                client_order_id=client_order_id,
                decision_id=decision_id,
                method=method,
                connection_generation=0,
                decision_ts_ns=decision_ts_ns,
                gateway_call_ts_ns=gateway_call_ts_ns,
                dispatch_ts_ns=dispatch_ts_ns,
                wire_ts_ns=0,
                response_ts_ns=completed_ts_ns,
                outcome="transport_unknown",
                status_code=None,
                exchange_order_status="",
                error=f"{type(protocol_error).__name__}: {protocol_error}",
            )
            self._enqueue_receipt(receipt)
            raise protocol_error
        receipt = _order_gateway_receipt_payload(
            transport="rest",
            recorded_at_ns=completed_ts_ns,
            request_id=request_id,
            client_order_id=client_order_id,
            decision_id=decision_id,
            method=method,
            connection_generation=0,
            decision_ts_ns=decision_ts_ns,
            gateway_call_ts_ns=gateway_call_ts_ns,
            dispatch_ts_ns=dispatch_ts_ns,
            wire_ts_ns=0,
            response_ts_ns=completed_ts_ns,
            outcome="successes",
            status_code=None,
            exchange_order_status=str(result.get("status", "")),
            error="",
        )
        self._enqueue_receipt(receipt)
        return response

    def new_order(
        self,
        *,
        _narrowgate_decision_ts_ns: int = 0,
        _narrowgate_decision_id: str = "",
        _narrowgate_order_side: str = "",
        **params: Any,
    ) -> Any:
        risk_adding = self._new_order_is_risk_adding(params)
        admission_ticket = self._capture_write_admission_ticket(
            reject_if_async_experiment_expired=risk_adding,
            reject_if_new_order_revoked=True,
        )
        side = self._normalized_order_side(
            _narrowgate_order_side or params.get("side")
        )
        client_order_id = str(params.get("newClientOrderId", ""))
        self._remember_client_order_side(client_order_id, side)

        def operation() -> Any:
            self._reject_receipt_failed_new_order_before_dispatch()
            websocket_gateway = self.websocket_order_gateway
            if websocket_gateway is not None:
                return websocket_gateway.new_order(
                    _narrowgate_decision_ts_ns=_narrowgate_decision_ts_ns,
                    _narrowgate_decision_id=_narrowgate_decision_id,
                    **params,
                )
            rest_client = self._rest_client_for_side(side)
            return self._rest_request(
                method="order.place",
                operation=rest_client.new_order,
                params=params,
                decision_ts_ns=_narrowgate_decision_ts_ns,
                decision_id=_narrowgate_decision_id,
            )
        return self._run_or_enqueue_side_write(
            side,
            operation,
            admission_ticket=admission_ticket,
            reject_if_async_experiment_expired=risk_adding,
            reject_if_new_order_revoked=True,
        )

    def new_order_async(
        self,
        *,
        _narrowgate_decision_ts_ns: int = 0,
        _narrowgate_decision_id: str = "",
        _narrowgate_order_side: str = "",
        _narrowgate_done_callback: Callable[[Future[Any]], None] | None = None,
        **params: Any,
    ) -> Future[Any]:
        """Admit one exact new-order write to its bounded side FIFO."""

        risk_adding = self._new_order_is_risk_adding(params)
        admission_ticket = self._capture_write_admission_ticket(
            reject_if_async_experiment_expired=risk_adding,
            reject_if_new_order_revoked=True,
        )
        if not self._async_order_lanes:
            raise RuntimeError("asynchronous order lanes are not enabled")
        side = self._normalized_order_side(
            _narrowgate_order_side or params.get("side")
        )
        frozen_params = dict(params)
        client_order_id = str(frozen_params.get("newClientOrderId", ""))
        self._remember_client_order_side(client_order_id, side)

        def operation() -> Any:
            self._reject_receipt_failed_new_order_before_dispatch()
            websocket_gateway = self.websocket_order_gateway
            if websocket_gateway is not None:
                return websocket_gateway.new_order(
                    _narrowgate_decision_ts_ns=_narrowgate_decision_ts_ns,
                    _narrowgate_decision_id=_narrowgate_decision_id,
                    **frozen_params,
                )
            rest_client = self._rest_client_for_side(side)
            return self._rest_request(
                method="order.place",
                operation=rest_client.new_order,
                params=frozen_params,
                decision_ts_ns=_narrowgate_decision_ts_ns,
                decision_id=_narrowgate_decision_id,
            )

        with self._write_admission_lock:
            self._validate_write_admission_ticket(
                admission_ticket,
                reject_if_async_experiment_expired=risk_adding,
                reject_if_new_order_revoked=True,
            )
            return self._async_lane_by_side[side].submit(
                lambda: self._run_side_write(
                    side,
                    operation,
                ),
                done_callback=_narrowgate_done_callback,
            )

    def cancel_order(
        self,
        *,
        _narrowgate_decision_ts_ns: int = 0,
        _narrowgate_decision_id: str = "",
        _narrowgate_order_side: str = "",
        **params: Any,
    ) -> Any:
        client_order_id = str(params.get("origClientOrderId", ""))
        try:
            side = self._resolve_cancel_side(
                client_order_id=client_order_id,
                explicit_side=_narrowgate_order_side,
            )
        except ValueError:
            # Compatibility/query callers that do not own an in-process order
            # cannot be assigned to a side lane.  Serialize them as an
            # exclusive write rather than guessing a side.
            side = ""

        admission_ticket = (
            self._capture_write_admission_ticket() if side else None
        )

        def operation() -> Any:
            websocket_gateway = self.websocket_order_gateway
            if websocket_gateway is not None:
                return websocket_gateway.cancel_order(
                    _narrowgate_decision_ts_ns=_narrowgate_decision_ts_ns,
                    _narrowgate_decision_id=_narrowgate_decision_id,
                    **params,
                )
            rest_client = (
                self._rest_client_for_side(side)
                if side
                else self.rest_safety_order_client
            )
            return self._rest_request(
                method="order.cancel",
                operation=rest_client.cancel_order,
                params=params,
                decision_ts_ns=_narrowgate_decision_ts_ns,
                decision_id=_narrowgate_decision_id,
            )
        if not side:
            return self._run_safety_exclusive_write(operation)
        assert admission_ticket is not None
        return self._run_or_enqueue_side_write(
            side,
            operation,
            admission_ticket=admission_ticket,
        )

    def cancel_order_async(
        self,
        *,
        _narrowgate_decision_ts_ns: int = 0,
        _narrowgate_decision_id: str = "",
        _narrowgate_order_side: str = "",
        _narrowgate_done_callback: Callable[[Future[Any]], None] | None = None,
        **params: Any,
    ) -> Future[Any]:
        """Admit one exact cancel to the same FIFO as that side's submits."""

        admission_ticket = self._capture_write_admission_ticket()
        if not self._async_order_lanes:
            raise RuntimeError("asynchronous order lanes are not enabled")
        frozen_params = dict(params)
        side = self._resolve_cancel_side(
            client_order_id=str(frozen_params.get("origClientOrderId", "")),
            explicit_side=_narrowgate_order_side,
        )

        def operation() -> Any:
            websocket_gateway = self.websocket_order_gateway
            if websocket_gateway is not None:
                return websocket_gateway.cancel_order(
                    _narrowgate_decision_ts_ns=_narrowgate_decision_ts_ns,
                    _narrowgate_decision_id=_narrowgate_decision_id,
                    **frozen_params,
                )
            rest_client = self._rest_client_for_side(side)
            return self._rest_request(
                method="order.cancel",
                operation=rest_client.cancel_order,
                params=frozen_params,
                decision_ts_ns=_narrowgate_decision_ts_ns,
                decision_id=_narrowgate_decision_id,
            )

        with self._write_admission_lock:
            self._validate_write_admission_ticket(admission_ticket)
            return self._async_lane_by_side[side].submit(
                lambda: self._run_side_write(
                    side,
                    operation,
                ),
                done_callback=_narrowgate_done_callback,
            )

    def cancel_open_orders(self, **params: Any) -> Any:
        def operation() -> Any:
            return self._rest_request(
                method="order.cancel_all",
                operation=self.rest_safety_order_client.cancel_open_orders,
                params=params,
                decision_ts_ns=0,
                decision_id="",
            )

        return self._run_safety_exclusive_write(operation)

    def close(self) -> None:
        with self._close_lock:
            self._close_once()

    def _close_once(self) -> None:
        with self._write_admission_lock:
            with self._write_admission_state_lock:
                if self._shutdown_complete:
                    return
                if not self._closed:
                    self._closed = True
                    self._new_order_admission_revoked = True
                    self._write_admission_epoch += 1
                    self._write_admission_barrier_active = True
        deadline = time.monotonic() + self._async_order_lane_drain_timeout_s
        if not self._write_barrier.wait_idle(deadline=deadline):
            raise TimeoutError("active order writes did not drain before shutdown")
        errors: list[BaseException] = []
        for lane in self._async_order_lanes.values():
            try:
                lane.close(deadline=deadline)
            except BaseException as exc:  # pragma: no cover - shutdown tail
                errors.append(exc)
        # Do not close the dispatcher or network transport while a lane may
        # still hand off a completion.  A later close() retries the bounded
        # drain without reopening write admission.
        if errors:
            raise RuntimeError(
                "one or more asynchronous order lanes failed to drain"
            ) from errors[0]
        for completion_dispatcher in self._completion_dispatchers.values():
            try:
                completion_dispatcher.close(deadline=deadline)
            except BaseException as exc:  # pragma: no cover - shutdown tail
                raise RuntimeError(
                    "order completion dispatcher failed to drain"
                ) from exc
        websocket_gateway = self.websocket_order_gateway
        if websocket_gateway is not None:
            websocket_gateway.close()
        with self._write_admission_state_lock:
            self._shutdown_complete = True

    def health_snapshot(self) -> dict[str, object]:
        websocket_health = (
            self.websocket_order_gateway.health_snapshot()
            if self.websocket_order_gateway is not None
            else {"enabled": False, "transport": "binance_usdm_websocket_api"}
        )
        with self._correlation_lock:
            correlation_count = len(self._request_correlations)
            side_identity_count = len(self._client_order_sides)
            private_visibility_counts = dict(self._private_visibility_counts)
        with self._write_admission_state_lock:
            receipt_failure = self._receipt_failure
            receipt_failure_count = self._receipt_failure_count
        return {
            "schema_version": "narrowgate.binance_usdm_order_gateway.v1",
            "active_transport": self.active_transport,
            "cancel_all_transport": "rest",
            "request_correlation_count": correlation_count,
            "client_order_side_identity_count": side_identity_count,
            "private_visibility_counts": private_visibility_counts,
            "receipt_failure": receipt_failure,
            "receipt_failure_count": receipt_failure_count,
            "async_order_lanes_enabled": self.async_order_lanes_enabled,
            "shutdown_complete": self.shutdown_complete,
            "new_order_admission_revoked": bool(
                self._new_order_admission_revoked
            ),
            "cross_side_order_lanes_enabled": (
                self.cross_side_order_lanes_enabled
            ),
            "async_order_lane_max_runtime_s": (
                self._async_order_lane_max_runtime_s
            ),
            "async_order_lane_deadline_monotonic": (
                self.async_order_lane_deadline_monotonic
            ),
            "async_order_lane_runtime_expired": (
                self.async_order_lanes_enabled
                and time.monotonic()
                >= self._async_order_lane_deadline_monotonic
            ),
            "async_order_lanes": {
                side: lane.health_snapshot()
                for side, lane in self._async_order_lanes.items()
            },
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
    wall_time_ns: Callable[[], int] | None = None,
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
        wall_time_ns=wall_time_ns,
        monotonic_ns=monotonic_ns,
    )
