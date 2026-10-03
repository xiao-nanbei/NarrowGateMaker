"""Bybit public REST/WebSocket market-data connectors for shadow evidence.

The adapter polls only unauthenticated V5 market endpoints.  It deliberately
has no order/account methods, so enabling it cannot create a Bybit execution
path.  Exchange event time and local receive time are preserved separately;
REST response freshness alone is not treated as market-event freshness.
"""

from __future__ import annotations

import logging
import json
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any, Optional

import requests

from live.venues.common import (
    DailyJsonlRecorder,
    publish_cross_trade_arrays,
)
from market_fusion import (
    BYBIT_VENUE,
    PERP_MARKET,
    SPOT_MARKET,
    market_key,
    normalize_symbol,
)


logger = logging.getLogger("bybit_reference")

BYBIT_PUBLIC_REST = "https://api.bybit.com"


class BybitPublicRestReferenceClient:
    """Poll Bybit public BBO/trades and normalize them into venue-aware state."""

    def __init__(
        self,
        signal,
        cfg,
        *,
        project_root: Optional[Path] = None,
        session: Optional[requests.Session] = None,
    ):
        self.signal = signal
        self.cfg = cfg
        self.project_root = Path(project_root or Path.cwd())
        self.symbol = normalize_symbol(getattr(cfg, "symbol", "BTCUSDT"), "BTCUSDT")
        self.market_type = str(getattr(cfg, "instrument_type", PERP_MARKET)).strip().lower()
        if self.market_type not in {PERP_MARKET, SPOT_MARKET}:
            raise ValueError(f"unsupported Bybit instrument_type={self.market_type!r}")
        default_category = "spot" if self.market_type == SPOT_MARKET else "linear"
        self.category = str(
            getattr(cfg, "product_type", default_category) or default_category
        ).lower()
        if self.category in {"usdt-futures", "usdc-futures", "perp", "perpetual"}:
            self.category = "linear"
        self.rest_url = str(
            getattr(cfg, "rest_url", BYBIT_PUBLIC_REST) or BYBIT_PUBLIC_REST
        ).rstrip("/")
        self.poll_interval_s = max(0.05, float(getattr(cfg, "poll_interval_ms", 250.0)) / 1000.0)
        self.trade_poll_interval_s = max(
            self.poll_interval_s,
            float(getattr(cfg, "trade_poll_interval_ms", 500.0)) / 1000.0,
        )
        self.request_timeout_s = max(0.2, float(getattr(cfg, "request_timeout_s", 2.0)))
        self.max_source_age_s = max(0.1, float(getattr(cfg, "max_source_age_s", 2.0)))
        self.record_interval_ms = max(0.0, float(getattr(cfg, "record_interval_ms", 100.0)))
        self.record_trades = bool(getattr(cfg, "record_trades", True))
        self._record_enabled = bool(getattr(cfg, "record_enabled", False))
        record_queue_size = max(1, int(getattr(cfg, "record_queue_size", 20_000)))
        self.transport = "rest"

        record_dir = Path(str(getattr(cfg, "record_dir", "logs/external_venues")))
        if not record_dir.is_absolute():
            record_dir = self.project_root / record_dir
        self._recorder = DailyJsonlRecorder(
            record_dir,
            file_prefix=f"bybit_{self.market_type}_{self.symbol.lower()}",
            thread_name="bybit-event-writer",
            queue_size=record_queue_size,
        )

        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": "NarrowGateMaker/bybit-public-reference"})
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._state_lock = threading.Lock()
        self._last_book_receive_ns = 0
        self._last_trade_receive_ns = 0
        self._last_book_exchange_ms = 0
        self._last_trade_exchange_ms = 0
        self._last_record_book_ns = 0
        self._last_book_sequence: Optional[int] = None
        self._book_count = 0
        self._trade_count = 0
        self._poll_count = 0
        self._error_count = 0
        self._sequence_regressions = 0
        self._last_error = ""
        self._last_request_rtt_ms = float("inf")
        self._last_http_status = 0
        self._rate_limit_remaining = -1
        self._seen_trade_ids: set[str] = set()
        self._seen_trade_fifo: deque[str] = deque()

    @property
    def market_id(self) -> str:
        return market_key(BYBIT_VENUE, self.market_type, self.symbol)

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        if self._record_enabled:
            self._recorder.start()
        self._thread = threading.Thread(target=self._run, daemon=True, name="bybit-public-rest")
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread is not None:
            self._thread.join(timeout=max(5.0, self.request_timeout_s + 1.0))
        self._thread = None
        self._recorder.stop()
        try:
            self.session.close()
        except Exception:
            pass

    def snapshot(self, now_ns: Optional[int] = None) -> dict[str, Any]:
        current_ns = int(now_ns or time.time_ns())
        with self._state_lock:
            book_ns = self._last_book_receive_ns
            trade_ns = self._last_trade_receive_ns
            state = {
                "enabled": int(self._running),
                "market_id": self.market_id,
                "transport": self.transport,
                "book_count": self._book_count,
                "trade_count": self._trade_count,
                "poll_count": self._poll_count,
                "error_count": self._error_count,
                "reconnect_count": 0,
                "sequence_regressions": self._sequence_regressions,
                "last_error": self._last_error,
                "last_book_exchange_ms": self._last_book_exchange_ms,
                "last_trade_exchange_ms": self._last_trade_exchange_ms,
                "request_rtt_ms": self._last_request_rtt_ms,
                "last_http_status": self._last_http_status,
                "rate_limit_remaining": self._rate_limit_remaining,
            }
        state["book_age_ms"] = (current_ns - book_ns) / 1_000_000.0 if book_ns else float("inf")
        state["trade_age_ms"] = (current_ns - trade_ns) / 1_000_000.0 if trade_ns else float("inf")
        current_ms = current_ns / 1_000_000.0
        state["book_event_age_ms"] = (
            max(0.0, current_ms - state["last_book_exchange_ms"])
            if state["last_book_exchange_ms"]
            else float("inf")
        )
        state["trade_event_age_ms"] = (
            max(0.0, current_ms - state["last_trade_exchange_ms"])
            if state["last_trade_exchange_ms"]
            else float("inf")
        )
        state["book_transport_lag_ms"] = (
            max(0.0, book_ns / 1_000_000.0 - state["last_book_exchange_ms"])
            if book_ns and state["last_book_exchange_ms"]
            else float("inf")
        )
        state["trade_transport_lag_ms"] = (
            max(0.0, trade_ns / 1_000_000.0 - state["last_trade_exchange_ms"])
            if trade_ns and state["last_trade_exchange_ms"]
            else float("inf")
        )
        max_age_ms = self.max_source_age_s * 1000.0
        state["book_stale"] = int(
            state["book_age_ms"] > max_age_ms or state["book_event_age_ms"] > max_age_ms
        )
        state["trade_stale"] = int(
            state["trade_age_ms"] > max_age_ms or state["trade_event_age_ms"] > max_age_ms
        )
        state["stale"] = state["book_stale"]
        state.update(
            {f"record_{key}": value for key, value in self._recorder.snapshot().items()}
        )
        return state

    def _run(self) -> None:
        next_book = 0.0
        next_trade = 0.0
        while self._running:
            now = time.monotonic()
            did_work = False
            if now >= next_book:
                did_work = True
                next_book = now + self.poll_interval_s
                try:
                    self.poll_book()
                except Exception as exc:
                    self._record_error(f"book: {exc}")
            now = time.monotonic()
            if now >= next_trade:
                did_work = True
                next_trade = now + self.trade_poll_interval_s
                try:
                    self.poll_trades()
                except Exception as exc:
                    self._record_error(f"trade: {exc}")
            if not did_work:
                time.sleep(min(0.02, max(0.001, min(next_book, next_trade) - now)))

    def _request(self, path: str, params: dict[str, Any]) -> tuple[dict, int, float]:
        started_ns = time.time_ns()
        response = self.session.get(
            f"{self.rest_url}{path}", params=params, timeout=self.request_timeout_s
        )
        receive_ns = time.time_ns()
        rtt_ms = (receive_ns - started_ns) / 1_000_000.0
        response.raise_for_status()
        payload = response.json()
        if int(payload.get("retCode", -1)) != 0:
            raise RuntimeError(f"retCode={payload.get('retCode')} retMsg={payload.get('retMsg')}")
        try:
            remaining = int(response.headers.get("X-Bapi-Limit-Status", -1))
        except (TypeError, ValueError):
            remaining = -1
        with self._state_lock:
            self._poll_count += 1
            self._last_request_rtt_ms = rtt_ms
            self._last_http_status = int(response.status_code)
            self._rate_limit_remaining = remaining
        return payload, receive_ns, rtt_ms

    def poll_book(self) -> None:
        payload, receive_ns, rtt_ms = self._request(
            "/v5/market/orderbook",
            {"category": self.category, "symbol": self.symbol, "limit": 1},
        )
        self.handle_book_result(payload.get("result") or {}, receive_ns=receive_ns, rtt_ms=rtt_ms)

    def poll_trades(self) -> None:
        payload, receive_ns, rtt_ms = self._request(
            "/v5/market/recent-trade",
            {"category": self.category, "symbol": self.symbol, "limit": 100},
        )
        self.handle_trade_result(payload.get("result") or {}, receive_ns=receive_ns, rtt_ms=rtt_ms)

    def handle_book_result(self, result: dict, *, receive_ns: int, rtt_ms: float = 0.0) -> None:
        bids = result.get("b") or []
        asks = result.get("a") or []
        if not bids or not asks:
            return
        bid, bid_size = float(bids[0][0]), float(bids[0][1])
        ask, ask_size = float(asks[0][0]), float(asks[0][1])
        exchange_ms = int(result.get("cts", 0) or result.get("ts", 0) or 0)
        seq = int(result.get("seq", 0) or 0)
        if exchange_ms <= 0 or bid <= 0 or ask <= bid:
            return
        with self._state_lock:
            if self._last_book_sequence is not None and seq and seq < self._last_book_sequence:
                self._sequence_regressions += 1
            if seq:
                self._last_book_sequence = seq
            self._last_book_receive_ns = receive_ns
            self._last_book_exchange_ms = exchange_ms
            self._book_count += 1
        self.signal.on_book_ticker(
            {
                "s": self.symbol,
                "b": str(bid),
                "B": str(bid_size),
                "a": str(ask),
                "A": str(ask_size),
                "E": exchange_ms,
            },
            market_type=self.market_type,
            venue=BYBIT_VENUE,
            receive_ts_ns=receive_ns,
            sequence_number=seq or None,
        )
        if not self._record_enabled:
            return
        if self.record_interval_ms > 0:
            min_gap_ns = int(self.record_interval_ms * 1_000_000)
            if receive_ns - self._last_record_book_ns < min_gap_ns:
                return
        self._last_record_book_ns = receive_ns
        self._recorder.submit(
            {
                "market_id": self.market_id,
                "transport": self.transport,
                "event_type": "book",
                "exchange_event_ts_ns": exchange_ms * 1_000_000,
                "local_receive_ts_ns": receive_ns,
                "transport_lag_ms": receive_ns / 1_000_000.0 - exchange_ms,
                "request_rtt_ms": rtt_ms,
                "sequence_number": seq or None,
                "bid": bid,
                "bid_size": bid_size,
                "ask": ask,
                "ask_size": ask_size,
            }
        )

    def handle_trade_result(self, result: dict, *, receive_ns: int, rtt_ms: float = 0.0) -> None:
        rows = result.get("list") or []
        if not isinstance(rows, list):
            return
        normalized = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            trade_id = str(row.get("execId", ""))
            exchange_ms = int(row.get("time", 0) or 0)
            price = float(row.get("price", 0) or 0)
            size = float(row.get("size", 0) or 0)
            side = str(row.get("side", "")).lower()
            if (
                not trade_id
                or exchange_ms <= 0
                or price <= 0
                or size <= 0
                or side not in {"buy", "sell"}
            ):
                continue
            normalized.append((exchange_ms, trade_id, price, size, side, row))
        accepted = []
        for exchange_ms, trade_id, price, size, side, row in sorted(normalized):
            if not self._remember_trade(trade_id):
                continue
            accepted.append((exchange_ms, trade_id, price, size, side, row))
        if not accepted:
            return
        with self._state_lock:
            self._last_trade_receive_ns = receive_ns
            self._last_trade_exchange_ms = max(
                self._last_trade_exchange_ms,
                max(row[0] for row in accepted),
            )
            self._trade_count += len(accepted)
        sequences = [
            int(item[5].get("seq", 0) or 0) or None for item in accepted
        ]
        publish_cross_trade_arrays(
            self.signal,
            symbol=self.symbol,
            ts_ms=[item[0] for item in accepted],
            prices=[item[2] for item in accepted],
            quantities=[item[3] for item in accepted],
            is_buyer_maker=[item[4] == "sell" for item in accepted],
            market_type=self.market_type,
            venue=BYBIT_VENUE,
            receive_ts_ns=receive_ns,
            sequence_numbers=sequences,
        )
        for exchange_ms, trade_id, price, size, side, row in accepted:
            if self._record_enabled and self.record_trades:
                self._recorder.submit(
                    {
                        "market_id": self.market_id,
                        "transport": self.transport,
                        "event_type": "trade",
                        "exchange_event_ts_ns": exchange_ms * 1_000_000,
                        "local_receive_ts_ns": receive_ns,
                        "transport_lag_ms": receive_ns / 1_000_000.0 - exchange_ms,
                        "request_rtt_ms": rtt_ms,
                        "sequence_number": int(row.get("seq", 0) or 0) or None,
                        "trade_id": trade_id,
                        "price": price,
                        "size": size,
                        "side": side,
                        "is_block_trade": bool(row.get("isBlockTrade", False)),
                        "is_rpi_trade": bool(row.get("isRPITrade", False)),
                    }
                )

    def _remember_trade(self, trade_id: str) -> bool:
        if trade_id in self._seen_trade_ids:
            return False
        self._seen_trade_ids.add(trade_id)
        self._seen_trade_fifo.append(trade_id)
        while len(self._seen_trade_fifo) > 20_000:
            expired = self._seen_trade_fifo.popleft()
            self._seen_trade_ids.discard(expired)
        return True

    def _record_error(self, message: str) -> None:
        with self._state_lock:
            self._error_count += 1
            self._last_error = str(message)
        logger.warning("Bybit public %s reference error: %s", self.transport, message)


class BybitPublicWebSocketReferenceClient(BybitPublicRestReferenceClient):
    """Public Bybit spot/perp WebSocket using the REST-normalized state model."""

    def __init__(
        self,
        signal,
        cfg,
        *,
        project_root: Optional[Path] = None,
    ):
        super().__init__(signal, cfg, project_root=project_root)
        self.transport = "websocket"
        default_url = f"wss://stream.bybit.com/v5/public/{self.category}"
        self.websocket_url = str(getattr(cfg, "websocket_url", default_url) or default_url)
        self.book_channel = str(getattr(cfg, "book_channel", "orderbook.1") or "orderbook.1")
        self.trade_channel = str(getattr(cfg, "trade_channel", "publicTrade") or "publicTrade")
        self._ws = None
        self._ping_thread: Optional[threading.Thread] = None
        self._reconnect_count = 0

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        if self._record_enabled:
            self._recorder.start()
        self._thread = threading.Thread(target=self._run_ws, daemon=True, name="bybit-public-ws")
        self._thread.start()
        self._ping_thread = threading.Thread(
            target=self._ping_loop, daemon=True, name="bybit-public-ping"
        )
        self._ping_thread.start()

    def stop(self) -> None:
        self._running = False
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                pass
        if self._thread is not None:
            self._thread.join(timeout=5.0)
        if self._ping_thread is not None:
            self._ping_thread.join(timeout=2.0)
        self._thread = None
        self._ping_thread = None
        self._ws = None
        self._recorder.stop()
        try:
            self.session.close()
        except Exception:
            pass

    def snapshot(self, now_ns: Optional[int] = None) -> dict[str, Any]:
        state = super().snapshot(now_ns=now_ns)
        state["transport"] = self.transport
        state["reconnect_count"] = self._reconnect_count
        return state

    def _run_ws(self) -> None:
        import websocket

        backoff_s = 1.0
        while self._running:
            try:
                ws = websocket.WebSocketApp(
                    self.websocket_url,
                    on_open=self._on_ws_open,
                    on_message=self._on_ws_message,
                    on_error=self._on_ws_error,
                    on_close=self._on_ws_close,
                )
                self._ws = ws
                ws.run_forever(ping_interval=0)
                backoff_s = 1.0
            except Exception as exc:
                self._record_error(f"websocket: {exc}")
            finally:
                self._ws = None
            if not self._running:
                break
            self._reconnect_count += 1
            time.sleep(backoff_s)
            backoff_s = min(30.0, backoff_s * 2.0)

    def _ping_loop(self) -> None:
        while self._running:
            for _ in range(20):
                if not self._running:
                    return
                time.sleep(1.0)
            ws = self._ws
            if ws is None:
                continue
            try:
                ws.send(json.dumps({"op": "ping"}, separators=(",", ":")))
            except Exception as exc:
                self._record_error(f"ping: {exc}")

    def _on_ws_open(self, ws) -> None:
        topics = [f"{self.book_channel}.{self.symbol}", f"{self.trade_channel}.{self.symbol}"]
        ws.send(json.dumps({"op": "subscribe", "args": topics}, separators=(",", ":")))
        logger.info("Bybit public WebSocket subscribed: %s", self.market_id)

    def _on_ws_message(self, _ws, message: Any) -> None:
        receive_ns = time.time_ns()
        if isinstance(message, (bytes, bytearray)):
            message = message.decode("utf-8", errors="ignore")
        try:
            payload = json.loads(message) if isinstance(message, str) else message
            self.handle_ws_payload(payload, receive_ns=receive_ns)
        except Exception as exc:
            self._record_error(f"message: {exc}")

    def handle_ws_payload(self, payload: Any, *, receive_ns: Optional[int] = None) -> None:
        """Normalize one public message; public for deterministic tests."""
        if not isinstance(payload, dict) or payload.get("op") in {"pong", "subscribe"}:
            return
        if payload.get("success") is False:
            self._record_error(f"subscription: {payload}")
            return
        topic = str(payload.get("topic", ""))
        receive_ns = int(receive_ns or time.time_ns())
        if topic.startswith("orderbook."):
            data = payload.get("data") or {}
            if isinstance(data, dict):
                normalized = dict(data)
                normalized.setdefault("ts", payload.get("ts", 0))
                normalized.setdefault("cts", payload.get("cts", 0))
                self.handle_book_result(normalized, receive_ns=receive_ns)
        elif topic.startswith("publicTrade."):
            rows = []
            for row in payload.get("data") or []:
                if not isinstance(row, dict):
                    continue
                rows.append({
                    "execId": row.get("i", ""),
                    "symbol": row.get("s", self.symbol),
                    "price": row.get("p", "0"),
                    "size": row.get("v", "0"),
                    "side": row.get("S", ""),
                    "time": row.get("T", 0),
                    "seq": row.get("seq", 0),
                    "isBlockTrade": row.get("BT", False),
                    "isRPITrade": row.get("RPI", False),
                })
            self.handle_trade_result({"list": rows}, receive_ns=receive_ns)

    def _on_ws_error(self, _ws, error: Any) -> None:
        self._record_error(str(error))

    def _on_ws_close(self, _ws, status_code: Any, message: Any) -> None:
        if self._running:
            logger.warning("Bybit public WebSocket closed: %s %s", status_code, message)
