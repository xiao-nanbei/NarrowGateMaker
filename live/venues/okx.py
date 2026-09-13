"""Read-only OKX public REST/WebSocket connectors for shadow evidence.

Only unauthenticated market-data endpoints are exposed.  The adapter cannot
access an account or route orders.  OKX swap quantities are contract counts;
they are converted to base BTC with the configured contract multiplier before
entering the shared signal/recorder schema.
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
from market_fusion import OKX_VENUE, PERP_MARKET, SPOT_MARKET, market_key, normalize_symbol


logger = logging.getLogger("okx_reference")
OKX_PUBLIC_REST = "https://openapi.okx.com"
OKX_PUBLIC_WS = "wss://ws.okx.com:8443/ws/v5/public"


class OkxPublicRestReferenceClient:
    """Poll OKX public top-of-book/trades into the venue-aware shadow state."""

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
            raise ValueError(f"unsupported OKX instrument_type={self.market_type!r}")
        default_instrument = "BTC-USDT" if self.market_type == SPOT_MARKET else "BTC-USDT-SWAP"
        self.instrument_id = str(
            getattr(cfg, "instrument_id", default_instrument) or default_instrument
        ).strip().upper()
        self.contract_multiplier = float(getattr(cfg, "contract_multiplier", 1.0) or 1.0)
        if self.contract_multiplier <= 0.0:
            raise ValueError("OKX contract_multiplier must be positive")
        self.rest_url = str(
            getattr(cfg, "rest_url", OKX_PUBLIC_REST) or OKX_PUBLIC_REST
        ).rstrip("/")
        self.poll_interval_s = max(
            0.05, float(getattr(cfg, "poll_interval_ms", 250.0)) / 1000.0
        )
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
        self.source_size_unit = "base_asset" if self.market_type == SPOT_MARKET else "contracts"

        record_dir = Path(str(getattr(cfg, "record_dir", "logs/external_venues")))
        if not record_dir.is_absolute():
            record_dir = self.project_root / record_dir
        self._recorder = DailyJsonlRecorder(
            record_dir,
            file_prefix=f"okx_{self.market_type}_{self.symbol.lower()}",
            thread_name="okx-event-writer",
            queue_size=record_queue_size,
        )
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": "NarrowGateMaker/okx-public-reference"})
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
        self._seen_trade_ids: set[str] = set()
        self._seen_trade_fifo: deque[str] = deque()

    @property
    def market_id(self) -> str:
        return market_key(OKX_VENUE, self.market_type, self.symbol)

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        if self._record_enabled:
            self._recorder.start()
        self._thread = threading.Thread(target=self._run, daemon=True, name="okx-public-rest")
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
                "rate_limit_remaining": -1,
            }
        state["book_age_ms"] = (
            (current_ns - book_ns) / 1_000_000.0 if book_ns else float("inf")
        )
        state["trade_age_ms"] = (
            (current_ns - trade_ns) / 1_000_000.0 if trade_ns else float("inf")
        )
        current_ms = current_ns / 1_000_000.0
        state["book_event_age_ms"] = (
            max(0.0, current_ms - state["last_book_exchange_ms"])
            if state["last_book_exchange_ms"] else float("inf")
        )
        state["trade_event_age_ms"] = (
            max(0.0, current_ms - state["last_trade_exchange_ms"])
            if state["last_trade_exchange_ms"] else float("inf")
        )
        state["book_transport_lag_ms"] = (
            max(0.0, book_ns / 1_000_000.0 - state["last_book_exchange_ms"])
            if book_ns and state["last_book_exchange_ms"] else float("inf")
        )
        state["trade_transport_lag_ms"] = (
            max(0.0, trade_ns / 1_000_000.0 - state["last_trade_exchange_ms"])
            if trade_ns and state["last_trade_exchange_ms"] else float("inf")
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
        if str(payload.get("code", "")) != "0":
            raise RuntimeError(f"code={payload.get('code')} msg={payload.get('msg')}")
        with self._state_lock:
            self._poll_count += 1
            self._last_request_rtt_ms = rtt_ms
            self._last_http_status = int(response.status_code)
        return payload, receive_ns, rtt_ms

    def poll_book(self) -> None:
        payload, receive_ns, rtt_ms = self._request(
            "/api/v5/market/books", {"instId": self.instrument_id, "sz": "1"}
        )
        rows = payload.get("data") or []
        if rows:
            self.handle_book_result(rows[0], receive_ns=receive_ns, rtt_ms=rtt_ms)

    def poll_trades(self) -> None:
        payload, receive_ns, rtt_ms = self._request(
            "/api/v5/market/trades", {"instId": self.instrument_id, "limit": "100"}
        )
        self.handle_trade_result(payload.get("data") or [], receive_ns=receive_ns, rtt_ms=rtt_ms)

    def handle_book_result(self, result: dict, *, receive_ns: int, rtt_ms: float = 0.0) -> None:
        bids = result.get("bids") or []
        asks = result.get("asks") or []
        if not bids or not asks:
            return
        bid = float(bids[0][0])
        ask = float(asks[0][0])
        bid_size = float(bids[0][1]) * self.contract_multiplier
        ask_size = float(asks[0][1]) * self.contract_multiplier
        exchange_ms = int(result.get("ts", 0) or 0)
        seq = int(result.get("seqId", 0) or 0)
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
            {"s": self.symbol, "b": str(bid), "B": str(bid_size),
             "a": str(ask), "A": str(ask_size), "E": exchange_ms},
            market_type=self.market_type,
            venue=OKX_VENUE,
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
        self._recorder.submit({
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
            "source_size_unit": self.source_size_unit,
            "contract_multiplier": self.contract_multiplier,
        })

    def handle_trade_result(
        self, rows: list[dict], *, receive_ns: int, rtt_ms: float = 0.0
    ) -> None:
        normalized = []
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            trade_id = str(row.get("tradeId", ""))
            exchange_ms = int(row.get("ts", 0) or 0)
            price = float(row.get("px", 0) or 0)
            size = float(row.get("sz", 0) or 0) * self.contract_multiplier
            side = str(row.get("side", "")).lower()
            if not trade_id or exchange_ms <= 0 or price <= 0 or size <= 0 or side not in {"buy", "sell"}:
                continue
            normalized.append((exchange_ms, trade_id, price, size, side))
        accepted = []
        for exchange_ms, trade_id, price, size, side in sorted(normalized):
            if not self._remember_trade(trade_id):
                continue
            accepted.append((exchange_ms, trade_id, price, size, side))
        if not accepted:
            return
        with self._state_lock:
            self._last_trade_receive_ns = receive_ns
            self._last_trade_exchange_ms = max(
                self._last_trade_exchange_ms,
                max(row[0] for row in accepted),
            )
            self._trade_count += len(accepted)
        publish_cross_trade_arrays(
            self.signal,
            symbol=self.symbol,
            ts_ms=[item[0] for item in accepted],
            prices=[item[2] for item in accepted],
            quantities=[item[3] for item in accepted],
            is_buyer_maker=[item[4] == "sell" for item in accepted],
            market_type=self.market_type,
            venue=OKX_VENUE,
            receive_ts_ns=receive_ns,
            sequence_numbers=[None] * len(accepted),
        )
        for exchange_ms, trade_id, price, size, side in accepted:
            if self._record_enabled and self.record_trades:
                self._recorder.submit({
                    "market_id": self.market_id,
                    "transport": self.transport,
                    "event_type": "trade",
                    "exchange_event_ts_ns": exchange_ms * 1_000_000,
                    "local_receive_ts_ns": receive_ns,
                    "transport_lag_ms": receive_ns / 1_000_000.0 - exchange_ms,
                    "request_rtt_ms": rtt_ms,
                    "sequence_number": None,
                    "trade_id": trade_id,
                    "price": price,
                    "size": size,
                    "side": side,
                    "source_size_unit": self.source_size_unit,
                    "contract_multiplier": self.contract_multiplier,
                })

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
        logger.warning("OKX public %s reference error: %s", self.transport, message)


class OkxPublicWebSocketReferenceClient(OkxPublicRestReferenceClient):
    """Public OKX BBO/trade stream using the REST-normalized state model."""

    def __init__(
        self,
        signal,
        cfg,
        *,
        project_root: Optional[Path] = None,
    ):
        super().__init__(signal, cfg, project_root=project_root)
        self.transport = "websocket"
        self.websocket_url = str(
            getattr(cfg, "websocket_url", OKX_PUBLIC_WS) or OKX_PUBLIC_WS
        )
        self.book_channel = str(getattr(cfg, "book_channel", "bbo-tbt") or "bbo-tbt")
        self.trade_channel = str(getattr(cfg, "trade_channel", "trades") or "trades")
        self._ws = None
        self._ping_thread: Optional[threading.Thread] = None
        self._reconnect_count = 0

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        if self._record_enabled:
            self._recorder.start()
        self._thread = threading.Thread(
            target=self._run_ws, daemon=True, name="okx-public-ws"
        )
        self._thread.start()
        self._ping_thread = threading.Thread(
            target=self._ping_loop, daemon=True, name="okx-public-ping"
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
                ws.send("ping")
            except Exception as exc:
                self._record_error(f"ping: {exc}")

    def _on_ws_open(self, ws) -> None:
        args = [
            {"channel": self.book_channel, "instId": self.instrument_id},
            {"channel": self.trade_channel, "instId": self.instrument_id},
        ]
        ws.send(json.dumps({"op": "subscribe", "args": args}, separators=(",", ":")))
        logger.info("OKX public WebSocket subscribed: %s", self.market_id)

    def _on_ws_message(self, _ws, message: Any) -> None:
        receive_ns = time.time_ns()
        if isinstance(message, (bytes, bytearray)):
            message = message.decode("utf-8", errors="ignore")
        if message == "pong":
            return
        try:
            payload = json.loads(message) if isinstance(message, str) else message
            self.handle_ws_payload(payload, receive_ns=receive_ns)
        except Exception as exc:
            self._record_error(f"message: {exc}")

    def handle_ws_payload(self, payload: Any, *, receive_ns: Optional[int] = None) -> None:
        """Normalize one public message; public for deterministic tests."""
        if not isinstance(payload, dict):
            return
        if payload.get("event") == "error":
            self._record_error(f"subscription: {payload}")
            return
        if payload.get("event") in {"subscribe", "unsubscribe"}:
            return
        arg = payload.get("arg") or {}
        channel = str(arg.get("channel", ""))
        rows = payload.get("data") or []
        receive_ns = int(receive_ns or time.time_ns())
        if channel == self.book_channel:
            for row in rows if isinstance(rows, list) else []:
                if isinstance(row, dict):
                    self.handle_book_result(row, receive_ns=receive_ns)
        elif channel == self.trade_channel:
            self.handle_trade_result(rows, receive_ns=receive_ns)

    def _on_ws_error(self, _ws, error: Any) -> None:
        self._record_error(str(error))

    def _on_ws_close(self, _ws, status_code: Any, message: Any) -> None:
        if self._running:
            logger.warning("OKX public WebSocket closed: %s %s", status_code, message)
