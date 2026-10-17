"""Bitget public market-data connector for cross-venue shadow evidence.

This adapter intentionally connects only to Bitget's public WebSocket. It has
no order methods and never reads API credentials. External data can therefore
be observed alongside the Binance execution market without creating a second
execution path by accident.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from live.venues.common import (
    DailyJsonlRecorder,
    publish_cross_trade_arrays,
)
from market_fusion import (
    BITGET_VENUE,
    PERP_MARKET,
    SPOT_MARKET,
    market_key,
    normalize_symbol,
)

logger = logging.getLogger("bitget_reference")

BITGET_PUBLIC_WS = "wss://ws.bitget.com/v3/ws/public"


@dataclass(frozen=True)
class BitgetSubscription:
    symbol: str = "BTCUSDT"
    product_type: str = "USDT-FUTURES"
    book_channel: str = "books1"
    trade_channel: str = "trade"
    api_version: int = 2

    def args(self) -> list[dict[str, str]]:
        if self.api_version >= 3:
            return [
                {
                    "instType": self.product_type.lower(),
                    "topic": self.book_channel,
                    "symbol": self.symbol,
                },
                {
                    "instType": self.product_type.lower(),
                    "topic": self.trade_channel,
                    "symbol": self.symbol,
                },
            ]
        return [
            {
                "instType": self.product_type,
                "channel": self.book_channel,
                "instId": self.symbol,
            },
            {
                "instType": self.product_type,
                "channel": self.trade_channel,
                "instId": self.symbol,
            },
        ]


class BitgetPublicReferenceClient:
    """Reconnectable public BBO/trade stream with receive-time telemetry."""

    def __init__(
        self,
        signal,
        cfg,
        *,
        project_root: Path | None = None,
    ):
        self.signal = signal
        self.cfg = cfg
        self.project_root = Path(project_root or Path.cwd())
        self.market_type = str(getattr(cfg, "instrument_type", PERP_MARKET)).strip().lower()
        if self.market_type not in {PERP_MARKET, SPOT_MARKET}:
            raise ValueError(f"unsupported Bitget instrument_type={self.market_type!r}")
        default_product_type = "SPOT" if self.market_type == SPOT_MARKET else "USDT-FUTURES"
        self.websocket_url = str(
            getattr(cfg, "websocket_url", BITGET_PUBLIC_WS) or BITGET_PUBLIC_WS
        )
        api_version = 3 if "/v3/" in self.websocket_url else 2
        default_trade_channel = "publicTrade" if api_version >= 3 else "trade"
        self.subscription = BitgetSubscription(
            symbol=normalize_symbol(getattr(cfg, "symbol", "BTCUSDT"), "BTCUSDT"),
            product_type=str(
                getattr(cfg, "product_type", default_product_type) or default_product_type
            ).upper(),
            book_channel=str(getattr(cfg, "book_channel", "books1") or "books1"),
            trade_channel=str(
                getattr(cfg, "trade_channel", default_trade_channel)
                or default_trade_channel
            ),
            api_version=api_version,
        )
        self.max_source_age_s = max(0.1, float(getattr(cfg, "max_source_age_s", 2.0)))
        self.record_interval_ms = max(0.0, float(getattr(cfg, "record_interval_ms", 100.0)))
        self.record_trades = bool(getattr(cfg, "record_trades", True))
        record_queue_size = max(1, int(getattr(cfg, "record_queue_size", 20_000)))

        record_dir = Path(str(getattr(cfg, "record_dir", "logs/external_venues")))
        if not record_dir.is_absolute():
            record_dir = self.project_root / record_dir
        self._recorder = DailyJsonlRecorder(
            record_dir,
            file_prefix=f"bitget_{self.market_type}_{self.subscription.symbol.lower()}",
            thread_name="bitget-event-writer",
            queue_size=record_queue_size,
        )
        self._record_enabled = bool(getattr(cfg, "record_enabled", False))

        self._running = False
        self._thread: threading.Thread | None = None
        self._ping_thread: threading.Thread | None = None
        self._ws = None
        self._state_lock = threading.Lock()
        self._last_book_receive_ns = 0
        self._last_trade_receive_ns = 0
        self._last_book_exchange_ms = 0
        self._last_trade_exchange_ms = 0
        self._last_record_book_ns = 0
        self._book_count = 0
        self._trade_count = 0
        self._error_count = 0
        self._reconnect_count = 0
        self._sequence_regressions = 0
        self._last_book_sequence: int | None = None
        self._last_error = ""

    @property
    def market_id(self) -> str:
        return market_key(BITGET_VENUE, self.market_type, self.subscription.symbol)

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        if self._record_enabled:
            self._recorder.start()
        self._thread = threading.Thread(target=self._run, daemon=True, name="bitget-public-ws")
        self._thread.start()
        self._ping_thread = threading.Thread(
            target=self._ping_loop, daemon=True, name="bitget-public-ping"
        )
        self._ping_thread.start()

    def stop(self) -> None:
        self._running = False
        ws = self._ws
        if ws is not None:
            try:
                ws.close()
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

    def snapshot(self, now_ns: int | None = None) -> dict[str, Any]:
        current_ns = int(now_ns or time.time_ns())
        with self._state_lock:
            book_ns = self._last_book_receive_ns
            trade_ns = self._last_trade_receive_ns
            state = {
                "enabled": int(self._running),
                "market_id": self.market_id,
                "book_count": self._book_count,
                "trade_count": self._trade_count,
                "error_count": self._error_count,
                "reconnect_count": self._reconnect_count,
                "sequence_regressions": self._sequence_regressions,
                "last_error": self._last_error,
                "last_book_exchange_ms": self._last_book_exchange_ms,
                "last_trade_exchange_ms": self._last_trade_exchange_ms,
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
        # books1 is the fair-value/reference availability contract. Trade
        # silence degrades flow features independently but must not mark a
        # fresh BBO source unavailable.
        state["stale"] = state["book_stale"]
        state.update(
            {f"record_{key}": value for key, value in self._recorder.snapshot().items()}
        )
        return state

    def _run(self) -> None:
        import websocket

        backoff_s = 1.0
        while self._running:
            try:
                ws = websocket.WebSocketApp(
                    self.websocket_url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._ws = ws
                ws.run_forever(ping_interval=0)
            except Exception as exc:
                self._record_error(f"run_forever: {exc}")
            finally:
                self._ws = None

            if not self._running:
                break
            with self._state_lock:
                self._reconnect_count += 1
            time.sleep(backoff_s)
            backoff_s = min(30.0, backoff_s * 2.0)

    def _ping_loop(self) -> None:
        while self._running:
            for _ in range(30):
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
                try:
                    ws.close()
                except Exception:
                    pass

    def _on_open(self, ws) -> None:
        payload = {"op": "subscribe", "args": self.subscription.args()}
        ws.send(json.dumps(payload, separators=(",", ":")))
        logger.info("Bitget public reference subscribed: %s", self.market_id)

    def _on_message(self, _ws, message: Any) -> None:
        receive_ns = time.time_ns()
        if isinstance(message, (bytes, bytearray)):
            message = message.decode("utf-8", errors="ignore")
        if message == "pong":
            return
        try:
            payload = json.loads(message) if isinstance(message, str) else message
            self.handle_payload(payload, receive_ts_ns=receive_ns)
        except Exception as exc:
            self._record_error(f"message: {exc}")

    def handle_payload(self, payload: Any, *, receive_ts_ns: int | None = None) -> None:
        """Normalize one Bitget payload; public for deterministic unit tests."""
        if not isinstance(payload, dict):
            return
        if payload.get("event") == "error":
            self._record_error(f"subscription {payload.get('code')}: {payload.get('msg')}")
            return
        arg = payload.get("arg") or {}
        channel = str(arg.get("channel") or arg.get("topic") or "")
        rows = payload.get("data") or []
        if not isinstance(rows, list):
            return
        receive_ns = int(receive_ts_ns or time.time_ns())
        if channel.startswith("books"):
            for row in rows:
                self._handle_book(row, receive_ns)
        elif channel == self.subscription.trade_channel:
            self._handle_trades(rows, receive_ns)

    def _handle_book(self, row: Any, receive_ns: int) -> None:
        if not isinstance(row, dict):
            return
        bids = row.get("bids") or row.get("b") or []
        asks = row.get("asks") or row.get("a") or []
        if not bids or not asks:
            return
        bid, bid_size = float(bids[0][0]), float(bids[0][1])
        ask, ask_size = float(asks[0][0]), float(asks[0][1])
        exchange_ms = int(row.get("ts", 0) or 0)
        seq = int(row.get("seq", 0) or 0)
        previous_seq = int(row.get("pseq", 0) or 0) or None
        with self._state_lock:
            prior_sequence = self._last_book_sequence
            if prior_sequence is not None and seq and seq < prior_sequence:
                self._sequence_regressions += 1
            if seq:
                self._last_book_sequence = seq
            self._last_book_receive_ns = receive_ns
            self._last_book_exchange_ms = exchange_ms
            self._book_count += 1
        event = {
            "s": self.subscription.symbol,
            "b": str(bid),
            "B": str(bid_size),
            "a": str(ask),
            "A": str(ask_size),
            "E": exchange_ms,
        }
        self.signal.on_book_ticker(
            event,
            market_type=self.market_type,
            venue=BITGET_VENUE,
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
                "transport": "websocket",
                "event_type": "book",
                "exchange_event_ts_ns": exchange_ms * 1_000_000,
                "local_receive_ts_ns": receive_ns,
                "transport_lag_ms": (receive_ns / 1_000_000.0 - exchange_ms)
                if exchange_ms
                else None,
                "sequence_number": seq or None,
                "previous_sequence_number": previous_seq,
                "gap_flag": (
                    previous_seq != prior_sequence
                    if previous_seq is not None and prior_sequence is not None
                    else None
                ),
                "bid": bid,
                "bid_size": bid_size,
                "ask": ask,
                "ask_size": ask_size,
            }
        )

    def _handle_trades(self, rows: list[Any], receive_ns: int) -> None:
        accepted: list[tuple[int | None, int, float, float, str, str]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            exchange_ms = int(row.get("ts", row.get("T", 0)) or 0)
            price = float(row.get("price", row.get("p", 0)) or 0)
            size = float(row.get("size", row.get("v", 0)) or 0)
            side = str(row.get("side", row.get("S", ""))).lower()
            if exchange_ms <= 0 or price <= 0 or size <= 0 or side not in {"buy", "sell"}:
                continue
            trade_id_value = row.get("tradeId", row.get("i"))
            try:
                sequence = int(trade_id_value) if trade_id_value is not None else None
            except (TypeError, ValueError):
                sequence = None
            accepted.append(
                (sequence, exchange_ms, price, size, side, str(trade_id_value or ""))
            )
        if not accepted:
            return
        accepted.sort(key=lambda row: row[1])
        with self._state_lock:
            self._last_trade_receive_ns = receive_ns
            self._last_trade_exchange_ms = max(
                self._last_trade_exchange_ms,
                max(row[1] for row in accepted),
            )
            self._trade_count += len(accepted)
        publish_cross_trade_arrays(
            self.signal,
            symbol=self.subscription.symbol,
            ts_ms=[row[1] for row in accepted],
            prices=[row[2] for row in accepted],
            quantities=[row[3] for row in accepted],
            # Bitget side is taker side. Binance m=true means taker SELL.
            is_buyer_maker=[row[4] == "sell" for row in accepted],
            market_type=self.market_type,
            venue=BITGET_VENUE,
            receive_ts_ns=receive_ns,
            sequence_numbers=[row[0] for row in accepted],
        )
        if self._record_enabled and self.record_trades:
            for sequence, exchange_ms, price, size, side, trade_id in accepted:
                self._recorder.submit(
                    {
                        "market_id": self.market_id,
                        "transport": "websocket",
                        "event_type": "trade",
                        "exchange_event_ts_ns": exchange_ms * 1_000_000,
                        "local_receive_ts_ns": receive_ns,
                        "transport_lag_ms": receive_ns / 1_000_000.0 - exchange_ms,
                        "sequence_number": sequence,
                        "trade_id": trade_id,
                        "price": price,
                        "size": size,
                        "side": side,
                    }
                )

    def _record_error(self, message: str) -> None:
        with self._state_lock:
            self._error_count += 1
            self._last_error = str(message)[:300]
        logger.warning("Bitget public reference error: %s", message)

    def _on_error(self, _ws, error: Any) -> None:
        self._record_error(str(error))

    def _on_close(self, _ws, status_code: Any, message: Any) -> None:
        if self._running:
            logger.warning("Bitget public reference closed: %s %s", status_code, message)
