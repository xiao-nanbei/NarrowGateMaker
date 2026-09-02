"""
WebSocket Handler — 管理所有 WebSocket 连接和事件分发。

连接:
  1. Market stream: aggTrade + partial_book_depth + bookTicker
  2. User data stream: ORDER_TRADE_UPDATE + ACCOUNT_UPDATE

事件路由:
  aggTrade         → signal.on_agg_trade()
  partial_depth    → signal.on_depth()
  bookTicker       → engine.inventory.update_mark_price()
  ORDER_TRADE_UPDATE → engine.orders.on_order_update()
  ACCOUNT_UPDATE   → engine.inventory.sync (optional)
"""

import json
import logging
import math
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode

from execution.active_order_depth_path import (
    ActiveOrderDepthPathState,
    ActiveOrderDepthPathTracker,
)
from live.orderbook.binance_usdm import (
    BinanceUsdMDeepBook,
    DeepLevelState,
)
from live.venues.common import DailyJsonlRecorder
from market_fusion import (
    BINANCE_VENUE,
    PERP_MARKET,
    SPOT_MARKET,
    STABLECOIN_ANCHOR_ROLE,
    build_market_specs,
    market_key,
)

logger = logging.getLogger("ws_handler")

_USER_STREAM_SHUTDOWN_JOIN_TIMEOUT_S = 5.0
_USER_STREAM_STARTUP_READY_TIMEOUT_S = 30.0


@dataclass(frozen=True)
class DynamicFillHazardVisibleSnapshot:
    """One generation-consistent q90 input assembled at a common cutoff."""

    feature_ready_ts_ns: int
    generation: int
    paths: tuple[ActiveOrderDepthPathState, ...]
    deep_book: dict[str, Any]


class WSHandler:
    """
    Manages market data and user data WebSocket streams.

    Uses binance-futures-connector's UMFuturesWebsocketClient.
    """

    def __init__(self, engine, cfg):
        """
        engine: strategy.maker_engine.MakerEngine
        cfg: live.config.Config
        """
        self.engine = engine
        self.cfg = cfg

        self._ws_market = None
        self._ws_public = None
        self._ws_deep = None
        self._ws_spot = None
        self._ws_user = None
        self._user_thread: Optional[threading.Thread] = None
        self._user_restart_thread: Optional[threading.Thread] = None
        self._user_stream_lifecycle_lock = threading.RLock()
        self._user_stream_active = False
        # ``_rest_client`` remains a compatibility alias for older fixtures.
        # Production binds the snapshot and listen-key roles independently.
        self._rest_client = None
        self._market_snapshot_client = None
        self._listen_key_client = None
        self._listen_key: Optional[str] = None
        self._listen_key_thread: Optional[threading.Thread] = None
        self._startup_lock = threading.RLock()
        self._public_market_phase_lock = threading.Lock()
        self._private_user_stream_started = False
        self._public_market_streams_started = False
        self._public_market_streams_starting = False
        self._public_market_startup_closed = False
        # Serialize complete private callbacks.  Startup holds this lock while
        # it reconciles and publishes the prospective epoch, so an event is
        # either wholly represented by the initial state or wholly delivered
        # to the newly attached writer; it can never straddle that boundary.
        self._user_event_callback_lock = threading.RLock()
        self._user_stream_shutdown_join_timeout_s = (
            _USER_STREAM_SHUTDOWN_JOIN_TIMEOUT_S
        )
        self._spot_thread: Optional[threading.Thread] = None
        self._spot_stream_active = False
        self._external_clients: list[object] = []
        self._deep_book: Optional[BinanceUsdMDeepBook] = None
        self._deep_book_lock = threading.Lock()
        self._deep_book_start_lock = threading.Lock()
        self._deep_book_session_id = 0
        self._deep_book_reconnect_requested = False
        self._deep_book_last_maintenance_monotonic = 0.0
        self._deep_book_stale_restart_count = 0
        self._active_order_depth_tracker = ActiveOrderDepthPathTracker()
        self._active_order_depth_lock = threading.Lock()
        self._dynamic_fill_hazard_snapshot_lock = threading.Lock()
        self._dynamic_fill_hazard_snapshot = DynamicFillHazardVisibleSnapshot(
            feature_ready_ts_ns=0,
            generation=0,
            paths=(),
            deep_book={},
        )
        self._market_tape: Optional[DailyJsonlRecorder] = None
        self._market_tape_last_book_ns: dict[str, int] = {}
        self._market_tape_last_depth_sequence: dict[str, int] = {}
        self._running = False

        # Stats
        self._agg_trade_count = 0
        self._exec_trade_count = 0
        self._cross_trade_count = 0
        self._depth_count = 0
        self._deep_depth_count = 0
        self._spot_book_ticker_count = 0
        self._spot_trade_count = 0
        self._user_event_stats_lock = threading.Lock()
        self._user_event_count = 0
        self._last_user_event_monotonic_s = 0.0
        self._user_stream_connected = False
        self._user_stream_generation = 0
        self._user_stream_session_token = 0
        self._user_stream_ready_event = threading.Event()

        self._market_session_id = 0
        self._market_trade_seen: dict[str, float] = {}
        self._market_book_seen: dict[str, float] = {}
        self._market_depth_seen: dict[str, float] = {}
        self._spot_trade_seen: dict[str, float] = {}
        self._spot_book_seen: dict[str, float] = {}
        self._stream_watchdog_interval = 5.0
        self._exec_stream_silence_timeout = 45.0
        self._anchor_stream_silence_timeout = 45.0
        self._apply_stream_timeouts()
        self._market_reconnect_requested = False
        self._market_request_seq = 0
        self._market_request_desc: dict[str, str] = {}

    def _apply_stream_timeouts(self):
        ws_cfg = self.cfg.websocket
        self._exec_stream_silence_timeout = max(
            1.0, float(getattr(ws_cfg, "exec_stream_silence_timeout_s", 45.0))
        )
        self._anchor_stream_silence_timeout = max(
            1.0, float(getattr(ws_cfg, "anchor_stream_silence_timeout_s", 45.0))
        )

    def _futures_ws_root(self) -> str:
        return (
            "wss://stream.binancefuture.com"
            if self.cfg.api.testnet
            else "wss://fstream.binance.com"
        )

    def _market_stream_base_url(self) -> str:
        return f"{self._futures_ws_root()}/market"

    def _public_stream_base_url(self) -> str:
        return f"{self._futures_ws_root()}/public"

    def _private_stream_base_url(self) -> str:
        return f"{self._futures_ws_root()}/private"

    def _private_user_stream_url(self, listen_key: str) -> str:
        query = urlencode(
            {
                "listenKey": listen_key,
                "events": "ORDER_TRADE_UPDATE/ACCOUNT_UPDATE/listenKeyExpired",
            }
        )
        return f"{self._private_stream_base_url()}/ws?{query}"

    def start(
        self,
        rest_client=None,
        *,
        market_snapshot_client=None,
        listen_key_client=None,
    ):
        """
        Start all WebSocket connections.

        ``rest_client`` is the compatibility fallback. Production supplies a
        dedicated public snapshot client and a dedicated listen-key client so
        neither can block order or reconciliation traffic.

        This compatibility entry point preserves the all-stream API while
        enforcing the safe private-before-public startup order.  Startup code
        that must freeze an epoch between those phases should call
        :meth:`start_private_user_stream` and
        :meth:`start_public_market_streams` explicitly.
        """
        market_snapshot_client = market_snapshot_client or rest_client
        listen_key_client = listen_key_client or rest_client
        if market_snapshot_client is None or listen_key_client is None:
            raise ValueError("market snapshot and listen-key clients are required")
        self.start_private_user_stream(
            rest_client,
            listen_key_client=listen_key_client,
        )
        if not self.wait_for_user_stream_ready(_USER_STREAM_STARTUP_READY_TIMEOUT_S):
            raise RuntimeError(
                "private user stream did not become ready before public startup"
            )
        private_state = self.user_event_safety_snapshot()
        self.start_public_market_streams(
            rest_client,
            market_snapshot_client=market_snapshot_client,
            expected_user_stream_generation=int(
                private_state.get("user_stream_generation", 0) or 0
            ),
        )

        logger.info("All WebSocket streams started")

    def _bind_compatibility_rest_client(self, rest_client) -> None:
        if rest_client is None:
            return
        if (
            self._rest_client is not None
            and self._rest_client is not rest_client
            and (
                self._private_user_stream_started
                or self._public_market_streams_started
            )
        ):
            raise RuntimeError("WebSocket compatibility REST client is already bound")
        self._rest_client = rest_client

    def _ensure_listen_key_renewal_thread(self, listen_key_client) -> None:
        thread = self._listen_key_thread
        if thread is not None and thread.is_alive():
            return
        self._listen_key_thread = threading.Thread(
            target=self._listen_key_renewal_loop,
            args=(listen_key_client,),
            daemon=True,
            name="listen-key-renewal",
        )
        self._listen_key_thread.start()

    def start_private_user_stream(
        self,
        rest_client=None,
        *,
        listen_key_client=None,
    ) -> None:
        """Start only the private user stream and listen-key renewal.

        No public, deep-book, spot, external-venue, or market-tape component is
        touched.  This lets startup establish and admit the private execution
        stream before a prospective epoch is frozen and before the first
        market event can reach the signal graph.
        """

        listen_key_client = listen_key_client or rest_client
        if listen_key_client is None:
            raise ValueError("listen-key client is required")
        with self._startup_lock:
            self._bind_compatibility_rest_client(rest_client)
            if self._private_user_stream_started:
                if self._listen_key_client is not listen_key_client:
                    raise RuntimeError("listen-key client is already bound")
                self._ensure_listen_key_renewal_thread(listen_key_client)
                return

            self._running = True
            self._listen_key_client = listen_key_client
            self._private_user_stream_started = True
            try:
                logger.info("Starting user data WebSocket...")
                if not self._start_user_stream(listen_key_client):
                    raise RuntimeError("user data WebSocket failed to launch")
                self._ensure_listen_key_renewal_thread(listen_key_client)
            except BaseException:
                self._private_user_stream_started = False
                try:
                    self._stop_user_stream()
                except BaseException:
                    logger.critical(
                        "User-data WebSocket cleanup after startup failure failed",
                        exc_info=True,
                    )
                if not self._public_market_streams_started:
                    self._running = False
                raise
            logger.info("Private user WebSocket stream started")

    def start_public_market_streams(
        self,
        rest_client=None,
        *,
        market_snapshot_client=None,
        expected_user_stream_generation: int,
    ) -> None:
        """Start public market, deep-book, spot, and external data sources."""

        from binance.websocket.um_futures.websocket_client import (
            UMFuturesWebsocketClient,
        )

        market_snapshot_client = market_snapshot_client or rest_client
        if market_snapshot_client is None:
            raise ValueError("market snapshot client is required")
        with self._startup_lock:
            self._bind_compatibility_rest_client(rest_client)
            if not self._private_user_stream_started:
                raise RuntimeError(
                    "private user stream must start before public market streams"
                )
            private_state = self.user_event_safety_snapshot()
            current_generation = int(
                private_state.get("user_stream_generation", 0) or 0
            )
            if (
                not bool(private_state.get("user_stream_connected"))
                or current_generation <= 0
                or current_generation != int(expected_user_stream_generation)
            ):
                raise RuntimeError(
                    "public market streams require the admitted private-stream "
                    "generation"
                )
            with self._public_market_phase_lock:
                if self._public_market_streams_started:
                    if self._market_snapshot_client is not market_snapshot_client:
                        raise RuntimeError("market snapshot client is already bound")
                    return
                if self._public_market_streams_starting:
                    raise RuntimeError(
                        "public market stream startup is already in progress"
                    )
                self._public_market_streams_starting = True
                self._public_market_startup_closed = False

            self._running = True
            self._market_snapshot_client = market_snapshot_client
            deep_book_started = False
            try:
                self._start_market_tape()
                symbol = self.cfg.symbol.lower()
                market_symbols = self._market_symbols()
                spot_symbols = self._spot_anchor_symbols()

                logger.info("Starting market trade WebSocket...")
                self._ws_market = UMFuturesWebsocketClient(
                    stream_url=self._market_stream_base_url(),
                    on_message=self._on_market_message,
                    on_close=self._on_market_close,
                )
                logger.info("Starting public data WebSocket...")
                self._ws_public = UMFuturesWebsocketClient(
                    stream_url=self._public_stream_base_url(),
                    on_message=self._on_market_message,
                    on_close=self._on_public_close,
                )
                self._market_session_id += 1
                self._reset_stream_watchdog_state(market_symbols, spot_symbols)

                self._subscribe_market_streams(symbol, market_symbols)
                self._subscribe_public_streams(symbol, market_symbols)
                deep_book_started = bool(self._start_deep_book_stream())
                self._start_spot_stream(spot_symbols)
                self._start_external_venue_streams()
                self._arm_stream_silence_watchdog(
                    market_symbols, spot_symbols, self._market_session_id
                )
                final_private_state = self.user_event_safety_snapshot()
                if (
                    not bool(final_private_state.get("user_stream_connected"))
                    or int(
                        final_private_state.get("user_stream_generation", 0) or 0
                    )
                    != int(expected_user_stream_generation)
                ):
                    raise RuntimeError(
                        "private user stream changed during public market startup"
                    )
                # Commit STARTING -> ACTIVE under the same short lock used by
                # close callbacks.  A close either wins first and makes startup
                # fail, or observes ACTIVE and runs the normal reconnect path.
                # No network stop/join operation is ever performed under this
                # phase lock.
                with self._public_market_phase_lock:
                    if self._public_market_startup_closed:
                        raise RuntimeError(
                            "public market WebSocket closed during startup"
                        )
                    self._public_market_streams_starting = False
                    self._public_market_streams_started = True
            except BaseException:
                with self._public_market_phase_lock:
                    self._public_market_streams_starting = False
                    self._public_market_streams_started = False
                    self._public_market_startup_closed = False
                self._market_session_id += 1
                self._stop_external_venue_streams()
                self._stop_market_tape()
                self._stop_deep_book_stream()
                for attr_name in ("_ws_market", "_ws_public"):
                    client = getattr(self, attr_name)
                    if client is not None:
                        try:
                            client.stop()
                        except Exception:
                            pass
                        setattr(self, attr_name, None)
                self._stop_spot_stream()
                if not self._private_user_stream_started:
                    self._running = False
                raise
            if not deep_book_started:
                self._schedule_deep_book_reconnect()
            logger.info("Public market WebSocket streams started")

    @contextmanager
    def hold_user_event_callbacks(self):
        """Hold complete private callbacks across a startup state boundary."""

        with self._user_event_callback_lock:
            yield

    def _deep_book_enabled(self) -> bool:
        return bool(getattr(self.cfg.websocket, "deep_book_enabled", False))

    def _start_deep_book_stream(self) -> bool:
        """Start the independent execution-symbol diff-depth pipeline."""
        with self._deep_book_start_lock:
            self._stop_deep_book_stream()
            if not self._running or not self._deep_book_enabled():
                return True
            snapshot_client = self._market_snapshot_client or self._rest_client
            if snapshot_client is None:
                logger.error("Deep book cannot start before REST client is available")
                return False

            from binance.websocket.um_futures.websocket_client import (
                UMFuturesWebsocketClient,
            )

            ws_cfg = self.cfg.websocket
            book = BinanceUsdMDeepBook(
                snapshot_client,
                symbol=self.cfg.symbol,
                tick_size=self.cfg.tick_size,
                snapshot_levels=int(ws_cfg.deep_book_snapshot_levels),
                max_buffer_events=int(ws_cfg.deep_book_max_buffer_events),
                resync_backoff_s=float(ws_cfg.deep_book_resync_backoff_s),
            )
            with self._deep_book_lock:
                self._deep_book_session_id += 1
                session_id = self._deep_book_session_id
                self._deep_book = book

            try:
                client = UMFuturesWebsocketClient(
                    stream_url=self._public_stream_base_url(),
                    on_message=self._on_deep_book_message,
                    on_close=lambda *_args: self._on_deep_book_close(session_id),
                )
                with self._deep_book_lock:
                    stale_start = (
                        session_id != self._deep_book_session_id
                        or not self._running
                    )
                    if not stale_start:
                        self._ws_deep = client
                if stale_start:
                    client.stop()
                    return False
                request_id = self._next_market_request_id(
                    f"SUBSCRIBE {self.cfg.symbol.lower()}@depth"
                    f"@{int(ws_cfg.deep_book_speed)}ms"
                )
                client.diff_book_depth(
                    symbol=self.cfg.symbol.lower(),
                    speed=int(ws_cfg.deep_book_speed),
                    id=request_id,
                )
                book.start()
                logger.info(
                    "Deep book requested: symbol=%s snapshot=%d diff=%dms",
                    self.cfg.symbol.upper(),
                    int(ws_cfg.deep_book_snapshot_levels),
                    int(ws_cfg.deep_book_speed),
                )
                return True
            except Exception as exc:
                logger.error("Deep book startup failed: %s", exc)
                self._stop_deep_book_stream()
                return False

    def _stop_deep_book_stream(self) -> None:
        with self._deep_book_lock:
            self._deep_book_session_id += 1
            client = self._ws_deep
            book = self._deep_book
            self._ws_deep = None
            self._deep_book = None
        if book is not None:
            book.stop()
        with self._active_order_depth_lock:
            self._active_order_depth_tracker.reset()
        if client is not None:
            try:
                client.stop()
            except Exception:
                pass

    def _schedule_deep_book_reconnect(self) -> None:
        if (
            not self._running
            or not self._public_market_streams_started
            or not self._deep_book_enabled()
        ):
            return
        with self._deep_book_lock:
            if self._deep_book_reconnect_requested:
                return
            self._deep_book_reconnect_requested = True

        def _reconnect() -> None:
            try:
                while (
                    self._running
                    and self._public_market_streams_started
                    and self._deep_book_enabled()
                ):
                    time.sleep(2.0)
                    if self._start_deep_book_stream():
                        return
            finally:
                with self._deep_book_lock:
                    self._deep_book_reconnect_requested = False
                    reconnect_needed = bool(
                        self._running
                        and self._public_market_streams_started
                        and self._deep_book_enabled()
                        and self._ws_deep is None
                    )
                if reconnect_needed:
                    self._schedule_deep_book_reconnect()

        threading.Thread(
            target=_reconnect,
            daemon=True,
            name="binance-deep-book-reconnect",
        ).start()

    def _on_deep_book_close(self, session_id: int) -> None:
        with self._deep_book_lock:
            if session_id != self._deep_book_session_id:
                return
            book = self._deep_book
            self._ws_deep = None
        if book is not None:
            book.invalidate("diff-depth websocket closed")
        if self._running and self._public_market_streams_started:
            logger.warning("Deep-book WebSocket closed; scheduling reconnect")
            self._schedule_deep_book_reconnect()

    def maintain_deep_book(self, *, now_ns: Optional[int] = None) -> None:
        """Recover a stale deep stream without touching the strategy feed."""
        if not self._running or not self._deep_book_enabled():
            return
        monotonic_now = time.monotonic()
        if (
            self._deep_book_last_maintenance_monotonic > 0.0
            and monotonic_now - self._deep_book_last_maintenance_monotonic < 1.0
        ):
            return
        self._deep_book_last_maintenance_monotonic = monotonic_now

        snapshot = self.deep_book_snapshot(now_ns=now_ns)
        if snapshot["syncing"]:
            return
        if not snapshot["stale"]:
            return

        with self._deep_book_lock:
            book = self._deep_book
            reconnect_pending = self._deep_book_reconnect_requested
        if book is not None:
            book.invalidate("diff-depth stream stale")
        if reconnect_pending:
            return
        self._deep_book_stale_restart_count += 1
        logger.warning(
            "Deep-book stream stale: age_ms=%.1f; scheduling independent reconnect",
            float(snapshot["age_ms"]),
        )
        self._schedule_deep_book_reconnect()

    def _on_deep_book_message(self, _, message) -> None:
        """Apply deep diff events without touching top-20 strategy features."""
        receive_ns = time.time_ns()
        try:
            if isinstance(message, (bytes, bytearray)):
                message = message.decode("utf-8", errors="ignore")
            data = json.loads(message) if isinstance(message, str) else message
            if not isinstance(data, dict):
                return
            if "stream" in data and "data" in data:
                data = data["data"]
            if not isinstance(data, dict):
                return
            if "error" in data:
                logger.error("Deep-book stream request failed: %s", data["error"])
                with self._deep_book_lock:
                    book = self._deep_book
                if book is not None:
                    book.invalidate(f"stream request failed: {data['error']}")
                self._schedule_deep_book_reconnect()
                return
            if data.get("e") != "depthUpdate":
                return
            with self._deep_book_lock:
                book = self._deep_book
            if book is None:
                return
            self._deep_depth_count += 1
            book.on_diff_event(data, receive_ts_ns=receive_ns)
        except Exception as exc:
            logger.error("Deep-book message error: %s", exc)

    def deep_book_level_state(
        self,
        side: str,
        price: float,
        *,
        now_ns: Optional[int] = None,
    ) -> Optional[DeepLevelState]:
        with self._deep_book_lock:
            book = self._deep_book
        if book is None:
            return None
        return book.level_state(
            side,
            price,
            now_ns=now_ns,
            max_age_ms=float(self.cfg.websocket.deep_book_max_age_s) * 1_000.0,
        )

    def maintain_active_order_depth_paths(
        self,
        *,
        now_ns: Optional[int] = None,
    ) -> tuple[ActiveOrderDepthPathState, ...]:
        """Update exact-level shadow paths for currently active orders."""

        ready_ns = int(now_ns if now_ns is not None else time.time_ns())
        if not self._deep_book_enabled():
            with self._active_order_depth_lock:
                self._active_order_depth_tracker.reset()
            with self._dynamic_fill_hazard_snapshot_lock:
                self._dynamic_fill_hazard_snapshot = (
                    DynamicFillHazardVisibleSnapshot(
                        feature_ready_ts_ns=ready_ns,
                        generation=0,
                        paths=(),
                        deep_book={
                            "enabled": 0,
                            "valid": 0,
                            "feature_ready_ts_ns": ready_ns,
                        },
                    )
                )
            return ()
        orders = self.engine.orders.get_active_orders()
        with self._deep_book_lock:
            book = self._deep_book
        if book is None:
            with self._active_order_depth_lock:
                self._active_order_depth_tracker.reset()
            paths: tuple[ActiveOrderDepthPathState, ...] = ()
            deep_book = {
                "enabled": 1,
                "valid": 0,
                "feature_ready_ts_ns": ready_ns,
                "last_error": "not_started",
            }
            generation = 0
        else:
            max_age_ms = (
                float(self.cfg.websocket.deep_book_max_age_s) * 1_000.0
            )
            with book.atomic_read():
                deep_book = book.snapshot(
                    now_ns=ready_ns,
                    max_age_ms=max_age_ms,
                )
                with self._active_order_depth_lock:
                    paths = self._active_order_depth_tracker.sync(
                        orders,
                        level_state=lambda side, price: book.level_state(
                            side,
                            price,
                            now_ns=ready_ns,
                            max_age_ms=max_age_ms,
                        ),
                        feature_ready_ts_ns=ready_ns,
                    )
                generation = int(deep_book.get("generation", 0) or 0)
        snapshot = DynamicFillHazardVisibleSnapshot(
            feature_ready_ts_ns=ready_ns,
            generation=generation,
            paths=tuple(paths),
            deep_book=dict(deep_book),
        )
        with self._dynamic_fill_hazard_snapshot_lock:
            self._dynamic_fill_hazard_snapshot = snapshot
        return tuple(paths)

    def dynamic_fill_hazard_visible_snapshot(
        self,
    ) -> DynamicFillHazardVisibleSnapshot:
        with self._dynamic_fill_hazard_snapshot_lock:
            return self._dynamic_fill_hazard_snapshot

    def active_order_depth_states(
        self,
    ) -> tuple[ActiveOrderDepthPathState, ...]:
        with self._active_order_depth_lock:
            return self._active_order_depth_tracker.snapshot()

    def retain_active_order_depth_path(self, client_order_id: str) -> bool:
        """Fail fast for the retired post-terminal path-retention contract."""

        with self._active_order_depth_lock:
            return self._active_order_depth_tracker.retain(client_order_id)

    def release_active_order_depth_path(self, client_order_id: str) -> None:
        with self._active_order_depth_lock:
            self._active_order_depth_tracker.release(client_order_id)

    def terminal_active_order_depth_path(self, client_order_id: str) -> None:
        """Remove an exchange-terminal order from the fill-risk path set."""

        with self._active_order_depth_lock:
            self._active_order_depth_tracker.discard(client_order_id)

    def active_order_depth_snapshot(self) -> dict:
        with self._active_order_depth_lock:
            states = self._active_order_depth_tracker.snapshot()
            retained = self._active_order_depth_tracker.retained_count()
            return {
                "tracked": len(states),
                "retained": retained,
                "valid": sum(int(state.valid) for state in states),
                "ambiguous": sum(int(state.ambiguous) for state in states),
                "uncovered": sum(int(not state.covered) for state in states),
                "invalid": sum(int(not state.valid) for state in states),
                "max_age_ms": max(
                    (float(state.age_ms) for state in states),
                    default=0.0,
                ),
            }

    def deep_book_snapshot(self, *, now_ns: Optional[int] = None) -> dict:
        with self._deep_book_lock:
            book = self._deep_book
        if book is None:
            return {
                "enabled": int(self._deep_book_enabled()),
                "valid": 0,
                "stale": 1,
                "syncing": 0,
                "symbol": self.cfg.symbol.upper(),
                "generation": 0,
                "last_update_id": 0,
                "age_ms": float("inf"),
                "bid_levels": 0,
                "ask_levels": 0,
                "best_bid": 0.0,
                "best_bid_qty": 0.0,
                "best_ask": 0.0,
                "best_ask_qty": 0.0,
                "buffer_events": 0,
                "gap_count": 0,
                "resync_count": 0,
                "ignored_events": 0,
                "buffer_overflow_count": 0,
                "trade_count": 0,
                "last_trade_receive_ts_ns": 0,
                "last_trade_feature_ready_ts_ns": 0,
                "last_trade_exchange_ts_ns": 0,
                "last_receive_ts_ns": 0,
                "feature_ready_ts_ns": int(
                    now_ns if now_ns is not None else time.time_ns()
                ),
                "stale_restart_count": int(
                    self._deep_book_stale_restart_count
                ),
                "last_error": "disabled" if not self._deep_book_enabled() else "not_started",
            }
        snapshot = book.snapshot(
            now_ns=now_ns,
            max_age_ms=float(self.cfg.websocket.deep_book_max_age_s) * 1_000.0,
        )
        snapshot["stale_restart_count"] = int(
            self._deep_book_stale_restart_count
        )
        return snapshot

    def _external_source_configs(self) -> list[object]:
        external = getattr(self.cfg, "external_venues", None)
        if not getattr(external, "enabled", False):
            return []
        return [source for source in getattr(external, "sources", []) if getattr(source, "enabled", False)]

    def _start_external_venue_streams(self):
        self._stop_external_venue_streams()
        sources = self._external_source_configs()
        if not sources:
            return
        from live.venues import (
            BitgetPublicReferenceClient,
            BybitPublicRestReferenceClient,
            BybitPublicWebSocketReferenceClient,
            OkxPublicRestReferenceClient,
            OkxPublicWebSocketReferenceClient,
        )

        project_root = Path(__file__).resolve().parent.parent
        for source in sources:
            venue = str(getattr(source, "venue", "")).strip().lower()
            websocket_transport = False
            if venue == "bitget":
                client_cls = BitgetPublicReferenceClient
                websocket_transport = True
            elif venue == "bybit":
                transport = str(getattr(source, "transport", "rest")).strip().lower()
                websocket_transport = transport == "websocket"
                client_cls = (
                    BybitPublicWebSocketReferenceClient
                    if websocket_transport
                    else BybitPublicRestReferenceClient
                )
            elif venue == "okx":
                transport = str(getattr(source, "transport", "rest")).strip().lower()
                websocket_transport = transport == "websocket"
                client_cls = (
                    OkxPublicWebSocketReferenceClient
                    if websocket_transport
                    else OkxPublicRestReferenceClient
                )
            else:
                logger.warning("Skipping unsupported external venue: %s", venue)
                continue
            client = client_cls(self.engine.signal, source, project_root=project_root)
            client.start()
            self._external_clients.append(client)
        if self._external_clients:
            logger.info(
                "External venue shadow streams started: %s",
                ", ".join(client.market_id for client in self._external_clients),
            )

    def _stop_external_venue_streams(self):
        clients = self._external_clients
        self._external_clients = []
        stop_threads: list[threading.Thread] = []

        def stop_client(client: object) -> None:
            try:
                client.stop()
            except Exception as exc:
                logger.warning("External venue stop failed: %s", exc)

        # Close all transports together.  Serial stop can spend one WebSocket
        # join timeout per venue and exceed the process shutdown grace period.
        for client in clients:
            thread = threading.Thread(
                target=stop_client,
                args=(client,),
                daemon=True,
                name=f"external-stop-{getattr(client, 'market_id', 'unknown')}",
            )
            thread.start()
            stop_threads.append(thread)
        deadline = time.monotonic() + 4.0
        for thread in stop_threads:
            thread.join(timeout=max(0.0, deadline - time.monotonic()))
        unfinished = [thread.name for thread in stop_threads if thread.is_alive()]
        if unfinished:
            logger.warning(
                "External venue stop exceeded 4s grace period: %s",
                ", ".join(unfinished),
            )

    def external_venue_snapshot(self) -> list[dict]:
        snapshots = []
        for client in self._external_clients:
            try:
                snapshots.append(client.snapshot())
            except Exception as exc:
                snapshots.append({"enabled": 1, "stale": 1, "last_error": str(exc)})
        return snapshots

    def _start_market_tape(self) -> None:
        self._stop_market_tape()
        cfg = self.cfg.logging
        if not bool(getattr(cfg, "market_tape_enabled", False)):
            return
        root = Path(str(getattr(cfg, "market_tape_dir", "logs/market_tape")))
        if not root.is_absolute():
            root = Path(__file__).resolve().parent.parent / root
        self._market_tape = DailyJsonlRecorder(
            root,
            file_prefix="binance_receive_tape",
            thread_name="binance-market-tape-writer",
            queue_size=max(1, int(getattr(cfg, "market_tape_queue_size", 500_000))),
        )
        self._market_tape.start()
        self._market_tape_last_book_ns.clear()
        self._market_tape_last_depth_sequence.clear()
        logger.info("Binance receive-time market tape enabled: %s", root)

    def _stop_market_tape(self) -> None:
        recorder = self._market_tape
        self._market_tape = None
        if recorder is not None:
            recorder.stop()

    def market_tape_snapshot(self) -> dict:
        recorder = self._market_tape
        if recorder is None:
            return {
                "enabled": 0,
                "submitted": 0,
                "written": 0,
                "dropped": 0,
                "invalid": 0,
                "queue_depth": 0,
                "queue_capacity": 0,
                "queue_high_watermark": 0,
                "queue_age_ms": 0.0,
                "last_queue_age_ms": 0.0,
                "max_queue_age_ms": 0.0,
            }
        return {"enabled": 1, **recorder.snapshot()}

    def _record_binance_market_event(
        self,
        data: dict,
        *,
        market_type: str,
        event_type: str,
        receive_ts_ns: int,
        feature_ready_ts_ns: int,
    ) -> None:
        recorder = self._market_tape
        if recorder is None:
            return
        log_cfg = self.cfg.logging
        event_type = str(event_type).lower()
        if event_type == "book" and not bool(
            getattr(log_cfg, "market_tape_record_books", True)
        ):
            return
        if event_type == "trade" and not bool(
            getattr(log_cfg, "market_tape_record_trades", True)
        ):
            return
        if event_type == "depth" and not bool(
            getattr(log_cfg, "market_tape_record_depth", False)
        ):
            return

        symbol = str(data.get("s", self.cfg.symbol)).strip().upper()
        if not symbol:
            return
        source_id = market_key(BINANCE_VENUE, market_type, symbol)
        if event_type == "book":
            interval_ms = max(
                0.0, float(getattr(log_cfg, "market_tape_book_interval_ms", 0.0))
            )
            previous_receive = self._market_tape_last_book_ns.get(source_id, 0)
            if interval_ms > 0.0 and receive_ts_ns - previous_receive < int(
                interval_ms * 1_000_000
            ):
                return
            self._market_tape_last_book_ns[source_id] = receive_ts_ns

        exchange_ms = int(
            data.get("T", 0)
            or data.get("E", 0)
            or data.get("ts", 0)
            or 0
        )
        sequence_raw = (
            data.get("u")
            or data.get("lastUpdateId")
            or data.get("a")
            or data.get("t")
        )
        try:
            sequence = int(sequence_raw) if sequence_raw is not None else None
        except (TypeError, ValueError):
            sequence = None

        previous_sequence = None
        gap_flag = None
        if event_type == "depth" and sequence is not None:
            previous_sequence = self._market_tape_last_depth_sequence.get(source_id)
            declared_previous = data.get("pu")
            try:
                declared_previous = (
                    int(declared_previous) if declared_previous is not None else None
                )
            except (TypeError, ValueError):
                declared_previous = None
            if previous_sequence is not None and declared_previous is not None:
                gap_flag = int(declared_previous != previous_sequence)
            elif previous_sequence is not None:
                gap_flag = int(sequence <= previous_sequence)
            self._market_tape_last_depth_sequence[source_id] = sequence

        row = {
            "market_id": source_id,
            "transport": "websocket",
            "event_type": event_type,
            "exchange_event_ts_ns": exchange_ms * 1_000_000,
            "local_receive_ts_ns": int(receive_ts_ns),
            "feature_ready_ts_ns": int(feature_ready_ts_ns),
            "sequence_number": sequence,
            "previous_sequence_number": previous_sequence,
            "gap_flag": gap_flag,
            "event_timestamp_source": str(
                data.get("_event_timestamp_source", "exchange" if exchange_ms else "missing")
            ),
        }
        if event_type == "book":
            row.update(
                {
                    "bid": float(data.get("b", 0.0) or 0.0),
                    "bid_size": float(data.get("B", 0.0) or 0.0),
                    "ask": float(data.get("a", 0.0) or 0.0),
                    "ask_size": float(data.get("A", 0.0) or 0.0),
                }
            )
        elif event_type == "trade":
            is_buyer_maker = bool(data.get("m", False))
            source_event_type = str(data.get("e", "")).strip()
            is_aggregate = source_event_type == "aggTrade"
            first_trade_id = (
                int(data["f"])
                if is_aggregate and data.get("f") is not None
                else None
            )
            last_trade_id = (
                int(data["l"])
                if is_aggregate and data.get("l") is not None
                else None
            )
            id_range_count = (
                last_trade_id - first_trade_id + 1
                if first_trade_id is not None
                and last_trade_id is not None
                and last_trade_id >= first_trade_id
                else None
            )
            row.update(
                {
                    "trade_id": str(data.get("a", data.get("t", "")) or ""),
                    "trade_stream_type": (
                        "aggregate" if is_aggregate else "untyped"
                    ),
                    "trade_payload_schema_version": (
                        "binance_usdm_aggtrade.v2"
                        if is_aggregate and market_type == PERP_MARKET
                        else "binance_spot_aggtrade.v2"
                        if is_aggregate
                        else "unknown"
                    ),
                    "trade_source_contract_id": (
                        "binance_usdm_public_aggtrade_receive_time.v1"
                        if is_aggregate and market_type == PERP_MARKET
                        else "binance_spot_public_aggtrade_receive_time.v1"
                        if is_aggregate
                        else "unknown"
                    ),
                    "aggregate_trade_id": (
                        int(data["a"])
                        if is_aggregate and data.get("a") is not None
                        else None
                    ),
                    "first_trade_id": first_trade_id,
                    "last_trade_id": last_trade_id,
                    "individual_trade_count_from_id_range": id_range_count,
                    "individual_trade_count_semantics": (
                        "derived_id_range_not_receive_time_individual_events"
                        if id_range_count is not None
                        else "unavailable"
                    ),
                    "price": float(data.get("p", 0.0) or 0.0),
                    "size": float(data.get("q", 0.0) or 0.0),
                    "normal_quantity": (
                        float(data["nq"])
                        if is_aggregate and data.get("nq") is not None
                        else None
                    ),
                    "aggressor_side": "sell" if is_buyer_maker else "buy",
                }
            )
        else:
            bids = data.get("b", []) or []
            asks = data.get("a", []) or []
            row.update(
                {
                    "bid": float(bids[0][0]) if bids else None,
                    "bid_size": float(bids[0][1]) if bids else None,
                    "ask": float(asks[0][0]) if asks else None,
                    "ask_size": float(asks[0][1]) if asks else None,
                    "bids": bids,
                    "asks": asks,
                }
            )
        recorder.submit(row)

    def _market_symbols(self) -> list[str]:
        multi = getattr(self.cfg, "multi_market", None)
        if not getattr(multi, "enabled", False):
            return [self.cfg.symbol.lower()]

        specs = build_market_specs(
            self.cfg.symbol,
            getattr(multi, "market_stage", "minimal"),
            getattr(multi, "reference_symbol", None),
        )
        symbols = []
        for spec in specs:
            if spec.market_type != PERP_MARKET:
                continue
            sym = spec.symbol.lower()
            if sym not in symbols:
                symbols.append(sym)
        return symbols or [self.cfg.symbol.lower()]

    def _spot_anchor_symbols(self) -> list[str]:
        multi = getattr(self.cfg, "multi_market", None)
        if not getattr(multi, "enabled", False):
            return []

        stage = str(getattr(multi, "market_stage", "minimal") or "minimal").lower()
        if stage not in {"enhanced", "full"}:
            return []

        specs = build_market_specs(
            self.cfg.symbol,
            stage,
            getattr(multi, "reference_symbol", None),
            getattr(multi, "stablecoin_anchor_symbol", "USDCUSDT"),
        )
        symbols = []
        for spec in specs:
            if spec.market_type != SPOT_MARKET:
                continue
            sym = spec.symbol.lower()
            if sym not in symbols:
                symbols.append(sym)
        return symbols

    def _spot_trade_anchor_symbols(self) -> list[str]:
        """Spot symbols whose trades feed features; FX anchors need BBO only."""
        multi = getattr(self.cfg, "multi_market", None)
        if not getattr(multi, "enabled", False):
            return []
        stage = str(getattr(multi, "market_stage", "minimal") or "minimal").lower()
        if stage not in {"enhanced", "full"}:
            return []
        specs = build_market_specs(
            self.cfg.symbol,
            stage,
            getattr(multi, "reference_symbol", None),
            getattr(multi, "stablecoin_anchor_symbol", "USDCUSDT"),
        )
        return [
            spec.symbol.lower()
            for spec in specs
            if spec.market_type == SPOT_MARKET
            and spec.role != STABLECOIN_ANCHOR_ROLE
        ]

    def _ordered_market_symbols(self, symbol: str, market_symbols: list[str]) -> list[str]:
        ordered_market_symbols = []
        seen_symbols = set()
        for market_symbol in [symbol, *market_symbols]:
            market_key = self._stream_key(market_symbol)
            if not market_key or market_key in seen_symbols:
                continue
            seen_symbols.add(market_key)
            ordered_market_symbols.append(market_key)
        return ordered_market_symbols

    def _subscribe_market_streams(self, symbol: str, market_symbols: list[str]):
        """Subscribe the required USD-M aggTrade streams."""
        ordered_market_symbols = self._ordered_market_symbols(symbol, market_symbols)

        for index, market_symbol in enumerate(ordered_market_symbols):
            try:
                request_id = self._next_market_request_id(
                    f"SUBSCRIBE {market_symbol}@aggTrade"
                )
                self._ws_market.agg_trade(symbol=market_symbol, id=request_id)
                if market_symbol == symbol:
                    logger.info(
                        f"Subscribe requested id={request_id}: {market_symbol}@aggTrade"
                    )
                else:
                    logger.info(
                        f"Subscribe requested id={request_id}: {market_symbol}@aggTrade (reference)"
                    )
                if index == 0 and len(ordered_market_symbols) > 1:
                    time.sleep(0.1)
            except Exception as e:
                if market_symbol == symbol:
                    logger.error(f"Subscribe aggTrade failed ({market_symbol}): {e}")
                else:
                    logger.error(
                        f"Subscribe reference aggTrade failed ({market_symbol}): {e}"
                    )

        try:
            request_id = self._next_market_request_id("LIST_SUBSCRIPTIONS market")
            self._ws_market.list_subscribe(id=request_id)
            logger.info(f"Requested market subscription snapshot id={request_id}")
        except Exception as e:
            logger.debug(f"LIST_SUBSCRIPTIONS request failed: {e}")

    def _subscribe_public_streams(self, symbol: str, market_symbols: list[str]):
        """Subscribe futures public streams on Binance's /public endpoint."""
        ordered_market_symbols = self._ordered_market_symbols(symbol, market_symbols)

        request_id = self._next_market_request_id(
            f"SUBSCRIBE {symbol}@depth{self.cfg.websocket.depth_levels}@{self.cfg.websocket.depth_speed}ms"
        )
        self._ws_public.partial_book_depth(
            symbol=symbol,
            level=self.cfg.websocket.depth_levels,
            speed=self.cfg.websocket.depth_speed,
            id=request_id,
        )
        logger.info(
            f"Subscribed: {symbol}@depth{self.cfg.websocket.depth_levels}"
            f"@{self.cfg.websocket.depth_speed}ms"
        )

        for market_symbol in ordered_market_symbols:
            request_id = self._next_market_request_id(
                f"SUBSCRIBE {market_symbol}@bookTicker"
            )
            self._ws_public.book_ticker(symbol=market_symbol, id=request_id)
            logger.info(f"Subscribed: {market_symbol}@bookTicker")

        try:
            request_id = self._next_market_request_id("LIST_SUBSCRIPTIONS public")
            self._ws_public.list_subscribe(id=request_id)
            logger.info(f"Requested public subscription snapshot id={request_id}")
        except Exception as e:
            logger.debug(f"Public LIST_SUBSCRIPTIONS request failed: {e}")

    @staticmethod
    def _request_key(request_id) -> str:
        return str(request_id) if request_id is not None else ""

    def _next_market_request_id(self, description: str) -> int:
        self._market_request_seq += 1
        request_id = self._market_request_seq
        self._market_request_desc[self._request_key(request_id)] = description
        return request_id

    def _pop_market_request_desc(self, request_id) -> Optional[str]:
        return self._market_request_desc.pop(self._request_key(request_id), None)

    def _reset_stream_watchdog_state(
        self, market_symbols: list[str], spot_symbols: list[str]
    ):
        now = time.time()
        market_symbols = [self._stream_key(s) for s in market_symbols if s]
        spot_symbols = [self._stream_key(s) for s in spot_symbols if s]

        self._market_trade_seen = {symbol: now for symbol in market_symbols}

        self._market_book_seen = {symbol: now for symbol in market_symbols}
        self._market_depth_seen = {symbol: now for symbol in market_symbols}

        spot_trade_symbols = {
            self._stream_key(symbol)
            for symbol in self._spot_trade_anchor_symbols()
            if symbol
        }
        self._spot_trade_seen = {symbol: now for symbol in spot_trade_symbols}
        self._spot_book_seen = {symbol: now for symbol in spot_symbols}

    def _spot_stream_base_url(self) -> str:
        return "wss://testnet.binance.vision" if self.cfg.api.testnet else "wss://stream.binance.com:9443"

    def _start_spot_stream(self, symbols: list[str]):
        if not symbols:
            return

        try:
            import websocket
        except Exception as exc:
            logger.warning(f"Spot anchor stream disabled: websocket-client unavailable ({exc})")
            return

        trade_symbols = set(self._spot_trade_anchor_symbols())
        streams = "/".join([
            *(f"{symbol}@bookTicker" for symbol in symbols),
            *(f"{symbol}@aggTrade" for symbol in symbols if symbol in trade_symbols),
        ])
        url = f"{self._spot_stream_base_url()}/stream?streams={streams}"
        self._spot_stream_active = True

        def on_message(ws, message):
            self._on_spot_message(message)

        def on_error(ws, error):
            logger.warning(f"Spot anchor WebSocket error: {error}")

        def on_close(ws, status_code, message):
            if self._running and self._spot_stream_active:
                logger.warning(f"Spot anchor WebSocket closed: {status_code} {message}")

        def run():
            while self._running and self._spot_stream_active:
                ws = websocket.WebSocketApp(
                    url,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                )
                self._ws_spot = ws
                ws.run_forever(ping_interval=20, ping_timeout=10)
                if self._running and self._spot_stream_active:
                    time.sleep(5)

        self._spot_thread = threading.Thread(target=run, daemon=True)
        self._spot_thread.start()
        logger.info(
            "Subscribed spot anchors: bookTicker=%s aggTrade=%s",
            ",".join(symbols),
            ",".join(sorted(trade_symbols)),
        )

    def _stop_spot_stream(self):
        self._spot_stream_active = False
        if self._ws_spot:
            try:
                self._ws_spot.close()
            except Exception:
                pass
            self._ws_spot = None

    @staticmethod
    def _stream_key(symbol: str) -> str:
        return str(symbol or "").strip().lower()

    def _arm_stream_silence_watchdog(
        self, market_symbols: list[str], spot_symbols: list[str], session_id: int
    ):
        """Reconnect only when the execution-book transport becomes silent.

        ``aggTrade`` and ``bookTicker`` are event-driven feeds: silence can mean
        that the market simply did not trade or the BBO did not change.  Those
        clocks still drive feature freshness, but they cannot establish a dead
        socket.  The execution partial-depth stream is periodic and therefore
        remains the reconnect authority.  Auxiliary reference feeds degrade in
        their consumers instead of restarting unrelated futures sockets.
        """
        market_symbols = [self._stream_key(s) for s in market_symbols if s]
        del spot_symbols
        if not market_symbols:
            return

        def _watch():
            while self._running and session_id == self._market_session_id:
                time.sleep(self._stream_watchdog_interval)
                if not self._running or session_id != self._market_session_id:
                    return

                now_ts = time.time()
                stale = self._execution_stream_silence_reasons(
                    market_symbols=market_symbols,
                    now_ts=now_ts,
                )

                if not stale:
                    continue

                logger.warning(
                    "Stream silence detected, reconnecting market streams: "
                    + ", ".join(stale[:6])
                )
                self._restart_market_stream_after_silence()
                return

        threading.Thread(target=_watch, daemon=True).start()

    def _execution_stream_silence_reasons(
        self,
        *,
        market_symbols: list[str],
        now_ts: float,
    ) -> list[str]:
        """Return reconnect-authoritative execution depth silence only."""

        exec_symbol = self._stream_key(self.cfg.symbol)
        if exec_symbol not in market_symbols:
            return []
        last_seen = self._market_depth_seen.get(exec_symbol, float(now_ts))
        age = max(0.0, float(now_ts) - float(last_seen))
        if age <= self._exec_stream_silence_timeout:
            return []
        return [f"{exec_symbol}@executionDepth {age:.0f}s"]

    def _restart_market_stream_after_silence(self):
        if not self._running or not self._public_market_streams_started:
            return
        if self._market_reconnect_requested:
            return
        self._market_reconnect_requested = True
        if self._ws_market:
            try:
                self._ws_market.stop()
            except Exception:
                pass
            self._ws_market = None
        if self._ws_public:
            try:
                self._ws_public.stop()
            except Exception:
                pass
            self._ws_public = None
        try:
            self._reconnect_market()
        finally:
            self._market_reconnect_requested = False

    def _start_user_stream(self, rest_client) -> bool:
        """Create listen key and start user data stream."""

        with self._user_stream_lifecycle_lock:
            return self._start_user_stream_locked(rest_client)

    def _start_user_stream_locked(self, rest_client) -> bool:
        """Start one user stream while holding the lifecycle lock."""

        if not self._running:
            return False

        try:
            import websocket
        except Exception as exc:
            logger.error(
                f"Failed to start user data stream: websocket-client unavailable ({exc})"
            )
            return False

        try:
            self._stop_user_stream()
            if not self._running:
                return False

            resp = rest_client.new_listen_key()
            self._listen_key = resp.get("listenKey", "")
            if not self._listen_key:
                logger.error("Failed to get listen key")
                return False

            url = self._private_user_stream_url(self._listen_key)
            self._user_stream_active = True

            def make_callbacks():
                session: dict[str, int] = {}

                def on_message(ws, message):
                    self._on_user_message(ws, message, session.get("token", -1))

                def on_open(ws):
                    self._on_user_open(ws, session.get("token", -1))

                def on_error(ws, error):
                    self._on_user_error(ws, error, session.get("token", -1))

                def on_close(ws, status_code, message):
                    self._on_user_close(
                        ws, status_code, message, session.get("token", -1)
                    )

                return session, on_message, on_open, on_error, on_close

            def run():
                while self._running and self._user_stream_active:
                    session, on_message, on_open, on_error, on_close = make_callbacks()

                    ws = websocket.WebSocketApp(
                        url,
                        on_open=on_open,
                        on_message=on_message,
                        on_error=on_error,
                        on_close=on_close,
                    )
                    token = self._install_user_stream_app(ws)
                    if token is None:
                        close = getattr(ws, "close", None)
                        if callable(close):
                            close()
                        break
                    session["token"] = token
                    try:
                        ws.run_forever(ping_interval=20, ping_timeout=10)
                    except Exception as exc:
                        logger.warning("User data WebSocket loop failed: %s", exc)
                    finally:
                        self._release_user_stream_app(ws, token)
                    if self._running and self._user_stream_active:
                        time.sleep(2)

            self._user_thread = threading.Thread(target=run, daemon=True)
            self._user_thread.start()
            logger.info(f"User data stream started, listen_key={self._listen_key[:8]}...")
            return True
        except Exception as e:
            logger.error(f"Failed to start user data stream: {e}")
            return False

    def _latch_user_stream_quiescence_failure(
        self,
        *,
        thread_name: str,
        error: BaseException,
    ) -> None:
        latch = getattr(self.engine, "latch_runtime_fatal", None)
        if not callable(latch):
            return
        try:
            latch(
                reason=f"USER_STREAM_SHUTDOWN_NOT_QUIESCENT:{thread_name}",
                error=error,
                reconciliation_required=True,
                defer_reconciliation=True,
            )
        except BaseException:
            logger.critical(
                "Failed to latch user-stream shutdown uncertainty",
                exc_info=True,
            )

    def _join_user_stream_thread(
        self,
        thread: Optional[threading.Thread],
        *,
        thread_name: str,
    ) -> None:
        if thread is None or not thread.is_alive():
            return
        if thread is threading.current_thread():
            error = RuntimeError(
                f"cannot quiesce {thread_name} from its own callback thread"
            )
            self._latch_user_stream_quiescence_failure(
                thread_name=thread_name,
                error=error,
            )
            raise error
        timeout_s = max(
            0.0,
            float(self._user_stream_shutdown_join_timeout_s),
        )
        thread.join(timeout=timeout_s)
        if thread.is_alive():
            error = RuntimeError(
                f"{thread_name} did not stop within {timeout_s:.3f}s"
            )
            self._latch_user_stream_quiescence_failure(
                thread_name=thread_name,
                error=error,
            )
            raise error

    def _stop_user_stream(self):
        with self._user_stream_lifecycle_lock:
            self._stop_user_stream_locked()

    def _stop_user_stream_locked(self):
        with self._user_event_stats_lock:
            self._user_stream_active = False
            self._user_stream_connected = False
            self._user_stream_ready_event.clear()
            ws_user = self._ws_user
        if ws_user:
            try:
                close = getattr(ws_user, "close", None)
                if callable(close):
                    close()
                else:
                    ws_user.stop()
            except Exception:
                pass
        thread = self._user_thread
        self._join_user_stream_thread(
            thread,
            thread_name="user-data WebSocket thread",
        )
        with self._user_event_stats_lock:
            self._user_stream_session_token += 1
            self._user_stream_connected = False
            self._user_stream_ready_event.clear()
            if self._ws_user is ws_user:
                self._ws_user = None
        if thread is self._user_thread:
            self._user_thread = None

    def _restart_user_stream_after_callback(self, reason: str) -> None:
        try:
            self.restart_user_stream(reason)
        except BaseException as exc:
            self._latch_user_stream_quiescence_failure(
                thread_name="user-data restart controller",
                error=exc,
            )
            logger.critical("Deferred user-stream restart failed", exc_info=True)
        finally:
            if self._user_restart_thread is threading.current_thread():
                self._user_restart_thread = None

    def restart_user_stream(self, reason: str = ""):
        """Best-effort user-data reconnect after a position sync discrepancy."""
        listen_key_client = self._listen_key_client or self._rest_client
        if not self._running or listen_key_client is None:
            return
        if self._user_thread is threading.current_thread():
            restart_thread = self._user_restart_thread
            if restart_thread is not None and restart_thread.is_alive():
                return
            restart_thread = threading.Thread(
                target=self._restart_user_stream_after_callback,
                args=(reason,),
                daemon=True,
                name="user-stream-restart-controller",
            )
            self._user_restart_thread = restart_thread
            restart_thread.start()
            return
        msg = "Restarting user data WebSocket"
        if reason:
            msg += f": {reason}"
        logger.warning(msg)
        self._start_user_stream(listen_key_client)

    def _listen_key_renewal_loop(self, rest_client):
        """Renew listen key periodically (every 30 min, validity = 60 min)."""
        while self._running:
            interval = max(1, int(self.cfg.performance.listen_key_renew))
            # Sleep in short increments so we can exit quickly on shutdown
            for _ in range(interval):
                if not self._running:
                    return
                time.sleep(1)
            if not self._running:
                break
            try:
                rest_client.renew_listen_key(self._listen_key)
                logger.debug("Listen key renewed")
            except Exception as e:
                logger.error(f"Listen key renewal failed: {e}")
                # Try to recreate
                try:
                    self._start_user_stream(rest_client)
                except Exception as e2:
                    logger.error(f"Listen key recreate failed: {e2}")

    # ── message handlers ──

    def _on_market_message(self, _, message):
        """Route market data messages to appropriate handlers."""
        receive_ns = time.time_ns()
        try:
            if isinstance(message, (bytes, bytearray)):
                message = message.decode("utf-8", errors="ignore")

            if isinstance(message, str):
                data = json.loads(message)
            else:
                data = message

            if not isinstance(data, dict):
                return

            # Handle combined stream wrapper: {"stream":"...","data":{...}}
            if "stream" in data and "data" in data:
                data = data["data"]

            if not isinstance(data, dict):
                return

            if "error" in data:
                request_id = data.get("id")
                desc = self._pop_market_request_desc(request_id)
                logger.error(
                    f"Futures stream request failed id={request_id} "
                    f"desc={desc or 'unknown'} error={data.get('error')}"
                )
                return

            if "result" in data and "id" in data and "e" not in data:
                request_id = data.get("id")
                desc = self._pop_market_request_desc(request_id)
                result = data.get("result")
                if isinstance(result, list):
                    streams = ", ".join(result)
                    logger.info(
                        f"Futures stream snapshot id={request_id} "
                        f"desc={desc or 'LIST_SUBSCRIPTIONS'} streams=[{streams}]"
                    )
                else:
                    logger.info(
                        f"Futures stream ACK id={request_id} "
                        f"desc={desc or 'request'} result={result}"
                    )
                return

            event_type = data.get("e", "")

            if event_type == "aggTrade":
                self._agg_trade_count += 1
                event_symbol = str(data.get("s", "")).upper()
                event_key = self._stream_key(event_symbol)
                if event_key:
                    self._market_trade_seen[event_key] = time.time()
                is_execution_symbol = event_symbol == self.cfg.symbol.upper()
                if is_execution_symbol:
                    self._exec_trade_count += 1
                    self.engine.signal.on_agg_trade(
                        data,
                        receive_ts_ns=receive_ns,
                        sequence_number=data.get("a", data.get("t")),
                    )
                    with self._deep_book_lock:
                        deep_book = self._deep_book
                    if deep_book is not None:
                        deep_book.on_agg_trade(
                            data,
                            receive_ts_ns=receive_ns,
                        )
                else:
                    self._cross_trade_count += 1
                    self.engine.signal.on_cross_agg_trade(
                        data,
                        market_type=PERP_MARKET,
                        receive_ts_ns=receive_ns,
                        sequence_number=data.get("a", data.get("t")),
                    )
                self._record_binance_market_event(
                    data,
                    market_type=PERP_MARKET,
                    event_type="trade",
                    receive_ts_ns=receive_ns,
                    feature_ready_ts_ns=time.time_ns(),
                )

                # Update mark price from trade
                price = float(data.get("p", 0))
                if price > 0 and is_execution_symbol:
                    self.engine.inventory.update_mark_price(price)

            elif event_type == "depthUpdate":
                self._depth_count += 1
                event_key = self._stream_key(str(data.get("s", "")))
                if event_key:
                    # The execution partial-depth stream is periodic and is the
                    # only event-silence clock allowed to restart market/public
                    # transport.  bookTicker and aggTrade remain feature clocks.
                    self._market_depth_seen[event_key] = time.time()
                self.engine.signal.on_depth(
                    data,
                    receive_ts_ns=receive_ns,
                )
                self._record_binance_market_event(
                    data,
                    market_type=PERP_MARKET,
                    event_type="depth",
                    receive_ts_ns=receive_ns,
                    feature_ready_ts_ns=time.time_ns(),
                )

            elif event_type == "bookTicker":
                # Best bid/ask update
                bid = float(data.get("b", 0))
                ask = float(data.get("a", 0))
                event_key = self._stream_key(str(data.get("s", "")))
                if event_key:
                    self._market_book_seen[event_key] = time.time()
                self.engine.signal.on_book_ticker(
                    data,
                    receive_ts_ns=receive_ns,
                    sequence_number=data.get("u"),
                )
                self._record_binance_market_event(
                    data,
                    market_type=PERP_MARKET,
                    event_type="book",
                    receive_ts_ns=receive_ns,
                    feature_ready_ts_ns=time.time_ns(),
                )
                if bid > 0 and ask > 0:
                    if str(data.get("s", "")).upper() == self.cfg.symbol.upper():
                        mid = (bid + ask) / 2.0
                        self.engine.inventory.update_mark_price(mid)
                        # Store BBO for Post Only guard
                        self.engine._best_bid = bid
                        self.engine._best_ask = ask

        except Exception as e:
            logger.error(f"Market message error: {e}")

    def _on_spot_message(self, message):
        """Route Binance spot bookTicker messages for cross-market anchors."""
        receive_ns = time.time_ns()
        try:
            if isinstance(message, (bytes, bytearray)):
                message = message.decode("utf-8", errors="ignore")

            if isinstance(message, str):
                data = json.loads(message)
            else:
                data = message

            if not isinstance(data, dict):
                return

            if "stream" in data and "data" in data:
                data = data["data"]

            event_type = data.get("e", "")
            if event_type == "aggTrade":
                self._spot_trade_count += 1
                event_key = self._stream_key(str(data.get("s", "")))
                if event_key:
                    self._spot_trade_seen[event_key] = time.time()
                self.engine.signal.on_cross_agg_trade(
                    data,
                    market_type=SPOT_MARKET,
                    receive_ts_ns=receive_ns,
                    sequence_number=data.get("a", data.get("t")),
                )
                self._record_binance_market_event(
                    data,
                    market_type=SPOT_MARKET,
                    event_type="trade",
                    receive_ts_ns=receive_ns,
                    feature_ready_ts_ns=time.time_ns(),
                )
                return

            if not all(k in data for k in ("s", "b", "a")):
                return

            if not any(data.get(key) for key in ("T", "E", "ts")):
                data["_event_timestamp_source"] = "local_synthetic"
                data["E"] = int(time.time() * 1000)
            else:
                data["_event_timestamp_source"] = "exchange"
            self._spot_book_ticker_count += 1
            event_key = self._stream_key(str(data.get("s", "")))
            if event_key:
                self._spot_book_seen[event_key] = time.time()
            self.engine.signal.on_book_ticker(
                data,
                market_type=SPOT_MARKET,
                receive_ts_ns=receive_ns,
                sequence_number=data.get("u"),
            )
            self._record_binance_market_event(
                data,
                market_type=SPOT_MARKET,
                event_type="book",
                receive_ts_ns=receive_ns,
                feature_ready_ts_ns=time.time_ns(),
            )

        except Exception as e:
            logger.error(f"Spot message error: {e}")

    def _on_user_message(self, ws, message, token: int):
        """Route user data messages."""
        with self._user_event_callback_lock:
            # Revalidate after entering the serialization boundary: this
            # callback may have waited behind startup while its session was
            # disconnected or replaced.
            with self._user_event_stats_lock:
                if token != self._user_stream_session_token or ws is not self._ws_user:
                    return
            self._on_current_user_message(message)

    def _on_current_user_message(self, message) -> None:
        receive_ts_ns = time.time_ns()
        event_type = ""
        try:
            if isinstance(message, (bytes, bytearray)):
                message = message.decode("utf-8", errors="ignore")

            if isinstance(message, str):
                data = json.loads(message)
            else:
                data = message

            if not isinstance(data, dict):
                return

            # Handle combined stream wrapper: {"stream":"...","data":{...}}
            if "stream" in data and "data" in data:
                data = data["data"]

            event_type = data.get("e", "")

            # Ignore subscription acks and non-event payloads.
            if not event_type:
                return

            with self._user_event_stats_lock:
                self._user_event_count += 1
                self._last_user_event_monotonic_s = time.monotonic()

            if event_type == "ORDER_TRADE_UPDATE":
                order_data = data.get("o", {})
                order_data["_local_receive_ts_ns"] = receive_ts_ns
                order_data["_feature_ready_ts_ns"] = time.time_ns()
                order_gateway = getattr(self.engine, "order_gateway", None)
                record_private_visibility = getattr(
                    order_gateway, "record_private_order_visibility", None
                )
                private_evidence_error: Exception | None = None
                if callable(record_private_visibility):
                    # Record the raw private-stream observation before the
                    # order ledger de-duplicates or transitions it.  NEW may
                    # precede the synchronous REST/WebSocket response, so the
                    # evidence row must not depend on response-first ordering.
                    try:
                        record_private_visibility(
                            order_data,
                            receive_ts_ns=receive_ts_ns,
                        )
                    except Exception as exc:
                        # Evidence overload is fatal and must be surfaced, but
                        # it must never prevent an already-observed exchange
                        # fill/terminal event from reaching the authoritative
                        # ledger and its immediate risk callbacks.
                        private_evidence_error = exc
                self.engine.orders.on_order_update(order_data)
                if private_evidence_error is not None:
                    raise RuntimeError(
                        "private order visibility evidence admission failed"
                    ) from private_evidence_error

            elif event_type == "ACCOUNT_UPDATE":
                account_data = data.get("a", {})

                # Skip sync when reason is ORDER — ORDER_TRADE_UPDATE handles
                # fills authoritatively. Syncing here double-counts the fill
                # because ACCOUNT_UPDATE arrives before ORDER_TRADE_UPDATE.
                # 中文说明：用户流的成交真相以 ORDER_TRADE_UPDATE 为准。
                # ACCOUNT_UPDATE 只用于非成交原因的兜底同步；真正 missed fill
                # 由 main loop 的 REST sync 记录为 SYNC_ADJUST 并触发降级。
                reason = account_data.get("m", "")
                if reason == "ORDER":
                    pass  # skip — fill will be processed via ORDER_TRADE_UPDATE
                else:
                    # ACCOUNT_UPDATE has no per-order cumulative cursor.  Use a
                    # bounded REST positionRisk + accountTrades snapshot instead
                    # of installing an unidentifiable reconciliation barrier.
                    self.engine.sync_position(required=True)

            elif event_type == "listenKeyExpired":
                logger.warning("Listen key expired, reconnecting...")
                self.restart_user_stream("listen key expired")

        except Exception as e:
            if event_type in {"ORDER_TRADE_UPDATE", "ACCOUNT_UPDATE"}:
                self.engine.latch_runtime_fatal(
                    reason=f"USER_EVENT_CALLBACK_FAILURE:{event_type}",
                    error=e,
                    reconciliation_required=True,
                )
                logger.critical("User message callback failed: %s", e, exc_info=True)
            else:
                logger.error(f"User message error: {e}")

    def user_event_safety_snapshot(
        self,
        *,
        now_monotonic_s: Optional[float] = None,
    ) -> dict[str, Any]:
        """Return general user-stream liveness facts for operational health."""

        now_monotonic_s = (
            time.monotonic()
            if now_monotonic_s is None
            else float(now_monotonic_s)
        )
        with self._user_event_stats_lock:
            event_count = int(self._user_event_count)
            last_event = float(self._last_user_event_monotonic_s)
            connected = bool(self._user_stream_connected)
            generation = int(self._user_stream_generation)
        age_s = (
            max(0.0, now_monotonic_s - last_event)
            if last_event > 0.0
            else None
        )
        return {
            "user_event_count": event_count,
            "last_user_event_age_s": age_s,
            "user_stream_connected": connected,
            "user_stream_generation": generation,
        }

    def wait_for_user_stream_ready(self, timeout_s: float) -> bool:
        """Wait until the current private-stream generation is connected.

        Readiness belongs to a successfully opened, currently installed user
        stream.  Installation, disconnect, release, restart, and stop all
        clear the event, so a stale generation cannot satisfy a later wait.
        """

        timeout_s = float(timeout_s)
        if not math.isfinite(timeout_s) or timeout_s < 0.0:
            raise ValueError("user-stream readiness timeout must be finite and non-negative")
        deadline = time.monotonic() + timeout_s
        while True:
            with self._user_event_stats_lock:
                if (
                    self._running
                    and self._user_stream_active
                    and self._user_stream_connected
                    and self._ws_user is not None
                    and self._user_stream_generation > 0
                ):
                    return True
                if not self._running or not self._user_stream_active:
                    return False
                # An event without the matching connected state is stale or
                # raced a disconnect.  Clear it while holding the same lock
                # used by every lifecycle transition before waiting again.
                self._user_stream_ready_event.clear()
            remaining_s = deadline - time.monotonic()
            if remaining_s <= 0.0:
                return False
            self._user_stream_ready_event.wait(remaining_s)

    def _on_market_close(self, _):
        logger.warning("Market trade WebSocket closed")
        with self._public_market_phase_lock:
            if self._public_market_streams_starting:
                self._public_market_startup_closed = True
                return
            reconnect = bool(
                self._running and self._public_market_streams_started
            )
        if self._market_reconnect_requested:
            return
        if reconnect:
            logger.info("Reconnecting futures market/public streams in 2s...")
            if self._ws_market:
                try:
                    self._ws_market.stop()
                except Exception:
                    pass
                self._ws_market = None
            time.sleep(2)
            self._reconnect_market()

    def _on_public_close(self, _):
        logger.warning("Public data WebSocket closed")
        with self._public_market_phase_lock:
            if self._public_market_streams_starting:
                self._public_market_startup_closed = True
                return
            reconnect = bool(
                self._running and self._public_market_streams_started
            )
        if self._market_reconnect_requested:
            return
        if reconnect:
            logger.info("Reconnecting futures market/public streams in 2s...")
            if self._ws_public:
                try:
                    self._ws_public.stop()
                except Exception:
                    pass
                self._ws_public = None
            time.sleep(2)
            self._reconnect_market()

    def _install_user_stream_app(self, ws) -> Optional[int]:
        with self._user_event_stats_lock:
            if not self._running or not self._user_stream_active:
                return None
            self._user_stream_session_token += 1
            token = self._user_stream_session_token
            self._ws_user = ws
            self._user_stream_connected = False
            self._user_stream_ready_event.clear()
            return token

    def _on_user_open(self, ws, token: int) -> None:
        with self._user_event_stats_lock:
            if token != self._user_stream_session_token or ws is not self._ws_user:
                return
            if not self._running or not self._user_stream_active:
                return
            if self._user_stream_connected:
                return
            self._user_stream_generation += 1
            self._user_stream_connected = True
            self._user_stream_ready_event.set()

    def _set_user_stream_disconnected(self, ws, token: int) -> bool:
        with self._user_event_stats_lock:
            if token != self._user_stream_session_token or ws is not self._ws_user:
                return False
            self._user_stream_connected = False
            self._user_stream_ready_event.clear()
            return True

    def _release_user_stream_app(self, ws, token: int) -> bool:
        with self._user_event_stats_lock:
            if token != self._user_stream_session_token or ws is not self._ws_user:
                return False
            self._user_stream_connected = False
            self._user_stream_ready_event.clear()
            self._ws_user = None
            return True

    def _on_user_error(self, ws, error, token: int) -> None:
        if self._set_user_stream_disconnected(ws, token):
            logger.warning(f"User data WebSocket error: {error}")

    def _on_user_close(self, ws, _status_code, _message, token: int) -> None:
        if not self._set_user_stream_disconnected(ws, token):
            return
        logger.warning("User data WebSocket closed")
        if self._running and self._user_stream_active:
            logger.info("User data WebSocket reconnect loop will retry in 2s...")

    def _reconnect_market(self):
        """Reconnect futures market/public data WebSockets (not user stream)."""
        if not self._running or not self._public_market_streams_started:
            return
        from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient

        symbol = self.cfg.symbol.lower()
        market_symbols = self._market_symbols()
        spot_symbols = self._spot_anchor_symbols()

        self._stop_spot_stream()

        self._ws_market = UMFuturesWebsocketClient(
            stream_url=self._market_stream_base_url(),
            on_message=self._on_market_message,
            on_close=self._on_market_close,
        )
        self._ws_public = UMFuturesWebsocketClient(
            stream_url=self._public_stream_base_url(),
            on_message=self._on_market_message,
            on_close=self._on_public_close,
        )
        self._market_session_id += 1
        self._reset_stream_watchdog_state(market_symbols, spot_symbols)
        self._subscribe_market_streams(symbol, market_symbols)
        self._subscribe_public_streams(symbol, market_symbols)
        self._start_spot_stream(spot_symbols)
        self._arm_stream_silence_watchdog(
            market_symbols, spot_symbols, self._market_session_id
        )
        logger.info("Futures market/public WebSockets reconnected")

    @staticmethod
    def _market_stream_changed(old_cfg, new_cfg) -> bool:
        old_multi = getattr(old_cfg, "multi_market", None)
        new_multi = getattr(new_cfg, "multi_market", None)
        return (
            old_cfg.symbol != new_cfg.symbol
            or old_cfg.websocket.depth_levels != new_cfg.websocket.depth_levels
            or old_cfg.websocket.depth_speed != new_cfg.websocket.depth_speed
            or getattr(old_multi, "enabled", False) != getattr(new_multi, "enabled", False)
            or getattr(old_multi, "market_stage", "minimal") != getattr(new_multi, "market_stage", "minimal")
            or getattr(old_multi, "reference_symbol", None) != getattr(new_multi, "reference_symbol", None)
            or getattr(old_multi, "stablecoin_anchor_symbol", "USDCUSDT")
            != getattr(new_multi, "stablecoin_anchor_symbol", "USDCUSDT")
        )

    @staticmethod
    def _external_stream_signature(cfg) -> tuple:
        external = getattr(cfg, "external_venues", None)
        sources = []
        for source in getattr(external, "sources", []):
            sources.append(
                tuple(
                    getattr(source, name, None)
                    for name in (
                        "venue",
                        "enabled",
                        "transport",
                        "symbol",
                        "instrument_type",
                        "product_type",
                        "websocket_url",
                        "rest_url",
                        "book_channel",
                        "trade_channel",
                        "poll_interval_ms",
                        "trade_poll_interval_ms",
                        "max_source_age_s",
                        "record_enabled",
                        "record_interval_ms",
                        "record_trades",
                        "record_queue_size",
                        "record_dir",
                    )
                )
            )
        return (
            bool(getattr(external, "enabled", False)),
            bool(getattr(external, "shadow_only", True)),
            tuple(sources),
        )

    @staticmethod
    def _market_tape_signature(cfg) -> tuple:
        logging_cfg = cfg.logging
        return tuple(
            getattr(logging_cfg, name, None)
            for name in (
                "market_tape_enabled",
                "market_tape_dir",
                "market_tape_record_books",
                "market_tape_record_trades",
                "market_tape_record_depth",
                "market_tape_book_interval_ms",
                "market_tape_queue_size",
            )
        )

    @staticmethod
    def _deep_book_signature(cfg) -> tuple:
        ws_cfg = cfg.websocket
        return (
            cfg.symbol,
            cfg.tick_size,
            bool(getattr(ws_cfg, "deep_book_enabled", False)),
            int(getattr(ws_cfg, "deep_book_snapshot_levels", 1000)),
            int(getattr(ws_cfg, "deep_book_speed", 100)),
            int(getattr(ws_cfg, "deep_book_max_buffer_events", 20000)),
            float(getattr(ws_cfg, "deep_book_resync_backoff_s", 1.0)),
            float(getattr(ws_cfg, "deep_book_max_age_s", 2.0)),
        )

    def on_config_reload(self, old_cfg, new_cfg):
        """Apply runtime config updates; reconnect market stream when needed."""
        external_changed = self._external_stream_signature(old_cfg) != self._external_stream_signature(new_cfg)
        market_tape_changed = self._market_tape_signature(old_cfg) != self._market_tape_signature(new_cfg)
        deep_book_changed = self._deep_book_signature(old_cfg) != self._deep_book_signature(new_cfg)
        self.cfg = new_cfg
        self._apply_stream_timeouts()

        if not self._running:
            return

        if old_cfg.performance.listen_key_renew != new_cfg.performance.listen_key_renew:
            logger.info(
                "Updated listen-key renew interval: "
                f"{old_cfg.performance.listen_key_renew}s -> {new_cfg.performance.listen_key_renew}s"
            )

        if old_cfg.api.testnet != new_cfg.api.testnet:
            logger.warning(
                "api.testnet changed via reload, but REST client endpoint cannot be hot-swapped. "
                "Restart process to fully apply endpoint changes."
            )
            return

        # A private-first startup deliberately leaves every market producer
        # dormant until the caller freezes the prospective epoch.  Config
        # reload cannot implicitly cross that boundary.
        if not self._public_market_streams_started:
            return

        if external_changed:
            logger.info("External venue config changed, reconnecting shadow streams...")
            self._start_external_venue_streams()

        if market_tape_changed:
            logger.info("Market tape config changed, restarting shadow recorder...")
            self._start_market_tape()

        if deep_book_changed:
            logger.info("Deep-book config changed, reconnecting independent stream...")
            if not self._start_deep_book_stream():
                self._schedule_deep_book_reconnect()

        if not self._market_stream_changed(old_cfg, new_cfg):
            return

        if old_cfg.symbol != new_cfg.symbol:
            logger.warning(
                f"Symbol changed on reload: {old_cfg.symbol} -> {new_cfg.symbol}. "
                "Market stream will reconnect, but a full restart is recommended."
            )

        logger.info("Market stream config changed, reconnecting WebSocket...")
        if self._ws_market:
            try:
                self._ws_market.stop()
            except Exception:
                pass
            self._ws_market = None
        if self._ws_public:
            try:
                self._ws_public.stop()
            except Exception:
                pass
            self._ws_public = None
        self._reconnect_market()

    # ── lifecycle ──

    def stop(self):
        """Stop all WebSocket connections."""
        with self._startup_lock:
            self._running = False
            self._private_user_stream_started = False
            with self._public_market_phase_lock:
                self._public_market_streams_started = False
                self._public_market_streams_starting = False
                self._public_market_startup_closed = False
        shutdown_errors: list[BaseException] = []

        self._stop_external_venue_streams()
        self._stop_market_tape()
        self._stop_deep_book_stream()

        if self._ws_market:
            try:
                self._ws_market.stop()
            except Exception:
                pass
            self._ws_market = None

        if self._ws_public:
            try:
                self._ws_public.stop()
            except Exception:
                pass
            self._ws_public = None

        try:
            self._stop_user_stream()
        except BaseException as exc:
            shutdown_errors.append(exc)
            logger.critical("User-data WebSocket shutdown failed", exc_info=True)

        for attr_name, thread_name in (
            ("_listen_key_thread", "listen-key renewal thread"),
            ("_user_restart_thread", "user-data restart controller"),
        ):
            thread = getattr(self, attr_name, None)
            try:
                self._join_user_stream_thread(
                    thread,
                    thread_name=thread_name,
                )
            except BaseException as exc:
                shutdown_errors.append(exc)
                logger.critical("%s shutdown failed", thread_name, exc_info=True)
            else:
                if thread is getattr(self, attr_name, None):
                    setattr(self, attr_name, None)

        self._stop_spot_stream()

        user_event_safety = self.user_event_safety_snapshot()
        logger.info(
            f"WSHandler stopped. Stats: "
            f"aggTrades={self._agg_trade_count}, "
            f"execTrades={self._exec_trade_count}, "
            f"crossTrades={self._cross_trade_count}, "
            f"depth={self._depth_count}, "
            f"deepDepth={self._deep_depth_count}, "
            f"spotBookTickers={self._spot_book_ticker_count}, "
            f"spotTrades={self._spot_trade_count}, "
            f"user_events={user_event_safety['user_event_count']}"
        )
        if shutdown_errors:
            raise RuntimeError(
                "user-stream shutdown did not reach callback quiescence"
            ) from shutdown_errors[0]
