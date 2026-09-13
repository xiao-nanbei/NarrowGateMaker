#!/usr/bin/env python3
"""Probe Binance USD-M snapshot-plus-diff deep-book synchronization."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from binance.um_futures import UMFutures
from binance.websocket.um_futures.websocket_client import (
    UMFuturesWebsocketClient,
)

from live.config import load_config
from live.orderbook.binance_usdm import BinanceUsdMDeepBook


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="live/config.yaml")
    parser.add_argument("--timeout-s", type=float, default=12.0)
    parser.add_argument("--minimum-levels", type=int, default=900)
    args = parser.parse_args()

    cfg = load_config(args.config)
    root = (
        "wss://stream.binancefuture.com"
        if cfg.api.testnet
        else "wss://fstream.binance.com"
    )
    book = BinanceUsdMDeepBook(
        UMFutures(),
        symbol=cfg.symbol,
        tick_size=cfg.tick_size,
        snapshot_levels=cfg.websocket.deep_book_snapshot_levels,
        max_buffer_events=cfg.websocket.deep_book_max_buffer_events,
        resync_backoff_s=cfg.websocket.deep_book_resync_backoff_s,
    )
    errors: list[Any] = []

    def on_message(_, message: Any) -> None:
        try:
            data = json.loads(message) if isinstance(message, str) else message
            if isinstance(data, dict) and "stream" in data and "data" in data:
                data = data["data"]
            if isinstance(data, dict) and data.get("e") == "depthUpdate":
                book.on_diff_event(data, receive_ts_ns=time.time_ns())
            elif isinstance(data, dict) and "error" in data:
                errors.append(data["error"])
        except Exception as exc:
            errors.append(repr(exc))

    client = UMFuturesWebsocketClient(
        stream_url=f"{root}/public",
        on_message=on_message,
    )
    try:
        client.diff_book_depth(
            symbol=cfg.symbol.lower(),
            speed=cfg.websocket.deep_book_speed,
            id=91_250,
        )
        book.start()
        deadline = time.monotonic() + max(1.0, float(args.timeout_s))
        snapshot: dict[str, Any] = {}
        while time.monotonic() < deadline:
            snapshot = book.snapshot(
                max_age_ms=cfg.websocket.deep_book_max_age_s * 1_000.0
            )
            if (
                snapshot["valid"]
                and snapshot["last_update_id"] > 0
                and snapshot["bid_levels"] >= args.minimum_levels
                and snapshot["ask_levels"] >= args.minimum_levels
            ):
                break
            time.sleep(0.05)
        print(
            json.dumps(
                {
                    "config": {
                        "top_depth_levels": cfg.websocket.depth_levels,
                        "top_depth_speed": cfg.websocket.depth_speed,
                        "deep_enabled": cfg.websocket.deep_book_enabled,
                        "deep_snapshot_levels": (
                            cfg.websocket.deep_book_snapshot_levels
                        ),
                        "deep_speed": cfg.websocket.deep_book_speed,
                        "deep_buffer": (
                            cfg.websocket.deep_book_max_buffer_events
                        ),
                    },
                    "snapshot": snapshot,
                    "errors": errors,
                },
                sort_keys=True,
            )
        )
        if (
            errors
            or not snapshot.get("valid")
            or snapshot.get("bid_levels", 0) < args.minimum_levels
            or snapshot.get("ask_levels", 0) < args.minimum_levels
        ):
            return 2
        return 0
    finally:
        client.stop()
        book.stop()


if __name__ == "__main__":
    raise SystemExit(main())
