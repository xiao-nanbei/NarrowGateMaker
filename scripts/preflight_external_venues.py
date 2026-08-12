#!/usr/bin/env python3
"""Run public external venue connectors without account/order access."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from live.config import load_config
from live.venues import (
    BitgetPublicReferenceClient,
    BybitPublicRestReferenceClient,
    BybitPublicWebSocketReferenceClient,
    OkxPublicRestReferenceClient,
    OkxPublicWebSocketReferenceClient,
)
from strategy.signal import SignalEngine


def _client_class(source):
    venue = str(source.venue).lower()
    transport = str(source.transport).lower()
    if venue == "bitget":
        return BitgetPublicReferenceClient
    if venue == "bybit":
        return (
            BybitPublicWebSocketReferenceClient
            if transport == "websocket"
            else BybitPublicRestReferenceClient
        )
    if venue == "okx":
        return (
            OkxPublicWebSocketReferenceClient
            if transport == "websocket"
            else OkxPublicRestReferenceClient
        )
    raise ValueError(f"unsupported external venue: {venue}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("live/config.yaml"))
    parser.add_argument("--duration-s", type=float, default=15.0)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--record", action="store_true")
    args = parser.parse_args()

    cfg = load_config(args.config)
    multi = cfg.multi_market
    signal = SignalEngine(
        enable_ml=False,
        symbol=cfg.symbol,
        reference_symbol=multi.reference_symbol,
        stablecoin_anchor_symbol=multi.stablecoin_anchor_symbol,
    )
    clients = []
    for source in cfg.external_venues.sources:
        if not source.enabled:
            continue
        source.record_enabled = bool(args.record)
        client = _client_class(source)(
            signal,
            source,
            project_root=ROOT,
        )
        clients.append(client)

    started = time.time_ns()
    try:
        for client in clients:
            client.start()
        time.sleep(max(1.0, args.duration_s))
        snapshots = [client.snapshot() for client in clients]
        flow = signal.global_flow_state().to_dict()
    finally:
        for client in reversed(clients):
            client.stop()
    failed = [
        row["market_id"]
        for row in snapshots
        if int(row.get("book_count", 0)) <= 0
        or int(row.get("error_count", 0)) > 0
        or int(row.get("book_stale", 1)) != 0
    ]
    summary = {
        "status": "ok" if not failed else "failed",
        "duration_s": (time.time_ns() - started) / 1_000_000_000.0,
        "public_only": True,
        "order_methods": False,
        "sources": snapshots,
        "failed_sources": failed,
        "global_flow": flow,
    }
    text = json.dumps(summary, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
