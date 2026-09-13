#!/usr/bin/env python3
"""Switch only external reference sources to public WebSocket transport.

The editor preserves the rest of ``live/config.yaml`` byte-for-byte, including
strategy parameters and comments. REST URLs remain as recovery metadata.
"""

from __future__ import annotations

import argparse
import difflib
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path


SETTINGS = {
    ("bitget", "perp"): {
        "transport": "websocket",
        "websocket_url": "wss://ws.bitget.com/v3/ws/public",
        "book_channel": "books1",
        "trade_channel": "publicTrade",
        "record_interval_ms": "0.0",
    },
    ("bitget", "spot"): {
        "transport": "websocket",
        "websocket_url": "wss://ws.bitget.com/v3/ws/public",
        "book_channel": "books1",
        "trade_channel": "publicTrade",
        "record_interval_ms": "0.0",
    },
    ("bybit", "perp"): {
        "transport": "websocket",
        "websocket_url": "wss://stream.bybit.com/v5/public/linear",
        "book_channel": "orderbook.1",
        "trade_channel": "publicTrade",
        "record_interval_ms": "0.0",
    },
    ("bybit", "spot"): {
        "transport": "websocket",
        "websocket_url": "wss://stream.bybit.com/v5/public/spot",
        "book_channel": "orderbook.1",
        "trade_channel": "publicTrade",
        "record_interval_ms": "0.0",
    },
    ("okx", "perp"): {
        "transport": "websocket",
        "websocket_url": "wss://ws.okx.com:8443/ws/v5/public",
        "book_channel": "bbo-tbt",
        "trade_channel": "trades",
        "record_interval_ms": "0.0",
    },
    ("okx", "spot"): {
        "transport": "websocket",
        "websocket_url": "wss://ws.okx.com:8443/ws/v5/public",
        "book_channel": "bbo-tbt",
        "trade_channel": "trades",
        "record_interval_ms": "0.0",
    },
}


def _value(text: str) -> str:
    return text.strip().strip('"\'').lower()


def _set_field(block: list[str], key: str, value: str) -> list[str]:
    rendered = value if key == "record_interval_ms" else f'"{value}"'
    pattern = re.compile(rf"^(\s{{6}}){re.escape(key)}\s*:")
    for index, line in enumerate(block):
        if pattern.match(line):
            ending = "\n" if line.endswith("\n") else ""
            block[index] = f"      {key}: {rendered}{ending}"
            return block
    insert_at = next(
        (
            index + 1
            for index, line in enumerate(block)
            if re.match(r"^\s{6}(settlement_currency|product_type)\s*:", line)
        ),
        min(1, len(block)),
    )
    block.insert(insert_at, f"      {key}: {rendered}\n")
    return block


def update_external_sources(text: str) -> tuple[str, list[str]]:
    lines = text.splitlines(keepends=True)
    start = next(
        (index for index, line in enumerate(lines) if line.startswith("external_venues:")),
        None,
    )
    if start is None:
        raise ValueError("config has no external_venues section")
    end = len(lines)
    for index in range(start + 1, len(lines)):
        line = lines[index]
        if line.strip() and not line.startswith((" ", "\t", "#")):
            end = index
            break

    source_starts = [
        index
        for index in range(start + 1, end)
        if re.match(r"^\s{4}- venue\s*:", lines[index])
    ]
    updated: list[str] = []
    for reverse_index in range(len(source_starts) - 1, -1, -1):
        block_start = source_starts[reverse_index]
        block_end = source_starts[reverse_index + 1] if reverse_index + 1 < len(source_starts) else end
        block = lines[block_start:block_end]
        venue_match = re.search(r"venue\s*:\s*(.+)", block[0])
        instrument_line = next(
            (line for line in block if re.match(r"^\s{6}instrument_type\s*:", line)),
            "",
        )
        instrument_match = re.search(r"instrument_type\s*:\s*(.+)", instrument_line)
        if venue_match is None or instrument_match is None:
            continue
        identity = (_value(venue_match.group(1)), _value(instrument_match.group(1)))
        settings = SETTINGS.get(identity)
        if settings is None:
            continue
        for key, value in settings.items():
            block = _set_field(block, key, value)
        lines[block_start:block_end] = block
        updated.append(":".join(identity))
        end += len(block) - (block_end - block_start)
    return "".join(lines), sorted(updated)


def update_market_tape(text: str, *, enabled: bool) -> str:
    lines = text.splitlines(keepends=True)
    start = next(
        (index for index, line in enumerate(lines) if line.startswith("logging:")),
        None,
    )
    if start is None:
        raise ValueError("config has no logging section")
    end = len(lines)
    for index in range(start + 1, len(lines)):
        line = lines[index]
        if line.strip() and not line.startswith((" ", "\t", "#")):
            end = index
            break
    block = lines[start:end]
    settings = {
        "market_tape_enabled": "true" if enabled else "false",
        "market_tape_dir": '"logs/market_tape"',
        "market_tape_record_books": "true",
        "market_tape_record_trades": "true",
        "market_tape_record_depth": "false",
        "market_tape_book_interval_ms": "0.0",
        "market_tape_queue_size": "500000",
    }
    keys = tuple(settings)
    existing_indices = [
        index
        for index, line in enumerate(block)
        if any(re.match(rf"^\s{{2}}{re.escape(key)}\s*:", line) for key in keys)
    ]
    root_comment_index = next(
        (index for index, line in enumerate(block) if line.startswith("#")),
        len(block),
    )
    target_index = min(
        existing_indices[0] if existing_indices else len(block),
        root_comment_index,
    )
    block = [
        line
        for line in block
        if not any(re.match(rf"^\s{{2}}{re.escape(key)}\s*:", line) for key in keys)
    ]
    insert_at = target_index - sum(
        index < target_index for index in existing_indices
    )
    for key, value in settings.items():
        block.insert(insert_at, f"  {key}: {value}\n")
        insert_at += 1
    lines[start:end] = block
    return "".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("live/config.yaml"))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-backup", action="store_true")
    parser.add_argument(
        "--enable-market-tape",
        action="store_true",
        help="also enable full-fidelity Binance BBO/trade receive-time capture",
    )
    args = parser.parse_args()

    path = args.config.expanduser().resolve()
    original = path.read_text(encoding="utf-8")
    updated, sources = update_external_sources(original)
    if args.enable_market_tape:
        updated = update_market_tape(updated, enabled=True)
    if len(sources) != len(SETTINGS):
        raise RuntimeError(
            f"expected {len(SETTINGS)} external sources, updated {len(sources)}: {sources}"
        )
    if args.dry_run:
        print("".join(difflib.unified_diff(
            original.splitlines(keepends=True),
            updated.splitlines(keepends=True),
            fromfile=str(path),
            tofile=str(path),
        )))
        return 0
    if updated == original:
        print(f"already configured: {path}")
        return 0
    if not args.no_backup:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        shutil.copy2(path, path.with_name(f"{path.name}.bak.{stamp}"))
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(updated, encoding="utf-8")
    os.replace(temporary, path)
    print(f"updated {path}: {','.join(sources)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
