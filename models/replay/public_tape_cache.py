"""Read-only numeric cache of the public exchange tape, not strategy state."""

from __future__ import annotations

from dataclasses import dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile

import numpy as np

from models.tick_data_types import HistoricalExchangeBookEvent

EVENT = np.dtype(
    [
        ("ts", "<i8"),
        ("ordinal", "<i8"),
        ("offset", "<i8"),
        ("count", "<i8"),
        ("kind", "<i8"),
        ("source", "<i8"),
    ]
)
LEVEL = np.dtype([("side", "u1"), ("tick", "<i8"), ("quantity", "<f8")])


@dataclass(frozen=True)
class NumericPublicTape:
    root: Path
    manifest: dict

    @property
    def day_start_ns(self):
        return self.manifest["day_start_ns"]

    def __iter__(self):
        for chunk in self.manifest["chunks"]:
            events = np.load(self.root / chunk["events"], mmap_mode="r", allow_pickle=False)
            levels = np.load(self.root / chunk["levels"], mmap_mode="r", allow_pickle=False)
            for event in events:
                begin, count = int(event["offset"]), int(event["count"])
                yield HistoricalExchangeBookEvent(
                    market_id=self.manifest["market_id"],
                    event_type=self.manifest["kinds"][int(event["kind"])],
                    exchange_ts_ns=int(event["ts"]),
                    exchange_ts_source="unknown",
                    local_receive_ts_ns=0,
                    levels=tuple(
                        (
                            "bid" if level["side"] == 0 else "ask",
                            int(level["tick"]),
                            float(level["quantity"]),
                        )
                        for level in levels[begin : begin + count]
                    ),
                    source=self.manifest["sources"][int(event["source"])],
                    source_ordinal=int(event["ordinal"]),
                    sequence_scope="provider_ordered",
                )


def cached_public_tape(tape, cache_dir, *, manifest_id, chunk_events=8192):
    """Verify on admission; publish once atomically, fresh cursor per iteration."""
    if chunk_events <= 0:
        raise ValueError("positive chunk_events required")
    identity = dict(
        schema="public-numeric-book-v1", input_manifest=manifest_id, tick=str(tape.tick_size)
    )
    key = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    root = cache_dir / key
    with (cache_dir / (key + ".lock")).open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if not root.exists():
            stage = Path(tempfile.mkdtemp(prefix=key + ".part-", dir=cache_dir))
            try:
                meta = dict(
                    identity=identity,
                    day_start_ns=tape.day_start_ns,
                    market_id=tape.bundle.manifest["plan"]["market_id"],
                    sources=[],
                    kinds=[],
                    chunks=[],
                )
                events, levels = [], []

                def flush():
                    index = len(meta["chunks"])
                    chunk = dict(events=f"events-{index}.npy", levels=f"levels-{index}.npy")
                    np.save(
                        stage / chunk["events"], np.asarray(events, dtype=EVENT), allow_pickle=False
                    )
                    np.save(
                        stage / chunk["levels"], np.asarray(levels, dtype=LEVEL), allow_pickle=False
                    )
                    chunk["hashes"] = {}
                    for name in (chunk["events"], chunk["levels"]):
                        with (stage / name).open("rb") as f:
                            chunk["hashes"][name] = hashlib.file_digest(f, "sha256").hexdigest()
                    meta["chunks"].append(chunk)
                    events.clear()
                    levels.clear()

                for event in tape:
                    if event.event_type not in meta["kinds"]:
                        meta["kinds"].append(event.event_type)
                    if event.source not in meta["sources"]:
                        meta["sources"].append(event.source)
                    events.append(
                        (
                            event.exchange_ts_ns,
                            event.source_ordinal,
                            len(levels),
                            len(event.levels),
                            meta["kinds"].index(event.event_type),
                            meta["sources"].index(event.source),
                        )
                    )
                    for side, tick, quantity in event.levels:
                        if side not in {"bid", "ask"}:
                            raise ValueError("unknown public book side")
                        levels.append((int(side == "ask"), tick, quantity))
                    if len(events) >= chunk_events:
                        flush()
                if events:
                    flush()
                (stage / "manifest.json").write_text(json.dumps(meta, sort_keys=True))
                os.rename(stage, root)
            except BaseException:
                shutil.rmtree(stage)
                raise
        meta = json.loads((root / "manifest.json").read_text())
        if meta["identity"] != identity:
            raise ValueError("numeric tape cache identity mismatch")
        for chunk in meta["chunks"]:
            for name, expected in chunk["hashes"].items():
                with (root / name).open("rb") as f:
                    if hashlib.file_digest(f, "sha256").hexdigest() != expected:
                        raise ValueError("numeric tape cache checksum mismatch")
        return NumericPublicTape(root, meta)
