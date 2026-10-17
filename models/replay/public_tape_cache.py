"""Read-only numeric cache of the public exchange tape, not strategy state."""

from __future__ import annotations

from dataclasses import dataclass, field
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
from functools import cached_property
from bisect import bisect_right

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
    _verified: dict = field(default_factory=dict, compare=False, repr=False)

    def _check_chunk(self, chunk):
        for name, expected in chunk["sizes"].items():
            path = (self.root / name).resolve()
            if path.parent != self.root.resolve():
                raise ValueError("numeric tape cache file escapes chunk directory")
            stat = path.stat()
            identity = (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
            if self._verified.get(name) != identity:
                if stat.st_size != expected:
                    raise ValueError("numeric tape cache length mismatch")
                array = np.load(path, mmap_mode="r", allow_pickle=False)
                expected_dtype = EVENT if name == chunk["events"] else LEVEL
                if array.ndim != 1 or array.dtype != expected_dtype:
                    raise ValueError("numeric tape cache schema mismatch")
                self._verified[name] = identity

    @property
    def day_start_ns(self):
        return self.manifest["day_start_ns"]

    def __iter__(self):
        return self.iter_from(0)

    @cached_property
    def _chunk_starts(self):
        starts = [0]
        for chunk in self.manifest["chunks"]:
            self._check_chunk(chunk)
            events = np.load(self.root / chunk["events"], mmap_mode="r", allow_pickle=False)
            starts.append(starts[-1] + len(events))
        return tuple(starts)

    def iter_from(self, source_read_count):
        """Independent exact source cursor, including already-prefetched rows."""
        starts = self._chunk_starts
        if type(source_read_count) is not int or not 0 <= source_read_count <= starts[-1]:
            raise ValueError("numeric tape source cursor out of bounds")
        first = bisect_right(starts, source_read_count) - 1
        for index in range(first, len(self.manifest["chunks"])):
            chunk = self.manifest["chunks"][index]
            self._check_chunk(chunk)
            events = np.load(self.root / chunk["events"], mmap_mode="r", allow_pickle=False)
            levels = np.load(self.root / chunk["levels"], mmap_mode="r", allow_pickle=False)
            if (np.any(events["offset"] < 0) or np.any(events["count"] < 0)
                    or np.any(events["offset"] + events["count"] > len(levels))
                    or np.any(events["kind"] < 0) or np.any(events["kind"] >= len(self.manifest["kinds"]))
                    or np.any(events["source"] < 0) or np.any(events["source"] >= len(self.manifest["sources"]))):
                raise ValueError("numeric tape cache row bounds mismatch")
            begin_row = max(0, source_read_count - starts[index])
            # Zero-copy column views avoid a structured scalar field lookup for
            # every event/level. Keep the original slices and constructor checks.
            # Decode primitive columns once per bounded chunk. Iterating numpy
            # scalars and converting each level repeatedly is substantially more
            # allocation-heavy; tolist preserves the same int/float values.
            columns = tuple(np.asarray(events[name])[begin_row:].tolist() for name in EVENT.names)
            sides = np.asarray(levels["side"]).tolist()
            ticks = np.asarray(levels["tick"]).tolist()
            quantities = np.asarray(levels["quantity"]).tolist()
            for ts, ordinal, offset, size, kind, source in zip(*columns):  # noqa: B905 -- same array
                begin, count = int(offset), int(size)
                end = begin + count
                yield HistoricalExchangeBookEvent(
                    market_id=self.manifest["market_id"],
                    event_type=self.manifest["kinds"][int(kind)],
                    exchange_ts_ns=int(ts),
                    exchange_ts_source="unknown",
                    local_receive_ts_ns=0,
                    levels=tuple(
                        (
                            "bid" if side == 0 else "ask",
                            int(tick),
                            float(quantity),
                        )
                        for side, tick, quantity in zip(  # noqa: B905 -- identical slices
                            sides[begin:end], ticks[begin:end], quantities[begin:end]
                        )
                    ),
                    source=self.manifest["sources"][int(source)],
                    source_ordinal=int(ordinal),
                    sequence_scope="provider_ordered",
                )


def cached_public_tape(tape, cache_dir, *, manifest_id, chunk_events=8192):
    """Verify on admission; publish once atomically, fresh cursor per iteration."""
    if chunk_events <= 0:
        raise ValueError("positive chunk_events required")
    identity = dict(
        schema="public-numeric-book-v2", input_manifest=manifest_id, tick=str(tape.tick_size)
    )
    key = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    root = cache_dir / key
    with (cache_dir / (key + ".lock")).open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if root.exists():
            try:
                existing = json.loads((root / "manifest.json").read_text())
                if existing["identity"] != identity:
                    raise ValueError("numeric tape cache identity mismatch")
                cached = NumericPublicTape(root, existing)
                for chunk in existing["chunks"]:
                    cached._check_chunk(chunk)
            except (OSError, ValueError, KeyError, TypeError, EOFError):
                shutil.rmtree(root)
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
                    chunk["sizes"] = {}
                    for name in (chunk["events"], chunk["levels"]):
                        chunk["sizes"][name] = (stage / name).stat().st_size
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
        result = NumericPublicTape(root, meta)
        for chunk in meta["chunks"]:
            result._check_chunk(chunk)
        return result
