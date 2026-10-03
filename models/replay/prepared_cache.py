"""Atomic numeric public-input cache. Private runtime state is never persisted."""

from dataclasses import fields
from contextlib import nullcontext
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile

import numpy as np
import pandas as pd

from data.runtime import ConsumerBundle
from data.facts import open_fact_shard
from models.tick_data_types import HistoricalBBOData, HistoricalL2Data
from models.replay.public_input import PreparedReplayInputs
from models.replay.public_tape_cache import cached_public_tape, NumericPublicTape


def _runtime_tape_lock(name, size):
    path = Path(name)
    return (size == 0 and len(path.parts) == 2 and path.parts[0] == "tape"
            and path.suffix == ".lock" and len(path.stem) == 64
            and all(c in "0123456789abcdef" for c in path.stem))


def prepare_cached(root, tick_size, cache_dir, loader, *, require_existing=False):
    bundle = root if isinstance(root, ConsumerBundle) else ConsumerBundle(root)
    # Admission checks bytes once per prepared owner, never per event/candidate.
    # A changed source with an unchanged manifest must not hit a stale cache.
    for source in bundle.source_paths():
        manifest = json.loads((source / "manifest.json").read_text())
        for item in manifest["files"]:
            open_fact_shard(source, item, verified_files=bundle.verified)
    for name in ("bars", "depth", "features"):
        bundle._verified_parquet(name)
    identity = dict(
        schema="public-prepared-v2",
        tick=str(tick_size),
        manifest=bundle.input_manifest_id,
    )
    key = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    cache_dir = Path(cache_dir)
    if not require_existing:
        cache_dir.mkdir(parents=True, exist_ok=True)
    destination = cache_dir / key
    context = nullcontext(None) if require_existing else (cache_dir / (key + ".lock")).open("a")
    with context as lock:
        if lock is not None:
            fcntl.flock(lock, fcntl.LOCK_EX)
        if require_existing and not destination.is_dir():
            raise FileNotFoundError(f"required prepared cache is absent: {destination}")
        if destination.exists():
            try:
                existing = json.loads((destination / "manifest.json").read_text())
                if existing["identity"] != identity:
                    raise ValueError("prepared cache identity mismatch")
                for name, size in existing["files"].items():
                    if _runtime_tape_lock(name, size):
                        continue
                    path = (destination / name).resolve()
                    if not path.is_relative_to(destination.resolve()) or path.stat().st_size != size:
                        raise ValueError("prepared cache length or location mismatch")
                for name, filename in existing["arrays"].items():
                    array = np.load(destination / filename, mmap_mode="r", allow_pickle=False)
                    schema = existing["array_schemas"][name]
                    if list(array.shape) != schema["shape"] or array.dtype.str != schema["dtype"]:
                        raise ValueError("prepared cache array schema mismatch")
                pd.read_parquet(destination / "bars.parquet")
            except (OSError, ValueError, KeyError, TypeError, EOFError):
                if require_existing:
                    raise
                # This directory is a reconstructible cache, never source evidence.
                shutil.rmtree(destination)
        if not destination.exists():
            prepared = loader(bundle, tick_size=tick_size)
            inputs = prepared.inputs
            stage = Path(tempfile.mkdtemp(prefix=key + ".part-", dir=cache_dir))
            try:
                meta = dict(
                    identity=identity,
                    arrays={},
                    array_schemas={},
                    trades_attrs=inputs["trades"].attrs,
                    trades_columns=list(inputs["trades"].columns),
                    books={},
                )

                def save(name, value):
                    array = np.asarray(value)
                    if array.dtype.hasobject:
                        if not all(isinstance(v, str) for v in array):
                            raise ValueError("non-string object cannot enter numeric cache")
                        array = np.asarray(value, dtype=str)
                    filename = name + ".npy"
                    np.save(stage / filename, array, allow_pickle=False)
                    meta["arrays"][name] = filename
                    meta["array_schemas"][name] = {"shape": list(array.shape), "dtype": array.dtype.str}

                for name in meta["trades_columns"]:
                    save("trade-" + name, inputs["trades"][name].to_numpy())
                inputs["bars"].to_parquet(stage / "bars.parquet")
                for name in ("bbo", "l2"):
                    meta["books"][name] = {}
                    for field in fields(inputs[name]):
                        value = getattr(inputs[name], field.name)
                        if isinstance(value, np.ndarray):
                            save(name + "-" + field.name, value)
                            meta["books"][name][field.name] = {"array": name + "-" + field.name}
                        else:
                            meta["books"][name][field.name] = {"value": value}
                save("variance_ts", prepared.variance_ts)
                save("variance", prepared.variance)
                tape = cached_public_tape(
                    inputs["exchange_book_event_tape"],
                    stage / "tape",
                    manifest_id=identity["manifest"],
                )
                meta["tape_root"] = str(tape.root.relative_to(stage))
                meta["files"] = {}
                for path in stage.rglob("*"):
                    if path.is_file():
                        name, size = str(path.relative_to(stage)), path.stat().st_size
                        if not _runtime_tape_lock(name, size):
                            meta["files"][name] = size
                (stage / "manifest.json").write_text(json.dumps(meta, sort_keys=True))
                os.rename(stage, destination)
            except BaseException:
                shutil.rmtree(stage)
                raise
        meta = json.loads((destination / "manifest.json").read_text())
        if meta["identity"] != identity:
            raise ValueError("prepared cache identity mismatch")
        for name, expected in meta["files"].items():
            if _runtime_tape_lock(name, expected):
                continue
            path = (destination / name).resolve()
            if not path.is_relative_to(destination.resolve()) or path.stat().st_size != expected:
                raise ValueError("prepared cache length or location mismatch")
    arrays = {
        name: np.load(destination / path, mmap_mode="r", allow_pickle=False)
        for name, path in meta["arrays"].items()
    }
    trades = pd.DataFrame(
        {name: arrays["trade-" + name] for name in meta["trades_columns"]}, copy=False
    )
    trades.attrs = meta["trades_attrs"]

    def book(name, constructor):
        return constructor(
            **{
                key: arrays[value["array"]] if "array" in value else value["value"]
                for key, value in meta["books"][name].items()
            }
        )

    tape_root = destination / meta["tape_root"]
    inputs = dict(
        bundle=bundle,
        trades=trades,
        bars=pd.read_parquet(destination / "bars.parquet"),
        frames=tuple(bundle.frames()),
        bbo=book("bbo", HistoricalBBOData),
        l2=book("l2", HistoricalL2Data),
        exchange_book_event_tape=NumericPublicTape(
            tape_root, json.loads((tape_root / "manifest.json").read_text())
        ),
        contract_parity="shared_input_adapter",
        native_observation_parity="not_proven",
        economic_admission=False,
    )
    return PreparedReplayInputs.create(inputs, tick_size, arrays["variance_ts"], arrays["variance"])
