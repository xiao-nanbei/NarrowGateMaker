"""Atomic persistence for trusted, local replay-runtime checkpoints.

Pickle preserves shared order ownership, RNG and NumPy objects. These files are
implementation checkpoints, not portable model artifacts: only load files from
your own replay process, using the same code/runtime. Never accept an uploaded
pickle through Studio or deserialize one supplied by an untrusted party.
"""

from __future__ import annotations

import os
import io
import hashlib
import json
import pickle
import tempfile
import threading
from functools import lru_cache
from pathlib import Path
from types import MappingProxyType

from models.exchange_book_replay import HistoricalExchangeBookScheduler


def public_checkpoint_binding(input_manifest_id, params, predictions):
    """Bind the actual prepared replay, excluding only its output sink.

    This is computed only for requested checkpoint operations, never per event.
    Source identity covers the actual checkpoint/execution owners, not Git HEAD
    (which need not identify a dirty implementation).
    """
    import sys
    import numpy as np
    from models import backtest_tick
    from models.replay import l2_journal, runtime_input_window

    def normalize(value):
        if isinstance(value, dict):
            return {key: normalize(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [normalize(item) for item in value]
        if isinstance(value, np.ndarray):
            return normalize(value.tolist())
        if isinstance(value, np.generic):
            return value.item()
        if isinstance(value, Path):
            return str(value)
        return value

    payload = {key: normalize(value) for key, value in params.items()
               if key not in {'_l2_journal', '_replay_progress_callback'}}
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, allow_nan=False).encode())
    def bind_prediction(values):
        if isinstance(values, dict):
            for name in sorted(values):
                digest.update(name.encode())
                bind_prediction(values[name])
            return
        array = np.ascontiguousarray(values)
        if array.dtype.hasobject:
            raise ValueError('checkpoint predictions require numeric arrays')
        digest.update(str((array.shape, array.dtype.str)).encode())
        digest.update(array.tobytes())

    if predictions is not None:
        for values in predictions:
            bind_prediction(values)
    return dict(input_manifest_id=input_manifest_id, effective_inputs_sha256=digest.hexdigest(),
                python_version=sys.version,
                source_sha256={Path(module.__file__).name: hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
                               for module in (backtest_tick, l2_journal, runtime_input_window)},
                checkpoint_io_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())


def _restore_policy_object(cls, state, reentrant):
    restored = cls.__new__(cls)
    restored.__dict__.update(state)
    restored._lock = threading.RLock() if reentrant else threading.Lock()
    return restored


def _restore_mapping_proxy(values):
    return MappingProxyType(values)


@lru_cache(maxsize=1)
def _policy_state_types():
    # These are the actual live policy/window classes reused by offline B0.
    # Synchronization primitives belong to the process, not the saved economics.
    from strategy.boolean_cooldown_live import (
        LiveBooleanCooldownPolicy, ReceiveTimeMidEmaWindows, RuntimeCooldownPolicyEvaluator,
    )
    from strategy.boolean_cooldown_buy_e3 import LiveBuyE3CooldownPolicy, ReceiveTimeFullMidEmaWindows
    windows = (ReceiveTimeMidEmaWindows, ReceiveTimeFullMidEmaWindows)
    classes = (*windows, LiveBooleanCooldownPolicy, LiveBuyE3CooldownPolicy,
               RuntimeCooldownPolicyEvaluator)
    return frozenset(classes), frozenset(windows)


def _policy_state_reducer(obj):
    classes, windows = _policy_state_types()
    if type(obj) not in classes:
        return NotImplemented
    if getattr(obj, "_native_hot_path", None) is not None:
        raise TypeError("native cooldown hot-path state export is not implemented")
    reentrant = type(obj) in windows
    if ((reentrant and obj._lock._is_owned()) or not obj._lock.acquire(blocking=False)):
        raise RuntimeError("cannot checkpoint a policy while a callback owns its lock")
    try:
        state = {name: value for name, value in vars(obj).items() if name != "_lock"}
    finally:
        obj._lock.release()
    return _restore_policy_object, (type(obj), state, reentrant)


def _restore_book_scheduler(state):
    scheduler = HistoricalExchangeBookScheduler.__new__(HistoricalExchangeBookScheduler)
    scheduler.__dict__.update(state)
    # simulate_tick binds the unread source tail before executing another event.
    # None deliberately cannot pretend to be an exhausted, valid source.
    scheduler._iterator = None
    return scheduler


def _reduce_book_scheduler(scheduler):
    # Do not deepcopy independently: the enclosing Pickler's memo preserves
    # aliases between sequence/book and any other runtime references.
    return _restore_book_scheduler, ({
        name: value for name, value in vars(scheduler).items() if name != "_iterator"
    },)


class _RuntimePickler(pickle.Pickler):
    def reducer_override(self, obj):
        if isinstance(obj, HistoricalExchangeBookScheduler):
            return _reduce_book_scheduler(obj)
        if isinstance(obj, MappingProxyType):
            return _restore_mapping_proxy, (dict(obj),)
        return _policy_state_reducer(obj)


def clone_runtime_state(runtime):
    """Clone with the same graph-preserving reducers used by persisted resumes."""
    stream = io.BytesIO()
    _RuntimePickler(stream, protocol=pickle.HIGHEST_PROTOCOL).dump(runtime)
    stream.seek(0)
    return pickle.load(stream)


def save_runtime_checkpoint(path: str | Path, checkpoint: dict) -> None:
    """Replace the previous checkpoint only after the complete new file is durable."""
    if checkpoint.get("schema") != "tick_replay_runtime.v1":
        raise ValueError("unsupported tick runtime checkpoint")
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            writer = _HashingWriter(stream)
            _RuntimePickler(writer, protocol=pickle.HIGHEST_PROTOCOL).dump(checkpoint)
            stream.flush()
            os.fsync(stream.fileno())
            metadata = {
                "schema": "tick_replay_checkpoint_metadata.v1",
                "checkpoint_sha256": writer.digest.hexdigest(),
                "checkpoint_bytes": stream.tell(),
                "cut_ts_ms": checkpoint.get("cut_ts_ms"),
            }
        runtime = checkpoint.get("runtime")
        if runtime is not None and hasattr(runtime, "trade_ts") and metadata["cut_ts_ms"] is not None:
            from models.replay.runtime_input_window import runtime_input_context_start
            metadata["context_ms"] = 300_000
            metadata["required_context_start_ms"] = runtime_input_context_start(
                runtime, int(metadata["cut_ts_ms"]) - 300_000, 300_000,
            )
        os.replace(temporary, path)
        # The data file is durable first. A crash between the two replacements
        # leaves a missing/mismatched sidecar, never an apparently valid one.
        _save_checkpoint_metadata(path, metadata)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


class _HashingWriter:
    """Hash bytes during pickle output, without another full-file read or copy."""

    def __init__(self, stream):
        self.stream = stream
        self.digest = hashlib.sha256()

    def write(self, value):
        written = self.stream.write(value)
        self.digest.update(memoryview(value)[:written])
        return written


def _save_checkpoint_metadata(path: Path, metadata: dict) -> None:
    target = path.with_name(path.name + ".metadata.json")
    fd, temporary = tempfile.mkstemp(prefix=f".{target.name}.", dir=target.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(metadata, stream, sort_keys=True, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def load_runtime_checkpoint_metadata(path: str | Path) -> dict:
    """Inspect an owned checkpoint without allocating its runtime graph.

    Legacy files lacking a sidecar raise FileNotFoundError. Callers explicitly
    choose whether to migrate a trusted legacy checkpoint; a mismatched sidecar
    must never be treated as permission to use a stale cutoff.
    """
    path = Path(path)
    metadata = json.loads(path.with_name(path.name + ".metadata.json").read_text())
    if metadata.get("schema") != "tick_replay_checkpoint_metadata.v1":
        raise ValueError("unsupported checkpoint metadata")
    with path.open("rb") as stream:
        before = os.fstat(stream.fileno())
        if before.st_size != metadata["checkpoint_bytes"]:
            raise ValueError("checkpoint metadata size mismatch")
        actual = hashlib.file_digest(stream, "sha256").hexdigest()
        after = os.fstat(stream.fileno())
    if (actual != metadata["checkpoint_sha256"] or
            (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns)):
        raise ValueError("checkpoint metadata digest mismatch")
    return metadata


def load_trusted_runtime_checkpoint(path: str | Path) -> dict:
    """Load only a trusted local checkpoint; pickle is not a safe exchange format."""
    with Path(path).open("rb") as stream:
        checkpoint = pickle.load(stream)
    if not isinstance(checkpoint, dict) or checkpoint.get("schema") != "tick_replay_runtime.v1":
        raise ValueError("unsupported tick runtime checkpoint")
    return checkpoint
