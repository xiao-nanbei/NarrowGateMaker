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
    from models.exchange_book_replay import ReceiveTimeCooldownReplayAdapter

    cooldown_bindings = {}

    def bind_cooldown(adapter):
        # Bind immutable inputs, not the cursor/EMA state that the checkpoint
        # itself owns. The evaluator and snapshot emitter may be one object.
        if id(adapter) in cooldown_bindings:
            return cooldown_bindings[id(adapter)]
        arrays = hashlib.sha256()
        for value in (adapter._depth.ts_ms, adapter._depth.bid_px,
                      adapter._depth.ask_px, adapter._depth.bid_qty,
                      adapter._depth.ask_qty, adapter._receive, adapter._ready):
            value = np.ascontiguousarray(value)
            arrays.update(str((value.shape, value.dtype.str)).encode())
            arrays.update(memoryview(value).cast('B'))
        policies = {}
        for side, policy in sorted(adapter._policies.items()):
            policies[side] = {
                'identity': dict(adapter.cpp_policy_bindings[side]),
                'type': type(policy).__module__ + '.' + type(policy).__qualname__,
                'warmup_s': policy.windows.warmup_s,
                'max_feature_age_s': policy.windows.max_feature_age_s,
                'native': (_native_cooldown_binding(policy._native_cpp)
                           if policy._native_hot_path is not None else None),
            }
        binding = {'cooldown_depth_sha256': arrays.hexdigest(), 'policies': policies}
        cooldown_bindings[id(adapter)] = binding
        return binding

    def normalize(value):
        if isinstance(value, ReceiveTimeCooldownReplayAdapter):
            return bind_cooldown(value)
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


def _native_cooldown_binding(cpp):
    from strategy import native_cooldown, boolean_cooldown_live, boolean_cooldown_buy_e3
    def digest(path):
        with open(path, "rb") as stream:
            return hashlib.file_digest(stream, "sha256").hexdigest()
    return {"interface": cpp.APPLICATION_INTERFACE_VERSION,
            "binary_sha256": digest(cpp.__file__),
            "policy_sources": {module.__name__: digest(module.__file__) for module in
                               (native_cooldown, boolean_cooldown_live, boolean_cooldown_buy_e3)}}


def validate_native_cooldown_checkpoint(evaluator):
    """Cold preflight for the actual replay adapter, before any replay events."""
    if evaluator is None:
        return
    from models.exchange_book_replay import ReceiveTimeCooldownReplayAdapter
    policies = (evaluator._policies.values() if isinstance(evaluator, ReceiveTimeCooldownReplayAdapter)
                else (evaluator,))
    for policy in policies:
        native = getattr(policy, "_native_hot_path", None)
        if native is not None and not all(callable(getattr(native, method, None))
                                         for method in ("export_state", "restore_state")):
            raise TypeError("native cooldown backend has no complete checkpoint capability")


def _restore_policy_object(cls, state, reentrant, native_state=None, native_binding=None):
    restored = cls.__new__(cls)
    restored.__dict__.update(state)
    restored._lock = threading.RLock() if reentrant else threading.Lock()
    if native_state is not None:
        from strategy.native_cooldown import build_hot_path
        from strategy.boolean_cooldown_buy_e3 import LiveBuyE3CooldownPolicy
        cpp, native = build_hot_path(
            restored, profile="BUY" if cls is LiveBuyE3CooldownPolicy else "SELL",
            warmup_s=restored.windows.warmup_s,
            max_feature_age_s=restored.windows.max_feature_age_s, requested=True,
        )
        if native is None or not callable(getattr(native, "restore_state", None)):
            raise RuntimeError("native cooldown checkpoint requires state-capable native backend")
        if _native_cooldown_binding(cpp) != native_binding:
            raise ValueError("native cooldown checkpoint binary/source identity mismatch")
        native.restore_state(native_state)
        restored._native_cpp, restored._native_hot_path = cpp, native
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
    return frozenset(classes), frozenset((*windows, LiveBooleanCooldownPolicy, LiveBuyE3CooldownPolicy))


def _policy_state_reducer(obj):
    classes, windows = _policy_state_types()
    if type(obj) not in classes:
        return NotImplemented
    native = getattr(obj, "_native_hot_path", None)
    reentrant = type(obj) in windows
    if ((reentrant and obj._lock._is_owned()) or not obj._lock.acquire(blocking=False)):
        raise RuntimeError("cannot checkpoint a policy while a callback owns its lock")
    try:
        state = {name: value for name, value in vars(obj).items()
                 if name not in {"_lock", "_native_cpp", "_native_hot_path"}}
        if native is not None:
            if not callable(getattr(native, "export_state", None)):
                raise TypeError("native cooldown backend has no complete state export")
            native_state = native.export_state()
            native_binding = _native_cooldown_binding(obj._native_cpp)
        else:
            native_state = None
            native_binding = None
            if "_native_hot_path" in vars(obj):
                state.update(_native_cpp=None, _native_hot_path=None)
    finally:
        obj._lock.release()
    return _restore_policy_object, (type(obj), state, reentrant, native_state, native_binding)


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
