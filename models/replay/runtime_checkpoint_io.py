"""Atomic persistence for trusted, local replay-runtime checkpoints.

Pickle preserves shared order ownership, RNG and NumPy objects. These files are
implementation checkpoints, not portable model artifacts: only load files from
your own replay process, using the same code/runtime. Never accept an uploaded
pickle through Studio or deserialize one supplied by an untrusted party.
"""

from __future__ import annotations

import os
import io
import pickle
import tempfile
import threading
from functools import lru_cache
from pathlib import Path
from types import MappingProxyType

from models.exchange_book_replay import HistoricalExchangeBookScheduler


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
            _RuntimePickler(stream, protocol=pickle.HIGHEST_PROTOCOL).dump(checkpoint)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def load_trusted_runtime_checkpoint(path: str | Path) -> dict:
    """Load only a trusted local checkpoint; pickle is not a safe exchange format."""
    with Path(path).open("rb") as stream:
        checkpoint = pickle.load(stream)
    if not isinstance(checkpoint, dict) or checkpoint.get("schema") != "tick_replay_runtime.v1":
        raise ValueError("unsupported tick runtime checkpoint")
    return checkpoint
