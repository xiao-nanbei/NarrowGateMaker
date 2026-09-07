"""Atomic persistence for trusted, local replay-runtime checkpoints.

Pickle preserves shared order ownership, RNG and NumPy objects. These files are
implementation checkpoints, not portable model artifacts: only load files from
your own replay process, using the same code/runtime. Never accept an uploaded
pickle through Studio or deserialize one supplied by an untrusted party.
"""

from __future__ import annotations

import os
import pickle
import tempfile
from pathlib import Path

from models.exchange_book_replay import HistoricalExchangeBookScheduler


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
        return NotImplemented


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
