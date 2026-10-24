"""Single-writer artifact IO shared by replay and research owners."""

import fcntl
import json
import os
from contextlib import contextmanager
from pathlib import Path


def numeric_json(value):
    import numpy as np

    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    raise TypeError(type(value).__name__)


def atomic_json(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w") as target:
        json.dump(value, target, default=numeric_json)
        target.flush()
        os.fsync(target.fileno())
    os.replace(temporary, path)


def load_json(path):
    return json.loads(Path(path).read_text())


@contextmanager
def cell_lock(directory):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / "execution.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
