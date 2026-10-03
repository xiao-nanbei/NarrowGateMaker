"""Single-writer snapshots. Diagnostics are never a recovery journal."""
import copy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import threading
import time
import uuid


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def read(path):
    return json.loads(Path(path).read_text())


def atomic(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + "." + uuid.uuid4().hex + ".part")
    try:
        with tmp.open("xb") as stream:
            os.chmod(tmp, 0o600)
            stream.write(canonical(value))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
        fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        tmp.unlink(missing_ok=True)


class FileLock:
    def __init__(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.stream = open(path, "a+")
        try:
            fcntl.flock(self.stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            self.stream.close()
            raise

    def close(self):
        self.stream.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class Store:
    def __init__(self, root, initial=None, name="state"):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.file_lock = FileLock(self.root / ("control.lock" if name == "state" else name + ".lock"))
        self.path = self.root / (name + ".json")
        self.mutex = threading.RLock()
        self.writes = 0
        self.poisoned = False
        try:
            self.state = read(self.path) if self.path.exists() else copy.deepcopy(initial or {})
            if not isinstance(self.state, dict):
                raise ValueError("invalid snapshot; explicit recovery required")
        except BaseException:
            self.file_lock.close()
            raise

    def transition(self, request_id, mutation, payload=None):
        with self.mutex:
            if self.poisoned: raise RuntimeError("snapshot write failed previously; restart and reconcile before further mutations")
            requests = self.state.get("requests", {})
            identity = digest(payload)
            if request_id in requests:
                old = requests[request_id]
                if old["identity"] != identity:
                    raise ValueError("request_id reused with different payload")
                return copy.deepcopy(old["reply"])
            new = copy.deepcopy(self.state)
            reply = mutation(new)
            if request_id:
                new.setdefault("requests", {})[request_id] = {"identity": identity, "reply": copy.deepcopy(reply)}
            if new != self.state:
                new["revision"] = self.state.get("revision", 0) + 1
                try:
                    atomic(self.path, new)  # Never acknowledge a failed commit.
                except BaseException:
                    self.poisoned = True
                    raise
                self.state = new
                self.writes += 1
                # Optional diagnostics, never replayed as an authority/recovery log.
                try:
                    with (self.root/"events.jsonl").open("ab") as log:
                        log.write(canonical({"time":time.time(),"revision":new["revision"],"request_id":request_id})+b"\n")
                except OSError:
                    pass # A committed transition remains committed if diagnostics fail.
            return copy.deepcopy(reply)

    def backup(self, destination, retain=5):
        with self.mutex:
            target = Path(destination).resolve()
            target.mkdir(parents=True, exist_ok=True)
            path = target / f"state-{time.time_ns()}.json"
            atomic(path, self.state)
            for old in sorted(target.glob("state-*.json"))[:-max(1, retain)]:
                old.unlink()
            return str(path)

    def close(self):
        self.file_lock.close()
