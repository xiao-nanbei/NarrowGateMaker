"""Retire terminal replay lock files, not idle coordination locks.

An unlocked flock file is not generally disposable: another writer can already
have its inode open. Only terminal, non-reenterable objects are eligible here.
Unknown roles, dependency lockfiles, unfinished/recoverable accounts and shared
controller/publication locks remain untouched. No PID is killed or state edited.
"""

from __future__ import annotations

import fcntl
import json
import os
import stat
import subprocess
from collections import Counter
from pathlib import Path

DEPENDENCY_LOCKS = {
    "requirements.lock",
    "uv.lock",
    "poetry.lock",
    "pipfile.lock",
    "yarn.lock",
    "cargo.lock",
    "pdm.lock",
    "bun.lock",
    "composer.lock",
    "gemfile.lock",
    "packages.lock",
    "flake.lock",
    "package-lock.json",
    "pnpm-lock.yaml",
}
TERMINAL = {"COMPLETE", "STOPPED", "FAILED"}


def physical(path: Path) -> Path:
    path = Path(os.path.abspath(path))
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise ValueError(f"symlink is outside maintenance scope: {path}")
    return path


def fingerprint(path: Path) -> tuple:
    s = path.lstat()
    return s.st_dev, s.st_ino, s.st_mode, s.st_nlink, s.st_size, s.st_mtime_ns, s.st_ctime_ns


def read(path: Path):
    physical(path)
    with path.open() as stream:
        return json.load(stream)


def identity_key(value):
    if not isinstance(value, dict):
        return None
    keys = ("experiment_id", "parent_id", "arm_id")
    if not all(isinstance(value.get(k), str) and value[k] for k in keys):
        return None
    return tuple(value[k] for k in keys)


def identities(value):
    if isinstance(value, dict):
        key = identity_key(value)
        if key:
            yield key
        for child in value.values():
            yield from identities(child)
    elif isinstance(value, list):
        for child in value:
            yield from identities(child)


class PoolSnapshot:
    """Read existing task authority once; any concurrent change aborts apply."""

    def __init__(self, path: Path):
        self.path = physical(path)
        self.before = fingerprint(self.path)
        self.state = read(self.path)
        self.unchanged()
        self.tasks = self.state["tasks"]
        self.attempts = self.state["attempts"]
        self.completed = {}
        self.protected = set()
        for task in self.tasks.values():
            if task.get("status") != "COMPLETE":
                self.protected.update(identities(task.get("spec", {})))
                self.protected.update(identities(task.get("resume", {})))
        for attempt in self.attempts.values():
            key = identity_key(attempt.get("result", {}).get("summary", {}).get("identity"))
            if key:
                self.completed.setdefault(key, []).append(attempt)
            if attempt.get("status") != "COMPLETE":
                self.protected.update(identities(attempt.get("spec", {})))

    def unchanged(self):
        if fingerprint(self.path) != self.before:
            raise ValueError("PoolRun state changed; stop this pass and inspect next invocation")

    def terminal_attempt(self, aid):
        a = self.attempts.get(aid)
        if not a or a.get("status") not in TERMINAL:
            raise ValueError("Attempt is unknown or nonterminal")
        t = self.tasks.get(a.get("task_id"), {})
        # Even a terminal old Attempt is retained while its Task can be resumed.
        if t.get("status") not in TERMINAL:
            raise ValueError("Task has queued, held, unknown or recovery work")
        return a

    def complete_account(self, ident):
        key = identity_key(ident)
        if not key or key in self.protected:
            raise ValueError("account has unresolved, queued or recovery identity")
        matches = []
        for a in self.completed.get(key, []):
            if a.get("status") != "COMPLETE":
                continue
            t = self.tasks.get(a.get("task_id"), {})
            bound = a.get("result", {}).get("summary", {}).get("identity")
            if t.get("status") == "COMPLETE" and bound == ident:
                matches.append(a)
        if not matches:
            raise ValueError("no exact COMPLETE Task/Attempt identity")
        return matches


def dead_pid(record):
    """Conservative under PID reuse and permission failure; no false stale claim."""
    pid = record.get("pid")
    if type(pid) is not int or pid <= 0:
        raise ValueError("missing original process identity")
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return
    raise ValueError("original PID still exists or cannot be inspected")


def classify(path: Path, pool: PoolSnapshot):
    if path.name.lower() in DEPENDENCY_LOCKS:
        return "keep", "dependency_lock", []
    s = path.lstat()
    if not stat.S_ISREG(s.st_mode) or s.st_nlink != 1:
        return "keep", "not_single_regular_file", []
    if s.st_size:
        return "keep", "nonempty_lock_metadata_requires_owner", []
    if path.name == "runner.lock":
        root = path.parent
        if root.parent.parent.name != "attempts":
            return "keep", "unknown_runner_layout", []
        a = pool.terminal_attempt(root.name)
        local = read(root / "attempt.json")
        if local.get("id") != a["id"] or local.get("spec_hash") != a.get("spec_hash"):
            raise ValueError("local Attempt identity differs")
        exit_receipt = read(root / "exit.json")
        if exit_receipt.get("tree_exited") is not True:
            raise ValueError("process-tree exit not established")
        if exit_receipt.get("kind") not in {"RESULT_PENDING", "STOPPED", "FAILED"}:
            raise ValueError("unresolved exit receipt")
        dead_pid(read(root / "runner.json"))
        dead_pid(read(root / "process.json"))
        return "eligible", "terminal_poolrun_runner", [root]
    if path.name == "execution.lock":
        root = path.parent
        ident = read(root / "identity.json")
        completion = read(root / "completion.json")
        if completion.get("identity") != ident or completion.get("economic_complete") is not True:
            raise ValueError("account completion identity not established")
        if completion.get("durable_cut_ms") != ident.get("end_ns", -1) // 10**6:
            raise ValueError("account has not reached its terminal cut")
        payload = completion.get("payload_sha256", {})
        if not payload or path.name in payload:
            raise ValueError("lock is payload or completion manifest unavailable")
        pool.complete_account(ident)
        # Existing accounting acceptance belongs to the completed PoolRun task.
        # We remove zero-byte coordination only, never payloads/checkpoints.
        return "eligible", "completed_account_execution", [root]
    return "keep", "persistent_or_unknown_lock_owner", []


def open_paths(roots, *, timeout=30):
    """One process-table observation, not one expensive lsof traversal per file."""
    result = subprocess.run(
        ["lsof", "-nP", "-Fpn"],
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0 or result.stderr:
        raise ValueError("open-file inventory unavailable; no deletion permitted")
    pid = None
    found = []
    for line in result.stdout.splitlines():
        if line.startswith("p"):
            pid = int(line[1:])
        elif line.startswith("n/") and pid != os.getpid():
            path = Path(line[1:])
            if any(path == root or path.is_relative_to(root) for root in roots):
                found.append(path)
    return found


def scan_locks(roots, excluded=()):
    """Metadata traversal only. Never follow links, mounts, Git or index output."""
    errors = []
    paths = []
    for root in roots:
        root = physical(root)
        if not root.is_dir():
            raise ValueError(f"scan root unavailable: {root}")
        device = root.stat().st_dev
        stack = [root]
        while stack:
            directory = stack.pop()
            try:
                with os.scandir(directory) as entries:
                    for entry in entries:
                        path = Path(entry.path)
                        if entry.name == ".git" or path in excluded or entry.is_symlink():
                            continue
                        st = entry.stat(follow_symlinks=False)
                        if st.st_dev != device:
                            continue
                        if stat.S_ISDIR(st.st_mode):
                            stack.append(path)
                        elif entry.name.endswith(".lock"):
                            paths.append(path)
            except OSError as exc:
                errors.append({"path": str(directory), "error": str(exc)})
    return sorted(paths), errors


def clean(roots, pool_path, *, apply=False, excluded=()):
    pool = PoolSnapshot(pool_path)
    paths, errors = scan_locks(roots, excluded)
    report = {
        "mode": "apply" if apply else "dry_run",
        "pool_revision": pool.state.get("revision"),
        "scanned_locks": len(paths),
        "removed_files": 0,
        "logical_bytes_removed": 0,
        "records": [],
        "errors": errors,
    }
    eligible = []
    for path in paths:
        row = {"path": str(path)}
        try:
            physical(path)
            before = fingerprint(path)
            status, reason, scopes = classify(path, pool)
            row.update(status=status, reason=reason, logical_bytes=before[4])
            if status == "eligible":
                eligible.append((path, before, scopes, row))
        except (OSError, ValueError, KeyError, TypeError) as exc:
            row.update(status="keep", reason=str(exc))
        report["records"].append(row)
    # No open-file scan is needed when there is nothing actionable. Unknown roles
    # remain in the index but do not trigger a recurring full payload inventory.
    if eligible:
        opened = open_paths(roots)
        for path, before, scopes, row in eligible:
            if any(p == s or p.is_relative_to(s) for p in opened for s in scopes):
                row.update(status="keep", reason="open_file_in_owner_scope")
                continue
            try:
                pool.unchanged()
                # NOFOLLOW + inode comparison prevents symlink/replacement races.
                fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    if fingerprint(path) != before or os.fstat(fd).st_ino != before[1]:
                        raise ValueError("lock changed after inspection")
                    status, _, _ = classify(path, pool)
                    if status != "eligible":
                        raise ValueError("owner eligibility changed after inspection")
                    if apply:
                        # Only terminal objects: a fresh runner may check its
                        # immutable exit/completion receipt, never resume economics.
                        path.unlink()
                        row["status"] = "removed"
                        report["removed_files"] += 1
                        report["logical_bytes_removed"] += before[4]
                finally:
                    os.close(fd)
            except (OSError, ValueError, KeyError, TypeError) as exc:
                row.update(status="keep", reason=str(exc))
    report["counts"] = dict(Counter(r["status"] for r in report["records"]))
    return report
