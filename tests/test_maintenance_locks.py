"""Terminal-object cleanup must not confuse unlocked coordination with garbage."""

import json
import os
import subprocess
import sys

import pytest

from narrowgate.maintenance import locks


def put(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    # pytest's macOS tmp directory itself is a symlink; production rejects links.
    root = tmp_path.resolve()
    identity = {"experiment_id": "example", "parent_id": "parent", "arm_id": "A", "end_ns": 2000000}
    state = {
        "revision": 1,
        "tasks": {"task": {"status": "COMPLETE", "spec": {}}},
        "attempts": {
            "attempt": {
                "id": "attempt",
                "task_id": "task",
                "spec_hash": "frozen",
                "status": "COMPLETE",
                "result": {"summary": {"identity": identity}},
            }
        },
    }
    state_path = root / "pool.json"
    put(state_path, state)
    account = root / "account"
    put(account / "identity.json", identity)
    put(
        account / "completion.json",
        {
            "identity": identity,
            "economic_complete": True,
            "durable_cut_ms": 2,
            "payload_sha256": {"summary.json": "original"},
        },
    )
    lock = account / "execution.lock"
    lock.touch()
    monkeypatch.setattr(locks, "open_paths", lambda roots: [])
    return root, state_path, state, lock


def test_complete_lock_removed_once_without_touching_payload(workspace):
    root, state_path, _, lock = workspace
    content = (lock.parent / "completion.json").read_bytes()
    dry = locks.clean([root], state_path)
    assert dry["counts"] == {"eligible": 1} and lock.exists()
    result = locks.clean([root], state_path, apply=True)
    assert result["removed_files"] == 1 and result["logical_bytes_removed"] == 0
    assert not lock.exists()
    assert (lock.parent / "completion.json").read_bytes() == content
    assert locks.clean([root], state_path, apply=True)["removed_files"] == 0


@pytest.mark.parametrize(
    "name",
    [
        "requirements.lock",
        "uv.lock",
        "control.lock",
        "agent.lock",
        "preparation.lock",
        "unknown.lock",
    ],
)
def test_persistent_and_dependency_locks_stay(workspace, name):
    root, state_path, _, lock = workspace
    lock.rename(lock.with_name(name))
    assert locks.clean([root], state_path, apply=True)["removed_files"] == 0
    assert lock.with_name(name).exists()


@pytest.mark.parametrize("status", ["PENDING", "RUNNING", "UNKNOWN", "RESULT_PENDING", "STOPPED"])
def test_noncomplete_account_authority_retained(workspace, status):
    root, state_path, state, lock = workspace
    state["tasks"]["task"]["status"] = status
    put(state_path, state)
    assert locks.clean([root], state_path, apply=True)["removed_files"] == 0
    assert lock.exists()


def test_separate_held_recovery_consumer_protects_complete_identity(workspace):
    root, state_path, state, lock = workspace
    identity = state["attempts"]["attempt"]["result"]["summary"]["identity"]
    state["tasks"]["held"] = {"status": "PENDING", "spec": {"nested": {"identity": identity}}}
    put(state_path, state)
    assert locks.clean([root], state_path, apply=True)["removed_files"] == 0
    assert lock.exists()


@pytest.mark.parametrize("mutation", ["identity", "cut", "payload", "incomplete", "missing"])
def test_incomplete_or_payload_locks_stay(workspace, mutation):
    root, state_path, _, lock = workspace
    path = lock.parent / "completion.json"
    obj = json.loads(path.read_text())
    if mutation == "identity":
        obj["identity"]["parent_id"] = "different"
    if mutation == "cut":
        obj["durable_cut_ms"] = 1
    if mutation == "payload":
        obj["payload_sha256"]["execution.lock"] = "frozen-payload"
    if mutation == "incomplete":
        obj["economic_complete"] = False
    if mutation == "missing":
        path.unlink()
    else:
        put(path, obj)
    assert locks.clean([root], state_path, apply=True)["removed_files"] == 0
    assert lock.exists()


def test_open_owner_file_and_busy_lock_are_protected(workspace, monkeypatch):
    root, state_path, _, lock = workspace
    monkeypatch.setattr(locks, "open_paths", lambda roots: [lock.parent / "journal.jsonl"])
    assert locks.clean([root], state_path, apply=True)["removed_files"] == 0
    monkeypatch.setattr(locks, "open_paths", lambda roots: [])
    # Real second process, not a mock of flock: holding an inode blocks cleanup.
    code = "import fcntl,sys; f=open(sys.argv[1]); fcntl.flock(f,fcntl.LOCK_EX); print('ready',flush=True); sys.stdin.read()"
    process = subprocess.Popen(
        [sys.executable, "-c", code, str(lock)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout.readline().strip() == "ready"
        assert locks.clean([root], state_path, apply=True)["removed_files"] == 0
        assert lock.exists()
    finally:
        process.communicate("")


def test_changed_state_or_replaced_inode_never_deleted(workspace, monkeypatch):
    root, state_path, state, lock = workspace

    def replace_state(roots):
        state["tasks"]["task"]["status"] = "PENDING"
        put(state_path, state)
        return []

    monkeypatch.setattr(locks, "open_paths", replace_state)
    assert locks.clean([root], state_path, apply=True)["removed_files"] == 0
    state["tasks"]["task"]["status"] = "COMPLETE"
    put(state_path, state)

    def replace_lock(roots):
        lock.unlink()
        lock.write_text("new writer metadata")
        return []

    monkeypatch.setattr(locks, "open_paths", replace_lock)
    assert locks.clean([root], state_path, apply=True)["removed_files"] == 0
    assert lock.read_text() == "new writer metadata"


def test_links_git_and_missing_volume_are_not_cleaned(workspace):
    root, state_path, _, lock = workspace
    os.link(lock, root / "alias.lock")
    symlink = root / "linked"
    symlink.symlink_to(lock.parent, target_is_directory=True)
    hidden = root / ".git" / "index.lock"
    hidden.parent.mkdir()
    hidden.touch()
    assert locks.clean([root], state_path, apply=True)["removed_files"] == 0
    assert hidden.exists() and lock.exists()
    with pytest.raises(ValueError, match="unavailable"):
        locks.clean([root / "missing-volume"], state_path, apply=True)
    with pytest.raises(ValueError, match="symlink"):
        locks.clean([symlink], state_path, apply=True)


def test_terminal_runner_requires_exit_and_absent_original_pids(workspace, monkeypatch):
    root, state_path, state, _ = workspace
    attempt = root / "agent" / "attempts" / "task" / "attempt"
    put(attempt / "attempt.json", state["attempts"]["attempt"])
    put(attempt / "exit.json", {"kind": "RESULT_PENDING", "tree_exited": True})
    put(attempt / "runner.json", {"pid": 99999999})
    put(attempt / "process.json", {"pid": 99999998})
    lock = attempt / "runner.lock"
    lock.touch()

    def absent(pid, signal):
        raise ProcessLookupError()

    monkeypatch.setattr(os, "kill", absent)
    assert locks.classify(lock, locks.PoolSnapshot(state_path))[0] == "eligible"
    monkeypatch.setattr(os, "kill", lambda pid, signal: None)
    with pytest.raises(ValueError, match="still exists"):
        locks.classify(lock, locks.PoolSnapshot(state_path))
    put(attempt / "exit.json", {"kind": "UNKNOWN", "tree_exited": False})
    with pytest.raises(ValueError, match="exit"):
        locks.classify(lock, locks.PoolSnapshot(state_path))


def test_lsof_failure_fails_closed(workspace, monkeypatch):
    root, state_path, _, lock = workspace

    def failed(roots):
        raise ValueError("open-file inventory unavailable")

    monkeypatch.setattr(locks, "open_paths", failed)
    with pytest.raises(ValueError, match="open-file"):
        locks.clean([root], state_path, apply=True)
    assert lock.exists()
