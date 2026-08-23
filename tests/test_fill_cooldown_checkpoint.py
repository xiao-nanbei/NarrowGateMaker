from __future__ import annotations

import json
import os
import stat
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

import strategy.maker_engine as subject
from live.config import Config
from strategy.maker_engine import MakerEngine

OLD_ARTIFACT = f"BUY_E3:{'a' * 64}"
NEW_ARTIFACT = f"BUY_E3:{'b' * 64}"


def _clock(monkeypatch: pytest.MonkeyPatch, initial_ms: int) -> dict[str, int]:
    value = {"ms": int(initial_ms)}
    monkeypatch.setattr(
        subject,
        "time",
        SimpleNamespace(
            time=lambda: value["ms"] / 1_000.0,
            time_ns=lambda: value["ms"] * 1_000_000,
        ),
    )
    return value


def _engine(
    checkpoint: Path,
    *,
    active_identity: str = "B0",
) -> MakerEngine:
    cfg = Config()
    cfg.strategy.fill_cooldown = 85.0
    cfg.strategy.fill_cooldown_consecutive_reset_policy = "opposite_fill_only"
    cfg.logging.fill_cooldown_checkpoint = str(checkpoint)

    engine = object.__new__(MakerEngine)
    engine.cfg = cfg
    engine._buy_e3_cooldown_policy = (
        None if active_identity == "B0" else SimpleNamespace(deadline_identity=active_identity)
    )
    engine._fill_cooldown_until = {"BUY": 0.0, "SELL": 0.0}
    engine._consec_buy = 0.0
    engine._consec_sell = 0.0
    engine._last_same_side_fill_epoch_ms = {"BUY": 0, "SELL": 0}
    engine._last_fill_side = ""
    engine._fill_cooldown_deadline_identity = {"BUY": "B0", "SELL": "B0"}
    engine._fill_cooldown_natural_b0_until = {"BUY": 0.0, "SELL": 0.0}
    engine._fill_cooldown_checkpoint_path = checkpoint
    engine._fill_cooldown_checkpoint_lock = threading.RLock()
    engine._fill_cooldown_checkpoint_sequence = 0
    engine._fill_cooldown_checkpoint_loaded = False
    engine._fill_cooldown_restore_mode = "fresh_b0_no_checkpoint"
    return engine


def _seed_buy_state(
    engine: MakerEngine,
    *,
    now_ms: int,
    duration_ms: int,
    units: float,
    identity: str,
) -> None:
    engine._consec_buy = float(units)
    engine._last_same_side_fill_epoch_ms["BUY"] = int(now_ms)
    engine._last_fill_side = "BUY"
    engine._fill_cooldown_until["BUY"] = (now_ms + duration_ms) / 1_000.0
    engine._fill_cooldown_deadline_identity["BUY"] = identity
    engine._fill_cooldown_natural_b0_until["BUY"] = (
        now_ms + int(round(85_000.0 * max(1.0, units)))
    ) / 1_000.0


def test_crash_restart_preserves_same_artifact_absolute_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    start_ms = 1_900_000_000_000
    clock = _clock(monkeypatch, start_ms)
    checkpoint = tmp_path / "state.json"
    writer = _engine(checkpoint, active_identity=OLD_ARTIFACT)
    _seed_buy_state(
        writer,
        now_ms=start_ms,
        duration_ms=2_048_000,
        units=3.0,
        identity=OLD_ARTIFACT,
    )
    writer._persist_fill_cooldown_checkpoint()

    assert stat.S_IMODE(checkpoint.stat().st_mode) == 0o600
    assert not list(tmp_path.glob(".*.tmp"))
    clock["ms"] += 125_000
    restarted = _engine(checkpoint, active_identity=OLD_ARTIFACT)
    restored = restarted.restore_fill_cooldown_checkpoint(now_ms=clock["ms"])

    assert restored["restore_mode"] == "exact_same_artifact_resume"
    assert restored["buy_deadline_identity"] == OLD_ARTIFACT
    assert restored["buy_remaining_ms"] == 1_923_000
    assert restored["consec_buy"] == 3.0
    assert restored["checkpoint_sequence"] == 2


def test_atomic_checkpoint_failure_preserves_previous_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now_ms = 1_900_050_000_000
    _clock(monkeypatch, now_ms)
    checkpoint = tmp_path / "state.json"
    engine = _engine(checkpoint)
    _seed_buy_state(
        engine,
        now_ms=now_ms,
        duration_ms=85_000,
        units=1.0,
        identity="B0",
    )
    engine._persist_fill_cooldown_checkpoint()
    previous = checkpoint.read_bytes()

    engine._consec_buy = 2.0
    engine._fill_cooldown_until["BUY"] = (now_ms + 170_000) / 1_000.0

    def fail_replace(_source: Path, _target: Path) -> None:
        raise OSError("injected replace failure")

    monkeypatch.setattr(subject.os, "replace", fail_replace)
    with pytest.raises(OSError, match="injected replace failure"):
        engine._persist_fill_cooldown_checkpoint()

    assert engine._fill_cooldown_checkpoint_sequence == 1
    assert checkpoint.read_bytes() == previous
    assert not list(tmp_path.glob(".*.tmp"))


def test_graceful_stop_flushes_latest_b0_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now_ms = 1_900_100_000_000
    clock = _clock(monkeypatch, now_ms)
    checkpoint = tmp_path / "state.json"
    engine = _engine(checkpoint)
    _seed_buy_state(
        engine,
        now_ms=now_ms,
        duration_ms=170_000,
        units=2.0,
        identity="B0",
    )
    engine._running = True
    engine.signal = SimpleNamespace(stop=lambda: None)
    engine._cancel_all_orders = lambda: None
    engine.orders = SimpleNamespace(cancel_all_local=lambda: None)
    engine._order_lifecycle_live_writer_v2 = None
    engine._exact_opportunity_tape_runtime = None

    engine.stop()
    clock["ms"] += 10_000
    restarted = _engine(checkpoint)
    restored = restarted.restore_fill_cooldown_checkpoint(now_ms=clock["ms"])

    assert restored["restore_mode"] == "b0_checkpoint_resume"
    assert restored["buy_deadline_identity"] == "B0"
    assert restored["buy_remaining_ms"] == 160_000


@pytest.mark.parametrize(
    ("units", "expected_total_ms"),
    ((0.5, 85_000), (3.0, 255_000)),
)
def test_disabled_rollback_converts_e3_to_natural_residual_b0(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    units: float,
    expected_total_ms: int,
) -> None:
    start_ms = 1_900_200_000_000
    clock = _clock(monkeypatch, start_ms)
    checkpoint = tmp_path / f"state-{units}.json"
    writer = _engine(checkpoint, active_identity=OLD_ARTIFACT)
    _seed_buy_state(
        writer,
        now_ms=start_ms,
        duration_ms=2_048_000,
        units=units,
        identity=OLD_ARTIFACT,
    )
    writer._persist_fill_cooldown_checkpoint()

    clock["ms"] += 10_000
    rollback = _engine(checkpoint)
    restored = rollback.restore_fill_cooldown_checkpoint(now_ms=clock["ms"])

    assert restored["restore_mode"] == "rollback_to_b0"
    assert restored["buy_deadline_identity"] == "B0"
    assert restored["buy_remaining_ms"] == expected_total_ms - 10_000
    assert restored["buy_remaining_ms"] < 2_048_000 - 10_000


def test_artifact_drift_converts_old_e3_deadline_to_b0(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    start_ms = 1_900_300_000_000
    clock = _clock(monkeypatch, start_ms)
    checkpoint = tmp_path / "state.json"
    writer = _engine(checkpoint, active_identity=OLD_ARTIFACT)
    _seed_buy_state(
        writer,
        now_ms=start_ms,
        duration_ms=2_048_000,
        units=2.0,
        identity=OLD_ARTIFACT,
    )
    writer._persist_fill_cooldown_checkpoint()

    clock["ms"] += 20_000
    replacement = _engine(checkpoint, active_identity=NEW_ARTIFACT)
    restored = replacement.restore_fill_cooldown_checkpoint(now_ms=clock["ms"])

    assert restored["restore_mode"] == "artifact_identity_changed_to_b0"
    assert restored["buy_deadline_identity"] == "B0"
    assert restored["buy_remaining_ms"] == 150_000


@pytest.mark.parametrize(
    ("active_identity", "expected_mode"),
    (
        ("B0", "rollback_to_b0"),
        (NEW_ARTIFACT, "artifact_identity_changed_to_b0"),
    ),
)
def test_e3_expiry_does_not_drop_still_active_natural_b0_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    active_identity: str,
    expected_mode: str,
) -> None:
    start_ms = 1_900_350_000_000
    clock = _clock(monkeypatch, start_ms)
    checkpoint = tmp_path / f"state-{expected_mode}.json"
    writer = _engine(checkpoint, active_identity=OLD_ARTIFACT)
    _seed_buy_state(
        writer,
        now_ms=start_ms,
        duration_ms=79_000,
        units=1.0,
        identity=OLD_ARTIFACT,
    )
    writer._persist_fill_cooldown_checkpoint()

    clock["ms"] += 80_000
    replacement = _engine(checkpoint, active_identity=active_identity)
    restored = replacement.restore_fill_cooldown_checkpoint(now_ms=clock["ms"])

    assert restored["restore_mode"] == expected_mode
    assert restored["buy_deadline_identity"] == "B0"
    assert restored["buy_remaining_ms"] == 5_000


@pytest.mark.parametrize(
    ("active_identity", "expected_mode"),
    (
        ("B0", "rollback_to_b0"),
        (NEW_ARTIFACT, "artifact_identity_changed_to_b0"),
    ),
)
def test_runtime_e3_expiry_then_rollback_restores_natural_b0_residual(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    active_identity: str,
    expected_mode: str,
) -> None:
    start_ms = 1_900_375_000_000
    clock = _clock(monkeypatch, start_ms)
    checkpoint = tmp_path / f"state-expired-{expected_mode}.json"
    writer = _engine(checkpoint, active_identity=OLD_ARTIFACT)
    _seed_buy_state(
        writer,
        now_ms=start_ms,
        duration_ms=79_000,
        units=1.0,
        identity=OLD_ARTIFACT,
    )
    writer._persist_fill_cooldown_checkpoint()

    clock["ms"] += 80_000
    writer._expire_fill_cooldown_state("BUY", clock["ms"] / 1_000.0)
    expired = json.loads(checkpoint.read_text(encoding="ascii"))
    assert expired["state"]["buy_deadline_identity"] == "B0"
    assert expired["state"]["buy_remaining_ms"] == 0
    assert expired["buy_natural_b0_deadline_ms"] == start_ms + 85_000

    replacement = _engine(checkpoint, active_identity=active_identity)
    restored = replacement.restore_fill_cooldown_checkpoint(now_ms=clock["ms"])

    assert restored["restore_mode"] == expected_mode
    assert restored["buy_deadline_identity"] == "B0"
    assert restored["buy_remaining_ms"] == 5_000


def test_downtime_expiry_does_not_recreate_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    start_ms = 1_900_400_000_000
    clock = _clock(monkeypatch, start_ms)
    checkpoint = tmp_path / "state.json"
    writer = _engine(checkpoint, active_identity=OLD_ARTIFACT)
    _seed_buy_state(
        writer,
        now_ms=start_ms,
        duration_ms=2_048_000,
        units=3.0,
        identity=OLD_ARTIFACT,
    )
    writer._persist_fill_cooldown_checkpoint()

    clock["ms"] += 2_100_000
    restarted = _engine(checkpoint, active_identity=OLD_ARTIFACT)
    restored = restarted.restore_fill_cooldown_checkpoint(now_ms=clock["ms"])

    assert restored["restore_mode"] == "expired_to_b0"
    assert restored["buy_deadline_identity"] == "B0"
    assert restored["buy_remaining_ms"] == 0


def test_runtime_deadline_expiry_is_checkpointed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    start_ms = 1_900_450_000_000
    clock = _clock(monkeypatch, start_ms)
    checkpoint = tmp_path / "state.json"
    engine = _engine(checkpoint)
    _seed_buy_state(
        engine,
        now_ms=start_ms,
        duration_ms=1_000,
        units=1.0,
        identity="B0",
    )
    engine._persist_fill_cooldown_checkpoint()

    clock["ms"] += 2_000
    engine._expire_fill_cooldown_state("BUY", clock["ms"] / 1_000.0)
    payload = json.loads(checkpoint.read_text(encoding="ascii"))

    assert payload["checkpoint_sequence"] == 2
    assert payload["state"]["buy_remaining_ms"] == 0
    assert payload["state"]["buy_deadline_identity"] == "B0"


def test_missing_checkpoint_initializes_disabled_b0_epoch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now_ms = 1_900_500_000_000
    _clock(monkeypatch, now_ms)
    checkpoint = tmp_path / "missing.json"
    engine = _engine(checkpoint)

    restored = engine.restore_fill_cooldown_checkpoint(now_ms=now_ms)

    assert restored["restore_mode"] == "b0_checkpoint_resume"
    assert restored["checkpoint_loaded"] is True
    assert restored["checkpoint_sequence"] == 1
    assert restored["buy_deadline_identity"] == "B0"
    assert restored["buy_remaining_ms"] == 0
    assert checkpoint.is_file()
    assert stat.S_IMODE(checkpoint.stat().st_mode) == 0o600
    payload = json.loads(checkpoint.read_text(encoding="ascii"))
    assert payload["active_buy_e3_deadline_identity"] == "B0"
    assert payload["checkpoint_sequence"] == 1

    activated = _engine(checkpoint, active_identity=OLD_ARTIFACT)
    accepted = activated.restore_fill_cooldown_checkpoint(now_ms=now_ms + 1_000)
    assert accepted["restore_mode"] == "expired_to_b0"
    assert accepted["checkpoint_loaded"] is True
    assert accepted["checkpoint_sequence"] == 2
    assert accepted["buy_deadline_identity"] == "B0"


def test_active_e3_deleted_checkpoint_fails_closed_on_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now_ms = 1_900_550_000_000
    _clock(monkeypatch, now_ms)
    checkpoint = tmp_path / "state.json"
    writer = _engine(checkpoint, active_identity=OLD_ARTIFACT)
    _seed_buy_state(
        writer,
        now_ms=now_ms,
        duration_ms=2_048_000,
        units=2.0,
        identity=OLD_ARTIFACT,
    )
    writer._persist_fill_cooldown_checkpoint()
    checkpoint.unlink()

    restarted = _engine(checkpoint, active_identity=OLD_ARTIFACT)
    with pytest.raises(
        RuntimeError,
        match="active BUY E3 requires an existing fill cooldown checkpoint/epoch marker",
    ):
        restarted.restore_fill_cooldown_checkpoint(now_ms=now_ms + 1_000)

    assert not checkpoint.exists()


def test_tampered_checkpoint_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now_ms = 1_900_600_000_000
    _clock(monkeypatch, now_ms)
    checkpoint = tmp_path / "state.json"
    writer = _engine(checkpoint)
    writer._persist_fill_cooldown_checkpoint()
    payload = json.loads(checkpoint.read_text(encoding="ascii"))
    payload["checkpoint_sequence"] += 1
    checkpoint.write_text(json.dumps(payload), encoding="ascii")
    os.chmod(checkpoint, 0o600)

    with pytest.raises(ValueError, match="canonical SHA256 mismatch"):
        _engine(checkpoint).restore_fill_cooldown_checkpoint(now_ms=now_ms)


def test_truncated_checkpoint_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now_ms = 1_900_625_000_000
    _clock(monkeypatch, now_ms)
    checkpoint = tmp_path / "state.json"
    checkpoint.write_bytes(b'{"schema_version":')
    os.chmod(checkpoint, 0o600)

    with pytest.raises(ValueError, match="not canonical JSON"):
        _engine(checkpoint).restore_fill_cooldown_checkpoint(now_ms=now_ms)


def test_semantically_malformed_checkpoint_fails_closed_with_valid_hash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now_ms = 1_900_650_000_000
    _clock(monkeypatch, now_ms)
    checkpoint = tmp_path / "state.json"
    writer = _engine(checkpoint)
    writer._persist_fill_cooldown_checkpoint()
    payload = json.loads(checkpoint.read_text(encoding="ascii"))
    payload["state"]["buy_remaining_ms"] = True
    payload["canonical_checkpoint_sha256"] = writer._fill_cooldown_checkpoint_sha256(payload)
    checkpoint.write_bytes(writer._fill_cooldown_checkpoint_canonical_bytes(payload))
    os.chmod(checkpoint, 0o600)

    with pytest.raises(ValueError, match="BUY timing is invalid"):
        _engine(checkpoint).restore_fill_cooldown_checkpoint(now_ms=now_ms)


def test_e3_checkpoint_without_natural_b0_reference_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now_ms = 1_900_675_000_000
    _clock(monkeypatch, now_ms)
    checkpoint = tmp_path / "state.json"
    writer = _engine(checkpoint, active_identity=OLD_ARTIFACT)
    _seed_buy_state(
        writer,
        now_ms=now_ms,
        duration_ms=2_048_000,
        units=2.0,
        identity=OLD_ARTIFACT,
    )
    writer._persist_fill_cooldown_checkpoint()
    payload = json.loads(checkpoint.read_text(encoding="ascii"))
    payload["buy_natural_b0_deadline_ms"] = 0
    payload["canonical_checkpoint_sha256"] = writer._fill_cooldown_checkpoint_sha256(payload)
    checkpoint.write_bytes(writer._fill_cooldown_checkpoint_canonical_bytes(payload))
    os.chmod(checkpoint, 0o600)

    with pytest.raises(ValueError, match="lacks its natural B0 reference"):
        _engine(checkpoint, active_identity=OLD_ARTIFACT).restore_fill_cooldown_checkpoint(
            now_ms=now_ms
        )


def test_checkpoint_path_swap_during_read_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now_ms = 1_900_690_000_000
    _clock(monkeypatch, now_ms)
    checkpoint = tmp_path / "state.json"
    writer = _engine(checkpoint)
    writer._persist_fill_cooldown_checkpoint()
    replacement = tmp_path / "replacement.json"
    replacement.write_bytes(checkpoint.read_bytes())
    os.chmod(replacement, 0o600)

    real_read = subject.os.read
    swapped = False

    def swapping_read(descriptor: int, size: int) -> bytes:
        nonlocal swapped
        chunk = real_read(descriptor, size)
        if chunk and not swapped:
            swapped = True
            os.replace(replacement, checkpoint)
        return chunk

    monkeypatch.setattr(subject.os, "read", swapping_read)
    with pytest.raises((PermissionError, RuntimeError)):
        _engine(checkpoint).restore_fill_cooldown_checkpoint(now_ms=now_ms)
    assert swapped is True


def test_symlink_checkpoint_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now_ms = 1_900_700_000_000
    _clock(monkeypatch, now_ms)
    target = tmp_path / "target.json"
    target.write_text("{}\n", encoding="ascii")
    os.chmod(target, 0o600)
    checkpoint = tmp_path / "state.json"
    checkpoint.symlink_to(target)

    with pytest.raises(OSError):
        _engine(checkpoint).restore_fill_cooldown_checkpoint(now_ms=now_ms)


def test_non_private_checkpoint_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now_ms = 1_900_800_000_000
    _clock(monkeypatch, now_ms)
    checkpoint = tmp_path / "state.json"
    writer = _engine(checkpoint)
    writer._persist_fill_cooldown_checkpoint()
    os.chmod(checkpoint, 0o644)

    with pytest.raises(PermissionError, match="0600"):
        _engine(checkpoint).restore_fill_cooldown_checkpoint(now_ms=now_ms)
