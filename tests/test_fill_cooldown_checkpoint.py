from __future__ import annotations

import os
import signal as signal_module
import stat
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

import strategy.maker_engine as subject
from live.config import Config
from strategy import fill_cooldown_checkpoint as checkpoint_store
from strategy.fill_cooldown_checkpoint import (
    FILL_COOLDOWN_WAL_FILE_BYTES,
    FILL_COOLDOWN_WAL_HEADER_BYTES,
    FILL_COOLDOWN_WAL_SLOT_BYTES,
    FillCooldownCheckpointCorruptionError,
    FillCooldownCheckpointWAL,
)
from strategy.maker_engine import MakerEngine
from strategy.order_manager import Side

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
    engine.order_gateway = SimpleNamespace()
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


def _engine_checkpoint_payload(path: Path) -> dict[str, object]:
    with FillCooldownCheckpointWAL(path) as wal:
        latest = wal.read_latest()
    assert latest is not None
    return latest.payload


def _wait_for_sigkill(pid: int) -> None:
    waited_pid, status = os.waitpid(pid, 0)
    assert waited_pid == pid
    assert os.WIFSIGNALED(status), status
    assert os.WTERMSIG(status) == signal_module.SIGKILL


def _target_second_fill_payload(
    path: Path,
    *,
    first_fill_ms: int,
) -> dict[str, object]:
    """Build the exact sequence-two payload a second BUY fill would commit."""

    target = _engine(path)
    target._fill_cooldown_checkpoint_sequence = 1
    _seed_buy_state(
        target,
        now_ms=first_fill_ms + 1_000,
        duration_ms=170_000,
        units=2.0,
        identity="B0",
    )
    target._fill_cooldown_last_fill_cursor = {
        "trade_id": 101,
        "order_id": 11,
        "cumulative_filled_qty": 0.001,
        "trade_time_ms": first_fill_ms + 1_000,
        "side": "BUY",
    }
    target._persist_fill_cooldown_checkpoint()
    payload = _engine_checkpoint_payload(path)
    target.close_fill_cooldown_checkpoint_store()
    return payload


def _restore_and_reconcile_second_fill(
    checkpoint: Path,
    *,
    first_fill_ms: int,
) -> tuple[MakerEngine, int, int]:
    restarted = _engine(checkpoint)
    restarted.restore_fill_cooldown_checkpoint(now_ms=first_fill_ms + 2_000)
    requests: list[dict[str, object]] = []

    def account_trades(**request: object) -> list[dict[str, object]]:
        requests.append(dict(request))
        if int(request.get("fromId", 0) or 0) > 101:
            return []
        return [
            {
                "id": 101,
                "orderId": 11,
                "time": first_fill_ms + 1_000,
                "qty": str(restarted.cfg.strategy.order_size),
                "buyer": True,
            }
        ]

    restarted.rest = SimpleNamespace(get_account_trades=account_trades)
    restarted.reconciliation_client = restarted.rest
    first = restarted.reconcile_fill_cooldown_checkpoint_gap()
    units_after_first = restarted._consec_buy
    second = restarted.reconcile_fill_cooldown_checkpoint_gap()

    assert restarted._consec_buy == pytest.approx(units_after_first)
    assert restarted._fill_cooldown_until["BUY"] >= (
        first_fill_ms + 1_000 + 170_000
    ) / 1_000.0
    assert _engine_checkpoint_payload(checkpoint)[
        "last_authoritative_fill_cursor"
    ]["trade_id"] == 101
    return restarted, int(first["recovered_fill_count"]), int(
        second["recovered_fill_count"]
    )


class _ProcessFillInventory:
    """Small deterministic inventory used inside the forked real callback."""

    def __init__(self) -> None:
        self.snapshot = SimpleNamespace(qty=0.0)
        self.consecutive_losses = 0

    def on_fill(
        self,
        side: str,
        qty: float,
        _price: float,
        _commission: float,
        _trade_time_ms: int,
        **_identity: object,
    ) -> float:
        self.snapshot.qty += float(qty) if side == "BUY" else -float(qty)
        return float(qty)

    def pop_runtime_evidence_error(self) -> None:
        return None

    @property
    def net_position(self) -> float:
        return float(self.snapshot.qty)


def _configure_process_fill_engine(
    checkpoint: Path,
    *,
    now_ms: int,
) -> MakerEngine:
    engine = _engine(checkpoint)
    engine.restore_fill_cooldown_checkpoint(now_ms=now_ms)
    engine.cfg.strategy.order_size = 0.001
    engine.cfg.lot_size = 0.0001
    engine.cfg.strategy.markout_ema_span_fills = 0
    engine.cfg.strategy.markout_spread_scale = 0.0
    engine.cfg.strategy.max_inventory = 1.0
    engine.inventory = _ProcessFillInventory()
    engine._post_fill_quote_response = SimpleNamespace(
        record_fill=lambda **_kwargs: None
    )
    engine._base_asset = "BTC"
    engine._quote_asset = "USDC"
    engine._settlement_asset = "USDC"
    engine._commission_unit_error = None
    engine._log_order_outcome = lambda *_args, **_kwargs: None
    engine._loss_cooldown_max_observed_consecutive_losses = 0
    engine._loss_cooldown_losing_round_trips = 0
    engine._loss_cooldown_winning_or_flat_round_trips = 0
    engine._adaptive_add_cooldown_multiplier = lambda *_args: 1.0
    engine._boolean_cooldown_policy = None
    engine._buy_e3_cooldown_policy = None
    engine._mo_pending = []
    engine._bid_cid = None
    engine._ask_cid = None
    engine._pop_order_context = lambda _cid: None
    return engine


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
    assert restored["checkpoint_sequence"] == 1


def test_valid_legacy_json_is_atomically_migrated_before_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now_ms = 1_900_025_000_000
    _clock(monkeypatch, now_ms)
    checkpoint = tmp_path / "state.json"
    writer = _engine(checkpoint)
    writer._persist_fill_cooldown_checkpoint()
    payload = _engine_checkpoint_payload(checkpoint)
    writer.close_fill_cooldown_checkpoint_store()
    checkpoint.unlink()
    payload["schema_version"] = subject.FILL_COOLDOWN_CHECKPOINT_SCHEMA_V1
    payload.pop("last_authoritative_fill_cursor")
    payload["canonical_checkpoint_sha256"] = (
        writer._fill_cooldown_checkpoint_sha256(payload)
    )
    checkpoint.write_bytes(writer._fill_cooldown_checkpoint_canonical_bytes(payload))
    os.chmod(checkpoint, 0o600)

    restored = _engine(checkpoint).restore_fill_cooldown_checkpoint(now_ms=now_ms)

    assert checkpoint.read_bytes().startswith(checkpoint_store.FILL_COOLDOWN_WAL_MAGIC)
    assert restored["checkpoint_sequence"] == 1
    assert _engine_checkpoint_payload(checkpoint)["checkpoint_sequence"] == 1


def test_cursorless_restore_preserves_old_gap_boundary_until_reconciliation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkpoint_ms = 1_900_030_000_000
    clock = _clock(monkeypatch, checkpoint_ms)
    checkpoint = tmp_path / "state.json"
    writer = _engine(checkpoint)
    writer._persist_fill_cooldown_checkpoint()
    writer.close_fill_cooldown_checkpoint_store()

    clock["ms"] += 60_000
    restarted = _engine(checkpoint)
    restored = restarted.restore_fill_cooldown_checkpoint(now_ms=clock["ms"])
    requests: list[dict[str, object]] = []

    def account_trades(**kwargs: object) -> list[dict[str, object]]:
        requests.append(dict(kwargs))
        return [
            {
                "id": 101,
                "orderId": 11,
                "time": checkpoint_ms + 1_000,
                "qty": "0.001",
                "buyer": True,
            }
        ]

    restarted.rest = SimpleNamespace(get_account_trades=account_trades)
    restarted.reconciliation_client = restarted.rest
    recovered = restarted.reconcile_fill_cooldown_checkpoint_gap()

    assert restored["checkpoint_sequence"] == 1
    assert requests == [
        {"symbol": restarted.cfg.symbol, "startTime": checkpoint_ms, "limit": 1000}
    ]
    assert recovered["recovered_fill_count"] == 1
    assert _engine_checkpoint_payload(checkpoint)[
        "last_authoritative_fill_cursor"
    ]["trade_id"] == 101


def test_restart_recovers_fill_after_last_durable_cursor_conservatively(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now_ms = 1_900_040_000_000
    _clock(monkeypatch, now_ms)
    checkpoint = tmp_path / "state.json"
    writer = _engine(checkpoint)
    _seed_buy_state(
        writer,
        now_ms=now_ms,
        duration_ms=85_000,
        units=1.0,
        identity="B0",
    )
    writer._fill_cooldown_last_fill_cursor = {
        "trade_id": 100,
        "order_id": 10,
        "cumulative_filled_qty": 0.001,
        "trade_time_ms": now_ms,
        "side": "BUY",
    }
    writer._persist_fill_cooldown_checkpoint()
    writer.close_fill_cooldown_checkpoint_store()

    restarted = _engine(checkpoint)
    restarted.restore_fill_cooldown_checkpoint(now_ms=now_ms + 2_000)
    requests = []

    def account_trades(**kwargs):
        requests.append(dict(kwargs))
        return [
            {
                "id": 101,
                "orderId": 11,
                "time": now_ms + 1_000,
                "qty": "0.001",
                "buyer": True,
            }
        ]

    restarted.rest = SimpleNamespace(get_account_trades=account_trades)
    restarted.reconciliation_client = restarted.rest
    recovery = restarted.reconcile_fill_cooldown_checkpoint_gap()

    assert requests == [
        {"symbol": restarted.cfg.symbol, "fromId": 101, "limit": 1000}
    ]
    assert recovery == {
        "recovered_fill_count": 1,
        "mode": "conservative_exchange_trade_recovery",
    }
    expected_units = 1.0 + 0.001 / restarted.cfg.strategy.order_size
    assert restarted._consec_buy == pytest.approx(expected_units)
    assert restarted._fill_cooldown_until["BUY"] == pytest.approx(
        (now_ms + 1_000) / 1_000.0
        + restarted.cfg.strategy.fill_cooldown * expected_units
    )
    payload = _engine_checkpoint_payload(checkpoint)
    assert payload["last_authoritative_fill_cursor"]["trade_id"] == 101


def test_gap_reconciliation_rejects_duplicate_trade_ids(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now_ms = 1_900_042_000_000
    _clock(monkeypatch, now_ms)
    checkpoint = tmp_path / "state.json"
    writer = _engine(checkpoint)
    writer._fill_cooldown_last_fill_cursor = {
        "trade_id": 100,
        "order_id": 10,
        "cumulative_filled_qty": 0.001,
        "trade_time_ms": now_ms,
        "side": "BUY",
    }
    writer._persist_fill_cooldown_checkpoint()
    writer.close_fill_cooldown_checkpoint_store()

    restarted = _engine(checkpoint)
    restarted.restore_fill_cooldown_checkpoint(now_ms=now_ms + 1_000)
    duplicate = {
        "id": 101,
        "orderId": 11,
        "time": now_ms + 500,
        "qty": "0.001",
        "buyer": True,
    }
    restarted.rest = SimpleNamespace(
        get_account_trades=lambda **_kwargs: [duplicate, dict(duplicate)]
    )
    restarted.reconciliation_client = restarted.rest

    with pytest.raises(RuntimeError, match="not strictly increasing"):
        restarted.reconcile_fill_cooldown_checkpoint_gap()


@pytest.mark.parametrize(
    ("crash_boundary", "new_checkpoint_durable", "expected_recovered"),
    (
        ("after_fill_ledger_before_cooldown", False, 1),
        ("after_cooldown_before_risk_cancel", False, 1),
        ("after_risk_cancel_before_checkpoint_sync", False, 1),
        ("after_checkpoint_sync_before_callback_return", True, 0),
    ),
)
def test_fill_callback_crash_boundaries_recover_from_exchange_cursor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_boundary: str,
    new_checkpoint_durable: bool,
    expected_recovered: int,
) -> None:
    """Every business-level crash boundary restores a conservative deadline."""

    del crash_boundary  # The id documents the exact business boundary under test.
    now_ms = 1_900_045_000_000
    _clock(monkeypatch, now_ms)
    checkpoint = tmp_path / "state.json"
    writer = _engine(checkpoint)
    _seed_buy_state(
        writer,
        now_ms=now_ms,
        duration_ms=85_000,
        units=1.0,
        identity="B0",
    )
    writer._fill_cooldown_last_fill_cursor = {
        "trade_id": 100,
        "order_id": 10,
        "cumulative_filled_qty": 0.001,
        "trade_time_ms": now_ms,
        "side": "BUY",
    }
    writer._persist_fill_cooldown_checkpoint()
    if new_checkpoint_durable:
        _seed_buy_state(
            writer,
            now_ms=now_ms + 1_000,
            duration_ms=170_000,
            units=2.0,
            identity="B0",
        )
        writer._fill_cooldown_last_fill_cursor = {
            "trade_id": 101,
            "order_id": 11,
            "cumulative_filled_qty": 0.001,
            "trade_time_ms": now_ms + 1_000,
            "side": "BUY",
        }
        writer._persist_fill_cooldown_checkpoint()
    writer.close_fill_cooldown_checkpoint_store()

    restarted = _engine(checkpoint)
    restarted.restore_fill_cooldown_checkpoint(now_ms=now_ms + 2_000)

    def account_trades(**request: object) -> list[dict[str, object]]:
        if int(request["fromId"]) > 101:
            return []
        return [
            {
                "id": 101,
                "orderId": 11,
                "time": now_ms + 1_000,
                "qty": str(restarted.cfg.strategy.order_size),
                "buyer": True,
            }
        ]

    restarted.rest = SimpleNamespace(get_account_trades=account_trades)
    restarted.reconciliation_client = restarted.rest
    recovery = restarted.reconcile_fill_cooldown_checkpoint_gap()

    assert recovery["recovered_fill_count"] == expected_recovered
    assert restarted._consec_buy == pytest.approx(2.0)
    assert restarted._fill_cooldown_until["BUY"] >= (
        now_ms + 1_000 + 170_000
    ) / 1_000.0
    assert _engine_checkpoint_payload(checkpoint)[
        "last_authoritative_fill_cursor"
    ]["trade_id"] == 101


def test_torn_wal_checkpoint_failure_preserves_previous_record(
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
    engine._consec_buy = 2.0
    engine._fill_cooldown_until["BUY"] = (now_ms + 170_000) / 1_000.0

    def fail_pwrite(_descriptor: int, _raw: bytes, _offset: int) -> int:
        raise OSError("injected WAL write failure")

    monkeypatch.setattr(checkpoint_store.os, "pwrite", fail_pwrite)
    with pytest.raises(OSError, match="injected WAL write failure"):
        engine._persist_fill_cooldown_checkpoint()

    assert engine._fill_cooldown_checkpoint_sequence == 1
    assert _engine_checkpoint_payload(checkpoint)["checkpoint_sequence"] == 1
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
    engine._cancel_all_orders = lambda: True
    engine.orders = SimpleNamespace(cancel_all_local=lambda: None)
    engine.sync_position = lambda *, required=False: True
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
    expired = _engine_checkpoint_payload(checkpoint)
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
    payload = _engine_checkpoint_payload(checkpoint)

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
    payload = _engine_checkpoint_payload(checkpoint)
    assert payload["active_buy_e3_deadline_identity"] == "B0"
    assert payload["checkpoint_sequence"] == 1

    activated = _engine(checkpoint, active_identity=OLD_ARTIFACT)
    accepted = activated.restore_fill_cooldown_checkpoint(now_ms=now_ms + 1_000)
    assert accepted["restore_mode"] == "expired_to_b0"
    assert accepted["checkpoint_loaded"] is True
    assert accepted["checkpoint_sequence"] == 1
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
    payload = _engine_checkpoint_payload(checkpoint)
    payload["checkpoint_sequence"] += 1
    writer._fill_cooldown_checkpoint_wal.close()
    FillCooldownCheckpointWAL(checkpoint).write(payload)

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
    payload = _engine_checkpoint_payload(checkpoint)
    payload["checkpoint_sequence"] = 2
    payload["state"]["checkpoint_sequence"] = 2
    payload["state"]["buy_remaining_ms"] = True
    payload["canonical_checkpoint_sha256"] = writer._fill_cooldown_checkpoint_sha256(payload)
    writer._fill_cooldown_checkpoint_wal.close()
    FillCooldownCheckpointWAL(checkpoint).write(payload)

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
    payload = _engine_checkpoint_payload(checkpoint)
    payload["checkpoint_sequence"] = 2
    payload["state"]["checkpoint_sequence"] = 2
    payload["buy_natural_b0_deadline_ms"] = 0
    payload["canonical_checkpoint_sha256"] = writer._fill_cooldown_checkpoint_sha256(payload)
    writer._fill_cooldown_checkpoint_wal.close()
    FillCooldownCheckpointWAL(checkpoint).write(payload)

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

    real_pread = checkpoint_store.os.pread
    swapped = False

    def swapping_pread(descriptor: int, size: int, offset: int) -> bytes:
        nonlocal swapped
        chunk = real_pread(descriptor, size, offset)
        if (
            chunk
            and size > len(checkpoint_store.FILL_COOLDOWN_WAL_MAGIC)
            and not swapped
        ):
            swapped = True
            os.replace(replacement, checkpoint)
        return chunk

    monkeypatch.setattr(checkpoint_store.os, "pread", swapping_pread)
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


def _wal_payload(sequence: int, *, deadline_ms: int) -> dict[str, object]:
    return {
        "checkpoint_sequence": sequence,
        "deadline_ms": deadline_ms,
        "state": {"checkpoint_sequence": sequence},
    }


def test_two_slot_wal_is_fixed_private_and_recovers_highest_sequence(
    tmp_path: Path,
) -> None:
    wal_path = tmp_path / "fill-cooldown.wal"
    with FillCooldownCheckpointWAL(wal_path) as wal:
        first = wal.write(_wal_payload(1, deadline_ms=200_000))
        second = wal.write(_wal_payload(2, deadline_ms=300_000))
        latest = wal.read_latest()

    assert first.slot_index == 0
    assert second.slot_index == 1
    assert latest is not None
    assert latest.sequence == 2
    assert latest.payload["deadline_ms"] == 300_000
    assert wal_path.stat().st_size == FILL_COOLDOWN_WAL_FILE_BYTES
    assert stat.S_IMODE(wal_path.stat().st_mode) == 0o600
    assert not list(tmp_path.glob("*.tmp"))


def test_wal_rejects_sequence_reuse_or_regression(tmp_path: Path) -> None:
    wal_path = tmp_path / "fill-cooldown.wal"
    with FillCooldownCheckpointWAL(wal_path) as wal:
        wal.write(_wal_payload(10, deadline_ms=500_000))
        wal.write(_wal_payload(11, deadline_ms=600_000))
        with pytest.raises(ValueError, match="strictly increase"):
            wal.write(_wal_payload(2, deadline_ms=1))
        with pytest.raises(ValueError, match="strictly increase"):
            wal.write(_wal_payload(11, deadline_ms=1))
        latest = wal.read_latest()

    assert latest is not None
    assert latest.sequence == 11
    assert latest.payload["deadline_ms"] == 600_000


@pytest.mark.parametrize("corruption_offset", (0, FILL_COOLDOWN_WAL_HEADER_BYTES + 4))
def test_corrupt_newer_wal_slot_falls_back_without_shortening_deadline(
    tmp_path: Path,
    corruption_offset: int,
) -> None:
    wal_path = tmp_path / "fill-cooldown.wal"
    with FillCooldownCheckpointWAL(wal_path) as wal:
        wal.write(_wal_payload(1, deadline_ms=500_000))
        wal.write(_wal_payload(2, deadline_ms=1))

    descriptor = os.open(wal_path, os.O_RDWR)
    try:
        absolute_offset = FILL_COOLDOWN_WAL_SLOT_BYTES + corruption_offset
        original = os.pread(descriptor, 1, absolute_offset)
        assert original
        os.pwrite(descriptor, bytes([original[0] ^ 0xFF]), absolute_offset)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)

    with FillCooldownCheckpointWAL(wal_path) as restarted:
        restored = restarted.read_latest()

    assert restored is not None
    assert restored.sequence == 1
    assert restored.payload["deadline_ms"] == 500_000
    assert restored.ignored_invalid_slots == (1,)


def test_engine_restore_uses_valid_second_slot_when_first_magic_is_torn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now_ms = 1_900_800_000_000
    _clock(monkeypatch, now_ms)
    checkpoint = tmp_path / "state.json"
    writer = _engine(checkpoint)
    writer._persist_fill_cooldown_checkpoint()
    writer._persist_fill_cooldown_checkpoint()
    writer.close_fill_cooldown_checkpoint_store()

    descriptor = os.open(checkpoint, os.O_RDWR)
    try:
        os.pwrite(descriptor, b"X", 0)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)

    restarted = _engine(checkpoint)
    restored = restarted.restore_fill_cooldown_checkpoint(now_ms=now_ms + 1_000)

    assert restored["checkpoint_sequence"] == 2
    assert restarted._fill_cooldown_checkpoint_sequence == 2


def test_truncated_newer_wal_slot_falls_back_and_next_write_repairs_file(
    tmp_path: Path,
) -> None:
    wal_path = tmp_path / "fill-cooldown.wal"
    with FillCooldownCheckpointWAL(wal_path) as wal:
        wal.write(_wal_payload(1, deadline_ms=500_000))
        wal.write(_wal_payload(2, deadline_ms=600_000))

    # Retain the complete first slot and only a torn prefix of the newer slot.
    os.truncate(
        wal_path,
        FILL_COOLDOWN_WAL_SLOT_BYTES + FILL_COOLDOWN_WAL_HEADER_BYTES + 3,
    )
    with FillCooldownCheckpointWAL(wal_path) as restarted:
        restored = restarted.read_latest()
        assert restored is not None
        assert restored.sequence == 1
        assert restored.payload["deadline_ms"] == 500_000
        restarted.write(_wal_payload(2, deadline_ms=700_000))
        repaired = restarted.read_latest()

    assert repaired is not None
    assert repaired.sequence == 2
    assert repaired.payload["deadline_ms"] == 700_000
    assert wal_path.stat().st_size == FILL_COOLDOWN_WAL_FILE_BYTES


def test_partial_newer_pwrite_falls_back_to_previous_valid_slot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wal_path = tmp_path / "fill-cooldown.wal"
    wal = FillCooldownCheckpointWAL(wal_path)
    wal.write(_wal_payload(1, deadline_ms=500_000))
    real_pwrite = checkpoint_store.os.pwrite
    injected = False

    def partial_then_fail(descriptor: int, raw: bytes, offset: int) -> int:
        nonlocal injected
        if not injected and offset >= FILL_COOLDOWN_WAL_SLOT_BYTES:
            injected = True
            partial = max(1, min(len(raw) // 2, FILL_COOLDOWN_WAL_HEADER_BYTES + 3))
            real_pwrite(descriptor, raw[:partial], offset)
            raise OSError("injected torn slot write")
        return real_pwrite(descriptor, raw, offset)

    monkeypatch.setattr(checkpoint_store.os, "pwrite", partial_then_fail)
    with pytest.raises(OSError, match="injected torn slot write"):
        wal.write(_wal_payload(2, deadline_ms=1))
    restored = wal.read_latest()
    wal.close()

    assert injected is True
    assert restored is not None
    assert restored.sequence == 1
    assert restored.payload["deadline_ms"] == 500_000


@pytest.mark.parametrize(
    ("fault_stage", "expected_sequence"),
    (
        ("before_pwrite", 1),
        # These two unit-test crash points have a structurally complete record.
        # Only the fdatasync boundary promises power-loss durability, but a
        # complete visible record is safe to accept after a process restart.
        ("after_pwrite", 2),
        ("after_fdatasync", 2),
    ),
)
def test_wal_fault_injection_boundaries_restore_a_complete_record(
    tmp_path: Path,
    fault_stage: str,
    expected_sequence: int,
) -> None:
    wal_path = tmp_path / f"fill-cooldown-{fault_stage}.wal"
    active_fault: str | None = None

    def inject(stage: str) -> None:
        if active_fault == stage:
            raise OSError(f"injected {stage}")

    wal = FillCooldownCheckpointWAL(wal_path, fault_injector=inject)
    wal.write(_wal_payload(1, deadline_ms=500_000))
    active_fault = fault_stage
    with pytest.raises(OSError, match=f"injected {fault_stage}"):
        wal.write(_wal_payload(2, deadline_ms=600_000))
    wal.close()

    with FillCooldownCheckpointWAL(wal_path) as restarted:
        restored = restarted.read_latest()

    assert restored is not None
    assert restored.sequence == expected_sequence
    assert restored.payload["deadline_ms"] in {500_000, 600_000}


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX process signals")
@pytest.mark.parametrize(
    ("fault_stage", "expected_first_gap_recovery"),
    (
        ("before_pwrite", 1),
        ("after_pwrite", 0),
        ("before_fdatasync", 0),
        ("after_fdatasync", 0),
    ),
)
def test_process_sigkill_at_wal_boundaries_never_shortens_or_loses_fill_gap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault_stage: str,
    expected_first_gap_recovery: int,
) -> None:
    """A real process death selects either old+gap or the complete new slot."""

    first_fill_ms = 1_900_900_000_000
    _clock(monkeypatch, first_fill_ms + 1_000)
    checkpoint = tmp_path / f"process-{fault_stage}.wal"
    baseline = _engine(checkpoint)
    _seed_buy_state(
        baseline,
        now_ms=first_fill_ms,
        duration_ms=85_000,
        units=1.0,
        identity="B0",
    )
    baseline._fill_cooldown_last_fill_cursor = {
        "trade_id": 100,
        "order_id": 10,
        "cumulative_filled_qty": 0.001,
        "trade_time_ms": first_fill_ms,
        "side": "BUY",
    }
    baseline._persist_fill_cooldown_checkpoint()
    baseline.close_fill_cooldown_checkpoint_store()
    target_payload = _target_second_fill_payload(
        tmp_path / f"target-{fault_stage}.wal",
        first_fill_ms=first_fill_ms,
    )

    pid = os.fork()
    if pid == 0:  # pragma: no cover - the parent verifies the process status.
        def kill_at_boundary(stage: str) -> None:
            if stage == fault_stage:
                os.kill(os.getpid(), signal_module.SIGKILL)

        wal = FillCooldownCheckpointWAL(
            checkpoint,
            fault_injector=kill_at_boundary,
        )
        wal.write(target_payload)
        os._exit(73)

    _wait_for_sigkill(pid)
    restarted, first_recovered, second_recovered = (
        _restore_and_reconcile_second_fill(
            checkpoint,
            first_fill_ms=first_fill_ms,
        )
    )

    assert first_recovered == expected_first_gap_recovery
    assert second_recovered == 0
    assert restarted._consec_buy == pytest.approx(2.0)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX process signals")
@pytest.mark.parametrize(
    ("crash_boundary", "expected_first_gap_recovery", "expected_marker"),
    (
        ("after_risk_cancel", 1, b"risk_cancel"),
        ("before_callback_return", 0, b"risk_cancel,callback_tail"),
    ),
)
def test_real_fill_callback_process_death_recovers_exactly_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_boundary: str,
    expected_first_gap_recovery: int,
    expected_marker: bytes,
) -> None:
    """Exercise the production fill ordering across actual SIGKILL restart."""

    first_fill_ms = 1_900_950_000_000
    _clock(monkeypatch, first_fill_ms + 1_000)
    checkpoint = tmp_path / f"callback-{crash_boundary}.wal"
    baseline = _engine(checkpoint)
    _seed_buy_state(
        baseline,
        now_ms=first_fill_ms,
        duration_ms=85_000,
        units=1.0,
        identity="B0",
    )
    baseline._fill_cooldown_last_fill_cursor = {
        "trade_id": 100,
        "order_id": 10,
        "cumulative_filled_qty": 0.001,
        "trade_time_ms": first_fill_ms,
        "side": "BUY",
    }
    baseline._persist_fill_cooldown_checkpoint()
    baseline.close_fill_cooldown_checkpoint_store()
    read_fd, write_fd = os.pipe()

    pid = os.fork()
    if pid == 0:  # pragma: no cover - the parent verifies durable recovery.
        os.close(read_fd)
        engine = _configure_process_fill_engine(
            checkpoint,
            now_ms=first_fill_ms + 1_000,
        )

        def risk_cancel(_side: str) -> None:
            os.write(write_fd, b"risk_cancel")
            if crash_boundary == "after_risk_cancel":
                os.kill(os.getpid(), signal_module.SIGKILL)

        def callback_tail(_cid: str) -> None:
            os.write(write_fd, b",callback_tail")
            os.kill(os.getpid(), signal_module.SIGKILL)

        engine._cancel_cooldown_side_order = risk_cancel
        engine._pop_order_context = callback_tail
        order = SimpleNamespace(
            side=Side.BUY,
            price=70_000.0,
            client_order_id="buy-process-fill",
            order_id=11,
            filled_qty=0.001,
            is_terminal=(crash_boundary == "before_callback_return"),
        )
        event = {
            "_fill_qty": 0.001,
            "_fill_price": 70_000.0,
            "_fill_commission": 0.0,
            "_fill_commission_asset": "USDC",
            "T": first_fill_ms + 1_000,
            "t": 101,
            "i": 11,
            "z": 0.001,
        }
        engine._on_fill(order, event)
        os._exit(74)

    os.close(write_fd)
    marker = b""
    while True:
        chunk = os.read(read_fd, 1024)
        if not chunk:
            break
        marker += chunk
    os.close(read_fd)
    _wait_for_sigkill(pid)
    assert marker == expected_marker

    restarted, first_recovered, second_recovered = (
        _restore_and_reconcile_second_fill(
            checkpoint,
            first_fill_ms=first_fill_ms,
        )
    )
    assert first_recovered == expected_first_gap_recovery
    assert second_recovered == 0
    assert restarted._consec_buy == pytest.approx(2.0)


def test_wal_rejects_symlink_and_non_private_files_but_allows_hardlinks(
    tmp_path: Path,
) -> None:
    target = tmp_path / "target.wal"
    with FillCooldownCheckpointWAL(target) as wal:
        wal.write(_wal_payload(1, deadline_ms=500_000))

    symlink = tmp_path / "symlink.wal"
    symlink.symlink_to(target)
    with pytest.raises(OSError):
        FillCooldownCheckpointWAL(symlink).read_latest()

    os.chmod(target, 0o640)
    with pytest.raises(PermissionError, match="0600"):
        FillCooldownCheckpointWAL(target).read_latest()
    os.chmod(target, 0o600)

    hardlink = tmp_path / "hardlink.wal"
    os.link(target, hardlink)
    with FillCooldownCheckpointWAL(target) as wal:
        assert wal.read_latest() is not None


def test_wal_fails_closed_when_both_slots_are_corrupt(tmp_path: Path) -> None:
    wal_path = tmp_path / "fill-cooldown.wal"
    with FillCooldownCheckpointWAL(wal_path) as wal:
        wal.write(_wal_payload(1, deadline_ms=500_000))
        wal.write(_wal_payload(2, deadline_ms=600_000))

    descriptor = os.open(wal_path, os.O_RDWR)
    try:
        os.pwrite(descriptor, b"X", 0)
        os.pwrite(descriptor, b"Y", FILL_COOLDOWN_WAL_SLOT_BYTES)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)

    with pytest.raises(
        FillCooldownCheckpointCorruptionError,
        match="no valid recovery slot",
    ):
        FillCooldownCheckpointWAL(wal_path).read_latest()
