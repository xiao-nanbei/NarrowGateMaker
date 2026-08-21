from __future__ import annotations

from types import SimpleNamespace

import pytest

from models.backtest_tick import _resolve_fill_cooldown_reset_policy
from strategy.fill_cooldown import (
    RESET_OPPOSITE_FILL_ONLY,
    RESET_OPPOSITE_FILL_OR_EXPIRY,
    cooldown_duration_after_same_side_fill,
    normalize_consecutive_reset_policy,
    update_same_side_fill_units,
)
from strategy.maker_engine import MakerEngine


def test_formal_replay_requires_explicit_reset_policy() -> None:
    with pytest.raises(ValueError, match="requires fill_cooldown"):
        _resolve_fill_cooldown_reset_policy({"replay_purpose": "formal"})

    assert _resolve_fill_cooldown_reset_policy(
        {
            "replay_purpose": "formal",
            "fill_cooldown_consecutive_reset_policy": RESET_OPPOSITE_FILL_ONLY,
        }
    ) == (RESET_OPPOSITE_FILL_ONLY, False)


def test_legacy_boolean_is_historical_compatibility_only() -> None:
    assert normalize_consecutive_reset_policy(
        "", legacy_reset_on_expiry=True
    ) == RESET_OPPOSITE_FILL_OR_EXPIRY
    with pytest.raises(ValueError, match="conflicts"):
        normalize_consecutive_reset_policy(
            RESET_OPPOSITE_FILL_ONLY,
            legacy_reset_on_expiry=True,
        )


def test_same_side_units_include_reducing_fill_quantity() -> None:
    buy, sell, units = update_same_side_fill_units(
        side="BUY",
        fill_qty=0.0005,
        order_size=0.001,
        lot_size=0.001,
        buy_units=1.0,
        sell_units=4.0,
    )
    assert units == pytest.approx(0.5)
    assert buy == pytest.approx(1.5)
    assert sell == 0.0

    # Role is deliberately absent from the API: reducing and add fills use the
    # same side/quantity counter contract.
    buy, sell, units = update_same_side_fill_units(
        side="SELL",
        fill_qty=0.002,
        order_size=0.001,
        lot_size=0.001,
        buy_units=buy,
        sell_units=sell,
    )
    assert units == pytest.approx(2.0)
    assert buy == 0.0
    assert sell == pytest.approx(2.0)


def test_zero_duration_reducing_fill_preserves_absolute_add_deadline() -> None:
    # An add fill started an 85-second BUY cooldown at t=10s. A same-side
    # reducing fill at t=40s increments the BUY units but must leave the live
    # deadline at t=95s, represented as 55 seconds relative to the new fill.
    remaining = cooldown_duration_after_same_side_fill(
        previous_fill_ts_ms=10_000,
        previous_cooldown_ms=85_000.0,
        current_fill_ts_ms=40_000,
        new_cooldown_ms=0.0,
    )
    assert remaining == pytest.approx(55_000.0)
    assert 40_000 + remaining == pytest.approx(95_000.0)

    restarted = cooldown_duration_after_same_side_fill(
        previous_fill_ts_ms=10_000,
        previous_cooldown_ms=85_000.0,
        current_fill_ts_ms=40_000,
        new_cooldown_ms=170_000.0,
    )
    assert restarted == pytest.approx(170_000.0)

    expired = cooldown_duration_after_same_side_fill(
        previous_fill_ts_ms=10_000,
        previous_cooldown_ms=20_000.0,
        current_fill_ts_ms=40_000,
        new_cooldown_ms=0.0,
    )
    assert expired == 0.0


def _bare_engine(policy: str) -> MakerEngine:
    engine = object.__new__(MakerEngine)
    engine.cfg = SimpleNamespace(
        strategy=SimpleNamespace(
            fill_cooldown_consecutive_reset_policy=policy,
        )
    )
    engine._fill_cooldown_until = {"BUY": 0.0, "SELL": 0.0}
    engine._consec_buy = 2.5
    engine._consec_sell = 0.0
    engine._last_same_side_fill_epoch_ms = {"BUY": 900, "SELL": 0}
    engine._last_fill_side = "BUY"
    return engine


def test_live_baseline_expiry_preserves_units_and_state_roundtrips() -> None:
    engine = _bare_engine(RESET_OPPOSITE_FILL_ONLY)
    engine._fill_cooldown_until["BUY"] = 2.0
    engine._expire_fill_cooldown_state("BUY", 3.0)
    assert engine._consec_buy == pytest.approx(2.5)

    engine._fill_cooldown_until["BUY"] = 5.0
    payload = engine.fill_cooldown_state_snapshot(now_ms=3_000)
    restored = _bare_engine(RESET_OPPOSITE_FILL_ONLY)
    restored._consec_buy = 0.0
    restored.restore_fill_cooldown_state(payload, now_ms=10_000)
    assert restored._consec_buy == pytest.approx(2.5)
    assert restored._fill_cooldown_until["BUY"] == pytest.approx(12.0)
    assert restored._last_fill_side == "BUY"


def test_historical_expiry_policy_clears_units() -> None:
    engine = _bare_engine(RESET_OPPOSITE_FILL_OR_EXPIRY)
    engine._fill_cooldown_until["BUY"] = 2.0
    engine._expire_fill_cooldown_state("BUY", 3.0)
    assert engine._consec_buy == 0.0


def test_buy_e3_rollback_clamps_long_deadline_to_natural_b0() -> None:
    source = _bare_engine(RESET_OPPOSITE_FILL_ONLY)
    source._consec_buy = 2.0
    source._last_same_side_fill_epoch_ms["BUY"] = 1_000
    source._fill_cooldown_until["BUY"] = 2_049.0
    source._fill_cooldown_deadline_identity = {
        "BUY": f"BUY_E3:{'a' * 64}",
        "SELL": "B0",
    }
    payload = source.fill_cooldown_state_snapshot(now_ms=1_000)

    rollback = _bare_engine(RESET_OPPOSITE_FILL_ONLY)
    rollback._buy_e3_cooldown_policy = None
    rollback.restore_fill_cooldown_state(payload, now_ms=10_000)

    # Natural B0 deadline is 1s + 85s * 2 units = 171s, not 2049s.
    assert rollback._fill_cooldown_until["BUY"] == pytest.approx(171.0)
    assert rollback._fill_cooldown_deadline_identity["BUY"] == "B0"


def test_same_buy_e3_artifact_may_restore_its_exact_deadline() -> None:
    source = _bare_engine(RESET_OPPOSITE_FILL_ONLY)
    identity = f"BUY_E3:{'b' * 64}"
    source._fill_cooldown_until["BUY"] = 2_049.0
    source._fill_cooldown_deadline_identity = {"BUY": identity, "SELL": "B0"}
    payload = source.fill_cooldown_state_snapshot(now_ms=1_000)

    restored = _bare_engine(RESET_OPPOSITE_FILL_ONLY)
    restored._buy_e3_cooldown_policy = SimpleNamespace(deadline_identity=identity)
    restored.restore_fill_cooldown_state(payload, now_ms=10_000)
    assert restored._fill_cooldown_until["BUY"] == pytest.approx(2058.0)
    assert restored._fill_cooldown_deadline_identity["BUY"] == identity
