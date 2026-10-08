from __future__ import annotations

import pytest

from strategy.buy_soft_widen_release import (
    evaluate_buy_soft_widen_release,
    inventory_role,
)


def _evaluate(**overrides):
    params = {
        "enabled": True,
        "decision_ts_ms": 1_000,
        "target_decision_ts_ms": 1_000,
        "target_role": "opener",
        "inventory_btc": 0.0,
        "lot_size_btc": 0.001,
        "allow_post": True,
        "allow_exposure_increase": True,
        "hard_reason_active": False,
        "baseline_spread_mult": 1.35,
        "spread_mult_cap": 1.0,
    }
    params.update(overrides)
    return evaluate_buy_soft_widen_release(**params)


def test_inventory_role_is_pre_decision_and_side_specific() -> None:
    assert inventory_role(0.0, 0.001) == "opener"
    assert inventory_role(0.001, 0.001) == "add"
    assert inventory_role(-0.001, 0.001) == "reducing"


def test_exact_opener_target_releases_only_existing_soft_widen() -> None:
    decision = _evaluate()
    assert decision.target_reached
    assert decision.role == "opener"
    assert decision.eligible
    assert decision.requested
    assert decision.effective
    assert decision.baseline_spread_mult == pytest.approx(1.35)
    assert decision.selected_spread_mult == pytest.approx(1.0)


def test_add_is_separate_and_reducing_never_qualifies() -> None:
    add = _evaluate(target_role="add", inventory_btc=0.002)
    reducing = _evaluate(target_role="add", inventory_btc=-0.002)
    assert add.eligible and add.effective
    assert reducing.role == "reducing"
    assert not reducing.eligible
    assert not reducing.requested


def test_wrong_timestamp_or_hard_block_cannot_mutate_policy() -> None:
    not_target = _evaluate(decision_ts_ms=999)
    blocked = _evaluate(hard_reason_active=True)
    assert not not_target.target_reached
    assert not_target.selected_spread_mult == pytest.approx(1.35)
    assert blocked.target_reached
    assert not blocked.eligible
    assert blocked.selected_spread_mult == pytest.approx(1.35)


def test_v1_rejects_action_redefinition() -> None:
    with pytest.raises(ValueError, match="spread_mult_cap=1.0"):
        _evaluate(spread_mult_cap=1.1)
