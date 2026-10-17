from argparse import Namespace

import pytest

from models.tick_ab import (
    EXPERIMENTS,
    _boolean_arm_factory,
    _dynamic_skew_arm_factory,
    _inventory_arm_factory,
)


def test_migrated_tick_ab_families_are_registered() -> None:
    assert {"boolean", "dynamic_skew", "inventory_control"} <= set(EXPERIMENTS)


def test_boolean_factory_preserves_false_true_control_pairs() -> None:
    args = Namespace(
        tests=["dynamic_cap_enabled", "ml.enabled"],
        include_source_wiring_tests=False,
    )
    arms = _boolean_arm_factory(args)
    assert [arm.name for arm in arms] == [
        "dynamic_cap_enabled_false",
        "dynamic_cap_enabled_true",
        "ml.enabled_false",
        "ml.enabled_true",
    ]
    assert [arm.control for arm in arms] == [True, False, True, False]
    assert arms[2].ml_enabled is False
    assert arms[3].ml_enabled is True


def test_boolean_source_wiring_remains_explicitly_gated() -> None:
    args = Namespace(
        tests=["multi_market.enabled"],
        include_source_wiring_tests=False,
    )
    with pytest.raises(SystemExit, match="source-wiring diagnostic"):
        _boolean_arm_factory(args)


def test_dynamic_factory_respects_selected_test_and_grid() -> None:
    args = Namespace(tests=["ret_skew"], ret_skew_values=[5.0, 10.0])
    arms = _dynamic_skew_arm_factory(args)
    assert [arm.name for arm in arms] == [
        "baseline",
        "ret_skew_5",
        "ret_skew_10",
    ]
    assert arms[0].control


def test_inventory_factory_keeps_zero_control_and_cartesian_grid() -> None:
    args = Namespace(asym_values=[0.03], fade_values=[0.0, 0.5])
    arms = _inventory_arm_factory(args)
    assert arms[0].control
    assert arms[0].overrides == {
        "inventory_asym_strength": 0.0,
        "inventory_signal_fade_strength": 0.0,
    }
    assert len(arms) == 3
