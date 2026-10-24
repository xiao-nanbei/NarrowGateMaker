from argparse import Namespace

import pytest

from models import backtest_comparison
from models.backtest_comparison import (
    EXPERIMENTS,
    _boolean_arm_factory,
    _dynamic_skew_arm_factory,
    _inventory_arm_factory,
)
from narrowgate.cli import main


def test_backtest_comparison_families_are_registered() -> None:
    assert {"boolean", "dynamic_skew", "inventory_control"} <= set(EXPERIMENTS)


def test_comparison_cli_lists_without_replaying(monkeypatch, capsys) -> None:
    def unexpected_replay(*args, **kwargs):
        pytest.fail("listing the comparison registry must not run a replay")

    monkeypatch.setattr(backtest_comparison, "run_experiment", unexpected_replay)
    assert main(["compare-backtests", "--list"]) == 0
    output = capsys.readouterr().out
    assert "narrowgate compare-backtests" in output
    assert all(name in output for name in EXPERIMENTS)


def test_comparison_cli_preserves_selected_arm_arguments(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(
        backtest_comparison,
        "run_experiment",
        lambda experiment, args: calls.append((experiment, args)),
    )
    args = ["--days", "2026-05-01", "--arms", "baseline", "--workers", "4"]
    assert main(["compare-backtests", "spread_cap", *args]) == 0
    assert calls == [(EXPERIMENTS["spread_cap"], args)]


def test_comparison_cli_rejects_unknown_experiment() -> None:
    with pytest.raises(SystemExit, match="Unknown experiment"):
        main(["compare-backtests", "not-a-registered-experiment"])


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
