"""A narrow optional validator must preserve the Python event contract."""
import dataclasses
import math
from types import SimpleNamespace

import numpy as np
import pytest

import models.tick_data_types as types
from strategy.native_runtime import load_native_module, validate_native_capabilities
from test_replay_prepared import bundle as bundle, parameters


@pytest.fixture
def kernel(monkeypatch):
    pytest.importorskip("narrowgate_cpp")
    module = load_native_module()
    validate_native_capabilities(
        module, symbols=("canonical_book_levels_valid",),
        abi_versions={"CANONICAL_BOOK_LEVEL_CHECK_ABI": 1},
    )
    monkeypatch.setattr(types, "_NATIVE_LEVEL_CHECK", module.canonical_book_levels_valid)
    return module.canonical_book_levels_valid


def outcome(levels, **extra):
    try:
        return types.HistoricalExchangeBookEvent(
            market_id="market", event_type="delta", exchange_ts_ns=100,
            levels=levels, source="fixture", **extra,
        )
    except Exception as error:
        return type(error), str(error)


def test_scalar_quantity_validation_matches_numpy_reference(monkeypatch):
    monkeypatch.setattr(types, '_NATIVE_LEVEL_CHECK_ENABLED', False)
    rng = np.random.default_rng(438)
    values = list(rng.integers(0, 2**64, size=2000, dtype=np.uint64).view(np.float64))
    values += [0., -0., math.inf, -math.inf, math.nan, 5e-324, -5e-324,
               '1.5', 'nan', 'inf', 'bad', None]
    for value in values:
        with monkeypatch.context() as patch:
            patch.setattr(types, 'math', SimpleNamespace(isfinite=np.isfinite))
            expected = outcome((('bid', 100, value),))
        actual = outcome((('bid', 100, value),))
        assert actual == expected
        if isinstance(actual, types.HistoricalExchangeBookEvent):
            assert math.copysign(1., actual.levels[0][2]) == math.copysign(1., expected.levels[0][2])


@pytest.mark.parametrize("levels", [
    (("bid", 100, 1.0), ("ask", 101, 0.0)),
    (("ask", 101, -0.0), ("bid", 100, 2.0), ("bid", 100, 3.0)),
    (("bid", 10 ** 100, 1.0),),
    ((" BUY ", "100", "1.5"),),
    [["sell", 101.9, 0]],
    (("bid", True, 1.0),),
    (("bid", np.int64(100), np.float64(1)),),
    (("bogus", 100, 1.0),),
    (("bid", 0, 1.0),),
    (("bid", -1, 1.0),),
    (("bid", 100, -1.0),),
    (("bid", 100, math.inf),),
    (("bid", 100, math.nan),),
    (("bid", "bad", 1.0),),
    (("bid", 100, "bad"),),
    (("bid", 100),),
    (("bid", 100, 1.0, 2),),
    (), None,
])
def test_native_levels_preserve_values_order_and_errors(monkeypatch, kernel, levels):
    monkeypatch.setattr(types, "_NATIVE_LEVEL_CHECK_ENABLED", False)
    expected = outcome(levels)
    monkeypatch.setattr(types, "_NATIVE_LEVEL_CHECK_ENABLED", True)
    actual = outcome(levels)
    assert actual == expected
    if dataclasses.is_dataclass(actual):
        assert dataclasses.asdict(actual) == dataclasses.asdict(expected)
        for a, b in zip(actual.levels, expected.levels, strict=True):
            assert math.copysign(1, a[2]) == math.copysign(1, b[2])


def test_native_levels_rechecks_mutable_inputs(monkeypatch, kernel):
    monkeypatch.setattr(types, "_NATIVE_LEVEL_CHECK_ENABLED", True)
    levels = [["bid", 100, 1.0]]
    assert kernel(levels) is False
    assert isinstance(outcome(levels), types.HistoricalExchangeBookEvent)
    levels[0][2] = -1
    assert outcome(levels)[0] is ValueError


def test_native_levels_gap_and_empty_rules_unchanged(monkeypatch, kernel):
    for enabled in (False, True):
        monkeypatch.setattr(types, "_NATIVE_LEVEL_CHECK_ENABLED", enabled)
        empty = types.HistoricalExchangeBookEvent("market", "source_gap", 100)
        assert empty.levels == ()
        with pytest.raises(ValueError, match="cannot contain levels"):
            types.HistoricalExchangeBookEvent(
                "market", "source_gap", 100, levels=(("bid", 1, 0.0),),
            )


def test_native_failure_propagates_without_python_fallback(monkeypatch):
    def fail(_):
        raise RuntimeError("native failure")
    monkeypatch.setattr(types, "_NATIVE_LEVEL_CHECK_ENABLED", True)
    monkeypatch.setattr(types, "_NATIVE_LEVEL_CHECK", fail)
    assert outcome((("bid", 1, 0.0),)) == (RuntimeError, "native failure")


def test_native_disabled_never_calls_kernel(monkeypatch):
    def fail(_):
        raise AssertionError("disabled kernel called")
    monkeypatch.setattr(types, "_NATIVE_LEVEL_CHECK_ENABLED", False)
    monkeypatch.setattr(types, "_NATIVE_LEVEL_CHECK", fail)
    assert isinstance(outcome((("bid", 1, 0.0),)), types.HistoricalExchangeBookEvent)


def test_native_levels_full_replay_and_persisted_successor(
    bundle, monkeypatch, kernel, tmp_path,
):
    from models import backtest_tick as replay
    from models.replay import runtime_checkpoint_io as cio
    from test_tick_runtime_checkpoint import assert_same

    prepared = replay.prepare_public_inputs(bundle, tick_size=.1)
    monkeypatch.setattr(types, "_NATIVE_LEVEL_CHECK_ENABLED", False)
    expected = replay.simulate_prepared_inputs(prepared, parameters())
    monkeypatch.setattr(types, "_NATIVE_LEVEL_CHECK_ENABLED", True)
    assert_same(replay.simulate_prepared_inputs(prepared, parameters()), expected)
    cp = replay.simulate_prepared_inputs(
        prepared, parameters(), checkpoint_at_ts_ms=1800,
    )["_replay_checkpoint"]
    path = tmp_path / "native.checkpoint"
    cio.save_runtime_checkpoint(path, cp)
    restored = cio.load_trusted_runtime_checkpoint(path)
    successor = replay.simulate_prepared_inputs(
        prepared, parameters(), resume_checkpoint=restored,
        checkpoint_at_ts_ms=2600, consume_resume_checkpoint=True,
    )["_replay_checkpoint"]
    actual = replay.simulate_prepared_inputs(
        prepared, parameters(), resume_checkpoint=successor,
        consume_resume_checkpoint=True,
    )
    assert_same(actual, expected)
