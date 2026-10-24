"""The state owner retains the original namespace/alias checkpoint contract."""

from types import SimpleNamespace

import numpy as np
import pytest

from narrowgate.replay.executor.state import ReplayRuntimeState


class _Writer:
    def __init__(self, events, name):
        self.events, self.name = events, name

    def checkpoint_prefix(self, receipt):
        self.events.append((self.name, receipt))
        return {"writer": self.name, "receipt": receipt}


class _Production:
    def receipt(self):
        return {"count": 3}


def _runtime(cls):
    order = {"qty": .002}
    events = []
    return cls(
        bid_orders=[order], pending=[order], q=.002, cash=-200.,
        rng=np.random.default_rng(17), si=2, max_samples=5,
        pnl_arr=np.arange(5.), inv_arr=np.arange(5.), ts_arr=np.arange(5),
        trade_ts=np.array([100, 200, 300]), n_trades=3,
        replay_event=(1, 200, True, False, False),
        replay_progress_callback=lambda _: None,
        l2_journal=_Writer(events, "l2"), target_journal=_Writer(events, "target"),
        l2_production=_Production(), target_production=_Production(),
        rows=np.arange(10),
    )


@pytest.mark.parametrize("cls", [SimpleNamespace, ReplayRuntimeState])
def test_capture_has_original_schema_aliases_prefixes_and_no_process_handles(cls):
    runtime = _runtime(cls)
    checkpoint = ReplayRuntimeState.capture_checkpoint(runtime, cut_ts_ms=200)
    assert checkpoint.keys() == {"schema", "scope", "cut_ts_ms", "loaded_window",
                                 "runtime", "next_event", "l2_prefix", "target_prefix"}
    assert checkpoint["schema"] == "tick_replay_runtime.v1"
    assert checkpoint["scope"] == "same_loaded_window_runtime_resume"
    assert checkpoint["loaded_window"] == (100, 300, 3)
    assert checkpoint["next_event"] is runtime.replay_event
    captured = checkpoint["runtime"]
    assert captured is not runtime
    assert captured.bid_orders[0] is captured.pending[0] is runtime.bid_orders[0]
    assert captured.rng is runtime.rng
    assert captured.replay_progress_callback is None
    assert captured.l2_journal is captured.target_journal is None
    assert "rows" not in vars(captured)
    assert "rows" in vars(runtime)
    assert callable(runtime.replay_progress_callback)
    assert runtime.l2_journal.events == [("l2", {"count": 3}), ("target", {"count": 3})]
    assert captured.max_samples == captured.si == 2
    for name in ("pnl_arr", "inv_arr", "ts_arr"):
        np.testing.assert_array_equal(getattr(captured, name), getattr(runtime, name)[:2])
        assert not np.shares_memory(getattr(captured, name), getattr(runtime, name))


@pytest.mark.parametrize("cls", [SimpleNamespace, ReplayRuntimeState])
def test_consuming_restore_transfers_root_identity_without_wrapping(cls):
    runtime = _runtime(cls)
    runtime.root_alias = runtime
    checkpoint = {"runtime": runtime}
    restored = ReplayRuntimeState.restore_checkpoint_runtime(checkpoint, consume=True)
    assert restored is runtime is restored.root_alias
    assert type(restored) is cls
    assert checkpoint == {"consumed": True}
    assert "rows" not in vars(restored)


@pytest.mark.parametrize("cls", [SimpleNamespace, ReplayRuntimeState])
def test_branch_restore_clones_alias_graph_and_rng_without_consuming(cls):
    checkpoint = ReplayRuntimeState.capture_checkpoint(_runtime(cls), cut_ts_ms=200)
    original = checkpoint["runtime"]
    restored = ReplayRuntimeState.restore_checkpoint_runtime(checkpoint, consume=False)
    assert checkpoint["runtime"] is original
    assert not checkpoint.get("consumed")
    assert restored is not original
    assert restored.bid_orders[0] is restored.pending[0]
    assert restored.bid_orders[0] is not original.bid_orders[0]
    assert restored.rng is not original.rng
    assert restored.rng.bit_generator.state == original.rng.bit_generator.state
    assert restored.q == original.q
    assert restored.cash == original.cash
