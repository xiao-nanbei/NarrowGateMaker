"""Ownership boundaries of the extracted executor, beyond facade imports."""

import ast
import inspect
import subprocess
import sys
import textwrap
from dataclasses import FrozenInstanceError, fields

import pytest

from models import backtest_tick
from narrowgate.replay.executor import ReplayExecutor, ReplayServices
from narrowgate.replay.executor.state import ReplayContext, ReplayRuntimeState
from models.replay.runtime_checkpoint_io import (
    load_trusted_runtime_checkpoint,
    save_runtime_checkpoint,
)
from tests.test_tick_runtime_checkpoint import assert_same, scenario


def test_legacy_entry_assembles_but_does_not_own_economic_event_loop():
    source = ast.parse(textwrap.dedent(inspect.getsource(backtest_tick.simulate_tick)))
    assert not any(isinstance(node, (ast.For, ast.While, ast.AsyncFor)) for node in ast.walk(source))
    assert ReplayExecutor.run_events.__module__ == 'narrowgate.replay.executor.events'
    assert ReplayExecutor.initialize.__module__ == 'narrowgate.replay.executor.initialization'
    assert ReplayExecutor.finalize.__module__ == 'narrowgate.replay.executor.finalization'


def test_executor_can_import_without_legacy_facade_or_research():
    result = subprocess.run([sys.executable, '-B', '-c', '''
import sys
from narrowgate.replay.executor import ReplayExecutor
assert 'models.backtest_tick' not in sys.modules
assert not any(name == 'research' or name.startswith(('research.', 'live.')) for name in sys.modules)
assert ReplayExecutor.run_events.__module__ == 'narrowgate.replay.executor.events'
'''], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stdout + result.stderr


def test_executor_implementation_does_not_load_or_compile_source():
    from pathlib import Path
    import narrowgate.replay.executor as package

    owners = list(Path(package.__file__).parent.glob('*.py'))
    assert owners
    for path in owners:
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                assert node.func.id not in {'exec', 'eval', 'compile'}, (path.name, node.lineno)


def test_assembly_captures_current_dependencies_in_an_immutable_service_object(monkeypatch):
    services = ReplayServices(**{
        field.name: getattr(backtest_tick, field.name) for field in fields(ReplayServices)
    })
    with pytest.raises(FrozenInstanceError):
        services.compute_quote_core = object()

    class CapturedDependency(RuntimeError):
        pass

    def marker(*args, **kwargs):
        raise CapturedDependency('actual quote call reached injected dependency')

    # The owner is already imported; assembly must not keep a stale copied
    # legacy global, nor import the entire facade back into its modules.
    monkeypatch.setattr(backtest_tick, 'compute_quote_core', marker)
    args, kwargs = scenario('ordinary')
    with pytest.raises(CapturedDependency, match='actual quote call'):
        backtest_tick.simulate_tick(*args, **kwargs)
    assert services.compute_quote_core is not marker


@pytest.mark.parametrize('mode', ['ordinary', 'async', 'compute', 'timeout', 'emergency', 'close_replace'])
def test_actual_event_execution_keeps_exchange_before_quote_and_order_phases(monkeypatch, mode):
    market_phases = (
        '_bind_event', '_advance_event_market', '_match_event_orders',
        '_drain_planned_quote_stop',
    )
    quote_phases = (
        '_capture_quote_decision', '_refresh_quote_inputs', '_compute_quote_targets',
        '_record_quote_context', '_apply_quote_side_policies', '_apply_quote_response',
        '_apply_quote_inventory_limits', '_apply_quote_entry_policies',
        '_apply_quote_fill_cooldowns', '_apply_quote_recovery_actions',
        '_apply_order_action_policies', '_apply_order_safety_constraints',
        '_resolve_quote_order_updates', '_select_quote_risk_actions',
        '_finalize_order_actions', '_record_order_decisions',
        '_execute_order_decisions', '_finish_quote_decision',
    )
    events = []

    def traced(name, original):
        def run(self, *args, **kwargs):
            if name == '_bind_event':
                events.append([])
            events[-1].append(name)
            return original(self, *args, **kwargs)
        return run

    for name in (*market_phases, *quote_phases):
        monkeypatch.setattr(ReplayExecutor, name, traced(name, getattr(ReplayExecutor, name)))
    args, kwargs = scenario(mode)
    backtest_tick.simulate_tick(*args, **kwargs)
    full_decisions = 0
    for event in events:
        assert tuple(event[:4]) == market_phases
        quoted = tuple(name for name in event if name in quote_phases)
        assert quoted == quote_phases[:len(quoted)]
        full_decisions += quoted == quote_phases
    if mode == 'close_replace':
        # This fixture spends every opportunity in the close-only early exit.
        assert any('_capture_quote_decision' in event for event in events)
        assert full_decisions == 0
    else:
        assert full_decisions > 0


def test_actual_checkpoint_path_uses_state_owner_for_capture_transfer_and_clone(monkeypatch):
    calls = []
    capture = ReplayRuntimeState.capture_checkpoint
    restore = ReplayRuntimeState.restore_checkpoint_runtime

    def record_capture(runtime, *, cut_ts_ms):
        calls.append(('capture', cut_ts_ms))
        return capture(runtime, cut_ts_ms=cut_ts_ms)

    def record_restore(checkpoint, *, consume, immutable_inputs=()):
        calls.append(('restore', consume))
        return restore(checkpoint, consume=consume, immutable_inputs=immutable_inputs)

    monkeypatch.setattr(ReplayRuntimeState, 'capture_checkpoint', staticmethod(record_capture))
    monkeypatch.setattr(ReplayRuntimeState, 'restore_checkpoint_runtime', staticmethod(record_restore))
    args, kwargs = scenario('compute')
    expected = backtest_tick.simulate_tick(*args, **kwargs)
    first = backtest_tick.simulate_tick(*args, **kwargs, checkpoint_at_ts_ms=299)['_replay_checkpoint']
    second = backtest_tick.simulate_tick(
        *args, **kwargs, resume_checkpoint=first, consume_resume_checkpoint=True,
        checkpoint_at_ts_ms=1099,
    )['_replay_checkpoint']
    actual = backtest_tick.simulate_tick(*args, **kwargs, resume_checkpoint=second)
    assert_same(actual, expected)
    assert calls == [('capture', 299), ('restore', True), ('capture', 1099), ('restore', False)]
    assert first['consumed'] is True and 'runtime' not in first
    assert 'runtime' in second and not second.get('consumed')


@pytest.mark.parametrize('mode', ['ordinary', 'async', 'compute', 'timeout', 'emergency', 'close_replace'])
def test_checkpoint_owns_runtime_not_executor_or_process_context(mode, tmp_path):
    args, kwargs = scenario(mode)
    expected = backtest_tick.simulate_tick(*args, **kwargs)
    partial = backtest_tick.simulate_tick(*args, **kwargs, checkpoint_at_ts_ms=299)
    checkpoint = partial['_replay_checkpoint']
    runtime = checkpoint['runtime']
    assert isinstance(runtime, ReplayRuntimeState)
    assert not any(isinstance(value, (ReplayExecutor, ReplayContext, ReplayServices))
                   for value in vars(runtime).values())
    path = tmp_path / 'runtime.checkpoint'
    save_runtime_checkpoint(path, checkpoint)
    restored = load_trusted_runtime_checkpoint(path)
    assert isinstance(restored['runtime'], ReplayRuntimeState)
    actual = backtest_tick.simulate_tick(*args, **kwargs, resume_checkpoint=restored,
                                        consume_resume_checkpoint=True)
    assert_same(actual, expected)
    assert restored['consumed'] is True and 'runtime' not in restored
