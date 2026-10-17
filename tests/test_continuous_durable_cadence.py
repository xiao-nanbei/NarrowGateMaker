"""Operational callback cadence must not change simulated window/state flow."""
from copy import deepcopy
from types import SimpleNamespace

import pytest

from research.families.f01_fixed_parameter_racing import defense_urgency_continuous as runner

HOUR = 3_600_000
END = 107 * 24 * HOUR


def run(monkeypatch, interval=None, resume=None):
    import models.backtest_tick as tick
    import models.replay.public_accounting as accounting
    import models.replay.runtime_input_window as windows
    from research.families.f01_fixed_parameter_racing import defense_urgency as arms

    monkeypatch.setattr(runner, 'continuous_bounds', lambda _: (0, END * 10**6))
    monkeypatch.setattr(runner, 'admit_continuous_inputs', lambda *a, **k: None)
    monkeypatch.setattr(arms, 'arm_parameters', lambda p, a: (dict(p), a))
    monkeypatch.setattr(windows, 'runtime_input_context_start', lambda runtime, begin, context, **k: begin)
    monkeypatch.setattr(accounting, 'public_equity_ledger', lambda *a, **k: {'accounting': {'complete': True}})
    calls, saved, loaded = [], [], []

    def simulate(prepared, params, *, prepared_predictions, checkpoint_at_ts_ms,
                 resume_checkpoint, consume_resume_checkpoint):
        previous = 0 if resume_checkpoint is None else resume_checkpoint['runtime']['value']
        cut = END if checkpoint_at_ts_ms is None else checkpoint_at_ts_ms
        calls.append((cut, previous, consume_resume_checkpoint))
        return {'value': previous + 1, '_replay_checkpoint': {
            'cut_ts_ms': cut, 'runtime': {'value': previous + 1}}}

    def prepare(begin, end, **kwargs):
        loaded.append((begin, end, kwargs))
        return object(), object()

    monkeypatch.setattr(tick, 'simulate_prepared_inputs', simulate)
    timelines = SimpleNamespace(bundle=SimpleNamespace(manifest={'plan': {'start_ns': 0}}, root='test'), prepare=prepare)
    result = runner.replay_continuous_final_windows(timelines, 'B0', calendar={},
        selection_path=None, run_identity='test', common_params={'tick_size': .1},
        initial_capital=10000, max_mark_age_ns=10**9, funding={},
        on_checkpoint=lambda arm, cp: saved.append(deepcopy(cp)),
        resume_checkpoint=resume, durable_interval_ms=interval,
        separate_feature_context=True)
    return result, calls, saved, loaded


def test_daily_persistence_leaves_all_hourly_windows_and_state_identical(monkeypatch):
    hourly = run(monkeypatch)
    daily = run(monkeypatch, 24 * HOUR)
    assert hourly[0] == daily[0]
    assert hourly[1] == daily[1]
    assert hourly[3] == daily[3]
    assert len(hourly[2]) == 2567
    assert len(daily[2]) == 107  # first hour + 106 complete UTC days
    assert [c['cut_ts_ms'] for c in daily[2]] == [HOUR] + list(range(24 * HOUR, END, 24 * HOUR))


def test_resume_publishes_first_window_then_aligned_days(monkeypatch):
    full = run(monkeypatch, 24 * HOUR)
    checkpoint = full[2][2]
    resumed = run(monkeypatch, 24 * HOUR, deepcopy(checkpoint))
    assert resumed[0] == full[0]
    assert resumed[2][0]['cut_ts_ms'] == checkpoint['cut_ts_ms'] + HOUR
    assert all(c['cut_ts_ms'] % (24 * HOUR) == 0 for c in resumed[2][1:])


@pytest.mark.parametrize('interval', [True, 0, -1, HOUR//2, HOUR+1, 25*HOUR])
def test_reject_invalid_durable_interval(monkeypatch, interval):
    with pytest.raises(ValueError, match='durable interval'):
        run(monkeypatch, interval)
