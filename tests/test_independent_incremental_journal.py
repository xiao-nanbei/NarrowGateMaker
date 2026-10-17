"""Independent accounts keep hourly durability without quadratic journal copies."""
import json
from pathlib import Path

import pytest

from execution.chunked_parquet_journal import iter_chunked_parquet_journal
from research.families.f01_fixed_parameter_racing import mechanism_execution as shared
from research.families.f01_fixed_parameter_racing.inventory_soft_control import ARMS, arm_parameters
from research.families.f01_fixed_parameter_racing.inventory_soft_control_execution import diagnostics
from tests.test_defense_urgency_execution import options
from tests.test_inventory_soft_control import baseline
from tests.test_optional_update_branch import synthetic_parent  # noqa: F401
from tests.test_tick_runtime_checkpoint import assert_same


def configuration(prepared, base, output, arm, monkeypatch):
    old = options(prepared, base, output, arm)
    original = baseline()
    original.pop('account_start', None)
    original.pop('response_action_policy')
    params = dict(old['common_params'], **original)
    params = arm_parameters(json.loads(json.dumps(params)), arm)
    ident = old['spec']
    monkeypatch.setattr(shared, 'DAY_NS', (ident['end_ns']-ident['start_ns'])//2)
    monkeypatch.setattr(shared, 'HOUR_MS', 5000)  # Compressed fixture clock only.
    return dict(ident=ident, params=params, prepared_predictions=None,
        initial_capital=10000, max_mark_age_ns=old['max_mark_age_ns'],
        funding=old['funding'], output=output, full_calendar=True,
        diagnostic_builder=diagnostics, retain_completed_checkpoint=True,
        validate_output=lambda path: shared.validate_bound_cell(path, ident, lambda p, a: None))


def equal_outputs(left, right):
    for name in ('accounting', 'daily-ledger', 'effective_params', 'metrics', 'diagnostics'):
        assert json.loads((left/f'{name}.json').read_text()) == json.loads((right/f'{name}.json').read_text())
    a, b = [json.loads((root/'result.json').read_text()) for root in (left, right)]
    ja, jb = a.pop('_l2_journal'), b.pop('_l2_journal')
    assert_same(a, b)
    assert list(iter_chunked_parquet_journal(ja['manifest'], verify_checksum=True)) == list(
        iter_chunked_parquet_journal(jb['manifest'], verify_checksum=True))


@pytest.mark.parametrize('arm', ARMS[1:])
@pytest.mark.parametrize('crash_cut', [None, 1, 2])
def test_exact_outputs_with_hourly_saves_and_fresh_process_recovery(
        synthetic_parent, tmp_path, monkeypatch, arm, crash_cut):  # noqa: F811
    prepared, base = synthetic_parent
    cfg = configuration(prepared, base, tmp_path/'ordinary', arm, monkeypatch)
    shared.execute_bound_cell(prepared, **cfg)
    target = tmp_path/'incremental'
    cfg.update(output=target, incremental_journal=True)
    cuts = []
    def observed(status):
        cuts.append(status['durable_cut_ms'])
        prefixes = list((target/'journal').glob('*/prefix-*.json'))
        assert len(prefixes) == 1  # Only the latest valid recovery prefix.
        if len(cuts) == crash_cut:
            raise RuntimeError('simulated owner disconnect after durable save')
    if crash_cut is not None:
        with pytest.raises(RuntimeError, match='owner disconnect'):
            shared.execute_bound_cell(prepared, **cfg, progress=observed)
        assert (target/'latest.checkpoint').exists()
    assert shared.execute_bound_cell(prepared, **cfg, progress=observed)['status'] == 'COMPLETE'
    assert (target/'latest.checkpoint').exists()
    equal_outputs(tmp_path/'ordinary', target)
    assert shared.execute_bound_cell(prepared, **cfg)['status'] == 'ALREADY_COMPLETE'


def test_failed_replacement_save_keeps_previous_prefix(synthetic_parent, tmp_path, monkeypatch):  # noqa: F811
    import models.replay.runtime_checkpoint_io as checkpoint_io
    prepared, base = synthetic_parent
    cfg = configuration(prepared, base, tmp_path/'out', ARMS[1], monkeypatch)
    cfg['incremental_journal'] = True
    save = checkpoint_io.save_runtime_checkpoint
    saved = []
    def fail_second(path, state):
        if saved:
            raise OSError('simulated storage failure before replacement')
        save(path, state)
        saved.append(state['l2_prefix']['manifest'])
    monkeypatch.setattr(checkpoint_io, 'save_runtime_checkpoint', fail_second)
    with pytest.raises(OSError, match='storage failure'):
        shared.execute_bound_cell(prepared, **cfg)
    assert Path(saved[0]).is_file()
    checkpoint_io.load_trusted_runtime_checkpoint(tmp_path/'out/latest.checkpoint')
    monkeypatch.setattr(checkpoint_io, 'save_runtime_checkpoint', save)
    assert shared.execute_bound_cell(prepared, **cfg)['status'] == 'COMPLETE'


def test_non_retaining_owner_never_deletes_final_sink(synthetic_parent, tmp_path, monkeypatch):  # noqa: F811
    prepared, base = synthetic_parent
    cfg = configuration(prepared, base, tmp_path/'out', ARMS[1], monkeypatch)
    cfg.update(incremental_journal=True, retain_completed_checkpoint=False)
    assert shared.execute_bound_cell(prepared, **cfg)['status'] == 'COMPLETE'
    assert not (tmp_path/'out/latest.checkpoint').exists()
    cfg['validate_output'](tmp_path/'out')


def test_legacy_checkpoint_stays_on_original_release(synthetic_parent, tmp_path, monkeypatch):  # noqa: F811
    prepared, base = synthetic_parent
    cfg = configuration(prepared, base, tmp_path/'out', ARMS[1], monkeypatch)
    def crash(status):
        raise RuntimeError('stop after original hourly checkpoint')
    with pytest.raises(RuntimeError):
        shared.execute_bound_cell(prepared, **cfg, progress=crash)
    before = (tmp_path/'out/latest.checkpoint').read_bytes()
    journals = set((tmp_path/'out/journal').iterdir())
    with pytest.raises(ValueError, match='original journal release'):
        shared.execute_bound_cell(prepared, **cfg, incremental_journal=True)
    assert (tmp_path/'out/latest.checkpoint').read_bytes() == before
    assert set((tmp_path/'out/journal').iterdir()) == journals
    assert shared.execute_bound_cell(prepared, **cfg)['status'] == 'COMPLETE'


def test_final_output_failure_preserves_last_hour_recovery(synthetic_parent, tmp_path, monkeypatch):  # noqa: F811
    prepared, base = synthetic_parent
    cfg = configuration(prepared, base, tmp_path/'reference', ARMS[1], monkeypatch)
    shared.execute_bound_cell(prepared, **cfg)
    target = tmp_path/'failed'
    cfg.update(output=target, incremental_journal=True)
    atomic = shared._atomic
    def failed(path, value):
        if path.name == 'metrics.json':
            raise OSError('final output interruption')
        atomic(path, value)
    monkeypatch.setattr(shared, '_atomic', failed)
    with pytest.raises(OSError, match='output interruption'):
        shared.execute_bound_cell(prepared, **cfg)
    assert (target/'latest.checkpoint').is_file()
    assert not (target/'completion.json').exists()
    monkeypatch.setattr(shared, '_atomic', atomic)
    assert shared.execute_bound_cell(prepared, **cfg)['status'] == 'COMPLETE'
    equal_outputs(tmp_path/'reference', target)
