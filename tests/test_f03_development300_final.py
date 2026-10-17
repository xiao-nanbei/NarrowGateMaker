from copy import deepcopy

import pytest

from research.families.f03_causal_13_head import development300_final as f


def cell():
    return dict(experiment_id=f.EXPERIMENT, baseline_id=f.final.BASELINE,
        parent_id='final-000-2026-05-28', arm_id=f.CANDIDATE,
        start_ns=f.final.START_NS, end_ns=f.final.START_NS+2*f.final.DAY_NS,
        input_manifest_id='1'*64, model_identity='2'*64, release_identity='3'*64,
        seed_pair={'rng_seed': 42, 'latency_seed': 59})


def test_exact_new_candidate_and_calendar():
    spec = cell()
    assert f.identity(spec) == spec
    last = dict(spec, parent_id='final-053-2026-09-11',
                start_ns=f.final.END_NS-f.final.DAY_NS, end_ns=f.final.END_NS)
    assert f.identity(last) == last


@pytest.mark.parametrize('key,value', [('arm_id', 'M0'), ('baseline_id', 'B0_RESPONSE_20261007'),
    ('experiment_id', f.final.EXPERIMENT), ('end_ns', f.final.END_NS+1)])
def test_no_old_arm_or_contract_alias(key, value):
    spec = cell()
    spec[key] = value
    with pytest.raises(ValueError):
        f.identity(spec)


def test_existing_executor_receives_only_new_identity(monkeypatch):
    calls = []
    monkeypatch.setattr(f.final, 'execute_cell', lambda prepared, **kwargs: calls.append((prepared, kwargs)))
    original = {'spec': cell(), 'initial_capital': 10000}
    frozen = deepcopy(original)
    f.execute_cell('same-prepare', **original)
    assert original == frozen
    assert calls == [('same-prepare', dict(original, experiment=f.EXPERIMENT, arms=f.ARMS))]
