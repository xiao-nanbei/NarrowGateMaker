import json

import pytest

from narrowgate.replay.account import effective_parameter_record
from research.families.f08_side_taker_lifecycle.depth_price_execution import (
    validate_identity, verify_final_selection,
)
from research.families.f08_side_taker_lifecycle.depth_price_replay import DepthPriceReplayRuntime


def cell():
    policy = dict(arm='PRICE_ONLY',constant=[.1,-.1])
    ident = dict(experiment_id='F08_DEPTH_PRICING_ADAPTIVE_20261024',
        baseline_id='B0_20261007_02', account_contract='DEPTH_PRICE_INDEPENDENT407',
        parent_id='development-000-2025-08-01',arm_id='PRICE_ONLY_u+10_v-10',policy=policy,
        seed_pair=dict(rng_seed=42,latency_seed=59),start_ns=1754006400000000000,
        end_ns=1754179200000000000,input_manifest_id='a'*64,release_identity='b'*64)
    baseline = dict(spec=dict(ident,arm_id='DEFENSE_ON__URGENCY_OFF'))
    return ident,baseline,policy


def test_original_account_and_fixed_control_identity():
    args = cell()
    assert validate_identity(*args) == args[0]
    result = validate_identity(*args)
    result['policy']['constant'][0] = 0
    assert args[0]['policy']['constant'] == [.1,-.1]


@pytest.mark.parametrize('field,value', [('parent_id','development-000-2025-08-02'),
    ('end_ns',1754179200000000001),('seed_pair',dict(rng_seed=43,latency_seed=59)),
    ('arm_id','PRICE_ONLY_u+10_v+10'),('baseline_id','current'),('release_identity','unfrozen')])
def test_reject_foreign_identity(field,value):
    ident,baseline,policy = cell()
    ident[field] = value
    with pytest.raises(ValueError):
        validate_identity(ident,baseline,policy)


def test_no_implicit_final_constant_selection():
    ident,baseline,policy = cell()
    ident.update(parent_id='final-000-2026-05-28',start_ns=1779926400000000000,
                 end_ns=1780099200000000000)
    baseline['spec'] = dict(ident,arm_id='DEFENSE_ON__URGENCY_OFF')
    with pytest.raises(ValueError,match='selection'):
        validate_identity(ident,baseline,policy)


def test_parameter_record_keeps_frozen_contract_not_mutation_or_observer():
    runtime = DepthPriceReplayRuntime(arm='PRICE_ONLY',constant=(.1,.1),tick_size=.1,
        cost_profile=dict(measurement_id='synthetic-only',publication_ns=0,query_ns=100))
    params = dict(_quote_price_runtime=runtime,_replay_progress_callback=lambda row: None)
    before = effective_parameter_record(params)
    runtime.begin_capture(100)
    runtime.correction()
    runtime.finish_capture()
    assert effective_parameter_record(params) == before
    assert before == {'_quote_price_runtime':json.loads(json.dumps(runtime.contract))}
    with pytest.raises(TypeError):
        effective_parameter_record({'arbitrary_object':object()})


def test_final_selection_reads_complete_frozen_record(tmp_path):
    from narrowgate.replay.account import sha
    path = tmp_path/'selection.json'
    record = dict(experiment='F08_DEPTH_PRICING_ADAPTIVE_20261024', baseline='B0_20261007_02',
                  selection=dict(parents=150, days=300, final_outcomes_used=False,
                                 constant=[.1, -.1], reuse_baseline=False))
    path.write_text(json.dumps(record))
    policy = dict(arm='PRICE_ONLY', constant=[.1, -.1],
                  development_selection=dict(path=str(path), sha256=sha(path)))
    assert verify_final_selection(policy) == record
    policy['constant'] = [-.1, -.1]
    with pytest.raises(ValueError, match='differs from full'):
        verify_final_selection(policy)
    policy['constant'] = [.1, -.1]
    path.write_text(json.dumps(dict(record, baseline='different')))
    with pytest.raises(ValueError, match='selection changed'):
        verify_final_selection(policy)


@pytest.mark.parametrize('change', [dict(parents=149), dict(days=298),
    dict(final_outcomes_used=True), dict(reuse_baseline=True)])
def test_final_selection_rejects_partial_future_tuned_or_duplicate_zero(tmp_path, change):
    from narrowgate.replay.account import sha
    path = tmp_path/'selection.json'
    selected = dict(parents=150, days=300, final_outcomes_used=False,
                    constant=[.1, -.1], reuse_baseline=False)
    selected.update(change)
    path.write_text(json.dumps(dict(experiment='F08_DEPTH_PRICING_ADAPTIVE_20261024',
                                    baseline='B0_20261007_02', selection=selected)))
    with pytest.raises(ValueError):
        verify_final_selection(dict(constant=[.1, -.1],
            development_selection=dict(path=str(path), sha256=sha(path))))


def test_final_selection_does_not_accept_a_truthy_flag():
    with pytest.raises(ValueError, match='frozen Development record'):
        verify_final_selection(dict(constant=[.1, -.1], development_selection=True))
