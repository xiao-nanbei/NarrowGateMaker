import numpy as np

from research.families.f08_side_taker_lifecycle.response_action_labels import same_result, advance_to_root


def test_full_result_arrays_and_nan_compare_without_ambiguous_truth():
    left = dict(curve=np.array([1., np.nan, 3.]), fills=[{'quantity': np.float64(.001)}])
    right = dict(curve=np.array([1., np.nan, 3.]), fills=[{'quantity': .001}])
    assert same_result(left, right)
    right['curve'][2] = 4.
    assert not same_result(left, right)
    assert not same_result(left, {'curve': np.array([1., np.nan])})


def test_same_time_sides_reuse_root_without_reentering_executor():
    import pytest
    root = {'cut_ts_ms': 5001}
    def forbidden(*args, **kwargs):
        raise AssertionError('same cut must not execute')
    assert advance_to_root(forbidden, None, {}, None, root, 5001) is root
    with pytest.raises(ValueError, match='backwards'):
        advance_to_root(forbidden, None, {}, None, root, 5000)
    def forward(*args, **kwargs):
        assert kwargs['resume_checkpoint'] is root
        assert kwargs['checkpoint_at_ts_ms'] == 5010
        return {'_replay_checkpoint': {'cut_ts_ms': 5010}}
    assert advance_to_root(forward, None, {}, None, root, 5010)['cut_ts_ms'] == 5010


def test_root_transfer_preserves_independent_following_and_branch_clones():
    import copy
    root = {'cut_ts_ms': 1, 'runtime': {'rows': []}}
    following = copy.deepcopy(root)
    def simulate(*args, **kwargs):
        old = kwargs['resume_checkpoint']
        assert kwargs['consume_resume_checkpoint']
        state = old.pop('runtime')
        old['consumed'] = True
        state['rows'].append(kwargs['checkpoint_at_ts_ms'])
        return {'_replay_checkpoint': {'cut_ts_ms': kwargs['checkpoint_at_ts_ms'], 'runtime': state}}
    next_root = advance_to_root(simulate, None, {}, None, root, 2,
                                consume_resume_checkpoint=True)
    assert root['consumed'] and 'runtime' not in root
    for _ in range(2):
        branch = copy.deepcopy(next_root)
        branch['runtime']['rows'].append('branch')
    assert next_root['runtime']['rows'] == [2]
    assert following['runtime']['rows'] == []
    assert advance_to_root(simulate, None, {}, None, next_root, 2,
                           consume_resume_checkpoint=True) is next_root
    final = advance_to_root(simulate, None, {}, None, next_root, 3,
                            consume_resume_checkpoint=True)
    assert final['runtime']['rows'] == [2, 3]


def test_external_phase_timing_restores_wrappers_after_failure():
    from types import SimpleNamespace
    import pytest
    from models.replay.phase_timing import ReplayPhaseTiming
    timing = ReplayPhaseTiming()
    def fail():
        raise ValueError('expected')
    owner = SimpleNamespace(call=fail)
    with pytest.raises(ValueError):
        with timing.measure_methods([(owner, 'call', 'nested')]):
            with timing.phase('parent'):
                with timing.phase('output'):
                    owner.call()
    assert owner.call is fail
    assert timing.current is None
    assert timing.report()['nested_calls'] == {'nested': 1}
    assert set(timing.report()['exclusive_wall']) == {'parent', 'output'}
