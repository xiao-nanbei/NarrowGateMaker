"""Clock corrections must not turn live replay processes into UNKNOWN."""
from types import SimpleNamespace
import asyncio

import pytest

from poolrun import model, runner
from poolrun.agent import Agent
from poolrun.store import atomic
from test_core import report, state, task


@pytest.fixture
def clock_process(monkeypatch):
    values = SimpleNamespace(raw=100.125, adjusted=102.125, boot=10, session='boot-a', status='running')
    proc = SimpleNamespace(
        create_time=lambda: values.adjusted,
        status=lambda: values.status,
        _proc=SimpleNamespace(create_time=lambda *, monotonic: values.raw),
    )
    monkeypatch.setattr(runner.sys, 'platform', 'darwin')
    monkeypatch.setattr(runner.psutil, 'Process', lambda pid: proc)
    monkeypatch.setattr(runner.psutil, 'boot_time', lambda: values.boot)
    monkeypatch.setattr(runner, 'boot_session', lambda: values.session)
    return values


def test_clock_correction_and_import_epoch_do_not_change_identity(clock_process):
    original = runner.identity(12)
    clock_process.adjusted -= 2
    clock_process.boot += 1
    assert runner.alive(original)
    fresh = runner.identity(12)
    assert fresh['kernel_created'] == original['kernel_created']
    assert fresh['boot_session'] == original['boot_session']


@pytest.mark.parametrize('change', ['reuse', 'reboot', 'zombie', 'unknown_version', 'missing_birth'])
def test_stable_identity_rejects_replacement_or_incomplete_proof(clock_process, change):
    original = runner.identity(12)
    if change == 'reuse':
        clock_process.raw += .001
    elif change == 'reboot':
        clock_process.session = 'boot-b'
    elif change == 'zombie':
        clock_process.status = runner.psutil.STATUS_ZOMBIE
    elif change == 'unknown_version':
        original['identity_version'] = 3
    else:
        original.pop('kernel_created')
    assert not runner.alive(original)


def test_legacy_identity_is_not_silently_relaxed(clock_process):
    original = {'pid': 12, 'created': clock_process.adjusted, 'boot': clock_process.boot}
    assert runner.alive(original)
    clock_process.adjusted -= 2
    assert not runner.alive(original)


@pytest.mark.parametrize('reason', [
    'runner exited without tree-exit receipt',
    'runner liveness unconfirmed without tree-exit receipt',
])
@pytest.mark.parametrize('mismatch', ['none', 'unverified', 'pid', 'generation', 'owner', 'replaced'])
def test_runner_uncertainty_recovery_keeps_original_attempt(reason, mismatch):
    s = state()
    model.submit(s, [task()])
    a = model.start(s, 't', 'h', report())
    process = {'pid': 12, 'created': 100.125, 'boot': 10}
    model.event(s, 'h', {'kind': 'RUNNING', 'attempt_id': a['id'], 'process': process})
    model.event(s, 'h', {'kind': 'UNKNOWN', 'attempt_id': a['id'], 'failure': {'reason': reason}})
    evidence = {'process': dict(process), 'generation': a['generation'], 'stable_runner_verified': True}
    host = 'h'
    if mismatch == 'unverified':
        evidence.pop('stable_runner_verified')
    elif mismatch == 'pid':
        evidence['process']['pid'] = 13
    elif mismatch == 'generation':
        evidence['generation'] += 1
    elif mismatch == 'owner':
        host = 'other'
    elif mismatch == 'replaced':
        s['tasks']['t']['attempt_id'] = 'different'
    assert model.reconcile_heartbeat(s, host, a['id'], evidence) is (mismatch == 'none')
    if mismatch == 'none':
        a = s['attempts'][a['id']]
        assert a['heartbeat_recovery']['previous_failure'] == {'reason': reason}
        assert a['status'] == s['tasks']['t']['status'] == 'RUNNING'
        assert s['tasks']['t']['generation'] == 1
        assert len(s['attempts']) == 1


def test_agent_reports_original_identity_only_after_both_stable_checks(tmp_path, monkeypatch):
    agent = Agent({'root': str(tmp_path), 'host': 'h', 'socket': str(tmp_path/'unused.sock')})
    root = tmp_path/'attempt'
    root.mkdir()
    original = {'pid': 12, 'created': 100.125, 'boot': 10}
    atomic(root/'attempt.json', {'generation': 1})
    atomic(root/'process.json', dict(original, identity_version=2, original_identity=original))
    atomic(root/'runner.json', {'pid': 13, 'identity_version': 2})
    agent.store.transition(None, lambda s: s['attempts'].update({'a': {'directory': str(root)}}))
    try:
        monkeypatch.setattr('poolrun.agent.alive', lambda info: True)
        assert agent.report()['live_processes']['a'] == {
            'process': original, 'generation': 1, 'stable_runner_verified': True,
        }
        monkeypatch.setattr('poolrun.agent.alive', lambda info: info['pid'] == 12)
        assert agent.report()['live_processes'] == {}
        atomic(root/'exit.json', {'kind': 'RESULT_PENDING'})
        monkeypatch.setattr('poolrun.agent.alive', lambda info: True)
        assert agent.report()['live_processes'] == {}
    finally:
        agent.store.close()


def test_reconciliation_only_never_approves_or_launches_environment(tmp_path, monkeypatch):
    definition = {'python': '/changed/python', 'python_sha256': 'old'}
    atomic(tmp_path/'agent-state.json', {'attempts': {}, 'releases': {}, 'events': {},
                                       'environments': {'old': definition}, 'errors': {}})
    config = {'root': str(tmp_path), 'host': 'h', 'socket': str(tmp_path/'unused.sock'),
              'reconcile_only': True, 'environments': {'old': definition}}
    monkeypatch.setattr('poolrun.agent.verify_environment', lambda d: pytest.fail('not an approval'))
    agent = Agent(config)
    try:
        with pytest.raises(ValueError, match='cannot launch'):
            agent.launch({})
        with pytest.raises(ValueError, match='cannot stage'):
            asyncio.run(agent.staging({}))
        assert agent.store.state['environments'] == {'old': definition}
        assert agent.report()['environment_bindings'] == {}
    finally:
        agent.store.close()
    config['environments'] = {'new': definition}
    with pytest.raises(ValueError, match='cannot register'):
        Agent(config)


def test_retained_environment_is_not_approval_for_new_work(tmp_path):
    atomic(tmp_path/'agent-state.json', {'attempts': {}, 'releases': {}, 'events': {},
        'environments': {'old': {'python': '/old/python'}}, 'errors': {}})
    agent = Agent({'root': str(tmp_path), 'host': 'h', 'socket': str(tmp_path/'unused.sock')})
    try:
        assert agent.report()['environment_bindings'] == {}
        with pytest.raises(ValueError, match='no longer approved'):
            agent.launch({'id': 'new', 'release': {'env_id': 'old'}})
        assert agent.store.state['attempts'] == {}
    finally:
        agent.store.close()
