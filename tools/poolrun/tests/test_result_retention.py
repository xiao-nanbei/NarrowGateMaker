import asyncio
import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest

from poolrun.agent import Agent
from poolrun.result_retention import no_open_files, retire
from poolrun.store import Store, atomic, digest, read


@pytest.fixture
def case(tmp_path, monkeypatch):
    agent_root = tmp_path/'agent'
    root = agent_root/'attempts/task/a1'
    source = root/'outputs'
    target = tmp_path/'retained/a1'
    source.mkdir(parents=True)
    target.mkdir(parents=True)
    files = []
    for name in ('one', 'nested/two'):
        data = name.encode()
        for base in (source, target):
            (base/name).parent.mkdir(exist_ok=True)
            (base/name).write_bytes(data)
        files.append({'file': name, 'size': len(data), 'sha256': hashlib.sha256(data).hexdigest()})
    receipt = dict(kind='RESULT_PENDING', tree_exited=True, validated=True, result={'files': files})
    saved = dict(durable=True, uri=str(target), files=files)
    event = dict(receipt, kind='COMPLETE', attempt_id='a1', durable_receipt=saved)
    for name, value in [('attempt', {'id': 'a1'}), ('exit', receipt), ('durable', saved),
                        ('process', {'pid': 991}), ('runner', {'pid': 992})]:
        atomic(root/(name+'.json'), value)
    (root/'checkpoints').mkdir()
    (root/'checkpoints/needed').write_text('keep')
    entry = {'directory': str(root), 'result_retirement': {
        'status': 'pending', 'event_sha256': digest(event), 'retry_at': 0}}
    policy = {'mode': 'retire_local_publication', 'roots': [str(target.parent)]}
    monkeypatch.setattr('poolrun.result_retention.psutil.pid_exists', lambda pid: False)
    monkeypatch.setattr('poolrun.result_retention.no_open_files', lambda path: None)
    return SimpleNamespace(agent_root=agent_root, root=root, source=source, target=target,
                           files=files, entry=entry, policy=policy, event=event)


def run(c):
    return retire(c.agent_root, 'a1', c.entry, c.policy)


def test_retire_only_manifest_copies_and_retry(case):
    (case.source/'unlisted').write_text('unique')
    result = run(case)
    assert result['logical_bytes_removed_this_pass'] == sum(f['size'] for f in case.files)
    assert run(case)['logical_bytes_removed_this_pass'] == 0
    assert (case.source/'unlisted').read_text() == 'unique'
    assert (case.root/'checkpoints/needed').read_text() == 'keep'
    assert (case.target/'one').read_text() == 'one'


@pytest.mark.parametrize('failure', ['target_corrupt', 'source_corrupt', 'ack_changed', 'manifest_changed',
                                    'target_missing', 'pid_alive', 'unapproved_root', 'linked_parent',
                                    'overlap', 'not_exited', 'foreign_attempt', 'open_files'])
def test_fail_closed(case, monkeypatch, failure):
    if failure == 'target_corrupt': (case.target/'one').write_text('bad')
    elif failure == 'source_corrupt': (case.source/'one').write_text('bad')
    elif failure == 'ack_changed': case.entry['result_retirement']['event_sha256'] = 'wrong'
    elif failure in ('manifest_changed', 'overlap'):
        saved = read(case.root/'durable.json')
        if failure == 'manifest_changed': saved['files'] = []
        else: saved['uri'] = str(case.source)
        atomic(case.root/'durable.json', saved)
        case.entry['result_retirement']['event_sha256'] = digest(dict(case.event, durable_receipt=saved))
    elif failure == 'target_missing': (case.target/'nested/two').unlink()
    elif failure == 'pid_alive': monkeypatch.setattr('poolrun.result_retention.psutil.pid_exists', lambda pid: True)
    elif failure == 'unapproved_root': case.policy['roots'] = [str(case.root)]
    elif failure == 'linked_parent':
        old = case.target/'nested'
        old.rename(case.target/'moved')
        old.symlink_to(case.target/'moved', target_is_directory=True)
    elif failure == 'not_exited':
        receipt = read(case.root/'exit.json'); receipt['tree_exited'] = False
        atomic(case.root/'exit.json', receipt)
    elif failure == 'foreign_attempt': atomic(case.root/'attempt.json', {'id': 'other'})
    elif failure == 'open_files':
        def blocked(path): raise ValueError('opened')
        monkeypatch.setattr('poolrun.result_retention.no_open_files', blocked)
    with pytest.raises((ValueError, OSError)):
        run(case)
    assert (case.source/'one').exists()
    assert (case.source/'nested/two').exists()


def test_partial_unlink_failure_resumes_without_economic_restart(case, monkeypatch):
    original = Path.unlink
    def fail_second(path, *args, **kwargs):
        if path == case.source/'nested/two': raise OSError('disk error')
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'unlink', fail_second)
    with pytest.raises(OSError): run(case)
    assert not (case.source/'one').exists()
    monkeypatch.setattr(Path, 'unlink', original)
    assert run(case)['logical_bytes_removed_this_pass'] == len(b'nested/two')
    assert all((case.target/f['file']).exists() for f in case.files)


def make_agent(c, tmp_path):
    agent = Agent.__new__(Agent)
    agent.config = {'host': 'h', 'result_retention': c.policy}
    agent.root = c.agent_root
    agent.store = Store(tmp_path/'state', {'attempts': {'a1': {'directory': str(c.root)}},
        'events': {'event-a1-COMPLETE': {'event': c.event}}, 'acked': []})
    agent.heavy_lock = asyncio.Lock()
    agent.retiring = None
    agent.uploads = set()
    agent.preparing = None
    agent.tasks = set()
    return agent


@pytest.mark.parametrize('answer,queued', [({'accepted': True}, True),
    ({'accepted': False, 'reason': 'stale generation'}, False),
    ({'accepted': False, 'reason': 'correctness hold'}, False)])
def test_only_positive_master_ack_schedules_cleanup(case, tmp_path, answer, queued):
    agent = make_agent(case, tmp_path)
    async def request(*args): return answer
    agent.request = request
    try:
        asyncio.run(agent.flush())
        assert ('result_retirement' in agent.store.state['attempts']['a1']) is queued
        assert (case.source/'one').exists()
        if queued:
            # Reopen persisted state, as after an Agent restart.
            path = agent.store.root
            agent.store.close(); agent.store = Store(path)
            async def maintenance():
                agent.schedule_retirement()
                agent.schedule_retirement()
                assert len(agent.tasks) == 1
                await asyncio.gather(*agent.tasks)
            asyncio.run(maintenance())
            assert agent.store.state['attempts']['a1']['result_retirement']['status'] == 'retired'
    finally:
        agent.store.close()


def test_cleanup_failure_is_visible_and_does_not_change_completion(case, tmp_path):
    agent = make_agent(case, tmp_path)
    agent.store.transition(None, lambda s: s['attempts']['a1'].update(case.entry))
    (case.target/'one').unlink()
    try:
        asyncio.run(agent.retire_result('a1'))
        state = agent.store.state['attempts']['a1']['result_retirement']
        assert state['status'] == 'blocked'
        assert state['retry_at'] > 0
        assert (case.source/'one').exists()
        assert not agent.uploads
    finally:
        agent.store.close()


@pytest.mark.parametrize('code,out,err,blocked', [(1, '', '', False), (0, 'p123', '', True),
    (1, '', 'permission denied', True), (2, '', '', True)])
def test_open_file_probe_requires_unambiguous_negative(monkeypatch, code, out, err, blocked):
    monkeypatch.setattr('poolrun.result_retention.subprocess.run',
        lambda *a, **kw: SimpleNamespace(returncode=code, stdout=out, stderr=err))
    if blocked:
        with pytest.raises(ValueError): no_open_files('/tmp')
    else:
        no_open_files('/tmp')
