import asyncio
import copy
import json

import pytest

from poolrun import __main__ as cli
from poolrun.batch import apply_plan, load_plan, summarize
from poolrun.master import Master
from poolrun.release import create
from poolrun.ssh_transport import ConnectionLost
from poolrun.store import atomic, digest, read


@pytest.fixture
def batch(tmp_path):
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'worker.py').write_text('print("synthetic only")\n')
    allow = tmp_path / 'include.txt'
    allow.write_text('worker.py\n')
    rel = create('p', source, allow, 'r1', dict(
        env_id='python', platforms=['test'], argv=['python', 'worker.py'],
        validator_argv=['python', 'worker.py'], checkpoint_contract='none',
        output_contract='synthetic'), tmp_path / 'packages')
    obj = dict(id='a' * 64, sha256='a' * 64, size=1, sources=[])
    atomic(tmp_path / 'object.json', obj)
    task = dict(id='one', project='p', inputs=[obj['id']], code_policy={'release': 'r1'},
                resources=dict(cpus=1, memory_bytes=100, disk_bytes=100), side_effects=False)
    (tmp_path / 'tasks.jsonl').write_text(json.dumps(task) + '\n')
    path = tmp_path / 'batch.json'
    atomic(path, dict(schema='poolrun.batch.v1', releases=[str(rel.relative_to(tmp_path))],
                      objects=['object.json'], tasks='tasks.jsonl'))
    return path


@pytest.fixture
def master(tmp_path):
    value = Master(tmp_path / 'master', {'hosts': {'h': dict(
        platform='test', slots=2, cpus=2, memory_bytes=1000, disk_bytes=1000)}})
    yield value
    value.store.close()


def send_to(master, fail_after=None):
    async def send(config, op, body):
        reply = master.handle('admin', op, body, body['request_id'])
        if body['op'] == fail_after:
            raise ConnectionLost('ACK lost after actual commit')
        return reply
    return send


def test_apply_registers_and_queues_once_after_lost_ack(batch, master):
    operations = load_plan(batch)
    assert [x['op'] for x in operations] == ['release', 'object', 'submit']
    with pytest.raises(ConnectionLost):
        asyncio.run(apply_plan({}, operations, send_to(master, 'submit')))
    assert list(master.store.state['tasks']) == ['one']
    result = asyncio.run(apply_plan({}, load_plan(batch), send_to(master)))
    assert result['status'] == 'ACKNOWLEDGED'
    assert master.store.state['tasks']['one']['generation'] == 0
    assert master.store.state['attempts'] == {}
    assert len(master.store.state['requests']) == 3


def test_apply_paths_relative_and_check_offline(batch, monkeypatch, capsys):
    monkeypatch.chdir(batch.parent / 'source')
    def forbidden(*args, **kwargs):
        raise AssertionError('offline check opened a connection')
    monkeypatch.setattr(cli, 'Client', forbidden)
    assert cli.main(['--config', 'absent.json', 'apply', '--plan', str(batch), '--check']) == 0
    assert json.loads(capsys.readouterr().out)['status'] == 'LOCAL_CHECK_PASSED'


def test_plan_rejects_partial_or_duplicate_inputs_before_mutation(batch):
    tasks = batch.parent / 'tasks.jsonl'
    tasks.write_text(tasks.read_text() * 2)
    with pytest.raises(ValueError, match='unique'):
        load_plan(batch)
    tasks.write_text('{}\n')
    with pytest.raises(ValueError, match='identities'):
        load_plan(batch)
    (batch.parent / 'packages/r1/code.zip').write_bytes(b'partial')
    with pytest.raises(ValueError, match='invalid release package'):
        load_plan(batch)


def test_apply_changed_spec_does_not_restart_or_overwrite(batch, master):
    asyncio.run(apply_plan({}, load_plan(batch), send_to(master)))
    before = copy.deepcopy(master.store.state['tasks'])
    task_file = batch.parent / 'tasks.jsonl'
    task = json.loads(task_file.read_text())
    task['parameters'] = {'changed': True}
    task_file.write_text(json.dumps(task))
    with pytest.raises(ValueError, match='immutable business spec conflict'):
        asyncio.run(apply_plan({}, load_plan(batch), send_to(master)))
    assert master.store.state['tasks'] == before


def test_saved_request_retry_uses_exact_payload_and_endpoint(tmp_path, monkeypatch, capsys):
    config = dict(socket='/example/socket', request_dir=str(tmp_path / 'requests'))
    atomic(tmp_path / 'client.json', config)
    body = dict(request_id='saved-request', op='submit', tasks=[])
    path = tmp_path / 'requests' / (digest(body['request_id']) + '.json')
    atomic(path, dict(request=dict(op='admin', body=body, endpoint=config['socket']),
                      status='UNKNOWN_UNTIL_ACK'))
    calls = []
    async def send(cfg, op, payload):
        calls.append((cfg, op, payload))
        return {'added': 0}
    monkeypatch.setattr(cli, 'remote', send)
    args = ['--config', str(tmp_path / 'client.json'), 'request-retry', '--id', 'saved-request']
    assert cli.main(args) == 0
    assert calls == [(config, 'admin', body)]
    config['socket'] = '/different/socket'
    atomic(tmp_path / 'client.json', config)
    assert cli.main(args) == 2
    assert len(calls) == 1
    assert 'endpoint differs' in capsys.readouterr().out


def test_remote_persists_unknown_and_retry_receives_same_master_ack(tmp_path, master, monkeypatch):
    config = dict(socket='/example/socket', request_dir=str(tmp_path / 'requests'))
    body = dict(request_id='object-operation', op='object', object=dict(
        id='b' * 64, sha256='b' * 64, size=0, sources=[]))
    class Client:
        lose = True
        def __init__(self, config):
            pass
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            pass
        async def request(self, op, payload):
            result = master.handle('admin', op, payload, payload['request_id'])
            if Client.lose:
                Client.lose = False
                raise ConnectionLost('lost after commit')
            return result
    monkeypatch.setattr(cli, 'Client', Client)
    with pytest.raises(ConnectionLost):
        asyncio.run(cli.remote(config, 'admin', body))
    record = tmp_path / 'requests' / (digest(body['request_id']) + '.json')
    assert read(record)['status'] == 'UNKNOWN_UNTIL_ACK'
    asyncio.run(cli.remote(config, 'admin', body))
    assert read(record)['status'] == 'ACKNOWLEDGED'
    assert len(master.store.state['objects']) == 1
    assert len(master.store.state['requests']) == 1


def test_summary_separates_attempts_preparation_results_and_project(master):
    state = copy.deepcopy(master.store.state)
    state['tasks'] = {
        'one': dict(spec={'project': 'p'}, status='RUNNING', attempt_id='new'),
        'two': dict(spec={'project': 'p'}, status='WAIT_DATA', attempt_id=None, reason='missing input'),
        'three': dict(spec={'project': 'p'}, status='RESULT_PENDING', attempt_id='upload'),
        'four': dict(spec={'project': 'other'}, status='COMPLETE', attempt_id=None),
    }
    state['attempts'] = {
        'new': dict(task_id='one', host='h', status='RUNNING'),
        'old': dict(task_id='one', host='h', status='FAILED'),
        'upload': dict(task_id='three', host='h', status='RESULT_PENDING'),
    }
    state['preparations'] = {'two': {'host': 'h'}}
    value = summarize(state, 'p')
    assert value['tasks'] == {'RUNNING': 1, 'WAIT_DATA': 1, 'RESULT_PENDING': 1}
    assert value['hosts']['h']['attempts'] == {'RUNNING': 1, 'RESULT_PENDING': 1}
    assert value['hosts']['h']['preparing'] == ['two']
    assert value['attention']['two']['reason'] == 'missing input'
    assert value['hosts']['h']['disk_free'] is None
