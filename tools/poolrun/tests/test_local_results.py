import asyncio
from poolrun.agent import Agent
from poolrun.store import Store, atomic


def test_host_disk_result_needs_no_network_ticket(tmp_path, monkeypatch):
    agent = Agent.__new__(Agent)
    agent.root = tmp_path
    agent.config = {'host': 'local', 'result_storage': 'host_disk', 'result_adapter': ['local-save']}
    agent.store = Store(tmp_path, {'errors': {}, 'result_acquires': {}})
    agent.uploads = set()
    events = []
    agent.enqueue = lambda aid, event: events.append(event)

    async def reject_network(*args):
        raise AssertionError('local persistence must not request a network ticket')

    agent.request = reject_network
    monkeypatch.setattr('poolrun.agent.adapter', lambda *args: {'durable': True, 'uri': str(tmp_path/'saved')})
    receipt = {'kind': 'RESULT_PENDING', 'result': {'files': [{'size': 100000000}]}}
    try:
        asyncio.run(agent._upload('attempt1', tmp_path, receipt))
        assert events[0]['kind'] == 'COMPLETE'
        assert (tmp_path/'durable.json').exists()
        assert not agent.store.state['result_acquires']
        assert not agent.store.state['errors']
    finally:
        agent.store.close()


def test_verified_durable_receipt_finishes_after_upload_retries_exhausted(tmp_path):
    agent = Agent.__new__(Agent)
    agent.root = tmp_path
    agent.config = {'host': 'local'}
    agent.store = Store(tmp_path, {
        'attempts': {'attempt1': {'directory': str(tmp_path)}},
        'events': {}, 'upload_retry': {'attempt1': [5, 0]},
        'acked': ['event-attempt1-RESULT_PENDING'],
    })
    agent.uploads = set()
    result = {'files': [{'file': 'result.json', 'sha256': 'a' * 64, 'size': 100}]}
    receipt = {'kind': 'RESULT_PENDING', 'result': result,
               'tree_exited': True, 'validated': True}
    durable = {'durable': True, 'uri': str(tmp_path/'saved'), 'files': result['files']}
    atomic(tmp_path/'exit.json', receipt)
    atomic(tmp_path/'durable.json', durable)
    try:
        # No running event loop: any attempt to schedule a new upload would fail.
        agent.supervise()
        event = agent.store.state['events']['event-attempt1-COMPLETE']['event']
        assert event == dict(receipt, kind='COMPLETE', durable_receipt=durable,
                             attempt_id='attempt1')
        agent.supervise()
        assert len(agent.store.state['events']) == 1
        assert agent.store.state['upload_retry']['attempt1'] == [5, 0]
    finally:
        agent.store.close()
