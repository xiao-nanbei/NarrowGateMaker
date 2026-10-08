import asyncio
from poolrun.agent import Agent
from poolrun.store import Store


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
