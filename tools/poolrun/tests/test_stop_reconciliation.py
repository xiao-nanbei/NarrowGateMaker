import pytest

from poolrun import runner
from poolrun.store import atomic, read


@pytest.fixture
def stopped(tmp_path, monkeypatch):
    atomic(tmp_path/'attempt.json', {'id': 'a'})
    atomic(tmp_path/'exit.json', {'kind': 'UNKNOWN', 'failure': {'reason': 'permission'}})
    atomic(tmp_path/'control/terminate.json',
           {'kind': 'STOP', 'mode': 'terminate', 'attempt_id': 'a', 'request_id': 'r'})
    atomic(tmp_path/'process.json', {'pid': 98761})
    atomic(tmp_path/'runner.json', {'pid': 98762})
    monkeypatch.setattr(runner, 'alive', lambda _: False)
    monkeypatch.setattr(runner.psutil, 'pid_exists', lambda _: False)
    def gone(*_):
        raise ProcessLookupError()
    monkeypatch.setattr(runner.os, 'killpg', gone)
    return tmp_path


def test_verified_stop_preserves_prior_receipt_and_is_idempotent(stopped):
    result = runner.reconcile_terminated(stopped)
    assert result['kind'] == 'STOPPED' and result['tree_exited']
    assert read(stopped/'exit-before-stop-reconciliation.json')['kind'] == 'UNKNOWN'
    assert runner.reconcile_terminated(stopped) == result


@pytest.mark.parametrize('obstacle', ['worker', 'pid_reuse', 'group', 'permission', 'foreign_stop'])
def test_uncertain_tree_or_foreign_stop_does_not_publish_exit(stopped, monkeypatch, obstacle):
    if obstacle == 'worker':
        monkeypatch.setattr(runner, 'alive', lambda _: True)
    elif obstacle == 'pid_reuse':
        monkeypatch.setattr(runner.psutil, 'pid_exists', lambda _: True)
    elif obstacle == 'group':
        monkeypatch.setattr(runner.os, 'killpg', lambda *_: None)
    elif obstacle == 'permission':
        def denied(*_):
            raise PermissionError()
        monkeypatch.setattr(runner.os, 'killpg', denied)
    else:
        atomic(stopped/'control/terminate.json',
               {'kind': 'STOP', 'mode': 'terminate', 'attempt_id': 'other', 'request_id': 'r'})
    with pytest.raises((ValueError, PermissionError)):
        runner.reconcile_terminated(stopped)
    assert read(stopped/'exit.json')['kind'] == 'UNKNOWN'
