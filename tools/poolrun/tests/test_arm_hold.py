import copy
import time

import pytest

from poolrun import model
from poolrun.master import Master
from test_core import report, state, task


def arm_task(name, arm, project='p'):
    return dict(task(name), project=project, parameters={'cell': {'arm_id': arm}})


def test_arm_hold_covers_future_pending_not_other_arms_or_active_attempts(tmp_path):
    master = Master(tmp_path, {'hosts': state()['hosts']})
    try:
        master.store.transition(None, lambda s: s.update(releases=state()['releases']))
        master.store.transition(None, lambda s: model.submit(s, [arm_task('running', 'R2'), arm_task('keep', 'R1')]))
        master.store.transition(None, lambda s: model.start(s, 'running', 'h', report()))
        attempts = copy.deepcopy(master.store.state['attempts'])
        body = {'op': 'hold', 'project': 'p', 'arm_id': 'R2'}
        with pytest.raises(PermissionError):
            master.handle('agent:h', 'admin', body, 'forbidden-arm-hold')
        assert master.handle('admin', 'admin', body, 'owner-arm-hold')['held']
        assert master.store.state['attempts'] == attempts
        assert master.store.state['holds'] == []
        master.store.transition(None, lambda s: model.submit(s, [arm_task('future', 'R2')]))
        s = master.store.state
        assert model.eligible(s, s['tasks']['future'], 'h', report(), time.time()) == 'owner arm dispatch hold'
        assert model.eligible(s, s['tasks']['keep'], 'h', report(), time.time()) is None
        with pytest.raises(ValueError, match='owner arm dispatch hold'):
            model.start(s, 'future', 'h', report())
        assert not master.handle('admin', 'admin', dict(body, enabled=False), 'owner-arm-release')['held']
        assert model.eligible(master.store.state, master.store.state['tasks']['future'], 'h', report(), time.time()) is None
        with pytest.raises(ValueError, match='unknown project/arm'):
            master.handle('admin', 'admin', dict(body, project='other'), 'wrong-project-arm')
    finally:
        master.store.close()
