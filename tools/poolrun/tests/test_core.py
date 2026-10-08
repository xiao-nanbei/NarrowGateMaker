import copy
import json
from pathlib import Path
import pytest
from poolrun import model
from poolrun.store import Store, read
from poolrun.release import create, prepare
from poolrun.data import Cache


def state():
    s = model.initial()
    s["hosts"]["h"] = {"slots": 2, "memory_bytes": 1000, "disk_bytes": 1000, "cpus": 2, "platform": "test"}
    s["releases"]["p/r1"] = {"project":"p", "release_id":"r1", "platforms":["test"]}
    return s


def task(name="t"):
    return {"id":name,"project":"p","code_policy":{"release":"r1"},"resources":{"memory_bytes":100,"disk_bytes":100,"cpus":1},"side_effects":False}


def report():
    return {"releases":["p/r1"],"objects":[],"memory_available":1000,"disk_free":1000,"inodes_free":1000}


def test_atomic_idempotence_and_corruption(tmp_path):
    store = Store(tmp_path, {"counter":0})
    def inc(s): s["counter"] += 1; return s["counter"]
    assert store.transition("same",inc,{"x":1}) == 1
    assert store.transition("same",inc,{"x":1}) == 1
    with pytest.raises(ValueError): store.transition("same",inc,{"x":2})
    with pytest.raises(BlockingIOError): Store(tmp_path)
    store.close()
    (tmp_path/"state.json").write_text("broken")
    with pytest.raises(json.JSONDecodeError): Store(tmp_path)


def test_failed_commit_does_not_advance(tmp_path, monkeypatch):
    store = Store(tmp_path, {"counter":0})
    def fail(*args): raise OSError("disk full")
    monkeypatch.setattr("poolrun.store.atomic", fail)
    with pytest.raises(OSError): store.transition("x",lambda s:s.update(counter=1))
    assert store.state["counter"] == 0
    store.close()


def test_idempotent_reply_is_frozen_not_live_alias(tmp_path):
    store=Store(tmp_path,{"record":{"status":"ACTIVE"}})
    first=store.transition("acquire",lambda s:s["record"])
    store.transition("finish",lambda s:s["record"].update(status="COMPLETE"))
    assert store.transition("acquire",lambda s:None)==first=={"status":"ACTIVE"}
    store.close()


def test_import_rollout_start_binding():
    s=state(); spec=task(); model.submit(s,[spec]); s["releases"]["p/r2"] = dict(s["releases"]["p/r1"],release_id="r2")
    a=model.start(s,"t","h",report())
    model.rollout(s,"p","r2")
    assert a["release"]["release_id"]=="r1"
    model.submit(s,[task("second")]); model.rollout(s,"p","r2"); model.submit(s,[task("second")])
    assert s["tasks"]["second"]["code_policy"]["release"]=="r2"
    changed=task(); changed["parameters"]={"x":3}
    with pytest.raises(ValueError): model.submit(s,[changed])


def test_stale_result_fence_and_unknown():
    s=state(); model.submit(s,[task()]); a=model.start(s,"t","h",report())
    model.event(s,"h",{"attempt_id":a["id"],"kind":"UNKNOWN"})
    assert model.resources_used(s,"h")["slots"] == 1
    with pytest.raises(ValueError): model.retry(s,"t")
    model.retry(s,"t",uncertain=True)
    result=model.event(s,"h",{"attempt_id":a["id"],"kind":"COMPLETE"})
    assert result["accepted"] is False


@pytest.mark.parametrize('mismatch', ['none', 'pid', 'birth', 'boot', 'generation', 'reason', 'command', 'terminal', 'owner', 'replaced'])
def test_heartbeat_recovery_requires_same_live_attempt(mismatch):
    s = state(); model.submit(s, [task()]); a = model.start(s, 't', 'h', report())
    process = {'pid': 123, 'created': 100.0, 'boot': 10.0}
    model.event(s, 'h', {'attempt_id': a['id'], 'kind': 'RUNNING', 'process': process})
    model.event(s, 'h', {'attempt_id': a['id'], 'kind': 'UNKNOWN',
        'failure': {'reason': 'heartbeat lost, process death unproven'}})
    a = s['attempts'][a['id']]
    evidence = {'generation': a['generation'], 'process': dict(process)}
    host = 'h'
    if mismatch in ('pid', 'birth', 'boot'):
        evidence['process'][{'birth': 'created'}.get(mismatch, mismatch)] += 1
    if mismatch == 'generation': evidence['generation'] += 1
    if mismatch == 'reason': a['failure']['reason'] = 'runner exited without tree-exit receipt'
    if mismatch == 'command': a['command'] = {'kind': 'STOP'}
    if mismatch == 'terminal': a['status'] = 'COMPLETE'
    if mismatch == 'owner': host = 'other'
    if mismatch == 'replaced': s['tasks']['t']['attempt_id'] = 'other'
    before = copy.deepcopy(s)
    assert model.reconcile_heartbeat(s, host, a['id'], evidence) is (mismatch == 'none')
    if mismatch == 'none':
        assert a['status'] == s['tasks']['t']['status'] == 'RUNNING'
        assert 'finished' not in a and 'failure' not in a
        assert s['tasks']['t']['failures'] == 0
        assert len(s['attempts']) == 1
        assert not model.reconcile_heartbeat(s, host, a['id'], evidence)
    else:
        assert s == before


def test_fresh_agent_poll_recovers_heartbeat_without_dispatch(tmp_path):
    from poolrun.master import Master
    s = state(); model.submit(s, [task()]); a = model.start(s, 't', 'h', report())
    process = {'pid': 123, 'created': 100.0, 'boot': 10.0}
    model.event(s, 'h', {'attempt_id': a['id'], 'kind': 'RUNNING', 'process': process})
    model.event(s, 'h', {'attempt_id': a['id'], 'kind': 'UNKNOWN',
        'failure': {'reason': 'heartbeat lost, process death unproven'}})
    master = Master(tmp_path, {'hosts': s['hosts']})
    try:
        master.store.transition(None, lambda target: target.update(s))
        rpt = dict(report(), attempts=[a['id']])
        master.poll({'host': 'h', 'report': rpt})
        assert master.store.state['tasks']['t']['status'] == 'UNKNOWN'
        rpt['live_processes'] = {a['id']: {'generation': a['generation'], 'process': process}}
        answer = master.poll({'host': 'h', 'report': rpt})
        assert master.store.state['tasks']['t']['status'] == 'RUNNING'
        assert not answer.get('commands') and not answer.get('offer')
        assert len(master.store.state['attempts']) == 1
    finally:
        master.store.close()


def test_explicit_soft_memory_retry_is_bounded():
    s=state();spec=task();spec['max_attempts']=1
    model.submit(s,[spec]);a=model.start(s,'t','h',report())
    model.event(s,'h',{'attempt_id':a['id'],'kind':'FAILED','failure':{'reason':'soft_memory_watchdog','oom_evidence':'soft_watchdog'}})
    with pytest.raises(ValueError,match='attempt limit'):model.retry(s,'t')
    model.retry(s,'t',override_soft_memory_failure=True)
    assert not s['tasks']['t']['avoid_hosts']
    assert s['tasks']['t']['spec']['max_attempts']==1
    a=model.start(s,'t','h',report())
    model.event(s,'h',{'attempt_id':a['id'],'kind':'FAILED','failure':{'reason':'soft_memory_watchdog'}})
    with pytest.raises(ValueError,match='already used'):model.retry(s,'t',override_soft_memory_failure=True)


def test_explicit_retry_budget_preserves_spec_and_only_adds_one_attempt():
    s=state();spec=task();spec['max_attempts']=1
    model.submit(s,[spec]);a=model.start(s,'t','h',report())
    model.event(s,'h',{'attempt_id':a['id'],'kind':'FAILED','failure':{'reason':'input initialization'}})
    original=copy.deepcopy(s['tasks']['t']['spec'])
    with pytest.raises(ValueError,match='attempt limit'):model.retry(s,'t')
    for invalid in (True,1,0,2.5):
        with pytest.raises(ValueError,match='explicit attempt limit'):
            model.retry(s,'t',max_attempts=invalid)
    assert model.retry(s,'t',max_attempts=2)=={'status':'PENDING'}
    assert s['tasks']['t']['spec']==original
    assert s['attempts'][a['id']]['status']=='FAILED'
    second=model.start(s,'t','h',report())
    assert second['id']!=a['id']
    model.event(s,'h',{'attempt_id':second['id'],'kind':'FAILED'})
    with pytest.raises(ValueError,match='attempt limit'):model.retry(s,'t')


def test_admin_retry_budget_is_idempotent_and_not_agent_authority(tmp_path):
    from poolrun.master import Master
    s=state();spec=task();spec['max_attempts']=1
    model.submit(s,[spec]);a=model.start(s,'t','h',report())
    model.event(s,'h',{'attempt_id':a['id'],'kind':'FAILED'})
    master=Master(tmp_path,{'hosts':s['hosts']})
    try:
        master.store.transition('initialize',lambda target:target.update(s))
        body={'op':'retry','task':'t','max_attempts':2}
        with pytest.raises(PermissionError):master.handle('agent:h','admin',body,'retry-budget')
        assert master.handle('admin','admin',body,'retry-budget')=={'status':'PENDING'}
        assert master.handle('admin','admin',body,'retry-budget')=={'status':'PENDING'}
        assert master.store.state['tasks']['t']['attempt_limit_override']==2
    finally:master.store.close()


def test_admin_can_release_correctness_hold_without_touching_attempts(tmp_path):
    from poolrun.master import Master
    master = Master(tmp_path, {"hosts": state()["hosts"]})
    try:
        master.handle("admin", "admin", {"op": "hold", "project": "p"}, "hold")
        assert master.store.state["holds"] == ["p"]
        body = {"op": "hold", "project": "p", "enabled": False}
        with pytest.raises(PermissionError):
            master.handle("agent:h", "admin", body, "release")
        for _ in range(2):
            assert master.handle("admin", "admin", body, "release") == {"released": "p"}
        assert master.store.state["holds"] == []
        assert master.store.state["attempts"] == {}
    finally:
        master.store.close()


def test_pending_placement_preserves_business_and_started_attempts(tmp_path):
    from poolrun.master import Master
    s = state(); s['hosts']['other'] = dict(s['hosts']['h'])
    specs = [dict(task(name), hosts=['h']) for name in ('queued', 'active', 'retried')]
    model.submit(s, specs)
    active = model.start(s, 'active', 'h', report())
    retry = model.start(s, 'retried', 'h', report())
    model.event(s, 'h', {'attempt_id': retry['id'], 'kind': 'FAILED'})
    model.retry(s, 'retried')
    before = copy.deepcopy(s)
    master = Master(tmp_path, {'hosts': s['hosts']})
    try:
        master.store.transition('init', lambda target: target.update(s))
        body = {'op': 'pending-placement', 'project': 'p', 'hosts': ['other']}
        with pytest.raises(PermissionError):
            master.handle('agent:h', 'admin', body, 'move')
        for hosts in ([], ['missing'], ['h', 'h']):
            with pytest.raises(ValueError):
                master.handle('admin', 'admin', dict(body, hosts=hosts), str(hosts))
        result = master.handle('admin', 'admin', body, 'move')
        assert result['updated'] == ['queued']
        assert master.handle('admin', 'admin', body, 'move') == result
        after = master.store.state
        assert after['attempts'] == before['attempts']
        assert after['attempts'][active['id']]['host'] == 'h'
        for name in before['tasks']:
            assert after['tasks'][name]['spec_hash'] == before['tasks'][name]['spec_hash']
            assert after['tasks'][name]['spec'] == before['tasks'][name]['spec']
        assert model.eligible(after, after['tasks']['queued'], 'h', report(), 0) == 'host restriction'
        assert model.eligible(after, after['tasks']['queued'], 'other', report(), 0) is None
        assert 'placement_hosts' not in after['tasks']['active']
        assert 'placement_hosts' not in after['tasks']['retried']
    finally:
        master.store.close()


def test_guards_and_result_evidence():
    s=state();model.submit(s,[task()]);t=s["tasks"]["t"]
    assert "inode" in model.eligible(s,t,"h",dict(report(),inodes_free=0),0)
    assert "memory" in model.eligible(s,t,"h",dict(report(),memory_available=1),0)
    a=model.start(s,"t","h",report())
    with pytest.raises(ValueError): model.event(s,"h",{"attempt_id":a["id"],"kind":"COMPLETE"})
    model.event(s,"h",{"attempt_id":a["id"],"kind":"RESULT_PENDING"})
    assert model.resources_used(s,"h")["slots"]==0
    assert model.resources_used(s,"h")["disk_bytes"]==100


def test_owner_memory_admission_override_keeps_other_budgets():
    s=state(); model.submit(s,[task("a"),task("b"),task("c")])
    s["hosts"]["h"]["memory_bytes"]=1
    r=dict(report(),memory_available=0)
    assert model.eligible(s,s["tasks"]["a"],"h",r,0)=="reserved memory_bytes"
    s["hosts"]["h"]["memory_admission"]=False
    assert model.choose(s,"h",r)=="a"
    model.start(s,"a","h",r); model.start(s,"b","h",r)
    assert model.eligible(s,s["tasks"]["c"],"h",r,0)=="slots reserved"
    s["hosts"]["h"]["slots"]=3
    assert model.eligible(s,s["tasks"]["c"],"h",r,0)=="reserved cpus"
    s["hosts"]["h"]["cpus"]=3
    assert model.eligible(s,s["tasks"]["c"],"h",dict(r,disk_free=0),0)=="live disk guard"
    assert model.eligible(s,s["tasks"]["c"],"h",r,0) is None


def test_retry_resource_override_not_business_mutation():
    s=state();spec=task();model.submit(s,[spec]);a=model.start(s,"t","h",report())
    old=copy.deepcopy(s["tasks"]["t"]["spec"])
    model.event(s,"h",{"attempt_id":a["id"],"kind":"FAILED","failure":{"oom_evidence":"soft_watchdog"}})
    model.retry(s,"t",memory_bytes=200)
    assert s["tasks"]["t"]["spec"]==old
    assert "h" in s["tasks"]["t"]["avoid_hosts"]


def test_package_immutable_and_cache(tmp_path):
    work=tmp_path/"work";work.mkdir();(work/"a.py").write_text("one")
    include=tmp_path/"include";include.write_text("a.py\n")
    f=create("p",work,include,"r1",{},tmp_path/"packages")
    desc=read(f);data=(f.parent/"code.zip").read_bytes()
    (work/"a.py").write_text("two")
    target=prepare(tmp_path/"host",desc,data)
    assert (Path(target)/"a.py").read_text()=="one"
    with pytest.raises(ValueError): create("p",work,include,"r1",{},tmp_path/"packages")
    c=Cache(tmp_path/"host")
    from poolrun.release import sha
    part=tmp_path/"part";part.write_bytes(b"blob");oid=sha(part)
    c.publish(oid,part,4);assert oid in c.ready()
    assert c.gc([oid],[oid])==[]
    assert c.gc([],[oid])==[oid]


def test_pause_not_complete_and_hold():
    s=state();model.submit(s,[task()]);a=model.start(s,"t","h",report())
    model.event(s,"h",{"attempt_id":a["id"],"kind":"PAUSED","tree_exited":True,"checkpoint":{"file":"cp"}})
    assert s["tasks"]["t"]["status"]=="PAUSED"
    s["holds"].append("p")
    assert model.event(s,"h",{"attempt_id":a["id"],"kind":"COMPLETE"})["reason"]=="correctness hold"
