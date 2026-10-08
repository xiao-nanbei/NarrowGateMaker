import asyncio
import copy
from pathlib import Path
import subprocess
import sys
import time
import pytest
from poolrun import model,transfer
from poolrun.agent import Agent
from poolrun.config import reject_legacy
from poolrun.data import Cache
from poolrun.release import sha
from poolrun.runner import adapter
from poolrun.store import Store,atomic,read
from test_core import state,task,report


def test_dependency_and_correctness_hold():
    s=state();a=task("a");b=task("b");b["depends_on"]=["a"]
    model.submit(s,[a,b]);assert "WAIT_DEPENDENCY" in model.eligible(s,s["tasks"]["b"],"h",report(),1)
    s["tasks"]["a"]["status"]="COMPLETE";s["holds"].append("p")
    assert "hold" in model.eligible(s,s["tasks"]["b"],"h",report(),1)


def test_transfer_quotas_and_noncloud_route():
    s=state();s["hosts"]["other"]=copy.deepcopy(s["hosts"]["h"])
    obj={"id":"a"*64,"size":10*2**20};source={"route":"direct","zone":"noncloud"}
    policy={"global_concurrency":1,"routes":{"direct":{},"netdisk":{}}}
    with pytest.raises(ValueError,match="noncloud"):transfer.admit(s,"h",obj,source,policy)
    source["route"]="netdisk"
    ticket=transfer.admit(s,"h",obj,source,policy)
    assert transfer.admit(s,"h",obj,source,policy)==ticket
    with pytest.raises(ValueError,match="quota"):transfer.admit(s,"other",obj,source,policy)
    transfer.finish(s,"h",ticket["id"],True,10)
    assert transfer.admit(s,"other",obj,source,policy)["host"]=="other"


def test_corrupt_and_partial_download_never_ready(tmp_path):
    c=Cache(tmp_path);part=tmp_path/"partial";part.write_bytes(b"bad")
    with pytest.raises(ValueError):c.publish("0"*64,part,100)
    assert c.ready()==[]
    link=c.root/"blobs"/("a"*64);link.symlink_to(part)
    with pytest.raises(ValueError):c.gc([],["a"*64])


def test_retired_control_configuration_rejected():
    for config in ({'master': 'http://127.0.0.1'}, {'dev_http': True}, {'admin_token': 'old'}, {'ca_file': 'old'}):
        with pytest.raises(ValueError, match='migrate'): reject_legacy(config)


def test_actual_atomic_kill_window(tmp_path):
    atomic(tmp_path/"state.json",{"value":0})
    code="from poolrun.store import atomic\nfrom pathlib import Path\np=Path(__import__('sys').argv[1])\nfor i in range(100000): atomic(p, {'value':i,'payload':'x'*20000})"
    p=subprocess.Popen([sys.executable,"-c",code,str(tmp_path/"state.json")])
    time.sleep(.12);p.kill();p.wait()
    assert isinstance(read(tmp_path/"state.json")["value"],int)


def test_incompatible_checkpoint_adapter(tmp_path):
    project=Path(__file__).resolve().parents[1]/"examples/synthetic"
    cp=tmp_path/"cp";atomic(cp,{"contract":"old","cursor":1,"values":[3]})
    r=adapter([sys.executable,str(project/"checkpoint_adapter.py")],{
        "checkpoint":{"path":str(cp),"sha256":sha(cp),"spec_hash":"same"},"spec_hash":"same",
        "target_release":{"checkpoint_contract":"synthetic-v1"},"business":{"parameters":{"steps":2}}},tmp_path)
    assert r["decision"]=="INCOMPATIBLE" and r["resume_argv"]==[]


def test_duplicate_launch_intent_becomes_unknown(tmp_path):
    root=tmp_path/"attempt";root.mkdir()
    atomic(root/"attempt.json",{"id":"a"})
    atomic(root/"spawn-intent.json",{"previous":"ambiguous"})
    subprocess.run([sys.executable,"-m","poolrun.runner",str(root)],check=True)
    assert read(root/"exit.json")["kind"]=="UNKNOWN"


def test_sigkill_is_not_assumed_oom():
    s=state();model.submit(s,[task()]);a=model.start(s,"t","h",report())
    model.event(s,"h",{"attempt_id":a["id"],"kind":"FAILED","failure":{"reason":"exit_137","oom_evidence":None}})
    model.retry(s,"t")
    assert s["tasks"]["t"]["avoid_hosts"]==[]


def test_request_failure_poison_stops_subsequent_dispatch(tmp_path,monkeypatch):
    store=Store(tmp_path,{"x":0})
    monkeypatch.setattr("poolrun.store.atomic",lambda *a:(_ for _ in ()).throw(OSError("fsync")))
    with pytest.raises(OSError):store.transition("a",lambda s:s.update(x=1))
    with pytest.raises(RuntimeError,match="restart"):store.transition("b",lambda s:s.update(x=2))
    store.close()


def test_agent_preparation_error_is_persisted(tmp_path):
    agent = Agent({"root": str(tmp_path), "host": "h", 'ssh': {'target': 'unused', 'identity_file': '/unused', 'known_hosts_file': '/unused'}})
    try:
        asyncio.run(agent._staging({"task_id": "t", "reservation": {"memory_bytes": 2**100}}))
        assert agent.store.state["errors"]["t"] == "preparation memory guard"
        assert agent.store.state["staging_retry"]["t"][0] == 1
        assert agent.preparing is None
    finally:
        agent.store.close()


def test_agent_upload_error_does_not_recompute(tmp_path):
    agent = Agent({"root": str(tmp_path), "host": "h", 'ssh': {'target': 'unused', 'identity_file': '/unused', 'known_hosts_file': '/unused'}})
    try:
        asyncio.run(agent._upload("attempt-a", tmp_path, {}))
        assert agent.store.state["errors"]["attempt-a"] == "RESULT_PENDING: no durable result adapter configured"
        assert agent.store.state["upload_retry"]["attempt-a"][0] == 1
        assert agent.store.state["attempts"] == {}
    finally:
        agent.store.close()


def test_agent_liveness_requires_runner_and_child_and_no_exit(tmp_path, monkeypatch):
    agent = Agent({'root': str(tmp_path), 'host': 'h', 'socket': str(tmp_path/'unused.sock')})
    root = tmp_path/'attempt'; root.mkdir()
    from poolrun.store import atomic
    atomic(root/'attempt.json', {'generation': 1})
    atomic(root/'process.json', {'pid': 12, 'created': 1, 'boot': 1})
    atomic(root/'runner.json', {'pid': 13, 'created': 2, 'boot': 1})
    agent.store.transition(None, lambda s: s['attempts'].update({'a': {'directory': str(root)}}))
    try:
        monkeypatch.setattr('poolrun.agent.alive', lambda p: p['pid'] == 12)
        assert not agent.report()['live_processes']
        monkeypatch.setattr('poolrun.agent.alive', lambda p: True)
        assert agent.report()['live_processes']['a']['generation'] == 1
        atomic(root/'exit.json', {'kind': 'RESULT_PENDING'})
        assert not agent.report()['live_processes']
    finally:
        agent.store.close()
