import subprocess
import sys
import time
from poolrun.store import atomic,read


def attempt(root,code,memory=128*2**20,deadline=None,validator=True):
    for name in ("control","outputs","checkpoints","release"):(root/name).mkdir(parents=True,exist_ok=True)
    (root/"release/job.py").write_text(code)
    (root/"release/validator.py").write_text("import json;print(json.dumps({'valid':"+str(validator)+",'files':['result']}))")
    data={"id":"test","spec":{},"spec_hash":"hash","resources":{"memory_bytes":memory},"hard_deadline":deadline,
        "release":{"argv":[sys.executable,str(root/"release/job.py")],"validator_argv":[sys.executable,str(root/"release/validator.py")]},
        "paths":{"release":str(root/"release"),"outputs":str(root/"outputs")}}
    atomic(root/"attempt.json",data)
    p=subprocess.Popen([sys.executable,"-m","poolrun.runner",str(root)])
    p.wait(timeout=15)
    return read(root/"exit.json")


def test_zero_exit_validator_failure_is_not_success(tmp_path):
    r=attempt(tmp_path,"pass",validator=False)
    assert r["kind"]=="FAILED" and "validator" in r["failure"]["reason"]


def test_soft_memory_watchdog_evidence(tmp_path):
    root=tmp_path/'agent/attempts/task/attempt'
    atomic(tmp_path/'agent/runtime-memory-policy.json',{'defaults':{'soft_memory_watchdog':True}})
    r=attempt(root,"import time;data=bytearray(10000000);time.sleep(5)",memory=1024)
    assert r["kind"]=="FAILED" and r["failure"]["oom_evidence"]=="soft_watchdog"


def test_owner_memory_policy_is_project_scoped_and_dynamic(tmp_path):
    from poolrun.runner import soft_memory_limit
    root=tmp_path/'agent'/'attempts'/'task'/'attempt'
    root.mkdir(parents=True)
    a={'spec':{'project':'p'},'resources':{'memory_bytes':1024}}
    assert soft_memory_limit(root,a) is None
    atomic(tmp_path/'agent/runtime-memory-policy.json',{'projects':{'p':{'soft_memory_watchdog':False}}})
    assert soft_memory_limit(root,a) is None
    a['spec']['project']='other'
    assert soft_memory_limit(root,a) is None
    atomic(tmp_path/'agent/runtime-memory-policy.json',{'defaults':{'soft_memory_watchdog':True},'projects':{'p':{'soft_memory_watchdog':False}}})
    assert soft_memory_limit(root,a)==1024
    a['spec']['project']='p'
    assert soft_memory_limit(root,a) is None


def test_default_memory_estimate_does_not_kill_new_project(tmp_path):
    root=tmp_path/'agent/attempts/new-task/new-attempt'
    r=attempt(root,"import time,pathlib;data=bytearray(10000000);time.sleep(.3);pathlib.Path('../outputs/result').write_text('ok')",memory=1024)
    assert r['kind']=='RESULT_PENDING'


def test_actual_sigkill_no_oom_evidence(tmp_path):
    r=attempt(tmp_path,"import os,signal;os.kill(os.getpid(),signal.SIGKILL)")
    assert r["kind"]=="FAILED" and r["failure"]["oom_evidence"] is None


def test_offline_hard_deadline_and_child_tree(tmp_path):
    r=attempt(tmp_path,"import subprocess,sys,time;subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)']);time.sleep(30)",deadline=time.time()+.4)
    assert r["kind"]=="STOPPED" and r["tree_exited"] is True


def test_missing_checkpoint_is_failure_not_success(tmp_path):
    for name in ("control","outputs","checkpoints"):(tmp_path/name).mkdir()
    atomic(tmp_path/"control/pause.json",{"request_id":"pause","attempt_id":"test"})
    r=attempt(tmp_path,"pass")
    assert r["kind"]=="FAILED"
