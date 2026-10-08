"""Real isolated OpenSSH master + three Agents + independent business processes."""
import asyncio
import base64
import json
from pathlib import Path
import platform
import subprocess
import sys
import time
import tempfile
import uuid
import pytest
import psutil
from poolrun.release import create, sha
from poolrun.store import atomic, read
from poolrun.ssh_transport import Client, ConnectionLost
from ssh_lab import SSHLab, Unavailable

REPO=Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("mode",["checkpoint","restart"])
def test_three_agents_checkpoint_and_master_restart(tmp_path,mode):
    asyncio.run(scenario(tmp_path,mode))


async def scenario(root,mode="checkpoint"):
    socket_dir = tempfile.TemporaryDirectory(prefix='pr-', dir='/tmp')
    control_socket = Path(socket_dir.name)/'master.sock'
    try: lab = SSHLab(root/'ssh', control_socket)
    except Unavailable as exc: pytest.skip(str(exc))
    plat=platform.system().lower()+"-"+platform.machine().lower()
    contract={"env_id":"py1","platforms":[plat],"argv":["${python}","${release}/job.py","--attempt","${attempt}"],
        "validator_argv":["${python}","${release}/validator.py"],"checkpoint_adapter_argv":["${python}","${release}/checkpoint_adapter.py"],
        "checkpoint_contract":"synthetic-v1","output_contract":"synthetic-v1"}
    packages=[]
    for version in ("r1","r2"):
        packages.append(create("demo",REPO/"examples/synthetic",REPO/"examples/synthetic/include.txt",version,contract,root/"packages"))
    hosts={h:{"slots":1,"memory_bytes":512*2**20,"disk_bytes":2**30,"cpus":1,"platform":plat,"disk_headroom":0} for h in ("a","b","c")}
    config={"hosts":hosts,"socket":str(control_socket),
        "transfer_policy":{"global_concurrency":2,"routes":{"netdisk":{"account_concurrency":2}}}}
    atomic(root/"master.json",config)
    processes=[];logs=[]
    def spawn(argv,name):
        log=(root/(name+".log")).open("ab");logs.append(log)
        p=subprocess.Popen([sys.executable,"-m","poolrun",*argv],cwd=REPO,stdout=log,stderr=log);processes.append(p);return p
    master=spawn(["master","--root",str(root/"control"),"--config",str(root/"master.json")],"master")
    async with Client(lab.config('admin')) as client:
        async def status():
            return await client.request('status')
        async def admin(op,**kwargs):
            return await client.request('admin', {'op':op,'request_id':uuid.uuid4().hex,**kwargs})
        async def wait(predicate,seconds=40):
            end=time.time()+seconds
            while time.time()<end:
                try:
                    data=await status()
                    if predicate(data):return data
                except ConnectionLost:pass
                await asyncio.sleep(.15)
            raise AssertionError("timeout: "+json.dumps(await status())[:4000])
        try:
            # Master/socket startup failure is not an SSH capability skip.
            for _ in range(100):
                if control_socket.exists(): break
                await asyncio.sleep(.05)
            assert control_socket.exists(), (root/'master.log').read_text()
            await status()  # Once sshd starts, auth/protocol defects must fail, not skip.
            for f in packages: await admin("release",release=read(f),package=base64.b64encode((f.parent/"code.zip").read_bytes()).decode())
            blob=root/"data.bin";blob.write_bytes(b"synthetic verified data"*200)
            oid=sha(blob)
            await admin("object",object={"id":oid,"sha256":oid,"size":blob.stat().st_size,"sources":[{"route":"netdisk","path":str(blob),"zone":"noncloud"}]})
            tasks=[{"id":f"t{i}","project":"demo","code_policy":{"release":"r1"},"parameters":{"steps":200 if i==0 else 20,"delay":.02,"seed":1701},
                "side_effects":False,"inputs":[oid],"resources":{"memory_bytes":128*2**20,"disk_bytes":2**20,"cpus":1}} for i in range(7)]
            await admin("submit",tasks=tasks)
            agents={}
            for host in hosts:
                conf={**lab.config('agent:'+host),"host":host,"root":str(root/host),"disk_headroom":0,"poll_seconds":.1,
                    "environments":{"py1":{"python":sys.executable,"python_sha256":sha(Path(sys.executable).resolve())}},
                    "transports":{"netdisk":[sys.executable,str(REPO/"examples/local_adapter.py")]},
                    "result_adapter":[sys.executable,str(REPO/"examples/local_adapter.py"),str(root/"durable")]}
                atomic(root/(host+".json"),conf)
                agents[host]=spawn(["agent","--config",str(root/(host+".json"))],host)
            data=await wait(lambda s:s["tasks"]["t0"]["status"]=="RUNNING")
            aid=data["tasks"]["t0"]["attempt_id"]
            owner=data['attempts'][aid]['host']
            # Kill only that Agent's control ssh, not Agent/runner/business process.
            ssh_children=[p for p in psutil.Process(agents[owner].pid).children() if p.name()=='ssh']
            assert ssh_children
            attempt_dir=root/owner/'attempts/t0'/aid
            business=read(attempt_dir/'process.json')
            for child in ssh_children: child.kill()
            await asyncio.sleep(.4)
            assert psutil.pid_exists(business['pid'])
            data=await status()
            assert data['tasks']['t0']['attempt_id']==aid
            active_rss={str(p.pid):psutil.Process(p.pid).memory_info().rss for p in processes if p.poll() is None}
            tree=[psutil.Process(p.pid) for p in [*processes,lab.process] if p.poll() is None]
            tree.append(psutil.Process(client.process.pid))
            tree={p.pid:p for parent in tree for p in [parent,*parent.children(recursive=True)]}
            transport_resources=[]
            for p in tree.values():
                try:
                    argv=p.cmdline()
                    role='gateway' if 'ssh-gateway' in argv else 'sshd' if 'sshd' in p.name() else 'ssh' if p.name()=='ssh' else 'master' if 'master' in argv else 'agent' if 'agent' in argv else 'runner/business'
                    transport_resources.append({'role':role,'name':p.name(),'rss_bytes':p.memory_info().rss,'cpu_seconds':sum(p.cpu_times()[:2])})
                except (psutil.NoSuchProcess,psutil.AccessDenied):pass
            await admin("rollout",project="demo",release="r2",scope="pending")
            await admin("upgrade",task="t0",release="r2",mode=mode)
            data=await wait(lambda s:s["tasks"]["t0"]["status"]=="RUNNING" and s["tasks"]["t0"]["attempt_id"]!=aid)
            new=data["tasks"]["t0"]["attempt_id"]
            assert new != aid and data["attempts"][new]["release"]["release_id"]=="r2"
            owner=data["attempts"][new]["host"]
            agents[owner].terminate();agents[owner].wait(timeout=10)
            agents[owner]=spawn(["agent","--config",str(root/(owner+".json"))],owner)
            master.terminate();master.wait(timeout=10)
            master=spawn(["master","--root",str(root/"control"),"--config",str(root/"master.json")],"master")
            data=await wait(lambda s:all(t["status"]=="COMPLETE" for t in s["tasks"].values()),60)
            assert {a["host"] for a in data["attempts"].values()}=={"a","b","c"}
            assert data["attempts"][aid]["status"]==("PAUSED" if mode=="checkpoint" else "STOPPED")
            assert len(list((root/"durable").rglob("result.json")))==7
            assert len([x for x in data["transfers"].values() if x["object_id"]==oid])==3 # one shared content transfer per Agent, not per task
            for h in hosts:assert (root/h/"cache/blobs"/oid).exists()
            before=data["snapshot_writes"];start=time.perf_counter()
            for _ in range(10):await status()
            response_ms=(time.perf_counter()-start)*100
            await asyncio.sleep(1)
            idle=await status()
            assert idle["snapshot_writes"]==before
            atomic(root/"acceptance.json",{"complete":7,"agents":3,"upgrade_mode":mode,"master_restart":True,"agent_restart":True,
                "data_transfers":3,"active_rss_bytes":active_rss,"transport_process_tree":transport_resources,"ssh_disconnect_runner_survived":True,"idle_rss_bytes":{str(p.pid):psutil.Process(p.pid).memory_info().rss for p in processes if p.poll() is None},
                "mean_status_ms":response_ms,"idle_snapshot_writes_per_second":idle["snapshot_writes"]-before,"snapshot_writes_after_master_restart":data["snapshot_writes"]})
        finally:
            for p in processes:
                if p.poll() is None:p.terminate()
            for p in processes:
                try:p.wait(timeout=10)
                except subprocess.TimeoutExpired:p.kill();p.wait()
            for log in logs:log.close()
            lab.close()
            socket_dir.cleanup()
