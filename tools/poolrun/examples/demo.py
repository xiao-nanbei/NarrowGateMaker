"""Generate private localhost configs, or run three independent Agents. No external hosts."""
import argparse
import asyncio
import base64
import json
from pathlib import Path
import platform
import subprocess
import sys
import time
import uuid
import psutil
from poolrun.release import create,sha
from poolrun.store import atomic,read
from poolrun.ssh_transport import Client, ConnectionLost
from poolrun.config import load

HERE=Path(__file__).resolve().parent


async def demo(root,init_only,connections=None):
    root=root.resolve()
    if root.exists():raise ValueError("use a new demo directory; existing evidence will not be overwritten")
    root.mkdir(parents=True)
    plat=platform.system().lower()+"-"+platform.machine().lower()
    hosts={h:{"platform":plat,"slots":1,"cpus":1,
        "memory_bytes":512*2**20,"disk_bytes":2**30,"disk_headroom":0} for h in ("host-a","host-b","host-c")}
    atomic(root/"control.json",{"hosts":hosts,
        "transfer_policy":{"global_concurrency":3,"routes":{"netdisk":{"account_concurrency":3}}}})
    template={'ssh':{'target':'poolrun-demo','identity_file':'/approved/demo/key','known_hosts_file':'/approved/demo/known_hosts'}}
    atomic(root/"client.json",load(connections/'admin.json') if connections else template)
    contract={"env_id":"stdlib-demo","platforms":[plat],"argv":["${python}","${release}/job.py","--attempt","${attempt}"],
        "validator_argv":["${python}","${release}/validator.py"],"checkpoint_adapter_argv":["${python}","${release}/checkpoint_adapter.py"],
        "checkpoint_contract":"synthetic-v1","output_contract":"synthetic-v1"}
    atomic(root/"release-contract.json",contract)
    packages=[create("calc-demo",HERE/"synthetic",HERE/"synthetic/include.txt",version,contract,root/"packages") for version in ("r1","r2")]
    for h,policy in hosts.items():
        atomic(root/(h+".json"),{**(load(connections/(h+'.json')) if connections else template),"host":h,"root":str(root/h),"poll_seconds":.2,
            "environments":{"stdlib-demo":{"python":sys.executable,"python_sha256":sha(Path(sys.executable).resolve())}},
            "result_adapter":[sys.executable,str(HERE/"local_adapter.py"),str(root/"durable-results")]})
    tasks=[{"id":f"demo-{i:03}","project":"calc-demo","code_policy":{"release":"r1"},"side_effects":False,
        "parameters":{"steps":100 if i==0 else 40,"delay":.03,"seed":17},
        "resources":{"cpus":1,"memory_bytes":128*2**20,"disk_bytes":2**20}} for i in range(12)]
    (root/"tasks.jsonl").write_text("\n".join(json.dumps(t) for t in tasks)+"\n")
    if init_only:print("Generated:",root);return
    if not connections:raise ValueError('provide isolated approved SSH configs via --connections; no system sshd changes are made')
    processes=[];logs=[]
    def spawn(args,name):
        out=(root/(name+".log")).open("ab");logs.append(out)
        p=subprocess.Popen([sys.executable,"-m","poolrun",*args],stdout=out,stderr=out);processes.append(p);return p
    spawn(["master","--root",str(root/"control"),"--config",str(root/"control.json")],"master")
    async with Client(load(root/'client.json')) as client:
        async def status():
            return await client.request('status')
        async def send(op,**kwargs):
            return await client.request('admin',{'op':op,'request_id':uuid.uuid4().hex,**kwargs})
        try:
            for _ in range(100):
                try:await status();break
                except ConnectionLost:await asyncio.sleep(.1)
            for f in packages:await send("release",release=read(f),package=base64.b64encode((f.parent/"code.zip").read_bytes()).decode())
            await send("submit",tasks=tasks)
            for h in hosts:spawn(["agent","--config",str(root/(h+".json"))],h)
            upgraded=False;active_rss={};end=time.time()+120
            while time.time()<end:
                s=await status();counts={}
                for t in s["tasks"].values():counts[t["status"]]=counts.get(t["status"],0)+1
                if not upgraded and s["tasks"]["demo-000"]["status"]=="RUNNING":
                    active_rss={"master" if i==0 else "agent-"+str(i):psutil.Process(p.pid).memory_info().rss for i,p in enumerate(processes)}
                    await send("rollout",project="calc-demo",release="r2",scope="pending")
                    await send("upgrade",task="demo-000",release="r2",mode="checkpoint")
                    upgraded=True
                if counts.get("COMPLETE")==12:break
                await asyncio.sleep(.2)
            else:raise TimeoutError("demo did not finish; inspect preserved logs/state")
            before=s["snapshot_writes"];await asyncio.sleep(1);s=await status()
            receipt={"complete":12,"attempts":len(s["attempts"]),"cross_version_checkpoint":upgraded,"active_rss_bytes":active_rss,
                "idle_rss_bytes":{"master" if i==0 else "agent-"+str(i):psutil.Process(p.pid).memory_info().rss for i,p in enumerate(processes)},
                "idle_snapshot_writes_per_second":s["snapshot_writes"]-before,
                "note":"localhost simulation only; local result-copy adapter, not cloud/netdisk validation"}
            atomic(root/"demo-receipt.json",receipt)
            print(json.dumps(receipt,indent=2));print("Evidence:",root)
        finally:
            # Stop only this demo's service processes. Business evidence is preserved.
            for p in processes:
                if p.poll() is None:p.terminate()
            for p in processes:
                try:p.wait(timeout=5)
                except subprocess.TimeoutExpired:p.kill();p.wait()
            for log in logs:log.close()


if __name__=="__main__":
    p=argparse.ArgumentParser();p.add_argument("--root",type=Path,default=Path("demo-run"));p.add_argument("--init-only",action="store_true")
    p.add_argument('--connections',type=Path,help='isolated SSH configs admin.json and host-a/b/c.json; forced commands must bind this demo master socket')
    a=p.parse_args();asyncio.run(demo(a.root,a.init_only,a.connections))
