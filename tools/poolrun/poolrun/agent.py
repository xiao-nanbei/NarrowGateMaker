"""Outbound-only Agent. Transfer adapters are explicit policy, not network guesswork."""
import asyncio
import os
from pathlib import Path
import platform
import shutil
import tempfile
import base64
import subprocess
import sys
import time
import uuid
import psutil
from .data import Cache
from .release import expand, prepare, sha, relative
from .runner import adapter, alive, identity
from .store import Store, atomic, digest, read
from .isolation import available
from .environment import verify as verify_environment
from .ssh_transport import Client, ConnectionLost, RemoteError


class Agent:
    def __init__(self, config):
        if 'socket' in config and 'ssh' in config:
            raise ValueError('select one control transport')
        self.session = Client(config)
        self.config = config; self.root = Path(config["root"]).resolve()
        self.store = Store(self.root, {"attempts": {}, "releases": {}, "events": {}, "environments": {}, "errors": {}}, name="agent-state")
        self.cache = Cache(self.root)
        self.preparing = None
        self.tasks = set()
        self.uploads = set()
        self.backoff = dict(self.store.state.get("staging_retry",{}))
        self.heavy_lock=asyncio.Lock()
        self.preparation_reservation={}
        self.progress_samples={}
        from .cache_inventory import Inventory
        self.cache_inventory = Inventory(config.get('prepared_cache_roots', {}))
        self.platform = platform.system().lower() + "-" + platform.machine().lower()
        for eid, definition in config.get("environments", {}).items():
            old = self.store.state["environments"].get(eid)
            if old and old != definition: raise ValueError("environment ID rebinding forbidden: " + eid)
            verify_environment(definition)
            self.store.transition(None, lambda s, eid=eid, d=definition: s["environments"].update({eid:d}))

    async def request(self, op, body):
        return await self.session.request(op, body)

    def report(self):
        stat = os.statvfs(self.root)
        inventory, inventory_age = self.cache_inventory.report()
        progress={}
        live_processes = {}
        for aid,entry in self.store.state["attempts"].items():
            directory = Path(entry["directory"])
            if not (directory / "exit.json").exists():
                try:
                    process = read(directory / "process.json")
                    runner = read(directory / "runner.json")
                    attempt = read(directory / "attempt.json")
                    if alive(process) and alive(runner):
                        live_processes[aid] = {"process": process, "generation": attempt["generation"]}
                except (OSError, ValueError, KeyError):
                    pass
            file=Path(entry["directory"])/"progress.json"
            if file.exists() and file.stat().st_size<1024*1024:
                try:
                    value=read(file);signature=digest(value);old=self.progress_samples.get(aid)
                    progress[aid]={"value":value,"mtime":file.stat().st_mtime,"changed_since_previous_poll":old is not None and old!=signature}
                    self.progress_samples[aid]=signature
                except (OSError,ValueError):pass
            pause=Path(entry["directory"])/"pause-status.json"
            if pause.exists():progress.setdefault(aid,{})["pause"]=read(pause)
        return {"platform": self.platform, "releases": list(self.store.state["releases"]), "objects": self.cache.ready(),
            "environment_bindings":{eid:{"python":d["python"],"identity":digest(d)} for eid,d in self.store.state["environments"].items()},
            "attempts": list(self.store.state["attempts"]), "live_processes": live_processes,
            "memory_available": psutil.virtual_memory().available,
            "disk_free": shutil.disk_usage(self.root).free, "inodes_free": stat.f_favail,
            "prepared":list(self.store.state.get("prepared",{})),
            "cache_inventory":inventory, "cache_inventory_age_seconds":inventory_age,
            "progress":progress,"preparation_reservation":self.preparation_reservation,
            "hard_isolation": available(self.config.get("cgroup_root")), "preparing": self.preparing, "errors": self.store.state["errors"]}

    def enqueue(self, aid, event):
        event = dict(event, attempt_id=aid)
        rid = "event-" + aid + "-" + event["kind"]
        if rid in self.store.state.get("acked", []): return
        body = {"host": self.config["host"], "request_id": rid, "event": event}
        self.store.transition(None, lambda s: s["events"].setdefault(rid, body))

    async def flush(self):
        for rid, body in list(self.store.state["events"].items()):
            answer = await self.request("event", body)
            if not answer.get("accepted") and answer.get("reason") == "correctness hold": continue
            def ack(s, rid=rid):
                s["events"].pop(rid, None)
                s.setdefault("acked", []).append(rid)
            self.store.transition(None, ack)

    async def staging(self,job):
        self.preparing=job["task_id"]
        async with self.heavy_lock:await self._staging(job)

    async def _staging(self, job):
        self.preparing = job["task_id"]
        self.preparation_reservation=job.get("reservation",{})
        try:
            if psutil.virtual_memory().available<self.preparation_reservation.get("memory_bytes",0)+job.get("memory_headroom",0):raise ValueError("preparation memory guard")
            if shutil.disk_usage(self.root).free<self.preparation_reservation.get("disk_bytes",0)+self.config.get("disk_headroom",8*2**30):raise ValueError("preparation disk guard")
            rel = job["release"]; key = rel["project"] + "/" + rel["release_id"]
            if rel["env_id"] not in self.store.state["environments"]: raise ValueError("environment not approved")
            definition=self.store.state["environments"][rel["env_id"]]
            expected=rel.get("environment_manifests",{}).get(self.platform)
            if expected and definition.get("manifest_sha256")!=expected:raise ValueError("release dependency contract mismatches host environment")
            if self.platform not in rel["platforms"]: raise ValueError("unsupported release platform")
            if key not in self.store.state["releases"]:
                # A bounded package on a separate short SSH session cannot block heartbeats.
                async with Client(self.config) as packages:
                    response = await packages.request('packages', {'project': rel['project'], 'release': rel['release_id']})
                    data = base64.b64decode(response['package'], validate=True)
                location = await asyncio.to_thread(prepare, self.root, rel, data)
                self.store.transition(None, lambda s: s["releases"].update({key: location}))
            for obj in job["objects"]:
                if obj["id"] in self.cache.ready(): continue
                if shutil.disk_usage(self.root).free < obj["size"] + self.config.get("disk_headroom", 8*2**30): raise ValueError("disk staging guard")
                routes = [r for r in obj.get("sources", []) if r["route"] in self.config.get("transports", {})]
                if not routes: raise ValueError("WAIT_TRANSFER: no approved route")
                # Large noncloud traffic must use netdisk; cloud direct is explicit only.
                source = routes[0]
                if (source.get("zone") != "cloud" or self.config.get("network_zone") != "cloud") and obj["size"] > self.config.get("small_file_bytes", 1024*1024) and source["route"] != "netdisk":
                    raise ValueError("large noncloud transfer requires netdisk")
                part = self.root / "incoming" / (obj["id"] + ".part")
                part.parent.mkdir(parents=True, exist_ok=True)
                state=self.store.state.get("transfers",{}).get(obj["id"])
                if state and state.get("finish"):
                    await self.request("transfer",state["finish"])
                    self.store.transition(None,lambda s, oid=obj["id"]:s["transfers"].pop(oid,None));state=None
                if not state:
                    state={"request":{"request_id":"acquire-"+uuid.uuid4().hex,
                        "host":self.config["host"],"action":"acquire","object_id":obj["id"],"source":source}}
                    self.store.transition(None,lambda s, oid=obj["id"], state=state:s.setdefault("transfers",{}).update({oid:state}))
                ticket=await self.request("transfer",state["request"])
                started=time.time();success=False
                try:
                    receipt = await asyncio.to_thread(adapter, self.config["transports"][source["route"]],
                        {"action": "fetch", "object": obj, "source": source, "destination": str(part), "host": self.config["host"],
                         "rate_bytes_per_second":ticket["rate_bytes_per_second"],"transfer_id":ticket["id"]}, self.root, self.config.get("transfer_timeout", 3600))
                    if receipt.get("status") == "NEED_AUTH": raise ValueError("NEED_AUTH")
                    if receipt.get("status") != "READY": raise ValueError("transfer did not report READY")
                    self.cache.publish(obj["id"], part, obj["size"]);success=True
                finally:
                    finish={"request_id":"finish-"+ticket["id"],"host":self.config["host"],"action":"finish",
                        "transfer_id":ticket["id"],"success":success,"elapsed":time.time()-started}
                    self.store.transition(None,lambda s, oid=obj["id"], finish=finish:s["transfers"][oid].update(finish=finish))
                    await self.request("transfer",finish)
                    self.store.transition(None,lambda s, oid=obj["id"]:s["transfers"].pop(oid,None))
            recipe=job.get("prepared")
            if recipe and digest(recipe) not in self.store.state.get("prepared",{}):
                if not rel.get("prepare_argv"):raise ValueError("release has no prepared-cache builder")
                if recipe.get("platform")!=self.platform or recipe.get("contract")!=rel.get("prepared_contract"):raise ValueError("prepared cache ABI/recipe contract mismatch")
                parent=self.cache.root/"prepared";parent.mkdir(parents=True,exist_ok=True)
                tmp=Path(tempfile.mkdtemp(prefix=".build-",dir=parent))
                try:
                    request={"recipe":recipe,"inputs":str(self.cache.root/"blobs"),"destination":str(tmp)}
                    env=self.store.state["environments"][rel["env_id"]]
                    paths={"python":env["python"],"release":self.store.state["releases"][key]}
                    result=await asyncio.to_thread(adapter,expand(rel["prepare_argv"],paths),request,self.store.state["releases"][key],3600)
                    if result.get("status")!="READY":raise ValueError("prepared builder not READY")
                    for entry in result["files"]:
                        file=tmp/relative(entry["file"])
                        if file.is_symlink() or tmp.resolve() not in file.resolve().parents or sha(file)!=entry["sha256"]:raise ValueError("invalid prepared artifact")
                    atomic(tmp/"receipt.json",{"recipe":recipe,"files":result["files"]})
                    dest=parent/digest(recipe)
                    if dest.exists():shutil.rmtree(tmp)
                    else:os.rename(tmp,dest)
                    self.store.transition(None,lambda s:s.setdefault("prepared",{}).update({digest(recipe):str(dest)}))
                finally:
                    if tmp.exists():shutil.rmtree(tmp)
            self.store.transition(None, lambda s: s["errors"].pop(job["task_id"], None))
        except Exception as exc:
            error = str(exc)
            self.store.transition(None, lambda s: s["errors"].update({job["task_id"]: error}))
            previous=self.backoff.get(job["task_id"],(0,0))[0]+1
            self.backoff[job["task_id"]]=(previous,time.time()+min(300,2**previous))
            self.store.transition(None,lambda s:s.setdefault("staging_retry",{}).update({job["task_id"]:self.backoff[job["task_id"]]}))
        finally:
            self.preparing = None
            self.preparation_reservation={}

    def launch(self, a):
        aid = a["id"]
        if aid in self.store.state["attempts"]: return
        key = a["release"]["project"] + "/" + a["release"]["release_id"]
        root = self.root / "attempts" / a["task_id"] / aid
        for name in ("control", "outputs", "checkpoints"): (root/name).mkdir(parents=True, exist_ok=True)
        env = self.store.state["environments"][a["release"]["env_id"]]
        if a.get("environment_binding")!={"python":env["python"],"identity":digest(env)}:raise ValueError("START environment mapping differs from approved host binding")
        a = dict(a, platform=self.platform, paths={"python": env["python"], "release": self.store.state["releases"][key],
            "attempt": str(root), "business": str(root/"business.json"), "outputs": str(root/"outputs"),
            "checkpoints": str(root/"checkpoints"), "control": str(root/"control"), "cache": str(self.cache.root/"blobs")},
            hard_deadline=self.config.get("hard_deadline"),checkpoint_at=self.config.get("checkpoint_at"))
        if available(self.config.get("cgroup_root")):a["cgroup_root"]=self.config["cgroup_root"]
        a["paths"]["attempt_dir"]=str(root)
        if a["spec"].get("prepared"):a["paths"]["prepared"]=self.store.state["prepared"][digest(a["spec"]["prepared"])]
        for ref in a["spec"].get("business_spec",{}).get("input_refs",[]):
            a["paths"]["input:"+ref["name"]]=str(self.cache.path(ref["object_id"]))
        atomic(root / "business.json", a["spec"])
        atomic(root / "attempt.json", a)
        # Persist before spawning; restart never blindly launches this intent again.
        self.store.transition(None, lambda s: s["attempts"].update({aid: {"directory": str(root), "launch_intent": True}}))
        child=subprocess.Popen([sys.executable, "-m", "poolrun.runner", str(root)], start_new_session=True,
                         stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, close_fds=True)
        atomic(root/"runner.json",identity(child.pid))

    def controls(self, command):
        entry = self.store.state["attempts"].get(command["attempt_id"])
        if not entry: return
        root = Path(entry["directory"])
        if command["mode"] == "checkpoint":
            atomic(root/"control"/"pause.json", dict(command, action="checkpoint_and_exit"))
        else: atomic(root/"control"/"terminate.json", command)

    async def collect(self,command):
        if self.preparing or self.uploads:return
        rid=command["request_id"]
        old=self.store.state.get("gc_receipts",{}).get(rid)
        if old is None:
            keys=set(self.store.state["releases"])-set(command["protected_releases"])
            objects=set(command["reconstructible"])-set(command["protected_objects"])
            receipt={"releases":sorted(keys),"objects":sorted(objects),"applied":command["apply"],"environments":"external environments are never deleted"}
            if command["apply"]:
                self.cache.gc(command["protected_objects"],command["reconstructible"])
                for key in keys:
                    path=Path(self.store.state["releases"][key])
                    if path.is_symlink() or (self.root/"releases").resolve() not in path.resolve().parents:raise ValueError("GC path escaped release root")
                    if path.exists():
                        path.chmod(0o700)
                        for child in path.rglob("*"):
                            if child.is_dir() and not child.is_symlink():child.chmod(0o700)
                        shutil.rmtree(path)
                self.store.transition(None,lambda s:[s["releases"].pop(key,None) for key in keys])
            self.store.transition(None,lambda s:s.setdefault("gc_receipts",{}).update({rid:receipt}))
            old=receipt
        await self.request("gc-ack",{"request_id":"gc-ack-"+rid,"host":self.config["host"],"command_id":rid,"receipt":old})

    async def upload(self,aid,root,receipt):
        self.uploads.add(aid)
        async with self.heavy_lock:await self._upload(aid,root,receipt)

    async def _upload(self, aid, root, receipt):
        self.uploads.add(aid)
        ticket=None;success=False;started=time.time()
        try:
            previous_finish=root/"transfer-finish.json"
            if previous_finish.exists():
                await self.request("transfer",read(previous_finish));previous_finish.unlink(missing_ok=True)
            if not self.config.get("result_adapter"): raise ValueError("no durable result adapter configured")
            round_number=self.store.state.get("upload_retry",{}).get(aid,[0,0])[0]
            pending=self.store.state.get('result_acquires',{}).get(aid)
            if not pending:
                pending={"request_id":"result-acquire-"+aid+"-"+str(round_number),"host":self.config["host"],
                    "action":"acquire_result","attempt_id":aid,"size":sum(f["size"] for f in receipt["result"]["files"]),"route":self.config.get("result_route","netdisk")}
                self.store.transition(None,lambda s:s.setdefault('result_acquires',{}).update({aid:pending}))
            if self.config.get("result_storage") != "host_disk":
                ticket=await self.request("transfer",pending)
            self.store.transition(None,lambda s:s['result_acquires'].pop(aid,None))
            saved = await asyncio.to_thread(adapter, self.config["result_adapter"],
                {"action": "save_result", "attempt_id": aid, "directory": str(root/"outputs"), "result": receipt["result"],
                 "rate_bytes_per_second":ticket["rate_bytes_per_second"] if ticket else 0}, self.root, 3600)
            if saved.get("durable") is not True: raise ValueError("result is not durably saved")
            atomic(root/"durable.json", saved)
            self.enqueue(aid, dict(receipt, kind="COMPLETE", durable_receipt=saved))
            success=True
        except Exception as exc:
            error = str(exc)
            self.store.transition(None, lambda s: s["errors"].update({aid: "RESULT_PENDING: " + error}))
            count=self.store.state.get("upload_retry",{}).get(aid,[0,0])[0]+1
            self.store.transition(None,lambda s:s.setdefault("upload_retry",{}).update({aid:[count,time.time()+min(300,2**count)]}))
        finally:
            if ticket:
                finish={"request_id":"result-finish-"+ticket["id"],"host":self.config["host"],"action":"finish","transfer_id":ticket["id"],"success":success,"elapsed":time.time()-started}
                atomic(root/"transfer-finish.json",finish)
                try:await self.request("transfer",finish)
                except (ValueError,ConnectionLost,asyncio.TimeoutError):pass
            self.uploads.discard(aid)

    def supervise(self):
        for aid, entry in self.store.state["attempts"].items():
            root = Path(entry["directory"])
            if (root/"exit.json").exists():
                receipt = read(root/"exit.json")
                self.enqueue(aid, receipt)
                if receipt["kind"] == "RESULT_PENDING":
                    if (root/"durable.json").exists(): self.enqueue(aid, dict(receipt, kind="COMPLETE", durable_receipt=read(root/"durable.json")))
                    elif aid not in self.uploads and self.store.state.get("upload_retry",{}).get(aid,[0,0])[0]<5 and self.store.state.get("upload_retry",{}).get(aid,[0,0])[1]<time.time():
                        task = asyncio.create_task(self.upload(aid, root, receipt)); self.tasks.add(task); task.add_done_callback(self.tasks.discard)
            elif (root/"process.json").exists():
                if (root/"runner.json").exists() and not alive(read(root/"runner.json")):
                    self.enqueue(aid,{"kind":"UNKNOWN","failure":{"reason":"runner exited without tree-exit receipt"}})
                else:self.enqueue(aid, {"kind": "RUNNING", "process": read(root/"process.json")})
            elif time.time() - (root/"attempt.json").stat().st_mtime > 10:
                self.enqueue(aid, {"kind": "UNKNOWN", "failure": {"reason": "launch intent without acknowledged child"}})

    async def run(self):
        async with self.session:
            try:
                while True:
                    try:
                        self.supervise(); await self.flush()
                        for entry in self.store.state["attempts"].values():
                            finish=Path(entry["directory"])/"transfer-finish.json"
                            if finish.exists():
                                await self.request("transfer",read(finish));finish.unlink(missing_ok=True)
                        reply = await self.request("poll", {"host": self.config["host"], "report": self.report()})
                        if reply.get("gc"):await self.collect(reply["gc"])
                        for cmd in reply.get("commands", []):
                            if cmd["kind"] == "START": self.launch(cmd["attempt"])
                            elif cmd["kind"] == "STOP": self.controls(cmd)
                        # Persist before sending, retain through disconnection/restart.
                        # Reconcile above before retrying the same uncertain START.
                        if not self.store.state.get('pending_start') and reply.get("offer") and time.time() < self.config.get("stop_accepting_at", float("inf")):
                            pending = {"host": self.config["host"], "task_id": reply["offer"], "request_id": "start-"+uuid.uuid4().hex}
                            self.store.transition(None, lambda s, pending=pending: s.update(pending_start=pending))
                        if self.store.state.get('pending_start'):
                            try:
                                a = await self.request('start', self.store.state['pending_start'])
                            except RemoteError as exc:
                                if exc.code == 'INVALID_REQUEST':
                                    self.store.transition(None, lambda s: s.pop('pending_start', None))
                                raise
                            self.launch(a)
                            self.store.transition(None, lambda s: s.pop('pending_start', None))
                        if reply.get("prepare") and not self.preparing and self.backoff.get(reply["prepare"]["task_id"],(0,0))[1]<time.time() and self.backoff.get(reply["prepare"]["task_id"],(0,0))[0]<5:
                            task = asyncio.create_task(self.staging(reply["prepare"])); self.tasks.add(task); task.add_done_callback(self.tasks.discard)
                    except (ConnectionLost, asyncio.TimeoutError, ValueError) as e:
                        print("Agent control unavailable/rejected:", str(e), file=sys.stderr, flush=True)
                    await asyncio.sleep(self.config.get("poll_seconds", 2))
            finally:
                for task in self.tasks: task.cancel()
                await asyncio.gather(*self.tasks, return_exceptions=True)
                self.store.close()
