"""One master, JSON snapshots, authenticated outbound Agent polling."""
import asyncio
import copy
from pathlib import Path
import time
from . import model
from .release import identifier, verify_package
from .store import Store, atomic
from . import transfer
from .schedule import preparation_host
from .cache_inventory import locality_reason


class Master:
    def __init__(self, root, config):
        from .config import reject_legacy
        reject_legacy(config)
        self.root = Path(root).resolve()
        self.config = config
        self.store = Store(root, model.initial())
        self.reports = {}  # Deliberately not a 1-second snapshot writer.
        self.seen = {}
        self.started_at = time.time()
        def configure(s):
            for host, policy in config["hosts"].items():
                identifier(host)
                previous = s["hosts"].get(host, {})
                clean=dict(policy)
                if previous and previous["platform"]!=clean["platform"]:raise ValueError("host platform changed under stable host ID")
                if not previous:s["hosts"][host] = dict(clean, draining=False)
            return None
        self.store.transition(None, configure)

    def handle(self, principal, op, body, request_id):
        """Synchronous dispatch: only this resident Master owns the Store."""
        agent_ops = {'poll', 'start', 'event', 'transfer', 'gc-ack', 'packages'}
        admin_ops = {'status', 'admin', 'backup', 'snapshot', 'packages'}
        if not isinstance(principal, str): raise PermissionError('invalid principal')
        b = copy.deepcopy(body)
        if any(k in b for k in ('role', 'principal', 'remote_command', '_principal', '_operation')):
            raise PermissionError('client cannot choose identity')
        if principal.startswith('agent:'):
            host = principal.removeprefix('agent:')
            if host not in self.config['hosts'] or op not in agent_ops:
                raise PermissionError('agent operation forbidden')
            if b.get('host', host) != host: raise PermissionError('host impersonation')
            b['host'] = host
        elif principal != 'admin' or op not in admin_ops:
            raise PermissionError('administrator operation required')
        if 'request_id' in b and b['request_id'] != request_id:
            raise ValueError('request identity mismatch')
        b.update(request_id=request_id, _principal=principal, _operation=op)
        # Namespace saved identities by authenticated principal and operation.
        from .store import digest
        b['request_id'] = digest({'principal': principal, 'op': op, 'request_id': request_id})
        cached = self.store.state.get('requests', {}).get(b['request_id'])
        if cached:
            if cached['identity'] != digest(b): raise ValueError('request_id reused with different payload')
            return copy.deepcopy(cached['reply'])
        methods = {'transfer': self.transfer_api, 'gc-ack': self.gc_ack, 'packages': self.package}
        return methods.get(op, getattr(self, op, None))(b)

    def mutate(self, body, action):
        rid = body.get("request_id")
        if not isinstance(rid, str) or not 8 <= len(rid) <= 512:
            raise ValueError("stable request_id required")
        return self.store.transition(rid, action, body)

    def status(self, b):
        s = copy.deepcopy(self.store.state)
        s.pop("requests", None)
        s["live"] = self.reports
        now=time.time()
        s['cache_locality']={tid:{h:locality_reason(s,t,h,self.reports,self.seen,now,self.started_at)
                                 for h in s['hosts']}
                             for tid,t in s['tasks'].items() if t['status'] in model.PENDING and t['spec'].get('cache_affinity')}
        s["snapshot_writes"] = self.store.writes
        return s

    def poll(self, b):
        host = b["host"]
        now = time.time()
        report = b["report"]
        self.reports[host] = report
        self.seen[host] = now
        # Reconcile IDs, including uncertain launch windows, before giving new work.
        expected = [a for a in self.store.state["attempts"].values()
                    if a["host"] == host and a["status"] in model.ACTIVE | {"RESULT_PENDING"}]
        local_ids = set(report.get("attempts", []))
        unknown=local_ids-set(self.store.state["attempts"])
        if unknown:
            self.store.transition(None,lambda s:s.update(reconciliation_required={"host":host,"unknown_attempts":sorted(unknown)}))
            return {"blocked":"foreign attempts; restore/reconcile authoritative state before dispatch"}
        commands = []
        for a in expected:
            evidence = report.get("live_processes", {}).get(a["id"])
            if a["status"] == "UNKNOWN" and a["id"] in local_ids and evidence:
                # Fresh authenticated poll, backed by Agent PID/birth/boot checks.
                self.store.transition(None, lambda s, a=a, evidence=evidence:
                    model.reconcile_heartbeat(s, host, a["id"], evidence))
            if a["id"] not in local_ids:
                if a["status"] == "STARTING": commands.append({"kind": "START", "attempt": a})
                elif a["status"] != "UNKNOWN":
                    self.store.transition(None, lambda s, a=a: model.event(s, host, {
                        "attempt_id": a["id"], "kind": "UNKNOWN", "failure": {"reason": "Agent cannot reconcile attempt"}}))
            if a.get("command"): commands.append(a["command"])
        if commands: return {"commands": commands}
        s = self.store.state
        if host in s.get("gc_commands",{}):return {"gc":s["gc_commands"][host]}
        for key, targets in s.get("prepare_requests", {}).items():
            if host in targets and key not in report.get("releases", []):
                return {"prepare": {"task_id": "release:"+key, "release": s["releases"][key], "objects": []}}
        # Upgrade readiness is per host; never pause old work before target is staged.
        for a in expected:
            pending=a.get("upgrade")
            if not pending:continue
            key=model.release_key(a["spec"]["project"],pending["release"])
            if key not in report.get("releases",[]):
                return {"prepare":{"task_id":"upgrade:"+a["id"],"release":s["releases"][key],"objects":[]}}
            if a["status"] in {"STARTING","RUNNING"}:
                def pause(s,a=a,pending=pending):
                    row=s["attempts"][a["id"]]
                    row["command"]={"kind":"STOP","attempt_id":a["id"],"mode":pending["mode"],"request_id":pending["request_id"]}
                    row["status"]="PAUSING" if pending["mode"]=="checkpoint" else "STOPPING"
                    s["tasks"][a["task_id"]]["status"]=row["status"]
                self.store.transition(None,pause)
                return {"commands":[self.store.state["attempts"][a["id"]]["command"]]}
        locality = {tid: locality_reason(s,t,host,self.reports,self.seen,now,self.started_at)
                    for tid,t in s['tasks'].items() if t['status'] in model.PENDING}
        tid = model.choose(s, host, report, allowed={tid for tid in s['tasks'] if not locality.get(tid)})
        if tid:
            return {"offer": tid}  # Not START authorization.
        if s["hosts"][host].get("draining"): return {}
        # At most one staging target per task and one prefetch per host.
        for t in sorted(s["tasks"].values(), key=lambda x: x["submitted"]):
            if t["status"] not in model.PENDING: continue
            tid = t["spec"]["id"]
            if locality.get(tid): continue
            owner = s.get("preparations",{}).get(tid)
            if owner and owner["host"] != host: continue
            reason = model.eligible(s, t, host, report, now)
            if reason and (reason.startswith("missing data") or reason in {"release not ready","prepared cache not ready"}):
                if any(v["host"] == host and k != tid for k,v in s.get("preparations",{}).items()): continue
                key = model.release_key(t["spec"]["project"], t["code_policy"]["release"])
                if key not in s["releases"]: continue
                plan=preparation_host(s,t,{h:r for h,r in self.reports.items() if now-self.seen.get(h,0)<30})
                if plan and plan["host"]!=host:continue
                objects = [s["objects"][oid] for oid in t["spec"].get("inputs", []) if oid not in report.get("objects", [])]
                if sum(o["size"] for o in objects) > s["hosts"][host].get("prefetch_bytes", 8*2**30): continue
                if not owner:self.store.transition(None,lambda st, tid=tid:st.setdefault("preparations",{}).update({tid:{"host":host,"created":now}}))
                recipe=t["spec"].get("prepared") or {}
                reservation={"memory_bytes":recipe.get("memory_bytes",64*2**20),"disk_bytes":sum(o["size"] for o in objects)+recipe.get("disk_bytes",0),"cpus":recipe.get("cpus",0)}
                return {"prepare": {"task_id": tid, "release": s["releases"][key], "objects": objects,"prepared":t["spec"].get("prepared"),
                    "reservation":reservation,"memory_headroom":s["hosts"][host].get("memory_headroom",0)}}
        return {}

    def start(self, b):
        host = b["host"]
        if host not in self.reports or time.time()-self.seen[host] > 30: raise ValueError("fresh reconciliation required")
        reason=locality_reason(self.store.state,self.store.state['tasks'][b['task_id']],host,
                               self.reports,self.seen,time.time(),self.started_at)
        if reason: raise ValueError(reason)
        def begin(s):
            a=model.start(s,b["task_id"],host,self.reports[host])
            s.setdefault("preparations",{}).pop(b["task_id"],None)
            return a
        a = self.mutate(b, begin)
        return a

    def event(self, b):
        def change(s):
            answer=model.event(s,b["host"],b["event"])
            a=s["attempts"][b["event"]["attempt_id"]];up=a.get("upgrade")
            if answer.get("accepted") and up and ((up["mode"]=="checkpoint" and a["status"]=="PAUSED") or (up["mode"]=="terminate" and a["status"]=="STOPPED")):
                t=s["tasks"][a["task_id"]]
                if up["mode"]=="checkpoint":
                    t["resume"]={"checkpoint":a["checkpoint"],"source_attempt":a["id"],"source_release":a["release"],"host":a["host"]}
                    t["resume_host"]=a["host"]
                t.update(status="PENDING",attempt_id=None,code_policy={"release":up["release"],"missing":"wait"})
                a.pop("upgrade",None)
            return answer
        return self.mutate(b, change)

    def transfer_api(self,b):
        host=b["host"]
        def change(s):
            if b["action"]=="acquire_result":
                a=s["attempts"][b["attempt_id"]]
                if a["host"]!=host or a["status"]!="RESULT_PENDING":raise ValueError("result upload is not eligible")
                obj={"id":"result-"+a["id"],"size":b["size"]}
                source={"route":b["route"],"source_id":host,"zone":s["hosts"][host].get("network_zone","noncloud")}
                return transfer.admit(s,host,obj,source,self.config.get("transfer_policy",{}))
            if b["action"]=="acquire":
                obj=s["objects"][b["object_id"]]
                source=b["source"]
                if source not in obj["sources"]:raise ValueError("unregistered source")
                return transfer.admit(s,host,obj,source,self.config.get("transfer_policy",{}))
            return transfer.finish(s,host,b["transfer_id"],b["success"],b["elapsed"])
        return self.mutate(b,change)

    def gc_ack(self,b):
        def change(s):
            command=s.get("gc_commands",{}).get(b["host"])
            if command and command["request_id"]==b["command_id"]:s["gc_commands"].pop(b["host"])
            s.setdefault("gc_receipts",{})[b["host"]]=b["receipt"]
            return {"accepted":True}
        return self.mutate(b,change)

    def package(self, b):
        import base64
        from .ssh_transport import MAX_PACKAGE
        project, release = identifier(b['project']), identifier(b['release'])
        key = model.release_key(project, release)
        rel = self.store.state['releases'][key]
        path = self.root / 'packages' / project / release / 'code.zip'
        with path.open('rb') as stream:
            data = stream.read(MAX_PACKAGE + 1)
        if len(data) > MAX_PACKAGE: raise ValueError('package exceeds limit')
        verify_package(rel, data)
        return {'package': base64.b64encode(data).decode()}

    def admin(self, b):
        op = b["op"]
        def change(s):
            if op == "submit": return model.submit(s, b["tasks"])
            if op == "object":
                obj = b["object"]; oid = obj["id"]
                if oid != obj["sha256"] or len(oid) != 64 or any(c not in "0123456789abcdef" for c in oid) or obj["size"] < 0: raise ValueError("object identity invalid")
                if oid in s["objects"] and s["objects"][oid] != obj: raise ValueError("immutable object conflict")
                s["objects"][oid] = obj
                return {"registered": oid}
            if op == "release":
                import base64
                rel = b["release"]; data = base64.b64decode(b["package"], validate=True)
                verify_package(rel, data)
                for field in ("env_id", "platforms", "argv", "validator_argv", "checkpoint_contract", "output_contract"):
                    if not rel.get(field): raise ValueError("release requires " + field)
                key = model.release_key(rel["project"], rel["release_id"])
                if key in s["releases"] and s["releases"][key] != rel: raise ValueError("immutable release conflict")
                target = self.root / "packages" / key
                target.mkdir(parents=True, exist_ok=True)
                from .store import read
                if (target/"release.json").exists() and read(target/"release.json")!=rel:raise ValueError("on-disk immutable release conflict")
                from .data import publish_bytes
                publish_bytes(target / "code.zip", data)
                atomic(target / "release.json", rel)
                s["releases"][key] = rel
                return {"registered": key}
            if op == "rollout": return model.rollout(s, b["project"], b["release"], b.get("scope", "pending"))
            if op == "prepare":
                key=model.release_key(b["project"],b["release"])
                if key not in s["releases"]:raise ValueError("unknown release")
                if set(b["hosts"])-set(s["hosts"]):raise ValueError("unknown host")
                s.setdefault("prepare_requests",{})[key]=b["hosts"]
                return {"preparing":key,"hosts":b["hosts"]}
            if op == "upgrade":
                t=s["tasks"][b["task"]];a=s["attempts"][t["attempt_id"]]
                if a["status"] not in {"RUNNING","STARTING"}:raise ValueError("upgrade requires an active attempt; use resume for a paused attempt")
                key=model.release_key(t["spec"]["project"],b["release"])
                target=s["releases"][key]
                if b["mode"]=="checkpoint" and not target.get("checkpoint_adapter_argv"):raise ValueError("no target checkpoint adapter")
                a["upgrade"]={"release":b["release"],"mode":"checkpoint" if b["mode"]=="checkpoint" else "terminate","request_id":b["request_id"]}
                return {"status":"preparing target before stopping source"}
            if op == "host-update":
                h=s["hosts"][b["host"]]
                for k,v in b["resources"].items():
                    if k == "memory_admission":
                        if type(v) is not bool: raise ValueError("memory_admission must be boolean")
                        h[k] = v
                        continue
                    if k not in {"slots","memory_bytes","disk_bytes","cpus"} or v<0:raise ValueError("invalid host resource update")
                    h[k]=v
                return {"updated":b["host"],"active_attempts_unchanged":True}
            if op == "pending-placement":
                hosts = b["hosts"]
                if not isinstance(hosts, list) or not hosts or len(set(hosts)) != len(hosts):
                    raise ValueError("placement requires distinct explicit hosts")
                if set(hosts) - set(s["hosts"]): raise ValueError("unknown host")
                updated = []
                for tid, task in s["tasks"].items():
                    if task["spec"]["project"] == b["project"] and task["status"] in model.PENDING and task["attempt_id"] is None and task["generation"] == 0:
                        task["placement_hosts"] = list(hosts)
                        updated.append(tid)
                return {"updated": updated, "active_attempts_unchanged": True}
            if op == "gc":
                protected_releases=set();protected_objects=set()
                for t in s["tasks"].values():
                    if t["status"] in model.PENDING:
                        protected_releases.add(model.release_key(t["spec"]["project"],t["code_policy"]["release"]))
                        protected_objects.update(t["spec"].get("inputs",[]))
                for a in s["attempts"].values():
                    if a["host"]==b["host"] and a["status"] in model.ACTIVE|{"RESULT_PENDING","PAUSED"}:
                        protected_releases.add(model.release_key(a["spec"]["project"],a["release"]["release_id"]))
                        protected_objects.update(a["spec"].get("inputs",[]))
                for project,release in s.get("project_defaults",{}).items():protected_releases.add(model.release_key(project,release))
                # Two most recently registered versions per project are rollback pins.
                for project in {r["project"] for r in s["releases"].values()}:
                    keys=[k for k,r in s["releases"].items() if r["project"]==project]
                    protected_releases.update(keys[-2:])
                command={"request_id":b["request_id"],"apply":b.get("apply",False),"protected_releases":sorted(protected_releases),
                    "protected_objects":sorted(protected_objects),"reconstructible":[oid for oid,obj in s["objects"].items() if obj.get("reconstructible") and obj.get("sources")]}
                s.setdefault("gc_commands",{})[b["host"]]=command
                return command
            if op == "drain":
                s["hosts"][b["host"]]["draining"] = b.get("enabled", True)
                return {"draining": b.get("enabled", True)}
            if op == "retry": return model.retry(s, b["task"], b.get("uncertain", False), b.get("memory_bytes"), b.get("override_soft_memory_failure", False), b.get("max_attempts"))
            if op == "hold":
                if b.get("enabled", True) is False:
                    s["holds"] = [project for project in s["holds"] if project != b["project"]]
                    return {"released": b["project"]}
                if b["project"] not in s["holds"]: s["holds"].append(b["project"])
                return {"held": b["project"]}
            if op == "stop":
                t = s["tasks"][b["task"]]; a = s["attempts"][t["attempt_id"]]
                if a["status"] not in model.ACTIVE: raise ValueError("not active")
                if b["mode"] not in ("checkpoint", "terminate"): raise ValueError("invalid stop mode")
                a["command"] = {"kind": "STOP", "attempt_id": a["id"], "mode": b["mode"], "request_id": b["request_id"], "deadline": b.get("deadline")}
                a["status"] = t["status"] = "PAUSING" if b["mode"] == "checkpoint" else "STOPPING"
                return a["command"]
            if op == "resume":
                t = s["tasks"][b["task"]]; a = s["attempts"][t["attempt_id"]]
                if a["status"]=="FAILED" and a.get("resume"):
                    a=s["attempts"][a["resume"]["source_attempt"]]
                if a["status"] != "PAUSED": raise ValueError("valid exited checkpoint required")
                key = model.release_key(t["spec"]["project"], b["release"])
                if key not in s["releases"]: raise ValueError("unknown target")
                if not s["releases"][key].get("checkpoint_adapter_argv"): raise ValueError("target has no checkpoint adapter")
                t["resume"] = {"checkpoint": a["checkpoint"], "source_attempt": a["id"], "source_release": a["release"], "host": a["host"]}
                t.update(status="PENDING", attempt_id=None, code_policy={"release": b["release"], "missing": "wait"})
                # Checkpoint data is local until explicitly moved using an approved adapter.
                t["resume_host"] = a["host"]
                return {"resume_pending": True}
            raise ValueError("unknown operation")
        return self.mutate(b, change)

    def backup(self, b):
        return self.mutate(b, lambda s: {"path": self.store.backup(self.root / "backups")})

    def snapshot(self,b):
        return self.store.state

    async def expiry(self):
        while True:
            await asyncio.sleep(5)
            now = time.time()
            for a in list(self.store.state['attempts'].values()):
                if a['status'] in model.ACTIVE - {'UNKNOWN'} and now-self.seen.get(a['host'], self.started_at) > self.config.get('heartbeat_timeout', 90):
                    self.store.transition(None, lambda s, a=a: model.event(s, a['host'], {
                        'attempt_id': a['id'], 'kind': 'UNKNOWN',
                        'failure': {'reason': 'heartbeat lost, process death unproven'}}))


async def run_server(root, config):
    import os
    import signal
    from .ssh_transport import serve_socket
    path = Path(root).resolve()
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.stat().st_uid != os.getuid(): raise PermissionError('state root owner mismatch')
    os.chmod(path, 0o700)
    master = Master(root, config)  # Lock before checking/removing a stale socket.
    expiry = asyncio.create_task(master.expiry())
    server = asyncio.create_task(serve_socket(master, config.get('socket', str(path/'master.sock'))))
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT): loop.add_signal_handler(sig, server.cancel)
    try:
        await server
    finally:
        expiry.cancel()
        await asyncio.gather(expiry, return_exceptions=True)
        master.store.close()


def serve(root, config):
    try: asyncio.run(run_server(root, config))
    except asyncio.CancelledError: pass
