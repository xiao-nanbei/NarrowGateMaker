"""Pure state transitions; a result has one authoritative generation, not exactly-once execution."""
import copy
import time
import uuid
from .release import identifier
from .store import digest

ACTIVE = {"STARTING", "RUNNING", "PAUSING", "STOPPING", "UNKNOWN"}
PENDING = {"PENDING", "WAIT_DATA", "WAIT_RELEASE", "RETRY_WAIT"}


def initial():
    return {"schema": 1, "tasks": {}, "attempts": {}, "hosts": {}, "releases": {},
            "objects": {}, "requests": {}, "project_turn": 0, "holds": [], "prepare_requests": {}, "transfers": {}}


def release_key(project, release):
    return identifier(project) + "/" + identifier(release)


def submit(s, tasks):
    added = 0
    for source in tasks:
        t = copy.deepcopy(source)
        # Accept the document's verbose schema as well as the compact CLI schema.
        if "task_id" in t:
            t["id"] = t.pop("task_id")
            business = t.get("business_spec", {})
            t["inputs"] = [x["object_id"] for x in business.get("input_refs", [])]
            t["depends_on"] = business.get("depends_on", [])
            t["parameters"] = business.get("params", {})
            t["args"] = business.get("args", [])
            r = t["resources"]
            t["resources"] = {"cpus": r.get("cpu", 1), "memory_bytes": int(r["memory_gib"]*2**30),
                "disk_bytes": int((r.get("temp_gib",0)+r.get("output_gib",0)+r.get("checkpoint_gib",0))*2**30)}
            t["side_effects"] = t.get("side_effects") != "isolated_outputs"
            t["max_attempts"] = t.get("retry", {}).get("max_attempts", 3)
        task_id = identifier(t["id"])
        identifier(t["project"])
        policy = t.pop("code_policy")
        if not policy.get("release"):
            policy["release"] = s.get("project_defaults", {}).get(t["project"])
        if not policy.get("release"): raise ValueError("explicit release or registered project default required")
        r = t.setdefault("resources", {})
        for field in ("memory_bytes", "disk_bytes", "cpus"):
            if not isinstance(r.get(field), (int, float)) or r[field] <= 0:
                raise ValueError("positive resource budgets required: " + field)
        if not t.get("validator_required", True):
            raise ValueError("business validator is mandatory")
        for obj in t.get("inputs", []):
            if obj not in s["objects"]:
                raise ValueError("unknown input object: " + obj)
        if t.get("prepared"):
            recipe=t["prepared"]
            if not recipe.get("platform") or not recipe.get("contract") or not recipe.get("recipe_version"):raise ValueError("prepared recipe requires platform, contract, recipe_version")
            inputs=sorted(t.get("inputs",[]))
            if "input_ids" in recipe and sorted(recipe["input_ids"])!=inputs:raise ValueError("prepared recipe input identity mismatch")
            recipe["input_ids"]=inputs
        if 'cache_affinity' in t:
            hint=t['cache_affinity']
            if (not isinstance(hint,dict) or not isinstance(hint.get('namespace'),str)
                    or not hint['namespace'] or not isinstance(hint.get('identity'),dict) or not hint['identity']):
                raise ValueError('cache_affinity requires namespace and exact cache identity')
        old = s["tasks"].get(task_id)
        if old:
            if old["spec_hash"] != digest(t):
                raise ValueError("immutable business spec conflict: " + task_id)
            continue  # Imports never undo a rollout.
        s["tasks"][task_id] = {"spec": t, "spec_hash": digest(t), "code_policy": policy,
            "status": "PENDING", "generation": 0, "attempt_id": None,
            "submitted": time.time(), "failures": 0, "avoid_hosts": [], "reason": "queued"}
        added += 1
    return {"added": added}


def resources_used(s, host):
    used = {"slots": 0, "memory_bytes": 0, "disk_bytes": 0, "cpus": 0}
    for a in s["attempts"].values():
        if a["host"] != host:
            continue
        if a["status"] in ACTIVE:
            used["slots"] += 1
            for key in ("memory_bytes", "cpus"):
                used[key] += a["resources"][key]
        if a["status"] in ACTIVE | {"RESULT_PENDING", "PAUSED"}:
            used["disk_bytes"] += a["resources"]["disk_bytes"]
    return used


def selected_release(s,t,report):
    project=t["spec"]["project"];policy=t["code_policy"]
    target=release_key(project,policy["release"])
    if target in report.get("releases",[]):return target
    if policy.get("missing")=="fallback":
        required=s["releases"].get(target,{}).get("output_contract")
        for version in policy.get("allowed_fallback",[]):
            key=release_key(project,version)
            if key in report.get("releases",[]) and s["releases"].get(key,{}).get("output_contract")==required:return key
    return target


def eligible(s, t, host, report, now):
    h = s["hosts"][host]
    spec = t["spec"]
    if s.get("reconciliation_required"): return "backup/foreign attempt reconciliation required"
    if host in s.get("gc_commands",{}):return "host cache GC pending"
    if t.get("resume_host") and t["resume_host"] != host: return "checkpoint not staged on this host"
    if t["status"] not in PENDING: return "task is not pending"
    for dependency in spec.get("depends_on", []):
        upstream = s["tasks"].get(dependency)
        if not upstream or upstream["status"] != "COMPLETE": return "WAIT_DEPENDENCY: " + dependency
        if upstream["spec"]["project"] in s["holds"]: return "upstream correctness hold"
    if h.get("draining"): return "host draining"
    if now >= h.get("stop_accepting_at", float("inf")): return "host acceptance deadline"
    if host in t["avoid_hosts"]: return "previous failure excludes this host"
    allowed_hosts = t.get("placement_hosts", spec.get("hosts"))
    if allowed_hosts and host not in allowed_hosts: return "host restriction"
    if spec.get("prepared") and spec["prepared"]["platform"]!=h["platform"]:return "prepared platform mismatch"
    if spec.get("requires_hard_isolation") and not report.get("hard_isolation"): return "hard isolation unavailable"
    if not set(spec.get("capabilities", [])).issubset(h.get("capabilities", [])): return "capabilities"
    if spec["project"] in s["holds"]: return "correctness hold"
    key = selected_release(s,t,report)
    if key not in s["releases"]: return "release unregistered"
    rel = s["releases"][key]
    required = t["code_policy"].get("required_output_contract") or spec.get("business_spec", {}).get("output_contract")
    if required and required != rel.get("output_contract"): return "output contract mismatch"
    if h["platform"] not in rel["platforms"]: return "release platform unsupported"
    if key not in report.get("releases", []): return "release not ready"
    if rel.get("env_id") and rel["env_id"] not in report.get("environment_bindings",{}):return "WAIT_ENV: environment mapping unavailable"
    missing = set(spec.get("inputs", [])) - set(report.get("objects", []))
    if missing: return "missing data: " + ",".join(sorted(missing))
    if spec.get("prepared") and digest(spec["prepared"]) not in report.get("prepared",[]): return "prepared cache not ready"
    used = resources_used(s, host)
    for key in ("memory_bytes","disk_bytes","cpus"):
        used[key]+=report.get("preparation_reservation",{}).get(key,0)
    if used["slots"] >= h["slots"]: return "slots reserved"
    need = dict(spec["resources"], **t.get("resource_override", {}))
    enforce_memory = h.get("memory_admission", True) is not False
    for k in ("memory_bytes", "disk_bytes", "cpus"):
        if k == "memory_bytes" and not enforce_memory: continue
        if used[k] + need[k] > h[k]: return "reserved " + k
    if enforce_memory and report.get("memory_available", 0) < need["memory_bytes"] + h.get("memory_headroom", 0): return "live memory guard"
    if report.get("disk_free", 0) < need["disk_bytes"] + h.get("disk_headroom", 0): return "live disk guard"
    if report.get("inodes_free", 0) < spec.get("inodes", 128): return "inode guard"
    if now < t.get("retry_after", 0): return "retry backoff"
    return None


def choose(s, host, report, now=None, allowed=None):
    now = now or time.time()
    for t in sorted(s["tasks"].values(),key=lambda x:x["submitted"]):
        if allowed is not None and t['spec']['id'] not in allowed: continue
        need=dict(t["spec"]["resources"],**t.get("resource_override",{}))
        if now-t["submitted"]>60 and need["memory_bytes"]<=s["hosts"][host]["memory_bytes"]:
            reason=eligible(s,t,host,report,now)
            if reason in {"reserved memory_bytes","live memory guard"}:
                return None # Let in-flight work drain to make headroom for the aged large job.
    candidates = [t for t in s["tasks"].values() if (allowed is None or t['spec']['id'] in allowed)
                  and eligible(s, t, host, report, now) is None]
    if not candidates: return None
    projects = sorted({t["spec"]["project"] for t in candidates})
    project = projects[s["project_turn"] % len(projects)]
    candidates = [t for t in candidates if t["spec"]["project"] == project]
    # Age is unbounded: a low priority job cannot starve forever behind new jobs.
    candidates.sort(key=lambda t: (-(t["spec"].get("priority", 0) + (now-t["submitted"])/300), t["submitted"]))
    return candidates[0]["spec"]["id"]


def start(s, task_id, host, report):
    t = s["tasks"][task_id]
    reason = eligible(s, t, host, report, time.time())
    if reason: raise ValueError(reason)
    release = s["releases"][selected_release(s,t,report)]
    aid = uuid.uuid4().hex
    generation = t["generation"] + 1
    a = {"id": aid, "task_id": task_id, "host": host, "generation": generation,
         "status": "STARTING", "created": time.time(), "spec_hash": t["spec_hash"],
         "spec": copy.deepcopy(t["spec"]), "release": copy.deepcopy({k:v for k,v in release.items() if k!="files"}),
         "resources": dict(t["spec"]["resources"], **t.get("resource_override", {})), "resume": t.pop("resume", None)}
    if release.get("env_id"):a["environment_binding"]=copy.deepcopy(report["environment_bindings"][release["env_id"]])
    s["attempts"][aid] = a
    t.update(status="STARTING", attempt_id=aid, generation=generation, reason="START permission persisted")
    s["project_turn"] += 1
    return copy.deepcopy(a)


def reconcile_heartbeat(s, host, aid, evidence):
    """Recover only a lost-heartbeat uncertainty, never a failed/replaced process."""
    a = s["attempts"][aid]
    t = s["tasks"][a["task_id"]]
    if (a["host"] != host or a["status"] != "UNKNOWN"
            or (a.get("failure") or {}).get("reason") != "heartbeat lost, process death unproven"
            or a.get("command") or t["attempt_id"] != aid
            or t["generation"] != a["generation"]
            or evidence.get("generation") != a["generation"]
            or not a.get("process") or evidence.get("process") != a["process"]):
        return False
    a["heartbeat_recovery"] = {"at": time.time(), "previous_failure": a["failure"]}
    a.pop("failure", None)
    a.pop("finished", None)
    a["status"] = t["status"] = "RUNNING"
    t["reason"] = "same live process reconciled after heartbeat recovery"
    return True


def event(s, host, e):
    a = s["attempts"][e["attempt_id"]]
    if a["host"] != host: raise ValueError("wrong attempt owner")
    t = s["tasks"][a["task_id"]]
    if t["attempt_id"] != a["id"] or t["generation"] != a["generation"]:
        return {"accepted": False, "reason": "stale generation"}
    kind = e["kind"]
    if a["status"]=="COMPLETE" and kind!="COMPLETE":return {"accepted":False,"reason":"already terminal"}
    if kind == "RUNNING":
        if a["status"] == "STARTING":
            a["status"] = t["status"] = "RUNNING"
            a["process"] = e["process"]
    elif kind == "RESULT_PENDING":
        if a["status"] in ACTIVE:
            a["status"] = t["status"] = "RESULT_PENDING"
    elif kind == "COMPLETE":
        if a["status"] == "COMPLETE":
            if a["result"] != e["result"]: raise ValueError("conflicting result")
            return {"accepted": True}
        if t["spec"]["project"] in s["holds"]: return {"accepted": False, "reason": "correctness hold"}
        if not e.get("tree_exited") or not e.get("validated") or not e.get("durable_receipt"):
            raise ValueError("missing completion evidence")
        a.update(status="COMPLETE", result=e["result"], durable_receipt=e["durable_receipt"], finished=time.time())
        t.update(status="COMPLETE", reason="validated and durably saved")
    elif kind == "PAUSED":
        if not e.get("tree_exited") or not e.get("checkpoint"): raise ValueError("pause evidence missing")
        a.update(status="PAUSED", checkpoint=e["checkpoint"])
        t.update(status="PAUSED", reason="checkpoint preserved; not a completed task")
    elif kind in {"FAILED", "UNKNOWN", "STOPPED"}:
        a.update(status=kind, failure=e.get("failure"), finished=time.time())
        t.update(status=kind, reason=e.get("failure", {}).get("reason", kind))
        if kind == "FAILED": t["failures"] += 1
    else: raise ValueError("unknown event")
    return {"accepted": True}


def rollout(s, project, release, scope="pending"):
    if scope != "pending": raise ValueError("future-only rollout requires pending scope")
    if release_key(project, release) not in s["releases"]: raise ValueError("unknown release")
    s.setdefault("project_defaults", {})[project] = release
    changed = []
    for tid, t in s["tasks"].items():
        if t["spec"]["project"] == project and t["status"] in PENDING and not t["attempt_id"]:
            t["code_policy"] = {"release": release, "missing": "wait"}
            changed.append(tid)
    return {"changed": changed}


def retry(s, task_id, uncertain=False, memory_bytes=None, override_soft_memory_failure=False, max_attempts=None):
    t = s["tasks"][task_id]
    if t["status"] not in {"FAILED", "STOPPED", "UNKNOWN"}: raise ValueError("task is not retryable")
    if t["status"] == "UNKNOWN" and (not uncertain or t["spec"].get("side_effects", True)):
        raise ValueError("uncertain execution requires isolated outputs, no side effects and explicit approval")
    old = s["attempts"][t["attempt_id"]]
    limit = t.get("attempt_limit_override", t["spec"].get("max_attempts", 3))
    if max_attempts is not None:
        if type(max_attempts) is not int or max_attempts < limit or max_attempts <= t["generation"]:
            raise ValueError("explicit attempt limit must not decrease and must allow this retry")
        limit = max_attempts
    if override_soft_memory_failure:
        if old["status"] != "FAILED" or (old.get("failure") or {}).get("reason") != "soft_memory_watchdog":
            raise ValueError("override requires confirmed soft-memory-watchdog failure")
        if t.get("soft_memory_retry_authorized"):
            raise ValueError("one-time soft memory retry already used")
        t["soft_memory_retry_authorized"] = True
    elif t["generation"] >= limit: raise ValueError("attempt limit reached")
    if not override_soft_memory_failure and (old["status"] == "UNKNOWN" or (old.get("failure") or {}).get("oom_evidence")):
        t["avoid_hosts"].append(old["host"])
    if memory_bytes is not None:
        if memory_bytes < t["spec"]["resources"]["memory_bytes"]: raise ValueError("memory estimate cannot decrease")
        # Runtime reservation is distinct from business parameters/spec hash.
        t["resource_override"] = {"memory_bytes": memory_bytes}
    if max_attempts is not None:
        # Administrative budget, not a mutation of the frozen business spec.
        t["attempt_limit_override"] = limit
    old["status"] = "REVOKED" if old["status"] == "UNKNOWN" else old["status"]
    t.update(status="PENDING", attempt_id=None, retry_after=time.time(), reason="explicit retry approved")
    return {"status": t["status"]}
