"""Owner-configured compute inventory; observations never submit or restart work."""

import base64
import copy
import hashlib
import json
import math
import re
import shlex
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

REFRESH_SECONDS = 30
STALE_SECONDS = 90
PROBE_TIMEOUT = 15
LIMITATIONS = [
    "Resources are hosts or elastic pools, not a count of demo worker processes.",
    "Observed canonical-runner jobs are read-only and are never claimed or resubmitted by Studio.",
    "Observed completion is process-status evidence, not economic or final-artifact admission.",
    "Roles describe owner placement policy, not an installed real-market scheduling adapter.",
    "Azure account, pool capacity and quota may change; registration does not allocate resources.",
]

# One fixed metadata probe shared by local and SSH hosts. Only owner-selected
# small process-status JSON is read; logs, market tapes and economics are not.
HOST_PROBE = r"""
import base64,json,os,platform,subprocess,sys,time
from pathlib import Path
cfg=json.loads(base64.b64decode(sys.argv[1]))
def command(args):
    return subprocess.check_output(args,text=True,timeout=3).strip()
hardware={'architecture':platform.machine(),'vcpu':os.cpu_count(),
          'cpu_name':None,'memory_gib':None,'memory_available_gib':None}
if sys.platform=='darwin':
    hardware['cpu_name']=command(['sysctl','-n','machdep.cpu.brand_string'])
    hardware['memory_gib']=int(command(['sysctl','-n','hw.memsize']))/2**30
elif sys.platform.startswith('linux'):
    cpu=Path('/proc/cpuinfo').read_text()
    hardware['cpu_name']=next((s.split(':',1)[1].strip() for s in cpu.splitlines()
                               if s.startswith('model name')),None)
    memory={line.split(':',1)[0]:int(line.split()[1])
            for line in Path('/proc/meminfo').read_text().splitlines()
            if line.startswith(('MemTotal:','MemAvailable:'))}
    hardware['memory_gib']=memory.get('MemTotal',0)/2**20
    hardware['memory_available_gib']=memory.get('MemAvailable',0)/2**20
jobs=[]
processes=command(['ps','-axo','pid=,command=']).splitlines() if cfg.get('jobs') else []
for item in cfg.get('jobs',[]):
    found=False
    match=item.get('process_contains',[])
    for line in processes:
        parts=line.strip().split(None,1)
        if (len(parts)==2 and int(parts[0])!=os.getpid() and match
                and all(value in parts[1] for value in match)):
            found=True
    state='running' if found else ('not_started' if item.get('planned') else 'unknown')
    updated=None
    error=None
    if item.get('status_path'):
        path=Path(item['status_path'])
        try:
            if path.is_file():
                if path.stat().st_size>65536:
                    raise ValueError('status file too large')
                status=json.loads(path.read_text())
                code=status.get('exit_code',status.get('returncode'))
                terminal=status.get('state',status.get('status'))
                if code is not None:
                    state='completed' if code==0 else 'failed'
                elif terminal in ('completed','failed','canceled'):
                    state=terminal
                elif terminal=='running' and not found:
                    state='unknown'
                if found and state in ('completed','failed','canceled'):
                    state='unknown'
                    error='terminal_status_conflicts_with_live_process'
                updated=status.get('ended_unix_s',status.get('completed_unix_s'))
                if not isinstance(updated,(int,float)):
                    updated=path.stat().st_mtime
        except (OSError,ValueError,TypeError):
            state='unknown'
            error='status_file_unreadable_or_invalid'
    jobs.append({'id':item['id'],'status':state,'updated_unix_s':updated,'error':error})
print(json.dumps({'hardware':hardware,'jobs':jobs},allow_nan=False))
"""


def timestamp(value):
    if value is None:
        return None
    return datetime.fromtimestamp(value, UTC).isoformat()


def safe_text(value, limit=240):
    text = str(value or "")[:limit]
    text = re.sub(r"(?:/[\w.~-]+){2,}[^\s;]*|[A-Za-z]:\\[^\s;]*", "[private locator]", text)
    return re.sub(r"\b(?:\d{1,3}\.){3}\d{1,3}\b|\S+@\S+", "[private endpoint]", text)


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-zA-Z0-9_-]{1,100}", value):
        raise ValueError("invalid resource or job identifier")
    return value


def number(value):
    return (
        value
        if isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value >= 0
        else None
    )


def load_manifest(path):
    if path.stat().st_mode & 0o077:
        raise ValueError("compute resource manifest must be owner-only (mode 0600)")
    raw = path.read_bytes()
    if len(raw) > 256_000:
        raise ValueError("resource manifest exceeds bounded size")
    data = json.loads(raw)
    if data.get("visibility") != "local_only_do_not_publish":
        raise ValueError("resource manifest must be owner-private")
    items = data.get("resources")
    if not isinstance(items, list) or len(items) > 16:
        raise ValueError("provide at most sixteen configured resources")
    ids = set()
    for item in items:
        resource_id = identifier(item["id"])
        if resource_id in ids:
            raise ValueError("duplicate resource")
        ids.add(resource_id)
        if item["kind"] not in {"local", "lan", "azure"}:
            raise ValueError("unsupported resource kind")
        probe = item["probe"]
        if probe["type"] != {"local": "local", "lan": "ssh", "azure": "azure_batch"}[item["kind"]]:
            raise ValueError("resource kind and fixed probe disagree")
        if probe["type"] == "ssh":
            if not re.fullmatch(r"[a-zA-Z0-9_.-]+", probe["alias"]) or probe["alias"].startswith(
                "-"
            ):
                raise ValueError("SSH probe requires a configured alias, not arbitrary arguments")
            if not Path(probe["python"]).is_absolute():
                raise ValueError("SSH probe requires an absolute existing interpreter")
        if probe["type"] == "azure_batch":
            for key in ("cli", "config_dir"):
                if not Path(probe[key]).is_absolute():
                    raise ValueError("Azure CLI and authentication directory must be explicit")
            if not re.fullmatch(r"https://[a-zA-Z0-9.-]+/?", probe["account_endpoint"]):
                raise ValueError("invalid Azure Batch endpoint")
            if item.get("jobs"):
                raise ValueError("Azure pool inventory does not inspect node-local job files")
        roles = item.get("roles", {})
        if set(roles) - {"training", "replay", "data_processing"} or any(
            value not in {"preferred", "allowed", "disabled", "unknown"} for value in roles.values()
        ):
            raise ValueError("invalid owner placement roles")
        jobs = item.get("jobs", [])
        if not isinstance(jobs, list) or len(jobs) > 64:
            raise ValueError("too many observed jobs")
        job_ids = set()
        for job in jobs:
            job_id = identifier(job["id"])
            if job_id in job_ids:
                raise ValueError("duplicate observed job")
            job_ids.add(job_id)
            if job.get("status_path") and not Path(job["status_path"]).is_absolute():
                raise ValueError("job status path must be absolute and owner-selected")
            patterns = job.get("process_contains", [])
            if not isinstance(patterns, list) or any(
                not isinstance(p, str) or not p for p in patterns
            ):
                raise ValueError("process match must be fixed argument substrings")
            if patterns and max(map(len, patterns)) < 16:
                raise ValueError("process match must identify a specific canonical run")
        for worker in item.get("worker_ids", []):
            identifier(worker)
    return items, hashlib.sha256(raw).hexdigest()


def initial(item):
    return {
        "id": item["id"],
        "label": safe_text(item.get("label", item["id"])),
        "kind": item["kind"],
        "state": "unknown",
        "checked_at": None,
        "last_error": None,
        "hardware": dict.fromkeys(
            ("cpu_name", "vcpu", "memory_gib", "memory_available_gib", "architecture")
        ),
        "capacity": {"running_nodes": None, "target_nodes": None},
        "roles": {
            key: item.get("roles", {}).get(key, "unknown")
            for key in ("training", "replay", "data_processing")
        },
        "scheduler": {
            "mode": "external_observer",
            "can_submit": False,
            "reason": "Observed resources are not a real-market or training submission adapter",
        },
        "jobs": [
            {
                "id": job["id"],
                "label": safe_text(job.get("label", job["id"])),
                "status": "unknown",
                "updated_at": None,
                "arm": safe_text(job.get("arm")) or None,
            }
            for job in item.get("jobs", [])
        ],
        "worker_ids": item.get("worker_ids", []),
        "notes": [safe_text(note) for note in item.get("notes", [])],
    }


def run_json(command, *, env=None):
    result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=PROBE_TIMEOUT)
    if result.returncode:
        # CLI/SSH stderr can contain endpoints, credentials or private paths.
        raise RuntimeError(f"probe_exit_{result.returncode}")
    if len(result.stdout) > 128_000:
        raise ValueError("probe_output_exceeds_limit")
    return json.loads(result.stdout)


def observe(item):
    import os

    result = initial(item)
    probe = item["probe"]
    try:
        if probe["type"] == "azure_batch":
            command = [
                probe["cli"],
                "batch",
                "pool",
                "show",
                "--pool-id",
                probe["pool_id"],
                "--account-name",
                probe["account_name"],
                "--account-endpoint",
                probe["account_endpoint"],
                "--only-show-errors",
                "--output",
                "json",
                "--query",
                "{state:state,allocationState:allocationState,vmSize:vmSize,currentDedicatedNodes:currentDedicatedNodes,currentLowPriorityNodes:currentLowPriorityNodes,targetDedicatedNodes:targetDedicatedNodes,targetLowPriorityNodes:targetLowPriorityNodes}",
            ]
            data = run_json(
                command,
                env=dict(
                    os.environ,
                    AZURE_CONFIG_DIR=probe["config_dir"],
                    AZURE_CORE_COLLECT_TELEMETRY="no",
                ),
            )
            counts = [
                number(data.get(key))
                for key in (
                    "currentDedicatedNodes",
                    "currentLowPriorityNodes",
                    "targetDedicatedNodes",
                    "targetLowPriorityNodes",
                )
            ]
            running = sum(counts[:2]) if None not in counts[:2] else None
            target = sum(counts[2:]) if None not in counts[2:] else None
            result["capacity"] = {"running_nodes": running, "target_nodes": target}
            result["state"] = "scaled_to_zero" if running == target == 0 else "unknown"
            result["hardware"]["cpu_name"] = safe_text(data.get("vmSize")) or None
            result["scheduler"]["mode"] = "not_connected"
            result["notes"].extend(
                [
                    "Pool state: " + safe_text(data.get("state")),
                    "Allocation state: " + safe_text(data.get("allocationState")),
                    "Pool capacity is not worker or SSH health; quota and credit are unverified",
                ]
            )
        else:
            encoded = base64.b64encode(json.dumps({"jobs": item.get("jobs", [])}).encode()).decode()
            command = [sys.executable, "-I", "-c", HOST_PROBE, encoded]
            if probe["type"] == "ssh":
                remote = shlex.join([probe["python"], "-I", "-c", HOST_PROBE, encoded])
                command = [
                    "ssh",
                    "-o",
                    "BatchMode=yes",
                    "-o",
                    "StrictHostKeyChecking=yes",
                    "-o",
                    "ConnectTimeout=10",
                    "-o",
                    "ConnectionAttempts=1",
                    probe["alias"],
                    remote,
                ]
            data = run_json(command)
            result["state"] = "online"
            hardware = data["hardware"]
            result["hardware"] = {
                key: safe_text(hardware.get(key)) or None
                if key in ("cpu_name", "architecture")
                else number(hardware.get(key))
                for key in result["hardware"]
            }
            observed = {row["id"]: row for row in data["jobs"]}
            for job in result["jobs"]:
                row = observed[job["id"]]
                if row["status"] not in {
                    "running",
                    "completed",
                    "failed",
                    "canceled",
                    "not_started",
                    "unknown",
                }:
                    raise ValueError("invalid_job_state")
                job.update(
                    status=row["status"], updated_at=timestamp(number(row.get("updated_unix_s")))
                )
                if row.get("error"):
                    result["notes"].append(safe_text(row["error"]))
    except subprocess.TimeoutExpired:
        result["last_error"] = "probe_timeout_15s"
    except RuntimeError as exc:
        result["state"] = "unknown"
        result["last_error"] = probe["type"] + "_" + str(exc)
    except (OSError, ValueError, TypeError, KeyError):
        result["state"] = "unknown"
        result["last_error"] = "probe_failed_no_current_health_claim"
    result["checked_at"] = timestamp(time.time())
    return result


class ResourceCatalog:
    """A single control-service refresh loop; HTTP only reads sanitized snapshots."""

    def __init__(self, manifest_path: Path | None):
        self.path = manifest_path
        self.lock = threading.Lock()
        self.refresh_lock = threading.Lock()
        self.generation = 0
        self.observed_wall = None
        self.observed_mono = None
        self.items = []
        if self.path:
            items, _ = load_manifest(self.path)
            self.items = [initial(item) for item in items]

    def refresh(self):
        if not self.path or not self.refresh_lock.acquire(blocking=False):
            return
        started = time.time()
        try:
            items, digest = load_manifest(self.path)
            with ThreadPoolExecutor(max_workers=4) as executor:
                observed = list(executor.map(observe, items))
            _, current_digest = load_manifest(self.path)
            ended = time.time()
            with self.lock:
                if digest != current_digest:
                    return  # Configuration changed while probes ran; discard this generation.
                if ended < started or (
                    self.observed_wall is not None and ended < self.observed_wall
                ):
                    for item in self.items:
                        item["state"] = "stale"
                        item["last_error"] = "clock_moved_backwards"
                        for job in item["jobs"]:
                            if job["status"] == "running":
                                job["status"] = "unknown"
                    return
                self.items = observed
                self.observed_wall, self.observed_mono = ended, time.monotonic()
                self.generation += 1
        except Exception:
            # Preserve visibility of the registration, never preserve a green
            # badge after an unexpected refresh or owner-manifest failure.
            with self.lock:
                for item in self.items:
                    item["state"] = "unknown"
                    item["last_error"] = "resource_refresh_failed"
                    for job in item["jobs"]:
                        if job["status"] == "running":
                            job["status"] = "unknown"
        finally:
            self.refresh_lock.release()

    def snapshot(self):
        with self.lock:
            items = copy.deepcopy(self.items)
            if self.observed_wall is not None:
                age = time.monotonic() - self.observed_mono
                backwards = time.time() < self.observed_wall - 1
                if age > STALE_SECONDS or age < 0 or backwards:
                    for item in items:
                        item["state"] = "stale"
                        item["last_error"] = (
                            "observation_expired" if not backwards else "clock_moved_backwards"
                        )
                        for job in item["jobs"]:
                            if job["status"] == "running":
                                job["status"] = "unknown"
            return {
                "schema_version": "compute_resources.v1",
                "observed_at": timestamp(self.observed_wall),
                "items": items,
                "limitations": LIMITATIONS,
            }
