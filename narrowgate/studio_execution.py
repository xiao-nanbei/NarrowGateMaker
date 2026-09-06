"""Thin operator-registered offline plans for the existing Studio job lifecycle.

The private operator fixes executable arguments and paths on control and worker.
This is not a sandbox for hostile operator scripts or a live-trading launcher.
"""

import hashlib
import json
import math
import os
import platform
import re
import subprocess
import sys
import time
from pathlib import Path

from narrowgate.studio_resources import identifier, safe_text

CLASSIFICATION = "operator_registered_offline"
RUNNER = "operator-registered-offline"
FILES = {
    "stdout.log",
    "stderr.log",
    "environment.json",
    "execution.json",
    "summaries.json",
    "owner-locators.json",
}
LOG_LIMIT = 256_000
SUMMARY_LIMIT = 256_000


def target_signature(target):
    """One configuration comparison, not an input hash or artifact authority chain."""
    return hashlib.sha256(
        json.dumps(target, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def initialize(db):
    db.executescript("""
        CREATE TABLE IF NOT EXISTS execution_jobs (
            job_id TEXT PRIMARY KEY, plan_id TEXT NOT NULL, revision TEXT NOT NULL,
            requested_resource_id TEXT NOT NULL, contract TEXT NOT NULL, resource_id TEXT,
            UNIQUE(plan_id,revision));
        CREATE TABLE IF NOT EXISTS execution_workers (
            worker_id TEXT PRIMARY KEY, resource_id TEXT NOT NULL, plans TEXT NOT NULL);
    """)


def job_metadata(db, job_id):
    row = db.execute("SELECT * FROM execution_jobs WHERE job_id=?", (job_id,)).fetchone()
    if row is None:
        return {
            "classification": "synthetic_non_economic",
            "plan_id": None,
            "revision": None,
            "requested_resource_id": None,
            "resource_id": None,
            "role": None,
            "queue_reason": None,
        }
    return {
        "classification": CLASSIFICATION,
        "plan_id": row["plan_id"],
        "revision": row["revision"],
        "requested_resource_id": row["requested_resource_id"],
        "resource_id": row["resource_id"],
        "role": json.loads(row["contract"])["role"],
        "queue_reason": None,
    }


def relative_file(value):
    path = Path(value)
    if not value or path.is_absolute() or ".." in path.parts or str(path) != value:
        raise ValueError(
            "output names must be relative files under the registered output directory"
        )
    return value


class Catalog:
    def __init__(self, path=None):
        self.path = path
        self.resources, self.plans = {}, {}
        if path is None:
            return
        if path.stat().st_mode & 0o077 or path.stat().st_size > 1_000_000:
            raise ValueError("execution manifest must be bounded and owner-only (0600)")
        data = json.loads(path.read_text())
        if data.get("visibility") != "local_only_do_not_publish":
            raise ValueError("execution manifest must be operator-private")
        for resource in data.get("resources", []):
            rid = identifier(resource["id"])
            if rid in self.resources or resource["kind"] not in {"local", "lan", "azure"}:
                raise ValueError("invalid or duplicate execution resource")
            self.resources[rid] = resource
        for plan in data.get("plans", []):
            pid = identifier(plan["id"])
            identifier(plan["revision"])
            if pid in self.plans or plan.get("live") is not False:
                raise ValueError("only unique explicitly offline plans can be registered")
            if plan["role"] not in {"training", "replay", "data_processing"}:
                raise ValueError("unsupported offline role")
            if not isinstance(plan.get("enabled", True), bool) or not plan.get("targets"):
                raise ValueError("plan requires fixed targets and boolean enablement")
            for rid, target in plan["targets"].items():
                if rid not in self.resources or not self.allowed(plan, rid):
                    raise ValueError("plan role is forbidden on its registered resource")
                argv = target["argv"]
                if (
                    not isinstance(argv, list)
                    or len(argv) < 2
                    or any(not isinstance(arg, str) or "\0" in arg for arg in argv)
                ):
                    raise ValueError("plan argv must be a fixed argument list")
                if not Path(argv[0]).is_absolute() or not Path(argv[0]).name.startswith("python"):
                    raise ValueError("offline adapter accepts an explicit Python executable")
                if argv[1] == "-m":
                    if len(argv) < 3 or argv[2].split(".")[0] in {"live", "subprocess", "os"}:
                        raise ValueError("live or shell modules are not an offline plan")
                elif not (Path(argv[1]).is_absolute() and argv[1].endswith(".py")):
                    raise ValueError(
                        "register a fixed Python script or offline module; no -c/shell"
                    )
                if any("/live/main.py" in arg or "/live/run.sh" in arg for arg in argv):
                    raise ValueError("live startup is not a Studio plan")
                for key in ("cwd", "output_dir"):
                    if not Path(target[key]).is_absolute():
                        raise ValueError("worker paths must be absolute in its private manifest")
                if any(not Path(p).is_absolute() for p in target.get("required_files", [])):
                    raise ValueError("required files must be explicit worker-local paths")
                outputs = target["required_outputs"]
                summaries = target.get("summary_files", [])
                if not outputs or len(set(outputs)) != len(outputs):
                    raise ValueError("required output names must be nonempty and unique")
                for name in outputs + summaries:
                    relative_file(name)
                if set(summaries) - set(outputs):
                    raise ValueError("small summaries must also be required outputs")
                for key in ("memory_gib", "max_seconds"):
                    value = target[key]
                    if (
                        isinstance(value, bool)
                        or not isinstance(value, (int, float))
                        or not math.isfinite(value)
                        or value <= 0
                    ):
                        raise ValueError("memory and time budgets must be finite positive numbers")
                if target["max_seconds"] > 7 * 86400:
                    raise ValueError("plan timeout exceeds the bounded seven-day adapter limit")
                for key, value in target.get("env", {}).items():
                    if not re.fullmatch(r"[A-Z][A-Z0-9_]*", key) or not isinstance(value, str):
                        raise ValueError(
                            "registered environment must contain literal string values"
                        )
                    if any(
                        word in key
                        for word in ("KEY", "SECRET", "TOKEN", "PASSWORD", "CREDENTIAL", "LIVE")
                    ):
                        raise ValueError(
                            "credentials and live selectors do not belong in offline plans"
                        )
                patterns = target.get("exclusive_process_contains", [])
                if not isinstance(patterns, list) or any(
                    not isinstance(p, str) or not p for p in patterns
                ):
                    raise ValueError("exclusive process match must be fixed argument substrings")
                if patterns and max(map(len, patterns)) < 16:
                    raise ValueError("exclusive process match is too broad")
            if set(plan.get("preferred_resources", [])) - plan["targets"].keys():
                raise ValueError("preferred resources must be registered plan targets")
            self.plans[pid] = plan

    def allowed(self, plan, rid):
        resource = self.resources[rid]
        if plan["role"] == "training" and resource["kind"] == "lan":
            return False
        return resource.get("roles", {}).get(plan["role"], "disabled") in {"preferred", "allowed"}

    def contract(self, plan):
        # Job ownership freezes the registered revision and portable output contract,
        # not another hash/receipt/authority hierarchy. Commands never enter HTTP.
        return {
            "id": plan["id"],
            "revision": plan["revision"],
            "label": safe_text(plan.get("label", plan["id"])),
            "role": plan["role"],
            "preferred_resources": plan.get("preferred_resources", []),
            "targets": {
                rid: {
                    "signature": target_signature(target),
                    "required_outputs": target["required_outputs"],
                    "summary_files": target.get("summary_files", []),
                }
                for rid, target in plan["targets"].items()
            },
        }

    def candidates(self, contract, requested):
        if requested != "auto":
            return [requested]
        candidates = contract["preferred_resources"] or list(contract["targets"])
        if contract["role"] != "training":
            # Local replay is a deliberate operator/user choice, never a silent
            # fallback when the LAN worker is unavailable or cloud has zero nodes.
            if not contract["preferred_resources"]:
                candidates = [rid for rid in candidates if self.resources[rid]["kind"] != "local"]
        else:
            candidates = sorted(
                candidates,
                key=lambda rid: self.resources[rid].get("roles", {}).get("training") != "preferred",
            )
        return candidates


def memory_available_gib():
    if sys.platform.startswith("linux"):
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) / 2**20
    if sys.platform == "darwin":
        data = subprocess.check_output(["vm_stat"], text=True, timeout=3)
        page_size = int(re.search(r"page size of (\d+) bytes", data).group(1))
        pages = sum(
            int(match.group(1))
            for name in ("free", "inactive", "speculative")
            if (match := re.search(rf"Pages {name}:\s+(\d+)", data))
        )
        return pages * page_size / 2**30
    return None


def preflight(target):
    try:
        if not Path(target["argv"][0]).is_file() or not os.access(target["argv"][0], os.X_OK):
            return False, "python_executable_unavailable"
        if not Path(target["cwd"]).is_dir() or any(
            not Path(p).is_file() for p in target.get("required_files", [])
        ):
            return False, "required_inputs_or_working_directory_unavailable"
        if target["argv"][1] != "-m" and not Path(target["argv"][1]).is_file():
            return False, "registered_script_unavailable"
        if Path(target["output_dir"]).exists():
            return False, "fixed_output_already_exists_no_overwrite_or_resume"
        memory = memory_available_gib()
        if memory is None or memory < target["memory_gib"]:
            return False, "insufficient_available_memory"
        patterns = target.get("exclusive_process_contains", [])
        if patterns:
            processes = subprocess.check_output(
                ["ps", "-axo", "pid=,command="], text=True, timeout=3
            )
            if any(all(pattern in line for pattern in patterns) for line in processes.splitlines()):
                return False, "registered_external_process_already_running"
        return True, None
    except (OSError, ValueError, subprocess.SubprocessError):
        return False, "worker_preflight_unavailable"


def registration(catalog, resource_id):
    if resource_id not in catalog.resources:
        raise ValueError("worker resource is not in its private execution manifest")
    plans = []
    for plan in catalog.plans.values():
        if resource_id in plan["targets"]:
            ready, reason = preflight(plan["targets"][resource_id])
            plans.append(
                {
                    "id": plan["id"],
                    "revision": plan["revision"],
                    "signature": target_signature(plan["targets"][resource_id]),
                    "ready": ready and plan.get("enabled", True),
                    "reason": reason,
                }
            )
    return {"resource_id": resource_id, "plans": plans}


def worker_view(db, lease_seconds):
    result = []
    for row in db.execute(
        "SELECT e.*,n.last_seen FROM execution_workers e JOIN nodes n ON e.worker_id=n.id"
    ):
        busy = (
            db.execute(
                "SELECT 1 FROM execution_jobs e JOIN jobs j ON j.id=e.job_id "
                "WHERE e.resource_id=? AND j.status NOT IN ('completed','failed','canceled')",
                (row["resource_id"],),
            ).fetchone()
            is not None
        )
        result.append(
            {
                "id": row["worker_id"],
                "resource_id": row["resource_id"],
                "online": row["last_seen"] >= time.time() - lease_seconds,
                "busy": busy,
                "plans": json.loads(row["plans"]),
            }
        )
    return result


def selection(catalog, contract, requested, workers):
    plan = catalog.plans.get(contract["id"])
    if not plan or plan["revision"] != contract["revision"] or not plan.get("enabled", True):
        return None, "registered_plan_revision_unavailable_or_disabled"
    if catalog.contract(plan) != contract:
        return None, "registered_plan_configuration_changed"
    candidates = catalog.candidates(contract, requested)
    for rid in candidates:
        if rid not in plan["targets"] or not catalog.allowed(plan, rid):
            continue
        for worker in workers:
            if worker["resource_id"] != rid or not worker["online"] or worker["busy"]:
                continue
            if any(
                p["id"] == contract["id"]
                and p["revision"] == contract["revision"]
                and p.get("signature") == contract["targets"][rid]["signature"]
                and p["ready"]
                for p in worker["plans"]
            ):
                return worker["id"], None
    return (
        None,
        "waiting_for_selected_resource_worker_inputs_memory_or_capacity_no_auto_local_fallback",
    )


def plans_view(catalog, workers, attempts=None):
    items = []
    for plan in catalog.plans.values():
        resources = []
        for rid in plan["targets"]:
            matching = [worker for worker in workers if worker["resource_id"] == rid]
            states = [
                p
                for worker in matching
                if worker["online"]
                for p in worker["plans"]
                if p["id"] == plan["id"] and p["revision"] == plan["revision"]
            ]
            resources.append(
                {
                    "id": rid,
                    "label": safe_text(catalog.resources[rid].get("label", rid)),
                    "eligible": catalog.allowed(plan, rid),
                    "online": any(w["online"] for w in matching),
                    "ready": any(p["ready"] for p in states),
                    "reason": next((p["reason"] for p in states if p.get("reason")), None)
                    if states
                    else "worker_not_connected_or_revision_not_loaded",
                }
            )
        items.append(
            {
                "id": plan["id"],
                "revision": plan["revision"],
                "label": safe_text(plan.get("label", plan["id"])),
                "role": plan["role"],
                "enabled": plan.get("enabled", True),
                "eligible_resources": resources,
                "attempt": (attempts or {}).get((plan["id"], plan["revision"])),
                "requirements": {
                    "continuous_plan": True,
                    "required_output_count": max(
                        len(t["required_outputs"]) for t in plan["targets"].values()
                    ),
                },
                "warnings": [
                    "One complete fixed plan; no takeover, split-day execution "
                    "or automatic lost retry"
                ],
            }
        )
    return {
        "items": items,
        "limitations": [
            "Operator-registered offline commands only; no browser commands or credentials",
            "Unavailable resources leave work queued; Azure is never allocated by this adapter",
        ],
    }


def tail(path, limit=LOG_LIMIT):
    with path.open("rb") as stream:
        stream.seek(max(0, path.stat().st_size - limit))
        return stream.read(limit).decode("utf-8", errors="replace")


def output_metadata(target):
    root = Path(target["output_dir"]).resolve()
    files, summaries = [], {}
    for name in target["required_outputs"]:
        path = root / name
        if (
            path.is_symlink()
            or not path.is_file()
            or not path.resolve().is_relative_to(root)
            or path.stat().st_size == 0
        ):
            raise ValueError("required_result_file_missing_or_invalid")
        files.append({"name": name, "size_bytes": path.stat().st_size})
        with path.open("rb") as stream:
            os.fsync(stream.fileno())
        if name in target.get("summary_files", []):
            if path.stat().st_size > SUMMARY_LIMIT:
                raise ValueError("registered_summary_exceeds_small_upload_limit")
            summaries[name] = json.loads(path.read_text())
    for directory in {root, *((root / name).parent for name in target["required_outputs"])}:
        fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    return files, summaries


def execute(client, worker_id, session, job, root, stopping, catalog, resource_id):
    from narrowgate import studio

    directory = root / identifier(job["id"])
    directory.mkdir(mode=0o700)
    route = f"/api/workers/{worker_id}/jobs/{job['id']}"
    payload = {"session": session}
    status, error, code, elapsed = "failed", None, None, 0.0
    outputs, summaries, target = [], {}, None
    runner_python = "not_started"
    try:
        plan = catalog.plans[job["plan_id"]]
        if (
            plan["revision"] != job["revision"]
            or resource_id != job["resource_id"]
            or not catalog.allowed(plan, resource_id)
        ):
            raise ValueError("claimed_plan_revision_or_resource_mismatch")
        target = plan["targets"][resource_id]
        if target_signature(target) != job.get("target_signature"):
            raise ValueError("claimed_plan_configuration_changed")
        ready, reason = preflight(target)
        if not ready:
            raise ValueError(reason)
        version = subprocess.check_output(
            [target["argv"][0], "--version"], text=True,
            stderr=subprocess.STDOUT, timeout=3,
        ).strip()
        if not re.fullmatch(r"Python \d+\.\d+\.\d+[^\s]*", version):
            raise ValueError("registered_python_version_unavailable")
        runner_python = version.removeprefix("Python ")
        Path(target["output_dir"]).mkdir(parents=True, exist_ok=False, mode=0o700)
        environment = {
            key: os.environ[key]
            for key in ("PATH", "LANG", "SYSTEMROOT", "TMPDIR")
            if key in os.environ
        }
        environment.update({"PYTHONUNBUFFERED": "1", **target.get("env", {})})
        status, error, code, elapsed = studio.run_child(
            client,
            route,
            payload,
            target["argv"],
            Path(target["cwd"]),
            environment,
            directory,
            stopping,
            target["max_seconds"],
        )
        if status == "completed":
            outputs, summaries = output_metadata(target)
    except (KeyError, ValueError, OSError, subprocess.SubprocessError) as exc:
        status, error = "failed", safe_text(str(exc))
    for name in ("stdout.log", "stderr.log"):
        path = directory / name
        if not path.exists():
            path.touch(mode=0o600)
    if error:
        with (directory / "stderr.log").open("a") as stream:
            stream.write(error + "\n")
            stream.flush()
            os.fsync(stream.fileno())
    record = {
        "plan_id": job["plan_id"],
        "revision": job["revision"],
        "resource_id": resource_id,
        "role": job["role"],
        "required_outputs": outputs,
    }
    files = {name: tail(directory / name) for name in ("stdout.log", "stderr.log")}
    files["execution.json"] = studio.dumps(record)
    files["summaries.json"] = studio.dumps(summaries)
    files["environment.json"] = studio.dumps(
        {
            "python": runner_python,
            "worker_python": sys.version.split()[0],
            "platform": platform.platform(),
            "runner": RUNNER,
            "elapsed_seconds": elapsed,
            "returncode": code,
            "resource_id": resource_id,
            "cwd": target["cwd"] if target else None,
        }
    )
    files["owner-locators.json"] = studio.dumps(
        {
            "output_dir": target["output_dir"] if target else None,
            "stdout_path": str(directory / "stdout.log"),
            "stderr_path": str(directory / "stderr.log"),
        }
    )
    payload.update(status=status, error=error, files=files)
    studio.atomic_text(directory / "publication.json", studio.dumps(payload))
    studio.publish_outbox(client, route, payload, directory)


def validate_publication(files, job, contract):
    if not {"execution.json", "summaries.json", "owner-locators.json"} <= files.keys():
        raise ValueError("registered execution metadata must be durable before completion")
    result = json.loads(files["execution.json"])
    environment = json.loads(files["environment.json"])
    if any(result[key] != job[key] for key in ("plan_id", "revision", "resource_id", "role")):
        raise ValueError("execution publication does not match its claimed plan/revision/resource")
    if (
        type(environment.get("returncode")) is not int
        or environment["returncode"] != 0
        or environment.get("runner") != RUNNER
    ):
        raise ValueError("successful fixed runner exit is required")
    target = contract["targets"][job["resource_id"]]
    outputs = result["required_outputs"]
    if (
        len(outputs) != len(target["required_outputs"])
        or {r["name"] for r in outputs} != set(target["required_outputs"])
        or any(type(r["size_bytes"]) is not int or r["size_bytes"] <= 0 for r in outputs)
    ):
        raise ValueError("all required result files must be present and nonempty")
    if set(json.loads(files["summaries.json"])) != set(target["summary_files"]):
        raise ValueError("only registered small summaries may be published")


def public_value(value):
    if isinstance(value, dict):
        return {safe_text(key): public_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [public_value(item) for item in value]
    return safe_text(value, limit=LOG_LIMIT) if isinstance(value, str) else value


def report(files):
    record = json.loads(files["execution.json"])
    environment = json.loads(files["environment.json"])
    return {
        "schema_version": "registered_execution_report.v1",
        "classification": CLASSIFICATION,
        **{key: record[key] for key in ("plan_id", "revision", "resource_id", "role")},
        "summary": public_value(json.loads(files["summaries.json"])),
        "artifacts": record["required_outputs"],
        "environment": {
            key: environment.get(key)
            for key in (
                "python",
                "platform",
                "runner",
                "elapsed_seconds",
                "returncode",
                "resource_id",
            )
        },
        "limitations": [
            "Operator-registered offline process, not a synthetic demo or live action",
            "Large original outputs and complete logs remain on the worker persistent disk",
            "Process completion and file existence are not economic or policy promotion gates",
        ],
    }
