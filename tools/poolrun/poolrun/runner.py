"""Durable launch intent and independent process supervision, including Agent restarts."""
import argparse
import json
import os
from pathlib import Path
import signal
import sys
import subprocess
import time
import psutil
from .store import FileLock, atomic, read
from .release import expand, relative, sha
from .isolation import Group


def identity(pid):
    p = psutil.Process(pid)
    return {"pid": pid, "created": p.create_time(), "boot": psutil.boot_time()}


def alive(info):
    try:
        p = psutil.Process(info["pid"])
        return abs(p.create_time()-info["created"]) < .01 and abs(psutil.boot_time()-info["boot"]) < 1 and p.status() != psutil.STATUS_ZOMBIE
    except (psutil.NoSuchProcess, psutil.AccessDenied): return False


def adapter(argv, request, cwd, timeout=60):
    p = subprocess.run(argv, input=json.dumps(request), text=True, stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE, cwd=cwd, timeout=timeout, check=False)
    if p.returncode: raise ValueError("adapter failed (exit %d); private stderr not copied to control plane" % p.returncode)
    if len(p.stdout) > 1024*1024: raise ValueError("oversized adapter receipt")
    return json.loads(p.stdout)


def soft_memory_limit(directory, attempt):
    """Admission estimates are not kill limits; soft termination is opt-in."""
    policy = Path(directory).parents[2] / "runtime-memory-policy.json"
    selected = {}
    if policy.exists():
        config = read(policy)
        selected = dict(config.get("defaults", {}))
        selected.update(config.get("projects", {}).get(attempt["spec"].get("project"), {}))
    if selected.get("soft_memory_watchdog") is True:
        return attempt["resources"]["memory_bytes"]
    return None


def run(directory):
    directory = Path(directory).resolve()
    with FileLock(directory / "runner.lock"):
        if (directory / "exit.json").exists(): return
        a = read(directory / "attempt.json")
        # A process may have launched before PID acknowledgement. Never guess it did not.
        intent = directory / "spawn-intent.json"
        if intent.exists():
            atomic(directory / "exit.json", {"kind": "UNKNOWN", "failure": {"reason": "unresolved prior spawn intent"}})
            return
        paths = a["paths"]
        release = a["release"]
        argv = expand(release["argv"] + a["spec"].get("args",[]), paths)
        env = {"PATH": os.defpath, "LANG": "C.UTF-8", "PYTHONNOUSERSITE": "1", "PYTHONDONTWRITEBYTECODE": "1",
               "OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1", "MKL_NUM_THREADS": "1"}
        env.update(a.get("environment", {}))
        group=None
        try:
            if a.get("resume"):
                req = dict(a["resume"], target_release=release, business=a["spec"], spec_hash=a["spec_hash"], platform=a["platform"])
                receipt = adapter(expand(release["checkpoint_adapter_argv"], paths), req, paths["release"])
                if receipt.get("decision") != "DIRECT": raise ValueError("checkpoint not directly compatible; conversion requires a separately approved converter")
                if not receipt.get("resume_argv"): raise ValueError("adapter omitted required resume arguments")
                argv += receipt["resume_argv"]
                atomic(directory / "resume-decision.json", receipt)
            if a.get("cgroup_root"):group=Group(a["cgroup_root"],a["id"],a["resources"]["memory_bytes"])
            atomic(intent, {"runner": identity(os.getpid()), "argv": argv, "cwd": paths["release"], "env":env,"time": time.time()})
            with (directory / "stdout.log").open("ab") as out, (directory / "stderr.log").open("ab") as err:
                command=[sys.executable,str(Path(__file__).with_name("exec_gate.py")),str(directory)] if group else argv
                child = subprocess.Popen(command, cwd=paths["release"], env=env, stdout=out, stderr=err, start_new_session=True)
            info = identity(child.pid)
            atomic(directory / "process.json", info)
            if group:
                group.attach(child.pid)
                atomic(directory/"exec-go.json",{})
            known = {child.pid: info}
            soft_oom = False; stopped = False; peak = 0
            while True:
                for pid, record in list(known.items()):
                    if alive(record):
                        try:
                            for p in psutil.Process(pid).children(recursive=True): known.setdefault(p.pid, identity(p.pid))
                        except psutil.Error: pass
                running = [pid for pid,record in known.items() if alive(record)]
                if group:
                    for pid in group.pids():
                        try:
                            known.setdefault(pid,identity(pid))
                            if pid not in running and alive(known[pid]):running.append(pid)
                        except psutil.Error:pass
                # Process-group enumeration also catches reparented children in the same group.
                for p in psutil.process_iter(["pid"]):
                    try:
                        if os.getpgid(p.pid) == child.pid and p.status() != psutil.STATUS_ZOMBIE:
                            known.setdefault(p.pid, identity(p.pid))
                            if p.pid not in running: running.append(p.pid)
                    except (OSError, psutil.Error): pass
                if not running and child.poll() is not None: break
                rss = 0
                for pid in running:
                    try: rss += psutil.Process(pid).memory_info().rss
                    except psutil.Error: pass
                peak = max(peak, rss)
                limit = soft_memory_limit(directory, a)
                if not group and limit is not None and rss > limit:
                    soft_oom = True
                terminate = directory / "control" / "terminate.json"
                if a.get("checkpoint_at") and time.time()>=a["checkpoint_at"] and not (directory/"control/pause.json").exists():
                    atomic(directory/"control/pause.json",{"request_id":"deadline-"+a["id"],"attempt_id":a["id"],"action":"checkpoint_and_exit","deadline":a.get("hard_deadline")})
                pause_request=directory/"control/pause.json"
                if pause_request.exists() and not (directory/"pause-status.json").exists():
                    policy=read(pause_request)
                    if policy.get("deadline") and time.time()>policy["deadline"]:
                        atomic(directory/"pause-status.json",{"status":"UPGRADE_BLOCKED","reason":"pause deadline expired; process still running; no automatic kill authorized"})
                deadline = a.get("hard_deadline")
                if soft_oom or terminate.exists() or (deadline and time.time() >= deadline):
                    stopped = not soft_oom
                    try: os.killpg(child.pid, signal.SIGTERM)
                    except ProcessLookupError: pass
                    for pid in running:
                        try: psutil.Process(pid).terminate()
                        except psutil.Error: pass
                    time.sleep(.3)
                    for pid,record in known.items():
                        if alive(record):
                            try: psutil.Process(pid).kill()
                            except psutil.Error: pass
                time.sleep(.1)
            rc = child.wait()
            if group:
                evidence=group.events();atomic(directory/"memory-events.json",evidence)
                if evidence.get("oom_kill",0)>0:raise RuntimeError("cgroup_oom_kill")
            if soft_oom:
                raise RuntimeError("soft_memory_watchdog")
            if stopped:
                atomic(directory / "exit.json", {"kind": "STOPPED", "tree_exited": True, "peak_rss": peak, "failure": {"reason": "explicit termination/deadline"}}); return
            pause = directory / "control" / "pause.json"
            if pause.exists():
                receipt = read(directory / "checkpoints" / "checkpoint_receipt.json")
                request = read(pause)
                if rc != 0 or receipt.get("request_id") != request["request_id"] or receipt.get("attempt_id") != a["id"]:
                    raise ValueError("checkpoint lineage/exit mismatch")
                cp = directory / "checkpoints" / relative(receipt["file"])
                if cp.is_symlink() or (directory/"checkpoints").resolve() not in cp.resolve().parents or sha(cp) != receipt["sha256"]: raise ValueError("checkpoint integrity failure")
                receipt["path"] = str(cp)
                receipt["spec_hash"] = a["spec_hash"]
                atomic(directory / "exit.json", {"kind": "PAUSED", "tree_exited": True, "checkpoint": receipt, "peak_rss": peak}); return
            if rc != 0: raise RuntimeError("exit_%d (not proof of OOM)" % rc)
            result = adapter(expand(release["validator_argv"], paths), {"attempt": a, "outputs": paths["outputs"]}, paths["release"])
            if result.get("valid") is not True: raise ValueError("business validator rejected output")
            required=set(a["spec"].get("completion",{}).get("required_outputs",[]))
            if not required.issubset(result.get("files",[])):raise ValueError("required output missing")
            objects = []
            for name in result.get("files", []):
                f = directory / "outputs" / relative(name)
                if f.is_symlink() or not f.is_file() or (directory / "outputs").resolve() not in f.resolve().parents: raise ValueError("invalid result path")
                objects.append({"file": name, "size": f.stat().st_size, "sha256": sha(f)})
            if not objects: raise ValueError("validator must name durable output files")
            atomic(directory / "exit.json", {"kind": "RESULT_PENDING", "tree_exited": True, "validated": True,
                  "result": {"files": objects, "summary": result.get("summary")}, "peak_rss": peak})
        except Exception as e:
            # Any still-running process makes a failure uncertain, never retryable by inference.
            live = (directory / "process.json").exists() and alive(read(directory / "process.json"))
            atomic(directory / "exit.json", {"kind": "UNKNOWN" if live else "FAILED",
                  "failure": {"reason": str(e), "oom_evidence": "soft_watchdog" if str(e)=="soft_memory_watchdog" else "cgroup_memory.events" if str(e)=="cgroup_oom_kill" else None}})
        finally:
            if group:
                try:group.close()
                except OSError:pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(); parser.add_argument("directory")
    run(parser.parse_args().directory)
