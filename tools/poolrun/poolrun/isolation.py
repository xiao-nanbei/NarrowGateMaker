"""Optional delegated Linux cgroup v2; never claim hard isolation on macOS."""
import os
from pathlib import Path
import platform


def available(root):
    if not root or platform.system()!="Linux":return False
    p=Path(root)
    try:return "memory" in (p/"cgroup.controllers").read_text().split() and os.access(p,os.W_OK)
    except OSError:return False


class Group:
    def __init__(self,root,attempt,memory):
        if not available(root):raise ValueError("delegated cgroup v2 memory controller unavailable")
        self.path=Path(root)/("poolrun-"+attempt)
        self.path.mkdir(exist_ok=False)
        try:
            (self.path/"memory.max").write_text(str(memory))
            (self.path/"memory.oom.group").write_text("1")
        except BaseException:
            self.path.rmdir();raise

    def attach(self,pid):(self.path/"cgroup.procs").write_text(str(pid))

    def pids(self):return [int(x) for x in (self.path/"cgroup.procs").read_text().split()]

    def events(self):
        return {k:int(v) for k,v in (line.split() for line in (self.path/"memory.events").read_text().splitlines())}

    def close(self):
        if not self.pids():self.path.rmdir()
