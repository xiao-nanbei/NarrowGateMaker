"""Wait until the runner attaches this process to its cgroup, then exec frozen argv."""
import json
import os
from pathlib import Path
import sys
import time
root=Path(sys.argv[1])
deadline=time.time()+30
while not (root/"exec-go.json").exists():
    if time.time()>deadline:raise SystemExit(124)
    time.sleep(.01)
spec=json.loads((root/"spawn-intent.json").read_text())
os.chdir(spec["cwd"])
os.execvpe(spec["argv"][0],spec["argv"],spec["env"])
