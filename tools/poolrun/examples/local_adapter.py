"""LOCAL DEMO ONLY: filesystem stand-in for netdisk. Never represents a tested cloud route."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
r = json.load(sys.stdin)
if r["action"] == "fetch":
    shutil.copyfile(r["source"]["path"], r["destination"])
    print(json.dumps({"status": "READY"}))
elif r["action"] == "save_result":
    target = Path(sys.argv[1]) / r["attempt_id"]
    target.mkdir(parents=True, exist_ok=True)
    for obj in r["result"]["files"]:
        source = Path(r["directory"])/obj["file"]
        dest = target/obj["file"]
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, dest)
        with dest.open("rb") as f: os.fsync(f.fileno())
        if hashlib.sha256(dest.read_bytes()).hexdigest() != obj["sha256"]: raise ValueError("bad copy")
    fd = os.open(target, os.O_RDONLY); os.fsync(fd); os.close(fd)
    print(json.dumps({"durable": True, "uri": str(target), "files": r["result"]["files"]}))
