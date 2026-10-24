"""A real resumable toy application: cursor + RNG state + accumulated output."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import time


def write(path, value):
    tmp = path.with_suffix(".tmp")
    with tmp.open("w") as f:
        json.dump(value, f); f.flush(); os.fsync(f.fileno())
    os.replace(tmp, path)


p = argparse.ArgumentParser()
p.add_argument("--attempt", required=True); p.add_argument("--resume")
a = p.parse_args(); root = Path(a.attempt)
attempt = json.loads((root/"attempt.json").read_text())
spec = json.loads((root/"business.json").read_text())
rng = random.Random(spec["parameters"].get("seed", 1701))
values = []; cursor = 0
if a.resume:
    checkpoint = json.loads(Path(a.resume).read_text())
    cursor = checkpoint["cursor"]; values = checkpoint["values"]
    rng.setstate((checkpoint["rng"][0], tuple(checkpoint["rng"][1]), checkpoint["rng"][2]))
for i in range(cursor, spec["parameters"]["steps"]):
    values.append(rng.randrange(1000000)); cursor = i+1
    write(root/"progress.json", {"cursor": cursor})
    time.sleep(spec["parameters"].get("delay", .01))
    pause = root/"control"/"pause.json"
    if pause.exists():
        request = json.loads(pause.read_text())
        cp = root/"checkpoints"/"state.json"
        write(cp, {"cursor": cursor, "values": values, "rng": rng.getstate(), "contract": "synthetic-v1"})
        write(root/"checkpoints"/"checkpoint_receipt.json", {"request_id": request["request_id"], "attempt_id": attempt["id"],
            "file": "state.json", "sha256": hashlib.sha256(cp.read_bytes()).hexdigest(), "cursor": cursor, "contract": "synthetic-v1"})
        raise SystemExit(0)
write(root/"outputs"/"result.json", {"values": values, "count": len(values), "sum": sum(values)})
