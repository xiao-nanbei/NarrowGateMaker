import hashlib
import json
from pathlib import Path
import sys
r = json.load(sys.stdin)
c = r["checkpoint"]
target = r["target_release"]
try:
    data = Path(c["path"]).read_bytes()
    state = json.loads(data)
    valid = (hashlib.sha256(data).hexdigest() == c["sha256"] and c["spec_hash"] == r["spec_hash"]
        and target["checkpoint_contract"] == "synthetic-v1" == state["contract"]
        and len(state["values"]) == state["cursor"] and state["cursor"] <= r["business"]["parameters"]["steps"])
    print(json.dumps({"decision": "DIRECT" if valid else "INCOMPATIBLE", "resume_argv": ["--resume", c["path"]] if valid else []}))
except Exception:
    print(json.dumps({"decision": "UNKNOWN"}))
