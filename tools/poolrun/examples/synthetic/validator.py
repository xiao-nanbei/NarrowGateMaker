import json
from pathlib import Path
import random
import sys
r = json.load(sys.stdin)
spec = r["attempt"]["spec"]
rng = random.Random(spec["parameters"].get("seed",1701))
expected = [rng.randrange(1000000) for _ in range(spec["parameters"]["steps"])]
actual = json.loads((Path(r["outputs"])/"result.json").read_text())
print(json.dumps({"valid": actual["values"] == expected and actual["sum"]==sum(expected), "files": ["result.json"], "summary": {"sum": sum(expected)}}))
