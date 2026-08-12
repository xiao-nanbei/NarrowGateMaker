#!/usr/bin/env python3
"""CLI wrapper for the read-only legacy v12 cache semantic audit."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models.replay_cache_v12_semantic_audit import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
