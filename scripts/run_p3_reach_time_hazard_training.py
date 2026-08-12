#!/usr/bin/env python3
"""Run the frozen prediction-only F02 reach-time hazard audit."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import data_paths  # noqa: E402
from research.families.f02_empirical_p3_touch.audit import (  # noqa: E402
    p3_reach_time_hazard_training as training,
)

DEFAULT_OUTPUT_DIR = (
    data_paths.data_root(ROOT)
    / "model_runs"
    / "p3_aggressive_reach_time_conditioned_hazard_v1_20260804"
)
DEFAULT_SCRATCH_ROOT = data_paths.cache_root(ROOT) / "training_scratch" / training.IDENTITY


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, default=training.DEFAULT_SPEC_PATH)
    parser.add_argument(
        "--source-manifest",
        type=Path,
        default=training.DEFAULT_SOURCE_MANIFEST_PATH,
    )
    parser.add_argument(
        "--cache-summary",
        action="append",
        type=Path,
        required=True,
        help=(
            "Repeat for the weighted 200-day cache summary and the provider "
            "source-overlap cache summary."
        ),
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--scratch-root", type=Path, default=DEFAULT_SCRATCH_ROOT)
    parser.add_argument(
        "--max-expanded-rows",
        type=int,
        default=training.DEFAULT_MAX_EXPANDED_ROWS,
        help="Fail before fit when one side/split can exceed this risk-row cap.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    script_path = Path(__file__).resolve()
    training.run_hazard_training(
        cache_summary_paths=args.cache_summary,
        output_dir=args.output_dir,
        scratch_root=args.scratch_root,
        spec_path=args.spec,
        source_manifest_path=args.source_manifest,
        maximum_expanded_rows=args.max_expanded_rows,
        progress_stream=sys.stdout,
        invocation_identity={
            "path": str(script_path),
            "sha256": training.sha256_file(script_path),
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
