#!/usr/bin/env python3
"""Run Python/C++ CIF inference parity for the F07 v1.6 artifact."""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path

from research.families.f07_active_order_continuation.audit import (
    active_order_lifecycle_cif_cpp_parity_v1_5 as core,
)
from research.families.f07_active_order_continuation.audit import (
    order_lifecycle_v2_cif_successor_v1_6 as provenance,
)


@contextmanager
def _successor_contract() -> Iterator[None]:
    replacements = {
        "provenance": provenance,
        "TRAINING_IDENTITY": provenance.TRAINING_IDENTITY,
        "IDENTITY": provenance.PARITY_IDENTITY,
        "SCHEMA_VERSION": provenance.PARITY_SCHEMA_VERSION,
    }
    previous = {name: getattr(core, name) for name in replacements}
    for name, value in replacements.items():
        setattr(core, name, value)
    try:
        yield
    finally:
        for name, value in previous.items():
            setattr(core, name, value)


def run_parity(**kwargs: object) -> dict[str, object]:
    with _successor_contract():
        return core.run_parity(**kwargs)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--training-report", type=Path, required=True)
    parser.add_argument("--successor-amendment", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    report = run_parity(
        artifact_path=args.artifact,
        training_report_path=args.training_report,
        amendment_path=args.successor_amendment,
        output_path=args.out,
    )
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
