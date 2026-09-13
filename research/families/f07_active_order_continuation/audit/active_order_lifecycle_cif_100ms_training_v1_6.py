#!/usr/bin/env python3
"""Train the mechanics-only CIF from the homogeneous F07 v1.6 panel."""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path

from research.families.f07_active_order_continuation.audit import (
    active_order_lifecycle_cif_100ms_training_v1_5 as core,
)
from research.families.f07_active_order_continuation.audit import (
    order_lifecycle_v2_cif_successor_v1_6 as provenance,
)


@contextmanager
def _successor_contract() -> Iterator[None]:
    replacements = {
        "provenance": provenance,
        "IDENTITY": provenance.TRAINING_IDENTITY,
        "SCHEMA_VERSION": provenance.TRAINING_SCHEMA_VERSION,
        "REPORT_SCHEMA_VERSION": provenance.TRAINING_REPORT_SCHEMA_VERSION,
    }
    previous = {name: getattr(core, name) for name in replacements}
    for name, value in replacements.items():
        setattr(core, name, value)
    try:
        yield
    finally:
        for name, value in previous.items():
            setattr(core, name, value)


def train_panel(**kwargs: object) -> tuple[dict[str, object], dict[str, object]]:
    with _successor_contract():
        return core.train_panel(**kwargs)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--successor-amendment", type=Path, required=True)
    parser.add_argument("--lockstep-report", type=Path, required=True)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    _, report = train_panel(
        plan_path=args.plan,
        amendment_path=args.successor_amendment,
        lockstep_report_path=args.lockstep_report,
        artifact_path=args.artifact,
        report_path=args.report,
    )
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
