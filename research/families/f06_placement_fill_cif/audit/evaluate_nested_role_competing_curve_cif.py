#!/usr/bin/env python3
"""Evaluate v6 nested role-calibrated CIFs with the unchanged v4 gate."""

from __future__ import annotations

import argparse
import json
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from data_paths import data_root
from models.audit.experiment_manifest import git_workspace_identity
from research.families.f06_placement_fill_cif import FAMILY_DOCS
from research.families.f06_placement_fill_cif.audit.evaluate_competing_curve_cif import (
    evaluate as evaluate_v4_gate,
)
from research.families.f06_placement_fill_cif.audit.full_curve_fill_cif import ROOT, _sha256
from research.families.f06_placement_fill_cif.audit.placement_fill_spec import (
    load_placement_fill_spec,
)
from research.governance.paths import resolve_research_path

DEFAULT_REPORT = (
    data_root(ROOT)
    / "reports"
    / "placement_fill_nested_role_competing_cif_v6_development_20260727"
    / "report.json"
)
V4_SPEC = (
    FAMILY_DOCS / "placement_fill_full_curve_competing_cif_v4_spec_20260727.json"
)
V6_SPEC = (
    FAMILY_DOCS / "placement_fill_nested_role_competing_cif_v6_spec_20260727.json"
)


def _atomic_json(payload: Mapping[str, Any], path: Path) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def evaluate(
    report_path: Path,
    *,
    bootstrap_samples: int | None = None,
    bootstrap_seed: int | None = None,
) -> dict[str, Any]:
    """Apply the byte-identical v4 gate to the v6 OOF curve."""

    report = json.loads(report_path.read_text(encoding="utf-8"))
    v4_spec = load_placement_fill_spec(V4_SPEC)
    v6_spec = load_placement_fill_spec(V6_SPEC)
    if v6_spec["reporting"]["curve_level_gate"] != v4_spec["reporting"][
        "curve_level_gate"
    ]:
        raise RuntimeError("v6 attempted to change the frozen v4 curve gate")
    expected_base = str(v6_spec["lineage"]["base_evaluator_sha256"])
    base_path = resolve_research_path(str(v6_spec["lineage"]["base_evaluator"]))
    if _sha256(base_path) != expected_base:
        raise RuntimeError("v6 base curve evaluator identity changed")
    oof_identity = report["outputs"]["oof_nested_role_predictions"]
    compatibility = dict(report)
    compatibility["outputs"] = dict(report["outputs"])
    compatibility["outputs"]["oof_competing_predictions"] = oof_identity
    with tempfile.TemporaryDirectory(prefix="narrowgate_v6_curve_gate_") as directory:
        compatibility_path = Path(directory) / "report.json"
        compatibility_path.write_text(
            json.dumps(compatibility, sort_keys=True), encoding="utf-8"
        )
        result = evaluate_v4_gate(
            compatibility_path,
            bootstrap_samples=bootstrap_samples,
            bootstrap_seed=bootstrap_seed,
        )
    result.update(
        {
            "schema_version": "placement_nested_role_curve_evaluation.v1",
            "family_id": str(report["family_id"]),
            "spec": {"path": str(V6_SPEC), "sha256": _sha256(V6_SPEC)},
            "report": {
                "path": str(report_path),
                "sha256": _sha256(report_path),
            },
            "oof": oof_identity,
            "gate_inherited_without_change_from": {
                "path": str(V4_SPEC),
                "sha256": _sha256(V4_SPEC),
            },
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_or_live_authorization": False,
            "git": git_workspace_identity(ROOT),
        }
    )
    return result


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--bootstrap-samples", type=int)
    parser.add_argument("--bootstrap-seed", type=int)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    report = args.report.expanduser().resolve()
    output = (
        args.output.expanduser().resolve()
        if args.output is not None
        else report.parent / "curve_evaluation.json"
    )
    payload = evaluate(
        report,
        bootstrap_samples=args.bootstrap_samples,
        bootstrap_seed=args.bootstrap_seed,
    )
    _atomic_json(payload, output)
    print(
        json.dumps(
            {
                "family_id": payload["family_id"],
                "development_curve_gate_passed": payload[
                    "development_curve_gate_passed"
                ],
                "validation_read": False,
                "action_or_live_authorization": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
