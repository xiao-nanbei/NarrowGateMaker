"""Audit frozen ordered-common-support v1 claims without mutating the identity."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd

from data_paths import data_root, relocate_marketdata_path
from research.families.f06_placement_fill_cif.audit.paired_lifecycle_contract import (
    PAIRED_ACTIONS,
    common_clock_diagnostics,
)

ROOT = Path(__file__).resolve().parents[4]
DEFAULT_SPEC = (
    ROOT
    / "research"
    / "families"
    / "f06_placement_fill_cif"
    / "docs"
    / "ordered_common_support_fill_surface_v1_spec_20260728.json"
)
DEFAULT_REPORT = (
    data_root(ROOT)
    / "reports"
    / "ordered_common_support_fill_surface_v1_development_20260728"
    / "report.json"
)
DEFAULT_OUTPUT = (
    ROOT
    / "research"
    / "families"
    / "f06_placement_fill_cif"
    / "docs"
    / "ordered_common_support_fill_surface_v1_contract_audit_20260728.json"
)
SCHEMA_VERSION = "ordered_common_support_contract_audit.v1"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _identity(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    return {
        "path": str(resolved),
        "sha256": _sha256(resolved),
        "size_bytes": int(resolved.stat().st_size),
    }


def _atomic_json(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    os.replace(temporary, path)


def _verify_identity(identity: dict[str, Any]) -> None:
    path = relocate_marketdata_path(identity["path"]).resolve()
    actual = _sha256(path)
    if actual != str(identity["sha256"]):
        raise RuntimeError(f"identity hash mismatch for {path}: {actual}")


def _violating_cohorts(report: dict[str, Any]) -> tuple[int, set[tuple[str, str]]]:
    violations = 0
    affected: set[tuple[str, str]] = set()
    for fold in report["folds"]:
        _verify_identity(fold["ordered_oof"])
        predictions = pd.read_parquet(
            fold["ordered_oof"]["path"],
            columns=[
                "day",
                "cohort_id",
                "action",
                "horizon_ms",
                "fill_probability",
            ],
        )
        for _, group in predictions.groupby("horizon_ms", observed=True):
            pivot = group.pivot(
                index=["day", "cohort_id"],
                columns="action",
                values="fill_probability",
            ).dropna(subset=list(PAIRED_ACTIONS))
            mask = (
                pivot["closer_1tick"] + 1e-12 < pivot["current"]
            ) | (pivot["current"] + 1e-12 < pivot["farther_1tick"])
            violations += int(mask.sum())
            affected.update((str(day), str(cohort)) for day, cohort in pivot.index[mask])
    return violations, affected


def _clock_audit(
    report: dict[str, Any], request_state_index: Path
) -> dict[str, Any]:
    violations, affected = _violating_cohorts(report)
    index = pd.read_csv(request_state_index, dtype={"day": str})
    parts: list[pd.DataFrame] = []
    affected_by_day: dict[str, set[str]] = {}
    for day, cohort in affected:
        affected_by_day.setdefault(day, set()).add(cohort)
    for row in index.itertuples(index=False):
        cohorts = affected_by_day.get(str(row.day))
        if not cohorts:
            continue
        payload = Path(str(row.payload_path)).expanduser().resolve()
        if _sha256(payload) != str(row.payload_sha256):
            raise RuntimeError(f"request-state payload hash mismatch: {payload}")
        frame = pd.read_parquet(
            payload,
            columns=["day", "cohort_id", "action", "pre_request_exposure_ms"],
        )
        parts.append(frame.loc[frame["cohort_id"].astype(str).isin(cohorts)])
    if not parts:
        raise RuntimeError("no affected lifecycle cohorts found in request-state cache")
    lifecycle = pd.concat(parts, ignore_index=True)
    diagnostics = common_clock_diagnostics(
        lifecycle,
        clock_column="pre_request_exposure_ms",
        group_columns=("day", "cohort_id"),
    )
    return {
        "apparent_violation_rows": int(violations),
        "report_violation_rows": int(
            sum(int(row["violations"]) for row in report["monotonicity_contract"])
        ),
        "unique_affected_cohorts": int(len(affected)),
        "affected_cache_cohorts_found": int(diagnostics["complete_groups"]),
        "all_affected_cohorts_use_unequal_realized_clocks": bool(
            diagnostics["violating_groups"] == len(affected)
        ),
        "realized_exposure_clock": diagnostics,
        "interpretation": (
            "The ordered hazard was integrated over action-specific realized "
            "exposure. This is an implementation-contract failure, not evidence "
            "that the LightGBM distance monotonicity constraint failed."
        ),
    }


def _downstream_artifacts_absent() -> dict[str, Any]:
    family_ids = (
        "placement_action_value_surface_v1",
        "placement_quote_action_uplift_v1",
    )
    report_root = data_root(ROOT) / "reports"
    found: dict[str, list[str]] = {}
    for family_id in family_ids:
        matches: list[str] = []
        for base in (ROOT, report_root):
            if not base.exists():
                continue
            matches.extend(
                str(path.resolve())
                for path in base.rglob(f"*{family_id}*")
            )
        found[family_id] = sorted(set(matches))
    return {
        "searched_family_ids": list(family_ids),
        "path_matches": found,
        "artifacts_absent": all(not matches for matches in found.values()),
    }


def run(spec_path: Path, report_path: Path, output_path: Path) -> dict[str, Any]:
    spec_path = spec_path.expanduser().resolve()
    report_path = report_path.expanduser().resolve()
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    report = json.loads(report_path.read_text(encoding="utf-8"))
    spec_identity = _identity(spec_path)
    report_identity = _identity(report_path)
    if report["spec"]["sha256"] != spec_identity["sha256"]:
        raise RuntimeError("Development report does not point to the frozen spec")
    implementation = (ROOT / spec["implementation"]["path"]).resolve()
    if _sha256(implementation) != spec["implementation"]["sha256"]:
        raise RuntimeError("frozen implementation hash no longer matches the spec")
    request_index = Path(
        spec["source_identity"]["request_state_index"]["path"]
    ).resolve()
    if _sha256(request_index) != spec["source_identity"]["request_state_index"]["sha256"]:
        raise RuntimeError("request-state index hash no longer matches the spec")

    activation = report["activation_transport"]
    pending = report["pending_nuisance_bound"]
    unconstrained = report["unconstrained_diagnostic"]
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "family_id": str(spec["family_id"]),
        "audit_date": "2026-07-28",
        "frozen_inputs": {
            "spec": spec_identity,
            "development_report": report_identity,
            "implementation": _identity(implementation),
            "request_state_index": _identity(request_index),
        },
        "audit_implementation": _identity(Path(__file__)),
        "hard_gate_reconciliation": {
            "action_curves_passed": int(
                sum(bool(row["curve_pass"]) for row in report["action_curves"])
            ),
            "action_curves_total": int(len(report["action_curves"])),
            "activation_calibration_passed": int(
                sum(bool(row["activation_calibration_pass"]) for row in activation)
            ),
            "activation_calibration_total": int(len(activation)),
            "support_calibration_passed": int(
                sum(bool(row["support_calibration_pass"]) for row in activation)
            ),
            "support_calibration_total": int(len(activation)),
            "transport_probability_bounds_passed": int(
                sum(bool(row["transport_bound_pass"]) for row in activation)
            ),
            "transport_probability_bounds_total": int(len(activation)),
            "pending_posterior_predictive_passed": int(
                sum(bool(row["posterior_predictive_pass"]) for row in pending)
            ),
            "pending_posterior_predictive_total": int(len(pending)),
            "pending_economic_bounds_passed": int(
                sum(bool(row["economic_bound_pass"]) for row in pending)
            ),
            "pending_economic_bounds_total": int(len(pending)),
            "independent_closure_reasons": [
                "common_lifecycle_clock_implementation_contract_failed",
                "activation_and_support_absolute_calibration_failed",
                "pending_fill_economic_uncertainty_bound_failed",
            ],
        },
        "clock_reaudit": _clock_audit(report, request_index),
        "unconstrained_comparison_erratum": {
            "rows_without_detected_significant_harm": int(
                sum(not bool(row["significantly_worse"]) for row in unconstrained)
            ),
            "rows_total": int(len(unconstrained)),
            "frozen_noninferiority_margin_present": False,
            "valid_claim": "no statistically detected harm under the frozen diagnostic",
            "invalid_claim": "the ordered model was proven non-inferior",
            "future_noninferiority_contract": (
                "UCB95(L_ordered - L_unconstrained) <= epsilon_L, with "
                "epsilon_L frozen before outcomes"
            ),
        },
        "training_weight_erratum": {
            "implementation_formula": "interval_exposure_seconds / 3",
            "correct_claim": (
                "counterfactual action replication factors sum to one while "
                "interval exposure weighting remains intact"
            ),
            "incorrect_literal_claim": "each cohort's numeric interval weights sum to one",
        },
        "test_coverage_erratum": {
            "existing_test_scope": "equal prediction clocks and monotone hazard output",
            "missing_in_frozen_v1": (
                "end-to-end assertion that the evaluator uses one cohort-common "
                "ex-ante scheduled request clock"
            ),
            "future_contract_helper": (
                "research.families.f06_placement_fill_cif.audit.paired_lifecycle_contract."
                "assert_common_prediction_clock"
            ),
        },
        "downstream_identity_audit": _downstream_artifacts_absent(),
        "permissions": {
            "validation_read": bool(report["validation_read"]),
            "sealed_holdout_read": bool(report["sealed_holdout_read"]),
            **report["gates"],
        },
        "decision": "erratum_recorded_family_remains_closed_on_development",
    }
    _atomic_json(payload, output_path.expanduser().resolve())
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run(args.spec, args.report, args.output)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
