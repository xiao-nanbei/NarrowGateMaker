#!/usr/bin/env python3
"""Audit the exact scope of the admitted modeled-queue Boolean OOF result."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any

from data_paths import data_root

IDENTITY = (
    "causal_multichannel_window_boolean_cooldown_duration_v2_"
    "owner_modeled_queue_v1_interpretation_errata"
)
SCHEMA_VERSION = f"{IDENTITY}.v1"
OOF_IDENTITY = "causal_multichannel_window_boolean_cooldown_duration_v2_owner_modeled_queue_v1"

DEFAULT_OOF_ROOT = data_root(Path(__file__).resolve().parents[4]) / (
    "reports/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_20260810/"
    "owner_modeled_queue_nested_oof_v1"
)
DEFAULT_OWNER_SPEC = Path(
    "research/families/f05_fill_quality_quote_ev/docs/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_"
    "owner_modeled_queue_v1_spec_20260811.json"
)
DEFAULT_HIERARCHY_AMENDMENT = Path(
    "research/families/f05_fill_quality_quote_ev/docs/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_"
    "execution_amendment_v3_20260811.json"
)


class TruthAuditError(RuntimeError):
    """Raised when an admitted artifact does not match the audited identity."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("ascii")).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TruthAuditError(f"JSON root is not an object: {path}")
    return payload


def _boolean_cells(report: Mapping[str, Any]) -> Iterator[tuple[str, str, str, Mapping[str, Any]]]:
    results = report.get("results")
    if not isinstance(results, Mapping):
        raise TruthAuditError("OOF report lacks results")
    for panel, panel_payload in results.items():
        if not isinstance(panel_payload, Mapping):
            raise TruthAuditError("OOF panel payload is invalid")
        for side, side_payload in panel_payload.items():
            if not isinstance(side_payload, Mapping):
                raise TruthAuditError("OOF side payload is invalid")
            for block, cell in side_payload.items():
                boolean = cell.get("boolean") if isinstance(cell, Mapping) else None
                if not isinstance(boolean, Mapping):
                    raise TruthAuditError("OOF Boolean cell is invalid")
                yield str(panel), str(side), str(block), boolean


def _boolean_policies(selected: Mapping[str, Any]) -> Iterator[Mapping[str, Any]]:
    for panel_payload in selected.values():
        if not isinstance(panel_payload, Mapping):
            raise TruthAuditError("selected-candidate panel is invalid")
        for side_payload in panel_payload.values():
            if not isinstance(side_payload, Mapping):
                raise TruthAuditError("selected-candidate side is invalid")
            for cell in side_payload.values():
                policies = cell.get("boolean") if isinstance(cell, Mapping) else None
                if not isinstance(policies, list):
                    raise TruthAuditError("selected Boolean policy list is invalid")
                for policy in policies:
                    if not isinstance(policy, Mapping):
                        raise TruthAuditError("selected Boolean policy is invalid")
                    yield policy


def _policy_shape(policy: Mapping[str, Any]) -> str:
    rules = policy.get("ordered_first_match_rules")
    if not isinstance(rules, list) or not rules:
        raise TruthAuditError("policy has no ordered rules")
    if len(rules) > 1:
        return "multi_rule_ordered"
    clauses = rules[0].get("clauses") if isinstance(rules[0], Mapping) else None
    if not isinstance(clauses, list) or not clauses:
        raise TruthAuditError("policy rule has no clauses")
    literal_counts: list[int] = []
    for clause in clauses:
        literals = clause.get("literals") if isinstance(clause, Mapping) else None
        if not isinstance(literals, list) or not literals:
            raise TruthAuditError("policy clause has no literals")
        literal_counts.append(len(literals))
    if literal_counts == [1]:
        return "single_literal"
    if len(literal_counts) == 1 and literal_counts[0] == 2:
        return "two_literal_and"
    if literal_counts == [1, 1]:
        return "two_clause_or"
    return "other_bounded_dnf"


def summarize_policy_search(selected: Mapping[str, Any]) -> dict[str, Any]:
    policies = list(_boolean_policies(selected))
    shapes = Counter(_policy_shape(policy) for policy in policies)
    rule_counts = [len(policy["ordered_first_match_rules"]) for policy in policies]
    return {
        "outer_fold_policy_count": len(policies),
        "policy_shape_counts": dict(sorted(shapes.items())),
        "single_rule_policy_count": sum(count == 1 for count in rule_counts),
        "multi_rule_ordered_policy_count": sum(count > 1 for count in rule_counts),
        "large_feature_block_outcome_blind_predicate_budget": 32,
        "predicate_budget_semantics": (
            "hash-stable channel/semantic-stratified subset selected before outcome search"
        ),
        "full_predicate_universe_searched": False,
        "general_high_order_boolean_architecture_tested": False,
    }


def summarize_oof(report: Mapping[str, Any]) -> dict[str, Any]:
    outer_rows: list[dict[str, Any]] = []
    inner_means: list[float] = []
    m0_passed_sides: set[str] = set()
    for panel, side, block, boolean in _boolean_cells(report):
        partial = boolean.get("partial_identification")
        gate = boolean.get("deployment_gate")
        folds = boolean.get("folds")
        if not isinstance(partial, Mapping) or not isinstance(gate, Mapping) or not isinstance(
            folds, list
        ):
            raise TruthAuditError("OOF Boolean summary is incomplete")
        passed = gate.get("passed_for_owner_repeated_policy_successor") is True
        if block == "M0" and passed:
            m0_passed_sides.add(side)
        mean = float(partial["identified_mean_usdc"])
        lcb = float(partial["identified_lcb_usdc"])
        outer_rows.append(
            {
                "panel": panel,
                "side": side,
                "feature_block": block,
                "identified_mean_usdc": mean,
                "identified_lcb_usdc": lcb,
                "absolute_cell_gate_passed": passed,
            }
        )
        for fold in folds:
            inner = fold.get("inner_partial_identification") if isinstance(fold, Mapping) else None
            if not isinstance(inner, Mapping):
                raise TruthAuditError("OOF fold lacks inner partial-identification summary")
            inner_means.append(float(inner["identified_mean_usdc"]))

    denominators = report.get("panel_denominators")
    if not isinstance(denominators, Mapping):
        raise TruthAuditError("OOF report lacks panel denominators")
    test_days = {
        str(panel): {
            str(side): len(side_row["outer_oof_test_days"])
            for side, side_row in panel_row["sides"].items()
        }
        for panel, panel_row in denominators.items()
    }
    return {
        "absolute_boolean_cells": len(outer_rows),
        "absolute_boolean_cells_passed": sum(
            row["absolute_cell_gate_passed"] for row in outer_rows
        ),
        "outer_positive_point_estimate_cells": sum(
            row["identified_mean_usdc"] > 0.0 for row in outer_rows
        ),
        "outer_negative_point_estimate_cells": sum(
            row["identified_mean_usdc"] < 0.0 for row in outer_rows
        ),
        "inner_selected_fold_count": len(inner_means),
        "inner_positive_point_estimate_folds": sum(value > 0.0 for value in inner_means),
        "inner_selected_mean_min_usdc": min(inner_means),
        "inner_selected_mean_max_usdc": max(inner_means),
        "M0_absolute_passed_sides": sorted(m0_passed_sides),
        "supported_sides_under_frozen_M0_first_hierarchy": sorted(m0_passed_sides),
        "outer_oof_test_day_counts": test_days,
        "cells": outer_rows,
    }


def build_truth_audit(
    *,
    report: Mapping[str, Any],
    selected: Mapping[str, Any],
    owner_spec: Mapping[str, Any],
    hierarchy_amendment: Mapping[str, Any],
    weighting_sensitivity: Mapping[str, Any] | None = None,
    bindings: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if report.get("identity") != OOF_IDENTITY or owner_spec.get("identity") != OOF_IDENTITY:
        raise TruthAuditError("owner modeled-queue identity drifted")
    source = owner_spec.get("modeled_label_source")
    if not isinstance(source, Mapping):
        raise TruthAuditError("owner spec modeled-label source is missing")
    census = report.get("modeled_label_census")
    if not isinstance(census, Mapping):
        raise TruthAuditError("OOF report modeled-label census is missing")
    opportunities = int(source["opportunity_rows"])
    executed_arms = int(source["arm_rows"])
    arms_per_opportunity = int(source["arm_count_per_opportunity"])
    if executed_arms != opportunities * arms_per_opportunity:
        raise TruthAuditError("owner spec executed-arm census is inconsistent")
    dense_slots = int(census["arm_rows"])

    frozen_gate = hierarchy_amendment.get("post_outer_oof_gate_contract")
    family = frozen_gate.get("feature_family_selection") if isinstance(frozen_gate, Mapping) else None
    if not isinstance(family, Mapping):
        raise TruthAuditError("frozen feature-family hierarchy is missing")

    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "audited_identity": OOF_IDENTITY,
        "status": "interpretation_corrected_existing_admitted_oof_only",
        "evidence_separation": {
            "strict_native_historical_path": (
                "blocked_before_formal_economic_panel_on_non_identifiable_same_millisecond_ordering"
            ),
            "owner_modeled_queue_path": (
                "limited_one_shot_nested_oof_completed_without_exchange_queue_authority"
            ),
            "paths_may_be_combined_into_one_closure_claim": False,
        },
        "label_census": {
            "opportunities": opportunities,
            "executed_arm_rows": executed_arms,
            "arms_per_opportunity": arms_per_opportunity,
            "historical_report_dense_side_action_slots": dense_slots,
            "dense_slots_are_executed_arms": False,
            "historical_arm_rows_field_is_misnamed": dense_slots != executed_arms,
        },
        "policy_search": summarize_policy_search(selected),
        "oof": summarize_oof(report),
        "feature_family_gate": {
            "frozen_hierarchy": list(family["hierarchy"]),
            "frozen_multiple_comparison_control": family["multiple_comparison_control"],
            "implemented_report_gate": "14 independent absolute block cells versus CONTROL",
            "frozen_hierarchy_fully_implemented": False,
            "paired_M1_minus_M0_available": False,
            "paired_M2_minus_M1_available": False,
            "continuous_minus_boolean_available": False,
            "interpretation": (
                "M0 absolute failed for BUY and SELL, so supported_sides remains empty; "
                "M1, M2, and continuous incremental claims are not identified by this result"
            ),
        },
        "statistical_scope": {
            "interval": "campaign-equal weighted UTC-day cluster sandwich normal_1.96",
            "cluster_bootstrap": False,
            "small_cluster_t_correction": False,
            "fourteen_cell_simultaneous_correction": False,
            "future_single_cell_pass_confirmatory_without_successor_inference": False,
        },
        "weighting_sensitivity": dict(weighting_sensitivity or {}),
        "current_conclusion": (
            "strict-native labels blocked; limited modeled-queue one-shot search did not pass OOF"
        ),
        "claims_not_supported": [
            "strict-native no-policy closure",
            "M1 adds no value over M0",
            "M2 adds no value over M1",
            "continuous state adds no value over Boolean state",
            "all multichannel high-order Boolean cooldown policies fail",
            "repeated-policy full-path PnL is negative",
            "85 seconds is optimal",
        ],
        "permissions": {
            "validation_read": False,
            "sealed_holdout_read": False,
            "unified_policy_frozen": False,
            "repeated_policy_run": False,
            "action_authorized": False,
            "live_authorized": False,
        },
        "bindings": dict(bindings or {}),
    }
    payload["canonical_sha256"] = _canonical_sha256(payload)
    return payload


def equal_day_weighting_sensitivity(path: Path) -> dict[str, Any]:
    """Compare campaign-weighted and equal-day M2 point estimates."""

    import numpy as np
    import pandas as pd

    frame = pd.read_parquet(
        path,
        columns=(
            "utc_day",
            "side",
            "method",
            "feature_block",
            "panel_scope",
            "point_identified",
            "uplift_usdc",
            "campaign_weight",
        ),
    )
    frame = frame.loc[
        (frame["method"] == "bounded_sparse_boolean_dnf")
        & (frame["panel_scope"] == "prefix33_raw_m2_common_support")
        & (frame["feature_block"] == "M2")
        & frame["point_identified"].astype(bool)
    ]
    output: dict[str, Any] = {}
    for side in ("BUY", "SELL"):
        side_frame = frame.loc[frame["side"] == side]
        if side_frame.empty:
            raise TruthAuditError(f"equal-day sensitivity lacks {side} M2 rows")
        campaign_weighted = float(
            np.average(side_frame["uplift_usdc"], weights=side_frame["campaign_weight"])
        )
        daily_values = []
        for _, day_frame in side_frame.groupby("utc_day", sort=True):
            daily_values.append(
                float(
                    np.average(
                        day_frame["uplift_usdc"], weights=day_frame["campaign_weight"]
                    )
                )
            )
        equal_day = float(np.mean(daily_values))
        output[side] = {
            "campaign_weighted_mean_usdc": campaign_weighted,
            "equal_day_mean_usdc": equal_day,
            "test_days": len(daily_values),
            "point_estimate_sign_flips": (campaign_weighted > 0) != (equal_day > 0),
        }
    return {
        "panel": "prefix33_raw_m2_common_support",
        "feature_block": "M2",
        "method": "bounded_sparse_boolean_dnf",
        "result": output,
        "interpretation": (
            "point-estimate sign depends on day weighting; lower-bound no-pass is unchanged"
        ),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--oof-root", type=Path, default=DEFAULT_OOF_ROOT)
    parser.add_argument("--owner-spec", type=Path, default=DEFAULT_OWNER_SPEC)
    parser.add_argument(
        "--hierarchy-amendment", type=Path, default=DEFAULT_HIERARCHY_AMENDMENT
    )
    parser.add_argument("--output", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    oof_root = args.oof_root.expanduser().resolve()
    report_path = oof_root / "report.json"
    selected_path = oof_root / "selected_candidates.json"
    outer_oof_path = oof_root / "outer_oof.parquet"
    spec_path = args.owner_spec.expanduser().resolve()
    hierarchy_path = args.hierarchy_amendment.expanduser().resolve()
    payload = build_truth_audit(
        report=_load_json(report_path),
        selected=_load_json(selected_path),
        owner_spec=_load_json(spec_path),
        hierarchy_amendment=_load_json(hierarchy_path),
        weighting_sensitivity=equal_day_weighting_sensitivity(outer_oof_path),
        bindings={
            "report_path": str(report_path),
            "report_sha256": _file_sha256(report_path),
            "selected_candidates_path": str(selected_path),
            "selected_candidates_sha256": _file_sha256(selected_path),
            "outer_oof_path": str(outer_oof_path),
            "outer_oof_sha256": _file_sha256(outer_oof_path),
            "owner_spec_path": str(spec_path),
            "owner_spec_sha256": _file_sha256(spec_path),
            "hierarchy_amendment_path": str(hierarchy_path),
            "hierarchy_amendment_sha256": _file_sha256(hierarchy_path),
        },
    )
    encoded = _canonical_json(payload) + "\n"
    if args.output is None:
        print(encoded, end="")
    else:
        output = args.output.expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(encoded, encoding="ascii")
        print(str(output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "IDENTITY",
    "TruthAuditError",
    "build_truth_audit",
    "equal_day_weighting_sensitivity",
    "summarize_oof",
    "summarize_policy_search",
]
