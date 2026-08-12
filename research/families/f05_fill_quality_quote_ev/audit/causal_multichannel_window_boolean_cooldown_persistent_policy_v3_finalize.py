"""Finalize paired economic inference for the persistent Boolean cooldown v3 OOF.

This command does not fit or refit a policy.  It binds the completed v3 outer
OOF artifact back to the exact modeled-queue label panel, reconstructs paired
policy contrasts, and publishes the preregistered day-level simultaneous
inference.  Any unidentified contrast remains explicit and blocks promotion.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_modeled_oof as modeled,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_persistent_policy_v3_inference as inference,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_persistent_policy_v3_oof as v3_oof,
)

IDENTITY = "causal_multichannel_window_boolean_cooldown_persistent_policy_v3"
SCHEMA_VERSION = f"{IDENTITY}.paired_inference.v1"
METHOD = "outer_train_tree_compiled_boolean_rule_policy"
CONTINUOUS_METHOD = "continuous_multioutput_decision_tree"
SEED = 20260812
BOOTSTRAP_DRAWS = 99_999
CONFIDENCE = 0.95


class PersistentPolicyV3FinalizeError(RuntimeError):
    """Raised when a bound v3 OOF artifact cannot be finalized."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        value = float(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(_canonical_json(_json_safe(payload)) + "\n", encoding="ascii")
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def _verify_oof_artifact(path: Path, expected_manifest_sha256: str) -> dict[str, Any]:
    root = path.expanduser().resolve()
    manifest_path = root / "manifest.json"
    success_path = root / "_SUCCESS"
    oof_path = root / "outer_oof.parquet"
    policies_path = root / "selected_policies.json"
    if not all(
        candidate.is_file()
        for candidate in (manifest_path, success_path, oof_path, policies_path)
    ):
        raise PersistentPolicyV3FinalizeError("v3 OOF artifact is incomplete")
    actual_manifest_sha256 = _sha256(manifest_path)
    if actual_manifest_sha256 != expected_manifest_sha256:
        raise PersistentPolicyV3FinalizeError("v3 OOF manifest SHA256 drifted")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("identity") != IDENTITY:
        raise PersistentPolicyV3FinalizeError("v3 OOF identity drifted")
    canonical_sha256 = str(manifest.get("canonical_sha256", ""))
    if success_path.read_text(encoding="ascii").strip() != canonical_sha256:
        raise PersistentPolicyV3FinalizeError("v3 OOF success marker drifted")
    expected_files = {
        str(item["relative_path"]): str(item["sha256"])
        for item in manifest.get("files", [])
    }
    for candidate in (oof_path, policies_path):
        if _sha256(candidate) != expected_files.get(candidate.name, ""):
            raise PersistentPolicyV3FinalizeError(
                f"v3 OOF file SHA256 drifted: {candidate.name}"
            )
    return {
        "root": str(root),
        "manifest_path": str(manifest_path),
        "manifest_sha256": actual_manifest_sha256,
        "canonical_sha256": canonical_sha256,
        "outer_oof_path": str(oof_path),
        "outer_oof_sha256": _sha256(oof_path),
        "selected_policies_path": str(policies_path),
        "selected_policies_sha256": _sha256(policies_path),
    }


def _verify_continuous_oof_artifact(
    path: Path, expected_manifest_sha256: str
) -> dict[str, Any]:
    root = path.expanduser().resolve()
    manifest_path = root / "manifest.json"
    success_path = root / "_SUCCESS"
    oof_path = root / "outer_oof.parquet"
    if not all(candidate.is_file() for candidate in (manifest_path, success_path, oof_path)):
        raise PersistentPolicyV3FinalizeError("continuous OOF artifact is incomplete")
    actual_manifest_sha256 = _sha256(manifest_path)
    if actual_manifest_sha256 != expected_manifest_sha256:
        raise PersistentPolicyV3FinalizeError("continuous OOF manifest SHA256 drifted")
    if success_path.read_text(encoding="ascii").strip() != actual_manifest_sha256:
        raise PersistentPolicyV3FinalizeError("continuous OOF success marker drifted")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("identity") != (
        "causal_multichannel_window_boolean_cooldown_duration_v2_owner_modeled_queue_v1"
    ):
        raise PersistentPolicyV3FinalizeError("continuous OOF identity drifted")
    expected_oof_sha256 = next(
        (
            str(item["sha256"])
            for item in manifest.get("files", [])
            if item.get("relative_path") == "outer_oof.parquet"
        ),
        "",
    )
    actual_oof_sha256 = _sha256(oof_path)
    if actual_oof_sha256 != expected_oof_sha256:
        raise PersistentPolicyV3FinalizeError("continuous outer OOF SHA256 drifted")
    return {
        "root": str(root),
        "manifest_path": str(manifest_path),
        "manifest_sha256": actual_manifest_sha256,
        "outer_oof_path": str(oof_path),
        "outer_oof_sha256": actual_oof_sha256,
        "method": CONTINUOUS_METHOD,
    }


def _policy(scope: str, side: str, block: str) -> inference.PolicyRef:
    return inference.PolicyRef(
        panel_scope=scope,
        side=side,
        feature_block=block,
        method=METHOD,
    )


def _contrast_specs(outer_oof: pd.DataFrame) -> list[tuple[str, inference.PolicyRef, inference.PolicyRef]]:
    scopes = {
        "prefix40_modeled_label_development": ("M0", "M1"),
        "prefix33_raw_m2_common_support": ("M0", "M1", "M2"),
    }
    specs: list[tuple[str, inference.PolicyRef, inference.PolicyRef]] = []
    present = set(
        outer_oof.loc[:, ["panel_scope", "side", "feature_block"]]
        .astype(str)
        .itertuples(index=False, name=None)
    )
    for scope, blocks in scopes.items():
        for side in ("BUY", "SELL"):
            for block in ("R0", *blocks):
                if (scope, side, block) not in present:
                    raise PersistentPolicyV3FinalizeError(
                        f"v3 OOF is missing {scope}/{side}/{block}"
                    )
            prefix = "prefix40" if scope.startswith("prefix40") else "prefix33"
            specs.append(
                (
                    f"{prefix}:{side}:R0-CONTROL",
                    _policy(scope, side, "R0"),
                    _policy(scope, side, inference.CONTROL_BLOCK),
                )
            )
            for block in blocks:
                specs.append(
                    (
                        f"{prefix}:{side}:{block}-CONTROL",
                        _policy(scope, side, block),
                        _policy(scope, side, inference.CONTROL_BLOCK),
                    )
                )
            for parent, child in zip(blocks, blocks[1:], strict=False):
                specs.append(
                    (
                        f"{prefix}:{side}:{child}-{parent}",
                        _policy(scope, side, child),
                        _policy(scope, side, parent),
                    )
                )
    return specs


def _action_summary(outer_oof: pd.DataFrame) -> dict[str, Any]:
    result: dict[str, Any] = {}
    groups = outer_oof.groupby(
        ["panel_scope", "side", "feature_block"], sort=True, observed=True
    )
    for (scope, side, block), rows in groups:
        key = f"{scope}/{side}/{block}"
        actions = rows["selected_action"].astype(str)
        result[key] = {
            "rows": int(len(rows)),
            "outer_test_days": int(rows["utc_day"].nunique()),
            "campaigns": int(rows["campaign_cluster_id"].nunique()),
            "nonbaseline_action_rate": float((actions != rows["control_action"].astype(str)).mean()),
            "point_identified_rate": float(rows["point_identified"].astype(bool).mean()),
            "selected_action_counts": {
                str(action): int(count)
                for action, count in actions.value_counts().sort_index().items()
            },
            "role_counts": {
                str(role): int(count)
                for role, count in rows["role_at_fill"].astype(str).value_counts().sort_index().items()
            },
        }
    return result


def _literal_names(policy: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                str(literal["predicate"])
                for rule in policy.get("ordered_first_match_rules", [])
                for clause in rule.get("clauses", [])
                for literal in clause.get("literals", [])
            }
        )
    )


def _policy_actions(policy: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(
        str(rule["action"])
        for rule in policy.get("ordered_first_match_rules", [])
    )


def _equal_day_campaign_weights(metadata: pd.DataFrame) -> np.ndarray:
    if metadata.empty:
        raise PersistentPolicyV3FinalizeError("distribution audit denominator is empty")
    rows = metadata.loc[:, ["utc_day", "campaign_cluster_id"]].copy()
    opportunities_per_campaign = rows.groupby(
        ["utc_day", "campaign_cluster_id"], observed=True
    )["campaign_cluster_id"].transform("size")
    campaigns_per_day = (
        rows.drop_duplicates()
        .groupby("utc_day", observed=True)["campaign_cluster_id"]
        .size()
    )
    day_count = int(rows["utc_day"].nunique())
    weights = (
        1.0
        / opportunities_per_campaign.to_numpy(dtype=float)
        / rows["utc_day"].map(campaigns_per_day).to_numpy(dtype=float)
        / day_count
    )
    if not math.isclose(float(weights.sum()), 1.0, abs_tol=1e-12):
        raise PersistentPolicyV3FinalizeError("distribution audit weights drifted")
    return weights


def _adjacent_jaccard(values: Sequence[set[str]]) -> list[float]:
    return [
        len(left & right) / len(left | right) if left | right else 1.0
        for left, right in zip(values, values[1:], strict=False)
    ]


def run_distribution_audit(
    *,
    outer_oof: pd.DataFrame,
    panel: modeled.PreparedPanel,
    config: modeled.FrozenConfig,
    selected_policies: Mapping[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    cells: dict[str, Any] = {}
    for cell_key, cell_payload in selected_policies.items():
        try:
            scope, side, block = str(cell_key).split("/")
        except ValueError as exc:
            raise PersistentPolicyV3FinalizeError(
                f"invalid selected-policy cell key: {cell_key}"
            ) from exc
        fold_reports = list(cell_payload.get("fold_reports", []))
        if len(fold_reports) != len(config.outer_folds[scope]):
            raise PersistentPolicyV3FinalizeError(
                f"selected-policy fold count drifted for {cell_key}"
            )
        fold_literal_sets: list[set[str]] = []
        fold_action_sets: list[set[str]] = []
        fold_summaries: list[dict[str, Any]] = []
        for fold_report in fold_reports:
            fold_id = str(fold_report["fold_id"])
            policy = dict(fold_report["outer_policy"])
            literals = _literal_names(policy)
            actions = _policy_actions(policy)
            fold_literal_sets.append(set(literals))
            fold_action_sets.append(set(actions))
            train_index, _ = modeled.observation_end_aware_purge(
                panel,
                side=side,
                train_days=tuple(fold_report["train_days"]),
                test_days=tuple(fold_report["test_days"]),
                fold_id=fold_id,
                stage="persistent_policy_v3.distribution_audit",
            )
            test_index = panel.metadata.index[
                (panel.metadata["side"] == side)
                & panel.metadata["utc_day"].isin(fold_report["test_days"])
            ]
            train_metadata = panel.metadata.loc[train_index]
            test_metadata = panel.metadata.loc[test_index]
            train_weights = _equal_day_campaign_weights(train_metadata)
            test_weights = _equal_day_campaign_weights(test_metadata)
            missing_literals = set(literals) - set(panel.features.columns)
            if missing_literals:
                raise PersistentPolicyV3FinalizeError(
                    f"policy literals absent from bound feature panel: {sorted(missing_literals)}"
                )
            for literal in literals:
                train_values = pd.to_numeric(
                    panel.features.loc[train_index, literal], errors="coerce"
                ).to_numpy(dtype=float)
                test_values = pd.to_numeric(
                    panel.features.loc[test_index, literal], errors="coerce"
                ).to_numpy(dtype=float)
                train_prevalence = inference.tri_state_prevalence(
                    train_values, train_weights
                )
                test_prevalence = inference.tri_state_prevalence(
                    test_values, test_weights
                )
                rows.append(
                    {
                        "cell": cell_key,
                        "panel_scope": scope,
                        "side": side,
                        "feature_block": block,
                        "fold_id": fold_id,
                        "predicate": literal,
                        "train_true_rate": train_prevalence.true_rate,
                        "test_true_rate": test_prevalence.true_rate,
                        "true_rate_delta": (
                            test_prevalence.true_rate - train_prevalence.true_rate
                        ),
                        "train_unobserved_rate": train_prevalence.unobserved_rate,
                        "test_unobserved_rate": test_prevalence.unobserved_rate,
                        "unobserved_rate_delta": (
                            test_prevalence.unobserved_rate
                            - train_prevalence.unobserved_rate
                        ),
                        "weighted_smd": inference.weighted_smd(
                            train_values,
                            test_values,
                            train_weights,
                            test_weights,
                        ),
                        "weighted_psi": inference.weighted_psi(
                            train_values,
                            test_values,
                            train_weights,
                            test_weights,
                            bins=3,
                        ),
                    }
                )
            fold_oof = outer_oof.loc[
                (outer_oof["panel_scope"] == scope)
                & (outer_oof["side"] == side)
                & (outer_oof["feature_block"] == block)
                & (outer_oof["fold_id"] == fold_id)
            ]
            fold_summaries.append(
                {
                    "fold_id": fold_id,
                    "train_days": int(train_metadata["utc_day"].nunique()),
                    "test_days": int(test_metadata["utc_day"].nunique()),
                    "train_opportunities": int(len(train_metadata)),
                    "test_opportunities": int(len(test_metadata)),
                    "train_add_rate": float(
                        (train_metadata["role_at_fill"].astype(str) == "add").mean()
                    ),
                    "test_add_rate": float(
                        (test_metadata["role_at_fill"].astype(str) == "add").mean()
                    ),
                    "selected_literal_count": len(literals),
                    "selected_actions": list(actions),
                    "outer_action_rate": float(
                        fold_oof["selected_nonbaseline"].astype(bool).mean()
                    ),
                    "outer_unidentified_rate": float(
                        1.0 - fold_oof["point_identified"].astype(bool).mean()
                    ),
                }
            )
        adjacent_literal_jaccard = _adjacent_jaccard(fold_literal_sets)
        adjacent_action_jaccard = _adjacent_jaccard(fold_action_sets)
        cells[cell_key] = {
            "folds": fold_summaries,
            "adjacent_literal_jaccard": adjacent_literal_jaccard,
            "adjacent_action_jaccard": adjacent_action_jaccard,
            "union_literal_count": len(set().union(*fold_literal_sets)),
            "common_literal_count": len(set.intersection(*fold_literal_sets)),
            "union_action_count": len(set().union(*fold_action_sets)),
            "common_action_count": len(set.intersection(*fold_action_sets)),
        }
    distribution_rows = pd.DataFrame.from_records(rows)
    finite_smd = distribution_rows["weighted_smd"].replace(
        [np.inf, -np.inf], np.nan
    )
    summary = {
        "cells": cells,
        "predicate_fold_rows": int(len(distribution_rows)),
        "maximum_absolute_weighted_smd": float(finite_smd.abs().max()),
        "maximum_weighted_psi": float(distribution_rows["weighted_psi"].max()),
        "maximum_absolute_true_rate_delta": float(
            distribution_rows["true_rate_delta"].abs().max()
        ),
        "maximum_absolute_unobserved_rate_delta": float(
            distribution_rows["unobserved_rate_delta"].abs().max()
        ),
    }
    return distribution_rows, summary


def run_inference(
    *,
    outer_oof: pd.DataFrame,
    panel: modeled.PreparedPanel,
    continuous_oof: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    required_methods = set(outer_oof["method"].astype(str).unique())
    if required_methods != {METHOD}:
        raise PersistentPolicyV3FinalizeError(
            f"unexpected v3 OOF methods: {sorted(required_methods)}"
        )
    all_contrasts: list[pd.DataFrame] = []
    day_values: dict[str, pd.DataFrame] = {}
    censoring: dict[str, inference.CensoringSensitivity] = {}
    campaign_sensitivity: dict[str, inference.WeightedEstimate] = {}
    for hypothesis, lhs, rhs in _contrast_specs(outer_oof):
        rows = inference.build_paired_policy_contrast(
            outer_oof,
            panel,
            lhs=lhs,
            rhs=rhs,
        )
        rows.insert(0, "hypothesis", hypothesis)
        all_contrasts.append(rows)
        day_values[hypothesis] = inference.equal_day_contributions(rows)
        censoring[hypothesis] = inference.censoring_tipping_bound(rows)
        campaign_sensitivity[hypothesis] = inference.campaign_weighted_sensitivity(rows)

    continuous_hypotheses: list[str] = []
    if continuous_oof is not None:
        continuous_rows = continuous_oof.loc[
            continuous_oof["method"].astype(str) == CONTINUOUS_METHOD
        ].copy()
        if continuous_rows.empty:
            raise PersistentPolicyV3FinalizeError("bound comparator has no continuous OOF rows")
        combined = pd.concat([outer_oof, continuous_rows], ignore_index=True)
        cells = (
            outer_oof.loc[:, ["panel_scope", "side", "feature_block"]]
            .drop_duplicates()
            .sort_values(["panel_scope", "side", "feature_block"], kind="stable")
        )
        for cell in cells.itertuples(index=False):
            prefix = "prefix40" if str(cell.panel_scope).startswith("prefix40") else "prefix33"
            hypothesis = (
                f"{prefix}:{cell.side}:{cell.feature_block}:CONTINUOUS-BOOLEAN"
            )
            rows = inference.build_paired_policy_contrast(
                combined,
                panel,
                lhs=inference.PolicyRef(
                    str(cell.panel_scope),
                    str(cell.side),
                    str(cell.feature_block),
                    CONTINUOUS_METHOD,
                ),
                rhs=_policy(
                    str(cell.panel_scope), str(cell.side), str(cell.feature_block)
                ),
            )
            rows.insert(0, "hypothesis", hypothesis)
            all_contrasts.append(rows)
            day_values[hypothesis] = inference.equal_day_contributions(rows)
            censoring[hypothesis] = inference.censoring_tipping_bound(rows)
            campaign_sensitivity[hypothesis] = (
                inference.campaign_weighted_sensitivity(rows)
            )
            continuous_hypotheses.append(hypothesis)

    simultaneous = inference.webb_wild_day_max_t(
        day_values,
        draws=BOOTSTRAP_DRAWS,
        seed=SEED,
        confidence=CONFIDENCE,
    )
    hierarchies = {
        "prefix40": {
            side: (
                f"prefix40:{side}:M0-CONTROL",
                f"prefix40:{side}:M1-CONTROL",
                f"prefix40:{side}:M1-M0",
            )
            for side in ("BUY", "SELL")
        },
        "prefix33": {
            side: (
                f"prefix33:{side}:M0-CONTROL",
                f"prefix33:{side}:M1-CONTROL",
                f"prefix33:{side}:M1-M0",
                f"prefix33:{side}:M2-CONTROL",
                f"prefix33:{side}:M2-M1",
            )
            for side in ("BUY", "SELL")
        },
    }
    hierarchy_results = {
        scope: inference.apply_feature_hierarchy(
            simultaneous,
            hierarchy,
            censoring=censoring,
        )
        for scope, hierarchy in hierarchies.items()
    }
    report = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "research_type": "outcome_informed_exploratory_successor",
        "queue_authority": "modeled_queue_not_strict_native",
        "primary_estimand": "identified_only_equal_UTC_day_of_campaign_equal_opportunity_policy_contrast_usdc",
        "inference": {
            "method": "shared_UTC_day_Webb_six_point_wild_max_t",
            "draws": simultaneous.draws,
            "seed": simultaneous.seed,
            "confidence": simultaneous.confidence,
            "critical_value": simultaneous.critical_value,
            "shared_days": list(simultaneous.shared_days),
            "studentization": simultaneous.studentization,
            "family": list(simultaneous.bands),
        },
        "hypotheses": {
            hypothesis: {
                "simultaneous_band": asdict(simultaneous[hypothesis]),
                "censoring": asdict(censoring[hypothesis]),
                "campaign_weighted_sensitivity": asdict(
                    campaign_sensitivity[hypothesis]
                ),
                "day_values": day_values[hypothesis].to_dict(orient="records"),
            }
            for hypothesis in simultaneous.bands
        },
        "continuous_minus_boolean_hypotheses": continuous_hypotheses,
        "hierarchies": {
            scope: asdict(decision) for scope, decision in hierarchy_results.items()
        },
        "action_support": _action_summary(outer_oof),
        "permissions": {
            "fresh_outer_evidence": False,
            "strict_queue_policy_eligible": False,
            "unified_policy_frozen": False,
            "repeated_policy_run": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    return pd.concat(all_contrasts, ignore_index=True), report


def _publish(
    output: Path,
    *,
    report: Mapping[str, Any],
    contrasts: pd.DataFrame,
    distribution_audit: pd.DataFrame,
    bindings: Mapping[str, Any],
) -> dict[str, Any]:
    destination = output.expanduser().resolve()
    if destination.exists():
        raise PersistentPolicyV3FinalizeError(f"output already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = destination.parent / f".{destination.name}.staging-{uuid.uuid4().hex}"
    staging.mkdir()
    try:
        _write_json(staging / "report.json", report)
        _write_json(staging / "bindings.json", bindings)
        contrasts.to_parquet(staging / "paired_contrasts.parquet", index=False)
        distribution_audit.to_parquet(
            staging / "distribution_audit.parquet", index=False
        )
        files = [
            {
                "relative_path": path.name,
                "bytes": path.stat().st_size,
                "sha256": _sha256(path),
            }
            for path in sorted(staging.iterdir())
        ]
        manifest_body = {
            "schema_version": f"{SCHEMA_VERSION}.manifest",
            "identity": IDENTITY,
            "files": files,
            "permissions": report["permissions"],
        }
        manifest = {
            **manifest_body,
            "canonical_sha256": hashlib.sha256(
                _canonical_json(manifest_body).encode("ascii")
            ).hexdigest(),
        }
        _write_json(staging / "manifest.json", manifest)
        (staging / "_SUCCESS").write_text(
            manifest["canonical_sha256"] + "\n", encoding="ascii"
        )
        with (staging / "_SUCCESS").open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(staging, destination)
        return {
            "output": str(destination),
            "manifest_sha256": _sha256(destination / "manifest.json"),
            "canonical_sha256": manifest["canonical_sha256"],
        }
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--config-sha256", required=True)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--spec-sha256", required=True)
    parser.add_argument("--feature-manifest", type=Path, required=True)
    parser.add_argument("--feature-manifest-sha256", required=True)
    parser.add_argument("--execution-amendment", type=Path, required=True)
    parser.add_argument("--execution-amendment-sha256", required=True)
    parser.add_argument("--feature-table-glob", action="append", default=None)
    parser.add_argument("--oof-root", type=Path, required=True)
    parser.add_argument("--oof-manifest-sha256", required=True)
    parser.add_argument("--continuous-oof-root", type=Path, required=True)
    parser.add_argument("--continuous-oof-manifest-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    globs = tuple(args.feature_table_glob or ("*.parquet", "**/*.parquet"))
    config = modeled.load_frozen_config(
        args.config,
        expected_sha256=args.config_sha256,
        spec_path=args.spec,
        expected_spec_sha256=args.spec_sha256,
        feature_manifest_path=args.feature_manifest,
        feature_manifest_sha256=args.feature_manifest_sha256,
        feature_table_globs=globs,
    )
    config, amendment = v3_oof.load_v3_execution_amendment(
        args.execution_amendment,
        expected_sha256=args.execution_amendment_sha256,
        config=config,
    )
    oof_binding = _verify_oof_artifact(args.oof_root, args.oof_manifest_sha256)
    continuous_binding = _verify_continuous_oof_artifact(
        args.continuous_oof_root,
        args.continuous_oof_manifest_sha256,
    )
    panel, panel_bindings = modeled.load_bound_panel(
        config,
        execution_amendment=amendment,
    )
    outer_oof = pd.read_parquet(oof_binding["outer_oof_path"])
    continuous_oof = pd.read_parquet(continuous_binding["outer_oof_path"])
    selected_policies = json.loads(
        Path(oof_binding["selected_policies_path"]).read_text(encoding="utf-8")
    )
    contrasts, report = run_inference(
        outer_oof=outer_oof,
        panel=panel,
        continuous_oof=continuous_oof,
    )
    distribution_rows, distribution_report = run_distribution_audit(
        outer_oof=outer_oof,
        panel=panel,
        config=config,
        selected_policies=selected_policies,
    )
    report = {
        **report,
        "config_sha256": config.sha256,
        "execution_amendment_sha256": amendment.sha256,
        "oof_manifest_sha256": oof_binding["manifest_sha256"],
        "continuous_oof_manifest_sha256": continuous_binding["manifest_sha256"],
        "distribution_audit": distribution_report,
    }
    bindings = {
        "panel": panel_bindings,
        "outer_oof": oof_binding,
        "continuous_outer_oof": continuous_binding,
        "code": {
            "finalizer": {"path": str(Path(__file__).resolve()), "sha256": _sha256(Path(__file__).resolve())},
            "inference": {
                "path": str(Path(inference.__file__).resolve()),
                "sha256": _sha256(Path(inference.__file__).resolve()),
            },
        },
    }
    result = _publish(
        args.output,
        report=report,
        contrasts=contrasts,
        distribution_audit=distribution_rows,
        bindings=bindings,
    )
    print(_canonical_json(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
