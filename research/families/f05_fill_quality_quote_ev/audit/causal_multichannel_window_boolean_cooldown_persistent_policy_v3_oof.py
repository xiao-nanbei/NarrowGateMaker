"""Outcome-informed multichannel Boolean cooldown successor.

This module does not repair or overwrite the historical v2 result.  It reuses
the admitted owner-modelled one-shot labels, fits policies only on chronological
training rows, compiles shallow multi-output trees into observed-state guarded
AND/OR/NOT policies, and executes those policies once on untouched outer rows.

The resulting evidence is owner-route exploratory evidence only.  It has no
strict queue, action, repeated-policy, or live authority.
"""

from __future__ import annotations

import argparse
import hashlib
import inspect
import json
import math
import os
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.tree import DecisionTreeRegressor

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_modeled_oof as modeled,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_nested_oof import (
    CONTROL_ACTION,
    AndClause,
    BooleanCooldownPolicy,
    BooleanRule,
    TriLiteral,
    _infer_channel_group,
    _infer_semantic_group,
    duration_vocabulary,
)

IDENTITY = "causal_multichannel_window_boolean_cooldown_persistent_policy_v3"
METHOD = "outer_train_tree_compiled_boolean_rule_policy"
SCHEMA_VERSION = f"{IDENTITY}.owner_modeled_queue_oof.v1"
OWNER_ROUTE = "owner_risk_accepted_modelled_queue_exploration"
DEFAULT_RANDOM_SEED = 20260812
EXECUTION_AMENDMENT_SCHEMA = f"{IDENTITY}.owner_modeled_queue_execution.v1"
DEFAULT_DESIGN_PATH = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "causal_multichannel_window_boolean_cooldown_persistent_policy_v3_design_20260812.json"
)


class PersistentPolicyV3Error(RuntimeError):
    """Raised when the successor execution contract is violated."""


@dataclass(frozen=True, slots=True)
class ComplexityProfile:
    name: str
    feature_budget: int
    max_depth: int
    max_leaf_nodes: int
    min_samples_leaf: int

    def __post_init__(self) -> None:
        values = (
            self.feature_budget,
            self.max_depth,
            self.max_leaf_nodes,
            self.min_samples_leaf,
        )
        if any(value <= 0 for value in values):
            raise PersistentPolicyV3Error("complexity profile values must be positive")


DEFAULT_PROFILES = (
    ComplexityProfile("small", 64, 2, 4, 50),
    ComplexityProfile("medium", 128, 3, 8, 30),
    ComplexityProfile("large", 256, 4, 16, 20),
)


@dataclass(frozen=True, slots=True)
class TreeFitAudit:
    side: str
    feature_block: str
    profile: str
    training_rows: int
    training_days: int
    training_campaigns: int
    selected_feature_count: int
    nonbaseline_leaf_count: int
    compiled_rule_count: int
    compiled_clause_count: int
    compiled_literal_count: int
    neutral_training_targets: int
    training_action_rate: float
    candidate_id: str


@dataclass(frozen=True, slots=True)
class CellExecution:
    panel_scope: str
    side: str
    feature_block: str
    oof_rows: pd.DataFrame
    fold_reports: tuple[Mapping[str, Any], ...]
    selected_policies: tuple[Mapping[str, Any], ...]
    purge_audits: tuple[Mapping[str, Any], ...]


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_v3_execution_amendment(
    path: Path,
    *,
    expected_sha256: str,
    config: modeled.FrozenConfig,
) -> tuple[modeled.FrozenConfig, modeled.ExecutionAmendmentBinding]:
    amendment_path = Path(path).expanduser().resolve()
    if len(expected_sha256) != 64 or _sha256(amendment_path) != expected_sha256:
        raise PersistentPolicyV3Error("v3 execution amendment SHA256 mismatch")
    payload = json.loads(amendment_path.read_text(encoding="utf-8"))
    if (
        payload.get("schema_version") != EXECUTION_AMENDMENT_SCHEMA
        or payload.get("identity") != IDENTITY
        or payload.get("status") != "frozen_before_v3_refit_on_consumed_development"
    ):
        raise PersistentPolicyV3Error("v3 execution amendment identity/schema/status drifted")
    permissions = payload.get("permissions")
    required_false = (
        "validation_read",
        "sealed_holdout_read",
        "unified_policy_frozen",
        "repeated_policy_run",
        "action_authorized",
        "live_authorized",
    )
    if not isinstance(permissions, Mapping) or any(
        permissions.get(name) is not False for name in required_false
    ):
        raise PersistentPolicyV3Error("v3 execution permissions drifted")
    expected_artifacts = {
        "predecessor_config": (config.path, config.sha256),
        "predecessor_owner_spec": (config.spec_path, config.spec_sha256),
        "modeled_label_manifest": (
            config.labels.manifest_path,
            config.labels.manifest_sha256,
        ),
        "feature_panel_manifest": (
            config.features.manifest_path,
            config.features.manifest_sha256,
        ),
        "v3_design_contract": (
            DEFAULT_DESIGN_PATH,
            _sha256(DEFAULT_DESIGN_PATH),
        ),
    }
    artifacts = payload.get("artifact_bindings")
    if not isinstance(artifacts, Mapping):
        raise PersistentPolicyV3Error("v3 artifact bindings are missing")
    normalized_artifacts: dict[str, Mapping[str, Any]] = {}
    for name, (expected_path, expected_hash) in expected_artifacts.items():
        row = artifacts.get(name)
        if not isinstance(row, Mapping):
            raise PersistentPolicyV3Error(f"v3 artifact binding is missing: {name}")
        observed_path = Path(str(row.get("path", ""))).expanduser().resolve()
        observed_hash = str(row.get("sha256", ""))
        if observed_path != expected_path.resolve() or observed_hash != expected_hash:
            raise PersistentPolicyV3Error(f"v3 artifact binding drifted: {name}")
        if not observed_path.is_file() or _sha256(observed_path) != observed_hash:
            raise PersistentPolicyV3Error(f"v3 artifact bytes drifted: {name}")
        normalized_artifacts[name] = {
            "path": str(observed_path),
            "sha256": observed_hash,
        }
    code_rows = payload.get("code_bindings")
    if not isinstance(code_rows, list) or not code_rows:
        raise PersistentPolicyV3Error("v3 code bindings are missing")
    code_bindings: list[tuple[Path, str]] = []
    for row in code_rows:
        if not isinstance(row, Mapping):
            raise PersistentPolicyV3Error("v3 code binding is invalid")
        code_path = Path(str(row.get("path", ""))).expanduser().resolve()
        code_hash = str(row.get("sha256", ""))
        if not code_path.is_file() or len(code_hash) != 64 or _sha256(code_path) != code_hash:
            raise PersistentPolicyV3Error(f"v3 code binding drifted: {code_path}")
        code_bindings.append((code_path, code_hash))
    required_code = {Path(__file__).resolve(), Path(modeled.__file__).resolve()}
    policy_source = inspect.getsourcefile(BooleanCooldownPolicy)
    if policy_source is None:
        raise PersistentPolicyV3Error("cannot resolve Boolean policy source")
    required_code.add(Path(policy_source).resolve())
    if required_code - {path for path, _ in code_bindings}:
        raise PersistentPolicyV3Error("v3 execution amendment omits required code")
    libraries = payload.get("library_versions")
    observed_libraries = modeled.runtime_library_versions()
    if not isinstance(libraries, Mapping) or dict(libraries) != observed_libraries:
        raise PersistentPolicyV3Error("v3 library bindings drifted")
    identity = str(payload.get("execution_identity_sha256", ""))
    body = dict(payload)
    body.pop("execution_identity_sha256", None)
    if identity != _canonical_sha256(body):
        raise PersistentPolicyV3Error("v3 execution identity drifted")
    binding = modeled.ExecutionAmendmentBinding(
        path=amendment_path,
        sha256=expected_sha256,
        execution_identity_sha256=identity,
        artifact_bindings=normalized_artifacts,
        code_bindings=tuple(code_bindings),
        library_versions=observed_libraries,
    )
    return (
        replace(
            config,
            code_bindings=binding.code_bindings,
            expected_library_versions=binding.library_versions,
        ),
        binding,
    )


def _campaign_weights(metadata: pd.DataFrame) -> np.ndarray:
    counts = metadata.groupby("campaign_cluster_id", observed=True)["utc_day"].transform("count")
    weights = (1.0 / counts.astype(float)).to_numpy(dtype=float)
    totals = (
        pd.Series(weights, index=metadata.index)
        .groupby(metadata["campaign_cluster_id"], observed=True)
        .sum()
    )
    if not np.allclose(totals.to_numpy(dtype=float), 1.0, atol=1e-12, rtol=0.0):
        raise PersistentPolicyV3Error("campaign training weights do not sum to one")
    return weights


def _quality_ranked_predicates(
    panel: modeled.PreparedPanel,
    *,
    train_index: pd.Index,
    candidates: Sequence[str],
    limit: int,
) -> tuple[str, ...]:
    """Select a feature-only, channel-stratified outer-train predicate pool."""

    if limit <= 0:
        raise PersistentPolicyV3Error("predicate pool limit must be positive")
    rows: list[tuple[str, str, str, float, str]] = []
    names = tuple(candidates)
    for start in range(0, len(names), 256):
        chunk = names[start : start + 256]
        matrix = panel.features.loc[train_index, list(chunk)].to_numpy(dtype=np.int8)
        observed = matrix != -1
        observed_count = observed.sum(axis=0)
        true_count = (matrix == 1).sum(axis=0)
        false_count = (matrix == 0).sum(axis=0)
        denominator = max(1, len(train_index))
        quality = (
            observed_count.astype(float)
            / denominator
            * np.minimum(true_count, false_count)
            / np.maximum(observed_count, 1)
        )
        for index, name in enumerate(chunk):
            score = float(quality[index])
            if not math.isfinite(score) or score <= 0.0:
                continue
            rows.append(
                (
                    _infer_channel_group(name),
                    _infer_semantic_group(name),
                    name,
                    score,
                    _canonical_sha256(["predicate", name]),
                )
            )
    if not rows:
        raise PersistentPolicyV3Error("outer-train predicate pool is empty")
    grouped: dict[tuple[str, str], list[tuple[str, float, str]]] = {}
    for channel, semantic, name, quality, stable in rows:
        grouped.setdefault((channel, semantic), []).append((name, quality, stable))
    for values in grouped.values():
        values.sort(key=lambda row: (-row[1], row[2]))
    strata = sorted(grouped, key=lambda value: _canonical_sha256(["stratum", *value]))
    selected: list[str] = []
    while len(selected) < min(limit, len(rows)):
        progressed = False
        for stratum in strata:
            bucket = grouped[stratum]
            if not bucket:
                continue
            selected.append(bucket.pop(0)[0])
            progressed = True
            if len(selected) >= limit:
                break
        if not progressed:
            break
    return tuple(selected)


def _nested_feature_pool(
    panel: modeled.PreparedPanel,
    *,
    config: modeled.FrozenConfig,
    train_index: pd.Index,
    feature_block: str,
    feature_budget: int,
) -> tuple[str, ...]:
    """Build cumulative M0/M1/M2 pools without using economic outcomes."""

    block_names = {
        name: tuple(config.feature_blocks[name].boolean_predicates)
        for name in ("M0", "M1", "M2", "R0")
    }
    if feature_block == "R0":
        return _quality_ranked_predicates(
            panel,
            train_index=train_index,
            candidates=block_names["R0"],
            limit=min(feature_budget, len(block_names["R0"])),
        )
    m0 = tuple(block_names["M0"])
    if feature_block == "M0":
        return _quality_ranked_predicates(
            panel,
            train_index=train_index,
            candidates=m0,
            limit=len(m0),
        )
    m1_extra = tuple(sorted(set(block_names["M1"]) - set(m0)))
    if feature_block == "M1":
        base = _quality_ranked_predicates(
            panel,
            train_index=train_index,
            candidates=m0,
            limit=len(m0),
        )
        room = max(0, feature_budget - len(base))
        extra = _quality_ranked_predicates(
            panel,
            train_index=train_index,
            candidates=m1_extra,
            limit=min(room, len(m1_extra)),
        )
        return tuple(dict.fromkeys((*base, *extra)))
    if feature_block != "M2":
        raise PersistentPolicyV3Error(f"unsupported feature block: {feature_block}")
    m2_extra = tuple(sorted(set(block_names["M2"]) - set(block_names["M1"])))
    base = _quality_ranked_predicates(
        panel,
        train_index=train_index,
        candidates=m0,
        limit=len(m0),
    )
    room = max(0, feature_budget - len(base))
    m1_room = min(len(m1_extra), room // 2)
    m2_room = min(len(m2_extra), room - m1_room)
    m1_selected = _quality_ranked_predicates(
        panel,
        train_index=train_index,
        candidates=m1_extra,
        limit=m1_room,
    )
    m2_selected = _quality_ranked_predicates(
        panel,
        train_index=train_index,
        candidates=m2_extra,
        limit=m2_room,
    )
    interleaved: list[str] = []
    for index in range(max(len(m1_selected), len(m2_selected))):
        if index < len(m1_selected):
            interleaved.append(m1_selected[index])
        if index < len(m2_selected):
            interleaved.append(m2_selected[index])
    return tuple(dict.fromkeys((*base, *interleaved)))


def _leaf_constraints(
    model: DecisionTreeRegressor,
    feature_names: Sequence[str],
) -> list[tuple[int, dict[str, tuple[float, float]]]]:
    tree = model.tree_
    leaves: list[tuple[int, dict[str, tuple[float, float]]]] = []

    def walk(node: int, constraints: dict[str, tuple[float, float]]) -> None:
        left = int(tree.children_left[node])
        right = int(tree.children_right[node])
        if left == right:
            leaves.append((node, dict(constraints)))
            return
        feature = feature_names[int(tree.feature[node])]
        threshold = float(tree.threshold[node])
        lower, upper = constraints.get(feature, (-math.inf, math.inf))
        left_constraints = dict(constraints)
        left_constraints[feature] = (lower, min(upper, threshold))
        right_constraints = dict(constraints)
        right_constraints[feature] = (max(lower, threshold), upper)
        walk(left, left_constraints)
        walk(right, right_constraints)

    walk(0, {})
    return leaves


def _constraint_clause(
    constraints: Mapping[str, tuple[float, float]],
) -> AndClause | None:
    literals: list[TriLiteral] = []
    for name, (lower, upper) in constraints.items():
        allowed = [state for state in (0, 1) if state > lower and state <= upper]
        if not allowed:
            return None
        if allowed == [0]:
            literals.append(TriLiteral(name, True))
        elif allowed == [1]:
            literals.append(TriLiteral(name, False))
        elif allowed != [0, 1]:
            raise PersistentPolicyV3Error("tree split produced an invalid tri-state interval")
    if not literals:
        return None
    return AndClause(tuple(sorted(literals)))


def _compile_tree_policy(
    model: DecisionTreeRegressor,
    *,
    side: str,
    feature_names: Sequence[str],
    action_scales: np.ndarray,
) -> tuple[BooleanCooldownPolicy, int]:
    actions = duration_vocabulary(side)[1:]
    by_action: dict[str, list[AndClause]] = {action: [] for action in actions}
    ranked_fallback: list[tuple[float, str, AndClause]] = []
    positive_leaf_count = 0
    values = np.asarray(model.tree_.value, dtype=float)
    for node, constraints in _leaf_constraints(model, feature_names):
        clause = _constraint_clause(constraints)
        if clause is None:
            continue
        raw = values[node]
        prediction = raw[:, 0] if raw.ndim == 2 else raw.reshape(-1)
        prediction = prediction * action_scales
        action_index = int(np.argmax(prediction))
        action = actions[action_index]
        score = float(prediction[action_index])
        ranked_fallback.append((score, action, clause))
        if score > 0.0:
            by_action[action].append(clause)
            positive_leaf_count += 1
    if not any(by_action.values()):
        if not ranked_fallback:
            raise PersistentPolicyV3Error("tree produced no compilable observed-state leaf")
        _, action, clause = max(
            ranked_fallback,
            key=lambda row: (row[0], row[1], row[2].key),
        )
        by_action[action].append(clause)
    action_scores: list[tuple[float, str]] = []
    for action, clauses in by_action.items():
        if not clauses:
            continue
        index = actions.index(action)
        matching_scores = []
        for node, constraints in _leaf_constraints(model, feature_names):
            clause = _constraint_clause(constraints)
            if clause is not None and clause in clauses:
                raw = values[node]
                prediction = raw[:, 0] if raw.ndim == 2 else raw.reshape(-1)
                matching_scores.append(float(prediction[index] * action_scales[index]))
        action_scores.append((max(matching_scores), action))
    rules = []
    for _, action in sorted(action_scores, key=lambda row: (-row[0], row[1])):
        clauses = tuple(sorted(set(by_action[action]), key=lambda clause: clause.key))
        rules.append(BooleanRule(action=action, clauses=clauses))
    return BooleanCooldownPolicy(side=side, rules=tuple(rules)), positive_leaf_count


def _fit_tree_policy(
    panel: modeled.PreparedPanel,
    *,
    config: modeled.FrozenConfig,
    side: str,
    feature_block: str,
    train_index: pd.Index,
    profile: ComplexityProfile,
    random_seed: int,
    feature_names: Sequence[str] | None = None,
) -> tuple[BooleanCooldownPolicy, TreeFitAudit]:
    metadata = panel.metadata.loc[train_index]
    if set(metadata["side"]) != {side}:
        raise PersistentPolicyV3Error("tree training rows pool sides")
    features = (
        tuple(feature_names)
        if feature_names is not None
        else _nested_feature_pool(
            panel,
            config=config,
            train_index=train_index,
            feature_block=feature_block,
            feature_budget=profile.feature_budget,
        )
    )
    if not features or len(features) > profile.feature_budget:
        raise PersistentPolicyV3Error("frozen feature pool exceeds its profile budget")
    matrix = panel.features.loc[train_index, list(features)].to_numpy(dtype=np.float32)
    actions = duration_vocabulary(side)[1:]
    control = panel.outcomes.loc[train_index, CONTROL_ACTION].to_numpy(dtype=float)
    control_supported = panel.supported.loc[train_index, CONTROL_ACTION].to_numpy(dtype=bool)
    targets = np.zeros((len(train_index), len(actions)), dtype=np.float64)
    neutral = 0
    scales = np.ones(len(actions), dtype=float)
    for action_index, action in enumerate(actions):
        candidate = panel.outcomes.loc[train_index, action].to_numpy(dtype=float)
        candidate_supported = panel.supported.loc[train_index, action].to_numpy(dtype=bool)
        known = (
            control_supported
            & candidate_supported
            & np.isfinite(control)
            & np.isfinite(candidate)
        )
        effect = np.zeros(len(train_index), dtype=float)
        effect[known] = candidate[known] - control[known]
        neutral += int((~known).sum())
        observed = np.abs(effect[known])
        scale = float(np.quantile(observed, 0.75)) if observed.size else 1.0
        scales[action_index] = max(scale, 1e-9)
        targets[:, action_index] = effect / scales[action_index]
    weights = _campaign_weights(metadata)
    model = DecisionTreeRegressor(
        max_depth=profile.max_depth,
        max_leaf_nodes=profile.max_leaf_nodes,
        min_samples_leaf=profile.min_samples_leaf,
        random_state=random_seed,
    )
    model.fit(matrix, targets, sample_weight=weights)
    policy, positive_leaf_count = _compile_tree_policy(
        model,
        side=side,
        feature_names=features,
        action_scales=scales,
    )
    train_actions = policy.choose(panel.features.loc[train_index, list(features)])
    audit = TreeFitAudit(
        side=side,
        feature_block=feature_block,
        profile=profile.name,
        training_rows=len(train_index),
        training_days=int(metadata["utc_day"].nunique()),
        training_campaigns=int(metadata["campaign_cluster_id"].nunique()),
        selected_feature_count=len(features),
        nonbaseline_leaf_count=positive_leaf_count,
        compiled_rule_count=len(policy.rules),
        compiled_clause_count=sum(len(rule.clauses) for rule in policy.rules),
        compiled_literal_count=sum(
            len(clause.literals) for rule in policy.rules for clause in rule.clauses
        ),
        neutral_training_targets=neutral,
        training_action_rate=float(np.mean(train_actions != CONTROL_ACTION)),
        candidate_id=policy.candidate_id,
    )
    return policy, audit


def _equal_day_point(rows: pd.DataFrame) -> tuple[float, float, int]:
    identified = rows.loc[rows["point_identified"]].copy()
    if identified.empty:
        return math.nan, math.inf, 0
    identified["campaign_weight"] = modeled._campaign_weights(identified)
    campaign = (
        identified.assign(
            weighted_uplift=identified["campaign_weight"] * identified["uplift_usdc"]
        )
        .groupby(["utc_day", "campaign_cluster_id"], observed=True)["weighted_uplift"]
        .sum()
        .reset_index()
    )
    day = campaign.groupby("utc_day", observed=True)["weighted_uplift"].mean()
    values = day.to_numpy(dtype=float)
    mean = float(values.mean())
    se = float(values.std(ddof=1) / math.sqrt(len(values))) if len(values) > 1 else math.inf
    return mean, se, len(values)


def _test_index(
    panel: modeled.PreparedPanel,
    *,
    side: str,
    days: Sequence[str],
) -> pd.Index:
    return panel.metadata.index[
        (panel.metadata["side"] == side) & panel.metadata["utc_day"].isin(days)
    ]


def _run_cell(
    panel: modeled.PreparedPanel,
    *,
    config: modeled.FrozenConfig,
    panel_scope: str,
    side: str,
    feature_block: str,
    profiles: Sequence[ComplexityProfile],
    random_seed: int,
) -> CellExecution:
    folds = config.outer_folds[panel_scope]
    outer_rows: list[pd.DataFrame] = []
    fold_reports: list[Mapping[str, Any]] = []
    policies: list[Mapping[str, Any]] = []
    purge_audits: list[Mapping[str, Any]] = []
    for outer_index, outer in enumerate(folds):
        outer_train, outer_purge = modeled.observation_end_aware_purge(
            panel,
            side=side,
            train_days=outer.train_days,
            test_days=outer.test_days,
            fold_id=outer.fold_id,
            stage=f"{METHOD}.outer_refit",
        )
        maximum_budget = max(profile.feature_budget for profile in profiles)
        maximum_pool = _nested_feature_pool(
            panel,
            config=config,
            train_index=outer_train,
            feature_block=feature_block,
            feature_budget=maximum_budget,
        )
        inner_results: list[dict[str, Any]] = []
        for profile_index, profile in enumerate(profiles):
            profile_features = maximum_pool[: min(profile.feature_budget, len(maximum_pool))]
            inner_rows: list[pd.DataFrame] = []
            fit_audits: list[Mapping[str, Any]] = []
            for inner_index, inner in enumerate(modeled._inner_folds(outer, config)):
                train_index, purge = modeled.observation_end_aware_purge(
                    panel,
                    side=side,
                    train_days=inner.train_days,
                    test_days=inner.test_days,
                    fold_id=inner.fold_id,
                    stage=f"{METHOD}.inner_fit",
                )
                policy, audit = _fit_tree_policy(
                    panel,
                    config=config,
                    side=side,
                    feature_block=feature_block,
                    train_index=train_index,
                    profile=profile,
                    random_seed=random_seed + outer_index * 100 + profile_index * 10 + inner_index,
                    feature_names=profile_features,
                )
                test_index = _test_index(panel, side=side, days=inner.test_days)
                actions = policy.choose(panel.features.loc[test_index, list(policy.predicate_columns)])
                rows = modeled._evaluate_actions(
                    panel,
                    side=side,
                    opportunity_index=test_index,
                    actions=actions,
                    fold_id=inner.fold_id,
                    stage=f"{METHOD}.inner_oof",
                    candidate_id=policy.candidate_id,
                )
                inner_rows.append(rows)
                fit_audits.append(asdict(audit))
                purge_audits.append(asdict(purge))
            combined = pd.concat(inner_rows, ignore_index=True)
            mean, se, days = _equal_day_point(combined)
            if not math.isfinite(mean):
                continue
            inner_results.append(
                {
                    "profile": profile,
                    "equal_day_mean_usdc": mean,
                    "equal_day_standard_error_usdc": se,
                    "day_count": days,
                    "action_rate": float(combined["selected_nonbaseline"].mean()),
                    "fit_audits": fit_audits,
                }
            )
        if not inner_results:
            raise PersistentPolicyV3Error(f"{panel_scope}/{side}/{feature_block} has no inner policy")
        best = max(inner_results, key=lambda row: row["equal_day_mean_usdc"])
        cutoff = best["equal_day_mean_usdc"] - best["equal_day_standard_error_usdc"]
        tied = [row for row in inner_results if row["equal_day_mean_usdc"] >= cutoff]
        selected = min(
            tied,
            key=lambda row: (
                row["profile"].max_leaf_nodes,
                row["profile"].max_depth,
                row["profile"].feature_budget,
            ),
        )
        profile = selected["profile"]
        selected_features = maximum_pool[: min(profile.feature_budget, len(maximum_pool))]
        policy, fit_audit = _fit_tree_policy(
            panel,
            config=config,
            side=side,
            feature_block=feature_block,
            train_index=outer_train,
            profile=profile,
            random_seed=random_seed + outer_index * 1000 + 999,
            feature_names=selected_features,
        )
        outer_test = _test_index(panel, side=side, days=outer.test_days)
        actions = policy.choose(panel.features.loc[outer_test, list(policy.predicate_columns)])
        rows = modeled._evaluate_actions(
            panel,
            side=side,
            opportunity_index=outer_test,
            actions=actions,
            fold_id=outer.fold_id,
            stage=f"{METHOD}.outer_oof",
            candidate_id=policy.candidate_id,
        )
        outer_rows.append(rows)
        purge_audits.append(asdict(outer_purge))
        payload = policy.payload()
        policies.append(payload)
        fold_reports.append(
            {
                "fold_id": outer.fold_id,
                "train_days": list(outer.train_days),
                "test_days": list(outer.test_days),
                "selected_profile": asdict(profile),
                "one_standard_error_cutoff_usdc": cutoff,
                "inner_profile_evidence": [
                    {
                        **{key: value for key, value in row.items() if key != "profile"},
                        "profile": asdict(row["profile"]),
                    }
                    for row in inner_results
                ],
                "outer_fit_audit": asdict(fit_audit),
                "outer_policy": payload,
                "candidate_replaced_by_baseline_before_outer_oof": False,
                "outer_outcomes_used_for_fit": False,
            }
        )
    oof = pd.concat(outer_rows, ignore_index=True)
    oof["method"] = METHOD
    oof["feature_block"] = feature_block
    oof["panel_scope"] = panel_scope
    return CellExecution(
        panel_scope=panel_scope,
        side=side,
        feature_block=feature_block,
        oof_rows=oof,
        fold_reports=tuple(fold_reports),
        selected_policies=tuple(policies),
        purge_audits=tuple(purge_audits),
    )


def _comparison_cells(config: modeled.FrozenConfig) -> tuple[tuple[str, str, str], ...]:
    cells: list[tuple[str, str, str]] = []
    for scope in config.report_scopes:
        for side in ("BUY", "SELL"):
            for block in config.panel_feature_blocks[scope]:
                cells.append((scope, side, block))
    return tuple(cells)


def run_boolean_oof(
    panel: modeled.PreparedPanel,
    *,
    config: modeled.FrozenConfig,
    profiles: Sequence[ComplexityProfile] = DEFAULT_PROFILES,
    workers: int = 1,
    emit_progress: bool = False,
    cells: Sequence[tuple[str, str, str]] | None = None,
) -> tuple[pd.DataFrame, dict[str, Any], list[dict[str, Any]]]:
    if workers <= 0:
        raise PersistentPolicyV3Error("workers must be positive")
    requested_cells = tuple(cells) if cells is not None else _comparison_cells(config)
    available_cells = set(_comparison_cells(config))
    if not requested_cells or len(set(requested_cells)) != len(requested_cells):
        raise PersistentPolicyV3Error("requested cells must be nonempty and unique")
    if set(requested_cells) - available_cells:
        raise PersistentPolicyV3Error("requested cell is outside the frozen panel")
    results: list[CellExecution] = []

    def execute(cell: tuple[str, str, str]) -> CellExecution:
        scope, side, block = cell
        return _run_cell(
            panel,
            config=config,
            panel_scope=scope,
            side=side,
            feature_block=block,
            profiles=profiles,
            random_seed=DEFAULT_RANDOM_SEED + requested_cells.index(cell) * 10000,
        )

    if workers == 1:
        for index, cell in enumerate(requested_cells, start=1):
            result = execute(cell)
            results.append(result)
            if emit_progress:
                print(
                    _canonical_json(
                        {
                            "event": "cell_completed",
                            "completed": index,
                            "total": len(requested_cells),
                            "cell": list(cell),
                        }
                    ),
                    flush=True,
                )
    else:
        with ThreadPoolExecutor(max_workers=min(workers, len(requested_cells))) as executor:
            futures = {executor.submit(execute, cell): cell for cell in requested_cells}
            for index, future in enumerate(as_completed(futures), start=1):
                cell = futures[future]
                results.append(future.result())
                if emit_progress:
                    print(
                        _canonical_json(
                            {
                                "event": "cell_completed",
                                "completed": index,
                                "total": len(requested_cells),
                                "cell": list(cell),
                            }
                        ),
                        flush=True,
                    )
    results.sort(key=lambda value: (value.panel_scope, value.side, value.feature_block))
    rows = pd.concat([result.oof_rows for result in results], ignore_index=True)
    policies: dict[str, Any] = {}
    purges: list[dict[str, Any]] = []
    for result in results:
        key = f"{result.panel_scope}/{result.side}/{result.feature_block}"
        policies[key] = {
            "fold_reports": list(result.fold_reports),
            "outer_policies": list(result.selected_policies),
        }
        purges.extend(result.purge_audits)
    return rows, policies, purges


def _write_json(path: Path, value: Any) -> None:
    path.write_text(_canonical_json(value) + "\n", encoding="utf-8")


def publish_output(
    output: Path,
    *,
    report: Mapping[str, Any],
    rows: pd.DataFrame,
    policies: Mapping[str, Any],
    purges: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output.name}.", dir=output.parent))
    try:
        rows.to_parquet(staging / "outer_oof.parquet", index=False)
        _write_json(staging / "selected_policies.json", policies)
        _write_json(staging / "purge_audits.json", list(purges))
        _write_json(staging / "report.json", report)
        files = []
        for path in sorted(staging.iterdir()):
            files.append(
                {
                    "relative_path": path.name,
                    "bytes": path.stat().st_size,
                    "sha256": _sha256(path),
                }
            )
        manifest_body = {
            "schema_version": f"{SCHEMA_VERSION}.manifest.v1",
            "identity": IDENTITY,
            "files": files,
            "permissions": {
                "action_authorized": False,
                "repeated_policy_run": False,
                "live_authorized": False,
            },
        }
        manifest = {**manifest_body, "canonical_sha256": _canonical_sha256(manifest_body)}
        _write_json(staging / "manifest.json", manifest)
        (staging / "_SUCCESS").write_text(manifest["canonical_sha256"] + "\n", encoding="ascii")
        if output.exists():
            raise PersistentPolicyV3Error(f"output already exists: {output}")
        os.replace(staging, output)
        return {
            "output": str(output),
            "manifest_sha256": _sha256(output / "manifest.json"),
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
    parser.add_argument("--output", type=Path)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument(
        "--cell",
        action="append",
        default=None,
        help="Optional frozen cell as panel_scope/SIDE/feature_block; repeatable.",
    )
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
    config, amendment = load_v3_execution_amendment(
        args.execution_amendment,
        expected_sha256=args.execution_amendment_sha256,
        config=config,
    )
    if args.preflight_only:
        label_binding = modeled.verify_input_artifact(config.labels)
        feature_binding = modeled.verify_input_artifact(config.features)
        print(
            _canonical_json(
                {
                    "identity": IDENTITY,
                    "config_sha256": config.sha256,
                    "execution_identity_sha256": amendment.execution_identity_sha256,
                    "modeled_label_manifest_sha256": label_binding.manifest_sha256,
                    "feature_manifest_sha256": feature_binding.manifest_sha256,
                    "permissions": {
                        "action_authorized": False,
                        "repeated_policy_run": False,
                        "live_authorized": False,
                    },
                }
            )
        )
        return 0
    if args.output is None:
        raise PersistentPolicyV3Error("--output is required unless --preflight-only is set")
    panel, bindings = modeled.load_bound_panel(config, execution_amendment=amendment)
    cells = None
    if args.cell:
        parsed = []
        for value in args.cell:
            parts = tuple(str(value).split("/"))
            if len(parts) != 3:
                raise PersistentPolicyV3Error("--cell must be panel_scope/SIDE/feature_block")
            parsed.append((parts[0], parts[1].upper(), parts[2].upper()))
        cells = tuple(parsed)
    rows, policies, purges = run_boolean_oof(
        panel,
        config=config,
        workers=args.workers,
        emit_progress=True,
        cells=cells,
    )
    report = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "research_type": "outcome_informed_exploratory_successor",
        "evidence_route": OWNER_ROUTE,
        "method": METHOD,
        "config_sha256": config.sha256,
        "binding_sha256": bindings["binding_sha256"],
        "profiles": [asdict(profile) for profile in DEFAULT_PROFILES],
        "opportunities": int(len(panel.metadata)),
        "oof_rows": int(len(rows)),
        "cells": int(rows[["panel_scope", "side", "feature_block"]].drop_duplicates().shape[0]),
        "strict_queue_policy_eligible": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "permissions": {
            "unified_policy_frozen": False,
            "repeated_policy_run": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    result = publish_output(
        args.output,
        report=report,
        rows=rows,
        policies=policies,
        purges=purges,
    )
    print(_canonical_json(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
