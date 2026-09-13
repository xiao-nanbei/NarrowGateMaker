"""Orchestrate strict-native cooldown-v2 day panels and nested OOF.

The study consumes the formal strict-label panel manifest.  It never accepts a
standalone economic table: every day is admitted through
``assemble_day_label_panel`` and is then hash-validated.  All assignment
opportunities remain in the denominator audit, while only opportunities with
the exact side-specific eight-arm vocabulary enter the economic learner.

Book and trade thresholds are supplied as separate outcome-blind 2025
artifacts.  M0 predicates are fitted inside each outer/inner chronological
training fold from the full causal denominator, never from economic labels.
At a 2026 strict-native assignment cutoff, independently observed book and
trade predicates may share a Boolean clause because they are evaluated on one
common exchange-visible snapshot.  That target-time join does not fabricate a
joint 2025 reference clock.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
import shutil
import sys
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from statistics import NormalDist
from typing import Any

import numpy as np
import pandas as pd
import sklearn
from sklearn.tree import DecisionTreeRegressor

from data_paths import resolve_portable_path
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_features import (
    M0_REQUIRED_FIELDS,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_label_panel import (
    PANEL_IDENTITY,
    assemble_day_label_panel,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_nested_oof import (
    CONTROL_ACTION,
    FEATURE_BLOCKS,
    BooleanCooldownPolicy,
    ChronologicalFold,
    ClusteredEstimate,
    NestedOofContractError,
    NestedOofResult,
    OuterFoldExecution,
    SearchConfig,
    SupportAudit,
    clustered_estimate,
    duration_vocabulary,
    evaluate_post_oof_deployment_gate,
    expanding_chronological_folds,
    generate_bounded_candidates,
    role_support_audit,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_predicates import (
    PredicateArtifact,
    PredicateContractError,
    fit_predicate_artifact,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_strict_label_panel_runner import (
    PANEL_SCHEMA_VERSION,
    RUNNER_IDENTITY,
)

IDENTITY = "causal_multichannel_window_boolean_cooldown_duration_v2"
STUDY_IDENTITY = f"{IDENTITY}.multiday_label_panel_nested_oof.v1"
FORMAL_PANEL_SCHEMA = PANEL_SCHEMA_VERSION
PREDICATE_BUNDLE_SCHEMA = f"{STUDY_IDENTITY}.predicate_bundle.v1"
PROGRESS_SCHEMA = f"{STUDY_IDENTITY}.progress.v1"
REPORT_SCHEMA = f"{STUDY_IDENTITY}.report.v3"
MANIFEST_SCHEMA = f"{STUDY_IDENTITY}.admission.v3"

SIDES = ("BUY", "SELL")
PANEL_SCOPES = ("prefix40", "added10", "pooled50")
PANEL_ROLE_MAP = {
    "prefix40_development": "prefix40",
    "added10_late_diagnostic": "added10",
}
BOOK_OR_TRADE_GROUPS = frozenset({"book", "trade"})
FORMAL_SUPPORT_IDENTITY = "full_D_minus_1_D_D_plus_1"
FORMAL_OBSERVED_DAY_COUNT = 41
FORMAL_PREFIX_DAY_COUNT = 33
FORMAL_ADDED_DAY_COUNT = 8
FORMAL_DAY_DENOMINATORS = {
    "prefix40": {
        "report_key": "prefix_exact_label_economic",
        "nominal_mechanics_days": 40,
        "exact_label_economic_days": 33,
        "reduced_support_diagnostic_days": 7,
    },
    "added10": {
        "report_key": "added_exact_label_economic",
        "nominal_mechanics_days": 10,
        "exact_label_economic_days": 8,
        "reduced_support_diagnostic_days": 2,
    },
    "pooled50": {
        "report_key": "pooled_exact_label_economic",
        "nominal_mechanics_days": 50,
        "exact_label_economic_days": 41,
        "reduced_support_diagnostic_days": 9,
    },
}


class CooldownStudyError(RuntimeError):
    """Raised when a study input, admission, fold, or output drifts."""


@dataclass(frozen=True, slots=True)
class ContinuousComparatorConfig:
    """Pre-outcome capacity grid for the raw-state diagnostic comparator."""

    max_depth_candidates: tuple[int, ...] = (2, 4)
    min_samples_leaf: int = 20
    random_state: int = 20260810

    def __post_init__(self) -> None:
        if (
            not self.max_depth_candidates
            or tuple(sorted(set(self.max_depth_candidates)))
            != self.max_depth_candidates
            or any(depth < 1 for depth in self.max_depth_candidates)
        ):
            raise CooldownStudyError(
                "continuous comparator depths must be positive, unique, and sorted"
            )
        if self.min_samples_leaf < 1:
            raise CooldownStudyError(
                "continuous comparator minimum leaf size must be positive"
            )

    def payload(self) -> dict[str, Any]:
        return {
            "model_family": "raw_state_multioutput_regression_tree_diagnostic",
            "max_depth_candidates": list(self.max_depth_candidates),
            "min_samples_leaf": self.min_samples_leaf,
            "random_state": self.random_state,
            "capacity_selected_in": "inner_chronological_folds_only",
            "outer_fold_role": "execute_inner_frozen_capacity_only",
            "may_replace_boolean_policy": False,
            "may_grant_action_or_live": False,
        }


@dataclass(frozen=True, slots=True)
class StudyConfig:
    outer_folds: int = 4
    outer_minimum_train_days: int = 12
    search: SearchConfig = SearchConfig()
    economic_epsilon_usdc: float = 0.0
    minimum_deployment_action_rate: float = 0.0
    minimum_deployment_campaigns: int = 2
    minimum_deployment_days: int = 2
    engineering_allow_nonformal_panel: bool = False
    continuous_comparator: ContinuousComparatorConfig = ContinuousComparatorConfig()

    def __post_init__(self) -> None:
        if self.outer_folds < 1 or self.outer_minimum_train_days < 1:
            raise CooldownStudyError("outer fold settings must be positive")
        if (
            self.outer_minimum_train_days
            < self.search.inner_minimum_train_days + self.search.inner_folds
        ):
            raise CooldownStudyError(
                "outer minimum train days cannot support the frozen inner folds"
            )
        if not math.isfinite(self.economic_epsilon_usdc):
            raise CooldownStudyError("economic epsilon must be finite")
        if not 0.0 <= self.minimum_deployment_action_rate <= 1.0:
            raise CooldownStudyError("deployment action rate must be in [0, 1]")
        if self.minimum_deployment_campaigns < 1 or self.minimum_deployment_days < 1:
            raise CooldownStudyError("deployment support counts must be positive")

    def payload(self) -> dict[str, Any]:
        return {
            "outer_folds": self.outer_folds,
            "outer_minimum_train_days": self.outer_minimum_train_days,
            "search": asdict(self.search),
            "economic_epsilon_usdc": self.economic_epsilon_usdc,
            "minimum_deployment_action_rate": self.minimum_deployment_action_rate,
            "minimum_deployment_campaigns": self.minimum_deployment_campaigns,
            "minimum_deployment_days": self.minimum_deployment_days,
            "engineering_allow_nonformal_panel": self.engineering_allow_nonformal_panel,
            "continuous_comparator": self.continuous_comparator.payload(),
        }


@dataclass(frozen=True, slots=True)
class DaySource:
    day: str
    panel_role: str
    manifest_path: Path
    manifest_sha256: str


@dataclass(frozen=True, slots=True)
class ArtifactReference:
    path: Path
    sha256: str


@dataclass(frozen=True, slots=True)
class LoadedPredicateBundle:
    path: Path
    sha256: str
    canonical_sha256: str
    book: Mapping[str, PredicateArtifact]
    trade: Mapping[str, PredicateArtifact]
    m0: Mapping[tuple[str, str, str], PredicateArtifact]
    artifact_references: Mapping[str, ArtifactReference]


@dataclass(frozen=True, slots=True)
class FoldPlan:
    panel_scope: str
    side: str
    observed_days: tuple[str, ...]
    exact_label_days: tuple[str, ...]
    excluded_days: tuple[str, ...]
    outer_folds: tuple[ChronologicalFold, ...]
    unavailable_reason: str | None


@dataclass(frozen=True, slots=True)
class EvaluationContext:
    side: str
    panel_scope: str
    predicate_columns: tuple[str, ...]
    metadata: pd.DataFrame
    predicates: pd.DataFrame
    outcomes: pd.DataFrame


@dataclass(frozen=True, slots=True)
class ContinuousFeatureSpec:
    numeric_fields: tuple[str, ...]
    categorical_fields: tuple[tuple[str, tuple[str, ...]], ...]

    def __post_init__(self) -> None:
        categorical_names = tuple(field for field, _ in self.categorical_fields)
        if tuple(sorted(set(self.numeric_fields))) != self.numeric_fields:
            raise CooldownStudyError("continuous numeric feature fields drifted")
        if tuple(sorted(set(categorical_names))) != categorical_names:
            raise CooldownStudyError("continuous categorical feature fields drifted")
        if set(self.numeric_fields) & set(categorical_names):
            raise CooldownStudyError("continuous feature field has two encodings")
        if not self.numeric_fields and not self.categorical_fields:
            raise CooldownStudyError("continuous comparator feature schema is empty")

    def payload(self) -> dict[str, Any]:
        return {
            "numeric_fields": list(self.numeric_fields),
            "categorical_fields": {
                field: list(categories) for field, categories in self.categorical_fields
            },
        }


@dataclass(frozen=True, slots=True)
class ContinuousComparatorResult:
    side: str
    feature_block: str
    panel_scope: str
    oof_rows: pd.DataFrame
    fold_reports: tuple[Mapping[str, Any], ...]
    raw_feature_names: tuple[str, ...]


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


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CooldownStudyError(f"cannot read JSON object: {path}") from exc
    if not isinstance(payload, dict):
        raise CooldownStudyError(f"JSON root is not an object: {path}")
    return payload


def _resolve(path: str | Path, *, relative_to: Path) -> Path:
    candidate = resolve_portable_path(path)
    if not candidate.is_absolute():
        candidate = relative_to / candidate
    return candidate.resolve()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}")
    with temporary.open("x", encoding="ascii") as handle:
        handle.write(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    _fsync_directory(path.parent)


def _normalize_day(value: Any) -> str:
    try:
        parsed = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise CooldownStudyError(f"invalid UTC day: {value!r}") from exc
    if parsed.tzinfo is not None:
        parsed = parsed.tz_convert("UTC").tz_localize(None)
    if parsed != parsed.normalize():
        raise CooldownStudyError(f"UTC day contains a time component: {value!r}")
    return parsed.strftime("%Y-%m-%d")


def _validate_hash_reference(
    raw: Mapping[str, Any], *, relative_to: Path, label: str
) -> ArtifactReference:
    path = _resolve(str(raw.get("path", "")), relative_to=relative_to)
    expected = str(raw.get("sha256", ""))
    if len(expected) != 64 or not path.is_file() or _sha256(path) != expected:
        raise CooldownStudyError(f"{label} path/hash drifted")
    return ArtifactReference(path=path, sha256=expected)


def _frozen_days_from_spec(spec: Mapping[str, Any]) -> tuple[tuple[str, ...], int, int]:
    ordered = spec.get("ordered_utc_days")
    strict_source = spec.get("source_separation", {}).get("strict_native_2026", {})
    if not isinstance(ordered, Mapping) or not isinstance(strict_source, Mapping):
        raise CooldownStudyError("formal v2 Spec lacks the frozen strict-native calendar")
    prefix = tuple(_normalize_day(day) for day in ordered.get("prefix40", ()))
    added = tuple(_normalize_day(day) for day in ordered.get("added10", ()))
    reduced = frozenset(
        _normalize_day(day) for day in strict_source.get("reduced_support_days", ())
    )
    if len(prefix) != 40 or len(added) != 10 or len(set((*prefix, *added))) != 50:
        raise CooldownStudyError("formal v2 Spec 40+10 calendar drifted")
    if strict_source.get("full_support_identity") != FORMAL_SUPPORT_IDENTITY:
        raise CooldownStudyError("formal strict-native support identity drifted")
    full = tuple(day for day in (*prefix, *added) if day not in reduced)
    prefix_count = sum(day in set(prefix) for day in full)
    added_count = sum(day in set(added) for day in full)
    if (
        len(full) != FORMAL_OBSERVED_DAY_COUNT
        or prefix_count != FORMAL_PREFIX_DAY_COUNT
        or added_count != FORMAL_ADDED_DAY_COUNT
    ):
        raise CooldownStudyError("formal 41-day full-support calendar drifted")
    return full, prefix_count, added_count


def _validate_formal_panel_manifest(
    path: Path,
    *,
    allow_engineering_panel: bool = False,
) -> tuple[dict[str, Any], tuple[DaySource, ...]]:
    manifest_path = Path(path).expanduser().resolve()
    manifest = _load_json(manifest_path)
    if manifest.get("schema_version") != FORMAL_PANEL_SCHEMA:
        raise CooldownStudyError("formal strict-label panel schema drifted")
    if manifest.get("identity") != RUNNER_IDENTITY:
        raise CooldownStudyError("formal strict-label runner identity drifted")
    formal = manifest.get("formal_full_support_run") is True
    if not formal and not allow_engineering_panel:
        raise CooldownStudyError("study requires a formal full-support strict-label panel")
    raw_days = manifest.get("ordered_days")
    rows = manifest.get("day_manifests")
    if not isinstance(raw_days, list) or not isinstance(rows, list):
        raise CooldownStudyError("formal panel day denominator is missing")
    ordered_days = tuple(_normalize_day(day) for day in raw_days)
    if ordered_days != tuple(sorted(set(ordered_days))):
        raise CooldownStudyError("formal panel days must be chronological and unique")
    if int(manifest.get("day_count", -1)) != len(ordered_days) or len(rows) != len(ordered_days):
        raise CooldownStudyError("formal panel day counts drifted")
    by_day: dict[str, Mapping[str, Any]] = {}
    for raw in rows:
        if not isinstance(raw, Mapping):
            raise CooldownStudyError("formal panel day entry is invalid")
        day = _normalize_day(raw.get("day"))
        if day in by_day:
            raise CooldownStudyError("formal panel repeats a day manifest")
        by_day[day] = raw
    if tuple(by_day) != ordered_days:
        raise CooldownStudyError("formal day-manifest ordering drifted")
    sources: list[DaySource] = []
    for day in ordered_days:
        raw = by_day[day]
        reference = _validate_hash_reference(
            {
                "path": raw.get("manifest_path"),
                "sha256": raw.get("manifest_sha256"),
            },
            relative_to=manifest_path.parent,
            label=f"strict day manifest {day}",
        )
        day_manifest = _load_json(reference.path)
        if day_manifest.get("schema_version") != (
            f"{IDENTITY}.strict_native_one_shot_labels.v1.day.v2"
        ):
            raise CooldownStudyError(f"strict day schema drifted for {day}")
        if _normalize_day(day_manifest.get("target_day")) != day:
            raise CooldownStudyError(f"strict day target drifted for {day}")
        role = PANEL_ROLE_MAP.get(str(day_manifest.get("panel_role", "")))
        if role is None:
            raise CooldownStudyError(f"strict day panel role drifted for {day}")
        sources.append(
            DaySource(
                day=day,
                panel_role=role,
                manifest_path=reference.path,
                manifest_sha256=reference.sha256,
            )
        )
    prefix_count = sum(source.panel_role == "prefix40" for source in sources)
    added_count = sum(source.panel_role == "added10" for source in sources)
    if int(manifest.get("prefix40_full_support_count", -1)) != prefix_count:
        raise CooldownStudyError("formal prefix40 support count drifted")
    if int(manifest.get("added10_full_support_count", -1)) != added_count:
        raise CooldownStudyError("formal added10 support count drifted")
    if formal:
        if manifest.get("source_support_identity") != FORMAL_SUPPORT_IDENTITY:
            raise CooldownStudyError("formal panel source support identity drifted")
        spec_reference = _validate_hash_reference(
            {
                "path": manifest.get("spec_path"),
                "sha256": manifest.get("spec_sha256"),
            },
            relative_to=manifest_path.parent,
            label="formal v2 Spec",
        )
        frozen_days, frozen_prefix_count, frozen_added_count = _frozen_days_from_spec(
            _load_json(spec_reference.path)
        )
        if ordered_days != frozen_days:
            raise CooldownStudyError("formal panel is not the exact frozen 41-day calendar")
        if prefix_count != frozen_prefix_count or added_count != frozen_added_count:
            raise CooldownStudyError("formal panel role counts drifted from the frozen Spec")
    elif manifest.get("run_kind") != "engineering_test_fixture":
        raise CooldownStudyError("nonformal panel lacks an explicit engineering identity")
    return manifest, tuple(sources)


def _load_predicate_artifact(reference: ArtifactReference) -> PredicateArtifact:
    try:
        artifact = PredicateArtifact.from_json(reference.path.read_text(encoding="utf-8"))
    except (OSError, PredicateContractError) as exc:
        raise CooldownStudyError(f"predicate artifact is invalid: {reference.path}") from exc
    return artifact


def _definition_groups(artifact: PredicateArtifact) -> frozenset[str]:
    return frozenset(definition.clock_group for definition in artifact.definitions)


def _validate_market_artifact(artifact: PredicateArtifact, *, side: str, channel: str) -> None:
    if artifact.side != side:
        raise CooldownStudyError(f"{channel} artifact side drifted for {side}")
    if artifact.source_role != "outcome_blind_2025_single_channel":
        raise CooldownStudyError(f"{channel} artifact is not outcome-blind 2025")
    if not artifact.clock_separated_2025:
        raise CooldownStudyError(f"{channel} artifact lacks 2025 clock separation")
    if not artifact.reference_days or any(
        not day.startswith("2025-") for day in artifact.reference_days
    ):
        raise CooldownStudyError(f"{channel} artifact reference is not 2025")
    groups = _definition_groups(artifact) - {"context"}
    if groups - {channel}:
        raise CooldownStudyError(
            f"{channel} artifact contains a foreign clock group: {sorted(groups)}"
        )
    if not groups:
        raise CooldownStudyError(f"{channel} artifact contains no {channel} predicates")


def _validate_m0_artifact(
    artifact: PredicateArtifact,
    *,
    side: str,
    expected_train_days: Sequence[str] | None = None,
) -> None:
    if artifact.side != side:
        raise CooldownStudyError(f"M0 artifact side drifted for {side}")
    if artifact.source_role != "inner_chronological_development":
        raise CooldownStudyError("M0 artifact is not inner-chronological")
    if _definition_groups(artifact) - {"context"}:
        raise CooldownStudyError("M0 artifact contains market-clock predicates")
    if any(definition.block != "M0" for definition in artifact.definitions):
        raise CooldownStudyError("M0 artifact contains a non-M0 definition")
    if expected_train_days is not None and tuple(artifact.reference_days) != tuple(
        expected_train_days
    ):
        raise CooldownStudyError("M0 artifact train-day identity drifted")


def _load_predicate_bundle(path: Path) -> LoadedPredicateBundle:
    bundle_path = Path(path).expanduser().resolve()
    raw = _load_json(bundle_path)
    expected_canonical = str(raw.get("canonical_sha256", ""))
    body = dict(raw)
    body.pop("canonical_sha256", None)
    if len(expected_canonical) != 64 or _canonical_sha256(body) != expected_canonical:
        raise CooldownStudyError("predicate bundle canonical SHA256 drifted")
    if raw.get("schema_version") != PREDICATE_BUNDLE_SCHEMA:
        raise CooldownStudyError("predicate bundle schema drifted")
    if raw.get("identity") != IDENTITY:
        raise CooldownStudyError("predicate bundle identity drifted")
    if (
        raw.get("cross_clock_clause_authorized") is not False
        or raw.get("cross_clock_clause_scope") != "2025_reference_rows_only"
    ):
        raise CooldownStudyError("2025 predicate bundle clock scope drifted")
    strict_target = raw.get("strict_2026_target_snapshot")
    if (
        not isinstance(strict_target, Mapping)
        or strict_target.get("book_trade_predicates_may_be_combined_by_study") is not True
    ):
        raise CooldownStudyError("predicate bundle lacks strict target-time join semantics")
    references: dict[str, ArtifactReference] = {}
    market: dict[str, dict[str, PredicateArtifact]] = {
        "book": {},
        "trade": {},
    }
    for channel in ("book", "trade"):
        channel_rows = raw.get(channel)
        if not isinstance(channel_rows, Mapping) or set(channel_rows) != set(SIDES):
            raise CooldownStudyError(
                f"predicate bundle must provide {channel} artifacts for BUY and SELL"
            )
        for side in SIDES:
            entry = channel_rows[side]
            if not isinstance(entry, Mapping):
                raise CooldownStudyError(f"invalid {channel}/{side} artifact entry")
            reference = _validate_hash_reference(
                entry,
                relative_to=bundle_path.parent,
                label=f"{channel}/{side} predicate artifact",
            )
            artifact = _load_predicate_artifact(reference)
            _validate_market_artifact(artifact, side=side, channel=channel)
            references[f"{channel}:{side}"] = reference
            market[channel][side] = artifact

    m0_rows = raw.get("m0_artifacts", [])
    if m0_rows not in (None, []):
        raise CooldownStudyError(
            "external M0 artifacts are forbidden; the study fits them inside each "
            "frozen chronological training fold"
        )
    return LoadedPredicateBundle(
        path=bundle_path,
        sha256=_sha256(bundle_path),
        canonical_sha256=expected_canonical,
        book=market["book"],
        trade=market["trade"],
        m0={},
        artifact_references=references,
    )


def _validate_day_panel(
    destination: Path,
    *,
    source: DaySource,
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    destination = Path(destination).resolve()
    manifest_path = destination / "manifest.json"
    success_path = destination / "_SUCCESS"
    if not manifest_path.is_file() or not success_path.is_file():
        raise CooldownStudyError(f"day-panel admission is incomplete: {destination}")
    manifest = _load_json(manifest_path)
    success = _load_json(success_path)
    if success.get("manifest_sha256") != _sha256(manifest_path):
        raise CooldownStudyError(f"day-panel success hash drifted for {source.day}")
    if manifest.get("schema_version") != PANEL_IDENTITY:
        raise CooldownStudyError(f"day-panel schema drifted for {source.day}")
    if manifest.get("identity") != IDENTITY:
        raise CooldownStudyError(f"day-panel identity drifted for {source.day}")
    if _normalize_day(manifest.get("target_day")) != source.day:
        raise CooldownStudyError(f"day-panel target drifted for {source.day}")
    if manifest.get("panel_role") != source.panel_role:
        raise CooldownStudyError(f"day-panel role drifted for {source.day}")
    source_row = manifest.get("day_manifest")
    if not isinstance(source_row, Mapping):
        raise CooldownStudyError(f"day-panel source binding is missing for {source.day}")
    bound_path = _resolve(str(source_row.get("path", "")), relative_to=manifest_path.parent)
    if bound_path != source.manifest_path or source_row.get("sha256") != source.manifest_sha256:
        raise CooldownStudyError(f"day-panel source binding drifted for {source.day}")
    if manifest.get("complete_case_filter_applied") is not False:
        raise CooldownStudyError(f"day-panel hid incomplete labels for {source.day}")
    if manifest.get("economic_outcomes_read") is not True:
        raise CooldownStudyError(f"day-panel economic-read identity drifted for {source.day}")

    frames: dict[str, pd.DataFrame] = {}
    for key in ("opportunities", "labels"):
        row = manifest.get(key)
        if not isinstance(row, Mapping):
            raise CooldownStudyError(f"day-panel {key} metadata is missing")
        path = _resolve(str(row.get("path", "")), relative_to=manifest_path.parent)
        if not path.is_file() or _sha256(path) != row.get("sha256"):
            raise CooldownStudyError(f"day-panel {key} hash drifted for {source.day}")
        frame = pd.read_parquet(path)
        if len(frame) != int(row.get("rows", -1)):
            raise CooldownStudyError(f"day-panel {key} row count drifted for {source.day}")
        frames[key] = frame
    opportunities = frames["opportunities"]
    labels = frames["labels"]
    if opportunities.empty:
        raise CooldownStudyError(f"day-panel has no denominator rows for {source.day}")
    if opportunities["snapshot_id"].duplicated().any():
        raise CooldownStudyError(f"day-panel snapshot IDs repeat for {source.day}")
    if labels.duplicated(["opportunity_id", "duration_policy_id"]).any():
        raise CooldownStudyError(f"day-panel labels repeat for {source.day}")
    generated = opportunities.loc[opportunities["labels_generated"]].copy()
    generated_ids = set(generated["opportunity_id"].astype(str))
    if set(labels["opportunity_id"].astype(str)) != generated_ids:
        raise CooldownStudyError(
            f"day-panel label denominator drifted for {source.day}"
        )
    for raw in generated.to_dict(orient="records"):
        opportunity_id = str(raw["opportunity_id"])
        side = str(raw["side"]).upper()
        rows = labels.loc[labels["opportunity_id"].astype(str) == opportunity_id]
        vocabulary = set(duration_vocabulary(side))
        if len(rows) != 8 or set(rows["duration_policy_id"].astype(str)) != vocabulary:
            raise CooldownStudyError(
                f"day-panel did not retain all eight arms for {source.day}"
            )
        strict_count = int(rows["strict_native_label"].sum())
        if strict_count != int(raw["strict_arm_count"]):
            raise CooldownStudyError(
                f"day-panel strict-arm count drifted for {source.day}"
            )
        if 8 - strict_count != int(raw["unsupported_arm_count"]):
            raise CooldownStudyError(
                f"day-panel unsupported-arm count drifted for {source.day}"
            )
    non_strict = labels.loc[~labels["strict_native_label"]]
    if not non_strict["terminal_value_usdc"].isna().all():
        raise CooldownStudyError(
            f"unsupported arm exposes a point label for {source.day}"
        )
    point_status = labels["economic_point_label_status"].astype(str)
    if not point_status.isin({"eligible", "unsupported_redacted"}).all():
        raise CooldownStudyError(
            f"arm point-label status drifted for {source.day}"
        )
    strict = labels.loc[labels["strict_native_label"]]
    if not strict["economic_point_label_status"].eq("eligible").all():
        raise CooldownStudyError(f"strict arm point-label status drifted for {source.day}")
    redacted = labels.loc[point_status.eq("unsupported_redacted")]
    if redacted["strict_native_label"].any() or not redacted[
        "terminal_value_usdc"
    ].isna().all():
        raise CooldownStudyError(
            f"unsupported-redacted arm exposes an economic label for {source.day}"
        )
    eligible_non_strict = labels.loc[
        point_status.eq("eligible") & ~labels["strict_native_label"]
    ]
    if not (
        eligible_non_strict["right_censored"]
        | ~eligible_non_strict["washout_complete"]
    ).all():
        raise CooldownStudyError(
            f"eligible execution arm lacks a censoring reason for {source.day}"
        )
    strict_values = pd.to_numeric(
        strict["terminal_value_usdc"],
        errors="coerce",
    )
    if strict_values.isna().any() or not np.isfinite(strict_values.to_numpy()).all():
        raise CooldownStudyError(f"strict arm lacks a finite point label for {source.day}")
    if not opportunities["utc_day"].astype(str).eq(source.day).all():
        raise CooldownStudyError(f"day-panel opportunity day drifted for {source.day}")
    if not opportunities["panel_role"].astype(str).eq(source.panel_role).all():
        raise CooldownStudyError(f"day-panel opportunity role drifted for {source.day}")
    return manifest, opportunities, labels


def _admit_day_panels(
    *,
    sources: Sequence[DaySource],
    work_root: Path,
    progress_path: Path,
    progress: dict[str, Any],
) -> tuple[tuple[dict[str, Any], pd.DataFrame, pd.DataFrame], ...]:
    panels: list[tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]] = []
    day_root = work_root / "day_panels"
    day_root.mkdir(parents=True, exist_ok=True)
    for source in sources:
        destination = day_root / f"day={source.day}"
        if not destination.exists():
            try:
                assemble_day_label_panel(
                    source.manifest_path,
                    destination=destination,
                )
            except Exception as exc:
                raise CooldownStudyError(
                    f"strict day-panel admission failed for {source.day}"
                ) from exc
        panel = _validate_day_panel(destination, source=source)
        panels.append(panel)
        manifest_path = destination / "manifest.json"
        progress["days"][source.day] = {
            "status": "admitted",
            "panel_role": source.panel_role,
            "source_manifest_path": str(source.manifest_path),
            "source_manifest_sha256": source.manifest_sha256,
            "day_panel_manifest_path": str(manifest_path),
            "day_panel_manifest_sha256": _sha256(manifest_path),
        }
        _atomic_json(progress_path, progress)
    return tuple(panels)


def _denominator_status(row: pd.Series) -> str:
    if not bool(row["labels_generated"]):
        return "unlabeled"
    if bool(row["joint_strict_native_label"]):
        return "exact_eight_arm"
    if bool(row.get("joint_right_censored", False)):
        return "right_censored_or_incomplete"
    return "non_exact_eight_arm"


def _combine_day_panels(
    panels: Sequence[tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    opportunities = pd.concat([panel[1] for panel in panels], ignore_index=True, sort=False)
    labels = pd.concat([panel[2] for panel in panels], ignore_index=True, sort=False)
    if opportunities["snapshot_id"].duplicated().any():
        raise CooldownStudyError("snapshot IDs repeat across admitted days")
    opportunities["denominator_status"] = opportunities.apply(_denominator_status, axis=1)
    opportunities["economic_learner_eligible"] = opportunities["denominator_status"].eq(
        "exact_eight_arm"
    )
    exact = opportunities.loc[opportunities["economic_learner_eligible"]].copy()
    if exact.empty:
        raise CooldownStudyError("formal panel contains no exact eight-arm opportunity")
    if exact["opportunity_id"].isna().any():
        raise CooldownStudyError("exact opportunity lacks an identity")
    exact_ids = set(exact["opportunity_id"].astype(str))
    economic_labels = labels.loc[labels["opportunity_id"].astype(str).isin(exact_ids)].copy()
    if not economic_labels["strict_native_label"].eq(True).all():  # noqa: E712
        raise CooldownStudyError("non-strict arm entered the economic learner")
    for side in SIDES:
        side_exact = exact.loc[exact["side"].astype(str).str.upper() == side]
        vocabulary = set(duration_vocabulary(side))
        for opportunity_id in side_exact["opportunity_id"].astype(str):
            arm_rows = economic_labels.loc[
                economic_labels["opportunity_id"].astype(str) == opportunity_id
            ]
            if len(arm_rows) != 8 or set(arm_rows["duration_policy_id"].astype(str)) != vocabulary:
                raise CooldownStudyError(
                    "economic learner requires the exact side-specific eight arms"
                )
    economic = economic_labels.merge(
        exact,
        on=[
            "opportunity_id",
            "utc_day",
            "panel_role",
            "side",
            "role_at_fill",
            "campaign_id",
        ],
        how="inner",
        validate="many_to_one",
        suffixes=("_label", ""),
    )
    if len(economic) != len(economic_labels):
        raise CooldownStudyError("exact labels did not join their feature rows")
    economic["terminal_value_usdc"] = pd.to_numeric(
        economic["terminal_value_usdc"], errors="coerce"
    )
    if (
        economic["terminal_value_usdc"].isna().any()
        or not np.isfinite(economic["terminal_value_usdc"].to_numpy(dtype=float)).all()
    ):
        raise CooldownStudyError("exact economic labels are nonfinite")
    economic["strict_native_label"] = True
    return opportunities, economic


def _scope_days(sources: Sequence[DaySource], scope: str) -> tuple[str, ...]:
    if scope == "pooled50":
        return tuple(source.day for source in sources)
    return tuple(source.day for source in sources if source.panel_role == scope)


def _scope_denominator_binding(
    *,
    scope: str,
    exact_label_calendar_days: Sequence[str],
    formal_full_support_run: bool,
) -> dict[str, Any]:
    try:
        frozen = FORMAL_DAY_DENOMINATORS[scope]
    except KeyError as exc:
        raise CooldownStudyError(f"unknown reporting scope: {scope}") from exc
    exact_days = tuple(exact_label_calendar_days)
    expected_exact_count = int(frozen["exact_label_economic_days"])
    return {
        "scope_identity": str(frozen["report_key"]),
        "economic_statistics_denominator": "exact_label_economic",
        "nominal_mechanics": {
            "day_count": int(frozen["nominal_mechanics_days"]),
            "economic_statistics_bound": False,
            "purpose": "calendar_and_mechanics_reporting_only",
        },
        "exact_label_economic": {
            "formal_day_count": expected_exact_count,
            "observed_manifest_day_count": len(exact_days),
            "observed_manifest_days": list(exact_days),
            "formal_contract_satisfied": bool(
                formal_full_support_run and len(exact_days) == expected_exact_count
            ),
            "economic_statistics_bound": True,
            "strict_native_full_support_only": True,
        },
        "reduced_support_diagnostic": {
            "day_count": int(frozen["reduced_support_diagnostic_days"]),
            "economic_statistics_bound": False,
            "pooled_into_exact_label_economics": False,
            "economic_labels_manufactured": False,
        },
    }


def _day_denominator_report(
    *,
    sources: Sequence[DaySource],
    formal_full_support_run: bool,
) -> dict[str, Any]:
    bindings = {
        scope: _scope_denominator_binding(
            scope=scope,
            exact_label_calendar_days=_scope_days(sources, scope),
            formal_full_support_run=formal_full_support_run,
        )
        for scope in PANEL_SCOPES
    }
    return {
        "partition_contract": (
            "nominal_mechanics_equals_exact_label_economic_plus_"
            "reduced_support_diagnostic"
        ),
        "nominal_mechanics_denominator": {
            "prefix_days": 40,
            "added_days": 10,
            "pooled_days": 50,
            "economic_statistics_bound": False,
        },
        "exact_label_economic_denominator": {
            "prefix_days": 33,
            "added_days": 8,
            "pooled_days": 41,
            "observed_manifest_days": len(sources),
            "economic_statistics_bound": True,
            "only_source_for_reported_economic_statistics": True,
        },
        "reduced_support_diagnostic_denominator": {
            "prefix_days": 7,
            "added_days": 2,
            "pooled_days": 9,
            "economic_statistics_bound": False,
            "pooled_into_exact_label_economics": False,
            "economic_labels_manufactured": False,
        },
        "partition_checks": {
            "prefix_33_plus_7_equals_40": True,
            "added_8_plus_2_equals_10": True,
            "pooled_41_plus_9_equals_50": True,
        },
        "formal_full_support_contract_satisfied": bool(
            formal_full_support_run
            and all(
                binding["exact_label_economic"]["formal_contract_satisfied"]
                for binding in bindings.values()
            )
        ),
        "scope_bindings": {
            str(binding["scope_identity"]): binding for binding in bindings.values()
        },
    }


def _build_fold_plans(
    *,
    sources: Sequence[DaySource],
    economic: pd.DataFrame,
    config: StudyConfig,
) -> dict[tuple[str, str], FoldPlan]:
    plans: dict[tuple[str, str], FoldPlan] = {}
    for scope in PANEL_SCOPES:
        observed_days = _scope_days(sources, scope)
        for side in SIDES:
            side_rows = economic.loc[economic["side"].astype(str).str.upper() == side]
            if scope != "pooled50":
                side_rows = side_rows.loc[side_rows["panel_role"] == scope]
            exact_days = tuple(
                day for day in observed_days if day in set(side_rows["utc_day"].astype(str))
            )
            excluded = tuple(day for day in observed_days if day not in set(exact_days))
            reason: str | None = None
            folds: tuple[ChronologicalFold, ...] = ()
            if len(observed_days) - config.outer_minimum_train_days < config.outer_folds:
                reason = "insufficient_observed_calendar_days_for_frozen_outer_folds"
            else:
                try:
                    folds = expanding_chronological_folds(
                        observed_days,
                        fold_prefix=f"{scope}.{side}.outer",
                        n_folds=config.outer_folds,
                        minimum_train_days=config.outer_minimum_train_days,
                    )
                    for outer in folds:
                        inner_folds = expanding_chronological_folds(
                            outer.train_days,
                            fold_prefix=f"{outer.fold_id}.inner",
                            n_folds=config.search.inner_folds,
                            minimum_train_days=config.search.inner_minimum_train_days,
                        )
                        for fold in (*inner_folds, outer):
                            if not set(fold.test_days).intersection(exact_days):
                                raise CooldownStudyError(
                                    f"frozen calendar fold has zero exact economic rows: "
                                    f"{fold.fold_id}/{side}"
                                )
                except (NestedOofContractError, CooldownStudyError) as exc:
                    reason = str(exc)
                    folds = ()
            plans[(scope, side)] = FoldPlan(
                panel_scope=scope,
                side=side,
                observed_days=observed_days,
                exact_label_days=exact_days,
                excluded_days=excluded,
                outer_folds=folds,
                unavailable_reason=reason,
            )
    return plans


def _m0_required_fold_ids(plan: FoldPlan, config: StudyConfig) -> dict[str, tuple[str, ...]]:
    required: dict[str, tuple[str, ...]] = {}
    for outer in plan.outer_folds:
        required[outer.fold_id] = outer.train_days
        inner_folds = expanding_chronological_folds(
            outer.train_days,
            fold_prefix=f"{outer.fold_id}.inner",
            n_folds=config.search.inner_folds,
            minimum_train_days=config.search.inner_minimum_train_days,
        )
        for inner in inner_folds:
            required[inner.fold_id] = inner.train_days
    return required


def _validate_bundle_fold_coverage(
    bundle: LoadedPredicateBundle,
    *,
    plans: Mapping[tuple[str, str], FoldPlan],
    config: StudyConfig,
) -> None:
    expected: dict[tuple[str, str, str], tuple[str, ...]] = {}
    for (scope, side), plan in plans.items():
        for fold_id, days in _m0_required_fold_ids(plan, config).items():
            expected[(scope, side, fold_id)] = days
    missing = sorted(set(expected) - set(bundle.m0))
    extra = sorted(set(bundle.m0) - set(expected))
    if missing or extra:
        raise CooldownStudyError(
            f"M0 fold artifact coverage drifted: missing={missing}, extra={extra}"
        )
    for key, days in expected.items():
        _validate_m0_artifact(bundle.m0[key], side=key[1], expected_train_days=days)


def _m0_reference_frame(
    denominator: pd.DataFrame,
    *,
    side: str,
    train_days: Sequence[str],
) -> pd.DataFrame:
    required = ("utc_day", "snapshot_id", *M0_REQUIRED_FIELDS)
    missing = sorted(set(required) - set(denominator))
    if missing:
        raise CooldownStudyError(f"denominator lacks M0 reference fields: {missing}")
    rows = denominator.loc[
        denominator["utc_day"].astype(str).isin(set(train_days))
        & denominator["side"].astype(str).str.upper().eq(side),
        list(required),
    ].copy()
    if rows.empty:
        raise CooldownStudyError("fold-local M0 reference frame is empty")
    rows["utc_day"] = rows["utc_day"].astype(str)
    rows["side"] = rows["side"].astype(str).str.upper()
    rows = rows.sort_values(["utc_day", "snapshot_id"], kind="stable").reset_index(drop=True)
    if tuple(rows["utc_day"].drop_duplicates()) != tuple(train_days):
        raise CooldownStudyError("fold-local M0 reference omits a frozen training day")
    return rows


def _reference_frame_identity(frame: pd.DataFrame) -> str:
    schema = tuple((str(name), str(frame[name].dtype)) for name in frame)
    row_hashes = pd.util.hash_pandas_object(frame, index=False).to_numpy(dtype="<u8")
    digest = hashlib.sha256()
    digest.update(_canonical_json(schema).encode("ascii"))
    digest.update(row_hashes.tobytes(order="C"))
    return digest.hexdigest()


def _fit_fold_local_m0_artifacts(
    *,
    bundle: LoadedPredicateBundle,
    denominator: pd.DataFrame,
    plans: Mapping[tuple[str, str], FoldPlan],
    config: StudyConfig,
    artifact_root: Path,
) -> LoadedPredicateBundle:
    expected: dict[tuple[str, str, str], tuple[str, ...]] = {}
    for (scope, side), plan in plans.items():
        for fold_id, days in _m0_required_fold_ids(plan, config).items():
            expected[(scope, side, fold_id)] = days
    artifacts: dict[tuple[str, str, str], PredicateArtifact] = {}
    references = dict(bundle.artifact_references)
    for key, train_days in sorted(expected.items()):
        scope, side, fold_id = key
        frame = _m0_reference_frame(denominator, side=side, train_days=train_days)
        reference_identity = _reference_frame_identity(frame)
        try:
            artifact = fit_predicate_artifact(
                frame,
                side=side,
                source_role="inner_chronological_development",
                reference_identity_sha256=reference_identity,
                reference_days=train_days,
                source_clock_identity="strict_native_assignment_visible_context_v1",
            )
        except PredicateContractError as exc:
            raise CooldownStudyError(f"fold-local M0 fit failed: {key}") from exc
        safe_fold = hashlib.sha256(fold_id.encode("utf-8")).hexdigest()[:16]
        path = artifact_root / scope / side / f"{safe_fold}.json"
        if path.exists():
            existing = _load_predicate_artifact(
                ArtifactReference(path=path, sha256=_sha256(path))
            )
            if existing != artifact:
                raise CooldownStudyError(f"resumed M0 artifact drifted: {key}")
        else:
            _atomic_json(path, artifact.to_dict())
        reference = ArtifactReference(path=path.resolve(), sha256=_sha256(path))
        artifacts[key] = artifact
        references[f"m0:{scope}:{side}:{fold_id}"] = reference
    fitted = replace(bundle, m0=artifacts, artifact_references=references)
    _validate_bundle_fold_coverage(fitted, plans=plans, config=config)
    return fitted


def preflight_study(
    *,
    formal_panel_manifest: Path,
    predicate_bundle: Path,
    config: StudyConfig | None = None,
) -> dict[str, Any]:
    """Validate immutable inputs without opening economic label payloads."""

    settings = config or StudyConfig()
    formal, sources = _validate_formal_panel_manifest(
        formal_panel_manifest,
        allow_engineering_panel=settings.engineering_allow_nonformal_panel,
    )
    bundle = _load_predicate_bundle(predicate_bundle)
    return {
        "schema_version": f"{STUDY_IDENTITY}.preflight.v1",
        "identity": STUDY_IDENTITY,
        "formal_panel_manifest": {
            "path": str(Path(formal_panel_manifest).resolve()),
            "sha256": _sha256(Path(formal_panel_manifest).resolve()),
            "declared_day_count": int(formal["day_count"]),
            "observed_day_count": len(sources),
            "prefix40_role_count": sum(source.panel_role == "prefix40" for source in sources),
            "added10_role_count": sum(source.panel_role == "added10" for source in sources),
        },
        "predicate_bundle": {
            "path": str(bundle.path),
            "sha256": bundle.sha256,
            "canonical_sha256": bundle.canonical_sha256,
            "book_trade_artifacts_clock_separated": True,
        },
        "config": settings.payload(),
        "economic_outcomes_read": False,
        "strict_day_panels_opened": False,
        "action_authorized": False,
        "live_authorized": False,
    }


def _artifact_input_frame(features: pd.DataFrame, artifact: PredicateArtifact) -> pd.DataFrame:
    columns = tuple(name for name, _ in artifact.input_schema)
    missing = sorted(set(columns) - set(features))
    if missing:
        raise CooldownStudyError(f"admitted feature rows lack artifact inputs: {missing[:12]}")
    return features.loc[:, columns].copy()


def _transform_artifact(
    features: pd.DataFrame, artifact: PredicateArtifact
) -> tuple[pd.DataFrame, dict[str, tuple[str, ...]]]:
    try:
        transformed = artifact.transform(
            _artifact_input_frame(features, artifact),
            expected_artifact_sha256=artifact.canonical_sha256,
        )
    except PredicateContractError as exc:
        raise CooldownStudyError("predicate transform failed closed") from exc
    columns = transformed.columns.copy()
    columns.index = features.index
    return columns, dict(transformed.block_mapping)


def _predicate_group_map(
    book: PredicateArtifact,
    trade: PredicateArtifact,
    m0: PredicateArtifact,
) -> dict[str, str]:
    output: dict[str, str] = {}
    selected = (
        (book, {"book"}),
        (trade, {"trade"}),
        (m0, {"context"}),
    )
    for artifact, allowed_groups in selected:
        for definition in artifact.definitions:
            if definition.clock_group not in allowed_groups:
                continue
            prior = output.get(definition.name)
            if prior is not None and prior != definition.clock_group:
                raise CooldownStudyError("predicate clock group collides across artifacts")
            output[definition.name] = definition.clock_group
    return output


def _combine_predicate_view(
    *,
    features: pd.DataFrame,
    book_artifact: PredicateArtifact,
    trade_artifact: PredicateArtifact,
    m0_artifact: PredicateArtifact,
) -> tuple[pd.DataFrame, dict[str, tuple[str, ...]], dict[str, str]]:
    book_all, _ = _transform_artifact(features, book_artifact)
    trade_all, _ = _transform_artifact(features, trade_artifact)
    m0_all, _ = _transform_artifact(features, m0_artifact)
    book_definitions = tuple(
        definition for definition in book_artifact.definitions if definition.clock_group == "book"
    )
    trade_definitions = tuple(
        definition for definition in trade_artifact.definitions if definition.clock_group == "trade"
    )
    m0_definitions = tuple(
        definition for definition in m0_artifact.definitions if definition.clock_group == "context"
    )
    book = book_all.loc[:, [definition.name for definition in book_definitions]]
    trade = trade_all.loc[:, [definition.name for definition in trade_definitions]]
    m0 = m0_all.loc[:, [definition.name for definition in m0_definitions]]
    collisions = (set(book) & set(trade)) | (set(book) & set(m0)) | (set(trade) & set(m0))
    if collisions:
        raise CooldownStudyError(
            f"predicate names collide across source artifacts: {sorted(collisions)[:12]}"
        )
    combined = pd.concat([book, trade, m0], axis=1)
    r0 = {definition.name for definition in book_definitions if definition.block == "R0"}
    m0_columns = {definition.name for definition in m0_definitions}
    m1 = m0_columns | {
        definition.name for definition in book_definitions if definition.block in {"R0", "M1"}
    }
    m2 = (
        m1
        | {definition.name for definition in book_definitions}
        | {definition.name for definition in trade_definitions}
    )
    blocks = {
        "R0": tuple(sorted(r0)),
        "M0": tuple(sorted(m0_columns)),
        "M1": tuple(sorted(m1)),
        "M2": tuple(sorted(m2)),
    }
    if any(not blocks[block] for block in FEATURE_BLOCKS):
        raise CooldownStudyError("R0/M0/M1/M2 predicate view cannot be empty")
    if not set(blocks["M0"]) <= set(blocks["M1"]) <= set(blocks["M2"]):
        raise CooldownStudyError("M0/M1/M2 predicate views are not cumulative")
    return combined, blocks, _predicate_group_map(book_artifact, trade_artifact, m0_artifact)


def _continuous_feature_spec(
    *,
    book_artifact: PredicateArtifact,
    trade_artifact: PredicateArtifact,
    m0_artifact: PredicateArtifact,
    feature_block: str,
) -> ContinuousFeatureSpec:
    """Recover raw source fields without reusing quantile predicate values."""

    if feature_block not in FEATURE_BLOCKS:
        raise CooldownStudyError("unknown continuous comparator feature block")
    book_definitions = tuple(
        definition
        for definition in book_artifact.definitions
        if definition.clock_group == "book"
    )
    trade_definitions = tuple(
        definition
        for definition in trade_artifact.definitions
        if definition.clock_group == "trade"
    )
    m0_definitions = tuple(
        definition
        for definition in m0_artifact.definitions
        if definition.clock_group == "context"
    )
    if feature_block == "R0":
        selected = tuple(
            definition for definition in book_definitions if definition.block == "R0"
        )
    elif feature_block == "M0":
        selected = m0_definitions
    elif feature_block == "M1":
        selected = m0_definitions + tuple(
            definition
            for definition in book_definitions
            if definition.block in {"R0", "M1"}
        )
    else:
        selected = m0_definitions + book_definitions + trade_definitions
    numeric: set[str] = set()
    categorical: dict[str, set[str]] = {}
    for definition in selected:
        field = str(definition.source_field)
        if definition.kind == "categorical_equals":
            if definition.category is None:
                raise CooldownStudyError("categorical comparator feature lacks a category")
            categorical.setdefault(field, set()).add(str(definition.category).lower())
        else:
            numeric.add(field)
    collisions = numeric & set(categorical)
    if collisions:
        raise CooldownStudyError(
            f"raw comparator field has numeric and categorical definitions: {sorted(collisions)}"
        )
    return ContinuousFeatureSpec(
        numeric_fields=tuple(sorted(numeric)),
        categorical_fields=tuple(
            (field, tuple(sorted(categories)))
            for field, categories in sorted(categorical.items())
        ),
    )


def _continuous_feature_spec_for_fold(
    *,
    bundle: LoadedPredicateBundle,
    scope: str,
    side: str,
    fold_id: str,
    feature_block: str,
) -> ContinuousFeatureSpec:
    key = (scope, side, fold_id)
    if key not in bundle.m0:
        raise CooldownStudyError(f"M0 fold artifact is missing: {key}")
    return _continuous_feature_spec(
        book_artifact=bundle.book[side],
        trade_artifact=bundle.trade[side],
        m0_artifact=bundle.m0[key],
        feature_block=feature_block,
    )


def _continuous_matrix_pair(
    *,
    train: pd.DataFrame,
    test: pd.DataFrame,
    spec: ContinuousFeatureSpec,
) -> tuple[np.ndarray, np.ndarray, tuple[str, ...]]:
    required = set(spec.numeric_fields) | {
        field for field, _ in spec.categorical_fields
    }
    missing = sorted(required - set(train))
    missing_test = sorted(required - set(test))
    if missing or missing_test:
        raise CooldownStudyError(
            f"raw comparator fields are missing: train={missing[:8]} test={missing_test[:8]}"
        )
    train_columns: list[np.ndarray] = []
    test_columns: list[np.ndarray] = []
    feature_names: list[str] = []
    for field in spec.numeric_fields:
        train_numeric = pd.to_numeric(train[field], errors="coerce").to_numpy(dtype=float)
        test_numeric = pd.to_numeric(test[field], errors="coerce").to_numpy(dtype=float)
        train_missing = ~np.isfinite(train_numeric)
        test_missing = ~np.isfinite(test_numeric)
        finite_train = train_numeric[~train_missing]
        median = float(np.median(finite_train)) if len(finite_train) else 0.0
        train_columns.extend(
            [
                np.where(train_missing, median, train_numeric),
                train_missing.astype(float),
            ]
        )
        test_columns.extend(
            [
                np.where(test_missing, median, test_numeric),
                test_missing.astype(float),
            ]
        )
        feature_names.extend((f"raw::{field}", f"missing::{field}"))
    for field, categories in spec.categorical_fields:
        train_values = train[field].astype("string").str.lower()
        test_values = test[field].astype("string").str.lower()
        train_missing = train_values.isna().to_numpy()
        test_missing = test_values.isna().to_numpy()
        for category in categories:
            train_columns.append((train_values == category).fillna(False).to_numpy(dtype=float))
            test_columns.append((test_values == category).fillna(False).to_numpy(dtype=float))
            feature_names.append(f"category::{field}::{category}")
        train_columns.append(train_missing.astype(float))
        test_columns.append(test_missing.astype(float))
        feature_names.append(f"missing::{field}")
    train_matrix = np.column_stack(train_columns).astype(float, copy=False)
    test_matrix = np.column_stack(test_columns).astype(float, copy=False)
    if not np.isfinite(train_matrix).all() or not np.isfinite(test_matrix).all():
        raise CooldownStudyError("raw comparator matrix contains a nonfinite value")
    names = tuple(feature_names)
    if len(names) != len(set(names)) or train_matrix.shape[1] != len(names):
        raise CooldownStudyError("raw comparator feature identity drifted")
    return train_matrix, test_matrix, names


def _policy_respects_clock_groups(
    policy: BooleanCooldownPolicy, predicate_groups: Mapping[str, str]
) -> bool:
    """Validate predicate ownership without banning a strict target-time join.

    The supplied 2025 artifacts were fitted independently.  Their thresholds
    retain separate source clocks.  The policy itself is evaluated only on a
    2026 strict-native row whose book and trade states share the assignment
    cutoff, so a mixed AND clause is legal when every literal is observed.
    """

    accepted = {"book", "trade", "context"}
    predicates = {
        literal.predicate
        for rule in policy.rules
        for clause in rule.clauses
        for literal in clause.literals
    }
    return not (predicates - set(predicate_groups)) and all(
        str(predicate_groups[predicate]) in accepted for predicate in predicates
    )


def _bounded_clock_safe_candidates(
    *,
    side: str,
    predicate_columns: Sequence[str],
    predicate_groups: Mapping[str, str],
    config: SearchConfig,
) -> tuple[BooleanCooldownPolicy, ...]:
    missing = set(predicate_columns) - set(predicate_groups)
    if missing:
        raise CooldownStudyError(
            f"predicate clock group mapping is incomplete: {sorted(missing)}"
        )
    accepted = {"book", "trade", "context"}
    invalid = {
        str(predicate_groups[predicate])
        for predicate in predicate_columns
        if str(predicate_groups[predicate]) not in accepted
    }
    if invalid:
        raise CooldownStudyError(
            f"predicate clock group mapping contains invalid values: {sorted(invalid)}"
        )
    bounded_groups = {
        predicate: str(predicate_groups[predicate]) for predicate in predicate_columns
    }
    candidates = generate_bounded_candidates(
        side=side,
        predicate_columns=predicate_columns,
        config=config,
        predicate_clock_groups=bounded_groups,
    )
    safe = tuple(policy for policy in candidates if _policy_respects_clock_groups(policy, predicate_groups))
    if not safe:
        raise CooldownStudyError("clock-safe candidate universe is empty")
    return safe


def _campaign_weights(rows: pd.DataFrame) -> pd.Series:
    counts = rows.groupby("campaign_cluster_id", observed=True)["opportunity_id"].transform("count")
    weights = 1.0 / counts.astype(float)
    check = weights.groupby(rows["campaign_cluster_id"], observed=True).sum()
    if not np.allclose(check.to_numpy(dtype=float), 1.0, atol=1e-12, rtol=0.0):
        raise CooldownStudyError("campaign total OOF weight drifted from one")
    return weights


def _reweight(rows: pd.DataFrame) -> pd.DataFrame:
    output = rows.copy()
    output["campaign_weight"] = _campaign_weights(output)
    return output


def _build_evaluation_context(
    *,
    economic: pd.DataFrame,
    predicate_view: pd.DataFrame,
    predicate_columns: Sequence[str],
    side: str,
    panel_scope: str,
    days: Sequence[str],
) -> EvaluationContext:
    day_set = set(days)
    side_rows = economic.loc[
        (economic["side"].astype(str).str.upper() == side)
        & economic["utc_day"].astype(str).isin(day_set)
    ].copy()
    if panel_scope != "pooled50":
        side_rows = side_rows.loc[side_rows["panel_role"] == panel_scope]
    if side_rows.empty:
        raise CooldownStudyError("evaluation context has no exact labels")
    opportunities = (
        side_rows[
            [
                "opportunity_id",
                "utc_day",
                "panel_role",
                "side",
                "role_at_fill",
                "campaign_id",
            ]
        ]
        .drop_duplicates("opportunity_id")
        .set_index("opportunity_id")
        .sort_values(["utc_day", "campaign_id"], kind="stable")
    )
    if opportunities.index.duplicated().any():
        raise CooldownStudyError("economic opportunity identity is duplicated")
    missing_predicates = sorted(set(opportunities.index) - set(predicate_view.index))
    if missing_predicates:
        raise CooldownStudyError("predicate view omits exact economic opportunities")
    predicate_rows = predicate_view.loc[opportunities.index, list(predicate_columns)]
    vocabulary = duration_vocabulary(side)
    outcomes = side_rows.pivot(
        index="opportunity_id",
        columns="duration_policy_id",
        values="terminal_value_usdc",
    ).loc[opportunities.index, list(vocabulary)]
    return EvaluationContext(
        side=side,
        panel_scope=panel_scope,
        predicate_columns=tuple(predicate_columns),
        metadata=opportunities,
        predicates=predicate_rows,
        outcomes=outcomes,
    )


def _evaluate_context(
    context: EvaluationContext,
    *,
    policy: BooleanCooldownPolicy,
    fold_id: str,
    stage: str,
) -> pd.DataFrame:
    opportunities = context.metadata
    actions = policy.choose(context.predicates)
    chosen = np.fromiter(
        (
            float(context.outcomes.at[opportunity_id, action])
            for opportunity_id, action in zip(opportunities.index, actions, strict=True)
        ),
        dtype=float,
        count=len(opportunities),
    )
    control = context.outcomes[CONTROL_ACTION].to_numpy(dtype=float, copy=False)
    rows = pd.DataFrame(
        {
            "opportunity_id": opportunities.index.astype(str),
            "utc_day": opportunities["utc_day"].to_numpy(dtype=object),
            "panel_role": opportunities["panel_role"].to_numpy(dtype=object),
            "side": context.side,
            "role_at_fill": opportunities["role_at_fill"].to_numpy(dtype=object),
            "campaign_cluster_id": opportunities["campaign_id"].to_numpy(dtype=object),
            "selected_action": actions,
            "control_action": CONTROL_ACTION,
            "selected_nonbaseline": actions != CONTROL_ACTION,
            "selected_value_usdc": chosen,
            "control_value_usdc": control,
            "uplift_usdc": chosen - control,
            "candidate_id": policy.candidate_id,
            "fold_id": fold_id,
            "evaluation_stage": stage,
        }
    )
    return _reweight(rows)


def _evaluate_policy(
    *,
    economic: pd.DataFrame,
    predicate_view: pd.DataFrame,
    predicate_columns: Sequence[str],
    policy: BooleanCooldownPolicy,
    side: str,
    panel_scope: str,
    days: Sequence[str],
    fold_id: str,
    stage: str,
) -> pd.DataFrame:
    context = _build_evaluation_context(
        economic=economic,
        predicate_view=predicate_view,
        predicate_columns=predicate_columns,
        side=side,
        panel_scope=panel_scope,
        days=days,
    )
    return _evaluate_context(context, policy=policy, fold_id=fold_id, stage=stage)


def _build_continuous_context(
    *,
    economic: pd.DataFrame,
    features: pd.DataFrame,
    spec: ContinuousFeatureSpec,
    side: str,
    panel_scope: str,
    days: Sequence[str],
) -> EvaluationContext:
    required = tuple(spec.numeric_fields) + tuple(
        field for field, _ in spec.categorical_fields
    )
    placeholder = pd.DataFrame(index=features.index, columns=required)
    context = _build_evaluation_context(
        economic=economic,
        predicate_view=placeholder,
        predicate_columns=required,
        side=side,
        panel_scope=panel_scope,
        days=days,
    )
    raw = features.loc[context.metadata.index, list(required)].copy()
    return EvaluationContext(
        side=context.side,
        panel_scope=context.panel_scope,
        predicate_columns=required,
        metadata=context.metadata,
        predicates=raw,
        outcomes=context.outcomes,
    )


def _continuous_tree_rows(
    *,
    train_context: EvaluationContext,
    test_context: EvaluationContext,
    spec: ContinuousFeatureSpec,
    max_depth: int,
    config: ContinuousComparatorConfig,
    fold_id: str,
    stage: str,
) -> tuple[pd.DataFrame, tuple[str, ...], str]:
    train_matrix, test_matrix, feature_names = _continuous_matrix_pair(
        train=train_context.predicates,
        test=test_context.predicates,
        spec=spec,
    )
    vocabulary = duration_vocabulary(train_context.side)
    if vocabulary != duration_vocabulary(test_context.side):
        raise CooldownStudyError("continuous comparator side vocabulary drifted")
    noncontrol = vocabulary[1:]
    train_control = train_context.outcomes[CONTROL_ACTION].to_numpy(
        dtype=float, copy=False
    )
    train_targets = (
        train_context.outcomes.loc[:, list(noncontrol)].to_numpy(dtype=float)
        - train_control[:, None]
    )
    weight_rows = pd.DataFrame(
        {
            "opportunity_id": train_context.metadata.index.astype(str),
            "campaign_cluster_id": train_context.metadata["campaign_id"].to_numpy(
                dtype=object
            ),
        }
    )
    weights = _campaign_weights(weight_rows).to_numpy(dtype=float, copy=False)
    model = DecisionTreeRegressor(
        criterion="squared_error",
        splitter="best",
        max_depth=max_depth,
        min_samples_leaf=config.min_samples_leaf,
        random_state=config.random_state,
    )
    model.fit(train_matrix, train_targets, sample_weight=weights)
    prediction = np.asarray(model.predict(test_matrix), dtype=float)
    if prediction.ndim == 1:
        prediction = prediction.reshape(-1, 1)
    if prediction.shape != (len(test_context.metadata), len(noncontrol)):
        raise CooldownStudyError("continuous comparator prediction shape drifted")
    if not np.isfinite(prediction).all():
        raise CooldownStudyError("continuous comparator emitted a nonfinite prediction")
    predicted_uplift = np.column_stack(
        [np.zeros(len(test_context.metadata), dtype=float), prediction]
    )
    action_indices = np.argmax(predicted_uplift, axis=1)
    actions = np.asarray(vocabulary, dtype=object)[action_indices]
    chosen = np.fromiter(
        (
            float(test_context.outcomes.at[opportunity_id, action])
            for opportunity_id, action in zip(
                test_context.metadata.index, actions, strict=True
            )
        ),
        dtype=float,
        count=len(test_context.metadata),
    )
    control = test_context.outcomes[CONTROL_ACTION].to_numpy(dtype=float, copy=False)
    model_payload = {
        "identity": f"{STUDY_IDENTITY}.continuous_comparator.v1",
        "side": train_context.side,
        "feature_spec": spec.payload(),
        "raw_feature_names": list(feature_names),
        "max_depth": max_depth,
        "min_samples_leaf": config.min_samples_leaf,
        "random_state": config.random_state,
        "training_days": sorted(
            set(train_context.metadata["utc_day"].astype(str).tolist())
        ),
        "target": "seven_noncontrol_assignment_to_washout_uplifts_vs_CONTROL_85N",
        "control_score_fixed_zero": True,
    }
    model_id = _canonical_sha256(model_payload)
    rows = pd.DataFrame(
        {
            "opportunity_id": test_context.metadata.index.astype(str),
            "utc_day": test_context.metadata["utc_day"].to_numpy(dtype=object),
            "panel_role": test_context.metadata["panel_role"].to_numpy(dtype=object),
            "side": test_context.side,
            "role_at_fill": test_context.metadata["role_at_fill"].to_numpy(
                dtype=object
            ),
            "campaign_cluster_id": test_context.metadata["campaign_id"].to_numpy(
                dtype=object
            ),
            "selected_action": actions,
            "control_action": CONTROL_ACTION,
            "selected_nonbaseline": actions != CONTROL_ACTION,
            "selected_value_usdc": chosen,
            "control_value_usdc": control,
            "uplift_usdc": chosen - control,
            "candidate_id": model_id,
            "fold_id": fold_id,
            "evaluation_stage": stage,
            "model_family": "raw_state_multioutput_regression_tree_diagnostic",
            "max_depth": max_depth,
            "raw_feature_count": len(feature_names),
            "raw_feature_schema_sha256": _canonical_sha256(list(feature_names)),
        }
    )
    return _reweight(rows), feature_names, model_id


def _select_continuous_capacity(
    *,
    economic: pd.DataFrame,
    features: pd.DataFrame,
    bundle: LoadedPredicateBundle,
    plan: FoldPlan,
    outer: ChronologicalFold,
    feature_block: str,
    config: StudyConfig,
) -> tuple[int, ClusteredEstimate]:
    inner_folds = expanding_chronological_folds(
        outer.train_days,
        fold_prefix=f"{outer.fold_id}.inner",
        n_folds=config.search.inner_folds,
        minimum_train_days=config.search.inner_minimum_train_days,
    )
    ranked: list[tuple[float, int, ClusteredEstimate]] = []
    for max_depth in config.continuous_comparator.max_depth_candidates:
        inner_rows: list[pd.DataFrame] = []
        for inner in inner_folds:
            spec = _continuous_feature_spec_for_fold(
                bundle=bundle,
                scope=plan.panel_scope,
                side=plan.side,
                fold_id=inner.fold_id,
                feature_block=feature_block,
            )
            train_context = _build_continuous_context(
                economic=economic,
                features=features,
                spec=spec,
                side=plan.side,
                panel_scope=plan.panel_scope,
                days=inner.train_days,
            )
            test_context = _build_continuous_context(
                economic=economic,
                features=features,
                spec=spec,
                side=plan.side,
                panel_scope=plan.panel_scope,
                days=inner.test_days,
            )
            rows, _, _ = _continuous_tree_rows(
                train_context=train_context,
                test_context=test_context,
                spec=spec,
                max_depth=max_depth,
                config=config.continuous_comparator,
                fold_id=inner.fold_id,
                stage="continuous_inner_oof",
            )
            inner_rows.append(rows)
        combined = _reweight(pd.concat(inner_rows, ignore_index=True))
        estimate = clustered_estimate(
            combined, confidence=config.search.confidence
        )
        ranked.append((-estimate.mean_usdc, max_depth, estimate))
    ranked.sort(key=lambda row: (row[0], row[1]))
    _, selected_depth, estimate = ranked[0]
    return selected_depth, estimate


def _run_continuous_comparator(
    *,
    economic: pd.DataFrame,
    features: pd.DataFrame,
    bundle: LoadedPredicateBundle,
    plan: FoldPlan,
    feature_block: str,
    config: StudyConfig,
) -> ContinuousComparatorResult:
    outer_rows: list[pd.DataFrame] = []
    fold_reports: list[Mapping[str, Any]] = []
    all_feature_names: set[str] = set()
    for outer in plan.outer_folds:
        selected_depth, inner_estimate = _select_continuous_capacity(
            economic=economic,
            features=features,
            bundle=bundle,
            plan=plan,
            outer=outer,
            feature_block=feature_block,
            config=config,
        )
        spec = _continuous_feature_spec_for_fold(
            bundle=bundle,
            scope=plan.panel_scope,
            side=plan.side,
            fold_id=outer.fold_id,
            feature_block=feature_block,
        )
        train_context = _build_continuous_context(
            economic=economic,
            features=features,
            spec=spec,
            side=plan.side,
            panel_scope=plan.panel_scope,
            days=outer.train_days,
        )
        test_context = _build_continuous_context(
            economic=economic,
            features=features,
            spec=spec,
            side=plan.side,
            panel_scope=plan.panel_scope,
            days=outer.test_days,
        )
        rows, feature_names, model_id = _continuous_tree_rows(
            train_context=train_context,
            test_context=test_context,
            spec=spec,
            max_depth=selected_depth,
            config=config.continuous_comparator,
            fold_id=outer.fold_id,
            stage="continuous_outer_oof",
        )
        all_feature_names.update(feature_names)
        outer_rows.append(rows)
        fold_reports.append(
            {
                "fold_id": outer.fold_id,
                "train_days": list(outer.train_days),
                "test_days": list(outer.test_days),
                "selected_max_depth": selected_depth,
                "inner_estimate": asdict(inner_estimate),
                "outer_model_id": model_id,
                "raw_feature_count": len(feature_names),
                "candidate_replaced_by_baseline_before_outer_oof": False,
            }
        )
    combined = _reweight(pd.concat(outer_rows, ignore_index=True))
    if combined["opportunity_id"].duplicated().any():
        raise CooldownStudyError(
            "continuous outer OOF opportunity appears more than once"
        )
    return ContinuousComparatorResult(
        side=plan.side,
        feature_block=feature_block,
        panel_scope=plan.panel_scope,
        oof_rows=combined,
        fold_reports=tuple(fold_reports),
        raw_feature_names=tuple(sorted(all_feature_names)),
    )


def _continuous_result_report(
    result: ContinuousComparatorResult,
    *,
    confidence: float,
    search_config: SearchConfig,
) -> dict[str, Any]:
    estimate = clustered_estimate(result.oof_rows, confidence=confidence)
    support = _support(result.oof_rows, search_config)
    return _json_safe(
        {
            "status": "completed",
            "identity": f"{STUDY_IDENTITY}.continuous_comparator.v1",
            "model_family": "raw_state_multioutput_regression_tree_diagnostic",
            "side": result.side,
            "feature_block": result.feature_block,
            "panel_scope": result.panel_scope,
            "raw_feature_count_union": len(result.raw_feature_names),
            "raw_feature_schema_sha256": _canonical_sha256(
                list(result.raw_feature_names)
            ),
            "folds": list(result.fold_reports),
            "action_rate": float(result.oof_rows["selected_nonbaseline"].mean()),
            "support_diagnostic": asdict(support),
            "campaign_weighted_day_cluster_uplift": asdict(estimate),
            "opener_add_support": role_support_audit(
                result.oof_rows, confidence=confidence
            ),
            "uses_raw_unquantized_source_fields": True,
            "boolean_policy_replacement_authorized": False,
            "action_authorized": False,
            "live_authorized": False,
        }
    )


def _support(rows: pd.DataFrame, config: SearchConfig) -> SupportAudit:
    acted = rows.loc[rows["selected_nonbaseline"]]
    opportunities = int(len(acted))
    campaigns = int(acted["campaign_cluster_id"].nunique())
    days = int(acted["utc_day"].nunique())
    return SupportAudit(
        action_opportunities=opportunities,
        action_campaigns=campaigns,
        action_days=days,
        action_rate=float(opportunities / len(rows)) if len(rows) else 0.0,
        passed=bool(
            opportunities >= config.minimum_action_opportunities
            and campaigns >= config.minimum_action_campaigns
            and days >= config.minimum_action_days
        ),
    )


def _panel_role_support(rows: pd.DataFrame) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for role in ("prefix40", "added10"):
        subset = rows.loc[rows["panel_role"] == role]
        acted = subset.loc[subset["selected_nonbaseline"]]
        output[role] = {
            "opportunities": int(len(subset)),
            "campaigns": int(subset["campaign_cluster_id"].nunique()),
            "days": int(subset["utc_day"].nunique()),
            "action_opportunities": int(len(acted)),
            "action_rate": float(len(acted) / len(subset)) if len(subset) else 0.0,
            "campaign_weighted_day_cluster_uplift": (
                None
                if subset.empty
                else asdict(
                    clustered_estimate(_reweight(subset))
                )
            ),
        }
    return output


def _predicate_view_for_fold(
    *,
    features: pd.DataFrame,
    bundle: LoadedPredicateBundle,
    scope: str,
    side: str,
    fold_id: str,
) -> tuple[pd.DataFrame, dict[str, tuple[str, ...]], dict[str, str]]:
    key = (scope, side, fold_id)
    if key not in bundle.m0:
        raise CooldownStudyError(f"M0 fold artifact is missing: {key}")
    return _combine_predicate_view(
        features=features,
        book_artifact=bundle.book[side],
        trade_artifact=bundle.trade[side],
        m0_artifact=bundle.m0[key],
    )


def _select_candidate(
    *,
    economic: pd.DataFrame,
    features: pd.DataFrame,
    bundle: LoadedPredicateBundle,
    plan: FoldPlan,
    outer: ChronologicalFold,
    feature_block: str,
    config: StudyConfig,
) -> tuple[BooleanCooldownPolicy, ClusteredEstimate, SupportAudit, int]:
    outer_view, outer_blocks, group_map = _predicate_view_for_fold(
        features=features,
        bundle=bundle,
        scope=plan.panel_scope,
        side=plan.side,
        fold_id=outer.fold_id,
    )
    predicate_columns = outer_blocks[feature_block]
    candidates = _bounded_clock_safe_candidates(
        side=plan.side,
        predicate_columns=predicate_columns,
        predicate_groups=group_map,
        config=config.search,
    )
    inner_folds = expanding_chronological_folds(
        outer.train_days,
        fold_prefix=f"{outer.fold_id}.inner",
        n_folds=config.search.inner_folds,
        minimum_train_days=config.search.inner_minimum_train_days,
    )
    inner_contexts: dict[str, tuple[EvaluationContext, dict[str, str]]] = {}
    for inner in inner_folds:
        view = _predicate_view_for_fold(
            features=features,
            bundle=bundle,
            scope=plan.panel_scope,
            side=plan.side,
            fold_id=inner.fold_id,
        )
        if view[1][feature_block] != predicate_columns:
            raise CooldownStudyError("fold-specific M0 artifacts changed predicate identities")
        context = _build_evaluation_context(
            economic=economic,
            predicate_view=view[0],
            predicate_columns=predicate_columns,
            side=plan.side,
            panel_scope=plan.panel_scope,
            days=inner.test_days,
        )
        inner_contexts[inner.fold_id] = (context, view[2])
    ranked: list[
        tuple[
            float,
            tuple[int, int, int],
            str,
            BooleanCooldownPolicy,
            ClusteredEstimate,
            SupportAudit,
        ]
    ] = []
    for policy in candidates:
        inner_rows: list[pd.DataFrame] = []
        for inner in inner_folds:
            context, inner_groups = inner_contexts[inner.fold_id]
            if not _policy_respects_clock_groups(policy, inner_groups):
                raise CooldownStudyError("inner policy contains an unbound predicate")
            inner_rows.append(
                _evaluate_context(
                    context,
                    policy=policy,
                    fold_id=inner.fold_id,
                    stage="inner_oof",
                )
            )
        combined = _reweight(pd.concat(inner_rows, ignore_index=True))
        support = _support(combined, config.search)
        if not support.passed:
            continue
        estimate = clustered_estimate(combined, confidence=config.search.confidence)
        ranked.append(
            (
                -estimate.mean_usdc,
                policy.complexity,
                policy.candidate_id,
                policy,
                estimate,
                support,
            )
        )
    if not ranked:
        raise CooldownStudyError(
            f"no supported nonbaseline inner candidate for {outer.fold_id}/{feature_block}"
        )
    ranked.sort(key=lambda row: (row[0], row[1], row[2]))
    _, _, _, selected, estimate, support = ranked[0]
    if not _policy_respects_clock_groups(selected, group_map):
        raise CooldownStudyError("selected policy contains an unbound predicate")
    # The caller evaluates this exact object on the outer fold. It is never
    # replaced by CONTROL_85N because an inner confidence interval crosses zero.
    return selected, estimate, support, len(candidates)


def _run_one_nested_result(
    *,
    economic: pd.DataFrame,
    features: pd.DataFrame,
    bundle: LoadedPredicateBundle,
    plan: FoldPlan,
    feature_block: str,
    config: StudyConfig,
) -> NestedOofResult:
    executions: list[OuterFoldExecution] = []
    for outer in plan.outer_folds:
        policy, inner_estimate, inner_support, _ = _select_candidate(
            economic=economic,
            features=features,
            bundle=bundle,
            plan=plan,
            outer=outer,
            feature_block=feature_block,
            config=config,
        )
        view, blocks, groups = _predicate_view_for_fold(
            features=features,
            bundle=bundle,
            scope=plan.panel_scope,
            side=plan.side,
            fold_id=outer.fold_id,
        )
        if not _policy_respects_clock_groups(policy, groups):
            raise CooldownStudyError("outer policy contains an unbound predicate")
        outer_rows = _evaluate_policy(
            economic=economic,
            predicate_view=view,
            predicate_columns=blocks[feature_block],
            policy=policy,
            side=plan.side,
            panel_scope=plan.panel_scope,
            days=outer.test_days,
            fold_id=outer.fold_id,
            stage="outer_oof",
        )
        if not outer_rows["candidate_id"].eq(policy.candidate_id).all():
            raise CooldownStudyError("inner-frozen candidate changed before outer OOF")
        outer_support = _support(outer_rows, config.search)
        executions.append(
            OuterFoldExecution(
                fold_id=outer.fold_id,
                train_days=outer.train_days,
                test_days=outer.test_days,
                selected_policy=policy,
                inner_estimate=inner_estimate,
                inner_support=inner_support,
                outer_support=outer_support,
                candidate_was_replaced_by_baseline=False,
                oof_rows=outer_rows,
            )
        )
    oof_rows = _reweight(
        pd.concat([execution.oof_rows for execution in executions], ignore_index=True)
    )
    if oof_rows["opportunity_id"].duplicated().any():
        raise CooldownStudyError("outer OOF opportunity appears more than once")
    combined_support = _support(oof_rows, config.search)
    evidence_role = {
        "prefix40": "development_prefix40",
        "added10": "late_diagnostic_added10_not_validation_or_holdout",
        "pooled50": "observed_prefix_plus_late_diagnostic_not_validation_or_holdout",
    }[plan.panel_scope]
    role_counts = (
        features.loc[
            (features["side"].astype(str).str.upper() == plan.side)
            & features["utc_day"].astype(str).isin(plan.exact_label_days),
            "panel_role",
        ]
        .value_counts()
        .sort_index()
        .astype(int)
        .to_dict()
    )
    return NestedOofResult(
        side=plan.side,
        feature_block=feature_block,
        panel_scope=plan.panel_scope,
        folds=tuple(executions),
        oof_rows=oof_rows,
        estimate=clustered_estimate(oof_rows, confidence=config.search.confidence),
        combined_support=combined_support,
        role_support=role_support_audit(
            oof_rows, confidence=config.search.confidence
        ),
        panel_role_counts=role_counts,
        permissions={
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        },
        evidence_role=evidence_role,
    )


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (np.bool_, bool)):
        return bool(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        number = float(value)
        return number if math.isfinite(number) else None
    return value


def _opportunity_weighted_estimate(rows: pd.DataFrame, *, confidence: float) -> dict[str, Any]:
    values = rows["uplift_usdc"].to_numpy(dtype=float, copy=False)
    mean = float(values.mean())
    influence = values - mean
    cluster_sum = (
        pd.Series(influence).groupby(rows["utc_day"].reset_index(drop=True), observed=True).sum()
    )
    days = int(len(cluster_sum))
    if days < 2:
        standard_error = math.inf
    else:
        variance = (
            days
            / (days - 1.0)
            * float(np.square(cluster_sum.to_numpy(dtype=float)).sum())
            / (len(rows) * len(rows))
        )
        standard_error = math.sqrt(max(0.0, variance))
    critical = NormalDist().inv_cdf(0.5 + confidence / 2.0)
    margin = critical * standard_error
    return _json_safe(
        {
            "mean_usdc": mean,
            "standard_error_usdc": standard_error,
            "lcb_usdc": mean - margin,
            "ucb_usdc": mean + margin,
            "confidence": confidence,
            "opportunities": len(rows),
            "days": days,
            "interval_cluster_contract": "utc_day_cluster_over_opportunity_rows",
        }
    )


def _result_report(
    result: NestedOofResult,
    *,
    plan: FoldPlan,
    config: StudyConfig,
    denominator_binding: Mapping[str, Any],
    partial_identification_unresolved: bool,
) -> dict[str, Any]:
    gate = evaluate_post_oof_deployment_gate(
        result,
        economic_epsilon_usdc=config.economic_epsilon_usdc,
        minimum_action_rate=config.minimum_deployment_action_rate,
        minimum_campaigns=config.minimum_deployment_campaigns,
        minimum_days=config.minimum_deployment_days,
        require_both_roles=True,
    )
    statistical_gate = asdict(gate)
    external_reasons: list[str] = []
    if partial_identification_unresolved:
        external_reasons.append("partial_identification_unresolved")
    final_candidate_eligible = result.feature_block != "R0"
    if not final_candidate_eligible:
        external_reasons.append("r0_reproduction_not_final_candidate_eligible")
    effective_gate = {
        **statistical_gate,
        "passed": bool(gate.passed and not external_reasons),
        "decision": (
            "research_evidence_supported"
            if gate.passed and not external_reasons
            else "abstain"
        ),
        "reasons": [*statistical_gate["reasons"], *external_reasons],
        "statistical_gate_passed": bool(gate.passed),
        "external_contract_blockers": external_reasons,
        "action_authorized": False,
        "live_authorized": False,
    }
    folds = []
    for execution in result.folds:
        folds.append(
            {
                "fold_id": execution.fold_id,
                "train_days": list(execution.train_days),
                "test_days": list(execution.test_days),
                "selected_candidate_id": execution.selected_policy.candidate_id,
                "selected_policy": execution.selected_policy.payload(),
                "selected_rules": [rule.payload() for rule in execution.selected_policy.rules],
                "inner_estimate": asdict(execution.inner_estimate),
                "inner_support": asdict(execution.inner_support),
                "outer_support": asdict(execution.outer_support),
                "candidate_replaced_by_baseline_before_outer_oof": False,
                "outer_action_rate": float(execution.oof_rows["selected_nonbaseline"].mean()),
            }
        )
    summary = result.summary()
    nominal_scope_alias = str(summary.pop("panel_scope"))
    evidence_role = str(summary.pop("evidence_role"))
    panel_role_counts = summary.pop("panel_role_counts")
    return _json_safe(
        {
            **summary,
            "economic_scope_identity": denominator_binding["scope_identity"],
            "economic_evidence_role": evidence_role,
            "denominator_binding": dict(denominator_binding),
            "exact_label_economic_calendar_days": list(plan.observed_days),
            "exact_label_economic_calendar_day_count": len(plan.observed_days),
            "side_days_with_exact_economic_rows": list(plan.exact_label_days),
            "side_exact_economic_row_day_count": len(plan.exact_label_days),
            "side_days_without_exact_economic_rows": list(plan.excluded_days),
            "economic_rows_by_nominal_calendar_role": panel_role_counts,
            "deprecated_diagnostic_aliases": {
                "panel_scope": {
                    "value": nominal_scope_alias,
                    "deprecated": True,
                    "diagnostic_only": True,
                    "must_not_be_used_as_an_economic_day_denominator": True,
                    "replacement": "economic_scope_identity",
                }
            },
            "folds": folds,
            "action_rate": result.estimate.action_rate,
            "opportunity_weighted_uplift": _opportunity_weighted_estimate(
                result.oof_rows, confidence=config.search.confidence
            ),
            "campaign_weighted_day_cluster_uplift": asdict(result.estimate),
            "opener_add_support": dict(result.role_support),
            "frozen_panel_role_decomposition": _panel_role_support(result.oof_rows),
            "final_candidate_eligible": final_candidate_eligible,
            "statistical_deployment_gate_after_outer_oof": statistical_gate,
            "deployment_gate_after_outer_oof": effective_gate,
            "deployment_gate_can_grant_action_or_live": False,
        }
    )


def _denominator_report(
    denominator: pd.DataFrame,
    *,
    sources: Sequence[DaySource],
) -> dict[str, Any]:
    status_counts = denominator["denominator_status"].value_counts().sort_index().to_dict()
    by_side_role = (
        denominator.groupby(["side", "role_at_fill"], dropna=False, observed=True)
        .agg(
            opportunities=("snapshot_id", "size"),
            exact_eight_arm=("economic_learner_eligible", "sum"),
            days=("utc_day", "nunique"),
        )
        .reset_index()
        .to_dict(orient="records")
    )
    exact_count = int(denominator["economic_learner_eligible"].sum())
    unsupported_count = int((~denominator["economic_learner_eligible"]).sum())
    unsupported_arm_rows = int(
        pd.to_numeric(
            denominator.get("unsupported_arm_count", pd.Series(0, index=denominator.index)),
            errors="coerce",
        ).fillna(0).sum()
    )
    return _json_safe(
        {
            "exact_label_manifest_day_count": len(sources),
            "exact_label_prefix_day_count": sum(
                source.panel_role == "prefix40" for source in sources
            ),
            "exact_label_added_day_count": sum(
                source.panel_role == "added10" for source in sources
            ),
            "economic_day_denominator_identity": "exact_label_economic_only",
            "opportunity_rows": len(denominator),
            "exact_eight_arm_opportunities": exact_count,
            "excluded_economic_opportunities": unsupported_count,
            "unsupported_arm_rows": unsupported_arm_rows,
            "exact_eight_arm_opportunity_fraction": (
                exact_count / len(denominator) if len(denominator) else 0.0
            ),
            "status_counts": status_counts,
            "side_role_support": by_side_role,
            "unlabeled_and_right_censored_retained": True,
            "complete_case_filter_applied": False,
            "denominator_rows_retained": True,
            "point_label_common_support_filter_applied": True,
            "unsupported_opportunity_sensitivity_implemented": False,
            "partial_identification_unresolved": unsupported_count > 0,
            "research_supported_promotion_blocked_by_partial_identification": (
                unsupported_count > 0
            ),
            "economic_filter_contract": (
                "only_explicit_joint_strict_side_specific_eight_arm_opportunities"
            ),
        }
    )


def _paired_feature_block_estimate(
    *,
    baseline: NestedOofResult,
    successor: NestedOofResult,
    confidence: float,
) -> dict[str, Any]:
    if (
        baseline.side != successor.side
        or baseline.panel_scope != successor.panel_scope
    ):
        raise CooldownStudyError("feature-block comparison identity drifted")
    paired = _paired_oof_estimate(
        baseline_rows=baseline.oof_rows,
        successor_rows=successor.oof_rows,
        confidence=confidence,
    )
    return _json_safe(
        {
            "baseline_feature_block": baseline.feature_block,
            "successor_feature_block": successor.feature_block,
            **paired,
        }
    )


def _paired_oof_estimate(
    *,
    baseline_rows: pd.DataFrame,
    successor_rows: pd.DataFrame,
    confidence: float,
) -> dict[str, Any]:
    identity_columns = (
        "opportunity_id",
        "utc_day",
        "campaign_cluster_id",
        "side",
        "role_at_fill",
    )
    left = baseline_rows.set_index("opportunity_id", drop=False).sort_index()
    right = successor_rows.set_index("opportunity_id", drop=False).sort_index()
    if not left.index.equals(right.index):
        raise CooldownStudyError("feature-block OOF opportunities are not paired")
    for column in identity_columns[1:]:
        if not left[column].equals(right[column]):
            raise CooldownStudyError(
                f"feature-block paired identity drifted on {column}"
            )
    paired = right.loc[:, list(identity_columns)].copy()
    paired["uplift_usdc"] = (
        right["uplift_usdc"].to_numpy(dtype=float, copy=False)
        - left["uplift_usdc"].to_numpy(dtype=float, copy=False)
    )
    paired["selected_nonbaseline"] = (
        right["selected_nonbaseline"].to_numpy(dtype=bool, copy=False)
        | left["selected_nonbaseline"].to_numpy(dtype=bool, copy=False)
    )
    estimate = clustered_estimate(_reweight(paired), confidence=confidence)
    return _json_safe(
        {
            "estimate": asdict(estimate),
            "paired_opportunity_count": len(paired),
            "paired_identity": "same_outer_oof_opportunity_and_day_campaign_cluster",
        }
    )


def _feature_family_selection_report(
    *,
    results: Mapping[str, NestedOofResult],
    continuous_results: Mapping[str, ContinuousComparatorResult],
    block_reports: Mapping[str, Mapping[str, Any]],
    confidence: float,
    partial_identification_unresolved: bool,
) -> dict[str, Any]:
    if set(results) != set(FEATURE_BLOCKS) or set(continuous_results) != set(
        FEATURE_BLOCKS
    ):
        raise CooldownStudyError("feature-family result set drifted")
    comparison_count = 2
    familywise_confidence = float(confidence)
    per_comparison_confidence = 1.0 - (
        (1.0 - familywise_confidence) / comparison_count
    )
    comparisons = {
        "M1_minus_M0": _paired_feature_block_estimate(
            baseline=results["M0"],
            successor=results["M1"],
            confidence=per_comparison_confidence,
        ),
        "M2_minus_M1": _paired_feature_block_estimate(
            baseline=results["M1"],
            successor=results["M2"],
            confidence=per_comparison_confidence,
        ),
    }
    selected: str | None = None
    selection_trace: list[dict[str, Any]] = []
    m0_gate = block_reports["M0"][
        "statistical_deployment_gate_after_outer_oof"
    ]
    if bool(m0_gate["passed"]):
        selected = "M0"
        selection_trace.append(
            {"feature_block": "M0", "selected": True, "reason": "absolute_gate_passed"}
        )
    else:
        selection_trace.append(
            {"feature_block": "M0", "selected": False, "reason": "absolute_gate_failed"}
        )
    m1_increment = comparisons["M1_minus_M0"]["estimate"]
    m1_gate = block_reports["M1"][
        "statistical_deployment_gate_after_outer_oof"
    ]
    m1_supersedes = bool(
        m1_gate["passed"] and float(m1_increment["lcb_usdc"]) > 0.0
    )
    selection_trace.append(
        {
            "feature_block": "M1",
            "selected": m1_supersedes,
            "reason": (
                "paired_incremental_lcb_positive_and_absolute_gate_passed"
                if m1_supersedes
                else "hierarchical_increment_or_absolute_gate_failed"
            ),
        }
    )
    if m1_supersedes:
        selected = "M1"
    m2_increment = comparisons["M2_minus_M1"]["estimate"]
    m2_gate = block_reports["M2"][
        "statistical_deployment_gate_after_outer_oof"
    ]
    m2_supersedes = bool(
        m1_supersedes
        and m2_gate["passed"]
        and float(m2_increment["lcb_usdc"]) > 0.0
    )
    selection_trace.append(
        {
            "feature_block": "M2",
            "selected": m2_supersedes,
            "reason": (
                "paired_incremental_lcb_positive_and_absolute_gate_passed"
                if m2_supersedes
                else "hierarchical_increment_or_absolute_gate_failed"
            ),
        }
    )
    if m2_supersedes:
        selected = "M2"
    boolean_vs_continuous: dict[str, Any] | None = None
    boolean_information_loss_detected = False
    if selected is not None:
        boolean_vs_continuous = {
            "boolean_feature_block": selected,
            "successor_model": "raw_state_multioutput_regression_tree_diagnostic",
            **_paired_oof_estimate(
                baseline_rows=results[selected].oof_rows,
                successor_rows=continuous_results[selected].oof_rows,
                confidence=confidence,
            ),
        }
        boolean_information_loss_detected = bool(
            float(boolean_vs_continuous["estimate"]["lcb_usdc"]) > 0.0
        )
    blockers: list[str] = []
    if selected is None:
        blockers.append("no_hierarchical_feature_block_passed")
    if partial_identification_unresolved:
        blockers.append("partial_identification_unresolved")
    if boolean_information_loss_detected:
        blockers.append("continuous_state_comparator_outperforms_boolean")
    return _json_safe(
        {
            "selection_contract": (
                "hierarchical_M0_then_Bonferroni_paired_M1_minus_M0_then_"
                "M2_minus_M1_no_unrestricted_best_block_selection"
            ),
            "r0_role": "pipeline_reproduction_only_not_final_candidate",
            "familywise_confidence": familywise_confidence,
            "comparison_count": comparison_count,
            "per_comparison_confidence": per_comparison_confidence,
            "comparisons": comparisons,
            "selection_trace": selection_trace,
            "statistically_selected_feature_block": selected,
            "continuous_state_comparator_status": "completed",
            "continuous_state_comparator_required_before_policy_freeze": True,
            "continuous_state_comparator_model_family": (
                "raw_state_multioutput_regression_tree_diagnostic"
            ),
            "continuous_state_comparator_runtime": {
                "sklearn_version": sklearn.__version__,
            },
            "selected_boolean_vs_continuous": boolean_vs_continuous,
            "boolean_information_loss_detected": boolean_information_loss_detected,
            "continuous_state_comparator_may_replace_boolean_policy": False,
            "unified_policy_freeze_eligible": bool(selected is not None and not blockers),
            "unified_policy_freeze_blockers": blockers,
            "action_authorized": False,
            "live_authorized": False,
        }
    )


def _scope_reports(
    *,
    economic: pd.DataFrame,
    denominator: pd.DataFrame,
    sources: Sequence[DaySource],
    bundle: LoadedPredicateBundle,
    plans: Mapping[tuple[str, str], FoldPlan],
    config: StudyConfig,
    formal_full_support_run: bool,
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    features = (
        denominator.loc[denominator["economic_learner_eligible"]]
        .copy()
        .set_index("opportunity_id", drop=False)
    )
    reports: dict[str, Any] = {}
    oof_frames: list[pd.DataFrame] = []
    continuous_oof_frames: list[pd.DataFrame] = []
    for scope in PANEL_SCOPES:
        scope_denominator = (
            denominator
            if scope == "pooled50"
            else denominator.loc[denominator["panel_role"] == scope]
        )
        scope_partial_identification_unresolved = bool(
            (~scope_denominator["economic_learner_eligible"]).any()
        )
        denominator_binding = _scope_denominator_binding(
            scope=scope,
            exact_label_calendar_days=_scope_days(sources, scope),
            formal_full_support_run=formal_full_support_run,
        )
        report_key = str(denominator_binding["scope_identity"])
        scope_report: dict[str, Any] = {
            "economic_scope_identity": report_key,
            "economic_statistics_denominator": "exact_label_economic",
            "denominator_binding": denominator_binding,
            "sides": {},
        }
        for side in SIDES:
            plan = plans[(scope, side)]
            if plan.unavailable_reason is not None:
                scope_report["sides"][side] = {
                    "status": "not_run",
                    "reason": plan.unavailable_reason,
                    "denominator_binding": denominator_binding,
                    "exact_label_economic_calendar_days": list(plan.observed_days),
                    "side_days_with_exact_economic_rows": list(plan.exact_label_days),
                    "side_days_without_exact_economic_rows": list(plan.excluded_days),
                    "action_authorized": False,
                    "live_authorized": False,
                }
                continue
            side_blocks: dict[str, Any] = {}
            side_results: dict[str, NestedOofResult] = {}
            side_continuous_reports: dict[str, Any] = {}
            side_continuous_results: dict[str, ContinuousComparatorResult] = {}
            for block in FEATURE_BLOCKS:
                result = _run_one_nested_result(
                    economic=economic,
                    features=features.loc[features["side"].astype(str).str.upper() == side],
                    bundle=bundle,
                    plan=plan,
                    feature_block=block,
                    config=config,
                )
                side_results[block] = result
                side_blocks[block] = _result_report(
                    result,
                    plan=plan,
                    config=config,
                    denominator_binding=denominator_binding,
                    partial_identification_unresolved=(
                        scope_partial_identification_unresolved
                    ),
                )
                rows = result.oof_rows.copy()
                rows.insert(0, "feature_block", block)
                rows.insert(0, "panel_scope", scope)
                rows.insert(0, "panel_scope_is_deprecated_nominal_alias", True)
                rows.insert(0, "economic_scope_identity", report_key)
                rows.insert(0, "economic_denominator_identity", "exact_label_economic")
                oof_frames.append(rows)
                continuous = _run_continuous_comparator(
                    economic=economic,
                    features=features.loc[
                        features["side"].astype(str).str.upper() == side
                    ],
                    bundle=bundle,
                    plan=plan,
                    feature_block=block,
                    config=config,
                )
                side_continuous_results[block] = continuous
                side_continuous_reports[block] = _continuous_result_report(
                    continuous,
                    confidence=config.search.confidence,
                    search_config=config.search,
                )
                continuous_rows = continuous.oof_rows.copy()
                continuous_rows.insert(0, "feature_block", block)
                continuous_rows.insert(0, "panel_scope", scope)
                continuous_rows.insert(
                    0, "panel_scope_is_deprecated_nominal_alias", True
                )
                continuous_rows.insert(0, "economic_scope_identity", report_key)
                continuous_rows.insert(
                    0, "economic_denominator_identity", "exact_label_economic"
                )
                continuous_oof_frames.append(continuous_rows)
            scope_report["sides"][side] = {
                "status": "completed",
                "feature_blocks": side_blocks,
                "continuous_state_comparator_feature_blocks": (
                    side_continuous_reports
                ),
                "feature_family_selection": _feature_family_selection_report(
                    results=side_results,
                    continuous_results=side_continuous_results,
                    block_reports=side_blocks,
                    confidence=config.search.confidence,
                    partial_identification_unresolved=(
                        scope_partial_identification_unresolved
                    ),
                ),
                "pooled_with_other_side": False,
                "action_authorized": False,
                "live_authorized": False,
            }
        reports[report_key] = scope_report
    if not oof_frames:
        raise CooldownStudyError("no panel scope had enough exact days for OOF")
    if not continuous_oof_frames:
        raise CooldownStudyError("continuous comparator produced no outer OOF rows")
    return (
        reports,
        pd.concat(oof_frames, ignore_index=True),
        pd.concat(continuous_oof_frames, ignore_index=True),
    )


def _write_parquet(path: Path, frame: pd.DataFrame) -> dict[str, Any]:
    frame.to_parquet(path, index=False, compression="zstd")
    with path.open("rb") as handle:
        os.fsync(handle.fileno())
    return {
        "path": path.name,
        "rows": int(len(frame)),
        "columns": list(frame.columns),
        "sha256": _sha256(path),
    }


def _binding_payload(
    *,
    formal_panel_manifest: Path,
    predicate_bundle: LoadedPredicateBundle,
    config: StudyConfig,
) -> dict[str, Any]:
    source_path = Path(__file__).resolve()
    dependency_paths = {
        "label_panel": source_path.with_name(
            "causal_multichannel_window_boolean_cooldown_label_panel.py"
        ),
        "nested_oof": source_path.with_name(
            "causal_multichannel_window_boolean_cooldown_nested_oof.py"
        ),
        "predicates": source_path.with_name(
            "causal_multichannel_window_boolean_cooldown_predicates.py"
        ),
        "strict_label_panel_runner": source_path.with_name(
            "causal_multichannel_window_boolean_cooldown_strict_label_panel_runner.py"
        ),
    }
    return {
        "identity": STUDY_IDENTITY,
        "formal_panel_manifest_path": str(Path(formal_panel_manifest).resolve()),
        "formal_panel_manifest_sha256": _sha256(Path(formal_panel_manifest).resolve()),
        "predicate_bundle_path": str(predicate_bundle.path),
        "predicate_bundle_sha256": predicate_bundle.sha256,
        "predicate_bundle_canonical_sha256": predicate_bundle.canonical_sha256,
        "study_code_path": str(source_path),
        "study_code_sha256": _sha256(source_path),
        "implementation_dependencies": {
            key: {"path": str(path), "sha256": _sha256(path)}
            for key, path in sorted(dependency_paths.items())
        },
        "config": config.payload(),
    }


def _load_or_initialize_progress(
    *,
    path: Path,
    binding: Mapping[str, Any],
    sources: Sequence[DaySource],
) -> dict[str, Any]:
    binding_sha = _canonical_sha256(binding)
    if path.exists():
        progress = _load_json(path)
        if progress.get("schema_version") != PROGRESS_SCHEMA:
            raise CooldownStudyError("study progress schema drifted")
        if progress.get("binding_sha256") != binding_sha:
            raise CooldownStudyError("study resume binding drifted")
        if tuple(progress.get("ordered_days", [])) != tuple(source.day for source in sources):
            raise CooldownStudyError("study resume day denominator drifted")
        return progress
    progress = {
        "schema_version": PROGRESS_SCHEMA,
        "identity": STUDY_IDENTITY,
        "binding": dict(binding),
        "binding_sha256": binding_sha,
        "ordered_days": [source.day for source in sources],
        "days": {
            source.day: {
                "status": "pending",
                "panel_role": source.panel_role,
                "source_manifest_path": str(source.manifest_path),
                "source_manifest_sha256": source.manifest_sha256,
            }
            for source in sources
        },
        "economic_outcomes_read": False,
        "nested_oof_completed": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    _atomic_json(path, progress)
    return progress


def _acquire_lock(path: Path) -> Any:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+", encoding="ascii")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        handle.close()
        raise CooldownStudyError("another study process owns the output") from exc
    return handle


def _publish_output(
    *,
    output: Path,
    binding: Mapping[str, Any],
    sources: Sequence[DaySource],
    bundle: LoadedPredicateBundle,
    denominator: pd.DataFrame,
    economic: pd.DataFrame,
    oof_rows: pd.DataFrame,
    continuous_oof_rows: pd.DataFrame,
    report: Mapping[str, Any],
    day_panels: Sequence[tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]],
) -> dict[str, Any]:
    output = Path(output).resolve()
    if output.exists():
        existing = validate_study_output(output)
        if existing.get("binding_sha256") != _canonical_sha256(binding):
            raise CooldownStudyError("existing study output binding differs from this request")
        return existing
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = output.parent / (f".{output.name}.staging.{os.getpid()}.{uuid.uuid4().hex}")
    staging.mkdir()
    try:
        denominator_row = _write_parquet(staging / "denominator_rows.parquet", denominator)
        economic_row = _write_parquet(staging / "exact_eight_arm_rows.parquet", economic)
        oof_row = _write_parquet(staging / "outer_oof_rows.parquet", oof_rows)
        continuous_oof_row = _write_parquet(
            staging / "continuous_outer_oof_rows.parquet", continuous_oof_rows
        )
        _atomic_json(staging / "report.json", report)
        report_row = {
            "path": "report.json",
            "sha256": _sha256(staging / "report.json"),
        }
        day_rows = []
        admitted_panel_root = staging / "day_panels"
        admitted_panel_root.mkdir()
        for source, panel in zip(sources, day_panels, strict=True):
            source_manifest = panel[0]
            manifest = json.loads(_canonical_json(source_manifest))
            panel_root = admitted_panel_root / f"day={source.day}"
            panel_root.mkdir()
            for key in ("opportunities", "labels"):
                row = manifest[key]
                source_part = Path(str(row["path"])).resolve()
                if not source_part.is_file() or _sha256(source_part) != row["sha256"]:
                    raise CooldownStudyError(f"day-panel {key} drifted before admission")
                destination_part = panel_root / f"{key}.parquet"
                shutil.copy2(source_part, destination_part)
                with destination_part.open("rb") as handle:
                    os.fsync(handle.fileno())
                row["path"] = destination_part.name
            manifest_path = panel_root / "manifest.json"
            _atomic_json(manifest_path, manifest)
            _atomic_json(
                panel_root / "_SUCCESS",
                {"manifest_sha256": _sha256(manifest_path)},
            )
            day_rows.append(
                {
                    "day": source.day,
                    "panel_role": source.panel_role,
                    "manifest_path": str(
                        Path("day_panels") / f"day={source.day}" / "manifest.json"
                    ),
                    "manifest_sha256": _sha256(manifest_path),
                    "source_manifest_path": str(source.manifest_path),
                    "source_manifest_sha256": source.manifest_sha256,
                }
            )
        predicate_rows: dict[str, dict[str, Any]] = {}
        admitted_predicate_root = staging / "predicate_artifacts"
        admitted_predicate_root.mkdir()
        for index, (key, value) in enumerate(sorted(bundle.artifact_references.items())):
            destination = admitted_predicate_root / f"{index:04d}.json"
            shutil.copy2(value.path, destination)
            with destination.open("rb") as handle:
                os.fsync(handle.fileno())
            if _sha256(destination) != value.sha256:
                raise CooldownStudyError(f"predicate artifact drifted during admission: {key}")
            predicate_rows[key] = {
                "path": str(Path("predicate_artifacts") / destination.name),
                "sha256": value.sha256,
            }
        manifest = {
            "schema_version": MANIFEST_SCHEMA,
            "identity": STUDY_IDENTITY,
            "binding": dict(binding),
            "binding_sha256": _canonical_sha256(binding),
            "exact_label_manifest_day_count": len(sources),
            "nominal_mechanics_day_count": 50,
            "formal_exact_label_day_count": 41,
            "reduced_support_diagnostic_day_count": 9,
            "economic_statistics_denominator": "exact_label_economic_only",
            "reduced_support_pooled_into_economic_statistics": False,
            "reduced_support_economic_labels_manufactured": False,
            "day_panels": day_rows,
            "predicate_artifacts": predicate_rows,
            "artifacts": {
                "denominator_rows": denominator_row,
                "exact_eight_arm_rows": economic_row,
                "outer_oof_rows": oof_row,
                "continuous_outer_oof_rows": continuous_oof_row,
                "report": report_row,
            },
            "complete_case_filter_applied": False,
            "unlabeled_right_censored_denominator_retained": True,
            "point_label_common_support_filter_applied": True,
            "unsupported_opportunity_sensitivity_implemented": False,
            "research_supported_promotion_requires_partial_identification_resolved": True,
            "economic_learner_filter": "explicit_exact_side_specific_eight_arm_only",
            "book_trade_same_clause_allowed": True,
            "book_trade_threshold_references_joined_in_2025": False,
            "pooled_side_policy_created": False,
            "deployment_gate_evaluated_after_outer_oof": True,
            "outer_fold_support_audited": True,
            "combined_action_support_audited": True,
            "continuous_state_comparator_completed": True,
            "continuous_state_comparator_may_replace_boolean_policy": False,
            "permissions": {
                "research_evidence_only": True,
                "validation_read": False,
                "sealed_holdout_read": False,
                "action_authorized": False,
                "live_authorized": False,
            },
        }
        _atomic_json(staging / "manifest.json", manifest)
        manifest_sha = _sha256(staging / "manifest.json")
        (staging / "manifest.sha256").write_text(
            f"{manifest_sha}  manifest.json\n", encoding="ascii"
        )
        with (staging / "manifest.sha256").open("rb") as handle:
            os.fsync(handle.fileno())
        _atomic_json(staging / "_SUCCESS", {"manifest_sha256": manifest_sha})
        os.replace(staging, output)
        _fsync_directory(output.parent)
        return validate_study_output(output)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def _validate_support_payload(
    payload: Any,
    expected: SupportAudit,
    *,
    label: str,
) -> None:
    if not isinstance(payload, Mapping):
        raise CooldownStudyError(f"{label} is missing")
    required = {
        "action_opportunities",
        "action_campaigns",
        "action_days",
        "action_rate",
        "passed",
    }
    if required - set(payload):
        raise CooldownStudyError(f"{label} fields are incomplete")
    for field in ("action_opportunities", "action_campaigns", "action_days"):
        if int(payload[field]) != int(getattr(expected, field)):
            raise CooldownStudyError(f"{label} {field} drifted")
    if not math.isclose(
        float(payload["action_rate"]),
        expected.action_rate,
        rel_tol=0.0,
        abs_tol=1e-12,
    ):
        raise CooldownStudyError(f"{label} action_rate drifted")
    if payload["passed"] is not expected.passed:
        raise CooldownStudyError(f"{label} passed flag drifted")


def _validate_role_audit_contract(payload: Any, *, label: str) -> None:
    if not isinstance(payload, Mapping) or set(payload) != {"opener", "add"}:
        raise CooldownStudyError(f"{label} role audit is missing")
    required = {
        "opportunities",
        "campaigns",
        "days",
        "action_opportunities",
        "action_campaigns",
        "action_days",
        "action_rate",
        "campaign_weighted_mean_uplift_usdc",
        "campaign_day_clustered_uplift_interval",
        "tail_diagnostics",
    }
    for role in ("opener", "add"):
        audit = payload[role]
        if not isinstance(audit, Mapping) or required - set(audit):
            raise CooldownStudyError(f"{label} {role} audit fields are incomplete")
        if int(audit["opportunities"]) > 0 and (
            not isinstance(audit["campaign_day_clustered_uplift_interval"], Mapping)
            or not isinstance(audit["tail_diagnostics"], Mapping)
        ):
            raise CooldownStudyError(f"{label} {role} interval/tail audit is missing")


def _validate_report_support_audits(
    *,
    report: Mapping[str, Any],
    binding: Mapping[str, Any],
    oof_rows: pd.DataFrame,
) -> None:
    config_payload = binding.get("config")
    search_payload = (
        config_payload.get("search") if isinstance(config_payload, Mapping) else None
    )
    if not isinstance(search_payload, Mapping):
        raise CooldownStudyError("study search binding is missing")
    try:
        search_config = SearchConfig(**dict(search_payload))
    except (TypeError, ValueError, NestedOofContractError) as exc:
        raise CooldownStudyError("study search binding is invalid") from exc

    exact_results = report.get("exact_label_economic_results")
    if not isinstance(exact_results, Mapping):
        raise CooldownStudyError("exact-label economic result scopes are missing")
    for scope_id, scope_report in exact_results.items():
        if not isinstance(scope_report, Mapping):
            raise CooldownStudyError("exact-label scope report is invalid")
        sides = scope_report.get("sides")
        if not isinstance(sides, Mapping):
            raise CooldownStudyError("exact-label side reports are missing")
        for side, side_report in sides.items():
            if not isinstance(side_report, Mapping):
                raise CooldownStudyError("exact-label side report is invalid")
            if side_report.get("status") != "completed":
                continue
            feature_blocks = side_report.get("feature_blocks")
            continuous_blocks = side_report.get(
                "continuous_state_comparator_feature_blocks"
            )
            if not isinstance(feature_blocks, Mapping) or not isinstance(
                continuous_blocks, Mapping
            ):
                raise CooldownStudyError("completed side lacks feature-block reports")
            if set(feature_blocks) != set(FEATURE_BLOCKS) or set(continuous_blocks) != set(
                FEATURE_BLOCKS
            ):
                raise CooldownStudyError("feature-block support reports drifted")
            for block in FEATURE_BLOCKS:
                block_report = feature_blocks[block]
                if not isinstance(block_report, Mapping):
                    raise CooldownStudyError("Boolean feature-block report is invalid")
                rows = oof_rows.loc[
                    oof_rows["economic_scope_identity"].astype(str).eq(str(scope_id))
                    & oof_rows["side"].astype(str).eq(str(side))
                    & oof_rows["feature_block"].astype(str).eq(block)
                ]
                if rows.empty:
                    raise CooldownStudyError("reported Boolean support has no OOF rows")
                combined = _support(rows, search_config)
                _validate_support_payload(
                    block_report.get("combined_action_support"),
                    combined,
                    label=f"{scope_id}/{side}/{block} combined support",
                )
                folds = block_report.get("folds")
                if not isinstance(folds, list) or not folds:
                    raise CooldownStudyError("Boolean outer-fold reports are missing")
                report_fold_ids = [str(fold.get("fold_id", "")) for fold in folds]
                observed_fold_ids = sorted(set(rows["fold_id"].astype(str)))
                if sorted(report_fold_ids) != observed_fold_ids or len(
                    report_fold_ids
                ) != len(set(report_fold_ids)):
                    raise CooldownStudyError("Boolean outer-fold support identities drifted")
                expected_by_fold: dict[str, SupportAudit] = {}
                for fold in folds:
                    if not isinstance(fold, Mapping):
                        raise CooldownStudyError("Boolean outer-fold report is invalid")
                    fold_id = str(fold["fold_id"])
                    fold_rows = rows.loc[rows["fold_id"].astype(str).eq(fold_id)]
                    expected = _support(fold_rows, search_config)
                    expected_by_fold[fold_id] = expected
                    _validate_support_payload(
                        fold.get("outer_support"),
                        expected,
                        label=f"{scope_id}/{side}/{block}/{fold_id} outer support",
                    )
                gate = block_report.get("statistical_deployment_gate_after_outer_oof")
                if not isinstance(gate, Mapping):
                    raise CooldownStudyError("post-OOF deployment gate is missing")
                _validate_support_payload(
                    gate.get("combined_support"),
                    combined,
                    label=f"{scope_id}/{side}/{block} gate combined support",
                )
                gate_fold_support = gate.get("outer_fold_support")
                if not isinstance(gate_fold_support, Mapping) or set(
                    gate_fold_support
                ) != set(expected_by_fold):
                    raise CooldownStudyError("deployment gate outer-fold support drifted")
                for fold_id, expected in expected_by_fold.items():
                    _validate_support_payload(
                        gate_fold_support[fold_id],
                        expected,
                        label=f"{scope_id}/{side}/{block}/{fold_id} gate support",
                    )
                zero_action_folds = sorted(
                    fold_id
                    for fold_id, expected in expected_by_fold.items()
                    if expected.action_opportunities == 0
                )
                if sorted(gate.get("zero_action_outer_folds", [])) != zero_action_folds:
                    raise CooldownStudyError("zero-action outer-fold audit drifted")
                if zero_action_folds and (
                    gate.get("passed") is not False
                    or "outer_fold_without_nonbaseline_action"
                    not in gate.get("reasons", [])
                ):
                    raise CooldownStudyError("zero-action outer fold did not fail the gate")
                _validate_role_audit_contract(
                    block_report.get("opener_add_support"),
                    label=f"{scope_id}/{side}/{block} Boolean",
                )
                continuous_report = continuous_blocks[block]
                if not isinstance(continuous_report, Mapping):
                    raise CooldownStudyError("continuous feature-block report is invalid")
                _validate_role_audit_contract(
                    continuous_report.get("opener_add_support"),
                    label=f"{scope_id}/{side}/{block} continuous",
                )


def validate_study_output(output: Path) -> dict[str, Any]:
    """Validate every admitted result file, row count, hash, and permission."""

    root = Path(output).expanduser().resolve()
    manifest_path = root / "manifest.json"
    success_path = root / "_SUCCESS"
    sha_path = root / "manifest.sha256"
    if not root.is_dir() or not manifest_path.is_file():
        raise CooldownStudyError(f"study output is not admitted: {root}")
    manifest = _load_json(manifest_path)
    manifest_sha = _sha256(manifest_path)
    if (
        not success_path.is_file()
        or _load_json(success_path).get("manifest_sha256") != manifest_sha
    ):
        raise CooldownStudyError("study success marker drifted")
    if not sha_path.is_file() or sha_path.read_text(encoding="ascii") != (
        f"{manifest_sha}  manifest.json\n"
    ):
        raise CooldownStudyError("study manifest SHA256 file drifted")
    if manifest.get("schema_version") != MANIFEST_SCHEMA:
        raise CooldownStudyError("study output schema drifted")
    if manifest.get("identity") != STUDY_IDENTITY:
        raise CooldownStudyError("study output identity drifted")
    binding = manifest.get("binding")
    if not isinstance(binding, Mapping) or manifest.get("binding_sha256") != _canonical_sha256(
        binding
    ):
        raise CooldownStudyError("study output binding drifted")
    for path_key, hash_key, label in (
        (
            "formal_panel_manifest_path",
            "formal_panel_manifest_sha256",
            "formal panel input",
        ),
        ("predicate_bundle_path", "predicate_bundle_sha256", "predicate bundle input"),
        ("study_code_path", "study_code_sha256", "study implementation"),
    ):
        bound_path = Path(str(binding.get(path_key, "")))
        if not bound_path.is_file() or _sha256(bound_path) != binding.get(hash_key):
            raise CooldownStudyError(f"{label} hash drifted")
    dependencies = binding.get("implementation_dependencies")
    if not isinstance(dependencies, Mapping) or not dependencies:
        raise CooldownStudyError("study implementation dependency bindings are missing")
    for label, row in dependencies.items():
        if not isinstance(row, Mapping):
            raise CooldownStudyError("study dependency binding is invalid")
        path = Path(str(row.get("path", "")))
        if not path.is_file() or _sha256(path) != row.get("sha256"):
            raise CooldownStudyError(f"study dependency hash drifted: {label}")
    permissions = manifest.get("permissions")
    if (
        not isinstance(permissions, Mapping)
        or permissions.get("validation_read") is not False
        or permissions.get("sealed_holdout_read") is not False
        or permissions.get("action_authorized") is not False
        or permissions.get("live_authorized") is not False
    ):
        raise CooldownStudyError("study output exceeds research-only authority")
    if manifest.get("complete_case_filter_applied") is not False:
        raise CooldownStudyError("study output claims a complete-case filter")
    if manifest.get("point_label_common_support_filter_applied") is not True:
        raise CooldownStudyError("study output hides its point-label support filter")
    if manifest.get("unsupported_opportunity_sensitivity_implemented") is not False:
        raise CooldownStudyError("unsupported-opportunity sensitivity status drifted")
    if manifest.get("pooled_side_policy_created") is not False:
        raise CooldownStudyError("study output pools BUY and SELL")
    if (
        manifest.get("outer_fold_support_audited") is not True
        or manifest.get("combined_action_support_audited") is not True
    ):
        raise CooldownStudyError("study output support-audit contract drifted")
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, Mapping):
        raise CooldownStudyError("study output artifact table is missing")
    frames: dict[str, pd.DataFrame] = {}
    for key in (
        "denominator_rows",
        "exact_eight_arm_rows",
        "outer_oof_rows",
        "continuous_outer_oof_rows",
    ):
        row = artifacts.get(key)
        if not isinstance(row, Mapping):
            raise CooldownStudyError(f"study output lacks {key}")
        path = root / str(row.get("path", ""))
        if not path.is_file() or _sha256(path) != row.get("sha256"):
            raise CooldownStudyError(f"study output hash drifted for {key}")
        frame = pd.read_parquet(path)
        if len(frame) != int(row.get("rows", -1)):
            raise CooldownStudyError(f"study output row count drifted for {key}")
        if list(frame.columns) != list(row.get("columns", [])):
            raise CooldownStudyError(f"study output schema drifted for {key}")
        frames[key] = frame
    report_row = artifacts.get("report")
    if not isinstance(report_row, Mapping):
        raise CooldownStudyError("study report metadata is missing")
    report_path = root / str(report_row.get("path", ""))
    if not report_path.is_file() or _sha256(report_path) != report_row.get("sha256"):
        raise CooldownStudyError("study report hash drifted")
    report = _load_json(report_path)
    if report.get("schema_version") != REPORT_SCHEMA:
        raise CooldownStudyError("study report schema drifted")
    report_permissions = report.get("permissions")
    if (
        not isinstance(report_permissions, Mapping)
        or report_permissions.get("validation_read") is not False
        or report_permissions.get("sealed_holdout_read") is not False
        or report_permissions.get("action_authorized") is not False
        or report_permissions.get("live_authorized") is not False
    ):
        raise CooldownStudyError("study report exceeds research-only authority")
    if (
        report.get("validation_read") is not False
        or report.get("sealed_holdout_read") is not False
    ):
        raise CooldownStudyError("study report read protected evidence")
    if "panel_scopes" in report:
        raise CooldownStudyError("study report emits the deprecated nominal panel scopes")
    day_denominators = report.get("day_denominators")
    if not isinstance(day_denominators, Mapping):
        raise CooldownStudyError("study report day denominators are missing")
    expected_counts = {
        "nominal_mechanics_denominator": (40, 10, 50),
        "exact_label_economic_denominator": (33, 8, 41),
        "reduced_support_diagnostic_denominator": (7, 2, 9),
    }
    for key, expected in expected_counts.items():
        row = day_denominators.get(key)
        if not isinstance(row, Mapping) or (
            int(row.get("prefix_days", -1)),
            int(row.get("added_days", -1)),
            int(row.get("pooled_days", -1)),
        ) != expected:
            raise CooldownStudyError(f"study report {key} drifted")
    reduced = day_denominators["reduced_support_diagnostic_denominator"]
    if (
        reduced.get("economic_statistics_bound") is not False
        or reduced.get("pooled_into_exact_label_economics") is not False
        or reduced.get("economic_labels_manufactured") is not False
    ):
        raise CooldownStudyError("reduced-support reporting boundary drifted")
    exact_results = report.get("exact_label_economic_results")
    expected_result_keys = {
        str(row["report_key"]) for row in FORMAL_DAY_DENOMINATORS.values()
    }
    if not isinstance(exact_results, Mapping) or set(exact_results) != expected_result_keys:
        raise CooldownStudyError("exact-label economic result scopes drifted")
    comparator = report.get("continuous_state_comparator")
    if (
        not isinstance(comparator, Mapping)
        or comparator.get("status") != "completed"
        or comparator.get("model_family")
        != "raw_state_multioutput_regression_tree_diagnostic"
        or comparator.get("may_replace_boolean_policy") is not False
        or comparator.get("may_grant_action_or_live") is not False
    ):
        raise CooldownStudyError("continuous comparator report contract drifted")
    denominator = frames["denominator_rows"]
    economic = frames["exact_eight_arm_rows"]
    oof = frames["outer_oof_rows"]
    continuous_oof = frames["continuous_outer_oof_rows"]
    if not {
        "denominator_status",
        "economic_learner_eligible",
    } <= set(denominator):
        raise CooldownStudyError("denominator audit fields are missing")
    if not economic["strict_native_label"].eq(True).all():  # noqa: E712
        raise CooldownStudyError("non-strict label entered admitted economic rows")
    exact_ids = set(
        denominator.loc[denominator["economic_learner_eligible"], "opportunity_id"].astype(str)
    )
    if set(economic["opportunity_id"].astype(str)) != exact_ids:
        raise CooldownStudyError("economic rows drifted from explicit denominator filter")
    if set(oof["side"].astype(str)) - set(SIDES):
        raise CooldownStudyError("OOF rows contain a pooled side")
    if not oof["evaluation_stage"].eq("outer_oof").all():
        raise CooldownStudyError("non-outer rows entered the final OOF artifact")
    if not {
        "economic_denominator_identity",
        "economic_scope_identity",
        "panel_scope_is_deprecated_nominal_alias",
    } <= set(oof):
        raise CooldownStudyError("OOF reporting denominator fields are missing")
    if not oof["economic_denominator_identity"].eq("exact_label_economic").all():
        raise CooldownStudyError("OOF rows drifted from the exact-label denominator")
    if not oof["panel_scope_is_deprecated_nominal_alias"].eq(True).all():  # noqa: E712
        raise CooldownStudyError("OOF nominal scope alias lacks its deprecated marker")
    if not continuous_oof["evaluation_stage"].eq("continuous_outer_oof").all():
        raise CooldownStudyError("continuous comparator contains non-outer rows")
    if not continuous_oof["model_family"].eq(
        "raw_state_multioutput_regression_tree_diagnostic"
    ).all():
        raise CooldownStudyError("continuous comparator model family drifted")
    if set(continuous_oof["side"].astype(str)) - set(SIDES):
        raise CooldownStudyError("continuous comparator contains a pooled side")
    if not continuous_oof["economic_denominator_identity"].eq(
        "exact_label_economic"
    ).all():
        raise CooldownStudyError("continuous comparator denominator drifted")
    boolean_keys = set(
        zip(
            oof["economic_scope_identity"].astype(str),
            oof["side"].astype(str),
            oof["feature_block"].astype(str),
            oof["opportunity_id"].astype(str),
            strict=True,
        )
    )
    continuous_keys = set(
        zip(
            continuous_oof["economic_scope_identity"].astype(str),
            continuous_oof["side"].astype(str),
            continuous_oof["feature_block"].astype(str),
            continuous_oof["opportunity_id"].astype(str),
            strict=True,
        )
    )
    if continuous_keys != boolean_keys:
        raise CooldownStudyError(
            "continuous comparator is not paired to Boolean outer OOF"
        )
    _validate_report_support_audits(
        report=report,
        binding=binding,
        oof_rows=oof,
    )
    if (
        manifest.get("continuous_state_comparator_completed") is not True
        or manifest.get("continuous_state_comparator_may_replace_boolean_policy")
        is not False
    ):
        raise CooldownStudyError("continuous comparator manifest contract drifted")
    day_panels = manifest.get("day_panels")
    if not isinstance(day_panels, list) or len(day_panels) != int(
        manifest.get("exact_label_manifest_day_count", -1)
    ):
        raise CooldownStudyError("admitted day-panel denominator drifted")
    if (
        manifest.get("nominal_mechanics_day_count") != 50
        or manifest.get("formal_exact_label_day_count") != 41
        or manifest.get("reduced_support_diagnostic_day_count") != 9
        or manifest.get("economic_statistics_denominator")
        != "exact_label_economic_only"
        or manifest.get("reduced_support_pooled_into_economic_statistics") is not False
        or manifest.get("reduced_support_economic_labels_manufactured") is not False
    ):
        raise CooldownStudyError("study manifest denominator contract drifted")
    for row in day_panels:
        path = root / str(row.get("manifest_path", ""))
        if not path.is_file() or _sha256(path) != row.get("manifest_sha256"):
            raise CooldownStudyError("an admitted day-panel manifest drifted")
        source_path = Path(str(row.get("source_manifest_path", ""))).resolve()
        source_hash = str(row.get("source_manifest_sha256", ""))
        if not source_path.is_file() or _sha256(source_path) != source_hash:
            raise CooldownStudyError("an admitted day-panel source manifest drifted")
        _validate_day_panel(
            path.parent,
            source=DaySource(
                day=_normalize_day(row.get("day")),
                panel_role=str(row.get("panel_role", "")),
                manifest_path=source_path,
                manifest_sha256=source_hash,
            ),
        )
    predicate_artifacts = manifest.get("predicate_artifacts")
    if not isinstance(predicate_artifacts, Mapping):
        raise CooldownStudyError("predicate artifact bindings are missing")
    for row in predicate_artifacts.values():
        if not isinstance(row, Mapping):
            raise CooldownStudyError("predicate artifact binding is invalid")
        path = root / str(row.get("path", ""))
        if not path.is_file() or _sha256(path) != row.get("sha256"):
            raise CooldownStudyError("predicate artifact input hash drifted")
    return manifest


def run_study(
    *,
    formal_panel_manifest: Path,
    predicate_bundle: Path,
    output: Path,
    config: StudyConfig | None = None,
) -> dict[str, Any]:
    """Resume day admissions, run nested OOF, and atomically publish results."""

    settings = config or StudyConfig()
    output = Path(output).expanduser().resolve()
    formal, sources = _validate_formal_panel_manifest(
        formal_panel_manifest,
        allow_engineering_panel=settings.engineering_allow_nonformal_panel,
    )
    bundle = _load_predicate_bundle(predicate_bundle)
    binding = _binding_payload(
        formal_panel_manifest=Path(formal_panel_manifest),
        predicate_bundle=bundle,
        config=settings,
    )
    if output.exists():
        existing = validate_study_output(output)
        if existing.get("binding_sha256") != _canonical_sha256(binding):
            raise CooldownStudyError("existing study output binding differs from this request")
        return existing
    work_root = output.parent / f".{output.name}.work"
    work_root.mkdir(parents=True, exist_ok=True)
    lock = _acquire_lock(work_root / ".study.lock")
    try:
        progress_path = work_root / "progress.json"
        progress = _load_or_initialize_progress(
            path=progress_path,
            binding=binding,
            sources=sources,
        )
        day_panels = _admit_day_panels(
            sources=sources,
            work_root=work_root,
            progress_path=progress_path,
            progress=progress,
        )
        denominator, economic = _combine_day_panels(day_panels)
        plans = _build_fold_plans(
            sources=sources,
            economic=economic,
            config=settings,
        )
        bundle = _fit_fold_local_m0_artifacts(
            bundle=bundle,
            denominator=denominator,
            plans=plans,
            config=settings,
            artifact_root=work_root / "fold_local_m0_predicates",
        )
        scopes, oof_rows, continuous_oof_rows = _scope_reports(
            economic=economic,
            denominator=denominator,
            sources=sources,
            bundle=bundle,
            plans=plans,
            config=settings,
            formal_full_support_run=formal.get("formal_full_support_run") is True,
        )
        progress["economic_outcomes_read"] = True
        progress["nested_oof_completed"] = True
        progress["action_authorized"] = False
        progress["live_authorized"] = False
        _atomic_json(progress_path, progress)
        denominator_audit = _denominator_report(denominator, sources=sources)
        day_denominators = _day_denominator_report(
            sources=sources,
            formal_full_support_run=formal.get("formal_full_support_run") is True,
        )
        report = _json_safe(
            {
                "schema_version": REPORT_SCHEMA,
                "identity": STUDY_IDENTITY,
                "input_binding_sha256": _canonical_sha256(binding),
                "formal_panel": {
                    "path": str(Path(formal_panel_manifest).resolve()),
                    "sha256": _sha256(Path(formal_panel_manifest).resolve()),
                    "declared_day_count": int(formal["day_count"]),
                    "exact_label_manifest_day_count": len(sources),
                    "contains_reduced_support_days": False,
                    "economic_statistics_denominator": "exact_label_economic",
                },
                "predicate_bundle": {
                    "path": str(bundle.path),
                    "sha256": bundle.sha256,
                    "canonical_sha256": bundle.canonical_sha256,
                    "2025_book_trade_clock_separated": True,
                    "2025_reference_frames_joined": False,
                    "strict_2026_target_same_clause_book_trade_allowed": True,
                    "m0_thresholds_fit_within_frozen_chronological_folds": True,
                },
                "day_denominators": day_denominators,
                "denominator_audit": denominator_audit,
                "partial_identification": {
                    "point_label_common_support_filter_applied": True,
                    "unsupported_opportunity_sensitivity_implemented": False,
                    "unresolved": denominator_audit["partial_identification_unresolved"],
                    "research_supported_promotion_blocked": denominator_audit[
                        "research_supported_promotion_blocked_by_partial_identification"
                    ],
                },
                "exact_label_economic_results": scopes,
                "continuous_state_comparator": {
                    "status": "completed",
                    "model_family": (
                        "raw_state_multioutput_regression_tree_diagnostic"
                    ),
                    "config": settings.continuous_comparator.payload(),
                    "sklearn_version": sklearn.__version__,
                    "capacity_selection": "inner_chronological_folds_only",
                    "outer_fold_role": "execute_inner_frozen_capacity_only",
                    "may_replace_boolean_policy": False,
                    "may_grant_action_or_live": False,
                },
                "legacy_panel_scopes_field_emitted": False,
                "buy_sell_pooled": False,
                "deployment_gate_order": "after_outer_oof_only",
                "validation_read": False,
                "sealed_holdout_read": False,
                "permissions": {
                    "research_evidence_only": True,
                    "validation_read": False,
                    "sealed_holdout_read": False,
                    "action_authorized": False,
                    "live_authorized": False,
                },
            }
        )
        return _publish_output(
            output=output,
            binding=binding,
            sources=sources,
            bundle=bundle,
            denominator=denominator,
            economic=economic,
            oof_rows=oof_rows,
            continuous_oof_rows=continuous_oof_rows,
            report=report,
            day_panels=day_panels,
        )
    finally:
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        lock.close()


def finalize_study(output: Path) -> dict[str, Any]:
    """Finalize means validate the already atomically admitted run."""

    return validate_study_output(output)


def _config_from_path(path: Path | None) -> StudyConfig:
    if path is None:
        return StudyConfig()
    raw = _load_json(Path(path).expanduser().resolve())
    search_raw = raw.get("search", {})
    if not isinstance(search_raw, Mapping):
        raise CooldownStudyError("study config search section is invalid")
    continuous_raw = raw.get("continuous_comparator", {})
    if not isinstance(continuous_raw, Mapping):
        raise CooldownStudyError(
            "study config continuous comparator section is invalid"
        )
    try:
        search = SearchConfig(**dict(search_raw))
        continuous = ContinuousComparatorConfig(
            max_depth_candidates=tuple(
                int(value)
                for value in continuous_raw.get("max_depth_candidates", (2, 4))
            ),
            min_samples_leaf=int(continuous_raw.get("min_samples_leaf", 20)),
            random_state=int(continuous_raw.get("random_state", 20260810)),
        )
        return StudyConfig(
            outer_folds=int(raw.get("outer_folds", 4)),
            outer_minimum_train_days=int(raw.get("outer_minimum_train_days", 12)),
            search=search,
            economic_epsilon_usdc=float(raw.get("economic_epsilon_usdc", 0.0)),
            minimum_deployment_action_rate=float(raw.get("minimum_deployment_action_rate", 0.0)),
            minimum_deployment_campaigns=int(raw.get("minimum_deployment_campaigns", 2)),
            minimum_deployment_days=int(raw.get("minimum_deployment_days", 2)),
            engineering_allow_nonformal_panel=bool(
                raw.get("engineering_allow_nonformal_panel", False)
            ),
            continuous_comparator=continuous,
        )
    except (TypeError, ValueError) as exc:
        raise CooldownStudyError("study config is invalid") from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_inputs(command: argparse.ArgumentParser, *, include_output: bool) -> None:
        command.add_argument("--formal-panel-manifest", type=Path, required=True)
        command.add_argument("--predicate-bundle", type=Path, required=True)
        command.add_argument("--study-config", type=Path)
        if include_output:
            command.add_argument("--output", type=Path, required=True)

    add_inputs(subparsers.add_parser("preflight"), include_output=False)
    add_inputs(subparsers.add_parser("run"), include_output=True)
    for name in ("finalize", "validate"):
        command = subparsers.add_parser(name)
        command.add_argument("--output", type=Path, required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "preflight":
            payload = preflight_study(
                formal_panel_manifest=args.formal_panel_manifest,
                predicate_bundle=args.predicate_bundle,
                config=_config_from_path(args.study_config),
            )
        elif args.command == "run":
            payload = run_study(
                formal_panel_manifest=args.formal_panel_manifest,
                predicate_bundle=args.predicate_bundle,
                output=args.output,
                config=_config_from_path(args.study_config),
            )
        elif args.command == "finalize":
            payload = finalize_study(args.output)
        else:
            payload = validate_study_output(args.output)
    except CooldownStudyError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(_json_safe(payload), indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "CooldownStudyError",
    "IDENTITY",
    "MANIFEST_SCHEMA",
    "PREDICATE_BUNDLE_SCHEMA",
    "REPORT_SCHEMA",
    "STUDY_IDENTITY",
    "StudyConfig",
    "finalize_study",
    "main",
    "preflight_study",
    "run_study",
    "validate_study_output",
]
