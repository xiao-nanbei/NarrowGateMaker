"""Owner-route nested OOF for modelled-queue cooldown-duration labels.

This successor is intentionally separate from the strict-native v9 identity.
It binds an immutable configuration, the historical v1 eight-arm modelled-
queue labels, and an independently materialized multichannel feature panel.
It may produce owner-route exploratory support for a later sequential replay;
it can never create strict-queue, action, or live authority.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import errno
import fnmatch
import hashlib
import inspect
import json
import math
import multiprocessing
import os
import platform
import shutil
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from statistics import NormalDist
from typing import Any

import numpy as np
import pandas as pd
import pyarrow
import sklearn
from sklearn.tree import DecisionTreeRegressor

from data_paths import resolve_portable_path
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_nested_oof import (
    CONTROL_ACTION,
    FEATURE_BLOCKS,
    BooleanCooldownPolicy,
    ChronologicalFold,
    SearchConfig,
    duration_vocabulary,
    expanding_chronological_folds,
    generate_bounded_candidates,
)
from research.governance.public_machine_projection import (
    PublicMachineProjectionError,
    source_document_path,
    source_identity_sha256,
)

IDENTITY = "causal_multichannel_window_boolean_cooldown_duration_v2_owner_modeled_queue_v1"
CONFIG_SCHEMA = (
    "causal_multichannel_window_boolean_cooldown_duration_v2.owner_modeled_queue.study_config.v1"
)
SPEC_SCHEMA = "causal_multichannel_window_boolean_cooldown_duration_v2.owner_modeled_queue.spec.v1"
OUTPUT_SCHEMA = f"{IDENTITY}.nested_oof_output.v1"
MANIFEST_SCHEMA = f"{IDENTITY}.atomic_admission.v1"
EXECUTION_AMENDMENT_SCHEMA = f"{IDENTITY}.oof_execution_amendment.v1"
EVIDENCE_ROUTE = "owner_risk_accepted_modelled_queue_exploration"
QUEUE_AUTHORITY = "modelled_queue_without_exchange_queue_authority"
EXPECTED_FEATURE_MANIFEST_IDENTITY = (
    "causal_multichannel_window_boolean_cooldown_duration_v2."
    "owner_modeled_queue_feature_panel.v1"
)
PREDICATE_BUNDLE_SCHEMA = (
    "causal_multichannel_window_boolean_cooldown_duration_v2."
    "multiday_label_panel_nested_oof.v1.predicate_bundle.v1"
)
PREDICATE_ARTIFACT_SCHEMA = (
    "causal_multichannel_window_boolean_cooldown_duration_v2.predicate_artifact.v1"
)
PREDICATE_ARTIFACT_IDENTITY = "causal_multichannel_window_boolean_cooldown_duration_v2"
PREDICATE_CLOCKS = ("book", "trade")
FEATURE_PANEL_SUCCESS_NAME = "_PANEL_SUCCESS"
SIDES = ("BUY", "SELL")
PANEL_SCOPES = (
    "prefix40_modeled_label_development",
    "prefix33_raw_m2_common_support",
)
NOT_RUN_PANELS = ("added10", "pooled50")
DEFAULT_SPEC_PATH = Path(
    "research/families/f05_fill_quality_quote_ev/docs/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_"
    "owner_modeled_queue_v1_spec_20260811.json"
)
DEFAULT_SPEC_SHA256 = "362cb1848da44e8b6f4e274ab4e99f7077e9cea7efafcbd77cc404e53774c666"
DEFAULT_CONFIG_PATH = Path(
    "research/families/f05_fill_quality_quote_ev/docs/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_"
    "owner_modeled_queue_v1_study_config_20260811.json"
)
DEFAULT_CONFIG_SHA256 = "636074fdbf52b363bcde953926db0a529e5f9ac349324cddd4473f70f56e6659"


class ModeledOofError(RuntimeError):
    """Raised when a frozen identity, panel, fold, or output drifts."""


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


def _require_exact_source_document(
    path: Path,
    expected_sha256: str,
    *,
    role: str,
) -> Path:
    """Verify a public projection, then return its exact execution source."""

    public_path = Path(path).expanduser().resolve()
    if len(str(expected_sha256)) != 64:
        raise ModeledOofError(f"{role} expected SHA256 is invalid")
    try:
        observed_source_sha256 = source_identity_sha256(public_path)
        exact_source_path = source_document_path(public_path, require_private=True)
    except (OSError, PublicMachineProjectionError) as exc:
        raise ModeledOofError(f"{role} exact source is unavailable or invalid") from exc
    if observed_source_sha256 != expected_sha256:
        raise ModeledOofError(f"{role} SHA256 mismatch")
    return exact_source_path


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ModeledOofError(f"cannot read JSON object: {path}") from exc
    if not isinstance(payload, dict):
        raise ModeledOofError(f"JSON root is not an object: {path}")
    return payload


def _normalize_day(value: Any) -> str:
    try:
        parsed = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise ModeledOofError(f"invalid UTC day: {value!r}") from exc
    if parsed.tzinfo is not None:
        parsed = parsed.tz_convert("UTC").tz_localize(None)
    if parsed != parsed.normalize():
        raise ModeledOofError(f"UTC day includes a time component: {value!r}")
    return parsed.strftime("%Y-%m-%d")


def runtime_library_versions() -> dict[str, str]:
    """Return the exact runtime versions bound into every output."""

    return {
        "python": platform.python_version(),
        "numpy": np.__version__,
        "pandas": pd.__version__,
        "pyarrow": pyarrow.__version__,
        "scikit_learn": sklearn.__version__,
    }


@dataclass(frozen=True, slots=True)
class ArtifactSpec:
    manifest_path: Path
    manifest_sha256: str
    table_globs: tuple[str, ...]
    expected_identity: str | None


@dataclass(frozen=True, slots=True)
class PredicateDefinition:
    name: str
    source_field: str
    block: str
    kind: str
    clock_group: str
    side: str
    threshold: float | None
    quantile: float | None


@dataclass(frozen=True, slots=True)
class PredicateArtifactBinding:
    path: Path
    file_sha256: str
    canonical_sha256: str
    side: str
    clock_group: str
    source_clock_identity: Mapping[str, str]
    reference_days: tuple[str, ...]
    reference_identity_sha256: str
    definitions: tuple[PredicateDefinition, ...]


@dataclass(frozen=True, slots=True)
class PredicateBundleArtifact:
    path: Path
    file_sha256: str
    canonical_sha256: str
    artifacts: Mapping[str, PredicateArtifactBinding]
    artifact_binding_sha256: str


@dataclass(frozen=True, slots=True)
class PredicateMaterializationBinding:
    bundle: PredicateBundleArtifact
    definitions_by_block_side: Mapping[
        str, Mapping[str, tuple[PredicateDefinition, ...]]
    ]
    predicate_names_by_block: Mapping[str, tuple[str, ...]]
    source_fields_by_block: Mapping[str, tuple[str, ...]]
    materialization_identity_sha256: str


@dataclass(frozen=True, slots=True)
class TimestampField:
    name: str
    unit: str

    def __post_init__(self) -> None:
        if not self.name or self.unit not in {"ns", "us", "ms", "s"}:
            raise ModeledOofError("timestamp fields require a name and ns/us/ms/s unit")


@dataclass(frozen=True, slots=True)
class ColumnContract:
    opportunity: str = "opportunity_id"
    day: str = "utc_day"
    side: str = "side"
    role: str = "role_at_fill"
    campaign: str = "campaign_id"
    action: str = "duration_policy_id"
    outcome: str = "assignment_to_washout_value_usdc"
    assignment_time: TimestampField = TimestampField("assignment_ts_ns", "ns")
    observation_end: tuple[TimestampField, ...] = (TimestampField("washout_ts_ns", "ns"),)
    eligible: str = "training_label_eligible"
    right_censored: str = "right_censored"
    joint_censored: str = "joint_censored"
    exact_queue_eligible: str = "exact_queue_policy_eligible"


@dataclass(frozen=True, slots=True)
class FeatureBlockSpec:
    boolean_predicates: tuple[str, ...]
    continuous_features: tuple[str, ...]

    def __post_init__(self) -> None:
        for values, label in (
            (self.boolean_predicates, "Boolean predicate"),
            (self.continuous_features, "continuous feature"),
        ):
            if not values or len(values) != len(set(values)) or any(not value for value in values):
                raise ModeledOofError(f"{label} list must be nonempty and unique")


@dataclass(frozen=True, slots=True)
class ContinuousConfig:
    max_depth_candidates: tuple[int, ...]
    min_samples_leaf: int
    minimum_train_rows_per_action: int
    random_state: int

    def __post_init__(self) -> None:
        if (
            not self.max_depth_candidates
            or tuple(sorted(set(self.max_depth_candidates))) != self.max_depth_candidates
            or any(value < 1 for value in self.max_depth_candidates)
        ):
            raise ModeledOofError("continuous depth candidates must be positive and sorted")
        if self.min_samples_leaf < 1 or self.minimum_train_rows_per_action < 1:
            raise ModeledOofError("continuous support settings must be positive")


@dataclass(frozen=True, slots=True)
class DeploymentConfig:
    economic_epsilon_usdc: float
    minimum_action_rate: float
    minimum_action_campaigns: int
    minimum_action_days: int
    require_full_identification: bool
    require_outer_fold_nonbaseline_action: bool
    require_opener_and_add_reporting: bool

    def __post_init__(self) -> None:
        if not math.isfinite(self.economic_epsilon_usdc):
            raise ModeledOofError("economic epsilon must be finite")
        if not 0.0 <= self.minimum_action_rate <= 1.0:
            raise ModeledOofError("minimum action rate must be in [0, 1]")
        if self.minimum_action_campaigns < 1 or self.minimum_action_days < 1:
            raise ModeledOofError("deployment support counts must be positive")


@dataclass(frozen=True, slots=True)
class FrozenConfig:
    path: Path
    sha256: str
    payload: Mapping[str, Any]
    spec_path: Path
    spec_sha256: str
    spec_payload: Mapping[str, Any]
    labels: ArtifactSpec
    features: ArtifactSpec
    columns: ColumnContract
    prefix40_days: tuple[str, ...]
    added10_days: tuple[str, ...]
    report_scopes: tuple[str, ...]
    panel_days: Mapping[str, tuple[str, ...]]
    panel_feature_blocks: Mapping[str, tuple[str, ...]]
    not_run_panels: Mapping[str, Mapping[str, Any]]
    outer_folds: Mapping[str, tuple[ChronologicalFold, ...]]
    feature_blocks: Mapping[str, FeatureBlockSpec]
    search: SearchConfig
    continuous: ContinuousConfig
    deployment: DeploymentConfig
    minimum_inner_identified_weight_fraction: float
    uplift_bounds_usdc: tuple[float, float] | None
    predicate_channel_groups: Mapping[str, str]
    predicate_semantic_groups: Mapping[str, str]
    predicate_clock_groups: Mapping[str, str]
    code_bindings: tuple[tuple[Path, str], ...]
    expected_library_versions: Mapping[str, str]
    predicate_materialization: PredicateMaterializationBinding | None = None


@dataclass(frozen=True, slots=True)
class InputBinding:
    manifest_path: Path
    manifest_sha256: str
    table_paths: tuple[Path, ...]
    table_sha256: Mapping[str, str]
    manifest_identity: str | None


@dataclass(frozen=True, slots=True)
class ExecutionAmendmentBinding:
    path: Path
    sha256: str
    execution_identity_sha256: str
    artifact_bindings: Mapping[str, Mapping[str, Any]]
    code_bindings: tuple[tuple[Path, str], ...]
    library_versions: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class PreparedPanel:
    metadata: pd.DataFrame
    outcomes: pd.DataFrame
    supported: pd.DataFrame
    features: pd.DataFrame
    observation_end_ts_ns: pd.Series
    unsupported_reasons: pd.DataFrame
    redacted_finite_outcomes: int


@dataclass(frozen=True, slots=True)
class PurgeAudit:
    fold_id: str
    stage: str
    side: str
    train_days: tuple[str, ...]
    test_days: tuple[str, ...]
    test_boundary_ts_ns: int
    train_opportunities_before: int
    train_opportunities_after: int
    purged_cross_boundary: int
    purged_unknown_observation_end: int


@dataclass(frozen=True, slots=True)
class PartialIdentification:
    denominator_opportunities: int
    identified_opportunities: int
    unidentified_opportunities: int
    identified_weight_fraction: float
    selected_action_unsupported: int
    control_action_unsupported: int
    identified_mean_usdc: float | None
    identified_standard_error_usdc: float | None
    identified_lcb_usdc: float | None
    identified_ucb_usdc: float | None
    population_lower_bound_usdc: float | None
    population_upper_bound_usdc: float | None
    uplift_bounds_usdc: tuple[float, float] | None
    point_identified: bool


@dataclass(frozen=True, slots=True)
class MethodResult:
    side: str
    feature_block: str
    panel_scope: str
    method: str
    oof_rows: pd.DataFrame
    fold_reports: tuple[Mapping[str, Any], ...]
    partial_identification: PartialIdentification
    deployment_gate: Mapping[str, Any]
    selected_candidates: tuple[Mapping[str, Any], ...]
    purge_audits: tuple[PurgeAudit, ...]


@dataclass(frozen=True, slots=True)
class ComparisonCell:
    panel_scope: str
    side: str
    feature_block: str

    @property
    def identity(self) -> str:
        return f"{self.panel_scope}/{self.side}/{self.feature_block}"


@dataclass(frozen=True, slots=True)
class ComparisonCellResult:
    cell: ComparisonCell
    boolean: MethodResult
    continuous: MethodResult


_WORKER_PANEL: PreparedPanel | None = None
_WORKER_CONFIG: FrozenConfig | None = None


def _timestamp_field(payload: Mapping[str, Any]) -> TimestampField:
    try:
        return TimestampField(str(payload["name"]), str(payload["unit"]))
    except (KeyError, TypeError) as exc:
        raise ModeledOofError("timestamp field is incomplete") from exc


def _column_contract(payload: Mapping[str, Any]) -> ColumnContract:
    defaults = ColumnContract()
    observation = payload.get("observation_end")
    if observation is None:
        observation_fields = defaults.observation_end
    elif isinstance(observation, list):
        observation_fields = tuple(_timestamp_field(value) for value in observation)
    else:
        raise ModeledOofError("observation_end must be a list")
    assignment_payload = payload.get("assignment_time")
    assignment = (
        defaults.assignment_time
        if assignment_payload is None
        else _timestamp_field(assignment_payload)
    )
    values = {
        name: str(payload.get(name, getattr(defaults, name)))
        for name in (
            "opportunity",
            "day",
            "side",
            "role",
            "campaign",
            "action",
            "outcome",
            "eligible",
            "right_censored",
            "joint_censored",
            "exact_queue_eligible",
        )
    }
    return ColumnContract(
        **values,
        assignment_time=assignment,
        observation_end=observation_fields,
    )


def _feature_manifest_schema(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    schema = payload.get("feature_schema", payload)
    if not isinstance(schema, Mapping):
        raise ModeledOofError("feature manifest schema is invalid")
    return schema


def _require_feature_panel_admission(manifest_path: Path, manifest_sha256: str) -> Path:
    success_path = manifest_path.parent / FEATURE_PANEL_SUCCESS_NAME
    if not success_path.is_file():
        raise ModeledOofError("multichannel feature panel lacks _PANEL_SUCCESS admission")
    try:
        admitted_sha256 = success_path.read_text(encoding="ascii").strip()
    except OSError as exc:
        raise ModeledOofError("cannot read multichannel feature panel admission marker") from exc
    if admitted_sha256 != manifest_sha256:
        raise ModeledOofError("multichannel feature panel admission marker SHA256 mismatch")
    return success_path.resolve()


def _is_sha256(value: Any) -> bool:
    text = str(value)
    return len(text) == 64 and all(character in "0123456789abcdef" for character in text)


def _verified_canonical_identity(payload: Mapping[str, Any], *, label: str) -> str:
    observed = str(payload.get("canonical_sha256", ""))
    canonical_payload = dict(payload)
    canonical_payload.pop("canonical_sha256", None)
    if not _is_sha256(observed) or _canonical_sha256(canonical_payload) != observed:
        raise ModeledOofError(f"{label} canonical identity drifted")
    return observed


def _definition_signature(definition: PredicateDefinition) -> tuple[Any, ...]:
    return (
        definition.name,
        definition.source_field,
        definition.block,
        definition.kind,
        definition.clock_group,
        definition.quantile,
    )


def _parse_predicate_artifact(
    path: Path,
    *,
    expected_file_sha256: str,
    side: str,
    clock_group: str,
) -> PredicateArtifactBinding:
    if not path.is_file():
        raise ModeledOofError(f"2025 predicate artifact is missing: {path}")
    if not _is_sha256(expected_file_sha256) or _sha256(path) != expected_file_sha256:
        raise ModeledOofError(f"2025 predicate artifact SHA256 mismatch: {path}")
    payload = _load_json(path)
    canonical_sha256 = _verified_canonical_identity(
        payload, label=f"2025 {clock_group}/{side} predicate artifact"
    )
    expected_clock = {
        "book": "provider_local_receive_time_right_boundary_100ms",
        "trade": "binance_exchange_trade_time",
    }[clock_group]
    if (
        payload.get("schema_version") != PREDICATE_ARTIFACT_SCHEMA
        or payload.get("identity") != PREDICATE_ARTIFACT_IDENTITY
        or payload.get("side") != side
        or payload.get("source_role") != "outcome_blind_2025_single_channel"
        or payload.get("clock_separated_2025") is not True
        or payload.get("clause_clock_policy") != "single_book_or_trade_clock_group"
        or payload.get("cross_channel_threshold_fitting") is not False
        or payload.get("source_clock_identity") != {"shared": expected_clock}
    ):
        raise ModeledOofError(f"2025 {clock_group}/{side} predicate artifact identity drifted")

    raw_reference_days = payload.get("reference_days")
    if not isinstance(raw_reference_days, list) or not raw_reference_days:
        raise ModeledOofError("2025 predicate artifact reference days are missing")
    reference_days = tuple(_normalize_day(value) for value in raw_reference_days)
    if (
        reference_days != tuple(sorted(set(reference_days)))
        or any(not day.startswith("2025-") for day in reference_days)
    ):
        raise ModeledOofError("predicate artifact reference days are not unique 2025 days")
    reference_identity = str(payload.get("reference_identity_sha256", ""))
    if not _is_sha256(reference_identity):
        raise ModeledOofError("predicate artifact reference identity is invalid")

    raw_quantiles = payload.get("quantiles")
    if not isinstance(raw_quantiles, list) or not raw_quantiles:
        raise ModeledOofError("predicate artifact quantile vocabulary is missing")
    try:
        quantiles = tuple(float(value) for value in raw_quantiles)
    except (TypeError, ValueError) as exc:
        raise ModeledOofError("predicate artifact quantiles are invalid") from exc
    if (
        quantiles != tuple(sorted(set(quantiles)))
        or any(not math.isfinite(value) or not 0.0 < value < 1.0 for value in quantiles)
    ):
        raise ModeledOofError("predicate artifact quantiles are invalid")

    raw_input_schema = payload.get("input_schema")
    if not isinstance(raw_input_schema, list) or not raw_input_schema:
        raise ModeledOofError("predicate artifact input schema is missing")
    input_schema: dict[str, str] = {}
    for row in raw_input_schema:
        if (
            not isinstance(row, list)
            or len(row) != 2
            or not isinstance(row[0], str)
            or not isinstance(row[1], str)
            or row[0] in input_schema
        ):
            raise ModeledOofError("predicate artifact input schema is invalid")
        input_schema[row[0]] = row[1]
    if input_schema.get("side") != "text":
        raise ModeledOofError("predicate artifact input schema lacks the side identity")

    raw_definitions = payload.get("definitions")
    if not isinstance(raw_definitions, list) or not raw_definitions:
        raise ModeledOofError("predicate artifact definitions are missing")
    definitions: list[PredicateDefinition] = []
    seen_names: set[str] = set()
    forbidden_source_fragments = (
        "assignment_to_washout",
        "terminal_value",
        "terminal_pnl",
        "economic_outcome",
        "duration_policy_id",
        "reward",
        "uplift",
    )
    for row in raw_definitions:
        if not isinstance(row, Mapping):
            raise ModeledOofError("predicate artifact definition is invalid")
        name = str(row.get("name", ""))
        source_field = str(row.get("source_field", ""))
        block = str(row.get("block", ""))
        kind = str(row.get("kind", ""))
        row_clock = str(row.get("clock_group", ""))
        if (
            not name
            or name in seen_names
            or not source_field
            or block not in {"R0", "M1", "M2"}
            or kind not in {"preserved_tri", "quantile_ge"}
            or row_clock != clock_group
            or input_schema.get(source_field) != "numeric"
            or any(fragment in source_field.lower() for fragment in forbidden_source_fragments)
        ):
            raise ModeledOofError("predicate artifact definition identity is invalid")
        threshold_raw = row.get("threshold")
        quantile_raw = row.get("quantile")
        if kind == "preserved_tri":
            if name != source_field or threshold_raw is not None or quantile_raw is not None:
                raise ModeledOofError("preserved tri-state predicate definition drifted")
            threshold = None
            quantile = None
        else:
            if isinstance(threshold_raw, bool) or isinstance(quantile_raw, bool):
                raise ModeledOofError("quantile predicate threshold is invalid")
            try:
                threshold = float(threshold_raw)
                quantile = float(quantile_raw)
            except (TypeError, ValueError) as exc:
                raise ModeledOofError("quantile predicate threshold is invalid") from exc
            if not math.isfinite(threshold) or quantile not in quantiles:
                raise ModeledOofError("quantile predicate threshold is invalid")
            quantile_code = int(round(quantile * 10_000.0))
            expected_name = (
                f"tri::quantile::{source_field}::ge::q{quantile_code:04d}"
            )
            if name != expected_name:
                raise ModeledOofError("quantile predicate name/threshold identity drifted")
        definitions.append(
            PredicateDefinition(
                name=name,
                source_field=source_field,
                block=block,
                kind=kind,
                clock_group=clock_group,
                side=side,
                threshold=threshold,
                quantile=quantile,
            )
        )
        seen_names.add(name)
    return PredicateArtifactBinding(
        path=path,
        file_sha256=expected_file_sha256,
        canonical_sha256=canonical_sha256,
        side=side,
        clock_group=clock_group,
        source_clock_identity={"shared": expected_clock},
        reference_days=reference_days,
        reference_identity_sha256=reference_identity,
        definitions=tuple(definitions),
    )


def load_2025_predicate_bundle(contract: Mapping[str, Any]) -> PredicateBundleArtifact:
    """Verify the outcome-blind bundle and all four side/clock artifacts."""

    if (
        contract.get("role") != "predicate_threshold_scale_support_and_missingness_only"
        or contract.get("economic_outcomes_read") is not False
        or contract.get("cooldown_labels_generated") is not False
        or contract.get("queue_or_lifecycle_authority") is not False
        or contract.get("source_identity_is_model_input") is not False
    ):
        raise ModeledOofError("owner spec 2025 predicate input permissions drifted")
    bundle_path = Path(str(contract.get("predicate_bundle_path", ""))).expanduser().resolve()
    expected_bundle_sha256 = str(contract.get("predicate_bundle_file_sha256", ""))
    if not bundle_path.is_file():
        raise ModeledOofError(f"2025 predicate bundle is missing: {bundle_path}")
    if (
        not _is_sha256(expected_bundle_sha256)
        or _sha256(bundle_path) != expected_bundle_sha256
    ):
        raise ModeledOofError("2025 predicate bundle SHA256 mismatch")
    payload = _load_json(bundle_path)
    canonical_sha256 = _verified_canonical_identity(
        payload, label="2025 predicate bundle"
    )
    strict_target = payload.get("strict_2026_target_snapshot")
    if (
        payload.get("schema_version") != PREDICATE_BUNDLE_SCHEMA
        or payload.get("identity") != PREDICATE_ARTIFACT_IDENTITY
        or payload.get("m0_artifacts") != []
        or payload.get("cross_clock_clause_authorized") is not False
        or payload.get("cross_clock_clause_scope") != "2025_reference_rows_only"
        or not isinstance(strict_target, Mapping)
        or strict_target.get("authority_owner") != "2026_strict_denominator_study"
        or strict_target.get("book_trade_predicates_may_be_combined_by_study") is not True
        or strict_target.get("required_condition")
        != (
            "book and trade predicates are evaluated on the same admitted strict target "
            "snapshot and causal feature-ready cutoff"
        )
    ):
        raise ModeledOofError("2025 predicate bundle identity/schema drifted")

    root = bundle_path.parent.resolve()
    artifacts: dict[str, PredicateArtifactBinding] = {}
    reference_days: tuple[str, ...] | None = None
    seen_definition_names: dict[str, set[str]] = {side: set() for side in SIDES}
    for clock_group in PREDICATE_CLOCKS:
        clock_payload = payload.get(clock_group)
        if not isinstance(clock_payload, Mapping) or set(clock_payload) != set(SIDES):
            raise ModeledOofError(
                f"2025 predicate bundle must bind BUY/SELL {clock_group} artifacts"
            )
        for side in SIDES:
            row = clock_payload.get(side)
            if not isinstance(row, Mapping):
                raise ModeledOofError("2025 predicate artifact binding is invalid")
            relative = Path(str(row.get("path", "")))
            if relative.is_absolute():
                raise ModeledOofError("2025 predicate artifact path must be bundle-relative")
            artifact_path = (root / relative).resolve()
            if not artifact_path.is_relative_to(root):
                raise ModeledOofError("2025 predicate artifact escapes its bundle root")
            artifact = _parse_predicate_artifact(
                artifact_path,
                expected_file_sha256=str(row.get("sha256", "")),
                side=side,
                clock_group=clock_group,
            )
            if reference_days is None:
                reference_days = artifact.reference_days
            elif artifact.reference_days != reference_days:
                raise ModeledOofError("2025 predicate artifact reference-day panels drifted")
            names = {definition.name for definition in artifact.definitions}
            if seen_definition_names[side] & names:
                raise ModeledOofError("2025 predicate name repeats across clock artifacts")
            seen_definition_names[side].update(names)
            artifacts[f"{clock_group}.{side}"] = artifact

        buy = {definition.name: definition for definition in artifacts[f"{clock_group}.BUY"].definitions}
        sell = {
            definition.name: definition
            for definition in artifacts[f"{clock_group}.SELL"].definitions
        }
        if set(buy) != set(sell) or any(
            _definition_signature(buy[name]) != _definition_signature(sell[name])
            for name in buy
        ):
            raise ModeledOofError(
                f"2025 {clock_group} BUY/SELL predicate structures drifted"
            )

    binding_payload = {
        "bundle": {
            "path": str(bundle_path),
            "file_sha256": expected_bundle_sha256,
            "canonical_sha256": canonical_sha256,
        },
        "artifacts": {
            name: {
                "path": str(artifact.path),
                "file_sha256": artifact.file_sha256,
                "canonical_sha256": artifact.canonical_sha256,
                "reference_identity_sha256": artifact.reference_identity_sha256,
                "source_clock_identity": dict(artifact.source_clock_identity),
            }
            for name, artifact in sorted(artifacts.items())
        },
        "economic_outcomes_read": False,
    }
    return PredicateBundleArtifact(
        path=bundle_path,
        file_sha256=expected_bundle_sha256,
        canonical_sha256=canonical_sha256,
        artifacts=artifacts,
        artifact_binding_sha256=_canonical_sha256(binding_payload),
    )


def _predicate_channel_from_source(source_field: str) -> str:
    if source_field.startswith("value::"):
        return source_field.removeprefix("value::").split("::", 1)[0]
    if source_field.startswith("tri::"):
        return source_field.removeprefix("tri::").split("__h", 1)[0]
    return "outcome_blind_2025_threshold"


def bind_2025_predicate_materialization(
    bundle: PredicateBundleArtifact,
    *,
    feature_blocks: Mapping[str, FeatureBlockSpec],
    predicate_channel_groups: Mapping[str, str],
    predicate_semantic_groups: Mapping[str, str],
    predicate_clock_groups: Mapping[str, str],
) -> tuple[
    PredicateMaterializationBinding,
    dict[str, FeatureBlockSpec],
    dict[str, str],
    dict[str, str],
    dict[str, str],
]:
    """Bind only artifact definitions supported by each frozen feature block."""

    if set(feature_blocks) != set(FEATURE_BLOCKS):
        raise ModeledOofError("predicate materialization needs R0/M0/M1/M2 blocks")
    original_r0 = feature_blocks["R0"]
    original_m0 = feature_blocks["M0"]
    definitions_by_block_side: dict[
        str, dict[str, tuple[PredicateDefinition, ...]]
    ] = {}
    predicate_names_by_block: dict[str, tuple[str, ...]] = {
        "R0": (),
        "M0": (),
    }
    source_fields_by_block: dict[str, tuple[str, ...]] = {"R0": (), "M0": ()}
    updated_blocks = dict(feature_blocks)
    channel_groups = dict(predicate_channel_groups)
    semantic_groups = dict(predicate_semantic_groups)
    clock_groups = dict(predicate_clock_groups)
    trade_channels = {
        _predicate_channel_from_source(definition.source_field)
        for artifact in bundle.artifacts.values()
        if artifact.clock_group == "trade"
        for definition in artifact.definitions
    }

    allowed_origins = {"M1": {"M1"}, "M2": {"M1", "M2"}}
    for target_block in ("M1", "M2"):
        base = feature_blocks[target_block]
        continuous = set(base.continuous_features)
        predicates = set(base.boolean_predicates)
        side_definitions: dict[str, tuple[PredicateDefinition, ...]] = {}
        for side in SIDES:
            selected: list[PredicateDefinition] = []
            for artifact in bundle.artifacts.values():
                if artifact.side != side:
                    continue
                for definition in artifact.definitions:
                    if definition.block not in allowed_origins[target_block]:
                        continue
                    if (
                        definition.kind == "quantile_ge"
                        and definition.source_field in continuous
                    ):
                        selected.append(definition)
                    elif (
                        definition.kind == "preserved_tri"
                        and definition.source_field in predicates
                    ):
                        existing_clock = clock_groups.get(definition.name)
                        if existing_clock in {"book", "trade"} and (
                            existing_clock != definition.clock_group
                        ):
                            raise ModeledOofError(
                                "preserved predicate clock group drifted from 2025 artifact"
                            )
                        channel_groups.setdefault(
                            definition.name,
                            _predicate_channel_from_source(definition.source_field),
                        )
                        semantic_groups.setdefault(
                            definition.name, definition.name.rsplit("::", 1)[-1]
                        )
                        clock_groups[definition.name] = definition.clock_group
            quantile_definitions = tuple(
                sorted(
                    (value for value in selected if value.kind == "quantile_ge"),
                    key=lambda value: value.name,
                )
            )
            side_definitions[side] = quantile_definitions
        buy = {value.name: value for value in side_definitions["BUY"]}
        sell = {value.name: value for value in side_definitions["SELL"]}
        if set(buy) != set(sell) or any(
            _definition_signature(buy[name]) != _definition_signature(sell[name])
            for name in buy
        ):
            raise ModeledOofError(
                f"side-specific 2025 predicates drifted for frozen block {target_block}"
            )
        names = tuple(sorted(buy))
        collision = set(names) & set(base.boolean_predicates)
        if collision:
            raise ModeledOofError(
                "2025 quantile predicates collide with immutable feature columns: "
                f"{sorted(collision)}"
            )
        for name in names:
            definition = buy[name]
            channel_groups[name] = _predicate_channel_from_source(definition.source_field)
            quantile_code = int(round(float(definition.quantile) * 10_000.0))
            semantic_groups[name] = f"quantile_ge_q{quantile_code:04d}"
            clock_groups[name] = definition.clock_group
        updated_blocks[target_block] = FeatureBlockSpec(
            boolean_predicates=(*base.boolean_predicates, *names),
            continuous_features=base.continuous_features,
        )
        definitions_by_block_side[target_block] = side_definitions
        predicate_names_by_block[target_block] = names
        source_fields_by_block[target_block] = tuple(
            sorted({value.source_field for value in side_definitions["BUY"]})
        )
        for predicate in base.boolean_predicates:
            if predicate.startswith("tri::") and clock_groups.get(predicate) not in {
                "book",
                "trade",
            }:
                channel = _predicate_channel_from_source(predicate)
                clock_groups[predicate] = (
                    "trade" if channel in trade_channels else "book"
                )

    if updated_blocks["R0"] != original_r0 or updated_blocks["M0"] != original_m0:
        raise ModeledOofError("2025 predicate binding may not mutate R0 or M0")
    for predicate in {
        value for block in updated_blocks.values() for value in block.boolean_predicates
    }:
        if clock_groups.get(predicate) not in {"book", "trade", "context"}:
            clock_groups[predicate] = "context"

    identity_payload = {
        "bundle_artifact_binding_sha256": bundle.artifact_binding_sha256,
        "threshold_source": "outcome_blind_2025_predicate_artifacts",
        "R0_mutated": False,
        "M0_mutated": False,
        "selected": {
            block: {
                side: [asdict(value) for value in definitions_by_block_side[block][side]]
                for side in SIDES
            }
            for block in ("M1", "M2")
        },
        "economic_outcomes_read": False,
    }
    materialization = PredicateMaterializationBinding(
        bundle=bundle,
        definitions_by_block_side=definitions_by_block_side,
        predicate_names_by_block=predicate_names_by_block,
        source_fields_by_block=source_fields_by_block,
        materialization_identity_sha256=_canonical_sha256(identity_payload),
    )
    return (
        materialization,
        updated_blocks,
        channel_groups,
        semantic_groups,
        clock_groups,
    )


def predicate_materialization_binding_payload(
    binding: PredicateMaterializationBinding,
) -> dict[str, Any]:
    """Compact preflight payload committing every threshold without duplicating it."""

    return {
        "bundle": {
            "path": str(binding.bundle.path),
            "file_sha256": binding.bundle.file_sha256,
            "canonical_sha256": binding.bundle.canonical_sha256,
            "artifact_binding_sha256": binding.bundle.artifact_binding_sha256,
        },
        "artifacts": {
            name: {
                "path": str(artifact.path),
                "file_sha256": artifact.file_sha256,
                "canonical_sha256": artifact.canonical_sha256,
                "reference_identity_sha256": artifact.reference_identity_sha256,
            }
            for name, artifact in sorted(binding.bundle.artifacts.items())
        },
        "materialization_identity_sha256": binding.materialization_identity_sha256,
        "materialized_predicate_counts": {
            block: len(names)
            for block, names in binding.predicate_names_by_block.items()
        },
        "materialized_source_counts": {
            block: len(names)
            for block, names in binding.source_fields_by_block.items()
        },
        "threshold_source": "outcome_blind_2025_predicate_artifacts",
        "economic_outcomes_read": False,
        "R0_mutated": False,
    }


def materialize_2025_predicates(
    features: pd.DataFrame,
    metadata: pd.DataFrame,
    *,
    binding: PredicateMaterializationBinding,
) -> pd.DataFrame:
    """Apply side-specific frozen thresholds with explicit UNOBSERVED output."""

    if not features.index.equals(metadata.index):
        raise ModeledOofError("predicate materialization feature/metadata order drifted")
    sides = metadata["side"].astype(str).str.upper()
    if set(sides) - set(SIDES):
        raise ModeledOofError("predicate materialization received an invalid side")
    by_side_name: dict[str, dict[str, PredicateDefinition]] = {
        side: {} for side in SIDES
    }
    for block in ("M1", "M2"):
        for side in SIDES:
            for definition in binding.definitions_by_block_side.get(block, {}).get(side, ()):
                existing = by_side_name[side].get(definition.name)
                if existing is not None and existing != definition:
                    raise ModeledOofError("repeated materialized predicate definition drifted")
                by_side_name[side][definition.name] = definition
    names = set(by_side_name["BUY"])
    if names != set(by_side_name["SELL"]):
        raise ModeledOofError("materialized predicate universe is not side-complete")
    collision = names & set(features)
    if collision:
        raise ModeledOofError(
            f"materialized predicates already exist in feature artifact: {sorted(collision)}"
        )
    ordered_names = tuple(sorted(names))
    if not ordered_names:
        return features.copy()
    names_by_source: dict[str, list[str]] = {}
    for name in ordered_names:
        buy_source = by_side_name["BUY"][name].source_field
        sell_source = by_side_name["SELL"][name].source_field
        if buy_source != sell_source:
            raise ModeledOofError("side-specific predicate sources drifted")
        if buy_source not in features:
            raise ModeledOofError(f"materialized predicate source is missing: {buy_source}")
        names_by_source.setdefault(buy_source, []).append(name)

    side_values = sides.to_numpy(copy=False)
    side_masks = {side: side_values == side for side in SIDES}
    column_index = {name: index for index, name in enumerate(ordered_names)}
    matrix = np.full((len(features), len(ordered_names)), -1, dtype=np.int8)
    for source_field, source_names in names_by_source.items():
        numeric = pd.to_numeric(features[source_field], errors="coerce").to_numpy(
            dtype=float,
            na_value=np.nan,
        )
        finite = np.isfinite(numeric)
        target_columns = np.asarray(
            [column_index[name] for name in source_names], dtype=np.intp
        )
        for side in SIDES:
            observed_rows = np.flatnonzero(side_masks[side] & finite)
            if observed_rows.size == 0:
                continue
            thresholds = np.asarray(
                [float(by_side_name[side][name].threshold) for name in source_names],
                dtype=float,
            )
            matrix[np.ix_(observed_rows, target_columns)] = (
                numeric[observed_rows, np.newaxis] >= thresholds[np.newaxis, :]
            ).astype(np.int8)
    materialized_frame = pd.DataFrame(
        matrix,
        index=features.index,
        columns=ordered_names,
        dtype=np.int8,
        copy=False,
    )
    return pd.concat((features, materialized_frame), axis=1)


def _group_columns_by_required_days(
    columns: Sequence[str],
    required_days: Mapping[str, set[str]],
) -> tuple[tuple[tuple[str, ...], tuple[str, ...]], ...]:
    grouped: dict[tuple[str, ...], list[str]] = {}
    for column in columns:
        days = tuple(sorted(required_days[column]))
        grouped.setdefault(days, []).append(column)
    return tuple(
        (days, tuple(names))
        for days, names in sorted(grouped.items(), key=lambda item: item[0])
    )


def _validate_three_valued_columns_batched(
    frame: pd.DataFrame,
    utc_days: pd.Series,
    columns: Sequence[str],
    required_days: Mapping[str, set[str]],
    *,
    chunk_size: int = 256,
) -> None:
    """Validate large predicate blocks without inserting or recasting columns."""

    for days, grouped_columns in _group_columns_by_required_days(columns, required_days):
        rows = frame.index[utc_days.isin(days)]
        for start in range(0, len(grouped_columns), chunk_size):
            chunk = grouped_columns[start : start + chunk_size]
            numeric = frame.loc[rows, list(chunk)].apply(
                pd.to_numeric, errors="coerce"
            )
            valid = numeric.notna() & numeric.isin((-1, 0, 1))
            valid_by_column = valid.all(axis=0)
            if not bool(valid_by_column.all()):
                invalid = str(valid_by_column.index[~valid_by_column][0])
                raise ModeledOofError(
                    f"Boolean predicate {invalid!r} is not three-valued"
                )


def _validate_finite_columns_batched(
    frame: pd.DataFrame,
    utc_days: pd.Series,
    columns: Sequence[str],
    required_days: Mapping[str, set[str]],
    *,
    chunk_size: int = 128,
) -> None:
    """Validate continuous inputs in bounded temporary matrices without rewrites."""

    for days, grouped_columns in _group_columns_by_required_days(columns, required_days):
        rows = frame.index[utc_days.isin(days)]
        for start in range(0, len(grouped_columns), chunk_size):
            chunk = grouped_columns[start : start + chunk_size]
            numeric = frame.loc[rows, list(chunk)].apply(
                pd.to_numeric, errors="coerce"
            )
            values = numeric.to_numpy(dtype=float, na_value=np.nan)
            finite_by_column = np.isfinite(values).all(axis=0)
            if not bool(finite_by_column.all()):
                invalid = str(chunk[int(np.flatnonzero(~finite_by_column)[0])])
                raise ModeledOofError(
                    f"continuous feature {invalid!r} must be finite on its frozen panel"
                )


def _feature_artifact_columns(config: FrozenConfig) -> set[str]:
    columns = {config.columns.opportunity}
    materialized = (
        set()
        if config.predicate_materialization is None
        else {
            name
            for names in config.predicate_materialization.predicate_names_by_block.values()
            for name in names
        }
    )
    for block in config.feature_blocks.values():
        columns.update(set(block.boolean_predicates) - materialized)
        columns.update(block.continuous_features)
    return columns


def load_frozen_config(
    path: Path,
    *,
    expected_sha256: str,
    spec_path: Path,
    expected_spec_sha256: str,
    feature_manifest_path: Path,
    feature_manifest_sha256: str,
    feature_table_globs: Sequence[str] = ("*.parquet", "**/*.parquet"),
) -> FrozenConfig:
    """Load the exact frozen owner spec/config plus a bound feature artifact."""

    config_path = Path(path).expanduser().resolve()
    actual_sha256 = _sha256(config_path)
    if actual_sha256 != expected_sha256:
        raise ModeledOofError("frozen config SHA256 mismatch")
    payload = _load_json(config_path)
    if payload.get("schema_version") != CONFIG_SCHEMA or payload.get("identity") != IDENTITY:
        raise ModeledOofError("frozen config identity/schema drifted")
    if payload.get("config_status") != "frozen_before_owner_oof_economic_read":
        raise ModeledOofError("study config was not frozen before the owner OOF read")
    permissions = payload.get("permissions")
    if not isinstance(permissions, Mapping):
        raise ModeledOofError("frozen config permissions are missing")
    if (
        permissions.get("validation_read") is not False
        or permissions.get("sealed_holdout_read") is not False
    ):
        raise ModeledOofError("Validation and sealed holdout must remain unread")
    if (
        permissions.get("action_authorized") is not False
        or permissions.get("live_authorized") is not False
    ):
        raise ModeledOofError("owner OOF config may not carry action/live authority")
    resolved_spec = _require_exact_source_document(
        spec_path,
        expected_spec_sha256,
        role="frozen owner spec",
    )
    actual_spec_sha256 = _sha256(resolved_spec)
    spec = _load_json(resolved_spec)
    if spec.get("schema_version") != SPEC_SCHEMA or spec.get("identity") != IDENTITY:
        raise ModeledOofError("frozen owner spec identity/schema drifted")
    strict_boundary = spec.get("strict_native_boundary")
    if (
        not isinstance(strict_boundary, Mapping)
        or strict_boundary.get("exact_queue_policy_eligible") is not False
    ):
        raise ModeledOofError("owner spec must preserve the strict-queue failure boundary")
    label_source = spec.get("modeled_label_source")
    if not isinstance(label_source, Mapping):
        raise ModeledOofError("owner spec modeled-label source is missing")
    if (
        int(label_source.get("opportunity_rows", -1)) != 8600
        or int(label_source.get("arm_rows", -1)) != 68800
        or int(label_source.get("arm_count_per_opportunity", -1)) != 8
        or int(label_source.get("joint_censored_opportunities", -1)) != 171
        or int(label_source.get("point_label_eligible_opportunities", -1)) != 8429
    ):
        raise ModeledOofError("frozen modeled-label census counts drifted")
    labels = ArtifactSpec(
        manifest_path=_resolve_bound_path(label_source["admission_manifest_path"]),
        manifest_sha256=str(label_source["admission_manifest_file_sha256"]),
        table_globs=("execution/runs/formal/*/arm_traces.parquet",),
        expected_identity=str(label_source["identity"]),
    )
    resolved_feature_manifest = Path(feature_manifest_path).expanduser().resolve()
    if len(feature_manifest_sha256) != 64 or not feature_table_globs:
        raise ModeledOofError("multichannel feature artifact binding is incomplete")
    if _sha256(resolved_feature_manifest) != feature_manifest_sha256:
        raise ModeledOofError("multichannel feature manifest SHA256 mismatch")
    _require_feature_panel_admission(resolved_feature_manifest, feature_manifest_sha256)
    feature_manifest = _load_json(resolved_feature_manifest)
    if _manifest_identity(feature_manifest) != EXPECTED_FEATURE_MANIFEST_IDENTITY:
        raise ModeledOofError("multichannel feature manifest identity mismatch")
    feature_schema = _feature_manifest_schema(feature_manifest)
    join_key = feature_manifest.get("label_join_key", feature_manifest.get("join_key", ""))
    if (
        feature_manifest.get("economic_outcomes_read") is not False
        or feature_manifest.get("arm_economic_labels_read") is not False
        or feature_manifest.get("validation_read") is not False
        or feature_manifest.get("sealed_holdout_read") is not False
        or join_key != "opportunity_id"
    ):
        raise ModeledOofError("multichannel feature manifest lacks outcome-blind provenance")
    features = ArtifactSpec(
        manifest_path=resolved_feature_manifest,
        manifest_sha256=feature_manifest_sha256,
        table_globs=tuple(str(value) for value in feature_table_globs),
        expected_identity=EXPECTED_FEATURE_MANIFEST_IDENTITY,
    )
    prefix40 = tuple(_normalize_day(value) for value in spec.get("development_days", ()))
    if len(prefix40) != 40 or len(set(prefix40)) != 40:
        raise ModeledOofError("owner spec must bind exactly 40 unique Development days")
    analysis_panels = spec.get("analysis_panels")
    if not isinstance(analysis_panels, Mapping):
        raise ModeledOofError("owner spec analysis panels are missing")
    prefix33_contract = analysis_panels.get("prefix33_raw_m2_common_support")
    if not isinstance(prefix33_contract, Mapping):
        raise ModeledOofError("raw-M2 common-support panel is missing")
    exclusions = {_normalize_day(value) for value in prefix33_contract.get("excluded_days", ())}
    prefix33 = tuple(day for day in prefix40 if day not in exclusions)
    if len(prefix33) != 33 or len(exclusions) != 7:
        raise ModeledOofError("raw-M2 common-support denominator drifted from 33 days")
    support_split = feature_manifest.get("frozen_support_split")
    if not isinstance(support_split, Mapping):
        raise ModeledOofError("multichannel feature support split is missing")
    if (
        tuple(str(day) for day in support_split.get("prefix40_days", ())) != prefix40
        or tuple(str(day) for day in support_split.get("m2_common_support_days", ())) != prefix33
    ):
        raise ModeledOofError("multichannel feature support split drifted")
    scopes = tuple(str(value) for value in payload.get("analysis_panels", ()))
    if scopes != PANEL_SCOPES:
        raise ModeledOofError("study config must run the frozen prefix40 and prefix33 panels")
    panel_days = {
        "prefix40_modeled_label_development": prefix40,
        "prefix33_raw_m2_common_support": prefix33,
    }
    panel_feature_blocks = {
        scope: tuple(str(value) for value in analysis_panels[scope]["eligible_feature_blocks"])
        for scope in scopes
    }
    if panel_feature_blocks != {
        "prefix40_modeled_label_development": ("R0", "M0", "M1"),
        "prefix33_raw_m2_common_support": ("R0", "M0", "M1", "M2"),
    }:
        raise ModeledOofError("feature-block panel assignment drifted")
    raw_blocks = feature_schema.get("feature_blocks")
    if not isinstance(raw_blocks, Mapping) or set(raw_blocks) != set(FEATURE_BLOCKS):
        raise ModeledOofError("feature blocks must define exactly R0/M0/M1/M2")
    blocks = {
        block: FeatureBlockSpec(
            boolean_predicates=tuple(
                str(value) for value in raw_blocks[block]["boolean_predicates"]
            ),
            continuous_features=tuple(
                str(value) for value in raw_blocks[block]["continuous_features"]
            ),
        )
        for block in FEATURE_BLOCKS
    }
    for attribute in ("boolean_predicates", "continuous_features"):
        if not (
            set(getattr(blocks["M0"], attribute))
            <= set(getattr(blocks["M1"], attribute))
            <= set(getattr(blocks["M2"], attribute))
        ):
            raise ModeledOofError(f"M0/M1/M2 {attribute} must be cumulative")
    try:
        raw_search = dict(payload["search"])
        if raw_search.get("exploratory_candidate_requires_positive_lcb") is not False:
            raise ModeledOofError("exploratory candidate may not require a pre-OOF LCB")
        if raw_search.get("baseline_is_candidate") is not False:
            raise ModeledOofError("baseline may not compete in exploratory candidate selection")
        search_fields = set(SearchConfig.__dataclass_fields__)
        search = SearchConfig(
            **{key: value for key, value in raw_search.items() if key in search_fields}
        )
        raw_continuous = payload["continuous_comparator"]
        if (
            raw_continuous.get("run_for_every_feature_block") is not True
            or raw_continuous.get("may_replace_boolean_policy") is not False
            or raw_continuous.get("may_grant_action_or_live") is not False
        ):
            raise ModeledOofError("continuous comparator permissions drifted")
        continuous = ContinuousConfig(
            max_depth_candidates=tuple(
                int(value) for value in raw_continuous["max_depth_candidates"]
            ),
            min_samples_leaf=int(raw_continuous["min_samples_leaf"]),
            minimum_train_rows_per_action=int(raw_continuous["min_samples_leaf"]),
            random_state=int(raw_continuous["random_state"]),
        )
        raw_gate = payload["post_oof_gate"]
        if (
            raw_gate.get("only_prefix_panels_may_grant_owner_support") is not True
            or raw_gate.get("added10_or_pooled50_may_grant_support") is not False
        ):
            raise ModeledOofError("post-OOF panel authority drifted")
        deployment = DeploymentConfig(
            economic_epsilon_usdc=float(raw_gate["economic_epsilon_usdc"]),
            minimum_action_rate=float(raw_gate["minimum_action_rate"]),
            minimum_action_campaigns=int(raw_gate["minimum_action_campaigns"]),
            minimum_action_days=int(raw_gate["minimum_action_days"]),
            require_full_identification=bool(raw_gate.get("require_full_identification", False)),
            require_outer_fold_nonbaseline_action=bool(
                raw_gate["require_outer_fold_nonbaseline_action"]
            ),
            require_opener_and_add_reporting=bool(raw_gate["require_opener_and_add_reporting"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ModeledOofError("search/deployment config is invalid") from exc
    outer_fold_count = int(payload.get("outer_folds", 0))
    outer_minimum_train_days = int(payload.get("outer_minimum_train_days", 0))
    outer_folds = {
        scope: expanding_chronological_folds(
            days,
            fold_prefix=f"{scope}.outer",
            n_folds=outer_fold_count,
            minimum_train_days=outer_minimum_train_days,
        )
        for scope, days in panel_days.items()
    }
    group_payload = feature_schema.get("predicate_groups", {})
    if not isinstance(group_payload, Mapping):
        raise ModeledOofError("predicate group maps are invalid")
    predicate_input = spec.get("outcome_blind_2025_input")
    if not isinstance(predicate_input, Mapping):
        raise ModeledOofError("owner spec outcome-blind 2025 predicate input is missing")
    predicate_bundle = load_2025_predicate_bundle(predicate_input)
    (
        predicate_materialization,
        blocks,
        predicate_channel_groups,
        predicate_semantic_groups,
        predicate_clock_groups,
    ) = bind_2025_predicate_materialization(
        predicate_bundle,
        feature_blocks=blocks,
        predicate_channel_groups=dict(group_payload.get("channel", {})),
        predicate_semantic_groups=dict(group_payload.get("semantic", {})),
        predicate_clock_groups=dict(group_payload.get("clock", {})),
    )
    for attribute in ("boolean_predicates", "continuous_features"):
        if not (
            set(getattr(blocks["M0"], attribute))
            <= set(getattr(blocks["M1"], attribute))
            <= set(getattr(blocks["M2"], attribute))
        ):
            raise ModeledOofError(
                f"materialized M0/M1/M2 {attribute} must remain cumulative"
            )
    not_run_panels = {
        name: dict(analysis_panels[name])
        for name in NOT_RUN_PANELS
        if isinstance(analysis_panels.get(name), Mapping)
    }
    if set(not_run_panels) != set(NOT_RUN_PANELS) or any(
        value.get("status") != "not_run_not_imputed" for value in not_run_panels.values()
    ):
        raise ModeledOofError("added10/pooled50 must remain not-run and not-imputed")
    return FrozenConfig(
        path=config_path,
        sha256=actual_sha256,
        payload=payload,
        spec_path=resolved_spec,
        spec_sha256=actual_spec_sha256,
        spec_payload=spec,
        labels=labels,
        features=features,
        columns=_column_contract(payload.get("columns", {})),
        prefix40_days=prefix40,
        added10_days=(),
        report_scopes=scopes,
        panel_days=panel_days,
        panel_feature_blocks=panel_feature_blocks,
        not_run_panels=not_run_panels,
        outer_folds=outer_folds,
        feature_blocks=blocks,
        search=search,
        continuous=continuous,
        deployment=deployment,
        minimum_inner_identified_weight_fraction=0.0,
        uplift_bounds_usdc=None,
        predicate_channel_groups=predicate_channel_groups,
        predicate_semantic_groups=predicate_semantic_groups,
        predicate_clock_groups=predicate_clock_groups,
        code_bindings=(),
        expected_library_versions=runtime_library_versions(),
        predicate_materialization=predicate_materialization,
    )


def _manifest_identity(payload: Mapping[str, Any]) -> str | None:
    for key in ("identity", "artifact_identity", "study_identity"):
        value = payload.get(key)
        if value is not None:
            return str(value)
    return None


def _resolve_bound_path(value: Any) -> Path:
    path = resolve_portable_path(str(value))
    if not path.is_absolute():
        path = Path(__file__).resolve().parents[4] / path
    return path.resolve()


def _resolve_exact_source_bound_path(value: Any, *, role: str) -> Path:
    """Resolve a historical public locator to the exact private source bytes."""

    resolved = _resolve_bound_path(value)
    try:
        return source_document_path(resolved, require_private=True)
    except (OSError, PublicMachineProjectionError) as exc:
        raise ModeledOofError(f"{role} exact source is unavailable or invalid") from exc


def _required_oof_code_paths() -> tuple[Path, ...]:
    paths = [Path(__file__).resolve()]
    dependency = inspect.getsourcefile(BooleanCooldownPolicy)
    if dependency is None:
        raise ModeledOofError("cannot resolve Boolean OOF implementation dependency")
    paths.append(Path(dependency).resolve())
    return tuple(paths)


def load_execution_amendment(
    path: Path,
    *,
    expected_sha256: str,
    config: FrozenConfig,
) -> tuple[FrozenConfig, ExecutionAmendmentBinding]:
    """Verify the complete OOF execution identity before any economic table read."""

    amendment_path = _require_exact_source_document(
        path,
        expected_sha256,
        role="OOF execution amendment",
    )
    payload = _load_json(amendment_path)
    if (
        payload.get("schema_version") != EXECUTION_AMENDMENT_SCHEMA
        or payload.get("identity") != IDENTITY
        or payload.get("status") != "frozen_before_owner_oof_economic_read"
    ):
        raise ModeledOofError("OOF execution amendment identity/schema/status drifted")
    permissions = payload.get("permissions")
    if not isinstance(permissions, Mapping) or any(
        permissions.get(name) is not False
        for name in (
            "validation_read",
            "sealed_holdout_read",
            "action_authorized",
            "live_authorized",
        )
    ):
        raise ModeledOofError("OOF execution amendment permissions drifted")

    raw_artifacts = payload.get("artifact_bindings")
    if not isinstance(raw_artifacts, Mapping):
        raise ModeledOofError("OOF execution amendment artifact bindings are missing")
    expected_artifacts: dict[str, tuple[Path, str, str | None, str | None]] = {
        "frozen_config": (config.path, config.sha256, IDENTITY, None),
        "frozen_owner_spec": (config.spec_path, config.spec_sha256, IDENTITY, None),
        "modeled_label_manifest": (
            config.labels.manifest_path,
            config.labels.manifest_sha256,
            config.labels.expected_identity,
            None,
        ),
        "feature_panel_manifest": (
            config.features.manifest_path,
            config.features.manifest_sha256,
            EXPECTED_FEATURE_MANIFEST_IDENTITY,
            None,
        ),
    }
    if config.predicate_materialization is not None:
        bundle = config.predicate_materialization.bundle
        expected_artifacts["outcome_blind_2025_predicate_bundle"] = (
            bundle.path,
            bundle.file_sha256,
            PREDICATE_ARTIFACT_IDENTITY,
            bundle.canonical_sha256,
        )
        for artifact_name, artifact in sorted(bundle.artifacts.items()):
            expected_artifacts[f"outcome_blind_2025_predicate_{artifact_name}"] = (
                artifact.path,
                artifact.file_sha256,
                PREDICATE_ARTIFACT_IDENTITY,
                artifact.canonical_sha256,
            )
    normalized_artifacts: dict[str, Mapping[str, Any]] = {}
    for name, (
        expected_path,
        expected_hash,
        expected_identity,
        expected_canonical,
    ) in expected_artifacts.items():
        row = raw_artifacts.get(name)
        if not isinstance(row, Mapping):
            raise ModeledOofError(f"OOF execution amendment lacks {name} binding")
        resolved = (
            _resolve_exact_source_bound_path(
                row.get("path"),
                role="OOF execution amendment frozen_owner_spec",
            )
            if name == "frozen_owner_spec"
            else _resolve_bound_path(row.get("path"))
        )
        bound_hash = str(row.get("sha256", ""))
        if resolved != expected_path.resolve() or bound_hash != expected_hash:
            raise ModeledOofError(f"OOF execution amendment {name} binding drifted")
        if not resolved.is_file() or _sha256(resolved) != bound_hash:
            raise ModeledOofError(f"OOF execution amendment {name} bytes drifted")
        if expected_identity is not None:
            if str(row.get("identity", "")) != expected_identity:
                raise ModeledOofError(f"OOF execution amendment {name} identity drifted")
            artifact_payload = _load_json(resolved)
            if _manifest_identity(artifact_payload) != expected_identity:
                raise ModeledOofError(f"OOF execution amendment {name} file identity drifted")
            if expected_canonical is not None and (
                str(row.get("canonical_sha256", "")) != expected_canonical
                or _verified_canonical_identity(
                    artifact_payload,
                    label=f"OOF execution amendment {name}",
                )
                != expected_canonical
            ):
                raise ModeledOofError(
                    f"OOF execution amendment {name} canonical identity drifted"
                )
        normalized_artifacts[name] = {
            "path": str(resolved),
            "sha256": bound_hash,
            "identity": expected_identity,
            **(
                {"canonical_sha256": expected_canonical}
                if expected_canonical is not None
                else {}
            ),
        }
    _require_feature_panel_admission(
        config.features.manifest_path,
        config.features.manifest_sha256,
    )

    raw_code = payload.get("code_bindings")
    if not isinstance(raw_code, list) or not raw_code:
        raise ModeledOofError("OOF execution amendment code bindings are missing")
    code_bindings: list[tuple[Path, str]] = []
    seen_paths: set[Path] = set()
    for row in raw_code:
        if not isinstance(row, Mapping):
            raise ModeledOofError("OOF execution amendment code binding is invalid")
        resolved = _resolve_bound_path(row.get("path"))
        expected = str(row.get("sha256", ""))
        if resolved in seen_paths:
            raise ModeledOofError("OOF execution amendment repeats a code binding")
        if not resolved.is_file() or len(expected) != 64 or _sha256(resolved) != expected:
            raise ModeledOofError(f"OOF execution amendment code binding drifted: {resolved}")
        seen_paths.add(resolved)
        code_bindings.append((resolved, expected))
    missing_required_code = set(_required_oof_code_paths()) - seen_paths
    if missing_required_code:
        raise ModeledOofError(
            "OOF execution amendment lacks required code bindings: "
            f"{sorted(str(value) for value in missing_required_code)}"
        )

    libraries = payload.get("library_versions")
    observed_libraries = runtime_library_versions()
    if not isinstance(libraries, Mapping) or dict(libraries) != observed_libraries:
        raise ModeledOofError("OOF execution amendment library versions drifted")
    identity = str(payload.get("execution_identity_sha256", ""))
    identity_payload = dict(payload)
    identity_payload.pop("execution_identity_sha256", None)
    if identity != _canonical_sha256(identity_payload):
        raise ModeledOofError("OOF execution amendment canonical identity drifted")

    binding = ExecutionAmendmentBinding(
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


def verify_input_artifact(spec: ArtifactSpec) -> InputBinding:
    """Verify the manifest and every selected Parquet table byte-for-byte."""

    if _sha256(spec.manifest_path) != spec.manifest_sha256:
        raise ModeledOofError(f"input manifest SHA256 mismatch: {spec.manifest_path}")
    manifest = _load_json(spec.manifest_path)
    identity = _manifest_identity(manifest)
    if spec.expected_identity is not None and identity != spec.expected_identity:
        raise ModeledOofError(f"input manifest identity mismatch: {spec.manifest_path}")
    files = manifest.get("files")
    if not isinstance(files, list):
        raise ModeledOofError("input manifest must expose a files list")
    selected: list[tuple[Path, str, str]] = []
    for row in files:
        if not isinstance(row, Mapping):
            raise ModeledOofError("input manifest file row is invalid")
        relative = str(row.get("relative_path", ""))
        if any(fnmatch.fnmatch(relative, pattern) for pattern in spec.table_globs):
            selected.append(
                (spec.manifest_path.parent / relative, str(row.get("sha256", "")), relative)
            )
    if not selected:
        raise ModeledOofError("input table globs selected no files")
    if len({relative for _, _, relative in selected}) != len(selected):
        raise ModeledOofError("input manifest repeats a selected table")
    table_hashes: dict[str, str] = {}
    for path, expected, _relative in sorted(selected, key=lambda row: row[2]):
        if path.suffix != ".parquet" or not path.is_file() or len(expected) != 64:
            raise ModeledOofError(f"selected input table is invalid: {path}")
        actual = _sha256(path)
        if actual != expected:
            raise ModeledOofError(f"input table SHA256 mismatch: {path}")
        table_hashes[str(path.resolve())] = actual
    return InputBinding(
        manifest_path=spec.manifest_path,
        manifest_sha256=spec.manifest_sha256,
        table_paths=tuple(Path(path) for path in table_hashes),
        table_sha256=table_hashes,
        manifest_identity=identity,
    )


def _read_parquet_tables(paths: Sequence[Path], columns: Sequence[str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in paths:
        try:
            frames.append(pd.read_parquet(path, columns=list(columns)))
        except (OSError, ValueError, KeyError) as exc:
            raise ModeledOofError(f"cannot read bound Parquet table: {path}") from exc
    return pd.concat(frames, ignore_index=True)


def _bool_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame:
        raise ModeledOofError(f"required Boolean label column is missing: {column}")
    values = frame[column]
    if (
        values.isna().any()
        or not values.map(lambda value: isinstance(value, (bool, np.bool_))).all()
    ):
        raise ModeledOofError(f"label column {column!r} must contain explicit bools")
    return values.astype(bool)


def _timestamp_ns(values: pd.Series, *, unit: str) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    multiplier = {"ns": 1, "us": 1_000, "ms": 1_000_000, "s": 1_000_000_000}[unit]
    converted = numeric * multiplier
    if converted.dropna().lt(0).any() or converted.dropna().gt(np.iinfo(np.int64).max).any():
        raise ModeledOofError("timestamp is outside int64 nanosecond support")
    return converted.astype("Float64")


def _assert_opportunity_constant(
    frame: pd.DataFrame, opportunity: str, columns: Sequence[str]
) -> None:
    grouped = frame.groupby(opportunity, sort=False, observed=True)
    for column in columns:
        if grouped[column].nunique(dropna=False).gt(1).any():
            raise ModeledOofError(f"opportunity rows disagree on {column!r}")


def prepare_modeled_panel(
    labels: pd.DataFrame,
    features: pd.DataFrame,
    *,
    config: FrozenConfig,
) -> PreparedPanel:
    """Join modelled labels to features while preserving null arm outcomes."""

    columns = config.columns
    required = {
        columns.opportunity,
        columns.day,
        columns.side,
        columns.role,
        columns.campaign,
        columns.action,
        columns.outcome,
        columns.assignment_time.name,
        columns.eligible,
        columns.right_censored,
        columns.joint_censored,
        columns.exact_queue_eligible,
        *(field.name for field in columns.observation_end),
    }
    missing = required - set(labels)
    if missing:
        raise ModeledOofError(f"label tables are missing columns: {sorted(missing)}")
    frame = labels.copy()
    if frame.duplicated([columns.opportunity, columns.action]).any():
        raise ModeledOofError("label opportunity/action rows are not unique")
    label_contract = config.spec_payload.get("modeled_label_source", {})
    expected_arm_rows = int(label_contract.get("arm_rows", len(frame)))
    expected_opportunities = int(
        label_contract.get("opportunity_rows", frame[columns.opportunity].nunique())
    )
    if len(frame) != expected_arm_rows:
        raise ModeledOofError("modeled-label arm census row count drifted")
    frame[columns.opportunity] = frame[columns.opportunity].astype(str)
    frame[columns.action] = frame[columns.action].astype(str)
    frame[columns.side] = frame[columns.side].astype(str).str.upper()
    if set(frame[columns.side]) - set(SIDES):
        raise ModeledOofError("labels contain an invalid side")
    frame[columns.day] = frame[columns.day].map(_normalize_day)
    all_days = set(config.prefix40_days)
    if set(frame[columns.day]) - all_days:
        raise ModeledOofError("labels contain a day outside the frozen panel")
    if set(frame[columns.day]) != all_days:
        raise ModeledOofError("modeled labels do not cover the exact prefix40 denominator")
    frame["panel_role"] = "prefix40_modeled_label_development"
    frame[columns.role] = frame[columns.role].astype(str).str.strip().str.lower()
    if set(frame[columns.role]) - {"opener", "add"}:
        raise ModeledOofError("labels must preserve opener/add role")
    if _bool_series(frame, columns.exact_queue_eligible).any():
        raise ModeledOofError("owner modelled-queue input may not claim exact queue eligibility")
    eligible = _bool_series(frame, columns.eligible)
    right_censored = _bool_series(frame, columns.right_censored)
    joint_censored = _bool_series(frame, columns.joint_censored)
    outcomes = pd.to_numeric(frame[columns.outcome], errors="coerce")
    supported = eligible & ~right_censored & ~joint_censored & outcomes.notna()
    if (eligible & ~right_censored & ~joint_censored & outcomes.isna()).any():
        raise ModeledOofError("eligible uncensored arm has a missing economic outcome")
    redacted = int((~supported & outcomes.notna()).sum())
    frame["_supported"] = supported
    frame["_outcome"] = outcomes.where(supported, np.nan)
    frame["_unsupported_reason"] = np.select(
        [right_censored, joint_censored, ~eligible, outcomes.isna()],
        ["right_censored", "joint_censored", "label_ineligible", "outcome_missing"],
        default="supported",
    )
    frame["_assignment_ts_ns"] = _timestamp_ns(
        frame[columns.assignment_time.name], unit=columns.assignment_time.unit
    )
    if frame["_assignment_ts_ns"].isna().any():
        raise ModeledOofError("assignment timestamps must be observed")
    endpoint_columns: list[str] = []
    for index, field in enumerate(columns.observation_end):
        name = f"_observation_end_{index}_ns"
        frame[name] = _timestamp_ns(frame[field.name], unit=field.unit)
        if field.name == "washout_ts_ns" and frame[name].isna().any():
            raise ModeledOofError("every modeled arm must expose washout_ts_ns")
        endpoint_columns.append(name)
    frame["_arm_observation_end_ts_ns"] = frame[endpoint_columns].max(axis=1, skipna=True)
    if (
        frame["_arm_observation_end_ts_ns"].notna()
        & (frame["_arm_observation_end_ts_ns"] < frame["_assignment_ts_ns"])
    ).any():
        raise ModeledOofError("an arm observation end precedes its assignment")
    constants = (
        columns.day,
        "panel_role",
        columns.side,
        columns.role,
        columns.campaign,
        "_assignment_ts_ns",
    )
    _assert_opportunity_constant(frame, columns.opportunity, constants)
    for side in SIDES:
        subset = frame.loc[frame[columns.side] == side]
        expected = set(duration_vocabulary(side))
        action_sets = subset.groupby(columns.opportunity, observed=True)[columns.action].agg(set)
        if not action_sets.map(lambda values, expected=expected: values == expected).all():
            raise ModeledOofError(f"{side} opportunity lacks the exact frozen eight-arm vocabulary")
    metadata = (
        frame[[columns.opportunity, *constants]]
        .drop_duplicates(columns.opportunity)
        .set_index(columns.opportunity)
        .rename(
            columns={
                columns.day: "utc_day",
                columns.side: "side",
                columns.role: "role_at_fill",
                columns.campaign: "campaign_cluster_id",
                "_assignment_ts_ns": "assignment_ts_ns",
            }
        )
    )
    metadata["source_campaign_id"] = metadata["campaign_cluster_id"].astype(str)
    metadata["campaign_cluster_id"] = (
        metadata["utc_day"].astype(str)
        + "::"
        + metadata["side"].astype(str)
        + "::"
        + metadata["source_campaign_id"]
    )
    metadata = metadata.sort_values(
        ["utc_day", "assignment_ts_ns", "campaign_cluster_id"], kind="stable"
    )
    if len(metadata) != expected_opportunities:
        raise ModeledOofError("modeled-label opportunity census count drifted")
    observation_end = (
        frame.groupby(columns.opportunity, observed=True)["_arm_observation_end_ts_ns"]
        .max()
        .reindex(metadata.index)
    )
    outcome_matrix = frame.pivot(
        index=columns.opportunity, columns=columns.action, values="_outcome"
    ).reindex(metadata.index)
    support_matrix = frame.pivot(
        index=columns.opportunity, columns=columns.action, values="_supported"
    ).reindex(metadata.index)
    reason_matrix = frame.pivot(
        index=columns.opportunity, columns=columns.action, values="_unsupported_reason"
    ).reindex(metadata.index)
    expected_censored = int(
        label_contract.get(
            "joint_censored_opportunities",
            joint_censored.groupby(frame[columns.opportunity], observed=True).any().sum(),
        )
    )
    observed_censored = int(
        joint_censored.groupby(frame[columns.opportunity], observed=True).any().sum()
    )
    expected_eligible = int(
        label_contract.get(
            "point_label_eligible_opportunities", expected_opportunities - expected_censored
        )
    )
    observed_eligible = int(support_matrix.all(axis=1).sum())
    if observed_censored != expected_censored or observed_eligible != expected_eligible:
        raise ModeledOofError("joint-censor/point-eligible opportunity counts drifted")
    all_feature_columns = _feature_artifact_columns(config)
    missing_features = all_feature_columns - set(features)
    if missing_features:
        raise ModeledOofError(f"feature panel is missing columns: {sorted(missing_features)}")
    feature_frame = features.loc[:, sorted(all_feature_columns)].copy()
    feature_frame[columns.opportunity] = feature_frame[columns.opportunity].astype(str)
    if feature_frame[columns.opportunity].duplicated().any():
        raise ModeledOofError("feature panel repeats an opportunity")
    feature_frame = feature_frame.set_index(columns.opportunity)
    if set(feature_frame.index) != set(metadata.index):
        raise ModeledOofError("feature and label opportunity denominators differ")
    feature_frame = feature_frame.reindex(metadata.index)
    if config.predicate_materialization is not None:
        feature_frame = materialize_2025_predicates(
            feature_frame,
            metadata,
            binding=config.predicate_materialization,
        )
    predicate_columns = sorted(
        {value for block in config.feature_blocks.values() for value in block.boolean_predicates}
    )
    required_predicate_days: dict[str, set[str]] = {name: set() for name in predicate_columns}
    required_continuous_days: dict[str, set[str]] = {
        name: set()
        for block in config.feature_blocks.values()
        for name in block.continuous_features
    }
    for scope, block_names in config.panel_feature_blocks.items():
        required_days = set(config.panel_days[scope])
        for block_name in block_names:
            block = config.feature_blocks[block_name]
            for column in block.boolean_predicates:
                required_predicate_days[column].update(required_days)
            for column in block.continuous_features:
                required_continuous_days[column].update(required_days)
    _validate_three_valued_columns_batched(
        feature_frame,
        metadata["utc_day"],
        predicate_columns,
        required_predicate_days,
    )
    continuous_columns = sorted(
        {value for block in config.feature_blocks.values() for value in block.continuous_features}
    )
    _validate_finite_columns_batched(
        feature_frame,
        metadata["utc_day"],
        continuous_columns,
        required_continuous_days,
    )
    return PreparedPanel(
        metadata=metadata,
        outcomes=outcome_matrix,
        supported=support_matrix.astype(bool),
        features=feature_frame,
        observation_end_ts_ns=observation_end,
        unsupported_reasons=reason_matrix,
        redacted_finite_outcomes=redacted,
    )


def observation_end_aware_purge(
    panel: PreparedPanel,
    *,
    side: str,
    train_days: Sequence[str],
    test_days: Sequence[str],
    fold_id: str,
    stage: str,
) -> tuple[pd.Index, PurgeAudit]:
    """Purge train opportunities whose arm observations reach the test interval."""

    normalized_side = str(side).upper()
    metadata = panel.metadata
    train_set = {_normalize_day(value) for value in train_days}
    test_set = {_normalize_day(value) for value in test_days}
    train_index = metadata.index[
        (metadata["side"] == normalized_side) & metadata["utc_day"].isin(train_set)
    ]
    test_index = metadata.index[
        (metadata["side"] == normalized_side) & metadata["utc_day"].isin(test_set)
    ]
    if train_index.empty or test_index.empty:
        raise ModeledOofError(f"fold {fold_id} has an empty side-specific train/test interval")
    boundary = int(metadata.loc[test_index, "assignment_ts_ns"].min())
    ends = panel.observation_end_ts_ns.reindex(train_index)
    known = ends.notna()
    before = known & (ends < boundary)
    kept = train_index[before.to_numpy(dtype=bool)]
    audit = PurgeAudit(
        fold_id=fold_id,
        stage=stage,
        side=normalized_side,
        train_days=tuple(sorted(train_set)),
        test_days=tuple(sorted(test_set)),
        test_boundary_ts_ns=boundary,
        train_opportunities_before=int(len(train_index)),
        train_opportunities_after=int(len(kept)),
        purged_cross_boundary=int((known & ~before).sum()),
        purged_unknown_observation_end=int((~known).sum()),
    )
    if kept.empty:
        raise ModeledOofError(f"observation-end purge emptied fold {fold_id}")
    return kept, audit


def _campaign_weights(rows: pd.DataFrame) -> pd.Series:
    counts = rows.groupby("campaign_cluster_id", observed=True)["opportunity_id"].transform("count")
    weights = 1.0 / counts.astype(float)
    check = weights.groupby(rows["campaign_cluster_id"], observed=True).sum()
    if not np.allclose(check.to_numpy(dtype=float), 1.0, atol=1e-12, rtol=0.0):
        raise ModeledOofError("campaign total weight drifted from one")
    return weights


def _evaluate_actions(
    panel: PreparedPanel,
    *,
    side: str,
    opportunity_index: pd.Index,
    actions: Sequence[str],
    fold_id: str,
    stage: str,
    candidate_id: str,
) -> pd.DataFrame:
    metadata = panel.metadata.loc[opportunity_index]
    action_array = np.asarray(tuple(actions), dtype=object)
    if len(action_array) != len(metadata):
        raise ModeledOofError("selected action vector length drifted")
    vocabulary = set(duration_vocabulary(side))
    if set(action_array) - vocabulary:
        raise ModeledOofError("selected action falls outside the side vocabulary")
    outcomes = panel.outcomes.loc[opportunity_index]
    supported = panel.supported.loc[opportunity_index]
    unsupported_reasons = panel.unsupported_reasons.loc[opportunity_index]
    row_numbers = np.arange(len(metadata))
    action_positions = np.asarray([outcomes.columns.get_loc(action) for action in action_array])
    control_position = outcomes.columns.get_loc(CONTROL_ACTION)
    outcome_values = outcomes.to_numpy(dtype=float, copy=False)
    support_values = supported.to_numpy(dtype=bool, copy=False)
    reason_values = unsupported_reasons.to_numpy(dtype=object, copy=False)
    selected_value = outcome_values[row_numbers, action_positions]
    control_value = outcome_values[:, control_position]
    selected_supported = support_values[row_numbers, action_positions]
    control_supported = support_values[:, control_position]
    selected_reason = reason_values[row_numbers, action_positions]
    control_reason = reason_values[:, control_position]
    arm_values_identified = (
        selected_supported
        & control_supported
        & np.isfinite(selected_value)
        & np.isfinite(control_value)
    )
    same_action_consistency = action_array == CONTROL_ACTION
    # The policy contrast is exactly zero when both policies choose the same
    # action, even if that opportunity's absolute terminal value is censored.
    # This identifies the contrast without fabricating either arm outcome.
    identified = arm_values_identified | same_action_consistency
    uplift = np.where(
        same_action_consistency,
        0.0,
        np.where(arm_values_identified, selected_value - control_value, np.nan),
    )
    rows = pd.DataFrame(
        {
            "opportunity_id": metadata.index.astype(str),
            "utc_day": metadata["utc_day"].to_numpy(dtype=object),
            "panel_role": metadata["panel_role"].to_numpy(dtype=object),
            "side": side,
            "role_at_fill": metadata["role_at_fill"].to_numpy(dtype=object),
            "campaign_cluster_id": metadata["campaign_cluster_id"].to_numpy(dtype=object),
            "selected_action": action_array,
            "control_action": CONTROL_ACTION,
            "selected_nonbaseline": action_array != CONTROL_ACTION,
            "selected_supported": selected_supported,
            "control_supported": control_supported,
            "selected_support_reason": selected_reason,
            "control_support_reason": control_reason,
            "contrast_identified_by_same_action_consistency": same_action_consistency,
            "point_identified": identified,
            "selected_value_usdc": np.where(
                arm_values_identified, selected_value, np.nan
            ),
            "control_value_usdc": np.where(
                arm_values_identified, control_value, np.nan
            ),
            "uplift_usdc": uplift,
            "candidate_id": candidate_id,
            "fold_id": fold_id,
            "evaluation_stage": stage,
        }
    )
    rows["campaign_weight"] = _campaign_weights(rows)
    return rows


def partial_identification(
    rows: pd.DataFrame,
    *,
    confidence: float,
    uplift_bounds_usdc: tuple[float, float] | None,
) -> PartialIdentification:
    if rows.empty:
        raise ModeledOofError("cannot summarize an empty evaluation")
    weights = rows["campaign_weight"].to_numpy(dtype=float, copy=False)
    identified = rows["point_identified"].to_numpy(dtype=bool, copy=False)
    total_weight = float(weights.sum())
    identified_weight = float(weights[identified].sum())
    identified_fraction = identified_weight / total_weight
    if identified.any():
        values = rows.loc[identified, "uplift_usdc"].to_numpy(dtype=float, copy=False)
        observed_weights = weights[identified]
        mean = float(np.dot(observed_weights, values) / identified_weight)
        influence = observed_weights * (values - mean)
        clusters = (
            pd.Series(influence)
            .groupby(rows.loc[identified, "utc_day"].reset_index(drop=True), observed=True)
            .sum()
        )
        if len(clusters) < 2:
            standard_error = math.inf
        else:
            variance = (
                len(clusters)
                / (len(clusters) - 1.0)
                * float(np.square(clusters.to_numpy(dtype=float)).sum())
                / (identified_weight * identified_weight)
            )
            standard_error = math.sqrt(max(0.0, variance))
        critical = NormalDist().inv_cdf(0.5 + confidence / 2.0)
        lcb = mean - critical * standard_error
        ucb = mean + critical * standard_error
    else:
        mean = standard_error = lcb = ucb = None
    missing_weight = total_weight - identified_weight
    if missing_weight <= 1e-15:
        population_lower = mean
        population_upper = mean
    elif uplift_bounds_usdc is None:
        population_lower = population_upper = None
    else:
        observed_sum = 0.0
        if identified.any():
            observed_sum = float(
                np.dot(
                    weights[identified],
                    rows.loc[identified, "uplift_usdc"].to_numpy(dtype=float, copy=False),
                )
            )
        population_lower = (observed_sum + missing_weight * uplift_bounds_usdc[0]) / total_weight
        population_upper = (observed_sum + missing_weight * uplift_bounds_usdc[1]) / total_weight
    return PartialIdentification(
        denominator_opportunities=int(len(rows)),
        identified_opportunities=int(identified.sum()),
        unidentified_opportunities=int((~identified).sum()),
        identified_weight_fraction=float(identified_fraction),
        selected_action_unsupported=int((~rows["selected_supported"]).sum()),
        control_action_unsupported=int((~rows["control_supported"]).sum()),
        identified_mean_usdc=mean,
        identified_standard_error_usdc=standard_error,
        identified_lcb_usdc=lcb,
        identified_ucb_usdc=ucb,
        population_lower_bound_usdc=population_lower,
        population_upper_bound_usdc=population_upper,
        uplift_bounds_usdc=uplift_bounds_usdc,
        point_identified=bool(missing_weight <= 1e-15),
    )


def _action_support(rows: pd.DataFrame) -> dict[str, Any]:
    acted = rows.loc[rows["selected_nonbaseline"]]
    return {
        "action_opportunities": int(len(acted)),
        "action_campaigns": int(acted["campaign_cluster_id"].nunique()),
        "action_days": int(acted["utc_day"].nunique()),
        "action_rate": float(len(acted) / len(rows)) if len(rows) else 0.0,
    }


def _post_oof_gate(
    rows: pd.DataFrame,
    partial: PartialIdentification,
    *,
    scope: str,
    config: FrozenConfig,
) -> dict[str, Any]:
    support = _action_support(rows)
    reasons: list[str] = []
    if scope not in config.panel_days or not set(config.panel_days[scope]) <= set(
        config.prefix40_days
    ):
        reasons.append("panel_is_not_a_frozen_prefix40_development_subset")
    if config.deployment.require_full_identification and not partial.point_identified:
        reasons.append("oof_not_fully_point_identified")
    if partial.identified_lcb_usdc is None or not (
        partial.identified_lcb_usdc > config.deployment.economic_epsilon_usdc
    ):
        reasons.append("identified_oof_lcb_not_above_economic_epsilon")
    if not partial.point_identified:
        if partial.population_lower_bound_usdc is None:
            reasons.append("partial_identification_lower_bound_unavailable")
        elif not (partial.population_lower_bound_usdc > config.deployment.economic_epsilon_usdc):
            reasons.append("partial_identification_lower_bound_not_above_economic_epsilon")
    if support["action_rate"] < config.deployment.minimum_action_rate:
        reasons.append("action_rate_below_minimum")
    if support["action_campaigns"] < config.deployment.minimum_action_campaigns:
        reasons.append("action_campaigns_below_minimum")
    if support["action_days"] < config.deployment.minimum_action_days:
        reasons.append("action_days_below_minimum")
    fold_actions = rows.groupby("fold_id", observed=True)["selected_nonbaseline"].any()
    if config.deployment.require_outer_fold_nonbaseline_action and not fold_actions.all():
        reasons.append("one_or_more_outer_folds_have_no_nonbaseline_action")
    observed_roles = set(rows["role_at_fill"])
    if (
        config.deployment.require_opener_and_add_reporting
        and not {
            "opener",
            "add",
        }
        <= observed_roles
    ):
        reasons.append("opener_or_add_outer_oof_reporting_missing")
    passed = not reasons
    return {
        "evaluated_after_outer_oof": True,
        "evidence_route": EVIDENCE_ROUTE,
        "panel_scope": scope,
        "passed_for_owner_repeated_policy_successor": passed,
        "decision": "owner_replay_candidate_supported" if passed else "abstain",
        "reasons": reasons,
        "action_support": support,
        "action_authorized": False,
        "live_authorized": False,
        "strict_queue_authorized": False,
    }


def _continuous_diagnostic_gate(
    rows: pd.DataFrame,
    *,
    scope: str,
) -> dict[str, Any]:
    return {
        "evaluated_after_outer_oof": True,
        "evidence_route": EVIDENCE_ROUTE,
        "panel_scope": scope,
        "passed_for_owner_repeated_policy_successor": False,
        "decision": "diagnostic_only",
        "reasons": ["continuous_comparator_cannot_nominate_a_policy_successor"],
        "action_support": _action_support(rows),
        "action_authorized": False,
        "live_authorized": False,
        "strict_queue_authorized": False,
    }


def _validate_folds(
    panel: PreparedPanel,
    folds: Sequence[ChronologicalFold],
    *,
    scope: str,
    config: FrozenConfig,
) -> None:
    if scope not in config.panel_days:
        raise ModeledOofError(f"unknown modeled-label analysis panel: {scope}")
    allowed = set(config.panel_days[scope])
    observed = set(panel.metadata.loc[panel.metadata["utc_day"].isin(allowed), "utc_day"])
    if observed != allowed:
        raise ModeledOofError(f"feature/label panel does not cover all frozen days for {scope}")
    seen: set[str] = set()
    for fold in folds:
        if (set(fold.train_days) | set(fold.test_days)) - allowed:
            raise ModeledOofError(f"fold {fold.fold_id} references a day outside {scope}")
        if seen & set(fold.test_days):
            raise ModeledOofError("outer test folds overlap")
        seen.update(fold.test_days)


def _evaluate_boolean(
    panel: PreparedPanel,
    *,
    policy: BooleanCooldownPolicy,
    days: Sequence[str],
    feature_block: FeatureBlockSpec,
    fold_id: str,
    stage: str,
    opportunity_index: pd.Index | None = None,
) -> pd.DataFrame:
    eligible_index = panel.metadata.index[
        (panel.metadata["side"] == policy.side) & panel.metadata["utc_day"].isin(days)
    ]
    if opportunity_index is None:
        index = eligible_index
    else:
        requested = pd.Index(opportunity_index)
        if requested.has_duplicates or not set(requested) <= set(eligible_index):
            raise ModeledOofError(
                f"Boolean fold {fold_id} evaluation index escapes its side/day interval"
            )
        requested_set = set(requested)
        index = eligible_index[eligible_index.isin(requested_set)]
    if index.empty:
        raise ModeledOofError(f"Boolean fold {fold_id} has no opportunities")
    actions = policy.choose(panel.features.loc[index, list(feature_block.boolean_predicates)])
    return _evaluate_actions(
        panel,
        side=policy.side,
        opportunity_index=index,
        actions=actions,
        fold_id=fold_id,
        stage=stage,
        candidate_id=policy.candidate_id,
    )


def _inner_folds(outer: ChronologicalFold, config: FrozenConfig) -> tuple[ChronologicalFold, ...]:
    return expanding_chronological_folds(
        outer.train_days,
        fold_prefix=f"{outer.fold_id}.inner",
        n_folds=config.search.inner_folds,
        minimum_train_days=config.search.inner_minimum_train_days,
    )


def _candidate_rank(partial: PartialIdentification, candidate_id: str) -> tuple[float, str]:
    mean = partial.identified_mean_usdc
    return (-float(mean) if mean is not None else math.inf, candidate_id)


def run_boolean_nested_oof(
    panel: PreparedPanel,
    *,
    config: FrozenConfig,
    side: str,
    feature_block_name: str,
    scope: str,
    candidate_policies: Sequence[BooleanCooldownPolicy] | None = None,
) -> MethodResult:
    """Run exploratory Boolean selection without any pre-OOF LCB abstention."""

    block = config.feature_blocks[feature_block_name]
    folds = config.outer_folds[scope]
    _validate_folds(panel, folds, scope=scope, config=config)
    if candidate_policies is None:
        candidates = generate_bounded_candidates(
            side=side,
            predicate_columns=block.boolean_predicates,
            config=config.search,
            predicate_channel_groups=config.predicate_channel_groups or None,
            predicate_semantic_groups=config.predicate_semantic_groups or None,
            predicate_clock_groups=config.predicate_clock_groups or None,
        )
    else:
        candidates = tuple(candidate_policies)
    if not candidates or any(policy.side != side or not policy.rules for policy in candidates):
        raise ModeledOofError("Boolean exploration requires nonbaseline side-specific candidates")
    fold_reports: list[dict[str, Any]] = []
    selected_candidates: list[Mapping[str, Any]] = []
    outer_rows: list[pd.DataFrame] = []
    purge_audits: list[PurgeAudit] = []
    for outer in folds:
        outer_kept_index, outer_audit = observation_end_aware_purge(
            panel,
            side=side,
            train_days=outer.train_days,
            test_days=outer.test_days,
            fold_id=outer.fold_id,
            stage="outer_boolean_candidate_selection_guard",
        )
        purge_audits.append(outer_audit)
        inner = _inner_folds(outer, config)
        inner_evaluation_indices: dict[str, pd.Index] = {}
        inner_evaluation_reports: list[dict[str, Any]] = []
        for fold in inner:
            kept_metadata = panel.metadata.loc[outer_kept_index]
            evaluation_index = outer_kept_index[
                kept_metadata["utc_day"].isin(fold.test_days).to_numpy(dtype=bool)
            ]
            if evaluation_index.empty:
                raise ModeledOofError(
                    f"outer observation-end purge emptied Boolean candidate evaluation "
                    f"for {fold.fold_id}"
                )
            inner_evaluation_indices[fold.fold_id] = evaluation_index
            inner_evaluation_reports.append(
                {
                    "fold_id": fold.fold_id,
                    "evaluation_days": list(fold.test_days),
                    "evaluation_opportunity_count": int(len(evaluation_index)),
                    "evaluation_opportunity_ids": [str(value) for value in evaluation_index],
                    "restricted_to_outer_observation_end_purged_train_ids": True,
                    "candidate_training_rows_used": False,
                }
            )
        ranked: list[tuple[tuple[float, str], BooleanCooldownPolicy, PartialIdentification]] = []
        for policy in candidates:
            inner_rows: list[pd.DataFrame] = []
            for fold in inner:
                inner_rows.append(
                    _evaluate_boolean(
                        panel,
                        policy=policy,
                        days=fold.test_days,
                        feature_block=block,
                        fold_id=fold.fold_id,
                        stage="inner_oof",
                        opportunity_index=inner_evaluation_indices[fold.fold_id],
                    )
                )
            combined = pd.concat(inner_rows, ignore_index=True)
            combined["campaign_weight"] = _campaign_weights(combined)
            support = _action_support(combined)
            partial = partial_identification(
                combined,
                confidence=config.search.confidence,
                uplift_bounds_usdc=config.uplift_bounds_usdc,
            )
            if (
                support["action_opportunities"] >= config.search.minimum_action_opportunities
                and support["action_campaigns"] >= config.search.minimum_action_campaigns
                and support["action_days"] >= config.search.minimum_action_days
                and partial.identified_weight_fraction
                >= config.minimum_inner_identified_weight_fraction
                and partial.identified_mean_usdc is not None
            ):
                ranked.append((_candidate_rank(partial, policy.candidate_id), policy, partial))
        if not ranked:
            raise ModeledOofError(
                f"outer fold {outer.fold_id} has no supported nonbaseline Boolean candidate"
            )
        ranked.sort(key=lambda value: value[0])
        _, selected, inner_partial = ranked[0]
        rows = _evaluate_boolean(
            panel,
            policy=selected,
            days=outer.test_days,
            feature_block=block,
            fold_id=outer.fold_id,
            stage="outer_oof",
        )
        outer_rows.append(rows)
        selected_candidates.append(selected.payload())
        fold_reports.append(
            {
                "fold_id": outer.fold_id,
                "train_days": list(outer.train_days),
                "test_days": list(outer.test_days),
                "selected_candidate_id": selected.candidate_id,
                "candidate_replaced_by_baseline_before_outer_oof": False,
                "selection_used_lcb_abstention": False,
                "inner_partial_identification": asdict(inner_partial),
                "outer_action_support": _action_support(rows),
                "outer_purge": asdict(outer_audit),
                "outer_purge_kept_opportunity_count": int(len(outer_kept_index)),
                "outer_purge_kept_opportunity_ids": [
                    str(value) for value in outer_kept_index
                ],
                "inner_candidate_selection_evaluations": inner_evaluation_reports,
                "candidate_selection_training_rows_used": False,
            }
        )
    oof = pd.concat(outer_rows, ignore_index=True)
    oof["campaign_weight"] = _campaign_weights(oof)
    partial = partial_identification(
        oof,
        confidence=config.search.confidence,
        uplift_bounds_usdc=config.uplift_bounds_usdc,
    )
    return MethodResult(
        side=side,
        feature_block=feature_block_name,
        panel_scope=scope,
        method="bounded_sparse_boolean_dnf",
        oof_rows=oof,
        fold_reports=tuple(fold_reports),
        partial_identification=partial,
        deployment_gate=_post_oof_gate(oof, partial, scope=scope, config=config),
        selected_candidates=tuple(selected_candidates),
        purge_audits=tuple(purge_audits),
    )


def _fit_continuous_model(
    panel: PreparedPanel,
    *,
    side: str,
    train_index: pd.Index,
    feature_columns: Sequence[str],
    depth: int,
    config: FrozenConfig,
) -> tuple[DecisionTreeRegressor, dict[str, int]]:
    matrix = panel.features.loc[train_index, list(feature_columns)].to_numpy(dtype=float)
    actions = duration_vocabulary(side)[1:]
    support = panel.supported.loc[train_index, list(actions)].to_numpy(dtype=bool)
    targets = panel.outcomes.loc[train_index, list(actions)].to_numpy(dtype=float)
    mask = support.all(axis=1) & np.isfinite(targets).all(axis=1)
    support_count = int(mask.sum())
    support_counts = {action: support_count for action in actions}
    if support_count < config.continuous.minimum_train_rows_per_action:
        raise ModeledOofError("continuous comparator lacks jointly supported nonbaseline arm rows")
    eligible_index = train_index[mask]
    eligible_metadata = panel.metadata.loc[eligible_index]
    campaign_counts = eligible_metadata.groupby("campaign_cluster_id", observed=True)[
        "utc_day"
    ].transform("count")
    sample_weight = (1.0 / campaign_counts.astype(float)).to_numpy(dtype=float)
    campaign_totals = (
        pd.Series(sample_weight, index=eligible_index)
        .groupby(eligible_metadata["campaign_cluster_id"], observed=True)
        .sum()
    )
    if not np.allclose(campaign_totals.to_numpy(dtype=float), 1.0, atol=1e-12, rtol=0.0):
        raise ModeledOofError("continuous training campaign weights drifted from one")
    model = DecisionTreeRegressor(
        max_depth=depth,
        min_samples_leaf=config.continuous.min_samples_leaf,
        random_state=config.continuous.random_state,
    )
    model.fit(matrix[mask], targets[mask], sample_weight=sample_weight)
    return model, support_counts


def _continuous_actions(
    panel: PreparedPanel,
    *,
    test_index: pd.Index,
    feature_columns: Sequence[str],
    model: DecisionTreeRegressor,
    side: str,
) -> np.ndarray:
    matrix = panel.features.loc[test_index, list(feature_columns)].to_numpy(dtype=float)
    actions = duration_vocabulary(side)[1:]
    predictions = np.asarray(model.predict(matrix), dtype=float)
    if predictions.shape != (len(test_index), len(actions)):
        raise ModeledOofError("continuous multi-output prediction shape drifted")
    chosen = np.argmax(predictions, axis=1)
    return np.asarray([actions[index] for index in chosen], dtype=object)


def _continuous_fold_rows(
    panel: PreparedPanel,
    *,
    config: FrozenConfig,
    side: str,
    block: FeatureBlockSpec,
    fold: ChronologicalFold,
    depth: int,
    stage: str,
) -> tuple[pd.DataFrame, PurgeAudit, Mapping[str, int]]:
    train_index, audit = observation_end_aware_purge(
        panel,
        side=side,
        train_days=fold.train_days,
        test_days=fold.test_days,
        fold_id=fold.fold_id,
        stage=stage,
    )
    test_index = panel.metadata.index[
        (panel.metadata["side"] == side) & panel.metadata["utc_day"].isin(fold.test_days)
    ]
    model, counts = _fit_continuous_model(
        panel,
        side=side,
        train_index=train_index,
        feature_columns=block.continuous_features,
        depth=depth,
        config=config,
    )
    actions = _continuous_actions(
        panel,
        test_index=test_index,
        feature_columns=block.continuous_features,
        model=model,
        side=side,
    )
    candidate_id = _canonical_sha256(
        {
            "method": "continuous_multioutput_decision_tree",
            "side": side,
            "depth": depth,
            "features": list(block.continuous_features),
            "available_actions": list(duration_vocabulary(side)[1:]),
        }
    )
    rows = _evaluate_actions(
        panel,
        side=side,
        opportunity_index=test_index,
        actions=actions,
        fold_id=fold.fold_id,
        stage=stage,
        candidate_id=candidate_id,
    )
    return rows, audit, counts


def run_continuous_nested_oof(
    panel: PreparedPanel,
    *,
    config: FrozenConfig,
    side: str,
    feature_block_name: str,
    scope: str,
) -> MethodResult:
    """Run a per-arm continuous comparator alongside every Boolean block."""

    block = config.feature_blocks[feature_block_name]
    folds = config.outer_folds[scope]
    _validate_folds(panel, folds, scope=scope, config=config)
    fold_reports: list[Mapping[str, Any]] = []
    selected_candidates: list[Mapping[str, Any]] = []
    outer_rows: list[pd.DataFrame] = []
    purge_audits: list[PurgeAudit] = []
    for outer in folds:
        ranked: list[
            tuple[tuple[float, str], int, PartialIdentification, tuple[PurgeAudit, ...]]
        ] = []
        for depth in config.continuous.max_depth_candidates:
            inner_rows: list[pd.DataFrame] = []
            inner_audits: list[PurgeAudit] = []
            for inner in _inner_folds(outer, config):
                rows, inner_audit, _ = _continuous_fold_rows(
                    panel,
                    config=config,
                    side=side,
                    block=block,
                    fold=inner,
                    depth=depth,
                    stage="inner_continuous_oof",
                )
                inner_rows.append(rows)
                inner_audits.append(inner_audit)
            combined = pd.concat(inner_rows, ignore_index=True)
            combined["campaign_weight"] = _campaign_weights(combined)
            partial = partial_identification(
                combined,
                confidence=config.search.confidence,
                uplift_bounds_usdc=config.uplift_bounds_usdc,
            )
            if (
                partial.identified_weight_fraction
                >= config.minimum_inner_identified_weight_fraction
                and partial.identified_mean_usdc is not None
            ):
                ranked.append(
                    (
                        _candidate_rank(partial, str(depth)),
                        depth,
                        partial,
                        tuple(inner_audits),
                    )
                )
        if not ranked:
            raise ModeledOofError(
                f"outer fold {outer.fold_id} has no supported nonbaseline continuous candidate"
            )
        ranked.sort(key=lambda value: value[0])
        _, depth, inner_partial, selected_inner_audits = ranked[0]
        purge_audits.extend(selected_inner_audits)
        rows, audit, counts = _continuous_fold_rows(
            panel,
            config=config,
            side=side,
            block=block,
            fold=outer,
            depth=depth,
            stage="outer_continuous_oof",
        )
        purge_audits.append(audit)
        outer_rows.append(rows)
        candidate = {
            "method": "continuous_multioutput_decision_tree",
            "side": side,
            "max_depth": depth,
            "feature_columns": list(block.continuous_features),
            "control_excluded_from_exploratory_argmax": True,
            "candidate_replaced_by_baseline_before_outer_oof": False,
        }
        selected_candidates.append(candidate)
        fold_reports.append(
            {
                "fold_id": outer.fold_id,
                "train_days": list(outer.train_days),
                "test_days": list(outer.test_days),
                "selected_max_depth": depth,
                "candidate_replaced_by_baseline_before_outer_oof": False,
                "selection_used_lcb_abstention": False,
                "inner_partial_identification": asdict(inner_partial),
                "outer_action_support": _action_support(rows),
                "outer_purge": asdict(audit),
                "train_support_rows_by_nonbaseline_action": dict(counts),
            }
        )
    oof = pd.concat(outer_rows, ignore_index=True)
    oof["campaign_weight"] = _campaign_weights(oof)
    partial = partial_identification(
        oof,
        confidence=config.search.confidence,
        uplift_bounds_usdc=config.uplift_bounds_usdc,
    )
    return MethodResult(
        side=side,
        feature_block=feature_block_name,
        panel_scope=scope,
        method="continuous_multioutput_decision_tree",
        oof_rows=oof,
        fold_reports=tuple(fold_reports),
        partial_identification=partial,
        deployment_gate=_continuous_diagnostic_gate(
            oof,
            scope=scope,
        ),
        selected_candidates=tuple(selected_candidates),
        purge_audits=tuple(purge_audits),
    )


def _result_summary(result: MethodResult, *, confidence: float) -> dict[str, Any]:
    role_support: dict[str, Any] = {}
    for role in ("opener", "add"):
        rows = result.oof_rows.loc[result.oof_rows["role_at_fill"] == role].copy()
        if rows.empty:
            role_support[role] = {"opportunities": 0, "partial_identification": None}
        else:
            rows["campaign_weight"] = _campaign_weights(rows)
            role_support[role] = {
                "opportunities": int(len(rows)),
                "action_support": _action_support(rows),
                "partial_identification": asdict(
                    partial_identification(
                        rows,
                        confidence=confidence,
                        uplift_bounds_usdc=result.partial_identification.uplift_bounds_usdc,
                    )
                ),
            }
    return {
        "side": result.side,
        "feature_block": result.feature_block,
        "panel_scope": result.panel_scope,
        "method": result.method,
        "partial_identification": asdict(result.partial_identification),
        "deployment_gate": dict(result.deployment_gate),
        "unsupported_reason_counts": {
            "selected": {
                str(key): int(value)
                for key, value in result.oof_rows["selected_support_reason"]
                .value_counts(dropna=False)
                .sort_index()
                .items()
            },
            "control": {
                str(key): int(value)
                for key, value in result.oof_rows["control_support_reason"]
                .value_counts(dropna=False)
                .sort_index()
                .items()
            },
        },
        "folds": list(result.fold_reports),
        "role_support": role_support,
    }


def _comparison_cells(config: FrozenConfig) -> tuple[ComparisonCell, ...]:
    cells = tuple(
        ComparisonCell(scope, side, block)
        for scope in config.report_scopes
        for side in SIDES
        for block in config.panel_feature_blocks[scope]
    )
    if not cells or len(cells) != len(set(cells)):
        raise ModeledOofError("OOF comparison cells must be nonempty and unique")
    return cells


def _emit_cell_progress(
    *,
    status: str,
    cell: ComparisonCell,
    completed: int,
    total: int,
    worker_count: int,
    requested_worker_count: int,
    fallback_reason: Mapping[str, str] | None = None,
    result: ComparisonCellResult | None = None,
) -> None:
    payload: dict[str, Any] = {
        "status": status,
        "completed": completed,
        "total": total,
        "cell": cell.identity,
        "panel_scope": cell.panel_scope,
        "side": cell.side,
        "feature_block": cell.feature_block,
        "completed_cells": completed,
        "total_cells": total,
        "worker_count": worker_count,
        "requested_worker_count": requested_worker_count,
        "parallel_fallback": fallback_reason is not None,
    }
    if fallback_reason is not None:
        payload["fallback_reason"] = dict(fallback_reason)
    if result is not None:
        payload.update(
            {
                "boolean_gate_passed": bool(
                    result.boolean.deployment_gate.get("passed", False)
                ),
                "boolean_identified_mean_usdc": (
                    result.boolean.partial_identification.identified_mean_usdc
                ),
            }
        )
    print(_canonical_json(payload), flush=True)


def _emit_parallel_fallback(
    *,
    completed: int,
    total: int,
    requested_worker_count: int,
    fallback_reason: Mapping[str, str],
) -> None:
    print(
        _canonical_json(
            {
                "status": "parallel_fallback",
                "completed": completed,
                "total": total,
                "cell": "all",
                "completed_cells": completed,
                "total_cells": total,
                "worker_count": 1,
                "requested_worker_count": requested_worker_count,
                "parallel_fallback": True,
                "fallback_reason": dict(fallback_reason),
            }
        ),
        flush=True,
    )


def _run_comparison_cell(
    panel: PreparedPanel,
    config: FrozenConfig,
    cell: ComparisonCell,
) -> ComparisonCellResult:
    if (
        cell.panel_scope not in config.report_scopes
        or cell.side not in SIDES
        or cell.feature_block not in config.panel_feature_blocks[cell.panel_scope]
    ):
        raise ModeledOofError(f"unknown OOF comparison cell: {cell.identity}")
    expected_test_days = {
        day
        for fold in config.outer_folds[cell.panel_scope]
        for day in fold.test_days
    }
    expected_ids = set(
        panel.metadata.index[
            (panel.metadata["side"] == cell.side)
            & panel.metadata["utc_day"].isin(expected_test_days)
        ]
    )
    if not expected_ids:
        raise ModeledOofError(f"{cell.panel_scope}/{cell.side} has no outer OOF denominator")
    boolean = run_boolean_nested_oof(
        panel,
        config=config,
        side=cell.side,
        feature_block_name=cell.feature_block,
        scope=cell.panel_scope,
    )
    continuous = run_continuous_nested_oof(
        panel,
        config=config,
        side=cell.side,
        feature_block_name=cell.feature_block,
        scope=cell.panel_scope,
    )
    for result in (boolean, continuous):
        if set(result.oof_rows["opportunity_id"]) != expected_ids:
            raise ModeledOofError(
                f"{cell.identity}/{result.method} denominator drifted"
            )
    return ComparisonCellResult(cell=cell, boolean=boolean, continuous=continuous)


def _initialize_comparison_worker(panel: PreparedPanel, config: FrozenConfig) -> None:
    """Install one immutable panel snapshot per spawned worker.

    The parent reads and verifies the economic table once. Spawn serializes that
    prepared snapshot once per worker, avoiding unsafe shared pandas state and
    any worker-side artifact reads on macOS.
    """

    global _WORKER_PANEL, _WORKER_CONFIG
    _WORKER_PANEL = panel
    _WORKER_CONFIG = config


def _run_comparison_cell_worker(cell: ComparisonCell) -> ComparisonCellResult:
    if _WORKER_PANEL is None or _WORKER_CONFIG is None:
        raise ModeledOofError("OOF comparison worker was not initialized")
    return _run_comparison_cell(_WORKER_PANEL, _WORKER_CONFIG, cell)


def _comparison_worker_probe() -> str:
    if _WORKER_PANEL is None or _WORKER_CONFIG is None:
        raise ModeledOofError("OOF comparison worker probe was not initialized")
    return "ready"


def _pool_unavailable_reason(exc: BaseException) -> dict[str, str] | None:
    if isinstance(exc, NotImplementedError):
        return {"exception_type": type(exc).__name__, "message": str(exc)}
    unsupported_errno = {
        errno.EACCES,
        errno.EPERM,
        errno.ENOSYS,
        getattr(errno, "ENOTSUP", errno.ENOSYS),
    }
    if isinstance(exc, OSError) and exc.errno in unsupported_errno:
        return {"exception_type": type(exc).__name__, "message": str(exc)}
    return None


def _execute_cells_serially(
    panel: PreparedPanel,
    *,
    config: FrozenConfig,
    cells: Sequence[ComparisonCell],
    emit_progress: bool,
    requested_worker_count: int,
    fallback_reason: Mapping[str, str] | None,
) -> tuple[ComparisonCellResult, ...]:
    results: list[ComparisonCellResult] = []
    completed = 0
    for cell in cells:
        if emit_progress:
            _emit_cell_progress(
                status="cell_started",
                cell=cell,
                completed=completed,
                total=len(cells),
                worker_count=1,
                requested_worker_count=requested_worker_count,
                fallback_reason=fallback_reason,
            )
        result = _run_comparison_cell(panel, config, cell)
        results.append(result)
        completed += 1
        if emit_progress:
            _emit_cell_progress(
                status="cell_complete",
                cell=cell,
                completed=completed,
                total=len(cells),
                worker_count=1,
                requested_worker_count=requested_worker_count,
                fallback_reason=fallback_reason,
                result=result,
            )
    return tuple(results)


def _execute_comparison_cells(
    panel: PreparedPanel,
    *,
    config: FrozenConfig,
    workers: int,
    emit_progress: bool,
) -> tuple[ComparisonCellResult, ...]:
    if isinstance(workers, bool) or not isinstance(workers, int) or workers < 1:
        raise ModeledOofError("workers must be a positive integer")
    cells = _comparison_cells(config)
    worker_count = min(workers, len(cells))
    if worker_count == 1:
        return _execute_cells_serially(
            panel,
            config=config,
            cells=cells,
            emit_progress=emit_progress,
            requested_worker_count=workers,
            fallback_reason=None,
        )

    context = multiprocessing.get_context("spawn")
    try:
        executor = concurrent.futures.ProcessPoolExecutor(
            max_workers=worker_count,
            mp_context=context,
            initializer=_initialize_comparison_worker,
            initargs=(panel, config),
        )
    except (OSError, NotImplementedError) as exc:
        fallback_reason = _pool_unavailable_reason(exc)
        if fallback_reason is None:
            raise
        if emit_progress:
            _emit_parallel_fallback(
                completed=0,
                total=len(cells),
                requested_worker_count=workers,
                fallback_reason=fallback_reason,
            )
        return _execute_cells_serially(
            panel,
            config=config,
            cells=cells,
            emit_progress=emit_progress,
            requested_worker_count=workers,
            fallback_reason=fallback_reason,
        )

    completed = 0
    results: dict[ComparisonCell, ComparisonCellResult] = {}
    future_cells: dict[
        concurrent.futures.Future[ComparisonCellResult], ComparisonCell
    ] = {}
    try:
        probe = executor.submit(_comparison_worker_probe)
        if probe.result() != "ready":
            raise ModeledOofError("OOF comparison worker probe returned an invalid result")
    except (OSError, NotImplementedError) as exc:
        fallback_reason = _pool_unavailable_reason(exc)
        executor.shutdown(wait=True, cancel_futures=True)
        if fallback_reason is None:
            raise
        if emit_progress:
            _emit_parallel_fallback(
                completed=0,
                total=len(cells),
                requested_worker_count=workers,
                fallback_reason=fallback_reason,
            )
        return _execute_cells_serially(
            panel,
            config=config,
            cells=cells,
            emit_progress=emit_progress,
            requested_worker_count=workers,
            fallback_reason=fallback_reason,
        )
    except BaseException:
        executor.shutdown(wait=True, cancel_futures=True)
        raise

    try:
        for cell in cells:
            if emit_progress:
                _emit_cell_progress(
                    status="cell_submitted",
                    cell=cell,
                    completed=completed,
                    total=len(cells),
                    worker_count=worker_count,
                    requested_worker_count=workers,
                )
            future_cells[executor.submit(_run_comparison_cell_worker, cell)] = cell
        for future in concurrent.futures.as_completed(future_cells):
            submitted_cell = future_cells[future]
            result = future.result()
            if result.cell != submitted_cell or result.cell in results:
                raise ModeledOofError("parallel OOF cell result identity drifted")
            results[result.cell] = result
            completed += 1
            if emit_progress:
                _emit_cell_progress(
                    status="cell_complete",
                    cell=result.cell,
                    completed=completed,
                    total=len(cells),
                    worker_count=worker_count,
                    requested_worker_count=workers,
                    result=result,
                )
    finally:
        executor.shutdown(wait=True, cancel_futures=False)
    if set(results) != set(cells):
        raise ModeledOofError("OOF comparison cell result set is incomplete")
    return tuple(results[cell] for cell in cells)


def run_all_comparisons(
    panel: PreparedPanel,
    *,
    config: FrozenConfig,
    emit_progress: bool = False,
    workers: int = 1,
) -> tuple[dict[str, Any], pd.DataFrame, dict[str, Any], list[dict[str, Any]]]:
    """Compare R0/M0/M1/M2 Boolean and continuous policies by side."""

    if not isinstance(workers, int) or isinstance(workers, bool) or workers <= 0:
        raise ModeledOofError("workers must be a positive integer")
    label_source = config.spec_payload.get("modeled_label_source")
    if not isinstance(label_source, Mapping):
        raise ModeledOofError("owner spec modeled-label source is missing")
    source_opportunities = int(label_source.get("opportunity_rows", -1))
    source_arm_rows = int(label_source.get("arm_rows", -1))
    arms_per_opportunity = int(
        label_source.get(
            "arm_count_per_opportunity",
            source_arm_rows // source_opportunities if source_opportunities > 0 else -1,
        )
    )
    if (
        arms_per_opportunity <= 0
        or source_arm_rows != source_opportunities * arms_per_opportunity
    ):
        raise ModeledOofError("owner spec executed-arm census drifted")
    executed_arm_rows = len(panel.metadata) * arms_per_opportunity

    report: dict[str, Any] = {
        "schema_version": OUTPUT_SCHEMA,
        "identity": IDENTITY,
        "evidence_route": EVIDENCE_ROUTE,
        "queue_authority": QUEUE_AUTHORITY,
        "strict_queue_policy_eligible": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "redacted_finite_unsupported_outcomes": panel.redacted_finite_outcomes,
        "modeled_label_census": {
            "opportunities": int(len(panel.metadata)),
            "arm_rows": executed_arm_rows,
            "arm_count_per_opportunity": arms_per_opportunity,
            "dense_side_action_slots": int(panel.supported.size),
            "dense_side_action_slots_are_executed_arms": False,
            "point_eligible_opportunities": int(panel.supported.all(axis=1).sum()),
            "not_point_eligible_opportunities": int((~panel.supported.all(axis=1)).sum()),
            "unsupported_arm_values_imputed": 0,
        },
        "results": {},
        "panel_denominators": {},
        "not_run_panels": {
            name: {
                **dict(contract),
                "modeled_labels_imputed": False,
                "economic_oof_run": False,
                "may_grant_support": False,
            }
            for name, contract in config.not_run_panels.items()
        },
        "permissions": {
            "research_authority": "owner_route_exploratory_only",
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    all_rows: list[pd.DataFrame] = []
    policies: dict[str, Any] = {}
    purges: list[dict[str, Any]] = []
    for scope in config.report_scopes:
        report["results"][scope] = {}
        report["panel_denominators"][scope] = {
            "days": list(config.panel_days[scope]),
            "day_count": len(config.panel_days[scope]),
            "eligible_feature_blocks": list(config.panel_feature_blocks[scope]),
            "all_blocks_use_common_scope_denominator": True,
            "sides": {},
        }
        policies[scope] = {}
        for side in SIDES:
            report["results"][scope][side] = {}
            policies[scope][side] = {}
            expected_test_days = {
                day for fold in config.outer_folds[scope] for day in fold.test_days
            }
            expected_ids = set(
                panel.metadata.index[
                    (panel.metadata["side"] == side)
                    & panel.metadata["utc_day"].isin(expected_test_days)
                ]
            )
            if not expected_ids:
                raise ModeledOofError(f"{scope}/{side} has no outer OOF denominator")
            report["panel_denominators"][scope]["sides"][side] = {
                "outer_oof_opportunities": len(expected_ids),
                "outer_oof_test_days": sorted(expected_test_days),
            }
    cell_results = _execute_comparison_cells(
        panel,
        config=config,
        workers=workers,
        emit_progress=emit_progress,
    )
    for cell_result in cell_results:
        cell = cell_result.cell
        boolean = cell_result.boolean
        continuous = cell_result.continuous
        report["results"][cell.panel_scope][cell.side][cell.feature_block] = {
            "boolean": _result_summary(boolean, confidence=config.search.confidence),
            "continuous": _result_summary(continuous, confidence=config.search.confidence),
        }
        policies[cell.panel_scope][cell.side][cell.feature_block] = {
            "boolean": list(boolean.selected_candidates),
            "continuous": list(continuous.selected_candidates),
        }
        for result in (boolean, continuous):
            rows = result.oof_rows.copy()
            rows["method"] = result.method
            rows["feature_block"] = cell.feature_block
            rows["panel_scope"] = cell.panel_scope
            all_rows.append(rows)
            purges.extend(asdict(audit) for audit in result.purge_audits)
    return report, pd.concat(all_rows, ignore_index=True), policies, purges


def _code_bindings(config: FrozenConfig) -> dict[str, str]:
    if dict(config.expected_library_versions) != runtime_library_versions():
        raise ModeledOofError("frozen OOF runtime library versions drifted")
    paths = list(_required_oof_code_paths())
    paths.extend(path for path, _ in config.code_bindings)
    expected = {str(path): sha256 for path, sha256 in config.code_bindings}
    output: dict[str, str] = {}
    for path in paths:
        actual = _sha256(path)
        if str(path) in expected and actual != expected[str(path)]:
            raise ModeledOofError(f"frozen code binding drifted: {path}")
        output[str(path)] = actual
    return dict(sorted(output.items()))


def load_bound_panel(
    config: FrozenConfig,
    *,
    execution_amendment: ExecutionAmendmentBinding,
) -> tuple[PreparedPanel, dict[str, Any]]:
    """Verify manifests/hashes, then load only the frozen label/feature columns."""

    if not isinstance(execution_amendment, ExecutionAmendmentBinding):
        raise ModeledOofError("verified OOF execution amendment is required before economic read")
    if tuple(config.code_bindings) != execution_amendment.code_bindings:
        raise ModeledOofError("OOF execution amendment/code binding drifted before economic read")
    if dict(config.expected_library_versions) != dict(execution_amendment.library_versions):
        raise ModeledOofError("OOF execution amendment/library binding drifted before economic read")
    label_binding = verify_input_artifact(config.labels)
    feature_binding = verify_input_artifact(config.features)
    columns = config.columns
    label_columns = {
        columns.opportunity,
        columns.day,
        columns.side,
        columns.role,
        columns.campaign,
        columns.action,
        columns.outcome,
        columns.assignment_time.name,
        columns.eligible,
        columns.right_censored,
        columns.joint_censored,
        columns.exact_queue_eligible,
        *(field.name for field in columns.observation_end),
    }
    feature_columns = _feature_artifact_columns(config)
    labels = _read_parquet_tables(label_binding.table_paths, sorted(label_columns))
    features = _read_parquet_tables(feature_binding.table_paths, sorted(feature_columns))
    panel = prepare_modeled_panel(labels, features, config=config)
    bindings = {
        "frozen_config": {"path": str(config.path), "sha256": config.sha256},
        "frozen_owner_spec": {
            "path": str(config.spec_path),
            "sha256": config.spec_sha256,
        },
        "modeled_labels": {
            "manifest_path": str(label_binding.manifest_path),
            "manifest_sha256": label_binding.manifest_sha256,
            "manifest_identity": label_binding.manifest_identity,
            "tables": label_binding.table_sha256,
        },
        "multichannel_features": {
            "manifest_path": str(feature_binding.manifest_path),
            "manifest_sha256": feature_binding.manifest_sha256,
            "manifest_identity": feature_binding.manifest_identity,
            "tables": feature_binding.table_sha256,
        },
        "execution_amendment": {
            "path": str(execution_amendment.path),
            "sha256": execution_amendment.sha256,
            "execution_identity_sha256": execution_amendment.execution_identity_sha256,
            "artifact_bindings": execution_amendment.artifact_bindings,
        },
        "code": _code_bindings(config),
        "library_versions": runtime_library_versions(),
    }
    if config.predicate_materialization is not None:
        bindings["outcome_blind_2025_predicates"] = (
            predicate_materialization_binding_payload(config.predicate_materialization)
        )
    bindings["binding_sha256"] = _canonical_sha256(bindings)
    return panel, bindings


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not math.isfinite(float(value)) else float(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(_canonical_json(_json_safe(payload)) + "\n", encoding="ascii")
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def publish_atomic_output(
    output: Path,
    *,
    config: FrozenConfig,
    bindings: Mapping[str, Any],
    report: Mapping[str, Any],
    oof_rows: pd.DataFrame,
    policies: Mapping[str, Any],
    purge_audits: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Publish a complete output directory with one same-filesystem rename."""

    destination = Path(output).expanduser().resolve()
    if destination.exists():
        raise ModeledOofError(f"refusing to replace existing output: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    stage = destination.parent / f".{destination.name}.staging-{uuid.uuid4().hex}"
    stage.mkdir()
    try:
        (stage / "frozen_config.json").write_bytes(config.path.read_bytes())
        with (stage / "frozen_config.json").open("rb") as handle:
            os.fsync(handle.fileno())
        (stage / "frozen_owner_spec.json").write_bytes(config.spec_path.read_bytes())
        with (stage / "frozen_owner_spec.json").open("rb") as handle:
            os.fsync(handle.fileno())
        _write_json(stage / "bindings.json", bindings)
        _write_json(stage / "report.json", report)
        _write_json(stage / "selected_candidates.json", policies)
        _write_json(stage / "purge_audits.json", {"rows": list(purge_audits)})
        oof_rows.to_parquet(stage / "outer_oof.parquet", index=False)
        with (stage / "outer_oof.parquet").open("rb") as handle:
            os.fsync(handle.fileno())
        files = []
        for path in sorted(stage.iterdir()):
            files.append(
                {
                    "relative_path": path.name,
                    "bytes": path.stat().st_size,
                    "sha256": _sha256(path),
                }
            )
        manifest = {
            "schema_version": MANIFEST_SCHEMA,
            "identity": IDENTITY,
            "evidence_route": EVIDENCE_ROUTE,
            "queue_authority": QUEUE_AUTHORITY,
            "config_sha256": config.sha256,
            "owner_spec_sha256": config.spec_sha256,
            "binding_sha256": bindings["binding_sha256"],
            "files": files,
            "permissions": {
                "strict_queue_authorized": False,
                "action_authorized": False,
                "live_authorized": False,
            },
        }
        _write_json(stage / "manifest.json", manifest)
        manifest_sha256 = _sha256(stage / "manifest.json")
        (stage / "_SUCCESS").write_text(manifest_sha256 + "\n", encoding="ascii")
        with (stage / "_SUCCESS").open("rb") as handle:
            os.fsync(handle.fileno())
        _fsync_directory(stage)
        os.replace(stage, destination)
        _fsync_directory(destination.parent)
        return {**manifest, "manifest_sha256": manifest_sha256, "output": str(destination)}
    except Exception:
        shutil.rmtree(stage, ignore_errors=True)
        raise


def run_from_config(
    config_path: Path,
    *,
    expected_config_sha256: str,
    spec_path: Path,
    expected_spec_sha256: str,
    feature_manifest_path: Path,
    feature_manifest_sha256: str,
    feature_table_globs: Sequence[str],
    execution_amendment_path: Path,
    execution_amendment_sha256: str,
    output: Path,
    workers: int = 1,
) -> dict[str, Any]:
    config = load_frozen_config(
        config_path,
        expected_sha256=expected_config_sha256,
        spec_path=spec_path,
        expected_spec_sha256=expected_spec_sha256,
        feature_manifest_path=feature_manifest_path,
        feature_manifest_sha256=feature_manifest_sha256,
        feature_table_globs=feature_table_globs,
    )
    config, execution_amendment = load_execution_amendment(
        execution_amendment_path,
        expected_sha256=execution_amendment_sha256,
        config=config,
    )
    panel, bindings = load_bound_panel(
        config,
        execution_amendment=execution_amendment,
    )
    report, rows, policies, purges = run_all_comparisons(
        panel,
        config=config,
        emit_progress=True,
        workers=workers,
    )
    report = {
        **report,
        "config_sha256": config.sha256,
        "binding_sha256": bindings["binding_sha256"],
    }
    return publish_atomic_output(
        output,
        config=config,
        bindings=bindings,
        report=report,
        oof_rows=rows,
        policies=policies,
        purge_audits=purges,
    )


def _positive_worker_count(value: str) -> int:
    try:
        workers = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("workers must be a positive integer") from exc
    if workers < 1:
        raise argparse.ArgumentTypeError("workers must be a positive integer")
    return workers


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("preflight", "run"):
        child = subparsers.add_parser(command)
        child.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
        child.add_argument("--config-sha256", default=DEFAULT_CONFIG_SHA256)
        child.add_argument("--spec", type=Path, default=DEFAULT_SPEC_PATH)
        child.add_argument("--spec-sha256", default=DEFAULT_SPEC_SHA256)
        child.add_argument("--feature-manifest", type=Path, required=True)
        child.add_argument("--feature-manifest-sha256", required=True)
        child.add_argument("--execution-amendment", type=Path, required=True)
        child.add_argument("--execution-amendment-sha256", required=True)
        child.add_argument(
            "--feature-table-glob",
            action="append",
            dest="feature_table_globs",
            default=None,
        )
        if command == "run":
            child.add_argument("--output", type=Path, required=True)
            child.add_argument("--workers", type=_positive_worker_count, default=1)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    feature_table_globs = tuple(args.feature_table_globs or ("*.parquet", "**/*.parquet"))
    if args.command == "preflight":
        config = load_frozen_config(
            args.config,
            expected_sha256=args.config_sha256,
            spec_path=args.spec,
            expected_spec_sha256=args.spec_sha256,
            feature_manifest_path=args.feature_manifest,
            feature_manifest_sha256=args.feature_manifest_sha256,
            feature_table_globs=feature_table_globs,
        )
        config, execution_amendment = load_execution_amendment(
            args.execution_amendment,
            expected_sha256=args.execution_amendment_sha256,
            config=config,
        )
        panel, bindings = load_bound_panel(
            config,
            execution_amendment=execution_amendment,
        )
        label_source = config.spec_payload.get("modeled_label_source")
        if not isinstance(label_source, Mapping):
            raise ModeledOofError("owner spec modeled-label source is missing")
        print(
            _canonical_json(
                {
                    "identity": IDENTITY,
                    "config_sha256": config.sha256,
                    "binding_sha256": bindings["binding_sha256"],
                    "opportunities": int(len(panel.metadata)),
                    "arm_rows": int(label_source["arm_rows"]),
                    "arm_count_per_opportunity": int(
                        label_source["arm_count_per_opportunity"]
                    ),
                    "dense_side_action_slots": int(panel.supported.size),
                    "redacted_finite_unsupported_outcomes": panel.redacted_finite_outcomes,
                    "predicate_materialization_identity_sha256": (
                        None
                        if config.predicate_materialization is None
                        else config.predicate_materialization.materialization_identity_sha256
                    ),
                    "permissions": {"action_authorized": False, "live_authorized": False},
                }
            )
        )
        return 0
    result = run_from_config(
        args.config,
        expected_config_sha256=args.config_sha256,
        spec_path=args.spec,
        expected_spec_sha256=args.spec_sha256,
        feature_manifest_path=args.feature_manifest,
        feature_manifest_sha256=args.feature_manifest_sha256,
        feature_table_globs=feature_table_globs,
        execution_amendment_path=args.execution_amendment,
        execution_amendment_sha256=args.execution_amendment_sha256,
        output=args.output,
        workers=args.workers,
    )
    print(_canonical_json(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "CONFIG_SCHEMA",
    "EVIDENCE_ROUTE",
    "EXECUTION_AMENDMENT_SCHEMA",
    "EXPECTED_FEATURE_MANIFEST_IDENTITY",
    "ExecutionAmendmentBinding",
    "FrozenConfig",
    "IDENTITY",
    "MANIFEST_SCHEMA",
    "MethodResult",
    "ModeledOofError",
    "OUTPUT_SCHEMA",
    "PartialIdentification",
    "PreparedPanel",
    "QUEUE_AUTHORITY",
    "load_bound_panel",
    "load_execution_amendment",
    "load_frozen_config",
    "observation_end_aware_purge",
    "partial_identification",
    "prepare_modeled_panel",
    "publish_atomic_output",
    "run_all_comparisons",
    "run_boolean_nested_oof",
    "run_continuous_nested_oof",
    "run_from_config",
    "runtime_library_versions",
    "verify_input_artifact",
]
