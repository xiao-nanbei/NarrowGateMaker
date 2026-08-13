"""Fixed formal backend for the offline F05 full-multiscale successor.

The backend accepts only the orchestrator's ``FormalOfflineBundle``.  It loads
and revalidates the five outcome-blind mechanics tables, materializes one-shot
training labels only for a purged outer-train request through one fixed replay
adapter, and evaluates every fitted candidate through paired sequential replay.

The real replay adapter is a separate, fixed module.  Formal execution stops
before value access when the adapter is absent or when the admitted canonical
panel lacks fields needed by the fixed bridge.  Synthetic outcomes, neutral-zero
targets, custom evaluators, and one-shot aggregation are never fallback paths.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_nested_oof_v1 as nested,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_orchestrator_v1 as orchestrator,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_v1 as offline,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_nested_oof import (
    duration_vocabulary,
)

IDENTITY = f"{offline.IDENTITY}.offline_repeated_policy_backend_v1"
FORMAL_RESULT_SCHEMA = orchestrator.FORMAL_RESULT_SCHEMA
MECHANICS_READY_STATUS = "formal_offline_replay_mechanics_ready"
FORMAL_PANEL_SCHEMA_READY_STATUS = "formal_panel_schema_ready"
CANONICAL_FIELDS_BLOCKED_STATUS = "blocked_missing_canonical_fields"
CANONICAL_REPLAY_ADAPTER_MODULE = (
    "research.families.f05_fill_quality_quote_ev.audit."
    "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
    "offline_replay_adapter_v1"
)
CANONICAL_REPLAY_ADAPTER_FACTORY = "build_canonical_replay_adapter"
CANONICAL_REPLAY_ADAPTER_IDENTITY = f"{offline.IDENTITY}.offline_replay_adapter_v1"
LABEL_REPLAY_RECEIPT_SCHEMA = f"{IDENTITY}.outer_train_label_replay_receipt.v1"
SEQUENTIAL_REPLAY_RECEIPT_SCHEMA = f"{IDENTITY}.sequential_replay_receipt.v1"
BLOCKED_STATUS = "mechanics_only_backend_incomplete"

_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_PANEL_ROLES = (
    "metadata",
    "boolean_features",
    "continuous_features",
    "exact_owner_actions",
    "replay_inputs",
)
_FORBIDDEN_FILE_ROLES = frozenset(
    {
        "action_outcomes",
        "action_supported",
        "economic_outcomes",
        "one_shot_training_labels",
    }
)
_FORBIDDEN_ECONOMIC_COLUMN_PARTS = (
    "action_outcome",
    "economic_outcome",
    "terminal_value",
    "closed_campaign_value",
    "campaign_value_usdc",
    "reward",
    "profit",
    "pnl",
    "markout",
    "value_label",
)
_OUTCOME_BLIND_REPLAY_DECLARATIONS = frozenset(
    {
        "candidate_actions_generated",
        "economic_outcomes_read",
        "labels_read",
    }
)
_INDEX_COLUMNS = ("utc_day", "opportunity_id")
REPLAY_METADATA_DIRECT_BINDINGS = {
    "side": "side",
    "assignment_ts_ns": "assignment_ts_ns",
    "observation_end_ts_ns": "observation_end_ts_ns",
    "baseline_duration_ms": "baseline_duration_ms",
    "role_at_fill": "role_at_fill",
    "inventory_after_fill_btc": "inventory_after_fill_btc",
}
REPLAY_FILL_VISIBLE_MS_SOURCE = "fill_visible_ts_ns"
_BUY_OWNER_ACTIONS = frozenset({"CONTROL_85N"})
_SELL_OWNER_ACTIONS = frozenset(
    {"CONTROL_85N", "FIXED_166S", "FIXED_211S", "FIXED_1748S"}
)


class OfflineRepeatedPolicyBackendError(RuntimeError):
    """Raised when formal backend inputs or replay receipts drift."""


class OfflineRepeatedPolicyBackendIncomplete(OfflineRepeatedPolicyBackendError):
    """Raised when formal replay cannot proceed without inventing evidence."""

    def __init__(self, result_manifest: Mapping[str, Any]) -> None:
        self.result_manifest = dict(result_manifest)
        blocker = str(self.result_manifest.get("blocker", "canonical replay adapter missing"))
        status = str(self.result_manifest.get("status", BLOCKED_STATUS))
        super().__init__(f"{status}: {blocker}")


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _document_sha256(value: Mapping[str, Any], field: str) -> str:
    payload = dict(value)
    payload.pop(field, None)
    return _canonical_sha256(payload)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _frame_sha256(frame: pd.DataFrame | pd.Series) -> str:
    value = frame.to_frame() if isinstance(frame, pd.Series) else frame
    header = {
        "columns": [str(column) for column in value.columns],
        "dtypes": [str(dtype) for dtype in value.dtypes],
        "index_name": None if value.index.name is None else str(value.index.name),
        "index_dtype": str(value.index.dtype),
        "rows": int(len(value)),
    }
    digest = hashlib.sha256()
    digest.update(_canonical_sha256(header).encode("ascii"))
    hashed = pd.util.hash_pandas_object(value, index=True, categorize=False)
    digest.update(hashed.to_numpy(dtype="<u8", copy=False).tobytes())
    return digest.hexdigest()


def _normalize_day(value: Any) -> str:
    try:
        parsed = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise OfflineRepeatedPolicyBackendError(f"invalid UTC day: {value!r}") from exc
    if parsed.tzinfo is not None:
        parsed = parsed.tz_convert("UTC").tz_localize(None)
    if parsed != parsed.normalize():
        raise OfflineRepeatedPolicyBackendError(f"UTC day has a time component: {value!r}")
    return parsed.strftime("%Y-%m-%d")


def _normalize_side(value: Any) -> str:
    side = str(value).strip().upper()
    if side not in {"BUY", "SELL"}:
        raise OfflineRepeatedPolicyBackendError(f"invalid cooldown side: {value!r}")
    return side


def _require_sha(value: Any, *, label: str) -> str:
    digest = str(value)
    if _SHA_RE.fullmatch(digest) is None:
        raise OfflineRepeatedPolicyBackendError(f"{label} is not a lowercase SHA256")
    return digest


def _parquet_schema(path: Path) -> tuple[int, Mapping[str, Any]]:
    try:
        parquet = pq.ParquetFile(path)
    except (OSError, ValueError) as exc:
        raise OfflineRepeatedPolicyBackendError(f"cannot inspect mechanics Parquet: {path}") from exc
    schema = parquet.schema_arrow
    return int(parquet.metadata.num_rows), {
        "columns": [str(name) for name in schema.names],
        "types": [str(schema.field(index).type) for index in range(len(schema))],
    }


def _economic_columns(columns: Sequence[Any], *, role: str) -> tuple[str, ...]:
    return tuple(
        str(column)
        for column in columns
        if not (
            role == "replay_inputs"
            and str(column) in _OUTCOME_BLIND_REPLAY_DECLARATIONS
        )
        if any(token in str(column).lower() for token in _FORBIDDEN_ECONOMIC_COLUMN_PARTS)
    )


def _verify_outcome_blind_replay_declarations(
    frame: pd.DataFrame,
    *,
    schema: Mapping[str, Any],
) -> None:
    schema_types = dict(zip(schema["columns"], schema["types"], strict=True))
    for column in sorted(_OUTCOME_BLIND_REPLAY_DECLARATIONS & set(frame.columns)):
        if schema_types.get(column) != "bool":
            raise OfflineRepeatedPolicyBackendError(
                f"replay declaration {column} must be non-nullable Boolean false"
            )
        values = frame[column]
        if values.isna().any() or set(values.tolist()) != {False}:
            raise OfflineRepeatedPolicyBackendError(
                f"replay declaration {column} must be false for every row"
            )


def _verify_bound_panel_file(
    role: str,
    path: Path,
    binding: Mapping[str, Any],
) -> pd.DataFrame:
    resolved = path.expanduser().resolve()
    if resolved.suffix != ".parquet" or not resolved.is_file():
        raise OfflineRepeatedPolicyBackendError(f"panel {role} is not a Parquet file")
    expected_sha = _require_sha(binding.get("sha256"), label=f"panel {role} SHA256")
    if _file_sha256(resolved) != expected_sha:
        raise OfflineRepeatedPolicyBackendError(f"panel {role} byte hash drifted")
    if "size_bytes" in binding and int(binding["size_bytes"]) != resolved.stat().st_size:
        raise OfflineRepeatedPolicyBackendError(f"panel {role} size drifted")
    rows, schema = _parquet_schema(resolved)
    if "rows" in binding and int(binding["rows"]) != rows:
        raise OfflineRepeatedPolicyBackendError(f"panel {role} row count drifted")
    if "schema" in binding and binding["schema"] != schema:
        raise OfflineRepeatedPolicyBackendError(f"panel {role} schema drifted")
    if forbidden := _economic_columns(schema["columns"], role=role):
        raise OfflineRepeatedPolicyBackendError(
            f"panel {role} contains pre-injected economic columns: {sorted(forbidden)}"
        )
    try:
        frame = pd.read_parquet(resolved)
    except (OSError, ValueError) as exc:
        raise OfflineRepeatedPolicyBackendError(f"cannot load panel {role}") from exc
    if role == "replay_inputs":
        _verify_outcome_blind_replay_declarations(frame, schema=schema)
    return frame


def _ordered_day_audit(values: pd.Series, selected_days: Sequence[str], *, role: str) -> pd.Series:
    days = values.map(_normalize_day)
    transitions: list[str] = []
    for day in days:
        if not transitions or transitions[-1] != day:
            transitions.append(day)
    if tuple(transitions) != tuple(selected_days):
        raise OfflineRepeatedPolicyBackendError(f"panel {role} day order drifted")
    return days


def _index_panel_table(
    frame: pd.DataFrame,
    *,
    role: str,
    selected_days: Sequence[str],
) -> pd.DataFrame:
    missing = set(_INDEX_COLUMNS) - set(frame.columns)
    if missing:
        raise OfflineRepeatedPolicyBackendError(
            f"panel {role} lacks stable row keys: {sorted(missing)}"
        )
    rows = frame.copy()
    rows["utc_day"] = _ordered_day_audit(rows["utc_day"], selected_days, role=role)
    identifiers = rows["opportunity_id"].astype(str)
    if identifiers.str.strip().eq("").any() or identifiers.duplicated().any():
        raise OfflineRepeatedPolicyBackendError(f"panel {role} opportunity ids are invalid")
    rows["opportunity_id"] = identifiers
    return rows.set_index("opportunity_id", drop=False)


@dataclass(frozen=True, slots=True)
class FormalExecutionBindings:
    execution_manifest_sha256: str
    source_manifest_sha256: str
    panel_manifest_sha256: str
    fold_manifest_sha256: str
    nested_fold_manifest_sha256: str
    exact_owner_policy_sha256: str
    exact_owner_predicate_bundle_sha256: str
    exact_owner_private_config_sha256: str

    def payload(self) -> dict[str, str]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class OutcomeBlindMechanics:
    panel: nested.NestedOofPanel
    replay_inputs: pd.DataFrame
    selected_days: tuple[str, ...]
    fold_manifest: nested.ProspectiveFoldManifest
    bindings: FormalExecutionBindings
    file_sha256: Mapping[str, str]
    mechanics_receipt_sha256: str


@dataclass(frozen=True, slots=True)
class CanonicalOuterTrainReplayRequest:
    label_request: nested.FoldScopedOneShotLabelRequest
    replay_input_sha256: str
    bindings: FormalExecutionBindings


@dataclass(frozen=True, slots=True)
class CanonicalOneShotReplayResult:
    outcomes: pd.DataFrame
    supported: pd.DataFrame
    receipt: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class CanonicalSequentialReplayRequest:
    evaluation_request: nested.EvaluationRequest
    replay_input_sha256: str
    bindings: FormalExecutionBindings


@dataclass(frozen=True, slots=True)
class CanonicalSequentialReplayResult:
    rows: pd.DataFrame
    receipt: Mapping[str, Any]


class CanonicalReplayAdapter(Protocol):
    identity: str
    artifact_sha256: str

    def preflight_formal_panel_schema(
        self,
        *,
        metadata_columns: Sequence[str],
        replay_input_columns: Sequence[str],
    ) -> Mapping[str, Any]: ...

    def build_search_contract(
        self,
        mechanics: OutcomeBlindMechanics,
    ) -> tuple[tuple[nested.CandidateLadderEntry, ...], nested.ContinuousComparatorEntry]: ...

    def preflight_formal_economics(
        self,
        mechanics: OutcomeBlindMechanics,
    ) -> Mapping[str, Any]: ...

    def generate_outer_train_one_shot_labels(
        self,
        request: CanonicalOuterTrainReplayRequest,
        replay_inputs: pd.DataFrame,
    ) -> CanonicalOneShotReplayResult: ...

    def evaluate_repeated_policy(
        self,
        request: CanonicalSequentialReplayRequest,
        replay_inputs: pd.DataFrame,
    ) -> CanonicalSequentialReplayResult: ...


def _build_fold_manifest(
    source_manifest: Mapping[str, Any],
    selected_days: Sequence[str],
) -> tuple[nested.ProspectiveFoldManifest, str, Mapping[str, Any]]:
    days = tuple(selected_days)
    try:
        bound = offline.derive_bound_nested_fold_manifest(source_manifest)
    except offline.OfflineSourceGateError as exc:
        raise OfflineRepeatedPolicyBackendError(
            "source admission cannot derive the frozen 4x3 nested folds"
        ) from exc
    if tuple(bound.get("active_days") or ()) != days:
        raise OfflineRepeatedPolicyBackendError("source nested-fold days drifted")
    source_sha = _require_sha(
        bound.get("source_fold_manifest_sha256"),
        label="source fold manifest SHA256",
    )
    nested_sha = _require_sha(
        bound.get("nested_fold_manifest_sha256"),
        label="nested-fold manifest SHA256",
    )
    outer = bound.get("outer_folds")
    if not isinstance(outer, list) or len(outer) != 4:
        raise OfflineRepeatedPolicyBackendError("bound nested-fold census drifted")
    return (
        nested.ProspectiveFoldManifest(
            active_days=days,
            outer_folds=tuple(outer),
            manifest_sha256=nested_sha,
        ),
        source_sha,
        bound,
    )


def _bindings(
    bundle: orchestrator.FormalOfflineBundle,
    *,
    nested_fold_manifest: Mapping[str, Any],
    nested_fold_sha256: str,
    source_fold_sha256: str,
) -> FormalExecutionBindings:
    execution = bundle.execution_manifest
    source = bundle.source_manifest
    panel = bundle.panel_manifest
    if execution.get("backend") != {
        "module": orchestrator.CANONICAL_BACKEND_MODULE,
        "function": orchestrator.CANONICAL_BACKEND_FUNCTION,
        "custom_evaluator_allowed": False,
    }:
        raise OfflineRepeatedPolicyBackendError("custom or alternate evaluator is forbidden")
    if execution.get("execution_contract", {}).get("control") != "B0_CURRENT_EXACT":
        raise OfflineRepeatedPolicyBackendError("formal control drifted from exact B0")
    if execution.get("fold_manifest_sha256") != source_fold_sha256:
        raise OfflineRepeatedPolicyBackendError("execution fold binding drifted")
    if execution.get("nested_fold_manifest") != nested_fold_manifest:
        raise OfflineRepeatedPolicyBackendError("execution nested-fold manifest drifted")
    if execution.get("nested_fold_manifest_sha256") != nested_fold_sha256:
        raise OfflineRepeatedPolicyBackendError("execution nested-fold SHA256 drifted")
    owner = source.get("exact_current_owner_baseline")
    if isinstance(owner, Mapping) and owner.get("policy_sha256") != offline.ACTIVE_OWNER_POLICY_SHA256:
        raise OfflineRepeatedPolicyBackendError("source exact B0 identity drifted")
    values = FormalExecutionBindings(
        execution_manifest_sha256=_require_sha(
            execution.get("canonical_execution_manifest_sha256"),
            label="execution manifest SHA256",
        ),
        source_manifest_sha256=_require_sha(
            source.get("canonical_manifest_sha256"), label="source manifest SHA256"
        ),
        panel_manifest_sha256=_require_sha(
            panel.get("canonical_panel_manifest_sha256"), label="panel manifest SHA256"
        ),
        fold_manifest_sha256=source_fold_sha256,
        nested_fold_manifest_sha256=nested_fold_sha256,
        exact_owner_policy_sha256=_require_sha(
            panel.get("exact_current_owner_policy_sha256"), label="exact B0 policy SHA256"
        ),
        exact_owner_predicate_bundle_sha256=_require_sha(
            panel.get("exact_current_predicate_bundle_sha256"),
            label="exact B0 predicate SHA256",
        ),
        exact_owner_private_config_sha256=_require_sha(
            panel.get("exact_current_private_config_sha256"),
            label="exact B0 config SHA256",
        ),
    )
    if values.exact_owner_policy_sha256 != offline.ACTIVE_OWNER_POLICY_SHA256:
        raise OfflineRepeatedPolicyBackendError("panel exact B0 policy drifted")
    if values.exact_owner_predicate_bundle_sha256 != offline.ACTIVE_PREDICATE_BUNDLE_SHA256:
        raise OfflineRepeatedPolicyBackendError("panel exact B0 predicate bundle drifted")
    if values.exact_owner_private_config_sha256 != offline.ACTIVE_PRIVATE_CONFIG_SHA256:
        raise OfflineRepeatedPolicyBackendError("panel exact B0 config drifted")
    if execution.get("canonical_execution_manifest_sha256") != _document_sha256(
        execution, "canonical_execution_manifest_sha256"
    ):
        raise OfflineRepeatedPolicyBackendError("execution manifest canonical hash drifted")
    if panel.get("canonical_panel_manifest_sha256") != _document_sha256(
        panel, "canonical_panel_manifest_sha256"
    ):
        raise OfflineRepeatedPolicyBackendError("panel manifest canonical hash drifted")
    return values


def load_outcome_blind_mechanics(
    bundle: orchestrator.FormalOfflineBundle,
) -> OutcomeBlindMechanics:
    """Load the five mechanics tables and independently revalidate their identity."""

    if not isinstance(bundle, orchestrator.FormalOfflineBundle):
        raise TypeError("formal backend accepts only FormalOfflineBundle")
    if set(bundle.panel_files) != set(_PANEL_ROLES):
        raise OfflineRepeatedPolicyBackendError("formal mechanics file census drifted")
    manifest_files = bundle.panel_manifest.get("files")
    if not isinstance(manifest_files, Mapping):
        raise OfflineRepeatedPolicyBackendError("panel file bindings are missing")
    forbidden_roles = set(manifest_files) & _FORBIDDEN_FILE_ROLES
    if forbidden_roles:
        raise OfflineRepeatedPolicyBackendError(
            f"pre-injected economic files are forbidden: {sorted(forbidden_roles)}"
        )
    if set(manifest_files) != set(_PANEL_ROLES):
        raise OfflineRepeatedPolicyBackendError("panel manifest file census drifted")
    selected_days = tuple(_normalize_day(day) for day in bundle.source_manifest.get("selected_days", ()))
    if len(selected_days) != offline.REQUIRED_DAYS or selected_days != tuple(sorted(set(selected_days))):
        raise OfflineRepeatedPolicyBackendError("formal panel must bind 30 chronological days")
    if tuple(bundle.panel_manifest.get("selected_days") or ()) != selected_days:
        raise OfflineRepeatedPolicyBackendError("panel/source day identity drifted")
    folds, source_fold_sha, nested_fold_manifest = _build_fold_manifest(
        bundle.source_manifest,
        selected_days,
    )
    bindings = _bindings(
        bundle,
        nested_fold_manifest=nested_fold_manifest,
        nested_fold_sha256=folds.manifest_sha256,
        source_fold_sha256=source_fold_sha,
    )
    loaded: dict[str, pd.DataFrame] = {}
    file_hashes: dict[str, str] = {}
    for role in _PANEL_ROLES:
        binding = manifest_files[role]
        if not isinstance(binding, Mapping):
            raise OfflineRepeatedPolicyBackendError(f"panel {role} binding is malformed")
        path = bundle.panel_files[role].expanduser().resolve()
        loaded[role] = _verify_bound_panel_file(role, path, binding)
        file_hashes[role] = str(binding["sha256"])

    metadata = _index_panel_table(loaded["metadata"], role="metadata", selected_days=selected_days)
    missing_metadata = set(nested.REQUIRED_METADATA_COLUMNS) - set(metadata.columns)
    if missing_metadata:
        raise OfflineRepeatedPolicyBackendError(
            f"metadata columns are missing: {sorted(missing_metadata)}"
        )
    metadata["side"] = metadata["side"].map(_normalize_side)
    if set(metadata["side"]) != {"BUY", "SELL"}:
        raise OfflineRepeatedPolicyBackendError("formal mechanics must preserve BUY and SELL")
    if set(metadata["panel_role"].astype(str)) != {offline.PANEL_ROLE}:
        raise OfflineRepeatedPolicyBackendError("mechanics panel role drifted")

    indexed: dict[str, pd.DataFrame] = {"metadata": metadata}
    for role in _PANEL_ROLES[1:]:
        rows = _index_panel_table(loaded[role], role=role, selected_days=selected_days)
        if not rows.index.equals(metadata.index):
            raise OfflineRepeatedPolicyBackendError(f"panel {role} row index drifted")
        if not rows["utc_day"].equals(metadata["utc_day"]):
            raise OfflineRepeatedPolicyBackendError(f"panel {role} row-day identity drifted")
        if "side" in rows:
            rows["side"] = rows["side"].map(_normalize_side)
            if not rows["side"].equals(metadata["side"]):
                raise OfflineRepeatedPolicyBackendError(f"panel {role} side identity drifted")
        indexed[role] = rows

    boolean_columns = [
        column
        for column in indexed["boolean_features"].columns
        if column not in {*_INDEX_COLUMNS, "side"}
    ]
    continuous_columns = [
        column
        for column in indexed["continuous_features"].columns
        if column not in {*_INDEX_COLUMNS, "side"}
    ]
    if not boolean_columns or not continuous_columns:
        raise OfflineRepeatedPolicyBackendError("formal feature blocks are empty")
    boolean = indexed["boolean_features"].loc[:, boolean_columns]
    if not np.isin(boolean.to_numpy(copy=False), (-1, 0, 1)).all():
        raise OfflineRepeatedPolicyBackendError("Boolean features are not three-valued")
    continuous = indexed["continuous_features"].loc[:, continuous_columns]
    if any(not pd.api.types.is_numeric_dtype(dtype) for dtype in continuous.dtypes):
        raise OfflineRepeatedPolicyBackendError("continuous features must be numeric")

    owner_rows = indexed["exact_owner_actions"]
    if "exact_owner_action" not in owner_rows:
        raise OfflineRepeatedPolicyBackendError("exact owner action column is missing")
    owner_actions = owner_rows["exact_owner_action"].astype(str).rename("exact_owner_action")
    buy_actions = set(owner_actions.loc[metadata["side"] == "BUY"])
    sell_actions = set(owner_actions.loc[metadata["side"] == "SELL"])
    if not buy_actions <= _BUY_OWNER_ACTIONS or not sell_actions <= _SELL_OWNER_ACTIONS:
        raise OfflineRepeatedPolicyBackendError("row-wise exact B0 action vocabulary drifted")

    replay_inputs = indexed["replay_inputs"]
    replay_inputs = replay_inputs.copy()
    for target, source in REPLAY_METADATA_DIRECT_BINDINGS.items():
        if source not in metadata:
            continue
        expected = metadata[source]
        if target == "side":
            expected = expected.map(_normalize_side)
        if target in replay_inputs:
            observed = replay_inputs[target]
            if target == "side":
                observed = observed.map(_normalize_side)
            if not observed.equals(expected):
                raise OfflineRepeatedPolicyBackendError(
                    f"replay input {target} drifted from admitted metadata"
                )
        else:
            replay_inputs[target] = expected
    if (
        "fill_visible_ts_ms" not in replay_inputs
        and REPLAY_FILL_VISIBLE_MS_SOURCE in metadata
    ):
        visible_ns = metadata[REPLAY_FILL_VISIBLE_MS_SOURCE].astype("int64")
        if (visible_ns % 1_000_000).ne(0).any():
            raise OfflineRepeatedPolicyBackendError(
                "fill-visible nanoseconds are not exactly millisecond aligned"
            )
        replay_inputs["fill_visible_ts_ms"] = visible_ns // 1_000_000
    if "side" not in replay_inputs:
        raise OfflineRepeatedPolicyBackendError("replay inputs cannot bind side")
    if "replay_input_receipt_sha256" not in replay_inputs:
        raise OfflineRepeatedPolicyBackendError(
            "replay inputs lack per-opportunity source receipts"
        )
    if not replay_inputs["replay_input_receipt_sha256"].astype(str).map(
        lambda value: _SHA_RE.fullmatch(value) is not None
    ).all():
        raise OfflineRepeatedPolicyBackendError("replay input receipt SHA256 is invalid")

    panel = nested.NestedOofPanel(
        metadata=metadata.loc[:, list(nested.REQUIRED_METADATA_COLUMNS)].copy(),
        boolean_features=boolean.copy(),
        continuous_features=continuous.copy(),
        exact_owner_actions=owner_actions.copy(),
    )
    panel.validate(
        active_days=selected_days,
        sides=("BUY", "SELL"),
        panel_role=offline.PANEL_ROLE,
        earliest_eligible_day=None,
    )
    mechanics_body = {
        "schema_version": f"{IDENTITY}.outcome_blind_mechanics_receipt.v1",
        "selected_days": list(selected_days),
        "file_sha256": file_hashes,
        "metadata_sha256": _frame_sha256(panel.metadata),
        "boolean_features_sha256": _frame_sha256(panel.boolean_features),
        "continuous_features_sha256": _frame_sha256(panel.continuous_features),
        "exact_owner_actions_sha256": _frame_sha256(panel.exact_owner_actions),
        "replay_inputs_sha256": _frame_sha256(replay_inputs),
        "bindings": bindings.payload(),
        "economic_outcomes_present": False,
    }
    return OutcomeBlindMechanics(
        panel=panel,
        replay_inputs=replay_inputs.copy(),
        selected_days=selected_days,
        fold_manifest=folds,
        bindings=bindings,
        file_sha256=file_hashes,
        mechanics_receipt_sha256=_canonical_sha256(mechanics_body),
    )


def _validate_adapter_shape(adapter: Any) -> CanonicalReplayAdapter:
    if getattr(adapter, "identity", None) != CANONICAL_REPLAY_ADAPTER_IDENTITY:
        raise OfflineRepeatedPolicyBackendError("canonical replay adapter identity drifted")
    _require_sha(getattr(adapter, "artifact_sha256", None), label="replay adapter artifact SHA256")
    for method in (
        "preflight_formal_panel_schema",
        "preflight_formal_economics",
        "build_search_contract",
        "generate_outer_train_one_shot_labels",
        "evaluate_repeated_policy",
    ):
        if not callable(getattr(adapter, method, None)):
            raise OfflineRepeatedPolicyBackendError(f"canonical replay adapter lacks {method}")
    return adapter


def _preflight_bound_panel_schema(
    bundle: orchestrator.FormalOfflineBundle,
    adapter: CanonicalReplayAdapter,
) -> Mapping[str, Any]:
    """Audit formal field coverage before any mechanics values are loaded."""

    manifest_files = bundle.panel_manifest.get("files")
    if not isinstance(manifest_files, Mapping):
        raise OfflineRepeatedPolicyBackendError("panel file bindings are missing")
    schemas: dict[str, Sequence[str]] = {}
    for role in ("metadata", "replay_inputs"):
        binding = manifest_files.get(role)
        if not isinstance(binding, Mapping):
            raise OfflineRepeatedPolicyBackendError(f"panel {role} binding is malformed")
        _rows, schema = _parquet_schema(bundle.panel_files[role])
        if binding.get("schema") != schema:
            raise OfflineRepeatedPolicyBackendError(f"panel {role} schema drifted")
        schemas[role] = tuple(str(value) for value in schema["columns"])
    result = adapter.preflight_formal_panel_schema(
        metadata_columns=schemas["metadata"],
        replay_input_columns=schemas["replay_inputs"],
    )
    if not isinstance(result, Mapping):
        raise OfflineRepeatedPolicyBackendError("panel-schema preflight returned a custom payload")
    expected_permissions = {
        "economic_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    if (
        result.get("identity") != adapter.identity
        or result.get("adapter_artifact_sha256") != adapter.artifact_sha256
        or result.get("permissions") != expected_permissions
    ):
        raise OfflineRepeatedPolicyBackendError("panel-schema preflight identity drifted")
    status = str(result.get("status", ""))
    if status not in {
        FORMAL_PANEL_SCHEMA_READY_STATUS,
        CANONICAL_FIELDS_BLOCKED_STATUS,
    }:
        raise OfflineRepeatedPolicyBackendError("panel-schema preflight status drifted")
    missing = result.get("missing_canonical_fields")
    if not isinstance(missing, list):
        raise OfflineRepeatedPolicyBackendError("panel-schema preflight lacks field census")
    if status == CANONICAL_FIELDS_BLOCKED_STATUS and not missing:
        raise OfflineRepeatedPolicyBackendError("blocked panel-schema preflight lacks blockers")
    if status == FORMAL_PANEL_SCHEMA_READY_STATUS and missing:
        raise OfflineRepeatedPolicyBackendError("ready panel-schema preflight carries blockers")
    return dict(result)


def _preflight_adapter(
    mechanics: OutcomeBlindMechanics,
    adapter: CanonicalReplayAdapter,
) -> Mapping[str, Any]:
    result = adapter.preflight_formal_economics(mechanics)
    if not isinstance(result, Mapping):
        raise OfflineRepeatedPolicyBackendError("adapter preflight returned a custom payload")
    expected_permissions = {
        "economic_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    if (
        result.get("identity") != adapter.identity
        or result.get("adapter_artifact_sha256") != adapter.artifact_sha256
        or result.get("permissions") != expected_permissions
    ):
        raise OfflineRepeatedPolicyBackendError("adapter preflight identity drifted")
    status = str(result.get("status", ""))
    if status not in {
        MECHANICS_READY_STATUS,
        BLOCKED_STATUS,
        CANONICAL_FIELDS_BLOCKED_STATUS,
    }:
        raise OfflineRepeatedPolicyBackendError("adapter preflight status drifted")
    blockers = result.get("missing_canonical_fields")
    if status != MECHANICS_READY_STATUS:
        if not isinstance(blockers, list) or not blockers:
            raise OfflineRepeatedPolicyBackendError("blocked adapter preflight lacks blockers")
    elif blockers is not None and blockers != ():
        raise OfflineRepeatedPolicyBackendError("ready adapter preflight carries blockers")
    return dict(result)


def _load_canonical_replay_adapter() -> CanonicalReplayAdapter:
    try:
        module = importlib.import_module(CANONICAL_REPLAY_ADAPTER_MODULE)
    except ModuleNotFoundError as exc:
        raise OfflineRepeatedPolicyBackendError(
            "fixed historical replay adapter is not implemented"
        ) from exc
    factory = getattr(module, CANONICAL_REPLAY_ADAPTER_FACTORY, None)
    if not callable(factory):
        raise OfflineRepeatedPolicyBackendError("fixed replay adapter factory is unavailable")
    module_path_value = getattr(module, "__file__", None)
    if not module_path_value:
        raise OfflineRepeatedPolicyBackendError("fixed replay adapter has no source artifact")
    module_path = Path(module_path_value).expanduser().resolve()
    adapter = _validate_adapter_shape(factory())
    if type(adapter).__module__ != CANONICAL_REPLAY_ADAPTER_MODULE:
        raise OfflineRepeatedPolicyBackendError("custom replay adapter implementation is forbidden")
    if adapter.artifact_sha256 != _file_sha256(module_path):
        raise OfflineRepeatedPolicyBackendError("fixed replay adapter source hash drifted")
    return adapter


def build_outer_train_label_replay_receipt(
    request: CanonicalOuterTrainReplayRequest,
    *,
    adapter_identity: str,
    adapter_artifact_sha256: str,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": LABEL_REPLAY_RECEIPT_SCHEMA,
        "adapter_identity": adapter_identity,
        "adapter_artifact_sha256": adapter_artifact_sha256,
        "label_request_sha256": request.label_request.request_sha256,
        "side": request.label_request.side,
        "outer_fold_id": request.label_request.outer_fold_id,
        "train_days": list(request.label_request.train_days),
        "row_sha256": request.label_request.row_sha256,
        "replay_input_sha256": request.replay_input_sha256,
        "bindings": request.bindings.payload(),
        "outer_train_only": True,
        "outer_test_rows_read": 0,
        "precomputed_economic_file_used": False,
        "one_shot_effect_aggregation_used": False,
    }
    payload["receipt_sha256"] = _document_sha256(payload, "receipt_sha256")
    return payload


def build_sequential_replay_receipt(
    request: CanonicalSequentialReplayRequest,
    *,
    adapter_identity: str,
    adapter_artifact_sha256: str,
) -> dict[str, Any]:
    evaluation = request.evaluation_request
    payload: dict[str, Any] = {
        "schema_version": SEQUENTIAL_REPLAY_RECEIPT_SCHEMA,
        "adapter_identity": adapter_identity,
        "adapter_artifact_sha256": adapter_artifact_sha256,
        "side": evaluation.side,
        "days": list(evaluation.days),
        "fold_id": evaluation.fold_id,
        "stage": evaluation.stage,
        "candidate_policy_sha256": evaluation.candidate.expected_executed_policy_sha256,
        "exact_owner_policy_sha256": offline.ACTIVE_OWNER_POLICY_SHA256,
        "replay_input_sha256": request.replay_input_sha256,
        "bindings": request.bindings.payload(),
        "repeated_sequential_policy": True,
        "one_shot_effect_aggregation_used": False,
        "custom_evaluator_used": False,
    }
    payload["receipt_sha256"] = _document_sha256(payload, "receipt_sha256")
    return payload


class CanonicalFoldScopedLabelProvider:
    """Generate labels from replay inputs for exactly one purged outer train."""

    def __init__(self, mechanics: OutcomeBlindMechanics, adapter: CanonicalReplayAdapter) -> None:
        self.mechanics = mechanics
        self.adapter = _validate_adapter_shape(adapter)
        self.receipts: list[Mapping[str, Any]] = []
        self._folds = {
            str(row["fold_id"]): row for row in mechanics.fold_manifest.outer_folds
        }

    def __call__(
        self,
        request: nested.FoldScopedOneShotLabelRequest,
    ) -> nested.FoldScopedOneShotLabelBatch:
        if not isinstance(request, nested.FoldScopedOneShotLabelRequest):
            raise OfflineRepeatedPolicyBackendError("label provider received a custom request")
        fold = self._folds.get(request.outer_fold_id)
        if fold is None or tuple(fold["train_days"]) != tuple(request.train_days):
            raise OfflineRepeatedPolicyBackendError("label request outer-train fold drifted")
        side = _normalize_side(request.side)
        if tuple(request.duration_vocabulary) != tuple(duration_vocabulary(side)):
            raise OfflineRepeatedPolicyBackendError("label request duration vocabulary drifted")
        index = pd.Index(request.row_ids, name=self.mechanics.panel.metadata.index.name)
        if index.has_duplicates or not index.isin(self.mechanics.panel.metadata.index).all():
            raise OfflineRepeatedPolicyBackendError("label request row identity is invalid")
        metadata = self.mechanics.panel.metadata.loc[index]
        if set(metadata["side"].map(_normalize_side)) != {side}:
            raise OfflineRepeatedPolicyBackendError("label request pooled sides")
        if set(metadata["utc_day"].map(_normalize_day)) - set(request.train_days):
            raise OfflineRepeatedPolicyBackendError("outer-test labels were requested")
        if set(metadata["utc_day"].map(_normalize_day)) & set(fold["test_days"]):
            raise OfflineRepeatedPolicyBackendError("outer-test labels were requested")
        expected_rows = tuple(str(value) for value in index)
        if request.row_sha256 != _canonical_sha256(list(expected_rows)):
            raise OfflineRepeatedPolicyBackendError("label request row hash drifted")
        mechanics_sha = _canonical_sha256(
            {
                "metadata_sha256": _frame_sha256(metadata),
                "boolean_features_sha256": _frame_sha256(
                    self.mechanics.panel.boolean_features.loc[index]
                ),
                "continuous_features_sha256": _frame_sha256(
                    self.mechanics.panel.continuous_features.loc[index]
                ),
                "exact_owner_actions_sha256": _frame_sha256(
                    self.mechanics.panel.exact_owner_actions.loc[index]
                ),
            }
        )
        request_body = {
            "schema": f"{nested.IDENTITY}.fold_scoped_one_shot_label_request.v1",
            "side": side,
            "outer_fold_id": request.outer_fold_id,
            "train_days": list(request.train_days),
            "row_ids": list(expected_rows),
            "row_sha256": request.row_sha256,
            "mechanics_sha256": mechanics_sha,
            "duration_vocabulary": list(request.duration_vocabulary),
        }
        if request.mechanics_sha256 != mechanics_sha or request.request_sha256 != _canonical_sha256(
            request_body
        ):
            raise OfflineRepeatedPolicyBackendError("label request mechanics binding drifted")
        replay_inputs = self.mechanics.replay_inputs.loc[index].copy()
        adapter_request = CanonicalOuterTrainReplayRequest(
            label_request=request,
            replay_input_sha256=_frame_sha256(replay_inputs),
            bindings=self.mechanics.bindings,
        )
        result = self.adapter.generate_outer_train_one_shot_labels(
            adapter_request, replay_inputs
        )
        if not isinstance(result, CanonicalOneShotReplayResult):
            raise OfflineRepeatedPolicyBackendError("label replay returned a custom payload")
        expected_receipt = build_outer_train_label_replay_receipt(
            adapter_request,
            adapter_identity=self.adapter.identity,
            adapter_artifact_sha256=self.adapter.artifact_sha256,
        )
        if dict(result.receipt) != expected_receipt:
            raise OfflineRepeatedPolicyBackendError("outer-train label replay receipt drifted")
        batch = nested.bind_fold_scoped_one_shot_labels(
            request,
            outcomes=result.outcomes,
            supported=result.supported,
            provider_identity=self.adapter.identity,
            provider_artifact_sha256=self.adapter.artifact_sha256,
        )
        self.receipts.append(
            {
                **expected_receipt,
                "label_payload_sha256": batch.label_payload_sha256,
                "label_receipt_sha256": batch.receipt_sha256,
            }
        )
        return batch


class CanonicalSequentialEvaluator:
    """Evaluate only paired repeated policies through the fixed replay adapter."""

    def __init__(self, mechanics: OutcomeBlindMechanics, adapter: CanonicalReplayAdapter) -> None:
        self.mechanics = mechanics
        self.adapter = _validate_adapter_shape(adapter)
        self.receipts: list[Mapping[str, Any]] = []

    def __call__(self, request: nested.EvaluationRequest) -> pd.DataFrame:
        if not isinstance(request, nested.EvaluationRequest):
            raise OfflineRepeatedPolicyBackendError("sequential evaluator received a custom request")
        side = _normalize_side(request.side)
        days = tuple(_normalize_day(day) for day in request.days)
        if not days or set(days) - set(self.mechanics.selected_days):
            raise OfflineRepeatedPolicyBackendError("sequential request escaped admitted days")
        mask = self.mechanics.replay_inputs["utc_day"].isin(days) & (
            self.mechanics.replay_inputs["side"] == side
        )
        replay_inputs = self.mechanics.replay_inputs.loc[mask].copy()
        if replay_inputs.empty or set(replay_inputs["utc_day"]) != set(days):
            raise OfflineRepeatedPolicyBackendError("sequential replay inputs are incomplete")
        adapter_request = CanonicalSequentialReplayRequest(
            evaluation_request=request,
            replay_input_sha256=_frame_sha256(replay_inputs),
            bindings=self.mechanics.bindings,
        )
        result = self.adapter.evaluate_repeated_policy(adapter_request, replay_inputs)
        if not isinstance(result, CanonicalSequentialReplayResult):
            raise OfflineRepeatedPolicyBackendError("sequential replay returned a custom payload")
        expected_receipt = build_sequential_replay_receipt(
            adapter_request,
            adapter_identity=self.adapter.identity,
            adapter_artifact_sha256=self.adapter.artifact_sha256,
        )
        if dict(result.receipt) != expected_receipt:
            raise OfflineRepeatedPolicyBackendError("sequential replay receipt drifted")
        if not isinstance(result.rows, pd.DataFrame):
            raise OfflineRepeatedPolicyBackendError("sequential replay rows are not a DataFrame")
        required_bindings = {
            "sequential_batch_receipt_sha256": expected_receipt["receipt_sha256"],
            "execution_manifest_sha256": self.mechanics.bindings.execution_manifest_sha256,
            "source_manifest_sha256": self.mechanics.bindings.source_manifest_sha256,
            "panel_manifest_sha256": self.mechanics.bindings.panel_manifest_sha256,
            "fold_manifest_sha256": self.mechanics.bindings.fold_manifest_sha256,
        }
        for column, expected in required_bindings.items():
            if column not in result.rows or set(result.rows[column].astype(str)) != {expected}:
                raise OfflineRepeatedPolicyBackendError(
                    f"sequential replay row binding drifted: {column}"
                )
        validated = nested._validate_evaluation(result.rows, request)
        if validated["one_shot_effect_aggregation_used"].astype(bool).any():
            raise OfflineRepeatedPolicyBackendError("one-shot aggregation is forbidden")
        self.receipts.append(expected_receipt)
        return validated


def _blocked_bundle_result(
    bundle: orchestrator.FormalOfflineBundle,
    *,
    blocker: str,
    status: str = BLOCKED_STATUS,
    missing_canonical_fields: Sequence[str] = (),
) -> dict[str, Any]:
    """Build a pre-mechanics blocker from the fully revalidated formal bundle."""

    execution = bundle.execution_manifest
    source = bundle.source_manifest
    panel = bundle.panel_manifest
    result: dict[str, Any] = {
        "schema_version": FORMAL_RESULT_SCHEMA,
        "identity": IDENTITY,
        "status": status,
        "blocker": blocker,
        "execution_manifest_sha256": execution["canonical_execution_manifest_sha256"],
        "source_manifest_sha256": source["canonical_manifest_sha256"],
        "panel_manifest_sha256": panel["canonical_panel_manifest_sha256"],
        "fold_manifest_sha256": execution["fold_manifest_sha256"],
        "nested_fold_manifest_sha256": execution["nested_fold_manifest_sha256"],
        "exact_owner_policy_sha256": panel["exact_current_owner_policy_sha256"],
        "exact_owner_predicate_bundle_sha256": panel[
            "exact_current_predicate_bundle_sha256"
        ],
        "exact_owner_private_config_sha256": panel[
            "exact_current_private_config_sha256"
        ],
        "repeated_sequential_policy": False,
        "one_shot_effect_aggregation_used": False,
        "economic_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "permissions": {
            "economic_outcomes_read": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    if missing_canonical_fields:
        result["missing_canonical_fields"] = list(missing_canonical_fields)
    return result


def _blocked_result(
    mechanics: OutcomeBlindMechanics,
    *,
    blocker: str,
    status: str = BLOCKED_STATUS,
    missing_canonical_fields: Sequence[str] = (),
) -> dict[str, Any]:
    result = {
        "schema_version": FORMAL_RESULT_SCHEMA,
        "identity": IDENTITY,
        "status": status,
        "blocker": blocker,
        "execution_manifest_sha256": mechanics.bindings.execution_manifest_sha256,
        "source_manifest_sha256": mechanics.bindings.source_manifest_sha256,
        "panel_manifest_sha256": mechanics.bindings.panel_manifest_sha256,
        "fold_manifest_sha256": mechanics.bindings.fold_manifest_sha256,
        "nested_fold_manifest_sha256": mechanics.bindings.nested_fold_manifest_sha256,
        "mechanics_receipt_sha256": mechanics.mechanics_receipt_sha256,
        "exact_owner_policy_sha256": mechanics.bindings.exact_owner_policy_sha256,
        "exact_owner_predicate_bundle_sha256": (
            mechanics.bindings.exact_owner_predicate_bundle_sha256
        ),
        "exact_owner_private_config_sha256": (
            mechanics.bindings.exact_owner_private_config_sha256
        ),
        "repeated_sequential_policy": False,
        "one_shot_effect_aggregation_used": False,
        "economic_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "permissions": {
            "economic_outcomes_read": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    if missing_canonical_fields:
        result["missing_canonical_fields"] = list(missing_canonical_fields)
    return result


def _completed_result(
    mechanics: OutcomeBlindMechanics,
    *,
    adapter: CanonicalReplayAdapter,
    result: nested.NestedOofExecutionResult,
    label_receipts: Sequence[Mapping[str, Any]],
    sequential_receipts: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": FORMAL_RESULT_SCHEMA,
        "identity": IDENTITY,
        "status": "learning_algorithm_nested_oof_complete_no_action_or_live_authority",
        "execution_manifest_sha256": mechanics.bindings.execution_manifest_sha256,
        "source_manifest_sha256": mechanics.bindings.source_manifest_sha256,
        "panel_manifest_sha256": mechanics.bindings.panel_manifest_sha256,
        "fold_manifest_sha256": mechanics.bindings.fold_manifest_sha256,
        "nested_fold_manifest_sha256": mechanics.bindings.nested_fold_manifest_sha256,
        "mechanics_receipt_sha256": mechanics.mechanics_receipt_sha256,
        "exact_owner_policy_sha256": mechanics.bindings.exact_owner_policy_sha256,
        "exact_owner_predicate_bundle_sha256": (
            mechanics.bindings.exact_owner_predicate_bundle_sha256
        ),
        "exact_owner_private_config_sha256": (
            mechanics.bindings.exact_owner_private_config_sha256
        ),
        "canonical_replay_adapter_identity": adapter.identity,
        "canonical_replay_adapter_sha256": adapter.artifact_sha256,
        "label_replay_receipts": list(label_receipts),
        "sequential_replay_receipts": list(sequential_receipts),
        "nested_oof_report": result.report(),
        "repeated_sequential_policy": True,
        "one_shot_effect_aggregation_used": False,
        "economic_outcomes_read": True,
        "validation_read": False,
        "sealed_holdout_read": False,
        "permissions": {
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }


def run_canonical_offline_economics(
    execution_manifest_path: Path,
) -> Mapping[str, Any]:
    """Strictly reload the sole formal manifest, then run the fixed backend."""

    if not isinstance(execution_manifest_path, Path):
        raise TypeError("formal backend accepts only an execution-manifest Path")
    bundle = orchestrator.load_formal_offline_bundle(execution_manifest_path)
    try:
        adapter = _load_canonical_replay_adapter()
    except OfflineRepeatedPolicyBackendError as exc:
        raise OfflineRepeatedPolicyBackendIncomplete(
            _blocked_bundle_result(bundle, blocker=str(exc))
        ) from exc
    schema_preflight = _preflight_bound_panel_schema(bundle, adapter)
    if schema_preflight["status"] != FORMAL_PANEL_SCHEMA_READY_STATUS:
        missing = tuple(
            str(value) for value in schema_preflight["missing_canonical_fields"]
        )
        result = _blocked_bundle_result(
            bundle,
            blocker=",".join(missing),
            status=CANONICAL_FIELDS_BLOCKED_STATUS,
            missing_canonical_fields=missing,
        )
        result["adapter_preflight"] = schema_preflight
        raise OfflineRepeatedPolicyBackendIncomplete(result)
    mechanics = load_outcome_blind_mechanics(bundle)
    preflight = _preflight_adapter(mechanics, adapter)
    if preflight["status"] != MECHANICS_READY_STATUS:
        missing = tuple(str(value) for value in preflight["missing_canonical_fields"])
        raise OfflineRepeatedPolicyBackendIncomplete(
            _blocked_result(
                mechanics,
                blocker=",".join(missing),
                status=(
                    CANONICAL_FIELDS_BLOCKED_STATUS
                    if preflight["status"] == CANONICAL_FIELDS_BLOCKED_STATUS
                    else BLOCKED_STATUS
                ),
                missing_canonical_fields=missing,
            )
        )
    ladder, continuous = adapter.build_search_contract(mechanics)
    provider = CanonicalFoldScopedLabelProvider(mechanics, adapter)
    evaluator = CanonicalSequentialEvaluator(mechanics, adapter)
    result = nested.run_nested_chronological_oof(
        mechanics.panel,
        fold_manifest=mechanics.fold_manifest,
        ladder=ladder,
        continuous=continuous,
        evaluator=evaluator,
        label_provider=provider,
        config=nested.NestedOofConfig(
            sides=("BUY", "SELL"),
            panel_role=offline.PANEL_ROLE,
            earliest_eligible_day=None,
        ),
    )
    return _completed_result(
        mechanics,
        adapter=adapter,
        result=result,
        label_receipts=provider.receipts,
        sequential_receipts=evaluator.receipts,
    )


def preflight_canonical_offline_economics(
    execution_manifest_path: Path,
) -> Mapping[str, Any]:
    """Validate formal identity and replay availability without reading economics."""

    if not isinstance(execution_manifest_path, Path):
        raise TypeError("formal preflight accepts only an execution-manifest Path")
    bundle = orchestrator.load_formal_offline_bundle(execution_manifest_path)
    try:
        adapter = _load_canonical_replay_adapter()
    except OfflineRepeatedPolicyBackendError as exc:
        return _blocked_bundle_result(bundle, blocker=str(exc))
    schema_preflight = _preflight_bound_panel_schema(bundle, adapter)
    if schema_preflight["status"] != FORMAL_PANEL_SCHEMA_READY_STATUS:
        missing = tuple(
            str(value) for value in schema_preflight["missing_canonical_fields"]
        )
        result = _blocked_bundle_result(
            bundle,
            blocker=",".join(missing),
            status=CANONICAL_FIELDS_BLOCKED_STATUS,
            missing_canonical_fields=missing,
        )
        result["adapter_preflight"] = schema_preflight
        return result
    mechanics = load_outcome_blind_mechanics(bundle)
    adapter_preflight = _preflight_adapter(mechanics, adapter)
    if adapter_preflight["status"] != MECHANICS_READY_STATUS:
        missing = tuple(
            str(value) for value in adapter_preflight["missing_canonical_fields"]
        )
        result = _blocked_result(
            mechanics,
            blocker=",".join(missing),
            status=(
                CANONICAL_FIELDS_BLOCKED_STATUS
                if adapter_preflight["status"] == CANONICAL_FIELDS_BLOCKED_STATUS
                else BLOCKED_STATUS
            ),
            missing_canonical_fields=missing,
        )
        result["adapter_preflight"] = adapter_preflight
        return result
    return {
        "schema_version": FORMAL_RESULT_SCHEMA,
        "identity": IDENTITY,
        "status": MECHANICS_READY_STATUS,
        "execution_manifest_sha256": mechanics.bindings.execution_manifest_sha256,
        "source_manifest_sha256": mechanics.bindings.source_manifest_sha256,
        "panel_manifest_sha256": mechanics.bindings.panel_manifest_sha256,
        "fold_manifest_sha256": mechanics.bindings.fold_manifest_sha256,
        "nested_fold_manifest_sha256": mechanics.bindings.nested_fold_manifest_sha256,
        "exact_owner_policy_sha256": mechanics.bindings.exact_owner_policy_sha256,
        "adapter_preflight": adapter_preflight,
        "repeated_sequential_policy": False,
        "one_shot_effect_aggregation_used": False,
        "economic_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "permissions": {
            "economic_outcomes_read": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }


__all__ = [
    "BLOCKED_STATUS",
    "CANONICAL_FIELDS_BLOCKED_STATUS",
    "CANONICAL_REPLAY_ADAPTER_IDENTITY",
    "CanonicalFoldScopedLabelProvider",
    "CanonicalOneShotReplayResult",
    "CanonicalOuterTrainReplayRequest",
    "CanonicalSequentialEvaluator",
    "CanonicalSequentialReplayRequest",
    "CanonicalSequentialReplayResult",
    "FormalExecutionBindings",
    "IDENTITY",
    "OfflineRepeatedPolicyBackendError",
    "OfflineRepeatedPolicyBackendIncomplete",
    "OutcomeBlindMechanics",
    "build_outer_train_label_replay_receipt",
    "build_sequential_replay_receipt",
    "preflight_canonical_offline_economics",
    "run_canonical_offline_economics",
]
