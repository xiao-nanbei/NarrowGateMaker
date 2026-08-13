#!/usr/bin/env python3
"""Build outcome-blind, source-bound F05 cooldown mechanics rows.

The builder owns manifest validation, exact-owner evaluation, stable row
identity, and atomic per-day admission.  A separate fixed adapter owns the true
market-window and causal-v12 overlay assembly; both layers fail closed instead
of reusing an old denominator or manufacturing replay rows.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import importlib
import json
import math
import os
import re
import shutil
import tempfile
import uuid
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

import pyarrow as pa
import pyarrow.parquet as pq

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_mechanics_v1 as mechanics,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_native_observation_batch_v1 as observation_batch,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_v1 as offline,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_runtime_policy as runtime_policy,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_features import (
    M0_REQUIRED_FIELDS,
    CausalWindowObservation,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_native_observation_cache import (
    NativeObservationCacheError,
    open_admitted_observation_cache,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_replay_emitter import (
    CooldownV2ReplayEmitter,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_snapshot import (
    HISTORICAL_EXCHANGE_EVENT_PROFILE,
    IDENTITY_HASH_FIELDS,
    CooldownAssignmentSnapshotV2,
)

LEGACY_PANEL_IDENTITY = f"{offline.IDENTITY}.offline_panel_builder_v1"
IDENTITY = f"{offline.IDENTITY}.offline_sequential_panel_v2"
SCHEMA_VERSION = f"{IDENTITY}.manifest.v1"
DAY_SCHEMA_VERSION = f"{IDENTITY}.day_manifest.v1"
ADAPTER_RESULT_SCHEMA = f"{IDENTITY}.adapter_result.v1"
SEQUENTIAL_REPLAY_INPUT_IDENTITY = (
    "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
    "offline_sequential_replay_input_v2"
)
OPPORTUNITY_ID_SCHEMA_VERSION = f"{LEGACY_PANEL_IDENTITY}.opportunity_id.v1"
PANEL_ROLE = "family_specific_unconsumed_historical_development"
QUEUE_IDENTITY = "modeled_queue_with_same_millisecond_ambiguity_censoring"
SAME_MILLISECOND_AMBIGUITY_POLICY = "censor"
REPLAY_ENGINE = "python"
CANONICAL_ADAPTER_IDENTITY = f"{offline.IDENTITY}.offline_b0_mechanics_adapter_v2"
CANONICAL_ADAPTER_MODULE = (
    "research.families.f05_fill_quality_quote_ev.audit."
    "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
    "offline_b0_mechanics_adapter_v1"
)
CANONICAL_ADAPTER_FACTORY = "build_canonical_b0_mechanics_adapter"
BLOCKED_ADAPTER_STATUS = "blocked_canonical_b0_mechanics_adapter_unavailable"
EXPECTED_NATIVE_OBSERVATION_SCHEMA = f"{observation_batch.IDENTITY}.manifest.v2"
EXPECTED_OBSERVATION_CONTEXT_DAY_COUNT = 34
EXPECTED_CONTINUATION_ONLY_DAYS = (
    "2026-06-29",
    "2026-07-03",
    "2026-07-16",
    "2026-08-06",
)
FORMAL_DAY_REPLAY_WORKERS = 6
FORMAL_OPPORTUNITY_COUNT = 3_516
PORTABLE_BINDING_FILENAME = "portable_replay_binding_v2.json"
SEQUENTIAL_CACHE_DIRECTORY = (
    "f05_full_multiscale_successor_offline_sequential_replay_cache_v2"
)

PANEL_ROLES = mechanics.PANEL_FILE_ROLES
OWNER_ACTIONS = mechanics.OWNER_ACTION_VOCABULARY
_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_FEATURE_METADATA_FIELDS = frozenset(
    {
        "schema_version",
        "identity",
        "feature_block",
        "base_window_width_ns",
        "maximum_explicit_window_count",
        "last_window_right_ts_ns",
        "feature_ready_ts_ns",
        "decision_ts_ns",
        "market_generation",
        "depth_generation",
        "window_count",
        "gap_window_count",
        "warmup_admitted",
        "warmup_identity",
        "support_valid",
        "channel_support_valid",
    }
)
_FORBIDDEN_FEATURE_PREFIXES = ("label_",)
_FORBIDDEN_ADAPTER_KEYS = (
    "pnl",
    "profit",
    "reward",
    "markout",
    "terminal_value",
    "closed_campaign_value",
    "economic_outcome",
    "action_outcome",
    "candidate_action",
)
_ADAPTER_RESULT_FIELDS = frozenset(
    {
        "schema_version",
        "identity",
        "utc_day",
        "replay_engine",
        "queue_identity",
        "same_millisecond_ambiguity_policy",
        "exposure_fill_scope",
        "current_owner_b0_executed",
        "candidate_actions_generated",
        "economic_outcomes_read",
        "labels_read",
        "snapshots_emitted",
        "market_window_identity_sha256",
        "model_overlay_identity_sha256",
        "latency_identity_sha256",
        "queue_random_identity_sha256",
        "replay_input_receipt_sha256",
        "assignment_mechanics",
    }
)


class OfflinePanelBuilderError(RuntimeError):
    """Raised when a panel-builder identity or mechanics invariant drifts."""


class OfflinePanelBuilderBlocked(OfflinePanelBuilderError):
    """Raised when validated inputs lack the fixed true-replay adapter."""

    status = BLOCKED_ADAPTER_STATUS


@dataclass(frozen=True, slots=True)
class OwnerArtifactPaths:
    policy: Path
    predicate_bundle: Path
    private_config: Path


@dataclass(frozen=True, slots=True)
class ValidatedPanelInputs:
    project_data_root: Path
    marketdata_root: Path
    source_manifest_path: Path
    source_manifest: Mapping[str, Any]
    book_view_root: Path
    book_view_manifest: Mapping[str, Any]
    native_observation_manifest_path: Path
    native_observation_manifest: Mapping[str, Any]
    native_observation_root: Path
    features_manifest_path: Path
    features_manifest: Mapping[str, Any]
    feature_files: Mapping[str, Path]
    owner_artifacts: OwnerArtifactPaths
    selected_days: tuple[str, ...]
    observation_context_days: tuple[str, ...]
    continuation_only_days: tuple[str, ...]
    replay_context_days: tuple[str, ...]
    input_binding_sha256: str


@dataclass(frozen=True, slots=True)
class DayMaterializationRequest:
    utc_day: str
    panel_role: str
    queue_identity: str
    same_millisecond_ambiguity_policy: str
    bbo_path: Path
    l2_path: Path
    features_path: Path
    source_manifest_path: Path
    book_view_manifest_path: Path
    features_manifest_path: Path
    private_config_path: Path
    native_observation_root: Path
    source_receipts: Mapping[str, str]
    input_binding_sha256: str


class B0MechanicsReplayAdapter(Protocol):
    """Fixed adapter boundary for true B0 market-window/overlay assembly."""

    identity: str

    def identity_hashes(self, request: DayMaterializationRequest) -> Mapping[str, str]: ...

    def run_day(
        self,
        request: DayMaterializationRequest,
        *,
        emitter: CooldownV2ReplayEmitter,
        evaluator: Any,
    ) -> Mapping[str, Any]: ...


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


def _load_json(path: Path, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OfflinePanelBuilderError(f"cannot load {label}: {path}") from exc
    if not isinstance(payload, dict):
        raise OfflinePanelBuilderError(f"{label} root must be an object")
    return payload


def _require_sha(value: Any, *, label: str) -> str:
    digest = str(value).strip().lower()
    if _SHA_RE.fullmatch(digest) is None:
        raise OfflinePanelBuilderError(f"{label} is not a lowercase SHA256")
    return digest


def _require_day(value: Any) -> str:
    day = str(value)
    if _DAY_RE.fullmatch(day) is None:
        raise OfflinePanelBuilderError(f"invalid UTC day: {value!r}")
    return day


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    temporary: str | None = None
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="ascii",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = handle.name
            json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if temporary is not None:
            Path(temporary).unlink(missing_ok=True)


def _parquet_binding(path: Path) -> dict[str, Any]:
    parquet = pq.ParquetFile(path)
    schema = parquet.schema_arrow
    return {
        "file": path.name,
        "sha256": _file_sha256(path),
        "size_bytes": int(path.stat().st_size),
        "rows": int(parquet.metadata.num_rows),
        "schema": {
            "columns": [str(name) for name in schema.names],
            "types": [str(schema.field(index).type) for index in range(len(schema))],
        },
    }


def _write_parquet(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        raise OfflinePanelBuilderError(f"cannot write empty mechanics role: {path.stem}")
    columns = tuple(sorted({str(key) for row in rows for key in row}))
    normalized = [{column: row.get(column) for column in columns} for row in rows]
    table = pa.Table.from_pylist(normalized)
    pq.write_table(table, path, compression="zstd")
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def _validate_owner_artifacts(paths: OwnerArtifactPaths) -> None:
    expected = {
        "policy": offline.ACTIVE_OWNER_POLICY_SHA256,
        "predicate_bundle": offline.ACTIVE_PREDICATE_BUNDLE_SHA256,
        "private_config": offline.ACTIVE_PRIVATE_CONFIG_SHA256,
    }
    for role, path in (
        ("policy", paths.policy),
        ("predicate_bundle", paths.predicate_bundle),
        ("private_config", paths.private_config),
    ):
        resolved = path.expanduser().resolve()
        if not resolved.is_file() or _file_sha256(resolved) != expected[role]:
            raise OfflinePanelBuilderError(f"exact current owner {role} hash drifted")
    evaluator = runtime_policy.load_runtime_policy(
        policy_path=paths.policy,
        predicate_bundle_path=paths.predicate_bundle,
        expected_policy_sha256=offline.ACTIVE_OWNER_POLICY_SHA256,
        expected_predicate_bundle_sha256=offline.ACTIVE_PREDICATE_BUNDLE_SHA256,
    )
    if not evaluator.binding_valid:
        raise OfflinePanelBuilderError(
            f"exact current owner evaluator failed binding: {evaluator.binding_error}"
        )
    if evaluator.policy_sha256 != offline.ACTIVE_OWNER_POLICY_SHA256:
        raise OfflinePanelBuilderError("exact owner evaluator policy identity drifted")
    if evaluator.predicate_bundle_sha256 != offline.ACTIVE_PREDICATE_BUNDLE_SHA256:
        raise OfflinePanelBuilderError("exact owner evaluator predicate identity drifted")


def _validate_features_only_manifest(
    path: Path,
    *,
    required_context_days: Sequence[str],
) -> tuple[dict[str, Any], dict[str, Path]]:
    manifest_path = path.expanduser().resolve()
    payload = _load_json(manifest_path, label="features-only manifest")
    if payload.get("labels_materialized") is not False:
        raise OfflinePanelBuilderError("features-only manifest materialized labels")
    if payload.get("config_sha256") != offline.ACTIVE_PRIVATE_CONFIG_SHA256:
        raise OfflinePanelBuilderError("features-only config hash drifted from current B0")
    feature_dag_sha = _require_sha(
        payload.get("feature_dag_sha256"), label="features-only Feature DAG"
    )
    if payload.get("derived_datasets") not in ([], None):
        raise OfflinePanelBuilderError("features-only manifest contains derived datasets")
    split = payload.get("split")
    daily = payload.get("daily_files")
    if not isinstance(split, Mapping) or set(split) != {"inference"}:
        raise OfflinePanelBuilderError("features-only split must be inference-only")
    if not isinstance(daily, list) or payload.get("daily_file_count") != len(daily):
        raise OfflinePanelBuilderError("features-only daily file census drifted")
    files: dict[str, Path] = {}
    digest = hashlib.sha256()
    for row in daily:
        if not isinstance(row, Mapping):
            raise OfflinePanelBuilderError("features-only daily binding is malformed")
        day = _require_day(row.get("day"))
        if day in files:
            raise OfflinePanelBuilderError(f"duplicate features-only day: {day}")
        file_name = str(row.get("file", ""))
        candidate = (manifest_path.parent / file_name).resolve()
        try:
            candidate.relative_to(manifest_path.parent)
        except ValueError as exc:
            raise OfflinePanelBuilderError("features-only file escaped its root") from exc
        if not candidate.is_file():
            raise OfflinePanelBuilderError(f"features-only daily file is missing: {day}")
        size = int(candidate.stat().st_size)
        sha = _file_sha256(candidate)
        if size != int(row.get("size_bytes", -1)) or sha != row.get("sha256"):
            raise OfflinePanelBuilderError(f"features-only daily identity drifted: {day}")
        schema = pq.ParquetFile(candidate).schema_arrow
        forbidden = [
            name
            for name in schema.names
            if any(str(name).lower().startswith(prefix) for prefix in _FORBIDDEN_FEATURE_PREFIXES)
        ]
        if forbidden:
            raise OfflinePanelBuilderError(
                f"features-only daily file contains labels: {day}: {forbidden[:3]}"
            )
        files[day] = candidate
        digest.update(f"{day}\0{file_name}\0{size}\0{sha}\n".encode())
    inference_days = tuple(_require_day(day) for day in split["inference"])
    if inference_days != tuple(sorted(files)):
        raise OfflinePanelBuilderError("features-only inference split/file order drifted")
    if digest.hexdigest() != payload.get("daily_manifest_sha256"):
        raise OfflinePanelBuilderError("features-only daily manifest hash drifted")
    missing_context = set(required_context_days) - set(files)
    if missing_context:
        raise OfflinePanelBuilderError(
            f"features-only panel lacks D-1/D/D+1 context days: {sorted(missing_context)}"
        )
    payload = dict(payload)
    payload["_validated_feature_dag_sha256"] = feature_dag_sha
    return payload, files


def _derive_context_contract(
    source: Mapping[str, Any],
    selected_days: Sequence[str],
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    """Independently derive replay and observation days from target receipts."""

    selected = tuple(_require_day(day) for day in selected_days)
    target_rows = source.get("target_day_receipts")
    if not isinstance(target_rows, list):
        raise OfflinePanelBuilderError("canonical target-day receipts are missing")
    by_day: dict[str, Mapping[str, Any]] = {}
    for row in target_rows:
        if not isinstance(row, Mapping):
            raise OfflinePanelBuilderError("canonical target-day receipt is malformed")
        day = _require_day(row.get("utc_day"))
        if day in by_day:
            raise OfflinePanelBuilderError(f"canonical target day is duplicated: {day}")
        by_day[day] = row
    if set(selected) - set(by_day):
        raise OfflinePanelBuilderError("selected target-day receipt census drifted")

    replay_days: set[str] = set()
    observation_days: set[str] = set(selected)
    for day in selected:
        context = by_day[day].get("context_days")
        if not isinstance(context, Mapping) or set(context) != {
            "D_minus_1",
            "D",
            "D_plus_1",
        }:
            raise OfflinePanelBuilderError(f"canonical D-1/D/D+1 context drifted: {day}")
        target = date.fromisoformat(day)
        expected = {
            "D_minus_1": (target - timedelta(days=1)).isoformat(),
            "D": day,
            "D_plus_1": (target + timedelta(days=1)).isoformat(),
        }
        if {key: str(context.get(key)) for key in expected} != expected:
            raise OfflinePanelBuilderError(f"canonical D-1/D/D+1 dates drifted: {day}")
        replay_days.update(expected.values())
        observation_days.add(expected["D_plus_1"])

    observation = tuple(sorted(observation_days))
    continuation = tuple(day for day in observation if day not in set(selected))
    replay = tuple(sorted(replay_days))
    if offline.REQUIRED_DAYS == 30:
        if selected != tuple(offline.PRIMARY_TARGET_DAYS):
            raise OfflinePanelBuilderError("formal selected target-day identity drifted")
        if (
            len(observation) != EXPECTED_OBSERVATION_CONTEXT_DAY_COUNT
            or continuation != EXPECTED_CONTINUATION_ONLY_DAYS
        ):
            raise OfflinePanelBuilderError("formal D/D+1 observation context drifted")
    return replay, observation, continuation


def validate_inputs(
    *,
    source_manifest_path: Path,
    book_view_root: Path,
    native_observation_manifest_path: Path,
    native_observation_root: Path,
    features_manifest_path: Path,
    owner_artifacts: OwnerArtifactPaths,
    layout: offline.OfflineSourceLayout | None = None,
    validation_workers: int = 1,
) -> ValidatedPanelInputs:
    """Fully revalidate every outcome-blind input before replay materialization."""

    active_layout = layout or offline.default_layout()
    source_path = source_manifest_path.expanduser().resolve()
    try:
        source = offline.validate_canonical_manifest(
            source_path,
            rehash_sources=True,
            layout=active_layout,
        )
    except Exception as exc:
        raise OfflinePanelBuilderError("canonical source manifest failed validation") from exc
    selected_days = tuple(_require_day(day) for day in source.get("selected_days", ()))
    if (
        len(selected_days) != offline.REQUIRED_DAYS
        or len(set(selected_days)) != len(selected_days)
        or source.get("panel_role") != PANEL_ROLE
        or source.get("queue_identity") != QUEUE_IDENTITY
    ):
        raise OfflinePanelBuilderError("canonical source panel identity drifted")
    replay_context_days, observation_days, continuation_only_days = _derive_context_contract(
        source, selected_days
    )
    try:
        book_view = mechanics.validate_book_view(
            book_view_root.expanduser().resolve(),
            layout=active_layout,
        )
    except Exception as exc:
        raise OfflinePanelBuilderError("normalized book view failed validation") from exc
    if (
        tuple(book_view.get("selected_target_days", ())) != selected_days
        or tuple(book_view.get("context_days", ())) != replay_context_days
        or book_view.get("source_manifest", {}).get("canonical_sha256")
        != source.get("canonical_manifest_sha256")
        or book_view.get("same_millisecond_ambiguity_policy") != SAME_MILLISECOND_AMBIGUITY_POLICY
    ):
        raise OfflinePanelBuilderError("normalized book view source/day identity drifted")
    observation_path = native_observation_manifest_path.expanduser().resolve()
    try:
        observations = observation_batch.validate_batch_manifest(
            observation_path,
            output_root=native_observation_root.expanduser().resolve(),
            layout=active_layout,
            workers=int(validation_workers),
            deep=False,
        )
    except Exception as exc:
        raise OfflinePanelBuilderError("native observation manifest failed validation") from exc
    batch_observation_days, batch_continuation_only_days = (
        observation_batch._observation_context_days(source, selected_days)
    )
    if (
        observation_batch.SCHEMA_VERSION != EXPECTED_NATIVE_OBSERVATION_SCHEMA
        or observations.get("schema_version") != EXPECTED_NATIVE_OBSERVATION_SCHEMA
        or batch_observation_days != observation_days
        or batch_continuation_only_days != continuation_only_days
        or tuple(observations.get("selected_target_days", ())) != selected_days
        or observations.get("selected_target_day_count") != offline.REQUIRED_DAYS
        or tuple(observations.get("observation_context_days", ())) != observation_days
        or observations.get("observation_context_day_count") != len(observation_days)
        or tuple(observations.get("continuation_only_days", ())) != continuation_only_days
        or observations.get("continuation_days_create_target_assignments") is not False
        or observations.get("source_manifest", {}).get("canonical_manifest_sha256")
        != source.get("canonical_manifest_sha256")
        or any(value is not False for value in observations.get("permissions", {}).values())
    ):
        raise OfflinePanelBuilderError("native observation source/day identity drifted")
    observation_rows = observations.get("days")
    if (
        not isinstance(observation_rows, list)
        or tuple(str(row.get("utc_day")) for row in observation_rows if isinstance(row, Mapping))
        != observation_days
    ):
        raise OfflinePanelBuilderError("native observation per-day order drifted")
    for row in observation_rows:
        if not isinstance(row, Mapping):
            raise OfflinePanelBuilderError("native observation per-day row is malformed")
        utc_day = str(row.get("utc_day"))
        is_target = utc_day in selected_days
        expected_role = "selected_target" if is_target else "continuation_only"
        if (
            row.get("observation_role") != expected_role
            or row.get("target_assignment_eligible") is not is_target
            or _SHA_RE.fullmatch(str(row.get("observation_receipt_sha256", ""))) is None
        ):
            raise OfflinePanelBuilderError(
                f"native observation role/assignment contract drifted: {utc_day}"
            )
    features, feature_files = _validate_features_only_manifest(
        features_manifest_path,
        required_context_days=replay_context_days,
    )
    _validate_owner_artifacts(owner_artifacts)
    binding = {
        "source_manifest_file_sha256": _file_sha256(source_path),
        "source_manifest_canonical_sha256": source["canonical_manifest_sha256"],
        "source_selection_sha256": source["selection_sha256"],
        "book_view_manifest_sha256": _file_sha256(
            book_view_root.expanduser().resolve() / "manifest.json"
        ),
        "book_view_canonical_sha256": book_view["canonical_manifest_sha256"],
        "native_observation_manifest_sha256": _file_sha256(observation_path),
        "native_observation_canonical_sha256": observations["canonical_manifest_sha256"],
        "features_manifest_sha256": _file_sha256(features_manifest_path.expanduser().resolve()),
        "features_daily_manifest_sha256": features["daily_manifest_sha256"],
        "owner_policy_sha256": offline.ACTIVE_OWNER_POLICY_SHA256,
        "owner_predicate_bundle_sha256": offline.ACTIVE_PREDICATE_BUNDLE_SHA256,
        "owner_private_config_sha256": offline.ACTIVE_PRIVATE_CONFIG_SHA256,
        "selected_days": list(selected_days),
        "observation_context_days": list(observation_days),
        "continuation_only_days": list(continuation_only_days),
        "replay_context_days": list(replay_context_days),
        "panel_role": PANEL_ROLE,
        "queue_identity": QUEUE_IDENTITY,
        "same_millisecond_ambiguity_policy": SAME_MILLISECOND_AMBIGUITY_POLICY,
    }
    return ValidatedPanelInputs(
        project_data_root=active_layout.project_data_root.expanduser().resolve(),
        marketdata_root=active_layout.marketdata_root.expanduser().resolve(),
        source_manifest_path=source_path,
        source_manifest=source,
        book_view_root=book_view_root.expanduser().resolve(),
        book_view_manifest=book_view,
        native_observation_manifest_path=observation_path,
        native_observation_manifest=observations,
        native_observation_root=native_observation_root.expanduser().resolve(),
        features_manifest_path=features_manifest_path.expanduser().resolve(),
        features_manifest=features,
        feature_files=feature_files,
        owner_artifacts=owner_artifacts,
        selected_days=selected_days,
        observation_context_days=observation_days,
        continuation_only_days=continuation_only_days,
        replay_context_days=replay_context_days,
        input_binding_sha256=_canonical_sha256(binding),
    )


def _book_path(inputs: ValidatedPanelInputs, day: str, kind: str) -> Path:
    matches = [
        row
        for row in inputs.book_view_manifest.get("files", ())
        if isinstance(row, Mapping) and row.get("day") == day and row.get("kind") == kind
    ]
    if len(matches) != 1:
        raise OfflinePanelBuilderError(f"book-view {kind} binding is missing: {day}")
    path = inputs.book_view_root / kind / f"{offline.SYMBOL}-{kind}-{day}.parquet"
    if not path.is_file() or _file_sha256(path) != matches[0].get("sha256"):
        raise OfflinePanelBuilderError(f"book-view {kind} bytes drifted: {day}")
    return path


def _source_receipts(inputs: ValidatedPanelInputs, day: str) -> dict[str, str]:
    target = next(
        (
            row
            for row in inputs.source_manifest.get("target_day_receipts", ())
            if isinstance(row, Mapping) and row.get("utc_day") == day
        ),
        None,
    )
    if not isinstance(target, Mapping):
        raise OfflinePanelBuilderError(f"selected day receipt is missing: {day}")
    context = target.get("context_days")
    receipt_files = inputs.source_manifest.get("source_day_receipt_files")
    if not isinstance(context, Mapping) or not isinstance(receipt_files, Mapping):
        raise OfflinePanelBuilderError(f"source context receipts are missing: {day}")
    context_days = tuple(_require_day(context.get(role)) for role in ("D_minus_1", "D", "D_plus_1"))
    if context_days[1] != day:
        raise OfflinePanelBuilderError(f"source target context drifted: {day}")
    continuation_day = context_days[2]
    native_rows = {
        str(row.get("utc_day")): row
        for row in inputs.native_observation_manifest.get("days", ())
        if isinstance(row, Mapping)
    }
    native = native_rows.get(day)
    continuation_native = native_rows.get(continuation_day)
    if not isinstance(native, Mapping) or not isinstance(continuation_native, Mapping):
        raise OfflinePanelBuilderError(
            f"blocked_missing_canonical_fields: native observation D/D+1: {day},{continuation_day}"
        )
    context_hashes: dict[str, str] = {}
    for role, context_day in zip(("D_minus_1", "D", "D_plus_1"), context_days, strict=True):
        binding = receipt_files.get(context_day)
        if not isinstance(binding, Mapping):
            raise OfflinePanelBuilderError(f"source-day receipt binding is missing: {context_day}")
        context_hashes[role] = _require_sha(
            binding.get("canonical_sha256"), label=f"{day} {role} source receipt"
        )
    book_rows = {
        (str(row.get("day")), str(row.get("kind"))): row
        for row in inputs.book_view_manifest.get("files", ())
        if isinstance(row, Mapping) and str(row.get("day")) in context_days
    }
    expected_book_keys = {
        (context_day, kind) for context_day in context_days for kind in ("bbo", "l2")
    }
    if set(book_rows) != expected_book_keys:
        raise OfflinePanelBuilderError(f"book-view D-1/D/D+1 receipts are missing: {day}")
    book_hashes = {
        context_day: {
            kind: _require_sha(
                book_rows[(context_day, kind)].get("sha256"),
                label=f"{context_day} normalized {kind.upper()}",
            )
            for kind in ("bbo", "l2")
        }
        for context_day in context_days
    }
    feature_hashes = {
        context_day: _file_sha256(inputs.feature_files[context_day]) for context_day in context_days
    }
    return {
        "source_manifest_canonical_sha256": _require_sha(
            inputs.source_manifest.get("canonical_manifest_sha256"),
            label="canonical source manifest",
        ),
        "source_selection_sha256": _require_sha(
            inputs.source_manifest.get("selection_sha256"),
            label="canonical source selection",
        ),
        "target_day_receipt_sha256": _require_sha(
            target.get("day_receipt_sha256"), label=f"{day} target receipt"
        ),
        "replay_context_days_json": json.dumps(list(context_days), separators=(",", ":")),
        "context_source_receipts_sha256": _canonical_sha256(context_hashes),
        "context_book_receipts_sha256": _canonical_sha256(book_hashes),
        "context_feature_receipts_sha256": _canonical_sha256(feature_hashes),
        "book_view_canonical_sha256": _require_sha(
            inputs.book_view_manifest.get("canonical_manifest_sha256"),
            label="normalized book view",
        ),
        "bbo_sha256": book_hashes[day]["bbo"],
        "l2_sha256": book_hashes[day]["l2"],
        "native_batch_canonical_sha256": _require_sha(
            inputs.native_observation_manifest.get("canonical_manifest_sha256"),
            label="native observation batch",
        ),
        "native_cache_manifest_sha256": _require_sha(
            native.get("cache_manifest_file_sha256"), label=f"{day} native cache manifest"
        ),
        "native_cache_parquet_sha256": _require_sha(
            native.get("cache_parquet_sha256"), label=f"{day} native cache parquet"
        ),
        "native_cache_observation_sha256": _require_sha(
            native.get("cache_observation_sha256"), label=f"{day} native observations"
        ),
        "native_source_binding_sha256": _require_sha(
            native.get("source_binding_sha256"), label=f"{day} native source binding"
        ),
        "native_observation_receipt_sha256": _require_sha(
            native.get("observation_receipt_sha256"),
            label=f"{day} native observation receipt",
        ),
        "native_observation_role": str(native.get("observation_role")),
        "native_target_assignment_eligible": str(native.get("target_assignment_eligible")).lower(),
        "continuation_day": continuation_day,
        "continuation_source_day_receipt_sha256": context_hashes["D_plus_1"],
        "continuation_bbo_sha256": book_hashes[continuation_day]["bbo"],
        "continuation_l2_sha256": book_hashes[continuation_day]["l2"],
        "continuation_native_cache_manifest_sha256": _require_sha(
            continuation_native.get("cache_manifest_file_sha256"),
            label=f"{continuation_day} native cache manifest",
        ),
        "continuation_native_cache_parquet_sha256": _require_sha(
            continuation_native.get("cache_parquet_sha256"),
            label=f"{continuation_day} native cache parquet",
        ),
        "continuation_native_cache_observation_sha256": _require_sha(
            continuation_native.get("cache_observation_sha256"),
            label=f"{continuation_day} native observations",
        ),
        "continuation_native_source_binding_sha256": _require_sha(
            continuation_native.get("source_binding_sha256"),
            label=f"{continuation_day} native source binding",
        ),
        "continuation_native_observation_receipt_sha256": _require_sha(
            continuation_native.get("observation_receipt_sha256"),
            label=f"{continuation_day} native observation receipt",
        ),
        "continuation_native_observation_role": str(continuation_native.get("observation_role")),
        "continuation_native_target_assignment_eligible": str(
            continuation_native.get("target_assignment_eligible")
        ).lower(),
        "continuation_use_role": "continuation_context_for_target",
        "continuation_creates_target_assignments": "false",
        "features_manifest_file_sha256": _file_sha256(inputs.features_manifest_path),
        "features_daily_manifest_sha256": _require_sha(
            inputs.features_manifest.get("daily_manifest_sha256"),
            label="features-only daily manifest",
        ),
        "features_day_file_sha256": _file_sha256(inputs.feature_files[day]),
        "continuation_features_day_file_sha256": feature_hashes[continuation_day],
        "feature_dag_sha256": _require_sha(
            inputs.features_manifest.get("_validated_feature_dag_sha256"),
            label="features-only Feature DAG",
        ),
    }


def _day_request(inputs: ValidatedPanelInputs, day: str) -> DayMaterializationRequest:
    return DayMaterializationRequest(
        utc_day=day,
        panel_role=PANEL_ROLE,
        queue_identity=QUEUE_IDENTITY,
        same_millisecond_ambiguity_policy=SAME_MILLISECOND_AMBIGUITY_POLICY,
        bbo_path=_book_path(inputs, day, "bbo"),
        l2_path=_book_path(inputs, day, "l2"),
        features_path=inputs.feature_files[day],
        source_manifest_path=inputs.source_manifest_path,
        book_view_manifest_path=inputs.book_view_root / "manifest.json",
        features_manifest_path=inputs.features_manifest_path,
        private_config_path=inputs.owner_artifacts.private_config,
        native_observation_root=inputs.native_observation_root,
        source_receipts=_source_receipts(inputs, day),
        input_binding_sha256=inputs.input_binding_sha256,
    )


def _portable_bound_path(path: Path, *, inputs: ValidatedPanelInputs) -> str:
    resolved = path.expanduser().resolve()
    repository_root = Path(__file__).resolve().parents[4]
    if resolved == inputs.owner_artifacts.private_config.expanduser().resolve():
        return "${NARROWGATE_LIVE_CONFIG}"
    try:
        return offline._portable_path(
            resolved,
            project_data=inputs.project_data_root,
            market_data=inputs.marketdata_root,
        )
    except offline.OfflineSourceGateError:
        try:
            relative = resolved.relative_to(repository_root)
        except ValueError as exc:
            raise OfflinePanelBuilderError(
                f"canonical replay input is outside portable roots: {resolved.name}"
            ) from exc
        return f"${{NARROWGATE_ROOT}}/{relative.as_posix()}"


def _canonical_day_projection(
    inputs: ValidatedPanelInputs,
    day: str,
) -> dict[str, Any]:
    request = _day_request(inputs, day)
    payload: dict[str, Any] = {
        "utc_day": day,
        "panel_role": request.panel_role,
        "queue_identity": request.queue_identity,
        "same_millisecond_ambiguity_policy": (
            request.same_millisecond_ambiguity_policy
        ),
        "bbo_path": _portable_bound_path(request.bbo_path, inputs=inputs),
        "bbo_sha256": _file_sha256(request.bbo_path),
        "l2_path": _portable_bound_path(request.l2_path, inputs=inputs),
        "l2_sha256": _file_sha256(request.l2_path),
        "features_path": _portable_bound_path(request.features_path, inputs=inputs),
        "features_sha256": _file_sha256(request.features_path),
        "source_manifest_path": _portable_bound_path(
            request.source_manifest_path, inputs=inputs
        ),
        "source_manifest_sha256": _file_sha256(request.source_manifest_path),
        "book_view_manifest_path": _portable_bound_path(
            request.book_view_manifest_path, inputs=inputs
        ),
        "book_view_manifest_sha256": _file_sha256(request.book_view_manifest_path),
        "features_manifest_path": _portable_bound_path(
            request.features_manifest_path, inputs=inputs
        ),
        "features_manifest_sha256": _file_sha256(request.features_manifest_path),
        "private_config_path": _portable_bound_path(
            request.private_config_path, inputs=inputs
        ),
        "private_config_sha256": _file_sha256(request.private_config_path),
        "native_observation_root": _portable_bound_path(
            request.native_observation_root, inputs=inputs
        ),
        "source_receipts": dict(request.source_receipts),
        "input_binding_sha256": request.input_binding_sha256,
    }
    payload["projection_receipt_sha256"] = _canonical_sha256(payload)
    return payload


def _ensure_portable_replay_binding(
    inputs: ValidatedPanelInputs,
    *,
    output_root: Path,
) -> dict[str, Any]:
    replay_adapter = importlib.import_module(
        "research.families.f05_fill_quality_quote_ev.audit."
        "causal_multichannel_window_boolean_cooldown_full_multiscale_"
        "successor_offline_replay_adapter_v1"
    )
    root = output_root.expanduser().resolve()
    binding_path = root / "_bindings" / PORTABLE_BINDING_FILENAME
    cache_root = (
        inputs.project_data_root
        / "cache"
        / "replay_dag"
        / SEQUENTIAL_CACHE_DIRECTORY
    ).resolve()
    try:
        cache_root.relative_to((inputs.project_data_root / "cache" / "replay_dag").resolve())
    except ValueError as exc:
        raise OfflinePanelBuilderError("sequential replay cache escaped its governed root") from exc
    projections = {
        day: _canonical_day_projection(inputs, day) for day in inputs.selected_days
    }
    binding: dict[str, Any] = {
        "schema_version": f"{replay_adapter.IDENTITY}.portable_replay_binding.v1",
        "identity": SEQUENTIAL_REPLAY_INPUT_IDENTITY,
        "panel_identity": IDENTITY,
        "selected_days": list(inputs.selected_days),
        "selected_day_count": len(inputs.selected_days),
        "fixed_bridge": replay_adapter._expected_fixed_bridge(),
        "target_day_end_terminalized": False,
        "d_plus_1_new_target_assignments_allowed": False,
        "assignment_to_common_washout_required": True,
        "observation_end_semantics": (
            "outcome_blind_common_D_plus_1_administrative_bound_v1"
        ),
        "native_observation_batch_manifest": {
            "path": _portable_bound_path(
                inputs.native_observation_manifest_path, inputs=inputs
            ),
            "file_sha256": _file_sha256(inputs.native_observation_manifest_path),
            "canonical_manifest_sha256": _require_sha(
                inputs.native_observation_manifest.get("canonical_manifest_sha256"),
                label="native observation batch canonical identity",
            ),
        },
        "day_projections": projections,
    }
    if binding_path.exists():
        if _load_json(binding_path, label="portable replay binding") != binding:
            raise OfflinePanelBuilderError("immutable portable replay binding drifted")
    else:
        _atomic_json(binding_path, binding)
        if _load_json(binding_path, label="portable replay binding") != binding:
            raise OfflinePanelBuilderError("portable replay binding write drifted")
    binding_sha = _file_sha256(binding_path)
    return {
        "path": binding_path,
        "portable_path": _portable_bound_path(binding_path, inputs=inputs),
        "sha256": binding_sha,
        "portable_day_cache_root": offline._portable_path(
            cache_root,
            project_data=inputs.project_data_root,
            market_data=inputs.marketdata_root,
        ),
        "day_projections": projections,
    }


def _validate_identity_hashes(
    values: Mapping[str, str], *, inputs: ValidatedPanelInputs
) -> dict[str, str]:
    if set(values) != set(IDENTITY_HASH_FIELDS):
        raise OfflinePanelBuilderError("B0 adapter execution identity hash census drifted")
    hashes = {
        name: _require_sha(values[name], label=f"B0 adapter {name}")
        for name in IDENTITY_HASH_FIELDS
    }
    if hashes["config_sha256"] != offline.ACTIVE_PRIVATE_CONFIG_SHA256:
        raise OfflinePanelBuilderError("B0 adapter config identity drifted")
    if hashes["baseline_identity_sha256"] != offline.ACTIVE_OWNER_POLICY_SHA256:
        raise OfflinePanelBuilderError("B0 adapter baseline is not the exact owner policy")
    if hashes["feature_dag_sha256"] != inputs.features_manifest.get(
        "_validated_feature_dag_sha256"
    ):
        raise OfflinePanelBuilderError("B0 adapter Feature DAG identity drifted")
    return hashes


class _RecordingOwnerEvaluator:
    def __init__(self, delegate: Any) -> None:
        self._delegate = delegate
        self.decisions: dict[str, Any] = {}

    def evaluate(self, snapshot: CooldownAssignmentSnapshotV2, baseline_duration_ms: float) -> Any:
        decision = self._delegate.evaluate(snapshot, baseline_duration_ms)
        snapshot_id = str(snapshot.snapshot_id)
        if snapshot_id in self.decisions:
            raise OfflinePanelBuilderError("exact owner evaluator saw a duplicate snapshot")
        if (
            str(decision.snapshot_id) != snapshot_id
            or str(decision.policy_sha256) != offline.ACTIVE_OWNER_POLICY_SHA256
            or str(decision.predicate_bundle_sha256) != offline.ACTIVE_PREDICATE_BUNDLE_SHA256
            or str(decision.action_id) not in OWNER_ACTIONS
        ):
            raise OfflinePanelBuilderError("exact owner decision identity drifted")
        self.decisions[snapshot_id] = decision
        return decision

    def audit(self) -> Mapping[str, Any]:
        return self._delegate.audit()


def _load_owner_evaluator(inputs: ValidatedPanelInputs) -> _RecordingOwnerEvaluator:
    evaluator = runtime_policy.load_runtime_policy(
        policy_path=inputs.owner_artifacts.policy,
        predicate_bundle_path=inputs.owner_artifacts.predicate_bundle,
        expected_policy_sha256=offline.ACTIVE_OWNER_POLICY_SHA256,
        expected_predicate_bundle_sha256=offline.ACTIVE_PREDICATE_BUNDLE_SHA256,
    )
    if not evaluator.binding_valid:
        raise OfflinePanelBuilderError(
            f"exact owner evaluator failed binding: {evaluator.binding_error}"
        )
    return _RecordingOwnerEvaluator(evaluator)


def _load_canonical_adapter() -> B0MechanicsReplayAdapter:
    try:
        module = importlib.import_module(CANONICAL_ADAPTER_MODULE)
    except ModuleNotFoundError as exc:
        if exc.name == CANONICAL_ADAPTER_MODULE:
            raise OfflinePanelBuilderBlocked(BLOCKED_ADAPTER_STATUS) from exc
        raise
    factory = getattr(module, CANONICAL_ADAPTER_FACTORY, None)
    if not callable(factory):
        raise OfflinePanelBuilderBlocked(BLOCKED_ADAPTER_STATUS)
    adapter = factory()
    if getattr(adapter, "identity", None) != CANONICAL_ADAPTER_IDENTITY:
        raise OfflinePanelBuilderError("canonical B0 mechanics adapter identity drifted")
    return adapter


def _stitch_observation_caches(
    target: Iterable[CausalWindowObservation],
    continuation: Iterable[CausalWindowObservation],
) -> Iterator[CausalWindowObservation]:
    """Join overlapping caches and rebase their cache-local generation ordinals."""

    last_right: int | None = None
    last_market: int | None = None
    last_depth: int | None = None
    for observation in target:
        right_ns = int(observation.right_ts_ns)
        market_generation = int(observation.market_generation)
        depth_generation = int(observation.depth_generation)
        if (
            (last_right is not None and right_ns <= last_right)
            or (last_market is not None and market_generation <= last_market)
            or (last_depth is not None and depth_generation <= last_depth)
        ):
            raise OfflinePanelBuilderError("target observation cache is not strictly ordered")
        last_right = right_ns
        last_market = market_generation
        last_depth = depth_generation
        yield observation
    if last_right is None or last_market is None or last_depth is None:
        raise OfflinePanelBuilderError("target observation cache is empty")

    market_offset: int | None = None
    depth_offset: int | None = None
    admitted = False
    for observation in continuation:
        right_ns = int(observation.right_ts_ns)
        if not admitted and right_ns <= last_right:
            continue
        admitted = True
        if market_offset is None or depth_offset is None:
            market_offset = last_market + 1 - int(observation.market_generation)
            depth_offset = last_depth + 1 - int(observation.depth_generation)
        rebased = replace(
            observation,
            market_generation=int(observation.market_generation) + market_offset,
            depth_generation=int(observation.depth_generation) + depth_offset,
        )
        if (
            right_ns <= last_right
            or int(rebased.market_generation) <= last_market
            or int(rebased.depth_generation) <= last_depth
        ):
            raise OfflinePanelBuilderError(
                "continuation cache is not strictly ordered after generation rebasing"
            )
        last_right = right_ns
        last_market = int(rebased.market_generation)
        last_depth = int(rebased.depth_generation)
        yield rebased


def _validate_adapter_result(
    raw: Mapping[str, Any],
    *,
    day: str,
    snapshot_count: int,
) -> dict[str, Any]:
    if not isinstance(raw, Mapping) or set(raw) != _ADAPTER_RESULT_FIELDS:
        raise OfflinePanelBuilderError("B0 adapter result schema drifted")
    for key in raw:
        lower = str(key).lower()
        if any(token in lower for token in _FORBIDDEN_ADAPTER_KEYS):
            if key not in {"candidate_actions_generated", "economic_outcomes_read"}:
                raise OfflinePanelBuilderError("B0 adapter returned an economic field")
    required = {
        "schema_version": ADAPTER_RESULT_SCHEMA,
        "identity": CANONICAL_ADAPTER_IDENTITY,
        "utc_day": day,
        "replay_engine": REPLAY_ENGINE,
        "queue_identity": QUEUE_IDENTITY,
        "same_millisecond_ambiguity_policy": SAME_MILLISECOND_AMBIGUITY_POLICY,
        "exposure_fill_scope": "exposure_increasing_only",
        "current_owner_b0_executed": True,
        "candidate_actions_generated": False,
        "economic_outcomes_read": False,
        "labels_read": False,
        "snapshots_emitted": snapshot_count,
    }
    if any(raw.get(key) != value for key, value in required.items()):
        raise OfflinePanelBuilderError("B0 adapter result contract drifted")
    result = dict(raw)
    for key in (
        "market_window_identity_sha256",
        "model_overlay_identity_sha256",
        "latency_identity_sha256",
        "queue_random_identity_sha256",
        "replay_input_receipt_sha256",
    ):
        result[key] = _require_sha(result.get(key), label=f"B0 adapter {key}")
    assignment_mechanics = result.get("assignment_mechanics")
    if (
        not isinstance(assignment_mechanics, Mapping)
        or len(assignment_mechanics) != snapshot_count
        or any(not isinstance(row, Mapping) for row in assignment_mechanics.values())
    ):
        raise OfflinePanelBuilderError(
            "B0 adapter assignment mechanics denominator drifted"
        )
    normalized_mechanics: dict[str, dict[str, Any]] = {}
    expected_fields = {
        "campaign_id",
        "order_id",
        "exposure_fill_ordinal",
        "assignment_equity_usdc",
    }
    for snapshot_id, row in assignment_mechanics.items():
        if set(row) != expected_fields:
            raise OfflinePanelBuilderError("B0 assignment mechanics schema drifted")
        normalized = {
            "campaign_id": int(row["campaign_id"]),
            "order_id": int(row["order_id"]),
            "exposure_fill_ordinal": int(row["exposure_fill_ordinal"]),
            "assignment_equity_usdc": float(row["assignment_equity_usdc"]),
        }
        if (
            normalized["campaign_id"] <= 0
            or normalized["order_id"] < 0
            or normalized["exposure_fill_ordinal"] <= 0
            or not math.isfinite(normalized["assignment_equity_usdc"])
        ):
            raise OfflinePanelBuilderError("B0 assignment mechanics value drifted")
        normalized_mechanics[str(snapshot_id)] = normalized
    result["assignment_mechanics"] = normalized_mechanics
    return result


def _opportunity_id(
    snapshot: CooldownAssignmentSnapshotV2,
    *,
    day: str,
    input_binding_sha256: str,
) -> str:
    return "f05-offline-" + _canonical_sha256(
        {
            "schema_version": OPPORTUNITY_ID_SCHEMA_VERSION,
            "utc_day": day,
            "snapshot_id": snapshot.snapshot_id,
            "assignment_id": snapshot.assignment_id,
            "fill_event_id": snapshot.fill_event_id,
            "lineage_id": snapshot.lineage_id,
            "lineage_revision": snapshot.lineage_revision,
            "partial_fill_ordinal": snapshot.partial_fill_ordinal,
            "input_binding_sha256": input_binding_sha256,
        }
    )


def _assignment_identity(
    snapshot: CooldownAssignmentSnapshotV2,
) -> tuple[int, int]:
    """Cross-check the frozen replay assignment identifiers."""

    assignment = str(snapshot.assignment_id).split(":")
    fill = str(snapshot.fill_event_id).split(":")
    client = str(snapshot.client_order_id)
    if (
        len(assignment) != 6
        or assignment[0] != "cooldown-v2"
        or assignment[1] != offline.SYMBOL
        or len(fill) != 5
        or fill[0] != "fill"
        or not client.startswith("replay-order-")
    ):
        raise OfflinePanelBuilderError("frozen replay assignment identity is malformed")
    try:
        assignment_ts_ms = int(assignment[2])
        assignment_order_id = int(assignment[4])
        assignment_ordinal = int(assignment[5])
        fill_order_id = int(fill[1])
        fill_partial_ordinal = int(fill[2])
        fill_ts_ms = int(fill[3])
        fill_ordinal = int(fill[4])
        client_order_id = int(client.removeprefix("replay-order-"))
    except ValueError as exc:
        raise OfflinePanelBuilderError(
            "frozen replay assignment identity contains a non-integer component"
        ) from exc
    m0 = snapshot.m0_context.to_dict()
    assignment_ts_ns = int(m0["assignment_ts_ns"])
    m0_ordinal = m0.get("exposure_fill_ordinal")
    if (
        assignment_order_id < 0
        or assignment_ordinal <= 0
        or assignment_order_id != fill_order_id
        or assignment_order_id != client_order_id
        or assignment_ordinal != fill_ordinal
        or (m0_ordinal is not None and assignment_ordinal != int(m0_ordinal))
    ):
        raise OfflinePanelBuilderError("frozen replay order/ordinal identities disagree")
    if (
        assignment[3] != str(m0["side"])
        or fill_partial_ordinal != int(snapshot.partial_fill_ordinal)
        or assignment_ts_ms != fill_ts_ms
        or assignment_ts_ns != assignment_ts_ms * 1_000_000
    ):
        raise OfflinePanelBuilderError("frozen replay fill clock/side identity disagrees")
    return assignment_order_id, assignment_ordinal


def _observation_end_ts_ns(day: str) -> int:
    target = date.fromisoformat(_require_day(day))
    return int((target + timedelta(days=2) - date(1970, 1, 1)).days) * 86_400 * 1_000_000_000


def _sequential_context_bindings(
    request: DayMaterializationRequest,
) -> dict[str, str]:
    receipts = request.source_receipts
    continuation_day = _require_day(receipts["continuation_day"])
    expected = (date.fromisoformat(request.utc_day) + timedelta(days=1)).isoformat()
    if continuation_day != expected:
        raise OfflinePanelBuilderError("D+1 continuation day drifted")
    native_observation = _require_sha(
        receipts.get("continuation_native_cache_observation_sha256"),
        label=f"{continuation_day} native observation bytes",
    )
    market_identity = _canonical_sha256(
        {
            "schema_version": f"{SEQUENTIAL_REPLAY_INPUT_IDENTITY}.D_plus_1_market.v1",
            "utc_day": continuation_day,
            "source_day_receipt_sha256": _require_sha(
                receipts.get("continuation_source_day_receipt_sha256"),
                label=f"{continuation_day} source receipt",
            ),
            "bbo_sha256": _require_sha(
                receipts.get("continuation_bbo_sha256"),
                label=f"{continuation_day} BBO bytes",
            ),
            "l2_sha256": _require_sha(
                receipts.get("continuation_l2_sha256"),
                label=f"{continuation_day} L2 bytes",
            ),
            "native_observation_sha256": native_observation,
            "queue_identity": QUEUE_IDENTITY,
            "same_millisecond_ambiguity_policy": SAME_MILLISECOND_AMBIGUITY_POLICY,
        }
    )
    feature_identity = _canonical_sha256(
        {
            "schema_version": f"{SEQUENTIAL_REPLAY_INPUT_IDENTITY}.D_plus_1_feature.v1",
            "utc_day": continuation_day,
            "feature_file_sha256": _require_sha(
                receipts.get("continuation_features_day_file_sha256"),
                label=f"{continuation_day} feature bytes",
            ),
            "feature_dag_sha256": _require_sha(
                receipts.get("feature_dag_sha256"),
                label="Feature DAG",
            ),
            "features_daily_manifest_sha256": _require_sha(
                receipts.get("features_daily_manifest_sha256"),
                label="features daily manifest",
            ),
        }
    )
    context_identity = _canonical_sha256(
        {
            "schema_version": f"{SEQUENTIAL_REPLAY_INPUT_IDENTITY}.D_plus_1_context.v1",
            "utc_day": continuation_day,
            "market_identity_sha256": market_identity,
            "feature_identity_sha256": feature_identity,
            "native_observation_sha256": native_observation,
            "native_observation_receipt_sha256": _require_sha(
                receipts.get("continuation_native_observation_receipt_sha256"),
                label=f"{continuation_day} native observation receipt",
            ),
            "new_target_assignments_allowed": False,
            "target_day_end_terminalized": False,
            "assignment_to_common_washout_required": True,
        }
    )
    return {
        "d_plus_1_utc_day": continuation_day,
        "d_plus_1_market_identity_sha256": market_identity,
        "d_plus_1_feature_identity_sha256": feature_identity,
        "d_plus_1_native_observation_sha256": native_observation,
        "d_plus_1_context_receipt_sha256": context_identity,
    }


def _campaign_cluster_id(
    *,
    utc_day: str,
    campaign_id: int,
    adapter_result: Mapping[str, Any],
) -> str:
    return "b0-campaign-" + _canonical_sha256(
        {
            "schema_version": f"{SEQUENTIAL_REPLAY_INPUT_IDENTITY}.campaign_cluster.v1",
            "b0_replay_input_receipt_sha256": adapter_result[
                "replay_input_receipt_sha256"
            ],
            "market_window_identity_sha256": adapter_result[
                "market_window_identity_sha256"
            ],
            "utc_day": utc_day,
            "campaign_id": int(campaign_id),
        }
    )


def _predecessor_day_opportunity_sha256(
    inputs: ValidatedPanelInputs,
    *,
    utc_day: str,
    observed_ids: Sequence[str],
) -> str | None:
    if len(inputs.selected_days) != offline.REQUIRED_DAYS or offline.REQUIRED_DAYS != 30:
        return None
    path = (
        inputs.project_data_root
        / "cache"
        / "replay_dag"
        / "f05_full_multiscale_successor_offline_panel_builder_v1"
        / utc_day
        / "metadata.parquet"
    )
    if not path.is_file():
        raise OfflinePanelBuilderError(
            f"predecessor opportunity denominator is missing: {utc_day}"
        )
    table = pq.read_table(path, columns=["opportunity_id"])
    expected = tuple(str(value) for value in table["opportunity_id"].to_pylist())
    observed = tuple(str(value) for value in observed_ids)
    if observed != expected or set(observed) != set(expected):
        raise OfflinePanelBuilderError(
            f"v2 opportunity IDs drifted from the immutable predecessor: {utc_day}"
        )
    return _canonical_sha256(list(expected))


def _split_feature_row(
    snapshot: CooldownAssignmentSnapshotV2,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    feature = snapshot.feature_row.to_dict()
    metadata: dict[str, Any] = {}
    boolean: dict[str, Any] = {}
    continuous: dict[str, Any] = {}
    for name, value in feature.items():
        if name in M0_REQUIRED_FIELDS:
            continue
        if name in _FEATURE_METADATA_FIELDS:
            metadata[f"feature::{name}"] = value
        elif name.startswith("tri::") or (
            name.startswith("channel::") and name.endswith("::observed")
        ):
            boolean[name] = value
        elif name.startswith("value::"):
            continuous[name] = value
        else:
            raise OfflinePanelBuilderError(f"unclassified M2 feature field: {name}")
    if not boolean or not continuous:
        raise OfflinePanelBuilderError("full M2 feature row was not materialized")
    reconstructed = {
        **{name.removeprefix("feature::"): value for name, value in metadata.items()},
        **boolean,
        **continuous,
        **snapshot.m0_context.to_dict(),
    }
    if reconstructed != feature:
        raise OfflinePanelBuilderError("full M2 feature row projection lost fields")
    return metadata, boolean, continuous


def _validate_exposure_fill(snapshot: CooldownAssignmentSnapshotV2) -> None:
    if snapshot.visibility_profile != HISTORICAL_EXCHANGE_EVENT_PROFILE:
        raise OfflinePanelBuilderError("mechanics snapshot visibility profile drifted")
    if snapshot.feature_block != "M2" or snapshot.economic_outcomes_read:
        raise OfflinePanelBuilderError("mechanics snapshot is not outcome-blind M2")
    m0 = snapshot.m0_context.to_dict()
    side = str(m0["side"])
    before = float(m0["inventory_before_fill_btc"])
    after = float(m0["inventory_after_fill_btc"])
    increasing = (side == "BUY" and before >= -1e-12 and after > before) or (
        side == "SELL" and before <= 1e-12 and after < before
    )
    if not increasing or str(m0["role_at_fill"]) not in {"opener", "add"}:
        raise OfflinePanelBuilderError("snapshot is not an exposure-increasing fill")
    if (
        str(m0["queue_state_before_fill"]) != "unknown"
        or m0["queue_ahead_before_fill_btc"] is not None
        or bool(m0["target_price_displayed_qty_is_queue_ahead"])
    ):
        raise OfflinePanelBuilderError("modeled-queue snapshot overclaimed queue authority")


def _project_rows(
    *,
    request: DayMaterializationRequest,
    snapshots: Sequence[CooldownAssignmentSnapshotV2],
    decisions: Mapping[str, Any],
    adapter_result: Mapping[str, Any],
    sequential_binding: Mapping[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    output = {role: [] for role in PANEL_ROLES}
    seen: set[str] = set()
    context = _sequential_context_bindings(request)
    projection = sequential_binding.get("day_projections", {}).get(request.utc_day)
    if not isinstance(projection, Mapping):
        raise OfflinePanelBuilderError("portable replay binding lacks target-day projection")
    day_input_sha256 = _require_sha(
        projection.get("projection_receipt_sha256"),
        label=f"{request.utc_day} day input",
    )
    observation_end_ts_ns = _observation_end_ts_ns(request.utc_day)
    assignment_mechanics = adapter_result["assignment_mechanics"]
    for snapshot in snapshots:
        _validate_exposure_fill(snapshot)
        opportunity_id = _opportunity_id(
            snapshot,
            day=request.utc_day,
            input_binding_sha256=request.input_binding_sha256,
        )
        if opportunity_id in seen:
            raise OfflinePanelBuilderError("stable opportunity ID collided")
        seen.add(opportunity_id)
        decision = decisions.get(snapshot.snapshot_id)
        if decision is None:
            raise OfflinePanelBuilderError("snapshot lacks an exact owner B0 decision")
        mechanics_row = assignment_mechanics.get(str(snapshot.snapshot_id))
        if not isinstance(mechanics_row, Mapping):
            raise OfflinePanelBuilderError("snapshot lacks B0 assignment mechanics")
        campaign_id = int(mechanics_row.get("campaign_id", 0) or 0)
        assignment_equity_usdc = float(
            mechanics_row.get("assignment_equity_usdc", float("nan"))
        )
        order_id, exposure_fill_ordinal = _assignment_identity(snapshot)
        if (
            campaign_id <= 0
            or not math.isfinite(assignment_equity_usdc)
            or int(mechanics_row.get("order_id", 0) or 0) != order_id
            or int(mechanics_row.get("exposure_fill_ordinal", 0) or 0)
            != exposure_fill_ordinal
        ):
            raise OfflinePanelBuilderError("B0 assignment mechanics are invalid")
        campaign_cluster_id = _campaign_cluster_id(
            utc_day=request.utc_day,
            campaign_id=campaign_id,
            adapter_result=adapter_result,
        )
        m0 = snapshot.m0_context.to_dict()
        if observation_end_ts_ns <= int(m0["assignment_ts_ns"]):
            raise OfflinePanelBuilderError("administrative observation end precedes assignment")
        feature_metadata, boolean, continuous = _split_feature_row(snapshot)
        base = {"utc_day": request.utc_day, "opportunity_id": opportunity_id}
        output["metadata"].append(
            {
                **base,
                "panel_role": PANEL_ROLE,
                "snapshot_id": snapshot.snapshot_id,
                "assignment_id": snapshot.assignment_id,
                "fill_event_id": snapshot.fill_event_id,
                "client_order_id": snapshot.client_order_id,
                "lineage_id": snapshot.lineage_id,
                "lineage_revision": snapshot.lineage_revision,
                "partial_fill_ordinal": snapshot.partial_fill_ordinal,
                "partial_fill_qty_btc": snapshot.partial_fill_qty_btc,
                "policy_input_valid": snapshot.policy_input_valid,
                "snapshot_fallback_policy_id": snapshot.fallback_policy_id,
                "snapshot_fallback_reason": snapshot.fallback_reason,
                "source_bundle_sha256": snapshot.source_bundle_sha256,
                "campaign_cluster_id": campaign_cluster_id,
                "observation_end_ts_ns": observation_end_ts_ns,
                **m0,
                **feature_metadata,
            }
        )
        output["boolean_features"].append({**base, **boolean})
        output["continuous_features"].append({**base, **continuous})
        output["exact_owner_actions"].append(
            {
                **base,
                "exact_owner_action": str(decision.action_id),
                "exact_owner_duration_ms": int(decision.duration_ms),
                "owner_support_valid": bool(decision.support_valid),
                "owner_fallback_reason": decision.fallback_reason,
                "owner_matched_rule_index": decision.matched_rule_index,
                "owner_policy_sha256": str(decision.policy_sha256),
                "owner_predicate_bundle_sha256": str(decision.predicate_bundle_sha256),
            }
        )
        output["replay_inputs"].append(
            {
                **base,
                "replay_engine": REPLAY_ENGINE,
                "queue_identity": QUEUE_IDENTITY,
                "same_millisecond_ambiguity_policy": (SAME_MILLISECOND_AMBIGUITY_POLICY),
                "source_manifest_canonical_sha256": request.source_receipts[
                    "source_manifest_canonical_sha256"
                ],
                "target_day_receipt_sha256": request.source_receipts["target_day_receipt_sha256"],
                "replay_context_days_json": request.source_receipts["replay_context_days_json"],
                "context_source_receipts_sha256": request.source_receipts[
                    "context_source_receipts_sha256"
                ],
                "context_book_receipts_sha256": request.source_receipts[
                    "context_book_receipts_sha256"
                ],
                "context_feature_receipts_sha256": request.source_receipts[
                    "context_feature_receipts_sha256"
                ],
                "native_cache_observation_sha256": request.source_receipts[
                    "native_cache_observation_sha256"
                ],
                "native_observation_role": request.source_receipts["native_observation_role"],
                "native_target_assignment_eligible": request.source_receipts[
                    "native_target_assignment_eligible"
                ],
                "continuation_day": request.source_receipts["continuation_day"],
                "continuation_native_observation_receipt_sha256": (
                    request.source_receipts["continuation_native_observation_receipt_sha256"]
                ),
                "continuation_native_observation_role": request.source_receipts[
                    "continuation_native_observation_role"
                ],
                "continuation_use_role": request.source_receipts["continuation_use_role"],
                "continuation_creates_target_assignments": False,
                "feature_file_sha256": _file_sha256(request.features_path),
                "continuation_feature_file_sha256": request.source_receipts[
                    "continuation_features_day_file_sha256"
                ],
                "exact_owner_policy_sha256": offline.ACTIVE_OWNER_POLICY_SHA256,
                "exact_owner_predicate_bundle_sha256": (offline.ACTIVE_PREDICATE_BUNDLE_SHA256),
                "exact_owner_private_config_sha256": (offline.ACTIVE_PRIVATE_CONFIG_SHA256),
                "replay_input_receipt_sha256": adapter_result[
                    "replay_input_receipt_sha256"
                ],
                "portable_replay_binding_path": sequential_binding["portable_path"],
                "portable_replay_binding_sha256": sequential_binding["sha256"],
                "portable_day_cache_root": sequential_binding[
                    "portable_day_cache_root"
                ],
                "day_replay_workers": FORMAL_DAY_REPLAY_WORKERS,
                "day_input_sha256": day_input_sha256,
                "market_window_identity_sha256": adapter_result[
                    "market_window_identity_sha256"
                ],
                "model_overlay_identity_sha256": adapter_result[
                    "model_overlay_identity_sha256"
                ],
                "latency_identity_sha256": adapter_result[
                    "latency_identity_sha256"
                ],
                "queue_random_identity_sha256": adapter_result[
                    "queue_random_identity_sha256"
                ],
                "campaign_id": campaign_id,
                "order_id": order_id,
                "exposure_fill_ordinal": exposure_fill_ordinal,
                "assignment_equity_usdc": assignment_equity_usdc,
                **context,
                "d_plus_1_new_target_assignments_allowed": False,
                "target_day_end_terminalized": False,
                "assignment_to_common_washout_required": True,
                "economic_outcomes_read": False,
                "labels_read": False,
                "candidate_actions_generated": False,
            }
        )
    counts = {role: len(rows) for role, rows in output.items()}
    if len(set(counts.values())) != 1 or next(iter(counts.values()), 0) == 0:
        raise OfflinePanelBuilderError("per-day mechanics role row counts drifted")
    return output


def materialize_day(
    inputs: ValidatedPanelInputs,
    day: str,
    *,
    output_root: Path,
    adapter: B0MechanicsReplayAdapter | None = None,
) -> dict[str, Any]:
    """Run exact B0 mechanics and atomically admit one selected UTC day."""

    utc_day = _require_day(day)
    if utc_day not in inputs.selected_days:
        raise OfflinePanelBuilderError("day is outside the canonical selected panel")
    active_adapter = adapter or _load_canonical_adapter()
    if getattr(active_adapter, "identity", None) != CANONICAL_ADAPTER_IDENTITY:
        raise OfflinePanelBuilderError("B0 mechanics adapter identity drifted")
    sequential_binding = _ensure_portable_replay_binding(
        inputs,
        output_root=output_root,
    )
    request = _day_request(inputs, utc_day)
    identity_hashes = _validate_identity_hashes(
        active_adapter.identity_hashes(request), inputs=inputs
    )
    try:
        observation_cache = open_admitted_observation_cache(
            inputs.native_observation_root,
            utc_day,
            deep=False,
        )
        continuation_day = str(request.source_receipts["continuation_day"])
        continuation_cache = open_admitted_observation_cache(
            inputs.native_observation_root,
            continuation_day,
            deep=False,
        )
    except NativeObservationCacheError as exc:
        raise OfflinePanelBuilderError(
            "blocked_missing_canonical_fields: native observation cache failed "
            f"D/D+1 admission: {utc_day}"
        ) from exc
    all_snapshots: list[CooldownAssignmentSnapshotV2] = []
    warmup_cutoff_ts_ns = (
        (date.fromisoformat(utc_day) - date(1970, 1, 1)).days * 86_400 * 1_000_000_000
    )
    continuation_start_ts_ns = warmup_cutoff_ts_ns + 86_400 * 1_000_000_000
    continuation_end_ts_ns = continuation_start_ts_ns + 86_400 * 1_000_000_000
    observations = _stitch_observation_caches(
        observation_cache.observations(),
        continuation_cache.observations_between(
            start_feature_ready_ts_ns=continuation_start_ts_ns,
            end_feature_ready_ts_ns=continuation_end_ts_ns,
        ),
    )
    emitter = CooldownV2ReplayEmitter(
        feature_block="M2",
        observations=observations,
        warmup_cutoff_ts_ns=warmup_cutoff_ts_ns,
        warmup_identity=str(request.source_receipts["native_source_binding_sha256"]),
        identity_hashes=identity_hashes,
        source_cursor_prefixes={
            "market": f"offline-b0-market:{utc_day}",
            "depth": f"offline-b0-depth:{utc_day}",
            "trade": f"offline-b0-trade:{utc_day}",
        },
        snapshot_sink=all_snapshots.append,
        retain_snapshots=False,
    )
    evaluator = _load_owner_evaluator(inputs)
    raw_result = active_adapter.run_day(
        request,
        emitter=emitter,
        evaluator=evaluator,
    )
    adapter_result = _validate_adapter_result(
        raw_result,
        day=utc_day,
        snapshot_count=len(all_snapshots),
    )
    if len(evaluator.decisions) != len(all_snapshots):
        raise OfflinePanelBuilderError("exact owner B0 did not evaluate every snapshot")
    emitter_audit = emitter.audit()
    if (
        emitter_audit.snapshots_emitted != len(all_snapshots)
        or emitter_audit.economic_outcomes_read
    ):
        raise OfflinePanelBuilderError("snapshot emitter audit drifted")
    snapshots = [
        snapshot
        for snapshot in all_snapshots
        if warmup_cutoff_ts_ns
        <= int(snapshot.m0_context.to_dict()["assignment_ts_ns"])
        < continuation_start_ts_ns
    ]
    rows = _project_rows(
        request=request,
        snapshots=snapshots,
        decisions=evaluator.decisions,
        adapter_result=adapter_result,
        sequential_binding=sequential_binding,
    )
    destination = output_root.expanduser().resolve() / utc_day
    if destination.exists():
        raise OfflinePanelBuilderError(f"immutable day already exists: {utc_day}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f".{utc_day}.staging-", dir=destination.parent))
    try:
        file_bindings: dict[str, Any] = {}
        for role in PANEL_ROLES:
            path = stage / f"{role}.parquet"
            _write_parquet(path, rows[role])
            file_bindings[role] = _parquet_binding(path)
        row_ids = [row["opportunity_id"] for row in rows["metadata"]]
        predecessor_opportunity_id_sha256 = _predecessor_day_opportunity_sha256(
            inputs,
            utc_day=utc_day,
            observed_ids=row_ids,
        )
        manifest: dict[str, Any] = {
            "schema_version": DAY_SCHEMA_VERSION,
            "identity": IDENTITY,
            "status": "outcome_blind_b0_mechanics_day_admitted",
            "utc_day": utc_day,
            "panel_role": PANEL_ROLE,
            "queue_identity": QUEUE_IDENTITY,
            "same_millisecond_ambiguity_policy": (SAME_MILLISECOND_AMBIGUITY_POLICY),
            "input_binding_sha256": inputs.input_binding_sha256,
            "adapter_identity": CANONICAL_ADAPTER_IDENTITY,
            "adapter_result_sha256": _canonical_sha256(adapter_result),
            "sequential_replay_input_identity": SEQUENTIAL_REPLAY_INPUT_IDENTITY,
            "portable_replay_binding_path": sequential_binding["portable_path"],
            "portable_replay_binding_sha256": sequential_binding["sha256"],
            "day_input_sha256": sequential_binding["day_projections"][utc_day][
                "projection_receipt_sha256"
            ],
            "observation_end_semantics": (
                "outcome_blind_common_D_plus_1_administrative_bound_v1"
            ),
            "observation_end_ts_ns": _observation_end_ts_ns(utc_day),
            "owner_policy_sha256": offline.ACTIVE_OWNER_POLICY_SHA256,
            "owner_predicate_bundle_sha256": (offline.ACTIVE_PREDICATE_BUNDLE_SHA256),
            "owner_private_config_sha256": offline.ACTIVE_PRIVATE_CONFIG_SHA256,
            "opportunity_count": len(row_ids),
            "replay_context_days": json.loads(request.source_receipts["replay_context_days_json"]),
            "context_source_receipts_sha256": request.source_receipts[
                "context_source_receipts_sha256"
            ],
            "context_book_receipts_sha256": request.source_receipts["context_book_receipts_sha256"],
            "context_feature_receipts_sha256": request.source_receipts[
                "context_feature_receipts_sha256"
            ],
            "native_observation_schema_version": EXPECTED_NATIVE_OBSERVATION_SCHEMA,
            "native_observation_receipt_sha256": request.source_receipts[
                "native_observation_receipt_sha256"
            ],
            "continuation_context_day": continuation_day,
            "continuation_use_role": "continuation_context_for_target",
            "continuation_native_observation_receipt_sha256": request.source_receipts[
                "continuation_native_observation_receipt_sha256"
            ],
            "continuation_owner_decision_count": len(all_snapshots) - len(snapshots),
            "new_target_assignments_from_continuation_day": 0,
            "opportunity_id_sha256": _canonical_sha256(row_ids),
            "predecessor_opportunity_id_sha256": predecessor_opportunity_id_sha256,
            "predecessor_opportunity_ids_matched": (
                predecessor_opportunity_id_sha256 is not None
            ),
            "owner_action_counts": dict(
                sorted(
                    Counter(
                        row["exact_owner_action"] for row in rows["exact_owner_actions"]
                    ).items()
                )
            ),
            "files": file_bindings,
            "permissions": {
                "economic_outcomes_read": False,
                "labels_read": False,
                "candidate_actions_generated": False,
                "action_authorized": False,
                "live_authorized": False,
            },
        }
        manifest["canonical_manifest_sha256"] = _document_sha256(
            manifest, "canonical_manifest_sha256"
        )
        _atomic_json(stage / "manifest.json", manifest)
        os.replace(stage, destination)
        _fsync_directory(destination.parent)
    except Exception:
        shutil.rmtree(stage, ignore_errors=True)
        raise
    try:
        return validate_day(
            destination,
            expected_input_binding=inputs.input_binding_sha256,
        )
    except Exception:
        shutil.rmtree(destination, ignore_errors=True)
        _fsync_directory(destination.parent)
        raise


def validate_day(day_root: Path, *, expected_input_binding: str) -> dict[str, Any]:
    root = day_root.expanduser().resolve()
    manifest = _load_json(root / "manifest.json", label="mechanics day manifest")
    if (
        manifest.get("schema_version") != DAY_SCHEMA_VERSION
        or manifest.get("identity") != IDENTITY
        or manifest.get("canonical_manifest_sha256")
        != _document_sha256(manifest, "canonical_manifest_sha256")
        or manifest.get("input_binding_sha256") != expected_input_binding
        or manifest.get("panel_role") != PANEL_ROLE
        or manifest.get("queue_identity") != QUEUE_IDENTITY
        or manifest.get("same_millisecond_ambiguity_policy") != SAME_MILLISECOND_AMBIGUITY_POLICY
    ):
        raise OfflinePanelBuilderError("mechanics day manifest identity drifted")
    if manifest.get("permissions") != {
        "economic_outcomes_read": False,
        "labels_read": False,
        "candidate_actions_generated": False,
        "action_authorized": False,
        "live_authorized": False,
    }:
        raise OfflinePanelBuilderError("mechanics day permissions drifted")
    target = date.fromisoformat(_require_day(manifest.get("utc_day")))
    expected_context = [
        (target - timedelta(days=1)).isoformat(),
        target.isoformat(),
        (target + timedelta(days=1)).isoformat(),
    ]
    continuation_count = manifest.get("continuation_owner_decision_count")
    new_continuation_assignments = manifest.get("new_target_assignments_from_continuation_day")
    if (
        manifest.get("replay_context_days") != expected_context
        or manifest.get("continuation_context_day") != expected_context[2]
        or manifest.get("continuation_use_role") != "continuation_context_for_target"
        or manifest.get("native_observation_schema_version") != EXPECTED_NATIVE_OBSERVATION_SCHEMA
        or not isinstance(continuation_count, int)
        or isinstance(continuation_count, bool)
        or continuation_count < 0
        or not isinstance(new_continuation_assignments, int)
        or isinstance(new_continuation_assignments, bool)
        or new_continuation_assignments != 0
    ):
        raise OfflinePanelBuilderError("mechanics day continuation contract drifted")
    for field in (
        "context_source_receipts_sha256",
        "context_book_receipts_sha256",
        "context_feature_receipts_sha256",
        "native_observation_receipt_sha256",
        "continuation_native_observation_receipt_sha256",
        "portable_replay_binding_sha256",
        "day_input_sha256",
    ):
        _require_sha(manifest.get(field), label=f"mechanics day {field}")
    if (
        manifest.get("sequential_replay_input_identity")
        != SEQUENTIAL_REPLAY_INPUT_IDENTITY
        or manifest.get("observation_end_semantics")
        != "outcome_blind_common_D_plus_1_administrative_bound_v1"
        or manifest.get("observation_end_ts_ns")
        != _observation_end_ts_ns(str(manifest["utc_day"]))
    ):
        raise OfflinePanelBuilderError("mechanics day sequential-input contract drifted")
    binding_path = root.parent / "_bindings" / PORTABLE_BINDING_FILENAME
    if (
        not binding_path.is_file()
        or _file_sha256(binding_path) != manifest["portable_replay_binding_sha256"]
    ):
        raise OfflinePanelBuilderError("mechanics day portable binding bytes drifted")
    files = manifest.get("files")
    if not isinstance(files, Mapping) or set(files) != set(PANEL_ROLES):
        raise OfflinePanelBuilderError("mechanics day file census drifted")
    row_count: int | None = None
    row_keys: tuple[str, ...] | None = None
    for role in PANEL_ROLES:
        path = root / f"{role}.parquet"
        current = _parquet_binding(path)
        if current != files[role]:
            raise OfflinePanelBuilderError(f"mechanics day {role} bytes drifted")
        table = pq.read_table(path, columns=["utc_day", "opportunity_id"])
        days = tuple(str(value) for value in table["utc_day"].to_pylist())
        keys = tuple(str(value) for value in table["opportunity_id"].to_pylist())
        if any(day != manifest["utc_day"] for day in days) or len(set(keys)) != len(keys):
            raise OfflinePanelBuilderError(f"mechanics day {role} row identity drifted")
        if row_count is None:
            row_count, row_keys = len(keys), keys
        elif len(keys) != row_count or keys != row_keys:
            raise OfflinePanelBuilderError(f"mechanics day {role} row order drifted")
        if role == "metadata":
            metadata = pq.read_table(
                path,
                columns=["observation_end_ts_ns", "campaign_cluster_id"],
            )
            if set(metadata["observation_end_ts_ns"].to_pylist()) != {
                manifest["observation_end_ts_ns"]
            } or any(
                not str(value).startswith("b0-campaign-")
                for value in metadata["campaign_cluster_id"].to_pylist()
            ):
                raise OfflinePanelBuilderError("mechanics metadata v2 fields drifted")
        elif role == "replay_inputs":
            replay = pq.read_table(
                path,
                columns=[
                    "day_input_sha256",
                    "portable_replay_binding_sha256",
                    "d_plus_1_utc_day",
                    "d_plus_1_new_target_assignments_allowed",
                    "target_day_end_terminalized",
                    "assignment_to_common_washout_required",
                    "campaign_id",
                    "order_id",
                    "exposure_fill_ordinal",
                    "assignment_equity_usdc",
                ],
            ).to_pydict()
            expected_d_plus_1 = (target + timedelta(days=1)).isoformat()
            if (
                set(replay["day_input_sha256"]) != {manifest["day_input_sha256"]}
                or set(replay["portable_replay_binding_sha256"])
                != {manifest["portable_replay_binding_sha256"]}
                or set(replay["d_plus_1_utc_day"]) != {expected_d_plus_1}
                or any(replay["d_plus_1_new_target_assignments_allowed"])
                or any(replay["target_day_end_terminalized"])
                or not all(replay["assignment_to_common_washout_required"])
                or any(int(value) <= 0 for value in replay["campaign_id"])
                    or any(int(value) < 0 for value in replay["order_id"])
                or any(int(value) <= 0 for value in replay["exposure_fill_ordinal"])
                or any(not math.isfinite(float(value)) for value in replay["assignment_equity_usdc"])
            ):
                raise OfflinePanelBuilderError("mechanics replay-input v2 fields drifted")
    if row_count != manifest.get("opportunity_count") or _canonical_sha256(
        list(row_keys or ())
    ) != manifest.get("opportunity_id_sha256"):
        raise OfflinePanelBuilderError("mechanics day opportunity census drifted")
    predecessor_sha = manifest.get("predecessor_opportunity_id_sha256")
    if predecessor_sha is not None:
        _require_sha(predecessor_sha, label="predecessor opportunity ID census")
        if (
            manifest.get("predecessor_opportunity_ids_matched") is not True
            or predecessor_sha != manifest.get("opportunity_id_sha256")
        ):
            raise OfflinePanelBuilderError("predecessor opportunity-ID receipt drifted")
    elif manifest.get("predecessor_opportunity_ids_matched") is not False:
        raise OfflinePanelBuilderError("predecessor opportunity-ID scope drifted")
    return manifest


def _progress_payload(
    *,
    inputs: ValidatedPanelInputs,
    workers: int,
    status: str,
    pending: Sequence[str],
    running: Sequence[str],
    completed: Sequence[str],
    reused: Sequence[str],
    failed: Mapping[str, str],
) -> dict[str, Any]:
    return {
        "schema_version": f"{IDENTITY}.progress.v1",
        "identity": IDENTITY,
        "status": status,
        "input_binding_sha256": inputs.input_binding_sha256,
        "total_days": len(inputs.selected_days),
        "observation_context_day_count": len(inputs.observation_context_days),
        "continuation_only_day_count": len(inputs.continuation_only_days),
        "workers": workers,
        "pending_days": list(pending),
        "running_days": list(running),
        "completed_days": list(completed),
        "reused_days": list(reused),
        "failed_days": dict(sorted(failed.items())),
        "updated_at_utc": datetime.now(tz=UTC).isoformat(),
        "economic_outcomes_read": False,
        "labels_read": False,
        "candidate_actions_generated": False,
    }


def _materialize_worker(
    inputs: ValidatedPanelInputs,
    day: str,
    output_root: Path,
) -> dict[str, Any]:
    return materialize_day(inputs, day, output_root=output_root)


def _merged_panel_manifest(
    *,
    inputs: ValidatedPanelInputs,
    day_manifests: Sequence[Mapping[str, Any]],
    panel_root: Path,
) -> dict[str, Any]:
    files = {role: _parquet_binding(panel_root / f"{role}.parquet") for role in PANEL_ROLES}
    manifest: dict[str, Any] = {
        "schema_version": f"{IDENTITY}.merged_panel.v1",
        "identity": IDENTITY,
        "status": "outcome_blind_b0_mechanics_panel_admitted",
        "selected_days": list(inputs.selected_days),
        "selected_day_count": len(inputs.selected_days),
        "observation_context_days": list(inputs.observation_context_days),
        "continuation_only_days": list(inputs.continuation_only_days),
        "replay_context_days": list(inputs.replay_context_days),
        "continuation_days_create_target_assignments": False,
        "native_observation_schema_version": EXPECTED_NATIVE_OBSERVATION_SCHEMA,
        "input_binding_sha256": inputs.input_binding_sha256,
        "sequential_replay_input_identity": SEQUENTIAL_REPLAY_INPUT_IDENTITY,
        "day_manifest_sha256": {
            str(row["utc_day"]): str(row["canonical_manifest_sha256"]) for row in day_manifests
        },
        "files": files,
        "permissions": {
            "economic_outcomes_read": False,
            "labels_read": False,
            "candidate_actions_generated": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    manifest["canonical_manifest_sha256"] = _document_sha256(manifest, "canonical_manifest_sha256")
    return manifest


def _validate_merged_panel(
    panel_root: Path,
    *,
    inputs: ValidatedPanelInputs,
    day_manifests: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    manifest = _load_json(panel_root / "manifest.json", label="merged panel manifest")
    expected = _merged_panel_manifest(
        inputs=inputs,
        day_manifests=day_manifests,
        panel_root=panel_root,
    )
    if manifest != expected:
        raise OfflinePanelBuilderError("merged panel manifest identity drifted")
    expected_ids: tuple[str, ...] | None = None
    for role in PANEL_ROLES:
        path = panel_root / f"{role}.parquet"
        if _parquet_binding(path) != manifest["files"][role]:
            raise OfflinePanelBuilderError(f"merged {role} bytes drifted")
        table = pq.read_table(path, columns=["utc_day", "opportunity_id"])
        days = tuple(str(value) for value in table["utc_day"].to_pylist())
        ids = tuple(str(value) for value in table["opportunity_id"].to_pylist())
        if any(day not in inputs.selected_days for day in days):
            raise OfflinePanelBuilderError(f"merged {role} contains an unselected day")
        if expected_ids is None:
            expected_ids = ids
        elif ids != expected_ids:
            raise OfflinePanelBuilderError("merged five-table row identity drifted")
    if offline.REQUIRED_DAYS == 30:
        if len(expected_ids or ()) != FORMAL_OPPORTUNITY_COUNT:
            raise OfflinePanelBuilderError("formal v2 opportunity denominator is not 3,516")
        predecessor = (
            inputs.project_data_root
            / "cache"
            / "replay_dag"
            / "f05_full_multiscale_successor_offline_panel_builder_v1"
            / "panel"
            / "metadata.parquet"
        )
        if not predecessor.is_file():
            raise OfflinePanelBuilderError("immutable predecessor panel is unavailable")
        prior_ids = tuple(
            str(value)
            for value in pq.read_table(
                predecessor,
                columns=["opportunity_id"],
            )["opportunity_id"].to_pylist()
        )
        if expected_ids != prior_ids or set(expected_ids or ()) != set(prior_ids):
            raise OfflinePanelBuilderError(
                "formal v2 opportunity denominator drifted from predecessor"
            )
    return manifest


def _merge_day_roles(
    *,
    inputs: ValidatedPanelInputs,
    output_root: Path,
    day_manifests: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    destination = output_root / "panel"
    if destination.exists():
        return _validate_merged_panel(
            destination,
            inputs=inputs,
            day_manifests=day_manifests,
        )
    stage = output_root / f".panel.staging-{uuid.uuid4().hex}"
    stage.mkdir(parents=True, exist_ok=False)
    try:
        for role in PANEL_ROLES:
            tables = [
                pq.read_table(output_root / day / f"{role}.parquet") for day in inputs.selected_days
            ]
            merged = pa.concat_tables(tables, promote_options="default")
            path = stage / f"{role}.parquet"
            pq.write_table(merged, path, compression="zstd")
            with path.open("rb") as handle:
                os.fsync(handle.fileno())
        manifest = _merged_panel_manifest(
            inputs=inputs,
            day_manifests=day_manifests,
            panel_root=stage,
        )
        _atomic_json(stage / "manifest.json", manifest)
        os.replace(stage, destination)
        _fsync_directory(output_root)
    except Exception:
        shutil.rmtree(stage, ignore_errors=True)
        raise
    return _validate_merged_panel(
        destination,
        inputs=inputs,
        day_manifests=day_manifests,
    )


def build_selected_days(
    inputs: ValidatedPanelInputs,
    *,
    output_root: Path,
    adapter: B0MechanicsReplayAdapter | None = None,
    workers: int = 1,
) -> dict[str, Any]:
    """Materialize all and only the 30 source-manifest-selected days."""

    if not 1 <= int(workers) <= 8:
        raise OfflinePanelBuilderError("workers must be in [1, 8]")
    if len(inputs.selected_days) != offline.REQUIRED_DAYS:
        raise OfflinePanelBuilderError("formal panel must contain exactly 30 selected days")
    output = output_root.expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    manifest_path = output / "manifest.json"
    if manifest_path.exists():
        return validate_panel(output, inputs=inputs)

    reused: list[str] = []
    completed: list[str] = []
    pending: list[str] = []
    day_rows: dict[str, dict[str, Any]] = {}
    for day in inputs.selected_days:
        day_root = output / day
        if day_root.exists():
            day_rows[day] = validate_day(
                day_root,
                expected_input_binding=inputs.input_binding_sha256,
            )
            reused.append(day)
        else:
            pending.append(day)

    progress_path = output / "progress.json"
    _atomic_json(
        progress_path,
        _progress_payload(
            inputs=inputs,
            workers=int(workers),
            status="running" if pending else "merging",
            pending=pending,
            running=(),
            completed=completed,
            reused=reused,
            failed={},
        ),
    )
    if adapter is not None:
        if workers != 1:
            raise OfflinePanelBuilderError("injected adapter is limited to workers=1")
        for day in tuple(pending):
            day_rows[day] = materialize_day(
                inputs,
                day,
                output_root=output,
                adapter=adapter,
            )
            completed.append(day)
            pending.remove(day)
            _atomic_json(
                progress_path,
                _progress_payload(
                    inputs=inputs,
                    workers=1,
                    status="running" if pending else "merging",
                    pending=pending,
                    running=(),
                    completed=completed,
                    reused=reused,
                    failed={},
                ),
            )
    elif pending and workers == 1:
        for day in tuple(pending):
            day_rows[day] = _materialize_worker(inputs, day, output)
            completed.append(day)
            pending.remove(day)
            _atomic_json(
                progress_path,
                _progress_payload(
                    inputs=inputs,
                    workers=1,
                    status="running" if pending else "merging",
                    pending=pending,
                    running=(),
                    completed=completed,
                    reused=reused,
                    failed={},
                ),
            )
    elif pending:
        failed: dict[str, str] = {}
        queue = list(pending)
        running: dict[concurrent.futures.Future[dict[str, Any]], str] = {}
        with concurrent.futures.ProcessPoolExecutor(max_workers=int(workers)) as executor:
            while queue and len(running) < workers:
                day = queue.pop(0)
                running[executor.submit(_materialize_worker, inputs, day, output)] = day
            while running:
                done, _ = concurrent.futures.wait(
                    running,
                    timeout=30.0,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                for future in done:
                    day = running.pop(future)
                    try:
                        day_rows[day] = future.result()
                    except Exception as exc:
                        failed[day] = f"{type(exc).__name__}: {exc}"
                        for outstanding in running:
                            outstanding.cancel()
                        _atomic_json(
                            progress_path,
                            _progress_payload(
                                inputs=inputs,
                                workers=int(workers),
                                status="failed",
                                pending=queue,
                                running=tuple(running.values()),
                                completed=completed,
                                reused=reused,
                                failed=failed,
                            ),
                        )
                        raise OfflinePanelBuilderError(
                            f"formal day worker failed closed: {day}: {exc}"
                        ) from exc
                    completed.append(day)
                    if queue:
                        next_day = queue.pop(0)
                        running[executor.submit(_materialize_worker, inputs, next_day, output)] = (
                            next_day
                        )
                _atomic_json(
                    progress_path,
                    _progress_payload(
                        inputs=inputs,
                        workers=int(workers),
                        status="running" if running or queue else "merging",
                        pending=queue,
                        running=tuple(running.values()),
                        completed=completed,
                        reused=reused,
                        failed=failed,
                    ),
                )

    days = [day_rows[day] for day in inputs.selected_days]
    merged = _merge_day_roles(
        inputs=inputs,
        output_root=output,
        day_manifests=days,
    )
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": "outcome_blind_b0_mechanics_days_admitted",
        "selected_days": list(inputs.selected_days),
        "selected_day_count": len(inputs.selected_days),
        "observation_context_days": list(inputs.observation_context_days),
        "observation_context_day_count": len(inputs.observation_context_days),
        "continuation_only_days": list(inputs.continuation_only_days),
        "continuation_days_create_target_assignments": False,
        "replay_context_days": list(inputs.replay_context_days),
        "native_observation_schema_version": EXPECTED_NATIVE_OBSERVATION_SCHEMA,
        "input_binding_sha256": inputs.input_binding_sha256,
        "adapter_identity": CANONICAL_ADAPTER_IDENTITY,
        "sequential_replay_input_identity": SEQUENTIAL_REPLAY_INPUT_IDENTITY,
        "day_manifest_sha256": {row["utc_day"]: row["canonical_manifest_sha256"] for row in days},
        "merged_panel_manifest_sha256": merged["canonical_manifest_sha256"],
        "reused_day_count": len(reused),
        "newly_materialized_day_count": len(completed),
        "permissions": {
            "economic_outcomes_read": False,
            "labels_read": False,
            "candidate_actions_generated": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    result["canonical_manifest_sha256"] = _document_sha256(result, "canonical_manifest_sha256")
    _atomic_json(manifest_path, result)
    _atomic_json(
        progress_path,
        _progress_payload(
            inputs=inputs,
            workers=int(workers),
            status="complete",
            pending=(),
            running=(),
            completed=completed,
            reused=reused,
            failed={},
        ),
    )
    return result


def validate_panel(output_root: Path, *, inputs: ValidatedPanelInputs) -> dict[str, Any]:
    root = output_root.expanduser().resolve()
    result = _load_json(root / "manifest.json", label="panel-builder manifest")
    if (
        result.get("schema_version") != SCHEMA_VERSION
        or result.get("identity") != IDENTITY
        or result.get("canonical_manifest_sha256")
        != _document_sha256(result, "canonical_manifest_sha256")
        or result.get("selected_days") != list(inputs.selected_days)
        or result.get("observation_context_days") != list(inputs.observation_context_days)
        or result.get("observation_context_day_count") != len(inputs.observation_context_days)
        or result.get("continuation_only_days") != list(inputs.continuation_only_days)
        or result.get("continuation_days_create_target_assignments") is not False
        or result.get("replay_context_days") != list(inputs.replay_context_days)
        or result.get("native_observation_schema_version") != EXPECTED_NATIVE_OBSERVATION_SCHEMA
        or result.get("input_binding_sha256") != inputs.input_binding_sha256
        or result.get("sequential_replay_input_identity")
        != SEQUENTIAL_REPLAY_INPUT_IDENTITY
    ):
        raise OfflinePanelBuilderError("panel-builder manifest identity drifted")
    days = [
        validate_day(root / day, expected_input_binding=inputs.input_binding_sha256)
        for day in inputs.selected_days
    ]
    merged = _validate_merged_panel(
        root / "panel",
        inputs=inputs,
        day_manifests=days,
    )
    if result.get("merged_panel_manifest_sha256") != merged.get("canonical_manifest_sha256"):
        raise OfflinePanelBuilderError("root/merged panel binding drifted")
    return result


def adapter_preflight() -> Mapping[str, Any]:
    """Report the fixed adapter blocker without reading outcomes."""

    try:
        adapter = _load_canonical_adapter()
    except OfflinePanelBuilderBlocked:
        return {
            "identity": IDENTITY,
            "status": BLOCKED_ADAPTER_STATUS,
            "canonical_adapter_module": CANONICAL_ADAPTER_MODULE,
            "economic_outcomes_read": False,
            "labels_read": False,
            "candidate_actions_generated": False,
        }
    return {
        "identity": IDENTITY,
        "status": "canonical_b0_mechanics_adapter_available",
        "adapter_identity": adapter.identity,
        "economic_outcomes_read": False,
        "labels_read": False,
        "candidate_actions_generated": False,
    }


def _default_cli_paths() -> dict[str, Path]:
    root = Path(__file__).resolve().parents[4]
    data = offline.default_layout().project_data_root
    native_root = data / "cache/replay_dag/f05_full_multiscale_offline_native_observation_v1"
    return {
        "source_manifest": data
        / "reports/f05_full_multiscale_offline_source_audit_v1/canonical_offline_v1/"
        "canonical_source_manifest.json",
        "book_view_root": data / "f05_offline_normalized_book_view_v1/canonical_offline_v2",
        "native_observation_manifest": native_root
        / "_offline_native_observation_batch_manifest.json",
        "native_observation_root": native_root,
        "features_manifest": data
        / "f05_offline_causal_v12_features_only_v2/causal_feature_manifest.json",
        "owner_policy": root / "models/private/f05_boolean_cooldown_owner_v1/policy.json",
        "owner_predicate_bundle": root
        / "models/private/f05_boolean_cooldown_owner_v1/predicate_bundle.json",
        "owner_config": root / "docs/private/live_config.current.local.yaml",
        "output_root": data
        / "cache/replay_dag/f05_full_multiscale_successor_offline_sequential_panel_v2",
    }


def _workers(value: str) -> int:
    workers = int(value)
    if not 1 <= workers <= 8:
        raise argparse.ArgumentTypeError("workers must be in [1, 8]")
    return workers


def _add_bound_input_arguments(parser: argparse.ArgumentParser) -> None:
    defaults = _default_cli_paths()
    parser.add_argument("--source-manifest", type=Path, default=defaults["source_manifest"])
    parser.add_argument("--book-view-root", type=Path, default=defaults["book_view_root"])
    parser.add_argument(
        "--native-observation-manifest",
        type=Path,
        default=defaults["native_observation_manifest"],
    )
    parser.add_argument(
        "--native-observation-root",
        type=Path,
        default=defaults["native_observation_root"],
    )
    parser.add_argument("--features-manifest", type=Path, default=defaults["features_manifest"])
    parser.add_argument("--owner-policy", type=Path, default=defaults["owner_policy"])
    parser.add_argument(
        "--owner-predicate-bundle",
        type=Path,
        default=defaults["owner_predicate_bundle"],
    )
    parser.add_argument("--owner-config", type=Path, default=defaults["owner_config"])


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    preflight = subparsers.add_parser("preflight")
    _add_bound_input_arguments(preflight)
    build_day = subparsers.add_parser("build-day")
    _add_bound_input_arguments(build_day)
    build_day.add_argument("--day", required=True)
    build_day.add_argument(
        "--output-root",
        type=Path,
        default=_default_cli_paths()["output_root"],
    )
    build = subparsers.add_parser("build")
    _add_bound_input_arguments(build)
    build.add_argument(
        "--output-root",
        type=Path,
        default=_default_cli_paths()["output_root"],
    )
    build.add_argument("--workers", type=_workers, default=1)
    validate = subparsers.add_parser("validate")
    _add_bound_input_arguments(validate)
    validate.add_argument(
        "--output-root",
        type=Path,
        default=_default_cli_paths()["output_root"],
    )
    return parser.parse_args(argv)


def _inputs_from_args(args: argparse.Namespace) -> ValidatedPanelInputs:
    return validate_inputs(
        source_manifest_path=args.source_manifest,
        book_view_root=args.book_view_root,
        native_observation_manifest_path=args.native_observation_manifest,
        native_observation_root=args.native_observation_root,
        features_manifest_path=args.features_manifest,
        owner_artifacts=OwnerArtifactPaths(
            policy=args.owner_policy,
            predicate_bundle=args.owner_predicate_bundle,
            private_config=args.owner_config,
        ),
    )


def preflight_inputs(inputs: ValidatedPanelInputs) -> dict[str, Any]:
    adapter = _load_canonical_adapter()
    day_receipts: dict[str, Any] = {}
    for day in inputs.selected_days:
        request = _day_request(inputs, day)
        hashes = _validate_identity_hashes(adapter.identity_hashes(request), inputs=inputs)
        preflight_day = getattr(adapter, "preflight_day", None)
        day_receipts[day] = (
            preflight_day(request)
            if callable(preflight_day)
            else {"status": "adapter_identity_hashes_valid", "identity_hashes": hashes}
        )
    return {
        "schema_version": f"{IDENTITY}.preflight.v1",
        "identity": IDENTITY,
        "status": "ready_for_outcome_blind_panel_materialization",
        "selected_days": list(inputs.selected_days),
        "selected_day_count": len(inputs.selected_days),
        "input_binding_sha256": inputs.input_binding_sha256,
        "adapter_identity": adapter.identity,
        "day_receipts": day_receipts,
        "economic_outcomes_read": False,
        "labels_read": False,
        "candidate_actions_generated": False,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        inputs = _inputs_from_args(args)
        if args.command == "preflight":
            result = preflight_inputs(inputs)
        elif args.command == "build-day":
            result = materialize_day(
                inputs,
                args.day,
                output_root=args.output_root,
            )
        elif args.command == "build":
            result = build_selected_days(
                inputs,
                output_root=args.output_root,
                workers=args.workers,
            )
        else:
            result = validate_panel(args.output_root, inputs=inputs)
    except Exception as exc:
        blocked = getattr(exc, "as_dict", None)
        payload = (
            blocked()
            if callable(blocked)
            else {
                "identity": IDENTITY,
                "status": "failed_closed",
                "error_type": type(exc).__name__,
                "error": str(exc),
                "economic_outcomes_read": False,
                "labels_read": False,
                "candidate_actions_generated": False,
            }
        )
        print(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False))
        return 2
    print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))
    return 0


__all__ = [
    "ADAPTER_RESULT_SCHEMA",
    "BLOCKED_ADAPTER_STATUS",
    "B0MechanicsReplayAdapter",
    "CANONICAL_ADAPTER_IDENTITY",
    "DayMaterializationRequest",
    "IDENTITY",
    "OfflinePanelBuilderBlocked",
    "OfflinePanelBuilderError",
    "OwnerArtifactPaths",
    "PANEL_ROLE",
    "QUEUE_IDENTITY",
    "ValidatedPanelInputs",
    "adapter_preflight",
    "build_selected_days",
    "main",
    "materialize_day",
    "parse_args",
    "preflight_inputs",
    "validate_day",
    "validate_inputs",
    "validate_panel",
]


if __name__ == "__main__":
    raise SystemExit(main())
