#!/usr/bin/env python3
"""Build the concrete 71-day F03 current-v9 10s control artifact.

This module is deliberately disjoint from the daily and continuous runners.
It performs outcome-blind data binding only: the frozen 71-day source plan,
model-free market windows, the deployed v12/P3/Feature-DAG identity, the v9
10-second overlays, latency, and a common restart-safe initial state.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import pickle
import shutil
import tempfile
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from data_paths import data_root, external_cache_root, window_cache_root
from models import backtest_tick as bt
from models import data_windows
from models.backtest_config import load_operational_baseline_binding
from models.replay import narrowgate_continuous_tick_adapter as continuous_adapter
from models.replay.replay_state_checkpoint import ContinuousReplayState
from models.replay.restart_aware_continuous_ab import canonical_sha256
from models.replay_cache_components import (
    component_directory,
    file_reference,
    load_model_overlay,
    model_overlay_identity,
    references_sha256,
    write_model_overlay,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_dual_overlay_ml_ab_replay as dual_abi,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_native_40day_full_path_ml_ab as native_runner,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_restart_aware_continuous_execution_v1_2 as framework,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_restart_aware_continuous_execution_v1_3 as concrete,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_v9_10s_control_overlay_repair as control_repair,
)
from research.governance.public_machine_projection import source_identity_sha256

ROOT = Path(__file__).resolve().parents[4]
IDENTITY = "causal_v12_current_v9_10s_control_71d_policy_artifact_builder_v1"
ADMISSION_SCHEMA_VERSION = f"{IDENTITY}.admission"
INITIAL_STATE_SCHEMA_VERSION = f"{IDENTITY}.initial_state"
OVERLAY_INDEX_SCHEMA_VERSION = f"{IDENTITY}.overlay_index"
WINDOW_RECEIPT_SCHEMA_VERSION = f"{IDENTITY}.market_window"
OFFLINE_CONFIG_SCHEMA_VERSION = f"{IDENTITY}.offline_execution_config"
ASOF_FEATURE_SCHEMA_VERSION = f"{IDENTITY}.asof_feature_projection"
POLICY_IDENTITY = "current_v9_10s_control_via_v10_observability_only_successor_71d_v1"

EXPECTED_DAYS = framework.EXPECTED_DAYS
CONTROL_ARM = "control"
V12_MANIFEST = "control-policy-v1.2.json"
V13_MANIFEST = "control-policy-v1.3.json"
OVERLAY_INDEX = "control-overlay-index.json"
INITIAL_STATE = "common-initial-state.json"
ADMISSION_MARKER = "_SUCCESS"
WINDOW_RECEIPT_SUFFIX = ".receipt.json"
OFFLINE_CONFIG = "current-operational-offline-execution-projection.yaml"
OFFLINE_CONFIG_RECEIPT = "current-operational-offline-execution-projection.json"
ARCHIVED_FEATURE_DAG_ENV = "NARROWGATE_ARCHIVED_FEATURE_DAG"

DEFAULT_FRAMEWORK_PLAN = framework.DEFAULT_OUTPUT_ROOT / framework.PLAN_FILENAME
DEFAULT_FORMAL_40_PLAN = external_cache_root(ROOT) / (
    "replay_dag/"
    "f03_causal_v12_1s_native_40day_full_path_ml_ab_v3/execution-plan.json"
)
DEFAULT_CONTROL_PANEL = external_cache_root(ROOT) / (
    "f03_v9_10s_control_overlay_repair_v1/"
    "control_overlay_panel_admission_v1_1/panel-manifest.json"
)
DEFAULT_LATENCY_PROFILE = data_root(ROOT) / (
    "reports/"
    "formal_recalibration_20260715/"
    "ec2_aws_tokyo_2vcpu4g_20260710_14_rest_latency.csv.gz"
)
DEFAULT_ENGINE_STATE_SCHEMA = ROOT / (
    "research/shared/replay_lifecycle/docs/continuous_replay_state_v1_contract.json"
)
DEFAULT_ARCHIVED_FEATURE_DAG = Path(
    os.environ.get(
        ARCHIVED_FEATURE_DAG_ENV,
        data_root(ROOT)
        / "remote_live_retirement"
        / "owner_private_original_aws_epoch_20260811"
        / "runtime_source/features/feature_dag.py",
    )
).expanduser().resolve(strict=False)
DEFAULT_HISTORICAL_V9_IDENTITY = ROOT / (
    "research/families/f10_live_replay_attribution/docs/"
    "operational_baseline_identity_20260804_v9.json"
)
DEFAULT_LOCAL_WINDOW_CACHE = window_cache_root(ROOT)
DEFAULT_CACHE_ROOT = external_cache_root(ROOT) / (
    "replay_dag/"
    "f03_current_v9_10s_control_71d_policy_artifact_v1"
)
DEFAULT_TEMP_ADMISSION = Path(tempfile.gettempdir()).resolve() / (
    "f03_current_v9_10s_control_71d_policy_artifact_v1"
)
DEFAULT_DURABLE_ADMISSION = DEFAULT_CACHE_ROOT / "policy-admission"
DEFAULT_CONCRETE_PLAN_ROOT = concrete.DEFAULT_OUTPUT_ROOT


class F03ControlArtifactBuildError(RuntimeError):
    """Raised when a concrete control input cannot be recovered exactly."""


@dataclass(frozen=True, slots=True)
class BuilderPaths:
    framework_plan: Path = DEFAULT_FRAMEWORK_PLAN
    formal_40_plan: Path = DEFAULT_FORMAL_40_PLAN
    control_panel: Path = DEFAULT_CONTROL_PANEL
    latency_profile: Path = DEFAULT_LATENCY_PROFILE
    engine_state_schema: Path = DEFAULT_ENGINE_STATE_SCHEMA
    archived_feature_dag: Path = DEFAULT_ARCHIVED_FEATURE_DAG
    historical_v9_identity: Path = DEFAULT_HISTORICAL_V9_IDENTITY
    cache_root: Path = DEFAULT_CACHE_ROOT
    local_window_cache: Path = DEFAULT_LOCAL_WINDOW_CACHE


DEFAULT_PATHS = BuilderPaths()


def _load_json(path: Path, *, role: str) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise F03ControlArtifactBuildError(f"missing {role}: {resolved}")
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise F03ControlArtifactBuildError(f"invalid {role}: {resolved}") from exc
    if not isinstance(payload, dict):
        raise F03ControlArtifactBuildError(f"{role} must be a JSON object")
    return payload


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _artifact(
    path: Path,
    *,
    role: str,
    expected_sha256: str | None = None,
    expected_size: int | None = None,
) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise F03ControlArtifactBuildError(f"missing {role}: {resolved}")
    size = resolved.stat().st_size
    if size <= 0:
        raise F03ControlArtifactBuildError(f"empty {role}: {resolved}")
    if expected_size is not None and size != int(expected_size):
        raise F03ControlArtifactBuildError(f"{role} size drift")
    observed = _file_sha256(resolved)
    identity_sha256 = observed
    if expected_sha256 is not None and observed != expected_sha256:
        identity_sha256 = source_identity_sha256(resolved)
        if identity_sha256 != expected_sha256:
            raise F03ControlArtifactBuildError(f"{role} SHA256 drift")
    return {"path": str(resolved), "sha256": identity_sha256, "size_bytes": size}


def _artifact_from_row(row: Mapping[str, Any], *, role: str) -> dict[str, Any]:
    return _artifact(
        Path(str(row.get("path", ""))),
        role=role,
        expected_sha256=str(row.get("sha256", "")),
        expected_size=int(row.get("size_bytes", -1)),
    )


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.partial"
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_text(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.partial"
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_pickle(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.partial"
    try:
        with temporary.open("wb") as handle:
            pickle.dump(payload, handle, protocol=pickle.HIGHEST_PROTOCOL)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_copy(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.parent / f".{target.name}.{uuid.uuid4().hex}.partial"
    try:
        with source.open("rb") as read_handle, temporary.open("wb") as write_handle:
            shutil.copyfileobj(read_handle, write_handle, length=8 << 20)
            write_handle.flush()
            os.fsync(write_handle.fileno())
        os.replace(temporary, target)
        _fsync_directory(target.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_link_or_copy(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.parent / f".{target.name}.{uuid.uuid4().hex}.partial"
    try:
        try:
            os.link(source, temporary)
        except OSError:
            with source.open("rb") as read_handle, temporary.open("wb") as write_handle:
                shutil.copyfileobj(read_handle, write_handle, length=8 << 20)
                write_handle.flush()
                os.fsync(write_handle.fileno())
        os.replace(temporary, target)
        _fsync_directory(target.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _prior_day(day: str) -> str:
    return (date.fromisoformat(day) - timedelta(days=1)).isoformat()


def _source_identity(row: Mapping[str, Any]) -> str:
    required = (
        "day",
        "book_identity",
        "book_root",
        "feature_identity",
        "exact_queue_authority",
        "exact_lifecycle_authority",
        "continuous_economic_sensitivity_authority",
        "artifacts",
    )
    if any(key not in row for key in required):
        raise F03ControlArtifactBuildError(f"incomplete frozen source row: {row.get('day')}")
    return canonical_sha256({key: row[key] for key in required})


def _source_profile(row: Mapping[str, Any]) -> str:
    identity = str(row.get("book_identity", ""))
    if identity == "native_available":
        return "native"
    if identity == "provider_normalized_sensitivity":
        return "provider_normalized"
    raise F03ControlArtifactBuildError(
        f"unsupported 71-day source identity on {row.get('day')}: {identity}"
    )


def load_frozen_sources(
    framework_plan: Path,
    *,
    verify_source_hashes: bool = False,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    plan = concrete.load_historical_framework_plan(framework_plan.expanduser().resolve())
    if verify_source_hashes:
        for row in (plan.get("continuous_plan") or {}).get("source_bindings") or []:
            for artifact in row.get("artifacts", ()):
                _artifact(artifact, role=f"source {row.get('day')} {artifact.get('role')}")
    rows = (plan.get("continuous_plan") or {}).get("source_bindings") or []
    if tuple(str(row.get("day", "")) for row in rows) != EXPECTED_DAYS:
        raise F03ControlArtifactBuildError("F03 source plan lost the exact ordered 71 days")
    by_day: dict[str, dict[str, Any]] = {}
    for raw in rows:
        row = dict(raw)
        day = str(row["day"])
        row["source_profile"] = _source_profile(row)
        row["source_identity_sha256"] = _source_identity(row)
        artifacts = row.get("artifacts")
        if not isinstance(artifacts, list):
            raise F03ControlArtifactBuildError(
                f"{day} frozen source artifacts must remain an ordered list"
            )
        by_role = {
            str(artifact.get("role", "")): dict(artifact)
            for artifact in artifacts
            if isinstance(artifact, Mapping)
        }
        if set(by_role) != {"bbo", "l2", "feature"} or len(by_role) != len(artifacts):
            raise F03ControlArtifactBuildError(
                f"{day} frozen source artifacts lack unique bbo/l2/feature roles"
            )
        row["artifacts_by_role"] = by_role
        by_day[day] = row
    profiles = [row["source_profile"] for row in by_day.values()]
    if profiles.count("native") != 52 or profiles.count("provider_normalized") != 19:
        raise F03ControlArtifactBuildError("F03 71-day native/provider strata drifted")
    return plan, by_day


def _model_bundle_binding(model_dir: Path) -> dict[str, Any]:
    resolved = model_dir.expanduser().resolve()
    bundle_meta = _artifact(resolved / "bundle_meta.json", role="v12 bundle_meta")
    bundle_payload = _load_json(Path(bundle_meta["path"]), role="v12 bundle meta")
    targets = tuple(str(value) for value in bundle_payload.get("targets", ()))
    if targets != control_repair.EXPECTED_HEADS:
        raise F03ControlArtifactBuildError("current v12 bundle is not the ordered 13-head ABI")
    artifacts: list[dict[str, Any]] = [bundle_meta]
    for head in targets:
        artifacts.append(_artifact(resolved / f"{head}.txt", role=f"v12 {head} model"))
        artifacts.append(_artifact(resolved / f"{head}_meta.json", role=f"v12 {head} metadata"))
    signatures = data_windows._model_artifact_signatures(resolved)  # noqa: SLF001
    references = list(data_windows._signature_references(signatures))  # noqa: SLF001
    for reference in references:
        source_sha256 = source_identity_sha256(reference["locator"]["path"])
        if source_sha256 != reference["sha256"]:
            reference["sha256"] = source_sha256
            reference["hash_provenance"] = {
                "kind": "public_projection_registered_source_identity"
            }
    content_identity = references_sha256(references)
    return {
        "directory": str(resolved),
        "bundle_meta": bundle_meta,
        "artifacts": artifacts,
        "content_identity_sha256": content_identity,
        "reference_identity": tuple(references),
    }


def _load_yaml_mapping(path: Path, *, role: str) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise F03ControlArtifactBuildError(f"missing {role}: {resolved}")
    try:
        payload = yaml.safe_load(resolved.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise F03ControlArtifactBuildError(f"invalid {role}: {resolved}") from exc
    if not isinstance(payload, dict):
        raise F03ControlArtifactBuildError(f"{role} must be a YAML mapping")
    return payload


def _mapping_differences(
    left: Any,
    right: Any,
    *,
    prefix: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    if isinstance(left, Mapping) and isinstance(right, Mapping):
        differences: list[dict[str, Any]] = []
        for key in sorted(set(left) | set(right), key=str):
            child = (*prefix, str(key))
            if key not in left:
                differences.append(
                    {"path": ".".join(child), "before": None, "after": right[key]}
                )
            elif key not in right:
                differences.append(
                    {"path": ".".join(child), "before": left[key], "after": None}
                )
            else:
                differences.extend(
                    _mapping_differences(left[key], right[key], prefix=child)
                )
        return differences
    if left != right:
        return [{"path": ".".join(prefix), "before": left, "after": right}]
    return []


def _validate_control_config_semantics(payload: Mapping[str, Any]) -> None:
    strategy = payload.get("strategy")
    ml = payload.get("ml")
    if not isinstance(strategy, Mapping) or not isinstance(ml, Mapping):
        raise F03ControlArtifactBuildError("operational config lacks strategy/ml mappings")
    if ml.get("enabled") is not True:
        raise F03ControlArtifactBuildError("operational control does not enable causal-v12")
    if strategy.get("dynamic_fill_hazard_action_enabled") is not False:
        raise F03ControlArtifactBuildError("operational control enables q90 action")
    if strategy.get("buy_fill_selection_live_enabled") is not False:
        raise F03ControlArtifactBuildError("operational control enables BUY fill selection")


def _offline_projection_payload(
    raw_payload: Mapping[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    _validate_control_config_semantics(raw_payload)
    projected = copy.deepcopy(dict(raw_payload))
    journal = projected.get("lifecycle_journal_v2")
    if not isinstance(journal, dict):
        raise F03ControlArtifactBuildError(
            "operational config lacks lifecycle_journal_v2 mapping"
        )
    if journal.get("enabled") is not True:
        raise F03ControlArtifactBuildError(
            "current operational config no longer has the expected remote journal writer"
        )
    journal["enabled"] = False
    differences = _mapping_differences(raw_payload, projected)
    expected = [
        {
            "path": "lifecycle_journal_v2.enabled",
            "before": True,
            "after": False,
        }
    ]
    if differences != expected:
        raise F03ControlArtifactBuildError(
            "offline execution projection changes more than the host-only journal writer"
        )
    _validate_control_config_semantics(projected)
    return projected, differences


def _materialize_offline_execution_config(
    *,
    raw_config: Mapping[str, Any],
    cache_root: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    raw_artifact = _artifact_from_row(raw_config, role="raw operational config")
    raw_payload = _load_yaml_mapping(Path(raw_artifact["path"]), role="raw operational config")
    projected, differences = _offline_projection_payload(raw_payload)
    metadata_root = cache_root.expanduser().resolve() / "metadata"
    config_path = metadata_root / OFFLINE_CONFIG
    rendered = (
        "# Generated outcome-blind offline execution projection.\n"
        "# The raw operational config remains separately hash-bound.\n"
        + yaml.safe_dump(
            projected,
            sort_keys=False,
            default_flow_style=False,
            allow_unicode=False,
        )
    )
    _atomic_text(config_path, rendered)
    reloaded = _load_yaml_mapping(config_path, role="offline execution projection")
    observed_differences = _mapping_differences(raw_payload, reloaded)
    if observed_differences != differences:
        raise F03ControlArtifactBuildError("offline execution projection YAML drifted")
    projection_artifact = _artifact(config_path, role="offline execution projection")
    params = native_runner._load_formal_base_params(config_path)  # noqa: SLF001
    if (
        params.get("ml_enabled") is not True
        or params.get("dynamic_fill_hazard_action_enabled") is not False
        or params.get("buy_fill_selection_live_enabled") is not False
    ):
        raise F03ControlArtifactBuildError(
            "offline execution projection changed the control policy semantics"
        )
    receipt_payload = {
        "schema_version": OFFLINE_CONFIG_SCHEMA_VERSION,
        "identity": IDENTITY,
        "raw_operational_config": raw_artifact,
        "offline_execution_config": projection_artifact,
        "structured_differences": differences,
        "journal_writer_disabled_for_offline_materialization": True,
        "quote_policy_change": False,
        "economic_outcomes_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    receipt_payload["projection_identity_sha256"] = canonical_sha256(receipt_payload)
    receipt_path = metadata_root / OFFLINE_CONFIG_RECEIPT
    _atomic_json(receipt_path, receipt_payload)
    receipt_artifact = _artifact(receipt_path, role="offline config projection receipt")
    validate_offline_execution_config(
        raw_config=raw_artifact,
        offline_config=projection_artifact,
        receipt=receipt_artifact,
    )
    return projection_artifact, receipt_artifact, receipt_payload


def validate_offline_execution_config(
    *,
    raw_config: Mapping[str, Any],
    offline_config: Mapping[str, Any],
    receipt: Mapping[str, Any],
) -> dict[str, Any]:
    raw_artifact = _artifact_from_row(raw_config, role="raw operational config")
    offline_artifact = _artifact_from_row(
        offline_config, role="offline execution projection"
    )
    receipt_artifact = _artifact_from_row(
        receipt, role="offline config projection receipt"
    )
    raw_payload = _load_yaml_mapping(
        Path(raw_artifact["path"]), role="raw operational config"
    )
    offline_payload = _load_yaml_mapping(
        Path(offline_artifact["path"]), role="offline execution projection"
    )
    _, expected_differences = _offline_projection_payload(raw_payload)
    if _mapping_differences(raw_payload, offline_payload) != expected_differences:
        raise F03ControlArtifactBuildError("offline execution config semantic drift")
    receipt_payload = _load_json(
        Path(receipt_artifact["path"]), role="offline config projection receipt"
    )
    observed_identity = str(receipt_payload.pop("projection_identity_sha256", ""))
    if (
        receipt_payload.get("schema_version") != OFFLINE_CONFIG_SCHEMA_VERSION
        or canonical_sha256(receipt_payload) != observed_identity
        or receipt_payload.get("structured_differences") != expected_differences
        or receipt_payload.get("quote_policy_change") is not False
        or receipt_payload.get("journal_writer_disabled_for_offline_materialization")
        is not True
    ):
        raise F03ControlArtifactBuildError("offline config projection receipt drifted")
    if (
        receipt_payload.get("raw_operational_config") != raw_artifact
        or receipt_payload.get("offline_execution_config") != offline_artifact
    ):
        raise F03ControlArtifactBuildError("offline config projection artifacts drifted")
    params = native_runner._load_formal_base_params(  # noqa: SLF001
        Path(offline_artifact["path"])
    )
    if (
        params.get("ml_enabled") is not True
        or params.get("dynamic_fill_hazard_action_enabled") is not False
        or params.get("buy_fill_selection_live_enabled") is not False
    ):
        raise F03ControlArtifactBuildError("offline config projection is not control-only")
    return {
        "raw_operational_config": raw_artifact,
        "offline_execution_config": offline_artifact,
        "receipt": receipt_artifact,
        "projection_identity_sha256": observed_identity,
        "structured_differences": expected_differences,
    }


def load_operational_projection(
    paths: BuilderPaths,
    *,
    materialize_offline_config: bool = False,
) -> dict[str, Any]:
    """Bind current bytes while proving the v10 successor changed observation only."""

    binding = load_operational_baseline_binding(root=ROOT)
    if binding is None or not bool(binding.get("config_exists")):
        raise F03ControlArtifactBuildError("current operational baseline binding is absent")
    pointer = dict(binding["pointer"])
    identity = dict(binding["identity"])
    predecessor = identity.get("predecessor") or {}
    backtest = identity.get("backtest_baseline") or {}
    if (
        predecessor.get("baseline_id") != framework.parent.one_second_replay.EXPECTED_BASELINE_ID
        or backtest.get("economic_path_equivalent_to_predecessor") is not True
        or pointer.get("ml_enabled") is not True
        or pointer.get("dynamic_fill_hazard_action_enabled") is not False
        or pointer.get("buy_fill_selection_live_enabled") is not False
    ):
        raise F03ControlArtifactBuildError(
            "current successor does not prove current-v9 economic-path equivalence"
        )
    historical = _artifact(
        paths.historical_v9_identity,
        role="historical v9 identity",
        expected_sha256=str(predecessor.get("identity_sha256", "")),
    )
    historical_payload = _load_json(Path(historical["path"]), role="historical v9 identity")
    if historical_payload.get("baseline_id") != predecessor.get("baseline_id"):
        raise F03ControlArtifactBuildError("historical v9 predecessor identity drifted")

    config = _artifact(
        Path(binding["config_path"]),
        role="current operational config",
        expected_sha256=str(pointer.get("live_config_sha256", "")),
    )
    raw_config_payload = _load_yaml_mapping(
        Path(config["path"]), role="current operational config"
    )
    _, structured_differences = _offline_projection_payload(raw_config_payload)
    baseline_identity = _artifact(
        Path(binding["identity_path"]),
        role="current operational identity",
        expected_sha256=str(pointer.get("identity_sha256", "")),
    )
    model = _model_bundle_binding(Path(binding["model_path"]))
    declared_model = identity.get("model") or {}
    if model["bundle_meta"]["sha256"] != declared_model.get("bundle_meta_sha256"):
        raise F03ControlArtifactBuildError("current model bundle SHA256 drifted")

    p3_payload = identity.get("p3") or {}
    p3_path = Path(str(p3_payload.get("path", "")))
    if not p3_path.is_absolute():
        p3_path = ROOT / p3_path
    p3 = _artifact(
        p3_path,
        role="current P3 v2 artifact",
        expected_sha256=str(p3_payload.get("sha256", "")),
    )
    if p3_payload.get("horizon_s") != 10.0 or p3_payload.get("event_type") != "touch":
        raise F03ControlArtifactBuildError("current control no longer binds the v9 P3 ABI")

    declared_dag_sha = str((identity.get("runtime_code") or {}).get("features/feature_dag.py", ""))
    feature_dag = _artifact(
        paths.archived_feature_dag,
        role="deployed Feature DAG",
        expected_sha256=declared_dag_sha,
    )
    workspace_dag = _artifact(ROOT / "features/feature_dag.py", role="workspace Feature DAG")
    offline_config: dict[str, Any] | None = None
    offline_receipt: dict[str, Any] | None = None
    projection_receipt_payload: dict[str, Any] | None = None
    if materialize_offline_config:
        offline_config, offline_receipt, projection_receipt_payload = (
            _materialize_offline_execution_config(
                raw_config=config,
                cache_root=paths.cache_root,
            )
        )
    return {
        "pointer": _artifact(Path(binding["pointer_path"]), role="operational pointer"),
        "baseline_identity": baseline_identity,
        "historical_v9_identity": historical,
        "raw_operational_config": config,
        "operational_config": offline_config,
        "offline_config_receipt": offline_receipt,
        "offline_config_projection": projection_receipt_payload,
        "offline_config_structured_differences": structured_differences,
        "model": model,
        "p3": p3,
        "feature_dag": feature_dag,
        "workspace_feature_dag_observation": workspace_dag,
        "current_baseline_id": str(pointer["baseline_id"]),
        "historical_v9_baseline_id": str(predecessor["baseline_id"]),
        "semantic_feature_dag_sha256": str(declared_model.get("feature_dag_sha256", "")),
        "projection_reason": "v10_observability_retirement_no_quote_policy_change",
    }


def _validate_formal_40_plan(path: Path) -> dict[str, dict[str, Any]]:
    resolved = path.expanduser().resolve()
    plan = _load_json(resolved, role="formal F03 40-day plan")
    marker = resolved.parent / native_runner.PLAN_SUCCESS
    if not marker.is_file() or marker.read_text(encoding="ascii").strip() != _file_sha256(
        resolved
    ):
        raise F03ControlArtifactBuildError("formal 40-day plan atomic marker drifted")
    identity_payload = plan.get("identity_payload")
    if not isinstance(identity_payload, Mapping) or plan.get(
        "plan_identity_sha256"
    ) != canonical_sha256(identity_payload):
        raise F03ControlArtifactBuildError("formal 40-day plan identity drifted")
    rows = identity_payload.get("days") or []
    if len(rows) != 40:
        raise F03ControlArtifactBuildError("formal F03 plan is not the 40-day anchor")
    by_day: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise F03ControlArtifactBuildError("formal 40-day plan has an invalid day row")
        day = str(row.get("utc_day", ""))
        window = row.get("window")
        if day not in EXPECTED_DAYS or not isinstance(window, Mapping):
            raise F03ControlArtifactBuildError("formal 40-day window binding is invalid")
        by_day[day] = dict(window)
    if len(by_day) != 40:
        raise F03ControlArtifactBuildError("formal 40-day plan contains duplicate days")
    return by_day


def _expected_window_authority(source_profile: str) -> str:
    if source_profile == "native":
        return "native_formal_lifecycle"
    if source_profile == "provider_normalized":
        return "provider_normalized_causal"
    raise F03ControlArtifactBuildError(f"unsupported source profile: {source_profile}")


def _validate_window_object(window: Any, *, day: str, source_profile: str) -> None:
    if getattr(window, "ml_data", None) is not None:
        raise F03ControlArtifactBuildError(f"{day} market window is not model-free")
    if getattr(window, "book_source_authority", "") != _expected_window_authority(
        source_profile
    ):
        raise F03ControlArtifactBuildError(f"{day} market window source authority drifted")
    trades = getattr(window, "trades", None)
    if trades is None or len(trades) == 0:
        raise F03ControlArtifactBuildError(f"{day} market window lacks execution trades")
    if getattr(window, "bbo_data", None) is None or getattr(window, "l2_data", None) is None:
        raise F03ControlArtifactBuildError(f"{day} market window lacks BBO/L2 state")


def validate_market_window(
    artifact: Mapping[str, Any],
    *,
    day: str,
    source_profile: str,
    loader: Callable[[Path], Any] | None = None,
) -> dict[str, Any]:
    receipt = _artifact_from_row(artifact, role=f"{day} market window")
    load = loader
    if load is None:

        def load(path: Path) -> Any:
            with path.open("rb") as handle:
                return pickle.load(handle)

    window = load(Path(receipt["path"]))
    _validate_window_object(window, day=day, source_profile=source_profile)
    return receipt


def _window_target(cache_root: Path, *, day: str, source_identity_sha256: str) -> Path:
    return (
        cache_root.expanduser().resolve()
        / "market_windows"
        / day
        / f"btcusdc_{day}_{source_identity_sha256[:16]}_model_free.pkl"
    )


def _window_receipt_path(window_path: Path) -> Path:
    return window_path.with_suffix(window_path.suffix + WINDOW_RECEIPT_SUFFIX)


def _load_window_receipt(
    window_path: Path,
    *,
    day: str,
    source_identity_sha256: str,
    source_profile: str,
    deep: bool,
) -> dict[str, Any] | None:
    receipt_path = _window_receipt_path(window_path)
    if not window_path.is_file() or not receipt_path.is_file():
        return None
    payload = _load_json(receipt_path, role=f"{day} market-window receipt")
    identity_payload = dict(payload)
    observed_identity = str(identity_payload.pop("receipt_identity_sha256", ""))
    if (
        payload.get("schema_version") != WINDOW_RECEIPT_SCHEMA_VERSION
        or payload.get("day") != day
        or payload.get("source_identity_sha256") != source_identity_sha256
        or payload.get("source_profile") != source_profile
        or canonical_sha256(identity_payload) != observed_identity
    ):
        raise F03ControlArtifactBuildError(f"{day} market-window receipt identity drifted")
    raw_artifact = payload.get("market_window") or {}
    if deep:
        artifact = _artifact_from_row(raw_artifact, role=f"{day} market window")
    else:
        window_file = Path(str(raw_artifact.get("path", ""))).expanduser().resolve()
        expected_size = int(raw_artifact.get("size_bytes", -1))
        expected_sha = str(raw_artifact.get("sha256", ""))
        if (
            not window_file.is_file()
            or expected_size <= 0
            or window_file.stat().st_size != expected_size
            or len(expected_sha) != 64
        ):
            raise F03ControlArtifactBuildError(
                f"{day} market-window shallow receipt drifted"
            )
        artifact = {
            "path": str(window_file),
            "sha256": expected_sha,
            "size_bytes": expected_size,
        }
    if Path(artifact["path"]) != window_path.resolve():
        raise F03ControlArtifactBuildError(f"{day} market-window receipt path drifted")
    if deep:
        validate_market_window(artifact, day=day, source_profile=source_profile)
    return artifact


def _publish_window_receipt(
    *,
    window_path: Path,
    day: str,
    source: Mapping[str, Any],
    build_mode: str,
    input_window: Mapping[str, Any] | None,
    provider_authority_view: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    artifact = _artifact(window_path, role=f"{day} admitted market window")
    payload = {
        "schema_version": WINDOW_RECEIPT_SCHEMA_VERSION,
        "identity": IDENTITY,
        "day": day,
        "source_profile": source["source_profile"],
        "source_identity_sha256": source["source_identity_sha256"],
        "build_mode": build_mode,
        "input_window": dict(input_window) if input_window is not None else None,
        "source_artifacts": list(source["artifacts"]),
        "provider_authority_view": (
            dict(provider_authority_view)
            if provider_authority_view is not None
            else None
        ),
        "market_window": artifact,
        "economic_outcomes_read": False,
        "strict_raw_native_queue_authority": False,
        "live_authority": False,
    }
    payload["receipt_identity_sha256"] = canonical_sha256(payload)
    _atomic_json(_window_receipt_path(window_path), payload)
    return artifact


def _provider_authority_view(
    *,
    day: str,
    source: Mapping[str, Any],
    sources: Mapping[str, Mapping[str, Any]],
    cache_root: Path,
) -> tuple[Path, dict[str, Any]]:
    source_root = Path(str(source["book_root"])).expanduser().resolve()
    provider_rows = {
        source_day: row
        for source_day, row in sources.items()
        if row["source_profile"] == "provider_normalized"
        and Path(str(row["book_root"])).expanduser().resolve() == source_root
    }
    if day not in provider_rows:
        raise F03ControlArtifactBuildError(f"{day} lacks its frozen provider source row")
    view_identity_payload = {
        "schema_version": f"{IDENTITY}.provider_authority_view",
        "source_root": str(source_root),
        "provider_day_source_identities": {
            source_day: row["source_identity_sha256"]
            for source_day, row in provider_rows.items()
        },
        "source_authority": "provider_normalized_causal",
        "queue_mode": "provider_visible_level",
        "exact_queue_policy_eligible": False,
    }
    view_identity = canonical_sha256(view_identity_payload)
    view_root = (
        cache_root.expanduser().resolve()
        / "source_authority_views"
        / f"provider_normalized_{view_identity[:16]}"
    )
    manifest_payload = {
        **view_identity_payload,
        "dataset_version": f"f03_provider_authority_view_{view_identity[:16]}",
        "view_identity_sha256": view_identity,
        "economic_outcomes_read": False,
    }
    manifest_path = view_root / "manifest.json"
    _atomic_json(manifest_path, manifest_payload)
    quality_lines = [
        "day,source_authority,formal_lifecycle_replay_eligible,"
        "provider_sensitivity_replay_eligible,exact_queue_policy_eligible"
    ]
    quality_lines.extend(
        f"{source_day},provider_normalized_causal,false,true,false"
        for source_day in provider_rows
    )
    quality_path = view_root / "daily_quality.csv"
    _atomic_text(quality_path, "\n".join(quality_lines) + "\n")

    linked_inputs: list[dict[str, Any]] = []
    for source_day in (_prior_day(day), day):
        for role in ("bbo", "l2"):
            filename = f"BTCUSDC-{role}-{source_day}.parquet"
            input_path = source_root / role / filename
            expected = None
            if source_day == day:
                expected = source["artifacts_by_role"][role]
            input_artifact = _artifact(
                input_path,
                role=f"{day} provider {source_day} {role}",
                expected_sha256=(str(expected["sha256"]) if expected else None),
                expected_size=(int(expected["size_bytes"]) if expected else None),
            )
            output_path = view_root / role / filename
            if not output_path.is_file() or _file_sha256(output_path) != input_artifact[
                "sha256"
            ]:
                _atomic_link_or_copy(input_path, output_path)
            output_artifact = _artifact(
                output_path,
                role=f"{day} provider authority-view {source_day} {role}",
                expected_sha256=str(input_artifact["sha256"]),
                expected_size=int(input_artifact["size_bytes"]),
            )
            linked_inputs.append(
                {
                    "day": source_day,
                    "role": role,
                    "source": input_artifact,
                    "view": output_artifact,
                }
            )
    binding = {
        "schema_version": f"{IDENTITY}.provider_authority_view_binding",
        "view_identity_sha256": view_identity,
        "source_root": str(source_root),
        "view_root": str(view_root),
        "manifest": _artifact(manifest_path, role="provider authority-view manifest"),
        "daily_quality": _artifact(
            quality_path, role="provider authority-view daily quality"
        ),
        "linked_inputs": linked_inputs,
        "economic_outcomes_read": False,
        "strict_queue_authorized": False,
    }
    binding["binding_identity_sha256"] = canonical_sha256(binding)
    return view_root, binding


def _materialize_window_from_sources(
    *,
    day: str,
    source: Mapping[str, Any],
    sources: Mapping[str, Mapping[str, Any]],
    operational: Mapping[str, Any],
    paths: BuilderPaths,
    target: Path,
) -> dict[str, Any]:
    params = native_runner._load_formal_base_params(  # noqa: SLF001
        Path(operational["operational_config"]["path"])
    )
    source_profile = str(source["source_profile"])
    params.update(
        {
            "queue_ahead_mode": (
                "exact_level" if source_profile == "native" else "provider_visible_level"
            ),
            "queue_l2_cancel_ahead_enabled": False,
            "window_cache_write_enabled": False,
            "legacy_monolithic_window_cache_write_enabled": False,
            "replay_purpose": "f03_control_71d_policy_artifact_materialization",
            "replay_promotion_eligible": False,
            "_formal_quality_allowed_days": [_prior_day(day), day],
            "model_dir": operational["model"]["directory"],
            "resolved_model_dir": operational["model"]["directory"],
        }
    )
    provider_view: dict[str, Any] | None = None
    if source_profile == "provider_normalized":
        book_root, provider_view = _provider_authority_view(
            day=day,
            source=source,
            sources=sources,
            cache_root=paths.cache_root,
        )
    else:
        book_root = Path(str(source["book_root"])).expanduser().resolve()
    bt.BBO_DIR = book_root / "bbo"
    bt.L2_DIR = book_root / "l2"
    feature_path = Path(
        str(source["artifacts_by_role"]["feature"]["path"])
    ).resolve()
    try:
        window = data_windows.load_tick_window(
            day,
            params,
            load_ml=False,
            require_ml=False,
            run_ml_inference=False,
            feature_dir=feature_path.parent,
            require_target_feature_files=False,
            cross_market_enabled=True,
            with_ml_cache=False,
            require_historical_bbo=True,
            require_formal_l2=False,
            cache_dir=paths.local_window_cache,
            refresh_cache=False,
        )
    except SystemExit as exc:
        raise F03ControlArtifactBuildError(f"{day} market-window source gate failed") from exc
    window.ml_data = None
    window.ml_cache = {}
    _validate_window_object(window, day=day, source_profile=source_profile)
    _atomic_pickle(target, window)
    return _publish_window_receipt(
        window_path=target,
        day=day,
        source=source,
        build_mode="rebuilt_from_frozen_market_sources",
        input_window=None,
        provider_authority_view=provider_view,
    )


def materialize_market_window(
    *,
    day: str,
    source: Mapping[str, Any],
    sources: Mapping[str, Mapping[str, Any]],
    operational: Mapping[str, Any],
    formal_windows: Mapping[str, Mapping[str, Any]],
    paths: BuilderPaths,
    deep_existing: bool = True,
) -> tuple[dict[str, Any], str]:
    target = _window_target(
        paths.cache_root,
        day=day,
        source_identity_sha256=str(source["source_identity_sha256"]),
    )
    existing = _load_window_receipt(
        target,
        day=day,
        source_identity_sha256=str(source["source_identity_sha256"]),
        source_profile=str(source["source_profile"]),
        deep=deep_existing,
    )
    if existing is not None:
        return existing, "reuse_admitted"

    formal = formal_windows.get(day)
    if formal is not None:
        source_artifact = _artifact_from_row(formal, role=f"{day} formal model-free window")
        validate_market_window(
            source_artifact,
            day=day,
            source_profile=str(source["source_profile"]),
        )
        _atomic_copy(Path(source_artifact["path"]), target)
        target_artifact = _publish_window_receipt(
            window_path=target,
            day=day,
            source=source,
            build_mode="byte_exact_copy_from_formal_40day_plan",
            input_window=source_artifact,
        )
        if target_artifact["sha256"] != source_artifact["sha256"]:
            raise F03ControlArtifactBuildError(f"{day} formal window copy changed bytes")
        return target_artifact, "copy_formal_40day"

    artifact = _materialize_window_from_sources(
        day=day,
        source=source,
        sources=sources,
        operational=operational,
        paths=paths,
        target=target,
    )
    return artifact, "build_from_sources"


def _component_binding(cache_root: Path, identity: Mapping[str, Any]) -> dict[str, Any]:
    identity_payload = dict(identity)
    identity_sha = canonical_sha256(identity_payload)
    directory = component_directory(
        cache_root,
        namespace="model_overlay_day",
        symbol=str(identity_payload["symbol"]),
        day=str(identity_payload["day"]),
        identity_sha256=identity_sha,
    )
    manifest_path = directory / "manifest.json"
    manifest = _load_json(manifest_path, role=f"{identity_payload['day']} direct overlay manifest")
    files = manifest.get("files") or {}
    if len(files) != 1:
        raise F03ControlArtifactBuildError(
            f"{identity_payload['day']} direct overlay data manifest is ambiguous"
        )
    data_path = directory / next(iter(files))
    return {
        "cache_root": str(cache_root.expanduser().resolve()),
        "identity": identity_payload,
        "identity_sha256": identity_sha,
        "manifest": _artifact(manifest_path, role=f"{identity_payload['day']} overlay manifest"),
        "data": _artifact(data_path, role=f"{identity_payload['day']} overlay data"),
    }


def _validate_overlay_binding(
    binding: Mapping[str, Any],
    *,
    day: str,
    model_bundle_identity_sha256: str,
) -> dict[str, Any]:
    schedule = dual_abi.load_bound_v9_control_overlay(
        binding,
        expected_day=day,
        expected_model_bundle_identity_sha256=model_bundle_identity_sha256,
    )
    if schedule.target_grid_row_count != control_repair.ROWS_PER_DAY:
        raise F03ControlArtifactBuildError(f"{day} overlay lost the complete 10s grid")
    return dict(binding)


def _load_control_panel(path: Path) -> tuple[dict[str, Any], dict[str, Mapping[str, Any]]]:
    panel = control_repair.validate_panel(path.expanduser().resolve())
    payload = panel.get("identity_payload") or {}
    rows = payload.get("components") or []
    by_day = {str(row.get("utc_day", "")): row for row in rows if isinstance(row, Mapping)}
    if len(by_day) != 40:
        raise F03ControlArtifactBuildError("control overlay panel is not the admitted 40 days")
    return panel, by_day


def _panel_ml_data(
    *,
    day: str,
    row: Mapping[str, Any],
    model_identity: str,
) -> tuple[Any, ...]:
    component = control_repair._validate_component(Path(str(row["directory"])))  # noqa: SLF001
    directory = Path(str(component["directory"]))
    if component["admission_mode"] == "reference_existing_exact":
        binding = _load_json(directory / "reference.json", role=f"{day} control reference")
        return dual_abi.load_bound_v9_control_overlay(
            binding,
            expected_day=day,
            expected_model_bundle_identity_sha256=model_identity,
        ).ml_data
    return control_repair._validate_ml_data(  # noqa: SLF001
        control_repair._load_generated_component(directory),  # noqa: SLF001
        day=day,
    )


def _overlay_feature_references(
    *,
    day: str,
    sources: Mapping[str, Mapping[str, Any]],
    panel_rows: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    target = _artifact_from_row(
        sources[day]["artifacts_by_role"]["feature"], role=f"{day} target feature"
    )
    references = [
        file_reference(
            Path(target["path"]),
            role="causal_features",
            logical_source=f"features/{day}",
            sha256=str(target["sha256"]),
            hash_provenance={"kind": "frozen_f03_source_plan_sha256"},
        )
    ]
    panel_row = panel_rows.get(day)
    if panel_row is not None:
        component_manifest = Path(str(panel_row["directory"])) / "manifest.json"
        component = _artifact(component_manifest, role=day)
        references.append(
            file_reference(
                Path(component["path"]),
                role="admitted_control_component",
                logical_source=f"f03_control_overlay/{day}",
                sha256=str(component["sha256"]),
                hash_provenance={"kind": "admitted_control_panel_manifest_sha256"},
            )
        )
    else:
        prior = _prior_day(day)
        if prior not in sources:
            raise F03ControlArtifactBuildError(f"{day} lacks a frozen D-1 feature source")
        prior_feature = _artifact_from_row(
            sources[prior]["artifacts_by_role"]["feature"], role=f"{day} D-1 feature"
        )
        references.append(
            file_reference(
                Path(prior_feature["path"]),
                role="causal_features",
                logical_source=f"features/{prior}",
                sha256=str(prior_feature["sha256"]),
                hash_provenance={"kind": "frozen_f03_source_plan_sha256"},
            )
        )
    return references


def _asof_feature_projection_frame(
    features: pd.DataFrame,
    *,
    canonical_labels_ms: np.ndarray,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not isinstance(features.index, pd.DatetimeIndex):
        raise F03ControlArtifactBuildError("feature projection requires DatetimeIndex")
    ordered = features.sort_index()
    if ordered.index.has_duplicates:
        raise F03ControlArtifactBuildError("feature projection source has duplicate timestamps")
    source_ms = ordered.index.to_numpy(dtype="datetime64[ns]").astype(np.int64) // 1_000_000
    labels = np.asarray(canonical_labels_ms, dtype=np.int64)
    positions = np.searchsorted(source_ms, labels, side="right") - 1
    if len(labels) == 0 or np.any(positions < 0):
        raise F03ControlArtifactBuildError(
            "feature projection lacks a causally prior ready state"
        )
    observed_ms = source_ms[positions]
    ages_ms = labels - observed_ms
    if np.any(ages_ms < 0):
        raise F03ControlArtifactBuildError("feature projection used future state")
    selected = ordered.iloc[positions].copy()
    selected.index = pd.to_datetime(labels, unit="ms", utc=True)
    if selected.isna().all(axis=1).any():
        raise F03ControlArtifactBuildError("feature projection selected an empty source row")
    audit = {
        "row_count": int(len(labels)),
        "exact_generation_count": int(np.count_nonzero(ages_ms == 0)),
        "asof_hold_count": int(np.count_nonzero(ages_ms > 0)),
        "max_asof_age_ms": int(np.max(ages_ms)),
        "mean_asof_age_ms": float(np.mean(ages_ms)),
        "source_observation_min_ts_ms": int(np.min(observed_ms)),
        "source_observation_max_ts_ms": int(np.max(observed_ms)),
        "canonical_label_min_ts_ms": int(np.min(labels)),
        "canonical_label_max_ts_ms": int(np.max(labels)),
        "future_rows_used": 0,
        "interpolation_used": False,
        "semantics": "latest_causally_ready_v9_prediction_state",
    }
    return selected, audit


def _prepare_asof_feature_projection(
    *,
    day: str,
    sources: Mapping[str, Mapping[str, Any]],
    cache_root: Path,
) -> dict[str, Any]:
    prior = _prior_day(day)
    if day not in sources or prior not in sources:
        raise F03ControlArtifactBuildError(f"{day} lacks frozen D-1 feature context")
    target_artifact = _artifact_from_row(
        sources[day]["artifacts_by_role"]["feature"], role=f"{day} target feature"
    )
    prior_artifact = _artifact_from_row(
        sources[prior]["artifacts_by_role"]["feature"], role=f"{day} D-1 feature"
    )
    identity_payload = {
        "schema_version": ASOF_FEATURE_SCHEMA_VERSION,
        "day": day,
        "prior_feature": prior_artifact,
        "target_feature": target_artifact,
        "canonical_labels": "canonical_visibility_grid_minus_10s",
        "selection_semantics": "latest_source_label_at_or_before_canonical_label",
        "interpolation_allowed": False,
        "future_state_allowed": False,
    }
    identity_sha = canonical_sha256(identity_payload)
    root = (
        cache_root.expanduser().resolve()
        / "feature_projections"
        / day
        / identity_sha
    )
    target_path = root / "asof_features.parquet"
    prior_path = root / "empty_prior_features.parquet"
    receipt_path = root / "manifest.json"
    if target_path.is_file() and prior_path.is_file() and receipt_path.is_file():
        receipt = _load_json(receipt_path, role=f"{day} asof feature projection")
        observed_identity = str(receipt.pop("projection_identity_sha256", ""))
        if (
            receipt.get("identity_payload") != identity_payload
            or canonical_sha256(receipt) != observed_identity
        ):
            raise F03ControlArtifactBuildError(f"{day} asof feature projection drifted")
        receipt["projection_identity_sha256"] = observed_identity
        target_binding = _artifact_from_row(
            receipt.get("asof_features") or {}, role=f"{day} asof features"
        )
        prior_binding = _artifact_from_row(
            receipt.get("empty_prior_features") or {}, role=f"{day} empty prior features"
        )
    else:
        prior_frame = pd.read_parquet(Path(prior_artifact["path"]))
        target_frame = pd.read_parquet(Path(target_artifact["path"]))
        combined = pd.concat((prior_frame, target_frame)).sort_index()
        labels = control_repair.canonical_feature_labels(day)
        selected, audit = _asof_feature_projection_frame(
            combined,
            canonical_labels_ms=labels,
        )
        control_repair._atomic_parquet(target_path, selected)  # noqa: SLF001
        control_repair._atomic_parquet(prior_path, selected.iloc[:0])  # noqa: SLF001
        target_binding = _artifact(target_path, role=f"{day} asof features")
        prior_binding = _artifact(prior_path, role=f"{day} empty prior features")
        receipt = {
            "schema_version": ASOF_FEATURE_SCHEMA_VERSION,
            "identity_payload": identity_payload,
            "audit": audit,
            "asof_features": target_binding,
            "empty_prior_features": prior_binding,
            "economic_outcomes_read": False,
            "action_authorized": False,
            "live_authorized": False,
        }
        receipt["projection_identity_sha256"] = canonical_sha256(receipt)
        _atomic_json(receipt_path, receipt)
    receipt_binding = _artifact(receipt_path, role=f"{day} asof feature manifest")
    references = [
        file_reference(
            Path(target_binding["path"]),
            role="causal_features",
            logical_source=f"f03_asof_features/{day}",
            sha256=str(target_binding["sha256"]),
            hash_provenance={"kind": "asof_feature_projection_sha256"},
        ),
        file_reference(
            Path(receipt_binding["path"]),
            role="feature_projection_manifest",
            logical_source=f"f03_asof_features/{day}/manifest",
            sha256=str(receipt_binding["sha256"]),
            hash_provenance={"kind": "projection_manifest_sha256"},
        ),
    ]
    return {
        "identity_sha256": identity_sha,
        "prior_path": str(Path(prior_binding["path"])),
        "target_path": str(Path(target_binding["path"])),
        "receipt": receipt_binding,
        "references": references,
        "audit": receipt["audit"],
    }


def materialize_control_overlay(
    *,
    day: str,
    source: Mapping[str, Any],
    market_window: Mapping[str, Any],
    sources: Mapping[str, Mapping[str, Any]],
    panel_rows: Mapping[str, Mapping[str, Any]],
    operational: Mapping[str, Any],
    paths: BuilderPaths,
) -> tuple[dict[str, Any], str]:
    panel_row = panel_rows.get(day)
    feature_projection: dict[str, Any] | None = None
    if panel_row is None:
        feature_projection = _prepare_asof_feature_projection(
            day=day,
            sources=sources,
            cache_root=paths.cache_root,
        )
        feature_references = list(feature_projection["references"])
    else:
        feature_references = _overlay_feature_references(
            day=day,
            sources=sources,
            panel_rows=panel_rows,
        )
    model_references = list(operational["model"]["reference_identity"])
    market_context_identity = canonical_sha256(
        {
            "schema_version": f"{IDENTITY}.market_context_binding",
            "day": day,
            "source_identity_sha256": source["source_identity_sha256"],
            "market_window_sha256": market_window["sha256"],
        }
    )
    identity = model_overlay_identity(
        symbol="BTCUSDC",
        day=day,
        market_context_identity_sha256=market_context_identity,
        feature_source_identity=feature_references,
        model_bundle_identity=model_references,
        toxicity_horizon_s=10,
        cross_market_enabled=True,
        run_ml_inference=True,
    )
    expected_model_identity = operational["model"]["content_identity_sha256"]
    if identity["model_bundle_identity_sha256"] != expected_model_identity:
        raise F03ControlArtifactBuildError("overlay/model content identity algorithm drifted")
    loaded = load_model_overlay(cache_root=paths.cache_root, identity=identity)
    mode = "reuse_admitted"
    if loaded is None:
        if panel_row is not None:
            ml_data = _panel_ml_data(
                day=day,
                row=panel_row,
                model_identity=expected_model_identity,
            )
            mode = "convert_admitted_40day_control"
        else:
            if feature_projection is None:
                raise F03ControlArtifactBuildError(
                    f"{day} lacks its asof feature projection"
                )
            prior_feature = Path(feature_projection["prior_path"])
            target_feature = Path(feature_projection["target_path"])
            ml_data = control_repair._generate_ml_data(  # noqa: SLF001
                prior_feature,
                target_feature,
                day=day,
                model_dir=Path(operational["model"]["directory"]),
            )
            mode = "infer_from_frozen_features_with_v9_causal_asof_hold"
        write_model_overlay(cache_root=paths.cache_root, identity=identity, ml_data=ml_data)
    binding = _component_binding(paths.cache_root, identity)
    return (
        _validate_overlay_binding(
            binding,
            day=day,
            model_bundle_identity_sha256=expected_model_identity,
        ),
        mode,
    )


def _causal_initial_mark(
    *,
    first_day: str,
    checkpoint_ts_ms: int,
    first_source: Mapping[str, Any],
) -> tuple[float, int, dict[str, Any]]:
    import pyarrow.parquet as pq

    prior = _prior_day(first_day)
    root = Path(str(first_source["book_root"])).expanduser().resolve()
    bbo_path = root / "bbo" / f"BTCUSDC-bbo-{prior}.parquet"
    artifact = _artifact(bbo_path, role="D-1 causal initial-state BBO")
    parquet = pq.ParquetFile(bbo_path)
    columns = set(parquet.schema_arrow.names)
    required = {"timestamp", "best_bid", "best_ask"}
    if not required.issubset(columns):
        raise F03ControlArtifactBuildError("initial-state BBO schema is unsupported")
    selected: tuple[int, float, float] | None = None
    for index in range(parquet.num_row_groups - 1, -1, -1):
        table = parquet.read_row_group(index, columns=sorted(required))
        timestamp = table.column("timestamp").to_numpy(zero_copy_only=False).astype(np.int64)
        mask = timestamp <= int(checkpoint_ts_ms)
        if not np.any(mask):
            continue
        positions = np.flatnonzero(mask)
        position = int(positions[-1])
        bid = float(table.column("best_bid")[position].as_py())
        ask = float(table.column("best_ask")[position].as_py())
        selected = (int(timestamp[position]), bid, ask)
        break
    if selected is None:
        raise F03ControlArtifactBuildError("no causally visible BBO exists for initial state")
    timestamp, bid, ask = selected
    if not np.isfinite((bid, ask)).all() or bid <= 0 or ask < bid:
        raise F03ControlArtifactBuildError("initial-state BBO is invalid")
    return (bid + ask) / 2.0, timestamp, artifact


def build_initial_state(
    *,
    admission_root: Path,
    framework_plan: Mapping[str, Any],
    sources: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    operations = framework_plan.get("operations") or []
    if not operations:
        raise F03ControlArtifactBuildError("framework plan has no operations")
    first_operation = operations[0]
    first_day = EXPECTED_DAYS[0]
    checkpoint = int(first_operation["start_ts_ms"])
    mark, mark_ts_ms, mark_artifact = _causal_initial_mark(
        first_day=first_day,
        checkpoint_ts_ms=checkpoint,
        first_source=sources[first_day],
    )
    state = ContinuousReplayState(
        arm_id=CONTROL_ARM,
        checkpoint_ts_ms=checkpoint,
        cash_usdc=0.0,
        position_btc=0.0,
        average_entry_price=0.0,
        cumulative_realized_pnl_usdc=0.0,
        cumulative_fees_usdc=0.0,
        equity_anchor_usdc=0.0,
        last_mark_price=mark,
        cumulative_pnl_usdc=0.0,
        orders_terminal=True,
        feature_warmup_ready=False,
        quoting_enabled=False,
    )
    state.validate(require_restart_safe=True)
    payload = {
        "schema_version": INITIAL_STATE_SCHEMA_VERSION,
        "identity": "flat_zero_economics_before_first_71day_restart_warmup",
        "derivation": {
            "first_day": first_day,
            "checkpoint_ts_ms": checkpoint,
            "first_operation_id": first_operation["operation_id"],
            "mark_source": mark_artifact,
            "mark_source_ts_ms": mark_ts_ms,
            "mark_is_causally_visible": mark_ts_ms <= checkpoint,
            "mark_formula": "(best_bid + best_ask) / 2",
            "cash_position_campaign_source": "frozen_common_flat_panel_start_contract",
            "future_market_values_used": False,
        },
        "continuous_replay_state": state.to_dict(),
        "economic_outcomes_read": False,
    }
    payload["initial_state_identity_sha256"] = canonical_sha256(payload)
    path = admission_root / INITIAL_STATE
    _atomic_json(path, payload)
    return _artifact(path, role="common initial state")


def _existing_materialization(
    *,
    day: str,
    source: Mapping[str, Any],
    paths: BuilderPaths,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    window_path = _window_target(
        paths.cache_root,
        day=day,
        source_identity_sha256=str(source["source_identity_sha256"]),
    )
    window = _load_window_receipt(
        window_path,
        day=day,
        source_identity_sha256=str(source["source_identity_sha256"]),
        source_profile=str(source["source_profile"]),
        deep=False,
    )
    overlay: dict[str, Any] | None = None
    for manifest in (
        paths.cache_root
        / "components_v2"
        / "model_overlay_day"
        / "btcusdc"
        / day
    ).glob("*/manifest.json"):
        payload = _load_json(manifest, role=f"{day} overlay candidate")
        identity = payload.get("identity")
        if not isinstance(identity, Mapping):
            continue
        if (
            identity.get("market_context_identity_sha256")
            == canonical_sha256(
                {
                    "schema_version": f"{IDENTITY}.market_context_binding",
                    "day": day,
                    "source_identity_sha256": source["source_identity_sha256"],
                    "market_window_sha256": (window or {}).get("sha256"),
                }
            )
            and identity.get("run_ml_inference") is True
        ):
            overlay = _component_binding(paths.cache_root, identity)
            break
    return window, overlay


def preflight(
    *,
    paths: BuilderPaths = DEFAULT_PATHS,
    verify_source_hashes: bool = False,
) -> dict[str, Any]:
    framework_plan, sources = load_frozen_sources(
        paths.framework_plan,
        verify_source_hashes=verify_source_hashes,
    )
    operational = load_operational_projection(paths)
    formal_windows = _validate_formal_40_plan(paths.formal_40_plan)
    control_panel, panel_rows = _load_control_panel(paths.control_panel)
    _artifact(paths.latency_profile, role="latency profile")
    _artifact(paths.engine_state_schema, role="engine-state schema")
    missing_windows: list[str] = []
    missing_overlays: list[str] = []
    durable_windows = 0
    durable_overlays = 0
    for day in EXPECTED_DAYS:
        window, overlay = _existing_materialization(day=day, source=sources[day], paths=paths)
        if window is None:
            missing_windows.append(day)
        else:
            durable_windows += 1
        if overlay is None:
            missing_overlays.append(day)
        else:
            durable_overlays += 1
    return {
        "schema_version": f"{IDENTITY}.preflight",
        "identity": IDENTITY,
        "framework_plan_identity_sha256": framework_plan["plan_identity_sha256"],
        "ordered_days": list(EXPECTED_DAYS),
        "day_count": len(EXPECTED_DAYS),
        "native_day_count": sum(
            row["source_profile"] == "native" for row in sources.values()
        ),
        "provider_normalized_day_count": sum(
            row["source_profile"] == "provider_normalized" for row in sources.values()
        ),
        "formal_40day_window_count": len(formal_windows),
        "control_panel_day_count": len(panel_rows),
        "control_panel_identity_sha256": control_panel["panel_identity_sha256"],
        "durable_window_count": durable_windows,
        "durable_overlay_count": durable_overlays,
        "missing_window_days": missing_windows,
        "missing_overlay_days": missing_overlays,
        "current_baseline_id": operational["current_baseline_id"],
        "historical_v9_baseline_id": operational["historical_v9_baseline_id"],
        "model_bundle_identity_sha256": operational["model"][
            "content_identity_sha256"
        ],
        "economic_outcomes_read": False,
        "continuous_economic_replay_started": False,
        "ready_to_build": not missing_windows and not missing_overlays,
    }


def materialize(
    *,
    paths: BuilderPaths = DEFAULT_PATHS,
    max_days: int | None = None,
) -> dict[str, Any]:
    if max_days is not None and max_days <= 0:
        raise F03ControlArtifactBuildError("max_days must be positive when provided")
    _, sources = load_frozen_sources(paths.framework_plan)
    operational = load_operational_projection(paths, materialize_offline_config=True)
    formal_windows = _validate_formal_40_plan(paths.formal_40_plan)
    _, panel_rows = _load_control_panel(paths.control_panel)
    selected = EXPECTED_DAYS[:max_days] if max_days is not None else EXPECTED_DAYS
    rows: list[dict[str, Any]] = []
    for ordinal, day in enumerate(selected, start=1):
        window, window_mode = materialize_market_window(
            day=day,
            source=sources[day],
            sources=sources,
            operational=operational,
            formal_windows=formal_windows,
            paths=paths,
        )
        print(f"WINDOW {ordinal}/{len(selected)} {day} {window_mode}", flush=True)
        overlay, overlay_mode = materialize_control_overlay(
            day=day,
            source=sources[day],
            market_window=window,
            sources=sources,
            panel_rows=panel_rows,
            operational=operational,
            paths=paths,
        )
        print(f"OVERLAY {ordinal}/{len(selected)} {day} {overlay_mode}", flush=True)
        rows.append(
            {
                "day": day,
                "source_profile": sources[day]["source_profile"],
                "source_identity_sha256": sources[day]["source_identity_sha256"],
                "market_window": window,
                "window_mode": window_mode,
                "control_overlay_binding": overlay,
                "overlay_mode": overlay_mode,
            }
        )
    return {
        "schema_version": f"{IDENTITY}.materialization",
        "selected_day_count": len(selected),
        "complete_71day_materialization": tuple(selected) == EXPECTED_DAYS,
        "rows": rows,
        "economic_outcomes_read": False,
        "continuous_economic_replay_started": False,
    }


def _policy_identity_payload(
    *,
    operational: Mapping[str, Any],
    initial_state: Mapping[str, Any],
    latency_profile: Mapping[str, Any],
    engine_state_schema: Mapping[str, Any],
    overlay_index: Mapping[str, Any],
    days: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "identity": POLICY_IDENTITY,
        "arm": CONTROL_ARM,
        "cadence_ms": 10_000,
        "historical_v9_baseline_id": operational["historical_v9_baseline_id"],
        "current_operational_successor_id": operational["current_baseline_id"],
        "raw_operational_config_sha256": operational["raw_operational_config"][
            "sha256"
        ],
        "offline_execution_config_sha256": operational["operational_config"]["sha256"],
        "offline_config_projection_receipt_sha256": operational[
            "offline_config_receipt"
        ]["sha256"],
        "baseline_identity_sha256": operational["baseline_identity"]["sha256"],
        "historical_v9_identity_sha256": operational["historical_v9_identity"]["sha256"],
        "bundle_meta_sha256": operational["model"]["bundle_meta"]["sha256"],
        "model_bundle_identity_sha256": operational["model"]["content_identity_sha256"],
        "p3_sha256": operational["p3"]["sha256"],
        "feature_dag_sha256": operational["feature_dag"]["sha256"],
        "feature_dag_semantic_sha256": operational["semantic_feature_dag_sha256"],
        "initial_state_sha256": initial_state["sha256"],
        "latency_profile_sha256": latency_profile["sha256"],
        "engine_state_schema_sha256": engine_state_schema["sha256"],
        "overlay_index_sha256": overlay_index["sha256"],
        "days": {
            day: {
                "source_profile": row["source_profile"],
                "source_identity_sha256": row["source_identity_sha256"],
                "market_window_sha256": row["market_window"]["sha256"],
                "control_overlay_identity_sha256": row["control_overlay_binding"][
                    "identity_sha256"
                ],
            }
            for day, row in days.items()
        },
        "ml_enabled": True,
        "q90_action_enabled": False,
        "buy_fill_selection_enabled": False,
        "execution_abi": continuous_adapter.SCHEMA_VERSION,
        "strict_raw_native_queue_authority": False,
        "receive_time_transport_authority": False,
        "action_authority": False,
        "live_authority": False,
    }


def _collect_complete_days(
    *,
    sources: Mapping[str, Mapping[str, Any]],
    operational: Mapping[str, Any],
    panel_rows: Mapping[str, Mapping[str, Any]],
    paths: BuilderPaths,
    window_loader: Callable[[Path], Any] | None = None,
) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for ordinal, day in enumerate(EXPECTED_DAYS, start=1):
        source = sources[day]
        window_path = _window_target(
            paths.cache_root,
            day=day,
            source_identity_sha256=str(source["source_identity_sha256"]),
        )
        window = _load_window_receipt(
            window_path,
            day=day,
            source_identity_sha256=str(source["source_identity_sha256"]),
            source_profile=str(source["source_profile"]),
            deep=False,
        )
        if window is None:
            raise F03ControlArtifactBuildError(
                f"{day} market window is not durably materialized"
            )
        validate_market_window(
            window,
            day=day,
            source_profile=str(source["source_profile"]),
            loader=window_loader,
        )
        overlay, mode = materialize_control_overlay(
            day=day,
            source=source,
            market_window=window,
            sources=sources,
            panel_rows=panel_rows,
            operational=operational,
            paths=paths,
        )
        print(f"VALIDATE {ordinal}/{len(EXPECTED_DAYS)} {day} overlay={mode}", flush=True)
        rows[day] = {
            "source_profile": source["source_profile"],
            "source_identity_sha256": source["source_identity_sha256"],
            "market_window": window,
            "control_overlay_binding": overlay,
        }
    return rows


def build_admission(
    *,
    admission_root: Path,
    paths: BuilderPaths = DEFAULT_PATHS,
    window_loader: Callable[[Path], Any] | None = None,
) -> dict[str, Any]:
    root = admission_root.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    (root / ADMISSION_MARKER).unlink(missing_ok=True)
    framework_plan, sources = load_frozen_sources(paths.framework_plan)
    operational = load_operational_projection(paths, materialize_offline_config=True)
    _, panel_rows = _load_control_panel(paths.control_panel)
    days = _collect_complete_days(
        sources=sources,
        operational=operational,
        panel_rows=panel_rows,
        paths=paths,
        window_loader=window_loader,
    )

    initial_state = build_initial_state(
        admission_root=root,
        framework_plan=framework_plan,
        sources=sources,
    )
    latency = _artifact(paths.latency_profile, role="latency profile")
    engine_schema = _artifact(paths.engine_state_schema, role="engine-state schema")
    overlay_index_payload = {
        "schema_version": OVERLAY_INDEX_SCHEMA_VERSION,
        "identity": POLICY_IDENTITY,
        "ordered_days": list(EXPECTED_DAYS),
        "model_bundle_identity_sha256": operational["model"]["content_identity_sha256"],
        "days": {
            day: {
                "source_profile": row["source_profile"],
                "source_identity_sha256": row["source_identity_sha256"],
                "market_window": row["market_window"],
                "control_overlay_binding": row["control_overlay_binding"],
            }
            for day, row in days.items()
        },
        "economic_outcomes_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    overlay_index_payload["index_identity_sha256"] = canonical_sha256(overlay_index_payload)
    overlay_index_path = root / OVERLAY_INDEX
    _atomic_json(overlay_index_path, overlay_index_payload)
    overlay_index = _artifact(overlay_index_path, role="71-day control overlay index")

    policy_identity_payload = _policy_identity_payload(
        operational=operational,
        initial_state=initial_state,
        latency_profile=latency,
        engine_state_schema=engine_schema,
        overlay_index=overlay_index,
        days=days,
    )
    policy_identity_sha = canonical_sha256(policy_identity_payload)
    native_days = [day for day in EXPECTED_DAYS if days[day]["source_profile"] == "native"]
    provider_days = [
        day for day in EXPECTED_DAYS if days[day]["source_profile"] == "provider_normalized"
    ]
    projection = {
        "classification": "explicit_observability_only_successor_projection",
        "historical_v9_baseline_id": operational["historical_v9_baseline_id"],
        "current_operational_successor_id": operational["current_baseline_id"],
        "historical_v9_identity": operational["historical_v9_identity"],
        "current_operational_pointer": operational["pointer"],
        "current_operational_identity": operational["baseline_identity"],
        "current_operational_config_raw": operational["raw_operational_config"],
        "offline_execution_config": operational["operational_config"],
        "offline_execution_config_receipt": operational["offline_config_receipt"],
        "offline_execution_config_structured_differences": operational[
            "offline_config_structured_differences"
        ],
        "offline_projection_disables_only_host_journal_writer": True,
        "quote_policy_change": False,
        "observation_only_change": True,
        "historical_889f_config_bytes_recovered": False,
        "historical_889f_config_substituted": False,
    }
    core = {
        "arm": CONTROL_ARM,
        "identity": POLICY_IDENTITY,
        "cadence_ms": 10_000,
        "baseline_id": framework.parent.one_second_replay.EXPECTED_BASELINE_ID,
        "bundle_meta": operational["model"]["bundle_meta"],
        "feature_dag": operational["feature_dag"],
        "operational_config": operational["operational_config"],
        "raw_operational_config": operational["raw_operational_config"],
        "offline_execution_config_receipt": operational["offline_config_receipt"],
        "baseline_identity": operational["baseline_identity"],
        "initial_state": initial_state,
        "latency_profile": latency,
        "engine_state_schema": engine_schema,
        "p3_artifact": operational["p3"],
        "historical_v9_identity": operational["historical_v9_identity"],
        "operational_successor_projection": projection,
        "policy_identity_payload": policy_identity_payload,
        "policy_identity_sha256": policy_identity_sha,
        "model_bundle_identity_sha256": operational["model"]["content_identity_sha256"],
        "native_days": native_days,
        "provider_days": provider_days,
        "ml_enabled": True,
        "q90_action_enabled": False,
        "buy_fill_selection_enabled": False,
        "execution_abi": continuous_adapter.SCHEMA_VERSION,
        "strict_raw_native_queue_authority": False,
        "receive_time_transport_authority": False,
        "action_authorized": False,
        "live_authorized": False,
        "economic_outcomes_read": False,
    }
    v12_payload = {
        "schema_version": framework.POLICY_ARTIFACT_SCHEMA_VERSION,
        **core,
        "overlay_indices": [overlay_index],
        "days": {
            day: {
                "overlay_manifest": row["control_overlay_binding"]["manifest"],
                "overlay_data": row["control_overlay_binding"]["data"],
                "overlay_identity_sha256": row["control_overlay_binding"][
                    "identity_sha256"
                ],
                "source_profile": row["source_profile"],
                "source_identity_sha256": row["source_identity_sha256"],
                "market_window": row["market_window"],
            }
            for day, row in days.items()
        },
    }
    v13_payload = {
        "schema_version": concrete.POLICY_SCHEMA_VERSION,
        **core,
        "overlay_index": overlay_index,
        "days": {
            day: {
                "source_profile": row["source_profile"],
                "source_identity_sha256": row["source_identity_sha256"],
                "market_window": row["market_window"],
                "control_overlay_binding": row["control_overlay_binding"],
            }
            for day, row in days.items()
        },
    }
    v12_path = root / V12_MANIFEST
    v13_path = root / V13_MANIFEST
    _atomic_json(v12_path, v12_payload)
    _atomic_json(v13_path, v13_payload)
    framework.load_policy_artifacts(v12_path, expected_arm=CONTROL_ARM)
    observed_v13 = concrete._load_policy_manifest(  # noqa: SLF001
        v13_path,
        expected_arm=CONTROL_ARM,
    )
    if observed_v13["policy_identity_sha256"] != policy_identity_sha:
        raise F03ControlArtifactBuildError("v1.3 policy identity changed during admission")
    marker_payload = {
        "schema_version": ADMISSION_SCHEMA_VERSION,
        "identity": IDENTITY,
        "policy_identity_sha256": policy_identity_sha,
        "ordered_days_sha256": canonical_sha256(list(EXPECTED_DAYS)),
        "v12_manifest": _artifact(v12_path, role="v1.2 control policy manifest"),
        "v13_manifest": _artifact(v13_path, role="v1.3 control policy manifest"),
        "overlay_index": overlay_index,
        "initial_state": initial_state,
        "economic_outcomes_read": False,
        "continuous_economic_replay_started": False,
        "strict_queue_authorized": False,
        "receive_time_transport_authorized": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    marker_payload["admission_identity_sha256"] = canonical_sha256(marker_payload)
    _atomic_json(root / ADMISSION_MARKER, marker_payload)
    return validate_admission(root, paths=paths, deep=True, window_loader=window_loader)


def validate_admission(
    admission_root: Path,
    *,
    paths: BuilderPaths = DEFAULT_PATHS,
    deep: bool = True,
    window_loader: Callable[[Path], Any] | None = None,
) -> dict[str, Any]:
    root = admission_root.expanduser().resolve()
    marker = _load_json(root / ADMISSION_MARKER, role="control policy admission marker")
    if marker.get("schema_version") != ADMISSION_SCHEMA_VERSION:
        raise F03ControlArtifactBuildError("control policy admission schema drifted")
    observed_identity = str(marker.pop("admission_identity_sha256", ""))
    if canonical_sha256(marker) != observed_identity:
        raise F03ControlArtifactBuildError("control policy admission identity drifted")
    marker["admission_identity_sha256"] = observed_identity
    for name in ("v12_manifest", "v13_manifest", "overlay_index", "initial_state"):
        _artifact_from_row(marker[name], role=f"admission {name}")
    v12_path = Path(marker["v12_manifest"]["path"])
    v13_path = Path(marker["v13_manifest"]["path"])
    framework.load_policy_artifacts(v12_path, expected_arm=CONTROL_ARM)
    v13 = concrete._load_policy_manifest(v13_path, expected_arm=CONTROL_ARM)  # noqa: SLF001
    projection = validate_offline_execution_config(
        raw_config=v13.get("raw_operational_config") or {},
        offline_config=v13["operational_config"],
        receipt=v13.get("offline_execution_config_receipt") or {},
    )
    _, sources = load_frozen_sources(paths.framework_plan)
    if tuple(v13["days"]) != EXPECTED_DAYS:
        raise F03ControlArtifactBuildError("admitted control lost the exact 71-day order")
    for day in EXPECTED_DAYS:
        row = v13["days"][day]
        if (
            row["source_profile"] != sources[day]["source_profile"]
            or row["source_identity_sha256"] != sources[day]["source_identity_sha256"]
        ):
            raise F03ControlArtifactBuildError(f"{day} admitted source binding drifted")
        if deep:
            validate_market_window(
                row["market_window"],
                day=day,
                source_profile=row["source_profile"],
                loader=window_loader,
            )
            _validate_overlay_binding(
                row["overlay"]["binding"],
                day=day,
                model_bundle_identity_sha256=v13["model_bundle_identity_sha256"],
            )
    return {
        "schema_version": ADMISSION_SCHEMA_VERSION,
        "admission_root": str(root),
        "admission_identity_sha256": observed_identity,
        "policy_identity_sha256": v13["policy_identity_sha256"],
        "v12_manifest": marker["v12_manifest"],
        "v13_manifest": marker["v13_manifest"],
        "day_count": len(v13["days"]),
        "native_day_count": sum(
            row["source_profile"] == "native" for row in v13["days"].values()
        ),
        "provider_normalized_day_count": sum(
            row["source_profile"] == "provider_normalized"
            for row in v13["days"].values()
        ),
        "deep_validated": deep,
        "offline_config_projection_identity_sha256": projection[
            "projection_identity_sha256"
        ],
        "economic_outcomes_read": False,
        "continuous_economic_replay_started": False,
        "action_authorized": False,
        "live_authorized": False,
    }


def prepare_control_only_plan(
    *,
    admission_root: Path,
    plan_root: Path = DEFAULT_CONCRETE_PLAN_ROOT,
    paths: BuilderPaths = DEFAULT_PATHS,
) -> dict[str, Any]:
    admission = validate_admission(admission_root, paths=paths, deep=False)
    manifest = Path(admission["v13_manifest"]["path"])
    plan = concrete.prepare_execution_plan(
        framework_plan=paths.framework_plan,
        output_root=plan_root,
        control_artifacts=manifest,
        candidate_artifacts=None,
    )
    concrete.validate_execution_plan(plan_root / concrete.PLAN_FILENAME)
    control = (plan.get("policy_artifacts") or {}).get(CONTROL_ARM)
    if not isinstance(control, Mapping):
        raise F03ControlArtifactBuildError("concrete plan failed to bind the control policy")
    if "control_authoritative_market_window_and_overlay_unbound" in plan.get("blockers", ()):
        raise F03ControlArtifactBuildError("concrete plan retained the control binding blocker")
    return {
        "schema_version": f"{IDENTITY}.control_only_plan",
        "plan_path": str((plan_root / concrete.PLAN_FILENAME).resolve()),
        "plan_sha256": _file_sha256(plan_root / concrete.PLAN_FILENAME),
        "plan_identity_sha256": plan["plan_identity_sha256"],
        "control_policy_bound": True,
        "blockers": list(plan.get("blockers", ())),
        "execution_eligible": plan["execution_eligible"],
        "economic_outcomes_read": False,
        "continuous_economic_replay_started": False,
        "action_authorized": False,
        "live_authorized": False,
    }


def admit_control_policy_and_prepare_plan(
    *,
    temporary_admission_root: Path = DEFAULT_TEMP_ADMISSION,
    durable_admission_root: Path = DEFAULT_DURABLE_ADMISSION,
    plan_root: Path = DEFAULT_CONCRETE_PLAN_ROOT,
    paths: BuilderPaths = DEFAULT_PATHS,
    window_loader: Callable[[Path], Any] | None = None,
) -> dict[str, Any]:
    temporary_root = temporary_admission_root.expanduser().resolve()
    durable_root = durable_admission_root.expanduser().resolve()
    if temporary_root == durable_root:
        raise F03ControlArtifactBuildError(
            "temporary and durable admission roots must be disjoint"
        )
    temporary = build_admission(
        admission_root=temporary_root,
        paths=paths,
        window_loader=window_loader,
    )
    ephemeral_root = Path(tempfile.gettempdir()).resolve()
    if temporary_root == ephemeral_root or not temporary_root.is_relative_to(
        ephemeral_root
    ):
        raise F03ControlArtifactBuildError(
            "the first admission must be staged below the system temporary root"
        )
    durable = build_admission(
        admission_root=durable_root,
        paths=paths,
        window_loader=window_loader,
    )
    if temporary["policy_identity_sha256"] != durable["policy_identity_sha256"]:
        raise F03ControlArtifactBuildError(
            "temporary and durable control policy identities differ"
        )
    plan = prepare_control_only_plan(
        admission_root=durable_root,
        plan_root=plan_root,
        paths=paths,
    )
    return {
        "schema_version": f"{IDENTITY}.admit_control_plan",
        "temporary_admission": temporary,
        "durable_admission": durable,
        **plan,
    }


def _paths_from_args(args: argparse.Namespace) -> BuilderPaths:
    return BuilderPaths(
        framework_plan=args.framework_plan,
        formal_40_plan=args.formal_40_plan,
        control_panel=args.control_panel,
        latency_profile=args.latency_profile,
        engine_state_schema=args.engine_state_schema,
        archived_feature_dag=args.archived_feature_dag,
        historical_v9_identity=args.historical_v9_identity,
        cache_root=args.cache_root,
        local_window_cache=args.local_window_cache,
    )


def _add_common_paths(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--framework-plan", type=Path, default=DEFAULT_FRAMEWORK_PLAN)
    parser.add_argument("--formal-40-plan", type=Path, default=DEFAULT_FORMAL_40_PLAN)
    parser.add_argument("--control-panel", type=Path, default=DEFAULT_CONTROL_PANEL)
    parser.add_argument("--latency-profile", type=Path, default=DEFAULT_LATENCY_PROFILE)
    parser.add_argument(
        "--engine-state-schema", type=Path, default=DEFAULT_ENGINE_STATE_SCHEMA
    )
    parser.add_argument(
        "--archived-feature-dag", type=Path, default=DEFAULT_ARCHIVED_FEATURE_DAG
    )
    parser.add_argument(
        "--historical-v9-identity", type=Path, default=DEFAULT_HISTORICAL_V9_IDENTITY
    )
    parser.add_argument("--cache-root", type=Path, default=DEFAULT_CACHE_ROOT)
    parser.add_argument(
        "--local-window-cache", type=Path, default=DEFAULT_LOCAL_WINDOW_CACHE
    )


def _summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "schema_version",
        "ready_to_build",
        "day_count",
        "native_day_count",
        "provider_normalized_day_count",
        "durable_window_count",
        "durable_overlay_count",
        "missing_window_days",
        "missing_overlay_days",
        "selected_day_count",
        "complete_71day_materialization",
        "admission_root",
        "policy_identity_sha256",
        "plan_path",
        "control_policy_bound",
        "blockers",
        "execution_eligible",
        "economic_outcomes_read",
        "continuous_economic_replay_started",
        "action_authorized",
        "live_authorized",
    )
    return {key: payload[key] for key in keys if key in payload}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    preflight_parser = commands.add_parser("preflight")
    materialize_parser = commands.add_parser("materialize")
    build_parser = commands.add_parser("build")
    validate_parser = commands.add_parser("validate")
    plan_parser = commands.add_parser("prepare-control-plan")
    admit_parser = commands.add_parser("admit-control-plan")
    for command in (
        preflight_parser,
        materialize_parser,
        build_parser,
        validate_parser,
        plan_parser,
        admit_parser,
    ):
        _add_common_paths(command)
    preflight_parser.add_argument("--verify-source-hashes", action="store_true")
    materialize_parser.add_argument("--max-days", type=int)
    build_parser.add_argument("--admission-root", type=Path, default=DEFAULT_TEMP_ADMISSION)
    validate_parser.add_argument("--admission-root", type=Path, default=DEFAULT_TEMP_ADMISSION)
    validate_parser.add_argument("--shallow", action="store_true")
    plan_parser.add_argument("--admission-root", type=Path, default=DEFAULT_DURABLE_ADMISSION)
    plan_parser.add_argument("--plan-root", type=Path, default=DEFAULT_CONCRETE_PLAN_ROOT)
    admit_parser.add_argument(
        "--temporary-admission-root",
        type=Path,
        default=DEFAULT_TEMP_ADMISSION,
    )
    admit_parser.add_argument(
        "--durable-admission-root",
        type=Path,
        default=DEFAULT_DURABLE_ADMISSION,
    )
    admit_parser.add_argument("--plan-root", type=Path, default=DEFAULT_CONCRETE_PLAN_ROOT)
    args = parser.parse_args()
    paths = _paths_from_args(args)
    if args.command == "preflight":
        result = preflight(paths=paths, verify_source_hashes=args.verify_source_hashes)
    elif args.command == "materialize":
        result = materialize(paths=paths, max_days=args.max_days)
    elif args.command == "build":
        result = build_admission(admission_root=args.admission_root, paths=paths)
    elif args.command == "validate":
        result = validate_admission(
            args.admission_root,
            paths=paths,
            deep=not args.shallow,
        )
    elif args.command == "prepare-control-plan":
        result = prepare_control_only_plan(
            admission_root=args.admission_root,
            plan_root=args.plan_root,
            paths=paths,
        )
    else:
        result = admit_control_policy_and_prepare_plan(
            temporary_admission_root=args.temporary_admission_root,
            durable_admission_root=args.durable_admission_root,
            plan_root=args.plan_root,
            paths=paths,
        )
    print(json.dumps(_summary(result), indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
