#!/usr/bin/env python3
"""Build the outcome-blind M0 panel for the modeled-queue cooldown successor.

Each frozen v1 Development day is replayed exactly once with the Python
baseline and without a duration treatment.  A lightweight emitter receives the
fill-visible ``m0_context`` already constructed inside ``models/backtest_tick``.
The emitted rows are admitted only after their opportunity identities and
mechanics agree with the immutable v1 census.

This runner deliberately does not open duration-arm traces or interpret replay
PnL.  Its queue evidence remains modeled/unknown and cannot acquire strict
raw-native queue authority.
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
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from data_paths import data_root
from models import backtest_tick as bt
from research.families.f05_fill_quality_quote_ev.audit import (
    multiscale_ema_add_wait_incremental_value_v1_1_study as v1_window_loader,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    multiscale_ema_boolean_cooldown_duration_policy_study as v1_study,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_features import (
    M0_REQUIRED_FIELDS,
    FeatureContractError,
    validate_m0_context,
)

ROOT = Path(__file__).resolve().parents[4]
DATA_ROOT = data_root(ROOT)
IDENTITY = (
    "causal_multichannel_window_boolean_cooldown_duration_v2_"
    "owner_modeled_queue_m0_panel_v1"
)
SCHEMA_VERSION = f"{IDENTITY}.v1"
SOURCE_IDENTITY = "multiscale_ema_boolean_cooldown_duration_policy_v1"
SOURCE_ROOT = DATA_ROOT / (
    "reports/"
    "multiscale_ema_boolean_cooldown_duration_policy_v1_20260810"
)
DEFAULT_OUTPUT = DATA_ROOT / (
    "reports/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_20260810/"
    "owner_modeled_queue_m0_panel_v1"
)

EXPECTED_SOURCE_ADMISSION_SHA256 = (
    "a203efbf985848b7b24486a9e36ac18286e22dc22c057ad73b2d23f561c775cb"
)
EXPECTED_SOURCE_CENSUS_MANIFEST_SHA256 = (
    "1e8c47ae15e4badfa1aedd7cdfaaef0fd3ce9f8e4bc5438e960384cec835a033"
)
EXPECTED_SOURCE_ADMISSION_IDENTITY_SHA256 = (
    "f168fe4324c7210285b0dfde5a43533a9790bc587749332b9c4b6c7c85a877ed"
)
EXPECTED_V1_WINDOW_SPEC_SHA256 = (
    "b59f9f5a3c9cbdd1fa714abe6ddf8ef23e19654374c354a6840e6f943a7c6908"
)
EXPECTED_V1_REPLAY_PLAN_SHA256 = (
    "5c47033e67b75a9cbef6c336825dbea713b117c54674828d956f6626886fb7d4"
)
EXPECTED_V1_OPERATIONAL_CONFIG_SHA256 = (
    "62a6add8d46c2695205e278ecb41bcaa16dc8199e683ef9114c21f6118b04e18"
)
EXPECTED_V1_REPLAY_BASELINE_SHA256 = (
    "1070d280f8679689ffb07733b7ae91226000f987e35ce6a993bf79090d65e047"
)
ARCHIVED_REPLAY_BASELINE_RELATIVE = (
    "documents/research/families/f10_live_replay_attribution/docs/"
    "current_live_held_ber_replay_baseline_40d_20260809.json"
)
EXPECTED_DEVELOPMENT_DAYS = 40
EXPECTED_OPPORTUNITIES = 8_600
MAX_DAY_WORKERS = 1
QUEUE_PATH_SEMANTICS = (
    "native_l2_exact_level_replay_model_without_exchange_queue_authority"
)
FILL_CLOCK_SEMANTICS = (
    "native_exchange_event_revealed_at_replay_event_clock_"
    "no_live_receive_time_claim"
)

FORBIDDEN_OUTPUT_COLUMNS = frozenset(
    {
        "assignment_to_washout_value_usdc",
        "censor_time_mid_mark_usdc",
        "censor_time_executable_mark_usdc",
        "terminal_pnl_usdc",
        "closed_campaign_value_usdc",
        "duration_policy_id",
        "candidate_policy_id",
        "reward",
    }
)


class ModeledM0PanelError(RuntimeError):
    """Raised when source identity, replay, or atomic admission drifts."""


@dataclass(frozen=True, slots=True)
class SourcePart:
    utc_day: str
    opportunity_count: int
    census_data_path: Path
    census_data_sha256: str
    census_manifest_path: Path
    census_manifest_sha256: str


@dataclass(frozen=True, slots=True)
class SourceBinding:
    root: Path
    admission_path: Path
    admission_sha256: str
    admission_identity_sha256: str
    census_manifest_path: Path
    census_manifest_sha256: str
    census_execution_identity_sha256: str
    archived_replay_baseline_path: Path
    archived_replay_baseline_sha256: str
    ordered_utc_days: tuple[str, ...]
    parts: tuple[SourcePart, ...]

    def part_for_day(self, day: str) -> SourcePart:
        for part in self.parts:
            if part.utc_day == day:
                return part
        raise ModeledM0PanelError(f"day is outside the admitted v1 census: {day}")


@dataclass(frozen=True, slots=True)
class M0CaptureReceipt:
    snapshot_id: str
    assignment_id: str
    policy_input_valid: bool
    fallback_policy_id: str | None
    fallback_reason: str | None
    source_bundle_sha256: str


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ModeledM0PanelError(f"JSON artifact must be an object: {path}")
    return payload


def _fsync_file(path: Path) -> None:
    with Path(path).open("rb") as handle:
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.tmp-{uuid.uuid4().hex}"
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


def _validate_hash(path: Path, expected: str, *, role: str) -> None:
    if not Path(path).is_file() or _sha256_file(path) != str(expected):
        raise ModeledM0PanelError(f"{role} hash drifted: {path}")


def _dependency_bindings() -> tuple[dict[str, str], ...]:
    paths = (
        ("m0_panel_runner", Path(__file__)),
        ("python_replay", Path(bt.__file__)),
        ("v1_census_and_window_loader", Path(v1_study.__file__)),
        ("v1_window_source_loader", Path(v1_window_loader.__file__)),
        ("native_window_loader", Path(v1_window_loader.native_runner.__file__)),
        (
            "control_overlay_loader",
            Path(v1_window_loader.control_repair.__file__),
        ),
        (
            "m0_feature_contract",
            ROOT
            / "research/families/f05_fill_quality_quote_ev/audit/"
            "causal_multichannel_window_boolean_cooldown_features.py",
        ),
    )
    bindings: list[dict[str, str]] = []
    for role, path in paths:
        resolved = path.expanduser().resolve()
        if not resolved.is_file():
            raise ModeledM0PanelError(f"required dependency is missing: {resolved}")
        bindings.append(
            {"role": role, "path": str(resolved), "sha256": _sha256_file(resolved)}
        )
    return tuple(bindings)


def _resolve_contract_path(raw_path: str) -> Path:
    path = Path(str(raw_path)).expanduser()
    if not path.is_absolute():
        path = ROOT / path
    return path.resolve()


def _replay_source_contract(source: SourceBinding) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Load execution inputs without consulting the mutable current pointer.

    The v1 helper's old ``_spec_and_plan`` also validates a governance pointer
    that was intentionally advanced after v1 admission.  That pointer is not a
    replay input.  This successor validates the immutable baseline copy in the
    ORICO admission plus the exact config, plan, windows, and control overlay
    that actually construct the modeled-queue baseline.
    """

    spec_path = Path(v1_window_loader.SPEC).expanduser().resolve()
    plan_path = Path(v1_window_loader.PLAN).expanduser().resolve()
    _validate_hash(
        spec_path,
        EXPECTED_V1_WINDOW_SPEC_SHA256,
        role="v1 window-source spec",
    )
    _validate_hash(
        plan_path,
        EXPECTED_V1_REPLAY_PLAN_SHA256,
        role="v1 replay execution plan",
    )
    _validate_hash(
        source.archived_replay_baseline_path,
        EXPECTED_V1_REPLAY_BASELINE_SHA256,
        role="admitted v1 replay baseline",
    )
    spec = _load_json(spec_path)
    plan = _load_json(plan_path)
    if spec.get("identity") != v1_window_loader.IDENTITY:
        raise ModeledM0PanelError("v1 window-source identity drifted")
    source_contract = spec.get("source_contract") or {}
    denominator = source_contract.get("denominator_source_spec") or {}
    config = source_contract.get("operational_config") or {}
    execution_plan = source_contract.get("execution_plan") or {}
    config_path = _resolve_contract_path(str(config.get("path", "")))
    if (
        str(denominator.get("sha256", ""))
        != EXPECTED_V1_REPLAY_BASELINE_SHA256
        or str(config.get("sha256", ""))
        != EXPECTED_V1_OPERATIONAL_CONFIG_SHA256
        or str(execution_plan.get("sha256", ""))
        != EXPECTED_V1_REPLAY_PLAN_SHA256
        or _resolve_contract_path(str(execution_plan.get("path", "")))
        != plan_path
    ):
        raise ModeledM0PanelError("v1 replay source contract drifted")
    _validate_hash(
        config_path,
        EXPECTED_V1_OPERATIONAL_CONFIG_SHA256,
        role="v1 operational config",
    )
    spec_days = tuple(
        str(day)
        for day in (spec.get("development_denominator") or {}).get(
            "ordered_utc_days", ()
        )
    )
    plan_days = tuple(
        str(row.get("utc_day", ""))
        for row in (plan.get("identity_payload") or {}).get("days", ())
    )
    if spec_days != source.ordered_utc_days or plan_days != source.ordered_utc_days:
        raise ModeledM0PanelError("v1 replay day denominator drifted")
    binding = {
        "window_source_spec_path": str(spec_path),
        "window_source_spec_sha256": EXPECTED_V1_WINDOW_SPEC_SHA256,
        "replay_plan_path": str(plan_path),
        "replay_plan_sha256": EXPECTED_V1_REPLAY_PLAN_SHA256,
        "operational_config_path": str(config_path),
        "operational_config_sha256": EXPECTED_V1_OPERATIONAL_CONFIG_SHA256,
        "archived_replay_baseline_path": str(
            source.archived_replay_baseline_path
        ),
        "archived_replay_baseline_sha256": (
            source.archived_replay_baseline_sha256
        ),
        "superseded_governance_pointer_is_execution_input": False,
        "mutable_operational_pointer_read": False,
    }
    return spec, plan, binding


def _execution_identity(source: SourceBinding) -> dict[str, Any]:
    _, _, replay_source_binding = _replay_source_contract(source)
    payload: dict[str, Any] = {
        "identity": IDENTITY,
        "source_identity": SOURCE_IDENTITY,
        "source_admission_sha256": source.admission_sha256,
        "source_admission_identity_sha256": source.admission_identity_sha256,
        "source_census_manifest_sha256": source.census_manifest_sha256,
        "source_census_execution_identity_sha256": (
            source.census_execution_identity_sha256
        ),
        "ordered_utc_days": list(source.ordered_utc_days),
        "replay_engine": "python",
        "replay_count_per_day": 1,
        "duration_treatment_applied": False,
        "exchange_book_event_tape_supplied": False,
        "exact_queue_policy_eligible": False,
        "queue_path_semantics": QUEUE_PATH_SEMANTICS,
        "economic_outcomes_read": False,
        "arm_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "dependencies": list(_dependency_bindings()),
        "replay_source_binding": replay_source_binding,
        "m0_required_fields": list(M0_REQUIRED_FIELDS),
    }
    payload["execution_identity_sha256"] = _canonical_sha256(payload)
    return payload


def _admission_inventory(admission: Mapping[str, Any]) -> dict[str, str]:
    rows = admission.get("files")
    if not isinstance(rows, list) or not rows:
        raise ModeledM0PanelError("v1 admission file inventory is missing")
    inventory: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise ModeledM0PanelError("v1 admission file inventory row is invalid")
        relative_path = str(row.get("relative_path", ""))
        sha256 = str(row.get("sha256", ""))
        if not relative_path or len(sha256) != 64 or relative_path in inventory:
            raise ModeledM0PanelError("v1 admission file inventory drifted")
        inventory[relative_path] = sha256
    return inventory


def load_source_binding(source_root: Path = SOURCE_ROOT) -> SourceBinding:
    root = Path(source_root).expanduser().resolve()
    admission_path = root / "admission_manifest.json"
    census_manifest_path = root / "execution/census_manifest.json"
    _validate_hash(
        admission_path,
        EXPECTED_SOURCE_ADMISSION_SHA256,
        role="v1 ORICO admission manifest",
    )
    _validate_hash(
        census_manifest_path,
        EXPECTED_SOURCE_CENSUS_MANIFEST_SHA256,
        role="v1 census manifest",
    )
    admission = _load_json(admission_path)
    if (
        admission.get("identity") != SOURCE_IDENTITY
        or admission.get("admission_identity_sha256")
        != EXPECTED_SOURCE_ADMISSION_IDENTITY_SHA256
        or Path(str(admission.get("target_root", ""))).expanduser().resolve()
        != root
    ):
        raise ModeledM0PanelError("v1 ORICO admission identity drifted")
    permissions = admission.get("permissions") or {}
    if (
        permissions.get("validation_read") is not False
        or permissions.get("sealed_holdout_read") is not False
    ):
        raise ModeledM0PanelError("v1 source consumed locked evidence")
    inventory = _admission_inventory(admission)
    census_relative = "execution/census_manifest.json"
    if inventory.get(census_relative) != EXPECTED_SOURCE_CENSUS_MANIFEST_SHA256:
        raise ModeledM0PanelError("v1 census manifest is not admission-bound")
    archived_replay_baseline_path = root / ARCHIVED_REPLAY_BASELINE_RELATIVE
    if (
        inventory.get(ARCHIVED_REPLAY_BASELINE_RELATIVE)
        != EXPECTED_V1_REPLAY_BASELINE_SHA256
    ):
        raise ModeledM0PanelError("v1 replay baseline is not admission-bound")
    _validate_hash(
        archived_replay_baseline_path,
        EXPECTED_V1_REPLAY_BASELINE_SHA256,
        role="admitted v1 replay baseline",
    )

    census = _load_json(census_manifest_path)
    days = tuple(str(day) for day in census.get("ordered_utc_days", ()))
    raw_parts = census.get("parts")
    if (
        census.get("identity") != SOURCE_IDENTITY
        or census.get("status") != "formal_full_development_census"
        or census.get("economic_outcomes_read") is not False
        or census.get("validation_read") is not False
        or census.get("sealed_holdout_read") is not False
        or int(census.get("day_count", -1)) != EXPECTED_DEVELOPMENT_DAYS
        or int(census.get("opportunity_count", -1)) != EXPECTED_OPPORTUNITIES
        or len(days) != EXPECTED_DEVELOPMENT_DAYS
        or len(set(days)) != len(days)
        or not isinstance(raw_parts, list)
        or len(raw_parts) != len(days)
    ):
        raise ModeledM0PanelError("v1 frozen census denominator drifted")

    source_parts: list[SourcePart] = []
    parts_by_day = {str(row.get("utc_day", "")): row for row in raw_parts}
    if set(parts_by_day) != set(days):
        raise ModeledM0PanelError("v1 census part days drifted")
    for day in days:
        row = parts_by_day[day]
        census_data_path = root / f"execution/census/{day}/opportunities.parquet"
        census_day_manifest = root / f"execution/census/{day}/manifest.json"
        data_relative = f"execution/census/{day}/opportunities.parquet"
        manifest_relative = f"execution/census/{day}/manifest.json"
        data_sha256 = str(row.get("data_sha256", ""))
        manifest_sha256 = str(row.get("manifest_sha256", ""))
        if (
            inventory.get(data_relative) != data_sha256
            or inventory.get(manifest_relative) != manifest_sha256
        ):
            raise ModeledM0PanelError(f"{day} census is not admission-bound")
        _validate_hash(census_data_path, data_sha256, role=f"{day} v1 census")
        _validate_hash(
            census_day_manifest,
            manifest_sha256,
            role=f"{day} v1 census manifest",
        )
        source_parts.append(
            SourcePart(
                utc_day=day,
                opportunity_count=int(row.get("opportunity_count", -1)),
                census_data_path=census_data_path,
                census_data_sha256=data_sha256,
                census_manifest_path=census_day_manifest,
                census_manifest_sha256=manifest_sha256,
            )
        )
    if sum(part.opportunity_count for part in source_parts) != EXPECTED_OPPORTUNITIES:
        raise ModeledM0PanelError("v1 source part opportunity count drifted")
    return SourceBinding(
        root=root,
        admission_path=admission_path,
        admission_sha256=EXPECTED_SOURCE_ADMISSION_SHA256,
        admission_identity_sha256=EXPECTED_SOURCE_ADMISSION_IDENTITY_SHA256,
        census_manifest_path=census_manifest_path,
        census_manifest_sha256=EXPECTED_SOURCE_CENSUS_MANIFEST_SHA256,
        census_execution_identity_sha256=str(
            census.get("execution_identity_sha256", "")
        ),
        archived_replay_baseline_path=archived_replay_baseline_path,
        archived_replay_baseline_sha256=EXPECTED_V1_REPLAY_BASELINE_SHA256,
        ordered_utc_days=days,
        parts=tuple(source_parts),
    )


class ModeledM0CaptureEmitter:
    """Capture only replay-provided M0 state; never build features or labels."""

    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []
        self._snapshot_ids: set[str] = set()

    def capture_exposure_fill(
        self,
        *,
        assignment_id: str,
        fill_event_id: str,
        client_order_id: str,
        lineage_id: str,
        lineage_revision: int,
        partial_fill_ordinal: int,
        partial_fill_qty_btc: float,
        fill_exchange_ts_ns: int,
        fill_visible_ts_ns: int,
        m0_context: Mapping[str, Any],
    ) -> M0CaptureReceipt:
        try:
            m0 = validate_m0_context(m0_context)
        except FeatureContractError as exc:
            raise ModeledM0PanelError(f"invalid replay M0 context: {exc}") from exc
        if (
            str(m0["queue_state_before_fill"]) != "unknown"
            or m0["queue_ahead_before_fill_btc"] is not None
            or str(m0["target_price_displayed_qty_status"]) != "unknown"
            or m0["target_price_displayed_qty_btc"] is not None
            or bool(m0["target_price_displayed_qty_known"])
        ):
            raise ModeledM0PanelError(
                "modeled-queue M0 capture attempted to claim exact queue state"
            )
        if (
            int(fill_visible_ts_ns) != int(m0["fill_visible_ts_ns"])
            or int(fill_exchange_ts_ns) != int(fill_visible_ts_ns)
            or int(partial_fill_ordinal) != int(m0["partial_fill_ordinal"])
            or not math.isclose(
                float(partial_fill_qty_btc),
                float(m0["fill_qty_btc"]),
                rel_tol=0.0,
                abs_tol=1e-12,
            )
        ):
            raise ModeledM0PanelError("emitter arguments disagree with M0 context")
        identity_payload = {
            "identity": IDENTITY,
            "assignment_id": str(assignment_id),
            "fill_event_id": str(fill_event_id),
            "client_order_id": str(client_order_id),
            "lineage_id": str(lineage_id),
            "lineage_revision": int(lineage_revision),
            "partial_fill_ordinal": int(partial_fill_ordinal),
            "fill_visible_ts_ns": int(fill_visible_ts_ns),
        }
        snapshot_id = f"modeled-m0-{_canonical_sha256(identity_payload)}"
        if snapshot_id in self._snapshot_ids:
            raise ModeledM0PanelError("modeled M0 snapshot identity collided")
        self._snapshot_ids.add(snapshot_id)
        source_bundle_sha256 = _canonical_sha256(
            {
                "snapshot_identity": identity_payload,
                "m0_context": m0,
                "exact_queue_policy_eligible": False,
            }
        )
        self.records.append(
            {
                "snapshot_id": snapshot_id,
                "assignment_id": str(assignment_id),
                "fill_event_id": str(fill_event_id),
                "client_order_id": str(client_order_id),
                "lineage_id": str(lineage_id),
                "lineage_revision": int(lineage_revision),
                "fill_exchange_ts_ns": int(fill_exchange_ts_ns),
                "source_bundle_sha256": source_bundle_sha256,
                "m0_context": m0,
            }
        )
        return M0CaptureReceipt(
            snapshot_id=snapshot_id,
            assignment_id=str(assignment_id),
            policy_input_valid=True,
            fallback_policy_id=None,
            fallback_reason=None,
            source_bundle_sha256=source_bundle_sha256,
        )

    def audit(self) -> dict[str, Any]:
        return {
            "schema_version": f"{SCHEMA_VERSION}.capture_emitter_audit",
            "snapshots_emitted": len(self.records),
            "feature_block": "M0_only",
            "exact_queue_policy_eligible": False,
            "economic_outcomes_read": False,
            "arm_outcomes_read": False,
        }


def _v1_opportunity_id(day: str, row: Mapping[str, Any]) -> str:
    return _canonical_sha256(
        {
            "schema_version": "cooldown_duration_opportunity_identity.v1",
            "utc_day": str(day),
            "exposure_fill_ordinal": int(row["exposure_fill_ordinal"]),
            "fill_visible_ts_ms": int(row["fill_visible_ts_ms"]),
            "side": str(row["side"]),
            "role_at_fill": str(row["role_at_fill"]),
            "order_id": int(row["order_id"]),
            "campaign_id": int(row["campaign_id"]),
            "baseline_duration_ms": float(row["baseline_duration_ms"]),
            "fill_clock_semantics": str(row["fill_clock_semantics"]),
            "live_receive_time_authority": bool(row["live_receive_time_authority"]),
        }
    )


def _load_v1_census_day(source: SourceBinding, day: str) -> pd.DataFrame:
    part = source.part_for_day(day)
    _validate_hash(
        part.census_manifest_path,
        part.census_manifest_sha256,
        role=f"{day} v1 census manifest",
    )
    _validate_hash(
        part.census_data_path,
        part.census_data_sha256,
        role=f"{day} v1 census",
    )
    day_manifest = _load_json(part.census_manifest_path)
    if (
        day_manifest.get("identity") != SOURCE_IDENTITY
        or day_manifest.get("utc_day") != day
        or day_manifest.get("economic_outcomes_read") is not False
        or day_manifest.get("validation_read") is not False
        or day_manifest.get("sealed_holdout_read") is not False
        or int(day_manifest.get("opportunity_count", -1))
        != part.opportunity_count
        or (day_manifest.get("book_source_contract") or {}).get(
            "exact_queue_policy_eligible"
        )
        is not False
    ):
        raise ModeledM0PanelError(f"{day} v1 census manifest drifted")
    frame = pd.read_parquet(part.census_data_path)
    if (
        len(frame) != part.opportunity_count
        or frame["opportunity_id"].duplicated().any()
        or frame["exposure_fill_ordinal"].duplicated().any()
        or not frame["exact_queue_policy_eligible"].eq(False).all()
        or not frame["queue_path_semantics"].eq(QUEUE_PATH_SEMANTICS).all()
        or FORBIDDEN_OUTPUT_COLUMNS.intersection(frame.columns)
    ):
        raise ModeledM0PanelError(f"{day} v1 census data drifted")
    return frame.sort_values("exposure_fill_ordinal", kind="stable").reset_index(
        drop=True
    )


def _assert_exact(left: Any, right: Any, *, field: str, day: str) -> None:
    if left != right:
        raise ModeledM0PanelError(f"{day} replay/census {field} drifted")


def _assert_float(left: Any, right: Any, *, field: str, day: str) -> None:
    if not math.isclose(
        float(left), float(right), rel_tol=0.0, abs_tol=1e-12
    ):
        raise ModeledM0PanelError(f"{day} replay/census {field} drifted")


def build_m0_panel(
    *,
    day: str,
    census: pd.DataFrame,
    result: Mapping[str, Any],
    emitter: ModeledM0CaptureEmitter,
    source_part: SourcePart,
) -> pd.DataFrame:
    """Validate a baseline replay against v1 and project only M0 state."""

    traces = list(result.get("_cooldown_duration_opportunity_trace") or ())
    receipts = list(result.get("_cooldown_v2_snapshot_receipts") or ())
    emitter_audit = dict(result.get("_cooldown_v2_snapshot_emitter_audit") or {})
    if (
        len(traces) != len(census)
        or len(receipts) != len(census)
        or len(emitter.records) != len(census)
        or int(emitter_audit.get("snapshots_emitted", -1)) != len(census)
        or emitter_audit.get("economic_outcomes_read") is not False
        or emitter_audit.get("arm_outcomes_read") is not False
    ):
        raise ModeledM0PanelError(f"{day} replay M0 denominator drifted")

    trace_by_ordinal: dict[int, Mapping[str, Any]] = {}
    for row in traces:
        ordinal = int(row["exposure_fill_ordinal"])
        if ordinal in trace_by_ordinal:
            raise ModeledM0PanelError(f"{day} replay ordinal is duplicated")
        trace_by_ordinal[ordinal] = row
    capture_by_snapshot = {str(row["snapshot_id"]): row for row in emitter.records}
    if len(capture_by_snapshot) != len(emitter.records):
        raise ModeledM0PanelError(f"{day} captured snapshot identity collided")
    receipt_by_ordinal: dict[int, Mapping[str, Any]] = {}
    for row in receipts:
        ordinal = int(row["exposure_fill_ordinal"])
        if ordinal in receipt_by_ordinal:
            raise ModeledM0PanelError(f"{day} snapshot receipt ordinal duplicated")
        receipt_by_ordinal[ordinal] = row

    expected_ordinals = set(census["exposure_fill_ordinal"].astype(int))
    if (
        set(trace_by_ordinal) != expected_ordinals
        or set(receipt_by_ordinal) != expected_ordinals
    ):
        raise ModeledM0PanelError(f"{day} replay/census ordinal set drifted")

    output_rows: list[dict[str, Any]] = []
    exact_fields = (
        "exposure_fill_ordinal",
        "fill_visible_ts_ms",
        "fill_exchange_ts_ms",
        "side",
        "role_at_fill",
        "order_id",
        "campaign_id",
        "fill_clock_semantics",
        "live_receive_time_authority",
    )
    float_fields = (
        "inventory_before_fill_btc",
        "inventory_after_fill_btc",
        "fill_qty_btc",
        "consecutive_units_after",
        "baseline_duration_ms",
    )
    for census_row in census.to_dict("records"):
        ordinal = int(census_row["exposure_fill_ordinal"])
        trace = trace_by_ordinal[ordinal]
        receipt = receipt_by_ordinal[ordinal]
        for field in exact_fields:
            _assert_exact(trace[field], census_row[field], field=field, day=day)
        for field in float_fields:
            _assert_float(trace[field], census_row[field], field=field, day=day)
        opportunity_id = _v1_opportunity_id(day, trace)
        _assert_exact(
            opportunity_id,
            str(census_row["opportunity_id"]),
            field="opportunity_id",
            day=day,
        )
        snapshot_id = str(receipt["snapshot_id"])
        if snapshot_id not in capture_by_snapshot:
            raise ModeledM0PanelError(f"{day} snapshot receipt lacks captured M0")
        captured = capture_by_snapshot[snapshot_id]
        m0 = validate_m0_context(captured["m0_context"])
        for field, expected in (
            ("side", trace["side"]),
            ("role_at_fill", trace["role_at_fill"]),
            ("inventory_before_fill_btc", trace["inventory_before_fill_btc"]),
            ("inventory_after_fill_btc", trace["inventory_after_fill_btc"]),
            ("fill_qty_btc", trace["fill_qty_btc"]),
            ("consecutive_units_after", trace["consecutive_units_after"]),
            ("baseline_duration_ms", trace["baseline_duration_ms"]),
        ):
            if isinstance(expected, (float, np.floating)):
                _assert_float(m0[field], expected, field=f"m0.{field}", day=day)
            else:
                _assert_exact(m0[field], expected, field=f"m0.{field}", day=day)
        expected_ts_ns = int(trace["fill_visible_ts_ms"]) * 1_000_000
        if (
            int(m0["assignment_ts_ns"]) != expected_ts_ns
            or int(m0["fill_visible_ts_ns"]) != expected_ts_ns
            or int(captured["fill_exchange_ts_ns"]) != expected_ts_ns
        ):
            raise ModeledM0PanelError(f"{day} M0 fill-visible clock drifted")
        if (
            str(m0["queue_state_before_fill"]) != "unknown"
            or m0["queue_ahead_before_fill_btc"] is not None
            or str(m0["target_price_displayed_qty_status"]) != "unknown"
            or m0["target_price_displayed_qty_btc"] is not None
        ):
            raise ModeledM0PanelError(f"{day} modeled M0 invented queue evidence")
        if (
            str(receipt["side"]) != str(trace["side"])
            or str(receipt["role_at_fill"]) != str(trace["role_at_fill"])
            or int(receipt["campaign_id"]) != int(trace["campaign_id"])
            or not bool(receipt["policy_input_valid"])
        ):
            raise ModeledM0PanelError(f"{day} M0 receipt mechanics drifted")
        output_rows.append(
            {
                "utc_day": day,
                "opportunity_id": opportunity_id,
                "exposure_fill_ordinal": ordinal,
                "fill_visible_ts_ms": int(trace["fill_visible_ts_ms"]),
                "fill_exchange_ts_ms": int(trace["fill_exchange_ts_ms"]),
                "order_id": int(trace["order_id"]),
                "campaign_id": int(trace["campaign_id"]),
                "snapshot_id": snapshot_id,
                "assignment_id": str(captured["assignment_id"]),
                "fill_event_id": str(captured["fill_event_id"]),
                "client_order_id": str(captured["client_order_id"]),
                "lineage_id": str(captured["lineage_id"]),
                "lineage_revision": int(captured["lineage_revision"]),
                "source_bundle_sha256": str(captured["source_bundle_sha256"]),
                **m0,
                "replay_engine": "python",
                "duration_treatment_applied": False,
                "exchange_book_event_tape_supplied": False,
                "exact_queue_policy_eligible": False,
                "queue_path_semantics": QUEUE_PATH_SEMANTICS,
                "economic_outcomes_read": False,
                "arm_outcomes_read": False,
                "v1_census_data_sha256": source_part.census_data_sha256,
                "v1_census_manifest_sha256": source_part.census_manifest_sha256,
            }
        )
    frame = pd.DataFrame(output_rows)
    missing_m0 = sorted(set(M0_REQUIRED_FIELDS) - set(frame.columns))
    if (
        missing_m0
        or FORBIDDEN_OUTPUT_COLUMNS.intersection(frame.columns)
        or frame["opportunity_id"].duplicated().any()
        or frame["exposure_fill_ordinal"].duplicated().any()
        or not frame["exact_queue_policy_eligible"].eq(False).all()
        or not frame["queue_state_before_fill"].eq("unknown").all()
        or frame["queue_ahead_before_fill_btc"].notna().any()
    ):
        raise ModeledM0PanelError(f"{day} projected M0 panel failed admission")
    return frame.sort_values("exposure_fill_ordinal", kind="stable").reset_index(
        drop=True
    )


def _day_root(output: Path, day: str) -> Path:
    return Path(output).expanduser().resolve() / "days" / day


def _schema_payload(path: Path) -> list[dict[str, str]]:
    schema = pq.read_schema(path)
    return [{"name": field.name, "type": str(field.type)} for field in schema]


def _validate_day(
    *,
    output: Path,
    day: str,
    execution_identity_sha256: str,
    source_part: SourcePart,
) -> dict[str, Any]:
    root = _day_root(output, day)
    data_path = root / "m0_context.parquet"
    manifest_path = root / "manifest.json"
    success_path = root / "_SUCCESS"
    if not data_path.is_file() or not manifest_path.is_file() or not success_path.is_file():
        raise ModeledM0PanelError(f"{day} M0 admission is incomplete")
    manifest = _load_json(manifest_path)
    success = _load_json(success_path)
    if (
        manifest.get("identity") != IDENTITY
        or manifest.get("utc_day") != day
        or manifest.get("execution_identity_sha256")
        != execution_identity_sha256
        or manifest.get("source_census_data_sha256")
        != source_part.census_data_sha256
        or manifest.get("source_census_manifest_sha256")
        != source_part.census_manifest_sha256
        or manifest.get("economic_outcomes_read") is not False
        or manifest.get("arm_outcomes_read") is not False
        or manifest.get("exact_queue_policy_eligible") is not False
        or manifest.get("duration_treatment_applied") is not False
        or success.get("manifest_sha256") != _sha256_file(manifest_path)
    ):
        raise ModeledM0PanelError(f"{day} M0 admission identity drifted")
    _validate_hash(data_path, str(manifest.get("data_sha256", "")), role=f"{day} M0")
    frame = pd.read_parquet(data_path)
    if (
        len(frame) != int(manifest.get("row_count", -1))
        or len(frame) != source_part.opportunity_count
        or _canonical_sha256(_schema_payload(data_path))
        != manifest.get("parquet_schema_sha256")
        or sorted(M0_REQUIRED_FIELDS) != sorted(manifest.get("m0_columns", ()))
        or frame["opportunity_id"].duplicated().any()
        or not frame["exact_queue_policy_eligible"].eq(False).all()
        or not frame["queue_state_before_fill"].eq("unknown").all()
        or frame["queue_ahead_before_fill_btc"].notna().any()
        or FORBIDDEN_OUTPUT_COLUMNS.intersection(frame.columns)
    ):
        raise ModeledM0PanelError(f"{day} M0 admission data drifted")
    return manifest


def _admit_day(
    *,
    output: Path,
    day: str,
    frame: pd.DataFrame,
    execution_identity: Mapping[str, Any],
    source_part: SourcePart,
    replay_audit: Mapping[str, Any],
) -> dict[str, Any]:
    destination = _day_root(output, day)
    execution_sha = str(execution_identity["execution_identity_sha256"])
    if destination.exists():
        return _validate_day(
            output=output,
            day=day,
            execution_identity_sha256=execution_sha,
            source_part=source_part,
        )
    parent = destination.parent
    parent.mkdir(parents=True, exist_ok=True)
    staging = parent / f".{day}.staging-{uuid.uuid4().hex}"
    staging.mkdir()
    try:
        data_path = staging / "m0_context.parquet"
        frame.to_parquet(data_path, index=False, compression="zstd")
        _fsync_file(data_path)
        schema_payload = _schema_payload(data_path)
        final_data_path = destination / data_path.name
        manifest = {
            "schema_version": f"{SCHEMA_VERSION}.day_manifest",
            "identity": IDENTITY,
            "status": "owner_modeled_queue_m0_day_admitted",
            "utc_day": day,
            "execution_identity_sha256": execution_sha,
            "data_path": str(final_data_path),
            "data_sha256": _sha256_file(data_path),
            "row_count": int(len(frame)),
            "parquet_schema": schema_payload,
            "parquet_schema_sha256": _canonical_sha256(schema_payload),
            "m0_columns": list(M0_REQUIRED_FIELDS),
            "source_census_data_path": str(source_part.census_data_path),
            "source_census_data_sha256": source_part.census_data_sha256,
            "source_census_manifest_path": str(source_part.census_manifest_path),
            "source_census_manifest_sha256": source_part.census_manifest_sha256,
            "replay_engine": "python",
            "replay_count": 1,
            "duration_treatment_applied": False,
            "exchange_book_event_tape_supplied": False,
            "exact_queue_policy_eligible": False,
            "queue_path_semantics": QUEUE_PATH_SEMANTICS,
            "replay_emitter_audit": dict(replay_audit),
            "economic_outcomes_read": False,
            "arm_outcomes_read": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        }
        manifest_path = staging / "manifest.json"
        with manifest_path.open("w", encoding="utf-8") as handle:
            json.dump(manifest, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        success_path = staging / "_SUCCESS"
        with success_path.open("w", encoding="utf-8") as handle:
            json.dump(
                {"manifest_sha256": _sha256_file(manifest_path)},
                handle,
                sort_keys=True,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        _fsync_directory(staging)
        os.replace(staging, destination)
        _fsync_directory(parent)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return _validate_day(
        output=output,
        day=day,
        execution_identity_sha256=execution_sha,
        source_part=source_part,
    )


def _load_replay_inputs(
    source: SourceBinding, day: str
) -> tuple[Any, dict[str, Any], dict[str, Any], Any]:
    spec, plan, replay_binding = _replay_source_contract(source)
    day_rows = {
        str(row.get("utc_day", "")): row
        for row in (plan.get("identity_payload") or {}).get("days", ())
    }
    if day not in day_rows:
        raise ModeledM0PanelError(f"{day} is outside the v1 replay plan")
    day_row = day_rows[day]
    window_row = day_row.get("window") or {}
    window_path = Path(str(window_row.get("path", ""))).expanduser().resolve()
    _validate_hash(
        window_path,
        str(window_row.get("sha256", "")),
        role=f"{day} v1 replay window",
    )
    window = v1_window_loader.native_runner._load_bound_window(window_path)
    control = (plan.get("identity_payload") or {}).get("control_sources") or {}
    control_path = Path(str(control.get("path", ""))).expanduser().resolve()
    _validate_hash(
        control_path,
        str(control.get("sha256", "")),
        role="v1 control overlay panel",
    )
    schedule = v1_window_loader.control_repair.load_admitted_control_schedule(
        control_path,
        panel_sha256=str(control.get("sha256", "")),
        panel_identity_sha256=str(control.get("panel_identity_sha256", "")),
        day=day,
    )
    raw_params, projection = v1_window_loader._offline_params(spec)
    raw_params["ber_exposure_add_only"] = False
    shared = {
        "ml_data": schedule.ml_data,
        "bbo_data": window.bbo_data,
        "l2_data": window.l2_data,
        "var_ti": window.var_ti,
        "var_retsq": window.var_retsq,
    }
    if bool(getattr(window, "exact_queue_policy_eligible", False)):
        raise ModeledM0PanelError(
            f"{day} unexpectedly acquired exact queue policy authority"
        )
    if shared.get("exchange_book_event_tape") is not None:
        raise ModeledM0PanelError(f"{day} modeled replay received a raw book tape")
    params = v1_study._prepare_base_params(raw_params, trace_opportunities=True)
    if bool(params.get("cooldown_duration_fork_enabled", False)):
        raise ModeledM0PanelError(f"{day} baseline projection enabled a treatment")
    return window, params, dict(shared), {
        "projection": projection,
        "replay_source_binding": replay_binding,
    }


def run_day(
    day: str,
    *,
    source: SourceBinding,
    output: Path = DEFAULT_OUTPUT,
) -> dict[str, Any]:
    source_part = source.part_for_day(day)
    execution_identity = _execution_identity(source)
    destination = _day_root(output, day)
    if destination.exists():
        return _validate_day(
            output=output,
            day=day,
            execution_identity_sha256=str(
                execution_identity["execution_identity_sha256"]
            ),
            source_part=source_part,
        )
    census = _load_v1_census_day(source, day)
    window, params, shared, _ = _load_replay_inputs(source, day)
    emitter = ModeledM0CaptureEmitter()
    params["trace_cooldown_duration_opportunities_max"] = len(census) + 1
    params["cooldown_v2_snapshot_emitter"] = emitter
    params["cooldown_duration_fork_enabled"] = False
    if shared.get("exchange_book_event_tape") is not None:
        raise ModeledM0PanelError("raw exchange-book tape is forbidden in M0 panel")
    result = bt._simulate_tick_with_engine(
        "python",
        window.trades,
        window.var_ts_ms,
        window.var_ssq,
        params,
        **shared,
    )
    frame = build_m0_panel(
        day=day,
        census=census,
        result=result,
        emitter=emitter,
        source_part=source_part,
    )
    return _admit_day(
        output=output,
        day=day,
        frame=frame,
        execution_identity=execution_identity,
        source_part=source_part,
        replay_audit=emitter.audit(),
    )


def _requested_days(source: SourceBinding, requested: Sequence[str]) -> tuple[str, ...]:
    if not requested:
        return source.ordered_utc_days
    selected = set(str(day) for day in requested)
    unknown = sorted(selected - set(source.ordered_utc_days))
    if unknown:
        raise ModeledM0PanelError(f"days are outside frozen Development: {unknown}")
    return tuple(day for day in source.ordered_utc_days if day in selected)


def _validate_workers(workers: int) -> int:
    if isinstance(workers, bool) or workers < 1 or workers > MAX_DAY_WORKERS:
        raise ModeledM0PanelError(
            f"workers must be between 1 and {MAX_DAY_WORKERS}; each replay loads "
            "one full modeled-queue day"
        )
    return int(workers)


def validate_panel(
    *, source: SourceBinding, output: Path = DEFAULT_OUTPUT
) -> dict[str, Any]:
    execution_identity = _execution_identity(source)
    execution_sha = str(execution_identity["execution_identity_sha256"])
    parts: list[dict[str, Any]] = []
    total_rows = 0
    schema_hashes: set[str] = set()
    for day in source.ordered_utc_days:
        source_part = source.part_for_day(day)
        manifest = _validate_day(
            output=output,
            day=day,
            execution_identity_sha256=execution_sha,
            source_part=source_part,
        )
        manifest_path = _day_root(output, day) / "manifest.json"
        parts.append(
            {
                "utc_day": day,
                "row_count": int(manifest["row_count"]),
                "data_path": str(_day_root(output, day) / "m0_context.parquet"),
                "data_sha256": str(manifest["data_sha256"]),
                "manifest_path": str(manifest_path),
                "manifest_sha256": _sha256_file(manifest_path),
            }
        )
        total_rows += int(manifest["row_count"])
        schema_hashes.add(str(manifest["parquet_schema_sha256"]))
    if total_rows != EXPECTED_OPPORTUNITIES or len(schema_hashes) != 1:
        raise ModeledM0PanelError("formal M0 panel denominator/schema drifted")
    payload = {
        "schema_version": f"{SCHEMA_VERSION}.panel_manifest",
        "identity": IDENTITY,
        "status": "owner_modeled_queue_m0_panel_admitted",
        "execution_identity": execution_identity,
        "ordered_utc_days": list(source.ordered_utc_days),
        "day_count": len(source.ordered_utc_days),
        "opportunity_count": total_rows,
        "m0_columns": list(M0_REQUIRED_FIELDS),
        "parquet_schema_sha256": next(iter(schema_hashes)),
        "parts": parts,
        "source_admission_path": str(source.admission_path),
        "source_admission_sha256": source.admission_sha256,
        "source_census_manifest_path": str(source.census_manifest_path),
        "source_census_manifest_sha256": source.census_manifest_sha256,
        "replay_engine": "python",
        "one_baseline_replay_per_day": True,
        "duration_treatment_applied": False,
        "exchange_book_event_tape_supplied": False,
        "exact_queue_policy_eligible": False,
        "queue_path_semantics": QUEUE_PATH_SEMANTICS,
        "economic_outcomes_read": False,
        "arm_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "research_supported_promotion_eligible": False,
        "owner_risk_accepted_successor_only": True,
        "action_authorized": False,
        "live_authorized": False,
    }
    _atomic_json(Path(output).expanduser().resolve() / "panel_manifest.json", payload)
    return payload


def preflight(
    *, source_root: Path = SOURCE_ROOT, output: Path = DEFAULT_OUTPUT
) -> dict[str, Any]:
    source = load_source_binding(source_root)
    identity = _execution_identity(source)
    destination = Path(output).expanduser().resolve()
    probe = destination if destination.exists() else destination.parent
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    free_bytes = shutil.disk_usage(probe).free
    return {
        "schema_version": f"{SCHEMA_VERSION}.preflight",
        "identity": IDENTITY,
        "status": "ready_for_outcome_blind_m0_replay",
        "execution_identity": identity,
        "source_root": str(source.root),
        "output_root": str(destination),
        "development_day_count": len(source.ordered_utc_days),
        "expected_opportunity_count": EXPECTED_OPPORTUNITIES,
        "max_day_workers": MAX_DAY_WORKERS,
        "output_filesystem_free_bytes": int(free_bytes),
        "economic_outcomes_read": False,
        "arm_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }


def run_panel(
    *,
    source: SourceBinding,
    days: Sequence[str],
    workers: int,
    output: Path = DEFAULT_OUTPUT,
) -> dict[str, Any]:
    _validate_workers(workers)
    manifests = [
        run_day(day, source=source, output=output)
        for day in _requested_days(source, days)
    ]
    requested = _requested_days(source, days)
    if requested == source.ordered_utc_days:
        return validate_panel(source=source, output=output)
    return {
        "schema_version": f"{SCHEMA_VERSION}.partial_run",
        "identity": IDENTITY,
        "status": "partial_development_m0_days_admitted_not_a_panel",
        "ordered_utc_days": list(requested),
        "day_count": len(requested),
        "row_count": sum(int(row["row_count"]) for row in manifests),
        "formal_panel_admitted": False,
        "economic_outcomes_read": False,
        "arm_outcomes_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("preflight", "run", "validate"))
    parser.add_argument("--days", nargs="*", default=())
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--source-root", type=Path, default=SOURCE_ROOT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    if args.command == "preflight":
        if args.days:
            raise ModeledM0PanelError("preflight does not accept a partial day list")
        payload = preflight(source_root=args.source_root, output=args.output)
    else:
        source = load_source_binding(args.source_root)
        if args.command == "run":
            payload = run_panel(
                source=source,
                days=args.days,
                workers=args.workers,
                output=args.output,
            )
        else:
            if args.days:
                raise ModeledM0PanelError("validate requires the complete 40-day panel")
            _validate_workers(args.workers)
            payload = validate_panel(source=source, output=args.output)
    print(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    main()
