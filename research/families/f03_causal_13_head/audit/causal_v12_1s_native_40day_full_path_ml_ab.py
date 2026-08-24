#!/usr/bin/env python3
"""Execution-only 40-day F03 cadence A/B runner and scorecard glue.

The formal pair is current-v9 causal-v12 10s ML-ON versus the true 1s
13-head ML-ON successor.  Preparation is economic-outcome blind.  Each day is
published atomically and can be resumed.  Validation and holdout are forbidden.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
import shutil
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from data_paths import external_cache_root, resolve_portable_path
from models.audit import experiment_scorecard_v2
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_2026_native_overlay_binding as candidate_binding,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_dual_overlay_ml_ab_replay as dual_abi,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_ml_ab_replay as candidate_abi,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_native_40day_full_path_ml_ab_execution_amendment as execution_amendment,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_v9_10s_control_overlay_repair as control_repair,
)
from research.families.f03_causal_13_head.audit import full_path_ml_ab

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = "causal_v12_1s_native_40day_full_path_ml_ab.v3"
PLAN_SCHEMA_VERSION = f"{SCHEMA_VERSION}.execution_plan"
DAY_SCHEMA_VERSION = f"{SCHEMA_VERSION}.day"
PANEL_SCHEMA_VERSION = f"{SCHEMA_VERSION}.panel"
IDENTITY = "causal_v12_1s_native_40day_v9_10s_vs_1s_ml_on_full_path_v3"
EXPECTED_DAY_COUNT = 40
ARMS = dual_abi.ARMS
DEFAULT_PRECOMMIT = ROOT / (
    "research/families/f03_causal_13_head/docs/"
    "causal_v12_1s_cadence_full_path_economic_precommit_v1_20260805.json"
)
DEFAULT_OUTPUT_ROOT = (
    external_cache_root(ROOT)
    / "replay_dag/f03_causal_v12_1s_native_40day_full_path_ml_ab_v3"
)
PLAN_FILENAME = "execution-plan.json"
PLAN_SUCCESS = "_PLAN_SUCCESS"
DAY_SUCCESS = "_SUCCESS"
PANEL_MANIFEST = "panel-manifest.json"
PANEL_SUCCESS = "_PANEL_SUCCESS"
STORAGE_RESERVE_BYTES = 60 * (1 << 30)
ESTIMATED_OUTPUT_BYTES = 4 * (1 << 30)
ACCOUNTING_TOLERANCE_USDC = 1e-6
CAMPAIGN_MAE_TRACE_MAX = 1_000_000
REPLAY_PARITY_ATOL = 1e-9
CAMPAIGN_MAE_TRACE_FIELD = "campaign_adverse_excursion_so_far"
CAMPAIGN_MAE_TRACE_SEMANTICS = (
    "decision_visible_running_minimum_campaign_pnl_since_campaign_start_usdc"
)


class NativeFullPathABError(ValueError):
    """Raised when the frozen A/B execution identity is incomplete."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 << 20), b""):
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


def _resolve_path(path: str | Path) -> Path:
    candidate = resolve_portable_path(path, root=ROOT)
    if not candidate.is_absolute():
        candidate = ROOT / candidate
    return candidate.resolve()


def _load_json(path: Path, *, role: str) -> dict[str, Any]:
    resolved = _resolve_path(path)
    if not resolved.is_file():
        raise NativeFullPathABError(f"missing {role}: {resolved}")
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise NativeFullPathABError(f"invalid {role}: {resolved}") from exc
    if not isinstance(payload, dict):
        raise NativeFullPathABError(f"{role} must be a JSON object")
    return payload


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
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


def _atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.tmp-{uuid.uuid4().hex}"
    try:
        with temporary.open("w", encoding="ascii") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _artifact(path_value: Any, sha_value: Any, *, role: str) -> dict[str, Any]:
    path = _resolve_path(str(path_value))
    if not path.is_file():
        raise NativeFullPathABError(f"missing {role}: {path}")
    observed = _sha256_file(path)
    if observed != str(sha_value):
        raise NativeFullPathABError(f"{role} SHA256 drift")
    return {"path": str(path), "sha256": observed, "size_bytes": path.stat().st_size}


def _artifact_row(row: Mapping[str, Any], *, role: str) -> dict[str, Any]:
    return _artifact(row.get("path"), row.get("sha256"), role=role)


def _storage_gate(output_root: Path) -> dict[str, Any]:
    resolved = output_root.expanduser().resolve()
    required_prefix = external_cache_root(ROOT).resolve()
    try:
        resolved.relative_to(required_prefix)
    except ValueError as exc:
        raise NativeFullPathABError(
            "large F03 A/B output must stay in the configured external cache"
        ) from exc
    probe = resolved if resolved.exists() else resolved.parent
    while not probe.exists():
        probe = probe.parent
    free = shutil.disk_usage(probe).free
    required = STORAGE_RESERVE_BYTES + int(2.5 * ESTIMATED_OUTPUT_BYTES)
    if free < required:
        raise NativeFullPathABError(
            f"external-cache storage gate failed: free={free}, required={required}"
        )
    return {"free_bytes": free, "required_bytes": required, "passed": True}


def _validate_precommit(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = _load_json(path, role="frozen F03 economic precommit")
    if payload.get("schema_version") != ("causal_v12_1s_cadence_full_path_economic_precommit.v1"):
        raise NativeFullPathABError("unsupported F03 economic precommit")
    intervention = payload.get("intervention") or {}
    if intervention.get("control") != "current_v9_causal_v12_10s_ml_on" or (
        intervention.get("candidate") != "causal_v12_1s_full_schema_13_head_ml_on"
    ):
        raise NativeFullPathABError("precommit is not the dual-ML-ON cadence estimand")
    if intervention.get("control_cadence_ms") != 10_000 or (
        intervention.get("candidate_cadence_ms") != 1_000
    ):
        raise NativeFullPathABError("precommit cadence identity drift")
    panel = payload.get("native_development_panel") or {}
    days = list(panel.get("days") or ())
    if len(days) != EXPECTED_DAY_COUNT or days != sorted(set(days)):
        raise NativeFullPathABError("precommit native denominator is not ordered 40 days")
    confirmation = payload.get("confirmation_panels") or {}
    expected_confirmation_panels = ("validation", "family_specific_sealed_holdout")
    for name in expected_confirmation_panels:
        row = confirmation.get(name)
        if not isinstance(row, Mapping) or not isinstance(row.get("days"), list):
            raise NativeFullPathABError(f"precommit {name} denominator is malformed")
        if row["days"]:
            raise NativeFullPathABError("this execution identity forbids Validation/holdout")
    if (
        confirmation.get(
            "research_supported_successor_requires_new_precommit_before_any_successor_outcome_read"
        )
        is not True
    ):
        raise NativeFullPathABError("precommit confirmation successor boundary drift")
    if set(confirmation) != {
        *expected_confirmation_panels,
        "research_supported_successor_requires_new_precommit_before_any_successor_outcome_read",
    }:
        raise NativeFullPathABError("precommit confirmation schema drift")
    access = payload.get("evidence_access_at_freeze") or {}
    if any(
        access.get(field) is not False
        for field in (
            "candidate_native_development_pnl_read",
            "validation_read",
            "sealed_holdout_read",
        )
    ):
        raise NativeFullPathABError("precommit outcome-access boundary is not frozen")
    score = payload.get("scorecard") or {}
    profile = _artifact(
        str(score.get("frozen_payload_path", "")),
        score.get("frozen_payload_sha256"),
        role="frozen action_alpha_v2 profile",
    )
    frozen_payload = _load_json(Path(profile["path"]), role="frozen score profile")
    if frozen_payload != experiment_scorecard_v2.score_profile_payload("action_alpha_v2"):
        raise NativeFullPathABError("frozen action_alpha_v2 payload differs from implementation")
    if score.get("contract") != experiment_scorecard_v2.score_profile_contract("action_alpha_v2"):
        raise NativeFullPathABError("action_alpha_v2 contract drift")
    return payload, {
        "path": str(path.resolve()),
        "sha256": _sha256_file(path),
        "score_profile": profile,
    }


def _validate_candidate_panel(path: Path, *, expected_days: Sequence[str]) -> dict[str, Any]:
    panel = _load_json(path, role="candidate 1s overlay panel")
    success = path.parent / candidate_binding.PANEL_SUCCESS_FILENAME
    if not success.is_file() or success.read_text(encoding="ascii").strip() != _sha256_file(path):
        raise NativeFullPathABError("candidate overlay panel atomic admission drift")
    if (
        panel.get("schema_version") != candidate_binding.PANEL_SCHEMA_VERSION
        or panel.get("identity") != candidate_binding.IDENTITY
        or panel.get("execution_input_eligible") is not True
        or panel.get("economic_outcomes_read") is not False
        or panel.get("completed_day_count") != EXPECTED_DAY_COUNT
    ):
        raise NativeFullPathABError("candidate overlay panel is not execution eligible")
    identity = panel.get("identity_payload")
    if not isinstance(identity, Mapping) or panel.get("panel_identity_sha256") != (
        _canonical_sha256(identity)
    ):
        raise NativeFullPathABError("candidate overlay panel identity cannot be reproduced")
    if list(identity.get("ordered_days") or ()) != list(expected_days):
        raise NativeFullPathABError("candidate overlay denominator differs from precommit")
    rows = identity.get("daily_overlays")
    if not isinstance(rows, list) or len(rows) != EXPECTED_DAY_COUNT:
        raise NativeFullPathABError("candidate overlay daily bindings are incomplete")
    by_day = {str(row.get("utc_day")): dict(row) for row in rows if isinstance(row, Mapping)}
    if list(by_day) != list(expected_days):
        raise NativeFullPathABError("candidate overlay daily order differs from precommit")
    for day, row in by_day.items():
        _artifact(
            row["overlay_manifest_path"],
            row["overlay_manifest_sha256"],
            role=f"{day} candidate manifest",
        )
        _artifact(row["overlay_path"], row["overlay_sha256"], role=f"{day} candidate overlay")
    return {
        "path": str(path.resolve()),
        "sha256": _sha256_file(path),
        "panel_identity_sha256": panel["panel_identity_sha256"],
        "bundle_meta_sha256": identity["bundle_meta_sha256"],
        "days": by_day,
    }


def _validate_control_sources(
    path: Path,
    *,
    expected_days: Sequence[str],
) -> dict[str, Any]:
    try:
        panel = control_repair.validate_panel(path)
    except control_repair.ControlOverlayRepairError as exc:
        raise NativeFullPathABError("control input is not an admitted v9 successor panel") from exc
    if panel.get("identity") != control_repair.IDENTITY:
        raise NativeFullPathABError("control successor panel identity drift")
    identity = panel["identity_payload"]
    if list(identity.get("ordered_utc_days") or ()) != list(expected_days):
        raise NativeFullPathABError("control successor denominator differs from precommit")
    repair_plan = control_repair.validate_plan(_resolve_path(str(identity["plan_path"])))
    repair_rows = {row["utc_day"]: row for row in repair_plan["identity_payload"]["daily"]}
    components = {row["utc_day"]: row for row in identity["components"]}
    if list(repair_rows) != list(expected_days) or list(components) != list(expected_days):
        raise NativeFullPathABError("control successor daily order drift")
    by_day: dict[str, dict[str, Any]] = {}
    for day in expected_days:
        row = repair_rows[day]
        window_artifact = _artifact_row(
            row["window_binding"]["window"], role=f"{day} model-free v9 window"
        )
        native = row.get("native_book_artifacts")
        if not isinstance(native, list) or len(native) != 48:
            raise NativeFullPathABError(f"{day} lacks exact warmup/target source bindings")
        native_artifacts = [
            {"role": item.get("role"), **_artifact_row(item, role=f"{day} native source")}
            for item in native
        ]
        by_day[day] = {
            "window": window_artifact,
            "control_component": dict(components[day]),
            "native_book_artifacts": native_artifacts,
            "daily_source_identity_sha256": row.get("source_daily_identity_sha256"),
        }
    global_identity = identity["global_binding"]
    frozen_artifacts = {}
    for name in (
        "operational_config",
        "p3_artifact",
        "queue_calibration",
        "source_contract",
        "latency_profile",
    ):
        row = global_identity.get(name)
        if not isinstance(row, Mapping):
            raise NativeFullPathABError(f"control successor lacks {name}")
        frozen_artifacts[name] = {
            **dict(row),
            **_artifact_row(row, role=f"control successor {name}"),
        }
    return {
        "path": str(path.resolve()),
        "sha256": _sha256_file(path),
        "panel_identity_sha256": panel["panel_identity_sha256"],
        "successor_identity": control_repair.IDENTITY,
        "v9_model_bundle_identity_sha256": global_identity["model_bundle"][
            "content_identity_sha256"
        ],
        "global_identity_sha256": global_identity.get("source_global_identity_sha256"),
        **frozen_artifacts,
        "days": by_day,
    }


def _runtime_artifacts() -> dict[str, dict[str, Any]]:
    import narrowgate_cpp

    paths = {
        "runner": Path(__file__),
        "dual_overlay_abi": Path(dual_abi.__file__),
        "candidate_overlay_abi": Path(candidate_abi.__file__),
        "candidate_overlay_binding": Path(candidate_binding.__file__),
        "control_overlay_successor": Path(control_repair.__file__),
        "full_path_metrics": Path(full_path_ml_ab.__file__),
        "scorecard_v2": Path(experiment_scorecard_v2.__file__),
        "backtest_tick": ROOT / "models/backtest_tick.py",
        "data_windows": ROOT / "models/data_windows.py",
        "backtest_config": ROOT / "models/backtest_config.py",
        "cpp_module": Path(narrowgate_cpp.__file__),
    }
    return {
        name: {
            "path": str(path.resolve()),
            "sha256": _sha256_file(path.resolve()),
            "size_bytes": path.resolve().stat().st_size,
        }
        for name, path in paths.items()
    }


def prepare_execution_plan(
    *,
    candidate_overlay_panel_manifest: Path | None,
    control_overlay_panel_manifest: Path | None,
    execution_amendment_path: Path | None,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    precommit_path: Path = DEFAULT_PRECOMMIT,
) -> dict[str, Any]:
    """Freeze all execution inputs without loading an economic outcome."""

    if candidate_overlay_panel_manifest is None or control_overlay_panel_manifest is None:
        raise NativeFullPathABError(
            "formal candidate bundle/overlay and v9 control overlay source are required"
        )
    try:
        amendment = execution_amendment.validate_execution_amendment(
            execution_amendment_path,
            candidate_overlay_panel_manifest=candidate_overlay_panel_manifest.resolve(),
            control_overlay_panel_manifest=control_overlay_panel_manifest.resolve(),
            precommit_path=precommit_path.resolve(),
        )
        amendment_binding = execution_amendment.amendment_reference(
            execution_amendment_path.resolve(), amendment
        )
    except (execution_amendment.ExecutionAmendmentError, NativeFullPathABError) as exc:
        raise NativeFullPathABError("exact successor execution amendment rejected") from exc
    storage = _storage_gate(output_root)
    precommit, precommit_binding = _validate_precommit(precommit_path.resolve())
    days = list(precommit["native_development_panel"]["days"])
    candidate = _validate_candidate_panel(
        candidate_overlay_panel_manifest.resolve(), expected_days=days
    )
    control = _validate_control_sources(
        control_overlay_panel_manifest.resolve(), expected_days=days
    )
    if control["operational_config"]["sha256"] != precommit["baseline"]["config_sha256"]:
        raise NativeFullPathABError("control source config differs from frozen v9 precommit")
    daily = []
    for ordinal, day in enumerate(days, start=1):
        daily.append(
            {
                "ordinal": ordinal,
                "utc_day": day,
                "window": control["days"][day]["window"],
                "control_component": control["days"][day]["control_component"],
                "candidate_overlay": candidate["days"][day],
                "native_book_artifacts": control["days"][day]["native_book_artifacts"],
                "daily_source_identity_sha256": control["days"][day][
                    "daily_source_identity_sha256"
                ],
            }
        )
    identity_payload = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "comparison": "candidate_1s_ml_on_minus_v9_10s_ml_on",
        "arms": list(ARMS),
        "both_arms_ml_enabled": True,
        "predecessor_ml_off_control_forbidden": True,
        "only_arm_difference": "feature_model_and_inference_cadence",
        "execution_amendment": amendment_binding,
        "precommit": precommit_binding,
        "candidate_panel": {key: value for key, value in candidate.items() if key != "days"},
        "control_sources": {key: value for key, value in control.items() if key != "days"},
        "runtime_artifacts": _runtime_artifacts(),
        "output_root": str(output_root.expanduser().resolve()),
        "ordered_utc_days": days,
        "days": daily,
        "bootstrap": dict(precommit["comparison"]),
        "score_profile_contract": experiment_scorecard_v2.score_profile_contract("action_alpha_v2"),
    }
    plan = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "identity": IDENTITY,
        "created_at_utc": datetime.now(tz=UTC).isoformat(),
        "status": "prepared_outcomes_unread",
        "plan_identity_sha256": _canonical_sha256(identity_payload),
        "identity_payload": identity_payload,
        "day_count": EXPECTED_DAY_COUNT,
        "economic_outcomes_read": False,
        "development_pnl_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
        "atomic_daily_admission": True,
        "resume_safe": True,
        "storage_gate": storage,
    }
    root = output_root.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    plan_path = root / PLAN_FILENAME
    marker = root / PLAN_SUCCESS
    if plan_path.exists() or marker.exists():
        if not (plan_path.is_file() and marker.is_file()):
            raise NativeFullPathABError("incomplete prior execution-plan admission")
        existing = validate_execution_plan(
            plan_path, execution_amendment_path=execution_amendment_path
        )
        if existing["plan_identity_sha256"] != plan["plan_identity_sha256"]:
            raise NativeFullPathABError("existing execution plan has another identity")
        return existing
    _atomic_json(plan_path, plan)
    _atomic_text(marker, _sha256_file(plan_path) + "\n")
    return plan | {"plan_path": str(plan_path), "plan_sha256": _sha256_file(plan_path)}


def validate_execution_plan(
    plan_path: Path,
    *,
    execution_amendment_path: Path | None,
) -> dict[str, Any]:
    plan_path = plan_path.expanduser().resolve()
    plan = _load_json(plan_path, role="F03 1s full-path execution plan")
    if plan.get("schema_version") != PLAN_SCHEMA_VERSION or plan.get("identity") != IDENTITY:
        raise NativeFullPathABError("unsupported F03 1s full-path plan")
    payload = plan.get("identity_payload")
    if not isinstance(payload, Mapping) or plan.get("plan_identity_sha256") != (
        _canonical_sha256(payload)
    ):
        raise NativeFullPathABError("execution plan identity cannot be reproduced")
    marker = plan_path.parent / PLAN_SUCCESS
    if not marker.is_file() or marker.read_text(encoding="ascii").strip() != _sha256_file(
        plan_path
    ):
        raise NativeFullPathABError("execution plan admission marker drift")
    if plan.get("day_count") != EXPECTED_DAY_COUNT or any(
        plan.get(field) is not False
        for field in (
            "economic_outcomes_read",
            "development_pnl_read",
            "validation_read",
            "sealed_holdout_read",
            "action_authorized",
            "live_authorized",
        )
    ):
        raise NativeFullPathABError("execution plan crossed its preparation boundary")
    amendment_binding = payload.get("execution_amendment")
    if not isinstance(amendment_binding, Mapping) or execution_amendment_path is None:
        raise NativeFullPathABError("execution plan requires its exact successor amendment")
    resolved_amendment = execution_amendment_path.expanduser().resolve()
    if resolved_amendment != Path(str(amendment_binding.get("path", ""))).resolve():
        raise NativeFullPathABError("run amendment path differs from the prepared plan")
    try:
        amendment = execution_amendment.validate_execution_amendment(
            resolved_amendment,
            candidate_overlay_panel_manifest=Path(payload["candidate_panel"]["path"]),
            control_overlay_panel_manifest=Path(payload["control_sources"]["path"]),
            precommit_path=Path(payload["precommit"]["path"]),
        )
        observed_binding = execution_amendment.amendment_reference(resolved_amendment, amendment)
    except (execution_amendment.ExecutionAmendmentError, NativeFullPathABError) as exc:
        raise NativeFullPathABError("bound successor execution amendment rejected") from exc
    if observed_binding != dict(amendment_binding):
        raise NativeFullPathABError("execution amendment binding drifted after prepare")
    _validate_precommit(Path(payload["precommit"]["path"]))
    if payload.get("score_profile_contract") != experiment_scorecard_v2.score_profile_contract(
        "action_alpha_v2"
    ):
        raise NativeFullPathABError("execution plan score profile drift")
    for name, artifact in payload.get("runtime_artifacts", {}).items():
        _artifact_row(artifact, role=f"runtime artifact {name}")
    for name in (
        "operational_config",
        "p3_artifact",
        "queue_calibration",
        "source_contract",
        "latency_profile",
    ):
        _artifact_row(payload["control_sources"][name], role=f"control {name}")
    control_panel_path = Path(payload["control_sources"]["path"])
    if _sha256_file(control_panel_path) != payload["control_sources"]["sha256"]:
        raise NativeFullPathABError("control successor panel SHA256 drift")
    control_panel = control_repair.validate_panel(control_panel_path)
    if (
        control_panel["panel_identity_sha256"]
        != payload["control_sources"]["panel_identity_sha256"]
    ):
        raise NativeFullPathABError("control successor panel identity drift")
    for row in payload.get("days", ()):
        day = row["utc_day"]
        _artifact_row(row["window"], role=f"{day} model-free window")
        _artifact(
            row["candidate_overlay"]["overlay_path"],
            row["candidate_overlay"]["overlay_sha256"],
            role=f"{day} candidate overlay",
        )
        _artifact(
            row["candidate_overlay"]["overlay_manifest_path"],
            row["candidate_overlay"]["overlay_manifest_sha256"],
            role=f"{day} candidate manifest",
        )
        for native in row["native_book_artifacts"]:
            _artifact_row(native, role=f"{day} native source")
    return plan | {"plan_path": str(plan_path), "plan_sha256": _sha256_file(plan_path)}


def _load_formal_base_params(config_path: Path) -> dict[str, Any]:
    from models import backtest_tick as bt
    from models.backtest_config import load_tick_base_params

    bt.configure_symbol("BTCUSDC")
    params = load_tick_base_params(
        symbol="BTCUSDC",
        config_path=config_path,
        configure_symbol=bt.configure_symbol,
        require_historical_bbo=True,
    )
    params.update(
        {
            "ml_enabled": True,
            "execution_trade_source": "trades",
            "market_context_warmup_days": 1,
            "replay_event_clock": "merged",
            "queue_ahead_mode": "exact_level",
            "queue_l2_cancel_ahead_enabled": False,
            "collect_curves": False,
            "trace_fills_max": 1_000_000,
            "trace_fills_window_s": 30.0,
            "trace_campaign_repair_max": CAMPAIGN_MAE_TRACE_MAX,
            "dynamic_fill_hazard_action_enabled": False,
            "dynamic_fill_hazard_shadow_enabled": False,
            "dynamic_fill_hazard_cpp_parity_enabled": False,
            "buy_fill_selection_live_enabled": False,
            "buy_fill_selection_shadow_enabled": False,
            "sync_adjust_replay_mode": "disabled",
            "markout_side_asymmetry_sign": 1.0,
        }
    )
    return params


def _load_bound_window(path: Path) -> Any:
    from models.data_windows import _load_cached_window

    window = _load_cached_window(path)
    if window is None:
        raise NativeFullPathABError("bound model-free window is incompatible")
    if getattr(window, "ml_data", None) is not None:
        raise NativeFullPathABError("bound market window is not model-free")
    if getattr(window, "book_source_authority", None) != "native_formal_lifecycle":
        raise NativeFullPathABError("bound market window lacks native lifecycle authority")
    return window


def _validate_campaign_mae_trace_capacity(
    params: Mapping[str, Any],
    *,
    expected: int,
) -> int:
    observed = int(params.get("trace_campaign_repair_max", 0) or 0)
    if expected <= 0 or observed <= 0 or observed != expected:
        raise NativeFullPathABError(
            "trace_campaign_repair_max must be positive and equal to the amendment"
        )
    return observed


def _required_result(result: Mapping[str, Any], key: str, *, arm: str) -> Any:
    if key not in result:
        raise NativeFullPathABError(f"replay result lacks required {key} for {arm}")
    value = result[key]
    if isinstance(value, (int, float)) and not math.isfinite(float(value)):
        raise NativeFullPathABError(f"replay result has nonfinite {key} for {arm}")
    return value


class _CampaignMaeTraceProbe:
    """A non-action scorer that makes Python emit campaign-state MAE rows."""

    model_id = "f03_campaign_mae_trace_probe_v1"
    training_end_day = "not_applicable_non_action_probe"

    @staticmethod
    def score(_inventory: float, _features: Mapping[str, Any]) -> float:
        return 0.5


def _assert_cpp_python_fill_path_lockstep(
    cpp_result: Mapping[str, Any],
    python_result: Mapping[str, Any],
) -> None:
    integer_fields = ("fills_bid", "fills_ask", "fills_total")
    numeric_fields = (
        "pnl",
        "terminal_mtm_pnl",
        "terminal_mark_price",
        "abs_inventory_time_s",
        "max_inventory",
        "final_inventory",
    )
    for field in integer_fields:
        if int(_required_result(cpp_result, field, arm="cpp")) != int(
            _required_result(python_result, field, arm="python")
        ):
            raise NativeFullPathABError(f"campaign MAE trace parity mismatch: {field}")
    for field in numeric_fields:
        left = float(_required_result(cpp_result, field, arm="cpp"))
        right = float(_required_result(python_result, field, arm="python"))
        if not math.isclose(left, right, rel_tol=0.0, abs_tol=REPLAY_PARITY_ATOL):
            raise NativeFullPathABError(f"campaign MAE trace parity mismatch: {field}")

    cpp_fills = list(cpp_result.get("_fill_trace") or ())
    python_fills = list(python_result.get("_fill_trace") or ())
    if len(cpp_fills) != len(python_fills):
        raise NativeFullPathABError("campaign MAE trace parity mismatch: fill count")
    exact_fields = ("side", "fill_ts")
    float_fields = (
        "quote_px",
        "fill_qty",
        "inventory_before_fill",
        "inventory_after_fill",
    )
    for index, (cpp_fill, python_fill) in enumerate(zip(cpp_fills, python_fills, strict=True)):
        for field in exact_fields:
            if cpp_fill.get(field) != python_fill.get(field):
                raise NativeFullPathABError(
                    f"campaign MAE trace fill-path mismatch at {index}: {field}"
                )
        for field in float_fields:
            left = float(cpp_fill.get(field, math.nan))
            right = float(python_fill.get(field, math.nan))
            if not (
                math.isfinite(left)
                and math.isfinite(right)
                and math.isclose(left, right, rel_tol=0.0, abs_tol=REPLAY_PARITY_ATOL)
            ):
                raise NativeFullPathABError(
                    f"campaign MAE trace fill-path mismatch at {index}: {field}"
                )


def _simulate_cpp_with_campaign_mae_trace(
    engine: str,
    trades_df: pd.DataFrame,
    var_ts_ms: Any,
    var_ssq: Any,
    params: Mapping[str, Any],
    **kwargs: Any,
) -> Mapping[str, Any]:
    """Keep C++ economics and admit Python MAE only after exact fill-path lockstep."""

    from models import backtest_tick as bt

    if engine != "cpp":
        raise NativeFullPathABError("formal F03 economics require the C++ replay arm")
    trace_max = int(params.get("trace_campaign_repair_max", 0) or 0)
    if trace_max != CAMPAIGN_MAE_TRACE_MAX or trace_max <= 0:
        raise NativeFullPathABError("trace_campaign_repair_max must match the amendment")
    for action_key in ("multi_market_policy_enabled", "post_fill_quote_response_enabled"):
        if bool(params.get(action_key, False)):
            raise NativeFullPathABError(
                f"campaign MAE trace probe would enter an active policy: {action_key}"
            )

    cpp_result = bt._simulate_tick_with_engine(
        "cpp", trades_df, var_ts_ms, var_ssq, dict(params), **kwargs
    )
    python_result = bt._simulate_tick_with_engine(
        "python",
        trades_df,
        var_ts_ms,
        var_ssq,
        dict(params),
        campaign_repair_model=_CampaignMaeTraceProbe(),
        **kwargs,
    )
    _assert_cpp_python_fill_path_lockstep(cpp_result, python_result)
    trace = python_result.get("_campaign_repair_trace")
    if not isinstance(trace, list):
        raise NativeFullPathABError("Python campaign MAE probe did not return a trace")
    output = dict(cpp_result)
    output["_campaign_repair_trace"] = trace
    output["_campaign_mae_trace_audit"] = {
        "source": "python_probe_locked_to_cpp_fill_path",
        "trace_campaign_repair_max": trace_max,
        "trace_row_count": len(trace),
        "cpp_python_fill_path_mismatch_count": 0,
    }
    return output


def _project_arm(
    *,
    day: str,
    arm: str,
    result: Mapping[str, Any],
    order_size: float,
    campaign_mae_trace_max: int,
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    if campaign_mae_trace_max <= 0:
        raise NativeFullPathABError("trace_campaign_repair_max=0 is forbidden")
    required = (
        "pnl",
        "terminal_mtm_pnl",
        "terminal_mark_price",
        "fills_bid",
        "fills_ask",
        "fills_total",
        "abs_inventory_time_s",
        "max_inventory",
        "final_inventory",
    )
    for key in required:
        _required_result(result, key, arm=arm)
    fill_trace = list(result.get("_fill_trace") or ())
    if len(fill_trace) != int(result["fills_total"]):
        raise NativeFullPathABError(
            f"{day} {arm} fill trace truncated: {len(fill_trace)} != {result['fills_total']}"
        )
    campaigns = full_path_ml_ab.reconstruct_campaigns(
        fill_trace,
        day=day,
        panel_role="historical_native_development",
        arm=arm,
        terminal_mark_price=float(result["terminal_mark_price"]),
        order_size=order_size,
    )
    campaign_frame = pd.DataFrame(campaigns)
    metrics = full_path_ml_ab._campaign_day_metrics(campaign_frame)
    closed_value = (
        float(
            campaign_frame.loc[campaign_frame["closed"].astype(bool), "terminal_value_usdc"].sum()
        )
        if not campaign_frame.empty
        else 0.0
    )
    negative_value = (
        float(
            campaign_frame.loc[
                campaign_frame["terminal_value_usdc"] < 0.0, "terminal_value_usdc"
            ].sum()
        )
        if not campaign_frame.empty
        else 0.0
    )
    accounting_error = float(metrics["campaign_terminal_value_usdc"]) - float(
        result["terminal_mtm_pnl"]
    )
    if abs(accounting_error) > ACCOUNTING_TOLERANCE_USDC:
        raise NativeFullPathABError(f"{day} {arm} campaign accounting mismatch")
    buy = full_path_ml_ab._side_trace_metrics(fill_trace, "BUY")
    sell = full_path_ml_ab._side_trace_metrics(fill_trace, "SELL")
    blockers: list[str] = []
    campaign_mae = None
    repair_trace = result.get("_campaign_repair_trace")
    trace_rows = len(repair_trace) if isinstance(repair_trace, list) else 0
    if isinstance(repair_trace, list) and trace_rows >= campaign_mae_trace_max:
        blockers.append("campaign_mae_trace_capacity_reached")
    elif isinstance(repair_trace, list) and repair_trace:
        values: list[float] = []
        last_by_campaign: dict[int, tuple[int, float]] = {}
        for row in repair_trace:
            if not isinstance(row, Mapping) or CAMPAIGN_MAE_TRACE_FIELD not in row:
                raise NativeFullPathABError(f"{day} {arm} campaign MAE trace row is invalid")
            try:
                campaign_id = int(row["campaign_id"])
                ts_ns = int(row["ts_ns"])
                value = float(row[CAMPAIGN_MAE_TRACE_FIELD])
            except (KeyError, TypeError, ValueError) as exc:
                raise NativeFullPathABError(
                    f"{day} {arm} campaign MAE trace identity is invalid"
                ) from exc
            if campaign_id <= 0 or ts_ns < 0:
                raise NativeFullPathABError(
                    f"{day} {arm} campaign MAE trace identity is invalid"
                )
            if not math.isfinite(value) or value > ACCOUNTING_TOLERANCE_USDC:
                raise NativeFullPathABError(f"{day} {arm} campaign MAE trace value is invalid")
            previous = last_by_campaign.get(campaign_id)
            if previous is not None:
                previous_ts_ns, previous_value = previous
                if ts_ns < previous_ts_ns:
                    raise NativeFullPathABError(
                        f"{day} {arm} campaign MAE trace time is not monotone"
                    )
                if value > previous_value + ACCOUNTING_TOLERANCE_USDC:
                    raise NativeFullPathABError(
                        f"{day} {arm} campaign adverse excursion is not a running minimum"
                    )
            last_by_campaign[campaign_id] = (ts_ns, value)
            values.append(value)
        campaign_mae = float(min(values))
    elif int(result["fills_total"]) == 0:
        campaign_mae = 0.0
    else:
        blockers.append("campaign_mae_not_emitted_by_authoritative_replay")
    trace_audit = result.get("_campaign_mae_trace_audit")
    mismatch_count = -1
    if isinstance(repair_trace, list) and repair_trace and not any(
        blocker == "campaign_mae_trace_capacity_reached" for blocker in blockers
    ):
        if not isinstance(trace_audit, Mapping):
            blockers.append("campaign_mae_cpp_python_fill_path_audit_missing")
        else:
            mismatch_count = int(trace_audit.get("cpp_python_fill_path_mismatch_count", -1))
            if mismatch_count != 0:
                blockers.append("campaign_mae_cpp_python_fill_path_mismatch")
    closed = (
        campaign_frame[campaign_frame["closed"].astype(bool)]
        if not campaign_frame.empty
        else campaign_frame
    )
    summary = {
        "day": day,
        "arm": arm,
        "pnl_usdc": float(result["pnl"]),
        "terminal_mtm_pnl_usdc": float(result["terminal_mtm_pnl"]),
        "closed_campaign_value_usdc": closed_value,
        "negative_campaign_terminal_value_usdc": negative_value,
        "fills_bid": int(result["fills_bid"]),
        "fills_ask": int(result["fills_ask"]),
        "fills_total": int(result["fills_total"]),
        "abs_inventory_time_btc_s": float(result["abs_inventory_time_s"]),
        "max_inventory_btc": float(result["max_inventory"]),
        "final_inventory_btc": float(result["final_inventory"]),
        "buy_maker_value_30s_bps": buy["maker_value_30s_bps"],
        "sell_maker_value_30s_bps": sell["maker_value_30s_bps"],
        "campaign_mae_usdc": campaign_mae,
        "campaign_mae_trace_rows": trace_rows,
        "campaign_mae_trace_capacity": campaign_mae_trace_max,
        "campaign_mae_trace_field": CAMPAIGN_MAE_TRACE_FIELD,
        "campaign_mae_trace_semantics": CAMPAIGN_MAE_TRACE_SEMANTICS,
        "campaign_mae_trace_source": (
            trace_audit.get("source") if isinstance(trace_audit, Mapping) else None
        ),
        "campaign_mae_cpp_python_fill_path_mismatch_count": (
            mismatch_count
        ),
        "repair_event_rate": (
            float(campaign_frame["closed"].astype(bool).mean()) if not campaign_frame.empty else 0.0
        ),
        "mean_closed_repair_time_s": (
            float(closed["duration_s"].mean()) if not closed.empty else 0.0
        ),
        "campaign_accounting_error_usdc": accounting_error,
        "metric_blockers": blockers,
        **metrics,
    }
    fills = pd.DataFrame(fill_trace)
    if not fills.empty:
        fills.insert(0, "arm", arm)
        fills.insert(0, "day", day)
    return summary, campaign_frame, fills


def _validate_day_admission(day_dir: Path, *, plan_identity: str, day: str) -> dict[str, Any]:
    manifest_path = day_dir / "manifest.json"
    marker = day_dir / DAY_SUCCESS
    manifest = _load_json(manifest_path, role=f"{day} day manifest")
    if not marker.is_file() or marker.read_text(encoding="ascii").strip() != _sha256_file(
        manifest_path
    ):
        raise NativeFullPathABError(f"{day} day admission marker drift")
    if (
        manifest.get("schema_version") != DAY_SCHEMA_VERSION
        or manifest.get("plan_identity_sha256") != plan_identity
        or manifest.get("utc_day") != day
    ):
        raise NativeFullPathABError(f"{day} day identity drift")
    for role in ("summary", "campaigns", "fills"):
        _artifact_row(manifest[role], role=f"{day} {role}")
    return manifest


def execute_day(
    plan_path: Path,
    *,
    day: str,
    execution_amendment_path: Path,
    base_params_loader: Callable[[Path], Mapping[str, Any]] | None = None,
    window_loader: Callable[[Path], Any] | None = None,
    simulate: Callable[..., Mapping[str, Any]] | None = None,
    allow_test_only_candidate: bool = False,
) -> dict[str, Any]:
    """Execute and atomically admit one paired day; reruns reuse exact output."""

    plan = validate_execution_plan(plan_path, execution_amendment_path=execution_amendment_path)
    payload = plan["identity_payload"]
    rows = {row["utc_day"]: row for row in payload["days"]}
    if day not in rows:
        raise NativeFullPathABError(f"day is outside frozen denominator: {day}")
    output_root = Path(payload["output_root"])
    final_dir = output_root / "days" / day
    if final_dir.exists():
        return _validate_day_admission(
            final_dir, plan_identity=plan["plan_identity_sha256"], day=day
        ) | {"reused": True}
    lock_path = output_root / ".execution.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        if final_dir.exists():
            return _validate_day_admission(
                final_dir, plan_identity=plan["plan_identity_sha256"], day=day
            ) | {"reused": True}
        row = rows[day]
        for name, artifact in payload["runtime_artifacts"].items():
            _artifact_row(artifact, role=f"runtime artifact {name}")
        control = control_repair.load_admitted_control_schedule(
            Path(payload["control_sources"]["path"]),
            panel_sha256=payload["control_sources"]["sha256"],
            panel_identity_sha256=payload["control_sources"]["panel_identity_sha256"],
            day=day,
        )
        candidate = candidate_abi.load_admitted_one_second_overlay(
            Path(row["candidate_overlay"]["overlay_dir"]),
            allow_test_only=allow_test_only_candidate,
        )
        if candidate.utc_day != day:
            raise NativeFullPathABError("candidate overlay day differs from execution day")
        window_path = Path(row["window"]["path"])
        if _sha256_file(window_path) != row["window"]["sha256"]:
            raise NativeFullPathABError("model-free window drifted after prepare")
        loader = _load_bound_window if window_loader is None else window_loader
        window = loader(window_path)
        precommit = _load_json(
            Path(payload["precommit"]["path"]),
            role="frozen precommit",
        )
        operational_config = _artifact_row(
            payload["control_sources"]["operational_config"],
            role="frozen control operational config",
        )
        if operational_config["sha256"] != (precommit.get("baseline") or {}).get(
            "config_sha256"
        ):
            raise NativeFullPathABError(
                "plan control config differs from the frozen precommit"
            )
        params_loader = (
            _load_formal_base_params if base_params_loader is None else base_params_loader
        )
        base_params = dict(params_loader(Path(operational_config["path"])))
        expected_trace_max = int(payload["execution_amendment"]["trace_campaign_repair_max"])
        _validate_campaign_mae_trace_capacity(base_params, expected=expected_trace_max)
        replay_simulator = _simulate_cpp_with_campaign_mae_trace if simulate is None else simulate
        started = time.perf_counter()
        replay = dual_abi.run_dual_overlay_tick_replay(
            window=window,
            base_params=base_params,
            control_schedule=control,
            candidate_schedule=candidate,
            engine="cpp",
            simulate=replay_simulator,
        )
        summaries: list[dict[str, Any]] = []
        campaigns: list[pd.DataFrame] = []
        fills: list[pd.DataFrame] = []
        for arm in ARMS:
            summary, campaign_frame, fill_frame = _project_arm(
                day=day,
                arm=arm,
                result=replay["arms"][arm],
                order_size=float(base_params["order_size"]),
                campaign_mae_trace_max=expected_trace_max,
            )
            summaries.append(summary)
            campaigns.append(campaign_frame)
            fills.append(fill_frame)
        staging = output_root / ".staging" / f"{day}-{uuid.uuid4().hex}"
        staging.mkdir(parents=True)
        try:
            summary_path = staging / "summary.json"
            campaigns_path = staging / "campaigns.parquet"
            fills_path = staging / "fills.parquet"
            _atomic_json(
                summary_path,
                {
                    "schema_version": DAY_SCHEMA_VERSION,
                    "utc_day": day,
                    "comparison": "candidate_1s_ml_on_minus_v9_10s_ml_on",
                    "arms": summaries,
                    "replay_identity": replay["identity"],
                    "runtime_seconds": time.perf_counter() - started,
                    "validation_read": False,
                    "sealed_holdout_read": False,
                    "action_authorized": False,
                    "live_authorized": False,
                },
            )
            pd.concat(campaigns, ignore_index=True).to_parquet(
                campaigns_path, index=False, compression="zstd"
            )
            nonempty_fills = [frame for frame in fills if not frame.empty]
            (
                pd.concat(nonempty_fills, ignore_index=True) if nonempty_fills else pd.DataFrame()
            ).to_parquet(fills_path, index=False, compression="zstd")
            manifest = {
                "schema_version": DAY_SCHEMA_VERSION,
                "identity": IDENTITY,
                "utc_day": day,
                "plan_identity_sha256": plan["plan_identity_sha256"],
                "execution_amendment_sha256": payload["execution_amendment"]["sha256"],
                "daily_source_identity_sha256": row["daily_source_identity_sha256"],
                "window_sha256": row["window"]["sha256"],
                "control_overlay_identity_sha256": control.identity_sha256,
                "candidate_overlay_identity_sha256": candidate.overlay_identity_sha256,
                "both_arms_ml_enabled": True,
                "summary": {
                    **_artifact(summary_path, _sha256_file(summary_path), role="summary"),
                    "path": str(final_dir / summary_path.name),
                },
                "campaigns": {
                    **_artifact(campaigns_path, _sha256_file(campaigns_path), role="campaigns"),
                    "path": str(final_dir / campaigns_path.name),
                },
                "fills": {
                    **_artifact(fills_path, _sha256_file(fills_path), role="fills"),
                    "path": str(final_dir / fills_path.name),
                },
                "economic_outcomes_read": True,
                "validation_read": False,
                "sealed_holdout_read": False,
                "action_authorized": False,
                "live_authorized": False,
            }
            manifest_path = staging / "manifest.json"
            _atomic_json(manifest_path, manifest)
            _atomic_text(staging / DAY_SUCCESS, _sha256_file(manifest_path) + "\n")
            final_dir.parent.mkdir(parents=True, exist_ok=True)
            os.replace(staging, final_dir)
            _fsync_directory(final_dir.parent)
        finally:
            if staging.exists():
                shutil.rmtree(staging)
        return _validate_day_admission(
            final_dir, plan_identity=plan["plan_identity_sha256"], day=day
        ) | {"reused": False}


def _bootstrap(values: np.ndarray, *, draws: int, seed: int) -> dict[str, Any]:
    if values.ndim != 1 or len(values) == 0 or not np.isfinite(values).all():
        raise NativeFullPathABError("paired day bootstrap received invalid values")
    rng = np.random.default_rng(seed)
    sampled = rng.choice(values, size=(draws, len(values)), replace=True).mean(axis=1)
    return {
        "estimate": float(np.mean(values)),
        "lower_bound": float(np.quantile(sampled, 0.025)),
        "upper_bound": float(np.quantile(sampled, 0.975)),
        "daily_positive_rate": float(np.mean(values > 0.0)),
        "sum_delta": float(np.sum(values)),
        "days": int(len(values)),
    }


def _paired(
    daily: pd.DataFrame,
    column: str,
    *,
    direction: str,
    draws: int,
    seed: int,
) -> dict[str, Any]:
    wide = daily.pivot(index="day", columns="arm", values=column).sort_index()
    if set(wide.columns) != set(ARMS):
        raise NativeFullPathABError(f"metric {column} lacks exact paired days")
    wide = wide.reindex(columns=list(ARMS))
    if wide.isna().any().any():
        raise NativeFullPathABError(f"metric {column} lacks exact paired days")
    control = wide[ARMS[0]].to_numpy(dtype=float)
    candidate = wide[ARMS[1]].to_numpy(dtype=float)
    delta = candidate - control if direction == "candidate_minus_control" else control - candidate
    return _bootstrap(delta, draws=draws, seed=seed)


def _score_panel(
    daily: pd.DataFrame,
    campaigns: pd.DataFrame,
    *,
    plan: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    precommit = _load_json(Path(plan["identity_payload"]["precommit"]["path"]), role="precommit")
    draws = int(precommit["comparison"]["bootstrap_draws"])
    seed = int(precommit["comparison"]["bootstrap_seed"])
    metric_blockers = sorted(
        {
            blocker
            for values in daily["metric_blockers"]
            for blocker in (values if isinstance(values, list) else [])
        }
    )
    paired = {
        "closed_campaign_value": _paired(
            daily,
            "closed_campaign_value_usdc",
            direction="candidate_minus_control",
            draws=draws,
            seed=seed,
        ),
        "conditional_net_value": _paired(
            daily,
            "terminal_mtm_pnl_usdc",
            direction="candidate_minus_control",
            draws=draws,
            seed=seed + 1,
        ),
        "negative_terminal_protection": _paired(
            daily,
            "negative_campaign_terminal_value_usdc",
            direction="candidate_minus_control",
            draws=draws,
            seed=seed + 2,
        ),
        "q10_shortfall_protection": _paired(
            daily,
            "campaign_q10_usdc",
            direction="candidate_minus_control",
            draws=draws,
            seed=seed + 3,
        ),
        "campaign_cvar10_protection": _paired(
            daily,
            "campaign_cvar10_usdc",
            direction="candidate_minus_control",
            draws=draws,
            seed=seed + 4,
        ),
        "maximum_inventory_avoidance": _paired(
            daily,
            "max_inventory_btc",
            direction="control_minus_candidate",
            draws=draws,
            seed=seed + 5,
        ),
        "inventory_time_avoidance": _paired(
            daily,
            "abs_inventory_time_btc_s",
            direction="control_minus_candidate",
            draws=draws,
            seed=seed + 6,
        ),
        "repair_event": _paired(
            daily,
            "repair_event_rate",
            direction="candidate_minus_control",
            draws=draws,
            seed=seed + 7,
        ),
        "repair_time_avoidance_s": _paired(
            daily,
            "mean_closed_repair_time_s",
            direction="control_minus_candidate",
            draws=draws,
            seed=seed + 8,
        ),
        "full_panel_continuous_mtm": _paired(
            daily,
            "terminal_mtm_pnl_usdc",
            direction="candidate_minus_control",
            draws=draws,
            seed=seed + 9,
        ),
    }
    mae_blocked = any(blocker.startswith("campaign_mae_") for blocker in metric_blockers)
    if not mae_blocked:
        paired["campaign_mae_avoidance"] = _paired(
            daily,
            "campaign_mae_usdc",
            direction="candidate_minus_control",
            draws=draws,
            seed=seed + 10,
        )
    totals = daily.groupby("arm", sort=False).sum(numeric_only=True)
    fill_retention = float(
        totals.loc[ARMS[1], "fills_total"] / max(totals.loc[ARMS[0], "fills_total"], 1.0)
    )
    paired["fills_retention"] = {"estimate": fill_retention}
    parity_gate = plan["identity_payload"]["execution_amendment"].get(
        "training_feature_parity_gate"
    )
    parity_gate_bound = isinstance(parity_gate, Mapping)
    fill_path_counts = daily["campaign_mae_cpp_python_fill_path_mismatch_count"].astype(int)
    fill_path_parity_bound = bool((fill_path_counts >= 0).all())
    fill_path_mismatch_count = int(fill_path_counts.clip(lower=0).sum())
    execution_integrity = {
        "identity_and_hash_parity_passed": True,
        "runtime_and_daily_artifact_hashes_validated": True,
        "campaign_accounting_max_abs_error_usdc": float(
            daily["campaign_accounting_error_usdc"].abs().max()
        ),
        "campaign_accounting_within_tolerance": bool(
            daily["campaign_accounting_error_usdc"].abs().max()
            <= ACCOUNTING_TOLERANCE_USDC
        ),
        "cpp_python_fill_path_mismatch_count": fill_path_mismatch_count,
        "cpp_python_fill_path_parity_bound": fill_path_parity_bound,
        "cpp_python_fill_path_parity_passed": bool(
            fill_path_parity_bound and fill_path_mismatch_count == 0
        ),
        "python_cpp_feature_parity_bound": parity_gate_bound,
        "python_cpp_feature_mismatch_count": 0 if parity_gate_bound else None,
        "python_cpp_prediction_parity_bound": False,
        "python_cpp_prediction_mismatch_count": None,
        "tick_gtx_spread_cap_parity_bound": False,
        "tick_gtx_spread_cap_mismatch_count": None,
        "training_feature_parity_gate": dict(parity_gate) if parity_gate_bound else None,
    }
    campaign_counts = campaigns.groupby("arm").size()
    effective_rows = int(campaign_counts.min()) if len(campaign_counts) == 2 else 0
    validity_failures = [
        "native_40_day_daily_fresh_start_is_not_continuous_path_authority",
        *metric_blockers,
    ]
    required_score_metrics = {
        row["name"]
        for row in experiment_scorecard_v2.score_profile_payload("action_alpha_v2")["metrics"]
    }
    validity_failures.extend(
        f"missing_score_metric:{name}" for name in sorted(required_score_metrics - paired.keys())
    )
    evidence = {
        "schema_version": experiment_scorecard_v2.CANONICAL_EVIDENCE_SCHEMA_VERSION,
        "experiment_id": IDENTITY,
        "family_id": "F03_causal_13_head",
        "panel_role": "development",
        "input_identity": {
            "plan_identity_sha256": plan["plan_identity_sha256"],
            "execution_amendment_sha256": plan["identity_payload"]["execution_amendment"]["sha256"],
            "precommit_sha256": plan["identity_payload"]["precommit"]["sha256"],
            "candidate_panel_identity_sha256": plan["identity_payload"]["candidate_panel"][
                "panel_identity_sha256"
            ],
            "control_overlay_successor_panel_sha256": plan["identity_payload"]["control_sources"][
                "sha256"
            ],
        },
        "score_profile_contract": experiment_scorecard_v2.score_profile_contract("action_alpha_v2"),
        "validity_failures": validity_failures,
        "family_gate_failures": [],
        "metrics": paired,
        "n_rows": effective_rows,
        "n_days": EXPECTED_DAY_COUNT,
        "effective_sample_size": float(effective_rows),
        "minimum_behavior_propensity": 0.5,
        "unsupported_mass": 0.0,
        "overlap_violations": 0,
        "candidate_rate": 0.5,
        "invariant_violations": [],
        "continuous_path_accounting": {
            "schema_version": experiment_scorecard_v2.CONTINUOUS_PATH_SCHEMA_VERSION,
            "utc_day_role": "bootstrap_cluster_only",
            "cash_carried_across_utc_days": False,
            "inventory_carried_across_utc_days": False,
            "campaign_state_carried_across_utc_days": False,
            "panel_final_inventory_mtm_included": True,
            "forced_day_end_liquidations": 0,
            "day_end_state_resets": EXPECTED_DAY_COUNT - 1,
            "day_end_campaign_terminals": 0,
            "daily_pnl_sum_usdc": float(
                totals.loc[ARMS[1], "terminal_mtm_pnl_usdc"]
                - totals.loc[ARMS[0], "terminal_mtm_pnl_usdc"]
            ),
            "continuous_panel_pnl_usdc": float(
                totals.loc[ARMS[1], "terminal_mtm_pnl_usdc"]
                - totals.loc[ARMS[0], "terminal_mtm_pnl_usdc"]
            ),
            "daily_accounting_identity_max_abs_error_usdc": float(
                daily["campaign_accounting_error_usdc"].abs().max()
            ),
            "panel_final_inventory_btc": float(
                daily.loc[daily["arm"].eq(ARMS[1]), "final_inventory_btc"].iloc[-1]
            ),
            "panel_final_mark_price_usdc_per_btc": 0.0,
            "panel_final_inventory_mtm_usdc": 0.0,
        },
    }
    raw = experiment_scorecard_v2.score_canonical_evidence(evidence, profile_id="action_alpha_v2")
    gates = precommit["common_noncompensable_gates"]
    additional = {
        "terminal_mtm_lcb_positive": paired["conditional_net_value"]["lower_bound"] > 0.0,
        "closed_campaign_lcb_positive": paired["closed_campaign_value"]["lower_bound"] > 0.0,
        "campaign_q10_lcb_nonnegative": paired["q10_shortfall_protection"]["lower_bound"] >= 0.0,
        "campaign_cvar10_lcb_nonnegative": paired["campaign_cvar10_protection"]["lower_bound"]
        >= 0.0,
        "maximum_inventory_lcb_nonnegative": paired["maximum_inventory_avoidance"]["lower_bound"]
        >= 0.0,
        "inventory_time_lcb_nonnegative": paired["inventory_time_avoidance"]["lower_bound"] >= 0.0,
        "campaign_mae_avoidance_lcb_nonnegative": bool(
            not mae_blocked
            and paired["campaign_mae_avoidance"]["lower_bound"]
            >= float(gates["campaign_mae_avoidance_lcb_minimum"])
        ),
        "identity_and_hash_parity": execution_integrity["identity_and_hash_parity_passed"],
        "campaign_accounting_parity": execution_integrity[
            "campaign_accounting_within_tolerance"
        ],
        "cpp_python_fill_path_parity": execution_integrity[
            "cpp_python_fill_path_parity_passed"
        ],
        "python_cpp_feature_parity_bound": execution_integrity[
            "python_cpp_feature_parity_bound"
        ],
        "python_cpp_prediction_parity_bound": execution_integrity[
            "python_cpp_prediction_parity_bound"
        ],
        "tick_gtx_spread_cap_parity_bound": execution_integrity[
            "tick_gtx_spread_cap_parity_bound"
        ],
        "buy_maker_value_lcb": _paired(
            daily,
            "buy_maker_value_30s_bps",
            direction="candidate_minus_control",
            draws=draws,
            seed=seed + 11,
        ),
        "sell_maker_value_lcb": _paired(
            daily,
            "sell_maker_value_30s_bps",
            direction="candidate_minus_control",
            draws=draws,
            seed=seed + 12,
        ),
        "multi_level_long_loss": _paired(
            daily,
            "multi_level_long_negative_value_usdc",
            direction="candidate_minus_control",
            draws=draws,
            seed=seed + 13,
        ),
        "multi_level_short_loss": _paired(
            daily,
            "multi_level_short_negative_value_usdc",
            direction="candidate_minus_control",
            draws=draws,
            seed=seed + 14,
        ),
    }
    owner_failures = list(raw["validity"]["failures"]) + list(raw["support"]["failures"])
    hard_failures = list(raw["hard_gates"]["failures"])
    fill_failure = (
        f"fills_retention_below_{experiment_scorecard_v2.ACTION_ALPHA_V2.minimum_fills_retention:g}"
    )
    if fill_failure in hard_failures and 0.8 <= fill_retention <= 1.2:
        hard_failures.remove(fill_failure)
    elif not 0.8 <= fill_retention <= 1.2:
        owner_failures.append("fills_retention_outside_owner_0.80_1.20")
    owner_failures.extend(hard_failures)
    for key, value in additional.items():
        if isinstance(value, bool) and not value:
            owner_failures.append(f"owner_gate_failed:{key}")
    if additional["buy_maker_value_lcb"]["lower_bound"] < float(
        gates["buy_maker_value_delta_lcb_minimum_bps"]
    ):
        owner_failures.append("owner_gate_failed:buy_maker_value")
    if additional["sell_maker_value_lcb"]["lower_bound"] < float(
        gates["sell_maker_value_delta_lcb_minimum_bps"]
    ):
        owner_failures.append("owner_gate_failed:sell_maker_value")
    if additional["multi_level_long_loss"]["lower_bound"] < 0.0:
        owner_failures.append("owner_gate_failed:multi_level_long_loss")
    if additional["multi_level_short_loss"]["lower_bound"] < 0.0:
        owner_failures.append("owner_gate_failed:multi_level_short_loss")
    owner_failures.append("continuous_71_day_confirmation_not_part_of_this_runner")
    owner = {
        "schema_version": f"{SCHEMA_VERSION}.owner_route",
        "promotion_label": "owner_risk_accepted_promotion",
        "raw_action_alpha_v2_scorecard_preserved": True,
        "raw_scorecard_sha256": raw["scorecard_sha256"],
        "only_allowed_override": "fills_retention_0.80_to_1.20",
        "fill_retention": fill_retention,
        "additional_gates": additional,
        "failures": sorted(set(owner_failures)),
        "owner_progression_eligible": False,
        "reason": "native_40_day_is_development_and_continuous_71_day_is_separate_required_evidence",
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
        "ranking_score": None,
    }
    return raw, owner, {
        "paired_metrics": paired,
        "metric_blockers": metric_blockers,
        "execution_integrity": execution_integrity,
    }


def finalize_panel(
    plan_path: Path,
    *,
    execution_amendment_path: Path,
) -> dict[str, Any]:
    plan = validate_execution_plan(plan_path, execution_amendment_path=execution_amendment_path)
    payload = plan["identity_payload"]
    output_root = Path(payload["output_root"])
    if (output_root / PANEL_SUCCESS).is_file():
        return read_panel(
            plan_path,
            execution_amendment_path=execution_amendment_path,
        )
    day_manifests = [
        _validate_day_admission(
            output_root / "days" / day,
            plan_identity=plan["plan_identity_sha256"],
            day=day,
        )
        for day in payload["ordered_utc_days"]
    ]
    daily_rows: list[dict[str, Any]] = []
    campaign_frames: list[pd.DataFrame] = []
    fill_frames: list[pd.DataFrame] = []
    for manifest in day_manifests:
        summary = _load_json(Path(manifest["summary"]["path"]), role="daily summary")
        daily_rows.extend(summary["arms"])
        campaign_frames.append(pd.read_parquet(manifest["campaigns"]["path"]))
        fill_frames.append(pd.read_parquet(manifest["fills"]["path"]))
    daily = pd.DataFrame(daily_rows).sort_values(["day", "arm"])
    campaigns = pd.concat(campaign_frames, ignore_index=True)
    fills = pd.concat(fill_frames, ignore_index=True)
    if len(daily) != 2 * EXPECTED_DAY_COUNT:
        raise NativeFullPathABError("panel lacks exactly two arm rows per day")
    raw, owner, metrics = _score_panel(daily, campaigns, plan=plan)
    daily_path = output_root / "daily.parquet"
    campaigns_path = output_root / "campaigns.parquet"
    fills_path = output_root / "fills.parquet"
    raw_path = output_root / "action-alpha-v2-scorecard.json"
    owner_path = output_root / "owner-route.json"
    report_path = output_root / "report.json"
    daily.to_parquet(daily_path, index=False, compression="zstd")
    campaigns.to_parquet(campaigns_path, index=False, compression="zstd")
    fills.to_parquet(fills_path, index=False, compression="zstd")
    _atomic_json(raw_path, raw)
    _atomic_json(owner_path, owner)
    report = {
        "schema_version": PANEL_SCHEMA_VERSION,
        "identity": IDENTITY,
        "created_at_utc": datetime.now(tz=UTC).isoformat(),
        "comparison": "candidate_1s_ml_on_minus_v9_10s_ml_on",
        "day_count": EXPECTED_DAY_COUNT,
        "execution_amendment_sha256": payload["execution_amendment"]["sha256"],
        "development_only": True,
        "paired_metrics": metrics["paired_metrics"],
        "metric_blockers": metrics["metric_blockers"],
        "execution_integrity": metrics["execution_integrity"],
        "raw_scorecard": raw,
        "owner_route": owner,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    _atomic_json(report_path, report)
    manifest = {
        "schema_version": PANEL_SCHEMA_VERSION,
        "identity": IDENTITY,
        "plan_identity_sha256": plan["plan_identity_sha256"],
        "execution_amendment_sha256": payload["execution_amendment"]["sha256"],
        "days": [
            {
                "utc_day": day,
                "manifest_sha256": _sha256_file(output_root / "days" / day / "manifest.json"),
            }
            for day in payload["ordered_utc_days"]
        ],
        "daily": _artifact(daily_path, _sha256_file(daily_path), role="daily panel"),
        "campaigns": _artifact(campaigns_path, _sha256_file(campaigns_path), role="campaign panel"),
        "fills": _artifact(fills_path, _sha256_file(fills_path), role="fill panel"),
        "raw_scorecard": _artifact(raw_path, _sha256_file(raw_path), role="raw scorecard"),
        "owner_route": _artifact(owner_path, _sha256_file(owner_path), role="owner route"),
        "report": _artifact(report_path, _sha256_file(report_path), role="panel report"),
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    manifest_path = output_root / PANEL_MANIFEST
    _atomic_json(manifest_path, manifest)
    _atomic_text(output_root / PANEL_SUCCESS, _sha256_file(manifest_path) + "\n")
    return report | {
        "panel_manifest_path": str(manifest_path),
        "panel_manifest_sha256": _sha256_file(manifest_path),
    }


def read_panel(
    plan_path: Path,
    *,
    execution_amendment_path: Path,
) -> dict[str, Any]:
    """Read only a fully admitted panel after validating every bound artifact."""

    plan = validate_execution_plan(plan_path, execution_amendment_path=execution_amendment_path)
    payload = plan["identity_payload"]
    output_root = Path(payload["output_root"])
    manifest_path = output_root / PANEL_MANIFEST
    marker = output_root / PANEL_SUCCESS
    manifest = _load_json(manifest_path, role="F03 panel manifest")
    if not marker.is_file() or marker.read_text(encoding="ascii").strip() != _sha256_file(
        manifest_path
    ):
        raise NativeFullPathABError("F03 panel admission marker drift")
    if (
        manifest.get("schema_version") != PANEL_SCHEMA_VERSION
        or manifest.get("identity") != IDENTITY
        or manifest.get("plan_identity_sha256") != plan["plan_identity_sha256"]
        or manifest.get("execution_amendment_sha256")
        != payload["execution_amendment"]["sha256"]
    ):
        raise NativeFullPathABError("F03 panel identity drift")
    expected_days = list(payload["ordered_utc_days"])
    day_rows = list(manifest.get("days") or ())
    if [row.get("utc_day") for row in day_rows if isinstance(row, Mapping)] != expected_days:
        raise NativeFullPathABError("F03 panel day order drift")
    for row, day in zip(day_rows, expected_days, strict=True):
        day_manifest = output_root / "days" / day / "manifest.json"
        _validate_day_admission(
            day_manifest.parent,
            plan_identity=plan["plan_identity_sha256"],
            day=day,
        )
        if row.get("manifest_sha256") != _sha256_file(day_manifest):
            raise NativeFullPathABError(f"{day} panel day binding drift")
    for role in ("daily", "campaigns", "fills", "raw_scorecard", "owner_route", "report"):
        _artifact_row(manifest[role], role=f"panel {role}")
    report = _load_json(Path(manifest["report"]["path"]), role="F03 panel report")
    if (
        report.get("schema_version") != PANEL_SCHEMA_VERSION
        or report.get("identity") != IDENTITY
        or report.get("execution_amendment_sha256")
        != payload["execution_amendment"]["sha256"]
    ):
        raise NativeFullPathABError("F03 panel report identity drift")
    return report | {
        "panel_manifest_path": str(manifest_path),
        "panel_manifest_sha256": _sha256_file(manifest_path),
        "panel_admission_validated": True,
    }


def execute_plan(
    plan_path: Path,
    *,
    execution_amendment_path: Path,
    days: Sequence[str] | None = None,
) -> dict[str, Any]:
    plan = validate_execution_plan(plan_path, execution_amendment_path=execution_amendment_path)
    ordered = list(plan["identity_payload"]["ordered_utc_days"])
    selected = ordered if days is None else list(days)
    if (
        not selected
        or selected != sorted(set(selected))
        or any(day not in ordered for day in selected)
    ):
        raise NativeFullPathABError("execution days must be a sorted subset of the frozen 40")
    for index, day in enumerate(selected, start=1):
        result = execute_day(
            plan_path,
            day=day,
            execution_amendment_path=execution_amendment_path,
        )
        print(f"[{index:02d}/{len(selected)}] {day} reused={result['reused']}", flush=True)
    output_root = Path(plan["identity_payload"]["output_root"])
    completed = [day for day in ordered if (output_root / "days" / day / DAY_SUCCESS).is_file()]
    if len(completed) != EXPECTED_DAY_COUNT:
        return {
            "status": "partial_execution",
            "completed_day_count": len(completed),
            "economic_outcomes_read": bool(completed),
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        }
    return finalize_panel(plan_path, execution_amendment_path=execution_amendment_path)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    prepare = sub.add_parser("prepare")
    prepare.add_argument("--candidate-overlay-panel-manifest", type=Path, required=True)
    prepare.add_argument("--control-overlay-panel-manifest", type=Path, required=True)
    prepare.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    prepare.add_argument("--precommit", type=Path, default=DEFAULT_PRECOMMIT)
    prepare.add_argument("--execution-amendment", type=Path, required=True)
    run = sub.add_parser("run")
    run.add_argument("--plan", type=Path, required=True)
    run.add_argument("--execution-amendment", type=Path, required=True)
    run.add_argument("--days", nargs="*")
    score = sub.add_parser("finalize")
    score.add_argument("--plan", type=Path, required=True)
    score.add_argument("--execution-amendment", type=Path, required=True)
    read = sub.add_parser("read")
    read.add_argument("--plan", type=Path, required=True)
    read.add_argument("--execution-amendment", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "prepare":
        result = prepare_execution_plan(
            candidate_overlay_panel_manifest=args.candidate_overlay_panel_manifest,
            control_overlay_panel_manifest=args.control_overlay_panel_manifest,
            execution_amendment_path=args.execution_amendment,
            output_root=args.output_root,
            precommit_path=args.precommit,
        )
    elif args.command == "run":
        result = execute_plan(
            args.plan,
            execution_amendment_path=args.execution_amendment,
            days=args.days,
        )
    elif args.command == "finalize":
        result = finalize_panel(args.plan, execution_amendment_path=args.execution_amendment)
    else:
        result = read_panel(args.plan, execution_amendment_path=args.execution_amendment)
    print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
