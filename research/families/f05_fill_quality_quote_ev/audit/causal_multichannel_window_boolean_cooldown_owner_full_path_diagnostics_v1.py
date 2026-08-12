#!/usr/bin/env python3
"""Read-only posthoc diagnostics for the owner cooldown full-path replay.

The module accepts either the owner runner root containing atomically admitted
daily artifacts or its finalized ``panel`` directory.  It never mutates the
source artifacts and never grants research, action, or live authority.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_full_path_v1 as owner,
)

IDENTITY = (
    "causal_multichannel_window_boolean_cooldown_"
    "owner_full_path_diagnostics_v1"
)
SCHEMA_VERSION = f"{IDENTITY}.v1"
SOURCE_IDENTITY = owner.IDENTITY
CONTROL_ARM = owner.CONTROL_ARM
CANDIDATE_ARM = owner.CANDIDATE_ARM
ARMS = (CONTROL_ARM, CANDIDATE_ARM)

REQUIRED_DAILY_COLUMNS = {
    "day",
    "arm",
    "terminal_mtm_pnl_usdc",
    "closed_campaign_value_usdc",
    "fills_total",
}
REQUIRED_CAMPAIGN_COLUMNS = {
    "day",
    "arm",
    "inventory_side",
    "closed",
    "terminal_value_usdc",
    "multi_level",
}
REQUIRED_FILL_COLUMNS = {
    "day",
    "arm",
    "side",
    "fill_qty",
    "quote_px",
    "ev_30s",
    "inventory_role_at_submit",
}
REQUIRED_DECISION_COLUMNS = set(owner.DECISION_COLUMNS)


class OwnerFullPathDiagnosticsError(RuntimeError):
    """Raised when posthoc inputs are incomplete, corrupt, or identity-drifted."""


@dataclass(frozen=True)
class DiagnosticInputs:
    """Validated, in-memory view of one partial or finalized replay panel."""

    source_root: Path
    input_mode: str
    expected_days: tuple[str, ...]
    admitted_days: tuple[str, ...]
    missing_days: tuple[str, ...]
    incomplete_day_directories: tuple[str, ...]
    unexpected_day_directories: tuple[str, ...]
    progress_rows: tuple[dict[str, Any], ...]
    bindings: tuple[dict[str, Any], ...]
    daily: pd.DataFrame
    campaigns: pd.DataFrame
    fills: pd.DataFrame
    decisions: pd.DataFrame


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path, *, role: str) -> dict[str, Any]:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise OwnerFullPathDiagnosticsError(f"missing {role}: {resolved}")
    try:
        value = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OwnerFullPathDiagnosticsError(f"invalid {role}: {resolved}") from exc
    if not isinstance(value, dict):
        raise OwnerFullPathDiagnosticsError(f"{role} must be a JSON object")
    return value


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, (np.floating,)):
        value = float(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _frozen_days() -> list[str]:
    spec = owner.baseline50._spec()
    days = owner.baseline50.ordered_days(spec)
    if len(days) != 50 or len(days) != len(set(days)):
        raise OwnerFullPathDiagnosticsError("frozen owner denominator drifted")
    return [str(day) for day in days]


def _require_columns(frame: pd.DataFrame, required: set[str], *, role: str) -> None:
    missing = sorted(required - set(frame.columns))
    if missing:
        raise OwnerFullPathDiagnosticsError(f"{role} lacks columns: {missing}")


def _read_parquet(path: Path, *, role: str) -> pd.DataFrame:
    try:
        return pd.read_parquet(Path(path))
    except Exception as exc:  # pragma: no cover - backend errors vary by platform
        raise OwnerFullPathDiagnosticsError(f"cannot read {role}: {path}") from exc


def _normalise_source(source: Path) -> tuple[Path, Path]:
    resolved = Path(source).expanduser().resolve()
    if resolved.is_file():
        if resolved.name != "report.json" or resolved.parent.name != "panel":
            raise OwnerFullPathDiagnosticsError(
                "file input must be the finalized panel/report.json"
            )
        return resolved.parent.parent, resolved.parent
    if resolved.name == "panel":
        return resolved.parent, resolved
    return resolved, resolved / "panel"


def _validate_source_permissions(permissions: Mapping[str, Any], *, role: str) -> None:
    forbidden = (
        "research_supported",
        "action_authorized",
        "live_authorized",
        "strict_native_queue_authority",
        "strict_queue_authority",
        "receive_time_transport_authority",
        "continuous_replay_authority",
    )
    enabled = sorted(key for key in forbidden if bool(permissions.get(key, False)))
    if enabled:
        raise OwnerFullPathDiagnosticsError(
            f"{role} unexpectedly carries authority: {enabled}"
        )


def _verify_panel_file(panel: Path, row: Mapping[str, Any]) -> Path:
    relative = str(row.get("relative_path", ""))
    if not relative or Path(relative).name != relative:
        raise OwnerFullPathDiagnosticsError("panel manifest contains unsafe path")
    path = panel / relative
    if not path.is_file() or _sha256_file(path) != str(row.get("sha256", "")):
        raise OwnerFullPathDiagnosticsError(f"final panel artifact drifted: {path}")
    return path


def _validate_frames(
    daily: pd.DataFrame,
    campaigns: pd.DataFrame,
    fills: pd.DataFrame,
    decisions: pd.DataFrame,
    *,
    admitted_days: Sequence[str],
) -> None:
    _require_columns(daily, REQUIRED_DAILY_COLUMNS, role="daily arm panel")
    if not campaigns.empty:
        _require_columns(
            campaigns, REQUIRED_CAMPAIGN_COLUMNS, role="campaign panel"
        )
    if not fills.empty:
        _require_columns(fills, REQUIRED_FILL_COLUMNS, role="fill panel")
    if not decisions.empty:
        _require_columns(
            decisions, REQUIRED_DECISION_COLUMNS, role="candidate decision panel"
        )

    admitted = set(admitted_days)
    for role, frame in (
        ("daily", daily),
        ("campaign", campaigns),
        ("fill", fills),
        ("decision", decisions),
    ):
        if frame.empty:
            continue
        unknown_days = sorted(set(frame["day"].astype(str)) - admitted)
        if unknown_days:
            raise OwnerFullPathDiagnosticsError(
                f"{role} panel contains unadmitted days: {unknown_days}"
            )
    unknown_arms = sorted(set(daily["arm"].astype(str)) - set(ARMS))
    if unknown_arms:
        raise OwnerFullPathDiagnosticsError(
            f"daily panel contains unknown arms: {unknown_arms}"
        )
    for day, frame in daily.groupby("day", sort=False):
        arms = list(frame["arm"].astype(str))
        if len(arms) != 2 or set(arms) != set(ARMS):
            raise OwnerFullPathDiagnosticsError(
                f"{day} does not contain exactly one row per arm"
            )
    for role, frame in (("campaign", campaigns), ("fill", fills)):
        if frame.empty:
            continue
        unknown = sorted(set(frame["arm"].astype(str)) - set(ARMS))
        if unknown:
            raise OwnerFullPathDiagnosticsError(
                f"{role} panel contains unknown arms: {unknown}"
            )


def _load_final_panel(
    source_root: Path,
    panel: Path,
    expected_days: Sequence[str],
) -> DiagnosticInputs:
    manifest_path = panel / "manifest.json"
    marker_path = panel / owner.PANEL_SUCCESS
    if not manifest_path.is_file() or not marker_path.is_file():
        raise OwnerFullPathDiagnosticsError("final panel admission is incomplete")
    if marker_path.read_text(encoding="ascii").strip() != _sha256_file(
        manifest_path
    ):
        raise OwnerFullPathDiagnosticsError("final panel admission marker drifted")
    manifest = _load_json(manifest_path, role="final panel manifest")
    if manifest.get("identity") != SOURCE_IDENTITY:
        raise OwnerFullPathDiagnosticsError("final panel source identity drifted")
    _validate_source_permissions(
        manifest.get("permissions") or {}, role="final panel"
    )
    files = {
        str(row.get("relative_path")): _verify_panel_file(panel, row)
        for row in list(manifest.get("files") or ())
        if isinstance(row, Mapping)
    }
    required = {
        "report.json",
        "daily_arms.parquet",
        "campaigns.parquet",
        "fills.parquet",
        "candidate_decisions.parquet",
    }
    missing = sorted(required - set(files))
    if missing:
        raise OwnerFullPathDiagnosticsError(
            f"final panel manifest lacks artifacts: {missing}"
        )
    report = _load_json(files["report.json"], role="final panel report")
    if report.get("identity") != SOURCE_IDENTITY:
        raise OwnerFullPathDiagnosticsError("final panel report identity drifted")
    _validate_source_permissions(report.get("permissions") or {}, role="panel report")
    panel_contract = report.get("panel") or {}
    if (
        panel_contract.get("daily_fresh_start") is not True
        or panel_contract.get("continuous_replay") is not False
    ):
        raise OwnerFullPathDiagnosticsError("final panel time semantics drifted")

    daily = _read_parquet(files["daily_arms.parquet"], role="daily arm panel")
    campaigns = _read_parquet(files["campaigns.parquet"], role="campaign panel")
    fills = _read_parquet(files["fills.parquet"], role="fill panel")
    decisions = _read_parquet(
        files["candidate_decisions.parquet"], role="candidate decision panel"
    )
    admitted_days = tuple(dict.fromkeys(daily["day"].astype(str).tolist()))
    if admitted_days != tuple(expected_days):
        raise OwnerFullPathDiagnosticsError("final panel day denominator drifted")
    _validate_frames(
        daily,
        campaigns,
        fills,
        decisions,
        admitted_days=admitted_days,
    )
    return DiagnosticInputs(
        source_root=source_root,
        input_mode="final_panel",
        expected_days=tuple(expected_days),
        admitted_days=admitted_days,
        missing_days=(),
        incomplete_day_directories=(),
        unexpected_day_directories=(),
        progress_rows=(),
        bindings=(
            {
                "role": "panel_manifest",
                "path": str(manifest_path),
                "sha256": _sha256_file(manifest_path),
            },
        ),
        daily=daily,
        campaigns=campaigns,
        fills=fills,
        decisions=decisions,
    )


def _read_progress(source_root: Path) -> tuple[dict[str, Any], ...]:
    rows: list[dict[str, Any]] = []
    progress_root = source_root / "progress"
    if not progress_root.is_dir():
        return ()
    for path in sorted(progress_root.glob("*.json")):
        try:
            row = _load_json(path, role=f"progress {path.stem}")
        except OwnerFullPathDiagnosticsError:
            row = {"day": path.stem, "state": "invalid_progress_artifact"}
        rows.append(row)
    return tuple(rows)


def _load_partial_days(
    source_root: Path,
    expected_days: Sequence[str],
) -> DiagnosticInputs:
    expected = set(expected_days)
    days_root = source_root / "days"
    actual_directories = (
        sorted(path.name for path in days_root.iterdir() if path.is_dir())
        if days_root.is_dir()
        else []
    )
    unexpected = sorted(set(actual_directories) - expected)
    incomplete: list[str] = []
    admitted: list[str] = []
    summaries: list[dict[str, Any]] = []
    campaign_frames: list[pd.DataFrame] = []
    fill_frames: list[pd.DataFrame] = []
    decision_frames: list[pd.DataFrame] = []
    bindings: list[dict[str, Any]] = []

    for day in expected_days:
        day_root = days_root / day
        if not day_root.exists():
            continue
        if not (day_root / "manifest.json").is_file() or not (
            day_root / owner.DAY_SUCCESS
        ).is_file():
            incomplete.append(day)
            continue
        manifest = owner._load_admitted_day(source_root, day)
        if manifest is None:  # pragma: no cover - guarded by files above
            incomplete.append(day)
            continue
        _validate_source_permissions(
            manifest.get("permissions") or {}, role=f"{day} admission"
        )
        summary = _load_json(Path(manifest["summary"]["path"]), role=f"{day} summary")
        if summary.get("identity") != SOURCE_IDENTITY or summary.get("day") != day:
            raise OwnerFullPathDiagnosticsError(f"{day} summary identity drifted")
        _validate_source_permissions(
            {
                "research_supported": summary.get("research_supported", False),
                "action_authorized": summary.get("action_authorized", False),
                "live_authorized": summary.get("live_authorized", False),
                "strict_queue_authority": summary.get(
                    "strict_queue_authority", False
                ),
                "receive_time_transport_authority": summary.get(
                    "receive_time_transport_authority", False
                ),
            },
            role=f"{day} summary",
        )
        arms = [dict(row) for row in list(summary.get("arms") or ())]
        if {str(row.get("arm")) for row in arms} != set(ARMS):
            raise OwnerFullPathDiagnosticsError(f"{day} summary arm identity drifted")
        summaries.extend(arms)
        campaign_frames.append(
            _read_parquet(Path(manifest["campaigns"]["path"]), role=f"{day} campaigns")
        )
        fill_frames.append(
            _read_parquet(Path(manifest["fills"]["path"]), role=f"{day} fills")
        )
        decision_frames.append(
            _read_parquet(
                Path(manifest["candidate_decisions"]["path"]),
                role=f"{day} candidate decisions",
            )
        )
        manifest_path = day_root / "manifest.json"
        bindings.append(
            {
                "role": "day_manifest",
                "day": day,
                "path": str(manifest_path),
                "sha256": _sha256_file(manifest_path),
            }
        )
        admitted.append(day)

    daily = pd.DataFrame(summaries)
    campaigns = (
        pd.concat(campaign_frames, ignore_index=True)
        if campaign_frames
        else pd.DataFrame(columns=sorted(REQUIRED_CAMPAIGN_COLUMNS))
    )
    nonempty_fills = [frame for frame in fill_frames if not frame.empty]
    fills = (
        pd.concat(nonempty_fills, ignore_index=True)
        if nonempty_fills
        else pd.DataFrame(columns=sorted(REQUIRED_FILL_COLUMNS))
    )
    nonempty_decisions = [frame for frame in decision_frames if not frame.empty]
    decisions = (
        pd.concat(nonempty_decisions, ignore_index=True)
        if nonempty_decisions
        else pd.DataFrame(columns=owner.DECISION_COLUMNS)
    )
    if admitted:
        _validate_frames(
            daily,
            campaigns,
            fills,
            decisions,
            admitted_days=admitted,
        )
    elif not daily.empty:
        raise OwnerFullPathDiagnosticsError("partial loader admitted no days")

    return DiagnosticInputs(
        source_root=source_root,
        input_mode="partial_admitted_days",
        expected_days=tuple(expected_days),
        admitted_days=tuple(admitted),
        missing_days=tuple(day for day in expected_days if day not in set(admitted)),
        incomplete_day_directories=tuple(incomplete),
        unexpected_day_directories=tuple(unexpected),
        progress_rows=_read_progress(source_root),
        bindings=tuple(bindings),
        daily=daily,
        campaigns=campaigns,
        fills=fills,
        decisions=decisions,
    )


def load_inputs(source: Path = owner.DEFAULT_OUTPUT) -> DiagnosticInputs:
    """Validate and load a final panel or the currently admitted partial days."""

    source_root, panel = _normalise_source(source)
    expected_days = _frozen_days()
    panel_manifest = panel / "manifest.json"
    panel_marker = panel / owner.PANEL_SUCCESS
    if panel_manifest.exists() or panel_marker.exists():
        return _load_final_panel(source_root, panel, expected_days)
    return _load_partial_days(source_root, expected_days)


def _ratio(numerator: float | int, denominator: float | int) -> float | None:
    return float(numerator) / float(denominator) if float(denominator) > 0.0 else None


def _numeric(values: Sequence[float] | np.ndarray) -> dict[str, Any]:
    array = np.asarray(values, dtype=float)
    array = array[np.isfinite(array)]
    if len(array) == 0:
        return {
            "count": 0,
            "sum": 0.0,
            "mean": None,
            "median": None,
            "p10": None,
            "p90": None,
            "min": None,
            "max": None,
        }
    return {
        "count": int(len(array)),
        "sum": float(array.sum()),
        "mean": float(array.mean()),
        "median": float(np.median(array)),
        "p10": float(np.quantile(array, 0.10)),
        "p90": float(np.quantile(array, 0.90)),
        "min": float(array.min()),
        "max": float(array.max()),
    }


def _normalise_text(value: Any, *, missing: str = "UNSPECIFIED") -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return missing
    text = str(value).strip()
    return text if text else missing


def _normalise_fallback_reason(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    if not text or text.lower() in {"none", "null", "nan"}:
        return None
    return text


def _decision_group(frame: pd.DataFrame, keys: Sequence[str]) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    rows: list[dict[str, Any]] = []
    grouped = frame.groupby(list(keys), dropna=False, sort=True)
    for raw_key, group in grouped:
        key_values = raw_key if isinstance(raw_key, tuple) else (raw_key,)
        row = {key: _json_safe(value) for key, value in zip(keys, key_values, strict=True)}
        row.update(
            {
                "decisions": int(len(group)),
                "days": int(group["day"].nunique()),
                "campaigns": int(
                    group[["day", "campaign_id"]].drop_duplicates().shape[0]
                ),
                "support_valid_count": int(group["support_valid"].sum()),
                "support_valid_rate": float(group["support_valid"].mean()),
                "nonbaseline_count": int(group["_nonbaseline"].sum()),
                "nonbaseline_rate": float(group["_nonbaseline"].mean()),
                "effective_duration_changed_count": int(
                    group["_duration_changed"].sum()
                ),
                "effective_duration_changed_rate": float(
                    group["_duration_changed"].mean()
                ),
                "explicit_fallback_count": int(group["_explicit_fallback"].sum()),
                "explicit_fallback_rate": float(
                    group["_explicit_fallback"].mean()
                ),
                "contract_control_count": int(group["_contract_control"].sum()),
                "contract_control_rate": float(group["_contract_control"].mean()),
                "duration_delta_ms": _numeric(group["_duration_delta_ms"]),
            }
        )
        rows.append(row)
    return rows


def _mechanics(inputs: DiagnosticInputs) -> dict[str, Any]:
    frame = inputs.decisions.copy()
    supported_daily = pd.DataFrame()
    if not inputs.daily.empty:
        supported_daily = inputs.daily[
            inputs.daily["arm"].astype(str).eq(CANDIDATE_ARM)
        ].copy()
    supported_days = (
        supported_daily.loc[
            supported_daily.get(
                "candidate_supported_day", pd.Series(False, index=supported_daily.index)
            ).astype(bool),
            "day",
        ]
        .astype(str)
        .tolist()
        if not supported_daily.empty
        else []
    )
    fallback_days = [day for day in inputs.admitted_days if day not in set(supported_days)]
    fallback_reasons = Counter()
    if not supported_daily.empty and "candidate_fallback_reason" in supported_daily:
        for value in supported_daily["candidate_fallback_reason"]:
            reason = _normalise_text(value, missing="none")
            if reason != "none":
                fallback_reasons[reason] += 1

    if frame.empty:
        return {
            "decision_count": 0,
            "nonbaseline_count": 0,
            "nonbaseline_rate": None,
            "effective_duration_changed_count": 0,
            "effective_duration_changed_rate": None,
            "support_valid_count": 0,
            "support_valid_rate": None,
            "contract_control_count": 0,
            "contract_control_rate": None,
            "supported_days": supported_days,
            "unsupported_fallback_days": fallback_days,
            "daily_fallback_reasons": dict(sorted(fallback_reasons.items())),
            "side_role_action_duration": [],
            "side_role": [],
            "action_duration": [],
            "fallback_reasons": {},
        }

    frame["side"] = frame["side"].map(lambda value: _normalise_text(value).upper())
    frame["role_at_fill"] = frame["role_at_fill"].map(
        lambda value: _normalise_text(value).lower()
    )
    frame["action_id"] = frame["action_id"].map(_normalise_text)
    frame["duration_ms"] = pd.to_numeric(frame["duration_ms"], errors="raise")
    frame["baseline_duration_ms"] = pd.to_numeric(
        frame["baseline_duration_ms"], errors="raise"
    )
    frame["support_valid"] = frame["support_valid"].astype(bool)
    frame["_duration_delta_ms"] = (
        frame["duration_ms"] - frame["baseline_duration_ms"]
    )
    frame["_duration_changed"] = frame["_duration_delta_ms"].abs().gt(1e-9)
    frame["_nonbaseline"] = ~frame["action_id"].eq("CONTROL_85N")
    frame["_fallback_reason"] = frame["fallback_reason"].map(
        _normalise_fallback_reason
    )
    frame["_contract_control"] = frame["_fallback_reason"].eq(
        "buy_control_by_contract"
    )
    frame["_explicit_fallback"] = (
        frame["_fallback_reason"].notna() & ~frame["_contract_control"]
    )
    frame["duration_ms"] = frame["duration_ms"].round().astype("int64")
    fallback_counts = Counter(
        frame.loc[frame["_explicit_fallback"], "_fallback_reason"].tolist()
    )
    action_counts = Counter(frame["action_id"].tolist())
    return {
        "decision_count": int(len(frame)),
        "nonbaseline_count": int(frame["_nonbaseline"].sum()),
        "nonbaseline_rate": float(frame["_nonbaseline"].mean()),
        "effective_duration_changed_count": int(frame["_duration_changed"].sum()),
        "effective_duration_changed_rate": float(frame["_duration_changed"].mean()),
        "support_valid_count": int(frame["support_valid"].sum()),
        "support_valid_rate": float(frame["support_valid"].mean()),
        "contract_control_count": int(frame["_contract_control"].sum()),
        "contract_control_rate": float(frame["_contract_control"].mean()),
        "control_action_count": int(frame["action_id"].eq("CONTROL_85N").sum()),
        "action_counts": dict(sorted(action_counts.items())),
        "supported_days": supported_days,
        "unsupported_fallback_days": fallback_days,
        "daily_fallback_reasons": dict(sorted(fallback_reasons.items())),
        "fallback_reasons": dict(sorted(fallback_counts.items())),
        "duration_ms": _numeric(frame["duration_ms"]),
        "baseline_duration_ms": _numeric(frame["baseline_duration_ms"]),
        "duration_delta_ms": _numeric(frame["_duration_delta_ms"]),
        "side_role_action_duration": _decision_group(
            frame, ("side", "role_at_fill", "action_id", "duration_ms")
        ),
        "side_role": _decision_group(frame, ("side", "role_at_fill")),
        "action_duration": _decision_group(frame, ("action_id", "duration_ms")),
    }


def _fill_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {
            "fills": 0,
            "fill_qty_btc": 0.0,
            "fill_notional_usdc": 0.0,
            "maker_value_30s_usdc": 0.0,
            "maker_value_30s_bps": None,
            "toxic_30s_count": 0,
            "toxic_30s_rate": None,
        }
    quantity = pd.to_numeric(frame["fill_qty"], errors="raise").to_numpy(float)
    price = pd.to_numeric(frame["quote_px"], errors="raise").to_numpy(float)
    ev = pd.to_numeric(frame["ev_30s"], errors="raise").to_numpy(float)
    notional = float(np.sum(quantity * price))
    value = float(np.sum(quantity * ev))
    toxic = (
        frame["toxic_30s"].astype(bool).to_numpy()
        if "toxic_30s" in frame
        else ev < 0.0
    )
    return {
        "fills": int(len(frame)),
        "fill_qty_btc": float(quantity.sum()),
        "fill_notional_usdc": notional,
        "maker_value_30s_usdc": value,
        "maker_value_30s_bps": 1e4 * value / notional if notional > 0.0 else None,
        "toxic_30s_count": int(toxic.sum()),
        "toxic_30s_rate": float(toxic.mean()) if len(toxic) else None,
    }


def _delta_metrics(
    control: Mapping[str, Any],
    candidate: Mapping[str, Any],
    *,
    fields: Sequence[str],
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for field in fields:
        left = control.get(field)
        right = candidate.get(field)
        result[field] = (
            float(right) - float(left)
            if left is not None and right is not None
            else None
        )
    return result


def _fill_decomposition(inputs: DiagnosticInputs) -> dict[str, Any]:
    frame = inputs.fills.copy()
    if frame.empty:
        return {
            "semantics": "aggregate_fill_path_decomposition_not_fill_pairing",
            "overall": _arm_decomposition(pd.DataFrame(), _fill_metrics),
            "side_role": [],
        }
    frame["side"] = frame["side"].map(lambda value: _normalise_text(value).upper())
    frame["role"] = frame["inventory_role_at_submit"].map(
        lambda value: _normalise_text(value).lower()
    )
    observed = set(zip(frame["side"], frame["role"], strict=False))
    required = {
        (side, role)
        for side in ("BUY", "SELL")
        for role in ("opener", "add", "reducing")
    }
    keys = sorted(observed | required)
    rows: list[dict[str, Any]] = []
    for side, role in keys:
        subset = frame[frame["side"].eq(side) & frame["role"].eq(role)]
        rows.append({"side": side, "role": role, **_arm_decomposition(subset, _fill_metrics)})
    side_rows = []
    for side in ("BUY", "SELL"):
        side_rows.append(
            {
                "side": side,
                **_arm_decomposition(frame[frame["side"].eq(side)], _fill_metrics),
            }
        )
    role_rows = []
    for role in sorted(set(frame["role"]) | {"opener", "add", "reducing"}):
        role_rows.append(
            {
                "role": role,
                **_arm_decomposition(frame[frame["role"].eq(role)], _fill_metrics),
            }
        )
    return {
        "semantics": "aggregate_fill_path_decomposition_not_fill_pairing",
        "overall": _arm_decomposition(frame, _fill_metrics),
        "by_side": side_rows,
        "by_role": role_rows,
        "side_role": rows,
    }


def _campaign_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {
            "campaigns": 0,
            "closed_campaigns": 0,
            "closed_rate": None,
            "terminal_value_usdc": 0.0,
            "terminal_value_mean_usdc": None,
            "positive_campaigns": 0,
            "negative_campaigns": 0,
            "negative_terminal_value_usdc": 0.0,
            "q10_usdc": None,
            "cvar10_usdc": None,
            "p05_usdc": None,
            "p01_usdc": None,
            "median_usdc": None,
            "min_usdc": None,
            "max_usdc": None,
        }
    value = pd.to_numeric(frame["terminal_value_usdc"], errors="raise").to_numpy(float)
    q10 = float(np.quantile(value, 0.10))
    negative = value[value < 0.0]
    return {
        "campaigns": int(len(frame)),
        "closed_campaigns": int(frame["closed"].astype(bool).sum()),
        "closed_rate": float(frame["closed"].astype(bool).mean()),
        "terminal_value_usdc": float(value.sum()),
        "terminal_value_mean_usdc": float(value.mean()),
        "positive_campaigns": int((value > 0.0).sum()),
        "negative_campaigns": int((value < 0.0).sum()),
        "negative_terminal_value_usdc": float(negative.sum()),
        "q10_usdc": q10,
        "cvar10_usdc": float(value[value <= q10].mean()),
        "p05_usdc": float(np.quantile(value, 0.05)),
        "p01_usdc": float(np.quantile(value, 0.01)),
        "median_usdc": float(np.median(value)),
        "min_usdc": float(value.min()),
        "max_usdc": float(value.max()),
    }


def _arm_decomposition(frame: pd.DataFrame, metric_fn: Any) -> dict[str, Any]:
    metrics: dict[str, dict[str, Any]] = {}
    for arm in ARMS:
        arm_frame = (
            frame[frame["arm"].astype(str).eq(arm)]
            if not frame.empty and "arm" in frame
            else pd.DataFrame()
        )
        metrics[arm] = metric_fn(arm_frame)
    fields = sorted(set(metrics[CONTROL_ARM]) & set(metrics[CANDIDATE_ARM]))
    numeric_fields = [
        field
        for field in fields
        if isinstance(metrics[CONTROL_ARM][field], (int, float))
        and not isinstance(metrics[CONTROL_ARM][field], bool)
    ]
    result = {
        "control": metrics[CONTROL_ARM],
        "candidate": metrics[CANDIDATE_ARM],
        "delta_candidate_minus_control": _delta_metrics(
            metrics[CONTROL_ARM], metrics[CANDIDATE_ARM], fields=numeric_fields
        ),
    }
    count_field = "fills" if "fills" in fields else "campaigns"
    result[f"{count_field}_retention"] = _ratio(
        metrics[CANDIDATE_ARM][count_field], metrics[CONTROL_ARM][count_field]
    )
    return result


def _campaign_decomposition(inputs: DiagnosticInputs) -> dict[str, Any]:
    frame = inputs.campaigns.copy()
    if frame.empty:
        return {
            "semantics": "aggregate_path_decomposition_not_campaign_pairing",
            "overall": _arm_decomposition(pd.DataFrame(), _campaign_metrics),
            "inventory_side_level": [],
        }
    frame["inventory_side"] = frame["inventory_side"].map(
        lambda value: _normalise_text(value).upper()
    )
    frame["level"] = np.where(frame["multi_level"].astype(bool), "MULTI", "SINGLE")
    rows: list[dict[str, Any]] = []
    for side in ("LONG", "SHORT"):
        for level in ("SINGLE", "MULTI"):
            subset = frame[
                frame["inventory_side"].eq(side) & frame["level"].eq(level)
            ]
            rows.append(
                {
                    "inventory_side": side,
                    "inventory_level": level,
                    **_arm_decomposition(subset, _campaign_metrics),
                }
            )
    side_rows = [
        {
            "inventory_side": side,
            **_arm_decomposition(
                frame[frame["inventory_side"].eq(side)], _campaign_metrics
            ),
        }
        for side in ("LONG", "SHORT")
    ]
    level_rows = [
        {
            "inventory_level": level,
            **_arm_decomposition(frame[frame["level"].eq(level)], _campaign_metrics),
        }
        for level in ("SINGLE", "MULTI")
    ]
    return {
        "semantics": "aggregate_path_decomposition_not_campaign_pairing",
        "overall": _arm_decomposition(frame, _campaign_metrics),
        "by_inventory_side": side_rows,
        "by_inventory_level": level_rows,
        "inventory_side_level": rows,
        "terminal_tail": {
            "overall": _arm_decomposition(frame, _campaign_metrics),
            "by_inventory_side": side_rows,
            "multi_level": level_rows[1],
        },
    }


def _daily_economics(inputs: DiagnosticInputs) -> dict[str, Any]:
    if inputs.daily.empty:
        return {
            "days": 0,
            "control": {},
            "candidate": {},
            "paired_delta": {},
        }
    daily = inputs.daily.copy()
    rows: list[dict[str, Any]] = []
    for day, group in daily.groupby("day", sort=True):
        by_arm = {str(row["arm"]): row for _, row in group.iterrows()}
        control = by_arm[CONTROL_ARM]
        candidate = by_arm[CANDIDATE_ARM]
        rows.append(
            {
                "day": str(day),
                "terminal_delta_usdc": float(candidate["terminal_mtm_pnl_usdc"])
                - float(control["terminal_mtm_pnl_usdc"]),
                "closed_campaign_delta_usdc": float(
                    candidate["closed_campaign_value_usdc"]
                )
                - float(control["closed_campaign_value_usdc"]),
                "fills_delta": int(candidate["fills_total"])
                - int(control["fills_total"]),
            }
        )
    paired = pd.DataFrame(rows)
    arms: dict[str, Any] = {}
    for arm in ARMS:
        selected = daily[daily["arm"].astype(str).eq(arm)]
        arms[arm] = {
            "days": int(len(selected)),
            "terminal_mtm_pnl_usdc": float(
                selected["terminal_mtm_pnl_usdc"].sum()
            ),
            "terminal_mtm_pnl_usdc_per_day": float(
                selected["terminal_mtm_pnl_usdc"].mean()
            ),
            "closed_campaign_value_usdc": float(
                selected["closed_campaign_value_usdc"].sum()
            ),
            "fills_total": int(selected["fills_total"].sum()),
        }
    terminal_delta = paired["terminal_delta_usdc"].to_numpy(float)
    q10 = float(np.quantile(terminal_delta, 0.10)) if len(terminal_delta) else None
    return {
        "days": int(len(paired)),
        "control": arms[CONTROL_ARM],
        "candidate": arms[CANDIDATE_ARM],
        "fill_retention": _ratio(
            arms[CANDIDATE_ARM]["fills_total"], arms[CONTROL_ARM]["fills_total"]
        ),
        "paired_delta": {
            "terminal_mtm_pnl_usdc": _numeric(terminal_delta),
            "closed_campaign_value_usdc": _numeric(
                paired["closed_campaign_delta_usdc"].to_numpy(float)
            ),
            "fills": _numeric(paired["fills_delta"].to_numpy(float)),
            "positive_terminal_delta_days": int((terminal_delta > 0.0).sum()),
            "zero_terminal_delta_days": int((terminal_delta == 0.0).sum()),
            "terminal_delta_q10_usdc": q10,
            "terminal_delta_cvar10_usdc": (
                float(terminal_delta[terminal_delta <= q10].mean())
                if q10 is not None
                else None
            ),
        },
        "daily_rows": rows,
    }


def _source_status(inputs: DiagnosticInputs) -> dict[str, Any]:
    progress_states = Counter(
        _normalise_text(row.get("state"), missing="unknown")
        for row in inputs.progress_rows
    )
    return {
        "input_mode": inputs.input_mode,
        "source_root": str(inputs.source_root),
        "expected_days": len(inputs.expected_days),
        "admitted_days": len(inputs.admitted_days),
        "admitted_day_list": list(inputs.admitted_days),
        "missing_days": list(inputs.missing_days),
        "incomplete_day_directories": list(inputs.incomplete_day_directories),
        "unexpected_day_directories": list(inputs.unexpected_day_directories),
        "fraction_admitted": _ratio(len(inputs.admitted_days), len(inputs.expected_days)),
        "progress_state_counts": dict(sorted(progress_states.items())),
        "progress_rows": list(inputs.progress_rows),
        "input_bindings": list(inputs.bindings),
    }


def status(source: Path = owner.DEFAULT_OUTPUT) -> dict[str, Any]:
    """Return validated partial/final source status without creating artifacts."""

    return _source_status(load_inputs(source))


def diagnose(source: Path = owner.DEFAULT_OUTPUT) -> dict[str, Any]:
    """Produce read-only mechanics and economic path decomposition."""

    inputs = load_inputs(source)
    return _json_safe(
        {
            "schema_version": SCHEMA_VERSION,
            "identity": IDENTITY,
            "source_identity": SOURCE_IDENTITY,
            "created_at_utc": datetime.now(tz=UTC).isoformat(),
            "status": _source_status(inputs),
            "evidence_scope": {
                "evidence_route": "owner_risk_accepted_outcome_informed_posthoc",
                "queue_semantics": "modeled_queue",
                "market_clock": "historical_exchange_time_merged_100ms",
                "daily_fresh_start": True,
                "continuous_replay": False,
                "raw_snapshot_delta_exact_queue_used": False,
                "receive_time_transport_used": False,
                "python_authoritative": True,
                "python_cpp_policy_parity_claimed": False,
                "diagnostic_only": True,
            },
            "mechanics": _mechanics(inputs),
            "fills_and_maker_value": _fill_decomposition(inputs),
            "campaigns_and_terminal_tail": _campaign_decomposition(inputs),
            "daily_economics": _daily_economics(inputs),
            "permissions": {
                "research_supported": False,
                "strict_native_queue_authority": False,
                "receive_time_transport_authority": False,
                "continuous_replay_authority": False,
                "action_authorized": False,
                "live_authorized": False,
            },
            "interpretation_limits": [
                "posthoc diagnostics do not alter the frozen owner policy",
                "candidate-control campaign rows are aggregate path decompositions, not paired campaign identities",
                "modeled-queue daily-fresh-start evidence cannot establish strict-native queue or live transport authority",
                "partial mode includes only atomically admitted days and is not a final 50-day result",
            ],
        }
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("status", "diagnose"), nargs="?", default="diagnose")
    parser.add_argument("--source", type=Path, default=owner.DEFAULT_OUTPUT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    payload = status(args.source) if args.command == "status" else diagnose(args.source)
    print(json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
