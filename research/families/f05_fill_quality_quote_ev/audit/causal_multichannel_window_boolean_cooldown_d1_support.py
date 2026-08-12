#!/usr/bin/env python3
"""Outcome-blind D-1/D/D+1 support audit for cooldown-duration v2.

The audit reads source identities only.  It never loads fills, campaigns,
rewards, PnL, markouts, or strategy outcomes.  Missing source support is kept
as an explicit reduced-support day; a malformed or drifting frozen denominator
fails closed.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from data_paths import data_root, marketdata_root

IDENTITY = "causal_multichannel_window_boolean_cooldown_duration_v2"
SCHEMA_VERSION = f"{IDENTITY}.d_minus_1_d_d_plus_1_support_audit.v1"
SYMBOL = "BTCUSDC"
EXCHANGE = "binance_futures"
EXPECTED_V2_SPEC_SHA256 = (
    "7064c12f1872c0ac7f9d07d15dd60dcc9053360256f309fe5d67fd96834b784f"
)
EXPECTED_PANEL_SPEC_SHA256 = (
    "98762d1000ff27aa6bbc72e3e219c6a73abf49da4bdd6f95f0f1679ec04a4abb"
)

PREFIX40 = (
    "2026-04-17", "2026-04-18", "2026-04-19", "2026-04-20",
    "2026-04-22", "2026-04-23", "2026-05-01", "2026-05-02",
    "2026-05-03", "2026-05-04", "2026-05-05", "2026-05-06",
    "2026-05-13", "2026-05-29", "2026-05-30", "2026-05-31",
    "2026-06-02", "2026-06-03", "2026-06-05", "2026-06-06",
    "2026-06-07", "2026-06-08", "2026-06-09", "2026-06-10",
    "2026-06-11", "2026-06-12", "2026-06-13", "2026-06-14",
    "2026-06-15", "2026-06-16", "2026-06-17", "2026-06-18",
    "2026-06-19", "2026-06-20", "2026-06-21", "2026-06-22",
    "2026-06-23", "2026-06-24", "2026-06-25", "2026-06-26",
)
ADDED10 = (
    "2026-06-29", "2026-07-03", "2026-07-04", "2026-07-05",
    "2026-07-06", "2026-07-07", "2026-07-08", "2026-07-09",
    "2026-07-10", "2026-07-16",
)

ROOT = Path(__file__).resolve().parents[4]
DATA_ROOT = data_root(ROOT)
V2_SPEC = ROOT / (
    "research/families/f05_fill_quality_quote_ev/docs/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_spec_20260810.json"
)
PANEL_SPEC = ROOT / (
    "research/families/f10_live_replay_attribution/docs/"
    "current_live_held_ber_replay_baseline_50d_spec_20260810.json"
)
STRICT_SPEC = ROOT / (
    "research/families/f10_live_replay_attribution/docs/"
    "current_live_held_ber_strict_native_latency_baseline_50d_v1_spec_20260810.json"
)
DEFAULT_PREFIX_OVERLAY_PANEL = DATA_ROOT / (
    "cache/f03_v9_10s_control_overlay_repair_v1/"
    "control_overlay_panel_admission_v1_1/panel-manifest.json"
)
DEFAULT_50D_PLAN = DATA_ROOT / (
    "cache/current_live_held_ber_baseline_50d_20260810/execution-plan.json"
)

_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_TRADE_HEADER = ("id", "price", "qty", "quote_qty", "time", "is_buyer_maker")


class SupportAuditError(RuntimeError):
    """Raised when a frozen identity or source index is malformed."""


@dataclass(frozen=True)
class AuditPaths:
    v2_spec: Path = V2_SPEC
    panel_spec: Path = PANEL_SPEC
    strict_spec: Path = STRICT_SPEC
    raw_cryptohft_root: Path = marketdata_root() / "cryptohftdata"
    normalized_root: Path = DATA_ROOT / "normalized_l2_100ms_v2"
    individual_trade_root: Path = DATA_ROOT / "raw_trades" / SYMBOL
    feature_manifest: Path = DATA_ROOT / (
        "features_btcusdc_causal_v12_expanded_source_aware_semantics_v6_20260802/"
        "causal_feature_manifest.json"
    )
    model_bundle_meta: Path = ROOT / (
        "models/saved_btcusdc_causal_v12_expanded_source_aware_semantics_v6_"
        "20260802_live_canary/bundle_meta.json"
    )
    prefix_overlay_panel: Path = DEFAULT_PREFIX_OVERLAY_PANEL
    panel_execution_plan: Path = DEFAULT_50D_PLAN


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _load_json(path: Path, *, label: str) -> dict[str, Any]:
    resolved = Path(path).expanduser().resolve()
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SupportAuditError(f"cannot read {label}: {resolved}") from exc
    if not isinstance(payload, dict):
        raise SupportAuditError(f"{label} root must be an object: {resolved}")
    return payload


def _require_sha(value: Any, *, label: str) -> str:
    digest = str(value or "").lower()
    if _SHA_RE.fullmatch(digest) is None:
        raise SupportAuditError(f"{label} is not a lowercase SHA256")
    return digest


def _require_bound_file(
    path: Path,
    expected_sha256: str,
    *,
    label: str,
) -> Path:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise SupportAuditError(f"{label} is missing: {resolved}")
    expected = _require_sha(expected_sha256, label=f"{label} expected hash")
    observed = file_sha256(resolved)
    if observed != expected:
        raise SupportAuditError(
            f"{label} hash drifted: {observed} != {expected} ({resolved})"
        )
    return resolved


def _canonical_day(value: Any, *, label: str) -> str:
    text = str(value)
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise SupportAuditError(f"{label} is not an ISO UTC day: {text!r}") from exc
    if parsed.isoformat() != text:
        raise SupportAuditError(f"{label} is not canonical YYYY-MM-DD: {text!r}")
    return text


def _strict_days(values: Any, *, expected: Sequence[str], label: str) -> tuple[str, ...]:
    if not isinstance(values, list):
        raise SupportAuditError(f"{label} must be an ordered list")
    days = tuple(_canonical_day(value, label=label) for value in values)
    if len(days) != len(set(days)):
        raise SupportAuditError(f"{label} contains duplicate days")
    if days != tuple(expected):
        missing = sorted(set(expected) - set(days))
        extra = sorted(set(days) - set(expected))
        raise SupportAuditError(
            f"{label} drifted from the frozen denominator: missing={missing}, extra={extra}"
        )
    return days


def _load_frozen_denominator(
    paths: AuditPaths,
    *,
    expected_v2_spec_sha256: str,
    expected_panel_spec_sha256: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    v2_path = _require_bound_file(
        paths.v2_spec,
        expected_v2_spec_sha256,
        label="cooldown v2 spec",
    )
    panel_path = _require_bound_file(
        paths.panel_spec,
        expected_panel_spec_sha256,
        label="50-day panel spec",
    )
    v2 = _load_json(v2_path, label="cooldown v2 spec")
    panel = _load_json(panel_path, label="50-day panel spec")
    if v2.get("identity") != IDENTITY:
        raise SupportAuditError("cooldown v2 identity drifted")
    source = v2.get("source_separation", {}).get("strict_native_2026", {})
    if source.get("panel_spec_sha256") != expected_panel_spec_sha256:
        raise SupportAuditError("cooldown v2 panel hash binding drifted")
    v2_days = v2.get("ordered_utc_days", {})
    _strict_days(v2_days.get("prefix40"), expected=PREFIX40, label="v2 prefix40")
    _strict_days(v2_days.get("added10"), expected=ADDED10, label="v2 added10")
    panel_prefix = panel.get("immutable_prefix", {}).get("ordered_utc_days")
    panel_added = panel.get("added_panel", {}).get("ordered_utc_days")
    _strict_days(panel_prefix, expected=PREFIX40, label="panel prefix40")
    _strict_days(panel_added, expected=ADDED10, label="panel added10")
    if set(PREFIX40).intersection(ADDED10):
        raise SupportAuditError("frozen prefix40 and added10 overlap")
    return v2, panel


def _metadata_identity(path: Path, *, content_sha256: str | None = None) -> dict[str, Any]:
    stat = path.stat()
    row: dict[str, Any] = {
        "path": str(path.resolve()),
        "size_bytes": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
    }
    if content_sha256 is not None:
        row["sha256"] = content_sha256
    return row


def _audit_raw_72h(
    *,
    root: Path,
    target_day: str,
    verify_large_hashes: bool,
) -> dict[str, Any]:
    target = date.fromisoformat(target_day)
    rows: list[dict[str, Any]] = []
    missing: list[str] = []
    for offset, role in ((-1, "warmup"), (0, "target"), (1, "continuation")):
        source_day = (target + timedelta(days=offset)).isoformat()
        for hour in range(24):
            path = (
                root.resolve()
                / EXCHANGE
                / source_day
                / f"{hour:02d}"
                / f"{SYMBOL}_orderbook.parquet.zst"
            )
            if not path.is_file() or path.stat().st_size <= 0:
                missing.append(str(path))
                continue
            digest = file_sha256(path) if verify_large_hashes else None
            rows.append(
                {
                    "role": role,
                    "utc_day": source_day,
                    "hour": hour,
                    **_metadata_identity(path, content_sha256=digest),
                }
            )
    return {
        "supported": not missing and len(rows) == 72,
        "expected_hour_count": 72,
        "present_hour_count": len(rows),
        "missing_paths": missing,
        "content_sha256_verified": verify_large_hashes,
        "ordered_file_identity_sha256": canonical_sha256(rows),
        "total_size_bytes": sum(int(row["size_bytes"]) for row in rows),
    }


def _read_trade_header(path: Path) -> tuple[str, ...]:
    opener = gzip.open if path.suffix == ".gz" else Path.open
    if path.suffix == ".gz":
        with opener(path, "rt", newline="", encoding="utf-8") as handle:
            return tuple(next(csv.reader(handle), ()))
    with opener(path, "r", newline="", encoding="utf-8") as handle:
        return tuple(next(csv.reader(handle), ()))


def _trade_file(root: Path, day: str) -> tuple[Path | None, str | None]:
    candidates = (
        root / f"{SYMBOL}-trades-{day}.csv",
        root / f"{SYMBOL}-trades-{day}.csv.gz",
    )
    present = [path.resolve() for path in candidates if path.is_file()]
    if len(present) > 1:
        raise SupportAuditError(f"duplicate individual-trade sources for {day}: {present}")
    if not present:
        return None, "missing_official_individual_trade_file"
    path = present[0]
    if path.stat().st_size <= 0:
        return None, "empty_official_individual_trade_file"
    if _read_trade_header(path) != _TRADE_HEADER:
        return None, "individual_trade_schema_mismatch"
    return path, None


def _audit_trades(
    *,
    root: Path,
    target_day: str,
    verify_large_hashes: bool,
) -> dict[str, Any]:
    target = date.fromisoformat(target_day)
    rows: list[dict[str, Any]] = []
    missing: list[dict[str, str]] = []
    for offset, role in ((-1, "warmup"), (0, "target"), (1, "continuation")):
        source_day = (target + timedelta(days=offset)).isoformat()
        path, reason = _trade_file(root, source_day)
        if path is None:
            missing.append({"role": role, "utc_day": source_day, "reason": str(reason)})
            continue
        digest = file_sha256(path) if verify_large_hashes else None
        rows.append(
            {
                "role": role,
                "utc_day": source_day,
                **_metadata_identity(path, content_sha256=digest),
            }
        )
    return {
        "supported": not missing and len(rows) == 3,
        "expected_day_count": 3,
        "present_day_count": len(rows),
        "missing": missing,
        "content_sha256_verified": verify_large_hashes,
        "ordered_file_identity_sha256": canonical_sha256(rows),
        "total_size_bytes": sum(int(row["size_bytes"]) for row in rows),
    }


def _parse_bool(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes"}


def _normalized_index(
    root: Path,
    *,
    verify_artifact_hashes: bool,
) -> tuple[dict[str, dict[str, str]], dict[str, Any]]:
    manifest_path = root.resolve() / "manifest.json"
    quality_path = root.resolve() / "daily_quality.csv"
    manifest = _load_json(manifest_path, label="normalized L2 manifest")
    if manifest.get("dataset_version") != "normalized_l2_100ms_v2":
        raise SupportAuditError("normalized L2 dataset identity drifted")
    if manifest.get("symbol") != SYMBOL:
        raise SupportAuditError("normalized L2 symbol drifted")
    expected_quality = _require_sha(
        manifest.get("daily_quality", {}).get("sha256"),
        label="normalized daily-quality hash",
    )
    _require_bound_file(quality_path, expected_quality, label="normalized daily quality")
    rows: dict[str, dict[str, str]] = {}
    with quality_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        required = {
            "day", "formal_eligible", "formal_exclusion_reason",
            "bbo_sha256", "bbo_size_bytes", "l2_sha256", "l2_size_bytes",
        }
        if reader.fieldnames is None or not required.issubset(reader.fieldnames):
            raise SupportAuditError("normalized daily-quality schema drifted")
        for raw in reader:
            day = _canonical_day(raw["day"], label="normalized day")
            if day in rows:
                raise SupportAuditError(f"duplicate normalized day: {day}")
            rows[day] = dict(raw)
    return rows, {
        "root": str(root.resolve()),
        "manifest_path": str(manifest_path),
        "manifest_sha256": file_sha256(manifest_path),
        "daily_quality_path": str(quality_path),
        "daily_quality_sha256": expected_quality,
        "artifact_hashes_verified": verify_artifact_hashes,
    }


def _audit_normalized_dplus1(
    *,
    root: Path,
    index: Mapping[str, Mapping[str, str]],
    target_day: str,
    verify_artifact_hashes: bool,
) -> dict[str, Any]:
    continuation = (date.fromisoformat(target_day) + timedelta(days=1)).isoformat()
    row = index.get(continuation)
    if row is None:
        return {
            "supported": False,
            "utc_day": continuation,
            "reason": "d_plus_1_absent_from_normalized_registry",
        }
    if not _parse_bool(row.get("formal_eligible")):
        reason = str(row.get("formal_exclusion_reason") or "not_formal_eligible")
        return {"supported": False, "utc_day": continuation, "reason": reason}
    files: dict[str, Any] = {}
    for kind in ("bbo", "l2"):
        expected_sha = _require_sha(row.get(f"{kind}_sha256"), label=f"{kind} hash")
        expected_size = int(row.get(f"{kind}_size_bytes", 0))
        path = root.resolve() / kind / f"{SYMBOL}-{kind}-{continuation}.parquet"
        if not path.is_file() or path.stat().st_size != expected_size:
            return {
                "supported": False,
                "utc_day": continuation,
                "reason": f"formal_{kind}_file_missing_or_size_drifted",
            }
        if verify_artifact_hashes and file_sha256(path) != expected_sha:
            return {
                "supported": False,
                "utc_day": continuation,
                "reason": f"formal_{kind}_file_hash_drifted",
            }
        files[kind] = {
            "path": str(path),
            "sha256": expected_sha,
            "size_bytes": expected_size,
        }
    return {
        "supported": True,
        "utc_day": continuation,
        "reason": None,
        "files": files,
        "artifact_hashes_verified": verify_artifact_hashes,
    }


def _feature_index(
    manifest_path: Path,
    expected_sha256: str,
    *,
    verify_artifact_hashes: bool,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    path = _require_bound_file(manifest_path, expected_sha256, label="v12 feature manifest")
    manifest = _load_json(path, label="v12 feature manifest")
    expected_semantics = {
        "feature_semantics_version": 6,
        "feature_cutoff_semantics": "strict_exclusive_completed_bucket_end",
        "feature_ready_offset_ms": 10_000,
    }
    for key, expected in expected_semantics.items():
        if manifest.get(key) != expected:
            raise SupportAuditError(f"v12 feature semantics drifted: {key}")
    index: dict[str, dict[str, Any]] = {}
    for raw in manifest.get("daily_files", []):
        if not isinstance(raw, Mapping):
            raise SupportAuditError("v12 feature row must be an object")
        day = _canonical_day(raw.get("day"), label="v12 feature day")
        if day in index:
            raise SupportAuditError(f"duplicate v12 feature day: {day}")
        feature_path = path.parent / str(raw.get("file"))
        expected_file_sha = _require_sha(raw.get("sha256"), label=f"{day} feature hash")
        expected_size = int(raw.get("size_bytes", 0))
        present = feature_path.is_file() and feature_path.stat().st_size == expected_size
        hash_valid = present and (
            not verify_artifact_hashes or file_sha256(feature_path) == expected_file_sha
        )
        index[day] = {
            "path": str(feature_path.resolve()),
            "sha256": expected_file_sha,
            "size_bytes": expected_size,
            "present": present,
            "hash_valid": hash_valid,
        }
    return index, {
        "path": str(path),
        "sha256": expected_sha256,
        "artifact_hashes_verified": verify_artifact_hashes,
    }


def _validate_overlay_artifact(
    *,
    day: str,
    manifest_path: Path,
    expected_manifest_sha256: str | None,
    verify_artifact_hashes: bool,
) -> dict[str, Any]:
    path = manifest_path.expanduser().resolve()
    if not path.is_file():
        return {"present": False, "utc_day": day, "reason": "overlay_manifest_missing"}
    observed_manifest_sha = file_sha256(path)
    if expected_manifest_sha256 is not None:
        expected = _require_sha(expected_manifest_sha256, label=f"{day} overlay manifest")
        if observed_manifest_sha != expected:
            raise SupportAuditError(f"{day} overlay manifest hash drifted")
    manifest = _load_json(path, label=f"{day} overlay manifest")
    manifest_identity = manifest.get("identity")
    identity_day = (
        manifest_identity.get("utc_day")
        if isinstance(manifest_identity, Mapping)
        else None
    )
    manifest_day = str(identity_day or manifest.get("utc_day"))
    if manifest_day != day:
        raise SupportAuditError(f"overlay day drifted: {manifest_day} != {day}")
    files = manifest.get("files")
    if isinstance(files, Mapping) and "model_overlay.npz" in files:
        file_row = files["model_overlay.npz"]
        artifact = path.parent / "model_overlay.npz"
        expected_sha = _require_sha(file_row.get("sha256"), label=f"{day} overlay hash")
        expected_size = int(file_row.get("size_bytes", 0))
    elif isinstance(files, Mapping) and set(files) == {"reference.json"}:
        reference_row = files["reference.json"]
        reference_path = path.parent / "reference.json"
        expected_reference_sha = _require_sha(
            reference_row.get("sha256"), label=f"{day} overlay reference hash"
        )
        expected_reference_size = int(reference_row.get("size_bytes", 0))
        if (
            not reference_path.is_file()
            or reference_path.stat().st_size != expected_reference_size
            or file_sha256(reference_path) != expected_reference_sha
        ):
            return {
                "present": False,
                "utc_day": day,
                "reason": "overlay_reference_missing_size_or_hash_drifted",
            }
        reference = _load_json(reference_path, label=f"{day} overlay reference")
        reference_day = str((reference.get("identity") or {}).get("day"))
        if reference_day != day:
            raise SupportAuditError(f"overlay reference day drifted: {reference_day} != {day}")
        data = reference.get("data")
        if not isinstance(data, Mapping):
            raise SupportAuditError(f"{day} overlay reference lacks data identity")
        artifact = Path(str(data.get("path"))).expanduser().resolve()
        expected_sha = _require_sha(data.get("sha256"), label=f"{day} overlay hash")
        expected_size = int(data.get("size_bytes", 0))
    else:
        artifact = path.parent / "model_overlay.npz"
        expected_sha = _require_sha(manifest.get("overlay_sha256"), label=f"{day} overlay hash")
        expected_size = artifact.stat().st_size if artifact.is_file() else 0
    if not artifact.is_file() or artifact.stat().st_size != expected_size:
        return {"present": False, "utc_day": day, "reason": "overlay_artifact_missing_or_size_drifted"}
    if verify_artifact_hashes and file_sha256(artifact) != expected_sha:
        return {"present": False, "utc_day": day, "reason": "overlay_artifact_hash_drifted"}
    return {
        "present": True,
        "utc_day": day,
        "manifest_path": str(path),
        "manifest_sha256": observed_manifest_sha,
        "artifact_path": str(artifact),
        "artifact_sha256": expected_sha,
        "artifact_hash_verified": verify_artifact_hashes,
        "prior_feature_sha256": (
            manifest_identity.get("prior_feature_sha256")
            if isinstance(manifest_identity, Mapping)
            else None
        )
        or manifest.get("prior_feature_sha256"),
        "target_feature_sha256": (
            manifest_identity.get("target_feature_sha256")
            if isinstance(manifest_identity, Mapping)
            else None
        )
        or manifest.get("target_feature_sha256"),
    }


def _overlay_index(
    *,
    prefix_panel_path: Path,
    expected_prefix_panel_sha256: str,
    execution_plan_path: Path,
    expected_days: Sequence[str],
    verify_artifact_hashes: bool,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    prefix_path = _require_bound_file(
        prefix_panel_path,
        expected_prefix_panel_sha256,
        label="prefix overlay panel",
    )
    prefix_panel = _load_json(prefix_path, label="prefix overlay panel")
    payload = prefix_panel.get("identity_payload", {})
    _strict_days(payload.get("ordered_utc_days"), expected=PREFIX40, label="overlay prefix40")
    components = payload.get("components")
    if not isinstance(components, list):
        raise SupportAuditError("prefix overlay components must be a list")
    index: dict[str, dict[str, Any]] = {}
    for component in components:
        day = _canonical_day(component.get("utc_day"), label="prefix overlay day")
        if day in index:
            raise SupportAuditError(f"duplicate overlay day: {day}")
        index[day] = _validate_overlay_artifact(
            day=day,
            manifest_path=Path(str(component.get("manifest_path"))),
            expected_manifest_sha256=str(component.get("manifest_sha256")),
            verify_artifact_hashes=verify_artifact_hashes,
        )
    plan = _load_json(execution_plan_path, label="50-day execution plan")
    _strict_days(plan.get("ordered_utc_days"), expected=expected_days, label="50-day plan")
    overlays = plan.get("added_overlays")
    if not isinstance(overlays, list):
        raise SupportAuditError("added overlay rows must be a list")
    for row in overlays:
        day = _canonical_day(row.get("utc_day"), label="added overlay day")
        if day in index:
            raise SupportAuditError(f"duplicate overlay day: {day}")
        manifest_path = execution_plan_path.parent / "overlays" / day / "manifest.json"
        index[day] = _validate_overlay_artifact(
            day=day,
            manifest_path=manifest_path,
            expected_manifest_sha256=None,
            verify_artifact_hashes=verify_artifact_hashes,
        )
        if index[day].get("artifact_sha256") != row.get("overlay_sha256"):
            raise SupportAuditError(f"{day} added overlay binding drifted")
    if tuple(day for day in expected_days if day in index) != tuple(expected_days):
        missing = [day for day in expected_days if day not in index]
        raise SupportAuditError(f"50-day target overlay index is incomplete: {missing}")
    return index, {
        "prefix_panel_path": str(prefix_path),
        "prefix_panel_sha256": expected_prefix_panel_sha256,
        "execution_plan_path": str(execution_plan_path.resolve()),
        "execution_plan_sha256": file_sha256(execution_plan_path),
        "materialized_overlay_days": len(index),
    }


def _panel_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    full = [str(row["target_day"]) for row in rows if row["full_source_support"]]
    materialized = [
        str(row["target_day"]) for row in rows if row["materialized_execution_support"]
    ]
    reduced = [
        {
            "target_day": str(row["target_day"]),
            "reasons": list(row["reduced_support_reasons"]),
        }
        for row in rows
        if not row["full_source_support"]
    ]
    return {
        "requested_day_count": len(rows),
        "full_source_support_day_count": len(full),
        "full_source_support_days": full,
        "reduced_support_day_count": len(reduced),
        "reduced_support_days": reduced,
        "materialized_execution_support_day_count": len(materialized),
        "materialized_execution_support_days": materialized,
    }


def build_support_audit(
    *,
    paths: AuditPaths | None = None,
    expected_v2_spec_sha256: str = EXPECTED_V2_SPEC_SHA256,
    expected_panel_spec_sha256: str = EXPECTED_PANEL_SPEC_SHA256,
    verify_artifact_hashes: bool = True,
    verify_large_file_hashes: bool = False,
) -> dict[str, Any]:
    """Return the frozen source-support report without reading economics."""

    paths = paths or AuditPaths()
    _, panel = _load_frozen_denominator(
        paths,
        expected_v2_spec_sha256=expected_v2_spec_sha256,
        expected_panel_spec_sha256=expected_panel_spec_sha256,
    )
    strict = _load_json(paths.strict_spec, label="strict native spec")
    truth = strict.get("exchange_truth", {})
    if (
        truth.get("raw_root") != str(paths.raw_cryptohft_root)
        or truth.get("exchange") != EXCHANGE
        or truth.get("symbol") != SYMBOL
        or int(truth.get("warmup_hours", -1)) != 24
    ):
        raise SupportAuditError("strict-native raw source binding drifted")

    sources = panel.get("sources", {})
    feature_manifest_path = _require_bound_file(
        paths.feature_manifest,
        str(sources.get("feature_manifest_sha256")),
        label="panel v12 feature manifest",
    )
    model_bundle_path = _require_bound_file(
        paths.model_bundle_meta,
        str(sources.get("model_bundle_meta_sha256")),
        label="v12 model bundle metadata",
    )
    normalized, normalized_binding = _normalized_index(
        paths.normalized_root,
        verify_artifact_hashes=verify_artifact_hashes,
    )
    features, feature_binding = _feature_index(
        feature_manifest_path,
        str(sources.get("feature_manifest_sha256")),
        verify_artifact_hashes=verify_artifact_hashes,
    )
    overlays, overlay_binding = _overlay_index(
        prefix_panel_path=paths.prefix_overlay_panel,
        expected_prefix_panel_sha256=str(sources.get("prefix_control_overlay_panel_sha256")),
        execution_plan_path=paths.panel_execution_plan,
        expected_days=(*PREFIX40, *ADDED10),
        verify_artifact_hashes=verify_artifact_hashes,
    )

    rows: list[dict[str, Any]] = []
    for panel_name, days in (("prefix40", PREFIX40), ("added10", ADDED10)):
        for target_day in days:
            target = date.fromisoformat(target_day)
            warmup_day = (target - timedelta(days=1)).isoformat()
            continuation_day = (target + timedelta(days=1)).isoformat()
            raw = _audit_raw_72h(
                root=paths.raw_cryptohft_root,
                target_day=target_day,
                verify_large_hashes=verify_large_file_hashes,
            )
            trades = _audit_trades(
                root=paths.individual_trade_root,
                target_day=target_day,
                verify_large_hashes=verify_large_file_hashes,
            )
            normalized_dplus1 = _audit_normalized_dplus1(
                root=paths.normalized_root,
                index=normalized,
                target_day=target_day,
                verify_artifact_hashes=verify_artifact_hashes,
            )
            target_overlay = overlays[target_day]
            continuation_overlay = overlays.get(continuation_day)
            feature_roles = {
                role: features.get(day)
                for role, day in (
                    ("warmup", warmup_day),
                    ("target", target_day),
                    ("continuation", continuation_day),
                )
            }
            continuation_materialized = bool(
                continuation_overlay and continuation_overlay.get("present")
            )
            continuation_rebuildable = bool(
                feature_roles["target"]
                and feature_roles["target"].get("hash_valid")
                and feature_roles["continuation"]
                and feature_roles["continuation"].get("hash_valid")
            )
            continuation_ready = continuation_materialized or continuation_rebuildable
            reasons: list[str] = []
            if not raw["supported"]:
                reasons.append("raw_cryptohft_72h_incomplete")
            if not trades["supported"]:
                reasons.append("official_individual_trades_dminus1_d_dplus1_incomplete")
            if not normalized_dplus1["supported"]:
                reasons.append(
                    "normalized_dplus1_not_formal:"
                    + str(normalized_dplus1.get("reason"))
                )
            if not target_overlay.get("present"):
                reasons.append("target_v12_overlay_missing_or_invalid")
            if not continuation_ready:
                reasons.append("continuation_v12_overlay_not_materialized_or_rebuildable")
            full_support = not reasons
            materialized_support = bool(
                full_support and continuation_materialized
            )
            rows.append(
                {
                    "panel": panel_name,
                    "target_day": target_day,
                    "warmup_day": warmup_day,
                    "continuation_day": continuation_day,
                    "raw_cryptohft_72h": raw,
                    "official_individual_trades_72h": trades,
                    "normalized_bbo_l2_dplus1": normalized_dplus1,
                    "v12": {
                        "feature_presence": {
                            role: bool(receipt and receipt.get("hash_valid"))
                            for role, receipt in feature_roles.items()
                        },
                        "target_overlay_materialized": bool(target_overlay.get("present")),
                        "continuation_overlay_materialized": continuation_materialized,
                        "continuation_overlay_rebuildable": continuation_rebuildable,
                        "continuation_overlay_source_ready": continuation_ready,
                    },
                    "full_source_support": full_support,
                    "materialized_execution_support": materialized_support,
                    "reduced_support_reasons": reasons,
                }
            )

    prefix_rows = rows[: len(PREFIX40)]
    added_rows = rows[len(PREFIX40) :]
    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "generated_at_utc": datetime.now(tz=UTC).isoformat(),
        "audit_scope": "outcome_blind_source_presence_and_identity_only",
        "denominator": {
            "prefix40": list(PREFIX40),
            "added10": list(ADDED10),
            "pooled50": [*PREFIX40, *ADDED10],
            "v2_spec_path": str(paths.v2_spec.resolve()),
            "v2_spec_sha256": expected_v2_spec_sha256,
            "panel_spec_path": str(paths.panel_spec.resolve()),
            "panel_spec_sha256": expected_panel_spec_sha256,
        },
        "source_bindings": {
            "raw_cryptohft_root": str(paths.raw_cryptohft_root.resolve()),
            "normalized": normalized_binding,
            "individual_trade_root": str(paths.individual_trade_root.resolve()),
            "v12_features": feature_binding,
            "v12_model_bundle_meta": {
                "path": str(model_bundle_path),
                "sha256": file_sha256(model_bundle_path),
            },
            "v12_overlays": overlay_binding,
        },
        "support": {
            "prefix40": _panel_summary(prefix_rows),
            "added10": _panel_summary(added_rows),
            "pooled50": _panel_summary(rows),
        },
        "days": rows,
        "permissions": {
            "economic_outcomes_read": False,
            "cooldown_labels_generated": False,
            "orders_simulated": False,
            "action_authorized": False,
            "live_authorized": False,
            "exact_historical_receive_time_authority": False,
        },
    }
    identity_payload = dict(report)
    identity_payload.pop("generated_at_utc")
    report["report_identity_sha256"] = canonical_sha256(identity_payload)
    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--verify-large-file-hashes",
        action="store_true",
        help="Hash all raw hourly book and daily trade files (slow).",
    )
    parser.add_argument(
        "--skip-artifact-hashes",
        action="store_true",
        help="Trust frozen normalized/feature/overlay hashes after size checks.",
    )
    args = parser.parse_args(argv)
    report = build_support_audit(
        verify_artifact_hashes=not args.skip_artifact_hashes,
        verify_large_file_hashes=args.verify_large_file_hashes,
    )
    print(json.dumps(report, indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
