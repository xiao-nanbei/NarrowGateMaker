#!/usr/bin/env python3
"""Run the hash-bound conditional-P3 joint-quote mechanics preflight."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import tempfile
from collections import Counter
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from data_paths import data_root, relocate_marketdata_path
from research.families.f02_empirical_p3_touch.audit.p3_touch_exact_distance_surface import (
    P3TouchExactDistanceSurface,
)
from research.families.f05_fill_quality_quote_ev.audit.conditional_p3_joint_quote_value_preflight import (
    GRID_ACTIONS,
    IDENTITY,
    SIDE_ROW_COLUMNS,
    PreflightGates,
    evaluate_preflight,
)

ROOT = Path(__file__).resolve().parents[4]
DATA_ROOT = data_root(ROOT)
DEFAULT_SPEC = (
    ROOT
    / "research/families/f05_fill_quality_quote_ev/docs/"
    "conditional_p3_joint_quote_value_preflight_v1_spec_20260803.json"
)
DEFAULT_OUTPUT = (
    DATA_ROOT
    / "reports"
    / "conditional_p3_joint_quote_value_preflight_v1_development_20260803"
)
TICK_SIZE = 0.1


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _identity(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(resolved)
    return {
        "path": str(resolved),
        "size_bytes": int(resolved.stat().st_size),
        "sha256": _sha256_file(resolved),
    }


def _require_identity(raw: Mapping[str, Any], *, label: str) -> Path:
    path = relocate_marketdata_path(str(raw.get("path", ""))).resolve()
    actual = _identity(path)
    if actual["sha256"] != str(raw.get("sha256", "")):
        raise RuntimeError(f"{label} SHA256 changed: {path}")
    if "size_bytes" in raw and actual["size_bytes"] != int(raw["size_bytes"]):
        raise RuntimeError(f"{label} size changed: {path}")
    return path


def _canonical_spec_sha256(payload: Mapping[str, Any]) -> str:
    normalized = dict(payload)
    normalized.pop("canonical_spec_identity_sha256", None)
    encoded = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _load_spec(path: Path) -> dict[str, Any]:
    spec = json.loads(path.read_text(encoding="utf-8"))
    if spec.get("identity") != IDENTITY:
        raise RuntimeError("unexpected preflight identity")
    if spec.get("canonical_spec_identity_sha256") != _canonical_spec_sha256(spec):
        raise RuntimeError("preflight canonical Spec hash mismatch")
    permissions = spec.get("permissions", {})
    if any(
        bool(permissions.get(field, False))
        for field in ("action_experiment_authorized", "live_authority")
    ):
        raise RuntimeError("preflight Spec unexpectedly grants authority")
    return spec


def _price_to_tick(values: pd.Series) -> tuple[np.ndarray, np.ndarray]:
    numeric = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    ratio = numeric / TICK_SIZE
    nearest = np.rint(ratio)
    valid = (
        np.isfinite(numeric)
        & (numeric > 0.0)
        & (np.abs(ratio - nearest) <= 1e-9)
    )
    return nearest.astype(np.int64), valid


def _fold_by_day(v4_spec: Mapping[str, Any]) -> dict[str, str]:
    result: dict[str, str] = {}
    for fold in v4_spec["chronological_oof"]["folds"]:
        fold_id = str(fold["fold_id"])
        for day in fold["test_days"]:
            day = str(day)
            if day in result:
                raise RuntimeError(f"duplicate OOF day: {day}")
            result[day] = fold_id
    return result


def _context_bindings(
    *,
    cache_usage_path: Path,
    fold_by_day: Mapping[str, str],
    days: set[str],
) -> dict[str, dict[str, Any]]:
    usage = pd.read_csv(cache_usage_path, dtype={"day": str})
    usage = usage.loc[
        usage["source"].astype(str).eq("native") & usage["day"].isin(days)
    ].copy()
    if usage["day"].duplicated().any():
        duplicates = sorted(usage.loc[usage["day"].duplicated(), "day"].unique())
        raise RuntimeError(f"duplicate native P3 contexts: {duplicates}")
    bindings: dict[str, dict[str, Any]] = {}
    for row in usage.itertuples(index=False):
        day = str(row.day)
        path = Path(str(row.cache_path)).expanduser().resolve()
        bindings[day] = {
            "source": "native",
            "fold_id": str(fold_by_day[day]),
            "context": _identity(path),
        }
    missing = sorted(days - set(bindings))
    if missing:
        raise RuntimeError(f"P3 context cache is missing OOF days: {missing}")
    return bindings


def _load_day_rows(
    *,
    day: str,
    partition: Path,
    surface: P3TouchExactDistanceSurface,
) -> tuple[list[dict[str, Any]], Counter[str], dict[str, Any]]:
    manifest_path = partition / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if str(manifest.get("day")) != day:
        raise RuntimeError(f"resolution partition day mismatch: {partition}")
    source_path = _require_identity(manifest["source_panel"], label=f"{day} source panel")
    mechanics_raw = manifest["mechanics_cache"]
    mechanics_path = relocate_marketdata_path(mechanics_raw["payload_path"]).resolve()
    if _sha256_file(mechanics_path) != str(mechanics_raw["payload_sha256"]):
        raise RuntimeError(f"{day} mechanics payload SHA256 changed")

    source = pd.read_parquet(
        source_path,
        columns=[
            "cohort_id",
            "day",
            "side",
            "inventory_role",
            "submit_ts_ns",
            "feature_ready_ts_ns",
            "best_bid",
            "best_ask",
        ],
    )
    source = source.loc[
        pd.to_numeric(source["submit_ts_ns"], errors="coerce")
        .mod(10_000_000_000)
        .eq(0)
    ].copy()
    candidate_ids = source["cohort_id"].astype(str).tolist()
    mechanics = pd.read_parquet(
        mechanics_path,
        columns=[
            "cohort_id",
            "action",
            "price_tick",
            "activation_status",
            "fill_qty",
        ],
        filters=[("cohort_id", "in", candidate_ids)],
    )
    if mechanics.duplicated(["cohort_id", "action"]).any():
        raise RuntimeError(f"{day} mechanics action identity is not unique")
    if set(mechanics["action"].astype(str).unique()) - set(GRID_ACTIONS):
        raise RuntimeError(f"{day} mechanics contains an unknown grid action")
    action_index = mechanics.set_index(["cohort_id", "action"])

    reasons: Counter[str] = Counter()
    admitted: list[dict[str, Any]] = []
    for record in source.to_dict("records"):
        cohort_id = str(record["cohort_id"])
        try:
            grid = action_index.loc[cohort_id]
        except KeyError:
            reasons["mechanics_cohort_missing"] += 1
            continue
        if set(grid.index.astype(str)) != set(GRID_ACTIONS):
            reasons["mechanics_grid_incomplete"] += 1
            continue
        bid_ticks, bid_valid = _price_to_tick(pd.Series([record["best_bid"]]))
        ask_ticks, ask_valid = _price_to_tick(pd.Series([record["best_ask"]]))
        if not bool(bid_valid[0] and ask_valid[0] and bid_ticks[0] < ask_ticks[0]):
            reasons["source_bbo_not_integer_ticks"] += 1
            continue
        candidate_prices = {
            action: int(grid.loc[action, "price_tick"]) for action in GRID_ACTIONS
        }
        query = surface.query(
            day=day,
            decision_ts_ms=int(record["submit_ts_ns"]) // 1_000_000,
            best_bid_ticks=int(bid_ticks[0]),
            best_ask_ticks=int(ask_ticks[0]),
            candidate_price_ticks={
                str(record["side"]).upper(): [
                    candidate_prices[action] for action in GRID_ACTIONS
                ]
            },
        )
        if not query["supported"]:
            reasons[str(query["fallback_reason"])] += 1
            continue
        side = str(record["side"]).upper()
        predictions = {
            int(item["price_ticks"]): float(item["probability"])
            for item in query["predictions"][side]
        }
        row: dict[str, Any] = {
            "day": day,
            "decision_ts_ns": int(record["submit_ts_ns"]),
            "cohort_id": cohort_id,
            "side": side,
            "inventory_role": str(record["inventory_role"]),
            "feature_ready_ts_ns": max(
                int(record["feature_ready_ts_ns"]),
                int(query["feature_ready_ts_ms"]) * 1_000_000,
            ),
            "best_bid_ticks": int(bid_ticks[0]),
            "best_ask_ticks": int(ask_ticks[0]),
            "p3_fold_id": str(query["fold_id"]),
            "p3_context_sha256": str(query["artifact_hashes"]["context_npz_sha256"]),
            "p3_supported": 1,
        }
        for action in GRID_ACTIONS:
            price = candidate_prices[action]
            activation_status = str(grid.loc[action, "activation_status"])
            row[f"{action}__price_ticks"] = price
            row[f"{action}__p_touch"] = predictions[price]
            row[f"{action}__activated"] = int(activation_status == "active")
            row[f"{action}__filled"] = int(float(grid.loc[action, "fill_qty"]) > 0.0)
        admitted.append(row)

    audit = {
        "day": day,
        "source_panel": _identity(source_path),
        "resolution_partition_manifest": _identity(manifest_path),
        "mechanics_payload": _identity(mechanics_path),
        "canonical_source_rows": int(len(source)),
        "admitted_side_rows": int(len(admitted)),
        "fallback_reasons": dict(sorted(reasons.items())),
    }
    return admitted, reasons, audit


def _atomic_output(
    *,
    output_dir: Path,
    report: Mapping[str, Any],
    side_rows: pd.DataFrame,
    day_audit: pd.DataFrame,
) -> None:
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f"{output_dir.name}.", dir=output_dir.parent))
    try:
        side_path = stage / "side_support.parquet"
        audit_path = stage / "day_audit.parquet"
        report_path = stage / "report.json"
        side_rows.to_parquet(side_path, index=False, compression="zstd")
        day_audit.to_parquet(audit_path, index=False, compression="zstd")
        report_path.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        manifest = {
            "identity": IDENTITY,
            "report": _identity(report_path),
            "side_support": _identity(side_path),
            "day_audit": _identity(audit_path),
        }
        (stage / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        (stage / "COMPLETE").write_text(
            hashlib.sha256(
                json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
            + "\n",
            encoding="ascii",
        )
        if output_dir.exists():
            shutil.rmtree(output_dir)
        os.replace(stage, output_dir)
    finally:
        if stage.exists():
            shutil.rmtree(stage)


def run(*, spec_path: Path, output_dir: Path) -> dict[str, Any]:
    spec_path = spec_path.expanduser().resolve()
    output_dir = output_dir.expanduser().resolve()
    spec = _load_spec(spec_path)
    identities = spec["artifact_identities"]
    for name in (
        "preflight_implementation",
        "preflight_contract",
        "exact_distance_surface_implementation",
    ):
        _require_identity(identities[name], label=name)
    v4_1_path = _require_identity(identities["p3_v4_1_spec"], label="P3 v4.1 Spec")
    cache_usage_path = _require_identity(
        identities["p3_v4_cache_usage"], label="P3 v4 cache usage"
    )
    f06_report_path = _require_identity(
        identities["f06_resolution_report"], label="F06 resolution report"
    )
    f06_report = json.loads(f06_report_path.read_text(encoding="utf-8"))
    if any(
        bool(f06_report.get("permissions", {}).get(field, False))
        for field in ("action_experiment_authorized", "live_deployment_authorized")
    ):
        raise RuntimeError("F06 predecessor unexpectedly grants authority")

    v4_1 = json.loads(v4_1_path.read_text(encoding="utf-8"))
    v4_spec_path = _require_identity(
        v4_1["identities"]["original_v4_spec"], label="P3 v4 Spec"
    )
    v4_spec = json.loads(v4_spec_path.read_text(encoding="utf-8"))
    fold_by_day = _fold_by_day(v4_spec)
    overlap_days = set(map(str, f06_report["development_days"])) & set(fold_by_day)
    bindings = _context_bindings(
        cache_usage_path=cache_usage_path,
        fold_by_day=fold_by_day,
        days=overlap_days,
    )
    surface = P3TouchExactDistanceSurface(
        v4_1_spec=_identity(v4_1_path),
        day_bindings=bindings,
        tick_size=TICK_SIZE,
    )

    resolution_root = relocate_marketdata_path(spec["resolution_partition_root"])
    all_rows: list[dict[str, Any]] = []
    audits: list[dict[str, Any]] = []
    fallback_reasons: Counter[str] = Counter()
    for day in sorted(overlap_days):
        rows, reasons, audit = _load_day_rows(
            day=day,
            partition=resolution_root / f"day={day}",
            surface=surface,
        )
        all_rows.extend(rows)
        fallback_reasons.update(reasons)
        audits.append(audit)
    side_rows = pd.DataFrame(all_rows, columns=SIDE_ROW_COLUMNS)
    gate_spec = spec["support_gates"]
    gates = PreflightGates(
        minimum_supported_days=int(gate_spec["minimum_supported_days"]),
        required_oof_fold_count=int(gate_spec["required_oof_fold_count"]),
        minimum_days_per_oof_fold=int(gate_spec["minimum_days_per_oof_fold"]),
        minimum_filled_rows_per_side_role_action=int(
            gate_spec["minimum_filled_rows_per_side_role_action"]
        ),
        require_all_grid_activated=bool(gate_spec["require_all_grid_activated"]),
        require_exact_bbo_clock=bool(gate_spec["require_exact_bbo_clock"]),
    )
    report = evaluate_preflight(side_rows, gates=gates)
    report.update(
        {
            "spec": _identity(spec_path),
            "artifact_identities": identities,
            "input_day_count": int(len(overlap_days)),
            "input_days": sorted(overlap_days),
            "fallback_reasons": dict(sorted(fallback_reasons.items())),
            "historical_evidence_boundary": spec["historical_evidence_boundary"],
        }
    )
    _atomic_output(
        output_dir=output_dir,
        report=report,
        side_rows=side_rows,
        day_audit=pd.DataFrame(audits),
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    report = run(spec_path=args.spec, output_dir=args.output_dir)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
