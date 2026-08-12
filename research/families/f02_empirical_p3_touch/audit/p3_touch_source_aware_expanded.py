#!/usr/bin/env python3
"""Source-aware expanded P3 touch calibration and historical transport audit.

This successor keeps the frozen v2 estimator unchanged. It fits separate
2025-provider, 2026-current, and pooled-expanded empirical touch curves, then
scores them on the already-read 2026 historical transport panels. No output
from this module has operational, action, or live authority.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from data_paths import resolve_portable_path
from research.families.f02_empirical_p3_touch.audit.p3_touch_calibration import (
    window_reaches,
)
from research.families.f02_empirical_p3_touch.fill_probability import (
    FillProbabilityModel,
)
from research.governance.paths import resolve_research_path

ROOT = Path(__file__).resolve().parents[4]
SPEC_SCHEMA_VERSION = "narrowgate_p3_touch_source_aware_expanded.v3.spec"
DAY_MANIFEST_SCHEMA_VERSION = "narrowgate_p3_touch_day_manifest.v3"
REPORT_SCHEMA_VERSION = "narrowgate_p3_touch_source_aware_expanded.v3"
MODEL_SCHEMA_VERSION = "narrowgate_p3_touch_calibration.v3"
SIDES = ("BUY", "SELL")
FIT_MODELS = ("2025_provider", "2026_train", "expanded")
EVALUATION_MODELS = ("current_v2", *FIT_MODELS)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(payload: Any) -> str:
    raw = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _canonical_identity(payload: Mapping[str, Any], field: str) -> str:
    normalized = dict(payload)
    normalized.pop(field, None)
    return canonical_sha256(normalized)


def _require_file_identity(identity: Mapping[str, Any], label: str) -> Path:
    path = resolve_research_path(str(identity["path"]))
    if not path.is_file():
        raise FileNotFoundError(f"{label} missing: {path}")
    observed = sha256_file(path)
    expected = str(identity["sha256"])
    if observed != expected:
        raise ValueError(
            f"{label} hash mismatch: observed={observed} expected={expected}"
        )
    return path


def validate_contract_structure(
    spec: Mapping[str, Any],
    day_manifest: Mapping[str, Any],
) -> None:
    if spec.get("schema_version") != SPEC_SCHEMA_VERSION:
        raise ValueError("unsupported P3 v3 spec schema")
    if _canonical_identity(spec, "canonical_spec_identity_sha256") != spec.get(
        "canonical_spec_identity_sha256"
    ):
        raise ValueError("P3 v3 canonical spec hash mismatch")
    if day_manifest.get("schema_version") != DAY_MANIFEST_SCHEMA_VERSION:
        raise ValueError("unsupported P3 v3 day-manifest schema")
    if _canonical_identity(
        day_manifest, "canonical_manifest_identity_sha256"
    ) != day_manifest.get("canonical_manifest_identity_sha256"):
        raise ValueError("P3 v3 canonical day-manifest hash mismatch")

    estimand = spec["estimand"]
    if estimand.get("event_type") != "touch":
        raise ValueError("P3 v3 event_type must be touch")
    if float(estimand.get("horizon_s", 0.0)) != 10.0:
        raise ValueError("P3 v3 horizon must be exactly 10 seconds")
    if estimand.get("distance_unit") != "USDC_per_BTC":
        raise ValueError("P3 v3 distance unit must be USDC_per_BTC")
    if bool(estimand.get("queue_included", True)):
        raise ValueError("P3 v3 must not include queue conversion")
    if bool(estimand.get("lifecycle_included", True)):
        raise ValueError("P3 v3 must not include order lifecycle")

    panels = day_manifest["panels"]
    expected_counts = {
        "fit_2025_provider": 93,
        "fit_2026_current": 69,
        "historical_2026_validation": 24,
        "historical_2026_test_diagnostic": 24,
    }
    all_days: list[str] = []
    for name, expected in expected_counts.items():
        days = [str(day) for day in panels.get(name, [])]
        if len(days) != expected:
            raise ValueError(f"{name} must contain {expected} days")
        if days != sorted(days) or len(days) != len(set(days)):
            raise ValueError(f"{name} days must be chronological and unique")
        all_days.extend(days)
    if len(all_days) != len(set(all_days)):
        raise ValueError("P3 v3 panels overlap")

    permissions = spec["permissions"]
    if not bool(permissions.get("validation_previously_read", False)):
        raise ValueError("historical validation must remain marked previously read")
    for forbidden in (
        "prediction_authority",
        "action_authority",
        "live_authority",
        "overwrite_current_v2_artifact",
    ):
        if bool(permissions.get(forbidden, False)):
            raise ValueError(f"P3 v3 cannot grant {forbidden}")


def load_contract(spec_path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    spec_path = spec_path.expanduser().resolve()
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    manifest_identity = spec["identities"]["day_manifest"]
    day_manifest_path = _require_file_identity(manifest_identity, "day_manifest")
    day_manifest = json.loads(day_manifest_path.read_text(encoding="utf-8"))
    validate_contract_structure(spec, day_manifest)
    for label, identity in spec["identities"].items():
        _require_file_identity(identity, label)
    return spec, day_manifest


def _identity_rows_sha256(rows: Sequence[Mapping[str, str]]) -> str:
    normalized = sorted(
        (
            {
                "day": str(row["day"]),
                "kind": str(row["kind"]),
                "sha256": str(row["sha256"]),
            }
            for row in rows
        ),
        key=lambda row: (row["day"], row["kind"], row["sha256"]),
    )
    return canonical_sha256(normalized)


def _verify_2025_selection(
    day_manifest: Mapping[str, Any],
) -> tuple[list[dict[str, str]], dict[str, Path]]:
    source = day_manifest["source_identities"]["fit_2025_provider"]
    quality_root = resolve_portable_path(str(source["quality_root"])).resolve()
    bbo_root = resolve_portable_path(str(source["bbo_root"])).resolve()
    trade_root = resolve_portable_path(str(source["trade_root"])).resolve()
    expected_days = list(day_manifest["panels"]["fit_2025_provider"])

    selected: list[str] = []
    quality_rows: list[dict[str, str]] = []
    bbo_rows: list[dict[str, str]] = []
    trade_rows: list[dict[str, str]] = []
    for quality_path in sorted(quality_root.glob("BTCUSDC-2025-*.json")):
        payload = json.loads(quality_path.read_text(encoding="utf-8"))
        if payload.get("provider_normalized_replay_candidate") is not True:
            continue
        day = str(payload["day"])
        selected.append(day)
        bbo_path = bbo_root / f"BTCUSDC-bbo-{day}.parquet"
        trade_path = trade_root / f"BTCUSDC-aggTrades-{day}.csv"
        if not bbo_path.is_file() or not trade_path.is_file():
            raise FileNotFoundError(f"missing 2025 P3 input for {day}")
        bbo_hash = sha256_file(bbo_path)
        if bbo_hash != str(payload["bbo_output"]["sha256"]):
            raise ValueError(f"2025 provider BBO hash mismatch for {day}")
        quality_rows.append(
            {"day": day, "kind": "quality", "sha256": sha256_file(quality_path)}
        )
        bbo_rows.append({"day": day, "kind": "bbo", "sha256": bbo_hash})
        trade_rows.append(
            {"day": day, "kind": "aggTrades", "sha256": sha256_file(trade_path)}
        )
    if selected != expected_days:
        raise ValueError("current 2025 provider selection differs from frozen P3 manifest")

    checks = {
        "quality_identity_sha256": _identity_rows_sha256(quality_rows),
        "bbo_identity_sha256": _identity_rows_sha256(bbo_rows),
        "trade_identity_sha256": _identity_rows_sha256(trade_rows),
        "combined_input_identity_sha256": _identity_rows_sha256(
            [*bbo_rows, *trade_rows]
        ),
    }
    for key, observed in checks.items():
        expected = str(source[key])
        if observed != expected:
            raise ValueError(
                f"2025 provider {key} mismatch: observed={observed} expected={expected}"
            )
    by_day = {
        row["day"]: row
        for row in [*bbo_rows, *trade_rows]
    }
    del by_day  # The full rows are returned; this assignment documents uniqueness.
    return [*bbo_rows, *trade_rows], {
        "bbo": bbo_root,
        "trade": trade_root,
    }


def _verify_2026_inputs(
    spec: Mapping[str, Any],
    day_manifest: Mapping[str, Any],
) -> tuple[list[dict[str, str]], dict[str, Path]]:
    report_path = resolve_portable_path(
        str(spec["identities"]["current_v2_report"]["path"])
    ).resolve()
    report = json.loads(report_path.read_text(encoding="utf-8"))
    source = day_manifest["source_identities"]["current_2026_reference"]
    bbo_root = resolve_portable_path(str(source["bbo_root"])).resolve()
    trade_root = resolve_portable_path(str(source["trade_root"])).resolve()
    frozen_hashes = {
        Path(str(row["path"])).name: str(row["sha256"])
        for row in report["inputs"]
    }
    panels = day_manifest["panels"]
    all_days = [
        *panels["fit_2026_current"],
        *panels["historical_2026_validation"],
        *panels["historical_2026_test_diagnostic"],
    ]
    rows: list[dict[str, str]] = []
    for day in all_days:
        for kind, path in (
            ("bbo", bbo_root / f"BTCUSDC-bbo-{day}.parquet"),
            ("aggTrades", trade_root / f"BTCUSDC-aggTrades-{day}.csv"),
        ):
            if not path.is_file():
                raise FileNotFoundError(f"missing current P3 input for {day}: {path}")
            observed = sha256_file(path)
            expected = frozen_hashes.get(path.name)
            if observed != expected:
                raise ValueError(
                    f"current P3 input hash mismatch for {path.name}: "
                    f"observed={observed} expected={expected}"
                )
            rows.append({"day": day, "kind": kind, "sha256": observed})
    return rows, {"bbo": bbo_root, "trade": trade_root}


def reach_cache_key(
    *,
    day: str,
    bbo_sha256: str,
    trade_sha256: str,
    horizon_s: float,
    max_bbo_age_ms: int,
    estimator_sha256: str,
) -> str:
    return canonical_sha256(
        {
            "schema_version": "narrowgate_p3_touch_reaches_cache.v1",
            "day": day,
            "bbo_sha256": bbo_sha256,
            "trade_sha256": trade_sha256,
            "horizon_s": float(horizon_s),
            "max_bbo_age_ms": int(max_bbo_age_ms),
            "estimator_sha256": estimator_sha256,
        }
    )


def _atomic_npz(path: Path, payload: Mapping[str, np.ndarray]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="wb", suffix=".npz", dir=path.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        np.savez_compressed(handle, **payload)
    os.replace(temporary, path)


def _day_reach_task(payload: Mapping[str, Any]) -> dict[str, Any]:
    day = str(payload["day"])
    cache_path = resolve_portable_path(str(payload["cache_path"]), root=ROOT).resolve()
    expected_key = str(payload["cache_key"])
    if cache_path.is_file():
        with np.load(cache_path, allow_pickle=False) as cached:
            cached_key = str(cached["cache_key"].item())
            if cached_key == expected_key:
                return {
                    "day": day,
                    "BUY": cached["BUY"].astype(np.float64, copy=True),
                    "SELL": cached["SELL"].astype(np.float64, copy=True),
                    "book_age_ms": cached["book_age_ms"].astype(
                        np.float64, copy=True
                    ),
                    "cache_hit": True,
                    "cache_path": str(cache_path),
                    "cache_key": expected_key,
                }

    reach = window_reaches(
        day=day,
        bbo_path=resolve_portable_path(str(payload["bbo_path"]), root=ROOT),
        trade_path=resolve_portable_path(str(payload["trade_path"]), root=ROOT),
        horizon_s=float(payload["horizon_s"]),
        max_bbo_age_ms=int(payload["max_bbo_age_ms"]),
    )
    _atomic_npz(
        cache_path,
        {
            "cache_key": np.asarray(expected_key),
            "BUY": np.asarray(reach["BUY"], dtype=np.float64),
            "SELL": np.asarray(reach["SELL"], dtype=np.float64),
            "book_age_ms": np.asarray(reach["book_age_ms"], dtype=np.float64),
        },
    )
    return {
        "day": day,
        "BUY": reach["BUY"],
        "SELL": reach["SELL"],
        "book_age_ms": reach["book_age_ms"],
        "cache_hit": False,
        "cache_path": str(cache_path),
        "cache_key": expected_key,
    }


def empirical_curve(reaches: np.ndarray, grid: np.ndarray) -> np.ndarray:
    values = np.sort(np.asarray(reaches, dtype=np.float64))
    if values.size == 0:
        return np.zeros_like(grid, dtype=np.float64)
    first = np.searchsorted(values, grid, side="left")
    curve = (values.size - first).astype(np.float64) / float(values.size)
    return np.minimum.accumulate(np.clip(curve, 0.0, 1.0))


def integrated_brier(
    reaches: np.ndarray,
    probability_grid: np.ndarray,
    distance_grid: np.ndarray,
) -> float:
    """Uniform-grid integrated Brier without materializing N x D labels."""
    values = np.asarray(reaches, dtype=np.float64)
    probabilities = np.asarray(probability_grid, dtype=np.float64)
    grid = np.asarray(distance_grid, dtype=np.float64)
    if values.size == 0:
        raise ValueError("integrated Brier requires at least one opportunity")
    if probabilities.shape != grid.shape:
        raise ValueError("probability and distance grids must have equal shape")
    if np.any(np.diff(grid) <= 0.0):
        raise ValueError("distance grid must be strictly increasing")
    if np.any((probabilities < 0.0) | (probabilities > 1.0)):
        raise ValueError("probabilities must lie in [0, 1]")

    touched_count = np.searchsorted(grid, values, side="right")
    one_loss_prefix = np.concatenate(
        ([0.0], np.cumsum(np.square(1.0 - probabilities)))
    )
    zero_loss_prefix = np.concatenate(
        ([0.0], np.cumsum(np.square(probabilities)))
    )
    losses = one_loss_prefix[touched_count]
    losses += zero_loss_prefix[-1] - zero_loss_prefix[touched_count]
    return float(np.mean(losses) / float(grid.size))


def _curve_identity(
    values: np.ndarray,
    grid: np.ndarray,
    *,
    metadata: Mapping[str, Any],
) -> tuple[np.ndarray, dict[str, Any]]:
    curve = empirical_curve(values, grid)
    model = FillProbabilityModel(
        model_type="empirical_survival",
        delta_grid=grid.tolist(),
        probability_grid=curve.tolist(),
        schema_version=MODEL_SCHEMA_VERSION,
        metadata=dict(metadata),
    )
    delta_star = model.optimal_delta(delta_max=float(grid[-1]))
    return curve, {
        "windows": int(values.size),
        "touch_at_best_rate": float(np.mean(values >= 0.0)),
        "delta_star": float(delta_star),
        "kappa_eff": float(model.effective_kappa(delta_star)),
        "probability_at_delta_star": float(model.prob(delta_star)),
    }


def _bootstrap_delta(
    values: np.ndarray,
    *,
    draws: int,
    seed: int,
) -> dict[str, Any]:
    values = np.asarray(values, dtype=np.float64)
    if values.ndim != 1 or values.size == 0:
        raise ValueError("paired day bootstrap needs a non-empty vector")
    rng = np.random.default_rng(seed)
    sampled = rng.choice(values, size=(draws, values.size), replace=True).mean(axis=1)
    return {
        "days": int(values.size),
        "mean_delta": float(np.mean(values)),
        "median_delta": float(np.median(values)),
        "improved_day_rate": float(np.mean(values < 0.0)),
        "ci95_day_cluster_bootstrap": [
            float(np.quantile(sampled, 0.025)),
            float(np.quantile(sampled, 0.975)),
        ],
    }


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
        temporary = Path(handle.name)
    os.replace(temporary, path)


def _write_model(
    *,
    path: Path,
    curve: np.ndarray,
    grid: np.ndarray,
    metadata: Mapping[str, Any],
) -> dict[str, Any]:
    model = FillProbabilityModel(
        model_type="empirical_survival",
        delta_grid=grid.tolist(),
        probability_grid=curve.tolist(),
        schema_version=MODEL_SCHEMA_VERSION,
        metadata=dict(metadata),
    )
    model.save(path)
    delta_star = model.optimal_delta(delta_max=float(grid[-1]))
    return {
        "path": str(path.resolve()),
        "sha256": sha256_file(path),
        "delta_star": float(delta_star),
        "kappa_eff": float(model.effective_kappa(delta_star)),
        "probability_at_delta_star": float(model.prob(delta_star)),
    }


def run_audit(args: argparse.Namespace) -> dict[str, Any]:
    spec_path = args.spec.expanduser().resolve()
    spec, day_manifest = load_contract(spec_path)
    output_dir = args.output_dir.expanduser().resolve()
    cache_dir = args.cache_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    if any(output_dir.iterdir()):
        raise FileExistsError(f"P3 v3 output directory must be empty: {output_dir}")

    rows_2025, roots_2025 = _verify_2025_selection(day_manifest)
    rows_2026, roots_2026 = _verify_2026_inputs(spec, day_manifest)
    hash_lookup: dict[tuple[str, str], str] = {}
    for row in [*rows_2025, *rows_2026]:
        key = (str(row["day"]), str(row["kind"]))
        if key in hash_lookup:
            raise ValueError(f"duplicate P3 input identity: {key}")
        hash_lookup[key] = str(row["sha256"])

    grid_contract = spec["estimand"]["distance_grid"]
    grid = np.arange(
        float(grid_contract["minimum"]),
        float(grid_contract["maximum"])
        + 0.5 * float(grid_contract["step"]),
        float(grid_contract["step"]),
        dtype=np.float64,
    )
    horizon_s = float(spec["estimand"]["horizon_s"])
    max_bbo_age_ms = int(spec["estimand"]["max_bbo_age_ms"])
    estimator_sha = str(spec["identities"]["frozen_v2_estimator"]["sha256"])
    panel_results: dict[str, dict[str, dict[str, Any]]] = {}
    cache_rows: list[dict[str, Any]] = []

    tasks: list[dict[str, Any]] = []
    for panel, days in day_manifest["panels"].items():
        roots = roots_2025 if panel == "fit_2025_provider" else roots_2026
        panel_results[panel] = {}
        for day in days:
            bbo_path = roots["bbo"] / f"BTCUSDC-bbo-{day}.parquet"
            trade_path = roots["trade"] / f"BTCUSDC-aggTrades-{day}.csv"
            bbo_sha = hash_lookup[(day, "bbo")]
            trade_sha = hash_lookup[(day, "aggTrades")]
            key = reach_cache_key(
                day=day,
                bbo_sha256=bbo_sha,
                trade_sha256=trade_sha,
                horizon_s=horizon_s,
                max_bbo_age_ms=max_bbo_age_ms,
                estimator_sha256=estimator_sha,
            )
            tasks.append(
                {
                    "panel": panel,
                    "day": day,
                    "bbo_path": str(bbo_path),
                    "trade_path": str(trade_path),
                    "horizon_s": horizon_s,
                    "max_bbo_age_ms": max_bbo_age_ms,
                    "cache_key": key,
                    "cache_path": str(cache_dir / f"BTCUSDC-{day}-{key}.npz"),
                }
            )

    by_task = {(task["panel"], task["day"]): task for task in tasks}
    with concurrent.futures.ProcessPoolExecutor(max_workers=int(args.workers)) as pool:
        future_map = {
            pool.submit(_day_reach_task, task): (task["panel"], task["day"])
            for task in tasks
        }
        completed = 0
        for future in concurrent.futures.as_completed(future_map):
            panel, day = future_map[future]
            result = future.result()
            panel_results[panel][day] = result
            cache_rows.append(
                {
                    "panel": panel,
                    "day": day,
                    "cache_hit": bool(result["cache_hit"]),
                    "cache_path": result["cache_path"],
                    "cache_key": result["cache_key"],
                }
            )
            completed += 1
            if completed % 10 == 0 or completed == len(tasks):
                print(f"P3 reaches: {completed}/{len(tasks)} days", flush=True)

    for panel, days in day_manifest["panels"].items():
        if sorted(panel_results[panel]) != list(days):
            raise RuntimeError(f"P3 panel assembly mismatch: {panel}")

    def values_for(panels: Sequence[str], side: str | None = None) -> np.ndarray:
        chunks: list[np.ndarray] = []
        for panel in panels:
            for day in day_manifest["panels"][panel]:
                if side is None:
                    chunks.extend(
                        [panel_results[panel][day][s] for s in SIDES]
                    )
                else:
                    chunks.append(panel_results[panel][day][side])
        return np.concatenate(chunks)

    fit_panels = {
        name: list(contract["panels"])
        for name, contract in spec["fit_models"].items()
    }
    model_curves: dict[str, np.ndarray] = {}
    curve_summary: dict[str, Any] = {}
    for model_name, panels in fit_panels.items():
        metadata = {
            "identity": spec["identity"],
            "fit_model": model_name,
            "fit_panels": panels,
            "fit_days": [
                day for panel in panels for day in day_manifest["panels"][panel]
            ],
            "event_type": "touch",
            "horizon_s": horizon_s,
            "distance_unit": "USDC_per_BTC",
            "distance_origin": spec["estimand"]["distance_origin"],
            "touch_source": "side-correct Binance aggTrades against causal last-known source-specific BBO",
            "queue_included": False,
            "source_aware": True,
            "spec_path": str(spec_path),
            "spec_sha256": sha256_file(spec_path),
            "day_manifest_sha256": sha256_file(
                resolve_research_path(spec["identities"]["day_manifest"]["path"])
            ),
            "prediction_authority": False,
            "action_authority": False,
            "live_authority": False,
        }
        pooled = values_for(panels)
        pooled_curve, pooled_identity = _curve_identity(
            pooled, grid, metadata=metadata
        )
        model_curves[model_name] = pooled_curve
        side_summary: dict[str, Any] = {}
        for side in SIDES:
            _, side_summary[side] = _curve_identity(
                values_for(panels, side),
                grid,
                metadata={**metadata, "side": side},
            )
        curve_summary[model_name] = {
            "panels": panels,
            "pooled": pooled_identity,
            "by_side": side_summary,
        }

    current_model = FillProbabilityModel.load(
        resolve_portable_path(spec["identities"]["current_v2_artifact"]["path"])
    )
    current_grid = np.asarray(current_model.delta_grid, dtype=np.float64)
    if not np.array_equal(current_grid, grid):
        raise ValueError("current P3 v2 grid differs from frozen v3 grid")
    model_curves["current_v2"] = np.asarray(
        current_model.probability_grid, dtype=np.float64
    )
    reproduction_max_abs = float(
        np.max(np.abs(model_curves["2026_train"] - model_curves["current_v2"]))
    )
    reproduction_passed = reproduction_max_abs <= float(
        spec["evaluation"]["current_reproduction_gate"][
            "max_abs_probability_difference"
        ]
    )
    if not reproduction_passed:
        raise RuntimeError(
            "current 2026 curve failed frozen v2 reproduction: "
            f"max_abs={reproduction_max_abs}"
        )

    model_artifacts: dict[str, Any] = {}
    for model_name in FIT_MODELS:
        panels = fit_panels[model_name]
        model_artifacts[model_name] = _write_model(
            path=output_dir / f"p3_touch_10s_{model_name}_params.json",
            curve=model_curves[model_name],
            grid=grid,
            metadata={
                "identity": spec["identity"],
                "fit_model": model_name,
                "fit_panels": panels,
                "fit_days": [
                    day
                    for panel in panels
                    for day in day_manifest["panels"][panel]
                ],
                "event_type": "touch",
                "horizon_s": horizon_s,
                "distance_unit": "USDC_per_BTC",
                "distance_origin": spec["estimand"]["distance_origin"],
                "touch_source": "side-correct Binance aggTrades against causal last-known source-specific BBO",
                "queue_included": False,
                "source_aware": True,
                "spec_sha256": sha256_file(spec_path),
                "day_manifest_sha256": str(
                    spec["identities"]["day_manifest"]["sha256"]
                ),
                "prediction_authority": False,
                "action_authority": False,
                "live_authority": False,
            },
        )

    diagnostic_curves: dict[str, Any] = {}
    for panel in spec["evaluation"]["panel_empirical_refits"]["panels"]:
        pooled_curve, pooled_identity = _curve_identity(
            values_for([panel]),
            grid,
            metadata={"event_type": "touch", "horizon_s": horizon_s},
        )
        by_side: dict[str, Any] = {}
        for side in SIDES:
            _, by_side[side] = _curve_identity(
                values_for([panel], side),
                grid,
                metadata={
                    "event_type": "touch",
                    "horizon_s": horizon_s,
                    "side": side,
                },
            )
        diagnostic_curves[panel] = {
            "purpose": "drift_diagnostic_only",
            "pooled": pooled_identity,
            "by_side": by_side,
            "probability_grid": pooled_curve.tolist(),
        }

    daily_rows: list[dict[str, Any]] = []
    calibration_rows: list[dict[str, Any]] = []
    evaluation_panels = list(spec["evaluation"]["historical_panels"])
    for panel in evaluation_panels:
        panel_observed: dict[str, np.ndarray] = {}
        for side in SIDES:
            panel_values = values_for([panel], side)
            observed_curve = empirical_curve(panel_values, grid)
            panel_observed[side] = observed_curve
            for model_name in EVALUATION_MODELS:
                probabilities = model_curves[model_name]
                brier_at_distance = (
                    observed_curve * np.square(1.0 - probabilities)
                    + (1.0 - observed_curve) * np.square(probabilities)
                )
                calibration_rows.extend(
                    {
                        "panel": panel,
                        "side": side,
                        "model": model_name,
                        "distance_usdc_per_btc": float(distance),
                        "predicted_probability": float(predicted),
                        "observed_probability": float(observed),
                        "prediction_minus_observation": float(predicted - observed),
                        "brier": float(brier),
                    }
                    for distance, predicted, observed, brier in zip(
                        grid,
                        probabilities,
                        observed_curve,
                        brier_at_distance,
                        strict=True,
                    )
                )
        for day in day_manifest["panels"][panel]:
            for side in SIDES:
                reaches = panel_results[panel][day][side]
                for model_name in EVALUATION_MODELS:
                    daily_rows.append(
                        {
                            "panel": panel,
                            "day": day,
                            "side": side,
                            "model": model_name,
                            "windows": int(reaches.size),
                            "integrated_brier": integrated_brier(
                                reaches, model_curves[model_name], grid
                            ),
                        }
                    )

    daily = pd.DataFrame(daily_rows)
    daily["delta_vs_current_v2"] = np.nan
    current_lookup = daily.loc[
        daily["model"].eq("current_v2"),
        ["panel", "day", "side", "integrated_brier"],
    ].rename(columns={"integrated_brier": "current_brier"})
    daily = daily.merge(current_lookup, on=["panel", "day", "side"], how="left")
    daily["delta_vs_current_v2"] = (
        daily["integrated_brier"] - daily["current_brier"]
    )
    daily.drop(columns=["current_brier"], inplace=True)

    bootstrap_contract = spec["evaluation"]["bootstrap"]
    comparisons: dict[str, Any] = {}
    side_means_nonpositive = True
    pooled_upper_negative = True
    for panel_index, panel in enumerate(evaluation_panels):
        comparisons[panel] = {"by_side": {}}
        expanded = daily[
            daily["panel"].eq(panel) & daily["model"].eq("expanded")
        ]
        for side_index, side in enumerate(SIDES):
            values = expanded.loc[
                expanded["side"].eq(side), "delta_vs_current_v2"
            ].to_numpy(dtype=np.float64)
            evidence = _bootstrap_delta(
                values,
                draws=int(bootstrap_contract["draws"]),
                seed=int(bootstrap_contract["seed"]) + 10 * panel_index + side_index,
            )
            comparisons[panel]["by_side"][side] = evidence
            side_means_nonpositive &= evidence["mean_delta"] <= 0.0
        pooled = (
            expanded.groupby("day", sort=True)["delta_vs_current_v2"]
            .mean()
            .to_numpy(dtype=np.float64)
        )
        pooled_evidence = _bootstrap_delta(
            pooled,
            draws=int(bootstrap_contract["draws"]),
            seed=int(bootstrap_contract["seed"]) + 100 + panel_index,
        )
        comparisons[panel]["pooled"] = pooled_evidence
        pooled_upper_negative &= (
            pooled_evidence["ci95_day_cluster_bootstrap"][1] < 0.0
        )

    calibration_gate = {
        "current_2026_reproduction": reproduction_passed,
        "all_historical_panel_side_mean_brier_deltas_lte_zero": bool(
            side_means_nonpositive
        ),
        "each_historical_panel_pooled_day_cluster_ci95_upper_lt_zero": bool(
            pooled_upper_negative
        ),
    }
    calibration_gate["all_passed"] = all(calibration_gate.values())

    daily_path = output_dir / "daily_proper_scores.csv"
    calibration_path = output_dir / "calibration_by_distance.parquet"
    cache_path = output_dir / "cache_usage.csv"
    daily.to_csv(daily_path, index=False)
    pd.DataFrame(calibration_rows).to_parquet(calibration_path, index=False)
    pd.DataFrame(cache_rows).sort_values(["panel", "day"]).to_csv(
        cache_path, index=False
    )

    input_manifest = {
        "schema_version": "narrowgate_p3_touch_source_aware_input_manifest.v3",
        "2025_provider": {
            "rows": rows_2025,
            "identity_sha256": _identity_rows_sha256(rows_2025),
        },
        "2026_current": {
            "rows": rows_2026,
            "identity_sha256": _identity_rows_sha256(rows_2026),
        },
    }
    input_manifest_path = output_dir / "input_manifest.json"
    _atomic_json(input_manifest_path, input_manifest)

    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "identity": spec["identity"],
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "spec": {"path": str(spec_path), "sha256": sha256_file(spec_path)},
        "day_manifest": spec["identities"]["day_manifest"],
        "implementation": {
            "path": str(Path(__file__).resolve()),
            "sha256": sha256_file(Path(__file__).resolve()),
            "frozen_v2_estimator_sha256": estimator_sha,
        },
        "estimand": spec["estimand"],
        "panel_day_counts": {
            name: len(days) for name, days in day_manifest["panels"].items()
        },
        "model_artifacts": model_artifacts,
        "curve_summary": curve_summary,
        "panel_empirical_drift_diagnostic": diagnostic_curves,
        "current_reproduction": {
            "max_abs_probability_difference": reproduction_max_abs,
            "passed": reproduction_passed,
        },
        "historical_transport_integrated_brier": comparisons,
        "calibration_gate_before_quote_path": calibration_gate,
        "quote_path": {
            "status": "required_not_yet_run",
            "arms": spec["quote_path_comparison"]["arms"],
            "calibration_contract_may_change_after_quote_path": False,
        },
        "decision": "pending_quote_path_comparison"
        if calibration_gate["all_passed"]
        else "do_not_replace_current_static_p3_from_expanded_v3",
        "permissions": spec["permissions"],
        "outputs": {
            "daily_proper_scores": str(daily_path),
            "calibration_by_distance": str(calibration_path),
            "cache_usage": str(cache_path),
            "input_manifest": str(input_manifest_path),
        },
    }
    report_path = output_dir / "report.json"
    _atomic_json(report_path, report)

    manifest_entries: dict[str, Any] = {}
    for path in sorted(output_dir.iterdir()):
        if path.is_file() and path.name != "manifest.json":
            manifest_entries[path.name] = {
                "size_bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
    output_manifest = {
        "schema_version": "narrowgate_p3_touch_source_aware_output_manifest.v3",
        "identity": spec["identity"],
        "created_at_utc": report["created_at_utc"],
        "spec_sha256": sha256_file(spec_path),
        "implementation_sha256": report["implementation"]["sha256"],
        "files": manifest_entries,
        "permissions": spec["permissions"],
    }
    _atomic_json(output_dir / "manifest.json", output_manifest)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--spec",
        type=Path,
        default=ROOT
        / "research/families/f02_empirical_p3_touch/docs/"
        "p3_touch_source_aware_expanded_v3_spec_20260803.json",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path.home()
        / "Library/Caches/NarrowGate_BTCUSDC/p3_touch_reaches_v1",
    )
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    if int(args.workers) <= 0:
        parser.error("--workers must be positive")
    report = run_audit(args)
    print(
        json.dumps(
            {
                "identity": report["identity"],
                "decision": report["decision"],
                "calibration_gate": report[
                    "calibration_gate_before_quote_path"
                ],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
