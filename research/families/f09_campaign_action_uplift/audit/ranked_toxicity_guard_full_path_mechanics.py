#!/usr/bin/env python3
"""Run the frozen 40-day ranked-toxicity full-path mechanics audit.

The driver performs two independent authoritative Python replays per UTC day.
The first pass records the untreated quote denominator and held v12 toxicity
score.  Strictly past-only side-specific p90 thresholds are then frozen before
the second pass regenerates the candidate order, queue, fill, and inventory
path.  This module reads mechanics only; economic result fields are forbidden.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import math
import os
import shutil
import tempfile
import time
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from data_paths import relocate_marketdata_path
from execution.chunked_parquet_journal import iter_chunked_parquet_journal
from research.families.f09_campaign_action_uplift.audit.ranked_toxicity_guard_authoritative_replay_v1_5 import (
    RankedToxicityBaselineShadowCaptureV15,
    RankedToxicityGuardAuthoritativeReplayV15,
    RankedToxicityThresholdUnreadyReplayV15,
    baseline_opportunities_from_manifests,
    build_past_only_threshold_schedule_v15,
)

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = "ranked_toxicity_guard_full_path_mechanics.v1"
SIDES = ("BUY", "SELL")
FORBIDDEN_ECONOMIC_KEYS = frozenset(
    {
        "pnl",
        "terminal_mtm_pnl",
        "inventory_adjusted_pnl",
        "reward",
        "markout",
        "campaign_tail",
        "terminal_value",
        "toxic_fill",
    }
)


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


def canonical_spec_sha256(payload: Mapping[str, Any]) -> str:
    normalized = dict(payload)
    normalized.pop("canonical_spec_identity_sha256", None)
    return canonical_sha256(normalized)


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
        temporary = Path(handle.name)
    os.replace(temporary, path)


def _atomic_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False, newline=""
    ) as handle:
        frame.to_csv(handle, index=False)
        temporary = Path(handle.name)
    os.replace(temporary, path)


def _repo_path(value: str | Path) -> Path:
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (ROOT / path).resolve()


def _data_path(value: str | Path) -> Path:
    return relocate_marketdata_path(Path(value).expanduser()).resolve()


def _require_identity(identity: Mapping[str, Any], label: str) -> Path:
    path = _repo_path(str(identity["path"]))
    if not path.is_file():
        raise FileNotFoundError(f"{label} is missing: {path}")
    observed = sha256_file(path)
    expected = str(identity["sha256"])
    if observed != expected:
        raise ValueError(
            f"{label} SHA256 mismatch: observed={observed} expected={expected}"
        )
    return path


def load_spec(path: Path) -> dict[str, Any]:
    spec = json.loads(path.read_text(encoding="utf-8"))
    if spec.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported ranked-toxicity mechanics spec")
    if canonical_spec_sha256(spec) != spec.get("canonical_spec_identity_sha256"):
        raise ValueError("ranked-toxicity mechanics canonical spec hash mismatch")
    if sha256_file(Path(__file__).resolve()) != spec.get("implementation_sha256"):
        raise ValueError("ranked-toxicity mechanics implementation hash mismatch")

    days = [str(day) for day in spec["panels"]["development_days"]]
    if days != sorted(set(days)) or len(days) != 40:
        raise ValueError("Development panel must contain 40 unique chronological days")
    grade_a = set(str(day) for day in spec["panels"]["grade_a_days"])
    grade_b = set(str(day) for day in spec["panels"]["grade_b_days"])
    if grade_a & grade_b or grade_a | grade_b != set(days):
        raise ValueError("Grade-A/Grade-B partition does not cover Development")

    permissions = spec.get("permissions") or {}
    if permissions.get("mechanics_execution_allowed") is not True:
        raise ValueError("mechanics execution is not authorized by the frozen spec")
    for forbidden in (
        "economic_outcomes_read",
        "validation_read",
        "sealed_holdout_read",
        "action_experiment_authorized",
        "live_deployment_authorized",
    ):
        if bool(permissions.get(forbidden, False)):
            raise ValueError(f"mechanics spec cannot grant {forbidden}")
    if spec.get("economic_outcome_columns_read") != []:
        raise ValueError("mechanics spec names economic outcome columns")

    for label, identity in spec.get("artifact_identities", {}).items():
        _require_identity(identity, label)
    for label, identity in spec.get("implementation_identities", {}).items():
        _require_identity(identity, label)

    feature_dir = _data_path(spec["data_identity"]["feature_dir"])
    feature_manifest = feature_dir / "causal_feature_manifest.json"
    expected_manifest = str(spec["data_identity"]["feature_manifest_sha256"])
    if sha256_file(feature_manifest) != expected_manifest:
        raise ValueError("40-day feature manifest SHA256 mismatch")
    for day in days:
        feature_path = feature_dir / f"features_{day}.parquet"
        if not feature_path.is_file():
            raise FileNotFoundError(f"missing Development feature day: {feature_path}")
    return spec


def storage_gate(spec: Mapping[str, Any]) -> dict[str, Any]:
    gate = spec["storage_gate"]
    cache_root = Path(str(spec["storage"]["cache_root"])).expanduser().resolve()
    evidence_root = _data_path(spec["storage"]["evidence_root"])
    cache_root.mkdir(parents=True, exist_ok=True)
    evidence_root.mkdir(parents=True, exist_ok=True)
    checks: dict[str, Any] = {}
    for label, root in (("cache", cache_root), ("evidence", evidence_root)):
        usage = shutil.disk_usage(root)
        free_gib = float(usage.free) / (1024.0**3)
        estimate = float(gate[f"estimated_{label}_output_gib"])
        required = max(
            float(gate["absolute_minimum_free_gib"]),
            float(gate["reserve_free_gib"])
            + float(gate["output_multiple"]) * estimate,
        )
        if free_gib < required:
            raise RuntimeError(
                f"{label} storage gate failed: free={free_gib:.2f} GiB "
                f"required={required:.2f} GiB"
            )
        checks[label] = {
            "path": str(root),
            "free_gib_before": free_gib,
            "estimated_output_gib": estimate,
            "required_free_gib": required,
            "passed": True,
        }
    return checks


def _resolved_source(spec: Mapping[str, Any], key: str) -> Path:
    return _data_path(spec["data_identity"][key])


def _configure_params(spec: Mapping[str, Any], day: str) -> dict[str, Any]:
    from models import backtest_tick as bt
    from models.backtest_config import (
        load_tick_base_params,
        validate_formal_replay_calibration,
    )
    from models.replay_contract import configure_fixed_latency_distribution

    config_path = _repo_path(spec["baseline"]["config_path"])
    queue_path = _resolved_source(spec, "queue_calibration_path")
    model_dir = _repo_path(spec["model_identity"]["model_dir"])
    params = load_tick_base_params(
        symbol="BTCUSDC",
        config_path=config_path,
        configure_symbol=bt.configure_symbol,
        require_historical_bbo=True,
        queue_calibration_path=queue_path,
        strict_calibration=True,
    )
    params.update(
        {
            "execution_trade_source": "trades",
            "market_context_warmup_days": 1,
            "replay_event_clock": "merged",
            "replay_clock_interval_ms": 100,
            "exchange_book_queue_mode": "strict",
            "queue_ahead_mode": "exact_level",
            "queue_l2_cancel_ahead_enabled": False,
            "collect_curves": False,
            "trace_decisions_max": 0,
            "trace_quotes_max": 0,
            "trace_fills_max": 0,
            "decision_trace_profile": "mechanics_only",
            "rng_seed": int(spec["replay_contract"]["rng_seed"]),
            "sync_adjust_replay_mode": "stress",
            "sync_adjust_stress_seed": int(
                spec["replay_contract"]["sync_stress_seed"]
            ),
            "sync_adjust_stress_interval_s": float(
                spec["replay_contract"]["sync_stress_interval_s"]
            ),
            "replay_purpose": "ranked_toxicity_mechanics_only",
            "replay_initial_state_mode": "fresh_start",
            "replay_promotion_eligible": False,
            "initial_inventory": 0.0,
            "initial_entry_price": 0.0,
            "dynamic_fill_hazard_shadow_enabled": True,
            "dynamic_fill_hazard_action_enabled": False,
            "dynamic_fill_hazard_cpp_parity_enabled": False,
            "buy_fill_selection_live_enabled": True,
            "ml_enabled": True,
            "model_dir": str(model_dir),
            "resolved_model_dir": str(model_dir),
            "markout_side_asymmetry_sign": 1.0,
            "window_cache_write_enabled": True,
            "legacy_monolithic_window_cache_write_enabled": False,
            "_formal_quality_allowed_days": [
                (date.fromisoformat(day) - timedelta(days=1)).isoformat(),
                day,
            ],
        }
    )
    trade = spec["data_identity"]["execution_trades"]
    params.update(
        {
            "individual_trades_manifest_path": str(
                _data_path(trade["manifest_path"])
            ),
            "individual_trades_manifest_sha256": str(trade["manifest_sha256"]),
            "individual_trades_integrity_report_path": str(
                _data_path(trade["quality_path"])
            ),
            "individual_trades_integrity_report_sha256": str(
                trade["quality_sha256"]
            ),
        }
    )
    latency = spec["data_identity"]["latency"]
    samples = bt._load_live_perf_latency_samples(
        _data_path(latency["path"]),
        mode=str(latency["mode"]),
    )
    params["_new_order_latency_samples_ms"] = samples[
        "new_order_latency_samples_ms"
    ]
    params["_cancel_order_latency_samples_ms"] = samples[
        "cancel_order_latency_samples_ms"
    ]
    configure_fixed_latency_distribution(
        params,
        scenario="baseline",
        profile_id=str(latency["profile_id"]),
        environment=str(latency["environment"]),
        baseline_clip_quantile=float(latency["baseline_clip_quantile"]),
    )
    validate_formal_replay_calibration(params, require_latency=True)
    expected = spec["baseline"]["required_projection"]
    actual = {
        "ml_enabled": bool(params.get("ml_enabled", False)),
        "q90_shadow_enabled": bool(
            params.get("dynamic_fill_hazard_shadow_enabled", False)
        ),
        "q90_action_enabled": bool(
            params.get("dynamic_fill_hazard_action_enabled", False)
        ),
        "buy_fill_selection_enabled": bool(
            params.get("buy_fill_selection_live_enabled", False)
        ),
    }
    if actual != expected:
        raise ValueError(f"operational baseline projection drifted: {actual}")
    return params


def _load_window(spec: Mapping[str, Any], day: str, params: dict[str, Any]):
    from models import backtest_tick as bt
    from models.data_windows import load_tick_window

    normalized_root = _resolved_source(spec, "normalized_l2_root")
    feature_dir = _resolved_source(spec, "feature_dir")
    model_dir = _repo_path(spec["model_identity"]["model_dir"])
    bt.configure_symbol("BTCUSDC", model_dir_override=model_dir)
    bt.BBO_DIR = normalized_root / "bbo"
    bt.L2_DIR = normalized_root / "l2"
    return load_tick_window(
        day,
        params,
        load_ml=True,
        require_ml=True,
        run_ml_inference=True,
        feature_dir=feature_dir,
        require_target_feature_files=True,
        cross_market_enabled=True,
        with_ml_cache=False,
        require_historical_bbo=True,
        require_formal_l2=False,
        cache_dir=Path(str(spec["storage"]["window_cache_dir"])).expanduser(),
        refresh_cache=False,
    )


def _exchange_tape(spec: Mapping[str, Any], day: str, params: Mapping[str, Any]):
    from models.exchange_book_replay import CryptoHFTExchangeBookTape

    return CryptoHFTExchangeBookTape(
        raw_root=_resolved_source(spec, "native_orderbook_root"),
        day=day,
        symbol="BTCUSDC",
        tick_size=float(spec["symbol_rules"]["tick_size"]),
        warmup_hours=int(spec["replay_contract"]["native_warmup_hours"]),
        strict_complete=True,
    )


def _simulate_with_binding(
    spec: Mapping[str, Any],
    day: str,
    binding: Any,
) -> dict[str, Any]:
    from models import backtest_tick as bt

    params = _configure_params(spec, day)
    window = _load_window(spec, day, params)
    result = bt._simulate_tick_with_engine(
        "python",
        window.trades,
        window.var_ts_ms,
        window.var_ssq,
        params,
        ml_data=window.ml_data,
        bbo_data=window.bbo_data,
        l2_data=window.l2_data,
        var_ti=window.var_ti,
        var_retsq=window.var_retsq,
        reference_event_tapes=window.reference_event_tapes,
        campaign_repair_data=window.campaign_repair_data,
        campaign_repair_model=window.campaign_repair_model,
        historical_global_flow_data=window.historical_global_flow_data,
        exchange_book_event_tape=_exchange_tape(spec, day, params),
        ranked_toxicity_guard_binding=binding,
    )
    audit = result.get("ranked_toxicity_guard_binding_audit")
    if not isinstance(audit, Mapping):
        raise RuntimeError("authoritative replay omitted guard binding audit")
    return dict(audit)


def _day_cache_root(spec: Mapping[str, Any], day: str) -> Path:
    return (
        Path(str(spec["storage"]["cache_root"])).expanduser().resolve()
        / str(spec["canonical_spec_identity_sha256"])
        / day
    )


def _run_baseline_day(payload: Mapping[str, Any]) -> dict[str, Any]:
    spec = dict(payload["spec"])
    day = str(payload["day"])
    started = time.monotonic()
    root = _day_cache_root(spec, day)
    output_dir = root / "baseline_v1_5"
    descriptor_path = root / "baseline_descriptor.json"
    if descriptor_path.is_file():
        descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
        manifest = Path(descriptor["manifest_path"])
        if (
            descriptor.get("spec_sha256") == spec["canonical_spec_identity_sha256"]
            and manifest.is_file()
            and sha256_file(manifest) == descriptor.get("manifest_sha256")
        ):
            return descriptor
        raise RuntimeError(f"stale baseline descriptor for {day}")
    if output_dir.exists():
        raise RuntimeError(f"unadmitted baseline output already exists: {output_dir}")
    binding = RankedToxicityBaselineShadowCaptureV15(
        output_dir=output_dir,
        lineage_namespace=f"{spec['family_id']}|{day}",
        sides=SIDES,
        chunk_rows=int(spec["replay_contract"]["journal_chunk_rows"]),
    )
    audit = _simulate_with_binding(spec, day, binding)
    manifest = output_dir / "manifest.json"
    if not manifest.is_file() or not bool(audit["journal_manifest"].get("closed")):
        raise RuntimeError(f"baseline journal was not atomically closed for {day}")
    descriptor = {
        "schema_version": f"{SCHEMA_VERSION}.baseline_day_descriptor",
        "day": day,
        "spec_sha256": spec["canonical_spec_identity_sha256"],
        "manifest_path": str(manifest),
        "manifest_sha256": sha256_file(manifest),
        "row_count": int(audit["baseline_shadow_rows"]),
        "prediction_bucket_count": int(audit["prediction_bucket_count"]),
        "quote_decision_count": int(audit["quote_decision_count"]),
        "runtime_s": float(time.monotonic() - started),
        "economic_outcome_columns_read": [],
    }
    _atomic_json(descriptor_path, descriptor)
    return descriptor


def _schedule_paths(spec: Mapping[str, Any]) -> tuple[Path, Path]:
    evidence = _data_path(spec["storage"]["evidence_root"])
    return evidence / "threshold_schedule.json", evidence / "threshold_support.csv"


def build_and_freeze_thresholds(
    spec: Mapping[str, Any], baseline_rows: Sequence[Mapping[str, Any]]
) -> tuple[dict[str, dict[str, tuple[float, str]]], pd.DataFrame, dict[str, Any]]:
    manifests = {
        str(row["day"]): str(row["manifest_path"]) for row in baseline_rows
    }
    opportunities = baseline_opportunities_from_manifests(manifests)
    days = [str(day) for day in spec["panels"]["development_days"]]
    schedule, support = build_past_only_threshold_schedule_v15(
        opportunities,
        development_days=days,
        quantile=float(spec["threshold_contract"]["quantile"]),
        minimum_prior_days=int(spec["threshold_contract"]["minimum_prior_days"]),
        minimum_prior_buckets=int(
            spec["threshold_contract"]["minimum_prior_buckets"]
        ),
    )
    for day in days:
        readiness = [day in schedule[side] for side in SIDES]
        if readiness[0] != readiness[1]:
            raise RuntimeError(
                f"mixed BUY/SELL threshold readiness is unsupported on {day}"
            )
    schedule_payload = {
        "schema_version": f"{SCHEMA_VERSION}.past_only_threshold_schedule",
        "family_id": spec["family_id"],
        "spec_sha256": spec["canonical_spec_identity_sha256"],
        "opportunity_rows": int(len(opportunities)),
        "opportunity_identity_sha256": canonical_sha256(
            opportunities.to_dict("records")
        ),
        "schedules": {
            side: {
                day: {
                    "threshold": float(value[0]),
                    "source_identity_sha256": str(value[1]),
                }
                for day, value in sorted(schedule[side].items())
            }
            for side in SIDES
        },
        "economic_outcome_columns_read": [],
        "permissions": {
            "mechanics_read": False,
            "economic_outcomes_read": False,
            "action_experiment_authorized": False,
            "live_deployment_authorized": False,
        },
    }
    schedule_payload["canonical_schedule_sha256"] = canonical_sha256(
        schedule_payload
    )
    schedule_path, support_path = _schedule_paths(spec)
    if schedule_path.exists() or support_path.exists():
        existing = json.loads(schedule_path.read_text(encoding="utf-8"))
        if existing != schedule_payload:
            raise RuntimeError("frozen threshold schedule differs from recomputation")
    else:
        _atomic_json(schedule_path, schedule_payload)
        _atomic_csv(support_path, support)
    return schedule, support, schedule_payload


def _run_candidate_day(payload: Mapping[str, Any]) -> dict[str, Any]:
    spec = dict(payload["spec"])
    day = str(payload["day"])
    schedule = payload["schedule"]
    baseline_manifest = Path(str(payload["baseline_manifest"])).resolve()
    root = _day_cache_root(spec, day)
    descriptor_path = root / "candidate_descriptor.json"
    if descriptor_path.is_file():
        descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
        manifest_identities = descriptor.get("journal_manifest_identities") or {}
        expected_ready = bool(all(day in schedule[side] for side in SIDES))
        manifests_complete = (
            set(manifest_identities) == set(SIDES) if expected_ready else not manifest_identities
        )
        manifests_valid = all(
            Path(identity["path"]).is_file()
            and sha256_file(Path(identity["path"])) == identity["sha256"]
            for identity in manifest_identities.values()
        )
        if (
            descriptor.get("spec_sha256") == spec["canonical_spec_identity_sha256"]
            and bool(descriptor.get("threshold_ready")) == expected_ready
            and manifests_complete
            and manifests_valid
        ):
            return descriptor
        raise RuntimeError(f"stale candidate descriptor for {day}")
    started = time.monotonic()
    ready = all(day in schedule[side] for side in SIDES)
    if ready:
        output_root = root / "candidate_v1_5"
        if output_root.exists():
            raise RuntimeError(f"unadmitted candidate output exists: {output_root}")
        binding = RankedToxicityGuardAuthoritativeReplayV15(
            baseline_manifest_path=baseline_manifest,
            output_root=output_root,
            frozen_model_sha256=str(spec["model_identity"]["bundle_meta_sha256"]),
            threshold_schedule={
                side: {
                    key: (float(value[0]), str(value[1]))
                    for key, value in schedule[side].items()
                }
                for side in SIDES
            },
            sides=SIDES,
            chunk_rows=int(spec["replay_contract"]["journal_chunk_rows"]),
        )
    else:
        output_root = None
        binding = RankedToxicityThresholdUnreadyReplayV15(
            baseline_manifest_path=baseline_manifest
        )
    audit = _simulate_with_binding(spec, day, binding)
    manifest_identities: dict[str, dict[str, str]] = {}
    if ready:
        assert output_root is not None
        for side in SIDES:
            manifest_path = output_root / side.lower() / "manifest.json"
            manifest_payload = (audit.get("journal_manifests") or {}).get(side)
            if (
                not manifest_path.is_file()
                or not isinstance(manifest_payload, Mapping)
                or not bool(manifest_payload.get("closed"))
            ):
                raise RuntimeError(
                    f"candidate journal was not atomically closed for {day} {side}"
                )
            manifest_identities[side] = {
                "path": str(manifest_path),
                "sha256": sha256_file(manifest_path),
            }
    descriptor = {
        "schema_version": f"{SCHEMA_VERSION}.candidate_day_descriptor",
        "day": day,
        "spec_sha256": spec["canonical_spec_identity_sha256"],
        "threshold_ready": bool(ready),
        "binding_audit": audit,
        "journal_manifest_identities": manifest_identities,
        "runtime_s": float(time.monotonic() - started),
        "economic_outcome_columns_read": [],
    }
    _atomic_json(descriptor_path, descriptor)
    return descriptor


def _run_parallel(
    worker,
    payloads: Sequence[Mapping[str, Any]],
    *,
    workers: int,
) -> list[dict[str, Any]]:
    if workers <= 1:
        return [worker(payload) for payload in payloads]
    rows: list[dict[str, Any]] = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(worker, payload): payload for payload in payloads}
        for future in concurrent.futures.as_completed(futures):
            row = future.result()
            rows.append(row)
            print(
                f"[{worker.__name__}] {row['day']} "
                f"{row.get('runtime_s', 0.0):.1f}s",
                flush=True,
            )
    return sorted(rows, key=lambda row: str(row["day"]))


def _candidate_journal_rows(descriptor: Mapping[str, Any], side: str):
    identity = (descriptor.get("journal_manifest_identities") or {}).get(side)
    if not identity:
        return iter(())
    manifest_path = Path(str(identity["path"]))
    if sha256_file(manifest_path) != str(identity["sha256"]):
        raise RuntimeError(f"candidate journal manifest hash drifted: {manifest_path}")
    return iter_chunked_parquet_journal(manifest_path)


def _day_side_mechanics(
    descriptor: Mapping[str, Any], side: str, grade: str
) -> dict[str, Any]:
    day = str(descriptor["day"])
    counts: defaultdict[str, int] = defaultdict(int)
    assignments: dict[str, tuple[str, float]] = {}
    activated: set[str] = set()
    changed: set[str] = set()
    changed_roles: set[str] = set()
    for row in _candidate_journal_rows(descriptor, side):
        event = str(row.get("event_type", ""))
        counts[event] += 1
        prospective_id = str(row.get("prospective_campaign_side_id", "") or "")
        if event == "prediction_bucket":
            counts["prediction_bucket_observed"] += 1
            if float(row["toxicity_score"]) >= float(row["threshold"]):
                counts["prediction_bucket_exceeded"] += 1
            if int(row["feature_ready_ts_ms"]) > int(row["prediction_observed_ts_ms"]):
                counts["feature_clock_violation"] += 1
        elif event == "quote_decision":
            eligible = bool(row.get("baseline_shadow_eligible", False)) and bool(
                row.get("baseline_shadow_exposure_increasing", False)
            )
            if eligible:
                counts["eligible_decision"] += 1
                if float(row["toxicity_score"]) >= float(row["threshold"]):
                    counts["eligible_decision_exceeded"] += 1
            if int(row["feature_ready_ts_ms"]) * 1_000_000 > int(row["event_ts_ns"]):
                counts["feature_clock_violation"] += 1
        elif event == "prospective_campaign_side_assignment":
            if not prospective_id or prospective_id in assignments:
                raise RuntimeError(f"duplicate/empty assignment on {day} {side}")
            assignments[prospective_id] = (
                str(row["action"]),
                float(row["behavior_propensity"]),
            )
        if bool(row.get("activated", False)) and prospective_id:
            activated.add(prospective_id)
        if event == "final_quote_action" and bool(
            row.get("final_quote_action_changed", False)
        ):
            if prospective_id:
                changed.add(prospective_id)
            role = str(row.get("role", "")).lower()
            if role in {"opener", "add"}:
                changed_roles.add(role)
        if event == "cancel_requested" and bool(row.get("guard_initiated", False)):
            counts["guard_cancel_requested"] += 1
        if event == "exchange_terminal" and str(row.get("terminal_reason", "")) == "cancel_ack":
            counts["cancel_ack"] += 1

    audit = (descriptor["binding_audit"].get("adapters") or {}).get(side) or {}
    zero = audit.get("zero_tolerance_counts") or {}
    propensities = [value[1] for value in assignments.values()]
    weights = np.asarray([1.0 / value for value in propensities], dtype=float)
    ess = (
        float(weights.sum() ** 2 / np.square(weights).sum())
        if weights.size
        else 0.0
    )
    return {
        "day": day,
        "grade": grade,
        "side": side,
        "threshold_ready": bool(descriptor["threshold_ready"]),
        "prediction_bucket_observed": counts["prediction_bucket_observed"],
        "prediction_bucket_exceeded": counts["prediction_bucket_exceeded"],
        "eligible_decision": counts["eligible_decision"],
        "eligible_decision_exceeded": counts["eligible_decision_exceeded"],
        "assignment_count": len(assignments),
        "candidate_assignment_count": sum(
            action == "ranked_toxicity_guard" for action, _ in assignments.values()
        ),
        "campaign_activated": len(activated),
        "final_quote_action_changed": len(changed),
        "opener_action_change_supported": "opener" in changed_roles,
        "add_action_change_supported": "add" in changed_roles,
        "minimum_behavior_propensity": min(propensities) if propensities else math.nan,
        "effective_sample_size": ess,
        "guard_cancel_requested": counts["guard_cancel_requested"],
        "cancel_ack": counts["cancel_ack"],
        "feature_clock_violations": counts["feature_clock_violation"],
        "post_terminal_hazard_or_cursor_reuse": int(
            zero.get("post_terminal_hazard_or_cursor_reuse", 0)
        ),
        "reducing_quote_changes": int(zero.get("reducing_quote_changes", 0)),
        "baseline_shadow_mismatches": int(
            zero.get("control_candidate_baseline_shadow_mismatch", 0)
        ),
        "campaign_side_rerandomizations": int(
            zero.get("campaign_side_rerandomization", 0)
        ),
        "execution_complete": bool(audit.get("execution_complete", False)),
        "zero_tolerance_passed": bool(audit.get("zero_tolerance_passed", False)),
    }


def _ratio(numerator: float, denominator: float) -> float:
    return float(numerator) / float(denominator) if denominator else math.nan


def summarize_mechanics(
    spec: Mapping[str, Any], candidate_rows: Sequence[Mapping[str, Any]]
) -> tuple[pd.DataFrame, dict[str, Any]]:
    grade_a = set(str(day) for day in spec["panels"]["grade_a_days"])
    daily_rows: list[dict[str, Any]] = []
    for descriptor in candidate_rows:
        day = str(descriptor["day"])
        grade = "A" if day in grade_a else "B"
        if descriptor["threshold_ready"]:
            for side in SIDES:
                daily_rows.append(_day_side_mechanics(descriptor, side, grade))
    daily = pd.DataFrame(daily_rows)
    if daily.empty:
        raise RuntimeError("no threshold-ready mechanics rows were produced")
    side_results: dict[str, Any] = {}
    hard_gates = spec["mechanics_hard_gates"]
    for side in SIDES:
        frame = daily[daily["side"].eq(side)]
        sums = frame.select_dtypes(include=["number", "bool"]).sum()
        assignments = int(frame["assignment_count"].sum())
        ess = float(frame["effective_sample_size"].sum())
        minimum_propensity = float(frame["minimum_behavior_propensity"].min())
        result = {
            "threshold_ready_days": int(frame["day"].nunique()),
            "grade_a_ready_days": int(frame.loc[frame["grade"].eq("A"), "day"].nunique()),
            "grade_b_ready_days": int(frame.loc[frame["grade"].eq("B"), "day"].nunique()),
            "prediction_bucket_exceedance_rate": _ratio(
                sums["prediction_bucket_exceeded"], sums["prediction_bucket_observed"]
            ),
            "eligible_decision_exceedance_rate": _ratio(
                sums["eligible_decision_exceeded"], sums["eligible_decision"]
            ),
            "campaign_activation_rate": _ratio(
                sums["campaign_activated"], assignments
            ),
            "final_quote_action_change_rate": _ratio(
                sums["final_quote_action_changed"], assignments
            ),
            "assignments": assignments,
            "candidate_assignments": int(frame["candidate_assignment_count"].sum()),
            "minimum_behavior_propensity": minimum_propensity,
            "effective_sample_size": ess,
            "ess_fraction": _ratio(ess, assignments),
            "opener_support_days": int(
                frame.loc[frame["opener_action_change_supported"], "day"].nunique()
            ),
            "add_support_days": int(
                frame.loc[frame["add_action_change_supported"], "day"].nunique()
            ),
            "guard_cancel_requests": int(frame["guard_cancel_requested"].sum()),
            "cancel_ACKs": int(frame["cancel_ack"].sum()),
            "cancel_ACK_coverage": _ratio(
                frame["cancel_ack"].sum(), frame["guard_cancel_requested"].sum()
            ),
            "feature_ready_clock_violations": int(
                frame["feature_clock_violations"].sum()
            ),
            "post_terminal_hazard_or_cursor_reuse": int(
                frame["post_terminal_hazard_or_cursor_reuse"].sum()
            ),
            "reducing_quote_changes": int(frame["reducing_quote_changes"].sum()),
            "baseline_shadow_mismatches": int(
                frame["baseline_shadow_mismatches"].sum()
            ),
            "campaign_side_rerandomizations": int(
                frame["campaign_side_rerandomizations"].sum()
            ),
            "execution_complete_all_days": bool(frame["execution_complete"].all()),
            "zero_tolerance_passed_all_days": bool(
                frame["zero_tolerance_passed"].all()
            ),
        }
        rate_range = hard_gates["prediction_bucket_exceedance_rate_range"]
        eligible_range = hard_gates["eligible_decision_exceedance_rate_range"]
        activation_range = hard_gates["campaign_activation_rate_range"]
        gates = {
            "threshold_ready_days": result["threshold_ready_days"]
            >= int(hard_gates["minimum_threshold_ready_days"]),
            "assignments": assignments >= int(hard_gates["minimum_assignments"]),
            "behavior_propensity": minimum_propensity
            >= float(hard_gates["minimum_behavior_propensity"]),
            "ESS": result["ess_fraction"]
            >= float(hard_gates["minimum_ESS_fraction_of_assignments"]),
            "prediction_bucket_exceedance": float(rate_range[0])
            <= result["prediction_bucket_exceedance_rate"]
            <= float(rate_range[1]),
            "eligible_decision_exceedance": float(eligible_range[0])
            <= result["eligible_decision_exceedance_rate"]
            <= float(eligible_range[1]),
            "campaign_activation": float(activation_range[0])
            <= result["campaign_activation_rate"]
            <= float(activation_range[1]),
            "final_quote_action_change": result["final_quote_action_change_rate"]
            >= float(hard_gates["minimum_final_quote_action_change_rate"]),
            "opener_support": result["opener_support_days"]
            >= int(hard_gates["minimum_opener_support_days"]),
            "add_support": result["add_support_days"]
            >= int(hard_gates["minimum_add_support_days"]),
            "cancel_ACK_coverage": math.isclose(
                result["cancel_ACK_coverage"],
                float(hard_gates["cancel_ACK_coverage"]),
                rel_tol=0.0,
                abs_tol=1e-12,
            ),
            "feature_clock": result["feature_ready_clock_violations"] == 0,
            "post_terminal_risk_set": result[
                "post_terminal_hazard_or_cursor_reuse"
            ]
            == 0,
            "reducing_quote_unchanged": result["reducing_quote_changes"] == 0,
            "baseline_shadow_parity": result["baseline_shadow_mismatches"] == 0,
            "assignment_stability": result["campaign_side_rerandomizations"] == 0,
            "execution_complete": result["execution_complete_all_days"],
            "zero_tolerance": result["zero_tolerance_passed_all_days"],
        }
        result["hard_gates"] = gates
        result["mechanics_supported"] = all(gates.values())
        side_results[side] = result
    report = {
        "schema_version": f"{SCHEMA_VERSION}.development_report",
        "family_id": spec["family_id"],
        "spec_sha256": spec["canonical_spec_identity_sha256"],
        "development_days": 40,
        "threshold_unready_days": sorted(
            set(spec["panels"]["development_days"]) - set(daily["day"])
        ),
        "sides": side_results,
        "mechanics_supported": all(
            side_results[side]["mechanics_supported"] for side in SIDES
        ),
        "mechanics_read": True,
        "economic_outcome_columns_read": [],
        "development_economic_outcome_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_experiment_authorized": False,
        "live_deployment_authorized": False,
    }
    report["report_identity_sha256"] = canonical_sha256(report)
    return daily, report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument(
        "--stage",
        choices=("baseline", "candidate", "summarize", "all"),
        default="all",
    )
    parser.add_argument("--workers", type=int, default=2)
    args = parser.parse_args()
    spec = load_spec(args.spec.expanduser().resolve())
    gate = storage_gate(spec)
    days = [str(day) for day in spec["panels"]["development_days"]]
    baseline_payloads = [{"spec": spec, "day": day} for day in days]
    baseline_rows = _run_parallel(
        _run_baseline_day,
        baseline_payloads,
        workers=max(1, int(args.workers)),
    )
    schedule, support, schedule_payload = build_and_freeze_thresholds(
        spec, baseline_rows
    )
    evidence = _data_path(spec["storage"]["evidence_root"])
    _atomic_json(evidence / "storage_gate.json", gate)
    _atomic_json(
        evidence / "baseline_index.json",
        {
            "schema_version": f"{SCHEMA_VERSION}.baseline_index",
            "spec_sha256": spec["canonical_spec_identity_sha256"],
            "days": baseline_rows,
            "threshold_schedule_sha256": schedule_payload[
                "canonical_schedule_sha256"
            ],
            "economic_outcome_columns_read": [],
        },
    )
    if args.stage == "baseline":
        print(json.dumps({"baseline_days": len(baseline_rows)}, indent=2))
        return

    candidate_payloads = [
        {
            "spec": spec,
            "day": day,
            "schedule": schedule,
            "baseline_manifest": next(
                row["manifest_path"] for row in baseline_rows if row["day"] == day
            ),
        }
        for day in days
    ]
    candidate_rows = _run_parallel(
        _run_candidate_day,
        candidate_payloads,
        workers=max(1, int(args.workers)),
    )
    _atomic_json(
        evidence / "candidate_index.json",
        {
            "schema_version": f"{SCHEMA_VERSION}.candidate_index",
            "spec_sha256": spec["canonical_spec_identity_sha256"],
            "days": candidate_rows,
            "economic_outcome_columns_read": [],
        },
    )
    if args.stage == "candidate":
        print(json.dumps({"candidate_days": len(candidate_rows)}, indent=2))
        return

    daily, report = summarize_mechanics(spec, candidate_rows)
    _atomic_csv(evidence / "daily_mechanics.csv", daily)
    _atomic_json(evidence / "report.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
