"""Frozen identity and latency semantics for formal tick replay evidence."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, MutableMapping
from pathlib import Path
from typing import Any

import numpy as np

from strategy.replay_controls import (
    LOSS_COOLDOWN_SEMANTICS,
    SYNC_DEGRADE_SEMANTICS,
    SYNC_DEGRADE_TAPE_SCHEMA,
    effective_decision_to_gateway_latency_seed,
    effective_latency_seed,
    replay_hard_risk_limits,
)

FORMAL_REPLAY_CONTRACT_SCHEMA = "narrowgate_formal_replay_contract.v4"
STANDARD_INITIAL_STATE_SCHEMA = "narrowgate_standard_initial_state.v1"
KEYED_LATENCY_SAMPLER_VERSION = "keyed_splitmix64_v1"
DEFAULT_LATENCY_ENVIRONMENT = "provider_neutral_unspecified"
INDIVIDUAL_TRADES_REPAIR_ID = "individual_trade_side_repaired_20260725"
INDIVIDUAL_TRADES_REPAIRED_DAYS = tuple(f"2026-07-{day:02d}" for day in range(4, 12))

_FROZEN_QUEUE_KEYS = (
    "queue_ahead_base_mult",
    "queue_deplete_base_mult",
    "queue_ahead_buy_exposure_mult",
    "queue_ahead_buy_reducing_mult",
    "queue_ahead_sell_exposure_mult",
    "queue_ahead_sell_reducing_mult",
)
_MODEL_SUFFIXES = {".json", ".txt", ".bin", ".model", ".pkl", ".joblib"}
_INDIVIDUAL_TRADE_SOURCES = {
    "trades",
    "individual_trade",
    "individual_trades",
    "binance_usdm_individual_trades",
}


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def sha256_file(path: str | Path | None) -> str:
    if path is None:
        return ""
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        return ""
    digest = hashlib.sha256()
    with resolved.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _resolve_path(raw: Any, *, root: Path | None = None) -> Path | None:
    text = str(raw or "").strip()
    if not text:
        return None
    path = Path(text).expanduser()
    if not path.is_absolute() and root is not None:
        path = root / path
    return path.resolve()


def _is_individual_trade_source(value: Any) -> bool:
    normalized = str(value or "").strip().lower().replace("-", "_")
    return normalized in _INDIVIDUAL_TRADE_SOURCES


def _valid_sha256(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return len(text) == 64 and all(char in "0123456789abcdef" for char in text)


def _individual_trade_manifest_rows(path: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if path.suffix.lower() == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise ValueError("individual-trades manifest JSON must contain an object")
        raw_rows = payload.get("daily_files")
        if raw_rows is None:
            raw_rows = payload.get("files")
        if not isinstance(raw_rows, list):
            raise ValueError("individual-trades manifest must contain daily_files or files")
        rows: list[dict[str, Any]] = []
        for raw_row in raw_rows:
            if not isinstance(raw_row, Mapping):
                continue
            day = str(raw_row.get("day", "") or "")
            raw_path = str(
                raw_row.get("raw_file") or raw_row.get("path") or raw_row.get("file") or ""
            )
            raw_sha256 = str(raw_row.get("raw_sha256") or raw_row.get("sha256") or "").lower()
            rows.append(
                {
                    "day": day,
                    "file_name": Path(raw_path).name if raw_path else "",
                    "raw_sha256": raw_sha256,
                    "raw_size_bytes": int(
                        raw_row.get("raw_size_bytes") or raw_row.get("size_bytes") or 0
                    ),
                }
            )
        metadata = {
            "schema_version": str(payload.get("schema") or payload.get("schema_version") or ""),
            "symbol": str(payload.get("symbol", "") or ""),
            "source": str(payload.get("source") or payload.get("raw_root") or ""),
            "content_sha256": str(
                payload.get("daily_manifest_sha256")
                or payload.get("manifest_identity_sha256")
                or ""
            ),
        }
        return metadata, rows

    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        fields = line.strip().split(maxsplit=1)
        if len(fields) != 2 or not _valid_sha256(fields[0]):
            continue
        file_name = fields[1].lstrip("*")
        day = ""
        for token in Path(file_name).stem.split("-"):
            if len(token) == 10 and token[4:5] == "-" and token[7:8] == "-":
                day = token
                break
        if not day:
            marker = "trades-"
            start = file_name.find(marker)
            if start >= 0:
                day = file_name[start + len(marker) : start + len(marker) + 10]
        rows.append(
            {
                "day": day,
                "file_name": Path(file_name).name,
                "raw_sha256": fields[0].lower(),
                "raw_size_bytes": 0,
            }
        )
    if not rows:
        raise ValueError("individual-trades checksum manifest contains no valid rows")
    return {
        "schema_version": "sha256_file_list",
        "symbol": "",
        "source": "",
        "content_sha256": "",
    }, rows


def _individual_trades_identity(
    params: Mapping[str, Any],
    *,
    root: Path | None,
) -> dict[str, Any]:
    source = str(params.get("execution_trade_source", "individual_trades") or "")
    if not _is_individual_trade_source(source):
        return {
            "source": source,
            "status": "not_applicable",
            "evidence_scope": "not_applicable",
            "manifest_path": "",
            "manifest_sha256": "",
            "manifest_content_sha256": "",
            "integrity_report_path": "",
            "integrity_report_sha256": "",
            "repair_identity": {
                "id": INDIVIDUAL_TRADES_REPAIR_ID,
                "required_days": list(INDIVIDUAL_TRADES_REPAIRED_DAYS),
                "days": [],
                "complete": False,
            },
        }

    manifest_path = _resolve_path(
        params.get("individual_trades_manifest_path")
        or params.get("execution_trade_manifest_path"),
        root=root,
    )
    integrity_path = _resolve_path(
        params.get("individual_trades_integrity_report_path")
        or params.get("execution_trade_quality_path"),
        root=root,
    )
    manifest_sha256 = sha256_file(manifest_path)
    integrity_sha256 = sha256_file(integrity_path)
    declared_manifest_sha256 = str(
        params.get("individual_trades_manifest_sha256", "") or ""
    ).lower()
    declared_integrity_sha256 = str(
        params.get("individual_trades_integrity_report_sha256")
        or params.get("execution_trade_quality_sha256")
        or ""
    ).lower()

    metadata: dict[str, Any] = {
        "schema_version": "",
        "symbol": "",
        "source": "",
        "content_sha256": "",
    }
    rows: list[dict[str, Any]] = []
    parse_error = ""
    if manifest_path is not None and manifest_sha256:
        try:
            metadata, rows = _individual_trade_manifest_rows(manifest_path)
        except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            parse_error = str(exc)

    rows_by_day = {str(row.get("day", "")): row for row in rows if str(row.get("day", ""))}
    repaired_rows = [
        rows_by_day[day]
        for day in INDIVIDUAL_TRADES_REPAIRED_DAYS
        if day in rows_by_day and _valid_sha256(rows_by_day[day].get("raw_sha256"))
    ]
    missing_repaired_days = [
        day
        for day in INDIVIDUAL_TRADES_REPAIRED_DAYS
        if day not in {str(row.get("day", "")) for row in repaired_rows}
    ]

    manifest_hash_matches = (
        not declared_manifest_sha256 or declared_manifest_sha256 == manifest_sha256
    )
    integrity_hash_matches = (
        not declared_integrity_sha256 or declared_integrity_sha256 == integrity_sha256
    )
    if not manifest_sha256:
        status = "missing_manifest"
    elif parse_error:
        status = "invalid_manifest"
    elif not manifest_hash_matches:
        status = "manifest_hash_mismatch"
    elif integrity_path is not None and not integrity_sha256:
        status = "missing_integrity_report"
    elif not integrity_hash_matches:
        status = "integrity_report_hash_mismatch"
    elif missing_repaired_days:
        status = "repair_identity_incomplete"
    else:
        status = "verified"

    days = sorted(rows_by_day)
    return {
        "source": source,
        "status": status,
        "evidence_scope": ("formal_eligible" if status == "verified" else "diagnostic_only"),
        "manifest_path": str(manifest_path or ""),
        "manifest_sha256": manifest_sha256,
        "declared_manifest_sha256": declared_manifest_sha256,
        "manifest_hash_matches_declared": manifest_hash_matches,
        "manifest_schema_version": str(metadata.get("schema_version", "")),
        "manifest_content_sha256": str(metadata.get("content_sha256", "")),
        "manifest_symbol": str(metadata.get("symbol", "")),
        "manifest_source": str(metadata.get("source", "")),
        "manifest_day_count": len(rows_by_day),
        "manifest_first_day": days[0] if days else "",
        "manifest_last_day": days[-1] if days else "",
        "manifest_parse_error": parse_error,
        "integrity_report_path": str(integrity_path or ""),
        "integrity_report_sha256": integrity_sha256,
        "declared_integrity_report_sha256": declared_integrity_sha256,
        "integrity_report_hash_matches_declared": integrity_hash_matches,
        "repair_identity": {
            "id": INDIVIDUAL_TRADES_REPAIR_ID,
            "required_days": list(INDIVIDUAL_TRADES_REPAIRED_DAYS),
            "days": repaired_rows,
            "missing_days": missing_repaired_days,
            "complete": not missing_repaired_days,
        },
    }


def artifact_tree_identity(path: str | Path | None) -> dict[str, Any]:
    """Hash the deployable files in a model directory without hashing caches."""
    resolved = _resolve_path(path)
    if resolved is None:
        return {"path": "", "files": [], "sha256": ""}
    if resolved.is_file():
        rows = [{"path": resolved.name, "sha256": sha256_file(resolved)}]
    elif resolved.is_dir():
        rows = [
            {
                "path": str(candidate.relative_to(resolved)),
                "sha256": sha256_file(candidate),
            }
            for candidate in sorted(resolved.rglob("*"))
            if candidate.is_file()
            and not candidate.name.startswith(".")
            and candidate.suffix.lower() in _MODEL_SUFFIXES
        ]
    else:
        rows = []
    return {
        "path": str(resolved),
        "files": rows,
        "sha256": hashlib.sha256(_canonical_bytes(rows)).hexdigest() if rows else "",
    }


def _finite_samples(values: Any) -> np.ndarray:
    try:
        samples = np.asarray(values if values is not None else [], dtype=np.float64).ravel()
    except (TypeError, ValueError):
        return np.empty(0, dtype=np.float64)
    return np.ascontiguousarray(
        samples[np.isfinite(samples) & (samples >= 0.0)],
        dtype=np.float64,
    )


def _sample_identity(values: Any) -> dict[str, Any]:
    samples = _finite_samples(values)
    payload = [float(value) for value in samples]
    return {
        "count": int(samples.size),
        "sha256": hashlib.sha256(_canonical_bytes(payload)).hexdigest(),
        "min_ms": float(samples.min()) if samples.size else 0.0,
        "median_ms": float(np.median(samples)) if samples.size else 0.0,
        "max_ms": float(samples.max()) if samples.size else 0.0,
    }


def _local_work_samples(params: Mapping[str, Any]) -> tuple[np.ndarray, np.ndarray]:
    """Validate paired work samples before any generic latency normalization."""
    tail = np.asarray(params.get("_requote_tail_work_samples_ms", ()), dtype=np.float64)
    loop = np.asarray(params.get("_main_loop_work_samples_ms", ()), dtype=np.float64)
    if tail.size and (
        tail.ndim != 1 or not np.all(np.isfinite(tail)) or np.any(tail < 0.0)
    ):
        raise ValueError("requote tail work samples must be finite nonnegative 1D values")
    if loop.size and (
        loop.ndim != 2 or loop.shape[1] != 2
        or not np.all(np.isfinite(loop)) or np.any(loop < 0.0)
    ):
        raise ValueError("main-loop work samples must be finite nonnegative Nx2 values")
    if np.any(tail > 0.0):
        total = np.asarray(
            params.get("_decision_to_gateway_latency_samples_ms", ()), dtype=np.float64,
        )
        if (
            total.ndim != 1 or total.shape != tail.shape
            or not np.all(np.isfinite(total)) or np.any(total < 0.0)
        ):
            raise ValueError("requote tail work samples must align with finite total compute")
    if np.any(tail > 0.0) or np.any(loop > 0.0):
        sleep = float(params.get("replay_main_loop_sleep_ms", 0) or 0)
        mode = str(params.get("rest_gateway_timing_mode", "disabled") or "disabled").lower()
        if not np.isfinite(sleep) or sleep <= 0.0 or mode != "sampled_serial":
            raise ValueError("local work requires main-loop sampled_serial replay")
    return tail, loop


def configure_fixed_latency_distribution(
    params: MutableMapping[str, Any],
    *,
    scenario: str,
    profile_id: str,
    environment: str | Mapping[str, Any],
    baseline_clip_quantile: float = 0.99,
    stress_spike_probability: float = 0.001,
    stress_spike_multiplier: float = 5.0,
) -> MutableMapping[str, Any]:
    """Freeze stable latency samples; synthetic tail spikes are stress-only."""
    _local_work_samples(params)
    normalized_scenario = str(scenario or "baseline").lower()
    if normalized_scenario not in {"baseline", "stress"}:
        raise ValueError("latency scenario must be baseline or stress")
    clip_quantile = float(baseline_clip_quantile)
    if not 0.5 <= clip_quantile <= 1.0:
        raise ValueError("baseline latency clip quantile must be within [0.5, 1.0]")
    probability = float(stress_spike_probability)
    multiplier = float(stress_spike_multiplier)
    if not 0.0 <= probability <= 1.0:
        raise ValueError("stress spike probability must be within [0, 1]")
    if multiplier < 1.0:
        raise ValueError("stress spike multiplier must be at least 1")

    for key in (
        "_decision_to_gateway_latency_samples_ms",
        "_new_order_latency_samples_ms",
        "_new_order_exchange_effective_latency_samples_ms",
        "_cancel_order_latency_samples_ms",
        "_cancel_exchange_effective_latency_samples_ms",
        "_cancel_ack_visibility_latency_samples_ms",
        "_private_fill_visibility_latency_samples_ms",
        "_exec_book_visibility_delay_samples_ms",
    ):
        samples = _finite_samples(params.get(key, ()))
        if samples.size:
            cap = float(np.quantile(samples, clip_quantile))
            params[key] = np.ascontiguousarray(np.minimum(samples, cap), dtype=np.float64)

    params["latency_sampler_version"] = KEYED_LATENCY_SAMPLER_VERSION
    params["latency_profile_id"] = str(profile_id or "").strip()
    params["latency_environment"] = (
        dict(environment) if isinstance(environment, Mapping) else str(environment or "").strip()
    )
    params["latency_scenario"] = normalized_scenario
    params["latency_baseline_clip_quantile"] = clip_quantile
    params["latency_stress_enabled"] = normalized_scenario == "stress"
    params["latency_stress_spike_probability"] = probability
    params["latency_stress_spike_multiplier"] = multiplier
    params["latency_rare_spike_policy"] = "stress_only"
    return params


def load_standard_initial_state(path: str | Path) -> dict[str, float]:
    resolved = Path(path).expanduser().resolve()
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if payload.get("schema_version") != STANDARD_INITIAL_STATE_SCHEMA:
        raise ValueError(
            f"standard initial state must use {STANDARD_INITIAL_STATE_SCHEMA}: {resolved}"
        )
    if payload.get("active_orders"):
        raise ValueError("formal standard initial state cannot contain active orders")
    output = {
        "initial_inventory": float(payload.get("initial_inventory", 0.0) or 0.0),
        "initial_entry_price": float(payload.get("initial_entry_price", 0.0) or 0.0),
    }
    if not all(math.isfinite(value) for value in output.values()):
        raise ValueError(f"standard initial state contains non-finite values: {resolved}")
    if abs(output["initial_inventory"]) > 0.0 and output["initial_entry_price"] <= 0.0:
        raise ValueError("non-flat standard initial state requires a positive entry price")
    return output


def _sync_event_tape_metadata(path: Path | None) -> dict[str, Any]:
    if path is None or not path.is_file():
        return {
            "schema_version": "",
            "environment": "",
            "start_ts_ms": 0,
            "end_ts_ms": 0,
            "event_count": 0,
        }
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("sync-adjust event tape must contain an object")
    schema = str(payload.get("schema_version", "") or "")
    if schema != SYNC_DEGRADE_TAPE_SCHEMA:
        raise ValueError("sync-adjust event tape schema is invalid")
    events = payload.get("events")
    if not isinstance(events, list):
        raise ValueError("sync-adjust event tape events must be a list")
    return {
        "schema_version": schema,
        "environment": str(payload.get("environment", "") or ""),
        "start_ts_ms": int(payload.get("start_ts_ms", 0) or 0),
        "end_ts_ms": int(payload.get("end_ts_ms", 0) or 0),
        "event_count": int(len(events)),
    }


def _artifact_identity(params: Mapping[str, Any], *, root: Path | None) -> dict[str, Any]:
    config_path = _resolve_path(params.get("_config_path"), root=root)
    p3_path = _resolve_path(params.get("fill_probability_model_path"), root=root)
    queue_path = _resolve_path(params.get("queue_calibration_path"), root=root)
    formal_l2_manifest_path = _resolve_path(
        params.get("formal_l2_manifest_path"),
        root=root,
    )
    model_path = _resolve_path(
        params.get("resolved_model_dir") or params.get("model_dir"), root=root
    )
    buy_score_path = _resolve_path(params.get("buy_fill_selection_live_model_path"), root=root)
    hazard_model_path = _resolve_path(
        params.get("dynamic_fill_hazard_shadow_model_path"), root=root
    )
    hazard_policy_path = _resolve_path(
        params.get("dynamic_fill_hazard_action_policy_path"), root=root
    )
    sync_tape_path = _resolve_path(
        params.get("sync_adjust_event_tape_path"), root=root
    )
    sync_tape_metadata = _sync_event_tape_metadata(sync_tape_path)
    return {
        "config": {"path": str(config_path or ""), "sha256": sha256_file(config_path)},
        "model": artifact_tree_identity(model_path),
        "p3": {"path": str(p3_path or ""), "sha256": sha256_file(p3_path)},
        "queue": {"path": str(queue_path or ""), "sha256": sha256_file(queue_path)},
        "formal_l2": {
            "dataset_root": str(params.get("formal_l2_dataset_root", "") or ""),
            "manifest_path": str(formal_l2_manifest_path or ""),
            "manifest_sha256": sha256_file(formal_l2_manifest_path),
        },
        "individual_trades": _individual_trades_identity(params, root=root),
        "buy_fill_selection": {
            "path": str(buy_score_path or ""),
            "sha256": sha256_file(buy_score_path),
        },
        "dynamic_fill_hazard_model": {
            "path": str(hazard_model_path or ""),
            "sha256": sha256_file(hazard_model_path),
            "declared_sha256": str(
                params.get("dynamic_fill_hazard_shadow_model_sha256", "") or ""
            ).lower(),
        },
        "dynamic_fill_hazard_policy": {
            "path": str(hazard_policy_path or ""),
            "sha256": sha256_file(hazard_policy_path),
            "declared_sha256": str(
                params.get("dynamic_fill_hazard_action_policy_sha256", "") or ""
            ).lower(),
        },
        "sync_adjust_event_tape": {
            "path": str(sync_tape_path or ""),
            "sha256": sha256_file(sync_tape_path),
            "declared_sha256": str(
                params.get("sync_adjust_event_tape_sha256", "") or ""
            ).lower(),
            **sync_tape_metadata,
        },
    }


def build_replay_contract(
    params: Mapping[str, Any],
    *,
    purpose: str = "formal",
    initial_state_mode: str = "fresh_start",
    initial_state_artifact: str | Path | None = None,
    root: str | Path | None = None,
) -> dict[str, Any]:
    """Build a canonical replay identity from the parameters actually executed."""
    normalized_purpose = str(purpose or "formal").lower()
    if normalized_purpose not in {
        "formal",
        "exploratory",
        "live_alignment",
        "diagnostic",
    }:
        raise ValueError(
            "replay purpose must be formal, exploratory, live_alignment, or diagnostic"
        )
    normalized_initial = str(initial_state_mode or "fresh_start").lower()
    if normalized_initial not in {"fresh_start", "frozen_standard"}:
        raise ValueError("initial state mode must be fresh_start or frozen_standard")
    latency_seed = effective_latency_seed(params)
    decision_to_gateway_latency_seed = (
        effective_decision_to_gateway_latency_seed(params)
    )
    project_root = Path(root).expanduser().resolve() if root is not None else None
    state_path = _resolve_path(initial_state_artifact, root=project_root)

    artifacts = _artifact_identity(params, root=project_root)
    individual_trades_diagnostic = (artifacts.get("individual_trades") or {}).get(
        "evidence_scope"
    ) == "diagnostic_only"
    sync_mode = str(
        params.get("sync_adjust_replay_mode", "disabled") or "disabled"
    ).strip().lower()
    sync_enabled = bool(params.get("sync_adjust_degrade_enabled", False))
    q90_enabled = bool(params.get("dynamic_fill_hazard_action_enabled", False))
    sync_promotion_eligible = bool(
        (not sync_enabled and sync_mode == "disabled")
        or (sync_enabled and sync_mode == "frozen_tape")
    )
    legacy_gamma = float(params.get("gamma", 0.0) or 0.0)
    inventory_reference_qty = float(
        params.get("inventory_reference_qty", 1.0) or 0.0
    )
    eta_inventory = params.get("eta_inventory")
    if eta_inventory is None:
        eta_inventory = legacy_gamma * inventory_reference_qty
    a_spread = params.get("a_spread")
    if a_spread is None:
        a_spread = legacy_gamma
    risk_per_order = params.get("risk_per_order")
    if risk_per_order is None:
        risk_per_order = a_spread
    execution_intensity_slope = params.get("execution_intensity_slope")
    if execution_intensity_slope is None:
        execution_intensity_slope = params.get("kappa", 0.0)
    quote_horizon_s = float(params.get("quote_horizon_s", 1.0) or 0.0)
    risk_horizon_s = params.get("risk_horizon_s")
    if risk_horizon_s is None:
        risk_horizon_s = quote_horizon_s
    historical_p3_adapter = bool(
        params.get("historical_p3_scalar_adapter_enabled", True)
    )
    p3_side_bbo_floor = bool(params.get("p3_side_bbo_floor_enabled", False))
    private_fill_samples = _finite_samples(
        params.get("_private_fill_visibility_latency_samples_ms", ())
    )
    private_fill_visibility_enabled = bool(
        private_fill_samples.size and np.any(private_fill_samples > 0.0)
    )
    decision_to_gateway_samples = _finite_samples(
        params.get("_decision_to_gateway_latency_samples_ms", ())
    )
    decision_to_gateway_enabled = bool(
        decision_to_gateway_samples.size
        and np.any(decision_to_gateway_samples > 0.0)
    )
    decision_to_gateway_identity: dict[str, Any] | None = None
    if decision_to_gateway_enabled:
        decision_to_gateway_identity = {
            "evidence_scope": "diagnostic_only",
            "clock": "decision_to_first_gateway_request",
            "sampling_unit": "one_keyed_draw_per_decision",
            "market_snapshot": "frozen_at_decision_time",
            "serial_row_origin": "after_decision_compute_delay",
            "scope": "normal_quote_requests_only_not_ttl_fill_or_safety",
            "samples": _sample_identity(decision_to_gateway_samples),
            "seed": decision_to_gateway_latency_seed,
        }
    rest_gateway_mode = str(
        params.get("rest_gateway_timing_mode", "disabled") or "disabled"
    ).strip().lower()
    pre_snapshot_samples = np.asarray(
        params.get("_pre_snapshot_compute_latency_samples_ms", ()),
        dtype=np.float64,
    )
    if pre_snapshot_samples.size and (
        pre_snapshot_samples.ndim != 1
        or not np.all(np.isfinite(pre_snapshot_samples))
        or np.any(pre_snapshot_samples < 0.0)
    ):
        raise ValueError("pre-snapshot compute samples must be finite nonnegative 1D values")
    if np.any(pre_snapshot_samples > 0.0):
        total_samples = np.asarray(
            params.get("_decision_to_gateway_latency_samples_ms", ()),
            dtype=np.float64,
        )
        if (
            total_samples.shape != pre_snapshot_samples.shape
            or not np.all(np.isfinite(total_samples))
            or np.any(pre_snapshot_samples > total_samples)
        ):
            raise ValueError("pre-snapshot compute samples must align with and not exceed total")
        if (
            rest_gateway_mode != "sampled_serial"
            or float(params.get("replay_main_loop_sleep_ms", 0) or 0) <= 0
        ):
            raise ValueError("pre-snapshot compute requires main-loop sampled_serial replay")
        assert decision_to_gateway_identity is not None
        decision_to_gateway_identity.update(
            clock="requote_entry_to_first_gateway_request",
            market_snapshot="captured_after_pre_snapshot_compute",
            computation_split={
                "backend": "python_only",
                "sampling": "same_total_sample_index_and_seed_per_requote_entry",
                "prediction_cutoff": "requote_entry_before_pre_snapshot_compute",
                "snapshot_capture": "requote_entry_plus_pre_snapshot_compute",
                "pre_snapshot_samples": _sample_identity(pre_snapshot_samples),
                "post_snapshot_compute": (
                    "round_total_minus_round_pre_not_independently_sampled"
                ),
                "total_accounting": "pre_plus_post_equals_total_not_added_again",
            },
        )
    if rest_gateway_mode not in {"disabled", "paired_npz", "sampled_serial"}:
        raise ValueError(
            "rest_gateway_timing_mode must be disabled, paired_npz or sampled_serial"
        )
    requote_tail_work_samples, main_loop_work_samples = _local_work_samples(params)
    direct_return_samples = params.get("_serial_rest_return_samples_by_operation")
    direct_return_semantics = str(
        params.get("_serial_rest_return_sample_semantics", "") or ""
    ).strip()
    if direct_return_samples is not None:
        if rest_gateway_mode != "sampled_serial" or params.get("rest_gateway_timing_profile_path"):
            raise ValueError("direct REST-return samples require sampled_serial without a profile")
        if not isinstance(direct_return_samples, Mapping) or set(direct_return_samples) != {
            "new", "cancel"
        }:
            raise ValueError("direct REST-return samples require new and cancel operations")
        if not direct_return_semantics:
            raise ValueError("direct REST-return samples must declare observed or proxy semantics")
        for values in direct_return_samples.values():
            rows = np.asarray(values, dtype=np.float64)
            if (
                rows.ndim != 2 or rows.shape[1] != 3 or rows.shape[0] == 0
                or not np.all(np.isfinite(rows)) or np.any(rows < 0.0)
                or np.any(rows[:, 0] > rows[:, 1]) or np.any(rows[:, 0] > rows[:, 2])
            ):
                raise ValueError("invalid effective/ACK/HTTP REST-return sample triples")
    rest_gateway_identity: dict[str, Any] | None = None
    if rest_gateway_mode == "paired_npz":
        profile_path = _resolve_path(
            params.get("rest_gateway_timing_profile_path"),
            root=project_root,
        )
        rest_gateway_identity = {
            "mode": rest_gateway_mode,
            "evidence_scope": "diagnostic_only",
            "sampling_unit": "whole_observed_request_row",
            "request_slot_order": [
                "cancel_buy",
                "cancel_sell",
                "new_buy",
                "new_sell",
            ],
            "exact_request_mask_required": True,
            "profile": {
                "path": str(profile_path or ""),
                "sha256": sha256_file(profile_path),
            },
            "seed": int(params.get("rest_gateway_timing_seed", latency_seed) or latency_seed),
        }
    elif rest_gateway_mode == "sampled_serial":
        rest_gateway_identity = {
            "mode": rest_gateway_mode,
            "evidence_scope": "diagnostic_only",
            "sampling_unit": "independent_request_with_paired_effective_ack",
            "request_slot_order": [
                "cancel_buy", "cancel_sell", "new_buy", "new_sell"
            ],
            "request_start": "max_decision_ready_previous_local_ack",
            "joint_request_replay": False,
            "sample_identity_source": "latency_lifecycle_samples",
            "seed": latency_seed,
        }
        profile_path = _resolve_path(
            params.get("rest_gateway_timing_profile_path"), root=project_root
        )
        if profile_path is not None:
            rest_gateway_identity.update(
                {
                    "sampling_unit": "independent_request_with_paired_effective_private_return",
                    "request_start": "max_decision_ready_previous_rest_return",
                    "response_clock_semantics": "paired_observed_upper_bound",
                    "cancel_continuation": "skip_new_if_not_terminal_at_rest_return",
                    "sample_identity_source": "profile",
                    "profile": {
                        "path": str(profile_path),
                        "sha256": sha256_file(profile_path),
                    },
                }
            )
        elif direct_return_samples is not None:
            rest_gateway_identity.update(
                sampling_unit="independent_request_with_paired_effective_private_return",
                request_start="max_decision_ready_previous_rest_return",
                response_clock_semantics=direct_return_semantics,
                cancel_continuation="skip_new_if_not_terminal_at_rest_return",
                sample_identity_source="operation_pooled_direct_samples",
                sample_columns=["exchange_effective_ms", "local_ack_ms", "http_return_ms"],
                operation_samples={
                    operation: _sample_identity(values)
                    for operation, values in sorted(direct_return_samples.items())
                },
            )
    diagnostic_latency_selected = bool(
        private_fill_visibility_enabled
        or decision_to_gateway_identity is not None
        or rest_gateway_identity is not None
    )
    visibility_mode = str(
        params.get("exec_book_visibility_mode", "sampled") or "sampled"
    ).strip().lower()
    visibility_identity: dict[str, Any] | None = None
    visibility_promotion_eligible = True
    if visibility_mode == "sampled_joint":
        profile_path = _resolve_path(
            params.get("exec_book_visibility_delay_profile_path"),
            root=project_root,
        )
        source_identity = {
            "path": str(profile_path or ""),
            "sha256": sha256_file(profile_path),
            "profile_id": str(
                params.get("exec_book_visibility_delay_profile_id", "") or ""
            ),
        }
        raw_inputs: dict[str, np.ndarray] = {}
        inputs_valid = True
        for name, param_name in (
            ("book", "_exec_book_visibility_paired_delay_ms"),
            ("depth", "_exec_depth_visibility_paired_delay_ms"),
            ("trade", "_exec_trade_visibility_paired_delay_ms"),
        ):
            try:
                values = np.asarray(params.get(param_name, ()), dtype=np.float64)
            except (TypeError, ValueError):
                values = np.empty(0, dtype=np.float64)
            raw_inputs[name] = values
            inputs_valid = bool(
                inputs_valid
                and values.ndim == 1
                and values.size > 0
                and np.all(np.isfinite(values))
                and np.all(values >= 0.0)
            )
        aligned = len({values.size for values in raw_inputs.values()}) == 1
        complete = bool(
            source_identity["sha256"]
            and source_identity["profile_id"]
            and inputs_valid
            and aligned
        )
        visibility_identity = {
            "mode": visibility_mode,
            "evidence_scope": "formal_eligible" if complete else "diagnostic_only",
            "source_profile": source_identity,
            "inputs": {
                name: _sample_identity(values)
                for name, values in raw_inputs.items()
            },
        }
        visibility_promotion_eligible = complete
    elif visibility_mode == "profile_source_stratified":
        profile_path = _resolve_path(
            params.get("exec_source_stratified_profile_path"),
            root=project_root,
        )
        visibility_identity = {
            "mode": visibility_mode,
            "evidence_scope": "diagnostic_only",
            "source_profile": {
                "path": str(profile_path or ""),
                "sha256": sha256_file(profile_path),
                "declared_sha256": str(
                    params.get("exec_source_stratified_profile_sha256", "") or ""
                ).lower(),
                "profile_id": str(
                    params.get("exec_source_stratified_profile_id", "") or ""
                ),
                "market_id": str(
                    params.get("exec_source_stratified_profile_market_id", "")
                    or ""
                ),
                "transport": str(
                    params.get("exec_source_stratified_profile_transport", "")
                    or ""
                ).lower(),
            },
        }
        visibility_promotion_eligible = False
    elif visibility_mode == "message_schedule":
        profile_path = _resolve_path(
            params.get("exec_message_delivery_profile_path"), root=project_root
        )
        visibility_identity = {
            "mode": visibility_mode,
            "evidence_scope": "diagnostic_only",
            "sampling_unit": "assigned_once_per_source_message",
            "visibility_boundary": "feature_ready_ns_strictly_before_local_now",
            "head_of_line": "within_declared_connection_order",
            # The caller's existing execution recipe belongs here: source
            # assignment, callback FIFO, parent mapping and policy producer.
            # Do not infer these from arrays or add another leaf-hash chain.
            "input_semantics": dict(
                params.get("exec_message_delivery_input_semantics", {})
            ),
            "max_exec_book_visible_age_s": float(
                params.get("max_exec_book_visible_age_s", 5.0)
            ),
            "max_exec_book_source_lag_s": float(
                params.get("max_exec_book_source_lag_s", 5.0)
            ),
            "seed": int(params.get("exec_book_visibility_delay_seed", 0) or 0),
        }
        if profile_path is not None:
            visibility_identity["source_profile"] = {
                "path": str(profile_path),
                "sha256": sha256_file(profile_path),
            }
        visibility_promotion_eligible = False
    for key, default in (
        ("maker_fee", 0.0), ("taker_fee", 0.00036),
        ("tick_size", 0.1), ("lot_size", 0.001),
        ("min_qty", None), ("min_notional", None),
    ):
        value = params.get(key, default)
        if value is None and key in {"min_qty", "min_notional"}:
            continue
        value = float(value)
        if not math.isfinite(value):
            raise ValueError(f"{key} must be finite")
        if key in {"tick_size", "lot_size"} and value <= 0.0:
            raise ValueError(f"{key} must be positive")
        if key in {"min_qty", "min_notional"} and value < 0.0:
            raise ValueError(f"{key} must be non-negative")
    hard_risk_limits = replay_hard_risk_limits(params)
    contract = {
        "schema_version": FORMAL_REPLAY_CONTRACT_SCHEMA,
        "purpose": normalized_purpose,
        "promotion_eligible": bool(
            normalized_purpose == "formal"
            and str(params.get("latency_scenario", "baseline")) == "baseline"
            and not bool(params.get("queue_calibration_diagnostic_only", False))
            and not individual_trades_diagnostic
            and sync_promotion_eligible
            and visibility_promotion_eligible
            and not diagnostic_latency_selected
        ),
        "artifacts": artifacts,
        "p3": {
            "schema_version": str(params.get("fill_probability_schema_version", "")),
            "model_type": str(params.get("fill_probability_model_type", "")),
            "event_type": str(params.get("fill_probability_event_type", "")),
            "horizon_s": float(params.get("fill_probability_horizon_s", 0.0) or 0.0),
            "distance_origin": str(
                params.get("fill_probability_distance_origin", "")
            ),
            "distance_unit": str(params.get("fill_probability_distance_unit", "")),
            "side": str(params.get("fill_probability_side", "")),
            "queue_included": params.get("fill_probability_queue_included"),
            "artifact_sha256": str(
                params.get("fill_probability_artifact_sha256", "") or ""
            ),
            "delta_star": float(params.get("p3_delta_star", 0.0) or 0.0),
            "kappa_eff": float(params.get("p3_kappa_eff", 0.0) or 0.0),
            "historical_scalar_adapter_enabled": historical_p3_adapter,
            "side_bbo_floor_enabled": p3_side_bbo_floor,
            "consumer_mode": (
                "historical_pair_projection"
                if historical_p3_adapter
                else "same_side_bbo_floor"
                if p3_side_bbo_floor
                else "inactive"
            ),
        },
        "quote_unit_contract": {
            "formula_identity": "as_shaped_empirical_controller",
            "quote_math_mode": str(
                params.get("quote_math_mode", "legacy_v0") or "legacy_v0"
            ),
            "inventory_unit": "base_asset",
            "normalized_inventory_unit": "dimensionless",
            "price_unit": "quote_asset_per_base_asset",
            "variance_rate_unit": "price_squared_per_second",
            "duration_unit": "second",
            "eta_inventory_unit": "inverse_price",
            "a_spread_unit": "inverse_price",
            "risk_per_order_unit": "inverse_price",
            "execution_intensity_slope_unit": "inverse_price",
            "inventory_reference_qty": inventory_reference_qty,
            "order_size": float(params.get("order_size", 0.0) or 0.0),
            "eta_inventory": float(eta_inventory),
            "a_spread": float(a_spread),
            "risk_per_order": float(risk_per_order),
            "execution_intensity_slope": float(execution_intensity_slope),
            "quote_horizon_s": quote_horizon_s,
            "risk_horizon_s": float(risk_horizon_s),
        },
        "queue": {
            "schema_version": str(params.get("queue_calibration_schema_version", "")),
            "apply_mode": str(params.get("queue_calibration_apply_mode", "")),
            "fit_days": list(params.get("queue_calibration_fit_days") or []),
            "diagnostic_only": bool(params.get("queue_calibration_diagnostic_only", False)),
            "diagnostic_parent_sha256": str(
                params.get("queue_calibration_diagnostic_parent_sha256", "") or ""
            ),
            "diagnostic_note": str(params.get("queue_calibration_diagnostic_note", "") or ""),
            "replay_params": {
                key: (
                    float(params[key])
                    if key in params and math.isfinite(float(params[key]))
                    else None
                )
                for key in _FROZEN_QUEUE_KEYS
            },
        },
        "causal_event_semantics": {
            "execution_trade_source": str(
                params.get("execution_trade_source", "individual_trades")
            ),
            "replay_event_clock": str(params.get("replay_event_clock", "trade")),
            "replay_clock_interval_ms": int(params.get("replay_clock_interval_ms", 0) or 0),
            "require_historical_bbo": bool(params.get("require_historical_bbo", False)),
            "require_formal_l2": bool(params.get("require_formal_l2", False)),
            "verify_formal_l2_hashes": bool(params.get("verify_formal_l2_hashes", False)),
            "feature_visibility": (
                "feature_ready_ns_strictly_before_local_now"
                if visibility_mode == "message_schedule"
                else "feature_ready_ts_lte_decision_ts"
            ),
            "queue_event_visibility": "exchange_time_causal",
            "terminal_equity": "cash_plus_inventory_times_terminal_mark",
            "hypothetical_terminal_taker_fee_in_final_pnl": False,
            "execution": {
                "maker_fee": float(params.get("maker_fee", 0.0)),
                "taker_fee": float(params.get("taker_fee", 0.00036)),
                "fee_rate_unit": "fraction_of_fill_price_times_filled_base_quantity",
                "fee_sign": "positive_cost_negative_rebate",
                "tick_size": float(params.get("tick_size", 0.1)),
                "tick_size_unit": "quote_per_base",
                "lot_size": float(params.get("lot_size", 0.001)),
                "lot_size_unit": "base_quantity",
                "rounding": "side_aware_price_tick_and_quantity_lot_on_order_paths",
                "min_qty": (
                    float(params["min_qty"]) if params.get("min_qty") is not None else None
                ),
                "min_notional": (
                    float(params["min_notional"])
                    if params.get("min_notional") is not None else None
                ),
                "min_qty_unit": "base_quantity",
                "min_notional_unit": "quote_currency",
                "exchange_filter_limitations": (
                    "min_qty_and_min_notional_are_declared_not_enforced_by_tick_replay; "
                    "full_exchange_filter_and_fee_tier_simulation_not_implemented"
                ),
            },
            "exec_book_visibility_mode": visibility_mode,
            "exec_depth_visibility_source_offset_ms": int(
                params.get("exec_depth_visibility_source_offset_ms", 0) or 0
            ),
            "fill_cooldown_consecutive_reset_policy": str(
                params.get("fill_cooldown_consecutive_reset_policy", "") or ""
            ),
        },
        "path_dependent_controls": {
            "hard_risk_limits": {
                key: value if math.isfinite(value) else None
                for key, value in zip(
                    ("max_daily_loss", "max_position_value", "emergency_close_dd"),
                    hard_risk_limits,
                    strict=True,
                )
            },
            "consecutive_loss_cooldown": {
                "semantics_version": str(
                    params.get(
                        "consecutive_loss_cooldown_semantics",
                        LOSS_COOLDOWN_SEMANTICS,
                    )
                    or ""
                ),
                "max_consecutive_losses": int(
                    params.get("max_consecutive_losses", 0) or 0
                ),
                "cooldown_after_loss_s": float(
                    params.get("cooldown_after_loss", 0.0) or 0.0
                ),
                "round_trip_clock": "full_close_or_flip_then_next_policy_clock",
            },
            "dynamic_fill_hazard_q90": {
                "enabled": q90_enabled,
                "side": "BUY",
                "replay_authority": (
                    "python_native_exchange_book" if q90_enabled else "disabled"
                ),
                "model_artifact": artifacts["dynamic_fill_hazard_model"],
                "policy_artifact": artifacts["dynamic_fill_hazard_policy"],
                "exchange_book_queue_mode": str(
                    params.get("exchange_book_queue_mode", "disabled") or "disabled"
                ),
                "native_exchange_book_root": str(
                    params.get("native_exchange_book_root", "") or ""
                ),
                "native_exchange_book_warmup_hours": int(
                    params.get("native_exchange_book_warmup_hours", 0) or 0
                ),
                "daily_source_identity": "sha256_per_snapshot_delta_file",
                "pending_cancel_fillable": True,
                "reentry": "after_cancel_ack_and_score_recovery",
            },
            "sync_adjust_degrade": {
                "config_enabled": sync_enabled,
                "mode": sync_mode,
                "semantics_version": str(
                    params.get("sync_adjust_semantics", SYNC_DEGRADE_SEMANTICS)
                    or ""
                ),
                "pause_s": float(params.get("sync_adjust_pause_s", 0.0) or 0.0),
                "cancel_orders": bool(
                    params.get("sync_adjust_cancel_orders", True)
                ),
                "environment": str(
                    artifacts["sync_adjust_event_tape"].get(
                        "environment", ""
                    )
                    or ""
                ),
                "declared_environment": str(
                    params.get("sync_adjust_event_environment", "") or ""
                ),
                "event_tape": artifacts["sync_adjust_event_tape"],
                "promotion_eligible": sync_promotion_eligible,
            },
        },
        "initial_state": {
            "mode": normalized_initial,
            "artifact_path": str(state_path or ""),
            "artifact_sha256": sha256_file(state_path),
            "initial_inventory": float(params.get("initial_inventory", 0.0) or 0.0),
            "initial_entry_price": float(params.get("initial_entry_price", 0.0) or 0.0),
        },
        "latency": {
            "profile_id": str(params.get("latency_profile_id", "")),
            "environment": params.get("latency_environment", ""),
            "scenario": str(params.get("latency_scenario", "baseline")),
            "sampler_version": str(params.get("latency_sampler_version", "")),
            "rng_seed": int(params.get("rng_seed", 42)),
            "latency_seed": latency_seed,
            "exec_book_visibility_seed": int(params.get("exec_book_visibility_delay_seed", 0) or 0),
            "market_data_profile": {
                "path": str(params.get("market_data_latency_profile_path", "")),
                "sha256": str(params.get("market_data_latency_profile_sha256", "")),
                "profile_id": str(params.get("market_data_latency_profile_id", "")),
                "environment": params.get("market_data_latency_environment", {}),
                "mode": str(params.get("market_data_latency_mode", "exchange_zero")),
                "seed": int(params.get("market_data_latency_seed", 7) or 7),
            },
            "new_order_samples": _sample_identity(params.get("_new_order_latency_samples_ms", ())),
            "cancel_order_samples": _sample_identity(
                params.get("_cancel_order_latency_samples_ms", ())
            ),
            "exec_book_visibility_samples": _sample_identity(
                params.get("_exec_book_visibility_delay_samples_ms", ())
            ),
            "new_order_fixed_ms": float(params.get("new_order_latency_ms", 0.0) or 0.0),
            "cancel_order_fixed_ms": float(params.get("cancel_order_latency_ms", 0.0) or 0.0),
            "jitter_ms": float(params.get("latency_jitter_ms", 0.0) or 0.0),
            "baseline_clip_quantile": float(
                params.get("latency_baseline_clip_quantile", 0.99) or 0.99
            ),
            "stress_spike_probability": float(
                params.get("latency_stress_spike_probability", 0.001) or 0.0
            ),
            "stress_spike_multiplier": float(
                params.get("latency_stress_spike_multiplier", 5.0) or 1.0
            ),
            "rare_spike_policy": str(params.get("latency_rare_spike_policy", "stress_only")),
        },
        "live_alignment_scope": (
            "unit_clock_state_machine_gate_order_diagnostics_only"
            if normalized_purpose == "live_alignment"
            else "not_applicable"
        ),
        "live_alignment_not_required": [
            "campaign_identity",
            "per_event_fill_identity",
            "daily_pnl_identity",
        ],
    }
    if visibility_identity is not None:
        contract["causal_event_semantics"][
            "exec_book_visibility_identity"
        ] = visibility_identity
    main_loop_sleep_ms = float(params.get("replay_main_loop_sleep_ms", 0) or 0)
    if main_loop_sleep_ms:
        contract["causal_event_semantics"]["main_loop"] = {
            "replay_main_loop_sleep_ms": main_loop_sleep_ms,
            "wake_clock": "actual_tick_and_rest_return_then_sleep",
            "requote_anchor": "actual_requote_start",
            "dynamic_requote_clock": (
                "delivered_1s_bars_before_due_check"
                if visibility_mode == "message_schedule"
                else "source_1s_bars_before_due_check"
            ),
        }
        if np.any(requote_tail_work_samples > 0.0) or np.any(main_loop_work_samples > 0.0):
            local_work: dict[str, Any] = {
                "backend": "python_only",
                "evidence_scope": "diagnostic_only",
                "scope": "total_local_work_not_exact_inter_request_gaps",
                "accounting": "first_gateway_compute_and_rest_not_recharged",
                "clock": "before_tick_then_tick_and_tail_then_after_tick_then_sleep",
            }
            if np.any(requote_tail_work_samples > 0.0):
                local_work["requote_tail"] = {
                    "samples": _sample_identity(requote_tail_work_samples),
                    "sampling": "same_total_sample_index_and_seed_per_requote_entry",
                    "seed": decision_to_gateway_latency_seed,
                    "clock": "after_last_http_return_or_no_request_compute",
                }
            if np.any(main_loop_work_samples > 0.0):
                local_work["loop"] = {
                    "samples": _sample_identity(main_loop_work_samples),
                    "row_count": int(main_loop_work_samples.shape[0]),
                    "columns": ["before_tick_ms", "after_tick_ms"],
                    "sampling": "one_keyed_paired_row_per_loop_start",
                    "seed": decision_to_gateway_latency_seed,
                }
            contract["causal_event_semantics"]["main_loop"]["local_work"] = local_work
    if private_fill_visibility_enabled:
        contract["latency"]["private_fill_visibility"] = {
            "evidence_scope": "diagnostic_only",
            "clock": "exchange_fill_to_local_private_callback_visibility",
            "samples": _sample_identity(private_fill_samples),
        }
    if decision_to_gateway_identity is not None:
        contract["latency"]["decision_to_gateway"] = (
            decision_to_gateway_identity
        )
    if rest_gateway_identity is not None:
        contract["latency"]["serial_rest_gateway"] = rest_gateway_identity
    # Optional split-lifecycle identities are emitted only when the caller
    # actually selects the new model.  Merely upgrading the executor must not
    # churn every legacy B0 contract/root hash with empty fields.
    for param_name, contract_name in (
        (
            "_new_order_exchange_effective_latency_samples_ms",
            "new_order_exchange_effective_samples",
        ),
        (
            "_cancel_exchange_effective_latency_samples_ms",
            "cancel_exchange_effective_samples",
        ),
        (
            "_cancel_ack_visibility_latency_samples_ms",
            "cancel_ack_visibility_samples",
        ),
    ):
        if _finite_samples(params.get(param_name, ())).size:
            contract["latency"][contract_name] = _sample_identity(
                params.get(param_name, ())
            )
    contract["contract_sha256"] = hashlib.sha256(_canonical_bytes(contract)).hexdigest()
    return contract


def _formal_contract_errors(params: Mapping[str, Any], contract: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    purpose = str(contract.get("purpose", ""))
    initial = contract.get("initial_state", {})
    latency = contract.get("latency", {})
    causal = contract.get("causal_event_semantics", {})
    artifacts = contract.get("artifacts", {})
    p3_contract = contract.get("p3", {})
    quote_units = contract.get("quote_unit_contract", {})
    controls = contract.get("path_dependent_controls", {})
    loss_control = controls.get("consecutive_loss_cooldown", {})
    q90_control = controls.get("dynamic_fill_hazard_q90", {})
    sync_control = controls.get("sync_adjust_degrade", {})
    private_fill_visibility = latency.get("private_fill_visibility") or {}
    decision_to_gateway = latency.get("decision_to_gateway") or {}
    serial_rest_gateway = latency.get("serial_rest_gateway") or {}
    if private_fill_visibility and purpose != "diagnostic":
        errors.append(
            "private-fill visibility latency is diagnostic-only and requires "
            "replay purpose diagnostic"
        )
    if decision_to_gateway and purpose != "diagnostic":
        errors.append(
            "decision-to-gateway compute latency is diagnostic-only and "
            "requires replay purpose diagnostic"
        )
    if serial_rest_gateway and purpose != "diagnostic":
        errors.append(
            "serial REST gateway timing is diagnostic-only and requires "
            "replay purpose diagnostic"
        )
    if (
        serial_rest_gateway.get("mode") == "paired_npz"
        or "profile" in serial_rest_gateway
    ) and not str(
        (serial_rest_gateway.get("profile") or {}).get("sha256", "") or ""
    ):
        errors.append("paired serial REST gateway timing profile identity is missing")
    if purpose == "formal":
        p3_delta_star = float(p3_contract.get("delta_star", 0.0) or 0.0)
        p3_kappa_eff = float(p3_contract.get("kappa_eff", 0.0) or 0.0)
        if p3_delta_star > 0.0 or p3_kappa_eff > 0.0:
            try:
                from strategy.quote_core import validate_p3_touch_identity

                validate_p3_touch_identity(
                    {
                        "event_type": p3_contract.get("event_type"),
                        "horizon_s": p3_contract.get("horizon_s"),
                        "distance_origin": p3_contract.get("distance_origin"),
                        "distance_unit": p3_contract.get("distance_unit"),
                        "side": p3_contract.get("side"),
                        "queue_included": p3_contract.get("queue_included"),
                        "artifact_sha256": p3_contract.get("artifact_sha256"),
                    },
                    require_artifact_hash=True,
                )
            except ValueError as exc:
                errors.append(f"formal P3 identity is incomplete: {exc}")
            historical_adapter = bool(
                p3_contract.get("historical_scalar_adapter_enabled", False)
            )
            side_bbo_floor = bool(p3_contract.get("side_bbo_floor_enabled", False))
            if historical_adapter == side_bbo_floor:
                errors.append(
                    "formal P3 consumer must select exactly one explicit projection mode"
                )
        for name in (
            "inventory_reference_qty",
            "order_size",
            "eta_inventory",
            "a_spread",
            "risk_per_order",
            "execution_intensity_slope",
            "quote_horizon_s",
            "risk_horizon_s",
        ):
            try:
                value = float(quote_units.get(name, 0.0))
            except (TypeError, ValueError):
                value = math.nan
            if not math.isfinite(value) or value <= 0.0:
                errors.append(
                    f"formal quote unit contract requires positive finite {name}"
                )
        if str(quote_units.get("formula_identity", "")) != (
            "as_shaped_empirical_controller"
        ):
            errors.append("formal quote formula identity is missing or invalid")
        if str(quote_units.get("quote_math_mode", "")) not in {
            "legacy_v0",
            "quantity_aware_v1",
        }:
            errors.append("formal quote math mode is missing or invalid")
        expected_unit_identities = {
            "inventory_unit": "base_asset",
            "normalized_inventory_unit": "dimensionless",
            "price_unit": "quote_asset_per_base_asset",
            "variance_rate_unit": "price_squared_per_second",
            "duration_unit": "second",
            "eta_inventory_unit": "inverse_price",
            "a_spread_unit": "inverse_price",
            "risk_per_order_unit": "inverse_price",
            "execution_intensity_slope_unit": "inverse_price",
        }
        for name, expected in expected_unit_identities.items():
            if str(quote_units.get(name, "")) != expected:
                errors.append(
                    f"formal quote unit contract requires {name}={expected}"
                )
        for name in ("config", "p3", "queue"):
            if not str((artifacts.get(name) or {}).get("sha256", "")):
                errors.append(f"formal replay requires a frozen {name} artifact hash")
        model_identity = artifacts.get("model") or {}
        if str(model_identity.get("path", "")) and not str(model_identity.get("sha256", "")):
            errors.append("configured model directory has no frozen deployable artifacts")
        if bool(params.get("buy_fill_selection_live_enabled", False)) and not str(
            (artifacts.get("buy_fill_selection") or {}).get("sha256", "")
        ):
            errors.append("enabled BUY fill-selection scorer artifact is missing")
        if _is_individual_trade_source(causal.get("execution_trade_source")):
            trades_identity = artifacts.get("individual_trades") or {}
            if not str(trades_identity.get("manifest_sha256", "")):
                errors.append("formal individual-trades replay requires a frozen manifest")
            elif str(trades_identity.get("status", "")) != "verified":
                errors.append(
                    "formal individual-trades identity is not verified: "
                    f"{trades_identity.get('status', 'unknown')}"
                )
        queue_params = (contract.get("queue") or {}).get("replay_params", {})
        if any(queue_params.get(key) is None for key in _FROZEN_QUEUE_KEYS):
            errors.append("queue artifact runtime parameters are incomplete")
        if causal.get("replay_event_clock") != "merged":
            errors.append("formal replay requires the merged causal event clock")
        try:
            from strategy.fill_cooldown import normalize_consecutive_reset_policy

            normalize_consecutive_reset_policy(
                causal.get("fill_cooldown_consecutive_reset_policy"),
                require_explicit=True,
            )
        except ValueError as exc:
            errors.append(str(exc))
        visibility_mode = str(causal.get("exec_book_visibility_mode", ""))
        visibility_identity = causal.get("exec_book_visibility_identity") or {}
        if visibility_mode == "paired":
            errors.append("paired live visibility is live_alignment-only")
        elif visibility_mode in {"profile_source_stratified", "message_schedule"}:
            errors.append(
                f"{visibility_mode} visibility is diagnostic-only"
            )
        elif visibility_mode == "sampled_joint" and str(
            visibility_identity.get("evidence_scope", "")
        ) != "formal_eligible":
            errors.append(
                "formal sampled_joint visibility requires a complete frozen "
                "source/input identity"
            )
        if int(causal.get("exec_depth_visibility_source_offset_ms", 0) or 0) != 0:
            errors.append("source-time boundary offsets are live_alignment-only")
        if params.get("initial_live_state"):
            errors.append("full live warm-start state is live_alignment-only")
        if params.get("_empirical_requote_ts_ms") is not None:
            try:
                if len(params.get("_empirical_requote_ts_ms")):
                    errors.append("empirical live requote clocks are live_alignment-only")
            except TypeError:
                errors.append("empirical live requote clocks are live_alignment-only")
        if initial.get("mode") == "fresh_start":
            if abs(float(initial.get("initial_inventory", 0.0) or 0.0)) > 1e-12:
                errors.append("fresh-start replay requires zero initial inventory")
            if abs(float(initial.get("initial_entry_price", 0.0) or 0.0)) > 1e-12:
                errors.append("fresh-start replay requires zero initial entry price")
        elif not str(initial.get("artifact_sha256", "")):
            errors.append("frozen standard initial state requires an artifact hash")
        if str(latency.get("scenario", "")) == "stress" and contract.get(
            "promotion_eligible", True
        ):
            errors.append("latency stress runs cannot be promotion evidence")
        loss_limit = int(loss_control.get("max_consecutive_losses", 0) or 0)
        loss_pause = float(loss_control.get("cooldown_after_loss_s", 0.0) or 0.0)
        if (loss_limit > 0) != (loss_pause > 0.0):
            errors.append(
                "consecutive-loss cooldown requires both a positive threshold and duration"
            )
        if (loss_limit > 0 or loss_pause > 0.0) and str(
            loss_control.get("semantics_version", "")
        ) != LOSS_COOLDOWN_SEMANTICS:
            errors.append(
                "consecutive-loss cooldown semantics identity is invalid"
            )

        if bool(q90_control.get("enabled", False)):
            if not bool(params.get("dynamic_fill_hazard_shadow_enabled", False)):
                errors.append("BUY q90 action requires the dynamic hazard model")
            if str(q90_control.get("exchange_book_queue_mode", "")) != "strict":
                errors.append("BUY q90 formal replay requires strict native exchange-book mode")
            if not bool(params.get("require_formal_l2", False)):
                errors.append("BUY q90 formal replay requires the frozen formal L2 identity")
            if not str((artifacts.get("formal_l2") or {}).get("manifest_sha256", "")):
                errors.append("BUY q90 formal replay is missing the formal L2 manifest hash")
            native_root = Path(
                str(q90_control.get("native_exchange_book_root", "") or "")
            ).expanduser()
            if not str(q90_control.get("native_exchange_book_root", "") or ""):
                errors.append("BUY q90 formal replay requires a native exchange-book root")
            elif not native_root.is_dir():
                errors.append("BUY q90 native exchange-book root does not exist")
            if int(q90_control.get("native_exchange_book_warmup_hours", 0) or 0) < 0:
                errors.append("BUY q90 native exchange-book warmup hours are invalid")
            for name in (
                "dynamic_fill_hazard_model",
                "dynamic_fill_hazard_policy",
            ):
                identity = artifacts.get(name) or {}
                actual = str(identity.get("sha256", "") or "").lower()
                declared = str(identity.get("declared_sha256", "") or "").lower()
                if not actual or not declared or actual != declared:
                    errors.append(f"{name} artifact hash is missing or mismatched")

        sync_mode = str(sync_control.get("mode", "") or "")
        sync_enabled = bool(sync_control.get("config_enabled", False))
        if sync_mode not in {"disabled", "frozen_tape", "censor", "stress"}:
            errors.append("sync-adjust replay mode is invalid")
        if sync_enabled and sync_mode == "disabled":
            errors.append(
                "formal replay cannot omit the enabled live sync-adjust control"
            )
        if not sync_enabled and sync_mode != "disabled":
            errors.append(
                "sync-adjust events cannot run while the live control is disabled"
            )
        if sync_mode != "disabled" and str(
            sync_control.get("semantics_version", "")
        ) != SYNC_DEGRADE_SEMANTICS:
            errors.append("sync-adjust event ordering semantics identity is invalid")
        if sync_mode in {"frozen_tape", "censor"}:
            tape = sync_control.get("event_tape") or {}
            actual = str(tape.get("sha256", "") or "").lower()
            declared = str(tape.get("declared_sha256", "") or "").lower()
            if not actual or not declared or actual != declared:
                errors.append("sync-adjust event tape hash is missing or mismatched")
            environment = str(sync_control.get("environment", "") or "")
            declared_environment = str(
                sync_control.get("declared_environment", "") or ""
            )
            if not environment or not declared_environment:
                errors.append("sync-adjust event tape environment identity is missing")
            elif environment != declared_environment:
                errors.append("sync-adjust event tape environment identity is mismatched")
            if str(tape.get("schema_version", "") or "") != SYNC_DEGRADE_TAPE_SCHEMA:
                errors.append("sync-adjust event tape schema identity is invalid")
            coverage_start = int(tape.get("start_ts_ms", 0) or 0)
            coverage_end = int(tape.get("end_ts_ms", 0) or 0)
            if coverage_start <= 0 or coverage_end < coverage_start:
                errors.append("sync-adjust event tape coverage identity is invalid")
        if sync_mode in {"censor", "stress"} and contract.get(
            "promotion_eligible", True
        ):
            errors.append("sync-adjust censor/stress runs cannot be promotion evidence")
    if str(latency.get("sampler_version", "")) != KEYED_LATENCY_SAMPLER_VERSION:
        errors.append(f"latency sampler must be {KEYED_LATENCY_SAMPLER_VERSION}")
    if not str(latency.get("profile_id", "")):
        errors.append("latency profile requires an environment/version label")
    if not latency.get("environment"):
        errors.append("latency environment identity is missing")
    if str(latency.get("rare_spike_policy", "")) != "stress_only":
        errors.append("rare latency spikes must be stress-only")
    return errors


def freeze_replay_contract(
    params: MutableMapping[str, Any],
    *,
    purpose: str = "formal",
    initial_state_mode: str = "fresh_start",
    initial_state_artifact: str | Path | None = None,
    root: str | Path | None = None,
) -> dict[str, Any]:
    normalized_initial = str(initial_state_mode or "fresh_start").lower()
    if str(purpose or "formal").lower() == "formal" and params.get("initial_live_state"):
        raise RuntimeError("full live warm-start state is live_alignment-only")
    if normalized_initial == "fresh_start":
        params["initial_inventory"] = 0.0
        params["initial_entry_price"] = 0.0
        params.pop("initial_live_state", None)
    elif normalized_initial == "frozen_standard":
        if initial_state_artifact is None:
            raise ValueError("frozen_standard requires an initial-state artifact")
        params.update(load_standard_initial_state(initial_state_artifact))
        params.pop("initial_live_state", None)
    contract = build_replay_contract(
        params,
        purpose=purpose,
        initial_state_mode=normalized_initial,
        initial_state_artifact=initial_state_artifact,
        root=root,
    )
    errors = _formal_contract_errors(params, contract)
    if errors:
        raise RuntimeError("Replay contract failed: " + "; ".join(errors))
    params["replay_contract"] = contract
    params["replay_contract_sha256"] = contract["contract_sha256"]
    params["replay_purpose"] = contract["purpose"]
    params["replay_initial_state_mode"] = normalized_initial
    params["replay_promotion_eligible"] = bool(contract["promotion_eligible"])
    return contract


def validate_frozen_replay_contract(params: Mapping[str, Any]) -> dict[str, Any]:
    expected = params.get("replay_contract")
    if not isinstance(expected, Mapping):
        raise RuntimeError("formal replay contract is missing")
    initial = expected.get("initial_state", {})
    rebuilt = build_replay_contract(
        params,
        purpose=str(expected.get("purpose", "formal")),
        initial_state_mode=str(initial.get("mode", "fresh_start")),
        initial_state_artifact=str(initial.get("artifact_path", "")) or None,
    )
    errors = _formal_contract_errors(params, rebuilt)
    if rebuilt.get("contract_sha256") != expected.get("contract_sha256"):
        errors.append(
            "runtime config/model/P3/queue/causal/latency identity differs from the frozen contract"
        )
    if errors:
        raise RuntimeError("Replay contract failed: " + "; ".join(errors))
    return rebuilt


def write_replay_contract(contract: Mapping[str, Any], path: str | Path) -> Path:
    output = Path(path).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(contract, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return output
