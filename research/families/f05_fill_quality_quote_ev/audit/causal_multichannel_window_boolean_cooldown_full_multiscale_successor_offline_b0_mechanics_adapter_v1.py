#!/usr/bin/env python3
"""Canonical outcome-blind B0 mechanics adapter for the F05 offline panel.

This module materializes one target day from already admitted source manifests.
It executes the current owner policy in the Python modeled-queue replay through
the target day and its D+1 continuation context, but it neither reads nor
returns replay economics, labels, one-shot outcomes, or candidate actions.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import tempfile
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import yaml

from live import runtime_policy as live_runtime_policy
from models import backtest_tick as bt
from models.backtest_config import load_tick_base_params
from research.families.f03_causal_13_head.audit import (
    causal_v12_v9_10s_control_overlay_repair as control_overlay,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_panel_builder_v1 as panel_builder,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_v1 as offline,
)

IDENTITY = panel_builder.CANONICAL_ADAPTER_IDENTITY
RESULT_SCHEMA = panel_builder.ADAPTER_RESULT_SCHEMA
BLOCKED_STATUS = "blocked_missing_canonical_fields"
ROOT = Path(__file__).resolve().parents[4]

_RESEARCH_ACTION_FLAGS = (
    "buy_soft_widen_release_probe_enabled",
    "conditional_p3_reach_gate_enabled",
    "conditional_p3_reach_budget_policy_enabled",
    "cooldown_duration_fork_enabled",
    "ema_add_wait_fork_enabled",
    "local_action_ope_enabled",
    "queue_value_keep_cancel_enabled",
    "safe_add_rearm_randomized_enabled",
    "sell_add_skip_ope_enabled",
    "state_conditioned_quote_policy_enabled",
    "state_conditioned_rearm_enabled",
    "variance_time_lineage_randomized_enabled",
)


class OfflineB0MechanicsAdapterError(RuntimeError):
    """Raised when the fixed adapter or its replay identity drifts."""


class BlockedMissingCanonicalFields(OfflineB0MechanicsAdapterError):
    """Fail-closed result for absent canonical bytes or required semantics."""

    status = BLOCKED_STATUS

    def __init__(self, utc_day: str, fields: Sequence[str]) -> None:
        unique = tuple(sorted({str(field) for field in fields if str(field).strip()}))
        if not unique:
            raise ValueError("blocked canonical-field census cannot be empty")
        self.utc_day = str(utc_day)
        self.fields = unique
        super().__init__(f"{self.status}: {self.utc_day}: {','.join(self.fields)}")

    def as_dict(self) -> dict[str, Any]:
        return {
            "identity": IDENTITY,
            "status": self.status,
            "utc_day": self.utc_day,
            "missing_canonical_fields": list(self.fields),
            "economic_outcomes_read": False,
            "labels_read": False,
            "candidate_actions_generated": False,
        }


@dataclass(frozen=True, slots=True)
class _SourceArtifact:
    path: Path
    sha256: str
    size_bytes: int


@dataclass(frozen=True, slots=True)
class _ReplayInputs:
    utc_day: str
    continuation_day: str
    trades: pd.DataFrame
    var_ts_ms: np.ndarray
    var_ssq: np.ndarray
    var_ti: np.ndarray
    var_retsq: np.ndarray
    bbo_data: Any
    l2_data: Any
    ml_data: tuple[Any, ...]
    params: Mapping[str, Any]
    market_window_identity_sha256: str
    model_overlay_identity_sha256: str
    latency_identity_sha256: str
    queue_random_identity_sha256: str
    replay_input_receipt_sha256: str


@dataclass(frozen=True, slots=True)
class _OfflineParams:
    params: Mapping[str, Any]
    raw_mapping_sha256: str
    projected_mapping_sha256: str
    changed_paths: tuple[str, ...]


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _identity_value(value: Any) -> Any:
    """Project frozen replay parameters into a canonical JSON identity."""

    if isinstance(value, np.ndarray):
        return [_identity_value(item) for item in value.tolist()]
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, Mapping):
        return {
            str(key): _identity_value(item)
            for key, item in sorted(value.items(), key=lambda row: str(row[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_identity_value(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    raise OfflineB0MechanicsAdapterError(
        f"unsupported replay identity value: {type(value).__name__}"
    )


def _parameter_identity(
    params: Mapping[str, Any],
    *,
    schema_version: str,
    names: Sequence[str],
) -> str:
    return _canonical_sha256(
        {
            "schema_version": schema_version,
            "parameters": {
                name: _identity_value(params.get(name)) for name in names
            },
        }
    )


_LATENCY_IDENTITY_PARAMETERS = (
    "new_order_latency_ms",
    "cancel_order_latency_ms",
    "latency_jitter_ms",
    "latency_seed",
    "latency_sampler_version",
    "latency_stress_enabled",
    "latency_stress_spike_probability",
    "latency_stress_spike_multiplier",
    "latency_profile_id",
    "latency_environment",
    "latency_scenario",
    "live_perf_latency_mode",
    "_new_order_latency_samples_ms",
    "_cancel_order_latency_samples_ms",
    "_exec_book_visibility_delay_samples_ms",
    "exec_book_visibility_delay_mean_ms",
)

_QUEUE_RANDOM_IDENTITY_PARAMETERS = (
    "rng_seed",
    "queue_ahead_mode",
    "queue_l2_cancel_ahead_enabled",
    "exchange_book_queue_mode",
    "queue_base",
    "queue_decay",
    "maker_fill_prob",
    "buy_fill_prob",
    "sell_fill_prob",
    "queue_ahead_buy_exposure_mult",
    "queue_ahead_buy_reducing_mult",
    "queue_ahead_sell_exposure_mult",
    "queue_ahead_sell_reducing_mult",
    "queue_regime_calibration_enabled",
    "_queue_calibration",
)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path, *, field: str, utc_day: str) -> dict[str, Any]:
    if not path.is_file():
        raise BlockedMissingCanonicalFields(utc_day, (field,))
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BlockedMissingCanonicalFields(utc_day, (field,)) from exc
    if not isinstance(payload, dict):
        raise BlockedMissingCanonicalFields(utc_day, (field,))
    return payload


def _verify_artifact(
    path: Path,
    *,
    expected_sha256: str,
    expected_size: int | None,
    field: str,
    utc_day: str,
) -> _SourceArtifact:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise BlockedMissingCanonicalFields(utc_day, (field,))
    observed_size = int(resolved.stat().st_size)
    observed_sha = _file_sha256(resolved)
    if observed_sha != str(expected_sha256) or (
        expected_size is not None and observed_size != int(expected_size)
    ):
        raise BlockedMissingCanonicalFields(utc_day, (f"{field}_identity",))
    return _SourceArtifact(resolved, observed_sha, observed_size)


def _manifest_artifact(
    row: Mapping[str, Any],
    *,
    layout: offline.OfflineSourceLayout,
    field: str,
    utc_day: str,
) -> _SourceArtifact:
    try:
        path = offline._resolve_portable(str(row["path"]), layout=layout)
        sha256 = str(row["sha256"])
        size_bytes = int(row["size_bytes"])
    except (KeyError, TypeError, ValueError, offline.OfflineSourceGateError) as exc:
        raise BlockedMissingCanonicalFields(utc_day, (field,)) from exc
    return _verify_artifact(
        path,
        expected_sha256=sha256,
        expected_size=size_bytes,
        field=field,
        utc_day=utc_day,
    )


def _source_receipt(
    source_manifest: Mapping[str, Any],
    *,
    source_day: str,
    layout: offline.OfflineSourceLayout,
    utc_day: str,
) -> tuple[dict[str, Any], _SourceArtifact]:
    bindings = source_manifest.get("source_day_receipt_files")
    row = bindings.get(source_day) if isinstance(bindings, Mapping) else None
    if not isinstance(row, Mapping):
        raise BlockedMissingCanonicalFields(utc_day, (f"source_day_receipt:{source_day}",))
    try:
        receipt_path = offline._resolve_portable(str(row["path"]), layout=layout)
        receipt_sha256 = str(row["sha256"])
    except (KeyError, TypeError, offline.OfflineSourceGateError) as exc:
        raise BlockedMissingCanonicalFields(
            utc_day, (f"source_day_receipt:{source_day}",)
        ) from exc
    # Source-receipt bindings predate the generic artifact schema and bind the
    # complete bytes by SHA256 without a redundant size field.
    artifact = _verify_artifact(
        receipt_path,
        expected_sha256=receipt_sha256,
        expected_size=None,
        field=f"source_day_receipt:{source_day}",
        utc_day=utc_day,
    )
    receipt = _load_json(
        artifact.path,
        field=f"source_day_receipt:{source_day}",
        utc_day=utc_day,
    )
    if receipt.get("source_day") != source_day or receipt.get(
        "source_day_receipt_sha256"
    ) != row.get("canonical_sha256"):
        raise BlockedMissingCanonicalFields(
            utc_day, (f"source_day_receipt:{source_day}:canonical_identity",)
        )
    return receipt, artifact


def _book_artifacts(
    request: panel_builder.DayMaterializationRequest,
    *,
    days: Sequence[str],
) -> tuple[dict[str, _SourceArtifact], dict[str, Any]]:
    manifest = _load_json(
        request.book_view_manifest_path,
        field="book_view_manifest",
        utc_day=request.utc_day,
    )
    rows = {
        (str(row.get("day")), str(row.get("kind"))): row
        for row in manifest.get("files", ())
        if isinstance(row, Mapping)
    }
    output: dict[str, _SourceArtifact] = {}
    for day in days:
        for kind in ("bbo", "l2"):
            row = rows.get((day, kind))
            if not isinstance(row, Mapping):
                raise BlockedMissingCanonicalFields(request.utc_day, (f"normalized_{kind}:{day}",))
            path = (
                request.book_view_manifest_path.parent
                / kind
                / (f"{offline.SYMBOL}-{kind}-{day}.parquet")
            )
            output[f"{day}:{kind}"] = _verify_artifact(
                path,
                expected_sha256=str(row.get("sha256", "")),
                expected_size=int(row.get("size_bytes", -1)),
                field=f"normalized_{kind}:{day}",
                utc_day=request.utc_day,
            )
    if (
        output[f"{request.utc_day}:bbo"].path != request.bbo_path.resolve()
        or output[f"{request.utc_day}:l2"].path != request.l2_path.resolve()
    ):
        raise OfflineB0MechanicsAdapterError("target book-view request path drifted")
    return output, manifest


def _feature_artifacts(
    request: panel_builder.DayMaterializationRequest,
    *,
    days: Sequence[str],
) -> tuple[dict[str, _SourceArtifact], dict[str, Any]]:
    manifest = _load_json(
        request.features_manifest_path,
        field="features_only_manifest",
        utc_day=request.utc_day,
    )
    if (
        manifest.get("labels_materialized") is not False
        or manifest.get("config_sha256") != offline.ACTIVE_PRIVATE_CONFIG_SHA256
    ):
        raise OfflineB0MechanicsAdapterError("features-only identity drifted")
    rows = {
        str(row.get("day")): row
        for row in manifest.get("daily_files", ())
        if isinstance(row, Mapping)
    }
    output: dict[str, _SourceArtifact] = {}
    for day in days:
        row = rows.get(day)
        if not isinstance(row, Mapping):
            raise BlockedMissingCanonicalFields(request.utc_day, (f"features_only:{day}",))
        path = request.features_manifest_path.parent / str(row.get("file", ""))
        output[day] = _verify_artifact(
            path,
            expected_sha256=str(row.get("sha256", "")),
            expected_size=int(row.get("size_bytes", -1)),
            field=f"features_only:{day}",
            utc_day=request.utc_day,
        )
    if output[request.utc_day].path != request.features_path.resolve():
        raise OfflineB0MechanicsAdapterError("target features-only request path drifted")
    return output, manifest


def _configuration(request: panel_builder.DayMaterializationRequest) -> tuple[Path, Path]:
    config = _verify_artifact(
        request.private_config_path,
        expected_sha256=offline.ACTIVE_PRIVATE_CONFIG_SHA256,
        expected_size=None,
        field="exact_owner_private_config",
        utc_day=request.utc_day,
    )
    try:
        payload = yaml.safe_load(config.path.read_text(encoding="utf-8"))
        model_value = payload["ml"]["model_dir"]
    except (OSError, TypeError, KeyError, yaml.YAMLError) as exc:
        raise BlockedMissingCanonicalFields(
            request.utc_day, ("causal_v12_model_dir_binding",)
        ) from exc
    model_dir = Path(str(model_value))
    if not model_dir.is_absolute():
        model_dir = ROOT / model_dir
    model_dir = model_dir.resolve()
    required = [
        model_dir / "bundle_meta.json",
        *(model_dir / f"{head}.txt" for head in control_overlay.POLICY_HEADS),
        *(model_dir / f"{head}_meta.json" for head in control_overlay.POLICY_HEADS),
        model_dir / "fill_prob_params.json",
    ]
    missing = [f"causal_v12_bundle:{path.name}" for path in required if not path.is_file()]
    if missing:
        raise BlockedMissingCanonicalFields(request.utc_day, missing)
    return config.path, model_dir


def _model_bundle_identity(model_dir: Path) -> str:
    rows = [
        (path.name, _file_sha256(path), int(path.stat().st_size))
        for path in sorted(model_dir.iterdir())
        if path.is_file()
    ]
    return _canonical_sha256(rows)


def _load_trade(path: Path, *, individual: bool) -> pd.DataFrame:
    return bt._read_individual_trade_csv(path) if individual else bt._read_aggtrade_csv(path)


def _causal_bars(aggtrades: pd.DataFrame) -> pd.DataFrame:
    frame = aggtrades.loc[:, ["transact_time", "price"]].copy()
    frame["timestamp"] = (
        frame["transact_time"].to_numpy(dtype=np.int64, copy=False) // 1_000
    ) * 1_000
    grouped = frame.groupby("timestamp", sort=True)["price"]
    bars = pd.DataFrame({"close": grouped.last(), "trade_count": grouped.size()})
    return bt.causal_complete_1s_bars(bars)


@contextmanager
def _bound_book_directories(bbo_dir: Path, l2_dir: Path) -> Iterator[None]:
    previous_bbo, previous_l2 = bt.BBO_DIR, bt.L2_DIR
    bt.BBO_DIR, bt.L2_DIR = bbo_dir, l2_dir
    try:
        yield
    finally:
        bt.BBO_DIR, bt.L2_DIR = previous_bbo, previous_l2


def _concatenate_ml_data(parts: Sequence[tuple[Any, ...]], *, utc_day: str) -> tuple[Any, ...]:
    if len(parts) != 2 or any(len(part) != len(parts[0]) for part in parts):
        raise OfflineB0MechanicsAdapterError("D/D+1 model overlay shape drifted")
    main_count = control_overlay.MAIN_ARRAY_COUNT
    values: list[Any] = [
        np.concatenate([np.asarray(part[index]) for part in parts]) for index in range(main_count)
    ]
    mappings = [part[-1] for part in parts]
    if any(not isinstance(mapping, Mapping) for mapping in mappings):
        raise OfflineB0MechanicsAdapterError("D/D+1 feature mapping is malformed")
    keys = tuple(sorted(str(key) for key in mappings[0]))
    if any(tuple(sorted(str(key) for key in mapping)) != keys for mapping in mappings[1:]):
        raise OfflineB0MechanicsAdapterError("D/D+1 feature mapping keys drifted")
    values.append(
        {key: np.concatenate([np.asarray(mapping[key]) for mapping in mappings]) for key in keys}
    )
    combined = tuple(values)
    ready = np.asarray(combined[0], dtype=np.int64)
    if len(ready) != 2 * control_overlay.ROWS_PER_DAY or np.any(ready[1:] <= ready[:-1]):
        raise BlockedMissingCanonicalFields(
            utc_day, ("causal_v12_overlay:D_plus_0_plus_D_plus_1_grid",)
        )
    return combined


def _mapping_difference_paths(left: Any, right: Any, prefix: str = "") -> list[str]:
    if isinstance(left, Mapping) and isinstance(right, Mapping):
        differences: list[str] = []
        for key in sorted(set(left) | set(right), key=str):
            child = f"{prefix}.{key}" if prefix else str(key)
            if key not in left or key not in right:
                differences.append(child)
            else:
                differences.extend(_mapping_difference_paths(left[key], right[key], child))
        return differences
    return [prefix] if left != right else []


@contextmanager
def _scoped_owner_baseline_authority() -> Iterator[None]:
    key = live_runtime_policy.F05_BOOLEAN_COOLDOWN_OWNER_OVERRIDE_ENV
    previous = os.environ.get(key)
    os.environ[key] = "1"
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = previous


def _load_params(config_path: Path) -> _OfflineParams:
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise OfflineB0MechanicsAdapterError("exact owner config is not a mapping")
    projected = copy.deepcopy(dict(raw))
    journal = projected.get("lifecycle_journal_v2")
    if not isinstance(journal, dict) or journal.get("enabled") is not True:
        raise OfflineB0MechanicsAdapterError(
            "exact owner config lacks the expected live lifecycle writer"
        )
    journal["enabled"] = False
    changed_paths = tuple(_mapping_difference_paths(raw, projected))
    if changed_paths != ("lifecycle_journal_v2.enabled",):
        raise OfflineB0MechanicsAdapterError(
            f"offline projection escaped writer-only allowlist: {changed_paths}"
        )
    bt.configure_symbol("BTCUSDC")
    with tempfile.TemporaryDirectory(prefix="narrowgate-f05-offline-config-") as directory:
        projection_path = Path(directory) / "config.yaml"
        projection_path.write_text(
            yaml.safe_dump(projected, sort_keys=False, allow_unicode=False),
            encoding="utf-8",
        )
        with _scoped_owner_baseline_authority():
            params = load_tick_base_params(
                symbol="BTCUSDC",
                config_path=projection_path,
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
            "exchange_book_queue_mode": "disabled",
            "collect_curves": False,
            "dynamic_fill_hazard_action_enabled": False,
            "dynamic_fill_hazard_shadow_enabled": False,
            "dynamic_fill_hazard_cpp_parity_enabled": False,
            "buy_fill_selection_live_enabled": False,
            "buy_fill_selection_shadow_enabled": False,
            "sync_adjust_replay_mode": "disabled",
            "markout_side_asymmetry_sign": 1.0,
            "rng_seed": int(params.get("rng_seed", 42) or 42),
        }
    )
    for flag in _RESEARCH_ACTION_FLAGS:
        params[flag] = False
    return _OfflineParams(
        params=params,
        raw_mapping_sha256=_canonical_sha256(raw),
        projected_mapping_sha256=_canonical_sha256(projected),
        changed_paths=changed_paths,
    )


def _materialize_replay_inputs(
    request: panel_builder.DayMaterializationRequest,
) -> _ReplayInputs:
    if (
        request.panel_role != panel_builder.PANEL_ROLE
        or request.queue_identity != panel_builder.QUEUE_IDENTITY
        or request.same_millisecond_ambiguity_policy != "censor"
    ):
        raise OfflineB0MechanicsAdapterError("panel-builder request identity drifted")
    target = date.fromisoformat(request.utc_day)
    days = tuple((target + timedelta(days=offset)).isoformat() for offset in (-1, 0, 1))
    if (
        request.source_receipts.get("replay_context_days_json")
        != json.dumps(list(days), separators=(",", ":"))
        or request.source_receipts.get("continuation_day") != days[2]
        or request.source_receipts.get("continuation_use_role") != "continuation_context_for_target"
        or request.source_receipts.get("continuation_creates_target_assignments") != "false"
    ):
        raise OfflineB0MechanicsAdapterError("D-1/D/D+1 request contract drifted")
    layout = offline.default_layout()
    source_manifest = _load_json(
        request.source_manifest_path,
        field="canonical_source_manifest",
        utc_day=request.utc_day,
    )
    if (
        request.utc_day not in source_manifest.get("selected_days", ())
        or source_manifest.get("panel_role") != panel_builder.PANEL_ROLE
        or source_manifest.get("same_millisecond_policy")
        != "censor_without_joint_book_trade_sequence"
        or source_manifest.get("canonical_manifest_sha256")
        != request.source_receipts.get("source_manifest_canonical_sha256")
    ):
        raise OfflineB0MechanicsAdapterError("canonical source request identity drifted")

    receipt_artifacts: dict[str, _SourceArtifact] = {}
    receipts: dict[str, Mapping[str, Any]] = {}
    individual: dict[str, _SourceArtifact] = {}
    aggregate: dict[str, _SourceArtifact] = {}
    for day in days:
        receipt, receipt_artifact = _source_receipt(
            source_manifest,
            source_day=day,
            layout=layout,
            utc_day=request.utc_day,
        )
        receipt_artifacts[day] = receipt_artifact
        receipts[day] = receipt
        individual[day] = _manifest_artifact(
            receipt["individual_trades"],
            layout=layout,
            field=f"individual_trades:{day}",
            utc_day=request.utc_day,
        )
        aggregate[day] = _manifest_artifact(
            receipt["aggtrades"],
            layout=layout,
            field=f"aggtrades:{day}",
            utc_day=request.utc_day,
        )

    book, book_manifest = _book_artifacts(request, days=days)
    features, features_manifest = _feature_artifacts(request, days=days)
    context_source_hashes = {
        role: str(receipts[day]["source_day_receipt_sha256"])
        for role, day in zip(("D_minus_1", "D", "D_plus_1"), days, strict=True)
    }
    context_book_hashes = {
        day: {kind: book[f"{day}:{kind}"].sha256 for kind in ("bbo", "l2")} for day in days
    }
    context_feature_hashes = {day: features[day].sha256 for day in days}
    if (
        request.source_receipts.get("context_source_receipts_sha256")
        != _canonical_sha256(context_source_hashes)
        or request.source_receipts.get("context_book_receipts_sha256")
        != _canonical_sha256(context_book_hashes)
        or request.source_receipts.get("context_feature_receipts_sha256")
        != _canonical_sha256(context_feature_hashes)
    ):
        raise OfflineB0MechanicsAdapterError("D-1/D/D+1 receipt binding drifted")
    config_path, model_dir = _configuration(request)

    execution_frames = [_load_trade(individual[day].path, individual=True) for day in days[1:]]
    trades = pd.concat(execution_frames, ignore_index=True, copy=False)
    if trades.empty or not trades["transact_time"].is_monotonic_increasing:
        raise BlockedMissingCanonicalFields(
            request.utc_day, ("individual_trades:D_plus_0_plus_D_plus_1_order",)
        )
    aggregate_frames = [_load_trade(aggregate[day].path, individual=False) for day in days]
    aggtrades = pd.concat(aggregate_frames, ignore_index=True, copy=False)
    if aggtrades.empty or not aggtrades["transact_time"].is_monotonic_increasing:
        raise BlockedMissingCanonicalFields(
            request.utc_day, ("aggtrades:D_minus_1_plus_D_plus_0_plus_D_plus_1_order",)
        )
    bars = _causal_bars(aggtrades)
    var_ts_ms, var_ssq = bt.build_rolling_variance(bars)
    _, var_ti = bt.build_trade_intensity(bars)
    _, var_retsq = bt.build_squared_returns(bars)

    with _bound_book_directories(
        book[f"{request.utc_day}:bbo"].path.parent,
        book[f"{request.utc_day}:l2"].path.parent,
    ):
        bbo_data = bt.load_bbo_data(days=days, quality_allowed_days=days)
        l2_data = bt.load_l2_data(days=days, quality_allowed_days=days)
    if bbo_data is None or l2_data is None:
        raise BlockedMissingCanonicalFields(
            request.utc_day, ("modeled_queue_book_view:D_minus_1_D_D_plus_1",)
        )

    try:
        target_overlay = control_overlay._generate_ml_data(
            features[days[0]].path,
            features[days[1]].path,
            day=days[1],
            model_dir=model_dir,
        )
        continuation_overlay = control_overlay._generate_ml_data(
            features[days[1]].path,
            features[days[2]].path,
            day=days[2],
            model_dir=model_dir,
        )
        ml_data = _concatenate_ml_data(
            (
                control_overlay._validate_ml_data(target_overlay, day=days[1]),
                control_overlay._validate_ml_data(continuation_overlay, day=days[2]),
            ),
            utc_day=request.utc_day,
        )
    except (control_overlay.ControlOverlayRepairError, ValueError) as exc:
        raise BlockedMissingCanonicalFields(
            request.utc_day, ("causal_v12_overlay:D_plus_0_plus_D_plus_1",)
        ) from exc

    offline_params = _load_params(config_path)
    params = dict(offline_params.params)
    model_identity = _model_bundle_identity(model_dir)
    market_identity_payload = {
        "schema_version": f"{IDENTITY}.market_window.v1",
        "utc_day": request.utc_day,
        "context_days": list(days),
        "source_manifest_canonical_sha256": source_manifest["canonical_manifest_sha256"],
        "source_receipts": {
            day: {
                "receipt_sha256": receipt_artifacts[day].sha256,
                "individual_trades_sha256": individual[day].sha256,
                "aggtrades_sha256": aggregate[day].sha256,
                "bbo_sha256": book[f"{day}:bbo"].sha256,
                "l2_sha256": book[f"{day}:l2"].sha256,
            }
            for day in days
        },
        "book_view_canonical_sha256": book_manifest["canonical_manifest_sha256"],
        "execution_trade_rows": int(len(trades)),
        "same_millisecond_ambiguity_policy": "censor",
        "queue_identity": panel_builder.QUEUE_IDENTITY,
    }
    market_window_identity = _canonical_sha256(market_identity_payload)
    model_overlay_payload = {
        "schema_version": f"{IDENTITY}.model_overlay.v1",
        "utc_day": request.utc_day,
        "continuation_day": days[2],
        "features_daily_manifest_sha256": features_manifest["daily_manifest_sha256"],
        "features": {day: features[day].sha256 for day in days},
        "feature_dag_sha256": features_manifest["feature_dag_sha256"],
        "model_bundle_sha256": model_identity,
        "prediction_rows": int(len(np.asarray(ml_data[0]))),
    }
    model_overlay_identity = _canonical_sha256(model_overlay_payload)
    latency_identity = _parameter_identity(
        params,
        schema_version=f"{IDENTITY}.latency_identity.v1",
        names=_LATENCY_IDENTITY_PARAMETERS,
    )
    queue_random_names = tuple(
        dict.fromkeys(
            (
                *_QUEUE_RANDOM_IDENTITY_PARAMETERS,
                *(name for name in sorted(params) if str(name).endswith("_seed")),
            )
        )
    )
    queue_random_identity = _parameter_identity(
        params,
        schema_version=f"{IDENTITY}.queue_random_identity.v1",
        names=queue_random_names,
    )
    replay_receipt = _canonical_sha256(
        {
            "schema_version": f"{IDENTITY}.replay_input.v1",
            "input_binding_sha256": request.input_binding_sha256,
            "market_window_identity_sha256": market_window_identity,
            "model_overlay_identity_sha256": model_overlay_identity,
            "config_sha256": offline.ACTIVE_PRIVATE_CONFIG_SHA256,
            "config_raw_mapping_sha256": offline_params.raw_mapping_sha256,
            "config_offline_projection_mapping_sha256": (
                offline_params.projected_mapping_sha256
            ),
            "config_offline_projection_changed_paths": list(offline_params.changed_paths),
            "config_offline_projection_changes_quote_policy": False,
            "owner_baseline_load_authority": "scoped_owner_risk_accepted_existing_B0",
            "owner_baseline_load_authority_persisted": False,
            "owner_policy_sha256": offline.ACTIVE_OWNER_POLICY_SHA256,
            "replay_engine": "python",
            "queue_identity": panel_builder.QUEUE_IDENTITY,
            "rng_seed": int(params["rng_seed"]),
            "latency_identity_sha256": latency_identity,
            "queue_random_identity_sha256": queue_random_identity,
            "target_assignment_interval": f"{days[1]}T00:00:00Z/{days[2]}T00:00:00Z",
            "continuation_context_interval": f"{days[2]}T00:00:00Z/next_utc_midnight",
        }
    )
    return _ReplayInputs(
        utc_day=request.utc_day,
        continuation_day=days[2],
        trades=trades,
        var_ts_ms=var_ts_ms,
        var_ssq=var_ssq,
        var_ti=var_ti,
        var_retsq=var_retsq,
        bbo_data=bbo_data,
        l2_data=l2_data,
        ml_data=ml_data,
        params=params,
        market_window_identity_sha256=market_window_identity,
        model_overlay_identity_sha256=model_overlay_identity,
        latency_identity_sha256=latency_identity,
        queue_random_identity_sha256=queue_random_identity,
        replay_input_receipt_sha256=replay_receipt,
    )


def _execute_outcome_blind_replay(
    replay: _ReplayInputs,
    *,
    emitter: Any,
    evaluator: Any,
) -> dict[str, dict[str, Any]]:
    params = dict(replay.params)
    params["cooldown_v2_snapshot_emitter"] = emitter
    params["cooldown_duration_policy_evaluator"] = evaluator
    # This limit is an outcome-blind capacity bound. Each modeled fill is
    # driven by one individual trade, so the number of exposure assignments
    # cannot exceed the execution-trade row count.
    params["trace_cooldown_duration_opportunities_max"] = max(
        1, int(len(replay.trades))
    )
    result = bt._simulate_tick_with_engine(
        "python",
        replay.trades,
        replay.var_ts_ms,
        replay.var_ssq,
        params,
        ml_data=replay.ml_data,
        bbo_data=replay.bbo_data,
        l2_data=replay.l2_data,
        var_ti=replay.var_ti,
        var_retsq=replay.var_retsq,
    )
    if not isinstance(result, Mapping):
        raise OfflineB0MechanicsAdapterError("B0 simulator result is not a mapping")
    raw_opportunities = result.get("_cooldown_duration_opportunity_trace")
    raw_receipts = result.get("_cooldown_v2_snapshot_receipts")
    if not isinstance(raw_opportunities, list) or not isinstance(raw_receipts, list):
        raise OfflineB0MechanicsAdapterError(
            "B0 simulator lacks outcome-blind assignment mechanics traces"
        )
    opportunities: dict[int, Mapping[str, Any]] = {}
    for raw in raw_opportunities:
        if not isinstance(raw, Mapping):
            raise OfflineB0MechanicsAdapterError("B0 assignment trace row is malformed")
        ordinal = int(raw.get("exposure_fill_ordinal", 0) or 0)
        if ordinal <= 0 or ordinal in opportunities:
            raise OfflineB0MechanicsAdapterError(
                "B0 assignment trace ordinal is invalid or duplicated"
            )
        opportunities[ordinal] = raw
    assignments: dict[str, dict[str, Any]] = {}
    for raw in raw_receipts:
        if not isinstance(raw, Mapping):
            raise OfflineB0MechanicsAdapterError("B0 snapshot receipt is malformed")
        snapshot_id = str(raw.get("snapshot_id", ""))
        ordinal = int(raw.get("exposure_fill_ordinal", 0) or 0)
        opportunity = opportunities.get(ordinal)
        if not snapshot_id or snapshot_id in assignments or opportunity is None:
            raise OfflineB0MechanicsAdapterError(
                "B0 snapshot receipt cannot be joined to assignment mechanics"
            )
        campaign_id = int(opportunity.get("campaign_id", 0) or 0)
        order_id = int(opportunity.get("order_id", 0) or 0)
        assignment_equity = float(opportunity.get("assignment_equity_usdc", float("nan")))
        receipt_campaign_id = int(raw.get("campaign_id", 0) or 0)
        opportunity_side = str(opportunity.get("side", "")).upper()
        receipt_side = str(raw.get("side", "")).upper()
        opportunity_role = str(opportunity.get("role_at_fill", ""))
        receipt_role = str(raw.get("role_at_fill", ""))
        if (
            campaign_id <= 0
            or order_id < 0
            or not np.isfinite(assignment_equity)
            or campaign_id != receipt_campaign_id
            or opportunity_side != receipt_side
            or opportunity_role != receipt_role
        ):
            raise OfflineB0MechanicsAdapterError(
                "B0 assignment mechanics disagree with the atomic snapshot receipt: "
                f"ordinal={ordinal}, campaign={campaign_id}/{receipt_campaign_id}, "
                f"order_id={order_id}, side={opportunity_side}/{receipt_side}, "
                f"role={opportunity_role}/{receipt_role}, "
                f"assignment_equity_finite={np.isfinite(assignment_equity)}"
            )
        assignments[snapshot_id] = {
            "campaign_id": campaign_id,
            "order_id": order_id,
            "exposure_fill_ordinal": ordinal,
            "assignment_equity_usdc": assignment_equity,
        }
    if len(assignments) != len(raw_receipts) or set(opportunities) != {
        int(row.get("exposure_fill_ordinal", 0) or 0)
        for row in raw_receipts
        if isinstance(row, Mapping)
    }:
        raise OfflineB0MechanicsAdapterError(
            "B0 outcome-blind opportunity and snapshot denominators differ"
        )
    # No terminal, markout, PnL, fill-quality, or candidate result key is read.
    return assignments


@dataclass(frozen=True, slots=True)
class CanonicalB0MechanicsAdapter:
    """Fixed adapter loaded by the source-bound panel builder."""

    identity: str = IDENTITY

    def preflight_day(self, request: panel_builder.DayMaterializationRequest) -> Mapping[str, Any]:
        target = date.fromisoformat(request.utc_day)
        days = tuple((target + timedelta(days=offset)).isoformat() for offset in (-1, 0, 1))
        source = _load_json(
            request.source_manifest_path,
            field="canonical_source_manifest",
            utc_day=request.utc_day,
        )
        missing: list[str] = []
        bindings = source.get("source_day_receipt_files")
        for day in days:
            if not isinstance(bindings, Mapping) or day not in bindings:
                missing.append(f"source_day_receipt:{day}")
        book = _load_json(
            request.book_view_manifest_path,
            field="book_view_manifest",
            utc_day=request.utc_day,
        )
        book_keys = {
            (str(row.get("day")), str(row.get("kind")))
            for row in book.get("files", ())
            if isinstance(row, Mapping)
        }
        features = _load_json(
            request.features_manifest_path,
            field="features_only_manifest",
            utc_day=request.utc_day,
        )
        feature_days = {
            str(row.get("day"))
            for row in features.get("daily_files", ())
            if isinstance(row, Mapping)
        }
        feature_rows = {
            str(row.get("day")): row
            for row in features.get("daily_files", ())
            if isinstance(row, Mapping)
        }
        for day in days:
            for kind in ("bbo", "l2"):
                if (day, kind) not in book_keys:
                    missing.append(f"normalized_{kind}:{day}")
            if day not in feature_days:
                missing.append(f"features_only:{day}")
        for day in days[1:]:
            row = feature_rows.get(day)
            if not isinstance(row, Mapping):
                continue
            path = request.features_manifest_path.parent / str(row.get("file", ""))
            if not path.is_file():
                missing.append(f"features_only:{day}")
            elif pq.ParquetFile(path).metadata.num_rows != control_overlay.ROWS_PER_DAY:
                missing.append(f"features_only_canonical_10s_grid:{day}")
        _configuration(request)
        if missing:
            raise BlockedMissingCanonicalFields(request.utc_day, missing)
        return {
            "identity": IDENTITY,
            "status": "D_minus_1_D_D_plus_1_canonical_fields_available",
            "utc_day": request.utc_day,
            "context_days": list(days),
            "target_assignment_days": [request.utc_day],
            "continuation_only_days": [days[2]],
            "economic_outcomes_read": False,
            "labels_read": False,
            "candidate_actions_generated": False,
        }

    def identity_hashes(
        self, request: panel_builder.DayMaterializationRequest
    ) -> Mapping[str, str]:
        config_path, model_dir = _configuration(request)
        features_manifest = _load_json(
            request.features_manifest_path,
            field="features_only_manifest",
            utc_day=request.utc_day,
        )
        p3_path = model_dir / "fill_prob_params.json"
        implementation = {
            "adapter_sha256": _file_sha256(Path(__file__).resolve()),
            "backtest_tick_sha256": _file_sha256(ROOT / "models/backtest_tick.py"),
            "control_overlay_sha256": _file_sha256(Path(control_overlay.__file__).resolve()),
            "panel_builder_sha256": _file_sha256(Path(panel_builder.__file__).resolve()),
        }
        return {
            "config_sha256": _file_sha256(config_path),
            "code_sha256": _canonical_sha256(implementation),
            "model_sha256": _model_bundle_identity(model_dir),
            "p3_sha256": _file_sha256(p3_path),
            "feature_dag_sha256": str(features_manifest.get("feature_dag_sha256", "")),
            "execution_abi_sha256": _file_sha256(ROOT / "models/backtest_tick.py"),
            "baseline_identity_sha256": offline.ACTIVE_OWNER_POLICY_SHA256,
        }

    def run_day(
        self,
        request: panel_builder.DayMaterializationRequest,
        *,
        emitter: Any,
        evaluator: Any,
    ) -> Mapping[str, Any]:
        replay = _materialize_replay_inputs(request)
        assignment_mechanics = _execute_outcome_blind_replay(
            replay, emitter=emitter, evaluator=evaluator
        )
        audit = emitter.audit()
        snapshots_emitted = int(audit.snapshots_emitted)
        if snapshots_emitted <= 0 or bool(audit.economic_outcomes_read):
            raise OfflineB0MechanicsAdapterError(
                "outcome-blind B0 replay emitted no admissible mechanics snapshots"
            )
        if len(assignment_mechanics) != snapshots_emitted:
            raise OfflineB0MechanicsAdapterError(
                "B0 assignment mechanics did not cover every emitted snapshot"
            )
        return {
            "schema_version": RESULT_SCHEMA,
            "identity": IDENTITY,
            "utc_day": request.utc_day,
            "replay_engine": "python",
            "queue_identity": panel_builder.QUEUE_IDENTITY,
            "same_millisecond_ambiguity_policy": "censor",
            "exposure_fill_scope": "exposure_increasing_only",
            "current_owner_b0_executed": True,
            "candidate_actions_generated": False,
            "economic_outcomes_read": False,
            "labels_read": False,
            "snapshots_emitted": snapshots_emitted,
            "market_window_identity_sha256": replay.market_window_identity_sha256,
            "model_overlay_identity_sha256": replay.model_overlay_identity_sha256,
            "latency_identity_sha256": replay.latency_identity_sha256,
            "queue_random_identity_sha256": replay.queue_random_identity_sha256,
            "replay_input_receipt_sha256": replay.replay_input_receipt_sha256,
            "assignment_mechanics": assignment_mechanics,
        }


def build_canonical_b0_mechanics_adapter() -> CanonicalB0MechanicsAdapter:
    """Return the one adapter identity accepted by the formal panel builder."""

    return CanonicalB0MechanicsAdapter()


__all__ = [
    "BLOCKED_STATUS",
    "IDENTITY",
    "BlockedMissingCanonicalFields",
    "CanonicalB0MechanicsAdapter",
    "OfflineB0MechanicsAdapterError",
    "build_canonical_b0_mechanics_adapter",
]
