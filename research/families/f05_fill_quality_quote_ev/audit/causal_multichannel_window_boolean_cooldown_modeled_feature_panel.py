#!/usr/bin/env python3
"""Build an outcome-blind multichannel feature panel on the v1 census.

This owner-path bridge deliberately keeps three evidence identities separate:

* the existing v1 opportunity census supplies modeled-queue assignment rows;
* immutable v1 BBO-mid predicates/state are projected directly from that census;
* the bound normalized WindowData supplies M1 state on all 40 days;
* raw-native book events plus individual trades supply only the M2 increment on
  the frozen 33-day common support.

The module never opens an arm trace or an economic-label artifact.  Full M0
support requires an explicit, one-to-one enrichment keyed by ``opportunity_id``.
Without that enrichment the caller must opt into a reduced identity; missing
M0 fields remain null/UNOBSERVED and cannot acquire policy authority.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from data_paths import data_root, marketdata_root
from models import backtest_tick as bt
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_features as feature_engine,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_native_features as native_features,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_native_observation_cache as observation_cache,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_windows as normalized_windows,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_snapshot import (
    expected_feature_columns,
)

IDENTITY = (
    "causal_multichannel_window_boolean_cooldown_duration_v2."
    "owner_modeled_queue_feature_panel.v1"
)
SCHEMA_VERSION = f"{IDENTITY}.schema.v1"
OWNER_QUEUE_IDENTITY = "owner_modeled_queue"
FULL_M0_SUPPORT_IDENTITY = "owner_modeled_queue_full_explicit_M0_enrichment"
REDUCED_M0_SUPPORT_IDENTITY = "owner_modeled_queue_reduced_M0_unobserved"
QUEUE_PATH_SEMANTICS = (
    "v1_native_l2_exact_level_modeled_queue_without_exchange_queue_authority"
)

ROOT = Path(__file__).resolve().parents[4]
DEFAULT_DATA_ROOT = data_root(ROOT)
DEFAULT_CENSUS_ROOT = DEFAULT_DATA_ROOT / (
    "reports/multiscale_ema_boolean_cooldown_duration_policy_v1_20260810/"
    "execution/census"
)
DEFAULT_OUTPUT_ROOT = DEFAULT_DATA_ROOT / (
    "reports/causal_multichannel_window_boolean_cooldown_duration_v2_20260810/"
    "owner_modeled_queue_feature_panel_v1"
)
DEFAULT_RAW_NATIVE_ROOT = marketdata_root() / "cryptohftdata"
DEFAULT_NATIVE_CACHE = DEFAULT_DATA_ROOT / (
    "cache/replay_dag/native_exchange_book_hour_v1"
)
DEFAULT_NATIVE_OBSERVATION_CACHE = DEFAULT_DATA_ROOT / (
    "cache/replay_dag/"
    "causal_multichannel_window_boolean_cooldown_native_observation_v1"
)
DEFAULT_NORMALIZED_EXECUTION_PLAN = DEFAULT_DATA_ROOT / (
    "cache/replay_dag/f03_causal_v12_1s_native_40day_full_path_ml_ab_v3/"
    "execution-plan.json"
)

FEATURE_BLOCKS = ("R0", "M0", "M1", "M2")
DAY_SUCCESS = "_SUCCESS"
M0_PREDICATE_PREFIX = "predicate::m0::"
EXPECTED_M0_PROVIDER_IDENTITY = (
    "causal_multichannel_window_boolean_cooldown_duration_v2_"
    "owner_modeled_queue_m0_panel_v1"
)
EXPECTED_M0_PROVIDER_STATUS = "owner_modeled_queue_m0_day_admitted"
EXPECTED_NORMALIZED_PLAN_IDENTITY = (
    "causal_v12_1s_native_40day_v9_10s_vs_1s_ml_on_full_path_v3"
)
EXPECTED_V1_CENSUS_IDENTITY = "multiscale_ema_boolean_cooldown_duration_policy_v1"
SOURCE_SPLIT_SCHEMA_VERSION = (
    "causal_multichannel_window_boolean_cooldown_duration_v2."
    "owner_modeled_queue.source_split.v1"
)
R0_SOURCE_IDENTITY = "immutable_v1_census_bbo_mid_ema_state"
M1_SOURCE_IDENTITY = "normalized_modeled_label_bbo_l2_all40"
M2_SOURCE_IDENTITY = "raw_cryptohft_snapshot_delta_official_trades_prefix33"

V1_R0_HALF_LIVES_S = (0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0)
V1_R0_PREDICATE_SUFFIXES = (
    "favorable",
    "last_cross_favorable",
    "cross_age_le_fast",
    "cross_age_le_slow",
    "persistence_ge_fast",
    "persistence_ge_slow",
    "distance_ge_provider_sigma",
    "expanding",
)
V1_R0_CONTINUOUS_PAIR_SUFFIXES = (
    "cross_age_s",
    "arrangement_persistence_s",
    "favorable_distance_bps",
    "abs_distance_bps",
    "volatility_normalized",
    "favorable_distance_velocity_bps_per_s",
)


def _half_life_label(value: float) -> str:
    return f"h{float(value):g}s".replace(".", "p")


V1_R0_PAIR_LABELS = tuple(
    (_half_life_label(fast), _half_life_label(slow))
    for index, fast in enumerate(V1_R0_HALF_LIVES_S)
    for slow in V1_R0_HALF_LIVES_S[index + 1 :]
)
IMMUTABLE_R0_PREDICATE_COLUMNS = tuple(
    f"predicate::ema_pair_{fast}_{slow}:{suffix}"
    for fast, slow in V1_R0_PAIR_LABELS
    for suffix in V1_R0_PREDICATE_SUFFIXES
)
IMMUTABLE_R0_CONTINUOUS_COLUMNS = (
    "ema_causal_volatility_bps",
    *(
        name
        for half_life in V1_R0_HALF_LIVES_S
        for name in (
            f"ema_rel_mid_bps_{_half_life_label(half_life)}",
            f"ema_slope_bps_per_s_{_half_life_label(half_life)}",
        )
    ),
    *(
        f"ema_pair_{fast}_{slow}_{suffix}"
        for fast, slow in V1_R0_PAIR_LABELS
        for suffix in V1_R0_CONTINUOUS_PAIR_SUFFIXES
    ),
)
IMMUTABLE_R0_COLUMNS = (
    *IMMUTABLE_R0_PREDICATE_COLUMNS,
    *IMMUTABLE_R0_CONTINUOUS_COLUMNS,
)

PREFIX40_DAYS = (
    "2026-04-17",
    "2026-04-18",
    "2026-04-19",
    "2026-04-20",
    "2026-04-22",
    "2026-04-23",
    "2026-05-01",
    "2026-05-02",
    "2026-05-03",
    "2026-05-04",
    "2026-05-05",
    "2026-05-06",
    "2026-05-13",
    "2026-05-29",
    "2026-05-30",
    "2026-05-31",
    "2026-06-02",
    "2026-06-03",
    "2026-06-05",
    "2026-06-06",
    "2026-06-07",
    "2026-06-08",
    "2026-06-09",
    "2026-06-10",
    "2026-06-11",
    "2026-06-12",
    "2026-06-13",
    "2026-06-14",
    "2026-06-15",
    "2026-06-16",
    "2026-06-17",
    "2026-06-18",
    "2026-06-19",
    "2026-06-20",
    "2026-06-21",
    "2026-06-22",
    "2026-06-23",
    "2026-06-24",
    "2026-06-25",
    "2026-06-26",
)
M2_EXCLUDED_DAYS = frozenset(
    {
        "2026-04-20",
        "2026-04-23",
        "2026-05-06",
        "2026-05-13",
        "2026-05-31",
        "2026-06-03",
        "2026-06-26",
    }
)
M2_COMMON_SUPPORT_DAYS = tuple(
    day for day in PREFIX40_DAYS if day not in M2_EXCLUDED_DAYS
)

# The base projection remains public for compatibility with the dedicated M0
# provider.  Formal feature construction reads CENSUS_SAFE_PROJECTION_COLUMNS,
# which adds only the frozen v1 R0 fields and never opens an arm/value column.
CENSUS_INPUT_COLUMNS = (
    "schema_version",
    "fill_clock_semantics",
    "live_receive_time_authority",
    "exposure_fill_ordinal",
    "fill_visible_ts_ms",
    "fill_exchange_ts_ms",
    "side",
    "role_at_fill",
    "order_id",
    "campaign_id",
    "inventory_before_fill_btc",
    "inventory_after_fill_btc",
    "fill_qty_btc",
    "unit_qty_btc",
    "consecutive_units_before",
    "consecutive_units_after",
    "prior_deadline_ts_ms",
    "baseline_duration_ms",
    "baseline_deadline_ts_ms",
    "canonical_mid",
    "best_bid",
    "best_ask",
    "decision_visible_bbo_index",
    "decision_visible_l2_index",
    "market_event_index",
    "utc_day",
    "campaign_side_id",
    "assignment_ts_ns",
    "opportunity_id",
    "source_profile",
    "formal_lifecycle_replay_eligible",
    "exact_queue_policy_eligible",
    "queue_path_semantics",
)
CENSUS_SAFE_PROJECTION_COLUMNS = (*CENSUS_INPUT_COLUMNS, *IMMUTABLE_R0_COLUMNS)

DIRECT_M0_FROM_CENSUS = {
    "assignment_ts_ns",
    "fill_visible_ts_ns",
    "side",
    "role_at_fill",
    "inventory_before_fill_btc",
    "inventory_after_fill_btc",
    "fill_qty_btc",
    "consecutive_units_after",
    "baseline_duration_ms",
}

FORBIDDEN_ECONOMIC_COLUMN_FRAGMENTS = (
    "arm_",
    "assignment_to_terminal",
    "closed_campaign_value",
    "terminal_value",
    "terminal_pnl",
    "economic_outcome",
    "reward",
    "q_value",
    "uplift",
    "washout_value",
)


class ModeledFeaturePanelError(RuntimeError):
    """Raised when an owner-path feature artifact would overclaim support."""


@dataclass(frozen=True, slots=True)
class FeatureBuildAudit:
    opportunity_count: int
    observation_count: int
    last_observation_right_ts_ns: int
    full_m0_support: bool
    support_identity: str
    m2_day_supported: bool
    block_supported_rows: Mapping[str, int]
    normalized_m1_observation_count: int = 0
    normalized_m1_last_right_ts_ns: int = 0
    raw_m2_observation_count: int = 0
    raw_m2_last_right_ts_ns: int = 0
    economic_outcomes_read: bool = False
    arm_economic_labels_read: bool = False


def sha256_file(path: Path) -> str:
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


def _is_sha256(value: Any) -> bool:
    text = str(value)
    return len(text) == 64 and all(character in "0123456789abcdef" for character in text)


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ModeledFeaturePanelError(f"cannot load JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise ModeledFeaturePanelError(f"JSON root must be an object: {path}")
    return payload


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_file(path: Path) -> None:
    with Path(path).open("rb") as handle:
        os.fsync(handle.fileno())


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _reject_economic_columns(columns: Iterable[str], *, source: str) -> None:
    allowed = {
        "campaign_mae_to_date_usdc",
        "economic_outcomes_read",
        "arm_economic_labels_read",
        "arm_outcomes_read",
    }
    rejected = sorted(
        name
        for name in (str(column) for column in columns)
        if name not in allowed
        and any(fragment in name.lower() for fragment in FORBIDDEN_ECONOMIC_COLUMN_FRAGMENTS)
    )
    if rejected:
        raise ModeledFeaturePanelError(
            f"{source} contains prohibited arm/economic columns: {rejected}"
        )


def _validate_immutable_r0(frame: pd.DataFrame) -> None:
    missing = sorted(set(IMMUTABLE_R0_COLUMNS) - set(frame.columns))
    if missing:
        raise ModeledFeaturePanelError(
            f"census lacks immutable v1 R0 columns: {missing[:8]}"
        )
    if len(IMMUTABLE_R0_PREDICATE_COLUMNS) != 360:
        raise ModeledFeaturePanelError("immutable v1 R0 predicate universe drifted")
    if len(IMMUTABLE_R0_CONTINUOUS_COLUMNS) != 291:
        raise ModeledFeaturePanelError("immutable v1 R0 continuous universe drifted")
    predicate = frame.loc[:, list(IMMUTABLE_R0_PREDICATE_COLUMNS)]
    if predicate.isna().any(axis=None):
        raise ModeledFeaturePanelError("immutable v1 R0 predicate contains null")
    if not all(pd.api.types.is_bool_dtype(dtype) for dtype in predicate.dtypes):
        raise ModeledFeaturePanelError("immutable v1 R0 predicate is not Boolean")
    continuous = frame.loc[:, list(IMMUTABLE_R0_CONTINUOUS_COLUMNS)].apply(
        pd.to_numeric,
        errors="coerce",
    )
    if not bool(continuous.notna().to_numpy().all()):
        raise ModeledFeaturePanelError("immutable v1 R0 continuous state is missing")
    if not bool(np.isfinite(continuous.to_numpy(dtype=float)).all()):
        raise ModeledFeaturePanelError("immutable v1 R0 continuous state is nonfinite")


def _validate_census_frame(
    frame: pd.DataFrame,
    *,
    day: str,
    require_immutable_r0: bool = False,
) -> pd.DataFrame:
    if frame.empty:
        raise ModeledFeaturePanelError(f"census day has no opportunities: {day}")
    expected_columns = (
        CENSUS_SAFE_PROJECTION_COLUMNS
        if require_immutable_r0
        else CENSUS_INPUT_COLUMNS
    )
    if tuple(frame.columns) != expected_columns:
        raise ModeledFeaturePanelError("census projection schema drifted")
    _reject_economic_columns(frame.columns, source="v1 census projection")
    if require_immutable_r0:
        _validate_immutable_r0(frame)
    if frame["opportunity_id"].astype(str).duplicated().any():
        raise ModeledFeaturePanelError("census opportunity_id is not unique")
    if frame["exposure_fill_ordinal"].duplicated().any():
        raise ModeledFeaturePanelError("census exposure-fill ordinal is not unique")
    if not frame["utc_day"].astype(str).eq(day).all():
        raise ModeledFeaturePanelError("census UTC day drifted")
    if frame["live_receive_time_authority"].astype(bool).any():
        raise ModeledFeaturePanelError("historical census claimed live receive-time authority")
    if frame["exact_queue_policy_eligible"].astype(bool).any():
        raise ModeledFeaturePanelError("owner modeled-queue census claimed exact queue authority")
    if not frame["side"].astype(str).str.upper().isin(("BUY", "SELL")).all():
        raise ModeledFeaturePanelError("census side is not BUY/SELL")
    if not frame["role_at_fill"].astype(str).str.lower().isin(("opener", "add")).all():
        raise ModeledFeaturePanelError("census role is not opener/add")

    visible_ns = frame["fill_visible_ts_ms"].astype("int64") * 1_000_000
    if not (visible_ns == frame["assignment_ts_ns"].astype("int64")).all():
        raise ModeledFeaturePanelError("assignment and fill-visible clocks drifted")
    if (visible_ns <= 0).any() or not visible_ns.is_monotonic_increasing:
        raise ModeledFeaturePanelError("fill-visible cutoff is not positive/chronological")
    if not frame["exposure_fill_ordinal"].astype("int64").is_monotonic_increasing:
        raise ModeledFeaturePanelError("exposure-fill ordinal is not chronological")
    if (frame["fill_qty_btc"].astype(float) <= 0.0).any():
        raise ModeledFeaturePanelError("census contains non-positive fill quantity")
    if (frame["baseline_duration_ms"].astype(float) <= 0.0).any():
        raise ModeledFeaturePanelError("census contains non-positive baseline duration")

    normalized = frame.copy()
    normalized["side"] = normalized["side"].astype(str).str.upper()
    normalized["role_at_fill"] = normalized["role_at_fill"].astype(str).str.lower()
    return normalized.reset_index(drop=True)


def load_census_day(
    day: str,
    *,
    census_root: Path = DEFAULT_CENSUS_ROOT,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Read only the allowlisted, outcome-blind v1 census projection."""

    try:
        canonical_day = date.fromisoformat(str(day)).isoformat()
    except ValueError as exc:
        raise ModeledFeaturePanelError(f"invalid UTC day: {day!r}") from exc
    day_root = Path(census_root).expanduser().resolve() / canonical_day
    data_path = day_root / "opportunities.parquet"
    manifest_path = day_root / "manifest.json"
    if not data_path.is_file() or not manifest_path.is_file():
        raise ModeledFeaturePanelError(f"missing v1 census admission for {canonical_day}")

    manifest = _load_json(manifest_path)
    book_contract = manifest.get("book_source_contract")
    if (
        manifest.get("identity") != EXPECTED_V1_CENSUS_IDENTITY
        or manifest.get("utc_day") != canonical_day
        or manifest.get("economic_outcomes_read") is not False
        or manifest.get("validation_read") is not False
        or manifest.get("sealed_holdout_read") is not False
        or not isinstance(book_contract, Mapping)
        or book_contract.get("exact_queue_policy_eligible") is not False
    ):
        raise ModeledFeaturePanelError("v1 census manifest identity/support drifted")
    observed_hash = sha256_file(data_path)
    if observed_hash != str(manifest.get("data_sha256", "")):
        raise ModeledFeaturePanelError("v1 census parquet hash drifted")

    schema_names = tuple(pq.ParquetFile(data_path).schema_arrow.names)
    missing = sorted(set(CENSUS_SAFE_PROJECTION_COLUMNS) - set(schema_names))
    if missing:
        raise ModeledFeaturePanelError(f"v1 census is missing columns: {missing}")
    # Reading with an explicit projection is the key no-label invariant.
    table = pq.read_table(data_path, columns=list(CENSUS_SAFE_PROJECTION_COLUMNS))
    frame = _validate_census_frame(
        table.to_pandas(),
        day=canonical_day,
        require_immutable_r0=True,
    )
    binding = {
        "identity": str(manifest["identity"]),
        "utc_day": canonical_day,
        "manifest_path": str(manifest_path),
        "manifest_sha256": sha256_file(manifest_path),
        "data_path": str(data_path),
        "data_sha256": observed_hash,
        "columns_read": list(CENSUS_SAFE_PROJECTION_COLUMNS),
        "immutable_r0_predicate_columns": list(IMMUTABLE_R0_PREDICATE_COLUMNS),
        "immutable_r0_predicate_count": len(IMMUTABLE_R0_PREDICATE_COLUMNS),
        "immutable_r0_continuous_columns": list(IMMUTABLE_R0_CONTINUOUS_COLUMNS),
        "immutable_r0_continuous_count": len(IMMUTABLE_R0_CONTINUOUS_COLUMNS),
        "immutable_r0_source": "v1_opportunities_parquet_direct_projection",
        "source_execution_identity_sha256": str(
            manifest.get("execution_identity_sha256", "")
        ),
        "economic_outcomes_read": False,
        "arm_economic_labels_read": False,
        "join_key": "opportunity_id",
        "exact_queue_policy_eligible": False,
    }
    binding["binding_sha256"] = canonical_sha256(binding)
    return frame, binding


def _load_enrichment_table(path: Path) -> tuple[pd.DataFrame, tuple[str, ...]]:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise ModeledFeaturePanelError(f"M0 enrichment is missing: {resolved}")
    required = {"opportunity_id", *feature_engine.M0_REQUIRED_FIELDS}
    if resolved.suffix == ".parquet":
        names = tuple(pq.ParquetFile(resolved).schema_arrow.names)
        _reject_economic_columns(names, source="M0 enrichment")
        if not required.issubset(names):
            missing = sorted(required - set(names))
            raise ModeledFeaturePanelError(
                f"M0 enrichment lacks the full projected M0 schema: {missing}"
            )
        return (
            pq.read_table(resolved, columns=sorted(required)).to_pandas(),
            names,
        )
    if resolved.suffix == ".csv":
        names = tuple(pd.read_csv(resolved, nrows=0).columns)
        _reject_economic_columns(names, source="M0 enrichment")
        if not required.issubset(names):
            missing = sorted(required - set(names))
            raise ModeledFeaturePanelError(
                f"M0 enrichment lacks the full projected M0 schema: {missing}"
            )
        return pd.read_csv(resolved, usecols=sorted(required)), names
    raise ModeledFeaturePanelError("M0 enrichment must be Parquet or CSV")


def _same_number(left: Any, right: Any) -> bool:
    try:
        return math.isclose(float(left), float(right), rel_tol=0.0, abs_tol=1e-12)
    except (TypeError, ValueError):
        return False


def validate_m0_enrichment(
    opportunities: pd.DataFrame,
    enrichment: pd.DataFrame,
) -> pd.DataFrame:
    """Validate a complete M0 provider and bind it by opportunity_id only."""

    _reject_economic_columns(enrichment.columns, source="M0 enrichment")
    required = {"opportunity_id", *feature_engine.M0_REQUIRED_FIELDS}
    if set(enrichment.columns) != required:
        raise ModeledFeaturePanelError("M0 enrichment schema is not exact")
    if enrichment["opportunity_id"].astype(str).duplicated().any():
        raise ModeledFeaturePanelError("M0 enrichment opportunity_id is not unique")
    expected_ids = set(opportunities["opportunity_id"].astype(str))
    actual_ids = set(enrichment["opportunity_id"].astype(str))
    if actual_ids != expected_ids:
        missing = sorted(expected_ids - actual_ids)
        extra = sorted(actual_ids - expected_ids)
        raise ModeledFeaturePanelError(
            f"M0 enrichment coverage drifted: missing={missing[:5]} extra={extra[:5]}"
        )

    indexed = enrichment.assign(
        opportunity_id=enrichment["opportunity_id"].astype(str)
    ).set_index("opportunity_id")
    if not indexed.index.is_unique:
        raise ModeledFeaturePanelError("M0 enrichment opportunity_id is not unique")
    normalized_rows: list[dict[str, Any]] = []
    for census_row in opportunities.to_dict("records"):
        opportunity_id = str(census_row["opportunity_id"])
        raw = indexed.loc[opportunity_id].to_dict()
        # Arrow/Pandas materializes nullable numeric fields as NaN.  Restore the
        # contract's explicit UNOBSERVED value before validating; never turn it
        # into zero or a finite sentinel.
        for name in feature_engine.M0_NULLABLE_FIELDS:
            if pd.isna(raw[name]):
                raw[name] = None
        try:
            normalized = feature_engine.validate_m0_context(raw)
        except feature_engine.FeatureContractError as exc:
            raise ModeledFeaturePanelError(
                f"invalid M0 enrichment for {opportunity_id}: {exc}"
            ) from exc
        expected_direct = {
            "assignment_ts_ns": int(census_row["assignment_ts_ns"]),
            "fill_visible_ts_ns": int(census_row["fill_visible_ts_ms"]) * 1_000_000,
            "side": str(census_row["side"]).upper(),
            "role_at_fill": str(census_row["role_at_fill"]).lower(),
            "inventory_before_fill_btc": float(census_row["inventory_before_fill_btc"]),
            "inventory_after_fill_btc": float(census_row["inventory_after_fill_btc"]),
            "fill_qty_btc": float(census_row["fill_qty_btc"]),
            "consecutive_units_after": float(census_row["consecutive_units_after"]),
            "baseline_duration_ms": float(census_row["baseline_duration_ms"]),
        }
        for name, expected in expected_direct.items():
            observed = normalized[name]
            if isinstance(expected, str):
                matches = str(observed) == expected
            elif isinstance(expected, int):
                matches = int(observed) == expected
            else:
                matches = _same_number(observed, expected)
            if not matches:
                raise ModeledFeaturePanelError(
                    f"M0 enrichment disagrees with census {name} for {opportunity_id}"
                )
        normalized_rows.append({"opportunity_id": opportunity_id, **normalized})
    return pd.DataFrame(normalized_rows)


def load_m0_enrichment(
    path: Path,
    *,
    opportunities: pd.DataFrame,
    manifest_path: Path,
    census_binding: Mapping[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Load a full explicit provider without touching any arm label."""

    resolved = Path(path).expanduser().resolve()
    projected, provider_columns = _load_enrichment_table(resolved)
    frame = validate_m0_enrichment(opportunities, projected)
    resolved_manifest = Path(manifest_path).expanduser().resolve()
    if not resolved_manifest.is_file():
        raise ModeledFeaturePanelError("full M0 enrichment requires its day manifest")
    manifest = _load_json(resolved_manifest)
    arm_read_flag = manifest.get(
        "arm_economic_labels_read",
        manifest.get("arm_outcomes_read"),
    )
    execution_identity_sha256 = str(
        manifest.get("execution_identity_sha256", "")
    )
    expected_day = str(census_binding.get("utc_day", ""))
    if (
        manifest.get("identity") != EXPECTED_M0_PROVIDER_IDENTITY
        or manifest.get("status") != EXPECTED_M0_PROVIDER_STATUS
        or manifest.get("utc_day") != expected_day
        or manifest.get("economic_outcomes_read") is not False
        or arm_read_flag is not False
        or manifest.get("duration_treatment_applied") is not False
        or str(manifest.get("data_sha256", "")) != sha256_file(resolved)
        or int(manifest.get("row_count", -1)) != len(frame)
        or manifest.get("exact_queue_policy_eligible") is not False
        or str(manifest.get("source_census_data_sha256", ""))
        != str(census_binding.get("data_sha256", ""))
        or str(manifest.get("source_census_manifest_sha256", ""))
        != str(census_binding.get("manifest_sha256", ""))
        or len(execution_identity_sha256) != 64
        or sorted(str(value) for value in manifest.get("m0_columns", ()))
        != sorted(feature_engine.M0_REQUIRED_FIELDS)
    ):
        raise ModeledFeaturePanelError("M0 enrichment manifest authority drifted")
    manifest_binding = {
        "path": str(resolved_manifest),
        "sha256": sha256_file(resolved_manifest),
        "identity": str(manifest["identity"]),
        "status": str(manifest["status"]),
        "utc_day": str(manifest["utc_day"]),
        "execution_identity_sha256": execution_identity_sha256,
        "source_census_data_sha256": str(
            manifest["source_census_data_sha256"]
        ),
        "source_census_manifest_sha256": str(
            manifest["source_census_manifest_sha256"]
        ),
    }
    binding = {
        "mode": "full_explicit_M0_enrichment",
        "support_identity": FULL_M0_SUPPORT_IDENTITY,
        "path": str(resolved),
        "sha256": sha256_file(resolved),
        "manifest": manifest_binding,
        "provider_identity": EXPECTED_M0_PROVIDER_IDENTITY,
        "execution_identity_sha256": execution_identity_sha256,
        "join_key": "opportunity_id",
        "provider_schema_columns": list(provider_columns),
        "columns_read": ["opportunity_id", *feature_engine.M0_REQUIRED_FIELDS],
        "row_count": int(len(frame)),
        "economic_outcomes_read": False,
        "arm_economic_labels_read": False,
    }
    binding["binding_sha256"] = canonical_sha256(binding)
    return frame, binding


def reduced_m0_binding() -> dict[str, Any]:
    missing = sorted(set(feature_engine.M0_REQUIRED_FIELDS) - DIRECT_M0_FROM_CENSUS)
    binding = {
        "mode": "explicit_reduced_M0_unobserved",
        "support_identity": REDUCED_M0_SUPPORT_IDENTITY,
        "observed_fields": sorted(DIRECT_M0_FROM_CENSUS),
        "unobserved_fields": missing,
        "missing_value_encoding": "null_with_UNOBSERVED_field_authority",
        "join_key": "opportunity_id",
        "economic_outcomes_read": False,
        "arm_economic_labels_read": False,
    }
    binding["binding_sha256"] = canonical_sha256(binding)
    return binding


def _reduced_m0_context(row: Mapping[str, Any]) -> dict[str, Any]:
    context = {name: None for name in feature_engine.M0_REQUIRED_FIELDS}
    context.update(
        {
            "assignment_ts_ns": int(row["assignment_ts_ns"]),
            "fill_visible_ts_ns": int(row["fill_visible_ts_ms"]) * 1_000_000,
            "side": str(row["side"]).upper(),
            "role_at_fill": str(row["role_at_fill"]).lower(),
            "inventory_before_fill_btc": float(row["inventory_before_fill_btc"]),
            "inventory_after_fill_btc": float(row["inventory_after_fill_btc"]),
            "fill_qty_btc": float(row["fill_qty_btc"]),
            "consecutive_units_after": float(row["consecutive_units_after"]),
            "baseline_duration_ms": float(row["baseline_duration_ms"]),
        }
    )
    return context


def _project_observation(
    observation: feature_engine.CausalWindowObservation,
    *,
    block: str,
) -> feature_engine.CausalWindowObservation:
    names = tuple(spec.name for spec in feature_engine.CHANNELS_BY_BLOCK[block])
    missing = sorted(set(names) - set(observation.values))
    if missing:
        raise ModeledFeaturePanelError(
            f"native M2 observation lacks {block} channels: {missing}"
        )
    return feature_engine.CausalWindowObservation(
        left_ts_ns=int(observation.left_ts_ns),
        right_ts_ns=int(observation.right_ts_ns),
        feature_ready_ts_ns=int(observation.feature_ready_ts_ns),
        market_generation=int(observation.market_generation),
        depth_generation=int(observation.depth_generation),
        values={name: observation.values[name] for name in names},
        source_gap=bool(observation.source_gap),
        source_stale=bool(observation.source_stale),
        warmup_admitted=bool(observation.warmup_admitted),
    )


def _market_feature_row(
    state: feature_engine.CausalMultichannelEmaState,
    *,
    side: str,
    decision_ts_ns: int,
) -> dict[str, Any]:
    """Snapshot channel state without fabricating a strict M0 context."""

    if state.last_feature_ready_ts_ns is None or state.last_right_ts_ns is None:
        raise ModeledFeaturePanelError("no completed causal market window is available")
    if int(state.last_feature_ready_ts_ns) > int(decision_ts_ns):
        raise ModeledFeaturePanelError("feature-ready state crossed fill-visible cutoff")
    output: dict[str, Any] = {
        "feature_engine_schema_version": feature_engine.SCHEMA_VERSION,
        "feature_engine_identity": feature_engine.IDENTITY,
        "base_window_width_ns": state.contract.base_window_width_ns,
        "maximum_explicit_window_count": state.contract.maximum_explicit_window_count,
        "last_window_right_ts_ns": int(state.last_right_ts_ns),
        "feature_ready_ts_ns": int(state.last_feature_ready_ts_ns),
        "decision_ts_ns": int(decision_ts_ns),
        "market_generation": int(state.last_market_generation or 0),
        "depth_generation": int(state.last_depth_generation or 0),
        "window_count": int(state.window_count),
        "gap_window_count": int(state.gap_window_count),
        "warmup_admitted": bool(state.warmup_admitted),
        "warmup_identity": str(state.warmup_identity),
    }
    channel_support = True
    for channel in state.channels.values():
        snapshot = channel.snapshot(side=str(side).upper(), decision_ts_ns=decision_ts_ns)
        channel_support &= bool(snapshot[f"channel::{channel.spec.name}::observed"])
        output.update(snapshot)
    output["channel_support_valid"] = bool(channel_support)
    output["market_support_valid"] = bool(state.warmup_admitted and channel_support)
    return _complete_market_schema(output, block=state.block)


def _complete_market_schema(row: Mapping[str, Any], *, block: str) -> dict[str, Any]:
    """Give observed and unsupported days one stable engine-column schema."""

    output = dict(row)
    excluded = {
        "schema_version",
        "identity",
        "feature_block",
        "support_valid",
        *feature_engine.M0_REQUIRED_FIELDS,
    }
    for name in expected_feature_columns(block):
        if name in excluded or name in output:
            continue
        if name.startswith("tri::"):
            output[name] = int(feature_engine.TriState.UNOBSERVED)
        elif name.startswith("channel::") and name.endswith("::observed"):
            output[name] = 0
        elif name == "channel_support_valid":
            output[name] = False
        else:
            output[name] = None
    return output


def _unsupported_m2_market_row(
    m1_market: Mapping[str, Any],
    *,
    decision_ts_ns: int,
) -> dict[str, Any]:
    """Retain an excluded day's row without fabricating raw M2 state."""

    output = _complete_market_schema(dict(m1_market), block="M2")
    m2_only_channels = {
        spec.name for spec in feature_engine.CHANNELS_BY_BLOCK["M2"]
    } - {spec.name for spec in feature_engine.CHANNELS_BY_BLOCK["M1"]}
    for name in expected_feature_columns("M2"):
        if not any(f"::{channel}" in name for channel in m2_only_channels):
            continue
        if name.startswith("tri::"):
            output[name] = int(feature_engine.TriState.UNOBSERVED)
        elif name.startswith("channel::") and name.endswith("::observed"):
            output[name] = 0
        else:
            output[name] = None
    output.update(
        {
            "decision_ts_ns": int(decision_ts_ns),
            "channel_support_valid": False,
            "market_support_valid": False,
            "m2_source_support_valid": False,
            "m2_source_support_reason": "frozen_prefix40_raw_M2_excluded_day",
        }
    )
    return output


def _supported_m2_market_row(
    m1_market: Mapping[str, Any],
    raw_m2_market: Mapping[str, Any],
    *,
    decision_ts_ns: int,
) -> dict[str, Any]:
    """Keep normalized M1 state and append only raw-native M2 channels."""

    output = _complete_market_schema(dict(m1_market), block="M2")
    m2_only_channels = {
        spec.name for spec in feature_engine.CHANNELS_BY_BLOCK["M2"]
    } - {spec.name for spec in feature_engine.CHANNELS_BY_BLOCK["M1"]}
    for name in expected_feature_columns("M2"):
        if any(f"::{channel}" in name for channel in m2_only_channels):
            output[name] = raw_m2_market.get(name)
    normalized_ready = int(m1_market["feature_ready_ts_ns"])
    raw_ready = int(raw_m2_market["feature_ready_ts_ns"])
    if normalized_ready > int(decision_ts_ns) or raw_ready > int(decision_ts_ns):
        raise ModeledFeaturePanelError("M1/M2 source crossed the fill-visible cutoff")
    output.update(
        {
            "decision_ts_ns": int(decision_ts_ns),
            "feature_ready_ts_ns": max(normalized_ready, raw_ready),
            "normalized_m1_feature_ready_ts_ns": normalized_ready,
            "normalized_m1_market_generation": int(
                m1_market.get("market_generation", 0)
            ),
            "normalized_m1_depth_generation": int(
                m1_market.get("depth_generation", 0)
            ),
            "raw_m2_feature_ready_ts_ns": raw_ready,
            "raw_m2_market_generation": int(
                raw_m2_market.get("market_generation", 0)
            ),
            "raw_m2_depth_generation": int(
                raw_m2_market.get("depth_generation", 0)
            ),
            "channel_support_valid": bool(
                m1_market.get("channel_support_valid", False)
                and raw_m2_market.get("channel_support_valid", False)
            ),
            "market_support_valid": bool(
                m1_market.get("market_support_valid", False)
                and raw_m2_market.get("market_support_valid", False)
            ),
            "m2_source_support_valid": bool(
                m1_market.get("market_support_valid", False)
                and raw_m2_market.get("market_support_valid", False)
            ),
            "m2_source_support_reason": "raw_M2_common_support_day",
        }
    )
    return output


def _immutable_r0_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """Project the v1 R0 values byte-for-value without recomputation."""

    output = {name: row[name] for name in IMMUTABLE_R0_COLUMNS}
    output.update(
        {
            "immutable_r0_source": "v1_opportunities_parquet_direct_projection",
            "immutable_r0_predicate_count": len(IMMUTABLE_R0_PREDICATE_COLUMNS),
            "immutable_r0_continuous_count": len(IMMUTABLE_R0_CONTINUOUS_COLUMNS),
            "market_support_valid": True,
        }
    )
    return output


def _common_owner_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "utc_day": str(row["utc_day"]),
        "opportunity_id": str(row["opportunity_id"]),
        "label_join_key": "opportunity_id",
        "exposure_fill_ordinal": int(row["exposure_fill_ordinal"]),
        "fill_visible_ts_ms": int(row["fill_visible_ts_ms"]),
        "fill_exchange_ts_ms": int(row["fill_exchange_ts_ms"]),
        "side": str(row["side"]).upper(),
        "role_at_fill": str(row["role_at_fill"]).lower(),
        "order_id": int(row["order_id"]),
        "campaign_id": int(row["campaign_id"]),
        "campaign_side_id": str(row["campaign_side_id"]),
        "inventory_before_fill_btc": float(row["inventory_before_fill_btc"]),
        "inventory_after_fill_btc": float(row["inventory_after_fill_btc"]),
        "fill_qty_btc": float(row["fill_qty_btc"]),
        "unit_qty_btc": float(row["unit_qty_btc"]),
        "consecutive_units_before": float(row["consecutive_units_before"]),
        "consecutive_units_after": float(row["consecutive_units_after"]),
        "prior_deadline_ts_ms": int(row["prior_deadline_ts_ms"]),
        "baseline_duration_ms": float(row["baseline_duration_ms"]),
        "baseline_deadline_ts_ms": int(row["baseline_deadline_ts_ms"]),
        "canonical_mid": float(row["canonical_mid"]),
        "best_bid": float(row["best_bid"]),
        "best_ask": float(row["best_ask"]),
        "decision_visible_bbo_index": int(row["decision_visible_bbo_index"]),
        "decision_visible_l2_index": int(row["decision_visible_l2_index"]),
        "market_event_index": int(row["market_event_index"]),
        "assignment_ts_ns": int(row["assignment_ts_ns"]),
        "owner_modeled_queue": True,
        "queue_evidence_identity": OWNER_QUEUE_IDENTITY,
        "queue_path_semantics": QUEUE_PATH_SEMANTICS,
        "exact_queue_policy_eligible": False,
        "live_receive_time_authority": False,
        "economic_outcomes_read": False,
        "arm_economic_labels_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_policy_authorized": False,
    }


def _tri_state(value: bool | None) -> int:
    if value is None:
        return int(feature_engine.TriState.UNOBSERVED)
    return int(feature_engine.TriState.TRUE if value else feature_engine.TriState.FALSE)


def _finite_or_none(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _m0_boolean_predicates(
    m0: Mapping[str, Any],
    *,
    unit_qty_btc: float,
) -> dict[str, int]:
    """Outcome-blind structural predicates for the cooldown action context.

    These predicates use only identities, natural inventory-unit boundaries,
    and comparisons between fields visible in the same fill snapshot.  They do
    not introduce an outcome-selected time threshold.
    """

    role_raw = m0.get("role_at_fill")
    role = None if role_raw is None else str(role_raw).lower()
    baseline_ms = _finite_or_none(m0.get("baseline_duration_ms"))
    campaign_age_s = _finite_or_none(m0.get("campaign_age_s"))
    inventory_after = _finite_or_none(m0.get("inventory_after_fill_btc"))
    unit_qty = _finite_or_none(unit_qty_btc)
    units_after = (
        None
        if inventory_after is None or unit_qty is None or unit_qty <= 0.0
        else abs(inventory_after) / unit_qty
    )
    last_same_age = _finite_or_none(m0.get("last_same_side_fill_age_s"))
    last_opposite_age = _finite_or_none(m0.get("last_opposite_side_fill_age_s"))
    queue_raw = m0.get("queue_state_before_fill")
    queue_state = None if queue_raw is None else str(queue_raw)
    owner_raw = m0.get("cooldown_deadline_owner")
    owner = None if owner_raw is None else str(owner_raw)
    campaign_add_count = _finite_or_none(m0.get("campaign_add_count"))
    campaign_mae = _finite_or_none(m0.get("campaign_mae_to_date_usdc"))
    consecutive_units = _finite_or_none(m0.get("consecutive_units_after"))

    def optional_bool(name: str) -> bool | None:
        value = m0.get(name)
        return None if value is None else bool(value)

    predicates: dict[str, bool | None] = {
        "role_is_add": None if role is None else role == "add",
        "fill_is_partial": optional_bool("fill_is_partial"),
        "queue_state_known": (
            None if queue_state is None else queue_state in {"exact", "known_zero"}
        ),
        "target_displayed_qty_known": optional_bool(
            "target_price_displayed_qty_known"
        ),
        "cooldown_blocker_active": optional_bool("cooldown_blocker_active"),
        "existing_cooldown_owner": None if owner is None else owner != "none",
        "previous_same_side_fill_observed": last_same_age is not None,
        "previous_opposite_side_fill_observed": last_opposite_age is not None,
        "campaign_has_prior_add": (
            None if campaign_add_count is None else campaign_add_count >= 1.0
        ),
        "campaign_add_count_ge_2": (
            None if campaign_add_count is None else campaign_add_count >= 2.0
        ),
        "campaign_mae_negative": None if campaign_mae is None else campaign_mae < 0.0,
        "campaign_age_gt_control_duration": (
            None
            if campaign_age_s is None or baseline_ms is None
            else campaign_age_s * 1_000.0 > baseline_ms
        ),
        "same_fill_age_le_control_duration": (
            None
            if last_same_age is None or baseline_ms is None
            else last_same_age * 1_000.0 <= baseline_ms
        ),
        "opposite_fill_age_le_control_duration": (
            None
            if last_opposite_age is None or baseline_ms is None
            else last_opposite_age * 1_000.0 <= baseline_ms
        ),
    }
    for threshold in (2, 3, 4, 5, 6):
        predicates[f"consecutive_units_ge_{threshold}"] = (
            None
            if consecutive_units is None
            else consecutive_units >= float(threshold)
        )
        predicates[f"inventory_units_after_ge_{threshold}"] = (
            None if units_after is None else units_after >= float(threshold) - 1e-10
        )
        predicates[f"control_duration_ge_{threshold}_units"] = (
            None
            if baseline_ms is None
            else baseline_ms >= 85_000.0 * float(threshold) - 1e-9
        )
    return {
        f"{M0_PREDICATE_PREFIX}{name}": _tri_state(value)
        for name, value in predicates.items()
    }


def build_feature_frames(
    opportunities: pd.DataFrame,
    *,
    m1_observations: Iterable[feature_engine.CausalWindowObservation],
    m1_warmup_identity: str,
    m2_observations: Iterable[feature_engine.CausalWindowObservation] | None = None,
    m2_warmup_identity: str | None = None,
    m0_enrichment: pd.DataFrame | None = None,
    allow_reduced_m0: bool = False,
    m2_day_supported: bool | None = None,
) -> tuple[dict[str, pd.DataFrame], FeatureBuildAudit]:
    """Join immutable R0, normalized M1, and optional raw M2 without outcomes."""

    if not str(m1_warmup_identity).strip():
        raise ModeledFeaturePanelError("normalized M1 warmup identity is empty")
    day_values = opportunities["utc_day"].astype(str).unique()
    if len(day_values) != 1:
        raise ModeledFeaturePanelError("feature build requires exactly one UTC day")
    day = str(day_values[0])
    if day in PREFIX40_DAYS:
        frozen_m2_support = day in M2_COMMON_SUPPORT_DAYS
        if m2_day_supported is not None and bool(m2_day_supported) != frozen_m2_support:
            raise ModeledFeaturePanelError(
                f"M2 support override disagrees with frozen prefix split for {day}"
            )
        m2_day_supported = frozen_m2_support
    elif m2_day_supported is None:
        raise ModeledFeaturePanelError(
            "non-prefix test/diagnostic day requires explicit M2 support identity"
        )
    m2_day_supported = bool(m2_day_supported)
    census = _validate_census_frame(
        opportunities.loc[:, list(CENSUS_SAFE_PROJECTION_COLUMNS)].copy(),
        day=day,
        require_immutable_r0=True,
    )

    if m2_day_supported and m2_observations is None:
        raise ModeledFeaturePanelError(
            "M2-supported day requires a distinct raw-native M2 stream"
        )
    if not m2_day_supported and m2_observations is not None:
        raise ModeledFeaturePanelError(
            "raw-native M2 stream is forbidden on a frozen M2-excluded day"
        )
    if m2_day_supported and not str(m2_warmup_identity or "").strip():
        raise ModeledFeaturePanelError("raw-native M2 warmup identity is empty")

    if m0_enrichment is None:
        if not allow_reduced_m0:
            missing = sorted(
                set(feature_engine.M0_REQUIRED_FIELDS) - DIRECT_M0_FROM_CENSUS
            )
            raise ModeledFeaturePanelError(
                "full M0 enrichment is required; explicitly opt into reduced "
                f"UNOBSERVED support to proceed without: {missing}"
            )
        m0_by_id: dict[str, dict[str, Any]] = {}
        full_m0_support = False
        support_identity = REDUCED_M0_SUPPORT_IDENTITY
    else:
        normalized_m0 = validate_m0_enrichment(census, m0_enrichment)
        m0_by_id = {
            str(row["opportunity_id"]): {
                name: row[name] for name in feature_engine.M0_REQUIRED_FIELDS
            }
            for row in normalized_m0.to_dict("records")
        }
        full_m0_support = True
        support_identity = FULL_M0_SUPPORT_IDENTITY

    m1_state = feature_engine.CausalMultichannelEmaState(block="M1")
    raw_m2_state = (
        feature_engine.CausalMultichannelEmaState(block="M2")
        if m2_day_supported
        else None
    )
    m1_iterator = iter(m1_observations)
    next_m1 = next(m1_iterator, None)
    raw_m2_iterator = iter(m2_observations or ())
    next_raw_m2 = next(raw_m2_iterator, None)
    m1_observation_count = 0
    m1_last_right_ts_ns = 0
    raw_m2_observation_count = 0
    raw_m2_last_right_ts_ns = 0
    output_rows: dict[str, list[dict[str, Any]]] = {
        block: [] for block in FEATURE_BLOCKS
    }

    for census_row in census.to_dict("records"):
        cutoff_ns = int(census_row["fill_visible_ts_ms"]) * 1_000_000
        while next_m1 is not None and int(next_m1.feature_ready_ts_ns) <= cutoff_ns:
            m1_observation = next_m1
            m1_state.update(_project_observation(m1_observation, block="M1"))
            if bool(m1_observation.warmup_admitted):
                m1_state.warmup_admitted = True
                m1_state.warmup_identity = str(m1_warmup_identity)
            m1_observation_count += 1
            m1_last_right_ts_ns = int(m1_observation.right_ts_ns)
            next_m1 = next(m1_iterator, None)
        while (
            raw_m2_state is not None
            and next_raw_m2 is not None
            and int(next_raw_m2.feature_ready_ts_ns) <= cutoff_ns
        ):
            raw_observation = next_raw_m2
            raw_m2_state.update(_project_observation(raw_observation, block="M2"))
            if bool(raw_observation.warmup_admitted):
                raw_m2_state.warmup_admitted = True
                raw_m2_state.warmup_identity = str(m2_warmup_identity)
            raw_m2_observation_count += 1
            raw_m2_last_right_ts_ns = int(raw_observation.right_ts_ns)
            next_raw_m2 = next(raw_m2_iterator, None)

        if not m1_observation_count:
            raise ModeledFeaturePanelError(
                "first opportunity precedes every normalized M1 feature-ready window"
            )
        if m2_day_supported and not raw_m2_observation_count:
            raise ModeledFeaturePanelError(
                "first opportunity precedes every raw-native M2 feature-ready window"
            )
        if (
            cutoff_ns - int(m1_state.last_right_ts_ns or 0)
            >= feature_engine.BASE_WINDOW_WIDTH_NS
        ):
            m1_state.mark_current_window_unobserved()
        if (
            raw_m2_state is not None
            and cutoff_ns - int(raw_m2_state.last_right_ts_ns or 0)
            >= feature_engine.BASE_WINDOW_WIDTH_NS
        ):
            raw_m2_state.mark_current_window_unobserved()

        opportunity_id = str(census_row["opportunity_id"])
        m0 = (
            m0_by_id[opportunity_id]
            if full_m0_support
            else _reduced_m0_context(census_row)
        )
        common = _common_owner_row(census_row)
        immutable_r0 = _immutable_r0_row(census_row)
        m0_fields = {name: m0[name] for name in feature_engine.M0_REQUIRED_FIELDS}
        m0_predicates = _m0_boolean_predicates(
            m0,
            unit_qty_btc=float(common["unit_qty_btc"]),
        )
        m0_meta = {
            "m0_support_valid": bool(full_m0_support),
            "m0_support_identity": support_identity,
            "m0_observed_fields_json": json.dumps(
                sorted(feature_engine.M0_REQUIRED_FIELDS)
                if full_m0_support
                else sorted(DIRECT_M0_FROM_CENSUS),
                separators=(",", ":"),
            ),
            "m0_unobserved_fields_json": json.dumps(
                []
                if full_m0_support
                else sorted(
                    set(feature_engine.M0_REQUIRED_FIELDS) - DIRECT_M0_FROM_CENSUS
                ),
                separators=(",", ":"),
            ),
            "m0_missing_value_semantics": (
                "none_full_explicit_enrichment"
                if full_m0_support
                else "null_means_UNOBSERVED_not_zero"
            ),
        }

        m1_market = _market_feature_row(
            m1_state,
            side=common["side"],
            decision_ts_ns=cutoff_ns,
        )
        m2_market = (
            _supported_m2_market_row(
                m1_market,
                _market_feature_row(
                    raw_m2_state,
                    side=common["side"],
                    decision_ts_ns=cutoff_ns,
                ),
                decision_ts_ns=cutoff_ns,
            )
            if m2_day_supported
            else _unsupported_m2_market_row(m1_market, decision_ts_ns=cutoff_ns)
        )

        output_rows["R0"].append(
            {
                **common,
                "feature_block": "R0",
                **immutable_r0,
                "m0_support_valid": False,
                "m0_support_identity": "not_part_of_R0_estimand",
                "support_valid": True,
            }
        )
        output_rows["M0"].append(
            {
                **common,
                "feature_block": "M0",
                **m0_fields,
                **m0_meta,
                **m0_predicates,
                "market_support_valid": True,
                "support_valid": bool(full_m0_support),
            }
        )
        output_rows["M1"].append(
            {
                **common,
                "feature_block": "M1",
                **m0_fields,
                **m0_meta,
                **m0_predicates,
                **immutable_r0,
                **m1_market,
                "support_valid": bool(
                    full_m0_support and m1_market["market_support_valid"]
                ),
            }
        )
        output_rows["M2"].append(
            {
                **common,
                "feature_block": "M2",
                **m0_fields,
                **m0_meta,
                **m0_predicates,
                **immutable_r0,
                **m2_market,
                "support_valid": bool(
                    full_m0_support
                    and m2_day_supported
                    and m2_market["market_support_valid"]
                ),
            }
        )

    frames = {
        block: pd.DataFrame(rows).sort_values(
            "exposure_fill_ordinal", kind="stable"
        ).reset_index(drop=True)
        for block, rows in output_rows.items()
    }
    supported = {
        block: int(frame["support_valid"].astype(bool).sum())
        for block, frame in frames.items()
    }
    return frames, FeatureBuildAudit(
        opportunity_count=int(len(census)),
        observation_count=int(
            m1_observation_count + raw_m2_observation_count
        ),
        last_observation_right_ts_ns=max(
            int(m1_last_right_ts_ns),
            int(raw_m2_last_right_ts_ns),
        ),
        full_m0_support=bool(full_m0_support),
        support_identity=support_identity,
        m2_day_supported=bool(m2_day_supported),
        block_supported_rows=supported,
        normalized_m1_observation_count=int(m1_observation_count),
        normalized_m1_last_right_ts_ns=int(m1_last_right_ts_ns),
        raw_m2_observation_count=int(raw_m2_observation_count),
        raw_m2_last_right_ts_ns=int(raw_m2_last_right_ts_ns),
    )


def _parquet_schema_sha256(path: Path) -> str:
    schema = pq.ParquetFile(path).schema_arrow
    return canonical_sha256(
        [(field.name, str(field.type), field.nullable) for field in schema]
    )


def admit_feature_day(
    *,
    day: str,
    frames: Mapping[str, pd.DataFrame],
    audit: FeatureBuildAudit,
    output_root: Path,
    census_binding: Mapping[str, Any],
    m0_binding: Mapping[str, Any],
    source_binding: Mapping[str, Any],
) -> dict[str, Any]:
    """Publish all four feature blocks and the day manifest atomically."""

    if set(frames) != set(FEATURE_BLOCKS):
        raise ModeledFeaturePanelError("feature block universe drifted")
    source_split = source_binding.get("source_split_semantics")
    if not isinstance(source_split, Mapping):
        raise ModeledFeaturePanelError("source split semantics are missing")
    expected_source_split = {
        "schema_version": SOURCE_SPLIT_SCHEMA_VERSION,
        "r0_source_identity": R0_SOURCE_IDENTITY,
        "m1_source_identity": M1_SOURCE_IDENTITY,
        "m1_supported": True,
        "raw_m2_used_for_m1": False,
        "normalized_m1_source_binding_sha256": source_split.get(
            "normalized_m1_source_binding_sha256"
        ),
        "m2_source_identity": M2_SOURCE_IDENTITY,
        "m2_supported": bool(audit.m2_day_supported),
        "raw_m2_source_opened": bool(audit.m2_day_supported),
        "raw_m2_source_binding_sha256": source_split.get(
            "raw_m2_source_binding_sha256"
        ),
    }
    if dict(source_split) != expected_source_split:
        raise ModeledFeaturePanelError("source split semantics drifted")
    if not _is_sha256(source_split["normalized_m1_source_binding_sha256"]):
        raise ModeledFeaturePanelError("normalized M1 source digest is invalid")
    raw_digest = source_split["raw_m2_source_binding_sha256"]
    if audit.m2_day_supported:
        if not _is_sha256(raw_digest):
            raise ModeledFeaturePanelError("supported raw M2 source digest is invalid")
    elif raw_digest is not None:
        raise ModeledFeaturePanelError("excluded raw M2 source digest must be null")
    final = Path(output_root).expanduser().resolve() / str(day)
    if final.exists():
        raise ModeledFeaturePanelError(f"feature day already exists: {final}")
    final.parent.mkdir(parents=True, exist_ok=True)
    staging = final.parent / f".{day}.staging-{os.getpid()}-{uuid.uuid4().hex}"
    staging.mkdir()
    try:
        block_bindings: dict[str, Any] = {}
        for block in FEATURE_BLOCKS:
            frame = frames[block]
            _reject_economic_columns(frame.columns, source=f"{block} feature frame")
            if len(frame) != audit.opportunity_count:
                raise ModeledFeaturePanelError(f"{block} row count drifted")
            path = staging / f"{block}.parquet"
            table = pa.Table.from_pandas(frame, preserve_index=False)
            pq.write_table(table, path, compression="zstd")
            _fsync_file(path)
            block_bindings[block] = {
                "path": f"{block}.parquet",
                "sha256": sha256_file(path),
                "schema_sha256": _parquet_schema_sha256(path),
                "row_count": int(len(frame)),
                "column_count": int(len(frame.columns)),
                "support_valid_count": int(frame["support_valid"].astype(bool).sum()),
            }

        code_bindings = {
            "builder_sha256": sha256_file(Path(__file__)),
            "feature_engine_sha256": sha256_file(
                Path(feature_engine.__file__).resolve()
            ),
            "native_feature_engine_sha256": sha256_file(
                Path(native_features.__file__).resolve()
            ),
        }
        from research.families.f05_fill_quality_quote_ev.audit import (
            causal_multichannel_window_boolean_cooldown_modeled_feature_batch as batch_builder,
        )

        code_bindings["batch_builder_sha256"] = sha256_file(
            Path(batch_builder.__file__).resolve()
        )
        input_identity = {
            "census_binding_sha256": str(census_binding["binding_sha256"]),
            "m0_binding_sha256": str(m0_binding["binding_sha256"]),
            "source_binding_sha256": canonical_sha256(source_binding),
            "code_bindings": code_bindings,
            "feature_schema_sha256": canonical_sha256(feature_engine.feature_schema()),
            "native_feature_schema_sha256": canonical_sha256(
                native_features.native_m2_book_feature_schema()
            ),
        }
        manifest: dict[str, Any] = {
            "schema_version": f"{SCHEMA_VERSION}.day_manifest.v1",
            "identity": IDENTITY,
            "utc_day": str(day),
            "support_identity": audit.support_identity,
            "owner_modeled_queue": True,
            "queue_evidence_identity": OWNER_QUEUE_IDENTITY,
            "queue_path_semantics": QUEUE_PATH_SEMANTICS,
            "exact_queue_policy_eligible": False,
            "full_m0_support": bool(audit.full_m0_support),
            "m2_day_supported": bool(audit.m2_day_supported),
            "source_split_semantics": dict(source_split),
            "frozen_support_split": {
                "prefix40_days": list(PREFIX40_DAYS),
                "prefix40_day_count": len(PREFIX40_DAYS),
                "m2_common_support_days": list(M2_COMMON_SUPPORT_DAYS),
                "m2_common_support_day_count": len(M2_COMMON_SUPPORT_DAYS),
                "m2_excluded_days": sorted(M2_EXCLUDED_DAYS),
                "denominator_rows_preserved_on_m2_excluded_days": True,
            },
            "opportunity_count": int(audit.opportunity_count),
            "observation_count": int(audit.observation_count),
            "last_observation_right_ts_ns": int(audit.last_observation_right_ts_ns),
            "normalized_m1_observation_count": int(
                audit.normalized_m1_observation_count
            ),
            "normalized_m1_last_right_ts_ns": int(
                audit.normalized_m1_last_right_ts_ns
            ),
            "raw_m2_observation_count": int(audit.raw_m2_observation_count),
            "raw_m2_last_right_ts_ns": int(audit.raw_m2_last_right_ts_ns),
            "block_supported_rows": dict(audit.block_supported_rows),
            "blocks": block_bindings,
            "census_binding": dict(census_binding),
            "m0_binding": dict(m0_binding),
            "source_binding": dict(source_binding),
            "input_identity": input_identity,
            "input_identity_sha256": canonical_sha256(input_identity),
            "label_join_key": "opportunity_id",
            "arm_label_paths_opened": [],
            "economic_outcomes_read": False,
            "arm_economic_labels_read": False,
            "model_trained": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_policy_authorized": False,
        }
        manifest["canonical_manifest_sha256"] = canonical_sha256(manifest)
        _atomic_json(staging / "manifest.json", manifest)
        success = staging / DAY_SUCCESS
        success.write_text(f"{manifest['canonical_manifest_sha256']}\n", encoding="ascii")
        _fsync_file(success)
        _fsync_directory(staging)
        os.replace(staging, final)
        _fsync_directory(final.parent)
        return manifest
    except BaseException:
        if staging.exists():
            for child in staging.iterdir():
                child.unlink(missing_ok=True)
            staging.rmdir()
        raise


def _individual_trade_paths(days: Sequence[str]) -> tuple[Path, ...]:
    root = Path(bt.RAW_TRADES_DIR) / str(bt.SYMBOL)
    output: list[Path] = []
    for day in days:
        candidates = (
            root / f"{bt.SYMBOL}-trades-{day}.csv",
            root / f"{bt.SYMBOL}-trades-{day}.csv.gz",
        )
        found = [path.resolve() for path in candidates if path.is_file()]
        if len(found) != 1:
            raise ModeledFeaturePanelError(
                f"official individual trades require one source for {day}: {found}"
            )
        output.append(found[0])
    return tuple(output)


def _load_normalized_m1_source(
    day: str,
    *,
    execution_plan_path: Path,
) -> tuple[
    Iterable[feature_engine.CausalWindowObservation],
    normalized_windows.WindowExtractionAccumulator,
    dict[str, Any],
]:
    """Open the exact normalized WindowData source used by the v1 labels."""

    plan_path = Path(execution_plan_path).expanduser().resolve()
    plan = _load_json(plan_path)
    payload = plan.get("identity_payload")
    if (
        plan.get("identity") != EXPECTED_NORMALIZED_PLAN_IDENTITY
        or not isinstance(payload, Mapping)
        or plan.get("plan_identity_sha256") != canonical_sha256(payload)
        or plan.get("day_count") != len(PREFIX40_DAYS)
        or plan.get("economic_outcomes_read") is not False
        or plan.get("development_pnl_read") is not False
        or plan.get("validation_read") is not False
        or plan.get("sealed_holdout_read") is not False
    ):
        raise ModeledFeaturePanelError("normalized execution-plan identity drifted")
    marker = plan_path.parent / "_PLAN_SUCCESS"
    if (
        not marker.is_file()
        or marker.read_text(encoding="ascii").strip() != sha256_file(plan_path)
    ):
        raise ModeledFeaturePanelError("normalized execution-plan admission drifted")
    ordered_days = tuple(str(value) for value in payload.get("ordered_utc_days", ()))
    if ordered_days != PREFIX40_DAYS:
        raise ModeledFeaturePanelError("normalized execution-plan denominator drifted")
    matches = [
        row
        for row in payload.get("days", ())
        if isinstance(row, Mapping) and str(row.get("utc_day")) == str(day)
    ]
    if len(matches) != 1:
        raise ModeledFeaturePanelError(f"normalized plan lacks one day row: {day}")
    day_row = matches[0]
    window_binding = day_row.get("window")
    if not isinstance(window_binding, Mapping):
        raise ModeledFeaturePanelError("normalized plan window binding is missing")
    window_path = Path(str(window_binding.get("path", ""))).expanduser().resolve()
    if not window_path.is_file():
        raise ModeledFeaturePanelError(f"bound normalized window is missing: {window_path}")
    if window_path.stat().st_size != int(window_binding.get("size_bytes", -1)):
        raise ModeledFeaturePanelError("bound normalized window size drifted")
    observed_window_sha256 = sha256_file(window_path)
    if observed_window_sha256 != str(window_binding.get("sha256", "")):
        raise ModeledFeaturePanelError("bound normalized window SHA256 drifted")

    from research.families.f03_causal_13_head.audit import (
        causal_v12_1s_native_40day_full_path_ml_ab as f03_full_path,
    )

    try:
        window = f03_full_path._load_bound_window(window_path)
    except f03_full_path.NativeFullPathABError as exc:
        raise ModeledFeaturePanelError("bound normalized WindowData was rejected") from exc
    bbo = getattr(window, "bbo_data", None)
    l2 = getattr(window, "l2_data", None)
    if (
        bbo is None
        or l2 is None
        or getattr(window, "book_source_authority", None) != "native_formal_lifecycle"
        or not bool(getattr(window, "formal_lifecycle_replay_eligible", False))
    ):
        raise ModeledFeaturePanelError("normalized WindowData source authority drifted")
    bbo_ts_ms = np.asarray(bbo.ts_ms, dtype=np.int64)
    l2_ts_ms = np.asarray(l2.ts_ms, dtype=np.int64)
    if (
        bbo_ts_ms.size == 0
        or l2_ts_ms.size == 0
        or np.any(np.diff(bbo_ts_ms) <= 0)
        or np.any(np.diff(l2_ts_ms) <= 0)
    ):
        raise ModeledFeaturePanelError("normalized BBO/L2 clock is invalid")
    width_ns = feature_engine.BASE_WINDOW_WIDTH_NS
    bbo_source_start_ns = int(bbo_ts_ms[0]) * 1_000_000
    l2_source_start_ns = int(l2_ts_ms[0]) * 1_000_000
    bbo_source_end_ns = int(bbo_ts_ms[-1]) * 1_000_000
    l2_source_end_ns = int(l2_ts_ms[-1]) * 1_000_000
    source_start_ns = min(bbo_source_start_ns, l2_source_start_ns)
    common_source_start_ns = max(bbo_source_start_ns, l2_source_start_ns)
    left_ns = source_start_ns - source_start_ns % width_ns
    target_start_ns = int(pd.Timestamp(str(day), tz="UTC").value)
    target_end_ns = int(
        (pd.Timestamp(str(day), tz="UTC") + pd.Timedelta(days=1)).value
    )
    if left_ns >= target_start_ns or target_start_ns >= target_end_ns:
        raise ModeledFeaturePanelError("normalized WindowData does not precede target end")
    audit = normalized_windows.WindowExtractionAccumulator()
    contract = normalized_windows.WindowExtractionContract(
        block="M1",
        source_clock_profile=normalized_windows.STRICT_EXCHANGE_TIME_PROFILE,
        left_ts_ns=left_ns,
        right_ts_ns=target_end_ns,
    )
    stream = normalized_windows.stream_causal_windows(
        contract=contract,
        bbo=bbo,
        l2=l2,
        trades=None,
        audit=audit,
    )

    def admitted_stream() -> Iterable[feature_engine.CausalWindowObservation]:
        for observation in stream:
            yield replace(
                observation,
                warmup_admitted=bool(observation.right_ts_ns >= target_start_ns),
            )

    required_full_warmup_start_ns = target_start_ns - 86_400_000_000_000
    exact_full_24h_warmup = bool(
        common_source_start_ns <= required_full_warmup_start_ns
    )
    available_warmup_span_ns = max(0, target_start_ns - common_source_start_ns)

    source_identity = {
        "identity": "v1_modeled_queue_bound_normalized_window_m1",
        "execution_plan_path": str(plan_path),
        "execution_plan_sha256": sha256_file(plan_path),
        "execution_plan_identity_sha256": str(plan["plan_identity_sha256"]),
        "daily_source_identity_sha256": str(
            day_row.get("daily_source_identity_sha256", "")
        ),
        "window_path": str(window_path),
        "window_sha256": observed_window_sha256,
        "window_size_bytes": int(window_path.stat().st_size),
        "window_book_source_authority": str(window.book_source_authority),
        "bbo_source": str(getattr(bbo, "source", "")),
        "l2_source": str(getattr(l2, "source", "")),
        "observed_source_start_ts_ns": int(source_start_ns),
        "observed_bbo_source_start_ts_ns": int(bbo_source_start_ns),
        "observed_l2_source_start_ts_ns": int(l2_source_start_ns),
        "observed_common_source_start_ts_ns": int(common_source_start_ns),
        "observed_bbo_source_end_ts_ns": int(bbo_source_end_ns),
        "observed_l2_source_end_ts_ns": int(l2_source_end_ns),
        "feature_window_left_ts_ns": int(left_ns),
        "target_day_start_ts_ns": int(target_start_ns),
        "feature_window_right_ts_ns": int(target_end_ns),
        "available_d_minus_1_warmup_span_ns": int(available_warmup_span_ns),
        "exact_full_24h_warmup": exact_full_24h_warmup,
        "warmup_admitted_at_target_day_start": True,
        "warmup_admission_semantics": (
            "target_day_start_after_consuming_bound_available_D_minus_1_span"
        ),
        "feature_clock_profile": normalized_windows.STRICT_EXCHANGE_TIME_PROFILE,
        "window_boundary": "100ms_left_closed_right_open_partial_excluded",
        "economic_outcomes_read": False,
        "arm_economic_labels_read": False,
    }
    source_identity["source_identity_sha256"] = canonical_sha256(source_identity)
    return admitted_stream(), audit, source_identity


def build_day_from_native_sources(
    day: str,
    *,
    census_root: Path = DEFAULT_CENSUS_ROOT,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    raw_native_root: Path = DEFAULT_RAW_NATIVE_ROOT,
    native_cache: Path = DEFAULT_NATIVE_CACHE,
    native_observation_cache_root: Path = DEFAULT_NATIVE_OBSERVATION_CACHE,
    normalized_execution_plan: Path = DEFAULT_NORMALIZED_EXECUTION_PLAN,
    m0_enrichment_path: Path | None = None,
    m0_enrichment_manifest: Path | None = None,
    allow_reduced_m0: bool = False,
) -> dict[str, Any]:
    """Build and atomically admit one historical exchange-time feature day."""

    opportunities, census_binding = load_census_day(day, census_root=census_root)
    if m0_enrichment_path is None:
        if not allow_reduced_m0:
            raise ModeledFeaturePanelError(
                "--m0-enrichment is required unless --allow-reduced-m0 is explicit"
            )
        enrichment = None
        m0_binding = reduced_m0_binding()
    else:
        if allow_reduced_m0:
            raise ModeledFeaturePanelError(
                "full M0 enrichment and reduced-M0 mode are mutually exclusive"
            )
        if m0_enrichment_manifest is None:
            raise ModeledFeaturePanelError(
                "full M0 enrichment requires an admitted day manifest"
            )
        enrichment, m0_binding = load_m0_enrichment(
            m0_enrichment_path,
            opportunities=opportunities,
            manifest_path=m0_enrichment_manifest,
            census_binding=census_binding,
        )

    m1_observations, normalized_audit, normalized_binding = (
        _load_normalized_m1_source(
            str(day),
            execution_plan_path=normalized_execution_plan,
        )
    )
    m2_day_supported = str(day) in M2_COMMON_SUPPORT_DAYS
    m2_observations: Iterable[feature_engine.CausalWindowObservation] | None = None
    raw_binding: dict[str, Any] | None = None
    raw_source_identity_sha256: str | None = None
    if m2_day_supported:
        admitted_cache = observation_cache.open_admitted_observation_cache(
            native_observation_cache_root,
            str(day),
            deep=False,
        )
        cache_manifest = dict(admitted_cache.manifest)
        if (
            cache_manifest.get("status")
            != "atomic_raw_native_observation_cache_admitted"
            or cache_manifest.get("formal_exchange_day") is not True
            or int(cache_manifest.get("observation_count", -1))
            != observation_cache.EXPECTED_FORMAL_WINDOW_COUNT
            or cache_manifest.get("economic_outcomes_read") is not False
            or cache_manifest.get("validation_read") is not False
            or cache_manifest.get("sealed_holdout_read") is not False
        ):
            raise ModeledFeaturePanelError(
                f"raw-native observation cache identity drifted for {day}"
            )
        cache_manifest_path = admitted_cache.day_root / observation_cache.MANIFEST_NAME
        cache_parquet_binding = cache_manifest.get("parquet")
        if not isinstance(cache_parquet_binding, Mapping):
            raise ModeledFeaturePanelError("raw-native cache Parquet binding is absent")
        raw_binding = {
            "identity": M2_SOURCE_IDENTITY,
            "feature_clock_profile": native_features.EXCHANGE_WINDOW_READY_CLOCK,
            "receive_time_transport_authority": False,
            "raw_native_observation_cache": {
                "identity": observation_cache.IDENTITY,
                "day_root": str(admitted_cache.day_root),
                "manifest_path": str(cache_manifest_path),
                "manifest_file_sha256": sha256_file(cache_manifest_path),
                "canonical_manifest_sha256": str(
                    cache_manifest["canonical_manifest_sha256"]
                ),
                "parquet_sha256": str(cache_parquet_binding["sha256"]),
                "observation_count": int(cache_manifest["observation_count"]),
                "observation_sha256": str(
                    cache_manifest["cache_readback_observation_sha256"]
                ),
                "source_binding_sha256": str(
                    cache_manifest["source_binding_sha256"]
                ),
                "implementation": dict(cache_manifest["implementation"]),
            },
            "raw_native_source_binding": dict(cache_manifest["source_binding"]),
            "official_trade_clock": "exchange_transact_time_ms",
            "window_boundary": "100ms_left_closed_right_open_partial_excluded",
            "economic_outcomes_read": False,
            "arm_economic_labels_read": False,
        }
        raw_source_identity_sha256 = canonical_sha256(raw_binding)
        raw_binding["source_identity_sha256"] = raw_source_identity_sha256
        raw_binding["book_feature_audit"] = dict(
            cache_manifest["book_feature_audit"]
        )
        raw_binding["trade_merge_audit"] = dict(cache_manifest["trade_merge_audit"])
        m2_observations = admitted_cache.observations()

    normalized_source_identity_sha256 = str(
        normalized_binding["source_identity_sha256"]
    )
    from research.families.f05_fill_quality_quote_ev.audit import (
        causal_multichannel_window_boolean_cooldown_modeled_feature_batch as batch_builder,
    )

    frames, audit = batch_builder.build_feature_frames_batch(
        opportunities,
        m1_observations=m1_observations,
        m1_warmup_identity=normalized_source_identity_sha256,
        m2_observations=m2_observations,
        m2_warmup_identity=raw_source_identity_sha256,
        m0_enrichment=enrichment,
        allow_reduced_m0=allow_reduced_m0,
    )
    normalized_binding["window_extraction_audit"] = asdict(normalized_audit.freeze())
    source_split_semantics = {
        "schema_version": SOURCE_SPLIT_SCHEMA_VERSION,
        "r0_source_identity": R0_SOURCE_IDENTITY,
        "m1_source_identity": M1_SOURCE_IDENTITY,
        "m1_supported": True,
        "raw_m2_used_for_m1": False,
        "normalized_m1_source_binding_sha256": (
            normalized_source_identity_sha256
        ),
        "m2_source_identity": M2_SOURCE_IDENTITY,
        "m2_supported": bool(m2_day_supported),
        "raw_m2_source_opened": bool(m2_day_supported),
        "raw_m2_source_binding_sha256": raw_source_identity_sha256,
    }
    source_binding = {
        "identity": "owner_modeled_queue_feature_source_split_v1",
        "immutable_r0": {
            "identity": R0_SOURCE_IDENTITY,
            "census_data_path": str(census_binding["data_path"]),
            "census_data_sha256": str(census_binding["data_sha256"]),
            "predicate_count": len(IMMUTABLE_R0_PREDICATE_COLUMNS),
            "continuous_count": len(IMMUTABLE_R0_CONTINUOUS_COLUMNS),
        },
        "normalized_m1": normalized_binding,
        "raw_m2": raw_binding,
        "source_split_semantics": source_split_semantics,
        "economic_outcomes_read": False,
        "arm_economic_labels_read": False,
    }
    return admit_feature_day(
        day=str(day),
        frames=frames,
        audit=audit,
        output_root=output_root,
        census_binding=census_binding,
        m0_binding=m0_binding,
        source_binding=source_binding,
    )


def resolve_m0_enrichment_day(
    root: Path,
    day: str,
) -> tuple[Path, Path | None]:
    """Resolve either the dedicated M0 provider or a generic enrichment layout."""

    base = Path(root).expanduser().resolve()
    candidates = (
        base / "days" / day / "m0_context.parquet",
        base / day / "m0_context.parquet",
        base / day / "m0_enrichment.parquet",
    )
    found = tuple(path for path in candidates if path.is_file())
    if len(found) != 1:
        raise ModeledFeaturePanelError(
            f"expected one M0 enrichment artifact for {day}, found: {found}"
        )
    manifest = found[0].parent / "manifest.json"
    if not manifest.is_file():
        raise ModeledFeaturePanelError(
            f"full M0 enrichment manifest is missing for {day}: {manifest}"
        )
    return found[0], manifest


def _requested_days(census_root: Path, requested: Sequence[str]) -> tuple[str, ...]:
    available = tuple(
        sorted(
            path.name
            for path in Path(census_root).expanduser().resolve().iterdir()
            if path.is_dir() and (path / "opportunities.parquet").is_file()
        )
    )
    if not requested:
        return available
    unknown = sorted(set(requested) - set(available))
    if unknown:
        raise ModeledFeaturePanelError(f"requested census days are unavailable: {unknown}")
    selected = set(requested)
    return tuple(day for day in available if day in selected)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--census-root", type=Path, default=DEFAULT_CENSUS_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--raw-native-root", type=Path, default=DEFAULT_RAW_NATIVE_ROOT)
    parser.add_argument("--native-cache", type=Path, default=DEFAULT_NATIVE_CACHE)
    parser.add_argument(
        "--native-observation-cache-root",
        type=Path,
        default=DEFAULT_NATIVE_OBSERVATION_CACHE,
    )
    parser.add_argument(
        "--normalized-execution-plan",
        type=Path,
        default=DEFAULT_NORMALIZED_EXECUTION_PLAN,
    )
    parser.add_argument("--days", nargs="*", default=())
    parser.add_argument(
        "--m0-enrichment-root",
        type=Path,
        help=(
            "Root containing days/<day>/m0_context.parquet or "
            "<day>/m0_enrichment.parquet plus an optional day manifest; "
            "only opportunity_id and the M0 projection are read."
        ),
    )
    parser.add_argument(
        "--allow-reduced-m0",
        action="store_true",
        help="Emit null/UNOBSERVED missing M0 fields under a reduced identity.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.m0_enrichment_root is None and not args.allow_reduced_m0:
        raise ModeledFeaturePanelError(
            "provide --m0-enrichment-root or explicitly select --allow-reduced-m0"
        )
    if args.m0_enrichment_root is not None and args.allow_reduced_m0:
        raise ModeledFeaturePanelError(
            "--m0-enrichment-root and --allow-reduced-m0 are mutually exclusive"
        )
    days = _requested_days(args.census_root, tuple(args.days))
    for day in days:
        enrichment_path: Path | None = None
        enrichment_manifest: Path | None = None
        if args.m0_enrichment_root is not None:
            enrichment_path, enrichment_manifest = resolve_m0_enrichment_day(
                args.m0_enrichment_root,
                day,
            )
        manifest = build_day_from_native_sources(
            day,
            census_root=args.census_root,
            output_root=args.output_root,
            raw_native_root=args.raw_native_root,
            native_cache=args.native_cache,
            native_observation_cache_root=args.native_observation_cache_root,
            normalized_execution_plan=args.normalized_execution_plan,
            m0_enrichment_path=enrichment_path,
            m0_enrichment_manifest=enrichment_manifest,
            allow_reduced_m0=bool(args.allow_reduced_m0),
        )
        print(
            json.dumps(
                {
                    "utc_day": day,
                    "support_identity": manifest["support_identity"],
                    "opportunity_count": manifest["opportunity_count"],
                    "manifest_sha256": manifest["canonical_manifest_sha256"],
                },
                sort_keys=True,
            ),
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
