#!/usr/bin/env python3
"""Frozen trainable schema and source contracts for the F03 1s successor."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any

SCHEMA_VERSION = "causal_v12_1s_trainable_schema.v1"
IDENTITY = "causal_v12_cadence_1s_source_aware_semantics_successor_v1"
FEATURE_DAG_ID = "causal_v12_full_trainable_1s.v1"
FEATURE_SEMANTICS_IDENTITY = "causal_v12_full_trainable_features_canonical_1s.v1"
LABEL_CONTRACT_IDENTITY = "causal_v12_13_head_label_contract_v3_preserved"
FEATURE_SEMANTICS_VERSION = 7
LABEL_SEMANTICS_VERSION = 3
CADENCE_MS = 1_000

REFERENCE_V12_ARTIFACTS = {
    "bundle_meta_sha256": "4bbd2746e360f99c7d58afbfd6b3cfd79b1fbc1f8d1f00fac0768616a50318e3",
    "feature_manifest_sha256": "5409a398d845eaf9a990dbf4f390cfa3aeff2b7dd014fd02d70b303a2f8a557f",
    "training_spec_sha256": "45ece69f8ea22ff35f5d1726dfa3a8ff5a8aab8297b66490af9d8287d8ee9328",
}

BASE_FEATURES = (
    "close",
    "volume",
    "buy_volume",
    "sell_volume",
    "trade_count",
    "buy_count",
    "sell_count",
)

TICK_FEATURES = (
    "tick_streak",
    "tick_mom_3s",
    "tick_mom_5s",
    "tick_mom_10s",
    "tick_ewm_3s",
    "tick_ewm_10s",
    "micro_ret_std",
    "micro_ret_skew",
    "micro_ret_kurt",
    "tick_reversal_freq",
    "flow_velocity",
    "flow_acceleration",
    "tick_streak_max",
    "tick_mom_range",
)

TAKER_TEMPO_FEATURES = tuple(
    f"taker_{stem}_{window}s"
    for stem in (
        "quote_imbalance",
        "signed_quote_sum",
        "trade_count_sum",
        "max_same_side_run",
        "buy_sweep_score",
        "sell_sweep_score",
        "buy_iceberg_pressure_sum",
        "sell_iceberg_pressure_sum",
    )
    for window in (5, 10, 30, 60)
)

EXECUTION_L2_FEATURES = (
    "l2_spread_bps",
    "l2_microprice_offset_bps",
    "l2_imbalance_l1",
    "l2_imbalance_l3",
    "l2_imbalance_l5",
    "l2_imbalance_l10",
    "l2_near_depth_total",
    "l2_depth_slope",
    "l2_depth_convexity",
    "l2_queue_concentration",
    "l2_quote_flip_rate",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
)

METRIC_FEATURES = (
    "oi_log",
    "oi_pct_change",
    "oi_zscore_1h",
    "oi_zscore_6h",
    "oi_momentum",
    "toptrader_ls_ratio",
    "crowd_ls_ratio",
    "taker_ls_ratio",
    "toptrader_ls_zscore",
    "crowd_ls_zscore",
    "taker_ls_zscore",
    "taker_ls_momentum",
    "oi_price_divergence",
)

LOCAL_MICROSTRUCTURE_FEATURES = (
    "volatility_30s",
    "volatility_60s",
    "volatility_300s",
    "volume_imbalance",
    "volume_imbalance_30s",
    "volume_imbalance_60s",
    "volume_imbalance_300s",
    "trade_intensity_30s",
    "trade_intensity_60s",
    "trade_intensity_300s",
    "vpin_30s",
    "vpin_60s",
    "vpin_300s",
    "price_velocity",
    "price_acceleration",
    "price_change_30s",
    "price_change_60s",
    "price_change_300s",
    "volatility_5s",
    "volume_imbalance_5s",
    "trade_intensity_5s",
    "vpin_5s",
    "price_change_5s",
    "avg_trade_size",
    "avg_trade_size_60s",
    "large_trade_ratio",
    "volume_zscore",
    "bar_spread",
    "bar_spread_bps",
    "return_1",
    "return_abs",
    "vol_regime_6h",
    "vol_regime_24h",
    "vol_regime_zscore",
)

CROSS_MARKET_FEATURES = (
    "cv_ref_perp_basis_bps",
    "cv_ref_perp_ret_10s",
    "cv_ref_perp_ret_30s",
    "cv_ref_perp_ret_60s",
    "cv_ref_perp_volatility_60s",
    "cv_ref_perp_volume_imbalance",
    "cv_ref_perp_trade_intensity_60s",
    "cv_ref_perp_vpin_60s",
    "cv_ref_perp_basis_residual_bps",
    "cv_ref_perp_age_s",
    "cv_ref_perp_available",
)

CALENDAR_FEATURES = (
    "cal_utc_hour",
    "cal_utc_weekday",
    "cal_utc_is_weekend",
    "cal_hour_sin",
    "cal_hour_cos",
    "cal_dow_sin",
    "cal_dow_cos",
    "cal_session_asia",
    "cal_session_tokyo",
    "cal_session_singapore_hk",
    "cal_session_europe",
    "cal_session_london",
    "cal_session_america",
    "cal_session_us_extended",
    "cal_session_asia_europe_overlap",
    "cal_session_europe_america_overlap",
    "cal_session_tokyo_singapore_overlap",
    "cal_session_london_us_overlap",
    "cal_session_active_count",
    "cal_cn_hour",
    "cal_cn_weekday",
    "cal_cn_is_weekend",
    "cal_cn_is_holiday",
    "cal_cn_is_adjusted_workday",
    "cal_cn_is_workday",
    "cal_cn_is_holiday_eve",
    "cal_cn_is_post_holiday",
    "cal_us_hour",
    "cal_us_weekday",
    "cal_us_is_weekend",
    "cal_us_is_sunday",
    "cal_us_is_sunday_evening",
    "cal_us_is_federal_holiday",
    "cal_us_is_nyse_trading_day",
    "cal_us_is_regular_hours",
    "cal_us_is_premarket",
    "cal_us_is_afterhours",
    "cal_us_is_holiday_eve",
    "cal_us_is_post_holiday",
    "cal_minutes_to_us_open",
    "cal_minutes_to_us_close",
    "cal_is_weekday_us_rth",
    "cal_is_weekend_core",
)

TIME_FEATURES = (
    "minutes_to_funding",
    "funding_phase",
    "funding_sin",
    "funding_cos",
    "dist_to_hour",
    "near_candle_close",
)

TRAINABLE_FEATURE_ORDER = (
    *BASE_FEATURES,
    *TICK_FEATURES,
    *TAKER_TEMPO_FEATURES,
    *EXECUTION_L2_FEATURES,
    *METRIC_FEATURES,
    *LOCAL_MICROSTRUCTURE_FEATURES,
    *CROSS_MARKET_FEATURES,
    *CALENDAR_FEATURES,
    *TIME_FEATURES,
)

HEAD_LABEL_LINKAGE = (
    ("dir_10s", "label_dir_10s", "binary"),
    ("ret_10s", "label_ret_10s", "regression"),
    ("vol_10s", "label_vol_10s", "regression"),
    ("dir_30s", "label_dir_30s", "binary"),
    ("ret_30s", "label_ret_30s", "regression"),
    ("vol_30s", "label_vol_30s", "regression"),
    ("dir_60s", "label_dir_60s", "binary"),
    ("ret_60s", "label_ret_60s", "regression"),
    ("vol_60s", "label_vol_60s", "regression"),
    ("tox_bid_5s", "label_tox_bid_5s", "binary"),
    ("tox_ask_5s", "label_tox_ask_5s", "binary"),
    ("tox_bid_10s", "label_tox_bid_10s", "binary"),
    ("tox_ask_10s", "label_tox_ask_10s", "binary"),
)


@dataclass(frozen=True, slots=True)
class SourceContract:
    name: str
    feature_names: tuple[str, ...]
    source_clock: str
    feature_ready_rule: str
    cadence: str
    freshness_rule: str
    missing_policy: str
    forward_fill_10s_rows: bool = False


SOURCE_CONTRACTS = (
    SourceContract(
        "local_completed_1s_bars",
        (*BASE_FEATURES, *TICK_FEATURES, *TAKER_TEMPO_FEATURES, *LOCAL_MICROSTRUCTURE_FEATURES),
        "exchange_bucket_start_ms",
        "bar.finalized_ts_ms <= cutoff_exclusive_ms",
        "1s",
        "strict_contiguous_completed_1s_through_cutoff_minus_1s",
        "fail_closed_on_gap_duplicate_or_late_bar",
    ),
    SourceContract(
        "execution_l2_completed_1s",
        EXECUTION_L2_FEATURES,
        "exchange_l2_bucket_start_ms",
        "snapshot.feature_ready_ts_ms <= cutoff_exclusive_ms",
        "1s",
        "exact_cutoff_minus_1s_bucket",
        "emit_null_unsupported_no_carry",
    ),
    SourceContract(
        "binance_metrics_5m",
        METRIC_FEATURES,
        "metric_event_ts_ms",
        "metric.feature_ready_ts_ms <= cutoff_exclusive_ms",
        "5m",
        "past_only_asof_with_max_age_300s",
        "emit_null_unsupported_no_default_value",
    ),
    SourceContract(
        "reference_perp_completed_1s",
        CROSS_MARKET_FEATURES,
        "reference_exchange_bucket_start_ms",
        "bar.finalized_ts_ms <= cutoff_exclusive_ms",
        "1s",
        "past_only_contiguous_tail_with_max_age_30s",
        "emit_null_unsupported_no_cross_source_forward_fill",
    ),
    SourceContract(
        "canonical_calendar_clock",
        (*CALENDAR_FEATURES, *TIME_FEATURES),
        "cutoff_exclusive_ms",
        "deterministic_at_cutoff_exclusive_ms",
        "1s",
        "supported_calendar_year_contract",
        "fail_closed_outside_supported_years",
    ),
)


def canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
        "ascii"
    )
    return hashlib.sha256(encoded).hexdigest()


def feature_order_sha256() -> str:
    return canonical_sha256(list(TRAINABLE_FEATURE_ORDER))


def head_linkage_payload() -> dict[str, Any]:
    return {
        "head_count": len(HEAD_LABEL_LINKAGE),
        "heads": [
            {"head": head, "label": label, "objective_family": objective}
            for head, label, objective in HEAD_LABEL_LINKAGE
        ],
        "label_contract_identity": LABEL_CONTRACT_IDENTITY,
        "label_estimand_changed": False,
        "label_semantics_version": LABEL_SEMANTICS_VERSION,
    }


def source_manifest_payload() -> dict[str, Any]:
    return {
        "cadence_ms": CADENCE_MS,
        "feature_dag_id": FEATURE_DAG_ID,
        "feature_semantics_identity": FEATURE_SEMANTICS_IDENTITY,
        "identity": IDENTITY,
        "sources": [asdict(contract) for contract in SOURCE_CONTRACTS],
        "ten_second_feature_rows_accepted_as_input": False,
    }


def trainable_schema_payload() -> dict[str, Any]:
    return {
        "cadence_ms": CADENCE_MS,
        "feature_count": len(TRAINABLE_FEATURE_ORDER),
        "feature_dag_id": FEATURE_DAG_ID,
        "feature_order": list(TRAINABLE_FEATURE_ORDER),
        "feature_order_sha256": feature_order_sha256(),
        "feature_semantics_identity": FEATURE_SEMANTICS_IDENTITY,
        "feature_semantics_version": FEATURE_SEMANTICS_VERSION,
        "head_linkage": head_linkage_payload(),
        "identity": IDENTITY,
        "label_values_read": False,
        "prediction_values_read": False,
        "reference_v12_artifacts": dict(REFERENCE_V12_ARTIFACTS),
        "schema_version": SCHEMA_VERSION,
        "source_manifest_sha256": canonical_sha256(source_manifest_payload()),
    }


def validate_trainable_schema() -> None:
    if len(TRAINABLE_FEATURE_ORDER) != 173:
        raise ValueError("causal-v12 trainable feature schema must contain 173 columns")
    if len(set(TRAINABLE_FEATURE_ORDER)) != len(TRAINABLE_FEATURE_ORDER):
        raise ValueError("causal-v12 trainable feature order contains duplicates")
    if len(HEAD_LABEL_LINKAGE) != 13:
        raise ValueError("causal-v12 head linkage must contain 13 heads")
    if len({head for head, _, _ in HEAD_LABEL_LINKAGE}) != 13:
        raise ValueError("causal-v12 head linkage contains duplicate heads")
    assigned = [name for contract in SOURCE_CONTRACTS for name in contract.feature_names]
    if len(assigned) != len(set(assigned)) or set(assigned) != set(TRAINABLE_FEATURE_ORDER):
        raise ValueError("every trainable feature must belong to exactly one source contract")
    if any(contract.forward_fill_10s_rows for contract in SOURCE_CONTRACTS):
        raise ValueError("1s successor cannot accept forward-filled 10s feature rows")


validate_trainable_schema()
