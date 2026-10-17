"""Three-phase placement race with request-time shared causal state."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from lightgbm import LGBMRegressor

from research.families.f06_placement_fill_cif.audit.full_curve_fill_cif import STATIC_MODEL_FEATURES
from research.families.f06_placement_fill_cif.audit.risk_set_expansion import (
    EVENT_ACK,
    EVENT_CENSOR,
    EVENT_FILL,
    expand_competing_risk_intervals_native,
)

SCHEMA_VERSION = "placement_fill_request_state_race.v2"

REQUEST_SHARED_NUMERIC_FEATURES = (
    "distance_ticks",
    "distance_delta_ticks",
    "request_order_distance_to_same_side_bbo_ticks",
    "request_book_age_ms",
    "request_bbo_spread_ticks",
    "request_book_imbalance",
    "request_microprice_shift_bps",
    "request_l2_near_depth_total",
    "request_l2_quote_flip_rate",
    "request_l2_book_refresh_ratio",
    "request_l2_book_cancel_ratio",
    "request_active_order_count",
    "request_pending_cancel_before_count",
    "request_request_batch_size",
    "request_order_age_ms",
    "request_remaining_qty",
    "request_queue_left",
    "request_queue_path_valid",
    "request_native_cancel_count",
    "request_native_cancel_qty",
    "request_native_refill_count",
    "request_native_refill_qty",
    "request_native_level_event_count",
    "request_market_return_bps_25ms",
    "request_market_return_bps_50ms",
    "request_market_return_bps_100ms",
    "request_market_return_bps_250ms",
    "request_market_return_bps_500ms",
    "request_market_return_bps_1000ms",
    "request_aggressive_buy_qty_25ms",
    "request_aggressive_buy_qty_50ms",
    "request_aggressive_buy_qty_100ms",
    "request_aggressive_buy_qty_250ms",
    "request_aggressive_buy_qty_500ms",
    "request_aggressive_buy_qty_1000ms",
    "request_aggressive_sell_qty_25ms",
    "request_aggressive_sell_qty_50ms",
    "request_aggressive_sell_qty_100ms",
    "request_aggressive_sell_qty_250ms",
    "request_aggressive_sell_qty_500ms",
    "request_aggressive_sell_qty_1000ms",
    "request_taker_imbalance_25ms",
    "request_taker_imbalance_50ms",
    "request_taker_imbalance_100ms",
    "request_taker_imbalance_250ms",
    "request_taker_imbalance_500ms",
    "request_taker_imbalance_1000ms",
    "request_trade_count_25ms",
    "request_trade_count_50ms",
    "request_trade_count_100ms",
    "request_trade_count_250ms",
    "request_trade_count_500ms",
    "request_trade_count_1000ms",
    "request_book_update_count_25ms",
    "request_book_update_count_50ms",
    "request_book_update_count_100ms",
    "request_book_update_count_250ms",
    "request_book_update_count_500ms",
    "request_book_update_count_1000ms",
)
REQUEST_CATEGORICAL_FEATURES = (
    "inventory_role",
    "action",
    "cancel_request_reason",
)


def empirical_bin_edges(
    duration_ms: np.ndarray,
    *,
    maximum_bins: int = 32,
) -> np.ndarray:
    """Derive a compact time grid only from the current outer-train panel."""

    values = np.asarray(duration_ms, dtype=float)
    values = values[np.isfinite(values) & (values > 0.0)]
    if values.size == 0:
        raise ValueError("cannot derive a risk grid without positive durations")
    if int(maximum_bins) < 2:
        raise ValueError("maximum_bins must be at least two")
    probabilities = np.linspace(
        0.0,
        1.0,
        min(int(maximum_bins), values.size),
    )
    edges = np.unique(np.r_[0.0, np.quantile(values, probabilities)])
    if len(edges) < 2:
        edges = np.asarray([0.0, float(values.max())], dtype=float)
    return edges.astype(np.float64)


def pending_event_kind(frame: pd.DataFrame) -> np.ndarray:
    fill_ts = pd.to_numeric(
        frame["first_pending_cancel_fill_ts_ns"], errors="coerce"
    ).fillna(0).to_numpy(np.int64)
    ack_ts = pd.to_numeric(
        frame["actual_cancel_ack_ts_ns"], errors="coerce"
    ).fillna(0).to_numpy(np.int64)
    fill = frame["pending_cancel_fill"].to_numpy(np.int8) != 0
    ack = frame["cancel_ack_observed"].to_numpy(np.int8) != 0
    output = np.full(len(frame), EVENT_CENSOR, dtype=np.uint8)
    output[ack] = EVENT_ACK
    output[fill & ((~ack) | (fill_ts < ack_ts))] = EVENT_FILL
    return output


def _encoded_base(
    frame: pd.DataFrame,
    *,
    numeric_features: tuple[str, ...],
    categorical_features: tuple[str, ...],
    encoded_columns: tuple[str, ...] | None = None,
) -> tuple[pd.DataFrame, tuple[str, ...]]:
    missing = sorted(
        set(numeric_features + categorical_features) - set(frame.columns)
    )
    if missing:
        raise ValueError(f"race model is missing causal features: {missing}")
    numeric = frame.loc[:, numeric_features].apply(pd.to_numeric, errors="coerce")
    numeric = numeric.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    categorical = pd.get_dummies(
        frame.loc[:, categorical_features].fillna("missing").astype(str),
        prefix=list(categorical_features),
        dtype=float,
    )
    output = pd.concat([numeric, categorical], axis=1)
    if encoded_columns is None:
        columns = tuple(sorted(output.columns))
    else:
        columns = encoded_columns
    return output.reindex(columns=columns, fill_value=0.0), columns


@dataclass
class CauseSpecificRateModel:
    bin_edges_ms: np.ndarray
    numeric_features: tuple[str, ...]
    categorical_features: tuple[str, ...]
    encoded_columns: tuple[str, ...]
    fill_model: LGBMRegressor
    ack_model: LGBMRegressor | None

    @classmethod
    def fit(
        cls,
        frame: pd.DataFrame,
        *,
        duration_column: str,
        event_kind: np.ndarray,
        numeric_features: tuple[str, ...],
        categorical_features: tuple[str, ...],
        maximum_bins: int = 32,
        random_seed: int = 20260728,
        include_ack: bool,
    ) -> CauseSpecificRateModel:
        duration = pd.to_numeric(
            frame[duration_column], errors="coerce"
        ).to_numpy(float)
        edges = empirical_bin_edges(duration, maximum_bins=maximum_bins)
        expanded = expand_competing_risk_intervals_native(
            duration, event_kind, edges
        )
        base, columns = _encoded_base(
            frame,
            numeric_features=numeric_features,
            categorical_features=categorical_features,
        )
        row_index = expanded["row_index"].to_numpy(np.int64)
        features = base.iloc[row_index].reset_index(drop=True)
        interval_width = (
            expanded["interval_end_ms"].to_numpy(float)
            - expanded["interval_start_ms"].to_numpy(float)
        )
        features["risk_elapsed_log1p"] = np.log1p(
            expanded["interval_start_ms"].to_numpy(float)
        )
        features["risk_interval_width_log1p"] = np.log1p(interval_width)
        encoded = tuple(features.columns)
        exposure_seconds = np.maximum(
            1e-6,
            interval_width
            * expanded["exposure_fraction"].to_numpy(float)
            / 1_000.0,
        )

        def fit_cause(target: str) -> LGBMRegressor:
            event = expanded[target].to_numpy(float)
            if int(event.sum()) < 2:
                raise ValueError(f"insufficient {target} events for a rate model")
            model = LGBMRegressor(
                objective="poisson",
                n_estimators=120,
                learning_rate=0.04,
                num_leaves=7,
                max_depth=3,
                min_child_samples=100,
                reg_lambda=2.0,
                random_state=int(random_seed),
                deterministic=True,
                force_col_wise=True,
                verbosity=-1,
                n_jobs=1,
            )
            model.fit(
                features,
                event / exposure_seconds,
                sample_weight=exposure_seconds,
            )
            return model

        fill_model = fit_cause("fill_target")
        ack_model = fit_cause("ack_target") if include_ack else None
        return cls(
            bin_edges_ms=edges,
            numeric_features=numeric_features,
            categorical_features=categorical_features,
            encoded_columns=encoded,
            fill_model=fill_model,
            ack_model=ack_model,
        )

    def predict_cif(self, frame: pd.DataFrame) -> pd.DataFrame:
        duration = np.full(len(frame), self.bin_edges_ms[-1], dtype=float)
        expanded = expand_competing_risk_intervals_native(
            duration,
            np.zeros(len(frame), dtype=np.uint8),
            self.bin_edges_ms,
        )
        base_columns = tuple(
            name
            for name in self.encoded_columns
            if name not in {"risk_elapsed_log1p", "risk_interval_width_log1p"}
        )
        base, _ = _encoded_base(
            frame,
            numeric_features=self.numeric_features,
            categorical_features=self.categorical_features,
            encoded_columns=base_columns,
        )
        row_index = expanded["row_index"].to_numpy(np.int64)
        features = base.iloc[row_index].reset_index(drop=True)
        start = expanded["interval_start_ms"].to_numpy(float)
        end = expanded["interval_end_ms"].to_numpy(float)
        features["risk_elapsed_log1p"] = np.log1p(start)
        features["risk_interval_width_log1p"] = np.log1p(end - start)
        features = features.reindex(columns=self.encoded_columns, fill_value=0.0)
        fill_rate = np.maximum(0.0, self.fill_model.predict(features))
        ack_rate = (
            np.maximum(0.0, self.ack_model.predict(features))
            if self.ack_model is not None
            else np.zeros(len(features))
        )
        dt_seconds = (end - start) / 1_000.0
        total_rate = fill_rate + ack_rate
        event_probability = 1.0 - np.exp(-total_rate * dt_seconds)
        fill_hazard = np.divide(
            event_probability * fill_rate,
            total_rate,
            out=np.zeros_like(total_rate),
            where=total_rate > 0.0,
        )
        ack_hazard = np.divide(
            event_probability * ack_rate,
            total_rate,
            out=np.zeros_like(total_rate),
            where=total_rate > 0.0,
        )
        output = expanded.loc[
            :, ["row_index", "bin_index", "interval_start_ms", "interval_end_ms"]
        ].copy()
        output["fill_hazard"] = fill_hazard
        output["ack_hazard"] = ack_hazard
        output["fill_cif"] = 0.0
        output["ack_cif"] = 0.0
        output["survival"] = 0.0
        for _, indices in output.groupby("row_index", sort=False).groups.items():
            survival = 1.0
            fill_cif = 0.0
            ack_cif = 0.0
            for index in indices:
                fill_cif += survival * float(output.at[index, "fill_hazard"])
                ack_cif += survival * float(output.at[index, "ack_hazard"])
                survival *= max(
                    0.0,
                    1.0
                    - float(output.at[index, "fill_hazard"])
                    - float(output.at[index, "ack_hazard"]),
                )
                output.at[index, "fill_cif"] = fill_cif
                output.at[index, "ack_cif"] = ack_cif
                output.at[index, "survival"] = survival
        return output


def fit_three_phase_by_side(
    frame: pd.DataFrame,
    *,
    maximum_bins: int = 32,
    random_seed: int = 20260728,
) -> dict[str, dict[str, CauseSpecificRateModel]]:
    """Fit pre-request and request-time races independently for BUY/SELL."""

    output: dict[str, dict[str, CauseSpecificRateModel]] = {}
    pre_numeric = tuple(
        name for name in STATIC_MODEL_FEATURES if name in frame.columns
    )
    if not pre_numeric:
        raise ValueError("request-state panel has no placement-time features")
    for side in ("BUY", "SELL"):
        side_frame = frame.loc[frame["side"].astype(str).eq(side)].copy()
        pre = side_frame.loc[side_frame["pre_request_observed"].eq(1)].copy()
        pre_kind = np.where(
            pre["pre_request_first_fill"].to_numpy(np.int8) != 0,
            EVENT_FILL,
            EVENT_CENSOR,
        ).astype(np.uint8)
        request = side_frame.loc[side_frame["request_model_risk_set"].eq(1)].copy()
        if request.empty:
            raise ValueError(f"{side} has no valid request-time risk rows")
        output[side] = {
            "pre_request_fill": CauseSpecificRateModel.fit(
                pre,
                duration_column="pre_request_exposure_ms",
                event_kind=pre_kind,
                numeric_features=pre_numeric,
                categorical_features=("inventory_role", "action"),
                maximum_bins=maximum_bins,
                random_seed=random_seed,
                include_ack=False,
            ),
            "pending_fill_ack": CauseSpecificRateModel.fit(
                request,
                duration_column="pending_risk_duration_ms",
                event_kind=pending_event_kind(request),
                numeric_features=REQUEST_SHARED_NUMERIC_FEATURES,
                categorical_features=REQUEST_CATEGORICAL_FEATURES,
                maximum_bins=maximum_bins,
                random_seed=random_seed + 1,
                include_ack=True,
            ),
        }
    return output
