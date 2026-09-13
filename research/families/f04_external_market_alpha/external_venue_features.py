"""Causal external-venue features for offline model research.

The normalized Bitget, Bybit, and OKX trade archives are one-second states.
Each source row covers ``[t, t + 1s)`` and is timestamped at ``t + 1s``.  The
helpers in this module preserve that right-edge visibility when they align the
state to either the existing ten-second model grid or a compact one-second
research head.

This module does not change the live policy.  Model bundles built from these
features must remain research-only until the same feature schema is available
from receive-time live feeds.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from pathlib import Path

import numpy as np
import pandas as pd

EXTERNAL_VENUES = ("bitget", "bybit", "okx")
EXTERNAL_FACTORS = ("perp", "spot")
EXTERNAL_HORIZONS_S = (1, 3, 5, 10)
FAST_LABEL_CLOSE_COLUMN = "_fast_local_label_close"
FEATURE_SCHEMA_VERSION = "external_venue_trade_state.v4"
BRIDGE_BAR_SCHEMA_VERSION = "binance_individual_trade_bar_1s.v1"
BINANCE_CROSS_SUFFIXES = (
    "basis_bps",
    "ret_10s",
    "ret_30s",
    "ret_60s",
    "volatility_60s",
    "volume_imbalance",
    "trade_intensity_60s",
    "vpin_60s",
    "basis_residual_bps",
    "age_s",
    "available",
)


def _day_start(day: str) -> pd.Timestamp:
    value = pd.Timestamp(day, tz="UTC")
    if value.strftime("%Y-%m-%d") != str(day):
        raise ValueError(f"day must be YYYY-MM-DD: {day}")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_individual_trade_bar(path: Path, day: str) -> None:
    metadata_path = path.with_suffix(path.suffix + ".meta.json")
    if not metadata_path.is_file():
        raise FileNotFoundError(f"{path}: missing trade-bar metadata")
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    if str(metadata.get("schema_version", "")) != BRIDGE_BAR_SCHEMA_VERSION:
        raise ValueError(f"{path}: unsupported trade-bar schema")
    if not metadata.get("complete"):
        raise ValueError(f"{path}: incomplete trade-bar metadata")
    if str(metadata.get("utc_day", "")) != day:
        raise ValueError(f"{path}: trade-bar UTC day mismatch")
    if str(metadata.get("source_data_type", "")) != "trades":
        raise ValueError(f"{path}: expected Binance individual trades")
    expected_sha256 = str(metadata.get("output_sha256", ""))
    if not expected_sha256 or _sha256(path) != expected_sha256:
        raise ValueError(f"{path}: trade-bar SHA256 mismatch")


def decision_grid_1s(day: str) -> pd.DatetimeIndex:
    """Return the 86,400 right-edge decision timestamps for one UTC day."""
    start = _day_start(day)
    return pd.date_range(
        start + pd.Timedelta(seconds=1),
        start + pd.Timedelta(days=1),
        freq="1s",
        tz="UTC",
    )


def _timestamp_index(frame: pd.DataFrame, *, label: str) -> pd.DataFrame:
    if "timestamp" not in frame:
        raise ValueError(f"{label}: timestamp is required")
    timestamp = pd.to_numeric(frame["timestamp"], errors="coerce")
    valid = timestamp.notna()
    output = frame.loc[valid].copy()
    output.index = pd.to_datetime(timestamp.loc[valid].round().astype("int64"), unit="ms", utc=True)
    output = output.sort_index()
    return output[~output.index.duplicated(keep="last")]


def _daily_consensus_path(data_dir: Path, factor: str, day: str) -> Path:
    directory = (
        Path(data_dir)
        / "external_venues"
        / "consensus"
        / f"{factor}_3venue"
        / "BTCUSDT"
        / "features_1s"
    )
    matches = sorted(directory.glob(f"*consensus-1s-{day}.parquet"))
    if len(matches) != 1:
        raise FileNotFoundError(
            f"expected one {factor} consensus file for {day} under {directory}; "
            f"found {len(matches)}"
        )
    return matches[0]


def _daily_cross_path(data_dir: Path, day: str) -> Path | None:
    path = (
        Path(data_dir)
        / "external_venues"
        / "consensus"
        / "spot_perp_3venue"
        / "BTCUSDT"
        / "features_1s"
        / f"BTCUSDT-spot-perp-state-1s-{day}.parquet"
    )
    return path if path.exists() else None


def _valid_run(values: pd.Series, periods: int) -> pd.Series:
    required = max(2, int(periods) + 1)
    return values.notna().rolling(required, min_periods=required).sum().eq(required)


def _numeric_column(
    frame: pd.DataFrame,
    name: str,
    *,
    default: float = np.nan,
) -> pd.Series:
    if name not in frame:
        return pd.Series(default, index=frame.index, dtype=float)
    return pd.to_numeric(frame[name], errors="coerce")


def _log_return(values: pd.Series, periods: int) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce").where(lambda item: item > 0.0)
    result = np.log(numeric / numeric.shift(periods))
    return result.where(_valid_run(numeric, periods))


def _direction_and_move_labels(label: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Split signed direction from the separate move/no-move target."""
    direction = pd.Series(np.nan, index=label.index, dtype=float)
    direction.loc[label.gt(0.0)] = 1.0
    direction.loc[label.lt(0.0)] = 0.0
    movement = label.ne(0.0).where(label.notna()).astype(float)
    return direction, movement


def normalize_fast_target_horizons(horizons_s: Iterable[int]) -> tuple[int, ...]:
    """Return a strict, ordered integer-second target grid.

    Historical external archives expose one-second causal states, so this
    research path cannot identify subsecond target horizons. The exact grid is
    an experiment input rather than a feature-schema constant.
    """

    horizons = tuple(sorted({int(value) for value in horizons_s}))
    if not horizons or horizons[0] <= 0:
        raise ValueError("fast target horizons must be positive integer seconds")
    return horizons


def _rolling_mean(values: pd.Series, periods: int, availability: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    valid = availability.fillna(False).astype(bool)
    numeric = numeric.where(valid)
    minimum = max(1, int(np.ceil(periods * 0.6)))
    return numeric.rolling(periods, min_periods=minimum).mean()


def _factor_features(
    frame: pd.DataFrame,
    *,
    factor: str,
    grid: pd.DatetimeIndex,
    horizons_s: Iterable[int],
) -> pd.DataFrame:
    state = _timestamp_index(frame, label=factor).reindex(grid)
    output = pd.DataFrame(index=grid)
    close_col = "common_factor_close" if "common_factor_close" in state else "close"
    close = pd.to_numeric(state[close_col], errors="coerce")
    available_venues = _numeric_column(state, "available_venues", default=0.0).fillna(0.0)
    available = available_venues.ge(2.0) & close.gt(0.0)
    flow = _numeric_column(state, "flow_imbalance", default=0.0)
    agreement = _numeric_column(state, "agreement_score", default=0.0)
    dispersion_name = (
        "return_dispersion_bps" if "return_dispersion_bps" in state else "dispersion_bps"
    )
    dispersion = _numeric_column(state, dispersion_name, default=0.0)

    output[f"cv_external_{factor}_available"] = available.astype(float)
    output[f"cv_external_{factor}_available_venues"] = available_venues
    age_name = "max_source_age_ms" if "max_source_age_ms" in state else "source_age_ms"
    output[f"cv_external_{factor}_source_age_ms"] = _numeric_column(state, age_name)
    output[f"cv_external_{factor}_confidence"] = _numeric_column(
        state, "consensus_confidence", default=0.0
    )

    factor_returns: dict[int, pd.Series] = {}
    for horizon in horizons_s:
        horizon = int(horizon)
        ret = _log_return(close.where(available), horizon)
        factor_returns[horizon] = ret
        output[f"cv_external_{factor}_ret_{horizon}s"] = ret
        output[f"cv_external_{factor}_flow_{horizon}s"] = _rolling_mean(flow, horizon, available)
        output[f"cv_external_{factor}_agreement_{horizon}s"] = _rolling_mean(
            agreement, horizon, available
        )
        output[f"cv_external_{factor}_dispersion_{horizon}s_bps"] = _rolling_mean(
            dispersion, horizon, available
        )

    for venue in EXTERNAL_VENUES:
        venue_close = _numeric_column(state, f"{venue}_close")
        venue_available = (
            _numeric_column(state, f"{venue}_available", default=0.0).fillna(0.0).gt(0.5)
        )
        venue_flow = _numeric_column(state, f"{venue}_flow_imbalance", default=0.0)
        output[f"cv_external_{venue}_{factor}_available"] = venue_available.astype(float)
        output[f"cv_external_{venue}_{factor}_source_age_ms"] = _numeric_column(
            state, f"{venue}_source_age_ms"
        )
        for horizon in horizons_s:
            horizon = int(horizon)
            output[f"cv_external_{venue}_{factor}_ret_{horizon}s"] = _log_return(
                venue_close.where(venue_available), horizon
            )
            output[f"cv_external_{venue}_{factor}_flow_{horizon}s"] = _rolling_mean(
                venue_flow, horizon, venue_available
            )

    return output


def build_external_feature_grid_1s(
    data_dir: Path,
    day: str,
    *,
    horizons_s: Iterable[int] = EXTERNAL_HORIZONS_S,
) -> pd.DataFrame:
    """Build model-ready causal features from all six external trade states."""
    grid = decision_grid_1s(day)
    horizons = tuple(sorted({int(value) for value in horizons_s if int(value) > 0}))
    if not horizons:
        raise ValueError("at least one positive horizon is required")

    factors: dict[str, pd.DataFrame] = {}
    pieces = []
    for factor in EXTERNAL_FACTORS:
        raw = pd.read_parquet(_daily_consensus_path(data_dir, factor, day))
        part = _factor_features(raw, factor=factor, grid=grid, horizons_s=horizons)
        factors[factor] = part
        pieces.append(part)

    cross_path = _daily_cross_path(data_dir, day)
    if cross_path is None:
        cross = pd.DataFrame(index=grid)
    else:
        cross_raw = pd.read_parquet(cross_path)
        cross = _timestamp_index(cross_raw, label="spot_perp").reindex(grid)
    cross_features = pd.DataFrame(index=grid)
    direct_columns = {
        "consensus_confidence": "cv_external_consensus_confidence",
        "perp_spot_basis_bps": "cv_external_perp_spot_basis_bps",
        "perp_minus_spot_bps": "cv_external_perp_minus_spot_bps",
        "spot_perp_agreement": "cv_external_spot_perp_agreement_1s",
        "venue_divergence_bps": "cv_external_venue_divergence_bps",
        "cross_instrument_available": "cv_external_cross_instrument_available",
        "fresh_perp_venues": "cv_external_fresh_perp_venues",
        "fresh_spot_venues": "cv_external_fresh_spot_venues",
    }
    for source, target in direct_columns.items():
        cross_features[target] = _numeric_column(cross, source, default=0.0)

    if cross_path is None:
        perp_available = factors["perp"]["cv_external_perp_available"].gt(0.5)
        spot_available = factors["spot"]["cv_external_spot_available"].gt(0.5)
        cross_features["cv_external_consensus_confidence"] = pd.concat(
            [
                factors["perp"]["cv_external_perp_confidence"],
                factors["spot"]["cv_external_spot_confidence"],
            ],
            axis=1,
        ).min(axis=1)
        cross_features["cv_external_cross_instrument_available"] = (
            perp_available & spot_available
        ).astype(float)
        cross_features["cv_external_fresh_perp_venues"] = factors["perp"][
            "cv_external_perp_available_venues"
        ]
        cross_features["cv_external_fresh_spot_venues"] = factors["spot"][
            "cv_external_spot_available_venues"
        ]
        cross_features["cv_external_cross_state_sidecar_available"] = 0.0
    else:
        cross_features["cv_external_cross_state_sidecar_available"] = 1.0

    for horizon in horizons:
        perp_ret = factors["perp"][f"cv_external_perp_ret_{horizon}s"]
        spot_ret = factors["spot"][f"cv_external_spot_ret_{horizon}s"]
        cross_features[f"cv_external_perp_minus_spot_ret_{horizon}s"] = perp_ret - spot_ret
        cross_features[f"cv_external_spot_perp_agreement_{horizon}s"] = (
            np.sign(perp_ret).eq(np.sign(spot_ret)) & perp_ret.notna() & spot_ret.notna()
        ).astype(float)

    output = pd.concat([*pieces, cross_features], axis=1)
    age_columns = [name for name in output if name.endswith("_source_age_ms")]
    available_columns = [name for name in output if name.endswith("_available")]
    value_columns = [name for name in output if name not in age_columns]
    output[value_columns] = output[value_columns].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    output[age_columns] = output[age_columns].replace([np.inf, -np.inf], np.nan).fillna(10_000.0)
    output[available_columns] = output[available_columns].clip(0.0, 1.0)
    return output.astype(np.float32)


def align_external_features_to_10s(
    external_1s: pd.DataFrame,
    target_index: pd.Index,
    *,
    decision_delay_s: int = 10,
) -> pd.DataFrame:
    """Align right-edge 1s state to left-labelled existing model rows."""
    target = pd.DatetimeIndex(pd.to_datetime(target_index, utc=True))
    decision = target + pd.Timedelta(seconds=int(decision_delay_s))
    aligned = external_1s.reindex(decision)
    aligned.index = target
    aligned.index.name = getattr(target_index, "name", None)
    age_columns = [name for name in aligned if name.endswith("_source_age_ms")]
    if age_columns:
        aligned.loc[:, age_columns] = aligned.loc[:, age_columns].fillna(10_000.0)
    other_columns = [name for name in aligned if name not in age_columns]
    aligned.loc[:, other_columns] = aligned.loc[:, other_columns].fillna(0.0)
    return aligned.astype(np.float32)


def enrich_10s_feature_day(
    base_features: pd.DataFrame,
    data_dir: Path,
    day: str,
) -> pd.DataFrame:
    """Return an existing 10s feature day plus causal external venue state."""
    existing = [name for name in base_features if name.startswith("cv_external_")]
    if existing:
        raise ValueError(f"base feature frame already has external columns: {existing[:3]}")
    base_features = base_features.copy()
    # A single historical day was generated with the older minimal market
    # stage.  Keep the original file immutable and make missing source state
    # explicit so schema/version absence cannot become a model feature.
    for prefix in ("cv_ref_perp", "cv_ref_spot", "cv_exec_spot"):
        for suffix in BINANCE_CROSS_SUFFIXES:
            column = f"{prefix}_{suffix}"
            if column in base_features:
                continue
            base_features[column] = 40.0 if suffix == "age_s" else 0.0
    external = build_external_feature_grid_1s(data_dir, day)
    aligned = align_external_features_to_10s(external, base_features.index)
    return base_features.join(aligned, how="left")


def _bars_on_decision_grid(
    path: Path,
    day: str,
    prefix: str,
    *,
    max_source_age_s: float = 2.0,
) -> pd.DataFrame:
    raw = pd.read_parquet(path)
    if raw.empty:
        raise ValueError(f"{path}: empty 1s bars")
    if isinstance(raw.index, pd.DatetimeIndex):
        index = pd.DatetimeIndex(pd.to_datetime(raw.index, utc=True)).floor("1s")
    else:
        numeric = pd.to_numeric(pd.Index(raw.index), errors="coerce")
        index = pd.to_datetime(numeric, unit="ms", utc=True, errors="coerce").floor("1s")
    raw = raw.copy()
    raw.index = index
    raw = raw[~raw.index.isna()].sort_index()
    raw = raw[~raw.index.duplicated(keep="last")]

    start = _day_start(day)
    left_grid = pd.date_range(
        start,
        start + pd.Timedelta(days=1) - pd.Timedelta(seconds=1),
        freq="1s",
    )
    state = raw.reindex(left_grid)
    if "last_event_ts_ms" in raw:
        raw_event_ns = pd.to_numeric(raw["last_event_ts_ms"], errors="coerce") * 1_000_000.0
    else:
        raw_event_ns = pd.Series(
            raw.index.as_unit("ns").asi8,
            index=raw.index,
            dtype="float64",
        )
    event_ns = raw_event_ns.reindex(left_grid).ffill()
    ready_grid = left_grid + pd.Timedelta(seconds=1)
    source_age_ms = (
        ready_grid.as_unit("ns").asi8 - event_ns.to_numpy(dtype="float64")
    ) / 1_000_000.0
    fresh = pd.Series(
        (source_age_ms >= 0.0) & (source_age_ms <= max(1.0, float(max_source_age_s) * 1_000.0)),
        index=left_grid,
    )
    close = pd.to_numeric(state["close"], errors="coerce").ffill().where(fresh)
    buy = _numeric_column(state, "buy_volume", default=0.0).fillna(0.0)
    sell = _numeric_column(state, "sell_volume", default=0.0).fillna(0.0)
    count = _numeric_column(state, "trade_count", default=0.0).fillna(0.0)
    volume = buy + sell
    imbalance = (buy - sell) / volume.replace(0.0, np.nan)

    output = pd.DataFrame(index=ready_grid)
    output[f"_{prefix}_close"] = close.to_numpy(dtype=float)
    valid = close.notna()
    output[f"{prefix}_available"] = valid.to_numpy(dtype=float)
    output[f"{prefix}_source_age_ms"] = np.where(
        valid.to_numpy(),
        source_age_ms,
        np.nan,
    )
    for horizon in EXTERNAL_HORIZONS_S:
        output[f"{prefix}_ret_{horizon}s"] = _log_return(close, horizon).to_numpy()
        output[f"{prefix}_flow_{horizon}s"] = (
            imbalance.fillna(0.0)
            .rolling(
                horizon,
                min_periods=max(1, int(np.ceil(horizon * 0.6))),
            )
            .mean()
        ).to_numpy()
        output[f"{prefix}_trade_count_{horizon}s"] = (
            count.rolling(horizon, min_periods=1).sum()
        ).to_numpy()
        output[f"{prefix}_log_volume_{horizon}s"] = np.log1p(
            volume.rolling(horizon, min_periods=1).sum()
        ).to_numpy()
    return output


def build_binance_bridge_feature_grid_1s(
    data_dir: Path,
    day: str,
    *,
    prefix: str = "m0_bridge",
    bridge_bar_dir: Path | None = None,
    max_source_age_s: float = 2.0,
) -> pd.DataFrame:
    """Build the causal Binance BTCUSDT-perpetual bridge state.

    Rows are right-edge visible: trades in ``[t, t + 1s)`` first appear at
    ``t + 1s``.  Callers may add a measured receive/feature latency before
    performing an as-of join to a decision timestamp.
    """

    resolved_bar_dir = (
        Path(data_dir) / "reference_bars_1s_trades_v1"
        if bridge_bar_dir is None
        else Path(bridge_bar_dir)
    )
    bridge_path = resolved_bar_dir / f"BTCUSDT-1s-{day}.parquet"
    _validate_individual_trade_bar(bridge_path, day)
    state = _bars_on_decision_grid(
        bridge_path,
        day,
        prefix,
        max_source_age_s=max_source_age_s,
    )
    return state.rename(columns={f"_{prefix}_close": f"{prefix}_close"})


def build_fast_feature_day(
    data_dir: Path,
    day: str,
    *,
    bridge_bar_dir: Path | None = None,
    max_bridge_source_age_s: float = 2.0,
    target_horizons_s: Iterable[int] = (),
) -> pd.DataFrame:
    """Build a compact causal 1s M0/M1 research panel.

    The cache always retains the causal local close used to derive labels.
    Optional target horizons are materialized for small direct callers, while
    the model trainer derives its declared horizon grid when reading each day.
    This keeps cache identity independent of arbitrary 1/3/5-second heads.
    """
    data_dir = Path(data_dir)
    local = _bars_on_decision_grid(
        data_dir / "bars_1s" / f"BTCUSDC-1s-{day}.parquet",
        day,
        "fast_local",
    )
    resolved_bridge_bar_dir = (
        data_dir / "reference_bars_1s_trades_v1" if bridge_bar_dir is None else Path(bridge_bar_dir)
    )
    bridge_path = resolved_bridge_bar_dir / f"BTCUSDT-1s-{day}.parquet"
    _validate_individual_trade_bar(bridge_path, day)
    bridge = _bars_on_decision_grid(
        bridge_path,
        day,
        "fast_binance_ref_perp",
        max_source_age_s=max_bridge_source_age_s,
    )
    external = build_external_feature_grid_1s(data_dir, day)
    output = local.join(bridge.drop(columns=["_fast_binance_ref_perp_close"]), how="left")
    output = output.join(external, how="left")

    local_close = pd.to_numeric(local["_fast_local_close"], errors="coerce")
    requested_horizons = tuple(target_horizons_s)
    target_horizons = (
        normalize_fast_target_horizons(requested_horizons) if requested_horizons else ()
    )
    label_columns: dict[str, pd.Series] = {FAST_LABEL_CLOSE_COLUMN: local_close}
    for horizon in target_horizons:
        future = local_close.shift(-horizon)
        label = np.log(future / local_close)
        label_columns[f"label_fast_ret_{horizon}s"] = label
        direction, movement = _direction_and_move_labels(label)
        label_columns[f"label_fast_dir_{horizon}s"] = direction
        label_columns[f"label_fast_move_{horizon}s"] = movement

    output = output.drop(columns=["_fast_local_close"])
    label_frame = pd.DataFrame(label_columns, index=output.index)
    label_frame["day"] = day
    output = pd.concat([output, label_frame], axis=1)
    numeric = output.select_dtypes(include=[np.number]).columns
    output[numeric] = output[numeric].replace([np.inf, -np.inf], np.nan).astype(np.float32)
    return output
