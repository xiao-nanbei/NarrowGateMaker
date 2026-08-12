from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from research.families.f04_external_market_alpha.external_venue_features import (
    FAST_LABEL_CLOSE_COLUMN,
    _direction_and_move_labels,
    align_external_features_to_10s,
    build_external_feature_grid_1s,
    decision_grid_1s,
)
from research.families.f04_external_market_alpha.external_venue_model import (
    _cache_path,
    _clean_xy,
    _read_days,
    build_horizon_decay_curve,
    chronological_split,
    development_training_split,
    fast_target_specs,
    target_spec,
)


def _write_consensus(root: Path, factor: str, day: str, closes: list[float]) -> None:
    start = int(pd.Timestamp(day, tz="UTC").timestamp() * 1000)
    timestamp = np.arange(1, len(closes) + 1, dtype=np.int64) * 1000 + start
    frame = pd.DataFrame(
        {
            "timestamp": timestamp,
            "common_factor_close": closes,
            "close": closes,
            "flow_imbalance": np.linspace(-0.5, 0.5, len(closes)),
            "agreement_score": 1.0,
            "return_dispersion_bps": 0.1,
            "available_venues": 3,
            "max_source_age_ms": 10.0,
            "consensus_confidence": 0.9,
        }
    )
    for offset, venue in enumerate(("bitget", "bybit", "okx")):
        frame[f"{venue}_close"] = np.asarray(closes) + offset * 0.01
        frame[f"{venue}_source_age_ms"] = 10.0 + offset
        frame[f"{venue}_available"] = 1
        frame[f"{venue}_flow_imbalance"] = 0.1 * (offset + 1)
    directory = (
        root / "external_venues" / "consensus" / f"{factor}_3venue" / "BTCUSDT" / "features_1s"
    )
    directory.mkdir(parents=True)
    frame.to_parquet(directory / f"BTCUSDT-bitget-bybit-okx-consensus-1s-{day}.parquet")


def _write_cross(root: Path, day: str, rows: int) -> None:
    start = int(pd.Timestamp(day, tz="UTC").timestamp() * 1000)
    timestamp = np.arange(1, rows + 1, dtype=np.int64) * 1000 + start
    frame = pd.DataFrame(
        {
            "timestamp": timestamp,
            "consensus_confidence": 0.8,
            "perp_spot_basis_bps": 1.0,
            "perp_minus_spot_bps": 0.2,
            "spot_perp_agreement": 1.0,
            "venue_divergence_bps": 0.1,
            "cross_instrument_available": 1,
            "fresh_perp_venues": 3,
            "fresh_spot_venues": 3,
        }
    )
    directory = (
        root / "external_venues" / "consensus" / "spot_perp_3venue" / "BTCUSDT" / "features_1s"
    )
    directory.mkdir(parents=True)
    frame.to_parquet(directory / f"BTCUSDT-spot-perp-state-1s-{day}.parquet")


def test_external_grid_is_right_edge_causal(tmp_path: Path) -> None:
    day = "2026-01-01"
    _write_consensus(tmp_path, "perp", day, [100.0, 101.0, 102.0, 104.0])
    _write_consensus(tmp_path, "spot", day, [200.0, 201.0, 202.0, 204.0])
    _write_cross(tmp_path, day, 4)

    frame = build_external_feature_grid_1s(tmp_path, day, horizons_s=(1, 3))
    first = pd.Timestamp("2026-01-01T00:00:01Z")
    second = pd.Timestamp("2026-01-01T00:00:02Z")
    fourth = pd.Timestamp("2026-01-01T00:00:04Z")

    assert len(frame) == 86_400
    assert frame.index[0] == first
    assert frame.loc[first, "cv_external_perp_ret_1s"] == 0.0
    assert np.isclose(
        frame.loc[second, "cv_external_perp_ret_1s"],
        np.log(101.0 / 100.0),
    )
    assert np.isclose(
        frame.loc[fourth, "cv_external_perp_ret_3s"],
        np.log(104.0 / 100.0),
    )


def test_ten_second_alignment_uses_bucket_end() -> None:
    index = pd.date_range("2026-01-01T00:00:01Z", periods=20, freq="1s")
    external = pd.DataFrame({"cv_external_value": np.arange(1, 21)}, index=index)
    target = pd.DatetimeIndex(
        [pd.Timestamp("2026-01-01T00:00:00Z"), pd.Timestamp("2026-01-01T00:00:10Z")]
    )
    aligned = align_external_features_to_10s(external, target)
    assert aligned["cv_external_value"].tolist() == [10.0, 20.0]


def test_ten_second_alignment_marks_missing_age_as_stale() -> None:
    index = pd.DatetimeIndex([pd.Timestamp("2026-01-01T00:00:10Z")])
    external = pd.DataFrame(
        {
            "cv_external_perp_source_age_ms": [12.0],
            "cv_external_perp_available": [1.0],
        },
        index=index,
    )
    target = pd.DatetimeIndex(
        [pd.Timestamp("2026-01-01T00:00:00Z"), pd.Timestamp("2026-01-01T00:00:10Z")]
    )
    aligned = align_external_features_to_10s(external, target)
    assert aligned["cv_external_perp_source_age_ms"].tolist() == [12.0, 10_000.0]
    assert aligned["cv_external_perp_available"].tolist() == [1.0, 0.0]


def test_decision_grid_has_exact_utc_day() -> None:
    grid = decision_grid_1s("2026-07-03")
    assert len(grid) == 86_400
    assert grid[0] == pd.Timestamp("2026-07-03T00:00:01Z")
    assert grid[-1] == pd.Timestamp("2026-07-04T00:00:00Z")


def test_fast_direction_excludes_zero_return_from_direction_target() -> None:
    returns = pd.Series([-0.1, 0.0, 0.2, np.nan])
    direction, movement = _direction_and_move_labels(returns)
    assert direction.iloc[0] == 0.0
    assert np.isnan(direction.iloc[1])
    assert direction.iloc[2] == 1.0
    assert np.isnan(direction.iloc[3])
    assert movement.iloc[:3].tolist() == [1.0, 0.0, 1.0]
    assert np.isnan(movement.iloc[3])


def test_chronological_split_keeps_two_embargoes_and_late_days() -> None:
    days = [f"2026-01-{value:02d}" for value in range(1, 12)]
    split = chronological_split(
        days,
        train_days=5,
        validation_days=2,
        test_days=2,
        embargo_days=1,
        late_days=("2026-01-12", "2026-01-13"),
    )
    assert split.train == tuple(days[:5])
    assert split.embargo_1 == (days[5],)
    assert split.validation == tuple(days[6:8])
    assert split.embargo_2 == (days[8],)
    assert split.test == tuple(days[9:11])
    assert split.late == ("2026-01-12", "2026-01-13")


def test_fast_external_delay_preserves_local_state(tmp_path: Path) -> None:
    day = "2026-01-01"
    path = _cache_path(tmp_path, "fast1s", day)
    path.parent.mkdir(parents=True)
    pd.DataFrame(
        {
            "fast_local_ret_1s": [1.0, 2.0, 3.0],
            "cv_external_perp_ret_1s": [10.0, 20.0, 30.0],
            "cv_external_perp_source_age_ms": [5.0, 6.0, 7.0],
        }
    ).to_parquet(path)
    frame = _read_days(
        tmp_path,
        "fast1s",
        [day],
        [
            "fast_local_ret_1s",
            "cv_external_perp_ret_1s",
            "cv_external_perp_source_age_ms",
        ],
        external_delay_s=1,
    )
    assert frame["fast_local_ret_1s"].tolist() == [1.0, 2.0, 3.0]
    assert frame["cv_external_perp_ret_1s"].tolist() == [0.0, 10.0, 20.0]
    assert frame["cv_external_perp_source_age_ms"].tolist() == [10_000.0, 1005.0, 1006.0]


def test_fast_labels_are_derived_from_declared_horizon_grid(tmp_path: Path) -> None:
    day = "2026-01-01"
    path = _cache_path(tmp_path, "fast1s", day)
    path.parent.mkdir(parents=True)
    pd.DataFrame(
        {
            "fast_local_ret_1s": [0.0, 0.1, -0.1, 0.2],
            FAST_LABEL_CLOSE_COLUMN: [100.0, 101.0, 100.0, 102.0],
        }
    ).to_parquet(path)
    frame = _read_days(
        tmp_path,
        "fast1s",
        [day],
        ["fast_local_ret_1s", "label_fast_dir_2s", "label_fast_move_2s"],
    )
    assert np.isnan(frame["label_fast_dir_2s"].iloc[0])
    assert frame["label_fast_dir_2s"].iloc[1] == 1.0
    assert np.isnan(frame["label_fast_dir_2s"].iloc[2])
    assert frame["label_fast_move_2s"].iloc[:2].tolist() == [0.0, 1.0]


def test_fast_target_specs_accept_dense_nonlegacy_horizons() -> None:
    specs = fast_target_specs(range(2, 7), kinds=("dir", "move"))
    assert tuple(specs) == (
        "dir_2s",
        "dir_3s",
        "dir_4s",
        "dir_5s",
        "dir_6s",
        "move_2s",
        "move_3s",
        "move_4s",
        "move_5s",
        "move_6s",
    )


def test_target_identity_is_cadence_aware_at_ten_seconds() -> None:
    assert target_spec("dir_10s", cadence="fast1s") == (
        "label_fast_dir_10s",
        "binary",
    )
    assert target_spec("dir_10s", cadence="10s") == ("label_dir_10s", "binary")


def test_missing_source_age_is_imputed_as_stale() -> None:
    frame = pd.DataFrame(
        {
            "cv_external_perp_source_age_ms": [np.nan, 7.0],
            "cv_external_perp_ret_1s": [np.nan, 0.1],
            "label_fast_dir_2s": [1.0, 0.0],
        }
    )
    features = ["cv_external_perp_source_age_ms", "cv_external_perp_ret_1s"]
    values, _ = _clean_xy(frame, features, "label_fast_dir_2s")
    assert values["cv_external_perp_source_age_ms"].tolist() == [10_000.0, 7.0]
    assert np.allclose(values["cv_external_perp_ret_1s"], [0.0, 0.1])


def test_early_stopping_uses_only_inner_development_days() -> None:
    days = tuple(f"2026-01-{value:02d}" for value in range(1, 21))
    inner = development_training_split(days, early_stop_days=5, embargo_days=1)
    assert inner.fit == days[:14]
    assert inner.embargo == (days[14],)
    assert inner.early_stop == days[15:]


def test_horizon_decay_selects_from_development_daily_curve_only() -> None:
    rows = []
    for day_index in range(20):
        day = f"2026-01-{day_index + 1:02d}"
        for horizon, gain in ((1, 0.002), (2, 0.020), (3, -0.004)):
            for profile, auc in (
                ("m0_local_binance", 0.55),
                ("m1_external_all", 0.55 + gain),
            ):
                rows.append(
                    {
                        "day": day,
                        "auc": auc,
                        "cadence": "fast1s",
                        "profile": profile,
                        "target": f"dir_{horizon}s",
                        "panel": "validation",
                    }
                )
    curve, selection = build_horizon_decay_curve(
        pd.DataFrame(rows),
        panel="validation",
        bootstrap_trials=500,
        min_paired_days=10,
    )
    assert selection["formal_selected_horizon_s"] == 2
    assert selection["selection_panel_role"] == "development_screening"
    assert selection["selection_uses_test_or_late"] is False
    selected = curve.loc[curve["horizon_s"].eq(2)].iloc[0]
    assert selected["simultaneous_lcb_95"] > 0.0
