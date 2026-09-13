from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from research.families.f02_empirical_p3_touch.audit.p3_reach_time_cache import (
    build_reach_label_surface,
    label_cache_key,
    load_label_cache,
    write_label_cache,
)
from research.families.f02_empirical_p3_touch.audit.p3_reach_time_context import (
    CONTEXT_COLUMNS,
    canonical_origins_ms,
    context_cache_key,
    extract_reach_time_context,
    load_context_cache,
    write_context_cache,
)
from research.families.f02_empirical_p3_touch.audit.p3_reach_time_surface import (
    ReachTimeGridSpec,
)


def _write_bbo(path: Path, day: str) -> None:
    start = int(pd.Timestamp(day, tz="UTC").timestamp() * 1000)
    timestamps = start + np.arange(0, 90_000, 1_000, dtype=np.int64)
    mid = 100.0 + np.arange(len(timestamps), dtype=np.float64) * 0.1
    pd.DataFrame(
        {
            "timestamp": timestamps,
            "best_bid": mid - 0.1,
            "best_ask": mid + 0.1,
        }
    ).to_parquet(path, index=False)


def _write_trades(path: Path, day: str) -> None:
    start = int(pd.Timestamp(day, tz="UTC").timestamp() * 1000)
    pd.DataFrame(
        {
            "price": [105.8, 106.4, 106.6],
            "transact_time": [start + 60_100, start + 60_200, start + 60_300],
            "is_buyer_maker": [True, False, False],
        }
    ).to_csv(path, index=False)


def test_canonical_origin_contract() -> None:
    origins = canonical_origins_ms("2026-01-01")
    assert len(origins) == 8_631
    day_start = int(pd.Timestamp("2026-01-01", tz="UTC").timestamp() * 1000)
    assert origins[0] == day_start + 60_000
    assert origins[-1] == day_start + 86_360_000


def test_context_and_label_cache_roundtrip(tmp_path: Path) -> None:
    day = "2026-01-01"
    bbo_path = tmp_path / "bbo.parquet"
    trade_path = tmp_path / "trades.csv"
    _write_bbo(bbo_path, day)
    _write_trades(trade_path, day)
    context = extract_reach_time_context(
        day=day,
        source_profile="native",
        bbo_path=bbo_path,
        administrative_censor_ms=86_330_000,
    )
    assert tuple(context.columns) == CONTEXT_COLUMNS
    assert len(context) == 1
    assert int(context.iloc[0]["feature_ready_ts_ms"]) <= int(
        context.iloc[0]["origin_ts_ms"]
    )

    context_key = context_cache_key(
        day=day,
        source_profile="native",
        bbo_sha256="b" * 64,
        extractor_sha256="e" * 64,
        tick_size=0.1,
        cadence_ms=10_000,
        administrative_censor_ms=86_330_000,
        max_bbo_age_ms=5_000,
        fast_window_s=10,
        slow_window_s=60,
        variance_floor=1e-6,
    )
    context_path = tmp_path / "context.parquet"
    write_context_cache(
        context_path,
        frame=context,
        cache_key=context_key,
        identity={"bbo_sha256": "b" * 64},
    )
    loaded_context, _ = load_context_cache(
        context_path, expected_cache_key=context_key
    )
    pd.testing.assert_frame_equal(loaded_context, context)

    grid = ReachTimeGridSpec(
        time_step_ms=100,
        max_horizon_ms=500,
        max_distance_ticks=20,
    )
    surface = build_reach_label_surface(
        context=context,
        trade_path=trade_path,
        spec=grid,
    )
    assert surface.buy_cumulative_reach_ticks.shape == (1, 5)
    assert surface.sell_cumulative_reach_ticks.shape == (1, 5)
    label_key = label_cache_key(
        day=day,
        context_cache_key=context_key,
        trade_sha256="t" * 64,
        label_kernel_sha256="k" * 64,
        tick_size=0.1,
        spec=grid,
    )
    label_path = tmp_path / "labels.npz"
    write_label_cache(
        label_path,
        origins_ms=context["origin_ts_ms"].to_numpy(dtype=np.int64),
        surface=surface,
        cache_key=label_key,
        identity={"trade_sha256": "t" * 64},
    )
    origins, restored, _ = load_label_cache(
        label_path, expected_cache_key=label_key
    )
    np.testing.assert_array_equal(origins, context["origin_ts_ms"])
    np.testing.assert_array_equal(
        restored.buy_cumulative_reach_ticks,
        surface.buy_cumulative_reach_ticks,
    )


def test_context_cache_rejects_manifest_tamper(tmp_path: Path) -> None:
    day = "2026-01-01"
    bbo_path = tmp_path / "bbo.parquet"
    _write_bbo(bbo_path, day)
    context = extract_reach_time_context(
        day=day,
        source_profile="native",
        bbo_path=bbo_path,
        administrative_censor_ms=86_330_000,
    )
    path = tmp_path / "context.parquet"
    write_context_cache(path, frame=context, cache_key="key", identity={})
    manifest_path = path.with_suffix(".parquet.manifest.json")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["rows"] += 1
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="canonical hash mismatch"):
        load_context_cache(path, expected_cache_key="key")


def test_context_rejects_off_tick_bbo(tmp_path: Path) -> None:
    day = "2026-01-01"
    bbo_path = tmp_path / "bbo.parquet"
    _write_bbo(bbo_path, day)
    frame = pd.read_parquet(bbo_path)
    frame.loc[frame.index[60], "best_bid"] += 0.02
    frame.to_parquet(bbo_path, index=False)
    with pytest.raises(ValueError, match="tick grid"):
        extract_reach_time_context(
            day=day,
            source_profile="native",
            bbo_path=bbo_path,
            administrative_censor_ms=86_330_000,
        )
