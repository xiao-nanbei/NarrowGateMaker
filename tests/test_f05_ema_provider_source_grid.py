from __future__ import annotations

from datetime import UTC, datetime

import numpy as np
import pandas as pd
import pytest

from models.replay.f05_ema_provider_source_grid import (
    F05ProviderSourceGridError,
    provider_ema_source_grid_batches,
    provider_encoder_feature_names,
)
from research.families.f05_fill_quality_quote_ev.audit.multiscale_ema_add_wait_incremental_value import (
    ContinuousTimeEmaSurface,
)


def _frame(timestamps: np.ndarray, mids: np.ndarray) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "best_bid": mids - 0.05,
            "best_ask": mids + 0.05,
        }
    )


def test_source_grid_uses_every_provider_row_and_matches_online_surface() -> None:
    day = "2025-08-02"
    start = int(datetime.fromisoformat(day).replace(tzinfo=UTC).timestamp() * 1_000)
    prior_ts = start + np.asarray([-500, -400, -300, -200, -100], dtype=np.int64)
    target_ts = start + np.asarray(
        [0, 100, 200, 400, 500, 900, 1_000, 1_100], dtype=np.int64
    )
    prior_mid = np.asarray([100.0, 100.1, 100.2, 100.1, 100.0])
    target_mid = np.asarray([100.2, 100.4, 100.1, 100.5, 100.3, 100.8, 100.6, 100.7])
    batches = list(
        provider_ema_source_grid_batches(
            _frame(prior_ts, prior_mid),
            _frame(target_ts, target_mid),
            day=day,
        )
    )
    assert [side for side, _, _ in batches] == ["BUY", "SELL"]
    assert all(len(matrix) == len(target_ts) for _, matrix, _ in batches)
    assert all(audit["sampling_stride"] == "none_all_admitted_source_rows" for _, _, audit in batches)

    names = provider_encoder_feature_names()
    assert names
    assert not any(name.endswith("_volatility_normalized") for name in names)
    expected: dict[str, list[list[float]]] = {"BUY": [], "SELL": []}
    surface = ContinuousTimeEmaSurface()
    for timestamp, mid in zip(
        np.concatenate((prior_ts, target_ts)),
        np.concatenate((prior_mid, target_mid)),
        strict=True,
    ):
        surface.update(ts_ns=int(timestamp) * 1_000_000, price=float(mid))
        if timestamp < start:
            continue
        for side in ("BUY", "SELL"):
            row = surface.feature_row(
                side=side,
                causal_volatility_bps=1.0,
                tick_bps=0.01,
            )
            expected[side].append([float(row[name]) for name in names])
    for side, matrix, _ in batches:
        assert np.allclose(
            matrix,
            np.asarray(expected[side], dtype=np.float64),
            rtol=0.0,
            atol=1e-10,
        )


def test_source_grid_rejects_rows_outside_admitted_resolution() -> None:
    day = "2025-08-02"
    start = int(datetime.fromisoformat(day).replace(tzinfo=UTC).timestamp() * 1_000)
    prior = _frame(np.asarray([start - 100]), np.asarray([100.0]))
    target = _frame(
        np.asarray([start, start + 125]),
        np.asarray([100.1, 100.2]),
    )
    with pytest.raises(F05ProviderSourceGridError, match="100ms grid"):
        list(provider_ema_source_grid_batches(prior, target, day=day))
