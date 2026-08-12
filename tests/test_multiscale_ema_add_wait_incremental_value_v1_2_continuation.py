from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from models.replay.f05_ema_add_wait_two_day_window import (
    F05ReplayDay,
    F05WindowStitchError,
    stitch_two_days,
)
from models.replay.f05_ema_provider_pretraining import provider_ema_feature_batches
from models.replay.f05_ema_source_encoder import fit_full_rank_encoder
from models.tick_data_types import HistoricalBBOData, HistoricalL2Data
from research.families.f05_fill_quality_quote_ev.audit import (
    multiscale_ema_add_wait_incremental_value_v1_2_study as study,
)
from research.families.f05_fill_quality_quote_ev.audit.multiscale_ema_add_wait_incremental_value import (
    ContinuousTimeEmaSurface,
    model_feature_names,
)

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / (
    "research/families/f05_fill_quality_quote_ev/docs/"
    "multiscale_ema_add_wait_incremental_value_v1_2_spec_20260809.json"
)


def _bbo(clock: list[int], offset: float = 0.0) -> HistoricalBBOData:
    bid = np.asarray([100.0 + offset + index for index in range(len(clock))])
    return HistoricalBBOData(
        np.asarray(clock, dtype=np.int64),
        bid,
        bid + 1.0,
        np.full(len(clock), 2.0),
        np.full(len(clock), 3.0),
    )


def _l2(clock: list[int], offset: float = 0.0) -> HistoricalL2Data:
    bid = np.asarray(
        [[100.0 + offset + index, 99.0 + offset + index] for index in range(len(clock))]
    )
    return HistoricalL2Data(
        np.asarray(clock, dtype=np.int64),
        bid,
        np.full_like(bid, 2.0),
        bid + 1.0,
        np.full_like(bid, 3.0),
    )


def _replay_day(
    day: str,
    *,
    bbo_clock: list[int],
    variance_clock: list[int],
    trade_clock: list[int],
    ml_clock: list[int],
    bbo_offset: float = 0.0,
    variance_offset: float = 0.0,
) -> F05ReplayDay:
    window = SimpleNamespace(
        trades=pd.DataFrame(
            {
                "transact_time": trade_clock,
                "price": np.arange(len(trade_clock), dtype=np.float64) + 100.0,
            }
        ),
        var_ts_ms=np.asarray(variance_clock, dtype=np.int64),
        var_ssq=np.arange(len(variance_clock), dtype=np.float64) + variance_offset,
        var_ti=np.arange(len(variance_clock), dtype=np.float64) + variance_offset + 10.0,
        var_retsq=np.arange(len(variance_clock), dtype=np.float64) + variance_offset + 20.0,
        bbo_data=_bbo(bbo_clock, bbo_offset),
        l2_data=_l2(bbo_clock, bbo_offset),
        ml_data=None,
    )
    ml_values = np.arange(len(ml_clock), dtype=np.float64)
    return F05ReplayDay(
        day=day,
        window=window,
        ml_data=(
            np.asarray(ml_clock, dtype=np.int64),
            ml_values,
            {"tox_bid": ml_values + 1.0, "tox_ask": ml_values + 2.0},
        ),
        identities={"window": "a" * 64, "overlay": "b" * 64},
    )


def test_two_day_stitch_preserves_first_day_locators() -> None:
    first = _replay_day(
        "2026-01-01",
        bbo_clock=[100, 200, 300],
        variance_clock=[100, 200, 300],
        trade_clock=[10, 20],
        ml_clock=[1, 2],
    )
    second = _replay_day(
        "2026-01-02",
        bbo_clock=[200, 300, 400, 500],
        variance_clock=[200, 300, 400, 500],
        trade_clock=[30, 40],
        ml_clock=[3, 4],
        bbo_offset=-1.0,
        variance_offset=100.0,
    )
    # Align the two raw overlap rows while leaving derived variance deliberately
    # different. Daily warmup can alter derived overlap, but raw book state cannot.
    second.window.bbo_data.best_bid[:2] = first.window.bbo_data.best_bid[1:]
    second.window.bbo_data.best_ask[:2] = first.window.bbo_data.best_ask[1:]
    second.window.l2_data.bid_px[:2] = first.window.l2_data.bid_px[1:]
    second.window.l2_data.ask_px[:2] = first.window.l2_data.ask_px[1:]

    window, ml_data, audit = stitch_two_days(first, second)

    assert np.array_equal(window.bbo_data.ts_ms[:3], first.window.bbo_data.ts_ms)
    assert np.array_equal(window.l2_data.bid_px[:3], first.window.l2_data.bid_px)
    assert np.array_equal(window.var_ssq[:3], first.window.var_ssq)
    assert window.bbo_data.ts_ms.tolist() == [100, 200, 300, 400, 500]
    assert window.trades["transact_time"].tolist() == [10, 20, 30, 40]
    assert ml_data[0].tolist() == [1, 2, 3, 4]
    assert audit["utc_midnight_resets_state"] is False


def test_two_day_stitch_rejects_raw_overlap_drift_and_non_successor() -> None:
    first = _replay_day(
        "2026-01-01",
        bbo_clock=[100, 200],
        variance_clock=[100, 200],
        trade_clock=[10],
        ml_clock=[1],
    )
    second = _replay_day(
        "2026-01-02",
        bbo_clock=[200, 300],
        variance_clock=[200, 300],
        trade_clock=[20],
        ml_clock=[2],
    )
    with pytest.raises(F05WindowStitchError, match="overlap changed"):
        stitch_two_days(first, second)

    non_successor = _replay_day(
        "2026-01-03",
        bbo_clock=[300, 400],
        variance_clock=[300, 400],
        trade_clock=[30],
        ml_clock=[3],
    )
    with pytest.raises(F05WindowStitchError, match=r"natural D\+1"):
        stitch_two_days(first, non_successor)


def test_provider_irregular_ema_matches_online_surface() -> None:
    day = "2025-08-02"
    day_start_ms = int(datetime.fromisoformat(day).replace(tzinfo=UTC).timestamp() * 1_000)
    prior = pd.DataFrame(
        {
            "timestamp": [day_start_ms - 3_700],
            "best_bid": [99.9],
            "best_ask": [100.1],
        }
    )
    offsets = np.asarray(
        [0, 125, 980, 1_100, 7_450, 10_000, 23_775, 40_000, 65_500],
        dtype=np.int64,
    )
    mids = np.asarray([100.0, 100.2, 99.8, 100.4, 100.1, 100.7, 99.9, 100.5, 100.3])
    target = pd.DataFrame(
        {
            "timestamp": day_start_ms + offsets,
            "best_bid": mids - 0.1,
            "best_ask": mids + 0.1,
        }
    )
    sample_ts = np.arange(day_start_ms, day_start_ms + 86_400_000, 10_000)
    features = pd.DataFrame(
        {"volatility_5s": np.full(len(sample_ts), 0.0002)},
        index=pd.to_datetime(sample_ts, unit="ms", utc=True),
    )

    batches = provider_ema_feature_batches(prior, target, features, day=day)
    source = pd.concat((prior, target), ignore_index=True).sort_values("timestamp")
    source_mid = 0.5 * (source["best_bid"] + source["best_ask"])
    surface = ContinuousTimeEmaSurface()
    cursor = 0
    names = model_feature_names()
    for sample_index in (0, 1, 2, 4, 7, 100, len(sample_ts) - 1):
        visible_ts = int(sample_ts[sample_index])
        while cursor < len(source) and int(source.iloc[cursor]["timestamp"]) <= visible_ts:
            surface.update(
                ts_ns=int(source.iloc[cursor]["timestamp"]) * 1_000_000,
                price=float(source_mid.iloc[cursor]),
            )
            cursor += 1
        current_mid = float(source_mid.iloc[cursor - 1])
        for side in ("BUY", "SELL"):
            expected = surface.feature_row(
                side=side,
                causal_volatility_bps=2.0,
                tick_bps=0.1 / current_mid * 10_000.0,
            )
            actual = batches[side][sample_index]
            assert np.allclose(
                actual,
                np.asarray([expected[name] for name in names], dtype=np.float64),
                rtol=0.0,
                atol=1e-10,
            )


def test_full_rank_2025_encoder_is_deterministic_and_lossless() -> None:
    rng = np.random.default_rng(20260809)
    values = rng.normal(size=(256, 6))
    names = tuple(f"ema_{index}" for index in range(values.shape[1]))
    first = fit_full_rank_encoder((values[:128], values[128:]), feature_names=names)
    second = fit_full_rank_encoder((values,), feature_names=names)
    assert np.allclose(first.mean, second.mean, rtol=0.0, atol=1e-14)
    assert np.allclose(first.components, second.components, rtol=0.0, atol=1e-12)
    encoded = first.transform(values)
    reconstructed = encoded @ first.components
    assert np.allclose(
        reconstructed,
        (values - first.mean) / first.scale,
        rtol=0.0,
        atol=1e-10,
    )


def test_v1_2_spec_uses_2025_only_for_representation_pretraining() -> None:
    spec = json.loads(SPEC.read_text(encoding="utf-8"))
    source = spec["source_aware_training"]
    permission = spec["permission_boundary"]
    assert source["provider_training_spec"]["expected_2025_days"] == 66
    assert source["provider_economic_outcomes_read"] is False
    assert source["provider_add_wait_labels_generated"] is False
    assert source["native_oof_only_for_incremental_evidence"] is True
    assert permission["f09_registration_authorized"] is False
    assert permission["action_authorized"] is False
    assert permission["live_authorized"] is False


def test_v1_2_runner_uses_frozen_source_identity_for_public_projections() -> None:
    assert study._spec_sha256() != study._sha256_file(study.SPEC)
    assert study._execution_amendment_sha256() != study._sha256_file(study.EXECUTION_AMENDMENT)


def test_v1_2_execution_fails_closed_when_predecessor_pointer_bytes_are_missing() -> None:
    with pytest.raises(
        study.StudyError,
        match="predecessor frozen operational baseline pointer exact bytes are missing",
    ):
        study._spec()
