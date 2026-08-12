from __future__ import annotations

from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from research.families.f02_empirical_p3_touch.audit.p3_touch_decision_cadence_context import (
    DECISION_CONTEXT_FIELDS,
    DecisionCadenceContextBatch,
)
from research.families.f02_empirical_p3_touch.audit.p3_touch_decision_cadence_transport import (
    ACTION_NAMES,
    TransportGates,
    aggregate_calibration_summaries,
    calibration_summary,
    evaluate_transport,
    score_decision_day,
    strict_future_aggressive_reach,
    summarize_scored_day,
)


class _FakeModels:
    def fold_id(self, day: str) -> str:
        return "fold_01" if day else ""

    def predict_v4(self, *, day, context, side, distances):
        assert day == "2026-06-08"
        assert side in {"BUY", "SELL"}
        assert np.all(np.asarray(context["feature_ready_ts_ms"]) <= context["start_ts_ms"])
        return np.clip(0.75 - 0.02 * np.asarray(distances), 0.01, 0.99)

    def predict_v2(self, distances):
        return np.clip(0.70 - 0.015 * np.asarray(distances), 0.01, 0.99)


def _context_batch() -> DecisionCadenceContextBatch:
    decision_ts = np.asarray([1_000_000, 1_000_000], dtype=np.int64)
    frame = pd.DataFrame(
        {
            "decision_id": ["buy", "sell"],
            "day": ["2026-06-08", "2026-06-08"],
            "side": ["BUY", "SELL"],
            "inventory_role": ["opener", "add"],
            "campaign_id": [1, 2],
            "decision_ts_ms": decision_ts,
            "baseline_price_tick": [990, 1_011],
            "best_bid": [100.0, 100.0],
            "best_ask": [100.1, 100.1],
            "supported": [True, True],
        }
    )
    values = {
        "start_ts_ms": decision_ts,
        "feature_ready_ts_ms": decision_ts - 1,
        "best_bid": np.asarray([100.0, 100.0]),
        "best_ask": np.asarray([100.1, 100.1]),
        "mid": np.asarray([100.05, 100.05]),
        "spread": np.asarray([0.1, 0.1]),
        "fast_variance": np.asarray([1.0, 1.0]),
        "slow_variance": np.asarray([4.0, 4.0]),
        "fast_sigma": np.asarray([1.0, 1.0]),
        "slow_sigma": np.asarray([2.0, 2.0]),
        "volatility_ratio": np.asarray([0.5, 0.5]),
        "book_age_ms": np.asarray([1.0, 1.0]),
    }
    for field in DECISION_CONTEXT_FIELDS:
        frame[field] = values[field]
    frame["unsupported_reason"] = None
    return DecisionCadenceContextBatch(
        frame=frame,
        metadata={"rows": 2, "supported_rows": 2},
    )


def test_strict_future_reach_excludes_both_time_boundaries():
    decisions = pd.DataFrame(
        {
            "decision_ts_ms": [1_000, 1_000],
            "side": ["BUY", "SELL"],
            "best_bid": [100.0, 100.0],
            "best_ask": [100.1, 100.1],
        }
    )
    timestamps = np.asarray([1_000, 1_001, 10_999, 11_000], dtype=np.int64)
    prices = np.asarray([50.0, 99.2, 101.3, 150.0])
    buyer_maker = np.asarray([True, True, False, False])

    reach = strict_future_aggressive_reach(
        decisions,
        trade_ts_ms=timestamps,
        trade_prices=prices,
        buyer_maker=buyer_maker,
    )

    assert reach[0] == pytest.approx(0.8)
    assert reach[1] == pytest.approx(1.2)


def test_scores_each_side_and_all_exact_candidate_distances():
    batch = _context_batch()
    scored = score_decision_day(
        batch,
        models=_FakeModels(),
        trade_ts_ms=np.asarray([1_000_001, 1_000_002], dtype=np.int64),
        trade_prices=np.asarray([99.1, 101.2]),
        buyer_maker=np.asarray([True, False]),
    )

    assert len(scored) == 2 * len(ACTION_NAMES)
    buy = scored.loc[scored["side"].eq("BUY")].set_index("action")
    sell = scored.loc[scored["side"].eq("SELL")].set_index("action")
    assert buy.loc["current", "distance_usdc_per_btc"] == pytest.approx(1.0)
    assert sell.loc["current", "distance_usdc_per_btc"] == pytest.approx(1.0)
    assert buy.loc["closer_4tick", "distance_usdc_per_btc"] == pytest.approx(0.6)
    assert sell.loc["farther_4tick", "distance_usdc_per_btc"] == pytest.approx(1.4)
    assert buy.loc["current", "touch_label"] == 0
    assert sell.loc["current", "touch_label"] == 1
    assert buy.loc["closer_4tick", "p_v4"] > buy.loc["farther_4tick", "p_v4"]

    daily, campaigns = summarize_scored_day(
        scored,
        denominator=batch.frame,
        context_batch=batch,
    )
    assert len(daily) == 4
    assert daily["context_coverage"].eq(1.0).all()
    assert daily["candidate_distance_coverage"].eq(1.0).all()
    assert len(campaigns) == 2


def _passing_daily_metrics() -> pd.DataFrame:
    rows = []
    for index in range(30):
        day = f"2026-06-{index + 1:02d}"
        fold = f"fold_{index // 8 + 1:02d}"
        for side in ("BUY", "SELL"):
            for metric in ("current", "candidate_grid"):
                rows.append(
                    {
                        "day": day,
                        "fold_id": fold,
                        "side": side,
                        "metric": metric,
                        "context_coverage": 0.99,
                        "candidate_distance_coverage": 0.99,
                        "brier_delta_v4_minus_v2": -0.01,
                    }
                )
    return pd.DataFrame(rows)


def _passing_calibration() -> pd.DataFrame:
    rows = []
    for side in ("BUY", "SELL"):
        for metric in ("current", "candidate_grid"):
            for model, iace in (("v4", 0.01), ("v2", 0.02)):
                rows.append(
                    {
                        "side": side,
                        "metric": metric,
                        "model": model,
                        "bin_id": 0,
                        "iace": iace,
                    }
                )
    return pd.DataFrame(rows)


def test_transport_gate_requires_coverage_and_all_frozen_folds():
    gates = TransportGates(bootstrap_draws=200)
    report = evaluate_transport(
        daily_metrics=_passing_daily_metrics(),
        calibration=_passing_calibration(),
        monotonicity_violations=0,
        gates=gates,
    )
    assert report["supported"] is True
    assert report["permissions"]["action_authority"] is False

    three_folds = _passing_daily_metrics().copy()
    three_folds["fold_id"] = np.resize(
        np.asarray(["fold_01", "fold_02", "fold_03"]), len(three_folds)
    )
    three_folds.loc[three_folds.index[0], "context_coverage"] = 0.80
    failed = evaluate_transport(
        daily_metrics=three_folds,
        calibration=_passing_calibration(),
        monotonicity_violations=0,
        gates=replace(gates, minimum_supported_days=1),
    )
    assert failed["supported"] is False
    assert failed["gate_results"]["required_oof_fold_count"] is False
    assert failed["gate_results"]["minimum_context_coverage"] is False


def test_calibration_summary_keeps_v4_and_v2_separate():
    batch = _context_batch()
    scored = score_decision_day(
        batch,
        models=_FakeModels(),
        trade_ts_ms=np.asarray([1_000_001, 1_000_002], dtype=np.int64),
        trade_prices=np.asarray([99.1, 101.2]),
        buyer_maker=np.asarray([True, False]),
    )
    calibration = calibration_summary(scored)
    assert set(calibration["model"]) == {"v4", "v2"}
    assert set(calibration["metric"]) == {"current", "candidate_grid"}
    combined = aggregate_calibration_summaries([calibration, calibration])
    assert combined["rows"].sum() == 2 * calibration["rows"].sum()
