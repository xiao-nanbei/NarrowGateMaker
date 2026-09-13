from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from models import backtest_tick as bt
from models.backtest_tick import _compile_cpp_fill_selection_static_rows
from strategy.fill_selection_model import (
    FillSelectionScoreEnsemble,
    build_fill_selection_feature_row,
    fill_selection_actionable,
)


def _write_test_model(tmp_path: Path) -> Path:
    payload = {
        "folds": [{
            "model": {
                "base_logit": -0.1,
                "contribution_scale": 0.5,
                "numeric_cuts": {
                    "fill_quality_score": [0.4, 0.8],
                    "inventory_ratio": [-0.1, 0.1],
                    "quote_distance_bps": [2.0, 10.0],
                },
                "categorical_features": [
                    "session_stack",
                    "side",
                    "quote_action",
                ],
                "contributions": {
                    "fill_quality_score": {
                        "missing": -0.25,
                        "b00": -0.5,
                        "b01": 0.5,
                        "b02": 1.0,
                    },
                    "inventory_ratio": {
                        "b00": -0.5,
                        "b01": 0.25,
                        "b02": 0.75,
                    },
                    "quote_distance_bps": {
                        "b00": 0.5,
                        "b01": 0.25,
                        "b02": -0.5,
                    },
                    "session_stack": {
                        "missing": -0.25,
                        "asia|london": 0.75,
                    },
                    "side": {"BUY": 0.5, "SELL": -0.5},
                    "quote_action": {"place": 0.25},
                },
            }
        }]
    }
    model_path = tmp_path / "fill_selection_model.json"
    model_path.write_text(json.dumps(payload), encoding="utf-8")
    return model_path


def _write_queue_rank_test_model(tmp_path: Path) -> Path:
    payload = {
        "folds": [{
            "model": {
                "base_logit": 0.0,
                "contribution_scale": 1.0,
                "numeric_cuts": {
                    "queue_local_rank": [0.75],
                },
                "categorical_features": [],
                "contributions": {
                    "queue_local_rank": {
                        "b00": -8.0,
                        "b01": 8.0,
                    },
                },
            }
        }]
    }
    model_path = tmp_path / "queue_rank_fill_selection_model.json"
    model_path.write_text(json.dumps(payload), encoding="utf-8")
    return model_path


def _row(*, allow_post: bool = True, allow_exposure: bool = True) -> dict:
    prediction = {
        "session_stack": "asia|london",
        "micro_macro_regime": "shock_transition",
        "quote_distance_bps": 999.0,
        "toxicity": 0.1,
    }
    quote_context = {
        "near_depth_total": 1.25,
        "toxicity": 0.49,
        "l2_book_refresh_ratio": 0.12,
        "l2_book_cancel_ratio": 0.03,
    }
    return build_fill_selection_feature_row(
        prediction_features=prediction,
        quote_context=quote_context,
        side="BUY",
        inventory=0.003,
        max_inventory=0.026,
        mid=64_000.0,
        base_price=63_968.0,
        allow_post=allow_post,
        allow_exposure_increase=allow_exposure,
        exposure_increasing=True,
        near_depth_total=1.25,
        toxicity=0.49,
        markout_ema=-0.25,
        queue_local_rank=0.4,
    )


def test_fill_selection_feature_contract_merges_prediction_and_quote_state() -> None:
    row = _row()

    assert row["session_stack"] == "asia|london"
    assert row["micro_macro_regime"] == "shock_transition"
    assert row["toxicity"] == pytest.approx(0.49)
    assert row["quote_distance_bps"] == pytest.approx(5.0)
    assert row["quote_action"] == "place"
    assert row["quote_allow_post"] == 1
    assert row["quote_allow_exposure_increase"] == 1
    assert row["order_exposure_increasing"] == 1
    assert row["fill_eligible"] is True


def test_fill_selection_feature_contract_preserves_actionable_gate() -> None:
    row = _row(allow_post=False, allow_exposure=True)

    assert row["quote_allow_post"] == 0
    assert row["quote_allow_exposure_increase"] == 1
    assert row["fill_eligible"] is False


def test_live_replay_rows_score_identically(tmp_path: Path) -> None:
    model = FillSelectionScoreEnsemble.load(_write_test_model(tmp_path))

    live_row = _row()
    replay_row = _row()
    assert model.score(live_row) == model.score(replay_row)


def test_fill_selection_actionability_requires_real_gate_and_no_hard_reason() -> None:
    assert fill_selection_actionable(
        threshold_hit=True,
        allow_post=True,
        allow_exposure_increase=True,
        hard_reason_active=False,
    )
    assert not fill_selection_actionable(
        threshold_hit=True,
        allow_post=True,
        allow_exposure_increase=True,
        hard_reason_active=True,
    )
    assert not fill_selection_actionable(
        threshold_hit=True,
        allow_post=False,
        allow_exposure_increase=True,
        hard_reason_active=False,
    )


def test_cpp_static_feature_compiler_matches_python_fold_contribution(tmp_path: Path) -> None:
    payload = {
        "folds": [{
            "model": {
                "base_logit": -0.2,
                "contribution_scale": 0.5,
                "numeric_cuts": {"fill_quality_score": [0.4, 0.8]},
                "categorical_features": ["session_stack"],
                "contributions": {
                    "fill_quality_score": {"b00": -1.0, "b01": 0.5, "b02": 2.0},
                    "session_stack": {"asia|london": 1.5},
                },
            }
        }]
    }
    model_path = tmp_path / "fill_selection_model.json"
    model_path.write_text(json.dumps(payload), encoding="utf-8")
    ts = np.asarray([1_000, 2_000], dtype=np.int64)
    feature_arrays = {
        "fill_quality_score": np.asarray([0.6, 0.9], dtype=np.float64),
        "session_stack": np.asarray(["asia|london", ""], dtype=object),
    }

    delta, missing, used = _compile_cpp_fill_selection_static_rows(
        model_path,
        ts,
        feature_arrays,
    )

    assert delta[:, 0] == pytest.approx([0.0, 1.0, 1.0])
    assert missing[:, 0] == pytest.approx([2.0, 0.0, 1.0])
    assert used[:, 0] == pytest.approx([0.0, 2.0, 1.0])

    scorer = FillSelectionScoreEnsemble.load(model_path)
    for row_idx in range(2):
        row = {
            "fill_quality_score": feature_arrays["fill_quality_score"][row_idx],
            "session_stack": feature_arrays["session_stack"][row_idx],
        }
        result = scorer.score(row)
        static_row_idx = row_idx + 1
        shrink = math.sqrt(
            used[static_row_idx, 0] / (used[static_row_idx, 0] + 4.0)
        )
        expected_logit = -0.2 + shrink * delta[static_row_idx, 0]
        expected = 1.0 / (1.0 + math.exp(-expected_logit))
        assert result.score == pytest.approx(expected)
        assert result.missing_features == int(missing[static_row_idx, 0])
        assert result.used_features == int(used[static_row_idx, 0])


def test_python_cpp_replay_use_identical_buy_score_and_action_gate(
    tmp_path: Path,
) -> None:
    pytest.importorskip("narrowgate_cpp")

    ts = np.arange(0, 5_000, 1_000, dtype=np.int64)
    trades = pd.DataFrame({
        "transact_time": ts,
        "price": np.full(ts.size, 100.0),
        "quantity": np.zeros(ts.size),
        "is_buyer_maker": np.zeros(ts.size, dtype=np.uint8),
    })
    one = np.asarray([0.0], dtype=np.float64)
    ml_data = (
        np.asarray([0], dtype=np.int64),
        np.asarray([0.5]),
        one,
        one,
        np.asarray([0.5]),
        np.asarray([0.5]),
        *([one] * len(bt.XMARKET_REPLAY_FEATURE_COLUMNS)),
        {
            "fill_quality_score": np.asarray([0.9]),
            "session_stack": np.asarray(["asia|london"], dtype=object),
            "l2_book_refresh_ratio": np.asarray([0.1]),
        },
    )
    params = {
        # Live keeps the BUY scorer active while the 13-head bundle is off.
        "ml_enabled": False,
        "buy_fill_selection_live_enabled": True,
        "buy_fill_selection_live_model_path": str(_write_test_model(tmp_path)),
        "buy_fill_selection_live_score_threshold": 0.44,
        "order_size": 0.001,
        "max_inventory": 0.026,
        "requote_interval": 1.0,
        "use_bar_pricing": True,
        "gamma": 0.05,
        "kappa": 0.073,
        "p3_kappa_eff_override": 0.055,
        "max_spread_bps": 24.0,
        "maker_fill_prob": 1.0,
        "maker_fee": 0.0,
        "max_exec_book_age_s": 0.0,
    }
    empty_i64 = np.empty(0, dtype=np.int64)
    empty_f64 = np.empty(0, dtype=np.float64)

    python_result = bt.simulate_tick(
        trades,
        empty_i64,
        empty_f64,
        params,
        ml_data=ml_data,
    )
    cpp_result = bt._simulate_tick_cpp(
        trades,
        empty_i64,
        empty_f64,
        params,
        ml_data=ml_data,
    )

    for key in (
        "buy_fill_selection_live_eval_count",
        "buy_fill_selection_live_hit_count",
        "buy_fill_selection_live_score_mean",
        "buy_fill_selection_live_score_max",
    ):
        assert cpp_result[key] == pytest.approx(python_result[key], abs=1e-15)


def test_python_cpp_buy_scorer_uses_causal_queue_local_rank(
    tmp_path: Path,
) -> None:
    pytest.importorskip("narrowgate_cpp")

    ts = np.arange(0, 7_000, 1_000, dtype=np.int64)
    prices = np.asarray([100.0, 99.0, 99.2, 99.8, 100.4, 100.8, 101.0])
    trades = pd.DataFrame({
        "transact_time": ts,
        "price": prices,
        "quantity": np.zeros(ts.size),
        "is_buyer_maker": np.zeros(ts.size, dtype=np.uint8),
    })
    one = np.asarray([0.0], dtype=np.float64)
    ml_data = (
        np.asarray([0], dtype=np.int64),
        np.asarray([0.5]),
        one,
        one,
        np.asarray([0.5]),
        np.asarray([0.5]),
        *([one] * len(bt.XMARKET_REPLAY_FEATURE_COLUMNS)),
        {
            "fill_quality_score": np.asarray([0.5]),
        },
    )
    params = {
        "ml_enabled": True,
        "buy_fill_selection_live_enabled": True,
        "buy_fill_selection_live_model_path": str(
            _write_queue_rank_test_model(tmp_path)
        ),
        "buy_fill_selection_live_score_threshold": 0.5,
        "order_size": 0.001,
        "max_inventory": 0.026,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "requote_clock": "fixed",
        "use_bar_pricing": True,
        "gamma": 0.05,
        "kappa": 1.0,
        "p3_kappa_eff_override": 1.0,
        "max_spread_bps": 24.0,
        "maker_fill_prob": 1.0,
        "maker_fee": 0.0,
        "max_exec_book_age_s": 0.0,
    }
    empty_i64 = np.empty(0, dtype=np.int64)
    empty_f64 = np.empty(0, dtype=np.float64)

    python_result = bt.simulate_tick(
        trades,
        empty_i64,
        empty_f64,
        params,
        ml_data=ml_data,
    )
    cpp_result = bt._simulate_tick_cpp(
        trades,
        empty_i64,
        empty_f64,
        params,
        ml_data=ml_data,
    )

    assert python_result["buy_fill_selection_live_hit_count"] > 0
    assert (
        python_result["buy_fill_selection_live_hit_count"]
        < python_result["buy_fill_selection_live_eval_count"]
    )
    for key in (
        "buy_fill_selection_live_eval_count",
        "buy_fill_selection_live_hit_count",
        "buy_fill_selection_live_score_mean",
        "buy_fill_selection_live_score_max",
    ):
        assert cpp_result[key] == pytest.approx(python_result[key], abs=1e-15)
