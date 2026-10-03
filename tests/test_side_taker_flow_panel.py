from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.families.f08_side_taker_lifecycle.audit.side_taker_flow_panel import (
    attach_side_taker_features,
    build_causal_taker_state,
    normalize_individual_trades,
    summarize_side_taker_panel,
)

DAY = "2026-07-20"
DAY_START_MS = int(pd.Timestamp(DAY, tz="UTC").timestamp() * 1_000)


def _trades() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "price": [100.0, 100.1, 99.9, 99.8, 100.0],
            "qty": [1.0, 2.0, 3.0, 4.0, 5.0],
            "quote_qty": [100.0, 200.0, 300.0, 400.0, 500.0],
            "time": [
                DAY_START_MS + 10,
                DAY_START_MS + 20,
                DAY_START_MS + 150,
                DAY_START_MS + 180,
                DAY_START_MS + 260,
            ],
            # false => buyer is taker; true => seller is taker.
            "is_buyer_maker": [False, False, True, True, False],
        }
    )


def test_normalize_individual_trades_preserves_taker_side_and_runs() -> None:
    source = _trades().iloc[[2, 0, 1, 4, 3]].reset_index(drop=True)
    result = normalize_individual_trades(source)

    assert result["trade_id"].tolist() == [1, 2, 3, 4, 5]
    assert result["taker_side"].tolist() == ["BUY", "BUY", "SELL", "SELL", "BUY"]
    assert result["same_side_run"].tolist() == [1, 2, 1, 2, 1]


def test_right_edge_state_and_maker_counterparty_mapping() -> None:
    state, identity = build_causal_taker_state(
        _trades(),
        day=DAY,
        resolution_ms=100,
        windows_ms=(100, 500),
        start_ms=DAY_START_MS,
        end_ms=DAY_START_MS + 500,
    )
    assert identity.policy_eligible is False

    decisions = pd.DataFrame(
        {
            "day": [DAY, DAY, DAY, DAY],
            "side": ["BUY", "SELL", "BUY", "SELL"],
            "decision_ts_ns": np.asarray(
                [
                    DAY_START_MS + 99,
                    DAY_START_MS + 100,
                    DAY_START_MS + 200,
                    DAY_START_MS + 200,
                ],
                dtype=np.int64,
            )
            * 1_000_000,
        }
    )
    panel = attach_side_taker_features(
        decisions,
        state,
        windows_ms=(100, 500),
    )

    # The first two BUY taker trades are not visible before the 100ms edge.
    assert panel.loc[0, "taker_feature_available"] == 0
    # SELL maker's counterparty is the BUY taker flow visible at 100ms.
    assert panel.loc[1, "counterparty_taker_side"] == "BUY"
    assert panel.loc[1, "counterparty_taker_quote_100ms"] == pytest.approx(300.0)
    # BUY maker's counterparty is the SELL taker flow visible at 200ms.
    assert panel.loc[2, "counterparty_taker_side"] == "SELL"
    assert panel.loc[2, "counterparty_taker_quote_100ms"] == pytest.approx(700.0)
    assert panel.loc[2, "counterparty_taker_current_run"] == pytest.approx(2.0)
    # The same state means away flow for SELL maker, not counterparty flow.
    assert panel.loc[3, "counterparty_taker_quote_100ms"] == pytest.approx(0.0)
    assert panel.loc[3, "away_taker_quote_100ms"] == pytest.approx(700.0)
    assert bool(
        (
            panel["taker_feature_ready_ts_ns"].dropna()
            <= panel.loc[
                panel["taker_feature_ready_ts_ns"].notna(),
                "decision_ts_ns",
            ]
        ).all()
    )


def test_fixed_delay_requires_identity_and_moves_visibility() -> None:
    with pytest.raises(ValueError, match="latency_profile_id"):
        build_causal_taker_state(
            _trades(),
            day=DAY,
            resolution_ms=100,
            windows_ms=(100,),
            visibility_mode="fixed_delay_replay",
            visibility_delay_ms=50.0,
            start_ms=DAY_START_MS,
            end_ms=DAY_START_MS + 500,
        )

    state, identity = build_causal_taker_state(
        _trades(),
        day=DAY,
        resolution_ms=100,
        windows_ms=(100,),
        visibility_mode="fixed_delay_replay",
        visibility_delay_ms=100.0,
        latency_profile_id="provider_neutral_test",
        start_ms=DAY_START_MS,
        end_ms=DAY_START_MS + 500,
    )
    assert identity.policy_eligible is False
    first_ready = int(state.loc[state["buy_taker_quote_100ms"] > 0, "feature_ready_ts_ns"].iloc[0])
    assert first_ready == (DAY_START_MS + 200) * 1_000_000


def test_summary_is_side_specific_and_descriptive() -> None:
    state, _ = build_causal_taker_state(
        _trades(),
        day=DAY,
        resolution_ms=100,
        windows_ms=(100,),
        start_ms=DAY_START_MS,
        end_ms=DAY_START_MS + 500,
    )
    timestamps = np.arange(DAY_START_MS + 100, DAY_START_MS + 500, 20)
    decisions = pd.DataFrame(
        {
            "day": DAY,
            "side": np.where(np.arange(len(timestamps)) % 2 == 0, "BUY", "SELL"),
            "decision_ts_ns": timestamps.astype(np.int64) * 1_000_000,
            "fill_value_markout_bps": np.linspace(-1.0, 1.0, len(timestamps)),
        }
    )
    panel = attach_side_taker_features(decisions, state, windows_ms=(100,))
    summary = summarize_side_taker_panel(panel, windows_ms=(100,))

    assert set(summary["sides"]) == {"BUY", "SELL"}
    assert summary["sides"]["BUY"]["rows"] == 10
    assert summary["sides"]["SELL"]["rows"] == 10
    assert "not an action counterfactual" in summary["interpretation"]
