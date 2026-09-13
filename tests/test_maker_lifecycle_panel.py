from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.families.f10_live_replay_attribution.audit.maker_lifecycle_panel import (
    add_external_consensus,
    add_lifecycle_targets,
    validate_source_panel,
    _advance_source_age_ms,
    _align_state,
)


def _source_row(**overrides):
    row = {
        "day": "2026-01-01",
        "decision_id": "decision-1",
        "decision_ts_ns": 2_000_000_000,
        "decision_ts_ms": 2_000,
        "campaign_id": 1,
        "side": "BUY",
        "inventory_role": "add",
        "action": "r0_block",
        "behavior_propensity": 1 / 3,
        "base_price": 100.0,
        "decision_mtm": 1.0,
        "terminal_mtm": -5.0,
        "terminal_campaign_pnl": -6.0,
        "campaign_closed": 1,
        "campaign_censored": 0,
        "campaign_duration_s": 10.0,
        "campaign_mae": 2.0,
        "reward": -6.0,
        "campaign_cost": 6.0,
        "queue_cost": 0.0,
        "fill_value": 0.0,
        "intervention_fill_count": 0,
        "intervention_fill_qty": 0.0,
        "fill_markout_30s_bps": np.nan,
    }
    row.update(overrides)
    return row


def test_source_panel_requires_one_exact_decision_per_campaign() -> None:
    frame = pd.DataFrame([_source_row(), _source_row(decision_id="decision-2")])
    with pytest.raises(ValueError, match="only one intervention"):
        validate_source_panel(frame)


def test_lifecycle_targets_are_mutually_exclusive() -> None:
    frame = pd.DataFrame(
        [
            _source_row(),
            _source_row(
                day="2026-01-02",
                decision_id="decision-2",
                campaign_censored=1,
                campaign_closed=0,
                terminal_campaign_pnl=1.0,
            ),
        ]
    )
    validate_source_panel(frame)
    labelled = add_lifecycle_targets(frame, tail_threshold_usdc=-5.0)
    assert labelled["lifecycle_event"].tolist() == ["repair_tail", "censored_non_tail"]
    assert labelled["target_decision_to_terminal_mtm"].tolist() == [-6.0, -6.0]


def test_causal_state_alignment_never_reads_future_row() -> None:
    index = pd.to_datetime([1_000_000_000, 2_000_000_000], unit="ns", utc=True)
    state = pd.DataFrame({"value": [1.0, 2.0]}, index=index)
    selected, ready = _align_state(
        np.array([1_500_000_000, 2_050_000_000], dtype=np.int64),
        state,
        delay_ms=100.0,
        columns=["value"],
    )
    assert selected["value"].tolist() == [1.0, 1.0]
    assert ready.tolist() == [1_100_000_000, 1_100_000_000]


def test_injected_latency_is_included_in_external_source_age() -> None:
    age = _advance_source_age_ms(
        pd.Series([20.0]),
        np.array([1_500_000_000], dtype=np.int64),
        np.array([1_100_000_000], dtype=np.int64),
        injected_delay_ms=100.0,
    )
    # Base event was at 1.0s with age 20ms; decision is at 1.5s. The 100ms
    # injection moved ready time but cannot make the underlying source younger.
    assert age.iloc[0] == pytest.approx(520.0)


def test_external_consensus_is_recomputed_for_leave_one_out() -> None:
    row = {"decision_ts_ns": 2_000_000_000, "m0_bridge_ret_1s": 0.0}
    for venue, ret in (("bitget", 0.001), ("bybit", 0.002), ("okx", 0.100)):
        for factor in ("spot", "perp"):
            row[f"m1_{venue}_{factor}_available"] = 1.0
            row[f"m1_{venue}_{factor}_source_age_ms"] = 10.0
            row[f"m1_{venue}_{factor}_feature_ready_ts_ns"] = 1_900_000_000
            for horizon in (1, 3, 5):
                row[f"m1_{venue}_{factor}_ret_{horizon}s"] = ret
                row[f"m1_{venue}_{factor}_flow_{horizon}s"] = ret
    frame = pd.DataFrame([row])
    full = add_external_consensus(
        frame, included_venues=("bitget", "bybit", "okx"), prefix="m1_full"
    )
    loo = add_external_consensus(
        frame, included_venues=("bitget", "bybit"), prefix="m1_loo"
    )
    assert full.loc[0, "m1_full_spot_ret_1s"] == pytest.approx(0.002)
    assert loo.loc[0, "m1_loo_spot_ret_1s"] == pytest.approx(0.0015)


def test_external_consensus_rejects_future_feature_timestamp() -> None:
    row = {"decision_ts_ns": 2_000_000_000, "m0_bridge_ret_1s": 0.0}
    for venue in ("bitget", "bybit"):
        for factor in ("spot", "perp"):
            row[f"m1_{venue}_{factor}_available"] = 1.0
            row[f"m1_{venue}_{factor}_source_age_ms"] = 10.0
            row[f"m1_{venue}_{factor}_feature_ready_ts_ns"] = 2_100_000_000
            for horizon in (1, 3, 5):
                row[f"m1_{venue}_{factor}_ret_{horizon}s"] = 0.001
                row[f"m1_{venue}_{factor}_flow_{horizon}s"] = 0.1
    with pytest.raises(ValueError, match="future-visible"):
        add_external_consensus(
            pd.DataFrame([row]),
            included_venues=("bitget", "bybit"),
            prefix="m1_test",
        )
