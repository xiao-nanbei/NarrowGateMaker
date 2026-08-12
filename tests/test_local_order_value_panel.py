from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.families.f07_active_order_continuation.audit import local_order_value_panel as order_value_panel
from research.families.f07_active_order_continuation.audit.local_order_value_panel import (
    COMPETING_RISK_LABEL_IDENTITY,
    DEFAULT_FEATURE_SPECS,
    EVENT_TYPES,
    SIMULATOR_AUDIT_SPECS,
    add_competing_risk_labels,
    add_first_mid_hit_labels,
    add_native_first_mid_hit_labels,
    validate_randomized_action_panel,
)
from research.families.f07_active_order_continuation.audit.local_order_value_replay import (
    _stream_concat_parquet,
    build_watch_manifest,
)


def _source() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "day": ["2026-01-01"] * 3,
            "decision_id": ["d1", "d2", "d3"],
            "order_id": [1, 2, 3],
            "campaign_id": [1, 2, 3],
            "side": ["BUY", "SELL", "BUY"],
            "decision_ts_ns": [1_000, 2_000, 3_000],
            "feature_ready_ts_ns": [999, 2_000, 2_999],
            "censor_ts_ns": [10_000, 10_000, 10_000],
            "fill_ts_ns": [2_000, np.nan, np.nan],
            "fill_value_markout_bps": [0.5, np.nan, np.nan],
            "cancel_ack_ts_ns": [3_000, 5_000, np.nan],
            "adverse_price_jump_ts_ns": [4_000, 4_000, np.nan],
            "repair_ts_ns": [8_000, 9_000, 6_000],
        }
    )


def test_first_event_is_mutually_exclusive_and_causal() -> None:
    panel = add_competing_risk_labels(_source())

    assert panel["first_event"].tolist() == [
        "favorable_fill",
        "adverse_price_jump",
        "campaign_repair",
    ]
    assert panel[[f"event_{event}" for event in EVENT_TYPES]].sum(axis=1).tolist() == [
        1,
        1,
        1,
    ]
    assert (panel["first_event_ts_ns"] >= panel["decision_ts_ns"]).all()
    assert set(panel["label_identity"]) == {COMPETING_RISK_LABEL_IDENTITY}
    assert set(panel["adverse_price_jump_timestamp_source"]) == {
        "adverse_price_jump_ts_ns"
    }
    assert not panel[
        "native_future_mid_first_hit_used_in_competing_risk"
    ].astype(bool).any()
    assert set(panel["cancel_event_role"]) == {
        "baseline_policy_action_or_censor"
    }
    assert set(panel["campaign_repair_event_role"]) == {
        "post_fill_campaign_transition"
    }
    assert not panel["competing_risk_action_independent"].astype(bool).any()


def test_future_feature_timestamp_is_rejected() -> None:
    source = _source()
    source.loc[0, "feature_ready_ts_ns"] = 1_001

    with pytest.raises(ValueError, match="feature_ready_ts_ns"):
        add_competing_risk_labels(source)


def test_native_exchange_book_state_is_simulator_only() -> None:
    policy_features = {spec.name for spec in DEFAULT_FEATURE_SPECS}
    simulator_fields = {spec.name for spec in SIMULATOR_AUDIT_SPECS}

    assert "exchange_book_queue_status" not in policy_features
    assert "simulator_queue_init" not in policy_features
    assert "simulator_queue_source" not in policy_features
    assert "exchange_book_queue_status" in simulator_fields
    assert "simulator_queue_init" in simulator_fields
    assert "simulator_queue_source" in simulator_fields
    assert {
        spec.available_at for spec in SIMULATOR_AUDIT_SPECS
    } == {"simulator_only"}


def test_simultaneous_events_without_shared_sequence_are_censored() -> None:
    source = _source().iloc[[0]].copy()
    source["cancel_ack_ts_ns"] = source["fill_ts_ns"]

    ambiguous = add_competing_risk_labels(source)
    assert ambiguous.iloc[0]["first_event"] == "censored"
    assert ambiguous.iloc[0]["first_event_ts_ns"] == source.iloc[0]["fill_ts_ns"]
    assert ambiguous.iloc[0]["label_censor_reason"] == (
        "same_ms_competing_event_ambiguous"
    )

    source["fill_event_seq"] = 1
    source["cancel_event_seq"] = 2
    panel = add_competing_risk_labels(source)
    assert panel.iloc[0]["first_event"] == "favorable_fill"


def test_randomized_action_panel_enforces_propensity_and_reward_identity() -> None:
    frame = pd.DataFrame(
        {
            "day": ["2026-01-01", "2026-01-01"],
            "decision_id": ["a", "b"],
            "campaign_id": [1, 2],
            "order_id": [10, 11],
            "action": ["keep", "cancel_until_state_exit"],
            "behavior_propensity": [0.5, 0.5],
            "behavior_prob_keep": [0.5, 0.5],
            "behavior_prob_cancel_until_state_exit": [0.5, 0.5],
            "reward": [1.0, -1.0],
            "fill_value": [1.5, 0.0],
            "campaign_cost": [0.5, 1.0],
            "queue_cost": [0.0, 0.0],
            "reward_identity_error": [0.0, 0.0],
        }
    )

    validate_randomized_action_panel(frame)

    frame.loc[0, "behavior_propensity"] = 0.4
    with pytest.raises(ValueError, match="behavior_propensity"):
        validate_randomized_action_panel(frame)


def test_fill_without_complete_value_horizon_is_right_censored() -> None:
    frame = _source().iloc[[0]].copy()
    frame["fill_value_horizon_censored"] = 1

    panel = add_competing_risk_labels(frame)

    assert panel.iloc[0]["first_event"] == "censored"
    assert (
        panel.iloc[0]["label_censor_reason"]
        == "fill_value_horizon_right_censored"
    )
    assert panel.iloc[0]["censor_ts_ns"] == frame.iloc[0]["fill_ts_ns"]


def test_first_mid_hit_labels_use_strictly_future_bbo(tmp_path) -> None:
    bbo_dir = tmp_path / "bbo"
    bbo_dir.mkdir()
    pd.DataFrame(
        {
            "timestamp": [1, 100, 200, 300, 400],
            "best_bid": [99.9, 99.9, 100.0, 99.8, 99.9],
            "best_ask": [100.1, 100.1, 100.2, 100.0, 100.1],
        }
    ).to_parquet(bbo_dir / "BTCUSDC-bbo-2026-01-01.parquet")
    source = pd.DataFrame(
        {
            "day": ["2026-01-01", "2026-01-01"],
            "decision_ts_ns": [100_000_000, 200_000_000],
            "best_bid": [99.9, 100.0],
            "best_ask": [100.1, 100.2],
        }
    )

    labeled = add_first_mid_hit_labels(
        source,
        bbo_dir=bbo_dir,
        symbol="BTCUSDC",
        tick_size=0.1,
        horizon_ms=200,
    )

    assert labeled["future_mid_first_hit_direction"].tolist() == [1, -1]
    assert labeled["future_mid_first_hit_ts_ns"].tolist() == [
        200_000_000,
        300_000_000,
    ]
    assert labeled["future_mid_first_hit_censored"].tolist() == [0, 0]


def test_native_first_mid_hit_labels_use_exchange_time_path(
    monkeypatch,
    tmp_path,
) -> None:
    def fake_mid_path(**_kwargs):
        return (
            np.asarray(
                [150_000_000, 200_000_000, 300_000_000],
                dtype=np.int64,
            ),
            np.asarray([100.0, 100.1, 99.9], dtype=float),
        )

    monkeypatch.setattr(
        order_value_panel,
        "_native_exchange_mid_path",
        fake_mid_path,
    )
    source = pd.DataFrame(
        {
            "day": ["2026-01-01", "2026-01-01"],
            "decision_ts_ns": [150_000_000, 200_000_000],
            "best_bid": [99.9, 100.0],
            "best_ask": [100.1, 100.2],
        }
    )

    labeled = add_native_first_mid_hit_labels(
        source,
        raw_root=tmp_path,
        symbol="BTCUSDC",
        tick_size=0.1,
        horizon_ms=200,
    )

    assert labeled["future_mid_first_hit_direction"].tolist() == [1, -1]
    assert labeled["future_mid_first_hit_ts_ns"].tolist() == [
        200_000_000,
        300_000_000,
    ]
    assert labeled["future_mid_first_hit_censored"].tolist() == [0, 0]
    assert set(labeled["future_mid_first_hit_source"]) == {
        "native_snapshot_delta_exchange_time_strictly_after_decision"
    }


def test_watch_manifest_uses_complete_order_lifetime_and_integer_ticks() -> None:
    trace = pd.DataFrame(
        {
            "day": ["2026-01-01"] * 3,
            "order_id": [7, 7, 8],
            "side": ["BUY", "BUY", "SELL"],
            "price": [99.9, 99.9, 100.1],
            "submit_ts": [900, 900, 2_000],
            "activate_ts": [1_000, 1_000, 2_100],
            "outcome_ts": [1_500, 1_800, 2_050],
            "outcome": ["fill", "cancel", "cancel"],
            "cancel_reason": ["", "replace", "cancel_before_active"],
            "campaign_id_at_submit": [3, 3, 4],
            "inventory_role_at_submit": ["add", "add", "reducing"],
            "reduce_only": [False, False, True],
        }
    )

    watch = build_watch_manifest(trace, tick_size=0.1)

    assert watch["watch_id"].tolist() == [
        "baseline_discovery:2026-01-01:7"
    ]
    assert watch.iloc[0]["price_tick"] == 999
    assert watch.iloc[0]["activate_ts_ms"] == 1_000
    assert watch.iloc[0]["stop_ts_ms"] == 1_800
    assert watch.iloc[0]["stop_reason"] == "cancel"
    assert watch.iloc[0]["source_row_count"] == 2


def test_watch_manifest_uses_explicit_trajectory_identity() -> None:
    trace = pd.DataFrame(
        {
            "day": ["2026-06-05"],
            "order_id": [7],
            "side": ["BUY"],
            "price": [100.0],
            "submit_ts": [900],
            "activate_ts": [1_000],
            "outcome_ts": [2_000],
            "outcome": ["cancel"],
        }
    )

    watch = build_watch_manifest(
        trace,
        tick_size=0.1,
        trajectory_id="deep250_sparse_g2",
    )

    assert watch.iloc[0]["trajectory_id"] == "deep250_sparse_g2"
    assert watch.iloc[0]["watch_id"] == (
        "deep250_sparse_g2:2026-06-05:7"
    )


def test_stream_concat_parquet_preserves_partition_rows(tmp_path) -> None:
    first = tmp_path / "first.parquet"
    second = tmp_path / "second.parquet"
    output = tmp_path / "combined.parquet"
    pd.DataFrame(
        {
            "day": ["2026-01-01", "2026-01-01"],
            "order_id": [1, 2],
            "event_seq": [0, 0],
        }
    ).to_parquet(first, index=False)
    pd.DataFrame(
        {
            "day": ["2026-01-02"],
            "order_id": [1],
            "event_seq": [0],
        }
    ).to_parquet(second, index=False)

    _stream_concat_parquet([first, second], output)

    combined = pd.read_parquet(output)
    assert combined.to_dict("records") == [
        {"day": "2026-01-01", "order_id": 1, "event_seq": 0},
        {"day": "2026-01-01", "order_id": 2, "event_seq": 0},
        {"day": "2026-01-02", "order_id": 1, "event_seq": 0},
    ]
