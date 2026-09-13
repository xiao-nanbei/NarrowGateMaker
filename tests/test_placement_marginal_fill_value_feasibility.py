from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.families.f06_placement_fill_cif.audit.placement_marginal_fill_value_feasibility import (
    _campaign_start_table,
    _common_activation_support,
    _maker_signed_value,
    _map_cohorts_to_campaigns,
    _select_current_request_state,
    build_pair_rows,
    classify_feasibility,
    reconstruct_campaign_terminals,
)


def test_maker_signed_usdc_value_has_one_favorable_direction() -> None:
    value = _maker_signed_value(
        pd.Series(["BUY", "SELL"]),
        pd.Series([0.001, 0.001]),
        pd.Series([100.0, 100.0]),
        pd.Series([101.0, 99.0]),
    )
    assert value.tolist() == pytest.approx([0.001, 0.001])


def test_current_request_state_does_not_shadow_counterfactual_action() -> None:
    request = pd.DataFrame(
        {
            "cohort_id": ["c1", "c1"],
            "action": ["current", "closer_1tick"],
            "cancel_request_reason": ["requote_replace", "requote_replace"],
        }
    )
    current = _select_current_request_state(request)
    assert current["cohort_id"].tolist() == ["c1"]
    assert "action" not in current.columns
    assert current["request_cancel_reason"].tolist() == ["requote_replace"]


def test_common_support_excludes_gtx_rejected_counterfactual() -> None:
    actions = _valued_actions()
    actions.loc[actions["action"].eq("closer_2tick"), "activation_status"] = (
        "gtx_reject"
    )
    assert _common_activation_support(actions).empty


def test_campaign_start_mapping_and_terminal_reconstruction_are_causal() -> None:
    source = pd.DataFrame(
        [
            {
                "cohort_id": "opener",
                "campaign_id": 0,
                "submit_ts_ns": 900_000_000,
                "campaign_age_s": 0.0,
                "inventory_role": "opener",
                "inventory": 0.0,
                "mid": 100.0,
                "campaign_pnl_so_far": 0.0,
                "side": "BUY",
                "current__first_fill_ts_ns": 1_000_000_000,
                "current__fill_qty": 0.001,
                "current__price_tick": 999,
            },
            {
                "cohort_id": "close",
                "campaign_id": 1,
                "submit_ts_ns": 2_000_000_000,
                "campaign_age_s": 1.0,
                "inventory_role": "reducing",
                "inventory": 0.001,
                "mid": 100.0,
                "campaign_pnl_so_far": -0.02,
                "side": "SELL",
                "current__first_fill_ts_ns": 2_100_000_000,
                "current__fill_qty": 0.001,
                "current__price_tick": 1001,
            },
        ]
    )
    starts = _campaign_start_table(source)
    mapping = _map_cohorts_to_campaigns(
        source, starts, opener_tolerance_ms=1.0
    ).set_index("cohort_id")
    assert mapping.loc["opener", "mapped_campaign_id"] == 1
    assert mapping.loc["opener", "campaign_mapping_source"] == "opener_start_match"

    terminals = reconstruct_campaign_terminals(
        source,
        bbo_ts_ms=np.asarray([1_000, 2_100], dtype=np.int64),
        bbo_mid=np.asarray([100.0, 100.0]),
        max_bbo_age_ms=1.0,
    )
    assert len(terminals) == 1
    assert terminals.iloc[0]["campaign_terminal_ts_ns"] == 2_100_000_000
    assert terminals.iloc[0]["baseline_campaign_terminal_pnl_usdc"] == pytest.approx(
        -0.0199
    )


def _valued_actions() -> pd.DataFrame:
    values = {
        "closer_4tick": -0.001,
        "closer_2tick": -0.001,
        "closer_1tick": -0.001,
        "current": 0.0,
        "farther_1tick": 0.0,
        "farther_2tick": 0.0,
        "farther_4tick": 0.0,
    }
    rows = []
    for action, value in values.items():
        filled = int(value != 0.0)
        rows.append(
            {
                "cohort_id": "c1",
                "day": "2026-01-01",
                "side": "BUY",
                "inventory_role": "add",
                "action": action,
                "activation_status": "active",
                "filled": filled,
                "fill_qty": 0.001 if filled else 0.0,
                "pending_fill": False,
                "pending_fill_qty": 0.0,
                "value_1000ms_usdc": value,
                "value_5000ms_usdc": value,
                "value_30000ms_usdc": value,
                "value_common_clock_usdc": value,
                "campaign_terminal_overlay_usdc": value,
                "mapped_campaign_id": 7,
                "campaign_mapping_source": "active_campaign_id",
                "cancel_request_reason": "requote_replace",
                "request_model_risk_set": 1,
                "request_valid_book": 1,
                "request_book_age_ms": 100.0,
                "baseline_campaign_terminal_pnl_usdc": -0.5,
            }
        )
    return pd.DataFrame(rows)


def test_pair_value_decomposition_uses_shallower_only_fill() -> None:
    pairs = build_pair_rows(
        _valued_actions(),
        tail_threshold_usdc=-5.0,
        fresh_book_max_age_ms=250.0,
    )
    row = pairs.loc[
        pairs["request_scope"].eq("all")
        & pairs["gap_ticks"].eq(2)
        & pairs["contrast"].eq("closer_current")
    ].iloc[0]
    assert row["marginal_shallower_fill"] == 1
    assert row["shared_fill"] == 0
    assert row["marginal_shallower_value_30s_usdc"] == pytest.approx(-0.001)
    assert row["shared_value_delta_30s_usdc"] == 0.0
    assert row["total_value_delta_30s_usdc"] == pytest.approx(0.001)
    assert row["campaign_terminal_overlay_delta_usdc"] == pytest.approx(0.001)


def test_only_two_or_four_tick_cells_can_pass_feasibility() -> None:
    rows = []
    for gap in (1, 2):
        rows.append(
            {
                "request_scope": "all",
                "side": "BUY",
                "inventory_role": "add",
                "gap_ticks": gap,
                "contrast": "closer_current",
                "campaign_terminal_overlay_delta_usdc": 0.001,
                "campaign_terminal_overlay_delta_usdc_simultaneous_lower": 0.0005,
                "campaign_terminal_overlay_delta_usdc_simultaneous_upper": 0.0015,
                "campaign_overlay_informative_coverage": 1.0,
                "primary_supported_days": 40,
                "primary_positive_day_fraction": 0.8,
                "primary_negative_day_fraction": 0.2,
                "pending_campaign_overlay_delta_usdc": 0.0,
                "pending_campaign_overlay_delta_usdc_simultaneous_lower": -0.00001,
                "pending_campaign_overlay_delta_usdc_simultaneous_upper": 0.00001,
                "campaign_tail_event_delta_simultaneous_lower": -0.01,
                "campaign_tail_event_delta_simultaneous_upper": 0.0,
            }
        )
    classified, decision = classify_feasibility(
        pd.DataFrame(rows),
        economic_epsilon_usdc=0.0001,
        minimum_supported_days=30,
        minimum_informative_campaign_coverage=0.95,
        minimum_daily_direction_fraction=0.65,
    )
    one_tick = classified.loc[classified["gap_ticks"].eq(1)].iloc[0]
    two_tick = classified.loc[classified["gap_ticks"].eq(2)].iloc[0]
    assert not one_tick["feasibility_supported"]
    assert two_tick["feasibility_supported"]
    assert decision == "marginal_fill_value_feasible_register_value_identity"
