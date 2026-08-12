from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from models import backtest_tick as bt
from models.tick_data_types import HistoricalBBOData
from research.families.f09_campaign_action_uplift.audit.lineage_randomized_outcome_contract import (
    validate_native_lineage_trace,
)
from research.families.f09_campaign_action_uplift.audit.multi_short_reducing_buy_aggression import (
    CANDIDATE_ACTION,
    CONTROL_ACTION,
    MultiShortRepairRandomizer,
    aggressive_maker_buy_price,
    treatment_should_end,
    treatment_should_start,
)

ROOT = Path(__file__).resolve().parents[1]
BASE_MS = 1_700_000_000_000


def test_aggressive_maker_buy_uses_frozen_price_formula() -> None:
    quote = aggressive_maker_buy_price(
        baseline_price=100.0,
        best_bid=101.0,
        best_ask=102.0,
        tick_size=0.1,
        mark_price=101.5,
    )
    assert quote.selected_price == pytest.approx(101.0)
    assert quote.improvement_ticks == pytest.approx(10.0)
    assert quote.changed is True
    assert quote.maker_valid is True

    clamped = aggressive_maker_buy_price(
        baseline_price=102.0,
        best_bid=101.0,
        best_ask=102.0,
        tick_size=0.1,
        mark_price=101.5,
    )
    assert clamped.selected_price == pytest.approx(101.9)
    assert clamped.selected_price < 102.0
    assert clamped.maker_valid is True

    btc_scale = aggressive_maker_buy_price(
        baseline_price=65_000.0,
        best_bid=65_001.2,
        best_ask=65_001.3,
        tick_size=0.1,
        mark_price=65_001.25,
    )
    assert btc_scale.selected_price == pytest.approx(65_001.2)
    assert btc_scale.maker_valid is True


def test_trigger_release_and_randomizer_contracts() -> None:
    assert treatment_should_start(-0.002)
    assert not treatment_should_start(-0.001)
    assert treatment_should_end(-0.001)
    assert treatment_should_end(0.0)
    assert not treatment_should_end(-0.002)

    randomizer = MultiShortRepairRandomizer(seed=19, family_id="repair_v1")
    assignments = [
        randomizer.assign(
            utc_day="2026-08-01",
            pre_assignment_campaign_uid=f"campaign-{index}",
        )
        for index in range(2_000)
    ]
    candidate_rate = np.mean(
        [row.action == CANDIDATE_ACTION for row in assignments]
    )
    assert 0.47 < candidate_rate < 0.53
    assert all(row.randomization_stratum == "2026-08-01|SELL" for row in assignments)


def _replay_path() -> tuple[pd.DataFrame, HistoricalBBOData]:
    timestamps = np.asarray(
        [BASE_MS + index * 1_000 for index in range(14)],
        dtype=np.int64,
    )
    prices = np.asarray(
        [
            100.0,
            103.4,
            103.4,
            110.0,
            110.0,
            115.0,
            115.0,
            100.0,
            100.0,
            95.0,
            95.0,
            90.0,
            90.0,
            90.0,
        ],
        dtype=np.float64,
    )
    trades = pd.DataFrame(
        {
            "transact_time": timestamps,
            "price": prices,
            "quantity": np.asarray(
                [
                    0.0,
                    0.001,
                    0.0,
                    0.001,
                    0.0,
                    0.001,
                    0.0,
                    0.001,
                    0.0,
                    0.001,
                    0.0,
                    0.001,
                    0.0,
                    0.0,
                ],
                dtype=np.float64,
            ),
            "is_buyer_maker": np.asarray(
                [False, False, False, False, False, False, False, True, False, True, False, True, False, False],
                dtype=np.uint8,
            ),
        }
    )
    bbo = HistoricalBBOData(
        ts_ms=timestamps,
        best_bid=prices - 0.5,
        best_ask=prices + 0.5,
        bid_qty=np.ones(len(prices), dtype=np.float64),
        ask_qty=np.ones(len(prices), dtype=np.float64),
    )
    return trades, bbo


def _replay_params(seed: int) -> dict[str, object]:
    return {
        "gamma": 0.01,
        "kappa": 1.0,
        "order_size": 0.001,
        "max_inventory": 0.01,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "requote_clock": "fixed",
        "maker_fee": 0.0,
        "taker_fee": 0.0,
        "tick_size": 0.1,
        "lot_size": 0.001,
        "queue_base": 0.0,
        "queue_decay": 0.0,
        "maker_fill_prob": 1.0,
        "use_bar_pricing": False,
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 1_000,
        "max_exec_book_age_s": 10.0,
        "collect_curves": False,
        "position_timeout": 0.0,
        "markout_ema_span_fills": 0,
        "fill_cooldown": 1.0,
        "fill_cooldown_reducing": 0.0,
        "fill_cooldown_apply_reducing": False,
        "fill_cooldown_consecutive_reset_policy": "opposite_fill_only",
        "fill_cooldown_clock_mode": "wall_time",
        "decision_trace_profile": "mechanics_only",
        "trace_decisions_max": 100_000,
        "trace_quotes_max": 100_000,
        "replay_initial_state_mode": "fresh_start",
        "lineage_randomized_outcome_contract_version": "v2",
        "multi_short_reducing_buy_aggression_enabled": True,
        "multi_short_reducing_buy_aggression_seed": int(seed),
        "multi_short_reducing_buy_aggression_family_id": (
            "test_multi_short_reducing_buy_aggression_v1"
        ),
        "multi_short_reducing_buy_aggression_probabilities": {
            CONTROL_ACTION: 0.5,
            CANDIDATE_ACTION: 0.5,
        },
        "trace_multi_short_reducing_buy_aggression_max": 100,
        "dynamic_fill_hazard_action_enabled": False,
        "dynamic_fill_hazard_cpp_parity_enabled": False,
        "dynamic_fill_hazard_mechanics_telemetry_enabled": False,
    }


def _run(seed: int) -> dict[str, object]:
    trades, bbo = _replay_path()
    return bt._simulate_tick_with_engine(
        "python",
        trades,
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        _replay_params(seed),
        bbo_data=bbo,
    )


def test_full_path_changes_only_reducing_buy_until_release() -> None:
    candidate = _run(1)
    control = _run(3)
    candidate_record = candidate[
        "_multi_short_reducing_buy_aggression_trace"
    ][0]
    control_record = control[
        "_multi_short_reducing_buy_aggression_trace"
    ][0]
    assert candidate_record["action"] == CANDIDATE_ACTION
    assert control_record["action"] == CONTROL_ACTION
    assert candidate_record["assignment_inventory_btc"] == pytest.approx(-0.002)
    assert candidate_record["trigger_fill_excluded_from_reward"] == 1
    assert candidate_record["treatment_release_inventory_btc"] == pytest.approx(
        -0.001
    )
    assert (
        candidate_record["lineage_terminal_reason"]
        == "inventory_recovered_to_release_threshold"
    )

    candidate_bids = {
        int(row["submit_ts"]): float(row["price"])
        for row in candidate["_quote_trace"]
        if row["side"] == "BUY"
    }
    control_bids = {
        int(row["submit_ts"]): float(row["price"])
        for row in control["_quote_trace"]
        if row["side"] == "BUY"
    }
    assert candidate_bids[BASE_MS + 3_000] > control_bids[BASE_MS + 3_000]
    assert candidate_bids[BASE_MS + 5_000] > control_bids[BASE_MS + 5_000]
    assert candidate_bids[BASE_MS + 7_000] == pytest.approx(
        control_bids[BASE_MS + 7_000]
    )
    assert candidate_record["actual_final_action_change_count"] > 0
    assert candidate_record["maker_violation_count"] == 0
    assert candidate_record["action_generated_ioc_or_taker_count"] == 0
    assert candidate_record["accounting_identity_error_usdc"] == pytest.approx(0.0)
    assert candidate[
        "_multi_short_reducing_buy_aggression_trace_audit"
    ]["coverage_complete"] is True


def test_native_trace_satisfies_shared_outcome_contract_v2() -> None:
    result = _run(1)
    foundation = json.loads(
        (
            ROOT
            / "research/families/f09_campaign_action_uplift/docs/"
            "lineage_randomized_outcome_contract_v2.json"
        ).read_text(encoding="utf-8")
    )
    validated = validate_native_lineage_trace(
        pd.DataFrame(result["_multi_short_reducing_buy_aggression_trace"]),
        foundation,
        event_journal=pd.DataFrame(
            result["_multi_short_reducing_buy_aggression_event_journal"]
        ),
        producer_audit=result[
            "_multi_short_reducing_buy_aggression_trace_audit"
        ],
    )
    assert len(validated) == 1
    assert validated.loc[0, "side"] == "SELL"
    assert validated.loc[
        0, "decision_to_campaign_terminal_value_usdc"
    ] == pytest.approx(
        validated.loc[0, "lineage_reward_usdc"]
        + validated.loc[0, "post_lineage_continuation_value_usdc"]
    )


def test_action_rejects_q90_drifted_thresholds_and_cpp_full_path() -> None:
    trades, bbo = _replay_path()
    q90 = _replay_params(1)
    q90["dynamic_fill_hazard_mechanics_telemetry_enabled"] = True
    with pytest.raises(ValueError, match="q90 off"):
        bt._simulate_tick_with_engine(
            "python",
            trades,
            np.empty(0, dtype=np.int64),
            np.empty(0, dtype=np.float64),
            q90,
            bbo_data=bbo,
        )

    drifted = _replay_params(1)
    drifted["multi_short_reducing_buy_aggression_trigger_inventory_btc"] = -0.003
    with pytest.raises(ValueError, match="trigger_inventory_btc=-0.002"):
        bt._simulate_tick_with_engine(
            "python",
            trades,
            np.empty(0, dtype=np.int64),
            np.empty(0, dtype=np.float64),
            drifted,
            bbo_data=bbo,
        )

    with pytest.raises(NotImplementedError, match="Python-authoritative"):
        bt._simulate_tick_with_engine(
            "cpp",
            trades,
            np.empty(0, dtype=np.int64),
            np.empty(0, dtype=np.float64),
            _replay_params(1),
            bbo_data=bbo,
        )
