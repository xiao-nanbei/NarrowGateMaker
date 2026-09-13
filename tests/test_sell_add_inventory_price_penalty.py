from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from models import backtest_tick as bt
from research.families.f09_campaign_action_uplift.audit.lineage_randomized_outcome_contract import (
    validate_native_lineage_trace,
)
from research.families.f09_campaign_action_uplift.audit.sell_add_inventory_price_penalty import (
    CANDIDATE_ACTION,
    CONTROL_ACTION,
    SellAddPenaltyRandomizer,
    apply_sell_add_price_penalty,
    sell_add_penalty_bps,
)

ROOT = Path(__file__).resolve().parents[1]
BASE_MS = 1_700_000_000_000


def test_fixed_penalty_curve_uses_short_inventory_units() -> None:
    assert sell_add_penalty_bps(0.0) == pytest.approx(0.0)
    assert sell_add_penalty_bps(-0.001) == pytest.approx(0.5)
    assert sell_add_penalty_bps(-0.002) == pytest.approx(1.0)
    assert sell_add_penalty_bps(-0.003) == pytest.approx(1.5)
    assert sell_add_penalty_bps(-0.010) == pytest.approx(1.5)


def test_price_penalty_rounds_outward_and_preserves_bid_under_pair_cap() -> None:
    unconstrained = apply_sell_add_price_penalty(
        baseline_bid=64_990.0,
        baseline_ask=65_000.0,
        mid=65_000.0,
        inventory_btc=-0.001,
        tick_size=0.1,
        max_pair_spread=100.0,
    )
    assert unconstrained.selected_ask == pytest.approx(65_003.3)
    assert unconstrained.requested_penalty_ticks == pytest.approx(33.0)
    assert unconstrained.cap_truncated is False

    truncated = apply_sell_add_price_penalty(
        baseline_bid=64_990.0,
        baseline_ask=65_000.0,
        mid=65_000.0,
        inventory_btc=-0.001,
        tick_size=0.1,
        max_pair_spread=10.2,
    )
    assert truncated.selected_ask == pytest.approx(65_000.2)
    assert truncated.realized_penalty_ticks == pytest.approx(2.0)
    assert truncated.cap_truncated is True
    assert truncated.fully_truncated is False

    fully_truncated = apply_sell_add_price_penalty(
        baseline_bid=64_990.0,
        baseline_ask=65_000.0,
        mid=65_000.0,
        inventory_btc=-0.001,
        tick_size=0.1,
        max_pair_spread=10.0,
    )
    assert fully_truncated.selected_ask == pytest.approx(65_000.0)
    assert fully_truncated.fully_truncated is True


def test_campaign_randomizer_is_stable_and_exact_half_propensity() -> None:
    randomizer = SellAddPenaltyRandomizer(seed=17, family_id="penalty_v1")
    assignments = [
        randomizer.assign(
            utc_day="2026-08-01",
            pre_assignment_campaign_uid=f"campaign-{index}",
        )
        for index in range(2_000)
    ]
    assert all(row.randomization_stratum == "2026-08-01|SELL" for row in assignments)
    candidate_rate = np.mean(
        [row.action == CANDIDATE_ACTION for row in assignments]
    )
    assert 0.47 < candidate_rate < 0.53
    assert randomizer.assign(
        utc_day="2026-08-01",
        pre_assignment_campaign_uid="campaign-10",
    ) == assignments[10]


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
        "use_bar_pricing": True,
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 100,
        "max_exec_book_age_s": 0.0,
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
        "sell_add_inventory_price_penalty_enabled": True,
        "sell_add_inventory_price_penalty_seed": int(seed),
        "sell_add_inventory_price_penalty_family_id": (
            "test_sell_add_inventory_price_penalty_v1"
        ),
        "sell_add_inventory_price_penalty_probabilities": {
            CONTROL_ACTION: 0.5,
            CANDIDATE_ACTION: 0.5,
        },
        "trace_sell_add_inventory_price_penalty_max": 100,
        "dynamic_fill_hazard_action_enabled": False,
        "dynamic_fill_hazard_cpp_parity_enabled": False,
        "dynamic_fill_hazard_mechanics_telemetry_enabled": False,
    }


def _replay_path() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "transact_time": np.asarray(
                [BASE_MS + index * 1_000 for index in range(12)],
                dtype=np.int64,
            ),
            "price": np.asarray(
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
                    95.0,
                ],
                dtype=np.float64,
            ),
            "quantity": np.asarray(
                [0.0, 0.001, 0.0, 0.001, 0.0, 0.001, 0.0, 0.001, 0.0, 0.001, 0.0, 0.0],
                dtype=np.float64,
            ),
            "is_buyer_maker": np.asarray(
                [False, False, False, False, False, False, False, True, False, True, False, False],
                dtype=np.uint8,
            ),
        }
    )


def _run(seed: int) -> dict[str, object]:
    return bt._simulate_tick_with_engine(
        "python",
        _replay_path(),
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        _replay_params(seed),
    )


def test_full_path_changes_sell_add_but_not_opener_or_reducing_bid() -> None:
    candidate = _run(1)
    control = _run(2)
    candidate_record = candidate["_sell_add_inventory_price_penalty_trace"][0]
    control_record = control["_sell_add_inventory_price_penalty_trace"][0]
    assert candidate_record["action"] == CANDIDATE_ACTION
    assert control_record["action"] == CONTROL_ACTION

    candidate_asks = [
        row for row in candidate["_quote_trace"] if row["side"] == "SELL"
    ]
    control_asks = [row for row in control["_quote_trace"] if row["side"] == "SELL"]
    assert candidate_asks[0]["price"] == pytest.approx(control_asks[0]["price"])
    assert candidate_asks[1]["price"] > control_asks[1]["price"]

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
    common_bid_times = sorted(set(candidate_bids) & set(control_bids))
    assert common_bid_times
    assert all(
        candidate_bids[ts] == pytest.approx(control_bids[ts])
        for ts in common_bid_times
    )
    assert candidate_record["actual_final_action_change_count"] > 0
    assert candidate_record["sell_add_fill_count"] == 1
    assert candidate_record["accounting_identity_error_usdc"] == pytest.approx(0.0)
    assert candidate["_sell_add_inventory_price_penalty_trace_audit"][
        "coverage_complete"
    ] is True


def test_native_trace_satisfies_shared_lineage_outcome_contract_v2() -> None:
    result = _run(1)
    foundation = json.loads(
        (
            ROOT
            / "research/families/f09_campaign_action_uplift/docs/"
            "lineage_randomized_outcome_contract_v2.json"
        ).read_text(encoding="utf-8")
    )
    validated = validate_native_lineage_trace(
        pd.DataFrame(result["_sell_add_inventory_price_penalty_trace"]),
        foundation,
        event_journal=pd.DataFrame(
            result["_sell_add_inventory_price_penalty_event_journal"]
        ),
        producer_audit=result[
            "_sell_add_inventory_price_penalty_trace_audit"
        ],
    )
    assert len(validated) == 1
    assert validated.loc[0, "side"] == "SELL"
    assert validated.loc[
        0, "decision_to_campaign_terminal_value_usdc"
    ] == pytest.approx(validated.loc[0, "lineage_reward_usdc"])


def test_action_rejects_q90_or_non_frozen_curve_and_cpp_full_path() -> None:
    q90 = _replay_params(1)
    q90["dynamic_fill_hazard_mechanics_telemetry_enabled"] = True
    with pytest.raises(ValueError, match="q90 off"):
        bt._simulate_tick_with_engine(
            "python",
            _replay_path(),
            np.empty(0, dtype=np.int64),
            np.empty(0, dtype=np.float64),
            q90,
        )

    drifted = _replay_params(1)
    drifted["sell_add_inventory_price_penalty_step_bps"] = 0.6
    with pytest.raises(ValueError, match="step_bps=0.5"):
        bt._simulate_tick_with_engine(
            "python",
            _replay_path(),
            np.empty(0, dtype=np.int64),
            np.empty(0, dtype=np.float64),
            drifted,
        )

    with pytest.raises(NotImplementedError, match="Python-authoritative"):
        bt._simulate_tick_with_engine(
            "cpp",
            _replay_path(),
            np.empty(0, dtype=np.int64),
            np.empty(0, dtype=np.float64),
            _replay_params(1),
        )
