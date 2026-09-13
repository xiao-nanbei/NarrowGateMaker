import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from models import backtest_tick as bt
from research.families.f04_external_market_alpha.audit.cross_venue_causal_fair_price import (
    HistoricalFairPriceData,
)
from research.families.f09_campaign_action_uplift.audit import (
    lineage_randomized_outcome_contract as lineage_contract,
)
from research.families.f09_campaign_action_uplift.audit.cross_venue_fair_center_shift import (
    CANDIDATE_ACTION,
    CONTROL_ACTION,
    FairCenterRandomizer,
    assignment_lead_direction,
    normalize_probabilities,
    project_action_pair,
)
from strategy.cross_venue_fair_price import CrossVenueFairPriceState

ROOT = Path(__file__).resolve().parents[1]
BASE_MS = 1_700_000_000_000


def _state(shift: float) -> CrossVenueFairPriceState:
    return CrossVenueFairPriceState(
        schema_version="cross_venue_causal_fair_price.v1",
        decision_ts_ns=1,
        valid=True,
        reason="valid",
        local_mid=100.0,
        fair_price=100.0 + shift,
        raw_lead_bps=shift * 100.0,
        gain=1.0,
        center_shift_price=shift,
        center_shift_bps=shift * 100.0,
        confidence=1.0,
        dispersion_bps=0.1,
        valid_venues=3,
        venue_ids=("bitget", "bybit", "okx"),
        minimum_basis_samples=30,
        lead_variance_bps2=1.0,
        noise_variance_bps2=0.5,
        max_source_age_ms=1.0,
        max_feed_latency_ms=1.0,
        max_feature_latency_ms=1.0,
        source_kinds=("receive_time_bbo",),
        transport_supported=True,
    )


def test_randomizer_is_pre_assignment_deterministic_and_exact_half() -> None:
    assert normalize_probabilities(None) == {
        CONTROL_ACTION: 0.5,
        CANDIDATE_ACTION: 0.5,
    }
    randomizer = FairCenterRandomizer(7, "fair-center-v1")
    first = randomizer.assign(
        utc_day="2026-04-20",
        assignment_lead_direction="BUY",
        pre_assignment_campaign_uid="prospective-1",
    )
    second = randomizer.assign(
        utc_day="2026-04-20",
        assignment_lead_direction="BUY",
        pre_assignment_campaign_uid="prospective-1",
    )
    assert first == second
    assert first.action in {CONTROL_ACTION, CANDIDATE_ACTION}
    assert first.randomization_stratum == "2026-04-20|BUY"


def test_direction_uses_ex_ante_fair_center_sign() -> None:
    assert assignment_lead_direction(_state(0.2)) == "BUY"
    assert assignment_lead_direction(_state(-0.2)) == "SELL"
    with pytest.raises(ValueError, match="zero"):
        assignment_lead_direction(_state(0.0))


def test_control_keeps_pair_while_candidate_moves_whole_pair() -> None:
    common = dict(
        state=_state(0.2),
        baseline_bid=99.0,
        baseline_ask=101.0,
        best_bid=99.5,
        best_ask=100.5,
        tick_size=0.1,
    )
    control = project_action_pair(CONTROL_ACTION, **common)
    candidate = project_action_pair(CANDIDATE_ACTION, **common)
    assert (control.candidate_bid, control.candidate_ask) == (99.0, 101.0)
    assert candidate.candidate_bid == pytest.approx(99.2)
    assert candidate.candidate_ask == pytest.approx(101.2)
    assert candidate.candidate_ask - candidate.candidate_bid == pytest.approx(2.0)


def _historical_data() -> HistoricalFairPriceData:
    return HistoricalFairPriceData(
        feature_ready_ts_ns=np.asarray([BASE_MS * 1_000_000], dtype=np.int64),
        fair_price=np.asarray([101.0]),
        gain=np.asarray([0.5]),
        confidence=np.asarray([1.0]),
        dispersion_bps=np.asarray([0.1]),
        valid_venues=np.asarray([3]),
        minimum_basis_samples=np.asarray([30]),
        lead_variance_bps2=np.asarray([1.0]),
        noise_variance_bps2=np.asarray([0.5]),
        max_source_age_ms=np.asarray([0.0]),
        valid=np.asarray([1]),
        reason=np.asarray(["valid"]),
    )


def _replay_params() -> dict[str, object]:
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
        "fill_cooldown": 85.0,
        "fill_cooldown_reducing": 0.0,
        "fill_cooldown_apply_reducing": False,
        "fill_cooldown_consecutive_reset_policy": "opposite_fill_only",
        "fill_cooldown_clock_mode": "wall_time",
        "lineage_randomized_outcome_contract_version": "v2",
        "cross_venue_fair_center_shift_enabled": True,
        "cross_venue_fair_center_shift_seed": 17,
        "cross_venue_fair_center_shift_family_id": "test_fair_center_v1",
        "trace_cross_venue_fair_center_shift_max": 100,
        "cross_venue_fair_center_shift_probabilities": {
            CONTROL_ACTION: 0.5,
            CANDIDATE_ACTION: 0.5,
        },
    }


def _no_fill_trades() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "transact_time": np.asarray(
                [BASE_MS, BASE_MS + 1_000, BASE_MS + 2_000],
                dtype=np.int64,
            ),
            "price": np.asarray([100.0, 100.0, 100.0]),
            "quantity": np.asarray([0.0, 0.0, 0.0]),
            "is_buyer_maker": np.asarray([False, False, False], dtype=np.uint8),
        }
    )


def test_full_path_assignment_precedes_path_and_censors_without_fill() -> None:
    result = bt._simulate_tick_with_engine(
        "python",
        _no_fill_trades(),
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        _replay_params(),
        historical_fair_price_data=_historical_data(),
    )
    trace = pd.DataFrame(result["_cross_venue_fair_center_shift_trace"])
    events = pd.DataFrame(
        result["_cross_venue_fair_center_shift_event_journal"]
    )
    foundation = json.loads(
        (
            ROOT
            / "research/families/f09_campaign_action_uplift/docs/"
            "lineage_randomized_outcome_contract_v2.json"
        ).read_text(encoding="utf-8")
    )

    validated = lineage_contract.validate_native_lineage_trace(
        trace,
        foundation,
        event_journal=events,
        producer_audit=result["_cross_venue_fair_center_shift_trace_audit"],
    )

    assert len(validated) == 1
    assert validated.iloc[0]["assignment_before_downstream_path"] == 1
    assert validated.iloc[0]["campaign_terminal_reason"] == (
        "no_fill_day_end_censored"
    )
    assert validated.iloc[0]["transport_supported"] == 0
    assert result["_cross_venue_fair_center_shift_trace_audit"][
        "coverage_complete"
    ] is True


@pytest.mark.parametrize("forced_action", [CONTROL_ACTION, CANDIDATE_ACTION])
def test_full_path_forced_arm_is_deterministic_diagnostic(
    forced_action: str,
) -> None:
    params = _replay_params()
    params["cross_venue_fair_center_shift_forced_action"] = forced_action
    result = bt._simulate_tick_with_engine(
        "python",
        _no_fill_trades(),
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        params,
        historical_fair_price_data=_historical_data(),
    )
    trace = pd.DataFrame(result["_cross_venue_fair_center_shift_trace"])

    assert trace["action"].tolist() == [forced_action]
    assert trace["behavior_propensity"].tolist() == [1.0]
    assert trace["behavior_prob_local_quote_center"].tolist() == [
        float(forced_action == CONTROL_ACTION)
    ]
    assert trace["behavior_prob_cross_venue_fair_quote_center"].tolist() == [
        float(forced_action == CANDIDATE_ACTION)
    ]


def test_full_path_requires_q90_off() -> None:
    params = _replay_params()
    params["dynamic_fill_hazard_action_enabled"] = True
    with pytest.raises(ValueError, match="q90 off"):
        bt._simulate_tick_with_engine(
            "python",
            _no_fill_trades(),
            np.empty(0, dtype=np.int64),
            np.empty(0, dtype=np.float64),
            params,
            historical_fair_price_data=_historical_data(),
        )


def test_cpp_fair_center_uses_same_tape_and_disabled_control_is_exact_noop() -> None:
    control_params = _replay_params()
    control_params["cross_venue_fair_center_shift_enabled"] = False
    control_params["trace_quotes_max"] = 100
    control_with_tape = bt._simulate_tick_with_engine(
        "cpp",
        _no_fill_trades(),
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        control_params,
        historical_fair_price_data=_historical_data(),
    )
    control_without_tape = bt._simulate_tick_with_engine(
        "cpp",
        _no_fill_trades(),
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        control_params,
    )
    for field in ("pnl", "fills_total", "fills_bid", "fills_ask", "final_inventory"):
        assert control_with_tape[field] == control_without_tape[field]
    assert control_with_tape["cross_venue_fair_center_tape_supplied"] is True
    assert control_with_tape["cross_venue_fair_center_eval_count"] == 0

    candidate_params = _replay_params()
    candidate_params["trace_quotes_max"] = 100
    candidate = bt._simulate_tick_with_engine(
        "cpp",
        _no_fill_trades(),
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        candidate_params,
        historical_fair_price_data=_historical_data(),
    )
    assert candidate["cross_venue_fair_center_shift_enabled"] is True
    assert candidate["cross_venue_fair_center_eval_count"] > 0
    assert candidate["cross_venue_fair_center_valid_count"] > 0
    assert candidate["cross_venue_fair_center_nonzero_request_count"] > 0
    assert candidate["cross_venue_fair_center_price_change_count"] > 0
    control_pair = control_with_tape["_quote_trace"][:2]
    candidate_pair = candidate["_quote_trace"][:2]
    assert [row["side"] for row in control_pair] == ["BUY", "SELL"]
    assert [row["side"] for row in candidate_pair] == ["BUY", "SELL"]
    for control_row, candidate_row in zip(control_pair, candidate_pair, strict=True):
        assert candidate_row["price"] - control_row["price"] == pytest.approx(0.5)
    assert (
        candidate_pair[1]["price"] - candidate_pair[0]["price"]
    ) == pytest.approx(control_pair[1]["price"] - control_pair[0]["price"])
