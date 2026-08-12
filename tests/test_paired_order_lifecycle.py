from __future__ import annotations

import pandas as pd

from research.families.f06_placement_fill_cif.audit.paired_order_lifecycle import (
    ACTION_ORDER,
    PlacementCohort,
    action_price_tick,
)
from research.families.f06_placement_fill_cif.audit.paired_order_lifecycle_smoke import (
    build_cohorts,
    simulate_paired_placements,
)
from models.exchange_book_replay import ExchangeBookLookup
from models.tick_data_types import HistoricalExchangeBookEvent


def _lookup(*, side: str, tick: int, quantity: float, asof: int = 1) -> ExchangeBookLookup:
    return ExchangeBookLookup(
        side=side,
        price_tick=tick,
        status="exact" if quantity > 0.0 else "known_zero",
        reason="test",
        quantity=quantity,
        asof_exchange_ts_ns=asof,
        segment_id=1,
        snapshot_min_tick=tick - 10,
        snapshot_max_tick=tick + 10,
    )


def _cohort(side: str = "BUY") -> PlacementCohort:
    baseline_tick = 100 if side == "BUY" else 101
    return PlacementCohort.create(
        cohort_id="c1",
        decision_id="d1",
        day="2026-05-13",
        side=side,
        inventory_role="opener",
        campaign_id=0,
        submit_ts_ns=1_000_000,
        activate_ts_ns=2_000_000,
        cancel_request_ts_ns=3_000_000,
        cancel_ack_ts_ns=4_000_000,
        observation_end_ts_ns=12_000_000,
        baseline_price_tick=baseline_tick,
        quantity=0.001,
        queue_deplete_mult=1.0,
        lot_size=0.001,
        decision_features={"feature_ready_ts_ns": 1_000_000},
    )


def test_action_prices_are_distance_not_signed_price_deltas() -> None:
    assert [action_price_tick("BUY", 100, action) for action in ACTION_ORDER] == [
        101,
        100,
        99,
    ]
    assert [action_price_tick("SELL", 101, action) for action in ACTION_ORDER] == [
        100,
        101,
        102,
    ]


def test_rejected_placement_has_requested_but_no_effective_price() -> None:
    child = _cohort("BUY").children["current"]
    child.activate(
        lookup=_lookup(side="bid", tick=100, quantity=1.0),
        best_bid_tick=99,
        best_ask_tick=100,
        same_boundary_native_event=False,
    )

    record = child.as_record()
    assert record["requested_price_tick"] == 100
    assert record["effective_price_tick"] is None
    assert record["activation_ts_ns"] == child.activate_ts_ns
    assert record["activation_status"] == "gtx_reject"


def test_strict_through_fills_all_better_prices_and_preserves_monotonicity() -> None:
    cohort = _cohort("BUY")
    for child in cohort.children.values():
        child.activate(
            lookup=_lookup(
                side="bid",
                tick=child.price_tick,
                quantity=5.0,
            ),
            best_bid_tick=102,
            best_ask_tick=103,
            same_boundary_native_event=False,
        )

    for child in cohort.children.values():
        child.apply_trade(
            ts_ns=2_500_000,
            trade_price_tick=99,
            trade_qty=0.001,
            is_buyer_maker=True,
        )

    assert cohort.children["closer_1tick"].fully_filled
    assert cohort.children["current"].fully_filled
    assert not cohort.children["farther_1tick"].filled
    row = cohort.as_wide_record()
    assert row["monotonicity_violation_count"] == 0
    assert row["closer_1tick__first_fill_mechanism"] == "strict_through"


def test_exact_fill_can_arrive_before_cancel_ack() -> None:
    cohort = _cohort("SELL")
    child = cohort.children["current"]
    child.activate(
        lookup=_lookup(side="ask", tick=101, quantity=0.0),
        best_bid_tick=99,
        best_ask_tick=100,
        same_boundary_native_event=False,
    )
    child.request_cancel(3_000_000)
    child.apply_trade(
        ts_ns=3_500_000,
        trade_price_tick=101,
        trade_qty=0.001,
        is_buyer_maker=False,
    )
    child.acknowledge_cancel(4_000_000)

    assert child.fully_filled
    assert child.state == "filled"
    assert child.fill_while_cancel_pending_qty == 0.001
    assert child.first_pending_cancel_fill_ts_ns == 3_500_000
    assert child.terminal_reason == "exact_queue"


def test_cancel_request_freezes_pre_request_order_and_queue_state() -> None:
    child = _cohort("BUY").children["current"]
    child.activate(
        lookup=_lookup(side="bid", tick=100, quantity=2.0),
        best_bid_tick=101,
        best_ask_tick=102,
        same_boundary_native_event=False,
    )
    child.native_cancel_count = 2
    child.native_cancel_qty = 0.4
    child.native_refill_count = 3
    child.native_refill_qty = 0.7
    child.native_level_event_count = 9

    child.request_cancel(3_000_000)
    child.queue_left = 0.0
    child.native_cancel_count = 99

    record = child.as_record()
    assert record["request_state_observed"] == 1
    assert record["request_order_state_before"] == "open"
    assert record["request_order_age_ms"] == 1.0
    assert record["request_remaining_qty"] == 0.001
    assert record["request_queue_left"] == 2.0
    assert record["request_queue_path_valid"] == 1
    assert record["request_native_cancel_count"] == 2
    assert record["request_native_refill_count"] == 3
    assert record["request_native_level_event_count"] == 9


def test_placement_and_active_horizons_use_different_risk_origins() -> None:
    cohort = PlacementCohort.create(
        cohort_id="delayed",
        decision_id="delayed",
        day="2026-05-13",
        side="BUY",
        inventory_role="opener",
        campaign_id=0,
        submit_ts_ns=1_000_000_000,
        activate_ts_ns=3_000_000_000,
        cancel_request_ts_ns=5_000_000_000,
        cancel_ack_ts_ns=6_000_000_000,
        observation_end_ts_ns=15_000_000_000,
        baseline_price_tick=100,
        quantity=0.001,
        queue_deplete_mult=1.0,
        lot_size=0.001,
        decision_features={"feature_ready_ts_ns": 1_000_000_000},
    )
    child = cohort.children["current"]
    child.activate(
        lookup=_lookup(side="bid", tick=100, quantity=0.0),
        best_bid_tick=101,
        best_ask_tick=102,
        same_boundary_native_event=False,
    )
    child.apply_trade(
        ts_ns=3_500_000_000,
        trade_price_tick=100,
        trade_qty=0.001,
        is_buyer_maker=True,
    )

    record = child.as_record()
    assert record["placement_observed_1000ms"] == 1
    assert record["placement_filled_1000ms"] == 0
    assert record["active_observed_1000ms"] == 1
    assert record["active_filled_1000ms"] == 1


def test_unknown_exact_queue_is_not_fabricated_but_through_remains_identified() -> None:
    cohort = _cohort("BUY")
    child = cohort.children["current"]
    unknown = ExchangeBookLookup(
        side="bid",
        price_tick=100,
        status="unknown",
        reason="outside_snapshot_range",
        quantity=None,
        asof_exchange_ts_ns=1,
        segment_id=1,
        snapshot_min_tick=101,
        snapshot_max_tick=110,
    )
    child.activate(
        lookup=unknown,
        best_bid_tick=101,
        best_ask_tick=102,
        same_boundary_native_event=False,
    )
    child.apply_trade(
        ts_ns=2_500_000,
        trade_price_tick=100,
        trade_qty=10.0,
        is_buyer_maker=True,
    )
    assert child.exact_touch_ts_ns == 2_500_000
    assert not child.filled
    child.apply_trade(
        ts_ns=2_600_000,
        trade_price_tick=99,
        trade_qty=0.001,
        is_buyer_maker=True,
    )
    assert child.fully_filled
    assert child.first_fill_mechanism == "strict_through"


class _SyntheticTape:
    day_start_ns = 1

    def __init__(self, events: list[HistoricalExchangeBookEvent]) -> None:
        self.events = events

    def __iter__(self):
        return iter(self.events)


def test_single_cohort_native_smoke_emits_wide_monotone_outcome() -> None:
    cohort = _cohort("BUY")
    cohort.observation_end_ts_ns = 6_000_000
    for child in cohort.children.values():
        child.observation_end_ts_ns = 6_000_000
    snapshot = HistoricalExchangeBookEvent(
        market_id="binance_futures:perpetual:BTCUSDC",
        event_type="snapshot",
        exchange_ts_ns=1_000_000,
        exchange_ts_source="transaction",
        last_update_id=1,
        levels=(
            ("bid", 101, 0.001),
            ("bid", 100, 0.001),
            ("bid", 99, 0.001),
            ("ask", 102, 1.0),
            ("ask", 103, 1.0),
        ),
    )
    trades = pd.DataFrame(
        {
            "trade_id": [1],
            "transact_time": [4],
            "price": [9.9],
            "quantity": [0.002],
            "is_buyer_maker": [True],
        }
    )
    rows, summary = simulate_paired_placements(
        [cohort],
        tape=_SyntheticTape([snapshot]),
        trades=trades,
        tick_size=0.1,
    )
    assert len(rows) == 1
    assert summary["monotonicity_violations"] == 0
    assert rows.loc[0, "closer_1tick__placement_filled_1000ms"] == 1
    assert rows.loc[0, "current__placement_filled_1000ms"] == 1
    assert rows.loc[0, "farther_1tick__placement_filled_1000ms"] == 1


def test_build_cohorts_requires_matching_causal_placement_decision() -> None:
    decisions = pd.DataFrame(
        [
            {
                "decision_id": "BTCUSDC:1:BUY",
                "decision_ts_ns": 1_000_000,
                "side": "BUY",
                "action": "place",
                "depth_age_s": 0.0,
            }
        ]
    )
    lifecycle = pd.DataFrame(
        [
            {
                "order_id": 7,
                "side": "BUY",
                "event_type": "submit",
                "event_ts_ns": 1_000_000,
                "event_seq": 1,
                "order_price": 10.0,
                "order_qty": 0.001,
                "inventory_role": "opener",
                "campaign_id": 0,
            },
            {
                "order_id": 7,
                "side": "BUY",
                "event_type": "activate",
                "event_ts_ns": 2_000_000,
                "event_seq": 2,
                "order_price": 10.0,
                "order_qty": 0.001,
                "inventory_role": "opener",
                "campaign_id": 0,
            },
            {
                "order_id": 7,
                "side": "BUY",
                "event_type": "cancel_request",
                "event_ts_ns": 3_000_000,
                "event_seq": 3,
                "event_reason": "requote_replace",
                "order_price": 10.0,
                "order_qty": 0.001,
                "inventory_role": "opener",
                "campaign_id": 0,
            },
        ]
    )
    quotes = pd.DataFrame(
        [
            {
                "order_id": 7,
                "outcome_ts": 5,
                "activate_ts": 2,
                "queue_deplete_mult": 1.0,
            }
        ]
    )
    cohorts, audit = build_cohorts(
        decisions,
        lifecycle,
        quotes,
        day="2026-05-13",
        tick_size=0.1,
        lot_size=0.001,
        max_cohorts=10,
        max_horizon_ms=10_000,
    )
    assert audit["cohorts"] == 1
    assert cohorts[0].baseline_price_tick == 100
    assert cohorts[0].cancel_request_reason == "requote_replace"
    assert cohorts[0].decision_features["feature_ready_ts_ns"] == 1_000_000


def test_build_cohorts_excludes_circuit_breaker_close_orders() -> None:
    lifecycle = pd.DataFrame(
        [
            {
                "order_id": 9,
                "side": "SELL",
                "event_type": "submit",
                "event_ts_ns": 1_000_000,
                "event_seq": 1,
                "order_price": 10.1,
                "order_qty": 0.001,
                "inventory_role": "reducing",
                "campaign_id": 1,
                "circuit_breaker_close": 1,
            },
            {
                "order_id": 9,
                "side": "SELL",
                "event_type": "activate",
                "event_ts_ns": 2_000_000,
                "event_seq": 2,
                "order_price": 10.1,
                "order_qty": 0.001,
                "inventory_role": "reducing",
                "campaign_id": 1,
            },
        ]
    )
    quotes = pd.DataFrame(
        [
            {
                "order_id": 9,
                "outcome_ts": 5,
                "activate_ts": 2,
                "queue_deplete_mult": 1.0,
                "circuit_breaker_close": False,
            }
        ]
    )
    cohorts, audit = build_cohorts(
        pd.DataFrame(),
        lifecycle,
        quotes,
        day="2026-05-13",
        tick_size=0.1,
        lot_size=0.001,
        max_cohorts=10,
        max_horizon_ms=10_000,
    )
    assert cohorts == []
    assert audit["excluded_circuit_breaker_close"] == 1
    assert audit.get("missing_decision", 0) == 0


def test_cancel_request_reason_is_frozen_in_wide_record() -> None:
    cohort = _cohort()
    cohort.cancel_request_reason = "requote_replace"
    row = cohort.as_wide_record()
    assert row["schema_version"] == "paired_order_lifecycle_smoke.v2"
    assert row["cancel_request_reason"] == "requote_replace"
