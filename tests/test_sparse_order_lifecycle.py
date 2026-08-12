from __future__ import annotations

import math
from pathlib import Path

import pandas as pd
import pytest

from research.families.f06_placement_fill_cif.audit.paired_order_lifecycle import ACTION_ORDER, PlacementCohort
from research.families.f06_placement_fill_cif.audit.sparse_order_lifecycle import (
    build_watch_manifest,
    simulate_sparse_paired_placements,
)
from models.exchange_book_replay import ExchangeBookLevelChange, ExchangeBookLookup


def _cohort() -> PlacementCohort:
    return PlacementCohort.create(
        cohort_id="2026-04-13:1",
        decision_id="decision-1",
        day="2026-04-13",
        side="BUY",
        inventory_role="opener",
        campaign_id=1,
        submit_ts_ns=500_000_000,
        activate_ts_ns=1_000_000_000,
        cancel_request_ts_ns=2_500_000_000,
        cancel_ack_ts_ns=3_000_000_000,
        observation_end_ts_ns=5_000_000_000,
        baseline_price_tick=100,
        quantity=0.002,
        queue_deplete_mult=1.0,
        lot_size=0.001,
        decision_features={"feature_ready_ts_ns": 500_000_000},
    )


def _seed_frame(cohort: PlacementCohort) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "watch_id": f"{cohort.cohort_id}:{action}",
                "seed_status": "exact",
                "seed_reason": "visible_quantity",
                "seed_qty": 0.0,
                "seed_asof_ts_ms": 999,
                "segment_id": 1,
                "seed_best_bid_tick": 101,
                "seed_best_ask_tick": 102,
                "ambiguous": False,
            }
            for action in ACTION_ORDER
        ]
    )


def _empty_events() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "watch_id",
            "exchange_ts_ms",
            "message_ordinal",
            "qty_after",
            "event_code",
            "state_status",
            "ambiguous",
        ]
    )


def _run_python_reference(cohort: PlacementCohort) -> pd.DataFrame:
    for child in cohort.children.values():
        child.activate(
            lookup=ExchangeBookLookup(
                side="bid",
                price_tick=child.price_tick,
                status="exact",
                reason="visible_quantity",
                quantity=0.0,
                asof_exchange_ts_ns=999_000_000,
                segment_id=1,
                snapshot_min_tick=90,
                snapshot_max_tick=110,
            ),
            best_bid_tick=101,
            best_ask_tick=102,
            same_boundary_native_event=False,
        )
        child.apply_trade(
            ts_ns=2_000_000_000,
            trade_price_tick=100,
            trade_qty=0.001,
            is_buyer_maker=True,
        )
        child.request_cancel(2_500_000_000)
        child.apply_trade(
            ts_ns=2_600_000_000,
            trade_price_tick=100,
            trade_qty=0.001,
            is_buyer_maker=True,
        )
        child.acknowledge_cancel(3_000_000_000)
        child.censor(5_000_000_000)
    return pd.DataFrame([cohort.as_wide_record()])


def _assert_same_value(left: object, right: object) -> None:
    if isinstance(left, float) and math.isnan(left):
        assert isinstance(right, float) and math.isnan(right)
    elif isinstance(left, float):
        assert float(right) == pytest.approx(left)
    else:
        assert right == left


def test_sparse_cpp_lifecycle_matches_python_authority(tmp_path: Path) -> None:
    pytest.importorskip("narrowgate_cpp")
    native_cohort = _cohort()
    python_cohort = _cohort()
    tape = tmp_path / "tape"
    tape.mkdir()
    _seed_frame(native_cohort).to_parquet(tape / "seeds.parquet", index=False)
    _empty_events().to_parquet(tape / "level_events.parquet", index=False)
    trades = pd.DataFrame(
        {
            "transact_time": [2_000, 2_600],
            "trade_id": [1, 2],
            "price": [10.0, 10.0],
            "quantity": [0.001, 0.001],
            "is_buyer_maker": [True, True],
        }
    )

    native, summary = simulate_sparse_paired_placements(
        [native_cohort],
        tape_dir=tape,
        trades=trades,
        tick_size=0.1,
        lot_size=0.001,
        queue_deplete_mult=1.0,
    )
    expected = _run_python_reference(python_cohort)

    assert summary["monotonicity_violations"] == 0
    for column in expected.columns:
        _assert_same_value(expected.iloc[0][column], native.iloc[0][column])


def test_watch_manifest_ends_native_watch_at_cancel_ack() -> None:
    frame = build_watch_manifest([_cohort()], tick_size=0.1)

    assert len(frame) == 3
    assert set(frame["side"]) == {"BUY"}
    assert set(frame["stop_ts_ms"]) == {3_001}


def test_sparse_level_path_matches_python_queue_accounting(tmp_path: Path) -> None:
    pytest.importorskip("narrowgate_cpp")
    native_cohort = _cohort()
    python_cohort = _cohort()
    tape = tmp_path / "tape"
    tape.mkdir()
    seeds = _seed_frame(native_cohort)
    seeds["seed_qty"] = 2.0
    seeds.to_parquet(tape / "seeds.parquet", index=False)
    current_watch = f"{native_cohort.cohort_id}:current"
    events = pd.DataFrame(
        [
            {
                "watch_id": current_watch,
                "exchange_ts_ms": 1_500,
                "message_ordinal": 1,
                "qty_after": 1.0,
                "event_code": "update",
                "state_status": "exact",
                "ambiguous": False,
            },
            {
                "watch_id": current_watch,
                "exchange_ts_ms": 2_550,
                "message_ordinal": 2,
                "qty_after": 2.0,
                "event_code": "update",
                "state_status": "exact",
                "ambiguous": False,
            },
        ]
    )
    events.to_parquet(tape / "level_events.parquet", index=False)
    trades = pd.DataFrame(
        {
            "transact_time": [1_600, 2_600],
            "trade_id": [1, 2],
            "price": [10.0, 10.0],
            "quantity": [1.0, 0.001],
            "is_buyer_maker": [True, True],
        }
    )

    for child in python_cohort.children.values():
        child.activate(
            lookup=ExchangeBookLookup(
                side="bid",
                price_tick=child.price_tick,
                status="exact",
                reason="visible_quantity",
                quantity=2.0,
                asof_exchange_ts_ns=999_000_000,
                segment_id=1,
                snapshot_min_tick=90,
                snapshot_max_tick=110,
            ),
            best_bid_tick=101,
            best_ask_tick=102,
            same_boundary_native_event=False,
        )
    current = python_cohort.children["current"]
    current.apply_level_change(
        ExchangeBookLevelChange(
            exchange_ts_ns=1_500_000_000,
            receive_ts_ns=1_500_000_000,
            side="bid",
            price_tick=100,
            quantity_before=2.0,
            quantity_after=1.0,
            event_type="update",
            segment_id=1,
            update_id=1,
        ),
        ambiguous_with_trade_or_activation=False,
    )
    for child in python_cohort.children.values():
        child.apply_trade(
            ts_ns=1_600_000_000,
            trade_price_tick=100,
            trade_qty=1.0,
            is_buyer_maker=True,
        )
        child.request_cancel(2_500_000_000)
    current.apply_level_change(
        ExchangeBookLevelChange(
            exchange_ts_ns=2_550_000_000,
            receive_ts_ns=2_550_000_000,
            side="bid",
            price_tick=100,
            quantity_before=1.0,
            quantity_after=2.0,
            event_type="update",
            segment_id=1,
            update_id=2,
        ),
        ambiguous_with_trade_or_activation=False,
    )
    for child in python_cohort.children.values():
        child.apply_trade(
            ts_ns=2_600_000_000,
            trade_price_tick=100,
            trade_qty=0.001,
            is_buyer_maker=True,
        )
        child.acknowledge_cancel(3_000_000_000)
        child.censor(5_000_000_000)
    expected = pd.DataFrame([python_cohort.as_wide_record()])

    native, _ = simulate_sparse_paired_placements(
        [native_cohort],
        tape_dir=tape,
        trades=trades,
        tick_size=0.1,
        lot_size=0.001,
        queue_deplete_mult=1.0,
    )

    for column in expected.columns:
        _assert_same_value(expected.iloc[0][column], native.iloc[0][column])
