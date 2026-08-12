from __future__ import annotations

import threading
import time
from dataclasses import asdict
from pathlib import Path

import pandas as pd
import pytest

from execution.exact_opportunity_tape import (
    ExactQuoteOpportunityTapeRow,
    empty_exact_opportunity_row,
    exact_quote_role,
)
from live.config import Config
from research.families.f04_external_market_alpha.audit.exact_opener_opportunity_tape import (
    validate_exact_opportunity_tape,
)
from strategy.maker_engine import MakerEngine
from strategy.order_manager import OrderManager, Side


def _row(event_type: str, event_ts_ns: int, side: str, **updates):
    payload = empty_exact_opportunity_row(
        event_type=event_type,
        event_ts_ns=event_ts_ns,
        symbol="BTCUSDC",
        side=side,
    )
    payload.update(updates)
    return asdict(ExactQuoteOpportunityTapeRow(**payload))


def _valid_tape() -> pd.DataFrame:
    common = {
        "decision_group_id": "g1",
        "decision_start_ts_ns": 100,
        "feature_ready_ts_ns": 120,
        "signed_inventory_before": 0.0,
        "role": "opener",
        "exposure_increasing": 1,
        "baseline_eligible": 1,
        "guard_valid": 1,
        "guard_reason": "valid",
        "guard_adverse_side": "BUY",
    }
    buy = _row(
        "decision",
        140,
        "BUY",
        **common,
        decision_id="g1:BUY",
        origin_decision_id="g1:BUY",
        baseline_quote_price=99.9,
        candidate_quote_price=99.8,
        requested_outward_ticks=1,
        effective_outward_ticks=1,
        client_order_id="cid-buy",
        final_executed_action="place",
        order_quantity=0.001,
    )
    sell = _row(
        "decision",
        141,
        "SELL",
        **common,
        decision_id="g1:SELL",
        origin_decision_id="g1:SELL",
        baseline_quote_price=100.1,
        candidate_quote_price=100.1,
        final_executed_action="keep",
        client_order_id="cid-sell",
        order_quantity=0.001,
    )
    submit = _row(
        "submit",
        130,
        "BUY",
        **common,
        decision_id="g1:BUY",
        origin_decision_id="g1:BUY",
        baseline_quote_price=99.9,
        candidate_quote_price=99.8,
        requested_outward_ticks=1,
        effective_outward_ticks=1,
        client_order_id="cid-buy",
        final_executed_action="place",
        lifecycle_sequence=1,
        order_state="PENDING_NEW",
        order_quantity=0.001,
        remaining_quantity=0.001,
    )
    return pd.DataFrame([submit, buy, sell])


def test_exact_role_uses_signed_inventory() -> None:
    assert exact_quote_role("BUY", 0.0) == "opener"
    assert exact_quote_role("SELL", 0.0) == "opener"
    assert exact_quote_role("BUY", 0.001) == "add"
    assert exact_quote_role("SELL", -0.001) == "add"
    assert exact_quote_role("BUY", -0.001) == "reducing"
    assert exact_quote_role("SELL", 0.001) == "reducing"


def test_exact_tape_validates_native_opener_denominator() -> None:
    report = validate_exact_opportunity_tape(_valid_tape())

    assert report["economic_outcomes_read"] is False
    assert report["external_outcome_tables_read"] is False
    assert report["operational_lifecycle_outcomes_read"] is True
    assert report["decision_groups"] == 1
    assert report["exact_eligible_opener_opportunities"] == 2
    assert report["candidate_quote_changes"] == 1
    assert report["candidate_rate"] == pytest.approx(0.5)
    assert report["pooled_candidate_rate_supported_diagnostic"] is True
    assert report["candidate_rate_gate_scope"] == (
        "BUY_and_SELL_each_independently"
    )
    assert report["candidate_rate_supported"] is False
    assert report["action_experiment_authorized"] is False


def test_exact_tape_requires_each_side_to_pass_candidate_rate() -> None:
    frame = _valid_tape()
    sell = (frame["event_type"] == "decision") & (frame["side"] == "SELL")
    frame.loc[sell, "candidate_quote_price"] = 100.2
    frame.loc[sell, "requested_outward_ticks"] = 1
    frame.loc[sell, "effective_outward_ticks"] = 1
    frame.loc[sell, "guard_adverse_side"] = "SELL"

    report = validate_exact_opportunity_tape(frame)

    assert report["candidate_rate_supported"] is True
    assert all(
        summary["candidate_rate_supported"]
        for summary in report["side_summaries"]
    )


def test_exact_tape_rejects_future_feature_clock() -> None:
    frame = _valid_tape()
    frame.loc[frame["event_type"] == "decision", "feature_ready_ts_ns"] = 150
    with pytest.raises(ValueError, match="after its journaled action time"):
        validate_exact_opportunity_tape(frame)


def test_exact_tape_rejects_unsigned_role_inference() -> None:
    frame = _valid_tape()
    frame.loc[
        (frame["event_type"] == "decision") & (frame["side"] == "SELL"),
        "role",
    ] = "add"
    with pytest.raises(ValueError, match="signed decision-visible inventory"):
        validate_exact_opportunity_tape(frame)


def test_exact_tape_rejects_missing_submit_lineage() -> None:
    frame = _valid_tape().loc[lambda value: value["event_type"] != "submit"]
    with pytest.raises(ValueError, match="lack native submit events"):
        validate_exact_opportunity_tape(frame)


def test_exact_tape_rejects_every_non_schema_column() -> None:
    frame = _valid_tape()
    frame["opaque_research_field"] = 0.0
    with pytest.raises(ValueError, match="unexpected columns"):
        validate_exact_opportunity_tape(frame)


def test_exact_tape_allows_only_internal_input_path_beyond_schema() -> None:
    frame = _valid_tape()
    frame["input_path"] = "/tmp/exact.csv"

    report = validate_exact_opportunity_tape(frame)

    assert report["decision_groups"] == 1


def test_exact_tape_rejects_baseline_eligible_non_quote_action() -> None:
    frame = _valid_tape()
    sell = (frame["event_type"] == "decision") & (frame["side"] == "SELL")
    frame.loc[sell, "final_executed_action"] = "pause"

    with pytest.raises(ValueError, match="final place, replace, or keep"):
        validate_exact_opportunity_tape(frame)


@pytest.mark.parametrize(
    ("column", "value", "message"),
    [
        ("guard_valid", 0, "guard_valid=1"),
        ("guard_adverse_side", "SELL", "match side"),
        ("requested_outward_ticks", 0, "exceed requested ticks"),
    ],
)
def test_exact_tape_rejects_candidate_change_without_guard_contract(
    column: str,
    value: object,
    message: str,
) -> None:
    frame = _valid_tape()
    buy = (frame["event_type"] == "decision") & (frame["side"] == "BUY")
    frame.loc[buy, column] = value

    with pytest.raises(ValueError, match=message):
        validate_exact_opportunity_tape(frame)


@pytest.mark.parametrize(
    ("selector", "column"),
    [
        (lambda frame: frame["event_type"] == "decision", "decision_id"),
        (lambda frame: frame["event_type"] == "submit", "client_order_id"),
    ],
)
def test_exact_tape_rejects_null_or_nan_identifiers(selector, column: str) -> None:
    frame = _valid_tape()
    frame.loc[selector(frame), column] = float("nan")

    with pytest.raises(ValueError, match="stable"):
        validate_exact_opportunity_tape(frame)


def test_exact_tape_rejects_duplicate_lifecycle_sequence() -> None:
    frame = _valid_tape()
    submit = frame.loc[frame["event_type"] == "submit"].iloc[0].copy()
    submit["event_type"] = "rest_ack"
    submit["event_ts_ns"] = 135
    submit["visibility_ts_ns"] = 135
    frame = pd.concat([frame, submit.to_frame().T], ignore_index=True)

    with pytest.raises(ValueError, match="unique and strictly increasing"):
        validate_exact_opportunity_tape(frame)


def test_exact_tape_rejects_multiple_terminal_outcomes_per_order() -> None:
    frame = _valid_tape()
    submit = frame.loc[frame["event_type"] == "submit"].iloc[0].copy()
    terminal_rows = []
    for sequence, event_type in ((2, "full_fill"), (3, "cancel_ack")):
        row = submit.copy()
        row["event_type"] = event_type
        row["event_ts_ns"] = 140 + sequence
        row["visibility_ts_ns"] = 140 + sequence
        row["lifecycle_sequence"] = sequence
        terminal_rows.append(row)
    frame = pd.concat(
        [frame, pd.DataFrame(terminal_rows)],
        ignore_index=True,
    )

    with pytest.raises(ValueError, match="multiple terminal outcomes"):
        validate_exact_opportunity_tape(frame)


def test_order_manager_emits_native_lifecycle_callbacks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    timestamps = iter([1_000_000_000, 2_000_000_000, 4_000_000_000])
    monkeypatch.setattr(
        "strategy.order_manager.time.time_ns",
        lambda: next(timestamps),
    )
    seen: list[tuple[str, int]] = []
    manager = OrderManager(
        on_lifecycle_event=lambda order, event_type, _event: seen.append(
            (event_type, len(order.lifecycle.events()))
        )
    )
    cid = manager.create_order("BTCUSDC", Side.BUY, 100.0, 0.001)
    manager.confirm_new(cid, 42)
    manager.on_order_update(
        {
            "c": cid,
            "X": "PARTIALLY_FILLED",
            "i": 42,
            "z": "0.0004",
            "L": "100.0",
            "ap": "100.0",
            "T": 3_500,
            "_local_receive_ts_ns": 3_600_000_000,
        }
    )
    manager.mark_pending_cancel(cid)
    manager.on_order_update(
        {
            "c": cid,
            "X": "CANCELED",
            "i": 42,
            "T": 4_500,
            "_local_receive_ts_ns": 5_000_000_000,
        }
    )

    assert [event for event, _ in seen] == [
        "rest_ack",
        "partial_fill",
        "cancel_ack",
    ]
    assert [sequence for _, sequence in seen] == [2, 3, 5]


class _Rest:
    def new_order(self, **_kwargs):
        return {"orderId": 42, "status": "NEW"}

    def cancel_order(self, **_kwargs):
        return {}


def test_live_producer_links_submit_and_cancel_to_origin_decision(
    tmp_path: Path,
) -> None:
    engine = object.__new__(MakerEngine)
    engine.cfg = Config()
    engine.cfg.symbol = "BTCUSDC"
    engine.rest = _Rest()
    engine._qty_precision = 3
    engine._price_precision = 1
    engine._order_ref_lock = threading.RLock()
    engine._order_context_lock = threading.RLock()
    engine._order_policy_context = {}
    engine._csv_log_lock = threading.Lock()
    engine._bid_cid = None
    engine._ask_cid = None
    engine._exact_opportunity_tape_path = str(tmp_path / "exact.csv")
    engine._log_order_outcome = lambda *_args, **_kwargs: None
    engine._record_perf_rest_latency = lambda *_args, **_kwargs: None
    engine._init_csv_log(
        engine._exact_opportunity_tape_path,
        list(ExactQuoteOpportunityTapeRow.__dataclass_fields__),
    )
    engine.orders = OrderManager(
        on_lifecycle_event=engine._on_order_lifecycle_event,
    )
    context = {
        "exact_decision_group_id": "g-live",
        "exact_decision_id": "g-live:BUY",
        "exact_decision_start_ts_ns": 1,
        "exact_feature_ready_ts_ns": 2,
        "exact_role": "opener",
        "exact_signed_inventory_before": 0.0,
        "exact_exposure_increasing": True,
        "exact_baseline_eligible": True,
        "exact_baseline_quote_price": 99.9,
        "exact_candidate_quote_price": 99.8,
        "exact_guard_valid": True,
        "exact_guard_reason": "valid",
        "exact_guard_adverse_side": "BUY",
        "exact_requested_outward_ticks": 1,
        "exact_effective_outward_ticks": 1,
        "exact_replaced_client_order_id": "",
        "exact_final_executed_action": "place",
        "exact_queue_reset": False,
    }
    cid = engine._place_order(
        "BTCUSDC",
        Side.BUY,
        99.9,
        0.001,
        decision_context=context,
        record_requote_perf=False,
    )
    assert cid
    assert engine._cancel_order(
        cid,
        record_requote_perf=False,
        trigger_decision_id="g-next:BUY",
    )
    exchange_ts_ns = time.time_ns()
    engine.orders.on_order_update(
        {
            "c": cid,
            "X": "CANCELED",
            "i": 42,
            "T": exchange_ts_ns // 1_000_000,
            "_local_receive_ts_ns": exchange_ts_ns + 1_000_000,
        }
    )

    tape = pd.read_csv(engine._exact_opportunity_tape_path)
    assert tape["event_type"].tolist() == [
        "submit",
        "rest_ack",
        "cancel_request",
        "cancel_ack",
    ]
    assert tape["origin_decision_id"].eq("g-live:BUY").all()
    cancel_request = tape.loc[tape["event_type"] == "cancel_request"].iloc[0]
    assert cancel_request["trigger_decision_id"] == "g-next:BUY"
