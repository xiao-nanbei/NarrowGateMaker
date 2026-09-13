from __future__ import annotations

import csv
import threading
from types import SimpleNamespace

import pytest

from execution.order_lifecycle import QuantityWeightedOrderLifecycle
from execution.order_lifecycle_journal import (
    ORDER_LIFECYCLE_JOURNAL_SCHEMA_VERSION,
    OrderLifecycleJournalRow,
    audit_order_lifecycle_journal,
    order_lifecycle_journal_payload,
)
from strategy.maker_engine import MakerEngine
from strategy.order_manager import OrderManager, Side


def _row(
    lifecycle: QuantityWeightedOrderLifecycle,
    source_event_type: str,
) -> dict[str, object]:
    return order_lifecycle_journal_payload(
        lifecycle=lifecycle,
        runtime_source="replay",
        source_event_type=source_event_type,
        client_order_id="17",
        exchange_order_id=17,
        symbol="BTCUSDC",
        side="BUY",
        order_state="open",
    )


def test_journal_records_dual_exposure_terminal_route_and_remaining_qty() -> None:
    lifecycle = QuantityWeightedOrderLifecycle(
        initial_quantity=0.001,
        submitted_ts_ns=1_000_000_000,
    )
    lifecycle.activate(
        2_200_000_000,
        exchange_ts_ns=2_000_000_000,
    )
    lifecycle.observe_fill(
        remaining_after=0.0004,
        visibility_ts_ns=4_500_000_000,
        exchange_ts_ns=4_000_000_000,
    )
    lifecycle.request_cancel(5_000_000_000)
    lifecycle.exchange_terminal(
        9_000_000_000,
        reason="cancel_ack",
        exchange_ts_ns=8_000_000_000,
    )

    row = _row(lifecycle, "cancel_ack")
    assert row["schema_version"] == ORDER_LIFECYCLE_JOURNAL_SCHEMA_VERSION
    assert row["remaining_quantity_after"] == pytest.approx(0.0004)
    assert row["quantity_time_exposure_visible_btc_s"] == pytest.approx(0.0041)
    assert row["quantity_time_exposure_exchange_btc_s"] == pytest.approx(0.0036)
    assert row["quantity_time_exposure_visibility_minus_exchange_btc_s"] == pytest.approx(0.0005)
    assert row["exchange_exposure_valid"] == 1
    assert row["exchange_exposure_complete"] == 1
    assert row["terminal_reason"] == "cancel_ack"
    assert row["terminal_policy_route"] == "PROSPECTIVE_CANCEL_REENTRY"


def test_journal_audit_reports_missing_activation_clock_without_cpp_authority() -> None:
    lifecycle = QuantityWeightedOrderLifecycle(
        initial_quantity=0.001,
        submitted_ts_ns=1_000_000_000,
    )
    lifecycle.activate(2_000_000_000)
    row = _row(lifecycle, "activate")

    assert row["quantity_time_exposure_exchange_btc_s"] is None
    assert row["exchange_exposure_valid"] == 0
    assert row["exchange_exposure_complete"] == 0
    assert row["exchange_exposure_invalid_reason"] == ("missing_exchange_timestamp:activate")
    audit = audit_order_lifecycle_journal([row])
    assert audit["exchange_exposure_null_row_count"] == 1
    assert audit["exchange_exposure_invalid_reason_counts"] == {
        "missing_exchange_timestamp:activate": 1
    }
    assert audit["cpp_exposure_authority"] is False


def test_live_order_manager_persists_shared_lifecycle_journal(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "order_lifecycle_journal.csv"
    engine = MakerEngine.__new__(MakerEngine)
    engine.cfg = SimpleNamespace(symbol="BTCUSDC")
    engine._csv_log_lock = threading.Lock()
    engine._order_lifecycle_journal_lock = threading.Lock()
    engine._journaled_lifecycle_sequence = {}
    engine._order_lifecycle_journal_path = str(path)
    engine._exact_opportunity_tape_path = ""
    MakerEngine._init_csv_log(
        str(path),
        list(OrderLifecycleJournalRow.__dataclass_fields__),
    )
    manager = OrderManager(on_lifecycle_event=engine._on_order_lifecycle_event)
    engine.orders = manager
    timestamps = iter((1_000_000_000, 2_200_000_000))
    monkeypatch.setattr(
        "strategy.order_manager.time.time_ns",
        lambda: next(timestamps),
    )

    cid = manager.create_order("BTCUSDC", Side.BUY, 100.0, 0.001)
    engine._record_exact_order_event(manager.get_order(cid), "submit")
    manager.confirm_new(cid, 42, exchange_ts_ns=2_000_000_000)

    with path.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert [row["lifecycle_event"] for row in rows] == ["submit", "activate"]
    assert rows[-1]["runtime_source"] == "live"
    assert rows[-1]["exchange_exposure_valid"] == "1"
    assert rows[-1]["exchange_exposure_complete"] == "0"
