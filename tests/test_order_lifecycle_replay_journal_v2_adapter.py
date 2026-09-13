from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

from models.backtest_tick import simulate_tick
from models.replay.order_lifecycle_v2_replay_adapter import (
    OrderLifecycleV2ReplayAdapter,
)
from models.tick_data_types import HistoricalBBOData


def _identity() -> dict[str, object]:
    return {
        "baseline_epoch_id": "prospective-test-epoch",
        "runtime_code_sha256": "a" * 64,
        "config_sha256": "b" * 64,
        "execution_abi": "order-lifecycle-v2",
    }


def _order(
    trace_id: int,
    *,
    side: str = "BUY",
    submit_ms: int = 1_000,
    quantity: float = 0.002,
) -> dict[str, object]:
    return {
        "trace_id": trace_id,
        "side": side,
        "submit_ts": submit_ms,
        "quote_ts": submit_ms,
        "quantity": quantity,
        "remaining": quantity,
    }


def _adapter(root: Path, *, session_id: str = "replay-test") -> OrderLifecycleV2ReplayAdapter:
    return OrderLifecycleV2ReplayAdapter(
        root=root,
        session_id=session_id,
        runtime_identity=_identity(),
        symbol="BTCUSDC",
        storage_format="parquet",
    )


def _rows(root: Path, session_id: str = "replay-test") -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for manifest_path in sorted(
        (root / f"session-{session_id}" / "parts").glob("part-*.manifest.json")
    ):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        data_path = manifest_path.parent / str(manifest["data_file"])
        rows.extend(pq.read_table(data_path).to_pylist())
    return sorted(rows, key=lambda row: (str(row["lifecycle_id"]), int(row["lifecycle_sequence"])))


def test_replay_adapter_covers_all_authoritative_terminal_routes(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path)

    filled = _order(1)
    adapter.submit(filled, 1_000)
    adapter.activate(filled, visibility_ts_ms=1_110, exchange_ts_ms=1_100)
    adapter.fill(
        filled,
        remaining_after=0.001,
        visibility_ts_ms=1_210,
        exchange_ts_ms=1_200,
        full_fill=False,
    )
    adapter.request_cancel(filled, 1_220)
    adapter.cancel_reject(
        filled,
        visibility_ts_ms=1_240,
        exchange_ts_ms=1_230,
    )
    adapter.fill(
        filled,
        remaining_after=0.0,
        visibility_ts_ms=1_260,
        exchange_ts_ms=1_250,
        full_fill=True,
    )

    cancelled = _order(2, side="SELL", submit_ms=2_000)
    adapter.submit(cancelled, 2_000)
    adapter.activate(cancelled, visibility_ts_ms=2_110, exchange_ts_ms=2_100)
    adapter.request_cancel(cancelled, 2_200)
    adapter.cancel_ack(
        cancelled,
        visibility_ts_ms=2_230,
        exchange_ts_ms=2_220,
    )

    rejected = _order(3, submit_ms=3_000)
    adapter.submit(rejected, 3_000)
    adapter.reject(rejected, visibility_ts_ms=3_120, exchange_ts_ms=3_100)

    expired = _order(4, submit_ms=4_000)
    adapter.submit(expired, 4_000)
    adapter.activate(expired, visibility_ts_ms=4_110, exchange_ts_ms=4_100)
    adapter.expire(expired, visibility_ts_ms=4_220, exchange_ts_ms=4_200)

    censored = _order(5, submit_ms=5_000)
    adapter.submit(censored, 5_000)
    adapter.activate(censored, visibility_ts_ms=5_110, exchange_ts_ms=5_100)
    adapter.shutdown_censor(censored, 5_500)

    health = adapter.close()
    rows = _rows(tmp_path)
    events = [str(row["lifecycle_event"]) for row in rows]
    assert {
        "submit",
        "activate",
        "partial_fill",
        "cancel_request",
        "cancel_rejected",
        "full_fill",
        "exchange_terminal",
        "local_shutdown_censor",
    }.issubset(events)
    assert {
        str(row["exchange_terminal_reason"])
        for row in rows
        if row["terminal_observation"] == "EXCHANGE_TERMINAL"
    } == {"cancel_ack", "expired", "full_fill", "rejected"}
    assert health["rows_committed"] == len(rows)
    assert len({str(row["event_id"]) for row in rows}) == len(rows)
    assert health["rows_dropped"] == 0
    assert health["error_count"] == 0
    assert health["formal_collection_valid"] is True
    assert health["economic_outcomes_read"] is False
    assert health["q90_action_authorized"] is False


def test_sub_lot_partial_fill_remains_in_exchange_risk_set(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path, session_id="sub-lot-partial")
    order = _order(6, quantity=0.001)
    adapter.submit(order, 1_000)
    adapter.activate(order, visibility_ts_ms=1_110, exchange_ts_ms=1_100)
    adapter.fill(
        order,
        remaining_after=0.0004,
        visibility_ts_ms=1_210,
        exchange_ts_ms=1_200,
        full_fill=False,
    )

    lifecycle = adapter._lifecycles[6]
    assert lifecycle.phase.name == "PARTIALLY_FILLED"
    assert lifecycle.remaining_quantity == pytest.approx(0.0004)
    assert lifecycle.fill_risk_active is True

    adapter.request_cancel(order, 1_220)
    adapter.cancel_ack(order, visibility_ts_ms=1_240, exchange_ts_ms=1_230)
    health = adapter.close()
    rows = _rows(tmp_path, "sub-lot-partial")
    partial = next(row for row in rows if row["lifecycle_event"] == "partial_fill")
    assert partial["remaining_quantity_after"] == pytest.approx(0.0004)
    assert partial["fill_risk_active_after"] is True
    assert health["rows_dropped"] == 0
    assert health["error_count"] == 0


def test_replay_adapter_restart_rebuild_is_deduplicated(tmp_path: Path) -> None:
    order = _order(7)
    first = _adapter(tmp_path, session_id="restart")
    first.submit(order, 1_000)
    first.activate(order, visibility_ts_ms=1_100, exchange_ts_ms=1_090)
    first.request_cancel(order, 1_200)
    first.cancel_ack(order, visibility_ts_ms=1_240, exchange_ts_ms=1_230)
    first_health = first.close()
    assert first_health["rows_committed"] == 4

    replayed = _adapter(tmp_path, session_id="restart")
    replayed.submit(order, 1_000)
    replayed.activate(order, visibility_ts_ms=1_100, exchange_ts_ms=1_090)
    replayed.request_cancel(order, 1_200)
    replayed.cancel_ack(order, visibility_ts_ms=1_240, exchange_ts_ms=1_230)
    second_health = replayed.close()

    rows = _rows(tmp_path, "restart")
    assert len(rows) == 4
    assert len({str(row["event_id"]) for row in rows}) == 4
    assert second_health["rows_committed"] == 4
    assert second_health["rows_dropped"] == 0
    assert second_health["error_count"] == 0


def _tiny_replay_inputs() -> tuple[pd.DataFrame, HistoricalBBOData]:
    base = 1_720_000_000_000
    trades = pd.DataFrame(
        {
            "transact_time": [base, base + 1_000, base + 2_000, base + 3_000],
            "price": [100.0, 100.0, 100.0, 100.0],
            "quantity": [0.0, 0.0, 0.0, 0.0],
            "is_buyer_maker": [False, False, False, False],
        }
    )
    bbo_ts = np.arange(base, base + 3_001, 100, dtype=np.int64)
    bbo = HistoricalBBOData(
        ts_ms=bbo_ts,
        best_bid=np.full(bbo_ts.size, 99.9),
        best_ask=np.full(bbo_ts.size, 100.1),
        bid_qty=np.full(bbo_ts.size, 1.0),
        ask_qty=np.full(bbo_ts.size, 1.0),
    )
    return trades, bbo


def _tiny_params() -> dict[str, object]:
    return {
        "gamma": 0.01,
        "kappa": 1.0,
        "order_size": 0.001,
        "max_inventory": 0.01,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "maker_fee": 0.0,
        "taker_fee": 0.0,
        "tick_size": 0.1,
        "lot_size": 0.001,
        "use_bar_pricing": True,
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 100,
        "collect_curves": False,
        "position_timeout": 0.0,
        "markout_ema_span_fills": 0,
        "max_exec_book_age_s": 0.0,
        "trace_local_order_lifecycle_max": 20,
    }


def _assert_replay_output_equal(left: object, right: object) -> None:
    if isinstance(left, np.ndarray):
        assert isinstance(right, np.ndarray)
        np.testing.assert_equal(left, right)
        return
    if isinstance(left, dict):
        assert isinstance(right, dict)
        assert left.keys() == right.keys()
        for key in left:
            _assert_replay_output_equal(left[key], right[key])
        return
    if isinstance(left, (list, tuple)):
        assert isinstance(right, type(left))
        assert len(left) == len(right)
        for left_item, right_item in zip(left, right, strict=True):
            _assert_replay_output_equal(left_item, right_item)
        return
    if isinstance(left, float) and np.isnan(left):
        assert isinstance(right, float) and np.isnan(right)
        return
    assert right == pytest.approx(left) if isinstance(left, float) else right == left


def test_disabled_replay_journal_v2_preserves_output_exactly(tmp_path: Path) -> None:
    trades, bbo = _tiny_replay_inputs()
    params = _tiny_params()
    baseline = simulate_tick(
        trades,
        np.asarray([trades["transact_time"].iloc[0]], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        bbo_data=bbo,
    )
    disabled = simulate_tick(
        trades,
        np.asarray([trades["transact_time"].iloc[0]], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        {**params, "order_lifecycle_journal_v2_enabled": False},
        bbo_data=bbo,
    )
    _assert_replay_output_equal(baseline, disabled)
    assert "_order_lifecycle_journal_v2_health" not in disabled
    assert not list(tmp_path.iterdir())


def test_authoritative_replay_hook_emits_zero_drop_tape(tmp_path: Path) -> None:
    trades, bbo = _tiny_replay_inputs()
    params = {
        **_tiny_params(),
        "order_lifecycle_journal_v2_enabled": True,
        "order_lifecycle_journal_v2_root": tmp_path,
        "order_lifecycle_journal_v2_session_id": "authoritative-replay",
        "_order_lifecycle_journal_v2_runtime_identity": _identity(),
    }
    result = simulate_tick(
        trades,
        np.asarray([trades["transact_time"].iloc[0]], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        bbo_data=bbo,
    )
    health = result["_order_lifecycle_journal_v2_health"]
    rows = _rows(tmp_path, "authoritative-replay")
    assert rows
    assert health["rows_committed"] == len(rows)
    assert health["rows_dropped"] == 0
    assert health["error_count"] == 0
    assert health["formal_collection_valid"] is True
    assert all("pnl" not in key and "markout" not in key for key in rows[0])
