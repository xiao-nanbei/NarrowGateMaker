from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from execution.order_lifecycle import QuantityWeightedOrderLifecycle
from execution.order_lifecycle_journal_v2 import (
    OrderLifecycleJournalV2BatchEmitter,
    OrderLifecycleJournalV2SourceCallback,
)
from models.backtest_tick import simulate_tick
from models.replay.order_lifecycle_v2_replay_adapter import (
    OrderLifecycleV2ReplayAdapter,
)
from models.tick_data_types import HistoricalBBOData
from research.families.f07_active_order_continuation.audit.order_lifecycle_v2_event_lockstep import (
    ATOMIC_ENVELOPE_SCHEMA_VERSION,
    audit_lifecycle_event_streams,
    audit_lifecycle_parquet_day,
)

DAY = "2026-08-01"
BASE_MS = int(datetime(2026, 8, 1, tzinfo=timezone.utc).timestamp() * 1_000)


def _identity() -> dict[str, object]:
    return {
        "baseline_epoch_id": "f07-lockstep-synthetic",
        "runtime_code_sha256": "a" * 64,
        "config_sha256": "b" * 64,
        "execution_abi": "order-lifecycle-v2",
    }


def _order(order_id: int, submit_ms: int, quantity: float) -> dict[str, object]:
    return {
        "trace_id": order_id,
        "side": "BUY" if order_id % 2 else "SELL",
        "submit_ts": submit_ms,
        "quote_ts": submit_ms,
        "quantity": quantity,
        "remaining": quantity,
    }


class _LegacyRows:
    def __init__(self) -> None:
        self.rows: list[dict[str, object]] = []

    def add(
        self,
        order: dict[str, object],
        *,
        event_type: str,
        visibility_ms: int,
        exchange_ms: int | None,
        state_before: str,
        state_after: str,
        remaining: float,
        reason: str = "",
    ) -> None:
        self.rows.append(
            {
                "symbol": "BTCUSDC",
                "order_id": int(order["trace_id"]),
                "event_type": event_type,
                "event_ts_ns": int(visibility_ms) * 1_000_000,
                "event_seq": len(self.rows) + 1,
                "event_reason": reason,
                "state_before": state_before,
                "state_after": state_after,
                "order_submit_ts_ns": int(order["submit_ts"]) * 1_000_000,
                "order_qty": float(order["quantity"]),
                "remaining_qty": float(remaining),
                "event_visibility_ts_ns": int(visibility_ms) * 1_000_000,
                "event_exchange_ts_ns": (
                    int(exchange_ms) * 1_000_000 if exchange_ms is not None else None
                ),
            }
        )


def _journal_rows(root: Path, session: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    parts = root / f"session-{session}" / "parts"
    for manifest_path in sorted(parts.glob("part-*.manifest.json")):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        rows.extend(pq.read_table(manifest_path.parent / manifest["data_file"]).to_pylist())
    return rows


def _synthetic_fixture(
    root: Path,
) -> tuple[list[dict[str, object]], list[dict[str, object]], Path]:
    session = "f07-lockstep"
    adapter = OrderLifecycleV2ReplayAdapter(
        root=root,
        session_id=session,
        runtime_identity=_identity(),
        symbol="BTCUSDC",
    )
    legacy = _LegacyRows()

    # Partial fill, fill while cancel-pending, cancel reject, then full fill.
    order = _order(1, BASE_MS + 1_000, 0.003)
    adapter.submit(order, BASE_MS + 1_000)
    legacy.add(
        order,
        event_type="submit",
        visibility_ms=BASE_MS + 1_000,
        exchange_ms=None,
        state_before="not_submitted",
        state_after="pending_new",
        remaining=0.003,
    )
    adapter.activate(
        order,
        visibility_ts_ms=BASE_MS + 1_020,
        exchange_ts_ms=BASE_MS + 1_010,
    )
    legacy.add(
        order,
        event_type="activate",
        visibility_ms=BASE_MS + 1_020,
        exchange_ms=BASE_MS + 1_010,
        state_before="pending_new",
        state_after="open",
        remaining=0.003,
    )
    adapter.fill(
        order,
        remaining_after=0.002,
        visibility_ts_ms=BASE_MS + 1_120,
        exchange_ts_ms=BASE_MS + 1_110,
        full_fill=False,
    )
    legacy.add(
        order,
        event_type="partial_fill",
        visibility_ms=BASE_MS + 1_120,
        exchange_ms=BASE_MS + 1_110,
        state_before="open",
        state_after="open",
        remaining=0.002,
    )
    adapter.request_cancel(order, BASE_MS + 1_140)
    legacy.add(
        order,
        event_type="cancel_request",
        visibility_ms=BASE_MS + 1_140,
        exchange_ms=None,
        state_before="open",
        state_after="pending_cancel",
        remaining=0.002,
    )
    adapter.fill(
        order,
        remaining_after=0.001,
        visibility_ts_ms=BASE_MS + 1_180,
        exchange_ts_ms=BASE_MS + 1_170,
        full_fill=False,
    )
    legacy.add(
        order,
        event_type="partial_fill",
        visibility_ms=BASE_MS + 1_180,
        exchange_ms=BASE_MS + 1_170,
        state_before="pending_cancel",
        state_after="pending_cancel",
        remaining=0.001,
    )
    adapter.cancel_reject(
        order,
        visibility_ts_ms=BASE_MS + 1_220,
        exchange_ts_ms=BASE_MS + 1_210,
    )
    legacy.add(
        order,
        event_type="cancel_reject",
        visibility_ms=BASE_MS + 1_220,
        exchange_ms=BASE_MS + 1_210,
        state_before="pending_cancel",
        state_after="open",
        remaining=0.001,
    )
    adapter.fill(
        order,
        remaining_after=0.0,
        visibility_ts_ms=BASE_MS + 1_300,
        exchange_ts_ms=BASE_MS + 1_290,
        full_fill=True,
    )
    legacy.add(
        order,
        event_type="full_fill",
        visibility_ms=BASE_MS + 1_300,
        exchange_ms=BASE_MS + 1_290,
        state_before="open",
        state_after="filled",
        remaining=0.0,
    )

    # Cancel ACK with remaining quantity.
    cancelled = _order(2, BASE_MS + 2_000, 0.002)
    adapter.submit(cancelled, BASE_MS + 2_000)
    legacy.add(
        cancelled,
        event_type="submit",
        visibility_ms=BASE_MS + 2_000,
        exchange_ms=None,
        state_before="not_submitted",
        state_after="pending_new",
        remaining=0.002,
    )
    adapter.activate(
        cancelled,
        visibility_ts_ms=BASE_MS + 2_020,
        exchange_ts_ms=BASE_MS + 2_010,
    )
    legacy.add(
        cancelled,
        event_type="activate",
        visibility_ms=BASE_MS + 2_020,
        exchange_ms=BASE_MS + 2_010,
        state_before="pending_new",
        state_after="open",
        remaining=0.002,
    )
    adapter.request_cancel(cancelled, BASE_MS + 2_100)
    legacy.add(
        cancelled,
        event_type="cancel_request",
        visibility_ms=BASE_MS + 2_100,
        exchange_ms=None,
        state_before="open",
        state_after="pending_cancel",
        remaining=0.002,
    )
    adapter.cancel_ack(
        cancelled,
        visibility_ts_ms=BASE_MS + 2_140,
        exchange_ts_ms=BASE_MS + 2_130,
    )
    legacy.add(
        cancelled,
        event_type="cancel_ack",
        visibility_ms=BASE_MS + 2_140,
        exchange_ms=BASE_MS + 2_130,
        state_before="pending_cancel",
        state_after="cancelled",
        remaining=0.002,
        reason="requote",
    )

    # Pre-activation reject.
    rejected = _order(3, BASE_MS + 3_000, 0.002)
    adapter.submit(rejected, BASE_MS + 3_000)
    legacy.add(
        rejected,
        event_type="submit",
        visibility_ms=BASE_MS + 3_000,
        exchange_ms=None,
        state_before="not_submitted",
        state_after="pending_new",
        remaining=0.002,
    )
    adapter.reject(
        rejected,
        visibility_ts_ms=BASE_MS + 3_040,
        exchange_ts_ms=BASE_MS + 3_030,
    )
    legacy.add(
        rejected,
        event_type="reject",
        visibility_ms=BASE_MS + 3_040,
        exchange_ms=BASE_MS + 3_030,
        state_before="pending_new",
        state_after="rejected",
        remaining=0.002,
        reason="gtx_would_cross_ask",
    )

    # Exchange expiry.
    expired = _order(4, BASE_MS + 4_000, 0.002)
    adapter.submit(expired, BASE_MS + 4_000)
    legacy.add(
        expired,
        event_type="submit",
        visibility_ms=BASE_MS + 4_000,
        exchange_ms=None,
        state_before="not_submitted",
        state_after="pending_new",
        remaining=0.002,
    )
    adapter.activate(
        expired,
        visibility_ts_ms=BASE_MS + 4_020,
        exchange_ts_ms=BASE_MS + 4_010,
    )
    legacy.add(
        expired,
        event_type="activate",
        visibility_ms=BASE_MS + 4_020,
        exchange_ms=BASE_MS + 4_010,
        state_before="pending_new",
        state_after="open",
        remaining=0.002,
    )
    adapter.expire(
        expired,
        visibility_ts_ms=BASE_MS + 4_120,
        exchange_ts_ms=BASE_MS + 4_110,
    )
    legacy.add(
        expired,
        event_type="expiry",
        visibility_ms=BASE_MS + 4_120,
        exchange_ms=BASE_MS + 4_110,
        state_before="open",
        state_after="cancelled",
        remaining=0.002,
        reason="expired",
    )

    # Local shutdown right-censor, not an exchange terminal.
    censored = _order(5, BASE_MS + 5_000, 0.002)
    adapter.submit(censored, BASE_MS + 5_000)
    legacy.add(
        censored,
        event_type="submit",
        visibility_ms=BASE_MS + 5_000,
        exchange_ms=None,
        state_before="not_submitted",
        state_after="pending_new",
        remaining=0.002,
    )
    adapter.activate(
        censored,
        visibility_ts_ms=BASE_MS + 5_020,
        exchange_ts_ms=BASE_MS + 5_010,
    )
    legacy.add(
        censored,
        event_type="activate",
        visibility_ms=BASE_MS + 5_020,
        exchange_ms=BASE_MS + 5_010,
        state_before="pending_new",
        state_after="open",
        remaining=0.002,
    )
    adapter.shutdown_censor(censored, BASE_MS + 5_200)
    legacy.add(
        censored,
        event_type="day_end_censor",
        visibility_ms=BASE_MS + 5_200,
        exchange_ms=None,
        state_before="open",
        state_after="censored",
        remaining=0.002,
        reason="end_of_window",
    )

    health = adapter.close()
    assert health["rows_dropped"] == 0
    assert health["error_count"] == 0
    return legacy.rows, _journal_rows(root, session), root / f"session-{session}"


def test_synthetic_terminal_routes_and_dual_clock_eq_are_lockstep(
    tmp_path: Path,
) -> None:
    legacy, journal, _session = _synthetic_fixture(tmp_path)
    report = audit_lifecycle_event_streams(
        day=DAY,
        legacy_rows=legacy,
        journal_v2_rows=journal,
    )

    assert report["mechanics_lockstep_passed"] is True
    assert report["mismatch_counts"] == {}
    assert report["counts"]["matched_lifecycle_count"] == 5
    assert report["terminal_counts"] == {
        "exchange_terminal": 4,
        "local_shutdown_censor": 1,
    }
    assert report["coverage"]["legacy_visible_exposure_supported_lifecycles"] == 5
    assert report["coverage"]["full_dual_clock_pairs"] > 0
    assert report["counts"]["cancel_reject_to_active_count"] == 0
    assert report["counts"]["cancel_reject_to_partially_filled_count"] == 1
    assert report["gates"]["cancel_reject_risk_set_continuation"] is True
    assert all(report["gates"].values())
    assert all(value is False for value in report["permissions"].values())
    assert report["scope"]["economic_outcomes_read"] is False


def test_remaining_quantity_drift_fails_closed(tmp_path: Path) -> None:
    legacy, journal, _session = _synthetic_fixture(tmp_path)
    changed = [dict(row) for row in legacy]
    changed[2]["remaining_qty"] = 0.0025

    report = audit_lifecycle_event_streams(
        day=DAY,
        legacy_rows=changed,
        journal_v2_rows=journal,
    )

    assert report["mechanics_lockstep_passed"] is False
    assert report["mismatch_counts"]["remaining_quantity_mismatch"] == 1
    assert report["gates"]["remaining_quantity_lockstep"] is False


def _v2_rows_with_post_terminal_recovery() -> list[dict[str, object]]:
    submit_ns = (BASE_MS + 8_000) * 1_000_000
    client_order_id = f"replay-BTCUSDC-8-{BASE_MS + 8_000}"
    lifecycle_id = f"synthetic:{client_order_id}"
    lifecycle = QuantityWeightedOrderLifecycle(0.002, submit_ns)
    emitter = OrderLifecycleJournalV2BatchEmitter(
        lifecycle_id=lifecycle_id,
        runtime_source="authoritative_python_replay",
        client_order_id=client_order_id,
        symbol="BTCUSDC",
        side="SELL",
        exchange_order_id="exchange-8",
    )
    rows: list[dict[str, object]] = []

    def emit(callback: str, visibility_ns: int, exchange_ns: int | None) -> None:
        batch = emitter.emit_unseen(
            lifecycle=lifecycle,
            callback=OrderLifecycleJournalV2SourceCallback(
                callback_id=f"{lifecycle_id}:{callback}:{len(rows)}",
                callback_type=callback,
                received_ts_ns=visibility_ns,
                exchange_ts_ns=exchange_ns,
            ),
        )
        rows.extend(batch.payloads())

    emit("submit", submit_ns, None)
    lifecycle.activate(submit_ns + 20_000_000, exchange_ts_ns=submit_ns + 10_000_000)
    emit("activate", submit_ns + 20_000_000, submit_ns + 10_000_000)
    lifecycle.request_cancel(submit_ns + 80_000_000)
    emit("cancel_request", submit_ns + 80_000_000, None)
    lifecycle.exchange_terminal(
        submit_ns + 120_000_000,
        reason="cancel_ack",
        exchange_ts_ns=submit_ns + 110_000_000,
    )
    emit("cancel_ack", submit_ns + 120_000_000, submit_ns + 110_000_000)
    lifecycle.enter_post_cancel_recovery(submit_ns + 130_000_000)
    emit("post_cancel_recovery", submit_ns + 130_000_000, None)
    return rows


def test_any_post_terminal_event_is_reported(tmp_path: Path) -> None:
    legacy, _journal, _session = _synthetic_fixture(tmp_path)
    order_8 = _order(8, BASE_MS + 8_000, 0.002)
    legacy_8 = _LegacyRows()
    legacy_8.add(
        order_8,
        event_type="submit",
        visibility_ms=BASE_MS + 8_000,
        exchange_ms=None,
        state_before="not_submitted",
        state_after="pending_new",
        remaining=0.002,
    )
    legacy_8.add(
        order_8,
        event_type="activate",
        visibility_ms=BASE_MS + 8_020,
        exchange_ms=BASE_MS + 8_010,
        state_before="pending_new",
        state_after="open",
        remaining=0.002,
    )
    legacy_8.add(
        order_8,
        event_type="cancel_request",
        visibility_ms=BASE_MS + 8_080,
        exchange_ms=None,
        state_before="open",
        state_after="pending_cancel",
        remaining=0.002,
    )
    legacy_8.add(
        order_8,
        event_type="cancel_ack",
        visibility_ms=BASE_MS + 8_120,
        exchange_ms=BASE_MS + 8_110,
        state_before="pending_cancel",
        state_after="cancelled",
        remaining=0.002,
        reason="requote",
    )

    report = audit_lifecycle_event_streams(
        day=DAY,
        legacy_rows=[*legacy, *legacy_8.rows],
        journal_v2_rows=[*_journal, *_v2_rows_with_post_terminal_recovery()],
    )

    assert report["mechanics_lockstep_passed"] is False
    assert report["mismatch_counts"]["v2_event_after_terminal_or_censor"] == 1
    assert report["gates"]["zero_post_terminal_events"] is False


def test_parquet_streaming_and_atomic_result_ignore_unprojected_economics(
    tmp_path: Path,
) -> None:
    fixture_root = tmp_path / "journal"
    legacy, _journal, session = _synthetic_fixture(fixture_root)
    legacy_with_unread_column = [{**row, "pnl_usdc": 999_999.0} for row in legacy]
    legacy_path = tmp_path / "legacy.parquet"
    pq.write_table(pa.Table.from_pylist(legacy_with_unread_column), legacy_path)
    output = tmp_path / "admitted" / "lockstep.json"

    report = audit_lifecycle_parquet_day(
        day=DAY,
        legacy_paths=[legacy_path],
        journal_v2_paths=[session],
        output_path=output,
        batch_size=2,
    )

    envelope = json.loads(output.read_text(encoding="utf-8"))
    assert envelope["schema_version"] == ATOMIC_ENVELOPE_SCHEMA_VERSION
    assert envelope["report_sha256"] == report["canonical_report_sha256"]
    assert envelope["report"]["mechanics_lockstep_passed"] is True
    assert envelope["report"]["scope"]["economic_outcomes_read"] is False
    assert envelope["report"]["input_artifacts"]["legacy"][0]["sha256"]
    assert not list(output.parent.glob("*.tmp"))


def test_authoritative_replay_and_legacy_trace_are_directly_auditable(
    tmp_path: Path,
) -> None:
    trades = pd.DataFrame(
        {
            "transact_time": [
                BASE_MS,
                BASE_MS + 1_000,
                BASE_MS + 2_000,
                BASE_MS + 3_000,
            ],
            "price": [100.0, 100.0, 100.0, 100.0],
            "quantity": [0.0, 0.0, 0.0, 0.0],
            "is_buyer_maker": [False, False, False, False],
        }
    )
    bbo_ts = np.arange(BASE_MS, BASE_MS + 3_001, 100, dtype=np.int64)
    bbo = HistoricalBBOData(
        ts_ms=bbo_ts,
        best_bid=np.full(bbo_ts.size, 99.9),
        best_ask=np.full(bbo_ts.size, 100.1),
        bid_qty=np.full(bbo_ts.size, 1.0),
        ask_qty=np.full(bbo_ts.size, 1.0),
    )
    params = {
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
        "trace_local_order_lifecycle_max": 100,
        "order_lifecycle_journal_v2_enabled": True,
        "order_lifecycle_journal_v2_root": tmp_path,
        "order_lifecycle_journal_v2_session_id": "authoritative",
        "_order_lifecycle_journal_v2_runtime_identity": _identity(),
    }
    result = simulate_tick(
        trades,
        np.asarray([BASE_MS], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        bbo_data=bbo,
    )

    report = audit_lifecycle_event_streams(
        day=DAY,
        legacy_rows=result["_local_order_lifecycle_trace"],
        journal_v2_rows=_journal_rows(tmp_path, "authoritative"),
    )

    assert report["mechanics_lockstep_passed"] is True
    assert report["mismatch_counts"] == {}
    assert report["coverage"]["legacy_dual_clock_is_not_assumed"] is True
