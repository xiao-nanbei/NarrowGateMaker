from __future__ import annotations

import threading
import time
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace

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
from strategy.order_manager import OrderManager, OrderState, Side


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
    assert report["candidate_rate_gate_scope"] == ("BUY_and_SELL_each_independently")
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
    assert all(summary["candidate_rate_supported"] for summary in report["side_summaries"])


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
            "s": "BTCUSDC",
            "c": cid,
            "S": "BUY",
            "X": "PARTIALLY_FILLED",
            "i": 42,
            "p": "100.0",
            "q": "0.001",
            "z": "0.0004",
            "l": "0.0004",
            "L": "100.0",
            "ap": "100.0",
            "T": 3_500,
            "_local_receive_ts_ns": 3_600_000_000,
        }
    )
    manager.mark_pending_cancel(cid)
    manager.on_order_update(
        {
            "s": "BTCUSDC",
            "c": cid,
            "S": "BUY",
            "X": "CANCELED",
            "i": 42,
            "p": "100.0",
            "q": "0.001",
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
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def new_order(self, **kwargs):
        self.calls.append(dict(kwargs))
        return {
            "orderId": 42,
            "status": "NEW",
            "clientOrderId": kwargs["newClientOrderId"],
            "symbol": kwargs["symbol"],
            "side": kwargs["side"],
            "origQty": kwargs["quantity"],
            "executedQty": "0",
        }

    def cancel_order(self, **_kwargs):
        return {}


class _SubmitTimeoutRest:
    def new_order(self, **_kwargs):
        raise TimeoutError("submit response lost")


class _ExchangeError(RuntimeError):
    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.exchange_response_authoritative = True


class _StructuredRejectRest:
    def __init__(self, code: int, message: str) -> None:
        self.code = code
        self.message = message

    def new_order(self, **_kwargs):
        raise _ExchangeError(self.code, self.message)


class _MessageOnlyRejectRest:
    def new_order(self, **_kwargs):
        raise RuntimeError("exchange said -5022 but supplied no structured code")


class _MalformedSubmitRest:
    def __init__(self, response: object) -> None:
        self.response = response

    def new_order(self, **_kwargs):
        return self.response


class _QueryRest:
    def __init__(self, response=None, error: BaseException | None = None) -> None:
        self.response = response
        self.error = error

    def query_order(self, **_kwargs):
        if self.error is not None:
            raise self.error
        return dict(self.response or {})


class _RawQueryRest:
    def __init__(self, response: object) -> None:
        self.response = response

    def query_order(self, **_kwargs):
        return self.response


def _query_response_for_order(order, **overrides) -> dict[str, object]:
    response: dict[str, object] = {
        "symbol": order.symbol,
        "clientOrderId": order.client_order_id,
        "side": order.side.value,
        "status": "NEW",
        "orderId": order.order_id or 77,
        "price": str(order.price),
        "origQty": str(order.quantity),
        "executedQty": str(order.filled_qty),
        "avgPrice": "0",
    }
    response.update(overrides)
    return response


class _BlockingSubmitRest:
    def __init__(self) -> None:
        self.entered = threading.Event()
        self.release = threading.Event()
        self.calls: list[dict[str, object]] = []

    def new_order(self, **kwargs):
        self.calls.append(dict(kwargs))
        self.entered.set()
        assert self.release.wait(timeout=5.0)
        return {
            "orderId": 43,
            "status": "NEW",
            "clientOrderId": kwargs["newClientOrderId"],
            "symbol": kwargs["symbol"],
            "side": kwargs["side"],
            "origQty": kwargs["quantity"],
            "executedQty": "0",
        }


def _bare_engine(rest) -> MakerEngine:
    engine = object.__new__(MakerEngine)
    engine.set_admitted_user_stream_generation(1)
    engine.set_event_source(SimpleNamespace(user_event_safety_snapshot=lambda: {
        "user_stream_connected": True, "user_stream_generation": 1,
    }))
    engine.cfg = Config()
    engine.cfg.symbol = "BTCUSDC"
    engine.rest = rest
    engine.order_gateway = engine.rest
    engine.reconciliation_client = engine.rest
    engine._qty_precision = 3
    engine._price_precision = 1
    engine._order_ref_lock = threading.RLock()
    engine._order_context_lock = threading.RLock()
    engine._order_policy_context = {}
    engine._bid_cid = None
    engine._ask_cid = None
    engine._close_gtx_rejects = 0
    engine._record_exact_order_event = lambda *_args, **_kwargs: None
    engine._log_order_outcome = lambda *_args, **_kwargs: None
    engine._record_perf_rest_latency = lambda *_args, **_kwargs: None
    engine.orders = OrderManager()
    return engine


def _install_exact_account_trade_sync(
    engine: MakerEngine,
    *,
    cid: str,
    exchange_order_id: int,
    cumulative_fill: float,
    price: float,
    commission: float = 0.01,
    commission_asset: str = "USDC",
) -> None:
    """Model an independent accountTrades row, not query-order fill economics."""

    def _sync_position(*, required: bool = False) -> bool:
        order = engine.orders.get_order(cid)
        assert order is not None
        delta = cumulative_fill - float(order.filled_qty)
        if delta > 1e-12:
            engine.orders.reconcile_exchange_trade(
                exchange_order_id=exchange_order_id,
                trade_id=9_001,
                symbol=order.symbol,
                side=order.side,
                quantity=delta,
                price=price,
                commission=commission,
                commission_asset=commission_asset,
                cumulative_fill=cumulative_fill,
                trade_time_ms=1_900_000_000_000,
            )
        return True

    engine.sync_position = _sync_position


def test_submit_timeout_stays_pending_for_reconcile_instead_of_zero_exposure() -> None:
    engine = _bare_engine(_SubmitTimeoutRest())

    cid = engine._place_order("BTCUSDC", Side.BUY, 99.9, 0.001)

    assert cid is not None
    assert engine._bid_cid == cid
    assert engine.orders.get_order(cid).state == OrderState.PENDING_NEW
    snapshot = engine.orders.lifecycle_snapshot(cid)
    assert snapshot is not None
    assert snapshot["phase"] == "SUBMITTED"
    assert snapshot["quantity_time_exposure_exchange_btc_s"] is None
    assert engine.orders.lifecycle_events(cid)[-1]["event"] == "submit_ack_unknown"


@pytest.mark.parametrize("side", [Side.BUY, Side.SELL])
def test_structured_gtx_minus_5022_is_exact_zero_exposure(side: Side) -> None:
    engine = _bare_engine(_StructuredRejectRest(-5022, "Post Only order will be rejected"))

    cid = engine._place_order("BTCUSDC", side, 99.9, 0.001)

    assert cid is None
    assert (engine._bid_cid if side == Side.BUY else engine._ask_cid) is None
    rejected = next(iter(engine.orders._history.values()))
    snapshot = rejected.lifecycle.snapshot()
    assert rejected.state == OrderState.REJECTED
    assert snapshot["terminal_reason"] == "rejected"
    assert snapshot["exchange_exposure_complete"] is True
    assert snapshot["quantity_time_exposure_exchange_btc_s"] == 0.0


def test_message_only_minus_5022_remains_unknown_and_side_owned() -> None:
    engine = _bare_engine(_MessageOnlyRejectRest())

    cid = engine._place_order("BTCUSDC", Side.BUY, 99.9, 0.001)

    assert cid is not None
    assert engine._bid_cid == cid
    assert engine.orders.get_order(cid).state == OrderState.PENDING_NEW
    assert engine.orders.lifecycle_snapshot(cid)["exchange_exposure_complete"] is False


@pytest.mark.parametrize(
    "response",
    [
        {},
        {"status": "NEW"},
        {"orderId": 88},
        {"orderId": "not-an-integer", "status": "NEW"},
    ],
)
@pytest.mark.parametrize("reducing", [False, True])
def test_malformed_submit_response_remains_pending_and_owned(
    response: object,
    reducing: bool,
) -> None:
    engine = _bare_engine(_MalformedSubmitRest(response))

    if reducing:
        engine._place_close_order("BTCUSDC", Side.SELL, 100.1, 0.001)
        cid = engine._ask_cid
    else:
        cid = engine._place_order("BTCUSDC", Side.SELL, 100.1, 0.001)

    assert cid is not None
    assert engine._ask_cid == cid
    assert engine.orders.get_order(cid).state == OrderState.PENDING_NEW
    snapshot = engine.orders.lifecycle_snapshot(cid)
    assert engine.orders.lifecycle_events(cid)[-1]["event"] == "submit_ack_unknown"
    assert snapshot["exchange_exposure_complete"] is False


@pytest.mark.parametrize("side", [Side.BUY, Side.SELL])
def test_structured_gtx_minus_5022_close_is_exact_zero_exposure(side: Side) -> None:
    engine = _bare_engine(_StructuredRejectRest(-5022, "Post Only order will be rejected"))
    price = 99.9 if side == Side.BUY else 100.1

    engine._place_close_order("BTCUSDC", side, price, 0.001)

    assert (engine._bid_cid if side == Side.BUY else engine._ask_cid) is None
    rejected = next(iter(engine.orders._history.values()))
    snapshot = rejected.lifecycle.snapshot()
    assert rejected.state == OrderState.REJECTED
    assert snapshot["exchange_exposure_complete"] is True
    assert snapshot["quantity_time_exposure_exchange_btc_s"] == 0.0


@pytest.mark.parametrize("response", [{}, {"orderId": 89}])
def test_emergency_close_unknown_response_retains_reducing_ownership(
    response: object,
) -> None:
    engine = _bare_engine(_MalformedSubmitRest(response))
    engine.inventory = SimpleNamespace(net_position=0.001)
    engine._running = True

    engine._emergency_close(100.0)

    cid = engine._ask_cid
    assert engine.is_running is False
    assert cid is not None
    assert engine.orders.get_order(cid).state == OrderState.PENDING_NEW
    snapshot = engine.orders.lifecycle_snapshot(cid)
    assert engine.orders.lifecycle_events(cid)[-1]["event"] == "submit_ack_unknown"
    assert snapshot["exchange_exposure_complete"] is False


def test_emergency_close_timeout_retains_reducing_ownership() -> None:
    engine = _bare_engine(_SubmitTimeoutRest())
    engine.inventory = SimpleNamespace(net_position=-0.001)
    engine._running = True

    engine._emergency_close(100.0)

    cid = engine._bid_cid
    assert engine.is_running is False
    assert cid is not None
    assert engine.orders.get_order(cid).state == OrderState.PENDING_NEW
    assert engine.orders.lifecycle_snapshot(cid)["exchange_exposure_complete"] is False


@pytest.mark.parametrize("route", ["opening", "reducing"])
@pytest.mark.parametrize("side", [Side.BUY, Side.SELL])
def test_rest_minus_2013_cannot_release_unknown_submit_ownership(
    route: str,
    side: Side,
) -> None:
    engine = _bare_engine(_SubmitTimeoutRest())
    price = 99.9 if side == Side.BUY else 100.1
    if route == "reducing":
        engine._place_close_order("BTCUSDC", side, price, 0.001)
        cid = engine._bid_cid if side == Side.BUY else engine._ask_cid
    else:
        cid = engine._place_order("BTCUSDC", side, price, 0.001)
    assert cid is not None
    engine.rest = _QueryRest(error=_ExchangeError(-2013, "Order does not exist"))
    engine.order_gateway = engine.rest
    engine.reconciliation_client = engine.rest

    resolution = engine.reconcile_pending_new_order(engine.orders.get_order(cid))

    assert resolution == "exchange_not_found_ack_still_unknown"
    assert (engine._bid_cid if side == Side.BUY else engine._ask_cid) == cid
    assert engine.orders.get_order(cid).state == OrderState.PENDING_NEW
    snapshot = engine.orders.lifecycle_snapshot(cid)
    assert snapshot["phase"] == "SUBMITTED"
    assert snapshot["exchange_exposure_complete"] is False
    assert snapshot["quantity_time_exposure_exchange_btc_s"] is None


@pytest.mark.parametrize("route", ["opening", "reducing"])
@pytest.mark.parametrize("side", [Side.BUY, Side.SELL])
def test_rest_minus_2013_cannot_release_pending_cancel_ownership(
    route: str,
    side: Side,
) -> None:
    engine = _bare_engine(_Rest())
    price = 99.9 if side == Side.BUY else 100.1
    if route == "reducing":
        engine._place_close_order("BTCUSDC", side, price, 0.001)
        cid = engine._bid_cid if side == Side.BUY else engine._ask_cid
    else:
        cid = engine._place_order("BTCUSDC", side, price, 0.001)
    assert cid is not None
    engine.orders.mark_pending_cancel(cid)
    engine.rest = _QueryRest(error=_ExchangeError(-2013, "Order does not exist"))
    engine.order_gateway = engine.rest
    engine.reconciliation_client = engine.rest

    resolution = engine.reconcile_pending_cancel_order(engine.orders.get_order(cid))

    assert resolution == "exchange_not_found_terminal_still_unknown"
    assert (engine._bid_cid if side == Side.BUY else engine._ask_cid) == cid
    assert engine.orders.get_order(cid).state == OrderState.PENDING_CANCEL


@pytest.mark.parametrize("route", ["opening", "reducing"])
@pytest.mark.parametrize("pending_state", [OrderState.PENDING_NEW, OrderState.PENDING_CANCEL])
@pytest.mark.parametrize("side", [Side.BUY, Side.SELL])
@pytest.mark.parametrize(
    "malformed_kind",
    [
        "none",
        "list",
        "bad_status_type",
        "unknown_status",
        "bad_order_id",
        "bad_orig_qty",
        "bad_executed_qty",
    ],
)
def test_malformed_query_response_keeps_pending_ownership(
    route: str,
    pending_state: OrderState,
    side: Side,
    malformed_kind: str,
) -> None:
    submit_rest = _SubmitTimeoutRest() if pending_state == OrderState.PENDING_NEW else _Rest()
    engine = _bare_engine(submit_rest)
    price = 99.9 if side == Side.BUY else 100.1
    if route == "reducing":
        engine._place_close_order("BTCUSDC", side, price, 0.001)
        cid = engine._bid_cid if side == Side.BUY else engine._ask_cid
    else:
        cid = engine._place_order("BTCUSDC", side, price, 0.001)
    assert cid is not None
    order = engine.orders.get_order(cid)
    assert order is not None
    if pending_state == OrderState.PENDING_CANCEL:
        engine.orders.mark_pending_cancel(cid)
        order = engine.orders.get_order(cid)
        assert order is not None
    assert order.state == pending_state
    lifecycle_events_before = tuple(engine.orders.lifecycle_events(cid))

    response: object
    if malformed_kind == "none":
        response = None
    elif malformed_kind == "list":
        response = []
    elif malformed_kind == "bad_status_type":
        response = _query_response_for_order(order, status=[])
    elif malformed_kind == "unknown_status":
        response = _query_response_for_order(order, status="UNKNOWN")
    elif malformed_kind == "bad_order_id":
        response = _query_response_for_order(order, orderId="not-an-order-id")
    elif malformed_kind == "bad_orig_qty":
        response = _query_response_for_order(order, origQty=[])
    else:
        response = _query_response_for_order(order, executedQty="0.002")
    engine.rest = _RawQueryRest(response)
    engine.order_gateway = engine.rest
    engine.reconciliation_client = engine.rest

    if pending_state == OrderState.PENDING_NEW:
        resolution = engine.reconcile_pending_new_order(order)
    else:
        resolution = engine.reconcile_pending_cancel_order(order)

    assert resolution == "query_malformed_still_unknown"
    assert (engine._bid_cid if side == Side.BUY else engine._ask_cid) == cid
    retained = engine.orders.get_order(cid)
    assert retained is order
    assert retained.state == pending_state
    assert tuple(engine.orders.lifecycle_events(cid)) == lifecycle_events_before
    assert retained.lifecycle.snapshot()["exchange_exposure_complete"] is False


@pytest.mark.parametrize("status", ["PARTIALLY_FILLED", "FILLED"])
def test_reconcile_uses_exact_account_trade_not_query_order_average(status: str) -> None:
    engine = _bare_engine(_SubmitTimeoutRest())
    cid = engine._place_order("BTCUSDC", Side.BUY, 99.9, 0.001)
    assert cid is not None
    order = engine.orders.get_order(cid)
    assert order is not None
    executed_qty = "0.0005" if status == "PARTIALLY_FILLED" else "0.001"
    engine.rest = _RawQueryRest(
        {
            **_query_response_for_order(
                order,
                status=status,
                executedQty=executed_qty,
                cummulativeQuoteQty=str(float(executed_qty) * 99.8),
            ),
            "avgPrice": None,
        }
    )
    engine.order_gateway = engine.rest
    engine.reconciliation_client = engine.rest
    _install_exact_account_trade_sync(
        engine,
        cid=cid,
        exchange_order_id=77,
        cumulative_fill=float(executed_qty),
        price=99.7,
    )

    resolution = engine.reconcile_pending_new_order(order)

    assert resolution == f"exchange_status_{status.lower()}_reconciled"
    retained = engine.orders.get_order(cid)
    if status == "PARTIALLY_FILLED":
        assert retained is not None
        assert retained.filled_qty == pytest.approx(0.0005)
    else:
        assert retained is not None
        assert retained.state == OrderState.FILLED
        assert retained.filled_qty == pytest.approx(0.001)
    assert retained.avg_fill_price == pytest.approx(99.7)


def test_query_positive_fill_without_account_trades_latches_and_retains_ownership() -> None:
    engine = _bare_engine(_SubmitTimeoutRest())
    cid = engine._place_order("BTCUSDC", Side.BUY, 99.9, 0.001)
    assert cid is not None
    order = engine.orders.get_order(cid)
    assert order is not None
    engine.rest = _RawQueryRest(
        _query_response_for_order(
            order,
            status="FILLED",
            executedQty="0.001",
            avgPrice="99.8",
        )
    )
    engine.order_gateway = engine.rest
    engine.reconciliation_client = engine.rest
    engine.sync_position = lambda *, required=False: (_ for _ in ()).throw(
        RuntimeError("accountTrades evidence unavailable")
    )

    with pytest.raises(RuntimeError, match="exact accountTrades reconciliation"):
        engine.reconcile_pending_new_order(order)

    retained = engine.orders.get_order(cid)
    assert retained is not None
    assert retained.filled_qty == 0.0
    assert retained.order_id == 77
    assert engine._bid_cid == cid
    assert engine._runtime_reconciliation_required is True


@pytest.mark.parametrize("route", ["opening", "reducing"])
@pytest.mark.parametrize("side", [Side.BUY, Side.SELL])
def test_pending_cancel_query_order_id_mismatch_keeps_ownership(
    route: str,
    side: Side,
) -> None:
    engine = _bare_engine(_Rest())
    price = 99.9 if side == Side.BUY else 100.1
    if route == "reducing":
        engine._place_close_order("BTCUSDC", side, price, 0.001)
        cid = engine._bid_cid if side == Side.BUY else engine._ask_cid
    else:
        cid = engine._place_order("BTCUSDC", side, price, 0.001)
    assert cid is not None
    engine.orders.mark_pending_cancel(cid)
    order = engine.orders.get_order(cid)
    assert order is not None
    engine.rest = _RawQueryRest(_query_response_for_order(order, orderId=order.order_id + 1))
    engine.order_gateway = engine.rest
    engine.reconciliation_client = engine.rest

    resolution = engine.reconcile_pending_cancel_order(order)

    assert resolution == "query_malformed_still_unknown"
    assert (engine._bid_cid if side == Side.BUY else engine._ask_cid) == cid
    assert engine.orders.get_order(cid).state == OrderState.PENDING_CANCEL


def test_pending_cancel_reconcile_applies_fill_before_releasing_ownership() -> None:
    engine = _bare_engine(
        _QueryRest(
            response={
                "symbol": "BTCUSDC",
                "clientOrderId": "pending-cancel",
                "side": "BUY",
                "status": "FILLED",
                "orderId": 72,
                "price": "99.9",
                "origQty": "0.001",
                "executedQty": "0.001",
                "avgPrice": "99.8",
                "updateTime": 1_900_000_000_000,
            }
        )
    )
    cid = engine.orders.create_order("BTCUSDC", Side.BUY, 99.9, 0.001)
    engine.rest.response["clientOrderId"] = cid
    engine.orders.confirm_new(cid, 72)
    engine.orders.mark_pending_cancel(cid)
    engine._bid_cid = cid
    _install_exact_account_trade_sync(
        engine,
        cid=cid,
        exchange_order_id=72,
        cumulative_fill=0.001,
        price=99.7,
    )

    resolution = engine.reconcile_pending_cancel_order(engine.orders.get_order(cid))

    order = engine.orders.get_order(cid)
    assert resolution == "exchange_status_filled_reconciled"
    assert order is not None
    assert order.state == OrderState.FILLED
    assert order.filled_qty == pytest.approx(0.001)
    assert engine.orders.active_count() == 0


def test_same_side_orphan_conflict_stops_quoting_and_keeps_both_orders() -> None:
    engine = _bare_engine(_Rest())
    engine._running = True
    engine.orders = OrderManager(on_lifecycle_event=engine._on_order_lifecycle_event)
    tracked = engine.orders.create_order("BTCUSDC", Side.BUY, 99.9, 0.001)
    engine.orders.confirm_new(tracked, 41)
    engine._bid_cid = tracked

    engine.orders.on_order_update(
        {
            "s": "BTCUSDC",
            "c": "mm_B_conflicting_orphan",
            "S": "BUY",
            "X": "NEW",
            "i": 42,
            "p": "99.8",
            "q": "0.001",
            "T": 1_900_000_000_000,
            "_local_receive_ts_ns": 1_900_000_100_000_000_000,
        }
    )

    assert engine.is_running is False
    assert engine._bid_cid == tracked
    assert {order.client_order_id for order in engine.orders.get_active_by_side(Side.BUY)} == {
        tracked,
        "mm_B_conflicting_orphan",
    }


@pytest.mark.parametrize("phase", ("reserve", "verify"))
def test_ownership_conflict_fatal_cancel_runs_outside_nonreentrant_ref_lock(
    phase: str,
) -> None:
    engine = _bare_engine(_Rest())
    engine._order_ref_lock = threading.Lock()
    engine._running = True
    tracked = engine.orders.create_order("BTCUSDC", Side.BUY, 99.9, 0.001)
    engine.orders.confirm_new(tracked, 41)
    candidate = engine.orders.create_order("BTCUSDC", Side.BUY, 99.8, 0.001)
    engine._bid_cid = tracked
    cancel_calls = []

    def _reentrant_cancel_open_orders(**_kwargs) -> dict:
        with engine._order_ref_lock:
            cancel_calls.append(True)
        return {}

    engine.rest = SimpleNamespace(cancel_open_orders=_reentrant_cancel_open_orders)
    engine.order_gateway = engine.rest
    engine.reconciliation_client = engine.rest
    engine.sync_position = lambda *, required=False: True
    result: list[bool] = []

    def _run_conflict() -> None:
        if phase == "reserve":
            result.append(
                engine._reserve_side_order_ownership(
                    side=Side.BUY,
                    cid=candidate,
                )
            )
        else:
            result.append(
                engine._verify_side_order_ownership(
                    side=Side.BUY,
                    cid=candidate,
                    phase="test_reentrant_cancel",
                )
            )

    worker = threading.Thread(target=_run_conflict)
    worker.start()
    worker.join(timeout=2.0)

    assert not worker.is_alive()
    assert result == [False]
    assert cancel_calls == [True]
    assert engine._bid_cid == tracked
    assert engine._runtime_reconciliation_required is True


def test_rest_in_flight_orphan_cannot_escape_same_side_ownership_guard() -> None:
    rest = _BlockingSubmitRest()
    engine = _bare_engine(rest)
    engine._running = True
    engine.orders = OrderManager(on_lifecycle_event=engine._on_order_lifecycle_event)
    result: dict[str, str | None] = {}

    submit = threading.Thread(
        target=lambda: result.setdefault(
            "cid",
            engine._place_order("BTCUSDC", Side.SELL, 100.1, 0.001),
        )
    )
    submit.start()
    assert rest.entered.wait(timeout=5.0)
    reserved_cid = engine._ask_cid
    assert reserved_cid is not None

    engine.orders.on_order_update(
        {
            "s": "BTCUSDC",
            "c": "mm_S_inflight_orphan",
            "S": "SELL",
            "X": "NEW",
            "i": 44,
            "p": "100.2",
            "q": "0.001",
            "T": 1_900_000_000_000,
            "_local_receive_ts_ns": 1_900_000_100_000_000_000,
        }
    )
    assert engine._order_submit_fail_closed is True
    rest.release.set()
    submit.join(timeout=5.0)

    assert not submit.is_alive()
    assert result["cid"] == reserved_cid
    assert engine.is_running is False
    assert engine._ask_cid == reserved_cid
    assert {order.client_order_id for order in engine.orders.get_active_by_side(Side.SELL)} == {
        reserved_cid,
        "mm_S_inflight_orphan",
    }
    assert len(rest.calls) == 1

    assert engine._place_order("BTCUSDC", Side.BUY, 99.9, 0.001) is None
    engine._place_close_order("BTCUSDC", Side.BUY, 99.9, 0.001)

    assert len(rest.calls) == 1
    assert engine.orders.get_active_by_side(Side.BUY) == []


@pytest.mark.parametrize("reducing", (False, True))
def test_latched_conflict_between_reservation_and_rest_aborts_submit(
    reducing: bool,
) -> None:
    rest = _Rest()
    engine = _bare_engine(rest)
    engine._running = True
    engine.orders = OrderManager(on_lifecycle_event=engine._on_order_lifecycle_event)
    reserve = engine._reserve_side_order_ownership

    def reserve_then_latch(*, side: Side, cid: str) -> bool:
        admitted = reserve(side=side, cid=cid)
        engine._order_submit_fail_closed = True
        engine._running = False
        return admitted

    engine._reserve_side_order_ownership = reserve_then_latch
    if reducing:
        result = engine._place_close_order("BTCUSDC", Side.BUY, 99.9, 0.001)
    else:
        result = engine._place_order("BTCUSDC", Side.BUY, 99.9, 0.001)

    assert result is None
    assert rest.calls == []
    assert engine._bid_cid is None
    assert engine.orders.active_count() == 0


@pytest.mark.parametrize(
    ("status", "executed_qty", "expected_state"),
    [
        ("NEW", "0", OrderState.OPEN),
        ("PARTIALLY_FILLED", "0.0004", OrderState.PARTIALLY_FILLED),
        ("FILLED", "0.001", OrderState.FILLED),
    ],
)
@pytest.mark.parametrize("route", ["opening", "reducing"])
@pytest.mark.parametrize("side", [Side.BUY, Side.SELL])
def test_rest_reconcile_preserves_unknown_activation_and_fill_clock(
    route: str,
    side: Side,
    status: str,
    executed_qty: str,
    expected_state: OrderState,
) -> None:
    engine = _bare_engine(_SubmitTimeoutRest())
    price = 99.9 if side == Side.BUY else 100.1
    if route == "reducing":
        engine._place_close_order("BTCUSDC", side, price, 0.001)
        cid = engine._bid_cid if side == Side.BUY else engine._ask_cid
    else:
        cid = engine._place_order("BTCUSDC", side, price, 0.001)
    assert cid is not None
    submitted = engine.orders.get_order(cid)
    assert submitted is not None
    engine.rest = _QueryRest(
        response={
            "symbol": "BTCUSDC",
            "clientOrderId": cid,
            "side": side.value,
            "status": status,
            "orderId": 77,
            "price": str(price),
            "origQty": "0.001",
            "executedQty": executed_qty,
            "avgPrice": str(price) if executed_qty != "0" else "0",
            "updateTime": 1_900_000_000_000,
        }
    )
    engine.order_gateway = engine.rest
    engine.reconciliation_client = engine.rest
    if float(executed_qty) > 0.0:
        _install_exact_account_trade_sync(
            engine,
            cid=cid,
            exchange_order_id=77,
            cumulative_fill=float(executed_qty),
            price=price,
        )

    resolution = engine.reconcile_pending_new_order(submitted)

    order = engine.orders.get_order(cid)
    assert order is not None
    snapshot = order.lifecycle.snapshot()
    assert resolution == f"exchange_status_{status.lower()}_reconciled"
    assert order.state == expected_state
    assert order.filled_qty == pytest.approx(float(executed_qty))
    assert snapshot["activation_ts_ns"] == 0
    assert snapshot["activation_exchange_ts_ns"] == 0
    exact_trade_ts_ns = 1_900_000_000_000_000_000
    assert snapshot["first_fill_exchange_ts_ns"] == (
        exact_trade_ts_ns if float(executed_qty) > 0.0 else 0
    )
    assert snapshot["terminal_exchange_ts_ns"] == (
        exact_trade_ts_ns if status == "FILLED" else 0
    )
    assert snapshot["visible_exposure_valid"] is False
    assert snapshot["exchange_exposure_valid"] is False
    assert snapshot["visible_exposure_complete"] is False
    assert snapshot["exchange_exposure_complete"] is False


@pytest.mark.parametrize(
    ("status", "executed_qty", "expected_state"),
    [
        ("NEW", "0", OrderState.OPEN),
        ("PARTIALLY_FILLED", "0.0004", OrderState.PENDING_CANCEL),
        ("FILLED", "0.001", OrderState.FILLED),
    ],
)
@pytest.mark.parametrize("route", ["opening", "reducing"])
@pytest.mark.parametrize("side", [Side.BUY, Side.SELL])
def test_pending_cancel_reconcile_keeps_known_activation_but_not_rest_event_clock(
    route: str,
    side: Side,
    status: str,
    executed_qty: str,
    expected_state: OrderState,
) -> None:
    engine = _bare_engine(_Rest())
    price = 99.9 if side == Side.BUY else 100.1
    if route == "reducing":
        engine._place_close_order("BTCUSDC", side, price, 0.001)
        cid = engine._bid_cid if side == Side.BUY else engine._ask_cid
    else:
        cid = engine._place_order("BTCUSDC", side, price, 0.001)
    assert cid is not None
    engine.orders.mark_pending_cancel(cid)
    pending = engine.orders.get_order(cid)
    assert pending is not None
    activation_ts_ns = pending.lifecycle.activation_ts_ns
    assert activation_ts_ns > 0
    engine.rest = _RawQueryRest(
        _query_response_for_order(
            pending,
            status=status,
            executedQty=executed_qty,
            avgPrice=str(price) if executed_qty != "0" else "0",
            updateTime=1_900_000_000_000,
        )
    )
    engine.order_gateway = engine.rest
    engine.reconciliation_client = engine.rest
    if float(executed_qty) > 0.0:
        _install_exact_account_trade_sync(
            engine,
            cid=cid,
            exchange_order_id=int(pending.order_id),
            cumulative_fill=float(executed_qty),
            price=price,
        )

    resolution = engine.reconcile_pending_cancel_order(pending)

    order = engine.orders.get_order(cid)
    assert order is not None
    snapshot = order.lifecycle.snapshot()
    assert resolution == f"exchange_status_{status.lower()}_reconciled"
    assert order.state == expected_state
    assert order.filled_qty == pytest.approx(float(executed_qty))
    assert snapshot["activation_ts_ns"] == activation_ts_ns
    assert snapshot["activation_exchange_ts_ns"] == 0
    exact_trade_ts_ns = 1_900_000_000_000_000_000
    assert snapshot["first_fill_exchange_ts_ns"] == (
        exact_trade_ts_ns if float(executed_qty) > 0.0 else 0
    )
    assert snapshot["terminal_exchange_ts_ns"] == (
        exact_trade_ts_ns if status == "FILLED" else 0
    )
    if expected_state in {OrderState.OPEN, OrderState.PENDING_CANCEL}:
        assert (engine._bid_cid if side == Side.BUY else engine._ask_cid) == cid


@pytest.mark.parametrize("side", [Side.BUY, Side.SELL])
def test_reducing_close_submit_timeout_remains_pending_and_owned(side: Side) -> None:
    engine = _bare_engine(_SubmitTimeoutRest())

    engine._place_close_order("BTCUSDC", side, 99.9, 0.001)

    cid = engine._bid_cid if side == Side.BUY else engine._ask_cid
    assert cid is not None
    assert engine.orders.get_order(cid).state == OrderState.PENDING_NEW
    snapshot = engine.orders.lifecycle_snapshot(cid)
    assert snapshot["phase"] == "SUBMITTED"
    assert snapshot["exchange_exposure_complete"] is False


def test_live_producer_links_submit_and_cancel_to_origin_decision(
    tmp_path: Path,
) -> None:
    engine = object.__new__(MakerEngine)
    engine.set_admitted_user_stream_generation(1)
    engine.set_event_source(SimpleNamespace(user_event_safety_snapshot=lambda: {
        "user_stream_connected": True, "user_stream_generation": 1,
    }))
    engine.cfg = Config()
    engine.cfg.symbol = "BTCUSDC"
    engine.rest = _Rest()
    engine.order_gateway = engine.rest
    engine.reconciliation_client = engine.rest
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
    assert not engine._cancel_order(
        cid,
        record_requote_perf=False,
        trigger_decision_id="g-next:BUY",
    )
    exchange_ts_ns = time.time_ns()
    engine.orders.on_order_update(
        {
            "s": "BTCUSDC",
            "c": cid,
            "S": "BUY",
            "X": "CANCELED",
            "i": 42,
            "p": "99.9",
            "q": "0.001",
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
