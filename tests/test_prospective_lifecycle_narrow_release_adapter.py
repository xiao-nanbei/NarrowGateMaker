from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

from strategy.maker_engine import MakerEngine

pytestmark = pytest.mark.skipif(
    os.environ.get("NARROW_RELEASE_STAGE") != "1",
    reason="requires the staged narrow-v9 successor import graph",
)


def test_narrow_successor_uses_direct_async_lifecycle_callback() -> None:
    calls: list[tuple[object, ...]] = []
    order = SimpleNamespace(client_order_id="cid-1")
    engine = MakerEngine.__new__(MakerEngine)
    engine._order_lifecycle_live_writer_v2 = SimpleNamespace(
        enqueue_order_event=lambda *args: calls.append(args) or True,
    )

    assert not hasattr(MakerEngine, "_record_order_lifecycle_journal")
    engine._on_order_lifecycle_event(order, "submit", {"sequence": 1})

    assert calls == [(order, "submit", {"sequence": 1})]


def test_narrow_successor_routes_rest_reconcile_to_async_writer() -> None:
    calls: list[tuple[object, ...]] = []
    order = SimpleNamespace(
        client_order_id="cid-2",
        lifecycle=SimpleNamespace(
            events=lambda: ({"visibility_ts_ns": 2_400_000_000},)
        ),
    )
    engine = MakerEngine.__new__(MakerEngine)
    engine.orders = SimpleNamespace(get_order=lambda _cid: order)
    engine._order_lifecycle_live_writer_v2 = SimpleNamespace(
        enqueue_order_event=lambda *args: calls.append(args) or True,
    )

    engine.record_reconciled_order_lifecycle(
        order.client_order_id,
        "cancel_rejected_reconciled",
    )

    assert len(calls) == 1
    assert calls[0][0] is order
    assert calls[0][1] == "cancel_rejected_reconciled"
    assert calls[0][2]["_local_receive_ts_ns"] == 2_400_000_000
