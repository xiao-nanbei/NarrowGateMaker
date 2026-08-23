from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from strategy.maker_engine import MakerEngine


def _engine_with_response(response: object) -> SimpleNamespace:
    return SimpleNamespace(
        cfg=SimpleNamespace(symbol="BTCUSDC"),
        rest=SimpleNamespace(get_position_risk=Mock(return_value=response)),
        inventory=SimpleNamespace(sync_from_exchange=Mock()),
    )


def test_sync_position_accepts_signed_empty_response_as_flat() -> None:
    engine = _engine_with_response([])

    assert MakerEngine.sync_position(engine, required=True) is True
    engine.rest.get_position_risk.assert_called_once_with(symbol="BTCUSDC")
    qty, entry, sync_start = engine.inventory.sync_from_exchange.call_args.args
    assert qty == 0.0
    assert entry == 0.0
    assert sync_start > 0.0


def test_sync_position_still_rejects_nonempty_wrong_symbol() -> None:
    engine = _engine_with_response([{"symbol": "ETHUSDC", "positionAmt": "0"}])

    with pytest.raises(RuntimeError, match="required position sync failed"):
        MakerEngine.sync_position(engine, required=True)
    engine.inventory.sync_from_exchange.assert_not_called()


def test_sync_position_rejects_nonlist_response() -> None:
    engine = _engine_with_response({"symbol": "BTCUSDC", "positionAmt": "0"})

    with pytest.raises(RuntimeError, match="required position sync failed"):
        MakerEngine.sync_position(engine, required=True)
    engine.inventory.sync_from_exchange.assert_not_called()
