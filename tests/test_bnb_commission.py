from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from live.commission import BnbCommissionValuator


START = 1_800_000_000_000
END = START + 59_999
FILL = END + 235
RATE = 700.0


def _client(row=None):
    return SimpleNamespace(index_price_klines=Mock(return_value=[
        [START, "699.8", "700.2", "699.1", str(RATE), "0", END]
        if row is None else row,
    ]))


@pytest.mark.parametrize("amount", [0.00004, -0.00004])
def test_bnb_fee_and_rebate_preserve_asset_and_signed_value(amount):
    client = _client()
    value = BnbCommissionValuator(client).value_in_quote(amount, "BNB", "USDC", FILL)
    assert value["quote_amount"] == pytest.approx(amount * RATE)
    assert value["raw_amount"] == amount
    assert value["raw_asset"] == "BNB"
    assert value["quote_asset"] == "USDC"
    assert value["exact_exchange_conversion"] is False
    assert value["close_time_ms"] < FILL
    client.index_price_klines.assert_called_once_with(
        pair="BNBUSDC", interval="1m", startTime=START, endTime=END, limit=1,
    )


def test_concurrent_ws_rest_share_one_immutable_rate_not_current_ticker():
    client = _client()
    valuator = BnbCommissionValuator(client)
    with ThreadPoolExecutor(4) as workers:
        results = list(workers.map(
            lambda _: valuator.value_in_quote(0.00004, "BNB", "USDC", FILL), range(12),
        ))
    assert all(result == results[0] for result in results)
    client.index_price_klines.assert_called_once()
    results[0]["rate"] = -1
    assert valuator.value_in_quote(0.00004, "BNB", "USDC", FILL)["rate"] == RATE
    # A fresh process/recovery uses the same completed minute.
    assert BnbCommissionValuator(_client()).value_in_quote(
        0.00004, "BNB", "USDC", FILL,
    ) == results[1]


@pytest.mark.parametrize("row", [
    [], [START], [START, 0, 0, 0, "nan", 0, END],
    [START, 0, 0, 0, "0", 0, END], [START, 0, 0, 0, "-1", 0, END],
    [START + 60_000, 0, 0, 0, "736", 0, END + 60_000],
    [START - 60_000, 0, 0, 0, "736", 0, END - 60_000],
    [START, 0, 0, 0, "736", 0, END + 1],
])
def test_unavailable_wrong_or_future_index_does_not_become_zero_fee(row):
    with pytest.raises(ValueError, match="valuation unavailable"):
        BnbCommissionValuator(_client(row)).value_in_quote(0.01, "BNB", "USDC", FILL)


def test_transport_error_is_not_retried_or_cached_as_zero():
    client = _client()
    client.index_price_klines.side_effect = TimeoutError("timeout")
    valuator = BnbCommissionValuator(client)
    with pytest.raises(ValueError, match="valuation unavailable"):
        valuator.value_in_quote(0.01, "BNB", "USDC", FILL)
    assert client.index_price_klines.call_count == 1
    assert not valuator._rates


@pytest.mark.parametrize("amount,asset,quote,clock", [
    (float("nan"), "BNB", "USDC", FILL),
    (0.01, "OTHER", "USDC", FILL), (0.01, "BNB", "UNKNOWN", FILL),
    (0.01, "BNB", "USDC", 0), (0.01, "BNB", "USDC", True),
    (0.01, "BNB", "USDC", FILL + 0.5),
    (0.01, "BNB", "USDC", None), (0.01, "BNB", "USDC", float("inf")),
    (0.01, "BNB", "USDC", float("nan")),
])
def test_invalid_identity_never_queries(amount, asset, quote, clock):
    client = _client()
    with pytest.raises(ValueError):
        BnbCommissionValuator(client).value_in_quote(amount, asset, quote, clock)
    client.index_price_klines.assert_not_called()
