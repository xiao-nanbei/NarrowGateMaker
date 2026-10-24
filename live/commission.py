"""Value Binance BNB fees without changing their original exchange identity.

The exchange debits BNB, not USDC. Quote PnL/risk accounting uses a labelled
market valuation: the preceding completed one-minute BNB/quote index close.
It is deterministic across WS/REST and recovery, never today's price applied
to an old fill, and is not claimed to be Binance's exact discount-conversion
rate. Missing data remains an error, not a zero fee or a stablecoin peg.
"""

from collections import OrderedDict
import math
import threading


class BnbCommissionValuator:
    """A bounded cache on its own zero-retry REST session, never the order pool."""

    def __init__(self, client):
        self._client = client
        self._lock = threading.Lock()
        self._rates: OrderedDict[tuple[str, int], dict] = OrderedDict()

    def value_in_quote(self, amount, asset, quote_asset, trade_time_ms):
        amount = float(amount)
        if not math.isfinite(amount) or str(asset).upper() != "BNB":
            raise ValueError("invalid BNB commission")
        quote = str(quote_asset).upper()
        if quote not in {"USDC", "USDT"}:
            raise ValueError("unsupported BNB commission quote asset")
        try:
            clock_ms = int(trade_time_ms)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("BNB commission requires an exact trade timestamp") from exc
        if isinstance(trade_time_ms, bool) or clock_ms != trade_time_ms:
            raise ValueError("BNB commission requires an exact trade timestamp")
        trade_time_ms = clock_ms
        start_ms = (trade_time_ms // 60_000 - 1) * 60_000
        if start_ms < 0:
            raise ValueError("BNB commission requires a completed historical minute")
        end_ms = start_ms + 59_999
        key = (quote, start_ms)
        with self._lock:
            evidence = self._rates.get(key)
            if evidence is None:
                try:
                    rows = self._client.index_price_klines(
                        pair="BNB" + quote, interval="1m",
                        startTime=start_ms, endTime=end_ms, limit=1,
                    )
                    if not isinstance(rows, list) or len(rows) != 1:
                        raise ValueError("missing exact BNB index minute")
                    row = rows[0]
                    if (len(row) < 7 or int(row[0]) != start_ms
                            or int(row[6]) != end_ms or end_ms >= trade_time_ms):
                        raise ValueError("BNB index minute has wrong/future time")
                    rate = float(row[4])
                    if not math.isfinite(rate) or rate <= 0:
                        raise ValueError("invalid BNB index close")
                except Exception as exc:
                    raise ValueError("BNB commission market valuation unavailable") from exc
                evidence = dict(
                    method="previous_closed_1m_index_close_market_valuation",
                    pair="BNB" + quote, rate=rate,
                    open_time_ms=start_ms, close_time_ms=end_ms,
                    exact_exchange_conversion=False,
                )
                self._rates[key] = evidence
                if len(self._rates) > 4096:
                    self._rates.popitem(last=False)
            self._rates.move_to_end(key)
            value = amount * evidence["rate"]
            if not math.isfinite(value):
                raise ValueError("nonfinite BNB commission quote valuation")
            return dict(evidence, raw_amount=amount, raw_asset="BNB",
                        quote_asset=quote, quote_amount=value)
