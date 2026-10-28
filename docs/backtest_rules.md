# Generic toy backtest rules

[English](backtest_rules.md) | [简体中文](backtest_rules.zh-CN.md)

Last materially synchronized: 2026-10-28

These are explicit teaching assumptions, not the private production replay contract.

1. Events must have strictly increasing, finite timestamps within the declared horizon. Invalid sides, nonpositive prices/volume, empty input and invalid configuration are rejected. Equal-time batches are not supported.
2. On each event, first match the previously resting quote against that event's trade. A sell aggressor can fill the bid when its trade price is at or below the bid; a buy aggressor can fill the ask at or above the ask. The first event cannot fill a quote created from itself.
3. Fill at the old quoted price, not at the observed trade price. Quantity is the smaller of quote size and event volume. A trade can fill at most one side. There is no queue-priority model; this optimistic rule is not a guarantee of an actual fill.
4. Apply the fill and fee, observe the new mid, then cancel/replace both old quotes using the updated inventory. Cancellation and replacement are instantaneous between events. Unfilled residuals do not carry to the next quote pair. A quote is never matched retroactively to the event that created it.
5. Begin with zero cash and zero inventory. For a signed fill quantity `dq`, update `inventory += dq` and `cash -= dq * price + fee`. Fees are nonnegative notional charges and use exact decimal arithmetic under Python's default Decimal context. This teaching implementation does not promise unlimited-precision accounting.
6. End equity is `cash + inventory * last_mid`. Open inventory remains marked, not forcibly liquidated; marked equity is not realized profit. Final quotes have no later event on which to fill.

Synthetic prices are a bounded integer random walk with independent fictional aggressor and volume draws. The seed controls reproducibility in the same Python runtime; no real data or fitted distribution is used. The CLI normalizes event times to a unit horizon. The formula is floating point, while tick rounding and cash accounting use Decimal.

Not modeled: queue depth, market impact, exchange price bands, price improvement, order acknowledgement, network latency, financing, funding, leverage, margin, liquidation, cash/inventory limits, disconnections or production safeguards. Short inventory and negative cash are permitted. The example does not demonstrate economic validity or suitability for deployment.

Use the tests to inspect causality, aggressor direction, partial fills, rounding, fees, mark-to-market, determinism and invalid inputs. Do not substitute this toy engine for a validated market-specific simulator.
