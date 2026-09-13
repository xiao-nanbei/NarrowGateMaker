# Retained Event-L2 Rebuild (2026-07-18)

Status: engineering/data validation. This document does not claim trading alpha and does not change the rolling live or replay baseline.

Current status (2026-07-27): the native snapshot/delta reconstruction and sequence findings remain valid. The old top-level one-second/top-10 versus 100ms/top-20 description is now historical: new normalized replay uses `normalized_l2_100ms_v2`, while exact queue research must stream native data. The 2026-07-04 through 2026-07-11 individual-trade files were later replaced after an all-true maker-side defect, so any hash or side-specific outcome tied to their former copies is superseded.

## Question

The retained historical `bbo/` and `l2/` files contain roughly 86,400 states per day. They are useful one-second containers, but they cannot resolve the sub-second shock, depletion, refill, recovery, keep, or cancel paths needed by the next local-order-value study.

Binance individual `trades` help, but they solve only part of the problem:

- `trades` preserve published matching events and aggressor direction;
- `aggTrades` may merge multiple matching events;
- neither stream records passive cancellation or refill;
- cancellation/refill requires price-level L2 snapshot/delta events.

The event-level reconstruction therefore combines Binance individual trades with the existing CryptoHFTData BTCUSDC price-level L2 tape.

On 2026-01-01, the public inputs contain 144,806 `aggTrades` versus 369,566 individual trades (2.55x as many rows), while total BTC quantity is exactly the same at 9,245.1328125 BTC. The extra rows preserve matching-event order rather than adding volume. There are 275,143 same-timestamp individual-trade rows, so the loader retains Binance `trade_id` and orders by `(transact_time, trade_id)`; it must not collapse equal timestamps.

## Source identity

The retained111 manifest has 24 raw BTCUSDC order-book hours on all 111 dates. The raw schema contains exchange/receive clocks, `event_type`, `U/u/pu`-style sequence IDs, side, price, and quantity.

Across retained111, the source contains 206,167,977 logical L2 messages. Their mean exchange-time interval is 46.52ms; 59.09% are within 50ms and 96.10% are within 100ms of the previous logical message. This is real event resolution, not a linear interpolation of the old one-second containers. The normalized replay artifact is a causal 100ms top-20 state stream; 10-50ms research must consume the source event deltas rather than relabel this wide state stream as a 10ms tape.

CryptoHFTData is an incomplete third-party collection. An hourly file may begin with deltas and contain no complete snapshot. Consequently the build exposes two identities:

| Identity | Initialization | Permitted use |
| --- | --- | --- |
| `snapshot` | Native complete source snapshot; strict `U/u/pu` continuation | Strict top-N reconstruction and source-valid queue studies |
| `delta-converged` | Empty book anchored at first observed `pu`; strict continuation; explicit burn-in | Validated top-20 path features after burn-in |

`delta-converged` is not exact deep-queue truth. It is never allowed to inherit the `snapshot` label.

## Truth-set validation

The delta-converged reconstruction was compared row-for-row against two days that also have native-snapshot strict top-20 output. Both use Binance transaction time, a 100ms output grid, and a 120-second day-start burn-in.

| UTC day | Native-snapshot rows | Delta rows after burn-in | Overlap rows | BBO exact | All 80 top-20 fields exact | Mismatched rows |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026-07-03 | 556,358 | 854,292 | 556,358 | 100.0000% | 100.0000% | 0 |
| 2026-07-12 | 55,030 | 846,060 | 55,030 | 100.0000% | 100.0000% | 0 |

The comparison is deliberately limited to timestamps after the first native snapshot. It validates the delta-converged identity for top-20 local shock/refill/recovery features on the observed overlap. It does not prove that the pre-snapshot period is exact, and it does not validate queue ahead at quote prices hundreds of ticks from the touch.

## Full retained111 audit

The final build contains 93,456,377 BBO rows and the same number of top-20 L2 rows. Strict whole-day gates produce:

| Gate | Passing UTC days |
| --- | ---: |
| Retained manifest | 111 |
| `U/u/pu` sequence valid | 80 |
| At least 99% normalized coverage | 101 |
| Both gates, complete schema, positive spread | 76 |

The 31 sequence-gap days and 10 coverage-below-99% days remain in the versioned root for diagnostics, but only the 76-day `eligible_days.csv` subset may enter a strict action panel.

The source snapshot grouping also received a correctness repair. A native snapshot contains thousands of price rows and can carry more than one recorder receive timestamp. Grouping by receive timestamp falsely counted 28,107 duplicate snapshots. Grouping by exchange snapshot event/update identity reduces that count to 94, preserves all 73 genuine sequence gaps, and adds 48 previously omitted normalized rows.

## One-day path regression

A 5,000-order July 3 local-order-value smoke was rerun twice with the same individual-trade clock and strategy identity. Only the L2 root changed. Book-state resolution moved from 1,000ms to 101ms. Average observed path counts changed from 2.08 cancel and 2.14 refill events per order under the one-second input to 7.59 cancel and 8.44 refill events, or 3.64x and 3.94x respectively. The fitted diagnostic half-lives moved from `1000/1000/1000ms` to `1000/500/101ms` for adverse market-order, cancel, and refill intensity.

This is not a strategy result. It shows that one-second input aliases the queue-reactive path and can mechanically force fitted decay toward one-second bins. The two runs also produced different order/fill/campaign paths, so their PnL must not be interpreted as a clean alpha A/B.

## Replay contract

Event-L2 research must use:

```text
individual trades
  + event BBO/L2 state-change timestamps
  + fixed timer events
  -> merged causal replay clock
```

Zero-quantity book/timer events advance book and lifecycle state but cannot consume queue or produce a fill. Every state lookup remains as-of:

```text
state_timestamp <= decision_timestamp
```

The window-cache identity includes the execution-trade source and BBO/L2 artifact paths. A `trades` + event-L2 run therefore cannot silently reuse an `aggTrades` + one-second cache.

The normalized timestamp is Binance transaction time `T`, not the AWS Tokyo strategy's feature-ready time. A policy experiment must still preserve two clocks: matching/queue consumption occurs at exchange time, while the strategy may consume the corresponding trade/book information only after the frozen environment-specific feed and feature delay. Reusing `T` as both clocks is an ideal-latency diagnostic, not formal executable evidence.

The independent trades and order-book archives also have no shared sequence number. Events with the same millisecond timestamp therefore have no provable cross-stream order. The current observation replay records `historical_book_visibility=exchange_time_asof_le_ideal_latency_diagnostic`; it must not use a favorable Python sort order as evidence. Formal action replay needs separate exchange-time matching and delayed feature-ready visibility.

## Daily eligibility

Each retained day receives a separate sequence audit. A day enters the event-L2 research manifest only when all of the following pass:

- 24/24 raw source hours;
- readable top-20 BBO and L2 output;
- at least 99% coverage under a 500ms freshness window;
- complete top-20 schema and 100% valid positive spread;
- accepted sequence updates and a real snapshot or delta anchor;
- zero `U/u/pu` gap, invalid sequence message, or time reversal.

A day that recovers after a sequence gap may remain useful for diagnostics, but the whole UTC day is excluded from the strict event-L2 action panel.

Canonical data identity is recorded under:

- `coverage.csv`
- `eligible_days.csv`
- `sequence_audit.json`
- `individual_trades_retained111.sha256`
- `files.sha256`
- `build_identity.json`

All 226 entries in `files.sha256` and all 111 individual-trade files passed SHA256 verification after the final rebuild.

## Scope

This rebuild removes the old one-second state-resolution ceiling for local top-20 research. The next valid experiment can estimate local shock/depletion/refill/recovery paths and `queue_value_keep_cancel_v1` on the eligible subset.

It does not by itself establish:

- exact deep-250 queue position;
- hidden liquidity or cancellation location relative to our order;
- cross-venue receive-time history;
- a dual exchange-time/feature-ready local-feed latency replay;
- profitable keep/cancel action uplift.

Those remain separate calibration and causal-policy questions.
