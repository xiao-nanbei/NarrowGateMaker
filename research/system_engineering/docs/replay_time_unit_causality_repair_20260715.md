# Replay Time, Unit, and Causality Repair (2026-07-15)

> Current status (2026-07-27): the defects and dimensional formulas documented here remain valid provenance, but the causal-v4 handoff and example template horizons are no longer current. `docs/time_unit_contract_repair_20260726.md` is authoritative: it adds calendar, commission, daily-PnL, tick/lot, post-fill-volatility and bar-clock repairs and defines `causal-v7` as the replacement identity. The current public quote horizon is 1 second.

## Decision

All ML-assisted tick-replay evidence generated from legacy 10-second feature artifacts is invalid for promotion. Formal replay rejects model metadata that does not declare bucket-end visibility, label horizon semantics, and nonzero training support for every feature.

The 2026-07-15 audit found 191 legacy `*_meta.json` files and none carried the new timing contract. The superseded local bundles were removed from `models/` on 2026-07-17. Their numerical model and replay conclusions have been deleted. Metadata must not be edited in place to make an old model appear causal.

## Confirmed Defects

1. `sigma_sq` is one-second absolute-price variance in `(USDC/BTC)^2 / second`. Circuit-breaker and exit-urgency code multiplied `sqrt(sigma_sq)` by both inventory and mid, producing the wrong unit.
2. AS/GLFT quote math used per-second variance without an explicit integration horizon.
3. A left-labelled feature row at time `t` contains `[t, t+10s)` but replay exposed it at `t`; live exposes it only when the bucket closes.
4. Daily feature generation reset 6h/24h/7d rolling state at UTC midnight.
5. `markout_hl=50` was implemented as EMA span `N=50`, whose true half-life is about 17.3 fill updates.
6. Live markout was resolved only during a later requote, so a nominal 10s observation could become roughly 10-20s. External lifecycle freshness also omitted injected visibility delay.
7. Some legacy markout/EV reports event-weighted partial fills and summed a `USDC/BTC` price difference as if it were `USDC` PnL.
8. The Python/C++ tick loops advanced quote timers primarily on execution trades, so sparse periods delayed TTL, cooldown, requote, pending-order, stale-book, and markout state.
9. Replay day exposure used `unique seconds containing trades / 86400`, and PnL-curve capacity used `event_count / interval_ms`. Both mixed event counts with wall-clock units; sparse days could be reported as half a day and fills/day was correspondingly inflated.

## Implemented Contracts

### Volatility and risk

For absolute-price variance:

```text
sigma_pnl_usdc = sqrt(sigma2_price_per_s * horizon_s) * abs(qty_btc)
```

No mid-price multiplier is allowed. `quote_horizon_s` and `pnl_volatility_horizon_s` are explicit live/replay/C++ parameters. The public template used 10s and 300s examples at the time of this repair; they were not promoted values. The current public/current quote horizon is explicitly 1s; the PnL risk horizon remains a separate contract and must not be inferred from the quote horizon.

### Feature visibility

For a left-labelled 10-second row:

```text
feature_ready_ts = feature_index + 10 seconds
```

Replay prediction lookup uses `feature_ready_ts`, never the raw index. Window cache version 5 invalidates cached prediction arrays created with the old timestamp semantics.

Daily feature generation accepts up to seven immediately contiguous prior UTC days as causal warmup and stops at the first missing day. External joins and labels are applied only after slicing back to the target day, so future days or non-contiguous history cannot enter the target row.

`label_ret_10s` and `label_dir_10s` remain compatibility names, but model metadata now states their real meaning: fill within `h`, followed by a markout `h` after fill, giving a decision-to-outcome span between `h` and `2h`. `label_vol_10s` is a fixed forward-10s absolute-price variance label.

### Markout and fill quantities

`markout_ema_span_fills` replaces the misleading active name `markout_hl`; the old field is only a deprecated configuration alias. `markout_horizon_s` is explicit. Live resolves pending markout on the frequent wall-clock tick, not only during quote recomputation. Python and C++ replay use the same horizon and historical-book mid when available.

Canonical order-level markout is quantity weighted:

```text
avg_markout_bps = sum(markout_bps * fill_qty_btc) / sum(fill_qty_btc)
sum_ev_30s_usdc = sum(ev_30s_usdc_per_btc * fill_qty_btc)
```

Tail rates remain event/campaign rates and are not volume weighted.

### Replay event clock

Python and C++ formal replay now consume the same causal merged stream:

```text
execution aggTrades
+ historical BBO/L2 state-change timestamps
+ fixed 100ms timer deadlines
```

Synthetic book/timer rows carry the latest causally visible execution price and zero quantity. They can advance order transitions, TTL, cooldown, requote, stale-book checks, markout observation, and inventory-time integration, but cannot consume queue, create fills, or update an inferred trade touch. Real trades at the same millisecond retain their stable input order.

Exploratory runs may explicitly use `replay_event_clock=trade` to reproduce a legacy diagnostic. Formal calibration rejects that mode and requires `replay_event_clock=merged` with a positive `replay_clock_interval_ms`. Replay duration and curve capacity now derive from elapsed milliseconds rather than event count or active-trade seconds.

### Verification

- The checkpoint repository suite passed after rebuilding the C++ extension and sharing the common live/replay side-policy guard.
- Real-data parity on `2026-07-03 00:00-00:10 UTC` with historical BBO/L2 and a 100ms merged clock: Python/C++ both produced 2 fills, 9,461 clock events, 101 requotes, and identical action counts; PnL differed by only `2.2e-15 USDC`.
- A full-day public-template diagnostic changed from 178 fills / `-0.97 USDC` on the legacy trade clock to 184 fills / `+1.18 USDC` on the merged clock. This is not a live-baseline or alpha result. It demonstrates that the timing repair changes order lifecycle outcomes, so legacy evidence cannot be kept on the assumption that shared lookahead/timing errors cancel in A/B deltas.

## Supersession

The causal-v2 model metrics, split counts, directory commands and policy A/B results have been removed. They were migration diagnostics, not current evidence. The later causal-v4, causal-v5 and causal-v6 exact model/replay numbers were also superseded by the 2026-07-26 repair. The maintained identity is now `causal-v7`, documented in `docs/time_unit_contract_repair_20260726.md`; its strict ML A/B did not pass promotion and 13-head inference remains disabled.

Current formal work must continue to:

1. bind model features, empirical P3, queue, latency and data manifests;
2. use merged clocks and causal feature-ready timestamps;
3. separate fixed-horizon toxicity labels from lifecycle/campaign outcomes;
4. evaluate explicit actions with chronological evidence and known overlap.
