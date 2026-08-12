# Three-Venue Global Reference Stage 0 - 2026-07-11

> **Current evidence status (2026-07-27).** The external trade-derived one-second state construction and leave-one-venue-out methodology remain useful. Exact maker markout, fill, campaign and PnL tables in this report are withdrawn because their local BTCUSDC outcome denominator used the superseded mixed L2/queue identity. The only maintained conclusion is that this Stage-0 experiment did not authorize an action; it does not establish that external information has no value. Any M1 successor must rebuild local outcomes under the current normalized/native contracts.

> **Frozen evidence boundary.** This report is the final baseline for the trades-derived causal one-second reference. It answers only whether second-scale external states sort later outcomes. It must not be extended by tuning more one-second thresholds, and it is not a negative result about receive-time cross-venue maker information. New work starts from public WebSocket BBO/trades, local receive time, event-driven flow state, and 10/25/50/100/250/500ms fill-toxicity labels.

For a self-contained Chinese methodology review, exact leave-one-venue-out tables, false-negative risks, and review questions, see `docs/global_market_reference_review_memo_20260711.md`.

## Scope

This audit evaluates a shadow-only hierarchical reference for Binance BTCUSDC perpetual. It does not change quotes, size, inventory limits, cancel behavior, or fills.

The reference separates three roles:

1. Bitget, Bybit, and OKX spot form a robust external spot innovation.
2. The same three venues' perpetual markets form a separate derivatives innovation.
3. Binance BTCUSDT perpetual is the local level bridge. Binance `USDCUSDT` spot converts that bridge into USDC terms:

   `BTCUSDC bridge = BTCUSDT perpetual / USDCUSDT spot`

Binance BTCUSDC spot remains a cross-check and freshness fallback. It is not an independent vote.

## Data Boundary

- Canonical universe: 111 retained UTC good days.
- OKX perpetual: 111 days, 399,947,490 normalized trades, 8,742,874 causal one-second states.
- OKX spot: 111 days, 69,915,914 normalized trades, 5,846,509 causal one-second states.
- Binance USDCUSDT spot: 111 days, 46,776,513 aggTrades, 8,792,656 one-second bars; raw CSV/ZIP inputs were removed after successful conversion.
- Three-venue outputs: 7,747,571 spot states, 9,225,272 perpetual states, and 7,547,083 spot/perpetual joined states.
- Hierarchical reference: 6,247,083 states, of which 4,659,028 pass the strict freshness, 2-of-3, direction-agreement, dispersion, and causal-basis gates.

All historical venue trades use right-edge visibility: events in `[t, t+1s)` become available at `t+1s`. This dataset cannot establish a 50/100ms cancel policy.

## Result

The hierarchical pending residual does not pass Stage 0.

- The only global-residual row with positive 30-second and campaign deltas in the full and all three leave-one-venue-out variants is `SELL submit 1s`. Its 30-second effect is only about `+0.05` to `+0.10 bps`, its five-second effect is negative, and daily sign consistency is close to one half.
- `SELL fill + divergent` has a larger 30-second markout clue, but its five-second effect is near zero or changes sign; campaign and repair results are not stable when a venue is removed.
- `BUY submit + perp_only_up` and `SELL fill + perp_only_up` also show isolated longer-horizon clues, but Bitget/OKX leave-out results weaken or reverse the short-horizon effect and campaign repair does not improve consistently.
- Small `spot_leading` buckets remain sample-starved and do not support a policy decision.

The third venue is useful mainly as a falsification instrument: it enables 2-of-3 consensus, median aggregation, outlier rejection, and leave-one-out tests. It has not yet converted the second-scale signal into stable maker alpha.

## Decision

- Freeze this one-second Stage 0 as diagnostic evidence; do not run further threshold or policy searches on the same aggregated state.
- Keep all six external spot/perpetual sources in receive-time shadow capture.
- Keep `USDCUSDT` as the preferred Binance currency-conversion anchor.
- Keep global residual, spot/perp divergence, leader confidence, and venue dispersion as order-level and campaign-level diagnostics.
- Do not create re-center, cancel, tighten, size, or lifecycle arms from this Stage 0 result.
- Any later policy test must first pass chronological evidence, latency stress, side markout, campaign terminal, repair, and tail gates.

The failure here is therefore **horizon and transport specific**: it rejects `trades -> causal 1s state -> maker action`. It does not reject `receive-time BBO/trades -> cross-venue flow/depletion -> fill toxicity`.

The earlier seven-day OKX report is retained only as historical provenance and is superseded by this 111-day audit.
