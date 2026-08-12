# Quote Decision Snapshot Atomicity v2

Last materially modified: 2026-08-04

## Status

`baseline_integrity_local_verified_not_deployed`

This successor closes the remaining clock and generic pricing boundaries in v1. It is baseline-integrity work, reads no economic outcomes, and changes no P3, ML, spread, inventory, cooldown, q90, fill-selection, or `exit_urgency` parameter.

## Contract

One immutable `QuoteDecisionSnapshot` now owns the complete execution-book view for a requote. It freezes depth, depth history, depth mid, the latest completed bar-pricing mid, bookTicker, generation counters, exchange time, local receive time, capture time, and lock timing.

The stale clocks are explicit:

\[
age_{visible}=t_{capture}-t_{receive}
\]

\[
lag_{source}=t_{receive}-t_{exchange}
\]

Live stale policy uses `age_visible`. Source transport has a separate gate. The historical `max_exec_book_age_s` field remains only for replay ABI compatibility.

bookTicker may guard post-only routing only when its prices and both clocks are valid and fresh. Otherwise the same frozen depth BBO is used and the exact fallback reason is journaled. Missing source timestamps remain missing; the feed adapter no longer fabricates exchange time from the local wall clock.

`use_bar_pricing=true` no longer rereads mutable `_close_history`: its completed bar close is frozen in the same snapshot, while all depth invariants remain mandatory.

## Telemetry

`quote_snapshot_integrity.csv` records generations, both BBOs, selected guard, fallback reason, split clocks, lock wait/hold, pricing mid, final routed prices, snapshot blocks, REST cancels, and routing actions. The audit also reports BBO spread/delta distributions, cancel-rate change against an equal preceding window, market-tape/external-recorder queue health, and severe runtime logs.

## Local Verification

- repository suite: `1611 passed, 4 skipped`;
- concurrent snapshots: 10,000;
- invalid snapshots and mid/microprice/tick/post-only violations: zero;
- lock wait p99: 28.083 us;
- lock hold p99: 25.085 us;
- deployment preflight: pass under the staged private config.

The machine-readable identity and exact hashes are in `quote_decision_snapshot_atomicity_v2_implementation_20260804.json`.

## Boundary

Deployment requires a controlled restart and a short engineering window. The window may promote this integrity successor based on routing, clock, lock, and feed-health parity only. Short-window PnL is not a rollback or promotion gate. The later `exit_urgency_strength=0.5` versus `0` experiment remains a separate campaign-level action identity.
