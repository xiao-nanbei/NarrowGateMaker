# Quote Decision Snapshot Atomicity v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

## Status

`baseline_integrity_implemented_local_not_deployed`

This is a live/replay input-integrity repair. It is not an action experiment, does not read economic outcomes, and does not change P3, ML, spread, `exit_urgency`, inventory, cooldown, or q90 parameters.

## Defect

The live requote path previously read `SignalEngine.mid_price` and later read `SignalEngine._last_depth` again inside quote computation. A depth websocket update between those reads could combine generation N mid with generation N+1 microprice, depth kappa, imbalance, and policy metrics. The separate bookTicker BBO used by post-only routing was also read later from mutable `MakerEngine` fields.

Replay normally executes those inputs serially, so the defect was a live-only source of quote-coordinate divergence.

## Implemented Contract

Each requote now obtains one immutable `QuoteDecisionSnapshot` under the `SignalEngine` market-state lock. The snapshot binds:

- depth bids and asks;
- depth-derived mid;
- the recent depth-history cutoff view;
- execution bookTicker BBO;
- depth and bookTicker generations;
- exchange, local receive, and snapshot-capture timestamps.

The same object now owns the execution-book inputs for quote core, side-policy L2 metrics, post-fill response, post-policy spread cap, C++ live routing, state-conditioned quote projection, external fair-price shadow projection, and timeout-closing routing.

The executable decision timestamp begins after the prediction and immutable book snapshot have both been assembled. A newer websocket generation may arrive while the decision executes, but it can only affect the next requote.

## Fail-Closed Invariants

The live requote is canceled/skipped if any required invariant fails:

\[
mid = \frac{bid_1 + ask_1}{2}
\]

\[
bid_1 \le microprice \le ask_1
\]

- positive, uncrossed depth top and post-only BBO;
- nonempty normalized depth when depth pricing is active;
- positive depth generation;
- present depth exchange and receive timestamps;
- `depth_receive_ts_ns <= snapshot_capture_ts_ns`;
- no crossed or future-received bookTicker state.

An invalid snapshot cancels active orders and emits `QUOTE_SNAPSHOT_BLOCK`. Normal diagnostics emit `QUOTE_SNAPSHOT` with both BBO sources, generations, and source timestamps.

## Verification Boundary

The regression suite covers immutable generation ownership, a feed update after snapshot capture, future receive-time rejection, frozen L2-history metrics, active-order fail-closed cancellation, and the absence of mutable `_last_depth` reads in `_compute_quotes()`.

Existing Python/C++ quote-core parity remains authoritative for quote math. This repair changes only the live input adapter; no C++ formula or economic parameter changed.

Implementation identities after the verified local run:

- `strategy/signal.py`: `ce992655d32855dcccb979173ad00ab9514931cda2f31eed7bea43a111491420`
- `strategy/maker_engine.py`: `f94ff21810579a77f2fc98bd86a58b2edfbf11366408bbdfceba0ead00cc8901`
- `tests/test_quote_decision_snapshot_atomicity.py`: `26e87ad4f2a20ff0cfbc214eea38197459b936ba466d1ef90be8589c5d071e6f`

Verification: `1590 passed, 4 skipped` with the project Python 3.12 virtual environment on 2026-08-03.

The repair is not yet deployed to EC2. Live restart, shadow coordinate parity, and production identity binding remain separate operational steps.

## Non-Claims

- It does not show that the race caused the 360-hour loss.
- It does not validate or invalidate `exit_urgency=0.5`.
- It does not authorize a P3 mapping, q90 action, or new quote action.
- It does not convert historical replay into independent economic evidence.
