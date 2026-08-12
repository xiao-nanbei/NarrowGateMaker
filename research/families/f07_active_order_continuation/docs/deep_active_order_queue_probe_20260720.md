# Deep Active-Order Queue Probe

Last materially modified: 2026-08-12

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Decision

The retained top-20 L2 container is adequate for quote-core state, but it is not an identified queue input for orders hundreds of ticks from BBO.

The existing `queue_value_cancel_reenter_v3` result remains a valid Development failure and must not be promoted. Its queue-state coefficients, thresholds, and action-level numerical estimates are superseded for mechanism interpretation. Validation and sealed holdout remain unread.

The next queue-value experiment is blocked on a strict sparse active-order queue tape. No live, config, C++, baseline, or strategy action changed.

## Frozen diagnostic identity

The comparison used:

- date: `2026-06-05`;
- current operational empirical P3 artifact;
- `delta_star=13.99908598`, `effective_kappa=0.06743811`;
- queue calibration v3 `q0.70`;
- individual Binance trades;
- merged 100 ms replay clock;
- AWS Tokyo EC2 REST latency profile;
- AWS Tokyo EC2 execution-book visibility profile;
- Python authoritative replay;
- external markets disabled.

The deep source was reconstructed from the previous UTC day's 24 hourly files as causal warmup plus all 24 target-day files. The builder waited for a native snapshot and then enforced strict `U/u/pu` continuity; it did not bootstrap from deltas. Its top 20 matched the retained container at the touch for every common row and at level 20 for more than `99.996%` of common rows.

## Full-day replay comparison

| Metric | Retained top-20 | Native deep-250 |
|---|---:|---:|
| Active-order rows | 19,378 | 19,042 |
| Placed orders | 1,628 | 2,098 |
| Fills | 1,595 | 2,062 |
| Campaigns | 669 | 895 |
| Median initialized queue | 0.116182 BTC | 0 BTC |
| Zero initialized queue | 0.083% | 54.732% |
| Favorable fills at 1 s | 6 | 7 |
| Adverse fills at 1 s | 15 | 8 |

Only two decision IDs overlapped. This is not a small queue metric drift: a different seed queue changes fills, inventory, campaign state, and all later decisions.

## Eligible-entry coverage

The old v3 panel had 52 eligible BUY exposure-increasing add entries on this day. At the same deterministic EC2 visibility time:

| Check | Count | Rate |
|---|---:|---:|
| Deep touch equals action-panel touch | 52 / 52 | 100% |
| Active price lies inside deep-250 range | 48 / 52 | 92.31% |
| Active price has nonzero public quantity | 19 / 52 | 36.54% |
| Active price is a valid known-zero level | 29 / 52 | 55.77% |
| Active price lies outside deep-250 range | 4 / 52 | 7.69% |

`36.54%` is not the queue coverage rate. A price inside the reconstructed book range but absent from the book is exact evidence of zero visible queue. The strict coverage rate is therefore `92.31%`; only the four out-of-range prices are unresolved.

For the 19 nonzero exact levels, the old initialized queue divided by visible deep quantity had median `3.97` and p90 `69.18`. The q0.70 fallback is not a portable substitute for exact active-price depth.

## Sparse tape audit

The implemented builder reads the native snapshot/delta stream once and emits only the watched active-price levels. A cold same-day build was rejected: the first target-day native snapshot does not arrive until `13:39 UTC`, so a same-day-only run understated coverage. The accepted builds use 24 hours of prior-day warmup.

| Discovery trajectory | Watches | Exact | Known zero | Unknown | Ambiguous | Strict usable |
|---|---:|---:|---:|---:|---:|---:|
| Retained top-20 | 19,379 | 7,971 | 11,399 | 9 | 3 | 99.938% |
| Native deep-250 | 19,045 | 7,825 | 11,209 | 11 | 6 | 99.911% |

Both accepted builds processed `5,879,342` logical messages across 11 native snapshot segments with zero sequence gaps, invalid sequence messages, source gaps, or time reversals. The aggregate exact-plus-known-zero gate exceeds `99.94%`, but strict usable coverage excludes same-millisecond ambiguity. The remaining unknown rows lie outside the native snapshot range; they are not silently treated as zero.

The generated `level_events.parquet` records exact updates and deletes during each order lifetime. Current replay integration is deliberately `seed_only_v1`: it replaces activation queue seeds, records all misses, rejects dense top-N cancel-ahead mixing, and leaves sparse cancel/refill-ahead events disabled until cross-stream same-millisecond ordering is implemented.

## Trajectory fixed-point audit

Queue seed changes alter fills, inventory, and every later quote, so a sparse tape built from one trajectory cannot be assumed to cover the next one. Closure therefore uses the exact market identity `(side, price_tick, activate_ts_ms)`; replay-local `order_id` is never used across generations.

The first sparse-seed replay retained only `6,972 / 19,045` discovery identities (`36.61%`). It then encountered 12,098 new identities and seven unusable shared seeds. After rebuilding the tape on that new trajectory, the second replay retained `15,143 / 19,071` identities (`79.40%`) and exposed 3,929 further identities plus 14 unusable seeds. Fills remained `2,086` and campaigns `943` in both sparse generations, but those values are diagnostic mechanism counters, not strategy evidence.

The predeclared final generation did not close:

| Transition | Exact identity overlap | Retention of prior trajectory | New identities |
|---|---:|---:|---:|
| g0 -> g1 | 6,972 | 36.61% | 12,099 |
| g1 -> g2 | 15,143 | 79.40% | 3,930 |
| g2 -> g3 | 16,206 | 84.97% | 2,799 |

Generation 3 still required 2,799 fitted/deep fallback seeds and had 14 unusable sparse seeds. Its fills/campaigns also changed from `2,086 / 943` to `2,072 / 937`. The formal closure artifact is `active_order_queue_closure_20260605_g0_g3.json`; it records every manifest and daily-summary hash and has `closed=false`, `promotion_status=diagnostic_only`.

This is the stop result. The watch-specific two-pass design is useful for diagnosing depth coverage, but it is not a self-consistent formal replay input: the queue tape changes the strategy trajectory, which creates a new set of watched levels. It will not be iterated until a convenient path appears.

The replacement design had to be trajectory independent:

1. Merge the native snapshot/delta stream directly into the replay scheduler.
2. Maintain an exchange-time price-level map independently of which orders the strategy happens to place.
3. Query that map when any passive order activates; do not pre-enumerate watch identities.
4. Keep receive-time top-20 state authoritative for quote features and exchange-time native state authoritative for queue/fill mechanics.
5. Reject or sensitivity-bracket orders outside the native snapshot range and same-millisecond trade/L2 events without shared ordering.

Only after that engine quantifies native seed coverage and path censoring may exact-level cancel/refill events and a new queue-value action family be evaluated. Hidden support cannot select the intervention denominator.

This replacement is now implemented in the Python authoritative replay as `HistoricalExchangeBookScheduler`. It consumes the complete native snapshot/delta tape, reconstructs every public price level independently of strategy orders, and exposes exact/known-zero/outside-range activation lookups. Sequence gaps, snapshot resets, and same-millisecond ordering ambiguity invalidate affected order paths. See `native_exchange_book_replay_scheduler_20260720.md`.

The representative strict native full-day data gate now passes. The 2026-06-05 panel contains 18,868 active-order rows: 7,912 exact, 10,952 known-zero, and four unknown activation levels. The 214 same-millisecond ambiguous rows and two snapshot-reset invalidations remain explicit path censoring. The optimized replay completes in 478.22 seconds, versus 2,072.27 seconds for the first implementation, with identical strategy and queue outputs apart from an internal segment-number relabel.

The causally hardened strict rerun kept all counts unchanged and completed in 541.2 seconds. Native exact/known-zero support no longer selects the randomized denominator. The first baseline-eligible decision entering the frozen causal state is retained; unknown seeds are pre-treatment support loss, while later ambiguity/reset remains post-treatment censoring.

This passes only the scheduler/data-layer gate. The old sparse-generation results and old K0/K1 thresholds remain closed. A new family, `queue_value_keep_cancel_native_exchange_v1`, has a separately frozen 54-day native-complete universe.

That family has now completed its preregistered Development stage. The state-dependent Hawkes/empirical-microprice bundle calibrated on the frozen state panel, and K1 mechanics correctly executed cancel ACK, state exit, linked re-entry, queue reset, latency, fills, and later campaign paths. However, activation support was only `95.718%` and complete native outcome support was `89.848%`. The strict censoring-aware `K1 - K0` reward interval was approximately `[-10.02,+10.03] USDC/intervention`. The direction-only all-row ITT point estimate was `+0.00556`, but its day-clustered interval crossed zero and BUY was negative.

This closes the native-exchange K1 definition at Development. Validation and sealed holdout remain unread, and no live action was enabled. Full evidence and artifact hashes are recorded in `native_exchange_book_replay_scheduler_20260720.md`.

## Data design

Building a wide deep-250 100 ms book for every retained day is wasteful: one full day consumed roughly 1 GiB, while most levels are never used by an active order. The rejected sparse design used two passes:

1. Run the fixed baseline with top-20 L2 only to discover every active order's side, integer price tick, activation time, and stop time.
2. Replay raw native snapshot/delta messages and retain only those watched price levels, together with continuity and ambiguity status.

Top-20/BBO remains authoritative for quote-core features. The sparse tape is authoritative only for:

- queue seed at activation;
- exact-level cancel, refill, and depletion;
- queue consumption and queue reset;
- whether the level is `known_zero`, `observed_nonzero`, `outside_range`, `sequence_gap`, or `same_ms_ambiguous`.

Queue and fill ordering uses exchange time. Policy visibility uses the frozen receive/feature delay. Same-millisecond trade/book events without a shared sequence must not be ordered favorably.

The native scheduler supersedes this watch-specific storage design. It parses the raw stream directly during replay and maintains the full price-level map in memory, so no order trajectory is needed to choose retained levels.

## Promotion gate for the data layer

Before freezing another action family:

- exact plus known-zero queue coverage must be at least 98%;
- sequence gaps and time reversals invalidate affected intervals;
- adjacent discovery trajectories must reach exact market-identity closure;
- formal replay must report every top-20/fitted fallback row and keep it out of ordinary native-exact OPE without deleting it from the randomized panel;
- sparse seeds and level changes must match deep reconstruction on representative days;
- exact-level sparse cancel/refill updates need an explicit trade/L2 same-millisecond ordering contract;
- top-20, deep, and sparse quote-core state must remain identical before the first queue-dependent divergence.

Only after this gate passes may a new 50/50 keep/cancel family be registered and evaluated on Development. K1 waits for both cancel ACK and adverse-state exit, and its first re-entry order stays linked to the original intervention. K0/K1 state exit uses the same action-invariant policy-visible shadow queue. The v1 ideal-information state additionally consumes strategy-independent native exact-level cancel/refill excitation, using the same event definition as its Hawkes fit. It is therefore exchange-time diagnostic evidence, not current-live promotion evidence. Development failed before any receive-time promotion gate was relevant. Validation and sealed holdout remain locked.

## Artifacts

Diagnostic artifacts are under:

`${NARROWGATE_RETIRED_DATA_ROOT}/reports/deep_active_order_l2_probe_20260720`

The original sparse full-day comparison is `top20_vs_deep250_20260605_summary.json`; eligible-entry attribution is `action_entry_coverage_20260605.csv`; fixed-point closure is `active_order_queue_closure_20260605_g0_g3.json`.

Native scheduler diagnostics are under:

`${NARROWGATE_RETIRED_DATA_ROOT}/reports/native_exchange_book_scheduler_20260720`

The frozen new-family identity is under:

`${NARROWGATE_RETIRED_DATA_ROOT}/reports/queue_value_keep_cancel_native_exchange_v1_20260720`
