# Native Exchange-Book Replay Scheduler

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

## Status

The Python authoritative tick replay now accepts the raw CryptoHFT snapshot/delta tape as a strategy-independent exchange-time state stream. This replaces the trajectory-dependent sparse watch tape as the intended queue-mechanics input.

This change does not enable a keep/cancel policy. It first establishes whether the local queue state is observable with enough coverage and ordering quality to define a new action family.

## Clock contract

The two book streams have different responsibilities:

| Stream | Clock | Responsibility |
|---|---|---|
| retained top-20 BBO/L2 | delayed policy visibility | quote features, guards, microprice inputs, policy decisions |
| native snapshot/delta | exchange transaction time | exact active-price public queue, cancel/refill path, fill mechanics |

For native messages, Binance `transaction_time` is the primary exchange timestamp and `event_time` is the fallback. CryptoHFT `received_time` remains attached as provenance. Formal reports count each timestamp source and reject receive-time or unknown fallback; they cannot silently call a receive-time substitution an exchange-time result.

An order activating at timestamp `t` may seed its queue only from native state strictly before `t`. A native update at exactly `t` is not silently placed before the order. A trade/book, activation/book, or cancel-ACK/book collision sharing one millisecond invalidates that order's exact path unless a shared ordering key exists. A read-only boundary preview marks this ambiguity without consuming or reordering native state.

## Trajectory independence

`HistoricalExchangeBookScheduler` reconstructs the complete public price-level map without receiving order IDs, inventory, campaigns, quotes, or actions. Strategy orders can:

- query one side/price tick at activation;
- consume emitted public level changes;
- record exact cancel/refill path statistics.

They cannot change which source messages are parsed or which levels are applied to the reconstructed book. The replay may filter the *notification payload* down to currently observed order prices, but every raw snapshot/delta row is still sequence-checked and applied. The same timestamp boundary therefore has the same full-book state fingerprint regardless of the strategy query trajectory.

The raw tape is parsed once. It is consumed lazily at each merged replay boundary and immediately before every execution trade; native timestamps are preserved on emitted level changes. Raw book-only events do not inflate the outer pandas event table or trigger policy decisions that could not observe them.

The old sparse watch tape remains an archived diagnostic and is mutually exclusive with native mode. Native mode also rejects the sampled top-N cancel-ahead overlay.

## Failure behavior

Formal mode is fail-fast:

- missing expected hourly files fail before replay;
- sequence gaps and source gaps invalidate the reconstructed state;
- a delta-only prefix may bootstrap the segment conservatively, but only explicitly touched levels become exact/known-zero;
- untouched prices remain unknown until a native snapshot establishes a range;
- an isolated unknown activation level does not discard the UTC day: the operational baseline continues on its delayed-book fallback;
- prices outside the snapshot range are unknown, not zero;
- same-millisecond activation/trade/book ordering is marked ambiguous;
- C++ replay rejects this input until its streaming scheduler reaches parity.

Snapshot resets invalidate already-active counterfactual queue paths. New orders may seed from the new segment after it is established.

Native support never decides whether a campaign enters the randomized action denominator. The first baseline-eligible decision entering the preregistered causal state is assigned before native seed support/status is inspected. Unknown seeds are pre-treatment support loss; snapshot resets and same-millisecond ambiguity are post-treatment censoring. Neither is removed through complete-case filtering.

## Raw-data preflight

The 2026-07-18 raw probe used 24 hours of warmup. The first valid native snapshot appeared after 110,161 pre-snapshot deltas and contained 2,000 price levels. The next 14,838 deltas were accepted with:

- zero sequence gaps;
- zero message-time reversals;
- one initialized snapshot segment;
- matching reconstructed top levels.

This establishes parser and sequence continuity on a real tape prefix. It is not an action-value result.

## Full-day equivalence and runtime

The 2026-06-05 representative day replayed 24 hours of prior-day warmup plus the complete target day. The optimized Arrow logical-message parser and active-price notification filter produced the same 18,868 order rows and the same 96-column panel as the unfiltered implementation. The only changed value was the internal segment number because conservative delta bootstrap creates an earlier segment boundary; all queue statuses, quantities, event paths, fills, and strategy outputs were identical.

| Metric | Unoptimized | Arrow parser | Filtered notifications + delta bootstrap |
|---|---:|---:|---:|
| Runtime | 2,072.27 s | 1,224.97 s | 478.22 s |
| Active-order rows | 18,868 | 18,868 | 18,868 |
| Exact queue | 7,912 | 7,912 | 7,912 |
| Known zero | 10,952 | 10,952 | 10,952 |
| Unknown | 4 | 4 | 4 |
| Same-ms ambiguous rows | 214 | 214 | 214 |
| Snapshot-reset invalidations | 2 | 2 | 2 |

The final path is about 4.3x faster than the original implementation and kept resident memory near 2.2 GiB for the representative worker. This is an engineering/data-layer result, not keep/cancel evidence.

After causal hardening, a second strict replay produced the same 18,868 rows and identical exact/known-zero/unknown and path-support counts in 541.2 seconds. Apart from the deliberate `diagnostic -> strict` label, its order-level state matched the optimized diagnostic panel.

## Strict replay command

The first formal diagnostic uses the current operational config, empirical P3 artifact, frozen q0.70 calibration identity, AWS Tokyo REST and execution-book visibility profiles, individual trades, and the retained 100 ms policy book:

```bash
python -m models.audit.local_order_value_replay \
  --days 2026-06-05 \
  --symbol BTCUSDC \
  --config docs/private/live_config.current.local.yaml \
  --strict-calibration \
  --queue-calibration-artifact <queue-v3-q070.json> \
  --execution-trade-source trades \
  --bbo-dir "$NARROWGATE_DATA_ROOT/replay_l2_retained100ms_v1/bbo" \
  --l2-dir "$NARROWGATE_DATA_ROOT/replay_l2_retained100ms_v1/l2" \
  --exchange-book-raw-root "$NARROWGATE_MARKETDATA_ROOT/cryptohftdata" \
  --exchange-book-mode strict \
  --exchange-book-warmup-hours 24 \
  --live-perf-telemetry <operator-live-perf.csv> \
  --latency-profile-id <operator-defined-latency-profile-id> \
  --exec-book-visibility-profile <operator-quote-decisions.csv> \
  --exec-book-visibility-profile-id \
    <operator-defined-visibility-profile-id> \
  --output-prefix <output-prefix>
```

The output manifest hashes the config, code checkpoint, raw hourly inputs, policy-visible BBO/L2 roots, P3/model identity, queue calibration, and latency profiles.

Formal native runners require the queue artifact explicitly. They do not silently inherit a mutable default path or ambient `MM_QUEUE_CALIBRATION_PATH`.

## Frozen native universe

The new family freezes 54 chronological target days from the 76 retained top-20/100 ms eligible days. A target enters the native universe only when all 24 target-day and 24 prior-day warmup files exist and every source day passes the sequence-gap, invalid-message, and time-reversal audit.

The immutable raw manifest covers 1,560 hourly files and 10,664,184,833 bytes. The frozen family is:

`queue_value_keep_cancel_native_exchange_v1`

The chronological roles are fixed before reading action outcomes:

- state fit: 12 days;
- state internal embargo: 1 day;
- state calibration: 5 days;
- state-to-action transition embargo: 1 day;
- action Development: 17 days;
- embargo: 1 day;
- Validation: 8 days;
- embargo: 1 day;
- sealed holdout: 8 days.

Validation and sealed-holdout outcomes remain unread until the preceding gate passes. Access requires a hash-bound decision identifying the evidence split, queue-model bundle, prior metadata, and an OPE report whose day-clustered uplift lower bound is strictly positive.

## Native observation and state calibration

The state-fit/calibration replay used 18 chronological days and produced 277,368 active-order observations. The corrected v4 label is the first native mid-price hit strictly after the decision, reconstructed from the same strategy-independent snapshot/delta stream:

- down first hit: 58,738 rows;
- no hit/flat through the horizon: 158,820 rows;
- up first hit: 59,810 rows;
- horizon-censored: 3 rows;
- exact or known-zero activation support: 99.9780%;
- complete native path support: 99.2562%;
- sequence gaps, source gaps, time reversals, receive-time fallback, and unknown timestamp fallback: zero.

The formal observation artifact is `state_fit_calibration_observations_v4_native_first_hit.panel.parquet`, SHA256 `a867fff54b03718717d7e0844325baa58b5b36a9e8742ce2b603455aebd28bc1`.

BUY and SELL models were fitted separately on 12 days, separated by one embargo day from five calibration days. The frozen bundle is `queue-value-bundle-49bd764bc5dd67e4`, SHA256 `8e08026914034f5c58d290a9801b0ff0f61069d091e3f1ae52fce3d9c418b371`. Its event contract is:

- adverse market-order intensity from public individual trades;
- cancel/refill intensity from native exact-level snapshot/delta changes;
- empirical microprice from the first native mid-price hitting event.

Empirical microprice improved multiclass Brier from `0.54230` to `0.52716` for BUY and from `0.53760` to `0.52299` for SELL. Cancel/refill calibration was modestly better than its constant-rate reference. The adverse market-order component only stayed within the preregistered calibration tolerance; its Brier score did not beat the constant reference. This is state calibration evidence, not action uplift.

## Keep/cancel Development result

The registered family used:

- `K0`: keep the active order and queue position;
- `K1`: cancel, wait for both ACK and frozen adverse-state exit, then bind the first re-entry order to the intervention;
- one intervention per campaign;
- 50/50 recorded propensity;
- BUY and SELL evaluated separately;
- no size, reducing-side, or inventory-limit change.

The one-day mechanics smoke passed: all assigned K1 actions received a cancel ACK, state exits reauthorized the linked re-entry path, and the randomized denominator retained one intervention per campaign.

Development then replayed 17 days and 1,448 campaigns: 741 K0 and 707 K1, with 711 BUY and 737 SELL interventions. All 707 K1 assignments received a cancel ACK; 328 reached the frozen state exit and submitted one linked re-entry order, of which 18 filled. Runtime queue excitation was `native_exchange_exact_level` for every intervention, and all native source integrity counters remained clean.

The strict evidence gate failed. Activation support was `1,386 / 1,448 = 95.718%`, below the frozen 98% gate. Complete native outcome support was `1,301 / 1,448 = 89.848%`; 62 rows lost their native campaign path before the intervention and 85 rows had same-millisecond ambiguity. These rows remain in the randomized denominator, so ordinary complete-case DR was blocked.

With reward clipped to `[-50, 50] USDC`, the pooled native-censoring Manski bound for `K1 - K0` was `[-10.0183, +10.0275] USDC/intervention`; its day-bootstrap lower 2.5% bound was `-11.6580`. BUY and SELL bounds also crossed zero materially.

The all-row mixed-simulator ITT is direction-only because it includes fallback or ambiguous native paths. Its pooled reward uplift was `+0.00556 USDC/intervention`, with day-clustered 95% interval `[-0.00585, +0.01745]`; BUY was `-0.00065` and SELL was `+0.01160`, and both side intervals crossed zero. K1 reduced intervention fills by about 40.9 percentage points. Across the paired daily trajectories, randomized PnL was only `+1.4769 USDC` over control while fills fell by 328; daily PnL improved on 8 of 17 days and worsened on 9.

The family therefore stops at Development. Validation and sealed holdout were not opened. No policy artifact, live configuration, baseline, or deployment changed. The formal ITT artifact is `development_v1.randomized_mixed_simulator_itt.json`, SHA256 `daea32ec0bbb3c9fba1ed56532bfe8270d3c3ebde46d3a7e467cd6790d41dcaa`.

Exchange-time queue evidence alone is diagnostic. Executable policy evidence would still require delayed policy-visible features and latency/ACK/cancel simulation. This family did not pass Development even under the favorable exchange-time information boundary, so receive-time promotion work is not justified for this K1 definition.
