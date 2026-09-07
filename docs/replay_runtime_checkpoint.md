# Replay runtime checkpoint: implementation status

[简体中文](replay_runtime_checkpoint.zh-CN.md)

Last materially modified: 2026-09-08
Last materially synchronized: 2026-09-08

The Python tick loop can pause **before** an event and save its runtime graph,
then resume that event once. A pause is not maintenance: it does not request
cancels, flatten inventory, reset a campaign, or produce final accounting.

## F01 command-line checkpoint

The existing F01 campaign runner accepts the same-window interface for one
continuous Python arm. Append these flags to an existing, otherwise unchanged
invocation (the timestamp below is only an example; choose it inside your window):

```bash
--continuous --engine python --workers 1 --arms baseline \
--checkpoint-at-ts-ms 1767229200000 \
--save-runtime-checkpoint /private/run/b0.runtime.pickle
```

It atomically writes the state and prints `status=checkpoint_saved` with
`completed=false`. It does **not** write partial campaign/PnL/funding reports or
declare a complete replay. To continue, keep the same dates, data, parameters and
runtime, remove the two checkpoint-output flags and append:

```bash
--resume-runtime-checkpoint /private/run/b0.runtime.pickle
```

You may supply another later cutoff/output path to save again. Funding and
campaign finalization run once after the resumed replay actually finishes.

For automatic bounded execution, use `--runtime-batch-seconds 21600 --runtime-batch-context-seconds 300 --save-runtime-checkpoint /private/run/b0.runtime.pickle` instead of manual cut/input-bound flags. These are example resource settings, not strategy parameters. After each durable save, the process replaces itself and resumes the same checkpoint with the next overlapping input batch; no parent retains the old arrays and no parallel arm is created. Keep a single owner for the checkpoint path and use a frozen source/runtime directory throughout. The final batch alone publishes accounting. On a failed batch, the last successfully saved checkpoint remains available; restart with the same automatic options plus `--resume-runtime-checkpoint` pointing to that file. Do not resume an already completed result as a new experiment. The caller still chooses batch size and enough real context for enabled consumers; this is not adaptive memory sizing or a waiver for missing inputs. A real ten-minute cold-start run passed two automatic process replacements at minutes four and eight, matching uninterrupted decisions, quotes, fills, campaigns and funding exactly; this does not qualify every date or provider boundary.

An additional real one-hour native-book diagnostic crossed midnight with an automatic save at midnight. All 1,768 decisions, 649 quotes, 32 fills, 12 campaigns, funding records and aggregate accounting matched uninterrupted execution. Only execution time and explicitly labelled loaded-batch metadata differed. These diagnostic artifacts remain in the private evidence store, not distributed with the public repository; this is checkpoint equivalence, not historical live economic exactness.

The accounting dates stay unchanged between invocations. To load a bounded
input batch, add `--runtime-input-bounds-ms START END` (inclusive milliseconds).
Only intersecting daily inputs are loaded; each is sliced before concatenation.
The first batch starts at the original accounting origin. A nonfinal batch must
save a checkpoint **before** its input end, leaving real lookahead. A resumed
batch retains the cutoff, every still-referenced pending cursor, and the required
lookback. Its final input end must reach the original accounting end before any
economic report is published. Input bounds are not new daily initial states.

For a bounded qualification spanning midnight, F01 also accepts `--replay-start-ts-ms START --replay-end-ts-ms END --continuous`. The inclusive start must lie in the first supplied UTC day and the inclusive end in the last. The experimental account starts at that declared origin; this does not restore historical live inventory. Full source/pre-roll inputs remain available, funding outside the declared window is excluded, and partial-window reports never count the result as complete UTC days. Keep this original start/end unchanged across checkpoint batches; `--runtime-input-bounds-ms` only rotates loaded data inside it. Omitting the start preserves the existing midnight-origin behavior.

Execution rows are cropped at the declared origin, with parent-message child indices rebased to that actual slice; source/pre-roll rows do not become earlier simulated fills. Day-specific quality eligibility may differ within one continuous window: the combined eligibility is true only if every component explicitly passes, and `book_quality_by_day` preserves each original result. This does not waive source/version compatibility or promote an ineligible day.

Batch source identities are retained in the checkpoint and final metadata.
The start stays on the original timer grid. Batch message-count and latency
summaries describe loaded inputs, not cumulative full-run observations; the
final daily row labels this scope explicitly. Parent packet completion retains
all required child timestamps even when execution rows are cropped.
The caller selects batch duration and context; adaptive memory-based sizing
and the complete multi-source 401-day execution remain unfinished. Daily input
preparation can still transiently load a full day before slicing it.

## Python interface and input rotation

For a declared empty signal startup rather than historical REST prefill, F01 accepts `--signal-cold-start` with continuous fresh-start Python replay and `--runtime-compute-clock prediction_delivery`. It starts with no computed prediction watermark and waits for the live shared minimum of 300 completed one-second signal bars. Completion follows delivered aggregate-trade callbacks, including the intervening no-trade bars that live generates; it is neither 300 sparse file rows nor 300 elapsed wall seconds. Pending exchange/private events and the existing safety clock still advance. This does not invent fresh order-book observations in a data gap.

The first source bucket, completed-bar count, normal requote clock, warmup status and later computed bucket persist through the runtime checkpoint, including a cut before warmup completes. Keep the cold-start flag unchanged on resume; restored state takes precedence over empty initialization. Output identifies this startup mode and the main-loop time that observed warmup completion. Before the first actual price, an empty experimental account may omit unpriced, non-trading timer rows; its declared process/accounting origin stays unchanged, no future price is borrowed, and reconciliation/system events cannot be omitted. The first priced event is reported separately. Synthetic save/load tests cover both sides of warmup and removal of unused pre-warmup prediction rows. A real first-calendar-day ten-minute diagnostic also matches across a four-minute cut and cropped-input restore. It does not recreate an actual historical live startup or qualify full feature-DAG equivalence; first catch-up compute still uses the declared measured catch-up stratum rather than claiming a separately measured cold-start cost. This mode is not implemented by the C++ tick loop.

```python
from models.backtest_tick import simulate_tick
from models.replay.runtime_checkpoint_io import (
    save_runtime_checkpoint,
    load_trusted_runtime_checkpoint,
)

partial = simulate_tick(
    trades, variance_times, variance_values, params,
    bbo_data=bbo, checkpoint_at_ts_ms=cut_ms,
)
if partial.get("completed") is False:
    save_runtime_checkpoint("results/runtime.pickle", partial["_replay_checkpoint"])
    result = simulate_tick(
        trades, variance_times, variance_values, params, bbo_data=bbo,
        resume_checkpoint=load_trusted_runtime_checkpoint("results/runtime.pickle"),
    )
```

By default the caller supplies the **same input window, parameters and runtime**
on resume. The Python qualification interface also accepts
`resume_input_batch=True` with an overlapping next input window: consumed input
prefixes are discarded, active array cursors are translated, and accumulated
accounting and order state are preserved. Native book files are reopened at the
saved within-file read cursor, retaining prefetched messages and book state.
Supply complete files containing that cursor, not arbitrary sliced iterators.

The F01 input-bounds interface uses this rotation mechanism. It must not be
described as a completed 401-day baseline or used to combine independent daily
fresh starts. The configured Python receive-time BUY/SELL adapter now has
stateful save/restore and rotation tests, including an actual replay fill that
updates cooldown. Process locks are recreated; protected EMA, pending window,
cooldown and counters are retained. Every undelivered callback must remain in the
next input batch. Before discarding an old prefix, the adapter can fold lazy depth callbacks strictly before the saved next event into the existing EMA in source order, without evaluating a policy or creating a fill. Equal-time and future callbacks cannot be discarded. A real cross-midnight bounded comparison also matched the uninterrupted decision, quote, fill, campaign and funding tables exactly; no-fill intervals before the cut are covered by the regression tests. The optional native cooldown hot-path object still needs state
export; it is rejected rather than replaced with fresh Python state. The full
private B0 configuration has passed a representative one-hour comparison for
both same-window file resume and actual input cropping. Decisions, quotes,
fills, campaign accounting and funding matched the uninterrupted reference;
wall time and loaded-input summary counts are not equality claims. This does
not qualify every date, source-provider transition or optional policy.
C++ tick-loop restore and arbitrary research emitter/native object serialization
are not supplied by this interface.

### Source-aware raw L2 restoration

`TardisExchangeBookTape` in [the existing book scheduler](../models/exchange_book_replay.py) reads explicitly supplied raw Tardis L2 files. It groups a whole snapshot or update atomically across CSV reader batches, preserves exchange and provider-receive timestamps, and uses the same book reconstruction implementation. Provider timestamps are not deployment-host latency measurements. The component rejects malformed levels and regressing source clocks rather than reordering records or inventing missing events.

These records do not carry Binance exchange sequence IDs. They therefore use `sequence_scope=provider_ordered`, with no fabricated `U/u/pu` values; strict exchange-sequence mode rejects them. A modeled diagnostic scheduler can apply them and save/restore its book, but reports a provider-ordered evidence scope even after subsequently switching to a native source. By default, switching providers requires a real snapshot, which replaces the prior source's levels without resetting strategy/account state. A component test and a short real-data reconstruction comparison do not establish full-calendar account replay readiness.

F01 accepts `--exchange-book-source-plan` with an owner-local JSON object containing `symbol` and `days`. Each day explicitly selects either `provider=tardis` with `raw_file`, or `provider=cryptohft` with `raw_root`. Include every requested input and warmup day. This route requires Python, diagnostic book mode and diagnostic replay purpose; it cannot be combined with the implicit native root and never automatically substitutes a provider. Both providers use the same scheduler and runtime checkpoint; the plan is read once and its digest is recorded. Prepared BBO/L2 inputs are still selected separately and must use the matching source clock.

Daily files can overlap in exchange time. If the next file opens with a real snapshot, the explicit plan hands over at that snapshot's original exchange timestamp, excluding the old file's overlapping tail. It keeps at most two streaming readers open and never changes a raw timestamp. If the next same-provider file starts with deltas instead, those updates are preserved and ordinary sequence/time checks still apply; a provider change without a snapshot is rejected by default. Input batches must include the following file before consuming such a handover, rather than discovering an earlier snapshot after already checkpointing the overlap. This source-selection rule must also match the prepared BBO/L2 inputs; book-component equality alone does not qualify account replay across a provider change.

A diagnostic plan may explicitly set `handover: invalidate_then_delta_bootstrap` on an incoming CryptoHFT day. At its first actual event time, the plan excludes the old source's overlapping tail and emits a `source_gap` before applying new deltas. This clears old book levels and invalidates prior queue evidence; it does not invent a snapshot, transfer undocumented exchange sequence continuity, or reset the account. The existing non-strict delta-bootstrap behavior then reconstructs only observed levels. The gap remains in source counters and checkpoint cursors, including when the next input batch starts on that day. Strict mode still rejects the gap. Missing levels, convergence and queue uncertainty remain limitations, not zero-depth truth or an exact-live claim. Use this route only as an explicitly modeled source discontinuity, and compare its complete account path before admitting the full-period baseline.

Source exclusions must follow the input they describe. The reference-market feature loader does not discard an available official-trade bar merely because that symbol/day has a historical CryptoHFT book exclusion. The reference BBO loader retains its book checks. This does not invent a missing bar, qualify an invalid book, or rewrite previously generated features; use new feature outputs when changing a source-selection environment.

The existing Tardis normalizer accepts `--timestamp-source exchange` to construct a separate `normalized_tardis_l2_exchange_100ms_v1` product. Its rows include only raw exchange events strictly before the 100ms right boundary. The clock sidecar retains original provider receive timestamps but reports `exchange_resample_age_us`, not provider visibility age. Historical provider transport is not added to current-host sampled delivery. The default provider-clock product is unchanged; even `--force` cannot change the clock of an existing day's product in place. Raw clock regressions are not silently sorted. This changes the prepared input environment, so old frozen results remain tied to their original products.

Batch boundaries must retain **real lookback and lookahead context**, rather than
inventing replacement snapshots: the tested local-rank consumer needs 120 seconds
of prior trades, and fill diagnostics can read five seconds ahead. Other enabled
consumers may need longer context. Pending orders/compute can retain an earlier
book cursor. The loader must retain that context too. Only the last batch does
terminal accounting; unused intermediate end timers are not execution events.

Per-message latency draws use global source-row positions. The latency sampler's
`source_row_offset` preserves the existing full-window draws when input prefixes
are dropped. `execution_message_delivery_params(..., prior_delivery=previous)`
also continues each feed's callback backlog from its prior completion clock and
checks overlapping messages retain their delivery times. Runtime tests cover
both ordinary and long callback-service cases. Source inventory/diagnostic
metadata describes the loaded batch; that metadata is not a full-run cumulative
coverage report. The finalizer must retain the per-batch source records.

The persisted object graph retains shared order references, pending new/cancel
and private-fill events, FIFO/HTTP and compute phases, policy state, random
generators, counters and traces. Native book input iterators are reopened at
their unread source cursor; prefetched events remain in the saved scheduler.
Operational progress callbacks are supplied by the resumed process, not saved.
Existing drained `ContinuousReplayState` is a different, deliberately partial
accounting checkpoint and is not substituted for this graph.

Files are written to a private temporary file, flushed, synced, atomically
replaced and followed by a directory sync. Failed serialization does not replace
the previous complete checkpoint. These are trusted local Python pickle files:
**never load a checkpoint from an untrusted party or expose pickle import through
Studio**. They are not portable exchange artifacts or a substitute for the run's
existing input/config manifest.

`tests/test_tick_runtime_checkpoint.py` compares complete results (including NaN
fields) after file round-trips and repeated cuts, including async close, pending
fills, source delivery, book lookahead and compute delays. Synthetic equality
qualifies these paths; it is not evidence of economic value or a complete test of
every private policy configuration.

`tests/test_tick_runtime_input_window.py` additionally compares complete outputs
after real array cropping, multiple window replacements, variance/prediction and
L2 cursor translation, delayed fill callbacks, and native raw-file rotation.
