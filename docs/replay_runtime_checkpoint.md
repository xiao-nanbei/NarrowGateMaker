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
The accounting dates stay unchanged between invocations. To load a bounded
input batch, add `--runtime-input-bounds-ms START END` (inclusive milliseconds).
Only intersecting daily inputs are loaded; each is sliced before concatenation.
The first batch starts at the original accounting origin. A nonfinal batch must
save a checkpoint **before** its input end, leaving real lookahead. A resumed
batch retains the cutoff, every still-referenced pending cursor, and the required
lookback. Its final input end must reach the original accounting end before any
economic report is published. Input bounds are not new daily initial states.

Batch source identities are retained in the checkpoint and final metadata.
The start stays on the original timer grid. Batch message-count and latency
summaries describe loaded inputs, not cumulative full-run observations; the
final daily row labels this scope explicitly. Parent packet completion retains
all required child timestamps even when execution rows are cropped.
The caller still schedules these overlapping batches; automatic batch sizing
and the complete multi-source 401-day execution remain unfinished. Daily input
preparation can still transiently load a full day before slicing it.

## Python interface and input rotation

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
next input batch. The optional native cooldown hot-path object still needs state
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

These records do not carry Binance exchange sequence IDs. They therefore use `sequence_scope=provider_ordered`, with no fabricated `U/u/pu` values; strict exchange-sequence mode rejects them. A modeled diagnostic scheduler can apply them and save/restore its book, but reports a provider-ordered evidence scope even after subsequently switching to a native source. Switching providers requires a real snapshot, which replaces the prior source's levels without resetting strategy/account state. A component test and a short real-data reconstruction comparison do not establish full-calendar replay readiness; complete real provider-transition qualification remains pending.

F01 accepts `--exchange-book-source-plan` with an owner-local JSON object containing `symbol` and `days`. Each day explicitly selects either `provider=tardis` with `raw_file`, or `provider=cryptohft` with `raw_root`. Include every requested input and warmup day. This route requires Python, diagnostic book mode and diagnostic replay purpose; it cannot be combined with the implicit native root and never automatically substitutes a provider. Both providers use the same scheduler and runtime checkpoint; the plan is read once and its digest is recorded. Prepared BBO/L2 inputs are still selected separately and must use the matching source clock.

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
