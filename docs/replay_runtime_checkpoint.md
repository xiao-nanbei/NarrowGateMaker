# Replay runtime checkpoint: implementation status

The Python tick loop can pause **before** an event and save its runtime graph,
then resume that event once. A pause is not maintenance: it does not request
cancels, flatten inventory, reset a campaign, or produce final accounting.

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

This is not yet wired into the production bounded multi-window runner. It must
not be described as a completed 401-day baseline or used to combine independent
daily fresh starts. Qualification of the configured receive-time BUY/SELL policy
adapter and end-to-end source delivery across input batches is still pending.
C++ tick-loop restore and arbitrary research emitter/native object serialization
are not supplied by this interface.

Batch boundaries must retain **real lookback and lookahead context**, rather than
inventing replacement snapshots: the tested local-rank consumer needs 120 seconds
of prior trades, and fill diagnostics can read five seconds ahead. Other enabled
consumers may need longer context. Pending orders/compute can retain an earlier
book cursor. The loader must retain that context too. Only the last batch does
terminal accounting; unused intermediate end timers are not execution events.

Per-message latency draws use global source-row positions. The latency sampler's
`source_row_offset` preserves the existing full-window draws when input prefixes
are dropped; FIFO delivery clocks also need their prior state preserved. Changing
only the sampler offset does not by itself qualify end-to-end delivery rotation.

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
