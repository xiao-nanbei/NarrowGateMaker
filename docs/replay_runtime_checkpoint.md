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

The caller currently supplies the **same input window, parameters and runtime**
on resume. This is a qualification interface, not yet the bounded multi-window
runner. It must not be used to combine independently initialized daily results
or described as a completed continuous multi-day baseline. C++ tick-loop state
restore and replacing/rebasing the loaded input batch are not implemented by
this interface. It also does not make arbitrary research emitters or native
extension objects serializable.

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
