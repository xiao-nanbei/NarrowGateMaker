# External reference feed dispatch soak (2026-07-11)

Last materially modified: 2026-07-27

Status: Frozen rejected-dispatcher experiment. Its production snapshot is historical, not current runtime authority.

## Scope

This is a system-engineering audit, not alpha evidence. It tests how the six Bitget/Bybit/OKX public WebSocket reference feeds reach `SignalEngine`; it does not change quotes, inventory limits, order size, guards, or the synchronous Binance order gateway. The external feeds remain shadow/reference inputs.

Target environment:

- AWS Tokyo (`ap-northeast-1`)
- `t3.medium`, 2 vCPU, 3.75 GiB, x86_64
- Amazon Linux 2023
- Python 3.9 live virtual environment
- persisted `native` compute profile
- six public WebSocket sources: Bitget, Bybit, and OKX spot/perpetual

## Original bottleneck

The original callback performed the full chain inline:

```text
receive -> JSON decode -> venue normalization -> dedupe/sort
        -> SignalEngine shared lock -> bar/global-flow update -> recorder
```

All source threads compete for the GIL and `SignalEngine._lock`. Bybit frames can contain many trades, and the old implementation took the signal lock once per trade. This explains why normal BBO latency can be small while burst-time p99 is large.

## Implemented experiment surface

The experiment added a bounded `ReferenceMessageDispatcher`, latest-value BBO mailboxes, FIFO trade/control frames, frame-level trade batching, and explicit queue telemetry. A full trade FIFO is counted and logged; it never fails silently. `SignalEngine.on_cross_agg_trade_batch()` preserves event order while taking the signal lock once per venue frame.

Two topologies were tested:

```text
A. six per-source Python consumers
callback -> source queue/mailbox -> source worker -> SignalEngine

B. one shared Python consumer
callbacks -> shared FIFO + one BBO mailbox per source -> worker -> SignalEngine
```

The finite queue budgets were:

| Queue | Capacity | Overflow behavior |
|---|---:|---|
| per-source dispatch FIFO | 4,096 frames | explicit trade drop/error telemetry |
| shared dispatch FIFO | 20,000 frames | explicit trade drop/error telemetry |
| BBO mailbox | one latest frame per source | replace old BBO and count coalesce |
| external JSONL recorder | 20,000 rows per source | explicit recorder drop telemetry |
| Binance receive-time tape | 20,000 rows | explicit recorder drop telemetry |

`HEALTH` reports dispatch depth/high-watermark/age, BBO coalescing, trade drops, handler errors, and recorder queue depth/age/drop counters. The tape preserves both `local_receive_ts_ns` and `feature_ready_ts_ns`, so moving work to a queue cannot make backlog disappear from the measurement.

## Results

The adjacent windows are operational preflights, not a causal market A/B. The regression is nevertheless too large to promote, and the queue counters provide an independent mechanism check.

### Feature-ready latency

| Path | Book feature p99 | Trade feature p99 | Decision |
|---|---:|---:|---|
| synchronous callback baseline, 600s | 0.43-6.20 ms | 0.72-27.13 ms | retained |
| six per-source workers, 600s | 32.97-105.33 ms | 43.85-546.68 ms | rejected |
| one shared worker, 420s | 17.75-45.64 ms | 747.16-1,960.82 ms | rejected |

Representative rows show why a single aggregate p99 would be misleading:

| Source/event | Sync p50/p99 | Six workers p50/p99 | Shared worker p50/p99 |
|---|---:|---:|---:|
| Bitget perp trade | 0.189 / 4.095 ms | 0.428 / 195.016 ms | 3.726 / 1,916.313 ms |
| Bybit perp trade | 0.235 / 27.130 ms | 0.996 / 123.400 ms | 3.683 / 1,957.381 ms |
| OKX perp trade | 0.092 / 0.715 ms | 0.436 / 159.297 ms | 1.702 / 1,960.821 ms |
| OKX spot book | 0.135 / 6.200 ms | 0.246 / 105.331 ms | 0.267 / 45.644 ms |

The per-source design created six additional runnable workers on a 2-vCPU VM. It moved contention from the network callbacks into worker scheduling and made both the GIL and shared signal lock more expensive.

The shared design reduced worker count but serialized all normalization and trade processing. During one burst its FIFO high-watermark reached 1,486 frames and maximum observed wait reached 4.05 seconds. Trade drops and handler errors were zero, which confirms that the bad p99 was real queueing rather than missing telemetry. A lossless queue can still be operationally stale.

## Recording and shutdown findings

Full receive-time recording was useful for the audit but was not free. During a burst, the Binance tape writer reached a 7,151-row high-watermark and 4.4-second maximum writer-queue age. After the required 3,600-second environment profile was captured, production recording was disabled while all six live reference feeds remained connected. Recording stays opt-in for a bounded audit window.

An adjacent 600-second main-loop sanity window did not show a hot-path regression after recording was disabled:

| Metric | Recording on p50/p99 | Recording off p50/p99 |
|---|---:|---:|
| `signal_compute_us` | 12.90 / 281.71 ms | 13.28 / 168.69 ms |
| `compute_quotes_us` | 0.421 / 5.713 ms | 0.420 / 3.875 ms |
| `update_orders_us` | 39.13 / 808.76 ms | 36.84 / 542.45 ms |
| `requote_total_us` | 45.96 / 989.49 ms | 41.95 / 796.61 ms |

REST new/cancel tails were mixed and the action mix differed, so these ratios are not a causal latency claim. They are a safety check supporting the simpler default: do not keep full raw recording enabled after its audit window ends.

A forced process stop also left an incomplete gzip member. Appending the next session to the same daily file made later valid members unreachable to ordinary gzip readers. `DailyJsonlRecorder` now creates a separate UTC-day/session file, so an abrupt stop can damage only its own session. The profiler tolerates an incomplete tail only when the file is demonstrably still being written; stale historical corruption remains fail-fast.

External transports now stop concurrently before the shared dispatcher is closed. This bounds shutdown by the slowest venue instead of summing six WebSocket join timeouts and avoids requiring `SIGKILL` during an ordinary restart.

## Production decision

Both Python dispatcher topologies are disabled. The current production system-engineering baseline is:

```text
external dispatch       = off
external raw recording  = off
Binance market tape     = off
external public feeds   = on, synchronous shadow/reference state
order gateway           = synchronous
native compute profile  = on
```

This does not mean the original callback architecture is ideal. It means that adding Python workers did not solve the measured bottleneck on this host. The next useful implementation boundary is narrower and more native:

1. shorten `SignalEngine._lock` ownership and avoid Python dict/deque creation under that lock;
2. aggregate reference trade frames into compact 5-10 ms per-source state;
3. update a native fixed-array global-flow state in one batch;
4. expose only a compact snapshot to the decision thread;
5. re-run the same environment-labeled soak before enabling any dispatcher.

That follow-up is now implemented. Venue trade frames cross one parallel-array ABI into a fixed-capacity native global-flow engine, and cross-market 1s bars use the same native batch. BBO and source-state records are mutated in place; no Python worker was added. On the target x86 host, complete signal ingress was about 5.9x faster for 8-trade frames and 16.8x faster for 32-trade frames. A 10.3-minute live preflight consumed 54,063 accepted trades and 441,254 BBO events with `0/0` trade/book ring overflow, no external error/reconnect/silence, and 100% native global-flow HEALTH hit. End-to-end requote p99 remained a broad process/order-lifecycle metric and did not justify a causal latency claim. Detailed implementation, parity, memory, and soak evidence is in `docs/native_global_flow_batch_soak_20260711.md`.

The Binance execution feed must remain strict FIFO with sequence/gap and stale data degradation. Latest-value BBO coalescing is acceptable for shadow reference state; silently dropping execution trades is not.

## Verification

The final local suite passed after the dispatcher, batch-parity, bounded-queue, active-gzip-tail, session-rotation, and concurrent-stop tests were added:

```text
176 passed, 4 skipped
```

The live rollback preserved two-sided orders, source freshness, zero dispatcher and recorder drops, and all existing safety controls. No strategy parameter was changed in this audit.
