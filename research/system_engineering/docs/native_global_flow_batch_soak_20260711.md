# Native fixed-array global-flow batch (2026-07-11)

Last materially modified: 2026-07-27

Status: Frozen implementation/soak evidence for the recorded host; current runtime authority remains the deployed config and HEALTH identity.

## Scope

This is system-execution engineering, not alpha evidence. It replaces Python per-trade object work in the six external reference callbacks with one compact frame update. It does not change quote policy, order size, inventory limits, guards, replace thresholds, or the synchronous order gateway. No Python worker was added.

Target live environment:

- AWS Tokyo (`ap-northeast-1`)
- `t3.medium`, 2 vCPU, 3.75 GiB, x86_64
- Amazon Linux 2023, CPython 3.9
- six public WebSocket sources: Bitget, Bybit, and OKX spot/perpetual
- persisted strict native profile

## Implementation

The old synchronous callback retained frame order, but still materialized one trade dictionary and one global-flow dataclass per event. It also updated market-source dictionaries and cross-market bars under `SignalEngine._lock`.

The new boundary is:

```text
venue frame
  -> parallel timestamp/price/size/maker arrays
  -> NativeGlobalFlowEngine.on_trade_batch()  # GIL released
  -> SignalEngine._lock once
       -> mutate the existing source-state record in place
       -> TradeBarAggregator.update_batch()    # GIL released
```

BBO updates use the same split: native L1 OFI/depletion/refill is updated before the Python state lock, while the latest ticker and same-second history record are mutated in place. `dict.setdefault(..., deque(...))` was removed from the high-frequency paths because it constructed a throwaway deque even when the key already existed.

`NativeGlobalFlowEngine` owns 16 fixed market slots. Each slot has a 32,768 trade ring and an 8,192 book ring. It performs no per-event allocation after construction. Capacity exhaustion is fail-fast; ring overwrites, stale events, and out-of-order events are explicit HEALTH counters. The current live source set uses 11/16 slots.

## Parity and contract tests

The same receive-time tape is fed to Python and native engines and compared field by field, including:

- aggressive buy/sell volume and trade imbalance;
- L1 OFI, depletion/refill, mid movement, freshness, and gap flags;
- 2-of-3 spot/perpetual consensus state;
- stale and out-of-order rejection;
- cross-second bar rollover and gap-bar generation;
- fixed-ring overflow reporting;
- strict runtime ABI preflight.

The complete local suite passed:

```text
176 passed, 4 skipped
```

## Target-host benchmark

`bench/bench_global_flow_batch.py` was run on the live x86 host with 1,000 frames and five rounds. The signal-ingress rows include NumPy normalization, source-state mutation, bar rollover, and global-flow ingestion.

| frame size | Python signal ingress | Native signal ingress | speedup |
|---:|---:|---:|---:|
| 1 trade | 22.17 us/frame | 13.17 us/frame | 1.68x |
| 8 trades | 82.73 us/frame | 14.07 us/frame | 5.88x |
| 32 trades | 311.25 us/frame | 18.55 us/frame | 16.78x |

The pure global-flow core scales from 1.61x at one trade/frame to about 39x at 32 trades/frame. The smaller end-to-end speedup is the relevant number: array normalization and Python source-state bookkeeping remain. This optimization is therefore aimed at burst tails, not at claiming every scalar callback is an order of magnitude faster.

## Live preflight

A 10.3-minute marker window ran with the unchanged strategy and synchronous order gateway:

| check | result |
|---|---:|
| native global-flow HEALTH hit | 100% (11 samples) |
| trade events seen / accepted | 55,254 / 54,063 |
| book events | 441,254 |
| out-of-order / stale counter delta | 8 / 1,188 |
| trade/book fixed-ring overflow | 0 / 0 |
| external errors / reconnect / stream silence | 0 / 0 / 0 |
| strict native fallback | none |

Old trades delivered in a later venue frame are rejected by exchange-event age instead of entering the 10-500 ms flow state. Their counter is evidence that the guard was exercised, not a dropped-event success metric.

The main-loop p99 was still dominated by broader process and order-lifecycle work: requote was about 852 ms, update-orders 465 ms, signal compute 324 ms, and quote compute 13.9 ms. This adjacent market window is not a causal A/B and does not prove an end-to-end p99 improvement. It proves deployment health, fixed-capacity headroom, and absence of silent overflow. REST/order lifecycle, VM scheduling, model work, and logging remain separate tail sources.

Process RSS moved from about 217 MiB before restart to about 249 MiB after the native process had warmed up. The difference includes restart/warmup state and is consistent with the roughly 30 MiB fixed-ring budget; it is not attributed entirely to the new object.

## Decision

Keep `NARROWGATE_CPP_GLOBAL_FLOW=1` in the strict native profile. Keep both Python dispatcher topologies disabled and do not add callback workers. Keep raw receive-time recording opt-in. Promotion here means a safer and cheaper shadow reference path; it does not promote global flow into quote policy.

The final two Python get-or-create micro-optimizations were synced while a live inventory campaign was active. They are on disk for the next safe restart; the running process was not restarted merely to load them because that would reset campaign/cooldown state.
