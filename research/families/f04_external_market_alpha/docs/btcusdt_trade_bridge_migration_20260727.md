# BTCUSDT historical trade-bridge migration (2026-07-27)

## Decision

Historical BTCUSDT bridge states now use official Binance individual-trade 1-second bars. CryptoHFTData remains authoritative only for the BTCUSDC execution-market native snapshot/delta path. Live BTCUSDT continues to use WebSocket book ticker.

The historical visibility contract is:

1. a bar with left edge `t` summarizes trades in `[t,t+1s)`;
2. its close first becomes visible at `t+1s`;
3. `last_event_ts_ms` records the final individual trade in the bucket;
4. a zero-trade row does not refresh the source;
5. carry-forward is limited to two seconds of source age;
6. a return is called one-second only when adjacent visible timestamps differ by exactly 1,000 ms.

The active hierarchical builder therefore requires separate inputs:

```text
--binance-futures-bar-dir     official BTCUSDT 1s trade bars
--binance-execution-bbo-dir   BTCUSDC execution BBO
```

`--binance-bbo-dir` remains a deprecated alias for the execution BBO only. It never restores the old BTCUSDT BBO bridge.

## Bridge comparison

The old BBO-mid bridge and new official-individual-trade-close bridge were aligned on 128 common dates and 10,873,353 causal one-second rows:

| Diagnostic | Result |
| --- | ---: |
| absolute level difference, p50 | 0.007263 bps |
| absolute level difference, p95 | 0.008398 bps |
| absolute level difference, p99 | 0.281209 bps |
| absolute 1s-return difference, p50 | 0.000001 bps |
| absolute 1s-return difference, p95 | 0.031052 bps |
| absolute 1s-return difference, p99 | 0.500116 bps |
| 1s-return correlation | 0.985672 |

This supports the trade bridge for second-scale local innovation and the 360-second slow basis. It does not make trade prices a substitute for BTCUSDC L2, queue, cancel, refill, spread, or active-order research.

The frozen replacement inputs contain 133 daily Parquet files built from 541,876,351 individual trades into 10,956,207 one-second bars. Bar plus metadata storage is 0.427 GiB; all 133 output hashes pass. The versioned hierarchical rebuild contains 114/114 successful dates, 114 output/source hash manifests, no temporary files, and occupies 0.830 GiB.

## Date-universe audit

The pre-retirement local CryptoHFT source inventory contained:

| Source | Complete 24-hour target dates | Raw size |
| --- | ---: | ---: |
| BTCUSDC order book | 147 | 22.095 GiB |
| BTCUSDT order book | 133 | 56.565 GiB |

Removing the BTCUSDT CryptoHFT gate therefore changes the broad local source ceiling from 133 to 147 dates, not 156. Fourteen BTCUSDC-only targets exist, but only eight currently have a complete D-1 BTCUSDC warmup source. A strict native sequence audit of those eight produced:

| Result | Dates |
| --- | ---: |
| snapshot/sequence eligible | 1 |
| no usable snapshot by target start | 6 |
| target sequence gap | 1 |

The sole new strict source candidate is `2026-05-16`. It still needs official trades, spot, metrics, feature and normalized-coverage admission before it can enter a particular formal experiment. The other dates may be studied only under a separately named delta-converged identity if that estimand allows it.

Audit artifacts live under:

```text
MarketData/NarrowGate_BTCUSDC/reports/
  btcusdt_trade_bridge_migration_20260727/
```

## Coverage decision

The 133-row normalized registry has 71 sequence-valid dates. Holding the other formal conditions fixed gives 66 dates at 99% whole-day coverage and 71 at 95%. The five extra dates have 20-33 missing minutes and maximum contiguous gaps of about 10-21 minutes.

The formal whole-day threshold remains 99%. A 95% threshold is permitted only for `segment_eligible` diagnostics that reset or censor all queue, order, label, inventory and campaign state at each source gap. It must not be used to promote a whole UTC day into the action-replay denominator.

The 99% bit is not sufficient by itself. Of the current 66 formal rows, 25 have a maximum internal gap above 10 seconds, 14 above 60 seconds, and six above 300 seconds. The existing p99-gap statistic does not expose rare long outages. Continuous order/campaign studies must add a max-gap rule or segment and censor the state path at every gap.

## Storage retirement

The 56.565 GiB BTCUSDT CryptoHFT payload could be removed only after:

1. active builders and tests use the official trade bridge;
2. current reference artifacts are rebuilt with the new source identity;
3. frozen old Stage 0 artifacts retain their source hashes and are labelled as historical BBO identity;
4. no active runner reads `BTCUSDT-bbo-*` or BTCUSDT CryptoHFT raw files.

Deleting that payload does not affect live book ticker or BTCUSDC native queue research. It does remove local byte-for-byte rebuild capability for old BTCUSDT-BBO historical artifacts, so the retirement must be explicit rather than an in-place source substitution.

The gate was completed on 2026-07-27. Exactly 3,192 BTCUSDT hourly files were removed, reclaiming 56.565 GiB. Post-delete validation found zero BTCUSDT raw order-book files and preserved all 3,528 BTCUSDC files across 147 complete target dates. Free disk space increased from about 95 GiB to 152 GiB. The old 128-file BTCUSDT BBO lineage and SHA manifest remain available for frozen Stage 0 reproduction.
