# MarketData good-day alignment audit (2026-07-20)

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Current status (2026-07-27): this document records the universe observed on 2026-07-20; its candidate-day counts and feature-bundle tables are not the current research universe. New work must use the strict normalized/native manifests and predecessor-day initialization contract in `docs/native_strict_universe_20260724.md` and `docs/normalized_l2_100ms_v2_20260725.md`. The conceptual requirement to intersect independent source identities, rather than recursively glob dates, remains authoritative.

## Scope

This audit separates four date identities that must not be treated as one recursive-glob universe:

1. Binance local market-data availability.
2. Frozen model-feature bundles.
3. Historical external-venue evidence.
4. Strict event-L2 and receive-time evidence.

The previous local manifest contained 122 dates through 2026-07-14. Adding 2026-07-15 through 2026-07-18 produces 126 candidate dates.

## Corrected local universe

`2026-07-13` was initially reconstructed with only one hour of warmup, which missed the native snapshot recorded at `2026-07-12 22:26:49.805 UTC`. The last 2026-07-12 update has `u=11031977853595`; the first 2026-07-13 update has the identical `pu=11031977853595`. Replaying that exact cross-day chain with 24 hours of warmup produces 846,678 top-20/100ms states from 88ms after midnight and raises fresh coverage from 70.98% to 98.54%.

The source still has no events between `06:36:54.536` and `06:58:00.377 UTC`. Therefore 2026-07-13 passes the existing >=90% **minimal local replay** gate, but it is not gap-free strict event-L2 evidence.

The corrected local market-data manifest contains 126 dates:

```text
${NARROWGATE_DATA_ROOT}/reports/good_day_alignment_20260720/
  minimal_complete_local_replay_marketdata_good_days_2026_through_2026-07-18.csv
```

Its SHA256 is:

```text
3f76f3f7635cf980e37c2a9010237a7166edd67ca31635d4b908112ce4bdbf1e
```

The older 122-day manifest is retained as an immutable historical experiment identity. Its old 2026-07-13 replay artifacts remain invalid because they were generated from the partial pre-repair L2 file.

## Alignment result

| Layer | Result |
| --- | --- |
| Futures aggTrades, both symbols | 126/126 candidate dates |
| Futures individual trades, both symbols | 126/126; 652,894,526 rows; no empty file, duplicate day, repeated adjacent ID, or non-monotonic ID |
| Spot aggTrades, both symbols | 126/126 candidate dates |
| Futures and spot 1s bars, both symbols | 126/126 candidate dates |
| BBO and L2, both symbols | 126/126 candidate dates; row counts and timestamp bounds agree within each symbol/day |
| Futures metrics, both symbols | repaired to 126/126 candidate dates |
| CryptoHFTData BTCUSDC | 126/126 dates with 24 hourly objects |
| Taker-tempo sidecars, both symbols | repaired to exactly 126/126 formal dates |

No zero-byte files or `.part`, `.partial`, `.tmp`, ZIP, or checksum residue remain in the audited local source/normalized/sidecar directories.

The metrics audit also found that Binance had revised 13 daily archives after the local copies were first downloaded:

- BTCUSDC: 2026-06-12 and 2026-07-03 through 2026-07-11.
- BTCUSDT: 2026-06-12, 2026-07-03, and 2026-07-04.

Those files were replaced with their current checksum-verified archives. Binance metrics history contains both interval-end timestamps (`00:05` through next-day `00:00`) and interval-start timestamps (`00:00` through `23:55`). `features.preprocess_metrics` now normalizes both source conventions to causal feature-ready time. All 252 normalized daily files have exactly 288 unique, monotonic rows from `00:05` through next-day `00:00`. Four official source files contain one timestamp delayed by one or two seconds; that jitter is preserved rather than snapped backward, so downstream 10-second joins remain causal. Frozen feature bundles were not rewritten and retain their original experiment hashes.

## Remaining identity boundaries

The root `bbo/` and `l2/` directories are date-complete but are not one homogeneous L2 dataset:

| Symbol | Identity | Dates |
| --- | ---: | ---: |
| BTCUSDC | top-10, about 1s | 118 |
| BTCUSDC | top-20, about 1s | 2 |
| BTCUSDC | top-20, about 100ms | 6 |
| BTCUSDT | top-10, about 1s | 114 |
| BTCUSDT | top-20, about 1s | 8 |
| BTCUSDT | top-20, about 100ms | 3 |
| BTCUSDT | deep-250, about 1s | 1 |

`2026-07-13` and `2026-07-16` pass the existing minimal coverage gate but are not gap-free whole-day event-L2 evidence. On 2026-07-16, the first normalized states arrive at 01:11:50 UTC for BTCUSDC and 01:34:00 UTC for BTCUSDT. Both dates are valid for the minimal >=90% local-replay scope, not for a strict whole-day event-L2 claim.

Use `replay_l2_retained100ms_v1/eligible_days.csv` for the frozen strict top-20/100ms study. It contains 76 eligible dates from its retained111 input.

## Frozen features

| Bundle | Declared | Physical | Status |
| --- | ---: | ---: | --- |
| `features_btcusdc/` | mutable workspace | 119 | Not a current formal universe |
| `features_btcusdc_causal_v2/` | 119 | 123 | Four unlisted files for 2026-07-12 through 2026-07-15; manifest selection is mandatory |
| `features_btcusdc_causal_v3_empirical_p3_20260718/` | 122 | 122 | Exact file/manifest parity, but 2026-07-12, 2026-07-14, and 2026-07-15 expose metrics five minutes early; invalid for formal test/scorer evidence |

Do not append dates to either frozen causal directory or discover its files with an unrestricted glob.

The causal-v4 model's train and validation dates predate the affected feature files, and its P3 artifact has an independent identity ending on 2026-07-11. The model's old test metrics, test/all ML A/B values, and blocked-cross-fit BUY scorer values have nevertheless been withdrawn. Repair requires a new bundle; the frozen directory itself remains unchanged for lineage.

## External venue scope

Bitget, Bybit, and OKX spot/perpetual raw trades and causal 1s features each cover the same 114 dates through 2026-07-06. Full three-venue consensus and the external model-feature 10s/fast1s datasets also cover those 114 dates.

The separately materialized leave-one-venue-out consensus directories remain at the older retained111 identity through 2026-07-03. This is not missing local market data, but a narrower external evidence scope. Any formal leave-one-out comparison must use those 111 dates unless all variants are rebuilt under a new 114-day manifest.

The Bitget perpetual retained111 CSVs still contain pre-migration `bitget/usdt-futures/...` absolute paths even though the canonical files now live under `bitget/perp/...`. Treat those CSVs as historical manifests rather than current path indexes. Several OKX manifests likewise retain hashes and paths for source ZIPs that were removed after verified UTC normalization.

## Raw-source extras

CryptoHFTData still contains BTCUSDT `2026-05-27` (24 objects, only 67.51% source coverage) and one `2026-05-26` hour. Both are excluded evidence, not good days. They are intentionally left untouched in this audit because the third-party raw source is hard to reproduce; formal loaders must select the corrected manifest rather than glob the raw root.
