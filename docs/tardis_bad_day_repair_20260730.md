# Tardis Bad-Day Raw Repair

Last materially modified: 2026-08-12

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](public_private_documentation_contract.md).

## Outcome

The delivery from the provider's `0730-beinan` URL was admitted under a new Tardis source identity. Its provenance name remains frozen, while the physical archive is stored directly under `tardis/{binance-futures,manifests}`. It was not copied into the CryptoHFTData tree and did not change any retained good-day manifest. This was a one-off historical delivery, not a recurring daily feed.

- Remote package: Binance Futures `BTCUSDC` `book_ticker` and `incremental_book_L2`.
- Available interval: 2026-01-01 through 2026-07-29 UTC. Both 2026-07-30 objects returned HTTP 404 at acquisition time.
- Downloaded: 420 unique zstd files, 55,996,845,705 compressed bytes, with no residual `.part` files.
- Validation: every file matched its remote Content-Length, decompressed to EOF, had a unique local path and a recorded SHA256, and exposed the expected CSV schema and non-empty data rows.
- `book_ticker`: 210 files, 2,378,083,722 rows.
- `incremental_book_L2`: 210 files, 8,047,653,197 rows.

The boundary-aware manifest is stored at:

```text
${NARROWGATE_MARKETDATA_ROOT}/tardis/manifests/binance_futures_btcusdc_20260101_20260730_download.json
```

## Historical bad-day admission

The candidate universe remains the frozen 67-day CryptoHFTData bad-day list from `reports/marketdata_repair_20260727/cryptohft_bad_days_after_repair_20260727.csv`. For all 67 candidate days:

- Tardis `book_ticker` and `incremental_book_L2` are raw-admissible: 67/67.
- Binance individual trades are present: 67/67.
- Bitget perpetual and spot trades are present: 67/67 for each source.
- Bybit perpetual and spot trades are present: 67/67 for each source.
- OKX perpetual and spot trades are present: 67/67 for each source.
- The combined raw-repair-ready gate is 67/67.

Bitget used the existing `missing_targets` then `D union D+1` source-day import contract. OKX used its UTC+8 archive importer and re-cut rows into UTC output days. Bybit used the official daily archive path. The Tardis delivery itself contains only Binance Futures; the project did not invent Tardis URLs for the other venues.

The machine-readable results are:

```text
${NARROWGATE_DATA_ROOT}/reports/tardis_bad_day_repair_20260730/raw_admission.csv
${NARROWGATE_DATA_ROOT}/reports/tardis_bad_day_repair_20260730/summary.json
```

## Boundary interpretation

Tardis daily archives are partitioned by local receive day but preserve the exchange timestamp. The raw admission therefore permits a five-second UTC handoff envelope and requires local timestamps to remain inside the declared day. This is a file-partition check, not a 100ms freshness gate. In the 67-day candidate set, the observed maxima were:

| Dataset | Max start offset | Max end gap |
| --- | ---: | ---: |
| `book_ticker` | 1.575s | 0.883s |
| `incremental_book_L2` | 2.667s | 0.094s |

The earlier one-second trial admitted only 34/67 because it incorrectly treated the first event-driven L2 message as fixed-cadence coverage. That trial is superseded and was not used to promote a day.

## Remaining evidence boundary

Raw repair is complete, but formal good-day repair is not yet established. Tardis L2 has `is_snapshot` and a complete first snapshot, but does not expose Binance `U/u/pu` sequence IDs. Consequently:

- `native_binance_sequence_ids_present=false`;
- `exact_queue_policy_eligible=false`;
- no frozen CryptoHFTData result or canonical normalized root is overwritten;
- no strategy arm, action, Validation, or holdout permission is created.

The next data-layer identity must stream the Tardis snapshot/deltas into a source-separated top-20/100ms candidate, audit internal gaps and causality, compare its BBO against native `book_ticker`, and then run the existing strict multi-source good-day gate. Only that later result can state how many of the 67 days become normalized replay days. Exact visible-level queue research must continue to fail closed without native sequence evidence.
