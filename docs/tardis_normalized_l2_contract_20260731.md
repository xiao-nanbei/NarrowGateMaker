# Tardis Top-20/100ms Reconstruction Contract

Last materially modified: 2026-08-12

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](public_private_documentation_contract.md).

## Identity and evidence boundary

The source-separated dataset identity is `normalized_tardis_l2_100ms_v1`, sourced from `tardis.0730-beinan.binance-futures.BTCUSDC.v1`. Its output root is:

```text
${NARROWGATE_DATA_ROOT}/normalized_tardis_l2_100ms_v1
```

It never writes into the CryptoHFTData raw tree and never overwrites `normalized_l2_100ms_v2`. Each admitted day has independently hashed BBO, L2, provider-clock, and quality files. The quality JSON is published last and is the atomic admission marker; an interrupted BBO/L2/clock replacement without a matching quality hash is not admitted. Reruns verify the three output hashes and both raw input identities before returning `validated_existing`.

Tardis `local_timestamp` is the Tardis provider's collection clock. It is not AWS Tokyo receive time and is not the live strategy's visibility clock. Every artifact therefore freezes:

```text
clock_source=tardis_provider_local
aws_tokyo_receive_time=false
policy_visible=false
live_transport_eligible=false
```

The raw incremental L2 schema has no Binance `U/u/pu` sequence identity. Snapshot/delta reconstruction can establish a provider-normalized replay candidate, but it cannot establish native sequence continuity or exact queue:

```text
native_binance_sequence_ids_present=false
native_sequence_continuity_proven=false
exact_queue_policy_eligible=false
```

## 100ms and snapshot semantics

The producer uses half-open provider-clock buckets and emits at the causal right boundary:

```text
state(b) contains only messages with tardis_provider_local < b
```

An event at the boundary is conservatively deferred to the next boundary. The last bucket whose right boundary is D+1 is not written into D. Missing event-driven buckets are not forward-generated. Consequently, raw bucket density is descriptive and is not the coverage gate.

All rows sharing `(exchange timestamp, provider local timestamp, is_snapshot)` form one logical message. The first row of a new snapshot block resets the book; all rows in that block are applied before a later bucket can be emitted. This prevents half-applied snapshots from appearing in normalized output.

The sidecar `clock/` parquet binds every normalized row to:

- the provider-local causal boundary;
- the maximum exchange timestamp already applied;
- the last provider-local input timestamp;
- boundary-to-last-input visibility delay.

The formal structural checks are a complete initial snapshot, no provider clock reversal, no `local < exchange` violation, complete top 20 on both sides, valid spread, 500ms forward freshness-union coverage of at least 99%, output p99 gap no greater than 500ms, and logical-message max gap no greater than 5s. `bucket_density` is reported separately.

## L2 versus native bookTicker

For every L2 right boundary, the audit uses only the latest bookTicker row with `tardis_provider_local < boundary`; future bookTicker rows are forbidden. The latest state may be at most 5s old. This is a cross-channel provider QA check, not a live transport check.

The frozen v1 engineering QA envelope is:

- comparable boundary ratio at least 99%;
- exact bid-and-ask price ratio at least 95%;
- bid-and-ask price within one BTCUSDC tick (`0.1`) at least 95%;
- quantity exactness and `max(0.001 BTC, 5%)` closeness are diagnostic only.

These are engineering tolerances for two independently published Binance channels, frozen before the 2025 batch. They are not statistical promotion gates and cannot grant policy or exact-queue eligibility. Changing them after the 2025 batch starts creates a new source contract.

## CryptoHFTData double-source audit

Overlap produces two distinct comparisons:

1. `exchange_time_causal_asof`: at the maximum exchange timestamp applied by the Tardis state, select only the latest CryptoHFTData state at or before that timestamp. Future CryptoHFTData rows are forbidden.
2. `clock_agnostic_nearest`: select the nearest normalized timestamp within 100ms. This is useful for detecting price/quantity reconstruction disagreement, but is explicitly not causality proof.

Both report top-20 exact price, within-one-tick price, exact quantity, and `max(0.001 BTC, 5%)` quantity agreement on a deterministic one-in-100 row sample. The comparison never upgrades missing `U/u/pu` evidence. Current raw inventory has one 2025 overlap day (`2025-12-31`, 24 CryptoHFTData hours) and 206 raw-complete 2026 overlap days. The number of formal CryptoHFTData days is smaller and remains governed by its frozen strict-day manifests.

## Development pilot and throughput

A 600-second Development pilot on 2026-06-02 produced:

| Metric | Result |
| --- | ---: |
| L2 raw rows | 237,067 |
| L2 reconstruction speed | 147,018 rows/s |
| Emitted snapshots | 5,965 |
| Bucket density | 99.4332% |
| 500ms freshness-union coverage | 99.8500% |
| Logical-message p99 gap upper bound | 100ms |
| Logical-message max gap | 185.756ms |
| Tardis L2/bookTicker causal comparable ratio | 100% |
| Exact BBO price agreement | 97.6530% |
| BBO agreement within one tick | 97.6865% |
| Causal violations / provider-clock reversals | 0 / 0 |

The pilot is intentionally `complete_day=false`, so it cannot pass the daily candidate gate. CryptoHFTData overlap was also diagnostic: with a strict 100ms matching envelope, exchange-time causal as-of support was 68.33% and nearest support was 80.00%; top-20 exact price agreement was 91.65% and 95.68%, respectively. These small 600-second samples do not define or change a gate.

The 2026 archive averages about 38.32 million L2 rows and 11.32 million bookTicker rows per day. At the conservative first-pilot throughput, normalization plus bookTicker QA is about 364 seconds per average day. A 153-day 2025 batch is therefore about 15.45 hours serial or 3.86 hours at an ideal four-process scaling. External disk contention makes a practical four-worker estimate roughly 4–7 hours. The runner supports one to four day-level processes, hash-validated resume, per-day atomic admission, single-day failure isolation, and a conservative 200MiB/day output storage preflight. Four workers are recommended for the `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` batch; if external-disk contention reduces throughput, retry with three. The batch summary binds the complete download-manifest SHA256 and stays compact; full diagnostics remain in one quality JSON per day. The 2025 batch must not start until its download manifest is complete and the `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` storage gate passes.

## Entry point

```bash
.venv/bin/python pipeline.py normalize-tardis \
  --manifest ${NARROWGATE_MARKETDATA_ROOT}/tardis/manifests/\
binance_futures_btcusdc_20250801_20251231_download.json \
  --days-file ${NARROWGATE_DATA_ROOT}/reports/\
tardis_normalized_l2_100ms_v1_20260731/technical_target_days_20250801_20251231.csv \
  --output-root ${NARROWGATE_DATA_ROOT}/\
normalized_tardis_l2_100ms_v1 \
  --workers 4
```

Batch orchestration may pass a frozen CSV with a `day` column through `--days-file` or repeat `--day`, and should write one atomic batch summary JSON. A full batch must not use `--pilot-duration-s`.
