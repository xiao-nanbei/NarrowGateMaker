# Market-Data Quality Rating Snapshot (2026-07-31)

Last materially modified: 2026-08-20

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](public_private_documentation_contract.md).

Status: historical snapshot, not a current denominator or freshness authority. The filename is retained for inbound references. The evidence below covers the explicit cutoffs stated in each row, with the canonical local ledger ending at 2026-07-25 and Tardis raw coverage ending at 2026-07-29.

Deployment note: all counts below remain a frozen historical snapshot and are not revised in place. Later operational routing, capture boundaries, and readiness evidence are owner-private and are not distributed with the public repository.

As of 2026-07-31, the project data estate is rated by intended use rather than by a single universal good-day flag. The overall research-data rating is **B**: raw evidence and storage integrity are strong, but the authoritative continuous exact-lifecycle denominator is still only 46 days and AWS Tokyo receive-time evidence has not reached its frozen 30-day threshold.

## Layer ratings

| Layer | Rating | Current evidence | Authority boundary |
| --- | --- | --- | --- |
| `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` storage and immutable raw integrity | A | 519 GiB free; admitted Tardis payloads have SHA256, size, schema and decompression checks | Removable-disk availability remains an operational dependency |
| 2025-08-01 through 2025-12-31 non-CryptoHFT sources | A technical | 15 sources x 153 days = 2,295/2,295 valid checks, zero failures | Technical availability only; not a frozen research denominator |
| 2025 Tardis provider-normalized top-20/100ms | B diagnostic | 153/153 rebuilt and hash-valid; 93/153 pass the provider-normalized replay gate | No Binance `U/u/pu`, no exact queue, no AWS receive-time, no policy/live authority |
| 2026 canonical BTCUSDC local replay | B overall | 206 audited days: A=46, B=31, C=7, D=57, F=65 | Only Grade A is uninterrupted formal lifecycle evidence |
| 2026 Tardis raw archive | A raw | 210 complete UTC days through 2026-07-29; 2026-07-30 is absent at the provider | Raw repair does not promote canonical good days |
| 2026 Tardis dual-source normalized audit | C diagnostic | 62 requested overlap days, 60 post-batch integrity-valid, 18 provider candidates | Source-separated diagnostic only; exact queue remains false |
| Derived causal feature context | B | causal-v10 has 140 hashed daily feature files; taker-tempo v4 has 141/141 | Must be filtered by the frozen A/B day contract; feature context has no action authority |
| AWS Tokyo receive-time capture | C readiness / A file integrity / B transport quality | 17 distinct valid full-window UTC days, 18 full captures, seven tapes each, zero recorder queue drops | Below the preregistered 30-day M0/M1 threshold; system-load episodes must remain visible |
| Daily freshness | C | Canonical full-day local inputs end at 2026-07-25; Tardis raw ends at 2026-07-29; 2026-07-30 has only a bounded receive-time window | 2026-07-26 onward is not a canonical full-day baseline denominator |

## Canonical 2026 day grades

The authoritative 2026 per-day ledger covers 2026-01-01 through 2026-07-25:

| Grade | Days | Allowed use |
| --- | ---: | --- |
| A | 46 | Formal training and uninterrupted queue/order/campaign replay |
| B | 31 | Gap-censored replay or sensitivity only; no lifecycle may cross a gap |
| C | 7 | Segmented diagnostics only, with state reset/censoring at each gap |
| D | 57 | Official trade/bar analysis only; native exact L2 identity is invalid |
| F | 65 | Excluded from the canonical local denominator because required raw inputs are incomplete |

The 46 Grade A days span 2026-04-13 through 2026-07-23. The 31 Grade B days may be used only when a study preregisters gap censoring. Combining A and B without that distinction is not valid.

## New Tardis evidence

Tardis materially improves raw and provider-normalized coverage, but it does not expose native Binance sequence identifiers. Therefore:

```text
native_binance_sequence_ids_present=false
native_sequence_continuity_proven=false
exact_queue_policy_eligible=false
aws_tokyo_receive_time=false
live_transport_eligible=false
```

The 93 provider candidates from 2025 and 18 candidates from the 2026 overlap can support a newly preregistered source-separated replay study. They cannot be silently appended to the canonical Grade A denominator.

## Receive-time readiness

The capture ledger contains 17 distinct valid full-window UTC days from 2026-07-12 through 2026-07-30. One duplicate full capture on 2026-07-21 does not increase the day denominator, and the 600-second diagnostic on 2026-07-23 does not count. The admitted full captures contain about 56.30 million events and 501 maker fills. Recorder queue drops are zero and strategy hashes are unchanged for every admitted window. The same windows record 648 severe log rows, with 520 concentrated on 2026-07-19. The maximum market-tape queue age is 3.812 seconds and the maximum external-record queue age is 330.5ms. These facts do not invalidate gzip/SHA admission, but downstream analysis must retain the system-load state or censor unsupported intervals.

The external M0/M1 audit remains blocked until 30 distinct full-window days and its chronological, side-specific, leave-one-venue-out and late-panel denominator checks pass.

## Authoritative artifacts

- Canonical per-day grades: `${NARROWGATE_DATA_ROOT}/reports/minimal_good_day_reaudit_20260727/daily_quality_20260101_20260725.csv`
- 2025 non-CryptoHFT full audit: `${NARROWGATE_DATA_ROOT}/reports/marketdata_backfill_20250801_20251231/final_audit_after_bybit_repair/summary.json`
- Tardis combined quality: `${NARROWGATE_DATA_ROOT}/reports/tardis_normalized_l2_100ms_v1_20260731/combined_2025_2026_quality.csv`
- Tardis bad-day raw admission: `${NARROWGATE_DATA_ROOT}/reports/tardis_bad_day_repair_20260730/raw_admission.csv`
- AWS receive-time ledger: `${NARROWGATE_DATA_ROOT}/receive_time_tape/capture_ledger.v1.jsonl`

This report is a current governance summary. It does not change any frozen good-day identity, split, Validation/holdout permission, action authorization or live authorization.
