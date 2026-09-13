# Minimal Market-Data Good-Day Reaudit

Date: 2026-07-27

## Current Minimal Contract

The local Binance BTCUSDC replay denominator no longer intersects every source ever collected. It requires:

- official Binance BTCUSDC/BTCUSDT futures aggTrades;
- official Binance BTCUSDC/BTCUSDT futures individual trades;
- official Binance BTCUSDC/BTCUSDT spot aggTrades;
- official Binance BTCUSDC/BTCUSDT futures metrics;
- CryptoHFTData BTCUSDC native snapshot/delta for target day `D` and natural UTC warmup day `D-1`.

CryptoHFTData BTCUSDT order-book files are explicitly excluded. Historical BTCUSDT bridge state is built from official Binance trades; live continues to use the Binance WebSocket book ticker.

## Result

The calendar audit covers 206 UTC days from 2026-01-01 through 2026-07-25.

| Gate | Days | Meaning |
|---|---:|---|
| Calendar days audited | 206 | No pre-existing retained-day filter |
| Minimal official/local raw complete | 141 | Old 133-day raw set plus eight recovered dates |
| BTCUSDC native sequence eligible | 84 | `D-1` snapshot seed, target updates, no sequence gap or invalid ordering |
| Historical 99% normalized formal | 76 | Existing registry contract; up from 66 |
| Grade A continuous formal | 46 | Formal plus maximum internal normalized gap at most 5 seconds |
| Grade B gap-censored replay | 31 | At least 99% coverage, but a formal/end-state or 5-second maximum-gap gate fails |
| Grade C segmented diagnostic | 7 | Sequence valid and 95%-99% normalized coverage |
| Grade D no exact L2 | 57 | Official raw exists, but native BTCUSDC sequence is invalid |
| Grade F missing required local raw | 65 | Native sequence is already invalid, so omitted official files were not bulk-downloaded |

The immutable 141-day registry is:

```text
${NARROWGATE_DATA_ROOT}/normalized_l2_100ms_v2_minimal141_20260727/
```

All 76 registry-formal BBO/L2 pairs passed size and SHA256 validation.

## Newly Recovered Dates

Removing CryptoHFTData BTCUSDT from the denominator exposed eight dates whose BTCUSDC source can be causally reconstructed. Official Binance raw sources, bars and metrics were downloaded and built for all eight. The 16 new individual-trade files contain 27,722,272 rows, zero non-monotonic trade IDs, zero invalid maker-side flags, and complete BUY/SELL identity.

| Day | Registry result | Max internal gap | Final grade |
|---|---|---:|---|
| 2026-04-12 | 98.99944%, below the frozen 99% floor | 620.367s | C |
| 2026-05-07 | formal | 675.858s | B |
| 2026-05-08 | formal | 2.618s | A |
| 2026-05-09 | formal | 64.662s | B |
| 2026-05-10 | formal | 5.805s | B |
| 2026-05-11 | formal | 2.723s | A |
| 2026-05-14 | formal | 16.933s | B |
| 2026-05-16 | formal | 3.439s | A |

Four old targets also became standard-formal after their natural-day warmup source was restored: 2026-04-13, 2026-04-30, 2026-05-12 and 2026-05-15. 2026-07-14 was downgraded because its final normalized state is 45.941 seconds before UTC midnight. The net historical-formal change is therefore `66 -> 76`.

## CryptoHFTData Remote Recheck

All 4,968 expected BTCUSDC hourly objects from the 2025-12-31 warmup through 2026-07-25 are locally present and decodable. For the 122 native-sequence failures, the audit downloaded an isolated fresh copy of 3,048 target/warmup hours:

- downloaded: 3,048;
- remote 404: 0;
- missing canonical counterpart: 0;
- byte-identical SHA256: 3,048;
- changed files: 0.

Thus transport and object availability are healthy. The remaining failures are provider-content limitations: 114 days are not snapshot-seeded at target start, 102 accept no target update before a later seed, and 13 contain a target sequence gap. Re-downloading those bytes cannot repair the lifecycle identity. The isolated refresh directory was removed after the comparison report was frozen.

## Coverage Policy

The 99% whole-day floor remains unchanged. Lowering it to 95% would admit seven additional dates, but they lose about 14.4-31.5 minutes of fresh state and have single gaps of about 10.1-21.1 minutes. Those dates may enter segmented diagnostics only, with queue, order, inventory and campaign state reset or censored at every gap.

The old `formal_eligible` bit is not sufficient for uninterrupted lifecycle work. Among the 76 standard-formal dates, 30 exceed the new 5-second maximum-gap budget. Whole-day order, queue and campaign studies should use the 46 Grade A dates. Pointwise feature studies may use broader dates only with explicit gap masks and no lifecycle crossing.

## Frozen Artifacts

```text
${NARROWGATE_DATA_ROOT}/reports/minimal_good_day_reaudit_20260727/
  calendar206_native_sequence.csv
  calendar206_native_sequence.json
  cryptohft_bad_day_remote_sha256_comparison.csv
  cryptohft_bad_day_remote_recheck.json
  new8_binance_individual_trades_quality.csv
  normalized_audit_sequence84.csv
  normalized_strict_days_sequence84.csv
  daily_quality_20260101_20260725.csv
  daily_quality_20260101_20260725.json
  grade_a_continuous_days.csv
  grade_b_gap_censored_days.csv
  grade_c_segmented_days.csv
  excluded_native_l2_days.csv
```

`daily_quality_20260101_20260725.csv` is the canonical per-trading-day grade ledger for this audit.
