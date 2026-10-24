# causal-v12 1s 2026 Native Source Coverage v1

Last materially modified: 2026-08-12

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Status

This is a read-only physical-source coverage audit for the F03 causal-v12 1s successor. It does not materialize features, train models, run predictions, read PnL, score transport, authorize economic replay, or grant baseline/action/live authority.

The machine-readable authority is [`causal_v12_1s_2026_native_source_coverage_v1_20260805.json`](causal_v12_1s_2026_native_source_coverage_v1_20260805.json). The auditor is [`causal_v12_1s_2026_native_source_coverage.py`](../audit/causal_v12_1s_2026_native_source_coverage.py).

## Frozen Denominators

The auditor reads the existing frozen causal-v12 panel specifications without modifying them:

| Denominator | Target days | Result under proposed exact profile |
|---|---:|---:|
| Historical native transport Development | 22 | 17 accepted, 5 rejected |
| Historical native late diagnostic | 22 | 20 accepted, 2 rejected |
| Matching full-path economic 22+22 | 44 | 37 accepted, 7 rejected |
| Frozen causal-v12 Development | 40 | 40 accepted, 0 rejected |

The panels contain 61 distinct target days and require 76 distinct calendar days after adding exact D-1 warmup days. The transport and economic 22+22 day lists match exactly.

## Existing Profile

The current `NATIVE_NORMALIZED_PROFILE` has exact resolver candidates only for `2026-07-26` through `2026-07-31`. Under its current path and authority contracts, only `2026-07-29` resolves successfully. That day is not part of the 61-day frozen union, so the current profile covers **0/61 frozen target days**.

The other profile candidates fail because the exact postfit tempo or per-day native quality authorities required by that profile are absent. The auditor did not search for fallback files and did not substitute a different warmup.

## Explicit Profile Candidate

Existing exact roots can form the read-only coverage candidate `native_historical_minimal141_individual_reference_v1_candidate`:

| Component | Exact root or authority |
|---|---|
| Local 1s trade-tempo | `${NARROWGATE_DATA_ROOT}/trade_features_causal_v5_expanded_20250801_20260725/BTCUSDC` |
| Tempo manifest | `${NARROWGATE_DATA_ROOT}/trade_features_causal_v5_expanded_20250801_20260725/manifest.json` |
| Native normalized L2 | `${NARROWGATE_DATA_ROOT}/normalized_l2_100ms_v2_minimal141_20260727/l2` |
| L2 manifest | `${NARROWGATE_DATA_ROOT}/normalized_l2_100ms_v2_minimal141_20260727/manifest.json` |
| L2 quality | `${NARROWGATE_DATA_ROOT}/normalized_l2_100ms_v2_minimal141_20260727/daily_quality.csv` |
| Metrics | `${NARROWGATE_DATA_ROOT}/raw_metrics` |
| BTCUSDT individual-trade reference bars | `${NARROWGATE_DATA_ROOT}/reference_bars_1s_trades_v1` |

Authority SHA256 values, every required file path, file SHA256, size, Parquet row count, quality role, and rejection reason are recorded in the JSON report. The profile is coverage-audited only and has not been added to the source resolver.

For native L2, target days require target formal eligibility while D-1 days require the separately declared `warmup_valid` role. A day rejected as a target is not automatically rejected as warmup. This distinction closes the earlier over-rejection of the 40-day panel without weakening either role.

## Remaining Gaps

Seven required BTCUSDT individual-trade 1s artifacts and matching authority files are absent:

`2026-04-12`, `2026-05-07`, `2026-05-08`, `2026-05-10`, `2026-05-11`, `2026-05-14`, and `2026-05-16`.

Exact raw BTCUSDT individual-trade files already exist on `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` for all seven days. Aggregate-trade `bars_1s` artifacts are also observable for these dates, but they were not used: replacing the frozen individual-trade reference source would be source mixing, not a fallback-equivalent warmup. Materializing the missing individual-trade references requires a separate explicit source identity.

`BTCUSDC-metrics-2026-07-19.csv` exists with 288 rows but fails the current strict causal metrics clock contract. Consequently `2026-07-19` is rejected as a target and `2026-07-20` is rejected because its exact D-1 metrics warmup is invalid. This audit neither reorders nor rewrites that source file.

The resulting unique-target coverage is 54/61. Rejected target days are:

`2026-04-13`, `2026-05-08`, `2026-05-11`, `2026-05-15`, `2026-05-16`, `2026-07-19`, and `2026-07-20`.

## Decision

The existing physical sources are sufficient to define a new explicit profile for the frozen 40-day Development denominator, but not for the complete 22+22 transport/economic denominator. Extending that profile to all frozen target days requires the missing individual-trade reference artifacts to receive their own authority and the `2026-07-19` metrics clock issue to be resolved under a separate governed repair. This audit does not authorize either operation.

No glob fallback, alternate warmup, large-data materialization, training, prediction, PnL read, source-resolver change, backtest change, or live change occurred.
