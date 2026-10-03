# F03 causal-v12 1s metrics source-ready amendment v1

Last materially modified: 2026-08-05

Status: implementation amendment; economic and prediction outcomes unread.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Scope

The retained Binance Futures raw metrics files contain nine delayed timestamps among 89,853 rows: two rows are one second after a nominal five-minute boundary and seven rows are two seconds after it. Two affected dates belong to the frozen 66-day F03 refit panel (`2025-08-15` and `2025-08-25`). Rejecting the whole day would change the frozen denominator for an outcome-blind transport artifact rather than represent the physical source faithfully.

This amendment accepts a per-row source-ready delay in the closed interval `[0ms, 2000ms]` relative to the row's ordinal five-minute start- or end-stamp. It preserves the observed timestamp as `feature_ready_ts_ms`; delayed rows are never snapped back to the nominal boundary. A delay above 2000ms, an early timestamp, a missing/extra row, a duplicate timestamp, or a schema/value failure remains fail-closed.

Physical CSV row order is not treated as a clock. The reader first requires 288 unique timestamps, records the number of physical clock inversions, and then stable-sorts the rows by timestamp before checking complete start/end grids. This admits the complete but shuffled `2026-07-19` file without changing any timestamp or metric value. A duplicate, missing, extra, early, or out-of-bound timestamp still fails.

The 2000ms bound is the maximum observed delay in the complete retained raw-metrics corpus audited on 2026-08-05. It is a frozen source-transport bound, not a model parameter and not selected from prediction or PnL results.

## Cache identity

`causal_v12_1s_daily_sources.py` remains part of the daily feature-cache code identity. Therefore pre-amendment cache directories are not hash-compatible. The 66-day rebuild must use a new `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` cache namespace; old cache artifacts remain untouched until the replacement is admitted and a reference audit authorizes cache deletion.

## Authority

This amendment grants only physical feature materialization eligibility. It does not authorize model training, prediction promotion, strategy action, or live deployment.
