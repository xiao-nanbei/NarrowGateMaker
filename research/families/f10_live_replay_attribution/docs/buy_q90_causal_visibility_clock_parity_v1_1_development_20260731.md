# BUY q90 causal visibility clock parity v1.1

Last materially modified: 2026-08-12

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Decision

`buy_q90_causal_visibility_clock_parity_v1_1` completed its mechanics-only Development run and failed the frozen same-date live parity contract. F07 v2 was not created. Validation, holdout, prediction, transport, action, rollback, and live permissions remain false.

This run read no PnL, markout, campaign outcome, or policy reward.

## Structural repair

The scheduler now keeps two clocks:

- exchange time reconstructs native sequence, queue truth, matching, and fills;
- feature-ready time controls strategy visibility and q90 evaluation.

Original exchange and provider timestamps remain immutable. Events are queued by feature-ready time with source-order head-of-line handling, and only exact feature-ready timestamp ties use the frozen book-before-trade rule. Live q90 reads an atomic deep-book/order-path snapshot after deep-book maintenance.

The first frozen implementation (`v1`) stopped at the first stateful C++ evaluation because C++ formed visible path age from provider time rather than feature-ready time. That failure is preserved separately. v1.1 corrected the continuous feature and added a two-second source/ready separation test.

## Mechanics results

| mode | evaluations | valid | valid rate | shadow cancel signals | applied cancels |
|---|---:|---:|---:|---:|---:|
| legacy mixed shadow | 775,192 | 3,896 | 0.503% | 5 | 0 |
| provider-receive shadow | 775,185 | 775,135 | 99.994% | 3,606 | 0 |
| same-date AWS-profile shadow | 775,185 | 775,085 | 99.987% | 2,505 | 0 |
| same-date AWS-profile apply | 784,825 | 784,675 | 99.981% | 0 | 192 |

The mixed-clock defect therefore explains the abnormal historical under-treatment. It treated ordinary transport latency as future information and removed almost the entire q90 risk set. Under the corrected clock, applied cancel intensity was 192 per replay day versus 184 in the observed live slice; the corrected cancel/hour ratio is 0.641, no longer approximately 1/190.

The stateful apply path recorded 192 cancel ACKs, 190 score recoveries, and 192 re-entries. Python/C++ mismatch count was zero across book, activation, evaluation, and lifecycle checks.

## Passed gates

- `future_feature_time_count = 0`.
- Every invalid row had an explicit reason.
- Native source gaps, sequence failures, time reversals, receive-time fallbacks, and unknown timestamp sources were zero.
- Legacy and corrected shadow modes had identical exchange-book truth fingerprint, native event counts, queue counters, BUY/SELL fills, and final inventory.
- Same-date latency profile id/hash and BTCUSDC book/trade groups matched.
- Python/C++ q90 kernel and lifecycle parity passed with zero mismatch.
- Score p10/p50/p90 ratios versus live were 1.145/1.244/1.328, inside the frozen factor-of-two range.
- Re-entry/cancel lifecycle ratio differed from live by 0.0272.

## Failed gates

The replay valid fraction exceeded live by 0.12983, above the frozen 0.05 tolerance. The live deficit was highly concentrated: 62,495 `order_price_outside_deep_book` rows came from only 23 retained canceled orders, all emitted as `hold_invalid`. Their median elapsed age was about 366 seconds and the maximum was about 1,341 seconds. This is a terminal/invalid hold liveness and deep-range support mismatch, not a residual future-feature clock error.

Cancel-role total variation was 0.151495, just above the frozen 0.15 limit. Replay cancels were 141 opener and 51 add; live cancels were 163 opener and 21 add. The limit is not changed after observing this result.

The Spec also recorded the live slice as 24 hours although the persisted rows span 14.747423 hours. The linked denominator errata corrects evaluation-rate and cancel/hour ratios to 0.9975 and 0.6412. Both gates remain passed, so the erratum does not alter the failure decision.

## Transport boundary

The 2026-07-25 AWS tape records BTCUSDC BBO and aggTrade receive/feature-ready times, but capture-time depth recording was disabled. The derived latency profile is therefore an environment-matched distribution sensitivity, not the captured q90 deep-diff event path. The day is Grade B. Exact AWS transport support is false independently of the two live-parity failures.

## Next permitted work

F10 may investigate the retained terminal-invalid hold and live/replay deep-range denominator as a mechanics problem. It must not retune q90, reinterpret the old ON/OFF economics, or create F07 v2 from this run. A future economic attribution requires a new frozen identity only after the mechanics contract and exact transport prerequisites pass.

Authoritative mechanics report: `${NARROWGATE_DATA_ROOT}/reports/buy_q90_causal_visibility_clock_parity_v1_1_20260731/development/report.json`
