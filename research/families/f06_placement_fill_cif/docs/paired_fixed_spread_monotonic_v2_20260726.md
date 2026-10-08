# Paired Fixed-Spread Monotonic Replay v2

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Date: 2026-07-26

Status: completed research diagnostic; no live policy, fill lookup, or strategy parameter change.

Experiment ID: `paired_fixed_spread_monotonic_v2_20260726`

## Question

For the same quote decision and the same future market path, how does moving a passive order farther from the same-side BBO change:

- exact-price touch;
- strictly-through touch;
- fill conditional on an active exact/through touch;
- first-fill probability within 1s, 5s, 10s, and the complete order lifecycle?

This is a controlled execution-geometry experiment. It does not estimate the causal PnL value of changing spread in the live strategy.

## Why v1 Was Withdrawn

`fixed_spread_fill_probability_v1` ran each distance in an independent strategy world. A fill at one distance could change inventory, cooldown, later quote decisions, and the denominator of subsequent orders. Its scalar matcher also treated a strictly-through trade as if it were an exact-price trade consuming only the current print quantity.

Those two semantics allowed a deeper order to fill while the corresponding shallower counterfactual did not. The reported `0 -> 1 tick` lifecycle inversion was therefore a matcher/lifecycle diagnostic, not a market queue discontinuity. The v1 probabilities and fitted local kappa values remain withdrawn.

## Paired Contract

Each side of each quote decision creates one cohort containing all 25 distances:

```text
0, 1, 2, 5, 10, 20, 40, 60, 80, 100, 120, 140, 160,
180, 200, 220, 240, 280, 320, 400, 500, 600, 800, 1000, 1200 ticks
```

Every member of a cohort shares:

- decision time and future exchange-time market path;
- sampled new-order latency;
- activation-time Post-Only book;
- TTL, cancel-request time, and cancel-ACK latency;
- order size and frozen queue calibration.

Counterfactual fills do not alter inventory, cooldown, later decisions, or any other cohort. A final cohort that reaches day end before its lifecycle completes is excluded at every distance, preserving paired denominators.

Matching is:

- exact-price trade: consume recorded trade quantity multiplied by the frozen queue-depletion calibration;
- strictly-through trade: force a full fill because every better passive price must already have traded;
- exact touch and through touch: recorded separately.

The C++ replay fails fast if a deeper order fills while a shallower order in the same cohort does not. It also checks full-fill quantity and all fixed-horizon fill indicators pathwise and after aggregation.

## Frozen Identity

- Input root: `${NARROWGATE_RETIRED_DATA_ROOT}/normalized_l2_100ms_v2`
- Descriptive panel: 128 UTC good days
- Formal panel: 62 days with `formal_eligible=true`
- Engine: C++ paired summary replay
- Book: exchange-time normalized top-20 at 100ms
- Queue evidence: exact visible level when available, otherwise frozen calibrated fallback
- Matching contract: `calibrated_exact_qty_plus_strict_through`
- Latency mode: frozen AWS Tokyo average distribution
- Config SHA256: `71d5516680055b30fbd17cf8fe092c6011daf18c33ba5ed2246161aa7d1dde9a`
- Dataset manifest SHA256: `f47e044b0607d135de713f8cbf13dad82decc051786dd4a896f21e014129b517`
- Empirical P3 SHA256: `cedec34851454b643be746746a1dd4bcc7e13807985c14e78504643ad6e71714`
- Queue artifact SHA256: `7756881704743f7a11a5a7a0f2439bf25cbdd9cbebd3aeb00f1135191148fadd`
- Execution-trade quality SHA256: `23869c15e17809ded9c3a6e8042890a3ea294d5e3b850e0ec6309df8c4d14c11`

The run used a dirty research checkpoint. The artifact manifest is the authoritative identity for the commit, dirty patch, runner, support module, and compiled-extension hashes.

All 128 individual-trade files contain both maker-side values. The run produced 6,400 daily sufficient-statistic rows.

## Formal Result

Selected points from the 62-day formal panel:

| Side | Ticks | Median bps | Exact touch | Through touch | Any touch | Fill given touch | 1s fill | 5s fill | 10s/lifecycle fill | 95% lifecycle CI | Queue fallback |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| BUY | 0 | 0.000 | 58.53% | 29.63% | 64.89% | 78.72% | 20.82% | 47.77% | 51.08% | [48.14%, 54.10%] | 0.02% |
| BUY | 1 | 0.016 | 22.31% | 37.67% | 49.20% | 98.92% | 18.92% | 45.38% | 48.66% | [45.64%, 51.85%] | 0.02% |
| BUY | 20 | 0.313 | 7.15% | 36.68% | 41.74% | 99.59% | 14.46% | 38.46% | 41.57% | [38.42%, 44.73%] | 0.04% |
| BUY | 40 | 0.625 | 6.92% | 29.04% | 33.25% | 99.42% | 9.77% | 30.26% | 33.06% | [30.11%, 36.08%] | 4.44% |
| BUY | 80 | 1.251 | 5.05% | 20.11% | 21.05% | 99.00% | 4.46% | 18.70% | 20.84% | [18.27%, 23.24%] | 79.70% |
| BUY | 140 | 2.189 | 2.90% | 10.59% | 10.79% | 98.99% | 1.54% | 9.34% | 10.68% | [9.13%, 12.31%] | 99.48% |
| BUY | 220 | 3.439 | 1.37% | 4.57% | 4.70% | 99.09% | 0.47% | 3.97% | 4.66% | [3.84%, 5.68%] | 99.99% |
| BUY | 400 | 6.253 | 0.34% | 0.94% | 1.03% | 99.45% | 0.08% | 0.86% | 1.02% | [0.76%, 1.36%] | 100.00% |
| SELL | 0 | 0.000 | 59.44% | 44.43% | 65.75% | 76.32% | 19.70% | 46.84% | 50.18% | [47.26%, 53.18%] | 0.02% |
| SELL | 1 | 0.016 | 21.16% | 45.70% | 49.69% | 98.53% | 18.77% | 45.62% | 48.96% | [45.98%, 52.01%] | 0.02% |
| SELL | 20 | 0.313 | 7.27% | 40.05% | 42.29% | 99.31% | 14.44% | 38.82% | 42.00% | [38.94%, 45.09%] | 0.05% |
| SELL | 40 | 0.625 | 6.99% | 31.47% | 33.49% | 99.13% | 9.68% | 30.34% | 33.20% | [30.23%, 36.13%] | 4.41% |
| SELL | 80 | 1.251 | 5.08% | 20.30% | 20.93% | 98.90% | 4.37% | 18.54% | 20.70% | [18.15%, 22.96%] | 78.48% |
| SELL | 140 | 2.189 | 2.84% | 10.45% | 10.60% | 98.87% | 1.51% | 9.14% | 10.48% | [8.94%, 12.05%] | 99.45% |
| SELL | 220 | 3.439 | 1.37% | 4.58% | 4.65% | 99.07% | 0.50% | 3.93% | 4.61% | [3.78%, 5.58%] | 99.99% |
| SELL | 400 | 6.253 | 0.36% | 1.01% | 1.06% | 99.48% | 0.09% | 0.90% | 1.06% | [0.80%, 1.39%] | 100.00% |

No pathwise or aggregate monotonicity violation occurred in either panel:

- filled-order count: zero violations;
- fully-filled count: zero violations;
- filled quantity: zero violations;
- 1s, 5s, 10s, and lifecycle probability: zero violations.

The corrected `0 -> 1 tick` lifecycle changes are:

- BUY: `51.08% -> 48.66%`, or `-2.41` percentage points;
- SELL: `50.18% -> 48.96%`, or `-1.22` percentage points.

## Interpretation

`fill given active touch` still rises sharply from 0 to 1 tick. It is not a pure queue-conversion probability: it mixes exact-price queue depletion, later-through fills, strictly-through forced fills, and the stronger path selected by a deeper touch. A through touch at a deeper price forces every better-priced passive order to have filled, so the conditional denominator becomes more selective as distance increases.

The unconditional paired probabilities answer the spread question. They are monotone at 1s, 5s, 10s, and lifecycle horizons. The old claim that a one-tick deeper quote has a higher lifecycle fill probability is rejected.

The curve is not yet exchange deep-queue truth. Queue fallback rises to roughly 79% at 80 ticks, 93% at 100 ticks, and more than 99% at 140 ticks. The far tail describes the frozen operational matching model. Native deep snapshot/delta queue evidence is still required before fitting a live fill-probability lookup.

## Decision

- The v1 runner, report, fitted lookup, image, and data artifacts are deleted. This report keeps only the failure mechanism so it cannot be rediscovered as evidence.
- Use v2 as the current controlled spread-fill baseline.
- Do not change live behavior or publish a live lookup from this run.
- Do not interpret `fill given touch` alone as fill probability.
- Any volatility-conditioned model must be fitted on the paired order denominator and preserve monotonicity by construction.

## Artifacts

```text
${NARROWGATE_RETIRED_DATA_ROOT}/reports/
  paired_fixed_spread_monotonic_v2_20260726/
    daily_results.csv
    paired_spread_fill_curve.csv
    paired_fixed_spread_curve.svg
    paired_fixed_spread_curve.png
    execution_trade_quality.csv
    manifest.json
```
