# BUY q90 Portfolio Path Attribution v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Decision

The frozen 40-day Development replay does **not** support the complete proposed mechanism:

`q90_portfolio_bias_mechanism_not_supported_development`

The q90 ON arm increased the SELL-minus-BUY exposure-fill imbalance on the 24 Grade-A primary days, but the other preregistered links did not pass:

- BUY exposure suppression narrowly missed its one-sided gate;
- SHORT campaign share did not have a positive lower bound;
- multi-level SHORT campaign share did not increase;
- terminal MTM did not worsen.

This result neither validates nor invalidates the current live policy. The fixed q90 model overlaps Development, and the replay action rate is not transported to the current live window.

## Frozen Contract

- Identity: `buy_q90_portfolio_path_attribution_v1`
- Contrast: q90 ON minus q90 OFF
- Primary panel: 24 Grade-A Development days
- Sensitivity panel: 16 Grade-B Development days
- Validation read: false
- Sealed holdout read: false
- Action authorization: false
- Live or automatic rollback authorization: false
- Engine: Python-authoritative full-path replay
- Queue: native snapshot/delta, strict exact-price level
- Execution source: Binance USD-M individual trades
- Shared between arms: market path, deterministic latency path, P3, queue calibration, cooldowns, loss guard, size and inventory limit
- C++ authority: native-book and BUY q90 scorer lockstep only; not full C++ tick replay authority

Campaigns are reconstructed independently after path divergence. No campaign is matched across arms after q90 changes the order or inventory path.

## Grade-A Primary Result

All effects below are paired daily means, q90 ON minus q90 OFF.

| Metric | Estimate | 95% day-cluster interval | Gate |
|---|---:|---:|---|
| terminal MTM, USDC/day | +0.09470 | [-0.23214, +0.45084] | no harm evidence |
| closed campaign value, USDC/day | +0.09061 | [-0.23923, +0.44908] | diagnostic |
| BUY exposure fills/hour | -0.22744 | [-0.45487, +0.00173] | fail |
| SELL exposure fills/hour | +0.20313 | [-0.00521, +0.41494] | diagnostic |
| SELL-minus-BUY exposure fills/hour | +0.43057 | [+0.00869, +0.85418] | pass |
| SHORT campaign share | +0.00851 | [-0.00247, +0.01922] | fail |
| multi-level SHORT share of all campaigns | -0.00124 | [-0.00633, +0.00386] | fail |
| multi-level rate among SHORT campaigns | -0.00630 | [-0.01597, +0.00324] | diagnostic |
| multi-level SHORT value, USDC/day | -0.12788 | [-0.33732, +0.05815] | diagnostic |

Only the side-imbalance link passed. The preregistered portfolio-bias contract required all four mechanism links, and the economic-harm contract required the terminal-MTM upper bound to be below zero. Both composite conclusions are false.

## Sensitivity And Descriptive Results

Grade B remains sensitivity-only and cannot rescue Grade A. Its terminal-MTM effect was positive, +0.70306 USDC/day with interval [+0.22705, +1.22846]. Its multi-level SHORT share increased, but its overall SHORT-share interval crossed zero and its exposure-imbalance interval also crossed zero.

Across all 40 days, the descriptive terminal-MTM effect was +0.33804 USDC/day with interval [+0.04751, +0.65533]. Aggregate terminal MTM was -192.16901 USDC with q90 OFF and -178.64726 USDC with q90 ON. This pooled descriptive result is not OOS policy validation and does not authorize keeping, promoting or removing the live policy.

## Action Support And Transport Boundary

The q90 ON replay produced:

- 25,555,868 score evaluations;
- 105 cancel requests and 105 cancel ACKs;
- zero pending-cancel fills;
- 104 re-entries;
- cancel activity on 34 of 40 days;
- 57 requests on Grade A and 48 on Grade B.

The replay covered 959.963 hours, or about 0.109 cancel requests/hour. The separate 120-hour live diagnostic records 2,501 requests, about 20.84/hour. Assuming the counters have the same operational meaning, live intensity is roughly 190 times the historical replay intensity. This comparison is contextual, not a decision gate: it may combine market-regime transport, receive/feature-ready timing, runtime state and counter-denominator effects.

Therefore the Development result answers only:

> On the frozen historical Development path, did the fixed q90 policy produce the complete hypothesized portfolio-bias and terminal-harm chain?

The answer is no. It does not answer whether the unusually active current live policy caused the observed 240-hour SHORT concentration. That question requires a separate hash-bound live-action-rate parity/transport identity or same-window diagnostic replay; the present Validation and holdout remain closed.

## Integrity Audit

- Evaluated days: 40/40, exactly matching the frozen denominator
- Grade A / Grade B: 24 / 16
- Market manifest: 1,222 files, 11,625,657,192 bytes
- Market manifest canonical SHA256: `27787b4aa129563c3d5bf173ef7705e44a511462de0c5989a8b1db7e8b457227`
- Source/sequence gaps: zero in both arms on all days
- Native event denominator: identical between arms on all days
- Maximum absolute campaign accounting error: `3.694822225952521e-13 USDC`
- Day parquet row counts and SHA256: all exact
- Frozen implementation hashes: all exact after the run
- Native module hash: exact
- Contract tests: 55 passed, 0 failed
- Python/C++ q90 kernel mismatch: fail-fast contract completed without a mismatch

## Authoritative Artifacts

Root: `${NARROWGATE_DATA_ROOT}/reports/buy_q90_portfolio_path_attribution_v1_20260731/development`

| Artifact | SHA256 |
|---|---|
| `report.json` | `b308871215e766b3f6f31fc84b49a894c632b5fe4b0b78b452bd6678a5fc055d` |
| `report.md` | `5b51fc1beb110753655936262bc494d67e63c1ba2104005f47790f94b85a8434` |
| `arm_daily.parquet` | `485026f9c2876f61464c33910b4b6c5c855f3c7d4147cb76df4e76d414dc7011` |
| `paired_daily.parquet` | `99245f60c824aded69384be4685c88a2ec5bfa00b4eb94ce8f94201fd1b4d344` |
| `market_source_manifest.json` | `d5e18848722eb0f2bc2b42cea2e0552a65aea684715354f247d5f2e0ed41e2d4` |

The frozen Spec and external authoritative artifacts must remain unchanged.
