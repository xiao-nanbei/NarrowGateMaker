# Causal-v9 Through 2026-07-25: Retraining and Replay Result

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Date: 2026-07-27

## Decision

The causal-v9 13-head bundle is **not promoted** and is **not deployed**. The corrected ML-OFF operational baseline remains the reference policy, with the empirical P3 artifact unchanged.

The candidate reduced inventory time, but it lost both raw PnL and terminal campaign value and created additional tail campaigns. This is a risk-control tradeoff, not executable alpha.

## Frozen Identity

- Good-day universe: 133 UTC days through 2026-07-25.
- Train: 106 good days through 2026-06-28.
- Embargo: 2026-06-29.
- Validation: 20 good days from 2026-06-30 through 2026-07-19.
- Embargo: 2026-07-20.
- Test: 2026-07-21 through 2026-07-25.
- Feature semantics: v5, with `feature_ready_ts = bucket_start + 10s`.
- Feature manifest SHA256: `7d78def05b23f6335e72beced362b9c6ac50f509f6895bc0b30feedd3e5489fe`.
- Corrected taker-tempo manifest SHA256: `af385300d2852c0e6cc8d3e5f1b50984e649dee6e33450cf44740dc189a72292`.
- Normalized 100ms L2 manifest SHA256: `88ff8faeef9976a10b1cf03e887e624e3cce5abb3d17de1b7920a58bb0de1275`.
- Empirical P3 SHA256: `cedec34851454b643be746746a1dd4bcc7e13807985c14e78504643ad6e71714`.
- P3 parameters: `delta_star=13.999086 USDC/BTC`, `kappa_eff=0.067356 (USDC/BTC)^-1`.
- Queue calibration: queue-v3 q0.70.
- Latency: `aws_tokyo_ec2_2vcpu4g_20260712`, seed 59.
- Replay state: daily fresh start; individual trades; C++ engine.
- Arm difference: only `ml_enabled`; BUY fill-selection and dynamic-hazard action overlays were disabled in both arms.

The invalid causal-v8 rebuild based on the mutable, side-corrupted taker-tempo root was excluded before this model was fitted.

## Predictive Diagnostics

The 13 heads were fitted on 915,796 train rows and evaluated on 43,200 Test5 rows. Selected Test5 diagnostics were:

| Head | Test metric |
|---|---:|
| 10s direction | AUC 0.5563 |
| 10s return | IC 0.0623 |
| 10s volatility | IC 0.5940 |
| 30s direction | AUC 0.5219 |
| 60s direction | AUC 0.5195 |
| bid toxicity 5s | AUC 0.5849 |
| ask toxicity 5s | AUC 0.5629 |

These are ranking diagnostics. They do not authorize a strategy action.

## Formal Panel Amendment

The preregistered formal panel originally contained four days. The formal loader stopped before producing arm outcomes because 2026-07-24 had only 97.8925% normalized L2 coverage. Its largest source gaps were approximately 744.7s, 675.3s, and 415.8s. Fresh downloads of the affected CryptoHFTData hours matched the local raw files byte-for-byte, so this is a source-side gap.

Consequently:

- Formal: 2026-07-21, 2026-07-22, 2026-07-23.
- Diagnostic only: 2026-07-24.
- Diagnostic only: 2026-07-25, because its required prior-day context is not formal eligible.

The amendment was frozen before any arm outcome was produced. Three formal days do not satisfy the original four-day deployment denominator.

## Formal Test3

Python/C++ parity on 2026-07-21 was exact: 332 fills versus 332 fills and zero PnL difference.

| Metric | ML OFF | ML ON | Delta / ratio |
|---|---:|---:|---:|
| Raw PnL (USDC) | -0.6571 | -2.8879 | -2.2308 |
| Terminal campaign PnL (USDC) | +3.5311 | +0.9043 | -2.6268 |
| Tail campaigns | 2 | 5 | +3 |
| Fills | 999 | 976 | 97.70% retained |
| Absolute inventory time | 499.71 | 454.03 | 90.86% |
| Campaigns | 227 | 217 | -10 |

Raw PnL delta was negative on all three formal days:

| Day | Raw delta | Terminal delta | Tail delta |
|---|---:|---:|---:|
| 2026-07-21 | -0.3379 | -0.4834 | +1 |
| 2026-07-22 | -1.7475 | -1.6718 | +2 |
| 2026-07-23 | -0.1453 | -0.4716 | 0 |

## Diagnostic Test2

| Metric | ML OFF | ML ON | Delta / ratio |
|---|---:|---:|---:|
| Raw PnL (USDC) | -2.3447 | -3.1677 | -0.8231 |
| Terminal campaign PnL (USDC) | -0.4871 | -1.3698 | -0.8827 |
| Tail campaigns | 0 | 0 | 0 |
| Fills | 462 | 452 | 97.84% retained |
| Absolute inventory time | 195.62 | 195.28 | 99.83% |

The candidate was worse on 2026-07-24. It was only marginally better on 2026-07-25: raw `+0.0043` and terminal `+0.0067` USDC.

Across all five test dates, treating the two invalid-context dates as diagnostic only, the candidate produced raw `-3.0538` USDC, terminal campaign `-3.5095` USDC, and three additional tail campaigns relative to ML OFF.

## Deployment Gates

| Gate | Requirement | Result |
|---|---:|---:|
| Formal denominator | 4 days | 3 days, fail |
| Raw PnL delta | > 0 | -2.2308, fail |
| Terminal campaign delta | >= 0 | -2.6268, fail |
| Tail campaign delta | <= 0 | +3, fail |
| Fill retention | >= 85% | 97.70%, pass |
| Inventory-time ratio | <= 1.10 | 0.9086, pass |
| Positive raw days | >= 3/4 | 0/3, fail |

No threshold was tuned after Test5 was read. The candidate remains an offline artifact for diagnosis and must not replace the ML-OFF live baseline.

## Calibration Governance

Perfect logistic calibration targets `alpha=0` and `beta=1`. The placement-CIF v1 tolerance `abs(alpha)<=0.01` is a preregistered judgmental engineering budget, not a theoretical constant. It remains frozen for v1.

A future v2 must derive side/role/horizon-specific tolerances from a declared action-EV error budget, report probability-scale calibration-in-the-large and O/E with day-clustered uncertainty, use past-only rolling recalibration, and verify that probability error does not reverse the ordering of supported actions.
