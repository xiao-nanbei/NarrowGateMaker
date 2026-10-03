# P3 Aggressive Reach-Time Conditioned Hazard v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Status: Development prediction evidence supported; no quote, action, shadow, or live authority.

## Result

The side-specific 100ms discrete-hazard model passed the frozen normal research path. It estimates the full aggressive-reach first-passage surface through the 30-second administrative censoring boundary. It does not estimate activation, queue conversion, fill-before-cancel, terminal value, or PnL.

| OOF result | BUY | SELL |
|---|---:|---:|
| Integrated-Brier improvement | +0.004463 | +0.004428 |
| Day-clustered 95% CI | [+0.002155, +0.007202] | [+0.002829, +0.006285] |
| Positive-day rate | 60.71% | 75.00% |
| OOF days | 84 | 84 |

The four expanding chronological folds passed independently by side. Overall context coverage was 99.835%, above the frozen 98% hard gate. Distance-CDF, distance-hazard, and time-CDF monotonicity violations were all zero; maximum probability-mass error was `2.554e-15`.

The 48 provider/native overlap days had aggregate prediction MAE of 0.006089 for BUY and 0.005553 for SELL, below the frozen 0.01 gate. Maximum daily MAE was higher, 0.015121 and 0.012421 respectively, and remains a transport sensitivity rather than a passed daily-uniformity claim.

The previously read 44 native diagnostic days were not used by any gate. Their proper-score direction remained positive, but they are historical diagnostics, not independent confirmation.

## Residual Boundary

Near-distance calibration remains imperfect. In several 5-20 and 21-100 tick cells the model underpredicts observed reach, especially at 5-30 second report horizons. Passing integrated Brier and monotonicity does not grant a quote mapping or prove economic value.

The operational fixed-10s P3 v2 artifact is unchanged. Any policy using this surface must freeze an independent action and pass a separate full-path economic A/B against the current operational baseline.

## Artifacts and Availability

| Artifact | SHA256 | Availability |
| --- | --- | --- |
| [Frozen Spec](p3_aggressive_reach_time_conditioned_hazard_v1_spec_20260804.md) | `943b3f9a11bad31bfb378c78df0ffff5bfff29fcb2ae40c26c6842ddea9b7bbf` | Public repository |
| [Source-day manifest](p3_reach_time_source_day_manifest_v1_20260804.json) | `3e124c2633a56e628f7593983c20fb444691fc6cf9e086e5d2233f9efb8278a1` | Public repository |
| Primary cache summary | `96503f91add2006f0133fdbdb2b54b210fec61081f49c8d7fea8b64b987379bd` | Private evidence store; not distributed with public repository |
| Overlap cache summary | `cd7e3c608ed2aefbf6f3705ec476bce45b9e43c036aea72ed0d9185287aed722` | Private evidence store; not distributed with public repository |
| Training report, canonical identity | `82573c1cf8dfc5623e238c31cbf156f99df5ad0bb2e7f25867b2cf3c9d3e5752` | Private evidence store; not distributed with public repository |
| Training report, file bytes | `417e618f6a30526af2b900d5a2acc3a4f62acf32a1b64badc9490027fe4ea3b8` | Private evidence store; not distributed with public repository |
| Final-model result | `3695948c0c4d3f420642fb5dcfc9be47a92b36b46db11503dd51e8ddff2b469d` | Private evidence store; not distributed with public repository |

The authoritative report has logical evidence ID `model_runs/p3_aggressive_reach_time_conditioned_hazard_v1_20260804/report.json`. Its SHA256 values identify retained bytes; they are not download links.

## Public References

See the [family README](../README.md), [design note](p3_aggressive_reach_time_surface_v1_design_20260804.md), [frozen Spec](p3_aggressive_reach_time_conditioned_hazard_v1_spec_20260804.md), [hazard implementation](../audit/p3_reach_time_conditioned_hazard.py), and [training implementation](../audit/p3_reach_time_hazard_training.py).
