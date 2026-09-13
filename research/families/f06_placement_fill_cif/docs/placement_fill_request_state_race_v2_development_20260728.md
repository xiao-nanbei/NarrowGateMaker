# Placement Fill Request-State Race v2: Development Result

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Status: Development complete; full learned three-phase prediction family closed. Validation and sealed holdout remain unread. No action or live deployment is authorized.

## Identity

- Fit specification: `docs/placement_fill_request_state_race_v2_fit_spec_v2_20260728.json`, SHA256 `2695b3ddb74f0f146d8f4370c9ebd9cf04d7634c562d6cad13faa07a2da395c5`.
- Development report: `${NARROWGATE_RETIRED_DATA_ROOT}/reports/placement_fill_request_state_race_v2_development_20260728_v3/oof_v2/report.json`, SHA256 `1f9ea35aafd0429c60a2262ba01afb84447e1c2646b01cf49e8e265a1ec762af`.
- Daily curve metrics SHA256: `a649be77a320d7e6d8ecd509931ce531bac228535ef00b065506c074c84a5f90`.
- Strict native-L2 universe: 76 order-level days.
- Frozen Development panel: 50 days and 800,853 placement cohorts.
- Request-state panel: 2,402,559 action rows.
- Chronological OOF: 29 days from six expanding folds, each with one outer embargo day and a fit-size-matched five-day calibration tail.

The first fit specification required fully role-specific pending-fill calibration. It aborted before OOF predictions because the earliest five-day calibration tail had only 11 BUY and 22 SELL unique pending fills; BUY add had one and BUY reducing had zero. The frozen v2 fit specification therefore uses side-pooled pending-fill scale, while the base hazard remains role-aware and pre-request fill and ACK calibration remain role-specific.

## Frozen Horizons

The report points came only from the first outer base-train duration distribution:

| Phase | p25 | p50 | p75 |
|---|---:|---:|---:|
| pre-request exposure | 4,997ms | 5,894ms | 8,382ms |
| request-to-terminal race | 6ms | 7ms | 9ms |

These are reporting cuts, not fixed action horizons. The 6-9ms pending cuts also explain why ACK-before-fill is overwhelmingly common after a request.

## Development Gates

| Component | BUY | SELL | Result |
|---|---:|---:|---|
| pre-request fill, opener/add/reducing | 3/3 | 3/3 | all support, Brier and absolute-calibration gates pass |
| conditional cancel ACK, opener/add/reducing | 3/3 | 3/3 | all gates pass |
| pending fill before ACK, opener/add/reducing | 0/3 | 0/3 | no role has a positive Brier lower bound |
| joint pending fill/ACK/no-event race | 3/3 | 3/3 | all proper-score and support gates pass |

Pre-request fill Brier improvement is positive and calibrated for all six side-role curves. Mean improvements range from about `+0.00085` to `+0.00203` for BUY and `+0.00090` to `+0.00178` for SELL.

Conditional ACK is also positive and calibrated for all six curves. Mean Brier improvement is about `+0.0312` to `+0.0331`. This is the main structural change from the old natural cancel-hazard families, where integrated ACK calibration failed 0/6.

Pending-fill results are different:

| Side/role | events at largest report cut | Brier improvement mean | 95% day-cluster interval |
|---|---:|---:|---:|
| BUY add | 23 | -0.00000062 | [-0.00000183, +0.00000053] |
| BUY opener | 50 | -0.00000120 | [-0.00000324, +0.00000013] |
| BUY reducing | 63 | -0.00000096 | [-0.00000210, +0.00000009] |
| SELL add | 7 | -0.00000140 | [-0.00001391, +0.00001435] |
| SELL opener | 47 | -0.00000169 | [-0.00000445, +0.00000131] |
| SELL reducing | 108 | +0.00000063 | [-0.00000159, +0.00000340] |

BUY add and SELL add also fail the predeclared event-support floor. SELL opener has a small positive calibration bias whose day-cluster interval does not contain zero. No pending-fill curve has evidence of incremental proper-score value over the side-role-action exposure-only baseline.

## Interpretation

Mechanizing cancel request and conditioning ACK survival on request-time state resolved the old cancel-CIF structural error. The remaining failure is not general absolute calibration: it is a narrow, rare cause occurring during a 6-9ms ACK race. A richer pending-fill ML model is not supported by these data.

The complete learned family is closed because it required every phase and role to pass. The passing pre-request fill and ACK components may remain diagnostic building blocks, but this report does not grant them independent live or action authority.

If the placement family continues, the next specification should treat pending-cancel fill as a side-pooled empirical nuisance or conservative day-cluster upper-bound cost. It must then re-evaluate the integrated fill/ACK CIF on Development under a new identity. It must not relax the failed v2 gate, read Validation, or turn the ACK model directly into KEEP/CANCEL policy.
