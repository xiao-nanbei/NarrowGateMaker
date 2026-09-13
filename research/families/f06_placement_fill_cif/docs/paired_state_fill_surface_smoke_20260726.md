# Paired state fill surface lifecycle smoke (2026-07-26)

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Status

This is a mechanics and data-lineage result. It is not a fitted fill surface, action uplift estimate, campaign-PnL counterfactual, or live-policy gate.

The frozen contract is `docs/paired_state_fill_surface_v1_spec_20260726.json` (SHA256 `fc20d96c2cf72a1e9c524f157cd118d66ee54f1e6535915dc752965c0455baa2`). It keeps Validation and sealed holdout unread. The smoke uses Development day 2026-05-13 only.

## Estimand and mechanics

Each actual baseline place/replace side-decision creates three independent shadow children:

- `closer_1tick`
- `current`
- `farther_1tick`

The children share submit time, the baseline new-order latency draw, public trades, native snapshot/delta events and the frozen baseline cancel request/ACK schedule. Each child separately resolves GTX activation, native visible queue, exact-price queue consumption and strictly-through fill. Shadow fills never change inventory, campaign state or subsequent baseline decisions.

The fixed placement horizons start at submit/decision time; the conditional active-order horizons start at activation. The panel also stores requested and effective action prices separately, and keeps the request-to-ACK interval in the fill exposure.

The primary future target is direct total fill CIF. Exact-price queue fill and through fill are mechanism diagnostics; their sum is not called queue conversion. If native queue support is unknown or same-millisecond ordering is ambiguous, exact-price fill is not fabricated. A strictly-through print remains identified because it proves that every better resting price was exhausted.

The operational BUY q90 cancel/re-entry action is not implemented by this replay. It is hashed and excluded as the frozen separate treatment `buy_exposure_adverse_q90_cancel_reentry_v1`. The resulting follow-up is named `frozen_baseline_followup_shadow`, not operational live probability.

## Smoke result

The expanded smoke contains 1,000 cohorts and 3,000 shadow children:

| Dimension | Count |
|---|---:|
| BUY / SELL | 551 / 449 |
| opener / reducing / add | 442 / 439 / 119 |
| baseline cancel-ACK follow-up observed | 949 |
| pathwise monotonicity violations | 0 |

| Action | Native queue valid | Any touch | Any fill | Exact queue fill | Through fill | Fill rate |
|---|---:|---:|---:|---:|---:|---:|
| closer 1 tick | 950 | 43 | 42 | 4 | 38 | 4.20% |
| current | 959 | 41 | 40 | 6 | 34 | 4.00% |
| farther 1 tick | 969 | 39 | 39 | 10 | 29 | 3.90% |

The total fill path is monotone in distance. Exact fills rise with distance while through fills fall because these are different mechanisms, which is why the direct CIF must remain primary.

Native queue invalidation affected 50/41/31 children for closer/current/farther, mostly because an activation, trade and native level update shared a millisecond. These rows remain in action-specific activation and through-fill diagnostics but are excluded from strict exact-queue inference.

Artifacts:

- `${NARROWGATE_RETIRED_DATA_ROOT}/reports/paired_state_fill_surface_v1_smoke_20260726/lifecycle_smoke1000_2026-05-13/paired_order_lifecycle_smoke.parquet`
- `${NARROWGATE_RETIRED_DATA_ROOT}/reports/paired_state_fill_surface_v1_smoke_20260726/lifecycle_smoke1000_2026-05-13/manifest.json`

The output is under 1 MiB and left 61.71 GiB free, above the project's 60 GiB reserve. A full multi-day panel is still prohibited at this storage level.

## What is now supported

The smoke closes the minimum mechanics gate for a future placement panel:

- one row per causal side-decision;
- actual baseline inventory role and campaign state so far;
- action-specific activation and GTX outcome;
- native-deep queue seed and subsequent cancel/refill path;
- exact vs through touch/fill identity;
- partial fill, cancel request/ACK and ACK-before/fill-before-ACK ordering;
- 1s/5s/10s and lifecycle support indicators;
- zero pathwise distance-monotonicity violations.

It does not close the statistical gate. The single day has only 39-42 fills per action and only 119 add decisions. No model should be fitted from this smoke. The next admissible step is to free additional storage, stream the Development panel by day, then fit direct dynamic fill CIF under the frozen candidate-model and prediction-gate contract. KEEP/REPLACE remains a separate active-order estimand and must not be inferred from this placement panel.
