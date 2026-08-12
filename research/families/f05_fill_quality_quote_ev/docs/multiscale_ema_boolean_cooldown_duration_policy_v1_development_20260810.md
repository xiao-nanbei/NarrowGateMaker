# Multiscale EMA Boolean Cooldown Duration Policy v1 Development Result

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Date: 2026-08-10

Identity: `multiscale_ema_boolean_cooldown_duration_policy_v1`

Status: `development_closed_no_side_policy_frozen`

## Question

This exploratory identity asked whether a side-specific sparse Boolean policy over the complete multiscale EMA-pair state could choose a total exposure-increasing add cooldown duration with higher terminal value than the current `CONTROL_85N` behavior.

It is distinct from v1.2. The earlier v1.2 identity estimated `ADD NOW` versus `WAIT ONE EXTERNAL EPOCH` with linear models. This identity instead evaluated a state-by-duration value surface and allowed bounded AND/OR/NOT rule lists to choose among eight frozen duration arms per side.

## Frozen Evidence

- The 2025 provider panel supplied unsupervised predicate normalization only: 66 admitted days and 112,090,884 side-specific 100 ms BBO rows. It supplied no cooldown reward, queue, fill, lifecycle, or PnL authority.
- Native 2026 data supplied every economic label: 40 historical Development days under `current_live_held_global_ber_control`.
- The full census contained 8,600 legal exposure-increasing fill opportunities and eight arms per opportunity, or 68,800 full-path fork rows.
- Joint censoring excluded 171 whole opportunities. No sampling or complete-case arm filtering was used; 8,429 opportunities remained eligible for fitting.
- BUY and SELL were trained separately over all 45 EMA pairs and 360 frozen predicates.
- Model discovery used four outer chronological folds, three inner chronological folds per outer fold, and day/campaign-clustered simultaneous bounds with 500 bootstrap draws.
- Validation and sealed holdout were not read.

## Outer OOF Result

| Side | OOF rows | Campaigns | OOF days | Non-control action rate | Uplift | LCB | Passed |
|---|---:|---:|---:|---:|---:|---:|---:|
| BUY | 2,929 | 1,812 | 24 | 0.00% | 0.0000 USDC | 0.0000 | No |
| SELL | 2,640 | 1,634 | 24 | 0.00% | 0.0000 USDC | 0.0000 | No |

For both sides, every one of the four outer-fold policies contained zero ordered rules and used `CONTROL_85N` as its default action. The inner search therefore produced no executable non-control duration policy. The zero OOF uplift is the value of an all-control policy, not evidence that every individual duration arm has exactly zero effect.

The frozen progression gate required a side-specific outer-OOF lower bound strictly above zero. Neither side passed, so no side was refit on full Development and no policy-level replay was authorized.

## Decision

The following steps were intentionally not run:

- learned-policy 40-day full-path replay;
- restart-aware continuous confirmation;
- F09 randomized action registration;
- Validation or sealed-holdout evaluation;
- live or shadow deployment.

The exact closure is:

```text
multiscale_ema_boolean_cooldown_duration_policy_v1
  -> BUY: no non-control outer-OOF policy
  -> SELL: no non-control outer-OOF policy
  -> Development closed
```

This closes this frozen Boolean duration identity. It does not establish that 85 seconds is optimal, does not close all future cooldown research, and does not rewrite the separate v1.0, v1.1, or v1.2 results.

## Integrity Notes

Before formal publication, two validation defects were caught and corrected: the inner-fold chronology validator now checks the actual three inner folds per outer fold, and the policy validator preserves the frozen predicate schema order. Side checkpoints are non-authoritative, hash-bound, and removed after a successful atomic final publication. The result-producing learner and orchestration hashes are bound in the v3 execution amendment.

The owner-authorized atomic admission target is:

`${NARROWGATE_DATA_ROOT}/reports/multiscale_ema_boolean_cooldown_duration_policy_v1_20260810`

Admission completed and was independently revalidated on 2026-08-10: 17,449 files, 7,576,857,347 bytes, 68,800 formal arm rows in both the chunk and admitted views, and 8,600 census rows. Admission identity SHA256: `f168fe4324c7210285b0dfde5a43533a9790bc587749332b9c4b6c7c85a877ed`.
