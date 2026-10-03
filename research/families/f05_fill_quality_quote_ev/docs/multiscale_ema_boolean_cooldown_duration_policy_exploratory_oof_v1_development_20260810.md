# Multiscale EMA Boolean Cooldown Exploratory OOF v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Date: 2026-08-10

Identity: `multiscale_ema_boolean_cooldown_duration_policy_exploratory_oof_v1`

Status: exploratory outer OOF complete; neither side is deployable.

## Boundary

The original `multiscale_ema_boolean_cooldown_duration_policy_v1` applied a deployment-strength simultaneous-LCB fallback before untouched outer OOF. It therefore evaluated an all-baseline policy and reported a 0% non-control action rate. That result remains valid as a conservative deployment abstention, but it does not test whether the best support-valid non-baseline Boolean rule transports out of sample.

This successor keeps the frozen duration arms, 45 EMA pairs, 360 predicates, chronological folds, censoring contract, and rule-search limits. In each outer fold it executes the best support-valid non-baseline rule selected using only earlier data, even when the inner or refit LCB crosses zero. Deployment gates are applied only after all untouched outer OOF rows are scored.

## Denominator

- Input opportunities: 8,600.
- Joint-censored opportunities: 171.
- Training-eligible opportunities: 8,429.
- Outer folds: four per side, eight total.
- Whole opportunities, not individual duration arms, are excluded by joint censoring; censor times are not used as economic labels.
- Validation and sealed holdout remain unread.

## Results

| Side | OOF rows | Campaigns | Days | Non-control rate | Point uplift (USDC/campaign-weight) | 95% LCB | Gate |
|---|---:|---:|---:|---:|---:|---:|---|
| BUY | 2,929 | 1,812 | 24 | 76.99% | -0.001511 | -0.004711 | Fail |
| SELL | 2,640 | 1,634 | 24 | 85.64% | -0.003952 | -0.018341 | Fail |

The candidate rules genuinely entered outer OOF, so this is not another all-baseline no-op. Both sides have negative point estimates and negative lower bounds. Neither side may proceed to F09 policy full-path replay, Validation, holdout, or live deployment under this identity.

This closes the frozen exploratory Boolean-duration candidate set. It does not establish that 85 seconds is optimal, and it does not generalize to a new state representation, duration set, lifecycle estimand, or policy class.

## Artifacts

Durable root: `${NARROWGATE_DATA_ROOT}/reports/multiscale_ema_boolean_cooldown_duration_policy_exploratory_oof_v1_20260810`

- Manifest SHA256: `89a63aea2fff66ebca1a66f44a36a531852748d2f702d5952d5ed6eaafde8fb7`
- Report SHA256: `c497ea1cc1ad11fdf46f61c49023ff9d31c6400e4189e02bd6a9f3230b8e6c28`
- OOF SHA256: `b6b9857403a27b28d92cbcc12a01d1abbc6c5bf91ec9345657e9ed2149d8f72f`
- Outer policies SHA256: `0765359565a8e2b679167d27a59f8e4583f5d25ff9bad28a3d5cf3ac25cb9ba9`

Permissions remain `action_authorized=false`, `f09_registration_authorized=false`, and `live_authorized=false`.
