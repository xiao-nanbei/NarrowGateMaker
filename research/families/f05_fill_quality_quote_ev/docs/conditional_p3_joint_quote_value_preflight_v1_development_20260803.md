# Conditional P3 Joint Quote Value Preflight v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Decision

Stop before fitting a direct quantity-weighted USDC value model.

```text
support_sufficient = false
economic_outcomes_read = false
value_model_fit_authorized = false
action_experiment_authorized = false
live_authority = false
```

## Frozen Question

This Development-only preflight asked whether the existing F06 paired lifecycle denominator had enough exact-distance support to train a joint quote value model using side-specific conditional P3 only as an ordinary input.

The candidate set contained the baseline joint quote plus independent BUY or SELL moves of 1, 2, or 4 ticks. It did not assume cross-side additivity, did not multiply P3 outside the future value model, and did not read PnL, reward, or markout.

## Result

| Support item | Observed | Required | Passed |
|---|---:|---:|---:|
| Distinct days | 28 | 30 | No |
| OOF folds | 3 | 4 | No |
| Minimum side x role x action fills | 1 | 30 | No |
| Paired joint quote buckets | 282 | diagnostic | N/A |
| Exact BBO clock | yes | yes | Yes |
| All candidate grid actions activated | yes | yes | Yes |

The thinnest cell was SELL add at `farther_4tick`, with one fill. Other side-role cells were also far below the frozen 30-fill minimum. Fitting a high-dimensional direct value surface here would estimate sparse lifecycle noise rather than action value.

F02's later policy-visible decision-cadence audit repaired the BBO visibility comparison and strongly supported conditional P3 prediction quality. It does not repair this independent F05 lifecycle/value support failure.

## Boundary

No value model, simultaneous confidence band, candidate action, randomized replay, Validation read, sealed-holdout read, or live permission was created. The correct next step is a new support-bearing exact decision/value denominator under a new frozen identity, not weaker cell gates or pooling across side and role.

## Frozen Artifacts

- Spec: `docs/conditional_p3_joint_quote_value_preflight_v1_spec_20260803.json`
- Spec SHA256: `6a9dbf0d2855cc0afa9d626aef938a7784ea632a3704247ba2f435b0a89be689`
- Authoritative report SHA256: `7e4968480be654902f8a03b6bc0c51d7da800a53b39d20cf3e496b3e75de8e5f`
- Authoritative output: `${NARROWGATE_DATA_ROOT}/reports/conditional_p3_joint_quote_value_preflight_v1_development_20260803`
