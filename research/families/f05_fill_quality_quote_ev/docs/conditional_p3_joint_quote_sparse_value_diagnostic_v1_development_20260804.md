# Conditional P3 Joint Quote Sparse Value Diagnostic v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Dual-Path Decision

The frozen `conditional_p3_joint_quote_value_preflight_v1` hard gate remains failed and immutable:

```text
hard_gate_path.passed = false
owner_progression_path.support_accepted = true
```

The owner accepted the observed `28 days / 3 folds / 1 minimum cell fill` support and authorized an outcome-informed Development continuation. This did not relabel the original `30 days / 4 folds / 30 fills` gate as passed.

## Owner-Path Result

The successor reconstructed direct quantity-weighted, single-side baseline-terminal overlay deltas for the frozen 13-action joint quote set. Of 282 paired buckets, 269 had complete value support. Three expanding chronological folds produced 126 OOF buckets over 13 days.

No non-baseline action passed the past-only day-clustered simultaneous screen:

| Item | Result |
|---|---:|
| Supported action-fold cells | 0 |
| OOF candidate selections | 0 / 126 |
| OOF baseline fallback | 100% |
| OOF selected-value mean | 0 USDC/bucket |

The largest early-fold point estimate was `BUY farther 4 ticks` at about `+3.7e-5 USDC/bucket`; its simultaneous lower bound was about `+1.6e-5`, still below the frozen `1e-4 USDC/bucket` economic threshold. In the later two folds the same action's lower bounds crossed zero.

Decision:

```text
owner_proxy_signal_not_supported_stop_before_action_identity
```

## Estimand Boundary

This was a full-information proxy diagnostic using F06's frozen baseline campaign-terminal overlay. It did not regenerate the complete action-dependent inventory, cooldown, queue, or campaign path and did not identify cross-side interactions. It therefore cannot authorize an F09 action even if a proxy point estimate is positive.

Both governance paths remain capable of reaching live in general. A standard hard-gate successor would use `research_supported_promotion`; a future owner successor with positive full-path economics and execution/safety parity would use `owner_risk_accepted_promotion`. This exact owner branch stops because the economic proxy produced no supported candidate.

Validation and sealed holdout were not read. No action or live permission was created.

## Frozen Artifacts

- Owner progression Spec SHA256: `a7d8367a197ef38c4ac1b8ccde92f5b71d2b33dff3d6b8c85e8284b0fb59f592`
- Owner progression result SHA256: `f223a1cdace586f8d14b52868f4249e4071637350b23005f5fa66858c2800a91`
- Sparse diagnostic Spec SHA256: `a5484090ef4c5e75b8016216f8b9b65f994473371f6da4c74ea84478eb3d17c1`
- Authoritative report SHA256: `1178770144e749272f2df13445d9abcf30467ab02dfaf4dae650ee48ed6ac118`
- Authoritative output: `${NARROWGATE_DATA_ROOT}/reports/conditional_p3_joint_quote_sparse_value_diagnostic_v1_20260804`
