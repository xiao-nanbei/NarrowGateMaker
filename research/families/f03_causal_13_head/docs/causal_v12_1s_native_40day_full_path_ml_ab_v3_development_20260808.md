# Causal-v12 1s Native 40-Day Full-Path ML A/B v3

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Decision: close the current 1-second cadence successor on Development. Keep the operational v9 10-second causal-v12 baseline unchanged. Validation, sealed holdout, action, and live permissions remain closed.

## Frozen Comparison

- Control: current v9 10-second ML-ON policy.
- Candidate: retrained 1-second full-schema ML-ON policy.
- Days: the frozen 40-day native Development panel.
- Shared mechanisms: P3 v2, q90 action OFF, BUY fill-selection action OFF, quote/execution ABI, latency, queue, cooldown, inventory, and accounting semantics.
- Runtime source identity: frozen `models/data_windows.py` SHA256 `91a690baf9636a5f9f6665d5f4a5385b114d4efebb00f82eb2a32455bf6b7223`.

## Result

| Metric | v9 10s control | 1s candidate | Candidate - control |
| --- | ---: | ---: | ---: |
| Terminal MTM PnL | -161.935091 USDC | -164.052065 USDC | -2.116974 USDC |
| Terminal MTM PnL/day | -4.048377 | -4.101302 | -0.052924 |
| Closed-campaign value | -162.123791 USDC | -163.690165 USDC | -1.566374 USDC |
| Fills | 16,959 | 14,758 | -2,201 |
| Fill retention | 100% | 87.0216% | -12.9784% |
| Inventory time | 5,327.642 BTC*s | 6,216.379 BTC*s | +888.737 BTC*s |

The day-clustered terminal-MTM difference was `-0.052924 USDC/day`, 95% interval `[-1.106631, +1.005055]`, with 19/40 positive days. Closed-campaign value was also negative on point estimate and its lower bound was not positive.

Tail/lifecycle direction was materially unfavorable:

- Campaign CVaR10 protection: `-0.176559/day`, 95% interval `[-0.294295, -0.066028]`.
- q10 shortfall protection: `-0.059563/day`, 95% interval `[-0.091067, -0.029543]`.
- Inventory-time avoidance: `-22.218436 BTC*s/day`, 95% interval `[-34.376329, -8.407483]`.
- Maximum-inventory avoidance: `-0.000975 BTC/day`, 95% interval `[-0.001700, -0.000150]`.
- Multi-level LONG terminal value changed from `-63.237078` to `-93.231090 USDC`.

Execution integrity passed for the surfaces this runner bound: 173-field feature parity, zero Python/C++ feature mismatches, zero C++/Python fill-path mismatches, identity/hash parity, and campaign accounting error below `2.8e-13 USDC`. Prediction-output and tick/GTX/spread-cap parity were not bound by this runner and therefore cannot be claimed.

## Interpretation

The candidate reduced participation but did not improve value, and it increased inventory exposure and worsened campaign tails. This is not a near-pass and does not justify reading Validation or running the 71-day continuous confirmation. It closes only this 1-second cadence/model/policy successor; it does not prove that every future sub-10-second model is harmful.

Authoritative report: `${NARROWGATE_DATA_ROOT}/cache/replay_dag/f03_causal_v12_1s_native_40day_full_path_ml_ab_v3/report.json`, SHA256 `0509a462b8bd991cfec9d5239102d0ef91abe240e7c6e6853e21a4b9da05d9ad`.
