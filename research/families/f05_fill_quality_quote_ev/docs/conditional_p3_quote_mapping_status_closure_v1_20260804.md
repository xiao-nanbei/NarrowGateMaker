# Conditional P3 Quote-Mapping Status Closure v1

Last materially modified: 2026-08-04

## Decision

Close the current F05 sparse-value quote selector. Do not close conditional P3.

The retained prediction statement is:

```text
p3_touch_volatility_conditioned_v4_1
= historical Development touch-probability prediction evidence
```

It remains subject to the disclosed owner coverage override and is not an independent confirmation or an operational P3 replacement.

## Two Mapping Results

The scalar adapter averaged BUY and SELL, compressed the conditional surface into dynamic `delta_star` and `kappa`, and reused both the AS/GLFT spread path and spread floor. That mapping is closed because `P_touch(d | x)` is not an AS fill-hazard elasticity and the adapter narrowed the quote twice through mismatched estimands.

The later F05 branch kept side-specific exact-distance P3 inputs and estimated quantity-weighted terminal-value overlays for 13 joint-quote candidates. Its mechanism direction was valid, but its economics were not actionable:

| Item | Result |
|---|---:|
| Complete value coverage | 269 / 282 = 95.39% |
| Outer OOF support | 13 days, 126 buckets |
| Supported action-fold cells | 0 |
| Baseline fallback | 126 / 126 |
| Strongest early point estimate | about `3.7e-5 USDC/bucket` |
| Frozen economic threshold | `1e-4 USDC/bucket` |

Later-fold intervals crossed zero. The blocker is economic resolution and chronological stability, rather than widespread missing value data.

The sparse study remained a baseline-terminal overlay. It did not regenerate inventory, queue, cooldown, campaign, or cross-side paths. Because it produced no executable candidate, paying for a 13-arm full-path replay is not justified.

## Governance Boundary

The immutable hard-gate failure remains recorded. The owner accepted an outcome-informed Development continuation, but that continuation also stops in F05. `owner_risk_accepted_promotion` is only a possible label for a future, independent identity that first produces positive full-path economics, execution parity, and safety support. It is not authority held by this branch.

Development economic outcomes were read. Validation and sealed holdout were not read. No F09 action or live permission was created. Thresholds, folds, and the candidate set must not be changed on the consumed Development panel.

The current operational P3 v2 remains unchanged. Conditional P3 v4.1 may be retained as shadow prediction/input evidence; both tested quote mappings have failed to produce an authorized action.
