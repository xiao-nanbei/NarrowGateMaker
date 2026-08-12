# Boolean Cooldown v1 Exploratory Selection Errata

Date: 2026-08-10

Historical identity: `multiscale_ema_boolean_cooldown_duration_policy_v1`

Successor identity: `multiscale_ema_boolean_cooldown_duration_policy_exploratory_oof_v1`

## Corrected Interpretation

The historical v1 result is a conservative deployment selector abstention. It is not an outer-OOF test of a non-control Boolean cooldown policy.

For each training fit, v1 calculated a simultaneous lower bound over the surviving rule family. When the selected rule list did not have a lower bound strictly above zero, it replaced the list with an empty rule set and `CONTROL_85N` before inner or outer execution. The one-standard-error complexity comparison could therefore include an all-control policy with zero mean and zero standard error.

Consequently, the reported 0% action rate and zero outer-OOF uplift mean:

```text
the deployment-oriented pre-screen abstained
```

They do not mean:

```text
an exploratory non-control Boolean policy was executed and failed in outer OOF
```

The original artifacts, hashes, and abstention result remain historical evidence. Their closure is narrowed to the exact pre-screened selector.

## Successor Selection Contract

The successor keeps the frozen v1 data, duration arms, predicate basis, complexity grid, support rules, chronological folds, embargo, and campaign weights. It changes only the ownership of evidence stages:

1. Inner Development search must select the best support-valid non-control candidate. A negative or zero training/refit LCB is recorded but cannot clear the candidate before outer OOF.
2. The all-control policy is not eligible as an exploratory complexity candidate.
3. Each frozen candidate is executed once on its untouched outer fold.
4. Only after all outer OOF rows are scored may the deployment gate abstain.
5. A failed outer gate closes deployment of that candidate; it does not rewrite the exploratory question as untested.

Validation and sealed holdout remain unread. The successor cannot grant action, F09, shadow, or live authority by itself.

## Implementation Corrections

- The Python duration fork now owns its target override and control-deadline state through explicit `nonlocal` bindings. The two affected mechanics fields are part of Python/C++ parity.
- Negating `last_cross_favorable` now requires an explicitly observed crossover. A never-observed crossover cannot become an adverse crossover by Boolean negation.
- The admitted Development panel has 8,600 opportunities and 45 crossover pairs; zero opportunities have a missing crossover state. This preserves the historical Development values while making prospective missing-state use fail closed.

## Censoring Denominator

All 8,600 opportunities materialized all eight arms. If any arm failed to reach the common economic washout before the data boundary, the entire opportunity was jointly censored. This excluded 171 opportunities and left 8,429 for model fitting. This is a whole-opportunity estimand restriction, not arm-wise complete-case repair, but it reduces the formal economic denominator and must be reported explicitly. Censor-time marks are diagnostic only and are not terminal-value labels.
