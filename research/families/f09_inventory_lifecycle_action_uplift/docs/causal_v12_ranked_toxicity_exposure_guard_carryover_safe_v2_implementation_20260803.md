# Ranked-Toxicity Carryover-Safe V2 Implementation

Last materially modified: 2026-08-03

Status: one-day authoritative smoke passed; formal 40-day mechanics not run.

## Root Cause

The failed candidate order was not stale and had not missed a lifecycle event. Order `13612` was still `ACTIVE`, exchange-nonterminal, owned by the current assignment episode, and inside the real fill-risk set. It had been submitted as a reducing SELL order, then became exposure-increasing after inventory changed.

The v2 adapter incorrectly treated submit-time inventory role as immutable. The repair now permits this one explicit transition only while the same order is exchange-live and `fill_risk_active=true`. It retains the order's queue and assignment ownership and writes an `active_order_role_transition_to_exposure` journal event. Unknown, terminal, or non-fill-risk active-order identities still fail immediately.

## Verification

The in-scope targeted regression suite passed `49` tests. The authoritative `2026-04-17` smoke consumed all `29,072` untreated quote decisions.

| Side | Episodes | Completed | Censored | Carryovers | Role transitions |
|---|---:|---:|---:|---:|---:|
| BUY | 337 | 336 | 1 | 58 | 0 |
| SELL | 337 | 336 | 1 | 50 | 1 |

Both sides reported zero cross-arm ownership, forced washout cancels, owner mismatches, terminal risk-set reuse, and other zero-tolerance violations. `execution_complete`, `zero_tolerance_passed`, and `carryover_contract_valid` were all true.

Machine-readable identities and smoke counts are recorded in the adjacent [`implementation JSON`](causal_v12_ranked_toxicity_exposure_guard_carryover_safe_v2_implementation_20260803.json). The new v2 Spec also binds the exact authoritative replay and window-loader bytes used by this smoke.

## Boundary

The run read mechanics/lifecycle events only. It did not read PnL, reward, markout, Validation, or sealed holdout. It does not authorize an action or live deployment. The formal frozen 40-day mechanics panel remains unexecuted.
