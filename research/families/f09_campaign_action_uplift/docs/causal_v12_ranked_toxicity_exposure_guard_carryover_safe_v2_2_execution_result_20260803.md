# Ranked-Toxicity Carryover-Safe V2.2 Execution Result

Last materially modified: 2026-08-03

Status: one-day plumbing smoke passed; formal mechanics not run.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Purpose

V2.2 preserves the frozen v2 and v2.1 records and replaces only the failed cross-identity tape reuse. It regenerates the untreated baseline shadow and candidate path under the same current loader, replay-cache DAG and code identity. The frozen `0.8` threshold is plumbing-only and carries no prediction or action authority.

## One-Day Result

The authoritative `2026-04-17` baseline produced `29,072` rows. The candidate consumed all `29,072`, leaving zero unconsumed rows.

| Contract counter | BUY | SELL |
|---|---:|---:|
| Baseline-shadow mismatch | 0 | 0 |
| Cross-arm order ownership | 0 | 0 |
| Forced washout cancel | 0 | 0 |
| Order-owner mismatch | 0 | 0 |
| Role lifecycle valid | true | true |
| Carryover lifecycle valid | true | true |
| Carryover transitions | 58 | 50 |

SELL retained one valid active-order role transition to exposure; BUY retained none. Both role and carryover lifecycle contracts passed. Old v2 episode counts were not used as a gate.

## Cache Contract

All replay-cache writers remained disabled. There were zero write attempts, the baseline and candidate cache trees were unchanged, and both passes used the same read-only cache identity. Legacy v13/components reads remained compatible; `WindowData` stayed ephemeral. The complete output uses the private locator `${NARROWGATE_EPHEMERAL_ROOT}/f09_carryover_v2_2_plumbing_smoke_20260803_run1` and is not distributed with the public repository.

## Verification

The v2.2, v2.1 and frozen carryover-safe v2 assignment tests passed: `21 passed`. The machine-readable identities, manifest hashes and allowed counters are recorded in the adjacent [`result JSON`](causal_v12_ranked_toxicity_exposure_guard_carryover_safe_v2_2_execution_result_20260803.json).

## Boundary

No PnL, reward, markout, Validation or sealed holdout result was read. No 40-day run was started. This result proves only the one-day baseline/candidate plumbing and lifecycle contract; it grants no action or live authority.
