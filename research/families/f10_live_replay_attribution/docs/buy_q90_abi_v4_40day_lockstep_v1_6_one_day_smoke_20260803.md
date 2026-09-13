# BUY q90 ABI v4 40-Day Lockstep v1.6 One-Day Smoke

Last materially modified: 2026-08-12

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Decision

The authoritative `2026-04-22` Development mechanics-only smoke passed under the v1.6 exact feature-ready visibility contract. The formal 40-day run was not started because the internal disk had only 56.63 GiB available, below the frozen 60 GiB reserve:

```text
one_day_event_lockstep_supported=true
storage_gate_blocked_40d=true
formal_40day_run_started=false
full_40day_mechanics_supported=false
```

No cache was deleted. No PnL, markout, campaign economics, Validation, or sealed holdout data was read. No deployment occurred, no threshold changed, and q90 operational action remained off.

## Visibility Contract

Exchange truth remains native-sequence ordered. Strategy visibility is batched by exact `feature_ready_ts_ns`. If an active BUY exact-price path is touched by book and execution evidence at the same exact ready boundary, within-batch ordering is unknowable and both runtimes persistently invalidate the path as `same_ms_exchange_book_ambiguity`.

The smoke synchronized 214 such Python authoritative path invalidations into the message-oriented C++ runtime. Three q90 evaluations carried the ambiguity reason, and Python/C++ mismatch remained zero. Distinct feature-ready boundaries are not broadened into a one-millisecond ambiguity window.

## Mechanics Result

| Check | Result |
|---|---:|
| Python/C++ mismatch | 0 |
| post-terminal hazard reuse | 0 |
| post-terminal cursor reuse | 0 |
| terminal cursor retention at end | 0 |
| unsupported terminal route | 0 |
| native invalid sequence | 0 |
| native source gap | 0 |
| future feature time | 0 |
| cancel request / ACK | 3 / 3 |
| post-cancel recovery / re-entry | 3 / 3 |
| terminal exchange-exposure complete | 8,797 / 8,797 orders |

The isolated mechanics replay deliberately applies the frozen q90 policy so that cancel/recovery transitions can be exercised. This does not alter the operational baseline: q90 live and default-backtest action remain off.

The day generated 61 prospective recovery evaluations, all invalid. This does not fail the event-lockstep smoke because invalid attribution remained closed and both runtimes agreed, but it supplies no valid prospective-score transport evidence. It must remain an explicit limitation of any later 40-day result.

## Persistence

The admitted day JSON is 6,441 bytes and the partial report is 6,735 bytes. They contain aggregate mechanics and lifecycle-audit counters only; the 34,554 row lifecycle journal was neither persisted nor returned across a worker boundary.

Authoritative result: `${NARROWGATE_DATA_ROOT}/reports/buy_q90_abi_v4_40day_lockstep_v1_6_20260803/development/days/2026-04-22.json`

Machine-readable smoke identity: [`buy_q90_abi_v4_40day_lockstep_v1_6_one_day_smoke_20260803.json`](buy_q90_abi_v4_40day_lockstep_v1_6_one_day_smoke_20260803.json)

## Authority

This result supports only the one-day Development event-lockstep mechanics contract. It does not complete the frozen 40-day denominator and creates no economic, action, Validation, holdout, deployment, or live authority. A future 40-day attempt must first pass the 60 GiB internal reserve without deleting caches and must use this unchanged v1.6 identity.
