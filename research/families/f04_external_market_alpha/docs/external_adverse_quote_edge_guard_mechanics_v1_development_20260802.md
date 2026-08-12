# External Adverse Quote Edge Guard Mechanics v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Status: Development outcome-blind mechanics complete; clock and denominator limited. No prediction, action-experiment, Validation, sealed-holdout, or live authority.

## Decision

Retain this run as BABEL E6/P2 mechanics evidence. It shows that a conservative all/LOO-consistent external adverse-edge state can survive the recorded AWS Tokyo visibility clock and move an outward-only quote coordinate. It does not provide the exact opportunity clock, lifecycle path, support, or economic evidence required to register an action.

P1 remains an independent background route. Its 30-day requirement blocks only `first_add_external_incremental_value_m0_m1_v1`; it did not block this run.

## Frozen Identity

| Item | Value |
|---|---:|
| Spec SHA256 | `aedf8b18afc1624f6c92e80cf6d7285c23de3dcc23a248a8f66538b38d0716eb` |
| Implementation SHA256 | `94cb3376b8499cf97b4c297093ff511d24af6236c63ca6e46b3d93418ccdbfaf` |
| Report payload SHA256 | `a52f0f13dc6abdcfc4179aced083338628be60b98c48ac44332ab7eafabaa922` |
| Capture ledger SHA256 | `99acb42418ed792262212a1c7da1e59c64f9fa2503f9661567c8ea74551c8fdf` |
| Cache root | `${NARROWGATE_CACHE_ROOT}/replay_dag/external_adverse_quote_edge_guard_mechanics_v1/20260802` |

The immutable ledger contains 19 valid full windows over 18 distinct UTC days. Historical quote logs cover 17 windows over 16 days; 2026-07-19 and 2026-07-25 have no quote-opportunity denominator. The run evaluated 9,138 paired quote decisions, or 18,276 side-opportunities.

## Inventory Contract Correction

`quote_decisions.inventory_ratio` is `abs(q) / max_inventory`; it does not carry the inventory sign and cannot identify opener/add/reducing. The first draft run that treated it as signed was discarded before publication.

The authoritative run reads only `timestamp` and signed `position` from the two historical state journals, joins the latest strictly-prior state, and treats a same-millisecond state update as role-ambiguous and guard-ineligible. Across the full quote-log union, 456,203 of 456,210 pairs have an observable role, one is same-millisecond ambiguous, and six precede the available state journal. Every pair used in the 16 evaluated capture days has an observable role.

## Receive-Time Mechanics

The runner consumed 50,359,162 public book events. Recorder files contained 4,481 input-order regressions in `feature_ready_ts_ns`; the maximum observed disorder was 209.563275ms. The frozen 5,000ms bounded reorder contract passed for every consumed tape, and the evaluated panel has zero future-feature-time violations.

At zero added delay:

| Side / role | Opportunities | Guard-eligible | Triggers | Trigger rate over eligible |
|---|---:|---:|---:|---:|
| BUY opener | 2,671 | 2,329 | 15 | 0.644% |
| BUY add | 3,213 | 1,558 | 3 | 0.193% |
| SELL opener | 2,671 | 2,669 | 5 | 0.187% |
| SELL add | 3,254 | 1,230 | 3 | 0.244% |
| Reducing, both sides | 6,467 | 0 | 0 | unchanged by contract |
| Total opener/add | 11,809 | 7,786 | 26 | 0.334% |

BUY triggered on 7 of the 16 evaluated days and SELL on 5. Only 6 of the 26 triggers were add opportunities; 20 were openers. This is direct evidence that the current P2 surface is not the exact first-add surface used by P1.

Only 89 of 9,138 quote pairs produced a conservative all/LOO-consistent adverse direction. The most common non-trigger states were a nonnegative conservative edge (6,021 pairs) and LOO direction disagreement (1,911 pairs); warmup and source-validity failures were kept separate. Thus the low action rate is mostly the result of the predeclared robust-consensus contract, not spread-cap clipping.

Trigger survival from the sampled quote-log opportunity was:

| Added visibility delay | BUY | SELL |
|---|---:|---:|
| 10ms | 88.9% | 87.5% |
| 100ms | 72.2% | 75.0% |
| 500ms | 61.1% | 75.0% |

The 18 BUY triggers requested a median 43 ticks outward, and the 8 SELL triggers requested a median 21.5 ticks. Their p90 values were 117.5 and 86.6 ticks. No trigger was clipped by the frozen 20bps pair-spread cap. The projected action mix was 13 places and 13 replaces; the 13 projected queue resets are upper bounds because the seven-tape capture has no authoritative order/ACK/queue journal.

Observed trigger episodes have approximately five-second median duration, but that number inherits the sparse historical quote-log cadence. It is not a continuous 100ms state-duration estimand.

## Boundary

This run read no reward, PnL, markout, future price, Validation, or sealed holdout. It selected no threshold and registered no policy.

The historical denominator remains non-authoritative because it lacks a stable decision ID, uses millisecond post-decision log-write time, uses a millisecond post-state-update inventory clock, omits two valid ledger days, and does not contain the exact cancel/ACK/queue path. Therefore:

- `transport_supported=false`
- `prediction_supported=false`
- `action_experiment_authorized=false`
- `live_deployment_authorized=false`

A successor must emit a native exact opportunity tape containing decision ID, decision-start and feature-ready timestamps, signed inventory and role, quote coordinates, order identity, activation, cancel/ACK, queue reset, and final action. P1 and P2 may merge only after both refer to that same exact quote surface: exact first-add, or a separately preregistered all-exposure-increasing prediction denominator.
