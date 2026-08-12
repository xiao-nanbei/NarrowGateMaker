# F03 1s Restart-Aware Continuous A/B v1.1 Execution Binding

Last materially modified: 2026-08-05

Status: `execution_plan_skeleton_candidate_unbound_results_closed`

## Scope

This successor amendment repairs execution bindings without modifying the frozen v1 preflight or its parent economic precommit. The v1 preflight recorded the parent precommit SHA256 as `f949c9...`, while the current frozen parent file is `9d4c07...` with canonical identity `7bc07d...`. The drift is acknowledged and resolved only through this v1.1 amendment; neither historical document is rewritten.

The amendment remains outcome blind. It does not bind a candidate bundle, run the tick engine, define an economic result reader, read PnL, or grant execution, action, live, Validation, holdout, or baseline-replacement authority.

## Source Authority

The complete 2026-04-17 through 2026-06-26 source identity contains 71 chronological days, exactly 52 native days and 19 provider-normalized sensitivity days. Every day binds BBO, L2, and feature artifacts by resolved path, byte size, and SHA256, for 213 artifacts total. Its canonical identity is `1bb7f8...d147`.

Provider-normalized days may contribute continuous PnL, inventory, and campaign sensitivity. They never receive exact queue or exact lifecycle authority. This tier is carried in each execution request and is machine validated before any future runner may consume the plan. Native exact authority is also excluded inside frozen restart gaps; the restart-aware result cannot upgrade missing gap lifecycle evidence.

## Restart And Accounting Interfaces

The amendment binds `restart_boundary_contract.v1` and `continuous_accounting_contract.v1` by implementation SHA256 and method ABI. The validator executes an outcome-free contract probe which confirms:

- an order awaiting cancel ACK prevents the transition to offline;
- cancel ACK terminality permits transient-state clearing;
- restart requires a fresh snapshot identity;
- `feature_ready_ts` later than `decision_ts` prevents quoting;
- continuous marked-equity accounting remains valid after restart.

Future execution requests require observed cancel terminality, source-covered warmup, and causal feature-ready attestations. These requirements do not claim that the 71-day full path has run; they are fail-closed inputs to the future executor.

## Candidate DAG

Once a candidate exists, its DAG must exactly match the canonical full 1-second feature contract: semantic identity, 1,000 ms cadence, 13-head linkage, feature order, source clocks, availability rules, and source manifest. The candidate bundle training identity must independently bind the same cadence, head order, and feature order.

Until the bundle, DAG, and exact 71 daily overlays are attached through a later execution amendment, the default CLI continues to stop at `candidate identity is not bound`. This v1.1 artifact is an execution-plan binding, not a completed full-path experiment.
