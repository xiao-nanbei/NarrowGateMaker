# F03 1s Restart-Aware Continuous Execution v1.2

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Status: `execution_layer_implemented_policy_artifacts_unbound_outcomes_closed`

## Scope

This successor implements the actual 71-day continuous execution substrate without modifying the frozen v1/v1.1 identities or the separate 40-day native runner. The control is current v9 10-second ML-ON; the candidate is true 1-second ML-ON. q90 action and BUY fill-selection are OFF in both arms.

The current command surface is deliberately outcome blind. `prepare` compiles and atomically admits the full operation tape; `validate` rechecks its canonical identity, runtime code, sources, policies and authority. The execution API exists and is tested, but the formal plan remains blocked until both exact 71-day policy artifact manifests are supplied. The candidate manifest must also bind the admitted 40-day exact-native primary receipt by hash, so this sensitivity cannot run first or replace the primary. No PnL or economic result was read while implementing this layer.

## Continuous State

Each arm owns one state chain across all 71 UTC dates. Cash, position, average entry price, economic campaign and cumulative accounting cross midnight unchanged. UTC day boundaries produce accounting/cluster slices only; they do not flatten inventory, clear orders, reset runtime state or create a fresh simulation.

The compiled tape contains six operation types: online trading, cancel drain, offline gap, causal warmup/re-entry, UTC accounting, and one panel terminal. Planned restart drains require terminal ACK/fill handling before orders, queue and cursors are cleared. Only process-local runtime state resets. The two arms use the same operation tape and the same deterministic latency/random-path receipt, but retain independent order, inventory and campaign paths.

Inventory is marked through every offline gap. The panel terminal applies one final inventory MTM without forcing the position flat. Gap trading, daily forced flat and UTC state reset fail closed in the executor.

## Authority

Every operation carries an explicit authority tier. Native online intervals may carry exact queue/lifecycle authority. Provider-normalized intervals contribute only continuous PnL, inventory and campaign sensitivity. Gaps, warmup, UTC accounting and panel terminal never acquire exact queue, lifecycle or q90 authority. This 71-day substrate is required sensitivity evidence after the 40-day exact-native primary; it cannot replace that primary experiment.

## Durability

Large output is restricted to the `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` cache. Each operation publishes a receipt and paired arm checkpoints through staging, file fsync, atomic rename and directory fsync. The checkpoints bind both economic state and opaque engine state in a chained SHA256 identity. Resume starts only from the last atomically admitted operation and fails on progress/checkpoint disagreement, source drift, overlay drift or runtime-code drift.

The execution layer does not aggregate, score or promote economic results. Promotion, action, live and baseline-replacement permissions remain false.

## Outcome-Blind Admission

The admitted plan is stored at `${NARROWGATE_DATA_ROOT}/cache/replay_dag/f03_causal_v12_1s_restart_aware_continuous_execution_v1_2/execution-plan.json`. Its canonical identity is `35b19a6b5b492bbce67e5b0d01e56a0517a7ca5f4c00c6bee05ae7d8ddb5d370`.

The tape has 523 operations: 131 online intervals, 106 cancel drains, 107 offline gaps, 107 warmup/resume events, 71 UTC accounting slices and one panel terminal. There are zero operation overlaps. The 106 drains correspond to every restart after the initial panel-start offline interval. All 213 source artifacts passed full SHA256 revalidation; provider exact-queue and exact-lifecycle operation counts are both zero.

The admitted plan remains `execution_eligible=false` with exactly two blockers: `control_71_day_policy_artifacts_unbound` and `candidate_71_day_policy_artifacts_unbound`. Economic outcomes remain unread.
