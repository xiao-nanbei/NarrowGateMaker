# Volatility-Time Add Rearm Live-Stack Parity v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Status: Development-only mechanics/parity completed; randomized action held.

This identity replaces v2.1's unbound blocker booleans with hash-bound source, artifact, event-tape, latency, test, and Python/C++ evidence. It reads no reward, PnL, markout, Validation, or sealed holdout.

## Result

- Decision: `hold_action_candidate_path_and_cpp_q90_parity_incomplete`.
- Variance-time clock mechanics: supported.
- Full live-stack parity: not supported.
- Offline randomized-action replay: not yet supported.
- AWS receive-time transport: not established; retained as a separate live authorization gate and not treated as an irrecoverable offline blocker.
- Live authorization: false.
- `unmasked_action_effective_rate`: undefined until the candidate regenerates the complete strategy path.

The predecessor's BUY 69.8% and SELL 61.6% rates remain only mechanical timing change rates at the 5-second threshold. They do not mean that six or seven in ten final quote actions change.

## Parity Cells

| Control | Result | Boundary |
|---|---|---|
| Variance-clock integrator | pass | Isolated clock kernel only |
| Same-side reducing fill deadline | pass | Preserves the absolute active add-cooldown deadline |
| Consecutive-loss cooldown | pass | Reward-path-dependent Python/C++ state machine |
| Sync-degrade frozen tape | pass for mechanics | Full-day bound tape has zero observed trigger; trigger behavior is synthetic parity evidence |
| BUY q90 cancel/ACK/recovery/re-entry | fail | Python supported; C++ lacks the native snapshot/delta scheduler and exact-level scorer |
| Variance-time full candidate path | fail | Candidate timing does not yet regenerate orders, fills, inventory, queue, and downstream blockers |

## State And Data Identity

All 40 Development target-day BBO files and the 40 D-1 warmup files are hash-bound. Of 22,854 source fill rows, 22,738 are safely reconstructible after the second within-day side transition, a 99.4924% rate. All 40 days have a safe within-day anchor, so daily fresh-start mechanics are supported. Continuous live lineage remains unsupported because prior-day counters and cooldown deadlines were not restored.

The frozen sync tape covers the full 2026-07-20 UTC day and is bound to its source live log. It contains zero observed sync-degrade triggers. This is valid zero-event coverage, not evidence that the trigger path occurred in that live window.

## Authority

The future intervention unit remains one same-side cooldown lineage until an opposite fill or explicit reset. No single fill episode or ordinary campaign assignment is authorized. Historical exact AWS receive timestamps are not required to run a future offline randomized replay with a frozen latency distribution; AWS receive-time transport and shadow parity remain mandatory before live authorization.

Frozen Spec: [`volatility_time_add_rearm_live_stack_parity_v1_spec_20260729.json`](volatility_time_add_rearm_live_stack_parity_v1_spec_20260729.json)

Authoritative report: `${NARROWGATE_RETIRED_DATA_ROOT}/reports/volatility_time_add_rearm_live_stack_parity_v1_20260729/development/report.json`

Report SHA256: `816882235421a7871cb2c967594c97c9664557faccbaae9ad50c09e0558e072f`

Manifest SHA256: `71a8f53a90c4ed3648ceba648a1c8addceb49115dfb2bd56fe1f80683532380c`

Targeted evidence: 44 tests passed, including the isolated variance clock, absolute-deadline preservation, loss cooldown, sync event ordering, Python q90 lifecycle, C++ q90 fail-closed behavior, D-1 BBO identity, and daily-lineage contracts.
