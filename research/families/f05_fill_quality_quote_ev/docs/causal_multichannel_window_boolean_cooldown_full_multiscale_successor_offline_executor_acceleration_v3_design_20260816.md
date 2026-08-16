# F05 Offline Executor Acceleration V3 Design

Last materially modified: 2026-08-16

Status: `verified_local_clean_commit_pending`.

## Question

Can the frozen F05 full-multiscale offline nested out-of-fold run keep the governed read-only day-input cache introduced by acceleration v2 while removing the single-day-parent bottleneck, without changing the 30-day denominator, candidate ladder, estimand, exact owner B0, replay semantics, folds, statistics, or evidence permissions?

## Why V3 Exists

Formal-v19 passed its zero-economic contract walk and exact-owner one-day mechanics gate, then entered the formal one-shot stage under acceleration v2. Its early launch performance gate observed 15 completed opportunities and 120 completed arms over 5.7983 minutes, or 2.58696 opportunities per minute. This was slower than the approximately 4.85 opportunities per minute observed during formal-v18, so v19 was stopped before a label batch returned and before any candidate, outer-test result, scorecard, or formal result existed. No intermediate economic outcome was inspected.

The v19 profile showed that read-only mmap removed repeated large-frame deserialization, but one active day parent still serialized opportunity discovery and could expose only the arms available around that day's event path. Because each opportunity contains eight frozen duration arms, the ninth arm token was only intermittently useful, while long event-path intervals left no independent day parent available to feed the shared arm pool.

## Frozen Research Boundary

Acceleration v3 changes scheduling only. It preserves the exact v19 source and panel manifests, 30 chronological Development days, opportunity identities, duration arms, candidate hierarchy, owner B0, Python modeled-queue event semantics, ambiguity censoring, random-source contract, sequential repeated-policy estimand, scorecard, and action/live permissions. Validation and sealed holdout remain unread. It makes no EC2, live-policy, quote, inventory, shadow, companion, or telemetry change.

## Balanced Global Topology

The one-shot stage now admits two immutable day parents into one non-nestable process pool. Both parents open their day arrays from the same governed read-only mmap namespace and submit arm work against one shared filesystem-backed token pool. The pool permits at most eight arm replay processes and at most two lightweight coordination supervisors globally across both days.

The CPU-token formula is `two active day parents + eight arm replay slots = ten`. Coordination supervisors only retain fork-ready snapshots, wait for child completion, and atomically admit receipts; they are bounded separately and are not counted as compute tokens. Shared advisory locks make the eight-arm limit global, rather than eight arms per parent. A process topology, mmap identity, or observed concurrency mismatch fails admission.

Two day parents allow one date to continue event discovery while the other date is waiting on long-tail arms or sparse opportunity intervals. Eight arm slots align with the frozen eight-arm duration vocabulary, avoiding the stranded ninth token observed under v2. Day jobs and returned results remain deterministically ordered by their frozen keys.

## Cache Boundary

The immutable day-input bytes retain the acceleration-v2 cache identity `f05_full_multiscale_offline_replay_executor_acceleration_v2`. Scheduler identity is intentionally separate and advances to `f05_full_multiscale_offline_replay_executor_acceleration_v3`. V3 may therefore reuse only complete, hash-valid, candidate-independent v2 day-input mmap bundles. It may not reuse v19 one-shot opportunity shards, semantic labels, sequential paths, B0 paths, candidates, or scorecards because the adapter and execution-manifest identities change.

Cold day-input materialization remains capped at two workers. The formal day workers receive compact opportunity and identity metadata through Python serialization; trades, BBO, L2, derived state, and frozen model overlays remain read-only memory maps. Candidate actions and economic outputs are forbidden from the mmap namespace.

## Verification and Early Performance Gate

Before formal-v20 starts, a clean commit and annotated tag must bind this contract and the unchanged cache identity. Scheduler, shared-prefix, mmap, LRU, backend, fail-closed, and full repository tests must pass. A fresh all-fold zero-economic walk must cover both sides, four outer folds, twelve inner folds, every fold-day slot, all candidates, matched controls, and the continuous comparator. The fixed one-day exact-owner mechanics gate must also pass under the new eight-arm topology.

Formal-v20 must be observed for at least 30 completed opportunities and at least five minutes before the early performance decision. Its completed-opportunity throughput must be no lower than the formal-v18 reference of 4.85 opportunities per minute and must exceed formal-v19 by at least 20 percent. Failure stops the process immediately, preserves receipts, and creates no automatic successor. Passing this engineering gate permits the frozen run to continue but creates no economic conclusion.

## Authority

This identity is offline execution infrastructure only. It creates no research-supported promotion, action authority, live authority, deployment, remote change, shadow, companion, or candidate-specific collection path.
