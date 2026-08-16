# F05 Offline Executor Acceleration V2 Design

Last materially modified: 2026-08-16

Status: `implemented_local_zero_economic_preflight_pending`.

## Question

Can the frozen F05 full-multiscale offline nested out-of-fold run execute its one-shot training-label stage under one real ten-CPU budget, with each market day loaded once through a read-only memory map, while preserving the exact 30-day panel, candidate ladder, estimand, B0 owner control, event semantics, chronological folds, and evidence permissions?

## Why V2 Exists

Acceleration v1 correctly added a global `policy x day` scheduler, candidate-independent B0 control reuse, and a content-addressed day-input mmap contract for sequential replay. Its one-shot path nevertheless retained the predecessor day-level scheduler: several Python day parents each materialized complete pandas market frames and independently forked shared-prefix supervisors and duration arms. The nominal ten-worker limit therefore did not cover the most expensive stage, repeated deserialization remained active, and copy-on-write pressure from multiple mutable simulator parents competed for memory bandwidth and CPU caches.

Formal-v18 was stopped by explicit owner instruction before completion so this execution defect could be corrected. Its ten atomic progress receipts remained in `running` state and recorded 509 completed opportunities and 4,072 completed duration arms. No one-shot label batch returned to the learner, no candidate was frozen, no outer-test policy result or scorecard was produced, and no formal economic conclusion was admitted. Those strategy-dependent v18 shards are not eligible for v19 reuse.

## Frozen Research Boundary

Acceleration v2 is computation-only. It keeps the same 30 chronological Development days, source and panel manifests, opportunity identities, duration vocabulary, candidate hierarchy, exact current owner B0, Python replay semantics, modeled-queue ambiguity censoring, random-source contract, repeated sequential-policy estimand, scorecard, and action/live permissions. Validation and sealed holdout remain unread. It makes no EC2, live-policy, quote, inventory, shadow, companion, or telemetry change.

## One-Shot Global Scheduler

The formal one-shot stage now submits immutable day jobs to one non-nestable global day pool with exactly one active day parent. That parent opens one content-addressed read-only mmap bundle and advances one baseline day path. Two lightweight POSIX supervisors may retain at most two opportunity snapshots so the arm queue remains fed, while one shared token pool admits at most nine arm replay processes across those snapshots.

The CPU-token formula is `one active day parent + nine arm replay slots = ten`. Supervisors coordinate fork, admission, and receipt collection and are explicitly excluded from the CPU-token count; their peak count remains bounded at two and is emitted in every day audit. No nested `ProcessPoolExecutor` is allowed. The outer orchestrator blocks while this pool executes, and one-shot results are returned in deterministic day order regardless of completion order inside the shared-prefix arm pool.

Each opportunity has eight frozen duration arms. Allowing two in-flight snapshots prevents the nine-slot arm pool from starving when one opportunity is finishing, while remaining far below v18's multiple day parents and per-day supervisor pools. Audit admission fails if the configured or observed arm/supervisor concurrency exceeds the frozen topology.

## Read-Only Day Inputs

Before a one-shot day can enter the worker pool, its individual trades, BBO, L2, variance state, trade-intensity state, squared-return state, and frozen model overlay must be atomically materialized into the governed replay-DAG cache. Cold materialization is capped at two concurrent workers to avoid recreating the v18 memory spike. The formal day process receives only compact opportunity and identity metadata through Python serialization; the multi-million-row market arrays are opened from the admitted bundle as read-only memory maps.

The mmap identity binds the source receipts, canonical day projection, exchange and feature-ready clocks, replay engine, queue identity, array names, order, dtype, shape, byte order, missing representation, and member SHA256 values. It contains no candidate action, order path, fill, inventory, campaign, or value output. A missing mmap binding, writable open, source drift, corrupt member, non-governed cache root, or cache identity mismatch fails before one-shot economics starts.

## Cache and Resume Boundary

V2 uses a new executor identity and a new mmap namespace. It may reuse immutable canonical source bytes and the frozen panel, but it cannot reuse v18 one-shot opportunity shards, semantic labels, sequential policy paths, or B0 cache entries because their adapter and execution-manifest hashes differ. Only complete, hash-valid v2 jobs may resume after interruption; partial or running state never counts as evidence.

## Verification and Launch Gate

Before formal-v19 may start, the new clean commit and annotated tag must bind the v2 executor contract, all code artifacts, the unchanged source/panel manifests, and a fresh formal execution manifest. The complete all-fold zero-economic walk must cover both sides, four outer folds, twelve inner folds, every fold-day slot, all candidates, matched controls, and the continuous comparator while reporting the exact one-shot topology and mmap materialization cap. Targeted scheduler, shared-prefix, mmap, LRU, backend, and fail-closed tests must pass, followed by the fixed one-day exact-owner mechanics gate.

Only after those checks may the detached formal run start. Intermediate economic outcomes remain unread. A performance improvement without identical frozen semantics is a failed accelerator, not new research evidence.

## Authority

This identity is offline execution infrastructure only. It creates no research-supported promotion, action authority, live authority, deployment, remote change, shadow, companion, or candidate-specific collection path.
