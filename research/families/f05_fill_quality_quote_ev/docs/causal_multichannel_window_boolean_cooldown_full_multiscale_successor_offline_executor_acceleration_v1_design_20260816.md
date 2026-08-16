# F05 Offline Executor Acceleration V1 Design

Last materially modified: 2026-08-16

Status: `python_accelerators_implemented_cpp_synthetic_smoke_only_no_formal_economic_authority`.

## Question

Can the full-multiscale offline nested out-of-fold execution be made materially faster without changing the frozen data, candidate ladder, feature semantics, policy decisions, event order, repeated-policy estimand, statistics, or evidence permissions?

## Existing Formal-V17 Boundary

Formal-v17 remains an immutable Python-authoritative execution under its own clean commit, annotated tag, execution manifest, and cache identity. Its running process must not be stopped, patched, rebound, or supplied with artifacts produced by this acceleration identity. This design does not create formal-v18, read an intermediate economic result, admit a candidate, or change action or live authority.

## Acceleration Identity

The execution-only successor identity is `f05_full_multiscale_offline_replay_executor_acceleration_v1`. It may change scheduling, immutable input materialization, exact-control reuse, and the implementation language of a parity-qualified event loop. It may not change any replay input byte, clock, order-event precedence, queue rule, random draw, policy output, fallback, campaign assignment, accounting result, or statistical denominator.

## Global Work Scheduling

The scheduler operates on immutable `policy x side x fold x UTC day x stage` jobs after the corresponding policy has been frozen by the chronological learner. It uses one global worker-token budget of ten and must not create nested process pools. Jobs may execute out of order, but admission and returned frames remain deterministically ordered by stage, side, fold, policy identity, and UTC day.

Learning dependencies remain strict. An outer policy cannot be scheduled before all of its inner evidence is complete and the policy artifact is frozen. An outer fold cannot use an outer-test result to fit or rank any policy. Scheduling concurrency therefore applies only within a dependency-ready wave and never changes the chronological research graph.

Every job keeps its current content-addressed cache key, lock, atomic staging directory, receipt, and corruption checks. A process interruption may resume only complete admitted jobs. Running, partial, failed, quarantined, or hash-drifted jobs are never treated as complete.

## Read-Only Day Input Cache

The daily trades, BBO, L2, variance state, and frozen model overlay may be materialized once into a read-only memory-mapped bundle. Its identity binds source receipts, ordered columns, dtype, shape, byte order, missing-value representation, exchange and feature-ready clocks, model and feature hashes, replay-engine input ABI, and the content SHA256 of every member.

The bundle contains only immutable replay inputs. It cannot contain candidate decisions, orders, fills, inventory, campaigns, terminal values, or another strategy-dependent path. Admission is atomic and single-writer. Every open revalidates the manifest and member hashes before exposing read-only arrays. A schema, source, clock, model, dtype, shape, or byte mismatch fails closed and rebuilds under a new identity rather than mutating an admitted bundle.

LRU access metadata is stored separately from content identity. Updating access time cannot alter the bundle manifest or any research receipt.

## Exact B0 Control Cache

The exact active-owner B0 daily path may be reused across candidate evaluations only when the candidate-independent control computation is provably identical. The control key must bind UTC day, target side, ordered target opportunity identity, source and day-input bundle hashes, initial state, exact owner policy and predicate bundle, Python or parity-qualified C++ engine identity, queue and latency parameters, random seed and random-source contract, emitter and accounting ABI, target-day and D+1 semantics, and every execution parameter read by the control arm.

Candidate policy identity is intentionally excluded from this control key; candidate output is never stored in the B0 cache. Fold identity may be excluded only when the complete ordered target opportunity set and all fold-derived inputs are byte-identical. Side may never be excluded. A new candidate receives its own candidate replay and a new paired projection receipt that binds the immutable control receipt and candidate receipt.

Control reuse is forbidden when common randomness is generated jointly from arm execution order, when the candidate can modify a shared object, when side or target opportunity support differs, or when any bound input differs. Tests must prove that corrupt, mismatched, cross-side, cross-target, or candidate-contaminated entries fail closed.

## C++ Repeated-Policy Engine

Python remains authoritative until C++ implements the complete repeated multichannel Boolean cooldown ABI. Required semantics include three-valued predicates, warmup and stale fallback, side and role eligibility, campaign age, consecutive fill units, current owner action, ordered Boolean rules, duration deadlines, repeated and partial-fill lineage, D+1 target suppression, coverage reason codes, deterministic checkpoint state, and campaign accounting.

C++ may become selectable only through an explicit parity-qualified engine identity. The qualification suite must show zero divergence from Python for policy decisions, fallback reasons, quote decisions, submit and cancel events, partial and full fills, inventory transitions, cooldown ownership, campaigns, terminal accounting, and deterministic checkpoints on synthetic edge fixtures and a frozen mechanics-only day. Until that suite passes, the existing fail-closed rejection of a repeated cooldown evaluator in the C++ path remains in force.

The implementation delivered under this identity is intentionally narrower than that final engine. It exposes a streaming Boolean cooldown runtime and a synthetic tick-replay hook, but its only accepted event-loop scope is `synthetic_full_replay_smoke`; caller-authored `full_replay` qualification is rejected. The smoke path now binds explicit BBO input and checks decisions, fill timestamps, fill prices, quantities, inventory transitions, cash, single-campaign terminal MTM, lineage, and checkpoint parity. It does not authorize formal execution because generic BUY and SELL delegates, complete dynamic M0/M1/M2 predicate materialization, authoritative warmup and stale semantics, per-fill cancel ordering, atomic assignment-snapshot receipts, campaign accounting receipts, and restart state-chain parity are not yet complete.

The C++ runtime is therefore a compiled mechanics prototype, not a selectable formal engine. Python remains the sole authoritative engine for the current nested OOF. Future C++ work must close every listed surface and bind a recognized immutable qualification receipt; booleans and syntactically valid hashes are never sufficient authority.

## Verification

The first verification tier is outcome-blind and synthetic. It checks deterministic scheduling under one and ten workers, cold and warm cache equivalence, interruption and resume, concurrent single-writer admission, input-bundle corruption, B0 cache isolation, and Python versus C++ mechanics parity without reading Development economic outputs.

The second tier may compare complete replay artifacts only under a newly frozen clean commit, annotated tag, execution manifest, cache identity, and explicit replacement admission. It must reproduce the same ordered rows, decisions, fills, campaigns, accounting values, scorecards, and permissions as the Python reference. A wall-time improvement without exact semantic equality is a failed accelerator, not new research evidence.

## Authority

This work is offline-only. It creates no shadow, companion, observer, writer, journal, feature dump, candidate telemetry, live hook, deployment, restart, or remote-host change. Validation and sealed holdout remain unread. The active owner policy remains unchanged, and this execution optimization has no research, action, or live authority.
