# Execution Worker And Progress Contract v1

Last materially modified: 2026-08-17

## Decision

Every new long-running NarrowGate experiment must use the host-wide worker governor in `models/audit/execution_governance.py`; a runner-local `--workers` value is not a host resource limit. The default governed capacity is ten worker tokens, one top-level execution leases its declared tokens before creating a pool, and a process that inherits an active lease cannot create another governed pool.

The governor uses one exclusive lock file per host token and writes a hash-bound lease receipt for acquisition and release. Failure to obtain the full declared budget is fail-closed; a runner may wait or be retried, but it must not silently oversubscribe the host. Threads or subprocesses inside the leased execution consume the parent budget and do not acquire a second lease.

## Topology

Every execution contract records the host capacity, requested tokens, outer-pool width, nested-pool width, and whether the work uses processes or threads. Parallel outer and nested pools are forbidden. A sequential inner loop is legal, and a single outer worker may use a separately declared inner width only when the resulting maximum concurrency does not exceed the leased budget.

## Progress

Every new long task must expose atomic job receipts with distinct `queued`, `dispatched`, `running`, `complete`, and `failed` states. `running` begins only inside the actual worker and records its PID; putting work into a logical queue or submitting it to an executor does not make it running.

The common `narrowgate_execution_progress.v1` summary records the stage DAG, expected and observed job counts, actual worker slots and PIDs, cache hits, misses and bytes, timestamps, throughput, and ETA. ETA is withheld until the current stage has at least two terminal jobs and positive elapsed time; it is never inferred merely from process age.

The summary can be rebuilt from immutable per-job receipts with `models.audit.execution_governance`. Cache metrics remain separated by cache identity, including day-input mmap, exact-B0 control, one-shot result, and sequential result caches, so a nominal acceleration cannot hide that the expensive stage is missing its cache.

## Admission

The worker lease and progress summary are execution evidence, not economic evidence. They grant no action or live authority. Formal result manifests bind the released worker-lease receipt, while data, model, policy, scorecard, and economic permissions continue to be governed by their own immutable manifests.

## Migration

Historical executions retain their recorded topology and progress limitations. This contract applies to new execution identities and future reruns. The F05 formal orchestrator is the first bound adopter; other long runners migrate when they create a new execution identity rather than rewriting frozen historical results.
