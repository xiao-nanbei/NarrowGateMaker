# F05 Offline Formal-V15 Semantic Fold-Reuse Execution Amendment

Last materially modified: 2026-08-14

Status: `pre_economic_execution_optimization_frozen_pending_clean_tag`.

Evidence availability: this amendment and its repository-relative JSON contract are public. The admitted 30-day mechanics panel, source receipts, formal manifests, replay caches, progress records, owner artifacts, and market data remain in the owner-private evidence store and are not distributed with the public repository.

## Question

Formal-v14 showed that the corrected shared-prefix executor was progressing without failures, but the expanding outer folds still recomputed the same complete side/day outer-train input frames. This amendment asks whether those byte-identical historical inputs can be rebound to a later fold without changing any research or economic semantics.

## Frozen Reuse Rule

Every fold keeps its own day-cache entry, progress record, receipt, request hash, and final label receipt. Cross-fold reuse is permitted only for `outer_train_one_shot` and only when the complete side/day replay-input frame has the same ordered index, columns, dtypes, values, source identities, owner-policy identity, candidate-duration vocabulary, formal execution identity, and adapter identity after removing exactly the provider-owned `outer_fold_id` and `fold_row_role` columns.

No other column may be ignored. A changed opportunity set, row order, value, dtype, side, day, source manifest, panel manifest, fold manifest, execution manifest, owner policy, candidate vocabulary, or adapter artifact produces a different semantic key and requires a new replay. Outer-test rows and repeated-policy inner/outer OOF evaluation rows are never admitted through this semantic cache.

The first admitted fold entry remains the immutable semantic source. A later fold receives a new fold-specific cache entry whose manifest binds the semantic receipt and the original source-cache receipt. Source Parquet bytes, frame hashes, cache manifests, and receipt hashes are revalidated on every load. Any missing source, malformed key, hash drift, frame disagreement, or incomplete admission fails closed.

## Outcome-Blind Denominator Audit

The frozen 30-day panel contains 8,215 purged outer-train opportunities across the four expanding folds. Recomputing the semantic key from the admitted replay inputs found 2,854 first-or-changed opportunities and 5,361 opportunities eligible for exact cross-fold reuse, a 65.2587% reuse fraction. The fold totals are 1,343, 1,758, 2,260, and 2,854 opportunities; reusable counts from prior folds are respectively 0, 1,343, 1,758, and 2,260. Across 134 side/day cells, 86 are exact semantic repeats.

This audit read no candidate result, terminal value, Validation, or sealed holdout. It only compared the admitted replay-input bytes and frozen fold membership.

## Concurrency

The day-worker contract remains six workers with the existing governed range of one through eight. The shared-prefix global arm pool increases from four to eight, which is the pre-existing hard maximum implemented by the executor. Opportunity-level atomic shards, per-opportunity eight-arm completeness, global capacity leases, stale-staging quarantine, and deterministic frame admission remain unchanged.

## Immutable Research Contract

Formal-v15 must reuse the same 30 admitted days, 3,516 mechanics opportunities, four-by-three chronological fold manifest, exact active owner B0, eight-duration vocabulary, candidate ladder, inner-train-only feature search, identified-only targets, sequential repeated-policy outer evaluation, hierarchy, simultaneous inference, scorecard, and permissions as formal-v14. The optimization may change wall time and cache provenance only; it may not change a label, candidate, action, statistical denominator, or decision rule.

Formal-v14 may be stopped only after this implementation passes its targeted cache, adapter, backend, orchestrator, and shared-prefix tests, receives a clean commit and annotated tag, binds a new immutable execution manifest, passes formal preflight, and reproduces the fixed one-day exact-owner mechanics gate. No formal-v14 progress record, partial shard, label, candidate, outer-test result, scorecard, or economic conclusion may be promoted into formal-v15.

## Authority

This amendment is offline-only. It creates no F05 companion, observer, writer, journal, feature dump, candidate telemetry, live hook, deployment, restart, or EC2 change. The active owner policy and its owner-risk-accepted authority remain unchanged. Validation and sealed holdout remain unread; action and new live authority remain false.
