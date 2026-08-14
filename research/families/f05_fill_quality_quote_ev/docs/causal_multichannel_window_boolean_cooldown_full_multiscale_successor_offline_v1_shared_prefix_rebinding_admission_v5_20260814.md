# F05 Offline Shared-Prefix Rebinding V5 Admission

Last materially modified: 2026-08-14

Status: `sequential_mechanics_shared_prefix_rebinding_v5_admitted_pre_economic`.

Evidence availability: this report and its repository-relative JSON receipt are public. The builder manifests, portable replay binding, admitted mechanics manifest, panel tables, source receipts, owner artifacts, replay caches, and market data are retained in the owner-private evidence store and are not distributed with the public repository; their SHA256 values are public integrity metadata only.

## Result

The corrected resumable shared-prefix implementation rebuilt all 30 frozen family-specific historical Development dates without replacement or failure. The builder and independent validator both accepted the result, and mechanics admission atomically bound the same 3,516 opportunities to canonical mechanics SHA `c90930f9bdb51996af33efaeb9ef4d5386716f3ba911fcf2e1e935a65a0c8cab`.

Relative to fill-trace v4, the v5 metadata, Boolean-feature, continuous-feature, and exact-owner-action tables are byte-identical. All five tables retain the same ordered 3,516 opportunity IDs and row-key SHA. The replay-input schema and all strategy-relevant values are unchanged; only the portable binding path changed because the immutable physical materialization moved to the v5 cache identity. The portable binding file SHA itself remains unchanged.

The implementation executes one exact-owner parent replay per day, forks from the identical in-memory state at each frozen target fill, and runs bounded duration continuations from that state. Per-opportunity shards are atomically admitted and resumable. Stale staging is quarantined, non-target fills are skipped, and exact owner action and policy identity are checked before every fork. Resume accounting now separately validates resumed targets, newly dispatched targets, newly completed arms, and the full admitted shard manifest.

## Evidence Boundary

This materialization generated no labels or candidate actions and read no counterfactual economic result, Validation result, or sealed-holdout result. It grants neither action nor live authority and does not modify the active owner policy, live runtime, live configuration, or EC2 deployment. Fill-trace v4, formal-v13, and the interrupted pre-fix mechanics attempt remain immutable historical diagnostics.

## Verification

The panel builder completed 30 of 30 dates and the independent builder validator exited successfully. Mechanics admission and its independent validator both exited successfully. Each table contains 3,516 rows with row-key SHA `e481d8a61eecb71a36e6e3c8f2be1630c483a466a5418adc583d6398c29330ec`. A structured full-panel comparison found identical opportunity order and exactly one changed replay-input column, `portable_replay_binding_path`; all other replay-input columns were equal.

The directed shared-prefix and recovery tests cover interrupted staging quarantine, target allowlists, owner-action drift, owner-policy drift, missing frozen targets, progress counters, exact-owner shared-prefix execution, and mixed resumed/new admission accounting. No live or strategy code was changed by this admission.

## Next Gate

Commit and annotate this admission from a clean worktree, bind formal-v14 to that exact tag and mechanics canonical SHA, rerun formal preflight, and rerun the fixed first-day one-worker exact-owner gate. Only then may the canonical orchestrator start a new nested chronological OOF run using the resumable v14 cache identity.
