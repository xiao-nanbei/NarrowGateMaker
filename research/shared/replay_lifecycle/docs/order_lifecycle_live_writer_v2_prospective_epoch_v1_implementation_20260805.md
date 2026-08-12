# Journal-v2 Live Successor And Prospective Baseline Epoch

Last materially modified: 2026-08-12

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Status: `deployed_first_prospective_epoch_admitted_transport_repair_pending`

## Boundary

This successor was deployed for a bounded, outcome-blind prospective collection. The first fully bound epoch was admitted atomically to `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` on 2026-08-08. It did not alter policy or read economic outcomes, and the 17 historical partially-bound epochs remain diagnostic-only.

The startup order is fixed as `engine.start/prefill + startup cancel`, epoch publication and writer attachment, WebSocket start, then the first maker-loop tick. No WebSocket event or quote decision can precede the epoch. Startup fails closed when collection is explicitly enabled and any required identity is missing, the frozen operational-baseline file hash differs, `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` is not mounted, or an exchange/local order remains active.

## Fully Bound Identity

The prospective artifact binds these ten independent hashes:

1. the complete Python runtime surface under `live`, `strategy`, `execution`, and `features`, plus the replay epoch builders;
2. loaded config file;
3. complete model-bundle contents;
4. P3 artifact;
5. Feature DAG;
6. execution ABI and native module;
7. action enablement;
8. initial inventory, campaign, cooldown, order, q90-hold, and account state;
9. live data-source configuration;
10. exchange, visibility, decision, latency, and feature-ready clock semantics.

`fully_bound` additionally requires 13 nonempty, versioned initial-state domains. They cover account/exchange state, complete inventory accounting, campaign state, reward-path loss cooldown, adverse-markout pause, sync-degrade, defense/stale guards, fill-cooldown lineage, order lifecycle, q90 runtime, post-fill response, quote-policy clocks, and Signal/Feature-DAG warmup state. The validator checks required fields within each domain; a `captured=true` placeholder or a captured-domain name alone is rejected.

Signal state binds the causal cutoff, last emitted bucket, full canonical Python bar/feature histories and fingerprints, market generations, rolling state, and backend evidence. A Python-only runtime is explicitly bound by `python_authoritative_bar_and_feature_history.v1`; it may have nonempty warmup history without fabricating C++ state. When a C++ feature engine is present, it is accepted only under the deterministic `canonical_python_bar_and_feature_history.v1` reconstruction contract and exact bar/history count parity. Native global-flow and cross aggregators must have zero events at the pre-WebSocket boundary. Opaque or nonempty native state is listed in `unsupported_initial_state_fields`, which prevents publication rather than downgrading it silently.

The state, evidence, and epoch manifest are written to a same-filesystem partial directory, fsynced, and atomically renamed. Its initial `formal_collection_valid` is false until journal parts have been admitted.

## Non-Blocking Boundary

The producer path performs no filesystem operation. An order callback only:

1. copies the small `QuantityWeightedOrderLifecycle` state;
2. constructs a stable callback identity;
3. calls bounded `queue.put_nowait`.

The dedicated worker owns validation, Parquet/JSONL persistence, fsync, health, part admission, and durable cursor advancement. While v2 is attached, the old synchronous lifecycle CSV path is bypassed. Queue-full, callback-validation, or write errors set the tape's `formal_collection_valid=false`; they do not raise through the order callback and do not stop quoting.

Legacy local shutdown mutation is translated only on the worker-bound private snapshot into `local_shutdown_censor`; it cannot assert exchange terminality. REST-reconciled cancel rejection is explicitly enqueued. Missing exchange clock evidence may invalidate the tape, but never blocks the maker.

Health reports enqueue/write p50, p99 and max latency, worker/process CPU, process maximum RSS, queue depth/HWM, drops, errors, and last flush. No maker main lock is acquired by the worker, and the producer never waits for the worker.

## Configuration

`lifecycle_journal_v2.enabled` defaults to false and is restart-only. An enabled configuration requires `${NARROWGATE_STORAGE_ROOT}` to be mounted, both output roots to be inside that mount, and an exact frozen baseline identity path plus SHA256. SIGHUP cannot enable, disable, or mutate this collection identity.

## Deployment And Admission

The first admitted session contains 2,664 exact Parquet rows and 589 durable lifecycle cursors. It passed the epoch, runtime, part, manifest, hash, cursor, health, queue, and atomic-admission gates with zero drops or errors. See [`prospective_lifecycle_remote_session_admission_v1_20260808.md`](prospective_lifecycle_remote_session_admission_v1_20260808.md).

The subsequent F07 transport audit found a producer-level duplicate activation: REST ACK activated an order and WebSocket `NEW` activated it again. The local successor makes the second callback lifecycle-idempotent. A new fully bound prospective epoch is required before transport can be reconsidered.

## Remaining Transport Gates

- Deploy the lifecycle-idempotent WebSocket `NEW` producer fix under a new code hash and fully bound prospective epoch.
- Emit the exact feature visibility companion used by q90 decisions.
- Re-admit the new bounded session and pass the original 0.05 valid-fraction and 0.15 composition-TV transport limits without relaxation.

The journal deployment and admission grant mechanics collection only. No q90 action, economic replay, or live-policy permission is granted.
