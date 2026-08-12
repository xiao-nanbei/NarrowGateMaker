# Prospective Lifecycle Narrow Production Release v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Status: `deterministic_candidate_built_runtime_release_gates_blocked`

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Scope

This work constructs a local-only overlay successor of the frozen EC2 v9 runtime. It did not invoke SSH, modify the current working-tree live entry points, deploy code, change an EC2 configuration, restart the maker, or read economic outcomes.

The authoritative predecessor is operational baseline v9, SHA256 `bfe835bf4b76fc675cd450eccf248cd1a3d179e2f9755425b40889f042c44638`. The supplied remote copies of `live/main.py`, `live/config.py`, and `strategy/maker_engine.py` match v9 exactly. Read-only remote evidence also binds `strategy/order_manager.py`, `live/ws_handler.py`, `strategy/signal.py`, `strategy/inventory_manager.py`, `features/feature_dag.py`, and the active `live/config.yaml`.

The remote `execution/` directory contains only `__init__.py` and `active_order_depth_path.py`. Therefore `execution/order_lifecycle.py` is correctly classified as a **new file**. It has no remote predecessor to patch.

## Deterministic Builder

The builder is `scripts/build_prospective_lifecycle_narrow_release.py`. It requires the repository `.venv` Python and performs the following fail-closed sequence:

1. Verify every frozen remote predecessor SHA256.
2. Verify every reviewed local transplant and new-module SHA256.
3. Verify the read-only remote absence evidence for all new runtime files.
4. Reconstruct existing files from exact v9 text with anchor-counted narrow edits; current broad working-tree entry points are never copied whole.
5. Append only `lifecycle_journal_v2` to the exact remote YAML and prove every pre-existing parsed field is unchanged.
6. Generate a side-effect-free `models/replay/__init__.py`; the broad local initializer is intentionally excluded because it imports modules outside this release.
7. Parse every Python file, run a local overlay import smoke, reject forbidden strategy/q90/P2 markers, and atomically publish the staging directory.
8. Emit a canonical manifest with `deployment_authorized=false`.

The clean staging candidate is:

```text
${NARROWGATE_EPHEMERAL_ROOT}/narrowgate_prospective_lifecycle_narrow_release_v1_20260805_final
```

Its canonical manifest SHA256 is `daff2f96fcb947e2564d0bf3dba4f3ec404b98b7f632a135fb1e295500997ce2`. Two independent test builds are byte-identical.

## Minimal Payload

Patch exact existing v9 files:

```text
live/main.py
live/config.py
live/config.yaml
live/ws_handler.py
strategy/maker_engine.py
strategy/order_manager.py
```

Add new runtime files:

```text
execution/order_lifecycle.py
execution/order_lifecycle_quantity_contract.py
execution/order_lifecycle_journal_storage_v2.py
execution/order_lifecycle_journal_v2.py
execution/order_lifecycle_journal_writer_v2.py
execution/order_lifecycle_live_writer_v2.py
execution/order_lifecycle_remote_spool_v2.py
execution/prospective_lifecycle_state_capture_v1.py
models/replay/__init__.py
models/replay/baseline_epoch_manifest.py
models/replay/prospective_baseline_epoch.py
scripts/lifecycle_journal_v2_collector.py
research/families/f10_live_replay_attribution/docs/operational_baseline_identity_20260804_v9.json
```

The large 13-domain initial-state capture is isolated in a generated new file. As a result, the `strategy/maker_engine.py` patch is 115 added and 5 removed lines relative to v9 instead of copying the working tree's 1,652/150-line mixed-concern delta.

## Preserved Identities

The payload contains no `cpp/**`, no `features/**`, no model bundle, and no P3 artifact. The builder binds but does not replace:

```text
features/feature_dag.py
strategy/signal.py
strategy/inventory_manager.py
cpp/narrowgate_cpp/streaming_features.cpp
causal-v12 model bundle
operational P3 v2 artifact
```

The candidate YAML is the exact v9 YAML plus one top-level collection section. All strategy, risk, ML, P3, q90, BUY selector, quote, cooldown, and execution parameters are semantically unchanged. q90 remains shadow ON/action OFF; BUY fill-selection remains OFF.

## Current Decision

The deterministic source candidate is valid, but it is **not yet safe to deploy directly from remote v9**. No source-predecessor evidence is missing now. The remaining remote evidence is runtime-specific:

- confirm Python `3.12.13` and PyArrow `24.0.0` in the actual deployed venv;
- bind the loaded native extension path and SHA256, then prove it is unchanged;
- apply the overlay only in an isolated release directory and run remote import smoke plus targeted lifecycle tests;
- prove complete 13-domain initial state and zero pre-epoch native events;
- run the bounded one-hour performance window with zero drops/errors;
- satisfy enqueue, quote-loop p99, queue HWM, CPU, RSS, write p99, and maker-thread filesystem-call gates;
- validate bounded-spool transfer and atomic local admission;
- rehearse rollback to exact v9 code/config with a controlled restart.

The first remote window remains mechanics/transport evidence only. It grants no q90 action, strategy action, PnL, or live-policy authority.

The final local preflight accepts the staging manifest and reports exactly one blocker class: `production_release_evidence_incomplete`. There are no remaining remote source/predecessor, payload-classification, config-semantics, or local import blockers.

## Verification

Local verification includes deterministic double-build, predecessor tamper, candidate tamper, manifest tamper, exact config semantics, forbidden payload, new-file classification, staged lifecycle callback routing, Python compilation, local overlay imports, preflight integration, and Ruff.

The final lifecycle-focused test selection completed with `88 passed`; Ruff completed with no findings. All commands used the repository `.venv/bin/python`.

The release must continue to use this narrow overlay. Repository-wide `make deploy` remains prohibited because it would replace unrelated code and the v9-bound Feature DAG.
