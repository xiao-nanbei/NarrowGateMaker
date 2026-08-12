# F07 Lifecycle-CIF Training Preflight v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Status: implemented; current live journal fails closed; no training, q90, action, economic, or live authority.

## Purpose

This preflight answers one bounded question: do the baseline identity and the future lifecycle-journal control-plane artifacts support registration of an F07 100 ms competing-risk training identity?

It reads only:

1. a validated `narrowgate_baseline_epoch_manifest.v1` object; and
2. exact `order_lifecycle_journal_v2_dataset_metadata.v1` schema/metadata.

It does not open lifecycle rows and does not read PnL, reward, markout, or campaign terminal value. Metadata is exact-schema and capped at 512 epochs and 400 chronological days, so hidden labels and unbounded payloads fail closed.

The implementation is `research/families/f07_active_order_continuation/audit/active_order_cif_training_preflight.py` at SHA256 `5987305a36b33323bc929b34b4f703728aaa1ad00ffad5b5f148b55448f160f5`.

## Readiness Layers

The report deliberately separates four gates:

- `baseline_epoch_ready`: every used epoch is fully identity-bound at its first decision, the restart audit is complete, and no unbound interval is present.
- `lifecycle_data_admission_ready`: the v2 producer, lossless callback batch, atomic admitted tape, epoch binding, grid, clocks, spells and censoring all pass.
- `training_identity_registration_ready`: data admission passes and the chronological 40-day panel, Python panel builder and frozen Python CIF kernel are bound.
- `chronological_python_cpp_lockstep_execution_ready`: the C++ panel/kernel and event/checkpoint lockstep runners are additionally bound.

Even a four-gate pass only permits freezing a successor identity. The preflight itself grants no panel-generation, model-training, q90 action, economic-read, action-experiment, live-deployment, or baseline-update authority.

## Mechanics Contract

The risk grid is exactly 100 ms on the causal visibility clock. Duplicate or missed edges fail closed. Causes retain the frozen order:

1. `favorable_fill`
2. `adverse_fill`
3. `cancel_ack`
4. `other_terminal`

The preflight consumes only the identity and SHA256 of a separately frozen full-fill classifier. It does not consume that classifier's markout or other economic labels.

A partial fill ends the current remaining-quantity spell and starts a new positive-remaining spell at the same last evaluated edge. Cancel request and cancel reject remain state transitions, never terminal causes. Local shutdown is explicit right censoring and cannot be encoded as exchange terminal. Orphan adoption uses delayed entry at the first authoritative observation, with an explicit reason and entry timestamp.

Visibility and exchange clocks remain separate. Missing exchange timestamps may be explicitly invalid with a reason; timestamps after visibility or clock regressions are zero-tolerance failures. Curves remain epoch-specific until a separate drift audit permits pooling.

## Current Result

The current 360-hour baseline manifest and current live v1 journal were run through the preflight. All four readiness gates are false. The deterministic report hash is `f56cfc22d05b9ec383e6d8187f40c9eea9ae753ce8b01b598542b62576eabd04`.

The baseline side is not ready because its restart audit is incomplete, it contains an unbound prefix, and all 17 epochs are partially bound with zero lifecycle-authorized epochs.

The journal side is not authoritative because the current live producer writes only the latest v1 lifecycle snapshot after a callback. It cannot prove that a callback producing activation and fill persisted both events. It also lacks:

- live and replay v2 batch-emitter integration;
- all-unseen-event batch emission and atomic batch commit;
- a durable lifecycle cursor and writer health/drop accounting;
- an atomically admitted v2 tape, admission manifest and health manifest;
- an exact 40-day chronological panel and frozen full-fill classifier;
- Python/C++ panel builders and a C++ competing-risk kernel;
- event-lockstep and checkpoint-resume lockstep runners.

Unknown counts do not pass as zero. The current legacy shutdown encoding, partial-fill spell counts, left-truncation handling, dual-clock coverage and post-terminal risk counts must all be re-derived from the admitted v2 tape.

## Required Order

1. Wire `OrderLifecycleJournalV2BatchEmitter` into both live and authoritative replay, preserving every unseen event from each callback.
2. Publish the v2 tape, admission manifest and writer health manifest atomically, with zero drops/errors and row-count agreement.
3. Finish the baseline restart/identity evidence and bind each order to one lifecycle-authorized epoch.
4. Freeze the 40-day chronological panel and upstream full-fill classifier identity without reading economics in this preflight.
5. Bind the Python/C++ panel builders, C++ CIF kernel, and event/checkpoint lockstep runners.
6. Rerun this preflight. Only then may a separate F07 training identity be registered.

The machine-readable contract and complete permission surface are frozen in `active_order_cif_training_preflight_v1_design_20260804.json`.
