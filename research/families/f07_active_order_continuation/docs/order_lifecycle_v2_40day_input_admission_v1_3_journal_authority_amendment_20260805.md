# F07 Journal-v2 Authority Writer Successor Amendment v1.3

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

## Status

`execution_successor_frozen_pending_40day_journal_v2_admission`

This successor binds the current journal writer without modifying the frozen v1.2 JSON or Markdown. The v1.2 identity remains historical evidence and still fails against the current source with `implementation_hash_mismatch`, exactly because its frozen writer SHA256 is no longer current.

The machine-readable successor is `order_lifecycle_v2_40day_input_admission_v1_3_journal_authority_amendment_20260805.json`. Its canonical SHA256 is `ec9005ae15d5d9271b5e341d49d6b9842f2fcc6d6b90a4207a72df323a94529c`.

## Frozen Lineage

The predecessor bytes remain fixed:

- v1.2 JSON file SHA256: `00d1d80c836b3555fcf5a00378b00ec52197c17dbcc54c6881e648270393761e`
- v1.2 canonical amendment SHA256: `bd2216e84281a99049cbd8c04ede798101179ae37b3a4272a5d584ec2d94f254`
- v1.2 Markdown file SHA256: `8427ac574513c270e398df598bbede6317a3f1401e5c5bc9aebb2ba573ddc0a4`
- v1.2 writer SHA256: `ea1bcf695a451add489fb772d22d4036529bd6ac2f7a2c34bcc3c94e46f7438e`

The v1.3 writer SHA256 is `7f648a1d56c1cacdab9c769e1882ed4ba1fe7c3bc0de40046c40dcc709c0b839`. The validator binds both hashes and requires that they differ.

## Drift Audit

The source diff is limited to recovery scheduling:

1. Startup and explicit `reconcile()` still perform full durable recovery.
2. A failed commit sets `recovery_required`; the next commit recovers before writing.
3. Successful steady-state commits no longer rescan all durable parts.
4. The replay adapter no longer calls `reconcile()` before every callback.

This is a 9-line addition and 3-line removal relative to the v1.2-bound writer. It does not change the journal schema, storage format, atomic part publication, cursor advancement ordering, economic input scope, or any permission.

## Authority And Gates

All v1.2 authority semantics and hard gates remain in force. Journal-v2 is the mechanics authority, legacy traces remain diagnostic-only, and all 40 days must still pass dual-clock, writer health, cancel-reject, terminal quantity, C++ event-stream, source identity, and daily fresh-start checks.

The successor changes no denominator, replay result, model, P3 artifact, Feature DAG, action, q90 state, or live policy.

## Permissions

```text
formal_40day_lockstep = false
cif_training = false
economic_evaluation = false
q90_action = false
prospective_live_epoch_transport = false
live_deployment = false
```
