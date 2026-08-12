# Cache Tier LRU Initial Migration

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Status: complete and validated; no cache was deleted.

## Result

- Hot free space before planning: 43,766,210,560 bytes (about 40.8 GiB).
- Frozen recovery target: 75,161,927,680 bytes (70 GiB).
- Migrated: 51 frozen cache artifacts, 32,128,795,393 bytes.
- Hot free space after migration: 75,889,786,880 bytes (about 70.7 GiB; `df -h` reports 71 GiB).
- Cold free space after migration: 471,439,880,192 bytes (`df -h` reports 439 GiB).
- Errors: zero.
- Warnings: zero.
- Transaction remnants: zero.
- Post-migration validation issues: zero.
- Deletions: zero.

The plan excluded two invalid legacy cache directories rather than touching them:

- `window_cache/active_order_queue_tape_v3`
- `window_cache/paired_action_resolution_sparse_tape_v1`

Both contain manifests that reference missing `level_events.parquet` files. They remain on the hot disk and require separate provenance/rebuild review.

## Binding

- Plan identity SHA256: `c8c2087aae9a725dda31f502b9249ae1f9a8d413e585a3f2d4d071cc3abadd4f`.
- Plan file: [cache_tier_lru_initial_migration_plan_20260808.json](cache_tier_lru_initial_migration_plan_20260808.json), file SHA256 `2b280eadfd5f14ce98c7a1d5da96052adb94346b991aaf62264a0cbab76b7c17`.
- Receipt identity SHA256: `26cbe26d93cd270292c0cadee9a4bfca71f71ed4bf00fa9be4a731df9f4bd500`.
- Receipt file: [cache_tier_lru_initial_migration_receipt_20260808.json](cache_tier_lru_initial_migration_receipt_20260808.json), file SHA256 `25bdbeccd0d0fc79cbac64e548f28b681da2ad2132696816266b4003ea3ca3a2`.

A strict access through the migrated hot path resolved to `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` and updated the same cold ledger row (`managed_by_lru=true`, `access_count=1`). A subsequent steady-state plan contained zero migrations and zero deletions.
