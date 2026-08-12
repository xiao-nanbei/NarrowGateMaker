# Cache Tier LRU Governance v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](public_private_documentation_contract.md).

Status: implemented; runtime access hooks and fail-closed migration/deletion workflow are available.

## Storage Roles

- Hot tier: `${NARROWGATE_CACHE_ROOT}`
- Cold tier: `${NARROWGATE_DATA_ROOT}/cache`
- Access ledger: hot-tier SQLite at `.cache_tier_lru/access_ledger.sqlite3`
- Raw data, reports, frozen artifacts, model bundles, and other authoritative non-cache data are never eligible.

The hot path remains stable after migration. A migrated artifact is replaced by a verified relative symlink to its cold copy, so existing frozen manifests and loaders do not need path rewrites.

## LRU Policy

1. Validated cache hits and completed atomic cache writes update the SQLite ledger on a best-effort basis. Ledger failure never changes cache read/write semantics and is recorded as a health event.
2. The hot filesystem has a 60 GiB free-space safety reserve and a 70 GiB recovery target.
3. When free space falls below 60 GiB, the migration plan selects eligible hot artifacts by oldest access time, then lowest access count, then stable relative path until projected free space reaches 70 GiB.
4. Referenced and frozen cache may move because the stable hot path is preserved. Reference class never grants deletion authority.
5. Cold deletion is limited to `unreferenced` artifacts created by this LRU. The artifact must have lived in cold storage for at least 180 days and must have no observed access during that interval. A later access restarts the inactivity interval.
6. Pre-existing `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` cache, unknown-reference cache, and transaction recovery remnants are never granted deletion authority automatically.

Accesses to nested day/hour artifacts are charged to the narrowest already managed ancestor. This keeps the LRU observation boundary identical to the migration boundary and prevents parent/child rows from competing in one plan.

## Transaction Contract

Every scan, plan, migration, and deletion takes the shared cache-tier lock. Plans bind the filesystem snapshot, artifact fingerprint, access row, thresholds, roots, and reference-audit SHA256. Execution requires both `--execute` and the operation-specific owner token derived from the frozen plan SHA256.

Migration copies to a partial path, verifies content, fsyncs it, atomically publishes cold content, atomically replaces hot content with a symlink, commits the ledger, and only then removes the hot backup. Deletion first moves cold content and the hot link to recoverable tombstones, commits the ledger deletion, and only then removes those tombstones. Interrupted backup/tombstone files are retained for reconciliation; generic stale cleanup may remove only harmless copy/link temporaries.

Material hot and cold artifacts with the same logical relative path fail scanning. A managed cold row without its hot compatibility symlink fails validation.

## Reference Authority

The current reference audit is [internal_cache_reference_audit_v1_20260808.csv](../research/shared/replay_lifecycle/docs/internal_cache_reference_audit_v1_20260808.csv), SHA256 `e9cea7e402e75ae588a9fd198b325c5d0fd95a40aaf0ad6679435613f7ca6099`.

It classifies 424 protected/referenced items (273.783 GiB logical) and two cache items for manual review. Its 42 safe unreferenced candidates are repo-local interpreter/test caches outside the hot-tier root; they are recorded as out of scope and are never migrated or deleted by this LRU. Protection blocks deletion, not transparent migration.

## Operations

```bash
.venv/bin/python scripts/manage_cache_tiers.py scan \
  --reference-audit-csv research/shared/replay_lifecycle/docs/internal_cache_reference_audit_v1_20260808.csv

.venv/bin/python scripts/manage_cache_tiers.py plan \
  --operation all \
  --output ${NARROWGATE_EPHEMERAL_ROOT}/narrowgate_cache_tier_plan.json

.venv/bin/python scripts/manage_cache_tiers.py apply \
  --plan ${NARROWGATE_EPHEMERAL_ROOT}/narrowgate_cache_tier_plan.json \
  --operation migrate \
  --owner-token OWNER-MIGRATE-<plan_sha256> \
  --execute

.venv/bin/python scripts/manage_cache_tiers.py validate \
  --plan ${NARROWGATE_EPHEMERAL_ROOT}/narrowgate_cache_tier_plan.json
```

Deletion uses a separately frozen plan and `OWNER-DELETE-<plan_sha256>`. Migration authority never implies deletion authority.

The initial capacity recovery is recorded in [cache_tier_lru_initial_migration_20260808.md](../research/shared/replay_lifecycle/docs/cache_tier_lru_initial_migration_20260808.md).
