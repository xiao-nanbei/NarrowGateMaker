# Replay Cache DAG v2 Governance Audit

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](public_private_documentation_contract.md).

Status: Componentized external-cache build complete; the audited unreferenced v10/v11 subset was pruned with an atomic receipt. v12/v13 remain preserved.

## Scope

This audit initially covered the 269 monolithic v10-v13 tick-window pickle files under the internal cache root. It reads file names, sizes, mtimes, payload hashes and exact textual references. A separate fail-closed prune tool later used that inventory to delete only the approved v10/v11 subset.

The audit implementation is `models/replay_cache_audit.py`; the CLI is `scripts/audit_legacy_replay_cache.py`.

The machine-readable inventory is [`replay_cache_legacy_reference_audit_20260804.json`](replay_cache_legacy_reference_audit_20260804.json), SHA256 `c03d3939fc403d45d64169bf79be5ebb573f776c87c4e0881c9d6b2f9e983eae`.

The 2026-08-04 rerun excludes prior audit outputs from the reference scan. Without that exclusion, an inventory can cite every basename it just listed and incorrectly classify itself as evidence.

## Inventory

| Version | Files | Distinct dates | Logical size |
|---|---:|---:|---:|
| v10 | 17 | 17 | 17.06 GiB |
| v11 | 1 | 1 | 1.13 GiB |
| v12 | 98 | 40 | 88.98 GiB |
| v13 | 153 | 129 | 171.43 GiB |
| Total before pruning | 269 | 132 | 278.60 GiB |

There are 42 dates with duplicate variants, 137 files beyond a one-file-per-day layout, and at most six variants on one date. This measures the upper bound of same-day duplication; it does not establish byte-equivalence or deletion eligibility.

## Reference Classes

Exact basename scanning found:

| Governance class | Files | Logical size | Current action |
|---|---:|---:|---|
| Frozen/evidence referenced v13 | 152 | 170.34 GiB | Preserve |
| No scanned textual reference | 117 | 108.27 GiB | Candidate only; approval required |

The 152 references come from three `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` manifests:

- `buy_q90_abi_v4_40day_lockstep_v1_20260803/development/input_cache_manifest.json`;
- `cache_prewarm_native_45d_causal_v11_ml_v13/manifest.json`;
- `cache_prewarm_provider_20250801_20251231_v13/manifest.json`.

All v10-v12 files and one v13 file were unreferenced by the initial exact textual scan. That alone was not deletion authority: a historical identity may bind a digest or derive a path without storing the basename. The subsequent semantic audit classified the versions separately:

- v10/v11: 18 files, 19,534,951,802 bytes, no exact or semantic frozen references; approved and deleted;
- v12: 98 files, 40 target dates, generic F06/F09 semantic bindings remain; preserve until variant identity can be resolved;
- v13: current compatibility cache; preserve all 153 files, including the one without an exact basename reference.

The v10/v11 execution receipt is `${NARROWGATE_DATA_ROOT}/cache_prune_receipts/legacy_window_cache_v10_v11_prune_receipt_20260804.json`, file SHA256 `1656f5eb0615a7792836a07f56b6b2e85a048472aa1af86c6d3800d2f1478e19`. Its internally canonical receipt SHA256 is `84bca26f67ef1997d2a8db7d3ef848dffd3ba3adb07ade1adf1f4517717a543b`. The complete pre-delete dry run is stored beside it, file SHA256 `b928583186e8907ad47fdae19ca3b9f9de5c14c2bda92abbc59d02575cd679bd`.

## Implemented Boundary

The v2 graph persists exactly three layers:

```text
native_book_hour
market_context_day_v2
model_overlay_day
```

`market_context_day_v2` stores compressed trades and rolling arrays plus source references. It does not copy normalized BBO/L2. `model_overlay_day` stores only model-bound arrays. `WindowData` is assembled in memory.

Source identity is relocation-neutral: only stable role, logical source and content SHA256 enter the identity digest. Absolute path, size and mtime remain in the manifest as locators/provenance and do not invalidate reuse after a same-byte disk move or mtime touch. Producer manifests supply hashes for normalized BBO/L2, bars, trades and features where available; direct content hashing is limited to small unmanifested inputs and synthetic fixtures.

Orders, queue position, cancel/ACK paths, fills, remaining quantity, inventory, cooldown, campaign state, markout, reward and PnL are explicitly forbidden from shared persistence.

## Verification

Synthetic fixtures verify:

- graph topology and the three persistent boundaries;
- compressed market-context round trip with DataFrame/ndarray parity;
- model-overlay round trip and independent reuse;
- schema, identity and file SHA256 validation;
- tamper detection;
- atomic directory publication and hash-compatible hits;
- same-byte path relocation/mtime changes preserve identity and cache hits;
- source byte changes produce a different identity;
- v13 and components_v1 read compatibility;
- zero-write audit behavior and frozen-reference classification.

Real-day componentized caches are now stored under the removable-volume cache namespace:

`${NARROWGATE_DATA_ROOT}/cache/replay_dag`

This path is deliberately separate from authoritative raw and normalized data. The 200-day F02 build plus the 48-day provider-overlap build completed there; the reusable DAG payload occupies about 198 MiB. Their build summaries are:

- `p3_reach_time_cache_build_200d_20260804.json`;
- `p3_reach_time_cache_build_provider_overlap_48d_20260804.json`.

The internal volume had fallen below the frozen 60 GiB reserve, so the bulk build was explicitly routed to `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` and never fell back silently. After the audited v10/v11 deletion, the internal volume has about 66 GiB available. New large cache jobs may still use `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}`'s project `cache/` namespace when the internal storage gate fails; a missing removable volume remains fail-closed.

No further legacy deletion is authorized by this audit. The v12 semantic identity must first be resolved, and any future v13 pruning needs a fresh reference/hash audit and a new owner-bound receipt.
