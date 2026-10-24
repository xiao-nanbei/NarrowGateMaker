# Internal Cache Reference Audit

Last materially modified: 2026-08-05

Status: read-only audit complete; no cache/raw/frozen artifact was deleted, moved, or modified.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Result

- Internal cache logical size: **274.878 GiB**.
- Audited deletion candidates: **0.030 GiB allocated**, 1226 inodes.
- Protected/referenced items: **424**, 273.783 GiB logical after nested-item de-duplication.
- Monolithic windows: **250/251 protected**, 0 safe candidates, 1 manual-review item.
- Protected current F07 v13 windows: **40**.
- All v12 windows remain protected because semantic variant ownership is unresolved.
- All v13 windows remain protected; the single no-exact-reference variant is manual review, not a candidate.

## Classification

| Class | Items | Logical GiB |
|---|---:|---:|
| `currently_referenced` | 41 | 54.196 |
| `frozen_historical_referenced` | 283 | 128.355 |
| `safely_unreferenced_deletion_candidate` | 42 | 0.028 |
| `superseded_but_referenced` | 100 | 91.232 |
| `unknown_manual_review` | 2 | 1.094 |

## Recommended Batches

1. Repo-local interpreter/test/linter caches: 0.030 GiB allocated. Re-run the validator immediately before any owner-approved deletion.

## Preserve / Manual Review

- `window_cache` v12: superseded but semantically referenced; do not delete until each variant is resolved.
- `window_cache` v13: current/frozen compatibility surface; preserve all variants, including the unmatched 2026-07-25 file.
- `components_v1`: code still has a legacy reader/default path and frozen F09 evidence records component reads.
- F06 mechanics/queue/request-state directories: frozen manifests bind their relocated legacy paths.
- Old q90 derived outputs on `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` are outside this deletion scope; their 40 current v13 inputs are protected.
- F03 failed v1 and partial v2 roots are on `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}`. The current v2 salvage decision is unresolved, so none is an internal-disk deletion candidate.

## Alias Audit

The retired `${HOME}/MarketData/NarrowGate_BTCUSDC` tree does not exist and no cache symlink was found. Legacy window-cache paths are resolved by `data_paths.relocate_marketdata_path()` to the internal cache; other retired data paths resolve to `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}`. Deleting an internal file can therefore break a frozen manifest even when that manifest records the retired path.

The companion CSV contains exact paths, sizes, mtimes, inode counts and reasons for every audited item.
