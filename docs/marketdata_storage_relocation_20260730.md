# MarketData APFS Relocation

Last materially modified: 2026-08-12

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](public_private_documentation_contract.md).

## Contract

The machine-local market-data authority moved from `${NARROWGATE_RETIRED_MARKETDATA_ROOT}` to `${NARROWGATE_MARKETDATA_ROOT}`. The external `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` device is formatted as APFS. The project deliberately does not create a compatibility symlink at the retired path.

Active code uses these roots:

```text
NARROWGATE_MARKETDATA_ROOT=<local-marketdata-root>
NARROWGATE_DATA_ROOT=<local-marketdata-root>/NarrowGate_BTCUSDC
NARROWGATE_CACHE_ROOT=<local-cache-root>
```

`NARROWGATE_MARKETDATA_ROOT` controls provider-level raw archives such as `cryptohftdata` and `tardis`. `NARROWGATE_DATA_ROOT` controls NarrowGate's normalized data, reports, imported live tapes, and replay outputs. `NARROWGATE_CACHE_ROOT` independently controls reproducible caches on the internal disk. `NARROWGATE_TICK_WINDOW_CACHE_DIR` may override only the tick window cache. `MM_DATA_ROOT` remains a deprecated data-root override.

## Frozen provenance

Historical Spec and report JSON files are evidence and are not byte-rewritten merely because storage moved. They may therefore contain `${NARROWGATE_RETIRED_MARKETDATA_ROOT}/...`. Runtime consumers must apply `data_paths.relocate_marketdata_path()` at the filesystem boundary and then verify the original SHA256. Relocation changes location, never dataset or artifact identity.

Four closed F09/F10 historical producer contracts predate that runtime rule: the cooldown one-cycle reaudit, first-add producer, first-opener contract, and first-opener producer. Their frozen Specs retain the retired absolute path and also bind old `backtest_tick.py` or `data_windows.py` implementation hashes. They therefore fail closed after the relocation and subsequent replay-engine development. They are not active training, cache, baseline, or live paths. Repair requires a new producer or registered archive-backed historical reproduction identity. Restoring the retired directory, adding a compatibility symlink, or rewriting the frozen Specs is forbidden.

## Operational rules

- Fail closed when `${NARROWGATE_STORAGE_ROOT}` is absent before a large download, replay, normalization, or report job.
- Keep reproducible caches on the internal disk. A missing removable volume must not redirect non-cache output into the repository or cache tree.
- Preserve source provenance. A Tardis file must not be written into the `cryptohftdata` raw tree or labelled as CryptoHFTData.
- Admit a repaired UTC day only after native sequence/bootstrap, timestamp, coverage, D-1 warmup, and cross-source good-day gates pass.
- Keep at least 50 GiB free and, before a new build, require free space of at least `60 GiB + 2.5 × expected final new output`.
- Store transfer and admission manifests under `${NARROWGATE_DATA_ROOT}/reports/`; disposable staging files are not formal evidence.

The machine-readable relocation identity is `docs/marketdata_storage_relocation_20260730.json`.

## Verification record

The APFS destination and the legacy source matched on 46,570 regular files, 6,949 directories, 266 symlinks, 12 pre-existing broken symlinks, 415 hardlink inode groups, and 349,998,927,580 logical bytes. A full content-checksum rsync completed successfully. After removing 1,344 AppleDouble sidecars introduced by the first extended-attribute attempt, the final structure dry-run produced zero differences. After separate explicit confirmation on 2026-07-31, the legacy `${NARROWGATE_RETIRED_MARKETDATA_ROOT}` copy (about 231 GiB at deletion time) was deleted. No compatibility symlink was created.

On 2026-07-31, all 266 replay compatibility links on `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` were retargeted from `${NARROWGATE_RETIRED_MARKETDATA_ROOT}/...` to `${NARROWGATE_MARKETDATA_ROOT}/...`. The 254 previously valid links remain valid; the same 12 known L2 links remain broken because their target payloads do not exist. No link on `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` still depends on the old disk path.

The two pre-split `window_cache` copies were checked in both rsync directions on 2026-07-31: 2,230 files and 68,139,019,746 logical bytes had zero metadata differences. The internal copy was moved on the same APFS filesystem to `${NARROWGATE_CACHE_ROOT}/window_cache` and the bidirectional dry-run remained clean afterward. Deleting the `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` duplicate remained a separate destructive operation, not implied by the path-contract change. After explicit confirmation on 2026-07-31, the approximately 64 GiB `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` duplicate was deleted. The internal `${NARROWGATE_CACHE_ROOT}/window_cache` copy remains authoritative.

A final old-to-`${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` non-cache dry-run found only three `.DS_Store` metadata differences totaling 55,308 bytes plus directory mtimes. No market-data, feature, model, report, tape, or replay payload was missing from `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}`. The old non-cache tree was therefore admitted for the explicitly confirmed deletion recorded above. Post-delete checks found no old absolute symlink dependency; the 12 pre-existing missing-target relative links remain unchanged by design. `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` had about 519 GiB free and the internal volume about 288 GiB free after the two deletions.

## Post-migration acquisition

After the verified copy, the `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` destination received a source-separated Tardis delivery of 420 Binance Futures BTCUSDC files totaling 55,996,845,705 compressed bytes. The completed raw admission covers 2026-01-01 through 2026-07-29 and makes all 67 historical bad-day candidates raw-repair-ready; formal normalized-good-day status is unchanged. See `docs/tardis_bad_day_repair_20260730.md`.

After the one-off Tardis acquisition closed, its redundant delivery directory level was removed. The physical root is now `${NARROWGATE_MARKETDATA_ROOT}/tardis/{binance-futures,manifests}`. Frozen manifest bytes retain the former path as provenance and are resolved through the documented relocation mapping; no `0730-beinan` compatibility symlink exists. Tardis is not part of the recurring incremental-day workflow.
