# Remote Main Archive - 2026-07-05

Last materially modified: 2026-07-27

Status: Historical repository-migration record; not a current runtime or data-retention contract.

This note records what was checked before replacing the remote `main` branch.

## Branches

- Old `origin/main`: `18ae487323a2d234e354aeac54896ddab2716647`
  - Commit message: legacy BTCUSDT research bootstrap
  - README title: `NarrowGate BTCUSDT`
  - Project title: `NarrowGate BTCUSDT 做市算法项目`
- BTCUSDC branch promoted to `main`: `btcusdc_maker`
  - Base commit before the 2026-07-05 local updates: `9e1538e56cad18d5d2071c7deb2026f499a0c4ca`
  - Commit message: `build BTCUSDC_maker_ml`

## Audit Result

`origin/main` was the retired BTCUSDT execution branch.  The current maintained project scope is BTCUSDC only; BTCUSDT is reference/source data only.

There were no file paths present only on old `origin/main` and absent from the BTCUSDC branch.  The useful information to preserve is the historical fact that old `main` represented BTCUSDT execution assumptions, configuration, model bundle references, and documentation.  Those assumptions must not be copied into BTCUSDC without fresh BTCUSDC retained-day replay, live/replay mechanism sanity, and campaign/order-level evidence.

## Current Decision

- Replace remote `main` with the BTCUSDC project branch.
- Keep BTCUSDT content only as archived branch history, not as active project documentation.
- Keep BTCUSDT raw trades only as BTCUSDC reference/source data aligned to the minimal complete BTCUSDC good-day universe.
