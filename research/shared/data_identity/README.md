# D: Data Identity And Good-Day Admission

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Documentation boundary: this README and the unit's tracked `docs/` are public. Owner-only artifact locators, unpublished evidence indexes, and private research context are resolved through this unit's ignored local `private/` catalog and are not distributed with the public repository. See the [public/private research layout](../../PRIVATE_EVIDENCE.md).

Canonical code remains at `data_quality.py`, `data_paths.py`, `models/audit/minimal_marketdata_daily_quality.py`, and `models/audit/native_exchange_book_universe_v2.py`.

The cross-family rule for deriving experiment denominators, chronological OOF folds, and training windows from this capability layer is frozen in [`unified_data_universe_and_split_contract_v1`](../experiment_governance/docs/unified_data_universe_and_split_contract_v1_20260812.md). One physical archive universe does not grant every day the same prediction, queue, action, or live-transport authority.

## Tardis normalized L2 coverage

The `normalized_tardis_l2_100ms_v1` coverage identity for 2025-08-01 through 2026-07-30 has 364 calendar days. The owner excluded 2026-07-30 because its source payload is unavailable, so it is not a normalization target or a raw download blocker.

- Target days: 363
- Atomically admitted `quality` + `bbo` + `l2` + `clock` days: 363
- Runnable missing days: 0
- Raw-blocked target days: 0
- Provider-normalized replay candidates: 180
- Policy-visible or exact-queue-authorized days: 0

The final machine-readable coverage report is stored under `$NARROWGATE_MARKETDATA_ROOT/reports/continuous_calendar_substrate_v1_20260803/normalized_coverage_final.json`. Its SHA256 is `2f927c1ee3be75087585c5e6ddd73c9bc2f91a97c86990a6e0943af45ed7981e`.

Artifact presence is not good-day admission. Provider-normalized days may be used only by research identities that explicitly permit that source and clock. They do not acquire native sequence, queue, policy, or live authority. The pre-existing 2026-05-31 and 2026-07-08 clock sidecars remain fail-closed under their frozen structural audits and were not overwritten during backfill.
