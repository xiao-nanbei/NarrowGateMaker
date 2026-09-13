# Legacy v12 Window Cache Semantic Reference Audit

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](public_private_documentation_contract.md).

Status: read-only; no cache file was deleted, moved, modified, or unpickled.

## Result

- v12 payloads: 98 across 40 UTC days (88.98 GiB).
- Exact-identity must retain: 0.
- Variant semantics unresolved: 98.
- Unreferenced rebuildable candidates: 0.
- Byte-identical same-day groups: 18.

A byte-identical payload is not automatically deletable: the filename digest is a lookup identity and may encode a different feature-context, inference, quality-day, or source-signature contract.

## Frozen Semantic Identities

| Identity | Referenced days | Matched v12 variants | Evidence |
|---|---:|---:|---|
| `placement_fill_request_state_race_v2` | 76 | 98 | `${NARROWGATE_ROOT}/research/families/f06_placement_fill_cif/docs/placement_fill_request_state_race_v2_spec_20260728.json` |
| `placement_fill_request_state_race_v2` | 50 | 96 | `${NARROWGATE_DATA_ROOT}/reports/placement_fill_request_state_race_v2_development_20260728_v3/placement/manifest.json` |
| `placement_fill_request_state_race_v2` | 50 | 96 | `${NARROWGATE_DATA_ROOT}/reports/placement_fill_request_state_race_v2_development_20260728_v3/placement/preflight_manifest.json` |
| `placement_fill_request_state_race_v2` | 1 | 0 | `${NARROWGATE_DATA_ROOT}/reports/placement_fill_request_state_race_v2_development_20260728_v3_smoke/placement/manifest.json` |
| `placement_fill_request_state_race_v2` | 1 | 0 | `${NARROWGATE_DATA_ROOT}/reports/placement_fill_request_state_race_v2_development_20260728_v3_smoke/placement/preflight_manifest.json` |
| `volatility_time_add_rearm_feasibility_source_v1` | 42 | 98 | `${NARROWGATE_DATA_ROOT}/reports/volatility_time_add_rearm_feasibility_v1_20260729/source_replay/campaign_outcome_replay_volatility_time_add_rearm_feasibility_source_v1_btcusdc.json` |

## Governance

`must_retain_exact_identity` requires preservation. `duplicate_but_variant_semantics_unresolved` also remains preservation-required until the historical key can be reconstructed or an exact successor artifact is admitted. `rebuildable_delete_candidate_unreferenced` is only a review queue; deletion still requires a fresh hash/reference audit and an explicit execution receipt.

Canonical audit SHA256: `8f2845d0dccb70ac663c572c506401cccfcd17f01187973d9542b83a14cc8e2f`
