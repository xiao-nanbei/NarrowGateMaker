# Tardis normalized L2 v1 contract errata

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](public_private_documentation_contract.md).

This errata does not change the running 153-day Development batch, its raw payloads, or any canonical good-day registry. The batch remains source-separated, diagnostic-only, `policy_visible=false`, and `exact_queue_policy_eligible=false`.

## Frozen batch-level identities

- normalizer code SHA256: `b458854f60f282faf1e97629ee7d2a1bef152ed4c08fa79c43c05ac0df86e8bd`
- 2025 download-manifest SHA256: `8e01dcd4aa31551ed203bcc5497ab57ad67f6fb04d08c085a5d235010177f13b`

The 306 raw payloads were independently re-hashed and fully decompressed before normalization. That terminal check found 306 unique valid payloads, zero failures, and zero `.part` files. The running v1 daily quality JSONs bind the manifest and per-source SHA256 values, but do not embed the normalizer code SHA256. Code identity is therefore an external batch-level binding, not a per-day embedded identity.

## Corrections to the original operational claims

1. The normalizer verifies that a download manifest is complete and records its raw SHA claims, but it does not independently re-hash each raw payload immediately before reconstruction. The separate 306-file terminal audit is the raw-integrity evidence for this batch.
2. A cached day validates BBO/L2/clock hashes and raw SHA claims, but cache identity does not bind the normalizer code SHA or the availability/hash of a later CryptoHFTData comparison file. Cache hardening must create a new implementation identity after this batch; the running code must not be changed mid-batch.
3. The governance freeze binds the technical day CSV, while its Tardis input manifest is the 2026 overlap manifest. The 2025 raw manifest must therefore be bound separately in final admission.
4. The original aggregate utility reads derived candidate booleans and output hash claims from daily quality JSONs. Those claims alone are not a sufficient final integrity check.

## Required post-batch admission

`data/audit_tardis_normalized_batch.py` is the fail-closed final auditor. It must run only after all workers finish. It independently:

- checks the frozen technical day set and 2025 raw-manifest identity;
- optionally re-hashes all raw payloads with `--rehash-raw`;
- re-hashes every BBO, L2, clock, quality, and available CryptoHFTData source;
- validates the top-20 schema and common monotonic 100ms UTC clock;
- verifies provider-local and exchange-cut sidecar causality;
- re-derives all candidate gates from primitive metrics;
- rejects missing days, duplicate identities, stale hashes, derived-gate mismatches, and any remaining temporary output.

Final admission must pass with the two frozen SHA256 values above. A passing engineering audit still does not promote a day into the canonical good-day set and does not grant queue, action, or live-policy eligibility.

## Physical archive root flattening

After the one-off historical acquisition and all admission audits completed, the archive was moved on the same APFS volume from:

```text
${NARROWGATE_MARKETDATA_ROOT}/tardis/0730-beinan/{binance-futures,manifests}
```

to:

```text
${NARROWGATE_MARKETDATA_ROOT}/tardis/{binance-futures,manifests}
```

No compatibility symlink remains. Frozen manifests and quality JSONs preserve their original path strings and SHA256 identities; active readers apply this single explicit relocation mapping when the frozen path is absent. The source identity retains `0730-beinan` as delivery provenance, not as a filesystem layout contract. Tardis is not a recurring daily source; subsequent daily increments continue through the original exchange/provider pipelines.
