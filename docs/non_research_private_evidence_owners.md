# Non-Research Private Evidence Owners

Last materially modified: 2026-08-12

Status: Current repository-wide ownership contract.

NarrowGate keeps public source code, public templates, tests, contracts, and publishable aggregate evidence in the tracked repository. Machine-specific resolution, restricted-source indexes, unpublished operational evidence, and exact private artifacts belong to one ignored owner-private directory; creating a `private/` directory does not make public code private and does not replace portable path resolution.

## Owner Map

| Owner-private root | Canonical responsibility |
| --- | --- |
| `docs/private/` | Repository-wide and cross-project runtime pointers, the current private live/replay configuration, historical operational configurations, private redaction maps, and the repository-level private artifact catalog. |
| `live/private/` | Live-component-local unpublished diagnostics, local runtime evidence indexes, and live implementation notes that are not shared repository authority. Current host and deploy-config authority remains owned by `docs/private/`. |
| `data/private/` | Restricted-source resolution, licensed-provider mappings, local data-admission locators, and unpublished source-quality evidence indexes. Raw market data remains outside the repository. |
| `models/private/` | Unpublished model-bundle locators, private checkpoint/evaluation indexes, the non-published working-tree projection index, and model-owner context. Large model bytes remain in the governed artifact store. |
| `execution/private/` | Unpublished lifecycle, queue, order, fill, and journal evidence indexes owned by the execution substrate. Raw account/order/fill tapes remain outside the repository. |
| `research/**/private/` | Research-unit-specific artifact catalogs, unpublished evidence locators, and owner-only research context, following [Public Research and Private Evidence Layout](../research/PRIVATE_EVIDENCE.md). |

An artifact has exactly one canonical private owner. A consuming unit records the owner's stable artifact ID and does not duplicate the private locator. Repository-wide current authority belongs to `docs/private/`; a component-private directory must not shadow or override it.

## Local Directory Contract

Each owner-private root is ignored by Git and begins with `README.local.md`, whose first line is `Local only — do not publish.` Each root has `catalog.current.local.json` using the public [private artifact catalog schema](../research/private_artifact_catalog.schema.json). Private JSON and YAML normally declare `local_only_do_not_publish`; exact historical bytes and schema-constrained runtime records may inherit classification from the ignored root marker and catalog rather than being modified.

Catalog entries use a stable `artifact_id`, exact local resolution, SHA256 and byte size when the bytes are evidence rather than secrets, an explicit role, `private_not_distributed` availability, `panel_role`, `read_gate`, verification time, and related public documents. An entry with `historical_or_operational_unspecified` is fail-closed for Development, Validation, and sealed-holdout use.

Secrets are not evidence artifacts. API keys, exchange secrets, passwords, signing keys, recovery material, and the contents or hash of `live/.env` remain in the ignored secret/runtime surface and must never be copied into a private catalog, private note, public document, test fixture, or evidence bundle.

## Publication and Handoff Gate

Before handoff, run [`audit_private_evidence.py`](../scripts/audit_private_evidence.py) and verify that all owner-private roots are ignored, markers and catalogs parse, every cataloged byte matches its SHA256 and size, current operational pointers have an operational read gate, historical artifacts are not presented as current, and public files contain only stable artifact IDs, approved placeholders, repository paths, or public URLs. Missing bytes, unresolved locators, duplicate ownership, and hash drift fail closed. Sanitized JSON below Git-ignored model bundle directories must be indexed as `private_working_tree_projection_not_distributed`; it may not appear in the public projection manifest.
