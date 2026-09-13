# Public Research and Private Evidence Layout

Last materially modified: 2026-08-12

Status: Current repository-wide convention.

Every concrete NarrowGate research unit has two documentation surfaces: public material belongs in the tracked unit `README.md`, `docs/`, source, tests, and sanitized evidence bundles; owner-only locators and operational context belong in that unit's ignored `private/` directory. The applicable units are the ten directories under `families/`, the four concrete layers under `shared/`, and `system_engineering/`. Umbrella directories such as `research/`, `research/families/`, and `research/shared/` do not need duplicate private catalogs.

## What May Be Published

Public GitHub material may include the research question, estimand, frozen protocol, feature and label definitions, data provenance at a logical or public-source level, chronological splits, source code, tests, aggregate results, limitations, decisions, promotion authority, and reproducible commands that use public placeholders. Small evidence bundles may be tracked only when redistribution is lawful, private fields are removed, and the document links them directly. A SHA256 value may identify a named artifact, but the same row must say whether the bytes are in the public repository, a public release, or a private evidence store that is not distributed.

Public files must not contain a personal absolute path, physical volume name, private host or IP address, SSH target, cloud instance or account identifier, process identifier, credential, raw live position, raw order or fill ledger, or owner-only account state. Use the placeholders in [Path Conventions](../docs/path_conventions.md), logical deployment epochs, repository-relative links, and the availability terms in the [Public and Private Documentation and Evidence Contract](../docs/public_private_documentation_contract.md).

Public JSON and CSV files that formerly embedded owner-specific locators are sanitized projections. Their private-source SHA256, public-projection SHA256, transformation identity, ownership, and availability are indexed in [`public_machine_document_projections.json`](public_machine_document_projections.json). The exact pre-redaction bytes live only under the owning unit's ignored `private/original_public_machine_records/`; because they are byte-preserved evidence, they are not edited to add a visibility field and instead inherit private classification from the ignored directory and catalog entry. Public projections preserve research statements and non-locator fields but do not claim byte identity with those private originals.

## What Is Read Privately

Each unit's `private/` directory may hold an owner-only catalog, exact local or remote locators, licensed-source mappings, raw-live evidence indexes, unpublished exploratory notes, and private review context. Large datasets and evidence artifacts normally remain in the external evidence store; the private catalog points to them and records their identity. Ignoring this directory in Git prevents accidental publication, but it is not encryption or a secret manager: API keys, exchange secrets, signing keys, passwords, and recovery material must remain in the project's approved secret store or ignored runtime configuration and must never be written into research notes. A schema-constrained private runtime/config record may keep its exact consumer-compatible bytes when the private directory marker and catalog classify it; do not inject an unsupported documentation field merely to label it.

An agent or researcher should read the public unit documentation first. It may then read that unit's private catalog only when the task authorizes use of owner-side evidence. A private locator may be used to resolve and validate evidence locally, but it must not be copied into a public report. Cross-family evidence is cataloged once by its owning unit; consumers refer to the owning unit and logical artifact ID instead of duplicating private paths.

## Local Directory Template

The repository ignores `research/**/private/`. A local checkout creates `private/README.local.md` in each concrete research unit as the do-not-publish marker and may add a `catalog.current.local.json` following the public [`private_artifact_catalog.schema.json`](private_artifact_catalog.schema.json), with this minimum shape:

```json
{
  "schema_version": "narrowgate_private_artifact_catalog_v1",
  "visibility": "local_only_do_not_publish",
  "documentation_scope": "local_only_do_not_publish",
  "unit_id": "F05",
  "entries": [
    {
      "artifact_id": "logical-stable-name",
      "role": "training_input|economic_output|audit_receipt|live_evidence",
      "local_path": "<resolved only in the ignored local file>",
      "sha256": "<64 lowercase hexadecimal characters>",
      "bytes": 0,
      "availability": "private_not_distributed",
      "panel_role": "development|validation|sealed_holdout|operational|historical",
      "read_gate": "<explicit authorization required before access>",
      "last_verified_utc": "<optional UTC timestamp>",
      "related_public_docs": ["<repository-relative path>"],
      "public_projection": "<optional repository-relative path>",
      "source_interval_utc": "<optional interval>",
      "notes": "<no credentials or secret values>"
    }
  ]
}
```

`artifact_id` is the stable reference used by public prose; `local_path` is private resolution; `sha256` and `bytes` verify identity; `availability` states whether third parties can obtain the bytes. `panel_role` and `read_gate` prevent Development work from reading Validation or sealed-holdout evidence. The legacy value `historical_or_operational_unspecified` is deliberately fail-closed: it cannot be read automatically by Development, Validation, or holdout workflows until an owner classifies it without consulting restricted outcomes. A catalog entry does not grant Development, Validation, holdout, action, or live authority, and a presentation-only public projection must not claim to be a rerun or a byte-identical replacement for the private source.

Bulk-migrated locator occurrences with null `sha256` or null `bytes` are a quarantine ledger, not admitted evidence. Only a semantic entry with a unique owner, resolvable byte source, verified SHA256 and size, explicit `panel_role`, and explicit `read_gate` may be used for owner-side reproduction. Missing historical bytes remain hash-only and fail closed; a current mutable file must never be substituted merely because its path looks similar.

## Publication Check

Before publishing, confirm that the public report stands on its own, every repository link resolves, every non-public artifact is explicitly labelled `private evidence store; not distributed with the public repository`, and scans find no private locators or live identifiers. Keep the private original and its hashes unchanged when producing a sanitized public projection; give the projection a distinct public identity and SHA256, whether it retains the former repository path or uses a new filename, and identify the transformation as locator or field redaction only.
