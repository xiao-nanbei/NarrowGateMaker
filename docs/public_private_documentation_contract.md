# Public and Private Documentation and Evidence Contract

[English](public_private_documentation_contract.md) | [简体中文](public_private_documentation_contract.zh-CN.md)

Last materially modified: 2026-08-29

Last materially synchronized: 2026-08-29

Status: Current public documentation contract.

Implementation receipt: [Public/Private Evidence Governance Handoff](public_private_governance_handoff_20260812.md).

This repository is intended to be understandable when read on GitHub without access to the owner's workstation, storage volumes, cloud account, or live-trading credentials. Human-facing Markdown is public unless it is stored under an ignored owner-private root defined by the [non-research owner map](non_research_private_evidence_owners.md) or a concrete research unit's ignored `private/` directory.

## Public human-facing documents

Public Markdown must explain the research question, data authority, estimand, split, result, limitations, and current status in ordinary language. Repository files must be linked with repository-relative Markdown links. External public sources must use stable public URLs.

Do not publish personal absolute paths, physical volume names, private IP addresses, SSH targets, instance identifiers, process IDs, account state, raw live positions, or credentials. Use the placeholders defined in [Path Conventions](path_conventions.md) and logical deployment-epoch names instead.

A SHA256 value is verification metadata, not a locator. A report may show a hash only beside a named artifact and an explicit availability value. Every referenced artifact must be either linked to a public repository or release asset, or labelled `private evidence store; not distributed with the public repository`. Never leave a bare hash or machine-local path and expect a public reader to infer where the artifact lives.

Unless a document provides a more specific artifact table, hashes embedded in historical public research reports identify owner-side evidence retained in the private evidence store and not distributed with the public repository. The owning unit's public README and private catalog define availability; a hash never implies that the bytes are downloadable from GitHub.

Historical findings remain historical facts after presentation-only cleanup. Reflowing a paragraph, replacing a machine-local locator with a logical locator, or adding an availability statement must not be described as a rerun or a new scientific result.

## Language and translation identity

Agent-only instructions use English prose and do not receive translation copies. This includes repository and owner-installed `SKILL.md` files, skill references, and agent metadata. Code identifiers, paths, formulas, hashes, and protocol tokens remain unchanged.

Maintained human-facing narrative guides use paired files: `name.md` is the canonical English document and `name.zh-CN.md` is its Simplified Chinese counterpart. Both files place reciprocal language links near the top and keep one prose language per body, except for code, identifiers, paths, formulas, proper nouns, and necessary first-use technical terms.

A substantive change to status, conclusions, safety boundaries, commands, links, or evidence availability must update both language files in the same change. Both files carry the same `Last materially synchronized: YYYY-MM-DD` value. The translation may be idiomatic, but it must preserve the same material claims, limitations, and authority boundaries; it must not become a shortened summary.

Machine-readable records, manifests, receipts, generated outputs, and hash-bound frozen evidence retain one byte identity and are not translated. Both language guides cite the same underlying artifact. Immutable historical Markdown may remain in its original language when editing or duplicating it would disturb evidence identity; a maintained bilingual index or reader summary should explain it instead of presenting a translation as the frozen source.

Do not create translation copies for the historical documentation tree in bulk. Prioritize current README, quickstart, architecture, contributor, security, operations, and research-navigation guides. Other maintained human guides receive or repair their language pair when they are materially edited. The English license text remains authoritative; any reader-facing Chinese explanation must state that it is non-authoritative.

## Public machine-readable records

Tracked JSON and other machine-readable records may preserve executed artifact hashes and frozen identity fields. Reader-facing locators should be repository-relative paths, approved placeholders, logical artifact IDs, or public URLs. If a public projection redacts a private locator, it must identify the private source artifact by hash and state that the transformation changed locators only; it must not claim byte identity with the private source.

Repository-wide public projections outside `research/` are indexed in [`public_machine_document_projections.json`](public_machine_document_projections.json); research-owned projections are indexed in [`research/public_machine_document_projections.json`](../research/public_machine_document_projections.json). These indexes bind each public projection to the SHA256 of its private source bytes without publishing the private locator.

A mutable public machine record must stop claiming projection identity when its successor changes governance semantics rather than merely redacting locators. Publish the successor as ordinary safe public JSON, remove its active projection-manifest entry, and record the retired predecessor public-projection hash, predecessor private-source hash, private availability, and a statement that the ignored predecessor source was not rewritten. Those predecessor hashes are historical retirement metadata only; they do not make the successor a projection or grant access to the private bytes.

A machine record is a public projection only when its bytes are available to a public checkout or are being added to the tracked public tree in the same change. Sanitized records that remain below Git-ignored model bundle directories are private working-tree projections, not GitHub artifacts. Their identities are kept in the ignored `models/private/` index and their availability is `private_working_tree_projection_not_distributed`; they must not be counted in the public projection manifest.

An artifact availability field should use one of: `public_repository`, `public_release`, `private_not_distributed`, `private_working_tree_projection_not_distributed`, `restricted_raw_source`, or `derived_reproducible_not_distributed`. Code that consumes placeholder-bearing records must resolve only an allowlisted placeholder and fail closed when it is unavailable.

## Private documents and locators

Cross-project private operational details belong under ignored `docs/private/`; live-, data-, model-, and execution-component evidence belongs to the corresponding ignored owner-private root defined in [Non-Research Private Evidence Owners](non_research_private_evidence_owners.md); research-specific locators and evidence indexes belong to the owning unit's ignored `private/` directory as defined in [Public Research and Private Evidence Layout](../research/PRIVATE_EVIDENCE.md). Private Markdown begins with `Local only — do not publish.` Private JSON or YAML uses a top-level `visibility` or `documentation_scope` value of `local_only_do_not_publish`. Exact byte-preserved historical sources below `private/original_public_machine_records/`, and schema-constrained runtime/config records whose consumer rejects unknown fields, remain unmodified and inherit private classification from the ignored directory, its `README.local.md`, and its catalog entry. The current host pointer, physical storage mapping, secret-bearing configuration, and owner-only artifact catalogs live on these private surfaces. They may contain exact local locators because they are not part of the public repository.

One artifact has one canonical private owner. Consumers name the owner and stable artifact ID instead of copying a local path into another catalog. Current repository-wide host/config authority remains under `docs/private/`; component-private roots cannot override it. A public operational pointer or governance identity may summarize the split between current live and replay defaults, but it cannot replace the private current-host pointer, exact live-config alias, owner release, or admitted evidence chain. A frozen health receipt is not latest-liveness authority, and operational evidence is not automatically action-occurrence, economic, or backtest authority. Secrets are never evidence artifacts, and neither the contents nor hash of `live/.env` may be placed in a catalog or document.

Private evidence remains authoritative for owner-side byte verification when its manifest and hashes validate. Unless a sanitized evidence bundle is actually published, public documentation must describe that limitation honestly rather than constructing a non-existent link.

## Review gate

Before publishing, run [`audit_public_documentation.py`](../scripts/audit_public_documentation.py) and, on an authorized owner checkout, [`audit_private_evidence.py`](../scripts/audit_private_evidence.py). The public audit scans human and machine documents, source code, public archives, structured process identifiers, projection availability, and repository links. The private audit verifies all owner roots, marker and catalog policy, permissions, semantic artifact hashes, public/private projection bindings, and the non-published model projection index. Also verify that Markdown reflow is idempotent and that protected code, table, formula, front-matter, and explicit hard-break blocks remain unchanged.
