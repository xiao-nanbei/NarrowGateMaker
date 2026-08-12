# Public/Private Evidence Governance Handoff

Last materially modified: 2026-08-12

Status: Current implementation receipt for the repository HEAD.

This handoff records the public/private governance applied to NarrowGate on 2026-08-12. It is a documentation, evidence-ownership, locator-redaction, and consumer-identity change; it is not a rerun, does not alter research outcomes, and grants no Development, Validation, sealed-holdout, action, or live authority.

Evidence availability: public contracts and sanitized projections are available through repository-relative links. Exact pre-redaction machine records, local locators, historical archives, and owner-side evidence are retained in ignored private owner stores and are not distributed with the public repository. Secrets are outside both evidence layers and are never cataloged.

## Governing Decision

A SHA256 is verification metadata, not private information by itself. A public report or Spec may retain a SHA only when it names the artifact, states which identity the SHA covers, and states whether the bytes are public or private. The private layer owns exact pre-redaction bytes, physical paths, current host/config resolution, process and account state, licensed-source mappings, and unpublished evidence; it does not own public research questions, estimands, thresholds, aggregate conclusions, or portable code merely because those objects contain hashes.

| Layer | Public GitHub content | Owner-private content |
| --- | --- | --- |
| Human documentation | Question, method, split, result, limitations, repository links, named SHA identities, and availability | Machine locators, unpublished review context, raw operational evidence indexes, and restricted-source resolution |
| Machine records | Sanitized projection, public-projection SHA, executed-source SHA, transformation, owner, and availability | Exact pre-redaction source bytes and their resolver |
| Historical evidence | Logical artifact ID, historical SHA, byte count, and honest availability | Exact archive or artifact bytes, verified before any member or row is read |
| Runtime and secrets | Generic template and portable placeholders | Current runtime pointer/config; credentials remain outside evidence catalogs entirely |

## Owner Layout

Twenty canonical ignored owner roots now exist. Fifteen are research owners: F01 through F10, the four concrete shared layers, and system engineering. Five are cross-project or component owners: `docs`, `live`, `data`, `models`, and `execution`. Namespace-only containers do not receive duplicate private directories; their evidence is owned by one concrete unit so that locators cannot drift across copies.

Every owner root has a do-not-publish marker and a machine-readable catalog, is ignored by Git, and uses restrictive local permissions. Cross-unit consumers use the owning unit plus a stable artifact ID rather than copying a private path. The public layout is defined by [Public Research and Private Evidence Layout](../research/PRIVATE_EVIDENCE.md), [Non-Research Private Evidence Owners](non_research_private_evidence_owners.md), and the [Public and Private Documentation and Evidence Contract](public_private_documentation_contract.md).

## Public Projection and Exact-Source Separation

Public JSON records that contained machine paths or process identifiers were converted into sanitized projections. Their exact prior bytes were copied without modification into the applicable ignored owner store before redaction. Projection-aware consumers verify the frozen executed-source SHA separately from the current public-projection SHA and, when owner-side reproduction is authorized, read the exact private source; they do not pretend that presentation bytes equal execution bytes.

Records below Git-ignored model bundle directories are not described as public repository artifacts. Their sanitized working-tree identities were removed from the public projection manifest and placed in the ignored model-owner index with availability `private_working_tree_projection_not_distributed`.

The two legacy research-layout archives were removed from the tracked public tree because their frozen members contained pre-contract owner-specific locators. Their exact hashes and sizes remain public in [Legacy Research Snapshot Availability](../research/governance/archive/README.md), while the exact archives are retained by the experiment-governance private owner and are hash-verified before historical reproduction. A public checkout without the archive fails closed.

## F05 Example Requested by the Owner

The public [`multiscale_ema_add_wait_incremental_value_v1_1` Spec](../research/families/f05_fill_quality_quote_ev/docs/multiscale_ema_add_wait_incremental_value_v1_1_spec_20260809.json) remains on GitHub because its research contract is public. Its current public-projection SHA256 is `0cb38c77fadcc9cc1748344c5915e450e3d47a24f117a76e4a9aa3a00940ddab`. The exact executed-source SHA256 is `b59f9f5a3c9cbdd1fa714abe6ddf8ef23e19654374c354a6840e6f943a7c6908`, its availability is `private_not_distributed`, and the exact bytes are registered by the F05 private owner as a historical executed Spec.

The public Spec now labels denominator, runtime config, historical pointer, and execution-plan identities explicitly. The frozen historical operational pointer SHA is retained, but its exact bytes could not be recovered from the governed evidence stores; the Spec therefore records `missing_from_governed_private_evidence_store` and forbids substituting the current mutable pointer. This is a deliberate fail-closed limitation, not a reason to rewrite the frozen identity.

## Implemented Controls

- Added the repository-wide public/private contract, owner maps, catalog schema, ignored directory rules, and skill instructions.
- Added public and private audits covering locators, host/cloud/process identifiers, structured JSON values, links, projection hashes, private-source hashes, archive members, owner uniqueness, permissions, panel roles, and read gates.
- Added allowlisted portable path resolution and replaced machine-specific source defaults with environment variables, owner-private pointers, data/cache helpers, or system temporary directories.
- Added projection-aware source helpers so active consumers distinguish executed/private source identity from public presentation identity.
- Preserved historical scientific identities; true code drift and missing historical bytes remain fail-closed instead of being “fixed” by rebinding to current files.
- Preserved the rule that Development code cannot read Validation or sealed-holdout evidence merely because a private locator exists.
- Kept `live/.env` and all credential material outside catalogs, hashes, notes, projections, and reports.

## Catalog Semantics

Bulk-migrated locator occurrences with null SHA or size are quarantine records only. They are not admitted evidence and retain `historical_or_operational_unspecified`, which is unreadable by automated Development, Validation, or holdout workflows. A semantic artifact is usable only when it has one owner, resolvable bytes, verified SHA256 and size, an explicit panel role, and an explicit read gate.

At the time of this receipt, the private audit covered 20 owner roots, 471 private files, 7,396 catalog entries, 34 semantic entries with verified bytes, 242 public projections with 242 verified exact private sources, and 160 non-published model projections with 160 verified working-tree files. The remaining 7,362 catalog rows are fail-closed migration/quarantine records rather than a claim that every legacy locator has been semantically admitted.

## Required Read Order for Another AI

1. Read the public unit README, public Spec/report, registry, and public governance contracts first.
2. Use a private catalog only when the user authorizes owner-side evidence access and only for the relevant unit.
3. Verify artifact SHA, size, panel role, and read gate before reading bytes; absence or mismatch fails closed.
4. Never copy a private locator, host, process ID, raw account state, or secret into a public answer or tracked file.
5. For a public projection, compare presentation bytes to `public_projection_sha256` and scientific execution identity to `source_private_sha256`; never interchange them.
6. Do not update a frozen historical SHA merely because current source changed. Resolve exact historical bytes or record an explicit unavailable/fail-closed state.
7. Do not treat a private catalog entry as research, action, or live authority.

## Validation Commands

```bash
.venv/bin/python scripts/audit_public_documentation.py --repo-root .
.venv/bin/python scripts/audit_private_evidence.py --repo-root .
.venv/bin/python -m pytest -q tests/test_audit_public_documentation.py tests/test_private_evidence_governance.py tests/test_public_machine_projection_identity.py
```

The public audit must end with zero findings. The private audit must verify every semantic catalog byte and every public/private projection pair. Presentation reflow must remain idempotent, and protected code, table, formula, front-matter, and explicit hard-break blocks must not change.

## Validation Result and Preserved Blockers

The final public audit passed with zero findings after scanning 656 public human/machine documents, 1,006 source files, 242 public projections, and 560 repository links. The final private audit passed with zero findings using the counts recorded above. The public Markdown reflow dry run scanned 308 files and reported zero changes, zero errors, zero manual-review items, and zero removable soft breaks. The governance-focused test set passed 77/77; source-locator cleanup also passed 102 runnable tests with 5 intentional skips, and the script-focused subset passed 33 tests with 1 deselection. Ruff, import-order checks, skill validation, and `git diff --check` passed for the governed surfaces.

The affected F05 suite passed 202 tests and retained 3 fail-closed failures. All three arise from one real execution-identity mismatch: the frozen `models/backtest_tick.py` SHA256 is `379daa3c31bd1261b7d755d11bff476a803836c3992306e6b130a3ea7b1c7f1b`, while the current source SHA256 is `d6bac9432e10d728723d29f556c87147ba1375bbb771d9ab8d51ad438e635f65`. This is substantive code drift rather than Markdown reflow or locator redaction, so the governance work deliberately did not rebind the frozen experiment. Several broader release/preflight tests also require historical temporary snapshots that no longer exist; those fixtures were not recreated or fabricated.

## Explicit Boundary

This receipt governs the current repository tree, not already-published Git history. Earlier commits may still contain paths or archive bytes that were once tracked. Rewriting or replacing public Git history is a separate destructive publication decision and was not authorized here. Ignored private directories are publication boundaries, not encryption or backup; owner-side backup and secret management remain separate responsibilities.
