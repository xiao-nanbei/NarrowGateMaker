# Source, Research, and Execution Identities

[English](identity_and_release.md) | [简体中文](identity_and_release.zh-CN.md)

Last materially modified: 2026-09-26

Last materially synchronized: 2026-09-26

Status: Current public naming and provenance guide.

Historical event: on 2026-09-13 the owner authorized history consolidation and removal of then-existing Git tags under `version-alpha-20260913`. Current `main` has a subsequent commit chain; retaining one mutable root is not a current requirement. Ordinary work follows current AGENTS and task permissions and is saved as local commits; pushing, tagging, merging and history rewriting are not automatically authorized. Python/C++ package version `0.1.2.dev0` does not uniquely identify execution: record exact commit/tree separately from external inputs/configuration.

Private recovery material retains earlier Git identities and is not distributed. Historical references must not be rebound to today's commit; frozen experiments retain their identities. The layers below explain evidence; historical tag workflows do not require ordinary documentation edits to rewrite Git history or create tags.

NarrowGateMaker uses several identities because source publication, scientific
questions, execution attempts, and result bytes answer different audit questions.
Do not collapse them into one version number or tag.

## Identity Layers

| Identity | What it names | What it does not prove |
| --- | --- | --- |
| Git commit and tree | Exact tracked public source | External data, runtime configuration, result bytes, or authority |
| Version or stability tag | A public source release or maintained stability milestone | A new research question or a completed formal run |
| Research identity or research `vXX` | One frozen sample, baseline/candidate ladder, folds, estimand, and statistical contract | A particular executor repair or run |
| Execution attempt ID | One admitted run in the `attempt-*` namespace | A package version, scientific-contract change, or successful result |
| Annotated execution tag | The exact clean source admitted for an attempt | Input bytes, completion, economic validity, action authority, or live authority |
| Pre-run attempt manifest | The binding among research contract, source, artifacts, runtime, cache, schema, and permissions | Result completion or permission beyond its explicit fields |
| Final or failure receipt | Immutable completion or failure bound back to the pre-run manifest | Authority not explicitly granted by separate governance |
| Artifact SHA256 | Exact bytes of a named artifact | Public availability or a location from which to obtain it |

## Normal Formal Chain (When Explicitly Authorized)

```text
development branch
-> stability gates
-> clean commit
-> annotated execution tag
-> SHA-bound pre-run manifest
-> final receipt
```

The final receipt records result artifact hashes and binds them to the immutable
pre-run manifest. A failure uses a failed-attempt receipt instead. The manifest is
never edited after the run to make the result fit.

## When an Identity Changes

| Change | Research identity | Execution attempt |
| --- | --- | --- |
| Fix an implementation bug or crash | Keep | New attempt after all gates pass |
| Repair cache, concurrency, resume, serialization, or performance behavior | Keep | New attempt after all gates pass |
| Change only public explanation without changing execution or conclusion | Keep | No attempt required |
| Change sample, baseline or candidate ladder, folds, estimand, or statistics | New | New attempt under the new identity |
| Rerun identical source and contract after an admitted infrastructure failure | Keep | New attempt with its own manifest and receipt |

Negative or inconclusive evidence is not a reason to mint a new research identity.
Neither is the desire for a cleaner version number.

## Tag Discipline (When Explicitly Authorized)

A version or stability tag and a research execution tag serve different readers.
The former names a source-release milestone. The latter is an annotated provenance
object for one admitted attempt. An execution attempt ID uses `attempt-*`; it must
not be disguised as `formal-vXX` or another research version.

When contribution discussion says "research attempt tag," it means that annotated
execution tag bound to one `attempt-*` manifest. The manifest carries the canonical
attempt ID. A release or stability tag must not be reused as a substitute for either
identity.

For formal execution:

- tag only the exact tested clean commit;
- use an annotated tag and bind it in the pre-run manifest;
- never move, replace, delete, or reuse the tag after execution;
- give a repaired run a new attempt ID, manifest, annotated tag, and receipt;
- state that action and live authority are false unless separate governance says
  otherwise.

## Authority Boundary

No identity object grants more than it explicitly says. In particular:

- a stable or release tag does not validate research;
- an attempt tag or manifest does not prove completion;
- a final receipt does not by itself authorize an action or deployment;
- a research result does not silently become the current live baseline;
- a SHA does not prove that public readers can access the bytes.

Use explicit permission fields and the current public governance documents rather
than inferring authority from a filename, date, version, tag, or successful check.
