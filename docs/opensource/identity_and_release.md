# Source, Research, and Execution Identities

[English](identity_and_release.md) | [简体中文](identity_and_release.zh-CN.md)

Last materially modified: 2026-09-13

Last materially synchronized: 2026-09-13

Status: Current public naming and provenance guide.

Current source milestone: `version-alpha-20260913`, a single mutable root commit on public and private `main`. The owner authorized consolidating history and removing all existing Git tags on 2026-09-13. Until the owner declares the version stable and permits a second commit, updates amend this root and do not automatically create release, improvement or execution tags. The Python and C++ package version remains `0.1.2.dev0`; record the exact commit/tree, not the package version or mutable branch name alone.

This alpha policy supersedes the normal tag workflow below during stabilization. A private local recovery bundle retains prior Git identities; it is not distributed with the repository. Dated historical tag/commit references are not rebound to alpha and may no longer resolve in a fresh clone. This rewrite is source maintenance, not a rerun, research admission or live deployment. Already running or frozen experiments keep their recorded source identities.

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

## Normal Formal Chain Outside Mutable Alpha

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

## Tag Discipline Outside Mutable Alpha

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
