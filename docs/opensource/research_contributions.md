# Research Evidence Pull Requests

Last materially modified: 2026-08-23

Status: Current public contribution rules for research contracts, formal execution,
and evidence publication.

This guide applies when a pull request adds or changes a research question, frozen
protocol, formal execution identity, result, failure receipt, or promotion claim.
Ordinary code and documentation fixes still follow [CONTRIBUTING.md](../../CONTRIBUTING.md).

## Classify the Change Before Reading Outcomes

A research identity names one scientific question. It changes only when the frozen
sample, baseline or candidate ladder, folds, estimand, or statistical contract
changes. Declare every changed element before inspecting restricted outcomes.

The following are execution changes under the same research identity:

- bug and correctness fixes;
- crashes, concurrency races, cache mismatches, or serialization repairs;
- executor performance work;
- resource-lifetime, interruption, resume, or output-writing repairs;
- presentation-only cleanup that does not change executed bytes or conclusions.

These changes require a new execution attempt when rerun, but they do not justify a
new research `vXX`. See [Source, Research, and Execution Identities](identity_and_release.md).

## Formal Sequence

Formal evidence must follow this order:

```text
development branch
-> representative stability gates without reading economics
-> exact tested clean commit
-> annotated execution tag
-> immutable SHA-bound pre-run manifest with attempt-* identity
-> formal run
-> immutable final receipt binding result hashes to that manifest
```

The pre-run stability gates are:

1. representative single-day end-to-end output;
2. an all-fold zero-economic walk;
3. concurrency and cache durability;
4. regression and implementation parity;
5. complete output-shape smoke, including scorecard and receipt serialization;
6. a clean worktree for the exact source to be admitted.

The checks must use the intended worker topology and cover mmap or other resource
lifetime, interruption and resume, atomic cache replacement, result aggregation,
and complete serialization. Intermediate economic values must remain unread.

The manifest binds the unchanged research contract, clean public commit and tree,
annotated tag, source artifacts, runtime configuration, cache namespace, output
schema, and permissions. Git identifies tracked source; artifact SHA256 values
identify external bytes. Neither substitutes for the other.

The canonical contract and executable checks are the
[Formal Execution Attempt and Evidence Freeze Contract](../../research/shared/experiment_governance/docs/formal_execution_attempt_and_evidence_freeze_contract_v1_20260821.md)
and [`formal_evidence_governance.py`](../../models/audit/formal_evidence_governance.py).

## Failed Attempts

An implementation failure must produce an immutable failed-attempt receipt. The
attempt cannot support economic inference. Return the repair to a development
branch, repeat all stability gates, and create a new `attempt-*` manifest only after
they pass.

Do not delete, rename, move, or rewrite historical failure tags, manifests, or
receipts. Do not import partial strategy-dependent caches or partial economic output
unless a separately validated cache contract proves exact semantic identity.

## Evidence PR Contents

A research evidence pull request must make the following reviewable from public
files:

- scientific question and research identity;
- frozen sample, source authority, baseline and candidate ladder, folds, estimand,
  statistical method, gates, and stop rules;
- chronological Development, Validation, and sealed-holdout roles and read
  permissions;
- execution attempt ID, exact clean public commit, annotated execution tag, and
  SHA-bound pre-run manifest;
- final receipt or failed-attempt receipt, without mutating the pre-run manifest;
- named result artifacts, hashes, and honest availability;
- aggregate results, limitations, decision, and supersession status in ordinary
  language;
- explicit prediction, action, deployment, and live authority values;
- reproducible public or synthetic checks, plus an honest statement for any
  unavailable private dependency.

A public report must remain understandable without the owner's machine. If artifact
bytes are not distributed, label them `private_not_distributed`; do not provide a
machine locator or pretend that a hash is a download link. Follow [Path Conventions](../path_conventions.md),
the [public/private documentation contract](../public_private_documentation_contract.md),
and the [public research evidence layout](../../research/PRIVATE_EVIDENCE.md).

## Prohibited Evidence Practices

- Do not submit private paths, hosts, addresses, account identifiers, credentials,
  datasets, raw live positions, orders, fills, or owner-side evidence.
- Do not read Validation or sealed-holdout outcomes without the frozen gate granting
  access.
- Do not change a split, gate, estimand, or candidate family after reading its
  outcome and retain the old research identity.
- Do not turn an ordinary executor repair into a research `vXX`.
- Do not rebind a frozen SHA to a convenient current file or fabricate a missing
  artifact link.
- Do not use a failed or partial attempt for economic inference.
- Do not describe presentation cleanup as a rerun.
- Do not treat correlation, prediction quality, or shadow output as action uplift.
- Do not infer action or live authority from a merged PR, passing test, final
  receipt, or positive point estimate.

## Review Result

Code acceptance and evidence acceptance are separate. Review may merge a reusable
correctness fix while rejecting the attached scientific claim, or preserve a
negative result while declining promotion. A result remains bounded by its frozen
contract, evidence split, source authority, and explicit permissions.
