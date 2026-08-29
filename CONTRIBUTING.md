# Contributing to NarrowGateMaker

<p><a href="CONTRIBUTING.md">English</a> | <a href="CONTRIBUTING.zh-CN.md">简体中文</a></p>

Last materially modified: 2026-08-29

Last materially synchronized: 2026-08-29

Thank you for helping improve NarrowGateMaker. This repository combines ordinary
software engineering with evidence-governed market-making research, so the review
path depends on what a change claims.

Start with the [goal-oriented contributor guide](docs/opensource/README.md). This is
a source-available project under the [PolyForm Noncommercial License](LICENSE),
which permits the noncommercial purposes stated in its terms and restricts
commercial use. A pull request does not grant commercial-use permission.

## Choose the Right Path

| Goal | Start here |
| --- | --- |
| Report a reproducible code bug | Use the GitHub bug report form |
| Propose a feature or mechanics change | Use the GitHub feature request form |
| Correct or clarify public documentation | Use the documentation report form |
| Propose or publish research evidence | Read the [research evidence PR rules](docs/opensource/research_contributions.md), then use the research evidence form |
| Report a vulnerability | Follow [SECURITY.md](SECURITY.md); do not open a detailed public issue |
| Ask about commercial authorization | Use the public commercial-license issue form; no dedicated licensing email is published |

For a substantial change, open an issue before investing in an implementation.
Keep one pull request focused on one reviewable goal.

## Development Workflow

1. Branch from the current maintained public branch.
2. Make the smallest change that resolves the stated problem.
3. Add or update tests in proportion to the behavioral risk.
4. Run the relevant checks from [Developer Checks](docs/dev/ci.md); maintainers also preserve the exact [branch-protection check names](docs/dev/branch_protection.md).
5. Inspect the complete diff for privacy, evidence, and authority claims.
6. Open a pull request using the repository template.

Ordinary code, build, test, and documentation changes follow this workflow. A bug
fix does not become a new research result merely because it affects a research
executor.

## Research Identity and Formal Execution

Research identity and execution identity are separate. A research identity changes
only when at least one frozen scientific-contract element changes:

- sample;
- baseline or candidate ladder;
- folds;
- estimand;
- statistical contract.

An implementation bug, crash, cache mismatch, concurrency race, serialization
error, or performance repair stays under the same research identity. It must not be
renamed as a new research `vXX`. It creates a new execution attempt after the repair
passes the stability gates.

The formal sequence is:

```text
development branch
-> stability gates
-> exact tested clean commit
-> annotated execution tag
-> SHA-bound pre-run manifest with an attempt-* identity
-> formal execution
-> immutable final receipt binding result hashes to the pre-run manifest
```

The stability gates cover representative single-day output, an all-fold
zero-economic walk, concurrency and cache durability, regression and parity,
complete output-shape smoke, and a clean worktree. The intended worker topology,
resource lifetime, interruption/resume behavior, atomic cache replacement,
aggregation, scorecard generation, and receipt serialization must be exercised
before economic values are read.

A failed formal run receives an immutable failed-attempt receipt and is ineligible
for economic inference. Repair it on the development line, repeat every stability
gate, and issue a new `attempt-*` identity. Do not move, rewrite, or delete the
failed tag, manifest, or receipt.

See the [formal execution contract](research/shared/experiment_governance/docs/formal_execution_attempt_and_evidence_freeze_contract_v1_20260821.md)
and the [identity guide](docs/opensource/identity_and_release.md) for the full
boundary.

## Public Contribution Boundary

Do not commit or paste any of the following into an issue, pull request, test
fixture, screenshot, log, or document:

- personal absolute paths or physical storage locations;
- private hostnames, IP addresses, SSH targets, cloud or account identifiers, or
  process identifiers;
- credentials, tokens, environment files, signing material, or secret-bearing
  configuration;
- private datasets, licensed source bytes, owner-side evidence, raw live account
  state, positions, orders, or fills;
- generated reports that disclose a private locator even when the report itself is
  otherwise harmless.

Use the placeholders from [Path Conventions](docs/path_conventions.md). A SHA256 is
verification metadata, not a downloadable location. Every referenced artifact must
be linked to public bytes or explicitly marked with its honest availability, such
as `private_not_distributed`. Never invent a link or substitute current bytes for a
missing frozen artifact.

Public research material must stand on its own without access to an owner's machine.
Read the [public/private documentation contract](docs/public_private_documentation_contract.md)
and [public research evidence layout](research/PRIVATE_EVIDENCE.md) before adding an
evidence artifact.

## Pull Request Expectations

Every pull request should state:

- the problem and the behavioral change;
- the files and authority surfaces affected;
- tests and public documentation audits run;
- whether the change is ordinary engineering, a new execution attempt under an
  unchanged research identity, or a changed scientific contract;
- artifact availability and evidence permissions, when research is involved;
- any remaining limitation or intentionally unavailable private dependency.

Research evidence pull requests have additional requirements in the
[research evidence guide](docs/opensource/research_contributions.md). Neither a
merged pull request nor a passing research run grants action or live authority.

## Documentation Audit

For any public documentation or machine-readable record change, run:

```bash
.venv/bin/python scripts/audit_public_documentation.py --repo-root .
git diff --check
```

The public audit must report zero findings. Public contributors are not expected or
authorized to inspect the owner-private evidence store.
