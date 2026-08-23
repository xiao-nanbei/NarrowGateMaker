# Open Source Contributor Guide

Last materially modified: 2026-08-23

Status: Current public participation and navigation guide.

Use this page to find the shortest path to the work you want to do. NarrowGateMaker
contains public software, public research contracts, and references to owner-side
evidence that is not distributed with the repository. Those surfaces have different
review rules.

## Navigate by Goal

| I want to... | Go to... |
| --- | --- |
| Understand and run the public project | [README](../../README.md), [Chinese README](../../README.zh-CN.md), and [Developer Checks](../dev/ci.md) |
| Take one UTC day from download to diagnostic replay | [One-day data pipeline](one_day_data_pipeline.md) |
| Understand permitted use | [License](../../LICENSE) |
| Report a code defect | [Bug report form](../../.github/ISSUE_TEMPLATE/bug_report.yml) and [contribution workflow](../../CONTRIBUTING.md) |
| Propose a focused feature | [Feature request form](../../.github/ISSUE_TEMPLATE/feature_request.yml) |
| Improve public docs | [Documentation report form](../../.github/ISSUE_TEMPLATE/documentation.yml) and [public documentation contract](../public_private_documentation_contract.md) |
| Propose or publish research evidence | [Research evidence PR rules](research_contributions.md) and [research evidence form](../../.github/ISSUE_TEMPLATE/research_evidence.yml) |
| Understand versions, tags, attempts, manifests, and receipts | [Source, research, and execution identities](identity_and_release.md) |
| Configure merge protection | [Required checks and branch protection](../dev/branch_protection.md) |
| Decode project vocabulary | [Glossary](glossary.md) |
| Report a vulnerability | [Security policy](../../SECURITY.md) |
| Ask about commercial authorization | [License](../../LICENSE) and [commercial-license form](../../.github/ISSUE_TEMPLATE/commercial_license.yml) |

The live GitHub forms are available through the
[issue chooser](https://github.com/xiao-nanbei/NarrowGateMaker/issues/new/choose).

## Public Clone Boundary

A public clone is expected to support code review, synthetic tests, public
documentation checks, and the public portions of research inspection. It does not
include owner-side datasets, evidence stores, model bundles, live configuration,
credentials, private host locators, or raw account state.

Do not fill those gaps by inventing paths, downloading unapproved substitutes, or
rebinding a frozen identity to current bytes. Missing private evidence remains
explicitly unavailable and fail-closed. Read [Path Conventions](../path_conventions.md)
and the [public/private documentation contract](../public_private_documentation_contract.md)
before publishing artifacts or commands.

## Contact Boundaries

The repository publishes no dedicated security or commercial-licensing email.
Security details belong only in the
[private vulnerability reporting form](https://github.com/xiao-nanbei/NarrowGateMaker/security/advisories/new).
When that form is unavailable, a public issue may request a private channel but must
not disclose the vulnerability. Commercial licensing questions may use the public
issue form, but the issue itself is neither confidential nor authorization.
