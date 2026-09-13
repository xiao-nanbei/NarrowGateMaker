# Scoped publication protocol

Last materially modified: 2026-09-20

Read only for commit/push/tag/release work. This preserves owner instructions, not standing permission to replay old destructive operations.

## Current alpha rule

Until the owner ends alpha, the existing instruction retains one root commit named `version-alpha-20260913` on intended NarrowGate main branches. Authorized completed changes amend it; preserve unrelated dirty bytes. Verify exact remote tips, retain private recovery and use expected-SHA force-with-lease only for authorized rewrites. Concurrent changes/protection failures stop publication; no blind force or bypass.

The initial squash/tag deletion/branch deletion were one-time historical operations. Do not repeat them from this document, delete later tags/branches or scoop up future dirty files. Verify state and retain uncertainty. Automatic improvement/execution tags are paused during alpha; after alpha use the owner-approved replacement protocol rather than silently reviving an old rule.

## Destination and scope

Verify actual visibility each time. Neither remote `private` nor suffix `-private` proves private access. Under the owner's 2026-09-19 instruction, ordinary source publication does not require the owner-wide private evidence audit, regardless of destination visibility. Follow the [public/private policy](../../../../docs/public_private_documentation_contract.md) for the actual published tree: inspect credentials, private locators, purchased data and links. Private catalog, permissions or read-gate findings outside the published content do not block source updates. This is not permission to publish private evidence or to claim its validation passed.

Publish authorized reviewed task-owned source/tests/docs only. No credentials, purchased data, models, sealed results, owner skills or private locators. Preserve unrelated edits. Report local changes, commit, each push and remote ref separately; one push does not synchronize both repos.

Research source identity need not be public: a local/private commit or retained immutable snapshot plus bound authorized overlay is valid. Public audit findings block public publication, not unrelated offline work under its own valid contract. Identity is not statistical/economic validation.

Save completed authorized source changes with a local amend before handoff. A commit is not push authorization: push only on an explicit owner request for the current task. Earlier standing automatic-publication instructions and background continuations do not authorize future pushes. Without a new push request, report the local commit and that remotes are unchanged. If explicitly requested publication is blocked, preserve work and report the precise blocker; do not widen the task just to clear unrelated findings.
