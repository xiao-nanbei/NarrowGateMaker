# Scoped publication protocol

Last materially modified: 2026-10-01

Read for commit, push and branch cleanup. This is the current publication authority; retired instruction snapshots are historical evidence, not competing defaults.

## Single-root code publication and branch cleanup

The owner explicitly renewed the rule on 2026-10-01: NarrowGate code repositories must always publish exactly one root commit on `main`. Each delivery replaces that root with the complete reviewed source tree; do not append a release commit or preserve a chain of per-task commits on `main`. Push the same root commit to the public and private established remotes after validation, unless a later task explicitly forbids publication. Local intermediate commits may aid development but must be consolidated before delivery. Do not include unrelated unfinished working-tree changes.

Before rewriting, inspect all worktrees and live remote refs, retain a private recoverable Git bundle, and record the exact old tips and new tree. Use explicit `--force-with-lease=refs/heads/main:<observed-tip>` separately for each destination, never blind force. Stop on a moved remote tip, divergence containing unreviewed work, or protection rejection; do not bypass protection. Verify both remote tips and that `git rev-list --count main` is one. A partial push is not synchronization; report each destination separately.

Before losing ancestry through consolidation, identify integrated development branches using ancestry or verified patch/tree equivalence. After successful publication delete those branches locally and on every remote where they exist, with expected-tip protection for remote deletion. Do not infer integration merely from age or branch name. Preserve unmerged unique work and report it. For a branch checked out in another worktree, preserve all working-tree bytes and active-job dependencies; safely detach its unchanged commit if appropriate before deleting its branch reference, without deleting the worktree. Do not delete evidence tags or immutable research artifacts under branch cleanup; automatic research tags remain paused unless separately authorized.

The blog repository has a separate explicit invariant: only `main` may exist locally or remotely. Work directly on `main`; consolidate any existing secondary branch's unique reviewed content into `main`, verify publication, then delete that secondary branch. Do not discard unmerged blog work to satisfy the branch count. The code single-root history rule does not by itself authorize rewriting the blog's entire historical commit chain. Blog source build, generated output and actual Pages deployment must be checked and reported separately.

Mutable `main` is not immutable research evidence. Preserve frozen commit/tree identities and bindings in private recovery/evidence storage; do not rewrite old run records to claim they used the new publication root.

## Destination and scope

Verify actual visibility each time. Neither remote `private` nor suffix `-private` proves private access. Under the owner's 2026-09-19 instruction, ordinary source publication does not require the owner-wide private evidence audit, regardless of destination visibility. Follow the [public/private policy](../../../../docs/public_private_documentation_contract.md) for the actual published tree: inspect credentials, private locators, purchased data and links. Private catalog, permissions or read-gate findings outside the published content do not block source updates. This is not permission to publish private evidence or to claim its validation passed.

Publish authorized reviewed task-owned source/tests/docs only. No credentials, purchased data, models, sealed results, owner skills or private locators. Preserve unrelated edits. Report local changes, commit, each push and remote ref separately; one push does not synchronize both repos.

Research source identity need not be public: a local/private commit or retained immutable snapshot plus bound authorized overlay is valid. Public audit findings block public publication, not unrelated offline work under its own valid contract. Identity is not statistical/economic validation.

The 2026-10-01 single-root and integrated-branch rules replace the former ordinary-commit/no-history-rewrite default. Publication is expected at each completed delivery, not deferred merely because an earlier task ended with local commits when the owner now explicitly authorizes consolidating and publishing the current tree. A subsequent explicit no-push request still takes precedence. Do not silently resolve concurrent work or widen a source publication into deployment.

Do not proactively dispatch, rerun or enable hosted CI. For ordinary commits use `[skip ci]` when the existing push/pull-request workflows honor it, retaining proportionate local validation and reporting hosted CI as skipped, not passed. Inspect actual triggers before pushing: skip markers do not disable scheduled or other independent workflows. Do not alter repository settings or remove workflows merely to avoid costs. Site publication is distinct from test CI; report automatic Pages publication separately and do not claim a push proves the site works. If an unavoidable paid workflow would run, report that concrete limitation before the push instead of promising no cost.
