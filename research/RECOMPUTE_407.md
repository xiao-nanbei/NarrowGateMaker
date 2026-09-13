# 407-day research implementation plan

2026-09-22 update: F03 completed all 858 strategy-shards; reuse its verified products rather than restarting package 5. See the [full results](families/f03_causal_13_head/README.md). Under explicit owner publication authorization, the existing generated F03 blog page and its indexes are synchronized; no authoring source has been located, so a future site rebuild must retain these changes. This scoped exception does not publish or complete other research packages.

[English](RECOMPUTE_407.md) | [简体中文](RECOMPUTE_407.zh-CN.md)

Last materially modified: 2026-09-22

Last materially synchronized: 2026-09-22

This is a question-oriented implementation plan, not a completed recomputation report. The machine-readable work list is [recompute_407.json](recompute_407.json). Historical closed, exhausted and promotion states describe their original identities and do not block new studies. Source, actual outcome-end, accounting and previous-use checks still apply.

## Inputs and the running F03 experiment

Use the 407 UTC days from 2025-08-01 through 2026-09-11 in [dataset_scope.json](../data/dataset_scope.json). BTCUSDC is execution and BTCUSDT is optional reference; the current F03 uses BTCUSDC only. Real funding is separate accounting input. Missing three-venue, spot, currency-bridge or own-order telemetry limits the corresponding subquestions; do not fabricate replacements.

Preserve current new-source F03 frozen artifacts and independent accounts; do not switch its running source. Four complete 13-head bundles use H=inf/240/120/60; training and early stopping stay inside T100, B100 selects one whole bundle, A50/C50 are diagnostic and F107 follows frozen selection. The later explicit ML-OFF approval supersedes the older no-control plan: 654 original plus 204 control strategy-shards total 858; ML-OFF is not a B selection candidate. The 52 final model files do not mean only 52 fitting calls. New features or targets need separate comparisons, not silent changes to the half-life experiment.

## Packages ordered by estimated incremental computation

This order is a budget estimate, not measured wall time. Freeze candidate budgets before execution; a null budget does not authorize unlimited compute. Shared facts, features and reference trajectories are counted once.

| Package | Families | Scientific question |
|---|---|---|
| 1 | F10/SYS | Accounting, clocks and attribution |
| 2 | F08 | Side-taker flow and hazard |
| 3 | F02 | Static P3 touch |
| 4 | F04 | Cross-market prediction |
| 5 | F03 | 13-head and time weighting |
| 6 | F02 | Conditional P3 and reach-time |
| 7 | F05 | Opportunity Quote-EV prediction |
| 8 | F09/F04/F05 | Fixed action comparisons |
| 9 | F01 | Fixed-parameter racing |
| 10 | F06 | Placement CIF and marginal value |
| 11 | F07/F05 | KEEP/CANCEL/REENTER and E/C |
| 12 | F05/F09 | Multiscale and duration policies |

## Execution dependencies and acceptance

Validate shared inputs, bounded-memory readers and execution/accounting first, then side-flow, static P3 and dual-market features. Reuse valid new F03 trajectories for attribution, opportunities and later action studies; changed actions require new order, inventory and risk paths. Placement and continuation share read-only events and complete-state forks; multiscale feature candidates reuse identical action/endpoint rewards rather than repeating forks.

Fork state includes exchange/local orders, pending requests, cash/inventory, queue estimates, feature/model/cooldown state, RNG and market cursors. Compare small samples with replay from the beginning; incomplete state is not checkpoint equivalence. Measure actual events, wall/CPU time, peak RSS, I/O and future fork-event counts without selecting benchmark dates by PnL.

Each package records questions, inputs, candidates, units, splits, parents, outputs and missing denominators. Distinguish not run, failed execution, missing input, valid negative and valid positive results. Interface smoke is not research completion; missing funding is not zero and terminal MTM is not free liquidation. Public-volume eligibility assumptions do not establish native/live parity.

## Documentation and blog

Current family READMEs point to new questions and actual implementation boundaries; old numbers do not populate new result tables. A single private Git bundle preserves the pre-rewrite source/document tree (private evidence store; not distributed with the public repository). Historical bodies temporarily remain read-only pending question-level consolidation, not current statuses. Preserve defect regression tests instead of rerunning erroneous economics version by version.

Consolidate the blog into nine topics: A0 overview, A1 inputs/accounting, A2 side flow, A3 P3, A4 13-head, A5 cross-market, A6 order value, A7 parameters/actions and A8 multiscale. Writing sources are not yet located; do not edit or publish generated HTML. Later build from actual sources and synchronize categories, tags, archives, search, images and enabled feed/sitemap. Write conclusions only after real results. This change does not publish the blog or push source.
