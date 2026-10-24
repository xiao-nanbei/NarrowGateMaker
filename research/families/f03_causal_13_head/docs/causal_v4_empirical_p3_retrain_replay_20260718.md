# Causal v4 empirical-P3 retrain and strict replay — current research question

Last materially modified: 2026-10-17

This public reading page replaces the old result narrative, tables and closure verdict; it is not the frozen original report or a rerun. Original experiment records and prior source history retain their own identities. Underlying detailed evidence is private and not distributed with the public repository.

## Question and scope

**F03-CADENCE-TRANSPORT — pending current-source validation.** Does a change in feature/decision cadence improve value under matched current-source execution?

Study-specific historical topic: `causal_v4_empirical_p3_retrain_replay_20260718`. Its old dates, candidate selection and previous-use are provenance, not a fresh current-source experiment. Old numerical gains/losses, probability metrics and promotion/closure labels are not reproduced as current evidence.

## Method to reconstruct

Hold model support, visibility, latency and accounting fixed where the estimand requires it; compare full accounts and count effective actions, not merely inference calls.

## Current evidence that may be used

The accepted new-source f03-407-t100-final107-v2 comparison completed 858 strategy shards. On 54 independent Final accounts covering 107 days per arm, selected Hinf net PnL was −189.212148 USDC and ML-OFF −269.042233 USDC. Both lose money, and Final has previous use. The later five-candidate comparison uses a different frozen B0; all five candidates underperform its total and both unit net returns. Neither result identifies a pure cadence effect.

See the [current family report](../README.md), [family question register](../research_questions.json) and [current recomputation ledger](../../../recompute_407.json). Results in those sources replace old current-facing claims only within their declared matching scope; this page does not assert that an unrelated question was retested.

## What remains unresolved

- The accepted new-source Hinf versus ML-OFF result answers a model-package comparison, not an isolated cadence effect.
- Bind the actual source, model, baseline, action semantics, accounting and previous-use; an old point estimate or closure label is not a current conclusion.

Engineering correctness, regression and restoration checks remain in their implementation/specification documents. A historical defect must not be forgotten, but its repair does not prove an economic result. This record neither launches a new experiment nor changes a running release.
