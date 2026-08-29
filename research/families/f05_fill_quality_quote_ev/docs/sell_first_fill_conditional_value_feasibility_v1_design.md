# SELL First-Fill Conditional Value Feasibility v1 - Design Draft

Last materially modified: 2026-07-30

Status: `development_contract_frozen_before_formal_native_artifact`. The machine-readable Spec is [`sell_first_fill_conditional_value_feasibility_v1_spec_20260730.json`](../../f10_live_replay_attribution/docs/sell_first_fill_conditional_value_feasibility_v1_spec_20260730.json). This remains prediction feasibility only, not an action family or permission to read Validation/holdout.

## Motivation

A private historical 120-hour observational diagnostic motivated this question. That result is not distributed, remains hypothesis-generating only, and cannot be reused as training or confirmatory evidence or define a score threshold.

This question is distinct from the closed first-add model:

- the prior F05 identity studied the first exposure-increasing **add** decision;
- this draft studies the first filled **SELL opener** path that creates a SHORT campaign.

The previous failed first-add risk score and post-result feature bins are not eligible inputs.

## Proposed Evidence Estimand

The principal economic target is direct post-decision campaign value, with a day-end MTM censor when the campaign has not flattened:

\[
Y_{\rm sell\ opener}
=
V(T_{\rm flat\ or\ day-end\ MTM})-V(t_{\rm opener\ submit})
\quad[\mathrm{USDC/decision}].
\]

Approximate 10-second markout is a mechanism slice only. It cannot replace decision-to-terminal value. This v1 evidence retains the exact decision-to-order-to-fill-to-campaign lifecycle join, but is explicitly conditional on a submit-time opener order producing the campaign-opening fill. It does **not** include unfilled, GTX-rejected, or cancelled opener opportunities and therefore cannot establish operational quote value.

The recent live report classified `opener` from inventory immediately before fill. That is a broader fill-time role. This identity additionally requires the exact order to have been submitted while flat with submit role `opener`. Any old add/reducing order that survives across a flat boundary and later reopens inventory is an unsupported lifecycle path, not a row that may be silently relabelled.

## Frozen Development Decisions

The frozen identity uses 24 Grade-A Development days as primary and 16 Grade-B days as separately fitted sensitivity evidence. SELL is primary; BUY is a negative control and cannot rescue SELL through pooling. The baseline is a past-only intercept because campaign state at a true flat opener boundary is mostly constant. The incremental model is a low-freedom standardized Ridge on the frozen submit-time local features, with expanding past-only OOF folds and day/campaign-clustered simultaneous intervals.

Unfilled/cancelled/GTX opener opportunities are deferred to a distinct future operational denominator identity. They cannot be added to v1 after outcomes are read. The recent 120-hour live window remains hypothesis generation only and may not tune features, thresholds, gates, or model complexity.

## Closure And Downstream Boundary

If decision-visible local features do not identify a cross-day stable negative SELL-opener subset with a simultaneous value bound below zero, close the local prediction branch. Do not rescue it with the live-window age split, the failed F05 first-add score, or F04 external features selected after seeing outcomes.

If prediction evidence eventually passes under a new frozen identity, it still does not authorize an action. A distinct F09 intervention, known propensity, full-path replay, and lineage/campaign outcome contract are required.
