# Full-Multiscale Boolean Cooldown Successor V1

Last materially modified: 2026-08-13

Status: `withdrawn_before_deployment_no_economic_read`; the companion was never deployed, emitted zero rows, changed no live runtime, and grants no collection, action, or live authority.

This document preserves the withdrawn prospective design as historical evidence. It is not a pending collection plan; the successor research moved to the separate [offline V1 identity](causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_v1_spec_20260813.md).

## Research Question

On top of the exact active owner cooldown policy, do the complete ten-half-life EMA bank, cross direction, higher-order Boolean interactions, and genuine trade/depth state add sequential campaign value?

The research mapping is:

```text
causally visible market/order/campaign state
-> inner-train-only bounded Boolean discovery
-> cooldown duration policy
-> repeated sequential full-path replay
-> campaign-terminal USDC increment versus exact active owner policy
```

The primary contrast is:

\[
\Delta=Y(\text{new repeated policy})-Y(\text{exact current owner policy}).
\]

One-shot duration labels may be used only inside training folds to discover candidate rules. Outer evaluation and promotion use a repeated sequential policy in which every eligible exposure-increasing fill invokes the frozen fold policy. One-shot effects are never added together and called policy PnL.

## Evidence Boundary

The 40-day, 50-day, and 71-day panels are consumed Development evidence. They may be used only for implementation regression, mechanics checks, and historical interpretation. They cannot select a feature, EMA pair, predicate, threshold, duration, model complexity, candidate, or promotion decision for this identity. Existing Validation and sealed holdout remain unread.

The withdrawn design had frozen a preregistration cutoff of `2026-08-12T17:45:29Z` and a first eligible closed UTC day of `2026-08-13`. It proposed a first-30-day prospective panel, but the companion was withdrawn before deployment and no such denominator was collected. These dates and rules remain historical design provenance only; they do not block or define the offline successor.

The historical design would have admitted each day from separately bound lifecycle, market/source, and decision-telemetry manifests. Its companion producer, day emitter, admission CLI, and fold freezer have been removed and are not executable or authorized. The owner-artifact audit remains available as a pure local implementation check and reads no economic outcomes.

## Active Baseline

`B0_CURRENT_EXACT` is the byte-bound `causal_multichannel_window_boolean_cooldown_owner_policy_v1`, not `CONTROL_85N`. BUY remains the predecessor `CONTROL_85N`; the current SELL owner rules, 4s/16s readiness guard, 16s/256s state, campaign-age branch, durations, safety, and fallback behavior are reproduced exactly. The active runtime is not changed by this study.

The named private owner policy and predicate bundle have been checked through the production evaluator over all 27 SELL and 27 BUY three-valued predicate assignments. The implementation-parity receipt reports zero mismatches. This establishes the exact baseline implementation binding only; it is neither OOF nor economic evidence.

## Candidate Ladder

| Identity | Information and role |
| --- | --- |
| `B0_CURRENT_EXACT` | Exact active owner policy |
| `B1_CAMPAIGN_AGE_ONLY` | Campaign-age-only duration learner with the common safety/readiness contract |
| `B2_CAMPAIGN_PLUS_H16_H256` | Campaign age plus 16s/256s cross recency |
| `B3_CURRENT_SEMANTIC_EQUIVALENT` | Current tri-state semantics with 4s/16s declared as a readiness guard rather than an economic predicate |
| `E1_FULL_EMA_BANK` | All ten half-lives and all 45 fast/slow pairs eligible before inner-fold screening |
| `E2_DIRECTIONAL_EMA` | Golden/death direction, last-cross direction, age, persistence, distance, normalized distance, slope, curvature, convergence, and expansion |
| `E3_HIGHER_ORDER_BOOLEAN` | Ordered multi-rule AND/OR/NOT policies with genuinely reachable higher-order interactions |
| `M2_TRUE_INCREMENTAL` | Trade flow and depth added only after the best campaign/EMA representation |
| `ACTION_MATCHED_CONTROLS::<candidate>` | A separate deterministic action-rate- and duration-distribution-matched control for each of E1, E2, E3, and M2 |

The existing side-specific eight-duration vocabularies remain unchanged so that this successor tests information and policy structure rather than silently optimizing a new duration grid. BUY and SELL are learned, reported, and promoted separately. Reducing quotes remain unchanged.

## Feature Universe And Bounded Search

The EMA bank is (h\in\{0.5,1,2,4,8,16,32,64,128,256\}\) seconds, yielding all 45 ordered fast/slow pairs. E1/E2 predicates from every pair are eligible for every inner-fold search. M2 adds local causally visible trade/flow/depth channels only after the best EMA/campaign representation is frozen inside that inner fold.

The first implementation is explicitly a bounded search, not an exhaustive architecture closure. Each purged inner-train fold may economically screen at most 1,024 features from the full eligible universe, then fit per-action identified-only trees with depth at most 6, at most 32 leaves, at most 7 ordered rules, at most 16 OR clauses per rule, and at most 6 literals per clause. No outer-train-wide feature pool may be reused inside inner folds. Candidate compression and all support counts are reported per fold.

Unknown action targets remain missing. Each duration action is fit only on rows where both that action and the exact active control are identified. Replicating unsupported rows must not change a selected policy. Neutral-zero imputation is prohibited.

The exact active control is row-specific. On each discovery row, the baseline action is the duration selected by the byte-bound current owner policy for that same causal snapshot; candidate effects are not mechanically subtracted from a fixed `CONTROL_85N` column.

## Chronological Split

The first 30 admitted active UTC days are frozen in arrival order. Four expanding outer folds use days 1-10 to predict days 11-15, days 1-15 to predict days 16-20, days 1-20 to predict days 21-25, and days 1-25 to predict days 26-30. Each outer-train block uses three expanding inner folds with at least five prior active days. Observation-end-aware purge removes any assignment whose descendant order, inventory, queue, cooldown, or campaign state overlaps the test boundary. The exact day manifest and source hashes are frozen before economic access.

Every inner fold independently performs its feature census, support calculation, economic screening, complexity selection, and candidate freeze using only its purged inner-train rows. Outer-test outcomes are read once for the formal report and never feed back into the search.

## Lifecycle And Clocks

Every row carries exchange, receive, feature-ready, policy-decision, fill-visible, and terminal clocks; role; consecutive fill units; campaign; inventory; chosen rule; duration; coverage reason; and campaign-terminal value. A pre-activation GTX `-5022` is exact zero exposure only when the exchange rejection is positively recorded and ACK state is known. Transport timeout, response loss, or unknown ACK is censored or reconciled, never encoded as zero.

All stages use the same coverage vocabulary: eligible feature-ready action, ineligible event, warmup incomplete, feature stale, predicate unobserved, safety fallback, policy control, source unavailable, cache unavailable, binding invalid, lifecycle unidentified, exact pre-activation GTX zero, and unknown-ACK censoring.

## Inference And Selection

The frozen hierarchy first tests M0 versus exact control, then paired M1 minus M0, then paired M2 minus M1. The continuous comparator is oriented as `continuous - Boolean`: a positive simultaneous lower bound marks the Boolean representation as dominated and blocks a Boolean freeze. It is not treated as another positive value gate.

All candidate and feature-family comparisons use shared day and week-block simultaneous families. Terminal value, closed-campaign value, negative-terminal protection, q10, CVaR10, campaign MAE, repair rate, repair-time avoidance, censoring avoidance, inventory-time avoidance, and maximum-inventory avoidance receive simultaneous bounds. Every candidate also emits the frozen `action_alpha_v1` canonical scorecard; a null ranking score or any scorecard hard-gate failure blocks final refit. Reports include common row/campaign/day denominators, feature-ready active days, zero-difference days, action rate, duration mix, fill/role/consecutive-unit strata, leave-one- and leave-two-top-day sensitivity, selected pairs, pair inclusion frequency, adjacent-fold Jaccard, rule stability, and every unsupported or unobserved reason.

Out-of-fold evidence supports the learning algorithm's fold-specific policies. It never becomes exact evidence for the later full-Development refit artifact. A final refit may be frozen only after formal OOF; the exact artifact then requires later independent canonical evidence or an explicitly owner-authorized randomized live canary that does not silently alter current owner authority.

## Stop And Promotion Rules

E1 may be called incrementally valuable only if its paired simultaneous lower bound is above zero against both `B0_CURRENT_EXACT` and the appropriate simplified baselines, the effect survives action-matched controls and top-day exclusions, and rule/pair stability is reportable. M2 may be called incrementally valuable only if `M2_TRUE_INCREMENTAL - best EMA baseline` has a positive paired simultaneous lower bound and the compiled rules actually use trade/depth predicates.

Failure closes only this frozen bounded successor. It does not revoke the current owner operational baseline. Success does not directly replace live: the exact final artifact still needs later independent evidence, repeated full-path continuity, receive-time transport, implementation parity, and an explicit promotion decision.

The machine-readable contract is [the successor Spec JSON](causal_multichannel_window_boolean_cooldown_full_multiscale_successor_v1_spec_20260813.json). The mechanics implementation is in `../audit/causal_multichannel_window_boolean_cooldown_full_multiscale_successor_v1.py`.

The prospective source adapter, day emitter, atomic admission, and confirmation ledger described by this withdrawn identity are no longer executable. Pure offline successor components retain the paired contract: the control arm must run the exact active owner artifact, the candidate arm must run its own frozen policy, copied arms and one-shot-effect aggregation are rejected, and unsupported source/cache days cannot be converted into zero deltas.

The prospective identity ended with zero rows because its companion was never deployed. That is a withdrawal fact, not an evidence denominator and not proof of missing historical data. No formal OOF economics, final artifact, action authority, or live authority exists under this identity; the current research path is the separate offline historical Development successor, and this companion may not be enabled or renamed.
