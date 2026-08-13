# Owner Boolean Cooldown Policy Interpretation Errata

Last materially modified: 2026-08-13

Status: interpretation correction only; the active policy, its hashes, its durations, and its live authorization are unchanged.

Evidence availability: this public errata and the referenced public identities are available in the repository. Exact policy bytes, predicate-bundle bytes, and owner-only execution evidence remain in the private evidence store and are not distributed. SHA256 values identify named byte artifacts; they are integrity metadata, not download links.

## Question

What did the v3 learning evidence actually establish, and which features does the exact owner-deployed policy really use?

## Correct Boundary

The current active identity remains `causal_multichannel_window_boolean_cooldown_owner_policy_v1`, with the permanent `owner_risk_accepted_promotion` label. The owner decision is not reclassified as a research error. The policy SHA256 remains `877a20033ff678bd7aa9b58069f37c3dc459b18db78c316b7e50023248f15a29`, and the predicate-bundle SHA256 remains `ba4c1bac2380564aa24d47d12796f3be5c0312cc88d28218ce84bd20e4170f37`.

The historical outer out-of-fold result belongs to the learning algorithm's four fold-specific policies. It is not exact out-of-fold evidence for the later full-Development refit artifact. In particular, the final `211s` action did not occur in any outer-fold policy. The exact final artifact therefore has no independent OOF result; its supporting economic evidence remains the owner-accepted 50-day repeated replay and 71-day restart-aware replay described in the [active release](causal_multichannel_window_boolean_cooldown_owner_active_release_v1_20260812.md).

## Search Boundary

The v3 learner did not place all 5,609 cumulative M2 predicates into economic competition. Its shallow profiles allowed at most 64/128/256 selected features and depth 2/3/4. All four SELL/M2 outer folds selected the small profile, whose depth-two tree could retain at most three split predicates. Feature ranking used observedness and TRUE/FALSE balance rather than incremental economic value, and the maximum feature pool was constructed from complete outer-train feature distributions before inner-fold fitting.

This remains valid historical owner-route exploration, but it is not a full-multiscale or full-multichannel architecture closure. The predecessor code and historical result are not rewritten.

## Compiled Policy Semantics

The policy was trained from a cumulative M2-labelled candidate source panel, but its compiled rules use only M0 campaign context and mid-price EMA predicates. It uses no trade-flow, trade-count, depth, depletion, refill, or queue predicate. Therefore `candidate_source_block=M2` is provenance, while `uses_m2_incremental_features=false` is the compiled-policy fact.

Let (A) mean `campaign_age > CONTROL duration`, (P) mean the `4s/16s` cross is recent, and (Q) mean the `16s/256s` cross is recent. The first rule is:

\[
1748s:(P\land A)\lor(\neg P\land A).
\]

When (P) is observed this reduces to (A); when (P) is unobserved, three-valued logic fails closed to the control. The `4s/16s` predicate is therefore a feature-readiness guard, not an economic duration branch. The economic branches are campaign age and `16s/256s` cross recency. This simplification is audit-only and does not remove the live guard.

## Unknown Targets And Hierarchy

The v3 multi-output tree initialized unsupported action targets to zero during discovery. Those rows were excluded from outer economic scoring, but the neutral-zero assumption could still affect split and duration discovery. Also, continuous-minus-Boolean contrasts entered the simultaneous family but not the final hierarchy. Both issues are repaired only in the new successor identity; changing the historical v3 implementation would invalidate its frozen code bindings.

The continuous contrast is oriented as `continuous - Boolean`. A positive simultaneous lower bound means the continuous representation significantly dominates Boolean state and must block a Boolean freeze; it is not another positive advancement gate.

## Coverage

The historical refit's feature-ready action rate and the 71-day replay action rate used different fallback/source-support denominators. Future reports must partition every eligible event into the same mutually exclusive reason vocabulary: eligible feature-ready action, policy control, ineligible event, warmup incomplete, stale feature, unobserved predicate, safety fallback, source unavailable, cache unavailable, binding invalid, or lifecycle unidentified.

The machine-readable interpretation is in [the owner-policy interpretation manifest](causal_multichannel_window_boolean_cooldown_owner_policy_v1_interpretation_manifest_20260813.json). The corrected research implementation is registered separately as `causal_multichannel_window_boolean_cooldown_full_multiscale_successor_v1`.

The exact private policy and predicate-bundle bytes were also loaded through the production runtime evaluator and compared with the public research interpretation over every three-valued assignment of the three compiled predicates. All 27 SELL cases and all 27 BUY fallback cases matched, with zero discrepancies. The private receipt artifact is `f05-full-multiscale-successor-exact-owner-artifact-parity-20260813`, SHA256 `57cb19565197c18e4535940e46e6fd8236278e9de62ce49528fc47bbcbfb84eb`; this is implementation-parity evidence only, not OOF or economic evidence.
