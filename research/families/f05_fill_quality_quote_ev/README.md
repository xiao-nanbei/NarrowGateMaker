# F05 Fill Quality, Toxicity, And Quote EV

Last materially modified: 2026-09-06

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../docs/public_private_documentation_contract.md).

Documentation boundary: this README and the unit's tracked `docs/` are public. Owner-only artifact locators, unpublished evidence indexes, and private research context are resolved through this unit's ignored local `private/` catalog and are not distributed with the public repository. See the [public/private research layout](../../PRIVATE_EVIDENCE.md).

Status: this public family does not publish current action flags, deployed side policies, baseline hashes, operational PnL, or owner authority. The public research implementations and aggregate conclusions below grant no action or live authority; deployment admission and the exact current baseline are private evidence.

The filled-only classifier and one-shot soft-widen implementation remain research code, but no pre-correction PnL result can authorize, close, or rank them on the current stack. A new study must estimate direct assignment-to-terminal incremental USDC using decision-visible inputs and the same live-held BER clock in both arms.

The current project-level PnL state is `decision_visible_fill_quality_alpha_missing`. A new feasibility identity may use the existing Development evidence with q90 frozen OFF identically in all arms; q90 terminal-risk-set repair and accumulation of new external receive-time dates are not foreground prerequisites. The target must be decision-visible post-fill net value with an exact lifecycle identity. Rows without that identity are excluded, and a fallback mid is forbidden.

The canonical prediction output is `expected_maker_markout_bps_per_opportunity_30s`. It is expected maker-signed markout in bps per quote opportunity, not USDC EV. `ev_30s` is a read-only compatibility alias for historical consumers.

The root files own Quote-EV modeling and training; `audit/` owns order-level denominators, nulls, toxicity, and selection scoring. The production scorer adapter remains under `strategy/`. Shared dependencies: D, R, S.

## E/C execution plumbing (untrained, not deployed)

[`strategy/risk_selection.py`](../../../strategy/risk_selection.py) supplies immutable
observations, quantity-aware `candidate_role()`, and the pure batch
`evaluate_risk_selection()` interface. Both sides use one visible snapshot. E compares
POST/WAIT for a flat opener; C compares KEEP/CANCEL for an existing order whose
remaining quantity only increases exposure. Other nonterminal orders are included in
the reachable-inventory range. Ambiguous roles, reducing/mixed orders, missing model
inputs, and absent models preserve the baseline decision and its existing protections.
The optional linear scorer estimates action-value differences in USDC; it is an
interface, not a trained or economically validated policy.

The existing F01
[`campaign_outcome_replay_audit.py`](../f01_fixed_parameter_racing/campaign_outcome_replay_audit.py)
accepts `--save-risk-opportunities` on its Python path to export eligible E/C
opportunities, including those that never fill. Keeping the existing frozen inputs,
an `--arm-spec-json` arm can select one recorded opportunity with an override such as:

```json
{"risk_selection_intervention": {"opportunity_id": "<recorded opportunity id>", "action": "WAIT"}}
```

Use `WAIT` only for E and `CANCEL` only for C. WAIT suppresses that submission, without
adding a cooldown; CANCEL uses the normal cancellation lifecycle and cannot remove
ownership before terminal confirmation. Collection is disabled by default.

The existing runner now validates common opportunity prefixes and assembles one
POST-minus-WAIT / KEEP-minus-CANCEL label per intervention at a common terminal mark,
including fees and funding. The [minimal training guide](docs/risk_selection_training.md)
([中文](docs/risk_selection_training.zh-CN.md)) describes the separate chronological
Ridge entrypoint. These are implementation capabilities, not a positive study result.
Complete checkpoint/copy-on-write branching, full-path learned-policy evaluation,
and the live adapter remain unfinished. No new research family or economic/deployment
claim is introduced. Historical results below are not results for this interface.

## Historical research results

The Development-only successor [`decision_visible_negative_fill_value_evidence_m0_v1_1`](docs/decision_visible_negative_fill_value_evidence_m0_v1_1_development_20260729.md) used F10's native, hash-frozen first-add decision-to-terminal artifact, one decision per campaign, direct USDC/decision targets, expanding chronological OOF, and separate BUY/SELL evidence. Neither side improved on the campaign-state nuisance model with a positive familywise lower bound, and the predicted high-risk groups' mean-value intervals crossed zero. The identity is closed on Development; Validation and sealed holdout remain unread.

This does not negate the older fill-toxicity and maker-markout evidence. It shows that the currently frozen decision-visible local microstructure fields do not reliably identify which first exposure-increasing add fill will have negative decision-to-campaign-terminal value. Existing fill-conditioned toxicity or `expected_maker_markout_bps_per_opportunity_30s` outputs cannot be substituted for this target and cannot authorize an F09 action.

The earlier F10 live diagnostic localized a hypothesis-generating SELL opener question, but the later paired quote-coordinate audit withdrew the large BUY/SELL opener edge gap because 31 rows mixed a fallback mid with exact lifecycle values. Both sides still show negative fresh maker value. The corrected Development-only [`sell_first_fill_conditional_value_feasibility_v3`](docs/sell_first_fill_conditional_value_feasibility_v3_development_20260730.md) tested that question using exact native first-opener lifecycles, submit-time causal features, and a fully formal target-day plus previous-natural-day L2 identity.

Grade-A SELL first-opener value was negative on average, with a simultaneous interval fully below zero. The local model nevertheless failed to improve the past-only intercept baseline and did not isolate a selectively worse subset; its predicted high-risk group was less negative at the point estimate. The local prediction branch is closed on Development. Validation and sealed holdout remain unread, and no F09 action or live change is authorized.

The conditional-P3 joint-quote branch now records both governance paths. The immutable hard gate remains failed: its overlap has 28 days, three OOF folds, 282 paired buckets, and one fill in the sparsest side-role-action cell versus the frozen `30 / 4 / 30` requirements. The owner separately accepted that support for an outcome-informed Development continuation without rewriting the hard-gate result.

The owner-path [`conditional_p3_joint_quote_sparse_value_diagnostic_v1`](docs/conditional_p3_joint_quote_sparse_value_diagnostic_v1_development_20260804.md) reconstructed 269/282 complete terminal-overlay buckets, so value coverage was 95.39%; widespread missing value data was not the primary blocker. It evaluated 126 OOF buckets across 13 days and three chronological folds. No non-baseline action passed the past-only simultaneous economic screen, so the rule selected baseline for all 126 OOF buckets. The strongest early signal was about `3.7e-5 USDC/bucket`, still below half of the frozen `1e-4` economic threshold; later-fold intervals crossed zero. The failure is economic resolution and chronological stability.

This closes the current F05 sparse-value quote selector, not conditional P3 probability prediction. v4.1 is retained only as an offline F02 artifact; it is not a live or shadow input. Operational P3 v2 remains unchanged. The owner path stops in F05 because it produced no executable candidate; a 13-arm regenerated full-path replay is therefore not authorized. Development economic outcomes were read, while Validation and sealed holdout remain unread.

The dual-path labels describe possible future evidence classes, not equal current evidence. A normal passing route may eventually receive `research_supported_promotion`. A genuinely new owner successor could retain `owner_risk_accepted_promotion` only after independent positive full-path economics, execution parity, and safety gates. This branch has neither route to F09. The exact scope is frozen in [`conditional_p3_quote_mapping_status_closure_v1`](docs/conditional_p3_quote_mapping_status_closure_v1_20260804.md).

The source-aware [`multiscale_ema_add_wait_incremental_value_source_aware_v1_2`](docs/multiscale_ema_add_wait_incremental_value_v1_2_development_20260809.md) successor used both 2025 and 2026 data without granting provider data economic authority. Its unsupervised 2025 encoder consumed all 112,090,884 admitted side-specific 100 ms BBO rows across 66 days. Direct ADD-minus-WAIT value heads were trained only inside native 2026 exact-lifecycle chronological folds. The complete native panel contains 320 labels; the frozen 24-day outer-fold union contains 192 OOF rows, 96 per side.

Neither side passed the incremental prediction gate. BUY was worse than M0 on both frozen loss metrics with both 95% intervals below zero. SELL had positive point improvements, but both intervals crossed zero. Validation and sealed holdout remain unread, and no F09, action, or live permission exists. This closes the EMA ADD-vs-WAIT identity, not every possible trend signal, and does not establish the current 85-second cooldown as an optimal constant.

The original deployment-oriented [`multiscale_ema_boolean_cooldown_duration_policy_v1`](docs/multiscale_ema_boolean_cooldown_duration_policy_v1_development_20260810.md) then tested the separate state-to-duration question requested after v1.2. It materialized the complete 40-day native census of 8,600 opportunities across eight frozen duration arms per side, producing 68,800 C++ full-path fork rows. After joint censoring, 8,429 opportunities entered side-specific nested chronological training over all 45 EMA pairs and 360 frozen predicates. The 2025 provider panel was used only for unsupervised predicate normalization; all duration-value labels came from native 2026 lifecycle replay. Its deployment-strength LCB fallback ran before untouched outer OOF, so its 0% non-control action rate is a conservative abstention result rather than a test of non-baseline rule transport.

The [`exploratory OOF successor`](docs/multiscale_ema_boolean_cooldown_duration_policy_exploratory_oof_v1_development_20260810.md) then forced the best support-valid non-baseline rule into every outer fold and applied deployment gates only after all OOF rows were scored. BUY changed the duration on 76.99% of rows but produced -0.001511 USDC/campaign-weight with a -0.004711 lower bound. SELL changed 85.64% of rows but produced -0.003952 with a -0.018341 lower bound. Both side gates failed. No learned-policy full-path, Validation, holdout, F09 registration, or live deployment is authorized. This closes the frozen Boolean-duration candidate set, not all possible cooldown state models, and does not identify 85 seconds as optimal.

The new exploratory successor [`causal_multichannel_window_boolean_cooldown_duration_v2`](docs/causal_multichannel_window_boolean_cooldown_duration_v2_design_20260810.md) keeps those v1 results immutable and changes the information and execution identity. It freezes a completed 100ms causal source grid, explicit feature-ready cutoff, three-valued Boolean logic, mandatory action-magnitude and campaign context, and separate BBO, individual-trade, and depth EMA channels. The old eight-duration vocabulary is retained so feature changes are not confounded with action-grid reselection.

The outcome-blind execution pipeline is now implemented and hash-bound. It includes the atomic `CooldownAssignmentSnapshotV2`, raw-native M2 extraction, D-1/D/D+1 support, target-day parent stop with D+1 child washout, POSIX copy-on-write eight-arm forks, separate 2025 provider-book and official-trade predicate materialization, fold-local M0 fitting, and a self-contained nested chronological OOF admission. The frozen 50-day calendar yields 41 formal full-support days: 33 from the historical 40-day prefix and 8 from the added 10-day late diagnostic; the other 9 days remain a separate reduced-support identity.

Execution amendment v3 binds the formal opportunity identity to partial-fill ordinal and quantity, stores and rehashes the complete assignment snapshot, and cross-checks source, execution, clock, order, side, role, quantity, and duration identities across the opportunity and all arms. It also separates statistical OOF gates from effective deployment gates. R0 is reproduction-only; M0, M1, and M2 are selected through frozen paired incremental gates rather than an unrestricted best-of-four search. A continuous-state comparator remains required before any unified Boolean policy can be frozen. Execution amendment v4 now implements that diagnostic on raw, unquantized source fields with capacity selected only inside chronological inner folds. It remains unrun on formal economics and can block a weaker Boolean policy, but can never replace the Boolean identity or grant action/live authority.

Execution amendment v5 closes three identity gaps before formal materialization: strict-native warmup admission is propagated from the raw D-1 source rather than inferred from the UTC cutoff; cooldown deadline ownership is a bounded M0 input (`none` or `existing_same_side_lineage`); and outer OOF deployment support is counted only on campaigns and days that actually receive a non-control duration. It also permits book/trade literals in the same clause only when they share the frozen feature-ready cutoff, and keeps causal impact, absorption, and ownership-attributed depletion/refill explicitly outside the current reduced M2 claim.

Execution amendment v6 binds the frozen 41/9 denominator to the existing outcome-blind native sequence audit and replaces calendar-adjacency coalescing with overlap-only D-1/D/D+1 source segmentation. All 41 formal targets have strict D and D+1 sequence support. The formal source union is eight segments, 57 unique UTC days, and 1,368 hours; target start must be snapshot-seeded and all strict source-counter deltas through D+1 must be zero. A D-1 warmup gap may recover only through a snapshot before the target boundary.

Execution amendment v7 aligns the strict-label consumer with the v3 target receipt emitted by that source plan. Historical v2 segment and target receipts fail closed; immutable validated native-hour cache files remain reusable.

The strict-native 48-opportunity benchmark is durably admitted on `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}`. All 48 eight-arm bundles completed; 22 were strict-exact and 26 failed closed per opportunity on queue ambiguity or invalidation. It remains engineering evidence only because it predates the hardened opportunity identity. The outcome-blind 2025 predicate artifact also completed all 134 book/trade parts, and the raw source union admitted 8/8 segments, 57 unique source days, 1,368 hours, and 41/41 target receipts with every frozen sequence and timestamp counter at zero.

That source admission did not make historical strict economics identifiable. Public trade rows provide millisecond timestamps while the book stream contains sub-millisecond ordering. Same-millisecond trade/book precedence cannot be recovered from the retained inputs, and inventing a tie-break would change queue seeds and fills. Execution amendments v8/v9 and the [`strict-native failure receipt`](docs/causal_multichannel_window_boolean_cooldown_duration_v2_strict_native_formal_execution_failure_20260811.md) therefore fail the 41-day formal label path closed. No reusable strict economic panel was produced or read; admitted raw source bytes remain reusable.

Owner-only modeled-queue executions, exact configs, report bytes, policy artifacts, baseline values, and operational decisions are private and `private_not_distributed`. The public repository retains the reusable modeled-OOF, replay-emitter, runtime-policy, feature, predicate, snapshot, and native-cache primitives. Its non-authoritative aggregate research conclusion is only that no side passed the frozen M0 research gate.

The historical full-multiscale exact-owner execution graph, its formal-attempt receipts, and deployment-bound projections are retained only in the private archive. Missing private evidence cannot be replaced with public defaults, and implementation parity does not establish economic validity or current authority.

Exact OOF, daily, policy, scorecard, baseline, and deployment bytes belong to the private evidence catalog. Public prose exposes method boundaries and non-authoritative aggregate conclusions only; current operational state and authority cannot be inferred from this repository.
