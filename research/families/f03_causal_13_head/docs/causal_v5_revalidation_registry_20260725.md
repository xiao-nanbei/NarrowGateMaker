# Causal v5 Revalidation Registry

Last materially modified: 2026-08-12

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Date: 2026-07-25

> Current status (2026-07-27): retain this registry as the authoritative withdrawal map for mixed-L2 and corrupted trade-side evidence. Its “current causal-v5” labels are historical. Subsequent time/calendar/unit repairs made `causal-v7` the maintained identity and superseded all exact causal-v5/v6 model and replay numbers. Completed/withdrawn family governance decisions remain in force; use `docs/time_unit_contract_repair_20260726.md` for current model semantics.

Status: evidence-governance registry. This document records required reruns, their completion status, interpretation downgrades, and formal withdrawals. Detailed results remain in the companion evidence reports. This registry does not authorize holdout access and does not change live policy.

## Scope

This registry covers evidence affected by either of these historical data identities:

1. the former mixed BTCUSDC `bbo/` and `l2/` roots, where most early days were approximately one-second top-10 states and recent days were approximately 100ms top-20 states;
2. the eight BTCUSDC individual-trade files for 2026-07-04 through 2026-07-11 whose `is_buyer_maker` column was incorrectly all `true`.

The normalized policy-visible book identity for new work is:

```text
${NARROWGATE_RETIRED_DATA_ROOT}/normalized_l2_100ms_v2/
```

Exact active-price queue research must additionally consume the native CryptoHFTData snapshot/delta stream. A normalized top-20 matrix plus q0.70 fallback is an operational counterfactual, not native deep-queue truth.

The repaired 2026-07-04 through 2026-07-11 individual-trade files must pass the two-sided `is_buyer_maker` quality gate before any side-specific result is read. New manifests must bind their file hashes.

The normalized-100ms P3 recalibration is an already completed anchor, not a pending item in this registry. Its old mixed-BBO result was independently reproduced to within about 0.2%, but future work must bind the normalized artifact and hashes. That P3 result does not rescue old queue, fill, campaign, or action evidence.

## Registry Rules

- **Must rerun** means the evidence remains part of the maintained baseline, model, denominator, or active research path. Its old exact values cannot be used until the listed entrypoint produces a new hash-bound identity.
- **Retain but downgrade** means a mechanism, data architecture, or descriptive result remains useful under a narrower interpretation. It cannot be used as exact queue truth, action uplift, or promotion evidence.
- **Formally withdraw; do not rerun** means the exact family is closed. Data repair does not reopen it. Any future experiment needs a new economic action, family ID, split, and preregistration.
- A still-sealed holdout stays sealed. Sealing does not make it available to rescue a withdrawn family.
- Blank or speculative v5 result fields are intentionally absent. Results enter this registry only after their experiment has completed and its manifest has been admitted.

## A. Must Rerun

### Completion Status

This table records completion of the required rebuild without changing the original registry contracts below.

| Registry ID | Status on 2026-07-25 | Conclusion recorded at completion |
|---|---|---|
| `CV5-R01` | complete | 128-day causal feature panel and 13-head bundle rebuilt; model remains shadow-only |
| `CV5-R02` | complete | Four BUY scorers rebuilt on the frozen Development/Validation split; every action gate failed |
| `CV5-R03` | complete | Lifecycle and opportunity null rebuilt; executable random-passive null fails Validation raw PnL and InvAdj |
| `CV5-R04` | complete | Clean ML-OFF/ON and q0.70 sensitivity rerun; no ML promotion and no queue parameter selected from replay PnL |
| `CV5-R05` | complete | Repaired Validation passes BUY adverse/favorable/repair prediction gates; action and live gates remain closed |
| `CV5-R06` | withdrawn and deleted | The independent-world matcher and observational nonlinear lookup were superseded by paired v2; no old probability or NLL remains admissible |
| `CV5-R07` | not rerun | Historical replay attribution remains withdrawn; current live-log attribution stays descriptive |

| Registry ID | Experiment ID and affected panel | Pollution source | Required new entrypoint | Holdout status |
|---|---|---|---|---|
| `CV5-R01` | `causal_v4_empirical_p3_retrain_replay_20260718`; 122-day feature/order identity, split `80 train / 1 embargo / 20 validation / 1 embargo / 20 test` | Causal book features were generated from the mixed L2 root; q0.70 supplied non-native queue; 2026-07-04..11 side labels and previously identified metrics-ready errors contaminate later exact model/replay evidence | `features/feature_engineer.py` on `normalized_l2_100ms_v2`, then `python -m models.ml_model`; bind normalized P3, current config, native/operational queue mode, latency, and repaired trade hashes | **Not sealed.** The old test was read and withdrawn. Causal v5 requires a newly frozen independent test/holdout identity |
| `CV5-R02` | `saved_btcusdc_buy_fill_selection_causal_v4_20260718` and `saved_btcusdc_buy_beats_opportunity_causal_v4_20260718`; blocked-day cross-fit over the 122-day order denominator | Mixed-L2 quote/queue features; downstream order paths; 2026-07-04..11 BUY/SELL trade-side corruption; cross-fitting means dropping only the eight report rows cannot repair training lineage | `python -m models.audit.order_level_panel`, then `python -m models.audit.fill_selection_score`; require exact `decision_id`/`client_order_id` lineage and repaired side-quality manifest | **No valid sealed holdout remains for these artifacts.** Freeze a causal-v5 scorer split before reading outcomes |
| `CV5-R03` | `fill_inventory_lifecycle_retained111_20260713` plus its order denominator, random opportunity/passive null, maker markout, campaign, FIFO/LIFO and terminal decomposition; 111 good days from 2026-01-01 through 2026-07-03 | Old mixed L2 and old queue changed which orders filled and therefore changed lots, campaigns, survival, null denominators, and exact PnL | `python -m models.audit.order_level_panel`; `python -m models.audit.fill_inventory_lifecycle`; `python -m models.audit.runner`; `python -m models.audit.null_baseline_panel`; `python models/campaign_outcome_replay_audit.py` | **N/A.** These are descriptive/counterfactual denominator audits. Any later action using them must freeze its own holdout |
| `CV5-R04` | `formal_recalibration_20260715` queue-v3 q0.70 artifact and the strict ML-OFF/ML-ON/queue-sensitivity tables in `causal_v4_empirical_p3_retrain_replay_20260718`; queue fit days 2026-07-10..11 and 122-day replay table | The q0.70 artifact itself was fitted from 2026-07-10..11 live `quote_decisions`/`order_outcomes` plus aggTrades and did **not** read the offline mixed `bbo/l2` roots. It remains a two-day, live-conditional calibration rather than a universal queue law. The invalidated evidence is its sensitivity, fills and exact PnL when applied through the superseded replay/L2 identity | Rerun the q0.70 sensitivity and exact PnL on normalized-100ms replay with `python models/campaign_outcome_replay_audit.py`, reporting operational-fallback and native exact-level modes separately. Keep q0.70 as the conditional reference unless new live order outcomes justify `python -m models.queue_calibration`; never select q from replay PnL | **Not sealed.** The old replay test/table was consumed and withdrawn. The live-fitted artifact is not withdrawn merely because the replay identity changed |
| `CV5-R05` | `dynamic_fill_hazard_m0_native_strict_nested_cal_v2`; Development 40 days, 2026-04-17..06-26; one-shot BUY Validation 10 days, 2026-06-28..07-08 | Native L2 is correct, but the Validation lifecycle and side-specific labels were generated before repair and include corrupted 2026-07-04..08 individual trades | Rebuild only the Validation lifecycle with `python -m models.audit.native_lifecycle_universe_v1`, then evaluate the frozen Development artifact with `python -m models.audit.dynamic_fill_hazard`; do not refit or retune from Validation | **Yes.** The 10-day family holdout, 2026-07-10..07-20, remains sealed. The previous Validation numbers are withdrawn |
| `CV5-R06` | deleted `spread_fill_nonlinearity_v1`; Development 40 days through 2026-06-26 and Validation 10 days through 2026-07-08 | Besides repaired side labels, its lifecycle outcome inherited the independent-world scalar matcher that could fill a deeper order while missing a shallower counterfactual | Do not rerun or restore this family. Any state-conditioned fill model must start from the paired v2 denominator and freeze a new split | The former holdout identity is retired with the family and cannot be reused as confirmatory evidence |
| `CV5-R07` | `dynamic_mechanism_campaign_audit_v1` replay component; Development56 from 2026-01-01 through 2026-06-12 | Top-20/q0.70 replay and superseded P3/book identity make exact add-toxicity, repair-delay, cap-hit, distance, campaign and PnL values approximate | `python -m models.audit.dynamic_mechanism_campaign_audit` after the causal-v5 baseline, scorer and queue identities are frozen | **N/A.** This is mechanism attribution, not an action holdout. Its actual live-log component is retained separately as non-causal evidence |

### Executed Causal-v5 Rerun Order (Historical)

1. Build the causal-v5 normalized feature and order denominator.
2. Rebuild lifecycle, null, markout, and campaign evidence.
3. Retrain the 13-head model and both BUY scorers.
4. Recalibrate or explicitly bind queue evidence and rerun strict ML-OFF/ON sensitivity.
5. Rerun the frozen native BUY Validation; replace spread-fill v1 with the paired v2 contract.
6. Rerun dynamic-mechanism attribution only after the preceding identities are fixed.

Completing this sequence did not promote an action family. Its exact causal-v5 model/replay values were later superseded by causal-v7, while the family closures and withdrawal decisions remain in force.

## B. Retain But Downgrade

| Registry ID | Experiment ID and date panel | Retained content | Downgraded content and new entrypoint | Holdout status |
|---|---|---|---|---|
| `CV5-D01` | `p3_touch_recalibration_normalized100ms_v2_20260725`; 117 UTC days, `69 train / 24 validation / 24 test` | Empirical touch curve, delta-star conclusion, and normalized input identity | The former mixed-BBO artifact is superseded. Cite only the normalized artifact; use `python -m models.audit.p3_touch_calibration` for future calibration | Test was read as calibration evidence; no sealed action holdout |
| `CV5-D02` | `paired_fixed_spread_monotonic_v2_20260726`; 128-day descriptive panel and 62-day formal-normalized tier | Paired fixed-distance geometry, shared lifecycle/latency path, exact-vs-through touch decomposition, and monotone 1s/5s/10s/lifecycle curves | The v1 implementation, report, fitted lookup, and artifacts were deleted; only the failure mechanism remains documented in v2. v2 still uses calibrated fallback for prices beyond visible top-20: about 79% at 80 ticks, 93% at 100 ticks, and above 99% at 140 ticks | **N/A for this descriptive replay.** Any fitted state-conditioned lookup or action must freeze a separate Development/Validation/holdout identity |
| `CV5-D03` | `global_reference_stage0_retained111_20260711`; 111 retained days from 2026-01-01 through 2026-07-03 | External trades-derived causal one-second state, three-venue 2-of-3 construction, spot/perp separation, USDCUSDT bridge, and leave-one-venue-out architecture | Exact maker markout, fill and campaign tables used the old local denominator and are withdrawn. If M1 is reopened, rebuild local outcomes through `python -m models.external_consensus_layer` plus `python -m models.audit.order_level_panel` | No formal action holdout; Stage 0 remains diagnostic-only |
| `CV5-D04` | `dynamic_mechanism_campaign_audit_v1` actual EC2 48-hour component | Actual live fills, campaign paths and mechanism counters remain genuine live observations | Treat adjacent/live-window attribution as descriptive and non-causal. Do not combine it numerically with the withdrawn Development replay until `CV5-R07` completes | N/A |
| `CV5-D05` | Python/C++ same-input parity, accounting/unit fixes, feature-ready timing, merged event clock, terminal-MTM contract, receive-time latency and live soak evidence | Implementation and systems conclusions under their frozen inputs | They do not prove exchange queue truth or action alpha and cannot validate old exact PnL tables | N/A |

## C. Formally Withdraw; Do Not Rerun

| Registry ID | Experiment ID and date panel | Withdrawal reason | New entrypoint | Holdout status |
|---|---|---|---|---|
| `CV5-W01` | Legacy `48-arm`, `512-arm`, and `1024-arm` parameter-racing generations; quick-smoke4, retained39, blocked71 and late4 panels | Exact PnL, ranking and winner identities depend on old left-labelled features, trade clock, historical P3 override, old queue, mixed L2, or old live-incident semantics. The panels were repeatedly consumed and are not confirmation evidence | **None.** If global parameter research is ever reopened, create new arm IDs with `python -m models.parameter_racing_sweep` on the causal-v5 baseline | **No.** retained39/blocked71/late4 are not sealed holdouts |
| `CV5-W02` | `side_specific_local_actions_causal_v4_20260718`; Development80 2026-01-01..06-02, Validation20 2026-06-04..06-23, sealed holdout20 2026-06-25..07-15 | Fixed local add actions failed to transport before the data withdrawal. Data repair does not justify reranking the old validation winner | **None for this ID.** A successor must use a new action and may use `models.audit.local_action_uplift` only under a new family spec | **Yes, but retired with this family.** Do not open it to rescue the old actions |
| `CV5-W03` | `buy_add_conditional_widen_causal_v4_v1` and `sell_add_repair_trend_skip_causal_v4_v1`; Development100 2026-01-01..06-23, Validation9 2026-06-25..07-03, holdout10 2026-07-05..07-15 | BUY one-tick geometry and SELL one-cycle skip failed Development and were explicitly closed. Their exact DR/campaign values are withdrawn with the old L2/queue identity | **None.** Any successor needs a materially different action and new family ID | **Yes.** Validation and holdout remain unused for promotion; the holdout stays sealed |
| `CV5-W04` | `queue_value_keep_cancel_v1`; Development56 2026-01-01..06-12, Validation9 2026-06-14..06-22, holdout9 2026-06-24..07-03; and `queue_value_cancel_reenter_v3`; Development25 2026-04-25..06-12 with the same later panels | The frozen K0/K1 thresholds used top-20/q0.70 rather than true active-price queue. The exact action definitions already failed and must not be retuned on repaired data | **None for v1/v3.** A successor must use native exact-level paths and a new `queue_value_*` family through `models.audit.queue_value_competing_risk` | **Yes.** Their holdouts remain sealed and retired with the families |
| `CV5-W05` | `buy_state_conditioned_rearm_after85_v1`, `sell_state_conditioned_rearm_after85_v1`, `buy_recovery_event_rearm_v1`, and `sell_recovery_event_rearm_v1`; Development56 2026-01-01..06-12, Validation9 2026-06-14..06-22, holdout9 2026-06-24..07-03 | The frozen recovery conjunctions failed support or action value. Old top-20/q0.70 numbers are withdrawn, but the family-level non-promotion remains final | **None.** Do not alter thresholds, hysteresis or component weights under these IDs | **Yes.** Holdouts remain sealed |
| `CV5-W06` | `first_add_marginal_order_value_v1`, `buy_first_add_skip_marginal_value_v1`, and `sell_campaign_add_permission_v1`; Development56 2026-01-01..06-12, Validation9 2026-06-14..06-22, holdout9 2026-06-24..07-03 | One-cycle first-add intervention was near no-op; stop-add-until-flat was participation shutdown. These are action-definition failures, not data defects that merit repetition | **None.** A future add-permission mechanism requires a new action and family identity | **Yes.** Holdouts remain sealed |
| `CV5-W07` | `safe_add_rearm_outcome_20260714`, `safe_add_rearm_path_policy_20260714`, `safe_add_rearm_state_policy_20260714`, and `state_conditioned_policy_artifact_20260715`; Development111 2026-01-01..07-03 and later5 2026-07-07..07-11 | Old feature/L2/queue identity; later5 directly overlaps the corrupted trade-side window; subsequent action families supersede these exploratory identities | **None.** Keep artifacts only as provenance | No valid sealed holdout was defined for these exploratory runs |
| `CV5-W08` | `queue_value_keep_cancel_native_exchange_v1` and `queue_value_net_hazard_keep_cancel_v2`; Development17 2026-05-06..06-14, Validation8 2026-06-16..06-23, holdout8 2026-06-25..07-03 | These already used native/exact evidence and dates before the trade-side corruption. They failed native support, uncertainty, or toxic-fill selectivity. Normalized-L2 repair does not change that scientific decision | **None for these IDs.** A new queue action requires a new estimand, action and family ID | **Yes.** Validation/holdout remain unread and sealed |
| `CV5-W09` | `dynamic_fill_hazard_m0_v2` v8; Development17 2026-05-06..06-14, Validation8 2026-06-16..06-23, holdout8 2026-06-25..07-03 | This exact prediction identity closed on Development and was superseded by the native-strict nested-calibration family in `CV5-R05` | **None.** Do not reopen v8 by changing its threshold or calibration after outcome access | **Yes.** Validation and holdout remain unread and sealed |
| `CV5-W10` | Historical random-null, direct quote-EV, cap-compression, markout-sign, stop-add/rearm exact tables, and pre-causal BUY-score buckets without a current canonical manifest | Their order/fill denominator, clock, queue or model identity is superseded. Historical exact values cannot be repaired by relabelling paths | **None for the historical IDs.** Their maintained methods may be rerun only through `CV5-R02`/`CV5-R03` under new manifests | No reusable sealed holdout |

## Explicit Legacy Ranking Withdrawal

The following evidence is invalid for current selection, comparison, or promotion:

```text
retained39 exact PnL and arm ordering
blocked71 exact PnL and arm ordering
late4 exact PnL and arm ordering
48-arm winner records
512-arm winner records
1024-arm winner records
```

No old arm identifier is a candidate for automatic rerun or live promotion. The historical statement that an arm failed promotion may remain as a conservative governance record, but its exact PnL, fills, tails, campaign counts, and rank must not be quoted as causal-v5 evidence.

## Admission Requirements For New Results

A completed rerun may be added only when its manifest records:

- experiment and family ID;
- Git commit plus dirty/untracked identity when applicable;
- config, P3, model and queue artifact hashes;
- normalized L2 and native snapshot/delta manifests;
- repaired execution-trade quality hash;
- exact Development, embargo, Validation and sealed-holdout panels;
- feature-ready and event-ordering contract;
- latency profile and random seed;
- Python/C++ engine identity where both paths are claimed;
- artifact paths and an explicit holdout-access decision.

Until then, the status in this registry remains unchanged.

## Source Documents

- `docs/legacy_l2_evidence_revalidation_20260725.md`
- `docs/historical_backtest_evidence_revalidation_20260720.md`
- `docs/p3_touch_recalibration_normalized100ms_v2_20260725.md`
- `docs/paired_fixed_spread_monotonic_v2_20260726.md`
- `docs/native_strict_universe_20260724.md`
- `docs/causal_v4_empirical_p3_retrain_replay_20260718.md`
- `docs/side_specific_action_uplift_existing_split_20260718.md`
- `docs/dynamic_mechanism_campaign_audit_20260722.md`
- `project.md`
