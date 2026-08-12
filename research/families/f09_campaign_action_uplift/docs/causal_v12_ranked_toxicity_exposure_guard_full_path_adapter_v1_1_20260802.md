# Ranked Toxicity Guard Full-Path Adapter v1.1

Last materially modified: 2026-08-02

## Status

This is an execution-only amendment to the frozen BUY and SELL v1 mechanics registrations. It does not change the action, p90 construction, random seeds, baseline, scorecard, or economic gates. No mechanics, PnL, markout, toxic-fill, Validation, or holdout result has been read.

The adapter contract tests passed before this amendment was frozen. This makes the two registered identities eligible for a later mechanics-only run; it does not authorize that run's result in advance and grants no action or live permission.

## Prospective Assignment

The first untreated, baseline-eligible exposure opportunity creates a stable `prospective_campaign_side_id` and a 0.5/0.5 assignment before any cancel, suppression, submission, or fill can occur. The identity remains present when the candidate submits no order and receives no fill. Multiple guard episodes inside that prospective campaign-side lineage reuse the same assignment.

UTC rollover does not reset assignment, guard state, order state, or campaign state. The threshold may advance to the new day's strictly past-only p90, but the randomization stratum remains the first opportunity's UTC day.

## Untreated Denominator

Each decision supplies two independently maintained untreated baseline-shadow snapshots, one from the control replay and one from the candidate replay. They must match exactly before the candidate permission is evaluated. Assignment, opportunity counts, and final-action denominators use this shadow state rather than the treatment-dependent candidate state.

Prediction input is one canonical collapsed 10-second bucket. A repeated bucket is forbidden; a changed score, feature-ready timestamp, decision clock, or model hash is an inconsistent duplicate and fails immediately. The frozen model hash and `feature_ready_ts <= decision_ts` are checked on every bucket.

## Lifecycle Routing

The adapter writes a complete execution journal and binds the frozen guard to the shared quantity-weighted order lifecycle:

```text
cancel reject
  -> ACTIVE or PARTIALLY_FILLED
  -> old fill-risk set remains active

cancel-pending partial fill
  -> CANCEL_PENDING
  -> remaining quantity updates

cancel ACK / pre-ACK full fill / expiry / order reject / shutdown terminal
  -> EXCHANGE_TERMINAL
  -> old queue, age, hazard, and depth cursor are cleared
  -> RELEASED when score recovered while waiting
  -> SUPPRESSING otherwise
```

A full fill must pass through the fill event so remaining quantity reaches zero before terminal retirement. `cancel reject` and terminal `order reject` are separate events. Reducing quotes remain byte-for-byte equivalent in permission, price, quantity, and final action.

## Zero-Tolerance Gates

The following counts must all equal zero before mechanics may be read:

- assignment after treatment;
- duplicate or inconsistent prediction bucket;
- hazard or cursor use after exchange terminal;
- reducing quote changes;
- control/candidate untreated baseline-shadow mismatch;
- campaign-side rerandomization.

The adapter emits only execution mechanics and explicit false permission fields. Economic columns are absent by contract.

## Frozen Boundary

- [BUY v1 mechanics registration](causal_v12_ranked_toxicity_exposure_guard_buy_v1_mechanics_spec_20260802.json)
- [SELL v1 mechanics registration](causal_v12_ranked_toxicity_exposure_guard_sell_v1_mechanics_spec_20260802.json)
- [v1.1 machine amendment](causal_v12_ranked_toxicity_exposure_guard_full_path_adapter_v1_1_execution_amendment_20260802.json)

The original v1 files remain unchanged. Any later change to threshold, assignment, action duration, baseline, denominator, or economic gate requires a new action identity rather than another execution amendment.
