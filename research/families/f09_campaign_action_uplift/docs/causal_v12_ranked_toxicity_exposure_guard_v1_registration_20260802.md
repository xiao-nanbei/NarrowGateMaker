# Causal v12 Ranked Toxicity Exposure Guard v1

Last materially modified: 2026-08-02

## Status

Two independent F09 identities are registered for mechanics only:

- `causal_v12_ranked_toxicity_exposure_guard_buy_v1`
- `causal_v12_ranked_toxicity_exposure_guard_sell_v1`

Neither mechanics nor economic outcomes have been read. Validation, sealed holdout, action, and live permissions remain false. A hash-bound full-path adapter is still required before either mechanics run may start.

## Baseline

Both identities use the exact operational v5 baseline:

- causal v12 ML enabled;
- q90 shadow enabled and q90 action disabled;
- BUY fill-selection enabled identically in both arms;
- raw `adverse_toxicity_threshold=2.0` remains disabled;
- reducing quotes and every non-guard strategy mechanism remain unchanged.

Turning BUY fill-selection off would define a different diagnostic baseline and cannot produce promotion evidence for the current live system.

The production q90 suspension release is hash-bound in both specs. The terminal active-risk-set repair is implemented and verified only in the local tree; it is not part of the deployed v5 runtime. Prospective placement recovery, full transport, and live parity remain unsupported, so neither guard may infer q90 action authority from the local repair.

## Rank Denominator

The p90 threshold uses only strictly earlier UTC days and only the first causally visible row in each completed 10-second prediction bucket satisfying:

- the matching side;
- baseline-eligible quote permission;
- exposure-increasing role (`opener` or `add`);
- `feature_ready_ts <= decision_ts`.

Repeated 100ms main-loop evaluations do not enter the empirical CDF. The threshold is frozen for the entire UTC day. There is no fallback threshold on days with fewer than five prior days or 500 prior eligible buckets.

## State Machine

The candidate is a persistent campaign-side permission action:

```text
BASELINE
  -> p90 crossing
GUARD_ACTIVE
  -> cancel active exposure order once
CANCEL_PENDING
  -> cancel ACK
SUPPRESSING
  -> next completed 10s score below the frozen p90
RELEASED
```

After cancel ACK, the old order leaves the fill-risk set. Its queue, age, hazard, and depth cursor cannot be used for recovery. Re-entry starts a new order lifecycle with age zero and a new queue. Reducing quotes always bypass the guard. Randomization occurs once per prospective campaign-side lineage at strict 0.5/0.5 propensity and never repeats at each prediction bucket.

## Two-Stage Read

Stage one can read only opportunity, assignment, cancel/ACK, state-transition, support, propensity, ESS, and final quote-action mechanics. PnL, markout, toxic fills, terminal value, and campaign tails are forbidden.

Only if every mechanics hard gate passes may a new hash-bound execution spec open Development economics. Future selectivity uses quantity fractions on the common assigned-quantity denominator. Missing 10-second BBO labels remain in the denominator and use a censoring bound adverse to the candidate. BUY and SELL must independently pass `action_execution_selective_v2`; pooled promotion is forbidden.

## Frozen Files

- [BUY mechanics spec](causal_v12_ranked_toxicity_exposure_guard_buy_v1_mechanics_spec_20260802.json)
- [SELL mechanics spec](causal_v12_ranked_toxicity_exposure_guard_sell_v1_mechanics_spec_20260802.json)
- [future outcome contract](causal_v12_ranked_toxicity_exposure_guard_outcome_contract_v1_20260802.json)
- [BUY registration audit](causal_v12_ranked_toxicity_exposure_guard_buy_v1_registration_audit_20260802.json)
- [SELL registration audit](causal_v12_ranked_toxicity_exposure_guard_sell_v1_registration_audit_20260802.json)

The registrations therefore establish a falsifiable action definition without claiming that the action has support, economic value, or live authority.
