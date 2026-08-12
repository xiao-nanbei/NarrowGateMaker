# Ranked-Toxicity Full-Path Mechanics v1.5 Implementation Failure

Last materially modified: 2026-08-02

## Status

`execution_preflight_failed_before_formal_mechanics_result`

The 40-day execution-only mechanics run did not start. A one-day Python-authoritative preflight found that campaign-side assignment is not a stable treatment unit while maker orders remain resting across an inventory campaign terminal. No PnL, markout, reward, Validation, or sealed holdout was read.

The frozen BUY/SELL action definitions, p90 construction, baseline, scorecard, and v1.4 bytes remain unchanged. The earlier statement that v1.4 was eligible for mechanics execution is withdrawn.

## Failure

On `2026-04-17`, the baseline path produced this sequence:

1. At `00:02:37.900Z`, BUY order `16` was submitted and activated as an exposure-increasing `add` under untreated lineage 1.
2. At `00:02:43.617Z`, reducing SELL order `17` fully filled, flattened the inventory, and ended that inventory campaign.
3. At `00:02:45.769Z`, BUY order `16` was still exchange-active. The next baseline decision correctly classified that same order as an `opener` under untreated lineage 2.

Ending lineage 1 therefore failed with:

```text
AdapterContractViolation: cannot end prospective campaign with active exposure orders: ['16']
```

This is not candidate-only path divergence. The complete 29,072-row baseline journal independently shows order `16` crossing the campaign boundary. If the runner rerandomized at lineage 2, the new arm could cancel or suppress an order created under lineage 1. The resulting observations would contain treatment carryover and would not identify the registered campaign-side action.

## Evidence Boundary

The completed baseline manifest is hash-bound in the machine-readable failure record. The BUY and SELL candidate manifests are deliberately left `closed=false`; they are partial execution diagnostics and are not formal mechanics evidence.

Before this failure, the preflight also found and fixed a separate replay sequencing defect: fill-cooldown cancellation attempted to cancel a fully filled order before list cleanup. `_request_cancel_all()` now ignores orders with less than one lot remaining. That fix was necessary, but it is not the cause of the assignment carryover above.

## Decision

The current campaign-side randomized identity cannot proceed to the 40-day mechanics run. A successor must preregister an assignment/carryover unit that remains stable until every order submitted under that assignment is exchange terminal, or preregister a genuine washout rule. Silently ending an assignment, canceling the inherited order, or transferring it to the new arm would each change the intervention.

F09 remains an active research family, but this ranked-toxicity execution path is blocked at outcome-blind preflight. Prediction, action, Validation, holdout, and live permissions remain false.

The machine-readable record is [`causal_v12_ranked_toxicity_exposure_guard_full_path_mechanics_v1_5_implementation_failure_20260802.json`](causal_v12_ranked_toxicity_exposure_guard_full_path_mechanics_v1_5_implementation_failure_20260802.json).
