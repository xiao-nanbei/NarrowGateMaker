# SELL First-Fill Conditional Value V2 Invalidation

Last materially modified: 2026-07-30

## Decision

`sell_first_fill_conditional_value_feasibility_v2` is invalidated before any Development outcome fit or inspection. Validation, sealed holdout, action, and live permissions remain false.

## Cause

The frozen 24A/16B denominator contained seven target days whose previous natural UTC day was not formal eligible in `normalized_l2_100ms_v2`. Formal native replay requires both the target and D-1 context day; using an older snapshot, target-day bootstrap, or a non-formal warmup would change causal queue identity.

The excluded targets are `2026-04-17`, `2026-04-22`, `2026-05-01`, `2026-05-13`, `2026-05-29`, `2026-06-02`, and `2026-06-05`.

## Execution Defect

The V2 parallel producer submitted all days before collecting results. A worker raised the first formal-L2 error, but `ProcessPoolExecutor` waited for the rest of the queue before surfacing it. No formal checkpoint or combined trace was admitted. V3 adds an outcome-blind all-day target plus D-1 preflight and uses a process pool that terminates queued work on the first worker exception.

## Successor

V3 uses the maximum currently available strict universe before the existing embargo: 22 Grade-A and 11 Grade-B days. The estimand, model features, SELL primary side, Grade-B sensitivity role, expected 13/6 OOF days, and economic gates are unchanged.
