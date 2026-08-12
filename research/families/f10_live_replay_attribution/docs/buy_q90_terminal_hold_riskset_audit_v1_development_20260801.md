# BUY q90 terminal-hold risk-set audit v1

Last materially modified: 2026-08-12

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Scope

This is a Development mechanics-only F10 identity. It does not read PnL, markout, campaign outcomes, Validation, or sealed holdout data. It cannot authorize F07 v2, an action experiment, a threshold change, a timeout, or live deployment.

The frozen source is the observed 2026-07-25 UTC live q90 slice already used by `buy_q90_causal_visibility_clock_parity_v1_1`. The audit joins q90 shadow rows, action rows, and exchange order outcomes by exact `client_order_id`.

## Result

All 23 implicated BUY orders had exactly one cancel request and one cancel ACK. After ACK, the exchange order was terminal, but the retained tracker continued to synthesize the same order as `PENDING_CANCEL` and score it with the active-order fill-hazard model.

| Metric | Result |
|---|---:|
| Terminal-hold orders | 23 |
| Post-ACK active-order hazard evaluations | 69,074 |
| Post-ACK rows marked valid | 6,579 |
| Post-ACK rows marked invalid | 62,495 |
| Old price above current mid | 62,466 / 62,495 |
| Recovered and re-entered | 18 / 23 |
| Ended the slice in terminal hold | 5 / 23 |

For a BUY order, `order_price > mid` is already a strict sufficient condition for `order_price > best_bid`. Those 62,466 observations cannot be repaired by loading more bid depth. The remaining 29 outside-book rows lack an exact best-bid or snapshot-floor field in the frozen shadow tape and remain explicit unresolved rows; they are not relabeled as depth failures.

The 18 apparent recoveries do not rescue the contract. They were also scored after cancel ACK using the terminal order's old active-risk state. They are therefore invalid recovery evidence even though they produced a re-entry.

## State contract

The lifecycle must distinguish three states:

1. `exchange_order_terminal`: ACK has removed all remaining exchange fill risk.
2. `q90_hold_terminal`: the policy permission hold may remain after the order terminates.
3. `post_cancel_recovery_state`: recovery must be evaluated by a separately frozen estimand that does not pretend the canceled order remains active.

An active-order fill-hazard observation is permitted only while the exchange order is truly `OPEN`, `PARTIALLY_FILLED`, or `PENDING_CANCEL`. Cancel ACK is a hard risk-set boundary.

## Decision

`terminal_active_riskset_contract_failed_post_cancel_recovery_undefined`

This identifies a state-machine and estimand defect, not a depth-capacity problem. The current identity does not invent a recovery timeout or a new market-state rule because either would change the live action. The inherited parity thresholds remain unchanged:

- valid-fraction absolute delta <= 0.05;
- cancel-role total variation <= 0.15.

They are not rerun until a separately frozen post-cancel recovery contract and Python/C++ implementation exist. Exact AWS deep-event capture remains a final transport gate, not a prerequisite for this 23-order audit.

This blocker is separate from the 240-hour loss attribution. It neither explains nor replaces the evidence about asymmetric BUY filtering, multi-level SHORT inventory, order aging, or adverse selection.

## Artifacts

- Frozen spec: `research/families/f10_live_replay_attribution/docs/buy_q90_terminal_hold_riskset_audit_v1_spec_20260801.json`
- Authoritative report: `${NARROWGATE_DATA_ROOT}/reports/buy_q90_terminal_hold_riskset_audit_v1_20260801/development/report.json`
- Complete lifecycle journal: `${NARROWGATE_DATA_ROOT}/reports/buy_q90_terminal_hold_riskset_audit_v1_20260801/development/lifecycle_journal.csv`
- Full invalid observations: `${NARROWGATE_DATA_ROOT}/reports/buy_q90_terminal_hold_riskset_audit_v1_20260801/development/invalid_observations.parquet`
- Artifact manifest: `${NARROWGATE_DATA_ROOT}/reports/buy_q90_terminal_hold_riskset_audit_v1_20260801/development/manifest.json`
