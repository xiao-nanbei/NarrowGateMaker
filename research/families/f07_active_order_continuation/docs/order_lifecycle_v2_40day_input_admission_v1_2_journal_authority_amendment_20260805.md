# F07 Journal-v2 Authority Execution Amendment v1.2

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

## Status

`execution_successor_frozen_pending_40day_journal_v2_admission`

This successor corrects one authority error in the frozen `order_lifecycle_v2_40day_input_admission_v1`: an unrecoverable legacy trace clock cannot remain a hard blocker after an authoritative dual-clock journal-v2 has been regenerated from the replay lifecycle.

The original v1 JSON, Markdown, denominator, and historical result remain unchanged. The machine-readable amendment is `order_lifecycle_v2_40day_input_admission_v1_2_journal_authority_amendment_20260805.json`, canonical SHA256 `bd2216e84281a99049cbd8c04ede798101179ae37b3a4272a5d584ec2d94f254`.

## Authority

The 40-day mechanics evidence now has one explicit authority chain:

```text
frozen v1 ordered 40-day denominator and source identities
  -> authoritative journal-v2 parts and writer health
  -> Python/C++ per-event event-stream lockstep
  -> formal 40-day mechanics report
```

The legacy trace is `diagnostic_reconciliation_only`. If available, the successor reports its schema, clock semantics, row count, and order-identity agreement. A missing trace, a single event clock, mixed clock coverage, a bad legacy hash, or a journal/legacy identity mismatch cannot change `lockstep_execution_eligible` and cannot grant mechanics authority.

This is not clock relaxation. Journal-v2 remains fail-closed:

- every event has a positive visibility timestamp;
- activation, cancel reject, partial/full fill, and exchange terminal events have valid exchange and source-callback exchange timestamps;
- exchange time is not after visibility time;
- after the first exchange-risk event, exchange-time quantity exposure is valid and reportable;
- pre-activation submit rows may correctly have a null exchange exposure value while remaining `valid=true, complete=false`.

## Hard Gates

All 40 ordered target days must independently pass:

1. Frozen v1 day order, D-1 warmup, target interval, source hashes, baseline, model, P3, Feature DAG, execution ABI, and runtime identity.
2. Journal-v2 schema, content-addressed Parquet parts, cursor chain, unique event/lifecycle identities, and one final terminal or censor per order.
3. Writer close/flush, atomic admission, zero drops, zero errors, row/callback agreement, and no partial or orphan payloads.
4. Explicit journal-v2 dual-clock and exchange-time exposure coverage.
5. Cancel-reject route support and journal-observed count agreement.
6. Exact terminal zero remainder under `order_lifecycle_terminal_remainder_zero_abs_1e-12.v1`; a positive sub-lot remainder is not a full fill.
7. A hash-bound C++ event-stream binding to the same journal schema, projection schema, quantity contract, and both cancel-reject continuation branches.
8. Daily fresh-start replay scope with no carry-in or left truncation.

The frozen v1 `current_producer_status` is a historical snapshot and is not an execution gate in this successor. Actual admitted day artifacts provide the evidence.

## Eligibility Semantics

`lockstep_execution_eligible=true` means only that the 40-day journal inputs are eligible to enter the formal Python/C++ event-stream lockstep.

It does not mean the lockstep has run. `mechanics_authority_eligible` remains false until the resulting formal 40-day lockstep report is executed and hash-bound. Daily fresh-start mechanics also do not authorize prospective live epoch transport or cross-midnight carry-in semantics.

## Verification

The successor tests cover:

- complete 40-day admission;
- missing legacy traces;
- 40 days of single-clock legacy traces while historical v1 still fails;
- invalid legacy artifacts;
- journal dual-clock and exposure failures;
- writer errors;
- cancel-reject capability mismatch;
- terminal sub-lot mismatch;
- C++ event-stream binding mismatch;
- unchanged v1 contract bytes and successor permissions.

The implementation and its tests use the repository `.venv` Python. No formal 40-day replay, CIF training, economic result, q90 action, live transport, or deployment was run by this amendment.

## Permissions

```text
formal_40day_lockstep = false
cif_training = false
economic_evaluation = false
q90_action = false
prospective_live_epoch_transport = false
live_deployment = false
```
