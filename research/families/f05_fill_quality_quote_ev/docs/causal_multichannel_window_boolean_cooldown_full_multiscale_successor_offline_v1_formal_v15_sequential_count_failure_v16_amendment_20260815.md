# F05 Formal-v15 Sequential Count Failure and Formal-v16 Amendment

Last materially modified: 2026-08-15

Status: `formal_v15_fail_closed_sequential_count_schema_no_final_evidence_v16_fix_frozen_pending_clean_tag`.

## Question

Formal-v15 completed the first outer-train one-shot label batch and then entered repeated sequential-policy evaluation. This amendment asks whether the stop was an economic result, a malformed replay result, or a structural aggregation defect, and freezes the minimum correction before any replacement execution.

## Formal-v15 Boundary

Formal-v15 completed all ten `outer1 / BUY` outer-train one-shot day entries: 809 opportunities and 6,472 duration arms, with ten complete progress receipts. It then atomically admitted two `outer1.inner1 / BUY` sequential day rows for 2026-07-11 and 2026-07-12. The first row contained `consecutive_units_count::6=1`; the second day had no assignment in that category and therefore omitted that category column. The v15 day concatenation took the union of columns only after concatenation, so pandas represented the structurally absent second-day category as `NaN`. The unchanged formal validator rejected it with `evaluation count 'consecutive_units_count::6' is invalid`.

This is a schema-normalization failure, not a negative or inconclusive economic estimate. Twelve atomic cache entries exist under the v15 execution identity: ten one-shot entries and two inner-OOF sequential entries. All twelve progress receipts are complete, but no inner-fold result returned to the learner, no candidate was frozen, no outer-test economics ran, and no scorecard or final report was admitted. The private cache-failure audit is owner evidence and is not distributed with the public repository.

## Formal-v16 Correction

Formal-v16 may normalize only category-count columns whose names begin with the frozen `action_count::`, `role_count::`, `consecutive_units_count::`, or `fallback_count::` prefixes. Before day concatenation it must compute the union of those category columns, validate every count actually emitted by a day as present, numeric, nonnegative, and integral, and add a typed integer zero only when the entire category column is structurally absent from that day. It must not fill an emitted `NaN`, negative value, fractional value, scalar count, economic value, or unsupported outcome. The existing post-concatenation count-group conservation checks remain authoritative.

The correction changes no market input, day, opportunity, fold, feature, predicate, duration, candidate ladder, exact-owner baseline, repeated-policy semantics, queue model, lifecycle rule, statistic, scorecard, or permission. Formal-v16 must use a clean commit and annotated tag, a fresh execution manifest and cache identity, passing formal preflight, and the fixed 81-opportunity one-day exact-owner mechanics gate before nested OOF may restart. No formal-v15 cache entry may be reused by formal-v16.

## Verification

The focused replay-adapter suite passes 61 tests. The broader F05 full-multiscale, shared-prefix, lifecycle-emitter suite passes 293 tests with 35 explicit skips, and Ruff passes the changed implementation and test. Regression coverage proves both sides of the contract: structurally absent categories become integer zero, while emitted `NaN`, negative, and fractional counts remain rejected.

## Permissions

Formal-v15 has no research conclusion and formal-v16 has not yet read economic results. Validation and sealed holdout remain unread. Action and live authority remain false. The active owner policy, private live configuration, runtime, quote prices, order sizes, BER, P3, q90 action, inventory limits, and lifecycle behavior are unchanged. No F05 shadow, companion, observer, writer, journal, feature dump, candidate telemetry, deployment, restart, or live configuration change was created.
