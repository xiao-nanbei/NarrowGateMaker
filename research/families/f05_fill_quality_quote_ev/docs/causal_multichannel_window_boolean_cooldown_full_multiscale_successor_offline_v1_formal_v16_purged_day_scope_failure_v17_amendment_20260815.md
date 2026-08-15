# F05 Formal-v16 Purged-Day Scope Failure and Formal-v17 Amendment

Last materially modified: 2026-08-15

Status: `formal_v16_fail_closed_outer2_purged_day_scope_no_final_evidence_v17_fix_frozen_pending_clean_tag`.

## Question

Formal-v16 completed the complete `outer1 / BUY` nested learning and outer-test replay sequence, then stopped while materializing `outer2 / BUY` outer-train labels. This amendment asks whether the stop was an economic result, a source-day loss, or an overly strict replay-input assertion, and freezes the minimum correction before any replacement execution.

## Formal-v16 Boundary

Formal-v16 completed ten `outer1 / BUY` one-shot day entries covering 809 opportunities and 6,472 duration arms, 25 inner-OOF day-policy entries covering 15 fold-specific policies, and 65 outer-OOF day-policy entries covering all 13 frozen candidate and comparator identities. All 100 atomic progress receipts are complete. The backend then moved to `outer2 / BUY` before it could return a complete nested-OOF result to the formal orchestrator.

The `outer2 / BUY` nominal training set contains 15 UTC days through 2026-07-21, while its untouched outer test starts on 2026-07-22. The washout-aware purge correctly removed all 58 BUY opportunities from 2026-07-21 because their common observation endpoints crossed the first outer-test assignment boundary. The provider therefore passed a non-empty purged row set covering 14 of the 15 nominal training days. The replay adapter incorrectly required the row set to cover every nominal day and raised `replay input day scope drifted` before generating any `outer2` label.

This is a fold-scope validation defect, not an economic gate result or missing canonical source day. Formal-v16 did read Development outcomes within `outer1`, and its learning algorithm froze and evaluated fold-specific policies there, but no complete nested result returned, no scorecard or formal result was admitted, and no economic or promotion conclusion is available. Intermediate economic values were not inspected or used to alter the successor search, action vocabulary, model complexity, statistics, or gates.

## Formal-v17 Correction

Formal-v17 may allow a non-empty subset of nominal days only for provider-owned outer-train one-shot inputs after the unchanged washout purge. Every observed row day must remain inside the nominal outer-train day set, row identities and the purged request hash must match exactly, and an unexpected day remains fail-closed. Inner- and outer-OOF sequential replay, one-day mechanics, and general preflight retain exact requested-day coverage.

The correction changes no source, panel, opportunity, purge rule, observation endpoint, fold boundary, feature, predicate, duration, candidate ladder, exact-owner control, replay economics, statistic, scorecard, or permission. Formal-v17 requires a clean commit and annotated tag, a fresh execution manifest and cache identity, passing formal preflight, and the fixed one-day exact-owner mechanics gate before nested OOF may restart. Formal-v17 must not reuse formal-v16 cache entries.

## Verification

The focused replay-adapter suite passes 63 tests. The complete full-multiscale successor test family passes 269 tests, and Ruff passes the changed implementation and test. Regression coverage proves that a purge-created nominal-day subset is accepted only in the outer-train label path, while the default exact-day path and unexpected-day rejection remain unchanged.

## Permissions

Validation and sealed holdout remain unread. Action and live authority remain false. The active owner policy, private live configuration, runtime, quote prices, order sizes, BER, P3, q90 action, inventory limits, and lifecycle behavior are unchanged. No F05 shadow, companion, observer, writer, journal, feature dump, candidate telemetry, deployment, restart, or live configuration change was created.
