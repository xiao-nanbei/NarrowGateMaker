# causal_multichannel_window_boolean_cooldown_duration_v2 strict-native formal execution failure

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Date: 2026-08-11

Machine-readable receipt: `causal_multichannel_window_boolean_cooldown_duration_v2_strict_native_formal_execution_failure_receipt_20260811.json`

## Binding

This failure record binds the unchanged v9 execution amendment:

`research/families/f05_fill_quality_quote_ev/docs/causal_multichannel_window_boolean_cooldown_duration_v2_execution_amendment_v9_20260811.json`

SHA256:

`04c7833677bea6d14d125a91ba5196b3e7cb989c22d1e88dd80951dddd87b34c`

## Formal result

The 41-day `formal_full_support_41d_v9` panel was attempted. No day-level result and no panel were admitted.

| Item | Observed result |
|---|---:|
| Requested days | 41 |
| Admitted days | 0 |
| Admitted panels | 0 |
| Validation read | No |
| Sealed holdout read | No |
| Formal economic result read | No |

For `2026-04-17`, the replay processed 5,058,417 events. The first opportunity had an exact shared prefix before assignment, but all eight treatment arms encountered same-millisecond ambiguity in the treatment suffix. Those arms were retained only as unsupported denominator evidence; their economic point labels were redacted.

After that opportunity, the baseline continuation accumulated queue invalidation and ambiguity. The next assignment therefore failed closed after 654.410 seconds with:

```text
SharedPrefixExecutionError: shared-prefix queue evidence is not exact before assignment: ['exchange_book_queue_invalidated_order_count', 'exchange_book_queue_ambiguous_event_count']
```

The persisted progress file subsequently marked `2026-04-18` as running, but no matching process remained. That entry is stale and is not an admission.

## Identifiability boundary

The historical public trade tape provides millisecond timestamps while the raw book tape contains sub-millisecond events. For a public trade and book changes sharing the same millisecond, their relative order is not identifiable from the retained source.

Consequently, the exact historical strict path is blocked. Resetting cumulative queue-failure counters, inventing a within-millisecond order, or converting unsupported arms into point labels would fabricate strict evidence and is prohibited.

## Scope and permissions

This result closes only the historical 41-day v9 strict-native one-shot label execution. It does not close the multichannel EMA Boolean cooldown research question.

An owner/modelled-queue successor may proceed only under a separate identity. It must permanently state that its labels are modelled-queue evidence, cannot inherit strict queue authority, and cannot be presented as exact historical queue evidence.

Current permissions:

```text
strict_label_execution_eligible=false
exact_queue_policy_eligible=false
nested_oof_under_v9_eligible=false
research_supported=false
action_authorized=false
live_authorized=false
```
