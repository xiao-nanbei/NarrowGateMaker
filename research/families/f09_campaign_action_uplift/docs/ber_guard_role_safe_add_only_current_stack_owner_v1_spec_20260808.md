# BER Guard Role-Safe Add-Only Current Stack Owner v1

Last materially modified: 2026-08-08

Status: Outcome-informed owner Development screen registered; candidate outcomes unread.

## Question

The experiment preserves the current BER signal, threshold `1.2`, and spread multiplier `2.0`. It changes only the inventory roles to which BER widening is applied.

- Control: current global, bilateral BER mapping.
- Candidate: BER protects only the exposure-increasing add side.
- Flat opener and reducing quotes use the exact BER-bypass quote.

This is a mechanics- and outcome-informed successor to the completed BER retirement experiment. It is not independent confirmation, it does not search any BER constant, and it can never be relabeled as research-supported. Any future promotion permanently retains the owner-risk-accepted label.

## Exact Composition

Before quote composition, live, Python replay, and C++ replay must produce the same BER input, EMA values, ratio, readiness, and active state. The authority is the current live clock: the latest completed 10-second feature value is held and sampled by each completed 1-second bar callback. The slow-EMA ready threshold is `1e-6` in all three paths.

At one immutable quote-decision snapshot, replay computes the current global BER quote and the same quote with `ber_active=false`.

- A quote role is determined from decision-pre inventory and its target quantity.
- Positive inventory: a non-crossing BUY add comes from global BER; a SELL reducing quote comes from bypass.
- Negative inventory: a non-crossing SELL add comes from global BER; a BUY reducing quote comes from bypass.
- Inventory within `1e-10 BTC` of flat: both opener sides come from bypass.
- A quantity that crosses flat is `mixed_cross_zero` and remains control.
- When BER is inactive, both arms are identical.

If the mixed pair reaches the existing spread cap, the implementation may move only the add side. If that cannot preserve the exact bypass price of the opener or reducing side, the role-safe composition becomes a no-op for that decision.

Normal replace/cancel/queue consequences of a changed effective price belong to the treatment. No cancel policy, requote clock, cooldown, quantity, P3, ML, q90, or lifecycle rule is changed directly.

The effective-change denominator is the set of canonical requote timestamps present in both arms, with two side rows per timestamp. Any control/candidate `n_requotes` mismatch fails closed; the denominator is never reconstructed from the treatment-dependent fill or inventory path.

## Progression

One native day must first pass role semantics and Python/C++ full-path parity. Only then may the remaining frozen 40-day Development panel be read. A complete Development pass permits only restart-aware continuous owner confirmation; daily fresh-start evidence cannot directly authorize live.

The machine-readable contract is `ber_guard_role_safe_add_only_current_stack_owner_v1_spec_20260808.json`.
