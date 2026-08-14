# F05 Formal V12 Fill-Trace Parity Amendment

Last materially modified: 2026-08-14

Status: `formal_v12_preflight_passed_one_day_blocked_asymmetric_fill_trace_capture_fixed_locally_v13_clean_binding_pending`.

Evidence availability: this Markdown report and its repository-relative JSON receipt are public. The formal-v12 execution manifest, preflight receipt, admitted mechanics panel, owner policy, predicate bundle, private execution configuration, market inputs, and transient diagnostic state are retained in the owner-private evidence store and are not distributed with the public repository; published SHA256 values are integrity metadata rather than reader-facing locators.

## Formal V12 Result

Formal-v12 bound the exact-owner artifact-projection amendment to clean commit `b50da75dbe776ba2f8c99cb33288be14fa874b17` and its annotated tag. Preflight returned `formal_offline_replay_mechanics_ready` with zero missing fields. The fixed first admitted day, `2026-06-27`, then failed closed at the first exact-owner no-op fill-prefix comparison with `B0-equivalent fork diverged before the common washout quarantine` before a one-day receipt could be admitted.

No one-day receipt, label cache, candidate policy, outer-test result, scorecard, Validation result, or sealed-holdout result was admitted. The failed replay computed transient path state inside the process only; no economic value was persisted, exposed, or used for model, predicate, threshold, duration, complexity, or promotion selection.

## Root Cause

The one-day gate enabled `trace_fills_max` for the authoritative exact-owner control but omitted the same trace setting from every exact-owner fork. The parity check therefore compared the populated control fill prefix with an empty fork fill prefix even when both arms used the same admitted owner action. The error was in the gate's observability input, not in the owner action, cooldown deadline, quote path, continuation policy, queue model, or washout contract.

The formal one-shot training path already enabled the fork fill trace whenever exact-owner parity was required. The omission was isolated to `_execute_exact_owner_one_day_mechanics`, which had duplicated the arm setup without that symmetric trace binding.

## Fix

The one-day gate now creates each fork runtime independently, then binds `trace_fills_max` to the same frozen `TRACE_LIMIT` used by the authoritative control before calling the duration arm. A regression test exercises the one-day adapter with a disabled default trace and proves that every parity fork receives the frozen trace limit. The underlying parity assertion remains fail-closed.

The parity error now reports the cutoff, both fill counts, first differing index, and the first control/fork fill tuple. This is diagnostic-only and does not weaken equality, alter simulation behavior, persist economic labels, or expand the research denominator.

## Verification And Boundary

The replay-adapter, formal backend, orchestrator, owner-runtime, and duration-study directed suite completed with 132 passed and one historical skip; Ruff passed for all modified source and test files. The active owner policy, predicate bundle, private live configuration, panel bytes, 30 selected dates, 3,516 opportunity IDs, duration vocabulary, candidate ladder, queue identity, ambiguity-censoring rule, live runtime, and EC2 state remain unchanged.

Formal-v12 remains immutable and cannot be resumed because its clean tag contains the asymmetric one-day trace gate. Action and live authority remain false for this successor. Validation and sealed holdout remain unread. The Unknown ACK lifecycle amendment remains independent and `implemented_local_predeploy_blocked`; this offline correction does not authorize a lifecycle deployment.

## Next Gate

Create a clean commit and annotated tag for this amendment, bind the unchanged exact-owner bridge v3 mechanics admission to a new formal-v13 manifest, rerun preflight, and rerun the fixed first-day one-worker exact-owner no-op gate. Formal nested OOF may begin only after the v13 one-day receipt is atomically admitted with all row-wise owner actions and fill prefixes matching.
