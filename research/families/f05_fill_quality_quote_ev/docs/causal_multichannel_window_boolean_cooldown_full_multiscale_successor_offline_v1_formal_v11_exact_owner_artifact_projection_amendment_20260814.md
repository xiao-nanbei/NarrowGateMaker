# F05 Formal V11 Exact-Owner Artifact Projection Amendment

Last materially modified: 2026-08-14

Status: `formal_v11_preflight_passed_one_day_blocked_exact_owner_artifact_projection_fixed_locally_v12_clean_binding_pending`.

Evidence availability: this Markdown report and its repository-relative JSON receipt are public. The formal-v11 execution manifest, preflight receipt, admitted mechanics panel, owner policy, predicate bundle, private execution configuration, market inputs, and transient diagnostic state are retained in the owner-private evidence store and are not distributed with the public repository; the published SHA256 values are integrity metadata rather than reader-facing locators.

## Formal V11 Result

Formal-v11 bound the exact-owner bridge v3 mechanics admission to clean commit `25444f8d439c077a34fd2993a8ef0d8cd009aa05` and its annotated tag. Preflight returned `formal_offline_replay_mechanics_ready` with zero missing fields. The fixed first admitted day, `2026-06-27`, was then run with one worker and failed closed before a one-day receipt could be admitted: the first aligned SELL opener expected the panel's `FIXED_211S`, while formal replay's supposed exact-owner evaluator returned `CONTROL_85N` for the same fill, order, campaign, and 85,000-millisecond baseline.

No one-day receipt, label cache, candidate policy, outer-test result, scorecard, Validation result, or sealed-holdout result was admitted. The failed process transiently reconstructed implementation path state only; those values were neither persisted as evidence nor used for model, predicate, threshold, duration, complexity, or promotion selection.

## Root Cause

The panel builder and formal replay did not use the same exact-owner evaluator. The panel builder loaded the hash-bound owner policy and frozen 2025 predicate bundle through the artifact-aware runtime evaluator, which transforms the full cumulative-M2 snapshot into the exact predicate columns consumed by the active owner rule. Formal replay instead used the generic research evaluator, which expects those selected predicate columns to have already been materialized directly in the feature row. The admitted Boolean panel intentionally stores the complete multichannel predicate universe rather than those legacy selected-column aliases, so the generic evaluator treated the owner rule as unsupported and fell back to `CONTROL_85N`.

The prior 54-case tri-state parity suite did not reveal this mismatch because it compared already-projected predicate rows. It established Boolean decision parity after projection, not parity of the full snapshot-to-artifact-predicate projection boundary.

## Fix

Formal replay now builds every exact-B0 delegate through the same artifact-aware runtime policy loader used by the panel builder. The wrapper binds the exact private owner policy and predicate-bundle SHA256 identities, validates every expected snapshot identity hash before evaluation, delegates full-snapshot predicate transformation to the runtime evaluator, and verifies the returned decision retains the exact owner artifact identities. This applies to one-shot control, row-wise owner no-op arms, sequential control, exact-owner candidate identity, opposite-side B0 fallback, fixed-policy fallback, and D+1 B0 fallback.

The formal fixed API remains non-injectable. No panel bytes, opportunity IDs, source days, owner actions, candidate ladder, duration vocabulary, queue identity, ambiguity-censoring rule, live evaluator, live configuration, or EC2 runtime changed. The active owner policy remains byte-for-byte unchanged.

## Verification And Boundary

A new regression test constructs a real artifact-bound runtime policy whose selected literal is absent from the raw snapshot row, verifies that formal B0 performs the artifact projection and chooses `FIXED_211S`, and verifies snapshot identity drift fails closed. The complete directed F05 offline and runtime-policy suite completed with 164 passed; Ruff passed for the modified replay adapter and regression test.

Formal-v11 remains immutable and cannot be resumed because its clean tag contains the incorrect evaluator bridge. Action and live authority remain false for this successor. Validation and sealed holdout remain unread. The Unknown ACK lifecycle amendment remains independent and `implemented_local_predeploy_blocked`; this offline correction does not authorize any lifecycle deployment.

## Next Gate

Create a clean commit and annotated tag for this amendment, bind the unchanged exact-owner bridge v3 mechanics admission to a new formal-v12 manifest, rerun preflight, and rerun the fixed first-day one-worker exact-owner no-op gate. Formal OOF may begin only after that one-day receipt is atomically admitted with row-wise owner-action parity and no new implementation blocker.
