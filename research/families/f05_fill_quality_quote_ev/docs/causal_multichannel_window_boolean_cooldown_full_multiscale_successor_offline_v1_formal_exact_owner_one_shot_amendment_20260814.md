# F05 Offline Full-Multiscale Exact-Owner One-Shot Amendment

Last materially modified: 2026-08-14

Status: `implemented_local_one_day_diagnostic_ready_clean_binding_required`.

## Boundary

This amendment changes only the offline one-shot training-label replay path. It does not modify the active owner policy, live evaluator, live configuration, EC2 runtime, quote price, quote size, inventory limits, BER, P3, q90 action, or reducing-quote behavior. Validation and sealed holdout remain unread, and action/live authority remain false for the offline successor.

## Formal V9 Failure

Formal-v9 was bound to clean commit `db423dfe...6f979` and annotated tag `research/f05/causal-multichannel-window-boolean-cooldown-full-multiscale-successor-offline/formal-duration-contract-v1-20260814`. Its canonical execution-manifest SHA is `a7c7f8b4...56bcd`, and its canonical preflight SHA is `69a0a41e...b001f`. Preflight passed with status `formal_offline_replay_mechanics_ready`.

The first outer fold queued ten BUY outer-train day jobs. All ten entered Python arm simulation and failed with `StudyError` while checking the first B0-equivalent duration path. The exact error was `B0-equivalent predecessor CONTROL_85N path diverged from the admitted exact-owner B0 path before common washout quarantine`; the historical error text still named `CONTROL_85N` because the old fork assumed that action was the universal control.

The root cause was an estimand mismatch. The admitted 3,516-row mechanics panel was generated under the exact current owner policy on both sides, but the one-shot duration executor rebuilt each day from `replay.params` without injecting the exact-owner snapshot emitter and evaluator. That silently reverted all pre-target and continuation cooldown decisions to the predecessor `85s x consecutive units` mechanism. Even a BUY target could therefore inherit a different earlier SELL path. The target fork was not a valid intervention on the admitted B0 state.

The ten progress receipts are all failed outer-train one-shot BUY jobs and bind ten UTC days; their ordered receipt-set SHA is `453686fd...b5d3`. The cache contains zero admitted entries, and formal-v9 has no `formal_result.json`, one-shot label table, fitted policy, outer-test replay, or scorecard. The simulations did compute and inspect assignment-to-washout or censor diagnostics before the parity exception, so this amendment records `development_path_outcomes_computed=true`; it does not repeat the earlier overbroad claim that no economic path was touched. Those intermediate values were discarded, were not exposed as a label artifact, and were not used for feature, duration, complexity, or candidate selection.

## Corrected Estimand

The formal backend now deterministically joins each replay row to its admitted `exact_owner_action`. Every one-shot arm starts with a fresh causal M2 snapshot emitter and a fresh exact-current-owner evaluator. The exact owner policy controls both BUY and SELL cooldown decisions before the target fill and again after the target lineage; only the frozen target opportunity is replaced by the selected duration arm.

The replay kernel accepts this composition only when an explicit offline one-shot baseline-policy flag is present, the evaluator policy SHA equals the bound owner SHA, and its target action equals the row-wise admitted owner action. The target arm has precedence only at the exact ordinal, timestamp, side, order, and campaign identity. A missing emitter/evaluator, malformed SHA, row/action mismatch, or attempted generic repeated-policy/fork combination fails closed.

Legacy predecessor forks retain `multiscale_ema_boolean_cooldown_duration_fork_trace.v2` with their original field set. The exact-owner composition alone emits v3 and binds the exact owner action, owner duration, owner policy SHA, and explicit composition flag. This prevents the v10 repair from changing historical v1/v2 trace bytes or assignment identities.

No-op parity is now row-wise rather than role-wise. The arm whose policy ID equals the admitted exact-owner action must reproduce the exact-owner fill prefix through common washout quarantine. For BUY this is normally `CONTROL_85N`; for SELL it may be `FIXED_166S`, `FIXED_211S`, or `FIXED_1748S`. Other duration arms are interventions and are not incorrectly required to match B0 after assignment.

## One-Day Gate

The formal orchestrator now exposes one fixed `diagnose-one-day` route. It accepts only the clean-tag-bound execution manifest, revalidates the complete source/panel chain, deterministically chooses the first admitted UTC day, and runs with one worker. Every admitted opportunity on that day is replayed only under its row-wise exact owner action. Each fork must reproduce the exact-owner fill prefix through common washout quarantine; the diagnostic cannot accept a day, evaluator, candidate, or custom worker count from the caller.

The immutable receipt contains only opportunity, side, role, action, washout/censor, parity, runtime-identity, and permission counts. The replay engine necessarily computes path values while reaching washout, so the receipt states that fact explicitly, but no economic value is retained, exposed, admitted as a label, or used for selection. Formal nested OOF remains blocked until this one-day receipt passes.

## Verification

The replay-binding, duration-study, adapter, backend, and orchestrator suites pass 130 tests with one documented skip. A synthetic full-path test proves that the exact-owner evaluator chooses `FIXED_1S`, a target fork applies `FIXED_2S` at only the frozen opportunity, the trace retains the owner action and owner duration, the legacy fork remains v2 without exact-owner fields, and the old unbound evaluator-plus-fork combination remains rejected. The broader Boolean-cooldown successor regression passes 691 tests with 39 documented skips. The complete repository suite passes 3,142 tests with 316 documented skips and one joblib physical-core-detection warning; Ruff, public governance, private governance, and `git diff --check` all pass. The machine-readable amendment receipt binds the commands, counts, source SHA256 values, and unchanged permission boundary.

## Next Gate

These exact source and document bytes require a clean append-only commit and annotated tag. Formal-v9 manifests, progress receipts, and cache keys are permanently non-reusable. A fresh formal-v10 manifest and preflight must bind the new tag, the derived row-wise owner action, the exact-owner one-shot ABI, and all prior source/fold/duration contracts. The fixed one-day diagnostic must then pass before the formal nested OOF run may read additional Development paths.

Formal-v9 consumed implementation-level Development path computation on ten outer-train days but produced no admissible economic evidence. Validation and sealed holdout remain unread. The current owner live policy and EC2 runtime are unchanged.
