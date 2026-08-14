# F05 Offline Fill-Trace Rebinding V4 Admission

Last materially modified: 2026-08-14

Status: `sequential_mechanics_fill_trace_rebinding_v4_admitted_pre_economic`.

Evidence availability: this Markdown report and its repository-relative JSON receipt are public. The builder manifests, portable replay binding, admitted mechanics manifest, panel tables, source receipts, owner policy, predicate bundle, private configuration, and underlying market data are retained in the owner-private evidence store and are not distributed with the public repository; their SHA256 values are public integrity metadata only.

## Result

The frozen 30-day offline Development denominator was rebuilt after the formal-v12 diagnosis showed that exact-owner control enabled fill tracing while the one-shot duration fork did not. The replay adapter now applies the same frozen trace limit to both arms before executing either path. This repair changes diagnostic observability only; the strict path-equality gate remains unchanged.

All five v4 tables contain the same 3,516 opportunities in the same order as exact-owner bridge v3. The metadata, Boolean-feature, continuous-feature, and exact-owner-action files are byte-identical to v3. Only `replay_inputs` changed, from SHA256 `c579fccf99bb77074ebe24018409dfe7e4af126818ceb3697077b8953f9b8cbf` to `e49739a5c6d650223630597caa0c47f9ca8196538f19f6806d507f062cd171d8`, because every row now binds the corrected portable replay implementation and its new SHA.

The panel builder completed 30 of 30 dates with zero replacement dates and zero failures. Its independent validator passed. Mechanics admission then revalidated the canonical source, selected-day receipts, normalized book view, all five table byte hashes and schemas, common ordered opportunity identity, active owner policy, predicate bundle, private execution configuration, and portable replay binding. The admitted mechanics canonical SHA is `49f16505e3e2f5d900c0125c8622574d82a9477d5dff3082ad58095929e2e9a0`.

## Evidence Boundary

This materialization read no counterfactual label, PnL, terminal value, Validation result, or sealed-holdout result and generated no candidate action. It grants neither action nor live authority and does not change the active owner policy, live configuration, or EC2 runtime. Exact-owner bridge v3 and formal-v12 remain immutable historical records.

Formal economics remains locked. A new clean commit and annotated tag must first bind this admission; formal-v13 must then pass schema/provenance preflight and the fixed first-day single-worker exact-owner mechanics gate before nested chronological OOF may begin.

## Verification

The builder validator and mechanics manifest validator both exited successfully. All five v4 files have 3,516 rows and the same row-key SHA256 `e481d8a61eecb71a36e6e3c8f2be1630c483a466a5418adc583d6398c29330ec`. The four non-replay files are byte-identical to v3. A structured replay-input comparison found identical columns and opportunity order; the only differing columns were `portable_replay_binding_path` and `portable_replay_binding_sha256` on all 3,516 rows.

The Unknown ACK lifecycle amendment remains independent and `implemented_local_predeploy_blocked`. No EC2 deployment, restart, configuration change, companion, observer, or shadow was created by this admission.

## Next Gate

Commit and tag this admission from a clean worktree, bind formal-v13 to that exact tag and mechanics canonical SHA, run formal preflight, and execute the fixed first day with one worker. Only an admitted `exact_owner_one_day_mechanics_complete` receipt may unlock nested chronological OOF through the canonical orchestrator.
