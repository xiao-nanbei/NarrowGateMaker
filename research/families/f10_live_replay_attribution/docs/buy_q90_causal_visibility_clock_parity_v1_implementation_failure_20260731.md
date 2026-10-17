# BUY q90 causal visibility clock parity v1: implementation failure

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

## Status

`buy_q90_causal_visibility_clock_parity_v1` is withdrawn as an implementation failure. It produced no authoritative final report and grants no prediction, transport, F07, action, Validation, holdout, or live authority.

The frozen Spec remains unchanged with canonical SHA256 `1420a13475531c76042bf77fd900891915a2c0738726a1c71ca38ef06d302811`.

## Failure

The three shadow modes completed, but the corrected stateful apply mode stopped at its first valid Python/C++ evaluation. Discrete lifecycle identity, action, feature source time, feature-ready time, generation, queue counters, and book values matched. The four predicted probabilities differed by roughly 0.5%-0.7%, so strict lockstep correctly failed.

Root cause: Python formed `visible_state_age_ms` from the last feature-ready timestamp, while C++ still formed the active-order path age from the provider source timestamp. The dual-clock scheduler repair exposed this previously hidden continuous-feature mismatch.

## Repair boundary

The C++ path age now uses `feature_ready_ts_ns`. A regression test separates source and ready time by two seconds and requires the native model to observe a 100ms visible-state age. The repaired run must use a new implementation identity (`v1_1`); this frozen failure is not rewritten or reinterpreted.

The completed shadow diagnostics are contextual only. In particular, the provider-clock shadow changed valid q90 evaluations from `3,896 / 775,192` to `775,135 / 775,185`, strongly locating the old weak-treatment result in the mixed-clock risk-set defect, but it is not a completed parity result.
