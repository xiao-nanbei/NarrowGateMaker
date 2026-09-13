# Active-order CIF C++ inference parity v1

Last materially modified: 2026-08-05

Status: C++ probability-state update implemented and locally parity-tested; no training, economic, q90-action, live, or baseline authority.

The native kernel now applies the same jointly normalized 100 ms competing-risk update as the Python inference-v1.1 reference for the frozen cause order:

1. `favorable_fill`;
2. `adverse_fill`;
3. `cancel_ack`;
4. `other_terminal`.

It accepts a chronological matrix of cause-specific rates, validates that every edge advances exactly once, and returns interval hazards, no-event probability, survival, cumulative incidence, and a resumable final probability state. C++ does not train a model and does not classify lifecycle events. Python remains the authority for starting a risk spell, partial-fill spell reset, phase transitions, and terminal routing.

The historical lifecycle-v1 implementation and its bound SHA256 remain unchanged. A separate inference-v1.1 removes a floating-point false rejection where a legal finite rate vector could fail because independently rounded `1-sum(hazards)` did not make `fsum` bit-exactly one. v1.1 constructs the probability complement residually without changing the hazard estimand.

The parity suite covers 300 time-varying rate edges, zero-rate intervals, checkpoint/resume, invalid-rate rejection, duplicate/missed edges, monotonicity, and probability-mass conservation. Python/C++ values agree within the frozen `rtol=2e-14`, `atol=2e-15` numerical tolerance. The focused Python and C++ mechanics suite reports 23 passed tests.

This closes the standalone native inference-kernel gap only. Authoritative use still requires an admitted journal-v2 replay tape, a fully bound epoch, a frozen 40-day panel and fill classifier, model/preprocessing artifacts, event and checkpoint lockstep, and live transport. q90 remains action-off.

The machine-readable identity and implementation hashes are in `active_order_competing_risk_cif_cpp_inference_parity_v1_20260805.json`.
