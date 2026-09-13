# Multiscale EMA Boolean Cooldown Duration Policy v1: Execution Semantics Errata

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

This errata does not change the frozen research question, candidate durations, EMA predicate universe, Development dates, or model-selection contract. It records the execution semantics required by the final implementation identity `ff4873a18b0f77d2ddd4bbe19bddc18d66a5ff3d3f628d76bc538b0f2bb9eb40`.

## Single-action label boundary

Each duration fork follows the current baseline until the target exposure-increasing fill, changes only that cooldown lineage, and then follows the frozen continuation policy. Once a fork first returns to flat, it enters `first_flat_exposure_quarantine_scheduler_drained_v2` so the label cannot absorb a second exposure campaign while another duration arm is still washing out.

The quarantine is an attribution device for single-action labels. It is not a candidate live mechanism and must not appear in the learned-policy full-path replay. There is no synthetic immediate cancellation: existing submit/cancel/ACK events are drained through the normal scheduler.

## Terminal clock

A fork becomes terminal at the current replay-visible market event only after all descendant orders, queue cursors, hazards, pending submits, and pending cancel ACKs have ended. A quarantine timestamp cannot backdate terminal status when an exchange event remains pending.

The formal fill clock is `native_exchange_event_revealed_at_replay_event_clock_no_live_receive_time_claim`. This study does not claim exact live receive-time authority.

## Economic authority

The 8,600-opportunity census, 68,800-arm denominator, control no-op attestation, and 32-arm Python/C++ subset establish execution eligibility only. Development economic outcomes remain unread at this amendment. After all formal arms are admitted, the economic read and nested chronological OOF training must be explicit.

Even a successful OOF model cannot authorize an action. BUY and SELL must separately pass a successor continuous full-path policy replay with no label quarantine before either side can be considered for live deployment.
