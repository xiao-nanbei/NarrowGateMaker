# Volatility-Time Add Rearm Feasibility v2.1 Errata

Last materially modified: 2026-07-29

This note narrows the interpretation of the frozen v2.1 evidence. It does not modify the frozen Spec, report, manifest, gates, or result.

- The frozen field `candidate_effective_rate` means `mechanical_effective_rate`: the fraction of baseline episodes with `abs(candidate_rearm_time - baseline_rearm_time) > 5s`. It is not the fraction of final quote actions changed after all other blockers.
- The reported zero Python/C++ mismatch is isolated variance-clock integrator parity on frozen baseline episodes. It is not full strategy-path parity.
- v2.1 uses daily fresh-start state. It does not restore the preceding UTC day's consecutive fill units or active cooldown deadline, so it has no continuous-live lineage authority.
- v2.1 reads D-1 BBO warmup data but did not freeze those file identities. The successor live-stack identity binds all 40 target-day and 40 D-1 BBO files.
- The three predecessor blocker booleans are diagnostic placeholders, not promotion evidence. The successor replaces them with hash-bound evidence cells and explicit failure reasons.

The authoritative successor mechanics result is [`volatility_time_add_rearm_live_stack_parity_v1`](volatility_time_add_rearm_live_stack_parity_v1_development_20260729.md). Reward, PnL, markout, Validation, and sealed holdout remain unread. No action experiment or live permission was created.
