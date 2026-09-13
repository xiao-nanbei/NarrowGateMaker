# F07 Active Order Continuation

Last materially modified: 2026-08-08

Documentation boundary: this README and the unit's tracked `docs/` are public. Owner-only artifact locators, unpublished evidence indexes, and private research context are resolved through this unit's ignored local `private/` catalog and are not distributed with the public repository. See the [public/private research layout](../../PRIVATE_EVIDENCE.md).

Status: keep/cancel, net-hazard, and dynamic-fill M0 research families are closed. Current deployment and collection state is private operational evidence and is not attested by this public README.

The old homogeneous `queue_value_competing_risk` CLI is source-read-only: no complete frozen reproduction contract exists, so it fails before reading data.

A private historical diagnostic found an observational association between order age and weaker approximate 10-second fill value. Because active age is selected by the realized market and baseline policy, that association is not KEEP versus CANCEL/REPLACE uplift and does not reopen this family.

F10 has now completed the fixed-policy [`buy_q90_portfolio_path_attribution_v1`](../f10_live_replay_attribution/docs/buy_q90_portfolio_path_attribution_v1_development_20260731.md) Development replay. Only the SELL-minus-BUY exposure-imbalance link passed; SHORT-share, multi-level-SHORT-share and terminal-harm evidence did not. The historical replay q90 action rate was also far below the current live diagnostic rate, so the result has no live keep/remove or rollback authority and does not reopen the closed F07 action families.

The subsequent F10 [`buy_q90_terminal_hold_riskset_audit_v1`](../f10_live_replay_attribution/docs/buy_q90_terminal_hold_riskset_audit_v1_development_20260801.md) found a stricter blocker: cancel ACK did not end the active-order fill-hazard risk set. The 18 apparent recoveries and five unresolved holds all depend on a terminal order being synthesized as `PENDING_CANCEL`.

The latest baseline-integrity implementation [`buy_q90_fresh_prospective_placement_recovery_v4`](../f10_live_replay_attribution/docs/buy_q90_fresh_prospective_placement_recovery_v4_implementation_20260802.md) builds on the dual-clock terminal-routing v2 contract. The old fill-risk set ends at exchange terminal, the exact-level cursor is removed, and both exchange-clock and strategy-visible remaining-quantity exposure are recorded in BTC*s. Only cancel ACK with positive remaining quantity enters fresh prospective recovery, which now uses the current BUY price, age zero, current causal book, fresh queue-at-tail, and current GTX support. Full fill cannot re-enter; reject/expiry, shutdown, and unknown outcomes retain separate fail-safe routes. F07 v2 remains blocked until the original 40-day lockstep and AWS transport gates pass without relaxed thresholds. The preceding v1-v3 implementation identities remain frozen.

The authoritative v1.6 40-day mechanics chain is now complete. Across 665,831 exact-native eligible lifecycle spells, Python/C++ event lockstep had zero transition mismatches and zero post-terminal hazard/queue reuse. The trained 100ms competing-risk CIF contains 120 cells; C++ inference differs from Python by at most `1.11e-16`, with zero checkpoint-resume difference. See [`active_order_lifecycle_cif_100ms_v1_6_40day_completion_20260808.md`](docs/active_order_lifecycle_cif_100ms_v1_6_40day_completion_20260808.md). This is mechanics evidence only. Prospective live transport evidence, epoch/session identity, and raw telemetry are owner-private and `private_not_distributed`; their absence grants no action or live authority.

The duplicate-activation producer fix is implemented locally and covered by targeted tests, but the failed prospective evidence remains immutable. A new fully bound epoch with exact feature visibility must pass the original gates before any independent economic replay. q90 action remains OFF and no economic outcome was read.

The exact deployment disposition and any collection state are private and not distributed. No public artifact grants q90 action or economic authority.

`audit/` owns active-order value, dynamic hazard, and queue-reactive models. Runtime policy adapters remain under `strategy/`. Shared dependencies: D, R, S, G.
