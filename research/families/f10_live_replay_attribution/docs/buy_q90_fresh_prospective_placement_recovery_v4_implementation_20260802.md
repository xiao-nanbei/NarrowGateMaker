# BUY q90 Fresh Prospective Placement Recovery v4

Last materially modified: 2026-08-02

## Scope

This successor identity implements the missing post-cancel mechanics for the BUY q90 baseline-integrity line. It does not change the q90 model, threshold, score, or action authority. q90 remains shadow ON and action OFF.

No PnL, markout, campaign reward, Validation, or sealed holdout was read.

## Fresh Recovery Contract

Fresh recovery is accepted only after `PROSPECTIVE_CANCEL_REENTRY`, which is produced by cancel ACK with positive remaining quantity. The evaluator uses:

- the current quote decision's BUY candidate price;
- age zero;
- the current causally visible book and trade state;
- a fresh queue-at-tail seed at the current candidate price level;
- current on-tick, non-crossing GTX eligibility;
- current candidate-level coverage and feature-ready time.

The retired order ID is retained only as lifecycle identity. Its depth cursor, queue path, elapsed age, cancel/refill history, and active-order hazard state are removed at exchange terminal and are not inputs to the fresh evaluator.

No new learned activation-probability model was introduced. In this identity, activation support means that the current candidate is GTX eligible and its current market state is causally covered.

## Terminal Routing

- cancel ACK with remaining quantity greater than zero enters prospective recovery;
- full fill and cancel ACK with zero remaining quantity end without re-entry;
- reject and expiry return to baseline resubmit routing;
- shutdown terminal outcomes do not re-enter;
- unknown terminal reasons fail before an unsupported policy transition.

Every exchange-terminal path removes the old active hazard state and depth cursor before routing. Post-terminal active-order hazard evaluation and cursor retention are zero-tolerance violations.

## Python/C++ Boundary

The native q90 ABI is now `dynamic_fill_hazard_native_book_q90.v4`. Python and C++ both rebuild the prospective observation from the current candidate price, age zero, current feature clocks, and current queue-at-tail. The old replay counter dictionary remains stable; v4 prospective counters are exposed through a separate adapter method.

This is native-kernel parity, not full C++ tick-replay authority.

## Authority

The implementation is local and not deployed. The 40-day event lockstep, AWS receive-time transport, inherited valid-fraction and role-TV gates, and a later independent economic replay remain outstanding. This identity grants no prediction, action-experiment, or live-deployment authority.

## Verification

- Focused q90, lifecycle, dual-clock journal, replay-clock, and Python/C++ parity suite: 67 passed.
- Full repository: 1409 passed, 4 skipped, 3 failed, 1 pre-existing joblib warning.
- The three failures are frozen F09 execution-amendment SHA256 checks. The new F10 lifecycle bytes and historical q90 successor-test bytes correctly no longer match those older F09 identities. F09 was not modified or re-frozen.

The machine-readable identity is [`buy_q90_fresh_prospective_placement_recovery_v4_implementation_20260802.json`](buy_q90_fresh_prospective_placement_recovery_v4_implementation_20260802.json).
