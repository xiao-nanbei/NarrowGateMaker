# BUY q90 Terminal Recovery Contract v1

Last materially modified: 2026-08-02

Status: baseline-integrity implementation verified; full-panel transport and economic evaluation remain closed.

## Contract

The exchange-order lifecycle is now explicit:

```text
SUBMITTED
  -> ACTIVE
  -> PARTIALLY_FILLED
  -> CANCEL_PENDING
  -> EXCHANGE_TERMINAL
  -> POST_CANCEL_RECOVERY
  -> REENTRY_ELIGIBLE
```

Only `ACTIVE`, `PARTIALLY_FILLED`, and `CANCEL_PENDING` belong to the fill-risk set. Cancel-pending orders remain fillable through ACK. Full fill, cancel ACK, reject, or expiry ends the old order path and removes its exact-level cursor. No terminal order may be synthesized as `PENDING_CANCEL`.

For partial fills, exposure is quantity weighted:

\[
E_q=\int_{T_{activation}}^{T_{terminal}}Q_{remaining}(t)\,dt
\quad [BTC\cdot s].
\]

First-fill latency is recorded separately. A partial fill updates remaining quantity before the next exposure interval accrues.

## q90 Boundary

After exchange terminal, q90 enters `POST_CANCEL_RECOVERY`. The active-order hazard runtime, old queue, old price, old age, and old path are discarded even when the old score recovered before ACK. Exposure-increasing re-entry is not authorized until a separate prospective-placement estimator evaluates current market state, a new candidate price, age zero, and a new queue distribution.

That prospective estimator is deliberately not implemented in this identity. The production q90 action therefore remains disabled while shadow prediction continues. Reducing BUY permission remains unchanged.

## Verification

- The local native q90 ABI advanced to `dynamic_fill_hazard_native_book_q90.v2`; this repair has not been deployed.
- The registered q90 graph advanced to `buy_q90_visibility_lifecycle_path_score.v2` and binds the order lifecycle, remaining quantity, BTC*s exposure, visible book path, and 100ms score order.
- Python live, Python replay, and the pybind C++ kernel share the terminal-path invariant.
- Targeted lifecycle, q90, replay, baseline, and preflight suites passed; their overlapping test selections are not summed into a synthetic count.
- Full repository suite: 1,273 passed, 4 skipped, one non-failing physical-core discovery warning.
- No PnL, markout, Validation, or sealed holdout result was read for this implementation.

The machine-readable identity and implementation hashes are in [`buy_q90_terminal_recovery_contract_v1_implementation_20260802.json`](buy_q90_terminal_recovery_contract_v1_implementation_20260802.json).

## Remaining Gates

This repair closes the known terminal active-order liveness trap. It does not yet pass q90 transport or create F07 v2. The next mechanics identity must add a prospective-placement recovery estimator, run the repaired 40-day parity, and retain the original valid-fraction and cancel-role thresholds. Economic q90 ON/OFF attribution may run only after those mechanics gates pass.
