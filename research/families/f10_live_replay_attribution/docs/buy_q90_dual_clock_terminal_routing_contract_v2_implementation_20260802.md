# BUY q90 Dual-Clock Terminal Routing Contract v2

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Status: baseline-integrity mechanics verified by targeted tests; full-panel lockstep, transport, economics, and live deployment remain closed.

## Dual Exposure

The lifecycle now exports two separate quantity-time estimands:

\[
E_q^{exchange}
=
\int Q_{remaining}(t_{exchange})\,dt,
\qquad
E_q^{visible}
=
\int Q_{remaining}(t_{visible})\,dt.
\]

Both use BTC*s. The historical `quantity_time_exposure_btc_s` field remains a read-only compatibility alias for `quantity_time_exposure_visible_btc_s`; it must not be described as physical exchange exposure. Exchange exposure is reported separately as `quantity_time_exposure_exchange_btc_s`.

Missing, future, or regressed exchange timestamps invalidate the physical estimand and produce a null value plus an explicit reason. The implementation does not borrow visibility timestamps to make physical coverage appear complete. Visible-minus-exchange exposure is retained as an information-delay diagnostic. Visible and exchange first-fill latencies are also separate.

An activation event without an exchange timestamp now fails closed at that event, before any later fill or terminal message arrives:

- `exchange_exposure_valid=false` means the physical clock is missing or regressed;
- `exchange_exposure_complete=false` means the order is still active or the physical terminal boundary is not complete;
- `quantity_time_exposure_exchange_btc_s=null` means physical exposure cannot be reported;
- `exchange_exposure_invalid_reason=missing_exchange_timestamp:activate` identifies this left-censoring case directly.

The shared `order_lifecycle_journal.v1` schema is now persisted by live and emitted by the authoritative Python q90 replay. Every event carries both exposures, validity/completeness/reason, remaining quantity, and terminal route. The replay output also includes machine-readable null coverage and invalid-reason counts.

This is deliberately **not** a three-runtime exposure-parity claim. The pybind q90 ABI does not calculate or export either quantity-time exposure. Current Python/C++ parity covers terminal routing, native path, score, cancel, and recovery transitions only. A future C++ exposure implementation would require a separate identity and tests.

## Terminal Routing

Leaving the exchange fill-risk set is not itself permission to re-enter:

| Exchange terminal outcome | Policy route |
|---|---|
| Cancel ACK with positive remaining quantity | `PROSPECTIVE_CANCEL_REENTRY` |
| Full fill or zero remaining quantity | `TERMINAL_COMPLETE` |
| Reject or expiry | `BASELINE_RESUBMIT` |
| Administrative or shutdown terminal | `SHUTDOWN_NO_REENTRY` |
| Unknown reason | `UNSUPPORTED`, fail closed |

Only cancel ACK with positive remaining quantity may enter `POST_CANCEL_RECOVERY`. Full fill cannot recreate same-side exposure, shutdown cannot re-enter, and reject/expiry return to ordinary baseline resubmission rather than q90 recovery. Every terminal path clears the old depth cursor and old active-order hazard state.

The cancel-ACK route still stops before action. A future evaluator must build a fresh candidate using current price, age zero, a fresh queue-at-tail distribution, activation/GTX risk, and current causal market state. That prospective placement estimator is not part of v2.

## Implementation Identity

- Native ABI: `dynamic_fill_hazard_native_book_q90.v3`.
- Feature graph: `buy_q90_visibility_lifecycle_path_score.v3`.
- Graph SHA256: `25e88ffb95994fd7ab7a2071f2187c364984b4b35988dd10305c214438a70ef9`.
- Python live, Python replay, and the pybind q90 kernel share terminal routing.
- Python live and replay share `order_lifecycle_journal.v1`; C++ has no quantity-time exposure authority.
- The local native extension was rebuilt under Python 3.12.13.
- 89 targeted lifecycle, q90, replay, cursor, preflight, and governance tests passed.
- Scoped lint and Python compilation passed.
- The full repository suite passed with 1,341 tests, 4 skips, and one environment-only joblib core-count warning.

No PnL, markout, Validation, or sealed holdout result was read. The 40-day event-lockstep and same-date AWS transport panels were not rerun. q90 shadow remains enabled, q90 action remains disabled, and this local repair has not been deployed.

The machine-readable identity and implementation hashes are in [`buy_q90_dual_clock_terminal_routing_contract_v2_implementation_20260802.json`](buy_q90_dual_clock_terminal_routing_contract_v2_implementation_20260802.json).

## Next Gate

The next identity may implement only the fresh prospective placement recovery surface. It must accept only `PROSPECTIVE_CANCEL_REENTRY` and construct current price, age-zero, fresh queue-at-tail, GTX/activation risk, and current causal market state without reading the terminal order's old cursor or path.

After that implementation is frozen, the 40-day gate must be split explicitly:

1. Python live/replay lifecycle-journal parity for both exposure clocks, remaining quantity, validity/completeness, null coverage, and invalid reason.
2. Python/C++ lockstep for terminal route, native path, score, cancel, and recovery transitions, with no C++ exposure-parity claim.
3. Zero post-terminal active-hazard evaluations, zero terminal cursor retention, and zero unsupported terminal routes.
4. Same-date AWS receive-time transport and the inherited valid-fraction and cancel-role gates.

All four remain mechanics-only. No economic q90 replay may be opened first.

The historical v1 identity remains frozen and is not rewritten by this v2 implementation.
