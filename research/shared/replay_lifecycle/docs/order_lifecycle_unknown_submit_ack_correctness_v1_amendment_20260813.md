# Unknown Submit ACK Lifecycle Correctness V1

Last materially modified: 2026-08-13

Status: `implemented_local_predeploy_blocked`.

## Boundary

This amendment repairs order-state truth after an ambiguous submit response. It is production-integrity work shared by every strategy and is independent of the withdrawn F05 prospective companion. It does not change quote price, size, cooldown policy, BER, P3, q90 action state, inventory limits, or the active owner policy, and it has not been deployed to EC2.

## Frozen Semantics

Only a structured exchange submit rejection with code `-5022` and an explicit guarantee that the order was not recorded may be encoded as `never_activated_exact_zero_exposure`.

A timeout, connection reset, HTTP 5xx, response loss, malformed response, or otherwise unknown submit acknowledgement remains `PENDING_NEW`. The strategy retains same-side order ownership and reconciles the client order ID before another same-side placement is allowed.

A reconciliation query returning `-2013` means that the query did not find the order at that instant; it does not prove that the original order never existed. The lifecycle therefore remains censored or unsupported and cannot be converted to exact-zero exposure.

A reconciliation response must be a mapping whose status, order ID, client ID, symbol, side, original quantity, and cumulative executed quantity are type-valid and consistent with the locally owned order. `None`, sequences, malformed mappings, identity mismatches, quantity regressions, and impossible status/quantity combinations return `query_malformed_still_unknown`; they do not mutate lifecycle state, release side ownership, or enter exact-zero exposure. Filled query responses may provide either an average fill price or Binance cumulative quote quantity, and neither source is treated as an activation clock.

A reconciliation query returning `NEW`, `PARTIALLY_FILLED`, or `FILLED` may restore known account quantities and terminal status, but its `updateTime` is not the historical activation timestamp. The unknown activation prefix remains censored, and no activation or fill clock is fabricated from that field.

A later private user-stream `NEW`, `PARTIALLY_FILLED`, `FILLED`, or `EXPIRED` event does not retroactively make the unknown submit prefix observable. Genuine private fill-event time may identify the fill event itself, but activation and the preceding exchange-exposure interval remain censored.

An orphan terminal callback preserves its reported cumulative executed quantity before terminalization. Every locally created order reserves its single-owner side before the REST submit can become visible and rechecks all nonterminal same-side lifecycles after the response. If an orphan arrives before or during that request, the engine retains both lifecycle records, latches a process-local submit block while holding the ownership lock, and stops new quoting fail closed instead of replacing either client-order identity. Opening, ordinary reducing, and emergency reducing submit entrypoints all reserve ownership before REST. A missing or malformed `status` or `orderId` is an unknown acknowledgement, not an implicit `NEW`; an emergency market close with an unknown acknowledgement remains `PENDING_NEW` and owns its side until reconciliation. Both ordinary opening and reducing entrypoints also check the latch immediately before REST; a conflict in that interval locally rejects the unsent order, releases its reservation, and never calls the exchange. The remainder of an already-running requote therefore cannot submit the opposite side after the conflict.

An order in `PENDING_CANCEL` is not terminal merely because a bulk open-order query omits it. Reconciliation uses the individual client-order identity; `-2013` remains unresolved, while an authoritative terminal status may close the lifecycle and preserve any cumulative fill.

Controlled startup requires a successful post-cancel account-position reconciliation before the prospective epoch, writers, or market/user streams start. A missing or failed position response blocks startup instead of assuming local inventory is authoritative.

The lifecycle event, journal row, asynchronous writer, remote spool, and resume path carry visible-exposure validity, exchange-exposure validity, completeness, and invalid-reason fields. `submit_ack_unknown` and `submit_ack_unknown_censored` are admitted event identities rather than writer-quarantine errors. A durability test now writes the unknown-ACK prefix, closes the asynchronous remote-spool writer, restarts the same admitted session, writes the shutdown censor, and verifies ordered unique rows with zero drops, errors, or quarantines.

## Covered Paths

The implementation covers BUY and SELL, opening, ordinary reducing, and emergency reducing close orders, explicit `-5022`, malformed or unknown acknowledgement, REST `-2013`, REST `NEW`/`PARTIALLY_FILLED`/`FILLED`, private user-stream recovery, orphan terminal fills, pending-cancel reconciliation, same-side orphan conflicts, and required startup position convergence. It preserves ownership while an order may still exist and distinguishes local shutdown censoring from an exchange terminal event.

The focused local lifecycle regression contains 230 passing tests and zero failures. The machine-verifiable command, source hashes, test hashes, and durability coverage are recorded in [the predeploy test receipt](order_lifecycle_unknown_submit_ack_correctness_v1_predeploy_test_receipt_20260813.json). Unknown-ACK journal events produced zero quarantine cases in this suite. No EC2 preflight, runtime release, rollback receipt, or post-start admission was performed.

## Permission

This implementation remains predeploy-blocked even though its focused local tests pass. It has no clean release tag or deployment receipt and grants no research, action, or live authority. A future runtime release requires a clean commit and annotated tag, separate preflight, independent runtime amendment, rollback receipt, and post-start lifecycle admission; it cannot ride on an F05 research deployment because no such deployment exists.
