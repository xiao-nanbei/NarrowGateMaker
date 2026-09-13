# Order Lifecycle Journal Writer v2

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

## Status

`infrastructure_implemented_local_not_integrated`

This identity adds a reusable persistence boundary around `order_lifecycle_journal.v2`. It does not modify `maker_engine.py`, `backtest_tick.py`, live configuration, or the research registry. It reads no PnL, reward, or markout fields and grants no q90 action or live authority.

Frozen implementation SHA256: `ea1bcf695a451add489fb772d22d4036529bd6ac2f7a2c34bcc3c94e46f7438e`. Frozen focused-test SHA256: `7b5317e6d1ba941395ab69521fae020356efdfa7d74fb1e8da21c8a1caee26b2`.

## Atomic Boundary

One source callback owns one complete batch of all unseen events for one order lifecycle. The durable transaction order is:

1. Validate the entire journal-v2 callback batch.
2. Write and `fsync` a Parquet or JSONL payload part, then atomically rename it.
3. Write and atomically rename its content-addressed manifest.
4. Atomically update writer health so the part is admitted.
5. Atomically advance the lifecycle cursor.

The cursor is therefore never the first durable object. It may lag an immutable part after a crash, but it may not lead one. Recovery validates every payload SHA256 and schema, reconstructs callback boundaries, updates health, and only then catches cursors up.

The runtime bridge never relies on the emitter's in-memory post-batch cursor. For every callback it constructs a temporary emitter from the durable cursor. If any write stage fails, a retry emits the same stable event IDs again.

## Restart And Deduplication

Parts are addressed by the callback's stable event IDs, callback identity, and post-commit checkpoint. Recovery rejects:

- lifecycle sequence gaps or overlaps;
- duplicate event IDs across parts;
- a cursor ahead of immutable parts;
- a cursor that splits a callback batch;
- data/manifest hash or row-count disagreement;
- any event after an explicit `local_shutdown_censor`.

A payload renamed before its manifest is an orphan, not an admitted batch. Health reports `orphan_payload_count > 0` and formal collection remains invalid until the same callback completes the manifest, health, and cursor sequence.

## Hot Start

Startup-active exchange orders place the writer in quarantine. No callback observed before cutover enters the formal denominator. Collection begins only after every startup-active order has a supported exchange-terminal reason. Pre-cutover lifecycle IDs remain permanently excluded from retrospective admission.

`local_shutdown_censor` does not prove that an exchange order is terminal and therefore cannot release hot-start quarantine.

## Terminal And Clock Semantics

Supported exchange-terminal reasons are `cancel_ack`, `cancel_ack_reconciled`, `expired`, `filled_before_cancel_ack`, `full_fill`, and `rejected`. Unknown terminal reasons fail closed before cursor advance.

Local shutdown must be represented explicitly as `local_shutdown_censor` with reason `administrative_cancel`, `local_shutdown_cancel`, or `shutdown`. It is a local observation boundary, not an exchange-terminal claim. No later event may be admitted for that lifecycle.

The writer validates and persists the journal-v2 row without transforming its dual-clock fields. Visible and exchange quantity-time exposure, validity, completeness, and invalid-reason fields retain their source values.

## Health

The atomic health file reports heartbeat, last successful flush, committed callback and row counts, drops, errors, quarantined callbacks, startup and pre-cutover exclusions, durable cursor count, local censors, orphan payloads, and formal collection validity. A daemon heartbeat can refresh the file even when no callback arrives. The synchronous writer has no lossy queue; any producer-reported drop permanently invalidates that session.

## Failure Tests

Fault injection covers both sides of payload, manifest, health, and cursor replacement. The tests verify that half writes, process restart, and a repeated callback produce every event exactly once in both Parquet and JSONL modes.

## Permission Boundary

This is infrastructure only. A later integration identity must wire the bridge to authoritative live/replay callbacks, admit a health-valid v2 tape, and pass 40-day event lockstep. Until then q90 remains shadow-only/action-off, and no economic result may be inferred from this writer.
