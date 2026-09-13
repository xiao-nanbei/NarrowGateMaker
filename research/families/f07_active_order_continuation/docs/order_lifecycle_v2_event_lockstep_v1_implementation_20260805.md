# F07 Order Lifecycle Journal-v2 Event Lockstep v1

Date: 2026-08-05

Last materially modified: 2026-08-05

Status: mechanics harness locally verified; formal 40-day run not started

## Scope

This identity audits the authoritative Python replay's `order_lifecycle_journal.v2` output against the existing local-order lifecycle trace. It reads mechanics only. It does not read PnL, reward, markout, campaign value, or any policy outcome, and it grants no CIF-training, q90-action, economic, or live authority.

Implementation:

- `research/families/f07_active_order_continuation/audit/order_lifecycle_v2_event_lockstep.py`
- `tests/test_order_lifecycle_v2_event_lockstep.py`
- machine-readable implementation record: `order_lifecycle_v2_event_lockstep_v1_implementation_20260805.json`

## Audit Semantics

The harness consumes one fresh-start replay UTC day at a time through projected Parquet batches. A lifecycle sequence therefore starts at one inside each target-day replay. This is not the prospective live transport contract: a live order may cross UTC midnight, so an epoch-wide live audit must retain its carry-in cursor/left-truncation state and may use UTC days only for clustering. It retains only mechanics fields, binds all input files by SHA256, and writes a canonical-hash envelope by temp-file, `fsync`, and atomic replace.

For the overlap between old and new traces it checks:

- stable client-order and lifecycle identity;
- unique event IDs and contiguous per-lifecycle sequence;
- legacy global event-sequence integrity;
- event/phase order and remaining-quantity chain;
- terminal reason;
- each visibility or exchange timestamp whose clock meaning exists in the old trace;
- visible risk spells and quantity-weighted exposure when the old trace has a complete visibility-clock projection.

The old trace has only one generic `event_ts_ns`. Its meaning differs by event: activation/reject use exchange time, while submit/cancel request/cancel ACK use strategy-visible time. The harness therefore does not manufacture a second clock. Missing clock support is reported as coverage, not counted as parity.

Journal-v2 dual-clock exposure is independently recomputed for every event:

\[
E_q^{visible}=\sum_j Q_j\Delta t_j^{visible}
\]

\[
E_q^{exchange}=\sum_j Q_j\Delta t_j^{exchange}
\]

It checks cumulative rows, invalid-reason coverage, risk intervals, terminal completeness, and zero events or fill-risk state after exchange terminal or local shutdown censor.

## Synthetic Verification

The fixtures exercise:

- partial fill;
- partial fill while cancel-pending;
- cancel reject returning to the appropriate active/partially-filled state;
- cancel ACK with remaining quantity;
- pre-activation reject;
- expiry;
- local shutdown right-censor;
- an intentionally illegal post-terminal recovery row;
- actual `simulate_tick()` output through both old trace and journal-v2;
- projected Parquet streaming and atomic result admission.

Local focused result:

```text
5 passed
Ruff passed
```

## Formal 40-day Blockers

The harness is ready, but the 40-day input identity is not. Before formal execution, the following must be closed:

1. Freeze a fully bound replay runtime identity and chronological day manifest.
2. Materialize and atomically admit each day's journal-v2 and legacy mechanics trace with exact row counts, zero writer drops, and zero errors.
3. Add explicit dual-clock fields to the old projection if full cross-source clock parity is required. Otherwise the formal result must preserve the current partial-coverage wording.
4. Add an authoritative replay source for exchange cancel rejection. The adapter and fixtures support it; the current simulator does not generate it.
5. Prove that terminal fills always leave zero quantity, or freeze a distinct terminal reason for any sub-lot untradeable remainder.
6. Bind the C++ lifecycle/CIF state stream to the same event identity for the later Python/C++ lockstep stage.

No 40-day tape, CIF fitting, economic result, or q90 action was read or run in this implementation step.
