# Order Lifecycle Journal-v2 Authoritative Python Replay Adapter v1

Date: 2026-08-05

Last materially modified: 2026-08-05

Status: implemented and locally verified; 40-day lockstep not yet run

## Scope

This identity connects the shared mechanics-only `order_lifecycle_journal.v2` schema and atomic writer to the authoritative Python tick replay. It does not read PnL, reward, markout, or any economic outcome and grants no q90 action, research-promotion, or live authority.

Implementation:

- `models/replay/order_lifecycle_v2_replay_adapter.py`
- minimal lifecycle transition hooks in `models/backtest_tick.py`
- `tests/test_order_lifecycle_replay_journal_v2_adapter.py`

The adapter is disabled by default. When disabled, replay does not construct a writer, create files, or add output fields. Existing replay mechanics and result keys remain unchanged.

## Replay contract

Enablement requires all of:

```text
order_lifecycle_journal_v2_enabled=true
order_lifecycle_journal_v2_root=<path>
order_lifecycle_journal_v2_session_id=<stable session>
_order_lifecycle_journal_v2_runtime_identity=<mechanics-only identity map>
```

Optional storage format is `parquet` by default or `jsonl` for diagnostics. The runtime identity is extended only with the adapter identity and explicit `economic_outcomes_read=false` and `q90_action_authorized=false` fields.

Each source callback mutates one shared `QuantityWeightedOrderLifecycle`, then atomically commits every unseen event through `OrderLifecycleJournalRuntimeBridgeV2`. Event IDs derive from stable lifecycle identity, sequence, clocks, phase, quantity, and reason. Durable cursors support deterministic replay rebuild after restart without duplicate rows.

Covered callback semantics:

- submit and activation;
- partial and full fill, including fill while cancel-pending;
- cancel request, cancel reject, and cancel ACK;
- exchange reject and expiry;
- end-of-replay local shutdown censor without asserting an exchange terminal.

Exchange-terminal reasons are normalized to the journal contract: `full_fill`, `cancel_ack`, `rejected`, or `expired`. Strategy-specific cancel labels remain in the existing replay trace but do not replace the physical terminal reason.

## Verification

Local verification on 2026-08-05:

```text
54 passed
```

The selected suite covered adapter routing, atomic writer fault/restart behavior, lifecycle schema validation, replay integration, and the existing q90 ABI-v4 lockstep preflight. Adapter-specific checks require:

- stable unique event IDs;
- durable-cursor restart dedupe;
- exact writer row count agreement;
- zero dropped rows and zero writer errors;
- explicit shutdown censor;
- no PnL/markout columns;
- disabled replay result-key parity;
- `economic_outcomes_read=false`;
- `q90_action_authorized=false`.

## Remaining 40-day lockstep blockers

This implementation makes the Python replay tape producible; it does not make the 40-day F07 evidence complete. Remaining blockers are:

1. Freeze a fully bound 40-day replay runtime identity and output/admission manifest, including code, config, data, Feature DAG, execution ABI, source clocks, and initial-state hashes.
2. Run all 40 retained Development days with journal-v2 enabled and require per-day atomic admission, exact callback/row/event-ID counts, zero drops, zero errors, and no unsupported terminal route.
3. Add or bind an authoritative replay event source for cancel-reject. The adapter and tests support the transition, but the current Python simulator does not synthesize exchange cancel rejection on the formal path.
4. Compare journal-v2 lifecycle state, quantities, clocks, terminal routes, and fill-risk intervals against the existing Python lifecycle trace for every event; explain rather than silently pool unsupported clock rows.
5. Emit the corresponding C++ lifecycle/CIF state and complete per-event Python/C++ lockstep. C++ model training is not required, but inference state and transition parity are.
6. Only after admitted 40-day tape and lockstep, train/evaluate the 100 ms time-varying CIF and perform AWS receive-time transport. Economic outcomes and q90 action remain closed throughout these mechanics stages.
