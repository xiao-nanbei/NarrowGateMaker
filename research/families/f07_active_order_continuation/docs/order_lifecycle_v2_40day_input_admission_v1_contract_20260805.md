# F07 Order Lifecycle Journal v2 40-Day Input Admission v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

## Status

`frozen_input_identity_pending_40day_artifact_admission`

This is a mechanics-only input and admission contract. It does not read PnL, markout, reward, campaign value, or any other economic outcome. It does not run the formal 40-day event lockstep. It grants no CIF training, q90 action, economic evaluation, live transport, or live deployment permission.

The machine-readable authority is `order_lifecycle_v2_40day_input_admission_v1_contract_20260805.json`, canonical identity SHA256 `3458cb12a9fcc9b65608018b9324fbb30de02e208ee3b26d000257800102dbd7`.

## Frozen Denominator

The ordered denominator is the existing F09/F10 Development panel of exactly 40 UTC target days, from 2026-04-17 through 2026-06-26 with the nonconsecutive allowlist frozen in the JSON contract. The contract binds, for every target day:

- the exact target interval `[D 00:00:00Z, D+1 00:00:00Z)`;
- the exact D-1 warmup interval `[D-1 00:00:00Z, D 00:00:00Z)`;
- the F10 denominator authority and F09 source contract;
- native L2, quality, execution-trade, latency, queue-calibration, model, P3, Feature DAG, baseline, adapter, schema, and code identities.

The current operational baseline is v9: causal-v12 ON, q90 shadow ON, q90 action OFF, and BUY fill-selection retired. Daily replay starts from a fresh runtime state and does not claim continuous live state.

## Admission Contract

Each day must be admitted through one atomic daily manifest and the panel must be admitted through one atomic aggregate manifest. Admission requires:

- content-addressed journal-v2 Parquet parts and manifests;
- exact journal-v2 and legacy trace schemas, SHA256, row counts, unique order, event, and lifecycle identities;
- complete gzip CRC/decompression and Parquet metadata reads;
- exact D-1 warmup and target market-data interval coverage;
- writer close/flush completion, zero dropped rows, zero errors, matching callback/row totals, and no orphan or partial payloads;
- one final terminal or explicit censor observation per order;
- explicit legacy visibility/exchange clock coverage;
- cancel-reject route support and observed counts;
- zero terminal sub-lot remainder under the frozen zero-remainder contract;
- an explicit C++ event-stream binding to the same journal schema and code identity. CIF kernel parity alone is not accepted as event-stream binding.

Only when all 40 ordered days pass every admission gate may the preflight emit `lockstep_execution_eligible=true`. Structured failure codes remain in the report; unsupported clock, cancel-reject, sub-lot, or C++ coverage is never silently imputed.

## Session Boundary

This identity freezes:

```text
replay_session_scope = fresh_start_per_target_day
lifecycle_sequence_starts_at_one_per_target_day = true
carry_in_lifecycle_count = 0
left_truncation_supported = false
prospective_live_epoch_transport_supported = false
```

This scope exists because the authoritative `--day` lockstep requires each journal lifecycle sequence to start at 1. It is valid only for the 40-day daily-fresh-start replay denominator.

It must not be extrapolated to an order that remains active across UTC midnight or to a prospective live baseline epoch. UTC day is an offline denominator and clustering/accounting unit, not a lifecycle reset boundary. Future live transport must use either one complete baseline-epoch/session or an explicitly frozen carry-in cursor with left truncation. That successor must retain cross-midnight orders and may not reset lifecycle sequence at UTC day boundaries.

## Current Blockers

The frozen identity records all four current producer capabilities as false:

1. Legacy traces do not yet provide authoritative explicit dual-clock coverage for all 40 days.
2. Cancel-reject is not yet bound across the admitted journal-v2 and legacy event streams.
3. The zero terminal sub-lot remainder contract is not yet evidenced for all admitted days.
4. A native C++ event-stream binding to journal-v2 has not yet been admitted.

Accordingly, no current 40-day panel manifest is declared eligible and no formal lockstep has been run by this work.

## Permissions

```text
cif_training = false
economic_evaluation = false
q90_action = false
prospective_live_epoch_transport = false
live_deployment = false
```
