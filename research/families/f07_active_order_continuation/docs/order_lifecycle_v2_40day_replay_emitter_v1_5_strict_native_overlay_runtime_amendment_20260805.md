# F07 Order Lifecycle v2 40-Day Replay Emitter v1.5 Amendment

Last materially modified: 2026-08-05

## Status

`implemented_not_executed`. This successor strengthens execution authority only. It does not read PnL, admit a 40-day panel, authorize CIF training, or authorize q90/live action.

## Strict Native Authority

F07 now runs with `strict_native_only=true`. Every lifecycle journal row records `simulator_queue_source` and `exact_queue_path_valid`. A spell is eligible for CIF only when every fill-risk row from activation onward uses `native_exchange_book` and retains an exact queue path. Missing seeds, sparse-watch seeds, fitted/top-N fallback, and later native-path invalidation remain available to preserve the baseline replay trajectory, but are explicitly censored from CIF.

Market-context BBO/L2 cannot become queue authority. Its normalized BBO and L2 leaves are hash-bound and independently reloaded; exact array fingerprints must match the bound window payload.

## Overlay Authority

The operational overlay is validated for canonical 10-second timestamps, unique increasing rows, row-aligned finite main arrays, bundle-required feature keys, the required 13 heads, and a frozen gap policy. A 20-second gap means causal sample-and-hold of the previous prediction; a larger gap fails the day closed. Feature-map NaNs retain LightGBM's frozen missing-value semantics, while infinities are rejected.

At least one bound day must carry an independent re-inference receipt with exact array parity. Every worker revalidates source, config, model, P3, DAG, runtime code, C++ module/ABI, latency, and queue artifacts before replay.

## Execution Integrity

Only one process may execute a canonical plan SHA at a time. That lock owner may use `--workers 1..8` to schedule independent day workers. Each day retains its own staging directory and atomic publish; the parent validates and publishes the panel manifest only after all 40 admitted days are present. A competing external executor for the same plan remains fail-closed.

The abandoned v1.4 first-day staging output is separately recorded as diagnostic-only and is not an admitted denominator day.

## Historical Reproduction Boundary

The journal-v2 ABI-v1 Python paths remain byte-for-byte frozen for the prior journal-authority amendment. Strict-native queue fields and ABI-v2 lockstep live only in successor modules whose paths and hashes are bound by this amendment. The C++ mirror accepts either the complete historical 41-column schema or the complete strict-native 43-column schema, reports the matching ABI, and rejects mixed generations. This preserves testable historical reproduction without weakening the old validator or rewriting the old amendment.

CIF queue authority is evaluated only over true fill-risk transitions: rows whose phase before or after is `ACTIVE`, `PARTIALLY_FILLED`, or `CANCEL_PENDING`. A physical terminal transition from one of those states remains in scope; valid post-terminal recovery or re-entry rows are outside fill risk and do not require a queue identity.

The machine-readable authority is the adjacent JSON document.
