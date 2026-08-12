# Causal-v12 1s Full Trainable Feature DAG v1

Date: 2026-08-05

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Status: Python full-schema engineering complete; training and live authority closed.

## Scope

This successor maps the current causal-v12 model ABI to an independent 1s Feature DAG. The frozen ABI contains 173 ordered features and 13 model heads. The head-to-label linkage remains `causal_v12_13_head_label_contract_v3_preserved`; only inference cadence and feature generation semantics are in scope.

The implementation reads raw completed 1s bars and explicit source observations. It does not accept a precomputed 10s feature row, forward-fill a 10s row onto the 1s grid, read labels, read predictions, or read PnL.

## Sources

The full schema is partitioned into five non-overlapping source contracts:

| Source | Features | Causal admission |
|---|---:|---|
| Local completed 1s bars | 87 | Strict contiguous bars through `cutoff-1s`; gaps, duplicates, and late bars fail closed |
| Execution-L2 completed 1s | 13 | Exact `cutoff-1s` bucket with `feature_ready <= cutoff`; no carry from an older second |
| Binance metrics 5m | 13 | Past-only as-of observation with explicit ready time and at most 300s age |
| Reference-perp completed 1s | 11 | Past-only contiguous tail, maximum 30s source age, explicit unsupported values |
| Canonical calendar clock | 49 | Deterministic at the canonical cutoff under the frozen calendar-year contract |

The source manifest SHA256 is `f2c473dac79a387e56c4aa1d166cfb511575a2de7f8666c0aef32067e4d1ad7e`.

## Schema And Fingerprint

The 173-column order exactly matches every head in the causal-v12 bundle. Its canonical order SHA256 is `5a6947850dfabefbf4e36bdbe986e39c96324e3714efb16d3410a4443ea1b797`.

Each row fingerprint binds:

- canonical cutoff;
- feature contract and source manifest hashes;
- ordered values represented by exact float hex strings;
- source and feature-ready timestamps;
- observation counts and unsupported/warmup states.

Appending or changing an event at the next cutoff cannot change the previous row. Auxiliary-source absence is represented as an explicit unsupported value; it is never repaired using a stale 10s feature row.

## Label Boundary

The 13 heads keep the existing 5/10/30/60-second labels and label semantics v3. The full-schema implementation freezes only names and linkage. It does not compute or inspect label values, and it does not change any estimand horizon.

## Remaining Blockers

This work does not yet authorize training or deployment. Remaining work is:

1. rebuild real-day 1s feature panels and establish Python offline parity;
2. implement the same nodes in C++ and obtain field-by-field fingerprint parity;
3. freeze the 1s source-day manifest, overlapping-label weights, embargoes, and chronological folds;
4. retrain and calibrate all 13 heads inside those folds;
5. run the full-path ML-OFF versus 1s ML-ON economic replay before any live canary.

No live, strategy, replay-engine, C++, or registry file is modified by this engineering identity.
