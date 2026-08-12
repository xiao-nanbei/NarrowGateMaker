# F03 causal-v12 cadence-successor preflight v1

Last materially modified: 2026-08-04

## Status

This identity is a bounded, read-only inventory preflight. It does not train a model, read prediction outcomes, read PnL, score a cadence, modify an artifact, change the registry, or alter the operational baseline.

The current live/backtest baseline remains causal-v12 with canonical 10-second inference, q90 action disabled, and BUY fill-selection disabled. The preflight does not grant prediction, action, or live authority.

## What v12 actually fixes

The current feature generator and frozen artifacts bind:

- a completed 10-second feature bucket;
- a 10-second feature-ready offset;
- live inference once per completed 10-second bucket, with sample-and-hold between buckets;
- feature semantics version 6 and label semantics version 3;
- the exact 13 heads below.

| Heads | Fixed estimand |
|---|---|
| `dir_10s`, `ret_10s` | fill within 10s, then 10s post-fill markout; decision dependency spans 10-20s |
| `dir_30s`, `ret_30s` | fill within 30s, then 30s post-fill markout; decision dependency spans 30-60s |
| `dir_60s`, `ret_60s` | fill within 60s, then 60s post-fill markout; decision dependency spans 60-120s |
| `vol_10s`, `vol_30s`, `vol_60s` | fixed forward absolute-price variance over 10/30/60s |
| `tox_bid_5s`, `tox_ask_5s` | fill within 5s, then side-adverse 5s post-fill markout; dependency spans 5-10s |
| `tox_bid_10s`, `tox_ask_10s` | fill within 10s, then side-adverse 10s post-fill markout; dependency spans 10-20s |

The `10s/30s/60s` direction and return names are therefore not ordinary fixed-forward returns. Their maximum future dependency is twice the named horizon.

## Feature basis is not an estimand

The model also carries trailing basis windows such as:

- tick state: 3/5/10s;
- taker tempo: 5/10/30/60s;
- local microstructure: 5/30/60/300s;
- cross-market return context: 10/30/60s;
- volatility regime: 6h/24h.

These windows remain candidate input basis functions. They do not define the label horizon and this preflight does not remove or select them.

## Bounded cadence identities

The 10-second v12 bundle is the frozen reference. Three successor candidates are inventoried independently:

| Role | Identity | Cadence | Feature DAG identity |
|---|---|---:|---|
| Reference | `causal_v12_expanded_source_aware_semantics_v6_canonical_10s_reference` | 10s | `live_10s_signal_cutoff.v1` |
| Candidate | `causal_v12_cadence_1s_source_aware_semantics_successor_v1` | 1s | `live_1s_signal_cutoff.v1` |
| Candidate | `causal_v12_cadence_2s_source_aware_semantics_successor_v1` | 2s | `live_2s_signal_cutoff.v1` |
| Candidate | `causal_v12_cadence_5s_source_aware_semantics_successor_v1` | 5s | `live_5s_signal_cutoff.v1` |

Each candidate requires its own Feature DAG, cutoff/ready semantics, generated features, source manifest, training spec, model output identity, and parity contract. Forward-filling the existing 10-second features is forbidden.

The first successor changes cadence only. All 13 existing label definitions are held fixed. A direction/return/toxicity horizon-decay study is a separate identity because changing horizon and cadence together would confound the estimand.

The 1/2/5-second rows create strongly overlapping labels, up to 120 seconds of future dependency. Their chronological weighting, fold embargo, calibration, and clustered evaluation contracts must be frozen before retraining.

## Historical panel boundary

The following 2026 native panels have already been read and remain diagnostic only:

- 22 historical native transport-development days;
- 22 historical native late-diagnostic days;
- 5 post-fit Grade-A diagnostic days;
- 1 post-fit gap-sensitivity day.

They may be used for transport diagnostics after a candidate is frozen. They may not select cadence, tune labels, or be represented as independent confirmation. No untouched chronological confirmation panel is frozen by this preflight.

## Exact retraining blockers

For every 1/2/5-second identity, the following are currently missing:

1. cadence-specific causal feature generator and Feature DAG implementation;
2. cadence-specific source and feature manifests;
3. next-cutoff perturbation and live/offline/Python/C++ feature parity contract;
4. overlapping-label weighting, embargo, and chronological calibration contract;
5. frozen cadence-specific training spec and model output identity.

Even after retraining, promotion remains blocked by:

- an untouched chronological confirmation panel;
- a separately frozen full-path ML-OFF versus candidate ML-ON economic identity;
- continuous-state accounting and replay parity;
- cadence-specific live scheduler/runtime ABI, deployment preflight, and rollback.

## Invocation

The preflight prints an in-memory JSON report to stdout and has no output-file or training option:

```bash
.venv/bin/python -m research.families.f03_causal_13_head.audit.causal_v12_cadence_successor_preflight
```

An input/hash/semantic mismatch raises immediately. A structurally valid audit returns `inventory_complete=true` while retaining `retraining_execution_eligible=false` until the listed artifacts exist.

The machine-readable contract is `causal_v12_cadence_successor_preflight_v1_design_20260804.json`.
