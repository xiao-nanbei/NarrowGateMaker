# Causal-v12 1s Feature Generator Prototype

Date: 2026-08-04

Last materially modified: 2026-08-04

Status: engineering preflight only; not training, action, or live eligible

## Identity

- Successor: `causal_v12_cadence_1s_source_aware_semantics_successor_v1`
- Feature DAG: `live_1s_signal_cutoff.v1`
- Feature semantics: `causal_13_head_features_canonical_1s.v1`
- Implementation: `research/families/f03_causal_13_head/audit/causal_v12_1s_feature_generator.py`

This is an isolated prototype. It does not modify `features/feature_dag.py`, `strategy/signal.py`, live code, the registry, the current 10s model, or the operational baseline.

## Causal Clock

For a canonical cutoff `c`, a raw 1s bar is visible only when:

```text
bar.start_ts_ms < c
bar.start_ts_ms + 1000 <= c
bar.finalized_ts_ms <= c
```

Every generated row also enforces:

```text
feature_ready_ts_ms <= cutoff_exclusive_ms <= decision_ts_ms
```

The bar beginning at `c` is strictly invisible to the row at `c`. A delayed calculation cannot retroactively admit a bar that was finalized after `c`. Missing seconds and duplicate source timestamps fail closed. Catch-up emits every unseen canonical 1s cutoff exactly once and advances its cursor only after the complete batch succeeds.

## Feature Semantics

The prototype accepts raw completed 1s bars. It cannot accept or forward-fill the existing 10s feature rows or model output. Each node records:

- unit;
- cadence;
- exact raw or feature dependencies;
- lookback and minimum observation count;
- source and source clock;
- availability clock;
- stateful flag;
- dynamic lag-state rule.

The retained basis includes 3/5/10s tick momentum, 5/10/30/60s taker quote imbalance, 5/30/60/300s local microstructure, 10/30/60s raw cross-market returns, and 6h/24h volatility regime state. These windows remain feature basis functions; they are not estimand horizons.

Missing cross-market support produces a null value with `source_unavailable_no_forward_fill`. Warmup also remains explicit. The generator never silently substitutes zero.

Labels and economic outcomes are structurally absent from the input API and forbidden as DAG dependencies. The feature namespace is exclusively `feature`; `label`, `target`, `future`, `reward`, and `pnl` dependencies fail contract validation.

## Fingerprint

The canonical row fingerprint binds the static node contract, cutoff, feature identity, sorted hexadecimal floating-point values, source and ready clocks, observation counts, and lag states. It intentionally excludes input iteration order, future bars, wall-clock execution time, labels, and outcomes.

The regression suite proves that the next 1s bar cannot alter the previous row, cutoff-minus-1s is visible while cutoff is not, reordered input has the same fingerprint, a visible input mutation changes the fingerprint, gaps and duplicates fail closed, and catch-up does not skip or duplicate cutoffs.

## Remaining Blockers

This prototype does not complete the 1s retraining identity. The following remain required:

- full v12 feature-schema mapping, including metrics, execution L2, and calendar nodes;
- Python/C++ per-field parity;
- cadence-specific feature and source manifests;
- overlapping-label and chronological-calibration contracts;
- cadence-specific training specification and model output identity.

No model was trained, no prediction or PnL outcome was read, and no action or live authority is granted.
