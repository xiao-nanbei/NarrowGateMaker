# Causal-v12 1s ML A/B Replay Successor ABI v1

Last materially modified: 2026-08-05

Status: `implemented_and_unit_verified_not_economically_run`

## Scope

This successor connects the admitted 1-second causal-v12 prediction overlay to the authoritative tick simulator without changing the historical 10-second `load_ml_predictions()` contract in `models/backtest_tick.py`.

The adapter validates all 13 prediction columns, the ordered head model and metadata hashes, the overlay hash, its atomic admission marker, and the exact 86,400-row canonical UTC-second grid. A feature is admissible only when:

\[
t_{feature\ ready}\le t_{decision}.
\]

The existing quote ABI consumes the explicit projection `dir_10s`, `vol_10s`, `ret_10s`, `tox_bid_10s`, and `tox_ask_10s`. The other eight heads remain mandatory identity inputs; they are not silently optional. Formal replay also reopens the bound research bundle and rehashes the bundle, all 13 model files, and all 13 metadata files. Manifest strings alone are not sufficient authority.

## Clock Semantics

`decision_ts_ms`, rather than provider or source time, is the replay prediction availability clock. Each canonical row is sample-and-held over:

\[
[t_{decision},t_{decision}+1000\mathrm{ms}).
\]

Events before the first decision receive no prediction. The caller must load a model-free market window with `load_tick_window(..., load_ml=False)`; supplying an old-loader `ml_data` payload fails closed.

## Paired Arms

Both arms bind the current immutable v9 operational baseline:

- q90 shadow state is identical between arms;
- q90 action is OFF;
- BUY fill-selection shadow and action are OFF;
- queue, latency, P3, cooldown, inventory, spread, and execution settings are identical;
- ML-OFF disables only the frozen model-controlled parameter surface;
- ML-ON injects the admitted 1-second schedule.

The new entry point is `research/families/f03_causal_13_head/audit/causal_v12_1s_ml_ab_replay.py`. It wraps the existing Python or C++ tick engine and does not modify shared replay semantics. The formal daily entry point also requires the overlay UTC day to equal the replay day and internally loads market state with `load_ml=False`, `require_ml=False`, and `run_ml_inference=False`.

## Permission Boundary

This implementation run did not load a real replay day, run either economic arm, or read PnL. It grants no prediction, action, live, Validation, or holdout authority. A later execution identity must bind the native day denominator, source manifests, overlay manifests, runtime code, engine ABI, and output admission before economic results are read.

## Verification

The overlay and successor tests pass (`20 passed`), and Ruff check/format pass. The private current-baseline config is byte-exact with the immutable v9 pointer at SHA256 `889f605d...`. A transient `8fe2ef16...` hash observed during this implementation was caused solely by an extra blank line introduced and then removed while preparing a separate prospective-collection config; it is not a new baseline identity. No pointer, baseline identity, or live process was changed here. A real 1-second A/B run must still freeze the native day/source, overlay, runtime, engine, and output-admission identities before outcomes.
