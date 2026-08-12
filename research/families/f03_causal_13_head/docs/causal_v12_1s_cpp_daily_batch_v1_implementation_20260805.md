# Causal-v12 1s C++ daily batch v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Status: implemented and accepted for targeted mechanics/performance testing. No full-day or 66-day materialization was started. This identity grants no training, economic, action, live, or baseline authority.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Scope

The F03-specific C++ batch kernel consumes raw completed 1-second local and reference bars, exact-bucket execution L2 observations, and past-only metrics. It emits the frozen 173 trainable features in exact schema order together with per-feature validity, source timestamp, feature-ready timestamp, observation count, and lag state. It never accepts Python-computed feature values and never reuses or forward-fills a 10-second feature row.

The batch ABI is `causal_v12_1s_cpp_daily_batch.v1`; the feature-order SHA256 is `5a6947850dfabefbf4e36bdbe986e39c96324e3714efb16d3410a4443ea1b797`. The native engine sorts and validates each raw daily source once, precomputes long rolling state once, and evaluates many canonical cutoffs in one pybind call. This removes per-cutoff full-history copies, scans, and Python/C++ calls.

The panel materializer now requires an explicit engine:

- `cpp_batch`: the only bulk-capable engine; cache identity binds the batch ABI, feature order, bridge, C++ kernel, and pybind source hashes.
- `python_oracle`: retained only for small parity checks and refuses full-day materialization.
- omitted or unknown engine: fail closed.

The C++ engine emits an engine-native canonical row fingerprint. Numerical Python/C++ parity remains the existing `rtol=2e-12`, `atol=2e-12` contract; bitwise equality with the Python row fingerprint is not claimed, and cache identities cannot be reused across engines.

## Targeted parity

Synthetic coverage includes all 173 fields and every metadata channel, cutoff-minus-1 ms visibility, cutoff exclusion, next-second perturbation invariance, lag-state transitions, supported synthetic flat gaps, rejected physical holes, and long-window EWM/6h/24h state.

For the admitted physical source bundle for 2025-08-02, using exact D-1 warmup from 2025-08-01, deterministic 100-row and 1,000-row C++ panels were compared with the pre-existing Python-oracle panels:

| Rows | Feature cells | Null mismatches | Metadata mismatches | Numeric result | Max absolute difference |
|---:|---:|---:|---:|---|---:|
| 100 | 17,300 | 0 | 0 | all cells pass tolerance | 9.313225746e-10 |
| 1,000 | 173,000 | 0 | 0 | all cells pass tolerance | 3.492459655e-09 |

The largest absolute difference was in `taker_signed_quote_sum_60s`; it remains inside the frozen relative/absolute tolerance. Exact clocks, counts, validity, lag states, row order, and null locations matched.

## Real small-sample benchmark

The benchmark reads the same `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` physical source bundle and measures source probe/load, native-engine preparation, feature computation, and Parquet write separately. The old comparator is the previously frozen Python-oracle benchmark on the same date and deterministic cutoff construction.

| Rows | C++ feature rows/s | Python rows/s | Compute speedup | C++ full-day extrapolation | Python extrapolation | End-to-end extrapolated speedup |
|---:|---:|---:|---:|---:|---:|---:|
| 100 | 1,206.56 | 6.1268 | 196.92x | 84.48s | 14,114.03s | 167.06x |
| 1,000 | 1,303.23 | 6.0671 | 214.80x | 76.88s | 14,251.90s | 185.37x |

For 1,000 rows, source probe/load plus engine preparation took 8.27 seconds, feature computation took 0.767 seconds, Parquet write took 0.027 seconds, and peak process RSS was 1,542,881,280 bytes. The 76.88-second number is a linear small-sample extrapolation for 86,400 rows, not a measured full-day run.

The requested 100x performance gate passes for both feature throughput and the end-to-end extrapolation. No full-day run is needed to establish that the old 3.9-hour Python-row loop is no longer the structural blocker.

## Remaining boundary

Physical-source parity has been demonstrated only on the deterministic small sample from the admitted 2025-08-02 provider-normalized source bundle. A future bulk admission must still run a full 86,400-row artifact validation, report actual wall time and peak memory, and bind its complete source manifest before training may begin. Native 2026 source parity and all 66 training days remain outside this implementation identity.

Labels, predictions, training, PnL, strategy code, and live deployment were not read or changed. The machine-readable identity and hashes are in `causal_v12_1s_cpp_daily_batch_v1_implementation_20260805.json`.

Final focused verification reports 65 passed and 1 skipped test, with Ruff, strict C++20 `-Wall -Wextra -Werror` syntax compilation, JSON parsing, and `git diff --check` all passing. The optional skip is an unavailable stale local probe and does not cover the active batch implementation.
