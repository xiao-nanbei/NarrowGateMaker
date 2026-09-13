# Causal-v12 1s C++ feature parity v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Status: the F03-specific C++ 1-second feature kernel and pybind ABI are implemented and locally parity-tested. This identity grants no training, economic, action, live, or baseline authority.

The native kernel independently consumes raw completed 1-second local bars, exact-bucket execution L2 observations, past-only metrics observations, and completed reference-perp bars. It does not accept Python-computed feature rows, 10-second feature rows, labels, predictions, PnL, or economic outcomes.

The output is fixed to the frozen causal-v12 trainable ABI:

- 173 columns in `TRAINABLE_FEATURE_ORDER`;
- feature-order SHA256 `5a6947850dfabefbf4e36bdbe986e39c96324e3714efb16d3410a4443ea1b797`;
- one value-validity bit, source timestamp, feature-ready timestamp, observation count, and lag-state per column;
- cutoff-exclusive 1-second causality and decision-time validation;
- the frozen 2025-2026 calendar and America/New_York DST semantics.

The parity suite checks every value and every dynamic state against the Python authority. It covers cutoff-minus-1 ms visibility, cutoff-time source exclusion, next-second perturbation invariance, exact L2 lateness, metrics staleness, missing reference state, long-window warmup transitions, explicit contiguous no-trade bars, multi-second gap rejection, supported holidays, and both US DST boundaries. Numeric comparison uses `rtol=2e-12` and `atol=2e-12`; clocks, counts, validity, lag-state, order, and hashes are exact. The focused parity suite reports 13 passed tests; the related F03 schema, generator, and cutoff regression set reports 55 passed tests.

This closes the synthetic Python/C++ feature-computation gap only. Physical source parity remains blocked on admission of the real 1-second daily source materializer, its D-1 warmup/source manifests, execution-L2 quality identity, metrics CSV clock contract, and real-day fingerprint comparison. No panel was materialized and no model was trained.

The machine-readable identity and implementation hashes are in `causal_v12_1s_cpp_feature_parity_v1_implementation_20260805.json`.
