# P3 Policy-Visible Decision-Cadence Transport v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

## Decision

The frozen conditional P3 v4.1 surface retains strong prediction quality on the F06 policy-visible quote-coordinate clock, but this historical Development identity stops before direct value fitting.

The prediction-quality components passed. The complete identity did not pass because its immutable support is only 28 distinct UTC days and three OOF folds, below the frozen requirements of 30 days and four folds.

```text
prediction_quality_component_passed = true
decision_cadence_transport_supported = false
direct_value_registration_eligible = false
value_model_fit_authorized = false
action_authority = false
live_authority = false
```

This is historical transport under a sampled AWS Tokyo visibility-age profile. It is not exact AWS receive-time transport or independent confirmation.

After the run, an implementation review narrowed this further: the current BBO coordinate exactly reproduces the F06 sampled quote coordinate, while the 60-second context is a deterministic synthetic visibility sensitivity. F06 did not persist the corresponding 61-point feature history, so this identity is not exact F06 feature-state parity. The frozen machine report remains unchanged; the [contract errata](p3_touch_policy_visible_decision_cadence_transport_v1_contract_errata_20260803.md) records this narrower interpretation.

## Clock Correction

The predecessor audit compared F06 quote coordinates with raw last-known normalized BBO. F06 actually sampled a visibility age from the frozen 2026-07-18 Tokyo profile using keyed SplitMix64 with seed `20260718` before choosing the visible BBO.

The successor reconstructs the F06 sampled current BBO at each decision. For the 60-second volatility context it applies the same deterministic keyed sampler independently to 61 synthetic one-second query points. It requires the reconstructed current BUY and SELL BBO ticks to equal the F06 decision coordinates exactly. Unsupported rows remain in the denominator and are never filled from a canonical 10-second context.

Across 467,093 baseline-eligible decisions:

| Measure | Result |
|---|---:|
| Supported contexts | 460,893 |
| Pooled context coverage | 98.6726% |
| Minimum daily context coverage | 98.3724% |
| Unsupported 60s histories | 6,144 |
| Unavailable or stale current BBO | 56 |

The predecessor's roughly 68%-69% side coverage was therefore primarily a missing visibility-layer mismatch, not evidence that v4.1 inherently fails at the policy-visible quote clock.

## Prediction Evidence

All score intervals are day-clustered historical Development diagnostics. Negative Brier deltas favor v4.1 over static P3 v2.

| Side | Surface | Mean Brier delta | 95% interval |
|---|---|---:|---:|
| BUY | seven-distance grid | -0.009663 | [-0.012454, -0.007143] |
| BUY | current distance | -0.009671 | [-0.012443, -0.007156] |
| SELL | seven-distance grid | -0.009826 | [-0.012407, -0.007377] |
| SELL | current distance | -0.009806 | [-0.012327, -0.007416] |

Calibration also improved materially:

| Side | Surface | v2 IACE | v4.1 IACE |
|---|---|---:|---:|
| BUY | seven-distance grid | 0.051870 | 0.002903 |
| BUY | current distance | 0.051901 | 0.002917 |
| SELL | seven-distance grid | 0.053786 | 0.003040 |
| SELL | current distance | 0.053749 | 0.003025 |

The final side-specific outputs made 2,762,748 ordered-distance comparisons with zero monotonicity violations. Minimum candidate-distance coverage was 97.3075% for BUY and 98.2830% for SELL.

## Why Value Fitting Did Not Start

Two independent support contracts block F05:

1. This transport identity has 28 days and three OOF folds, versus the frozen 30-day/four-fold requirement.
2. The previously frozen joint-quote preflight has only 282 paired quote buckets, and the minimum side x role x action cell contains one fill versus the required 30.

The second blocker remains even though the corrected P3 clock now clears its coverage, score, calibration, and monotonicity components. No direct quantity-weighted USDC model was fit, no simultaneous-LCB action was selected, and no F09 randomized identity was created.

## Interpretation Boundary

Supported interpretation:

> Under the frozen sampled-visibility sensitivity, side-specific conditional P3 v4.1 predicts ten-second aggressive reach substantially better than static P3 v2 on the available F06 historical decision denominator.

Unsupported interpretations:

- P3 is a fill CIF or queue-conversion model.
- Prediction improvement implies positive quote value.
- The route has exact AWS receive-time transport.
- The generated 60-second context is exact F06 persisted feature-state parity.
- The result authorizes a value model, quote mapping, action, or live change.
- The failed scalar-compression adapter may be revived.

## Frozen Artifacts and Availability

| Artifact | SHA256 | Availability |
| --- | --- | --- |
| [Public Spec projection](p3_touch_policy_visible_decision_cadence_transport_v1_spec_20260803.json) | `f5bd5f5b36553c2fcd43c72e6f389de9b36d32c1f36de9b7c68105289d2c48fd` | Public repository |
| Executed private Spec source | `331a3c59dca0a79e36ea6bea0c68eef17ffeefba167e66171a6151f3c29c19be` | Private evidence store; not distributed with public repository; canonical Spec identity `be341f6268faacd6fd35aa5d1b0791c0e421b6bea23efd887ebf3c0b188cca7c` |
| Authoritative report | `ccb6ef4c489f4573ae636fa1def044d56c3256ba09e8972ccb866353584becf1` | Private evidence store; not distributed with public repository |
| Authoritative manifest | `fc2af579da8a0a1f754e4a991a6aca96bfb3187651d657de5c080126b2c0822c` | Private evidence store; not distributed with public repository |
| Input manifest | `cf869904f130da48fe791d9e4ff889747dea20aebd82ce9c47f6f68cbb5ad0bc` | Private evidence store; not distributed with public repository |
| Daily metrics | `cdd9d0c0b1733da22be8a6b1d8f3a1bb5b2a59e4d305ccb7337d98c5e55ed0e5` | Private evidence store; not distributed with public repository |
| Context audit | `08614b07ee7ab6288b4012832239ccd73033ca1a972afb4788b14093031f881f` | Private evidence store; not distributed with public repository |

The retained output has logical evidence ID `reports/p3_touch_policy_visible_decision_cadence_transport_v1_development_20260803/`. SHA256 values identify retained bytes; they are not download links.

## Public References

See the [family README](../README.md), [contract errata](p3_touch_policy_visible_decision_cadence_transport_v1_contract_errata_20260803.md), [policy-visible context implementation](../audit/p3_touch_policy_visible_decision_context.py), [transport implementation](../audit/p3_touch_decision_cadence_transport.py), and [runner](../audit/run_p3_touch_policy_visible_decision_transport.py).
