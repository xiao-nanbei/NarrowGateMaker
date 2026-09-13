# P3 Touch Volatility-Conditioned v4.1 Development

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Status: historical Development prediction supported under an explicit owner coverage override. Quote mapping, action, operational artifact replacement, and live authority remain unregistered.

## Decision

The project owner changed the per-day causal-context coverage requirement from 98% to 95% after reading the v4 result. This is recorded as an outcome-informed governance decision, not disguised as the original preregistered threshold.

The frozen predecessor remains unchanged:

```text
p3_touch_volatility_conditioned_v4
decision = conditional_v4_prediction_gate_failed_development
coverage threshold = 98%
```

The successor changes exactly one field:

```text
p3_touch_volatility_conditioned_v4_1
evaluation.context_coverage_gate.minimum_fraction = 0.95
```

No model was retrained, no day was removed, no prediction was recomputed, and no other gate changed.

## Why 95% Passes

The minimum observed source-day context coverage was `96.849664%`. The three days below the predecessor's 98% threshold were:

| Day | Coverage |
| --- | ---: |
| 2026-05-28 | 97.533009% |
| 2026-06-01 | 96.849664% |
| 2026-06-04 | 97.741487% |

Missing windows remain censored. They are not forward-filled, interpolated, or assigned synthetic touch outcomes.

Under the 95% successor contract, every gate passes:

| Gate | Result |
| --- | --- |
| Context coverage | pass |
| Historical proper score | pass |
| Calibration | pass |
| Provider/native prediction transport | pass |
| Distance monotonicity | pass |
| Historical Development prediction | pass |

The inherited quantitative evidence remains:

- all four native historical proper-score cells passed;
- both metrics improved on 24/24 Validation and 24/24 test-diagnostic days;
- all 48 supported calibration cells passed;
- maximum source-transport prediction MAE was about `0.001610`;
- 197,231,404 monotonicity comparisons had zero violations.

The authoritative successor decision is:

```text
historical_development_prediction_supported_owner_coverage_override
```

## Authority Boundary

v4.1 supports the conditional touch-probability prediction structure on the already-read historical Development panels. It is not independent confirmation and does not prove maker PnL improvement.

It makes a separate conditional-P3-to-quote research identity eligible for registration. That successor must freeze the mapping from the full conditional curve to quote coordinates and run a full-path economic comparison against current v2. It may not silently collapse v4.1 back into one global kappa, and it must measure marginal-fill value, campaign PnL, and tails rather than treating more touches or fills as success.

The current v2 artifact remains the operational artifact. No live or backtest baseline parameter was changed by this prediction decision.

## Artifacts and Availability

| Artifact | Logical evidence ID | SHA256 | Availability |
| --- | --- | --- | --- |
| [Public successor Spec projection](p3_touch_volatility_conditioned_v4_1_spec_20260803.json) | Public repository | `b550991fae52fb3fd64fa3f92e6b59aeb893593db37d9496661175aa16fca20f` | Public repository |
| Executed private successor Spec source | Private evidence store | `4453af96039f4430de4ee353a0a38e11a1060ea265f8defc76c42223b7dd01ae` | Private evidence store; not distributed with public repository |
| Authoritative result | `reports/p3_touch_volatility_conditioned_v4_1_20260803/result.json` | `3e87103fedd33db5860424ba2fe4eab2d580cbd207631af6d57a9a92db1decae` | Private evidence store; not distributed with public repository |
| Output manifest | `reports/p3_touch_volatility_conditioned_v4_1_20260803/manifest.json` | `fbbaf4b2354582ca73da1402827093b29e66186e5c78b17264a20728747b3666` | Private evidence store; not distributed with public repository |
| Current v2 artifact | `reports/p3_touch_recalibration_normalized100ms_v2_20260725/p3_touch_10s_params.json` | `cedec34851454b643be746746a1dd4bcc7e13807985c14e78504643ad6e71714` | Private evidence store; not distributed with public repository |

The successor verifies all 26 predecessor manifest files before admitting the override. SHA256 values identify retained bytes and are not download links.

## Public References

See the [family README](../README.md), [failed v4 predecessor](p3_touch_volatility_conditioned_v4_development_20260803.md), [conditional quote-mapping successor](p3_touch_conditional_curve_quote_mapping_v1_development_20260803.md), and [coverage-override implementation](../audit/p3_touch_volatility_conditioned_coverage_override.py).
