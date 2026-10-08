# P3 Touch Volatility-Conditioned v4 Development

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Status: completed Development and historical native OOF diagnostics. The conditional prediction structure passed every model-quality gate, but the frozen identity failed its per-day input-coverage gate. No prediction, quote, action, artifact-replacement, or live authority was created. The current v2 operational artifact remains unchanged.

## Question

This identity tested whether the static unconditional P3 curve should be replaced by a causal conditional surface:

\[
P_{touch}\left(
d,
\frac{d}{\sigma_{price}\sqrt{10s}},
side,
spread,
regime
\right).
\]

The estimand remains a 10-second touch probability, not order arrival, queue fill, placement fill, or action value. Distance is measured in `USDC/BTC` from the same-side best quote at the window start.

## Frozen Structure

The model uses raw distance, volatility-normalized distance, BUY/SELL, current spread, fast and slow causal volatility, and a causal volatility regime. Raw distance and normalized distance both carry decreasing monotone constraints. Source identity and calendar year are excluded from the trading feature vector; they are used only for translation and transport diagnostics.

Training combines 93 provider-normalized 2025 days with strictly prior native 2026 days. The 48 previously read native historical days are evaluated in four chronological 12-day OOF folds. Each fold calibrates only on its final eight strictly prior native days with one shared positive-slope Platt transform.

The registered graph is `p3_touch_volatility_conditioned.v4`, with SHA256 `210ba301b14cc134ba4f7c93db973d535f3f8ea10242d5872b7afa0fb542612a`.

The reusable context cache contains 222 source-day artifacts covering 210 distinct UTC dates plus 12 source-overlap views. It occupies about 90.57 MiB in the private evidence store. Authoritative models and reports occupy about 2.55 MiB there.

## Prediction Results

Brier deltas below are:

\[
\mathrm{Brier}_{conditional\ v4}-\mathrm{Brier}_{current\ v2}.
\]

| Historical native panel | Metric | Pooled mean delta/day | 95% day-clustered interval | Improved days |
| --- | --- | ---: | ---: | ---: |
| Validation | Uniform full-distance | -0.00590204 | [-0.00833032, -0.00412380] | 24/24 |
| Validation | Current policy support | -0.01393824 | [-0.02041990, -0.00936985] | 24/24 |
| Test diagnostic | Uniform full-distance | -0.00461838 | [-0.00539819, -0.00392063] | 24/24 |
| Test diagnostic | Current policy support | -0.01105226 | [-0.01266715, -0.00959485] | 24/24 |

BUY and SELL mean deltas were negative in every panel and metric. The four frozen proper-score cells therefore passed without pooling away a weak side.

Calibration also passed in all 48 supported non-degenerate cells across side, fast-volatility quartile, slow-volatility quartile, and causal regime. The largest candidate-minus-current integrated calibration-error delta was `-0.00538532`, below the frozen maximum worsening of `+0.002`.

The final curve is not a near-constant probability surface. Representative states at `d=14 USDC/BTC` produced touch probabilities from about `0.0189` in a calm state to `0.2439` in a higher-volatility balanced state. These values are diagnostics only; they are not quote inputs.

## Structural Contracts

The final output passed:

- zero monotonicity violations in 197,231,404 adjacent-distance comparisons;
- zero positive monotonicity difference after shared calibration;
- all 48 supported calibration cells;
- all four historical proper-score cells;
- provider/native source-prediction transport.

Across the six historical source-overlap diagnostics, the maximum mean absolute provider/native prediction difference was about `0.001610`, below the frozen `0.01` gate. The six outcome-blind fit overlap days produced zero median level and volatility correction. This means no systematic median translation was required on those days; it does not claim that the two source tapes are byte-identical.

## Coverage Failure

The full identity nevertheless failed its frozen per-day context-coverage gate of 98%. Three native historical-validation days were below the threshold:

| Day | Retained windows | Coverage |
| --- | ---: | ---: |
| 2026-05-28 | 8,421 | 97.5330% |
| 2026-06-01 | 8,362 | 96.8497% |
| 2026-06-04 | 8,439 | 97.7415% |

The missing windows form real contiguous BBO interruptions. The largest spans were approximately 11.8 minutes on May 28, 11.8 minutes on June 1, and 11.2 minutes on June 4. They are not a cache-key, causal-cutoff, or model-training error.

The coverage failure is outcome-blind, but the frozen v4 contract did not authorize dropping failed days after inspection. The authoritative decision therefore remains:

```text
conditional_v4_prediction_gate_failed_development
```

The strong model-quality diagnostics must not be reinterpreted as a passing identity.

## Decision

This run supports the structural conclusion that conditional P3 is more appropriate than a pooled static kappa. It does not authorize a quote mapping.

Therefore:

- keep the current v2 operational artifact unchanged;
- keep v3 closed as a pooled static replacement;
- keep the v4 model and context caches as Development evidence;
- do not export a scalar kappa from v4;
- do not create a conditional-P3 quote action from this failed identity;
- do not modify the 98% coverage gate or silently drop the three days;
- if F02 continues, create an outcome-blind input-eligibility successor or repair the native source identity before rerunning prediction;
- only after a prediction identity passes may a separate full-path conditional-curve-to-quote identity be registered.

Validation and test dates were already historical diagnostics. No sealed holdout was read.

## Artifacts and Availability

| Artifact | Logical evidence ID | SHA256 | Availability |
| --- | --- | --- | --- |
| [Frozen v4 Spec](p3_touch_volatility_conditioned_v4_spec_20260803.json) | Public repository | `210ba301b14cc134ba4f7c93db973d535f3f8ea10242d5872b7afa0fb542612a` | Public repository; hash is the registered graph identity |
| Authoritative report | `reports/p3_touch_volatility_conditioned_v4_20260803/report.json` | `bf3fb78215708649b2c42784b64e34783b6440276a16881ded3c7ec57f49423b` | Private evidence store; not distributed with public repository |
| Output manifest | `reports/p3_touch_volatility_conditioned_v4_20260803/manifest.json` | `53c283fac31319ff2896c44bff9dfdb0a32c76d86ca52816deb85124873086c6` | Private evidence store; not distributed with public repository |
| Current v2 artifact | `reports/p3_touch_recalibration_normalized100ms_v2_20260725/p3_touch_10s_params.json` | `cedec34851454b643be746746a1dd4bcc7e13807985c14e78504643ad6e71714` | Private evidence store; not distributed with public repository |

All 26 manifest-bound files passed byte-size and SHA256 verification. SHA256 values identify retained bytes and are not download links.

## Public References

See the [family README](../README.md), [v4.1 coverage-override successor](p3_touch_volatility_conditioned_v4_1_development_20260803.md), [volatility-conditioned implementation](../audit/p3_touch_volatility_conditioned.py), and [window-context implementation](../audit/p3_touch_window_context.py).
