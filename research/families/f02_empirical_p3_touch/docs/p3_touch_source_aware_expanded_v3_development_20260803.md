# P3 Touch Source-Aware Expanded v3 Development

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Status: completed Development and historical-transport diagnostic. The pooled 2025+2026 static candidate is closed. The current v2 operational P3 artifact remains unchanged.

## Question

This identity asked whether 93 provider-normalized 2025 days could improve the current unconditional 10-second BTCUSDC touch curve without using exact queue, order lifecycle, or D-1 L2 warmup.

The estimand remained:

\[
P(\text{side-correct aggressive flow reaches distance }d\text{ within }10s).
\]

Distance is measured in `USDC/BTC` from the same-side best quote at the causal window start. Binance Futures official BTCUSDC aggTrades provide the touch label for every panel.

## Frozen Panels

| Panel | Days | BBO authority | Use |
| --- | ---: | --- | --- |
| 2025 provider | 93 | Tardis provider-local causal 100ms BBO | Development fit |
| 2026 current train | 69 | normalized 100ms v2 BBO | Current-curve reproduction and fit |
| 2026 historical validation | 24 | normalized 100ms v2 BBO | Previously-read transport diagnostic |
| 2026 historical test | 24 | normalized 100ms v2 BBO | Previously-read late diagnostic |

The 93-day P3 manifest was frozen before computing any 2025 touch outcome. P3 does not need D-1 admission because every non-overlapping window uses only the same day's last-known BBO at its own start. This does not change the cross-family 67-day D-1 manifest.

## Curve Results

| Curve | Windows | Touch at best | Delta star | Effective kappa |
| --- | ---: | ---: | ---: | ---: |
| 2025 provider | 1,606,854 | 0.719747 | 13.800048 | 0.09021619 |
| 2026 current train | 1,188,168 | 0.743172 | 13.999086 | 0.06735643 |
| Expanded pooled | 2,795,022 | 0.729705 | 13.800048 | 0.08195513 |
| 2026 historical validation empirical diagnostic | 412,574 | 0.750091 | 12.500306 | 0.08747649 |
| 2026 historical test empirical diagnostic | 413,908 | 0.733136 | 9.500348 | 0.13735325 |

The 2026-train curve reproduced current v2 with a maximum probability-grid difference of exactly zero. Relocating the input into the private evidence store therefore did not change the estimator or source bytes.

BUY and SELL were also fitted and reported separately. The 2025 provider curves were nearly symmetric: kappa was `0.09195004` for BUY and `0.08847326` for SELL. Source expansion did not reveal a static side bug.

## Proper Score

The frozen proper score is uniform-distance integrated Brier over the full `0.1` to `120.0 USDC/BTC` grid. Deltas below are:

\[
\text{Brier}_{expanded}-\text{Brier}_{current\ v2}.
\]

| Historical panel | Pooled mean delta/day | 95% day-clustered interval | Improved days |
| --- | ---: | ---: | ---: |
| Validation | +0.00025709 | [-0.00021786, +0.00074713] | 10/24 |
| Test diagnostic | -0.00021497 | [-0.00053257, +0.00009091] | 11/24 |

Validation worsened for both sides: BUY `+0.00026490`, SELL `+0.00024927`. The test point estimates improved for BUY `-0.00022884` and SELL `-0.00020110`, but both intervals crossed zero. The preregistered transport gate therefore failed.

## Full Quote Path

A separate 44-day native full-path diagnostic changed only the P3 artifact. Both arms used causal-v12 ML ON, q90 action OFF, BUY fill-selection action OFF, the same native books, features, queue, latency, cooldown, inventory rules, and corrected markout-side sign.

| Metric, 44 historical native days | Current v2 | Expanded v3 | Change |
| --- | ---: | ---: | ---: |
| Terminal MTM PnL | -124.7455 USDC | -161.2593 USDC | -36.5138 USDC |
| Fills | 15,248 | 17,424 | +14.27% |
| Requotes | 629,383 | 628,902 | -0.08% |
| Inventory time | 5,450.79 BTC-s | 5,591.54 BTC-s | +2.58% |
| Quote-order rows | 811,378 | 789,159 | -2.74% |
| P3 floor binding rate | 26.65% | 33.27% | +6.63 pp |
| Mean raw half-spread | 23.6635 | 21.0063 | -11.23% |
| Mean final pair spread | 50.6266 | 45.2073 | -10.70% |

The Development panel PnL delta was `-14.2451 USDC`, with mean daily delta `-0.6475` and 95% interval `[-1.7420, +0.5050]`. The late diagnostic delta was `-22.2687 USDC`, with mean daily delta `-1.0122` and interval `[-1.7435, -0.3039]`. Only 13/44 days had positive PnL deltas.

At quote timestamps and sides present in both diverged paths, 99.27% of matched prices changed. The mean absolute difference was about 29.74 ticks. This matching is a path diagnostic, not a same-state causal contrast: after the first different fills, inventory and later quote opportunities also diverge. It nevertheless proves that replacing kappa is not a harmless 0.2-USDC delta-star adjustment.

## Decision

The 2025 provider data are admissible and useful for P3 research, but pooling them with the 2026 train set into one unconditional curve does not transport reliably to the 2026 panels. It also narrows the actual quote path enough to increase fills while materially worsening historical terminal PnL.

Therefore:

- do not overwrite or deploy the current v2 artifact;
- do not tune source weights on the already-read 24+24 panels;
- keep the 93-day reach cache and source-specific curves as reusable evidence;
- close `p3_touch_source_aware_expanded_v3` as a static replacement candidate;
- if F02 continues, preregister a conditional P3 using raw distance and volatility-normalized distance, with side and spread/regime explicitly represented.

The next estimand should be structurally closer to:

\[
P_{touch}\left(d,\frac{d}{\sigma_{price}\sqrt H},side,spread,regime\right),
\]

not another pooled static kappa fit.

## Artifacts and Availability

| Artifact | Logical evidence ID | SHA256 | Availability |
| --- | --- | --- | --- |
| Calibration report | `reports/p3_touch_source_aware_expanded_v3_20260803/report.json` | `386336a1c027778060dc78c80130e2076764568425f8de8db9a7749126281a62` | Private evidence store; not distributed with public repository |
| Quote-path report | `reports/p3_touch_source_aware_expanded_v3_quote_path_20260803/report.json` | `03672d5ff18e094af7672ab77bcd17f63f3925327acf79fa88ed40ebabbb005d` | Private evidence store; not distributed with public repository |

The 210 day-level reach caches occupy about 8.17 MiB under the internal cache root. They are derived and reproducible; authoritative reports and manifests remain in the private evidence store. SHA256 values identify retained bytes and are not download links.

Validation and test were already historical diagnostics. No sealed holdout was read, and no prediction, action, operational-replacement, or live authority was created.

## Public References

See the [family README](../README.md), [frozen study Spec](p3_touch_source_aware_expanded_v3_spec_20260803.json), [source-day manifest](p3_touch_source_aware_expanded_v3_day_manifest_20260803.json), [quote-path Spec](p3_touch_source_aware_expanded_v3_quote_path_spec_20260803.json), [curve implementation](../audit/p3_touch_source_aware_expanded.py), and [quote-path comparison implementation](../audit/p3_touch_quote_path_comparison.py).
