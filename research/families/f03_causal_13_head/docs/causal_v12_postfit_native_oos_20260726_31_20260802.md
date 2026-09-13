# Causal v12 Post-Fit Native OOS Diagnostic

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Status: Development/post-fit diagnostic closed. No prediction, action, research promotion, or live authority is created by this result.

## Evidence Boundary

The candidate dates `2026-07-26` through `2026-07-31` were not used by the 66-day 2025 v12 training panel and were not part of either previously evaluated 22-day native v12 panel. Data-quality grades and the five-day primary panel were frozen before v12 outcomes were calculated.

These dates are not globally unseen. They have appeared in live/F10 operational attribution, so this run is a family-specific post-fit OOS diagnostic rather than an independent confirmation or sealed holdout. The v12 predictions were read before the full-path economic run; the five-day v12 PnL comparison was not.

## Data Identity

All 90 official-source availability checks passed across six UTC dates and 15 sources. BTCUSDC normalized native L2 passed sequence reconstruction on all six dates. The formal quality split is:

| Day | Grade | Coverage | Maximum internal gap | Use |
|---|---:|---:|---:|---|
| 2026-07-26 | A | 99.9973% | 2.676s | Primary |
| 2026-07-27 | A | 99.9968% | 3.152s | Primary |
| 2026-07-28 | B | 99.3460% | 419.735s | Sensitivity only |
| 2026-07-29 | A | 99.9973% | 2.571s | Primary |
| 2026-07-30 | A | 99.9973% | 2.668s | Primary |
| 2026-07-31 | A | 99.9961% | 3.768s | Primary |

The primary denominator therefore contains five Grade-A native lifecycle days. The Grade-B date cannot rescue a failed primary gate.

## Prediction Transport

Only 5 of 13 heads passed the pre-frozen per-head transport gate.

- Classification median AUC was `0.528418`, but only 2 of 7 classification heads had positive Brier skill.
- `dir_10s` and `tox_ask_10s` passed; `dir_30s`, `dir_60s`, and three other toxicity heads failed absolute-probability transport.
- All six regression heads retained positive rank direction.
- The three return heads had only very small positive RMSE skill.
- The three volatility heads ranked well but had negative RMSE skill, so their absolute scale did not transport.

The largest standardized feature shifts included `oi_log` (`+8.45`), `close` (`-4.02`), and `cv_ref_perp_basis_bps` (`+3.41`). Ranking transport is visible; absolute probability and volatility-scale transport are not supported.

## Full-Path ML-OFF/ON

Both arms used the same native queue/lifecycle replay, empirical P3, latency, cooldown, inventory rules, markout sign, and safety configuration. q90 and the BUY selector were disabled in both arms. Only the 13-head ML switch changed.

| Metric | ML-OFF | ML-ON | Result |
|---|---:|---:|---|
| Terminal MTM PnL | -11.6080 USDC | -5.5746 USDC | +6.0334 USDC total |
| Mean daily PnL delta |  |  | +1.2067 USDC/day |
| Day-clustered 95% CI |  |  | [-0.2435, +2.5635] USDC/day |
| Positive-day rate |  |  | 3/5 |
| Fills | 1,468 | 1,236 | 84.20% retention |
| Inventory-time ratio |  |  | 1.0270 |
| Closed campaigns | 426 | 324 | diagnostic |
| Campaign q10 | -0.16564 | -0.19310 | worse |

ML-ON improved the terminal PnL point estimate and reduced both multi-level LONG and SHORT losses in aggregate. Those improvements are not statistically stable over five UTC days. The primary PnL lower bound remained negative, fill retention failed the 90% floor, campaign q10/CVaR non-worsening failed, and SELL 30-second maker value failed its side-specific non-inferiority gate.

The canonical result is therefore:

`decision = close_causal_v12_economic_screen_on_historical_native_panels`

`ranking_score = null`

## Authority

This result does not rewrite the separate owner-authorized reversible live canary record. It does show that the canary must not be described as having passed the frozen v12 prediction or economic gates. No threshold, calibrator, feature list, or panel was changed after reading these outcomes, and neither Validation nor a sealed holdout was created from these dates.

Machine-readable artifacts:

- Prediction report SHA256: `82e4f679a07e72bebd9bae76de429bc31180a94df57c37be730b5f584ecd5a56`
- Full-path report SHA256: `42996932fabbf22c3d9e4a69676958404d2698dd00eadfbd8a4579c1d483f654`
- Full-path Spec identity: `ac2369a925f32976e99e1d7ae6e4b4c0db7e49818cc26362229e6d2c2afa7ea4`
