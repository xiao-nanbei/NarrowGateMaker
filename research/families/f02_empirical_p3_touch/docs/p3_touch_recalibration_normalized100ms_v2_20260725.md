# P3 Touch Recalibration On Normalized 100ms v2

Date: 2026-07-25

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Status: calibration evidence only; no live artifact or strategy change.

## Identity

The original chronological model split and Binance aggTrades inputs were held fixed. Only the BTCUSDC BBO root changed from the historical mixed-cadence directory to the logical data location:

```text
${NARROWGATE_DATA_ROOT}/normalized_l2_100ms_v2/bbo
```

The comparison covers the same 117 UTC days: 69 train, 24 validation, and 24 test days. It hashes 117 BBO plus 117 aggTrades files.

## Artifacts and Availability

| Artifact | SHA256 | Availability |
| --- | --- | --- |
| Combined BBO/trade input identity | `23ccaf9a3e6769cc7f263eb5b00cf9b799df3d0a230b7f86b82f7bf34d8bc300` | Source bytes in private evidence store; not distributed with public repository |
| Recalibrated 5s artifact | `888bffec950006ba798380e2dc1bacff9f59229b462b01b746abf2721d547ab4` | Private evidence store; not distributed with public repository |
| Recalibrated 10s artifact | `cedec34851454b643be746746a1dd4bcc7e13807985c14e78504643ad6e71714` | Private evidence store; not distributed with public repository |

The artifacts have logical evidence root `reports/p3_touch_recalibration_normalized100ms_v2_20260725/`. They do not overwrite the operational model bundle, and the SHA256 values identify retained bytes rather than providing download links.

## Result

| Horizon | Metric | Mixed BBO v1 | Normalized 100ms v2 | Relative change |
|---|---|---:|---:|---:|
| 5s | delta star | 10.999128 | 10.999128 | 0.000% |
| 5s | effective kappa | 0.08311357 | 0.08325351 | +0.168% |
| 5s | train probability at delta star | 0.15962199 | 0.15959170 | -0.019% |
| 10s | delta star | 13.999086 | 13.999086 | 0.000% |
| 10s | effective kappa | 0.06743811 | 0.06735643 | -0.121% |
| 10s | train probability at delta star | 0.20698745 | 0.20700694 | +0.009% |

Validation probability at delta star changed from `0.16111405` to `0.16039733` at 5s and from `0.20766477` to `0.20675981` at 10s. Test probability changed by less than `0.00006` in both horizons.

Across day/side cells, old-versus-new touch-rate correlation was at least `0.99995` for best-price, 10-USDC, and 20-USDC thresholds. Mean absolute daily-rate differences ranged from `0.00022` to `0.00049`.

## Interpretation

The mixed-cadence BBO identity did not materially change the P3 touch curve under this non-overlapping-window estimand. P3 asks whether side-correct aggressive flow reaches a quote distance; it does not model queue priority, cancel/refill paths, or fill-after-touch conversion. Its stability therefore does not rescue old queue, fill, campaign, or action-uplift results.

The former numeric P3 values may now be described as independently reproduced to within about 0.2% on the normalized BBO identity. The old artifact remains superseded because its input manifest is wrong for current research. Any future promotion or deployment must bind the new artifact and hashes explicitly.

## Public References

See the [family README](../README.md), [calibration implementation](../audit/p3_touch_calibration.py), and [source-aware v3 successor report](p3_touch_source_aware_expanded_v3_development_20260803.md). `${NARROWGATE_DATA_ROOT}` is a portable logical root; its workstation-specific resolution is intentionally not published.
