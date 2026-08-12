# External information decay v1: Development result

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Status

`external_information_decay_v1` completed Development screening on 2026-07-27. It retains the external-venue model family and replaces the former preferred `1s/3s/5s` target list with a predeclared complete integer-second curve.

This is prediction evidence only. Test and late panels were not read, and the result has no live-action authority.

## Frozen identity

- target: future BTCUSDC direction;
- comparison: M1 with Bitget/Bybit/OKX spot and perpetual state minus M0 with local BTCUSDC and Binance BTCUSDT state;
- support: every integer horizon from 1 through 18 seconds;
- visibility: right-edge causal one-second states with one whole-second external visibility delay;
- training: 69 chronological days, with a past-only seven-day inner early-stopping tail and one-day inner embargo;
- Development screening: 20 common UTC days, 2026-05-12 through 2026-06-12;
- selection: UTC-day clustered 2,000-trial bootstrap and a two-sided 95% simultaneous max-absolute band over all 18 horizons.

The 18-second upper support is the ceiling of the Development current-order active-lifetime p95 (`17.378s`). Choosing p95 is a judgmental engineering rule, not a theoretical constant.

## Result

The selected statistic is the mean paired daily AUC gain, not the pooled-row AUC difference.

| Horizon | Mean daily AUC gain | Positive days | Simultaneous 95% LCB | Best-horizon bootstrap frequency |
|---:|---:|---:|---:|---:|
| 1s | +0.002910 | 18/20 | +0.001393 | 76.5% |
| 2s | +0.002728 | 20/20 | +0.001211 | 14.0% |
| 3s | +0.002292 | 19/20 | +0.000775 | 0.6% |
| 4s | +0.002452 | 18/20 | +0.000935 | 8.0% |
| 5s | +0.002168 | 15/20 | +0.000651 | 0.05% |
| 6s | +0.001781 | 17/20 | +0.000264 | 0.05% |
| 7s | +0.001677 | 16/20 | +0.000160 | 0.0% |
| 8s | +0.001491 | 16/20 | -0.000026 | 0.0% |
| 9s | +0.001072 | 13/20 | -0.000445 | 0.0% |
| 10s | +0.001290 | 14/20 | -0.000226 | 0.05% |
| 18s | +0.001019 | 11/20 | -0.000498 | 0.0% |

The full 18-point curve is stored in `direction_horizon_decay_curve.csv`. Horizons 1 through 7 have positive simultaneous lower bounds. The frozen rule selects 1 second because it has the largest simultaneous lower bound. The empirical post-peak half-gain horizon is 9 seconds.

The result supersedes the historical statement that 3 seconds is a naturally preferred target. Under the current model, data identity and visibility delay, the strongest Development increment is at the shortest observable horizon. The one-second archive cannot determine whether the true peak lies below one second, so the selected value is a boundary result rather than a universal market horizon.

## Execution amendment

The pre-outcome specification froze model-code SHA256 `25be18653e8ac3445e4e91fc572e39dcbd02a7658b7a3605c2b7fb7678af45c4`. The first execution completed horizons 1 through 9, then failed before fitting 10 seconds because `dir_10s` collided with the historical 10-second head name.

The amendment made target identity cadence-aware:

- `fast1s/dir_10s` now means dynamically derived `label_fast_dir_10s`;
- historical `10s/dir_10s` continues to mean `label_dir_10s`;
- strict resume validates the existing model, metadata and all requested daily score rows before skipping a completed target.

No feature, label, model parameter or score path used by horizons 1 through 9 was changed. The amended model-code SHA256 is `5e420330e4ee1d8e44b8f4b45f6632936ac4083e3701fc2be1458f95ca34ce95`. The resumed run validated and retained the first nine target pairs, then fitted horizons 10 through 18.

The same audit also corrected two earlier fail-open paths before any v1 target was fitted: missing/delayed external age was formerly zero-filled as fresh, and outer Validation was formerly available for LightGBM early stopping. Missing age now maps to `10,000ms`, and early stopping uses only the past-only inner Development split.

## Artifact integrity

Output root:

```text
${NARROWGATE_RETIRED_DATA_ROOT}/model_runs/external_information_decay_v1_20260727
```

Integrity checks passed for:

- 36 unique models: 18 horizons times M0/M1;
- 720 unique daily score rows: 18 horizons times two profiles times 20 days;
- all model and metadata files;
- the metrics, daily metrics, split and curve hashes recorded by `direction_horizon_selection.json`.

Selection artifact SHA-bound inputs include manifest `04f8abe8db3c947b47e94f0dbe31be291f6fc30759ee4c15fe12597979a90c2a`.

## Decision

Retain `external_venue_model.py` and its tests as the current external information-decay research entrypoint. Do not delete or archive this family.

The 1-second direction head is eligible only as the selected prediction candidate for a separately frozen confirmation split. It must not be wired to cancel, widen, recenter or live quoting without a distinct action experiment. Internal feature lookbacks remain a fixed multiscale basis; this experiment selects the target horizon, not an optimal feature-kernel shape. A later receive-time event study is required for subsecond decay and dynamic event-time kernels.
