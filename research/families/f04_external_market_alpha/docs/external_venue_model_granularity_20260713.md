# External-venue model granularity audit (2026-07-13)

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Current status (2026-07-27): this is a historical cadence diagnostic, not a description of the current production model identity. The trade-derived external 1s/3s direction result remains diagnostic; exact maker outcome comparisons that used the former local BTCUSDC denominator are withdrawn. The old “production-style 10-second bundle” was superseded by later causal rebuilds and ultimately by the `causal-v7` contract, whose 13-head inference is currently disabled. No result here authorizes a live external feature.

## Current successor contract

The code and test pair is retained as the active external-information-decay research entrypoint. The historical fixed `1s/3s/5s` heads below are no longer the default target identity:

- `external_venue_trade_state.v4` caches one causal local label-close path;
- an experiment must declare a complete positive integer-second horizon grid;
- labels for every declared horizon are derived from that same path when each UTC day is read, so changing the grid does not duplicate market-state caches;
- the former 10-second family is available only for historical reproduction;
- LightGBM early stopping uses a past-only tail inside Development, never the outer Validation panel;
- M1-minus-M0 daily AUC is evaluated jointly over the full declared curve;
- a horizon is formally selected only when its simultaneous day-cluster 95% lower band is positive. The diagnostic peak and post-peak half-gain horizon are also reported.

The old split field named `validation` has already been inspected by the 2026-07-13 experiment. A successor may use that slot only as Development screening; its name does not make it untouched evidence. Formal confirmation requires a newly frozen family split over eligible external-data days. The selection artifact records this distinction explicitly.

The grid endpoints remain judgmental engineering support bounds and must be recorded in the experiment manifest. They are not theoretical constants. The historical one-second archive permits only integer-second target horizons; a subsecond decay curve requires the receive-time event tape.

As a pre-spec diagnostic, the current Development placement panel contains 659,358 activated orders with observed terminal lifetimes across 40 UTC days. Its active-lifetime p25/p50/p75/p95 values are `5.005s / 5.706s / 7.633s / 17.378s`. A successor decay family may therefore freeze the complete `1..18s` grid, with 18 seconds obtained by rounding the Development p95 upward. The choice to use p95 is still a judgmental support rule; neither 18 seconds nor p95 is a market-theory constant. No outcome from a future external-direction model was inspected to obtain this bound.

The 2026-07-27 audit also fixed two stale-state bugs. Delayed or missing `source_age_ms` values were previously zero-filled and could look maximally fresh; they now fail closed to `10,000ms`. Missing age features receive the same stale sentinel during model input cleaning.

The frozen `external_information_decay_v1` Development run is now complete. Across the declared `1..18s` curve, horizons 1 through 7 retain a positive day-clustered simultaneous 95% lower bound. The formal rule selects 1 second: its mean paired daily M1-minus-M0 AUC gain is `+0.002910`, with 18/20 positive days and a simultaneous lower bound of `+0.001393`. The empirical post-peak half-gain horizon is 9 seconds. This supersedes the historical interpretation that 3 seconds was a naturally preferred target. Because one second is the archive resolution boundary, it does not establish a universal optimum or a subsecond result. Full lineage and the execution-time 10-second target-name collision fix are recorded in `external_information_decay_v1_development_20260727.md`.

## Decision

Bitget, Bybit, and OKX BTCUSDT spot/perpetual history has now been added to two research model families:

- the then-current 10-second feature/model cadence; and
- a compact 1-second research head with 1/3/5-second targets.

The historical result supports a narrow diagnostic conclusion: external venue state contains short-lived information, but most directional increment is lost by the 10-second prediction clock. Under the conservative current-AWS visibility approximation, the 3-second direction head is the only direction target that keeps the same sign on validation, chronological test, and the three later days. This is research evidence, not a live promotion.

## What the old Binance reference model consumed

The Binance BTCUSDT futures source was not fed to LightGBM as individual trades:

1. raw `aggTrades` are read one row per aggregate trade;
2. `features/preprocess.py::aggregate_to_1s_bars()` groups those rows into one-second OHLCV, flow, and trade-count bars;
3. `features/feature_engineer.py::resample_to_10s()` resamples the one-second bars onto the model grid; and
4. live `SignalEngine.compute_signal()` only emits a new prediction after a completed 10-second bucket.

So the precise lineage is:

```text
per-trade input -> 1s state -> 10s feature row -> 10s LightGBM prediction
```

Some features are computed on the one-second path before sampling, but the model decision cadence in that experiment was 10 seconds.

## Data and causal alignment

The external panel contains six source streams:

```text
Bitget  BTCUSDT perpetual + spot
Bybit   BTCUSDT perpetual + spot
OKX     BTCUSDT perpetual + spot
```

All six sources cover the 111 retained good days. Three later complete days, 2026-07-04 through 2026-07-06, are used only as a small later panel. Each historical external row represents trades in `[t, t+1s)` and is visible at the right edge `t+1s`. The 10-second join maps a left-labelled row at `t` to the external state visible at `t+10s`; the fast head operates directly on the right-edge one-second grid.

The v2 cache has 114 files and 9,849,600 fast rows. Fast direction labels now exclude exactly-zero future returns instead of treating no move as down. A separate move/no-move label preserves the activity question:

| Horizon | Direction rows | Up share | Move share of all valid rows |
|---|---:|---:|---:|
| 1s | 4,842,698 | 50.06% | 49.17% |
| 3s | 7,308,064 | 50.10% | 74.20% |
| 5s | 8,206,816 | 50.15% | 83.33% |

## Split and environment identity

The experiment uses an explicit chronological split over good days:

| Panel | Days | Range |
|---|---:|---|
| train | 69 | 2026-01-01 through 2026-05-05 |
| embargo 1 | 1 | 2026-05-06 |
| validation | 20 | 2026-05-12 through 2026-06-12 |
| embargo 2 | 1 | 2026-06-13 |
| test | 20 | 2026-06-14 through 2026-07-03 |
| later | 3 | 2026-07-04 through 2026-07-06 |

These are selected good days, not continuous calendar intervals. The manifest SHA256 is `04f8abe8db3c947b47e94f0dbe31be291f6fc30759ee4c15fe12597979a90c2a`.

The executable fast result is tagged to the current reference environment:

```text
AWS Tokyo / t3.medium / 2 vCPU / 3.75 GiB / Amazon Linux 2023
profile: aws_tokyo_t3_medium_amzn2023_native_public_ws_20260711_p50_bucketed_1s
```

Historical archives do not contain same-collector receive timestamps. Because their minimum state interval is one second, any positive subsecond receive delay can only be represented conservatively as one whole visibility bucket. The zero-delay run is therefore an information upper bound, while the one-bucket run is a conservative executable approximation, not an exact latency model.

## Fast direction result

The table reports `M1 AUC - M0 AUC`. M0 uses local BTCUSDC and Binance BTCUSDT trade state; M1 adds all three external venues and both instrument layers.

### Current-AWS one-bucket visibility delay

| Target | Validation | Positive days | Test | Positive days | Later | Positive days |
|---|---:|---:|---:|---:|---:|---:|
| dir 1s | +0.00219 | 18/20 | +0.00118 | 17/20 | -0.00040 | 1/3 |
| dir 3s | **+0.00365** | **20/20** | **+0.00413** | **20/20** | **+0.00381** | **3/3** |
| dir 5s | +0.00236 | 15/20 | +0.00181 | 17/20 | -0.00045 | 1/3 |

The absolute 3-second AUC is `0.59445` on validation, `0.59611` on test, and `0.62382` on the three later days. The increment is stable but small.

### Zero-delay information upper bound

| Target | Validation gain | Test gain | Later gain |
|---|---:|---:|---:|
| dir 1s | +0.02396 | +0.02055 | +0.01483 |
| dir 3s | +0.01533 | +0.01617 | +0.01271 |
| dir 5s | +0.01085 | +0.01117 | +0.00602 |

The gap between this upper bound and the delayed run shows that directional external information decays within seconds. It does not prove that live receive latency costs exactly this amount, because the one-second archive cannot represent subsecond visibility.

## Activity is not direction alpha

The move/no-move target is easier and receives a consistent external increment:

| Target | Validation gain | Test gain | Later gain |
|---|---:|---:|---:|
| move 1s | +0.00269 | +0.00359 | +0.00192 |
| move 3s | +0.00462 | +0.00595 | +0.00204 |
| move 5s | +0.00525 | +0.00745 | +0.00392 |

Its absolute AUC reaches roughly 0.79-0.84 on validation. This is a useful activity/volatility state, not evidence that the model knows the profitable side of a maker quote.

## Historical 10-second model family

For binary targets, gain is AUC(M1)-AUC(M0). For regression targets, gain is MAE(M0)-MAE(M1), so positive is better.

| Target | Validation gain | Test gain | Later gain | Assessment |
|---|---:|---:|---:|---|
| dir 10s | +0.000050 | +0.000055 | +0.000151 | effectively flat |
| ret 10s | -0.000000130 | -0.000000057 | -0.000000238 | worse |
| ret 30s | +0.000000344 | +0.000000470 | +0.000000358 | consistent, economically tiny |
| ret 60s | +0.000000153 | +0.000000067 | -0.000000041 | unstable |
| vol 10s | +0.00050 | -0.00836 | -0.01312 | fails later panels |
| tox bid 10s | +0.000121 | +0.000344 | +0.000942 | pooled gain, weak daily signs |
| tox ask 10s | +0.000149 | +0.000187 | -0.002502 | fails later panel |

Only 30-second return MAE is same-sign across all panels (18/20, 19/20, 3/3 positive days), and the absolute improvement was too small to justify replacing the then-current bundle. Broadly adding external fields to that 10-second model identity was therefore rejected.

## Feature attribution

In the delayed 3-second direction model, external features account for about 32.2% of LightGBM gain importance; the zero-delay upper bound is about 49.6%. Leading external families are 1/3-second perpetual return and flow, cross-venue dispersion, and spot/perpetual divergence. Gain importance is only a model-use diagnostic; it is not causal uplift.

Historical `source_age_ms` measures time since the last archived trade. It is an activity feature and must not be described as same-machine receive latency.

## Artifacts and reproduction

Code:

- `features/external_venue_features.py`
- `models/external_venue_model.py`
- `tests/test_external_venue_model_features.py`

Artifacts live outside the repository:

```text
${NARROWGATE_DATA_ROOT}/model_features/external_venue_trade_state.v2/
${NARROWGATE_DATA_ROOT}/model_runs/external_venue_10s_full_20260713/
${NARROWGATE_DATA_ROOT}/model_runs/external_venue_fast_aws_tokyo_v2_20260713/
${NARROWGATE_DATA_ROOT}/model_runs/external_venue_fast_exchange_zero_upper_bound_20260713/
${NARROWGATE_DATA_ROOT}/model_runs/external_information_decay_v1_20260727/
```

Historical reproduction command:

```bash
.venv/bin/python models/external_venue_model.py \
  --output-dir "$NARROWGATE_DATA_ROOT/model_runs/external_venue_fast_aws_tokyo_v2_20260713" \
  --cadences fast1s \
  --targets-fast dir_1s dir_3s dir_5s move_1s move_3s move_5s \
  --profiles m0_local_binance m1_external_all \
  --threads 2 \
  --fast-train-row-stride 5 \
  --fast-validation-row-stride 10 \
  --external-delay-s 1 \
  --latency-profile aws_tokyo_t3_medium_amzn2023_native_public_ws_20260711_p50_bucketed_1s
```

A current decay experiment must declare the entire support rather than naming three preferred heads. Using the Development-p95 support diagnostic above, the first candidate command is:

```bash
.venv/bin/python models/external_venue_model.py \
  --output-dir "$NARROWGATE_DATA_ROOT/model_runs/external_information_decay_v1" \
  --cadences fast1s \
  --fast-horizon-min-s 1 \
  --fast-horizon-max-s 18 \
  --fast-horizon-step-s 1 \
  --fast-target-kinds dir \
  --score-panels validation \
  --external-delay-s 1 \
  --latency-profile aws_tokyo_t3_medium_amzn2023_native_public_ws_20260711_p50_bucketed_1s
```

This first pass writes `direction_horizon_decay_curve.csv` and `direction_horizon_selection.json`. Test and late panels are not opened by default. A later `--score-only --score-panels test` call must provide that frozen JSON through `--horizon-selection-artifact`; the trainer then refuses to score any horizon except the single Development-selected one. Prediction qualification still cannot authorize a cancel, widen, recenter or live action without a separate known-propensity action experiment.

The fast run is a screening model trained on deterministic row subsamples; panel scoring uses complete days.

## Promotion decision

No live config or live model path changes in this experiment.

The historical follow-up recommendation was to retain that 10-second bundle and add a separate 3-second shadow head. That bundle is no longer current. A future successor should start from the current receive-time feature ABI and target side-specific maker fill quality or a registered action estimand; prediction alone cannot promote an action. It requires latency-stress parity, current queue/campaign evidence and a frozen chronological family contract.
