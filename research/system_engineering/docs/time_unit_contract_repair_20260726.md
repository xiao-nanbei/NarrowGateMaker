# Time and Unit Contract Repair (2026-07-26)

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../docs/public_private_documentation_contract.md).

## Decision

The confirmed runtime, time-axis, feature, label, and reporting defects were repaired. The authoritative replacement identity is `causal-v7`; `causal-v5` and `causal-v6` exact replay/model numbers are superseded.

The repaired 13-head bundle is complete, but its strict ML A/B does not pass the strategy promotion gate. The rebuilt BUY fill-selection family also fails the joint fill-quality/campaign/tail gate. Both remain research artifacts. No EC2 configuration, live model, queue baseline, or strategy action was changed.

Deployment closure later on 2026-07-26 separated activation from identity: 13-head inference remains disabled, while the configured dormant bundle now points to the restart-safe `causal-v7` directory. Preflight and runtime validate all 13 metadata contracts even when inference is disabled. The active empirical P3 did not change: the v5 and v7 `fill_prob_params.json` files are byte-identical with SHA256 `cedec34851454b643be746746a1dd4bcc7e13807985c14e78504643ad6e71714`.

## Runtime contracts

| Contract | Corrected behavior |
|---|---|
| Minimum notional | C++ no longer enlarges an invalid base order to the minimum notional. Python and C++ leave it for the final exchange filter to reject. |
| ML failure | Enabled ML requires all 13 heads, metadata, matching model/schema widths, one causal manifest identity, and absolute-price-variance semantics. Load or inference failure is fail-closed. |
| ML-OFF variance | A neutral prediction leaves `vol_10s=0`; dimensionless realized-return volatility is never mixed with absolute price variance. |
| Daily PnL | Risk PnL rolls automatically at UTC midnight against marked total PnL, without recounting carried inventory or rewinding on delayed events. |
| Commission | The commission asset is retained. Quote/settlement commission is direct, base-asset commission is converted at fill price, and an unknown nonzero asset blocks further quoting. |
| Post-fill volatility | The interface is `volatility_bps`; live and replay both convert absolute price variance to bps standard deviation. |
| Tick/lot | Python and C++ replay read positive `tick_size` and `lot_size` from replay parameters. |
| Quote horizon | The public/current baseline explicitly uses one second; deploy preflight rejects a missing or non-positive horizon. |
| Legacy bar clock | Bar, prediction, and depth indices are converted to `datetime64[ms]`. Old bar-backtest results produced under mixed ns/ms clocks are invalid. |

## Feature and label contracts

- `volatility_5s`, imbalance, intensity, VPIN, and price change now use the trailing five causal one-second bars, rather than two ten-second bars.
- Forward labels use `[t,t+h)` and exclude the bar beginning exactly at `t+h`.
- M0/M1 fixed-horizon fill value uses the latest exchange-time BBO/L2 midpoint visible at the target. Target time, actual observation time, source, age, and censoring are stored separately.
- Maker-signed markout is positive when favorable on both BUY and SELL. The audit scorer no longer flips SELL a second time.
- Model metadata inherits semantic versions from the frozen feature manifest; the trainer no longer writes a stale hard-coded version.
- P3 `delta_star=13.99908598` is USDC/BTC, approximately 140 ticks at a 0.1-USDC/BTC tick. `kappa_eff=-d log(P_touch)/d delta` has inverse-price distance units; it is not an order-arrival intensity.

## Calendar defect

Parquet can preserve an index as `datetime64[ms]`, `datetime64[us]`, or `datetime64[ns]`. The former converter first cast datetime values to numbers and treated magnitudes above `1e15` as nanoseconds. Microsecond timestamps were therefore interpreted as dates in 1970.

Nineteen training days had completely wrong calendar/session features:

`2026-01-01`, `01-03`, `01-07`, `01-15`, `01-29`, `02-02`, `02-09`, `02-12`, `02-25`, `02-28`, `03-17`, `04-10`, `04-13`, `04-15`, `04-30`, `05-12`, `05-15`, `05-17`, and `05-28`.

Datetime physical units are now preserved before epoch conversion. Numeric epochs distinguish seconds, milliseconds, microseconds, and nanoseconds. The calendar tables also fail fast outside their declared local-year support. Validation and test dates were not affected by this particular defect, but the training distribution changed, so `causal-v6` cannot remain authoritative.

## Causal-v7 bundle

- Feature root: `${NARROWGATE_RETIRED_DATA_ROOT}/features_btcusdc_causal_v7_time_calendar_semantics_20260726`
- Model root: `models/saved_btcusdc_causal_v7_time_calendar_semantics_20260726`
- Feature manifest SHA256: `d9a81a56bdb8d115d9687124f4a8fe6fc0ff53bf86e760adcefb39be402de56c`
- P3 SHA256: `cedec34851454b643be746746a1dd4bcc7e13807985c14e78504643ad6e71714`
- Rows: 742,998 train / 172,798 validation / 172,799 historical test
- Heads: 13, each with 195 strict metadata features
- Feature semantics: version 5 on the manifest, training summary, and all 13 head metadata files

The metadata repair retrained all heads. All 13 LightGBM `.txt` hashes are byte-identical to the models used by the formal v7 A/B; only the 13 metadata files and training summary changed. The existing A/B predictions therefore remain numerically bound to the current trees, while the new manifest freezes the corrected metadata identity.

Selected historical test diagnostics:

| Head | Metric |
|---|---:|
| `dir_10s` | AUC 0.5363 |
| `ret_10s` | IC 0.0396 |
| `vol_10s` | IC 0.5779 |
| `tox_bid_5s` | AUC 0.5726 |
| `tox_ask_5s` | AUC 0.5565 |
| `tox_bid_10s` | AUC 0.5514 |
| `tox_ask_10s` | AUC 0.5413 |

These are prediction diagnostics, not action or PnL evidence.

## Formal replay identity

The corrected formal C++ replay completed 42 Development/Validation good days with zero failed days and generated 776,048 order rows. It used fresh-start, individual trades, normalized 100ms L2, queue-v3 q0.70, the frozen AWS Tokyo latency profile, and a disabled BUY action gate.

- File list: `${NARROWGATE_RETIRED_DATA_ROOT}/backtest_results_btcusdc/causal_v7_time_calendar_semantics_all42_20260726.filelist.csv`
- Replay-contract SHA256: `3cf83aa1876769f02c34fa8332423a48cf88cc7233b5cc49c4e228da62234067`
- File-list SHA256: `86bf0d02616cbae4554e7a964bfbe5334b21b2e76fbf30365ffc8986402d4b2e`

Relative to v6 on the same 42 days, v7 changed raw PnL by -3.7363 USDC, fills by +88, and absolute inventory time by +205.02 BTC-hours. The change does not imply deterioration from a calendar feature alone; it proves that the old nonlinear replay numbers do not survive the corrected training identity.

## BUY fill-selection rebuild

All four BUY-only targets were rebuilt with expanding walk-forward fitting. Validation9 was read after threshold freezing; the scorer's sealed holdout remained unread.

Validation threshold-hit minus threshold-miss:

| Target | Hit orders/fills | 20s markout bps | 30s markout bps | Terminal PnL/campaign | Bad rate | Tail rate |
|---|---:|---:|---:|---:|---:|---:|
| non_toxic | 4,437 / 184 | +0.4860 | +0.7557 | -0.1053 | +0.2001 | +0.0368 |
| beats_opportunity | 5,977 / 301 | +0.4195 | +0.5367 | -0.1133 | +0.2147 | +0.0350 |
| campaign_repair | 7,028 / 118 | -0.0897 | -0.2142 | +0.0298 | -0.1511 | +0.0169 |
| opportunity_and_campaign | 13,376 / 303 | +0.2226 | +0.0885 | -0.0091 | +0.0314 | +0.0034 |

No target jointly improves fill quality, terminal campaign value, bad-campaign rate, and tail. The family remains `shadow_only`; it does not authorize keep, cancel, widen, re-center, or any live action.

## Strict ML A/B

The A/B changed only ML OFF versus the v7 13-head bundle. BUY fill-selection actions were disabled, and Python/C++ parity differences were zero on the formal parity day. Intervals are day-cluster bootstrap 95% intervals.

| Panel | Raw PnL delta | Terminal delta | Inventory-adjusted delta | Fills | Absolute inventory time |
|---|---:|---:|---:|---:|---:|
| Validation20 | +1.038 total; daily CI [-0.3146,+0.3991] | -2.6465; CI [-0.5135,+0.2362] | +3.8847; CI [+0.1230,+0.2664] | -772 | +276.65 |
| Historical Test17 | -3.9585; daily CI [-0.5796,+0.1229] | -4.2107; CI [-0.5901,+0.0939] | +0.2269; CI crosses zero | -120 | +126.49 |

The historical Test17 panel has already been used and is revalidation, not a sealed holdout. Raw and terminal evidence does not pass; fills fall while inventory time rises on Validation20. The v7 bundle is not promoted to live.

## Queue sensitivity

ML and BUY actions were disabled while q0.55/q0.70/q0.85/q1.00 changed the queue-ahead calibration. Results are not monotonic in PnL or campaign tail:

| Panel/queue | PnL | Terminal | Fills | Campaigns | Tail | Inventory time |
|---|---:|---:|---:|---:|---:|---:|
| Validation20 q0.55 | -70.61 | -35.85 | 10,573 | 3,187 | 24 | 2,592.0 |
| Validation20 q0.70 | -65.95 | -30.02 | 9,886 | 2,930 | 27 | 2,551.0 |
| Validation20 q0.85 | -63.88 | -27.05 | 9,354 | 2,732 | 31 | 2,666.6 |
| Validation20 q1.00 | -64.14 | -26.74 | 8,910 | 2,487 | 30 | 2,779.3 |
| Historical Test17 q0.55 | -42.47 | -19.16 | 6,493 | 1,809 | 12 | 2,018.7 |
| Historical Test17 q0.70 | -41.75 | -17.52 | 6,070 | 1,694 | 17 | 1,955.2 |
| Historical Test17 q0.85 | -42.25 | -17.53 | 5,717 | 1,561 | 20 | 2,009.3 |
| Historical Test17 q1.00 | -42.01 | -17.17 | 5,360 | 1,471 | 20 | 2,093.5 |

Queue uncertainty materially changes fills and tail attribution. q0.70 remains the externally calibrated baseline; replay PnL is not used to choose a queue quantile.

## Superseded evidence

The following exact values must not be used for promotion:

- causal-v5 and causal-v6 model/replay A/B numbers;
- old M0/M1 labels based on a future first trade mislabeled as midpoint;
- SELL score/bucket reports derived from the double sign flip;
- legacy `backtest.py` and `backtest_ml.py` outputs produced with mixed ns/ms;
- any post-fill response result that mixed variance, standard deviation, and bps;
- historical live fills as evidence for the corrected model contract.

Historical live records still describe what the old process actually did. They cannot retroactively validate corrected features, labels, or runtime units.
