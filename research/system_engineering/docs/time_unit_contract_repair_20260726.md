# Time and Unit Contract Repair (2026-07-26)

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../docs/public_private_documentation_contract.md).

## Decision

The confirmed runtime, time-axis, feature, label, and reporting defects were repaired. The authoritative replacement identity is `causal-v7`; `causal-v5` and `causal-v6` exact replay/model numbers are superseded.

The repaired 13-head bundle is complete, but its strict ML A/B does not pass the strategy promotion gate. The rebuilt BUY fill-selection family also fails the joint fill-quality/inventory_lifecycle/tail gate. Both remain research artifacts. No EC2 configuration, live model, queue baseline, or strategy action was changed.

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

Old model scores are not current results. New-source model comparisons are reported in [F03](../../families/f03_causal_13_head/README.md); cadence and source transfer remain scoped [questions](../research_questions.json).

## Formal replay identity

The corrected formal C++ replay completed 42 Development/Validation good days with zero failed days and generated 776,048 order rows. It used fresh-start, individual trades, normalized 100ms L2, queue-v3 q0.70, the frozen AWS Tokyo latency profile, and a disabled BUY action gate.

- File list: `${NARROWGATE_RETIRED_DATA_ROOT}/backtest_results_btcusdc/causal_v7_time_calendar_semantics_all42_20260726.filelist.csv`
- Replay-contract SHA256: `3cf83aa1876769f02c34fa8332423a48cf88cc7233b5cc49c4e228da62234067`
- File-list SHA256: `86bf0d02616cbae4554e7a964bfbe5334b21b2e76fbf30365ffc8986402d4b2e`

The old v6/v7 monetary comparison is no longer a current result. The identity and metadata repair above is preserved as engineering evidence.

## Economic questions after engineering repair

The original BUY fill-selection, ML A/B and queue-sensitivity tables do not supply current model rankings or promotion decisions. They are now [registered applicability questions](../research_questions.json); [F05](../../families/f05_fill_quality_quote_ev/research_questions.json) owns decision-visible fill value. Preserve original provenance and previous-use. New Tardis results replace matching current-facing model/account claims, not native queue or live-equivalence claims. Regression and fixed-input parity remain valid at their stated engineering scope.

## Superseded evidence

The following exact values must not be used for promotion:

- causal-v5 and causal-v6 model/replay A/B numbers;
- old M0/M1 labels based on a future first trade mislabeled as midpoint;
- SELL score/bucket reports derived from the double sign flip;
- legacy `backtest.py` and `backtest_ml.py` outputs produced with mixed ns/ms;
- any post-fill response result that mixed variance, standard deviation, and bps;
- historical live fills as evidence for the corrected model contract.

Historical live records still describe what the old process actually did. They cannot retroactively validate corrected features, labels, or runtime units.
