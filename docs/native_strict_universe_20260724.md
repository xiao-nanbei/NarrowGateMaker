# Native strict replay universe (2026-07-24)

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

## Research-day contract

A retained good day is eligible only when its previous natural UTC day can initialize the order book at midnight. A previous-day file is context only: it is never promoted to a research observation merely because it is used for warmup.

The replay does not impute a missing previous day, initialize from an arbitrary delta, or lower the evidence gate after seeing an outcome.

## Source audit

| Stage | Days | Rule |
|---|---:|---|
| Retained good days through 2026-07-20 | 128 | Starting universe |
| Source-complete target days | 109 | Target day and previous natural day both have all 24 native raw hours |
| Native sequence-complete target days | 66 | Snapshot initialized at target start, accepted target updates, no target sequence/time failure |
| Strict top-20/100ms target days | 62 | Coverage >= 99%, p99 cadence <= 500ms, valid spread ratio >= 99.9% |

The 19 source-incomplete target days are excluded rather than repaired. Of the 109 source-complete candidates, 43 fail native snapshot/sequence identity. Four further dates fail target-day time coverage even after deterministic reconstruction:

- `2026-06-01`
- `2026-06-04`
- `2026-06-30`
- `2026-07-13`

These four dates may still be used as context for the following day when their state immediately before midnight is valid. That does not make them target research days.

## Midnight context

The 62 target days require nine additional previous-natural-day files that are not themselves target observations:

- `2026-04-16`
- `2026-04-21`
- `2026-04-30`
- `2026-05-12`
- `2026-05-28`
- `2026-06-01`
- `2026-06-04`
- `2026-06-30`
- `2026-07-13`

For all nine, the last normalized BBO/L2 state is within 0.2 seconds of UTC midnight. The frozen view therefore contains 62 target days plus nine warmup-only context days, while the statistical denominator remains 62.

## Frozen identities

- Raw source manifest SHA256: `a6ad756cc05e0dfb038a4bfbc4e4293f2021944a9fd1e2b71f3dffe9791251d7`
- Normalized target audit SHA256: `210a2971a96e9bf0cd8b18b2114ad1035013e82dc6f92e59975028fba8c6eca0`
- Warmup boundary audit SHA256: `e24a5d42a52ba1a09f3df4623ea64e32c359094f90a1f1d7e7a6f79cbc2b5938`
- Target plus warmup view SHA256: `085c299de65ffd3a0829022416d8df3566ff72fadd2210bbfe7661b2c9d5299f`

The normalized view is an absolute symlink index over immutable, per-file hashed Parquet artifacts. It does not duplicate the underlying L2 data.

## Frozen evidence split

The 62 target days were frozen before model outcomes:

| Panel | Days | Interval |
|---|---:|---|
| Development | 40 | 2026-04-17 through 2026-06-26 |
| Embargo 1 | 1 | 2026-06-27 |
| Validation | 10 | 2026-06-28 through 2026-07-08 |
| Embargo 2 | 1 | 2026-07-09 |
| Family-specific sealed holdout | 10 | 2026-07-10 through 2026-07-20 |

Development uses four expanding chronological folds with 20 minimum train days, one trailing embargo day, and five test days per fold. The resulting OOF panel contains 19 days from 2026-06-08 through 2026-06-26.

The split identity is `8d9688f0d8a6b8883421a0853ffc9d533f6fd59222cda9f87f2fc83d0e410967`. Validation and sealed holdout were not read while building or fitting the Development panel.

## Development lifecycle identity

The authoritative Python replay completed all 40 Development days with:

- explicit empirical P3 artifact, `delta_star=13.9991` and `kappa_eff=0.0674`;
- q0.70 queue-calibration artifact;
- individual trades for exchange-time matching;
- native snapshot/delta L2 with 24-hour previous-natural-day warmup;
- AWS Tokyo 2-vCPU/4-GiB latency and book-visibility identities;
- zero delta bootstrap, sequence gaps, invalid sequence messages, or time reversals.

The frozen lifecycle panel contains 23,654,774 rows. Queue state was missing for 158 of 670,053 lookups, or about 0.024%, below the pre-registered 0.1% integrity gate. The lifecycle panel identity is `4cbaca2a671422753d522a81f04d41625fae2f87c2947241fb8cfb19aad9f817`.

The derived dynamic fill risk set contains 7,054,800 intervals:

- 26,873 adverse fills;
- 8,111 favorable fills;
- 605,628 cancel-request censor intervals;
- 405,638 native adverse-jump transitions.

Cancel is an action/censor, native jump remains non-absorbing, and campaign repair starts only after delayed entry into a valid reducing path.

## Numerical contract

The larger denominator exposed an optimizer defect that the old 17-day panel did not reveal. All model features were finite, but rare valid book states reached roughly 649 training standard deviations. The old objective clipped the linear predictor while retaining the unclipped gradient, allowing L-BFGS to send coefficients toward infinity.

Before any valid model outcome was accepted, the family spec was re-frozen with:

- train-fold standardization followed by a fixed +/-12 feature clip;
- intercept bounds `[-25, 20]`;
- coefficient bounds `[-8, 8]`;
- zero gradient outside the clipped eta range;
- finite-input and finite-fit fail-fast checks.

The final fit emitted no numeric warnings. All OOF probabilities are finite, and fitted absolute coefficients remain below 1.86. The formal runtime is Python 3.12.13, NumPy 2.4.6, pandas 3.0.3, and SciPy 1.18.0; the family spec fails fast under a different runtime identity.

## Development prediction gate

`dynamic_fill_hazard_m0_native_strict_v1` produced the following OOF result:

| Side / cause | Events | AP lift | AUC | Mean top-20% lift | Daily sign | O/E | Brier skill | Gate |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| BUY adverse | 5,874 | 12.01x | 0.892 | 4.15x | 100% | 0.858 | +0.00675 | pass |
| BUY favorable | 1,807 | 8.47x | 0.829 | 3.44x | 100% | 0.909 | -0.00070 | fail |
| SELL adverse | 5,790 | 12.85x | 0.887 | 4.09x | 100% | 0.850 | +0.00791 | pass |
| SELL favorable | 1,869 | 8.82x | 0.831 | 3.50x | 100% | 0.861 | +0.00008 | pass |

BUY closed on Development because its favorable-fill head failed the frozen positive-Brier-skill gate. The original point-estimate gate technically made SELL eligible for Validation, but the favorable-fill margin was only `+0.00008`. A post-fit specification audit found that its day-clustered Brier improvement interval crossed zero. Validation was therefore not read. The v1 result remains a diagnostic of ranking versus calibration, not a promotion result.

## Nested chronological calibration

A new family, `dynamic_fill_hazard_m0_native_strict_nested_cal_v2`, was frozen before refitting. It preserves the same Development, Validation and sealed-holdout panels. Validation and holdout remain unread.

For every outer fold and every BUY/SELL cause:

1. expanding inner folds are generated strictly inside the outer-train dates;
2. raw inner-OOF probabilities fit a two-parameter affine cloglog calibrator;
3. the raw hazard is refit on the complete outer train;
4. that fold-local calibrator is applied once to the outer test.

The repair nuisance uses the same outer/inner structure. The gate now requires the day-clustered 95% lower bound of absolute Brier improvement over the exposure-only baseline to exceed zero. Twenty-four outer-model audits record zero overlap between calibration dates and outer-test dates. Every inner fold starts from the earliest date of its containing outer train.

| Side / cause | AP lift | AUC | O/E | Brier skill | Brier improvement 95% interval | Gate |
|---|---:|---:|---:|---:|---:|---|
| BUY adverse | 11.95x | 0.891 | 0.847 | +0.02111 | `[+6.15e-5,+8.39e-5]` | pass |
| BUY favorable | 8.39x | 0.828 | 0.791 | +0.00102 | `[-1.53e-6,+3.84e-6]` | inconclusive positive |
| BUY campaign repair | 12.38x | 0.881 | 0.759 | +0.02158 | `[+5.14e-5,+8.91e-5]` | pass |
| SELL adverse | 12.78x | 0.887 | 0.880 | +0.02344 | `[+6.48e-5,+8.89e-5]` | pass |
| SELL favorable | 8.80x | 0.831 | 0.782 | +0.00199 | `[-5.59e-7,+4.76e-6]` | inconclusive positive |
| SELL campaign repair | 11.35x | 0.881 | 0.727 | +0.01627 | `[+3.67e-5,+8.06e-5]` | pass |

Nested calibration materially improves adverse-fill and repair probability quality. It turns both favorable-fill point estimates slightly positive, but neither favorable head has a positive day-clustered lower bound. Both sides therefore close on Development:

- `prediction_gate_passed_sides=[]`;
- `validation_access_allowed=false`;
- no randomized keep/cancel panel is registered;
- no strategy or live behavior changes.

This is prediction evidence only. It does not authorize keep/cancel or infer action uplift.

## BUY Validation admission decision

The table above preserves the original strict-gate result: BUY favorable fill did not pass the preregistered requirement that the day-clustered 95% lower bound exceed zero. That result must not be rewritten as a strict pass.

It is also too coarse to call the BUY favorable head ineffective. Under the frozen 5,000-draw day-cluster bootstrap:

- absolute Brier improvement is `+1.07944e-6`;
- relative Brier skill is `+0.00102`;
- `P(Brier improvement > 0) = 76.98%`;
- 11 of 19 OOF days have positive absolute Brier improvement;
- AP lift is `8.39x`, AUC is `0.828`, and all non-interval prediction gates pass.

On 2026-07-24 the researcher explicitly admitted **BUY only** to Validation as an `inconclusive_positive` candidate. This is a Validation-screening decision, not a retroactive strict-gate pass. The original v2 summary and hashes remain unchanged.

The Validation contract is frozen before reading outcomes:

1. use the same model features, nested chronological calibration, P3, queue, latency, visibility, data, and causal event identities;
2. do not tune the model, threshold, bootstrap seed, or probability cutoff on Validation;
3. require all original support, ranking, O/E, adverse-fill, and repair gates;
4. for BUY favorable fill, require positive point improvement and `P(improvement > 0) >= 75%`;
5. keep SELL closed and keep the sealed holdout locked;
6. even a Validation pass only permits registration of the separate randomized keep/cancel action experiment. It does not authorize a live action.

The immutable admission record is `validation_admission_dynamic_fill_hazard_m0_native_strict_nested_cal_v2_buy_v1.json` beside the frozen experiment artifacts.

## BUY one-shot Validation result

The admitted BUY head was evaluated once on the ten frozen Validation days from 2026-06-28 through 2026-07-08. The Development model and calibrator were not refit, and the sealed holdout was not opened.

| BUY target | Events | AP lift | AUC | O/E | Brier skill | Improvement interval | Result |
|---|---:|---:|---:|---:|---:|---:|---|
| adverse fill | 1,769 | 12.70x | 0.898 | 0.903 | +0.02247 | `[+5.83e-5,+8.09e-5]` | pass |
| favorable fill | 465 | 10.30x | 0.837 | 0.742 | +0.00294 | `[+0.83e-6,+3.81e-6]` | pass |
| campaign repair | 660 | 7.10x | 0.862 | 0.193 | -0.38862 | `[-5.42e-4,-8.20e-5]` | fail |

The favorable-fill admission was therefore warranted: its absolute Brier improvement is `+2.36626e-6`, `P(Brier improvement > 0) = 99.84%`, and seven of ten Validation days improve on the exposure-only baseline. The adverse-fill head also passes every frozen gate.

The complete prediction family nevertheless closes on Validation because the campaign-repair nuisance fails the preregistered O/E, Brier-skill, and day-cluster lower-bound checks. It predicts about 3,419 repair events where 660 are observed. This failure cannot be waived after reading Validation.

The correct interpretation is consequently split:

- the BUY order-level favorable/adverse ranking and probability evidence replicated out of Development;
- the campaign-repair probability layer did not transport to Validation;
- `queue_value_keep_cancel_dynamic_fill_nested_cal_v2` is not registered;
- no action family, live change, or sealed-holdout access is authorized.

Any follow-up must be a separately frozen order-level action family that does not silently reuse the failed repair nuisance. Its action value still requires randomized replay with queue reset, cancel-ACK races, campaign attribution, and an untouched confirmation panel.
