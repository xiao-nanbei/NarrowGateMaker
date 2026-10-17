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

Cancel is an action/censor, native jump remains non-absorbing, and inventory lifecycle repair starts only after delayed entry into a valid reducing path.

## Numerical contract

The larger denominator exposed an optimizer defect that the old 17-day panel did not reveal. All model features were finite, but rare valid book states reached roughly 649 training standard deviations. The old objective clipped the linear predictor while retaining the unclipped gradient, allowing L-BFGS to send coefficients toward infinity.

Before any valid model outcome was accepted, the family spec was re-frozen with:

- train-fold standardization followed by a fixed +/-12 feature clip;
- intercept bounds `[-25, 20]`;
- coefficient bounds `[-8, 8]`;
- zero gradient outside the clipped eta range;
- finite-input and finite-fit fail-fast checks.

The final fit emitted no numeric warnings. All OOF probabilities are finite, and fitted absolute coefficients remain below 1.86. The formal runtime is Python 3.12.13, NumPy 2.4.6, pandas 3.0.3, and SciPy 1.18.0; the family spec fails fast under a different runtime identity.

## Current research question: dynamic hazard transport

The historical prediction, admission and Validation outcome tables are no longer current conclusions. `F07-DYNAMIC-HAZARD` in the [F07 question register](../research/families/f07_active_order_continuation/research_questions.json) asks whether favorable/adverse fill and repair probabilities transport under the current source and execution clocks. The source-audit and numerical-defect observations above retain their original engineering scope; they do not establish current model or action value.

The reusable method is nested chronological calibration: expanding inner folds stay inside each outer training fold; inner-OOF probabilities fit a calibrator; the raw hazard is refit on outer training data; that fold-local calibrator is applied once to the outer test. A new study must bind its training support, clocks, censoring, baseline and previous-use before comparing prediction and separately registered actions. It must not inherit the old close/admit verdict as a current result.

Current bounded U0/U1 account evidence is described in the [F07 family report](../research/families/f07_active_order_continuation/README.md). It does not validate this historical dynamic-hazard model. Original experiment identities and already-viewed panels remain historical provenance, not an untouched holdout.
