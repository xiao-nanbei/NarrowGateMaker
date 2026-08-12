# Placement Fill CIF V4 Validation Evidence

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Date: 2026-07-27

Status: Validation failed the frozen family gate. The sealed holdout remains unread. This result does not authorize an action arm or a live policy.

## Why V4 Exists

The v1 contract required `abs(calibration_intercept) <= 0.01` in every cell. Zero is the theoretical ideal, but `0.01` was a judgmental log-odds tolerance with no mapping to USDC action-value error or the observed day-level base-rate drift. V4 preserves the v1 evidence and replaces that point-estimate rule with a prediction-transfer contract based on:

- event and day support;
- day-clustered Brier improvement over the exposure-only baseline;
- calibration slope in `[0.8, 1.2]`;
- daily ranking direction;
- zero-centered probability/O-E intervals as diagnostics;
- a Development-frozen empirical day-level calibration-drift envelope.

This is a prediction gate only. Absolute probability use in quote EV still requires an economic error budget, and any quote action still requires known propensity and action-uplift evidence.

## Frozen Identity

The family spec is `docs/placement_fill_cif_v4_spec_20260727.json`, SHA256 `e2a9ef38aef8ff53493e92ca58852db7b76cd5aa57ce9a7b89d24b706ba7afaf`. The Development artifact is:

```text
${NARROWGATE_RETIRED_DATA_ROOT}/reports/
  direct_fill_cif_v4_development_20260727/direct_fill_cif.joblib
```

Its SHA256 is `2df28e11a02985500e9a7bb18ee32fc6ca56dca6a349ce72d4fdbb07a32ef52b`. No refit or recalibration was performed on Validation.

The Validation panel contains 167,300 paired placement cohorts and 1,505,652 model rows over ten frozen good days from 2026-06-28 through 2026-07-08. Its run identity is `98fb88beed12b89cf0184d734b762762e8f4af70ec49a7cc2d6f3c19767c42fc`.

## Validation Result

The three placement actions (`closer_1tick`, `current`, `farther_1tick`) are pooled within each cell. The 18 gates are:

\[
BUY/SELL \times opener/add/reducing \times 1s/5s/10s.
\]

Thirteen of eighteen cells passed. All cells passed support, Brier improvement, and daily rank direction. The five failures were calibration-transfer failures:

| Side | Role | Horizon | Events | Brier skill | Slope | Additional failure |
|---|---|---:|---:|---:|---:|---|
| BUY | opener | 10s | 4,103 | 2.70% | 0.796 | slope below 0.8 |
| BUY | add | 1s | 116 | 0.17% | 1.340 | slope above 1.2 |
| BUY | add | 10s | 1,694 | 1.18% | 0.797 | probability/O-E interval excludes zero/one |
| SELL | opener | 1s | 307 | 0.29% | 1.227 | probability/O-E interval excludes zero/one |
| SELL | add | 1s | 102 | 0.19% | 1.206 | slope above 1.2 |

The result is not a resurrection of the v1 `0.01` gate: every failure above would remain even if point calibration intercept were ignored. The model still ranks fills better than the exposure-only baseline, but its probability response is not stable enough across all side/role/horizon cells to claim a single transferable absolute fill surface.

The frozen result is stored at:

```text
${NARROWGATE_RETIRED_DATA_ROOT}/reports/
  direct_fill_cif_v4_validation_20260727/
```

The report SHA256 is `9026c194d16ecd8ff537dc6e3ca6c4087182804371b30db317f7e483eb7d59ca`.

## Action Resolution

Predicted placement probability is pathwise monotone, with zero violations. However, the shallow model gives a nonzero `closer_1tick - farther_1tick` probability difference to only 21.8%-36.7% of Validation cohorts; the median difference is zero in every cell. It is therefore more useful as a state-conditioned fill-risk ranker than as a one-tick quote optimizer.

## Strategy Boundary

If a future revision passes prediction transfer, it should first run as a shadow scorer after final quote geometry is known. For each side and inventory role it would emit `P(fill by 1s/5s/10s)` for the three placement candidates. Volatility enters through causal state features and volatility-normalized distance, while P3 remains the analytical spread floor.

No action follows from fill probability alone. The policy layer must compare:

\[
V(a\mid x)=P_{fill}(a\mid x)
E[net\ fill\ value\mid fill,a,x]
-\Delta C_{campaign}(a,x)
-C_{queue/reset/churn}(a,x).
\]

The BUY fill-selection score can provide ranking evidence for conditional fill quality, but it is not a calibrated USDC reward model. KEEP and REPLACE also remain a separate active-order estimand because KEEP preserves queue position and REPLACE resets it.

## Decision

`validation_prediction_gate_passed=false`, `action_or_live_authorization=false`, and `absolute_probability_ev_authorized=false`. The sealed holdout and late evidence panels were not opened.

The next revision may improve continuous distance resolution and use past-only probability recalibration, but the present Validation cannot be reused as a new confirmatory panel after tuning. The 13 passing cells may be inspected as diagnostics; they do not receive retrospective production permission.
