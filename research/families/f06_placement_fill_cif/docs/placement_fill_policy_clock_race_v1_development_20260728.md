# Placement Fill Policy-Clock Race v1: Development Result

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Status: closed on Development. Validation and sealed holdout remain unread. No action experiment or live deployment is authorized.

## Question

This family retained the placement estimand:

\[
P(T_{fill}\le t,\ T_{fill}<T_{cancelACK}
\mid current\ placement,x_0,baseline\ policy\ path).
\]

It did not inherit the v5 trailing role offsets or the v6 role/cause Platt maps. Cancel request was replayed as the frozen baseline policy stopping time; request-to-ACK latency was fitted separately on past-only data; partial and full fills remained possible until ACK.

This v1 artifact is policy-path-conditioned offline evidence. It is not an online cancel-request forecast and cannot score KEEP or REPLACE.

## Frozen Identity

- Fit spec: `placement_fill_policy_clock_race_v1_fit_spec_20260728.json`, SHA256 `bd73e73c965c8453f69d21596cf2257912bc25522a0592f5117db18e4eb1bee5`.
- Lifecycle panel manifest: SHA256 `0829b307d4d3f8b7a2679db24b399325bb0efa009763cad8b2e0ac356ef02b35`.
- Model implementation: SHA256 `f55b28b56ecfa930fdf8627c6d79d23e43664471cf95b87cf244bd12197a2285`.
- Evaluator: SHA256 `08e46e1481b030d6eb17c750c3b2708c25a7ab6b73cdfb8990f245ef0818d27e`.
- Config: SHA256 `fc3515cafb70615c920dbc17dffafabde7b7ec345fb63bd5e56bf071512b05f0`.
- Empirical P3: SHA256 `cedec34851454b643be746746a1dd4bcc7e13807985c14e78504643ad6e71714`.
- Development: 40 dates and 664,335 current-placement lifecycles.
- Chronological OOF: 19 later Development dates with a one-day embargo.

The empirical active-order report cuts were 5.010s, 5.801s, and 7.801s. The p99 fitted support was 34.9s. The 1s/5s/10s cuts remain diagnostics only.

## Lifecycle Audit

Policy-request parity passed:

- 625,845 cancel requests and 625,619 ACKs;
- zero ACK-without-request, ACK-before-request, missing reason, or timestamp mismatch against the baseline replay;
- 198 pending-cancel fill rows, totaling 0.198 BTC, were retained;
- 53 requests preceded the local activation ACK and were retained as valid in-flight-new-order races;
- 99.85% of requests were scheduled `requote_replace`; 941 were path triggers.

The request-to-ACK latency distribution had p25/p50/p75 of 6/7/9ms, p95 of 28ms, and p99 of 90ms.

## Development Gates

| Gate | Result | Detail |
|---|---:|---|
| Baseline policy request parity | pass | Exact timestamps/reasons and ordering passed |
| ACK-latency CDF | pass | 12/12 BUY/SELL threshold curves calibrated; no monotonicity violation |
| Probability identity | pass | Simplex valid; fill/cancel CIFs nondecreasing in time |
| Support | pass | 6/6 side-by-role curves passed |
| Joint Brier improvement | pass | Positive day-clustered lower bound on 6/6 curves |
| Fill Brier improvement | pass | Positive day-clustered lower bound on 6/6 curves |
| Absolute fill calibration | fail | 3/6 curves passed; BUY add, SELL add, and SELL reducing overpredicted |
| Cancel-ACK Brier improvement | fail | Lower bound was negative on 6/6 curves |
| Integrated cancel-ACK calibration | fail | Bias interval was below zero on 6/6 curves |
| Overall Development gate | **fail** | Validation remains closed |

The dynamic model's integrated fill bias ranged from about +0.11 to +0.36 percentage points. The integrated cancel-ACK bias ranged from about -1.37 to -0.96 percentage points. At the 5.010s cut cancel ACK was often slightly overpredicted, while at 5.801s and 7.801s it was underpredicted. The failure is therefore a curve-shape and conditional-race problem, not a single intercept.

## Interpretation

The new decomposition resolved the most important identity failure: cancel is no longer fitted as a stationary natural hazard. Marginal request-to-ACK CDF calibration is stable, and state-dependent fill features improve proper scores over a mechanism-matched exposure-only fill baseline.

That is not enough for an absolute action-value input. A side/role marginal ACK latency PMF does not fully transport through the varying request time and the selection induced by surviving without fill. Request reason, request-time system state, order age, and fill/ACK dependence can change the effective ACK CDF at each remaining horizon. The v1 race consequently assigns too much mass to no-event or fill and too little to later cancel ACK.

This result closes the v1 implementation, not the placement fill estimand and not the observed fill-ranking information. It also does not justify weakening the frozen gate after seeing Development.

## Next Research Boundary

A future version, if pre-frozen, should:

1. preserve deterministic baseline request replay and pending-cancel fills;
2. model request-to-ACK survival from causal request-time state and reason;
3. test dependence between ACK latency and fill survival rather than assuming a side/role marginal race is sufficient;
4. compare against the same mechanism-matched exposure-only fill baseline;
5. remain current-placement-only until prediction gates pass.

KEEP/REPLACE, queue reset, campaign value, and action uplift remain separate experiments. No Validation access is recommended for this v1 family.

## Artifacts

- Development report: `${NARROWGATE_RETIRED_DATA_ROOT}/reports/placement_fill_policy_clock_race_v1_development_20260728/report.json`, SHA256 `a5bfe54d484b9d1d955787db6a3052af7c2f1f2a39250d1f66a9791ed74357e1`.
- Gate evaluation: `curve_evaluation.json`, SHA256 `4b2532a955ed0147b0904a10b6c42746b94b092119c789a4eb277ad99a1ea39b`.
- OOF CIF rows: `oof_policy_clock_predictions.parquet`, SHA256 `c9ae5b3ca0fe435ddbc4c553c18d852b1f902b925696b53374f4c7086f10db0d`.
- OOF ACK latency: `oof_ack_latency_predictions.parquet`, SHA256 `9367871a93eb3677b2a9dbecaa2690a28bd1ddd2b83ba3cedb752206747ea2af`.
- Research-only model artifact: `policy_clock_fill_cif.joblib`, SHA256 `5f76b70cf3c76138cffd55bd85ce5e9cba8328174aa8d20217f3b4a025485d8d`.
