# Placement Fill CIF V1 Development Evidence

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Date: 2026-07-26

Status: Development prediction evidence only. Validation and the family-specific sealed holdout remain unread. This result does not authorize an action arm or a live policy.

## Storage And Data Identity

The retired mixed-granularity top-level `l2/` compatibility links and an unreferenced deep replay copy were removed after confirming that the formal path uses `normalized_l2_100ms_v2/{bbo,l2}` and the raw `cryptohftdata/` snapshot/delta authority. Fifty-three rebuildable v10 replay cache files were also removed, reclaiming about 60.16 GiB. The cleanup identity is recorded in:

```text
${NARROWGATE_RETIRED_DATA_ROOT}/reports/storage_cleanup_20260726.json
```

The raw snapshot/delta authority, canonical normalized 100ms L2, referenced replay roots, and top-level BBO bridge input were retained. The formal panel uses individual futures trades and native exchange-time snapshot/delta replay; it does not use the retired one-second L2 panel.

## Frozen Estimand

The family contract is `docs/placement_fill_cif_v1_spec_20260726.json`, SHA256 `3852fbcb4a588b616f00dd6c9dfc50475dc4f5b713113e40fbfe0ba4a659b26f`.

The placement actions are `closer_1tick`, `current`, and `farther_1tick`. For each action and horizon, the direct label is:

\[
P\left(
F < \min(t_0+h,C_{\mathrm{ACK}})
\mid do(a),x_0
\right),
\qquad h\in\{1s,5s,10s\}.
\]

Action-specific activation, GTX rejection, cancel ACK, and fills during the ACK race are retained. The three children share the decision, latency draw, TTL, and market path, and shadow fills do not feed back into inventory. Circuit-breaker close orders are system actions and are excluded through the lifecycle submit identity.

KEEP and REPLACE are not placement actions. Their risk origin is an already active order snapshot; KEEP preserves queue while REPLACE resets it. They remain the separate, unbuilt `active_order_continuation_surface_v1` estimand.

## Development Panel

The builder streamed one UTC day at a time and deleted each large baseline trace after partition admission. The final panel occupies about 208 MiB:

```text
${NARROWGATE_RETIRED_DATA_ROOT}/reports/
  placement_fill_cif_v1_development_20260726/
```

| Check | Result |
|---|---:|
| Frozen Development days | 40 / 40 |
| Placement cohorts | 664,335 |
| BUY / SELL | 329,276 / 335,059 |
| Opener / add / reducing | 234,362 / 108,903 / 321,070 |
| System close orders excluded | 421 |
| Queue-path validity, three actions | 96.87%–96.91% |
| Future-ready feature violations | 0 |
| Native sequence gaps / invalid messages | 0 / 0 |
| Observed pathwise monotonicity violations | 0 |
| Retained trace directories / staging entries | 0 / 0 |

The panel run identity is `1d9d1b5d96609ed79fe074f5e6cbd38a9ff1a80192f32dc39c7bc4dc9c393d86`. Its final manifest SHA256 is `a42d77d2e7a9cf320d646baa9f3e8e870c75457263d55762da4ed55aefd5f9a9`.

## Direct CIF Fit

BUY and SELL use separate depth-2 monotone histogram gradient-boosted models. Distance and volatility-normalized distance are constrained to decrease fill probability; horizon is constrained to increase it. The fit uses expanding chronological outer folds with a one-day embargo. Probability calibration uses only reusable past-only inner-OOF raw scores admitted to each outer train.

The model produced 2,950,881 OOF rows across 19 future Development days and had zero predicted pathwise monotonicity violations. Across all 18 side-by-role-by-horizon cells:

| Metric | Result |
|---|---:|
| Event support | 213–12,101 fills per cell |
| OOF day support | 19 days per cell |
| Brier improvement vs exposure-only | positive in 18 / 18 |
| Day-clustered Brier lower bound | positive in 18 / 18 |
| Brier skill | 0.15%–3.62% |
| Average-precision lift | 1.96x–6.35x |
| Daily rank direction | 66.7%–100% |
| Calibration slope in [0.8, 1.2] | 18 / 18 |
| Absolute calibration intercept <= 0.01 | 0 / 18 |

The absolute intercepts range from about `-0.247` to `+0.148`; even the smallest absolute intercept is about `0.018`. The model therefore provides stable conditional ranking and lower Brier error, but the frozen operational absolute-probability gate fails in every cell. Temporal and role-specific base rate drift remains after inner-OOF affine-logit calibration.

The diagnostic artifact is stored at:

```text
${NARROWGATE_RETIRED_DATA_ROOT}/reports/
  direct_fill_cif_v1_development_20260726/
```

The model-contract SHA256 is `fe589b9f8fb2746d188da64bf7347a10a4007780a32f0990b61e61ee2c96768e`. The joblib artifact SHA256 is `3b76cba21581831e75836f0fba7c41a63958551f1349a57da2d24a32e636d360`.

## Decision

`development_prediction_gate_passed=false` and `validation_access_allowed=false`. Validation and the sealed holdout were not read. The result may be used to diagnose state ranking and probability drift, but its probability level must not be consumed by quote core, cancel/re-enter, or a live lookup table.

The next model revision, if registered, must address causal base-rate drift inside a new frozen Development identity. It cannot relax the observed calibration gate after seeing this result. KEEP/REPLACE must continue under its own active-order risk set and action-uplift experiment.
