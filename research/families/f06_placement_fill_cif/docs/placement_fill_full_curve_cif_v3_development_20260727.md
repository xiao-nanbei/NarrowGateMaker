# Placement Fill Full-Curve CIF v3: Development Evidence

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Date: 2026-07-27

Status: Development diagnostic only. Validation and sealed holdout were not read. This artifact does not authorize a strategy action or live deployment.

## Estimand

The model estimates the complete placement lifecycle curve

\[
P(T_{fill}\le t,\ T_{fill}<T_{cancelACK}\mid x,a),
\]

instead of fitting separate 1s, 5s, and 10s binary models. On a 100ms grid, v3 fits side-specific cause hazards for fill and baseline-policy cancel ACK:

\[
S_j=S_{j-1}(1-h^{fill}_j-h^{cancelACK}_j),
\qquad
F^{fill}_j=F^{fill}_{j-1}+S_{j-1}h^{fill}_j.
\]

Activation remains action-specific and is multiplied into the active-order fill CIF. The risk endpoint comes from the action lifecycle's observed cancel ACK or administrative censoring, rather than a universal forecast horizon.

## Frozen Data Identity

- Development: 50 previously exposed days through 2026-07-08.
- Cohorts: 831,635.
- Action lifecycles: 2,494,905.
- Cancel-ACK exposure rows: 2,353,250.
- Chronological OOF predictions: 7,202,322.
- Full-curve grid: 100ms through the Development p99 support of 34.5s.
- Spec: `docs/placement_fill_full_curve_cif_v3_spec_20260727.json`.
- Validation access: false.
- Sealed-holdout access: false.

The former v4 Validation days were included in Development because their outcomes had already been read before this new full-curve family was formed. This family therefore uses a new Validation and sealed-holdout identity.

## Empirical Report Points

The primary report cuts were frozen from the Development distribution of active exposure through cancel ACK:

| Quantile | Time |
|---|---:|
| p25 | 5.010s |
| p50 | 5.816s |
| p75 | 7.900s |

The old 1s, 5s, and 10s values remain report-only compatibility cuts. They are not model targets, natural horizons, or independent promotion gates.

## Revision Lineage

| Version | Development finding | Disposition |
|---|---|---|
| v1 | Midpoint interval subsampling omitted deterministic early non-event intervals and overpredicted CIF by roughly 2x. | Frozen failed evidence. |
| v2 | Hash-stratified sampling fixed that bias, but fill-only survival continued accumulating after policy cancel ACK. | Frozen failed evidence. |
| v3 | Fill and cancel ACK are cause-specific hazards in one survival process. | Current Development diagnostic. |

The failures are retained under their original artifact identities; v3 does not rewrite them.

## Development Results

Observed and predicted probabilities at the empirical median exposure are:

| Side | Role | Observed | Predicted | Brier improvement |
|---|---|---:|---:|---:|
| BUY | add | 2.6845% | 2.6598% | +0.000364 |
| BUY | opener | 3.7820% | 3.6150% | +0.001158 |
| BUY | reducing | 3.9591% | 3.7412% | +0.001362 |
| SELL | add | 2.6161% | 2.7714% | +0.000445 |
| SELL | opener | 3.7077% | 3.4738% | +0.001147 |
| SELL | reducing | 3.6856% | 3.5768% | +0.001148 |

Across p25, p50, and p75, all six side-role curves have positive day-clustered Brier-improvement lower bounds. The integrated probability-bias interval includes zero for all six curves except BUY reducing, whose mean underprediction is about 0.249 percentage points and whose upper bound is near zero. That curve needs explicit attention before any qualification rule is frozen.

Monotonicity diagnostics:

- time violations: 0;
- action-distance violations above the frozen `1e-5` numerical tolerance: 0;
- maximum raw float32 distance discrepancy: `3.58e-6`.

The evaluator is `models/audit/evaluate_full_curve_fill_cif.py`; its frozen output is `curve_evaluation.json` beside the v3 report, SHA256 `223c352654655a97fb1fec7137103bdb2657c0ba56af69f63d1cb1fc244e7e47`.

## Remaining Boundary

The v3 OOF table exports the target fill CIF but not the cancel-ACK cumulative incidence as a separate column. Cancel ACK already enters the shared survival calculation, which fixes the v2 estimand error, but the separately requested cancel-head OOF calibration cannot yet be audited from the frozen table. Therefore v3 has no curve-level pass gate and cannot unlock Validation.

This export gap was resolved by `placement_fill_full_curve_competing_cif_v4`, which pre-froze and exported both causes. v4 passed identities, support and proper scores but failed cancel-ACK calibration for opener/reducing curves, so it closed on Development without reading Validation. See `docs/placement_fill_full_curve_competing_cif_v4_development_20260727.md`.

Before a later successor may read Validation it must pre-freeze:

1. a past-only calibration method that transfers across inventory roles;
2. probability error translated into an action-value sensitivity budget;
3. the exact Validation artifact and one-time access rule.

## Separate Estimands

- Placement uses the new order's activation latency, queue reset, scheduled cancel request, and cancel-ACK path.
- KEEP begins from an already active order and uses remaining TTL and current order age while preserving queue position.
- REPLACE includes cancel ACK, activation latency, GTX risk, and a reset queue.
- Campaign repair is a minute-scale delayed-entry inventory transition and is not part of this placement CIF.

Prediction quality alone cannot establish `V(candidate)-V(baseline)`. Any later quote action still requires a separate known-propensity replay and action-uplift audit.
