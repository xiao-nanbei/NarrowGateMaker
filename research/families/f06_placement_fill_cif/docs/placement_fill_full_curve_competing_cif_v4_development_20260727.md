# Placement Fill/Cancel-ACK Competing CIF v4

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Date: 2026-07-27

Decision: closed on Development. Validation and sealed holdout were not read. No action or live deployment is authorized.

## Frozen Question

v4 separately exports and calibrates the OOF probability simplex

\[
\left(
F_{fill}(t),
F_{cancelACK}(t),
1-F_{fill}(t)-F_{cancelACK}(t)
\right)
\]

for the placement estimand

\[
P(T_{fill}\le t,T_{fill}<T_{cancelACK}\mid x,a).
\]

The implementation, evaluator, curve-level gate, bootstrap identity, support requirements and numerical tolerances were frozen in `docs/placement_fill_full_curve_competing_cif_v4_spec_20260727.json` before the separately exported cancel-ACK OOF curve was read.

## Data And Output

- Development days: 50.
- Chronological OOF days: 24 after the expanding training prefix and embargoes.
- Placement cohorts: 831,635.
- Action lifecycles: 2,494,905.
- OOF rows: 7,202,322.
- Empirical report cuts: 5.010s, 5.816s, and 7.900s.
- Legacy 1s/5s/10s: report-only, excluded from the gate.
- OOF SHA256: `755b333f083c2fd370758f5087c4d5e9e6588a4ea5300cc138105c7086d6c2a5`.
- Evaluation SHA256: `ee962c3543afe6c333a83aabe70068809a6368e0263e7d1ef9c998c6d691f2a9`.

## Pre-Frozen Curve Gate

The gate is integrated equally over the empirical p25/p50/p75 cuts for each of six `side x inventory_role` curves. It requires:

1. probability-simplex and mutually exclusive target identities;
2. fill and cancel-ACK time monotonicity;
3. paired fill-distance monotonicity;
4. at least 20 OOF days and 100 events per cause;
5. positive day-clustered 95% lower bounds for joint three-state Brier improvement and both cause-specific Brier improvements;
6. fill and cancel-ACK integrated calibration-bias intervals containing zero.

There are no independent horizon-cell gates and no arbitrary `abs(intercept) <= 0.01` rule.

## Result

All hard identities passed:

- probability simplex: pass;
- time-monotonicity violations: 0;
- fill-distance violations above `1e-5`: 0;
- maximum raw float32 distance discrepancy: `3.49e-6`.

All six curves passed support and all three proper-score requirements. The joint Brier-improvement lower bounds ranged from about `0.0400` to `0.0459` or higher, and every fill/cancel cause-specific lower bound was positive.

The family failed cancel-ACK calibration in four curves:

| Curve | Integrated cancel bias | 95% day-cluster interval | Result |
|---|---:|---:|---|
| BUY add | -0.00364 | [-0.01117, +0.00462] | pass |
| BUY opener | -0.00755 | [-0.01416, -0.00110] | fail |
| BUY reducing | -0.00957 | [-0.01555, -0.00348] | fail |
| SELL add | +0.00447 | [-0.00501, +0.01372] | pass |
| SELL opener | -0.00772 | [-0.01504, -0.00085] | fail |
| SELL reducing | -0.01111 | [-0.01815, -0.00372] | fail |

All six fill-CIF calibration intervals contained zero. Thus the new export resolved the v3 observability gap and showed that the remaining defect is specifically the cancel-ACK cumulative incidence level, not fill ranking, proper scoring, support, or probability identities.

At the empirical median 5.816s, observed/predicted cancel-ACK rates included:

| Curve | Observed | Predicted |
|---|---:|---:|
| BUY opener | 46.21% | 45.57% |
| BUY reducing | 48.86% | 47.93% |
| SELL opener | 46.43% | 46.04% |
| SELL reducing | 48.01% | 46.73% |

The direction is mostly underprediction for opener/reducing and persists when integrated across the empirical lifetime cuts.

## Interpretation

v4 uses side-specific hazard heads and side-specific past-only hazard offsets. Inventory role is a feature, but the calibration layer does not separately reconcile opener/add/reducing cumulative baseline-policy cancel rates. A side-level interval-hazard offset can therefore improve Brier score while leaving role-specific CIF calibration drift.

This is not permission to add role-specific offsets after looking at v4 and then call the same family successful. Any successor must receive a new family identity and pre-freeze a past-only nested calibration procedure. In particular, Validation remains sealed for v4.

That successor was frozen as `placement_fill_role_calibrated_competing_cif_v5`. Its trailing train-only role offsets improved reducing calibration but worsened opener calibration; the unchanged gate again passed only add curves. v5 also closed on Development without reading Validation. See `docs/placement_fill_role_calibrated_competing_cif_v5_development_20260727.md`.

KEEP and REPLACE still require a separate active-order estimand. Campaign repair remains a delayed-entry, minute-scale inventory transition and is not part of this placement gate.
