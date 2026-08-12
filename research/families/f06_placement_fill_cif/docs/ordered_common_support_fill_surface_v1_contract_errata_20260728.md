# Ordered Common-Support Fill Surface v1: Contract Errata

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Status: post-run contract audit only. The frozen Spec, implementation, Development report, thresholds, panel access, and family decision are unchanged. Validation and sealed holdout remain unread.

## Audit Identity

- Frozen Spec SHA256: `c42146e2505cb8b567a2e653fb8a97f39169e6f5778d72781bf5660e9556b6e7`.
- Frozen Development report SHA256: `66262578a72c8afcb8307c502a4906b91555e329a3789ce401b8a98404af537d`.
- Machine-readable contract audit: `ordered_common_support_fill_surface_v1_contract_audit_20260728.json`, SHA256 `5384e6e286c33e1813fcf078cd9bd461eddbee0045d7a9c8c0d59fda052b116a`.
- The audit reverified the frozen implementation, request-state index, every ordered OOF artifact, and every request-state cache payload used for the affected cohorts.

## Closure Reconciliation

The family remains closed on Development for three independent hard-gate failures:

1. the evaluator did not implement the frozen cohort-common lifecycle clock;
2. activation calibration passed only 11/18 cells and all-three support calibration passed only 9/18 cells;
3. pending-fill economic uncertainty passed 0/36 cells.

The following positive diagnostics do not compensate for those failures:

- action-specific pre-request curves passed 18/18;
- conservative transport probability bounds passed 18/18;
- pending posterior-predictive checks passed 36/36.

## Apparent Monotonicity Violations

The audit independently reproduced 1,042 apparent violation rows and 399 unique affected cohorts. All 399 cohorts used unequal action-specific realized pre-request exposure clocks:

| Clock spread | Milliseconds |
|---|---:|
| minimum | 6 |
| p10 | 356 |
| median | 3,713 |
| p90 | 7,595 |
| maximum | 9,693 |

The constrained hazard was therefore integrated over different durations for closer, current, and farther. A monotone hazard does not imply ordered CIFs at different exposure times. These 1,042 rows are an evaluator-contract failure, not evidence that the LightGBM distance constraint failed.

The frozen v1 implementation is not modified or rerun. A future identity must use a pre-action cohort-common scheduled request clock, or explicitly model a joint action-specific lifecycle. The new `paired_lifecycle_contract.assert_common_prediction_clock` helper exists only as a prospective fail-fast contract and grants no permission to v1.

Following review, that prospective helper also rejects incomplete paired cohorts instead of dropping them. It accepts only a `PredictionClockContract` parsed from a future frozen family Spec. The Spec must bind the selected source to an allowed-source entry containing its causal cut, unit, producer identity SHA256, and explicit `ex_ante=true`, `cohort_common=true`, and `outcome_dependent=false` declarations. Passing a plausible column name alone is not sufficient.

## Diagnostic Wording

The frozen constrained-versus-unconstrained diagnostic rejects only when the 95% lower bound of ordered-minus-unconstrained Brier loss is above zero. Its 18/18 result supports only:

> No statistically significant harm was detected under this diagnostic.

It does not establish non-inferiority. A future non-inferiority identity must freeze an economic/statistical margin before outcomes and require:

\[
\operatorname{UCB}_{95\%}
\left(L_{\mathrm{ordered}}-L_{\mathrm{unconstrained}}\right)
\le \epsilon_L.
\]

The current 18/18 comparison remains diagnostic-only and cannot authorize prediction or action output.

## Weight Wording

The implementation uses:

\[
w_{i,a,j}=\frac{\mathrm{exposure\ seconds}_{i,a,j}}{3}.
\]

The accurate contract statement is:

> Counterfactual action replication factors sum to one, while interval exposure weighting remains intact.

The numeric interval weights within a cohort do not literally sum to one.

## Test Boundary

The original structural test verifies monotone hazard/CIF output when the three actions share the same prediction duration. It does not exercise the formal evaluator's duration construction. Future identities must include an end-to-end test that fails when action-specific realized fill/exposure time is used inside the common-clock monotonicity invariant.

## Permissions

The artifact search found no `placement_action_value_surface_v1` or `placement_quote_action_uplift_v1` code or report artifact. Final permissions remain:

```text
prediction_supported=false
transport_supported=false
economic_resolution_supported=false
action_uplift_supported=false
action_experiment_authorized=false
live_deployment_authorized=false
validation_read=false
sealed_holdout_read=false
```

This run is a chronological Development OOF prediction/lifecycle evaluation, not a strategy PnL backtest or action-uplift experiment.
