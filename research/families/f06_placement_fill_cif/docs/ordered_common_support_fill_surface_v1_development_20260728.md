# Ordered Common-Support Fill Surface v1: Development Result

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Status: Development complete and family closed. Validation and sealed holdout remain unread. No value, action experiment, or live deployment is authorized.

## Frozen Identity

- Spec: `ordered_common_support_fill_surface_v1_spec_20260728.json`, SHA256 `c42146e2505cb8b567a2e653fb8a97f39169e6f5778d72781bf5660e9556b6e7`.
- Development report: `${NARROWGATE_RETIRED_DATA_ROOT}/reports/ordered_common_support_fill_surface_v1_development_20260728/report.json`, SHA256 `66262578a72c8afcb8307c502a4906b91555e329a3789ce401b8a98404af537d`.
- Panel: the 50-day hash-frozen request-state Development panel and the same six expanding chronological folds used by request-state v2.
- Validation read: `false`.
- Sealed holdout read: `false`.

The implementation uses native C++ risk-set expansion and hash-compatible request-state caches. It did not rebuild or replay raw L2.

## Model Contract

BUY and SELL use separate Poisson fill hazards. The three actions share the same state and role representation. There is no action dummy; only `distance_ticks` changes within a paired cohort, and LightGBM receives a training-time decreasing monotone constraint on that feature. Each cohort has total training weight one. A shared positive side-by-role hazard calibrator is applied to all three actions.

Activation/GTX, common-support transport, and pending fill-before-ACK are separate components. Pending fill uses a past-only side-by-action-by-fresh-book Beta-Binomial nuisance and already includes the ACK competing process.

## Development Evidence

| Gate | Result |
|---|---:|
| action-specific pre-request curves | 18/18 pass |
| constrained vs unconstrained Brier diagnostic | 18/18 not significantly worse |
| common-support transport probability bound | 18/18 pass |
| maximum 99% transport bound | 0.00467 percentage points |
| activation absolute calibration | 11/18 pass |
| all-three support absolute calibration | 9/18 pass |
| fresh-book pending posterior predictive checks | 36/36 pass |
| pending economic uncertainty bound | 0/36 pass |
| final prediction identity | fail |

The fresh-book stratification fixed the earlier posterior-predictive undercoverage. It did not make the nuisance economically precise enough. The frozen 100bps stress uncertainty is approximately `0.000106` to `0.000298` USDC per decision, above the preregistered `0.0001` USDC bound in every cell.

Activation transport itself is tiny: all action-specific non-common-support bounds remain below `4.67e-05`. The past-only Beta levels nevertheless underpredict later activation/support rates in several side-role cells, so the absolute-calibration contract fails even though the conservative transport bound passes.

## Clock Qualification

The run emitted 1,042 apparent monotonicity violations over 1,444,563 paired cohort-cut rows, about 0.0721%. This is not evidence that the constrained hazard violated its distance contract.

The evaluator integrated each action to its own realized pre-request exposure. For the 399 affected cohorts, the three durations were never identical; their median max-minus-min difference was about 3.7 seconds. Filled actions often used their realized fill time while a surviving farther action used the later policy request time. Hazard ordering does not imply CIF ordering under unequal exposure clocks.

The spec defined monotonicity on a common lifecycle. Therefore this output is classified as an implementation-contract failure:

```text
monotonicity_contract_valid=false
```

It is not a failed statistical monotonicity result and cannot be repaired by post-outcome isotonic projection. A future identity would need a pre-action, cohort-common scheduled request clock or an explicit joint lifecycle model. The current identity is not rerun after this outcome was observed.

## Resolution Diagnostic

Only about 2.97%-9.22% of cohort cells received a nonzero predicted closer-minus-farther probability difference. This remains diagnostic: no economic identifiable set was selected, and point-estimate nonzero mass is not an action-resolution gate.

## Decision

The prediction identity closes on Development for three independent reasons:

1. the common-lifecycle monotonicity implementation contract was not met;
2. activation/support absolute calibration failed in multiple cells;
3. pending-fill uncertainty exceeded its frozen economic bound.

Consequently `placement_action_value_surface_v1` is not registered or trained, and `placement_quote_action_uplift_v1` is not created. The result grants no placement, cancel, replace, action-experiment, or live authority.
