# Placement Fill Empirical Pending Nuisance v3: Development Result

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Status: Development complete and family closed. Validation and sealed holdout remain unread. No action or live deployment is authorized.

## Frozen v2 Conclusion

The final v2 conclusion remains unchanged:

> v2 fixed the cancel policy clock and request-conditioned ACK structural errors. Pre-request fill and ACK are only Development-supported building blocks at the side-by-role aggregate level. They were not action-specific live surfaces because v2 lacked a dependence-residual audit and per-action calibration gate. Pending fill had only 298 events in the 29-day OOF panel at the largest 9ms cut, so it was an extremely sparse nuisance. The complete learned three-phase family closed on Development.

The v2 mechanics spec, fit spec and evaluator remain byte-for-byte frozen at their original hashes. v3 does not relax or reinterpret any v2 gate and does not read v2 Validation or holdout.

## Identity

- Family spec: `docs/placement_fill_empirical_pending_nuisance_v3_spec_20260728.json`, SHA256 `45979003b605a08a94eeaffc6023e76e315deb22f5b88737cd41d80cd763c12a`.
- Authoritative fit spec: `docs/placement_fill_empirical_pending_nuisance_v3_fit_spec_v3_20260728.json`, SHA256 `4ce0bfe49d55371c9162e6838fe916d67640e5f77c2a68b03597723d0dcdbb1e`.
- Evaluator: `models/audit/evaluate_empirical_pending_nuisance.py`, SHA256 `1007efc0a6c26d159cac94ed57671b0d5432824fc49a8d670d6ccca27f60d20c`.
- Development report: `${NARROWGATE_RETIRED_DATA_ROOT}/reports/placement_fill_empirical_pending_nuisance_v3_development_20260728_v3/report.json`, SHA256 `02edfa4cac4121a0378d006ad9fa347152cc054b92e60685f40b9e767162b313`.
- Pending nuisance artifact: SHA256 `8b21f6ff8562c86d17437c5315a59ae71c4d068666479b766d6f4acd3b8c408b`.

The first fit identity aborted before statistical artifacts because its merge selected `action_lifecycle_id` twice. The second produced a valid closed result but checked monotonicity only at the largest cut and omitted fold identity. The third fit changed no threshold or model; it only emitted all six frozen cuts, fold identity, and observed as well as predicted monotonicity. Earlier outputs remain preserved with explicit failure/supersession notices.

The run reused the hash-verified C++ lifecycle and request-state caches. It did not replay native L2. The schema-complete Development evaluation took about 75 seconds and produced about 2 MiB of new artifacts.

### Evidence-boundary errata

Three reporting qualifications do not change any frozen gate, result, or decision:

1. The evaluator computes `prediction - target`, so the ACK residual statistic is E-minus-O, despite an earlier O-minus-E label. Because the gate checks only whether the interval contains zero, the sign-name correction does not change the 36/36 supported-cell result.
2. The spec froze six residual features, but only `request_active_order_count`, `request_request_batch_size`, and `request_book_age_ms` produced non-degenerate train-defined low/high cells. Pending-cancel-before count, absolute 100ms return, and absolute 100ms taker imbalance had degenerate training quantiles and were skipped. The result is therefore 36/36 supported non-degenerate cells, not complete validation of all six frozen state variables.
3. Zero observed path violations validate consistency of the cached paired lifecycle output and the v3 common-support cohort construction. They are not an independent rerun or revalidation of the native matcher.

## Estimand

The dynamic landmark decomposition is:

\[
F_{\mathrm{total}}(H)=
F_{\mathrm{pre}}\!\left(\min(H,T_{\mathrm{req}})\right)
+
\mathbf 1_{\{H>T_{\mathrm{req}}\}}
S_{\mathrm{pre}}(T_{\mathrm{req}})
F_{\mathrm{pending}}\!\left(
H-T_{\mathrm{req}}
\mid X_{\mathrm{req}},Q_{\mathrm{rem}}
\right).
\]

GTX rejection is an activation outcome and is excluded from conditional-fill monotonicity. The residual audit is a model-sufficiency check. It does not identify or prove conditional independence between latent fill and ACK times.

Pending fill uses a past-only side-by-action Beta-Binomial parent. Request reason and inventory role are separate partially pooled children and are never fully crossed. Joint Brier is labelled ACK-dominated and supplies no second piece of pending-fill evidence.

## Development Result

| Gate | Result |
|---|---:|
| action-specific pre-request fill curves | 18/18 pass |
| action-specific conditional ACK curves | 18/18 pass |
| supported ACK residual cells | 36/36 pass |
| pending posterior-predictive state tails | 33/36 pass |
| common-support monotonicity | fail |
| complete prediction surface | fail |

The 36 required action curves are:

\[
2\ \text{sides}
\times 3\ \text{roles}
\times 3\ \text{actions}
\times 2\ \text{components}.
\]

All passed support, day-clustered proper-score and absolute-calibration gates. This upgrades the earlier aggregate evidence, but it does not create a live surface because the other frozen gates fail.

All joint cells are ACK-dominated. ACK represents about 99.9598% to 99.9913% of terminal events in these cells. The approximately doubled multiclass Brier gain mainly reflects simultaneous correction of ACK and its no-event complement, not independent pending-fill information.

## Residual Audit

All 36 supported non-degenerate ACK state-tail cells pass Bonferroni-adjusted day-cluster E-minus-O intervals. These cells come from the three residual features with estimable train-defined tails; the other three frozen features were skipped after their training quantiles degenerated. The empirical pending nuisance fails three 99% posterior-predictive checks, all in the same state:

| Side/action | State | Observed | Predicted | 99% interval |
|---|---|---:|---:|---:|
| SELL closer | low request book age | 25 | 11.02 | [3, 21] |
| SELL current | low request book age | 23 | 11.09 | [4, 21] |
| SELL farther | low request book age | 21 | 10.39 | [3, 20] |

This is a clear sufficiency failure of the side-by-action parent in fresh-book SELL states. It is not evidence that latent fill and ACK times are conditionally dependent, and it cannot be repaired inside this frozen family by adding book age after reading the result.

## Common-Support Monotonicity

The observed paired paths have zero violations at every pre-request and pending cut. Because v3 reused hash-verified lifecycle/request-state caches rather than replaying native L2, this confirms consistency of the cached paired lifecycle output and common-support cohort construction. It does not constitute an independent native-matcher validation.

The prediction surfaces do not preserve the same order:

- pre-request predictions: 1,047 violations over 1,444,563 common-support cohort-cut rows, about 0.0725%; 106 of 108 fold-side-role-horizon cells exceed the frozen `1e-6` limit;
- pending side-action nuisance: 21 of 108 fold-side-role-horizon cells reverse at least one adjacent action probability.

The rates are numerically small, but an action lookup must not claim a distance-monotone surface when the paired data mechanics are exactly monotone. No post-outcome isotonic projection is allowed under this identity.

## Quantity And Economic Bound

The full 50-day Development lifecycle contains 610 hypothetical action-specific pending-fill events over the complete request-to-ACK race. This does not replace the frozen v2 count of 298: that number refers to 29 OOF days and the 9ms report cut, while 610 uses all Development days, all three paired actions and the complete race.

Every positive event filled `0.001 BTC`, equal to the remaining quantity at request, so this panel contains no sub-lot pending partial fill. The artifact still reports all four frozen quantity estimands:

- expected pending-fill quantity;
- probability of positive pending fill;
- conditional quantity given a positive fill;
- pending quantity divided by request remaining quantity.

At the frozen 100bps stress value, parent-level probability uncertainty maps to about `4.46e-05` to `4.77e-05 USDC` per decision, below the `0.0001 USDC` one-tick-order reference. However, the fresh-book SELL posterior-predictive failure means this parent-level bound is not a valid uniform state bound. The project cannot yet declare pending uncertainty economically negligible in all states.

## Decision

`placement_fill_empirical_pending_nuisance_v3` closes on Development. Validation and sealed holdout remain unread. The result grants no cancel, replace, placement or live authority.

A later family may preregister a monotone pre-request model and a pending hierarchy that includes fresh-book SELL state. That would be an outcome-informed new identity and must restart on Development; it cannot edit this family or use Validation to rescue it.
