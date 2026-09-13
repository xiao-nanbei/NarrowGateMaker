# Placement Marginal Fill Value Feasibility v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Status: `closed_on_development`. This evidence-only family trains no fill model, reads no Validation or sealed holdout outcomes, and grants no Value, Action, or live authority.

## Question

The audit asks whether the extra fills created by moving a placement quote two or four ticks shallower have signed, quantity-weighted USDC value after campaign-terminal and differential pending uncertainty are propagated. A one-tick grid is retained only as a negative control.

For each same-path, all-actions-activated cohort:

\[
\Delta V_{deep-shallow}
=
V_{shared\ fill\ execution\ improvement}
-
V_{shallower-only\ marginal\ fills}
-
\Delta C_{campaign/pending}.
\]

The authoritative economic metric is the no-policy-feedback `campaign_terminal_overlay_delta_usdc`. It is a feasibility diagnostic, not a counterfactual action value.

## Frozen Identity

- Spec: `placement_marginal_fill_value_feasibility_v1_spec_20260729.json`, SHA256 `e1e55151e87f4b2ebf9837389b4a526a06f71bf33f9c1ce2f4053c4b1b85e4c5`.
- Implementation SHA256: `fa2d48eb7db8e43a0f552b07c84d00ee34655257674f64ae9a22d8679cc493c4`.
- Development: 50 frozen UTC days, 2026-04-13 through 2026-06-25 with retained gaps.
- Authoritative report: `${NARROWGATE_RETIRED_DATA_ROOT}/reports/placement_marginal_fill_value_feasibility_v1_development_20260729/report.json`, SHA256 `4ec56ade6197d1c9aa0e9bb8812df654eb33bdac7803410175a4f80691c7fcdd`.
- Cell metrics SHA256: `27f0f89b25740651c51aa8f33c35e5d29ee1f924f7bb442260d77d0767bb2b95`.

Two pre-authoritative runs stopped before any economic output was produced. The first exposed request-state `action` column shadowing; the second exposed GTX-rejected cohorts entering a contract that requires all seven actions to activate. Both mechanics defects were fixed and tested before the final Spec hash above was frozen. The final run has zero full-lifecycle monotonicity violations on common support.

## Development Result

| Gate | Formal 2/4-tick cells passing |
|---|---:|
| One-sided terminal-value interval beyond 0.0001 USDC/decision | 0 / 24 |
| Campaign attribution coverage at least 95% | 0 / 24 |
| At least 30 supported days | 24 / 24 |
| Daily direction stability | 0 / 24 |
| Differential pending uncertainty below economic LCB | 0 / 24 |
| Campaign tail non-worsening | 0 / 24 |
| Complete feasibility contract | **0 / 24** |

The one-tick negative control also passed 0/12 cells.

The paired run produced 6,882,642 contrast rows. Per formal cell, shallower-only marginal fills numbered 40 to 740 and represented about 0.068% to 0.365% of decisions. The campaign-terminal point estimates ranged from `-1.64994e-5` to `+2.28411e-5` USDC/decision. Every simultaneous interval crossed zero:

- lowest lower endpoint: `-5.42665e-5`;
- highest lower endpoint: `-1.49260e-5`;
- lowest upper endpoint: `+2.12677e-5`;
- highest upper endpoint: `+6.06082e-5`.

Thus even the largest point estimate was below the frozen `0.0001 USDC/decision` economic-resolution budget before uncertainty was applied.

Direct paired pending analysis materially narrowed the old absolute uncertainty envelope: the simultaneous differential pending radius was about `1.00191e-5 USDC/decision`. It did not rescue a cell because the primary terminal-value lower bound was zero everywhere.

Campaign-terminal attribution coverage ranged from 71.40% to 94.88%. This is also an independent fail-closed result: no side-role-action cell reached the frozen 95% coverage gate.

## Mechanism Slices

The 1s, 5s, and 30s marks remain mechanism slices, not natural action horizons. Two BUY four-tick `current_farther` marginal-fill cells had a one-sided positive 30s shallower-only value interval. One SELL-add four-tick `current_farther` cell had a barely one-sided positive total 30s deeper-minus-shallower interval:

\[
[1.29\times10^{-7},\,3.16\times10^{-5}]
\text{ USDC/decision}.
\]

Its campaign-terminal interval was `[-3.64e-5, +3.91e-5]`, so the short-horizon markout did not transport to the primary lifecycle result. These slices are diagnostics and cannot create a placement action.

## Decision

`close_placement_distance_value_path_development`

F06 remains closed. This result does not say that quote distance has no effect on fills; the prior paired audit already identified two- and four-tick fill differences. It says those differences do not establish signed conditional net value once marginal-fill value, campaign continuation, pending race, tail, and day-clustered uncertainty are kept in the same estimand.

Do not create:

- an ordered fill surface v2;
- `placement_action_value_surface_v1`;
- `placement_quote_action_uplift_v1`;
- a Validation/holdout read;
- a shadow or live placement-distance action.

Reopening requires genuinely independent evidence that changes the economic estimand or attribution coverage, not another calibration, monotonicity, or action-gap patch to this Development panel.
