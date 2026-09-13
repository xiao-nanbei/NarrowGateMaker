# Cooldown Action Leverage Frontier v1 - Development

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Decision

The tested cooldown temporal-permission action subspace is exhausted.

This conclusion is narrower than closing F09. F09 remains active for mechanism research with no registered action, but another clock, threshold, or number of blocked add cycles is not a sufficiently new economic intervention.

This audit did not run a replay, fit a model, estimate a new treatment contrast, read Validation or sealed holdout, create an action family, or grant live authority.

## Evidence Boundary

The frontier separates two evidence classes:

- `current_authoritative`: the 40-day current-stack one-cycle mechanics and variance-time randomized reports retain metric authority;
- `withdrawn_old_denominator_closure_authoritative`: state-conditioned rearm, recovery-event rearm, and stop-add-until-flat retain their frozen closure decisions, while their exact numbers are displayed only as historical scale diagnostics.

No cross-source pooled estimate was calculated. Action-change rates, candidate rates, selected-cycle fill rates, and fill-retention changes are different leverage proxies; the table keeps their identities rather than pretending they share one denominator.

## Frontier

| Action | Side | Evidence | Action / candidate rate | Fill-path divergence proxy | Fill retention | Reward, 95% CI (USDC) | Classification |
|---|---|---|---:|---:|---:|---:|---|
| one-cycle skip | BUY | current | 99.30% | 7.16% selected-cycle fills | n/a | not read | weak fill-path leverage |
| one-cycle skip | SELL | current | 99.20% | 6.26% selected-cycle fills | n/a | not read | weak fill-path leverage |
| state-conditioned rearm | BUY | withdrawn metrics; closure valid | 53.19% multicycle | n/a | n/a | +0.004932 [-0.001656, +0.012746] | sparse support, economics unsupported |
| state-conditioned rearm | SELL | withdrawn metrics; closure valid | 53.85% multicycle | n/a | n/a | -0.005041 [-0.013141, +0.002259] | sparse support, economics unsupported |
| recovery-event rearm | SELL | withdrawn metrics; closure valid | 10.95% candidate | 12.81% conservative suppression | 87.19% | -0.015603 [-0.029960, -0.003765] | economically harmful |
| variance-time rearm | BUY | current | 37.02% final action change | 4.80% absolute fill-count change | 104.80% | +0.000738 [-0.002000, +0.003577] | economics unsupported |
| variance-time rearm | SELL | current | 24.41% final action change | 0.59% absolute fill-count change | 99.41% | +0.001875 [-0.002303, +0.006242] | economics unsupported |
| stop-add-until-flat | SELL | withdrawn metrics; closure valid | 85.73% candidate | 89.22% expected suppression | 10.78% | +0.008155 [-0.021603, +0.037109] | participation shutdown |

The variance-time fill-count change is `abs(1 - fill_retention)`. It is not a paired per-order path divergence rate and is not used as a causal denominator. Likewise, historical reward divided by a fill-retention change is only a descriptive ratio, not marginal fill value.

## One-Cycle MDE

The current-stack one-cycle intervention changes the next quote action in about 99% of release episodes, but only 6%-7% of the suppressed orders would fill. Its design MDE therefore implies:

\[
\text{required conditional value scale}
=
\frac{\text{design MDE}}{\text{selected-cycle fill rate}}.
\]

| Side | Design MDE (USDC/release) | Selected-cycle fill rate | Required value per affected fill |
|---|---:|---:|---:|
| BUY | 0.014365 | 7.16% | 0.200692 USDC |
| SELL | 0.015456 | 6.26% | 0.246880 USDC |

These are required detectable effect scales, not profit estimates. They are far above the observed first-add average loss scale, so adding more dates to the same one-cycle action has low expected research value.

## Synthesis

The completed actions span the tested leverage range:

- one-cycle skip changes quotes but usually not fills;
- state-conditioned and recovery extensions persist across cycles but do not produce supported economic value;
- variance-time changes final actions while both primary reward intervals cross zero and lifecycle consistency fails;
- stop-add-until-flat removes about 89% of expected add fills and is an overbroad risk-control shutdown.

No tested row combines acceptable activity, a positive reward lower bound, and supported lifecycle/tail evidence. The correct governance state is:

`tested_cooldown_temporal_permission_action_subspace_exhausted`

This does not claim that every possible cooldown or every F09 action is exhausted. A successor must change the economic intervention itself, rather than choose another time scale or wait a different number of quote cycles.

## Permissions

- `f09_family_closed=false`
- `f09_status=mechanism_research_active_no_registered_action`
- `cross_source_pooled_estimate=false`
- `validation_read=false`
- `sealed_holdout_read=false`
- `action_experiment_authorized=false`
- `live_deployment_authorized=false`

F04 receive-time evidence collection continues independently in the background and is not a prerequisite for this conclusion.

## Identity

- Frozen spec file SHA256: `4e97d86cdfd9a7c327a429a15d79fc53ae0162125979eb27dd96be61b4f222dc`
- Canonical spec SHA256: `9b741e26da9c48c8f90f91ef96e5b5d5ff56926641178e7c2bc4b0cf174956b0`
- Evaluator SHA256: `1e27cdd12315f189c01c17f23783a36a162780619376b9522b27e28c3e59d827`
- Test SHA256: `6bc577ab53542be6227293036b6ee4a2685c49c4791fb9e188f826f33f543070`
- Machine report SHA256: `7db2fc231caf989d170e2a29198e30fb6a88326d9a7a581d0302b1fb2e01d133`
- Frontier CSV SHA256: `08262c1ae4ed27224897accf50a3a0943401b29d0e00f13e7bb8231d3f714c88`
- Machine manifest SHA256: `32b7019b5fe7a29f659a233e084e83ccf090682218e2a432957abf7854e345c6`
- Machine output: `${NARROWGATE_RETIRED_DATA_ROOT}/reports/cooldown_action_leverage_frontier_v1_20260730/development`
