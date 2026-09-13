# Volatility-Time Add-Rearm Negative-Result Attribution v1

Last materially modified: 2026-07-29

## Status

`diagnostic_complete_randomized_v1_closure_unchanged`

This is a post-result, Development-only diagnostic of the closed `volatility_time_add_rearm_randomized_replay_v1` action. It was not preregistered and has no ranking, selection, Validation, holdout, action, or live authority.

The authoritative randomized decision remains:

`close_variance_time_add_rearm_action_on_development`

## Terminal-value bridge

For each randomized lineage, the exact accounting identity is:

\[
V_{\mathrm{campaign\ terminal}}
=V_{\mathrm{pre\ assignment}}
+R_{\mathrm{lineage}}
+V_{\mathrm{post\ lineage\ continuation}}.
\]

All contrasts below are variance-time candidate minus fixed-wall-time control, with complete UTC-day cluster bootstrap intervals.

| Side | Component | Uplift and pointwise 95% interval, USDC/lineage |
|---|---|---:|
| BUY | Already accrued before assignment | +0.004202 [+0.001195, +0.007367] |
| BUY | Decision-to-lineage-terminal reward | +0.000738 [-0.002064, +0.003534] |
| BUY | After-lineage campaign continuation | +0.002667 [-0.000326, +0.005806] |
| BUY | Decision-to-campaign-terminal value | +0.003405 [-0.000436, +0.007309] |
| BUY | Original campaign-terminal metric | +0.007607 [+0.003230, +0.012099] |
| SELL | Already accrued before assignment | +0.000976 [-0.003790, +0.005062] |
| SELL | Decision-to-lineage-terminal reward | +0.001875 [-0.002291, +0.005985] |
| SELL | After-lineage campaign continuation | -0.002854 [-0.008642, +0.002260] |
| SELL | Decision-to-campaign-terminal value | -0.000979 [-0.008502, +0.006056] |
| SELL | Original campaign-terminal metric | -0.000003 [-0.010038, +0.009274] |

The BUY campaign-terminal lower bound was not a clean post-assignment effect. A material part of the apparent uplift had already accrued between campaign start and lineage assignment. Removing that carried PnL makes the decision-to-campaign-terminal interval cross zero.

SELL shows the opposite bridge. Its lineage reward point estimate is positive, but the estimated continuation after lineage termination is negative and reverses the decision-to-campaign-terminal point estimate. This is why the shorter lineage reward did not transport to campaign terminal value.

## Post-result slices

Causal 60-second variance was available for 17,417 / 17,460 lineages (99.75%). Variance state uses the frozen BUY/SELL reference rates and the post-result diagnostic bins `<0.5x`, `0.5-1x`, `1-2x`, and `>=2x`. Inventory state uses absolute inventory at assignment in 0.001 BTC units; it is not a consecutive-fill-unit label.

The following pointwise intervals exclude zero:

| Side | Diagnostic slice | Metric | Uplift and pointwise 95% interval |
|---|---|---|---:|
| BUY | inventory layer 1 | post-lineage continuation | +0.002924 [+0.000891, +0.005109] |
| BUY | variance 0.5-1x | lineage reward | +0.006901 [+0.000558, +0.012919] |
| SELL | variance <0.5x | lineage reward | +0.006222 [+0.002110, +0.010776] |
| SELL | variance <0.5x | decision-to-campaign-terminal | +0.011806 [+0.002504, +0.021646] |
| SELL | variance 0.5-1x | decision-to-campaign-terminal | -0.029020 [-0.061286, -0.008084] |
| SELL | variance 0.5-1x | post-lineage continuation | -0.026514 [-0.059314, -0.007225] |

These are multiple, post-result, pointwise comparisons. They identify possible questions for a genuinely new mechanism but cannot select a volatility regime, freeze a state threshold, reopen the whole-clock replacement, or access its later panels. No simultaneous selection band or independent confirmation was performed.

## Unsupported slices

The randomized panel did not persist exact earlier/later clock direction or the evolving consecutive same-side fill-unit state. Only 2,591 / 17,460 lineages (14.84%) exactly match the older baseline mechanics path. Since the randomized assignment changes later fills and lineage creation, transporting those baseline labels into the remaining randomized paths would be a post-treatment selection error.

Accordingly:

- earlier/later outcome attribution remains unsupported;
- consecutive-fill-unit outcome attribution remains unsupported;
- initial inventory-unit slices must not be renamed as consecutive fills;
- the 14.84% matched subset must not be used to define a candidate action.

## Research consequence

The whole variance-time replacement remains closed. The reusable result is the lineage-randomized full-path infrastructure and a cleaner outcome contract for future F09 experiments:

\[
V_{\mathrm{decision\ to\ terminal}}
=V_{\mathrm{campaign\ terminal}}
-V_{\mathrm{pre\ assignment}}.
\]

Future randomized identities should either use this post-decision terminal value directly or preregister a covariate-adjusted estimator. Campaign PnL earned before assignment should remain a balance diagnostic, not an action outcome.

## Authority

- `validation_read=false`
- `sealed_holdout_read=false`
- `ranking_score=null`
- `action_experiment_authorized=false`
- `live_deployment_authorized=false`

Machine outputs are frozen under `MarketData/NarrowGate_BTCUSDC/reports/volatility_time_add_rearm_negative_result_attribution_v1_20260729/development`.
