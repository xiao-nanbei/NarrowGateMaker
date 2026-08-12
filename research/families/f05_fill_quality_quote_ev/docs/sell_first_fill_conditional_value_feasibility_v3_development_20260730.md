# SELL First-Fill Conditional Value Feasibility v3

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Decision

`close_local_prediction_branch_on_development`

The corrected native replay confirms that SELL first-opener fills have negative average decision-to-campaign-terminal value in the frozen Grade-A Development panel. The frozen submit-time local feature model does not identify a selectively worse subset. Validation and sealed holdout were not read. No action experiment or live deployment is authorized.

## Identity

v3 is the first formal fit in this line whose entire target-day denominator and each previous-natural-day L2 warmup pass the normalized 100 ms formal identity. It contains 22 Grade-A and 11 Grade-B Development days. The native producer completed all 33 days and emitted 12,025 exact first-opener lifecycle rows with 99.8091% true-opener coverage.

The chronological evaluation uses 13 Grade-A OOF days and six Grade-B transport days. SELL is the primary side; BUY is descriptive. The target is direct post-decision value in USDC per observed first-opener fill:

`campaign_terminal_equity_usdc - decision_equity_usdc`

This is a fill-conditioned observational estimand. It is not operational quote value and cannot establish the counterfactual effect of suppressing an order.

## Development Result

| Panel | Side | OOF rows | Mean value (USDC) | Simultaneous interval | Local MSE improvement | Supported |
|---|---|---:|---:|---:|---:|---:|
| Grade A | SELL | 2,330 | -0.007151 | [-0.014335, -0.000496] | -0.0000782 | no |
| Grade B | SELL | 1,254 | -0.008095 | [-0.020291, -0.000857] | -0.0001753 | no |
| Grade A | BUY | 2,228 | -0.011994 | [-0.018521, -0.004809] | +0.0000008 | no |
| Grade B | BUY | 1,287 | -0.007831 | [-0.018774, +0.000719] | -0.0000264 | no |

The Grade-A SELL model selected 18.24% of OOF rows as high risk. Their observed mean was -0.002960 USDC, with simultaneous interval [-0.016102, +0.009637]. The selected-minus-complement value gap was +0.005126 USDC, with interval [-0.009080, +0.019862]. The point estimate is in the wrong direction: the predicted high-risk group was less negative than its complement.

The Grade-A SELL local model also failed to improve the past-only intercept baseline. Its MSE-improvement simultaneous interval was [-0.0002550, +0.0000364] USDC squared. Although 11 of 13 selected-group daily means were negative, the familywise lower bound on that fraction was only 15.38%, below the frozen 60% gate. Grade B preserved a negative selected-group mean but failed the same prediction and selectivity gates.

## Interpretation

The result supports one claim:

1. In this Development identity, an observed SELL first-opener fill has negative average downstream campaign value.

It does not support either of these claims:

1. The frozen submit-time local features identify which SELL opener fills are avoidable losses.
2. Cancelling, widening, delaying, or suppressing a SELL opener improves campaign value.

The 120-hour live diagnostic therefore transported at the aggregate-loss level, but not at the conditional-selection level. The local branch is closed without reading later panels. It must not be rescued with a new threshold, post-hoc subgroup, alternate target horizon, or a more flexible model on the same Development outcome.

External receive-time state remains a separate F04 incremental hypothesis once its independently frozen capture denominator is complete. Prediction evidence, if any, would still require a new randomized F09 action identity.

## Authority

Frozen fit identity:

- canonical fit SHA256: `f5eedd8feec8227d712df70104acb60d3083a7080ffa27d0927f22953cd9c73a`
- fit-spec file SHA256: `71e4579fa351227dc686a87243204d1b27a8f8bb154e400bea9c0909e81b6762`
- native trace SHA256: `3d28bc723c6a4f8a6a71748020411ee800303151d0068ce1cdc36483fd964f35`

Authoritative machine artifacts:

`${NARROWGATE_RETIRED_DATA_ROOT}/reports/sell_first_fill_conditional_value_feasibility_v3_20260730/development_fit`

- `report.json` SHA256: `c1a9f80e37662517467e3f72b1daebea0c638570d8a71fe59eba96c19a07f4a2`
- `manifest.json` SHA256: `24861599bb5047f6df47b843051ea8a24ec8d9f72fc05e889ff2d6e093cb9242`
- `oof_predictions.parquet` SHA256: `853b1314d7e0b7d3659afd11d623a18419e310e043ce56326c08cf5175c4be5e`
