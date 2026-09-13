# Decision-Visible Negative Fill-Value Evidence M0 v1.1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Decision

`close_decision_visible_negative_fill_value_prediction_development`

This identity used expanding chronological OOF and direct decision-to-campaign-terminal value in USDC per first-add decision. Validation and sealed holdout were not read. Prediction evidence does not authorize an action or live deployment.

v1.1 differs from the frozen v1 method only in its structural missingness contract: an unavailable queue value remains unavailable, is accompanied by a `queue_ahead_available` indicator, and receives a deterministic internal model placeholder. It is never reinterpreted as an observed zero queue.

## Development Result

The Grade-A primary OOF contained 327 BUY and 346 SELL campaigns over 13 test days per side. The Grade-B sensitivity OOF contained 172 BUY and 154 SELL campaigns over six test days per side.

| Side | MSE improvement (campaign-state minus local) | High-risk mean USDC | Familywise interval | Supported |
|---|---:|---:|---:|---:|
| BUY | -0.70865 | -0.04115 | [-0.11369, 0.03795] | no |
| SELL | -0.000986 | -0.05798 | [-0.14776, 0.01086] | no |

The local microstructure model did not improve the frozen campaign-state baseline with a positive familywise lower bound. The selected lower-quartile risk groups were negative on 61.5% of BUY days and 76.9% of SELL days, but both mean-value intervals crossed zero. Grade-B sensitivity also failed on both sides.

The result distinguishes two claims:

1. First exposure-increasing add decisions have negative average downstream value in the current Development denominator.
2. The frozen decision-visible local features cannot reliably identify which such decisions are the avoidable negative-value subset.

Only the first claim is supported. No F09 action was registered.

## Authority

The authoritative machine reports are outside the repository under:

`${NARROWGATE_RETIRED_DATA_ROOT}/reports/decision_visible_negative_fill_value_evidence_m0_v1_1_development_20260729`

- `report.json` SHA256: `ec19a2d2b1cdb941430332bc5b23f1551c9af5202243ff80d6472ce8ab18d767`
- `manifest.json` SHA256: `b750c91dfb77b37c354e74134eb396e9a0295a07f0aa839076be99252860c3a7`
