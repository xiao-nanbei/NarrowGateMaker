# Multiscale EMA ADD-vs-WAIT Incremental Value v1.2

Last materially modified: 2026-08-09

Status: closed on Development. Neither side may register an F09 add-permission successor. Validation and sealed holdout remain unread. No action or live change is authorized.

## Question

This identity tested whether a source-aware multiscale EMA representation adds chronological native out-of-fold information about

\[
Q(\mathrm{ADD\ NOW})-Q(\mathrm{WAIT\ ONE\ EXTERNAL\ EPOCH})
\]

beyond frozen campaign state and existing local/trend features. The next epoch is the first common decision-visible market generation, not a fixed elapsed time and not an order-state event. Both forks return to the same frozen continuation policy and remain owned until common economic washout.

## Data Contract

The model used both years, with different authority:

- 66 provider-normalized 2025 days trained an unsupervised full-rank EMA state representation from every admitted 100 ms BBO source row. This consumed 112,090,884 side rows. It read no economic outcomes and claimed no queue or lifecycle authority.
- 40 native 2026 Development days supplied 320 side-specific exact-lifecycle ADD-vs-WAIT labels and trained every direct value head inside chronological folds.
- The four frozen outer folds cover 24 test days and 192 OOF rows, 96 per side. The earlier 16 days and 128 rows are training history, not OOF observations.

The original 10-second provider-grid implementation was abandoned before it wrote an encoder or OOF artifact. The corrected encoder uses every normalized 100 ms source row; 100 ms is the source resolution, not an action horizon. The OOF denominator correction is bound in the immutable v3 execution amendment.

## Results

| Side | Squared-error reduction | 95% CI | Absolute-error reduction | 95% CI | Gate |
|---|---:|---:|---:|---:|---:|
| BUY | -0.0001981 | [-0.0004225, -0.0000192] | -0.0033583 | [-0.0070060, -0.0001345] | Fail |
| SELL | +0.0005834 | [-0.0078585, +0.0066019] | +0.0061066 | [-0.0092827, +0.0212533] | Fail |

BUY became worse on both frozen proper-loss comparisons, with both intervals entirely below zero. SELL had a positive point estimate on both comparisons, but neither lower bound was positive. Every fold contributed 24 rows per side, campaign total training weight was exactly one, and all 320 native labels reached common washout.

## Decision

The full 2025 EMA representation does not provide a robust incremental ADD-vs-WAIT value signal over M0 on the native 2026 chronological panel. This closes only `multiscale_ema_add_wait_incremental_value_source_aware_v1_2`; it does not claim that all trend information or all state-dependent cooldown policies are useless.

The current 85-second cooldown remains part of the operational baseline because this successor failed to justify a replacement. It is not thereby promoted to a theoretical or empirically optimal constant. Reopening this consumed branch by changing EMA half-lives, Ridge settings, the action epoch, feature subsets, or gates on the same Development panel is forbidden.

## Bound Artifacts

- Spec: `multiscale_ema_add_wait_incremental_value_v1_2_spec_20260809.json` (`c26eb2d8...`)
- Source-grid correction: `multiscale_ema_add_wait_incremental_value_v1_2_provider_source_grid_correction_20260809.json` (`8c3f9e5f...`)
- OOF execution amendment: `multiscale_ema_add_wait_incremental_value_v1_2_source_grid_oof_execution_amendment_v3_20260809.json` (`ab1b9240...`)
- 2025 encoder: `ema_encoder_2025_provider_source_grid.npz` (`369ee07e...`)
- Native OOF predictions: `native_oof_predictions_source_grid_v3.parquet` (`92a7bc21...`)
- Authoritative report: `report_source_grid_v3.json` (`8a6f8685...`)
