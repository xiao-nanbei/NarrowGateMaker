# F03 One-Second Dual-Overlay Full-Path A/B Execution Amendment v1

Last materially modified: 2026-08-05

Status: implemented locally, pre-outcome, execution blocked.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Estimand correction

The predecessor replay ABI compares ML-OFF with the one-second candidate. That is not the frozen cadence estimand and may not be used as the formal control. The successor compares:

\[
\text{current v9 causal-v12 10s ML-ON}
\quad\text{vs}\quad
\text{causal-v12 1s 13-head ML-ON}.
\]

Both arms keep `ml_enabled=true`, q90 action OFF, and BUY fill-selection OFF. They share the same model-free market context, queue inputs, configuration, P3, latency path, and replay code. The control receives the hash-bound v9 `window.ml_data`; the candidate receives the admitted one-second schedule. Calling `disable_ml_params` or passing null control `ml_data` is forbidden.

The candidate artifact must contain and hash-bind all 13 heads. The unchanged quote ABI consumes the same five policy outputs as v9 (`dir_10s`, `vol_10s`, `ret_10s`, `tox_bid_10s`, and `tox_ask_10s`). The other admitted heads are not silently claimed as economically exercised by this A/B.

## Execution module

The new runner prepares an outcome-blind 40-day plan, validates every physical input and runtime hash, publishes each UTC day atomically, and resumes only an identical admitted day. It aggregates closed-campaign value, terminal MTM, fills, side maker value, q10/CVaR, MAE, maximum inventory, inventory time, and multi-level LONG/SHORT loss. A metric absent from the authoritative replay is a named blocker; it is never reconstructed by assumption.

The raw `action_alpha_v2` result is preserved. The owner route may only apply the precommitted fill-retention interval `[0.80, 1.20]`; every original hard failure remains visible. Daily fresh-start output cannot satisfy the required 71-day continuous confirmation by itself.

Large execution artifacts are restricted to the `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` cache. Neither Validation nor sealed holdout can be attached to this runner.

## Control audit blocker

A read-only audit loaded the 40 pointer-bound v12 control overlays without loading market outcomes. The formal control grid is 8,640 timestamps from UTC `00:00:00` through `23:59:50` at ten-second cadence. Only 28/40 overlays satisfy that grid.

Eight days lack the initial control bucket, five lack the terminal bucket, and `2026-06-17` additionally contains a noncanonical D-1 row. `2026-05-13` is in both missing-boundary sets. No future timestamp was found. The runner therefore fails closed instead of padding, forward-filling, or splicing these overlays. The missing control overlays must be rebuilt from the v9 pointer-bound v12 bundle against the same hash-bound market contexts.

The formal one-second candidate bundle and 40-day candidate overlay panel have also not been provided. No execution plan was admitted and no Development PnL, Validation, holdout, action, or live permission was read or granted.

The machine-readable amendment is `causal_v12_1s_dual_overlay_full_path_ml_ab_execution_amendment_v1_20260805.json`.
