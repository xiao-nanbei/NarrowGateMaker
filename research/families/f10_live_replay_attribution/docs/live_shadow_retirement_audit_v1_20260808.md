# Live Shadow Retirement Audit v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Decision

Retire the continuous cross-venue fair-center log, the quote-time inventory threshold what-if stream, and logging for the already-closed depth candidates. Keep the promoted depth imbalance asymmetry active. This is an observability release only: it does not change quote prices, order exposure, cooldown, requote, inventory limits, model inference, q90 permission, or P3.

## Fair-Center Live/Replay Parity

The bounded live prefix contains 82,565 fair-center rows. Valid fair state was available for 81,563 rows (98.7864%), and 80,968 rows used all three external venues. Median absolute requested/effective movement was 13 ticks; p90 was 31 ticks. The candidate therefore had substantial coordinate leverage and was not a logging-only numerical no-op.

`quote_snapshot_integrity.csv` began later than the fair-center stream. Within its common time support, 60,977 fair rows matched an immutable live quote snapshot within 500 ms. Re-running the frozen projection on that exact state gave:

| Check | Result |
|---|---:|
| Matched live decisions | 60,977 |
| Requested shift tick mismatches | 0 |
| Effective shift tick mismatches | 0 |
| Candidate bid/ask mismatches | 0 |
| GTX clamp mismatches | 0 |
| Pair-spread preservation mismatches | 0 |
| Exact replay parity | 100.0000% |

Sixteen matched rows were GTX-clamped and all sixteen reproduced exactly. The remaining 21,588 fair rows predate the snapshot-integrity journal and are unmatched support, not observed mismatches. Fair-center projection preserves the already-capped baseline spread and has no independent spread-cap action.

## Economic Replay

The existing frozen 40-day randomized full-path replay is the relevant counterfactual economic test; the live shadow itself never mutated orders and therefore cannot supply an observed candidate PnL path.

Grade-A primary results were:

| Metric | Result |
|---|---:|
| Action-change rate | 100.00% |
| Fill retention | 97.00% |
| Activity retention | 92.35% |
| Reward uplift | +0.001446 USDC/assignment |
| Reward 95% interval | [-0.002732, +0.006053] |
| HT policy value | +0.6351 USDC/day |
| HT policy-value 95% interval | [-1.0143, +2.3994] USDC/day |
| BUY reward uplift | +0.003324 USDC/assignment |
| SELL reward uplift | -0.000429 USDC/assignment |
| Leave-Bybit-out policy value | -0.1986 USDC/day |

The mechanism genuinely changed quote and fill paths and did not obtain its point estimate through broad fill collapse. It nevertheless failed the frozen value lower bound and positive-day gates, while leave-Bybit-out reversed the economic direction. The supported conclusion is therefore: live/replay execution semantics are consistent, but the symmetric fair-center shift is not a transport-stable improvement over the baseline.

## Other Retirements

The inventory what-if file contained 505,838 rows across 1,349 active campaigns. Only 16,451 rows represented an event/state change; 489,387 rows (96.75%) repeated quote-time state. Campaign starts/ends, fills, and order terminal outcomes remain recorded in `maker.log`, `trades.csv`, and `order_outcomes.csv`.

The retained maker logs contained 11,704 `DEPTH_SHADOW` records. The promoted imbalance asymmetry was active on all of them, while microprice-kappa and depth-tox were active on zero. Turning off `depth_execution.shadow_enabled` therefore removes closed-candidate diagnostics without disabling the active imbalance quote input.

## Admission And Deployment

The read-only live prefix was admitted to:

`${NARROWGATE_DATA_ROOT}/reports/live_shadow_retirement_v1_20260808`

The archive contains 14 files and 1,029,344,252 bytes. All CSV records passed schema-width and final-newline validation; all post-transfer hashes matched. Archive manifest SHA256: `79f7e90bb932ed5c32ce011a462a25da8a1ac02c4a1b03ac9f9ac0c5d58df38a`.

The minimal runtime release changed only:

- `strategy.cross_venue_fair_price_shadow_enabled: true -> false`
- `depth_execution.shadow_enabled: true -> false`
- `logging.inventory_campaign_shadow_enabled: false` plus its path
- the inventory logger resolver, which returns no path when disabled

EC2 preflight passed with causal-v12 ML ON, q90 shadow ON/action OFF, and imbalance asymmetry ON. The controlled restart completed at 2026-08-08 06:50:10 UTC with PID 1981611. Over the post-restart verification window, both retired CSV byte sizes were unchanged, post-restart `DEPTH_SHADOW` count was zero, and the quote loop continued without a sustained severe error.

## Governance

- `orders_mutated_by_audit=false`
- `economic_outcomes_read_from_live_shadow=false`
- `frozen_full_path_economic_result_referenced=true`
- `fair_center_action_live_authorized=false`
- `fair_center_shadow_logging_enabled=false`
- `inventory_quote_time_shadow_logging_enabled=false`
- `closed_depth_candidate_shadow_logging_enabled=false`
- `active_depth_imbalance_asymmetry_enabled=true`
- `operational_quote_policy_changed=false`
- `historical_frozen_report_modified=false`

The historical F09 report's statement that the fair-center shadow was not active on EC2 is superseded only for operational provenance by this audit; its frozen experiment identity and economic result remain unchanged.
