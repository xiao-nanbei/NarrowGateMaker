# Volatility-Time Add-Rearm Randomized Replay v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

## Decision

`close_variance_time_add_rearm_action_on_development`

The variance-time clock remains a valid and reproducible mechanics component, but replacing the fixed `85s * consecutive_same_side_fill_units` add-rearm clock does not have a positive day-clustered reward lower bound for either side. Validation and sealed holdout remain unread. No action, shadow, AWS receive-time, or live authority is created.

## Frozen identity

- Randomization unit: one same-side fill-cooldown lineage.
- Start: first eligible exposure-increasing fill.
- End: opposite-side fill, explicit reset, or daily fresh-start censor.
- Control: fixed wall time, `85s * consecutive_same_side_fill_units`.
- Candidate: frozen variance-time budget and liveness bounds from v2.1.
- Propensity: exact `0.5 / 0.5`, assigned before the downstream path.
- Assignment is unchanged within a lineage; later same-side fills update the budget without rerandomization.
- Replay: Python-authoritative full path with strict native snapshot/delta queue and C++ BUY-q90 kernel lockstep.
- Full C++ tick-replay authority remains false.
- Score profile: `action_execution_v1`, SHA256 `f8ffd88cb86dc581f931f865b75f2d6244f1bb26a817abc4691e68a110c2ab8c`.

The authoritative F09 Development denominator is 40 days from 2026-04-17 through 2026-06-26. It contains 24 Grade-A and 16 Grade-B days; all 40 pass native-sequence and normalized-formal eligibility. The 50-day F06 placement lifecycle denominator is a different experiment identity and was not used.

## Mechanics and support

| Check | Result |
|---|---:|
| Development days | 40 / 40 |
| Lineages | 17,460 |
| BUY / SELL rows | 9,140 / 8,320 |
| Behavior propensity | 0.5 on every row |
| Both arms present in every day x side | 80 / 80 cells |
| Unsupported rows | 6 / 17,460 (0.034%) |
| Reward identity max absolute error | `1.11e-16` USDC |
| Native source gaps / invalid sequences | 0 / 0 |
| BUY-q90 C++ mismatches | 0 |
| Historical q90 fill-before-ACK branch | 0 |
| Sync-censored days | 0 |

The candidate was not a no-op and did not collapse participation:

| Side | Candidate assignment | Actual final-action change | 95% UTC-day interval | Fill retention |
|---|---:|---:|---:|---:|
| BUY | 50.03% | 37.02% | [31.93%, 41.91%] | 104.80% |
| SELL | 50.66% | 24.41% | [20.77%, 28.10%] | 99.41% |

Support, overlap, propensity, unsupported-mass, action-mechanics, and fill- retention gates pass for both sides. The family closes on economic and lifecycle gates, not because the intervention lacked mechanical strength.

## Development outcomes

All values below are candidate minus control and use complete UTC-day cluster bootstrap intervals.

| Side | Metric | Point | 95% UTC-day interval | Daily positive |
|---|---|---:|---:|---:|
| BUY | Primary lineage reward, USDC | +0.000738 | [-0.002000, +0.003577] | 47.5% |
| BUY | Campaign terminal value, USDC | +0.007607 | [+0.003349, +0.012043] | 65.0% |
| BUY | Inventory-time avoidance, BTC*s | +0.009843 | [-0.001394, +0.023596] | 57.5% |
| SELL | Primary lineage reward, USDC | +0.001875 | [-0.002303, +0.006242] | 65.0% |
| SELL | Campaign terminal value, USDC | -0.000003 | [-0.010238, +0.008853] | 57.5% |
| SELL | Inventory-time avoidance, BTC*s | +0.009464 | [-0.002802, +0.024931] | 55.0% |

BUY's terminal campaign metric has a positive lower bound, but the primary reward, q10 shortfall, campaign MAE, repair, censoring, and inventory-time contracts do not jointly pass. A secondary terminal metric cannot override those hard gates.

SELL has a positive reward point estimate and 65% daily-positive rate, but its reward interval crosses zero. Terminal value, negative-tail, q10, campaign MAE, repair-time, and inventory-time evidence also fail their frozen gates.

Consequently both scorecards have `ranking_score=null` and `promotion_status=development_failed_family_closed`. No pooled result may rescue either side.

## Reward boundary

The authoritative row reward is direct equity change from the lineage decision to its terminal event. The audit identity is:

`reward = maker-signed 30s fill value - campaign-path accounting residual - explicit queue/reset cost`.

The campaign-path term is an accounting residual, not a separately identified causal cost. Explicit queue/reset cost is zero because no independent USDC queue-priority price has been identified; queue resets and their resulting missed or added fills are nevertheless regenerated in the authoritative path. Maker-signed fill value starts at execution price, so half-spread is not added again. Campaign terminal MTM, MAE, repair, duration, and inventory time remain secondary consistency and tail gates rather than duplicate reward terms.

## Limitations and errata

- The run is daily fresh-start research, not continuous-live state carry.
- AWS receive-time transport remains unsupported and was not needed to close the offline Development family.
- C++ authority covers the native-book BUY-q90 kernel lockstep, not the full tick replay.
- No historical q90 fill-before-cancel-ACK branch occurred. The formal replay remained fail-fast on its first occurrence; synthetic lifecycle tests are not promoted to historical coverage.
- The Development q10 threshold was calculated from pooled control-side rows. BUY and SELL estimation and scorecards are otherwise separate. This pooled nuisance threshold must not be reused as a side-specific tail artifact. It cannot alter the closure because both sides independently fail primary reward and additional hard gates.

## Authority

- `validation_read=false`
- `sealed_holdout_read=false`
- `action_experiment_authorized=false`
- `live_deployment_authorized=false`
- `aws_receive_time_transport_supported=false`
- `full_cpp_tick_replay_authority=false`

The variance budget, liveness bounds, propensity, reward, and score profile must not be retuned on these outcomes. Reopening requires a genuinely new action mechanism and a new pre-registered identity, not another variance-time budget search.
