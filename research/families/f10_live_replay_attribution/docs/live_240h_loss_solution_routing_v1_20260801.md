# Live 240h Loss Solution Routing v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Status: `diagnostic_only_solution_routing`. This report separates recent live loss attribution from the independent BUY-q90 transport blocker. It does not estimate an action effect, register an action, read Validation/holdout, or authorize a live change.

## Evidence Identity

The observational window is 2026-07-20 21:45:53.653 UTC through 2026-07-30 21:45:53.653 UTC. The following source artifacts are persisted on `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}`:

| Artifact | SHA256 |
|---|---|
| `live_240h_analysis.json` | `25e0f7f17a16c184b26220709be580c77527add5c122bfbb8aa83d2c6842b129` |
| `live_240h_closed_campaigns.csv` | `4380213b0704dc7283c322808a8c6a21680524946573931b5ce36a29a1654de3` |
| `live_240h_enriched_fills.parquet` | `83554ec482ac1ef7eb0a648f8264609dcd37e7a747f6408111f55337b16d487d` |

All three live under `${NARROWGATE_DATA_ROOT}/reports/live_240h_loss_solution_analysis_v1_20260801/inputs/`. The rebuilt first 120 hours match the earlier frozen diagnostic on all 2,622 fill roles and all 983 closed campaigns; campaign PnL agrees to floating-point precision.

## What The Window Identifies

The 5,346 usable approximate 10-second observations have mean maker-signed value of -0.441 bps per fill. Entry edge remains positive at +2.597 bps, but the following market move is -3.038 bps. This is broad adverse-selection evidence, not a side-specific action rule: BUY/SELL opener, add, and reducing slices all have negative mean 10-second value.

Terminal loss is much more concentrated than the fill markout:

| Campaign slice | Campaigns | Terminal PnL (USDC) |
|---|---:|---:|
| All clean closed | 1,951 | -13.5712 |
| Single-level | 1,576 | -0.0786 |
| Multi-level | 375 | -13.4926 |
| Single-level SHORT | 1,011 | +0.0921 |
| Multi-level SHORT | 257 | -11.1471 |

Multi-level SHORT campaigns are 20.27% of SHORT campaigns and contribute 82.14% of the total net negative aggregate. Their aggregate PnL is negative in every one of the 11 UTC date slices, including the two partial boundary days. The exact SHORT depth profile is:

| Maximum units | Campaigns | Terminal PnL (USDC) | Mean/campaign (USDC) |
|---:|---:|---:|---:|
| 1 | 1,011 | +0.0921 | +0.000091 |
| 2 | 185 | -4.0380 | -0.021827 |
| 3 | 52 | -3.6132 | -0.069485 |
| 4 | 13 | -1.1941 | -0.091854 |
| 5 | 4 | -0.3304 | -0.082600 |
| 6 | 1 | -0.6371 | -0.637100 |
| 7 | 2 | -1.3343 | -0.667150 |

This is strong mechanism localization, but depth is endogenous: campaigns both receive additional fills and reach larger inventory while the market is moving adversely. The table does not identify the counterfactual PnL from blocking an add.

## Separate Q90 Boundary

The 23 terminal-order cases in [`buy_q90_terminal_hold_riskset_audit_v1`](buy_q90_terminal_hold_riskset_audit_v1_development_20260801.md) are an F10 transport/state-machine blocker. They are not included as an explanation for the -13.5712 USDC terminal aggregate.

That audit nevertheless changes the executable baseline boundary: the current q90 implementation has no supported post-cancel recovery estimand, so a new inventory-budget replay must not silently inherit q90 ON. A future mechanics identity must choose one of two ex-ante contracts:

1. freeze q90 OFF in both arms; or
2. bind a separately passed post-cancel recovery contract and original q90 parity gates.

Combining a q90 repair with an inventory-budget candidate would change two interventions at once and is forbidden. This report does not itself disable q90 on live.

## Action Routing

The next foreground question belongs to F09 and concerns cumulative SHORT exposure, not SELL opener selection:

\[
\text{normal cooldown release}
+
\text{maximum additional exposure-increasing units }B.
\]

The existing non-frozen [`post_cooldown_incremental_inventory_budget_feasibility_v1`](../../f09_campaign_action_uplift/docs/post_cooldown_incremental_inventory_budget_feasibility_v1_design.md) design is the correct mechanics family. Whole-unit budgets `B=1/2/3` create a bounded middle region between the closed stop-add-until-flat action and an unlimited control. With a one-unit opener, `B=1` can cap a supported lineage at two units, `B=2` at three units, and `B=3` at four units. `B=0` remains excluded because it recreates the previously closed participation-shutdown extreme.

The idealized observed net-deficit scale is useful only for feasibility planning:

- multi-level SHORT loss per affected campaign: 0.04337 USDC;
- recovering the full observed multi-level SHORT net deficit per SHORT campaign: 0.00879 USDC;
- the same accounting scale per all campaign: 0.00571 USDC.

These are not causal upper bounds. The latter two are nevertheless below the old contextual SELL MDE of 0.01546 USDC, so an all-campaign denominator is unlikely to provide useful resolution. The ex-ante denominator must be the first supported post-opener SELL-add/post-cooldown release surface. It must not select the 257 realized multi-level losers after observing their paths. A new MDE must be computed from that at-risk denominator before any reward is read.

## Required Sequence

1. Create a new executable mechanics-only identity; do not reuse the frozen v1/v1.1 implementation identity.
2. Freeze q90 OFF in both arms unless the separate recovery contract has already passed.
3. Derive `B=1/2/3` support outcome-blind from unlimited-control mechanics and require at least two distinct candidates.
4. Check exact unit conservation, action-change rate 5%-50%, fill retention at least 85%, activity retention at least 75%, unsupported mass at most 5%, reducing-side invariance, Python/C++ parity, and an at-risk design MDE.
5. Only if mechanics and economic resolution are adequate may F09 register a separate 0.5/0.5 lineage-randomized action using assignment-to-campaign- terminal direct USDC value.

The following routes remain closed or unsupported by this evidence:

- suppressing all SELL openers: single-level SHORT campaigns are approximately flat in aggregate;
- another cooldown clock or blocked-cycle threshold: the tested temporal- permission subspace is exhausted;
- order-age TTL/cancel tuning from this correlation alone: F07 keep/cancel and placement-distance evidence did not establish action value;
- q90 threshold/recovery retuning: its transport state machine must be repaired under a separate identity first;
- direct live deployment: no paired or randomized economic result exists.

## Decision

`route_to_short_incremental_inventory_budget_mechanics_after_q90_isolation`

All prediction, action, Validation, holdout, shadow, rollback, and live permissions remain false.
