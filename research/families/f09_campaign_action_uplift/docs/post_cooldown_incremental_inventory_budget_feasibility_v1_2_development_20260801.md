# Post-Cooldown Incremental Inventory Budget Feasibility v1.2

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Status

Development mechanics complete. The exact action surface is closed with:

```text
decision=close_inventory_budget_mechanics_insufficient_sell_grid_resolution
```

No reward, PnL, markout, Validation, sealed holdout, randomized action, or live outcome was read or authorized.

## Question

This identity tested a new intervention distinct from cooldown timing: after the first normal same-side cooldown release in a non-flat campaign, limit the additional exposure-increasing fill-unit budget while leaving every reducing quote and hard safety gate unchanged.

SELL was the primary side and BUY was a separate negative control. The BUY q90 action was disabled identically in control and candidate paths because its live transport contract remains unresolved. The result is therefore a q90-OFF reference mechanics result, not equivalence to the current live baseline.

## Frozen Evidence

- Development: 40 exact UTC days.
- Primary: 24 explicit Grade-A days.
- Sensitivity: 16 explicit Grade-B days; they did not enter the primary gate.
- Queue/path: native snapshot/delta full-path Python replay.
- Market identity: 1,222 files, 11,625,657,192 bytes, canonical SHA256 `27787b4aa129563c3d5bf173ef7705e44a511462de0c5989a8b1db7e8b457227`.
- Contract tests: 37 passed.
- q90 evaluations and actions: zero in every path.
- Budget conservation failures and one-order overshoots: zero.
- q90-OFF infinity-versus-disabled-budget equivalence passed on both frozen Grade-A check days and the Grade-B sensitivity check day.

## Primary Result

The Grade-A unlimited control generated the following outcome-blind fill-unit distribution:

| Side | Supported lineages | p25 | p50 | p75 | p90 | More than 1 unit | More than 2 units |
|---|---:|---:|---:|---:|---:|---:|---:|
| BUY | 1,811 | 0 | 0 | 1 | 1 | 4.64% | 0.22% |
| SELL | 1,753 | 0 | 0 | 1 | 1 | 3.31% | 0.34% |

After the frozen whole-unit rounding and zero-budget exclusion, both sides produced only:

```text
candidate_grid={BUY: [1], SELL: [1]}
```

The preregistered contract required at least two distinct nonzero candidates. Neither side therefore had sufficient inventory-budget resolution.

The complete B=1 paths supplied a second, independent reason not to register an action:

| Panel | Side | Episodes | Final quote action change | Fill retention | Activity retention | Unsupported | Mechanics pass |
|---|---|---:|---:|---:|---:|---:|---|
| Grade A primary | BUY | 1,745 | 86.36% | 98.98% | 97.07% | 0% | false |
| Grade A primary | SELL | 1,659 | 86.14% | 98.89% | 98.05% | 0% | false |
| Grade B sensitivity | BUY | 1,238 | 86.51% | 98.19% | 97.10% | 0% | false |
| Grade B sensitivity | SELL | 973 | 87.46% | 98.73% | 97.67% | 0% | false |

The action broadly changed submitted/replaced order paths, exceeding the frozen 50% maximum action-change rate, yet removed only about 1.1% of SELL fills. This is not a no-op at the order layer. It is a poor economic lever at the fill layer: repeated future add attempts are blocked, but very few of those attempts would have become additional fills.

## Interpretation

This is new action-mechanics evidence, not a repetition of the earlier 240-hour loss attribution. The earlier result established that multi-level SHORT campaigns carried most of the observed terminal loss. This experiment shows that a budget assigned only at the post-cooldown release surface does not expose a useful 1/2/3-unit control frontier under the frozen lineage semantics.

The result does not prove that cumulative SHORT inventory is harmless and does not close all inventory controls. It closes this exact q90-OFF, post-cooldown-lineage budget action before any economic outcome is read. No randomized PnL identity should be created from it.

## Data Boundary

The 40-day denominator is historical to this F09 identity, not the current project-wide maximum. The source-aware research universe now contains:

- 45 native-formal 2026 lifecycle days for exact queue/action replay;
- 67 provider-normalized target days beginning in 2025 for causal features, model training, calculation, and sensitivity replay.

The 2025 Tardis inventory contains 93 provider candidates before the mandatory target plus D-1 warmup intersection, which leaves 67 admitted target days. It does not contain Binance native `U/u/pu` sequence identity and has `exact_queue_policy_eligible=false`. Those days cannot be silently mixed into this exact-queue action gate. A later identity must freeze native and provider denominators separately.

## Artifacts

Authoritative output:

```text
${NARROWGATE_DATA_ROOT}/reports/
  post_cooldown_incremental_inventory_budget_feasibility_v1_2_20260801/
  development/
```

Key hashes:

- Spec: `c1f1e7d1ea873a5b1a0896a3de3616c4453499ba045c76199e30bc0833e356fb`
- `report.json`: `1c0e438943bcf0e8352b12c5557b33570320210f0c8bab4e853d219f8a181e87`
- `manifest.json`: `74d745c4ca7485f08202c7b8df8f080ef5a49d5a3f7bdad087f720caef4256e5`
- `side_budget_mechanics.csv`: `0b0b1b19b07e6e4c41b923fc18f27fe8e0cfb0344dbb558a2d20560251a6fe0f`
