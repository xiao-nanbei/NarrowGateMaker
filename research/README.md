# 407-day research system

[English](README.md) | [简体中文](README.zh-CN.md)

Last materially modified: 2026-10-17

Last materially synchronized: 2026-10-17

Current research entries: [F01 mechanism closure](families/f01_fixed_parameter_racing/README.md) and [F04 full-support direction training](families/f04_external_market_alpha/README.md). The original nine-arm Defense/Urgency study remains accepted under its original identity. The later mechanism study was owner-stopped with partial independent-account coverage and complete continuous Final107 results; neither ablation is promoted. `B0_DEFENSE_URGENCY_OFF_20261011` remains the reference. The small-support M0/M1 Final extension was owner-stopped and archived with eight accepted M0 accounts, nine stopped accounts and 91 unsubmitted paths. The five successor candidates have each completed 54 independent Final accounts and minimal archival closure. All five lost more than the same-contract B0 in total and on both turnover-normalized measures; none is promoted. See the [completed comparison](families/f03_causal_13_head/README.md#completed-five-candidate-final107-comparison) and [aggregate record](final107_comparison.json). Earlier response, F07 U1 and hazard findings are not rewritten. Loss reduction is not profitability or fresh-holdout validation.

Start with the [unified implementation plan](RECOMPUTE_407.md) and [12-package work list](recompute_407.json). Recompute scientific questions rather than reproducing successive old versions; historical closed/exhausted/promotion states do not block new studies. Previous-use, real input limits and independent-account boundaries remain binding. Backtest-count quotas were removed under the owner's October 17 instruction: required evaluations are not refused because an earlier numerical allowance was exhausted. Resource protections and identity/duplicate checks remain.

The new-source [F03](families/f03_causal_13_head/README.md) completed all 858 strategy-shards, including the later approved ML-OFF. Final net PnL was −189.212148 USDC for H=inf versus −269.042233 USDC for ML-OFF; both were negative. Previous-use and interim-outcome inspection remain disclosed. Other packages' integration/execution states are explicit in the work list. Full-system recomputation results are not published; interface tests do not mean purchased-data research has completed.

## Discover open questions

Follow `families[].research_questions` in the [family registry](registry.json) and select `status=pending_validation`. The current inventory contains four questions: F02 scalar-adapter; F05 multiscale cooldown, stale quotes/adverse fills, and markout versus holding lifecycle. A family without this field is not yet inventoried, not known to have no questions; this is not a complete historical audit.

When an old claim is encountered, assign it to the same existing family record with its source, evidence availability, methodological gaps and next validation. An old report establishes only what was recorded under that identity, not effectiveness on current data or strategy. Mark unverifiable provenance and methods requiring reconstruction explicitly; lack of evidence is not a negative result. Link the actual new report before resolving a question, without rewriting frozen history or automatically starting an experiment.

## Current families

These experiments belong to existing families according to the decision layer they change; no new family is introduced. Main reports explain methods and conclusions without distributing private configurations, account ledgers or run material. Each experiment retains its own B0 and execution scenario rather than forming one continuous return series. A completed subexperiment does not complete its whole family.

| Experiment | Owner and comparison | Supported conclusion |
| --- | --- | --- |
| Linked coefficients (historical gamma axis) plus asymmetric offset | [F01](families/f01_fixed_parameter_racing/README.md): crossed quote-controller axes | Single-account loss reduction coexisted with inventory risk and interaction; later reference selection does not prove optimality |
| E/C | [F05](families/f05_fill_quality_quote_ev/README.md): POST/WAIT, KEEP/CANCEL and random controls; C cross-references F07 | E/EC reduced loss but remained negative; C's increment was inconsistent, turnover differed and stable selectivity was not established |
| A00/A11/A10/A01 | [F01](families/f01_fixed_parameter_racing/README.md): ordinary outward/inward replacement thresholds | Uniform reduction was nearly unchanged and slightly worse; outward-only gains were concentrated with higher peak inventory; inward-only was worse |
| Target-quote variance-rate threshold | [F01](families/f01_fixed_parameter_racing/README.md): one dynamic D versus typical fixed-scale S | Both lost more than B0 across two accounts; no stable extra value of dynamics over the fixed scale was established |
| U0/U1 | [F07](families/f07_active_order_continuation/README.md): original versus event-driven evaluation, unchanged order thresholds | Aggregate loss fell across eight accounts, but per-turnover performance worsened and request/inventory exposure increased; not stable profitability or proven better selection |

E/C, threshold candidates and U1 are not automatically promoted to B0 or live. The table below retains the single maintained entry for every family.

| Family | Current entry |
|---|---|
| F01 | [research.families.f01_fixed_parameter_racing](families/f01_fixed_parameter_racing/README.md) |
| F02 | [research.families.f02_empirical_p3_touch](families/f02_empirical_p3_touch/README.md) |
| F03 | [research.families.f03_causal_13_head](families/f03_causal_13_head/README.md) |
| F04 | [research.families.f04_external_market_alpha](families/f04_external_market_alpha/README.md) |
| F05 | [research.families.f05_fill_quality_quote_ev](families/f05_fill_quality_quote_ev/README.md) |
| F06 | [research.families.f06_placement_fill_cif](families/f06_placement_fill_cif/README.md) |
| F07 | [research.families.f07_active_order_continuation](families/f07_active_order_continuation/README.md) |
| F08 | [research.families.f08_side_taker_lifecycle](families/f08_side_taker_lifecycle/README.md) |
| F09 | [research.families.f09_inventory_lifecycle_action_uplift](families/f09_inventory_lifecycle_action_uplift/README.md) |
| F10 | [research.families.f10_live_replay_attribution](families/f10_live_replay_attribution/README.md) |
| SYS | [research.system_engineering](system_engineering/README.md) |

## Implementation and evidence

Experiment creation and closure follow the [minimal archive standard](governance/EXPERIMENT_ARCHIVE.md): five retained evidence roles, shared dependencies instead of repeated full copies, explicit cleanup gates, and separate economic/storage status. Legacy frozen paths are indexed in place; inventory alone does not certify minimal retention.

The [input guide](INPUT_MIGRATION.md) distinguishes existing APIs from remaining limitations; the [registry](registry.json) identifies packages. Reuse shared data, replay, strategy and governance implementations rather than duplicate parsers or simulators. Parameter/action changes require new strategy paths; attribution and plotting reuse the same valid trajectory.

The [public/private layout](../docs/public_private_documentation_contract.md#evidence-owners-and-local-catalogs) keeps purchased data, models and results out of source publication. One private Git bundle preserves the complete pre-rewrite tree. Historical docs temporarily remain read-only pending question-level consolidation; their old numbers and statuses no longer lead navigation. New questions inherit neither old rankings nor fresh-holdout claims for already-used final periods.
