# Repository Filesystem-LRU Code and Document Audit

Initial audit date: 2026-07-27

Snapshot date: 2026-08-04

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](public_private_documentation_contract.md).

Status: Active maintenance queue. Filesystem age determines review order, never deletion by itself.

## Current conclusion

The repository has changed materially since the original 2026-07-27 audit. Research code now lives under the canonical v2 layout:

- `research/families/f01_*` through `research/families/f10_*`;
- `research/shared/{data_identity,replay_lifecycle,strategy_semantics,experiment_governance}`;
- `research/system_engineering`;
- `research/governance`.

The stable CSV entrypoint [`repository_lru_governance_queue_20260727.csv`](repository_lru_governance_queue_20260727.csv) has therefore been regenerated from the current filesystem rather than retaining the obsolete 468-row `research_*` snapshot. The filename keeps the date of the initial audit; the contents and this document state the current snapshot date.

This audit does not delete, relocate, archive, or rewrite frozen evidence. The working tree contains substantial ongoing F02/F05/F09/F10, baseline, Feature DAG, replay and documentation work. Those edits are preserved and are treated as current filesystem state, not as cleanup authorization.

The owner-authorized 2026-08-10 follow-up deleted only two unbound one-time notices: `docs/PRE_CLEANUP_MODEL_NOTICE_20260701.md` and `docs/private/public_redaction_artifacts_20260706.local.md`. The governance queue was updated in the same change. No frozen result, errata, incident record, or executable research identity was deleted.

## LRU method

Filesystem `mtime` is the only ordering clock. Git commit time, commit count and Git history are not used for ordering. The repository intentionally keeps one consolidated Git record to limit repository bloat. Git status is used only as a safety check so unrelated local work is not overwritten.

Age is interpreted together with:

- `Last materially modified` tags;
- imports, CLI/Makefile entrypoints and tests;
- configuration, baseline and model-artifact identities;
- research-family registry and migration manifests;
- frozen specification/report hashes;
- current-versus-historical wording and superseding conclusions.

A zero static-reference count is only a review trigger. Tests, dynamic entrypoints, plugin/CLI files, frozen evidence and operational scripts often have no literal in-repository consumer. They must not be deleted on that basis.

## Source scope and exclusions

The current queue contains maintained source, tests, Markdown, JSON contracts and engineering configuration. It excludes generated or separately governed material:

- `.git`, `.venv`, build output and Python/test/lint caches;
- `models/saved*` model bundles;
- `logs/`, `results/`, generated data-quality output and test fixtures;
- private configuration/evidence under `docs/private`;
- generated latency profiles under `live/profiles`;
- archived migration tarballs;
- MarketData on `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` and replay/window caches.

The static-reference count is conservative. It recognizes canonical paths, unique filenames and Python module names, but it is not a runtime import graph and cannot see reflection, shell-computed paths or external consumers.

## 2026-08-04 snapshot

| Class | Rows |
|---|---:|
| Code | 383 |
| Tests | 280 |
| Markdown documents | 212 |
| JSON specifications/registries | 185 |
| Engineering configuration | 12 |
| **Total** | **1,072** |

Research owns 579 rows under the canonical `research/` tree. The larger count relative to 2026-07-27 comes from the v2 family migration plus the placement, P3, lifecycle, continuous-accounting, action-governance and operational baseline work created after the original audit; it is not raw-market-data or cache growth.

| Priority | Rows | Meaning |
|---|---:|---|
| P0 | 23 | Current runtime, baseline, registry, Feature DAG or promotion authority; synchronize, never age-delete |
| P1 | 8 | Exact closed-action evaluator/test deletion group awaiting owner approval |
| P2 | 62 | Old current code/tests or unconfirmed semantics requiring near-term review |
| P3 | 316 | Frozen evidence or middle-LRU review |
| P4 | 663 | Recent work, stable package markers or previously governed current files |

There are 70 maintained Markdown files without a material-modification tag and 305 rows with zero detected static references. Neither set was bulk-touched: adding tags without substantive review would destroy the LRU signal, and zero references do not prove obsolescence.

## Current operational and research authority

The original audit's causal-v7/ML-OFF and pre-v7 baseline wording is historical. The current registry is `research/registry.json`; the operational pointer was last reconciled on 2026-08-12.

### Operational baseline v11

The current pointer resolves to `research/families/f10_live_replay_attribution/docs/operational_baseline_identity_20260812_v11.json`. The current identities are:

- baseline identity SHA256: `edb0de785b698c70bce2b2cdb169b78c89a5f6076015b7ab8a313557567dead4`;
- pointer SHA256: `3dcbcdb8cd994a2641a6a5920e53647338f7e55d2c23e3ef21738523de2277dd`;
- operational config SHA256: `800f4c025663ce6b54cfcf16d02ce510ccaf52545332ca4c19b1fbdf37f0cf85`.

The declared baseline keeps causal-v12 feature semantics v6, empirical P3 v2, q90 shadow ON/action OFF, BUY fill-selection shadow/action OFF, the v10 observation retirements, and active depth imbalance asymmetry. v11 adds only the SELL Boolean cooldown policy under a permanent `owner_risk_accepted_promotion` label. Its historical hard gates remain failed; BUY and reducing quotes retain predecessor behavior.

### F02 P3

The operational P3 v2 artifact remains unchanged. The source-aware static pool was rejected, while `p3_touch_volatility_conditioned_v4_1` retained historical Development prediction support under an explicitly outcome-informed owner coverage override. The later scalar-compression quote adapter was economically harmful and is closed. Conditional P3 may measure side-specific exact-distance aggressive-reach leverage inside a separately frozen action, but it may not export a dynamic scalar `kappa`/`delta_star` or generate a quote by itself.

### F09 action research

Inventory suppression, passive/aggressive repair and the specifically tested fixed temporal-permission actions did not establish positive terminal value. The later role-safe BER add-only candidate also closed on Development. F05's research-supported Boolean route failed, but its frozen SELL owner policy later produced favorable 50-day and 71-day point estimates and was deployed through explicit owner risk acceptance. This does not create F09 research authority or rewrite any failed gate.

### F10 q90 and live attribution

q90 production action remains suspended. The authoritative v1.6 40-day mechanics/CIF chain completed over 665,831 exact-native lifecycle spells with zero Python/C++ transition mismatch and zero post-terminal hazard/queue reuse. The first live/AWS transport failed closed on duplicate activation and missing exact feature-ready evidence. The producer fix is local, but a new fully bound epoch must pass the unchanged transport gates before economic outcomes can be read or the q90 threshold can be reconsidered.

### BABEL / F04

The first-add external M0/M1 identity has 18 distinct valid receive-time UTC days against a frozen requirement of 30. This blocks only that identity, not unrelated foreground research. The original adverse-edge mechanics action rate was 0.334%, below its 5% gate. The exact-opener v2.2 successor is disabled because its historical runtime hash is stale; first-add P1 and opener P2 must not be merged.

## Research-layout migration governance

The original document described the intermediate root-level `research_XX` layout. That layout is no longer canonical.

- layout v1 migrated 198 paths into the first family layout;
- layout v2 migrated 250 paths into `research/families`, `research/shared`, `research/system_engineering` and `research/governance`;
- active duplicate sources and compatibility symlinks are forbidden;
- `research/governance/paths.py` resolves historical paths through immutable manifests;
- v1/v2 archives preserve exact historical bytes for frozen reproduction.

Frozen documents may keep literal historical paths. Current imports, CLIs, tests, registries and operational docs must use canonical paths.

## P0 synchronization queue

P0 does not mean “oldest”; it means that a mismatch can misidentify the active system. Review these as one identity surface whenever any member changes:

1. `research/registry.json`, the v11 identity and `operational_baseline_current.json`;
2. `live/config.py`, `live/config.yaml`, `live/main.py`, `live/runtime_policy.py` and deploy preflight;
3. `strategy/maker_engine.py`, `strategy/signal.py`, `strategy/quote_core.py`, `features/feature_dag.py` and `strategy/model_contract.py`;
4. `models/backtest_config.py` and `models/backtest_tick.py`;
5. `README.md`, `README.zh-CN.md`, `project.md` and `research/README.md`;
6. dual-path and direct full-path promotion contracts.

The v11 pointer and identity are the current operational authority. The queue still marks governance files P0 because a future run must not silently attach new code or policy bytes to an old baseline identity.

## P1 approval-gated deletion group

The only file-deletion candidates carried forward are four exact, closed F09 action evaluators and their four tests:

| Evaluator | Test | Reason |
|---|---|---|
| `safe_add_rearm_hazard.py` | `test_safe_add_rearm_hazard.py` | Fixed-elapsed opportunity runner belongs to a closed family |
| `buy_conditional_widen_cate.py` | `test_buy_conditional_widen_cate.py` | BUY one-tick widen failed Development |
| `sell_repair_trend_skip_cate.py` | `test_sell_repair_trend_skip_cate.py` | SELL one-cycle skip failed Development |
| `sell_campaign_add_permission_cate.py` | `test_sell_campaign_add_permission_cate.py` | Stop-add-until-flat was an over-broad participation shutdown |

Canonical evaluator paths are under `research/families/f09_campaign_action_uplift/audit/`. These eight files may be removed only as an owner-approved atomic group after confirming that frozen reports/artifacts and generic randomized replay, causal-path and OPE plumbing remain reproducible. They were not deleted in this update.

## Shadow-surface rationalization

The current direct-promotion contract no longer treats persistent observation- only shadow as a mandatory strategy stage. Current governance is:

- BUY fill-selection shadow was retired after its negative current-stack point estimate, action suspension and feature/estimand audit;
- retire the cross-venue fair-center candidate shadow after the action closed;
- freeze the inventory what-if denominator, then retire its continuous writer;
- retire continuous depth diagnostics while retaining the promoted active imbalance-asymmetry action;
- retain q90 shadow only as a bounded exception until fresh recovery, lockstep and transport work close;
- move external receive-time evidence to the independent bounded recorder before removing its live-process inputs.

This is a logic/config/logging retirement plan, not permission to delete `maker_engine.py`, global-flow infrastructure or external-venue adapters.

## Storage safety

At this snapshot:

- the internal filesystem has approximately 57 GiB available, below the project's 60 GiB reserve;
- `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` has approximately 482 GiB available;
- authoritative non-cache MarketData lives under `${NARROWGATE_MARKETDATA_ROOT}`;
- window/replay caches remain separately governed on the internal disk.

Do not start a large replay, cache build, lifecycle rebuild or multi-horizon label job on the internal disk until the storage gate passes. LRU source age does not authorize deleting raw data, frozen artifacts or hash-referenced caches to make space.

## Next governance work

Proceed from oldest to newest within each priority class:

1. keep the P0 runtime/baseline/document identity synchronized as active work settles;
2. obtain an explicit owner decision on the eight-file P1 closed-evaluator group;
3. retire the remaining persistent shadow writers through separate config/runtime changes, preserving q90's bounded exception and the external recorder contract;
4. review the oldest untagged Markdown documents substantively, adding the material-modification tag only when claims, links, units and status were actually checked;
5. review zero-reference code for dynamic CLI/config/artifact consumers before proposing any new deletion candidate.

## Verification boundary

The refreshed CSV is required to satisfy:

- every listed path exists;
- paths are unique and sorted by filesystem mtime from oldest to newest;
- no active `research_XX` path remains;
- current canonical migration paths are used;
- the CSV contains the current audit Markdown and records its 2026-08-04 material-modification tag.

The queue was generated with repository `.venv/bin/python` (Python 3.12.13). No full test suite was run for this update. The BUY fill-selection action and shadow permissions are OFF in v9; no other strategy parameter or frozen research artifact was changed.
