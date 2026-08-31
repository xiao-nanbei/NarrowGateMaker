# Research Family Layout

<p><a href="README.md">English</a> | <a href="README.zh-CN.md">简体中文</a></p>

Last materially modified: 2026-08-30

Last materially synchronized: 2026-08-30

NarrowGate research is organized into ten strategy/evidence families, one system-engineering line, and four shared infrastructure layers. All research source, evidence, shared contracts, and governance metadata live under this single `research/` subtree.

Public research documents and owner-only evidence are separated by the repository-wide [Public Research and Private Evidence Layout](PRIVATE_EVIDENCE.md). Every concrete research unit keeps public methods and conclusions in its tracked README/docs and resolves non-published artifacts through its ignored local `private/` catalog.

Historical family-owned paths under `models/`, `models/audit/`, `features/`, `docs/`, `cpp/narrowgate_cpp/`, and the former root `research_*` directories have been removed. Active imports, commands, tests, and build files use the canonical `research.*` packages directly; no compatibility symlinks or duplicate source files are retained.

Versioned migration contracts live in `governance/migrations/`. `layout_v1.json` maps the first 198 removed paths into family ownership; `layout_v2.json` maps the former root research layout into this subtree. The exact boundary archives are private historical evidence and are not distributed with the public repository; [`governance/archive/README.md`](governance/archive/README.md) publishes their artifact IDs, SHA256 values, byte counts, and availability. An authorized historical reproduction must verify the matching private archive, while current canonical code must receive a new experiment identity when rerun.

| ID | Directory | Status | Shared layers |
|---|---|---|---|
| F01 | `families/f01_fixed_parameter_racing/` | alpha family closed; screening only | D, R, S, G |
| F02 | `families/f02_empirical_p3_touch/` | frozen replay/operational comparator dependency; successor prediction infrastructure is research-only | D, S |
| F03 | `families/f03_causal_13_head/` | causal-v12 semantics-v6 is a frozen replay/operational comparator; research q10 unresolved | D, R, S, G |
| F04 | `families/f04_external_market_alpha/` | BABEL-P1 count gate complete; exact lifecycle, common-row denominator, and causal-clock closure remain blocked; clock-limited E6/P2 quote mechanics complete; no action authority | D, R, G |
| F05 | `families/f05_fill_quality_quote_ev/` | evidence active; direct action archived; first-add USDC prediction closed on Development | D, R, S |
| F06 | `families/f06_placement_fill_cif/` | placement-distance fill/value paths closed; signed marginal value unidentified on Development | D, R, G |
| F07 | `families/f07_active_order_continuation/` | historical families closed; operational BUY trial is separate | D, R, S, G |
| F08 | `families/f08_side_taker_lifecycle/` | identity/parity research active; hazard M0 closed | D, R |
| F09 | `families/f09_campaign_action_uplift/` | frozen global-BER replay/operational comparator only; no registered action; tested cooldown temporal-permission action subspace exhausted | D, R, S, G |
| F10 | `families/f10_live_replay_attribution/` | active diagnostic line; first-add Development evidence complete, no action authority | R, S |
| SYS | `system_engineering/` | active engineering evidence; no alpha authority | R, S |

Shared layers are indexed under `shared/`:

- D: data identity and good-day admission;
- R: authoritative replay, queue, and lifecycle;
- S: shared live/replay strategy semantics;
- G: experiment identity, scorecard, OPE governance, and promotion control.

Moving a file between families is a governance change. Update `registry.json` and the path-migration record, then rerun the import, archive, build, and frozen-identity checks. Do not recreate a removed legacy path. A directory status never grants Validation, holdout, action, or live permission; those permissions remain in the family-specific frozen experiment identity.
