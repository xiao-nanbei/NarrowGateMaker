# NarrowGate BTCUSDC Codebase Architecture - 2026-07-05

Last materially modified: 2026-08-26

Current status (2026-07-29): family-specific source and evidence live under the single `research/` subtree registered in `research/registry.json`. Historical family paths and the former root `research_*` packages have been removed rather than retained as aliases. Active commands use canonical `research.families.*` packages, while versioned manifests and archives under `research/governance/` preserve both migration boundaries. Shared replay and governance implementations remain in their runtime-owned packages. New paired work uses `build_paired_daily_evidence()` followed by `research.families.f01_fixed_parameter_racing.audit.paired_screening`; panel transitions belong to `models.audit.panel_promotion_controller`. `paired_daily_selection()` is compatibility-only and has no independent ranking or promotion authority.

This document defines where code and research artifacts belong.  The goal is to keep the repository open-source readable: runnable code lives in stable modules, historical evidence lives in `docs/` or generated result folders, and one-off scripts do not become permanent public entrypoints.

## Top-Level Responsibilities

| Path | Responsibility | Notes |
| --- | --- | --- |
| `live/` | Live process, config loading, process entrypoints | Live-only safety and ops checks belong here.  Do not add research sweeps. |
| `live/orderbook/` | Runtime execution-market public-book reconstruction | Snapshot/diff sequence state only; historical payloads never live here. |
| `execution/` | State attached to NarrowGate's own orders | Active-order depth paths and queue-state bounds; no venue transport ownership. |
| `strategy/` | Shared strategy logic used by live and replay | Quote construction, maker engine policy, signal state, inventory state. |
| `models/` | Offline training, Python-reference replay, canonical research runners | No Markdown reports, no generated result files, no one-off bucket scripts. |
| `models/audit/` | Unified audit package | Campaign, fill/order denominator, order-level score, daily gate, and future evidence reports. |
| `research/families/` | Ten family-owned research workspaces | Source at family root/`audit/`, evidence under `docs/`; permissions remain experiment-specific. |
| `research/shared/` | Shared-layer ownership indexes | D/R/S/G code stays in its runtime package and is never copied into a family. |
| `research/system_engineering/` | Performance, time, transport, and deployment evidence | This line has no alpha or live-promotion authority. |
| `research/governance/` | Registry migration resolver and immutable layout archives | No import aliases, source duplicates, or compatibility symlinks. |
| `features/` | Feature engineering and preprocessing | Keep training/replay feature construction here, not in ad hoc model scripts. |
| `data/` | Offline download, import, normalization, and data-quality entrypoints | Contains tooling only. Payloads live under `${NARROWGATE_DATA_ROOT}`; no live state or strategy logic. |
| `cpp/` | pybind11 extension and C++ replay/quote/signal code | Only promote C++ paths after explicit Python parity. |
| `bench/` | Local/system benchmarks | Benchmarks must be labeled synthetic vs live soak. |
| `tests/` | Unit/parity/regression tests | Add tests for any live/replay behavior change. |
| `docs/` | Architecture, plans, historical audit notes, evidence summaries | Markdown belongs here, not in `models/`. |
| `logs/`, `results/` | Local generated outputs | Do not treat generated logs as source code. |

## Canonical Offline Entry Points

- `models/backtest_tick.py`: Python reference tick replay engine.
- `research/families/f01_fixed_parameter_racing/campaign_outcome_replay_audit.py`: campaign-level arm evaluation and campaign labels; accepts external arm specs with `--arm-spec-json`.
- `research/families/f01_fixed_parameter_racing/parameter_racing_sweep.py`: parameter coverage, quick-smoke, quick-full-main-effect, frozen validation, and candidate handoff.
- `research/families/f01_fixed_parameter_racing/parameter_selection.py`: parameter registry, arm generation and paired evidence construction. Its legacy selector is compatibility-only.
- `research/families/f01_fixed_parameter_racing/audit/paired_screening.py`: the canonical paired screening/ranking layer for new parameter research.
- `models/audit/experiment_scorecard.py`: versioned gates and weighted scores for paired and action/OPE evidence.
- `models/audit/panel_promotion_controller.py`: the separate, fail-closed Development/Validation/holdout transition controller; it never authorizes automatic live promotion.
- `models/alpha_evidence_ledger.py`: first entrypoint for fill-level alpha evidence after replay/live mechanism sanity.
- `python -m research.families.f10_live_replay_attribution.audit.runner`: routine audit reports.
- `python -m research.families.f05_fill_quality_quote_ev.audit.order_score_fast`: large retained-panel score sanity.
- `python -m research.families.f05_fill_quality_quote_ev.audit.fill_selection_score`: blocked-day OOS calibration of a quote-time non-toxic-fill selection score from order-level denominator rows.  It is an evidence/calibration tool, not a live policy entrypoint.

## Removed Or Archived Entrypoints

The following families are not current runnable entrypoints:

- `models/stage_t_*.py`: removed on 2026-07-05. Their pre-repair conclusions were removed as well. Recreate similar reports only by adding a causal report to `models/audit/` or `models/alpha_evidence_ledger.py`.
- `models/live_campaign_audit.py`: removed.  Use `research.families.f10_live_replay_attribution.audit.runner`.
- `models/live_episode_replay_compare.py`: removed.  Use unified live/replay audit reports.
- `models/live_offline_feature_audit.py`: removed.  Add feature sanity to `models/audit/` if needed.
- `models/local_liquidity_mechanism_audit.py`: removed on 2026-07-05.  Use `python -m research.families.f10_live_replay_attribution.audit.runner --reports local_liquidity_mechanism` with an order-level table or replay orders/fills.
- `models/response_kernel_audit.py`, `models/ou_reversion_bucket_audit.py`, `models/absorptive_capacity_audit.py`, `models/xmarket_as_moderator_audit.py`, and `models/alpha_bucket_intersection.py`: removed on 2026-07-05 after their shared response / half-life / absorptive-capacity / xmarket-moderator intersection output was merged into `research.families.f10_live_replay_attribution.audit.runner --reports local_liquidity_mechanism`.

Scripts with names like `*_shadow_*`, `*_bucket_*`, or experimental alpha audits are prototype/deep-dive tools unless this document, README, or `project.md` explicitly lists them as canonical.  Their output can be used as historical evidence, but not as direct live-promotion proof.

## Adding New Research Work

1. Add family-specific fields, labels, and models to the registered `research_*` directory. Add only genuinely cross-family contracts to `models/audit/`.
2. Add a CLI report to `research/families/f10_live_replay_attribution/audit/runner.py` when the output is reusable.
3. If the work is parameter selection, express it as an arm spec consumed by `campaign_outcome_replay_audit.py`, build paired daily evidence, and rank it through `paired_screen_v2`; do not create a custom selector or sweep-specific promotion rule.
4. If the work is alpha discovery, start from `alpha_evidence_ledger.py` or `order_level` evidence tables.  Buckets are diagnostic slices, not policy.
5. Document family conclusions in that family's `docs/` directory, retain a compatibility path when a frozen identity names the old `docs/` path, and keep result CSVs in the configured MarketData/backtest results directory.

## Deletion Rule

A research script can be deleted when all are true:

- It is not imported by canonical code or tests.
- Its historical conclusions are already recorded in `docs/` or generated results.
- It is not listed as a canonical entrypoint in this document, README, or `project.md`.
- A future rerun would be better implemented through `models/audit/`, `alpha_evidence_ledger.py`, or `campaign_outcome_replay_audit.py`.

Generated caches such as `__pycache__` and `.pytest_cache` should not be kept in the project tree.
