# Architecture and module ownership

[English](architecture.md) | [简体中文](architecture.zh-CN.md)

Last materially modified: 2026-09-25

Last materially synchronized: 2026-09-25

This is the single current architecture guide. The private repository is the development source; the public repository distributes reviewed source, not a competing implementation. Model weights, purchased inputs and private runtime evidence remain outside versioned source; see the [public/private contract](public_private_documentation_contract.md).

## Module responsibilities

| Owner | Responsibility |
| --- | --- |
| `data/`, `data/downloaders/` | Acquisition, source-bound facts, causal observations and input validation; not live state |
| `features/` | Shared feature engineering, including `quote_ev.py` |
| `narrowgate/runtime/` | Shared epoch identity and restored runtime state |
| `strategy/` | Live/replay quote, signal, policy and inventory logic |
| `execution/` | Own-order lifecycle, depth paths and queue bounds; not venue transport |
| `live/`, `live/orderbook/` | Live process/configuration and execution-market book reconstruction |
| `models/`, `models/replay/` | Transitional training/replay packages, tick executor, queues, windows and accounting |
| `models/audit/` | Existing shared audit consumers; not the default owner for new family-specific studies |
| `research/families/` | Family-specific models, experiments and public explanations |
| `research/shared/` | Shared-layer ownership indexes; implementations stay with runtime owners |
| `research/system_engineering/`, `research/governance/` | Engineering studies, experiment governance and layout archives |
| `narrowgate/`, `frontend/` | CLI and UI; not yet the sole core package |
| `cpp/`, `bench/`, `tests/` | Native implementation, labelled benchmarks and behavior/parity regressions |
| `scripts/`, `docs/` | Maintenance/deployment tools and maintained guides |

Market payloads, generated logs and results are not source code. Storage and acquisition commands are maintained only in the [data guide](../data/README.md); family evidence belongs to its registered owner.

## Dependency direction

Research consumes shared data, features and runtime contracts; new runtime contracts must not depend on research-family implementations. F05 may re-export shared features for its existing API, but must not keep a second implementation. Keep facts separate from strategy-visible observations, predictions from actions, and funding settlement from features. Live, training and replay retain separate state lifecycles. Native paths require fixed-input Python parity.

Extend the registered family in place; do not restore removed root `research_*` aliases, symlinks or duplicate source trees. A matching filename does not imply matching semantics. Existing shared audit/governance consumers are transitional, not a reason to move all research there.

## Maintained entry points

- [Data](../data/README.md): `python -m data` and explicit historical adapters.
- [Models](../models/README.md): `models/backtest_tick.py` is the Python reference executor; do not enlarge it with unrelated helpers.
- [Research](../research/README.md): the [407-day work list](../research/recompute_407.json) and each selected experiment own current inputs, methods and permissions; old closure states do not block new studies.
- F01 parameter tools: `campaign_outcome_replay_audit.py`, `parameter_racing_sweep.py` and `parameter_selection.py` under `research/families/f01_fixed_parameter_racing/`. Existing paired campaigns use `build_paired_daily_evidence()` and `audit/paired_screening.py`; `paired_daily_selection()` remains compatibility-only.
- Existing shared gates: `models/audit/experiment_scorecard.py` and `panel_promotion_controller.py`; these do not grant live authority or impose one campaign contract on every new study.
- Existing attribution/diagnostics: `models/alpha_evidence_ledger.py`, `research.families.f10_live_replay_attribution.audit.runner`, and F05 `audit.order_score_fast` / `audit.fill_selection_score`. Diagnostic buckets and scores are not policies or deployment evidence.
- [Contributor checks and CI](dev/ci.md): local verification and hosted-check responsibilities.

## Completed migrations

Eight acquisition adapters now live in `data/downloaders/`; the old claim that grouping was uncommitted is obsolete. `data/facts.py`, `data/observation.py` and `data/runtime.py` exist as shared input infrastructure; their presence is not proof of complete real-data validation. Epoch contracts moved to `narrowgate/runtime/`, and scalar quote-EV features moved to `features/quote_ev.py`. Family source uses canonical `research.families.*` packages.

Historical removal inventories and migration identities remain in `research/governance/` and retained private snapshots, not a second current architecture manual. The replaced architecture/ownership guides are recoverable from a verified pre-consolidation source bundle (private evidence store; not distributed).

## Remaining migrations and removal boundary

### Replay restoration scope

The maintained Python ConsumerBundle entry, `simulate_prepared_inputs`, accepts `checkpoint_at_ts_ms` and `resume_checkpoint`. `models/replay/runtime_checkpoint_io.py` persists the owned account/order/queue/RNG/policy graph; the prepared entry binds the input manifest, effective parameters, numeric predictions and execution-owner source. An output directory and progress callback are process-local rather than economic inputs. Resume restores the saved strategy, not a freshly initialized substitute. A checkpoint is trusted local implementation state, not a portable artifact or an accepted web upload.

`ReplayL2Journal` seals an immutable prefix without fabricating account closure, then copies verified records into a separate branch writer with their original logical event identities. Each branch continues independent production/submission/read-back counts. Tests cover persisted independent branches, public F06/F07 policy state, cold signal startup, asynchronous execution, pending order transitions and UTC accounting boundaries. Source/input/parameter mismatches and changed journal prefixes are errors, not compatibility fallbacks. Retired Makefile wrappers are removed; installed `narrowgate data` and `narrowgate replay` commands own those CLI surfaces.

A full previously evaluated two-day development F05 account was saved at the intervening UTC midnight and restored in a new process. All 1,082 fills and 452,728 L2 records, policy counters, UTC marks and complete accounting matched the retained cold-run baseline exactly; net PnL difference was zero. This is one engineering replay, no new fit or candidate. The source-bound receipt is indexed in the [existing work list](../research/recompute_407.json); underlying inputs and receipts are private, not distributed. The ten obsolete-loader fixture failures have since been closed by removing the retired loader/reload chain and migrating the tests to current admission rejection and component invariance. The targeted closure suite passes 535 tests; this is not a claim that every repository test passes.

Native cooldown now exports/restores complete value state, bound to configuration, binary and policy source, with new process-local locks. SELL/BUY cross-process and independent-fork tests and synthetic tick-loop account comparisons pass; a full native-enabled real account comparison remains unverified. These capabilities do not establish arbitrary cross-version recovery, crash-proof storage under every filesystem failure, or the scientific/economic completion of F06/F07. Deferred mutable variance outputs remain unsupported for checkpointing. A research plan must still bind its complete frozen environment, workload budget and permitted compute window; recovery is not permission to run overnight.

Executor decomposition, governance ownership consolidation, deployment/maintenance script separation, full `src/narrowgate` consolidation and root-manual shortening remain unfinished. Do not move active research code or split the large executor as a side effect of documentation cleanup. Future extraction needs bounded responsibilities and fixed-input quote/order/inventory/accounting regressions.

Before removing a historical entry, check imports, CLI/script/test consumers and frozen identities, and verify an actual recoverable source/material archive. A mutable alpha commit is not a recovery guarantee. Preserve historical evidence unchanged; do not replace its hashes with today's source or add missing-file skips. Generated caches and private results are not cleanup targets of this consolidation.
