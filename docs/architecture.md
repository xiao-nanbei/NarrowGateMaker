# Architecture and module ownership

[English](architecture.md) | [简体中文](architecture.zh-CN.md)

Last materially modified: 2026-10-24

Last materially synchronized: 2026-10-24

This is the single current architecture guide. The private repository is the development source; the public repository distributes reviewed source, not a competing implementation. Model weights, purchased inputs and private runtime evidence remain outside versioned source; see the [public/private contract](public_private_documentation_contract.md).

## Module responsibilities

| Owner | Responsibility |
| --- | --- |
| `data/`, `data/downloaders/` | Acquisition, source-bound facts, causal observations and input validation; not live state |
| `features/` | Shared feature engineering, including `quote_ev.py` |
| `narrowgate/prediction/` | P3 artifact validation, serialization and inference, with explicit artifact paths; no fitting or research imports |
| `narrowgate/runtime/` | Shared epoch identity and restored runtime state |
| `strategy/` | Live/replay quote, signal, policy and inventory logic |
| `execution/` | Own-order lifecycle, depth paths and queue bounds; not venue transport |
| `live/`, `live/orderbook/` | Live process/configuration and execution-market book reconstruction |
| `models/`, `models/replay/` | Transitional training/replay packages, public tick assembly, native-loop adaptation, queues, windows and accounting |
| `narrowgate/replay/executor/` | Python economic executor, runtime-state ownership, ordered event phases and explicit replay adapters |
| `models/audit/` | Existing shared audit consumers; not the default owner for new family-specific studies |
| `research/families/` | Family-specific models, experiments and public explanations |
| `research/shared/` | Shared-layer ownership indexes; implementations stay with runtime owners |
| `research/system_engineering/`, `research/governance/` | Engineering studies, experiment governance and layout archives |
| `narrowgate/`, `narrowgate/replay/` | CLI, resolved local run plans, complete finite-account execution/control and shared runtime contracts; not yet the sole core package |
| `cpp/`, `bench/`, `tests/` | Native implementation, labelled benchmarks and behavior/parity regressions |
| `scripts/`, `docs/` | Maintenance/deployment tools and maintained guides |
| `narrowgate/maintenance/` | Terminal runtime-lock retirement and the existing local file inventory/search; no job scheduling or live operations |
| `tools/poolrun/` | Independently installed generic finite-batch scheduler; no implicit takeover of research or live jobs |

Market payloads, generated logs and results are not source code. Storage and acquisition commands are maintained only in the [data guide](../data/README.md); family evidence belongs to its registered owner.

## Dependency direction

Research consumes shared data, features and runtime contracts; new runtime contracts must not depend on research-family implementations. F05 may re-export shared features for its existing API, but must not keep a second implementation. Keep facts separate from strategy-visible observations, predictions from actions, and funding settlement from features. Live, training and replay retain separate state lifecycles. Native paths require fixed-input Python parity.

P3 inference has one owner, `narrowgate/prediction/touch_probability.py`. MakerEngine, backtest configuration and deployment preflight import it directly, without importing F02 calibration or loading its training-data paths. The core still uses the original NumPy/SciPy prediction operations; this is not removal of SciPy's transitive dependencies. The F02 `touch_probability.py` entry retains offline fitting and a thin subclass for its historical symbol-local `load()`/`save()` defaults and trusted class locator; it inherits all validation and prediction methods. The core requires explicit paths and does not resolve training directories. MakerEngine's legacy optional-directory adapter still resolves the established model-directory default outside the core. This extraction does not migrate frozen releases or authorize loading old checkpoints under a new release.

The P3 extraction's affected checks pass 119 tests plus 51 extended tests (four ownership checks overlap). Two owner-only frozen F02 specification tests remain deselected by the existing historical-availability policy; they are not reported as passes. Comparison against the pre-extraction implementation gives identical predictions over 100,004 distances for each empirical/SU profile, identical slope/argmax and identical serialized bytes. A newly built 630-entry wheel was temporarily installed and loaded outside the checkout with research imports prohibited; P3 inference succeeds and private/test packages are excluded. This uses existing standalone dependencies, not a clean Linux/native release acceptance. Temporary build/install artifacts were removed; no fit, new market replay or deployment was performed.

Order-lifecycle journals have one maintained event/cursor implementation in `execution/order_lifecycle_journal_v2.py` and one replay callback implementation in `models/replay/order_lifecycle_v2_replay_adapter.py`. Strict-native modules are thin provenance/schema adapters over those implementations, not copied engines. Current mechanics diagnostics also emit v2 rows, explicitly labelled diagnostic rather than admitted native evidence. `order_lifecycle_journal.py` retains only the old v1 snapshot schema/builders for historical readers and regression fixtures; new producers do not import it. Existing v1 files are never silently appended with v2 columns or relabelled as v2 evidence. Frozen experiment packages retain their original bytes.

The callback-durable and day-buffered journal writers share one pure batch-admission plan in `execution/order_lifecycle_journal_writer_v2.py`; they retain distinct publication and recovery boundaries. `LifecycleJournalCommitResult.commit_scope` distinguishes `callback_durable` from `memory_buffer`: legacy `status="committed"` alone is not a durability claim. MakerEngine binds its lifecycle submission interface at assembly: direct commit through the central evidence queue, or enqueue through the standalone lifecycle owner; it does not select a fallback protocol per callback. Read-only `models/replay/order_exposure.py` remains a single-effective-clock, half-open summary, not a replacement for the quantity-weighted exchange/visible-clock state machine.

Extend the registered family in place; do not restore removed root `research_*` aliases, symlinks or duplicate source trees. A matching filename does not imply matching semantics. Existing shared audit/governance consumers are transitional, not a reason to move all research there. F05 ADD/WAIT studies share train-only transforms, fitting, cluster intervals and declared OOF membership validation in their existing `audit/multiscale_ema_add_wait_incremental_value.py` module. Historical study entries retain their exact specifications, feature lists, artifact identities and execution orchestration; their coverage counts are not global replay quotas.

## Maintained entry points

- [Local maintenance](maintenance.md): `narrowgate maintenance` previews or retires verified terminal runtime locks and refreshes the existing file index only after actual cleanup or an explicit index request.

- [PoolRun Lite](../tools/poolrun/README.md): an optional Python 3.12+ subproject with its own environment, `poolrun` CLI, tests and synthetic examples. It is not included in the default NarrowGate installation; production adapters and cross-host acceptance remain operator responsibilities.
- [Data](../data/README.md): `python -m data` and explicit historical adapters.
- [Models](../models/README.md): `models/backtest_tick.py` retains public input/parameter/service assembly and native adaptation; the Python event executor is owned by `narrowgate/replay/executor/`. Do not add a second loop to the facade.
- [Research](../research/README.md): the [407-day work list](../research/recompute_407.json) and each selected experiment own current inputs, methods and permissions; old closure states do not block new studies.
- F01 current public-input entry: [`public_input.py`](../research/families/f01_fixed_parameter_racing/public_input.py), with independent arms in `iter_parameter_candidates()` and complete shared settlement in `replay_economic_candidates()`. `inventory_lifecycle_outcome_replay_audit.py`, `parameter_racing_sweep.py`, `parameter_selection.py`, and `build_paired_daily_evidence()` / `audit/paired_screening.py` retain their historical or specific execution contracts; their old defaults and `paired_daily_selection()` are not the current entry.
- Existing shared gates: `models/audit/experiment_scorecard.py` and `panel_promotion_controller.py`; these do not grant live authority or impose one inventory lifecycle contract on every new study.
- Existing attribution/diagnostics: `models/alpha_evidence_ledger.py`, `research.families.f10_live_replay_attribution.audit.runner`, and F05 `audit.order_score_fast` / `audit.fill_selection_score`. Diagnostic buckets and scores are not policies or deployment evidence.
- [Contributor checks and CI](dev/ci.md): local verification and hosted-check responsibilities.

## Shared inputs and module ownership

Eight acquisition adapters live in `data/downloaders/`. `data/facts.py`, `data/observation.py` and `data/runtime.py` provide shared input infrastructure; their presence is not proof of complete real-data validation. `narrowgate/runtime/` owns epoch contracts, and `features/quote_ev.py` owns scalar quote-EV features. Family source uses `research.families.*` packages.

## Supported execution boundaries

### Replay restoration scope

The maintained Python ConsumerBundle entry, `simulate_prepared_inputs`, accepts `checkpoint_at_ts_ms` and `resume_checkpoint`. `models/replay/runtime_checkpoint_io.py` persists the owned account/order/queue/RNG/policy graph; the prepared entry binds the input manifest, effective parameters, numeric predictions and execution-owner source. An output directory and progress callback are process-local rather than economic inputs. Resume restores the saved strategy, not a freshly initialized substitute. A checkpoint is trusted local implementation state, not a portable artifact or an accepted web upload.

`ReplayL2Journal` seals an immutable prefix without fabricating account closure, then copies verified records into a separate branch writer with their original logical event identities. Each branch continues independent production/submission/read-back counts. Tests cover persisted independent branches, public F06/F07 policy state, cold signal startup, asynchronous execution, pending order transitions and UTC accounting boundaries. Source/input/parameter mismatches and changed journal prefixes are errors, not compatibility fallbacks. Retired Makefile wrappers are removed; installed `narrowgate data` and `narrowgate replay` commands own those CLI surfaces.

A full previously evaluated two-day development F05 account was saved at the intervening UTC midnight and restored in a new process. All 1,082 fills and 452,728 L2 records, policy counters, UTC marks and complete accounting matched the retained cold-run baseline exactly; net PnL difference was zero. This is one engineering replay, no new fit or candidate. The source-bound receipt is indexed in the [existing work list](../research/recompute_407.json); underlying inputs and receipts are private, not distributed. The ten obsolete-loader fixture failures have since been closed by removing the retired loader/reload chain and migrating the tests to current admission rejection and component invariance. The targeted closure suite passes 535 tests; this is not a claim that every repository test passes.

Native cooldown exports/restores complete value state, bound to configuration, binary and policy source, with new process-local locks. An explicitly authorized development072 engineering configuration changes only the two cooldown enable flags, preserving its original coefficient axis, ML parameters and F05 action. A new uninterrupted full account and a new-process midnight restoration match exactly: 980 fills, 419,755 L2 events, 490 cooldown decisions, inventory, fees, funding, UTC accounting and net PnL (difference zero). The cut retains nonzero inventory, an active order, unexpired SELL cooldown and pending native windows; exported state is exact before the next event. Operational wall-time telemetry is separate. Prepared-checkpoint binding now handles immutable cooldown inputs rather than attempting to JSON-serialize the runtime object; 30 focused tests pass, including two added cases. This does not establish cooldown profitability, arbitrary cross-version recovery, the full C++ tick loop, every filesystem failure, or F06/F07 scientific completion. Deferred mutable variance outputs remain unsupported. Frozen source identities, authorized research scope and actual resource/concurrency protections still apply; backtest-count, candidate-count and cumulative-compute quotas do not.

The isolated Linux candidate reuses its verified build, installed environment and earlier 270-test evidence. The owner-selected new Tardis coefficient axis0.05/asymmetry0.1 configuration now passes real-market assembly through MakerEngine, shared features, all13 heads, P3, native quoting and a nontrading intent recorder. Two warmed decisions and four intents match the local reference exactly; only lock wait/hold telemetry differs. The real Tardis observations use an explicitly simulated ordered transport, not proven native receive/sequence parity. This is candidate assembly acceptance, not activation, profitability or exhaustive market coverage. No current pointer, service or production configuration changed. Private source-bound receipts are indexed in the existing work list and are not distributed; independent from-start parameter research remains independent of unrelated engineering work.

The Python event loop and state ownership have been extracted, but large initialization/finalization and research-policy sections still need decomposition. Governance ownership consolidation, deployment/maintenance script separation, full `src/narrowgate` consolidation and root-manual shortening also remain unfinished. Do not move active research code as a side effect of documentation cleanup. Further extraction needs bounded responsibilities and fixed-input quote/order/inventory/accounting regressions.

Before removing a historical entry, check imports, CLI/script/test consumers and frozen identities, and verify an actual recoverable source/material archive. A mutable alpha commit is not a recovery guarantee. Preserve historical evidence unchanged; do not replace its hashes with today's source or add missing-file skips. Generated caches and private results are not cleanup targets of this consolidation.

## Active architecture migration

The owner's four acceptance criteria are the final scope: shared decision/order/accounting rules with explicit replay/live adapters; experiments expressed as policies/configuration/methods rather than executor copies or source edits; dependency-based retirement with recoverable unique evidence; and one fixed CLI/plan for run, pause, resume, complete reporting and release checking/installation. The following stages are one migration plan, not separate frameworks or approval gates. A stage implementation is not completion of all four criteria.

| Stage | Current boundary |
| --- | --- |
| M0: actual tree and runtime | Work from the dirty checkout without discarding previous changes. Active PoolRun releases remain bound to their original source and interpreter; current-source tests do not upgrade them. |
| M1: manual replay lifecycle | Implemented local single-bundle JSON-plan path, complete artifacts, cooperative pause and explicit disk resume. PoolRun dispatch of this plan and bounded-window continuous plan assembly remain pending. |
| M2: neutral core/package | Finite-account execution and IO moved from F01 to `narrowgate/replay/account.py` and `narrowgate/runtime/artifact_io.py`; P3 validation/inference moved from F02 to `narrowgate/prediction/touch_probability.py`, with current live/replay/preflight consumers migrated. Historical family adapters retain only path/API compatibility. Full `src/` layout, remaining inference ownership and optional research distribution remain pending. |
| M3: executor state and loop | `narrowgate/replay/event_clock.py` owns causal event construction. `narrowgate/replay/executor/` now owns runtime state, checkpoint capture/transfer, the event driver and phase implementations; the former monolithic facade loop is removed. Initialization/finalization and policy-state typing remain partial, not a completed migration. |
| M4: shared live boundary | Live and replay consume shared quote math, pre-policy sizing, replacement throttle/coalescing/cancel-first rules and position-fill accounting. Adapters preserve their distinct clocks, ledgers and numeric representations. Full decision/lifecycle consolidation and installed Linux acceptance remain pending. This source refactor does not update or restart live. |
| M5: research/retirement | Maintained F01/F04 consumers use the extracted shared account/IO owners. Historical frozen builders/releases are not migrated by importing current facades. Policy hooks and full version retirement remain pending. |
| M6: standalone release operations | Unified release build/check/install and clean Linux/native deployment acceptance remain pending. No new deployment commands are advertised as implemented. |

### Current state and event boundaries

`ReplayServices` binds explicit process dependencies at assembly. `ReplayRuntimeState` owns checkpointable account/order/FIFO/RNG/policy state; `ReplayContext` owns non-persistent input and call-scoped assembly. Checkpoint capture and consuming-versus-branch restoration have one implementation in the state owner, retaining the original v1 namespace graph, aliases, next-event boundary and journal prefixes. The executor package imports without the facade or research packages and never compiles extracted source. Frozen workers and checkpoints are not migrated by editing this checkout.

`events.py` is now an ordered driver, not another large execution body. Its phases advance exchange/market state and fills before private visibility and quote eligibility, then run quote calculation, policies, risk selection, order decisions, REST execution and sampling. Market progression remains active during strategy sleeps, computation and planned drains. Quote-time early exits return to the driver without skipping the preceding market phase. Fork washout shares one implementation; loop-local scratch values no longer require generated context fields. The extracted phase bodies retain their economic operations and ordering.

Replay's native import uses `strategy/native_runtime.py`, as live consumers already do. The old checkout-name/build-directory substring guess is removed; supported application interface, required entry points and an explicitly supplied runtime directory are checked once by their existing owner. Broken native imports remain errors, not silent Python fallback. Three obsolete source-text/layout assertions were retired; maintained tests instead exercise actual q90 callback order and terminal-state rejection, while existing visibility-boundary tests exercise real propagation.

Fixed-input verification compares all result fields across six modes (ordinary, asynchronous execution, computation delay, timeout, emergency and close replacement), both uninterrupted and at three saved cuts. This is behavior-preserving engineering evidence, not a new economic experiment or exhaustive proof of the entire architecture.

### Local plan commands implemented in M1

The event-clock extraction moves the original function without changing its body: execution trades retain stable same-millisecond ordering, synthetic clocks cannot fill, system events follow same-time market events, and cold-start/retained-price/terminal-boundary rules are unchanged. The old import is an alias to the single implementation, not a second engine. Standalone clock import does not load the tick executor, live or research packages. The affected suite passes 437 tests, including original prepared-clock reuse, midnight window recovery, complete-plan CLI and event-order regressions. Eleven existing native-backend tests now use the existing valid quote-config fixture instead of `None`, preserving their original no-fallback and error-identity assertions; production quote behavior is unchanged. Seven pandas deprecation warnings remain. This is a bounded M3 extraction, not completion of the state/decision/order/accounting migration, an installed-release acceptance or authorization to update frozen workers/checkpoints.

```bash
narrowgate replay plan --config /private/plans/account.json
narrowgate replay run --plan /private/plans/account.json --executor local
narrowgate runs status /private/results/account
narrowgate runs pause /private/results/account
narrowgate replay resume --run /private/results/account
narrowgate runs report /private/results/account
```

These commands neither connect to cloud hosts nor trade. `plan` resolves and validates without preparing or replaying. `run` executes in the foreground; use a separate terminal for `pause`. `PAUSE_REQUESTED` is only a request: the existing executor saves at its next supported hourly cut, or finishes the account first. Daily heavy checkpoints retain the original first-cut bootstrap save; a pause forces a durable save. `status` reports lock ownership, lifecycle, memory/durable cuts and recovery availability; a completion marker alone is not acceptance. `report` verifies all retained payload hashes and accounting identities before reporting complete PnL, USDC per BTC traded and traded-notional bps. Incomplete accounts return null terminal metrics, not zero.

The input JSON has exactly `schema`, `run_id`, `identity`, `bundle`, `params`, `model_dir`, `funding`, `accounting` and `output`. `schema` is `narrowgate.replay-plan.v1`; `identity` supplies `experiment_id`, versioned `baseline_id`, `arm_id`, `account_contract` and `parent_id`. `bundle` is the existing ConsumerBundle directory. `params` is either a full explicit effective-parameter object, or `{"base_file": "baseline-params.json", "overrides": {}}`; overrides may only name existing base keys. `model_dir` is explicit and must be null exactly when ML is disabled. `funding` names the original source-bound schedule JSON. `accounting` supplies `initial_capital` in USDC and `max_mark_age_ns`. `output` names an empty, separately owned result directory. Relative paths resolve against the plan, never the working directory or live configuration. No profitable/default parameters or substitute funding schedule are supplied.

`plan` exposes resolved parameters, per-parameter origins, units, artifact locations, source/model/input/funding bindings and limitations. Existing engine semantic checks still apply; this is not yet a closed schema for every historical parameter. The initial profile supports JSON-configured single-bundle accounts; it does not assemble private runtime-object policies, automatically restore cold inputs, or reuse global continuous timelines. It does not replace existing running research accounts. `--executor poolrun` is rejected rather than simulated. Legacy `replay --data-bundle ... --data-replay-config ... --summary-json ...` remains explicitly summary-only.

The run directory contains `resolved-plan.json`, identity/lifecycle/progress, complete result, accounting, daily ledger, metrics, diagnostics, full journal and hash-bound completion. The original necessary checkpoint is retained for owner-side maintenance rather than removed by a reporting command. A repeated completed run is verified and returned without replay. An existing incomplete run requires explicit `resume`; missing recovery state, changed parameters/source/model/input/funding or a foreign output fails closed. Resume accepts only trusted local checkpoints under the original release, not uploaded pickle files or automatic cross-version conversion. The command exits 0 on a successful operation (including an acknowledged pause request or incomplete status report), 2 on argument/admission/filesystem errors; unexpected execution failures remain nonzero with `lifecycle.json` failure details. Read status/report JSON to distinguish completed, paused, running and incomplete operations.

Tests use the real Python engine on a small synthetic ConsumerBundle, including a new-process disk restore compared with uninterrupted fills, journal and accounting. This is engineering evidence, not a new market-data replay or production/live acceptance. Full ML/native release parity, installed clean-environment execution and the remaining stages must still be established before declaring the architecture migration complete.

The initial migration's affected suite passes 122 tests, including retained F01/F04 execution/report regressions and cold loading of the old latency experiment's frozen runner. Scoped Ruff and diff whitespace checks pass. A wheel was built and temporarily installed, then imported with CLI help and source binding exercised outside the checkout using an isolated interpreter and no `PYTHONPATH`; its 628 archive entries contain the new modules and exclude private/test packages. This smoke reuses standalone Python dependencies, does not install a production release, and is not clean Linux/native or installed real-account acceptance. Temporary build/install artifacts were removed.
