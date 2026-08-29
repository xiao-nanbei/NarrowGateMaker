---
name: mm-as-glft-ml-spread
description: 'Design, audit, or evaluate the NarrowGate BTCUSDC AS-shaped empirical quote controller and its ML/risk adapters. Use for quote-unit and clock contracts, empirical P3 touch semantics, proxy-feature naming, negative-filter action research, data-quality exclusions, and replay/runtime semantic checks; BTCUSDT is reference/source data only. Do not treat the implementation as an Avellaneda-Stoikov or GLFT optimum.'
metadata:
  short-description: 'Research and validate NarrowGate maker actions'
---

# NarrowGate Empirical Quote Controller Skill

Last materially modified: 2026-08-29

## Goal
Build or evaluate an inventory-aware market-making workflow where:
- Avellaneda--Stoikov supplies only a reservation-price and symmetric-quote shape used for comparison and implementation structure.
- The maintained quote core is an **AS-shaped empirical quote controller**, not an exact reproduction or approximate optimum of Avellaneda--Stoikov or GLFT.
- Empirical spread, inventory, regime, depth, P3, and ML controls retain their own units, clocks, estimands, and evidence authority.
- NarrowGate is a negative filter: prefer pausing exposure when estimated maker value is unsafe or unidentified; never infer action value from a theoretical label, prediction score, or implementation parity alone.

## Literature and Mechanism Classification (Mandatory)
- `exact derivation`: a paper's mathematical object is reproduced explicitly and is not presented as the current controller when the implementation differs.
- `adapted proxy`: part of a published structure is retained while the estimand, data, clock, or action semantics differ.
- `analogy`: a paper motivates a design question but does not derive the current feature, parameter, threshold, or action.
- `archived research`: the route is historical, removed, or closed and grants no current action/runtime authority.
- Classify every literature claim into exactly one of these four relationships. A citation never proves economic value or live authority.
- Do not claim that Guéant directly implies `gamma_opt ∝ 1/(sigma*sqrt(L))`, a depth-based dynamic `kappa`, or the current liquidity multiplier. Those are unsupported empirical mappings, not theoretical deductions.

## Current Scope (Mandatory)
- The only actively maintained execution/maker project is `BTCUSDC` in `${NARROWGATE_ROOT}`.
- `${NARROWGATE_ROOT_BTCUSDT_ARCHIVED}` is retired/deleted locally. Do not instruct edits, syncs, tests, deployments, README updates, or live restarts in that repo unless the user explicitly recreates it and asks for archival work.
- BTCUSDT remains valid only as BTCUSDC reference/source data: reference perp features, source-quality exclusions, stale-anchor checks, basis/lead-lag analysis, and historical context.
- Binance BTCUSDT futures raw `daily/trades` is the active public reference trade source when a BTCUSDT trade stream is needed. As of 2026-07-05 local BTCUSDT raw-trade coverage is aligned to the 111-day BTCUSDC minimal complete good-day universe (`logs/data_audit/cleanup_20260705_align_btcusdt_reference/minimal_complete_good_days_2026.csv`), not the full calendar. Binance futures `daily/bookTicker` is not usable for current 2026 work because the public directory stops at 2024-03-30; Binance `bookDepth` is coarse percent-bucket depth and must not be treated as event-level BBO/L2 or queue data.
- BTCUSDT reference evidence must be generated per retained UTC day, using the same good-day manifest as BTCUSDC replay. Do not run or present BTCUSDT ref evidence as month chunks. Cross-day/month rollups are only secondary summaries of daily rows and must preserve the day-level support/pass/fail columns.
- Do not transfer BTCUSDT execution parameters, fees, model bundles, fills/day, or PnL conclusions into BTCUSDC decisions without a fresh BTCUSDC daily replay/validation.
- For live fill calibration, `maker` means only passive limit-order fills submitted by the NarrowGate live process; `taker` means manually submitted aggressive or discretionary orders. Comparisons of live and replay-baseline fills per day, BUY/SELL split, VWAP, markout, or campaign outcome must use only program-generated maker fills. Label or exclude manual taker rows, `SYNC_ADJUST`, manual closes, and intervention fills; never mix them into maker fill-selection evidence.
- Binance and CryptoHFT files, training, daily replay, and baseline day keys use UTC days by default. Convert phrases such as "last night," "this morning," or "the UTC+8 calendar day," and exchange-UI local-time screenshots, into explicit UTC start and end timestamps. Slice live and replay with the same UTC interval; never compare a UTC daily baseline directly with a UTC+8 calendar day.
- For one-day live/baseline calibration, when live inventory is not flat at the window start, pass the day-start `initial_inventory` and `initial_entry_price` reconstructed from live `trades.csv` or HEALTH state into replay, and calculate campaign and fill splits from that initial position. Formal retained daily OOS remains fresh-start by default. Do not carry inventory into promotion evidence unless the research question explicitly targets live-episode alignment.
- For hourly live-episode alignment, prefer the preceding 10-30 minutes of logs to reconstruct `initial_inventory`, `initial_entry_price`, campaign age, MAE, counters, and the last-fill cooldown. Replay `ORDER_UPDATE` only to audit active orders and sanity-check them against the latest HEALTH `orders=` value. Do not inject active orders into the replay fill path by default: without real queue-ahead, ACK, pending, and cancel-race state, doing so creates artificial fill selection. Injection is allowed only for an explicit `restore_active_orders` or exchange-lifecycle study and must be reported separately.
- Live/baseline mechanism calibration must not rely on fills per day alone. Also report BUY VWAP, SELL VWAP, `side_vwap_edge = SELL_VWAP - BUY_VWAP`, `edge_diff_bps`, and `edge_diff_usdc = (live_edge - replay_edge) * matched_qty`. If the side-VWAP edge discrepancy is large enough to change daily PnL materially, raw/inventory-adjusted PnL, arm PnL, and campaign-terminal PnL are mechanism-distortion diagnostics only and cannot serve as promotion evidence.
- Live/baseline calibration must verify that replay uses the same main model directory as live `ml.model_dir`. The live baseline model directory comes from `live/config.yaml`, for example `${NARROWGATE_MODEL_DIR}`; `models/backtest_config.py::load_tick_base_params()` must pass it to `backtest_tick.configure_symbol()` through `model_dir_override`. If replay falls back to `models/saved_btcusdc`, its direction, volatility, return, and toxicity models differ, so hourly fill/VWAP/PnL alignment is not trustworthy.

## Workstream Classification (Mandatory)
Before proposing or executing work, classify it as **strategy-nature** or **system-nature**. Do not mix the evidence standards.

### Strategy-nature work
Use this path for alpha, maker EV, risk-edge, and live-parameter questions.

- Start from `research/families/f10_live_replay_attribution/docs/historical_backtest_evidence_revalidation_20260720.md`, `research/system_engineering/docs/time_unit_contract_repair_20260726.md`, and `docs/value_provenance_registry_20260727.md`. The 2026-07-20 and 2026-07-25 operational JSON files are frozen historical deployment identities, not proof of the currently running remote process. Run deploy preflight and verify remote runtime hashes before naming a current baseline. Pre-repair parameter grids, null values, scorer buckets, arm rankings and historical live PnL must not be reconstructed as directional evidence.
- Formal replay must bind current code, private config, empirical P3, model, event-L2, queue, latency, initial state, data manifest and random seed. Missing identity or features must fail fast.
- The research unit is a UTC-day chronological evidence split with embargo. Use Development, Validation and family-specific sealed holdout from the existing retained universe; do not wait for future days by default.
- Evaluate explicit actions, not state correlations. Use one campaign-level intervention, a complete known propensity vector, full queue/latency/cancel/fill/campaign replay, support/ESS checks and terminal reward attribution exactly once.
- Report BUY and SELL separately and distinguish opener/add/reducing only where action overlap exists. Unsupported actions are `diagnostic_only`.
- Current accepted strategy reports start with causal-v4 and the 2026-07-18+ action families. A failed family is closed; do not turn it into a threshold search on the same evidence.
- State scores, quote EV and external venue signals are diagnostics until an action family demonstrates incremental value. Local M0 must pass before adding external M1.
- Fixed gamma, effective-kappa, cap, guard, cooldown, queue multiplier or replace threshold is never described as globally optimal. Operational values are baseline state; candidate values are identity-bound experiments.
- System latency, fewer REST calls, lower fills or one short live window cannot be called strategy improvement.

### Existing-Data Action-Uplift Method (Mandatory)
- Do not default to waiting for newly arriving good days. For each new action family, first freeze chronological `development`, `validation`, and `sealed_holdout` panels from the current retained good-day universe, with embargo days between them. Use `models.audit.evidence_split`; the normal replay path must refuse the sealed panel without an explicit one-shot unseal.
- A historical holdout used by unrelated hypotheses is **family-specific sealed evidence**, not globally untouched data. It is valid for a newly frozen action family provided its action, eligibility, features, propensity, reward, gates, code/config/model/P3/queue/latency identities are fixed before reading that family's outcomes.
- New dates become necessary only after the family changes following holdout access, the family-specific holdout is consumed, no overlap-supporting split remains, or production distribution shift needs confirmation. A negative or uncertain result is not a reason to wait for more days and retry the same family.
- Validate actions, not state correlations. Use one campaign-level randomized intervention with a complete logged propensity vector and replay the full queue, latency, fill, cancel, and later inventory path. Attribute terminal campaign value once: `reward = fill_value - incremental_campaign_cost - queue_reset_cost`.
- Report BUY and SELL separately. Split `opener/add/reducing` only where each role has real action overlap; unsupported roles are `diagnostic_only`. Pooled uplift must never hide a failed side.
- The first local add-quote family is frozen to `baseline`, `prevent_over_widen`, `widen_1tick`, and `recenter_1tick`, with replay propensities `0.40/0.20/0.20/0.20`, one intervention per campaign, size/reducing side/inventory limit unchanged.
- Development uses chronological past-only nuisance/action fitting. Validation is a fixed development-to-validation evaluation, not a combined cross-fit. Unseal holdout only for a side/action that passes predeclared development and validation reward, terminal, tail, overlap/ESS, daily-sign, activity, and inventory gates.
- Use `research.families.f09_campaign_action_uplift.audit.local_action_uplift` to generate randomized panels and `research.families.f09_campaign_action_uplift.audit.local_action_ope_report` for chronological or fixed-holdout DR evaluation. Do not retune clipping, actions, features, thresholds, or reward after validation/holdout; a failed family is a completed research result.
- 2026-07-18 first-family result: the frozen existing-data split used 80 development days, one embargo, 20 validation days, one embargo and a still-sealed 20-day holdout. Development BUY `widen_1tick` was `+0.01783 USDC/intervention` and SELL `recenter_1tick` was `+0.01318`, but both intervals crossed zero. Fixed validation reduced BUY widening to `+0.00394` with a 45% positive-day rate and reversed SELL recentering to `-0.00905`; candidate extreme-tail support was also absent. No action advances, the holdout stays sealed, and this family must not be retried by waiting for new dates. Use `research/families/f09_campaign_action_uplift/docs/side_specific_action_uplift_existing_split_20260718.md` as the result record.

### System-nature work
Use this path for C++ low latency, live hot path, telemetry, order lifecycle, and infrastructure. It can make a strategy executable, but it does not prove alpha.

- Manage live only through `live/run.sh`; verify profile, process, config hash, model and empirical P3 identity before interpreting telemetry.
- The production order adapter is synchronous. The failed latest-wins async gateway and its flags/telemetry were deleted; any future async design is a new bounded-queue experiment.
- Compare Python/native on the same configuration and marked soak windows after warmup and inventory sync. Report feed age, signal/quote/requote, REST tails, action mix, placed/fills, fallback and safety logs.
- Keep external venue callbacks synchronous with native global-flow batching on the current 2-vCPU target unless a new soak proves a different architecture.
- Receive-time tapes and latency profiles are environment-specific. A machine, region, network, gateway or process-layout change requires a new identity.
- Resolve the current live target from the ignored private deployment pointer or `NARROWGATE_LIVE_REMOTE`; never copy a host/IP from a dated AWS report. Query predecessor live logs only from the verified local retirement archives. The predecessor instance is terminated and has no remote SSH or rollback endpoint.
- Preserve AWS transport artifacts as AWS-labelled historical evidence, empirical priors, or replay sensitivities when their frozen identity is required. Matching machine size does not authorize relabelling them as the current provider's measured transport. Cross-host panels require explicit provider/runtime-epoch provenance and a frozen source-aware admission contract.
- Verify raw -> bars -> causal features -> predictions -> formal replay/live lineage and fail on feature or schema drift.

## Use When
Use this skill when user asks for:
- Auditing or modifying the AS-shaped empirical quote controller, including comparison with AS/GLFT papers.
- Unit, denomination, order-size, risk-horizon, or lifecycle-clock compatibility in quote logic.
- Fixed-horizon touch, placement-fill, active-order hazard, or terminal-maker-value semantics.
- Dynamic spread logic from short-term volatility forecasts.
- Feature engineering around imbalance, event rate, and toxicity proxies.
- Backtest/live consistency checks for maker strategy.
- A preregistered, identity-bound candidate test for spread, skew, fill, or cooldown controls.

## Project Anchors (This Codebase)
- Strategy baseline: `strategy/quote_core.py`, `strategy/maker_engine.py`
- Live inference path: `strategy/signal.py`, `live/ws_handler.py`, `live/config.yaml`
- Feature/data pipeline: `features/feature_engineer.py`, `features/preprocess.py`, `features/preprocess_metrics.py`
- Fill/intensity models: `research/families/f02_empirical_p3_touch/fill_probability.py` provides the empirical P3 touch surface; placement-fill research lives under `models/audit/`, and the research-only strategy adapter is `strategy/placement_fill_probability.py`.
- Backtests: `models/backtest.py`, `models/backtest_ml.py`, `models/backtest_tick.py`
- The removed `models/research/` and `models/legacy/` trees are historical references only. New reusable diagnostics belong under `models/audit/`; do not recreate old root-level one-off runners.
- There is no active Transformer/sequence-model runtime authority. The current deployed model identity comes from the configured LightGBM bundle and its frozen artifact manifest.

## Runtime File Responsibilities (Use As Source of Truth)
- `live/config.yaml`
  - All tunable runtime parameters: AS params, ML params, risk controls, and WebSocket settings.
  - Supports SIGHUP hot reload.
- `live/config.py`
  - YAML parser and config loader.
  - Applies environment-variable overrides for API credentials.

### State Machines
- `strategy/order_manager.py`
  - Order-state machine: `PENDING_NEW -> OPEN -> PARTIALLY_FILLED -> FILLED/CANCELED/EXPIRED`.
  - Event-driven by `ORDER_TRADE_UPDATE`.
  - Thread-safe order lifecycle handling.
- `strategy/inventory_manager.py`
  - Position-state machine: `FLAT -> OPEN -> CLOSING/TIMEOUT_CLOSING -> FLAT`.
  - Tracks local average entry and PnL cache.
  - Periodically reconciles position with exchange state.

### Signal Engine
- `strategy/signal.py`
  - Online computation of all 82 live features (tick momentum, depth, microstructure, time features).
  - Loads the configured LightGBM bundle and optional quote-level EV artifacts for real-time inference.
  - Treat the live feature schema as the runtime authority when checking parity.

### Core Engine
- `strategy/maker_engine.py`
  - AS-shaped empirical quote control with optional ML and risk adapters.
  - Risk checks, exit/position-timeout handling, cancel-only operational shutdown, and separate emergency drawdown handling.
  - Keep behavior aligned with shared strategy helpers and formal `models/backtest_tick.py` replay when proposing logic changes, while treating parity as implementation agreement rather than proof of correct units or economic value.
- `live/ws_handler.py`
  - Manages three WebSocket lanes: `aggTrade`, `partial_book_depth(20 levels @ 100ms)`, and `user_data`.
  - Handles listen-key lifecycle and renewal.
- `live/main.py`
  - Runtime entrypoint.
  - Supports `--dry-run` (simulation) and `--live` (production).
  - Runs periodic position sync and health checks.

### Docs Reference Priority
- For command examples and operating conventions, consult:
  - `README.md` in this repo
  - Historical BTCUSDT notes only when the user explicitly asks for archival context; they are not active operating references.

## Current Evidence Outputs (Mandatory)
- `models/backtest_tick.py` is the formal replay authority. Bar-level `models/backtest.py` and `models/backtest_ml.py` are legacy/exploratory surfaces and cannot produce promotion evidence.
- Do not infer evidence authority from historical filenames such as `*_sweep_results.csv`, `best_*`, `forward_*`, or `retained*`. Pre-repair rankings and generated result tables were deleted from the public conclusion surface.
- Every formal result must record code, config, empirical P3, model, queue, latency, event-L2, data-manifest, initial-state, random-seed and split hashes.
- Canonical strategy outputs are daily replay summaries, order-level denominators, campaign labels, action panels with known propensity, DR/ESS reports, and family-specific validation/sealed-holdout reports.
- Generated artifacts live under `data_paths.data_root()`/configured results roots; they are lineage inputs, not current conclusions unless referenced by the current evidence boundary.
- Regenerate predictions, feature panels and action panels after model or causal-schema changes. Never reuse stale sweep CSVs to recover a deleted winner.

## Live Deployment Region and Stream Freshness (Mandatory)
- Do not deploy Binance USD-M live jobs from remote regions/IPs that return REST error `451` (`Service unavailable from a restricted location`). Public market streams may still connect, but signed account, listen-key, exchange-info, and order routes are unusable. Resolve the current target from the private deployment pointer or `NARROWGATE_LIVE_REMOTE`; never infer it from a historical cloud-host record. Preflight live hosts with `time()`, signed `account()`, and a controlled user-stream startup before enabling the maker.
- For BTCUSDC live on USD-M futures, prefer `websocket.agg_trade: true`. The raw `@trade` stream can remain silent while depth/bookTicker stay fresh, which leaves taker-tempo/flow state stale and lets the maker continue quoting from old execution-flow features. If raw trade is tested again, require a stream-silence soak test before enabling it.
- Treat `Stream silence detected` on execution or reference streams as a live safety event, not a cosmetic warning. After switching stream mode, verify `logs/maker.log` for fresh HEALTH, requotes, ORDER_UPDATE, and absence of recurring stream-silence reconnects.
- If `SYNC_ADJUST` detects missed user-stream fills, the maker must hard-degrade by pausing exposure-increasing quotes, restarting the user data WebSocket, and waiting for the configured degrade window before returning to normal quoting.

## BTCUSDC Live Guard Calibration (Mandatory)
- Treat guard, cooldown, depth and replace values in the deployed private config as operational state, not proven optima.
- Calibrate live safety mechanisms from current receive-time telemetry and the corrected baseline identity. Do not inherit fixed bands or thresholds from pre-repair reports.
- In replay/live parity, compare placed activity, action mix, side split, spread, queue/fill path and campaign state before PnL.
- `fill_cd` may block only exposure-increasing quotes; reducing-side safety and stale-data handling remain invariant.

## Daily Replay and Aggregated Reporting (Mandatory)
- Do not use cross-day strings as the research unit. Strategy research conclusions must come from independent UTC-day fresh-start replay, not from one continuous cross-day state path.
- Prefer the explicit day interfaces: `models/backtest_tick.py --day YYYY-MM-DD`, `models/tick_ab.py ... --days YYYY-MM-DD ...`, `models/quote_ev_ab_tick.py ... --days YYYY-MM-DD ...`, `models/quote_decomposition_tick.py --days ...`, `research/families/f04_external_market_alpha/cross_market_shock_audit.py --days ...`, and `research/families/f05_fill_quality_quote_ev/train_quote_ev.py --train-days ... --valid-days ...`.
- Current BTCUSDC storage is daily-only for parquet containers after the 2026-06-28 audit: `bars_1s`, `bars_1s_spot`, `features_btcusdc/features_YYYY-MM-DD.parquet`, `metrics_5m/*-YYYY-MM-DD.parquet`, and `depth_1s/*-YYYY-MM-DD.parquet`. A `YYYY-MM.parquet` file under `${NARROWGATE_DATA_ROOT}` is a bug and should be deleted or rebuilt from daily sources.
- 2026-07-06 storage cleanup physically removed 2025 market-data surfaces that no longer belong to the current 2026 retained-good-day workflow: retired private BTCUSDT research data roots, retired BTCUSDT execution data roots, and all 2025 CryptoHFT raw hourly orderbook directories under the private market-data parent. Do not reference those paths in new training/backtest commands; regenerate from current retained manifests only if an explicit archival study requires it.
- Daily A/B rows must be labeled by explicit `YYYY-MM-DD` dates whenever possible. Cross-day rollups are only secondary summaries of daily rows; do not rank live candidates from a single continuous cross-day replay.
- Daily replay must not carry adverse/defense/markout/pause state across UTC days. Within each day, keep the long-gap reset (`--quality-segment-max-gap-s 3600`) for source outages. Do not replace the shorter `data_quality.mask_valid_horizon(..., max_gap_s=5)` horizon guard used by feature/label generation.
- Always report three fills/day denominators for aggregate parity:
  - `fills_per_full_calendar_day`: fills over the first-to-last calendar span, including removed/bad days.
  - `fills_per_quality_day`: fills over retained UTC dates after quality exclusions.
  - `fills_per_active_trade_day`: fills over retained active trade seconds; this is the closest denominator for comparing to short live log windows.
- A continuous cross-day replay with low fills/day is usually a replay-state artifact. The fix is retained UTC-day fresh-start replay plus quality-segment state reset at bad-date/long-gap boundaries.
- When comparing live logs to backtest, check placed-orders/day, fill/order probability, and guard/pause rates together. If fill probability per order is similar but placed-orders/day is much lower, inspect adverse/defense pause carry-over before changing strategy parameters.
- Before any formal sweep, run a small current-baseline mechanism smoke through the maintained racing/replay entrypoint. Do not call this optimal parameter selection. Freeze acceptable fills/activity, spread, pause/action mix, side split, queue/replace, inventory and campaign-tail budgets in the experiment manifest; do not inherit numerical bands from pre-repair reports.
- Current evidence policy: use `research/families/f10_live_replay_attribution/docs/historical_backtest_evidence_revalidation_20260720.md`, `research/system_engineering/docs/time_unit_contract_repair_20260726.md`, and `docs/value_provenance_registry_20260727.md` as the entry points. Treat `research/families/f10_live_replay_attribution/docs/operational_baseline_identity_20260720.json` and `research/families/f10_live_replay_attribution/docs/operational_baseline_identity_20260725.json` as frozen historical deployment identities. Pre-repair parameter grids, scorer values, null values and arm rankings must not be reconstructed from old result paths.
- Formal replay must bind current code, config, empirical P3, model, event-L2, initial state, queue artifact, latency profile and random seed. Missing formal model features or calibration inputs must fail fast.
- New strategy families use chronological Development, embargo, Validation and sealed holdout drawn from the current retained universe. Do not wait for future days by default, and do not reuse an opened holdout as confirmation.
- Action evidence requires one intervention per campaign, known behavior propensity, support/ESS checks, campaign-level reward attribution and a paired current baseline. Unsupported actions are diagnostic only.
- Current accepted strategy reports begin with the causal-v4 and 2026-07-18+ action-family documents. A negative result closes that exact family; do not turn it into a threshold search on the same evidence panel.
- Rolling live baseline rule: `baseline` means the currently deployed behavior and immutable identity, never a public demo config or a historical winner.

## Hybrid Adverse Markout Pause (Mandatory)
- Markout pause must be a bounded latch, not a permanent EMA sign check. The current parity-safe form is:
  - `pause_active = markout_ema < -pause_threshold and now < pause_until`
  - `pause_until = now + clamp(base_s * abs(markout_ema) / pause_threshold, min_s, max_s)`
  - `markout_ema(t) = markout_ema(last) * exp(-dt / tau_s)` before applying new markout observations.
- Extend `pause_until` only when a real fill markout resolves inside the allowed horizon. Drop pending markout observations that resolve after a long gap (`adverse_markout_max_resolve_gap_s`, currently 30s by default), because source outages or removed orderbook dates can otherwise create stale adverse state.
- Keep live and replay semantics aligned: Python quote core, live maker engine, backtest Python replay, and C++ replay all need the same `adverse_markout_pause_hybrid`, `base_s`, `min_s`, `max_s`, `decay_tau_s`, and stale-resolve-gap parameters.
- A `SYNC_ADJUST_DEGRADE` right after restart can temporarily make logs look one-sided. If exchange position sync changes local inventory, exposure-increasing quotes are paused for the configured window while reducing-side quotes remain allowed. Confirm whether the blocked side resumes after the window before diagnosing it as a quote-policy lockout.
- Pre-repair adverse threshold candidates and their PnL rankings have been deleted. Any guard retest must begin from the current operational baseline identity and a newly frozen action family.

## CryptoHFT Data Quality Rules (Mandatory)
- Treat `data_quality.py` as the code-level source for hard data exclusions in training, backtest, forward-test, queue/fill calibration, and live-vs-replay parity.
- The canonical audit table is `logs/data_audit/cryptohft_bad_days_20250801_20260624.csv`; rows with `source_unresolved_missing_objects` are source-side missing hourly objects, not local pipeline misses.
- As of 2026-05-27, BTCUSDT dates `2026-03-30` through `2026-04-09` returned source-side 404s after a credentialed rerun and must be treated as global hard-exclude dates for full-day training/backtest/parity metrics, including BTCUSDC runs that use BTCUSDT as a reference market.
- Raw-zero and normalized-missing days (`2026-03-31`, `2026-04-03`, `2026-04-04`, `2026-04-06`, `2026-04-07`) must be removed from training, replay, alignment, and any feature dataset used for model selection.
- Partial days with `raw_hours < 24` but normalized outputs present (`2026-03-30`, `2026-04-01`, `2026-04-02`, `2026-04-05`, `2026-04-08`, `2026-04-09`) may be inspected as partial-source diagnostics only; do not use them for fills/day, daily PnL, live-vs-replay parity, or parameter sweep conclusions.
- Tick replay must compute `n_days` from retained active seconds after exclusions; calendar-span denominators across removed days understate fills/day and PnL/day.
- Before retraining or retesting after data repair, regenerate features/predictions and remove stale sweep outputs so old contaminated rows cannot leak into winner selection.

## Binance Public Data Rules (Mandatory)
- Binance futures raw `trades` files preserve individual trade IDs/sequence and are useful for queue/fill calibration, toxic-flow labels, event-time intensity, and tick replay diagnostics. If live uses `aggTrade`, either aggregate raw trades to the same live schema or switch live/replay together; do not train on raw-trade-only fields that live cannot reproduce.
- Binance futures `daily/bookDepth` is only a coarse 30s-ish percent-depth aggregate. Do not use it for maker queue, exact L2, BBO event-cancel, or low-latency reference tests; use CryptoHFT exact BBO/L2 or live-captured bookTicker/diff-depth style data for those questions.
- Binance spot data is useful only as a live-computable cross-market anchor (`market_stage=enhanced` or `full`): spot-perp basis, lead/lag returns, spot flow imbalance, and spot liquidity/risk regime. Do not use delayed or unavailable spot fields in model training.
- Binance spot timestamps from 2025-01-01 onward are microseconds, while futures aggTrades/trades are millisecond-oriented in this pipeline. `features/preprocess.py` must normalize timestamp units before creating 1s bars; otherwise spot/perp alignment and cross-market features are invalid.
- For spot/perp experiments, validate timestamp alignment, symbol liquidity, and live WebSocket parity before trusting AUC/IC, fills/day, or parity conclusions.
- When `data_quality.py` or the CryptoHFT audit hard-excludes an orderbook date, Binance raw data for that date must be physically absent too. Do not leave a non-daily Binance `aggTrades` CSV that spans excluded days and rely only on downstream row filters.
- Preferred cleanup for Binance Vision data is: delete non-daily raw containers that overlap excluded orderbook dates, then refill only retained dates from daily Binance Vision files via `data/download_data.py --day-start ... --day-end ...` or the same downloader functions.
- The 2026-06-06 BTCUSDC/BTCUSDT cleanup audit lives in `logs/data_audit/cleanup_20260606_orderbook_quality/`: it downloaded 375 good daily `aggTrades` files, deleted 23 overlapping cross-day raw containers, and verified zero remaining bad-date daily hits and zero remaining non-daily `aggTrades` containers in the checked raw directories.
- The 2026-07-01 physical cleanup lives in `logs/data_audit/cleanup_20260701_bad_day_physical/`: using `data_quality.COMPLETE_DATA_POLICY.excluded_orderbook_days("BTCUSDC")` with cross-source exclusions, it deleted 664 bad-day files (~1.738GB) from `${NARROWGATE_DATA_ROOT}` across raw/BBO/L2/bars/depth/metrics/trade-feature layers and verified zero remaining bad-day file hits. Keep quality-valid daily good days as the OOS/research pool; do not shrink them to a tiny subset. Selection belongs in retained daily universes, not by leaving bad-date files physically present.
- The 2026-07-05 storage cleanup removed local 2025 MarketData artifacts/caches (~17.2GB) after the research baseline moved to 2026 retained/live-aligned daily evidence. Manifest: `logs/data_audit/cleanup_20260705_drop_2025/delete_manifest_2025_marketdata_predelete.csv`. BTCUSDT raw trades were then trimmed to the 111-day minimal complete BTCUSDC good-day universe, deleting 73 extra reference-only days (~15.85GB); manifest: `logs/data_audit/cleanup_20260705_align_btcusdt_reference/delete_manifest_btcusdt_raw_trades_not_good_20260705.csv`. Treat any 2025 references in old docs as archived historical context only; do not run current training/replay from 2025 unless the user explicitly asks to recreate that dataset.

## Market Roles and Historical Differences (Mandatory)
- `BTCUSDC` perpetual is the execution market; BTCUSDT and external venues are reference sources only.
- Read fees and exchange filters from the current config/exchange preflight. Do not preserve historical liquidity ratios or account-specific fee values as constants in research guidance.
- Any recommendation on spread, fill assumptions, or quote aggressiveness must state which market these assumptions target.
- Do not transfer `BTCUSDT` fill/fee assumptions directly to `BTCUSDC` (or vice versa) without explicit adjustment.

## Single-Repo Maintenance Policy (Mandatory)
- Active repo: `${NARROWGATE_ROOT}`.
- Retired/deleted repo: `${NARROWGATE_ROOT_BTCUSDT_ARCHIVED}`. Do not assume it exists.
- Legacy private BTCUSDT research repo names may still appear in old notes; treat them as `${NARROWGATE_ROOT_BTCUSDT_ARCHIVED}`-style archival references only, never as active execution outputs.
- There is no cross-repo sync checklist anymore. When modifying shared infrastructure, update this BTCUSDC repo and its docs/skill only.

## Documentation Freshness and LRU Policy (Mandatory)
- When materially creating or changing a Markdown document, add or update `Last materially modified: YYYY-MM-DD` next to its top-level date/status. Preserve the original experiment date: the modification tag records later maintenance and must not rewrite frozen experiment history.
- Advance the tag only after substantively reviewing the document's claims, links, units, and current-versus-historical language. Do not bulk-touch files or advance it for generated rebuilds, renames, timestamp cleanup, or formatting-only edits.
- Git commit, tree, and annotated-tag identities are the primary version authority for tracked public source and documentation. Use Git history and diffs to establish provenance, predecessor/successor relationships, releases, and rollback points; reconstructed-import commits must say that they are reconstructed rather than pretending to be the original execution commits.
- `Last materially modified` records the date on which the document's claims were last substantively reviewed. Filesystem mtime is only a local cleanup hint and is never authoritative across clones, checkouts, archives, or restored backups.
- Formal research, replay, build, and deployment work must start from a clean public commit or annotated tag. Record any authorized runtime overlay separately and never silently treat an uncommitted working tree as the canonical source identity.
- External data, private sources, model bundles, binaries, execution configs, and formal research contracts remain content-addressed, but only through one canonical root manifest per run or release. Its stable roots are the Git commit/tree, build artifact, config bundle, model bundle, dataset/source manifest, and research contract. A result receipt binds that root manifest plus the result digest; consumers must not copy the same leaf SHA into Python, tests, JSON, and prose.
- Git-tracked source and documentation use the Git commit/tree only. Do not add per-file SHA constants for tracked files. Cache digests are cache keys, not research or deployment authority. A mutable `current` pointer resolves an object and is never itself immutable evidence.
- Live startup validates one release-root identity. The release-manifest builder may validate its internal leaves once, but downstream runtime checks must bind the canonical root instead of re-declaring leaf tables. Unit tests use temporary synthetic artifacts and verify mismatch handling; they must never depend on the current owner artifact SHA.
- Interpret Git history, the document's material-modification tag, embedded experiment date, current references, code contracts, artifact manifests, and superseding conclusions together.
- Age is an audit-priority signal, never deletion proof. For each old code or document file, check imports, CLI/Makefile entrypoints, tests, configuration references, artifact manifests, and superseding conclusions. Classify it as current, historical/frozen, superseded-but-referenced, or deletion candidate before editing or deleting it.
- Do not mass-add modification tags before the first LRU inventory, because doing so destroys the very freshness ordering being audited. Preserve all unrelated local edits and frozen evidence.
- Do not add source files, tests, deployment wrappers, receipts, or documents for a one-off run when an existing configurable entrypoint can perform it. Search callers and current generic primitives first. New code must represent a reusable contract or a genuinely new mechanism, not the latest attempt at an operational step.
- When similar one-off implementations accumulate, move their shared behavior into the current generic module, migrate live callers, and remove superseded code and tests after a reference audit. Tests should protect stable generic behavior; do not create a new test file for every small deployment or parameter change.
- Do not encode implementation retries as `vN`, `vN+1`, `successor`, or `amendment` source/test filenames. Edit the canonical implementation in place. Versioned artifacts are reserved for immutable research evidence, external schemas, or compatibility migrations whose older identity still has an active consumer; Git already preserves ordinary code history.
- Cache is the lowest storage tier. During an authorized cleanup, delete reproducible cache entries older than 14 days unless an active process owns them or a current manifest explicitly pins them. Never apply this TTL to raw source data, shared normalized market data, private operational evidence, or frozen final research results.

## Language and Translation Contract (Mandatory)
- Keep agent-only instructions in English. This includes `.github/skills/**/SKILL.md`, skill `references/`, agent metadata, and owner-installed NarrowGate skill packages. Preserve code identifiers, paths, formulas, hashes, and protocol tokens exactly; do not create translated copies of machine-consumed instructions.
- Maintain current human-facing guides as language pairs: `name.md` is the canonical English document and `name.zh-CN.md` is its Simplified Chinese counterpart. Put reciprocal English and Simplified Chinese language links near the top, and keep one prose language per body except for code, identifiers, paths, formulas, proper nouns, and necessary first-use technical terms.
- Update both documents in the same change whenever status, conclusions, safety boundaries, commands, links, or other substantive meaning changes. Record the same `Last materially synchronized: YYYY-MM-DD` value in both files. A translation may be idiomatic, but it may not omit or weaken material claims.
- Do not translate JSON, YAML, CSV, manifests, receipts, generated records, or hash-bound frozen evidence. Both language guides must cite the same underlying artifact identity. Keep immutable historical Markdown in its original language when translation would disturb evidence identity; provide a bilingual maintained index or reader summary instead of duplicating the frozen record.
- Do not mass-copy the historical documentation tree. Prioritize README, quickstart, architecture, contributor, security, current operations, and research-navigation guides. Add or repair a language pair when another maintained human guide is materially edited.

## Public / Private Config Boundary (Mandatory)
- `live/config.yaml` in the public repo is a safe template, not the current live parameter snapshot.
- Private live configs live outside published docs, normally under ignored `docs/private/`, and must be passed explicitly with `NARROWGATE_LIVE_CONFIG=<private-config.yaml>`.
- Daily/campaign/baseline reports may record private config hash and model bundle labels, but must not publish full parameter snapshots, private hostnames, PIDs, raw live PnL, or absolute local paths.
- When comparing baseline vs arm, baseline means the current private live config + model + code version, not the public template.
- Before editing documentation, read [`docs/path_conventions.md`](../../../docs/path_conventions.md) and [`docs/public_private_documentation_contract.md`](../../../docs/public_private_documentation_contract.md). Treat human-facing Markdown as public unless it is below ignored `docs/private/`.
- Public reports must use repository-relative links, approved placeholders, logical deployment epochs, and explicit artifact availability. A SHA256 value identifies bytes but is never a substitute for a reader-accessible link or an honest `private evidence store; not distributed with the public repository` label.
- Cross-project private runtime pointers belong under ignored `docs/private/`; component-local evidence belongs to ignored `live/private/`, `data/private/`, `models/private/`, or `execution/private/`; research-specific locators and owner-only evidence indexes belong under the owning concrete research unit's ignored `private/`, following [`research/PRIVATE_EVIDENCE.md`](../../../research/PRIVATE_EVIDENCE.md) and the [`non-research owner map`](../../../docs/non_research_private_evidence_owners.md). Private Markdown must begin with `Local only — do not publish.` Private JSON/YAML normally declares `local_only_do_not_publish`; exact byte-preserved historical sources and schema-constrained runtime/config records may inherit classification from their ignored directory marker and catalog when adding a field would break identity or consumer compatibility. Keep exact host, storage, account, and secret-bearing locator details there, never in public reports. Treat `panel_role=historical_or_operational_unspecified` as fail-closed for Development, Validation, and holdout.
- A SHA may remain public only with a named artifact, explicit identity kind, and availability. Never bind an executed/private-source SHA to a public projection path as though the bytes were identical; use the projection-aware resolver. A machine record below a Git-ignored model bundle is `private_working_tree_projection_not_distributed`, not a public repository artifact.
- A SHA proves byte identity only. It does not prove data correctness, parameter reasonableness, leakage freedom, economic value, live-process health, order ownership, or exchange reconciliation. Require separate research, economic-authority, and runtime-health gates for those claims.
- Before handoff, run `scripts/audit_public_documentation.py` and, on an authorized owner checkout, `scripts/audit_private_evidence.py`. Public archive members, structured process identifiers, private locators, projection publishability, private catalog hashes, permissions, and source/projection dual identities must all pass.

## Python Interpreter Rule (Mandatory)
- The repository contract is Python `>=3.11`. For every local Python command, first set `PYTHON="${NARROWGATE_ROOT}/.venv/bin/python"`; from the repository root this is `.venv/bin/python`.
- Before tests, scripts, downloads, preprocessing, training, replay, lint, or compilation, fail fast unless `$PYTHON` exists, is executable, and reports Python 3.11 or newer. A newer project venv is valid; do not require the minor version to equal 3.11.
- Never try bare `python`, bare `python3`, or `/usr/bin/python3` first. The local macOS system interpreter is Python 3.9 and is unsupported. Do not run under 3.9 and only then retry with the venv.
- Use `$PYTHON -m pytest`, `$PYTHON -m pip`, `$PYTHON -m ruff`, `$PYTHON -m py_compile`, and `$PYTHON path/to/script.py`. If the venv is missing or below 3.11, report the environment problem; do not silently fall back or rebuild it.
- On the current remote live host, resolve the deployed NarrowGate venv and apply the same `>=3.11` preflight rather than assuming the local macOS path.

## Local macOS Command Rule (Mandatory)
- For local long-running, batch, download, preprocessing, training, or backtest commands, wrap the resolved project interpreter with macOS `caffeinate` so it keeps running if the display sleeps, for example: `PYTHON="${NARROWGATE_ROOT}/.venv/bin/python"; caffeinate -dimsu "$PYTHON" models/backtest_tick.py ...`.
- Do not use `caffeinate` for remote live-host process control; live process restarts/stops still require explicit user confirmation.

## Canonical Modeling Stack
1. AS-shaped empirical controller
- Normalize inventory explicitly: `n = q / q_ref`, where `q` and `q_ref` use the same base-asset denomination and `n` is dimensionless.
- Reservation controller: `r = fair - n * eta_inventory_eff * sigma_sq_per_s * inventory_risk_horizon_s`.
- Empirical pair-spread controller: `pair_spread = a_spread * sigma_sq_per_s * quote_horizon_s + (2/a_spread) * log(1 + a_spread/kappa_spread)`.
- Keep order quantity `z`, `q_ref`, `eta_inventory`, and `a_spread` explicit. The legacy `gamma` mapping exists only to reproduce frozen B0 behavior; it is not a portable CARA coefficient and cannot be moved across order size, account capital, symbol, or base denomination without new evidence.
- Admit the unit-contract migration as B0-equivalent only when final bid/ask, the historical P3 pair-spread floor, post-only correction, and tick rounding are identical. This repair must not change live quotes.
- Never call this pair-spread expression the AS or GLFT optimal spread. Avellaneda--Stoikov's displayed approximation and the current controller are different mathematical objects.

2. Empirical P3 touch identity
- Load `delta_star` and the legacy field `effective_kappa` only from an identity-bound P3 artifact.
- Preserve `event=touch`, fixed horizon, `origin=same_side_bbo`, price-distance unit, side scope, `queue_included=false`, and artifact identity through every consumer boundary.
- Interpret `effective_kappa` only as the local touch-curve slope `-d log(P_touch)/d distance`. It is not order-arrival intensity, fill hazard, touch-to-fill conversion, or GLFT `kappa`.
- The historical `2 * delta_star` projection is a symmetric pair-spread floor. It does not guarantee that each final quote is at least `delta_star` away from its same-side BBO after reservation shift, asymmetry, tick rounding, post-only correction, and caps.

3. ML spread and skew adapter
- Predict short-horizon risk/edge (direction, return, volatility, toxicity).
- Convert predictions into:
  - spread multiplier (wider when risk high),
  - reservation-price shift (skew),
  - side gating/defense when toxicity is elevated.

4. Required microstructure features
- Realized volatility: rolling sigma/sigma_sq across short windows.
- Weighted-mid proxy: top-N sizes combined with best bid/ask prices; keep the legacy `microprice` field only for ABI compatibility and never describe it as Stoikov's recursive micro-price estimator.
- Clock-volume imbalance: wall-clock signed-volume windows; keep legacy `vpin_*` fields only for ABI compatibility and never describe them as equal-volume-bucket VPIN.
- Trade-intensity-burst guard: fast/slow event-rate EMA ratio; keep legacy `ber_*` fields only for ABI compatibility and never describe it as a book-exhaustion BER estimator.
- Other toxicity proxies: adverse-selection labels and markout-aware risk scores. A proxy's name or prediction quality does not grant quote authority.

## Procedure
1. Confirm scope
- BTCUSDC execution symbol, date range, and optional BTCUSDT/reference-source stage.
- Objective: design, code audit, tuning plan, or revalidation.

2. Trace data lineage
- Verify raw -> bars -> features -> predictions -> backtest/live path.
- Ensure feature parity between training and live-computable fields.
- Check `data_quality.py` and the bad-day audit before trusting fills/day, daily PnL, or parity metrics.

3. Form quote equation layers
- Layer A: empirical inventory center and pair spread from `sigma_sq`, explicit horizons, `q_ref`, `eta_inventory`, `a_spread`, and the named spread adapter.
- Layer B: ML spread multiplier from vol/toxicity regime.
- Layer C: skew and side-policy from direction/ret/imbalance/toxicity.

4. Define safeguards
- Fee floor + tick floor.
- Inventory cap and urgency controls.
- Side-specific pause/defend logic under toxicity or markout stress.

5. Evaluate in correct order
- Tick replay first for microstructure realism.
- Bar-level tests for coarse screening only.
- Report candidate uplift with PnL, campaign tail, inventory stress, fill quality, overlap/ESS and confidence intervals; do not name a sample winner without the complete gate.

6. Produce implementation artifact
- If coding: patch config/strategy/backtest paths with parity checks.
- If planning: deliver one small preregistered candidate family, acceptance criteria, and rollback rules. Do not turn unit repair into an unrestricted parameter grid.

## Output Contract
Always return:
- Assumptions and target symbol/stage.
- Final quote decomposition (AS-shaped empirical controller + named empirical adapters + optional ML adjustments).
- Unit and clock identities for price, base quantity, variance rate, risk horizon, P3 horizon, and lifecycle exposure.
- Feature set using the truthful names `weighted_mid_proxy`, `clock_volume_imbalance`, and `trade_intensity_burst_guard`, with any legacy ABI names identified separately.
- Validation plan (tick period, metrics, pass/fail gates).
- Exact files to change (if any) and why.

## Constraints
- Require a coherent semantic/unit contract before implementation parity. Parity proves that two implementations agree under stated assumptions; it does not prove the assumptions, units, or strategy economics are correct.
- Do not use features in training that cannot be reproduced online.
- Keep side controls explainable (loggable reason masks/policies).
- Prefer robust parameter regions over single-point best rows.
- Treat a finite-order-size/quantity-aware spread, a true per-side same-side-BBO floor, an H5/H10 risk horizon, and a variance-time cooldown as separate behavior-changing research candidates. None has economic, action, or live authority merely because it improves dimensional interpretation.
- Treat fixed base-asset quantity limits and fixed USDC notional, loss, or drawdown limits as independent hard fuses, with the stricter applicable constraint binding. They are not one scale-invariant risk coordinate; an equity/volatility-aware replacement is another candidate and cannot silently remove them.
- Treat BUY E3 and the SELL owner cooldown, wherever owner-side evidence references them, as owner-authorized live risk experiments rather than research-hard-gate passes. They are not validated strategy optima, and public documentation must not infer whether either experiment is currently active.

## Quick Prompt Examples
- "Use mm-as-glft-ml-spread to audit the BTCUSDC empirical quote controller's inventory, order-size, and risk-horizon units."
- "Use this skill to audit whether current backtest_tick and live quote policy are parity-safe."
- "Use this skill to test one preregistered negative-filter candidate against the frozen baseline without relabelling P3 touch slope as GLFT intensity."
