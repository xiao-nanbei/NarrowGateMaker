---
name: mm-as-glft-ml-spread
description: 'Design, review, or tune the NarrowGate BTCUSDC market-making strategy that combines Avellaneda-Stoikov (AS), Gueant-Lehalle-Fernandez-Tapia (GLFT), and ML-based spread adaptation using realized volatility, buy/sell volume imbalance, and microstructure toxicity/VPIN features. Use for quote logic design, feature mapping, data-quality exclusions, backtest-to-live parity checks, and parameter sweep plans in the BTCUSDC execution project; BTCUSDT is reference/source data only.'
metadata:
  short-description: 'Research and validate NarrowGate maker actions'
---

# MM AS+GLFT+ML Spread Skill

Last materially modified: 2026-08-12

## Goal
Build or evaluate an inventory-aware market-making workflow where:
- AS is the baseline quote engine.
- GLFT-style intensity modeling refines optimal spread under fill probability assumptions.
- ML (LSTM/GRU/Transformer or existing tabular models) adapts spread and skew from short-horizon signals.
- Core state inputs include realized volatility, buy/sell imbalance, and toxicity/VPIN-like microstructure features.

## Current Scope (Mandatory)
- The only actively maintained execution/maker project is `BTCUSDC` in `${NARROWGATE_ROOT}`.
- `${NARROWGATE_ROOT_BTCUSDT_ARCHIVED}` is retired/deleted locally. Do not instruct edits, syncs, tests, deployments, README updates, or live restarts in that repo unless the user explicitly recreates it and asks for archival work.
- BTCUSDT remains valid only as BTCUSDC reference/source data: reference perp features, source-quality exclusions, stale-anchor checks, basis/lead-lag analysis, and historical context.
- Binance BTCUSDT futures raw `daily/trades` is the active public reference trade source when a BTCUSDT trade stream is needed. As of 2026-07-05 local BTCUSDT raw-trade coverage is aligned to the 111-day BTCUSDC minimal complete good-day universe (`logs/data_audit/cleanup_20260705_align_btcusdt_reference/minimal_complete_good_days_2026.csv`), not the full calendar. Binance futures `daily/bookTicker` is not usable for current 2026 work because the public directory stops at 2024-03-30; Binance `bookDepth` is coarse percent-bucket depth and must not be treated as event-level BBO/L2 or queue data.
- BTCUSDT reference evidence must be generated per retained UTC day, using the same good-day manifest as BTCUSDC replay. Do not run or present BTCUSDT ref evidence as month chunks. Cross-day/month rollups are only secondary summaries of daily rows and must preserve the day-level support/pass/fail columns.
- Do not transfer BTCUSDT execution parameters, fees, model bundles, fills/day, or PnL conclusions into BTCUSDC decisions without a fresh BTCUSDC daily replay/validation.
- Live fill calibration口径：`maker` 单指 NarrowGate live 程序自己报出的被动限价单成交；`taker` 单指用户手动下的主动/人工订单。比较 live 与 replay baseline 的 fills/day、BUY/SELL split、VWAP、markout、campaign outcome 时，只能使用程序 maker fills；手动 taker rows、`SYNC_ADJUST`、人工平仓/干预成交必须单独标记或剔除，不能混入 maker fill-selection 证据。
- Timezone口径：Binance/CryptoHFT 数据文件、训练、daily replay、baseline day key 默认都是 UTC day。若用户说“昨晚”“早上”“UTC+8 自然日”或用交易所 UI 的本地时间截图，必须先转换成明确 UTC start/end，并用同一 UTC 窗口同时切 live 和 replay；不能用 UTC daily baseline 直接对比 UTC+8 自然日。
- Live/baseline 单日校准如果 live 在窗口起点不是 flat，必须把 day-start `initial_inventory` / `initial_entry_price` 从 live `trades.csv` 或 HEALTH 状态传入 replay，并让 campaign/fill split 从该初始仓位开始算。正式 retained daily OOS 仍默认 fresh-start；不要把 carry inventory 用进策略晋级证据，除非研究问题明确是 live episode alignment。
- 小时级 live episode 对齐时，优先用起点前 10-30 分钟日志恢复 `initial_inventory` / `initial_entry_price`、campaign age/MAE/counters、last fill cooldown；`ORDER_UPDATE` 重放用于审计 active orders 并和最近 HEALTH `orders=` 做 sanity check。不要默认把 active orders 注入 replay 成交路径，因为缺少真实 queue ahead、ACK/pending/cancel race 状态时会制造新的假 fill selection；只有显式 `restore_active_orders`/exchange-lifecycle 研究才允许这样做，并必须单独报告。
- Live/baseline 机制校准不能只看 fills/day。必须同时报告 BUY VWAP、SELL VWAP、`side_vwap_edge = SELL_VWAP - BUY_VWAP`、`edge_diff_bps` 和 `edge_diff_usdc = (live_edge - replay_edge) * matched_qty`。若 side VWAP edge 差异已经达到会改写日 PnL 的量级，则 raw/InvAdj、arm PnL、campaign terminal PnL 都只能作为机制失真诊断，不能作为 promotion evidence。
- Live/baseline 校准必须确认 replay 使用的主模型目录等于 live `ml.model_dir`。当前 live baseline 的主模型目录来自 `live/config.yaml`，例如 `${NARROWGATE_MODEL_DIR}`；`models/backtest_config.py::load_tick_base_params()` 应通过 `model_dir_override` 传给 `backtest_tick.configure_symbol()`。若 replay 仍落到默认 `models/saved_btcusdc`，dir/vol/ret/tox 模型会不同，小时级 fill/VWAP/PnL 对齐不可信。

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
- AS or GLFT quote design and calibration.
- Dynamic spread logic from short-term volatility forecasts.
- Feature engineering around imbalance, trade arrival, and toxicity.
- Backtest/live consistency checks for maker strategy.
- A parameter sweep plan for spread, skew, fill, or cooldown controls.

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
  - AS quote logic with ML enhancements.
  - Risk checks, exit/position-timeout handling, cancel-only operational shutdown, and separate emergency drawdown handling.
  - Keep behavior aligned with shared strategy helpers and formal
    `models/backtest_tick.py` replay when proposing logic changes.
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
- External data, private sources, model bundles, binaries, execution configs, reports, and live epochs remain content-addressed by their SHA256 manifests. A Git commit SHA identifies the tracked public tree; it does not replace artifact SHA256 or prove that private/external bytes were present.
- Interpret Git history, the document's material-modification tag, embedded experiment date, current references, code contracts, artifact manifests, and superseding conclusions together.
- Age is an audit-priority signal, never deletion proof. For each old code or document file, check imports, CLI/Makefile entrypoints, tests, configuration references, artifact manifests, and superseding conclusions. Classify it as current, historical/frozen, superseded-but-referenced, or deletion candidate before editing or deleting it.
- Do not mass-add modification tags before the first LRU inventory, because doing so destroys the very freshness ordering being audited. Preserve all unrelated local edits and frozen evidence.

## Public / Private Config Boundary (Mandatory)
- `live/config.yaml` in the public repo is a safe template, not the current live parameter snapshot.
- Private live configs live outside published docs, normally under ignored `docs/private/`, and must be passed explicitly with `NARROWGATE_LIVE_CONFIG=<private-config.yaml>`.
- Daily/campaign/baseline reports may record private config hash and model bundle labels, but must not publish full parameter snapshots, private hostnames, PIDs, raw live PnL, or absolute local paths.
- When comparing baseline vs arm, baseline means the current private live config + model + code version, not the public template.
- Before editing documentation, read [`docs/path_conventions.md`](../../../docs/path_conventions.md) and [`docs/public_private_documentation_contract.md`](../../../docs/public_private_documentation_contract.md). Treat human-facing Markdown as public unless it is below ignored `docs/private/`.
- Public reports must use repository-relative links, approved placeholders, logical deployment epochs, and explicit artifact availability. A SHA256 value identifies bytes but is never a substitute for a reader-accessible link or an honest `private evidence store; not distributed with the public repository` label.
- Cross-project private runtime pointers belong under ignored `docs/private/`; component-local evidence belongs to ignored `live/private/`, `data/private/`, `models/private/`, or `execution/private/`; research-specific locators and owner-only evidence indexes belong under the owning concrete research unit's ignored `private/`, following [`research/PRIVATE_EVIDENCE.md`](../../../research/PRIVATE_EVIDENCE.md) and the [`non-research owner map`](../../../docs/non_research_private_evidence_owners.md). Private Markdown must begin with `Local only — do not publish.` Private JSON/YAML normally declares `local_only_do_not_publish`; exact byte-preserved historical sources and schema-constrained runtime/config records may inherit classification from their ignored directory marker and catalog when adding a field would break identity or consumer compatibility. Keep exact host, storage, account, and secret-bearing locator details there, never in public reports. Treat `panel_role=historical_or_operational_unspecified` as fail-closed for Development, Validation, and holdout.
- A SHA may remain public only with a named artifact, explicit identity kind, and availability. Never bind an executed/private-source SHA to a public projection path as though the bytes were identical; use the projection-aware resolver. A machine record below a Git-ignored model bundle is `private_working_tree_projection_not_distributed`, not a public repository artifact.
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
1. AS baseline
- Reservation price: `r = s - q * gamma * sigma_sq_per_s * horizon_s`
- Base spread uses the same explicit variance horizon plus the calibrated intensity term. Never mix absolute price variance with return variance.

2. Empirical P3 / GLFT-style intensity
- Load `delta_star` and `effective_kappa` from the current hashed P3 artifact.
- Treat horizon and fill definition as part of the calibration identity.

3. ML spread and skew adapter
- Predict short-horizon risk/edge (direction, return, volatility, toxicity).
- Convert predictions into:
  - spread multiplier (wider when risk high),
  - reservation-price shift (skew),
  - side gating/defense when toxicity is elevated.

4. Required microstructure features
- Realized volatility: rolling sigma/sigma_sq across short windows.
- Volume imbalance: buy/sell aggressive volume ratio and depth imbalance proxy.
- Toxicity/VPIN proxies: signed flow imbalance, adverse selection labels, markout-aware risk scores.
- Trade arrival intensity: event-rate features for liquidity regime adaptation.

## Procedure
1. Confirm scope
- Symbol, date range, and execution mode (BTCUSDC or BTCUSDT; minimal/enhanced market stage).
- Objective: design, code audit, tuning plan, or revalidation.

2. Trace data lineage
- Verify raw -> bars -> features -> predictions -> backtest/live path.
- Ensure feature parity between training and live-computable fields.
- Check `data_quality.py` and the bad-day audit before trusting fills/day, daily PnL, or parity metrics.

3. Form quote equation layers
- Layer A: AS/GLFT base spread from sigma_sq, gamma, kappa.
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
- If planning: deliver parameter grid, acceptance criteria, and rollback rules.

## Output Contract
Always return:
- Assumptions and target symbol/stage.
- Final quote decomposition (AS base + GLFT/intensity + ML adjustments).
- Feature set used for realized vol, imbalance, toxicity/VPIN.
- Validation plan (tick period, metrics, pass/fail gates).
- Exact files to change (if any) and why.

## Constraints
- Prioritize backtest-live parity over raw in-sample uplift.
- Do not use features in training that cannot be reproduced online.
- Keep side controls explainable (loggable reason masks/policies).
- Prefer robust parameter regions over single-point best rows.

## Quick Prompt Examples
- "Use mm-as-glft-ml-spread to design a BTCUSDC spread adapter with realized vol + VPIN gating."
- "Use this skill to audit whether current backtest_tick and live quote policy are parity-safe."
- "Use this skill to propose a retained-day sweep grid for gamma, p3_kappa_eff, kappa_ratio, depth_kappa_ratio, vol_blend, toxicity spread scaling, and cooldown."
