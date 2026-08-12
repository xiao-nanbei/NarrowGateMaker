# Audit Entrypoints - 2026-06-30

Last materially modified: 2026-07-31

> Current status (2026-07-27): this is a command/reference catalog accumulated across several research generations. The shared audit modules remain useful, but historical numerical examples and retired action-family commands below are not current evidence. Formal new work must bind the normalized 100ms L2, native queue source where required, repaired trade-side identity, merged replay clock, current unit contract, frozen family spec and score profile. Use `research/system_engineering/docs/time_unit_contract_repair_20260726.md`, `docs/normalized_l2_100ms_v2_20260725.md` and `docs/experiment_scorecard_v1_20260722.md` as the governing contracts.

默认 routine audit 入口：

```bash
python -m research.families.f10_live_replay_attribution.audit.runner \
  --symbol BTCUSDC \
  --log-dir logs \
  --start <UTC-ISO> \
  --end <UTC-ISO> \
  --reports campaign,campaign_policy_replay,fill_selection,quote_decisions,inventory_shadow,order_level,daily_gate \
  --out-prefix <output-prefix>
```

共享口径集中在：

- `models/audit/schema.py`：timestamp、UTC day、side、session；
- `models/audit/loaders.py`：CSV/log loaders；
- `research/families/f10_live_replay_attribution/audit/metrics.py`：campaign、fill/order denominator、quote decision、inventory shadow、order-level table、daily hard gate；
- `research/families/f10_live_replay_attribution/audit/runner.py`：统一 CLI 与输出。

## Action-Level Offline Policy Evaluation

`order_level` 与 fill-selection score 用于状态排序；它们不等价于 action uplift。正式的 action-level 入口是：

```bash
python -m research.families.f09_campaign_action_uplift.audit.offline_policy_evaluation \
  --panel-csv <action-panel.csv> \
  --out-prefix <output-prefix> \
  --feature side \
  --feature inventory_role \
  --feature inventory_ratio \
  --feature campaign_age_s \
  --feature queue_local_rank \
  --split-mode chronological \
  --min-train-days 30 \
  --embargo-days 1 \
  --test-days 10
```

它 cross-fit behavior propensity 与 action-specific `Q(x,a)`，输出 DM、clipped IPS/SNIPS、doubly robust value、day-cluster bootstrap、ESS、candidate unsupported mass 和 action support。默认 causal feature registry 拒绝未来 markout、fill outcome 和 terminal campaign 字段。

输入必须是一行一个独立 decision 的完整 action panel。现有 placed-only `order_level.csv` 不能识别 `pause/skip/re-center` 的价值；候选动作没有 behavior support 时，报告会 fail overlap gate，而不是用回归外推伪造 uplift。完整边界见 [`offline_policy_evaluation_20260712.md`](../research/families/f09_campaign_action_uplift/docs/offline_policy_evaluation_20260712.md)。

已知微随机化概率应写成完整的 `behavior_prob_<action>` 向量；OPE 会逐行核对总和、所选 action 的正概率，以及 `behavior_propensity` 是否与向量一致，不会重新拟合或覆盖真实 propensity。

已退役 action family 的 plumbing 回归入口，以及当前 local action panel 入口分别是：

```bash
# fill cooldown 的 R0/R1/R2 实际随机化 replay
python -m research.families.f09_campaign_action_uplift.audit.safe_add_rearm_randomized --help

# 校验 replay 已生成的真实 intervention rows
python -m research.families.f09_campaign_action_uplift.audit.safe_add_rearm_ope_panel --help

# 多 elapsed 只看 assignment/submit/fill support，并冻结 action family
python -m research.families.f09_campaign_action_uplift.audit.safe_add_rearm_support_preflight --help

# 对冻结 family 做 chronological DR + dev-trained later holdout
python -m research.families.f09_campaign_action_uplift.audit.safe_add_rearm_outcome_report --help

python -m research.families.f09_campaign_action_uplift.audit.safe_add_rearm_state_policy --help

# local exact-L2/queue/flow 的完整随机化 replay
python -m research.families.f09_campaign_action_uplift.audit.local_action_uplift --help

# 对冻结 panel 做 clipping/ESS/tail-support 敏感性审计
python -m research.families.f09_campaign_action_uplift.audit.local_action_ope_report --help
```

`safe_add_rearm_randomized` 每个 campaign 最多干预一次。R0 保持 cooldown，R1 只恢复一次 baseline add quote，R2 再把该 add quote 远离盘口一 tick；三者写入完整、严格为正的 behavior probability vector。旧 shadow probe 仍可用于诊断 would-fill，但不再由 `safe_add_rearm_ope_panel` 事后随机化，也不能进入 action-uplift OPE。

`safe_add_rearm_randomized --support-only` 只运行 action-bearing randomized replay，不额外运行 control，并从持久化 panel 移除所有 PnL/reward/markout/ terminal 字段。旧 fixed-elapsed rearm 的 outcome 数值已随旧 replay identity 删除；这些命令仅保留用于测试 action assignment、propensity 和 support plumbing，不得作为当前策略证据。

`local_action_uplift` v1 每个 campaign 最多一次干预，只作用于没有 active/pending same-side order 的 exposure-increasing `add` quote。它不使用 external reference，不改变 order size、inventory limit 或 reducing side。`opener/reducing` 在该版本没有 action overlap，只能标记为 unsupported，不能从 add-side 结果外推。

`order_level` 是从孤立 bucket 回到报价决策函数的主表。它每行对应一笔 placed order，并合并：

- quote-time state：side、price、mid、distance、spread/size multiplier、guard reason、L2 depth/refresh/cancel/flip、quote EV shadow 字段；
- outcome：是否成交、fill age、1s/5s/20s/30s maker-signed markout；
- campaign risk：campaign age、max inventory、MAE、是否 exposure-increasing；
- inventory intent：quote-time `inventory_role=opener/add/reducing`，以及只作事后诊断的 `fill_inventory_role` / `inventory_role_drift`；
- shadow policy flags：`inv006`、`age60m`、reducing-only；
- explainable score：`fill_probability_score`、`fill_quality_score`、`fill_probability_score`、`fill_quality_score`、`toxic_risk_score`、`campaign_risk_score`、`campaign_outcome_risk_score`、`resiliency_score`。

输出：

- `<prefix>.order_level.csv`
- `<prefix>.order_level_scores.csv`

这些 score 是研究/校准轴，不是 live policy。后续任何 tiny arm 都应该先说明它在 `order_level` 表里如何改变 spread、skew 或 lifecycle，而不是直接引用某个孤立 bucket。

`order_level_score_audit` 是第一层 sanity check：它不找 live 参数，只检查五个 score 是否按预期排序 outcome：

- `fill_probability_score`：高分是否提高 fill rate；
- `fill_quality_score`：高分是否改善成交后 markout；
- `toxic_risk_score`：按 side/day-side quantile 分桶后，p95 是否更能捕捉 tail/toxic；
- `campaign_risk_score`：高分是否对应更长 campaign age / 更大 max inventory；
- `campaign_outcome_risk_score`：高分是否对应更差 terminal campaign label、terminal PnL、tail-loss rate 或 early drawdown；
- `resiliency_score`：仅作 diagnostic，高分至少不能系统性更 toxic。

然后再把连续状态映射到三类 shadow knob：

- spread：`spread_widen_or_stop_add` / normal keep；
- skew：朝减库存方向偏移；
- lifecycle：缩短 TTL、加 cooldown、停止加仓。

示例，直接读 live/replay log 构建表并审计：

```bash
python -m research.families.f10_live_replay_attribution.audit.runner \
  --symbol BTCUSDC \
  --log-dir logs \
  --start <UTC-ISO> \
  --end <UTC-ISO> \
  --reports order_level,order_level_score_audit \
  --out-prefix <output-prefix>
```

示例，复用已有 order-level 表：

```bash
python -m research.families.f10_live_replay_attribution.audit.runner \
  --symbol BTCUSDC \
  --order-level-csv <prefix>.order_level.csv \
  --reports order_level_score_audit \
  --out-prefix <score-output-prefix>
```

示例，把 tick replay trace 转成相同 order-level schema：

```bash
python -m research.families.f10_live_replay_attribution.audit.runner \
  --symbol BTCUSDC \
  --replay-orders-csv <tick_quote_decomposition.*.orders.csv> \
  --replay-fills-csv <tick_quote_decomposition.*.fills.csv> \
  --reports order_level,order_level_score_audit \
  --out-prefix <replay-score-output-prefix>
```

Replay trace 的 order-level path 已经会从 prior fills 重建 campaign 状态：`campaign_age_s`、`campaign_duration_s`、`campaign_max_abs_qty`、`campaign_total_pnl`、`campaign_adverse_excursion`、加仓/减仓 fill count，以及 `shadow_block_inv006`、`shadow_block_age60m`、reducing-only shadow flags。它仍不是完整 counterfactual policy replay，但已经可以用于 campaign risk score sanity。

Python/C++ fill trace 现在都显式输出 `inventory_before_fill` / `inventory_after_fill` 和 20s markout/EV/toxic 字段。旧 trace 会按 UTC daily fresh-start 的完整 fill 序列因果重建 fill-time role，并将来源标成 `reconstructed_daily`；新 trace 标成 `exact_trace`。

Retained-all 大面板不要直接用 generic runner 同时跑 `order_level,order_level_score_audit`：multi-million-row Python dict table 会很慢、很吃内存。推荐两步：

```bash
python -m research.families.f10_live_replay_attribution.audit.runner \
  --symbol BTCUSDC \
  --replay-orders-csv <tick_quote_decomposition.*.orders.csv> \
  --replay-fills-csv <tick_quote_decomposition.*.fills.csv> \
  --reports order_level \
  --out-prefix <replay-score-output-prefix>

python -m research.families.f05_fill_quality_quote_ev.audit.order_score_fast \
  --order-level-csv <replay-score-output-prefix>.order_level.csv \
  --replay-fills-csv <tick_quote_decomposition.*.fills.csv> \
  --replay-orders-csv <tick_quote_decomposition.*.orders.csv> \
  --out-prefix <replay-score-output-prefix>
```

`order_score_fast` streams the cached order-level CSV and writes:

- `<prefix>.score_buckets_fast.csv`
- `<prefix>.score_daily_fast.csv`
- `<prefix>.score_calibration_fast.csv`
- `<prefix>.score_sanity_fast.csv`
- `<prefix>.score_daily_pass_summary_fast.csv`

When `--replay-fills-csv` is provided, the fast path reconstructs terminal campaign labels from replay fills and joins them by `(day, campaign_id)` while streaming the order-level table.  This avoids rewriting multi-GB order-level CSVs only to add `terminal_*` fields, while still reporting campaign terminal PnL, repair rate, bad-label rate, outcome-risk target, tail-loss rate, and early drawdown in score sanity outputs.

When `--replay-orders-csv` is provided, the fast path overlays replay-side quote-time fields onto the cached order-level rows by `(day, order_id)`: direct queue (`queue_init`, `queue_left`, `queue_local_rank`, queue multipliers), exact-L2 spread, fill eligibility, TTL budget, and observed filled lifetime. `observed_lifetime_ms` is reported only as a calibration target; it must not be used inside quote-time score definitions.  The calibration CSV currently reports fixed score deciles for `campaign_outcome_risk_score` and `fill_probability_score`, so those scores can be read as rank/calibration axes instead of live probabilities.

`fill_selection_score` 是下一层监督校准入口。它读取同一张 `order_level` denominator 表，但监督标签只来自已成交订单。未成交订单不会被当成负样本，它们只保留在 denominator 里，用于报告 score bucket 下的 orders、fills、fill rate 和 campaign outcome。

2026-07-06 之后，默认研究目标不再是单纯的“非 toxic 二分类”。新的主问题是：

```text
这笔 actual fill 是否打赢同 UTC day / same side 的 random opportunity？
或者它是否进入一个 terminal campaign PnL / repair 更好的库存 campaign？
```

这样做的目的，是直接训练“能否缩小 actual-vs-random selection gap”，而不是继续调一个容易误导的 weak non-toxic score。

示例：

```bash
python -m research.families.f05_fill_quality_quote_ev.audit.fill_selection_score \
  --order-level-csv <prefix>.order_level.csv \
  --out-prefix <fill-selection-output-prefix> \
  --folds 5 \
  --min-markout-20s-bps 0.0 \
  --min-markout-30s-bps -2.0
```

可选 target：

```text
--target-mode non_toxic
    旧标签：20s/30s markout/tail/campaign bad。

--target-mode beats_opportunity
    filled order 的 markout 打赢同日同侧 random opportunity benchmark。

--target-mode campaign_repair
    filled order 所在 campaign repair，terminal PnL 不低于门槛，
    且不是 bad/tail campaign。

--target-mode opportunity_or_campaign
    beats_opportunity 或 campaign_repair 任一成立。

--target-mode opportunity_and_campaign
    两者同时成立，通常只作严格诊断。
```

例子，直接训练 gap-to-random target：

```bash
python -m research.families.f05_fill_quality_quote_ev.audit.fill_selection_score \
  --order-level-filelist <retained-order-level-filelist.txt> \
  --out-prefix <beats-opportunity-output-prefix> \
  --side BUY \
  --folds 5 \
  --target-mode beats_opportunity \
  --target-horizon-s 30 \
  --opportunity-stat mean
```

例子，直接训练 campaign repair target：

```bash
python -m research.families.f05_fill_quality_quote_ev.audit.fill_selection_score \
  --order-level-filelist <retained-order-level-filelist.txt> \
  --out-prefix <campaign-repair-output-prefix> \
  --side SELL \
  --folds 5 \
  --target-mode campaign_repair \
  --min-terminal-pnl 0.0
```

当 BUY / SELL 呈现相反经济含义时，必须拆成 side-specific score：

```bash
python -m research.families.f05_fill_quality_quote_ev.audit.fill_selection_score \
  --order-level-filelist <retained-order-level-filelist.txt> \
  --out-prefix <buy-fill-selection-output-prefix> \
  --side BUY \
  --folds 5

python -m research.families.f05_fill_quality_quote_ev.audit.fill_selection_score \
  --order-level-filelist <retained-order-level-filelist.txt> \
  --out-prefix <sell-fill-selection-output-prefix> \
  --side SELL \
  --folds 5
```

当某一侧有完全不同的库存语义时，可以先限制 denominator。例如 SELL 同时包含“加空/开空”和“减多”，二者不能共用同一个 repair label：

```bash
python -m research.families.f05_fill_quality_quote_ev.audit.fill_selection_score \
  --order-level-filelist <retained-order-level-filelist.txt> \
  --out-prefix <sell-add-short-campaign-repair-output-prefix> \
  --side SELL \
  --exposure-increasing-only \
  --folds 5 \
  --target-mode campaign_repair \
  --min-terminal-pnl 0.0
```

Historical BUY/SELL bucket values from the pre-causality-repair panels have been removed. The entry point remains valid, but current evidence must be regenerated with the current model, P3, event clock and order-level identity. BUY and SELL targets remain separate; a ranking score alone never authorizes tightening, sizing or a lifecycle action.

输出：

- `<prefix>.fill_selection_calibration.csv`：OOS score decile 下的 order denominator、fill rate、target good rate、filled markout 和 campaign terminal outcome；
- `<prefix>.fill_selection_daily.csv`：按 UTC day / side / score decile 展开，检查是否只是少数日期支撑；
- `<prefix>.fill_selection_feature_effects.csv`：每个 quote-time 字段分箱对当前 target label 的 smoothed log-odds 贡献；
- `<prefix>.fill_selection_model.json`：blocked-day OOS 每折训练出的可解释 bin model。

这个 score 只能作为 quote-time fill-selection/campaign-outcome 校准层。它通过 daily OOS 之前，不得接入 quote EV 重训；更不得直接 tighten、加 size 或修改 live。

### Add-on campaign-tail score

`research.families.f09_campaign_action_uplift.audit.campaign_tail_score` 专门处理真正的 exposure-increasing add-on，不再把 opener、add、reducing 混在同一个 label 里：

- denominator：所有 submit 时 `inventory_role=add` 的 placed orders；
- supervision：订单实际成交，且 fill 时仍为 `fill_inventory_role=add`；
- target：默认只认 closed `loss_tail`，UTC 日末 `open_risk` 不当作安全的 0；
- split：BUY-long add 与 SELL-short add 分别训练，按连续 UTC day block 做 OOS；
- weighting：同一 campaign 的多个 add fills 合计权重为 1，避免长 campaign 靠重复 fills 支配模型；
- leakage guard：已经满足 `campaign_max_abs_qty >= 0.010` 或历史 `campaign_adverse_excursion <= -1` 的订单不进入主训练。这些是已有 hard-risk 状态，不是“未来 tail”预测。

```bash
python -m research.families.f09_campaign_action_uplift.audit.campaign_tail_score \
  --order-level-filelist <retained-order-level-filelist.txt> \
  --out-prefix <addon-campaign-tail-output-prefix> \
  --folds 5
```

主要输出：

- `*.addon_campaign_tail_scores.csv`：可按 `(day, client_order_id)` 回连统一 order-level table 的 OOS score extension；
- `*.addon_campaign_tail_calibration.csv`：按 side rank quintile 的分母、fill、tail、terminal PnL、duration、max inventory；
- `*.addon_campaign_tail_daily.csv` / `*.addon_campaign_tail_folds.csv`：daily / blocked OOS 稳定性；
- `*.addon_campaign_tail_feature_effects.csv` / `*.addon_campaign_tail_model.json`：可解释 quote-time feature effect 与 fold model。

旧 retained panel 的 campaign-tail 数值已删除。该 runner 当前只定义 side-specific、inventory-role-specific、campaign-balanced 的监督口径；任何 risk sorter 都必须在当前 corrected baseline 上重新生成，并通过 action-level 随机化后才允许解释为政策价值。

`null_baseline` 是 alpha 重启前的参照系，不是一个 policy arm。它复用统一 `order_level` 表，同时输出四层：

- `current_daily`：当前 baseline 实际 filled orders 的 20s/30s maker-signed markout、fill rate、terminal campaign label；
- `random_daily`：同 UTC day / side、同 fill 数量，随机抽 placed orders，用 `opportunity_markout_20s/30s_bps` 衡量 submit-time opportunity quality；
- `oracle_daily`：事后 top-k opportunity upper bound，以及 realized positive fill upper bound；
- `positive_intersection_daily`：`fill_quality high + campaign_outcome low + toxic low + fill_probability not low + local absorption + xmarket not adverse` 的 denominator / fill / campaign outcome。
- `condition_daily` / `condition_summary`：把若干 quote-time 可见条件与 current-vs-random 缺口放在同一张表里，专门回答“哪些条件能把 realized fills 从 adverse-selected 拉近 random/null”。

`opportunity_markout_*` 表示“这笔 placed order 如果在 submit 价立刻成交”的机会质量；它不是完整成交反事实，也不是 PnL。若 current actual fills 明显差于 random opportunity null，说明当前 fill selection 被 adverse selection 选中过；若 oracle 很厚，说明正 edge 上限存在，但 ex-ante 识别失败。示例：

```bash
python -m research.families.f10_live_replay_attribution.audit.runner \
  --symbol BTCUSDC \
  --replay-orders-csv <tick_quote_decomposition.*.orders.csv> \
  --replay-fills-csv <tick_quote_decomposition.*.fills.csv> \
  --reports order_level,null_baseline \
  --null-random-trials 64 \
  --out-prefix <null-baseline-output-prefix>
```

Retained-day panel runner:

```bash
python -m research.families.f05_fill_quality_quote_ev.audit.null_baseline_panel \
  --symbol BTCUSDC \
  --manifest <retained-good-day-manifest.csv> \
  --tag <tag> \
  --config <current-live-config.yaml> \
  --window-cache-dir ${NARROWGATE_CACHE_ROOT}/window_cache \
  --trace-quotes-max 180000 \
  --trace-fills-max 50000 \
  --random-trials 32
```

`--config` is required for current-baseline evidence.  Without it, trace generation may fall back to the public/example config rather than the rolling private live baseline, which invalidates current-vs-random and oracle reads.

### Executable passive null 与 formal replay integrity

上面的 `null_baseline` 是 **submit-time opportunity null**：它在统一 order-level denominator 上随机抽 placed opportunities，适合回答 actual fills 是否被 adverse selection 选中过，但不模拟随机报价动作引起的 queue、cooldown、inventory 和后续 campaign path。

完整反事实使用 `research/families/f01_fixed_parameter_racing/campaign_outcome_replay_audit.py`：

```bash
python research/families/f01_fixed_parameter_racing/campaign_outcome_replay_audit.py \
  --symbol BTCUSDC \
  --config <private-current-live-config.yaml> \
  --days <retained-good-days...> \
  --arms baseline \
  --random-passive-trials 8 \
  --random-passive-seed 20260710 \
  --strict-calibration \
  --engine cpp \
  --live-like-replay-baseline \
  --live-perf-telemetry <current-live-perf-telemetry.csv> \
  --window-cache-dir ${NARROWGATE_CACHE_ROOT}/window_cache \
  --workers 6
```

Executable passive null 会随机化 requote cadence 和 flat-state bid/ask geometry，但仍经过完整 queue、latency、replace/coalesce、cooldown、库存上限、campaign 与 terminal accounting。随机动作会改变后续状态，因此不能要求它与 baseline fills 完全相等。输出 `<prefix>.random_passive_null.csv` 同时包含 daily 与 `__pooled__` 行，并固定报告：

- baseline 与 random seed distribution 的 raw / InvAdj / terminal campaign；
- fills retention、BUY/SELL split、spread、pause/keep action mix；
- inventory-time ratio、tail delta、maker-signed markout；
- baseline / random 的 raw PnL per fill。

Integrity A/B 使用同一个 runner 的 `--integrity-diagnostic-arms`，固定比较 historical/off/sign-corrected markout feedback，以及 compress/pause-exposure/observe-only spread-cap action。正式研究建议同时开启 `--strict-calibration`；它要求显式 private config、有效 P3/effective-kappa、daily queue calibration、historical BBO/L2 和非零或 empirical REST latency，任一缺失都 fail fast。

窗口结束只做 MTM，不假设 taker liquidation：`terminal_fee_drag=0`，当前 `maker_fee_rate=0`；另行输出的 `terminal_liquidation_fee_estimate` 不进入 PnL。旧 markout-sign、cap-compression 和 executable-null 的精确 retained 结果建立在已废弃的 event clock 与 baseline 上，已经删除。对应命令仍可用于当前 corrected baseline 的实现完整性诊断，不能复用旧数值。

Order-level denominator panel uses the same rule:

```bash
python -m research.families.f05_fill_quality_quote_ev.audit.order_level_panel \
  --symbol BTCUSDC \
  --manifest <retained-good-day-manifest.csv> \
  --tag <tag> \
  --config <current-live-config.yaml> \
  --window-cache-dir ${NARROWGATE_CACHE_ROOT}/window_cache \
  --trace-quotes-max 180000 \
  --trace-fills-max 50000 \
  --refresh
```

Use `--refresh` after replay/audit schema changes.  As of 2026-07-06 Python replay emits real `markout_20s`; old order-level files where `markout_20s_bps` was silently copied from `markout_30s_bps` must not be used for holding-budget or fill-selection score calibration.

The pre-repair retained111 actual-vs-random and oracle values have been removed. Submit-time opportunity null remains a diagnostic definition, not a tradable random strategy or an action-value estimate. New null evidence must be regenerated with the corrected clock and current baseline identity.

`campaign_policy_replay` 是成交序列级 shadow replay：它按真实 fill 顺序重放，但用 shadow 自己的库存路径决定某笔 fill 是否继续加仓；命中规则时跳过该 fill，并继续用后续真实成交价 mark-to-market。它不是完整盘口/队列 counterfactual，但适合快速判断库存阈值、campaign age cap、reducing-only 这类风险规则是否会打到真实长 campaign。

当前内置 policy：

- observed sequence baseline；
- stop adding after abs inventory >= `0.006 / 0.008 / 0.010 BTC`；
- stop adding after campaign age >= `20 / 40 / 60 min`；
- reducing-only after campaign start；
- `0.006 BTC` 与 `20 / 40 / 60 min` 的组合。

## Research Evidence Tables

固定参数策略研究族已经正式关闭。旧 48/512/1024-arm、固定 cooldown、固定一档报价和全局参数 winner 不再具有当前选参或 promotion 权限；安全约束、执行校准和 rolling baseline 固定值仍保留。证据分层、理论边界和 successor 研究契约见 [`fixed_parameter_strategy_family_closed.md`](../research/families/f01_fixed_parameter_racing/docs/fixed_parameter_strategy_family_closed.md)。

Stage T / toxic-risk / shadow-avoidance / bucket search 的历史脚本已经从 `models/` 移除；历史 CSV / Markdown 结果可以继续作为输入，但人工审阅和跨实验比较要先进入统一 runner。当前已收束三类 report：

- `toxic_risk`：读取 toxic-risk bundle 对照、order denominator、shadow avoidance aggregate，输出 `<prefix>.toxic_risk.csv`；
- `shadow_avoidance`：读取 blocked historical OOS / subbucket candidates 与 daily support，输出 `<prefix>.shadow_avoidance.csv`；
- `bucket_evidence`：读取 local bucket research clues 与 daily support，输出 `<prefix>.bucket_evidence.csv`。
- `local_liquidity_mechanism`：从统一 order-level schema 计算 response / OU-style half-life / absorptive capacity / xmarket moderator 交集，输出 `<prefix>.local_liquidity_daily.csv`、`order_capacity.csv`、`rollup.csv` 和 `candidates.csv`。旧 `models/local_liquidity_mechanism_audit.py` 已删除。
- `live_replay_baseline_compare`：读取 live `order_outcomes.csv` / `quote_decisions.csv` 与 replay daily CSV，输出 maker-only day-level live/replay 对照：placed、fills、BUY/SELL split、BUY/SELL VWAP、side VWAP edge、action mix 和 pause/keep 差异。`trades.csv` 只用于 campaign/仓位类审计，不作为 maker fill VWAP 的主来源。
- `xmarket_ref_shadow`：把 BTCUSDT reference BBO mid 接到统一 order-level 表，只做 quote-time shadow evidence 与 event-cancel counterfactual。它现在同时输出 `pending_ref_bps_1s/3s/5s = ref_mid_return_bps - local_mid_return_bps` 的 raw residual、side-signed favorable/adverse bucket、daily sorting power，并对 filled orders 额外在 `fill_ts` 重采样同一 pending residual，用于 C1 post-fill campaign 调节证据。它不改变 replay outcome，也不复活 archived `xmarket_retreat` direct policy。若输入 BBO 是 `1s_snapshot`，输出中的 event-cancel 只能视为 coarse screening；任何 50/100/250ms live-facing 结论都必须先换成 raw bookTicker / BBO-event tape。

示例：

```bash
python -m research.families.f10_live_replay_attribution.audit.runner \
  --symbol BTCUSDC \
  --order-level-csv <prefix>.order_level.csv \
  --reports local_liquidity_mechanism \
  --out-prefix <output-prefix>
```

Live/replay baseline day compare:

```bash
python -m research.families.f10_live_replay_attribution.audit.runner \
  --symbol BTCUSDC \
  --trades <live_logs>/trades.csv \
  --order-outcomes <live_logs>/order_outcomes.csv \
  --quote-decisions <live_logs>/quote_decisions.csv \
  --replay-daily-csv <campaign_outcome_replay_*.daily.csv> \
  --reports live_replay_baseline_compare \
  --start 2026-07-01T00:00:00Z \
  --end 2026-07-04T00:00:00Z \
  --out-prefix <output-prefix>
```

BTCUSDT reference shadow evidence:

```bash
python -m research.families.f10_live_replay_attribution.audit.runner \
  --symbol BTCUSDC \
  --trades <live_or_replay_logs>/trades.csv \
  --order-outcomes <live_logs>/order_outcomes.csv \
  --quote-decisions <live_logs>/quote_decisions.csv \
  --reports xmarket_ref_shadow \
  --start 2026-07-03T00:00:00Z \
  --end 2026-07-04T00:00:00Z \
  --ref-symbol BTCUSDT \
  --ref-bbo-dir ${NARROWGATE_DATA_ROOT}/bbo \
  --local-bbo-dir ${NARROWGATE_DATA_ROOT}/bbo \
  --out-prefix <output-prefix>
```

Replay order-level table input is also supported:

```bash
python -m research.families.f10_live_replay_attribution.audit.runner \
  --symbol BTCUSDC \
  --order-level-csv <replay_order_level.csv> \
  --reports xmarket_ref_shadow \
  --ref-symbol BTCUSDT \
  --ref-bbo-dir ${NARROWGATE_DATA_ROOT}/bbo \
  --local-bbo-dir ${NARROWGATE_DATA_ROOT}/bbo \
  --out-prefix <output-prefix>
```

Outputs:

- `*.xmarket_ref_state_rollup.csv`: side / horizon / state buckets for `adverse_leading`, `adverse_confirmed`, `favorable_leading`, `favorable_confirmed`, and `neutral`.
- `*.xmarket_ref_pending_rollup.csv`: side / 1s/3s/5s / `pending_side_bucket` outcome rollup.
- `*.xmarket_ref_pending_daily.csv`: per UTC-day version of the same residual buckets; promotion discussion must use this day-level support.
- `*.xmarket_ref_pending_sorting.csv`: aggregate and daily favorable-vs-neutral- vs-adverse sorting rows for 30s markout and campaign labels.
- `*.xmarket_ref_fill_pending_rollup.csv`, `*.xmarket_ref_fill_pending_daily.csv`, `*.xmarket_ref_fill_pending_sorting.csv`: same residual evidence resampled at `fill_ts`; this is the correct first read for C1 campaign-risk moderation.
- `*.xmarket_ref_daily.csv`: the same evidence by UTC day.
- `*.xmarket_ref_event_cancel.csv`: counterfactual event-cancel pressure, saved toxic fills, false positive cancelled fills, and latency sensitivity.
- `*.xmarket_ref_orders.csv`: optional per-order tags via `--xmarket-ref-write-orders`; this can be large on retained-all panels.

历史 toxic-risk / shadow-avoidance / bucket evidence 表可以继续归一化：

```bash
python -m research.families.f10_live_replay_attribution.audit.runner \
  --symbol BTCUSDC \
  --reports toxic_risk,shadow_avoidance,bucket_evidence \
  --toxic-model-compare <historical_toxic_risk.*.model_compare.csv> \
  --toxic-order-aggregate <historical_toxic_risk.*.order_aggregate.csv> \
  --toxic-shadow-avoidance <historical_toxic_risk.*.shadow_avoidance_combined.csv> \
  --shadow-candidates <historical_shadow_avoidance.*.candidates.csv> \
  --shadow-daily <historical_shadow_avoidance.*.daily.csv> \
  --bucket-research-clues <historical_local_bucket.*.research_clues.csv> \
  --bucket-daily-support <historical_local_bucket.*.research_clues_daily_support.csv> \
  --out-prefix <output-prefix>
```

这些 report 只给 evidence verdict，例如 `strict_pass_review_required`、`watch_positive_proxy`、`sparse_watch`、`tail_or_false_block_high`。它们不是 live promotion gate；promotion 仍必须回到 plan 文档里的 data quality、mechanism、fill selection、daily stability、inventory-time、tail-loss 链路。

## Prototype / Deep-Dive Scripts

### Side-specific state-conditioned rearm

Generate a formal BUY or SELL action panel only after freezing a family-specific evidence split. The replay randomizes exactly once per campaign after the actual 85-second baseline add cooldown and never changes reducing quotes, size, or the inventory limit:

```bash
python -m research.families.f09_campaign_action_uplift.audit.state_conditioned_rearm_randomized \
  --side SELL \
  --panel development \
  --evidence-split <sell-rearm.evidence_split.json> \
  --config <current-config.yaml> \
  --queue-calibration-artifact <queue-v3.json> \
  --bbo-dir <strict-bbo-root> \
  --l2-dir <strict-l2-root> \
  --live-perf-telemetry <rest-latency.csv.gz> \
  --latency-profile-id <environment-bound-profile-id> \
  --output-prefix <sell-rearm-development>

python -m research.families.f09_campaign_action_uplift.audit.state_conditioned_rearm_ope \
  --side SELL \
  --panel-role development \
  --panel <sell-rearm-development.action_panel.csv> \
  --panel-metadata <sell-rearm-development.metadata.json> \
  --output-prefix <sell-rearm-development-ope>
```

`state_conditioned_rearm_randomized` is Python-authoritative and makes C++ replay fail fast until the multi-cycle state machine has parity. Validation and sealed holdout require a positive Development access decision; do not loosen a failed state definition on the same family identity. The frozen v1 result is in [`state_conditioned_rearm_after85_v1_20260722.md`](../research/families/f09_campaign_action_uplift/docs/state_conditioned_rearm_after85_v1_20260722.md).

### Causal safe-add path audit

`research/families/f09_campaign_action_uplift/causal_path_features.py` is the shared causal implementation for the fill-to-decision `shock -> refill -> recovery` path. New authoritative Python replay intervention rows include the path directly. Existing frozen panels can be enriched without rerunning outcomes:

```bash
python research/families/f09_campaign_action_uplift/audit/enrich_safe_add_rearm_paths.py \
  --input-panel <safe-add.action_panel.csv> \
  --output-panel <safe-add.path_panel.csv> \
  --metadata <safe-add.path_metadata.json> \
  --partial-dir <path-partials> \
  --symbol BTCUSDC \
  --workers 4
```

Freeze before reading outcomes, then evaluate the fixed path family:

```bash
python research/families/f09_campaign_action_uplift/audit/safe_add_rearm_state_policy.py freeze \
  --feature-family causal_path_v2 \
  --development-panel <safe-add.path_panel.csv> \
  --development-metadata <safe-add.metadata.json> \
  --development-days <safe-add.days.csv> \
  --source-family <support_preflight.frozen_action_family.json> \
  --path-metadata <safe-add.path_metadata.json> \
  --spec <state_conditioned_path_v2.spec.json>

python research/families/f09_campaign_action_uplift/audit/safe_add_rearm_state_policy.py evaluate \
  --feature-family causal_path_v2 \
  --development-panel <safe-add.path_panel.csv> \
  --development-metadata <safe-add.metadata.json> \
  --development-days <safe-add.days.csv> \
  --source-family <support_preflight.frozen_action_family.json> \
  --path-metadata <safe-add.path_metadata.json> \
  --spec <state_conditioned_path_v2.spec.json> \
  --output-prefix <state_conditioned_path_v2>
```

This remains Python action-panel infrastructure; it is not a live or C++ policy surface. Any result must be regenerated under the current corrected baseline and frozen evidence split.

### Maker lifecycle M0/M1 development panel

Build the one-decision-per-campaign panel with exact L2, Binance bridge, per-venue latency alignment, and external consensus:

```bash
python research/families/f10_live_replay_attribution/audit/maker_lifecycle_panel.py \
  --input-panel <safe-add.path_panel.csv> \
  --data-dir ${NARROWGATE_DATA_ROOT} \
  --latency-profile <aws-tokyo-latency-summary.json> \
  --output-panel <maker_lifecycle_panel_v1.parquet> \
  --metadata <maker_lifecycle_panel_v1.metadata.json>
```

Freeze the panel/model/split identity before reading outcomes, then run the development-only M0/M1 screen:

```bash
python research/families/f10_live_replay_attribution/audit/maker_lifecycle_screen.py freeze \
  --panel <maker_lifecycle_panel_v1.parquet> \
  --spec <maker_lifecycle_m0_m1.spec.json> \
  --min-train-days 50 \
  --test-days 20 \
  --embargo-days 1 \
  --blocked-folds 5

python research/families/f10_live_replay_attribution/audit/maker_lifecycle_screen.py evaluate \
  --panel <maker_lifecycle_panel_v1.parquet> \
  --spec <maker_lifecycle_m0_m1.spec.json> \
  --output-prefix <maker_lifecycle_m0_m1>
```

This runner never performs a side/time-nearest order join. Generic order context requires an exact `decision_id` or `client_order_id`; lifecycle labels in the action panel are attached directly to the replay decision identity. Results are side-specific prediction increments, not action uplift. The old development result document was generated under a superseded feature/replay identity and has been removed. Any new conclusion must rebuild the panel under the current causal feature, P3, queue, latency, baseline, and split identity.

这些脚本保留用于历史复盘或深度研究，但不再作为 routine evidence 入口。新增指标应先接入 `models/audit/`，避免一个指标一个脚本造成口径漂移。

| script | status | note |
| --- | --- | --- |
| `models/live_campaign_audit.py` | removed | campaign-only 旧入口已删除；routine campaign report 改用 `research.families.f10_live_replay_attribution.audit.runner` |
| `research/families/f01_fixed_parameter_racing/daily_smoke_sweep.py` | deep-dive runner | 仍用于跑 replay arm；结果进入 `runner --summary-csv ... --reports daily_gate` 做统一 gate 汇总 |
| `models/stage_t_*.py` | removed | Stage T 旧散脚本已删除；历史结果留在 `docs/` 与 backtest results，若要重建同类证据，先接入 `models/audit/` 或 `models/alpha_evidence_ledger.py` |
| `models/alpha_bucket_intersection.py` | removed | local-liquidity evidence intersection is now `runner --reports local_liquidity_mechanism` |
| `models/research/alpha_bucket_shadow_oos.py` | removed entrypoint | Historical conclusions remain in the dated research notes; new output belongs in `models/audit/`. |
| `models/research/local_microstructure_bucket_oos.py` | removed entrypoint | Historical conclusions remain in the dated research notes; the maintained implementation is under `models/audit/`. |
| `models/research/queue_flow_oos_bucket_report.py` | removed entrypoint | Historical conclusions remain in the dated research notes; the maintained implementation is under `models/audit/`. |
| `models/response_kernel_audit.py` | removed | response layer is now inside `local_liquidity_mechanism` |
| `models/ou_reversion_bucket_audit.py` | removed | OU-style half-life layer is now inside `local_liquidity_mechanism` |
| `models/absorptive_capacity_audit.py` | removed | absorptive-capacity denominator is now inside `local_liquidity_mechanism` |
| `models/xmarket_as_moderator_audit.py` | removed | xmarket adverse moderator layer is now inside `local_liquidity_mechanism` |
| `models/research/session_markout_bucket_audit.py` | archived research | session moderator research, not direct policy table |
| `models/raw_trade_shadow_labels.py` | removed | pre-repair one-off shadow-label bridge；当前成交毒性与 causal markout 统一进入 `research.families.f05_fill_quality_quote_ev.audit.fill_toxicity` / `fill_toxicity_incremental` |
| `models/source_incremental_audit.py` | removed | 旧 source-ablation runner 无当前入口/测试；M0/M1 incremental 与 leave-one-venue-out 统一进入 `fill_toxicity_incremental` / maker-lifecycle screen |

## InvAdj Rule

`inventory_adjusted_pnl = final_pnl - inventory_pnl` is a decomposition of inventory-path price drift, not a risk-adjusted alpha score.  It must not be used alone for promotion.  Every candidate report must include raw PnL, maker-signed markout, tail loss, inventory-time, campaign risk, false-block, and daily stability.
> **2026-07-17 source cleanup:** the archived `models/research/` and `models/legacy/` entrypoints were removed after their reusable metrics and outputs had moved into `models/audit/`. Commands below are historical experiment records, not runnable current entrypoints.
