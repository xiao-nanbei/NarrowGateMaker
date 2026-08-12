# NarrowGate BTCUSDC 做市算法项目

> Avellaneda-Stoikov + LightGBM ML增强 — 面向 Binance BTCUSDC 永续合约的做市算法研究与回测项目

Last materially modified: 2026-08-04

---

## 免责声明

中国大陆关于 crypto 交易及相关业务活动的监管环境具有不确定性，可能存在合规展业风险。本项目仅用于探究 C++/低延时系统、市场微观结构、价格行为学、做市模型和机器学习回测方法，供学习交流与技术研究使用；本人制作本项目不代表从事或建议任何 crypto 交易，也不构成财务、投资、法律或合规建议。任何人若使用本项目连接交易平台、下单交易、商业展业或作出投资决策，相关合规风险、资金损失、技术故障、市场风险及其他后果均由使用者自行承担，与笔者无关。

## License

本项目采用 [PolyForm Noncommercial License 1.0.0](LICENSE) 授权，仅允许非商业目的下的研究、学习、实验、测试与交流使用。任何商业部署、交易平台连接、下单交易、投资决策支持、合规展业或其他商业化使用均不在本授权范围内，除非另行取得笔者书面许可。

---

## 一、项目概述

本项目名为 **NarrowGate**。名字取自圣经“窄门”的意象：宽门通向拥挤与喧哗，窄门要求分辨、克制和等待。对应到 maker 算法，策略不试图迎接所有 tick，而是在 spread、bid/ask 相对 mid 的偏移、以及挂单 TTL / 报撤频率这几道门前筛选：只有当市场为了立刻成交而付出足够流动性补偿，且后续继续穿透的概率不高时，才允许某一侧被动挂单暴露。

NarrowGate 的核心不是简单的“超买卖、超卖买”，更不是趋势跟随或主动吃单，而是从 maker 的被动成交视角识别**短期流动性冲击**与**信息驱动重定价**的分界。策略只挂限价单；它观察对手方主动成交、盘口压力、reference/spot 确认和 fill 后 markout，判断某一侧被动挂单是在收取足够流动性补偿，还是正在暴露给逆向选择。若只是短期、未被多市场确认的流动性冲击，maker 可以用被动单收取 spread 与回归补偿；若是真实重定价，继续在该侧暴露会被 toxic flow 打穿。因此每一侧 quote 都要被当成一个可验证的 EV 问题：

$$
EV_{\text{side}} \approx P(\text{fill}\mid x) \cdot \left(\text{spread}_{\text{edge}} + E[\text{markout}\mid \text{fill},x]\right) - \text{adverse}_{\text{tail}} - \text{inventory}_{\text{cost}}
$$

本项目实现了一个**基于 Avellaneda-Stoikov (AS) 模型的做市策略研究框架**。当前唯一维护的 execution 口径是 Binance BTCUSDC 永续合约；代码支持 BTCUSDT perp/spot anchor 以及 Bitget、Bybit、OKX 等外部 reference source，但公开模板默认关闭 multi-market 与 external venues，实际 live source 组合由私有运行配置决定。`${NARROWGATE_ROOT_BTCUSDT_ARCHIVED}` 本地仓已停用并删除；BTCUSDT 不再作为独立 execution 分支维护。核心思路：

1. **AS 模型**提供 bid/ask 报价骨架，通过库存惩罚、波动率和成交强度调节 spread 与 reservation price
2. **ML / quote EV / shock classifier** 逐侧估计 `P(fill)`、fill 后 markout、adverse tail、盘口负载和 reference/spot 确认，区分可接的流动性冲击与应避开的信息冲击
3. **事件驱动报价引擎**通过逐笔行情驱动，把 spread、相对 mid 的 quote 偏移、TTL/报撤频率和库存约束落到可验证的限价报价逻辑上

### 文档口径

本文区分四类事实：**公开模板默认值**来自仓库内 `live/config.yaml`；**私有 live baseline**来自未入库的运行配置并会随部署滚动；**历史证据**只解释研究过程，不能覆盖当前默认值；**可选代码路径**表示代码支持但公开模板未启用。除非明确标注，本文中的“默认”均指公开模板，而不是 EC2 当前部署。

### 设计原则

- **回测-运行时一致性**：tick 回测支持 `--pricing-mode bar|microprice` 做 A/B；公开模板参数由 `live/config.yaml` 定义，私有 live baseline 必须通过对应配置及 `models/config_loader.py` / `models/backtest_config.py` 映射后再做 replay
- **风险优先**：多层风控（日亏损限额、持仓上限、熔断器、exit urgency），保证极端行情下的安全性
- **自适应**：动态报撤频率（RQ）、波动率 regime 感知、库存/生命周期控制与逐侧 shadow score，使策略只在短期条件仍成立的窗口里暴露；direct quote EV live policy 当前归档，不属于默认 live 口径

### 工作分层：策略性质 vs 系统性质

后续工作必须先标注属于哪一类，避免把“策略证据”和“系统优化”混成同一个结论。

**策略性质工作**回答“是否有可交易的 maker alpha / risk edge”。它只从 data quality、identity-bound daily 或 continuous/restart-aware replay、live/replay mechanism sanity、fill selection、maker-signed markout、campaign label、order-level score、inventory-time、tail loss 和 shadow evidence 出发。当前策略主线不是继续扫所谓最优参数，而是：

- 建立并复核 campaign-level label：flat -> nonzero -> flat 的最终 PnL、early drawdown、max inventory、duration、是否自然修复、exposure-increasing / reducing fill 序列。
- 做 order-level score sanity：重定义并验证 `fill_probability_score`、`fill_quality_score`、`campaign_outcome_risk_score`、`toxic_risk_score`、`resiliency_score`，确认它们在 retained historical daily panel 和 live positive/negative episodes 上有稳定解释力。
- 只有当某个证据通过 data quality -> mechanism -> fill selection -> OOS bucket/daily stability -> inventory-time/tail gate，才允许映射到 tiny spread / skew / lifecycle arm；session、reference、spot 只作为 moderator 或 risk veto，不能直接当参数表。

**系统性质工作**回答“同一套策略逻辑能否更稳定、更低尾延迟地执行”。它不证明 alpha，也不替代策略 gate；它优化的是 live hot path、C++ 边界、REST/order lifecycle 和 telemetry。当前系统主线是：

- `live/run.sh` 必须加载可审计的 `live/profiles/python.env|native.env`，启动日志打印 profile、全部 native flags 和 extension 路径；strict native 缺模块/API 时 fail fast，不能重启后静默回到 Python。
- 在 x86 live 机上做同配置、同线程限制、同 marker 口径的 Python/native soak，比较 `live_perf_telemetry.csv` 的 p50/p99/p99.9、fallback、WebSocket age、REST new/cancel tail；warmup、起点 inventory sync 和 sync-degrade 行必须排除。
- 根据 soak 结果决定是否继续提高加仓侧 `replace_min_price_change_ticks` / `replace_min_interval_ms`，减仓侧保持更敏捷，且 TTL、pause、stale-data 和库存安全撤单不被 coalescing 阻断。
- normal quote lifecycle 当前保持同步 order adapter。旧 per-side latest-wins async gateway 在 194.4 分钟目标机 soak 中几乎没有形成 coalesce，且 requote/order-update p99/p99.9 变差；2026-07-17 已删除实现、配置、telemetry ABI 和专属测试，不保留可误开的 dormant switch。
- 后续若重新研究异步 order gateway，必须以新的实验身份从 bounded queue、背压和 target-host soak 重新开始；不能复活旧开关，也不能先调 replace threshold 再解释尾延迟。

两条线可以共享 telemetry 和 replay 工具，但结论不能互相替代：系统优化让策略更可执行，策略证据决定是否值得执行。

### Rolling baseline 口径

从 2026-07-02 起，所有策略实验都采用滚动 live baseline：

```text
baseline = 当前已经部署并运行的 BTCUSDC live 行为
arm      = 在当前 baseline 上叠加的一个明确新机制 / 新参数 / 新模型 / 新执行约束
```

因此，`baseline` 不再指某个固定历史配置，也不指过去某次回测里的 best arm。当前 live 参数就是当前回测 baseline；新机制只能作为 arm 与这个 baseline 比较。arm 必须先相对当前 baseline 通过 retained UTC 日度 mechanism gate、campaign/outcome gate、inventory-time / tail / side-markout 检查，才允许考虑更新 live。一旦 live 配置、模型或关键代码更新并重启/热加载，下一轮回测的 baseline 也随之滚动到新的 live 行为；旧 baseline 只能作为历史参考行，不能作为新 arm 的正式晋级比较对象。live/replay 校准还必须报告日内 BUY/SELL 成交 VWAP（`buy_avg_fill_price` / `sell_avg_fill_price`）和对应成交量；成交数接近但成交均价明显偏移时，说明 fill selection 或订单生命周期仍未对齐，不能直接读 PnL。

每次写 daily/campaign/shadow 报告时必须记录 baseline source：私有运行配置哈希、模型 bundle 标识、本地 replay 来源、以及本地 replay 代码是否与远端 live 代码 byte-for-byte 一致。公开报告不得写出远端 host/PID、完整核心参数快照或原始 live PnL。如果本地代码只多了默认关闭的研究字段，而远端尚未同步，应写成“行为对齐但 hash 未完全对齐”，不能笼统写成已经完全对齐。

### 现有数据 action-uplift 口径（2026-07-18）

action-uplift 研究不再默认等待新增 good days。每个新 action family 先从当前 retained universe 冻结 chronological development、validation、sealed holdout，并在面板之间留 embargo；动作、eligibility、propensity、reward、feature、gate、代码、配置、模型、P3、queue 和 latency identity 必须在读取 outcome 前锁定。历史日期若被其他 hypothesis 使用过，只能称为该 family 的 sealed evidence，不能称为全局 untouched。只有 family 在读取 holdout 后发生变化、holdout 已耗尽、现有日期无法提供 overlap，或生产分布发生实质变化时，才要求新增日期；负结果或区间穿零本身不是“继续等日期”的理由。

从 2026-07-22 起，新研究还必须在 outcome 前冻结统一 score profile 的 `profile_id + SHA256`，并输出 `narrowgate_experiment_scorecard.v1`。打分器先执行 identity、causal timing、propensity/overlap、ESS、tail、fills retention、candidate-rate 与 family gate；任一硬门槛失败时 `ranking_score=null`，权重不能补偿失败。只有合格候选才按 alpha/defense/execution 的 versioned profile 排序，Development 通过只允许读取 Validation，sealed holdout 通过也只生成 shadow candidate，不自动修改 live。新 paired 参数研究统一使用 `build_paired_daily_evidence -> paired_screen_v2`，并只按 `scorecard_ranking_score` 排名；Pareto 和 `joint_paired_t` 仅作同分诊断。`paired_daily_selection()` 已降级为历史兼容适配器，其中 `selection_tier` 和 `candidate_for_blocked_oos` 均无 promotion 权限。是否读取下一 panel 由独立 `panel_promotion_controller.py` 决定，screening profile 永远不能解锁 Validation/holdout 或 live。实现和权重治理见 `models/audit/experiment_scorecard.py` 与 `docs/experiment_scorecard_v1_20260722.md`。

首个正式 family `side_specific_local_actions_causal_v4_20260718` 使用现有 122 个 causal-v4 good days，切分为 development 80 日、embargo 1 日、validation 20 日、embargo 1 日和 sealed holdout 20 日。每个 campaign 最多一次 exposure-increasing add 干预，behavior propensity 固定为 baseline/prevent-over-widen/widen-1tick/recenter-1tick=`0.40/0.20/0.20/0.20`；order size、reducing side、inventory limit、hard safety、empirical latency 和 queue 不变。development 有 5,746 个 intervention campaigns，validation 有 1,508 个，BUY/SELL 和四个动作均有 overlap。

2026-07-18 随后冻结了更窄的 `buy_add_conditional_widen_causal_v4_v1`：只在 BUY exposure-increasing add 上以 50/50 随机执行 baseline 与 widen-one-tick，SELL、size、reducing 和 inventory limit 不变，并明确排除 external reference。100 日 Development 得到 4,387 个唯一 campaigns，动作支持 2,207/2,180，fills/placed/campaign 保留均约 100%。基于 cross-fitted DR pseudo-outcome 的第二层 depth-2 honest tree 在 28 个未来评估日上的 reward uplift 为 `-0.00742 USDC/decision`，95% day-cluster interval `[-0.02657,+0.01304]`；campaign-cost、negative-terminal 和 q10-shortfall 下界也均为负。因此该 BUY one-tick quote-distance family 已正式关闭，9 日 Validation 与 10 日 sealed holdout 均未读取。详见 `research/families/f09_campaign_action_uplift/docs/buy_add_conditional_widen_causal_v4_v1_20260718.md`。

随后按预注册路线转入独立的 `sell_add_repair_trend_skip_causal_v4_v1`。它只在已经持有 short inventory 时，对第一次 baseline-eligible exposure-increasing SELL add 以 50/50 propensity 执行 baseline 或跳过一次报价周期；BUY、reducing、size、inventory limit 和 external reference 均不改变。100 日 Development 产生 2,961 个独立 campaigns，baseline/skip 支持为 1,506/1,455，随机路径保留 99.94% fills，campaign 与 inventory time 分别为 control 的 1.0009x/1.0007x。无条件 randomized path 相对 control 为 `-1.89 USDC`，不是全局改善。

chronological DR policy 在完成 50 日 warmup 后只在 28 个未来评估日的 766 行中选择 4 次 skip（0.52%，ESS 391）。reward uplift 虽为 `+0.000175 USDC/decision`，但 95% day-cluster interval 为 `[-0.000327,+0.000995]`；repair-first、trend-through avoidance 和 combined competing-risk utility 分别为 `-0.003671`、`-0.003689`、`-0.007177`，所有 lower-bound/campaign/downside gate 均失败。唯一 reward 为正的 supported leaf 也同时恶化 repair 与 trend-through，因此没有 eligible skip leaf。该 exact single-cycle SELL family 已正式关闭，9 日 Validation 与 10 日 sealed holdout 均未读取，live/C++/config/baseline 不变。完整口径见 `research/families/f09_campaign_action_uplift/docs/sell_add_repair_trend_skip_causal_v4_v1_20260718.md`。

development 中 BUY `widen_1tick` 的 DR reward uplift 为 `+0.01783 USDC/intervention`，95% 区间 `[-0.01233,+0.04682]`；SELL `recenter_1tick` 为 `+0.01318`，区间 `[-0.01481,+0.04277]`。二者都未通过 development gate，只在读取 validation 前冻结为诊断候选。fixed development-to-validation 后，BUY widen 收缩到 `+0.00394`，区间 `[-0.04063,+0.05211]`，正向日率仅 `45%`；SELL recenter 反号为 `-0.00905`，区间 `[-0.07135,+0.05033]`。validation 的表面赢家变成 development 为负的 SELL prevent-over-widen，这说明 winner rotation，而不是稳定 action alpha。候选动作在 `terminal_campaign_pnl <= -5 USDC` 上没有足够事件，tail gate 因 support 不足失败，不能把零事件解释为消除尾部。

结论：这组固定 local add actions 已由现有数据完成否证，不晋级、不修改 live/C++/config/baseline，也不读取 sealed holdout。下一代应建立新的 state-conditioned family，例如 local shock/refill/recovery eligibility，而不是继续扫固定 tick/秒数。完整报告见 `research/families/f09_campaign_action_uplift/docs/side_specific_action_uplift_existing_split_20260718.md`；执行入口为 `models.audit.evidence_split`、`research.families.f09_campaign_action_uplift.audit.local_action_uplift` 和 `research.families.f09_campaign_action_uplift.audit.local_action_ope_report`。

### 当前证据入口（2026-07-27 治理复核）

旧 retained/blocked/late 参数排名、旧 scorer 数值、旧 random-null 数值、旧 stop-add/rearm 精确 PnL，以及 48/512/1024-arm winner 记录已从当前项目说明删除。它们依赖已废弃的左标签 10 秒特征、trade-clock、历史 P3 override、旧 queue 或旧 live 事故语义，不能用于当前选参或 promotion。

当前可引用的结论只来自以下入口：

- `research/families/f10_live_replay_attribution/docs/historical_backtest_evidence_revalidation_20260720.md`：可信证据边界；
- `research/families/f10_live_replay_attribution/docs/operational_baseline_identity_20260720.json`：历史 empirical-P3 operational rollout 身份，冻结保留但不再代表当前运行；
- `research/families/f10_live_replay_attribution/docs/operational_baseline_identity_20260725.json`：最新冻结的历史部署身份，即 causal-v5 用户指定 trial；
- `research/families/f10_live_replay_attribution/docs/operational_baseline_identity_20260802.json`：causal-v12 semantics-v6 的历史 live-canary 快照，冻结保留；
- `research/families/f10_live_replay_attribution/docs/operational_baseline_identity_20260802_v4.json`：上一版 owner-directed rolling baseline，冻结保留；
- `research/families/f10_live_replay_attribution/docs/operational_baseline_identity_20260802_v5.json`：前一版滚动 baseline，冻结保留；它保持 13-head ML-ON、q90 shadow-ON/action-OFF，但 BUY fill-selection 仍为 ON；
- `research/families/f10_live_replay_attribution/docs/operational_baseline_identity_20260803_v6.json`：短暂的安全快照，q90 与 BUY fill-selection action 均关闭，BUY scorer 也暂停；
- `research/families/f10_live_replay_attribution/docs/operational_baseline_identity_20260803_v7.json`：v8 的冻结前身；运行时仍为 Python 3.12、13-head ML-ON 与 empirical P3，q90 shadow 保留且 action 关闭，同时 BUY fill-selection action 关闭、scorer shadow 开启。该 operational overlay 不改写已读 panel 的研究身份，campaign q10 仍未解决，research authority 仍为 false；
- `research/families/f10_live_replay_attribution/docs/operational_baseline_identity_20260804_v8.json`：v9 的冻结前身；它保持 v7 的策略语义，并新增已部署的 immutable quote snapshot、bookTicker freshness fallback 和分离的 visible-age/source-lag 合同。该 successor 仅修复 baseline integrity，没有读取 PnL，也没有授权新 action；
- `research/families/f10_live_replay_attribution/docs/operational_baseline_identity_20260804_v9.json`：v10 的历史前身；在 v8 远端代码上通过配置关闭 BUY fill-selection shadow，并清空其模型路径；
- `research/families/f10_live_replay_attribution/docs/operational_baseline_identity_20260808_v10.json`：当前滚动 live identity。它保持 causal-v12 ML-ON、empirical P3、global BER、q90 shadow-ON/action-OFF，以及 BUY fill-selection action/shadow-OFF；v10 只收缩无效 shadow 观测，不改变报价经济路径；
- `research/families/f10_live_replay_attribution/docs/operational_baseline_current.json`：指向当前不可变 live identity 和当前默认 replay control 的可变机器入口；
- `research/families/f10_live_replay_attribution/docs/current_live_held_ber_replay_baseline_50d_20260810.json`：当前 pooled 50 日回测 denominator。immutable 前 40 日为 terminal MTM `-144.251748 USDC`、closed-campaign value `-147.466348 USDC`、`17,118` fills；新增 10 日为 `-21.314331 USDC`、`-21.064631 USDC`、`3,029` fills；合并 50 日为 `-165.566079 USDC`、`-168.530979 USDC`、`20,147` fills。当前实现只消费 native-derived top-20/100ms BBO/L2，没有 raw snapshot/delta queue tape、经验 REST 延迟或 AWS receive/feature-ready 可见延迟，因此只承担同模拟器 diagnostic control，不授权订单路径 action，也不冒充连续 live PnL。严格 successor 已完成全部 50 日 source preflight，并在 `2026-06-29` 一日消费 5,086,247 条 native events、完成 19,460 次零 missing queue lookup 和 14,825 次 visibility-delay application；严格 50 日 panel 尚未运行；
- `research/system_engineering/docs/time_unit_contract_repair_20260726.md`：causal-v7、13-head ML-OFF、empirical-P3-on 历史回滚契约；实际远端仍须以 preflight、`run.sh status` 和启动 hash 核验；
- `research/system_engineering/docs/replay_time_unit_causality_repair_20260715.md`：量纲与时间因果修复；
- `research/families/f03_causal_13_head/docs/causal_v4_empirical_p3_retrain_replay_20260718.md`：模型训练/validation 身份；原 test、全 122 日 ML A/B 与 BUY scorer bucket 数值已因 metrics 提前 5 分钟可见而撤回，等待 corrected panel 重建；
- `research/families/f09_campaign_action_uplift/docs/side_specific_action_uplift_existing_split_20260718.md`、`research/families/f09_campaign_action_uplift/docs/buy_add_conditional_widen_causal_v4_v1_20260718.md`、`research/families/f09_campaign_action_uplift/docs/sell_add_repair_trend_skip_causal_v4_v1_20260718.md`、`research/families/f07_active_order_continuation/docs/queue_value_keep_cancel_v1_20260719.md`、`research/families/f07_active_order_continuation/docs/queue_value_cancel_reenter_v3_development_20260720.md`：当前 action-family 结果；
- `research/families/f07_active_order_continuation/docs/deep_active_order_queue_probe_20260720.md`：top-20/deep-250 queue 机制复核与 sparse tape gate；
- `research/families/f07_active_order_continuation/docs/native_exchange_book_replay_scheduler_20260720.md`：原生 snapshot/delta exchange-time scheduler、state calibration 与 native keep/cancel Development 停止结论。

旧数据产物只保留用于复核事故和重建 lineage，不再在 README、project 或博客中展示为策略结论。

### 2026-06-29 维护范围收缩

- 当前只维护 `~/Projects/NarrowGate_BTCUSDC`。本地 `~/Projects/NarrowGate_BTCUSDT` 已删除，后续不再做跨仓代码同步、参数同步、模型同步或文档口径同步。
- BTCUSDT 仍是 BTCUSDC 的 reference/source 市场：可以保留 Binance public data、source-missing day 排除、basis/lead-lag/anchor audit 等研究口径，但不能把它写成独立 execution 分支的当前事实。
- **代码与模型输入维度的最终真源** 以 `live/config.yaml`、`models/*_meta.json`、以及运行目录下的实际文件结构为准；本文件给出的是设计与当前实现摘要。
- 当前主线已经接入 **CryptoHFT 原生盘口重放** 生成的 `bbo/`、`l2/` 数据；旧的 `depth_1s` 百分比桶主要保留给历史代理口径与旧回测。
- 事件引擎/tick replay 侧已抽出共享 `strategy/quote_core.py`，统一输出 `raw_half_spread`、`raw_mid_shift`、`final_quote_delta` 与 `quote_context`；事件引擎侧同时保留 **per-side quote policy** 与结构化日志 `quote_decisions.csv` / `order_outcomes.csv`，用于解释为什么某侧被放宽、缩量、暂停或仅允许减仓。`order_outcomes.csv` 的成交执行事件使用 `event_type=filled`；旧日志里的 `fill` 仅作为兼容口径读取。

### 2026-06-20 前历史摘要

2026-06-20 之前，项目完成了从基础数据下载、特征工程、AS/ML 回测到事件驱动做市引擎的初始搭建，并先后尝试过动态 spread/γ、成交概率、库存偏移、ret shift、BER、Transformer、RL、toxicity gate 等策略与工程改造；同时修复了训练/运行时频率、手续费、滚动窗口、盘口粒度、配置映射和数据质量等一致性问题。这些工作已合入或被后续架构替代，旧参数 sweep、旧模型指标、跨日/月度回测排名和 BTCUSDT execution 结论已经从当前说明删除。当前判断从 2026-07-15 因果修复后的 replay identity、rolling baseline 与 family-specific chronological split 重新开始。

### 2026-06-24 时间轴 / source audit 修正

- 旧 enhanced feature 存在同一时间戳重复 5-6 次的问题；pandas 3 下 `DatetimeIndex.view("int64")` 还可能返回微秒，而旧代码按纳秒解释，影响 gap/horizon mask、label horizon 和部分 tick replay 时间换算。现在 `data_quality.py`、`feature_engineer.py`、shock/quote EV audit 与 replay 入口统一显式转换为纳秒。
- feature/dataset 写入前新增重复时间戳 fail-fast；rolling basis 只在 source 连续 segment 内计算，不能跨坏日期或长 gap；source freshness 使用真实 source event time，BBO 缺失时只允许回退到同 source trade stream。
- 已强制重建 enhanced feature 输入；修复前 source、horizon label、quote-EV sample count 和相关 A/B 数值已经删除，必须使用当前 causal 产物重训/重跑。

### 2026-06-30 unified calendar / session 口径

- 新增 `calendar_features.py` 作为唯一日历入口，不再在各脚本手写 NYSE/US/CN 节假日或 US RTH 判定。它统一输出 UTC、US/Eastern、Asia/Shanghai、NYSE/Federal/China holiday/workday、US Sunday、US RTH、weekend 和 global session 字段。
- 主模型 `feature_engineer.py` 保留旧字段名（如 `hour_sin`、`is_us_regular_hours`）并额外写 `cal_*`；live `strategy/signal.py` 的 legacy US-session 字段也使用同一 scalar helper。Quote EV label、scalar scoring、shadow arrays、OOS bucket audit 统一使用 `quote_cal_*`。
- `train_quote_ev.py` 曾增加 `calendar`、`calendar_local_flow`、`calendar_local_flow_xmarket*` feature modes；旧 `precompute_quote_ev_arrays.py` direct-policy fast path 已在 2026-07-17 删除。当前 quote-EV 只保留 canonical model/shadow audit，不再 materialize direct live action arrays。
- 修复前 calendar/local-flow 的 calibration、session bucket 和 markout 数值已经删除。统一 calendar helper 仍是有效工程契约，但日历字段只能作为候选 feature；不能据此生成 weekend disable、US-RTH tighten、spread、size、TTL 或 quote-EV policy。

---

## 二、项目文件结构

以下只列当前仓库中的核心入口；历史 BBO/L2/raw 数据位于 `${NARROWGATE_DATA_ROOT}`，不是仓库内固定目录。

```
NarrowGate_BTCUSDC/
├── data/                       # 离线下载、archive/import、规范化与质量审计代码
├── features/                   # 预处理、特征工程与 feature pipeline
├── models/                     # 共享 tick replay、数据窗口与实验治理 ABI
│   ├── backtest_tick.py        # Python 权威逐笔 replay
│   └── audit/                  # 跨研究族共享 scorecard/cache/promotion contract
├── research/
│   ├── families/f01_* ... f10_* # 研究族源码、冻结 spec 与证据文档
│   ├── shared/                  # D/R/S/G 四层共享依赖索引
│   ├── system_engineering/      # 系统性能、时钟与部署证据
│   └── governance/              # 路径迁移、冻结 archive 与 resolver
├── strategy/
│   ├── quote_core.py           # Python/C++ 共享报价语义
│   ├── maker_engine.py / signal.py
│   ├── order_manager.py
│   └── global_flow.py / global_reference.py
├── execution/
│   └── active_order_depth_path.py # 我方活动订单的 exact-level path/queue bounds
├── live/
│   ├── main.py / ws_handler.py
│   ├── orderbook/              # Binance 执行市场 REST snapshot + diff-depth 状态
│   ├── venues/                 # Bitget、Bybit、OKX reference adapters
│   └── config.py / config.yaml # 配置 schema 与公开安全模板
├── cpp/                        # native quote/signal/replay hot path
├── bench/                      # live path、gateway、quote core、replay benchmark
├── scripts/                    # EC2 soak、profile 与 public reference 配置工具
├── tests/
├── market_fusion.py
└── project.md
```

仓库内 `data/` 只存工具代码；真实 raw/normalized/feature 文件统一位于 `${NARROWGATE_DATA_ROOT}`。`live/orderbook/` 是进程内实时状态，不是第二个数据目录。

---

## 三、理论基础 — Avellaneda-Stoikov 模型

### 3.1 核心公式

**保留价格 (Reservation Price)**:
```
r(s, q, t) = s − q × γ × σ² × (T − t)
```
- `s`: 公允价。`use_bar_pricing=false` 时使用盘口 microprice；`use_bar_pricing=true` 时使用 1s bar close 复现旧回测口径
- `q`: 当前净库存 (正=多头, 负=空头)
- `γ`: 风险厌恶系数 — 控制做市商对库存风险的敏感度
- `σ²`: 短期价格方差（波动率的平方）
- `T − t`: 剩余做市时间周期

**最优 spread**:
```
δ = γ × σ² × (T − t) + (2/γ) × ln(1 + γ/κ)
```
- `κ`: P3 touch 概率对单边价格距离的局部对数斜率，`-d log(P_touch) / d delta`，单位为 `(USDC/BTC)^-1`；它不是订单事件到达率。当前 live/tick 优先使用 P3 `effective_kappa`/显式 override；`StrategyConfig.kappa` 只是缺少校准时的内部 fallback，不应把旧 YAML 中的 `0.073` 当作当前有效 κ

**报价**:
```
bid = r − δ/2    (买入限价)
ask = r + δ/2    (卖出限价)
```

### 3.2 直觉解释

| 参数 | 效果 | 实际意义 |
|------|------|---------|
| `γ` 增大 | spread 变宽、库存惩罚更强 | 更保守，利润低但亏损风险小 |
| `σ²` 增大 | spread 变宽、保留价格偏移更大 | 高波动时自动放宽 spread，减少逆向选择 |
| `q` 偏离零 | 保留价格偏移使报价倾向于减仓 | 库存自然回归，避免单边积累风险 |
| `κ` 增大 | spread 收窄 | 订单簿深（成交快），需更窄 spread 竞争 |

### 3.3 增强实现

本项目对经典 AS 模型做了如下增强：

| 增强 | 公式/方法 | 作用 |
|------|---------|------|
| **Microprice** | `fair = mid + imb × half_spread`，`imb = (bid_qty − ask_qty) / (bid_qty + ask_qty)` | 比 mid-price 更准确的公允价格估计 |
| **动态 κ** | `κ_eff = κ_base × clamp(avg_depth / baseline, 0.3, 3.0)` | 根据实时盘口深度自适应调节成交概率 |
| **κ_eff** | `κ_eff = fill_model.effective_kappa()` | 正式 artifact 使用 causal empirical-survival P3 calibration；旧 SU Johnson 只保留为迁移期兼容，不能作为新模型证据 |
| **Fee-aware floor** | `min_spread = 2 × fee × mid + tick` | 保证 spread 覆盖手续费 |
| **ML 波动率混合** | `σ² = vol_blend × σ²_ML + (1 − vol_blend) × σ²_rolling` | 融合预测波动率和历史波动率 |
| **ret_skew 偏移** | `r += clamp(pred_ret × ret_skew × mid, ±RSP×δ/2)` | 利用 ML 收益率预测偏移保留价格，clamp 防止单边持仓 |
| **方向 gamma 奖励** | `γ_eff = γ × (1 + bonus)` 当库存与 ML 方向一致 | 减少与 ML 信号方向相同的持仓期望 |
| **动态 RQ** | `rq = rq_max × exp(ln(rq_min/rq_max) × clamp(vol_ratio, 0, 2))` | 高波动快报撤，低波动慢报撤 |
| **库存衰减** | `adj_size = base × exp(−η × \|q\|/max_inv)` | 库存越大，逆向下单量越小 |
| **Exit urgency** | `asym += exit_urg × urgency`，urgency = `\|q\|/max_inv` | 持仓越重，报价越偏向减仓方向 |
| **库存时间暴露** | `abs_inventory_time_s = ∫ \|q(t)\|dt`，`notional_inventory_time_s = ∫ \|q(t)\|·mid(t)dt` | 衡量整个持仓期间承担的库存风险预算；A/B 同时看 raw/InvAdj 与单位库存小时 PnL |
| **InvAdj 语义边界** | `inventory_adjusted_pnl = final_pnl - inventory_pnl`，`inventory_pnl = ∫ q(t)dP_trade(t)` | 它是库存路径价格漂移分解项，不是库存风险惩罚后的 PnL。BUY 后下跌等 toxic markout 会进入负 `inventory_pnl`，再被 InvAdj 加回去；所以 InvAdj 只能和 raw PnL、maker-signed markout、tail loss、库存时间、false-block、daily stability 一起看 |
| **流动性 γ** | `γ *= 1/√(trade_intensity/baseline)` | 低流动性时段(亚洲白天)加宽spread |
| **ret_shift_max_pct (RSP)** | `rs_clamp = RSP × δ/2; r_shift = clamp(pred_ret × ret_skew × mid, ±rs_clamp)` + 库存衰减 | 限制 ret_skew 偏移幅度，防止 clamp 饱和导致单边持仓累积 |
| **CJP 库存比例偏移** | `r -= φ·(q/q_max)·δ` (Cartea, Jaimungal & Penalva 2015) | 库存越大，reservation price 越偏向减仓方向。满仓时偏移 δ/2，保证对手侧挂单紧贴盘口 |
| **pred_ret EMA 去均值** | `debiased = raw − EMA(raw)`，α = 2/(N+1) | 消除动量特征导致的持续方向偏置，仅保留 innovation 成分用于报价偏移 |
| **Toxicity gate** | `pred_tox_bid/ask >= threshold` 时暂停对应风险侧报价 | 当前 BTCUSDC 以 `live/config.yaml` 为准；阈值必须用 BTCUSDC tick/运行时 trace 验证 |
| **Quote-EV shadow evidence** | `P(fill)` + 1s/5s/30s markout buckets + extreme adverse 用于 order-level calibration 与审计 | direct tighten/widen/pause executor、预计算 fast-screening 脚本和 C++ action ABI 已删除；模型训练与 shadow score 保留，不能直接改 live 报价 |
| **Cross-market shock shadow** | `cross_market_shock_audit.py` 标注本地流动性冲击 vs BTCUSDT-reference/spot 信息冲击，并用 `data_quality.mask_valid_horizon()` 防止 30s label 跨 gap；`train_quote_ev.py --feature-mode xmarket` 训练旁路 quote EV 头；`quote_ev_shadow_eval.py` 只看已成交 markout 排序 | 离线/no-op 研究路径，不改配置文件，不自动接入报价。Bitget/Bybit/OKX spot/perpetual 已按 retained111 构建 2-of-3 consensus、hierarchical bridge 和 leave-one-venue-out；Stage 0 尚未支持 policy |

### 3.4a Cross-market / quote-EV / audit evidence current reading（2026-07-20 boundary）

早期 cross-market、enhanced spot、quote EV、local-flow、calendar、resiliency 等 run table 及方向性结论已从正文删除。它们来自 pre-cleanup replay、旧模型目录、旧 trace 或旧 promotion 假设，不能继续当作当前参数选择或特征选择先验。当前只保留研究契约：

1. **BTCUSDT / spot 是候选状态源，不是 direct alpha switch。** 旧的 `multi_market.enabled=true/false`、`xmarket_retreat_*`、reference favorable/adverse 统一 widen/pause/TTL 结论已经删除。任何新用途都必须从 local M0、source freshness 与 action-specific M1 增量重新验证。
2. **Direct quote EV live policy 已删除。** 早期 ask-only 或 both-side quote-EV A/B 数值不再保留为方向线索。quote EV 现在只作为 shadow calibration、fill-selection score、campaign outcome risk 的候选特征通道。
3. **InvAdj 不是库存风险惩罚 PnL。** 它只是把持仓随价格漂移的账本项从 raw 中拆开，可能中和 adverse selection 损失。任何候选都必须同时看 raw、maker-signed markout、tail loss、abs/notional inventory-time、campaign terminal PnL、MAE、daily stability 和 live mechanism distance。
4. **统一 audit runner 是当前证据入口。** 不再新增“一指标一脚本”的散入口。toxic-risk、shadow avoidance、bucket evidence、order-level denominator、campaign label、campaign policy replay、local liquidity mechanism、score sanity 都应接入现有 canonical runner、alpha ledger 或共享 audit utilities，输出统一 evidence table。
5. **当前训练/研究主线是 order-level + campaign-level。** 每笔 placed order 记录 quote-time state、fill outcome、1s/5s/20s/30s markout、campaign outcome、inventory exposure 和 explainable scores；只有稳定解释 fill quality / campaign terminal risk 的条件，才允许映射成 spread、skew、lifecycle 的 tiny shadow arm。

因此，本节不再保留旧的 Jan/Feb/Apr/May 汇总表、2025 扩样细表、quote-EV bundle 数字、Stage T/O/P 中间 bucket 明细或旧 direct-policy 候选。生成物可用于 lineage 审计，但不能恢复为策略结论；当前 promotion 证据必须从 causal feature、rolling baseline 与 family-specific chronological split 重新生成。

### 3.4b Source-by-source 增量验证

修复前 Binance reference 的 source-by-source MAE、IC、fill 与窗口排名已经删除。当前只保留实验契约：每个 source 必须有独立 freshness、basis、coverage 与 causal-ready-time 审计；local-only M0 与 source-augmented M1 使用相同模型、切分和 target。预测增量不等于报价动作价值，任何 source 都不能作为 `multi_market.enabled` 的直接收益开关。

### 3.4c-f Independent venue integration

Bitget、Bybit 与 OKX 的 BTCUSDT spot/perpetual 历史和 public WebSocket 接入已经统一到 venue-aware schema、UTC good-day manifest 与 causal right-edge builder。早期单 venue、双 venue和 retained7 中间报告已经被三 venue Stage 0 取代并删除；其阈值、bucket 差值和 policy 暗示不再构成当前证据。

实时 source 使用 public market-data channel，不依赖 API key，并记录 exchange、local receive 与 feature-ready 时间。历史 trades-derived 1 秒状态只适合 second-scale diagnostic；亚秒 maker 决策必须使用 receive-time BBO/trade tape。当前结论从下一节的三 venue报告开始。

### 3.4g Three-venue spot/perp global reference Stage 0（2026-07-11）

**研究边界更新**：这一代 Stage 0 已冻结为秒级 diagnostic baseline。它使用 `[t,t+1s)` 事件在右边界可见的 trades-derived state，无法区分 maker 所关心的跨 venue 领先、同步和已吸收状态。后续不再围绕该 1 秒 residual 扫阈值；新的 Stage 0 使用三家 public WebSocket BBO + trades 的本机 receive time，构建事件驱动 L1 OFI/depletion/refill 与 aggressive-flow state，并以 10/25/50/100/250/500ms maker-signed markout 和 fill-toxicity 为目标。旧结果只否定“1 秒聚合直接驱动 re-center/cancel”，不否定 receive-time cross-venue maker 信息。

OKX perpetual 与 spot 历史均已扩为 canonical retained111。下载页日包使用 UTC+8 边界，因此每个正式 UTC 日同时读取源日 D/D+1，再按事件时间裁到 UTC `[00:00, 24:00)`；swap 合约数量按 `ctVal=0.01 BTC` 转成 BTC。OKX perpetual 共 399,947,490 笔 trades / 8,742,874 条 causal 1s states，spot 共 69,915,914 / 5,846,509。成功导入后，非 good-day、ZIP、part 与临时文件均清零。

global reference 不把六条外部行情直接平均。Bitget/Bybit/OKX spot 与 perpetual 分别形成两个 robust common innovation；Binance BTCUSDT perpetual 只做本地 level bridge，不算第四票。Binance 官方交易对实际 symbol 为 `USDCUSDT`，含义是一单位 USDC 对应多少 USDT，因此桥接公式是 `BTCUSDT / USDCUSDT -> BTCUSDC`。这条 stablecoin anchor 也已按同一 111 日下载并聚合：46,776,513 笔 aggTrades、8,792,656 条 1s bars，原始 CSV/ZIP 已清理。BTCUSDC spot 只作为 cross-check/fallback。

三 venue 得到 7,747,571 条 spot states、9,225,272 条 perp states 和 7,547,083 条 spot/perp joined states。hierarchical reference 有 6,247,083 条状态，其中 4,659,028 条通过 freshness、2-of-3、方向一致、dispersion 与 causal slow-basis gate。full 与三个 leave-one-venue-out Stage 0 的结论仍未通过：唯一四版都保持 30s/campaign 同方向的 global-residual 行是 `SELL submit 1s`，但 30s 仅约 `+0.05~+0.10bps`，5s 为负，日度同号接近一半。`SELL fill + divergent` 有较大的 30s clue，但 5s、campaign 与 repair 在删 venue 后不稳定。因此第三个 venue 当前主要提供反证能力，不进入 re-center/cancel/widen/size/lifecycle policy。

live 的六条外部 spot/perp source 现已全部切到无需 API key 的 public WebSocket：Bitget v3 `books1/publicTrade`、Bybit `orderbook.1/publicTrade`、OKX `bbo-tbt/trades`。`strategy/global_flow.py` 按本机 receive time 维护 10/25/50/100/250/500ms aggressive flow、L1 OFI/depletion/refill、venue agreement 和 local/global gap；这里的 L1 变化只是 top-of-book proxy，不冒充 exact-L2 cancel attribution。成交毒性审计路径只使用 `feature_ready_ts_ns <= fill_ts` 的状态，并从 Binance BTCUSDC receive-time BBO 构造同一组 horizon 的 BUY/SELL maker-signed markout。

EC2 零 key 预检六源均收到 BBO/trades，connector error/reconnect 为 0；末端 transport lag 约为 Bitget 5-13ms、OKX 28-30ms、Bybit 38-40ms。正式 shadow HEALTH 显示 `marketTapeDropped=0`、`marketTapeInvalid=0`、`externalErrors=0`、`globalFlow100Valid=1`，且稳定为一组双边订单。receive-time tape 使用无损 `.jsonl.gz`，实测写盘投影由约 5.9GB/day 降到约 0.54GB/day，不做 1 秒或 100ms 时间聚合。这些数字只证明可观测性和系统可运行，不证明 cross-venue alpha。

同一环境随后冻结了第一份 3600 秒 market-data latency profile：AWS Tokyo `ap-northeast-1`、`t3.medium`、2 vCPU / 3.75GiB、x86_64、Amazon Linux 2023、native compute profile、同步 order gateway、public WebSocket。窗口为 `2026-07-11T08:54:29Z..09:54:29Z`，共 682,110 条事件。外部 BBO p50 为 Bitget 约 5-6ms、OKX 约 29-36ms、Bybit 约 38-40ms；外部 BBO p99 已进入约 268-701ms，Binance perpetual callback p99 约 2.8s。后者说明 profile 同时包含公网传输、exchange clock、callback/GIL/VM 调度和一次受控 restart，不能解释成纯网络单向延迟。机器可读 profile 在 `live/profiles/latency/aws_tokyo_t3_medium_amzn2023_native_public_ws_20260711_3600s.json`，人工报告在 `research/system_engineering/docs/aws_tokyo_market_data_latency_profile_20260711.md`。

2026-07-12 又完成一个无 restart 的 3600 秒 bounded capture。用户请求查看最近三小时，但 exact receive-time recorder 只覆盖最后 `09:54:16Z..10:54:16Z`，前两小时未用 HEALTH 日志伪造。七个 gzip 共约 70.5MB，gzip/SHA-256 全部通过；1,553,377 条事件无解析错误。外部 BBO 稳态中位数再次约为 Bitget 5-6ms、OKX 29-30ms、Bybit 37-41ms，feature-ready 本地开销中位数约 0.1-0.7ms。Binance BTCUSDT/BTCUSDC perp book 中位数分别约 68/125ms，但 p90 已约 1.16/1.21s，且五分钟切片反复出现，不能归入 p99 偶发毛刺；这更像 callback、GIL/VM scheduling 与录制负载下的排队，而不是公网单向传播。正式研究主口径因此使用 source/event-specific `profile_p50`；p90/p95 只作压力测试，可选固定 seed 的 0.5% p95-p99 毛刺混合，不让 p99/p99.9 决定策略排序。机器 profile 在 `live/profiles/latency/aws_tokyo_t3_medium_amzn2023_native_public_ws_20260712_3600s.json`，完整解释见 `research/system_engineering/docs/aws_tokyo_market_data_latency_stable_20260712.md`。profile 本身仍只是环境校准，不会自动启用任何 re-center/cancel/stop-add arm；策略研究必须显式传入 profile 和 environment identity。

历史 post-fill stop-add 的策略数值已删除；该 latency profile 仅用于系统可见性校准，不能继承为当前 action 证据。

`fill_toxicity.py` 现在显式支持 `exchange_zero`、真实 `captured`、`profile_p50`、固定 seed 的 `profile_empirical` 和 `profile_p99`。profile 模式从 exchange timestamp 重建 feature visibility；captured 模式绝不重复加延迟。四笔 maker BUY fill 的 wiring smoke 显示 p99 profile 基本抹掉 10ms flow，并把 100ms flow 方向相对 captured 的一致率降到 50%；markout 不变，因为当前没有 external policy。这个样本只能证明 latency simulation 已生效，不能作为 alpha 或 promotion 证据。`SYNC_ADJUST` 已从 maker fill 分母剔除；超过 1 秒才到达的 stale trade 仍保留在 raw tape/profile，但不进入 10-500ms `GlobalFlowState`。

部署过程中还发现并修复了一个独立系统风险：旧 `live/run.sh` 只按绝对路径寻找进程，无法识别历史相对路径 `live/main.py`，一次 restart 因而短暂留下两个 maker 进程和两组双边订单。旧进程及重复订单已清理；`start/restart` 现在同时识别两种命令行，且 PID 文件缺失时也会在启动前拒绝已有 maker 进程。

所有输入保持 shadow-only，不持有外部 venue API key，也不改变 Binance quote policy。完整证据见 `research/families/f04_external_market_alpha/docs/global_reference_stage0_retained111_20260711.md`；供独立模型或人工复核的完整方法、数值表、假阴性风险与问题清单见 `research/families/f04_external_market_alpha/docs/global_market_reference_review_memo_20260711.md`；从 receive-time tape、global flow、fill/campaign target 到 action uplift 的实施顺序见 `research/families/f04_external_market_alpha/docs/cross_venue_reference_to_alpha_roadmap_20260711.md`；旧 retained7 报告仅保留历史溯源。

2026-07-11 又在 EC2 上用零 key 实连验证三家 public WebSocket：Bitget 收到 `books1 + publicTrade`，Bybit 收到 `orderbook.1 + publicTrade`，OKX 收到 `bbo-tbt + trades`。因此 external reference 配置和 connector 不再保留任何 API-key 字段。OKX 10ms 全量增量 `books-l2-tbt/books50-l2-tbt` 仍需登录并受 VIP 等级限制，但当前 receive-time Stage 0 所需的 10ms BBO 与公开 trades 不依赖认证。

CryptoHFTData 的来源边界也已明确：它是个人/第三方维护的 Binance orderbook collection，不是 Binance 官方归档，缺日、缺小时、缺 snapshot 或连续段都可能发生。文件存在不代表 good day；必须通过 hourly parts、BBO/L2 coverage、gap、单调时间戳和 label horizon audit，失败日期不能进入 retained manifest。

### 2026-06-20 C++ live 边界重构：compact context / persistent features

早期 scalar C++ live benchmark 明显慢于 Python：`_compute_quotes` 约慢 91%，`quote + policy` 约慢 59%，`SIGNAL_FEATURES` 也因每 10s 重拷贝 bars/history 而近乎翻倍。profile 显示瓶颈不在 quote 数学本身，而在跨语言边界：每 tick 重建约 70 个配置字段、14 个 state 字段、5 个 prediction 字段、整本 depth levels，以及两个完整 `quote_context`/diagnostics dict；旧 signal overlay 则每次把最多 320 个 `Bar1s` 和全部 feature history 逐字段转成 pybind 对象。

本轮按“状态常驻、热路径紧凑、完整对象按需物化”重构：

- `QuoteCoreConfig` 在 Python/C++ 两侧按配置对象缓存；配置热重载时清空 MakerEngine cache。
- 新增 `compute_quote_core_live` flat binding：state/pred 用紧凑 tuple，depth 由 C++ 直接读取 Python level sequence，不再为每档盘口构造 `DepthLevel` Python wrapper。
- quote EV 关闭时，C++ 每 tick 只物化 side policy 需要的 adverse/defense/TTL/local-extreme/depth 字段，以及周期诊断日志实际读取的少量 diagnostics；bid/ask quote EV 任一开启时自动回到完整 context，避免模型特征缺失。
- 新增 C++ `SignalFeatureEngine`，持续持有最近 320 根 1s bars 和最多 60,480 条 10s history；Python 每秒增量推送一根完成 bar、每 10s 推送一条 history，不再在计算点批量复制整个窗口。
- `SIGNAL_STATE` 的逐事件 scalar pybind 仍没有净收益，因此保持默认关闭；没有为了“C++ 覆盖率”强行启用。
- fill model lazy-load 失败也会被缓存，避免依赖缺失时每 tick 重试 import/load。

固定环境复测：Python 3.12.13、AppleClang Release build、`--n 10000 --signal-n 1000`、ML 关闭以隔离 quote/feature CPU、每项 3 次取 mean 的中位数。

| 路径 | Python | 重构后 C++ | 变化 | 决策 |
|---|---:|---:|---:|---|
| live `_compute_quotes` | 42.85 us | 34.53 us | **-19.4%** | C++ compact path 有小幅净收益，仍为显式 opt-in |
| quote + policy | 74.55 us | 62.32 us | **-16.4%** | pybind 往返不再吞掉 quote core 收益 |
| 10s signal features | 803.20 us | 563.26 us | **-29.9%** | persistent C++ state 可继续 shadow/parity 验证 |
| 1s WS ingest (`SIGNAL_STATE`) | 22.42 us | 23.44 us | **+4.5%** | 仍慢，保持关闭 |

当时相关 quote/signal/tick replay parity 共 `17 passed`。这些数字只描述同机 synthetic CPU isolation，不代表网络、模型推理或端到端事件延迟；特别是 quote EV 开启后需要完整 context，收益会缩小。默认配置仍不因本次 benchmark 自动切换 C++。

### 2026-06-21 C++20 容器与类型现代化

构建标准从 C++17 升级为 C++20（`CMAKE_CXX_STANDARD=20` + target-level `cxx_std_20`）。本轮不改变 Python API、策略参数或运行配置，目标是消除 persistent signal/replay 内部的长期分配和类型歧义：

- `ArrayView<T>` 改为受 `Arithmetic` concept 约束的 `std::span<const T>`；NumPy 仍由 pybind 在 native 调用期间持有，C++ 使用无所有权连续视图。二维 L2 暂保留 `MatrixView`，因为标准 `mdspan` 属于 C++23。
- `SignalFeatureEngine` 的 320 bars / 60,480 history 从 `vector.erase(begin())` 改为固定容量 `CircularBuffer<T>`，满载后覆盖 head，push 从 O(N) 变为 O(1)。新增 wrap test，将多轮覆盖后的 persistent 输出与 stateless retained tail 逐项比较。
- 80 个 signal overlay 字段由 `std::map<std::string,double>` 改为 `SignalFeatureVector = std::array<double,80>`，使用 `enum class SignalFeatureId` 与 compile-time `std::array<std::string_view,80>` 注册；只有 pybind 返回 Python 时才物化 dict。窗口配置使用 `std::to_array`，名称查找使用 `std::ranges::lower_bound`。
- feature 统计函数改接 `std::span`/`subspan`，rolling return、vol regime、volume/trade intensity 直接索引 retained tail，不再先构建 60,480 长度的 `prev_closes/all_log_rets/abs_rets/vol6h_history` 临时 vector。小型 scratch vector 使用 `std::pmr::monotonic_buffer_resource`。
- replay 的 order state、bias side、quote-EV action、outcome、cancel reason 改为强类型 `enum class`；pybind property 仍返回原字符串，CSV/trace schema 不变。bid/ask orders 与 pending markouts 使用单次 replay 生命周期的 PMR arena，返回 summary/trace 保持普通 owning vector。
- 低风险现代化包括 `std::erase_if`、`concepts/requires`、`[[nodiscard]]`、`std::source_location`（未知 feature 诊断）以及仅放在非法输入/emergency 稀有分支的 `[[unlikely]]`。单个 replay 仍严格串行；后续只在逐行独立的 quote-core depth batch 增加了显式 `workers`/`std::jthread`，默认仍为 1。

验证与性能边界：

| 项目 | 结果 | 解释 |
|---|---:|---|
| C++20 core `-Wall -Wextra -Wpedantic` | 0 warning | quote/replay/streaming 三个核心编译单元 |
| synthetic/parity | `18 passed` | 新增 ring wrap + 全 80 feature parity |
| real-data golden | `4 passed` | May normal/high activity、Feb sparse、Jan cross-day A/B window；Python/C++ summary、PnL path、fills、inventory-time 与 trace 长度保持一致 |
| 满 60,480 history 的旧 vector 头删 | 80.05 us/push | 独立 `-O3` 容器微基准，不代表端到端延迟 |
| 满 history 的 ring push | 0.0026 us native；0.15 us 经 pybind | O(1) 覆盖；主要剩余成本是 Python/C++ 调用边界 |
| 满 history 的 80-feature compute | 111.23 us mean | 500 次调用；不再扫描/复制无关完整历史数组 |
| 常规 500-history 10s live path | 557.32 → 553.17 us | 约 -0.7%，短历史下原 O(N) 搬移尚未成为瓶颈 |
| quote + policy | 63.09 → 63.18 us | 基本不变，说明本轮容器改造未给无关 quote 路径制造收益假象或明显回归 |

结论：C++20 本身不是加速来源；真正收益来自它支持的清晰 view/固定布局与本轮数据结构重写。短时 benchmark 基本不变，长期满载 signal state 的最坏复杂度和分配压力被消除。所有 native 开关继续显式 opt-in，`live/config.yaml` 不因本轮改动自动调整。

### 2026-06-21 replay/depth/feature hot-path 第二轮

在真实窗口 golden parity 固化后，继续按 cache footprint 和 allocation profile 收紧研究路径：

- quote core 新增 `DepthView`/`DepthSideView`：既可 view owned `DepthSnapshot`，也可直接 view NumPy L2 price/qty 矩阵的一行。tick replay 与 `compute_quote_core_batch_depth` 不再逐次构造 `vector<DepthLevel>`；pybind 原有 `DepthSnapshot` API 保持兼容。
- replay loop 的 current-time BBO/L2 snapshot 改为 `bbo_idx/l2_idx` 单调推进，避免每次 requote 二分。订单 queue 仍以未来 `activate_ts` 查询，latency jitter 下该时间序列不保证单调，因此该路径有意保留 binary search，避免 cursor 前视或回退错误。
- `ReplayOrder` 拆成热状态与 cold trace：热结构只保留价格、remaining、queue、state/timestamps、四个 policy flags 和一个 nullable trace pointer；只有 `trace_quotes_max`/`trace_fills_max` 开启时，才从 replay arena-backed PMR pool 分配完整 `TraceOrderRow`。RAII deleter 在订单退出时归还 slot，避免 monotonic-only trace 在长窗口持续累积。
- fill trace 的 markout/window 定位改用 `lower_bound`/`upper_bound`，不再从第 0 条 trade 扫到窗口。trace 仍是诊断路径，默认关闭。
- `TickReplayParams.collect_curves` 默认 `true` 保持单次 API；C++ sweep、cap A/B、`tick_ab.py` 与 quote-EV A/B 自动设为 `false`。Sharpe/max drawdown 在 C++ 在线累计，因此 summary-only 不依赖三条 PnL/inventory vector；curve 模式按 elapsed/min requote 预留容量。
- `SignalFeatureEngine` 用滑窗可删除 Welford moments 增量维护 2,160/8,640 条 `return_abs` 与 60,480 条 `vol_regime_6h`。新增 `compute_values()` 固定顺序 NumPy array 和一次初始化的 `SIGNAL_FEATURE_NAMES`；旧 dict API 保留。
- depth batch 的每行独立计算支持显式 `workers=N`，并贯通到 `cross_market_shock_audit.py --quote-context-workers N`；每 worker 至少 4,096 行才创建线程。默认 `workers=1`，防止小 batch 和外层 day/arm 多进程发生过度订阅；单个 tick replay 仍不做内部并行。

同机 Release build 实测（Python 3.12.13，Apple Silicon）：

| 路径 | 结果 | 边界 |
|---|---:|---|
| 满 60,480 history `compute_values()` | 1.90 us mean | 第二轮增量 moments；第一轮完整扫描约 111.23 us |
| 满 history 兼容 dict 输出 | 4.42 us mean | 额外成本来自 80 个 Python dict entries |
| depth batch 100k×10 levels，1 worker | 35.48 ms / 2.82M rows/s | 全程 `DepthView`，无逐行 depth vector |
| 同 batch，4 workers | 10.08 ms / 9.92M rows/s | 3.52x；仅逐行独立 batch 显式启用 |
| 2M trades replay，保留 200k 曲线点 | 42.18 ms | 三条曲线 vector |
| 同 replay summary-only | 41.77 ms，0 曲线点 | CPU 约 1% 改善，主要收益是内存与 Python 转换 |

验证：快速 suite `20 passed, 4 skipped`；May normal/high、Feb sparse、Jan A/B 月窗口 real-data golden `4 passed`；C++20 core 在 `-Wall -Wextra -Wpedantic` 下 0 warning。短历史 live benchmark 没有稳定改善，因此这轮仍不修改 `live/config.yaml` 或默认 live native 开关。

### 2026-06-26 ARM 本机优化与 x86 live 的边界

本机训练/回测环境是 Apple Silicon/ARM64，而实际 live 常运行在 x86 Linux。CPU cache、memory ordering、线程调度、LightGBM/OpenMP 和原子/锁实现都可能不同，因此本机 benchmark 只能证明“本机离线研究路径有效”，不能直接证明 x86 live 会受益。

应标注为 **本机有效但 live 不一定有效** 的优化：

- `research/families/f03_causal_13_head/ml_model.py` 的 Apple Silicon/ARM64 假设：LightGBM native ARM64、NEON、统一内存和本机核心数只影响本机训练吞吐；x86 研究机或 live 机器应单独设置 `MM_LGB_THREADS`、`MM_LGB_HIST_POOL_MB` 并重测。
- 所有 multiprocessing sweep：`backtest_tick.py --sweep/--cap-ab/--sweep-cooldown`、`tick_ab.py --workers`、quote EV A/B workers 优化的是离线多 arm/day/window wall time，不代表 live 单线程决策尾延迟。
- `compute_quote_core_batch_depth(..., workers=N)` / `cross_market_shock_audit.py --quote-context-workers N`：逐行 batch 独立时在本机可加速，但与外层 multiprocessing 叠加容易过度订阅；x86 上 worker 数必须重新扫。
- tick replay 的 allocator/cache locality benchmark：`DepthView`、hot/cold order、summary-only curves、PMR trace pool 改善的是 replay 内存运动和 Python 转换，不等于 live quote/routing 自动更快。

应在 **x86 live 目标机器单独 benchmark** 的开关：

- `NARROWGATE_CPP_QUOTE_CORE=1`：验证 compact quote core 在 x86 上的 p50/p99/p99.9，而不是复用 Apple Silicon 的 16% 左右收益。
- `NARROWGATE_CPP_LIVE_ROUTING=1`：验证 tuple ABI 和 policy bundle 在 x86 CPython/pybind 下是否仍减少固定税。
- `NARROWGATE_CPP_SIGNAL_FEATURES=1`：验证 fixed-array feature kernel 在真实 event/feature/model merge 下的净收益；失败的 `NARROWGATE_CPP_SIGNAL_STATE` 实验实现与开关已删除。

当前 native 代码没有把 `std::atomic` 当 hot path 优化手段，C++ 内部显式并发主要是 depth batch 的 `std::jthread` 分片。若后续引入 atomics、lock-free queue 或线程池，必须在 ARM 与目标 x86 上分别用 perf/PMU 看 L1I、iTLB、branch miss、cache miss、上下文切换和 tail latency；尤其不能把 ARM 上的原子/自旋成本推断为 x86 live 成本。

本轮在一台私有 x86 Linux live/benchmark 主机上做了一次基准搭建：Amazon Linux 2023、Intel Xeon 系列 vCPU、L1d/L1i 均 32 KiB、clocksource=`tsc`。远端初始没有编译工具和 repo，因此安装 `gcc-c++`、`cmake`、`ninja-build`、`python3.11-devel`、`perf`，并只同步代码，不同步本机 data/features/models/logs。第一次按完整 `requirements.txt` 安装暴露出 live 部署问题：Linux 上 `torch` 会拉 CUDA wheels，小根盘很容易被训练依赖污染；x86 live/benchmark venv 改为轻量依赖（numpy/pandas/pyarrow/lightgbm/scikit-learn/scipy/pyyaml/requests/binance/websocket/pycryptodome/zstandard/pytest）+ `pip install -e cpp`。

新增两个可复跑脚本：

- `scripts/x86_live_env_audit.sh`：记录 OS、CPU/cache、clocksource、sysctl、IRQ、Python/narrowgate_cpp import 和 NumPy backend。
- `scripts/x86_live_benchmark.sh`：固定 `OMP_NUM_THREADS=OPENBLAS_NUM_THREADS=MKL_NUM_THREADS=NUMEXPR_NUM_THREADS=1`、`MALLOC_ARENA_MAX=1`，依次跑 Python baseline、C++ quote core、C++ signal features、候选组合和 compact live-routing ABI。
- `scripts/x86_live_soak.sh`：只跑 x86 候选组合 `QUOTE_CORE + SIGNAL_FEATURES` 的长样本 soak，默认 `--n 100000 --signal-n 10000`，并附带 compact routing ABI `--n 200000`。

同一 x86 机器、同一线程环境、ML 关闭、`--n 10000 --signal-n 1000` 的结果：

| 开关 | signal 10s mean/p99 | `_compute_quotes` mean/p99 | `quote+policy` mean/p99 | 结论 |
|---|---:|---:|---:|---|
| Python baseline | 3431 / 4159 us | 152 / 201 us | 240 / 305 us | x86 baseline |
| `NARROWGATE_CPP_QUOTE_CORE=1` | 3328 / 4233 us | 121 / 158 us | 210 / 270 us | x86 上 quote core 有净收益 |
| `NARROWGATE_CPP_SIGNAL_FEATURES=1` | 1973 / 2347 us | 153 / 194 us | 237 / 302 us | x86 上 signal feature 与 ARM 结论相反，明显有收益 |
| `QUOTE_CORE=1 + SIGNAL_FEATURES=1` | 1949 / 2199 us | 120 / 160 us | 209 / 269 us | 当前 x86 live 候选组合 |

随后在同一 x86 benchmark 机型上做更长时间 soak：baseline 与候选各 `--n 100000 --signal-n 10000`，routing compact ABI `--n 200000`，这次 benchmark 已输出 p99.9。

| 路径 | Python baseline mean/p99/p99.9 | `QUOTE_CORE + SIGNAL_FEATURES` mean/p99/p99.9 | 观察 |
|---|---:|---:|---|
| signal 10s features | 9610 / 17558 / 40658 us | 2167 / 4534 / 16115 us | 长历史下 Python feature path 尾延迟很大，C++ fixed-array 显著收敛 |
| live `_compute_quotes` | 172 / 236 / 4380 us | 123 / 166 / 225 us | C++ quote core 降低均值，并避免本轮 quote-only p99.9 尖刺 |
| `quote + policy` | 249 / 308 / 364 us | 219 / 281 / 358 us | 组合路径稳定小幅改善，policy 仍是 Python 主体 |
| compact routing ABI | n/a | 1.17 / 1.24 / 2.03 us | tuple ABI 固定税很低，适合作为 routing bundle 边界 |

历史 `NARROWGATE_CPP_SIGNAL_STATE=1` 实验没有额外收益，2026-07-17 已删除；pin 到单个 vCPU 对均值帮助不明显，且在 VM 上可能放大 p99 抖动。compact live-routing tuple ABI 单独测得约 `1.23 us mean / 1.63 us p99`，长跑为 `1.17 us mean / 2.03 us p99.9`，跨语言固定税已经很低。该 KVM 实例不暴露 `cycles/instructions/L1-icache/iTLB` 等硬件 PMU 事件，`perf stat` 只能看到 context-switch、cpu-migration、page-fault，因此最终 promotion 仍需在实际 live 机型重复 `scripts/x86_live_soak.sh` 并记录 p99/p99.9；本轮结论只允许把 `QUOTE_CORE + SIGNAL_FEATURES` 标为 x86 候选，不自动修改 `live/config.yaml`。

2026-07-01 在当前私有 live 机复测：Amazon Linux 2023、Intel Xeon 系列 vCPU、当前 venv / 当前模型 / 当前配置。短跑 synthetic benchmark 与真实 live telemetry 必须分开读：

- synthetic `bench_live_path.py --n 100000 --signal-n 10000`：`QUOTE_CORE + SIGNAL_FEATURES` 把 `signal 10s features` 从 `15970 / 50395 / 159093 us` 压到 `3630 / 14858 / 42108 us`（mean / p99 / p99.9），`quote+policy` 从 `486 / 1412 / 6592 us` 小幅降到 `438 / 1175 / 5128 us`。`_compute_quotes` 均值下降但 p99/p99.9 混合，不能单独作为打开 quote core 的理由。
- 当前真实 live 进程的旁路 `logs/live_perf_telemetry.csv` 显示：requote 与 order-update 的 p99/p99.9 主要由 REST cancel/new、同步 replace 和 signal compute 长尾驱动，quote math 本身不是最大尾部来源。公开文档不记录这一段私有 live p50/p99/p99.9 明细。
- WebSocket freshness 本轮整体正常。这说明当前尾延迟问题首先是 REST/order lifecycle 与 signal compute，而不是 WS 长时间 stale。

因此 current-live 结论更新为：C++ fixed-array signal path 在 x86 synthetic soak 中有明确 CPU 收益，但真实 live promotion 不能只看函数微基准；需要继续用 `live_perf_telemetry.csv` 对比开启/关闭 native flags 后的真实 requote p99/p99.9，并把 REST new/cancel tail 与计算 tail 分开报告。

2026-07-02 继续在当前 live 机读取真实 `logs/live_perf_telemetry.csv`，不改策略参数。当前 live 进程环境已开启 `NARROWGATE_CPP_QUOTE_CORE=1`、`NARROWGATE_CPP_SIGNAL_FEATURES=1`、`NARROWGATE_CPP_LIVE_ROUTING=1`，并设置 `OMP_NUM_THREADS=1`、`OPENBLAS_NUM_THREADS=1`、`MALLOC_ARENA_MAX=1`。私有 telemetry 显示 compact routing 基本进入真实 live 路径；但尾延迟仍主要来自 REST/order lifecycle，`compute_quotes_us` 已经不是 p99 主因。系统侧下一步不是再搬单个公式，而是继续降低不必要 replace：先评估 exposure-increasing side 更高 replace threshold、pending replace coalescing、以及 eventual async order manager；减仓方向和安全撤单仍必须保持更敏捷，不能为了 p99 阻断库存修复。

同日实现第一层 replace lifecycle 收口：`replace_pending_coalesce=true` 默认开启，只在本地同侧订单处于 `PENDING_NEW/PENDING_CANCEL` 时 coalesce 新 replace，等待交易所/order stream 状态收敛；`replace_cancel_first_exposure_increasing=false` 默认关闭，作为后续 soak arm 可让加仓侧 replace 先撤旧单、下一轮再补新单。该路径只处理 REST/order lifecycle p99，不改变 quote alpha；forced TTL、pause/stale-data、库存上限撤单、减仓侧 repair 不走 cancel-first。评估口径必须是 `live_perf_telemetry.csv` 的 action mix、REST p99/p99.9、placed/fill、双边报价完整性和库存修复影响。

2026-07-10 把这一条系统链路补成可复跑实现。`live/profiles/native.env` 固化 `QUOTE_CORE + SIGNAL_FEATURES + LIVE_ROUTING + STRICT` 与单线程数值库环境，`python.env` 只切换实现路径；`run.sh status/profile` 会显示持久化状态，`main.py` 启动时验证 native API 和 `.so` 来源。`scripts/analyze_live_soak.py` 用 CSV/log line marker 排除 restart warmup，并统一输出 action mix、REST/gateway 与 requote/signal/quote p50/p95/p99/p99.9、native hit、WS age、placed/fills 和安全日志计数。

同轮曾新增 `strategy/order_gateway.py`：BUY/SELL 各一个容量为 1 的 latest-wins intent slot，用来验证正常 quote REST 是否值得从同步主路径迁出。该实现只属于当时的系统实验，不是策略机制。

第一轮同配置短窗 preflight 的读法已经固化。Python-sync 与 native-sync 都使用相同线程限制、策略配置和 marker schema；native-sync 的 `cpp_routing_used=100%`，`requote_total_us` p50/p99 约从 `44.5/434.7ms` 降到 `40.5/318.3ms`，`compute_quotes_us` p99 从约 `14.4ms` 降到 `3.9ms`。两个窗口相邻而不重叠，所以这些只能证明部署方向和数量级，不能当因果收益。

随后保持 native profile、只打开 async gateway 做 10 分钟安全 preflight：gateway 窗口内 `178` 次 submit、`164` 次 cancel、`165` 次 place，`coalesce/timeout/error=0/0/0`；new/cancel p99 约 `172/184ms`，WebSocket freshness 与双边订单状态正常。`update_orders_us` p50 从同步 native 的约 `36.8ms` 降到 `15.5ms`，但 p99 因两个进程级尖刺升到约 `833ms`；同一时刻 signal/quote 也同时出现长尾，且每个 10 分钟窗口只有约百个 requote 样本。这一短窗只作为进入长 soak 的 preflight，不是 promotion 证据。

194.4 分钟 native-async 长 soak 已在 2026-07-10 完成。native routing hit 为 `99.64%`，双边 fills 为 `24/27`，WebSocket 无持续 silence，TTL/stale/safety cancel 仍工作；gateway 窗口 `coalesce/timeout/error=1/0/1`，说明真实 pending 覆盖几乎没有形成。与相邻 native-sync 窗口相比，`update_orders_us` p50 从约 `36.8ms` 降到 `15.9ms`，但 requote p99/p99.9 约升到 `720/1075ms`，update-orders p99/p99.9 约升到 `444/855ms`，signal 与 quote 尾部也同时变差。相邻窗口不能给出严格因果结论，但没有证据支持用更复杂线程模型替换同步生产路径；因此保持同步 order adapter，native C++ profile 保留，replace threshold 不因本次 system soak 额外调整。2026-07-17 已删除异步实现、开关、telemetry 和专属测试。

### 2026-06-22 live routing 固定布局 / signal 单一机器码主体

按“最小且可预测的热指令工作集”审计后，修复了两个仍会给 CPU 前端和 pybind 边界增加固定成本的实现：

- `compute_live_routing_decision` 从 binding 内的大 lambda 拆为纯 C++ `LiveRoutingInput` / `LiveRoutingPolicy` / `LiveRoutingResult`。Python 每次 routing 只传一个 22 字段 state tuple 和两个 5 字段 policy tuple，C++ 返回固定 11 字段 tuple；删除每 tick 的两次 `SidePolicyDecision.asdict()`、字符串 key 查询和结果 dict。真正报单时为审计日志构造的 `asdict` 保留，因为它只发生在 place/replace 冷路径。
- `compute_signal_feature_vector<Bars, History>` 改成单一非模板主体，输入为 `SegmentedSpanView<Bar1s>` / `SegmentedSpanView<FeatureHistoryRow>`。`CircularBuffer` 暴露 head-to-end + begin-to-head 两段 span；legacy vector API 使用单段 span，因此无需复制 320 bars / 60,480 history，也不再为 ring/vector 各生成一份完整 feature 函数。
- `bench/bench_live_routing_bridge.py` 只保留当前 compact ABI 边界基准；已失效的 dict ABI 不再提供可运行入口。

同机、同输入、Release + pybind ThinLTO 对照：

| 项目 | 修改前 | 修改后 | 结果 |
|---|---:|---:|---|
| routing binding mean | 6.024 us | 0.472 us | -92.2%，checksum 完全一致 |
| routing binding p99 | 6.375 us | 0.583 us | -90.9% |
| 满 60,480 history `compute_values()` mean | 1.901 us | 1.748 us | -8.0% |
| 同路径 p99 | 2.500 us | 1.917 us | -23.3% |
| signal 相关大计算代码 | 8,868 + 10,120 B | 9,184 + 1,136 B | 共享主体后减少 8,668 B |
| 扩展 `__text` | 478,392 B | 471,364 B | -7,028 B（-1.47%） |

`__text` 仍大于 L1I，但不能把 pybind 注册、batch、trace 和异常代码都算作一次 live 事件的热工作集。当前 quote core 仍为 7,528 B、replay 主循环 23,596 B；下一步判断 I-cache 风险仍应在目标 Linux CPU 上看 L1I/iTLB/branch miss 与 p99/p99.9，而不是继续为了缩小整个模块盲目拆 API。验证为相关 parity/contract `22 passed`，未修改 `live/config.yaml`，native routing 继续显式 opt-in。

---

## 四、数据管线

### 4.1 数据源与规模

| 数据 | 来源 | 期间 | 大小 | 说明 |
|------|------|------|------|------|
| perp aggTrades | data.binance.vision | 当前正式本地口径为 2026 retained/live-aligned daily | retained daily CSV | 逐笔成交聚合（与 historical orderbook 对齐）；不要保留跨坏日期的非日度 `aggTrades` |
| futures raw trades | data.binance.vision | BTCUSDC、BTCUSDT 均覆盖 111 个 2026 minimal complete good days | retained daily CSV | 单笔 trade ID/sequence；BTCUSDC 用于 execution event clock，BTCUSDT 用于 reference flow。它们不能替代 BBO/L2；清单见 `logs/data_audit/cleanup_20260705_align_btcusdt_reference/minimal_complete_good_days_2026.csv` |
| spot aggTrades | data.binance.vision | 2026 retained/enhanced 窗口按需 | retained daily CSV | Binance spot `daily/aggTrades`，用于 `cv_exec_spot_*` / `cv_ref_spot_*` enhanced cross-market features |
| bookDepth 百分比桶 | data.binance.vision | removed legacy source | none | 粗粒度百分比桶不是逐档 L2；2026-07-17 已删除 downloader、preprocessor、feature fallback 与 replay proxy |
| Metrics (OI/LS) | data.binance.vision | 2026 retained/enhanced 窗口按需 | daily CSV + daily parquet | 多空比/OI 5 分钟，已按日接入 `metrics_5m` |

**2026-06-28 历史粒度审计（已由 2026-08-02 continuous substrate 扩展）**：当时为隔离坏日和未冻结的跨日拼接，正式证据收敛到 UTC 日 fresh-start rows，并清理非日度 parquet 容器。该结论仍约束所有冻结为 `fresh_start` 的历史 identity，但不再表示项目禁止跨日状态。当前 v10 live identity 和 continuous-path scorecard 要求现金、库存及经济 campaign 跨午夜延续，UTC 日只作统计 cluster；带缺口日历必须使用版本化 restart manifest，在维护边界终止活动单并保留持仓 MTM。每日与连续口径不可混用，必须由实验 Spec 明确冻结。

### 4.2 预处理流水线

```
retained daily perp/spot aggTrades CSV
  ↓  features/preprocess.py — 按时间聚合
1s bars (parquet, 按 UTC 日文件)
  ↓  features/feature_engineer.py — 滚动窗口 + 时间编码 + metrics join
10s 特征矩阵 (parquet, 按 UTC 日 feature containers + dataset 汇总)
  ↓  划分
默认 `ml_model.py` 读取 `features_btcusdc/dataset_train.parquet`、`dataset_val.parquet`、`dataset_test.parquet`。2026-07-05 曾清理旧 2025 缓存，但后续 Tardis/官方多源补数已建立新的 source-aware 2025 universe；causal-v12 semantics-v6 使用其中 66 个 provider-normalized 2025 日训练，并在 2026 native panel 上做 transport/economic 诊断。该数据只能按其 source identity 使用，不能伪装成 native exact queue/lifecycle。完整边界见 `docs/expanded_research_day_universe_20250801_20260731.md`。
```

**训练样本时间衰减权重**: `w(t) = exp(−λ × months_ago)`，λ=0.1 (半衰期 ~7 个月)，通过 LightGBM `sample_weight` 传入。

### 4.2a 原生盘口流水线

```
CryptoHFT BTCUSDC orderbook hourly parquet.zst
  ↓  data/download_cryptohft_orderbook.py — 本地重放 order book
daily bbo / l2 parquet
  ↓  features/feature_engineer.py:add_execution_l2_features
10s execution-L2 特征 (state + flow)
```

说明：

- 原有 `bbo/`、`l2/` 约为一秒状态容器，继续保留给明确声明该身份的 baseline/parity 任务；
- `replay_l2_retained100ms_v1/{bbo,l2}` 是单独构建的 retained event-L2 研究根，不会覆盖 legacy root。它使用 BTCUSDC individual trades + CryptoHFT price-level delta + merged timer clock；
- `delta-converged` 产物只在显式 burn-in、top-20 真值比较和逐日 sequence gate 后用于 shock/depletion/refill/recovery 研究；它不是 native snapshot 或 deep-queue truth；
- 2026-07-18 retained111 最终构建为 93,456,377 行 BBO + 同量 top-20 L2；80 日 sequence 连续、101 日覆盖率不低于 99%，严格交集为 76 日。原始 logical L2 message 平均间隔 46.52ms，96.10% 不超过 100ms；normalized wide state 仍明确标成 100ms，不冒充 10ms event tape；
- native snapshot 必须按 exchange event/update identity 聚合价格行，不能按 recorder receive timestamp 拆分。修复后误报 duplicate snapshot 从 28,107 降至 94，73 个真实 sequence gaps 保持不变；
- `depth_1s` 仍保留为日文件，但主要用于旧代理特征与部分历史回测近端深度近似，不再是 ML 主线输入。

### 4.3 盘口数据边界

Data Vision daily `bookDepth` 只是百分比桶聚合，不是逐档 L2，也不是运行时 `depth20@100ms` 的历史版本。2026-07-17 已删除这条 legacy 下载、预处理、feature fallback 和 replay proxy 链。正式 ML/replay/queue/BBO-event 只使用 CryptoHFT 重建后的 `bbo/`、`l2/`，或 live-captured bookTicker/diff-depth 事件流。

Binance individual `trades` 只能恢复公开逐笔撮合和 aggressor flow，不能恢复被动撤单与 refill；后两者必须来自 `U/u/pu` price-level L2 delta。事件研究因此使用两条输入流，而不是把非 aggTrade 误称为 L2。当前 normalized event-L2 使用 Binance transaction time；独立 trades 与 book archive 没有跨流统一 sequence，同毫秒先后顺序不可观测。现阶段 artifact 必须标记为 `exchange_time_asof_le_ideal_latency_diagnostic`。正式 action replay 还要把 exchange-time matching 与 EC2 receive/feature-ready visibility 拆成双时钟。完整身份、真值比较和命令见 `docs/retained_event_l2_rebuild_20260718.md`。

### 4.4 Metrics 预处理

`features/preprocess_metrics.py` 将每日 OI/多空比 CSV 聚合为 `metrics_5m` parquet，事件引擎通过 REST 轮询同步。

---

## 五、特征体系 (工程输出 vs 模型输入)

| 类别 | 特征数 | 代表特征 |
|------|--------|---------|
| 微结构 (rolling) | 43 | volatility (5s/30s/60s), volume_imbalance, vpin, price_velocity |
| Tick momentum | 14 | tick_streak, tick_mom, micro_ret_skew/kurt |
| ~~Depth~~ | ~~15~~ → **0(默认禁用)** | ~~depth_imb, depth_slope, depth_concentration~~ |
| Vol regime | 3 | vol_regime_6h, vol_regime_24h, vol_regime_zscore |
| 时间编码 | 19 | hour_sin/cos, session/funding/US equity features |
| Metrics | 13 | oi_log, oi_zscore, ls_ratio, taker_ls_momentum |
| 其余派生列 | 若干 | bar级基础列、统计辅助列 |
| **工程输出** | **约 100+ 列** | 具体以当期 `features_*.parquet` 为准 |

> 当前模型输入维度不再用单一“全局维数”描述，**以各模型对应的 `*_meta.json` / `feature_cols` 为准**。不同 head（如 `dir/vol/ret` 与 `tox_*`）可以使用不同的输入列集合。Transformer 与 ret-stacking 兼容层已删除；当前 runtime 只加载 metadata-driven LightGBM heads。

**Depth 特征移除原因**: Data Vision 的 `bookDepth` 是百分比聚合桶，事件引擎当前订阅的是 `partial_book_depth` top 20 档 + `bookTicker`。top 20 档只能覆盖极近盘口，无法还原 ±0.2% 到 ±5% 的完整百分比桶；REST `/fapi/v1/depth` 也只是当前快照，最多 1000 档，仍不等价于 Data Vision 历史桶。因此 ML 训练继续排除 15 个 depth/notional 特征。事件引擎可以聚合已收到的 top-N depth 做 microprice、动态 κ 或近盘口 imbalance，但不能忠实生成 Data Vision 格式，除非改成维护更深的本地 order book 并配合可覆盖远端桶的数据源（如自采 diff depth 或 Tardis 这类历史原生 L2）。移除的特征: depth_imb_02, depth_imb_1pct, depth_imb_2pct, depth_imb_5pct, depth_slope_bid, depth_slope_ask, depth_concentration, notional_imb_02, depth_imb_02_d3, depth_imb_02_d6, depth_imb_02_ma6, depth_imb_02_ma30, depth_imb_divergence, bid_depth_02_norm, ask_depth_02_norm。

**Depth 执行层使用口径**: depth 不再作为 ML feature，但可以作为 maker 执行状态使用。当前事件引擎 WebSocket `partial_book_depth(level=20, speed=100ms)` 已用于 low-latency 执行层；REST `/fapi/v1/depth?limit=1000` 只能拿当前快照，适合启动/低频刷新更宽的盘口参考、校准 depth baseline 或在 WS 缺口后兜底，不适合每次 requote 高频调用。推荐分层如下:

| 用途 | 推荐数据源 | 当前状态 | 说明 |
|------|------------|----------|------|
| Microprice | WS top1/top3/top5 | 公开模板 `use_bar_pricing=false` | BTCUSDC live 使用近盘口；实际私有 baseline 仍以运行配置为准 |
| Order-book imbalance | WS topN | 代码入口存在，`book_imb_strength=0.0` | 可只影响报价 asym，不进入 ML；需 clean sweep 后再打开 |
| 动态 κ / spread | WS top5/top20 + 低频 REST baseline | 动态 κ 已接入；额外 spread 缩放未上线 | 薄 book 应降低 κ/扩大保护垫，厚 book 可更激进；baseline 可用 REST 低频校准 |
| Fill probability / queue risk | WS best levels + 自身订单状态 + 最近成交流 | backtest 有 `maker_fill_prob` 近似，事件引擎未做显式 queue 模型 | 只能估计队列风险，不能精确知道自身前方队列；可用于 quote TTL/撤换优先级 |
| Adverse selection / toxicity filter | ML tox + WS imbalance/microprice drift | ML tox gate 代码已接入但 BTCUSDC 当前默认关闭；adverse/defense guard 以 markout 为主 | depth 只能作为执行层二级触发器或 spread 放大器，不能直接替代 tox 模型 |

**Depth execution shadow 配置**: `live/config.yaml` 已定义 `depth_execution`，按三组拆开。公开模板中 `shadow_enabled=false`，三组真实交易开关也全部为 `false`；私有配置若只开启 shadow，则仅记录 `DEPTH_SHADOW` 候选指标，不改报价、不撤单、不停报。后续只有 clean A/B 证明有效时，才单独打开某一组 `enabled=true`。

| 配置组 | 真实开关 | 默认 | 候选指标/作用 |
|--------|----------|------|----------------|
| `microprice_kappa` | `enabled` | `false` | top-N microprice shift、depth-adjusted κ ratio；打开后可让 depth microprice/κ 参与报价 |
| `imbalance_asym` | `enabled` | `false` | top-N bid/ask qty imbalance；打开后只调整报价 asym，不进入 ML |
| `depth_tox_spread` | `enabled` | `false` | imbalance + microprice drift 的执行层 adverse-selection 指标；打开后只放大 spread，不做单边停报 |

**当前报价控制实现**：

- 事件引擎不再只靠分散的 `fill cooldown` / `tox gate` / `markout spread` 各自生效，而是先汇总成 **per-side quote policy**；
- 每边输出 `mode(normal/defend/pause)`、`allow_post`、`allow_exposure_increase`、`spread_mult`、`size_mult`、`reason_mask`；
- `flat_unilateral_max_s=120` 给 flat 状态的单边 exposure-only pause 设置最大存活时间：adverse guard 可以短暂只开一侧，但超时后必须恢复双边；恢复时仍保留 widen/size decay，且不绕过 stale、fill cooldown 或交易所硬门控；
- direct quote-EV 与 SELL resiliency live executor 已删除；相关历史字段只允许由 audit readers 解释旧日志，不能通过环境变量重新启用；
- `HEALTH` 继续输出库存时间暴露：`absInvTime`、`avgAbsInv`、`notionalInvTime`、`pnlPerInvHr`，用于观察 PnL 占用的库存风险预算；
- 当前落盘 `logs/quote_decisions.csv` 与 `logs/order_outcomes.csv`；后者记录 `placed/canceled/rejected/expired/filled` 等订单结果，成交执行事件统一为 `filled`。可通过 `quote_decomposition_tick.py` 生成 `raw_half_spread`、`raw_mid_shift`、`final_quote_delta_to_bbo`、lifetime、guard、markout 和 cancel reason。

**Live 运维启动口径**:

- 远端只使用 `live/run.sh start|stop|restart|status|profile|logs|reload` 管理 live 进程；
- 不要裸 `nohup python live/main.py`，因为它不会自动 source `live/.env`，容易导致 `BINANCE_API_KEY/BINANCE_API_SECRET` 缺失并启动失败；
- `live/run.sh` 会优先使用项目 `.venv/bin/python3`、写入 `logs/maker.pid`、先加载私有 `live/.env` 再加载非秘密 runtime profile，并在 `stop/restart` 时清理 orphan `live/main.py` 进程；
- 配置热更新仍用 `live/run.sh reload`；代码变更必须 `live/run.sh restart`，并在重启后检查 `HEALTH`、`ORDER_UPDATE`、`quote_decisions.csv` 和 `sell_resiliency_shadow.csv`。

**时段特征设计**:

| 时段 | 北京时间 | 特征 |
|------|---------|------|
| 亚洲 | 08:00-16:00 | 波动率偏低，spread 偏宽 |
| 欧洲 | 16:00-00:00 | 流动性提升，波动率上升 |
| 美洲 | 21:00-05:00 | 波动率最高，流动性最好 |
| 欧美重叠 | 21:00-00:00 | 成交量峰值 |

---

## 六、ML 模型

### 6.1 LightGBM 模型（最多 13 个 head）

代码支持 9 个基础 head（dir/ret/vol × 10s/30s/60s）和 4 个可选 toxicity head（bid/ask × 5s/10s）。运行时只加载模型 metadata 中实际声明的 head；公开模板 `ml.enabled=false`；私有 v10 operational baseline 则运行 causal-v12 semantics-v6、13-head ML-ON。causal-v12 在 2025 source-aware 训练与 2026 native transport 中保留部分排序和经济支持，但 research prediction/live authority 仍为 false；owner operational baseline 身份不能倒推成独立 prediction gate 通过。修复前 bundle 的树数、AUC/IC 和策略用途表已经删除，任何预测指标本身都不构成 action uplift。

### 6.2 Ret 模型在策略中的利用

旧 stacking 与 direct-ret-skew 的 IC、Sharpe 和 BTCUSDT 结论已经删除。`ret_skew` 仍是可选实现面，但 BTCUSDC 当前没有证据把收益率预测直接翻译成 reservation shift。

### 6.3 Empirical P3 成交校准

旧 SU-Johnson 参数与固定 delta/kappa 数值已删除，不再进入 feature、live 或 replay 真源。当前 10 秒 P3 由 bundle 内显式 empirical-survival artifact 提供，当前 v10 绑定的 causal-v12 artifact 记录 `delta_star=13.9990859817 USDC/BTC` 与 `effective_kappa=0.0673564264`。它们绑定 horizon、fill definition、数据和 artifact hash，不是可脱离校准身份单独搜索的固定策略参数。

### 6.4 历史 Transformer 研究（入口已删除）

早期 Transformer 配置、训练指标、blend 权重和 ret 输出诊断已经删除。仓库与 MarketData 均无可部署 artifact；2026-07-17 已删除模型实现、runtime loader/inference、torch live 依赖和 preflight。

### 6.5 历史 RL 研究（入口已删除）

早期 TD3 actor 只有 bar/SU-Johnson 环境中的研究结果和一个从未被调用的 lazy loader；它没有进入 live quote path，也没有 tick replay parity。2026-07-17 已删除训练入口、loader 和 `rl.*` 配置。旧指标只说明曾探索过该方向，不是当前模型、策略能力或可启用的 live 组件。

---

## 七、回测体系

### 7.1 Formal tick replay

当前正式研究入口是 `models/backtest_tick.py`：按 merged event clock 消费 individual trades、BBO/L2、timer 与 feature-ready rows，并重放 exact-level visible queue、empirical latency、cancel/replace、cooldown、guard 和 campaign path。旧 1s-bar touch/distance-decay 参数结论已经删除；bar runner 只能用于显式 legacy/exploratory 诊断，不能产出 promotion evidence。

### 7.2 回测-运行时对齐特性

以下功能在回测和事件引擎中均已实现，用于减少离线评估与运行时逻辑之间的偏差：

| 特性 | 回测实现 | 事件引擎实现 |
|------|---------|---------|
| Microprice | strict tick replay 使用原生 historical BBO/L2；`depth_1s` 仅为 legacy proxy | BTCUSDC 公开模板 `use_bar_pricing=false`，使用 WebSocket top-N depth；私有 baseline 以运行配置为准 |
| 动态 κ | strict tick replay 使用原生 L2；旧 bar replay 的 `depth_near` 仅作近似 | 运行时可用实时 top20 depth；与旧百分比桶不能视为逐桶完全对齐 |
| η 库存衰减 | `size × exp(−η × \|q\|/max_inv)` | 同公式 |
| Exit urgency | `asym += exit_urg × \|q\|/max_inv` | 同公式 |
| Book imbalance | `asym += imb × strength` (depth 30s→1s ffill) | 同公式，实时 100ms depth |
| 动态 RQ | `rq = rq_max × exp(ln(rq_min/rq_max) × vol_ratio)` | 同公式 |
| Anti-flip cap | 平仓方向 order ≤ abs(position) | 同逻辑 |
| Fee floor | `min_spread = 2 × fee × mid + tick` | 同逻辑 |
| **动态 spread L0 流动性** | `δ *= 1/√(trade_intensity/liq_baseline)` + clamp | 同公式，实时 trade_count |
| **动态 spread L1 vol regime** | `δ *= √σ²/vol_baseline` + clamp | 同公式 |
| **动态 spread L2 floor** | `if δ < 2×δ* → δ = 2×δ*` | `δ*` 从当前 P3 artifact 加载，不写死历史常数 |
| **动态 γ L3 库存升级** | `γ *= (1 + inv_ratio²)` (仅影响 reservation price) | 同公式 |
| **CJP 库存比例偏移** | `r -= φ·(q/q_max)·δ` (φ=inventory_skew_strength) | 同公式 |
| **Toxicity state** | 模型输出只作 score/guard 输入，具体 action 必须有独立证据 | 默认不以一个 toxicity threshold 直接决定 live action |
| **Quote-EV shadow** | split target: `P(fill)`、markout buckets、extreme adverse | direct executor 已删除，只保留训练与 shadow evidence |
| **ret_shift clamp+fading** | `rs_clamp = RSP×δ/2; if adds_exposure: r_shift *= (1−inv_ratio)` | 回测与运行时一致的 ret_skew clamp + 库存感知衰减 |
| **pred_ret EMA 去均值** | `debiased = raw − EMA(raw)`，α=2/(N+1), N=ret_demean_halflife | 回测与运行时一致的 pred_ret 去均值，消除动量偏置 |
### 7.3 早期机制验证归档

2026-06-20 前的 PnL/Sharpe 表、候选参数和 winner 已删除。相关代码机制若仍存在，也必须作为新 hypothesis 在当前 replay identity 下重新验证。

---

## 八、事件驱动报价引擎设计

### 8.1 系统架构

```
Execution feeds: Binance futures trade + depth + bookTicker
Reference feeds: Binance perp/spot anchors + optional Bitget/Bybit/OKX
Private feed: Binance user data
        │
        ▼
WS Handler ── decode / route / watchdog / reconnect / optional market tape
        │
        ▼
Signal Engine ── bars + global/reference state + optional model bundle
        │
        ▼
Maker Engine ── quote core + per-side policy + inventory/risk controls
        │
        ├── Order Gateway / Order Manager ── REST lifecycle + WS acknowledgements
        └── Inventory Manager ── position sync + exposure state
```

公开模板关闭 ML、multi-market、external venues 与 depth shadow；这些组件只有在对应配置显式开启且模型/数据齐备时才参与运行。历史 RL 链已删除。

2026-07-17 runtime/model 清理、保留边界与 causal calendar-only 重训结果见 [`research/families/f03_causal_13_head/docs/model_runtime_cleanup_retrain_20260717.md`](research/families/f03_causal_13_head/docs/model_runtime_cleanup_retrain_20260717.md)。

### 8.2 核心组件

| 组件 | 文件 | 关键特性 |
|------|------|---------|
| **MakerEngine** | `strategy/maker_engine.py` | AS+ML报价, microprice+动态κ, κ_eff, toxicity gate, 可选 ret_skew, 配置化 replace throttle, 动态RQ, 4层动态spread/γ(L0流动性δ + L1 vol-regime δ + L2 floor + L3库存γ), 可选 position-timeout `TIMEOUT_CLOSING` 升级(默认禁用), book_imb spread偏移 |
| **SignalEngine** | `strategy/signal.py` | 实时特征计算(工程输出约100+列), metadata-driven LightGBM heads, metrics REST轮询, REST aggTrades预热, `_prefill_10s_features()` 启动时10s特征历史预填充, bar完成回调(动态RQ) |
| **OrderManager** | `strategy/order_manager.py` | 订单状态机(OPEN/CLOSE/FLIP分类), WS事件驱动, 孤儿订单自动认领 |
| **InventoryManager** | `strategy/inventory_manager.py` | 持仓状态机, 交易所sync, `TIMEOUT_CLOSING` 状态sync屏蔽 |
| **风控** | MakerEngine 内 | 配置化日亏损/持仓上限、fill 后超限方向撤单、order-size cap、anti-flip cap 与 circuit breaker；具体阈值以所用配置为准 |

### 8.3 事件驱动流程

```
1. 收到 aggTrade → 更新 1s bar → 每 10s 触发特征计算 → ML 推理
2. 检查配置化报撤与订单生命周期条件:
   a. 动态 RQ / cooldown / stale guard 未满足 → 跳过
   b. 价格变化、最小 tick、replace threshold 或 pending/coalesce 条件不满足 → 保留当前订单
   c. 否则按订单状态撤换或提交新限价单
3. 收到 userData (成交回报):
   a. 更新库存
   b. fill 后即时检查 max_inv, cancel 超限方向挂单
   c. 按新库存重算报价
```

### 8.4 可选持仓退出升级机制

当前默认主线 `position_timeout=0`，因此该机制默认关闭；策略主线只做 maker 被动限价，不会因为方向或趋势判断主动吃单。下面是历史保留的可选退出升级逻辑：只有显式设置 `position_timeout > 0` 并进入 `TIMEOUT_CLOSING` 状态时才可能生效。

进入 `TIMEOUT_CLOSING` 状态，按持续时间逐级升级：

| 时间段 | 策略 | 手续费 |
|--------|------|--------|
| 0-30s | GTX at edge of spread | Maker (被动) |
| 30-60s | GTX + 1 tick into spread | Maker (更积极) |
| 60s+ | `position_timeout > 0` 时的 IOC 兜底 | Taker (当前默认禁用) |

## 九、参数配置

### 9.1 公开模板默认配置

本节只描述仓库内 `live/config.yaml`，它是可运行的安全研究模板，不是 EC2 私有 live baseline。私有 baseline 会随部署滚动，其参数、模型 bundle 和 source 组合必须从私有配置哈希与运行记录确认，不能由本表推断。

| 参数 | 公开模板值 | 说明 |
|------|-----------:|------|
| `symbol` | `BTCUSDC` | 唯一维护的 execution symbol |
| `strategy.gamma` | 0.01 | 模板风险厌恶系数 |
| `strategy.kappa_ratio` | 1.0 | κ 缩放 |
| `strategy.depth_kappa_ratio` | 0.3 | depth κ ratio |
| `strategy.vol_power` | 1.5 | volatility scaling |
| `strategy.order_size` | 0.001 BTC | 单边订单量 |
| `strategy.max_inventory` | 0.01 BTC | 模板持仓上限 |
| `strategy.max_spread_bps` | 12 | 模板 spread cap |
| `strategy.fill_cooldown` | 0s | 默认不启用 fill cooldown |
| `strategy.use_bar_pricing` | false | 使用盘口定价路径 |
| `strategy.adverse_guard_enabled` | false | 模板关闭 adverse guard |
| `strategy.position_timeout` | 0 | 模板关闭 timeout taker exit |
| `ml.enabled` | false | 模板不加载模型 |
| `ml.vol_blend` | 0.5 | 仅在 ML 开启时使用 |
| `ml.asym_strength` | 0.0 | 默认不施加 ML asymmetry |
| `ml.ret_skew` | 0.0 | 默认不施加 ret shift |
| `ml.model_dir` | `models/example_model_bundle` | 示例路径，不代表生产 bundle |
| `multi_market.enabled` | false | Binance reference/spot anchor 默认关闭 |
| `multi_market.market_stage` | `minimal` | 开启 multi-market 后的模板 stage |
| `external_venues.enabled` | false | Bitget/Bybit/OKX 默认关闭 |
| `depth_execution.shadow_enabled` | false | depth shadow 默认关闭 |
| `websocket.depth_levels/speed` | 20 / 100ms | Binance partial depth 订阅 |
| `fees.maker/taker` | 0 / 0.00036 | 模板手续费假设 |

`maker_fill_prob` 已迁为 tick replay 默认值/CLI 参数；旧 bar 回测的 `direction_aware_fill` 与 `fill_directional_strength` 也只保留在该历史入口，三者均不再属于 live `StrategyConfig` 或公开模板参数。

历史 private-live 参数、旧 effective κ、adverse threshold grid 和模型 bundle 排名不再列入当前文档，也不能由旧生成物恢复成 current evidence。

### 9.2 参数身份边界

BTCUSDT execution 的历史参数榜、模型目录和 sweep 排名已经删除；BTCUSDT 只作为 BTCUSDC reference/source。参数是否生效由当前配置 schema、启动 preflight、runtime telemetry 与代码测试共同确认，本文不再维护容易过期的静态“已使用/未使用”计数表。

---

## 十、实施状态

2026-06-20 前的 Step 1–24、修复明细、旧 sweep 表和公式复盘已统一收录到前文历史摘要，不再逐项展开。当前代码已经具备数据管线、训练/回测、事件驱动执行、风险控制与审计能力；后续实施状态以 2026-06-20 之后各专题章节及实际代码、配置和验证产物为准。

---

## 十一、当前 BTCUSDC 风险与观察项

> **口径边界**: 本章只记录当前 BTCUSDC execution 分支。BTCUSDT 的 liquidity、fee、model bundle 和参数结论不能直接写成本分支事实；BTCUSDT 只保留为 BTCUSDC reference/source 市场，不再维护独立 execution 仓或跨仓同步清单。

### 11.1 已更正的事实边界

| 项目 | BTCUSDC 当前口径 |
|------|------------------|
| Execution | BTCUSDC 永续 |
| Reference capability | BTCUSDT perp/spot；公开模板 `multi_market.enabled=false`，私有 live 是否启用以运行配置为准 |
| Maker fee | 0.0，当前 BTCUSDC maker promotion |
| 模型状态 | 公开模板 `ml.enabled=false`；私有 live bundle 由运行配置指定 |
| 不可直接迁移 | BTCUSDT config、`models/saved/` 模型、BTCUSDT tick A/B 阈值 |

### 11.2 当前 BTCUSDC 仍需观察的风险

1. **回测-运行时 fill 偏差**: 继续用 paper/forward-test 或同口径 trace 的 fill markout、同侧连续 fill、fills/day 与库存分布验证 exact-level L2 parity。
2. **逆向选择**: adverse/defense guard 已启用；toxicity gate 与 quote EV gate 默认关闭，需用 BTCUSDC quote trace/OOS tick replay 重新验证。
3. **库存纠偏**: `inventory_asym_strength`、`inventory_signal_fade_strength` 仍需独立 A/B，不能只凭历史 clean 候选叠加。
4. **Cross-market stale 风险**: BTCUSDT reference 是当前 BTCUSDC 主线的一部分，必须监控 reference stream stale、anchor guard 与 source-missing days 排除。
5. **ML 贡献边界**: `ret_skew=0.0` 是当前 retest 结果；后续若重启 ret channel，要重新做完整 OOS 和运行时 parity 验证。

### 11.3 BTCUSDT 参考边界

BTCUSDT 在当前项目中只允许承担 reference/source 角色：

- `reference_symbol=BTCUSDT` 的 cross-market features、basis/lead-lag、anchor freshness、source-missing day 排除。
- 作为 BTCUSDC 研究里的数据质量依赖和 reference stale 风险来源。
- 历史 BTCUSDT 参数表只作归档解释，不再作为当前维护任务。

不得再做的事情：

- 不再要求把共享逻辑同步到 `${NARROWGATE_ROOT_BTCUSDT_ARCHIVED}`。
- 不再维护 BTCUSDT 的 `live/config.yaml`、模型 bundle、paper/live 参数、PnL/fill/spread/cap-hit 结论。
- 不把 BTCUSDT execution 的旧结果当作 BTCUSDC 的参数证据。

### 11.4 后续验证优先级

| 优先级 | 验证项 | 目标 |
|--------|--------|------|
| P0 | `experiment_runner.py describe` / experiment manifest | 固化训练窗口、数据质量排除和生成物输出目录 |
| P0 | BTCUSDC quote decomposition trace | 生成 `raw_half_spread/raw_mid_shift/final_quote_delta` 与 fill markout 标签 |
| P0 | BTCUSDC quote EV split target | 用 BTCUSDC 数据训练 `P(fill)`、1s/5s/30s markout buckets、extreme adverse，不要复用 BTCUSDT 模型 |
| P1 | adverse/defense guard 运行时 parity | 固定连续参数，只切一个 boolean，跨至少两个窗口验证 |
| P1 | cross-market stale/anchor audit | 确认 reference stream 与 BTCUSDC execution 对齐，排除 source-missing days |

### 11.4b 旧验证体系归档（2026-07-06 cleanup）

原 `11.4b` 到 `11.4g` 里保留过大量 2026-05/06 的 adaptive TTL、spread cap、noise guard、bid-adverse、xmarket retreat、local-flow quote EV、session bucket、Stage T clue、SELL reversion、toxic-risk 等运行表格。那些表格现在已经从正文删除。它们仍然有研究史价值，但不能继续作为当前 baseline、live 参数或 promotion 证据。

作废或降级的内容包括：

- 单条连续跨日 / 月度 replay 得出的 fills/day、PnL、winner arm；
- 受 markout EMA latch、坏日/gap、错误分母、旧模型目录或旧 trace 影响的 A/B；
- `bid_adverse_*`、direct quote EV live、direct xmarket widen/TTL/retreat、SELL resiliency direct live 等旧 direct policy；
- 没有 Python/C++/live parity 的 fast sweep 结果；
- 只靠少成交、降库存时间或单日正 markout 支撑的 bucket。

保留下来的方法论结论是：旧机制可以重新提出为 hypothesis，但不能当 alpha 起点。任何候选都必须回到 rolling live baseline、causal data manifest、Python replay 权威路径、chronological validation、family-specific sealed holdout、campaign outcome 与 live mechanism distance 重新验证。

### 11.4c 当前验证流水线

当前策略研究只接受下面这条顺序：

1. **Data quality**：只用 retained good days；rolling feature 和 label horizon 不能跨坏日或长 gap。
2. **Live/replay mechanism parity**：先对齐 placed/day、fills/day、BUY/SELL split、spread、action mix、pause/block reason、VWAP、inventory path；baseline 必须滚动等于当前 live config。
3. **Order-level denominator**：每一笔 placed order 都要有 quote-time state、是否成交、fill age、markout、campaign label 和 score；不再只看 filled rows。
4. **Campaign-level label**：flat -> nonzero -> flat 的 terminal PnL、duration、max inventory、MAE、repair flag、tail/open-risk 是主要风险监督目标。
5. **Chronological evidence**：所有结论按 UTC day 输出，并使用 development、embargo、validation 与 family-specific sealed holdout；不再用月度路径或 pooled 平均直接选参。
6. **Constraint-first scoring**：先过 mechanism gate、tail、inventory-time、campaign MAE、side markout、raw/InvAdj 同看；再谈 median daily raw 或 candidate ranking。
7. **Shadow before live**：quote EV、xmarket、local-flow、calendar/session、campaign controls、cooldown/lifecycle 都必须先是 shadow report 或 replay shadow arm。

C++ 当前用于已 parity 的基础路径、quote-core/batch、fast screening 和系统侧低延时验证；涉及 replace throttle、pending coalesce、reducing cooldown、campaign controls、xmarket retreat、score-based lifecycle 的正式机制研究仍以 Python replay 为准，直到 C++ parity 补齐。

### 11.4d 当前 alpha / training 方向

当前不再从旧 arm 出发找“最优参数”，而是从 fill-level / order-level evidence 重新建训练目标：

- **Null baseline**：先比较 current strategy fills、random passive baseline 和 oracle-positive subset，确认当前策略是改善还是放大 toxic selection。
- **Side-specific fill-selection score**：BUY 和 SELL 必须分开定义“好成交”；`P(fill)` 只代表容易成交，不代表成交质量。
- **Campaign-outcome risk score**：用 quote-time 可见状态预测 terminal campaign loss、repair probability、early drawdown、MAE 和 open-risk。
- **Add-on campaign-tail score**：统一 order-level schema 明确区分 `inventory_role=opener/add/reducing`；只围绕 submit 与 fill 时都仍为 `add` 的 exposure-increasing fills，按 BUY-long / SELL-short 分侧训练 closed `loss_tail` 风险。fill-time role 只用于标签审计，不进入特征。
- **Local liquidity mechanism**：response kernel、OU half-life、depth refill/cancel、taker-flow decay 只作为是否可吸收冲击的机制证据；half-life 必须短于 fill age / TTL / holding budget。
- **BTCUSDT/spot reference**：从 direct gate 降级为 moderator / re-center / post-fill campaign risk。当前要测的是 pending reference residual、fill-time pending 和 risk calibration，不是旧 `multi_market=true/false`。
- **Policy knobs 收束**：任何有效条件最终只能映射到 spread、skew、lifecycle 三类小动作；不允许一次性叠复杂机制。

当前保留的是 data quality、显式 state contract 的 daily/continuous replay、hard gate、unified audit runner、order-level denominator 与 side-specific action-evaluation 基础设施；旧 campaign/score 方向、run table、winner 和 raw/InvAdj 数字不再保留在正文。

旧 add-on campaign-tail、safe-rearm 与 pre-repair M0/M1 精确结果已删除。它们曾帮助暴露 action support、campaign attribution 和 external-feature 增量问题，但不能作为当前模型或 policy 结论。当前 side-specific action evidence 从 2026-07-18 causal-v4 randomized panels 开始。

### 11.4.5 下一代状态条件动作层（2026-07-14）

项目已停止把“再找一组固定全局参数”当作下一代 alpha 形态。gamma、effective kappa、cap、inventory limit、cooldown 和 replace threshold 继续作为 rolling baseline 与安全边界；新增 `strategy/state_conditioned_quote_policy.py`，按 `BUY/SELL × opener/add/reducing` 识别状态，v1 只允许在 exposure-increasing `add` 表面选择 `baseline / prevent_over_widen / widen_1tick / recenter_1tick`。size、reducing quote 与 inventory limit 均不可改变，每个 campaign 最多一次干预。

artifact 强制记录 randomized support、behavior overlap、uplift LCB、feature freshness 和 promotion status。任一输入缺失/过期、支持不足、LCB 非正或 state advantage 不足均回退 baseline。Python tick replay 完整重放 queue、latency、fill 与 campaign terminal；live shadow 共用同一 tick/post-only/cap 动作几何；C++ 在 native parity 前 fail-fast。当前没有 artifact 通过 chronological OPE 与 sealed evidence，`live/config.yaml` 仍为 `disabled`，不是一次 live baseline 变更。旧 2026-07-15 artifact 数值已删除；当前 action evidence 见 `research/families/f09_campaign_action_uplift/docs/side_specific_action_uplift_existing_split_20260718.md`、`research/families/f09_campaign_action_uplift/docs/buy_add_conditional_widen_causal_v4_v1_20260718.md`、`research/families/f09_campaign_action_uplift/docs/sell_add_repair_trend_skip_causal_v4_v1_20260718.md` 与 `research/families/f07_active_order_continuation/docs/queue_value_keep_cancel_v1_20260719.md`。修正后的 cancel/re-enter v3 使用当前 operational empirical-P3 baseline、EC2 receive-visibility 延迟与完整 re-entry lineage；Development 的 DR reward uplift 为 `+0.00443 USDC/decision`，95% 区间 `[-0.04567,+0.04516]`，25 日随机化完整回放 PnL 反而减少 `8.62 USDC`。因此 Development gate 失败，Validation 与 sealed holdout 均未读取，完整报告见 `research/families/f07_active_order_continuation/docs/queue_value_cancel_reenter_v3_development_20260720.md`。

2026-07-20 的 native-snapshot deep-250 复核随后确认，v3 的 top-20 queue fallback 不是可识别的 active-price queue：在 2026-06-05，严格 deep 输入将 fills 从 `1,595` 改为 `2,062`、campaigns 从 `669` 改为 `895`，median queue seed 从 `0.1162 BTC` 改为 `0`，路径分歧后只剩 2 个共同 decision ID。52 个旧 eligible entry 中，48 个价格位于 deep-250 范围，其中 29 个是可确认的零可见队列，而旧 fallback 给了正 queue seed。因此 v3 只保留“不晋级”的安全结论，其 queue state、阈值和 DR 数值不能解释为真实深层队列因果证据。该复核随后先验证 baseline discovery + sparse active-order-price tape，并要求 formal replay 零 fallback；详见 `research/families/f07_active_order_continuation/docs/deep_active_order_queue_probe_20260720.md`。

同日 sparse fixed-point 审计继续跑到预注册的 g3 stop rule：g0→g1、g1→g2、g2→g3 的相邻轨迹保留率分别为 `36.61%`、`79.40%`、`84.97%`；g3 仍新增 `2,799` 个订单身份，并有 `2,799` 个 missing seed 与 `14` 个 unusable seed，fills/campaigns 也由 `2,086/943` 变成 `2,072/937`。因此 watch-specific 两遍法被判定为 `diagnostic_only`，不再继续迭代，也不读取任何 queue action outcome。native snapshot/delta 已作为独立于策略轨迹的 exchange-time 状态流接入 Python authoritative replay scheduler；它重建完整价位图，并在订单激活时提供 exact/known-zero/outside-range seed，同时记录逐价 cancel/refill。sequence gap、snapshot reset 与同毫秒 trade/L2 歧义会作废对应订单路径；top-20 receive-time 状态仍只服务 quote feature。冻结的 54 日新 family 随后完成 state fit/calibration 和 17 日 Development：1,448 个 campaign 以 50/50 propensity 分配 K0 keep 与 K1 cancel-until-state-exit，所有 K1 均真实经过 cancel ACK，328 次状态退出后完成 linked re-entry submit。原生 activation support 为 `95.718%`，完整 outcome support 为 `89.848%`，未过预注册 gate；保留 censored rows 后，`K1-K0` 严格 reward bound 为 `[-10.0183,+10.0275] USDC/intervention`。全行 mixed-simulator ITT 虽为 `+0.00556`，日聚类区间仍跨零，BUY 方向为负，而且 17 日 paired PnL 仅增加 `1.4769 USDC`、fills 减少 328。因此该 family 在 Development 关闭，Validation 与 sealed holdout 未读取，live/config/ baseline 均未改变；详见 `research/families/f07_active_order_continuation/docs/native_exchange_book_replay_scheduler_20260720.md`。

### 11.5 当前研究口径与后续观察

#### 11.5.1 回放量纲与时间因果修复（2026-07-15）

只读审计确认了两项会使历史参数/ML 证据失真的确定问题：`sigma_sq` 是一秒绝对价格变化方差，但 circuit breaker 与 exit urgency 又乘了一次 mid；10 秒左标签 feature row 则在 replay 中提前 10 秒可见。现已统一 Python/C++ 的 `quote_horizon_s`、风险金额公式与 `markout_horizon_s`，live markout 改为 wall-clock 到点结算；feature replay 使用 `bucket_end=index+10s`，日度特征加入遇缺日即停止的 7 日因果 warmup，partial-fill markout/EV 改为数量加权，外部 latency freshness 也计入注入延迟。

旧 model metadata、causal-v2 bundle、ML/multi-market A/B 与事故日 live/replay 数值已经从当前说明删除。formal replay 现在对 timing contract、feature schema、P3、queue 和 latency identity fail-fast；不能手工补 metadata。Python/C++ 共用 trade/BBO/L2/timer merged event clock，旧 trade-only clock 只允许显式 diagnostic。完整修复边界见 `research/system_engineering/docs/replay_time_unit_causality_repair_20260715.md`。

2026-07-17 随后完成了不依赖历史实盘成交的代码层 parity 审计。除补齐 `circuit_breaker_sigma/pnl_volatility_horizon_s` 的 live→Python/C++ 映射外，还发现 live `_build_side_policy()` 与 replay `_replay_side_policy_mult()` 已发生实质漂移：replay 缺 stale-warning、burst 与 size 语义，并会把部分 exposure-only guard 误作整侧 pause。共同 guard 现已合并为 `strategy.policy_guards.evaluate_common_side_policy()`，BUY scorer 也直接消费该共享结果；对应 checkpoint 的全仓测试和四个 real-data golden 均通过。目标机 strict native extension 重编译和 ABI preflight 通过后，运行配置只新增显式 quote `1s`、markout `10s`、risk `300s` 与 EMA span `50 fills` 四个字段，六个外部 venue、`USDCUSDT` anchor、tape 与策略参数均逐 leaf 保留，并完成受控 restart。完整边界见 `research/families/f10_live_replay_attribution/docs/live_replay_code_parity_20260717.md`。

同日补齐了 BUY fill-selection 的 C++ replay 合约。Python adapter 按 causal feature-ready row 将 `Prediction.feature_dict` 中不会被当前报价状态覆盖的字段，预编译为每个 fold 的 logit delta、missing 与 used 计数；C++ ABI v3 再合并 quote-time distance、depth、toxicity、inventory、shared side-policy allow flags，执行与 Python 一致的 shrink、fold average、missing gate 和 actionable hard gate。生产五 fold artifact 的 synthetic end-to-end score/hit 结果逐字段一致；含静态字段的真实 artifact 若走旧 ABI 会 fail-fast，不再允许静默缺特征。随后修复了两个完整窗口差异：Python 漏计 post-policy-only final cap compression，以及 C++ 仍调用旧 shallow-depth side-policy multiplier。C++ 现已对 BUY/SELL 无条件使用共享 common policy，并把 spread、size、hard pause 与 exposure 结果接入正式报价。May normal/high、Feb sparse、Jan A/B 四个 real-data golden 均严格通过 summary、PnL path、fills、inventory-time 与 trace-length parity，未放宽数值容差。

修复后的真实数据 parity 已在 `2026-07-03 00:00-00:10 UTC` 验证：Python/C++ 均为 2 fills、9,461 clock events、101 requotes，PnL 差 `2.2e-15 USDC`。完整日 public-template 诊断中 merged clock 相比 legacy trade clock 改变了 fills 与 PnL，因此旧回放数值不能继续沿用；该对照仅验证时间机制，不是 live 参数或 alpha 结论。

修复前与 causal-v2 的精确模型指标已经删除。当前 prediction evidence 从 causal-v4 empirical-P3 bundle 重新开始，并与 operational live model 明确分离。

| 项目 | 当前值 | 说明 |
|------|--------|------|
| Execution symbol | BTCUSDC | Binance USDⓈ-M BTCUSDC 永续 |
| Reference symbol | BTCUSDT | 代码支持 reference/source wiring；公开模板默认关闭，私有 live 以运行配置为准 |
| Model bundle | configuration-driven | 公开模板使用示例路径且关闭 ML；私有 live bundle 由运行配置指定 |
| Feature schema | metadata-driven | 以各模型 `*_meta.json` 的 `feature_cols` 为准 |
| P3 fill params | bundle 内 `fill_prob_params.json` | 事件引擎与 tick replay 共用 P3 参数 |
| Core params | private config snapshot | 以 `NARROWGATE_LIVE_CONFIG` 指向的私有运行配置为准 |
| Fill model params | private config snapshot | exact-level L2 parity 口径；公开文档不记录实盘参数值 |
| ML params | private config snapshot | BTCUSDC 模型目录；公开文档只记录 bundle 角色 |
| Toxicity gate | disabled | 不使用单一 toxicity threshold 直接决定 live action |
| Quote EV gate | disabled | Direct live quote-EV policy 已归档；quote EV 只做 offline/shadow evidence |
| BUY fill-selection soft-keep | enabled | 2026-07-06 起进入 rolling live baseline；`${NARROWGATE_MODEL_DIR}`，runtime threshold `0.44`，只 cap BUY soft-widen 回正常 spread，不加 size、不动 SELL、不绕过 hard gates；Python replay 与 C++ ABI v3 均已接入并通过四个 real-data golden，旧 ABI 或不完整 static payload 会 fail-fast |
| Flat unilateral TTL | enabled | `flat_unilateral_max_s=120`；防止 flat 时旧 markout EMA 把系统长期锁成单边 |

**后续观察**:
- 继续监控 BTCUSDT reference stale 与 cross-market anchor guard。
- 对比 paper/forward-test 或同口径 trace 的 fills/day、同侧连续 fill、库存分布与 BTCUSDC calibrated tick replay 的偏离。
- 累积足够 paper/forward-test 样本后，再决定是否引入 quote EV gate 或回退 no-ML/no-reference 口径。

---

## 2026-07-15 因果修复与当前 P3 边界

旧 ML、多行情以及仅由 execution trade 推进时钟的精确 PnL 数值，从本次 checkpoint 起统一降级为历史诊断，不能继续用于参数选择或 promotion。原因不是结论“不好看”，而是旧特征行在 bucket 开始时即被 replay 使用，包含了尚未完成的 10 秒桶；同时 BBO/L2、TTL 和 timer 只随下一笔成交推进，破坏了 live/replay 因果一致性。

本轮正式口径已完成：bucket-end `feature_ready_ts`、7 日因果 warmup、trade+BBO/L2+timer 合并事件时钟、价格方差单位修复、数量加权 markout、期末 MTM、strict P3/queue/REST-latency identity。10 秒 P3 主校准为 `delta*=13.9991 USDC/BTC`、`effective_kappa=0.067438`；5 秒敏感性结果为 `delta*=10.9991`、`effective_kappa=0.083114`。这说明 effective kappa 是带 horizon 和 fill definition 的校准产物，不是应在 YAML 中长期固定的普适常数。

AWS Tokyo 2-vCPU/4-GiB、Amazon Linux 2023 的 2026-07-10..14 REST profile 为：new p50/p95/p99/p99.9 `7.156/25.044/81.987/165.539ms`，cancel `6.938/27.692/90.019/177.090ms`。该 profile 只属于这组机器、区域、网络和 gateway 身份，更换环境必须重做。

queue artifact 升级为 v3，正式报告会记录 path、SHA256、fit days、schema、replay parameters 和参数来源。固定 queue scale 在 fit 与后续日期之间不稳定，旧默认也不能事后回选；当前缺口更像状态化 cancellation-ahead/book refresh，而不是 C++ 浮点漂移。该代 causal-v2 的精确 ML PnL 已删除，ML ON 仅作 research arm，不改 live。

当前 evidence boundary 见 [`research/families/f10_live_replay_attribution/docs/historical_backtest_evidence_revalidation_20260720.md`](research/families/f10_live_replay_attribution/docs/historical_backtest_evidence_revalidation_20260720.md)；正式模型与 replay 结果从 causal-v4 identity 重新开始。

### 2026-07-16 live/replay 时间轴复核

07-14 对比发现，Binance USD-M execution stream watchdog 会在自然的 15 秒无成交窗口后，把 `@aggTrade` 切换到 eventless raw `@trade`。这使 live 的 trade-derived volatility、ML/flow state 长期陈旧，dynamic requote 退回 10 秒；因此 07-12..14 不能作为健康 live 对固定 queue scale 的干净验证面板。

同时 replay 的历史 1 秒 bar 只含有成交秒，空白秒被压缩，dynamic requote、BER 和 volatility EMA 也没有运行在与 live 相同的墙钟上。当前 replay 会因果补齐 flat seconds，并将 window cache 升级为 v6；live 则只保留 USD-M `@aggTrade`，silence 只允许重连同一合法 stream。

事故复现口径可把 07-14 decisions/fills/分侧 fills 和 pair spread 收敛到约 5% 量级，但 exact path PnL 仍未对齐，因此该复现臂只用于诊断，不进入 baseline。connector-only 修复已在 bounded capture 结束后部署；配置哈希不变，重启后订阅恢复为 `btcusdc@aggTrade` / `btcusdt@aggTrade`，execution trade freshness 和约 5 秒 dynamic requote 恢复正常。下一次正式 parity 必须使用 feed 修复后新增的完整 UTC good day。

2026-07-13 的旧重建只预热一小时，漏掉了 2026-07-12 22:26 UTC 的原生 snapshot。使用 24 小时 warmup 后，跨午夜 `U/u/pu` 链严格连续，BTCUSDC top-20/100ms fresh coverage 从 70.98% 修复到 98.54%，该日重新进入 minimal local-replay universe。源端 06:36:54--06:58:00 仍无事件，所以它不是 gap-free event-L2 evidence；所有修复前同日 C++/Python、queue scale 与 PnL 结果继续失效，必须按新的 BBO/L2 hash 重跑。

### 2026-07-20 historical baseline 冻结

07-18 曾使用旧 P3 override 与 stale-close 事故语义做一次性历史复刻。该臂无法唯一恢复真实 queue priority、hidden liquidity、cancel-ahead 与 ACK/fill race，精确结果已删除；临时兼容开关和 arm 也已从源码移除。

正式 replay 另行把“全日累计 GTX reject”与“本轮 closing 连续 reject streak”拆开，并在 Python/C++ 中于新 closing、成功 GTX 与回到 flat 时重置 streak。修正后 current baseline 为 fills `139/135`（`+3.0%`）、breaker `11/11`、decision `15428/15580`（`-1.0%`）；同侧一秒内逐笔匹配 `84/135`，匹配项时间差中位数 `28.5ms`、价格差中位数 `6 ticks`。这通过机制级 baseline gate，但不构成 exact historical PnL parity。

从此所有策略实验只允许与同次运行的 current corrected baseline 做 paired 比较，并共享 code/config/P3/queue/data/initial-state/latency hashes 与随机种子。旧 live 配置、旧 effective kappa、旧 GTX/IOC 事故臂不得再作为参数选择、action uplift 或 promotion control。完整审计见 MarketData `reports/live_replay_20260718/live_replay_alignment_20260718.md`。

### 2026-07-20 historical empirical P3 operational rollout

EC2 rolling baseline 已移除历史 `p3_kappa_eff_override=0.055`，现在由部署 bundle 中的 empirical-survival P3 artifact 提供 `delta*=13.9990859817` 与 `effective_kappa=0.0674381136`。运行配置 SHA 为 `1ba03a6d...`，P3 artifact SHA 为 `f051ed23...`；重启后的 live quote trace 已确认实际 `kappa_used=0.067438`。当前 live 主模型仍是 `saved_btcusdc_causal_v3_calonly_20260717`，不能把 causal-v4 研究 bundle 或 rebuilt BUY scorer 误写成已部署。

部署入口新增 P3 preflight：模型目录和 `fill_prob_params.json` 必须存在，默认从 artifact 取值；非零 override 只有在显式设置 `NARROWGATE_ALLOW_P3_OVERRIDE_DEPLOY=1` 时才允许作为独立 trial。两份 current private config snapshot 也已收敛到同一 SHA。

旧回测完成分级复核：量纲/因果修复、empirical P3、C++ same-input parity、causal-v4 research-only/negative action-family 结论继续有效；旧参数 winner、arm 排名、scorer 数值和修复前精确 PnL 已从公开结论中删除。完整边界见 `research/families/f10_live_replay_attribution/docs/historical_backtest_evidence_revalidation_20260720.md`，该次历史机器可读 baseline 身份见 `research/families/f10_live_replay_attribution/docs/operational_baseline_identity_20260720.json`。后续 runtime 身份不得从这一段推断。

### 2026-07-22 formal replay contract v1

该版历史 contract 要求策略证据使用日度 `fresh_start`，或显式哈希的 `narrowgate_standard_initial_state.v1`；历史 live active orders、cooldown、markout 和 campaign 状态只允许进入 `live_alignment`。2026-08-02 起新增的 versioned continuous contract 可跨午夜传递现金、库存和 campaign，但必须单独冻结 calendar/restart identity，不能回写或重解释本 v1 历史实验。contract 同时冻结 config/model/P3/queue artifact、merged causal clock、历史 book 可见性、终局 MTM 口径和全部 latency identity。

REST new/cancel 延迟已由顺序 PRNG 改为 Python/C++ 共用的 `keyed_splitmix64_v1`：以 seed、事件时刻、原订单时刻、side 和 operation 为键，arm 分叉后不会错位消费随机数。稳定 baseline 使用带 AWS Tokyo 2vCPU/4G Amazon Linux 标签的固定分布；偶发 spike 仅在 deterministic stress scenario 中注入，stress artifact 不具备 promotion 资格。live alignment 从此只负责发现单位、时钟、状态机和 gate 顺序错误，不要求逐 fill、campaign 或 PnL 相等。完整规范见 `research/system_engineering/docs/formal_replay_contract_20260722.md`。

### 2026-07-22 side-specific state-conditioned rearm v1

已实现 Python 权威 replay 的 BUY/SELL 独立 action family：每个 campaign 只在当前 85 秒加仓 cooldown 真正结束后随机一次，`baseline_rearm` 与 `continue_block_until_recovery` 各 50%。候选只在 adverse move、持续 adverse flow、弱 refill 与弱 price/microprice recovery 同时成立时继续阻断 add quote，并按冻结 hysteresis 多周期延续；reducing quote、size、inventory limit 与外部 reference 均不变。C++ 在补齐该多周期状态机前 fail-fast，且没有任何 live wiring。

56 日 Development 结果关闭了两侧 v1。SELL active baseline/effective candidate 支持为 `42/39`，DR reward `-0.00504 USDC/decision`，95% 日聚类区间 `[-0.01314,+0.00226]`；BUY 支持为 `35/47`，reward 点估计 `+0.00493`，但区间 `[-0.00166,+0.01275]`，30 分钟 repair 与 duration 均恶化。两侧都未达到冻结的每 arm 50 行 support gate，日度正向率仅 28%/32%，5-USDC tail 事件为零而无法提供尾部证据。因此 Validation 与 sealed holdout 保持未读，current 85 秒 baseline 不变；这不证明固定 85 秒最优，只否定该冻结四条件 extension。完整证据见 `research/families/f09_campaign_action_uplift/docs/state_conditioned_rearm_after85_v1_20260722.md`。

### 2026-07-22 dynamic mechanism campaign audit v1

当前 EC2 operational baseline 的动态机制已按 campaign 做传导审计，身份冻结为 config `1ba03a6d...`、empirical P3 `f051ed23...`、queue-v3 q0.70 `77568817...` 与 AWS Tokyo 2vCPU/4GiB latency tape `2c025fc7...`。48 小时 live 诊断包含 508 个 closed campaigns：408 个无 add campaign 合计 `+0.3122` USDC，100 个至少一次 add 的 campaign 合计 `-9.3668`，最差 20 个合计 `-8.7043`。Development56 的 8,496 个无 add campaign 合计 `+11.0390`，1,878 个 add campaign 合计 `-178.0990`。

动态配置并非全部有效控制器。regime spread scale 确实传导到报价，P3 floor 在 Development add campaign 中约 `23.0%` 决策命中；但 `kappa_used / p3_kappa` 均值约 `1.00016`，depth-kappa 在普通状态几乎不动。dynamic cap 虽随波动变化，Development cap-hit/final-compress 仅约 `0.24%`，add campaign 约 `0.08%`，当前主要是 operational ceiling。fill cooldown 仍是 `85/170/255/...` 阶梯，不是连续状态控制器。

首次 add toxicity 比 reducing-side pause 显示出更大的作用空间。Development add campaign 的 first-add 30s maker-signed markout 均值约 `-1.01 bps`；按冻结诊断阈值，immediate-only、repair-only、mixed 三组分别贡献 `-104.6215`、`-23.8250`、`-40.7765` USDC。reducing defense pause 的支持探针则只有 LONG/SELL 侧 14 个、SHORT/BUY 侧 10 个 individual-trade strict-through interval；两侧都未达到预注册的每侧 20 个门槛，LONG 侧受影响负 add campaign 比例也低于 5%。因此 `reducing_repair_release_v1` 在 support stage 正式关闭，没有生成 action panel，也没有修改 live defense。

下一 family 已在读取 action outcome 前冻结为 `first_add_marginal_order_value_v1`：每个 campaign 只在第一个 baseline-eligible empty-order exposure-increasing add opportunity 做一次 50/50 `baseline_add_cycle` / `skip_one_add_cycle` 随机干预，BUY/SELL 分开验收；reducing quote、size、inventory limit、taker 与 external reference 均保持不变。观察到的 first-add loss 只构成实验动机，不构成 skip action uplift。完整证据见 `research/families/f10_live_replay_attribution/docs/dynamic_mechanism_campaign_audit_20260722.md`。

### 2026-07-22 SELL campaign add permission v1

在 one-cycle skip 作用过弱后，项目冻结了更强的 SELL-only campaign action：每个 short campaign 在第一个 baseline-eligible SELL add opportunity 以 50/50 propensity 分配 baseline 或 `stop_add_until_flat`，后者持续阻止所有 exposure-increasing SELL quote 直到回平。BUY、reducing、size、max inventory、taker 与 external reference 均不变。Development 行为面板有 1,734 个独立 campaigns、56 天、883/851 两臂，baseline 后续 SELL add fills 为 986，支持度与 overlap 均通过。

30-train/1-embargo/7-test 的 chronological DR + depth-2 honest-tree OOF 结果关闭了该 family。reward uplift 为 `+0.00815 USDC/campaign-decision`，但 UTC-day 95% 区间为 `[-0.02160,+0.03711]`；campaign-cost avoidance 下界为负，repair 与 day-end censoring 点估计略负。更关键的是，策略在 85.7% OOF rows 选择 stop-add，估计 SELL add-fill retention 仅 10.8%，远低于冻结的 85% 门槛。负终局、q10、MAE 与 repair-time 的改善因此属于大幅减少参与所换来的风险收缩，不是稳定 action alpha。

Validation 与 sealed holdout 均未读取，live/current baseline 不变。不得在已读取的 Development 上调 tree threshold、限制 candidate rate 后复活该 family。one-cycle skip 过弱、stop-until-flat 过强、post-85 extension 支持不足，下一 family 必须使用新的内生 recovery/rearm action identity，并重新预注册 randomized replay。完整证据见 `research/families/f09_campaign_action_uplift/docs/sell_campaign_add_permission_v1_20260722.md`。

### 2026-07-22 recovery-event rearm v1

已把下一代 rearm 条件冻结为四条 causal path：shock decay、refill、microprice recovery 与 queue recovery，使用等权几何均值。阈值选择严格 support-only，不读取 reward、PnL、markout、campaign cost、MAE 或 duration。SELL 阈值 `0.01600795` 对应 candidate rate `11.00%`、保守 fills retention `87.04%`；BUY 阈值 `0.02147895` 对应 `15.03%` 与 `87.49%`，两侧均通过 5%-30% candidate budget 和 85% retention floor。

SELL 作为预声明首侧进入 56 日 Development formal replay。实际 candidate rate `10.95%`、保守 fills retention `87.19%`、active baseline/candidate 支持 `97/83`，overlap 与 ESS 均通过；但 DR reward 为 `-0.01560 USDC/campaign`，UTC-day 95% 区间 `[-0.02996,-0.00377]`。campaign-cost avoidance、negative-terminal protection 与 MAE avoidance 的完整区间也均在零以下，repair 与 duration 点估计为负。统一 `action_defense_v1` scorecard 因此将 ranking score 置空并在 Development 关闭 `sell_recovery_event_rearm_v1`。Validation/sealed holdout 未读，BUY outcome 仍为 support-only，live/current baseline 不变。完整证据见 `research/families/f09_campaign_action_uplift/docs/recovery_event_rearm_v1_20260722.md`。

### 2026-07-22 queue-value competing-risk keep/cancel v2

项目已停止围绕 v1 adverse-state threshold 微调，改为对活动加仓订单直接拟合 BUY/SELL 分侧的 favorable fill、adverse fill、cancel、adverse price jump、campaign repair 与 queue recovery cause-specific hazard，并用 `V_keep - V_cancel/reenter` 决定是否进入 K0/K1 50/50 随机化。校准 candidate rate 约 15%，17 日 Development 实际覆盖 1,101 个独立 campaigns，eligible rate `17.10%`。

该动作没有形成选择性毒流过滤。K1 干预订单 fills retention 仅 `7.67%`，toxic fills retention 为 `8.29%`；toxic reduction leverage `0.9933`、reduction surplus `-0.0061`、toxic-share log ratio `-0.0771`，均表示毒流下降没有快于总成交下降。5,000 次 UTC-day bootstrap 的 pooled randomized ITT reward 为 `-0.01448 USDC/intervention`，95% 区间 `[-0.02577,-0.00239]`；BUY 明确有害，SELL 未证正。native seed/outcome support 为 `96.09%/89.92%`，也未过 98% gate。

因此统一 scorecard 将 ranking score 置空，Validation 与 sealed holdout 保持未读，live/current baseline 不变。今后 selective execution 使用 `action_execution_selective_v2`：允许成交量大幅下降，但 conditional value、toxic-reduction surplus 与 toxic-share log ratio 的日聚类下界必须同时为正。完整证据见 `research/families/f07_active_order_continuation/docs/queue_value_net_hazard_keep_cancel_v2_20260722.md`。

### 2026-07-23 side-specific taker flow 与双时钟 lifecycle v2

项目从此冻结 `BUY maker <- aggressive SELL taker`、`SELL maker <- aggressive BUY taker` 的分侧映射。最初的 100ms exchange-time 诊断曾在小面板观察到 BUY fill-quality 非对称，但未在更宽 denominator 复现；`side_taker_hazard_m0_v1` 又混入旧 jump、policy cancel 和 delayed-entry repair，因此只保留为 Development 阴性诊断，不创建 action，也不修改 live。

后续 v2 使用双时钟合同：Binance Vision individual trades 仅作为撮合、queue 消耗和 outcome truth，策略可见 taker-flow 只能在 mapped public `aggTrade` 的 feature-ready clock 后更新。recorder/source-contract 审计已通过；历史 parent 映射必须保留 `f/l` 与 source identity，internal-ID-gap block 不进入 strict queue outcome。该合同随后用于完整 start-stop lifecycle 和下节 dynamic-fill M0，因而当时的 recorder 阻塞状态只属于历史前置阶段，不代表当前研究状态。机制、关闭结论和双时钟细节分别见 `research/families/f08_side_taker_lifecycle/docs/side_specific_taker_flow_research_20260723.md`、`research/families/f08_side_taker_lifecycle/docs/side_taker_hazard_m0_v1_20260723.md` 与 `research/families/f08_side_taker_lifecycle/docs/binance_trade_lifecycle_contract_v2_20260723.md`。

### 2026-07-23 dynamic fill hazard M0 v2

项目已补齐完整 start-stop 订单生命周期，并把旧 static exponential competing risk 改成动态离散时间风险集：cancel request 作为 action/censor，native jump 作为非吸收状态转移，campaign repair 仅在 active reducing quote 可用后 delayed entry。BUY/SELL 分开，特征只使用 exact-safe L2/queue、price/microprice、refill/recovery 与 causal clock；不使用 historical quantity/notional 或 child count。

17 日 Development 有 3,057,751 个 formal fill-risk rows、291,695 张订单、11,660 次 adverse fill 和 4,072 次 favorable fill；repair 面板有 6,200 个 campaigns、6,152 次 repair。BUY/SELL adverse heads 均通过全部 gate，但两个 favorable heads 的 Brier skill 分别为 `-0.00565/-0.00131`，因此完整 side gate 均失败。`prediction_gate_passed_sides=[]`，Validation 与 sealed holdout 未读，`queue_value_keep_cancel_dynamic_fill_m0_v1` 没有创建 randomized action panel，DR/ESS/tail/selectivity 均不适用，live/current baseline 不变。完整证据见 `research/families/f07_active_order_continuation/docs/dynamic_fill_hazard_m0_v2_20260723.md`。

### 2026-07-25 normalized L2 100ms v2

BTCUSDC 新 replay、feature 与 lifecycle 入口已迁到唯一的 `normalized_l2_100ms_v2` top-20/100ms 身份；旧顶层 `l2/` 的 250 个独立混合频率文件已删除，剩余 6 个硬链接仅维持冻结 strict62 manifest 的历史绝对路径。原生 CryptoHFTData BTCUSDC snapshot/delta 仍是 execution deep queue 的权威源。

在保持 chronological split 与 aggTrades 不变、只替换 BBO 身份后，5s/10s P3 的 `delta*` 均未变化，`effective_kappa` 分别只变化 `+0.168%/-0.121%`。因此 P3 touch 结论已在新身份复现，但旧 artifact/hash 仍被 supersede；当前 live 未因本次数据研究自动换模。queue、fill、campaign、action-uplift 的旧精确数值不能由 P3 稳定性推导为有效，必须按 `docs/legacy_l2_evidence_revalidation_20260725.md` 的顺序重跑。

旧 128 日 fixed-spread v1 已降级为 matcher/lifecycle 事故诊断：它把 25 个距离放进独立策略世界，并把 strictly-through trade 当作 exact-price quantity consumption，因而制造了错误的 0→1 tick lifecycle 反转。旧概率、分段 effective-kappa 与 lookup 均撤回。

### 2026-07-27 BTCUSDT historical trade bridge

历史 BTCUSDT local bridge 已从第三方 CryptoHFT BBO 迁到官方 Binance individual trades 生成的版本化 1 秒 bar。每个 `[t,t+1s)` bar 只在 `t+1s` 可见，记录 `last_event_ts_ms`，并使用两秒 freshness 上限；live 仍使用 BTCUSDT WebSocket book ticker。133 日 bar 和 114 日 hierarchical reference 均带 source/output hash 并通过校验。活动代码不再读取 BTCUSDT CryptoHFT orderbook，3,192 个小时文件已删除，回收 56.565 GiB；BTCUSDC 的 3,528 个小时文件和 147 个完整目标日保持不变。

good-day 不再定义为所有历史 source 的隐式最小交集。当前 133 日旧交集移除 BTCUSDT CryptoHFT gate 后，BTCUSDC raw 的宽口径上限是 147 日；14 个新增目标中仅八日有完整 D-1 warmup，而 strict sequence 审计最终只有 `2026-05-16` 通过。版本化候选 `normalized_l2_100ms_v2_20260727` 为 133 日/66 formal；活动 canonical 仍冻结在 128 日/62 formal。把候选覆盖率从 99%降到 95%只增加五日，且这些日期存在 10-21 分钟最大连续缺口，因此 whole-day formal threshold 不下调。95%只允许用于显式 reset/censor 的 segment diagnostics。

替代实验 `paired_fixed_spread_monotonic_v2_20260726` 对每个 decision 一次性生成全部 25 个距离，共享 activation/cancel/TTL、latency 与行情路径，不让 counterfactual fill 反馈后续订单；exact trade 按数量消耗 queue，strict-through 强制 full fill，并对深价 fill/浅价 miss 做 pathwise fail-fast。128 日 descriptive 和 62 日 formal 均无 filled/full-filled/quantity/1s/5s/10s/lifecycle 单调性违例。formal 0→1 tick lifecycle fill 为 BUY `51.08%→48.66%`、SELL `50.18%→48.96%`。`fill | touch` 仍会上升，这是 deeper-through 条件样本选择，不能代替无条件成交概率。

80 tick 后 queue fallback 已约 79%，100 tick 约 93%，140 tick 后超过 99%；远端仍只是冻结 matching-model geometry，不能生成 live lookup。完整结果见 `research/families/f06_placement_fill_cif/docs/paired_fixed_spread_monotonic_v2_20260726.md`。

运行中发现 2026-07-04..11 的八个 local individual-trades 文件 maker-side 字段被错误写成全 true；已从 Binance Vision 原子替换，128/128 日均恢复双边支持并只重跑这八日。runner 现在在读取 outcome 前写入并校验独立的 `execution_trade_quality.csv`。旧 runner、数值报告和产物随后已物理删除；事故机制与当前有效结果统一保存在 `research/families/f06_placement_fill_cif/docs/paired_fixed_spread_monotonic_v2_20260726.md`。

下一阶段已经冻结 `research/families/f06_placement_fill_cif/docs/volatility_conditioned_fill_probability_design_20260726.md`。现有 P3 只估计 10 秒 touch/opportunity 并向 quote core 提供 `delta_star/kappa_eff`；dynamic fill hazard 是 100ms favorable/adverse active-order hazard；BUY fill-selection scorer 只做成交质量排序；causal-v5 13-head 不含订单成交概率。因此当前没有可直接回答 `P(fill before cancel/replace/TTL | distance, state)` 的完整模型。

现有 paired v2 只有 `side x distance x day` sufficient statistics，不能直接训练 state surface。下一阶段必须先生成逐 decision 的 native-deep lifecycle 宽表，并为 current/`-1 tick`/`+1 tick` 分别保存 action-specific activation/GTX、visible queue、first active touch type、exact-queue/through fill path、cancel request/ACK、partial fill、1s/5s/10s/lifecycle outcome 与 censoring；inventory role 必须来自正常 baseline trajectory，而不是 fixed-geometry 的 `initial_inventory=0`。

研究对象拆成两个 estimand：`placement_fill_surface_v1` 从提交前状态预测每个 action 自己的 activation 与后续 fill CIF；`active_order_continuation_surface_v1` 从已经活动的订单状态预测 KEEP。REPLACE/cancel-re-enter 会重置 queue，属于 lifecycle action，不能被当成简单 distance 变化或与 KEEP 强制共享单调约束。对于活动订单，主 target 直接使用 `CIF_exact_queue(h) + CIF_through(h)`；`P(fill | active touch)` 只作机制分解，不能称为纯 queue conversion。scheduled exposure 必须覆盖 cancel request 到 ACK，实际 label 终点为 `min(t0+h, cancel_ACK)`。

距离模型可使用 `distance / sqrt(sigma_sq_price_per_s * H_scheduled(action))` 作为无量纲诊断量，但保留 raw distance、vol regime、queue、microprice、flow、refill/recovery 和 lifecycle 状态。当前 BUY q90 cancel/re-entry policy 必须作为 frozen baseline treatment 明确进入 exit distribution。价值统一以 USDC 计量；maker-signed markout 已含 spread capture，queue 对 fill probability 的影响和 terminal campaign MTM 均不得重复扣除。只有新的 known-propensity action experiment 才能改变 live，本轮预测 surface 不直接上线。

2026-07-26 已冻结机器可读契约 `research/families/f06_placement_fill_cif/docs/paired_state_fill_surface_v1_spec_20260726.json`，并完成 Development 单日 native-deep mechanics smoke。1,000 个真实 baseline side-decisions 生成 3,000 个 `closer/current/farther` child，BUY/SELL 为 `551/449`，opener/reducing/add 为 `442/439/119`；三档 fills 为 `42/40/39`，路径单调性违例为 0。exact queue 与 through fill 已分开，BUY q90 被哈希并排除为 frozen separate treatment。该 smoke 仅证明逐 decision schema、activation/ACK race 与 matcher 可工作；单 action 只有 39--42 fills，且磁盘仍接近 60 GiB reserve，故不拟合 surface、不读取 Validation/holdout、不创建 action。完整记录见 `research/families/f06_placement_fill_cif/docs/paired_state_fill_surface_smoke_20260726.md`。

### 2026-07-25 causal-v5 evidence rebuild

在 normalized-100ms L2、修复后的 individual trades、empirical P3、q0.70 conditional queue、fresh-start 和固定 AWS Tokyo 延迟身份下，项目已重建 128 日 causal features、13-head model、42 日 formal order denominator、inventory lifecycle、BUY fill-selection、ML A/B、queue sensitivity、opportunity null 和 executable random-passive null。

新 13-head bundle 只保留 shadow：clean ML-ON 相对 ML-OFF 在 Validation20/Test17 的 raw 与 terminal 区间仍跨零，同时减少约 7%–9% fills、增加 inventory time。四个 BUY scorer 虽有 markout 排序，但没有一个同时改善 fill quality、campaign terminal 和 tail，action gate 全部关闭。修复后的 frozen BUY dynamic-fill Validation adverse/favorable/repair prediction heads 均通过；它只允许登记新的 randomized keep/cancel experiment，不允许直接改 action 或 live。

submit-time random placed opportunity 仍比 actual fill markout 高约 5 bps，但完整可执行 null 否定了“随机 maker 更好”的推论。Development33 的 random raw 点估计为 `+2.98`、区间跨零，InvAdj 为 `-2.40` `[-3.77,-1.06]`；Validation9 raw 为 `-8.85` `[-16.23,-0.60]`，InvAdj 为 `-0.576` `[-1.195,-0.019]`。32 个 seed 在 Validation 只有 1 个 raw 胜出，0 个 InvAdj 胜出。结论是 baseline 显著优于该 executable random family，但 baseline 自身仍为负，不能称为已验证盈利 alpha。

Python/C++ formal replay 已恢复代表日精确 parity：ML ON/OFF 的 2026-04-18、05-30、06-10，以及 executable-null 的 Development/Validation parity days 均为 fills 完全相等、PnL 误差不超过 `1e-9`。旧 48/512/1024-arm 与 retained39/blocked71/late4 排名继续正式撤回，不在旧 ID 下重跑。完整证据见 `research/families/f03_causal_13_head/docs/causal_v5_normalized100ms_revalidation_20260725.md` 和 `research/families/f03_causal_13_head/docs/causal_v5_revalidation_registry_20260725.md`。

### 2026-07-25 BUY exposure adverse q90 live action

按明确上线指令，项目新增独立哈希的 `buy_exposure_adverse_q90_cancel_reenter_v1`：仅 BUY opener/add 活动订单在 `P(adverse)-P(favorable) >= 0.005747997873332665` 时撤单；分数恢复到门槛以下后按原 baseline 重入。SELL、BUY reducing、size 与 inventory limit 不变。

现有 top-20/100ms 策略流保持不变，另用 1000 档 REST snapshot + 100ms diff-depth 维护活动订单价位的 queue/refill/recovery 路径。EC2 native 进程已完成两次真实 cancel ACK 与约 109/115ms 后的 baseline re-entry，健康计数为 2 cancels、2 reentries、197 keeps、零 retained 残留。该动作是显式 live trial，不改写此前 DR uplift/campaign-tail 尚未通过的研究结论。完整身份见 `research/families/f07_active_order_continuation/docs/buy_exposure_adverse_q90_live_policy_20260725.md`。

### 2026-07-25 causal-v5 operational baseline promotion

用户明确要求把 `models/saved_btcusdc_causal_v5_normalized100ms_20260725` 作为新 live baseline。该 bundle 来自 normalized-100ms L2、修复后的 2026-07-04..11 individual trades 与 empirical P3；13 个 head 均通过 `195/195` runtime feature-schema 加载检查。

这次上线是用户明确接受不确定性的 operational promotion，不改写研究结论：clean ML-ON 的 raw/terminal paired interval 仍跨零，fills 减少约 7%–9%，inventory time 增加。因此新的 baseline 身份必须标记为 `user_directed_trial`，后续 arm 一律与这一 bundle、其 P3 hash、当前 queue、固定 latency 和 fresh-start replay 成对比较；不能把“已上线”倒推成 strict alpha gate 已通过。旧 causal-v3 bundle 只保留为历史回滚身份。

### 2026-08-02 至 2026-08-09 causal-v12 operational baseline promotion

用户先明确接受低成本 live 验证，并在随后将 `causal_v12_expanded_source_aware_semantics_v6` 正式指定为当前 operational 和 backtest baseline。当前运行使用 Python 3.12.13、v6 Feature DAG 与未改变的 empirical P3；queue、latency、cooldown、size、inventory limit 和 hard safety gates 保持不变。最初晋级只修改身份；随后 v5 安全处置关闭 q90 action、保留 shadow。v6 根据当前栈 40 日成对诊断的负经济点估计，进一步暂停 BUY fill-selection action；v7 随后将 scorer/shadow 与 action 权限拆开，保持 action OFF、shadow ON；v8 再以 baseline-integrity successor 修复同一报价决策内的 depth/mid 原子性与执行时钟合同；v9 将没有 operational evidence 的 BUY fill-selection shadow 退役，保留 q90 shadow；v10 再停止已关闭 fair-center、inventory what-if 和 depth 候选的持续 shadow 写入，不改变报价经济路径。处置均通过可回滚变更和受控重启生效。

owner-amended v2 以 80%-120% fill band、closed-campaign primary outcome 和 loss/fill selectivity 重新登记 operational 判断：27 日中 19 日 PnL 改善，closed-campaign uplift 为 `+1.1050 USDC/day`，95% 区间 `[+0.4801,+1.7306]`，loss/fill ratio 为 `2.925`。但这些 panel 已经读过，且 campaign q10 区间仍跨零；因此 `ranking_score` 仍为 null，research prediction/live authority 仍为 false。当前 live identity 见 `research/families/f10_live_replay_attribution/docs/operational_baseline_identity_20260808_v10.json`；当前 pooled 回测 denominator 见 `research/families/f10_live_replay_attribution/docs/current_live_held_ber_replay_baseline_50d_20260810.json`。它严格使用 live-held BER 时钟；immutable 前 40 日得到 terminal MTM `-144.251748 USDC`、closed-campaign value `-147.466348 USDC` 和 `17,118` fills，合并 50 日得到 `-165.566079 USDC`、`-168.530979 USDC` 和 `20,147` fills。该实现仍是无 raw snapshot/delta tape、无经验 REST/receive-time 延迟的 native-derived diagnostic，strict-native latency successor 完成前不能授权订单路径 action。该 successor 已在 `2026-06-29` 通过一日 raw queue + latency mechanics，但尚无严格 50 日聚合经济结果。旧部署身份只承担不可变 provenance 与回滚，不再承担新实验的默认比较臂。

### 2026-08-02 continuous-path action scorecard v2

新的 action profile 不再把 UTC 日末当成经济终点。现金、库存和 campaign 状态必须跨日延续，逐日会计恒等式之和必须等于全 panel 连续 MTM；panel 最后仍未平仓的库存必须按最终 mid 计价。`day_end_inventory`、open-campaign MTM 和 censoring 的 ranking 权重与 hard-gate 权限均为零，只保留诊断。

新身份为 `action_alpha_v2`、`action_defense_v2`、`action_execution_v2` 与 `action_execution_selective_v3`。closed-campaign value 继承原主要 value 权重，旧 day-end censoring 权重转给 conditional net value；campaign q10/CVaR、MAE、最大库存和 inventory-time 继续作为不可补偿风险门。冻结 v1 profile 与历史结果不改写。ranked-toxicity mechanics 在读取结果前通过 execution-only successor 绑定 `action_execution_selective_v3`，动作、p90、随机种子与运营 baseline 未变。随后的一日权威执行预检发现，活动 maker 订单可以跨越 inventory campaign terminal，从旧 campaign-side assignment 的 add 变成新 assignment 的 opener；因此 v1.4 的 execution eligibility 已撤回，40 日 mechanics 未运行。新身份必须事前冻结 carryover-safe assignment 或 washout，不能让新臂接管旧臂订单。当前仍无正式 mechanics、经济、action 或 live 权限。合同与失败记录见 `docs/experiment_scorecard_v2_continuous_path_profiles_20260802.md` 和 `research/families/f09_campaign_action_uplift/docs/causal_v12_ranked_toxicity_exposure_guard_full_path_mechanics_v1_5_implementation_failure_20260802.md`。

### 2026-07-25 pre-restart 48h loss attribution

最终重启前 `2026-07-22 22:38:07 UTC` 至 `2026-07-24 22:38:07 UTC` 的 366 个完整 flat-to-flat campaign 合计 `-3.6686 USDC`。276 个无 add campaign 合计 `+0.1468`，90 个至少一次 exposure-increasing add 的 campaign 合计 `-3.8154`；LONG/SHORT add 分别为 `-2.3851/-1.4303`。结合上一段独立 live 窗口后，670 个无 add campaign 合计 `+0.3307`，186 个 add campaign 合计 `-13.0119`。稳定问题是持仓后的 marginal add value，而不是固定 BUY 或 SELL 方向。

q10 tail 的 add-fill markout 从 20 秒到 300 秒持续恶化 `-0.82/-1.00/-1.85/-5.33 bps`，非 tail 则在 20 秒后转正，说明有效分界位于 shock/refill/recovery/repair 路径。12 个完整 breaker campaign 合计 `-1.6486`，6 个 IOC close campaign 合计 `-0.9677`；但 non-breaker add campaign 仍亏 `-2.3416`，所以 breaker/IOC 是放大器而非主要 alpha 缺口。

下一信息 family 建议预注册为 `cross_venue_marginal_add_attribution_m1_v1`：只研究 inventory 已非零后的 add，opener/reducing 排除，BUY/SELL 分开；先比较 native local M0 与加入 Bitget/Bybit/OKX spot/perp consensus、Binance BTCUSDT bridge 及 USDTUSDC 的 M1，识别 global-confirmed trend-through 与 local absorption。预测增量未通过 chronological、leave-one-venue-out、freshness/calibration gate 前不创建动作。完整证据见 `research/families/f10_live_replay_attribution/docs/live_pre_restart_48h_loss_attribution_20260725.md`。

---

## 十二、参考文献与开源项目

### 学术论文

| 论文 | 作者 | 年份 | 本项目中的应用 |
|------|------|------|--------------|
| **High-frequency trading in a limit order book** | Avellaneda & Stoikov | 2008 | 核心做市模型：reservation price + optimal spread 公式 |
| **Dealing with the Inventory Risk. A solution to the market making problem** | Guéant, Lehalle & Fernandez-Tapia | 2012 | GLFT 扩展：成交概率随 spread 指数衰减的 κ 参数化 |
| **Algorithmic and High-Frequency Trading** | Cartea, Jaimungal & Penalva | 2015 | 动态 κ 从盘口深度估计、库存风险控制、逆向选择建模 |
| **Zero-intelligence realized variance estimation** | Gatheral & Oomen | 2010 | realized-variance 与微观结构噪声背景；不是项目 size-weighted BBO helper 的直接来源 |
| **The Micro-Price: A High Frequency Estimator of Future Prices** | Stoikov | 2018 | order-book imbalance 条件下的 micro-price；项目实现只是简化 size-weighted BBO proxy，不声称复现完整 estimator |
| **Flow Toxicity and Liquidity in a High Frequency World** | Easley, López de Prado & O'Hara | 2012 | VPIN proxy：volume-time imbalance/toxicity，用于 ML 微结构特征；不是原论文 estimator 的逐项复刻 |
| **Advances in Financial Machine Learning** | López de Prado | 2018 | 样本权重时间衰减、purged K-fold 交叉验证思路 |
| **LightGBM: A Highly Efficient Gradient Boosting Decision Tree** | Ke et al. | 2017 | ML 模型选型：histogram-based GBDT，9 模型实时推理 |
| **Deep RL for Market Making in Corporate Bonds** | Guéant & Manziuk | 2019 | P5 RL 做市：Actor-Critic + Matryoshka inventory expansion |
| **The Financial Mathematics of Market Making** | Guéant | 2016 | γ_opt ∝ 1/(σ·√L)，流动性自适应 γ 理论基础 |
| **Optimal market making** (arXiv:1605.01862v5) | Guéant | 2016 | P6: 一般强度函数 Λ(δ) GLFT，H_ξ(p) Legendre 变换，替代指数近似 |
| **Maker-Taker Fees and the Volatility of Order Flow** | Barzykin, Bergault & Guéant | 2023 | Maker-taker hybrid — 已移除（sweep 验证有害） |
| **High Frequency Automated Market Making Algorithms with Adverse Selection Risk Control via Reinforcement Learning** | Zhao & Linetsky | 2021 | Book Exhaustion Rate 毒性流量检测，spread 动态加宽 |
| **Automated Market Making and Loss-Versus-Rebalancing** | Milionis et al. | 2022 | 仅作为检查波动敏感性的 AMM/CFMM 类比；不证明 CLOB `vol_power=1.5` 或任何 exponent 最优 |
| **Efficient Policy Learning from Surrogate-Loss Classification Reductions** | Bennett & Kallus | 2020 | action-level offline policy learning、overlap 与 policy-value 估计方法参考 |
| **Double Reinforcement Learning for Efficient Off-Policy Evaluation in Markov Decision Processes** | Kallus & Uehara | 2020 | sequential OPE / doubly robust estimator；用于约束 action-uplift 研究，不直接生成 live action |
| **AIIF/DITF Adaptive Reward for RL Market Making** | Vicente et al. | 2023 | 已移除——AIIF/DITF RL 奖励自适应缩放 |

### 开源项目

| 项目 | Stars | 用途 |
|------|-------|------|
| [fedecaccia/avellaneda-stoikov](https://github.com/fedecaccia/avellaneda-stoikov) | 668★ | AS 模型参考实现 (Python)，含模拟与可视化 |
| [joaquinbejar/market-maker-rs](https://github.com/joaquinbejar/market-maker-rs) | 53★ | Rust 高性能 AS 实现，GLFT 扩展，动态 κ 从盘口校准 |
| [Haoyu-tech BTCUSDT L2 research](https://github.com/Haoyu-tech) | — | L2 BTCUSDT 做市研究，fee-aware spread floor，latency modeling |
| [HFTFramework](https://github.com/javifalces/HFTFramework) | 288★ | Deep RL (DQN) 做市，学习最优 γ |
| [ISAC](https://github.com/Panmani/ISAC) | 150★ | SAC 强化学习做市，智能 γ 控制 |
| [ccxt/ccxt](https://github.com/ccxt/ccxt) | 41k★ | 统一交易所 API 库，接口设计参考 |
| [binance-futures-connector-python](https://github.com/binance/binance-futures-connector-python) | — | 本项目使用的 Binance Futures REST + WebSocket SDK |
| [LightGBM](https://github.com/microsoft/LightGBM) | 17k★ | 本项目 ML 模型框架 |

### 推荐扩展阅读

| 主题 | 资源 |
|------|------|
| 做市理论综述 | Guéant, "The Financial Mathematics of Market Making" (2017) |
| 订单流毒性 | Easley & O'Hara, "Microstructure and Ambiguity" (2010) |
| 最优执行 | Almgren & Chriss, "Optimal Execution of Portfolio Transactions" (2001) |
| Binance API 文档 | [binance-docs.github.io/apidocs](https://binance-docs.github.io/apidocs/) |
