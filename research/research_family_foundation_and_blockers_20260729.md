# NarrowGate 研究族基础合同与推进瓶颈审计

Last materially modified: 2026-07-29

Status: current-code audit; no strategy, Validation, sealed holdout, or live permission change.

## 1. 审计结论

本次沿 [`research/registry.json`](registry.json) 的 F01–F10 与 SYS 逐族核对了当前代码、最新冻结报告和共享 D/R/S/G 合同，重点检查：数据粒度、时间可见性、事件身份、量纲、estimand、live/replay 共用语义，以及旧代码是否仍可能被误用。

结论不是“所有未推进的研究族都有基础 bug”。当前可以分成三类：

1. **存在应先修复的基础语义或量纲问题**：F02、F05；F03 若继续作为 action/value 输入，也需要新标签身份。
2. **当前实现没有确认的量纲错误，但受数据粒度或可观测性上限制约**：F04、F08、F10、SYS。
3. **最新合同已经可用，未推进是统计或经济结论本身**：F01、F06、F07、F09。其中 F06/F07/F08 仍保留可执行的历史 runner，属于治理风险，不代表最新合同仍在使用旧逻辑。

| 研究族 | 基础状态 | 当前真正瓶颈 | 是否应继续同一路径 |
|---|---|---|---|
| F01 固定参数 racing | 当前 screening 代码可用；旧 PnL 证据已撤回 | 全局固定参数缺乏跨 side/role/state/campaign 的可迁移 alpha | 否；只保留 screening |
| F02 Empirical P3 | **有跨层语义/时间尺度问题** | 10s `P(touch)` 的 `delta_star/kappa_eff` 被压缩后进入 1s quote core | 先修合同，再谈策略使用 |
| F03 Causal 13-head | 单位/因果修复已通过；**action 时间尺度仍错位** | 标签是先等 fill、再看 h，决策到结果为 h–2h；最新 replay 无经济增益 | 不应继续优化同一 13-head AUC |
| F04 External alpha | 无确认量纲错误；**1s 粒度是硬上限** | 最强结果落在 1s 下边界，无法识别亚秒峰值和真实 receive-time 衰减 | 需要亚秒 receive tape 新身份 |
| F05 Fill quality / Quote EV | **`EV` 量纲名实不符** | `P(fill) × markout_bps` 仍是 expected bps/opportunity，不是 USDC value | 先改值函数身份 |
| F06 Placement fill CIF | 最新 feasibility 合同可用 | 一档不可辨、两档以上虽可辨但 0/54 经济区间通过 | 否；当前路径已正确关闭 |
| F07 Active-order continuation | 新 lifecycle/hazard 合同可用 | favorable-fill 无稳定 skill，动作缺少 selectivity/support | 否；等待新机制证据，不建复杂 v3 |
| F08 Side-taker lifecycle | 双时钟与事件身份正确；历史精确复刻受限 | finalized aggregate 无法精确重建 live slice；宽 denominator 不复现信号 | 只推进 identity/parity 基础设施 |
| F09 Campaign action uplift | 当前单次 intervention OPE 合同可用 | 已登记动作经济下界失败、参与度塌缩或候选过少 | 旧动作不重试；新动作需新身份 |
| F10 Live/replay attribution | 记账与共享策略语义已修复 | 隐藏 queue、跨重启状态、同窗 receive tape；诊断不能识别因果 uplift | 继续做机制校准，不授权动作 |
| SYS 系统工程 | 无确认量纲错误；**环境依赖强** | 2vCPU/约4GiB 下队列陈旧和 main-loop p99，旧 profile 不能代表新 feed/process 布局 | 先重做当前部署剖析 |

## 2. 跨研究族应先处理的问题

### P0：F02 的 10 秒 touch 曲线不应直接充当 1 秒报价经济最优解

[`fill_probability.py`](families/f02_empirical_p3_touch/fill_probability.py) 当前已经正确声明它估计的是：

\[
P(\text{touch within calibrated horizon}\mid \delta),
\]

不包含 queue ahead、touch-to-fill conversion、partial fill、cancel/replace、markout、campaign cost 或 churn。`delta` 的单位是 USDC/BTC，`kappa_eff` 的单位是 `(USDC/BTC)^-1`，这两个数值量纲本身没有错。

问题在它的下游用途。`optimal_delta()` 仍最大化：

\[
\delta P_{\rm touch}(\delta),
\]

并称其为 expected execution revenue；[`strategy/quote_core.py`](../strategy/quote_core.py) 在 regime 开启时又执行 `delta = max(delta, 2 * p3_delta_star)`。当前 bundle 绑定的是 10s touch horizon，而 quote core 的 `quote_horizon_s` 是 1s。这会产生三层错配：

- **estimand**：touch 不是 fill；
- **时间尺度**：10s reachability floor 与 1s variance integration 混用；
- **经济目标**：`delta × P(touch)` 不是包含数量、fill quality 和 campaign cost 的净值。

这不是普通命名问题。P3 floor 可能钳住低波动状态下的 spread，使 F03/F05/F06/F10 看到的候选报价空间已经被一个 10s touch proxy 预先截断。当前 normalized-100ms 的 5s/10s P3 数值复现只证明曲线稳定，不证明这一策略用途正确，见 [`p3_touch_recalibration_normalized100ms_v2_20260725.md`](families/f02_empirical_p3_touch/docs/p3_touch_recalibration_normalized100ms_v2_20260725.md)。

建议：

1. 将 artifact/runtime ABI 明确为 `touch_curve(horizon_s)`，禁止再使用泛化的 `FillProbabilityModel` 名称授予 fill 语义；
2. formal/live config 强制 `p3_horizon_s` 与用途一起绑定，禁止只传 `delta_star/kappa_eff`；
3. 将 `2 * p3_delta_star` 从“经济最优 spread floor”降为单独可审计的历史 baseline mechanism；新的 placement/value 研究使用完整曲线，不继承该最优性解释；
4. 如需 1s 报价输入，单独校准 1s/剩余订单生命周期的 touch 曲线，不能把 10s `delta_star` 机械缩放。

### P0：F05 的 `ev_30s` 不是 USDC EV

[`quote_ev.py`](families/f05_fill_quality_quote_ev/quote_ev.py) 的 canonical bundle 计算：

\[
\texttt{ev\_30s}=P(\text{fill})\,E[m_{30s}\mid\text{fill}],
\]

而 markout bucket 的值是 bps。因此输出单位是 **expected maker-signed bps per quote opportunity**，不是 USDC/decision。代码没有乘成交数量和价格，也没有扣费用、campaign continuation cost、queue reset 或 churn。maker-signed markout 已包含成交价相对未来 mid 的价格价值，因此也不能再机械加一遍 spread。

当前 direct Quote-EV action 已归档，所以这不是正在运行的 live bug；但若继续用 `ev_30s` 作为 Value family 的基础，它会直接造成量纲和目标重复计算问题。

建议保留现有输出作为排序诊断，但改名为类似 `expected_maker_markout_bps_per_opportunity_30s`。真正的 value identity 应输出：

\[
E\left[\sum_i q_i\Delta P_{i,H}-\text{fees}
-\Delta C_{\rm campaign}-C_{\rm churn}\mid x,a\right]
\quad [\text{USDC/decision}],
\]

并显式支持 partial fill、remaining quantity 和 terminal MTM。

### P1：亚秒研究需要重新冻结当前机器上的 receive-to-decision 时钟

F04、F08、F10 的特征或标签已开始进入 100ms–1s 区间。SYS 的历史证据显示，2vCPU/约4GiB 环境中，多 worker dispatcher 会增加排队和 stale-event p99；native batch 虽降低 CPU/内存开销，但旧剖析不能证明当前多行情源、当前进程布局下的 end-to-end main-loop p99。`trade-to-receive lag` 还包含交易所聚合/发布等待，不是纯网络延迟。

因此，任何亚秒 action 研究之前都应冻结部署环境的 exchange timestamp、local receive、feature-ready、decision-ready、order submit 和 ACK 时钟，并给出 drop、queue age、event loop lag 与跨源 skew。否则模型看到的“100ms 信息”可能只是研究机 replay 的能力，而不是目标部署的可实现信息集。历史主机 soak 与性能画像属于私有运营证据，不在公共仓库分发。

### P1：已封闭的历史 runner 仍可执行

以下代码属于冻结证据，但仍暴露 CLI/main 入口：

- F06 的 `direct_fill_cif.py`、早期 full/competing/role curve runners；
- F07 的 [`queue_value_competing_risk.py`](families/f07_active_order_continuation/audit/queue_value_competing_risk.py)；
- F08 的 [`side_taker_hazard_calibration.py`](families/f08_side_taker_lifecycle/audit/side_taker_hazard_calibration.py)。

其中 F07 旧实现仍用齐次指数 competing-risk 公式同时处理 fill、cancel、jump 和 repair；F08 旧 M0 也建立在后来被否定的事件身份上。它们不应删除，因为冻结报告需要可复现，但应增加显式 `historical_evidence_only` 元数据，并让“生成新研究/策略 artifact”的入口 fail-fast；只有传入精确冻结 spec/hash 的复现模式才允许执行。

## 3. 共享基础层审计

### D：数据身份与 good-day

当前 [`models/audit/minimal_marketdata_daily_quality.py`](../models/audit/minimal_marketdata_daily_quality.py) 已经把 target day、完整 D-1 warmup、native sequence、normalized BBO/L2 coverage 和 formal maximum-gap gate 分开。严格 good day 使用多源必需集合的交集是正确的，因为 CryptoHFTData 会缺 hour，而多源时序对齐不能靠插值补齐。

需要持续避免两个误用：

- candidate universe 只表示 source 文件候选，不等于 sequence/causality 已通过；
- `coverage >= 99%` 只表示观测覆盖比例，不代表全天连续。长的内部缺口仍必须由 maximum-gap gate 拒绝，或把实验 estimand 明确限制在连续 segment 并正确 censor label。

当前未发现 D 层仍把 BTCUSDT CryptoHFT orderbook 作为全族必需交集；每个实验仍应冻结自己的 required-source manifest，不能共享一个过宽的“全项目 good day”。

### R/S：replay、生命周期和策略语义

[`time_unit_contract_repair_20260726.md`](system_engineering/docs/time_unit_contract_repair_20260726.md) 及当前测试已覆盖并修复：绝对价格方差、显式 quote horizon、ns/ms/s timestamp、maker-signed markout、commission currency、tick/lot、quantity-weighted PnL、terminal MTM、calendar/DST。最新 replay/lifecycle 路径未发现新的已确认量纲错误。

仍需坚持以下 canonical units：

| 变量 | 单位/身份 |
|---|---|
| BTCUSDC price distance | USDC/BTC |
| tick distance | ticks，必须与 price distance 分列 |
| `sigma_sq` | `(USDC/BTC)^2/s` |
| `sigma_sq * H` | `(USDC/BTC)^2` |
| return / markout | fraction 或 bps，字段名必须显式 |
| value / reward | USDC/decision 或 USDC/campaign |
| quantity | BTC |
| event timestamp | ns；转换为 ms/s 时字段名显式 |
| touch / fill / activation / ACK | 不同事件，不得互换 |

### G：实验治理

当前 chronological split、family-specific sealed evidence、scorecard、known propensity、promotion controller 的方向正确。目录 status 和 prediction pass 均不授予 action/live 权限。F06 的 Validation/holdout 未读、F07/F09 的失败 family 不重调，均符合当前合同。

## 4. 逐研究族审计

### F01 Fixed Parameter Racing

当前 `paired_screen_v2` 和统一 scorecard 可以继续做 alpha screening，旧 `paired_daily_selection()` 仅是兼容入口。历史固定参数 PnL 曾受 left-label timing、trade clock、P3、queue、mixed L2 和 incident semantics 污染，精确数值已在 [`fixed_parameter_strategy_family_closed.md`](families/f01_fixed_parameter_racing/docs/fixed_parameter_strategy_family_closed.md) 撤回。

**没有发现当前 screening 路径仍有新的量纲 bug。** Family 关闭的真实原因是：一个全局固定参数同时作用于 BUY/SELL、opener/add/reducing 和不同 campaign state，改善通常来自参与度变化而非可迁移 alpha。下一步不是合并更多旧 selector，而是让 F09 的独立 action family 承接具体、可识别的机制。

### F02 Empirical P3 Touch

基础数据已从 mixed-cadence BBO 修到 normalized 100ms，5s/10s 曲线稳定。当前问题是上文 P0 的下游语义：它只应提供 horizon-specific touch reachability，不应把 `delta_star` 解释为 fill/value 最优距离。**这是会影响其他族的当前基础问题。**

### F03 Causal 13-head

causal-v7/v9 已修复 feature-ready、calendar、方差与 P3 identity；当前 13-head 代码也明确承认 return/direction label 允许前 h 秒先成交，再观察成交后 h 秒，所以决策到 outcome 横跨 h–2h，并不是 fixed-forward-h return，见 [`ml_model.py`](families/f03_causal_13_head/ml_model.py)。

这在“成交后质量诊断”中可以成立，在 1s quote action/value 中却是 estimand 和时间尺度错位。最新 causal-v9 即使有预测增量，ML-ON replay 仍没有 PnL/terminal 改善，且 formal day denominator 很小，见 [`causal_v9_through_20260725_replay_20260727.md`](families/f03_causal_13_head/docs/causal_v9_through_20260725_replay_20260727.md)。

**真正瓶颈不是模型复杂度或 AUC。** 若继续，应建立 fixed-forward、action-specific、decision-time value/quantity label 的新身份；旧 13-head 保留兼容诊断，不直接授权报价。

### F04 External Market Alpha

BTCUSDT 已改用 Binance 官方 individual trades 构建 1s bars；Bitget/Bybit/OKX 历史状态使用 causal right edge，live 使用 receive time。当前未发现价格/收益率量纲错误。

[`external_information_decay_v1_development_20260727.md`](families/f04_external_market_alpha/docs/external_information_decay_v1_development_20260727.md) 的最佳增量落在 1s 曲线下边界；1–7s 有信息，但 1s archive 无法判断峰值在 50ms、200ms 还是 900ms，也不能还原真实网络、交易所发布和本机排队。**这是数据粒度硬瓶颈，不能用插值或更复杂模型解决。**

下一步只能是新建 receive-time subsecond shadow identity，并先通过 SYS p99 合同；同时把 activity/move prediction 与 direction/value/action uplift 分开。

### F05 Fill Quality, Toxicity and Quote EV

fill toxicity、maker-signed markout 和 fill-selection ranking 可以继续作为 evidence line；但 current family 目录只保留 taxonomy 文档，缺少与当前代码对应的独立 Quote-EV/fill- selection 结论索引，这是文档可追溯性缺口。

基础问题是上文 P0 的 `ev_30s` 单位。另一个 estimand 风险是只在已成交样本上验证 markout ranking：这能回答“已成交后谁更 toxic”，不能回答订单级 `P(fill) × value` 或 action uplift。推进前必须回到 order/opportunity denominator，并将数量、partial fill、费用和 campaign cost 纳入 USDC value。

### F06 Placement Fill CIF

最新 [`paired_action_resolution_feasibility_v1`](families/f06_placement_fill_cif/docs/paired_action_resolution_feasibility_v1_development_20260728.md) 使用 50 个 Development 日、800,853 cohorts、七档共同激活和 ex-ante common 5s clock；旧 ±1 tick 路径 parity 零 mismatch。当前未发现其 raw tick、价格、数量、时钟或经济界量纲错误。

结果本身已经回答了为什么不能进入下一步：相邻 1 tick 为 0/12 cells 可辨；总跨度 2 ticks 为 6/6、单边 2/4 ticks 为 12/12，但 economic interval 0/54 通过。最大确定性价格改善约 `3.28e-5 USDC/decision`，低于 pending uncertainty 的 `1.06e-4–2.98e-4 USDC/decision`。

**这是经济分辨率不足，不是需要 ordered surface v2 的模型问题。** 维持关闭是正确结论。只有独立、跨日 OOF 的 marginal-fill markout、filled quantity 和 campaign-cost 证据显著收窄 value bound，才可用新 family identity 重开；不能读取原 Validation/ holdout 救结果。

### F07 Active Order Continuation

旧 [`queue_value_competing_risk.py`](families/f07_active_order_continuation/audit/queue_value_competing_risk.py) 把 cancel、jump、repair 和 fill 放进齐次指数 competing risk，风险起点与事件身份不成立。它应视为历史实现，不能作为新研究基础。

当前 `dynamic_fill_hazard.py` 已改为 start-stop/discrete risk set，区分 cancel request/ACK、ACK 前 fill、remaining quantity、native non-absorbing jump 和 repair delayed entry；没有发现新量纲问题。Family 仍关闭，是因为 adverse head 通过不能补偿 favorable-fill head 缺乏 Brier skill，而 cancel 会摧毁未建模的 favorable queue option。此前动作也表现为候选太少、support 不足或参与度塌缩。

下一步不是扩大模型，而是等新的、可复现的 favorable-fill 机制证据或更强 observation contract；BUY q90 operational trial 必须继续与 research promotion 分离。

### F08 Side-Taker and Lifecycle Identity

当前双时钟合同是正确的：Vision individual trades 只作 exchange-time matching/queue outcome，mapped parent `aggTrade` 的 feature-ready clock 才是 policy 可见时间。不能用 individual exchange timestamp 提前生成 10/25/50/100ms 策略特征，见 [`binance_trade_lifecycle_contract_v2_20260723.md`](families/f08_side_taker_lifecycle/docs/binance_trade_lifecycle_contract_v2_20260723.md)。

当前瓶颈有两个：

- finalized historical parent 可能在 live 首个 slice 后继续扩展，因此无法仅靠归档 parent 精确复刻 live 的 quantity/count/`f..l`；block 内 child receive order 永久不是历史 policy feature；
- 初始 1,448 行 BUY 100ms pressure/markout 线索没有在 277,368 行宽 denominator 上复现，SELL 证据也弱。

所以 static side-taker hazard M0 关闭正确；identity/parity 基础设施仍可推进。需要同窗 receive tape 或显式 source-extension sensitivity，不应重启 constant-hazard v2。

### F09 Campaign Action Uplift

当前 randomized replay/OPE 具备 known propensity、chronological folds、day/campaign cluster、USDC reward、一个 campaign 最多一次 intervention 和统一 scorecard。所抽查测试未发现量纲或 propensity plumbing 错误。

它的边界也很明确：当前是单次 contextual intervention OPE，不是多步 sequential campaign policy evaluator。若将同一 campaign 内的连续 KEEP/REPLACE/REARM 动作都塞进现有 panel，会违反 estimand，而不是“增加样本”。

已登记 family 没推进，是因为 reward 下界跨零/为负、候选稀少、action strength 不足或参与度/成交数塌缩，不是基础代码阻塞。旧 BUY widen、SELL skip、stop-add/rearm 不应在已消费 panel 上改阈值重试。多步动作需要单独的 sequential OPE/随机化合同。

### F10 Live/Replay Attribution

共享 quote core、记账方向、quantity-weighted PnL、terminal MTM 和初始 inventory/order/ campaign state 的方向已经正确。当前未确认新的数值量纲 bug。

但早期 live/replay parity 记录已被后续单位与时钟修复 supersede，不能当作当前端到端 parity certificate。现有差异主要来自：不可观测 queue priority/hidden liquidity、submit/cancel/ACK race、跨重启活动单与 campaign 状态、以及历史与 live 不同的 receive-time 路径。该运营记录本身不在公共仓库分发。

私有的历史 loss-attribution 只能定位 add campaign 的亏损集中区；它不是因果 action 证据，也不授予公共部署权限。

下一步应冻结一个 **修复后、同窗、完整初始状态、完整 receive/order lifecycle tape** 的新 parity identity；其通过只授予 mechanism calibration。具体 action 仍交给 F09。

### SYS Low-Latency and Replay Parity

当前系统代码没有发现新的 ns/ms/s 或价格方差量纲错误。真正风险是系统容量改变了研究可见信息集：多个行情源在 2vCPU/约4GiB 机器上排队，会让 event processing p99 远大于网络 p99，且 lossless queue 也可能产生秒级 stale data。

下一步应以当前 feed 集合和进程布局重测，而不是继续引用旧 soak：

1. 每源 receive rate、decode CPU、queue depth/age 和 drop；
2. exchange → receive → feature-ready → decision-ready 分段 p50/p95/p99/max；
3. main loop、REST/order manager 与 recorder 是否争用 GIL/CPU；
4. 降级策略是否按信息年龄 fail closed，而不是继续消费陈旧状态；
5. 新 profile hash 进入 F04/F08/F10 的 artifact contract。

## 5. 建议执行顺序

1. **先修 F02 runtime contract**：分离 horizon-specific `P(touch)` 与 live spread floor；所有研究重新报告 P3 floor/cap hit rate，但不因此重读 sealed panel。
2. **修 F05 value 命名和单位**：保留现有 bps scorer 作为 diagnostic，禁止它输出或被解释为 USDC EV；新的 value family 先冻结 quantity-weighted 公式。
3. **重做当前 EC2 多行情源 p99 profile**：在此之前不启动新的亚秒 external/taker action family。
4. **给 F06/F07/F08 旧 runner 加历史复现边界**：不改冻结结果，不删除代码，只阻止它们生成新 identity 或策略 artifact。
5. **维持 F06、F07、F09 的阴性结论**：没有新独立机制或更窄经济误差界时，不应通过更复杂模型继续消耗同一 Development panel。
6. **F03 只保留兼容诊断**：如果要恢复 ML，建立 fixed-forward/action-value 新 family，不再用旧 13-head 的 predictive score 作为 promotion 目标。
7. **F04/F08/F10 共用一次新的 receive-time capture**：一个带 source contract、完整时钟和资源 profile 的 tape 可以同时服务三族，避免重复占用磁盘。

## 6. 验证范围

本次没有运行训练、下载数据、读取 Validation 或 sealed holdout，也没有修改策略参数。使用项目 `.venv/bin/python`（Python 3.12.13）执行完整测试：

```text
923 passed, 4 skipped
```

测试通过说明当前目录迁移、已覆盖的单位合同和结构接口没有回归；它不否定本报告识别的 estimand、时间尺度、可观测性和经济分辨率问题，因为这些问题大多是跨模块研究合同，而不是单元测试中的数值异常。
