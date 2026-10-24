# NarrowGate 研究族基础合同与推进瓶颈审计

Last materially modified: 2026-10-17

Status: historical July engineering audit with current research-question routing; not a current implementation audit or a new economic result.

## 1. 当前阅读口径

本文保留 2026 年 7 月审计的单位、时钟、风险集与工程缺陷记录。旧研究族经济判断、通过比例和“无需继续”的建议已移出当前结论；本次没有重新执行历史测试，也不把当时的接口名称当成当前代码事实。当前实现与已验收数据范围见各研究族报告；待验证问题从 [registry.json](registry.json) 的 `research_questions` 扫描。

## 2. 跨研究族应先处理的问题

### P0：F02 的 10 秒 touch 曲线不应直接充当 1 秒报价经济最优解

[`fill_probability.py`](families/f02_empirical_p3_touch/touch_probability.py) 当前已经正确声明它估计的是：

\[
P(\text{touch within calibrated horizon}\mid \delta),
\]

不包含 queue ahead、touch-to-fill conversion、partial fill、cancel/replace、markout、库存生命周期 cost 或 churn。`delta` 的单位是 USDC/BTC，`kappa_eff` 的单位是 `(USDC/BTC)^-1`，这两个数值量纲本身没有错。

问题在它的下游用途。`optimal_delta()` 仍最大化：

\[
\delta P_{\rm touch}(\delta),
\]

并称其为 expected execution revenue；[`strategy/quote_core.py`](../strategy/quote_core.py) 在 regime 开启时又执行 `delta = max(delta, 2 * p3_delta_star)`。当前 bundle 绑定的是 10s touch horizon，而 quote core 的 `quote_horizon_s` 是 1s。这会产生三层错配：

- **estimand**：touch 不是 fill；
- **时间尺度**：10s reachability floor 与 1s variance integration 混用；
- **经济目标**：`delta × P(touch)` 不是包含数量、fill quality 和 库存生命周期 cost 的净值。

这不是普通命名问题。P3 floor 可能钳住低波动状态下的 spread，使 F03/F05/F06/F10 看到的候选报价空间已经被一个 10s touch proxy 预先截断。当前 normalized-100ms 的 5s/10s P3 数值复现只证明曲线稳定，不证明这一策略用途正确，见 [`p3_touch_recalibration_normalized100ms_v2_20260725.md`](families/f02_empirical_p3_touch/docs/p3_touch_recalibration_normalized100ms_v2_20260725.md)。

建议：

1. 将 artifact/runtime ABI 明确为 `touch_curve(horizon_s)`，禁止再使用泛化的 `TouchProbabilityModel` 名称授予 fill 语义；
2. formal/live config 强制 `p3_horizon_s` 与用途一起绑定，禁止只传 `delta_star/kappa_eff`；
3. 将 `2 * p3_delta_star` 从“经济最优 spread floor”降为单独可审计的历史 baseline mechanism；新的 placement/value 研究使用完整曲线，不继承该最优性解释；
4. 如需 1s 报价输入，单独校准 1s/剩余订单生命周期的 touch 曲线，不能把 10s `delta_star` 机械缩放。

### P0：F05 的 `ev_30s` 不是 USDC EV

[`quote_ev.py`](families/f05_fill_quality_quote_ev/quote_ev.py) 的 canonical bundle 计算：

\[
\texttt{ev\_30s}=P(\text{fill})\,E[m_{30s}\mid\text{fill}],
\]

而 markout bucket 的值是 bps。因此输出单位是 **expected maker-signed bps per quote opportunity**，不是 USDC/decision。代码没有乘成交数量和价格，也没有扣费用、库存生命周期 continuation cost、queue reset 或 churn。maker-signed markout 已包含成交价相对未来 mid 的价格价值，因此也不能再机械加一遍 spread。

当前 direct Quote-EV action 已归档，所以这不是正在运行的 live bug；但若继续用 `ev_30s` 作为 Value family 的基础，它会直接造成量纲和目标重复计算问题。

建议保留现有输出作为排序诊断，但改名为类似 `expected_maker_markout_bps_per_opportunity_30s`。真正的 value identity 应输出：

\[
E\left[\sum_i q_i\Delta P_{i,H}-\text{fees}
-\Delta C_{\rm inventory lifecycle}-C_{\rm churn}\mid x,a\right]
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
| value / reward | USDC/decision 或 USDC/inventory_lifecycle |
| quantity | BTC |
| event timestamp | ns；转换为 ms/s 时字段名显式 |
| touch / fill / activation / ACK | 不同事件，不得互换 |

### G：实验治理

当前 chronological split、family-specific sealed evidence、scorecard、known propensity、promotion controller 的方向正确。目录 status 和 prediction pass 均不授予 action/live 权限。F06 的 Validation/holdout 未读、F07/F09 的失败 family 不重调，均符合当前合同。

## 4. 逐研究族的当前去向

| 研究族 | 统一问题入口 | 已验收结果 |
| --- | --- | --- |
| F01 | [待验证问题](families/f01_fixed_parameter_racing/research_questions.json) | [当前报告](families/f01_fixed_parameter_racing/README.md) |
| F02 | [待验证问题](families/f02_empirical_p3_touch/research_questions.json) | [当前报告](families/f02_empirical_p3_touch/README.md) |
| F03 | [待验证问题](families/f03_causal_13_head/research_questions.json) | [当前报告](families/f03_causal_13_head/README.md) |
| F04 | [待验证问题](families/f04_external_market_alpha/research_questions.json) | [当前报告](families/f04_external_market_alpha/README.md) |
| F05 | [待验证问题](families/f05_fill_quality_quote_ev/research_questions.json) | [当前报告](families/f05_fill_quality_quote_ev/README.md) |
| F06 | [待验证问题](families/f06_placement_fill_cif/research_questions.json) | [当前报告](families/f06_placement_fill_cif/README.md) |
| F07 | [待验证问题](families/f07_active_order_continuation/research_questions.json) | [当前报告](families/f07_active_order_continuation/README.md) |
| F08 | [待验证问题](families/f08_side_taker_lifecycle/research_questions.json) | [当前报告](families/f08_side_taker_lifecycle/README.md) |
| F09 | [待验证问题](families/f09_inventory_lifecycle_action_uplift/research_questions.json) | [当前报告](families/f09_inventory_lifecycle_action_uplift/README.md) |
| F10 | [待验证问题](families/f10_live_replay_attribution/research_questions.json) | [当前报告](families/f10_live_replay_attribution/README.md) |

新 Tardis 下已验收的训练和账户比较直接替代相同问题的旧表述；旧研究的方法缺口不因此被虚构为已经重做。F03 新模型比较、F04 五候选、F05 prefill E/C、F07 有界 U0/U1、F08 response 各有不同模型、动作和覆盖，不能互相替代。

### SYS Low-Latency and Replay Parity

当前系统代码没有发现新的 ns/ms/s 或价格方差量纲错误。真正风险是系统容量改变了研究可见信息集：多个行情源在 2vCPU/约4GiB 机器上排队，会让 event processing p99 远大于网络 p99，且 lossless queue 也可能产生秒级 stale data。

下一步应以当前 feed 集合和进程布局重测，而不是继续引用旧 soak：

1. 每源 receive rate、decode CPU、queue depth/age 和 drop；
2. exchange → receive → feature-ready → decision-ready 分段 p50/p95/p99/max；
3. main loop、REST/order manager 与 recorder 是否争用 GIL/CPU；
4. 降级策略是否按信息年龄 fail closed，而不是继续消费陈旧状态；
5. 新 profile hash 进入 F04/F08/F10 的 artifact contract。

## 5. 研究执行顺序不由旧结果决定

以当前人类授权和所属实验计划为准。单位与可见性检查保留，但旧阴性结果不形成永久禁止重做的规则；必要重做先审查方法、绑定当前数据与基线，再验收完整配对。原始结果、工程失败和 previous-use 保留，不能把已看过的 Final 重新称作 fresh holdout。

## 6. 验证范围

本次没有运行训练、下载数据、读取 Validation 或 sealed holdout，也没有修改策略参数。使用项目 `.venv/bin/python`（Python 3.12.13）执行完整测试：

```text
923 passed, 4 skipped
```

测试通过说明当前目录迁移、已覆盖的单位合同和结构接口没有回归；它不否定本报告识别的 estimand、时间尺度、可观测性和经济分辨率问题，因为这些问题大多是跨模块研究合同，而不是单元测试中的数值异常。
