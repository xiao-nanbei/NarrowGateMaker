# 固定参数策略研究族关闭报告

Date: 2026-07-27

Last materially modified: 2026-07-27

Status: **Closed research family**

Evidence cutoff: 2026-07-27

Live authority: **None**

## 决策摘要

NarrowGate 正式关闭以下 alpha 研究范式：

> 在全部市场状态上反复搜索一组全局固定的 `gamma`、`kappa`、cooldown、maximum inventory、spread cap、guard threshold 或固定一档报价动作，再以 pooled PnL 或某个历史面板的 winner 作为策略晋级依据。

关闭原因不是“固定常数在数学上不可能存在最优解”，而是项目没有得到一组在修复后的因果回放、side/role 分层、chronological transfer、campaign tail 和 action-uplift 口径下仍稳定为正的固定动作。历史搜索反复表现为：

- 重新分配成交量、库存时间和尾部风险，而不是稳定创造订单级净价值；
- Development 的赢家在 Validation 收缩、反号或发生 winner rotation；
- 某些防守参数通过减少参与改善局部风险指标，却同时切断有价值成交或自然修复；
- 同一规则对 BUY/SELL、opener/add/reducing 和不同盘口状态产生不同作用；
- 旧的精确 PnL/rank 又受到已修复的 feature timing、trade clock、P3、queue、mixed-L2 和历史 live incident 语义影响，不能继续用于当前选参。

因此，这一研究族的最终结论是：**不再把“寻找另一组全局固定参数”当作 alpha 发现路径。** 以后若出现新的状态条件、动作和经济机制，必须注册为新的 action family，而不是扩大旧 grid、改阈值或重命名旧 arm。

本报告不修改 live 配置，也不授权任何策略部署。

## 关闭边界

### 已关闭

- `gamma / kappa ratio / depth-kappa / cap / guard / cooldown / max inventory` 的全局联合 sweep 与 winner selection；
- 用 pooled raw PnL、Sharpe 或单个 retained panel 选择所谓“最优参数”；
- 固定 elapsed-time rearm、固定一档 widen/re-center、固定跳过一个 add cycle；
- 在已读取 outcome 的 panel 上改变 grid、阈值、模型自由度或评分权重来救回同一族；
- 把减少 fills、inventory time 或 campaign 数本身解释成 alpha；
- 把 P3 `delta_star/kappa_eff` 等执行校准量当成可自由搜索的 PnL knob。

### 没有关闭

- AS/GLFT 作为库存感知的报价坐标系；
- tick、lot、手续费、GTX/IOC 语义等交易所或账户规则；
- hard inventory limit、熔断、最大 pair spread 等安全边界；
- 为保证可复现而冻结的 baseline 参数、随机种子和 latency profile；
- P3 touch curve、queue、latency、volatility 等直接经验校准；
- 用固定 grid 做机制 smoke、单调性检查或 sensitivity analysis；
- side-specific、role-specific、state-conditioned 的 action-value 研究。

换句话说，固定值仍可作为**约束、校准产物和 rolling baseline 身份**，但不得再被描述成已经证明的普适 alpha 或市场常数。

## 证据分层

### 1. 历史全局参数搜索：数值撤回，研究权限关闭

项目曾运行 48/512/1024-arm generations，以及 retained39、blocked71、late4 等多轮筛选。它们覆盖 `gamma`、cap、guard、cooldown、queue 和执行组合。

这些旧实验的精确 PnL、排名和 winner 身份已正式撤回，因为其 replay identity 包含后来确认并修复的左标签特征、trade-clock、历史 P3 override、旧 queue、mixed L2 或 live incident 语义。它们不能作为当前 baseline 的数值证据，也不会在旧 ID 下重跑。详见：

- [Historical Backtest Evidence Revalidation](../../f10_live_replay_attribution/docs/historical_backtest_evidence_revalidation_20260720.md)
- [Legacy L2 Evidence Revalidation](../../../../docs/legacy_l2_evidence_revalidation_20260725.md)
- [Causal-v5 Normalized-100ms Revalidation](../../f03_causal_13_head/docs/causal_v5_normalized100ms_revalidation_20260725.md)
- [Causal-v5 Revalidation Registry](../../f03_causal_13_head/docs/causal_v5_revalidation_registry_20260725.md)

数值撤回并不恢复这些 arm 的研究权限。相反，它意味着没有任何旧 winner 保留了晋级依据；重新测试必须是新 family、新 causal identity 和新 action estimand。

### 2. 固定报价动作：没有稳定 transfer

最初的 side-specific 本地动作族比较 baseline、prevent-over-widen、`widen_1tick` 和 `recenter_1tick`。历史结果显示 Development 领导动作没有在 Validation 保持，出现 winner rotation，且极端 campaign tail 缺乏有效支持。该族已关闭，sealed holdout 未被用于救回失败候选。

随后冻结的 BUY add `baseline / widen_1tick` 和 SELL add `baseline / skip_one_add_cycle` 也都在 Development 关闭。它们的旧精确 DR、fill 和 campaign 数值已随旧 denominator 撤回，但 no-promotion 和 family closure 仍为最终决定：数据修复不能让一个已经读取过 outcome 的固定 action identity 自动重生。

相关记录：

- [Side-Specific Action Uplift](../../f09_campaign_action_uplift/docs/side_specific_action_uplift_existing_split_20260718.md)
- [BUY Conditional Widen](../../f09_campaign_action_uplift/docs/buy_add_conditional_widen_causal_v4_v1_20260718.md)
- [SELL Repair-vs-Trend Skip](../../f09_campaign_action_uplift/docs/sell_add_repair_trend_skip_causal_v4_v1_20260718.md)

### 3. 固定 cooldown 的替代规则：状态描述不等于动作价值

`85s` cooldown 是历史策略选择值，不是理论半衰期。项目尝试过在 85 秒后根据 adverse move、flow persistence、refill 和 recovery 决定继续 block。BUY/SELL 两侧均未通过完整 Development gate；更窄的 recovery-event 版本虽然满足 candidate-rate 和 fills-retention 支持，但 SELL 的条件净价值显著变差。

这些结果否定的是被冻结的动作映射，并不否定 shock/refill/recovery 作为状态变量。它们说明：识别“路径较差”不能直接推出“继续停止加仓更好”，因为该动作也可能切断 repair fill。

相关记录：

- [State-Conditioned Rearm After 85 Seconds](../../f09_campaign_action_uplift/docs/state_conditioned_rearm_after85_v1_20260722.md)
- [Recovery-Event Rearm](../../f09_campaign_action_uplift/docs/recovery_event_rearm_v1_20260722.md)
- [SELL Campaign Add Permission](../../f09_campaign_action_uplift/docs/sell_campaign_add_permission_v1_20260722.md)

### 4. 简单 threshold 防守会退化为 participation shutdown

在 native snapshot/delta 和 individual-trade identity 下，`queue_value_net_hazard_keep_cancel_v2` 已直接估计 competing-risk 与 `V_keep - V_cancel/reenter`。其 K1 动作只保留 `7.67%` 的 intervention fills，同时仍保留 `8.29%` 的 toxic fills；toxic reduction leverage 为 `0.9933`，没有选择性。随机化 pooled ITT 为 `-0.01448 USDC/intervention`，95% UTC-day 区间 `[-0.02577,-0.00239]`。

这是当前最清楚的机制证据之一：一个看似“减少毒流”的阈值动作如果等比例甚至更慢地减少 toxic fills，本质上只是停止参与。降低交易量本身允许，但必须换来更快的 toxic-fill 降幅和正的条件净价值。

详见 [Queue-Value Net-Hazard Keep/Cancel v2](../../f07_active_order_continuation/docs/queue_value_net_hazard_keep_cancel_v2_20260722.md)。

### 5. 名义上的动态参数也不自动产生 alpha

现有 dynamic-mechanism audit 表明：regime scaling 确实传导，但 P3 floor 会覆盖部分低波动调整；depth-kappa 在普通状态下接近常数；dynamic cap 几乎不触发；cooldown 仍呈 `85/170/255/...` 的阶梯结构。该审计是描述性 attribution，不是 action uplift，而且其中旧 replay 数值已经撤回。

它保留的工程教训是：把常数写成函数并不等于策略已经状态化。必须验证函数是否实际改变订单路径，以及这种改变是否带来正的条件价值。

详见 [Dynamic Mechanism Campaign Audit](../../f10_live_replay_attribution/docs/dynamic_mechanism_campaign_audit_20260722.md)。

## 理论解释

理论文献不会直接证明 NarrowGate 的某个参数族失败；项目的关闭决定来自上述实证。理论提供的是失败为何具有结构性的解释。

### AS/GLFT 给出控制形式，不给出普适常数

Avellaneda-Stoikov 和 GLFT 在明确的价格过程、风险偏好、有限时间跨度和订单到达强度假设下求解报价。其一般结构可以写成：

\[
a^*(x)=\arg\max_a V(x,a),
\]

其中状态 \(x\) 至少包含库存、波动率、剩余时间和到达强度。`gamma`、强度函数和 horizon 改变时，最优报价也改变。理论支持“参数必须绑定模型和状态”，不支持“历史 pooled winner 是跨状态常数”。

- [Avellaneda and Stoikov, *High-frequency trading in a limit order book*](https://doi.org/10.1080/14697680701381228)
- [Guéant, Lehalle and Fernandez-Tapia, *Dealing with the Inventory Risk. A solution to the market making problem*](https://arxiv.org/abs/1105.3115)
- [Guéant, *Optimal market making*](https://arxiv.org/abs/1605.01862)

### 订单强度依赖盘口状态和事件历史

Queue-reactive 模型让 limit、market 和 cancel 强度依赖当前订单簿状态；state-dependent Hawkes 又让订单流 excitation 与 spread、queue imbalance 等状态相互反馈。因此一个全局 `kappa`、固定 cooldown 或固定 adverse threshold 只能是混合状态后的平均近似：

\[
\lambda_k(t)=\lambda_k(\mathcal H_t,\text{book}_t,\text{side},\text{queue}_t),
\]

而不是只由一个常数决定。

- [Huang, Lehalle and Rosenbaum, *The Queue-Reactive Model*](https://arxiv.org/abs/1312.0563)
- [Morariu-Patrichi and Pakkanen, *State-dependent Hawkes processes and their application to limit order book modelling*](https://arxiv.org/abs/1809.08060)

### 公允价与毒性同样是状态条件对象

经验 microprice 把 spread、queue imbalance 和状态转移用于估计未来 mid；OFI 研究也表明短期价格变化与盘口事件和深度有关。因而同样的一档距离在不同 side、queue、refill 和 microprice 状态下具有不同的 fill value。

- [Stoikov, *The Micro-Price: A High Frequency Estimator of Future Prices*](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2970694)
- [Cont, Kukanov and Stoikov, *The Price Impact of Order Book Events*](https://doi.org/10.1093/jjfinec/nbt003)

### 预测好坏不等于动作 uplift

固定参数 sweep 估计的通常是某个完整策略路径的 pooled outcome；它无法单独回答同一 quote-time 状态下，候选动作相对 baseline 的条件增量价值：

\[
\tau(x)=E[R\mid do(a_1),x]-E[R\mid do(a_0),x].
\]

离线策略学习和 doubly robust OPE 要求明确 action、behavior propensity、overlap、action-specific reward 和 uncertainty。支持不足的状态应回退 baseline，而不是由回归外推或 pooled winner 决定。

- [Bennett and Kallus, *Efficient Policy Learning from Surrogate-Loss Classification Reductions*](https://proceedings.mlr.press/v119/bennett20a.html)
- [Kallus and Uehara, *Double Reinforcement Learning for Efficient Off-Policy Evaluation*](https://jmlr.org/papers/v21/19-827.html)
- [Laroche, Trichelair and Tachet des Combes, *Safe Policy Improvement with Baseline Bootstrapping*](https://proceedings.mlr.press/v97/laroche19a.html)

## 数值身份结论

当前 live 中仍存在固定数值，但它们的权限必须按来源解释。完整登记见 [Value Provenance Registry](../../../../docs/value_provenance_registry_20260727.md)。

| 数值 | 当前身份 | 不能怎样解释 |
|---|---|---|
| `gamma=0.046` | empirical policy selection | 不是理论风险厌恶常数或已证明最优值 |
| P3 `delta_star=13.9991 USDC/BTC` | empirical direct estimate | 不是 13.999 ticks，也不是 PnL knob |
| P3 `kappa_eff=0.067356 (USDC/BTC)^-1` | empirical direct estimate | 不是订单到达率 |
| quote horizon `1s` | judgmental engineering | 不是自然或最优预测 horizon |
| order size `0.001 BTC` | judgmental risk budget | 不是理论最优 size |
| max inventory `0.026 BTC` | hybrid risk budget | 不是扩大后即可增加 alpha 的参数 |
| requote `5s` | hybrid operating choice | 不是市场固有时钟 |
| add cooldown `85s` | empirical policy selection | 不是 Hawkes half-life |
| pair-spread cap `20bps` | hybrid safety cap | 不是单边 20bps 或 alpha 来源 |
| BUY threshold `0.44` | empirical ranking threshold | 不是 44% fill/favorable probability |

## 后续研究契约

固定参数族关闭后，研究顺序改为：

1. 分开 placement fill CIF 与 active-order KEEP/REPLACE estimand；
2. 用 side-specific、role-aware 的动态 fill hazard 建模订单风险集；
3. 用 queue-reactive intensity、refill/recovery path 和 empirical microprice 估计活动订单价值；
4. 冻结一个有真实作用强度的动作，并按 campaign 最多随机一次；
5. 使用真实 propensity、overlap、ESS、DR uplift、toxic-fill selectivity、campaign terminal 和 tail 共同评估；
6. 不确定或 unsupported 状态按 SPIBB 原则回退 rolling baseline；
7. local M0 通过后，外部 venue 只作为订单价值的增量 M1 特征。

每个 successor family 必须接入 canonical scorecard 和独立 promotion controller。prediction pass 只能授权后续随机化实验，不能直接授权 live action。

## 永久关闭规则

以下操作不能重新打开本研究族：

- 增加更多固定参数点或扩大搜索范围；
- 在旧 retained/blocked/late panel 上重新排名；
- 改用另一个 pooled PnL、Sharpe、InvAdj 或 winner score；
- 改 threshold、候选率、树深或置信门槛以救回已失败动作；
- 把旧 family 改名后继续使用同一 action semantics。

未来若有新的经济机制，必须同时具备新的 action semantics、family ID、冻结数据身份、chronological split、known propensity 和 score profile。那属于新的 state-conditioned action family，不属于本报告所关闭的固定参数族复活。

## 最终结论

固定参数并非在理论上“无解”；在任意冻结的模型和样本上，通常都可以找到一个局部或全局 pooled optimum。NarrowGate 已经证明的是另一件更重要的事：这些 pooled optimum 没有形成可迁移、可识别、兼顾成交选择与 campaign 尾部的稳定 maker action uplift。

所以本研究族以**完成的阴性结果**关闭。现有固定值继续作为 baseline、校准或安全约束存在；alpha 研究转向“在这个具体状态下，保留、撤单、重进、加仓或减仓哪个动作的条件净价值更高”。
