# 进攻型 / 防守型策略改动分类与验证框架（2026-07-06）

> 2026-07-27 修订：攻防分类仍有效，但旧参数 racing、causal-v4 数值和旧 L2/queue 路径不再具有当前证据效力。下式已按现行量纲修正：maker-signed markout 以成交价相对未来 mid 定义，本身已经包含成交价格优势，不能再机械地叠加一次 spread；条件 fill value 也必须乘以成交概率。新 paired 研究使用 `paired_screen_v2`，action 结论使用冻结 scorecard 与独立 panel controller。

本文档整理 2026-07-06 会话中关于「进攻型 arm vs 防守型 arm」的分类讨论，以及由此导出的不对称验证标准和 BTCUSDT ref 的机制设计结论。适用范围：`live/config.yaml` 参数改动、tick replay A/B arm、parameter racing 候选。

---

## 1. 定义：按改动作用在 EV 式的哪一侧

$$
\mathrm{EV}_{\text{side}}(a\mid x) \approx
\underbrace{P(\mathrm{fill}\mid a,x)
E[V_{\mathrm{fill}}\mid\mathrm{fill},a,x]}_{\text{成交机会与成交后净价值}}
-\underbrace{E[C_{\mathrm{campaign}}\mid a,x]
+C_{\mathrm{queue/reset/churn}}(a,x)}_{\text{库存尾部与执行成本}} .
$$

当 $V_{\mathrm{fill}}$ 使用 maker-signed markout 时，它已经包含成交价相对未来 mid 的价格优势；不得再额外加一次 `spread edge`。若使用 bps/per-BTC markout，还必须乘成交数量并转换成 USDC 后，才能与 campaign cost 相加减。

- **进攻型**：提高 $P(\mathrm{fill})$ 或扩大暴露，争取更多正的条件净成交价值；成交概率本身不是目标。
- **防守型**：压低 adverse selection 和库存尾部风险，收缩暴露。
- **第三类（repricing / re-center）**：整体平移 fair value / reservation，不改变 spread 宽度，一侧防守、一侧进攻同时发生（见第 5 节）。

### 1.1 适用边界：bucket 是证据，arm 是执行形态

这个分类可以覆盖当前大多数 arm 和 bucket，但必须先分清两层：

```text
bucket:
  quote-time 状态 / residual / rank / risk label
  例: trend_inventory_risk_high, ref_pending_up, refill_weak,
      campaign_risk_high, queue_back, flow_decel

arm:
  把一个或多个 bucket/score 映射到报价控制
  例: re-center, spread widen/tighten, inventory skew, TTL/cooldown/replace cadence
```

一个 bucket 本身不应该被命名成 policy，也不能因为它看起来 toxic 就直接变成 `pause` / `cancel` / `veto`。正确流程是：

```text
bucket evidence -> continuous score/residual -> one tiny mixed control
                -> daily mechanism gate -> campaign/order outcome gate
                -> shadow/live observe -> promotion discussion
```

因此，bucket 表可以按「状态来源」组织；arm 表必须按「控制轴 + 攻防属性」组织。同一个 bucket 可以支持多个 arm 形态，同一个 arm 也可以由多个 bucket 合取触发。

---

## 2. 三条轴

### 2.1 spread 轴（成立，直接可用）

- 更窄 spread = 用更高 $P(\mathrm{fill})$ 换更薄的每笔补偿 → 进攻。
- 更宽 spread = 防守。
- 对应 knob：`kappa_ratio`、`max_spread_bps`、`dynamic_cap_*`、`adverse_spread_mult`、`defense_spread_mult`。

### 2.2 订单曝光时间轴（成立，防守端有下限）

- 更长曝光 = fill 概率累积 + 队列优先权资产 → 进攻。
- 更短 TTL / 更频繁 requote = 防守。
- **注脚：过度撤单不是防守，是自毁。** 每次 cancel/replace 回到队尾并制造 REST churn。`replace_min_price_change_ticks=20 / replace_min_interval_ms=500` 正是承认这一点：减少曝光调整频率同时保住队列价值和尾延迟。曝光轴的防守端存在下限，越过之后是纯损耗。

### 2.3 skew 轴（原始表述不成立，必须按来源拆开）

「更偏的 ask-mid / mid-bid」不能直接归为进攻。同一个不对称形态有两种来源：

| skew 来源 | 方向 | 攻防属性 |
|---|---|---|
| **库存 skew** | AS reservation shift $r = s - q\gamma\sigma^2\tau$，偏向减仓侧 | **防守** |
| **信号 skew** | `ret_skew` / `inventory_asym` / 方向信号驱动，偏向加仓侧 | **进攻** |

推论：

- 持仓时「更均衡」反而是进攻——有库存还挂对称盘等于拒绝减仓、继续吃双边风险。
- 判据不是偏不偏，而是**偏移在缩小还是放大 exposure-increasing 一侧的成交概率**。side policy 里的 `exposure_increasing` 判断已经是这个口径。

---

## 3. 攻防对照表

| | 进攻型 | 防守型 |
|---|---|---|
| 作用 | 提高 $P(\mathrm{fill})$、扩大暴露 | 压低 adverse/tail、收缩暴露 |
| 手段 | 窄 spread、长曝光、大 size、放开加仓侧、信号 skew | 宽 spread、短 TTL、pause、cooldown、库存 skew、缩 size |
| 收益可见性 | **立即可见**（fills/day、raw PnL 上升） | 藏在尾部（campaign loss、tail 减少） |
| 成本可见性 | 藏在尾部（toxic fill、campaign 拖长） | **立即可见**（fills/day 下降） |
| 对应现有机制 | realized spread 下降、`fill_cooldown`↓、TTL↑、加仓侧 replace 更积极 | realized spread 上升、adverse/defense guard、markout pause、cooldown↑、thin depth widen、sync degrade |

注意：参数名本身不决定攻防属性。比如 `max_spread_bps` / `dynamic_cap_*` 到底是进攻还是防守，要看它在当前 baseline trace 里是否真的改变了 `final_spread`、cap-hit、fills 和 exposure。参数注册表只能存「预期方向」；正式报告必须用 replay/live summary 输出的实际机制变化校验方向。

---

## 4. 核心推论：不对称验证 gate

收益/成本可见性是镜像结构——**进攻的收益立即可见、成本在尾部；防守正好相反**。两类改动天然骗过不同的指标，验证标准必须不对称：

### 进攻型 arm 的 gate

- raw PnL / fills 变好是它「应该」发生的，**不构成证据**。
- 必须证明尾部没变差：side markout 不恶化、tail campaign 不增加、inventory time 不膨胀、bad campaign rate 不上升。
- 样本要求高（尾部事件稀疏）。

### 防守型 arm 的 gate

- fills/day 下降是它「应该」付出的，**不构成否决**。
- 必须证明误杀可控：positive false-block rate 低、fills retention 在阈值内、且尾部指标真的改善。
- 最大陷阱：「少成交被误读成更聪明」。

### mixed arm 必须拆开测

一攻一守的组合（如「窄 spread + 短 TTL」）pooled 结果无法归因，单因子阶段应按 direction 分层。

### 落地方式

在 `models/parameter_selection.py` 参数注册表给每个 knob 加 `direction: offensive / defensive / mixed / neutral` 标签；`parameter_racing_sweep.py` 按标签切换 `constraint_first_score` 的 gate 权重：

- 进攻臂加重 tail / markout / inventory-time 惩罚；
- 防守臂加重 fills-retention / false-block 惩罚。

---

## 5. 第三类机制：repricing（re-center）

攻防二分法漏掉的一类：**平移 fair value，不改 spread 宽度**。

```text
ref 上行（未被本地完全吸收）:
  ask 上移 → 不把便宜货卖给知情买方   （防守分量）
  bid 上移 → 顺着重定价方向追成交     （进攻分量）
```

性质：

- 目标是对 fills/day 近似中性（一侧变难成交、一侧变容易），改变的是 **fill selection**：同样数量的成交换成 markout 更好的那批。
- **连续小偏移作用于全部报价分母**，效果按天累积，不依赖稀有状态桶——统计可测性远高于二值稀有 gate。
- 按第 4 节口径属于 mixed arm，两组 gate 同时挂。

如果实测 fills/day、BUY/SELL split 或 action mix 大幅漂移，说明该 arm 已经不再是纯 re-center，而是隐性 spread / lifecycle / side-gate，需要拆开归因。

---

## 6. 案例：BTCUSDT ref 为什么不能当纯防守 arm

> **2026-07-11 口径更新**：本节的 `1s pending` 与 C1-C4 是历史机制假设，不是当前可晋级的 maker policy。后续 retained111 复核确认，右边界可见的 1 秒聚合把领先、同步和滞后压在同一桶中，只能做 second-scale diagnostic。当前研究入口已改为 receive-time WebSocket BBO/trades、事件驱动 flow/depletion state，以及 10/25/50/100/250/500ms fill-toxicity label。除非这条新链路先过 Stage 0，本节 C1-C4 不进入 replay arm 或 live arm。

### 6.1 稀有状态 gate 的统计结构问题

修复前的 `SELL 1s adverse_leading` 样本数、markout 差与 gate 结论已经删除。旧的右边界一秒聚合不能区分领先、同步和滞后，也不能为 event-cancel 提供 receive-time 动作证据。若重新研究稀有状态 gate，必须从当前 causal identity 报告支持度、false-positive denominator、latency survival 与 action uplift，不能继承旧方向。

### 6.2 ref 的正确机制形态（按证据链短→长排序）

1. **信号定义：只用未被吸收的残差，不用原始 ref return**

   $$
   \mathrm{pending}_t = \Delta p^{\mathrm{ref}}_{[t-w,t]} - \Delta p^{\mathrm{local}}_{[t-w,t]},
   \quad w \approx 1s
   $$

   这是待验证的 residual 定义，不继承修复前 `cv_ref_ret` A/B 的正负结论。

2. **fill 之后的用法（样本效率最高，证据链最短）**：ref 状态作为 campaign 风险调节器——adverse_leading 状态下成交的仓位进入更保守的 campaign 管理（加仓侧 cooldown 乘数、更早 stop-add、reduce 侧 skew 更强）。只作用于已发生的 fill，无 false-cancel 问题，直接接 `campaign_outcome_risk_score` 框架。

3. **与现有 guard 合取（待验证是否降低 false positive）**：ref confirm 作为 adverse markout pause / thin-depth widen 的强度乘数，本地信号 + ref 确认才全额触发。它是否优于独立 gate 必须由新的 randomized action panel 判断。

4. **主机制：clamped reservation shift + 侧向不对称**

   ```python
   shift = clamp(beta * pending, -cap_ticks, +cap_ticks)   # cap 1-3 tick
   reservation += shift
   # 可选二阶：pending 与库存同向恶化时，加仓侧再多让 1 tick
   ```

   连续、有界、随 pending 衰减自动归零；不 pause、不 cancel、不动 TTL，队列价值保留。

5. **梯度化曝光调节（替代 event-cancel）**：ref jump intensity 高的窗口，暴露侧 `replace_min_price_change_ticks` 20→10、TTL 缩短；安静时恢复。付出部分队列价值，不是全部 fill。

6. **进攻假设**：`reference_confirmed_absorbed` 表示本地被打、ref 未确认、盘口回填。可研究的机制形态是 **ref-unconfirmed 的本地下探 → 保持订单或避免额外 widen**。是否存在正 markout、是否有足够支持度以及采取哪种动作，都必须重新验证。

### 6.3 验证口径（repricing 作为 mixed arm）

- fills/day 与 action mix 相对 baseline 近似不变（机制量 gate，证明不是隐性防守）；
- SELL 侧 adverse 日 30s markout 改善、campaign terminal PnL 不差（防守分量收益）；
- bid 侧顺向成交 markout 不恶化、inventory time 不膨胀（进攻分量没变成追涨杀跌）。

### 6.4 工程路径

- live 热路径已有 BTCUSDT bookTicker 订阅（1s cross bar），reservation nudge 可在现有 requote 周期内实现；
- replay 侧需要 ref 价格的 trade 粒度对齐数组（已加载 BTCUSDT aggTrades，parity 要求远低于毫秒级 event-cancel）；
- **第一步不动报价**：shadow 记录 `pending` 分布及其对后续 1s/5s markout 的排序力；若 pending 大部分时间为零、非零时能排序 markout，再开 1-2 tick cap 的最小 arm。

---

## 7. BTCUSDT ref 机制实现规格

本节把 6.2 的形态建议落成可实现的规格。整个机制由四个组件构成，共享同一个信号层，按证据链短→长分阶段启用：

```text
                ┌──────────────────────────────────────┐
                │  信号层: pending 残差 (component 0)   │
                └──────┬───────────┬───────────┬───────┘
                       │           │           │
        ┌──────────────▼──┐  ┌─────▼───────┐  ┌▼──────────────────┐
        │ C1 post-fill    │  │ C2 guard    │  │ C3 repricing      │
        │ campaign 调节    │  │ 合取乘数     │  │ reservation shift │
        │ (fill 之后)      │  │ (防守强度)   │  │ (mixed, 全分母)    │
        └─────────────────┘  └─────────────┘  └───────────────────┘
                       ┌───────────────────────┐
                       │ C4 absorbed 进攻桶      │
                       │ (ref-unconfirmed 回填)  │
                       └───────────────────────┘
```

### 7.1 Component 0：信号层（所有组件共享）

**定义**：以 1s 窗口计算「ref 已动、本地未跟」的残差：

```python
# live: SignalEngine 内，BTCUSDT bookTicker mid 与本地 mid 同窗口
ref_ret   = (ref_mid_t  - ref_mid_{t-w})  / ref_mid_{t-w}     # w = 1s
local_ret = (local_mid_t - local_mid_{t-w}) / local_mid_{t-w}
pending_raw = ref_ret - local_ret                              # 单位: return

# 归一到 tick 便于 clamp / 解释
pending_ticks = pending_raw * local_mid / tick_size
```

**必须有的三个卫生处理**：

1. **staleness guard**：ref bookTicker 数据年龄超过 `xref_staleness_ms`（建议 1500ms）时 `pending = 0`。ref 断流时机制自动失效为 baseline，不允许用旧 ref 价格产生偏移。
2. **wall-clock decay**：`pending_ema *= exp(-dt / xref_decay_tau_s)`（建议 tau 2-5s）。传导完成后偏移自动归零，不留常驻 skew。
3. **basis 去均值**：USDT/USDC 有慢变 basis，`pending` 必须相对 basis EMA（分钟级半衰期）计算，否则 basis 漂移会被误读成常驻 lead。

**配置字段**（`live/config.yaml` 新增 `xref` 段，全部默认关闭）：

```yaml
xref:
  enabled: false               # 总开关；关闭时四个组件全部失效
  window_s: 1.0                # pending 计算窗口
  staleness_ms: 1500           # ref 数据年龄上限
  decay_tau_s: 3.0             # pending EMA 衰减
  basis_ema_halflife_s: 300.0  # basis 去均值半衰期
  shadow_only: true            # true 时只记日志，不改任何报价/policy
```

### 7.2 Component 1：post-fill campaign 调节（第一个启用）

**触发**：fill 发生时读取当时的 `pending_ticks` 与方向，写入 fill/campaign 记录。

**动作**（只作用于已成交后的管理，无 false-cancel 问题）：

```python
# fill 时 pending 与仓位方向同向恶化 (SELL fill 且 ref 仍在上行, 或镜像)
fill_ref_adverse = (side == "SELL" and pending_ticks > +thr) or \
                   (side == "BUY"  and pending_ticks < -thr)

if fill_ref_adverse:
    campaign.risk_score += xref_campaign_risk_weight
    # 派生动作走现有 campaign 框架，不新建通道:
    #   - 加仓侧 fill_cooldown 乘数 (如 1.5x)
    #   - campaign stop-add 阈值提前 (age/inventory 门槛乘 0.75)
    #   - reduce 侧库存 skew 增强
```

**接入点**：`campaign_outcome_risk_score` 框架 + `strategy/maker_engine.py` 的 fill 回调；replay 侧接 `models/audit/metrics.py` 的 campaign label。

**gate（防守型口径）**：fills retention 近似不变（它不挡 fill）、tail campaign rate 下降、positive false-block 不适用（无 block）。

**2026-07-06 Stage 1 实现状态**：

- `models/audit/metrics.py` 已把 `fill_exec_spot_pending_*` / `fill_ref_spot_pending_*` 接入 post-fill score：`post_fill_spot_pending_risk_score` 与 `post_fill_campaign_outcome_risk_score`。
- 这些 score 只在 `filled=1` 的 order rows 上有值；未成交订单保持 `missing`，避免把 post-fill 信息误当成 quote-time denominator。
- 原有 `campaign_outcome_risk_score` 保持 quote-time-only，不混入成交后 spot residual。
- 修复前 retained panel 的 bucket、markout、terminal PnL 与 daily-sign 数值已经删除，不能再声称该 score 已有 campaign-risk 排序力。
- 当前只保留 schema 与 causal feature 接点。若重启该 family，必须重新生成 fill-time panel，并用已知 propensity 的 campaign action 评估，而不是从旧 bucket 直接映射 quote-time re-center、pre-fill gate 或 live 开关。

### 7.3 Component 2：guard 合取乘数

**动作**：ref 确认作为现有防守 guard 的强度调制，不新建 gate：

```python
ref_confirm = abs(pending_ema_ticks) >= xref_confirm_ticks   # 建议 1-2 tick

# adverse markout pause: 本地信号 + ref 确认 → 全额; 仅本地 → 半额
effective_pause_s = base_pause_s * (1.0 if ref_confirm else 0.5)
# thin-depth / defense widen 同理: mult = 1 + (mult-1) * (1.0 or 0.5)
```

**关键约束**：合取只能**放大**已有触发或**缩小**未确认触发，不允许 ref 单独触发任何 pause/widen——保证 ref 断流时行为退回 baseline 附近。

**gate**：相对纯本地 guard，false-block（被挡的正 markout fill）下降、tail capture 不变差。

### 7.4 Component 3：pending repricing（主机制，mixed arm）

**动作**：clamped reservation shift，进入 quote core 的 fair price 组装：

```python
shift_ticks = clamp(xref_beta * pending_ema_ticks,
                    -xref_cap_ticks, +xref_cap_ticks)   # cap 1-3 tick 起步
reservation += shift_ticks * tick_size
# 可选二阶 (默认关): pending 与库存同向恶化时加仓侧再让 1 tick
if xref_inventory_interaction and pending_against_inventory:
    exposure_increasing_side_extra_ticks = 1
```

**接入点**：
- live：`strategy/quote_core.py` 的 fair/reservation 组装处，作为 $\Delta_{\text{cross-market}}$ 项（blog 1.2 节公式里已有这个位置，当前为空）；
- 所有下游 guard、cap、post-policy spread cap 顺序不变——repricing 在最上游，防守 guard 仍可覆盖它。

**gate（mixed 口径，两组同时挂）**：
- 机制量：fills/day、action mix、BUY/SELL split 相对 baseline 漂移 < 10%；
- 防守分量：adverse_leading 日的 SELL 侧 30s markout 改善；
- 进攻分量：顺向成交 markout 不恶化、abs inventory time 不膨胀。

### 7.5 Component 4：absorbed 进攻桶（最后启用）

**状态定义**：本地出现下探/冲击，且 ref **未确认**（`|pending| < thr` 且 ref 自身短窗 ret 不同向）——即 `reference_confirmed_absorbed` 的 quote-time 版本。

**动作**：进攻型、只作用于回补侧：

```python
if local_dip and not ref_confirm and near_depth_refill_ok:
    bid_policy.spread_mult = min(bid_policy.spread_mult, xref_absorb_tighten)  # 如 0.9
    bid_policy.order_ttl_ms = max(bid_policy.order_ttl_ms, extended_ttl)
```

**gate（进攻型口径）**：该桶 fill 的 30s markout ≥ neutral 桶、campaign terminal PnL 不差、tail 不增加；fills 增加本身不构成证据。

### 7.6 Replay parity 要求

旧的「增加 ref 1s mid 数组」已不再是正式路径。2026-07-12 起，Python tick replay 使用统一的 `HistoricalReferenceEvent` 与 feature-ready k-way scheduler：

1. 每个外部事件同时保留 exchange、local receive、feature-ready 时间；
2. quote/fill/order 状态在时刻 `t` 之前只消费 `feature_ready_ts_ns <= t` 的事件；
3. 同一个 `GlobalFlowEngine` 服务 fill-toxicity audit 与 policy replay；
4. 独立 `MultiMarketPolicy` 默认 no-op，位于 quote core 之后，只输出 add-side allow/veto，不把外部字段散落进 quote core；
5. 首个动作 `post_fill_stop_add` 使用 side-specific causal repair model，在每个 arm 自己的库存/campaign 路径上在线重算；缺失时 fail-fast，不允许把 baseline 序列或 terminal campaign 字段代替 quote-time score；
6. C++ replay 尚未支持该输入，显式请求时 fail-fast，正式机制研究继续以 Python replay 为权威。

严格 freshness v2 进一步要求 `consensus source age + right-edge cursor age` 仍在预算内。旧 stop-add paired replay 的精确结果建立在已废弃的回放时钟、特征可见性和历史 baseline 上，已从公开证据面删除。当前只保留方法边界：causal repair ranking 不等于 stop-add action uplift，任何后续动作必须在当前 corrected baseline 上用已知 propensity 的 randomized panel 重新识别。

### 7.7 分阶段晋级路径

下列旧路径已冻结，仅保留为历史设计记录。它不能再从 1 秒历史桶直接晋级：

```text
Legacy Stage 0  one-second shadow (frozen)
         记录: pending 分布、非零占比、staleness 率、
               pending@fill 对 1s/5s/30s markout 的分桶排序力
         通过条件: 非零时段 markout 单调排序、staleness < 5%
              │
Stage 1  C1 post-fill campaign 调节 (证据链最短, 无 false-cancel)
         gate: tail campaign ↓, fills retention ~100%
              │
Stage 2  C2 guard 合取 (降既有防守的误杀)
         gate: false-block ↓, tail capture 持平
              │
Stage 3  C3 repricing 最小 arm (cap=1-2 tick, beta 保守)
         gate: mixed 双组 gate (见 7.4)
              │
Stage 4  C4 absorbed 进攻桶
         gate: 进攻型口径 (见 7.5)
```

替代路径是：`market_tape.v1 -> receive-time GlobalFlowState -> sub-second fill-toxicity evidence -> action uplift -> tiny shadow arm`。每个 stage 独立通过 chronological daily OOS 才进入下一个；旧 1 秒 Stage 0 不计作新路径的通过证据。

### 7.8 独立 venue 证据边界

修复前的 Bitget/Bybit spot-perp Stage 0 数值与排序结论已经删除。那批结果使用旧 feature/replay 身份，不能继续支撑 moderator、re-center 或 policy 判断。独立 venue 仍只提供研究输入；任何新结论都必须基于 receive-time 可见状态、当前因果特征、明确 latency identity 和重新冻结的 action family。

### 7.9 失效保护清单

| 失效场景 | 行为 |
|---|---|
| ref WS 断流 / 重连 | staleness guard → pending=0 → 全组件退回 baseline |
| basis 漂移 | basis EMA 去均值吸收；EMA 未收敛期（启动 <10min）强制 pending=0 |
| pending 常驻非零（套利结构变化） | 监控非零占比 > 20% 报警，人工review 而非自动调 beta |
| clamp 长期打满 | cap hit rate > 5% 报警——说明 beta 过大或市场结构变化 |
| live/replay 口径分叉 | pending 计算共享同一模块；A/B 前跑 parity 对拍 |

---

## 8. 结论

1. spread 轴、曝光时间轴直接可用；skew 轴必须按来源拆成「库存 skew=防守 / 信号 skew=进攻」，均衡与否不是判据。
2. 攻防分类的真正价值不是描述性的，而是导出**不对称验证 gate**，应落地为参数注册表的 direction 标签 + racing score 的差异化权重。
3. 攻防二分之外存在第三类 repricing 机制：连续、作用于全分母、改变 fill selection 而非 fill 数量——统计可测性最好。
4. BTCUSDT ref 的出路不是防守 gate，而是「post-fill campaign 调节 + guard 合取 + pending 残差 repricing + absorbed 进攻桶」的组合，从证据链最短的 post-fill 用法开始。
5. ref 机制的实现按第 7 节规格执行：共享 pending 信号层（staleness/decay/basis 三重卫生处理），C1→C2→C3→C4 分阶段晋级，每阶段独立过 retained daily OOS；Stage 0 shadow 先行，全程默认 `xref.enabled=false`。

## 9. 全项目 arm / bucket 编排规则

后续所有策略研究都按同一张 schema 编排，不再让每个脚本各自发明分类：

### 9.1 Bucket schema

```text
bucket_id:
  state_family: local_liquidity | trend | campaign | xmarket | session | execution
  quote_time_fields: [...]
  side_scope: BUY | SELL | BOTH
  evidence_role: fill_probability | fill_quality | toxic_risk |
                 campaign_risk | repair_signal | moderator
  support: placed_count / fill_count / eligible_days
  leakage_status: quote_time_only | blocked
```

bucket 只回答「这个状态在 quote-time 是否有解释力」，不直接回答「应该怎么改报价」。

### 9.2 Arm schema

```text
arm_id:
  source_buckets_or_scores: [...]
  control_axis: re-center | spread | skew | lifecycle | size | safety
  direction: offensive | defensive | mixed | neutral
  expected_mechanism_delta:
    fills/day:
    action_mix:
    side_vwap_edge:
    inventory_time:
  hard_gates:
    ...
  outcome_gates:
    ...
```

### 9.3 Promotion rule

- bucket 通过，只能进入 shadow score 或 tiny arm 设计；
- arm 通过机制 gate，才能看 outcome；
- outcome 通过，才能 live shadow；
- live shadow 通过，才讨论 live policy；
- direct `pause/cancel/veto` 只能作为 safety arm，不作为 alpha 起点。
