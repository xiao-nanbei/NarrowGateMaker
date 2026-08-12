# 全球 BTC 市场 Reference 研究评审备忘录 - 2026-07-11

> **2026-07-27 证据修订。** 多 venue、spot/perp 分层、2-of-3 与 leave-one-out 架构仍可复用；本文依赖旧本地 BTCUSDC 成交/markout/campaign denominator 的精确数值已经撤销。当前只能保留“Stage 0 未授权 action”这一治理结论，不能把旧阴性结果解释为 external reference 没有价值。任何后继实验必须在 normalized 100ms/native queue、修复后的 trade side 与当前量纲合同下重建本地 outcome。

## 1. 希望评审的问题

NarrowGate 在 Binance BTCUSDC perpetual 上执行被动做市。当前研究问题不是“某个外部交易所是否永远领先 Binance”，而是：

> 在 quote time，能否利用多个独立市场区分全球 BTC 公允价格重定价、Binance 局部流动性冲击、衍生品清算冲击和 USDT/USDC 币种基差，从而改善被动成交的 maker-signed markout 与库存 campaign 终局？

本文记录当前已经完成的历史回测、没有通过的证据门槛、实现限制和下一步候选。希望评审者重点检查：因果时点、统计设计、reference 结构、目标函数以及是否存在被当前 Stage 0 误杀的合理机制。

后续工程与研究的 canonical phase/gate 定义见 `docs/cross_venue_reference_to_alpha_roadmap_20260711.md`。

## 2. 当前市场结构

### 2.1 执行与本地桥接

| 市场 | 角色 |
| --- | --- |
| Binance BTCUSDC perpetual | 唯一 execution market |
| Binance BTCUSDT perpetual | 同 venue 的高速价格 level bridge，不算外部投票 |
| Binance `USDCUSDT` spot | USDT 到 USDC 的币种换算锚 |
| Binance BTCUSDC spot | bridge cross-check 和 fallback |

Binance API 的正式稳定币 symbol 是 `USDCUSDT`，含义是一单位 USDC 对应多少 USDT，因此币种换算为：

```text
local_bridge_px_usdc = Binance_BTCUSDT_perp / Binance_USDCUSDT_spot
```

### 2.2 独立外部信息市场

Bitget、Bybit、OKX 各自提供 BTCUSDT spot 与 perpetual，共六条独立市场 tape。spot 和 perpetual 是两个因子，不是六张相互独立的选票；同一家 venue 的 spot/perp 共享 venue、网络和部分订单流风险。

外部行情当前只用于 shadow evidence。它们没有账户或下单接口，不修改 live quote、size、inventory limit、cancel/replace 或 lifecycle。

### 2.3 Public WebSocket 与认证边界

2026-07-11 已按官方文档并在 EC2 上用**零 API key**实连验证：

| venue | 无认证逐笔/成交流 | 无认证最小盘口或普通深度 | 认证例外 |
| --- | --- | --- | --- |
| Bitget | v3 `publicTrade`，real-time | `books1` 1ms snapshot；`books` 50ms incremental | private/account/order 才需登录 |
| Bybit | `publicTrade.{symbol}`，real-time | `orderbook.1` 10ms snapshot；level 50 为 20ms snapshot/delta | private/order-entry 才需登录 |
| OKX | public `trades`；`trades-all` 在 business WS | `bbo-tbt` 10ms snapshot；public `books` 100ms incremental | 10ms `books-l2-tbt`/`books50-l2-tbt` 需要登录且受 VIP5/VIP4 限制 |

EC2 preflight 分别收到 Bitget `books1 + publicTrade`、Bybit `orderbook.1 + publicTrade`、OKX `bbo-tbt + trades`。因此 NarrowGate 不保存 Bitget/Bybit/OKX API key；当前 receive-time reference 所需信息全部来自公开频道。官方规格见 [Bitget depth](https://www.bitget.com/api-doc/uta/websocket/public/Order-Book-Channel)、[Bitget public trades](https://www.bitget.com/api-doc/uta/websocket/public/New-Trades-Channel)、[Bybit connect](https://bybit-exchange.github.io/docs/v5/ws/connect)、[Bybit orderbook](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook)、[Bybit public trades](https://bybit-exchange.github.io/docs/v5/websocket/public/trade) 与 [OKX market-data guidance](https://www.okx.com/docs-v5/trick_en/)。

## 3. 历史数据边界

所有输入严格对齐同一份 111 个 retained UTC good days，不允许 reference 日期集合宽于 BTCUSDC execution universe。

| 数据 | retained days | normalized trades | causal 1s states/bars |
| --- | ---: | ---: | ---: |
| Bitget BTCUSDT perpetual | 111 | 150,243,942 | 7,068,971 |
| Bybit BTCUSDT perpetual | 111 | 252,387,353 | 7,276,709 |
| OKX BTCUSDT perpetual | 111 | 399,947,490 | 8,742,874 |
| Bitget BTCUSDT spot | 111 | 40,287,185 | 5,315,761 |
| Bybit BTCUSDT spot | 111 | 106,016,009 | 5,417,506 |
| OKX BTCUSDT spot | 111 | 69,915,914 | 5,846,509 |
| Binance USDCUSDT spot | 111 | 46,776,513 | 8,792,656 |

三 venue 聚合后：

- spot consensus: 7,747,571 states；
- perpetual consensus: 9,225,272 states；
- spot/perpetual fresh join: 7,547,083 states；
- hierarchical reference: 6,247,083 states；
- strict-valid reference: 4,659,028 states，约 74.58%。

历史外部 venue 输入主要是 trades，不是同一采集机记录的 exact BBO/L2 receive-time tape。每个 `[t, t+1s)` 内的事件只在右边界 `t+1s` 可见，禁止 future backfill；上一价格最多在 2 秒 freshness 内因果 carry-forward。因此本次实验能研究秒级排序，但不能证明 50ms/100ms event-cancel alpha。

## 4. Reference 定义

### 4.1 外部 spot/perpetual common innovation

对每个 venue 的 causal 1 秒 log return：

```text
r_v,t = log(mid_v,t / mid_v,t-1)
```

分别构造：

```text
G_spot,t = median(r_bitget_spot, r_bybit_spot, r_okx_spot)
G_perp,t = median(r_bitget_perp, r_bybit_perp, r_okx_perp)
```

规则为：

- 至少 2/3 venue fresh；
- 三个 fresh venue 使用中位数；两个 fresh venue 使用均值并降低 confidence；
- consensus 先在 return 横截面生成，再积分为 common-factor price；
- 禁止先取不同 venue 的价格 level 中位数再求 return，因为成员变化与长期 venue basis 会制造假 move；
- 同时输出 agreement、return dispersion、fresh venue count、outlier venue 和 leave-one-venue-out 状态。

### 4.2 本地 bridge、慢基差与未吸收残差

```text
bridge_t = Binance_BTCUSDT_perp_t / Binance_USDCUSDT_spot_t

basis_t = causal_rolling_median(
    log(Binance_BTCUSDC_perp / bridge) * 10000,
    window=360s,
    min_periods=30
).shift(1)

global_move_t = 0.5 * (G_spot,t + G_perp,t)
unabsorbed_t = global_move_t - Binance_BTCUSDT_perp_move_t

correction_t = clip(
    unabsorbed_t * consensus_confidence_t,
    -1 execution tick,
    +1 execution tick
)

ref_px_t = bridge_t * exp((basis_t + correction_t) / 10000)
residual_t = log(ref_px_t / Binance_BTCUSDC_perp_t) * 10000
```

strict-valid 还要求：

- spot 至少 2/3 fresh；
- perpetual 至少 2/3 fresh；
- spot/perpetual 同方向，或者两者都小于 0.05 bps；
- 两层最大 return dispersion 不超过 2 bps；
- Binance bridge、execution price 和滞后 slow basis 均有效。

`correction_t` 在本轮只生成 shadow state，并未真正移动 replay/live quote。

## 5. Stage 0 评估设计

每笔 placed order 都保留在 denominator 中，不只观察 fills。状态分别在 submit time 和 fill time 采样，horizon 为 1s/3s/5s；评价 label 包括：

- fill outcome 与 fill rate；
- 1s/5s/20s/30s maker-signed markout；
- campaign terminal PnL；
- campaign repair flag；
- campaign tail 与 adverse excursion；
- local absorption/reversion interaction；
- spot/perpetual cross-instrument state；
- full three-venue 与 leave-Bitget/Bybit/OKX-out 三个反证版本。

Stage 0 只回答 quote/fill-time state 有没有稳定排序力，不做 fill counterfactual，也不改变 queue、latency、cancel、库存路径或 PnL。

## 6. 主要结果

### 6.1 Hierarchical residual 没有通过 Stage 0

full 与三个 leave-one-venue-out 中，唯一同时保持 30s markout 和 campaign terminal 方向为正的 global-residual 行是 `SELL submit 1s`：

| variant | support days | adverse/neutral/favorable fills | F-A 1s | F-A 5s | F-A 20s | F-A 30s | 30s positive days | campaign F-A |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| full | 94 | 5,663 / 7,883 / 5,528 | -0.011 | -0.104 | +0.112 | +0.097 | 51/94 | +0.008 |
| leave Bitget out | 92 | 5,322 / 7,137 / 5,192 | -0.023 | -0.123 | +0.086 | +0.082 | 55/92 | +0.004 |
| leave Bybit out | 93 | 5,206 / 6,898 / 5,106 | -0.007 | -0.090 | +0.092 | +0.075 | 51/93 | +0.011 |
| leave OKX out | 93 | 5,155 / 6,828 / 5,102 | -0.003 | -0.107 | +0.082 | +0.053 | 51/93 | +0.010 |

数值单位为 bps，campaign 列为 favorable-minus-adverse 的平均 terminal PnL。

这个方向不够支持 policy：

- 最接近可执行时点的 1s/5s 反而为负；
- 20s/30s 改善只有约 0.05-0.10 bps；
- 30s 日度同号接近随机的一半；
- campaign terminal 增量极小；
- 可能是冲击后的慢 reversion clue，不是 quote-time fair-value lead。

### 6.2 `SELL fill + divergent` 是较大的长窗 clue，但不稳定

| variant | days/fills | vs neutral 1s | 5s | 20s | 30s | campaign terminal | repair-rate delta |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| full | 39 / 108 | +0.300 | +0.064 | +0.439 | +1.968 | +0.832 | -0.034 |
| leave Bitget out | 39 / 80 | +0.301 | -0.031 | -0.197 | +0.690 | -0.217 | -0.056 |
| leave Bybit out | 59 / 185 | -0.255 | +0.181 | +1.149 | +2.282 | +0.154 | -0.043 |
| leave OKX out | 53 / 188 | +0.028 | -0.045 | +0.786 | +1.904 | +0.197 | -0.047 |

它不能进入 policy，原因是：

- 样本小，且 leave-one-out 后状态成员与样本量明显变化；
- 1s/5s 方向不稳；
- leave-Bitget-out 的 campaign terminal 反号；
- repair-rate delta 四版都为负；
- 这是 fill-time attribution，不能直接偷渡成 submit-time cancel/re-center。

### 6.3 其他 clue

- `BUY submit + perp_only_up` 在 full 版本有长窗改善，但 leave-Bitget-out 反号；
- `BUY fill + spot_leading_up` 只有约 23-38 fills，样本不足；
- `SELL fill + perp_only_up` 有部分 30s 改善，但短窗、campaign 和 repair 不稳定；
- local absorption interaction 没有形成跨 horizon、跨 venue leave-out 的稳定交集。

## 7. 当前结论

### 已经支持

1. 第三个 venue 有实际研究价值，但目前价值主要是 2-of-3、median、outlier rejection、venue failure 隔离与 leave-one-out 反证。
2. spot/perpetual 必须分层。perp-only、spot-leading、confirmed、divergent 的含义不同，不能把六条行情平均成一个价格。
3. Binance BTCUSDT 应作为本地 level bridge，不应算第四个独立 vote。
4. `USDCUSDT` 是必要的币种换算锚，BTCUSDC spot 适合作为 fallback/cross-check。
5. 当前历史秒级 reference 对部分 fill/campaign 结果有解释力，但不足以形成稳定的 quote-time maker alpha。

### 尚未支持

1. 不能启用 direct re-center、cancel、tighten、size、stop-add 或 lifecycle arm。
2. 不能声称 OKX、Bybit 或 Bitget 中任何一家稳定领先 Binance。
3. 不能用历史 1 秒 trades 结果推断 50ms/100ms cancel 可执行性。
4. 不能把 30s 正向 clue 当成 1s quote-time alpha。
5. 不能把 live 六路 connector 正常运行当作策略收益证据。

### 本轮没有证明什么

Stage 0 未通过不等于全球市场数据无用，也不等于 Binance-only 信息充分。它只说明：

> 当前的 1 秒右边界 trades、2-of-3 median、spot/perp 同向 strict gate、slow basis 和 categorical sorting 组合，没有稳定识别可直接映射为 policy 的成交质量增量。

## 8. 最可能的假阴性来源

1. **时间分辨率错位**：跨 venue price discovery 可能只持续几十到几百毫秒，1 秒右边界已经让 Binance 吸收大部分信息。
2. **历史/live 数据层不同**：历史主要是 trades-derived state；live 已采 BBO/trades 与 local receive timestamp。两者不能直接宣称 parity。
3. **spot/perpetual 同向硬门槛过强**：真实 price discovery 可能先发生在 spot 或 perp，等两层确认后 edge 已经消失。
4. **categorical bucket 损失连续信息**：residual magnitude、confidence、dispersion 和 leader survival 可能需要连续 calibration，而不是 adverse/favorable/neutral 三桶。
5. **固定 360 秒 basis**：不同 session、波动和稳定币状态下，basis half-life 可能不同。
6. **目标 horizon 错位**：1s/5s markout、30s markout 和 campaign repair 可能对应不同机制，不应强求单一 state 同时优化全部目标。
7. **2-of-3 成员变化**：两源均值与三源中位数会改变状态定义；需要对 source-set transition 单独审计。
8. **execution latency 未进入 Stage 0**：即使预测成立，也可能活不过外部 feed、feature、decision、gateway 和 exchange ACK 的 p95/p99 总延迟。

## 9. 建议的下一阶段

### P0: 建立同采集机 receive-time tape

对六条外部 source 与 Binance local bridge 同时记录：

```text
exchange_event_ts_ns
local_receive_ts_ns
feature_ready_ts_ns
book sequence/gap
bid/ask/size
trade price/size/aggressor
```

先分析 50/100/250/500ms/1s leader survival，并做 p50/p95/p99 latency stress。只有预测 horizon 稳定长于总执行延迟，才有资格讨论 pre-fill action。

### P1: 连续增量预测，而不是直接建立 arm

使用 chronological walk-forward 和 embargo，比较：

```text
M0 = local Binance exact-L2/flow/queue/campaign state
M1 = M0 + global spot/perp/residual/confidence/dispersion/leader state
```

目标按 side 和 inventory role 分开：

- future local mid return；
- BUY/SELL maker-signed markout；
- opener/add/reducing fill quality；
- tail-before-repair；
- campaign terminal PnL / repair。

只有 M1 在 walk-forward、late holdout、leave-one-venue-out 和 latency stress 下稳定优于 M0，才称为 external information increment。

### P2: 若 P1 通过，再做最小 mixed arm

第一个政策只允许最多 1 tick bounded reservation re-center：

```text
shift = clip(k * residual * confidence, -1 tick, +1 tick)
```

不改变 size、inventory limit、reducing side、global pause 或 hard risk gate。要求 placed/fills、BUY/SELL split、spread 和 action mix 近似不变，同时改善 side markout、campaign terminal 和 tail。

### P3: 单独研究 local shock without global confirmation

该状态可能比“追随外部市场”更适合 maker：Binance local aggressive flow 很强，但三家外部 spot/perp 不确认，且本地 depth refill/microprice recovery 较快。这里 external reference 的作用是避免误杀可吸收 fill，而不是直接提供方向开关。

## 10. 请评审者回答的具体问题

1. 当前 `local bridge + 三 venue spot/perp common innovation + slow basis` 的分层结构是否合理？有没有结构性重复计价或遗漏？
2. 2-of-3 median 是否是第一版最稳健的 common factor，还是应该使用带 venue-specific latency/basis 的 state-space/Kalman/PCA 模型？如何避免后者过拟合？
3. spot/perpetual 必须同向的 strict gate 是否会系统性错过真正的 leader transition？应如何改成连续 confidence，同时保留 outlier protection？
4. 360 秒 causal rolling-median basis 是否合理？更好的 causal basis/half-life 估计是什么？
5. `SELL submit 1s` 的 1s/5s 负、20s/30s 微正更像什么机制：慢 reversion、label noise、policy-selection bias，还是 reference horizon 错配？
6. `SELL fill + divergent` 的 30s/campaign clue 与 repair-rate 下降应如何解释？它是否只是在标记更长、更冒险的库存持有，而不是 alpha？
7. 在历史只有 causal 1s trades、没有统一 receive-time BBO 时，还能做哪些有意义的反证，哪些问题必须等 live tape？
8. 下一步应优先做 global-confirmed local lag，还是 local shock without global confirmation？
9. 如何定义最合适的 action-uplift target，使 re-center、keep、replace、widen 的价值可以在同一框架比较，而不是只预测未来价格？
10. 当前 Stage 0 停止规则是否过严或过松？请给出一套不会因为反复探索而过拟合的
    walk-forward、late-holdout、multiple-testing 与 campaign-tail gate。

## 11. 代码与证据入口

- `market_fusion.py`: venue/instrument/role schema；
- `models/external_consensus_layer.py`: causal 1s consensus、2-of-3、leave-one-out；
- `models/global_reference_layer.py`: hierarchical offline reference；
- `strategy/global_reference.py`: live shadow reference state；
- `models/audit/external_venue_shadow_panel.py`: Stage 0 order/fill/campaign audit；
- `docs/global_reference_stage0_retained111_20260711.md`: canonical short report；
- `${NARROWGATE_RESULTS_DIR}/global_reference_3venue_stage0_retained111_20260711_btcusdc.*`: full outputs；
- corresponding `global_reference_leave_{bitget,bybit,okx}_out_*`: leave-one-out outputs。

## 12. 当前决策

继续采集六路 external receive-time shadow tape；保留 residual、spot/perp divergence、leader、confidence、dispersion 与 stablecoin basis 作为 order/campaign diagnostics。当前不启用任何由全球 reference 驱动的 live quote policy。
