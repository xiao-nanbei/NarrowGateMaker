# Binance trade/lifecycle contract v2

## 结论

本项目不再等待一个 Binance USD-M 官方并未公开支持的 futures individual receive-time stream。正确口径是两个时钟、三种数据身份：

| 数据 | 角色 | 可用于策略特征 |
|---|---|---|
| Binance Vision `trades` | individual exchange-time 撮合、queue 消耗与 outcome truth | 否，不能假装成 receive-time 信息 |
| Binance Vision `aggTrades` | individual `f..l` 到 live 消息的历史 parent | 仅作为 lineage parent |
| Binance USD-M public `aggTrade` WebSocket | live 策略真正可见的 trade flow，记录 receive/feature-ready | 是 |

Binance 官方 USD-M 市场流目录公开支持 Aggregate Trade Stream，没有声明 spot API 中的 raw Trade Stream：[USD-M Futures Aggregate Trade Streams](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/market)。

因此 historical replay 的因果规则冻结为：

```text
individual trade exchange_ts
    -> 只更新撮合、queue consumption 和 outcome

mapped parent aggTrade feature_ready_ts
    -> 才允许更新策略可见 taker-flow feature
```

不能从 individual exchange timestamp 提前生成 10/25/50/100ms policy feature。

## Individual to aggTrade mapping

新增 `models/audit/binance_trade_mapping.py`，按 aggregate 的 `f..l` trade-ID range 映射 individual trades，并验证：

- price 与 aggressor side；
- `q` total quantity，或存在 RPI 时的 `nq` normal quantity；
- aggregate timestamp 与 child timestamp span；
- feature-ready 不早于最后一个 child；
- queue outcome 是否具备连续 trade-ID coverage。

2026-07-20 BTCUSDC 全日结果：

| 项目 | 数值 |
|---|---:|
| individual rows | 986,452 |
| aggregate rows | 480,479 |
| mapped rows | 986,452 |
| exact `f..l` aggregate ranges | 480,435 |
| internal-ID-gap ranges | 44 |
| exact aggregate ratio | 99.990842% |

44 个区间中，现有 child 的价格、方向和数量仍与 aggregate 完全守恒，但 ID range 内部缺行。它们可保留为 aggregate feature timing，必须标记 `queue_outcome_exact=false`，不能用于 strict queue outcome。全日 strict queue gate 因此为 false；这不会错误地丢弃其余 480,435 个精确区间。

历史 `aggTrade.transact_time` 在真实文件中可能接近第一个 child，而不是最后一个 child。没有显式 receive-time 时，replay 的可见下界必须使用：

```text
max(parent transact_time, last child transact_time) + frozen latency
```

且必须记录环境标签和 latency profile id；零延迟、空 profile 只允许 diagnostics。

## Historical/live aggregate parity v3

`models/audit/historical_live_aggtrade_parity.py` 不要求 individual receive stream，而是将同窗 live `aggTrade` 与 Vision/REST canonical parent 按 aggregate ID 对齐。

AWS Tokyo 2vCPU/4GiB capture `20260721T094114Z` 与 2026-07-21 Vision 全日文件的审计结果：

| 项目 | 数值 |
|---|---:|
| live BTCUSDC aggregates | 10,367 |
| historical matches | 10,367 / 10,367 |
| price / quantity / aggressor side | 100% / 100% / 100% |
| exchange timestamp / aggregate sequence | 100% / 100% |
| transport lag p50 / p95 | 93.08ms / 932.78ms |
| feature latency p50 / p95 | 99.74us / 5.291ms |

该 capture 来自 recorder v1，原始 WebSocket 消息虽是 `aggTrade`，落盘时没有保存 `f/l`、typed schema 和 source-contract。因此基础市场字段全部通过，但正式状态仍为 `blocked`：

```text
failed_gates = [complete_f_l_lineage, supported_live_source_contract]
```

recorder 随后升级为 `binance_usdm_aggtrade.v2`，新增 aggregate ID、`f/l`、range-derived child count、`q/nq` 与 source-contract。2026-07-23 在同一 AWS Tokyo 实例完成了 600 秒 capture `20260723T010630Z`：7 个 gzip、351,106 个事件、零 recorder drop/invalid、策略哈希不变，其中 BTCUSDC live aggregates 为 2,421 条。

同日用官方 `GET /fapi/v1/aggTrades` 按 aggregate ID 下载 2,421 条 canonical parent。recorder contract 的全部 gate 通过：ID、价格、方向、trade timestamp、首个 child、source-contract、receive/feature-ready 因果时钟均一致，且每个 live slice 都是 canonical parent 的合法前缀。

但 exact historical payload replay 单独 blocked：2,420/2,421 条 `q/f/l` 完全相等，aggregate `278461494` 的 live slice 为 child `541603974..978`、`q=0.049`；REST 最终 parent 扩展到 `541603974..982`、`q=0.265`。`historicalTrades` 证明新增四笔均为同价、同方向、非 RPI 的正常成交，最晚发生在首笔后 95ms。因此它是实时可见 slice 与数据库 finalized parent 的身份差异，不是 recorder 字段映射错误。

正式结果冻结为：

| Gate | 结果 |
|---|---:|
| recorder/live source contract | passed |
| matched aggregate IDs | 2,421 / 2,421 |
| price / side / timestamp / first child | 100% |
| canonical-parent prefix compatibility | 100% |
| exact quantity and complete `f..l` | 99.958695% |
| canonical parent extensions | 1 / 2,421 (0.041305%) |

旧报告中的 `transport lag` 是 `local_receive - trade T`，包含 Binance 聚合/发布等待，不能解释成纯网络延迟。本窗应称为 trade-to-receive lag：p50 109.64ms、p95 1,293.12ms；feature latency p50 106.32us、p95 3.808ms。

live receive tape 中允许研究的字段包括：

- aggregate message count、quantity、quote notional、side 和 price path；
- aggregate receive/feature-ready interarrival 与 side run；
- `f..l` 推导的 child count，且整块只能在 parent ready 时一次性可见。

仅使用 Vision/REST finalized parent 时，`aggregate_quantity`、quote notional 和 `f..l` child count 不能再宣称精确复刻 live；它们需要同窗 receive tape，或必须在 historical replay 中 fail closed。以下字段永久是 diagnostic-only：individual receive timestamp、aggregate 块内 child interarrival、child receive order 和 child feature-ready time。

## Complete order lifecycle

新增 `models/audit/order_lifecycle.py` 与 `models/audit/event_identity_and_riskset_v2.py`，Python authoritative replay 现在可输出：

- submit、activation、GTX reject；
- cancel request 与 cancel ACK；
- ACK 前 partial/full fill、remaining quantity 和 pending-cancel fill quantity；
- native snapshot/delta price jump，作为非吸收 state transition；
- campaign repair delayed entry/exit；
- order/day/campaign censoring；
- 按 event sequence 构造的 start-stop risk intervals。

同毫秒、跨 trade 与 native-book 且没有共同 sequence 的事件保留为 diagnostic，排除出 formal fill-hazard risk rows。Repair 只有在 inventory 非零、campaign active、reducing quote active 且 eligible 时才进入风险集。

`local_order_value_replay.py` 会额外生成：

```text
*.lifecycle.parquet
*.risk_intervals.parquet
*.event_identity_and_riskset_v2.json
```

旧 partial cache 若缺 lifecycle artifact 不再允许复用。

## 当前 gate

本轮没有创建 hazard、action 或 live 策略改动。当前顺序是：

1. recorder v2 的 live source contract parity 已完成；
2. 在 frozen Development 上生成完整 lifecycle v2 panel 并通过身份审计；
3. 同 denominator chronological 复核只能使用 exact-safe 字段；quantity/count 需要 receive tape 或显式 source-extension sensitivity；
4. 复核通过后才允许登记窄化的 dynamic fill prediction experiment；
5. prediction 仍不能直接创建 action，动作必须另做带 propensity 的 randomized replay。

Bybit public individual trades 可在后续作为 external M1 信息源；它不能替代 Binance execution-market M0 的 receive-time 合同。
