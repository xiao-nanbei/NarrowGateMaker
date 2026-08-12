# live_taker_flow_parity_v1

> **Superseded boundary (2026-07-23):** v1 正确阻止了用历史 exchange-time individual trades 冒充 receive-time 数据，但“必须取得 futures individual receive stream”不是必要条件。当前契约改为 Vision individual outcome truth + mapped aggTrade visibility clock；见 [`binance_trade_lifecycle_contract_v2_20260723.md`](binance_trade_lifecycle_contract_v2_20260723.md)。本文保留为旧 capture schema 的审计记录。

## 结论

`live_taker_flow_parity_v1` 已冻结并 **blocked**。当前不能声称 Binance USD-M BTCUSDC futures 的 individual trade 与 live `aggTrade` 已完成同窗 receive-time / feature-ready parity。

截至 2026-07-23，Binance 官方 USD-M public WebSocket 目录公开列出 Aggregate Trade Stream，但没有列出 spot API 中那种 raw Trade Stream。不能因为 spot 支持 `@trade`，就推断 futures 也支持未文档化的 `@trade`。当前实现继续只订阅官方 `aggTrade`；任何 individual receive-time 数据必须带一个独立、受支持且冻结的 source identity。

官方目录：[USD-M Futures public WebSocket streams](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public)

## 当前 capture 审计

输入为 AWS Tokyo 2vCPU/4GiB capture `20260721T094114Z` 的 Binance tape。

| 项目 | 数值 |
|---|---:|
| BTCUSDC trade rows | 10,367 |
| typed aggregate rows | 0 |
| typed individual rows | 0 |
| legacy untyped rows | 10,367 |

旧 tape 的 trade 行来自 `aggTrade`，但没有保存 `trade_stream_type`、aggregate id 与 `f/l` individual trade ID range，因此它不能事后完成严格 lineage 匹配。历史 Binance Vision individual trades 又只有 exchange timestamp，没有同一 EC2 collector 的 receive/feature-ready timestamp，也不能替代这个 gate。

从本次改动开始，新 capture 会为 `aggTrade` 额外记录：

```text
trade_stream_type=aggregate
trade_payload_schema_version=binance_usdm_aggtrade.v2
trade_source_contract_id=binance_usdm_public_aggtrade_receive_time.v1
aggregate_trade_id
first_trade_id
last_trade_id
exchange_event_ts_ns
local_receive_ts_ns
feature_ready_ts_ns
```

这只补齐 aggregate lineage，不会伪造 individual stream。

## 当时的 Parity gate

v1 原计划等待 individual receive-time source 并按 `f..l` range 比较。该等待条件现已被 v2 的双时钟映射取代；以下列表只描述冻结的旧审计：

- child ID coverage；
- quantity 与 aggressor-side parity；
- exchange/receive/feature-ready causal timestamp；
- 100ms feature-ready bucket 是否一致；
- aggregate 相对最后一笔 individual event 的可见延迟。

当前结果：

```text
live_taker_flow_parity_passed = false
dynamic_fill_hazard_allowed = false
action_family_allowed = false
```

后续不订阅未文档化的 futures `@trade`。live 特征必须能够从官方 `aggTrade` 消息复现；历史 individual trades 只在 exchange-time 撮合和 queue outcome 中使用，其 policy visibility 统一延迟到 mapped parent 的 feature-ready clock。
