# BUY/SELL 分侧与 Taker Flow 研究边界 (2026-07-23)

Last materially modified: 2026-08-12

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## 结论

BUY 与 SELL 不能默认共享同一套微观结构响应。对 maker 而言，正确的成交对手映射是：

| Maker order | 直接成交对手 |
|---|---|
| BUY maker | aggressive SELL taker |
| SELL maker | aggressive BUY taker |

Binance individual trade 中 `is_buyer_maker=true` 表示 buyer 是 maker，因此该笔主动方是 SELL taker；`false` 表示主动方是 BUY taker。研究字段必须先完成这层映射，再谈 maker-side toxicity。

当前代码并非完全对称，但多数机制只是“状态分侧、参数共享”。这不足以证明 BUY 与 SELL 应使用同一个阈值、衰减速度或动作。

## 当前机制边界

| 机制 | 当前状态 | 下一步分侧方式 |
|---|---|---|
| toxicity heads | 已有 `tox_bid` / `tox_ask` | 按 maker side 与 opener/add/reducing 分别校准 |
| markout feedback | bid/ask EMA 状态分开，span/scale/sign 共享 | BUY/SELL 独立拟合响应幅度和衰减 |
| adverse guard | 方向判断分侧，threshold/pause/widen 共享 | 分侧估计 hazard-to-action uplift |
| defense guard | reducing 方向分侧，释放参数共享 | LONG repair SELL 与 SHORT repair BUY 分开 |
| fill selection | 只有 BUY live scorer | SELL 改用 repair-vs-trend-through target，不复用 BUY 标签 |
| P3 / effective kappa | 两侧共享 | 按 maker side、价格距离、regime、inventory role 校准 |
| queue-reactive Hawkes | 已有 BUY/SELL artifact | 同时纳入 counterparty 与 away-side taker flow |
| empirical microprice | 已有 BUY/SELL first-hit artifact | 加入分侧 shock/refill/recovery path |
| cooldown / rearm | 两侧时钟分开，base/weights 共享 | BUY-add 与 SELL-add 分开拟合 |
| replace / keep / cancel | 只按 increasing/reducing 拆分 | 分侧估计 queue reset cost 与 adverse-fill hazard |
| post-fill response | amplitude 与 add fraction 部分分侧 | 用 individual-trade path 拟合 side-specific response kernel |
| depth-kappa / BER | 对称共享 | bid depletion 与 ask depletion 分开验证 |
| inventory campaign | signed symmetric | LONG 与 SHORT repair/tail competing risk 分开 |
| dynamic cap | 全局风险上限 | 波动率可共享；只有 action support 足够时才拆 cap response |

现金流记账、maker fee、tick/lot、stale-data safety、单位契约和环境延迟身份应保持共享。它们是交易所或系统契约，不应为了“分侧”产生两套口径。

## 新研究面板

新增 [`models/audit/side_taker_flow_panel.py`](../audit/side_taker_flow_panel.py)，只生成研究特征，不修改 live 策略。

面板使用 Binance futures individual trades，并按 100ms 右边界形成 causal state。事件落在 `[t, t+100ms)` 时，只能从 `t+100ms` 开始使用。默认窗口为 100/250/500/1000/5000ms，分别保留：

- aggressive BUY/SELL count、quantity、quote notional；
- arrival rate、平均单笔 notional、same-side max run；
- sweep range、side interarrival、burst ratio；
- counterparty share、net counterparty pressure；
- maker-side adverse trade-price move。

每条 order decision 同时保留原始 BUY/SELL taker 字段和 maker 语义字段。例如 BUY maker 的 `counterparty_taker_quote_100ms` 来自 SELL taker，而 SELL maker 的同名字段来自 BUY taker。

历史 individual trades 只有 exchange timestamp。当前 Development 输出因此标记为 `exchange_time_diagnostic` 和 `policy_eligible=false`。注入冻结的 receive/feature delay 只能形成 latency-stressed diagnostic，不能进入 action replay；policy feature 必须由 mapped parent `aggTrade` 的 feature-ready clock 重建。逐笔块内 interarrival 与 receive order 无法从 live `aggTrade` 复现，永久保持 diagnostic-only。

## Development 诊断

冻结输出：

- panel: `${NARROWGATE_RETIRED_DATA_ROOT}/reports/side_taker_flow_v1_20260723/development_exchange_time_100ms.panel.parquet`
- summary: `${NARROWGATE_RETIRED_DATA_ROOT}/reports/side_taker_flow_v1_20260723/development_exchange_time_100ms.summary.json`
- denominator: 17 个 Development 日、1,448 个 campaign-level order-decision states；BUY/SELL actual fills 各 170。
- Validation 与 sealed holdout 均未读取。

为了避免 maker-side 选择差异混入原始 flow 对比，方向比较使用同一个 `feature_ready_ts` 的可见市场状态，只保留一行：

| Window | BUY/SELL taker quote ratio | BUY/SELL max-run ratio | 日聚类判断 |
|---|---:|---:|---|
| 100ms | 0.549 | 0.730 | quote difference 95% CI `[-2019.90,-111.18]`; run CI `[-1.345,-0.099]` |
| 250ms | 0.922 | 0.667 | quote CI 过零；run CI `[-3.117,-0.644]` |
| 500ms | 0.856 | 0.687 | quote CI 过零；run CI `[-4.138,-1.066]` |
| 1s | 0.934 | 0.843 | 两项 CI 均过零 |
| 5s | 0.919 | 0.932 | 两项 CI 均过零 |

这说明在当前 Development 的订单决策状态中，aggressive SELL taker 在亚秒级更集中，尤其是 same-side run；差异在 1-5s 聚合后明显减弱。它支持“不能默认方向对称”，但不说明反向动作一定盈利。

对 actual fills 的 fill-only 描述中，BUY maker 的 100ms net counterparty pressure 与 maker-signed markout 呈负向关系：pooled Spearman 为 `-0.146`，high-minus-low 为 `-0.086bps`，12 个可估日的 positive-day rate 为 25%，日 bootstrap 95% CI 为 `[-0.815,-0.040]bps`。其他 BUY 窗口和所有 SELL 窗口的日区间均穿过零。

因此目前唯一相对清楚的线索是：BUY maker 面对短时密集 SELL taker flow 时，成交质量可能更差。这个结果仍是 exchange-time、fill-conditioned association，不是 keep/cancel、widen 或 re-center 的 action uplift。

## 后续审计

`side_taker_hazard_m0_v1` 已关闭，但它不是干净的 side-slope 检验。冻结标签的 jump 仍来自旧 `adverse_price_jump_ts_ns`；cancel 是 baseline policy action/censor，repair 是 campaign transition。Pooled 与 split 还使用了不同 normalizer、intercept 和正则几何。

BUY split fill hazards 更差。SELL balanced loss 的 corrected composite 只剩 7 个双-head 完整日，favorable/adverse AUC 为 0.467/0.532。更重要的是，最初 1,448 行面板中的 BUY 100ms markout 信号没有在 277,368 行宽 denominator 上复现。因此结果仅是 internal chronological diagnostic，不创建 action、不进入 time-varying hazard v2、不读取 Validation/holdout，也不修改 live。

下一步先完成 `event_identity_and_riskset_v2` 与同窗 `historical_live_aggtrade_parity_v3` 的 recorder contract 已通过；finalized parent 的 quantity/count exact replay 仍 blocked。官方 USD-M public WebSocket 当前没有声明 raw individual Trade Stream，因此不会订阅未文档化的 futures `@trade`。Vision individual trades 只负责 exchange-time outcome，策略特征必须延迟到 mapped parent `aggTrade` 的 feature-ready clock；块内 receive/interarrival 不可用于 policy。随后才用同一 denominator 在 retained later days 复核原始信号。只有复现后，才允许登记一个简化 partial-pooled discrete-time 预测身份；预测通过也只能登记新的 randomized action experiment。完整纠正见 [`side_taker_hazard_m0_v1_20260723.md`](side_taker_hazard_m0_v1_20260723.md)。
