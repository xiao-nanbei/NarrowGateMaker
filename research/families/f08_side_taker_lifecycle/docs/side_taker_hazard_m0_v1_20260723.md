# side_taker_hazard_m0_v1 corrected audit

Last materially modified: 2026-08-12

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## 结论

`side_taker_hazard_m0_v1` 保持关闭。它不创建 action、不读取 Validation 或 sealed holdout、不修改 live，也不进入 `time_varying_side_hazard_v2`。

这轮结果只能作为 Development 内部的阴性诊断。原报告把面板称为 native first-hit competing risk、把 SELL composite 写成 9/9 days，并允许预测 gate 间接产生 action 许可；这三点均已纠正。

## 标签身份

冻结面板有 277,368 行、18 日。Native snapshot/delta 确实补充了盘口状态和 `future_mid_first_hit_ts_ns`，但 competing-risk 的 jump 标签仍来自 `adverse_price_jump_ts_ns`：

| 审计项 | 数值 |
|---|---:|
| adverse-price-jump first events | 171,839 |
| first-event timestamp 等于旧 jump timestamp | 171,839 |
| jump 行存在 native first-hit timestamp | 83,284 |
| first-event timestamp 等于 native first-hit | 26,304 |
| 未等于 native first-hit | 145,535 |
| 0ms jump exposure，拟合时被 clamp 为 1ms | 447 |

因此正确身份是：

```text
exact_order_id_mixed_market_policy_campaign_first_event.v2
```

其中 fill 与 price jump 属于市场路径，cancel 多数是 baseline policy action/censor，campaign repair 是 post-fill campaign transition，queue recovery 又是 recurrent count。它们不是 action-independent、可直接迁移到 keep/cancel 政策下的同一组 competing risks。

## 比较边界

Pooled 与 split 也不是干净的 side-slope 嵌套检验：

- pooled 的 `maker_side_buy` 是受 L2 正则的普通 coefficient；
- pooled、BUY、SELL 分别拟合不同 normalizer；
- split 有各自 intercept 与正则几何；
- AUC 只有点估计，没有 refit uncertainty、PR-AUC 或 within-day top-k precision。

因此 split loss 改善混合了 side base rate、缩放、intercept 和 slope，不能单独归因于 side-specific taker-flow slope。

## Chronological 结果

OOF 仍为 135,610 行、9 个 fold 内 future test days，但训练/测试日最晚到 2026-05-04，早于最初 2026-05-06 至 2026-06-14 的 hypothesis window。它是 internal chronological OOF，不是相对于假设形成过程的 confirmatory future evidence。

`delta` 是 split minus pooled 的按日 balanced log loss，负值仅表示 split 的概率 loss 更低：

| Maker side | Cause | 1s OOF events | Delta | Split AUC |
|---|---|---:|---:|---:|
| BUY | favorable fill | 26 | +0.05538 | 0.507 |
| BUY | adverse fill | 30 | +0.00597 | 0.514 |
| SELL | favorable fill | 26 | -0.04793 | 0.467 |
| SELL | adverse fill | 41 | -0.00823 | 0.532 |

BUY composite 为 `+0.03068`，9 个双-head 完整日全部更差。SELL adverse 只有 7 个可计算日，因此 corrected composite 只使用这 7 个 favorable/adverse 都可计算的日期，结果为 `-0.02800`，95% day-bootstrap CI `[-0.03096,-0.02529]`。不能再写成 SELL 9/9 days。

SELL 的改善主要是整体概率校准，favorable/adverse 排序仍接近随机。由于标签 estimand 与比较结构均无效，代码现在无条件输出：

```text
predictive_split_gate_passed = false
followup_randomized_experiment_registration_eligible = false
action_family_allowed = false
```

## Denominator 复核

最初 1,448 行 eligible-state 面板在 2026-05-06 至 2026-06-14 显示 100ms BUY/SELL quote ratio `0.549`，BUY fill markout high-minus-low 约 `-0.0857bps`。在当前 277,368 行、更宽且更早的 denominator 上：

- 100ms BUY/SELL quote ratio 为 `1.0022`，按日差异区间跨零；
- BUY fill markout high-minus-low 为 `-0.0121bps`，95% CI `[-0.1088,+0.0619]bps`。

初始信号没有在更宽总体上延续。M0 可以作为阴性诊断，但不能称为严格的 confirmatory failure，也不能通过增加更多 hazard heads 来补救。

## 后续前置条件

这条主线只有依次完成以下条件后才可重新登记新身份：

1. 通过 `event_identity_and_riskset_v2`：将 cancel request/ACK 拆开，把 jump 作为 state transition，把 repair 作为带 delayed entry 的 campaign transition，并输出 start-stop risk set、remaining quantity 与 partial-fill path。
2. 通过 `historical_live_aggtrade_parity_v3` 的 recorder contract。该 gate 已通过，但 finalized historical parent 的 quantity/count exact replay 仍 blocked；不订阅未文档化的 futures `@trade`；Vision individual trades 只作 exchange-time outcome，policy feature 统一在 mapped parent `aggTrade` feature-ready 后可见。块内 child interarrival 与 receive order 永久保持 diagnostic-only。
3. 用完全相同 denominator，在未参与窗口选择的 retained later days 复核 `BUY 100ms pressure -> markout`；失败即关闭 side-taker 主线。
4. 只有复现后才比较单一 train normalizer、真正不惩罚的 side intercept 和少量 side interaction/partial pooling，不直接拆成 6 causes x 5 bins x 2 sides。
5. 预测通过最多允许登记一个新的 randomized action experiment。动作、reward、propensity、queue reset、机会成本和 campaign interference 仍须另行冻结并用 action-uplift gate 验证。

v1 的冻结诊断与 v2 的替代契约见 [`event_identity_and_riskset_v1_20260723.md`](event_identity_and_riskset_v1_20260723.md) 和 [`binance_trade_lifecycle_contract_v2_20260723.md`](binance_trade_lifecycle_contract_v2_20260723.md)。

## 纠正后产物

- panel: `${NARROWGATE_RETIRED_DATA_ROOT}/reports/side_taker_hazard_m0_v1_20260723/state_fit_v4_exchange_time_100ms.panel.parquet`
- OOF: `${NARROWGATE_RETIRED_DATA_ROOT}/reports/side_taker_hazard_m0_v1_20260723/chronological_oof_predictions.corrected.parquet`
- summary: `${NARROWGATE_RETIRED_DATA_ROOT}/reports/side_taker_hazard_m0_v1_20260723/chronological_calibration_summary.corrected.json`
- dataset manifest: `${NARROWGATE_RETIRED_DATA_ROOT}/reports/side_taker_hazard_m0_v1_20260723/dataset_manifest.corrected.csv`

未带 `.corrected` 的旧 summary/manifest 仅作为 superseded audit trail 保留，不再作为研究证据。
