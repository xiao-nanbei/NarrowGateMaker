# event_identity_and_riskset_v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

> **Superseded implementation note (2026-07-23):** v1 的 blocked 诊断仍有效；replay 已新增完整 start-stop lifecycle 与 `event_identity_and_riskset_v2`。正式 Development panel 尚须重建并通过 v2 gate。实现与数据源契约见 [`binance_trade_lifecycle_contract_v2_20260723.md`](binance_trade_lifecycle_contract_v2_20260723.md)。

## 结论

`event_identity_and_riskset_v1` 已冻结并 **blocked**。当前 277,368 行、18 日的 order-value panel 不足以支持 `dynamic_fill_hazard_m0_v2`，也不允许创建 action。

这不是预测结果为负，而是 estimand 尚不可识别。当前 first-event 表把市场事件、baseline policy action 和 campaign transition 放在同一个从 `decision_ts` 开始的齐次风险集中；下一代模型必须先拆开这些身份。

## 冻结契约

### Fill

Fill hazard 只在订单已经激活且剩余数量大于零时进入风险集。每个 start-stop 区间必须记录 activation、区间起止、区间前后 remaining quantity、partial/full fill 数量和事件顺序。

### Cancel

Cancel request 是 behavior policy 决定的 stopping time：

\[
\tau_{ack}=\tau_{request}+L_{cancel}(\mathcal H_{\tau_{request}},s_{system})
\]

ACK 前仍可能 partial/full fill；ACK 只取消剩余量。因此 request、ACK、pending 期间 fill 和 request/ACK 时剩余量必须分别记录，不能把 ACK 当作自然市场 cause。

### Price jump

Jump 必须使用 native snapshot/delta 的 future-mid first hit，并解决同毫秒事件顺序。对于 fill-value estimand，jump 是状态转移，不是吸收终点；jump 后订单仍可继续成交。

### Campaign repair

Repair 使用 delayed-entry risk indicator：

\[
\lambda_R(t\mid\mathcal H_t)=Y_R(t)\widetilde\lambda_R(t\mid\mathcal H_t)
\]

只有 inventory 非零、campaign 有效且 reducing 路径已进入可修复状态时，`Y_R(t)=1`。必须记录 risk-entry/risk-exit、reducing quote active/eligible 和 repair timestamp。

## 真实面板审计

输入：

```text
${NARROWGATE_RETIRED_DATA_ROOT}/reports/
side_taker_hazard_m0_v1_20260723/
state_fit_v4_exchange_time_100ms.panel.parquet
```

输入 SHA256：`340c09bb2ca8c5eb53808f224f5e599ecf83652088263d2bde3ba0a6377b8ea1`。

| Cause | 当前事件数 | 关键缺口 |
|---|---:|---|
| favorable/adverse fill | 1,264 | activation、fill qty/partial、remaining qty path |
| cancel ACK | 102,703 | cancel request、request/ACK remaining qty、pending fill |
| adverse jump | 171,839 | native event sequence、same-ms ordering |
| campaign repair | 1,544 | delayed-entry、at-risk indicator、reducing path state |

当前 label identity 仍全部为 `exact_order_id_first_event`。171,839 个 legacy jump 中只有 26,304 个与 native first-hit timestamp 完全相同，145,535 个不同；另有 447 个 0ms legacy jump 和 1 行同时间事件歧义。

审计输出：

```text
${NARROWGATE_RETIRED_DATA_ROOT}/reports/
event_identity_and_riskset_v1_20260723/summary.json
```

## Gate

```text
dynamic_fill_hazard_event_gate_passed = false
dynamic_fill_hazard_allowed = false
action_family_allowed = false
```

后续必须先让 replay 输出完整 start-stop 生命周期字段，并在相同 panel 上通过本审计；这一步只解决事件与风险集身份，仍须单独通过 `live_taker_flow_parity_v1`。
