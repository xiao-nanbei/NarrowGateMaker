# 2025-08-01 至 2025-12-31 行情回填与 Tardis 重建

Last materially modified: 2026-08-12

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](public_private_documentation_contract.md).

## 结论

本次回填保持三类身份严格分离：

- 非 CryptoHFTData 技术可用性：15 路数据均达到 153/153 日；
- Tardis 原始数据：`book_ticker` 与 `incremental_book_L2` 均达到 153/153 日；
- Tardis provider-normalized top-20/100ms：153 日完成重建，但仅 93 日通过冻结工程 gate；
- CryptoHFTData/Tardis 双源：2025 新增段没有可用的规范化 CryptoHFT top-20 overlap，另以既有正式 good-day 交集冻结 62 个 2026 日期，其中 60 日通过 post-batch 完整性、18 日同时通过完整 Tardis provider gate。

这些结果没有修改 CryptoHFTData、canonical good-day、研究 denominator、策略或 live 权限。

## Tardis 原始数据

下载身份为 `tardis.0730-beinan.binance-futures.BTCUSDC.v1`：

- 153 个 UTC 自然日；
- 153 个 `book_ticker` 文件；
- 153 个 `incremental_book_L2` 文件；
- 306 个唯一 zstd 文件，共 37,870,428,980 bytes；
- 远端缺失 0、`.part` 残留 0；
- 两遍 SHA256、Content-Length、完整 zstd 解压、CSV 行数、header 与首尾记录校验均通过。

原始清单：

`${NARROWGATE_MARKETDATA_ROOT}/tardis/manifests/binance_futures_btcusdc_20250801_20251231_download.json`

## 非 CryptoHFTData 覆盖

严格审计 universe 为 15 路：

- Binance：BTCUSDC/BTCUSDT perp aggTrades、individual trades、metrics，BTCUSDC/BTCUSDT spot aggTrades，以及 USDCUSDT spot aggTrades anchor；
- Bitget：BTCUSDT perp/spot trades；
- Bybit：BTCUSDT perp/spot trades；
- OKX：BTCUSDT perp/spot trades。

首次 2,295 项审计仅发现 Bybit perp 的 2025-12-05/06/07/08/31 五个缺日。定向修复后完整复审结果为：

- 15 sources × 153 days = 2,295/2,295；
- failure = 0；
- all-sources-valid days = 153/153；
- 91,087,950,082 bytes；
- 2,291,664,792 rows；
- `identity=technical_availability_only_not_frozen_good_day`。

最终报告：

`${NARROWGATE_DATA_ROOT}/reports/marketdata_backfill_20250801_20251231/final_audit_after_bybit_repair/summary.json`

## Tardis top-20/100ms

独立输出身份为 `normalized_tardis_l2_100ms_v1`。100ms 状态使用 Tardis provider-local clock 的 half-open right boundary，只包含 `local_timestamp < boundary` 的事件。每个日期原子发布 BBO、top-20 L2、clock sidecar 与 quality JSON。

2025 的 153 日结果：

- post-batch 哈希/结构有效 153/153；
- provider-normalized replay candidate 93/153；
- causal violation 总数 0；
- provider-local clock reversal 总数 0；
- freshness 中位数 99.993866%；
- 60 个拒绝日中，26 日命中 logical max gap > 5s，42 日未通过 bookTicker/L2 冻结价格合同；原因有重叠；
- 另有 1 日出现 invalid-spread bucket；该原因也可能与上述拒绝原因重叠；
- 最严重 logical gap 为 1,141.646680s；
- 所有 306 个 raw payload 与 BBO/L2/clock 输出均在 post-batch 审计中重新核验。

权威产物：

- `${NARROWGATE_DATA_ROOT}/reports/tardis_normalized_l2_100ms_v1_20260731/technical_2025_postbatch_audit.json`
- `${NARROWGATE_DATA_ROOT}/reports/tardis_normalized_l2_100ms_v1_20260731/technical_2025_quality.csv`

## 双源对照

2025-12-31 虽有 CryptoHFTData raw 小库存，但没有可用于本合同的规范化 CryptoHFT top-20，因此不得伪造 2025 双源结果。双源面板改用已冻结的 62 个 2026 formal CryptoHFT good-day 与 Tardis raw 交集。

62 日均生成双源诊断；post-batch 审计中 60/62 日结构与哈希有效。2026-05-31 与 2026-07-08 存在大量 Tardis `local_timestamp < exchange_timestamp`，严格因果 sidecar 合同拒绝这两日。完整 Tardis provider gate 为 18/62。

在 60 个完整性有效日上：

| 指标 | min | median | p95 | max |
| --- | ---: | ---: | ---: | ---: |
| exchange-time causal matched ratio | 82.14% | 90.50% | 95.16% | 95.82% |
| causal top-20 price exact | 89.04% | 94.19% | 97.02% | 97.79% |
| causal top-20 price within one tick | 90.79% | 95.92% | 97.98% | 98.60% |
| nearest-100ms matched ratio | 99.04% | 99.84% | 99.99% | 100.00% |
| nearest-100ms top-20 price exact | 92.35% | 95.09% | 97.08% | 98.17% |

`nearest-100ms` 是 clock-agnostic 诊断，不是因果证明。正式研究若需要因果状态，应使用 exchange-time causal as-of，并显式处理其较低 matching support。

权威产物：

- `${NARROWGATE_DATA_ROOT}/reports/tardis_normalized_l2_100ms_v1_20260731/formal_2026_postbatch_audit.json`
- `${NARROWGATE_DATA_ROOT}/reports/tardis_normalized_l2_100ms_v1_20260731/combined_2025_2026_quality.json`
- `${NARROWGATE_DATA_ROOT}/reports/tardis_normalized_l2_100ms_v1_20260731/combined_2025_2026_quality.csv`

## 证据边界

Tardis incremental L2 不含 Binance `U/u/pu`，因此即使通过本合同，也只能是 provider-normalized replay candidate：

```text
native_binance_sequence_ids_present=false
native_sequence_continuity_proven=false
exact_queue_policy_eligible=false
aws_tokyo_receive_time=false
policy_visible=false
live_transport_eligible=false
```

本次新增日期不会自动进入 canonical good-day。若某研究族要使用 93 个 2025 Tardis candidate 或 18 个完整双源 candidate，必须登记新的 source-separated denominator、时间合同与研究身份。

## 存储

完成后 `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` 使用约 498GiB/954GiB，可用约 456GiB。相关目录约为：Tardis archive 89GiB、normalized Tardis 18GiB、external venues 53GiB，仍满足项目冻结容量门槛。
