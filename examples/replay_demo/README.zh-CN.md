# 公共回放示例

[English](README.md) | [简体中文](README.zh-CN.md)

Last materially modified: 2026-09-06

Last materially synchronized: 2026-09-06

状态：可离线运行的教学示例，随包提供输入和参考输出。

## 可以体验什么

跟随一笔买单入队，经历一次同价成交但自己未成交，再通过两次部分成交形成库存。随后一笔卖单平掉库存，另一笔卖单始终未成交并被撤销。安装完成后，不需要账户、API key、行情下载或 C++ 编译。价格和成交都是手写示例，不是策略收益样本。

## 运行

在仓库根目录使用已激活的 Python 3.11 或更新环境：

```bash
python -m pip install -e .
narrowgate replay-demo \
  --output-dir results/replay_demo \
  --verify-reference
```

安装完成后，该命令无需联网。`--verify-reference` 在写出文件前对照随包参考输出；只想体验示例时可以省略。

| 输出 | 可以查看什么 |
| --- | --- |
| `trace.jsonl` | 每个输入事件一行：报单、队列消耗、成交、撤单与终点盯市 |
| `summary.json` | 全部三笔订单、成交与未成交计数、库存 campaign、手续费、现金和终点 PnL |
| `receipt.json` | 输入/输出身份及机制检查结果，不是交易许可 |

同一源码字节下再次运行会产生字节一致的文件。receipt 时间是 fixture 固定值，不是当前时间。

## 跟随第一笔订单

下表直接对应公开 [tape](../../narrowgate/fixtures/replay_demo/synthetic_tape.jsonl) 和[参考轨迹](../../narrowgate/fixtures/replay_demo/reference/trace.jsonl)。时间为合成 tape 开始后的秒数，数量单位为 BTC。

| 时间 | 事件 | 队列与账户变化 |
| --- | --- | --- |
| 2s | 提交 `demo-buy-001`：在 100.00 买入 0.010 | 排在 0.015 后面，库存仍为零 |
| 3s | 100.00 上发生 0.010 主动卖出 | 前排量降到 0.005；我们的订单**未成交** |
| 4s | 100.00 上发生 0.009 主动卖出 | 消耗剩余前排量，再成交 0.004；库存变成 0.004 |
| 5s | 100.00 上发生 0.006 主动卖出 | 剩余 0.006 成交；库存变成 0.010 |
| 7–8s | 在 100.20 提交并成交 `demo-sell-001` | 先消耗该单前方的 0.005，再卖出 0.010，库存归零 |
| 10–12s | 提交并撤销 `demo-sell-002` | 始终未成交，但仍计入订单总数 |

fixture 的手续费为零，这次 0.010 往返交易得到 `0.010 × (100.20 − 100.00) = 0.002 USDC`。终点账户已经空仓，因此盯市不会额外增加库存价值。这是记账示例，不是预期收益。三次成交事件属于两笔已成交订单；市场在挂单价成交，不等于我们的订单必然成交。

## Fixture

- [`contract.json`](../../narrowgate/fixtures/replay_demo/contract.json) 冻结 fixture schema/engine version、输入 SHA256、共同分母、预期终端值和确定性 receipt 时间。JSON 中历史 classification/permission 字段仅为兼容性描述，不授予能力，也不控制输出资格。
- [`synthetic_tape.jsonl`](../../narrowgate/fixtures/replay_demo/synthetic_tape.jsonl) 是手工编写的最优盘口与成交序列，不是交易所数据，没有经验或经济权威。
- [`reference/`](../../narrowgate/fixtures/replay_demo/reference/) 保存与分发引擎字节对应的逐字节预期汇总、事件轨迹和 receipt。
- [`../../narrowgate/cli.py`](../../narrowgate/cli.py) 提供规范公共命令，委派给打包的 [`../../narrowgate/replay_demo.py`](../../narrowgate/replay_demo.py) 引擎。参考引擎仅支持这一小型 FIFO 最优盘口教学合同，不是私有数据完整回放，也不声称 live 真实性。

## 证据边界

runner 导入仓库连续记账总账，用于现金、库存、campaign 结束和盯市权益。小型 FIFO 最优盘口引擎不复现实测行情/报单延迟、撤单 ACK 竞态、隐藏流动性、完整历史 L2 或真实交易所排队位置。参考检查通过仅表示该教学示例可复现，不表示历史 maker 成交或 live PnL 精确一致。

打包引擎用常量固定示例分类、权限及输出资格：无论输入如何声明，`economic_evidence_eligible`、`promotion_eligible` 和 `live_action_eligible` 都保持 false。实现没有交易所/网络客户端或外部报单路径，不导入 live runtime，只读取传入的公共 fixture/reference 输入。`network_access=false` 之类 JSON 字段不是操作系统沙箱。tape 身份只验证一次，并在 receipt 中复用；不匹配时在回放前失败。共同分母、记账或终端值不匹配会写入 `failed_closed` 检查结果。

证据可用性：本示例引用的全部 tape、contract、engine、summary、trace、receipt 字节均随公共包分发，或位于 receipt 指明的仓库相对路径。未使用私有证据。

下一步：用[一日数据教程](../../docs/opensource/one_day_data_pipeline.zh-CN.md)处理自己的公开成交归档，解读订单簿结果前先查看其中的数据状态表。
