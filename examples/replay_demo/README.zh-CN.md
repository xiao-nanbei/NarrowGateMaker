# 公共回放示例

[English](README.md) | [简体中文](README.zh-CN.md)

Last materially modified: 2026-09-06

Last materially synchronized: 2026-09-06

状态：随包分发的合成机制 fixture；非经济证据、不具备晋级资格。

## 问题与结果

新用户能否不下载市场数据、不连接交易所，就验证 NarrowGate 的市场事件、maker 排队、订单成交、成交共同分母、库存 campaign、终端 PnL 和证据检查？可以。本 fixture 确定性地运行这条路径；通过仅表示公开演示机制与冻结合同相符。

## 运行

在仓库根目录使用 Python 3.11 或更新版本：

```bash
narrowgate replay-demo \
  --output-dir results/replay_demo \
  --verify-reference
```

命令写入 `summary.json`、`trace.jsonl` 和 `receipt.json`。同一源码字节下再次运行会产生字节一致的文件。receipt 时间为合同中的冻结值，runner 不读取运行时钟。

## Fixture

- [`contract.json`](../../narrowgate/fixtures/replay_demo/contract.json) 冻结 fixture schema/engine version、输入 SHA256、共同分母、预期终端值和确定性 receipt 时间。JSON 中历史 classification/permission 字段仅为兼容性描述，不授予能力，也不控制输出资格。
- [`synthetic_tape.jsonl`](../../narrowgate/fixtures/replay_demo/synthetic_tape.jsonl) 是手工编写的最优盘口与成交序列，不是交易所数据，没有经验或经济权威。
- [`reference/`](../../narrowgate/fixtures/replay_demo/reference/) 保存与分发引擎字节对应的逐字节预期汇总、事件轨迹和 receipt。
- [`../../narrowgate/cli.py`](../../narrowgate/cli.py) 提供规范公共命令，委派给打包的 [`../../narrowgate/replay_demo.py`](../../narrowgate/replay_demo.py) 引擎。参考引擎仅支持这一小型 FIFO 最优盘口教学合同，不是私有数据完整回放，也不声称 live 真实性。

## 证据边界

runner 导入仓库连续记账总账，用于 cash、inventory、campaign 结束和盯市 equity。排队耗尽和合成订单生命周期机制有意由文档所述参考引擎实现，因为完整历史回放所需的市场数据与校准工件不包含在本 fixture 中。

打包引擎用常量固定示例分类、权限及输出资格：无论输入如何声明，`economic_evidence_eligible`、`promotion_eligible` 和 `live_action_eligible` 都保持 false。实现没有交易所/网络客户端或外部报单路径，不导入 live runtime，只读取传入的公共 fixture/reference 输入。`network_access=false` 之类 JSON 字段不是操作系统沙箱。tape 身份只验证一次，并在 receipt 中复用；不匹配时在回放前失败。共同分母、记账或终端值不匹配会写入 `failed_closed` 检查结果。

证据可用性：本示例引用的全部 tape、contract、engine、summary、trace、receipt 字节均随公共包分发，或位于 receipt 指明的仓库相对路径。未使用私有证据。
