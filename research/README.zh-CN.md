# 研究家族布局

<p><a href="README.md">English</a> | <a href="README.zh-CN.md">简体中文</a></p>

Last materially modified: 2026-09-06

Last materially synchronized: 2026-09-06

NarrowGate 研究划分为十个策略/证据家族、一条系统工程线和四个共享基础设施层。所有研究源码、证据、共享合同及治理元数据都位于同一个 `research/` 子树下。

## 使用工具与阅读研究结果是两件事

先用[无需账户的回放示例](../examples/replay_demo/README.zh-CN.md)查看一笔订单的排队、成交与库存路径，或用[一日数据教程](../docs/opensource/one_day_data_pipeline.zh-CN.md)处理自己的行情。这些入口不要求某个历史策略假设已获研究通过。

下表描述研究状态，不是安装状态。`active` 表示仍在研究所列问题，`closed` 表示记录中的候选或假设在已测试条件下没有获得继续推进的依据。它们都不代表目录里每个可复用的数据加载器、模拟器、标签构建器或诊断工具已启用、已禁用、能够盈利或可直接部署。支持哪些输入应以该单元当前 README 和命令帮助为准；精确复现历史实验仍可能需要未公开工件。

新工作应复用合适的当前实现，并说明实际数据、延迟、排队和初始状态假设。不要把旧结论移植到不同环境，也不要用某个失败实验阻断无关的工具使用。CLI 可运行、实现测试通过，证明的是软件行为，不是经济价值或交易许可。

## 目录与研究历史

公共研究文档与仅所有者可见的证据按照仓库级[公共研究与私有证据布局](PRIVATE_EVIDENCE.md)分离。每个具体研究单元都在其受 Git 跟踪的 README/docs 中保存公共方法和结论，并通过其被忽略的本地 `private/` catalog 解析未公开的 artifact。

过去由各家族拥有、位于 `models/`、`models/audit/`、`features/`、`docs/`、`cpp/narrowgate_cpp/` 以及原根目录 `research_*` 下的路径均已删除。活跃 import、命令、测试和构建文件直接使用规范的 `research.*` package；不保留兼容 symlink 或重复源码文件。

带版本的迁移合同位于 `governance/migrations/`。`layout_v1.json` 将首批 198 个已删除路径映射到家族归属；`layout_v2.json` 将原根目录研究布局映射到当前子树。精确的边界 archive 属于私有历史证据，不随公共仓库分发；[`governance/archive/README.md`](governance/archive/README.md)公开其 artifact ID、SHA256、字节数和可用性。精确历史复现必须使用匹配的原始源码/archive；新工作直接使用当前规范实现，不得声称它复现了归档字节。

重新运行并不自动产生新的研究身份。只要冻结样本、baseline/candidate、fold、估计目标和统计合同不变，普通缺陷、cache、序列化或性能修复就保持原研究身份；新的正式运行记录独立执行尝试及实际源码/输入身份。科学合同中的这些要素变化才需要新研究身份。复用共享算法并不继承历史家族已消耗的数据面板、结论或权限。见[贡献指南](../CONTRIBUTING.zh-CN.md#研究身份与正式执行)。

| ID | 目录 | 研究状态 | 共享层 |
|---|---|---|---|
| F01 | `families/f01_fixed_parameter_racing/` | alpha 家族已关闭；仅用于筛选 | D, R, S, G |
| F02 | `families/f02_empirical_p3_touch/` | 冻结的 replay/operational comparator 依赖；后继预测基础设施仅属于研究层 | D, S |
| F03 | `families/f03_causal_13_head/` | causal-v12 semantics-v6 是冻结的 replay/operational comparator；research q10 未解决 | D, R, S, G |
| F04 | `families/f04_external_market_alpha/` | BABEL-P1 数量门已完成；exact lifecycle、共同分母与 causal clock 尚未闭合；受时钟限制的 E6/P2 quote mechanics 已完成；无 action authority | D, R, G |
| F05 | `families/f05_fill_quality_quote_ev/` | 证据线活跃；direct action 已归档；first-add USDC prediction 在 Development 阶段关闭 | D, R, S |
| F06 | `families/f06_placement_fill_cif/` | placement-distance fill/value 路径已关闭；signed marginal value 在 Development 上不可识别 | D, R, G |
| F07 | `families/f07_active_order_continuation/` | 历史家族已关闭；operational BUY trial 单独治理 | D, R, S, G |
| F08 | `families/f08_side_taker_lifecycle/` | identity/parity 研究活跃；hazard M0 已关闭 | D, R |
| F09 | `families/f09_campaign_action_uplift/` | 仅保留冻结的 global-BER replay/operational comparator；无已注册 action；已测试的 cooldown temporal-permission action 子空间已穷尽 | D, R, S, G |
| F10 | `families/f10_live_replay_attribution/` | 活跃诊断线；first-add Development 证据已完成，无 action authority | R, S |
| SYS | `system_engineering/` | 工程证据活跃；无 alpha authority | R, S |

共享层索引位于 `shared/`：

- D：data identity 与 good-day admission；
- R：权威 replay、queue 与 lifecycle；
- S：共享 live/replay strategy semantics；
- G：experiment identity、scorecard、OPE governance 与 promotion control。

在家族之间移动文件属于治理变更。请更新 `registry.json` 和路径迁移记录，然后重新运行 import、archive、build 和 frozen-identity 检查。不得重新创建已删除的 legacy path。目录状态绝不授予 Validation、holdout、action 或 live permission；这些 permission 仍由家族专属的 frozen experiment identity 管理。
