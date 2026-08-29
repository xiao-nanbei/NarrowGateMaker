# 研究家族布局

<p><a href="README.md">English</a> | <a href="README.zh-CN.md">简体中文</a></p>

Last materially modified: 2026-08-29

Last materially synchronized: 2026-08-29

NarrowGate 研究划分为十个策略/证据家族、一条系统工程线和四个共享基础设施层。所有研究源码、证据、共享合同及治理元数据都位于同一个 `research/` 子树下。

公共研究文档与仅所有者可见的证据按照仓库级[公共研究与私有证据布局](PRIVATE_EVIDENCE.md)分离。每个具体研究单元都在其受 Git 跟踪的 README/docs 中保存公共方法和结论，并通过其被忽略的本地 `private/` catalog 解析未公开的 artifact。

过去由各家族拥有、位于 `models/`、`models/audit/`、`features/`、`docs/`、`cpp/narrowgate_cpp/` 以及原根目录 `research_*` 下的路径均已删除。活跃 import、命令、测试和构建文件直接使用规范的 `research.*` package；不保留兼容 symlink 或重复源码文件。

带版本的迁移合同位于 `governance/migrations/`。`layout_v1.json` 将首批 198 个已删除路径映射到家族归属；`layout_v2.json` 将原根目录研究布局映射到当前子树。精确的边界 archive 属于私有历史证据，不随公共仓库分发；[`governance/archive/README.md`](governance/archive/README.md)公开其 artifact ID、SHA256、字节数和可用性。经授权的历史复现必须验证匹配的私有 archive，而当前规范代码在重新运行时必须获得新的 experiment identity。

| ID | 目录 | 状态 | 共享层 |
|---|---|---|---|
| F01 | `families/f01_fixed_parameter_racing/` | alpha 家族已关闭；仅用于筛选 | D, R, S, G |
| F02 | `families/f02_empirical_p3_touch/` | 当前运行基线的活跃依赖 | D, S |
| F03 | `families/f03_causal_13_head/` | causal-v12 semantics-v6 是当前运行与回测基线；research q10 未解决 | D, R, S, G |
| F04 | `families/f04_external_market_alpha/` | BABEL-P1 first-add M0/M1 在等待 30 个 receive-time 日；E6/P2 quote mechanics 已在并行路径完成且受时钟限制；无 action authority | D, R, G |
| F05 | `families/f05_fill_quality_quote_ev/` | 证据线活跃；direct action 已归档；first-add USDC prediction 在 Development 阶段关闭 | D, R, S |
| F06 | `families/f06_placement_fill_cif/` | placement-distance fill/value 路径已关闭；signed marginal value 在 Development 上不可识别 | D, R, G |
| F07 | `families/f07_active_order_continuation/` | 历史家族已关闭；operational BUY trial 单独治理 | D, R, S, G |
| F08 | `families/f08_side_taker_lifecycle/` | identity/parity 研究活跃；hazard M0 已关闭 | D, R |
| F09 | `families/f09_campaign_action_uplift/` | 机制研究活跃，无已注册 action；已测试的 cooldown temporal-permission action 子空间已穷尽 | D, R, S, G |
| F10 | `families/f10_live_replay_attribution/` | 活跃诊断线；first-add Development 证据已完成，无 action authority | R, S |
| SYS | `system_engineering/` | 工程证据活跃；无 alpha authority | R, S |

共享层索引位于 `shared/`：

- D：data identity 与 good-day admission；
- R：权威 replay、queue 与 lifecycle；
- S：共享 live/replay strategy semantics；
- G：experiment identity、scorecard、OPE governance 与 promotion control。

在家族之间移动文件属于治理变更。请更新 `registry.json` 和路径迁移记录，然后重新运行 import、archive、build 和 frozen-identity 检查。不得重新创建已删除的 legacy path。目录状态绝不授予 Validation、holdout、action 或 live permission；这些 permission 仍由家族专属的 frozen experiment identity 管理。
