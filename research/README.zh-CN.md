# 407 日研究体系

[English](README.md) | [简体中文](README.zh-CN.md)

Last materially modified: 2026-10-17

Last materially synchronized: 2026-10-17

当前研究入口：[F01机制实验收尾](families/f01_fixed_parameter_racing/README.zh-CN.md)与[F04完整训练支持方向模型](families/f04_external_market_alpha/README.zh-CN.md)。原Defense/Urgency九格实验继续按原身份保留已验收结论。后续机制实验按用户要求停止，独立账户日历仅部分完成，连续Final107完整完成，两项消融均不晋升。当前参照保持 `B0_DEFENSE_URGENCY_OFF_20261011`。小支持量M0/M1 Final扩展已按用户要求停止并归档，保留8个已验收M0账户、9个中止账户和91条未提交路径。后续五候选均完成54个独立Final账户及最小归档收尾。五候选总收益与两种成交量归一收益均劣于同合同B0，均不晋升；见[完整对比](families/f03_causal_13_head/README.zh-CN.md#五候选final107完整对比)及[汇总记录](final107_comparison.json)。旧响应、F07 U1及hazard结论不重写；少亏不等于盈利或新鲜holdout验证。

当前入口是[统一重算实施计划](RECOMPUTE_407.zh-CN.md)和[12 包任务清单](recompute_407.json)。按科学问题重算，不按旧版本逐次复演；历史 closed/exhausted/promotion 不阻止新研究。previous-use、真实输入限制及独立账户边界不因此解除。按所有者10月17日要求，已取消回测次数配额，不因旧数量预算用完而拒绝必要评价；资源保护、身份和防重复校验保持。

新链路 [F03](families/f03_causal_13_head/README.zh-CN.md) 已完成全部 858 次策略分片，包含后来批准的 ML-OFF。Final 净收益 H=inf 为 −189.212148 USDC，ML-OFF 为 −269.042233 USDC，两者均亏损；保留 previous-use 及中途查看结果的限制。其他包的集成与执行状态以任务清单为准；F03 完成不代表全体系、407 日参考数据验收或镜像完成。

## 扫描待验证问题

从[研究族索引](registry.json)逐项读取 `families[].research_questions`，筛选 `status=pending_validation`。当前已收录 F02 的 scalar-adapter，以及 F05 的多尺度冷却、慢更新挂单与不利成交、markout与持仓生命周期四个问题。没有该字段的研究族表示尚未整理，不表示没有问题；本次不是全历史研究的完整盘点。

以后遇到分散的旧结论，先归属到已有研究族的同一问题记录，注明旧出处、证据能否核实、方法缺口和下一步验证。旧报告只证明其原身份下记录过什么，不自动证明当前数据或策略有效。不能考证的标为来源不可核实，缺少标准实验的标为方法待重建；不能因缺证据直接判阴性。新验证完成后链接其实际报告再更新状态，不改写冻结历史，不让登记动作自动启动实验。

## 当前研究族

以下实验按所改变的决策层归入现有研究族，不另建研究族。主报告说明方法与结论；私有配置、逐账户账本和运行材料不随文档分发。各实验的 B0 和执行场景分别保留，不能串成同一条收益序列，已完成子实验也不等于整个研究族完成。

| 实验 | 归属与比较对象 | 能得出的结论 |
| --- | --- | --- |
| 联动系数（历史 gamma 轴）＋不对称偏移 | [F01](families/f01_fixed_parameter_racing/README.zh-CN.md)：交叉改变两个报价控制轴 | 单账户少亏与库存风险并存，两轴存在交互；后来的参照选择不等于证明最优 |
| E/C | [F05](families/f05_fill_quality_quote_ev/README.zh-CN.md)：POST/WAIT、KEEP/CANCEL 与随机对照；C 交叉引用 F07 | E/EC 减亏但仍亏损，C 增量不一致；成交量不同，未证明稳定选择性 |
| A00/A11/A10/A01 | [F01](families/f01_fixed_parameter_racing/README.zh-CN.md)：普通向外、向内改单门槛的四臂比较 | 统一降低基本持平略差；仅向外少亏但收益集中、库存峰值上升；仅向内更差 |
| 目标报价方差率动态门槛 | [F01](families/f01_fixed_parameter_racing/README.zh-CN.md)：单一动态 D 与固定典型尺度 S | 两账户合计 D/S 均弱于 B0；动态相对静态没有稳定额外价值证据 |
| U0/U1 | [F07](families/f07_active_order_continuation/README.zh-CN.md)：原评价时机与事件驱动评价，原订单门槛不变 | 八账户合计少亏，但单位成交额变差、请求及库存暴露增加；不等于稳定盈利或更好选单 |

E/C、门槛候选和 U1 均不因这些结果自动晋升 B0 或部署 live。下表保留全部研究族的唯一维护入口。

| 研究族 | 当前入口 |
|---|---|
| F01 | [research.families.f01_fixed_parameter_racing](families/f01_fixed_parameter_racing/README.zh-CN.md) |
| F02 | [research.families.f02_empirical_p3_touch](families/f02_empirical_p3_touch/README.zh-CN.md) |
| F03 | [research.families.f03_causal_13_head](families/f03_causal_13_head/README.zh-CN.md) |
| F04 | [research.families.f04_external_market_alpha](families/f04_external_market_alpha/README.zh-CN.md) |
| F05 | [research.families.f05_fill_quality_quote_ev](families/f05_fill_quality_quote_ev/README.zh-CN.md) |
| F06 | [research.families.f06_placement_fill_cif](families/f06_placement_fill_cif/README.zh-CN.md) |
| F07 | [research.families.f07_active_order_continuation](families/f07_active_order_continuation/README.zh-CN.md) |
| F08 | [research.families.f08_side_taker_lifecycle](families/f08_side_taker_lifecycle/README.zh-CN.md) |
| F09 | [research.families.f09_inventory_lifecycle_action_uplift](families/f09_inventory_lifecycle_action_uplift/README.zh-CN.md) |
| F10 | [research.families.f10_live_replay_attribution](families/f10_live_replay_attribution/README.zh-CN.md) |
| SYS | [research.system_engineering](system_engineering/README.zh-CN.md) |

## 实施与证据

实验创建和收尾遵循[最小归档标准](governance/EXPERIMENT_ARCHIVE.zh-CN.md)：五类必留证据、共享依赖不重复整套复制、明确清理门槛、经济与存储状态分开。旧冻结路径原地编目；完成盘点不等于最小保留验收通过。

[输入指南](INPUT_MIGRATION.zh-CN.md)说明已实现 API 与剩余边界；[注册表](registry.json)提供包路径。共享 data、replay、strategy、governance 代码继续复用，不复制解析器或撮合器。参数、动作改变后必须重新生成相应策略路径；单纯归因和作图复用同一有效轨迹。

[公私证据布局](../docs/public_private_documentation_contract.zh-CN.md#证据归属与本地目录)规定私有数据、模型及结果不随源码发布。改写前完整树保留一次私有 Git bundle；历史 docs 暂只读保留，待按问题合并，不再以旧数值和状态主导导航。新问题不继承旧排名，也不把已看过的最终时期包装成新 holdout。
