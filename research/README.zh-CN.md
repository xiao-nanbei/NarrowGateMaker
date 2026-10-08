# 407 日研究体系

[English](README.md) | [简体中文](README.zh-CN.md)

Last materially modified: 2026-10-08

Last materially synchronized: 2026-10-08

当前研究入口：[B0_RESPONSE 响应动作价值完整报告](families/f08_side_taker_lifecycle/response_baseline.zh-CN.md)。204 个独立账户／407 日经济执行与本机身份验收完成，净 PnL -1603.031182 USDC，仍为亏损；不是连续 407 日或全样本外验证。47 特征冻结模型、39 特征信息消融、Final 单次复用和 live 验收边界分别列明。它研究既有执行框架中的旧单动作，不是 U1 评价时机研究的成功或历史 hazard 结论的改写。

当前入口是[统一重算实施计划](RECOMPUTE_407.zh-CN.md)和[12 包任务清单](recompute_407.json)。按科学问题重算，不按旧版本逐次复演；历史 closed/exhausted/promotion 不阻止新研究。previous-use、真实输入限制及独立账户边界不因此解除。

新链路 [F03](families/f03_causal_13_head/README.zh-CN.md) 已完成全部 858 次策略分片，包含后来批准的 ML-OFF。Final 净收益 H=inf 为 −189.212148 USDC，ML-OFF 为 −269.042233 USDC，两者均亏损；保留 previous-use 及中途查看结果的限制。其他包的集成与执行状态以任务清单为准；F03 完成不代表全体系、407 日参考数据验收或镜像完成。

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

[输入指南](INPUT_MIGRATION.zh-CN.md)说明已实现 API 与剩余边界；[注册表](registry.json)提供包路径。共享 data、replay、strategy、governance 代码继续复用，不复制解析器或撮合器。参数、动作改变后必须重新生成相应策略路径；单纯归因和作图复用同一有效轨迹。

[公私证据布局](../docs/public_private_documentation_contract.zh-CN.md#证据归属与本地目录)规定私有数据、模型及结果不随源码发布。改写前完整树保留一次私有 Git bundle；历史 docs 暂只读保留，待按问题合并，不再以旧数值和状态主导导航。新问题不继承旧排名，也不把已看过的最终时期包装成新 holdout。
