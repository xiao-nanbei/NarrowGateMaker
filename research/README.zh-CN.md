# 407 日研究体系

[English](README.md) | [简体中文](README.zh-CN.md)

Last materially modified: 2026-09-22

Last materially synchronized: 2026-09-22

当前入口是[统一重算实施计划](RECOMPUTE_407.zh-CN.md)和[12 包任务清单](recompute_407.json)。按科学问题重算，不按旧版本逐次复演；历史 closed/exhausted/promotion 不阻止新研究。previous-use、真实输入限制及独立账户边界不因此解除。

新链路 [F03](families/f03_causal_13_head/README.zh-CN.md) 已完成全部 858 次策略分片，包含后来批准的 ML-OFF。Final 净收益 H=inf 为 −189.212148 USDC，ML-OFF 为 −269.042233 USDC，两者均亏损；保留 previous-use 及中途查看结果的限制。其他包的集成与执行状态以任务清单为准；F03 完成不代表全体系、407 日参考数据验收或镜像完成。

## 当前研究族

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
| F09 | [research.families.f09_campaign_action_uplift](families/f09_campaign_action_uplift/README.zh-CN.md) |
| F10 | [research.families.f10_live_replay_attribution](families/f10_live_replay_attribution/README.zh-CN.md) |
| SYS | [research.system_engineering](system_engineering/README.zh-CN.md) |

## 实施与证据

[输入指南](INPUT_MIGRATION.zh-CN.md)说明已实现 API 与剩余边界；[注册表](registry.json)提供包路径。共享 data、replay、strategy、governance 代码继续复用，不复制解析器或撮合器。参数、动作改变后必须重新生成相应策略路径；单纯归因和作图复用同一有效轨迹。

[公私证据布局](../docs/public_private_documentation_contract.zh-CN.md#证据归属与本地目录)规定私有数据、模型及结果不随源码发布。改写前完整树保留一次私有 Git bundle；历史 docs 暂只读保留，待按问题合并，不再以旧数值和状态主导导航。新问题不继承旧排名，也不把已看过的最终时期包装成新 holdout。
