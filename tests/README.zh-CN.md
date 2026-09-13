# 回归测试范围

[English](README.md) | [简体中文](README.zh-CN.md)

Last materially synchronized: 2026-09-19

当前回归测试应验证仍维护的实现行为，不能把历史报告中的字面声明当成今天实现正确的证明。退役纯文档断言不改变历史证据及其研究使用限制。研究族关闭、文件版本后缀或曾导致 CI 失败，都不是删除依据。

## 第一批有界退役

移除三个纯文档模块，共五个测试函数：`test_ber_guard_role_safe_add_only_current_stack_owner_v1_1.py`、`test_buy_q90_dual_clock_terminal_routing_contract_v2.py`、`test_buy_q90_runtime_authority_contract_v3.py`。其中固定 JSON 标志及英文文案不再作为日常软件回归要求。其他测试没有导入其辅助函数，既有历史可用性清单也未列入它们。没有删除生产行为测试，也不宣称新增了行为覆盖。

原始记录保持不变：[F09 执行勘误](../research/families/f09_campaign_action_uplift/docs/ber_guard_role_safe_add_only_current_stack_owner_v1_execution_estimand_errata_v1_20260808.json)、[F10 双时钟实施记录](../research/families/f10_live_replay_attribution/docs/buy_q90_dual_clock_terminal_routing_contract_v2_implementation_20260802.json)、[F10 运行授权实施记录](../research/families/f10_live_replay_attribution/docs/buy_q90_runtime_authority_contract_v3_implementation_20260802.json)。退役不改写历史结论，也不授予当前运行权限。

混合 ABI／历史测试、时标合同完整性、预测兼容接口、启动／重载拒绝、原生一致性、下载器迁移及 Studio 经济完整性测试仍保留。进一步合并前，需要明确保留哪些行为及其目标测试。历史复现继续使用[既有可用性清单](fixtures/public_clone_historical_test_availability.json)和 `NARROWGATE_RUN_HISTORICAL_REPRODUCTION_TESTS=1`，不新增第二份排除名单。移除这五项静态断言不代表已经测得 CI 提速。
