# F01 固定参数竞赛

[English](README.md) | [简体中文](README.zh-CN.md)

Last materially synchronized: 2026-09-06

文档边界：本 README 和本单元已跟踪的 `docs/` 为公开文档。Owner 工件位置、未公开证据
索引与私有研究上下文由本单元忽略的 `private/` 目录管理，不随公共仓库发布。
参见[公开/私有研究布局](../../PRIVATE_EVIDENCE.md)。

状态：作为 alpha 研究族已关闭。保留的 runner 只有筛选作用，使用统一配对 scorecard。
根目录 Python 文件提供参数竞赛与 campaign replay 入口；`audit/` 包含配对筛选实现；
`docs/` 保存关闭结论与架构记录。共享依赖：D、R、S、G。

## 连续 baseline 与资金费核算

现有 `campaign_outcome_replay_audit` runner 的 `--continuous` 支持连续 UTC `--days`，
使用 `--engine python --workers 1`。它只拼接行情输入，每个 arm 调用现有 simulator 一次。
首日保留因果预热；后续日期不重置订单、排队状态、cooldown、campaign 或风险状态。
不连续日期不能当成连续账户。此路径不声称完整 C++ scheduler 已获验证，也不声称精确
恢复历史 live 初态。外部 reference/repair 窗口拼接尚未实现，不能静默丢弃这些输入。

`--funding-history <frozen-fundingRate-json>` 按每个 arm 在交易所成交时钟上的实际模拟
持仓计入线性合约结算。文件包含有序 Binance `symbol`、`fundingTime`、`fundingRate` 和
结算 `markPrice`，应在执行前下载覆盖完整研究区间的数据。Runner 不在 replay 中下载
费率。正费率由多头支付、空头收取；结算不计为成交。同毫秒时按明确模型约定先结算再
处理成交；输出记录这种时间碰撞数量，不声称它是交易所精确顺序。

资金费进入 campaign value 和 `replay_net_pnl`；原字段 `replay_pnl` 仍表示 simulator
扣除交易手续费后的交易损益。资金费 CSV 逐次记录结算、仓位与现金流。此做法保留当前
live 风控/cooldown 使用交易损益的口径，不借核算修复改变策略。尚未模拟资金费引起的
保证金或权益风控反馈。没有输入资金费时标记为 `unmodeled`，不是已验证的零成本。

连续模式下，原名 `daily.csv` 的每行代表整个区段，通过
`accounting_window=continuous_segment`、`window_end_day` 和 `window_day_count` 明示。
它不是供 bootstrap 或选择使用的单日观测。不能与 daily fresh-start 统计混用，也不能
复用旧延迟环境下的 baseline。
