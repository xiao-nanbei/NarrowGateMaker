# 架构与模块归属

[English](architecture.md) | [简体中文](architecture.zh-CN.md)

Last materially modified: 2026-09-21

Last materially synchronized: 2026-09-21

本文是唯一当前架构说明。私有仓库是开发源，公有仓库分发经审阅的源码，不维护竞争实现。模型权重、购买行情和私有运行证据不进入版本化源码，见[公私合同](public_private_documentation_contract.zh-CN.md)。

## 模块职责

| 归属 | 职责 |
| --- | --- |
| `data/`、`data/downloaders/` | 获取、绑定来源的事实、因果观察与输入验证，不保存实盘状态 |
| `features/` | 共享特征工程，包括 `quote_ev.py` |
| `narrowgate/runtime/` | 共享 epoch 身份与运行状态恢复 |
| `strategy/` | 实盘／回放的报价、信号、策略和库存逻辑 |
| `execution/` | 自有订单生命周期、深度路径和队列边界，不负责交易所传输 |
| `live/`、`live/orderbook/` | 实盘进程／配置与执行市场订单簿重建 |
| `models/`、`models/replay/` | 过渡期训练／回放包、tick 执行器、队列、窗口与记账 |
| `models/audit/` | 现有共享审计消费者，不是新增研究族工作的默认归属 |
| `research/families/` | 研究族模型、实验与公开说明 |
| `research/shared/` | 共享层归属索引，实现仍由运行模块维护 |
| `research/system_engineering/`、`research/governance/` | 工程研究、实验治理与布局归档 |
| `narrowgate/`、`frontend/` | 命令与界面，尚非唯一核心包 |
| `cpp/`、`bench/`、`tests/` | 原生实现、明确标注的基准与行为／一致性回归 |
| `scripts/`、`docs/` | 维护／部署工具与当前指南 |

行情载荷、生成日志与结果不是源码。存储和获取命令统一由[数据指南](../data/README.zh-CN.md)维护；研究证据归所属登记单元。

## 依赖方向

研究使用共享数据、特征和运行契约；新增运行契约不能依赖研究族实现。F05 可为既有接口重新导出共享特征，但不能保留第二套实现。事实与策略可见观察、预测与动作、资金费结算与特征保持分离。实盘、训练和回放保留独立状态生命周期；原生路径须验证固定输入下的 Python 一致性。

原位扩展已登记研究族，不恢复已删除的根 `research_*` 别名、软链接或重复源码树。同名文件不代表相同语义。现有共享审计／治理消费者是过渡结构，不是将全部研究搬入该处的理由。

## 维护入口

- [数据](../data/README.zh-CN.md)：`python -m data` 与显式历史适配器。
- [模型](../models/README.zh-CN.md)：`models/backtest_tick.py` 是 Python 参考执行器，不再并入无关工具。
- [研究](../research/README.zh-CN.md)：[407日清单](../research/recompute_407.json)及选定实验定义当前输入、方法和权限；旧关闭状态不阻止新研究。
- F01 参数工具：`research/families/f01_fixed_parameter_racing/` 下的 `campaign_outcome_replay_audit.py`、`parameter_racing_sweep.py`、`parameter_selection.py`。既有配对实验使用 `build_paired_daily_evidence()` 与 `audit/paired_screening.py`；`paired_daily_selection()` 仅供兼容。
- 现有共享检查：`models/audit/experiment_scorecard.py` 与 `panel_promotion_controller.py`；不授予实盘权限，也不向所有新研究强加同一种 campaign 合同。
- 现有归因／诊断：`models/alpha_evidence_ledger.py`、`research.families.f10_live_replay_attribution.audit.runner` 和 F05 `audit.order_score_fast`／`audit.fill_selection_score`。诊断分桶和评分不是策略或部署证据。
- [贡献检查与 CI](dev/ci.zh-CN.md)：本地验证与托管检查职责。

## 已完成迁移

八个获取适配器已位于 `data/downloaders/`，旧文中“尚未提交”的描述已失效。`data/facts.py`、`data/observation.py` 和 `data/runtime.py` 已作为共享输入基础设施存在；文件存在不证明真实数据全量验证完成。Epoch 契约已迁入 `narrowgate/runtime/`，标量 quote-EV 特征已迁入 `features/quote_ev.py`。研究族使用规范 `research.families.*` 包。

历史删除清单与迁移身份留在 `research/governance/` 和保留的私有快照中，不再维护第二份当前架构手册。被替代的架构／归属指南可从已验证的收敛前源码包恢复（私有证据库，不随仓库分发）。

## 未完成迁移与删除边界

执行器拆分、治理归属整合、部署／维护脚本分离、整体 `src/narrowgate` 收敛和根手册精简仍未完成。文档清理不能顺带移动活跃研究代码或拆分巨型执行器；后续抽取需要明确职责，并验证固定输入下报价、订单、库存和记账一致性。

删除历史入口前，检查导入、命令／脚本／测试消费者及冻结身份，并确认真实可恢复的源码／材料归档。可变 alpha 提交不保证可恢复。历史证据保持原样，不用今天源码替换其哈希，也不添加缺文件跳过。生成缓存和私有结果不属于本次收敛的清理对象。
