# 架构与模块归属

[English](architecture.md) | [简体中文](architecture.zh-CN.md)

Last materially modified: 2026-09-26

Last materially synchronized: 2026-09-26

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
- F01 当前公共输入入口：[`public_input.py`](../research/families/f01_fixed_parameter_racing/public_input.py) 的 `iter_parameter_candidates()` 独立运行各臂，`replay_economic_candidates()` 添加完整统一结算。`campaign_outcome_replay_audit.py`、`parameter_racing_sweep.py`、`parameter_selection.py` 及 `build_paired_daily_evidence()`／`audit/paired_screening.py` 按各自历史或特定执行合同使用，不能把旧默认数据、参数或 `paired_daily_selection()` 当成当前入口。
- 现有共享检查：`models/audit/experiment_scorecard.py` 与 `panel_promotion_controller.py`；不授予实盘权限，也不向所有新研究强加同一种 campaign 合同。
- 现有归因／诊断：`models/alpha_evidence_ledger.py`、`research.families.f10_live_replay_attribution.audit.runner` 和 F05 `audit.order_score_fast`／`audit.fill_selection_score`。诊断分桶和评分不是策略或部署证据。
- [贡献检查与 CI](dev/ci.zh-CN.md)：本地验证与托管检查职责。

## 已完成迁移

八个获取适配器已位于 `data/downloaders/`，旧文中“尚未提交”的描述已失效。`data/facts.py`、`data/observation.py` 和 `data/runtime.py` 已作为共享输入基础设施存在；文件存在不证明真实数据全量验证完成。Epoch 契约已迁入 `narrowgate/runtime/`，标量 quote-EV 特征已迁入 `features/quote_ev.py`。研究族使用规范 `research.families.*` 包。

历史删除清单与迁移身份留在 `research/governance/` 和保留的私有快照中，不再维护第二份当前架构手册。被替代的架构／归属指南可从已验证的收敛前源码包恢复（私有证据库，不随仓库分发）。

## 未完成迁移与删除边界

### 回放恢复范围

维护中的 Python ConsumerBundle 入口 `simulate_prepared_inputs` 接受 `checkpoint_at_ts_ms` 和 `resume_checkpoint`。`models/replay/runtime_checkpoint_io.py` 保存自有账户、订单、队列、RNG 和策略对象图；准备输入入口绑定输入 manifest、有效参数、数值预测及执行归属源码。输出目录和进度回调属于进程，不是经济输入。恢复使用保存的策略，而非新初始化的替代对象。检查点是可信本地实现状态，不是可移植产物，也不是允许网页上传的格式。

`ReplayL2Journal` 封存不可变前缀而不伪造账户结束，再把核验后的记录复制到独立分支写入器，保留原逻辑事件身份。每个分支继续独立的生产、提交和读回计数。测试覆盖持久化独立分支、公开 F06/F07 策略状态、信号冷启动、异步执行、在途订单转换和 UTC 会计边界。源码、输入、参数不匹配及日志前缀变化会报错，不作兼容回退。退役 Makefile 包装已移除；安装后的 `narrowgate data` 和 `narrowgate replay` 是对应命令入口。

一个已评价过的完整两日开发 F05 账户在中间 UTC 零点保存，并由新进程恢复。全部 1,082 条成交、452,728 条 L2 记录、策略计数、UTC 估值及完整会计均与保留的从头运行基准精确一致，净 PnL 差额为零。这是一次工程回放，没有新增拟合或候选。绑定源码的回执登记在[现有工作清单](../research/recompute_407.json)；底层输入和回执为私有、不随仓库分发。十项旧加载器夹具失败现已通过退出旧加载／重载链、迁移到正式入口拒绝及组件不变性测试闭环。本轮定向集 535 项通过，不代表全仓所有测试通过。

原生 cooldown 支持完整值状态导出／恢复，绑定配置、二进制和策略源码，并重建进程锁。用户明确授权的开发072独立工程配置仅改变两个冷却启用标志，保留原系数轴、ML 参数和 F05 动作。新跑的完整不中断账户与 UTC 日界新进程恢复账户精确一致：980 条成交、419,755 条 L2 事件、490 次冷却决策、库存、费用、资金费、UTC 会计及净 PnL，差额为零。截断时保留非零库存、活动订单、未结束的 SELL 冷却及未闭合原生窗口；处理下一事件前的导出状态精确一致。真实机器耗时统计单列。公共检查点入口现绑定冷却不可变输入，不再尝试把运行对象直接序列化为 JSON；30 项定向测试通过，含两项新增用例。这不证明冷却盈利、任意跨版本恢复、完整 C++ tick 循环、所有文件系统故障或 F06/F07 科学研究完成。延迟可变方差输出仍不支持检查点；冻结环境、工作预算和计算时窗继续适用。

隔离 Linux 候选复用已核验构建、正式安装环境及此前270项测试证据。用户选定的新 Tardis 系数轴0.05／不对称强度0.1配置，已通过真实行情经 MakerEngine、共享特征、全部13头、P3、原生报价到无交易能力意图记录器的组装。两次预热完成决策与四条意图和本机参考精确一致，仅锁等待／持有耗时不同。真实 Tardis 观察采用明确声明的模拟有序传输，不是已证明交易所原生接收／序列一致。这是候选组装验收，不是激活、盈利或全面行情覆盖；current、服务和生产配置未改动。绑定源码的私有回执在现有工作清单登记，不随仓库分发；独立从头参数研究不受无关工程待办阻塞。

执行器拆分、治理归属整合、部署／维护脚本分离、整体 `src/narrowgate` 收敛和根手册精简仍未完成。文档清理不能顺带移动活跃研究代码或拆分巨型执行器；后续抽取需要明确职责，并验证固定输入下报价、订单、库存和记账一致性。

删除历史入口前，检查导入、命令／脚本／测试消费者及冻结身份，并确认真实可恢复的源码／材料归档。可变 alpha 提交不保证可恢复。历史证据保持原样，不用今天源码替换其哈希，也不添加缺文件跳过。生成缓存和私有结果不属于本次收敛的清理对象。
