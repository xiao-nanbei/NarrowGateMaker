# NarrowGate BTCUSDC 代码架构 — 2026-07-05

[English](codebase_architecture_20260705.md) | [简体中文](codebase_architecture_20260705.zh-CN.md)

Last materially modified: 2026-09-06

Last materially synchronized: 2026-09-06

当前布局由 2026-07-29 迁移建立，并于上述维护日期复核：家族源码与证据统一位于 `research/` 子树，由 `research/registry.json` 登记。历史家族路径和原根目录 `research_*` package 已删除，不保留别名。活跃命令使用规范的 `research.families.*` package；`research/governance/` 版本清单与 archive 保留两次迁移边界。共享 replay/governance 实现仍在其运行时所属 package。新配对工作使用 `build_paired_daily_evidence()`，再交给 `research.families.f01_fixed_parameter_racing.audit.paired_screening`；面板转换由 `models.audit.panel_promotion_controller` 管理。`paired_daily_selection()` 仅为兼容接口，没有独立排名或晋级权限。

本文定义代码与研究工件归属，目的是让公开源码仓库可理解：可运行代码位于稳定模块，公共研究说明在所属单元 `docs/`，生成或私有证据留在公共树之外。一次性脚本不应变成永久公共入口。

## 顶层职责

| 路径 | 职责 | 说明 |
| --- | --- | --- |
| `live/` | 实盘进程、配置加载、进程入口 | live 安全与运维检查在此，不添加研究 sweep。 |
| `live/orderbook/` | 运行时执行市场公共订单簿重建 | 只保存 snapshot/diff 序列状态，不存历史数据。 |
| `execution/` | 本系统订单附属状态 | 活跃订单深度路径和队列边界，不负责交易所 transport。 |
| `strategy/` | live/replay 共享策略逻辑 | 报价、maker policy、signal 与 inventory 状态。 |
| `models/` | 离线训练、Python 参考 replay、规范研究 runner | 不放 Markdown 报告、生成结果、一次性 bucket 脚本。 |
| `models/audit/` | 统一审计 package | campaign、成交/订单共同分母、订单评分和逐日检查。 |
| `research/families/` | 十个家族研究工作区 | 源码在家族根/`audit/`，证据在 `docs/`；权限属于具体实验。 |
| `research/shared/` | 共享层归属索引 | D/R/S/G 实现在运行时 package，不复制到家族。 |
| `research/system_engineering/` | 性能、时间、transport 与部署证据 | 不授予 alpha 或 live 晋级资格。 |
| `research/governance/` | registry 迁移解析和不可变布局 archive | 不存 import alias、重复源码或兼容 symlink。 |
| `features/` | 特征工程与预处理 | 训练/replay 特征构造放在此，不放临时模型脚本。 |
| `data/` | 离线下载、导入、标准化和质量入口 | 仅工具；数据位于 `${NARROWGATE_DATA_ROOT}`，不含 live 状态或策略逻辑。 |
| `cpp/` | pybind11 扩展及 C++ replay/quote/signal | C++ 路径晋级前先验证 Python parity。 |
| `bench/` | 本地/系统 benchmark | 必须区分 synthetic 与 live soak。 |
| `tests/` | 单元/parity/回归测试 | live/replay 行为改变时添加相应覆盖。 |
| `docs/` | 架构、计划、历史审计说明与证据汇总 | Markdown 不放在 `models/`。 |
| `logs/`, `results/` | 本地生成输出 | 日志不是源码。 |

## 规范离线入口

- `models/backtest_tick.py`：Python 参考 tick replay。
- `research/families/f01_fixed_parameter_racing/campaign_outcome_replay_audit.py`：campaign arm 评估/标签，支持 `--arm-spec-json`。
- `research/families/f01_fixed_parameter_racing/parameter_racing_sweep.py`：参数覆盖、quick-smoke、quick-full-main-effect、冻结验证和候选交付。
- `research/families/f01_fixed_parameter_racing/parameter_selection.py`：参数登记、arm 生成与配对证据；旧 selector 仅为兼容。
- `research/families/f01_fixed_parameter_racing/audit/paired_screening.py`：新参数研究的规范配对筛选/排名层。
- `models/audit/experiment_scorecard.py`：配对与 action/OPE 的版本化检查和加权评分。
- `models/audit/panel_promotion_controller.py`：独立的 Development/Validation/holdout 转换控制器，信息不足时拒绝推进，不授予自动 live 晋级。
- `models/alpha_evidence_ledger.py`：replay/live 机制检查后，成交级 alpha 证据的首选入口。
- `python -m research.families.f10_live_replay_attribution.audit.runner`：常规归因报告。
- `python -m research.families.f05_fill_quality_quote_ev.audit.order_score_fast`：大规模保留面板的评分检查。
- `python -m research.families.f05_fill_quality_quote_ev.audit.fill_selection_score`：订单共同分母上的按日隔离 OOS 校准；这是 quote-time 非有毒成交评分的证据/校准工具，不是 live policy 入口。

## 已删除或归档入口

下列名称不是当前可运行入口：

- `models/stage_t_*.py`：2026-07-05 删除，修复前结论也删除。类似报告应通过 `models/audit/` 或 `models/alpha_evidence_ledger.py` 的因果报告实现。
- `models/live_campaign_audit.py`：已删除，使用 `research.families.f10_live_replay_attribution.audit.runner`。
- `models/live_episode_replay_compare.py`：已删除，使用统一 live/replay 归因报告。
- `models/live_offline_feature_audit.py`：已删除，必要时向 `models/audit/` 添加特征检查。
- `models/local_liquidity_mechanism_audit.py`：2026-07-05 删除，使用 `python -m research.families.f10_live_replay_attribution.audit.runner --reports local_liquidity_mechanism`，输入订单表或 replay orders/fills。
- `models/response_kernel_audit.py`、`models/ou_reversion_bucket_audit.py`、`models/absorptive_capacity_audit.py`、`models/xmarket_as_moderator_audit.py`、`models/alpha_bucket_intersection.py`：2026-07-05 删除，其共享 response/half-life/absorptive-capacity/xmarket-moderator 交集输出已合并到上述 `local_liquidity_mechanism` 报告。

除非本文、README 或 `project.md` 明确列为规范入口，`*_shadow_*`、`*_bucket_*` 或实验性 alpha audit 均为原型/专项工具。输出可作为历史证据，但不是直接 live 晋级证明。

## 新研究放在哪里

1. 原位扩展所属的已登记 `research/families/fXX_*/` package；可复用工程研究放在 `research/system_engineering/`。不要新建根目录 `research_*` package 或恢复旧别名。真正共享实现放入现有运行时所属 package，在 `research/shared/` 记录归属。
2. 可复用输出可向 `research/families/f10_live_replay_attribution/audit/runner.py` 添加 CLI 报告。
3. 参数选择应表达为 `campaign_outcome_replay_audit.py` 消费的 arm spec，构建配对逐日证据，经 `paired_screen_v2` 排名，不新增自定义 selector 或 sweep 专用晋级规则。
4. Alpha 发现从 `alpha_evidence_ledger.py` 或 `order_level` 证据表开始。Bucket 是诊断切片，不是 policy。
5. 家族结论在所属 `docs/`，生成 CSV 在 `${NARROWGATE_RESULTS_DIR}`。旧路径仅作为现有迁移记录或原始 Git 历史中的来源信息，不在 HEAD 保留重复文件/兼容 symlink。精确历史重跑使用原源码/archive，当前工作使用规范 import。

## 删除规则

研究脚本在全部条件满足时可以删除：

- 规范代码或测试不再导入；
- 历史结论已记录在 `docs/` 或生成结果中；
- 本文、README 或 `project.md` 不再将其列为规范入口；
- 未来重跑更适合通过 `models/audit/`、`alpha_evidence_ledger.py` 或 `campaign_outcome_replay_audit.py` 实现。

`__pycache__`、`.pytest_cache` 等生成 cache 不应保存在项目树中。
