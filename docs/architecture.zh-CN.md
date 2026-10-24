# 架构与模块归属

[English](architecture.md) | [简体中文](architecture.zh-CN.md)

Last materially modified: 2026-10-24

Last materially synchronized: 2026-10-24

本文是唯一当前架构说明。私有仓库是开发源，公有仓库分发经审阅的源码，不维护竞争实现。模型权重、购买行情和私有运行证据不进入版本化源码，见[公私合同](public_private_documentation_contract.zh-CN.md)。

## 模块职责

| 归属 | 职责 |
| --- | --- |
| `data/`、`data/downloaders/` | 获取、绑定来源的事实、因果观察与输入验证，不保存实盘状态 |
| `features/` | 共享特征工程，包括 `quote_ev.py` |
| `narrowgate/prediction/` | 显式产物路径的P3校验、序列化和推理，不拟合、不导入研究模块 |
| `narrowgate/runtime/` | 共享 epoch 身份与运行状态恢复 |
| `strategy/` | 实盘／回放的报价、信号、策略和库存逻辑 |
| `execution/` | 自有订单生命周期、深度路径和队列边界，不负责交易所传输 |
| `live/`、`live/orderbook/` | 实盘进程／配置与执行市场订单簿重建 |
| `models/`、`models/replay/` | 过渡期训练／回放包、公开tick装配、原生循环适配、队列、窗口与记账 |
| `narrowgate/replay/executor/` | Python经济执行器、运行状态归属、有序事件阶段与显式回放适配 |
| `models/audit/` | 现有共享审计消费者，不是新增研究族工作的默认归属 |
| `research/families/` | 研究族模型、实验与公开说明 |
| `research/shared/` | 共享层归属索引，实现仍由运行模块维护 |
| `research/system_engineering/`、`research/governance/` | 工程研究、实验治理与布局归档 |
| `narrowgate/`、`narrowgate/replay/` | 命令行、本机解析后运行计划、完整有限账户执行/控制与共享运行契约，尚非唯一核心包 |
| `cpp/`、`bench/`、`tests/` | 原生实现、明确标注的基准与行为／一致性回归 |
| `scripts/`、`docs/` | 维护／部署工具与当前指南 |
| `narrowgate/maintenance/` | 终止对象运行锁退役及原本机文件索引／搜索，不调度任务、不操作 live |
| `tools/poolrun/` | 独立安装的通用有限批次调度器；不会默认接管研究或实盘任务 |

行情载荷、生成日志与结果不是源码。存储和获取命令统一由[数据指南](../data/README.zh-CN.md)维护；研究证据归所属登记单元。

## 依赖方向

研究使用共享数据、特征和运行契约；新增运行契约不能依赖研究族实现。F05 可为既有接口重新导出共享特征，但不能保留第二套实现。事实与策略可见观察、预测与动作、资金费结算与特征保持分离。实盘、训练和回放保留独立状态生命周期；原生路径须验证固定输入下的 Python 一致性。

P3推理只由`narrowgate/prediction/touch_probability.py`维护。MakerEngine、回放配置和部署预检直接使用它，不导入F02校准脚本、不加载其训练数据路径。核心仍使用原NumPy/SciPy预测运算，并非移除了SciPy的间接依赖。F02的`touch_probability.py`保留离线拟合，以及兼容历史按交易对解析的`load()`/`save()`默认路径和可信类定位的薄子类；全部校验和预测方法继承共享实现。核心要求显式路径，不选择训练目录。MakerEngine原有可选目录适配仍在核心外解析既有模型目录默认值。本次抽取不迁移冻结release，也不允许新release盲读旧断点。

P3抽取的受影响检查为119项加扩展51项通过（其中四项归属检查重复）；既有历史可用性规则仍排除两项仅所有者可用的冻结F02规格测试，不将它们计作通过。与抽取前实现比较，两种经验/SU模型各100,004个距离输入的预测值、斜率/最优距离和序列化字节完全一致。新构建的630条目wheel已临时安装，在仓库外禁止研究模块导入后仍可推理，包内排除private/test。此检查复用现有独立Python依赖，不等于干净Linux/native release验收。临时构建/安装产物已删除；没有拟合、启动新行情回测或部署。

订单生命周期日志只维护 `execution/order_lifecycle_journal_v2.py` 的一套事件／游标实现，以及 `models/replay/order_lifecycle_v2_replay_adapter.py` 的一套回放回调实现。严格原生模块只扩展来源与字段，不再复制整个引擎。当前机制诊断也输出 v2 行，明确标记为诊断，而不是已准入的原生证据。`order_lifecycle_journal.py` 只为历史读取和回归样本保留旧 v1 快照结构与构造函数，新生产入口不再导入它。已有 v1 文件不会混写 v2 列或冒充 v2 证据；冻结实验包保留原始字节。

逐回调落盘与按日缓冲的日志写入器，共用 `execution/order_lifecycle_journal_writer_v2.py` 中一套无副作用的批次准入逻辑，但保留不同的发布和恢复边界。`LifecycleJournalCommitResult.commit_scope` 区分 `callback_durable` 与 `memory_buffer`，旧 `status="committed"` 字段本身不代表已持久化。MakerEngine 在组装时绑定生命周期提交接口：由中央证据队列直接提交，或由独立生命周期写入器入队，不再在每次回调中选择回退协议。只读的 `models/replay/order_exposure.py` 仍是单一有效时钟、左闭右开区间的摘要，不替代区分交易所／可见时钟的数量加权状态机。

原位扩展已登记研究族，不恢复已删除的根 `research_*` 别名、软链接或重复源码树。同名文件不代表相同语义。现有共享审计／治理消费者是过渡结构，不是将全部研究搬入该处的理由。F05 ADD/WAIT 研究在原有 `audit/multiscale_ema_add_wait_incremental_value.py` 中共用仅基于训练集的变换、拟合、聚类区间和声明 OOF 覆盖校验。历史入口仍保留原规格、特征列表、制品身份和执行编排；其中的覆盖数量不是全局回测配额。

## 维护入口

- [本机维护](maintenance.zh-CN.md)：`narrowgate maintenance` 预览或退役已核实的终止对象运行锁；仅实际清理后或明确请求时刷新原文件索引。`narrowgate/maintenance/` 统一拥有该实现，不调度任务、不操作 live。

- [PoolRun Lite](../tools/poolrun/README.zh-CN.md)：可选的 Python 3.12+ 子项目，拥有独立环境、`poolrun` CLI、测试和合成示例，不进入 NarrowGate 默认安装；生产适配器和跨主机验收仍需操作者负责。
- [数据](../data/README.zh-CN.md)：`python -m data` 与显式历史适配器。
- [模型](../models/README.zh-CN.md)：`models/backtest_tick.py`保留公开输入／参数／服务装配及原生适配；Python事件执行器归`narrowgate/replay/executor/`。不向兼容入口添加第二套循环。
- [研究](../research/README.zh-CN.md)：[407日清单](../research/recompute_407.json)及选定实验定义当前输入、方法和权限；旧关闭状态不阻止新研究。
- F01 当前公共输入入口：[`public_input.py`](../research/families/f01_fixed_parameter_racing/public_input.py) 的 `iter_parameter_candidates()` 独立运行各臂，`replay_economic_candidates()` 添加完整统一结算。`inventory_lifecycle_outcome_replay_audit.py`、`parameter_racing_sweep.py`、`parameter_selection.py` 及 `build_paired_daily_evidence()`／`audit/paired_screening.py` 按各自历史或特定执行合同使用，不能把旧默认数据、参数或 `paired_daily_selection()` 当成当前入口。
- 现有共享检查：`models/audit/experiment_scorecard.py` 与 `panel_promotion_controller.py`；不授予实盘权限，也不向所有新研究强加同一种 库存生命周期 合同。
- 现有归因／诊断：`models/alpha_evidence_ledger.py`、`research.families.f10_live_replay_attribution.audit.runner` 和 F05 `audit.order_score_fast`／`audit.fill_selection_score`。诊断分桶和评分不是策略或部署证据。
- [贡献检查与 CI](dev/ci.zh-CN.md)：本地验证与托管检查职责。

## 共享输入与模块归属

八个获取适配器位于 `data/downloaders/`。`data/facts.py`、`data/observation.py` 和 `data/runtime.py` 提供共享输入基础设施；文件存在不证明真实数据全量验证完成。`narrowgate/runtime/` 管理 epoch 契约，`features/quote_ev.py` 管理标量 quote-EV 特征。研究族使用 `research.families.*` 包。

## 执行支持范围

### 回放恢复范围

维护中的 Python ConsumerBundle 入口 `simulate_prepared_inputs` 接受 `checkpoint_at_ts_ms` 和 `resume_checkpoint`。`models/replay/runtime_checkpoint_io.py` 保存自有账户、订单、队列、RNG 和策略对象图；准备输入入口绑定输入 manifest、有效参数、数值预测及执行归属源码。输出目录和进度回调属于进程，不是经济输入。恢复使用保存的策略，而非新初始化的替代对象。检查点是可信本地实现状态，不是可移植产物，也不是允许网页上传的格式。

`ReplayL2Journal` 封存不可变前缀而不伪造账户结束，再把核验后的记录复制到独立分支写入器，保留原逻辑事件身份。每个分支继续独立的生产、提交和读回计数。测试覆盖持久化独立分支、公开 F06/F07 策略状态、信号冷启动、异步执行、在途订单转换和 UTC 会计边界。源码、输入、参数不匹配及日志前缀变化会报错，不作兼容回退。退役 Makefile 包装已移除；安装后的 `narrowgate data` 和 `narrowgate replay` 是对应命令入口。

一个已评价过的完整两日开发 F05 账户在中间 UTC 零点保存，并由新进程恢复。全部 1,082 条成交、452,728 条 L2 记录、策略计数、UTC 估值及完整会计均与保留的从头运行基准精确一致，净 PnL 差额为零。这是一次工程回放，没有新增拟合或候选。绑定源码的回执登记在[现有工作清单](../research/recompute_407.json)；底层输入和回执为私有、不随仓库分发。十项旧加载器夹具失败现已通过退出旧加载／重载链、迁移到正式入口拒绝及组件不变性测试闭环。本轮定向集 535 项通过，不代表全仓所有测试通过。

原生 cooldown 支持完整值状态导出／恢复，绑定配置、二进制和策略源码，并重建进程锁。用户明确授权的开发072独立工程配置仅改变两个冷却启用标志，保留原系数轴、ML 参数和 F05 动作。新跑的完整不中断账户与 UTC 日界新进程恢复账户精确一致：980 条成交、419,755 条 L2 事件、490 次冷却决策、库存、费用、资金费、UTC 会计及净 PnL，差额为零。截断时保留非零库存、活动订单、未结束的 SELL 冷却及未闭合原生窗口；处理下一事件前的导出状态精确一致。真实机器耗时统计单列。公共检查点入口现绑定冷却不可变输入，不再尝试把运行对象直接序列化为 JSON；30 项定向测试通过，含两项新增用例。这不证明冷却盈利、任意跨版本恢复、完整 C++ tick 循环、所有文件系统故障或 F06/F07 科学研究完成。延迟可变方差输出仍不支持检查点；冻结源码身份、已授权研究范围和实际资源／并发保护继续适用；不设回测次数、候选数量或累计算力配额。

隔离 Linux 候选复用已核验构建、正式安装环境及此前270项测试证据。用户选定的新 Tardis 系数轴0.05／不对称强度0.1配置，已通过真实行情经 MakerEngine、共享特征、全部13头、P3、原生报价到无交易能力意图记录器的组装。两次预热完成决策与四条意图和本机参考精确一致，仅锁等待／持有耗时不同。真实 Tardis 观察采用明确声明的模拟有序传输，不是已证明交易所原生接收／序列一致。这是候选组装验收，不是激活、盈利或全面行情覆盖；current、服务和生产配置未改动。绑定源码的私有回执在现有工作清单登记，不随仓库分发；独立从头参数研究不受无关工程待办阻塞。

Python事件循环和状态归属已经拆出，但大型初始化、终点汇总及研究政策段仍需继续分解。治理归属整合、部署／维护脚本分离、整体`src/narrowgate`收敛和根手册精简也尚未完成。文档清理不能顺带移动活跃研究代码；后续抽取需要明确职责，并验证固定输入下报价、订单、库存和记账一致性。

删除历史入口前，检查导入、命令／脚本／测试消费者及冻结身份，并确认真实可恢复的源码／材料归档。可变 alpha 提交不保证可恢复。历史证据保持原样，不用今天源码替换其哈希，也不添加缺文件跳过。生成缓存和私有结果不属于本次收敛的清理对象。

## 当前架构迁移

用户的四项完成标准是最终范围：回放/live适配明确而共享决策、订单与会计规则；实验只定义政策、配置与方法，不复制执行器或修改源码字符串；旧版本按依赖退役且独有证据可恢复；固定CLI与一份计划支持运行、暂停、恢复、完整报告及release检查/安装。下表是一份迁移计划，不是多个框架或新增审批门。完成一个阶段不代表四项标准全部达成。

| 阶段 | 当前边界 |
| --- | --- |
| M0：实际源码与运行状态 | 在当前未提交工作树上推进，不丢弃已有改动。PoolRun在途release保持原源码与解释器绑定，当前源码测试不升级它们。 |
| M1：手动回放闭环 | 已实现本机单bundle JSON计划、完整产物、合作式暂停与显式磁盘恢复。新计划的PoolRun派发和有界窗口连续账户装配仍待完成。 |
| M2：中性核心与封包 | 有限账户执行和IO从F01迁至`narrowgate/replay/account.py`与`narrowgate/runtime/artifact_io.py`；P3校验/推理从F02迁至`narrowgate/prediction/touch_probability.py`，当前live/回放/部署预检消费者已迁移。历史家族适配只保留路径/API兼容。完整`src/`布局、其余推理归属及可选研究包仍待完成。 |
| M3：执行器状态与循环 | `narrowgate/replay/event_clock.py`管理因果事件构造。`narrowgate/replay/executor/`已接管运行状态、断点捕获／转移、事件驱动及各阶段实现，旧兼容入口中的巨型循环已移除。初始化／终点汇总和政策状态类型化仍未全部完成。 |
| M4：共享live边界 | live与回放使用共享报价数学、政策前数量、改单节流／合并／先撤规则及仓位成交会计。适配器保留各自时钟、账本和数值表示。完整决策／生命周期收敛及安装后Linux验收仍待完成；本次源码重构不升级或重启live。 |
| M5：研究与退役 | 维护中的F01/F04消费者使用新的共享账户/IO归属。历史冻结构建器/release不通过导入当前兼容壳来迁移。政策挂接及完整版本清退仍待完成。 |
| M6：独立release操作 | 统一release构建/检查/安装及干净Linux/native部署验收仍待完成。不把未实现的部署命令写成现有能力。 |

### 当前状态与事件边界

`ReplayServices`在装配时绑定显式进程依赖。`ReplayRuntimeState`管理可保存的账户／订单／FIFO／RNG／政策状态；`ReplayContext`管理不持久化的输入与调用期装配。断点捕获、单次消费恢复及独立分支恢复统一由状态owner实现，保留原v1 namespace对象图、别名、下一事件边界和日志前缀。执行器包可独立导入，不加载旧兼容入口或研究包，也不编译抽取出的源码。修改当前工作树不迁移冻结worker或旧断点。

`events.py`现在仅按顺序驱动各阶段，不再容纳另一段巨型执行体。先推进交易所／行情与成交，再处理私有可见性和报价资格，随后执行报价、政策、风险选择、订单决策、REST执行与采样。策略等待、计算和计划停止期间仍推进市场；报价阶段提前退出时回到主循环，不跳过此前市场阶段。fork风险完全消退判断复用单一实现，循环临时值不再需要生成式上下文字段；拆出的经济操作及顺序保持不变。

回放原生模块加载改用live消费者已经使用的`strategy/native_runtime.py`。删除通过仓库名／编译目录子串猜测来源的旧检查；应用接口、必需入口和显式指定的运行目录由既有owner统一校验。原生导入损坏仍报错，不悄悄切回Python。三项过时源码文字／布局断言已退役；维护测试改为执行实际q90回调顺序与终点状态拒绝，已有可见性边界测试继续验证真实传播。

固定输入验证逐字段比较普通、异步执行、计算延迟、超时、紧急平仓及平仓替换六种模式，分别覆盖不中断执行和三个保存切点。这是保持行为的工程证据，不是新增经济实验，也不代表整个架构已获全量证明。

### M1已实现的本机计划命令

事件时钟直接迁移原函数且函数体未改变：真实成交保留同毫秒稳定顺序，合成时钟不能成交，系统事件位于同时间行情事件之后，冷启动、保留价格和终点边界规则不变。旧导入只是单一实现的别名，不是第二个执行器；独立导入时钟不会加载tick执行器、live或研究包。受影响测试437项通过，包含原prepared时钟复用、跨午夜窗口恢复、完整计划CLI和事件顺序回归。11项既有原生后端测试改用已有合法报价配置fixture而非`None`，原“不回退后端”和异常身份断言均保留，生产报价逻辑未改。仍有7条pandas弃用警告。这只是M3的一段真实抽取，不代表状态／决策／订单／会计迁移完成、安装包验收完成，也不授权升级冻结worker或旧断点。

```bash
narrowgate replay plan --config /private/plans/account.json
narrowgate replay run --plan /private/plans/account.json --executor local
narrowgate runs status /private/results/account
narrowgate runs pause /private/results/account
narrowgate replay resume --run /private/results/account
narrowgate runs report /private/results/account
```

这些命令不连接云主机、不交易。`plan`只解析校验，不prepare或回放。`run`前台运行，`pause`从另一个终端发出。`PAUSE_REQUESTED`仅表示请求：执行器到下一个支持的小时切点保存，或账户先自然完成。每日重型断点保留原首次切点的启动保存；暂停强制保存durable。`status`报告锁占用、生命周期、内存/磁盘进度与可恢复性；完成标记不等于验收。`report`核验全部保留载荷哈希及会计等式后，才报告完整PnL、每BTC成交USDC收益及名义成交额bps。未完成账户的终点指标为null，不填零。

输入JSON严格包含`schema`、`run_id`、`identity`、`bundle`、`params`、`model_dir`、`funding`、`accounting`、`output`。`schema`为`narrowgate.replay-plan.v1`；`identity`提供`experiment_id`、带版本的`baseline_id`、`arm_id`、`account_contract`、`parent_id`。`bundle`指向原ConsumerBundle目录。`params`可以是完整显式有效参数，也可以是`{"base_file": "baseline-params.json", "overrides": {}}`；override只能指定base已有键。`model_dir`必须显式填写，恰在禁用ML时为null。`funding`指向原带来源身份的资金费时间表JSON。`accounting`提供USDC单位的`initial_capital`和`max_mark_age_ns`。`output`指向独立归属的空结果目录。相对路径按计划文件所在目录解析，不依赖当前工作目录或live配置。不提供盈利/default参数，不替代缺失资金费时间表。

`plan`展示最终参数、逐参数来源、单位、产物位置、源码/模型/输入/资金费绑定及限制。原执行器语义检查仍然生效，这还不是覆盖全部历史参数的封闭schema。首个profile只支持JSON配置的单bundle账户；不装配私有运行时政策对象，不自动恢复冷输入，不复用全局连续时间线。它不替换正在运行的研究账户。`--executor poolrun`明确拒绝，不模拟成功。旧`replay --data-bundle ... --data-replay-config ... --summary-json ...`明确保留为仅摘要适配。

运行目录包含`resolved-plan.json`、身份/生命周期/进度、完整result、accounting、daily ledger、metrics、diagnostics、完整journal及哈希绑定completion。原必要断点留给所属维护流程，不由报告命令删除。重复调用已完成账户只核验返回，不重新回放。已有未完成账户必须显式`resume`；缺失恢复状态、参数/源码/模型/输入/资金费变化或外来输出均拒绝。恢复仅接受原release下可信本地断点，不接受上传的pickle，不自动跨版本转换。操作成功返回0（包含已接收暂停请求或未完成状态查询），参数/准入/文件系统错误返回2；非预期执行错误仍为非零退出，并在`lifecycle.json`记录失败。必须读状态/报告JSON区分完成、暂停、运行与未完成。

测试使用真实Python执行器和小型合成ConsumerBundle，包含新进程磁盘恢复与不中断运行的成交、日志及会计比较。这是工程证据，不是新增真实行情回测或生产/live验收。完整ML/native release一致性、干净安装环境执行及其余阶段仍需完成，不能提前宣布架构迁移结束。

首批迁移的受影响测试共122项通过，包含原F01/F04执行/报告回归，以及旧延迟实验冻结执行器的冷加载。定向Ruff与diff空白检查通过。已构建wheel并临时安装，以隔离解释器、不设置`PYTHONPATH`在仓库外导入、执行CLI帮助及源码绑定；628个包内条目包含新模块且排除private/test包。该检查复用独立Python的依赖，不安装生产release，不等于干净Linux/native或安装后真实账户验收。临时构建/安装产物已删除。
