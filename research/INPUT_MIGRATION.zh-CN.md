# 研究输入迁移

当前 F03 已完成：[858 次策略分片及完整 Final 结果](families/f03_causal_13_head/README.zh-CN.md)。本轮新标签、冻结模型及日账不再是待办；下表其他族仍是实现／支持检查，不代表全部完成。live execution_v1 已实现但未激活，仍须量纲审查及独立部署验收。

[English](INPUT_MIGRATION.md) | [简体中文](INPUT_MIGRATION.zh-CN.md)

Last materially modified: 2026-09-22

Last materially synchronized: 2026-09-22

本文区分可复用实现、历史结论和新来源验收，不启动实验。日历和来源仍以 [dataset_scope.json](../data/dataset_scope.json) 为准。采购行情、私有模型和结果不分发。受控测试通过不代表真实经济结果验收。

## 各族边界

[统一计划](RECOMPUTE_407.zh-CN.md)与[12 包任务清单](recompute_407.json)是当前研究入口。历史关闭不阻止本轮新问题，但不重启旧身份、不清零 previous-use。当前 F03 和已批准的 ML-OFF 保持冻结运行。下面的 API 是实际支持边界，不是完成声明。

F01 新增 `replay_economic_candidates`：统一独立账户和共同执行参数，记录完整成交轨迹，并调用共享 `settle_public_replay` 结算。拒绝恢复实盘账户，缺失资金费保持未知，轨迹不全拒绝经济验收。旧诊断 API 保留兼容；完整候选预算与 407 日竞速尚未执行。

| 研究族 | 可复用接口及当前边界 | 仍需重建或证明 |
| --- | --- | --- |
| F01 | `campaign_outcome_replay_audit` 仍是历史执行包装器；共享账户算术可复用 | 显式公共输入执行器和冻结假设；旧参数排名不能迁移 |
| F02 | 新 `public_input_calibration` 校验训练期来源与交付绑定 | 新校准曲线；触达不等于排队后的成交概率 |
| F03 | 已有新面板、模型整包和仅执行市场回放接口 | 当前 F03 已完成；历史测试泄漏仍属无效独立旧证据 |
| F04 | `HistoricalReferenceScheduler` 按 ready 时间调度；公共执行侧回测拒绝参考请求 | 各市场清单、计价单位和多市场适配器；缺市场不补零 |
| F05 | `QuoteEVModel.load(..., input_identity=...)` 要求来源、训练、标签身份、明确有序列及拒绝或保留 NaN 策略 | 新机会面板与模型；旧补零预处理不是新特征适配器 |
| F06 | 冻结 `placement_fill_panel` 访问旧路径前须显式 `--legacy-input` | 新 placement、暴露和删失适配；L2 不提供原生队列身份 |
| F07 | 按市场身份比较活动订单轨迹的工具可复用 | 新事件流包含撤单在途及快照不确定性；旧 lockstep 不验收新输入 |
| F08 | 历史聚合一致性入口显式接受原生父包合同，不接受任意逐笔 | 新逐笔计数和派生组统计；不伪造聚合包或普通成交资格 |
| F09 | OPE 支持度和行为概率要求仅用于 OPE；配对模拟不造概率 | 新动作与赋值到终点奖励面板；赋值前利润不算干预奖励 |
| F10 | 边界权益记账和时钟诊断可复用 | 新 epoch、输入对应的归因；模拟一致不等于原生或 live 一致 |
| SYS | 已有来源绑定消费者及独立账户边界 | 生产语义版本及现有受控测试之外的恢复、并行等价性 |

以上是入口级核对，不宣称每个目录的所有脚本均已迁移。下列新适配器已经实现并有受控行为测试；标明待完成的训练、回放整链路仍未完成，新面板 API 不等于历史训练器已经迁移。保留行为测试和冻结历史记录，不改历史哈希，不将旧默认路径偷偷指向新行情。

## 可执行的新输入适配器

共享 [FeatureCursor](../data/feature_cursor.py) 校验消费者和来源身份，只选择已就绪帧，执行明确的最大帧年龄限制，并应用共享有效性掩码。缺少上下文时拒绝，不读取未来数据。`ConsumerBundle.stream()` 重放绑定的观察情景，不另建解析器。下列 API 由[合成受控测试](../tests/test_research_public_inputs.py) 验证，导入时不启动任务。

| 研究族／当前 API | 已实现行为 | 剩余局限 |
| --- | --- | --- |
| [F01 `replay_parameter_candidates`](families/f01_fixed_parameter_racing/public_input.py) | 调用公共输入回测器，共用假设、各候选独立调用；仅允许 gamma、kappa 和最大价差变化 | 诊断回测的资金费和完整净收益保持未知；不是完整经济参数竞赛 |
| [F04 `build_reference_panel`、`replay_reference_strategy`](families/f04_external_market_alpha/public_input.py) | 绑定市场、按 ready 时间取特征；显式预测器接入共用经济执行器 | 不适配旧权重、不恢复缺失市场；需要独立资金费及完整成交记录 |
| [F05 `build_opportunity_panel`、`train_opportunity_models`](families/f05_fill_quality_quote_ev/public_input.py) | 连接同来源机会与 outcome 身份，保留条件缺失和删失，按真实 outcome 终点剔除；拟合并原子发布所选方向的全部五个 quote-EV 头 | 需要明确的新成交／markout 结果、分桶、阈值和拟合参数；不复用旧 trace 清洗器或权重 |
| F05 `QuoteEVModel.predict_frame` | 加载模型推理前执行共享帧合同、因果截止和有效性掩码 | 需要新训练的兼容模型；本次没有训练真实模型 |
| [F06 `build_placement_panel`、`placement_risk_targets`、`replay_placement_strategy`](families/f06_placement_fill_cif/public_input.py) | 因果 placement 特征、竞争结果及可配置距离规则执行 | 需要显式新规则，不是自动重训的 CIF 模型 |
| [F07 `continuation_report`、`replay_continuation_strategy`](families/f07_active_order_continuation/public_input.py) | 生命周期诊断及通过原有延迟执行的可配置保留／撤单规则 | 不是已校准的 hazard 模型或原生队列闭合证明 |
| [F08 `load_visible_side_flow`](families/f08_side_taker_lifecycle/public_input.py) | 读取共享已关闭可见 Bar，保留成交量／额守恒及个体成交计数 | 不重建原生聚合包连续段或 live 个体成交间隔特征 |
| [F09 `build_action_panel`、`evaluate_action_panel`](families/f09_campaign_action_uplift/public_input.py) | 使用赋值至终点权益奖励，要求记录的概率，调用现有 OPE 并逐折剔除实际终点越界样本 | 仍需显式带时钟特征注册、动作支持和充足样本；不宣称线上因果收益 |
| [F10 `attribute_interval`](families/f10_live_replay_attribution/public_input.py) | 将账户边界绑定到输入、观察和 epoch，保留资金费未知及陈旧估值限制 | 不证明历史 live／原生一致；执行合同身份由运行提供，不从 L2 推断 |

机会、placement、订单事件和账户记录都携带所属消费者清单身份，必须来自匹配实验；行情本身不能生成这些记录或干预概率。订单事件保留原始顺序及整数纳秒生效时间。观察区间左闭右开；撤单请求不等于撤单生效，快照重置不关闭订单，生效前或终态后成交均拒绝。可空首次成交时钟保持整数精度。这些 API 不读取历史默认目录，不启动真实拟合，也不重启已关闭研究。

新适配器通过[研究族注册表](registry.json) 的 `public_input_module` 定位。F05 受控冒烟测试构建机会面板、拟合微型合成整包、经严格加载器加载并从带掩码的 FeatureFrame 推理。这是软件测试，不是研究拟合结果。训练器拒绝类别支持不足、特征合同不一致及输出目录已存在，不搜索超参数，也不读取留出集。多消费者包的实验编排和各历史机制的全部迁移并未因此完成。

## 连续多包与参考市场执行工作

`data.consumer_sequence.derive_consumer_sequence` 现已检查顺序相邻的消费者区间、相同市场／延迟／特征合同及来源 manifest 身份。它通过共享调度器生成一个仅新建的连续包，不拼接各自冷启动的特征文件。重复的预热来源包只读取一次，来源顺序冲突直接失败。盘口、在途观察和滚动窗口在包边界连续。这是输入准备，不是账户重置或可恢复的分布式执行器；受控测试中整段处理与拆包后重建得到相同特征、Bar 和深度。

F04 新增 `ReferenceSignalAdapter` 和 `replay_reference_strategy`：显式提供的新预测器接收执行市场特征及按 ready 时间向后查找、按市场命名的参考特征，再把现有五个报价通道输出交给维护中的执行器。输入 manifest、列顺序、缺失策略、最大年龄及模型／输出身份均需明确声明。缺少或过期的参考上下文直接失败；非法概率、波动率和非有限输出不补零。不会隐式换汇或加载历史外部市场模型。

F04 新入口还调用 `models.replay.public_accounting.settle_public_replay`。它要求完整且按实际执行顺序记录的成交，与执行器核对现金及期末库存，保留带符号手续费，并用显式新鲜度限制下的已交付 BBO 估值剩余库存。资金费使用单独绑定来源、完整声明结算时间表的输入；同刻先结算再成交，终点先结算再 MTM。缺少资金费或非零库存估值过期时，完整净收益保持未知。资金费属于执行后记账，不反馈至仓位规模或保证金政策。受控测试运行实际执行器，并另行验证亏损往返交易、费用、资金费、缺失估值及不完整成交记录。这不代表真实数据经济验收，也不证明动作或收益改善。

F06/F07 现已提供 `replay_placement_strategy` 和 `replay_continuation_strategy`，使用共享[新动作接口](../models/replay/public_strategy.py)。首版可配置实现是规则策略，不是已训练的 CIF 或 hazard 模型：F06 支持默认动作或向外扩大报价距离；F07 支持默认、保留或撤单。规则只读取声明的、已就绪的特征，各方向按顺序采用首个匹配规则。缺失值明确选择拒绝或跳过整次决策；过期／不存在的上下文和未声明列始终拒绝。不加载旧模型、不补零。KEEP 不能开启被禁用的方向、凭空创建订单或压过强制 P3／名义金额更新；CANCEL 通过原有延迟撤单流程发出请求，不立即删除交易所订单。后续取整、风控、路由时序和在途请求合并仍然生效。

策略合同使用 `research.action_policy.v1`，包括非空 `policy_id`、`family`（`F06` 或 `F07`）、准确的 `input_manifest_id`、有序 `feature_cols`、`missing_policy`（`reject` 或 `skip_decision`）、整数 `max_age_ns`、有界 `trace_limit`，以及同时包含 `BUY` 和 `SELL` 列表的 `rules`。每条规则声明 `feature`、`op`（`ge` 或 `le`）、数值 `threshold`、`action` 和 `spread_mult`。非扩大动作的倍数必须为 1，扩大动作至少为 1；空列表采用默认行为。规则和参数由实验显式提供，不从旧研究推断。消费者身份绑定来源、观察和特征合同；每次独立回测创建新策略实例。策略状态的 checkpoint／resume 尚未实现，入口明确拒绝。

两个入口都要求空仓初始账户和完整的有界成交记录，调用维护中的报价／订单／成交执行器，再用 F04 共用的独立记账模块结账。资金费缺失仍为未知。策略记录区分请求动作与最终处理意图，明确不是 ACK／成交证据；实际提交和成交保留在执行器记录里，策略记录被截断时会计数。受控测试检查默认策略的报价／现金／库存不变、实际提交价格变化、保留／撤单效果、缺失数据、因果截止及安全规则优先级。这是软件行为证据，不是新的经济表现或原生队列一致性证明。已有多包准备流程可以提供一个连续包，但不提供分布式策略 checkpoint 迁移。本次不重启旧关闭研究、不启动真实数据经济运行，也不修改 F03 作业。

本次 live 接口清理同时删除逐行选择写入协议的分支：MakerEngine 固定调用 `enqueue_csv_values`，队列满的测试替身也实现同一接口。WebSocket 旧启动参数仅在入口转换为快照／listen-key 职责，运行方法不再回退到仅为旧 fixture 保留的通用客户端。受支持的旧启动入口、明确职责装配和异常路径测试仍保留。本次只修改源码，不部署实盘。

## F05 加载合同

默认加载器不凭文件名推断兼容性。调用方提供 `input_contract_id`、`observation_contract_id`、`feature_contract_id`、`source_manifest_sha256`、`training_contract_id`、`label_contract_id`，每个模型的元数据必须匹配。每个 booster 的特征名及顺序必须完全一致，分桶值和类别必须显式声明。预测路径按元数据拒绝缺失值或保留 NaN，不把未知转成中性零。新特征应从声明的输入直接构建；已经过旧逻辑补零的字典无法恢复丢失的可观测性，不属于验收通过的适配器。

`load_legacy()` 明确保留历史默认列和补零 ABI。旧 shadow 评估器要求 `--legacy-input`；它是离线历史分析，不授权运行实盘 shadow。尚未执行新 F05 训练或经济回测。输出仍是每次报价机会的 maker 方向 bps，不是完整 USDC 净损益。

## 历史结论

旧阳性和阴性只在原数据、模型、动作、执行及样本身份内保留。参数排名、P3 数值、模型权重和收益需重新估计。关闭某个候选不等于否定整个机制族。已知泄漏或奖励归因问题使受影响解释失效，不连带否定无关分支：保留 F03 已记录的 causal-v4 泄漏范围，以及 F09 campaign 终点利润与赋值后增量奖励的区分。未验证的新来源表现保持待验收。换源、重训不清零 previous-use。

已修和未完成项见[审计修复进度](../docs/tardis_migration_audit_followup.zh-CN.md)。本索引明确：旧研究族文档中的“当前”不能自动解释为新数据集的现状；不修改冻结报告，也不授权重启旧实验。
