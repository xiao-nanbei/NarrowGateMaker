# 旧 L2 证据重新验证

[English](legacy_l2_evidence_revalidation_20260725.md) | [简体中文](legacy_l2_evidence_revalidation_20260725.zh-CN.md)

Last materially modified: 2026-09-13

Last materially synchronized: 2026-09-13

> 发布说明：`${NARROWGATE_*}` 与部署周期名称是逻辑定位符。主机侧数据和证据保存在私有证据存储中，除非提供仓库相对链接，否则不随公共仓库分发。参见[公私文档约定](public_private_documentation_contract.zh-CN.md)。

原审查日期：2026-07-25。

状态：证据管理决定，不改变策略或实盘政策。

历史完成更新（2026-07-27）：随后执行了标准化 P3、特征／模型、BUY scorer、生命周期／零假设和严格回放重建，并进行了更广泛的时间／日历／单位修复。当时的模型身份为 `causal-v7`，不是 causal-v5/v6；这不标识今天部署或新训练的模型。本文的撤回分类仍有效；文末计划属于历史记录，不是当前待办清单。

## 退役与研究影响（2026-09-13）

用户授权删除的共享派生 `bbo/` 目录已清理：256个文件，BTCUSDC、BTCUSDT 各128日，日期跨度为2026-01-01至2026-07-20。历史文档保留作参考。当前 owner-manifest／readability 索引和正在执行的48日盘口重建不引用该目录。此前可选的 BTCUSDT 参考特征读取路径，不是继续把这些文件当成当前数据的理由；删除后，可选 BBO 缺失不能被称为参考盘口特征已重建。已取得的 L2／逐笔需要单独生成并绑定派生特征。本次删除不代表与新来源逐行等价的验收。

| 历史研究／结果 | 当前使用状态 | 已记录原因／仍然有效的范围 |
| --- | --- | --- |
| F02 于2026-07-15从旧根目录校准的经验 P3 | 旧输入／工件已被取代，不再使用其身份 | 粒度和深度混用；后续100ms重新校准在0.2%以内复现触及曲线，不代表 P3 思路失败。 |
| F03 基于混合盘口的旧因果特征、13-head 指标与 ML A/B | 比较失真／仅供历史参考 | 盘口粒度和深度不一致会改变特征及执行路径；已退役 causal-v2 另有119个登记日与123个实际日文件不一致、旧切分／标签语义问题，见[F03退役说明](../research/families/f03_causal_13_head/README.zh-CN.md)。 |
| Causal-v4 test/all-122 ML A/B 与 blocked-cross-fit BUY scorer 数值 | 已撤回，不作为当前模型选择证据 | 三个 causal-v3 特征日期提前暴露五分钟 metrics；这是独立确认的特征缺陷，不归因于每个旧 BBO 行。 |
| event-L2 合同之前的 BUY widen、SELL 单周期 skip 与分侧动作表 | 精确 DR／成交／干预率／campaign 数值已撤回 | 事件／订单分母被取代；保留历史不晋级决定。删除不重新开放已关闭家族。 |
| Retained111 生命周期／FIFO，以及旧 random-null／direct quote-EV／cap-compression／markout-sign 数值 | 精确数值已撤回或须重新计算 | 混合或被取代的盘口／订单／时钟路径；短期 markout 仍不能代表完整生命周期价值。 |
| F04 使用旧本地 BBO 分母的 xmarket／spot／global Stage-0 maker markout 和 campaign 表 | maker 数值已撤回 | 独立逐笔生成的外部状态、价格方向诊断和共识构造，不因该盘口缺陷失效。 |
| 修复前48／512／1024臂排名及 gamma／cap／guard／cooldown 最优者 | 不得用于当前选择 | 使用旧盘口、时钟、队列、P3、模型和单位身份；旧胜出臂 ID 不是新候选。 |
| Top20 + q0.70 的 keep/cancel／rearm／recovery 结果 | 有条件的历史证据，不是原生深档队列真值 | 避开了1秒粒度缺陷，但保留原有限深度和队列假设。 |
| 仅确认使用过 BTCUSDT BBO 参考特征的历史模型 | 旧输入结果；换新数据后需重新验证 | 依赖关系本身不证明失真方向或大小；没有绑定输入／结果证据，不虚构某模型的损益变化。 |

具体依据见下文与[历史回测重新验证登记](../research/families/f10_live_replay_attribution/docs/historical_backtest_evidence_revalidation_20260720.md)。这是汇总已有记录，不是新跑的经济结果。原生输入研究、独立逐笔诊断、真实 live 观测和实现正确性结果，不被一概判为失效。本次没有打开封存结果、改变 previous-use 或执行新经济比较。

## 原决定

此前顶层 `bbo/` 和 `l2/` 是混合数据身份：

- BTCUSDC 早期多数文件为约1秒、前10档状态。
- 较新文件为约100ms、前20档状态。
- 研究程序可以通过同一个默认路径选到混合输入。

因此取消其 BTCUSDC 回放／特征默认入口。当时迁移选择的标准化默认目录为：

```text
${NARROWGATE_RETIRED_DATA_ROOT}/normalized_l2_100ms_v2/
```

精确可见价层队列研究还须流式读取原生 CryptoHFTData 快照／增量。100ms前20档矩阵不等于深档队列。

## 已测得的失真

问题不只是元数据：

- 将同一天从约1秒重建到100ms，观测撤单和补单路径计数分别变化约3.64倍和3.94倍。
- 拟合的诊断 adverse／cancel／refill 半衰期从 `1000/1000/1000ms` 变为 `1000/500/101ms`。
- 一个单日探测从前20档改为原生 deep-250 后，成交从1,595变为2,062，仅两个决策 ID 重合。

旧订单、成交、队列、campaign 和损益路径不能通过更改文件夹名称修复；它们是不同的反事实轨迹。

## 当时要求重建的内容

### 经验 P3

2026-07-15 工件明确读取旧顶层 `bbo/`，因此原输入身份已经被取代。不过冻结100ms BBO身份下的重新校准后来已完成：

- 5s `kappa_eff`：`0.08311357 -> 0.08325351`（`+0.168%`）。
- 10s `kappa_eff`：`0.06743811 -> 0.06735643`（`-0.121%`）。
- 5s／10s `delta_star` 在0.1 USDC网格上不变。

触及曲线结论在新输入身份下得到重新验证，但旧工件／哈希没有恢复有效。参见 `p3_touch_recalibration_normalized100ms_v2_20260725.md`。

受控固定价差探测不因本次撤回而失效：它绕过 P3 报价下限，直接相对100ms同侧 BBO指定距离。

### 因果特征和模型

标准化v2之前的因果特征包含从混合目录生成的 microprice、L2 imbalance、refresh/cancel 等字段。当时要求按以下顺序重建：

1. 具有明确桶就绪时间及v2数据身份的因果特征。
2. 13-head 模型包。
3. 严格的 ML-OFF／ML-ON 回放。
4. BUY 成交选择订单级面板、scorer、阈值和动作检查。

审查时的 BUY scorer 不因运行代码正确而得到重新验证；它后来被重建，仍未通过联合动作检查。

### 任何重新考虑的全局参数或动作家族

修复前48／512／1024臂排名、retained／blocked／late 损益表及旧 gamma／cap／guard／cooldown 胜出者属于归档，也含旧时钟、队列、P3、模型和单位身份。若研究允许重新考虑某家族，须在新身份上与修正 baseline 配对；旧臂 ID 不可直接复用为候选。

## 已撤回或有条件的数值证据

- 2026-07-18 BUY widen、SELL skip 和更早分侧动作表先于 event-L2 合同；DR增益、成交、干预率和campaign数值撤回。保守“不晋级”决定仍成立。
- Retained111 库存生命周期计数、5.8分钟FIFO中位数和92.1%的30秒存活率需重新计算。“30秒markout是早期毒性而非完整生命周期价值”的概念判断仍合理。
- 旧 xmarket／spot／global Stage-0 maker markout与campaign表用了旧本地BBO分母，精确maker数值撤回。外部逐笔生成的1秒状态、2-of-3共识和leave-one-venue-out结构不依赖混合BTCUSDC盘口；Stage 0只说明旧实验未建立maker动作价值，不证明外部信息无价值。
- 使用100ms前20档的keep/cancel、rearm和recovery避开1秒粒度缺陷；其数值只适用于声明的 `top20 + q0.70 fallback` 反事实身份，不是原生深档队列估计。不晋级决定仍安全。
- 将top20回放与旧P3结合的Development动态机制归因属于近似；其中实际live日志归因仍属live证据。
- 旧random-opportunity／executable-passive null、direct quote-EV、cap-compression和markout-sign数值依赖被取代的订单分母、时钟或回放路径。方法定义可参考，历史数值表归档。

## 不因该问题失效的证据

- 原生快照／增量调度器和原生strict-62日期集合。
- 使用原生精确价层快照／增量与逐笔的 `queue_value_net_hazard_keep_cancel_v2`。
- `dynamic_fill_hazard_m0_native_strict_nested_cal_v2` 及其一次性BUY Validation读取。
- 配对执行几何诊断 `paired_fixed_spread_monotonic_v2`。
- 从逐笔状态而非BTCUSDC L2生成的外部fast 1s／3s价格方向诊断。
- 会计、方差单位、终点市值计价、特征就绪时间和合并时钟的正确性修复。
- Python/C++同输入实现一致性。
- 实际live亏损归因、接收时间采集和AWS东京延迟／运行测量。
- OPE、SPIBB、scorecard、实验注册和晋级方法。

## 存储迁移记录

2026-07-25：

- BTCUSDC回放和特征默认改用 `normalized_l2_100ms_v2`。
- 删除250个独立旧 `l2/*.parquet`，释放约2.39 GiB。
- 因冻结严格视图仍引用，保留六个100ms硬链接锚点。
- 随后62个正式v2日期均通过大小和SHA256检查。
- 当时因BTCUSDT桥接与旧P3清单引用，暂留顶层 `bbo/`；该临时保留已在2026-09-13用户授权删除后结束。

当时也关闭四条复发路径：CryptoHFTData重建默认写入版本化暂存根而非顶层bbo/l2；正式回放核验实际加载的每个BBO/L2上下文日；P3默认使用 `normalized_l2_100ms_v2/bbo`；`MM_BBO_DIR` 与 `MM_L2_DIR` 必须成对指定。

另发现2026-07-04至07-11八个BTCUSDC逐笔文件的maker-side列被损坏为全true；当时已从Binance Vision原子替换，并含双向主动成交。固定价差执行器在读取结果前记录并检查独立的执行成交质量身份。

该次迁移未修改原生CryptoHFTData归档。

## 历史重跑顺序

1. 完成128日固定价差广域曲线。
2. 用独立原生深档回放确认。
3. 重建订单级分母、生命周期计数、FIFO/LIFO存活、随机零假设及markout/campaign分解。
4. 重建因果特征与13-head。
5. 重训BUY成交选择scorer。
6. 跑严格baseline、ML A/B和队列敏感性。
7. 仅重跑仍有经济研究意义且获准的动作家族。

后续身份完成了维护中的重建，但不恢复旧精确数值的有效性。旧“不晋级”决定可作为保守治理结果引用；旧损益、成交、队列与模型校准数值仍不得用于当前选择。
