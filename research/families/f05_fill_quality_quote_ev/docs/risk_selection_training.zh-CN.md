# E/C 配对标签训练

Last materially modified: 2026-09-06

Last materially synchronized: 2026-09-06

[English](risk_selection_training.md)

此入口用已经验证的模拟反事实标签拟合小型动作价值模型。它不启动回测、不下载私有数据、不声称复原真实逐笔成交，也不部署策略。拟合成功不等于已经证明盈利。

## 执行顺序

1. 保持当前 baseline、执行环境和 Development 日期不变。
2. 使用现有 F01 的 `--save-risk-opportunities` 留存全部可改变机会，包括从未成交的机会。选取规则在结果之前固定，不能按未来成交或亏损筛选。
3. 使用 `--risk-pair-baseline-arm` 和单机会覆盖参数，验证共同机会前缀、唯一动作变化、完整后续轨迹、共同终点估值、手续费和资金费。当前实现重新执行前缀，不声称具备完整 checkpoint 或 copy-on-write 恢复。
4. 验证后，分别拟合 E/BUY、E/SELL、C/BUY、C/SELL。
5. 之后仍须用完整样本外路径比较 B/E/C/EC，以及随机参与和空仓对照；单步标签不能代替最终经济检验，也不能单独授权部署。

E 在允许的空仓首次开仓机会比较 POST−WAIT；有模型时，价值严格大于零才 POST，等于零时 WAIT。C 对剩余数量仍纯增仓的订单比较 KEEP−CANCEL，仅负值才 CANCEL。没有模型或特征不可用时保留基线保护；这两类动作都不另设价格、数量、冷却或立即重挂。

## 最小离线命令

以下是 owner 提供的产物，不随公开仓库分发。在仓库环境安装研究依赖后运行：

```bash
PYTHON="$NARROWGATE_ROOT/.venv/bin/python"
"$PYTHON" -c 'import sys; assert sys.version_info >= (3, 11)'
"$PYTHON" -m research.families.f05_fill_quality_quote_ev.risk_selection_training \
  --labels "$NARROWGATE_RESULTS_DIR/pairs.risk_paired_labels.jsonl" \
  --feature-units "$NARROWGATE_RESULTS_DIR/feature_units.json" \
  --validation-start-ns "$VALIDATION_START_NS" \
  --alpha 1 --min-train-rows 8 --policy-id development-ec-ridge \
  --output-dir "$NARROWGATE_RESULTS_DIR/training"
```

`feature_units.json` 明确记录冻结的特征名及单位。模型使用已经记录的字段，不猜单位、不把缺失值补成零。特征选择和 Ridge alpha 必须在查看验证结果之前固定。八行只是可配置的工程最低支持量，不是统计样本量建议或经济门槛。

均值和尺度仅使用训练行。标签结果区间延伸到验证边界的训练行会剔除，同一订单不允许分到两侧。不提供随机行切分：许多机会共享同一行情路径和终点。单窗口小批可验证拟合流程，不能提供独立验证区间。

`policy.json` 可由 `strategy.risk_selection.RiskSelectionPolicy` 加载。 `training_report.json` 记录排除原因、各侧支持量、相对仅用过去均值的预测 MSE，以及是否仅为训练结果。支持不足的模型不写入 policy，不与另一侧混合补足。输出使用新的私有目录，不覆盖既有产物。

## 完整学习策略回放

拟合后，保留已有冻结的 F01 baseline 命令，增加 `--risk-selection-policy "$NARROWGATE_RESULTS_DIR/training/policy.json"`。此入口使用 Python diagnostic、明确的资金费记录和 `--continuous`；不能把连续日期拆成每天清零的实验臂。通过原有 arm-spec JSON 选择 B/E/C/EC，例如：

```json
[
  {"name": "E", "group": "risk_selection", "overrides": {"risk_selection_mode": "E"}},
  {"name": "C", "group": "risk_selection", "overrides": {"risk_selection_mode": "C"}},
  {"name": "EC", "group": "risk_selection", "overrides": {"risk_selection_mode": "EC"}}
]
```

在 `--arms` 中选取这些名称及 `baseline`。加载行情前只读取一次策略文件；各臂共享其参数和不可变输入，但独立维护订单、预算、库存及后续动作。默认 B 不评分；E/C/EC 在预算 reservation 前，用同一可见快照批量判断两侧。WAIT 不增加冷却，C 继续走正常撤单、ACK 和终态流程。

此模式自动保留完整机会表，含 policy ID、预测 USDC 价值差和原因。经济结果行同时记录动作数、变化数和回退数，以及净 PnL 和资金费。缺少某个侧模型时保留基线，不借用另一侧。不能与 `--risk-pair-baseline-arm` 混用：重叠的单次干预标签不是完整策略收益。这个开关没有启用 C++ 执行或 live 部署。

## 随机参与和空仓对照

同一个 F01 的实验臂覆盖参数可选择 `risk_selection_control=learned`（默认）、`random` 或 `flat`。这些对照复用现有机会收集器和正常订单路径；原有会改变报价节奏或几何形状的随机被动报价实验臂，不是匹配参与率对照。查看评估结果之前必须冻结比较设计；接口本身不提供经过科学校准的否决率。

R 使用 E、C 或 EC 模式，同一个 `--risk-selection-policy` 参考产物，以及 `risk_selection_random_rates`，将 `E:BUY`、`C:SELL` 等表面映射为 `[0,1]` 内的 WAIT/CANCEL 概率；还必须明确整数 `risk_selection_random_seed` 和非空 `risk_selection_random_scope`。否决率只能由冻结的训练区间估计，评估期间固定 seed、scope 和概率。参考评分器确实会运行，以严格复用相同的模型和特征支持范围，但模型价值不决定随机动作。缺模型、模型输入或否决率时保留基线，并明确计数；仅覆盖部分表面的 R 臂不能被描述成完整的四表面匹配对照。

随机抽样由 seed、scope 和机会身份共同确定，不推进行情、执行或延迟的随机数生成器。匹配训练期否决概率，不保证路径分叉后实际参与次数完全一致。必须按 BUY/SELL 和 E/C 表面报告实际判断数、否决数、回退质量及活动量，不能根据评估路径重新调整概率。

Flat 设置 `risk_selection_control=flat` 和 `risk_selection_mode=E`，从共同且已知空仓、无待确认订单的分段初态开始，整个分段每次 eligible E 都 WAIT。它不是只跳过首单、重置已有账户或强制清仓；非空仓或仍有待确认订单所有权的初态不受支持。即使其他实验臂共享策略产物，Flat 也不使用价值模型。

两种对照都要求 Python diagnostic、`--continuous` 和明确资金费输入，自动保留全部机会，且不能与单次干预标签混用。机会价值为 null，原因明确标识控制组。日报中的 `risk_selection_control_*` 计数与保持为零的 `risk_selection_policy_*` 动作和判断计数分开；`risk_selection_reference_evaluation_count` 和 `risk_selection_reference_policy_id` 如实记录 R 实际执行的参考模型工作，Flat 则为零和空。R 的 null 价值不表示从未计算模型。对照都不改变价格、数量、报价节奏、数量角色安全检查、预算所有权或撤单终态处理。Flat 等零活动对照不能证明选择性执行成功。

## 尚未覆盖

训练器只检查标签结构和金额关系，不重新核验原始行情；使用前仍须完成真实配对轨迹验证。排队和延迟仍是模型，资金费来自冻结记录，短窗口用共同终点 MTM，不只统计已实现收益。标签单位为每次动作的 USDC，不是概率、可加总的组合收益或最优订单规模证据。

当前接口没有提供 live adapter、候选新增计算耗时的实测、完整 checkpoint 分叉，或已经证明经济价值的上线模型。不能因为生成了 policy JSON 就声称这些工作已完成。
