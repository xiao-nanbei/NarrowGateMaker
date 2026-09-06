# E/C 配对标签训练

最后实质更新：2026-09-06 Last materially synchronized: 2026-09-06

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

## 尚未覆盖

训练器只检查标签结构和金额关系，不重新核验原始行情；使用前仍须完成真实配对轨迹验证。排队和延迟仍是模型，资金费来自冻结记录，短窗口用共同终点 MTM，不只统计已实现收益。标签单位为每次动作的 USDC，不是概率、可加总的组合收益或最优订单规模证据。

当前接口没有提供 live adapter、候选新增计算耗时的实测、完整 checkpoint 分叉，或已经证明经济价值的上线模型。不能因为生成了 policy JSON 就声称这些工作已完成。
