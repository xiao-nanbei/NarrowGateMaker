# Models：回放与分析兼容包

[English](README.md) | [简体中文](README.zh-CN.md)

Last materially modified: 2026-09-21

Last materially synchronized: 2026-09-21

该目录目前是过渡期的回放／分析包，不是只有机器学习模型的目录。tick 执行器、回放读取器、缓存工具及历史分析入口仍在这里；不要因为已有这个包，就把新的生产公共契约也放进来。实际归属及未完成迁移见[模块归属表](../docs/architecture.zh-CN.md)。研究族实现位于 `research.families.*`，已移除的 `research_*` 根目录不是导入别名。

## 模型产物与运行边界

生成的模型包不纳入 Git。保留当前运行身份实际需要的包、最新正式绑定的因果包及仍被一致性测试引用的评分产物。文件存在不等于模型晋级。正式回放需绑定模型元数据、特征可见时间、预热及标签语义，并独立绑定 P3、队列与延迟。过时模型不进入参数搜索；删除前检查引用并取得授权，保留必要冻结身份。

本文不声明当前部署了哪个模型或是否启用 ML，应从获准的部署配置和核实的运行身份解析。P3 即使与模型文件放在一起，也是独立校准组件。

实盘／回放共享的 epoch 契约已归入 `narrowgate/runtime/`；标量 quote-EV 特征在 `features/quote_ev.py`，生产消费者不再从 F05 取这些特征。训练与实验编排仍由对应研究族管理。现有 `models/audit/` 是分析设施，不是新运行契约的默认归属。`models/backtest_tick.py` 仍是维护中的 tick 执行器；历史一秒 Bar 回测不能代替经济 tick 回放。测试和训练使用仓库 `.venv/bin/python`。

## 预测消融入口

F03 13-head 的维护入口为 `research.families.f03_causal_13_head.ml_model`。来源与成交特征实验使用一致的特征清单、拆分、模型元数据和训练摘要合同。

```bash
.venv/bin/python -m research.families.f03_causal_13_head.ml_model --print-experiment-contract
```

非默认实验必须显式给出独立模型目录和实验 ID，训练完整13头，并标记 `promotion_authority=research_only`。来源配置与特征变体可以在事前约定的实验中组合；预测与回放结果本身不授予 live 权限。

## 外部信息衰减

F04 的 `external_venue_model.py` 是研究专用 Bitget／Bybit／OKX 外部信息训练器，独立于因果13头；历史成交时间一秒接口不等于 live 接收时间接口。当前运行必须声明完整整数秒目标网格，没有默认1／3／5秒目标。标签来自同一因果价格路径，早停留在 Development，输出按日聚类的同时区间选择结果。旧十秒头及显式旧目标列表仅用于兼容；后期评分限定于冻结选出的单个期限。没有外部预测产物因此获得 live 动作权限。
