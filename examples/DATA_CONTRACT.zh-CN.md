# 合成数据契约用例

[English](DATA_CONTRACT.md) | [简体中文](DATA_CONTRACT.zh-CN.md)

Last materially synchronized: 2026-09-19

安装项目后，在仓库根目录运行 `python -m examples.data_contract_demo`。用例在临时目录生成虚构的 Tardis 格式 CSV，调用当前事实解析及观察／特征消费者，打印结果后仅清理自身临时文件。不需要购买数据、联网、模型或授权市场记录。

受控盘口包含一份原子两档快照和一次绝对数量更新。成交包含重复 ID 及同时间不同 ID：只去掉重复记录，剩余两笔、数量合计 3。观察情景明确使用源时间戳代理模拟延迟；历史时钟证据未知仍保持未知。最初尚未交付的特征保持缺失，后续帧遵守就绪时间。生成产物包含观察、可见 Bar、独立 outcome Bar 和特征帧，不包含标签或训练模型。

这个极小用例展示接口，不证明 Top20 覆盖、407 日验收、实际供应商映射、原生消息一致性、经济有效性或所有研究族迁移完成。请结合[数据接口](../data/README.zh-CN.md)、[范围](../data/dataset_scope.json)、[运行层](../data/runtime.py)、[观察语义](../data/observation.py)、[研究族注册表](../research/registry.json)及[测试](../tests/test_data_contract_demo.py)阅读。用例可运行不代表可以将历史产物重新标成当前输入。更多因果、快照、成交身份及消费者测试位于 `tests/test_data_facts.py`、`tests/test_data_observation.py` 和 `tests/test_public_input_panel.py`。
