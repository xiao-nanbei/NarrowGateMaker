# 教科书式报价模型

[English](model.md) | [简体中文](model.zh-CN.md)

Last materially synchronized: 2026-10-28

示例实现有限期限 AS 近似公式：

```text
reservation = mid - inventory * gamma * sigma^2 * remaining
half_spread = gamma * sigma^2 * remaining / 2 + log(1 + gamma/kappa) / gamma
bid = reservation - half_spread
ask = reservation + half_spread
```

库存使用虚构资产数量；中间价和报价为单位资产价格。Sigma 是价格绝对变化的波动率，单位为价格除以时间平方根，不是百分比波动率，也不是高低价振幅。Gamma 为正风险厌恶系数，kappa 为正的价格倒数敏感系数，remaining 为非负的剩余时间。多头库存使双边报价下移，空头库存使双边报价上移。买价向下、卖价向上取整到指定价格档位。

参考文献为 [Avellaneda 与 Stoikov 的限价订单簿高频交易论文（2008）](https://math.nyu.edu/inmemoriam/avellaneda/HighFrequencyTrading.pdf)。这里仅实现其报价近似，不复现论文的随机成交模型或实证评估。

命令行示例在源码中直接选择虚构的归一化输入，没有模型拟合、预测信号、动态参数选择或生产配置。回测使用另行声明的简化穿价成交规则，不使用推导公式时的指数订单到达模型。解释输出前请阅读[回测限制](backtest_rules.zh-CN.md)。
