# 一日数据工程教程

[English](one_day_data_pipeline.md) | [简体中文](one_day_data_pipeline.zh-CN.md)

Last materially modified: 2026-09-14

Last materially synchronized: 2026-09-14

状态：当前统一 `data` 输入层教程；不运行训练或经济回放。

## 范围

本教程只是有界工程样本，不是研究选日规则。当前日历仍保留 2025-08-01 至 2026-09-11 的全部 407 个 UTC 日，涵盖执行市场 BTCUSDC 和参考市场 BTCUSDT 永续。缺失和困难日期留在清单，数据修复不改变 Development、Validation、holdout 或 previous-use 权限。

零网络体验请使用[合成回放示例](../../examples/replay_demo/README.zh-CN.md)，它与购买行情的验收分开。[数据指南](../../data/README.zh-CN.md)说明当前事实层、观察层和仍待完成的集成。

## 安装与配置

使用 Python 3.11 或更新版本。以下工程命令只需数据依赖，不调用策略或计算 PnL。

```bash
python -m pip install -e ".[data]"
python -m data --help

export INPUT_ARCHIVES="<private-purchased-archive-root>"
export DATA_OUTPUT="<private-derived-root>/input-engineering"
DAY=2025-08-01
```

购买的压缩原件保留在 raw，包括 `.incoming` 下需要保留的购买批次。输出写入独立 derived 根目录。两者都是真实目录，不建立供应商别名或软链接。交付地址和授权信息留在私有配置，不得放入公开问题、日志或 Git。公开命令命名统一，内部来源身份仍保持真实。

## 获取与清点

通过共享入口继续尚未完成的已配置购买批次：

```bash
python -m data download --config <private-delivery-config.json>
python -m data inventory --root "$INPUT_ARCHIVES" \
  --output "$DATA_OUTPUT/calendar-current.json"
```

下载仅获取原件，复用锁、回执和完整性检查，不退役原件或暗中换源。等待响应不能写成空文件成功。日历清单始终保留完整固定区间，独立于这个单日示例。

文件存在或已有校验回执不等于全文内容验收。无法证明的覆盖、缺口和研究权限保持 unknown。缺日必须作为记录留在总分母中。

## 转换与验收

输出使用新目录。转换拒绝覆盖已有事实包；选定原件缺失或存在歧义时，在构建前明确失败。

```bash
python -m data normalize --root "$INPUT_ARCHIVES" \
  --start "$DAY" --end "$DAY" --symbol BTCUSDC \
  --output "$DATA_OUTPUT/facts-$DAY"

python -m data validate --bundle "$DATA_OUTPUT/facts-$DAY" \
  --output "$DATA_OUTPUT/acceptance-$DAY.json"
```

默认选择盘口和逐笔成交。重复 `--symbol BTCUSDT` 可加入参考市场，重复 `--channel` 可指定频道子集。也可用私有 `--plan` 指定按序上下文文件和实际时钟映射证据。单日包不证明跨日初始化或跨日成交去重：相应检查须把充分相邻上下文纳入同一个包。

共享解析器保留连续消息边界和精确十进制。快照原子替换盘口，增量设置绝对数量；成交按市场和成交身份去重，不只按时间戳。供应商接收时间仅用于技术分组，无法证明的交易所时钟来源保持 unknown。

验收读取选定事实包，验证分片与原件身份绑定，跨文件连续重建盘口。来源缺口、时间回退和无效状态保留为发现。解析成功不代表新鲜观测、精确原生队列、全日历验收或经济可用。

## 后续与历史工具

共享观察和最小特征仍需接入生产消费者并完成端到端因果验收，才能用于新模型。标签需要实际 outcome 终点剔除和合规 split manifest；经济比较需要明确成交、延迟、费用、资金费和期末 MTM 合同。以上命令不会执行这些步骤。

旧获取流程及 Bar 定价诊断已从当前教程退役。可通过 `pipeline.py legacy --help` 查看历史模块，但不能把它们作为当前来源回退。保留历史机制文档，不把旧结果或缓存当作新契约证据。
