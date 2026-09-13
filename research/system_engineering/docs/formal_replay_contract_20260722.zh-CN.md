# 正式回放合同指南

[English](formal_replay_contract_20260722.md) | [简体中文](formal_replay_contract_20260722.zh-CN.md)

指南原始日期：2026-07-22

Last materially modified: 2026-09-06

Last materially synchronized: 2026-09-06

状态：当前实现的维护指南，不是不可变运行清单。[`models/replay_contract.py`](../../../models/replay_contract.py) 当前输出 `narrowgate_formal_replay_contract.v4`。历史 v1 执行保留其原始源码与工件身份。

## 用途

策略证据与 live 对齐使用不同回放身份。

- `formal`：在冻结、因果的回放合同下比较 baseline/candidate，需要 `--strict-calibration`，是唯一可能具备研究晋级资格的用途。
- `live_alignment`：诊断单位转换、事件时钟、状态机迁移与安全检查顺序，不要求与某个历史 live 窗口逐 campaign、逐 fill 或 PnL 相等。
- `diagnostic`：评估明确标注的运行时或建模假设，没有晋级资格。当前实测 REST 异步 GLOBAL FIFO 路径使用 Python 和此用途，不得改称 formal，也不得假定完整 C++ 调度器已经支持。

这种分离防止将同日 live 初始状态、配对 receive-time 轨迹或历史进程停顿变成隐含的策略参数。

## 初始状态

正式回放默认按 UTC 日 `fresh_start`：

- inventory = 0；
- entry price = 0；
- 不继承活跃订单、cooldown 计数、markout EMA 或 campaign。

实验可以指定 `frozen_standard`，但必须命名已提交或归档的 `narrowgate_standard_initial_state.v1` JSON 工件；它只能包含 inventory 与 entry price。另有局部诊断恢复接口，但不是完整 live checkpoint：[`replay_state_checkpoint.py`](../../../models/replay/replay_state_checkpoint.py) 会拒绝无法恢复所需运行域的 canonical live snapshot；C++ 还会拒绝更多非空订单/控制域。选择 `live_alignment` 不会让这些缺失域自动可恢复。

## 冻结身份

`models/replay_contract.py` 输出规范 JSON 合同及 SHA256，覆盖：

- live config 文件；
- 活跃模型目录与 BUY fill-selection 工件；
- 经验 P3/effective-kappa 工件及 horizon；
- queue-v3 工件、拟合日与最终运行时 queue multiplier；
- execution trade 来源、merged clock、历史 BBO 要求和 feature-ready-before-decision 语义；
- 初始状态模式与工件；
- 硬风险限额、手续费率和交易所 filter 声明，以及明确的建模局限；
- 延迟环境、分布、seed、scenario 与样本摘要。

所有 arm 继承同一合同。某个 arm 改动模型、P3、queue calibration、event clock、初始状态、延迟样本或 seed 时，必须在回放前报错。策略动作 override 可以按冻结候选集合变化。

## 延迟语义

基线延迟分布带操作者定义的环境/版本标签。迁移主机需要新的 profile 身份和回放合同；具体主机身份属于私有信息，不随公共仓库分发。

当前默认 `--latency-baseline-clip-quantile 1.0` 保留完整观测样本及尾部。更低分位数明确表示裁尾敏感性试验或精确历史复现，不是当前稳定环境默认值。实测异步 GLOBAL FIFO 输入要求 `1.0`。只有 `--latency-scenario stress` 添加罕见合成停顿；stress 结果明确不具备晋级资格。保留观测尾部不等于短样本可以准确估计 p99。

共享 Python/C++ new/cancel 抽样器使用 `keyed_splitmix64_v1`，延迟是以下输入的确定性函数：

```text
latency_seed, event timestamp, originating quote timestamp, side, operation
```

它不是顺序随机数流。因此某个 arm 多一次 cancel，不会使之后所有延迟抽样错位；共同决策仍使用共同随机路径。抽样器 parity 只覆盖这一共享机制，不覆盖所有运行时选项或完整异步调度器。baseline/candidate 各自维护订单、队列、成交与库存状态。

## 因果事件合同

正式证据要求：

- 冻结数据身份声明逐笔 execution trade 时，使用该来源；
- `replay_event_clock=merged` 且 timer interval 为正；
- 历史 BBO/L2 仅在决策边界或之前可见；
- 模型特征仅在 `feature_ready_ts <= decision_ts` 时可用；
- circuit breaker 使用 maker-close；
- 终端 equity 为 `cash + inventory * terminal_mark`，不虚构窗口末尾 taker 平仓。

`formal` 拒绝配对 live receive-time、实测 live requote clock、非零 source-time offset 和完整 live warm-start 请求。受支持的局部对齐输入使用 `live_alignment`；实测异步时序使用上述显式 `diagnostic` 路径。不能只换证据标签来绕过不支持的 backend/clock。快照年龄不是逐消息 delivery delay，不能互相替代。

## 示例

源码 checkout 安装 Python 3.11+ 和 `.[research]` 后，先检查当前帮助入口；它不读取市场数据：

```bash
python -m research.families.f01_fixed_parameter_racing.campaign_outcome_replay_audit --help
```

以下是命令模板，不是可直接运行的公共数据 fixture。用研究冻结输入替换 config、日期与环境占位符；公共 live 模板不是校准后的 baseline：

```bash
python -m research.families.f01_fixed_parameter_racing.campaign_outcome_replay_audit \
  --config "${NARROWGATE_PRIVATE_CONFIG_ROOT}/<frozen-replay-config>.yaml" \
  --days YYYY-MM-DD \
  --strict-calibration \
  --replay-purpose formal \
  --initial-state-mode fresh_start \
  --latency-profile-id "<operator-defined-profile-id>" \
  --latency-environment "<operator-defined-environment-id>" \
  --latency-scenario baseline \
  --latency-baseline-clip-quantile 1.0 \
  --rng-seed 42 \
  --latency-seed 59 \
  --engine python
```

runner 在 daily/rollup 产物旁写入 `*.replay_contract.json`，回放结果也带合同摘要。仅当所选 policy/runtime 选项已实现且有对应 Python/C++ parity 检查时，才使用 `--engine cpp`；共享内核测试通过并不足够。

## 解释边界

F01 campaign 归因使用模拟订单的实际成交价 `quote_px`，不是触发撮合的市场成交价
`fill_trade_px`。账本纳入带符号手续费，保留物理 `fill_sequence`，并将跨零成交
拆成关闭旧仓与开启新仓的经济腿，按数量分摊费用。开仓手续费属于新 campaign。
完整窗口的期末标记只估值残余库存，不制造平仓；未关闭 campaign 保持未关闭。
物理成交笔数与经济腿数是两个不同统计量。

这些 campaign 路径只在成交和最终窗口标记处估值，不在所有市场事件之间连续盯市，
所以路径极值只是成交点估值诊断。当前 runner 不计资金费和运行成本；
`economic_pnl_complete` 表示本地成交账本覆盖完整，不表示全部账户经济成本完整。
独立 fresh-start 日相加不是连续部署 PnL。新的动作价值标签必须使用共同绝对终点，
并覆盖研究要求的成本与状态，不能在修正归因后静默继承旧 campaign 标签。
旧结果文件保留；重新生成的归因是新的派生结果，不是改写执行历史。

Live alignment 成功意味着解释或发现了单位、时钟、状态迁移或安全检查顺序中的结构性错误。不可观测的队列优先级、异步 ACK/fill 顺序、继承 campaign 状态或随机网络停顿可以造成剩余差异，不要求逐 campaign/PnL 复制。策略比较使用冻结的正式 baseline。
