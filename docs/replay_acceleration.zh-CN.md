# 回测加速：实现与实测记录

[English](replay_acceleration.md) | [简体中文](replay_acceleration.zh-CN.md)

Last materially modified: 2026-09-22
Last materially synchronized: 2026-09-22

这是部分实现报告，不是 P0–P4 完成报告。审阅及实际起始版本为 `ed8b4991f64a26786139a8475214dd06a66df557`；不可变参考源码与逐次指标、结果保存在私有证据库，不随公共仓库发布。没有替换正在运行的冻结研究 worker，没有改动实盘服务或策略参数。

## 实际入口与环境

当前 F03 包装器调用 `simulate_public_inputs` → `load_public_replay_inputs` / `public_predictions` → Python `simulate_tick` → `settle_public_replay`。此前四模型重复解码输入。目标服务器为双路 Xeon Gold 6254，每路 18 核、每核两个线程，即 36 物理核、72 逻辑 CPU；两个 NUMA 节点，约 188 GiB 内存，可用 CPU 集 0–71。已检查的用户 cgroup 没有 CPU/内存上限，但这不代表独占资源。服务器有其他研究任务，检查时约 85 GiB 可用内存。研究 Python 为 3.12.3，该环境未发现 native 扩展。以下本地测量来自 macOS 开发机，不是服务器吞吐结论。

## 已实施改动

- P1：按 ABI、编译器、profile 和配置区分的持久 CMake/Ninja 研究构建目录；可选 ccache、关闭 IPO、保留浮点约束，不改生产 live-wheel。新进程显式加载产物，修复了 editable 安装钩子在设置 `PYTHONPATH` 后仍抢先加载旧扩展的问题。
- P2：新增 `prepare_public_inputs`、`simulate_prepared_inputs`，保留公开包装入口；盘口/方差数组只读，每次账户独立盘口游标。F01 只准备一次并逐候选立即记账；兼容的返回字典接口仍保留所有返回轨迹。公共模型使用单线程批量原始预测，保留全部 13 头，裁剪、EMA 和状态更新复用原实现；native 模型仍走原后端。
- P3：可选原子发布的数值缓存、只读 NumPy 映射、扁平变长盘口价位、准入时源文件/内容验证和独立游标。有界 spawn worker 只交换任务描述，结果写文件，不通过进程队列传巨大轨迹；每 worker 只保留一个分片。这还不是生产级内存准入调度器，必须依据实测内存和现有负载保守选择并发。

## 实测及限制

真实短窗口来自优化前已固定的工程输入，不按 PnL 选取。包含 1,717 笔执行成交、7,697 个合并时钟行、4 次策略成交。加强版对照保留有界决策/报价/成交轨迹并接入真实资金费，共享记账返回完整。三次参考与三次缓存结果文档均逐字节一致，包含经济输出；这不等于完整两日、高密度日或 native 一致性验收。

| 本机指标 | 参考 | 优化后 | 条件 |
| --- | ---: | ---: | --- |
| 短窗口处理耗时中位数 | 4.935 秒 | 2.922 秒 | 各三次；包含加载/模型、记账、序列化；这些记录尚不含最终文件写出 |
| 范围 | 4.802–4.971 秒 | 2.879–3.500 秒 | OS 页缓存未控制，有后台任务 |
| 预测及状态后处理中位数 | 0.626 秒 | 0.155 秒 | 同一 Python LightGBM 后端、全部头、原 EMA |
| 事件循环中位数 | 3.782 秒 | 2.653 秒 | 数值盘口缓存替代反复事实解码 |
| 进程 RSS 高水位 | 497–544 MB | 259–280 MB | 不同进程运行；不是 USS/PSS，未将多个 worker RSS 相加 |
| 固定四任务总耗时 | 单进程 14.584 秒 | 双进程 11.214 秒 | 较早的诊断输出测试未提供资金费，不是完整经济基准 |
| 首次数值缓存准备 | — | 2.486 秒 | 同窗口较早的缓存生成成本，未藏入热运行耗时 |
| 无变更 native 构建 | — | 0.320 秒 | 另加配置 0.597 秒；Ninja 无工作 |
| 注册单元增量编译/链接 | — | 7.104 秒 | 一个目标文件加链接；另加配置 0.597 秒 |

首次 native 编译/链接的 Ninja 时间线为 133.589 秒，但其构建后导入检查因旧 editable 扩展而正确失败，不能称作干净的首次端到端构建时间。修复加载器后已用新进程确认正确产物。未安装 ccache。当前测量命令另计结果文件写出，不能将上述早期记录追认为已包含写出。整体任务收益必须计入准备、进程启动、模型加载和缓存生成；本机未取得 PSS/USS。

参考采样 profile 显示盘口事实解码/转换及逐帧模型调用占有较多耗时。目标服务器工程验证使用已有行情并与运行任务目录隔离。服务器结果、完整账户分片、高密度/缺口场景和并发扫描仍待完成，不可将短窗口收益乘以 72。

## 阶段验收

| 阶段 | 实际状态 |
| --- | --- |
| P0 | 部分完成：真实入口、环境、本机短基准已测；完整代表性分片和目标吞吐未验收 |
| P1 | 已实现且本机验证；目标服务器 native 构建未验证 |
| P2 | 复用/批推理核心及本机一致性测试已实现；完整分片及冻结 F03 worker 集成待完成 |
| P3 | 可选缓存、spawn 入口、本机一致性与 1/2 进程试验已完成；服务器内存准入、NUMA 和并发验证待完成 |
| P4 | 未实现：旧 native 入口拒绝公共盘口 diagnostic 模式、显式 BBO 失效及真实私有成交回报/计算延迟；可续步执行/策略边界及一致性测试仍需实现 |
| P5 | 未实现：尚无共享 native 策略回调热点实测，不能据此引入 JIT |
| P6 | 未实现：当前工作负载是固定模型独立账户，不是反事实分叉任务 |

不能删除 native 拒绝检查、关闭延迟/失效处理，或在 C++ 包装里调用 Python 主循环就宣称 P4 完成。现有 Python 仍为执行语义权威，本次没有新增另一份完整策略。生产研究结果不得静默切换到尚未验收的优化版本。

## 可执行入口

以下已在本机执行，构建目录采用首条命令打印的实际值：

```bash
.venv/bin/python scripts/build_replay_native.py --jobs 2
.venv/bin/python scripts/run_replay_native.py --build-dir <printed-build-directory> --module pytest tests/test_cpp_signal_features.py -q
.venv/bin/python -m pytest tests/test_replay_prepared.py tests/test_research_public_inputs.py tests/test_signal_compute_telemetry.py tests/test_signal_feature_cutoff.py -q
```

以下接口已使用私有输入运行；替换为实际准入路径，不使用历史默认值。输出目录不可覆盖。缓存准备单独报告，比较时必须纳入相应成本。

```bash
.venv/bin/python -m models.replay.benchmark --bundle <consumer-bundle> --params <params.json> --model <model-directory> --funding <funding.json> --output <new-output-directory> --repeat 3
.venv/bin/python -m models.replay.benchmark --bundle <consumer-bundle> --params <params.json> --model <model-directory> --cache <cache-directory> --output <new-output-directory> --repeat 3
.venv/bin/python -m models.replay.batch --tasks <tasks.json> --workers 2 --output <new-summary.json>
```

任务文件为 JSON 列表，每项形如 `{ "id": "unique-id", "bundle": "...", "params": "...", "model": "...", "funding": "...", "cache": "...", "output": "..." }`。model/funding 可省略；缺资金费仍为不完整，不补零。每项保持完整独立账户和原有区间，不擅自按天切开。该工程入口不授予开启 F 或挑候选的权限，F01 仍保留严格报价参数许可边界。失败任务抛错，不能产生完整批次摘要。

## 维护位置

| 变更 | 归属 | 是否编译 |
| --- | --- | --- |
| 参数 | 显式回测配置/研究族候选合同 | 否 |
| 模型 | 冻结公共模型加载器、全部头原始推理 | 无需 C++；新 engine 和预测身份 |
| 行情特征/观察 | data 与特征合同 | 使相关输入缓存 schema/身份失效 |
| 策略规则 | 现有策略模块与 Python 回放权威 | 未新增双份 C++ 策略维护；P4 待完成 |
| 撮合/订单/时钟 | 现有执行器与定向一致性测试 | native 修改需增量构建和新 worker |
| 记账/报告 | 共享 public_accounting 与 benchmark | 否 |

已运行 127 项 prepared/公共输入/signal 测试及 46 项 native feature 测试通过；这些集合可能重叠，不是全仓库审计。包含缓存损坏、A→B→A 隔离、独立盘口游标、全部头 EMA 一致性。完整目标服务器验收仍未完成。
