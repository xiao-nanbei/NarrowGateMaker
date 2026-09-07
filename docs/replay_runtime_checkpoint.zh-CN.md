# Replay 运行状态断点：实现状态

[English](replay_runtime_checkpoint.md)

Last materially modified: 2026-09-08
Last materially synchronized: 2026-09-08

Python tick 循环可以在一个事件执行之前暂停、保存运行对象图，再恰好恢复该事件一次。暂停不是维护停机：不撤单、不平仓、不重置 campaign，也不生成最终会计结果。

## F01 命令行

在原有、保持不变的单臂命令上使用 `--continuous --engine python --workers 1 --arms baseline`，再添加以下参数。时间戳仅为示例，实际截止时间必须位于输入区间内。

```bash
--checkpoint-at-ts-ms 1767229200000 \
--save-runtime-checkpoint /private/run/b0.runtime.pickle
```

成功落盘后输出 `status=checkpoint_saved`、`completed=false`，不会生成提前结算的 campaign、PnL 或资金费报告。使用相同日期、参数、数据和运行环境，移除上述两个参数并添加 `--resume-runtime-checkpoint /private/run/b0.runtime.pickle` 即可恢复；也可以同时指定下一个更晚的保存点。

分批加载使用 `--runtime-input-bounds-ms START END`，两端均为包含在内的毫秒时间戳。完整会计日期仍由原来的 `--days` 定义；只加载与本批范围相交的日输入，每日输入先裁剪再拼接。第一批必须从原始会计起点开始。非最后一批必须在输入终点之前保存，保留真实的前视上下文；恢复批次必须保留截止事件、仍被引用的待处理游标和必要回看窗口。只有最终批次到达原始会计终点后才结算。

每批输入来源记录随断点保存，并进入最终元数据。输入起点保持原始定时器网格；本批消息数和延迟摘要只描述加载输入，最终行明确标注此范围，不能当成全区间累计观察。即使裁剪执行行，也保留父消息完成所需的全部子成交时间。目前调用方仍需安排重叠批次，尚未完成自动批次大小选择或整个 401 日多来源运行；每日准备过程在裁剪之前仍可能临时加载整日数据。不能把这些接口称为已经完成全区间 baseline，也不能用它们拼接独立的日度 fresh-start。

## Python 接口与状态边界

`simulate_tick(..., checkpoint_at_ts_ms=cut_ms)` 返回 `_replay_checkpoint`。使用 `models.replay.runtime_checkpoint_io` 中的 `save_runtime_checkpoint` 和 `load_trusted_runtime_checkpoint` 保存、读取，再通过 `resume_checkpoint=` 恢复。默认要求相同输入窗口；配合 `resume_input_batch=True` 可以换成具有不变重叠前缀的新窗口。

对象图保留订单共享引用、库存、会计、随机状态、未决 new/cancel、私有成交回调、HTTP/GLOBAL FIFO、计算阶段、策略状态、计数器和轨迹。原生订单簿迭代器从保存的文件内游标重新打开；已预取事件和已重建订单簿保留。运行进度回调由新进程提供。已排空订单状态的 `ContinuousReplayState` 是另一种局部会计快照，不能替代这个完整运行状态。

Python BUY/SELL 冷却适配器会保留 EMA、窗口、截止时间和计数器，仅重建进程锁。可选 native cooldown 热路径尚无状态导出，遇到它会拒绝，而不是静默换成新的 Python 状态；完整 C++ tick 循环和任意研究对象也不因此自动获得恢复能力。真实私有 B0 已通过代表性一小时的同一窗口恢复和实际输入裁剪对照，决策、报价、成交、campaign 及资金费与不中断运行一致。进程耗时和加载输入计数不要求相同；该测试不等于所有日期、跨供应商边界或可选策略均已验证。

### 保留来源差异的原始 L2 恢复

[现有盘口调度器](../models/exchange_book_replay.py)中的 `TardisExchangeBookTape` 读取明确指定的原始 Tardis L2 文件。它将跨 CSV 读取批次的一整条快照或增量原子分组，保留交易所时间和供应商接收时间，复用同一盘口重建实现。供应商时间不是部署主机延迟的实测值。组件遇到非法档位或来源时钟倒退会报错，不会重排记录或编造缺失事件。

这些记录不带 Binance 交易所序号，因此使用 `sequence_scope=provider_ordered`，不伪造 `U/u/pu`；严格交易所序号模式拒绝这种输入。模型诊断调度器可以应用并保存／恢复其盘口，但即使之后切回 native 来源，仍报告包含供应商顺序数据的证据范围。切换供应商需要真实快照，替换上一来源的盘口档位，不重置策略和账户状态。组件测试和短段真实重建对照不能证明全日历回测就绪。F01 来源计划接入与完整供应商切换验证尚未完成；这不是 native 输入缺失时自动启用的降级路径。

当前测试中的局部排名需要 120 秒历史成交，成交诊断可能读取未来 5 秒。其他启用的消费者可能需要更长上下文，未决订单或计算也可能引用更早的盘口。必须保留实际数据，不能用新造快照替代。每个来源的延迟抽样使用全局行号；`execution_message_delivery_params(..., prior_delivery=previous)` 保留已有消息到达时间和各连接的回调积压。每批覆盖统计不自动等于全区间覆盖报告，最终元数据必须保留各批来源记录。

## 持久化与验证

断点先写私有临时文件，flush/fsync 后原子替换，并同步目录；写入失败不覆盖前一个完整断点。文件使用 Python pickle，只允许读取自己可信进程生成的本地文件，禁止经 Studio 接收上传的 pickle。它不是跨平台交换格式，也不能替代既有配置和输入清单。

`tests/test_tick_runtime_checkpoint.py` 验证异步退出、在途成交、消息可见性、盘口前视及计算延迟下的重复保存／恢复。`tests/test_tick_runtime_input_window.py` 验证实际输入裁剪、数组游标、原生文件游标、策略和消息 FIFO 的连续性；F01 测试覆盖最终会计只运行一次。合成测试证明对应实现路径，不证明策略收益或所有私有配置都已通过。
