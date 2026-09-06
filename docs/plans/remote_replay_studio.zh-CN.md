# 远程回测工作台

[English](remote_replay_studio.md)

Last materially modified: 2026-09-06
Last materially synchronized: 2026-09-06

## 现在能做什么

Replay Studio 已提供浏览器工作台、持久化控制服务和独立 HTTP worker。首个适配器执行已有的[公开合成回放](../../examples/replay_demo/README.zh-CN.md)，没有另写撮合引擎。浏览器或 SSH 断线不会取消已提交的实验；只要状态目录仍在，控制进程重启后可以恢复任务和已发布结果。

这是能运行的第一阶段交付，不是已经完成的研究平台。真实行情 F01/F10 执行、当前私有 B0 和训练后的 E/C 候选尚未接入服务，界面明确关闭这些选项。创建两个演示臂会在独立输出目录运行同一 fixture；臂的名字不会自动产生不同经济策略。

服务不会启动 maker、读取实盘凭据、导入 current-live pointer、创建云资源或晋级策略。只接受内置合成数据集，不要通过演示适配器上传私有行情、账户或研究工件。

## 跑通完整链路

控制主机和 worker 使用相同、测试过的 checkout 或已安装 wheel，需要 Python 3.11 及以上。wheel 已包含构建后的前端，使用者不必安装 Node.js。

```bash
python -m pip install ".[studio]"
python -m narrowgate.studio serve --state-dir ./results/studio-control --port 8080
```

另一个终端或独立服务启动 worker：

```bash
python -m narrowgate.studio worker \
  --url http://127.0.0.1:8080 \
  --worker-id worker-a \
  --work-dir ./results/studio-worker-a
```

打开 `http://127.0.0.1:8080`，创建演示实验，检查订单、事件轨迹、campaign、账本和日志原文。第二个 worker 使用不同 ID 和工作目录。每个 worker 最多持有一个未完成任务；两个 worker 可以并行运行两个独立臂，但不能由此把连续库存路径随意拆成每天 fresh-start 再拼接。

前端开发和可复现构建见 [frontend/README.zh-CN.md](../../frontend/README.zh-CN.md)。真实数据诊断暂时仍通过公开[一日数据教程](../opensource/one_day_data_pipeline.zh-CN.md)运行，不走 Studio。

## 先一台远程主机，再扩展 worker

```text
浏览器 ── HTTP / SSH 隧道 ── 控制 API + 本地 SQLite + 已发布结果
                                 ▲
                                 │ HTTP 领取 / 心跳 / 发布
                             独立 worker
                                 │
                           现有 canonical replay CLI
```

浏览器调用 HTTP API，不执行 SSH 命令。在本机转发控制主机的 loopback 端口：

```bash
ssh -NT -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=30 -o ServerAliveCountMax=3 \
  -L 127.0.0.1:18080:127.0.0.1:8080 research@control-host
```

然后访问 `http://127.0.0.1:18080`。`control-host` 是操作者配置的 SSH 别名，不是项目附带服务器。服务故意只监听 loopback，不要为了访问它开放云防火墙或改为监听所有网卡。

另一台主机上的 worker 也先建立到控制主机的隧道，再把 `--url` 指向该主机自己的转发端口。worker 通过 API 通信，不能共享挂载并写入控制服务的 SQLite 文件。内置数据随包分发；任意私有数据登记、按数据位置调度仍是后续适配能力。

可选环境变量 `NARROWGATE_STUDIO_TOKEN` 开启 Bearer 验证。控制服务和 worker 使用同一份操作者生成的 token；浏览器“访问凭据”对话框只将它留在页面内存，不写 URL 或 Git。启用凭据后界面使用带认证的轮询，不把 token 放进 SSE URL。这是单 owner、SSH 隧道内的服务，不是公开多用户服务。

## 进程与存储

无人值守时，由操作系统分别管理控制服务和 worker，不让 SSH shell 决定任务生命期。Linux 用户服务的最小 worker 模板：

```ini
[Unit]
Description=NarrowGate synthetic replay worker
After=network-online.target

[Service]
Type=simple
WorkingDirectory=%h/narrowgate
ExecStart=%h/narrowgate/.venv/bin/python -m narrowgate.studio worker --url http://127.0.0.1:8080 --worker-id worker-a --work-dir %h/narrowgate/results/studio-worker-a
Restart=no
KillMode=control-group
TimeoutStopSec=20

[Install]
WantedBy=default.target
```

安装路径确定后统一修改一次。控制服务使用独立 unit 执行 `serve`；如需隧道，也单独管理。`KillMode=control-group` 很重要：强制杀死裸 worker 时，独立会话中的 replay 子进程可能仍然活着。正常退出会先 TERM、等待，必要时 kill 并回收子进程。主机重启或强杀不是可恢复的经济 checkpoint；应保留旧任务，检查进程和日志后再决定下一步。

控制状态目录和 worker 工作目录必须放在持久私有存储上。SQLite 只由控制主机使用，已发布结果与数据库一起保存。任务运行、执行状态未知或待上传时，不要清除 worker scratch/outbox。云临时盘消失会连同未上传结果一起丢失，浏览器重连无法找回。本版没有自动云资源创建、Blob 生命周期或删除动作。

## 故障语义

| 情况 | 行为 |
| --- | --- |
| 重复点击或提交响应丢失 | 同一个 `Idempotency-Key` 加同一输入返回原实验；同 key 不同输入拒绝。 |
| worker 领取响应丢失 | 复用持久 session 时返回原来的未完成任务，不会再领取第二项。 |
| 浏览器／SSH 断线 | 控制队列和 worker 进程不依赖浏览器继续存在。 |
| 控制服务短暂断线 | 网络／5xx 重试有上限；不会仅因 API 短暂不可用立即杀掉正在运行的子进程。 |
| 心跳超时 | 标为 `lost`，不自动判失败或重新排队；仅原 worker/session 可以重连和发布。 |
| 取消排队任务 | 不执行 runner，直接取消。 |
| 取消运行任务 | 保持 `cancel_requested`，直到 worker 终止／回收子进程并发布终态日志。 |
| 取消与计算成功交叉 | 结果发布前取消优先，保留日志但不成为已完成报告。 |
| 上传失败 | 不标完成；精确发布 payload 和日志留在 worker 目录；重启同一个 worker 续传，不重跑计算。 |
| 重启后已有执行目录但没有 outbox | 明确报执行状态未知；不能猜旧子进程已死，也不能重复运行。 |
| 重复发布 | 同一任务相同内容幂等；不同内容或其他 attempt 不能覆盖终态结果。 |

控制服务只有在 summary、trace、receipt、stdout、stderr、环境信息全部落盘并同步，且数据库事务提交后才标记完成。合成结果进入结果库时检查一次参考字节，不在每次查看图表时重读和哈希。没有新增研究 SHA 或许可层级。

演示 worker 实施 600 秒执行上限，上传大小有界。运行中日志仍在 worker 上；浏览器显示的是终态归档日志，不是实时 stdout。进度仅展示真实生命周期状态，不编造完成百分比。

## 结果与会计口径

仅用于显示的 `backtest_report.v1` 包含原 `summary.json` 和顺序不变的 `trace.jsonl`，不是研究授权。界面读取已有现金、库存、费用、campaign、终值 PnL，不重新计算；缺失字段仍为未知。三个演示订单全部显示，包括没有成交的撤单。

真实行情适配器启用前必须保留以下边界：

- `replay_pnl` 已包含交易手续费，不能再扣一次；资金费单独报告，并仅一次进入主净值。
- 连续 segment 汇总不是每个 UTC 日一条样本，应复用连续账本的日切片，不能将整段金额标成第一天的收益。
- 从零开始的 PnL 账本不是账户权益，不能编造收益率、年化表现或资本尺度 Sharpe。
- 队列位置和反事实成交是模型估计。两台主机一致证明实现可复现，不证明真实撮合队列或实盘经济路径完全一致。
- 仅比较完整、环境兼容的结果。各臂分别维护订单、库存、资金费、内生网关排队和随机状态；同一个 seed 不自动证明外生延迟抽样相同。
- 真实事件查询必须按时间、订单、campaign 有界分页；图表抽样不能改变事件顺序、会计或统计计算。

## 后续交付顺序

| 工作 | 当前状态／下一步验收 |
| --- | --- |
| 公开入门文档 | 已将无账户 demo 前置，补一笔订单的完整例子、数据状态表，并区分研究状态与工具可用性。 |
| 远程执行基础 | 已实现合成 demo 的持久队列、独立 worker、取消／失联状态、结果发布和浏览器检查。 |
| 当前 B0 接入 | 尚未开放。先适配完整 canonical F01/F10 结果及其数据／runtime／延迟身份、连续段会计，不修改正在执行的冻结 B0。 |
| E/C 研究 | 独立研究分支已有完整机会采集及单干预配对标签组装；不等于已有训练或通过经济验证的选择策略。 |
| 真实数据登记 | 提交真实市场任务之前，需要明确 Development／Validation／holdout 角色、不可变逻辑数据映射、源覆盖，以及禁止静默替换 backend/feed。 |
| 有界真实执行 | 接入白名单 canonical runner、CPU／RSS／磁盘预算及工件流式发布，绝不从浏览器接受任意 shell 命令。 |
| 完整分析 | 复用 campaign、资金费、scorecard、时序统计；补真实轨迹分页、分源延迟视图和不兼容原因说明。 |
| 多主机验证 | 在目标主机验证 wheel 服务、SSH 隧道、节点丢失、原 finalizer 接收后，才能宣称远程真实行情就绪。 |

研究工作与 UI 独立推进。不能为了填充页面而读取未完整的 baseline 经济结果、启动重复 Azure B0、重启 live 或打开封存结果。

## 验证命令

```bash
python -m pip install ".[studio,dev]"
python -m pytest tests/test_replay_studio.py tests/test_public_replay_demo.py tests/test_public_onboarding.py
python -m ruff check narrowgate examples --select E,F,I,UP,B
```

Studio 测试覆盖并发领取、幂等、重开控制数据库、失联／取消 worker、工件失败、原 demo CLI 执行以及 loopback/API 边界。这不等于 native queue 资格、经济回测或任意主机崩溃恢复证明。浏览器验收应创建两项任务、运行两个 worker、重新打开页面、查看未成交订单和 campaign、取消排队任务，并确认无法提交真实市场或 live runner。
