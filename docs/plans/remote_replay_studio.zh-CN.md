# 远程回测工作台

[English](remote_replay_studio.md)

Last materially modified: 2026-09-07
Last materially synchronized: 2026-09-07

## 现在能做什么

Replay Studio 已提供浏览器工作台、持久化控制服务和独立 HTTP worker。首个适配器执行已有的[公开合成回放](../../examples/replay_demo/README.zh-CN.md)，没有另写撮合引擎。浏览器或 SSH 断线不会取消已提交的实验；只要状态目录仍在，控制进程重启后可以恢复任务和已发布结果。

这是能运行的第一阶段交付，不是已经完成的研究平台。已完成的 owner 私有 B0 结果现可导入并只读查看，与合成任务分开。真实行情 F01/F10 执行和训练后的 E/C 候选仍关闭。创建两个演示臂会在独立输出目录运行同一 fixture；臂的名字不会自动产生不同经济策略。

服务不会启动 maker、读取实盘凭据、导入 current-live pointer、创建云资源或晋级策略。任务提交只接受内置合成数据集，不要通过演示适配器上传私有行情、账户或研究工件。B0 结果导入是 owner 本地 CLI 操作，不提供 HTTP 上传或任意路径读取接口。

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

## 只导入已完成 B0，不重跑

Owner 提供已有私有 `baseline_summary.json`、同目录 `input_plan.json` 及摘要选定的最终 segment 产物。这些输入保存在私有证据存储中，不随公开仓库分发。使用控制服务同一个 owner-only 状态目录（权限 `0700`）：

```bash
.venv/bin/python -m narrowgate.studio import-b0 \
  --state-dir "${NARROWGATE_RESULTS_DIR}/studio-control" \
  --summary "${NARROWGATE_PRIVATE_EVIDENCE_ROOT}/<tag>/baseline_summary.json"
```

导入器只按明确选定的 segment stems 读取小型摘要、输入计划、segment 元数据和汇总 CSV，检查覆盖、已完成 baseline/config 元数据及金额一致性。原始 fill、campaign 和 funding 文件只确认存在，不重新读取或计算哈希。缺失、partial、重叠或越出摘要目录的输入会被拒绝。现有私有 SQLite 只保存字段白名单内的精简报告，不复制原始工件或来源路径；一个由摘要生成的 ID 保证重复导入幂等。后续研究阶段、训练或执行许可说明改变，不会阻止查看既有 B0。导入不会创建 job，也不启动 replay、worker、Azure 同步或云资源。

独立的“真实 B0”视图使用只读 `/api/results` 接口，显示覆盖 UTC 日数、连续段、会计金额、选定产物的 local/Azure 执行来源，以及源摘要已记录的跨主机核验说明。来源是历史出处，不是当前云节点存活状态。导入一致性检查不会重做原始 fill/funding 或跨主机资格核验。`daily.csv` 每行仍是 segment 汇总；界面不制造每日收益、Sharpe 或账户权益曲线。交易 PnL 已含手续费和终点 MTM，资金费只加一次。native queue 缺失覆盖和运行时模型限制仍明确显示。合成 demo 任务及其 worker 保持独立。

## 连接市场上下文与历史模拟成交

导入报告后，owner 可按同一批明确选定的最终 fill trace 建立私有展示索引，并连接已有 BTCUSDC 市场 K 线。此操作不重新回放策略，也不修改原摘要、产物或会计金额。可选 `--bars-dir` 指向已有 `BTCUSDC-1s-YYYY-MM-DD.parquet` 文件；省略时仍可查询成交，但 K 线明确保持不可用。

```bash
.venv/bin/python -m narrowgate.studio connect-b0 \
  --state-dir "${NARROWGATE_RESULTS_DIR}/studio-control" \
  --result-id "<imported-result-id>" \
  --summary "${NARROWGATE_PRIVATE_EVIDENCE_ROOT}/<tag>/baseline_summary.json" \
  --bars-dir "${NARROWGATE_DATA_ROOT}/bars_1s"
```

连接信息和派生成交索引留在 owner-only SQLite 内。只有 CLI 能登记本地路径；浏览器 API 只接受结果 ID、UTC 时间窗和分页游标，不接受任意路径。重新连接仅替换展示索引，不改已导入的不可变报告。不复制、重新哈希或上传原始市场文件。

`GET /api/results/{id}/market` 返回 segment／UTC 日覆盖和源可用性。`GET /api/results/{id}/candles` 必须提供最多 24 小时的 UTC 毫秒半开区间（`start_ms`、`end_ms`），边界对齐 `interval_s`，取值为 `1,5,60,300`，最多返回 5,000 根 K 线。OHLC 和成交量只聚合已有市场 bars；缺行的秒不补造，也不前向填充，其数量不能区分没有成交的秒和数据源缺口。该行情明确标记为历史市场上下文 `context_only_not_exact_replay_binding`；登记本身不证明它与原回放输入字节完全一致。市场文件缺失、不可读或字段非法时返回明确不可用原因，绝不用策略自身成交拼 K 线。

`GET /api/results/{id}/fills` 使用同样有界的 UTC 时间窗，`limit` 最多 1,000，并通过不透明游标续页。同一时间戳的多笔成交分别保留，ID 按 segment 隔离，同时保留原始 fill sequence。执行 `price` 使用原账本价格 `quote_px`；触发行情成交价和订单限价是独立字段。物理成交时刻与私有可见时刻保持分开。成交前后库存是原日志的本地回调账，不编造成按物理成交顺序重建的库存。签名手续费保留正值成本、负值返佣；原交易 PnL 已含该费用。`campaign_id_at_submit` 不改称最终 campaign ID。

`GET /api/results/{id}/orders/{order_id}` 仅返回实际成交时记录的订单快照，最多 1,000 笔成交，超过时明确标记截断。目标报价建议不会变成有效订单。未成交订单生命周期、后续撤单结果和订单 PnL 未被原记录保存时保持不可用；缺失字段返回 null，生命周期完整性始终为 false。成交和订单统一标记 `simulated_historical_fills`，不是实盘成交。已有汇总报告仍为会计金额视图；图表查询不制造 PnL 曲线。

## 查看已有数据质量证据

Owner 可以把已有分源／版本审计 CSV 和只读文件元数据清单适配到日历，不下载数据、不重跑审计，也不改变冻结的回放输入：

```bash
.venv/bin/python -m narrowgate.studio_quality \
  --state-dir "${NARROWGATE_RESULTS_DIR}/studio-control" \
  --manifest "${NARROWGATE_PRIVATE_EVIDENCE_ROOT}/operator-selected.json"
```

私有 manifest 指定数据源身份、已有审计列及可选文件清单模式；简明字段映射见 [`import_quality`](../../narrowgate/studio_quality.py)。它不施加新的质量阈值。`GET /api/data-quality/catalog` 列出已登记的数据集和节点。`GET /api/data-quality?start_day=YYYY-MM-DD&end_day=YYYY-MM-DD` 返回最多 366 个 UTC 日的完整含首尾日历，可选 `dataset_id` 和 `node` 过滤。`/api/data-quality/export` 使用相同参数，仅导出修复／同步建议清单，不执行这些操作。

审计状态仅适用于明确的数据源版本和记录的检查范围，不证明另一节点已保存同一版本。文件存在、源质量、任务可用性和节点副本验证分别展示。节点副本不可访问或离线时保持未知，不自动判为缺失。没有区间报告不能画成全天绿色，没有审计的日子仍显示为未知，不从日历消失。日历不授予策略或回放准入。

## 先一台远程主机，再扩展 worker

### 真实计算资源与合成 worker 分开

可在同一个控制服务中指定 owner 私有资源清单：

```bash
python -m narrowgate.studio serve \
  --state-dir "${NARROWGATE_RESULTS_DIR}/studio-control" \
  --resources-manifest "${NARROWGATE_PRIVATE_EVIDENCE_ROOT}/compute-resources.json" \
  --port 8080
```

清单包含 `visibility: local_only_do_not_publish` 和 `resources` 列表。每项指定稳定 `id`、易识别的 `label`、`kind`（`local`、`lan` 或 `azure`）、预期 `roles`（`training`、`replay`、`data_processing`）和固定探测类型。本机探测读取控制主机状态；SSH 探测使用已有主机别名与解释器；Azure Batch 探测使用指定的现有 CLI 上下文、账户与池。地址、账户标识、本地路径和凭据不得写入公开配置或前端源码。删除 Azure 条目或切换已授权账户只需修改配置，不必改写界面。

`GET /api/compute-resources` 读取缓存的资源快照。控制服务在后台进行有超时限制的探测，页面请求不等待 SSH 或 Azure。界面显示友好别名、实测硬件与容量、检查时间、陈旧／不可达状态，以及选定的外部作业状态。Azure 零节点池表示已登记资源，不是在线主机。未配置资源时列表为空，不会虚构三台在线机器。历史 B0 的执行来源不能代替健康探测。

已有作业通过选定的小型状态文件和进程检查接入观察；它们是外部作业，不属于 Studio 队列。观察器不启动这些作业，也不读取未完成的经济结果。合成 worker 记录单独保留用于演示诊断，同一台 Mac 上的两个演示进程不能被计为两台物理资源。角色标签只是任务分配偏好，不证明性能、副本已就绪或真实行情执行器已接通。

分配任务应考虑当前负载、可用内存、数据位置、已验证的运行环境兼容性和实测吞吐。Owner 当前偏好 Mac M4 用于训练、LAN 用于适合的回放和数据准备，Azure 则按当前授权预算与到期日按需使用。M4 不代表每个模型库都使用 Metal。不得靠重启迁移正在运行的任务、为填满空闲资源重复运行 baseline，或把连续状态依赖的日期拆到不同主机。此资源视图不新增任意 shell 执行、自动创建云资源或真实行情任务提交；后者仍需要下文的有界 runner 适配器。

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
| 当前 B0 接入 | 已完成私有 B0 摘要可只读导入，保留连续段会计与导入一致性检查；真实行情任务执行仍关闭。 |
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
