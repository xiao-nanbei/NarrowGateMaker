# AWS EC2 Live 运维手册

<p><a href="aws_ec2_live.md">English</a> | <a href="aws_ec2_live.zh-CN.md">简体中文</a></p>

Last materially synchronized: 2026-09-05

Last materially modified: 2026-09-05

本文描述公共 NarrowGateMaker 代码在 AWS EC2 上的可复用部署模式，不包含当前主机、
credential、账户状态、active release、策略参数或 artifact identity。
`203.0.113.10` 是 RFC 5737 文档地址，不是部署目标。

五分钟上手指本地 synthetic demo。Live 部署是另一条由 owner 操作、消费私有配置和
交易权限的流程。

## 公共/私有边界

公共仓库提供 source publication、runtime verification、deployment envelope、
reconciliation 和进程入口工具。以下内容必须由 operator 提供并保存在 Git checkout
之外：

- 真实主机与访问路径；
- credential 与 service environment；
- active config、model、policy 与 authorization bundle；
- Linux wheels、依赖 lock 与 wheelhouse；
- deployment、reconciliation、activation 与 rollback 记录；
- 当前 health 与账户、订单、持仓状态。

不得把这些值粘贴进本文、issue、测试 fixture 或 tracked example。以下命令只使用逻辑
placeholder。

## 一次性实例准备

使用支持 systemd 和 CPython 3.12 的 64-bit Linux 镜像。Linux wheel 应由受控 Linux
builder 或 CI 构建，activation 时不能临时从网络解析依赖。

当 `narrowgate.service`、旧 `narrowgate-maker.service` 或任一 maker 进程正在运行时，
禁止在 EC2 主机编译 native extension。Canonical `make native-live-wheel` 入口会执行该
检查，默认只使用一个编译 job，并在 `MemAvailable` 低于 2,048 MiB 时拒绝启动。因此
2 GiB live 主机不属于合格 build host。优先使用至少 16 GiB RAM、精确
GNU C++ 11.5.0 的受控 Linux x86_64 Azure builder，但构建必须发生在 Amazon Linux 2023
或 manylinux_2_34-compatible 的 glibc 2.34 rootfs，并使用 CPython 3.12。通用 Ubuntu
24.04/glibc 2.39 产物不是 EC2 release artifact。在兼容 rootfs 中产出 live-only
`ec2-cascadelake-avx2` wheel，再把 immutable wheel 传入 release wheelhouse；不得使用
Azure 主机的 `-march=native`。关闭网络前，先从受控本地 wheelhouse 将
`cpp/pyproject.toml` 声明的 build requirements 及其传递依赖安装进专用 builder
environment。Production target 在 `PIP_NO_INDEX=1` 下使用
`--no-build-isolation --check-build-dependencies`；缺少 build dependency 属于 bootstrap
失败，不能因此联网解析。在 Amazon Linux 2023 qualification 冻结它们之前，build-tool
版本仍只是实测 builder input。EC2 安装后仍必须通过 native build receipt、import 与
Python/C++ parity；target-host 性能仍只能在 EC2 上测量。

主机要求：

- 加密 root storage；
- 不开放 public application port；
- 管理访问只允许经过批准的路径；
- 出站只允许部署所需的 venue 与运维 endpoint；
- 独立的非特权 `narrowgate` service user；
- release 与 private-artifact root 分离；
- systemd 是 live 唯一进程 owner。

SSH 示例可在 operator 的 `~/.ssh/config` 中使用 alias：

```text
Host narrowgate-example
    HostName 203.0.113.10
    User narrowgate
```

该地址仅用于文档。可用时优先使用 AWS Systems Manager Session Manager，否则把 EC2
security group 限制到 operator 的真实来源网络。不得提交 proxy、key path、host key
或真实地址。

一次性创建稳定目录：

```bash
sudo useradd --system --create-home --shell /bin/bash narrowgate
sudo install -d -o narrowgate -g narrowgate -m 0755 /opt/narrowgate/releases
sudo install -d -o narrowgate -g narrowgate -m 0700 /opt/narrowgate/private
sudo install -d -o root -g root -m 0700 /etc/narrowgate/releases
```

Release checkout 只含公共源码。Private root 保存 release-scoped runtime 与私有输入。
Root-owned mode-`0600` environment file 供 systemd 选择这些输入，避免 credential 出现在
命令行。

## 首次 release

从 clean operator/build checkout 发布一个精确 source release：

```bash
export NARROWGATE_RELEASE_TAG="<annotated-release-tag>"
export NARROWGATE_DEPLOY_TARGET="narrowgate@narrowgate-example"
export NARROWGATE_RELEASE_DIR="/opt/narrowgate/releases/<release-id>"

make publish-source-dry
make publish-source
```

Native 输入发生变化时，先在受控 builder 上产出 immutable native wheel，再 stage runtime
closure：

```bash
make native-live-wheel
```

该 target 默认写入 `dist/native/live/<full-git-commit>/`，因此 live-only artifact
不会与完整 developer wheel 或其他 commit 的 wheel 冲突。它排除 tick replay、F03
batch、F06/F07 与 dynamic-hazard research runtime；默认 developer CMake build 仍是完整
extension。该 target 不访问 package index，只消费已经准备好的 build environment。它有意与
`publish-source`、deployment preflight、
installation 和 activation 分离；这些 live-host 操作均不得编译源码。

`publish-source` 只传输源码。它不得传输 credential、private config、model、policy
artifact、runtime receipt 或 process-control authority，也不得启动或重启 live。

通过 operator 批准的私有通道，把完整 Linux runtime closure 放入
`/opt/narrowgate/private/<release-id>`。然后：

1. 使用 `live.deployment_runtime install` 创建 `venv-<execution-commit>`；
2. 运行 `verify-install` 与 `verify-static-tree`；
3. 通过该 release ignored 的 `.venv-active` selector 暴露精确环境；
4. 在 installed environment 中运行 `live.native_build_receipt`，把 native ABI 与
   Python/C++ parity smoke 绑定到本 release；
5. 对 private active config 运行 `make deploy-preflight`；
6. 根据精确 config、runtime、native、model 与已启用 policy bundle 构建 deployment
   envelope；
7. 所有输出记录保存在 release-scoped private directory。

命令帮助是 canonical field reference；不要把某次 release 的命令 transcript 复制进
文档：

```bash
python3.12 -m live.deployment_runtime install --help
python3.12 -m live.deployment_runtime verify-install --help
python3.12 -m live.deployment_runtime verify-static-tree --help
python3.12 -m live.deployment_runtime build-envelope --help
python3.12 -m live.native_build_receipt --help
```

Artifact closure 不完整属于 staging failure，不能通过允许 live host 联网解析依赖来
修补。

## Python-only 增量 release

只有同时满足以下条件，Python-only 修改才可以复用已经准入的精确 native wheel
字节：

- C++ source、compiler/runtime dependency、binding declaration、native ABI 和 native
  build option 都未改变；
- 被复用 wheel 与目标 OS、architecture 和 CPython 兼容；
- 新 release 仍通过 native import 与 Python/C++ parity smoke；
- operator 从新 clean commit 构建新的 Python root wheel。

第一项必须比较两份实际运行的 immutable release tree，包括 `cpp/`、
`pyproject.toml`、native build options 与 dependency/toolchain identity。不能假定
`git diff <old> <new>` 一定可用：cherry-pick 形成的 release graph 或最小 Git bundle
可能不包含旧 commit object。只要 release-tree comparison 发现任一 native 输入变化，
就必须重建 native wheel。

复用 native wheel **不代表**可以复用旧 virtual environment、install receipt、native
receipt、envelope 或 reconciliation；这些记录绑定旧 source/runtime 关系。

增量流程：

1. 发布新的精确 source release；
2. 比较新旧 immutable native-input tree；只有完全相同时才复制或引用已准入 native
   wheel；
3. 从新 commit 构建新的 root wheel；
4. 使用冻结 wheelhouse 创建新的 `venv-<new-execution-commit>`；
5. 生成并验证新的 install receipt；
6. 对被复用 native wheel、新 root wheel、新 environment 和新 execution commit 重跑
   `live.native_build_receipt`；
7. 重跑 parity smoke 与 preflight；
8. 构建新的 deployment envelope；
9. 执行 fresh stopped reconciliation 与正常 activation。

该路径消除了日常 Python 修改的不必要 native compilation，但不会削弱 commit-bound
runtime proof。无法证明 native compatibility 时必须重建 native wheel，不能猜测。

## systemd 是唯一进程 owner

只使用一个 systemd unit。`live/run.sh service` 完成一次 startup verification 后以 maker
进程替换自身。不得同时使用 `nohup`、第二层 supervisor、cron restart 或手工
`run.sh start|stop`。

```ini
[Unit]
Description=NarrowGate live maker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=narrowgate
WorkingDirectory=/opt/narrowgate/current
EnvironmentFile=/etc/narrowgate/releases/<release-id>.env
ExecStart=/opt/narrowgate/current/live/run.sh service
ExecReload=/opt/narrowgate/current/live/run.sh reload
Restart=no
KillSignal=SIGTERM
TimeoutStartSec=120
TimeoutStopSec=120

[Install]
WantedBy=multi-user.target
```

systemd 必须直接发送 `SIGTERM` 并保留完整 stop grace period。不要通过 `ExecStop` 再加
一套五秒 kill ladder。

## 每次 activation 前都要 fresh reconciliation

每次 start、restart、rollback 或 instance resume 都要新建 stopped exchange
reconciliation。旧文件只能证明过去某个 stopped 时刻。

如果 source、private environment、active config、locked runtime 与 deployment
envelope 已在 EC2 上准备完成，使用单一事务入口，不再人工拼接 activation 步骤：

```bash
python3.12 scripts/live_deploy_common.py activate-prepared-release --help
```

默认是无副作用 dry-run；只有 `--execute` 才执行一次 SSH 远端事务。Service identity 默认
为经过校验的 EC2 contract 用户 `ec2-user`，其他合法非 root 用户通过 `--service-user`
指定。停机前失败不会影响旧服务；停机后失败会让主机保持 stopped；candidate 已启动后且
pointer rename 前的 health 失败会停止 candidate。命令不会盲目重启旧 release，并且只在 bounded health
admission 通过后发布 current pointer。批准的控制路径需要 SOCKS5 时，只能使用经过校验的
`--socks5-proxy HOST:PORT`，不能注入任意 SSH option。

正常 prepared transaction 接受正在运行的 transient `narrowgate.service`。Stop 前会
证明 `active/running`、`Transient=yes`、精确 previous working directory、正数
`MainPID`、匹配的 `/proc/<pid>/cwd`，以及 previous release 的 `live/main.py` command
line。Persistent unit 或不明确进程会在 stop 前失败，且不会被修改。只有同一事务已经停止
经过验证的 previous release、但在 reconciliation 前失败时，才使用 `--resume-stopped`。
该模式要求 unit inactive 或不存在、进程严格 quiescent、current pointer 仍指向 previous，且
reconciliation/activation output 均不存在；随后重新验证 candidate 并生成 fresh
reconciliation。

若已选中的 live 进程后来因 execution state uncertain 以 78 退出，应使用独立的
`--recover-runtime-fatal`。调用方要提供该 selected release 的 deployment envelope、
activation receipt 和 stopped reconciliation 作为 lineage evidence；事务还会核验 activation
绑定的 runtime identity、最终 fail-closed runtime-health 中的同一 PID，以及 systemd journal
同一 invocation 的 operator-gated message 与 `EXIT_STATUS=78`。它只接受 inactive 或不存在的 unit 和
完全不存在的 maker/supervisor 进程。新 candidate 的 reconciliation/activation 路径必须唯一
且尚不存在，旧文件绝不当作 fresh evidence；journal 证明缺失或已轮转时，主机继续保持
stopped。该模式不会放宽 `--resume-stopped`。

两次 journal 查询都是有界流式读取：第一次由 runtime identity 的 PID 与时间戳约束，第二次
由已发现的 systemd invocation 约束；恢复流程不会在 EC2 内存中缓存该 unit 的完整历史日志。

私有 systemd `EnvironmentFile` 使用独立的 `NAME=value` 行；追加 deployment grant 前必须
确保文件以换行结束。只校验变量名存在性和语法，不能打印 secret value。

Admission 同时要求 `reconciliationPending=false`，且 `lastTickAge` 是 `[0, 1s]` 内有限值；
该界限来自现有 100ms 主循环安全时钟及有界 scheduler allowance。Private user stream 必须
已连接，且 advancing health observations 使用同一个正数 connection generation；观察窗口
不要求出现 private fill 或 user-stream event。Pointer rename 是提交点；若之后 parent-directory
`fsync` 失败，命令报告 commit uncertain，并保留 candidate 运行供人工核验。

进程内部采用 private-first 启动边界：warm-up 与遗留订单撤销、private stream ready、在完整
private callback 屏障内做精确对账、发布 prospective epoch 并挂载异步 writer、释放 callback、
启动 public market stream，最后启动周期 metrics polling。这个顺序防止 market event 或处理到
一半的 private callback 穿过 initial-state/evidence 边界。

正常模式的安全 activation 顺序：

1. 在旧服务仍运行时验证 prepared candidate 与 deployment envelope；
2. 停止 systemd，等待进程优雅退出；
3. 确认没有 maker process；
4. 在 live 完全停止时创建唯一、此前不存在的 reconciliation output；
5. 要求 signed venue read 在目标 credential/config 下显示 zero open orders 与 stable
   exact position；
6. 通过 systemd 启动 prepared release；
7. 在 bounded interval 内观察 process 与 runtime health；
8. 构建 activation receipt；
9. 最后发布 current pointer。

Runtime-fatal recovery 不会对已经死亡的服务执行第 1–3 步。它先验证 selected release
lineage、最终 fail-closed health、可信 systemd exit-78 invocation 与全局进程静默，再用新的
reconciliation output 从第 4 步汇入同一事务；第 4–9 步保持不变。

Reconciliation 应通过 bounded transient systemd unit 运行，这样它能读取同一个
root-owned environment，而不会把 credential 暴露到 operator shell。每次 attempt 使用
唯一 unit 与 output 名。Condition-skipped 或已被 collect 的 unit 不代表 fresh success。

## 判断 `LoadState=not-found`

使用 bounded read-only status query：

```bash
sudo systemctl show narrowgate.service \
  --property=LoadState,ActiveState,SubState,MainPID,ExecMainStatus,StateChangeTimestamp
```

`LoadState=not-found` 只表示 systemd 当前没有加载该名称的 unit definition。它本身不能
证明：

- maker process 不存在；
- exchange order 不存在；
- position 已完成 reconciliation；
- transient unit 已成功完成。

如果 `narrowgate.service` 应为 persistent，`not-found` 是 host configuration error：
activation 前必须恢复并 reload 经审核的 unit。如果预期 transient unit 已被 collect，
需要另外核对 command result 与新建 output。两种情况下，都必须检查 process family 并
执行 fresh stopped reconciliation，之后才能认为主机可安全 activation 或 stop。

## Health admission 与 current pointer

PID 正在运行是必要但不充分条件。Admission 必须确认：

- systemd 报告一个 active main process，且没有 restart loop；
- runtime health 当前有效，quote loop 持续推进；
- position 与 open-order reconciliation 已收敛；
- ownership conflict、fatal runtime 与 reconciliation-required latch 均未激活；
- 必需 market-data 与 private-event clock 当前有效；
- 最近日志不存在 unknown execution state。

Health admission 通过后，用 `live.deployment_runtime` 创建新 activation receipt 并发布
紧凑 current pointer。Pointer 只是 selector：

```json
{
  "release_id": "<release-id>",
  "activation_receipt_sha256": "<activation-receipt-root>",
  "schema_version": "<current-pointer-schema>",
  "status": "selected_activation"
}
```

`selected_activation` 是 lineage state，不是 health assertion。不得向 pointer 添加 leaf
artifact inventory、host routing、账户状态或 live metric。

## Live 热路径运行合同

准入后的进程把 canonical evidence 与 health publication 移出 decision thread 和
private-event thread。一个有界 FIFO worker 独占 CSV descriptor 与 atomic JSON
publication。Producer 在入队前冻结 payload；已接受 item 使用同一个全局 sequence，不能
静默丢行或重排。Queue 满、worker failure 或 I/O failure 都是 health 可见的 fatal
condition，不能伪装成成功采集。正常退出时先停止新 admission，再等待 FIFO barrier，drain
全部已接受 item，flush 并关闭 descriptor，同时报告 accepted、committed 与 uncommitted
计数。这是进程内顺序保证，不承诺抵御断电、kernel failure 或 storage failure。

USD-M REST traffic 使用独立的 persistent single-owner session。运行时会预先分配 BUY、
SELL 和 safety order session，但行为等价的默认路径仍让 BUY、SELL 与 cancel-all 共用一个
legacy global order session；只有显式启用 cross-side A/B 后，两条 side session 才进入热路径。
reconciliation、reconciliation worker fetch、market snapshot、metrics 与 listen-key
maintenance 始终分别走独立的 cold-role session。每个 pool 均有界且禁用 HTTP 自动重试。
这样冷请求的长尾不会占用当前生效的报撤连接，结果不明确的 order write 也不会被自动重放。
Write timeout 或其他 uncertain outcome 仍必须进入 exchange reconciliation。

Asynchronous response isolation 与 cross-side concurrency 是两个相互独立、仅重启生效的
A/B 开关，默认均关闭。只启用 `async_order_lanes_enabled` 时，等待 response 会移出 decision
thread，但 BUY/SELL 写入仍共用一个 `GLOBAL` FIFO，保持原有跨侧到达顺序。额外启用
`cross_side_order_lanes_enabled` 才会建立独立 BUY/SELL lane；它会改变交易所的跨侧到达
时序，属于经济 lifecycle 实验，不是透明的性能重构。

在 2-vCPU/4-GiB host 上，evidence 与 order-lane queue 必须保持有界，GC telemetry 只使用
固定 histogram bucket 而不保留样本，cold reconciliation 维持 single-flight。该规格不得
并发运行多个冷对账 fetch；必须为 decision、private-event 与 safety path 保留内存和一个
CPU 的余量。

Fill-cooldown checkpoint 不能延后 fill 后的即时风险动作。Engine 先撤销仍会继续增加
exposure 的订单，再把更新后的 checkpoint 写入带 sequence、checksum 的双槽 WAL。启动时
恢复最新有效 slot，撤销 stale exchange order，reconcile checkpoint 之后的 trade gap，
并在 position/open-order admission 完成后才恢复报价。任何顺序变更都必须覆盖每个写入
断点的 crash test，包括 newest-slot torn write、restart gap、duplicate fill 与 stale
order。

Signal computation 分成两条路径。请求已经完成的 10 秒 bucket 时，在复制历史 bar 或 L2
之前直接返回 cache。只有启用 native signal profile 时，新 bucket 才在 C++ ring 中增量
维护 execution-book rolling state，并只 materialize 必要 feature；Python fallback 不声称
具备这项优化。两条路径必须保持 feature、causal cutoff、prediction 和 action parity；
activation 必须暴露实际启用路径，延迟报告也必须把 `cached`、`new_bucket` 与 `catch_up`
样本分开。

## 有界 WebSocket order gateway A/B

Persistent REST 仍是 production default。`websocket_api_ab` 只是针对 Binance 官方 USD-M
WebSocket API 精确 endpoint 的 restart-only、短时 qualification transport；它不是 hot
reload switch，也不是自动 fallback。它必须作为独立 immutable release/config envelope
准备，同时保留一份完整可激活的 REST rollback release。

Gateway 预连接、最多允许一个 in-flight request、为每次请求分配唯一 identity，并通过
central FIFO 写入逐请求 evidence。Evidence 必须保留 transport request identity、client
order identity、dispatch time、authoritative ACK/error time、outcome 与 connection
generation。只要 frame 可能已经发出，timeout 或 disconnect 就属于 `UNKNOWN`：不得重试
write，必须停止新增 authority 并 reconcile。

Activation 前必须设置 hard `max_runtime_s`，并预先调度 verified REST rollback，使其留出
足够余量在 hard bound **之前**完成。Hard timer 只是停止 candidate 的 fail-safe，不是
rollback mechanism。Active rollback 无法完成时，应让 host 保持 stopped 并 reconcile，
不能延长实验。REST 与 WebSocket 必须按同一 request/client-order identity，比较 decision
→ private visibility 延迟，并同时比较 authoritative outcome 与 `UNKNOWN` rate。内部
decision → wire 时间不能作为跨 transport 主指标：当前 REST SDK 没有暴露可与 WebSocket
同口径直接观测的 wire time。ACK/error latency 与 reconnect 只作为辅助诊断，不能只看
ACK p99 更低就授权 transport。

## Activation 后延迟观测

Health admission 只能证明 release 安全启动，不能证明延迟已经改善。Live hot path 有变化
时，每次只观测一个 release 与 process epoch，并排除 restart/warm-up 行。

- 对 `live_perf_telemetry.csv` 只选择 `event=requote,status=ok`；拒绝负值和非有限
  duration，并对 requote total、quote computation 与 order update 报告样本数、p50、
  p95、p99 和最大值。
- Signal timing 必须按记录的 `cached`、`new_bucket`、`catch_up` path 分开，不能把不同
  工作量混在一个分布中。
- REST 分布只使用真正发生该 operation 的行，并报告 request count。只有 count/sum 的
  health 聚合字段只能计算 mean，不能据此声称 p99。REST/WebSocket 比较必须匹配同一
  request/client-order identity，报告 decision-to-private-visibility 与 authoritative
  outcome/`UNKNOWN`；不得比较 observation contract 不一致的 transport 内部 wire 时间。
- 对 terminal-driven replacement，分别统计 `arm`、`publish`、`decision` 和 `drop`
  marker，按 side 报告 terminal-visible-to-decision latency，并列出所有 drop reason。
- 单独记录 systemd restart count 与非 `ok` outcome；成功路径变短不能掩盖 failure 或
  restart churn。

相邻但不重叠的市场窗口只能作为运维观测，不能当作受控因果比较。通过延迟目标也不等于
PnL 改善、经济 no-harm 或 action authority 已经成立。

## Stop、resume 与 rollback

安全停止 instance：先停 systemd，确认进程退出，创建 fresh stopped reconciliation，
之后才能从批准的 control host 请求 EC2 stop。任一检查不可用或不确定时，保持 instance
运行并人工 reconciliation。

启动已有 instance 不等于 activation。确认 systemd 仍为 inactive，验证 selected
release 与 static runtime，创建 fresh stopped reconciliation，再启动并重复 health
admission。

Rollback 是另一次 verified deployment，必须重复 stop、fresh reconciliation、selector
change、startup、health admission、activation receipt 与 pointer publication。不得使用旧
reconciliation 或 pointer 盲目 restart。

## 相关文档

- [运维目录](README.zh-CN.md)
- [本地 live dry-run](live_dry_run.md)
- [公开/私有文档合同](../public_private_documentation_contract.zh-CN.md)
- [AWS Session Manager](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager.html)
- [EC2 security-group rules](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/security-group-rules.html)
