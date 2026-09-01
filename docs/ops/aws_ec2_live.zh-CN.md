# AWS EC2 Live 运维手册

<p><a href="aws_ec2_live.md">English</a> | <a href="aws_ec2_live.zh-CN.md">简体中文</a></p>

Last materially synchronized: 2026-09-02

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
指定。停机前失败不会影响旧服务；停机后失败会让主机保持 stopped；candidate 已启动后
health 失败会停止 candidate。命令不会盲目重启旧 release，并且只在 bounded health
admission 通过后发布 current pointer。批准的控制路径需要 SOCKS5 时，只能使用经过校验的
`--socks5-proxy HOST:PORT`，不能注入任意 SSH option。

Prepared transaction 当前只接受正在运行的 transient `narrowgate.service`。Stop 前会
证明 `active/running`、`Transient=yes`、精确 previous working directory、正数
`MainPID`、匹配的 `/proc/<pid>/cwd`，以及 previous release 的 `live/main.py` command
line。Persistent unit 或不明确进程会在 stop 前失败，且不会被修改。

安全 activation 顺序：

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
