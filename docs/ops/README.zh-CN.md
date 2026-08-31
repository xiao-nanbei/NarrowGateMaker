# 运维说明

<p><a href="README.md">English</a> | <a href="README.zh-CN.md">简体中文</a></p>

Last materially modified: 2026-08-31

Last materially synchronized: 2026-08-31

运维文档涵盖 dry-run 设置、私有配置边界、部署 guardrail 和 telemetry。这些文档不得包含私有主机、账户详情或原始 live PnL。

- 本地 dry-run 工作流：[live_dry_run.md](live_dry_run.md)
- Live 部署回执与历史路由属于所有者私有信息，不随公共仓库分发。
- Binance USD-M 1000-level snapshot/diff 健康探针：`.venv/bin/python scripts/probe_binance_deep_book.py --help`
- 当前部署和 live 命令：仓库 `README.md` / `README.zh-CN.md`

## Authority 预算

SHA256 只证明字节身份，不能代替部署、研究或健康语义。叶子 hash 留在拥有它们的
manifest 内；跨边界只暴露下一层真正需要的最小 root 集合。

| 边界 | 对外身份 | 刻意不向外复制的内容 |
| --- | --- | --- |
| 公开源码 | Git commit/tree；正式 release 再加一个 annotated tag | Git 已跟踪源码的逐文件 hash |
| Build/runtime | Runtime 或 wheelhouse manifest root | 每个 wheel、`RECORD`、依赖和 native 叶子 hash |
| Live release | Deployment-envelope root | Config、model、policy、lock、wheel 与 native 叶子 hash |
| 停机交易所屏障 | Reconciliation root | 在 service config 中重复账户、订单和仓位 hash |
| 已激活 live release | Activation-receipt root | 已由 receipt 绑定的 runtime identity 与 reconciliation 叶子 |
| 研究执行 | Source identity、runtime root、input-manifest root 与 output-receipt root | Cache key 和每个输入/输出叶子 hash |

可变 current pointer 不自哈希，只包含 `release_id`、activation-receipt root、schema
与 status；deployment-envelope root 从该不可变 activation receipt 派生并完成验证。
Owner-private release inventory 负责解析相应的不可变文件。Cache hash 只作为 cache
key，绝不授予 research 或 live authority。不得再把叶子 SHA 复制进 Python 常量、环境
变量、测试、Markdown、current pointer 或多层 receipt。

上传或启动远程 job 前，必须递归计算完整 materialization closure：admitted manifest
引用的每个文件，要么位于 transport bundle 内，要么作为单独命名的 task resource。
先对照各自所属 manifest 验证全部叶子，再用一个 input-manifest root 绑定 transport。
“归档上传成功”不等于 closure 已准入。

## 通用部署流程

公共部署内核不绑定云厂商，并明确分成两条边界：

- `make publish-source-dry` / `make publish-source` 只发布一个精确的公共源码 checkout；它们不会复制 config、model、credential、envelope、reconciliation receipt、runtime receipt，也不会执行任何进程控制命令。
- Runtime 构建与 activation 使用 checkout 之外、由部署者提供的私有材料。发布源码不等于取得交易授权，也不会启动交易。

源码 transport 要求本地 Git checkout 完全 clean。它从 `HEAD` 创建并验证 bundle，通过 SSH 流式传输到远端，在同一文件系统的 staging 目录 clone，验证精确 commit、tree 与 clean status，然后原子 rename 到指定的绝对 release path。若目标已是同一精确 release，则幂等接受；若已有 release 或 staging 的身份不同，则 fail closed。

安全顺序是：

1. 创建无特权 service user、由该用户拥有的 release parent，以及单独的 mode-`0700` private root；SSH 与出站凭证只开放 venue 连接所需的最小权限。
2. 从 clean public clone 运行 `make publish-source-dry`，核对绑定的 commit/tree，再运行 `make publish-source`。这一步只发布源码，绝不重启进程。
3. 把私有 config、model bundle 及其 authorization manifest、wheel、lock、wheelhouse 与已准入的输入 receipt 放在 private root 下，不能放进 Git checkout。Deployment envelope 与 stopped-exchange reconciliation 稍后必须针对精确 release 在该目录中构建。
4. 构建或接收 content-addressed wheelhouse，再用 `python3.12 -m live.deployment_runtime install` 创建 commit-bound 环境。Release wheel 必须来自 clean checkout/worktree；构建前清除生成的 `build/`、`dist/` 与 `*.egg-info` 状态，避免已删除文件从陈旧构建树进入 wheel。目标机器安装时不得从 package index 解析依赖。
5. 同时执行 `verify-install` 与 `verify-static-tree`，再把绝对 `venv-<execution-commit>` 路径绑定为 release 内被 Git 忽略的 `.venv-active` selector。
6. 从精确 checkout/runtime authority 构建 deployment envelope。Model authorization manifest 是必需的 envelope member；可选 policy artifacts 只能按完整组加入。针对私有配置执行 `make deploy-preflight`，并在 maker 完全停止时通过 `live/run.sh reconcile-stopped` 生成 exchange barrier。
7. 只有 process、runtime health、仓位/订单 reconciliation 和日志检查都通过后才能准入。然后构建紧凑的 activation receipt，再原子发布 current selector；精确 activation/rollback evidence 仅保存在私有侧。

Authority 命令都已经公开且通用：`live.deployment_runtime build-envelope` 从精确文件与 receipt 推导 deployment envelope；`live/run.sh reconcile-stopped ABSOLUTE_PATH` 只有在证明 maker 完全停止后才执行 signed exchange reads；`build-activation-receipt` 绑定已验证的 envelope、stopped reconciliation 和实际 live runtime identity；`publish-current-pointer` 在原子更新 selector 前会重新验证这条 lineage。必须使用它们输出的 canonical root，不能手写这些 JSON，也不能把旧 private receipt 复制进 current authority。

无需私有数据即可查看通用子命令与必填字段：

```bash
python3.12 -m live.deployment_runtime --help
python3.12 -m live.deployment_runtime install --help
python3.12 -m live.deployment_runtime verify-install --help
python3.12 -m live.deployment_runtime build-envelope --help
python3.12 -m live.deployment_runtime verify-envelope-startup --help
python3.12 -m live.deployment_runtime build-activation-receipt --help
python3.12 -m live.deployment_runtime publish-current-pointer --help
python3.12 -m live.native_build_receipt --help
python3.12 scripts/live_deploy_common.py source-release --help
```

## AWS EC2 示例

以下全部为占位符，不代表当前主机或 release identity。AWS live 部署不是“五分钟完成”的承诺；仓库 README 中的五分钟入口只指本地、不会交易的 demo。

EC2 创建检查清单：

- 使用带 systemd 的 64-bit Linux；locked live runtime 要求 CPython 3.12。
- 根卷启用加密，镜像中不保存可复用数据或凭证。
- SSH 入站只允许 operator CIDR，不开放公共应用端口。
- 出站只保留所配置 venue 的 HTTPS/WSS 端点；依赖在 build host 解析，目标安装时不联网解析 package。
- 使用专用无特权 `narrowgate` service user；root 仅用于主机与 service 设置。
- 在 operator/build host 使用下面的真实公开 URL clone；所有 operational authority 另行私有注入。

生产访问优先使用 [AWS Systems Manager Session Manager](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager.html)，或者通过 [EC2 security-group rules](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/security-group-rules.html) 严格限制 SSH。源码 transport 使用 operator 现有的 OpenSSH 配置，因此 SSM `ProxyCommand` 应放在 `~/.ssh/config`，不能作为不安全的命令行选项传入。

在实例上创建两个互相隔离的根目录。Release parent 由 service user 写入，且 group/other 不可写；private root 权限为 `0700`：

```bash
sudo useradd --system --create-home --shell /bin/bash narrowgate
sudo install -d -o narrowgate -g narrowgate -m 0755 /opt/narrowgate/releases
sudo install -d -o narrowgate -g narrowgate -m 0700 /opt/narrowgate/private
```

在 operator/build host clone 并发布一个精确、clean 的源码 release。locked live/native build 还必须提供一个显式的 annotated public release tag。传输层会验证 tag object 能 peel 到 `HEAD`，并且只传这个 tag，绝不传输本地全部 tags。纯源码 dry-run 可以省略它，但这样的结果不足以生成 live/native receipt。

```bash
git clone https://github.com/xiao-nanbei/NarrowGateMaker.git
cd NarrowGateMaker

export NARROWGATE_RELEASE_TAG="v0.1.2"
git checkout --detach "$NARROWGATE_RELEASE_TAG^{commit}"
export NARROWGATE_DEPLOY_TARGET="narrowgate@<ec2-address>"
export NARROWGATE_RELEASE_DIR="/opt/narrowgate/releases/<release-id>"

make publish-source-dry
make publish-source
```

该命令拒绝 dirty 本地 checkout，也拒绝相对 release path；它不会复制或读取任何私有材料。另行创建每个 release 的私有目录：

```bash
sudo install -d -o narrowgate -g narrowgate -m 0700 \
  /opt/narrowgate/private/<release-id>
```

通过部署者批准的 secret/artifact channel，把私有 config、model bundle 及其 authorization manifest、lock、wheelhouse、wheel 与已准入的输入 receipts 传到这个 private directory。不要预先复制其他 release 的 envelope 或 reconciliation；二者必须按下文构建。不得把私有材料放进 `/opt/narrowgate/releases/<release-id>`。

在实例上推导精确的 commit-bound venv 名称，并只使用已经传输且由 hash 绑定的 artifacts 安装。Install receipt 和 `venv-<commit>` 必须具有同一个私有 parent，因为 live startup 会强制检查这一关系：

```bash
RELEASE_DIR=/opt/narrowgate/releases/<release-id>
PRIVATE_DIR=/opt/narrowgate/private/<release-id>
COMMIT=$(git -C "$RELEASE_DIR" rev-parse HEAD)
VENV_DIR="$PRIVATE_DIR/venv-$COMMIT"
RECEIPT="$PRIVATE_DIR/install-receipt.json"
cd "$RELEASE_DIR"

python3.12 -m live.deployment_runtime install \
  --builder-python /usr/bin/python3.12 \
  --venv "$VENV_DIR" \
  --lock "$PRIVATE_DIR/runtime.lock.json" \
  --expected-lock-sha256 <lock-sha256> \
  --wheelhouse "$PRIVATE_DIR/wheelhouse" \
  --expected-wheelhouse-sha256 <wheelhouse-sha256> \
  --root-wheel "$PRIVATE_DIR/narrowgate.whl" \
  --root-wheel-sha256 <root-wheel-sha256> \
  --native-wheel "$PRIVATE_DIR/narrowgate-cpp.whl" \
  --native-wheel-sha256 <native-wheel-sha256> \
  --receipt "$RECEIPT"
```

记录 `install` 输出的 canonical receipt SHA256，在暴露 selector 前完成动态与静态两条验证命令：

```bash
python3.12 -m live.deployment_runtime verify-install \
  --builder-python /usr/bin/python3.12 \
  --venv "$VENV_DIR" \
  --lock "$PRIVATE_DIR/runtime.lock.json" \
  --expected-lock-sha256 <lock-sha256> \
  --wheelhouse "$PRIVATE_DIR/wheelhouse" \
  --expected-wheelhouse-sha256 <wheelhouse-sha256> \
  --root-wheel "$PRIVATE_DIR/narrowgate.whl" \
  --root-wheel-sha256 <root-wheel-sha256> \
  --native-wheel "$PRIVATE_DIR/narrowgate-cpp.whl" \
  --native-wheel-sha256 <native-wheel-sha256> \
  --receipt "$RECEIPT" \
  --expected-receipt-sha256 <install-receipt-canonical-sha256>

python3.12 -m live.deployment_runtime verify-static-tree \
  --venv "$VENV_DIR" \
  --receipt "$RECEIPT" \
  --expected-receipt-sha256 <install-receipt-canonical-sha256>

test ! -e "$RELEASE_DIR/.venv-active" && test ! -L "$RELEASE_DIR/.venv-active"
ln -s "$VENV_DIR" "$RELEASE_DIR/.venv-active"
test "$(readlink "$RELEASE_DIR/.venv-active")" = "$VENV_DIR"
```

从已经安装且绑定 commit 的环境执行通用 Linux native receipt。它会独立要求唯一 annotated tag、精确 wheel/runtime authority、native ABI 与 parity smoke tests：

```bash
NATIVE_RECEIPT="$PRIVATE_DIR/native-build-receipt.json"

"$VENV_DIR/bin/python3" -I -B "$RELEASE_DIR/live/native_build_receipt.py" \
  --repository-root "$RELEASE_DIR" \
  --annotated-tag "$NARROWGATE_RELEASE_TAG" \
  --wheel "$PRIVATE_DIR/narrowgate-cpp.whl" \
  --builder-python /usr/bin/python3.12 \
  --runtime-lock "$PRIVATE_DIR/runtime.lock.json" \
  --runtime-lock-sha256 <lock-sha256> \
  --dependency-wheelhouse "$PRIVATE_DIR/wheelhouse" \
  --dependency-wheelhouse-sha256 <wheelhouse-sha256> \
  --root-wheel "$PRIVATE_DIR/narrowgate.whl" \
  --root-wheel-sha256 <root-wheel-sha256> \
  --install-receipt "$RECEIPT" \
  --install-receipt-sha256 <install-receipt-canonical-sha256> \
  --output "$NATIVE_RECEIPT"
```

使用精确的私有 active config、native-build receipt 和由 model contract 选中并验证的 model authorization manifest 构建 deployment envelope。这一个必需 manifest 在内部绑定已准入的 model heads 与 P3 artifact；不得在部署命令、service environment 或 pointer 中重复它们的 leaf hashes。即使所有可选 action policy 都关闭，`--model-authorization` 也始终必需。如果 active config 绑定 SELL Boolean cooldown，必须同时提供 `--boolean-policy-file` 和
`--boolean-predicate-bundle`。如果绑定 BUY E3 policy，必须同时提供
`--policy-artifact-manifest`、`--policy-file` 和 `--predicate-bundle`。未启用的 policy
应整组省略。
将 `MODEL_AUTHORIZATION` 设置为 `scripts/preflight_live_deploy.py` 输出的精确 `model_authorization_path`；不得复制或重命名该文件。

```bash
ENVELOPE="$PRIVATE_DIR/deployment-envelope.json"
MODEL_AUTHORIZATION=<preflight-输出的精确-model_authorization_path>

python3.12 -m live.deployment_runtime build-envelope \
  --repository-root "$RELEASE_DIR" \
  --active-config "$PRIVATE_DIR/live-config.yaml" \
  --native-build-receipt "$NATIVE_RECEIPT" \
  --model-authorization "$MODEL_AUTHORIZATION" \
  --output "$ENVELOPE"
```

Config、model bundle、已构建的 deployment envelope 与已构建的 stopped-exchange reconciliation 都放在 `PRIVATE_DIR` 下，由 `narrowgate` 所有，私有文件权限为 `0600`。分两阶段准备 root-owned、mode-`0600` 的 service environment file，例如 `/etc/narrowgate/live.env`：先写入 config、envelope root 与 trusted Python locator，并为 stopped reconciliation 命令导出同一组值；再加入该命令打印的 reconciliation path 与 canonical root。Deployment envelope 是外部唯一的 release digest；nested manifests 在内部派生并验证 lock、wheelhouse、wheels、native module、interpreter、installed `RECORD`、config 与 policy members，不再把这些 leaf hashes 重复写进 service environment。

```bash
NARROWGATE_LIVE_CONFIG=<absolute-private-config-path>
NARROWGATE_DEPLOYMENT_ENVELOPE_PATH=<absolute-private-envelope-path>
NARROWGATE_DEPLOYMENT_ENVELOPE_CANONICAL_SHA256=<sha256>
NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_PATH=<absolute-private-reconciliation-path>
NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_CANONICAL_SHA256=<sha256>
NARROWGATE_STARTUP_TRUSTED_PYTHON_PATH=<absolute-cpython-3.12-builder-path-with-pip>
```

如果所选私有 policy 启用了对应 gated mechanism，其 invocation environment 还必须携带匹配的私有批准 flag：`NARROWGATE_ALLOW_Q90_PRIVATE_DEPLOY`、`NARROWGATE_ALLOW_F05_BUY_E3_PRIVATE_DEPLOY` 或 `NARROWGATE_ALLOW_F05_BOOLEAN_COOLDOWN_PRIVATE_DEPLOY`。不得设置无关 flag，也不得把这些批准提交到仓库。

Activation 前，在精确公共 checkout 中单独执行私有配置 preflight：

```bash
NARROWGATE_LIVE_CONFIG=<absolute-private-config-path> make deploy-preflight
```

Maker 完全停止后，在已经导出私有 config 与 deployment-envelope root 的环境中，以 `narrowgate` service user 生成 create-only exchange barrier。该命令会执行 signed venue REST reads，要求 open orders 为零且精确仓位稳定，并打印上述 reconciliation 环境字段所需的 path 与 canonical root。Account identity 保留在该 canonical payload 内，并与运行时 credential 比较，不再作为另一个外部 digest。

```bash
RECONCILIATION="$PRIVATE_DIR/stopped-exchange-reconciliation.json"
"$RELEASE_DIR/live/run.sh" reconcile-stopped "$RECONCILIATION"
```

`live/run.sh` 对 release startup authority 只接受 envelope path/root 与 trusted Python locator。它先用 trusted standard library 校验 canonical envelope，并在执行仓库代码前证明 Git commit/tree 与 clean checkout；随后调用 `verify-envelope-startup`，由 nested manifests 验证 runtime、installed distributions、全部 installed `RECORD` 与 `pip check`。Trusted Python path 是 OS bootstrap trust anchor：路径必须 canonical，文件必须 root-owned、非符号链接、单硬链接、可执行且 group/world 不可写。不要把 API credentials 或私有 authority 值写进 unit 或仓库。Service mode 只执行一次完整证明，随后 `run.sh` 用 maker 进程替换自身；systemd 是唯一进程 owner：

```ini
[Unit]
Description=NarrowGate live maker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=narrowgate
WorkingDirectory=/opt/narrowgate/current
EnvironmentFile=/etc/narrowgate/live.env
ExecStart=/opt/narrowgate/current/live/run.sh service
ExecReload=/opt/narrowgate/current/live/run.sh reload
Restart=no
KillSignal=SIGTERM
TimeoutStartSec=120
TimeoutStopSec=120

[Install]
WantedBy=multi-user.target
```

使用下文有时限的 transient systemd service 执行停机对账，每次 stop attempt 使用唯一
输出。systemd 读取 release-scoped、root-owned、mode-`0600` 的 environment file，
注入凭据而不把它们暴露给 operator shell；该文件必须选择预期 release 的 config 和
envelope。既有私有 release 目录应由 `narrowgate` 可写、mode 为 `0700`。不得复用旧
reconciliation，也不得把因 condition 跳过的 oneshot 退出状态当作本次成功。无需新增
持久 reconciliation unit；现场运行 `sudo -u narrowgate` 不是等价替代，因为该用户
不能读取 root-owned environment file。

只有全部私有 authority gate 都可用后，operator 才能原子切换另行准备的 `/opt/narrowgate/current` 文件系统 selector 并启动 service。源码发布本身绝不改变这个 selector。执行 `systemctl start narrowgate` 后，检查 `systemctl status narrowgate`、`live/run.sh status`、`logs/runtime_health.json` 与最近的 engine logs。PID 存活并不充分：仓位与 open-order reconciliation 必须收敛，而且不能存在 ownership 或 execution-state safety latch。不要在 unit 里添加 `ExecStop=live/run.sh stop`；systemd 应直接发送 `SIGTERM` 并给予完整的 `TimeoutStopSec` 优雅退出时间。`run.sh start|status|stop` 仍作为非 systemd 手工启动的兼容工具。

上述准入通过后，只绑定三个已验证的 activation 输入，然后发布紧凑的私有 current pointer。使用前面命令输出的 canonical root，以及已准入进程写入的绝对 runtime identity 路径：

```bash
ACTIVATION_RECEIPT="$PRIVATE_DIR/activation-receipt.json"
CURRENT_POINTER=/opt/narrowgate/private/current.json
RUNTIME_IDENTITY=<absolute-runtime-identity-path>

python3.12 -m live.deployment_runtime build-activation-receipt \
  --release-id <release-id> \
  --deployment-envelope "$ENVELOPE" \
  --deployment-envelope-sha256 <deployment-envelope-canonical-sha256> \
  --stopped-reconciliation "$RECONCILIATION" \
  --stopped-reconciliation-sha256 <stopped-reconciliation-canonical-sha256> \
  --runtime-identity "$RUNTIME_IDENTITY" \
  --output "$ACTIVATION_RECEIPT"

python3.12 -m live.deployment_runtime publish-current-pointer \
  --release-id <release-id> \
  --deployment-envelope "$ENVELOPE" \
  --deployment-envelope-sha256 <deployment-envelope-canonical-sha256> \
  --activation-receipt "$ACTIVATION_RECEIPT" \
  --activation-receipt-sha256 <activation-receipt-canonical-sha256> \
  --stopped-reconciliation "$RECONCILIATION" \
  --runtime-identity "$RUNTIME_IDENTITY" \
  --output "$CURRENT_POINTER"
```

JSON current pointer 只是一个四字段 release selector。其 `release_id` 通过
owner-private routing inventory 与对应 release directory 解析；pointer 本身不包含 host
routing 或 leaf artifact inventory。`status=selected_activation` 只表示 lineage 验证
选择了该 activation，不是 live-health 断言。旧的 verbose receipts、command transcript
和逐文件 hash inventory 可以继续作为私有 audit 附件，但不得参与 startup authority，
也不得复制进 current pointer。

Rollback 是另一次经过验证的部署，不是盲目 restart。先停止 service 并要求 clean stop，核对交易所仓位与 open orders，再把 release/config/envelope selectors 切到已验证的上一私有 release，重新运行 preflight 与 static-runtime verification，然后启动并重复 health admission。如果 stop 报告 uncertain execution state，在人工 reconciliation 完成前不得激活任一 release。

## EC2 日常运维

以下命令只使用 placeholder，并假定 systemd 是唯一进程 owner。不得再与手工
`run.sh start|stop` 兼容路径混用。

普通状态检查必须是有界只读操作：

```bash
sudo systemctl show narrowgate \
  --property=ActiveState,SubState,MainPID,ExecMainStatus,StateChangeTimestamp
sudo -u narrowgate /opt/narrowgate/current/live/run.sh status
```

停止 live，随后停止实例：

```bash
set -euo pipefail
: "${RELEASE_ID:?Set the intended release ID}"
[[ "$RELEASE_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]
ATTEMPT_ID="$(date -u +%Y%m%dT%H%M%SZ)-$(cat /proc/sys/kernel/random/uuid)"
RELEASE_DIR="/opt/narrowgate/releases/$RELEASE_ID"
RELEASE_ENV="/etc/narrowgate/releases/$RELEASE_ID.env"
OUTPUT="/opt/narrowgate/private/releases/$RELEASE_ID/stopped-$ATTEMPT_ID.json"
sudo systemctl stop narrowgate
test "$(systemctl is-active narrowgate)" = inactive
test -z "$(pgrep -f -- '[l]ive/main.py' || true)"
sudo test -d "$RELEASE_DIR"
sudo test -f "$RELEASE_ENV"
sudo test ! -e "$OUTPUT"
sudo systemd-run --wait --collect --service-type=oneshot \
  --unit="narrowgate-reconcile-$ATTEMPT_ID" \
  --property=User=narrowgate --property="EnvironmentFile=$RELEASE_ENV" \
  --property="WorkingDirectory=$RELEASE_DIR" --property=UMask=0077 \
  --property=NoNewPrivileges=true --property=TimeoutStartSec=120 \
  "$RELEASE_DIR/live/run.sh" reconcile-stopped "$OUTPUT"
sudo test -s "$OUTPUT"
test "$(systemctl is-active narrowgate)" = inactive
test -z "$(pgrep -f -- '[l]ive/main.py' || true)"
```

在 Bash 中运行并保证 operator 独占操作期间没有并发 activation 或 restart。
`--wait` 必须成功且此前不存在的输出现在必须存在；任何失败都会终止序列。后续
activation 使用这份新输出及其打印的 canonical root，不能使用同 release 的旧文件。

只有在 service 已 inactive、maker PID 已清零，并且 signed venue read 证明 open orders
为零且精确仓位稳定后，operator 才能在批准的控制机上执行
`aws ec2 stop-instances --instance-ids <instance-id>`。任一检查不可用或不确定时，保持
实例开启、拒绝 activation 并人工 reconciliation。不得用独立的五秒 kill ladder 截短
`TimeoutStopSec`。

启动已有实例不等于部署。先启动实例，确认 service 仍 inactive，验证选中的 current
pointer lineage 与 static runtime，生成新的 stopped-exchange reconciliation，再启动
systemd；health 观察通过后才能发布新 activation receipt。不得复用旧 reconciliation，
也不得把 `MainPID > 0` 当作准入。

常规部署不应在 EC2 上编译依赖或现场发现缺失 artifact。Linux runtime 和私有 bundle
预构建且 closure 完整时，合理的 operator 路径是源码 stage（两分钟以内）、离线验证
（数分钟）、stop/reconcile（通常五分钟以内）、有界 activation 观察（五到十分钟）。
Native 编译、依赖下载、closure 缺失、SSH 修复或人工 hash cascade 都属于前置条件失败，
不是正常部署耗时。

## Azure Batch 离线 replay

Azure Batch 是远程离线 executor，不是第二份 source of truth。本地 canonical 行情数据、
冻结日期、queue contract、seed 与 research permission 保持不变。Pool definition 可在
订阅周期内保留，但收费计算节点在研究批次之间必须缩到零。

一次性 pool bootstrap 必须 materialize：

1. 精确且 clean 的 source/runtime bundle；
2. 完整 input bundle 以及所有 manifest 引用的 receipt；
3. 冻结合同要求的真实 canonical 目录；
4. 冻结绝对路径无法改变时使用 bind mount；
5. Pool-scoped admin start task 只负责创建 host root、bind mount、解包、统一 owner/mode，并最后写 ready receipt；
6. Pool-scoped non-admin replay task 只读输入，只写 attempt-specific output root；
7. 私有目录由 replay 用户拥有且 mode 为 `0700`，私有 authority 文件 mode 为 `0600` 且只有一个 hard link。

不得用符号链接模拟 `/Volumes/...` 或其他冻结绝对 root；安全读取器会有意拒绝 symlink
路径组件。解包使用 `--no-same-owner`，随后统一 owner/mode，拒绝 group/world-writable
私有文件，并在提交 replay 前运行 start-task closure probe。Azure 的真实
`$AZ_BATCH_NODE_SHARED_DIR`、`$AZ_BATCH_TASK_WORKING_DIR` 与 `$AZ_BATCH_TASK_DIR`
必须和 compatibility bind mount 分开。若修复需要在已运行节点 materialize 一个小文件，
先用有界 admin preparation task，随后 replay 本身使用 non-admin。Admin replay task 只能是
临时 qualification 例外，不能成为 formal 目标状态。

公共仓库目前还没有一个 provider-neutral 命令同时创建 Batch pool 并完成上述 closure
probe。在这个通用 wrapper 实现前，私有 submission 层必须逐项执行这些检查；任一检查
不可用时禁止提交 formal task。本节是运维合同，不是在暗示一个并不存在的一键 CLI。

每个 formal task 对应一个 UTC day、占用一个 task slot、写入 attempt-specific output
namespace，并在最后写 `_SUCCESS`。进入 event loop 前，它验证 runtime root、
input-manifest root、freeze、supplement、plan 与所有小型 closure resource。失败 task 使用
新的 opaque attempt ID；resume 只跳过 manifest 与 success marker 完全匹配的输出。

故障恢复应保持低成本：

- 先 disable job，防止重复 task 启动；
- 已初始化节点保留一个有界的 30–60 分钟修复窗口；
- 只读取 task state 与错误日志，不提前读取结果经济值；
- Base runtime 与 data bundle 未变化时，用 task `ResourceFile` 交付小型缺失 immutable closure；
- 只有共享 base materialization 本身变化时才更新 pool start task 并 reimage；
- Queue 清空后把 target nodes 设为零，并确认没有意外残留收费 VM、disk、public IP 或 load balancer。

不得上传整个本地数据仓库、为一个小 receipt 漏项重建 20+ GiB bundle、在一个 task
内部跑多个 UTC day，或允许 task 从网络解析依赖。在预注册 aggregation 边界冻结并验证
前，经济结果继续保持 result-blind。
